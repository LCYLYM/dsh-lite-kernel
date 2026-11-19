package agent

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/LCYLYM/dsh-lite-kernel/session"
)

// Mailbox makes cross-agent notices durable. A notice is appended to the
// recipient's log before it is considered delivered; pending notices are
// replayed on resume, so a child finishing while its parent is idle can
// never lose its completion notice silently (discussion #13656).
//
// Event types:
//
//	mailbox/notice   {id, from, kind, payload}
//	mailbox/delivered{id}
type Notice struct {
	ID      string          `json:"id"`
	From    string          `json:"from"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type deliveredData struct {
	ID string `json:"id"`
}

// Post appends a notice to a session's log. It is durable before anyone
// has consumed it.
func Post(lg *session.Log, n Notice) error {
	_, err := lg.Append("mailbox/notice", n)
	return err
}

// Delivered marks a notice consumed — replay skips it.
func Delivered(lg *session.Log, id string) error {
	_, err := lg.Append("mailbox/delivered", deliveredData{ID: id})
	return err
}

// Pending scans the log for notices not yet delivered. Delivering twice
// is impossible by construction.
func Pending(lg *session.Log) ([]Notice, error) {
	delivered := map[string]bool{}
	var pending []Notice
	err := lg.ReadFrom(0, func(ev session.Event) bool {
		switch ev.Type {
		case "mailbox/notice":
			var n Notice
			if json.Unmarshal(ev.Data, &n) == nil && !delivered[n.ID] {
				pending = append(pending, n)
			}
		case "mailbox/delivered":
			var d deliveredData
			if json.Unmarshal(ev.Data, &d) == nil {
				delete(pendingSet(pending), d.ID)
				delivered[d.ID] = true
				pending = filterOut(pending, d.ID)
			}
		}
		return true
	})
	return pending, err
}

func pendingSet(ns []Notice) map[string]bool {
	m := make(map[string]bool, len(ns))
	for _, n := range ns {
		m[n.ID] = true
	}
	return m
}

func filterOut(ns []Notice, id string) []Notice {
	out := ns[:0]
	for _, n := range ns {
		if n.ID != id {
			out = append(out, n)
		}
	}
	return out
}

// mailboxState indexes notices per log: rebuilt once on first drain,
// then advanced only over newly appended events, so a step's drain
// costs O(new events) instead of O(history). One mutex serializes the
// whole select→inject→ack batch — two turns on the same log cannot
// inject the same notice twice.
type mailboxState struct {
	mu        sync.Mutex
	scanned   int64
	delivered map[string]bool
	pending   map[string]Notice
}

// maxDrainBatch caps one drain's injection well below any plausible
// context window — a giant batch would push its own inject event out
// of the tail window while its acks stayed in, delivering nothing to
// the model. Notices beyond the cap stay pending for the next step.
const maxDrainBatch = 64

var mailboxes sync.Map // *session.Log -> *mailboxState

func mailboxFor(lg *session.Log) *mailboxState {
	m, loaded := mailboxes.LoadOrStore(lg, &mailboxState{
		delivered: map[string]bool{}, pending: map[string]Notice{},
	})
	if !loaded {
		// The index must not root a closed log forever.
		lg.OnClose(func() { mailboxes.Delete(lg) })
	}
	return m.(*mailboxState)
}

// Drain scans new log events for notices, injects every pending one
// into the model-visible stream, then acknowledges. The inject event
// lands BEFORE the acknowledgements: a crash between them replays the
// notice as a duplicate (deduped for the model by BuildContext), never
// a silent loss.
func Drain(lg *session.Log) error {
	mb := mailboxFor(lg)
	mb.mu.Lock()
	defer mb.mu.Unlock()
	last := mb.scanned
	err := lg.ReadFrom(mb.scanned, func(ev session.Event) bool {
		last = ev.Seq
		switch ev.Type {
		case "mailbox/notice":
			var n Notice
			if json.Unmarshal(ev.Data, &n) == nil && !mb.delivered[n.ID] {
				mb.pending[n.ID] = n
			}
		case "mailbox/delivered":
			var d deliveredData
			if json.Unmarshal(ev.Data, &d) == nil {
				delete(mb.pending, d.ID)
				mb.delivered[d.ID] = true
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	mb.scanned = last
	if len(mb.pending) == 0 {
		return nil
	}
	ids := make([]string, 0, len(mb.pending))
	for id := range mb.pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > maxDrainBatch {
		ids = ids[:maxDrainBatch]
	}
	arr := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		n := mb.pending[id]
		arr = append(arr, map[string]any{
			"id": n.ID, "from": n.From, "kind": n.Kind, "payload": n.Payload,
		})
	}
	if _, err := lg.Append("mailbox/inject", map[string]any{"notices": arr}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := Delivered(lg, id); err != nil {
			return err
		}
		delete(mb.pending, id)
		mb.delivered[id] = true
	}
	return nil
}
