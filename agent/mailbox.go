package agent

import (
	"encoding/json"

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
