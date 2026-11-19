package kernel

import "sync"

// Bus is a minimal typed pub/sub. Subscribe returns a disposer; there are
// no wildcard or replay semantics — durable history lives in the session
// log, not in the bus.
type Bus[T any] struct {
	mu   sync.RWMutex
	next int
	subs map[int]chan T
}

func NewBus[T any]() *Bus[T] { return &Bus[T]{subs: map[int]chan T{}} }

// Publish delivers to each subscriber non-blockingly; a subscriber that
// does not keep up drops the event (consumers needing durability read the
// session log instead).
func (b *Bus[T]) Publish(v T) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- v:
		default:
		}
	}
}

// Subscribe buffers up to size events before dropping.
func (b *Bus[T]) Subscribe(size int) (<-chan T, Disposable) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	ch := make(chan T, size)
	b.subs[id] = ch
	var once sync.Once
	return ch, func() error {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			delete(b.subs, id)
			close(ch)
		})
		return nil
	}
}
