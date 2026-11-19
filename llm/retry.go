package llm

import (
	"context"
	"math/rand/v2"
	"time"
)

// Policy is a bounded exponential backoff. The default mirrors what a
// well-behaved unattended session needs: long enough to ride out a
// rolling rate-limit window, short enough to give up to a waiting state
// rather than spin forever (deepseek-harness discussion #13621).
type Policy struct {
	Retries int
	Base    time.Duration
	Max     time.Duration
	Jitter  float64
}

func DefaultPolicy() Policy {
	return Policy{Retries: 8, Base: 500 * time.Millisecond, Max: 30 * time.Second, Jitter: 0.2}
}

// Do runs fn until it succeeds, returns a non-retryable error, or the
// policy budget is spent. The last error is returned.
func (p Policy) Do(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt <= p.Retries; attempt++ {
		err = fn(ctx)
		var ce *CallError
		if err == nil || ctx.Err() != nil {
			return err
		}
		if !AsCallError(err, &ce) || !ce.Retryable() {
			return err
		}
		delay := p.backoff(attempt)
		if ce.RetryAfter > delay {
			delay = ce.RetryAfter
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return err
}

func (p Policy) backoff(attempt int) time.Duration {
	d := p.Base << attempt
	if d > p.Max || d <= 0 {
		d = p.Max
	}
	if p.Jitter > 0 {
		j := 1 + p.Jitter*(2*rand.Float64()-1)
		d = time.Duration(float64(d) * j)
	}
	return d
}

// AsCallError unwraps err into ce when it is a classified provider error.
func AsCallError(err error, ce **CallError) bool {
	for err != nil {
		if c, ok := err.(*CallError); ok {
			*ce = c
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
