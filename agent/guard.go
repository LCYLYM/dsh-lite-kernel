package agent

import (
	"strings"
	"unicode/utf8"
)

// Guard bounds what one turn may do. The upstream failure it prevents:
// a model emitting ~98k chars of self-repeating reasoning with zero tool
// calls while the host reported a normal "completed" turn — nobody
// noticed because step success only checked "was there an assistant
// message" (deepseek-harness discussions #13659, #13638).
type Guard struct {
	MaxSteps         int // steps allowed in one turn
	MaxZeroToolSteps int // consecutive steps with no tool call allowed
	RepeatWindow     int // trailing chars inspected for n-gram spam
	RepeatThreshold  int // repeats of the same short n-gram that trigger abort
	MaxOutputChars   int // per-step visible-output cap
	CallTimeout      int // per provider call, seconds; <=0 disables
	MaxSessionTokens int // lifetime prompt+completion tokens; 0 = unbounded
}

func DefaultGuard() Guard {
	return Guard{
		MaxSteps:         40,
		MaxZeroToolSteps: 4,
		RepeatWindow:     4096,
		RepeatThreshold:  40,
		MaxOutputChars:   256 << 10,
		CallTimeout:      120,
		MaxSessionTokens: 2_000_000,
	}
}

// CheckStep inspects one model output before it is committed.
// Returns (abort, reason).
func (g Guard) CheckStep(text string, toolCalls int) (bool, string) {
	if toolCalls == 0 && g.stuck(text) {
		return true, "output collapsed into repetition with no tool calls"
	}
	if g.MaxOutputChars > 0 && utf8.RuneCountInString(text) > g.MaxOutputChars {
		return true, "step exceeded MaxOutputChars"
	}
	return false, ""
}

// stuck reports whether the tail of s is dominated by one repeating
// n-gram — the "做。做。做。…" loop (#13659).
func (g Guard) stuck(s string) bool {
	if len(s) < 512 {
		return false
	}
	win := g.RepeatWindow
	if win <= 0 || len(s) < win {
		win = len(s)
	}
	tail := s[len(s)-win:]
	for n := 2; n <= 32; n += 2 {
		if len(tail) < n*(g.RepeatThreshold/2) {
			continue
		}
		unit := tail[len(tail)-n:]
		count := strings.Count(tail, unit)
		if count >= g.RepeatThreshold {
			return true
		}
	}
	return false
}
