package wasource

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Drop reasons. Each deliberate drop is counted under exactly one.
const (
	DropGroup           = "group"
	DropStatusBroadcast = "status_broadcast"
	DropBroadcastList   = "broadcast_list"
	DropNewsletter      = "newsletter"
	DropUnsupportedType = "unsupported_type" // reactions, protocol/revoke, stickers, ... (EventHandler's classify path)
	DropEdit            = "edit"             // edits arrive unwrapped as a new message; applying them is out of scope
	DropVoiceLive       = "voice_unsupported_live"
	DropHistorySync     = "history_sync"
	DropAfterStop       = "after_stop"
)

// maxEventTypeKeys caps how many distinct non-message event type names are
// tracked; the rest are folded into otherEventOverflow.
const (
	maxEventTypeKeys   = 32
	otherEventOverflow = "(other)"
)

// Stats counts what the source did with each event. Safe for concurrent
// use. It holds counts only, never content.
type Stats struct {
	mu          sync.Mutex
	forwarded   int64
	sinkFailed  int64
	sinkRetries int64
	lidUnmapped int64
	drops       map[string]int64
	otherEvents map[string]int64
}

func newStats() *Stats {
	return &Stats{drops: map[string]int64{}, otherEvents: map[string]int64{}}
}

func (s *Stats) drop(reason string) {
	s.mu.Lock()
	s.drops[reason]++
	s.mu.Unlock()
}

func (s *Stats) otherEvent(typeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.otherEvents[typeName]; !ok && len(s.otherEvents) >= maxEventTypeKeys {
		typeName = otherEventOverflow
	}
	s.otherEvents[typeName]++
}

func (s *Stats) add(field *int64) {
	s.mu.Lock()
	*field++
	s.mu.Unlock()
}

// StatsSnapshot is a point-in-time copy of Stats, safe to serialize.
type StatsSnapshot struct {
	Forwarded   int64            `json:"forwarded"`
	SinkFailed  int64            `json:"sink_failed"`
	SinkRetries int64            `json:"sink_retries"`
	LIDUnmapped int64            `json:"lid_unmapped"`
	Dropped     map[string]int64 `json:"dropped"`
	OtherEvents map[string]int64 `json:"other_events"`
}

func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := StatsSnapshot{
		Forwarded: s.forwarded, SinkFailed: s.sinkFailed, SinkRetries: s.sinkRetries, LIDUnmapped: s.lidUnmapped,
		Dropped: make(map[string]int64, len(s.drops)), OtherEvents: make(map[string]int64, len(s.otherEvents)),
	}
	for k, v := range s.drops {
		snap.Dropped[k] = v
	}
	for k, v := range s.otherEvents {
		snap.OtherEvents[k] = v
	}
	return snap
}

// String is a one-line summary for the periodic log.
func (s StatsSnapshot) String() string {
	parts := []string{
		"forwarded=" + itoa(s.Forwarded),
		"sink_failed=" + itoa(s.SinkFailed),
		"sink_retries=" + itoa(s.SinkRetries),
		"lid_unmapped=" + itoa(s.LIDUnmapped),
	}
	parts = append(parts, "dropped{"+mapString(s.Dropped)+"}", "other_events{"+mapString(s.OtherEvents)+"}")
	return strings.Join(parts, " ")
}

func mapString(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k + "=" + itoa(m[k])
	}
	return strings.Join(out, " ")
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
