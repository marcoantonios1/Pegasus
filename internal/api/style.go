package api

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrStyleProfileNotImplemented is GetStyleProfile's permanent return
// value until the real prerequisites exist — see GetStyleProfile's doc
// comment. Exported so a caller (Hermes) can detect this specific,
// expected state and fall back to some default tone/style rather than
// treating it as a generic failure.
var ErrStyleProfileNotImplemented = errors.New("api: GetStyleProfile is not implemented — no style-signal storage or extraction step exists yet")

// StyleProfile is the eventual output shape for proposal §3.5's style
// learning (sentence length, emoji usage, code-switching, humor style,
// per-relationship variation). Defined now so GetStyleProfile has a
// concrete return type to be implemented against later, but every field
// here is presently unpopulated — GetStyleProfile never returns a
// non-nil *StyleProfile today, only ErrStyleProfileNotImplemented.
type StyleProfile struct {
	ContactID uuid.UUID
}

// GetStyleProfile is BLOCKED and deliberately not faked. §3.5's style
// learning — sentence length, emoji usage, code-switching, humor style,
// per-relationship variation — has no schema, no extraction/analysis
// pipeline step, and no storage anywhere in this codebase as of this
// issue: no style_profiles table (or equivalent), nothing in
// internal/extraction that computes any of these signals from message
// history. This is the same category of gap as relationship-stats'
// humor_level (internal/reflection/relationship_stats.go) — a real
// implementation needs BOTH (a) a storage layer for these signals and (b)
// an actual analysis step somewhere in the pipeline that produces them
// from real message history, and building either of those is a
// meaningfully different, larger task than "implement a read method
// against data that already exists," which is this issue's actual scope.
//
// This always returns ErrStyleProfileNotImplemented rather than a
// StyleProfile populated with fabricated numbers. A method that silently
// returns made-up style characteristics is worse than one that visibly
// says it isn't built yet — Hermes would consume this output to shape its
// actual reply tone, and fabricated "he uses emoji 40% of the time" data
// masquerading as real signal is actively harmful in a way a clear error
// is not.
func (a *API) GetStyleProfile(ctx context.Context, contactID uuid.UUID) (*StyleProfile, error) {
	return nil, ErrStyleProfileNotImplemented
}
