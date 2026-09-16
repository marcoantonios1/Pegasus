package api

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// TestGetStyleProfile_ReturnsNotImplemented locks in the honest stub
// behavior (requirement 13) — this must keep failing loudly until real
// style-signal storage and an extraction step both exist, per
// GetStyleProfile's own doc comment. If this test ever needs to change to
// pass, that change must come with the real upstream pipeline built, not
// just a relaxed assertion here.
func TestGetStyleProfile_ReturnsNotImplemented(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	profile, err := a.GetStyleProfile(ctx, uuid.New())
	if !errors.Is(err, ErrStyleProfileNotImplemented) {
		t.Errorf("expected ErrStyleProfileNotImplemented, got %v", err)
	}
	if profile != nil {
		t.Errorf("expected a nil profile (no fabricated data), got %+v", profile)
	}
}
