package api

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/identity"
)

// withTestSelf binds the API to the test database's self entity and
// registers ids as Marco's sender identities (on both platforms), standing
// in for BootstrapSelf in tests that run against the shared dev database.
// That database already holds an is_self entity from earlier runs under an
// arbitrary name, which the strict BootstrapSelf would (correctly) refuse;
// BootstrapSelf itself is tested on fresh databases (bootstrap_self_test.go).
// With no ids, a unique placeholder identity is registered so that no
// test sender is accidentally Marco.
func (a *API) withTestSelf(t testing.TB, ids ...string) *API {
	t.Helper()
	ctx := context.Background()
	self, err := a.entities.GetSelf(ctx)
	if err != nil {
		t.Fatalf("withTestSelf: load self: %v", err)
	}
	if self == nil {
		if self, err = a.entities.GetOrCreateSelf(ctx, "Test Self "+uuid.NewString()); err != nil {
			t.Fatalf("withTestSelf: create self: %v", err)
		}
	}
	if len(ids) == 0 {
		ids = []string{"test-self-" + uuid.NewString()}
	}
	reg, problems := identity.New(identity.Config{SelfName: self.CanonicalName, WhatsApp: ids, Instagram: ids})
	if len(problems) > 0 {
		t.Fatalf("withTestSelf: %v", problems)
	}
	a.identity, a.self = reg, self
	return a
}
