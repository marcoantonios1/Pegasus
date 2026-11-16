package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/marcoantonios1/Pegasus/internal/identity"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// errSelfNotBootstrapped is returned by the write pipeline when it is used
// before BootstrapSelf: without the self entity, Marco's own messages
// would be attributed to a contact and his corrections silently ignored.
var errSelfNotBootstrapped = errors.New("self identity not bootstrapped: call API.BootstrapSelf before ingesting messages")

// SelfConflictError lists every reason BootstrapSelf refused to start.
// None are auto-repaired: merging entities is a separate tool, and wiping
// a disposable dev database is the remedy there.
type SelfConflictError struct{ Problems []string }

func (e *SelfConflictError) Error() string {
	return "self identity bootstrap refused:\n  - " + strings.Join(e.Problems, "\n  - ")
}

// BootstrapSelf ties the configured self identity to the single is_self
// entity. Call it once at startup, after migrations are validated and
// before any recovery or intake. It:
//
//   - loads the existing is_self entity, or creates one named
//     reg.SelfName() if none exists;
//   - refuses (SelfConflictError, nothing changed) when an existing self
//     has a different name, or when a NON-self entity already holds the
//     self name, an alias, or a configured identity string (earlier runs
//     attributed Marco's messages to a contact: two entities would claim
//     to be him);
//   - asserts exactly one is_self row exists;
//   - registers reg and caches the self entity for resolveSender and
//     resolveEntity.
//
// Safe to run concurrently from several processes: GetOrCreateSelf
// resolves the creation race, and the single-self index guarantees one row.
func (a *API) BootstrapSelf(ctx context.Context, reg *identity.Registry) (*memory.Entity, error) {
	if reg == nil {
		return nil, errors.New("BootstrapSelf: no self identity configured")
	}

	existing, err := a.entities.GetSelf(ctx)
	if err != nil {
		return nil, fmt.Errorf("BootstrapSelf: load existing self entity: %w", err)
	}

	var problems []string
	if existing != nil && !strings.EqualFold(existing.CanonicalName, reg.SelfName()) {
		problems = append(problems, fmt.Sprintf(
			"the existing self entity %s is named %q but the configured self name is %q; set the self name to %q, or rename that entity deliberately",
			existing.ID, existing.CanonicalName, reg.SelfName(), existing.CanonicalName))
	}

	check := func(kind, name string) {
		e, err := a.entities.GetByCanonicalName(ctx, name)
		if err != nil {
			problems = append(problems, fmt.Sprintf("look up %s %q: %v", kind, name, err))
			return
		}
		if e != nil && !e.IsSelf {
			problems = append(problems, fmt.Sprintf(
				"non-self entity %s (%q) already holds the %s %q: two entities would claim to be Marco (earlier ingestion likely attributed his messages to a contact). Merge them with a dedicated tool, or wipe the database if it is disposable dev data",
				e.ID, e.CanonicalName, kind, name))
		}
	}
	check("self name", reg.SelfName())
	for _, n := range reg.Aliases() {
		check("alias", n)
	}
	for _, n := range reg.AllIdentities() {
		check("configured identity", n)
	}
	if len(problems) > 0 {
		return nil, &SelfConflictError{Problems: problems}
	}

	self := existing
	if self == nil {
		self, err = a.entities.GetOrCreateSelf(ctx, reg.SelfName())
		if err != nil {
			return nil, fmt.Errorf("BootstrapSelf: create self entity: %w", err)
		}
		if !strings.EqualFold(self.CanonicalName, reg.SelfName()) {
			// A concurrent process created a differently named self first.
			return nil, &SelfConflictError{Problems: []string{fmt.Sprintf(
				"a concurrently created self entity %s is named %q, not %q", self.ID, self.CanonicalName, reg.SelfName())}}
		}
	}

	n, err := a.entities.CountSelf(ctx)
	if err != nil {
		return nil, fmt.Errorf("BootstrapSelf: count self entities: %w", err)
	}
	if n != 1 {
		return nil, fmt.Errorf("BootstrapSelf: expected exactly one is_self entity, found %d", n)
	}

	a.identity = reg
	a.self = self
	return self, nil
}

// Self returns the bootstrapped self entity, or nil before BootstrapSelf.
func (a *API) Self() *memory.Entity { return a.self }

// speakerLabel is the name an extraction window shows for a message's
// sender: the self entity's canonical name for Marco on every platform and
// path (live, import, recovery), so the model always sees "Marco" for his
// own messages; otherwise the path's existing label.
func (a *API) speakerLabel(sender *memory.Entity, fallback string) string {
	if sender != nil && sender.IsSelf {
		return sender.CanonicalName
	}
	return fallback
}
