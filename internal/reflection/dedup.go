package reflection

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Defaults for DedupConfig. Both are first-guess starting values, not
// validated against real data — see DedupConfig's doc comment.
const (
	// DefaultDedupSimilarityThreshold is the minimum object-literal
	// similarity score (see literalSimilarity) for two edges to be
	// considered duplicates. 0.85 is a starting guess, not a tuned value:
	// this needs real tuning against real duplicate/non-duplicate object
	// literal pairs once there's actual usage data to tune it against.
	DefaultDedupSimilarityThreshold = 0.85

	// DefaultDedupWindow is how close together two edges' FirstSeen must
	// be to be eligible for dedup at all. 24 hours is a starting guess,
	// not a tuned value — proposal §9.2 says "a short window" but gives
	// no number. Picked deliberately narrow: the whole point of this
	// window is to separate "two messages about the same event, minutes
	// or hours apart" (a dedup case) from "the same fact stated again
	// weeks later" (a reinforcement case, already handled by
	// EdgeStore.Reinforce — see the decay/reinforcement issue). Too wide
	// a window here would swallow that distinction.
	DefaultDedupWindow = 24 * time.Hour
)

// DedupConfig carries the two knobs proposal §9.2's duplicate-edge merging
// needs: how similar two object literals must be, and how close together
// in time, to count as the same fact rather than two. Neither default
// value below is validated — both are reasoned starting guesses (see
// their doc comments) that need real tuning once there's real duplicate/
// non-duplicate data to tune against. Zero values fall back to the
// DefaultDedupX constants, matching this codebase's existing
// zero-value-fallback pattern (see reflection.TriggerConfig).
type DedupConfig struct {
	// SimilarityThreshold is the minimum literalSimilarity score for two
	// object literals to be treated as duplicates. Only consulted when
	// both edges being compared have ObjectLiteral set — entity-reference
	// objects (ObjectID) use exact-match instead, see objectsAreDuplicates.
	SimilarityThreshold float64

	// Window is the maximum gap between two edges' FirstSeen for them to
	// be eligible for merging at all, regardless of object similarity.
	Window time.Duration
}

// DefaultDedupConfig returns a DedupConfig populated with the package
// defaults.
func DefaultDedupConfig() DedupConfig {
	return DedupConfig{
		SimilarityThreshold: DefaultDedupSimilarityThreshold,
		Window:              DefaultDedupWindow,
	}
}

func (c DedupConfig) withDefaults() DedupConfig {
	if c.SimilarityThreshold <= 0 {
		c.SimilarityThreshold = DefaultDedupSimilarityThreshold
	}
	if c.Window <= 0 {
		c.Window = DefaultDedupWindow
	}
	return c
}

// MergeDuplicates implements proposal §9.2's duplicate-edge merging as a
// standalone, testable batch pass: given a candidate set of recently-
// written edges (the caller — the consolidation orchestrator — decides
// what "recent" means and queries for it; this function does not query
// for candidates itself), find near-identical edges and collapse each
// cluster of duplicates down to one surviving edge.
//
// Duplicate definition (see objectsAreDuplicates and isDuplicatePair for
// the precise rules):
//   - Same SubjectID AND same Predicate — exact match, never fuzzy.
//   - Same object: for ObjectID (entity reference) this means identical —
//     two edges pointing at different entities are a contradiction
//     (§7.2's territory), not a duplicate, and are never merged here. For
//     ObjectLiteral (text), this means literalSimilarity scores at or
//     above cfg.SimilarityThreshold.
//   - FirstSeen within cfg.Window of each other — outside the window,
//     it's reinforcement of an ongoing fact (EdgeStore.Reinforce's job),
//     not a dedup case.
//
// Candidates already superseded (SupersededBy != nil) are skipped
// entirely — both as potential survivors and as potential duplicates.
// Merging a fact that's already been superseded (by a prior dedup pass or
// a correction) would either resurrect stale provenance onto a new
// survivor or chain supersession in a confusing way; neither is useful,
// so this pass only ever looks at edges that are still current.
//
// Tombstones (memory.IsTombstone) are excluded the same way, for a
// related but distinct reason — the delete-exclusion audit's own
// finding here: a tombstone IS live (SupersededBy == nil) by every other
// criterion, and by construction (SoftDelete, delete_memory.go) shares
// the SAME subject/predicate/object the edge it replaced had, so a
// later, genuinely live edge asserting a similar fact can land in the
// same subject+predicate+similarity cluster as it. Without this
// exclusion, the survivor-picking rule (earliest FirstSeen) could make
// the tombstone itself the survivor — a confidence-0/importance-0
// deletion marker absorbing a real live edge's source_message_ids,
// corrupting it, exactly backward from what a delete is supposed to do.
//
// is_correction and decay_locked edges are ALSO excluded before
// clustering even starts (logged via log, nil-safe — this package's
// Logger interface, trigger.go, has no separate debug level, so Info is
// used for this low-severity, "why didn't X merge" line too). This is a
// deliberate design choice, not the only one considered: the alternative
// — letting a locked edge win as survivor when it matches an ordinary
// duplicate — was rejected, because merging would append ordinary
// messages' source_message_ids onto a correction's audit trail (the
// exact history IsCorrection exists to keep clean), and fuzzy literal
// similarity could fold a message that merely LOOKS similar but means
// something different into a locked, maximally-trusted edge. The
// consequence, accepted rather than worked around: a near-duplicate
// ORDINARY edge can remain live right next to a locked one indefinitely
// — it is never merged into the lock, and simply decays normally on its
// own, like any other live edge the lock doesn't apply to.
//
// Merging is never deletion (EdgeStore.Delete is intentionally
// unimplemented, edges are never hard-deleted). For each cluster of 2+
// duplicate edges, EdgeStore.MergeDuplicateEdges (memory package) does
// the actual write, as one transaction — see its own doc comment for the
// full mechanism (lock everything, re-validate under the lock, narrow
// writes only) and the three real bugs in the previous, non-transactional
// implementation it fixes (last_reinforced could move backwards; no
// protection for locked edges reaching the write path even if they slip
// past the candidate filter above via a race; lost updates from
// overwriting whole rows from a stale in-memory snapshot). This
// function's own responsibilities are: deciding WHICH edges are even
// candidates (the filtering above), WHICH ones cluster together
// (clusterDuplicates), and WHO the survivor is:
//   - The survivor is the edge with the earliest FirstSeen.
//   - The survivor keeps its own ObjectLiteral (the earliest phrasing) —
//     duplicates' literals are not merged or combined into it. That's a
//     deliberate scope limit: combining text isn't specified by the
//     proposal, and inventing it risks losing information a future reader
//     might want from the original edges (still reachable via
//     SourceMessageIDs/SupersededBy).
//
// Per-cluster atomicity, not per-pass: each cluster is merged in its OWN
// transaction (MergeDuplicateEdges' own Begin/Commit), not one
// transaction for the whole candidate set. If a cluster's merge returns
// memory.ErrMergeConflict (its candidates no longer match reality by the
// time the transaction ran — one became locked, got superseded, or
// simply no longer exists), that ONE cluster is logged and skipped, and
// the pass continues with the rest. Any OTHER error stops the pass
// immediately and returns mergedCount as of the last successfully
// committed cluster — those commits are already individually consistent
// and don't need to be undone. Deliberately NOT one big transaction for
// the whole pass: this pass is interruptible (the Reflection Engine's
// idle trigger, trigger.go, cancels an in-flight pass the moment live
// activity resumes), and holding locks across potentially thousands of
// candidate edges for the whole pass's duration would contend far more
// with concurrent live ingestion than letting already-merged clusters
// simply stay merged while a later one fails or the pass gets cut short.
//
// mergedCount is the number of edges superseded as duplicates (i.e. the
// sum of each successfully merged cluster's size minus one), not the
// number of clusters.
//
// Mutates candidates' own *Edge values in place on a successful merge
// (survivor.SourceMessageIDs/LastReinforced; each duplicate's
// SupersededBy) — not a side effect to be surprised by, but required:
// the (not yet built — see decay.go's own doc comment) consolidation
// orchestrator is documented to pass this SAME candidate slice on to
// ApplyBatchDecay afterward, which decides what to compute per edge by
// checking SupersededBy on those same pointers. Mirrors the AUTHORITATIVE
// post-merge values MergeDuplicateEdges itself computed under the lock,
// not a separate recomputation in this package that could diverge from
// what was actually written.
//
// NOTE on SupersededBy overload: this reuses the same supersession
// mechanism §7.2's contradiction handling and CorrectMemory use, rather
// than inventing a new "merged" flag — but that means a future reader of
// SupersededBy can't tell, from the field alone, whether an edge was
// superseded because it was contradicted or because it was a duplicate
// collapsed into another edge. If that distinction ever matters (e.g. for
// an audit view), it would need something like IsCorrection=false plus a
// new boolean flag — deliberately not built here, just flagged as a known
// tradeoff rather than a silent one.
func MergeDuplicates(ctx context.Context, store *memory.EdgeStore, candidates []*memory.Edge, cfg DedupConfig, log Logger) (mergedCount int, err error) {
	cfg = cfg.withDefaults()

	live := make([]*memory.Edge, 0, len(candidates))
	for _, e := range candidates {
		if e.SupersededBy != nil || memory.IsTombstone(*e) {
			continue
		}
		if e.IsCorrection || e.DecayLocked {
			if log != nil {
				log.Info("dedup.candidate_excluded_locked", map[string]any{
					"edge_id": e.ID, "is_correction": e.IsCorrection, "decay_locked": e.DecayLocked,
				})
			}
			continue
		}
		live = append(live, e)
	}

	for _, group := range groupBySubjectPredicate(live) {
		for _, cluster := range clusterDuplicates(group, cfg) {
			if len(cluster) < 2 {
				continue
			}

			n, mergeErr := mergeCluster(ctx, store, cluster)
			if mergeErr != nil {
				if errors.Is(mergeErr, memory.ErrMergeConflict) {
					if log != nil {
						log.Info("dedup.cluster_skipped_conflict", map[string]any{
							"cluster_size": len(cluster), "survivor_id": cluster[0].ID,
						})
					}
					continue
				}
				return mergedCount, mergeErr
			}
			mergedCount += n
		}
	}

	return mergedCount, nil
}

// subjectPredicateKey groups candidates by the two fields that must match
// exactly for edges to even be considered for dedup — see MergeDuplicates.
type subjectPredicateKey struct {
	subjectID uuid.UUID
	predicate string
}

func groupBySubjectPredicate(edges []*memory.Edge) map[subjectPredicateKey][]*memory.Edge {
	groups := make(map[subjectPredicateKey][]*memory.Edge)
	for _, e := range edges {
		key := subjectPredicateKey{subjectID: e.SubjectID, predicate: e.Predicate}
		groups[key] = append(groups[key], e)
	}
	return groups
}

// clusterDuplicates groups a single (subject_id, predicate) group's edges
// into duplicate clusters using union-find: edges i and j are unioned
// whenever isDuplicatePair(i, j) holds. This is deliberately not just a
// pairwise merge — three or more near-duplicate edges (e.g. the same
// event mentioned across three separate messages, each slightly
// reworded) need to collapse onto a single survivor, not leave a
// duplicate-of-a-duplicate unmerged because it wasn't directly compared
// against the eventual survivor.
//
// One consequence of union-find clustering worth naming explicitly: the
// window check (isDuplicatePair) is pairwise, so a chain A-B-C where A~B
// and B~C are both within cfg.Window, but A and C are not, still ends up
// in one cluster together (transitively, via B). This is the standard
// behavior of chaining/greedy clustering and is accepted here rather than
// worked around — enforcing a single survivor-relative window instead
// would make the result depend on iteration order (which edge happens to
// become the survivor), which is worse.
//
// Each returned cluster is sorted by FirstSeen ascending, so cluster[0]
// is always the intended survivor (see mergeCluster).
func clusterDuplicates(edges []*memory.Edge, cfg DedupConfig) [][]*memory.Edge {
	sorted := make([]*memory.Edge, len(edges))
	copy(sorted, edges)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].FirstSeen.Before(sorted[j].FirstSeen)
	})

	parent := make([]int, len(sorted))
	for i := range parent {
		parent[i] = i
	}

	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if isDuplicatePair(sorted[i], sorted[j], cfg) {
				union(i, j)
			}
		}
	}

	byRoot := make(map[int][]*memory.Edge)
	var roots []int
	for i, e := range sorted {
		root := find(i)
		if _, seen := byRoot[root]; !seen {
			roots = append(roots, root)
		}
		byRoot[root] = append(byRoot[root], e)
	}

	clusters := make([][]*memory.Edge, 0, len(roots))
	for _, root := range roots {
		clusters = append(clusters, byRoot[root])
	}
	return clusters
}

// isDuplicatePair reports whether a and b satisfy both duplicate
// conditions: FirstSeen within cfg.Window of each other, and matching
// objects per objectsAreDuplicates. Callers are expected to have already
// grouped by (subject_id, predicate), so those two fields are not
// re-checked here.
func isDuplicatePair(a, b *memory.Edge, cfg DedupConfig) bool {
	diff := b.FirstSeen.Sub(a.FirstSeen)
	if diff < 0 {
		diff = -diff
	}
	if diff > cfg.Window {
		return false
	}
	return objectsAreDuplicates(a, b, cfg.SimilarityThreshold)
}

// objectsAreDuplicates implements the object half of the duplicate
// definition (proposal §9.2): for entity references (ObjectID set),
// "similar" collapses to "identical" — there is no fuzzy entity matching,
// and two edges pointing at different entities are a contradiction
// (§7.2), never a duplicate. For text (ObjectLiteral set), similarity is
// scored by literalSimilarity against threshold.
func objectsAreDuplicates(a, b *memory.Edge, threshold float64) bool {
	if a.ObjectID != nil || b.ObjectID != nil {
		if a.ObjectID == nil || b.ObjectID == nil {
			// One edge references an entity, the other holds a literal —
			// not comparable, and per the schema's XOR constraint this
			// shouldn't occur for two edges sharing subject+predicate in
			// practice, but it is not a duplicate either way.
			return false
		}
		return *a.ObjectID == *b.ObjectID
	}

	if a.ObjectLiteral == nil || b.ObjectLiteral == nil {
		return false
	}
	return literalSimilarity(*a.ObjectLiteral, *b.ObjectLiteral) >= threshold
}

// literalSimilarity scores how similar two object literals are, in
// [0, 1]. Implemented as the Dice coefficient (2*|A∩B| / (|A|+|B|)) over
// each string's set of normalized tokens (lowercased, punctuation
// stripped, split on whitespace).
//
// This was picked over character-level edit distance (Levenshtein ratio)
// or requiring a Postgres extension (pg_trgm trigram similarity) for two
// reasons: it needs no new dependency (pg_trgm would mean a migration
// just for this), and it naturally tolerates the specific kind of
// near-duplicate this system actually produces — two extractions of the
// same short fact from nearby messages, differing by a word or two
// ("went to the gym" vs. "went to the gym today") rather than by
// character-level typos. Token-set overlap scores that kind of variation
// well; it would score poorly on typo-level differences ("teh" vs "the"),
// which is an accepted tradeoff for a personal-scale system rather than a
// reason to reach for a heavier (e.g. ML-based) similarity measure.
func literalSimilarity(a, b string) float64 {
	ta := normalizedTokenSet(a)
	tb := normalizedTokenSet(b)

	if len(ta) == 0 && len(tb) == 0 {
		return 1
	}
	if len(ta) == 0 || len(tb) == 0 {
		return 0
	}

	intersection := 0
	for tok := range ta {
		if tb[tok] {
			intersection++
		}
	}

	return 2 * float64(intersection) / float64(len(ta)+len(tb))
}

// normalizedTokenSet lowercases s, strips punctuation, and splits on
// whitespace into a set of unique tokens.
func normalizedTokenSet(s string) map[string]bool {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return unicode.ToLower(r)
		}
		return ' '
	}, s)

	set := make(map[string]bool)
	for _, tok := range strings.Fields(cleaned) {
		set[tok] = true
	}
	return set
}

// mergeCluster collapses a single cluster (sorted by FirstSeen ascending,
// per clusterDuplicates) onto cluster[0] as the survivor, and returns the
// number of duplicate edges merged into it.
//
// The actual write is ONE call to EdgeStore.MergeDuplicateEdges — see its
// own doc comment for the transaction/locking/re-validation it does.
// This function's only remaining job on success is mirroring the
// AUTHORITATIVE post-merge values it returns back onto THIS cluster's
// own *Edge pointers (survivor and duplicates are the same objects the
// caller's candidates slice holds — see MergeDuplicates' own doc comment
// on why that mutation matters for whatever reads candidates
// afterward), not recomputing anything independently in Go that could
// diverge from what was actually committed under the lock.
func mergeCluster(ctx context.Context, store *memory.EdgeStore, cluster []*memory.Edge) (int, error) {
	survivor := cluster[0]
	duplicates := cluster[1:]

	dupIDs := make([]uuid.UUID, len(duplicates))
	for i, dup := range duplicates {
		dupIDs[i] = dup.ID
	}

	updatedSurvivor, err := store.MergeDuplicateEdges(ctx, survivor.ID, dupIDs)
	if err != nil {
		return 0, err
	}

	survivor.SourceMessageIDs = updatedSurvivor.SourceMessageIDs
	survivor.LastReinforced = updatedSurvivor.LastReinforced
	survivorID := survivor.ID
	for _, dup := range duplicates {
		dup.SupersededBy = &survivorID
	}

	return len(duplicates), nil
}
