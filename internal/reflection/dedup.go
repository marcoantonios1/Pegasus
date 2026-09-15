package reflection

import (
	"context"
	"fmt"
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
// Merging is never deletion (EdgeStore.Delete is intentionally
// unimplemented, edges are never hard-deleted). For each cluster of 2+
// duplicate edges:
//   - The survivor is the edge with the earliest FirstSeen.
//   - The survivor's SourceMessageIDs gains every other cluster member's
//     SourceMessageIDs appended on, via Update() (Reinforce doesn't touch
//     that field), and its LastReinforced is bumped via Reinforce() (not
//     Update()) to the latest FirstSeen among the cluster — the point in
//     time the fact was last independently reconfirmed. Update() is
//     called before Reinforce() specifically so Reinforce's timestamp
//     bump is the last write and can't be clobbered by Update() writing
//     back the survivor's pre-merge (stale) LastReinforced.
//   - The survivor keeps its own ObjectLiteral (the earliest phrasing) —
//     duplicates' literals are not merged or combined into it. That's a
//     deliberate scope limit: combining text isn't specified by the
//     proposal, and inventing it risks losing information a future reader
//     might want from the original edges (still reachable via
//     SourceMessageIDs/SupersededBy).
//   - Every other cluster member has its SupersededBy set to the
//     survivor's ID via Update(); no other field on it is touched.
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
//
// mergedCount is the number of edges superseded as duplicates (i.e. the
// sum of each cluster's size minus one), not the number of clusters.
func MergeDuplicates(ctx context.Context, store *memory.EdgeStore, candidates []*memory.Edge, cfg DedupConfig) (mergedCount int, err error) {
	cfg = cfg.withDefaults()

	live := make([]*memory.Edge, 0, len(candidates))
	for _, e := range candidates {
		if e.SupersededBy == nil {
			live = append(live, e)
		}
	}

	for _, group := range groupBySubjectPredicate(live) {
		for _, cluster := range clusterDuplicates(group, cfg) {
			if len(cluster) < 2 {
				continue
			}

			n, err := mergeCluster(ctx, store, cluster)
			if err != nil {
				return mergedCount, err
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
// number of duplicate edges merged into it. See MergeDuplicates' doc
// comment for the write ordering (Update before Reinforce) and why.
func mergeCluster(ctx context.Context, store *memory.EdgeStore, cluster []*memory.Edge) (int, error) {
	survivor := cluster[0]
	duplicates := cluster[1:]
	survivorID := survivor.ID

	mergedMessageIDs := append([]uuid.UUID{}, survivor.SourceMessageIDs...)
	latestConfirmation := survivor.FirstSeen
	for _, dup := range duplicates {
		mergedMessageIDs = append(mergedMessageIDs, dup.SourceMessageIDs...)
		if dup.FirstSeen.After(latestConfirmation) {
			latestConfirmation = dup.FirstSeen
		}
	}
	survivor.SourceMessageIDs = mergedMessageIDs

	if err := store.Update(ctx, survivor); err != nil {
		return 0, fmt.Errorf("dedup: update survivor edge %s with merged source_message_ids: %w", survivor.ID, err)
	}

	if err := store.Reinforce(ctx, survivor.ID, latestConfirmation); err != nil {
		return 0, fmt.Errorf("dedup: reinforce survivor edge %s: %w", survivor.ID, err)
	}
	survivor.LastReinforced = latestConfirmation

	for _, dup := range duplicates {
		dup.SupersededBy = &survivorID
		if err := store.Update(ctx, dup); err != nil {
			return 0, fmt.Errorf("dedup: supersede duplicate edge %s: %w", dup.ID, err)
		}
	}

	return len(duplicates), nil
}
