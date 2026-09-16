package memory

import (
	"time"

	"github.com/google/uuid"
)

// Entity is a person, topic, place, project, event, or object the system
// tracks facts about. See Pegasus proposal §6.2.
type Entity struct {
	ID            uuid.UUID      // see Pegasus proposal §6.2
	Type          string         // see Pegasus proposal §6.2 — 'person' | 'topic' | 'place' | 'project' | 'event' | 'object'
	CanonicalName string         // see Pegasus proposal §6.2
	IsSelf        bool           // see Pegasus proposal §6.2
	Metadata      map[string]any // see Pegasus proposal §6.2 — JSONB
	CreatedAt     time.Time      // see Pegasus proposal §6.2
	UpdatedAt     time.Time      // see Pegasus proposal §6.2
}

// Edge is a subject-predicate-object fact linking two entities, or an entity
// to a literal value. See Pegasus proposal §6.2.
type Edge struct {
	ID               uuid.UUID   // see Pegasus proposal §6.2
	SubjectID        uuid.UUID   // see Pegasus proposal §6.2
	Predicate        string      // see Pegasus proposal §6.2 — closed vocab enforced at app layer, not DB enum
	ObjectID         *uuid.UUID  // see Pegasus proposal §6.2 — nullable; exactly one of ObjectID/ObjectLiteral set, per DB check constraint
	ObjectLiteral    *string     // see Pegasus proposal §6.2 — nullable; exactly one of ObjectID/ObjectLiteral set, per DB check constraint
	Confidence       float64     // see Pegasus proposal §6.2
	Importance       float64     // see Pegasus proposal §6.2
	SourceType       string      // see Pegasus proposal §6.2 — 'whatsapp_text' | 'voice_transcript' | 'live_image' | 'user_correction' | ...
	SourceWeight     float64     // see Pegasus proposal §6.2
	SourceMessageIDs []uuid.UUID // see Pegasus proposal §6.2
	FirstSeen        time.Time   // see Pegasus proposal §6.2
	LastReinforced   time.Time   // see Pegasus proposal §6.2
	DecayRate        float64     // see Pegasus proposal §6.2
	DecayLocked      bool        // see Pegasus proposal §6.2
	SupersededBy     *uuid.UUID  // see Pegasus proposal §6.2 — nullable, self-referencing
	IsCorrection     bool        // see Pegasus proposal §6.2
}

// Message is a single inbound unit of communication (text, voice, image, or
// video) from a conversation. See Pegasus proposal §6.2.
type Message struct {
	ID                   uuid.UUID // see Pegasus proposal §6.2
	ConversationID       uuid.UUID // see Pegasus proposal §6.2
	SenderID             uuid.UUID // see Pegasus proposal §6.2
	MediaType            string    // see Pegasus proposal §6.2 — 'text' | 'voice' | 'image' | 'video'
	RawText              *string   // see Pegasus proposal §6.2 — nullable
	Transcript           *string   // see Pegasus proposal §6.2 — nullable
	TranscriptConfidence *float64  // see Pegasus proposal §6.2 — nullable
	MediaRef             *string   // see Pegasus proposal §6.2 — nullable
	Processed            bool      // see Pegasus proposal §6.2
	Timestamp            time.Time // see Pegasus proposal §6.2
}

// Embedding is a nomic-embed-text vector for a message, used for semantic
// retrieval. See Pegasus proposal §6.2.
type Embedding struct {
	ID        uuid.UUID // see Pegasus proposal §6.2
	MessageID uuid.UUID // see Pegasus proposal §6.2
	Vector    []float32 // see Pegasus proposal §6.2 — 768-dim, nomic-embed-text
}

// RelationshipStats is the Reflection Engine's per-contact relationship
// signal (proposal §10), recomputed during consolidation rather than live
// per message — see reflection.RecomputeRelationshipStats. One current
// row per contact (relationship_stats_contact_id_idx's UNIQUE constraint
// on ContactID), overwritten on each consolidation pass rather than kept
// as history — see migration 000006's comment for why.
type RelationshipStats struct {
	ID uuid.UUID

	// ContactID is the person entity this row is about — a 'person'-type
	// entity, never Marco's own is_self row. See Pegasus proposal §10.
	ContactID uuid.UUID

	// Frequency is this contact's message volume in the trailing window,
	// normalized against Marco's mean across all contacts over the same
	// window — ~1.0 means "about average", not an absolute rate. See
	// Pegasus proposal §10 and reflection.RecomputeRelationshipStats.
	Frequency float64

	// ReplySpeed is the median reply latency in seconds, stored raw
	// (format at read time, per this issue's schema note) — see Pegasus
	// proposal §10. -1 is a sentinel for "no reply pairs observed in the
	// window", distinct from 0 (an actual instant reply); see
	// reflection.RecomputeRelationshipStats.
	ReplySpeed float64

	// HumorLevel is currently always 0 — there is no upstream sentiment/
	// humor classification signal for this to read from yet (not in the
	// messages or edges schema). See reflection.RecomputeRelationshipStats'
	// doc comment for the full explanation; this is a known, flagged gap,
	// not a fabricated number.
	HumorLevel float64

	// Closeness is the hand-weighted composite of the above plus recency
	// and explicit signals (nickname_is/inside_joke_ref edge counts).
	// Weights are a first guess pending real-data tuning, per §10's own
	// "weights are set manually first". See
	// reflection.RecomputeRelationshipStats.
	Closeness float64

	ComputedAt time.Time
}
