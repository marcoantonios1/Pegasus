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
