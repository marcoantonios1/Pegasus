package memory

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RelationshipStatsStore struct {
	db *pgxpool.Pool
}

func NewRelationshipStatsStore(db *pgxpool.Pool) *RelationshipStatsStore {
	return &RelationshipStatsStore{db: db}
}

// Upsert writes stats for stats.ContactID, replacing any existing current
// row for that contact — relationship_stats_contact_id_idx's UNIQUE
// constraint on contact_id is what ON CONFLICT targets here. This table
// holds one current row per contact by design (see migration 000006's
// comment): a consolidation pass recomputing a contact's stats overwrites
// the previous pass's numbers rather than accumulating history. Sets
// stats.ID and stats.ComputedAt from the write.
//
// This table intentionally has no full CRUD (no GetByID, no Delete) —
// just this write path and GetByContactID below, per this issue's scope:
// a Recompute-and-fetch pattern, nothing more.
func (s *RelationshipStatsStore) Upsert(ctx context.Context, stats *RelationshipStats) error {
	return s.db.QueryRow(ctx, `
		INSERT INTO relationship_stats (contact_id, frequency, reply_speed, humor_level, closeness)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (contact_id) DO UPDATE SET
			frequency = EXCLUDED.frequency,
			reply_speed = EXCLUDED.reply_speed,
			humor_level = EXCLUDED.humor_level,
			closeness = EXCLUDED.closeness,
			computed_at = now()
		RETURNING id, computed_at
	`,
		stats.ContactID,
		stats.Frequency,
		stats.ReplySpeed,
		stats.HumorLevel,
		stats.Closeness,
	).Scan(&stats.ID, &stats.ComputedAt)
}

// GetByContactID returns the current relationship_stats row for contactID,
// or an error wrapping pgx.ErrNoRows if consolidation has never computed
// stats for that contact.
func (s *RelationshipStatsStore) GetByContactID(ctx context.Context, contactID uuid.UUID) (*RelationshipStats, error) {
	var r RelationshipStats

	err := s.db.QueryRow(ctx, `
		SELECT id, contact_id, frequency, reply_speed, humor_level, closeness, computed_at
		FROM relationship_stats
		WHERE contact_id = $1
	`, contactID).Scan(&r.ID, &r.ContactID, &r.Frequency, &r.ReplySpeed, &r.HumorLevel, &r.Closeness, &r.ComputedAt)
	if err != nil {
		return nil, err
	}

	return &r, nil
}
