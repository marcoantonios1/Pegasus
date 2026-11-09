// relationship_spotcheck is a throwaway manual-testing tool for
// reflection.RecomputeRelationshipStats (proposal §10) — it does NOT
// belong to any pipeline and nothing else calls it. Its only job is
// letting Marco hand-feed a handful of real message timestamps for one
// relationship he knows well into a local dev database, run the real
// stat computation against them, and eyeball whether frequency/
// reply_speed/closeness land where his own judgment says they should.
// That spot-check is the actual acceptance criterion §10 asks for and
// that automated tests can't stand in for (see relationship_stats_test.go's
// TestRecomputeRelationshipStats_OrdersContactsSensibly doc comment).
//
// Only sender + timestamp matter to the scoring — message text is never
// read or stored, so there's no reason to put real text content in the
// input file at all.
//
// Usage:
//
//	docker compose up -d postgres migrate   # local dev DB, once
//	go run ./cmd/relationship_spotcheck --messages data/some_contact.json
//
// Input file (see cmd/relationship_spotcheck/example_messages.json for a
// template with placeholder data): a JSON object naming the contact, an
// optional "now" (defaults to real time.Now()), and the message list —
// each entry just "sender" ("self" or "contact") and "at" (RFC3339
// timestamp). Save your real one under data/ (already gitignored) rather
// than inside cmd/ — don't commit real message timestamps.
//
// Self/contact entities are resolved by name (EntityStore.
// GetOrCreateByCanonicalName), not created fresh every run: entities.
// canonical_name has a case-insensitive UNIQUE index now (the entity-
// resolution race fix, migration 000008), so a second run against the
// same self_name/contact_name would otherwise hard-fail on that
// constraint instead of silently creating a second row the way it used
// to. A practical side effect worth knowing, not just a mechanical
// fix: running this tool twice against the SAME contact_name now
// resolves to the SAME entity and therefore the SAME relationship —
// the second run's messages add to the first run's, rather than each
// run scoring an isolated, single-use entity. For a one-off spot-check
// of a contact you've never spot-checked before, this makes no
// difference; for re-running after editing the input file, it means
// the earlier messages are still there too.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcoantonios1/Pegasus/internal/memory"
	"github.com/marcoantonios1/Pegasus/internal/reflection"
)

type inputMessage struct {
	Sender string `json:"sender"` // "self" or "contact"
	At     string `json:"at"`     // RFC3339
}

type input struct {
	SelfName    string         `json:"self_name"`
	ContactName string         `json:"contact_name"`
	Now         string         `json:"now"` // optional, RFC3339; defaults to time.Now()
	Messages    []inputMessage `json:"messages"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	messagesPath := flag.String("messages", "", "path to the input JSON file (see example_messages.json)")
	dsn := flag.String("db", "", "Postgres DSN (defaults to $PEGASUS_TEST_DATABASE_URL, then the standard local dev DSN)")
	flag.Parse()

	if *messagesPath == "" {
		return fmt.Errorf("--messages is required")
	}

	raw, err := os.ReadFile(*messagesPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", *messagesPath, err)
	}
	var in input
	if err := json.Unmarshal(raw, &in); err != nil {
		return fmt.Errorf("parse %s: %w", *messagesPath, err)
	}
	if in.SelfName == "" || in.ContactName == "" {
		return fmt.Errorf("self_name and contact_name are both required in the input file")
	}
	if len(in.Messages) == 0 {
		return fmt.Errorf("messages is empty — nothing to spot-check")
	}

	now := time.Now()
	if in.Now != "" {
		now, err = time.Parse(time.RFC3339, in.Now)
		if err != nil {
			return fmt.Errorf("parse now %q: %w", in.Now, err)
		}
	}

	resolvedDSN := *dsn
	if resolvedDSN == "" {
		resolvedDSN = os.Getenv("PEGASUS_TEST_DATABASE_URL")
	}
	if resolvedDSN == "" {
		resolvedDSN = "postgres://pegasus:pegasus@localhost:5432/pegasus?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, resolvedDSN)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", resolvedDSN, err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping %s: %w (is `docker compose up -d postgres migrate` running?)", resolvedDSN, err)
	}

	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	runCtx := context.Background()

	self, _, err := entityStore.GetOrCreateByCanonicalName(runCtx, in.SelfName, "person")
	if err != nil {
		return fmt.Errorf("resolve self entity: %w", err)
	}
	contact, _, err := entityStore.GetOrCreateByCanonicalName(runCtx, in.ContactName, "person")
	if err != nil {
		return fmt.Errorf("resolve contact entity: %w", err)
	}

	conversationID := uuid.New()
	var selfCount, contactCount int
	for i, m := range in.Messages {
		at, err := time.Parse(time.RFC3339, m.At)
		if err != nil {
			return fmt.Errorf("message %d: parse timestamp %q: %w", i, m.At, err)
		}

		var senderID uuid.UUID
		switch m.Sender {
		case "self":
			senderID = self.ID
			selfCount++
		case "contact":
			senderID = contact.ID
			contactCount++
		default:
			return fmt.Errorf(`message %d: sender must be "self" or "contact", got %q`, i, m.Sender)
		}

		msg := &memory.Message{
			ConversationID: conversationID,
			SenderID:       senderID,
			MediaType:      "text",
			Timestamp:      at,
		}
		if err := msgStore.Create(runCtx, msg); err != nil {
			return fmt.Errorf("insert message %d: %w", i, err)
		}
	}

	stats, err := reflection.RecomputeRelationshipStats(runCtx, statsStore, edgeStore, msgStore, self.ID, contact.ID, now)
	if err != nil {
		return fmt.Errorf("RecomputeRelationshipStats: %w", err)
	}

	since := now.Add(-reflection.DefaultRelationshipStatsWindow)
	fmt.Printf("Self:    %s (id=%s)\n", in.SelfName, self.ID)
	fmt.Printf("Contact: %s (id=%s)\n", in.ContactName, contact.ID)
	fmt.Printf("Messages inserted: %d (contact: %d, self: %d)\n", len(in.Messages), contactCount, selfCount)
	fmt.Printf("now=%s  window since=%s\n\n", now.Format(time.RFC3339), since.Format(time.RFC3339))

	fmt.Println("--- Computed relationship stats ---")
	fmt.Printf("Frequency:   %.3f   (1.0 = about as active as your average contact this window)\n", stats.Frequency)
	if stats.ReplySpeed < 0 {
		fmt.Println("ReplySpeed:  no reply pairs observed in the window")
	} else {
		fmt.Printf("ReplySpeed:  %.1fs (%s)\n", stats.ReplySpeed, time.Duration(stats.ReplySpeed*float64(time.Second)).Round(time.Second))
	}
	fmt.Printf("HumorLevel:  %.3f   (always 0 for now — no humor classifier upstream yet)\n", stats.HumorLevel)
	fmt.Printf("Closeness:   %.3f\n", stats.Closeness)

	return nil
}
