// backfill_embeddings is a one-shot batch utility that embeds every
// message written before embedding generation was wired into the write
// pipeline (internal/api/pipeline.go's embedMessages, and the gap
// cmd/phase0_dryrun/NOTES.md originally flagged: "backfilling old
// messages' missing embeddings ... a natural, likely-immediate follow-up
// issue"). It does not reimplement any embedding logic — it queries
// memory.MessageStore.ListWithoutEmbeddings for messages with no
// embeddings row, then calls api.API.BackfillEmbeddings (a thin exported
// wrapper over the exact same embedMessages the live/historical write
// pipeline already uses) to embed them, inheriting embedMessages' own
// idempotency (EmbeddingStore.GetByMessageID) and log-and-continue
// failure handling by construction.
// test
//	go run ./cmd/backfill_embeddings [--pegasus-db <dsn>] [--costguard-url <url>] [--agent <name>]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// backfillBatchSize is both the unit BackfillEmbeddings is called with
// and the progress-reporting cadence (a batch's own result is folded into
// the running total, then printed) — one call per batchSize messages
// rather than one call for the entire result set, specifically so a real
// run against a sizable backlog prints periodic progress instead of
// going silent until it's entirely done. Not a flag: the acceptance
// criteria names "every 100" as the expected cadence, not a
// caller-tunable one — matching this codebase's general preference for a
// const over a flag nobody asked to configure.
const backfillBatchSize = 100

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("backfill_embeddings", flag.ExitOnError)
	pegasusDSN := fs.String("pegasus-db", envOr("PEGASUS_DATABASE_URL", "postgres://pegasus:pegasus@localhost:5432/pegasus?sslmode=disable"), "Pegasus Postgres DSN")
	costguardURL := fs.String("costguard-url", envOr("COSTGUARD_URL", "http://localhost:8080"), "Costguard base URL")
	agent := fs.String("agent", envOr("BACKFILL_AGENT", "pegasus"), "Costguard X-Costguard-Agent tag this run's embedding requests carry")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	ctx := context.Background()

	// Same pool-construction pattern as cmd/phase0_dryrun's run.go: the
	// pgvector pgx codec must be registered (AfterConnect) for
	// EmbeddingStore's vector column to round-trip without manual
	// casting — a hard requirement for this tool specifically, since
	// embedding storage is its entire job.
	pegasusCfg, err := pgxpool.ParseConfig(*pegasusDSN)
	if err != nil {
		return fmt.Errorf("parse pegasus DSN: %w", err)
	}
	pegasusCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}
	pegasusPool, err := pgxpool.NewWithConfig(ctx, pegasusCfg)
	if err != nil {
		return fmt.Errorf("connect to Pegasus db: %w", err)
	}
	defer pegasusPool.Close()
	if err := pegasusPool.Ping(ctx); err != nil {
		return fmt.Errorf("ping Pegasus db: %w", err)
	}

	client := extraction.NewCostguardClient(*costguardURL)
	client.Agent = *agent

	messages := memory.NewMessageStore(pegasusPool)

	a := api.New(
		memory.NewEdgeStore(pegasusPool),
		messages,
		memory.NewEntityStore(pegasusPool),
		memory.NewEmbeddingStore(pegasusPool),
		memory.NewRelationshipStatsStore(pegasusPool),
		client,
	).WithPipeline(api.PipelineDeps{
		// Logger wired explicitly (rather than relying on a.logf's own
		// log.Printf fallback) so this is unmistakably the same logging
		// path ImportWhatsApp/ProcessLiveMessage failures already go
		// through, not a separate ad-hoc mechanism — matches
		// cmd/phase0_dryrun's own PipelineDeps.Logger wiring.
		Logger: log.Printf,
	})

	unembedded, err := messages.ListWithoutEmbeddings(ctx)
	if err != nil {
		return fmt.Errorf("list messages without embeddings: %w", err)
	}

	fmt.Printf("found %d message(s) with no embedding\n", len(unembedded))
	if len(unembedded) == 0 {
		return nil
	}

	var total api.EmbedMessagesResult
	for start := 0; start < len(unembedded); start += backfillBatchSize {
		end := start + backfillBatchSize
		if end > len(unembedded) {
			end = len(unembedded)
		}

		ids := make([]uuid.UUID, end-start)
		for i, m := range unembedded[start:end] {
			ids[i] = m.ID
		}

		result, err := a.BackfillEmbeddings(ctx, ids)
		if err != nil {
			return fmt.Errorf("backfill embeddings: %w", err)
		}

		total.Embedded += result.Embedded
		total.AlreadyEmbedded += result.AlreadyEmbedded
		total.SkippedNoText += result.SkippedNoText
		total.Failed += result.Failed

		fmt.Printf("processed %d/%d (embedded=%d already_embedded=%d skipped_no_text=%d failed=%d)\n",
			end, len(unembedded), total.Embedded, total.AlreadyEmbedded, total.SkippedNoText, total.Failed)
	}

	fmt.Printf("done — embedded=%d already_embedded=%d skipped_no_text=%d failed=%d\n",
		total.Embedded, total.AlreadyEmbedded, total.SkippedNoText, total.Failed)
	if total.Failed > 0 {
		fmt.Println("see the log lines above for which message IDs failed and why — each is safely re-runnable (this tool is idempotent), so re-running after addressing the cause will pick up exactly the ones still missing an embedding, nothing more.")
	}

	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
