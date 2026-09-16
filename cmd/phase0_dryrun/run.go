package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func runRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	exportPath := fs.String("export", "", "path to one real WhatsApp export .txt file — Marco's real data, not synthesized (see requirement 1)")
	pegasusDSN := fs.String("pegasus-db", envOr("PEGASUS_DATABASE_URL", "postgres://pegasus:pegasus@localhost:5432/pegasus?sslmode=disable"), "Pegasus Postgres DSN")
	costguardURL := fs.String("costguard-url", envOr("COSTGUARD_URL", "http://localhost:8080"), "Costguard base URL")
	costguardDSN := fs.String("costguard-db", envOr("COSTGUARD_DATABASE_URL", "postgres://costguard:costguard@localhost:5432/costguard?sslmode=disable"), "Costguard's own Postgres DSN (for usage_records — see NOTES.md for why this isn't fetched over HTTP)")
	agent := fs.String("agent", "pegasus", "Costguard X-Costguard-Agent tag this run's requests carry, and the usage_records.agent filter")
	out := fs.String("out", "", "output JSON path (RunResult)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *exportPath == "" || *out == "" {
		return fmt.Errorf("--export and --out are required")
	}

	ctx := context.Background()

	pegasusCfg, err := pgxpool.ParseConfig(*pegasusDSN)
	if err != nil {
		return fmt.Errorf("parse --pegasus-db: %w", err)
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

	var rejectedLines []string
	extractor := extraction.NewExtractor(client)
	extractor.Logger = func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		rejectedLines = append(rejectedLines, line)
		log.Println(line)
	}

	triager := extraction.NewTriager(client)

	a := api.New(
		memory.NewEdgeStore(pegasusPool),
		memory.NewMessageStore(pegasusPool),
		memory.NewEntityStore(pegasusPool),
		memory.NewEmbeddingStore(pegasusPool),
		memory.NewRelationshipStatsStore(pegasusPool),
		client,
	).WithPipeline(api.PipelineDeps{
		Triager:   triager,
		Extractor: extractor,
		Logger:    log.Printf,
	})

	// Margin around the run's actual [start, end) so Costguard's own
	// request timestamps (a different process, potentially a different
	// container with its own clock) aren't excluded by a few seconds of
	// skew — real-time-boxing a live run for usage attribution is
	// inherently approximate; see NOTES.md.
	const clockSkewMargin = 30 * time.Second

	started := time.Now()
	result, importErr := a.ImportWhatsApp(ctx, *exportPath)
	finished := time.Now()
	// importErr is deliberately not returned immediately — even a failed
	// or partial run's data (whatever ImportWhatsApp got through before
	// erroring) is worth capturing and reporting on, same reasoning as
	// ImportResult being returned alongside an error rather than nil.
	if result == nil {
		result = &api.ImportResult{}
	}

	usageFrom := started.Add(-clockSkewMargin)
	usageTo := finished.Add(clockSkewMargin)
	usage, usageErr := queryCostguardUsage(ctx, *costguardDSN, *agent, usageFrom, usageTo)
	if usageErr != nil {
		log.Printf("WARNING: could not load Costguard usage data: %v — RunResult.Usage will be empty; cost numbers in the report will be zero, not accurate", usageErr)
	}

	run := RunResult{
		ExportPath:             *exportPath,
		StartedAt:              started,
		FinishedAt:             finished,
		Import:                 result,
		RejectedTripleLogLines: rejectedLines,
		Usage:                  usage,
		UsageWindowStart:       usageFrom,
		UsageWindowEnd:         usageTo,
	}

	data, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run result: %w", err)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", *out, err)
	}

	fmt.Printf("wrote %s\n", *out)
	if importErr != nil {
		return fmt.Errorf("ImportWhatsApp returned an error (partial result still written to %s): %w", *out, importErr)
	}
	return nil
}

// queryCostguardUsage reads Costguard's own usage_records table directly
// (read-only), grouped by model and request path, for requests tagged
// with agent within [from, to).
//
// Why direct SQL instead of Costguard's HTTP admin API
// (/usage/summary, /usage/teams, /usage/projects, /usage/agents — all
// registered in costguard/internal/server/admin/routes.go): none of those
// endpoints break spend down by BOTH agent AND model in one call.
// /usage/agents gives total spend per agent with no model dimension;
// there's no /usage/models route at all, even though the underlying
// usage.Store interface has GetSpendByModel — it's just never wired to a
// route. This tool needs "how much did triage cost vs. extraction vs.
// embeddings" (three different models under the same "pegasus" agent
// tag), which the admin API genuinely cannot answer today. Costguard's
// own usage.Record schema (internal/usage/record.go in the Costguard
// repo) is the source of truth this query matches column-for-column.
func queryCostguardUsage(ctx context.Context, dsn, agent string, from, to time.Time) ([]UsageRow, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to Costguard db: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping Costguard db: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT model, path, count(*),
			COALESCE(sum(prompt_tokens), 0), COALESCE(sum(completion_tokens), 0),
			COALESCE(sum(total_tokens), 0), COALESCE(sum(estimated_cost_usd), 0)
		FROM usage_records
		WHERE agent = $1 AND timestamp_utc >= $2 AND timestamp_utc < $3
		GROUP BY model, path
		ORDER BY model, path
	`, agent, from, to)
	if err != nil {
		return nil, fmt.Errorf("query usage_records: %w", err)
	}
	defer rows.Close()

	var out []UsageRow
	for rows.Next() {
		var r UsageRow
		if err := rows.Scan(&r.Model, &r.Path, &r.RequestCount, &r.PromptTokens, &r.CompletionTokens, &r.TotalTokens, &r.EstimatedCostUSD); err != nil {
			return nil, fmt.Errorf("scan usage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
