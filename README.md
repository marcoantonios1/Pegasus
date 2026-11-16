# Pegasus

## Local development

```
cp .env.example .env
```

Set a real `POSTGRES_PASSWORD` in `.env` — generate one with:

```
openssl rand -hex 24
```

then lock the file down and start the stack:

```
chmod 600 .env
docker compose up -d postgres migrate
```

`postgres` runs `pgvector/pgvector:pg16` (Postgres 16 with the `vector`
extension preinstalled — required by migrations 000004/000005). `migrate`
applies everything in `db/migrations/` via `golang-migrate`.

Postgres's host port is bound to `127.0.0.1:5432`, not all interfaces, so
the database is only reachable from this machine — Go tools and tests
connect to it via `localhost`, same as before.

**Existing `postgres_data` volume? Read this before setting `POSTGRES_PASSWORD`.**
`POSTGRES_PASSWORD` is only applied when Postgres first initializes its data
directory. If `postgres_data` already exists from a previous run, changing
`POSTGRES_PASSWORD` in `.env` does **not** change the running database's
actual password — Postgres never re-reads it. Two ways to actually change it:

- **Rotate in place, keeping data (recommended):**

  ```
  docker exec -it pegasus-postgres psql -U pegasus -d pegasus
  ```

  then at the `psql` prompt:

  ```
  \password pegasus
  ```

  It prompts twice and never puts the password in your shell history or
  `psql`'s own history — this works regardless of what the old password
  was, because the official image trusts local-socket connections. Then
  pick up the new value from `.env`:

  ```
  docker compose up -d
  ```

  This only recreates the containers; the `postgres_data` volume and its
  data are untouched. **Do this before relying on the `backup` service** —
  until you do, the `backup` container's `PGPASSWORD` (sourced from the
  same `.env`) won't match the database's actual (old) password and its
  dumps will fail.

- **Start fresh (destroys data):** take a backup first (see "Backups"
  below), then:

  ```
  docker compose down -v
  docker compose up -d
  ```

  `down -v` removes the `postgres_data` volume, so Postgres reinitializes
  with whatever `POSTGRES_PASSWORD` is currently in `.env`.

### Running the DB-backed tests

The DB-backed tests (`internal/memory`, `internal/api`, `internal/reflection`)
connect using `PEGASUS_TEST_DATABASE_URL` if set, else a DSN built from
`POSTGRES_PASSWORD`. Export your `.env` into the shell first:

```
set -a; source .env; set +a
go test ./...
```

If no database is configured, DB-backed tests skip. If one *is* configured
but the password is wrong (e.g. it was rotated and this shell's env is
stale), they fail loudly instead of silently skipping.

## Running the service

`cmd/pegasus` is the long-running Pegasus process. Today it has **no live
message source** (the Whatsmeow client is its own issue), so it keeps the
pipeline machinery running — config validation, schema check, restart
recovery, the live-window flusher, the intake hook — and serves health
endpoints. Under compose:

```
docker compose up -d --build pegasus
curl -s 127.0.0.1:8095/readyz
```

`pegasus` waits for `postgres` (healthy) and `migrate` (completed). It reads
its settings from `.env` through `env_file`, the same file the other services
take `POSTGRES_PASSWORD` from. The only values in `docker-compose.yml`
itself are non-secret, container-specific overrides (DB host `postgres`,
listen address `0.0.0.0:8095`, Costguard URL). The health port is published
on `127.0.0.1:8095` only. `extra_hosts: host.docker.internal:host-gateway`
lets the container reach a Costguard running on the host, which is why
compose defaults `PEGASUS_COSTGUARD_URL` to `http://host.docker.internal:8080`.
Outside docker, the default is `http://localhost:8080`, the same default
`cmd/phase0_dryrun --costguard-url` uses.

Locally, without docker:

```
set -a; source .env; set +a
go run ./cmd/pegasus                 # or: go run ./cmd/pegasus --config pegasus.json
go run ./cmd/pegasus healthcheck     # GET /readyz; exit 0 ready, 1 not ready
```

Exit codes: `0` clean shutdown, `1` runtime failure (including a forced exit
on a second signal or a shutdown that overran its budget), `2` configuration
or usage error. Every config problem is reported at once, without secret
values.

### Health endpoints

- `GET /healthz`: `200` while the process is up.
- `GET /readyz`: `200` when a database ping succeeds within 2s and the
  startup migration check passed, `503` otherwise. The JSON body lists each
  check. Costguard reachability (`GET <costguard>/healthz`) is included as
  `informational` and never affects the status code: Costguard being down
  doesn't stop messages being stored, and failed extraction is recovered
  later.

The container healthcheck runs `/pegasus healthcheck`, so the image needs no
curl.

### Startup

In order: load and validate config, log it with secrets redacted (including
the fixed model names and the transcription threshold), connect to Postgres,
then **check** the schema version. The service never runs migrations. If
`schema_migrations` is missing, dirty, or behind/ahead of the newest
`db/migrations/*.up.sql` compiled into the binary, it exits telling you to
run `docker compose up migrate`. Then it builds the Costguard client and
does a non-fatal reachability probe. It builds the pipeline, takes the
recovery snapshot **before** any intake starts, starts the live-window
flusher and the health server, and starts background recovery. Live
sources start last.

### Shutdown and recovery

On SIGINT/SIGTERM, shutdown runs in this order:

1. Stop intake: the health server, then each live source. Anything a source
   delivers after its `Stop` is rejected and not stored.
2. Wait (bounded) for in-flight messages to finish, then cancel background
   recovery.
3. Cancel the live-window flusher and **wait** for its final flush of every
   open window (itself bounded by 30s).
4. Stop the reflection trigger (not wired yet: no consolidation orchestrator
   exists).
5. Close the database pool.

The whole sequence is bounded by `PEGASUS_SHUTDOWN_TIMEOUT` (default 60s).
Compose's `stop_grace_period` is 90s, so Docker never SIGKILLs the process
mid-flush. A second signal forces an immediate exit. Every message is stored
with `processed=false` **before** windowing. So anything whose extraction
didn't finish (shutdown timeout, forced exit, crash, Costguard down) is
picked up by the next start's recovery.

**Recover-on-start caveat:** recovery (`PEGASUS_RECOVER_ON_START=true`, the
default) treats **every** `processed=false` message that has text as backlog.
The `messages` table can't tell an abandoned live window from a bulk import
that is still running. So don't start (or restart) the service while a bulk
import (`ImportWhatsApp`) is mid-flight against the same database. Wait for
the import to finish, or set `PEGASUS_RECOVER_ON_START=false` for that start.

Not wired yet (logged at startup): the reflection engine (no consolidation
orchestrator yet), and live voice transcription (needs an `AudioFetcher`
from a live source). Until a live source exists, `PEGASUS_SELF_EXTERNAL_IDS`
being empty is harmless, but it is logged as a warning. Set it before enabling
one, or Marco's own messages would be attributed to a contact entity.

## Configuration

Precedence: **environment > optional JSON config file > default**. The file
is given with `--config` or `PEGASUS_CONFIG_FILE`. Its keys are the setting
names lower-cased without the `PEGASUS_` prefix (e.g. `{"db_host": "…",
"recover_on_start": false}`). Unknown keys are an error. Secrets
(`POSTGRES_PASSWORD`) come **only** from the environment. Durations use Go
syntax (`30s`, `5m`, `1m0s`). Every setting is also listed, commented out at
its default, in `.env.example`.

| Env var | Default | Meaning |
|---|---|---|
| `PEGASUS_CONFIG_FILE` | *(none)* | Optional JSON config file path (`--config` overrides). |
| `PEGASUS_DB_HOST` | `localhost` | Postgres host (compose sets `postgres`). |
| `PEGASUS_DB_PORT` | `5432` | Postgres port. |
| `PEGASUS_DB_NAME` | `pegasus` | Database name. |
| `PEGASUS_DB_USER` | `pegasus` | Database user. |
| `POSTGRES_PASSWORD` | *(required)* | Database password. **Secret**: environment only, never logged; the same variable the compose postgres service uses. |
| `PEGASUS_DB_SSLMODE` | `disable` | libpq sslmode. |
| `PEGASUS_DB_CONNECT_TIMEOUT` | `5s` | Startup connect + ping timeout. |
| `PEGASUS_COSTGUARD_URL` | `http://localhost:8080` | Costguard base URL (compose default: `http://host.docker.internal:8080`). |
| `PEGASUS_COSTGUARD_AGENT` | `pegasus` | `X-Costguard-Agent` attribution tag. |
| `PEGASUS_COSTGUARD_TIMEOUT` | `1m0s` | Per-request HTTP timeout for Costguard calls. |
| `PEGASUS_COSTGUARD_TRANSCRIPTION_RETRY_BACKOFF` | `30s` | Wait before transcription's one retry after a Costguard 502. |
| `PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD` | `-0.7` | Escalate a local transcription to OpenAI when its mean segment avg_logprob is below this. Must be negative. Arabic always escalates regardless. |
| `PEGASUS_TRANSCRIPTION_OPENAI_PROVIDER_HINT` | `openai_primary` | Costguard provider name for escalated transcription calls. |
| `PEGASUS_SELF_EXTERNAL_IDS` | *(empty, warns)* | Comma-separated sender IDs (e.g. WhatsApp JIDs) that are Marco. |
| `PEGASUS_LIVE_WINDOW_FLUSH_AFTER` | `5m0s` | Flush an open live window after this long with no new message. |
| `PEGASUS_LIVE_WINDOW_FLUSH_POLL_INTERVAL` | `30s` | How often the idle flusher checks. |
| `PEGASUS_RECOVER_ON_START` | `true` | Re-extract all `processed=false` messages with text in the background at startup (see the caveat above). |
| `PEGASUS_HTTP_ADDR` | `127.0.0.1:8095` | Health server listen address (compose: `0.0.0.0:8095` in the container, published on `127.0.0.1:8095`). |
| `PEGASUS_SHUTDOWN_TIMEOUT` | `1m0s` | Overall graceful-shutdown bound. Must exceed the 30s final flush; compose `stop_grace_period` (90s) must exceed it. |

Model names are **fixed constants** in `internal/extraction`, not settings.
They are logged at startup:

| Role | Model | Constant |
|---|---|---|
| Triage (pass 1) | `llama3.2:3b` | `extraction.TriageModel` |
| Extraction (pass 2) | `qwen3-coder:30b` | `extraction.Model` |
| Embeddings | `nomic-embed-text` | `extraction.EmbeddingModel` |
| Transcription | `whisper-1` | `extraction.TranscriptionModel` |

There is no log-level setting: the service uses the stdlib `log` package like
the rest of the codebase. It never logs message text, transcripts, Costguard
response bodies, or the database password.

## Git hooks

Install the pre-commit hook once per clone — it blocks committing any
file matching a known real-personal-data report-output pattern
(`review*.md`, `sample*.md`, `summary*.md`, `run*.json` — see
`scripts/hooks/check-sensitive-filenames.sh` for exactly what's covered
and why):

```
git config core.hooksPath scripts/hooks
```

This is a local, opt-in safety net (skippable with `--no-verify`, and
never installed automatically on a fresh clone) — the actual enforcement
backstop is CI (`.github/workflows/check-sensitive-filenames.yml`), which
runs the same check against every tracked file on every push/PR
regardless of whether this hook is installed. Dry-run/spot-check tooling
(`cmd/phase0_dryrun`, `cmd/whisper_spotcheck`) also refuses at the tool
level to write its own report output inside this repo in the first place
(`internal/reportpath.EnsureOutsideRepo`) — these three layers are
independent, not redundant: the hook and CI catch a file regardless of
how it entered a commit, not just ones these specific tools produced.

## Backups

The `backup` service in `docker-compose.yml` runs a daily `pg_dump` of the
`pegasus` database, gzipped, written to `./backups/` on the host — a bind
mount outside the `postgres_data` named volume, so a backup survives a
failure of the live data volume.

Start it alongside the rest of the stack:

```
docker compose up -d backup
```

It dumps once immediately on start, then every 24h (`BACKUP_INTERVAL_SECONDS`,
default `86400`). Files are named `pegasus_YYYY-MM-DD.sql.gz`. Retention is
14 days (`BACKUP_RETENTION_DAYS`) — `scripts/backup.sh` deletes anything
older via `find ./backups -name 'pegasus_*.sql.gz' -mtime +14 -delete` after
each run.

The dump command itself, if you want to run it by hand:

```
docker exec pegasus-postgres pg_dump -U pegasus pegasus | gzip > backups/pegasus_$(date +%Y-%m-%d).sql.gz
```

### Restoring a backup

Restore into a throwaway container first to verify the dump before trusting
it against real data. `${POSTGRES_PASSWORD}` below just needs to be set to
*some* value for this disposable container — export it from `.env`
(`set -a; source .env; set +a`) or use a freshly generated throwaway one:

```
docker run -d --name pegasus-restore-test \
  -e POSTGRES_DB=pegasus -e POSTGRES_USER=pegasus -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
  pgvector/pgvector:pg16

gunzip -c backups/pegasus_2026-07-17.sql.gz | docker exec -i pegasus-restore-test \
  psql -U pegasus -d pegasus -v ON_ERROR_STOP=1
```

**Verified 2026-07-17:** seeded 3 entities / 2 edges / 2 messages / 2
embeddings, ran the backup service, restored the resulting
`pegasus_2026-07-17.sql.gz` into a throwaway `pgvector/pgvector:pg16`
container, and confirmed row counts matched the source exactly:

| table      | source | restored |
|------------|--------|----------|
| entities   | 3      | 3        |
| edges      | 2      | 2        |
| messages   | 2      | 2        |
| embeddings | 2      | 2        |

## Encryption at rest

Postgres does not encrypt its data files on disk by default. Given the
schema stores raw conversation content (`messages.raw_text`,
`messages.transcript`), encryption at rest should happen at the **host
disk/volume level** (LUKS on Linux, FileVault on macOS) rather than via
column-level encryption (pgcrypto): disk encryption protects everything
uniformly with zero query or schema changes, whereas pgcrypto would mean
decrypting on every read and reworking the `internal/memory` store layer.

This is a deployment-host concern, not something this repo can turn on —
there's no code or migration that encrypts an already-provisioned disk.

**Before deploying to any host that will run this docker-compose stack,
confirm the disk/volume backing the `postgres_data` volume is encrypted:**

- **Linux (LUKS):** confirm the filesystem Docker's volume storage
  (`/var/lib/docker/volumes/...` or your configured `data-root`) lives on is
  inside a `cryptsetup`-encrypted partition (`cryptsetup status <device>` /
  `lsblk -o NAME,FSTYPE,MOUNTPOINT` should show a `crypto_LUKS` layer above
  it).
- **macOS (FileVault):** confirm FileVault is enabled (`fdesetup status`).

**Resolved 2026-07-17:** deployment target is Marco's local Mac (not a VPS).
`fdesetup status` confirms FileVault is On. Docker Desktop on macOS runs
containers inside a lightweight Linux VM whose backing disk image is itself
a file on this same FileVault-encrypted boot volume, so `postgres_data`'s
on-disk bytes are covered without any extra setup. If the deployment target
ever moves to a Linux VPS, this needs to be re-checked there (LUKS is not
on by default on most VPS images) — the guidance above still applies.
