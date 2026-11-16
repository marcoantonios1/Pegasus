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

`cmd/pegasus` is the long-running Pegasus process. It keeps the pipeline
machinery running (config validation, schema check, restart recovery, the
live-window flusher) and serves health endpoints. With
`PEGASUS_WHATSAPP_ENABLED=true` it also ingests live WhatsApp messages (see
"Live WhatsApp" below). The default is off, which runs it with no live
source. Under compose:

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
does a non-fatal reachability probe. It builds the pipeline, **bootstraps
the self identity** (see "Self identity"), takes the recovery snapshot
**before** any intake starts, starts the live-window
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
built on whatsmeow's `Download`). With WhatsApp enabled, the paired
account must be one of the configured WhatsApp self identities (see "Self
identity"); it is never added implicitly.

## Self identity

Marco is one entity in the graph: the single `is_self = true` entity. All
his messages, on every platform, are stored with that entity as sender, and
facts the model extracts about him land on it. That matters beyond
bookkeeping: **corrections are only honoured when the message's sender is
the self entity**, so a wrong or missing identity silently disables them.

Configure it with four settings (see "Configuration"):

```
PEGASUS_SELF_NAME=Marco
PEGASUS_SELF_ALIASES=Marco Antonios
PEGASUS_SELF_WHATSAPP_IDS=+961 70 123 456;Marco
PEGASUS_SELF_INSTAGRAM_NAMES=Marco Antonios
```

- **`PEGASUS_SELF_NAME`** becomes the self entity's `canonical_name`. Every
  extraction window labels his messages with it, on live, import and
  recovery paths alike, so the model always sees "Marco".
- **Aliases**, the self name and every identity string are also matched
  (case-insensitively) against extracted subjects and objects. So "Marco",
  "Marco Antonios" or his JID never create a second entity for him.
- **Identities are per platform.** A WhatsApp identity never matches an
  Instagram sender, even with the same string.
  - **WhatsApp:** his phone number or JID (`+961 70 123 456` is expanded to
    `96170123456@s.whatsapp.net`, device suffixes are stripped). Add the
    label `.txt` exports use for his own messages. Checked against two real
    exports, that label is **his WhatsApp profile name, `Marco`**; it is not
    `You` or a number.
  - **Instagram:** the display names his messages carry (`sender_name`),
    one entry per name he has ever used. Instagram exports have no stable
    sender ID; see [docs/identity.md](docs/identity.md) for why, and for
    the risks.
- In the environment, lists are `;`-separated (commas appear in display
  names). In the JSON config file they are arrays.
- Names shorter than 3 characters and empty entries are rejected.

**Startup bootstrap.** After the migration check, and before recovery or
any live source starts, the service loads the existing self entity, or
creates it named `PEGASUS_SELF_NAME`. It refuses to start, with exit code 2,
a message naming the entities involved, and no auto-repair, when:

- the existing self entity has a different name;
- a non-self entity already holds the self name, an alias or a configured
  identity. Typically earlier runs attributed Marco's messages to a contact,
  so two entities would claim to be him. Merging is a separate tool; for a
  disposable dev database, wipe it.

It also refuses a live WhatsApp session paired to an account that isn't in
`PEGASUS_SELF_WHATSAPP_IDS`.

A sender that is **not** a configured identity is never attributed to
Marco, even if its name matches his. If such a name collides with the self
entity, the message is refused with an error rather than misattributed.

The `cmd/` tools that build an API (`phase0_dryrun`, `backfill_embeddings`)
read the same settings (environment or `PEGASUS_CONFIG_FILE`) and fail fast
without them. `pegasus pair` doesn't need them.

## Live WhatsApp

The service connects as a **linked device** of Marco's WhatsApp account,
through [whatsmeow](https://github.com/tulir/whatsmeow).

**whatsmeow is an unofficial client.** WhatsApp does not support
third-party clients, and account restrictions or bans are possible. The
service only reads messages; it never sends any.

### Pairing

Pairing happens once, interactively. The long-running service never pairs.
With WhatsApp enabled and no paired session, it exits with code 2 and tells
you to run the pair command.

1. **Stop the service.** Two clients on one session replace each other
   (WhatsApp sends `StreamReplaced`, which the service treats as fatal):

   ```
   docker compose stop pegasus
   ```

2. Run the pair command. It prints a QR code in the terminal:

   ```
   docker compose run --rm pegasus pair
   ```

   On the phone: **Settings → Linked devices → Link a device**, then scan the
   code. Codes refresh every few seconds. If none is scanned in time, the
   command exits non-zero ("pairing timed out"); just run it again.
3. After the scan, **keep WhatsApp open on the phone and let the command
   finish.** It stays connected for 30 seconds to 3 minutes while WhatsApp
   reconnects the new device and sends its initial sync (which is
   acknowledged, not imported). Disconnecting earlier leaves the link
   half-finished, and the phone logs the device out about a minute later.
   It then prints the paired account with the number redacted to its last
   digits (e.g. `Paired as …4567@s.whatsapp.net`) and exits 0. Running it
   again when already paired just says so.
4. Make sure the paired number is in `PEGASUS_SELF_WHATSAPP_IDS` (the
   service refuses to start otherwise), set `PEGASUS_WHATSAPP_ENABLED=true`
   in `.env`, and start the service:

   ```
   docker compose up -d pegasus
   ```

On the host, without docker: `go run ./cmd/pegasus pair`. This uses
`PEGASUS_WHATSAPP_SESSION_PATH`, default `~/.pegasus/whatsmeow/session.db`.

### The session file is a credential

Anyone holding the session database can read and send messages as the
account. So:

- It lives **outside the repo**. Under compose it is on the dedicated
  `whatsmeow_session` named volume, at `/var/lib/pegasus/whatsmeow/session.db`.
  On the host it defaults to `~/.pegasus/whatsmeow/session.db`. The service
  refuses a path inside the git repository.
- It is created `0600` in a `0700` directory, before SQLite opens it. The
  `-wal`/`-shm` companions get the same mode.
- `*.db`, `*.db-wal`, `*.db-shm`, `*.sqlite`, `*.sqlite3` and `whatsmeow*`
  are in `.gitignore` and `.dockerignore`, and the filename hook/CI check
  blocks committing them.
- It is **deliberately not included in `./backups`**. The backup service only
  dumps Postgres. Losing the session costs a re-pair, not data.
- It uses the pure-Go SQLite driver (`modernc.org/sqlite`), so the image is
  still built with `CGO_ENABLED=0`.

**Re-pairing after a logout** (`logged_out`): the service removes the dead
device from the session store itself, so just stop the service and run the
pair command again. (If that removal fails, it logs so; then delete the
session as below.)

**Re-pairing voluntarily** (the device is still linked): unlink it on the
phone (Settings → Linked devices), stop the service, delete the session
file, then run the pair command again.
Under compose the image has no shell, so remove the whole session volume:
`docker compose rm -sf pegasus && docker volume rm pegasus_whatsmeow_session`
(the volume name is prefixed with the compose project name).

**Linked devices expire:** WhatsApp logs out linked devices when the phone
stays offline for too long (about two weeks). The service then reports
`logged_out` (see below), and you must re-pair.

### What live ingestion covers

Forwarded to the pipeline (`api.ProcessLiveMessage`):

- **1:1 chats only**: text messages in both directions, plus images and
  videos (stored with their caption and media reference, exactly as the
  adapter produces them).
- Your own messages, sent from any of your devices, are attributed to the
  self entity.

Deliberately dropped. Each drop is counted by reason in `/readyz` and the
periodic summary:

| Reason | What |
|---|---|
| `group` | Group chats (`@g.us`) |
| `status_broadcast` | Status updates |
| `broadcast_list` | Other broadcast lists |
| `newsletter` | Channels/newsletters |
| `unsupported_type` | Reactions, protocol/revoke messages, stickers, polls, locations, contacts, … |
| `edit` | Message edits (applying an edit is out of scope) |
| `voice_unsupported_live` | Voice notes. Live transcription needs a `Download`-based `AudioFetcher` (follow-up), and forwarding them now would leave permanently unprocessed rows. |
| `history_sync` | History-sync payloads (history import is out of scope) |
| `after_stop` | Events arriving after the source stopped |

Every other non-message event is counted by type name under `other_events`.

Identity: device suffixes are stripped (`123:7@s.whatsapp.net` becomes
`123@s.whatsapp.net`). `@lid` identities are mapped to phone-number JIDs when
the message or the session store knows the mapping. Otherwise the LID is
kept and counted as `lid_unmapped`.

Delivery: each message goes to `ProcessLiveMessage` synchronously, bounded by
`PEGASUS_LIVE_SINK_TIMEOUT`, with up to 3 attempts. That is safe because
ingestion is idempotent on the WhatsApp message ID. A redelivery of an
already-stored message is counted as `duplicates`, not `forwarded`; this is
normal shortly after linking, when whatsmeow re-requests messages it could
not decrypt yet. There is no in-process queue, because WhatsApp has already acknowledged the message: it is durable
once stored. A message that still fails is counted as `sink_failed` and
logged by message ID only. Logs never contain message text, and JIDs are
redacted to their last 4 digits. That includes whatsmeow's own log lines:
any run of 8 or more digits (phone numbers, LIDs) is cut to its last 4.

### Connection health

- **Transient disconnects** never affect readiness. whatsmeow reconnects by
  itself. If it hasn't within 30s, the service retries with capped
  exponential backoff plus jitter (`PEGASUS_WHATSAPP_RECONNECT_BACKOFF_*`),
  indefinitely, and warns every 10 minutes while still disconnected.
- **Fatal states** stop all retrying and log one `ERROR` naming the state and
  the fix. The process stays up: open windows still flush and recovery
  completes. But `/readyz` returns `503` with the reason, so the container
  shows unhealthy. Restart the service once fixed.

  | State | Fix |
  |---|---|
  | `logged_out` | Re-pair (device unlinked, revoked, or phone offline too long) |
  | `stream_replaced` | Another client is using the same session (e.g. `pair` ran while the service was up). Stop it, restart the service |
  | `client_outdated` | Update `go.mau.fi/whatsmeow`, rebuild |
  | `temporary_ban` | Wait for the ban to expire, restart |
  | `connect_failure` | Authentication-type refusal; re-pair if it persists |

`/readyz`'s `whatsapp` check carries the state, `connected_since`, the
reconnect count, and the forward/drop counters (counts only, never content).
The same summary is logged every 15 minutes and at shutdown.

Note: compose's `restart: unless-stopped` means an unpaired service (exit 2)
is restarted with Docker's growing backoff. The log says what to do; pair,
or set `PEGASUS_WHATSAPP_ENABLED=false`.

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
| `PEGASUS_SELF_NAME` | *(required)* | Marco's name: the self entity's `canonical_name` and his speaker label in every extraction window. Must match an existing self entity. |
| `PEGASUS_SELF_ALIASES` | *(none)* | Other names the model may use for him, `;`-separated (JSON array in the file). |
| `PEGASUS_SELF_WHATSAPP_IDS` | *(none)* | His WhatsApp phone number/JID plus his `.txt` export label (`Marco`), `;`-separated. At least one WhatsApp or Instagram identity is required. |
| `PEGASUS_SELF_INSTAGRAM_NAMES` | *(none)* | His Instagram display names (every name he has used), `;`-separated. See docs/identity.md. |
| `PEGASUS_LIVE_WINDOW_FLUSH_AFTER` | `5m0s` | Flush an open live window after this long with no new message. |
| `PEGASUS_LIVE_WINDOW_FLUSH_POLL_INTERVAL` | `30s` | How often the idle flusher checks. |
| `PEGASUS_RECOVER_ON_START` | `true` | Re-extract all `processed=false` messages with text in the background at startup (see the caveat above). |
| `PEGASUS_HTTP_ADDR` | `127.0.0.1:8095` | Health server listen address (compose: `0.0.0.0:8095` in the container, published on `127.0.0.1:8095`). |
| `PEGASUS_LIVE_SINK_TIMEOUT` | `5m0s` | Per-message bound on `ProcessLiveMessage` for live sources. A message that times out stays `processed=false` and is recovered next start. |
| `PEGASUS_WHATSAPP_ENABLED` | `false` | Run the live WhatsApp source. Needs a paired session (see "Live WhatsApp"); otherwise exits 2. |
| `PEGASUS_WHATSAPP_SESSION_PATH` | `~/.pegasus/whatsmeow/session.db` | whatsmeow session database (a credential; must be outside the repo). Compose: `/var/lib/pegasus/whatsmeow/session.db` on the `whatsmeow_session` volume. |
| `PEGASUS_WHATSAPP_RECONNECT_BACKOFF_BASE` | `2s` | First supervised-reconnect wait, doubling per attempt with ±20% jitter. |
| `PEGASUS_WHATSAPP_RECONNECT_BACKOFF_MAX` | `5m0s` | Cap on the reconnect backoff. Retries continue indefinitely. |
| `PEGASUS_WHATSAPP_LOG_LEVEL` | `INFO` | whatsmeow's log level (`DEBUG`, `INFO`, `WARN`, `ERROR`). Avoid `DEBUG`: it can print message payloads. |
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
