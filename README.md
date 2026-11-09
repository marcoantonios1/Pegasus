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
