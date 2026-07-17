# Pegasus

## Local development

```
docker compose up -d postgres migrate
```

`postgres` runs `pgvector/pgvector:pg16` (Postgres 16 with the `vector`
extension preinstalled — required by migrations 000004/000005). `migrate`
applies everything in `db/migrations/` via `golang-migrate`.

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
it against real data:

```
docker run -d --name pegasus-restore-test \
  -e POSTGRES_DB=pegasus -e POSTGRES_USER=pegasus -e POSTGRES_PASSWORD=pegasus \
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

**Open question:** this depends on which machine actually runs the stack —
a personal VPS vs. a local Mac have different answers here (VPS: needs LUKS
set up explicitly at provision time; a Mac with FileVault already on: this
is already satisfied, just confirm). Flagging rather than assuming — Marco
to confirm the deployment target.
