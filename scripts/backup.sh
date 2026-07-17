#!/bin/sh
# Daily pg_dump of the pegasus database. Connection settings come from the
# standard libpq env vars (PGHOST/PGUSER/PGPASSWORD/PGDATABASE) so no
# credentials are passed on the command line. Runs one dump immediately on
# start, then loops on a fixed interval — this container has no cron/systemd
# available, so the schedule is just a sleep loop.
set -eu

BACKUP_DIR="${BACKUP_DIR:-/backups}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-86400}"

mkdir -p "$BACKUP_DIR"

while true; do
    timestamp=$(date +%Y-%m-%d)
    dump_file="${BACKUP_DIR}/pegasus_${timestamp}.sql.gz"

    echo "[backup] $(date -Iseconds) dumping ${PGDATABASE} -> ${dump_file}"
    pg_dump | gzip > "${dump_file}.tmp"
    mv "${dump_file}.tmp" "${dump_file}"

    # Retention: keep the last N days of dumps, delete anything older.
    find "$BACKUP_DIR" -name 'pegasus_*.sql.gz' -mtime "+${RETENTION_DAYS}" -delete

    echo "[backup] sleeping ${INTERVAL_SECONDS}s until next run"
    sleep "$INTERVAL_SECONDS"
done
