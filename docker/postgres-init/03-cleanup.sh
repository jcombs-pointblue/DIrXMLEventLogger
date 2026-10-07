#!/bin/bash
# Installs the size-based cleanup and schedules it with pg_cron. Runs once, on first start.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -f /opt/eventlogger/purge_events_by_size.sql

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v schedule="$PURGE_SCHEDULE" -v max_mb="$EVENT_MAX_SIZE_MB" <<'EOSQL'
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule('purge-events-by-size', :'schedule',
                     format('SELECT purge_events_by_size(%s, 10000, 100)', :'max_mb'::int));
EOSQL
