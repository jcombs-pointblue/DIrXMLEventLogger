#!/bin/bash
# Creates the driver (write) and web UI (read-only) accounts on first start.
# Runs once, after 01-schema.sql, when the data volume is empty.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v db="$POSTGRES_DB" \
    -v reader_user="$READER_USER" -v reader_pw="$READER_PASSWORD" \
    -v writer_user="$WRITER_USER" -v writer_pw="$WRITER_PASSWORD" <<'EOSQL'
CREATE ROLE :"writer_user" LOGIN PASSWORD :'writer_pw';
GRANT CONNECT ON DATABASE :"db" TO :"writer_user";
GRANT USAGE ON SCHEMA public TO :"writer_user";
GRANT SELECT, INSERT ON dxmlevent TO :"writer_user";
GRANT USAGE ON SEQUENCE dxmlevent_id_seq TO :"writer_user";

CREATE ROLE :"reader_user" LOGIN PASSWORD :'reader_pw';
GRANT CONNECT ON DATABASE :"db" TO :"reader_user";
GRANT USAGE ON SCHEMA public TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"reader_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"reader_user";
EOSQL
