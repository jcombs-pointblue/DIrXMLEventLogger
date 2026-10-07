-- Upgrades a dxmlevent table created before release 2.0.0 to schema version 2:
-- the id primary key, PolicyLogger columns and the schemaversion column.
-- Safe to run more than once. Run as the table owner:
--   psql -d idmEvent -f "sql/MIGRATE 2 policy columns.sql"
-- Adding the id column rewrites the table; on a large table, run it in a quiet period.

BEGIN;

-- Surrogate key: the same eventid can now appear once per policy and stage
ALTER TABLE dxmlevent ADD COLUMN IF NOT EXISTS "id" bigserial;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
        WHERE c.conrelid = 'dxmlevent'::regclass AND c.contype = 'p' AND a.attname = 'eventid'
    ) THEN
        EXECUTE (SELECT 'ALTER TABLE dxmlevent DROP CONSTRAINT ' || quote_ident(conname)
                 FROM pg_constraint WHERE conrelid = 'dxmlevent'::regclass AND contype = 'p');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'dxmlevent'::regclass AND contype = 'p') THEN
        ALTER TABLE dxmlevent ADD PRIMARY KEY ("id");
    END IF;
END $$;

-- PolicyLogger columns (NULL for rows written by the EventLogger driver)
ALTER TABLE dxmlevent ADD COLUMN IF NOT EXISTS "channel" character varying
    CHECK ("channel" IN ('subscriber', 'publisher'));
ALTER TABLE dxmlevent ADD COLUMN IF NOT EXISTS "policy" character varying;
ALTER TABLE dxmlevent ADD COLUMN IF NOT EXISTS "stage" character varying
    CHECK ("stage" IN ('input', 'output'));

-- Shape of eventjson. Existing rows, and rows from older driver jars, get 1;
-- release 2.0.0 and later write 2 (the "schemaVersion" key in the JSON).
ALTER TABLE dxmlevent ADD COLUMN IF NOT EXISTS "schemaversion" smallint NOT NULL DEFAULT 1;

-- Older PolicyLogger rows carried channel and policy only inside the JSON
UPDATE dxmlevent
SET "policy"  = eventjson ->> 'logged-by-policy',
    "channel" = CASE lower(eventjson ->> 'logged-channel')
                    WHEN 'sub' THEN 'subscriber' WHEN 'subscriber' THEN 'subscriber'
                    WHEN 'pub' THEN 'publisher'  WHEN 'publisher'  THEN 'publisher'
                END,
    "stage"   = 'input'
WHERE "policy" IS NULL AND eventjson ? 'logged-by-policy';

CREATE UNIQUE INDEX IF NOT EXISTS ux_dxmlevent_driver_eventid ON dxmlevent ("eventid") WHERE "policy" IS NULL;
CREATE INDEX IF NOT EXISTS idx_eventid ON dxmlevent ("eventid");
CREATE INDEX IF NOT EXISTS idx_srcdn_cachedtime ON dxmlevent ("srcdn", "cachedtime");
CREATE INDEX IF NOT EXISTS idx_cachedtime ON dxmlevent ("cachedtime");
CREATE INDEX IF NOT EXISTS idx_srcdriver_policy ON dxmlevent ("srcdriver", "policy");
CREATE INDEX IF NOT EXISTS idx_srcdn_prefix ON dxmlevent ("srcdn" text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_srcdn_reverse_pattern ON dxmlevent (REVERSE("srcdn") text_pattern_ops);
-- Covered by idx_srcdriver_policy
DROP INDEX IF EXISTS idx_srcdriver;
-- Replaced by idx_srcdn_reverse_pattern: without text_pattern_ops, LIKE cannot use it
-- under a non-C collation such as en_US.UTF-8
DROP INDEX IF EXISTS idx_srcdn_reverse;

COMMIT;
