BEGIN;

CREATE TABLE IF NOT EXISTS dxmlevent (
	"id" bigserial PRIMARY KEY,
	"eventid" character varying NOT NULL,
	"classname" character varying NOT NULL,
	"srcdn" character varying,
	"srcentryid" character varying,
	"eventtype" character varying NOT NULL,
	"eventjson" jsonb NOT NULL,
	"xmlevent" text,
	"cachedtime" timestamp with time zone NOT NULL,
	"srcdriver" character varying,
	-- PolicyLogger rows only; NULL for rows written by the EventLogger driver itself
	"channel" character varying CHECK ("channel" IN ('subscriber', 'publisher')),
	"policy" character varying,
	"stage" character varying CHECK ("stage" IN ('input', 'output')),
	-- Shape of eventjson: 1 = written before schemaVersion existed, 2 = current (see docs/store.md)
	"schemaversion" smallint NOT NULL DEFAULT 1
);
-- One driver row per engine event; PolicyLogger may log the same event at many policies
CREATE UNIQUE INDEX IF NOT EXISTS ux_dxmlevent_driver_eventid ON dxmlevent ("eventid") WHERE "policy" IS NULL;
CREATE INDEX IF NOT EXISTS idx_eventid ON dxmlevent ("eventid");
-- Subtree queries: reverse(srcdn) LIKE reverse('%...')
CREATE INDEX IF NOT EXISTS idx_srcdn_reverse ON dxmlevent (REVERSE("srcdn"));
-- Timeline and event detail: srcdn = ? ORDER BY cachedtime
CREATE INDEX IF NOT EXISTS idx_srcdn_cachedtime ON dxmlevent ("srcdn", "cachedtime");
-- Recent events, dashboard, date filters and purge jobs
CREATE INDEX IF NOT EXISTS idx_cachedtime ON dxmlevent ("cachedtime");
-- By driver, and by driver + policy
CREATE INDEX IF NOT EXISTS idx_srcdriver_policy ON dxmlevent ("srcdriver", "policy");

COMMIT;
