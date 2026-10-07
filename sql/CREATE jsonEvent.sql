BEGIN;

CREATE TABLE IF NOT EXISTS dxmlevent (
	"eventid" character varying NOT NULL,
	"classname" character varying NOT NULL,
	"srcdn" character varying,
	"srcentryid" character varying,
	"eventtype" character varying NOT NULL,
	"eventjson" jsonb NOT NULL,
	"xmlevent" text,
	"cachedtime" timestamp with time zone NOT NULL,
	"srcdriver" character varying,
	PRIMARY KEY("eventid")
);
-- Subtree queries: reverse(srcdn) LIKE reverse('%...')
CREATE INDEX IF NOT EXISTS idx_srcdn_reverse ON dxmlevent (REVERSE("srcdn"));
-- Timeline and event detail: srcdn = ? ORDER BY cachedtime
CREATE INDEX IF NOT EXISTS idx_srcdn_cachedtime ON dxmlevent ("srcdn", "cachedtime");
-- Recent events, dashboard, date filters and purge jobs
CREATE INDEX IF NOT EXISTS idx_cachedtime ON dxmlevent ("cachedtime");
CREATE INDEX IF NOT EXISTS idx_srcdriver ON dxmlevent ("srcdriver");

COMMIT;
