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
CREATE INDEX idx_srcdn_reverse ON dxmlevent (REVERSE("srcdn"));
CREATE INDEX idx_srcdriver ON dxmlevent ("srcdriver");

COMMIT;
