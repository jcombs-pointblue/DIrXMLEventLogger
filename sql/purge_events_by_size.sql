-- Deletes the oldest events in batches until the table is under max_size_mb.
-- Does nothing when the table is already under the limit. See README "Size-based cleanup".
CREATE OR REPLACE FUNCTION purge_events_by_size(
    max_size_mb int DEFAULT 1000,
    batch_size int DEFAULT 10000,
    pause_ms int DEFAULT 100
)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    total_deleted bigint := 0;
    batch_deleted bigint;
BEGIN
    IF pg_total_relation_size('dxmlevent') <= max_size_mb::bigint * 1024 * 1024 THEN
        RETURN 0;
    END IF;

    LOOP
        DELETE FROM dxmlevent
        WHERE id IN (
            SELECT id FROM dxmlevent
            ORDER BY cachedtime ASC
            LIMIT batch_size
        );
        GET DIAGNOSTICS batch_deleted = ROW_COUNT;
        total_deleted := total_deleted + batch_deleted;
        EXIT WHEN batch_deleted = 0;
        PERFORM pg_sleep(pause_ms / 1000.0);
        EXIT WHEN pg_total_relation_size('dxmlevent') <= max_size_mb::bigint * 1024 * 1024;
    END LOOP;
    RETURN total_deleted;
END;
$$;
