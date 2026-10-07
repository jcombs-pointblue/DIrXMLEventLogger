# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-10-07

Upgrading from 1.0.0: run `sql/MIGRATE 2 policy columns.sql` on the database before deploying
the new jar or web UI (see the README, "Upgrading an existing database").

### Added
- Maven build (`pom.xml`) producing `dirxml-event-logger-<version>.jar`. It compiles against
  the real engine jars in `lib/` when present, otherwise against the compile-only stubs in
  `idm-api-stubs/`. No engine classes are packaged either way.
- GitHub release with the jar and the PostgreSQL JDBC driver attached, built on `v*` tags.
- `EventLoggerDriver.VERSION`, written to the trace on `init()`.
- Password redaction: password values are replaced with `***` in both `eventjson` and
  `xmlevent` before storage.
- Container deployment (`docker/compose.yml`): the web UI image (amd64/arm64, published to
  GHCR with the release version) and an optional bundled PostgreSQL that applies the schema,
  creates the writer and reader accounts, and schedules the size-based cleanup with pg_cron.
  `fetch-jars` downloads a release's jars for mounting into an engine container.
- `sql/purge_events_by_size.sql`.
- `docs/store.md`: the reader contract (columns, JSON shape per event type, query patterns,
  reader grants).
- Indexes on `(srcdn, cachedtime)`, `cachedtime`, `eventid` and `(srcdriver, policy)`.
- `channel`, `policy` and `stage` columns for PolicyLogger rows, and a
  `PolicyLogger.logEvent(..., stage, xml)` overload (stage defaults to `input`).
  Migration: `sql/MIGRATE 2 policy columns.sql`.
- Web UI shows the policy, channel and stage of PolicyLogger rows.
- `"schemaVersion": 2` in every converted JSON document, and a `schemaversion` column
  (1 for rows written before this release or by older driver jars, 2 for new rows).

### Changed
- The table's primary key is a new `id` column. `eventid` stays unique among the driver's own
  rows, but PolicyLogger can now log the same event at several policies and stages (before,
  those inserts failed as duplicates).
- Web UI event links use the row id (`/event?row=`); `/event?id=<eventid>` still works.
- Unparseable subscriber documents return an error instead of a retry, so they no longer
  block the queue.
- The driver no longer uses `XDSCommandDocument`, `com.novell.xsl.util.Util` or
  `com.novell.xml.dom.DocumentFactory`; only the core driver API is needed at runtime.
- The PostgreSQL JDBC jar is a Maven dependency instead of a file committed in `lib/`.

- Converters (JSON schemaVersion 2): modify values keep their `type`, `timestamp` and
  structured components, one entry per `<value>` (before, all values of an `<add-value>` were
  run together as text). Rename keeps `<new-name>`. A move's parent keeps its association as
  an object, and the event's own association is no longer taken from inside `<parent>`.
- `JsonToXmlConverter` rewritten: it reads `event-type` instead of guessing, supports move,
  and keeps every event attribute (timestamp, old-src-dn, ...). Round-trip tests cover all
  six event types; the README lists what the JSON does not keep.

- Subtree and DN-suffix `LIKE` queries can use an index under any collation (new
  `text_pattern_ops` indexes; the old `REVERSE(srcdn)` index could not serve `LIKE` under
  `en_US.UTF-8`). The README's subtree example was wrong: backslashes in a DN are `LIKE`
  escapes, so it needs `ESCAPE ''`.

### Removed
- `PolicyLogger.writeEventToDB(JSONObject, XmlDocument, boolean)`: it stored pre-built JSON
  that could not be redacted. Use `PolicyLogger.logEvent`.

### Fixed
- Web UI: database connections are closed after each request; the Flask debugger is off by
  default; bad `page` values no longer fail the request and `/recent` is capped at 1000 rows;
  the CSV export filename is sanitized.
- Building against the real engine jars also needs `dirxml_misc.jar` in `lib/` (`ShimParams`
  references `MessageSource` from it); the `engine` profile now adds it.

### Verified
- Compiled and tested against the Identity Manager 4.x engine jars (Designer 4.0).
- Ran on an Identity Manager 4.8.7 engine (eDirectory 9.2.8) against a 1.0.0 table upgraded with
  the migration script: add, modify and password-change events were logged with
  `schemaversion` 2 and the password stored as `***` in both `eventjson` and `xmlevent`, and
  queued events from before the upgrade were logged. PolicyLogger calls from an ECMAScript
  policy were not tested on an engine (unit and PostgreSQL integration tests only).

## [1.0.0] - 2026-04-23

First release: the EventLogger driver, PolicyLogger, the Flask web UI and the pg_cron
cleanup examples.

[Unreleased]: https://github.com/jcombs-pointblue/DIrXMLEventLogger/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/jcombs-pointblue/DIrXMLEventLogger/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/jcombs-pointblue/DIrXMLEventLogger/releases/tag/v1.0.0
