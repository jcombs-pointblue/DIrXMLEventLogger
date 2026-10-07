# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-07

### Added
- Maven build (`pom.xml`) producing `dirxml-event-logger-<version>.jar`. It compiles against
  the real engine jars in `lib/` when present, otherwise against the compile-only stubs in
  `idm-api-stubs/`. No engine classes are packaged either way.
- GitHub release with the jar and the PostgreSQL JDBC driver attached, built on `v*` tags.
- `EventLoggerDriver.VERSION`, written to the trace on `init()`.
- Password redaction: password values are replaced with `***` in both `eventjson` and
  `xmlevent` before storage.
- Container deployment: the web UI image (amd64/arm64, published to GHCR) and a compose file
  with an optional bundled PostgreSQL.
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

### Removed
- `PolicyLogger.writeEventToDB(JSONObject, XmlDocument, boolean)`: it stored pre-built JSON
  that could not be redacted. Use `PolicyLogger.logEvent`.

### Fixed
- Web UI: database connections are closed after each request; the Flask debugger is off by
  default; bad `page` values no longer fail the request and `/recent` is capped at 1000 rows;
  the CSV export filename is sanitized.

[Unreleased]: https://github.com/jcombs-pointblue/DIrXMLEventLogger/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/jcombs-pointblue/DIrXMLEventLogger/releases/tag/v1.0.0
