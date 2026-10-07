# Event store reference

This is the contract for programs that read the Event Logger's PostgreSQL store, such as the
DirXMLDev core, the DirXMLDevWeb workbench, the simulator and the bundled Flask UI. Readers use
the read-only account and never write to the table. Only the engine writes, through the
EventLogger driver and `PolicyLogger`.

Applies to schema version 2 (release 1.0.0 and later). Rows written earlier have
`schemaversion = 1`; see [Version 1 rows](#version-1-rows).

## Table

One table, `public.dxmlevent` by default. The driver option `tableName` and the web UI's
`TABLE_NAME` can point at another one.

| Column | Type | Null | Meaning |
|--------|------|------|---------|
| `id` | `bigserial` | no | Primary key. Increases in insert order. Use it to link to a row |
| `eventid` | `varchar` | no | The engine's `event-id` attribute. Unique among the driver's own rows (`policy IS NULL`). PolicyLogger can log the same event several times, once per policy and stage |
| `classname` | `varchar` | no | `class-name` of the event, e.g. `User` |
| `srcdn` | `varchar` | yes | `src-dn` of the event, in slash form: `\TREE\data\users\jdoe` |
| `srcentryid` | `varchar` | yes | `src-entry-id`, the eDirectory entry ID |
| `eventtype` | `varchar` | no | `add`, `modify`, `delete`, `sync`, `rename` or `move` |
| `eventjson` | `jsonb` | no | The event converted to JSON (shapes below). Passwords are `***` |
| `xmlevent` | `text` | yes | The XDS document as received, passwords masked. NULL when the driver runs with `storeXML=false` |
| `cachedtime` | `timestamptz` | no | Event time: the seconds part of the event's `timestamp` attribute (`1759846502#3` → 2025-10-07 14:15:02 UTC) |
| `srcdriver` | `varchar` | yes | DN of the driver the event came from. For driver rows, the EventLogger driver's own DN. For PolicyLogger rows, the DN the calling policy passed |
| `channel` | `varchar` | yes | PolicyLogger rows only: `subscriber` or `publisher` |
| `policy` | `varchar` | yes | PolicyLogger rows only: the policy name or DN, exactly as the policy passed it |
| `stage` | `varchar` | yes | PolicyLogger rows only: `input` (the document going into the policy) or `output` (its result) |
| `schemaversion` | `smallint` | no | Shape of `eventjson`: `2` for current rows, `1` for rows written before release 1.0.0 or by older driver jars |

**Driver rows and PolicyLogger rows.** `policy IS NULL` means the EventLogger driver captured the
event on its own subscriber channel, after its own filter. `policy IS NOT NULL` means a policy on
driver `srcdriver` logged the document at that point of its `channel`. `channel`, `policy` and
`stage` are either all set or all NULL.

### Indexes

| Index | Columns | Serves |
|-------|---------|--------|
| primary key | `id` | Row lookup |
| `ux_dxmlevent_driver_eventid` | `eventid` WHERE `policy IS NULL` (unique) | One driver row per engine event |
| `idx_eventid` | `eventid` | All rows for an engine event |
| `idx_srcdn_prefix` | `srcdn text_pattern_ops` | Subtree (`LIKE 'prefix%'`) |
| `idx_srcdn_reverse_pattern` | `REVERSE(srcdn) text_pattern_ops` | DN ends with (`reverse(srcdn) LIKE ...`) |
| `idx_srcdn_cachedtime` | `srcdn, cachedtime` | One object's timeline |
| `idx_cachedtime` | `cachedtime` | Time windows, recent events |
| `idx_srcdriver_policy` | `srcdriver, policy` | By driver, by driver and policy |

## JSON shape (schemaVersion 2)

Every document is one JSON object with these keys:

- **Event attributes:** every XML attribute of the event element, as a string, under its XDS
  name: `class-name`, `src-dn`, `src-entry-id`, `event-id`, `timestamp`, plus whatever else
  the event carries (`qualified-src-dn`, `old-src-dn`, `remove-old-name`, ...).
- **`event-type`:** `add`, `modify`, `delete`, `sync`, `rename` or `move`.
- **`schemaVersion`:** the number `2`.
- **`association`:** the event's own `<association>`, as `{"value": text, ...its attributes}`
  (for example `state`). Absent when there is none.
- **`operationData`:** `{child-element-name: text}` for the direct children of
  `<operation-data>`. Absent when there is none.
- **`logged-by-policy`, `logged-channel`:** PolicyLogger rows only, the policy name and channel
  as passed. Prefer the `policy` and `channel` columns.

**Values.** A value with no XML attributes is a plain string. Otherwise it is an object holding
its attributes (`type`, `timestamp`, ...) and `"value": text`. A structured value has
`"components": {name: text}` instead of `value`. An attribute with one value holds that value;
an attribute with several holds a list.

`eventjson` is `jsonb`, so PostgreSQL does not keep key order.

### add

`attributes` maps each `<add-attr>` name to its value or values. `password` is `***` when the
event carried one.

```json
{
  "event-type": "add", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007141502#1#1", "timestamp": "1759846502#3",
  "src-dn": "\\TREE\\data\\users\\jdoe", "src-entry-id": "35868",
  "qualified-src-dn": "O=data\\OU=users\\CN=jdoe",
  "association": {"state": "pending", "value": "jdoe"},
  "attributes": {
    "Surname": {"type": "string", "value": "Doe", "timestamp": "1759846502#1"},
    "Telephone Number": [
      {"type": "teleNumber", "value": "555-0100"},
      {"type": "teleNumber", "value": "555-0101"}
    ],
    "Facsimile Telephone Number": {
      "type": "structured", "components": {"faxNumber": "555-0199", "faxBitCount": "0"}
    },
    "Description": "plain value"
  },
  "password": "***",
  "operationData": {"correlation-id": "abc-123"}
}
```

### modify

`attributes` maps each `<modify-attr>` name to an object with any of `remove-all-values: true`,
`remove-values: [values]` and `add-values: [values]`. Each value is one `<value>` element.

```json
{
  "event-type": "modify", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007141600#1#2", "timestamp": "1759846560#2",
  "src-dn": "\\TREE\\data\\users\\jdoe", "src-entry-id": "35868",
  "association": {"state": "associated", "value": "jdoe"},
  "attributes": {
    "Given Name": {
      "remove-values": [{"type": "string", "value": "Johnny", "timestamp": "1759846502#2"}],
      "add-values": [{"type": "string", "value": "John", "timestamp": "1759846560#1"}]
    },
    "Telephone Number": {
      "remove-all-values": true,
      "add-values": [{"type": "teleNumber", "value": "555-0102"}, {"type": "teleNumber", "value": "555-0103"}]
    }
  },
  "operationData": {"correlation-id": "def-456"}
}
```

A password change arrives as a modify of a password attribute (for example
`nspmDistributionPassword`), with every value `***`.

### delete

```json
{
  "event-type": "delete", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007141700#1#3", "timestamp": "1759846620#1",
  "src-dn": "\\TREE\\data\\users\\jdoe", "src-entry-id": "35868",
  "association": {"state": "associated", "value": "jdoe"}
}
```

### sync

`attributes` is shaped as for add. `status` is the text of a `<status>` child, if any.

```json
{
  "event-type": "sync", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007141800#1#4", "timestamp": "1759846680#1",
  "src-dn": "\\TREE\\data\\users\\jdoe", "src-entry-id": "35868",
  "association": {"value": "CN=jdoe,OU=users,O=company"},
  "attributes": {"CN": {"type": "string", "value": "jdoe", "timestamp": "1759846502#1"}},
  "status": "success"
}
```

### rename

`new-name` is the new RDN from `<new-name>`. `src-dn` is the new DN and `old-src-dn` the old one.

```json
{
  "event-type": "rename", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007141900#1#5", "timestamp": "1759846740#1",
  "src-dn": "\\TREE\\data\\users\\john.doe", "old-src-dn": "\\TREE\\data\\users\\jdoe",
  "src-entry-id": "35868", "remove-old-name": "true",
  "association": {"state": "associated", "value": "jdoe"},
  "new-name": "john.doe"
}
```

### move

`parent` is the destination container: its attributes, plus its own `association` if it has one.

```json
{
  "event-type": "move", "schemaVersion": 2,
  "class-name": "User", "event-id": "edir3#20261007142000#1#6", "timestamp": "1759846800#1",
  "src-dn": "\\TREE\\data\\staff\\john.doe", "old-src-dn": "\\TREE\\data\\users\\john.doe",
  "src-entry-id": "35868",
  "association": {"state": "associated", "value": "jdoe"},
  "parent": {
    "src-dn": "\\TREE\\data\\staff", "src-entry-id": "40001",
    "association": {"value": "OU=staff,O=company"}
  }
}
```

### Rebuilding XML

When `xmlevent` is NULL, `com.pointblue.idm.eventlogger.xds2json.JsonToXmlConverter` (in the
release jar) rebuilds an equivalent XDS document from `eventjson`. It is lossy; the README
section [What the JSON does not keep](../README.md#rebuilding-xml-from-the-json) lists exactly
what is lost. Prefer `xmlevent` whenever it is present.

### Version 1 rows

Rows with `schemaversion = 1` have no `schemaVersion` key and differ as follows:

- **modify:** each entry of `add-values` and `remove-values` is the text of a whole
  `<add-value>` or `<remove-value>` element. It has no `type` or `timestamp`, and several
  values in one element are run together.
- **rename:** has no `new-name`. Older test documents used `from` and `to` keys instead.
- **move:** `parent.value` is the parent association's text, not an `association` object. The
  event's `association` may wrongly be the parent's.
- PolicyLogger rows have `channel`, `policy` and `stage` filled in only if the migration found
  `logged-by-policy` in the JSON; `stage` is then `input`.

## Query patterns

These are the queries the Flask UI runs, plus the ones the stack needs. All but free text use an
index. Use bound parameters, never string concatenation.

**One object's timeline.**

```sql
SELECT id, eventid, eventtype, cachedtime, srcdriver, channel, policy, stage, eventjson
FROM dxmlevent
WHERE srcdn = $1
ORDER BY cachedtime, id;
```

**A subtree:** everything below a container. DNs contain backslashes, which are also `LIKE`'s
escape character, so always write `ESCAPE ''`. End the container with `\` so `\users` does
not also match `\users2`. `%` and `_` in the DN still act as wildcards. If your DNs can contain
them, use `ESCAPE '!'` instead and put `!` before each `%`, `_` and `!` in `$1`.

```sql
SELECT id, srcdn, eventtype, cachedtime
FROM dxmlevent
WHERE srcdn LIKE $1 || '%' ESCAPE ''          -- $1 = '\TREE\data\users\'
ORDER BY cachedtime;
```

**By the end of the DN:** an object found by its name without knowing its container. This uses
the reverse index:

```sql
SELECT id, srcdn, eventtype, cachedtime
FROM dxmlevent
WHERE reverse(srcdn) LIKE reverse('%' || $1) ESCAPE ''   -- $1 = '\users\jdoe'
ORDER BY cachedtime;
```

**By driver**, the driver's own rows only, or all rows a driver's policies logged:

```sql
SELECT * FROM dxmlevent WHERE srcdriver = $1 AND policy IS NULL ORDER BY cachedtime DESC LIMIT 100;
SELECT * FROM dxmlevent WHERE srcdriver = $1 AND policy IS NOT NULL ORDER BY cachedtime DESC LIMIT 100;
```

**By policy:** the documents at policy P of driver D, the input to "save as case":

```sql
SELECT id, eventid, channel, stage, cachedtime, xmlevent
FROM dxmlevent
WHERE srcdriver = $1 AND policy = $2 AND stage = 'input'
ORDER BY cachedtime DESC;
```

**Every row for one engine event**, the driver's row first and then each policy stage:

```sql
SELECT * FROM dxmlevent WHERE eventid = $1 ORDER BY policy NULLS FIRST, id;
```

**Time window**, newest first:

```sql
SELECT id, srcdn, eventtype, srcdriver, cachedtime
FROM dxmlevent
WHERE cachedtime >= $1 AND cachedtime < $2
ORDER BY cachedtime DESC, id DESC
LIMIT 100;
```

**Free text** over the JSON. This scans the table, so combine it with a time window on large
stores:

```sql
SELECT id, srcdn, eventtype, cachedtime
FROM dxmlevent
WHERE eventjson::text ILIKE '%' || $1 || '%'
  AND cachedtime >= $2
ORDER BY cachedtime DESC
LIMIT 50;
```

**Structured JSON:** modify events that touched an attribute:

```sql
SELECT id, srcdn, cachedtime, eventjson -> 'attributes' -> $1 AS change
FROM dxmlevent
WHERE eventtype = 'modify' AND eventjson -> 'attributes' ? $1;
```

## Reader account

Readers connect with a login that can only `SELECT`. The bundled database (`docker/compose.yml`)
creates it on first start; on another server, create it as the table owner:

```sql
CREATE USER eventlogger_reader WITH PASSWORD '...';
GRANT CONNECT ON DATABASE "idmEvent" TO eventlogger_reader;
GRANT USAGE ON SCHEMA public TO eventlogger_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO eventlogger_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO eventlogger_reader;
```

The account cannot insert, update or delete, and it needs no access to the `id` sequence. The
driver uses a separate account with `SELECT, INSERT` on the table and `USAGE` on
`dxmlevent_id_seq`.

The data contains real names and DNs from the tree. Passwords are masked, but everything else in
an event is stored as the engine sent it.

## Stability

Within schema version 2:

- Columns and JSON keys may be added, but are not renamed or removed.
- The meaning of existing columns and keys does not change.

Any change that would break a reader raises `schemaVersion`, and the change is listed in
[CHANGELOG.md](../CHANGELOG.md).
