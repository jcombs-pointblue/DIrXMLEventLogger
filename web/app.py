import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, jsonify, Response
import csv
import io
import json

app = Flask(__name__)

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "idmEvent"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

TABLE_NAME = os.environ.get("TABLE_NAME", "public.dxmlevent")


def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search-objects")
def search_objects():
    """Autocomplete search for object DNs."""
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT DISTINCT srcdn FROM {TABLE_NAME}
                WHERE srcdn ILIKE %s
                ORDER BY srcdn
                LIMIT 20""",
            (f"%{q}%",),
        )
        return jsonify([row["srcdn"] for row in cur.fetchall()])


@app.route("/timeline")
def timeline():
    """Show a timeline of all events for a given object DN."""
    srcdn = request.args.get("srcdn", "").strip()
    event_type = request.args.get("event_type", "").strip()
    class_name = request.args.get("class_name", "").strip()
    src_driver = request.args.get("src_driver", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    page = int(request.args.get("page", "1"))
    per_page = 50

    if not srcdn:
        return render_template("timeline.html", events=[], srcdn="", total=0,
                               page=1, pages=0, event_type="", class_name="",
                               src_driver="", date_from="", date_to="",
                               event_types=[], class_names=[], src_drivers=[])

    conditions = ["srcdn = %s"]
    params = [srcdn]

    if event_type:
        conditions.append("eventtype = %s")
        params.append(event_type)
    if class_name:
        conditions.append("classname = %s")
        params.append(class_name)
    if src_driver:
        conditions.append("srcdriver = %s")
        params.append(src_driver)
    if date_from:
        conditions.append("cachedtime >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("cachedtime <= %s")
        params.append(date_to + " 23:59:59")

    where = " AND ".join(conditions)

    with get_db() as conn, conn.cursor() as cur:
        # Get filter options for this object
        cur.execute(f"SELECT DISTINCT eventtype FROM {TABLE_NAME} WHERE srcdn = %s ORDER BY eventtype", (srcdn,))
        event_types = [r["eventtype"] for r in cur.fetchall()]

        cur.execute(f"SELECT DISTINCT classname FROM {TABLE_NAME} WHERE srcdn = %s ORDER BY classname", (srcdn,))
        class_names = [r["classname"] for r in cur.fetchall()]

        cur.execute(f"SELECT DISTINCT srcdriver FROM {TABLE_NAME} WHERE srcdn = %s AND srcdriver IS NOT NULL ORDER BY srcdriver", (srcdn,))
        src_drivers = [r["srcdriver"] for r in cur.fetchall()]

        # Count
        cur.execute(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE {where}", params)
        total = cur.fetchone()["cnt"]
        pages = max(1, (total + per_page - 1) // per_page)

        # Fetch page
        offset = (page - 1) * per_page
        cur.execute(
            f"""SELECT eventid, classname, srcdn, srcentryid, eventtype,
                       eventjson, cachedtime, xmlevent, srcdriver
                FROM {TABLE_NAME}
                WHERE {where}
                ORDER BY cachedtime ASC, eventid ASC
                LIMIT %s OFFSET %s""",
            params + [per_page, offset],
        )
        events = cur.fetchall()

    # Format JSON for display
    for e in events:
        if e["eventjson"] and isinstance(e["eventjson"], str):
            try:
                e["eventjson"] = json.loads(e["eventjson"])
            except (json.JSONDecodeError, TypeError):
                pass

    is_htmx = request.headers.get("HX-Request") == "true"
    template = "partials/timeline_results.html" if is_htmx else "timeline.html"

    return render_template(template, events=events, srcdn=srcdn, total=total,
                           page=page, pages=pages, event_type=event_type,
                           class_name=class_name, src_driver=src_driver,
                           date_from=date_from, date_to=date_to,
                           event_types=event_types, class_names=class_names,
                           src_drivers=src_drivers)


@app.route("/event")
def event_detail():
    """Show full detail for a single event."""
    event_id = request.args.get("id", "").strip()
    if not event_id:
        return "Event ID required", 400

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT eventid, classname, srcdn, srcentryid, eventtype,
                       eventjson, cachedtime, xmlevent, srcdriver
                FROM {TABLE_NAME} WHERE eventid = %s""",
            (event_id,),
        )
        event = cur.fetchone()

    if not event:
        return "Event not found", 404

    if event["eventjson"] and isinstance(event["eventjson"], str):
        try:
            event["eventjson"] = json.loads(event["eventjson"])
        except (json.JSONDecodeError, TypeError):
            pass

    # For modify events, extract the changes summary
    changes = None
    if event["eventtype"] == "modify" and isinstance(event["eventjson"], dict):
        changes = extract_modify_changes(event["eventjson"])

    # Get previous and next events for this object
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT eventid, eventtype, cachedtime FROM {TABLE_NAME}
                WHERE srcdn = %s AND cachedtime <= %s AND eventid != %s
                ORDER BY cachedtime DESC, eventid DESC LIMIT 1""",
            (event["srcdn"], event["cachedtime"], event_id),
        )
        prev_event = cur.fetchone()

        cur.execute(
            f"""SELECT eventid, eventtype, cachedtime FROM {TABLE_NAME}
                WHERE srcdn = %s AND cachedtime >= %s AND eventid != %s
                ORDER BY cachedtime ASC, eventid ASC LIMIT 1""",
            (event["srcdn"], event["cachedtime"], event_id),
        )
        next_event = cur.fetchone()

    return render_template("event_detail.html", event=event, changes=changes,
                           prev_event=prev_event, next_event=next_event)


@app.route("/search")
def search():
    """Full-text search across event JSON payloads."""
    q = request.args.get("q", "").strip()
    event_type = request.args.get("event_type", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    page = int(request.args.get("page", "1"))
    per_page = 50

    if not q:
        return render_template("search.html", events=[], q="", total=0,
                               page=1, pages=0, event_type="", date_from="",
                               date_to="")

    conditions = ["eventjson::text ILIKE %s"]
    params = [f"%{q}%"]

    if event_type:
        conditions.append("eventtype = %s")
        params.append(event_type)
    if date_from:
        conditions.append("cachedtime >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("cachedtime <= %s")
        params.append(date_to + " 23:59:59")

    where = " AND ".join(conditions)

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE {where}", params)
        total = cur.fetchone()["cnt"]
        pages = max(1, (total + per_page - 1) // per_page)

        offset = (page - 1) * per_page
        cur.execute(
            f"""SELECT eventid, classname, srcdn, srcentryid, eventtype,
                       eventjson, cachedtime, srcdriver
                FROM {TABLE_NAME}
                WHERE {where}
                ORDER BY cachedtime DESC
                LIMIT %s OFFSET %s""",
            params + [per_page, offset],
        )
        events = cur.fetchall()

    for e in events:
        if e["eventjson"] and isinstance(e["eventjson"], str):
            try:
                e["eventjson"] = json.loads(e["eventjson"])
            except (json.JSONDecodeError, TypeError):
                pass

    is_htmx = request.headers.get("HX-Request") == "true"
    template = "partials/search_results.html" if is_htmx else "search.html"

    return render_template(template, events=events, q=q, total=total,
                           page=page, pages=pages, event_type=event_type,
                           date_from=date_from, date_to=date_to)


@app.route("/export/timeline")
def export_timeline():
    """Export timeline as CSV."""
    srcdn = request.args.get("srcdn", "").strip()
    if not srcdn:
        return "srcdn required", 400

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT eventid, classname, srcdn, srcentryid, eventtype,
                       eventjson::text as eventjson, cachedtime, xmlevent, srcdriver
                FROM {TABLE_NAME}
                WHERE srcdn = %s
                ORDER BY cachedtime ASC""",
            (srcdn,),
        )
        events = cur.fetchall()

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["eventid", "cachedtime", "eventtype",
                                                 "classname", "srcdn", "srcentryid",
                                                 "srcdriver", "eventjson", "xmlevent"])
    writer.writeheader()
    for e in events:
        writer.writerow(e)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=timeline_{srcdn.split(chr(92))[-1]}.csv"},
    )


@app.route("/recent")
def recent():
    """Show the most recent events across all objects."""
    limit = int(request.args.get("limit", "100"))
    event_type = request.args.get("event_type", "").strip()
    src_driver = request.args.get("src_driver", "").strip()

    conditions = []
    params = []

    if event_type:
        conditions.append("eventtype = %s")
        params.append(event_type)
    if src_driver:
        conditions.append("srcdriver = %s")
        params.append(src_driver)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT eventtype FROM {TABLE_NAME} ORDER BY eventtype")
        event_types = [r["eventtype"] for r in cur.fetchall()]

        cur.execute(f"SELECT DISTINCT srcdriver FROM {TABLE_NAME} WHERE srcdriver IS NOT NULL ORDER BY srcdriver")
        src_drivers = [r["srcdriver"] for r in cur.fetchall()]

        cur.execute(
            f"""SELECT eventid, classname, srcdn, srcentryid, eventtype,
                       eventjson, cachedtime, srcdriver
                FROM {TABLE_NAME}
                {where}
                ORDER BY cachedtime DESC, eventid DESC
                LIMIT %s""",
            params + [limit],
        )
        events = cur.fetchall()

    for e in events:
        if e["eventjson"] and isinstance(e["eventjson"], str):
            try:
                e["eventjson"] = json.loads(e["eventjson"])
            except (json.JSONDecodeError, TypeError):
                pass

    return render_template("recent.html", events=events, limit=limit,
                           event_type=event_type, src_driver=src_driver,
                           event_types=event_types, src_drivers=src_drivers)


@app.route("/stats")
def stats():
    """Dashboard with event statistics."""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME}")
        total = cur.fetchone()["total"]

        cur.execute(
            f"""SELECT eventtype, COUNT(*) as cnt
                FROM {TABLE_NAME} GROUP BY eventtype ORDER BY cnt DESC""")
        by_type = cur.fetchall()

        cur.execute(
            f"""SELECT classname, COUNT(*) as cnt
                FROM {TABLE_NAME} GROUP BY classname ORDER BY cnt DESC LIMIT 20""")
        by_class = cur.fetchall()

        cur.execute(
            f"""SELECT DATE(cachedtime) as day, COUNT(*) as cnt
                FROM {TABLE_NAME}
                WHERE cachedtime >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(cachedtime) ORDER BY day""")
        by_day = cur.fetchall()

        cur.execute(
            f"""SELECT srcdn, COUNT(*) as cnt
                FROM {TABLE_NAME} GROUP BY srcdn ORDER BY cnt DESC LIMIT 20""")
        most_active = cur.fetchall()

    return render_template("stats.html", total=total, by_type=by_type,
                           by_class=by_class, by_day=by_day,
                           most_active=most_active)


def extract_modify_changes(event_json):
    """Extract a human-readable summary of what changed in a modify event."""
    changes = []
    attrs = event_json.get("attributes", {})
    for attr_name, mods in attrs.items():
        if not isinstance(mods, dict):
            continue
        change = {"attribute": attr_name}
        if mods.get("remove-all-values"):
            change["action"] = "cleared"
        added = mods.get("add-values", [])
        removed = mods.get("remove-values", [])
        if removed:
            change["old"] = extract_values(removed)
        if added:
            change["new"] = extract_values(added)
        if not change.get("action"):
            if removed and added:
                change["action"] = "changed"
            elif added:
                change["action"] = "added"
            elif removed:
                change["action"] = "removed"
            else:
                change["action"] = "modified"
        changes.append(change)
    return changes


def extract_values(value_list):
    """Pull display values from converter output structure."""
    results = []
    for v in value_list:
        if isinstance(v, dict):
            # add-value/remove-value wraps a value element
            val = v.get("value", "")
            if not val:
                # might have nested structure
                for key, inner in v.items():
                    if isinstance(inner, dict) and "value" in inner:
                        val = inner["value"]
                        break
            results.append(str(val) if val else str(v))
        else:
            results.append(str(v))
    return ", ".join(results)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
