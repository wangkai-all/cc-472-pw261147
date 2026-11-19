"""Execution engine: batch ingest, rule evaluation, quarantine, replay and gate.

Design points
-------------
* Records are immutable once written. A repair never updates a row; replay
  creates a new child batch carrying the corrected payload under the same
  ``record_key``.
* ``quarantine_entries`` holds the *current* state per (dataset, record, rule);
  ``quarantine_events`` is append-only and keeps the full history, so a record
  fixed then broken again keeps every prior outcome.
* A replay always evaluates the *currently published* rule versions — this is
  what makes a rule upgrade take effect on repaired data.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from .rules import CheckFailure, Checker, RuleConfigError, validate_config

REPLAY_BATCH = "replay"
INGEST_BATCH = "ingest"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Rule versioning
# ---------------------------------------------------------------------------
def create_rule(
    conn: sqlite3.Connection,
    dataset: str,
    name: str,
    rule_type: str,
    config: dict[str, Any],
    change_note: str | None = None,
) -> dict[str, Any]:
    validate_config(rule_type, config)
    rule_id = _new_id("rule")
    now = _now()
    conn.execute(
        "INSERT INTO rules (rule_id, dataset, name, type, created_at) VALUES (?,?,?,?,?)",
        (rule_id, dataset, name, rule_type, now),
    )
    cur = conn.execute(
        """INSERT INTO rule_versions (rule_id, version, config, status, change_note, created_at)
           VALUES (?,1,?,'draft',?,?)""",
        (rule_id, json.dumps(config, ensure_ascii=False), change_note, now),
    )
    return get_rule_detail(conn, rule_id)


def publish_rule_version(
    conn: sqlite3.Connection, rule_id: str, change_note: str | None = None
) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM rules WHERE rule_id=?", (rule_id,)).fetchone()
    if row is None:
        raise LookupError(f"rule {rule_id} not found")
    draft = conn.execute(
        "SELECT * FROM rule_versions WHERE rule_id=? AND status='draft' ORDER BY version DESC",
        (rule_id,),
    ).fetchone()
    if draft is None:
        raise RuleConfigError("rule has no draft version to publish")
    now = _now()
    conn.execute(
        "UPDATE rule_versions SET status='archived' WHERE rule_id=? AND status='published'",
        (rule_id,),
    )
    conn.execute(
        "UPDATE rule_versions SET status='published', change_note=?, created_at=? WHERE id=?",
        (change_note or draft["change_note"], now, draft["id"]),
    )
    return get_rule_detail(conn, rule_id)


def upgrade_rule(
    conn: sqlite3.Connection, rule_id: str, config: dict[str, Any], change_note: str
) -> dict[str, Any]:
    """Create a new draft version of an existing rule (rule upgrade)."""
    row = conn.execute("SELECT * FROM rules WHERE rule_id=?", (rule_id,)).fetchone()
    if row is None:
        raise LookupError(f"rule {rule_id} not found")
    validate_config(row["type"], config)
    next_version = (
        conn.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM rule_versions WHERE rule_id=?",
            (rule_id,),
        ).fetchone()["v"]
    )
    conn.execute(
        """INSERT INTO rule_versions (rule_id, version, config, status, change_note, created_at)
           VALUES (?,? ,?,'draft',?,?)""",
        (rule_id, next_version, json.dumps(config, ensure_ascii=False), change_note, _now()),
    )
    return get_rule_detail(conn, rule_id)


def get_rule_detail(conn: sqlite3.Connection, rule_id: str) -> dict[str, Any]:
    rule = conn.execute("SELECT * FROM rules WHERE rule_id=?", (rule_id,)).fetchone()
    if rule is None:
        raise LookupError(f"rule {rule_id} not found")
    versions = conn.execute(
        "SELECT * FROM rule_versions WHERE rule_id=? ORDER BY version", (rule_id,)
    ).fetchall()
    return {
        "rule_id": rule["rule_id"],
        "dataset": rule["dataset"],
        "name": rule["name"],
        "type": rule["type"],
        "created_at": rule["created_at"],
        "versions": [
            {
                "version": v["version"],
                "status": v["status"],
                "config": json.loads(v["config"]),
                "change_note": v["change_note"],
                "created_at": v["created_at"],
            }
            for v in versions
        ],
    }


def list_rules(conn: sqlite3.Connection, dataset: str | None = None) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT rule_id FROM rules WHERE (? IS NULL OR dataset=?) ORDER BY created_at",
        (dataset, dataset),
    ).fetchall()
    return [get_rule_detail(conn, r["rule_id"]) for r in rows]


def _published_snapshot(conn: sqlite3.Connection, dataset: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT rv.*, r.name, r.type, r.rule_id
           FROM rule_versions rv JOIN rules r ON r.rule_id = rv.rule_id
           WHERE r.dataset=? AND rv.status='published'
           ORDER BY r.rule_id, rv.version""",
        (dataset,),
    ).fetchall()
    return [
        {
            "rule_id": r["rule_id"],
            "rule_name": r["name"],
            "rule_type": r["type"],
            "version": r["version"],
            "config": json.loads(r["config"]),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Record keys
# ---------------------------------------------------------------------------
def _pk_fields(conn: sqlite3.Connection, dataset: str) -> list[str] | None:
    row = conn.execute(
        """SELECT rv.config FROM rule_versions rv JOIN rules r ON r.rule_id = rv.rule_id
           WHERE r.dataset=? AND r.type='primary_key' AND rv.status='published'
           ORDER BY rv.version DESC LIMIT 1""",
        (dataset,),
    ).fetchone()
    return json.loads(row["config"])["fields"] if row else None


def _derive_key(row: dict[str, Any], pk_fields: list[str] | None, index: int) -> str:
    if pk_fields:
        vals = [row.get(f) for f in pk_fields]
        if any(v is None or (isinstance(v, str) and not v.strip()) for v in vals):
            return f"blankpk:{index}"
        return "~".join(str(v) for v in vals)
    if row.get("_key") is not None and str(row["_key"]).strip():
        return str(row["_key"])
    return f"row:{index}"


# ---------------------------------------------------------------------------
# Batch ingest + execution
# ---------------------------------------------------------------------------
def ingest_batch(conn: sqlite3.Connection, dataset: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError("ingest batch must contain at least one record")
    if not all(isinstance(r, dict) for r in rows):
        raise ValueError("every record must be a JSON object")
    batch_id = _new_id("batch")
    now = _now()
    pk_fields = _pk_fields(conn, dataset)
    keys = [_derive_key(row, pk_fields, i) for i, row in enumerate(rows)]
    conn.execute(
        """INSERT INTO batches (batch_id, dataset, source, parent_batch_id, root_batch_id,
                                status, record_count, created_at)
           VALUES (?,?,?,?,?, 'open', ?,?)""",
        (batch_id, dataset, INGEST_BATCH, None, batch_id, len(rows), now),
    )
    for row, key in zip(rows, keys):
        conn.execute(
            "INSERT INTO records (dataset, batch_id, record_key, payload, created_at) VALUES (?,?,?,?,?)",
            (dataset, batch_id, key, json.dumps(row, ensure_ascii=False), now),
        )
    execution = _run_execution(conn, batch_id)
    return {"batch": get_batch(conn, batch_id), "execution": execution}


def _load_batch_rows(conn: sqlite3.Connection, batch_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT record_key, payload FROM records WHERE batch_id=? ORDER BY id", (batch_id,)
    ).fetchall()
    out = []
    for r in rows:
        payload = json.loads(r["payload"])
        payload["_key"] = r["record_key"]
        out.append(payload)
    return out


def _reference_values(
    conn: sqlite3.Connection, ref_dataset: str, ref_field: str
) -> set[str]:
    """Values present in records of the reference dataset that are NOT currently
    quarantined — quarantined rows must not serve as valid references."""
    rows = conn.execute(
        """SELECT r.payload FROM records r
           WHERE r.dataset=?
             AND r.record_key NOT IN (
                 SELECT record_key FROM quarantine_entries
                 WHERE dataset=? AND status='open')""",
        (ref_dataset, ref_dataset),
    ).fetchall()
    values: set[str] = set()
    for r in rows:
        value = json.loads(r["payload"]).get(ref_field)
        if value is not None:
            values.add(str(value))
    return values


def _run_execution(conn: sqlite3.Connection, batch_id: str) -> dict[str, Any]:
    batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
    dataset = batch["dataset"]
    rows = _load_batch_rows(conn, batch_id)
    snapshot = _published_snapshot(conn, dataset)
    checkers = [Checker(s["rule_id"], s["rule_type"], s["config"]) for s in snapshot]

    # Primary-key collision scope. Same-key rows *within* this batch are caught
    # by the checker as duplicates. Across batches, a key reappears legitimately
    # in every replay of the same repair lineage (those are versions of the same
    # logical record, not collisions). Only a persisted key belonging to a
    # *different* lineage counts as a real primary-key conflict. Lineage
    # scoping alone suffices — no subtraction of open/under-repair keys, which
    # would wrongly erase a genuine cross-lineage collision.
    root_id = batch["root_batch_id"]
    existing_keys = {
        r["record_key"]
        for r in conn.execute(
            """SELECT DISTINCT r.record_key FROM records r
               JOIN batches b ON b.batch_id = r.batch_id
               WHERE r.dataset=? AND r.batch_id<>? AND b.root_batch_id<>?""",
            (dataset, batch_id, root_id),
        ).fetchall()
    }
    ref_sets: dict[str, set[str]] = {}
    for s in snapshot:
        if s["rule_type"] == "cross_table":
            cfg = s["config"]
            ref_sets[s["rule_id"]] = _reference_values(
                conn, cfg["ref_dataset"], cfg["ref_field"]
            )

    failures: list[tuple[dict[str, Any], dict[str, Any], CheckFailure]] = []
    per_rule_counts = {s["rule_id"]: {"evaluated": 0, "failed": 0} for s in snapshot}
    for row in rows:
        for s, checker in zip(snapshot, checkers):
            per_rule_counts[s["rule_id"]]["evaluated"] += 1
            failure = checker.check(
                row,
                row["_key"],
                existing_keys=existing_keys,
                ref_values=ref_sets.get(s["rule_id"], set()),
            )
            if failure:
                per_rule_counts[s["rule_id"]]["failed"] += 1
                failures.append((row, s, failure))

    execution_id = _new_id("exec")
    now = _now()
    conn.execute(
        """INSERT INTO executions (execution_id, batch_id, dataset, created_at,
                                   total, passed, failed, rule_versions_snapshot)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            execution_id,
            batch_id,
            dataset,
            now,
            len(rows),
            len(rows) - len({f[2].record_key for f in failures}),
            len({f[2].record_key for f in failures}),
            json.dumps(
                [
                    {"rule_id": s["rule_id"], "version": s["version"], "name": s["rule_name"]}
                    for s in snapshot
                ],
                ensure_ascii=False,
            ),
        ),
    )
    for s in snapshot:
        conn.execute(
            """INSERT INTO execution_rule_results
               (execution_id, rule_id, rule_version, rule_name, rule_type, evaluated, failed)
               VALUES (?,?,?,?,?,?,?)""",
            (
                execution_id,
                s["rule_id"],
                s["version"],
                s["rule_name"],
                s["rule_type"],
                per_rule_counts[s["rule_id"]]["evaluated"],
                per_rule_counts[s["rule_id"]]["failed"],
            ),
        )

    evaluated_keys = {row["_key"] for row in rows}
    failure_pairs = {(failure.record_key, snap["rule_id"]) for _, snap, failure in failures}
    _upsert_quarantine(conn, batch_id, execution_id, now, failures)
    _resolve_fixed(conn, batch_id, execution_id, now, dataset, evaluated_keys, failure_pairs)
    return get_execution(conn, execution_id)


def _upsert_quarantine(
    conn: sqlite3.Connection,
    batch_id: str,
    execution_id: str,
    now: str,
    failures: list[tuple[dict[str, Any], dict[str, Any], CheckFailure]],
) -> None:
    dataset = conn.execute(
        "SELECT dataset FROM batches WHERE batch_id=?", (batch_id,)
    ).fetchone()["dataset"]
    for row, snap, failure in failures:
        record_key, rule_id = failure.record_key, snap["rule_id"]
        existing = conn.execute(
            "SELECT * FROM quarantine_entries WHERE dataset=? AND record_key=? AND rule_id=?",
            (dataset, record_key, rule_id),
        ).fetchone()
        clean_payload = {k: v for k, v in row.items() if k != "_key"}
        if existing is None:
            cur = conn.execute(
                """INSERT INTO quarantine_entries
                   (dataset, record_key, rule_id, first_execution_id, latest_execution_id,
                    latest_batch_id, status, reason, payload, first_seen, updated_at)
                   VALUES (?, ?,?, ?,?, ?, 'open', ?,?, ?,?)""",
                (
                    dataset,
                    record_key,
                    rule_id,
                    execution_id,
                    execution_id,
                    batch_id,
                    failure.reason,
                    json.dumps(clean_payload, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            entry_id = cur.lastrowid
            event_type = "quarantined"
        else:
            entry_id = existing["entry_id"]
            ever_fixed = conn.execute(
                "SELECT 1 FROM quarantine_events WHERE entry_id=? AND event_type='fixed' LIMIT 1",
                (entry_id,),
            ).fetchone() is not None
            # re_quarantined only when the record had actually been fixed once;
            # a failure that persists across repair rounds stays "still_failing".
            event_type = "re_quarantined" if ever_fixed else "still_failing"
            conn.execute(
                """UPDATE quarantine_entries SET status='open', latest_execution_id=?,
                   latest_batch_id=?, reason=?, payload=?, updated_at=? WHERE entry_id=?""",
                (
                    execution_id,
                    batch_id,
                    failure.reason,
                    json.dumps(clean_payload, ensure_ascii=False),
                    now,
                    entry_id,
                ),
            )
        conn.execute(
            """INSERT INTO quarantine_events
               (entry_id, execution_id, batch_id, rule_version, event_type, reason, payload, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                entry_id,
                execution_id,
                batch_id,
                snap["version"],
                event_type,
                failure.reason,
                json.dumps(clean_payload, ensure_ascii=False),
                now,
            ),
        )


def _resolve_fixed(
    conn: sqlite3.Connection,
    batch_id: str,
    execution_id: str,
    now: str,
    dataset: str,
    evaluated_keys: set[str],
    failure_pairs: set[tuple[str, str]],
) -> None:
    """Open entries evaluated in this batch that no longer fail become 'fixed',
    and the transition is appended to the event history."""
    open_entries = conn.execute(
        "SELECT * FROM quarantine_entries WHERE dataset=? AND status='open'", (dataset,)
    ).fetchall()
    for entry in open_entries:
        if entry["record_key"] not in evaluated_keys:
            continue  # record not part of this batch; leave its state untouched
        if (entry["record_key"], entry["rule_id"]) in failure_pairs:
            continue
        conn.execute(
            "UPDATE quarantine_entries SET status='fixed', latest_execution_id=?, updated_at=? WHERE entry_id=?",
            (execution_id, now, entry["entry_id"]),
        )
        rule_version = conn.execute(
            "SELECT MAX(rule_version) AS v FROM quarantine_events WHERE entry_id=?",
            (entry["entry_id"],),
        ).fetchone()["v"]
        conn.execute(
            """INSERT INTO quarantine_events
               (entry_id, execution_id, batch_id, rule_version, event_type, reason, payload, created_at)
               VALUES (?,?,?,?, 'fixed', ?, ?,?)""",
            (
                entry["entry_id"],
                execution_id,
                batch_id,
                rule_version,
                "passes all current rules",
                entry["payload"],
                now,
            ),
        )


# ---------------------------------------------------------------------------
# Repair + replay
# ---------------------------------------------------------------------------
def repair_record(
    conn: sqlite3.Connection, dataset: str, record_key: str, payload: dict[str, Any]
) -> dict[str, Any]:
    """Stage a corrected payload on every open entry of the record.
    Nothing is re-validated here; validation happens on replay."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    entries = conn.execute(
        "SELECT * FROM quarantine_entries WHERE dataset=? AND record_key=? AND status='open'",
        (dataset, record_key),
    ).fetchall()
    if not entries:
        raise LookupError(f"no open quarantine entry for {dataset}/{record_key}")
    now = _now()
    for entry in entries:
        conn.execute(
            "UPDATE quarantine_entries SET payload=?, updated_at=? WHERE entry_id=?",
            (json.dumps(payload, ensure_ascii=False), now, entry["entry_id"]),
        )
    return {
        "dataset": dataset,
        "record_key": record_key,
        "updated_entries": len(entries),
        "payload": payload,
        "updated_at": now,
    }


def replay_failures(conn: sqlite3.Connection, parent_batch_id: str) -> dict[str, Any]:
    """Create a child batch containing only records still open in quarantine
    under the root lineage, using their staged (repaired) payloads."""
    parent = conn.execute(
        "SELECT * FROM batches WHERE batch_id=?", (parent_batch_id,)
    ).fetchone()
    if parent is None:
        raise LookupError(f"batch {parent_batch_id} not found")
    root = parent["root_batch_id"]
    dataset = parent["dataset"]

    # One logical record can carry several open entries (one per rule it
    # broke). A repair rewrites the payload on every one of them, but a record
    # must still appear exactly ONCE in the replay batch. Group by record_key
    # and take the most recently updated payload as the canonical version.
    open_rows = conn.execute(
        """SELECT qe.record_key, qe.payload, qe.updated_at
           FROM quarantine_entries qe
           JOIN batches b ON b.batch_id = qe.latest_batch_id
           WHERE qe.dataset=? AND qe.status='open' AND b.root_batch_id=?
           ORDER BY qe.record_key, qe.updated_at DESC, qe.entry_id DESC""",
        (dataset, root),
    ).fetchall()
    if not open_rows:
        raise EmptyReplayError(
            f"no open quarantine records under root batch {root}; nothing to replay"
        )

    canonical: dict[str, str] = {}
    for row in open_rows:
        # first row per key wins because of the DESC ordering above
        canonical.setdefault(row["record_key"], row["payload"])

    batch_id = _new_id("batch")
    now = _now()
    rows = [(json.loads(payload), key) for key, payload in sorted(canonical.items())]
    conn.execute(
        """INSERT INTO batches (batch_id, dataset, source, parent_batch_id, root_batch_id,
                                status, record_count, created_at)
           VALUES (?,?,?,?,?, 'open', ?,?)""",
        (batch_id, dataset, REPLAY_BATCH, parent_batch_id, root, len(rows), now),
    )
    for payload, key in rows:
        conn.execute(
            "INSERT INTO records (dataset, batch_id, record_key, payload, created_at) VALUES (?,?,?,?,?)",
            (dataset, batch_id, key, json.dumps(payload, ensure_ascii=False), now),
        )
    execution = _run_execution(conn, batch_id)
    return {"batch": get_batch(conn, batch_id), "execution": execution}


class EmptyReplayError(Exception):
    """Raised when a replay is requested but no open records remain."""


# ---------------------------------------------------------------------------
# Read models + gate
# ---------------------------------------------------------------------------
def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict[str, Any]:
    batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
    if batch is None:
        raise LookupError(f"batch {batch_id} not found")
    out = dict(batch)
    out["children"] = [
        r["batch_id"]
        for r in conn.execute(
            "SELECT batch_id FROM batches WHERE parent_batch_id=? ORDER BY created_at",
            (batch_id,),
        ).fetchall()
    ]
    return out


def list_batches(conn: sqlite3.Connection, dataset: str | None = None) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT batch_id FROM batches WHERE (? IS NULL OR dataset=?) ORDER BY created_at",
        (dataset, dataset),
    ).fetchall()
    return [get_batch(conn, r["batch_id"]) for r in rows]


def get_execution(conn: sqlite3.Connection, execution_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM executions WHERE execution_id=?", (execution_id,)
    ).fetchone()
    if row is None:
        raise LookupError(f"execution {execution_id} not found")
    rule_results = conn.execute(
        "SELECT * FROM execution_rule_results WHERE execution_id=? ORDER BY id",
        (execution_id,),
    ).fetchall()
    out = dict(row)
    out["rule_versions_snapshot"] = json.loads(row["rule_versions_snapshot"])
    out["rule_results"] = [dict(r) for r in rule_results]
    return out


def list_executions(conn: sqlite3.Connection, batch_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT execution_id FROM executions WHERE batch_id=? ORDER BY created_at", (batch_id,)
    ).fetchall()
    return [get_execution(conn, r["execution_id"]) for r in rows]


def list_quarantine(
    conn: sqlite3.Connection, dataset: str | None = None, status: str | None = None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT * FROM quarantine_entries
           WHERE (? IS NULL OR dataset=?) AND (? IS NULL OR status=?)
           ORDER BY updated_at DESC""",
        (dataset, dataset, status, status),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["payload"] = json.loads(r["payload"])
        item["history"] = get_entry_history(conn, r["entry_id"])
        out.append(item)
    return out


def get_entry_history(conn: sqlite3.Connection, entry_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM quarantine_events WHERE entry_id=? ORDER BY event_id", (entry_id,)
    ).fetchall()
    return [
        {
            "event_type": r["event_type"],
            "rule_version": r["rule_version"],
            "reason": r["reason"],
            "batch_id": r["batch_id"],
            "execution_id": r["execution_id"],
            "payload": json.loads(r["payload"]),
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def failure_samples(
    conn: sqlite3.Connection, batch_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """Failed records for a batch: open entries whose latest batch is this one."""
    rows = conn.execute(
        "SELECT * FROM quarantine_entries WHERE latest_batch_id=? ORDER BY entry_id LIMIT ?",
        (batch_id, limit),
    ).fetchall()
    return [
        {
            "entry_id": r["entry_id"],
            "record_key": r["record_key"],
            "rule_id": r["rule_id"],
            "status": r["status"],
            "reason": r["reason"],
            "payload": json.loads(r["payload"]),
        }
        for r in rows
    ]


def evaluate_gate(conn: sqlite3.Connection, root_batch_id: str) -> dict[str, Any]:
    root = conn.execute(
        "SELECT * FROM batches WHERE batch_id=?", (root_batch_id,)
    ).fetchone()
    if root is None:
        raise LookupError(f"batch {root_batch_id} not found")
    lineage = conn.execute(
        "SELECT * FROM batches WHERE root_batch_id=? ORDER BY created_at", (root_batch_id,)
    ).fetchall()
    batch_ids = [b["batch_id"] for b in lineage]
    placeholders = ",".join("?" for _ in batch_ids)

    open_rows = conn.execute(
        f"SELECT COUNT(*) AS c FROM quarantine_entries WHERE status='open' AND latest_batch_id IN ({placeholders})",
        batch_ids,
    ).fetchone()["c"]
    total_entries = conn.execute(
        f"SELECT COUNT(*) AS c FROM quarantine_entries WHERE latest_batch_id IN ({placeholders})",
        batch_ids,
    ).fetchone()["c"]

    exec_rows = conn.execute(
        f"SELECT * FROM executions WHERE batch_id IN ({placeholders}) ORDER BY created_at",
        batch_ids,
    ).fetchall()
    rule_rollup: dict[tuple[str, str], dict[str, Any]] = {}
    total_evaluated = 0
    for ex in exec_rows:
        for rr in conn.execute(
            "SELECT * FROM execution_rule_results WHERE execution_id=?", (ex["execution_id"],)
        ).fetchall():
            total_evaluated += rr["evaluated"]
            key = (rr["rule_id"], rr["rule_name"])
            slot = rule_rollup.setdefault(
                key,
                {
                    "rule_id": rr["rule_id"],
                    "rule_name": rr["rule_name"],
                    "rule_type": rr["rule_type"],
                    "latest_version": rr["rule_version"],
                    "evaluated": 0,
                    "failed": 0,
                },
            )
            slot["latest_version"] = rr["rule_version"]
            slot["evaluated"] += rr["evaluated"]
            slot["failed"] += rr["failed"]

    if open_rows == 0 and total_evaluated == 0:
        decision, reason = "warn", "no rules evaluated: data is unvalidated"
    elif open_rows == 0:
        decision, reason = "pass", "all quarantine entries resolved"
    else:
        decision, reason = "block", f"{open_rows} open quarantine record(s) remain"

    return {
        "root_batch_id": root_batch_id,
        "dataset": root["dataset"],
        "decision": decision,
        "reason": reason,
        "open_quarantine": open_rows,
        "total_quarantine_entries": total_entries,
        "lineage": [dict(b) for b in lineage],
        "rule_rollup": list(rule_rollup.values()),
        "executions": [
            {
                "execution_id": e["execution_id"],
                "batch_id": e["batch_id"],
                "total": e["total"],
                "passed": e["passed"],
                "failed": e["failed"],
                "snapshot": json.loads(e["rule_versions_snapshot"]),
                "created_at": e["created_at"],
            }
            for e in exec_rows
        ],
    }


def publish_batch(conn: sqlite3.Connection, root_batch_id: str) -> dict[str, Any]:
    gate = evaluate_gate(conn, root_batch_id)
    if gate["decision"] == "block":
        raise GateBlockedError(gate)
    conn.execute(
        "UPDATE batches SET status='published' WHERE root_batch_id=? AND status='open'",
        (root_batch_id,),
    )
    gate["lineage"] = [get_batch(conn, b["batch_id"]) for b in gate["lineage"]]
    gate["published"] = True
    return gate


class GateBlockedError(Exception):
    def __init__(self, gate: dict[str, Any]):
        self.gate = gate
        super().__init__(gate["reason"])
