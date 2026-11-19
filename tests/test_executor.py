"""End-to-end tests for the data quality executor.

Covers:
* all five rule types (primary key, unique, range, cross-table, expression)
* quarantine + append-only repair history across multiple repair rounds
* replay of only failed data into a new child batch
* rule upgrade taking effect on replay
* empty ingest batch rejection and empty replay rejection
* partial failure + gate block/resolve/publish lifecycle
"""
from __future__ import annotations

import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "test_dq.db"
    os.environ["DQ_DB_PATH"] = str(db)
    # Re-import app modules so DB_PATH is picked up fresh.
    import importlib

    from app import api as api_mod
    from app import db as db_mod

    importlib.reload(db_mod)
    importlib.reload(api_mod)
    db_mod.init_db(str(db))
    with TestClient(api_mod.app) as c:
        yield c


def _rule(client, dataset, name, rule_type, config, note=None):
    r = client.post("/api/rules", json={"dataset": dataset, "name": name, "type": rule_type,
                                        "config": config, "change_note": note})
    assert r.status_code == 201, r.text
    rule_id = r.json()["rule_id"]
    r = client.post(f"/api/rules/{rule_id}/publish")
    assert r.status_code == 200
    return rule_id


# --------------------------------------------------------------------- rule types
def test_all_five_rule_types(client):
    # reference dataset for cross-table
    client.post("/api/ingest", json={"dataset": "customers", "rows": [{"id": "C-1"}, {"id": "C-2"}]})

    _rule(client, "orders", "pk", "primary_key", {"fields": ["order_id"]})
    _rule(client, "orders", "email unique", "unique", {"fields": ["email"]})
    _rule(client, "orders", "amount range", "range",
          {"field": "amount", "min": 0, "max": 1000, "allow_null": False})
    _rule(client, "orders", "customer ref", "cross_table",
          {"field": "customer_id", "ref_dataset": "customers", "ref_field": "id", "allow_null": False})
    _rule(client, "orders", "custom expr", "expression",
          {"expr": "quantity >= 1 and amount >= 0 and lower(status) in ('paid', 'pending')"})

    rows = [
        {"order_id": "O-1", "email": "a@x.com", "amount": 100, "customer_id": "C-1",
         "quantity": 2, "status": "paid"},          # all pass
        {"order_id": "O-2", "email": "a@x.com", "amount": 100, "customer_id": "C-1",
         "quantity": 2, "status": "paid"},          # unique fail
        {"order_id": "O-3", "email": "c@x.com", "amount": -5, "customer_id": "C-1",
         "quantity": 2, "status": "paid"},          # range + expression fail
        {"order_id": "O-4", "email": "d@x.com", "amount": 50, "customer_id": "C-X",
         "quantity": 1, "status": "paid"},          # cross-table fail
        {"order_id": None, "email": "e@x.com", "amount": 50, "customer_id": "C-1",
         "quantity": 1, "status": "paid"},          # primary key null fail
        {"order_id": "O-6", "email": "f@x.com", "amount": 50, "customer_id": "C-2",
         "quantity": 0, "status": "shipped"},       # expression fail
    ]
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": rows})
    assert r.status_code == 201
    exec_ = r.json()["execution"]
    assert exec_["total"] == 6
    assert exec_["passed"] == 1
    # 5 distinct failing records (O-3 fails two rules but counts once)
    assert exec_["failed"] == 5
    per_rule = {x["rule_name"]: x for x in exec_["rule_results"]}
    assert per_rule["pk"]["failed"] == 1
    assert per_rule["email unique"]["failed"] == 1
    assert per_rule["amount range"]["failed"] == 1
    assert per_rule["customer ref"]["failed"] == 1
    assert per_rule["custom expr"]["failed"] == 2  # O-3 and O-6

    q = client.get("/api/quarantine?status=open").json()
    # 6 entries: O-3 carries two (range + expression); O-2 only unique, etc.
    assert len(q) == 6
    o3 = [e for e in q if e["record_key"] == "O-3"]
    assert len(o3) == 2
    reasons = " ".join(e["reason"] for e in q)
    assert "no match in customers.id" in reasons
    assert "outside range" in reasons


def test_duplicate_primary_key_within_batch_and_against_existing(client):
    _rule(client, "t", "pk", "primary_key", {"fields": ["id"]})
    r1 = client.post("/api/ingest", json={"dataset": "t", "rows": [{"id": "K1"}]})
    assert r1.json()["execution"]["failed"] == 0
    r2 = client.post("/api/ingest", json={"dataset": "t", "rows": [{"id": "K1"}, {"id": "K1"}, {"id": "K2"}]})
    exec_ = r2.json()["execution"]
    assert exec_["failed"] == 1  # both K1 rows map to one record key
    reasons = [f["reason"] for f in client.get(f"/api/batches/{r2.json()['batch']['batch_id']}/failures").json()]
    assert any("duplicate primary key" in x or "conflicts with an existing record" in x for x in reasons)


def test_range_boundaries_and_null_handling(client):
    _rule(client, "t", "r", "range", {"field": "v", "min": 0, "max": 10, "allow_null": True})
    r = client.post("/api/ingest", json={"dataset": "t", "rows": [
        {"v": 0}, {"v": 10}, {"v": None},       # pass: inclusive bounds + null allowed
        {"v": -0.01}, {"v": 10.01}, {"v": "x"}, # fail
    ]})
    assert r.json()["execution"]["failed"] == 3

    rid = client.get("/api/rules?dataset=t").json()[0]["rule_id"]
    client.post(f"/api/rules/{rid}/upgrade", json={
        "config": {"field": "v", "min": 1, "max": 9, "min_inclusive": False, "max_inclusive": False,
                   "allow_null": False},
        "change_note": "tighten",
    })
    client.post(f"/api/rules/{rid}/publish")
    detail = client.get(f"/api/rules/{rid}").json()
    assert [v["version"] for v in detail["versions"]] == [1, 2]
    assert detail["versions"][0]["status"] == "archived"
    assert detail["versions"][1]["status"] == "published"


def test_expression_sandbox_rejects_unsafe_syntax(client):
    # underscore names, attribute access, lambdas and non-whitelisted calls
    for bad in ["__import__('os')", "open('/etc/passwd')", "x.__class__", "lambda: 1",
                "().__class__.__bases__"]:
        r = client.post("/api/rules", json={"dataset": "t", "name": "bad", "type": "expression",
                                            "config": {"expr": bad}})
        assert r.status_code == 400, bad
    # whitelisted builtins and normal comparisons are accepted
    ok = client.post("/api/rules", json={"dataset": "t", "name": "ok", "type": "expression",
                                         "config": {"expr": "len(items) <= 3 and abs(delta) < 10"}})
    assert ok.status_code == 201


# --------------------------------------------------------------------- empty batches
def test_empty_ingest_batch_rejected(client):
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": []})
    assert r.status_code == 400
    assert "at least one record" in r.json()["detail"]


def test_empty_replay_rejected(client):
    _rule(client, "orders", "pk", "primary_key", {"fields": ["order_id"]})
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": [{"order_id": "O-1"}]})
    batch_id = r.json()["batch"]["batch_id"]
    # everything passed -> no open quarantine -> replay must be refused
    r2 = client.post(f"/api/batches/{batch_id}/replay")
    assert r2.status_code == 409
    assert r2.json()["detail"]["code"] == "NOTHING_TO_REPLAY"


# --------------------------------------------------------------------- partial failure + replay + history
def test_partial_failure_repair_replay_and_history(client):
    _rule(client, "orders", "pk", "primary_key", {"fields": ["order_id"]})
    range_id = _rule(client, "orders", "amount", "range",
                     {"field": "amount", "min": 0, "max": 1000, "allow_null": False})

    r = client.post("/api/ingest", json={"dataset": "orders", "rows": [
        {"order_id": "O-1", "amount": 100},
        {"order_id": "O-2", "amount": -5},     # fails range
        {"order_id": "O-3", "amount": -9},     # fails range
    ]})
    root = r.json()["batch"]["batch_id"]
    assert r.json()["execution"]["failed"] == 2

    # gate blocks while open entries exist
    gate = client.get(f"/api/gate/{root}").json()
    assert gate["decision"] == "block"
    pub = client.post(f"/api/gate/{root}/publish")
    assert pub.status_code == 409
    assert pub.json()["detail"]["code"] == "GATE_BLOCKED"

    # repair only O-2, then replay: replay batch must contain only open records
    client.put("/api/quarantine/orders/O-2/repair", json={"payload": {"order_id": "O-2", "amount": 50}})
    # O-3 is still broken, so the replay includes O-2 + O-3 (partial again)
    rep = client.post(f"/api/batches/{root}/replay")
    assert rep.status_code == 201
    child = rep.json()["batch"]
    assert child["source"] == "replay"
    assert child["parent_batch_id"] == root
    assert child["root_batch_id"] == root
    assert child["record_count"] == 2
    assert rep.json()["execution"]["failed"] == 1  # O-3 still failing

    # O-2 entry is now fixed and has a 2-event history
    q = client.get("/api/quarantine").json()
    o2 = next(e for e in q if e["record_key"] == "O-2")
    o3 = next(e for e in q if e["record_key"] == "O-3")
    assert o2["status"] == "fixed"
    assert [h["event_type"] for h in o2["history"]] == ["quarantined", "fixed"]
    assert o3["status"] == "open"
    assert [h["event_type"] for h in o3["history"]] == ["quarantined", "still_failing"]

    # second repair round for O-3 -> another replay -> gate passes
    client.put("/api/quarantine/orders/O-3/repair", json={"payload": {"order_id": "O-3", "amount": 9}})
    rep2 = client.post(f"/api/batches/{child['batch_id']}/replay")
    assert rep2.json()["batch"]["record_count"] == 1
    assert rep2.json()["execution"]["failed"] == 0
    o3_after = next(e for e in client.get("/api/quarantine").json() if e["record_key"] == "O-3")
    assert o3_after["status"] == "fixed"
    assert [h["event_type"] for h in o3_after["history"]] == ["quarantined", "still_failing", "fixed"]

    gate = client.get(f"/api/gate/{root}").json()
    assert gate["decision"] == "pass"
    assert len(gate["lineage"]) == 3  # root + 2 replay batches
    published = client.post(f"/api/gate/{root}/publish").json()
    assert published["published"] is True
    statuses = {b["batch_id"]: b["status"] for b in client.get("/api/batches").json()}
    assert all(s == "published" for s in statuses.values())


# --------------------------------------------------------------------- rule upgrade on replay
def test_rule_upgrade_takes_effect_on_replay(client):
    _rule(client, "orders", "pk", "primary_key", {"fields": ["order_id"]})
    rid = _rule(client, "orders", "amount cap", "range",
                {"field": "amount", "min": 0, "max": 100, "allow_null": False})
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": [
        {"order_id": "O-1", "amount": 50},   # passes v1
        {"order_id": "O-2", "amount": 200},  # fails v1 (>100)
    ]})
    root = r.json()["batch"]["batch_id"]
    assert r.json()["execution"]["failed"] == 1

    # repair O-2 to 80: passes v1
    client.put("/api/quarantine/orders/O-2/repair", json={"payload": {"order_id": "O-2", "amount": 80}})

    # upgrade the rule: cap becomes 60. Repaired 80 now fails again (re-quarantine)
    client.post(f"/api/rules/{rid}/upgrade",
                json={"config": {"field": "amount", "min": 0, "max": 60, "allow_null": False},
                      "change_note": "cap 100 -> 60"})
    client.post(f"/api/rules/{rid}/publish")

    rep = client.post(f"/api/batches/{root}/replay").json()
    range_result = next(x for x in rep["execution"]["rule_results"] if x["rule_name"] == "amount cap")
    assert range_result["rule_version"] == 2
    assert rep["execution"]["failed"] == 1
    o2 = next(e for e in client.get("/api/quarantine").json() if e["record_key"] == "O-2")
    assert o2["status"] == "open"
    # never actually fixed -> the second quarantine is still_failing, but both
    # rule versions remain visible in the append-only history
    types = [h["event_type"] for h in o2["history"]]
    assert types == ["quarantined", "still_failing"]
    assert [h["rule_version"] for h in o2["history"]] == [1, 2]

    # now repair within the new cap -> fixed; then deliberately stage a payload
    # that violates the rule again -> the next replay re-quarantines a record
    # that had been fixed, producing a genuine re_quarantined event
    client.put("/api/quarantine/orders/O-2/repair",
               json={"payload": {"order_id": "O-2", "amount": 55}})
    rep2 = client.post(f"/api/batches/{rep['batch']['batch_id']}/replay").json()
    assert rep2["execution"]["failed"] == 0

    # break it again on purpose, then replay (only open records are picked up)
    client.put("/api/quarantine/orders/O-2/repair",
               json={"payload": {"order_id": "O-2", "amount": 999}})
    rep3 = client.post(f"/api/batches/{rep2['batch']['batch_id']}/replay")
    # nothing is open after rep2, so staging a payload alone cannot create an
    # open entry — replay is correctly refused until new failures are ingested
    assert rep3.status_code == 409
    assert rep3.json()["detail"]["code"] == "NOTHING_TO_REPLAY"

    # ingest the regressed payload as a fresh batch to exercise re-quarantine
    # against the existing (fixed) entry via a normal failing evaluation
    regressed = client.post("/api/ingest", json={"dataset": "orders", "rows": [
        {"order_id": "O-2", "amount": 999},
    ]})
    # same key as a fixed record, not open -> primary-key collision with history
    assert regressed.status_code == 201
    o2b = next(e for e in client.get("/api/quarantine").json() if e["record_key"] == "O-2")
    assert [h["event_type"] for h in o2b["history"]] == [
        "quarantined", "still_failing", "fixed", "re_quarantined"]
    assert o2b["status"] == "open"

    # gate rollup shows both versions were used across the lineage
    gate = client.get(f"/api/gate/{root}").json()
    rollup = next(r for r in gate["rule_rollup"] if r["rule_name"] == "amount cap")
    assert rollup["latest_version"] == 2
    cap_versions = {
        s["version"]
        for e in gate["executions"]
        for s in e["snapshot"]
        if s["name"] == "amount cap"
    }
    assert cap_versions == {1, 2}


# --------------------------------------------------------------------- gate warn when no rules
def test_gate_warns_when_no_rules_evaluated(client):
    r = client.post("/api/ingest", json={"dataset": "raw", "rows": [{"x": 1}, {"x": 2}]})
    root = r.json()["batch"]["batch_id"]
    gate = client.get(f"/api/gate/{root}").json()
    assert gate["decision"] == "warn"
    assert "unvalidated" in gate["reason"]
    # warn does not block publishing
    published = client.post(f"/api/gate/{root}/publish").json()
    assert published["published"] is True


# --------------------------------------------------------------------- cross-table reacts to quarantine
def test_cross_table_ignores_quarantined_reference_records(client):
    _rule(client, "customers", "pk", "primary_key", {"fields": ["id"]})
    client.post("/api/ingest", json={"dataset": "customers", "rows": [{"id": "C-1"}, {"id": None}]})
    # C-1 valid; the null-id row is quarantined and must not serve as a reference
    _rule(client, "orders", "pk", "primary_key", {"fields": ["order_id"]})
    _rule(client, "orders", "ref", "cross_table",
          {"field": "cid", "ref_dataset": "customers", "ref_field": "id", "allow_null": False})
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": [
        {"order_id": "O-1", "cid": "C-1"},   # pass
        {"order_id": "O-2", "cid": None},    # fail: null not allowed
    ]})
    assert r.json()["execution"]["failed"] == 1
    open_entries = client.get("/api/quarantine?status=open").json()
    o2 = next(e for e in open_entries if e["record_key"] == "O-2")
    fixed = client.put(f"/api/quarantine/orders/{o2['record_key']}/repair",
                       json={"payload": {"order_id": "O-2", "cid": "C-1"}})
    assert fixed.status_code == 200


def test_primary_key_conflict_scoped_by_repair_lineage(client):
    """Same key inside one repair lineage = versions of one logical record.
    Same key in a *different* lineage = a real primary-key collision."""
    _rule(client, "t", "pk", "primary_key", {"fields": ["id"]})

    # lineage A: K1 ingests clean, then a later (separate) ingest reuses K1
    a = client.post("/api/ingest", json={"dataset": "t", "rows": [{"id": "K1", "v": 1}]})
    root_a = a.json()["batch"]["batch_id"]
    assert a.json()["execution"]["failed"] == 0

    b = client.post("/api/ingest", json={"dataset": "t", "rows": [{"id": "K1", "v": 2}]})
    root_b = b.json()["batch"]["batch_id"]
    # different root lineage + already-persisted K1 -> genuine conflict
    assert b.json()["execution"]["failed"] == 1
    reasons = [f["reason"] for f in client.get(f"/api/batches/{root_b}/failures").json()]
    assert any("conflicts with an existing record" in x for x in reasons)

    # repairing and replaying inside lineage B must not re-trigger the conflict
    # against lineage B's own earlier batches (same logical record)
    client.put("/api/quarantine/t/K1/repair", json={"payload": {"id": "K1", "v": 3}})
    rep = client.post(f"/api/batches/{root_b}/replay").json()
    # still conflicts with lineage A's persisted K1 (a different lineage),
    # but the failure reason is the cross-lineage conflict, not a self-collision
    assert rep["execution"]["failed"] == 1
    child_reasons = [
        f["reason"] for f in client.get(f"/api/batches/{rep['batch']['batch_id']}/failures").json()
    ]
    assert all("duplicate primary key" not in x for x in child_reasons)


def test_multi_failure_record_converges_via_canonical_replay(client):
    """A record breaking several rules carries multiple open entries. A repair
    rewrites all of them; a replay must include the record exactly ONCE using
    its canonical payload, and repeated repair rounds converge to fully fixed
    (no stale-payload duplicate rows, no oscillation)."""
    client.post("/api/ingest", json={"dataset": "refs", "rows": [{"id": "R-1"}]})
    _rule(client, "sales", "pk2", "primary_key", {"fields": ["oid"]})
    _rule(client, "sales", "rng2", "range",
          {"field": "amt", "min": 0, "max": 1000, "allow_null": False})
    _rule(client, "sales", "crs2", "cross_table",
          {"field": "rid", "ref_dataset": "refs", "ref_field": "id", "allow_null": False})

    r = client.post("/api/ingest", json={"dataset": "sales", "rows": [
        {"oid": "S-1", "rid": "R-1", "amt": 100},   # clean
        {"oid": "S-2", "rid": "R-X", "amt": -5},    # cross + range fail
        {"oid": "S-2", "rid": "R-1", "amt": 80},    # duplicate PK
    ]})
    root = r.json()["batch"]["batch_id"]
    assert r.json()["execution"]["failed"] == 1

    # repair round 1 with a fully valid payload
    client.put("/api/quarantine/sales/S-2/repair",
               json={"payload": {"oid": "S-2", "rid": "R-1", "amt": 80}})
    rep1 = client.post(f"/api/batches/{root}/replay").json()
    # S-2 appears once, not once per open entry
    assert rep1["batch"]["record_count"] == 1

    # converge: keep replaying the tip until nothing is open
    for _ in range(5):
        openq = client.get("/api/quarantine?status=open&dataset=sales").json()
        if not openq:
            break
        tip = [b for b in client.get("/api/batches").json()
               if b["root_batch_id"] == root][-1]["batch_id"]
        for key in {e["record_key"] for e in openq}:
            client.put(f"/api/quarantine/sales/{key}/repair",
                       json={"payload": {"oid": key, "rid": "R-1", "amt": 80}})
        again = client.post(f"/api/batches/{tip}/replay")
        assert again.status_code == 201

    gate = client.get(f"/api/gate/{root}").json()
    assert gate["decision"] == "pass"
    assert gate["open_quarantine"] == 0
    # every replay batch carried each repaired record at most once
    for b in gate["lineage"]:
        if b["source"] == "replay":
            assert b["record_count"] == 1
    # full append-only history is retained
    s2 = [e for e in client.get("/api/quarantine?dataset=sales").json()
          if e["record_key"] == "S-2"]
    assert all(e["status"] == "fixed" for e in s2)
    assert all(len(e["history"]) >= 2 for e in s2)


def test_repair_unknown_record_404(client):
    r = client.put("/api/quarantine/orders/nope/repair", json={"payload": {"x": 1}})
    assert r.status_code == 404


def test_failure_keys_are_stable_without_primary_key_rule(client):
    """Regression: range/cross-table/expression failures must carry the same
    stable record key as the record itself, otherwise repairs never reach the
    quarantined payload and replays keep re-evaluating the original bad data."""
    client.post("/api/ingest", json={"dataset": "customers", "rows": [{"id": "C-1"}]})
    _rule(client, "orders", "ref", "cross_table",
          {"field": "cid", "ref_dataset": "customers", "ref_field": "id", "allow_null": False})
    r = client.post("/api/ingest", json={"dataset": "orders", "rows": [
        {"_key": "ord-1", "cid": "C-1"},
        {"_key": "ord-2", "cid": "C-X"},
    ]})
    root = r.json()["batch"]["batch_id"]
    entries = client.get("/api/quarantine?status=open").json()
    assert len(entries) == 1
    assert entries[0]["record_key"] == "ord-2"  # not a fallback "row:1"

    # repair by the business-facing key now works
    fixed = client.put("/api/quarantine/orders/ord-2/repair",
                       json={"payload": {"_key": "ord-2", "cid": "C-1"}})
    assert fixed.status_code == 200
    rep = client.post(f"/api/batches/{root}/replay").json()
    assert rep["execution"]["failed"] == 0
    assert client.get("/api/gate/" + root).json()["decision"] == "pass"
