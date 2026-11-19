#!/usr/bin/env python
"""End-to-end smoke script driven through the real ASGI HTTP stack
(FastAPI TestClient), mirroring the UI demo flow."""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["DQ_DB_PATH"] = _tmp.name

from fastapi.testclient import TestClient  # noqa: E402

from app.api import app  # noqa: E402
from app.db import init_db  # noqa: E402

init_db(_tmp.name)
client = TestClient(app)


def call(method, path, body=None):
    r = client.request(method, path, json=body)
    return r.status_code, r.json()


checks = []


def check(name, cond, detail=""):
    checks.append((name, cond, detail))
    print(f"{'✔' if cond else '✘'} {name} {detail}")


# 1. reference dataset + five-rule setup (4 on orders; unique covered in pytest)
call("POST", "/api/ingest", {"dataset": "customers", "rows": [{"id": "C-001"}, {"id": "C-002"}]})
rule_ids = {}
for name, rtype, cfg in [
    ("订单主键", "primary_key", {"fields": ["order_id"]}),
    ("金额范围", "range", {"field": "amount", "min": 0, "max": 10000, "allow_null": False}),
    ("客户外键", "cross_table", {"field": "customer_id", "ref_dataset": "customers",
                                  "ref_field": "id", "allow_null": False}),
    ("表达式", "expression", {"expr": "quantity >= 1 and amount >= 0"}),
]:
    _, r = call("POST", "/api/rules", {"dataset": "orders", "name": name, "type": rtype, "config": cfg})
    rule_ids[name] = r["rule_id"]
    call("POST", f"/api/rules/{r['rule_id']}/publish")

# 2. partial-failure ingest. O-3 breaks TWO rules (range + expression) so it
# carries two open entries; O-2 breaks cross-table; O-4 breaks expression.
_, ing = call("POST", "/api/ingest", {"dataset": "orders", "rows": [
    {"order_id": "O-1", "customer_id": "C-001", "amount": 100, "quantity": 2},
    {"order_id": "O-2", "customer_id": "C-999", "amount": 50, "quantity": 1},
    {"order_id": "O-3", "customer_id": "C-002", "amount": -10, "quantity": 1},
    {"order_id": "O-4", "customer_id": "C-001", "amount": 200, "quantity": 0},
]})
root = ing["batch"]["root_batch_id"]
check("接入批次部分失败", ing["execution"]["failed"] == 3,
      f"total={ing['execution']['total']} failed={ing['execution']['failed']}")

# 3. gate blocks
_, gate = call("GET", f"/api/gate/{root}")
check("门禁拦截 open 隔离", gate["decision"] == "block", gate["reason"])
code, blocked = call("POST", f"/api/gate/{root}/publish")
check("block 状态禁止发布", code == 409 and blocked["detail"]["code"] == "GATE_BLOCKED")

# 4. repair only O-2 and O-4, then replay. O-3 is untouched and must still
#    fail (partial failure). Each repaired record appears exactly once even
#    though O-3 has two open entries (canonical payload per record key).
call("PUT", "/api/quarantine/orders/O-2/repair",
     {"payload": {"order_id": "O-2", "customer_id": "C-001", "amount": 50, "quantity": 1}})
call("PUT", "/api/quarantine/orders/O-4/repair",
     {"payload": {"order_id": "O-4", "customer_id": "C-001", "amount": 200, "quantity": 2}})
_, rep1 = call("POST", f"/api/batches/{root}/replay")
check("回放1只含失败记录且按记录键归一",
      rep1["batch"]["source"] == "replay" and rep1["batch"]["parent_batch_id"] == root
      and rep1["batch"]["record_count"] == 3 and rep1["execution"]["failed"] == 1,
      f"records={rep1['batch']['record_count']} failed={rep1['execution']['failed']}")

# 5. rule upgrade: cap 10000 -> 60. Repair O-3 to amount 80 (valid under v1,
#    but it violates the new v2 cap) -> replay re-evaluates with v2 and fails.
call("POST", f"/api/rules/{rule_ids['金额范围']}/upgrade",
     {"config": {"field": "amount", "min": 0, "max": 60, "allow_null": False}, "change_note": "cap 60"})
code, _ = call("POST", f"/api/rules/{rule_ids['金额范围']}/publish")
check("规则 v2 发布成功", code == 200)
call("PUT", "/api/quarantine/orders/O-3/repair",
     {"payload": {"order_id": "O-3", "customer_id": "C-002", "amount": 80, "quantity": 1}})
_, rep2 = call("POST", f"/api/batches/{rep1['batch']['batch_id']}/replay")
range_res = next(r for r in rep2["execution"]["rule_results"] if r["rule_name"] == "金额范围")
check("规则升级在回放中生效（v2 判定 80 超限）",
      range_res["rule_version"] == 2 and rep2["execution"]["failed"] == 1,
      f"range_rule_version={range_res['rule_version']}")

# 6. repair O-3 within the new cap -> replay #3 fully passes
call("PUT", "/api/quarantine/orders/O-3/repair",
     {"payload": {"order_id": "O-3", "customer_id": "C-002", "amount": 50, "quantity": 1}})
_, rep3 = call("POST", f"/api/batches/{rep2['batch']['batch_id']}/replay")
check("回放3全部通过", rep3["execution"]["failed"] == 0)

# 7. gate now passes; full lineage publish (root + 3 replay batches)
_, gate2 = call("GET", f"/api/gate/{root}")
versions = sorted({s["version"] for e in gate2["executions"] for s in e["snapshot"]
                   if s["name"] == "金额范围"})
check("门禁放行且血缘含4批次", gate2["decision"] == "pass" and len(gate2["lineage"]) == 4,
      f"decision={gate2['decision']} versions={versions}")
check("门禁汇总保留 v1/v2 两个规则版本", versions == [1, 2])
code, pub = call("POST", f"/api/gate/{root}/publish")
check("整棵血缘发布", code == 200 and pub["published"]
      and all(b["status"] == "published" for b in pub["lineage"]))

# 8. multi-round history retained on O-3's range entry. The range entry never
# reached "fixed" before its final pass, so every repeated failure is
# still_failing; re_quarantined is reserved for fixed->failing regressions
# (covered by pytest test_rule_upgrade_takes_effect_on_replay).
_, q = call("GET", "/api/quarantine?dataset=orders")
o3_range = next(e for e in q if e["record_key"] == "O-3" and "outside range" in e["reason"])
types = [h["event_type"] for h in o3_range["history"]]
check("同一记录多轮修复历史完整保留",
      types == ["quarantined", "still_failing", "still_failing", "fixed"],
      str(types))
check("历史中保留规则版本轨迹", [h["rule_version"] for h in o3_range["history"]] == [1, 1, 2, 2])

# 9. empty batch / empty replay guards
code, empty = call("POST", "/api/ingest", {"dataset": "orders", "rows": []})
check("空接入批次被拒绝", code == 400 and "at least one record" in empty["detail"])
code, er = call("POST", f"/api/batches/{root}/replay")
check("空回放被拒绝", code == 409 and er["detail"]["code"] == "NOTHING_TO_REPLAY")

# 10. unvalidated dataset -> gate warns but is publishable
_, raw = call("POST", "/api/ingest", {"dataset": "raw", "rows": [{"x": 1}]})
_, raw_gate = call("GET", f"/api/gate/{raw['batch']['root_batch_id']}")
check("无规则时门禁 warn", raw_gate["decision"] == "warn")

failed = [c for c in checks if not c[1]]
print(f"\n{len(checks) - len(failed)}/{len(checks)} smoke checks passed")
sys.exit(1 if failed else 0)
