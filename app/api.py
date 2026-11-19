"""REST API for the data quality executor."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlite3 import Connection

from . import executor
from .db import get_conn, init_db
from .expr import ExpressionError
from .rules import RuleConfigError


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(os.environ.get("DQ_DB_PATH"))
    yield


app = FastAPI(title="Pointwise Data Quality Executor", version="1.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- models
class RuleCreate(BaseModel):
    dataset: str
    name: str
    type: str
    config: dict[str, Any]
    change_note: str | None = None


class RuleUpgrade(BaseModel):
    config: dict[str, Any]
    change_note: str


class IngestRequest(BaseModel):
    dataset: str
    rows: list[dict[str, Any]]


class RepairRequest(BaseModel):
    payload: dict[str, Any]


# ---------------------------------------------------------------- exception mapping
@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _wrap(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (RuleConfigError, ValueError, ExpressionError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except executor.EmptyReplayError as exc:
        raise HTTPException(status_code=409, detail={"code": "NOTHING_TO_REPLAY", "message": str(exc)})
    except executor.GateBlockedError as exc:
        raise HTTPException(status_code=409, detail={"code": "GATE_BLOCKED", "gate": exc.gate})


# ---------------------------------------------------------------- exception mapping


# ---------------------------------------------------------------- rules
@app.post("/api/rules", status_code=201)
def create_rule(body: RuleCreate, conn: Connection = Depends(get_conn)):
    return _wrap(
        executor.create_rule,
        conn,
        body.dataset,
        body.name,
        body.type,
        body.config,
        body.change_note,
    )


@app.get("/api/rules")
def list_rules(dataset: str | None = None, conn: Connection = Depends(get_conn)):
    return executor.list_rules(conn, dataset)


@app.get("/api/rules/{rule_id}")
def get_rule(rule_id: str, conn: Connection = Depends(get_conn)):
    return _wrap(executor.get_rule_detail, conn, rule_id)


@app.post("/api/rules/{rule_id}/publish")
def publish_rule(rule_id: str, change_note: str | None = None, conn: Connection = Depends(get_conn)):
    return _wrap(executor.publish_rule_version, conn, rule_id, change_note)


@app.post("/api/rules/{rule_id}/upgrade")
def upgrade_rule(rule_id: str, body: RuleUpgrade, conn: Connection = Depends(get_conn)):
    return _wrap(executor.upgrade_rule, conn, rule_id, body.config, body.change_note)


# ---------------------------------------------------------------- batches / executions
@app.post("/api/ingest", status_code=201)
def ingest(body: IngestRequest, conn: Connection = Depends(get_conn)):
    try:
        return executor.ingest_batch(conn, body.dataset, body.rows)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/batches")
def list_batches(dataset: str | None = None, conn: Connection = Depends(get_conn)):
    return executor.list_batches(conn, dataset)


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str, conn: Connection = Depends(get_conn)):
    return _wrap(executor.get_batch, conn, batch_id)


@app.get("/api/batches/{batch_id}/executions")
def batch_executions(batch_id: str, conn: Connection = Depends(get_conn)):
    _wrap(executor.get_batch, conn, batch_id)
    return executor.list_executions(conn, batch_id)


@app.get("/api/batches/{batch_id}/failures")
def batch_failures(batch_id: str, limit: int = 20, conn: Connection = Depends(get_conn)):
    _wrap(executor.get_batch, conn, batch_id)
    return executor.failure_samples(conn, batch_id, limit)


# ---------------------------------------------------------------- quarantine / repair / replay
@app.get("/api/quarantine")
def list_quarantine(
    dataset: str | None = None,
    status: str | None = None,
    conn: Connection = Depends(get_conn),
):
    return executor.list_quarantine(conn, dataset, status)


@app.get("/api/quarantine/{entry_id}/history")
def entry_history(entry_id: int, conn: Connection = Depends(get_conn)):
    return {"history": executor.get_entry_history(conn, entry_id)}


@app.put("/api/quarantine/{dataset}/{record_key}/repair")
def repair(dataset: str, record_key: str, body: RepairRequest, conn: Connection = Depends(get_conn)):
    return _wrap(executor.repair_record, conn, dataset, record_key, body.payload)


@app.post("/api/batches/{batch_id}/replay", status_code=201)
def replay(batch_id: str, conn: Connection = Depends(get_conn)):
    return _wrap(executor.replay_failures, conn, batch_id)


# ---------------------------------------------------------------- gate
@app.get("/api/gate/{root_batch_id}")
def gate(root_batch_id: str, conn: Connection = Depends(get_conn)):
    return _wrap(executor.evaluate_gate, conn, root_batch_id)


@app.post("/api/gate/{root_batch_id}/publish")
def publish(root_batch_id: str, conn: Connection = Depends(get_conn)):
    return _wrap(executor.publish_batch, conn, root_batch_id)


# ---------------------------------------------------------------- static UI
_WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")
if os.path.isdir(_WEB_DIR):
    app.mount("/static", StaticFiles(directory=_WEB_DIR), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(os.path.join(_WEB_DIR, "index.html"))
