from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path


class AuditStore:
    """Tenant-scoped durable audit and effect ledger; effect plus audit is one SQLite transaction."""
    def __init__(self, path: str | Path):
        self.path = str(path)
        with closing(self._connect()) as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS tool_effects (
              idempotency_key TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, workflow_id TEXT NOT NULL,
              action_id TEXT NOT NULL, status TEXT NOT NULL, result_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS audit_events (
              event_id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL, workflow_id TEXT NOT NULL,
              correlation_id TEXT NOT NULL, event_type TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS audit_scope ON audit_events(tenant_id, workflow_id);
            """)
    def _connect(self) -> sqlite3.Connection:
        conn=sqlite3.connect(self.path)
        conn.row_factory=sqlite3.Row
        return conn
    def audit(self, tenant_id: str, workflow_id: str, correlation_id: str, event_type: str, detail: dict[str, object]) -> None:
        with closing(self._connect()) as conn:
            conn.execute("INSERT INTO audit_events(tenant_id,workflow_id,correlation_id,event_type,detail_json,created_at) VALUES(?,?,?,?,?,?)",(tenant_id,workflow_id,correlation_id,event_type,json.dumps(detail,sort_keys=True),datetime.now(UTC).isoformat()))
            conn.commit()
    def execute_effect(self, *, key: str, tenant_id: str, workflow_id: str, action_id: str, correlation_id: str) -> tuple[dict[str, object], bool]:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row=conn.execute("SELECT result_json FROM tool_effects WHERE idempotency_key=?",(key,)).fetchone()
            if row:
                conn.commit(); return json.loads(row["result_json"]), False
            result={"status":"SIMULATED_ROLLBACK_COMPLETE","effect_id":key[:16]}
            now=datetime.now(UTC).isoformat()
            conn.execute("INSERT INTO tool_effects VALUES(?,?,?,?,?,?,?)",(key,tenant_id,workflow_id,action_id,"COMPLETED",json.dumps(result,sort_keys=True),now))
            conn.execute("INSERT INTO audit_events(tenant_id,workflow_id,correlation_id,event_type,detail_json,created_at) VALUES(?,?,?,?,?,?)",(tenant_id,workflow_id,correlation_id,"TOOL_EFFECT_COMMITTED",json.dumps({"idempotency_key":key,"action_id":action_id},sort_keys=True),now))
            conn.commit(); return result, True
    def effects(self, tenant_id: str, workflow_id: str) -> list[dict[str, object]]:
        with closing(self._connect()) as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM tool_effects WHERE tenant_id=? AND workflow_id=?",(tenant_id,workflow_id))]
    def events(self, tenant_id: str, workflow_id: str) -> list[dict[str, object]]:
        with closing(self._connect()) as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM audit_events WHERE tenant_id=? AND workflow_id=? ORDER BY event_id",(tenant_id,workflow_id))]
