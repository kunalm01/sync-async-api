"""SQLite persistence: one row per request, one row per callback delivery attempt."""
import json
import time

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,          -- sync: done|failed ; async: queued|processing|delivering|delivered|failed
    payload TEXT NOT NULL,
    result TEXT,
    error TEXT,
    callback_url TEXT,
    idempotency_key TEXT UNIQUE,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    completed_at REAL,
    delivered_at REAL
);
CREATE INDEX IF NOT EXISTS ix_requests_mode_created ON requests(mode, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_requests_status ON requests(status);
CREATE TABLE IF NOT EXISTS callback_attempts (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    at REAL NOT NULL,
    status_code INTEGER,
    error TEXT,
    duration_ms REAL,
    PRIMARY KEY (request_id, attempt)
);
"""


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn: aiosqlite.Connection | None = None
        self._seq = 0

    async def open(self):
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()
        async with self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM requests") as cur:
            self._seq = (await cur.fetchone())[0]

    async def close(self):
        if self.conn:
            await self.conn.close()

    def next_seq(self) -> int:
        # Single event loop -> no race. Monotonic arrival order for async consumers.
        self._seq += 1
        return self._seq

    async def insert(self, rid, mode, status, payload, callback_url=None, idem=None, result=None, error=None):
        now = time.time()
        seq = self.next_seq()
        await self.conn.execute(
            "INSERT INTO requests (id, seq, mode, status, payload, result, error, callback_url, idempotency_key,"
            " created_at, completed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (rid, seq, mode, status, json.dumps(payload), json.dumps(result) if result else None, error,
             callback_url, idem, now, now if mode == "sync" else None),
        )
        await self.conn.commit()
        return seq, now

    async def update(self, rid, **fields):
        if "result" in fields and fields["result"] is not None:
            fields["result"] = json.dumps(fields["result"])
        cols = ", ".join(f"{k}=?" for k in fields)
        await self.conn.execute(f"UPDATE requests SET {cols} WHERE id=?", (*fields.values(), rid))
        await self.conn.commit()

    async def log_attempt(self, rid, attempt, status_code, error, duration_ms):
        await self.conn.execute(
            "INSERT OR REPLACE INTO callback_attempts VALUES (?,?,?,?,?,?)",
            (rid, attempt, time.time(), status_code, error, duration_ms),
        )
        await self.conn.execute("UPDATE requests SET attempts=? WHERE id=?", (attempt, rid))
        await self.conn.commit()

    async def get(self, rid):
        async with self.conn.execute("SELECT * FROM requests WHERE id=?", (rid,)) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        out = _row(row)
        async with self.conn.execute(
            "SELECT attempt, at, status_code, error, duration_ms FROM callback_attempts WHERE request_id=? ORDER BY attempt",
            (rid,),
        ) as cur:
            out["callback_attempts"] = [dict(r) for r in await cur.fetchall()]
        return out

    async def get_by_idem(self, key):
        async with self.conn.execute("SELECT * FROM requests WHERE idempotency_key=?", (key,)) as cur:
            row = await cur.fetchone()
        return _row(row) if row else None

    async def recent(self, mode=None, status=None, limit=50):
        q, args = "SELECT * FROM requests WHERE 1=1", []
        if mode:
            q += " AND mode=?"; args.append(mode)
        if status:
            q += " AND status=?"; args.append(status)
        q += " ORDER BY created_at DESC LIMIT ?"; args.append(limit)
        async with self.conn.execute(q, args) as cur:
            return [_row(r) for r in await cur.fetchall()]

    async def unfinished_async(self):
        async with self.conn.execute(
            "SELECT * FROM requests WHERE mode='async' AND status IN ('queued','processing','delivering') ORDER BY seq"
        ) as cur:
            return [_row(r) for r in await cur.fetchall()]

    async def stats(self):
        async with self.conn.execute("SELECT mode, status, COUNT(*) c FROM requests GROUP BY mode, status") as cur:
            return [dict(r) for r in await cur.fetchall()]


def _row(row) -> dict:
    d = dict(row)
    d["payload"] = json.loads(d["payload"])
    if d.get("result"):
        d["result"] = json.loads(d["result"])
    return d
