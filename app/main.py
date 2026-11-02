import asyncio
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import config
from .db import DB
from .dispatcher import Dispatcher
from .security import CallbackURLError, RateLimiter, validate_callback_url
from .work import do_work


class WorkIn(BaseModel):
    text: str = Field(max_length=10_000)
    rounds: int = Field(default=1000, ge=1, le=100_000)


class AsyncIn(WorkIn):
    callback_url: str


db = DB(config.DB_PATH)
dispatcher = Dispatcher(db)
limiter = RateLimiter(config.RATE_LIMIT_RPS, config.RATE_LIMIT_BURST)
sync_slots = asyncio.Semaphore(config.SYNC_CONCURRENCY)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.open()
    await dispatcher.start()
    yield
    await dispatcher.stop()
    await db.close()


app = FastAPI(title="Sync vs Async API", lifespan=lifespan)


@app.middleware("http")
async def guard(request: Request, call_next):
    if request.method == "POST":
        if int(request.headers.get("content-length") or 0) > config.MAX_BODY_BYTES:
            return JSONResponse({"detail": "payload too large"}, status_code=413)
        if not limiter.allow(request.client.host if request.client else "unknown"):
            return JSONResponse({"detail": "rate limited"}, status_code=429, headers={"Retry-After": "1"})
    return await call_next(request)


@app.post("/sync")
async def sync_endpoint(body: WorkIn):
    rid = str(uuid.uuid4())
    if sync_slots.locked():
        # Shed load instead of queueing unboundedly and blowing up latency for everyone.
        raise HTTPException(503, "server busy", headers={"Retry-After": "1"})
    async with sync_slots:
        t0 = time.perf_counter()
        result = await asyncio.to_thread(do_work, body.text, body.rounds)
        elapsed_ms = (time.perf_counter() - t0) * 1000
    await db.insert(rid, "sync", "done", body.model_dump(), result=result)
    return {"request_id": rid, "status": "done", "result": result, "work_ms": round(elapsed_ms, 2)}


@app.post("/async", status_code=202)
async def async_endpoint(body: AsyncIn, idempotency_key: str | None = Header(default=None)):
    if idempotency_key and (existing := await db.get_by_idem(idempotency_key)):
        return {"request_id": existing["id"], "status": existing["status"], "duplicate": True}
    try:
        await validate_callback_url(body.callback_url)
    except CallbackURLError as e:
        raise HTTPException(400, str(e))
    if dispatcher.queue.full():
        raise HTTPException(429, "queue full", headers={"Retry-After": "1"})
    rid = str(uuid.uuid4())
    payload = body.model_dump(exclude={"callback_url"})
    # Persist before enqueue so a crash after ACK never loses the job.
    try:
        seq, created = await db.insert(rid, "async", "queued", payload, callback_url=body.callback_url, idem=idempotency_key)
    except sqlite3.IntegrityError:
        # Two concurrent requests with the same key: the unique constraint picks one winner.
        existing = await db.get_by_idem(idempotency_key)
        return {"request_id": existing["id"], "status": existing["status"], "duplicate": True}
    dispatcher.submit({"id": rid, "seq": seq, "payload": payload, "callback_url": body.callback_url,
                       "created_at": created, "attempts": 0})
    return {"request_id": rid, "seq": seq, "status": "queued", "status_url": f"/requests/{rid}"}


@app.get("/requests")
async def list_requests(mode: Literal["sync", "async"] | None = None, status: str | None = None,
                        limit: int = Query(50, ge=1, le=500)):
    return await db.recent(mode, status, limit)


@app.get("/requests/{rid}")
async def get_request(rid: str):
    row = await db.get(rid)
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.get("/healthz")
async def healthz():
    return {"ok": True, "queue_depth": dispatcher.queue.qsize(), "pending_retries": len(dispatcher.retry_tasks),
            "counts": await db.stats()}
