"""Async pipeline: bounded FIFO queue -> worker pool -> work -> signed callback with retries."""
import asyncio
import json
import logging
import random
import time

import httpx

from . import config
from .db import DB
from .security import sign
from .work import do_work

log = logging.getLogger("dispatcher")


class Dispatcher:
    def __init__(self, db: DB):
        self.db = db
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=config.QUEUE_MAX)
        self.client: httpx.AsyncClient | None = None
        self.tasks: list[asyncio.Task] = []
        self.retry_tasks: set[asyncio.Task] = set()

    async def start(self):
        self.queue = asyncio.Queue(maxsize=config.QUEUE_MAX)  # bind to the running loop
        self.client = httpx.AsyncClient(
            timeout=config.CALLBACK_TIMEOUT_S,
            follow_redirects=False,  # a redirect could bounce us to an internal address
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
        # Crash recovery: anything not yet delivered is re-enqueued in original order.
        for job in await self.db.unfinished_async():
            self.queue.put_nowait(job)
        self.tasks = [asyncio.create_task(self._worker(i)) for i in range(config.WORKERS)]

    async def stop(self):
        for t in [*self.tasks, *self.retry_tasks]:
            t.cancel()
        await asyncio.gather(*self.tasks, *self.retry_tasks, return_exceptions=True)
        await self.client.aclose()

    def submit(self, job: dict) -> bool:
        try:
            self.queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            return False

    async def _worker(self, n: int):
        while True:
            job = await self.queue.get()
            try:
                await self._process(job)
            except Exception:
                log.exception("worker %d failed on %s", n, job["id"])
            finally:
                self.queue.task_done()

    async def _process(self, job: dict):
        rid = job["id"]
        result = job.get("result")
        if result is None:
            await self.db.update(rid, status="processing")
            p = job["payload"]
            try:
                # CPU work off the event loop so callbacks/ACKs stay responsive.
                result = await asyncio.to_thread(do_work, p["text"], p["rounds"])
            except Exception as e:
                await self.db.update(rid, status="failed", error=f"work error: {e}", completed_at=time.time())
                return
            await self.db.update(rid, status="delivering", result=result, completed_at=time.time())
            job["result"] = result
        await self._deliver(job, attempt=job.get("attempts", 0) + 1)

    async def _deliver(self, job: dict, attempt: int):
        rid = job["id"]
        body = json.dumps({
            "request_id": rid, "seq": job["seq"], "status": "done",
            "result": job["result"], "created_at": job["created_at"], "attempt": attempt,
        }).encode()
        ts = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "X-Request-Id": rid,
            "X-Signature-Timestamp": ts,
            "X-Signature": sign(body, ts),
            "Idempotency-Key": rid,  # receivers dedupe at-least-once deliveries
        }
        t0 = time.perf_counter()
        code, err = None, None
        try:
            r = await self.client.post(job["callback_url"], content=body, headers=headers)
            code = r.status_code
        except httpx.HTTPError as e:
            err = type(e).__name__
        dur = (time.perf_counter() - t0) * 1000
        await self.db.log_attempt(rid, attempt, code, err, dur)

        if code is not None and 200 <= code < 300:
            await self.db.update(rid, status="delivered", delivered_at=time.time())
            return
        # 4xx (except 408/429) = receiver rejected it; retrying won't help.
        permanent = code is not None and 400 <= code < 500 and code not in (408, 429)
        if permanent or attempt >= config.CALLBACK_MAX_ATTEMPTS:
            await self.db.update(rid, status="failed", error=f"callback failed: {err or code} after {attempt} attempts")
            return
        # Exponential backoff with full jitter, scheduled off-worker so a dead
        # receiver doesn't stall the pool for everyone else.
        delay = random.uniform(0, min(config.CALLBACK_BACKOFF_MAX_S, config.CALLBACK_BACKOFF_BASE_S * 2 ** (attempt - 1)))
        t = asyncio.create_task(self._retry_later(job, attempt + 1, delay))
        self.retry_tasks.add(t)
        t.add_done_callback(self.retry_tasks.discard)

    async def _retry_later(self, job, attempt, delay):
        await asyncio.sleep(delay)
        await self._deliver(job, attempt)
