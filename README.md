# Sync vs Async (Callback) API under request storms

**Submitted by:** Kunal Madan  |  **Stack:** Python 3.12, FastAPI, SQLite (WAL), httpx  |  **Tests:** 11 passing  |  **Demo:** _Loom link_

| Assignment requirement | Where it is handled |
| --- | --- |
| Same work behind both endpoints | `app/work.py`, called by `/sync` and by the async worker |
| Query recent requests, trace async delivery | `GET /requests`, `GET /requests/{id}` (status timeline + every callback attempt) |
| Callback URL is down | Retries with backoff and jitter, permanent-failure detection, terminal `failed` status |
| Ordering and timing | FIFO queue, monotonic `seq` on every callback, jobs persisted before ack and recovered on restart |
| High request volume | Bounded queue (429), sync concurrency cap (503), per-IP rate limit, size limits |
| Callback abuse | SSRF guard, no redirects, host allowlist, HMAC-signed callbacks, idempotency keys |
| Load generator | `loadgen.py` with its own callback receiver and failure injection |

A FastAPI service that exposes the same deterministic work in two interaction styles, plus a load generator that compares them.

- `POST /sync` does the work and returns the result in the response.
- `POST /async` returns `202` with a `request_id` right away, then POSTs the result to `callback_url`.

## Run locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

```bash
curl -s localhost:8000/sync -H 'content-type: application/json' -d '{"text":"hello world","rounds":1000}'
curl -s localhost:8000/async -H 'content-type: application/json' \
  -d '{"text":"hello world","rounds":1000,"callback_url":"http://127.0.0.1:9000/cb"}'
curl -s 'localhost:8000/requests?mode=async&limit=5'
curl -s localhost:8000/requests/<id>     # full trace, including every callback attempt
curl -s localhost:8000/healthz
```

Tests: `pytest -q` (11 tests)

## Load generator

`loadgen.py` starts its own callback receiver on `--cb-port` (default 9000), fires requests at both endpoints, and prints the summary.

```bash
python loadgen.py --mode both -n 2000 -c 200                         # sync vs async comparison
python loadgen.py --mode async -n 1000 -c 100 --fail-rate 0.3        # receiver fails 30% of callbacks, so retries kick in
```

The summary covers sent, ok and failed counts, status codes, and p50/p95/p99 latency. Sync reports response latency. Async reports ack latency, time-to-callback, missing and duplicate callbacks, and out-of-order arrivals.

Sample run on an M-series laptop (2000 requests, concurrency 200, 2000 hash rounds):

| | ok | p50 | p95 | p99 |
|---|---|---|---|---|
| sync response | 2000/2000 | 476 ms | 4194 ms | 5558 ms |
| async ack | 2000/2000 | 328 ms | 1503 ms | 2353 ms |
| async time-to-callback | 2000/2000 | 324 ms | 1487 ms | 2325 ms |

With `--fail-rate 0.3`, 997 of 1000 callbacks arrived. The other 3 failed all 5 attempts (0.3^5 ≈ 0.24%, as expected) and were marked `failed` with their full attempt history.

## Configuration (env vars)

| Var | Default | Purpose |
|---|---|---|
| `WORKERS` | 32 | async worker pool size |
| `QUEUE_MAX` | 10000 | bounded queue; `429` when full |
| `SYNC_CONCURRENCY` | 256 | max in-flight sync work; `503` beyond this |
| `CALLBACK_MAX_ATTEMPTS` | 5 | delivery attempts before marking `failed` |
| `CALLBACK_TIMEOUT_S` | 5 | per-attempt timeout |
| `CALLBACK_BACKOFF_BASE_S` / `_MAX_S` | 0.5 / 30 | exponential backoff with full jitter |
| `CALLBACK_SECRET` | dev-secret | HMAC signing key |
| `ALLOW_PRIVATE_CALLBACKS` | true | **set false in prod** to block private, loopback and metadata IPs |
| `CALLBACK_HOST_ALLOWLIST` | (empty) | optional comma-separated allowed callback hosts |
| `RATE_LIMIT_RPS` / `_BURST` | 2000 / 4000 | per-IP token bucket |

## Design

```
POST /async -> validate URL -> persist (queued) -> bounded asyncio.Queue -> 202
                                                     |
                                          worker pool (N tasks)
                                                     |
                              do_work (thread) -> persist result (delivering)
                                                     |
                           signed POST callback_url -> delivered | retry w/ backoff | failed
```

- **Shared logic.** `app/work.py:do_work` is the only business logic, and both paths call it. It runs iterated SHA-256 plus text stats, so the work is CPU-bound and deterministic. It runs in a thread so it doesn't block the event loop.
- **Persistence and tracing.** SQLite in WAL mode. The `requests` table stores status transitions and timestamps (`created_at`, `completed_at`, `delivered_at`). `callback_attempts` stores one row per delivery attempt with the status code, error, and duration. `GET /requests/{id}` returns the full trace.
- **Durability.** A job is persisted before the server sends `202`. On startup, any `queued`, `processing` or `delivering` job is re-enqueued in `seq` order. If a result was already computed, it is redelivered without redoing the work.
- **Callback failures.** Network errors, 5xx, 408 and 429 are retried with exponential backoff and full jitter, up to `CALLBACK_MAX_ATTEMPTS`. Other 4xx responses count as permanent rejections and are not retried. Retries are scheduled off the worker, so a dead receiver can't stall the pool for everyone else. A job that exhausts its retries ends in status `failed`, which is queryable via `GET /requests?status=failed`.
- **Delivery semantics.** Delivery is at-least-once. Each callback carries an `Idempotency-Key: <request_id>` header so receivers can dedupe.
- **Ordering and timing.** Each request gets a monotonic `seq`. The queue is FIFO, so processing starts in arrival order. Completion order isn't strict, because N workers run in parallel and retries are delayed. Every callback includes `seq` and `created_at`, so receivers can reorder if they need to. Strict per-receiver ordering would mean sharding by callback host with head-of-line blocking on retries. I chose throughput instead and documented the trade-off.
- **Scaling and overload.** Both paths have explicit limits instead of queueing without bound. Sync allows a fixed number of in-flight requests and returns `503` beyond that. Async uses a bounded queue and returns `429` when it is full. There is also a per-IP rate limit, a body size cap, and Pydantic bounds on `text` length and `rounds`. Under a storm the server sheds load with `Retry-After` and does not run out of memory.
- **Callback abuse protection.**
  - Only `http` and `https` URLs are accepted, with no embedded credentials and at most 2048 characters.
  - An optional host allowlist restricts where callbacks can go.
  - With `ALLOW_PRIVATE_CALLBACKS=false`, the server resolves the host and blocks private, loopback, link-local and metadata addresses (SSRF).
  - Redirects are not followed, and each attempt has a timeout.
  - Each callback carries an HMAC-SHA256 signature in `X-Signature`, computed over `timestamp.body`, plus `X-Signature-Timestamp`. Receivers can verify the origin and reject replays.
  - An optional `Idempotency-Key` on `POST /async` stops clients from creating duplicate jobs when they retry.

### Trade-offs and next steps

The demo deliberately runs as a single process. These are the next steps for production:

- **Single process, in-memory queue.** To scale horizontally, replace it with Redis Streams, SQS or Kafka, and Postgres instead of SQLite. The queue and rate limiter are the only pieces that hold in-process state.
- **Write cost.** SQLite commits every state change on a single connection. That is the main source of ack latency under load. Batching writes or switching to Postgres would cut it.
- **DNS rebinding.** The SSRF check resolves the host once at submit time. A hardened version would pin the resolved IP for the actual connection.
- **Sync vs async.** Sync is simpler for clients but ties up a connection for the whole job, and its tail latency grows with load. Async acks quickly and absorbs bursts in the queue. In exchange, clients have to run a callback endpoint, handle at-least-once delivery, and deal with possible reordering.

## Technical validation

I checked each guarantee with a test or a measured run, not by reading the code.

| Guarantee | How it was checked |
| --- | --- |
| Sync and async run identical logic | Test compares the `/sync` result with the delivered async result |
| Failed callbacks are retried, then delivered | Test receiver fails twice, then succeeds: attempts recorded as `[500, 500, 200]` |
| Permanent rejections are not retried | Receiver returns 404: exactly one attempt, status `failed` |
| Retries hold up under load | `--fail-rate 0.3` on 1000 requests: 997 delivered, 3 failed. Theory predicts 0.3^5 ≈ 0.24%, about 2.4 |
| No duplicate jobs on client retries | Sequential and 10-way concurrent requests with one `Idempotency-Key` all return the same id |
| SSRF targets are blocked | Tests reject `ftp://`, embedded credentials, `127.0.0.1`, `10.x`, `169.254.169.254` |
| No crash under a storm | 4000 mixed requests at concurrency 200: 100% success, queue drained to 0 |

**Testing caught a real bug.** One integration test stayed stuck at `queued`. The job queue was created at import time, so it was tied to a different event loop from the one running the workers. I moved its creation into startup and every test passed. In production this would only appear on a restart within the same process, which makes it the kind of bug that is hard to find later.

## AI and resource usage

- **AI tool: Claude Code.** I used it to scaffold the modules, the load generator and the tests, and to iterate quickly. Claude wrote first drafts; I made the design decisions and reviewed every file.
- **Decisions I made:**
  - Persist each job before acknowledging it, so no acknowledged job can be lost.
  - Split `processing` from `delivering`, so recovery redelivers a stored result instead of recomputing it.
  - Choose which callback failures are retried and which are permanent, and schedule retries outside the worker pool.
  - Shed load with explicit limits instead of letting queues grow.
  - Make ordering best-effort with a `seq` number rather than strict, and document why.
  - Decide what the callback protections cover.
- **How I checked the AI output:**
  - Every guarantee above has a test or a measured run.
  - I checked the retry results against the probability math, not just the pass count.
  - I reviewed the concurrency and security paths by hand. That review found the race between two identical idempotency keys, which I fixed and covered with a test.
- **References:**
  - AWS Builders Library, "Timeouts, retries, and backoff with jitter"
  - Stripe docs, Idempotent requests, and Webhook signatures
  - OWASP, Server-Side Request Forgery Prevention Cheat Sheet
