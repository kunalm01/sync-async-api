"""Load generator: hammers /sync and /async, runs its own callback receiver, prints a summary.

Usage:
  python loadgen.py --mode both -n 2000 -c 100
  python loadgen.py --mode async -n 1000 --fail-rate 0.3   # receiver fails 30% -> exercises retries
"""
import argparse
import asyncio
import json
import random
import statistics
import time
from collections import Counter

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

sent_at: dict[str, float] = {}       # request_id -> time we sent /async
callback_at: dict[str, float] = {}   # request_id -> first successful callback
callback_hits = Counter()            # request_id -> deliveries seen (dupes = at-least-once)
received_seqs: list[int] = []
FAIL_RATE = 0.0


async def callback(request: Request):
    data = json.loads(await request.body())
    rid = data["request_id"]
    if random.random() < FAIL_RATE:
        return JSONResponse({"err": "simulated"}, status_code=503)
    callback_hits[rid] += 1
    if rid not in callback_at:
        callback_at[rid] = time.perf_counter()
        received_seqs.append(data["seq"])
    return JSONResponse({"ok": True})


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def fmt(xs):
    return (f"p50={pct(xs,50):.1f}ms p95={pct(xs,95):.1f}ms p99={pct(xs,99):.1f}ms "
            f"max={max(xs):.1f}ms mean={statistics.mean(xs):.1f}ms") if xs else "n/a"


async def run(client, mode, n, conc, rounds, cb_url):
    sem = asyncio.Semaphore(conc)
    codes, lat = Counter(), []

    async def one(i):
        body = {"text": f"request {i} the quick brown fox", "rounds": rounds}
        if mode == "async":
            body["callback_url"] = cb_url
        async with sem:
            t0 = time.perf_counter()
            try:
                r = await client.post(f"/{mode}", json=body)
                codes[r.status_code] += 1
                lat.append((time.perf_counter() - t0) * 1000)
                if mode == "async" and r.status_code == 202:
                    sent_at[r.json()["request_id"]] = t0
            except httpx.HTTPError as e:
                codes[type(e).__name__] += 1

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    return codes, lat, time.perf_counter() - t0


async def main():
    global FAIL_RATE
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://127.0.0.1:8000")
    ap.add_argument("--mode", choices=["sync", "async", "both"], default="both")
    ap.add_argument("-n", type=int, default=1000)
    ap.add_argument("-c", type=int, default=100, help="concurrency")
    ap.add_argument("--rounds", type=int, default=2000, help="work size")
    ap.add_argument("--cb-port", type=int, default=9000)
    ap.add_argument("--fail-rate", type=float, default=0.0, help="fraction of callbacks the receiver rejects")
    ap.add_argument("--wait", type=float, default=60, help="max seconds to wait for callbacks")
    a = ap.parse_args()
    FAIL_RATE = a.fail_rate

    server = uvicorn.Server(uvicorn.Config(Starlette(routes=[Route("/cb", callback, methods=["POST"])]),
                                           port=a.cb_port, log_level="warning"))
    srv_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    cb_url = f"http://127.0.0.1:{a.cb_port}/cb"

    limits = httpx.Limits(max_connections=a.c, max_keepalive_connections=a.c)
    async with httpx.AsyncClient(base_url=a.target, timeout=30, limits=limits) as client:
        for mode in (["sync", "async"] if a.mode == "both" else [a.mode]):
            codes, lat, wall = await run(client, mode, a.n, a.c, a.rounds, cb_url)
            ok = codes.get(200 if mode == "sync" else 202, 0)
            print(f"\n=== {mode.upper()} ===  sent={a.n}  ok={ok}  fail={a.n - ok}  "
                  f"wall={wall:.2f}s  throughput={a.n / wall:.0f} req/s")
            print(f"  status codes: {dict(codes)}")
            print(f"  {'response latency' if mode == 'sync' else 'ack latency'}: {fmt(lat)}")
            if mode == "async":
                deadline = time.perf_counter() + a.wait
                while len(callback_at) < len(sent_at) and time.perf_counter() < deadline:
                    await asyncio.sleep(0.2)
                ttc = [(callback_at[r] - sent_at[r]) * 1000 for r in sent_at if r in callback_at]
                dupes = sum(v - 1 for v in callback_hits.values())
                inversions = sum(1 for x, y in zip(received_seqs, received_seqs[1:]) if y < x)
                print(f"  callbacks received: {len(ttc)}/{len(sent_at)}  missing={len(sent_at) - len(ttc)}  "
                      f"duplicates={dupes}")
                print(f"  time-to-callback: {fmt(ttc)}")
                print(f"  ordering: {inversions} out-of-order arrivals of {len(received_seqs)} "
                      f"(seq lets receivers reorder)")
        h = (await client.get("/healthz")).json()
        print(f"\nserver /healthz: {h}")

    server.should_exit = True
    await srv_task


if __name__ == "__main__":
    asyncio.run(main())
