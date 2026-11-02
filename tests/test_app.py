import asyncio
import os
import time

import httpx
import pytest

os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CALLBACK_BACKOFF_BASE_S", "0.01")

from app import config, main  # noqa: E402
from app.security import CallbackURLError, RateLimiter, validate_callback_url  # noqa: E402
from app.work import do_work  # noqa: E402


def test_work_is_deterministic():
    assert do_work("a b a", 10) == do_work("a b a", 10)
    assert do_work("a b a", 10)["top_word"] == "a"


@pytest.mark.parametrize("url", ["ftp://x.com/cb", "http://user:pw@x.com/cb", "file:///etc/passwd", "http:///cb"])
async def test_bad_callback_urls_rejected(url):
    with pytest.raises(CallbackURLError):
        await validate_callback_url(url)


async def test_private_ips_blocked_in_prod(monkeypatch):
    monkeypatch.setattr(config, "ALLOW_PRIVATE_CALLBACKS", False)
    for url in ["http://127.0.0.1/cb", "http://169.254.169.254/latest/meta-data", "http://10.0.0.5/cb"]:
        with pytest.raises(CallbackURLError):
            await validate_callback_url(url)


def test_rate_limiter():
    rl = RateLimiter(rate=0, burst=3)
    assert [rl.allow("ip") for _ in range(4)] == [True, True, True, False]


@pytest.fixture
async def client():
    async with main.lifespan(main.app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://t") as c:
            yield c


async def test_sync_and_async_share_logic_and_retry(client, monkeypatch):
    r = await client.post("/sync", json={"text": "hello world", "rounds": 5})
    assert r.status_code == 200
    expected = r.json()["result"]

    # Fake receiver: fails twice, then succeeds -> exercises retry path.
    calls = []

    async def fake_post(url, content, headers):
        calls.append(headers["X-Signature"])
        return httpx.Response(500 if len(calls) < 3 else 200)

    monkeypatch.setattr(main.dispatcher.client, "post", fake_post)
    r = await client.post("/async", json={"text": "hello world", "rounds": 5, "callback_url": "http://127.0.0.1:1/cb"})
    assert r.status_code == 202
    rid = r.json()["request_id"]

    deadline = time.time() + 5
    while (row := (await client.get(f"/requests/{rid}")).json())["status"] != "delivered" and time.time() < deadline:
        await asyncio.sleep(0.02)
    assert row["status"] == "delivered"
    assert row["result"] == expected
    assert [a["status_code"] for a in row["callback_attempts"]] == [500, 500, 200]


async def test_permanent_4xx_not_retried(client, monkeypatch):
    async def fake_post(url, content, headers):
        return httpx.Response(404)

    monkeypatch.setattr(main.dispatcher.client, "post", fake_post)
    rid = (await client.post("/async", json={"text": "x", "rounds": 1, "callback_url": "http://127.0.0.1:1/cb"})).json()["request_id"]
    for _ in range(100):
        row = (await client.get(f"/requests/{rid}")).json()
        if row["status"] == "failed":
            break
        await asyncio.sleep(0.02)
    assert row["status"] == "failed" and row["attempts"] == 1


async def test_idempotency_key(client):
    body = {"text": "x", "rounds": 1, "callback_url": "http://127.0.0.1:1/cb"}
    a = (await client.post("/async", json=body, headers={"Idempotency-Key": "k1"})).json()
    b = (await client.post("/async", json=body, headers={"Idempotency-Key": "k1"})).json()
    assert a["request_id"] == b["request_id"] and b["duplicate"]


async def test_idempotency_key_concurrent(client):
    body = {"text": "x", "rounds": 1, "callback_url": "http://127.0.0.1:1/cb"}
    rs = await asyncio.gather(*(client.post("/async", json=body, headers={"Idempotency-Key": "k2"}) for _ in range(10)))
    assert all(r.status_code in (200, 202) for r in rs)
    assert len({r.json()["request_id"] for r in rs}) == 1
