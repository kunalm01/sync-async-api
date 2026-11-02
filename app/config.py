import os


def _env(name: str, default, cast=str):
    v = os.getenv(name)
    return default if v is None else cast(v)


def _bool(v: str) -> bool:
    return v.lower() in ("1", "true", "yes")


DB_PATH = _env("DB_PATH", "data.db")
WORKERS = _env("WORKERS", 32, int)                      # async job workers
QUEUE_MAX = _env("QUEUE_MAX", 10_000, int)              # backpressure: 429 when full
SYNC_CONCURRENCY = _env("SYNC_CONCURRENCY", 256, int)   # max in-flight /sync work
CALLBACK_TIMEOUT_S = _env("CALLBACK_TIMEOUT_S", 5.0, float)
CALLBACK_MAX_ATTEMPTS = _env("CALLBACK_MAX_ATTEMPTS", 5, int)
CALLBACK_BACKOFF_BASE_S = _env("CALLBACK_BACKOFF_BASE_S", 0.5, float)
CALLBACK_BACKOFF_MAX_S = _env("CALLBACK_BACKOFF_MAX_S", 30.0, float)
CALLBACK_SECRET = _env("CALLBACK_SECRET", "dev-secret")  # HMAC signing key
# Local demo needs callbacks to 127.0.0.1; in prod set to false (SSRF guard).
ALLOW_PRIVATE_CALLBACKS = _env("ALLOW_PRIVATE_CALLBACKS", "true", _bool)
# Optional comma-separated host allowlist; empty = any public host.
CALLBACK_HOST_ALLOWLIST = [h.strip() for h in _env("CALLBACK_HOST_ALLOWLIST", "").split(",") if h.strip()]
RATE_LIMIT_RPS = _env("RATE_LIMIT_RPS", 2000.0, float)  # per client IP
RATE_LIMIT_BURST = _env("RATE_LIMIT_BURST", 4000, int)
MAX_BODY_BYTES = _env("MAX_BODY_BYTES", 64 * 1024, int)
