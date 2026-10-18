"""HTTP client for the Source API — the simulator's only way to change data."""

import threading
import time
import uuid

import httpx


class ApiError(Exception):
    def __init__(self, status: int, body):
        detail = body.get("detail") if isinstance(body, dict) else None
        self.status = status
        self.code = detail.get("code") if isinstance(detail, dict) else None
        self.detail = detail
        super().__init__(f"{status} {self.code or body}")


class SourceApi:
    def __init__(self, base_url: str, timeout: float = 10.0, retries: int = 3):
        self.http = httpx.Client(base_url=base_url, timeout=timeout,
                                 limits=httpx.Limits(max_connections=32, max_keepalive_connections=16))
        self.retries = retries
        self.lock = threading.Lock()
        self.counters = {"requests": 0, "retries": 0, "replays": 0, "rejected": 0, "failed": 0}

    def _count(self, name: str):
        with self.lock:
            self.counters[name] += 1

    def _send(self, method: str, path: str, **kwargs) -> httpx.Response:
        for attempt in range(self.retries + 1):
            self._count("requests")
            try:
                r = self.http.request(method, path, **kwargs)
            except httpx.TransportError:
                if attempt == self.retries:
                    self._count("failed")
                    raise
            else:
                if r.status_code < 500:
                    return r
                if attempt == self.retries:
                    self._count("failed")
                    return r
            self._count("retries")
            time.sleep(0.2 * 2 ** attempt)
        raise RuntimeError("unreachable")

    def command(self, path: str, body: dict | None = None, method: str = "POST", key: str | None = None) -> dict:
        """Send a business command; retries reuse the same Idempotency-Key, so they never apply twice.

        Webhook senders pass their own event id as `key`, so a redelivered callback is deduplicated.
        """
        key = key or str(uuid.uuid4())
        kwargs = {"headers": {"Idempotency-Key": key}}
        if method != "DELETE":
            kwargs["json"] = body or {}
        r = self._send(method, path, **kwargs)
        if r.headers.get("Idempotent-Replay"):
            self._count("replays")
        if r.status_code >= 400:
            if r.status_code < 500:
                self._count("rejected")
            raise ApiError(r.status_code, _json(r))
        return r.json()

    def query(self, path: str, **params):
        r = self._send("GET", path, params={k: v for k, v in params.items() if v is not None})
        if r.status_code >= 400:
            raise ApiError(r.status_code, _json(r))
        return r.json()


def _json(r: httpx.Response):
    try:
        return r.json()
    except ValueError:
        return r.text
