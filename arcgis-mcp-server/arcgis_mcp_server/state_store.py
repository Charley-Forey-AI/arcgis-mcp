"""Pluggable shared state store (in-memory default; Redis optional).

Used for:
- rate limiting buckets
- geocode cache
- session-scoped ArcGIS tokens
- one-time codes for OAuth callback
- pending token fallback (clients without Mcp-Session-Id)
- shareable map states (/map/s/<id>)
"""

from __future__ import annotations

import json
import secrets
import time
from dataclasses import dataclass
from typing import Any


def _now() -> float:
    return time.time()


def _json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), default=str)


def _json_loads(s: str) -> Any:
    return json.loads(s)


class StateStore:
    """Interface for a shared store."""

    def rate_limit_allow(self, *, key: str, limit: int, per_seconds: float) -> bool:  # pragma: no cover
        raise NotImplementedError

    def geocode_cache_get(self, *, key: str) -> Any | None:  # pragma: no cover
        raise NotImplementedError

    def geocode_cache_set(self, *, key: str, value: Any, ttl_seconds: int) -> None:  # pragma: no cover
        raise NotImplementedError

    def session_token_get(self, *, session_id: str) -> dict | None:  # pragma: no cover
        raise NotImplementedError

    def session_token_set(self, *, session_id: str, token: str, referer: str, ttl_seconds: int) -> None:  # pragma: no cover
        raise NotImplementedError

    def one_time_code_set(self, *, code: str, token: str, referer: str, ttl_seconds: int) -> None:  # pragma: no cover
        raise NotImplementedError

    def one_time_code_pop(self, *, code: str) -> dict | None:  # pragma: no cover
        raise NotImplementedError

    def pending_token_get(self) -> dict | None:  # pragma: no cover
        raise NotImplementedError

    def pending_token_set(self, *, token: str, referer: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def map_state_put(self, *, state_id: str, state: dict, ttl_seconds: int) -> None:  # pragma: no cover
        raise NotImplementedError

    def map_state_get(self, *, state_id: str) -> dict | None:  # pragma: no cover
        raise NotImplementedError


@dataclass
class _Entry:
    t: float
    value: Any


class InMemoryStateStore(StateStore):
    def __init__(self) -> None:
        self._rate_hits: dict[str, list[float]] = {}
        self._geocode_cache: dict[str, _Entry] = {}
        self._session_tokens: dict[str, _Entry] = {}
        self._one_time_codes: dict[str, _Entry] = {}
        self._pending_token: dict | None = None
        self._map_states: dict[str, _Entry] = {}

    def rate_limit_allow(self, *, key: str, limit: int, per_seconds: float) -> bool:
        now = _now()
        bucket = self._rate_hits.setdefault(key, [])
        cutoff = now - per_seconds
        bucket[:] = [t for t in bucket if t >= cutoff]
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        return True

    def geocode_cache_get(self, *, key: str) -> Any | None:
        e = self._geocode_cache.get(key)
        if not e:
            return None
        if isinstance(e.value, dict) and _now() <= float(e.value.get("expires_at", 0)):
            return e.value.get("value")
        self._geocode_cache.pop(key, None)
        return None

    def geocode_cache_set(self, *, key: str, value: Any, ttl_seconds: int) -> None:
        self._geocode_cache[key] = _Entry(
            t=_now(),
            value={"value": value, "expires_at": _now() + max(60, int(ttl_seconds))},
        )

    def session_token_get(self, *, session_id: str) -> dict | None:
        e = self._session_tokens.get(session_id)
        if not e:
            return None
        if isinstance(e.value, dict) and _now() <= float(e.value.get("expires_at", 0)):
            return e.value
        return None

    def session_token_set(self, *, session_id: str, token: str, referer: str, ttl_seconds: int) -> None:
        self._session_tokens[session_id] = _Entry(
            t=_now(),
            value={"token": token, "referer": referer, "expires_at": _now() + max(60, int(ttl_seconds))},
        )

    def one_time_code_set(self, *, code: str, token: str, referer: str, ttl_seconds: int) -> None:
        self._one_time_codes[code] = _Entry(
            t=_now(),
            value={"token": token, "referer": referer, "expires_at": _now() + max(60, int(ttl_seconds))},
        )

    def one_time_code_pop(self, *, code: str) -> dict | None:
        e = self._one_time_codes.pop(code, None)
        if not e or not isinstance(e.value, dict):
            return None
        if _now() > float(e.value.get("expires_at", 0)):
            return None
        return e.value

    def pending_token_get(self) -> dict | None:
        return self._pending_token

    def pending_token_set(self, *, token: str, referer: str) -> None:
        self._pending_token = {"token": token, "referer": referer}

    def map_state_put(self, *, state_id: str, state: dict, ttl_seconds: int) -> None:
        self._map_states[state_id] = _Entry(
            t=_now(),
            value={"state": state, "expires_at": _now() + max(60, int(ttl_seconds))},
        )

    def map_state_get(self, *, state_id: str) -> dict | None:
        e = self._map_states.get(state_id)
        if not e or not isinstance(e.value, dict):
            return None
        if _now() > float(e.value.get("expires_at", 0)):
            self._map_states.pop(state_id, None)
            return None
        st = e.value.get("state")
        return st if isinstance(st, dict) else None


class RedisStateStore(StateStore):
    def __init__(self, redis_client) -> None:
        self._r = redis_client

    def rate_limit_allow(self, *, key: str, limit: int, per_seconds: float) -> bool:
        window = max(1, int(per_seconds))
        bucket = int(_now() / window)
        k = f"rl:{key}:{bucket}"
        n = int(self._r.incr(k))
        if n == 1:
            self._r.expire(k, window + 2)
        return n <= int(limit)

    def geocode_cache_get(self, *, key: str) -> Any | None:
        raw = self._r.get(f"geo:{key}")
        if not raw:
            return None
        try:
            data = _json_loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
            return data.get("value") if isinstance(data, dict) else data
        except Exception:
            return None

    def geocode_cache_set(self, *, key: str, value: Any, ttl_seconds: int) -> None:
        k = f"geo:{key}"
        self._r.setex(k, max(60, int(ttl_seconds)), _json_dumps({"value": value}))

    def session_token_get(self, *, session_id: str) -> dict | None:
        raw = self._r.get(f"sess:{session_id}")
        if not raw:
            return None
        try:
            data = _json_loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def session_token_set(self, *, session_id: str, token: str, referer: str, ttl_seconds: int) -> None:
        self._r.setex(f"sess:{session_id}", max(60, int(ttl_seconds)), _json_dumps({"token": token, "referer": referer}))

    def one_time_code_set(self, *, code: str, token: str, referer: str, ttl_seconds: int) -> None:
        self._r.setex(f"code:{code}", max(60, int(ttl_seconds)), _json_dumps({"token": token, "referer": referer}))

    def one_time_code_pop(self, *, code: str) -> dict | None:
        pipe = self._r.pipeline()
        k = f"code:{code}"
        pipe.get(k)
        pipe.delete(k)
        raw, _ = pipe.execute()
        if not raw:
            return None
        try:
            data = _json_loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def pending_token_get(self) -> dict | None:
        raw = self._r.get("pending:token")
        if not raw:
            return None
        try:
            data = _json_loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def pending_token_set(self, *, token: str, referer: str) -> None:
        # Keep short TTL to reduce cross-user risk when used.
        self._r.setex("pending:token", 60 * 60, _json_dumps({"token": token, "referer": referer}))

    def map_state_put(self, *, state_id: str, state: dict, ttl_seconds: int) -> None:
        self._r.setex(f"map:{state_id}", max(60, int(ttl_seconds)), _json_dumps(state))

    def map_state_get(self, *, state_id: str) -> dict | None:
        raw = self._r.get(f"map:{state_id}")
        if not raw:
            return None
        try:
            data = _json_loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
            return data if isinstance(data, dict) else None
        except Exception:
            return None


def create_store_from_env() -> StateStore:
    """Create store based on env vars.

    - If ARCGIS_REDIS_URL is set, uses RedisStateStore (requires `redis` package)
    - Else uses InMemoryStateStore
    """

    import os
    import logging

    logger = logging.getLogger(__name__)
    redis_url = (os.environ.get("ARCGIS_REDIS_URL") or "").strip()
    if not redis_url:
        return InMemoryStateStore()
    try:
        import redis  # type: ignore
    except Exception as e:
        logger.warning("ARCGIS_REDIS_URL set but redis package missing (%s); falling back to in-memory store", e)
        return InMemoryStateStore()
    try:
        client = redis.from_url(redis_url)
        # basic connectivity check
        client.ping()
        return RedisStateStore(client)
    except Exception as e:
        logger.warning("Could not connect to Redis (%s); falling back to in-memory store", e)
        return InMemoryStateStore()


def new_state_id() -> str:
    return secrets.token_urlsafe(10)

