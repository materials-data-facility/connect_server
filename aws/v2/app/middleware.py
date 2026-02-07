import hashlib
import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

_RATE_LIMIT_STATE: Dict[str, Deque[float]] = defaultdict(deque)
_RATE_LOCK = threading.Lock()


def reset_middleware_state() -> None:
    """Reset in-memory middleware state for tests."""
    with _RATE_LOCK:
        _RATE_LIMIT_STATE.clear()


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _request_limit_for_path(path: str) -> int:
    if path.startswith("/submit"):
        return _env_int("RATE_LIMIT_SUBMIT_PER_MIN", 20)
    if path.startswith("/stream/create"):
        return _env_int("RATE_LIMIT_STREAM_CREATE_PER_MIN", 30)
    if path.startswith("/stream/") and path.endswith("/upload"):
        return _env_int("RATE_LIMIT_STREAM_UPLOAD_PER_MIN", 60)
    if path.startswith("/stream/"):
        return _env_int("RATE_LIMIT_STREAM_MUTATION_PER_MIN", 90)
    return _env_int("RATE_LIMIT_DEFAULT_PER_MIN", 120)


def _request_size_limit_bytes() -> int:
    return _env_int("MAX_REQUEST_BYTES", 1_048_576)


def _rate_limit_window_seconds() -> int:
    return _env_int("RATE_LIMIT_WINDOW_SECONDS", 60)


def _is_exempt_path(path: str) -> bool:
    return path in {"/health", "/docs", "/openapi.json", "/redoc", "/docs/oauth2-redirect"}


def _actor_key(request: Request) -> str:
    user_id = (request.headers.get("x-user-id") or "").strip()
    if user_id:
        return f"user:{user_id}"
    authz = (request.headers.get("authorization") or "").strip()
    if authz:
        digest = hashlib.sha256(authz.encode("utf-8")).hexdigest()[:12]
        return f"auth:{digest}"
    client_host = request.client.host if request.client else "unknown"
    return f"ip:{client_host}"


def _check_rate_limit(key: str, limit: int, window_sec: int) -> Tuple[bool, int]:
    if limit <= 0:
        return True, 0
    now = time.monotonic()
    cutoff = now - window_sec
    with _RATE_LOCK:
        bucket = _RATE_LIMIT_STATE[key]
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            retry_after = max(1, int(window_sec - (now - bucket[0])))
            return False, retry_after
        bucket.append(now)
    return True, 0


def configure_app_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def security_and_logging_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        method = request.method.upper()
        path = request.url.path
        start = time.monotonic()

        if not _is_exempt_path(path):
            if method in {"POST", "PUT", "PATCH"}:
                max_bytes = _request_size_limit_bytes()
                content_length = request.headers.get("content-length")
                if content_length:
                    try:
                        if int(content_length) > max_bytes:
                            return JSONResponse(
                                status_code=413,
                                content={
                                    "success": False,
                                    "error": f"Request body exceeds {max_bytes} bytes",
                                    "request_id": request_id,
                                },
                                headers={"X-Request-Id": request_id},
                            )
                    except ValueError:
                        pass
                body = await request.body()
                if len(body) > max_bytes:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "success": False,
                            "error": f"Request body exceeds {max_bytes} bytes",
                            "request_id": request_id,
                        },
                        headers={"X-Request-Id": request_id},
                    )

            actor = _actor_key(request)
            limit = _request_limit_for_path(path)
            allowed, retry_after = _check_rate_limit(
                key=f"{actor}:{path.split('/', 2)[1] if path.startswith('/') else path}",
                limit=limit,
                window_sec=_rate_limit_window_seconds(),
            )
            if not allowed:
                return JSONResponse(
                    status_code=429,
                    content={
                        "success": False,
                        "error": "Rate limit exceeded",
                        "request_id": request_id,
                        "retry_after_seconds": retry_after,
                    },
                    headers={
                        "Retry-After": str(retry_after),
                        "X-Request-Id": request_id,
                    },
                )

        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            log_data = {
                "event": "request_error",
                "request_id": request_id,
                "method": method,
                "path": path,
                "duration_ms": elapsed_ms,
            }
            logger.exception(json.dumps(log_data, sort_keys=True))
            raise

        elapsed_ms = int((time.monotonic() - start) * 1000)
        log_data = {
            "event": "request_complete",
            "request_id": request_id,
            "method": method,
            "path": path,
            "status_code": response.status_code,
            "duration_ms": elapsed_ms,
        }
        logger.info(json.dumps(log_data, sort_keys=True))
        response.headers["X-Request-Id"] = request_id
        return response
