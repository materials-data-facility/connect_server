import logging
import os
import uuid

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from v2.app.auth import get_auth, is_curator, is_submitter
from v2.app.middleware import configure_app_middleware

_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, _log_level, logging.INFO))
logger = logging.getLogger(__name__)

app = FastAPI(title="MDF Connect v2")

_cors_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "*")
_cors_origins = [o.strip() for o in _cors_raw.split(",") if o.strip()] if _cors_raw != "*" else ["*"]
_cors_allow_credentials = _cors_raw != "*"

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-User-Id", "X-User-Email", "X-User-Name", "X-Globus-Token", "X-MDF-Token", "X-Groups-Token"],
    allow_credentials=_cors_allow_credentials,
)
configure_app_middleware(app)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = uuid.uuid4().hex[:12]
    logger.exception("Unhandled exception request_id=%s path=%s", request_id, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "request_id": request_id},
    )


@app.get("/health")
async def health():
    return {"status": "ok", "service": "mdf-v2"}


@app.get("/auth/check")
async def auth_check(
    auth=Depends(get_auth),
):
    """Pre-flight auth check: returns identity and permissions.

    CLI calls this before file upload to catch auth/group issues early.
    """
    return {
        "success": True,
        "user_id": auth.user_id,
        "name": auth.name,
        "email": auth.user_email,
        "is_submitter": is_submitter(auth),
        "is_curator": is_curator(auth),
        "groups": list((auth.group_info or {}).keys()),
    }


from v2.app.routers import submissions, search, cards, curation, preview, admin  # noqa: E402
# from v2.app.routers import streams  # noqa: E402  — disabled until stream feature is ready
# from v2.app.routers import files  # noqa: E402  — disabled until stream feature is ready

app.include_router(submissions.router)
# app.include_router(streams.router)  — disabled until stream feature is ready
# app.include_router(files.router)  — disabled until stream feature is ready
app.include_router(search.router)
app.include_router(cards.router)
app.include_router(curation.router)
app.include_router(preview.router)
app.include_router(admin.router)
