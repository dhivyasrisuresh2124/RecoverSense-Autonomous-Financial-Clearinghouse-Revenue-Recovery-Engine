"""RecoverSense production-oriented FastAPI application."""
from __future__ import annotations
import logging
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.config import Settings, get_settings, validate_production_settings
from app.database import init_db
from app.routers import webhooks, simulate, decisions, opportunities, evaluation, audit, policy, failure_demo, optimization, flash_settle

settings = get_settings()

# Production-safe structured logging. Level is environment-driven; secrets
# are never emitted — messages use key=value fields and names, not values.
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("recoversense")


def api_documentation_config(settings: "Settings") -> dict:
    """Return the FastAPI docs/openapi URL configuration for the given settings.

    Interactive documentation must never be publicly exposed on a production
    deployment: in PRODUCTION_MODE (unless ENABLE_API_DOCS is explicitly set)
    /docs, /redoc and /openapi.json are not mounted at all, so they also never
    appear in the API-key-exempt PUBLIC_PATHS below. This is a pure function so
    deployment tests can assert the exposure policy without importing the app.
    """
    if settings.ENABLE_API_DOCS:
        return {"docs_url": "/docs", "redoc_url": "/redoc", "openapi_url": "/openapi.json"}
    return {"docs_url": None, "redoc_url": None, "openapi_url": None}


_DOC_CONFIG = api_documentation_config(settings)

app = FastAPI(
    title=settings.APP_NAME,
    description="Explainable, liquidity-aware recovery engine for soft UPI mandate failures.",
    version=settings.APP_VERSION,
    **_DOC_CONFIG,
)

# Public paths skip the API-key middleware. Documentation paths are only
# exempt when they are actually mounted (ENABLE_API_DOCS); in production they
# are absent from the app entirely (see api_documentation_config above).
PUBLIC_PATHS = {"/", "/health", "/readiness", "/webhooks/razorpay"}
PUBLIC_PATHS.update({url for url in _DOC_CONFIG.values() if url})


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    if request.method == "OPTIONS" or request.url.path in PUBLIC_PATHS:
        return await call_next(request)
    if settings.PRODUCTION_MODE:
        if not settings.API_KEY:
            logger.error("api_key_missing request_path=%s remote=%s", request.url.path, request.client.host if request.client else "unknown")
            return JSONResponse(status_code=503, content={"detail": "Production API key is not configured."})
        supplied = request.headers.get("X-API-Key", "")
        if supplied != settings.API_KEY:
            logger.warning("unauthorized_request request_path=%s remote=%s", request.url.path, request.client.host if request.client else "unknown")
            return JSONResponse(status_code=401, content={"detail": "Missing or invalid API key."})
    if request.headers.get("content-length"):
        try:
            if int(request.headers["content-length"]) > settings.MAX_BODY_BYTES:
                return JSONResponse(status_code=413, content={"detail": "Request body too large."})
        except ValueError:
            pass
    return await call_next(request)


# Middleware added last is outermost in FastAPI/Starlette.  CORS must wrap
# the API-key middleware so browser clients can read legitimate 401/413/error
# responses instead of seeing a misleading CORS failure.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "X-Razorpay-Signature"],
)


app.include_router(webhooks.router)
app.include_router(simulate.router)
app.include_router(decisions.router)
app.include_router(opportunities.router)
app.include_router(evaluation.router)
app.include_router(audit.router)
app.include_router(policy.router)
app.include_router(optimization.router)
app.include_router(flash_settle.router)
if settings.DEMO_MODE:
    app.include_router(failure_demo.router)


@app.on_event("startup")
def on_startup():
    if settings.PRODUCTION_MODE:
        missing = validate_production_settings(settings)
        if missing:
            logger.error("startup_configuration_failed missing_secrets=%s", ",".join(missing))
            raise RuntimeError(
                "PRODUCTION_MODE=true requires these environment variables: "
                + ", ".join(missing) + "."
            )
        # SQLite is single-process: more than one uvicorn worker would corrupt it.
        # Fail fast rather than silently running an unsafe configuration.
        workers = os.environ.get("UVICORN_WORKERS", "1").strip() or "1"
        if settings.DATABASE_URL.startswith("sqlite") and workers != "1":
            raise RuntimeError(
                "SQLite deployments must run with exactly 1 uvicorn worker "
                f"(got UVICORN_WORKERS={workers!r}). Use PostgreSQL for multiple workers."
            )
        # A wildcard CORS origin in production defeats the API-key gate.
        if "*" in settings.CORS_ORIGINS:
            raise RuntimeError(
                "PRODUCTION_MODE=true forbids a wildcard CORS origin ('*')."
            )
    init_db()
    if settings.PRODUCTION_MODE:
        from app.routers.decisions import load_production_model
        logger.info("startup loading_production_model path=%s", settings.resolved_model_path)
        if not load_production_model(settings.resolved_model_path):
            raise RuntimeError(f"Production model artifact not found or invalid: {settings.resolved_model_path}")
    else:
        from app.routers.decisions import load_or_train_demo_model
        logger.info("startup loading_or_training_demo_model")
        if not load_or_train_demo_model():
            raise RuntimeError("Demo recovery model could not be loaded or trained.")
    logger.info(
        "startup_complete mode=%s version=%s webhook_verification=%s",
        "production" if settings.PRODUCTION_MODE else "demo",
        settings.APP_VERSION,
        "enforced" if settings.RAZORPAY_WEBHOOK_SECRET else ("demo_skipped" if settings.DEMO_MODE else "DISABLED"),
    )


@app.get("/")
def root():
    return {
        "product": "RecoverSense",
        "version": settings.APP_VERSION,
        "positioning": "Explainable, liquidity-aware recovery for failed UPI mandate payments.",
        "mode": "production" if settings.PRODUCTION_MODE else "demo",
        "status": "ok",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "healthy", "version": settings.APP_VERSION, "mode": "production" if settings.PRODUCTION_MODE else "demo"}


@app.get("/readiness")
def readiness():
    checks = {"database": "unknown", "configuration": "ok", "model": "unknown"}
    try:
        from app.database import SessionLocal
        from sqlalchemy import text
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {type(exc).__name__}"
    from app.routers.decisions import model_readiness
    checks["model"] = model_readiness()
    if settings.PRODUCTION_MODE and not settings.RAZORPAY_WEBHOOK_SECRET:
        checks["configuration"] = "missing_webhook_secret"
    if settings.PRODUCTION_MODE and not settings.API_KEY:
        checks["configuration"] = "missing_api_key"
    status = "ready" if all(v in {"ok", "loaded"} for v in checks.values()) else "not_ready"
    return {"status": status, "checks": checks, "mode": "production" if settings.PRODUCTION_MODE else "demo"}
