import os
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

def _csv(name: str, default: str = "") -> list[str]:
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]


def _bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in {"1", "true", "yes", "on"}


def _backend_root_relative(path: str) -> str:
    """Anchor relative paths to the backend package root, not the shell cwd.

    Mirrors the SQLite path anchoring in app/database.py: keeps the default
    artifact path stable regardless of where the process is started from
    (e.g. `/app/backend` inside the container).
    """
    candidate = Path(path)
    if candidate.is_absolute():
        return str(candidate)
    return str(Path(__file__).resolve().parents[1] / candidate)


class Settings:
    """Environment-driven configuration.

    Every value is read from the process environment when the Settings
    instance is created (callers use the cached `get_settings()`), so
    production configuration comes from environment variables only.
    """

    def __init__(self) -> None:
        self.APP_NAME: str = "RecoverSense API"
        self.APP_VERSION: str = "1.0.0"
        self.DATABASE_URL: str = os.environ.get("DATABASE_URL", "sqlite:///./recoversense.db")
        self.ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
        self.RAZORPAY_KEY_ID: str = os.environ.get("RAZORPAY_KEY_ID", "")
        self.RAZORPAY_KEY_SECRET: str = os.environ.get("RAZORPAY_KEY_SECRET", "")
        self.RAZORPAY_WEBHOOK_SECRET: str = os.environ.get("RAZORPAY_WEBHOOK_SECRET", "")
        # Provider reads are opt-in outside production so local simulation remains offline.
        self.RAZORPAY_STATE_VERIFICATION_ENABLED: bool = _bool(
            "RAZORPAY_STATE_VERIFICATION_ENABLED", _bool("PRODUCTION_MODE", False)
        )
        # Real Razorpay UPI Autopay S2S subsequent-debit execution is OPT-IN
        # (default False). When disabled, the simulator fallback is used and
        # no real provider write is ever attempted.
        self.RAZORPAY_REAL_EXECUTION_ENABLED: bool = _bool(
            "RAZORPAY_REAL_EXECUTION_ENABLED", False
        )
        # Hard Test Mode safety limit for real execution, interpreted in
        # PAISE. Default 10000 paise = ₹100 — the maximum real execution
        # test amount. Requests above this ceiling are rejected before any
        # provider call.
        self.RAZORPAY_REAL_EXECUTION_MAX_AMOUNT: int = int(
            os.environ.get("RAZORPAY_REAL_EXECUTION_MAX_AMOUNT", "10000")
        )
        self.CORS_ORIGINS: list[str] = _csv(
            "CORS_ORIGINS",
            "http://localhost:5500,http://127.0.0.1:5500,http://localhost:5000,http://127.0.0.1:5000",
        )
        self.DEMO_MODE: bool = _bool("DEMO_MODE", True)
        self.PRODUCTION_MODE: bool = _bool("PRODUCTION_MODE", False)
        # Interactive API documentation (/docs, /redoc, /openapi.json) must not be
        # publicly reachable in production deployments. Default: enabled outside
        # PRODUCTION_MODE, disabled in PRODUCTION_MODE. Explicit override wins.
        self.ENABLE_API_DOCS: bool = _bool("ENABLE_API_DOCS", not self.PRODUCTION_MODE)
        self.API_KEY: str = os.environ.get("RECOVERSENSE_API_KEY", "")
        self.MODEL_PATH: str = os.environ.get(
            "RECOVERSENSE_MODEL_PATH",
            "artifacts/recovery_model.pkl",
        )
        self.MAX_BODY_BYTES: int = int(os.environ.get("MAX_BODY_BYTES", "1048576"))
        self.LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

    @property
    def resolved_model_path(self) -> str:
        return _backend_root_relative(self.MODEL_PATH)


PRODUCTION_REQUIRED_SECRETS = ("RAZORPAY_WEBHOOK_SECRET", "RECOVERSENSE_API_KEY")


def validate_production_settings(settings: Settings) -> list[str]:
    """Return the names of mandatory secrets missing in production mode.

    Returns a list of *environment variable names* (never values) so callers
    can fail fast without ever echoing a secret. Empty list means the
    configuration is complete.
    """
    if not settings.PRODUCTION_MODE:
        return []
    missing = []
    if not settings.RAZORPAY_WEBHOOK_SECRET:
        missing.append("RAZORPAY_WEBHOOK_SECRET")
    if not settings.API_KEY:
        missing.append("RECOVERSENSE_API_KEY")
    return missing


@lru_cache
def get_settings() -> Settings:
    return Settings()
