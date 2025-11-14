from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .premium_requests import PremiumRequestsAnalytics, PremiumRequestsConfigError

load_dotenv()

_PREMIUM_ENV = "COPILOT_PREMIUM_REQUESTS_CSV"
_PREMIUM_DEFAULT = Path("data/copilot/premium_requests_db.csv")

_PREMIUM_ANALYTICS: Optional[PremiumRequestsAnalytics] = None
_PREMIUM_ERROR: Optional[Exception] = None


def _resolve_path() -> Path:
    env_value = os.getenv(_PREMIUM_ENV)
    if env_value:
        return Path(env_value).expanduser().resolve()
    return _PREMIUM_DEFAULT.resolve()


def get_premium_requests_analytics() -> PremiumRequestsAnalytics:
    global _PREMIUM_ANALYTICS, _PREMIUM_ERROR
    if _PREMIUM_ANALYTICS is None and _PREMIUM_ERROR is None:
        csv_path = _resolve_path()
        try:
            _PREMIUM_ANALYTICS = PremiumRequestsAnalytics(csv_path)
        except Exception as exc:  # pragma: no cover - configuration stage
            _PREMIUM_ERROR = exc
            raise
    if _PREMIUM_ERROR:
        raise _PREMIUM_ERROR
    return _PREMIUM_ANALYTICS  # type: ignore[return-value]


def get_premium_requests_analytics_safe() -> tuple[Optional[PremiumRequestsAnalytics], Optional[Exception]]:
    try:
        analytics = get_premium_requests_analytics()
        return analytics, None
    except Exception as exc:
        return None, exc


__all__ = [
    "get_premium_requests_analytics",
    "get_premium_requests_analytics_safe",
    "PremiumRequestsAnalytics",
    "PremiumRequestsConfigError",
]
