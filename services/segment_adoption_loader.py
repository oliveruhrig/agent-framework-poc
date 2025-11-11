from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .segment_adoption import SegmentAdoptionAnalytics, SegmentAdoptionConfigError

load_dotenv()

_SEGMENT_ENV = "COPILOT_SEGMENT_ADOPTION_CSV"
_SEGMENT_DEFAULT = Path("data/copilot/segment_adoption.csv")

_SEGMENT_ANALYTICS: Optional[SegmentAdoptionAnalytics] = None
_SEGMENT_ERROR: Optional[Exception] = None


def _resolve_path() -> Path:
    env_value = os.getenv(_SEGMENT_ENV)
    if env_value:
        return Path(env_value).expanduser().resolve()
    return _SEGMENT_DEFAULT.resolve()


def get_segment_adoption_analytics() -> SegmentAdoptionAnalytics:
    global _SEGMENT_ANALYTICS, _SEGMENT_ERROR
    if _SEGMENT_ANALYTICS is None and _SEGMENT_ERROR is None:
        csv_path = _resolve_path()
        try:
            _SEGMENT_ANALYTICS = SegmentAdoptionAnalytics(csv_path)
        except Exception as exc:  # pragma: no cover - configuration stage
            _SEGMENT_ERROR = exc
            raise
    if _SEGMENT_ERROR:
        raise _SEGMENT_ERROR
    return _SEGMENT_ANALYTICS  # type: ignore[return-value]


def get_segment_adoption_analytics_safe() -> tuple[Optional[SegmentAdoptionAnalytics], Optional[Exception]]:
    try:
        analytics = get_segment_adoption_analytics()
        return analytics, None
    except Exception as exc:
        return None, exc


__all__ = [
    "get_segment_adoption_analytics",
    "get_segment_adoption_analytics_safe",
    "SegmentAdoptionAnalytics",
    "SegmentAdoptionConfigError",
]
