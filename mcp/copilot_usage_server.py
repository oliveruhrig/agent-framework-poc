from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.metrics_registry import MetricsRegistry, MetricsRegistryError
from services.segment_adoption_loader import (
    SegmentAdoptionAnalytics,
    SegmentAdoptionConfigError,
    get_segment_adoption_analytics_safe,
)
from services.premium_requests_loader import (
    PremiumRequestsAnalytics,
    PremiumRequestsConfigError,
    get_premium_requests_analytics_safe,
)

app = FastAPI(title="Copilot Usage MCP Server", version="1.0.0")

_SEGMENT_ANALYTICS: Optional[SegmentAdoptionAnalytics]
_SEGMENT_ERROR: Optional[Exception]
_SEGMENT_ANALYTICS, _SEGMENT_ERROR = get_segment_adoption_analytics_safe()

_PREMIUM_ANALYTICS: Optional[PremiumRequestsAnalytics]
_PREMIUM_ERROR: Optional[Exception]
_PREMIUM_ANALYTICS, _PREMIUM_ERROR = get_premium_requests_analytics_safe()

try:
    _METRICS_REGISTRY = MetricsRegistry()
    _METRICS_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - configuration stage
    _METRICS_REGISTRY = None
    _METRICS_ERROR = exc


def _ensure_registry() -> MetricsRegistry:
    if _METRICS_ERROR is not None:
        raise HTTPException(status_code=503, detail=f"Metrics registry unavailable: {_METRICS_ERROR}")
    if _METRICS_REGISTRY is None:
        raise HTTPException(status_code=503, detail="Metrics registry not loaded.")
    return _METRICS_REGISTRY


def _ensure_segment_analytics() -> SegmentAdoptionAnalytics:
    if _SEGMENT_ERROR is not None:
        raise HTTPException(status_code=503, detail=f"Segment adoption analytics unavailable: {_SEGMENT_ERROR}")
    if _SEGMENT_ANALYTICS is None:
        raise HTTPException(status_code=503, detail="Segment adoption analytics not initialised.")
    return _SEGMENT_ANALYTICS


def _ensure_premium_analytics() -> PremiumRequestsAnalytics:
    if _PREMIUM_ERROR is not None:
        raise HTTPException(status_code=503, detail=f"Premium requests analytics unavailable: {_PREMIUM_ERROR}")
    if _PREMIUM_ANALYTICS is None:
        raise HTTPException(status_code=503, detail="Premium requests analytics not initialised.")
    return _PREMIUM_ANALYTICS


class ToolDescription(BaseModel):
    name: str
    description: str
    arguments: Dict[str, str]


class ToolInvocation(BaseModel):
    tool_name: str = Field(..., description="Identifier of the tool to execute.")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Named arguments")


class ToolResult(BaseModel):
    tool_name: str
    result: str


_TOOL_METADATA: Dict[str, ToolDescription] = {
    "segment_adoption_segments": ToolDescription(
        name="segment_adoption_segments",
        description="Enumerate segments present in the segment adoption dataset.",
        arguments={},
    ),
    "segment_adoption_summary": ToolDescription(
        name="segment_adoption_summary",
        description="Summarise FTE and contractor adoption from the aggregated segment dataset.",
        arguments={
            "segment": "Optional segment filter",
            "start_month": "Earliest month (YYYY-MM)",
            "end_month": "Latest month (YYYY-MM)",
        },
    ),
    "segment_adoption_trend": ToolDescription(
        name="segment_adoption_trend",
        description="Time-series view of FTE or contractor adoption for a segment.",
        arguments={
            "segment": "Optional segment filter",
            "metric": "fte_adoption | non_fte_adoption | fte_active | non_fte_active",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
            "limit": "Number of recent points to return",
        },
    ),
    "segment_adoption_leaders": ToolDescription(
        name="segment_adoption_leaders",
        description="Rank segments by FTE/contractor adoption or active headcount.",
        arguments={
            "month": "Optional month (YYYY-MM) to filter",
            "metric": "fte_adoption | non_fte_adoption | fte_active | non_fte_active",
            "limit": "Top N segments to include",
        },
    ),
    "describe_metrics": ToolDescription(
        name="describe_metrics",
        description="Return catalogue entries for the analytics metrics.",
        arguments={"metric_ids": "Optional list of metric identifiers"},
    ),
    "premium_requests_summary": ToolDescription(
        name="premium_requests_summary",
        description="Summarise premium request usage, costs, and user counts across both enterprises.",
        arguments={
            "segment": "Optional segment filter",
            "user_type": "fte | contractor | all (default: all)",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
        },
    ),
    "premium_requests_trend": ToolDescription(
        name="premium_requests_trend",
        description="Month-by-month trend of premium requests, cost, or unique users.",
        arguments={
            "segment": "Optional segment filter",
            "user_type": "fte | contractor | all (default: all)",
            "metric": "requests | cost | users (default: requests)",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
            "limit": "Number of recent months to return",
        },
    ),
    "premium_requests_top_segments": ToolDescription(
        name="premium_requests_top_segments",
        description="Rank segments by premium request volume, cost, or user count.",
        arguments={
            "user_type": "fte | contractor | all (default: all)",
            "metric": "requests | cost | users (default: cost)",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
            "limit": "Top N segments",
        },
    ),
    "premium_requests_top_models": ToolDescription(
        name="premium_requests_top_models",
        description="Rank AI models by request volume and cost.",
        arguments={
            "segment": "Optional segment filter",
            "user_type": "fte | contractor | all (default: all)",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
            "limit": "Top N models",
        },
    ),
    "premium_requests_enterprise_breakdown": ToolDescription(
        name="premium_requests_enterprise_breakdown",
        description="Compare usage between manulife (EMU) and manulife-financial (legacy) enterprises.",
        arguments={
            "segment": "Optional segment filter",
            "user_type": "fte | contractor | all (default: all)",
            "start_month": "Start month (YYYY-MM)",
            "end_month": "End month (YYYY-MM)",
        },
    ),
}


@app.get("/health", response_model=Dict[str, str])
def healthcheck() -> Dict[str, str]:
    base = {"status": "ok"}
    if _SEGMENT_ERROR is not None:
        base["segmentAnalytics"] = "error"
    else:
        base["segmentAnalytics"] = "ready" if _SEGMENT_ANALYTICS is not None else "pending"
    if _PREMIUM_ERROR is not None:
        base["premiumAnalytics"] = "error"
    else:
        base["premiumAnalytics"] = "ready" if _PREMIUM_ANALYTICS is not None else "pending"
    if _METRICS_ERROR is not None:
        base["metrics"] = "error"
    else:
        base["metrics"] = "ready" if _METRICS_REGISTRY is not None else "missing"
    return base


@app.get("/mcp/tools", response_model=List[ToolDescription])
def list_tools() -> List[ToolDescription]:
    return list(_TOOL_METADATA.values())


def _execute_tool(tool_name: str, arguments: Dict[str, Any]) -> str:
    try:
        if tool_name == "segment_adoption_summary":
            segment_analytics = _ensure_segment_analytics()
            return segment_analytics.summary(
                segment=arguments.get("segment"),
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
            )
        if tool_name == "segment_adoption_segments":
            segment_analytics = _ensure_segment_analytics()
            segments = segment_analytics.available_segments()
            if not segments:
                return "No segments found in the dataset."
            return "Available segments:\n" + "\n".join(f"- {segment}" for segment in segments)
        if tool_name == "segment_adoption_trend":
            segment_analytics = _ensure_segment_analytics()
            metric = arguments.get("metric") or "fte_adoption"
            if metric not in {"fte_adoption", "non_fte_adoption", "fte_active", "non_fte_active"}:
                metric = "fte_adoption"
            return segment_analytics.trend(
                segment=arguments.get("segment"),
                metric=metric,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
                limit=int(arguments.get("limit", 6)),
            )
        if tool_name == "segment_adoption_leaders":
            segment_analytics = _ensure_segment_analytics()
            metric = arguments.get("metric") or "fte_adoption"
            if metric not in {"fte_adoption", "non_fte_adoption", "fte_active", "non_fte_active"}:
                metric = "fte_adoption"
            return segment_analytics.leaders(
                month=arguments.get("month"),
                metric=metric,
                limit=int(arguments.get("limit", 5)),
            )
        if tool_name == "describe_metrics":
            registry = _ensure_registry()
            metric_ids = arguments.get("metric_ids")
            if metric_ids is not None and not isinstance(metric_ids, list):
                raise SegmentAdoptionConfigError("metric_ids must be a list of metric identifiers")
            return registry.as_markdown(metric_ids)
        if tool_name == "premium_requests_summary":
            premium_analytics = _ensure_premium_analytics()
            user_type = arguments.get("user_type", "all")
            if user_type not in {"fte", "contractor", "all"}:
                user_type = "all"
            return premium_analytics.summary(
                segment=arguments.get("segment"),
                user_type=user_type,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
            )
        if tool_name == "premium_requests_trend":
            premium_analytics = _ensure_premium_analytics()
            user_type = arguments.get("user_type", "all")
            if user_type not in {"fte", "contractor", "all"}:
                user_type = "all"
            metric = arguments.get("metric", "requests")
            if metric not in {"requests", "cost", "users"}:
                metric = "requests"
            return premium_analytics.trend(
                segment=arguments.get("segment"),
                user_type=user_type,
                metric=metric,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
                limit=int(arguments.get("limit", 6)),
            )
        if tool_name == "premium_requests_top_segments":
            premium_analytics = _ensure_premium_analytics()
            user_type = arguments.get("user_type", "all")
            if user_type not in {"fte", "contractor", "all"}:
                user_type = "all"
            metric = arguments.get("metric", "cost")
            if metric not in {"requests", "cost", "users"}:
                metric = "cost"
            return premium_analytics.top_segments(
                user_type=user_type,
                metric=metric,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
                limit=int(arguments.get("limit", 5)),
            )
        if tool_name == "premium_requests_top_models":
            premium_analytics = _ensure_premium_analytics()
            user_type = arguments.get("user_type", "all")
            if user_type not in {"fte", "contractor", "all"}:
                user_type = "all"
            return premium_analytics.top_models(
                segment=arguments.get("segment"),
                user_type=user_type,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
                limit=int(arguments.get("limit", 5)),
            )
        if tool_name == "premium_requests_enterprise_breakdown":
            premium_analytics = _ensure_premium_analytics()
            user_type = arguments.get("user_type", "all")
            if user_type not in {"fte", "contractor", "all"}:
                user_type = "all"
            return premium_analytics.enterprise_breakdown(
                segment=arguments.get("segment"),
                user_type=user_type,
                start_month=arguments.get("start_month"),
                end_month=arguments.get("end_month"),
            )
    except SegmentAdoptionConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive path
        raise HTTPException(status_code=500, detail=f"Tool execution failed: {exc}") from exc
    raise HTTPException(status_code=404, detail=f"Unknown tool '{tool_name}'")


@app.post("/mcp/execute", response_model=ToolResult)
def execute_tool(payload: ToolInvocation) -> ToolResult:
    if payload.tool_name not in _TOOL_METADATA:
        raise HTTPException(status_code=404, detail=f"Tool '{payload.tool_name}' is not registered")
    result = _execute_tool(payload.tool_name, payload.arguments)
    return ToolResult(tool_name=payload.tool_name, result=result)


@app.get("/mcp/metrics", response_model=Dict[str, str])
def metrics_catalog(registry: MetricsRegistry = Depends(_ensure_registry)) -> Dict[str, str]:
    return {key: definition.as_bullet() for key, definition in registry.describe_metrics().items()}


# Convenience entry point ----------------------------------------------------

def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the MCP server using uvicorn."""

    import uvicorn

    uvicorn.run("mcp.copilot_usage_server:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
