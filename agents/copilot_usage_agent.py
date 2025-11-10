from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Annotated, Optional

from dotenv import load_dotenv
from agent_framework.azure import AzureAIAgentClient
from azure.identity.aio import AzureCliCredential
from pydantic import Field

from services.copilot_usage import AnalyticsConfigError, CopilotUsageAnalytics

load_dotenv()

_USAGE_ENV = "COPILOT_USAGE_CSV"
_INTERACTIONS_ENV = "COPILOT_INTERACTIONS_CSV"
_DATA_DEFAULT_DIR = Path("data")

_ANALYTICS: Optional[CopilotUsageAnalytics] = None
_ANALYTICS_ERROR: Optional[Exception] = None


def _resolve_data_path(env_name: str, default_name: str) -> Path:
    env_value = os.getenv(env_name)
    if env_value:
        return Path(env_value).expanduser().resolve()
    return (_DATA_DEFAULT_DIR / default_name).resolve()


def _get_analytics() -> CopilotUsageAnalytics:
    global _ANALYTICS, _ANALYTICS_ERROR
    if _ANALYTICS is None and _ANALYTICS_ERROR is None:
        usage_path = _resolve_data_path(_USAGE_ENV, "developer_monthly_usage.csv")
        interactions_path = _resolve_data_path(_INTERACTIONS_ENV, "copilot_interactions.csv")
        try:
            _ANALYTICS = CopilotUsageAnalytics(usage_path, interactions_path)
        except Exception as exc:  # pragma: no cover - configuration stage
            _ANALYTICS_ERROR = exc
            raise
    if _ANALYTICS_ERROR:
        raise _ANALYTICS_ERROR
    return _ANALYTICS  # type: ignore[return-value]


def _handle_analytics_errors(func):  # type: ignore[misc]
    def wrapper(*args, **kwargs):
        try:
            analytics = _get_analytics()
        except AnalyticsConfigError as exc:
            return f"Analytics configuration error: {exc}"
        except Exception as exc:  # pragma: no cover - defensive path
            return f"Failed to initialize analytics: {exc}"
        try:
            return func(analytics, *args, **kwargs)
        except AnalyticsConfigError as exc:
            return f"Unable to process request: {exc}"
        except Exception as exc:  # pragma: no cover - defensive path
            return f"Unexpected analytics failure: {exc}"

    return wrapper


@_handle_analytics_errors
def summarize_usage_tool(
    analytics: CopilotUsageAnalytics,
    division: Annotated[
        Optional[str],
        Field(description="Filter to a specific division, otherwise all divisions are included."),
    ] = None,
    start_month: Annotated[
        Optional[str],
        Field(description="Earliest month to include (YYYY-MM)."),
    ] = None,
    end_month: Annotated[
        Optional[str],
        Field(description="Latest month to include (YYYY-MM)."),
    ] = None,
) -> str:
    """Return an executive overview of Copilot adoption and impact."""

    return analytics.summarize_usage(
        division=division,
        start_month=start_month,
        end_month=end_month,
    )


@_handle_analytics_errors
def adoption_trend_tool(
    analytics: CopilotUsageAnalytics,
    division: Annotated[
        Optional[str],
        Field(description="Division to analyse. Leave blank for organization-wide trend."),
    ] = None,
    start_month: Annotated[Optional[str], Field(description="Start month inclusive (YYYY-MM).")]
    = None,
    end_month: Annotated[Optional[str], Field(description="End month inclusive (YYYY-MM).")]
    = None,
    limit: Annotated[
        int,
        Field(description="Number of recent months to include in the trend output."),
    ] = 6,
) -> str:
    """Return a month-by-month adoption trend."""

    return analytics.adoption_trend(
        division=division,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


@_handle_analytics_errors
def model_mix_tool(
    analytics: CopilotUsageAnalytics,
    division: Annotated[Optional[str], Field(description="Optional division filter.")]
    = None,
    start_month: Annotated[Optional[str], Field(description="Start month (YYYY-MM).")]
    = None,
    end_month: Annotated[Optional[str], Field(description="End month (YYYY-MM).")]
    = None,
    limit: Annotated[int, Field(description="Number of models to list.")]
    = 5,
) -> str:
    """Summarise the share of usage for each Copilot model."""

    return analytics.model_mix(
        division=division,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


@_handle_analytics_errors
def division_breakdown_tool(
    analytics: CopilotUsageAnalytics,
    start_month: Annotated[
        Optional[str],
        Field(description="Start month (YYYY-MM) for the division comparison."),
    ] = None,
    end_month: Annotated[
        Optional[str],
        Field(description="End month (YYYY-MM) for the division comparison."),
    ] = None,
    limit: Annotated[
        int,
        Field(description="Number of divisions to display."),
    ] = 5,
) -> str:
    """Compare divisions based on active Copilot usage."""

    return analytics.division_breakdown(
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


@_handle_analytics_errors
def list_divisions_tool(analytics: CopilotUsageAnalytics) -> str:
    """List the known divisions to help scope a query."""

    divisions = analytics.available_divisions()
    if not divisions:
        return "No divisions found in the developer usage dataset."
    return "Available divisions:\n" + "\n".join(f"- {division}" for division in divisions)


async def run_console_agent() -> None:
    """Run an interactive console session for management stakeholders."""

    try:
        _get_analytics()
    except Exception as exc:
        print(f"Warning: analytics data could not be initialised ({exc}).")

    print("Launching Copilot Usage Analyst. Type 'exit' to finish.\n")

    async with AzureCliCredential() as credential:
        client = AzureAIAgentClient(async_credential=credential)
        instructions = (
            "You are a GitHub Copilot adoption analyst supporting the CTO office. "
            "Use the registered analytics tools to answer every question. "
            "Always ground responses in the returned metrics, and mention assumptions when "
            "data is missing."
        )
        async with client.create_agent(
            name="CopilotUsageAnalyst",
            instructions=instructions,
            tools=[
                summarize_usage_tool,
                adoption_trend_tool,
                model_mix_tool,
                division_breakdown_tool,
                list_divisions_tool,
            ],
        ) as agent:
            while True:
                user_query = input("Management: ").strip()
                if not user_query:
                    continue
                if user_query.lower() in {"exit", "quit"}:
                    print("Session ended.")
                    break
                response = await agent.run(user_query)
                print(f"Analyst: {response}\n")


if __name__ == "__main__":
    asyncio.run(run_console_agent())
