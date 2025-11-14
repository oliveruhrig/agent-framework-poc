import asyncio
import re
from typing import Annotated, Optional, List

import httpx
from agent_framework.azure import AzureAIAgentClient
from azure.identity.aio import AzureCliCredential
from pydantic import Field

from pathlib import Path
import os
from dotenv import load_dotenv

def ensure_env_loaded() -> None:
    here = Path(__file__).resolve().parent
    for folder in [here, *here.parents]:
        env_path = folder / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
    else:
        raise RuntimeError("Could not locate a .env file for agent initialization.")
    if "AZURE_AI_PROJECT_ENDPOINT" not in os.environ:
        raise RuntimeError("AZURE_AI_PROJECT_ENDPOINT is missing after loading .env.")

ensure_env_loaded()


_GUARDRAIL_KEYWORDS = {
    "individual": "Please avoid querying individual developers.",
    "single developer": "Please avoid querying individual developers.",
    "email": "Email-level detail is not available.",
    "pii": "PII queries are blocked by policy.",
}


def _run_guardrails(message: str) -> Optional[str]:
    lowered = message.lower()
    for keyword, guidance in _GUARDRAIL_KEYWORDS.items():
        if keyword in lowered:
            return guidance
    id_match = re.search(r"developer\s+[0-9a-fA-F\-]{6,}", lowered)
    if id_match:
        return "Requests for specific developer identifiers are not permitted."
    return None


class McpBridge:
    """Simple HTTP bridge to the MCP analytics server."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def call(self, tool_name: str, **arguments) -> str:
        payload = {"tool_name": tool_name, "arguments": arguments}
        response = httpx.post(f"{self._base_url}/mcp/execute", json=payload, timeout=30.0)
        response.raise_for_status()
        data = response.json()
        return data.get("result", "No result returned by MCP server.")

    def available_tools(self) -> str:
        response = httpx.get(f"{self._base_url}/mcp/tools", timeout=10.0)
        response.raise_for_status()
        return response.text


_BRIDGE = McpBridge(base_url="http://127.0.0.1:8000")


def _call_bridge(tool: str, **kwargs) -> str:
    try:
        return _BRIDGE.call(tool, **kwargs)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        detail = exc.response.text
        return f"MCP server returned {status}: {detail}"
    except httpx.RequestError as exc:
        return f"Unable to reach MCP server: {exc}"

def list_segments_tool() -> str:
    return _call_bridge("segment_adoption_segments")


def describe_metrics_tool(
    metric_ids: Annotated[Optional[List[str]], Field(description="Specific metric identifiers.")] = None,
) -> str:
    return _call_bridge("describe_metrics", metric_ids=metric_ids)


def segment_adoption_summary_tool(
    segment: Annotated[Optional[str], Field(description="Optional segment filter.")] = None,
    start_month: Annotated[Optional[str], Field(description="Earliest month (YYYY-MM).")] = None,
    end_month: Annotated[Optional[str], Field(description="Latest month (YYYY-MM).")] = None,
) -> str:
    return _call_bridge(
        "segment_adoption_summary",
        segment=segment,
        start_month=start_month,
        end_month=end_month,
    )


def segment_adoption_trend_tool(
    segment: Annotated[Optional[str], Field(description="Optional segment filter.")] = None,
    metric: Annotated[str, Field(description="fte_adoption | non_fte_adoption | fte_active | non_fte_active")] = "fte_adoption",
    start_month: Annotated[Optional[str], Field(description="Start month (YYYY-MM).")] = None,
    end_month: Annotated[Optional[str], Field(description="End month (YYYY-MM).")] = None,
    limit: Annotated[int, Field(description="Number of points to include.")] = 6,
) -> str:
    return _call_bridge(
        "segment_adoption_trend",
        segment=segment,
        metric=metric,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


def segment_adoption_leaders_tool(
    month: Annotated[Optional[str], Field(description="Optional month (YYYY-MM).")] = None,
    metric: Annotated[str, Field(description="fte_adoption | non_fte_adoption | fte_active | non_fte_active")] = "fte_adoption",
    limit: Annotated[int, Field(description="Number of segments to list.")] = 5,
) -> str:
    return _call_bridge(
        "segment_adoption_leaders",
        month=month,
        metric=metric,
        limit=limit,
    )


async def run_console_agent(mcp_url: str = "http://127.0.0.1:8000") -> None:
    global _BRIDGE
    _BRIDGE = McpBridge(base_url=mcp_url)

    # Load environment variables to get AZURE_AI_PROJECT_ENDPOINT and AZURE_AI_MODEL_DEPLOYMENT_NAME
    from pathlib import Path
    from dotenv import load_dotenv
    import os
    
    # Find and load .env file
    start_dir = Path(__file__).resolve().parent
    for directory in [start_dir, *start_dir.parents]:
        env_file = directory / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            break
    
    # Verify required environment variables
    if "AZURE_AI_PROJECT_ENDPOINT" not in os.environ:
        raise RuntimeError("AZURE_AI_PROJECT_ENDPOINT is missing from environment")

    async with AzureCliCredential() as credential:
        # Create the AzureAIAgentClient directly - it will create the AgentsClient internally
        async with AzureAIAgentClient(async_credential=credential) as client:
            analytics_instructions = (
                "You are the Copilot Usage Analytics agent. Use the registered MCP tools to ground all"
                " answers. Summaries must reference quantitative metrics, compare FTE and contractor"
                " adoption when relevant, and state when data is missing."
            )
            tools = [
                list_segments_tool,
                segment_adoption_summary_tool,
                segment_adoption_trend_tool,
                segment_adoption_leaders_tool,
                describe_metrics_tool,
            ]
            # Use the client's create_agent method (returns a ChatAgent, not a context manager)
            agent = client.create_agent(
                name="CopilotUsageOrchestrator",
                instructions=analytics_instructions,
                tools=tools,
            )
            
            print("Copilot Usage Orchestrator online. Type 'exit' to quit.\n")
            while True:
                user_query = input("Management: ").strip()
                if not user_query:
                    continue
                if user_query.lower() in {"exit", "quit"}:
                    print("Session ended.")
                    break
                guard_message = _run_guardrails(user_query)
                if guard_message:
                    print(f"Governance: {guard_message}\n")
                    continue
                response = await agent.run(user_query, store=True)
                print(f"Analytics: {response}\n")


if __name__ == "__main__":
    asyncio.run(run_console_agent())
