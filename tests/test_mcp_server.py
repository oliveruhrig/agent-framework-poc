"""Functional tests for MCP server endpoints.

These tests verify that the MCP server correctly exposes all analytics tools
and returns expected response formats. They require the CSV data files to be
present and the server to be running.

Run with: pytest tests/test_mcp_server.py
Or to skip server tests: pytest tests/test_mcp_server.py -k "not server"
"""

import json
from typing import Any, Dict

import httpx
import pytest


MCP_BASE_URL = "http://127.0.0.1:8000"


@pytest.fixture
def mcp_client() -> httpx.Client:
    """Create an HTTP client for MCP server requests."""
    return httpx.Client(base_url=MCP_BASE_URL, timeout=30.0)


def test_health_endpoint(mcp_client: httpx.Client) -> None:
    """Verify the health endpoint returns status for all modules."""
    response = mcp_client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "ok"
    assert "segmentAnalytics" in data
    assert "premiumAnalytics" in data
    assert "metrics" in data


def test_list_tools_endpoint(mcp_client: httpx.Client) -> None:
    """Verify the tools endpoint lists all available tools."""
    response = mcp_client.get("/mcp/tools")
    assert response.status_code == 200
    
    tools = response.json()
    assert isinstance(tools, list)
    assert len(tools) > 0
    
    # Check for expected tool names
    tool_names = {tool["name"] for tool in tools}
    
    # Segment adoption tools
    assert "segment_adoption_segments" in tool_names
    assert "segment_adoption_summary" in tool_names
    assert "segment_adoption_trend" in tool_names
    assert "segment_adoption_leaders" in tool_names
    
    # Premium request tools
    assert "premium_requests_summary" in tool_names
    assert "premium_requests_trend" in tool_names
    assert "premium_requests_top_segments" in tool_names
    assert "premium_requests_top_models" in tool_names
    assert "premium_requests_enterprise_breakdown" in tool_names
    
    # Metrics tool
    assert "describe_metrics" in tool_names


def test_segment_adoption_segments(mcp_client: httpx.Client) -> None:
    """Test listing available segments."""
    payload = {
        "tool_name": "segment_adoption_segments",
        "arguments": {}
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert data["tool_name"] == "segment_adoption_segments"
    assert "result" in data
    assert "Available segments:" in data["result"] or "No segments" in data["result"]


def test_segment_adoption_summary(mcp_client: httpx.Client) -> None:
    """Test segment adoption summary without filters."""
    payload = {
        "tool_name": "segment_adoption_summary",
        "arguments": {}
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert data["tool_name"] == "segment_adoption_summary"
    assert "result" in data
    assert "all segments" in data["result"]


def test_segment_adoption_summary_with_filters(mcp_client: httpx.Client) -> None:
    """Test segment adoption summary with segment and date filters."""
    payload = {
        "tool_name": "segment_adoption_summary",
        "arguments": {
            "segment": "ASIA",
            "start_month": "2025-01",
            "end_month": "2025-07"
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    # Should either have data or indicate no records found
    assert ("ASIA" in data["result"] or "No segment adoption records" in data["result"])


def test_segment_adoption_trend(mcp_client: httpx.Client) -> None:
    """Test segment adoption trend analysis."""
    payload = {
        "tool_name": "segment_adoption_trend",
        "arguments": {
            "metric": "fte_adoption",
            "limit": 6
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "trend" in data["result"]


def test_segment_adoption_leaders(mcp_client: httpx.Client) -> None:
    """Test segment adoption leaders ranking."""
    payload = {
        "tool_name": "segment_adoption_leaders",
        "arguments": {
            "metric": "fte_adoption",
            "limit": 5
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "Top segments" in data["result"]


def test_premium_requests_summary(mcp_client: httpx.Client) -> None:
    """Test premium requests summary without filters."""
    payload = {
        "tool_name": "premium_requests_summary",
        "arguments": {}
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert data["tool_name"] == "premium_requests_summary"
    assert "result" in data
    assert "Premium request summary" in data["result"]


def test_premium_requests_summary_with_filters(mcp_client: httpx.Client) -> None:
    """Test premium requests summary with segment, user type, and date filters."""
    payload = {
        "tool_name": "premium_requests_summary",
        "arguments": {
            "segment": "ASIA",
            "user_type": "fte",
            "start_month": "2025-07",
            "end_month": "2025-07"
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    # Should have cost and request information
    result = data["result"]
    assert ("Total requests:" in result or "No premium request records" in result)


def test_premium_requests_trend(mcp_client: httpx.Client) -> None:
    """Test premium requests trend analysis."""
    payload = {
        "tool_name": "premium_requests_trend",
        "arguments": {
            "metric": "cost",
            "user_type": "all",
            "limit": 6
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "trend" in data["result"]


def test_premium_requests_top_segments(mcp_client: httpx.Client) -> None:
    """Test premium requests top segments ranking."""
    payload = {
        "tool_name": "premium_requests_top_segments",
        "arguments": {
            "metric": "cost",
            "user_type": "all",
            "limit": 5
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "Top segments" in data["result"]


def test_premium_requests_top_models(mcp_client: httpx.Client) -> None:
    """Test premium requests top models ranking."""
    payload = {
        "tool_name": "premium_requests_top_models",
        "arguments": {
            "limit": 10
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "Top AI models" in data["result"]


def test_premium_requests_enterprise_breakdown(mcp_client: httpx.Client) -> None:
    """Test premium requests enterprise breakdown (EMU vs Legacy)."""
    payload = {
        "tool_name": "premium_requests_enterprise_breakdown",
        "arguments": {
            "start_month": "2025-07",
            "end_month": "2025-07"
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    assert "Enterprise breakdown" in data["result"]


def test_describe_metrics(mcp_client: httpx.Client) -> None:
    """Test metrics catalogue description."""
    payload = {
        "tool_name": "describe_metrics",
        "arguments": {}
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
    # Should contain metric definitions
    result = data["result"]
    assert "utilisation" in result.lower() or "adoption" in result.lower()


def test_describe_specific_metrics(mcp_client: httpx.Client) -> None:
    """Test describing specific metrics by ID."""
    payload = {
        "tool_name": "describe_metrics",
        "arguments": {
            "metric_ids": ["fte_utilisation", "premium_request_cost"]
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data


def test_unknown_tool_returns_404(mcp_client: httpx.Client) -> None:
    """Test that requesting an unknown tool returns 404."""
    payload = {
        "tool_name": "nonexistent_tool",
        "arguments": {}
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 404


def test_invalid_arguments_handled_gracefully(mcp_client: httpx.Client) -> None:
    """Test that invalid arguments are handled gracefully."""
    payload = {
        "tool_name": "premium_requests_summary",
        "arguments": {
            "user_type": "invalid_type",
            "start_month": "not-a-date"
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    # Should either handle gracefully with 200 or return 400
    assert response.status_code in [200, 400]


# Parametrized tests for multiple scenarios
@pytest.mark.parametrize("user_type", ["fte", "contractor", "all"])
def test_premium_requests_user_types(mcp_client: httpx.Client, user_type: str) -> None:
    """Test premium requests summary with different user types."""
    payload = {
        "tool_name": "premium_requests_summary",
        "arguments": {
            "user_type": user_type
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data


@pytest.mark.parametrize("metric", ["requests", "cost", "users"])
def test_premium_requests_trend_metrics(mcp_client: httpx.Client, metric: str) -> None:
    """Test premium requests trend with different metric types."""
    payload = {
        "tool_name": "premium_requests_trend",
        "arguments": {
            "metric": metric,
            "limit": 3
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data


@pytest.mark.parametrize("metric", ["fte_adoption", "non_fte_adoption", "fte_active", "non_fte_active"])
def test_segment_adoption_trend_metrics(mcp_client: httpx.Client, metric: str) -> None:
    """Test segment adoption trend with different metric types."""
    payload = {
        "tool_name": "segment_adoption_trend",
        "arguments": {
            "metric": metric,
            "limit": 3
        }
    }
    response = mcp_client.post("/mcp/execute", json=payload)
    assert response.status_code == 200
    
    data = response.json()
    assert "result" in data
