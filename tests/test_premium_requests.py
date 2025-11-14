"""Unit tests for PremiumRequestsAnalytics class.

These tests verify the analytics logic for premium requests data processing,
including filtering, aggregation, and trend calculations.

Run with: pytest tests/test_premium_requests.py
"""

import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict

import pandas as pd
import pytest

from services.premium_requests import (
    DateRange,
    PremiumRequestsAnalytics,
    AnalyticsConfigError,
)


@pytest.fixture
def sample_csv_data() -> str:
    """Create sample CSV data for testing."""
    return """enterprise,requestId,timestamp,callPath,model,mfcgd_id,is_employee,segment,gross_cost_usd,discount_pct,net_cost_usd
manulife EMU,req-001,2025-07-15T10:00:00Z,/chat,gpt-4o,user001@manulife.com,1,ASIA,0.50,10.0,0.45
manulife EMU,req-002,2025-07-16T11:00:00Z,/chat,gpt-4o,user002@manulife.com,0,ASIA,0.30,10.0,0.27
manulife-financial,req-003,2025-07-17T12:00:00Z,/completions,claude-3.5-sonnet,user003@manulife.com,1,CANADA,1.20,15.0,1.02
manulife EMU,req-004,2025-08-15T10:00:00Z,/chat,gpt-4o,user001@manulife.com,1,ASIA,0.60,10.0,0.54
manulife-financial,req-005,2025-08-16T11:00:00Z,/chat,claude-3.5-sonnet,user004@manulife.com,1,US,0.80,15.0,0.68
"""


@pytest.fixture
def temp_csv_file(tmp_path, sample_csv_data: str):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / "premium_requests_test.csv"
    csv_file.write_text(sample_csv_data)
    return str(csv_file)


@pytest.fixture
def analytics(temp_csv_file: str) -> PremiumRequestsAnalytics:
    """Create a PremiumRequestsAnalytics instance with test data."""
    return PremiumRequestsAnalytics(csv_path=temp_csv_file)


def test_initialization_with_valid_csv(temp_csv_file: str) -> None:
    """Test that analytics initializes correctly with valid CSV."""
    analytics = PremiumRequestsAnalytics(csv_path=temp_csv_file)
    assert analytics is not None
    assert len(analytics._df) == 5  # 5 records in sample data


def test_initialization_with_missing_csv() -> None:
    """Test that initialization fails with missing CSV."""
    with pytest.raises(AnalyticsConfigError, match="Premium requests CSV file not found"):
        PremiumRequestsAnalytics(csv_path="/nonexistent/file.csv")


def test_initialization_with_invalid_csv(tmp_path) -> None:
    """Test that initialization fails with invalid CSV structure."""
    invalid_csv = tmp_path / "invalid.csv"
    invalid_csv.write_text("col1,col2\nval1,val2\n")
    
    with pytest.raises(AnalyticsConfigError, match="Missing required columns"):
        PremiumRequestsAnalytics(csv_path=str(invalid_csv))


def test_summary_all_data(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary with all data (no filters)."""
    result = analytics.summary()
    
    assert "Premium request summary" in result
    assert "Total requests: 5" in result
    assert "Unique users: 4" in result
    assert "Total gross cost: $3.40" in result
    assert "Total net cost: $2.96" in result


def test_summary_filtered_by_segment(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary filtered by segment."""
    result = analytics.summary(segment="ASIA")
    
    assert "segment: ASIA" in result
    assert "Total requests: 3" in result  # 3 ASIA requests
    assert "Unique users: 2" in result


def test_summary_filtered_by_user_type_fte(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary filtered by FTE users only."""
    result = analytics.summary(user_type="fte")
    
    assert "user type: FTE" in result
    assert "Total requests: 4" in result  # 4 FTE requests


def test_summary_filtered_by_user_type_contractor(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary filtered by contractor users only."""
    result = analytics.summary(user_type="contractor")
    
    assert "user type: Contractor" in result
    assert "Total requests: 1" in result  # 1 contractor request


def test_summary_filtered_by_date_range(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary filtered by date range."""
    result = analytics.summary(
        start_month="2025-07",
        end_month="2025-07"
    )
    
    assert "Period: 2025-07 to 2025-07" in result
    assert "Total requests: 3" in result  # 3 July requests


def test_summary_with_multiple_filters(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary with multiple filters combined."""
    result = analytics.summary(
        segment="ASIA",
        user_type="fte",
        start_month="2025-07",
        end_month="2025-07"
    )
    
    assert "segment: ASIA" in result
    assert "user type: FTE" in result
    assert "Period: 2025-07 to 2025-07" in result
    assert "Total requests: 1" in result  # Only 1 record matches all filters


def test_summary_with_no_matching_data(analytics: PremiumRequestsAnalytics) -> None:
    """Test summary when no data matches filters."""
    result = analytics.summary(
        segment="NONEXISTENT",
        start_month="2025-01",
        end_month="2025-01"
    )
    
    assert "No premium request records found" in result


def test_trend_by_cost(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis by cost metric."""
    result = analytics.trend(metric="cost", limit=6)
    
    assert "Premium requests trend" in result
    assert "metric: cost" in result
    assert "2025-07" in result
    assert "2025-08" in result


def test_trend_by_requests(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis by requests metric."""
    result = analytics.trend(metric="requests", limit=6)
    
    assert "metric: requests" in result
    assert "2025-07" in result
    assert "2025-08" in result


def test_trend_by_users(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis by users metric."""
    result = analytics.trend(metric="users", limit=6)
    
    assert "metric: users" in result
    assert "2025-07" in result
    assert "2025-08" in result


def test_trend_with_user_type_filter(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis with user type filter."""
    result = analytics.trend(metric="cost", user_type="fte", limit=6)
    
    assert "user type: FTE" in result


def test_trend_with_limit(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis respects limit parameter."""
    result = analytics.trend(metric="cost", limit=1)
    
    # Should only show 1 month (the most recent)
    lines = [line for line in result.split('\n') if line.strip() and not line.startswith('Premium')]
    month_lines = [line for line in lines if '2025-' in line]
    assert len(month_lines) <= 1


def test_trend_invalid_metric(analytics: PremiumRequestsAnalytics) -> None:
    """Test trend analysis with invalid metric."""
    with pytest.raises(AnalyticsConfigError, match="must be one of"):
        analytics.trend(metric="invalid_metric")


def test_top_segments_by_cost(analytics: PremiumRequestsAnalytics) -> None:
    """Test top segments ranking by cost."""
    result = analytics.top_segments(metric="cost", limit=5)
    
    assert "Top segments" in result
    assert "metric: cost" in result
    assert "ASIA" in result or "CANADA" in result


def test_top_segments_by_requests(analytics: PremiumRequestsAnalytics) -> None:
    """Test top segments ranking by requests."""
    result = analytics.top_segments(metric="requests", limit=5)
    
    assert "metric: requests" in result


def test_top_segments_by_users(analytics: PremiumRequestsAnalytics) -> None:
    """Test top segments ranking by users."""
    result = analytics.top_segments(metric="users", limit=5)
    
    assert "metric: users" in result


def test_top_segments_with_user_type_filter(analytics: PremiumRequestsAnalytics) -> None:
    """Test top segments with user type filter."""
    result = analytics.top_segments(metric="cost", user_type="fte", limit=5)
    
    assert "user type: FTE" in result


def test_top_segments_with_limit(analytics: PremiumRequestsAnalytics) -> None:
    """Test top segments respects limit parameter."""
    result = analytics.top_segments(metric="cost", limit=2)
    
    # Count segment entries (lines with segment names)
    lines = result.split('\n')
    segment_lines = [line for line in lines if any(seg in line for seg in ['ASIA', 'CANADA', 'US'])]
    assert len(segment_lines) <= 2


def test_top_models(analytics: PremiumRequestsAnalytics) -> None:
    """Test top models ranking."""
    result = analytics.top_models(limit=10)
    
    assert "Top AI models" in result
    assert "gpt-4o" in result or "claude-3.5-sonnet" in result


def test_top_models_with_limit(analytics: PremiumRequestsAnalytics) -> None:
    """Test top models respects limit parameter."""
    result = analytics.top_models(limit=1)
    
    # Should only show 1 model
    lines = result.split('\n')
    model_lines = [line for line in lines if 'gpt' in line.lower() or 'claude' in line.lower()]
    assert len(model_lines) <= 1


def test_top_models_with_date_filter(analytics: PremiumRequestsAnalytics) -> None:
    """Test top models with date range filter."""
    result = analytics.top_models(
        start_month="2025-07",
        end_month="2025-07",
        limit=10
    )
    
    assert "Period: 2025-07 to 2025-07" in result


def test_enterprise_breakdown(analytics: PremiumRequestsAnalytics) -> None:
    """Test enterprise breakdown (EMU vs Legacy)."""
    result = analytics.enterprise_breakdown()
    
    assert "Enterprise breakdown" in result
    assert "manulife EMU" in result
    assert "manulife-financial" in result


def test_enterprise_breakdown_with_date_filter(analytics: PremiumRequestsAnalytics) -> None:
    """Test enterprise breakdown with date range filter."""
    result = analytics.enterprise_breakdown(
        start_month="2025-07",
        end_month="2025-07"
    )
    
    assert "Period: 2025-07 to 2025-07" in result
    assert "manulife EMU" in result or "manulife-financial" in result


def test_date_range_parsing(analytics: PremiumRequestsAnalytics) -> None:
    """Test that date range parsing works correctly."""
    # Test valid date formats
    result = analytics.summary(start_month="2025-07", end_month="2025-08")
    assert "Period: 2025-07 to 2025-08" in result
    
    # Test with full timestamps (should be trimmed to month)
    result = analytics.summary(
        start_month="2025-07-01T00:00:00Z",
        end_month="2025-08-31T23:59:59Z"
    )
    assert "Period: 2025-07 to 2025-08" in result


def test_invalid_user_type(analytics: PremiumRequestsAnalytics) -> None:
    """Test that invalid user type raises error."""
    with pytest.raises(AnalyticsConfigError, match="must be one of"):
        analytics.summary(user_type="invalid_type")


def test_data_cleaning_on_load(temp_csv_file: str) -> None:
    """Test that data is cleaned properly on load."""
    analytics = PremiumRequestsAnalytics(csv_path=temp_csv_file)
    df = analytics._df
    
    # Check that month column is created
    assert 'month' in df.columns
    
    # Check that timestamps are parsed to datetime
    assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])
    
    # Check that numeric columns are numeric
    assert pd.api.types.is_numeric_dtype(df['gross_cost_usd'])
    assert pd.api.types.is_numeric_dtype(df['net_cost_usd'])
    
    # Check that is_employee is boolean
    assert pd.api.types.is_bool_dtype(df['is_employee'])


def test_mfcgd_id_normalization(temp_csv_file: str) -> None:
    """Test that mfcgd_id is normalized to lowercase."""
    analytics = PremiumRequestsAnalytics(csv_path=temp_csv_file)
    df = analytics._df
    
    # All mfcgd_id values should be lowercase
    assert df['mfcgd_id'].str.islower().all()


def test_empty_csv_handling(tmp_path) -> None:
    """Test handling of empty CSV file."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("enterprise,requestId,timestamp,callPath,model,mfcgd_id,is_employee,segment,gross_cost_usd,discount_pct,net_cost_usd\n")
    
    analytics = PremiumRequestsAnalytics(csv_path=str(empty_csv))
    result = analytics.summary()
    
    assert "No premium request records found" in result
