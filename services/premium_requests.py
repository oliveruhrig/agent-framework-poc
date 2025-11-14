"""Analytics for GitHub Copilot Premium Requests across both enterprises.

This module processes premium request logs from the manulife (EMU) and 
manulife-financial (legacy) GitHub enterprises. Engineers may have accounts 
in both enterprises, joined by their Entra ID (mfcgd_id).

Expected CSV schema
-------------------
Required columns:
  - collection_date: Date the data was collected
  - enterprise: 'manulife' or 'manulife-financial'
  - request_date: Date of the premium request
  - gh_id: GitHub username (enterprise-specific)
  - model: AI model used for the request
  - quantity: Number of requests
  - gross_amount: Cost before free quota discount
  - discount_amount: Discount from 300 free requests per account/month
  - net_amount: Actual billable cost to Manulife
  - mfcgd_id: Entra ID username (cross-enterprise identifier)
  - is_employee: TRUE for FTE, FALSE for contractor
  - segment: Business segment (matches segment_adoption.csv)
  - exceeds_quota: Whether request exceeded free monthly quota
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from pandas import DataFrame


class AnalyticsConfigError(RuntimeError):
    """Raised when required analytics inputs are missing or malformed."""


class PremiumRequestsConfigError(AnalyticsConfigError):
    """Raised when the premium requests dataset cannot be loaded."""


@dataclass(frozen=True)
class DateRange:
    """Helper for formatting date ranges in reports."""

    start: Optional[pd.Period]
    end: Optional[pd.Period]

    def description(self) -> str:
        if self.start and self.end:
            if self.start == self.end:
                return self.start.strftime("%Y-%m")
            return f"{self.start.strftime('%Y-%m')} to {self.end.strftime('%Y-%m')}"
        if self.start:
            return f"from {self.start.strftime('%Y-%m')}"
        if self.end:
            return f"up to {self.end.strftime('%Y-%m')}"
        return "all available months"


def _clean_cell(value: object) -> object:
    """Normalize CSV cell values."""
    if not isinstance(value, str):
        return value
    cleaned = value.replace("\u00a0", " ").strip().strip('"')
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.strip()
    if cleaned in {"", "NA", "N/A", "None"}:
        return pd.NA
    return cleaned


_UserType = Literal["fte", "contractor", "all"]


class PremiumRequestsAnalytics:
    """Provides analytics over GitHub Copilot Premium Request logs."""

    def __init__(self, csv_path: Path) -> None:
        if not csv_path.exists():
            raise PremiumRequestsConfigError(
                f"Premium requests CSV not found at {csv_path}. Set COPILOT_PREMIUM_REQUESTS_CSV."
            )
        self.csv_path = csv_path
        self.data = self._load(csv_path)

    def available_segments(self) -> list[str]:
        """Return list of segments present in the dataset."""
        return sorted(self.data["segment"].dropna().unique().tolist())

    def available_enterprises(self) -> list[str]:
        """Return list of enterprises present in the dataset."""
        return sorted(self.data["enterprise"].dropna().unique().tolist())

    def available_models(self) -> list[str]:
        """Return list of AI models used in premium requests."""
        return sorted(self.data["model"].dropna().unique().tolist())

    def summary(
        self,
        segment: Optional[str] = None,
        user_type: _UserType = "all",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> str:
        """Summarise premium request usage, costs, and user counts."""
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, user_type, period)
        
        if scoped.empty:
            return "No premium request records match the requested scope."

        scope_label = self._scope_label(segment, user_type)
        total_requests = float(scoped["quantity"].sum())
        unique_users = int(scoped["mfcgd_id"].nunique())
        gross_cost = float(scoped["gross_amount"].sum())
        discount = float(scoped["discount_amount"].sum())
        net_cost = float(scoped["net_amount"].sum())
        exceeded_quota = int(scoped[scoped["exceeds_quota"] == True].shape[0])
        
        lines = [
            f"Premium request summary for {scope_label} during {period.description()}:",
            f"- Total requests: {total_requests:,.0f}",
            f"- Unique users (by Entra ID): {unique_users:,}",
            f"- Gross cost: ${gross_cost:,.2f}",
            f"- Discount (free quota): ${discount:,.2f}",
            f"- Net billable cost: ${net_cost:,.2f}",
        ]
        
        if exceeded_quota > 0:
            lines.append(f"- Requests exceeding quota: {exceeded_quota:,}")
        
        # Top models
        top_models = scoped.groupby("model")["quantity"].sum().sort_values(ascending=False).head(3)
        if not top_models.empty:
            model_list = ", ".join([f"{model} ({int(qty):,})" for model, qty in top_models.items()])
            lines.append(f"- Top models: {model_list}")
        
        return "\n".join(lines)

    def trend(
        self,
        segment: Optional[str] = None,
        user_type: _UserType = "all",
        metric: Literal["requests", "cost", "users"] = "requests",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 6,
    ) -> str:
        """Show month-by-month trend of requests, cost, or unique users."""
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, user_type, period)
        
        if scoped.empty:
            return "No premium request records match the requested scope."

        scope_label = self._scope_label(segment, user_type)
        
        if metric == "requests":
            monthly = scoped.groupby("month")["quantity"].sum()
            metric_name = "requests"
            format_fn = lambda x: f"{int(x):,}"
        elif metric == "cost":
            monthly = scoped.groupby("month")["net_amount"].sum()
            metric_name = "net cost"
            format_fn = lambda x: f"${x:,.2f}"
        else:  # users
            monthly = scoped.groupby("month")["mfcgd_id"].nunique()
            metric_name = "unique users"
            format_fn = lambda x: f"{int(x):,}"
        
        monthly = monthly.sort_index().tail(limit)
        
        lines = [f"Premium request {metric_name} trend for {scope_label} ({period.description()}):"]
        for month, value in monthly.items():
            month_label = month.strftime("%Y-%m") if hasattr(month, "strftime") else str(month)
            lines.append(f"- {month_label}: {format_fn(value)}")
        
        return "\n".join(lines)

    def top_segments(
        self,
        user_type: _UserType = "all",
        metric: Literal["requests", "cost", "users"] = "cost",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 5,
    ) -> str:
        """Rank segments by requests, cost, or unique user count."""
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(None, user_type, period)
        
        if scoped.empty:
            return "No premium request records match the requested scope."

        user_label = self._user_type_label(user_type)
        
        if metric == "requests":
            grouped = scoped.groupby("segment")["quantity"].sum()
            metric_name = "requests"
            format_fn = lambda x: f"{int(x):,}"
        elif metric == "cost":
            grouped = scoped.groupby("segment")["net_amount"].sum()
            metric_name = "net cost"
            format_fn = lambda x: f"${x:,.2f}"
        else:  # users
            grouped = scoped.groupby("segment")["mfcgd_id"].nunique()
            metric_name = "unique users"
            format_fn = lambda x: f"{int(x):,}"
        
        top = grouped.sort_values(ascending=False).head(limit)
        
        lines = [f"Top segments by premium request {metric_name} for {user_label} ({period.description()}):"]
        for segment, value in top.items():
            lines.append(f"- {segment}: {format_fn(value)}")
        
        return "\n".join(lines)

    def top_models(
        self,
        segment: Optional[str] = None,
        user_type: _UserType = "all",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 5,
    ) -> str:
        """Rank AI models by request volume and cost."""
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, user_type, period)
        
        if scoped.empty:
            return "No premium request records match the requested scope."

        scope_label = self._scope_label(segment, user_type)
        
        model_stats = scoped.groupby("model").agg({
            "quantity": "sum",
            "net_amount": "sum",
        }).sort_values("net_amount", ascending=False).head(limit)
        
        lines = [f"Top AI models by cost for {scope_label} ({period.description()}):"]
        for model, row in model_stats.iterrows():
            requests = int(row["quantity"])
            cost = float(row["net_amount"])
            lines.append(f"- {model}: {requests:,} requests, ${cost:,.2f} net cost")
        
        return "\n".join(lines)

    def enterprise_breakdown(
        self,
        segment: Optional[str] = None,
        user_type: _UserType = "all",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> str:
        """Compare usage across manulife (EMU) vs manulife-financial (legacy)."""
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, user_type, period)
        
        if scoped.empty:
            return "No premium request records match the requested scope."

        scope_label = self._scope_label(segment, user_type)
        
        enterprise_stats = scoped.groupby("enterprise").agg({
            "quantity": "sum",
            "net_amount": "sum",
            "mfcgd_id": "nunique",
        })
        
        lines = [f"Enterprise breakdown for {scope_label} ({period.description()}):"]
        for enterprise, row in enterprise_stats.iterrows():
            requests = int(row["quantity"])
            cost = float(row["net_amount"])
            users = int(row["mfcgd_id"])
            ent_label = "EMU (manulife)" if enterprise == "manulife" else "Legacy (manulife-financial)"
            lines.append(f"- {ent_label}: {requests:,} requests, ${cost:,.2f} cost, {users:,} users")
        
        return "\n".join(lines)

    def _load(self, csv_path: Path) -> DataFrame:
        """Load and normalize premium requests CSV."""
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df = df.applymap(_clean_cell)
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        
        required = {"request_date", "mfcgd_id", "enterprise", "model", "quantity", "gross_amount", "discount_amount", "net_amount", "segment", "is_employee"}
        missing = required - set(df.columns)
        if missing:
            raise PremiumRequestsConfigError(
                f"Premium requests CSV missing required columns: {', '.join(sorted(missing))}"
            )
        
        # Parse dates and extract month
        df["request_date"] = pd.to_datetime(df["request_date"], errors="coerce")
        df.dropna(subset=["request_date"], inplace=True)
        df["month"] = df["request_date"].dt.to_period("M")
        
        # Numeric conversions
        for col in ["quantity", "gross_amount", "discount_amount", "net_amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        # Boolean conversion for is_employee and exceeds_quota
        df["is_employee"] = df["is_employee"].str.upper().isin(["TRUE", "T", "1", "YES"])
        if "exceeds_quota" in df.columns:
            df["exceeds_quota"] = df["exceeds_quota"].str.upper().isin(["TRUE", "T", "1", "YES"])
        else:
            df["exceeds_quota"] = False
        
        # Clean segment and enterprise
        df["segment"] = df["segment"].fillna("Unassigned")
        df["enterprise"] = df["enterprise"].fillna("unknown")
        
        return df

    def _filter(
        self, 
        segment: Optional[str], 
        user_type: _UserType,
        period: DateRange,
    ) -> DataFrame:
        """Apply segment, user type, and date filters."""
        df = self.data
        
        if segment:
            df = df[df["segment"].str.casefold() == segment.casefold()]
        
        if user_type == "fte":
            df = df[df["is_employee"] == True]
        elif user_type == "contractor":
            df = df[df["is_employee"] == False]
        
        if period.start is not None:
            df = df[df["month"] >= period.start]
        if period.end is not None:
            df = df[df["month"] <= period.end]
        
        return df

    def _normalize_range(
        self, start_month: Optional[str], end_month: Optional[str]
    ) -> DateRange:
        """Parse and validate date range."""
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise PremiumRequestsConfigError("start_month must be earlier than end_month")
        return DateRange(start=start, end=end)

    def _parse_month(self, value: Optional[str]) -> Optional[pd.Period]:
        """Parse YYYY-MM month string."""
        if value is None:
            return None
        try:
            return pd.Period(value, freq="M")
        except Exception as exc:
            raise PremiumRequestsConfigError(
                f"Unable to parse '{value}' as YYYY-MM month value"
            ) from exc

    def _scope_label(self, segment: Optional[str], user_type: _UserType) -> str:
        """Generate human-readable scope description."""
        parts = []
        if segment:
            parts.append(segment)
        if user_type == "fte":
            parts.append("FTE")
        elif user_type == "contractor":
            parts.append("contractors")
        return " ".join(parts) if parts else "all users"

    def _user_type_label(self, user_type: _UserType) -> str:
        """Generate user type label."""
        if user_type == "fte":
            return "FTE"
        elif user_type == "contractor":
            return "contractors"
        return "all users"


__all__ = [
    "PremiumRequestsAnalytics",
    "PremiumRequestsConfigError",
]
