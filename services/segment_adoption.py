from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from pandas import DataFrame

class AnalyticsConfigError(RuntimeError):
    """Raised when required analytics inputs are missing or malformed."""


@dataclass(frozen=True)
class DateRange:
    """Helper used to format month ranges for reporting."""

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

_SegmentMetric = Literal[
    "fte_adoption",
    "non_fte_adoption",
    "fte_active",
    "non_fte_active",
]


class SegmentAdoptionConfigError(AnalyticsConfigError):
    """Raised when the segment adoption dataset cannot be loaded."""


def _clean_cell(value: object) -> object:
    if not isinstance(value, str):
        return value
    cleaned = value.replace("\u00a0", " ").strip().strip("\"")
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("%", "")
    cleaned = cleaned.replace("-", "") if cleaned.replace("-", "").strip() == "" else cleaned
    cleaned = cleaned.strip()
    if cleaned in {"", "NA", "N/A", "None"}:
        return pd.NA
    return cleaned


def _safe_percentage(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = (numerator / denominator * 100).where((denominator > 0) & numerator.notna())
    return result.astype(float)


@dataclass(frozen=True)
class SegmentSummary:
    scope_label: str
    period: str
    fte_active: Optional[int]
    fte_seats: Optional[int]
    fte_coverage: Optional[float]
    fte_billing: Optional[float]
    contractor_active: Optional[int]
    contractor_seats: Optional[int]
    contractor_coverage: Optional[float]
    contractor_billing: Optional[float]

    def as_lines(self) -> list[str]:
        lines = [f"Segment adoption summary for {self.scope_label} during {self.period}:"]
        if self.fte_active is not None and self.fte_seats is not None:
            coverage = f" ({self.fte_coverage:.1f}% utilisation)" if self.fte_coverage is not None else ""
            billing = (
                f", billing programme {self.fte_billing:.1f}%"
                if self.fte_billing is not None
                else ""
            )
            lines.append(
                f"- FTE: {self.fte_active:,} active of {self.fte_seats:,} seats{coverage}{billing}"
            )
        if self.contractor_active is not None and self.contractor_seats is not None:
            if not (
                self.contractor_seats == 0
                and self.contractor_active == 0
                and self.contractor_billing is None
            ):
                coverage = (
                    f" ({self.contractor_coverage:.1f}% utilisation)"
                    if self.contractor_coverage is not None
                    else ""
                )
                billing = (
                    f", billing programme {self.contractor_billing:.1f}%"
                    if self.contractor_billing is not None
                    else ""
                )
                lines.append(
                    f"- Non-FTE: {self.contractor_active:,} active of {self.contractor_seats:,} seats{coverage}{billing}"
                )
        return lines


class SegmentAdoptionAnalytics:
    """Provides analytics over the aggregated segment-level adoption dataset."""

    def __init__(self, csv_path: Path) -> None:
        if not csv_path.exists():
            raise SegmentAdoptionConfigError(
                f"Segment adoption CSV not found at {csv_path}. Set COPILOT_SEGMENT_ADOPTION_CSV."
            )
        self.csv_path = csv_path
        self.data = self._load(csv_path)

    def available_segments(self) -> list[str]:
        return sorted(self.data["segment"].dropna().unique().tolist())

    def summary(
        self,
        segment: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, period)
        if scoped.empty:
            return "No segment adoption records match the requested scope."

        scope_label = segment or "all segments"
        summary = SegmentSummary(
            scope_label=scope_label,
            period=period.description(),
            fte_active=self._aggregate_int(scoped, "active_fte"),
            fte_seats=self._aggregate_int(scoped, "seats_fte"),
            fte_coverage=self._aggregate_percentage(scoped, "fte_utilisation_pct"),
            fte_billing=self._aggregate_percentage(scoped, "billing_adoption_fte"),
            contractor_active=self._aggregate_int(scoped, "active_non_fte"),
            contractor_seats=self._aggregate_int(scoped, "seats_non_fte"),
            contractor_coverage=self._aggregate_percentage(scoped, "non_fte_utilisation_pct"),
            contractor_billing=self._aggregate_percentage(scoped, "billing_adoption_non_fte"),
        )
        lines = summary.as_lines()

        peak = scoped.sort_values("fte_utilisation_pct", ascending=False).head(1)
        if not peak.empty:
            row = peak.iloc[0]
            month_label = row["month"].strftime("%Y-%m")
            utilisation = row["fte_utilisation_pct"]
            lines.append(
                f"Highest FTE coverage: {row['segment']} at {utilisation:.1f}% ({month_label})"
            )
        return "\n".join(lines)

    def trend(
        self,
        segment: Optional[str] = None,
        metric: _SegmentMetric = "fte_adoption",
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 6,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter(segment, period)
        if scoped.empty:
            return "No segment adoption records match the requested scope."

        grouped = self._group_monthly(scoped)
        if grouped.empty:
            return "No segment adoption records match the requested scope."

        metric_column, description = {
            "fte_adoption": ("fte_utilisation_pct", "FTE utilisation"),
            "non_fte_adoption": ("non_fte_utilisation_pct", "Non-FTE utilisation"),
            "fte_active": ("active_fte", "Active FTE"),
            "non_fte_active": ("active_non_fte", "Active Non-FTE"),
        }[metric]

        rows = grouped.tail(limit)
        scope_label = segment or "all segments"
        lines = [
            f"{description} trend for {scope_label} ({period.description()}):"
        ]
        for month, record in rows.iterrows():
            month_label = month.strftime("%Y-%m") if hasattr(month, "strftime") else str(month)
            value = record[metric_column]
            if pd.isna(value):
                lines.append(f"- {month_label}: no data")
                continue
            if metric in {"fte_active", "non_fte_active"}:
                lines.append(f"- {month_label}: {int(value):,}")
            else:
                lines.append(f"- {month_label}: {value:.1f}%")
        return "\n".join(lines)

    def leaders(
        self,
        month: Optional[str] = None,
        metric: _SegmentMetric = "fte_adoption",
        limit: int = 5,
    ) -> str:
        if month:
            target_month = self._parse_month(month)
            scoped = self.data[self.data["month"] == target_month]
            period_label = target_month.strftime("%Y-%m") if target_month else month
        else:
            scoped = self.data.copy()
            period_label = "all available months"
        if scoped.empty:
            return "No segment adoption data available for the requested period."

        aggregated = scoped.groupby("segment").agg(
            {
                "active_fte": "sum",
                "seats_fte": "sum",
                "active_non_fte": "sum",
                "seats_non_fte": "sum",
            }
        )
        aggregated["fte_utilisation_pct"] = _safe_percentage(
            aggregated["active_fte"], aggregated["seats_fte"]
        )
        aggregated["non_fte_utilisation_pct"] = _safe_percentage(
            aggregated["active_non_fte"], aggregated["seats_non_fte"]
        )

        metric_column, description = {
            "fte_adoption": ("fte_utilisation_pct", "FTE utilisation"),
            "non_fte_adoption": ("non_fte_utilisation_pct", "Non-FTE utilisation"),
            "fte_active": ("active_fte", "Active FTE"),
            "non_fte_active": ("active_non_fte", "Active Non-FTE"),
        }[metric]

        ordered = aggregated.sort_values(metric_column, ascending=False).head(limit)
        if ordered.empty:
            return "No segment adoption data available for the requested period."

        lines = [f"Top segments by {description} ({period_label}):"]
        for segment_name, row in ordered.iterrows():
            value = row[metric_column]
            if pd.isna(value):
                continue
            if metric in {"fte_active", "non_fte_active"}:
                lines.append(f"- {segment_name}: {int(value):,}")
            else:
                lines.append(f"- {segment_name}: {value:.1f}%")
        if len(lines) == 1:
            return "No segment adoption data available for the requested period."
        return "\n".join(lines)

    def _load(self, csv_path: Path) -> DataFrame:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df = df.applymap(_clean_cell)
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        rename_map = {
            "month": "month",
            "segment": "segment",
            "active_users_fte": "active_fte",
            "active_users_nonfte": "active_non_fte",
            "total_seats_fte": "seats_fte",
            "total_seats_nonfte": "seats_non_fte",
            "billing_adoption_fte": "billing_adoption_fte",
            "billing_adoption_nonfte": "billing_adoption_non_fte",
        }
        df = df.rename(columns=rename_map)
        required = {"month", "segment", "active_fte", "seats_fte"}
        missing = required - set(df.columns)
        if missing:
            raise SegmentAdoptionConfigError(
                "Segment adoption CSV missing required columns: " + ", ".join(sorted(missing))
            )

        df["month"] = pd.to_datetime(df["month"], errors="coerce").dt.to_period("M")
        df.dropna(subset=["month", "segment"], inplace=True)

        numeric_columns = [
            "active_fte",
            "active_non_fte",
            "seats_fte",
            "seats_non_fte",
            "billing_adoption_fte",
            "billing_adoption_non_fte",
        ]
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        df["active_non_fte"].fillna(0, inplace=True)
        df["seats_non_fte"].fillna(0, inplace=True)

        df["fte_utilisation_pct"] = _safe_percentage(df["active_fte"], df["seats_fte"])
        df["non_fte_utilisation_pct"] = _safe_percentage(
            df["active_non_fte"], df["seats_non_fte"]
        )
        df["billing_adoption_fte"] = df["billing_adoption_fte"].astype(float)
        if "billing_adoption_non_fte" in df.columns:
            df["billing_adoption_non_fte"] = df["billing_adoption_non_fte"].astype(float)
        else:
            df["billing_adoption_non_fte"] = pd.NA

        return df

    def _filter(self, segment: Optional[str], period: DateRange) -> DataFrame:
        df = self.data
        if segment:
            df = df[df["segment"].str.casefold() == segment.casefold()]
        if period.start is not None:
            df = df[df["month"] >= period.start]
        if period.end is not None:
            df = df[df["month"] <= period.end]
        return df

    def _group_monthly(self, scoped: DataFrame) -> DataFrame:
        grouped = scoped.groupby("month").agg(
            {
                "active_fte": "sum",
                "seats_fte": "sum",
                "active_non_fte": "sum",
                "seats_non_fte": "sum",
                "fte_utilisation_pct": "mean",
                "non_fte_utilisation_pct": "mean",
            }
        )
        grouped["fte_utilisation_pct"] = _safe_percentage(
            grouped["active_fte"], grouped["seats_fte"]
        )
        grouped["non_fte_utilisation_pct"] = _safe_percentage(
            grouped["active_non_fte"], grouped["seats_non_fte"]
        )
        return grouped

    def _normalize_range(
        self, start_month: Optional[str], end_month: Optional[str]
    ) -> DateRange:
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise SegmentAdoptionConfigError("start_month must be earlier than end_month")
        return DateRange(start=start, end=end)

    def _parse_month(self, value: Optional[str]) -> Optional[pd.Period]:
        if value is None:
            return None
        try:
            return pd.Period(value, freq="M")
        except Exception as exc:  # pragma: no cover - defensive parsing
            raise SegmentAdoptionConfigError(
                f"Unable to parse '{value}' as YYYY-MM month value"
            ) from exc

    def _aggregate_int(self, df: DataFrame, column: str) -> Optional[int]:
        if column not in df.columns:
            return None
        series = df[column].dropna()
        if series.empty:
            return None
        return int(series.sum())

    def _aggregate_percentage(self, df: DataFrame, column: str) -> Optional[float]:
        if column not in df.columns:
            return None
        series = df[column].dropna()
        if series.empty:
            return None
        return float(series.mean())


__all__ = [
    "SegmentAdoptionAnalytics",
    "SegmentAdoptionConfigError",
]
