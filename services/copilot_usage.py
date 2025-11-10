"""Analytics utilities for GitHub Copilot usage data.

This module loads pre-aggregated CSV exports that describe GitHub Copilot
adoption across an engineering organization and exposes helper methods used by
agent tools.

Expected CSV schemas
--------------------
1. Developer monthly usage (``COPILOT_USAGE_CSV``)
   Required columns:
     - ``developer_id``: Unique identifier for a developer
     - ``division``: Organization division name for the developer
     - ``month``: Month in ``YYYY-MM`` format representing the reporting period

2. Interaction metrics (``COPILOT_INTERACTIONS_CSV``)
   Required columns:
     - ``developer_id``
     - ``timestamp`` *or* ``month`` (``YYYY-MM``)
   Optional columns that increase fidelity when present:
     - ``division``
     - ``model``
     - ``request_count`` / ``requests`` / ``num_requests``
     - ``lines_suggested`` / ``suggested_lines``
     - ``lines_accepted`` / ``accepted_lines``

Additional columns are ignored. When divisions are missing in the interaction
CSV the loader joins the data from the developer monthly usage file.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, cast

import pandas as pd
from pandas import DataFrame, Series


class AnalyticsConfigError(RuntimeError):
    """Raised when required analytics inputs are missing or malformed."""


@dataclass(frozen=True)
class DateRange:
    """Simple helper to format month ranges."""

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


class CopilotUsageAnalytics:
    """Encapsulates data loading and analytics routines for Copilot usage."""

    def __init__(self, usage_csv: Path, interactions_csv: Path) -> None:
        self.usage_csv = usage_csv
        self.interactions_csv = interactions_csv

        if not self.usage_csv.exists():
            raise AnalyticsConfigError(
                f"Developer usage CSV not found at {self.usage_csv}. Set COPILOT_USAGE_CSV."
            )
        if not self.interactions_csv.exists():
            raise AnalyticsConfigError(
                "Interaction metrics CSV not found at "
                f"{self.interactions_csv}. Set COPILOT_INTERACTIONS_CSV."
            )

        self.developer_usage = self._load_developer_usage()
        self.interactions = self._load_interactions()

        self._request_column = self._select_first_numeric(
            self.interactions, ["request_count", "requests", "num_requests", "total_requests"]
        )
        self._suggested_column = self._select_first_numeric(
            self.interactions,
            ["lines_suggested", "suggested_lines", "lines_generated", "tokens_suggested"],
        )
        self._accepted_column = self._select_first_numeric(
            self.interactions,
            ["lines_accepted", "accepted_lines", "lines_committed", "tokens_accepted"],
        )

    def available_divisions(self) -> list[str]:
        return sorted(self.developer_usage["division"].dropna().unique().tolist())

    def summarize_usage(
        self,
        division: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        population = self._population_size(division)
        if population == 0:
            return (
                "No developers found for the specified division. "
                "Verify the COPILOT_USAGE_CSV content."
            )

        scoped = self._filter_interactions(division, period)
        active_developers = scoped["developer_id"].nunique()
        total_requests = self._sum_numeric(scoped, self._request_column)
        total_requests = total_requests if total_requests is not None else len(scoped)
        suggested_lines = self._sum_numeric(scoped, self._suggested_column)
        accepted_lines = self._sum_numeric(scoped, self._accepted_column)

        adoption_rate = (active_developers / population) * 100 if population else 0.0
        acceptance_rate = None
        if suggested_lines:
            acceptance_rate = (accepted_lines or 0) / suggested_lines * 100

        top_models = self._model_mix(scoped, top_n=3)

        details: list[str] = []
        target = division or "all divisions"
        details.append(f"Scope: {target} during {period.description()}")
        details.append(
            f"Active developers: {active_developers} of {population} total "
            f"({adoption_rate:.1f}% adoption)"
        )
        details.append(f"Copilot requests: {total_requests:,}")
        if suggested_lines is not None and accepted_lines is not None:
            details.append(
                "Lines accepted: "
                f"{accepted_lines:,} of {suggested_lines:,} "
                f"({acceptance_rate:.1f}% acceptance)" if acceptance_rate is not None else
                f"{accepted_lines:,} of {suggested_lines:,}"
            )
        elif accepted_lines is not None:
            details.append(f"Lines accepted: {accepted_lines:,}")
        if top_models:
            details.append("Top models: " + ", ".join(top_models))

        return "\n".join(details)

    def adoption_trend(
        self,
        division: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 6,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter_interactions(division, period)
        if scoped.empty:
            return "No interaction records match the requested scope."

        adoption = (
            scoped.groupby("month")["developer_id"].nunique().rename("active_developers")
        )
        population = self._population_by_month(division)
        joined = adoption.to_frame().join(population, how="left")
        joined["population"] = joined["population"].ffill()
        joined.dropna(subset=["population"], inplace=True)
        if joined.empty:
            return "Population data is missing for the requested period."

        joined["adoption_rate"] = (joined["active_developers"] / joined["population"]) * 100
        joined.sort_index(inplace=True)
        rows = joined.tail(limit)

        summary_lines = [
            f"Adoption trend for {division or 'all divisions'}," f" {period.description()}:"
        ]
        for record in rows.itertuples():
            month_value: Any = record.Index
            month_label = (
                month_value.strftime("%Y-%m")
                if hasattr(month_value, "strftime")
                else str(month_value)
            )
            active = int(getattr(record, "active_developers"))
            population_val = int(getattr(record, "population"))
            adoption_pct = float(getattr(record, "adoption_rate"))
            summary_lines.append(
                f"- {month_label}: {adoption_pct:.1f}% ({active} of {population_val})"
            )
        return "\n".join(summary_lines)

    def model_mix(
        self,
        division: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 5,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter_interactions(division, period)
        if scoped.empty:
            return "Model usage data is not available for the requested scope."
        if "model" not in scoped.columns:
            return "The interaction dataset does not contain a model column."

        requests = self._sum_numeric(scoped, self._request_column)
        weight = self._request_column if requests else None
        if weight:
            mix = scoped.groupby("model")[weight].sum()
        else:
            mix = scoped.groupby("model").size()

        total = float(mix.sum())
        if total == 0:
            return "No model usage captured for the requested scope."

        mix.sort_values(ascending=False, inplace=True)
        lines = [
            f"Model mix for {division or 'all divisions'} during {period.description()}:"
        ]
        for model, value in mix.head(limit).items():
            share = (value / total) * 100
            lines.append(f"- {model}: {share:.1f}% of usage")
        return "\n".join(lines)

    def division_breakdown(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 5,
    ) -> str:
        period = self._normalize_range(start_month, end_month)
        scoped = self._filter_interactions(None, period)
        if scoped.empty:
            return "No interactions available to compute division breakdown."

        if "division" not in scoped.columns:
            return "Division information is missing from the datasets."

        counts = scoped.groupby("division")["developer_id"].nunique()
        counts.sort_values(ascending=False, inplace=True)
        lines = [f"Top divisions by active Copilot users ({period.description()}):"]
        for division_value, total in counts.head(limit).items():
            division_label = str(division_value)
            population = self._population_size(division_label)
            adoption = (total / population) * 100 if population else 0
            lines.append(
                f"- {division_label}: {total} active developers ({adoption:.1f}% adoption)"
            )
        return "\n".join(lines)

    def _load_developer_usage(self) -> pd.DataFrame:
        df = pd.read_csv(self.usage_csv)
        required = {"developer_id", "division", "month"}
        missing = required - set(df.columns)
        if missing:
            raise AnalyticsConfigError(
                "Developer usage CSV is missing required columns: " + ", ".join(sorted(missing))
            )
        df = df.copy()
        df["division"] = df["division"].fillna("Unassigned")
        df["month"] = self._coerce_month(df["month"], source="developer usage")
        df.dropna(subset=["month"], inplace=True)
        return df

    def _load_interactions(self) -> pd.DataFrame:
        df = pd.read_csv(self.interactions_csv)
        if "month" in df.columns:
            df["month"] = self._coerce_month(df["month"], source="interaction metrics")
        elif "timestamp" in df.columns:
            timestamps = cast(Series, pd.to_datetime(df["timestamp"], errors="coerce"))
            if timestamps.isna().all():
                raise AnalyticsConfigError(
                    "Interaction metrics CSV contains invalid timestamp values."
                )
            df["month"] = timestamps.dt.to_period("M")
        else:
            raise AnalyticsConfigError(
                "Interaction metrics CSV must include a 'month' column or a 'timestamp' column."
            )
        df.dropna(subset=["month"], inplace=True)

        if "division" not in df.columns:
            df = cast(
                DataFrame,
                df.merge(
                    self.developer_usage[["developer_id", "division"]].drop_duplicates(),
                    on="developer_id",
                    how="left",
                ),
            )
        return cast(DataFrame, df)

    def _select_first_numeric(
        self, df: pd.DataFrame, candidates: Sequence[str]
    ) -> Optional[str]:
        for column in candidates:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                return column
        return None

    def _sum_numeric(self, df: pd.DataFrame, column: Optional[str]) -> Optional[float]:
        if not column or column not in df.columns:
            return None
        value = df[column].sum()
        return float(value)

    def _population_size(self, division: Optional[str]) -> int:
        if division:
            filtered = self.developer_usage[
                self.developer_usage["division"].str.casefold() == division.casefold()
            ]
            return int(filtered["developer_id"].nunique())
        return int(self.developer_usage["developer_id"].nunique())

    def _population_by_month(self, division: Optional[str]) -> pd.Series:
        scoped = self.developer_usage
        if division:
            scoped = scoped[scoped["division"].str.casefold() == division.casefold()]
        population = scoped.groupby("month")["developer_id"].nunique()
        return population.rename("population")

    def _filter_interactions(
        self, division: Optional[str], period: DateRange
    ) -> pd.DataFrame:
        df: DataFrame = self.interactions
        if division:
            df = cast(DataFrame, df.loc[df["division"].str.casefold() == division.casefold()])
        if period.start is not None:
            df = cast(DataFrame, df.loc[df["month"] >= period.start])
        if period.end is not None:
            df = cast(DataFrame, df.loc[df["month"] <= period.end])
        return df

    def _model_mix(self, df: pd.DataFrame, top_n: int) -> list[str]:
        if df.empty or "model" not in df.columns:
            return []
        weight_col = self._request_column if self._request_column in df.columns else None
        if weight_col:
            counts = df.groupby("model")[weight_col].sum()
        else:
            counts = df.groupby("model").size()
        total = float(counts.sum())
        if total == 0:
            return []
        counts.sort_values(ascending=False, inplace=True)
        formatted = []
        for model, value in counts.head(top_n).items():
            share = (value / total) * 100
            formatted.append(f"{model}: {share:.1f}%")
        return formatted

    def _normalize_range(
        self, start_month: Optional[str], end_month: Optional[str]
    ) -> DateRange:
        start = self._parse_month(start_month) if start_month else None
        end = self._parse_month(end_month) if end_month else None
        if start and end and start > end:
            raise AnalyticsConfigError("start_month must be earlier than end_month")
        return DateRange(start=start, end=end)

    def _parse_month(self, value: str) -> pd.Period:
        try:
            return pd.Period(value, freq="M")
        except Exception as exc:  # pragma: no cover - defensive parsing
            raise AnalyticsConfigError(
                f"Unable to parse '{value}' as YYYY-MM month value"
            ) from exc

    def _coerce_month(self, raw: Iterable, source: str) -> pd.PeriodIndex:
        parsed = cast(Series, pd.to_datetime(raw, errors="coerce"))
        if parsed.isna().all():
            raise AnalyticsConfigError(
                f"Failed to parse month values in {source}. Ensure YYYY-MM format."
            )
        return pd.PeriodIndex(parsed.dt.to_period("M"), freq="M")


__all__ = ["CopilotUsageAnalytics", "AnalyticsConfigError"]
