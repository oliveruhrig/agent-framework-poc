from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

import yaml

_DEFAULT_PATH = Path("config/metrics.yaml")


class MetricsRegistryError(RuntimeError):
    """Raised when the metrics registry cannot be loaded."""


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    definition: str
    owner: str
    min_aggregation_size: int
    freshness_days: int

    def as_bullet(self) -> str:
        return (
            f"{self.name} — {self.definition.strip()} (owner: {self.owner}; "
            f"min aggregation {self.min_aggregation_size}; refreshed every "
            f"{self.freshness_days} days)"
        )


class MetricsRegistry:
    """Loads and provides access to the analytics metric catalogue."""

    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = path or _DEFAULT_PATH
        if not self._path.exists():
            raise MetricsRegistryError(
                f"Metrics registry not found at {self._path.resolve()}"
            )
        self._metrics = self._load(self._path)

    def describe_metrics(self, metric_ids: Optional[Iterable[str]] = None) -> Dict[str, MetricDefinition]:
        if metric_ids is None:
            return dict(self._metrics)
        result: Dict[str, MetricDefinition] = {}
        for key in metric_ids:
            if key in self._metrics:
                result[key] = self._metrics[key]
        return result

    def as_markdown(self, metric_ids: Optional[Iterable[str]] = None) -> str:
        selected = self.describe_metrics(metric_ids)
        if not selected:
            return "No metric definitions available for the requested identifiers."
        lines = ["Metric catalogue:"]
        for metric in selected.values():
            lines.append(f"- {metric.as_bullet()}")
        return "\n".join(lines)

    def _load(self, path: Path) -> Dict[str, MetricDefinition]:
        try:
            raw = yaml.safe_load(path.read_text())
        except Exception as exc:  # pragma: no cover - defensive path
            raise MetricsRegistryError(f"Unable to parse metrics file {path}: {exc}") from exc
        metrics_block = raw.get("metrics", {}) if isinstance(raw, dict) else {}
        parsed: Dict[str, MetricDefinition] = {}
        for key, payload in metrics_block.items():
            try:
                parsed[key] = MetricDefinition(
                    name=str(payload["name"]),
                    definition=str(payload["definition"]),
                    owner=str(payload["owner"]),
                    min_aggregation_size=int(payload["min_aggregation_size"]),
                    freshness_days=int(payload["freshness_days"]),
                )
            except KeyError as exc:
                raise MetricsRegistryError(
                    f"Metric '{key}' is missing the required field {exc.args[0]}"
                ) from exc
        return parsed


__all__ = ["MetricsRegistry", "MetricsRegistryError", "MetricDefinition"]
