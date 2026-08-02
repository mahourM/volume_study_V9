from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True)
class ActorImportanceWeights:
    initial_size: Decimal = Decimal("1.0")
    refill_contracts: Decimal = Decimal("1.25")
    refill_count: Decimal = Decimal("1.0")
    executed_contracts: Decimal = Decimal("0.75")
    persistence: Decimal = Decimal("0.5")

    def __post_init__(self) -> None:
        for field_name in (
            "initial_size",
            "refill_contracts",
            "refill_count",
            "executed_contracts",
            "persistence",
        ):
            value = _decimal_value(getattr(self, field_name), field_name)
            if value < 0:
                raise ValueError(f"{field_name} weight must be non-negative")
            object.__setattr__(self, field_name, value)


@dataclass(frozen=True)
class ActorProxyConfig:
    importance_weights: ActorImportanceWeights = field(default_factory=ActorImportanceWeights)
    max_tracked_candidates: int = 8
    tick_size: Decimal = Decimal("0.25")
    replacement_tick_distance: int = 2
    replacement_time_gap_ms: int = 2_000
    size_similarity_min: Decimal = Decimal("0.50")
    size_similarity_max: Decimal = Decimal("2.00")
    recent_window_ms: int = 5_000
    active_contract_ratio_exit_threshold: Decimal = Decimal("0.35")
    refill_rate_drop_ratio: Decimal = Decimal("0.25")
    confirm_window_ms: int = 1_000
    cluster_exit_score_threshold: Decimal = Decimal("0.65")
    live_queue_maxsize: int = 10_000

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tick_size",
            _positive_decimal_value(self.tick_size, "tick_size"),
        )
        object.__setattr__(
            self,
            "size_similarity_min",
            _non_negative_decimal_value(self.size_similarity_min, "size_similarity_min"),
        )
        object.__setattr__(
            self,
            "size_similarity_max",
            _non_negative_decimal_value(self.size_similarity_max, "size_similarity_max"),
        )
        object.__setattr__(
            self,
            "active_contract_ratio_exit_threshold",
            _bounded_ratio(
                self.active_contract_ratio_exit_threshold,
                "active_contract_ratio_exit_threshold",
            ),
        )
        object.__setattr__(
            self,
            "refill_rate_drop_ratio",
            _bounded_ratio(self.refill_rate_drop_ratio, "refill_rate_drop_ratio"),
        )
        object.__setattr__(
            self,
            "cluster_exit_score_threshold",
            _bounded_ratio(self.cluster_exit_score_threshold, "cluster_exit_score_threshold"),
        )
        for field_name in (
            "max_tracked_candidates",
            "replacement_tick_distance",
            "replacement_time_gap_ms",
            "recent_window_ms",
            "confirm_window_ms",
            "live_queue_maxsize",
        ):
            value = int(getattr(self, field_name))
            if value <= 0:
                raise ValueError(f"{field_name} must be positive")
            object.__setattr__(self, field_name, value)
        if self.size_similarity_max < self.size_similarity_min:
            raise ValueError("size_similarity_max must be greater than or equal to size_similarity_min")


def _decimal_value(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal value") from exc


def _positive_decimal_value(value: Any, field_name: str) -> Decimal:
    result = _decimal_value(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _non_negative_decimal_value(value: Any, field_name: str) -> Decimal:
    result = _decimal_value(value, field_name)
    if result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


def _bounded_ratio(value: Any, field_name: str) -> Decimal:
    result = _non_negative_decimal_value(value, field_name)
    if result > 1:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return result
