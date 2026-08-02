from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from typing import Iterable

from core.bin_alignment import DecimalLike, to_decimal


CONTRACT_SPIKE_SCALE = Decimal("0.6745")
CONTRACT_SPIKE_THRESHOLD = Decimal("12.0")
NORMAL_CORE_PERCENTILE = Decimal("0.75")


@dataclass(frozen=True)
class ContractSpikeMetrics:
    p75: Decimal
    normal_median: Decimal
    normal_mad: Decimal
    scores: tuple[Decimal, ...]
    score_deviation: Decimal


def calculate_contract_spike_metrics(values: Iterable[DecimalLike]) -> ContractSpikeMetrics:
    totals = tuple(max(to_decimal(value), Decimal("0")) for value in values)
    positive_totals = sorted(value for value in totals if value > 0)
    if not positive_totals:
        return ContractSpikeMetrics(
            p75=Decimal("0"),
            normal_median=Decimal("0"),
            normal_mad=Decimal("0"),
            scores=tuple(Decimal("0") for _ in totals),
            score_deviation=Decimal("0"),
        )

    p75 = _percentile(positive_totals, NORMAL_CORE_PERCENTILE)
    normal_core = [value for value in positive_totals if value <= p75]
    normal_median = _median(normal_core)
    normal_mad = _median(abs(value - normal_median) for value in normal_core)
    if normal_mad == 0:
        scores = tuple(Decimal("0") for _ in totals)
    else:
        scores = tuple(
            CONTRACT_SPIKE_SCALE * (value - normal_median) / normal_mad
            if value > 0
            else Decimal("0")
            for value in totals
        )
    traded_scores = tuple(
        score for total, score in zip(totals, scores) if total > 0
    )
    score_mean = sum(traded_scores, Decimal("0")) / Decimal(len(traded_scores))
    score_variance = sum(
        ((score - score_mean) ** 2 for score in traded_scores),
        Decimal("0"),
    ) / Decimal(len(traded_scores))

    return ContractSpikeMetrics(
        p75=p75,
        normal_median=normal_median,
        normal_mad=normal_mad,
        scores=scores,
        score_deviation=score_variance.sqrt(),
    )


def is_contract_spike(score: DecimalLike) -> bool:
    return to_decimal(score) >= CONTRACT_SPIKE_THRESHOLD


def _percentile(sorted_values: list[Decimal], percentile: Decimal) -> Decimal:
    if len(sorted_values) == 1:
        return sorted_values[0]
    bounded = min(max(percentile, Decimal("0")), Decimal("1"))
    rank = bounded * Decimal(len(sorted_values) - 1)
    lower_index = int(rank.to_integral_value(rounding=ROUND_FLOOR))
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    fraction = rank - Decimal(lower_index)
    return sorted_values[lower_index] + (
        (sorted_values[upper_index] - sorted_values[lower_index]) * fraction
    )


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        return Decimal("0")
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")
