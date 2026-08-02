from __future__ import annotations

from dataclasses import dataclass, field

# Default importance weights per level type.
# POC is the single most important level in any profile.
# HVN marks local accumulation nodes — strong S/R.
# VAH / VAL mark the edges of the 70% value area — important but secondary.
_DEFAULT_TYPE_WEIGHTS: dict[str, float] = {
    "POC": 3.0,
    "HVN": 2.0,
    "VAH": 1.5,
    "VAL": 1.5,
}


@dataclass
class VolumeKeyLevelZone:
    """
    A price zone derived from one or more volume profile levels.

    zone_low / zone_high  — the merged tolerance band covering all contributing levels.
    price_center          — weighted-average price of contributing levels (weight = type weight).
    level_types           — unique level types contributing to this zone (e.g. ["POC", "HVN"]).
    source_profile_ids    — unique profile IDs whose levels fell into this zone.
    overlap_count         — number of distinct profiles contributing (≥2 means multi-profile confluence).
    strength_score        — higher = stronger zone:
                              base = Σ type_weight for every contributing level
                              bonus = (overlap_count − 1) × overlap_bonus_per_profile
    """

    zone_low: float
    zone_high: float
    price_center: float
    level_types: list[str] = field(default_factory=list)
    source_profile_ids: list[str] = field(default_factory=list)
    overlap_count: int = 1
    strength_score: float = 0.0

    def contains(self, price: float) -> bool:
        return self.zone_low <= price <= self.zone_high

    def distance_to(self, price: float) -> float:
        """Absolute distance from price to zone center."""
        return abs(price - self.price_center)

    def __repr__(self) -> str:
        types = "|".join(self.level_types)
        return (
            f"VolumeKeyLevelZone(center={self.price_center:.2f}, "
            f"band=[{self.zone_low:.2f}, {self.zone_high:.2f}], "
            f"types={types}, overlap={self.overlap_count}, strength={self.strength_score:.2f})"
        )


class VolumeKeyLevelAnalyzer:
    """
    Builds a consolidated map of key price levels from multiple volume profiles.

    Algorithm
    ---------
    1. Extract POC, VAH, VAL, and every HVN from each completed/live profile.
    2. Create a tolerance band  [price − tolerance, price + tolerance]  around each level.
    3. Merge all overlapping bands (sweep-line, O(n log n)) into unified zones.
    4. Score each zone:
         base_score   = Σ type_weight   for all contributing levels
         overlap_bonus = (unique_profile_count − 1) × overlap_bonus_per_profile
         strength     = base_score + overlap_bonus
    5. Return zones sorted by strength_score descending.

    Overlapping zones from different profiles receive a large bonus,
    making multi-timeframe confluences clearly stand out.

    Parameters
    ----------
    tolerance : float
        Absolute price tolerance added symmetrically around each level.
        Typical value: 1.5 × fixed_bin_size (e.g. $15 for BTC with $10 bins).
    level_type_weights : dict[str, float] | None
        Per-level-type importance weight.  Defaults to POC=3, HVN=2, VAH=1.5, VAL=1.5.
    overlap_bonus_per_profile : float
        Strength bonus added for each *additional* profile contributing to a zone.
        Default 2.0 means two-profile confluence equals a standalone POC in bonus alone.
    """

    def __init__(
        self,
        tolerance: float,
        level_type_weights: dict[str, float] | None = None,
        overlap_bonus_per_profile: float = 2.0,
    ) -> None:
        if tolerance <= 0.0:
            raise ValueError(f"tolerance must be positive, got {tolerance}")
        self.tolerance = tolerance
        self.level_type_weights: dict[str, float] = (
            level_type_weights if level_type_weights is not None else dict(_DEFAULT_TYPE_WEIGHTS)
        )
        self.overlap_bonus_per_profile = overlap_bonus_per_profile

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_zones(self, profiles: list[dict]) -> list[VolumeKeyLevelZone]:
        """
        Takes a list of exported profile payloads
        (from VolumeProfileWindowManager.export_profiles_payload())
        and returns merged key level zones sorted by strength_score descending.

        Empty list is returned if no profiles or no valid levels are found.
        """
        raw = self._extract_raw_levels(profiles)
        if not raw:
            return []
        raw.sort(key=lambda x: x[0])  # sort by price for sweep-line
        groups = self._merge_to_groups(raw)
        zones = [self._build_zone(g) for g in groups]
        zones.sort(key=lambda z: z.strength_score, reverse=True)
        return zones

    def nearest_support_zones(
        self,
        current_price: float,
        zones: list[VolumeKeyLevelZone],
        max_count: int = 3,
    ) -> list[VolumeKeyLevelZone]:
        """
        Returns up to max_count zones whose center is BELOW current_price,
        ordered by proximity (nearest first).
        """
        below = [z for z in zones if z.price_center < current_price]
        below.sort(key=lambda z: current_price - z.price_center)
        return below[:max_count]

    def nearest_resistance_zones(
        self,
        current_price: float,
        zones: list[VolumeKeyLevelZone],
        max_count: int = 3,
    ) -> list[VolumeKeyLevelZone]:
        """
        Returns up to max_count zones whose center is ABOVE current_price,
        ordered by proximity (nearest first).
        """
        above = [z for z in zones if z.price_center > current_price]
        above.sort(key=lambda z: z.price_center - current_price)
        return above[:max_count]

    def find_zone_at_price(
        self,
        price: float,
        zones: list[VolumeKeyLevelZone],
        min_strength: float = 0.0,
    ) -> VolumeKeyLevelZone | None:
        """
        Returns the strongest zone whose band contains price, or None.
        Optionally filter by minimum strength_score.
        """
        matching = [
            z for z in zones
            if z.contains(price) and z.strength_score >= min_strength
        ]
        return max(matching, key=lambda z: z.strength_score) if matching else None

    def is_price_near_key_zone(
        self,
        price: float,
        zones: list[VolumeKeyLevelZone],
        proximity: float = 0.0,
        min_strength: float = 0.0,
    ) -> VolumeKeyLevelZone | None:
        """
        Returns the strongest zone within `proximity` price units of `price`, or None.
        When proximity=0 this is equivalent to find_zone_at_price.
        """
        matching = [
            z for z in zones
            if z.distance_to(price) <= (z.zone_high - z.zone_low) / 2.0 + proximity
            and z.strength_score >= min_strength
        ]
        return max(matching, key=lambda z: z.strength_score) if matching else None

    def strongest_zone_between(
        self,
        price_low: float,
        price_high: float,
        zones: list[VolumeKeyLevelZone],
    ) -> VolumeKeyLevelZone | None:
        """
        Returns the strongest zone whose center falls within [price_low, price_high].
        Useful for finding key level between entry and stop-loss, or entry and target.
        """
        in_range = [z for z in zones if price_low <= z.price_center <= price_high]
        return max(in_range, key=lambda z: z.strength_score) if in_range else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_raw_levels(
        self, profiles: list[dict]
    ) -> list[tuple[float, str, str]]:
        """Returns list of (price, level_type, profile_id) for all valid levels."""
        result: list[tuple[float, str, str]] = []
        for profile in profiles:
            pid = str(profile.get("profile_id", ""))
            poc = float(profile.get("poc") or 0.0)
            vah = float(profile.get("vah") or 0.0)
            val = float(profile.get("val") or 0.0)
            if poc > 0.0:
                result.append((poc, "POC", pid))
            if vah > 0.0:
                result.append((vah, "VAH", pid))
            if val > 0.0:
                result.append((val, "VAL", pid))
            for hvn in profile.get("hvn_levels") or []:
                p = float(hvn) if hvn else 0.0
                if p > 0.0:
                    result.append((p, "HVN", pid))
        return result

    def _merge_to_groups(
        self, raw: list[tuple[float, str, str]]
    ) -> list[list[tuple[float, str, str]]]:
        """
        Sweep-line interval merge.
        Each level creates band [price−tol, price+tol].
        Adjacent / overlapping bands are merged into one group.
        raw must be sorted by price ascending.
        """
        tol = self.tolerance
        # group_high tracks the current group's upper band boundary
        groups: list[dict] = []
        for price, level_type, pid in raw:
            band_low = price - tol
            band_high = price + tol
            if not groups or band_low > groups[-1]["high"]:
                # No overlap with previous group → start new group
                groups.append({"high": band_high, "members": [(price, level_type, pid)]})
            else:
                # Overlap → extend and accumulate
                if band_high > groups[-1]["high"]:
                    groups[-1]["high"] = band_high
                groups[-1]["members"].append((price, level_type, pid))
        return [g["members"] for g in groups]

    def _build_zone(
        self, members: list[tuple[float, str, str]]
    ) -> VolumeKeyLevelZone:
        tol = self.tolerance
        prices = [p for p, _, _ in members]
        types = [lt for _, lt, _ in members]
        pids = [pid for _, _, pid in members]

        # Zone band: outer edges of all contributing tolerance bands
        zone_low = min(prices) - tol
        zone_high = max(prices) + tol

        # Weighted average price (level-type weight as per-level importance)
        weights = [self.level_type_weights.get(lt, 1.0) for lt in types]
        total_w = sum(weights)
        price_center = (
            sum(p * w for p, w in zip(prices, weights)) / total_w
            if total_w > 0.0
            else sum(prices) / len(prices)
        )

        # Unique contributors
        unique_types = list(dict.fromkeys(types))          # preserve first-seen order
        unique_pids = list(dict.fromkeys(pids))
        overlap_count = len(unique_pids)

        # Strength: sum of all type weights + overlap bonus for multi-profile confluence
        base_score = sum(weights)
        overlap_bonus = (overlap_count - 1) * self.overlap_bonus_per_profile
        strength_score = base_score + overlap_bonus

        return VolumeKeyLevelZone(
            zone_low=round(zone_low, 8),
            zone_high=round(zone_high, 8),
            price_center=round(price_center, 8),
            level_types=unique_types,
            source_profile_ids=unique_pids,
            overlap_count=overlap_count,
            strength_score=round(strength_score, 4),
        )
