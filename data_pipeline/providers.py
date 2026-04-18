from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zlib import crc32

import numpy as np
import pandas as pd

from .common import FIXTURES_ROOT
from .schemas import MarketMetadataRecord
from .utils import logistic, normalize_history_frame, parse_timestamp, read_json, utc_now_iso


@dataclass(slots=True)
class MockMarketBlueprint:
    market_id: str
    title: str
    question: str
    category: str
    families: list[str]
    tags: list[str]
    open_time: str
    close_time: str
    resolution_time: str
    status: str
    bias: float
    exposures: dict[str, float]
    lag_days: int = 0


class BaseMarketDataProvider(ABC):
    name: str

    @abstractmethod
    def fetch_market_metadata(self) -> list[MarketMetadataRecord]:
        raise NotImplementedError

    @abstractmethod
    def fetch_market_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        raise NotImplementedError


class MockMarketDataProvider(BaseMarketDataProvider):
    name = "mock"

    def __init__(self, seed: int = 17) -> None:
        self.seed = seed
        self._blueprints = {
            blueprint.market_id: blueprint
            for blueprint in [
                MockMarketBlueprint(
                    market_id="FED-JUN-2026-HIKE",
                    title="Fed hikes rates by June 2026",
                    question="Will the Federal Reserve deliver another rate hike by the June 2026 FOMC meeting?",
                    category="federal_reserve",
                    families=["federal_reserve", "monetary_policy", "interest_rates"],
                    tags=["fed", "fomc", "rate_hike", "interest_rates"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-06-17T19:00:00Z",
                    resolution_time="2026-06-17T19:00:00Z",
                    status="open",
                    bias=-0.16,
                    exposures={"policy_hawkish": 1.0, "inflation_pressure": 0.32},
                ),
                MockMarketBlueprint(
                    market_id="FED-SEP-2026-CUT",
                    title="Fed cuts rates by September 2026",
                    question="Will the Federal Reserve cut rates by the September 2026 FOMC meeting?",
                    category="federal_reserve",
                    families=["federal_reserve", "monetary_policy", "interest_rates"],
                    tags=["fed", "fomc", "rate_cut", "interest_rates"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-09-16T19:00:00Z",
                    resolution_time="2026-09-16T19:00:00Z",
                    status="open",
                    bias=-0.24,
                    exposures={"policy_dovish": 1.0, "recession_risk": 0.45},
                    lag_days=1,
                ),
                MockMarketBlueprint(
                    market_id="FED-DEC-2026-TERMINAL-ABOVE-5",
                    title="Fed terminal rate above 5% in 2026",
                    question="Will the Fed's terminal policy rate stay above 5.0% through the December 2026 meeting?",
                    category="federal_reserve",
                    families=["federal_reserve", "monetary_policy", "interest_rates"],
                    tags=["fed", "terminal_rate", "interest_rates", "monetary_policy"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-12-16T19:00:00Z",
                    resolution_time="2026-12-16T19:00:00Z",
                    status="open",
                    bias=-0.18,
                    exposures={"policy_hawkish": 1.08, "inflation_pressure": 0.28},
                ),
                MockMarketBlueprint(
                    market_id="CPI-MAY-2026-HOT",
                    title="CPI above 3.4% in May 2026",
                    question="Will year-over-year CPI print above 3.4% in the May 2026 release?",
                    category="inflation",
                    families=["inflation", "interest_rates"],
                    tags=["cpi", "headline_inflation", "inflation"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-06-10T12:30:00Z",
                    resolution_time="2026-06-10T12:30:00Z",
                    status="open",
                    bias=-0.2,
                    exposures={"inflation_pressure": 1.08, "policy_hawkish": 0.34},
                ),
                MockMarketBlueprint(
                    market_id="CORE-CPI-JUN-2026-ABOVE-3_5",
                    title="Core CPI above 3.5% in June 2026",
                    question="Will core CPI print above 3.5% in the June 2026 inflation release?",
                    category="inflation",
                    families=["inflation", "interest_rates"],
                    tags=["core_inflation", "core_cpi", "inflation"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-07-15T12:30:00Z",
                    resolution_time="2026-07-15T12:30:00Z",
                    status="open",
                    bias=-0.18,
                    exposures={"inflation_pressure": 1.12, "policy_hawkish": 0.38},
                ),
                MockMarketBlueprint(
                    market_id="CORE-PCE-JUL-2026-ABOVE-3",
                    title="Core PCE above 3.0% in July 2026",
                    question="Will core PCE inflation print above 3.0% in the July 2026 release?",
                    category="inflation",
                    families=["inflation", "federal_reserve"],
                    tags=["pce", "core_inflation", "inflation"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-08-28T12:30:00Z",
                    resolution_time="2026-08-28T12:30:00Z",
                    status="open",
                    bias=-0.16,
                    exposures={"inflation_pressure": 1.0, "policy_hawkish": 0.24},
                ),
                MockMarketBlueprint(
                    market_id="PAYROLLS-AUG-2026-ABOVE-225K",
                    title="Nonfarm payrolls above 225k in August 2026",
                    question="Will nonfarm payrolls print above 225,000 in the August 2026 jobs report?",
                    category="labor_market",
                    families=["labor_market", "jobs", "economic_growth"],
                    tags=["jobs_report", "nonfarm_payrolls", "labor_market"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-09-04T12:30:00Z",
                    resolution_time="2026-09-04T12:30:00Z",
                    status="open",
                    bias=-0.12,
                    exposures={"jobs_strength": 1.02, "growth_cycle": 0.35},
                ),
                MockMarketBlueprint(
                    market_id="UNEMPLOYMENT-SEP-2026-ABOVE-4_5",
                    title="Unemployment above 4.5% in September 2026",
                    question="Will the unemployment rate print above 4.5% in the September 2026 jobs report?",
                    category="labor_market",
                    families=["labor_market", "jobs", "economic_growth"],
                    tags=["unemployment", "jobs_report", "labor_market"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-10-02T12:30:00Z",
                    resolution_time="2026-10-02T12:30:00Z",
                    status="open",
                    bias=-0.24,
                    exposures={"labor_slack": 1.05, "recession_risk": 0.55},
                    lag_days=1,
                ),
                MockMarketBlueprint(
                    market_id="WAGE-GROWTH-AUG-2026-ABOVE-4",
                    title="Average hourly earnings above 4% in August 2026",
                    question="Will average hourly earnings grow faster than 4.0% year over year in the August 2026 jobs report?",
                    category="labor_market",
                    families=["labor_market", "jobs", "inflation"],
                    tags=["wage_growth", "jobs_report", "inflation"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-09-04T12:30:00Z",
                    resolution_time="2026-09-04T12:30:00Z",
                    status="open",
                    bias=-0.15,
                    exposures={"jobs_strength": 0.42, "inflation_pressure": 0.82},
                ),
                MockMarketBlueprint(
                    market_id="LFPR-SEP-2026-ABOVE-62_8",
                    title="Labor force participation above 62.8% in September 2026",
                    question="Will labor force participation print above 62.8% in the September 2026 jobs report?",
                    category="labor_market",
                    families=["labor_market", "jobs", "economic_growth"],
                    tags=["labor_force_participation", "labor_market", "jobs_report"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-10-02T12:30:00Z",
                    resolution_time="2026-10-02T12:30:00Z",
                    status="open",
                    bias=-0.14,
                    exposures={"jobs_strength": 0.46, "growth_cycle": 0.38},
                ),
                MockMarketBlueprint(
                    market_id="GDP-Q3-2026-ABOVE-2_5",
                    title="GDP growth above 2.5% in Q3 2026",
                    question="Will real GDP growth print above 2.5% in the Q3 2026 advance estimate?",
                    category="economic_growth",
                    families=["economic_growth"],
                    tags=["gdp", "economic_growth"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-10-29T12:30:00Z",
                    resolution_time="2026-10-29T12:30:00Z",
                    status="open",
                    bias=-0.12,
                    exposures={"growth_cycle": 1.02, "jobs_strength": 0.24},
                ),
                MockMarketBlueprint(
                    market_id="GDP-Q4-2026-NEGATIVE",
                    title="GDP contracts in Q4 2026",
                    question="Will real GDP growth be negative in the Q4 2026 advance estimate?",
                    category="economic_growth",
                    families=["economic_growth"],
                    tags=["gdp", "recession", "economic_growth"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2027-01-28T13:30:00Z",
                    resolution_time="2027-01-28T13:30:00Z",
                    status="open",
                    bias=-0.28,
                    exposures={"recession_risk": 1.0, "growth_cycle": -0.95},
                    lag_days=1,
                ),
                MockMarketBlueprint(
                    market_id="RECESSION-BY-2026-END",
                    title="US enters recession by end of 2026",
                    question="Will the US economy enter recession before the end of 2026?",
                    category="economic_growth",
                    families=["economic_growth"],
                    tags=["recession", "economic_outlook", "growth"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2027-01-15T13:30:00Z",
                    resolution_time="2027-01-15T13:30:00Z",
                    status="open",
                    bias=-0.22,
                    exposures={"recession_risk": 1.15, "growth_cycle": -0.82},
                    lag_days=1,
                ),
                MockMarketBlueprint(
                    market_id="SOFT-LANDING-THROUGH-2026",
                    title="US avoids recession through 2026",
                    question="Will the US economy avoid recession through the end of 2026?",
                    category="economic_growth",
                    families=["economic_growth"],
                    tags=["economic_outlook", "soft_landing", "growth"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2027-01-15T13:30:00Z",
                    resolution_time="2027-01-15T13:30:00Z",
                    status="open",
                    bias=-0.1,
                    exposures={"growth_cycle": 0.86, "recession_risk": -0.6, "policy_dovish": 0.15},
                ),
                MockMarketBlueprint(
                    market_id="US10Y-Q2-2026-ABOVE-4_5",
                    title="10Y Treasury yield above 4.5% in Q2 2026",
                    question="Will the US 10-year Treasury yield trade above 4.5% before the end of Q2 2026?",
                    category="interest_rates",
                    families=["interest_rates", "monetary_policy"],
                    tags=["treasury_yields", "10y", "interest_rates"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-06-30T20:00:00Z",
                    resolution_time="2026-06-30T20:00:00Z",
                    status="open",
                    bias=-0.12,
                    exposures={"yield_level": 1.0, "policy_hawkish": 0.42, "inflation_pressure": 0.24},
                ),
                MockMarketBlueprint(
                    market_id="US2Y-Q3-2026-ABOVE-4_75",
                    title="2Y Treasury yield above 4.75% in Q3 2026",
                    question="Will the US 2-year Treasury yield trade above 4.75% before the end of Q3 2026?",
                    category="interest_rates",
                    families=["interest_rates", "monetary_policy"],
                    tags=["treasury_yields", "2y", "interest_rates"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-09-30T20:00:00Z",
                    resolution_time="2026-09-30T20:00:00Z",
                    status="open",
                    bias=-0.15,
                    exposures={"yield_level": 1.12, "policy_hawkish": 0.62},
                ),
                MockMarketBlueprint(
                    market_id="CURVE-Q4-2026-STEEPEN",
                    title="10Y-2Y Treasury curve steepens by Q4 2026",
                    question="Will the US 10-year minus 2-year Treasury spread steepen to a positive slope by the end of Q4 2026?",
                    category="interest_rates",
                    families=["interest_rates", "economic_growth"],
                    tags=["yield_curve", "treasury_yields", "economic_outlook"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-12-31T20:00:00Z",
                    resolution_time="2026-12-31T20:00:00Z",
                    status="open",
                    bias=-0.2,
                    exposures={"curve_steepening": 1.0, "recession_risk": 0.45},
                    lag_days=1,
                ),
                MockMarketBlueprint(
                    market_id="BTC-SEP-2026-ABOVE-110K",
                    title="Bitcoin above 110k by September 2026",
                    question="Will Bitcoin trade above 110,000 dollars before the end of September 2026?",
                    category="crypto",
                    families=["crypto", "risk_assets"],
                    tags=["crypto", "bitcoin", "btc", "risk"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-09-30T20:00:00Z",
                    resolution_time="2026-09-30T20:00:00Z",
                    status="open",
                    bias=-0.15,
                    exposures={"crypto_bull": 1.08, "policy_dovish": 0.22},
                ),
                MockMarketBlueprint(
                    market_id="AUSTIN-JUL-2026-100F",
                    title="Austin hits 100F by July 2026",
                    question="Will Austin record a 100 degree Fahrenheit day before the end of July 2026?",
                    category="weather",
                    families=["weather", "heat"],
                    tags=["weather", "temperature", "texas", "heat"],
                    open_time="2026-01-15T00:00:00Z",
                    close_time="2026-07-31T23:00:00Z",
                    resolution_time="2026-07-31T23:00:00Z",
                    status="open",
                    bias=-0.3,
                    exposures={"heat": 1.15},
                ),
                MockMarketBlueprint(
                    market_id="HOUSE-2026-GOP-MAJORITY",
                    title="Republicans win House majority in 2026",
                    question="Will Republicans control the US House after the November 2026 election?",
                    category="politics",
                    families=["elections", "politics"],
                    tags=["elections", "politics", "house", "congress"],
                    open_time="2026-01-01T00:00:00Z",
                    close_time="2026-11-04T06:00:00Z",
                    resolution_time="2026-11-04T06:00:00Z",
                    status="open",
                    bias=-0.08,
                    exposures={"election_cycle": 1.0},
                ),
            ]
        }

    def fetch_market_metadata(self) -> list[MarketMetadataRecord]:
        return [
            MarketMetadataRecord(
                market_id=blueprint.market_id,
                ticker=blueprint.market_id,
                title=blueprint.title,
                question=blueprint.question,
                category=blueprint.category,
                families=blueprint.families,
                open_time=blueprint.open_time,
                close_time=blueprint.close_time,
                resolution_time=blueprint.resolution_time,
                status=blueprint.status,
                tags=blueprint.tags,
                source=self.name,
                extra={"generated_at": utc_now_iso()},
            )
            for blueprint in self._blueprints.values()
        ]

    def fetch_market_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        blueprint = self._blueprints[market.market_id]
        start = parse_timestamp(blueprint.open_time)
        end = parse_timestamp(blueprint.close_time) or parse_timestamp(blueprint.resolution_time)
        if start is None or end is None:
            raise ValueError(f"mock blueprint {market.market_id} is missing required times")

        periods = max(45, min(160, int((end - start).days) + 1))
        index = pd.date_range(start=start.normalize(), periods=periods, freq="D", tz="UTC")
        factors = self._build_factor_frame(periods)

        latent = np.full(periods, blueprint.bias, dtype=float)
        for factor_name, weight in blueprint.exposures.items():
            latent += weight * factors[factor_name].to_numpy()

        if blueprint.lag_days:
            latent = pd.Series(latent).shift(blueprint.lag_days).bfill().to_numpy()

        stable_offset = crc32(market.market_id.encode("utf-8")) % 10_000
        rng = np.random.default_rng(self.seed + stable_offset)
        latent += rng.normal(0.0, 0.065, size=periods)
        close = logistic(latent)
        close = np.clip(close, 0.03, 0.97)

        open_values = np.roll(close, 1)
        open_values[0] = close[0]
        open_values = np.clip(open_values + rng.normal(0.0, 0.014, size=periods), 0.02, 0.98)
        high = np.maximum(open_values, close) + np.abs(rng.normal(0.013, 0.009, size=periods))
        low = np.minimum(open_values, close) - np.abs(rng.normal(0.013, 0.009, size=periods))
        volume = 500 + (np.abs(np.diff(np.r_[close[0], close])) * 12_000) + rng.integers(0, 250, size=periods)

        frame = pd.DataFrame(
            {
                "timestamp": index,
                "open": np.clip(open_values, 0.0, 1.0),
                "high": np.clip(high, 0.0, 1.0),
                "low": np.clip(low, 0.0, 1.0),
                "close": close,
                "volume": volume.astype(float),
                "source": self.name,
            }
        )
        return normalize_history_frame(frame, market.market_id)

    def _build_factor_frame(self, periods: int) -> pd.DataFrame:
        t = np.arange(periods, dtype=float)
        policy_hawkish = (
            0.16 * np.sin(t / 7.0)
            + 0.72 * (t >= 20)
            - 0.58 * (t >= 52)
            + 0.42 * (t >= 86)
            + 0.08 * np.cos(t / 3.0)
        )
        inflation_pressure = 0.82 * policy_hawkish + 0.18 * np.cos(t / 4.2) + 0.14 * (t >= 36)
        jobs_strength = 0.32 * policy_hawkish + 0.42 * np.sin(t / 5.5) + 0.34 * (t >= 28) - 0.22 * (t >= 74)
        growth_cycle = 0.58 * jobs_strength - 0.28 * policy_hawkish + 0.22 * np.cos(t / 6.1) + 0.26 * (t >= 40) - 0.3 * (t >= 96)
        recession_risk = -0.88 * growth_cycle + 0.25 * np.sin(t / 8.5) + 0.18 * (t >= 94)
        policy_dovish = -0.92 * policy_hawkish + 0.55 * recession_risk + 0.12 * np.cos(t / 6.8)
        labor_slack = -0.8 * jobs_strength + 0.48 * recession_risk + 0.1 * np.sin(t / 4.4)
        yield_level = 0.76 * policy_hawkish + 0.24 * inflation_pressure + 0.14 * np.cos(t / 5.0)
        curve_steepening = 0.6 * recession_risk - 0.18 * policy_hawkish + 0.16 * np.cos(t / 4.8)
        crypto_bull = -0.3 * policy_hawkish + 0.58 * np.sin(t / 5.8) + 0.35 * (t >= 64) - 0.18 * (t >= 112)
        heat = 0.34 * np.sin(t / 4.2) + 0.72 * ((t >= 34) & (t < 82)) - 0.16 * (t >= 112)
        election_cycle = 0.18 * np.sin(t / 6.4) + 0.78 * (t >= 98)
        return pd.DataFrame(
            {
                "policy_hawkish": policy_hawkish,
                "policy_dovish": policy_dovish,
                "inflation_pressure": inflation_pressure,
                "jobs_strength": jobs_strength,
                "labor_slack": labor_slack,
                "growth_cycle": growth_cycle,
                "recession_risk": recession_risk,
                "yield_level": yield_level,
                "curve_steepening": curve_steepening,
                "crypto_bull": crypto_bull,
                "heat": heat,
                "election_cycle": election_cycle,
            }
        )


class SnapshotMarketDataProvider(BaseMarketDataProvider):
    name = "snapshot"

    def __init__(self, snapshot_dir: Path | None = None) -> None:
        self.snapshot_dir = snapshot_dir or FIXTURES_ROOT / "snapshot"

    def fetch_market_metadata(self) -> list[MarketMetadataRecord]:
        json_path = self.snapshot_dir / "markets.json"
        csv_path = self.snapshot_dir / "markets.csv"
        if json_path.exists():
            payload = read_json(json_path)
            records = payload["records"] if isinstance(payload, dict) and "records" in payload else payload
            return [MarketMetadataRecord.from_mapping(record) for record in records]
        if csv_path.exists():
            frame = pd.read_csv(csv_path)
            return [MarketMetadataRecord.from_mapping(record) for record in frame.to_dict(orient="records")]
        raise FileNotFoundError(
            f"expected a snapshot metadata file at {json_path} or {csv_path}"
        )

    def fetch_market_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        per_market_path = self.snapshot_dir / "history" / f"{market.market_id}.csv"
        aggregate_path = self.snapshot_dir / "history.csv"
        if per_market_path.exists():
            frame = pd.read_csv(per_market_path)
            return normalize_history_frame(frame, market.market_id)
        if aggregate_path.exists():
            frame = pd.read_csv(aggregate_path)
            filtered = frame[frame["market_id"] == market.market_id]
            return normalize_history_frame(filtered, market.market_id)
        raise FileNotFoundError(
            f"expected history for {market.market_id} at {per_market_path} or {aggregate_path}"
        )


def get_provider(provider_name: str, *, snapshot_dir: Path | None = None) -> BaseMarketDataProvider:
    normalized_name = provider_name.lower().strip()
    if normalized_name == "mock":
        return MockMarketDataProvider()
    if normalized_name == "snapshot":
        return SnapshotMarketDataProvider(snapshot_dir=snapshot_dir)
    # TODO: Swap in a real Kalshi/live provider here once API contracts and auth details are settled.
    raise ValueError(f"unsupported provider '{provider_name}'")
