from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import Any, TYPE_CHECKING
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zlib import crc32

import numpy as np
import pandas as pd

from .common import FIXTURES_ROOT, PIPELINE_PROGRESS_SCHEMA_VERSION
from .schemas import MarketMetadataRecord
from .utils import ensure_dir, logistic, normalize_history_frame, normalize_text, parse_timestamp, read_json, utc_now_iso, write_json

if TYPE_CHECKING:
    from .scope import PipelineScopeConfig


DEFAULT_PROVIDER_SETTINGS: dict[str, dict[str, Any]] = {
    "kalshi_live": {
        "base_url": "https://api.elections.kalshi.com/trade-api/v2",
        "timeout_seconds": 30,
        "retry_count": 3,
        "retry_backoff_seconds": 1.5,
        "market_page_limit": 1000,
        "event_page_limit": 200,
        "max_market_pages": 0,
        "max_event_pages": 0,
        "max_historical_pages": 0,
        "exclude_multivariate": True,
        "discovery_lookback_days": 120,
        "include_historical_markets": True,
        "max_historical_event_queries": 250,
        "metadata_cache_ttl_seconds": 900,
        "open_market_history_cache_ttl_seconds": 900,
        "closed_market_history_cache_ttl_seconds": 86400,
        "history_include_latest_before_start": False,
        "history_default_lookback_days": 120,
        "history_short_interval_minutes": 60,
        "history_long_interval_minutes": 1440,
        "history_short_duration_days": 45,
        "batch_candlestick_chunk_size": 100,
    }
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_dotenv(dotenv_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not dotenv_path.exists():
        return values
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        values[key.strip()] = raw_value.strip().strip("'").strip('"')
    return values


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def resolve_provider_settings(provider_name: str, *, config_path: Path | None = None) -> dict[str, Any]:
    normalized_name = provider_name.lower().strip()
    settings = json.loads(json.dumps(DEFAULT_PROVIDER_SETTINGS.get(normalized_name, {})))
    if config_path and config_path.exists():
        payload = read_json(config_path)
        provider_settings = payload.get("provider_settings", {})
        if isinstance(provider_settings, dict) and isinstance(provider_settings.get(normalized_name), dict):
            settings = _deep_merge(settings, provider_settings[normalized_name])
        elif isinstance(payload.get(normalized_name), dict):
            settings = _deep_merge(settings, payload[normalized_name])

    dotenv_values = _load_dotenv(Path(__file__).resolve().parent / ".env")
    env_prefix = normalized_name.upper()
    base_url = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_BASE_URL")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_BASE_URL")
        or os.environ.get("DATA_PIPELINE_PROVIDER_BASE_URL")
        or dotenv_values.get("DATA_PIPELINE_PROVIDER_BASE_URL")
    )
    timeout_seconds = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_TIMEOUT_SECONDS")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_TIMEOUT_SECONDS")
    )
    retry_count = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_RETRY_COUNT")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_RETRY_COUNT")
    )
    retry_backoff_seconds = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_RETRY_BACKOFF_SECONDS")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_RETRY_BACKOFF_SECONDS")
    )
    market_page_limit = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_MARKET_PAGE_LIMIT")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_MARKET_PAGE_LIMIT")
    )
    discovery_lookback_days = (
        os.environ.get(f"DATA_PIPELINE_{env_prefix}_DISCOVERY_LOOKBACK_DAYS")
        or dotenv_values.get(f"DATA_PIPELINE_{env_prefix}_DISCOVERY_LOOKBACK_DAYS")
    )
    if base_url:
        settings["base_url"] = base_url
    settings["timeout_seconds"] = _safe_int(timeout_seconds, _safe_int(settings.get("timeout_seconds"), 30))
    settings["retry_count"] = _safe_int(retry_count, _safe_int(settings.get("retry_count"), 3))
    settings["retry_backoff_seconds"] = _safe_float(
        retry_backoff_seconds,
        _safe_float(settings.get("retry_backoff_seconds"), 1.5),
    )
    settings["market_page_limit"] = _safe_int(market_page_limit, _safe_int(settings.get("market_page_limit"), 1000))
    settings["discovery_lookback_days"] = _safe_int(
        discovery_lookback_days,
        _safe_int(settings.get("discovery_lookback_days"), 120),
    )
    return settings


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


def _blueprint(
    *,
    market_id: str,
    title: str,
    question: str,
    category: str,
    families: list[str],
    tags: list[str],
    close_time: str,
    exposures: dict[str, float],
    open_time: str = "2026-01-01T00:00:00Z",
    resolution_time: str | None = None,
    status: str = "open",
    bias: float = -0.14,
    lag_days: int = 0,
) -> MockMarketBlueprint:
    return MockMarketBlueprint(
        market_id=market_id,
        title=title,
        question=question,
        category=category,
        families=families,
        tags=tags,
        open_time=open_time,
        close_time=close_time,
        resolution_time=resolution_time or close_time,
        status=status,
        bias=bias,
        exposures=exposures,
        lag_days=lag_days,
    )


def _build_mock_blueprints() -> list[MockMarketBlueprint]:
    return [
        _blueprint(
            market_id="CPI-MAY-2026-HOT",
            title="CPI above 3.4% in May 2026",
            question="Will year-over-year CPI print above 3.4% in the May 2026 release, signaling hotter-than-expected inflation?",
            category="inflation",
            families=["inflation", "interest_rates", "federal_reserve"],
            tags=["cpi", "headline_inflation", "inflation_surprise", "hotter_than_expected"],
            close_time="2026-06-10T12:30:00Z",
            bias=-0.20,
            exposures={"inflation_pressure": 1.12, "policy_hawkish": 0.36, "yield_level": 0.18},
        ),
        _blueprint(
            market_id="CPI-AUG-2026-BELOW-2_7",
            title="CPI below 2.7% in August 2026",
            question="Will year-over-year CPI print below 2.7% in the August 2026 release, reinforcing the disinflation trend?",
            category="inflation",
            families=["inflation", "interest_rates", "economic_growth"],
            tags=["cpi", "disinflation", "lower_than_expected_inflation", "inflation"],
            close_time="2026-09-11T12:30:00Z",
            bias=-0.12,
            exposures={"inflation_cooling": 1.06, "policy_dovish": 0.24, "recession_risk": 0.18},
            lag_days=1,
        ),
        _blueprint(
            market_id="CPI-SEP-2026-SURPRISE-HOT",
            title="September 2026 CPI comes in hotter than expected",
            question="Will the September 2026 CPI release be an upside inflation surprise versus consensus expectations?",
            category="inflation",
            families=["inflation", "interest_rates", "federal_reserve"],
            tags=["cpi", "inflation_surprise", "headline_inflation", "fed_watch"],
            close_time="2026-10-13T12:30:00Z",
            bias=-0.18,
            exposures={"inflation_pressure": 1.18, "policy_hawkish": 0.42, "yield_level": 0.20},
        ),
        _blueprint(
            market_id="CORE-CPI-JUN-2026-ABOVE-3_5",
            title="Core CPI above 3.5% in June 2026",
            question="Will core CPI print above 3.5% in the June 2026 inflation release?",
            category="inflation",
            families=["inflation", "interest_rates", "federal_reserve"],
            tags=["core_cpi", "core_inflation", "inflation", "sticky_inflation"],
            close_time="2026-07-15T12:30:00Z",
            bias=-0.18,
            exposures={"inflation_pressure": 1.16, "policy_hawkish": 0.40, "wage_pressure": 0.18},
        ),
        _blueprint(
            market_id="CORE-CPI-OCT-2026-BELOW-3_0",
            title="Core CPI below 3.0% in October 2026",
            question="Will core CPI cool below 3.0% in the October 2026 release?",
            category="inflation",
            families=["inflation", "interest_rates", "economic_growth"],
            tags=["core_cpi", "core_inflation", "disinflation", "inflation"],
            close_time="2026-11-12T13:30:00Z",
            bias=-0.12,
            exposures={"inflation_cooling": 1.00, "policy_dovish": 0.22, "growth_cycle": -0.10},
        ),
        _blueprint(
            market_id="PCE-JUN-2026-ABOVE-2_9",
            title="PCE inflation above 2.9% in June 2026",
            question="Will headline PCE inflation print above 2.9% in the June 2026 release?",
            category="inflation",
            families=["inflation", "federal_reserve", "interest_rates"],
            tags=["pce", "inflation", "fed_watch", "headline_inflation"],
            close_time="2026-07-31T12:30:00Z",
            bias=-0.16,
            exposures={"inflation_pressure": 1.04, "policy_hawkish": 0.28, "yield_level": 0.14},
        ),
        _blueprint(
            market_id="PCE-NOV-2026-BELOW-2_6",
            title="PCE inflation below 2.6% in November 2026",
            question="Will headline PCE inflation fall below 2.6% in the November 2026 release?",
            category="inflation",
            families=["inflation", "federal_reserve", "economic_growth"],
            tags=["pce", "disinflation", "inflation", "fed_watch"],
            close_time="2026-12-23T13:30:00Z",
            bias=-0.10,
            exposures={"inflation_cooling": 0.96, "policy_dovish": 0.24, "recession_risk": 0.16},
        ),
        _blueprint(
            market_id="CORE-PCE-JUL-2026-ABOVE-3_0",
            title="Core PCE above 3.0% in July 2026",
            question="Will core PCE inflation print above 3.0% in the July 2026 release?",
            category="inflation",
            families=["inflation", "federal_reserve", "interest_rates"],
            tags=["core_pce", "core_inflation", "inflation", "fed_watch"],
            close_time="2026-08-28T12:30:00Z",
            bias=-0.15,
            exposures={"inflation_pressure": 1.06, "policy_hawkish": 0.26, "yield_level": 0.12},
        ),
        _blueprint(
            market_id="CORE-PCE-DEC-2026-BELOW-2_7",
            title="Core PCE below 2.7% in December 2026",
            question="Will core PCE inflation fall below 2.7% in the December 2026 release?",
            category="inflation",
            families=["inflation", "federal_reserve", "economic_growth"],
            tags=["core_pce", "core_inflation", "disinflation", "fed_watch"],
            close_time="2027-01-29T13:30:00Z",
            bias=-0.08,
            exposures={"inflation_cooling": 1.02, "policy_dovish": 0.30, "growth_cycle": -0.12},
            lag_days=1,
        ),
        _blueprint(
            market_id="DISINFLATION-BY-2026-END",
            title="Disinflation trend continues through end of 2026",
            question="Will the US disinflation trend continue through the end of 2026?",
            category="inflation",
            families=["inflation", "economic_growth", "interest_rates"],
            tags=["disinflation", "inflation", "economic_outlook", "policy_path"],
            close_time="2027-01-15T13:30:00Z",
            bias=-0.10,
            exposures={"inflation_cooling": 1.00, "policy_dovish": 0.22, "recession_risk": 0.14},
        ),
        _blueprint(
            market_id="FED-JUN-2026-HIKE",
            title="Fed hikes rates by June 2026",
            question="Will the Federal Reserve deliver another rate hike by the June 2026 FOMC meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates"],
            tags=["fed", "fomc", "rate_hike", "policy_path"],
            close_time="2026-06-17T19:00:00Z",
            bias=-0.16,
            exposures={"policy_hawkish": 1.00, "inflation_pressure": 0.34},
        ),
        _blueprint(
            market_id="FOMC-JUL-2026-NO-CHANGE",
            title="Fed holds rates steady in July 2026",
            question="Will the Federal Reserve leave rates unchanged at the July 2026 FOMC meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates"],
            tags=["fed", "fomc", "no_change", "policy_path"],
            close_time="2026-07-29T19:00:00Z",
            bias=-0.12,
            exposures={"policy_stable": 1.00, "policy_hawkish": 0.22, "growth_cycle": 0.12},
        ),
        _blueprint(
            market_id="FED-SEP-2026-CUT",
            title="Fed cuts rates by September 2026",
            question="Will the Federal Reserve cut rates by the September 2026 FOMC meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "economic_growth"],
            tags=["fed", "fomc", "rate_cut", "policy_path"],
            close_time="2026-09-16T19:00:00Z",
            bias=-0.22,
            exposures={"policy_dovish": 1.00, "recession_risk": 0.42, "yield_level": -0.18},
            lag_days=1,
        ),
        _blueprint(
            market_id="FOMC-NOV-2026-NO-CHANGE",
            title="Fed holds rates steady in November 2026",
            question="Will the Federal Reserve leave rates unchanged at the November 2026 FOMC meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates"],
            tags=["fed", "fomc", "no_change", "policy_path"],
            close_time="2026-11-05T19:00:00Z",
            bias=-0.10,
            exposures={"policy_stable": 0.96, "inflation_pressure": 0.18, "growth_cycle": 0.10},
        ),
        _blueprint(
            market_id="FED-DEC-2026-TERMINAL-ABOVE-5",
            title="Fed terminal rate above 5% in 2026",
            question="Will the Fed's terminal policy rate stay above 5.0% through the December 2026 meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates"],
            tags=["fed", "terminal_rate", "policy_path", "interest_rates"],
            close_time="2026-12-16T19:00:00Z",
            bias=-0.18,
            exposures={"policy_hawkish": 1.08, "inflation_pressure": 0.30, "yield_level": 0.24},
        ),
        _blueprint(
            market_id="FIRST-CUT-BY-JUL-2026",
            title="First Fed rate cut arrives by July 2026",
            question="Will the Federal Reserve deliver its first rate cut by the July 2026 FOMC meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "economic_growth"],
            tags=["fed", "first_cut", "rate_cut", "policy_path"],
            close_time="2026-07-29T19:00:00Z",
            bias=-0.18,
            exposures={"policy_dovish": 0.92, "recession_risk": 0.30, "yield_level": -0.12},
            lag_days=1,
        ),
        _blueprint(
            market_id="CUMULATIVE-CUTS-BY-DEC-2026-75BP",
            title="Fed delivers at least 75bp of cuts by December 2026",
            question="Will the Federal Reserve cut rates by a cumulative 75 basis points or more by the December 2026 meeting?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "economic_growth"],
            tags=["fed", "cumulative_cuts", "rate_cut", "policy_path"],
            close_time="2026-12-16T19:00:00Z",
            bias=-0.24,
            exposures={"policy_dovish": 1.06, "recession_risk": 0.48, "yield_level": -0.22},
            lag_days=2,
        ),
        _blueprint(
            market_id="FOMC-JUN-2026-HAWKISH-DOT-PLOT",
            title="June 2026 FOMC dot plot comes in hawkish",
            question="Will the June 2026 FOMC meeting deliver a hawkish dot plot or guidance surprise?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "inflation"],
            tags=["fed", "fomc", "hawkish_guidance", "dot_plot"],
            close_time="2026-06-17T19:00:00Z",
            bias=-0.16,
            exposures={"policy_hawkish": 1.12, "inflation_pressure": 0.26, "yield_level": 0.18},
        ),
        _blueprint(
            market_id="FOMC-SEP-2026-DOVISH-STATEMENT",
            title="September 2026 FOMC statement turns dovish",
            question="Will the September 2026 FOMC statement or press conference be interpreted as dovish?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "economic_growth"],
            tags=["fed", "fomc", "dovish_guidance", "powell"],
            close_time="2026-09-16T19:00:00Z",
            bias=-0.18,
            exposures={"policy_dovish": 1.08, "recession_risk": 0.28, "yield_level": -0.14},
            lag_days=1,
        ),
        _blueprint(
            market_id="POWELL-JACKSON-HOLE-HAWKISH",
            title="Powell sounds hawkish at Jackson Hole 2026",
            question="Will Chair Powell deliver a hawkish policy message at Jackson Hole in August 2026?",
            category="federal_reserve",
            families=["federal_reserve", "monetary_policy", "interest_rates", "inflation"],
            tags=["powell", "jackson_hole", "hawkish_guidance", "fed"],
            close_time="2026-08-28T19:00:00Z",
            bias=-0.14,
            exposures={"policy_hawkish": 1.02, "inflation_pressure": 0.24, "yield_level": 0.16},
        ),
        _blueprint(
            market_id="PAYROLLS-AUG-2026-ABOVE-225K",
            title="Nonfarm payrolls above 225k in August 2026",
            question="Will nonfarm payrolls print above 225,000 in the August 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth", "monetary_policy"],
            tags=["jobs_report", "nonfarm_payrolls", "strong_labor_market", "fed_watch"],
            close_time="2026-09-04T12:30:00Z",
            bias=-0.12,
            exposures={"jobs_strength": 1.04, "growth_cycle": 0.36, "wage_pressure": 0.12},
        ),
        _blueprint(
            market_id="PAYROLLS-OCT-2026-BELOW-100K",
            title="Nonfarm payrolls below 100k in October 2026",
            question="Will nonfarm payrolls print below 100,000 in the October 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth", "monetary_policy"],
            tags=["jobs_report", "nonfarm_payrolls", "softening_labor_market", "fed_watch"],
            close_time="2026-11-06T13:30:00Z",
            bias=-0.20,
            exposures={"labor_slack": 0.96, "recession_risk": 0.38, "growth_cycle": -0.30},
            lag_days=1,
        ),
        _blueprint(
            market_id="JOBS-REPORT-DEC-2026-STRONG",
            title="December 2026 jobs report comes in strong",
            question="Will the December 2026 US jobs report show a strong labor market surprise?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth", "monetary_policy"],
            tags=["jobs_report", "labor_market", "strong_labor_market", "fed_watch"],
            close_time="2027-01-08T13:30:00Z",
            bias=-0.10,
            exposures={"jobs_strength": 1.08, "growth_cycle": 0.34, "policy_hawkish": 0.14},
        ),
        _blueprint(
            market_id="UNEMPLOYMENT-SEP-2026-ABOVE-4_5",
            title="Unemployment above 4.5% in September 2026",
            question="Will the unemployment rate print above 4.5% in the September 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth", "monetary_policy"],
            tags=["unemployment", "jobs_report", "softening_labor_market", "fed_watch"],
            close_time="2026-10-02T12:30:00Z",
            bias=-0.24,
            exposures={"labor_slack": 1.08, "recession_risk": 0.58, "policy_dovish": 0.16},
            lag_days=1,
        ),
        _blueprint(
            market_id="UNEMPLOYMENT-DEC-2026-BELOW-4_1",
            title="Unemployment below 4.1% in December 2026",
            question="Will the unemployment rate hold below 4.1% in the December 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth"],
            tags=["unemployment", "jobs_report", "strong_labor_market", "labor_market"],
            close_time="2027-01-08T13:30:00Z",
            bias=-0.08,
            exposures={"jobs_strength": 0.96, "growth_cycle": 0.25, "policy_hawkish": 0.10},
        ),
        _blueprint(
            market_id="WAGE-GROWTH-AUG-2026-ABOVE-4",
            title="Average hourly earnings above 4% in August 2026",
            question="Will average hourly earnings grow faster than 4.0% year over year in the August 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "inflation", "monetary_policy"],
            tags=["wage_growth", "jobs_report", "inflation", "fed_watch"],
            close_time="2026-09-04T12:30:00Z",
            bias=-0.15,
            exposures={"wage_pressure": 1.02, "inflation_pressure": 0.70, "jobs_strength": 0.32},
        ),
        _blueprint(
            market_id="WAGE-GROWTH-NOV-2026-BELOW-3_4",
            title="Average hourly earnings below 3.4% in November 2026",
            question="Will average hourly earnings slow below 3.4% year over year in the November 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "inflation", "monetary_policy"],
            tags=["wage_growth", "jobs_report", "disinflation", "fed_watch"],
            close_time="2026-12-04T13:30:00Z",
            bias=-0.10,
            exposures={"wage_pressure": -0.82, "inflation_cooling": 0.55, "labor_slack": 0.46, "policy_dovish": 0.12},
            lag_days=1,
        ),
        _blueprint(
            market_id="LFPR-SEP-2026-ABOVE-62_8",
            title="Labor force participation above 62.8% in September 2026",
            question="Will labor force participation print above 62.8% in the September 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth"],
            tags=["labor_force_participation", "jobs_report", "labor_market", "strong_labor_market"],
            close_time="2026-10-02T12:30:00Z",
            bias=-0.12,
            exposures={"participation_strength": 1.00, "jobs_strength": 0.48, "growth_cycle": 0.32},
        ),
        _blueprint(
            market_id="LFPR-DEC-2026-BELOW-62_4",
            title="Labor force participation below 62.4% in December 2026",
            question="Will labor force participation fall below 62.4% in the December 2026 jobs report?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth"],
            tags=["labor_force_participation", "jobs_report", "softening_labor_market", "labor_market"],
            close_time="2027-01-08T13:30:00Z",
            bias=-0.18,
            exposures={"participation_strength": -0.92, "labor_slack": 0.42, "recession_risk": 0.18},
            lag_days=1,
        ),
        _blueprint(
            market_id="LABOR-MARKET-SOFTENS-BY-Q4-2026",
            title="US labor market softens by Q4 2026",
            question="Will the US labor market clearly soften by the end of Q4 2026?",
            category="labor_market",
            families=["labor_market", "jobs", "economic_growth", "monetary_policy"],
            tags=["labor_market", "softening_labor_market", "economic_outlook", "fed_watch"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.20,
            exposures={"labor_slack": 1.00, "recession_risk": 0.52, "policy_dovish": 0.18},
            lag_days=1,
        ),
        _blueprint(
            market_id="GDP-Q2-2026-BELOW-1_5",
            title="GDP growth below 1.5% in Q2 2026",
            question="Will real GDP growth print below 1.5% in the Q2 2026 advance estimate?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["gdp", "economic_slowdown", "growth", "soft_landing"],
            close_time="2026-07-30T12:30:00Z",
            bias=-0.14,
            exposures={"growth_cycle": -0.56, "recession_risk": 0.48, "yield_level": -0.08},
        ),
        _blueprint(
            market_id="GDP-Q3-2026-ABOVE-2_5",
            title="GDP growth above 2.5% in Q3 2026",
            question="Will real GDP growth print above 2.5% in the Q3 2026 advance estimate?",
            category="economic_growth",
            families=["economic_growth", "labor_market"],
            tags=["gdp", "economic_growth", "expansion", "strong_growth"],
            close_time="2026-10-29T12:30:00Z",
            bias=-0.10,
            exposures={"growth_cycle": 1.06, "jobs_strength": 0.22},
        ),
        _blueprint(
            market_id="GDP-Q4-2026-NEGATIVE",
            title="GDP contracts in Q4 2026",
            question="Will real GDP growth be negative in the Q4 2026 advance estimate?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["gdp", "negative_gdp", "recession", "economic_slowdown"],
            close_time="2027-01-28T13:30:00Z",
            bias=-0.28,
            exposures={"recession_risk": 1.02, "growth_cycle": -0.96, "curve_steepening": 0.10},
            lag_days=1,
        ),
        _blueprint(
            market_id="GDP-Q1-2027-BELOW-1_0",
            title="GDP growth below 1.0% in Q1 2027",
            question="Will real GDP growth print below 1.0% in the Q1 2027 advance estimate?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["gdp", "economic_slowdown", "growth", "recession_risk"],
            close_time="2027-04-29T12:30:00Z",
            bias=-0.18,
            exposures={"recession_risk": 0.82, "growth_cycle": -0.74},
            lag_days=1,
        ),
        _blueprint(
            market_id="RECESSION-BY-2026-END",
            title="US enters recession by end of 2026",
            question="Will the US economy enter recession before the end of 2026?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["recession", "economic_outlook", "hard_landing", "growth"],
            close_time="2027-01-15T13:30:00Z",
            bias=-0.22,
            exposures={"recession_risk": 1.18, "growth_cycle": -0.82},
            lag_days=1,
        ),
        _blueprint(
            market_id="RECESSION-BY-Q2-2027",
            title="US enters recession by Q2 2027",
            question="Will the US economy enter recession by the end of Q2 2027?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["recession", "economic_outlook", "hard_landing", "growth"],
            close_time="2027-07-15T13:30:00Z",
            bias=-0.20,
            exposures={"recession_risk": 1.12, "growth_cycle": -0.76},
            lag_days=1,
        ),
        _blueprint(
            market_id="SOFT-LANDING-THROUGH-2026",
            title="US avoids recession through 2026",
            question="Will the US economy avoid recession through the end of 2026, delivering a soft landing?",
            category="economic_growth",
            families=["economic_growth", "monetary_policy", "interest_rates"],
            tags=["soft_landing", "economic_outlook", "growth", "expansion"],
            close_time="2027-01-15T13:30:00Z",
            bias=-0.10,
            exposures={"growth_cycle": 0.84, "recession_risk": -0.62, "policy_dovish": 0.14},
        ),
        _blueprint(
            market_id="HARD-LANDING-BY-Q1-2027",
            title="US hard landing by Q1 2027",
            question="Will the US economy experience a hard landing by the end of Q1 2027?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["hard_landing", "economic_outlook", "recession", "growth"],
            close_time="2027-03-31T20:00:00Z",
            bias=-0.22,
            exposures={"recession_risk": 1.08, "growth_cycle": -0.86, "labor_slack": 0.22},
            lag_days=1,
        ),
        _blueprint(
            market_id="ECONOMIC-SLOWDOWN-BY-Q4-2026",
            title="US economy slows materially by Q4 2026",
            question="Will the US economy show a clear slowdown by the end of Q4 2026?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "monetary_policy"],
            tags=["economic_slowdown", "economic_outlook", "growth", "soft_landing"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.18,
            exposures={"recession_risk": 0.84, "growth_cycle": -0.66, "policy_dovish": 0.12},
            lag_days=1,
        ),
        _blueprint(
            market_id="EXPANSION-THROUGH-Q1-2027",
            title="US expansion continues through Q1 2027",
            question="Will the US economic expansion continue through the end of Q1 2027?",
            category="economic_growth",
            families=["economic_growth", "labor_market", "interest_rates"],
            tags=["expansion", "economic_outlook", "growth", "soft_landing"],
            close_time="2027-03-31T20:00:00Z",
            bias=-0.08,
            exposures={"growth_cycle": 0.88, "recession_risk": -0.52},
        ),
        _blueprint(
            market_id="US10Y-Q2-2026-ABOVE-4_5",
            title="10Y Treasury yield above 4.5% in Q2 2026",
            question="Will the US 10-year Treasury yield trade above 4.5% before the end of Q2 2026?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "inflation"],
            tags=["treasury_yields", "10y", "interest_rates", "fed_watch"],
            close_time="2026-06-30T20:00:00Z",
            bias=-0.12,
            exposures={"yield_level": 1.00, "policy_hawkish": 0.42, "inflation_pressure": 0.24},
        ),
        _blueprint(
            market_id="US10Y-DEC-2026-BELOW-4_0",
            title="10Y Treasury yield below 4.0% by December 2026",
            question="Will the US 10-year Treasury yield trade below 4.0% before the end of December 2026?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "economic_growth"],
            tags=["treasury_yields", "10y", "interest_rates", "rate_cut"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.10,
            exposures={"yield_level": -0.92, "policy_dovish": 0.34, "recession_risk": 0.24},
            lag_days=1,
        ),
        _blueprint(
            market_id="US2Y-Q3-2026-ABOVE-4_75",
            title="2Y Treasury yield above 4.75% in Q3 2026",
            question="Will the US 2-year Treasury yield trade above 4.75% before the end of Q3 2026?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "inflation"],
            tags=["treasury_yields", "2y", "interest_rates", "fed_watch"],
            close_time="2026-09-30T20:00:00Z",
            bias=-0.15,
            exposures={"yield_level": 1.14, "policy_hawkish": 0.64, "inflation_pressure": 0.18},
        ),
        _blueprint(
            market_id="US2Y-Q1-2027-BELOW-3_9",
            title="2Y Treasury yield below 3.9% in Q1 2027",
            question="Will the US 2-year Treasury yield trade below 3.9% before the end of Q1 2027?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "economic_growth"],
            tags=["treasury_yields", "2y", "interest_rates", "rate_cut"],
            close_time="2027-03-31T20:00:00Z",
            bias=-0.12,
            exposures={"yield_level": -1.00, "policy_dovish": 0.46, "recession_risk": 0.28},
            lag_days=1,
        ),
        _blueprint(
            market_id="CURVE-Q4-2026-STEEPEN",
            title="10Y-2Y Treasury curve steepens by Q4 2026",
            question="Will the US 10-year minus 2-year Treasury spread steepen to a positive slope by the end of Q4 2026?",
            category="interest_rates",
            families=["interest_rates", "economic_growth", "monetary_policy"],
            tags=["yield_curve", "treasury_yields", "curve_steepening", "economic_outlook"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.18,
            exposures={"curve_steepening": 1.00, "recession_risk": 0.46, "yield_level": -0.10},
            lag_days=1,
        ),
        _blueprint(
            market_id="CURVE-INVERTED-THROUGH-Q3-2026",
            title="10Y-2Y Treasury curve stays inverted through Q3 2026",
            question="Will the US Treasury yield curve remain inverted through the end of Q3 2026?",
            category="interest_rates",
            families=["interest_rates", "economic_growth", "monetary_policy"],
            tags=["yield_curve", "treasury_yields", "curve_inversion", "fed_watch"],
            close_time="2026-09-30T20:00:00Z",
            bias=-0.14,
            exposures={"curve_steepening": -0.96, "policy_hawkish": 0.28, "growth_cycle": 0.10},
        ),
        _blueprint(
            market_id="US10Y-Q4-2026-ABOVE-4_75",
            title="10Y Treasury yield above 4.75% in Q4 2026",
            question="Will the US 10-year Treasury yield trade above 4.75% before the end of Q4 2026?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "inflation"],
            tags=["treasury_yields", "10y", "interest_rates", "inflation_surprise"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.14,
            exposures={"yield_level": 1.10, "inflation_pressure": 0.30, "policy_hawkish": 0.32},
        ),
        _blueprint(
            market_id="US2Y-BY-DEC-2026-BELOW-4_25",
            title="2Y Treasury yield below 4.25% by December 2026",
            question="Will the US 2-year Treasury yield trade below 4.25% before the end of December 2026?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "economic_growth"],
            tags=["treasury_yields", "2y", "interest_rates", "rate_cut"],
            close_time="2026-12-31T20:00:00Z",
            bias=-0.10,
            exposures={"yield_level": -0.86, "policy_dovish": 0.34, "recession_risk": 0.16},
            lag_days=1,
        ),
        _blueprint(
            market_id="FED-FUNDS-BY-DEC-2026-BELOW-4_25",
            title="Fed funds rate below 4.25% by December 2026",
            question="Will the effective fed funds rate move below 4.25% by the December 2026 FOMC meeting?",
            category="interest_rates",
            families=["interest_rates", "monetary_policy", "federal_reserve"],
            tags=["fed_funds", "interest_rates", "rate_cut", "fed_watch"],
            close_time="2026-12-16T19:00:00Z",
            bias=-0.12,
            exposures={"yield_level": -0.72, "policy_dovish": 0.50, "recession_risk": 0.20},
            lag_days=1,
        ),
        _blueprint(
            market_id="YIELD-CURVE-POSITIVE-BY-Q1-2027",
            title="10Y-2Y Treasury curve turns positive by Q1 2027",
            question="Will the US Treasury yield curve turn positive by the end of Q1 2027?",
            category="interest_rates",
            families=["interest_rates", "economic_growth", "monetary_policy"],
            tags=["yield_curve", "treasury_yields", "curve_steepening", "economic_outlook"],
            close_time="2027-03-31T20:00:00Z",
            bias=-0.16,
            exposures={"curve_steepening": 0.92, "recession_risk": 0.42, "yield_level": -0.12},
            lag_days=1,
        ),
        _blueprint(
            market_id="BTC-SEP-2026-ABOVE-110K",
            title="Bitcoin above 110k by September 2026",
            question="Will Bitcoin trade above 110,000 dollars before the end of September 2026?",
            category="crypto",
            families=["crypto", "risk_assets"],
            tags=["crypto", "bitcoin", "btc", "risk"],
            close_time="2026-09-30T20:00:00Z",
            bias=-0.15,
            exposures={"crypto_bull": 1.08, "policy_dovish": 0.22},
        ),
        _blueprint(
            market_id="AUSTIN-JUL-2026-100F",
            title="Austin hits 100F by July 2026",
            question="Will Austin record a 100 degree Fahrenheit day before the end of July 2026?",
            category="weather",
            families=["weather", "heat"],
            tags=["weather", "temperature", "texas", "heat"],
            close_time="2026-07-31T23:00:00Z",
            bias=-0.30,
            exposures={"heat": 1.15},
        ),
        _blueprint(
            market_id="HOUSE-2026-GOP-MAJORITY",
            title="Republicans win House majority in 2026",
            question="Will Republicans control the US House after the November 2026 election?",
            category="politics",
            families=["elections", "politics"],
            tags=["elections", "politics", "house", "congress"],
            close_time="2026-11-04T06:00:00Z",
            bias=-0.08,
            exposures={"election_cycle": 1.00},
        ),
    ]


class BaseMarketDataProvider(ABC):
    name: str

    @abstractmethod
    def fetch_market_metadata(
        self,
        *,
        scope_config: PipelineScopeConfig | None = None,
        discovery_mode: str = "all",
    ) -> list[MarketMetadataRecord]:
        raise NotImplementedError

    @abstractmethod
    def fetch_market_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        raise NotImplementedError

    def fetch_market_histories(self, markets: list[MarketMetadataRecord]) -> dict[str, pd.DataFrame]:
        return {}

    def should_refresh_metadata_cache(
        self,
        cache_path: Path,
        *,
        scope_config: PipelineScopeConfig | None = None,
    ) -> bool:
        return False

    def should_refresh_history_cache(self, market: MarketMetadataRecord, cache_path: Path) -> bool:
        return False

    def set_metadata_progress_path(self, progress_path: Path | None) -> None:
        return None

    def mark_metadata_progress(
        self,
        *,
        status: str,
        discovered_market_count: int | None = None,
        message: str | None = None,
    ) -> None:
        return None

    def set_metadata_snapshot_callback(
        self,
        callback: Callable[[list[MarketMetadataRecord], str], None] | None,
    ) -> None:
        return None


class MockMarketDataProvider(BaseMarketDataProvider):
    name = "mock"

    def __init__(self, seed: int = 17) -> None:
        self.seed = seed
        self._blueprints = {blueprint.market_id: blueprint for blueprint in _build_mock_blueprints()}

    def fetch_market_metadata(
        self,
        *,
        scope_config: PipelineScopeConfig | None = None,
        discovery_mode: str = "all",
    ) -> list[MarketMetadataRecord]:
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
            0.18 * np.sin(t / 7.0)
            + 0.76 * (t >= 18)
            - 0.54 * (t >= 52)
            + 0.40 * (t >= 86)
            + 0.08 * np.cos(t / 3.2)
        )
        inflation_pressure = 0.84 * policy_hawkish + 0.18 * np.cos(t / 4.2) + 0.12 * (t >= 36)
        growth_cycle = (
            0.18 * np.sin(t / 5.3)
            + 0.28 * np.cos(t / 6.1)
            + 0.34 * (t >= 30)
            - 0.40 * (t >= 92)
            - 0.10 * policy_hawkish
        )
        recession_risk = -0.86 * growth_cycle + 0.22 * np.sin(t / 8.4) + 0.18 * (t >= 94)
        policy_dovish = -0.90 * policy_hawkish + 0.54 * recession_risk + 0.10 * np.cos(t / 6.7)
        policy_stable = 0.62 * np.cos(t / 9.5) - 0.24 * np.abs(policy_hawkish) + 0.14 * growth_cycle
        inflation_cooling = -0.80 * inflation_pressure + 0.34 * recession_risk + 0.12 * np.sin(t / 4.4)
        jobs_strength = 0.34 * policy_hawkish + 0.44 * np.sin(t / 5.5) + 0.34 * (t >= 28) - 0.24 * (t >= 74)
        labor_slack = -0.82 * jobs_strength + 0.50 * recession_risk + 0.10 * np.sin(t / 4.4)
        wage_pressure = 0.56 * jobs_strength + 0.42 * inflation_pressure - 0.18 * labor_slack + 0.08 * np.cos(t / 4.9)
        participation_strength = 0.64 * jobs_strength - 0.26 * wage_pressure + 0.14 * np.cos(t / 5.7)
        yield_level = 0.74 * policy_hawkish + 0.24 * inflation_pressure - 0.18 * policy_dovish + 0.14 * np.cos(t / 5.0)
        curve_steepening = 0.62 * recession_risk - 0.16 * policy_hawkish + 0.18 * np.cos(t / 4.8)
        crypto_bull = -0.30 * policy_hawkish + 0.58 * np.sin(t / 5.8) + 0.35 * (t >= 64) - 0.18 * (t >= 112)
        heat = 0.34 * np.sin(t / 4.2) + 0.72 * ((t >= 34) & (t < 82)) - 0.16 * (t >= 112)
        election_cycle = 0.18 * np.sin(t / 6.4) + 0.78 * (t >= 98)
        return pd.DataFrame(
            {
                "policy_hawkish": policy_hawkish,
                "policy_dovish": policy_dovish,
                "policy_stable": policy_stable,
                "inflation_pressure": inflation_pressure,
                "inflation_cooling": inflation_cooling,
                "jobs_strength": jobs_strength,
                "labor_slack": labor_slack,
                "wage_pressure": wage_pressure,
                "participation_strength": participation_strength,
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

    def fetch_market_metadata(
        self,
        *,
        scope_config: PipelineScopeConfig | None = None,
        discovery_mode: str = "all",
    ) -> list[MarketMetadataRecord]:
        json_path = self.snapshot_dir / "markets.json"
        csv_path = self.snapshot_dir / "markets.csv"
        if json_path.exists():
            payload = read_json(json_path)
            records = payload["records"] if isinstance(payload, dict) and "records" in payload else payload
            return [MarketMetadataRecord.from_mapping(record) for record in records]
        if csv_path.exists():
            frame = pd.read_csv(csv_path)
            return [MarketMetadataRecord.from_mapping(record) for record in frame.to_dict(orient="records")]
        raise FileNotFoundError(f"expected a snapshot metadata file at {json_path} or {csv_path}")

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
        raise FileNotFoundError(f"expected history for {market.market_id} at {per_market_path} or {aggregate_path}")


class KalshiLiveMarketDataProvider(BaseMarketDataProvider):
    name = "kalshi_live"

    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = settings or json.loads(json.dumps(DEFAULT_PROVIDER_SETTINGS["kalshi_live"]))
        self.base_url = str(self.settings["base_url"]).rstrip("/")
        self._event_cache: dict[str, dict[str, Any]] = {}
        self._historical_cutoff_cache: dict[str, Any] | None = None
        self._metadata_progress_path: Path | None = None
        self._last_progress_write_at: float = 0.0
        self._metadata_snapshot_callback: Callable[[list[MarketMetadataRecord], str], None] | None = None

    def set_metadata_progress_path(self, progress_path: Path | None) -> None:
        self._metadata_progress_path = progress_path
        self._last_progress_write_at = 0.0

    def set_metadata_snapshot_callback(
        self,
        callback: Callable[[list[MarketMetadataRecord], str], None] | None,
    ) -> None:
        self._metadata_snapshot_callback = callback

    def mark_metadata_progress(
        self,
        *,
        status: str,
        discovered_market_count: int | None = None,
        message: str | None = None,
    ) -> None:
        self._write_progress(
            status=status,
            force=True,
            discovered_market_count=discovered_market_count,
            message=message,
        )

    def fetch_market_metadata(
        self,
        *,
        scope_config: PipelineScopeConfig | None = None,
        discovery_mode: str = "all",
    ) -> list[MarketMetadataRecord]:
        discovery_start_ts = self._discovery_start_ts(scope_config)
        relevant_event_tickers: set[str] | None = None
        relevant_series_tickers: set[str] | None = None
        if discovery_mode == "scoped" and scope_config is not None:
            events = self._fetch_events_index(min_close_ts=discovery_start_ts)
            event_by_ticker = {str(event["event_ticker"]): event for event in events if event.get("event_ticker")}
            self._event_cache.update(event_by_ticker)
            relevant_event_tickers, relevant_series_tickers = self._select_scoped_events(events, scope_config)

        def emit_snapshot(
            live_market_payloads: list[dict[str, Any]],
            historical_market_payloads: list[dict[str, Any]],
            stage_label: str,
        ) -> None:
            if self._metadata_snapshot_callback is None:
                return
            market_by_ticker: dict[str, dict[str, Any]] = {}
            for market in [*live_market_payloads, *historical_market_payloads]:
                ticker = str(market.get("ticker") or "").strip()
                if not ticker:
                    continue
                market_by_ticker[ticker] = market
            if not market_by_ticker:
                return
            snapshot_records = [
                self._market_to_record(market, self._event_cache.get(str(market.get("event_ticker"))))
                for _, market in sorted(market_by_ticker.items())
            ]
            if discovery_mode == "scoped" and scope_config is not None:
                from .scope import select_scoped_markets

                scoped_records, _ = select_scoped_markets(snapshot_records, scope_config)
                snapshot_records = scoped_records
            self._metadata_snapshot_callback(snapshot_records, stage_label)

        live_markets = self._fetch_live_markets(
            discovery_start_ts=discovery_start_ts,
            event_tickers=relevant_event_tickers,
            series_tickers=relevant_series_tickers,
            on_page=lambda page_live_markets, page_label: emit_snapshot(page_live_markets, [], page_label),
        )
        historical_markets: list[dict[str, Any]] = []
        historical_cutoff = self._get_historical_cutoff()
        cutoff_ts = parse_timestamp(historical_cutoff.get("market_settled_ts"))
        if (
            self.settings.get("include_historical_markets", True)
            and cutoff_ts is not None
            and discovery_start_ts < int(cutoff_ts.timestamp())
            and relevant_event_tickers
        ):
            historical_markets = self._fetch_historical_markets(
                event_tickers=relevant_event_tickers,
                series_tickers=relevant_series_tickers,
                on_page=lambda page_historical_markets, page_label: emit_snapshot(live_markets, page_historical_markets, page_label),
            )

        market_by_ticker: dict[str, dict[str, Any]] = {}
        for market in [*live_markets, *historical_markets]:
            ticker = str(market.get("ticker") or "").strip()
            if not ticker:
                continue
            market_by_ticker[ticker] = market

        missing_event_tickers = {
            str(market.get("event_ticker"))
            for market in market_by_ticker.values()
            if market.get("event_ticker") and str(market.get("event_ticker")) not in self._event_cache
        }
        if missing_event_tickers:
            self.mark_metadata_progress(
                status="running",
                discovered_market_count=len(market_by_ticker),
                message=f"Enriching event metadata for {len(missing_event_tickers)} discovered events.",
            )
            self._ensure_event_details(missing_event_tickers)
            emit_snapshot(
                list(market_by_ticker.values()),
                [],
                "event metadata enrichment",
            )

        records = [
            self._market_to_record(market, self._event_cache.get(str(market.get("event_ticker"))))
            for _, market in sorted(market_by_ticker.items())
        ]
        if discovery_mode == "scoped" and scope_config is not None:
            from .scope import select_scoped_markets

            scoped_records, _ = select_scoped_markets(records, scope_config)
            return scoped_records
        return records

    def fetch_market_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        frames = self.fetch_market_histories([market])
        if market.market_id in frames:
            return frames[market.market_id]
        return self._empty_history_frame(market.market_id)

    def fetch_market_histories(self, markets: list[MarketMetadataRecord]) -> dict[str, pd.DataFrame]:
        if not markets:
            return {}
        history_by_market: dict[str, pd.DataFrame] = {}
        live_markets_by_interval: dict[int, list[MarketMetadataRecord]] = {}
        historical_markets: list[MarketMetadataRecord] = []

        for market in markets:
            if self._is_historical_market(market):
                historical_markets.append(market)
                continue
            interval = self._history_period_interval_minutes(market)
            live_markets_by_interval.setdefault(interval, []).append(market)

        for interval, grouped_markets in live_markets_by_interval.items():
            history_by_market.update(self._fetch_live_histories_batch(grouped_markets, interval))
        for market in historical_markets:
            history_by_market[market.market_id] = self._fetch_historical_history(market)
        return history_by_market

    def should_refresh_metadata_cache(
        self,
        cache_path: Path,
        *,
        scope_config: PipelineScopeConfig | None = None,
    ) -> bool:
        ttl_seconds = _safe_int(self.settings.get("metadata_cache_ttl_seconds"), 900)
        if ttl_seconds <= 0 or not cache_path.exists():
            return False
        age_seconds = time.time() - cache_path.stat().st_mtime
        return age_seconds > ttl_seconds

    def should_refresh_history_cache(self, market: MarketMetadataRecord, cache_path: Path) -> bool:
        if not cache_path.exists():
            return True
        status = str(market.status or "").lower()
        if status in {"active", "open", "paused", "initialized", "unopened"}:
            ttl_seconds = _safe_int(self.settings.get("open_market_history_cache_ttl_seconds"), 900)
        else:
            ttl_seconds = _safe_int(self.settings.get("closed_market_history_cache_ttl_seconds"), 86400)
        if ttl_seconds <= 0:
            return False
        age_seconds = time.time() - cache_path.stat().st_mtime
        return age_seconds > ttl_seconds

    def _request_json(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        encoded_query = urllib_parse.urlencode(
            {key: value for key, value in (query or {}).items() if value is not None and value != ""},
            doseq=False,
        )
        url = f"{self.base_url}{path}"
        if encoded_query:
            url = f"{url}?{encoded_query}"
        timeout_seconds = _safe_int(self.settings.get("timeout_seconds"), 30)
        retry_count = _safe_int(self.settings.get("retry_count"), 3)
        retry_backoff_seconds = _safe_float(self.settings.get("retry_backoff_seconds"), 1.5)
        last_error_message = ""
        for attempt in range(retry_count + 1):
            req = urllib_request.Request(url, method="GET")
            try:
                with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
                    return json.load(response)
            except urllib_error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last_error_message = body or str(exc)
                if exc.code in {408, 409, 429, 500, 502, 503, 504} and attempt < retry_count:
                    time.sleep(retry_backoff_seconds * (attempt + 1))
                    continue
                raise RuntimeError(f"Kalshi request failed with HTTP {exc.code}: {last_error_message}") from exc
            except urllib_error.URLError as exc:
                last_error_message = str(exc.reason)
                if attempt < retry_count:
                    time.sleep(retry_backoff_seconds * (attempt + 1))
                    continue
                raise RuntimeError(f"Kalshi request failed: {last_error_message}") from exc
        raise RuntimeError(f"Kalshi request failed after retries: {last_error_message}")

    def _paginate(
        self,
        path: str,
        key: str,
        query: dict[str, Any] | None = None,
        *,
        max_pages: int = 0,
        progress_label: str | None = None,
        on_page: Callable[[list[dict[str, Any]], int], None] | None = None,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        all_records: list[dict[str, Any]] = []
        page_count = 0
        while True:
            page_query = dict(query or {})
            if cursor:
                page_query["cursor"] = cursor
            payload = self._request_json(path, page_query)
            all_records.extend(payload.get(key, []))
            next_cursor = payload.get("cursor") or None
            page_count += 1
            if progress_label:
                self._write_progress(
                    status="running",
                    discovered_market_count=len(all_records),
                    page_count=page_count,
                    message=f"{progress_label}: fetched {len(all_records)} records across {page_count} pages",
                )
            if on_page is not None:
                on_page(list(all_records), page_count)
            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
            if max_pages > 0 and page_count >= max_pages:
                break
        return all_records

    def _discovery_start_ts(self, scope_config: PipelineScopeConfig | None) -> int:
        if scope_config and scope_config.window_start:
            parsed = parse_timestamp(scope_config.window_start)
            if parsed is not None:
                return int(parsed.timestamp())
        return int(time.time()) - (_safe_int(self.settings.get("discovery_lookback_days"), 120) * 86_400)

    def _get_historical_cutoff(self) -> dict[str, Any]:
        if self._historical_cutoff_cache is None:
            self._historical_cutoff_cache = self._request_json("/historical/cutoff")
        return self._historical_cutoff_cache

    def _fetch_events_index(self, *, min_close_ts: int | None = None) -> list[dict[str, Any]]:
        query = {
            "limit": min(200, _safe_int(self.settings.get("event_page_limit"), 200)),
            "with_nested_markets": "false",
        }
        if min_close_ts:
            query["min_close_ts"] = min_close_ts
        return self._paginate(
            "/events",
            "events",
            query,
            max_pages=max(0, _safe_int(self.settings.get("max_event_pages"), 0)),
            progress_label="events index",
        )

    def _select_scoped_events(
        self,
        events: list[dict[str, Any]],
        scope_config: PipelineScopeConfig,
    ) -> tuple[set[str], set[str]]:
        from .scope import evaluate_scope_match

        relevant_event_tickers: set[str] = set()
        relevant_series_tickers: set[str] = set()
        for event in events:
            event_ticker = str(event.get("event_ticker") or "").strip()
            if not event_ticker:
                continue
            metadata_tokens = [
                str(event.get("series_ticker") or ""),
                str(event.get("category") or ""),
                str(event.get("title") or ""),
                str(event.get("sub_title") or ""),
            ]
            product_metadata = event.get("product_metadata") or {}
            for key, value in product_metadata.items():
                metadata_tokens.append(str(key))
                metadata_tokens.append(str(value))
            pseudo_record = MarketMetadataRecord(
                market_id=event_ticker,
                ticker=event_ticker,
                title=str(event.get("title") or event_ticker),
                question=" ".join(part for part in metadata_tokens if part).strip(),
                category=normalize_text(str(event.get("category") or "")).replace(" ", "_") or None,
                families=[
                    normalize_text(str(event.get("category") or "")).replace(" ", "_"),
                    normalize_text(str(event.get("series_ticker") or "")).replace(" ", "_"),
                ],
                tags=[normalize_text(token).replace(" ", "_") for token in metadata_tokens if normalize_text(token)],
                source=self.name,
            )
            if evaluate_scope_match(pseudo_record, scope_config).include:
                relevant_event_tickers.add(event_ticker)
                series_ticker = str(event.get("series_ticker") or "").strip()
                if series_ticker:
                    relevant_series_tickers.add(series_ticker)
        return relevant_event_tickers, relevant_series_tickers

    def _fetch_live_markets(
        self,
        *,
        discovery_start_ts: int,
        event_tickers: set[str] | None,
        series_tickers: set[str] | None,
        on_page: Callable[[list[dict[str, Any]], str], None] | None = None,
    ) -> list[dict[str, Any]]:
        query = {
            "limit": min(1000, _safe_int(self.settings.get("market_page_limit"), 1000)),
        }
        if self.settings.get("exclude_multivariate", True):
            query["mve_filter"] = "exclude"
        if discovery_start_ts:
            query["min_close_ts"] = discovery_start_ts

        if event_tickers and len(event_tickers) <= 100:
            all_markets: list[dict[str, Any]] = []
            for event_ticker in sorted(event_tickers):
                all_markets.extend(
                    self._paginate(
                        "/markets",
                        "markets",
                        {**query, "event_ticker": event_ticker},
                        max_pages=max(0, _safe_int(self.settings.get("max_market_pages"), 0)),
                        progress_label=f"live markets for event {event_ticker}",
                        on_page=(lambda page_markets, _page_count, event_ticker=event_ticker: on_page(all_markets + page_markets, f"live markets for event {event_ticker}") if on_page else None),
                    )
                )
            return all_markets
        if series_tickers and len(series_tickers) <= 100:
            all_markets = []
            for series_ticker in sorted(series_tickers):
                all_markets.extend(
                    self._paginate(
                        "/markets",
                        "markets",
                        {**query, "series_ticker": series_ticker},
                        max_pages=max(0, _safe_int(self.settings.get("max_market_pages"), 0)),
                        progress_label=f"live markets for series {series_ticker}",
                        on_page=(lambda page_markets, _page_count, series_ticker=series_ticker: on_page(all_markets + page_markets, f"live markets for series {series_ticker}") if on_page else None),
                    )
                )
            return all_markets
        return self._paginate(
            "/markets",
            "markets",
            query,
            max_pages=max(0, _safe_int(self.settings.get("max_market_pages"), 0)),
            progress_label="live markets",
            on_page=(lambda page_markets, _page_count: on_page(page_markets, "live markets") if on_page else None),
        )

    def _fetch_historical_markets(
        self,
        *,
        event_tickers: set[str] | None,
        series_tickers: set[str] | None,
        on_page: Callable[[list[dict[str, Any]], str], None] | None = None,
    ) -> list[dict[str, Any]]:
        query = {
            "limit": min(1000, _safe_int(self.settings.get("market_page_limit"), 1000)),
        }
        if self.settings.get("exclude_multivariate", True):
            query["mve_filter"] = "exclude"

        historical_markets: list[dict[str, Any]] = []
        max_queries = _safe_int(self.settings.get("max_historical_event_queries"), 250)
        if event_tickers:
            for index, event_ticker in enumerate(sorted(event_tickers)):
                if index >= max_queries:
                    break
                historical_markets.extend(
                    self._paginate(
                        "/historical/markets",
                        "markets",
                        {**query, "event_ticker": event_ticker},
                        max_pages=max(0, _safe_int(self.settings.get("max_historical_pages"), 0)),
                        progress_label=f"historical markets for event {event_ticker}",
                        on_page=(lambda page_markets, _page_count, event_ticker=event_ticker: on_page(historical_markets + page_markets, f"historical markets for event {event_ticker}") if on_page else None),
                    )
                )
            return historical_markets
        if series_tickers:
            for index, series_ticker in enumerate(sorted(series_tickers)):
                if index >= max_queries:
                    break
                historical_markets.extend(
                    self._paginate(
                        "/historical/markets",
                        "markets",
                        {**query, "series_ticker": series_ticker},
                        max_pages=max(0, _safe_int(self.settings.get("max_historical_pages"), 0)),
                        progress_label=f"historical markets for series {series_ticker}",
                        on_page=(lambda page_markets, _page_count, series_ticker=series_ticker: on_page(historical_markets + page_markets, f"historical markets for series {series_ticker}") if on_page else None),
                    )
                )
        return historical_markets

    def _write_progress(
        self,
        *,
        status: str,
        force: bool = False,
        discovered_market_count: int | None = None,
        page_count: int | None = None,
        message: str | None = None,
    ) -> None:
        if self._metadata_progress_path is None:
            return
        now = time.time()
        if not force and now - self._last_progress_write_at < 1.0:
            return
        payload: dict[str, Any] = {
            "artifact": "pipeline_progress",
            "provider": self.name,
            "schema_version": PIPELINE_PROGRESS_SCHEMA_VERSION,
            "generated_at": utc_now_iso(),
            "status": status,
        }
        if discovered_market_count is not None:
            payload["discovered_market_count"] = discovered_market_count
        if page_count is not None:
            payload["page_count"] = page_count
        if message:
            payload["message"] = message
        ensure_dir(self._metadata_progress_path.parent)
        write_json(self._metadata_progress_path, payload)
        self._last_progress_write_at = now

    def _ensure_event_details(self, event_tickers: set[str]) -> None:
        pending_event_tickers = sorted(ticker for ticker in event_tickers if ticker)
        if not pending_event_tickers:
            return

        max_workers = max(1, _safe_int(self.settings.get("event_detail_workers"), 12))

        def fetch_one(event_ticker: str) -> tuple[str, dict[str, Any] | None]:
            payload = self._request_json(f"/events/{event_ticker}")
            event = payload.get("event")
            return event_ticker, event if isinstance(event, dict) else None

        if max_workers <= 1 or len(pending_event_tickers) == 1:
            for event_ticker in pending_event_tickers:
                _, event = fetch_one(event_ticker)
                if event is not None:
                    self._event_cache[event_ticker] = event
            return

        with ThreadPoolExecutor(max_workers=min(max_workers, len(pending_event_tickers))) as executor:
            futures = {executor.submit(fetch_one, event_ticker): event_ticker for event_ticker in pending_event_tickers}
            for future in as_completed(futures):
                event_ticker, event = future.result()
                if event is not None:
                    self._event_cache[event_ticker] = event

    def _market_to_record(self, market: dict[str, Any], event: dict[str, Any] | None) -> MarketMetadataRecord:
        event = event or {}
        event_category = normalize_text(str(event.get("category") or "")).replace(" ", "_") or None
        series_ticker = str(event.get("series_ticker") or "").strip() or str(market.get("ticker", "")).split("-")[0]
        product_metadata = event.get("product_metadata") or {}
        family_tokens = [
            event_category or "",
            normalize_text(series_ticker).replace(" ", "_"),
            normalize_text(str(product_metadata.get("competition") or "")).replace(" ", "_"),
            normalize_text(str(product_metadata.get("competition_scope") or "")).replace(" ", "_"),
            normalize_text(str(market.get("strike_type") or "")).replace(" ", "_"),
        ]
        families = [token for token in family_tokens if token]
        tag_tokens: list[str] = []
        for value in [
            event.get("title"),
            event.get("sub_title"),
            market.get("yes_sub_title"),
            market.get("no_sub_title"),
            market.get("market_type"),
            market.get("status"),
            *product_metadata.values(),
        ]:
            normalized = normalize_text(str(value or "")).replace(" ", "_")
            if normalized:
                tag_tokens.append(normalized)

        question_parts = [
            str(market.get("rules_primary") or "").strip(),
            str(event.get("title") or "").strip(),
            str(event.get("sub_title") or "").strip(),
            str(market.get("yes_sub_title") or "").strip(),
        ]
        resolution_time = (
            market.get("settlement_ts")
            or market.get("expiration_time")
            or market.get("close_time")
        )
        return MarketMetadataRecord(
            market_id=str(market["ticker"]),
            ticker=str(market["ticker"]),
            title=str(market.get("title") or event.get("title") or market["ticker"]),
            question=" ".join(part for part in question_parts if part) or str(market.get("title") or market["ticker"]),
            category=event_category,
            families=families,
            open_time=market.get("open_time"),
            close_time=market.get("close_time"),
            resolution_time=resolution_time,
            status=market.get("status"),
            tags=tag_tokens,
            source=self.name,
            extra={
                "event_ticker": market.get("event_ticker"),
                "series_ticker": series_ticker,
                "event_title": event.get("title"),
                "event_sub_title": event.get("sub_title"),
                "event_category": event.get("category"),
                "settlement_ts": market.get("settlement_ts"),
                "occurrence_datetime": market.get("occurrence_datetime"),
                "rules_primary": market.get("rules_primary"),
                "rules_secondary": market.get("rules_secondary"),
                "product_metadata": product_metadata,
                "market_type": market.get("market_type"),
                "response_price_units": market.get("response_price_units"),
                "last_price_dollars": market.get("last_price_dollars"),
                "volume_fp": market.get("volume_fp"),
                "open_interest_fp": market.get("open_interest_fp"),
                "fetched_at": utc_now_iso(),
            },
        )

    def _is_historical_market(self, market: MarketMetadataRecord) -> bool:
        settlement_ts = parse_timestamp(str(market.extra.get("settlement_ts") or market.resolution_time or ""))
        cutoff = parse_timestamp(str(self._get_historical_cutoff().get("market_settled_ts") or ""))
        return settlement_ts is not None and cutoff is not None and settlement_ts < cutoff

    def _history_period_interval_minutes(self, market: MarketMetadataRecord) -> int:
        short_days = _safe_int(self.settings.get("history_short_duration_days"), 45)
        short_interval = _safe_int(self.settings.get("history_short_interval_minutes"), 60)
        long_interval = _safe_int(self.settings.get("history_long_interval_minutes"), 1440)
        opened_at = parse_timestamp(market.open_time)
        closed_at = parse_timestamp(market.close_time) or parse_timestamp(market.resolution_time)
        if opened_at is None or closed_at is None:
            return short_interval
        duration_days = max(1.0, (closed_at - opened_at).total_seconds() / 86_400)
        return short_interval if duration_days <= short_days else long_interval

    def _history_window(self, market: MarketMetadataRecord) -> tuple[int, int]:
        now_ts = int(time.time())
        lookback_days = _safe_int(self.settings.get("history_default_lookback_days"), 120)
        opened_at = parse_timestamp(market.open_time)
        closed_at = parse_timestamp(market.close_time) or parse_timestamp(market.resolution_time)
        start_ts = int(opened_at.timestamp()) if opened_at is not None else now_ts - (lookback_days * 86_400)
        end_ts = int(closed_at.timestamp()) if closed_at is not None else now_ts
        end_ts = max(start_ts + 60, min(end_ts, now_ts))
        return start_ts, end_ts

    def _fetch_live_histories_batch(
        self,
        markets: list[MarketMetadataRecord],
        interval_minutes: int,
    ) -> dict[str, pd.DataFrame]:
        history_by_market: dict[str, pd.DataFrame] = {}
        if not markets:
            return history_by_market

        batch_chunk_size = max(1, min(100, _safe_int(self.settings.get("batch_candlestick_chunk_size"), 100)))
        include_latest_before_start = str(self.settings.get("history_include_latest_before_start", False)).lower()
        chunk: list[MarketMetadataRecord] = []
        chunk_period_budget = 0
        for market in markets + [None]:
            if market is not None:
                start_ts, end_ts = self._history_window(market)
                estimated_periods = max(1, int(((end_ts - start_ts) / max(interval_minutes * 60, 60))) + 2)
                next_budget = chunk_period_budget + estimated_periods
                if chunk and (len(chunk) >= batch_chunk_size or next_budget > 8_000):
                    pass
                else:
                    chunk.append(market)
                    chunk_period_budget = next_budget
                    continue
            if not chunk:
                continue
            market_windows = {market.market_id: self._history_window(market) for market in chunk}
            start_ts = min(window[0] for window in market_windows.values())
            end_ts = max(window[1] for window in market_windows.values())
            query = {
                "market_tickers": ",".join(market.market_id for market in chunk),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": interval_minutes,
                "include_latest_before_start": include_latest_before_start,
            }
            try:
                payload = self._request_json("/markets/candlesticks", query)
                markets_payload = payload.get("markets", [])
                returned_tickers = set()
                for market_payload in markets_payload:
                    market_ticker = str(market_payload.get("market_ticker") or "")
                    if not market_ticker:
                        continue
                    returned_tickers.add(market_ticker)
                    frame = self._candlesticks_to_frame(
                        market_id=market_ticker,
                        candlesticks=market_payload.get("candlesticks", []),
                        historical=False,
                    )
                    market_start_ts, market_end_ts = market_windows.get(market_ticker, (start_ts, end_ts))
                    if not frame.empty:
                        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
                        frame = frame[
                            (frame["timestamp"] >= pd.to_datetime(market_start_ts, unit="s", utc=True))
                            & (frame["timestamp"] <= pd.to_datetime(market_end_ts, unit="s", utc=True))
                        ]
                        frame["timestamp"] = frame["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    history_by_market[market_ticker] = (
                        normalize_history_frame(frame, market_ticker)
                        if not frame.empty
                        else self._empty_history_frame(market_ticker)
                    )
                for chunk_market in chunk:
                    if chunk_market.market_id not in returned_tickers:
                        history_by_market[chunk_market.market_id] = self._fetch_live_history_single(chunk_market)
            except Exception:
                for chunk_market in chunk:
                    history_by_market[chunk_market.market_id] = self._fetch_live_history_single(chunk_market)
            chunk = [market] if market is not None else []
            if market is not None:
                market_start_ts, market_end_ts = self._history_window(market)
                chunk_period_budget = max(
                    1,
                    int(((market_end_ts - market_start_ts) / max(interval_minutes * 60, 60))) + 2,
                )
            else:
                chunk_period_budget = 0
        return history_by_market

    def _fetch_live_history_single(self, market: MarketMetadataRecord) -> pd.DataFrame:
        series_ticker = str(market.extra.get("series_ticker") or "").strip()
        if not series_ticker:
            self._ensure_event_details({str(market.extra.get("event_ticker") or "")})
            event = self._event_cache.get(str(market.extra.get("event_ticker") or ""))
            series_ticker = str((event or {}).get("series_ticker") or "").strip()
        if not series_ticker:
            return self._empty_history_frame(market.market_id)
        start_ts, end_ts = self._history_window(market)
        query = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": self._history_period_interval_minutes(market),
            "include_latest_before_start": str(self.settings.get("history_include_latest_before_start", False)).lower(),
        }
        try:
            payload = self._request_json(f"/series/{series_ticker}/markets/{market.market_id}/candlesticks", query)
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return self._empty_history_frame(market.market_id)
            raise
        return self._candlesticks_to_frame(market.market_id, payload.get("candlesticks", []), historical=False)

    def _fetch_historical_history(self, market: MarketMetadataRecord) -> pd.DataFrame:
        start_ts, end_ts = self._history_window(market)
        query = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": self._history_period_interval_minutes(market),
        }
        try:
            payload = self._request_json(f"/historical/markets/{market.market_id}/candlesticks", query)
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return self._empty_history_frame(market.market_id)
            raise
        return self._candlesticks_to_frame(market.market_id, payload.get("candlesticks", []), historical=True)

    def _candlesticks_to_frame(
        self,
        market_id: str,
        candlesticks: list[dict[str, Any]],
        *,
        historical: bool,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for candle in candlesticks or []:
            end_period_ts = candle.get("end_period_ts")
            if end_period_ts is None:
                continue
            price = candle.get("price") or {}
            open_key = "open" if historical else "open_dollars"
            high_key = "high" if historical else "high_dollars"
            low_key = "low" if historical else "low_dollars"
            close_key = "close" if historical else "close_dollars"
            open_price = price.get(open_key)
            high_price = price.get(high_key)
            low_price = price.get(low_key)
            close_price = price.get(close_key)
            if close_price is None:
                continue
            rows.append(
                {
                    "market_id": market_id,
                    "timestamp": pd.to_datetime(int(end_period_ts), unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "open": float(open_price) if open_price is not None else float(close_price),
                    "high": float(high_price) if high_price is not None else float(close_price),
                    "low": float(low_price) if low_price is not None else float(close_price),
                    "close": float(close_price),
                    "volume": float(candle.get("volume") if historical else candle.get("volume_fp") or 0.0),
                    "source": self.name,
                }
            )
        if not rows:
            return self._empty_history_frame(market_id)
        frame = pd.DataFrame(rows)
        return normalize_history_frame(frame, market_id)

    def _empty_history_frame(self, market_id: str) -> pd.DataFrame:
        return pd.DataFrame(columns=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"])


def get_provider(
    provider_name: str,
    *,
    snapshot_dir: Path | None = None,
    config_path: Path | None = None,
) -> BaseMarketDataProvider:
    normalized_name = provider_name.lower().strip()
    if normalized_name == "mock":
        return MockMarketDataProvider()
    if normalized_name == "snapshot":
        return SnapshotMarketDataProvider(snapshot_dir=snapshot_dir)
    if normalized_name == "kalshi_live":
        return KalshiLiveMarketDataProvider(resolve_provider_settings(normalized_name, config_path=config_path))
    raise ValueError(f"unsupported provider '{provider_name}'")
