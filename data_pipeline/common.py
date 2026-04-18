from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parent
CACHE_ROOT = PIPELINE_ROOT / "cache"
ARTIFACT_ROOT = PIPELINE_ROOT / "artifacts"
CONFIG_ROOT = PIPELINE_ROOT / "configs"
FIXTURES_ROOT = PIPELINE_ROOT / "fixtures"

SCOPE_SCHEMA_VERSION = "1.0"
RUN_SUMMARY_SCHEMA_VERSION = "1.0"
METADATA_SCHEMA_VERSION = "1.0"
RELATED_MARKETS_UNIVERSE_SCHEMA_VERSION = "1.0"
HISTORY_SCHEMA_VERSION = "1.0"
CANDIDATE_SCHEMA_VERSION = "1.0"
PAIR_FEATURES_SCHEMA_VERSION = "1.0"
COINTEGRATION_SCHEMA_VERSION = "1.0"
MARKET_CATALOG_SCHEMA_VERSION = "1.0"
CATEGORY_REGISTRY_SCHEMA_VERSION = "1.0"
MARKET_ASSIGNMENTS_SCHEMA_VERSION = "1.0"
DISCOVERY_REPORT_SCHEMA_VERSION = "1.0"
LLM_BATCH_SCHEMA_VERSION = "1.0"
LLM_APPLICATION_SCHEMA_VERSION = "1.0"
PROMOTION_REPORT_SCHEMA_VERSION = "1.0"
LLM_RUN_SCHEMA_VERSION = "1.0"
SCHEDULER_CYCLE_SCHEMA_VERSION = "1.0"
SCHEDULER_STATE_SCHEMA_VERSION = "1.0"
PIPELINE_PROGRESS_SCHEMA_VERSION = "1.0"


@dataclass(slots=True)
class PipelinePaths:
    provider_name: str
    scope_slug: str = "default"
    base_dir: Path = PIPELINE_ROOT

    @property
    def cache_dir(self) -> Path:
        return CACHE_ROOT / self.provider_name / self.scope_slug

    @property
    def artifacts_dir(self) -> Path:
        return ARTIFACT_ROOT / self.scope_slug

    @property
    def published_dir(self) -> Path:
        return self.artifacts_dir / "published"

    @property
    def metadata_cache_path(self) -> Path:
        return self.cache_dir / "market_metadata_raw.json"

    @property
    def history_cache_dir(self) -> Path:
        return self.cache_dir / "history"

    @property
    def metadata_artifact_path(self) -> Path:
        return self.artifacts_dir / "market_metadata.json"

    @property
    def related_markets_universe_path(self) -> Path:
        return self.artifacts_dir / "related_markets_universe.json"

    @property
    def published_metadata_artifact_path(self) -> Path:
        return self.published_dir / "market_metadata.json"

    @property
    def published_related_markets_universe_path(self) -> Path:
        return self.published_dir / "related_markets_universe.json"

    @property
    def history_artifact_path(self) -> Path:
        return self.artifacts_dir / "market_history.csv"

    @property
    def history_manifest_path(self) -> Path:
        return self.artifacts_dir / "history_manifest.json"

    @property
    def clusters_artifact_path(self) -> Path:
        return self.artifacts_dir / "market_clusters.json"

    @property
    def candidates_artifact_path(self) -> Path:
        return self.artifacts_dir / "related_candidates.json"

    @property
    def pair_features_artifact_path(self) -> Path:
        return self.artifacts_dir / "pair_features.csv"

    @property
    def published_pair_features_artifact_path(self) -> Path:
        return self.published_dir / "pair_features.csv"

    @property
    def cointegration_artifact_path(self) -> Path:
        return self.artifacts_dir / "cointegration_metrics.csv"

    @property
    def published_cointegration_artifact_path(self) -> Path:
        return self.published_dir / "cointegration_metrics.csv"

    @property
    def artifact_manifest_path(self) -> Path:
        return self.artifacts_dir / "artifact_manifest.json"

    @property
    def published_artifact_manifest_path(self) -> Path:
        return self.published_dir / "artifact_manifest.json"

    @property
    def scope_artifact_path(self) -> Path:
        return self.artifacts_dir / "run_scope.json"

    @property
    def published_scope_artifact_path(self) -> Path:
        return self.published_dir / "run_scope.json"

    @property
    def run_summary_path(self) -> Path:
        return self.artifacts_dir / "run_summary.json"

    @property
    def published_run_summary_path(self) -> Path:
        return self.published_dir / "run_summary.json"

    @property
    def pipeline_progress_path(self) -> Path:
        return self.artifacts_dir / "pipeline_progress.json"

    @property
    def categorization_dir(self) -> Path:
        return self.artifacts_dir / "categorization"

    @property
    def market_catalog_path(self) -> Path:
        return self.categorization_dir / "market_catalog.json"

    @property
    def category_registry_path(self) -> Path:
        return self.categorization_dir / "category_registry.json"

    @property
    def market_assignments_path(self) -> Path:
        return self.categorization_dir / "market_assignments.json"

    @property
    def discovery_report_path(self) -> Path:
        return self.categorization_dir / "discovery_report.json"

    @property
    def llm_batch_path(self) -> Path:
        return self.categorization_dir / "pending_llm_categorization_batch.json"

    @property
    def llm_prompt_path(self) -> Path:
        return self.categorization_dir / "pending_llm_categorization_prompt.txt"

    @property
    def llm_response_dir(self) -> Path:
        return self.categorization_dir / "llm_responses"

    @property
    def llm_application_dir(self) -> Path:
        return self.categorization_dir / "applied_decisions"

    @property
    def promotion_report_path(self) -> Path:
        return self.categorization_dir / "category_promotion_report.json"

    @property
    def llm_run_dir(self) -> Path:
        return self.categorization_dir / "llm_runs"

    @property
    def scheduler_runs_dir(self) -> Path:
        return self.categorization_dir / "scheduler_runs"

    @property
    def scheduler_state_path(self) -> Path:
        return self.categorization_dir / "scheduler_state.json"

    @property
    def scheduler_lock_path(self) -> Path:
        return self.categorization_dir / "scheduler.lock"
