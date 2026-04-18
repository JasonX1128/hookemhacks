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
HISTORY_SCHEMA_VERSION = "1.0"
CANDIDATE_SCHEMA_VERSION = "1.0"
PAIR_FEATURES_SCHEMA_VERSION = "1.0"
COINTEGRATION_SCHEMA_VERSION = "1.0"


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
    def metadata_cache_path(self) -> Path:
        return self.cache_dir / "market_metadata_raw.json"

    @property
    def history_cache_dir(self) -> Path:
        return self.cache_dir / "history"

    @property
    def metadata_artifact_path(self) -> Path:
        return self.artifacts_dir / "market_metadata.json"

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
    def cointegration_artifact_path(self) -> Path:
        return self.artifacts_dir / "cointegration_metrics.csv"

    @property
    def artifact_manifest_path(self) -> Path:
        return self.artifacts_dir / "artifact_manifest.json"

    @property
    def scope_artifact_path(self) -> Path:
        return self.artifacts_dir / "run_scope.json"

    @property
    def run_summary_path(self) -> Path:
        return self.artifacts_dir / "run_summary.json"
