from __future__ import annotations

import argparse
import json
import os
import random
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from .artifact_io import artifact_relative_path
from .common import (
    ARTIFACT_ROOT,
    CATEGORY_REGISTRY_SCHEMA_VERSION,
    DISCOVERY_REPORT_SCHEMA_VERSION,
    LLM_APPLICATION_SCHEMA_VERSION,
    LLM_BATCH_SCHEMA_VERSION,
    LLM_RUN_SCHEMA_VERSION,
    MARKET_ASSIGNMENTS_SCHEMA_VERSION,
    MARKET_CATALOG_SCHEMA_VERSION,
    PROMOTION_REPORT_SCHEMA_VERSION,
    SCHEDULER_CYCLE_SCHEMA_VERSION,
    SCHEDULER_STATE_SCHEMA_VERSION,
    PipelinePaths,
)
from .providers import get_provider
from .schemas import MarketMetadataRecord
from .scope import (
    PipelineScopeConfig,
    add_scope_arguments,
    persist_scope_artifact,
    resolve_scope_from_args,
    select_scoped_markets,
)
from .utils import (
    build_json_envelope,
    normalize_text,
    semantic_similarity,
    update_artifact_manifest,
    utc_now_iso,
    write_json,
    read_json,
    ensure_dir,
)


DEFAULT_WORKFLOW_SETTINGS: dict[str, Any] = {
    "discovery_mode": "all",
    "llm_thresholds": {
        "min_new_markets": 5,
        "min_unassigned_markets": 8,
        "min_markets_to_prepare_batch": 5,
        "min_markets_per_new_category": 3,
    },
    "promotion_thresholds": {
        "min_market_count": 3,
        "min_average_confidence": 0.8,
        "min_coherence": 0.35,
        "min_stability_runs": 1,
    },
    "llm_execution": {
        "provider": "google",
        "api_base_url": "https://generativelanguage.googleapis.com/v1beta",
        "api_key_env_var": "GEMINI_API_KEY",
        "model": "gemini-2.5-flash",
        "temperature": 0.1,
        "max_output_tokens": 4000,
        "timeout_seconds": 120,
        "retry_count": 2,
        "retry_backoff_seconds": 2.0,
    },
    "scheduler": {
        "pull_interval_seconds": 300,
        "success_cooldown_seconds": 0,
        "failure_cooldown_seconds": 60,
        "sleep_jitter_seconds": 0,
        "max_consecutive_failures": 0,
    },
}


def _load_workflow_settings(config_path: Path | None) -> dict[str, Any]:
    settings = json.loads(json.dumps(DEFAULT_WORKFLOW_SETTINGS))
    if config_path is None or not config_path.exists():
        return settings
    payload = read_json(config_path)
    workflow = payload.get("manual_categorization") or {}
    if workflow.get("discovery_mode") in {"all", "scoped"}:
        settings["discovery_mode"] = workflow["discovery_mode"]
    for section in ("llm_thresholds", "promotion_thresholds", "llm_execution", "scheduler"):
        if isinstance(workflow.get(section), dict):
            settings[section].update(workflow[section])
    return settings


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    return read_json(path) if path.exists() else None


def _load_dotenv(dotenv_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not dotenv_path.exists():
        return values
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_llm_execution_settings(config_path: Path | None) -> dict[str, Any]:
    settings = _load_workflow_settings(config_path)
    llm_settings = dict(settings["llm_execution"])
    dotenv_values = _load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key_env_var = str(
        os.environ.get("DATA_PIPELINE_LLM_API_KEY_ENV_VAR")
        or dotenv_values.get("DATA_PIPELINE_LLM_API_KEY_ENV_VAR")
        or llm_settings.get("api_key_env_var")
        or "OPENAI_API_KEY"
    )
    llm_settings["api_key_env_var"] = api_key_env_var

    env_model = os.environ.get("DATA_PIPELINE_LLM_MODEL") or dotenv_values.get("DATA_PIPELINE_LLM_MODEL")
    env_temperature = os.environ.get("DATA_PIPELINE_LLM_TEMPERATURE") or dotenv_values.get("DATA_PIPELINE_LLM_TEMPERATURE")
    env_max_tokens = os.environ.get("DATA_PIPELINE_LLM_MAX_TOKENS") or dotenv_values.get("DATA_PIPELINE_LLM_MAX_TOKENS")
    env_timeout = os.environ.get("DATA_PIPELINE_LLM_TIMEOUT_SECONDS") or dotenv_values.get("DATA_PIPELINE_LLM_TIMEOUT_SECONDS")
    env_retry_count = os.environ.get("DATA_PIPELINE_LLM_RETRY_COUNT") or dotenv_values.get("DATA_PIPELINE_LLM_RETRY_COUNT")
    env_backoff = os.environ.get("DATA_PIPELINE_LLM_RETRY_BACKOFF_SECONDS") or dotenv_values.get("DATA_PIPELINE_LLM_RETRY_BACKOFF_SECONDS")
    env_provider = os.environ.get("DATA_PIPELINE_LLM_PROVIDER") or dotenv_values.get("DATA_PIPELINE_LLM_PROVIDER")
    env_base_url = (
        os.environ.get("DATA_PIPELINE_LLM_BASE_URL")
        or dotenv_values.get("DATA_PIPELINE_LLM_BASE_URL")
        or os.environ.get("OPENAI_BASE_URL")
        or dotenv_values.get("OPENAI_BASE_URL")
    )
    env_api_key = (
        os.environ.get("DATA_PIPELINE_LLM_API_KEY")
        or dotenv_values.get("DATA_PIPELINE_LLM_API_KEY")
        or os.environ.get(api_key_env_var)
        or dotenv_values.get(api_key_env_var)
        or os.environ.get("GEMINI_API_KEY")
        or dotenv_values.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or dotenv_values.get("GOOGLE_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or dotenv_values.get("OPENAI_API_KEY")
    )

    if env_provider:
        llm_settings["provider"] = env_provider
    if env_model:
        llm_settings["model"] = env_model
    llm_settings["temperature"] = _safe_float(env_temperature, _safe_float(llm_settings.get("temperature"), 0.1))
    llm_settings["max_output_tokens"] = _safe_int(env_max_tokens, _safe_int(llm_settings.get("max_output_tokens"), 4000))
    llm_settings["timeout_seconds"] = _safe_int(env_timeout, _safe_int(llm_settings.get("timeout_seconds"), 120))
    llm_settings["retry_count"] = _safe_int(env_retry_count, _safe_int(llm_settings.get("retry_count"), 2))
    llm_settings["retry_backoff_seconds"] = _safe_float(
        env_backoff,
        _safe_float(llm_settings.get("retry_backoff_seconds"), 2.0),
    )
    if env_base_url:
        llm_settings["api_base_url"] = env_base_url
    llm_settings["api_key"] = env_api_key or ""
    return llm_settings


def _slugify_category_name(value: str) -> str:
    return "_".join(part for part in normalize_text(value).split(" ") if part)


def _manual_state_exists(paths: PipelinePaths) -> bool:
    return paths.market_catalog_path.exists() and paths.category_registry_path.exists()


def _load_legacy_supported_assignments() -> tuple[dict[str, str], set[str]]:
    assignments: dict[str, str] = {}
    categories: set[str] = set()
    for artifact_path in sorted(ARTIFACT_ROOT.glob("*/market_metadata.json")):
        payload = _load_json_if_exists(artifact_path)
        if not payload:
            continue
        for record in payload.get("records", []):
            market_id = str(record.get("market_id") or "").strip()
            category_name = str(record.get("category") or "").strip()
            if not market_id or not category_name:
                continue
            assignments.setdefault(market_id, category_name)
            categories.add(category_name)
    return assignments, categories


def _default_registry_entry(
    category_name: str,
    *,
    status: str,
    source: str,
    created_at: str | None = None,
    evidence_run_ids: list[str] | None = None,
) -> dict[str, Any]:
    created_at = created_at or utc_now_iso()
    return {
        "category_name": category_name,
        "status": status,
        "app_enabled": status == "promoted",
        "source": source,
        "created_at": created_at,
        "updated_at": created_at,
        "market_count": 0,
        "example_market_ids": [],
        "average_confidence": None,
        "coherence_score": None,
        "stability_run_count": len(set(evidence_run_ids or [])),
        "evidence_run_ids": sorted(set(evidence_run_ids or [])),
    }


def _load_registry(paths: PipelinePaths) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    payload = _load_json_if_exists(paths.category_registry_path) or {"records": []}
    records = [dict(record) for record in payload.get("records", [])]
    return records, {str(record["category_name"]): record for record in records}


def _load_catalog(paths: PipelinePaths) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    payload = _load_json_if_exists(paths.market_catalog_path) or {"records": []}
    records = [dict(record) for record in payload.get("records", [])]
    return records, {str(record["market_id"]): record for record in records}


def _sync_registry_metrics(
    registry_records: list[dict[str, Any]],
    catalog_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    assignments_by_category: dict[str, list[dict[str, Any]]] = {}
    for record in catalog_records:
        category_name = record.get("assigned_category")
        if not category_name:
            continue
        assignments_by_category.setdefault(str(category_name), []).append(record)

    synced: list[dict[str, Any]] = []
    for record in sorted(registry_records, key=lambda item: item["category_name"]):
        category_records = assignments_by_category.get(record["category_name"], [])
        confidences = [float(item["assignment_confidence"]) for item in category_records if item.get("assignment_confidence") is not None]
        evidence_run_ids = sorted(
            {
                str(run_id)
                for run_id in record.get("evidence_run_ids", [])
                if run_id
            }
            | {
                str(item["last_assignment_batch_id"])
                for item in category_records
                if item.get("last_assignment_batch_id")
            }
        )
        synced_record = {
            **record,
            "app_enabled": record.get("status") == "promoted",
            "updated_at": utc_now_iso(),
            "market_count": len(category_records),
            "example_market_ids": [item["market_id"] for item in sorted(category_records, key=lambda item: item["market_id"])[:5]],
            "average_confidence": round(sum(confidences) / len(confidences), 4) if confidences else None,
            "stability_run_count": len(evidence_run_ids),
            "evidence_run_ids": evidence_run_ids,
        }
        synced.append(synced_record)
    return synced


def _write_categorization_state(
    *,
    paths: PipelinePaths,
    provider_name: str,
    catalog_records: list[dict[str, Any]],
    registry_records: list[dict[str, Any]],
    discovery_report: dict[str, Any] | None = None,
    llm_batch: dict[str, Any] | None = None,
    promotion_report: dict[str, Any] | None = None,
) -> None:
    ensure_dir(paths.categorization_dir)
    sorted_catalog = sorted(catalog_records, key=lambda item: item["market_id"])
    synced_registry = _sync_registry_metrics(registry_records, sorted_catalog)

    catalog_payload = build_json_envelope(
        artifact_name="market_catalog",
        provider_name=provider_name,
        schema_version=MARKET_CATALOG_SCHEMA_VERSION,
        record_key="records",
        records=sorted_catalog,
    )
    write_json(paths.market_catalog_path, catalog_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_catalog",
        relative_path=artifact_relative_path(paths, paths.market_catalog_path),
        schema_version=MARKET_CATALOG_SCHEMA_VERSION,
        record_count=len(sorted_catalog),
    )

    registry_payload = build_json_envelope(
        artifact_name="category_registry",
        provider_name=provider_name,
        schema_version=CATEGORY_REGISTRY_SCHEMA_VERSION,
        record_key="records",
        records=synced_registry,
    )
    write_json(paths.category_registry_path, registry_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="category_registry",
        relative_path=artifact_relative_path(paths, paths.category_registry_path),
        schema_version=CATEGORY_REGISTRY_SCHEMA_VERSION,
        record_count=len(synced_registry),
    )

    assignment_records = [
        {
            "market_id": record["market_id"],
            "title": record["title"],
            "assigned_category": record.get("assigned_category"),
            "assignment_status": record.get("assignment_status"),
            "app_enabled": bool(record.get("app_enabled")),
            "assignment_confidence": record.get("assignment_confidence"),
            "assignment_source": record.get("assignment_source"),
            "assignment_updated_at": record.get("assignment_updated_at"),
            "last_assignment_batch_id": record.get("last_assignment_batch_id"),
        }
        for record in sorted_catalog
    ]
    assignments_payload = build_json_envelope(
        artifact_name="market_assignments",
        provider_name=provider_name,
        schema_version=MARKET_ASSIGNMENTS_SCHEMA_VERSION,
        record_key="records",
        records=assignment_records,
    )
    write_json(paths.market_assignments_path, assignments_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_assignments",
        relative_path=artifact_relative_path(paths, paths.market_assignments_path),
        schema_version=MARKET_ASSIGNMENTS_SCHEMA_VERSION,
        record_count=len(assignment_records),
    )

    if discovery_report is not None:
        discovery_payload = build_json_envelope(
            artifact_name="discovery_report",
            provider_name=provider_name,
            schema_version=DISCOVERY_REPORT_SCHEMA_VERSION,
            record_key="records",
            records=[],
            extra={"summary": discovery_report},
        )
        write_json(paths.discovery_report_path, discovery_payload)
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="discovery_report",
            relative_path=artifact_relative_path(paths, paths.discovery_report_path),
            schema_version=DISCOVERY_REPORT_SCHEMA_VERSION,
        )

    if llm_batch is not None:
        write_json(paths.llm_batch_path, llm_batch)
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="pending_llm_categorization_batch",
            relative_path=artifact_relative_path(paths, paths.llm_batch_path),
            schema_version=LLM_BATCH_SCHEMA_VERSION,
            record_count=len(llm_batch.get("markets_to_categorize", [])),
        )

    if promotion_report is not None:
        promotion_payload = build_json_envelope(
            artifact_name="category_promotion_report",
            provider_name=provider_name,
            schema_version=PROMOTION_REPORT_SCHEMA_VERSION,
            record_key="records",
            records=promotion_report.get("evaluations", []),
            extra={"summary": promotion_report},
        )
        write_json(paths.promotion_report_path, promotion_payload)
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="category_promotion_report",
            relative_path=artifact_relative_path(paths, paths.promotion_report_path),
            schema_version=PROMOTION_REPORT_SCHEMA_VERSION,
            record_count=len(promotion_report.get("evaluations", [])),
        )


def _threshold_summary(
    *,
    new_market_count: int,
    unassigned_market_count: int,
    eligible_market_count: int,
    settings: dict[str, Any],
) -> dict[str, Any]:
    thresholds = settings["llm_thresholds"]
    new_met = new_market_count >= int(thresholds["min_new_markets"])
    unassigned_met = unassigned_market_count >= int(thresholds["min_unassigned_markets"])
    size_met = eligible_market_count >= int(thresholds["min_markets_to_prepare_batch"])
    threshold_met = size_met and (new_met or unassigned_met)
    return {
        "threshold_met": threshold_met,
        "eligible_market_count": eligible_market_count,
        "new_market_count": new_market_count,
        "unassigned_market_count": unassigned_market_count,
        "checks": {
            "new_market_threshold_met": new_met,
            "unassigned_market_threshold_met": unassigned_met,
            "batch_size_threshold_met": size_met,
        },
        "thresholds": thresholds,
    }


def _build_catalog_record(
    *,
    discovered: MarketMetadataRecord,
    existing: dict[str, Any] | None,
    legacy_assignment: str | None,
    promoted_categories: set[str],
    seen_at: str,
) -> tuple[dict[str, Any], bool]:
    created_at = existing.get("first_seen_at") if existing else seen_at
    assigned_category = existing.get("assigned_category") if existing else None
    assignment_status = existing.get("assignment_status") if existing else "unassigned"
    assignment_source = existing.get("assignment_source") if existing else None
    assignment_confidence = existing.get("assignment_confidence") if existing else None
    assignment_reason = existing.get("assignment_reason") if existing else None
    assignment_updated_at = existing.get("assignment_updated_at") if existing else None
    last_assignment_batch_id = existing.get("last_assignment_batch_id") if existing else None
    if existing is None and legacy_assignment:
        assigned_category = legacy_assignment
        assignment_status = "promoted"
        assignment_source = "legacy_supported_artifact"
        assignment_confidence = 1.0
        assignment_reason = "Seeded from existing app-facing preprocessing artifacts during migration."
        assignment_updated_at = seen_at
    app_enabled = bool(assigned_category) and assignment_status == "promoted" and assigned_category in promoted_categories
    catalog_record = {
        **discovered.to_dict(),
        "raw_provider_category": discovered.category,
        "first_seen_at": created_at,
        "last_seen_at": seen_at,
        "assignment_status": assignment_status,
        "assigned_category": assigned_category,
        "app_enabled": app_enabled,
        "assignment_source": assignment_source,
        "assignment_confidence": assignment_confidence,
        "assignment_reason": assignment_reason,
        "assignment_updated_at": assignment_updated_at,
        "last_assignment_batch_id": last_assignment_batch_id,
        "needs_categorization": not bool(assigned_category),
        "needs_promotion_review": assignment_status == "candidate",
    }
    return catalog_record, existing is None


def pull_markets(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    snapshot_dir: Path | None,
    discovery_mode: str,
    config_path: Path | None,
    dry_run: bool = False,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    ensure_dir(paths.artifacts_dir)
    persist_scope_artifact(path=paths.scope_artifact_path, provider_name=provider_name, scope_config=scope_config)
    provider = get_provider(provider_name, snapshot_dir=snapshot_dir, config_path=config_path)
    discovered = sorted(
        provider.fetch_market_metadata(scope_config=scope_config, discovery_mode=discovery_mode),
        key=lambda record: record.market_id,
    )
    if discovery_mode == "scoped" and provider_name != "kalshi_live":
        discovered, _ = select_scoped_markets(discovered, scope_config)
    settings = _load_workflow_settings(config_path)

    catalog_records, catalog_by_id = _load_catalog(paths)
    registry_records, registry_by_name = _load_registry(paths)
    legacy_assignments, legacy_categories = _load_legacy_supported_assignments()
    if not registry_records:
        for category_name in sorted(legacy_categories):
            registry_records.append(
                _default_registry_entry(category_name, status="promoted", source="legacy_supported_artifact")
            )
        registry_by_name = {record["category_name"]: record for record in registry_records}
    promoted_categories = {
        name
        for name, record in registry_by_name.items()
        if record.get("status") == "promoted"
    }

    seen_at = utc_now_iso()
    updated_catalog: list[dict[str, Any]] = []
    new_market_ids: list[str] = []
    seeded_market_ids: list[str] = []
    for record in discovered:
        updated_record, is_new = _build_catalog_record(
            discovered=record,
            existing=catalog_by_id.get(record.market_id),
            legacy_assignment=legacy_assignments.get(record.market_id),
            promoted_categories=promoted_categories,
            seen_at=seen_at,
        )
        if is_new:
            new_market_ids.append(record.market_id)
            if updated_record["app_enabled"]:
                seeded_market_ids.append(record.market_id)
        updated_catalog.append(updated_record)

    known_unassigned_market_ids = [
        record["market_id"]
        for record in updated_catalog
        if not record["assigned_category"] and record["market_id"] not in new_market_ids
    ]
    candidate_reevaluation_market_ids = [
        record["market_id"]
        for record in updated_catalog
        if record.get("assignment_status") == "candidate"
    ]
    eligible_market_count = len(
        [
            record
            for record in updated_catalog
            if not record.get("app_enabled")
            and (not record.get("assigned_category") or record.get("assignment_status") == "candidate")
        ]
    )
    threshold_info = _threshold_summary(
        new_market_count=len([market_id for market_id in new_market_ids if market_id not in seeded_market_ids]),
        unassigned_market_count=len(known_unassigned_market_ids),
        eligible_market_count=eligible_market_count,
        settings=settings,
    )
    summary = {
        "discovery_mode": discovery_mode,
        "discovered_market_count": len(updated_catalog),
        "new_market_count": len(new_market_ids),
        "new_market_ids": new_market_ids,
        "new_markets_seeded_from_legacy_support_count": len(seeded_market_ids),
        "known_unassigned_market_count": len(known_unassigned_market_ids),
        "known_unassigned_market_ids": known_unassigned_market_ids,
        "candidate_reevaluation_market_count": len(candidate_reevaluation_market_ids),
        "candidate_reevaluation_market_ids": candidate_reevaluation_market_ids,
        "llm_threshold_status": threshold_info,
        "notes": [
            "Newly discovered markets remain app-disabled by default unless they were seeded from legacy supported artifacts.",
            "Promoted categories remain app-enabled; candidate categories and unassigned markets stay out of app-facing use.",
        ],
    }
    if not dry_run:
        _write_categorization_state(
            paths=paths,
            provider_name=provider_name,
            catalog_records=updated_catalog,
            registry_records=registry_records,
            discovery_report=summary,
        )
    return paths.discovery_report_path, summary


def load_app_enabled_market_records(paths: PipelinePaths) -> list[MarketMetadataRecord]:
    registry_payload = _load_json_if_exists(paths.category_registry_path)
    catalog_payload = _load_json_if_exists(paths.market_catalog_path)
    if not registry_payload or not catalog_payload:
        return []
    promoted_categories = {
        str(record["category_name"])
        for record in registry_payload.get("records", [])
        if record.get("status") == "promoted"
    }
    records: list[MarketMetadataRecord] = []
    for item in catalog_payload.get("records", []):
        assigned_category = item.get("assigned_category")
        if not item.get("app_enabled") or assigned_category not in promoted_categories:
            continue
        families = [str(value) for value in item.get("families", []) if value]
        if assigned_category and assigned_category not in families:
            families = [assigned_category, *families]
        payload = {
            **item,
            "category": assigned_category,
            "families": families,
        }
        records.append(MarketMetadataRecord.from_mapping(payload))
    return sorted(records, key=lambda record: record.market_id)


def prepare_llm_categorization_batch(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    config_path: Path | None,
    dry_run: bool = False,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    settings = _load_workflow_settings(config_path)
    catalog_payload = _load_json_if_exists(paths.market_catalog_path)
    registry_payload = _load_json_if_exists(paths.category_registry_path)
    if not catalog_payload or not registry_payload:
        raise FileNotFoundError("manual categorization state is missing; run pull_markets first")

    catalog_records = [dict(record) for record in catalog_payload.get("records", [])]
    registry_records = [dict(record) for record in registry_payload.get("records", [])]
    discovery_summary = (_load_json_if_exists(paths.discovery_report_path) or {}).get("summary", {})
    new_market_count = int(
        discovery_summary.get("llm_threshold_status", {}).get("new_market_count", discovery_summary.get("new_market_count", 0))
    )
    eligible_records = [
        {
            "market_id": record["market_id"],
            "title": record["title"],
            "question": record["question"],
            "raw_provider_category": record.get("raw_provider_category"),
            "families": record.get("families", []),
            "tags": record.get("tags", []),
            "assignment_status": record.get("assignment_status"),
        }
        for record in catalog_records
        if not record.get("app_enabled")
        and (not record.get("assigned_category") or record.get("assignment_status") == "candidate")
    ]
    known_unassigned_count = len([record for record in catalog_records if not record.get("assigned_category")])
    threshold_info = _threshold_summary(
        new_market_count=new_market_count,
        unassigned_market_count=known_unassigned_count,
        eligible_market_count=len(eligible_records),
        settings=settings,
    )
    batch_id = f"{scope_config.scope_slug}-{utc_now_iso().replace(':', '').replace('-', '')}"
    batch_payload = {
        "artifact": "pending_llm_categorization_batch",
        "schema_version": LLM_BATCH_SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "batch_id": batch_id,
        "scope": scope_config.to_dict(),
        "workflow_settings": settings,
        "threshold_status": threshold_info,
        "existing_promoted_categories": [
            {
                "category_name": record["category_name"],
                "market_count": record.get("market_count"),
                "example_market_ids": record.get("example_market_ids", []),
            }
            for record in registry_records
            if record.get("status") == "promoted"
        ],
        "existing_candidate_categories": [
            {
                "category_name": record["category_name"],
                "market_count": record.get("market_count"),
                "example_market_ids": record.get("example_market_ids", []),
            }
            for record in registry_records
            if record.get("status") == "candidate"
        ],
        "markets_to_categorize": eligible_records,
        "response_format": {
            "add_to_existing_categories": [
                {
                    "category_name": "existing_category_name",
                    "market_ids": ["market_a", "market_b"],
                    "confidence": 0.93,
                    "reason": "short explanation",
                }
            ],
            "propose_new_categories": [
                {
                    "category_name": "candidate_category_name",
                    "market_ids": ["market_x", "market_y", "market_z"],
                    "confidence": 0.88,
                    "reason": "short explanation",
                }
            ],
            "leave_unassigned": [
                {
                    "market_ids": ["market_q"],
                    "reason": "short explanation",
                }
            ],
        },
        "status": "ready" if threshold_info["threshold_met"] else "skipped_threshold_not_met",
    }
    prompt_text = "\n".join(
        [
            "Categorize the provided prediction markets into existing promoted categories, existing candidate categories, or new candidate categories.",
            "Return JSON only. Do not include markdown fences or prose.",
            "Every market_id in markets_to_categorize must appear exactly once in one of the three top-level arrays.",
            "Only use category names that already exist in the registry for add_to_existing_categories.",
            "Any category proposed in propose_new_categories must be a lowercase snake_case identifier and should group multiple closely related markets.",
            "Newly proposed categories remain candidate categories only; do not assume app promotion.",
            "Use concise reasons and numeric confidence values between 0 and 1.",
            "",
            json.dumps(batch_payload["response_format"], indent=2),
        ]
    )
    if not dry_run:
        ensure_dir(paths.categorization_dir)
        if threshold_info["threshold_met"]:
            paths.llm_prompt_path.write_text(prompt_text, encoding="utf-8")
        _write_categorization_state(
            paths=paths,
            provider_name=provider_name,
            catalog_records=catalog_records,
            registry_records=registry_records,
            llm_batch=batch_payload,
        )
    return paths.llm_batch_path, batch_payload


def _llm_response_json_schema() -> dict[str, Any]:
    assignment_item = {
        "type": "object",
        "properties": {
            "category_name": {"type": "string", "minLength": 1},
            "market_ids": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "minItems": 1,
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reason": {"type": "string", "minLength": 1},
        },
        "required": ["category_name", "market_ids", "confidence", "reason"],
        "additionalProperties": False,
    }
    leave_unassigned_item = {
        "type": "object",
        "properties": {
            "market_ids": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "minItems": 1,
            },
            "reason": {"type": "string", "minLength": 1},
        },
        "required": ["market_ids", "reason"],
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {
            "add_to_existing_categories": {
                "type": "array",
                "items": assignment_item,
            },
            "propose_new_categories": {
                "type": "array",
                "items": assignment_item,
            },
            "leave_unassigned": {
                "type": "array",
                "items": leave_unassigned_item,
            },
        },
        "required": [
            "add_to_existing_categories",
            "propose_new_categories",
            "leave_unassigned",
        ],
        "additionalProperties": False,
    }


def _extract_text_from_response_payload(response_payload: dict[str, Any]) -> str:
    if isinstance(response_payload.get("output_text"), str) and response_payload["output_text"].strip():
        return str(response_payload["output_text"]).strip()

    if isinstance(response_payload.get("text"), str) and response_payload["text"].strip():
        return str(response_payload["text"]).strip()

    candidates = response_payload.get("candidates")
    if isinstance(candidates, list):
        collected_candidates: list[str] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            content = candidate.get("content") or {}
            if not isinstance(content, dict):
                continue
            for part in content.get("parts", []):
                if not isinstance(part, dict):
                    continue
                if isinstance(part.get("text"), str) and part["text"].strip():
                    collected_candidates.append(part["text"].strip())
        if collected_candidates:
            return "\n".join(collected_candidates)

    collected: list[str] = []
    for item in response_payload.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            if isinstance(content.get("text"), str) and content["text"].strip():
                collected.append(content["text"].strip())
            elif isinstance(content.get("json"), dict):
                collected.append(json.dumps(content["json"], indent=2))
    return "\n".join(part for part in collected if part).strip()


def _basic_validate_llm_response_shape(response_payload: dict[str, Any]) -> dict[str, Any]:
    required_keys = {"add_to_existing_categories", "propose_new_categories", "leave_unassigned"}
    if set(response_payload.keys()) != required_keys:
        raise ValueError(f"parsed JSON must contain exactly these keys: {sorted(required_keys)}")
    for key in required_keys:
        if not isinstance(response_payload[key], list):
            raise ValueError(f"{key} must be an array")
    return response_payload


def _categorization_input_fingerprint(batch_payload: dict[str, Any]) -> str:
    fingerprint_payload = {
        "existing_promoted_categories": [
            record.get("category_name")
            for record in batch_payload.get("existing_promoted_categories", [])
        ],
        "existing_candidate_categories": [
            record.get("category_name")
            for record in batch_payload.get("existing_candidate_categories", [])
        ],
        "markets_to_categorize": [
            {
                "market_id": record.get("market_id"),
                "assignment_status": record.get("assignment_status"),
            }
            for record in batch_payload.get("markets_to_categorize", [])
        ],
    }
    return json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"))


def _load_scheduler_state(paths: PipelinePaths) -> dict[str, Any]:
    payload = _load_json_if_exists(paths.scheduler_state_path)
    return dict(payload.get("state", {})) if payload else {}


def _write_scheduler_state(paths: PipelinePaths, provider_name: str, state: dict[str, Any]) -> None:
    ensure_dir(paths.scheduler_state_path.parent)
    write_json(
        paths.scheduler_state_path,
        build_json_envelope(
            artifact_name="scheduler_state",
            provider_name=provider_name,
            schema_version=SCHEDULER_STATE_SCHEMA_VERSION,
            record_key="records",
            records=[],
            extra={"state": state},
        ),
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="scheduler_state",
        relative_path=artifact_relative_path(paths, paths.scheduler_state_path),
        schema_version=SCHEDULER_STATE_SCHEMA_VERSION,
    )


def _write_scheduler_cycle_summary(
    *,
    paths: PipelinePaths,
    provider_name: str,
    cycle_summary: dict[str, Any],
) -> Path:
    ensure_dir(paths.scheduler_runs_dir)
    cycle_id = cycle_summary["cycle_id"]
    output_path = paths.scheduler_runs_dir / f"{cycle_id}.json"
    write_json(
        output_path,
        build_json_envelope(
            artifact_name="scheduler_cycle",
            provider_name=provider_name,
            schema_version=SCHEDULER_CYCLE_SCHEMA_VERSION,
            record_key="records",
            records=[],
            extra={"summary": cycle_summary},
        ),
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="latest_scheduler_cycle",
        relative_path=artifact_relative_path(paths, output_path),
        schema_version=SCHEDULER_CYCLE_SCHEMA_VERSION,
    )
    return output_path


def _scheduler_log(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}")


def _scheduler_sleep(seconds: float, *, jitter_seconds: float = 0.0) -> float:
    total_seconds = max(0.0, seconds)
    if jitter_seconds > 0:
        total_seconds += random.uniform(0.0, max(0.0, jitter_seconds))
    if total_seconds > 0:
        time.sleep(total_seconds)
    return round(total_seconds, 3)


def _acquire_scheduler_lock(paths: PipelinePaths, *, provider_name: str) -> dict[str, Any]:
    ensure_dir(paths.scheduler_lock_path.parent)
    current_pid = os.getpid()
    lock_payload = {
        "pid": current_pid,
        "provider_name": provider_name,
        "created_at": utc_now_iso(),
    }
    if paths.scheduler_lock_path.exists():
        existing = _load_json_if_exists(paths.scheduler_lock_path)
        existing_pid = existing.get("pid") if existing else None
        if isinstance(existing_pid, int):
            try:
                os.kill(existing_pid, 0)
            except OSError:
                pass
            else:
                raise RuntimeError(
                    f"scheduler lock already held by pid {existing_pid} at {paths.scheduler_lock_path}"
                )
    write_json(paths.scheduler_lock_path, lock_payload)
    return lock_payload


def _release_scheduler_lock(paths: PipelinePaths) -> None:
    if paths.scheduler_lock_path.exists():
        try:
            paths.scheduler_lock_path.unlink()
        except OSError:
            pass


def _openai_responses_request(
    *,
    llm_settings: dict[str, Any],
    prompt_text: str,
    batch_payload: dict[str, Any],
) -> dict[str, Any]:
    request_payload = {
        "model": llm_settings["model"],
        "instructions": (
            f"{prompt_text}\n\n"
            "You must respond with a JSON object that matches the provided schema exactly."
        ),
        "input": json.dumps(
            {
                "batch_id": batch_payload["batch_id"],
                "threshold_status": batch_payload["threshold_status"],
                "existing_promoted_categories": batch_payload["existing_promoted_categories"],
                "existing_candidate_categories": batch_payload["existing_candidate_categories"],
                "markets_to_categorize": batch_payload["markets_to_categorize"],
            },
            indent=2,
        ),
        "temperature": llm_settings["temperature"],
        "max_output_tokens": llm_settings["max_output_tokens"],
        "store": False,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "market_categorization",
                "strict": True,
                "schema": _llm_response_json_schema(),
            }
        },
    }
    return request_payload


def _google_generate_content_request(
    *,
    llm_settings: dict[str, Any],
    prompt_text: str,
    batch_payload: dict[str, Any],
) -> dict[str, Any]:
    payload_text = json.dumps(
        {
            "batch_id": batch_payload["batch_id"],
            "threshold_status": batch_payload["threshold_status"],
            "existing_promoted_categories": batch_payload["existing_promoted_categories"],
            "existing_candidate_categories": batch_payload["existing_candidate_categories"],
            "markets_to_categorize": batch_payload["markets_to_categorize"],
        },
        indent=2,
    )
    return {
        "contents": [
            {
                "parts": [
                    {
                        "text": (
                            f"{prompt_text}\n\n"
                            "Return JSON only.\n\n"
                            "Batch payload:\n"
                            f"{payload_text}"
                        )
                    }
                ]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseJsonSchema": _llm_response_json_schema(),
            "temperature": llm_settings["temperature"],
            "maxOutputTokens": llm_settings["max_output_tokens"],
        },
    }


def _request_openai_response(
    *,
    llm_settings: dict[str, Any],
    request_payload: dict[str, Any],
    client_request_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    api_key = str(llm_settings.get("api_key") or "").strip()
    if not api_key:
        raise ValueError(
            "Missing OpenAI API key. Set OPENAI_API_KEY in the environment or data_pipeline/.env before running."
        )

    url = f"{str(llm_settings['api_base_url']).rstrip('/')}/responses"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Client-Request-Id": client_request_id,
    }

    last_error_message = ""
    last_status_code: int | None = None
    retry_count = max(0, int(llm_settings["retry_count"]))
    backoff_seconds = max(0.0, float(llm_settings["retry_backoff_seconds"]))
    timeout_seconds = max(1, int(llm_settings["timeout_seconds"]))
    mutable_request_payload = dict(request_payload)

    for attempt in range(retry_count + 1):
        body = json.dumps(mutable_request_payload).encode("utf-8")
        request = urllib_request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
                response_json = json.loads(raw_body)
                return response_json, {
                    "http_status": int(response.status),
                    "request_id": response.headers.get("x-request-id"),
                    "client_request_id": client_request_id,
                }
        except urllib_error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            last_status_code = int(exc.code)
            last_error_message = error_body or str(exc)
            if exc.code == 400 and "Unsupported parameter: 'temperature'" in last_error_message and "temperature" in mutable_request_payload:
                mutable_request_payload.pop("temperature", None)
                if attempt < retry_count:
                    continue
            if exc.code in {408, 409, 429, 500, 502, 503, 504} and attempt < retry_count:
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            raise RuntimeError(f"OpenAI request failed with HTTP {exc.code}: {last_error_message}") from exc
        except urllib_error.URLError as exc:
            last_error_message = str(exc.reason)
            if attempt < retry_count:
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            raise RuntimeError(f"OpenAI request failed: {last_error_message}") from exc

    raise RuntimeError(
        f"OpenAI request failed after retries; last_status_code={last_status_code}, last_error={last_error_message}"
    )


def _request_google_response(
    *,
    llm_settings: dict[str, Any],
    request_payload: dict[str, Any],
    client_request_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    api_key = str(llm_settings.get("api_key") or "").strip()
    if not api_key:
        raise ValueError(
            "Missing Gemini API key. Set GEMINI_API_KEY, GOOGLE_API_KEY, or data_pipeline/.env before running."
        )

    model_name = str(llm_settings["model"]).strip()
    if not model_name:
        raise ValueError("Missing llm_execution.model for Google provider")

    base_url = str(llm_settings["api_base_url"]).rstrip("/")
    url = f"{base_url}/models/{model_name}:generateContent?key={api_key}"
    headers = {
        "Content-Type": "application/json",
        "X-Client-Request-Id": client_request_id,
    }

    last_error_message = ""
    last_status_code: int | None = None
    retry_count = max(0, int(llm_settings["retry_count"]))
    backoff_seconds = max(0.0, float(llm_settings["retry_backoff_seconds"]))
    timeout_seconds = max(1, int(llm_settings["timeout_seconds"]))
    mutable_request_payload = json.loads(json.dumps(request_payload))

    unsupported_param_pairs = [
        ("generationConfig", "temperature"),
        ("generationConfig", "maxOutputTokens"),
    ]

    for attempt in range(retry_count + 1):
        body = json.dumps(mutable_request_payload).encode("utf-8")
        request = urllib_request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
                response_json = json.loads(raw_body)
                return response_json, {
                    "http_status": int(response.status),
                    "request_id": response.headers.get("x-request-id"),
                    "client_request_id": client_request_id,
                }
        except urllib_error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            last_status_code = int(exc.code)
            last_error_message = error_body or str(exc)
            if exc.code == 400:
                removed = False
                for parent_key, child_key in unsupported_param_pairs:
                    if f"'{child_key}'" in last_error_message and child_key in mutable_request_payload.get(parent_key, {}):
                        mutable_request_payload[parent_key].pop(child_key, None)
                        removed = True
                if removed and attempt < retry_count:
                    continue
            if exc.code in {408, 409, 429, 500, 502, 503, 504} and attempt < retry_count:
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            raise RuntimeError(f"Google Gemini request failed with HTTP {exc.code}: {last_error_message}") from exc
        except urllib_error.URLError as exc:
            last_error_message = str(exc.reason)
            if attempt < retry_count:
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            raise RuntimeError(f"Google Gemini request failed: {last_error_message}") from exc

    raise RuntimeError(
        f"Google Gemini request failed after retries; last_status_code={last_status_code}, last_error={last_error_message}"
    )


def run_llm_categorization(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    config_path: Path | None,
    dry_run: bool = False,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    batch_payload = _load_json_if_exists(paths.llm_batch_path)
    if not batch_payload:
        raise FileNotFoundError("no prepared LLM categorization batch found; run prepare_llm_categorization_batch first")
    if batch_payload.get("status") != "ready":
        raise ValueError(
            f"pending batch is not ready for execution (status={batch_payload.get('status')}); prepare a ready batch first"
        )
    if not paths.llm_prompt_path.exists():
        raise FileNotFoundError("prepared LLM prompt file is missing; rerun prepare_llm_categorization_batch")

    llm_settings = _resolve_llm_execution_settings(config_path)
    provider = str(llm_settings.get("provider") or "google").strip().lower()
    if provider not in {"google", "openai"}:
        raise ValueError(f"Unsupported llm_execution.provider={llm_settings.get('provider')}; supported values are google and openai")
    batch_id = str(batch_payload["batch_id"])
    run_id = f"{batch_id}-run-{utc_now_iso().replace(':', '').replace('-', '')}"
    run_dir = paths.llm_run_dir / run_id
    ensure_dir(run_dir)

    prompt_text = paths.llm_prompt_path.read_text(encoding="utf-8")
    if provider == "google":
        request_payload = _google_generate_content_request(
            llm_settings=llm_settings,
            prompt_text=prompt_text,
            batch_payload=batch_payload,
        )
    else:
        request_payload = _openai_responses_request(
            llm_settings=llm_settings,
            prompt_text=prompt_text,
            batch_payload=batch_payload,
        )

    batch_snapshot_path = run_dir / "batch_snapshot.json"
    prompt_snapshot_path = run_dir / "prompt.txt"
    request_payload_path = run_dir / "request_payload.json"
    response_raw_path = run_dir / "response_raw.json"
    response_text_path = run_dir / "response_text.txt"
    parsed_response_path = run_dir / "response_parsed.json"
    run_summary_path = run_dir / "run_summary.json"

    write_json(batch_snapshot_path, batch_payload)
    prompt_snapshot_path.write_text(prompt_text, encoding="utf-8")
    write_json(request_payload_path, request_payload)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "batch_id": batch_id,
        "market_count": len(batch_payload.get("markets_to_categorize", [])),
        "provider": provider,
        "model": llm_settings["model"],
        "temperature": llm_settings["temperature"],
        "max_output_tokens": llm_settings["max_output_tokens"],
        "request_payload_path": str(request_payload_path),
        "prompt_snapshot_path": str(prompt_snapshot_path),
        "batch_snapshot_path": str(batch_snapshot_path),
        "raw_response_path": None,
        "response_text_path": None,
        "parsed_response_path": None,
        "parsed_successfully": False,
        "validation_error": None,
        "request_error": None,
        "dry_run": dry_run,
    }

    if dry_run:
        summary["notes"] = [
            "Dry run wrote the request payload and prompt snapshot without sending an API request.",
        ]
        write_json(
            run_summary_path,
            build_json_envelope(
                artifact_name="llm_categorization_run",
                provider_name=provider_name,
                schema_version=LLM_RUN_SCHEMA_VERSION,
                record_key="records",
                records=[],
                extra={"summary": summary},
            ),
        )
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="latest_llm_categorization_run",
            relative_path=artifact_relative_path(paths, run_summary_path),
            schema_version=LLM_RUN_SCHEMA_VERSION,
        )
        return run_summary_path, summary

    client_request_id = run_id
    try:
        if provider == "google":
            response_payload, response_meta = _request_google_response(
                llm_settings=llm_settings,
                request_payload=request_payload,
                client_request_id=client_request_id,
            )
        else:
            response_payload, response_meta = _request_openai_response(
                llm_settings=llm_settings,
                request_payload=request_payload,
                client_request_id=client_request_id,
            )
    except (ValueError, RuntimeError) as exc:
        summary["request_error"] = str(exc)
        write_json(
            run_summary_path,
            build_json_envelope(
                artifact_name="llm_categorization_run",
                provider_name=provider_name,
                schema_version=LLM_RUN_SCHEMA_VERSION,
                record_key="records",
                records=[],
                extra={"summary": summary},
            ),
        )
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="latest_llm_categorization_run",
            relative_path=artifact_relative_path(paths, run_summary_path),
            schema_version=LLM_RUN_SCHEMA_VERSION,
        )
        raise
    write_json(response_raw_path, response_payload)
    summary["raw_response_path"] = str(response_raw_path)
    summary["response_meta"] = response_meta

    response_text = _extract_text_from_response_payload(response_payload)
    if response_text:
        response_text_path.write_text(response_text, encoding="utf-8")
        summary["response_text_path"] = str(response_text_path)

    try:
        parsed_payload = json.loads(response_text)
        _basic_validate_llm_response_shape(parsed_payload)
        write_json(parsed_response_path, parsed_payload)
        summary["parsed_response_path"] = str(parsed_response_path)
        summary["parsed_successfully"] = True
    except (json.JSONDecodeError, ValueError) as exc:
        summary["validation_error"] = str(exc)

    write_json(
        run_summary_path,
        build_json_envelope(
            artifact_name="llm_categorization_run",
            provider_name=provider_name,
            schema_version=LLM_RUN_SCHEMA_VERSION,
            record_key="records",
            records=[],
            extra={"summary": summary},
        ),
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="latest_llm_categorization_run",
        relative_path=artifact_relative_path(paths, run_summary_path),
        schema_version=LLM_RUN_SCHEMA_VERSION,
    )
    return run_summary_path, summary


def _validate_llm_response(
    *,
    batch_payload: dict[str, Any],
    response_payload: dict[str, Any],
    registry_by_name: dict[str, dict[str, Any]],
    known_market_ids: set[str],
) -> dict[str, Any]:
    required_keys = {"add_to_existing_categories", "propose_new_categories", "leave_unassigned"}
    if set(response_payload.keys()) != required_keys:
        raise ValueError(f"LLM response must contain exactly these keys: {sorted(required_keys)}")

    batch_market_ids = {item["market_id"] for item in batch_payload.get("markets_to_categorize", [])}
    seen_market_ids: set[str] = set()
    thresholds = batch_payload["workflow_settings"]["llm_thresholds"]

    def _record_market_ids(values: list[str], *, context: str) -> list[str]:
        normalized = [str(value) for value in values]
        for market_id in normalized:
            if market_id not in known_market_ids:
                raise ValueError(f"{context} references unknown market_id {market_id}")
            if market_id not in batch_market_ids:
                raise ValueError(f"{context} references market_id {market_id} that is not in the pending batch")
            if market_id in seen_market_ids:
                raise ValueError(f"market_id {market_id} appears multiple times in the LLM response")
            seen_market_ids.add(market_id)
        return normalized

    for item in response_payload["add_to_existing_categories"]:
        category_name = str(item.get("category_name") or "")
        if category_name not in registry_by_name:
            raise ValueError(f"unknown existing category {category_name}")
        confidence = item.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0.0 <= float(confidence) <= 1.0:
            raise ValueError(f"invalid confidence for existing category assignment {category_name}")
        reason = str(item.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"missing reason for existing category assignment {category_name}")
        _record_market_ids(list(item.get("market_ids") or []), context=f"add_to_existing_categories[{category_name}]")

    proposed_names: set[str] = set()
    for item in response_payload["propose_new_categories"]:
        category_name = str(item.get("category_name") or "")
        if not category_name or _slugify_category_name(category_name) != category_name:
            raise ValueError(f"proposed category name must be lowercase snake_case: {category_name}")
        if category_name in registry_by_name or category_name in proposed_names:
            raise ValueError(f"proposed category {category_name} already exists or is duplicated")
        proposed_names.add(category_name)
        market_ids = _record_market_ids(list(item.get("market_ids") or []), context=f"propose_new_categories[{category_name}]")
        if len(market_ids) < int(thresholds["min_markets_per_new_category"]):
            raise ValueError(
                f"proposed category {category_name} must include at least {thresholds['min_markets_per_new_category']} markets"
            )
        confidence = item.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0.0 <= float(confidence) <= 1.0:
            raise ValueError(f"invalid confidence for proposed category {category_name}")
        reason = str(item.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"missing reason for proposed category {category_name}")

    for item in response_payload["leave_unassigned"]:
        market_ids = _record_market_ids(list(item.get("market_ids") or []), context="leave_unassigned")
        if not market_ids:
            raise ValueError("leave_unassigned entries must include at least one market_id")
        reason = str(item.get("reason") or "").strip()
        if not reason:
            raise ValueError("leave_unassigned entries must include a reason")

    missing = sorted(batch_market_ids - seen_market_ids)
    if missing:
        raise ValueError(f"LLM response did not classify every pending market; missing {missing}")
    return response_payload


def apply_llm_categorization(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    response_path: Path,
    dry_run: bool = False,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    batch_payload = _load_json_if_exists(paths.llm_batch_path)
    if not batch_payload or batch_payload.get("status") != "ready":
        raise FileNotFoundError("no ready pending LLM categorization batch found")
    response_payload = read_json(response_path)
    catalog_records, catalog_by_id = _load_catalog(paths)
    registry_records, registry_by_name = _load_registry(paths)
    validated = _validate_llm_response(
        batch_payload=batch_payload,
        response_payload=response_payload,
        registry_by_name=registry_by_name,
        known_market_ids=set(catalog_by_id.keys()),
    )

    batch_id = str(batch_payload["batch_id"])
    applied_at = utc_now_iso()
    proposed_category_count = 0
    existing_assignment_count = 0

    for item in validated["add_to_existing_categories"]:
        category_name = item["category_name"]
        category_status = registry_by_name[category_name]["status"]
        for market_id in item["market_ids"]:
            record = catalog_by_id[market_id]
            record["assigned_category"] = category_name
            record["assignment_status"] = category_status
            record["app_enabled"] = category_status == "promoted"
            record["assignment_source"] = "llm_existing_category"
            record["assignment_confidence"] = round(float(item["confidence"]), 4)
            record["assignment_reason"] = item["reason"]
            record["assignment_updated_at"] = applied_at
            record["last_assignment_batch_id"] = batch_id
            record["needs_categorization"] = False
            record["needs_promotion_review"] = category_status == "candidate"
            existing_assignment_count += 1

    for item in validated["propose_new_categories"]:
        category_name = item["category_name"]
        registry_entry = _default_registry_entry(
            category_name,
            status="candidate",
            source="llm_proposed_category",
            created_at=applied_at,
            evidence_run_ids=[batch_id],
        )
        registry_records.append(registry_entry)
        registry_by_name[category_name] = registry_entry
        for market_id in item["market_ids"]:
            record = catalog_by_id[market_id]
            record["assigned_category"] = category_name
            record["assignment_status"] = "candidate"
            record["app_enabled"] = False
            record["assignment_source"] = "llm_proposed_category"
            record["assignment_confidence"] = round(float(item["confidence"]), 4)
            record["assignment_reason"] = item["reason"]
            record["assignment_updated_at"] = applied_at
            record["last_assignment_batch_id"] = batch_id
            record["needs_categorization"] = False
            record["needs_promotion_review"] = True
            proposed_category_count += 1

    for item in validated["leave_unassigned"]:
        for market_id in item["market_ids"]:
            record = catalog_by_id[market_id]
            record["assigned_category"] = None
            record["assignment_status"] = "unassigned"
            record["app_enabled"] = False
            record["assignment_source"] = "llm_leave_unassigned"
            record["assignment_confidence"] = None
            record["assignment_reason"] = item["reason"]
            record["assignment_updated_at"] = applied_at
            record["last_assignment_batch_id"] = batch_id
            record["needs_categorization"] = True
            record["needs_promotion_review"] = False

    decision_summary = {
        "batch_id": batch_id,
        "applied_at": applied_at,
        "response_source_path": str(response_path),
        "existing_category_assignment_count": existing_assignment_count,
        "proposed_candidate_category_count": len(validated["propose_new_categories"]),
        "proposed_candidate_category_market_count": proposed_category_count,
        "left_unassigned_count": sum(len(item["market_ids"]) for item in validated["leave_unassigned"]),
        "dry_run": dry_run,
    }

    response_archive_path = paths.llm_response_dir / f"{batch_id}.raw_response.json"
    decision_archive_path = paths.llm_application_dir / f"{batch_id}.application.json"
    if not dry_run:
        ensure_dir(paths.llm_response_dir)
        ensure_dir(paths.llm_application_dir)
        write_json(response_archive_path, response_payload)
        write_json(
            decision_archive_path,
            build_json_envelope(
                artifact_name="llm_categorization_application",
                provider_name=provider_name,
                schema_version=LLM_APPLICATION_SCHEMA_VERSION,
                record_key="records",
                records=[],
                extra={"summary": decision_summary, "validated_response": validated},
            ),
        )
        batch_payload["status"] = "applied"
        batch_payload["applied_at"] = applied_at
        batch_payload["response_archive_path"] = str(response_archive_path)
        _write_categorization_state(
            paths=paths,
            provider_name=provider_name,
            catalog_records=list(catalog_by_id.values()),
            registry_records=registry_records,
            llm_batch=batch_payload,
        )
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="latest_llm_categorization_application",
            relative_path=artifact_relative_path(paths, decision_archive_path),
            schema_version=LLM_APPLICATION_SCHEMA_VERSION,
        )
    return decision_archive_path, decision_summary


def _category_coherence(category_records: list[dict[str, Any]]) -> float | None:
    if len(category_records) < 2:
        return None
    scores: list[float] = []
    for index, left in enumerate(category_records):
        for right in category_records[index + 1 :]:
            scores.append(
                semantic_similarity(
                    " ".join([left.get("title", ""), left.get("question", ""), left.get("raw_provider_category", "")]),
                    " ".join([right.get("title", ""), right.get("question", ""), right.get("raw_provider_category", "")]),
                )
            )
    if not scores:
        return None
    return round(sum(scores) / len(scores), 4)


def evaluate_category_promotions(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    config_path: Path | None,
    promote: bool = False,
    dry_run: bool = False,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    settings = _load_workflow_settings(config_path)
    thresholds = settings["promotion_thresholds"]
    catalog_records, catalog_by_id = _load_catalog(paths)
    registry_records, registry_by_name = _load_registry(paths)
    evaluations: list[dict[str, Any]] = []
    promoted_category_names: list[str] = []

    for category_name, registry_record in sorted(registry_by_name.items()):
        if registry_record.get("status") != "candidate":
            continue
        category_market_records = [
            record
            for record in catalog_records
            if record.get("assigned_category") == category_name
        ]
        confidence_values = [
            float(record["assignment_confidence"])
            for record in category_market_records
            if record.get("assignment_confidence") is not None
        ]
        average_confidence = round(sum(confidence_values) / len(confidence_values), 4) if confidence_values else None
        coherence_score = _category_coherence(category_market_records)
        stability_run_count = len(set(registry_record.get("evidence_run_ids", [])))
        qualifies = (
            len(category_market_records) >= int(thresholds["min_market_count"])
            and average_confidence is not None
            and average_confidence >= float(thresholds["min_average_confidence"])
            and coherence_score is not None
            and coherence_score >= float(thresholds["min_coherence"])
            and stability_run_count >= int(thresholds["min_stability_runs"])
        )
        evaluations.append(
            {
                "category_name": category_name,
                "market_count": len(category_market_records),
                "average_confidence": average_confidence,
                "coherence_score": coherence_score,
                "stability_run_count": stability_run_count,
                "qualifies_for_promotion": qualifies,
                "example_market_ids": [record["market_id"] for record in sorted(category_market_records, key=lambda item: item["market_id"])[:5]],
            }
        )
        if promote and qualifies:
            registry_record["status"] = "promoted"
            registry_record["app_enabled"] = True
            registry_record["updated_at"] = utc_now_iso()
            promoted_category_names.append(category_name)
            for market_record in category_market_records:
                market_record["assignment_status"] = "promoted"
                market_record["app_enabled"] = True
                market_record["needs_promotion_review"] = False

    summary = {
        "evaluated_at": utc_now_iso(),
        "promotion_thresholds": thresholds,
        "evaluations": evaluations,
        "promoted_category_names": promoted_category_names,
        "dry_run": dry_run,
    }
    if not dry_run:
        _write_categorization_state(
            paths=paths,
            provider_name=provider_name,
            catalog_records=list(catalog_by_id.values()),
            registry_records=list(registry_by_name.values()),
            promotion_report=summary,
        )
    return paths.promotion_report_path, summary


def run_scheduler(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    config_path: Path | None,
    snapshot_dir: Path | None = None,
    discovery_mode: str | None = None,
    auto_apply: bool = False,
    auto_evaluate_promotions: bool = False,
    auto_promote: bool = False,
    once: bool = False,
    dry_run: bool = False,
    pull_interval_seconds: int | None = None,
    success_cooldown_seconds: int | None = None,
    failure_cooldown_seconds: int | None = None,
    sleep_jitter_seconds: float | None = None,
    max_consecutive_failures: int | None = None,
) -> tuple[Path, dict[str, Any]]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    workflow_settings = _load_workflow_settings(config_path)
    scheduler_settings = dict(workflow_settings["scheduler"])
    active_discovery_mode = discovery_mode or workflow_settings["discovery_mode"]
    interval_seconds = int(pull_interval_seconds if pull_interval_seconds is not None else scheduler_settings["pull_interval_seconds"])
    success_cooldown = int(
        success_cooldown_seconds if success_cooldown_seconds is not None else scheduler_settings["success_cooldown_seconds"]
    )
    failure_cooldown = int(
        failure_cooldown_seconds if failure_cooldown_seconds is not None else scheduler_settings["failure_cooldown_seconds"]
    )
    jitter_seconds = float(
        sleep_jitter_seconds if sleep_jitter_seconds is not None else scheduler_settings["sleep_jitter_seconds"]
    )
    max_failures = int(
        max_consecutive_failures if max_consecutive_failures is not None else scheduler_settings["max_consecutive_failures"]
    )

    _acquire_scheduler_lock(paths, provider_name=provider_name)
    consecutive_failures = 0
    last_cycle_summary: dict[str, Any] | None = None
    last_cycle_path = paths.scheduler_runs_dir / "latest-cycle-placeholder.json"

    try:
        while True:
            cycle_id = f"cycle-{utc_now_iso().replace(':', '').replace('-', '')}"
            cycle_started_at = utc_now_iso()
            cycle_summary: dict[str, Any] = {
                "cycle_id": cycle_id,
                "started_at": cycle_started_at,
                "mode": {
                    "auto_apply": auto_apply,
                    "auto_evaluate_promotions": auto_evaluate_promotions,
                    "auto_promote": auto_promote,
                    "once": once,
                    "dry_run": dry_run,
                },
                "settings": {
                    "discovery_mode": active_discovery_mode,
                    "pull_interval_seconds": interval_seconds,
                    "success_cooldown_seconds": success_cooldown,
                    "failure_cooldown_seconds": failure_cooldown,
                    "sleep_jitter_seconds": jitter_seconds,
                    "max_consecutive_failures": max_failures,
                },
                "actions": {
                    "pull_markets": False,
                    "prepare_batch": False,
                    "run_llm": False,
                    "apply": False,
                    "evaluate_promotions": False,
                    "skipped_existing_completed_batch": False,
                },
                "outcomes": {
                    "new_market_count": 0,
                    "threshold_met": False,
                    "batch_ready": False,
                    "parsed_response_available": False,
                },
                "files": {},
                "error": None,
            }

            sleep_seconds = interval_seconds
            scheduler_state = _load_scheduler_state(paths)
            try:
                _scheduler_log(f"Scheduler cycle {cycle_id}: pulling markets")
                pull_path, pull_summary = pull_markets(
                    provider_name=provider_name,
                    scope_config=scope_config,
                    snapshot_dir=snapshot_dir,
                    discovery_mode=active_discovery_mode,
                    config_path=config_path,
                    dry_run=dry_run,
                )
                cycle_summary["actions"]["pull_markets"] = True
                cycle_summary["files"]["pull_report_path"] = str(pull_path)
                cycle_summary["outcomes"]["new_market_count"] = int(
                    pull_summary.get("llm_threshold_status", {}).get("new_market_count", 0)
                )
                cycle_summary["outcomes"]["threshold_met"] = bool(
                    pull_summary.get("llm_threshold_status", {}).get("threshold_met", False)
                )

                if not cycle_summary["outcomes"]["threshold_met"]:
                    _scheduler_log("Threshold not met; sleeping without categorization")
                    sleep_seconds = interval_seconds
                else:
                    _scheduler_log("Threshold met; preparing categorization batch")
                    batch_path, batch_summary = prepare_llm_categorization_batch(
                        provider_name=provider_name,
                        scope_config=scope_config,
                        config_path=config_path,
                        dry_run=dry_run,
                    )
                    cycle_summary["actions"]["prepare_batch"] = True
                    cycle_summary["files"]["batch_path"] = str(batch_path)
                    cycle_summary["outcomes"]["batch_ready"] = batch_summary.get("status") == "ready"
                    batch_fingerprint = _categorization_input_fingerprint(batch_summary)
                    cycle_summary["outcomes"]["batch_input_fingerprint"] = batch_fingerprint

                    if batch_summary.get("status") != "ready":
                        _scheduler_log(f"Prepared batch not ready (status={batch_summary.get('status')}); sleeping")
                    elif scheduler_state.get("last_completed_input_fingerprint") == batch_fingerprint:
                        cycle_summary["actions"]["skipped_existing_completed_batch"] = True
                        cycle_summary["files"]["last_completed_llm_run_summary_path"] = scheduler_state.get(
                            "last_completed_llm_run_summary_path"
                        )
                        _scheduler_log("Pending batch matches the last completed LLM-reviewed batch; skipping rerun")
                    else:
                        _scheduler_log("Running LLM categorization")
                        llm_run_path, llm_run_summary = run_llm_categorization(
                            provider_name=provider_name,
                            scope_config=scope_config,
                            config_path=config_path,
                            dry_run=dry_run,
                        )
                        cycle_summary["actions"]["run_llm"] = True
                        cycle_summary["files"]["llm_run_summary_path"] = str(llm_run_path)
                        cycle_summary["outcomes"]["parsed_response_available"] = bool(
                            llm_run_summary.get("parsed_successfully")
                        )

                        if llm_run_summary.get("parsed_successfully"):
                            scheduler_state["last_completed_input_fingerprint"] = batch_fingerprint
                            scheduler_state["last_completed_batch_id"] = batch_summary.get("batch_id")
                            scheduler_state["last_completed_llm_run_summary_path"] = str(llm_run_path)
                            scheduler_state["last_completed_at"] = utc_now_iso()
                            scheduler_state["last_completed_parsed_response_path"] = llm_run_summary.get("parsed_response_path")
                            _write_scheduler_state(paths, provider_name, scheduler_state)
                            _scheduler_log("LLM response parsed successfully")

                            if auto_apply:
                                parsed_response_path = llm_run_summary.get("parsed_response_path")
                                if not parsed_response_path:
                                    raise RuntimeError("auto-apply requested but parsed_response_path is missing")
                                _scheduler_log("Applying categorization decisions")
                                apply_path, apply_summary = apply_llm_categorization(
                                    provider_name=provider_name,
                                    scope_config=scope_config,
                                    response_path=Path(parsed_response_path),
                                    dry_run=dry_run,
                                )
                                cycle_summary["actions"]["apply"] = True
                                cycle_summary["files"]["apply_summary_path"] = str(apply_path)
                                cycle_summary["outcomes"]["applied_assignment_count"] = int(
                                    apply_summary.get("existing_category_assignment_count", 0)
                                ) + int(apply_summary.get("proposed_candidate_category_market_count", 0))

                            if auto_evaluate_promotions:
                                _scheduler_log("Evaluating candidate category promotions")
                                promotion_path, promotion_summary = evaluate_category_promotions(
                                    provider_name=provider_name,
                                    scope_config=scope_config,
                                    config_path=config_path,
                                    promote=auto_promote,
                                    dry_run=dry_run,
                                )
                                cycle_summary["actions"]["evaluate_promotions"] = True
                                cycle_summary["files"]["promotion_report_path"] = str(promotion_path)
                                cycle_summary["outcomes"]["promoted_category_count"] = len(
                                    promotion_summary.get("promoted_category_names", [])
                                )
                            sleep_seconds = success_cooldown if success_cooldown > 0 else interval_seconds
                        else:
                            _scheduler_log("LLM run completed without a parsed response; leaving artifacts for review")

                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                cycle_summary["error"] = str(exc)
                _scheduler_log(f"Cycle failed: {exc}")
                sleep_seconds = failure_cooldown if failure_cooldown > 0 else interval_seconds
                if max_failures > 0 and consecutive_failures >= max_failures:
                    cycle_summary["terminated_after_max_failures"] = True
                    last_cycle_path = _write_scheduler_cycle_summary(
                        paths=paths,
                        provider_name=provider_name,
                        cycle_summary={
                            **cycle_summary,
                            "ended_at": utc_now_iso(),
                            "sleep_seconds_before_next_cycle": 0,
                            "consecutive_failures": consecutive_failures,
                        },
                    )
                    last_cycle_summary = cycle_summary
                    raise RuntimeError(
                        f"scheduler reached max_consecutive_failures={max_failures}; last error: {exc}"
                    ) from exc

            cycle_summary["ended_at"] = utc_now_iso()
            cycle_summary["sleep_seconds_before_next_cycle"] = 0 if once else _scheduler_sleep(
                sleep_seconds,
                jitter_seconds=jitter_seconds,
            )
            cycle_summary["consecutive_failures"] = consecutive_failures
            last_cycle_path = _write_scheduler_cycle_summary(
                paths=paths,
                provider_name=provider_name,
                cycle_summary=cycle_summary,
            )
            last_cycle_summary = cycle_summary
            _scheduler_log(
                "Cycle complete: "
                f"new={cycle_summary['outcomes'].get('new_market_count', 0)} "
                f"threshold_met={cycle_summary['outcomes'].get('threshold_met')} "
                f"prepared={cycle_summary['actions'].get('prepare_batch')} "
                f"llm={cycle_summary['actions'].get('run_llm')} "
                f"apply={cycle_summary['actions'].get('apply')} "
                f"promotions={cycle_summary['actions'].get('evaluate_promotions')}"
            )
            if once:
                break
    except KeyboardInterrupt:
        interrupt_summary = {
            "cycle_id": f"interrupt-{utc_now_iso().replace(':', '').replace('-', '')}",
            "started_at": utc_now_iso(),
            "ended_at": utc_now_iso(),
            "interrupted": True,
            "last_cycle_id": last_cycle_summary.get("cycle_id") if last_cycle_summary else None,
            "message": "Scheduler stopped by KeyboardInterrupt.",
        }
        last_cycle_path = _write_scheduler_cycle_summary(
            paths=paths,
            provider_name=provider_name,
            cycle_summary=interrupt_summary,
        )
        _scheduler_log("Scheduler stopped cleanly")
        last_cycle_summary = interrupt_summary
    finally:
        _release_scheduler_lock(paths)

    return last_cycle_path, last_cycle_summary or {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual market categorization workflow.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pull_parser = subparsers.add_parser("pull_markets", help="Fetch discoverable markets and update local categorization state.")
    add_scope_arguments(pull_parser)
    pull_parser.add_argument("--snapshot-dir", type=Path, default=None)
    pull_parser.add_argument("--discovery-mode", choices=["all", "scoped"], default=None)
    pull_parser.add_argument("--dry-run", action="store_true")

    prepare_parser = subparsers.add_parser(
        "prepare_llm_categorization_batch",
        help="Prepare an inspectable LLM categorization batch when thresholds are met.",
    )
    add_scope_arguments(prepare_parser)
    prepare_parser.add_argument("--dry-run", action="store_true")

    run_parser = subparsers.add_parser(
        "run_llm_categorization",
        help="Read a prepared batch, call the LLM, and save raw and parsed outputs without applying them.",
    )
    add_scope_arguments(run_parser)
    run_parser.add_argument("--dry-run", action="store_true")

    scheduler_parser = subparsers.add_parser(
        "run_scheduler",
        help="Long-running orchestration loop around pull, batch prep, LLM run, and optional apply/promotion steps.",
    )
    add_scope_arguments(scheduler_parser)
    scheduler_parser.add_argument("--snapshot-dir", type=Path, default=None)
    scheduler_parser.add_argument("--discovery-mode", choices=["all", "scoped"], default=None)
    scheduler_parser.add_argument("--pull-interval-seconds", type=int, default=None)
    scheduler_parser.add_argument("--success-cooldown-seconds", type=int, default=None)
    scheduler_parser.add_argument("--failure-cooldown-seconds", type=int, default=None)
    scheduler_parser.add_argument("--sleep-jitter-seconds", type=float, default=None)
    scheduler_parser.add_argument("--max-consecutive-failures", type=int, default=None)
    scheduler_parser.add_argument("--auto-apply", action=argparse.BooleanOptionalAction, default=False)
    scheduler_parser.add_argument("--auto-evaluate-promotions", action=argparse.BooleanOptionalAction, default=False)
    scheduler_parser.add_argument("--auto-promote", action=argparse.BooleanOptionalAction, default=False)
    scheduler_parser.add_argument("--once", action="store_true")
    scheduler_parser.add_argument("--dry-run", action="store_true")

    watch_parser = subparsers.add_parser(
        "watch_and_categorize",
        help="Alias for run_scheduler.",
    )
    add_scope_arguments(watch_parser)
    watch_parser.add_argument("--snapshot-dir", type=Path, default=None)
    watch_parser.add_argument("--discovery-mode", choices=["all", "scoped"], default=None)
    watch_parser.add_argument("--pull-interval-seconds", type=int, default=None)
    watch_parser.add_argument("--success-cooldown-seconds", type=int, default=None)
    watch_parser.add_argument("--failure-cooldown-seconds", type=int, default=None)
    watch_parser.add_argument("--sleep-jitter-seconds", type=float, default=None)
    watch_parser.add_argument("--max-consecutive-failures", type=int, default=None)
    watch_parser.add_argument("--auto-apply", action=argparse.BooleanOptionalAction, default=False)
    watch_parser.add_argument("--auto-evaluate-promotions", action=argparse.BooleanOptionalAction, default=False)
    watch_parser.add_argument("--auto-promote", action=argparse.BooleanOptionalAction, default=False)
    watch_parser.add_argument("--once", action="store_true")
    watch_parser.add_argument("--dry-run", action="store_true")

    apply_parser = subparsers.add_parser(
        "apply_llm_categorization",
        help="Validate and apply a structured LLM categorization response.",
    )
    add_scope_arguments(apply_parser)
    apply_parser.add_argument("--response-path", type=Path, required=True)
    apply_parser.add_argument("--dry-run", action="store_true")

    promote_parser = subparsers.add_parser(
        "evaluate_category_promotions",
        help="Evaluate candidate categories and optionally promote them.",
    )
    add_scope_arguments(promote_parser)
    promote_parser.add_argument("--promote", action="store_true")
    promote_parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        provider_name, scope_config = resolve_scope_from_args(args)
        config_path = getattr(args, "config", None)
        workflow_settings = _load_workflow_settings(config_path)
        discovery_mode = getattr(args, "discovery_mode", None) or workflow_settings["discovery_mode"]

        if args.command == "pull_markets":
            output_path, summary = pull_markets(
                provider_name=provider_name,
                scope_config=scope_config,
                snapshot_dir=args.snapshot_dir,
                discovery_mode=discovery_mode,
                config_path=config_path,
                dry_run=args.dry_run,
            )
        elif args.command == "prepare_llm_categorization_batch":
            output_path, summary = prepare_llm_categorization_batch(
                provider_name=provider_name,
                scope_config=scope_config,
                config_path=config_path,
                dry_run=args.dry_run,
            )
        elif args.command == "run_llm_categorization":
            output_path, summary = run_llm_categorization(
                provider_name=provider_name,
                scope_config=scope_config,
                config_path=config_path,
                dry_run=args.dry_run,
            )
        elif args.command in {"run_scheduler", "watch_and_categorize"}:
            output_path, summary = run_scheduler(
                provider_name=provider_name,
                scope_config=scope_config,
                config_path=config_path,
                snapshot_dir=args.snapshot_dir,
                discovery_mode=discovery_mode,
                auto_apply=args.auto_apply,
                auto_evaluate_promotions=args.auto_evaluate_promotions,
                auto_promote=args.auto_promote,
                once=args.once,
                dry_run=args.dry_run,
                pull_interval_seconds=args.pull_interval_seconds,
                success_cooldown_seconds=args.success_cooldown_seconds,
                failure_cooldown_seconds=args.failure_cooldown_seconds,
                sleep_jitter_seconds=args.sleep_jitter_seconds,
                max_consecutive_failures=args.max_consecutive_failures,
            )
        elif args.command == "apply_llm_categorization":
            output_path, summary = apply_llm_categorization(
                provider_name=provider_name,
                scope_config=scope_config,
                response_path=args.response_path,
                dry_run=args.dry_run,
            )
        else:
            output_path, summary = evaluate_category_promotions(
                provider_name=provider_name,
                scope_config=scope_config,
                config_path=config_path,
                promote=args.promote,
                dry_run=args.dry_run,
            )
        print(output_path)
        print(json.dumps(summary, indent=2, sort_keys=False))
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        raise SystemExit(f"Error: {exc}") from exc


if __name__ == "__main__":
    main()
