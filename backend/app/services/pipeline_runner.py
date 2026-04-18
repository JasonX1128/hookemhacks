from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
from typing import Any

from backend.app.core.config import Settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class PipelineLaunchResult:
    status: str
    running: bool
    started: bool
    command: list[str]
    config_path: str
    log_path: str
    pid: int | None
    started_at: str | None
    finished_at: str | None
    exit_code: int | None
    market_count: int | None
    artifact_market_count: int | None = None
    discovered_market_count: int | None = None
    pairwise_market_count: int | None = None
    progress_status: str | None = None
    progress_message: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "running": self.running,
            "started": self.started,
            "command": self.command,
            "config_path": self.config_path,
            "log_path": self.log_path,
            "pid": self.pid,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "exit_code": self.exit_code,
            "market_count": self.market_count,
            "artifact_market_count": self.artifact_market_count,
            "discovered_market_count": self.discovered_market_count,
            "pairwise_market_count": self.pairwise_market_count,
            "progress_status": self.progress_status,
            "progress_message": self.progress_message,
            "reason": self.reason,
        }


class PipelineRunner:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._last_exit_code: int | None = None
        self._last_log_path: str = ""

    @property
    def repo_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    @property
    def runs_dir(self) -> Path:
        path = self.repo_root / "backend" / "pipeline_runs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def state_path(self) -> Path:
        return self.runs_dir / "startup-refresh-state.json"

    def _read_state(self) -> dict[str, Any] | None:
        if not self.state_path.exists():
            return None
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _write_state(self, payload: dict[str, Any]) -> None:
        temp_path = self.state_path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(self.state_path)

    def _clear_state(self) -> None:
        if self.state_path.exists():
            self.state_path.unlink()

    def _pid_state(self, pid: int | None) -> str | None:
        if pid is None or pid <= 0:
            return None
        try:
            os.kill(pid, 0)
        except OSError:
            return None
        try:
            result = subprocess.run(
                ["ps", "-o", "state=", "-p", str(pid)],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception:
            return "?"
        if result.returncode != 0:
            return None
        state = result.stdout.strip()
        if not state:
            return None
        return state[0]

    def _pid_is_running(self, pid: int | None) -> bool:
        state = self._pid_state(pid)
        return state is not None and not state.startswith("Z")

    def _active_state(self) -> dict[str, Any] | None:
        state = self._read_state()
        if not state:
            return None
        pid = state.get("pid")
        if isinstance(pid, int) and self._pid_is_running(pid):
            return state
        return None

    def _artifact_market_count_for_config(self, config_path: Path) -> int | None:
        try:
            from data_pipeline.scope import load_scope_config

            scope_config = load_scope_config(config_path)
        except Exception:
            return None

        manifest_path = self.repo_root / "data_pipeline" / "artifacts" / scope_config.scope_slug / "artifact_manifest.json"
        if manifest_path.exists():
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                record_count = payload.get("artifacts", {}).get("market_metadata", {}).get("record_count")
                if isinstance(record_count, int):
                    return record_count
            except Exception:
                return None
        return None

    def _progress_payload_for_config(self, config_path: Path) -> dict[str, Any] | None:
        try:
            from data_pipeline.scope import load_scope_config

            scope_config = load_scope_config(config_path)
        except Exception:
            return None

        progress_path = self.repo_root / "data_pipeline" / "artifacts" / scope_config.scope_slug / "pipeline_progress.json"
        if not progress_path.exists():
            return None

        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _progress_market_count_for_config(self, config_path: Path) -> int | None:
        payload = self._progress_payload_for_config(config_path)
        if not payload:
            return None
        count = payload.get("discovered_market_count")
        return int(count) if isinstance(count, (int, float)) else None

    def _progress_artifact_market_count_for_config(self, config_path: Path) -> int | None:
        payload = self._progress_payload_for_config(config_path)
        if not payload:
            return None
        count = payload.get("artifact_market_count")
        return int(count) if isinstance(count, (int, float)) else None

    def _pairwise_market_count_for_config(self, config_path: Path) -> int | None:
        try:
            from data_pipeline.scope import load_scope_config

            scope_config = load_scope_config(config_path)
        except Exception:
            return None

        run_summary_path = self.repo_root / "data_pipeline" / "artifacts" / scope_config.scope_slug / "run_summary.json"
        if run_summary_path.exists():
            try:
                payload = json.loads(run_summary_path.read_text(encoding="utf-8"))
                summary = payload.get("summary")
                if not isinstance(summary, dict):
                    summary = payload.get("extra", {}).get("summary")
                pair_count = summary.get("comovement_pair_count") if isinstance(summary, dict) else None
                if isinstance(pair_count, int):
                    return pair_count
            except Exception:
                return None
        return None

    def _progress_status_for_config(self, config_path: Path) -> str | None:
        payload = self._progress_payload_for_config(config_path)
        status = payload.get("status") if payload else None
        return str(status) if isinstance(status, str) and status else None

    def _progress_message_for_config(self, config_path: Path) -> str | None:
        payload = self._progress_payload_for_config(config_path)
        message = payload.get("message") if payload else None
        return str(message) if isinstance(message, str) and message else None

    def _record_finished_state(self, *, pid: int | None, exit_code: int | None, config_path: Path) -> None:
        self._last_exit_code = exit_code
        self._finished_at = _utc_now_iso()
        artifact_market_count = self._artifact_market_count_for_config(config_path)
        if artifact_market_count is None:
            artifact_market_count = self._progress_artifact_market_count_for_config(config_path)
        discovered_market_count = self._progress_market_count_for_config(config_path)
        pairwise_market_count = self._pairwise_market_count_for_config(config_path)
        market_count = artifact_market_count if artifact_market_count is not None else discovered_market_count
        state = self._read_state() or {}
        if exit_code == 0:
            status = "completed"
            reason = state.get("reason")
        else:
            status = "failed"
            reason = state.get("reason") or f"Refresh process exited with code {exit_code}."
        self._write_state(
            {
                "status": status,
                "running": False,
                "pid": pid,
                "started_at": state.get("started_at", self._started_at),
                "finished_at": self._finished_at,
                "exit_code": exit_code,
                "log_path": state.get("log_path", self._last_log_path),
                "config_path": str(config_path),
                "market_count": market_count,
                "artifact_market_count": artifact_market_count,
                "discovered_market_count": discovered_market_count,
                "pairwise_market_count": pairwise_market_count,
                "progress_status": self._progress_status_for_config(config_path),
                "progress_message": self._progress_message_for_config(config_path),
                "reason": reason,
            }
        )

    def _refresh_process_state(self) -> None:
        if self._process is None:
            return
        exit_code = self._process.poll()
        if exit_code is None:
            return
        config_path = Path((self._read_state() or {}).get("config_path") or self.repo_root / "data_pipeline/configs/kalshi_live_all_pages.json")
        self._record_finished_state(pid=self._process.pid, exit_code=exit_code, config_path=config_path)
        self._process = None

    def _normalize_state(self, config_path: Path) -> dict[str, Any]:
        state = self._read_state() or {}
        if not state:
            return {}
        if str(state.get("status")) != "running":
            return state
        pid = state.get("pid")
        pid_state = self._pid_state(pid if isinstance(pid, int) else None)
        if pid_state is not None and not pid_state.startswith("Z"):
            return state

        artifact_market_count = self._artifact_market_count_for_config(config_path)
        if artifact_market_count is None:
            artifact_market_count = self._progress_artifact_market_count_for_config(config_path)
        discovered_market_count = self._progress_market_count_for_config(config_path)
        pairwise_market_count = self._pairwise_market_count_for_config(config_path)
        normalized = {
            **state,
            "status": "failed",
            "running": False,
            "finished_at": state.get("finished_at") or _utc_now_iso(),
            "market_count": artifact_market_count if artifact_market_count is not None else discovered_market_count,
            "artifact_market_count": artifact_market_count,
            "discovered_market_count": discovered_market_count,
            "pairwise_market_count": pairwise_market_count,
            "progress_status": self._progress_status_for_config(config_path),
            "progress_message": self._progress_message_for_config(config_path),
            "reason": f"Refresh process is no longer alive{f' (state {pid_state})' if pid_state else ''}.",
        }
        self._write_state(normalized)
        return normalized

    def current_startup_status(self, settings: Settings) -> PipelineLaunchResult:
        config_path = (self.repo_root / settings.pipeline_startup_config).resolve()
        artifact_market_count = self._artifact_market_count_for_config(config_path)
        if artifact_market_count is None:
            artifact_market_count = self._progress_artifact_market_count_for_config(config_path)
        discovered_market_count = self._progress_market_count_for_config(config_path)
        pairwise_market_count = self._pairwise_market_count_for_config(config_path)
        progress_status = self._progress_status_for_config(config_path)
        progress_message = self._progress_message_for_config(config_path)
        market_count = artifact_market_count if artifact_market_count is not None else discovered_market_count
        command = [
            settings.pipeline_startup_python,
            "-m",
            "data_pipeline.main",
            "all",
            "--config",
            str(config_path),
        ]
        with self._lock:
            self._refresh_process_state()
            active_state = self._active_state()
            if not settings.pipeline_startup_enabled:
                return PipelineLaunchResult(
                    status="disabled",
                    running=False,
                    started=False,
                    command=command,
                    config_path=str(config_path),
                    log_path=self._last_log_path,
                    pid=None,
                    started_at=self._started_at,
                    finished_at=self._finished_at,
                    exit_code=self._last_exit_code,
                    market_count=market_count,
                    artifact_market_count=artifact_market_count,
                    discovered_market_count=discovered_market_count,
                    pairwise_market_count=pairwise_market_count,
                    progress_status=progress_status,
                    progress_message=progress_message,
                    reason="BACKEND_PIPELINE_STARTUP_ENABLED is false.",
                )
            if active_state is not None:
                return PipelineLaunchResult(
                    status="running",
                    running=True,
                    started=False,
                    command=command,
                    config_path=str(config_path),
                    log_path=str(active_state.get("log_path") or self._last_log_path),
                    pid=int(active_state.get("pid")) if active_state.get("pid") is not None else None,
                    started_at=active_state.get("started_at") or self._started_at,
                    finished_at=active_state.get("finished_at") or self._finished_at,
                    exit_code=active_state.get("exit_code") if active_state.get("exit_code") is not None else self._last_exit_code,
                    market_count=market_count if market_count is not None else active_state.get("market_count"),
                    artifact_market_count=artifact_market_count if artifact_market_count is not None else active_state.get("artifact_market_count"),
                    discovered_market_count=discovered_market_count if discovered_market_count is not None else active_state.get("discovered_market_count"),
                    pairwise_market_count=pairwise_market_count if pairwise_market_count is not None else active_state.get("pairwise_market_count"),
                    progress_status=progress_status if progress_status is not None else active_state.get("progress_status"),
                    progress_message=progress_message if progress_message is not None else active_state.get("progress_message"),
                    reason="A startup-triggered pipeline refresh is currently running.",
                )
            state = self._normalize_state(config_path)
            status = "idle" if not state and self._started_at is None else str(state.get("status") or "completed")
            return PipelineLaunchResult(
                status=status,
                running=False,
                started=False,
                command=command,
                config_path=str(config_path),
                log_path=str(state.get("log_path") or self._last_log_path),
                pid=None,
                started_at=state.get("started_at") or self._started_at,
                finished_at=state.get("finished_at") or self._finished_at,
                exit_code=state.get("exit_code") if state.get("exit_code") is not None else self._last_exit_code,
                market_count=market_count if market_count is not None else state.get("market_count"),
                artifact_market_count=artifact_market_count if artifact_market_count is not None else state.get("artifact_market_count"),
                discovered_market_count=discovered_market_count if discovered_market_count is not None else state.get("discovered_market_count"),
                pairwise_market_count=pairwise_market_count if pairwise_market_count is not None else state.get("pairwise_market_count"),
                progress_status=progress_status if progress_status is not None else state.get("progress_status"),
                progress_message=progress_message if progress_message is not None else state.get("progress_message"),
                reason=str(state.get("reason")) if state.get("reason") else None,
            )

    def start_startup_refresh(self, settings: Settings) -> PipelineLaunchResult:
        config_path = (self.repo_root / settings.pipeline_startup_config).resolve()
        artifact_market_count = self._artifact_market_count_for_config(config_path)
        discovered_market_count = self._progress_market_count_for_config(config_path)
        pairwise_market_count = self._pairwise_market_count_for_config(config_path)
        progress_status = self._progress_status_for_config(config_path)
        progress_message = self._progress_message_for_config(config_path)
        market_count = artifact_market_count if artifact_market_count is not None else discovered_market_count
        command = [
            settings.pipeline_startup_python,
            "-m",
            "data_pipeline.main",
            "all",
            "--config",
            str(config_path),
        ]
        with self._lock:
            self._refresh_process_state()
            active_state = self._active_state()
            if not settings.pipeline_startup_enabled:
                return PipelineLaunchResult(
                    status="disabled",
                    running=False,
                    started=False,
                    command=command,
                    config_path=str(config_path),
                    log_path=self._last_log_path,
                    pid=None,
                    started_at=self._started_at,
                    finished_at=self._finished_at,
                    exit_code=self._last_exit_code,
                    market_count=market_count,
                    artifact_market_count=artifact_market_count,
                    discovered_market_count=discovered_market_count,
                    pairwise_market_count=pairwise_market_count,
                    progress_status=progress_status,
                    progress_message=progress_message,
                    reason="BACKEND_PIPELINE_STARTUP_ENABLED is false.",
                )
            if active_state is not None:
                return PipelineLaunchResult(
                    status="already_running",
                    running=True,
                    started=False,
                    command=command,
                    config_path=str(config_path),
                    log_path=str(active_state.get("log_path") or self._last_log_path),
                    pid=int(active_state.get("pid")) if active_state.get("pid") is not None else None,
                    started_at=active_state.get("started_at") or self._started_at,
                    finished_at=active_state.get("finished_at") or self._finished_at,
                    exit_code=active_state.get("exit_code") if active_state.get("exit_code") is not None else self._last_exit_code,
                    market_count=market_count if market_count is not None else active_state.get("market_count"),
                    artifact_market_count=artifact_market_count if artifact_market_count is not None else active_state.get("artifact_market_count"),
                    discovered_market_count=discovered_market_count if discovered_market_count is not None else active_state.get("discovered_market_count"),
                    pairwise_market_count=pairwise_market_count if pairwise_market_count is not None else active_state.get("pairwise_market_count"),
                    progress_status=progress_status if progress_status is not None else active_state.get("progress_status"),
                    progress_message=progress_message if progress_message is not None else active_state.get("progress_message"),
                    reason="A startup-triggered pipeline refresh is already running.",
                )
            cooldown_seconds = max(0, settings.pipeline_startup_cooldown_seconds)
            state = self._normalize_state(config_path)
            last_status = str(state.get("status") or "")
            should_enforce_cooldown = last_status in {"running", "completed", "cooldown_skipped", "already_running", "started"}
            if cooldown_seconds > 0 and self._started_at is not None and should_enforce_cooldown:
                started_at = datetime.fromisoformat(self._started_at.replace("Z", "+00:00"))
                elapsed_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                if elapsed_seconds < cooldown_seconds:
                    return PipelineLaunchResult(
                        status="cooldown_skipped",
                        running=False,
                        started=False,
                        command=command,
                        config_path=str(config_path),
                        log_path=self._last_log_path,
                        pid=None,
                        started_at=self._started_at,
                        finished_at=self._finished_at,
                        exit_code=self._last_exit_code,
                        market_count=market_count,
                        artifact_market_count=artifact_market_count,
                        discovered_market_count=discovered_market_count,
                        pairwise_market_count=pairwise_market_count,
                        progress_status=progress_status,
                        progress_message=progress_message,
                        reason=f"Last startup refresh began {int(elapsed_seconds)}s ago; cooldown is {cooldown_seconds}s.",
                    )

            run_id = datetime.now(timezone.utc).strftime("startup-refresh-%Y%m%dT%H%M%SZ")
            log_path = self.runs_dir / f"{run_id}.log"
            log_handle = log_path.open("w", encoding="utf-8")
            process = subprocess.Popen(
                command,
                cwd=self.repo_root,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self._process = process
            self._started_at = _utc_now_iso()
            self._finished_at = None
            self._last_exit_code = None
            self._last_log_path = str(log_path)
            self._write_state(
                {
                    "status": "running",
                    "running": True,
                    "pid": process.pid,
                    "started_at": self._started_at,
                    "finished_at": None,
                    "exit_code": None,
                    "log_path": str(log_path),
                    "config_path": str(config_path),
                    "market_count": market_count,
                    "artifact_market_count": artifact_market_count,
                    "discovered_market_count": discovered_market_count,
                    "pairwise_market_count": pairwise_market_count,
                    "progress_status": progress_status,
                    "progress_message": progress_message,
                }
            )
            return PipelineLaunchResult(
                status="started",
                running=True,
                started=True,
                command=command,
                config_path=str(config_path),
                log_path=str(log_path),
                pid=process.pid,
                started_at=self._started_at,
                finished_at=self._finished_at,
                exit_code=self._last_exit_code,
                market_count=market_count,
                artifact_market_count=artifact_market_count,
                discovered_market_count=discovered_market_count,
                pairwise_market_count=pairwise_market_count,
                progress_status=progress_status,
                progress_message=progress_message,
            )

    def stop_startup_refresh(self, settings: Settings) -> PipelineLaunchResult:
        config_path = (self.repo_root / settings.pipeline_startup_config).resolve()
        command = [
            settings.pipeline_startup_python,
            "-m",
            "data_pipeline.main",
            "all",
            "--config",
            str(config_path),
        ]
        with self._lock:
            self._refresh_process_state()
            active_state = self._active_state()
            if active_state is None:
                return self.current_startup_status(settings)

            pid = int(active_state.get("pid")) if active_state.get("pid") is not None else None
            stopped = False
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGTERM)
                    stopped = True
                except OSError:
                    stopped = False

            self._finished_at = _utc_now_iso()
            self._last_exit_code = -15 if stopped else self._last_exit_code
            self._write_state(
                {
                    "status": "stopped",
                    "running": False,
                    "pid": pid,
                    "started_at": active_state.get("started_at"),
                    "finished_at": self._finished_at,
                    "exit_code": self._last_exit_code,
                    "log_path": active_state.get("log_path"),
                    "config_path": str(config_path),
                    "market_count": self._artifact_market_count_for_config(config_path) or self._progress_market_count_for_config(config_path),
                    "artifact_market_count": self._artifact_market_count_for_config(config_path),
                    "discovered_market_count": self._progress_market_count_for_config(config_path),
                    "pairwise_market_count": self._pairwise_market_count_for_config(config_path),
                    "progress_status": self._progress_status_for_config(config_path),
                    "progress_message": self._progress_message_for_config(config_path),
                    "reason": "Refresh stopped manually from the extension.",
                }
            )
            if self._process is not None and pid == self._process.pid:
                self._process = None
            return PipelineLaunchResult(
                status="stopped",
                running=False,
                started=False,
                command=command,
                config_path=str(config_path),
                log_path=str(active_state.get("log_path") or self._last_log_path),
                pid=pid,
                started_at=active_state.get("started_at") or self._started_at,
                finished_at=self._finished_at,
                exit_code=self._last_exit_code,
                market_count=self._artifact_market_count_for_config(config_path) or self._progress_market_count_for_config(config_path),
                artifact_market_count=self._artifact_market_count_for_config(config_path),
                discovered_market_count=self._progress_market_count_for_config(config_path),
                pairwise_market_count=self._pairwise_market_count_for_config(config_path),
                progress_status=self._progress_status_for_config(config_path),
                progress_message=self._progress_message_for_config(config_path),
                reason="Refresh stopped manually from the extension.",
            )


pipeline_runner = PipelineRunner()
