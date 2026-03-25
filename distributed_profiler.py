#!/usr/bin/env python3
"""
Distributed master controller for profiling tasks.

This script coordinates the execution of measurement and curve fitting tasks
across multiple worker servers using SSH. It copies required binaries on a
shared filesystem, dispatches tasks according to the distribution plan, and
updates the analysis progress file to keep external consumers informed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import shutil


class ConfigError(Exception):
    """Raised when configuration inputs are invalid."""


class TaskError(Exception):
    """Raised when a distributed task fails after retries."""


@dataclass
class Server:
    id: str
    host: str
    port: int
    user: str
    ssh_key: Optional[str]
    max_concurrent_tasks: int
    work_dir: str
    semaphore: asyncio.Semaphore = field(init=False)

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

    @property
    def ssh_destination(self) -> str:
        return f"{self.user}@{self.host}"

    def base_command(self) -> List[str]:
        cmd = [
            shutil.which("ssh") or "ssh",
            "-p",
            str(self.port),
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
        ]
        if self.ssh_key:
            cmd.extend(["-i", self.ssh_key])
        cmd.append(self.ssh_destination)
        return cmd


@dataclass
class Task:
    name: str
    group: str
    remote_args: Sequence[str]
    timeout: int
    retries: int
    server: Optional[Server] = None


def setup_logger(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"Missing configuration file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_dir(path: Path, mode: int = 0o755) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        current_mode = path.stat().st_mode & 0o777
        if current_mode != mode:
            path.chmod(mode)
    except PermissionError as exc:
        logging.warning("Unable to set permissions for %s to %o: %s", path, mode, exc)


def copy_if_needed(src: Path, dest: Path) -> None:
    if not src.exists():
        raise ConfigError(f"Required file not found: {src}")
    if src.resolve() == dest.resolve():
        return
    ensure_dir(dest.parent)
    if dest.exists():
        dest.unlink()
    shutil.copy2(src, dest)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def render_progress_record(
    job_id: str,
    repo: str,
    repo_name: str,
    start_time: str,
    status: str,
    current_step: str,
    next_step: str,
    percent: float,
    end_time: Optional[str] = "",
    message: str = "",
    parallel_groups: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    progress_data = {
        "currentStep": current_step,
        "nextStep": next_step,
        "percent": round(percent, 2),
    }

    # Add parallel group progress if provided
    if parallel_groups:
        progress_data["parallelGroups"] = parallel_groups

    return {
        "id": job_id,
        "repo": repo,
        "repoName": repo_name,
        "startTime": start_time,
        "endTime": end_time or "",
        "status": status,
        "progress": progress_data,
        "result": {
            "errorCode": 0,
            "message": message,
            "repo": repo,
        },
    }


class DistributedProfiler:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cluster_config = load_json(Path(args.cluster_config))
        self.task_config = load_json(Path(args.task_config))
        self.profiler_config = load_json(Path(args.profiler_config))

        self.job_id = self._resolve_job_id(args.job_id)
        self.repo_path = Path(args.repo_path).resolve()
        self.repo_name = args.repo_name
        self.analysis_file = self._resolve_analysis_file_path(args.analysis_file)

        self.start_time = args.start_time
        self.repo = args.repo
        self.start_progress = float(args.start_progress)

        shared_mount = self.cluster_config["shared_storage"]["mount_point"]
        self.shared_mount = Path(shared_mount)
        ensure_dir(self.shared_mount)

        job_root = args.job_root or f"jobs/{self.job_id}"
        self.job_dir = self.shared_mount / job_root
        self.temp_dir = self.job_dir / "temp"
        self.state_file = self.job_dir / "job_state.json"
        self.worker_dest = self.job_dir / "worker.sh"
        self.fit_script_path = args.fit_script
        self.worker_source = Path(args.worker_script).resolve()

        ensure_dir(self.job_dir, mode=0o777)
        ensure_dir(self.temp_dir, mode=0o777)

        self.servers = [
            self._make_server(entry) for entry in self.cluster_config["servers"]
        ]

        self.iva_values = args.iva_values
        self.core_values = args.core_values
        self.algo = args.algo
        self.algo_original = args.algo_original
        self.iva_data = args.iva_data
        self.thmgr_api = args.thmgr_api
        self.curve_types = args.curve_types or []

        self.serial_progress = float(args.serial_progress)
        self.thmgr_progress = float(args.thmgr_progress)
        self.direct_progress = float(args.direct_progress)
        self.curve_progress = float(args.curve_progress)
        self.request_delay = float(args.request_delay)

        self.iva_file = self._job_path_for(args.iva_data_file)
        self.core_file = self._job_path_for(args.core_count_file)
        self.power_profile_path = self._job_path_for(args.power_profile_file)
        self.measurement_summary_path = self.job_dir / "measurement_summary.json"

        self._stage_required_files()
        self.power_profile_map = self._load_power_profile()
        
        self._persist_state()

        # Progress tracking
        self._progress_lock = asyncio.Lock()
        self._current_progress = self.start_progress

        # Track individual group progress for parallel execution
        self._group_progress = {
            "serial_measurements": {"completed": 0, "total": 0, "percent": 0.0},
            "parallel_thmgr_measurements": {"completed": 0, "total": 0, "percent": 0.0},
            "parallel_direct_measurements": {"completed": 0, "total": 0, "percent": 0.0},
        }

    def _resolve_job_id(self, job_id: str) -> str:
        if job_id and job_id.lower() != "auto":
            return job_id
        return uuid.uuid4().hex

    def _make_server(self, entry: Dict[str, Any]) -> Server:
        return Server(
            id=entry["id"],
            host=entry["host"],
            port=int(entry.get("port", 22)),
            user=entry["user"],
            ssh_key=entry.get("ssh_key"),
            max_concurrent_tasks=int(entry.get("max_concurrent_tasks", 4)),
            work_dir=entry.get("work_dir", "/tmp"),
        )

    def _resolve_analysis_file_path(self, analysis_file: str) -> Path:
        """Resolve analysis file to absolute path in repo directory."""
        path = Path(analysis_file)
        if path.is_absolute():
            return path
        return (self.repo_path / path).resolve()
        
    def _stage_required_files(self) -> None:
        logging.info("Staging workspace at %s", self.job_dir)
        copy_if_needed(self.worker_source, self.worker_dest)
        self.worker_dest.chmod(0o755)

        self._stage_binaries()
        self._stage_shared_files()
        self._stage_curve_inputs()

    def _stage_binaries(self) -> None:
        binaries = self.profiler_config.get("binaries", [])
        for entry in binaries:
            self._copy_from_repo(entry)

    def _stage_shared_files(self) -> None:
        shared_files = self.profiler_config.get("shared_files", [])
        for entry in shared_files:
            if isinstance(entry, dict):
                source = entry.get("source")
                if not source and "arg" in entry:
                    arg_name = entry["arg"]
                    source = getattr(self.args, arg_name, None)
                if not source:
                    logging.debug(
                        "Shared file entry %s missing source, skipping", entry
                    )
                    continue
                destination = entry.get("destination")
                self._copy_from_repo(source, destination)
                continue

            arg_value = getattr(self.args, entry, None)
            if arg_value:
                self._copy_from_repo(arg_value)
                continue

            self._copy_from_repo(entry)

    def _stage_curve_inputs(self) -> None:
        if not self.curve_types:
            return

        curve_overrides = self.profiler_config.get("curve_files", {})
        for curve_name in self.curve_types:
            sources: List[Union[str, Path]] = []
            override = (
                curve_overrides.get(curve_name)
                if isinstance(curve_overrides, dict)
                else None
            )
            if override:
                if isinstance(override, (list, tuple)):
                    sources.extend(override)
                else:
                    sources.append(override)
            else:
                sources.append(f"{curve_name}.json")

            errors: List[str] = []
            for source in sources:
                try:
                    self._copy_from_repo(source)
                except ConfigError as exc:
                    errors.append(str(exc))

            if errors:
                detail = "; ".join(errors)
                raise ConfigError(
                    f"Unable to stage curve input for '{curve_name}': {detail}"
                )

    def _copy_from_repo(
        self, source: Union[str, Path], destination: Optional[Union[str, Path]] = None
    ) -> None:
        src_path = self._resolve_source_path(Path(source))
        dest_relative = Path(destination) if destination else Path(source)
        dest_path = self._resolve_destination_path(dest_relative)
        copy_if_needed(src_path, dest_path)

    def _resolve_source_path(self, path: Path) -> Path:
        if path.is_absolute():
            if not path.exists():
                raise ConfigError(f"Required file not found: {path}")
            return path

        repo_candidate = (self.repo_path / path).resolve()
        if repo_candidate.exists():
            return repo_candidate

        cwd_candidate = (Path.cwd() / path).resolve()
        if cwd_candidate.exists():
            return cwd_candidate

        raise ConfigError(f"Required file not found: {(self.repo_path / path)}")

    def _resolve_destination_path(self, path: Path) -> Path:
        if path.is_absolute():
            path = Path(path.name)
        if ".." in path.parts:
            path = Path(path.name)
        return self.job_dir / path

    def _job_path_for(self, original: Optional[Union[str, Path]]) -> Optional[Path]:
        if not original:
            return None
        return self._resolve_destination_path(Path(original))

    def _load_power_profile(self) -> Dict[int, float]:
        profile: Dict[int, float] = {}
        if not self.power_profile_path or not self.power_profile_path.exists():
            logging.debug("Power profile file not staged; using empty profile")
            return profile

        try:
            with self.power_profile_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    parts = stripped.split(",")
                    if len(parts) < 2:
                        continue
                    try:
                        core = int(parts[0])
                        power = float(parts[1])
                        profile[core] = power
                    except ValueError:
                        logging.debug(
                            "Skipping malformed power profile line: %s", stripped
                        )
        except OSError as exc:
            logging.warning(
                "Unable to read power profile %s: %s", self.power_profile_path, exc
            )

        return profile

    def _persist_state(self) -> None:
        state_payload = {
            "job_id": self.job_id,
            "job_dir": str(self.job_dir),
            "temp_dir": str(self.temp_dir),
            "analysis_file": str(self.analysis_file),
            "repo": self.repo,
            "repo_name": self.repo_name,
            "start_time": self.start_time,
            "algo": self.algo,
            "algo_original": self.algo_original,
            "iva_data": self.iva_data,
            "thmgr_api": self.thmgr_api,
            "curve_types": self.curve_types,
        }
        write_json(self.state_file, state_payload)

    async def run(self) -> None:
        if self.args.mode == "measurement":
            await self._run_measurement_tasks()
        elif self.args.mode == "curve_fit":
            await self._run_curve_fit_tasks()
        else:
            raise ConfigError(f"Unsupported mode: {self.args.mode}")
        
    async def _run_measurement_tasks(self) -> None:
        logging.info("Starting distributed measurements")

        await self._update_progress(
            status="In progress",
            current_step="Distributed Measurements",
            next_step="Curve Fitting",
            percent=self._current_progress,
            include_parallel_groups=True,
        )

        # Run all measurement groups in parallel across different nodes
        # Pass parallel=True to change progress tracking behavior
        await asyncio.gather(
            self._dispatch_group("serial_measurements", parallel_mode=True),
            self._dispatch_group("parallel_thmgr_measurements", parallel_mode=True),
            self._dispatch_group("parallel_direct_measurements", parallel_mode=True)
        )

        # Update progress after all groups complete
        async with self._progress_lock:
            self._current_progress += (self.serial_progress +
                                      self.thmgr_progress +
                                      self.direct_progress)

        await self._update_progress(
            status="In progress",
            current_step="Curve Fitting",
            next_step="Complete",
            percent=self._current_progress,
        )

        measurements = self._collect_measurements()
        self._write_measurement_summary(measurements)

        logging.info("Measurement tasks finished")

    async def _run_curve_fit_tasks(self) -> None:
        if not self.curve_types:
            logging.info("No curve types provided, skipping curve fitting")
            return

        logging.info("Starting local curve fitting for %d types", len(self.curve_types))
        await self._update_progress(
            status="In progress",
            current_step="Curve Fitting",
            next_step="Analytics Generation",
            percent=self.start_progress,
        )

        await self._run_curve_fit_locally()

        final_percent = self.start_progress + self.curve_progress
        await self._update_progress(
            status="In progress",
            current_step="Analytics Generation",
            next_step="Complete",
            percent=final_percent,
        )

        logging.info("Curve fitting tasks finished")

    async def _run_curve_fit_locally(self) -> None:
        loop = asyncio.get_running_loop()
        for analysis in self.curve_types:
            await loop.run_in_executor(None, self._execute_local_curve_fit, analysis)

    def _execute_local_curve_fit(self, analysis: str) -> None:
        cmd = [
            "bash",
            str(self.worker_dest),
            "curve_fit",
            str(self.job_dir),
            self.fit_script_path,
            analysis,
        ]
        logging.debug(
            "Running local curve fit for %s with command: %s",
            analysis,
            " ".join(shlex.quote(part) for part in cmd),
        )
        process = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if process.stdout:
            logging.debug("Curve fit %s stdout: %s", analysis, process.stdout.strip())
        if process.stderr:
            logging.debug("Curve fit %s stderr: %s", analysis, process.stderr.strip())
        if process.returncode != 0:
            raise TaskError(
                f"Local curve fit for {analysis} failed with rc={process.returncode}: {process.stderr.strip()}"
            )
        logging.info("Local curve fit completed for %s", analysis)

    async def _dispatch_group(self, group_name: str, parallel_mode: bool = True) -> None:
        tasks = self._build_tasks_for_group(group_name)
        if not tasks:
            logging.info("No tasks for group %s", group_name)
            return

        # strategy = self.task_config["task_groups"][group_name]["distribution_strategy"]
        # self._assign_servers(tasks, strategy)
        task_meta = self.task_config["task_groups"][group_name]
        strategy = task_meta.get("distribution_strategy", "round_robin")
        self._assign_servers(group_name, task_meta, tasks, strategy)

        # Determine progress allocation for this group
        group_progress_map = {
            "serial_measurements": self.serial_progress,
            "parallel_thmgr_measurements": self.thmgr_progress,
            "parallel_direct_measurements": self.direct_progress,
        }
        group_progress_total = group_progress_map.get(group_name, 0)
        progress_per_task = group_progress_total / len(tasks) if tasks else 0

        # Initialize group progress tracking
        if group_name in self._group_progress:
            self._group_progress[group_name]["total"] = len(tasks)
            self._group_progress[group_name]["completed"] = 0
            self._group_progress[group_name]["percent"] = 0.0

        # Track completed tasks
        completed_count = 0
        completed_lock = asyncio.Lock()

        async def execute_and_track(task: Task, idx: int) -> None:
            nonlocal completed_count
            await self._execute_task_with_delay(task, idx)

            # Update progress after task completes
            async with completed_lock:
                completed_count += 1

                # Update individual group progress
                if group_name in self._group_progress:
                    self._group_progress[group_name]["completed"] = completed_count
                    self._group_progress[group_name]["percent"] = round(
                        (completed_count / len(tasks)) * 100, 2
                    )

                # In parallel mode, update with group details but don't increment global progress
                if parallel_mode:
                    # Periodically send progress updates with parallel group details
                    update_frequency = max(1, len(tasks) // 10)
                    if completed_count % update_frequency == 0 or completed_count == len(tasks):
                        await self._update_progress(
                            status="In progress",
                            current_step="Distributed Measurements",
                            next_step="Curve Fitting",
                            percent=self._current_progress,
                            include_parallel_groups=True,
                        )
                else:
                    # Sequential mode: update global progress per task
                    async with self._progress_lock:
                        self._current_progress += progress_per_task
                        # Update progress periodically (every 10% of tasks or every task if <10 tasks)
                        update_frequency = max(1, len(tasks) // 10)
                        if completed_count % update_frequency == 0 or completed_count == len(tasks):
                            await self._update_progress(
                                status="In progress",
                                current_step=self._get_group_step_name(group_name),
                                next_step=self._get_group_next_step(group_name),
                                percent=self._current_progress,
                            )
        coroutines = [
            execute_and_track(task, idx) for idx, task in enumerate(tasks)
        ]
        results = await asyncio.gather(*coroutines, return_exceptions=True)

        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            errors = ", ".join(str(failure) for failure in failures)
            raise TaskError(f"Group {group_name} failed: {errors}")

    def _get_group_step_name(self, group_name: str) -> str:
        """Get human-readable step name for a group."""
        step_names = {
            "serial_measurements": "Serial Measurements (Distributed)",
            "parallel_thmgr_measurements": "Parallel Time Optimized Measurements",
            "parallel_direct_measurements": "Parallel Time Direct Measurements",
        }
        return step_names.get(group_name, group_name)

    def _get_group_next_step(self, group_name: str) -> str:
        """Get next step name for a group."""
        next_steps = {
            "serial_measurements": "Parallel Time Optimized Measurements",
            "parallel_thmgr_measurements": "Parallel Time Direct Measurements",
            "parallel_direct_measurements": "Data Collection",
        }
        return next_steps.get(group_name, "Next Phase")
    
    def _get_server_by_id(self, server_id: str) -> Server:
        for server in self.servers:
            if server.id == server_id:
                return server
        raise ConfigError(f"Unknown server id '{server_id}' for assignment")

    def _build_tasks_for_group(self, group_name: str) -> List[Task]:
        task_meta = self.task_config["task_groups"].get(group_name)
        if not task_meta:
            raise ConfigError(f"Unknown task group: {group_name}")

        timeout = int(task_meta.get("timeout", 600))
        retries = int(task_meta.get("retry_on_failure", 0))
        tasks: List[Task] = []
        worker_path = str(self.worker_dest)

        if group_name == "serial_measurements":
            for idx, iva in enumerate(self.iva_values):
                serial_time_args = [
                    "bash",
                    worker_path,
                    "serial_time",
                    str(self.job_dir),
                    str(self.temp_dir),
                    self.algo_original,
                    str(iva),
                    str(idx),
                ]
                tasks.append(
                    Task(
                        name=f"serial-time-{idx}",
                        group=group_name,
                        remote_args=serial_time_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )

                serial_mem_args = [
                    "bash",
                    worker_path,
                    "serial_memory",
                    str(self.job_dir),
                    str(self.temp_dir),
                    self.algo_original,
                    str(iva),
                    str(idx),
                ]
                tasks.append(
                    Task(
                        name=f"serial-mem-{idx}",
                        group=group_name,
                        remote_args=serial_mem_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )
        elif group_name == "parallel_thmgr_measurements":
            parallel_iva = self.iva_values[-1] if self.iva_values else str(self.iva_data)
            for idx, core in enumerate(self.core_values):
                remote_args = [
                    "bash",
                    worker_path,
                    "thmgr",
                    str(self.job_dir),
                    str(self.temp_dir),
                    self.repo_name,
                    str(core),
                    str(parallel_iva),
                    self.thmgr_api,
                    str(idx),
                ]
                tasks.append(
                    Task(
                        name=f"thmgr-{idx}",
                        group=group_name,
                        remote_args=remote_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )
        elif group_name == "parallel_direct_measurements":
            parallel_iva = self.iva_values[-1] if self.iva_values else str(self.iva_data)
            for idx, core in enumerate(self.core_values):
                parallel_time_args = [
                    "bash",
                    worker_path,
                    "parallel_time",
                    str(self.job_dir),
                    str(self.temp_dir),
                    self.algo,
                    str(core),
                    str(parallel_iva),
                    str(idx),
                ]
                tasks.append(
                    Task(
                        name=f"parallel-time-{idx}",
                        group=group_name,
                        remote_args=parallel_time_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )

                parallel_mem_args = [
                    "bash",
                    worker_path,
                    "parallel_memory",
                    str(self.job_dir),
                    str(self.temp_dir),
                    self.algo,
                    str(core),
                    str(parallel_iva),
                    str(idx),
                ]
                tasks.append(
                    Task(
                        name=f"parallel-mem-{idx}",
                        group=group_name,
                        remote_args=parallel_mem_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )
        elif group_name == "curve_fitting":
            for analysis in self.curve_types:
                remote_args = [
                    "bash",
                    worker_path,
                    "curve_fit",
                    str(self.job_dir),
                    self.fit_script_path,
                    analysis,
                ]
                tasks.append(
                    Task(
                        name=f"curve-{analysis}",
                        group=group_name,
                        remote_args=remote_args,
                        timeout=timeout,
                        retries=retries,
                    )
                )
        else:
            raise ConfigError(f"Unsupported task group: {group_name}")

        return tasks

    def _assign_servers(
        self,
        group_name: str,
        task_meta: Dict[str, Any],
        tasks: List[Task],
        strategy: str,
    ) -> None:
        if not self.servers:
            raise ConfigError("No servers configured")

        # if strategy != "round_robin":
        #     raise ConfigError(f"Unsupported distribution strategy: {strategy}")

        # for idx, task in enumerate(tasks):
        #     server = self.servers[idx % len(self.servers)]
        #     task.server = server
        #     logging.debug("Assigned task %s to server %s", task.name, server.id)
        assigned_server_id = task_meta.get("assigned_server")
        if assigned_server_id:
            server = self._get_server_by_id(assigned_server_id)
            for task in tasks:
                task.server = server
                logging.debug(
                    "Assigned task %s in group %s to dedicated server %s",
                    task.name,
                    group_name,
                    server.id,
                )
            return

        if strategy != "round_robin":
            raise ConfigError(f"Unsupported distribution strategy: {strategy}")

        for idx, task in enumerate(tasks):
            server = self.servers[idx % len(self.servers)]
            task.server = server
            logging.debug("Assigned task %s to server %s", task.name, server.id)

    async def _execute_task_with_delay(self, task: Task, position: int) -> None:
        if self.request_delay > 0:
            await asyncio.sleep(position * self.request_delay)
        await self._execute_task(task)

    async def _execute_task(self, task: Task) -> None:
        if not task.server:
            raise ConfigError(f"Task {task.name} has no assigned server")

        server = task.server
        command_str = " ".join(shlex.quote(arg) for arg in task.remote_args)
        ssh_cmd = server.base_command() + [command_str]

        attempt = 0
        while attempt <= task.retries:
            attempt += 1
            async with server.semaphore:
                logging.debug(
                    "Running task %s on %s attempt %d", task.name, server.id, attempt
                )
                process = await asyncio.create_subprocess_exec(
                    *ssh_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=task.timeout
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    await process.communicate()
                    logging.warning(
                        "Task %s timed out on server %s", task.name, server.id
                    )
                    if attempt > task.retries:
                        raise TaskError(
                            f"Task {task.name} timed out on server {server.id}"
                        )
                    continue

            if process.returncode == 0:
                if stdout:
                    logging.debug(
                        "Task %s stdout: %s", task.name, stdout.decode().strip()
                    )
                if stderr:
                    logging.debug(
                        "Task %s stderr: %s", task.name, stderr.decode().strip()
                    )
                logging.info("Task %s completed on %s", task.name, server.id)
                return

            logging.error(
                "Task %s failed on %s (attempt %d/%d) rc=%s stderr=%s",
                task.name,
                server.id,
                attempt,
                task.retries,
                process.returncode,
                stderr.decode().strip(),
            )
            if attempt > task.retries:
                raise TaskError(
                    f"Task {task.name} failed on server {server.id} "
                    f"after {task.retries} retries (rc={process.returncode})"
                )
            await asyncio.sleep(1)

    def _read_measurement_series(
        self, prefix: str, count: int, epsilon: float = 1e-6
    ) -> List[float]:
        series: List[float] = []
        for idx in range(count):
            path = self.temp_dir / f"{prefix}_{idx}.tmp"
            if not path.exists():
                logging.warning("Missing measurement file %s", path)
                series.append(epsilon)
                continue

            try:
                content = path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                logging.warning("Unable to read measurement file %s: %s", path, exc)
                series.append(epsilon)
                continue

            parts = content.split(":")
            if len(parts) != 3:
                logging.warning(
                    "Unexpected measurement format in %s: %s", path, content
                )
                series.append(epsilon)
                continue

            try:
                value = float(parts[2])
            except ValueError:
                logging.warning("Non-numeric measurement in %s: %s", path, parts[2])
                value = epsilon

            if value <= 0:
                value = epsilon

            series.append(value)
        return series

    def _collect_measurements(self) -> Dict[str, List[float]]:
        iva_numeric = [float(v.split(',')[0]) for v in self.iva_values] if self.iva_values else []
        core_numeric = [int(c) for c in self.core_values] if self.core_values else []

        serial_time = self._read_measurement_series("serial_time", len(iva_numeric))
        serial_space = self._read_measurement_series("serial_space", len(iva_numeric))
        parallel_time = self._read_measurement_series(
            "parallel_time", len(core_numeric)
        )
        parallel_time_slow = self._read_measurement_series(
            "parallel_time_slow", len(core_numeric)
        )
        parallel_space = self._read_measurement_series(
            "parallel_space", len(core_numeric)
        )

        default_power = next(iter(self.power_profile_map.values()), 0.0)
        power_serial = [default_power] * len(serial_time)
        power_parallel = [
            self.power_profile_map.get(core, default_power) for core in core_numeric
        ]

        energy_serial = [
            round(time * power, 8) for time, power in zip(serial_time, power_serial)
        ]
        energy_parallel = [
            round(time * power, 8) for time, power in zip(parallel_time, power_parallel)
        ]

        base_parallel_time = parallel_time[0] if parallel_time else 1.0
        base_parallel_space = parallel_space[0] if parallel_space else 1.0
        base_power = power_parallel[0] if power_parallel else default_power or 1.0
        base_energy = (
            energy_parallel[0] if energy_parallel else (base_parallel_time * base_power)
        )

        def safe_div(numerator: float, denominator: float) -> float:
            return round(numerator / denominator, 6) if denominator else 0.0

        speedup = [safe_div(base_parallel_time, value) for value in parallel_time]
        freeup = [safe_div(base_parallel_space, value) for value in parallel_space]
        powerup = [safe_div(base_power, value) for value in power_parallel]
        energyup = [safe_div(base_energy, value) for value in energy_parallel]

        measurements = {
            "iva": iva_numeric,
            "core": core_numeric,
            "time_serial": serial_time,
            "space_serial": serial_space,
            "power_serial": power_serial,
            "energy_serial": energy_serial,
            "time_parallel": parallel_time,
            "time_parallel_slow": parallel_time_slow,
            "space_parallel": parallel_space,
            "power_parallel": power_parallel,
            "energy_parallel": energy_parallel,
            "speedup": speedup,
            "freeup": freeup,
            "powerup": powerup,
            "energyup": energyup,
        }

        logging.debug(
            "Collected measurements:\n %s", json.dumps(measurements, indent=2)
        )

        return measurements

    def _write_measurement_summary(self, measurements: Dict[str, List[float]]) -> None:
        try:
            write_json(self.measurement_summary_path, measurements)
            logging.info(
                "Measurement summary written to %s", self.measurement_summary_path
            )
        except OSError as exc:
            logging.warning(
                "Unable to write measurement summary %s: %s",
                self.measurement_summary_path,
                exc,
            )

    async def _update_progress(
        self, status: str, current_step: str, next_step: str, percent: float,
        include_parallel_groups: bool = True
    ) -> None:
        parallel_groups = None
        if include_parallel_groups:
            # Include detailed parallel group progress
            parallel_groups = {
                "serialMeasurements": self._group_progress["serial_measurements"],
                "parallelThmgrMeasurements": self._group_progress["parallel_thmgr_measurements"],
                "parallelDirectMeasurements": self._group_progress["parallel_direct_measurements"],
            }

        record = render_progress_record(
            job_id=self.job_id,
            repo=self.repo,
            repo_name=self.repo_name,
            start_time=self.start_time,
            status=status,
            current_step=current_step,
            next_step=next_step,
            percent=percent,
            parallel_groups=parallel_groups,
        )
        ensure_dir(self.analysis_file.parent)
        tmp_path = self.analysis_file.with_suffix(".tmp")
        write_json(tmp_path, record)
        tmp_path.replace(self.analysis_file)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distributed profiler controller")

    parser.add_argument(
        "--cluster-config", required=True, help="Path to cluster configuration JSON"
    )
    parser.add_argument(
        "--task-config", required=True, help="Path to task distribution JSON"
    )
    parser.add_argument(
        "--profiler-config", required=True, help="Path to profiler JSON"
    )
    parser.add_argument("--worker-script", required=True, help="Path to worker script")
    parser.add_argument(
        "--fit-script",
        default="/usr/bin/fit.py",
        help="Path to the fit script executable (defaults to /usr/bin/fit.py)",
    )

    parser.add_argument("--job-id", default="auto", help="Job identifier")
    parser.add_argument("--repo", required=True, help="Repository identifier")
    parser.add_argument("--repo-name", required=True, help="Repository name")
    parser.add_argument(
        "--repo-path", required=True, help="Path to the repository root"
    )
    parser.add_argument(
        "--analysis-file", required=True, help="Path to analysis status file"
    )
    parser.add_argument("--start-time", required=True, help="Job start timestamp")
    parser.add_argument(
        "--start-progress", default="0", help="Starting progress percent"
    )
    parser.add_argument(
        "--job-root", help="Optional relative job root directory within shared storage"
    )

    parser.add_argument("--algo", required=True, help="Optimized binary name")
    parser.add_argument(
        "--algo-original", required=True, help="Original serial binary name"
    )
    parser.add_argument(
        "--iva-data", required=True, help="IVA data argument passed to binaries"
    )
    parser.add_argument(
        "--thmgr-api", required=True, help="Thread manager API base URL"
    )

    parser.add_argument(
        "--iva-values", nargs="+", default=[], help="List of IVA values"
    )
    parser.add_argument(
        "--core-values", nargs="+", default=[], help="List of core counts"
    )
    parser.add_argument(
        "--curve-types",
        nargs="+",
        default=[],
        help="List of analysis base names for curve fitting (e.g. time-serial)",
    )

    parser.add_argument("--iva-data-file", help="Path to IVA data file")
    parser.add_argument(
        "--core_count_file", dest="core_count_file", help="Path to core count file"
    )
    parser.add_argument("--power_profile_file", help="Path to power profile file")

    parser.add_argument(
        "--serial-progress",
        default="25",
        help="Progress percent awarded to serial measurements",
    )
    parser.add_argument(
        "--thmgr-progress",
        default="20",
        help="Progress percent awarded to THMGR measurements",
    )
    parser.add_argument(
        "--direct-progress",
        default="20",
        help="Progress percent awarded to direct measurements",
    )
    parser.add_argument(
        "--curve-progress",
        default="25",
        help="Progress percent awarded to curve fitting",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between launching remote execution requests",
    )

    parser.add_argument(
        "--mode", choices=["measurement", "curve_fit"], default="measurement"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase log verbosity"
    )

    args = parser.parse_args(argv)
    setup_logger(args.verbose)
    return args


def main(argv: Sequence[str]) -> int:
    try:
        args = parse_args(argv)
        setup_logger(args.verbose)
        try:
            args_dict = {key: getattr(args, key) for key in vars(args)}
            logging.debug("Parsed arguments: %s", args_dict)
        except Exception:
            logging.debug("Parsed arguments (unable to serialize for logging): %s", args)

        profiler = DistributedProfiler(args)
        asyncio.run(profiler.run())
    except (ConfigError, TaskError) as exc:
        logging.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
