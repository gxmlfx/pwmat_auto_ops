#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Requires Python 3.8+. On MCloud, invoke this file with `python3`.
"""Parallel PWMAT dielectric-function workflow for MCloud/Slurm.

The script keeps one time point's steps sequential, while different time points
run in parallel. Every Slurm submission waits only for its own JobID. All file
operations use an explicit working directory; os.chdir() is never used.

Run a detached full calculation:
    python3 pwmat_dielectric_parallel.py --daemon

Watch progress:
    tail -f "$BASE_DIR/logs/pwmat_parallel.log"
    watch -n 5 'python3 pwmat_dielectric_parallel.py --show-status'
"""

from __future__ import annotations

import argparse
import atexit
import fcntl
import getpass
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


# ============================================================================
# User configuration: edit these values directly when running on MCloud.
# ============================================================================
DEFAULT_BASE_DIR = "/data/home/wangl/2.41ev,515nm/515nm_0.024_200fs"
DEFAULT_SOURCE_DIR = "/data/home/wangl/pwmat_auto_ops/sources"
DEFAULT_GAP_NAME = "gap"
DEFAULT_DIELECTRIC_NAME = "dielectric"

# Time-point selection used by the smart/default workflow when no CLI override is
# supplied. For example, 0, 4, 8, ..., 60.
STEP_START = 56
STEP_END = 200
STEP_LEN = 2
CALC_TIMES = list(range(STEP_START, STEP_END + 1, STEP_LEN))
# For a non-uniform set, replace the line above with an explicit list, e.g.
# CALC_TIMES = [0, 4, 8, 12, 16, 24, 32, 48, 60]

MAX_PARALLEL = 4
POLL_SECONDS = 20.0
JOB_HEARTBEAT_SECONDS = 60.0
SBATCH_MAX_ATTEMPTS = 5
SBATCH_RETRY_SECONDS = 60.0
FINAL_DIELECTRIC_FILE = "dielectric_imag.dat"
# ============================================================================

LOG = logging.getLogger("pwmat-pipeline")

GAP_SOURCE_FILES = [
    "etot.input",
    "etot.input2",
    "etot.input3",
    "etot.input4",
    "gen.kpt",
    "sub.sh",
    "O.SG15.PBE.UPF",
    "Si.SG15.PBE.UPF",
    "RPA.input",
    "RPA.sh",
]
DIELECTRIC_SOURCE_FILES = [
    "etot1.input-3-SCF",
    "etot.input-4-DOS",
    "sub.sh",
    "O.SG15.PBE.UPF",
    "Si.SG15.PBE.UPF",
    "RPA.input",
    "RPA.sh",
]

TERMINAL_FAILURE_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "TIMEOUT",
}
ACTIVE_STATES = {
    "COMPLETING",
    "CONFIGURING",
    "PENDING",
    "REQUEUED",
    "RESIZING",
    "RUNNING",
    "SUSPENDED",
}


class PipelineError(RuntimeError):
    """Raised when a workflow stage cannot complete safely."""


class CommandFailed(PipelineError):
    """Raised when a local command or sbatch call fails."""


def now_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")


def configure_logging(log_path: Path, console: bool) -> None:
    LOG.setLevel(logging.INFO)
    LOG.handlers.clear()
    LOG.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOG.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        LOG.addHandler(console_handler)


def daemonize(log_path: Path) -> None:
    """Detach the process from the controlling terminal and redirect stdio."""
    if os.name != "posix":
        raise PipelineError("--daemon is only supported on POSIX systems")

    first_pid = os.fork()
    if first_pid > 0:
        os._exit(0)

    os.setsid()

    second_pid = os.fork()
    if second_pid > 0:
        os._exit(0)

    os.umask(0o022)
    os.chdir("/")

    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    log_fd = os.open(
        str(log_path),
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o644,
    )
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    if log_fd > 2:
        os.close(log_fd)


class PidLock:
    """A non-blocking process lock that prevents duplicate orchestrators."""

    def __init__(self, path: Path):
        self.path = path
        self._file = None
        self._owned = False

    def __enter__(self) -> "PidLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a+", encoding="utf-8")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._file.seek(0)
            other_pid = self._file.read().strip() or "unknown"
            self._file.close()
            self._file = None
            raise PipelineError(
                "Another orchestrator is already running "
                f"(PID file: {self.path}, PID: {other_pid})"
            )

        self._file.seek(0)
        self._file.truncate()
        self._file.write(str(os.getpid()) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())
        self._owned = True
        atexit.register(self.close)
        return self

    def close(self) -> None:
        if self._file is None:
            return
        if self._owned:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        self._file.close()
        self._file = None

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def atomic_write_json(path: Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(
        f".{path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
    )
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


class StatusStore:
    """Thread-safe JSON status used by --show-status and external watch."""

    def __init__(
        self,
        path: Path,
        times: Sequence[int],
        previous_data: Optional[Dict] = None,
    ):
        self.path = path
        self._lock = threading.Lock()
        if previous_data and isinstance(previous_data.get("times"), dict):
            self._data = json.loads(json.dumps(previous_data, ensure_ascii=False))
            self._data["pid"] = os.getpid()
            self._data["resumed_at"] = now_text()
            self._data["updated_at"] = now_text()
        else:
            self._data = {
                "pid": os.getpid(),
                "started_at": now_text(),
                "updated_at": now_text(),
                "times": {},
            }

        for t in times:
            self._data["times"].setdefault(
                str(t),
                {
                    "status": "pending",
                    "stage": "",
                    "job_id": "",
                    "slurm_state": "",
                    "message": "",
                    "updated_at": now_text(),
                },
            )
        self._write_locked()

    def _write_locked(self) -> None:
        self._data["updated_at"] = now_text()
        try:
            atomic_write_json(self.path, self._data)
        except OSError as exc:
            LOG.warning("Could not write status file %s: %s", self.path, exc)

    def update(self, t: int, **changes) -> None:
        with self._lock:
            item = self._data["times"].setdefault(str(t), {})
            item.update(changes)
            item["updated_at"] = now_text()
            self._write_locked()

    def set_overall(self, **changes) -> None:
        with self._lock:
            self._data.update(changes)
            self._write_locked()

    def snapshot(self) -> Dict:
        with self._lock:
            return json.loads(json.dumps(self._data, ensure_ascii=False))


class Context:
    def __init__(self, args: argparse.Namespace, times: Sequence[int]):
        self.args = args
        self.times = list(times)
        self.base_dir = Path(args.base_dir).expanduser().resolve()
        self.source_dir = Path(args.source_dir).expanduser().resolve()
        self.tddos_dir = (
            Path(args.tddos_dir).expanduser().resolve()
            if args.tddos_dir
            else self.base_dir / "TDDOS"
        )
        self.gap_dir = self.base_dir / args.gap_name
        self.dielectric_dir = self.base_dir / args.dielectric_name
        self.log_dir = (
            Path(args.log_dir).expanduser().resolve()
            if args.log_dir
            else self.base_dir / "logs"
        )
        self.log_file = self.log_dir / "pwmat_parallel.log"
        self.pid_file = self.log_dir / "pwmat_parallel.pid"
        self.status_file = self.log_dir / "pwmat_parallel_status.json"
        self.previous_status = getattr(args, "previous_status", None) or {}
        self.status = StatusStore(
            self.status_file,
            self.times,
            previous_data=self.previous_status,
        )
        self.poll_seconds = float(args.poll_seconds)
        self.job_timeout_hours = float(args.job_timeout_hours)
        self.sbatch_max_attempts = max(1, int(args.sbatch_max_attempts))
        self.sbatch_retry_seconds = max(0.0, float(args.sbatch_retry_seconds))
        self.generate_occ_lock = threading.Lock()


def ensure_file(path: Path, description: str = "file") -> None:
    if not path.is_file():
        raise PipelineError(f"Required {description} does not exist: {path}")


def ensure_dir(path: Path, description: str = "directory") -> None:
    if not path.is_dir():
        raise PipelineError(f"Required {description} does not exist: {path}")


def copy_file(src: Path, dest: Path) -> None:
    ensure_file(src)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(dest))
    LOG.info("Copied %s -> %s", src, dest)


def scientific_time_label(t: int) -> str:
    """Match the TDDOS suffixes used by the original script."""
    if t == 0:
        return "0.000000E+00"
    if t < 10:
        return f"0.{t:1d}00000E+01"
    if t < 100:
        return f"0.{t:02d}0000E+02"
    if t < 1000:
        return f"0.{t:03d}000E+03"
    if t < 10000:
        return f"0.{t:04d}00E+04"
    raise ValueError(f"Time point {t} exceeds the supported four digits")


def times_from_args(args: argparse.Namespace) -> List[int]:
    if args.time is not None and args.times:
        raise PipelineError("Use either --time or --times, not both")
    if args.time is not None:
        return [int(args.time)]
    if args.times:
        result: List[int] = []
        for token in str(args.times).split(","):
            token = token.strip()
            if token:
                result.append(int(token))
        if not result:
            raise PipelineError("--times did not contain any integer")
        return sorted(set(result))
    return list(CALC_TIMES)


def unique_backup_path(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = path.with_name(f"{path.name}_bak_{stamp}")
    index = 1
    while candidate.exists() or candidate.is_symlink():
        candidate = path.with_name(f"{path.name}_bak_{stamp}_{index}")
        index += 1
    return candidate


def preflight_timepoint(ctx: Context, t: int) -> None:
    for fname in GAP_SOURCE_FILES:
        ensure_file(ctx.source_dir / fname, f"gap source {fname}")
    for fname in DIELECTRIC_SOURCE_FILES:
        ensure_file(ctx.source_dir / fname, f"dielectric source {fname}")

    ensure_dir(ctx.tddos_dir, "TDDOS directory")
    suffix = scientific_time_label(t)
    ensure_file(ctx.tddos_dir / f"OUT.RHO.{suffix}", f"TDDOS rho for t={t}")
    ensure_file(
        ctx.tddos_dir / f"restart.config.{suffix}",
        f"TDDOS restart.config for t={t}",
    )


def preflight_workspace(ctx: Context) -> None:
    for t in ctx.times:
        preflight_timepoint(ctx, t)


def prepare_workspace(ctx: Context) -> None:
    """Create fresh run directories and copy all per-time input files."""
    preflight_workspace(ctx)

    for directory in (ctx.gap_dir, ctx.dielectric_dir):
        if directory.exists() or directory.is_symlink():
            backup = unique_backup_path(directory)
            directory.rename(backup)
            LOG.info("Moved existing directory %s -> %s", directory, backup)
        directory.mkdir(parents=True, exist_ok=True)

    for t in ctx.times:
        subdir = ctx.gap_dir / str(t)
        subdir.mkdir(parents=True, exist_ok=True)
        for fname in GAP_SOURCE_FILES:
            copy_file(ctx.source_dir / fname, subdir / fname)

    for t in ctx.times:
        subdir = ctx.dielectric_dir / str(t)
        subdir.mkdir(parents=True, exist_ok=True)
        for fname in DIELECTRIC_SOURCE_FILES:
            copy_file(ctx.source_dir / fname, subdir / fname)

    for t in ctx.times:
        suffix = scientific_time_label(t)
        rho_src = ctx.tddos_dir / f"OUT.RHO.{suffix}"
        config_src = ctx.tddos_dir / f"restart.config.{suffix}"
        for destination_root in (ctx.gap_dir, ctx.dielectric_dir):
            subdir = destination_root / str(t)
            copy_file(rho_src, subdir / "IN.RHO")
            copy_file(config_src, subdir / "atom.config")

    ctx.status.set_overall(
        status="prepared",
        updated_at=now_text(),
        gap_dir=str(ctx.gap_dir),
        dielectric_dir=str(ctx.dielectric_dir),
    )
    LOG.info("Workspace prepared: %s, %s", ctx.gap_dir, ctx.dielectric_dir)


def run_command(
    argv: Sequence[str],
    cwd: Path,
    stage: str,
    t: Optional[int] = None,
    input_text: Optional[str] = None,
) -> str:
    """Run a command with explicit cwd, capture output, and log it."""
    tag = f"t={t} {stage}" if t is not None else stage
    LOG.info("[%s] Running: %s (cwd=%s)", tag, shlex.join(list(argv)), cwd)
    try:
        result = subprocess.run(
            list(argv),
            cwd=str(cwd),
            input=input_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise CommandFailed(
            f"[{tag}] Command not found: {argv[0]}. "
            "MCloud normally provides the loaded PWMAT environment; check PATH."
        ) from exc

    output = result.stdout or ""
    for line in output.splitlines():
        if line.strip():
            LOG.info("[%s] %s", tag, line.rstrip())

    if result.returncode != 0:
        detail_lines = [line for line in output.splitlines() if line.strip()][-20:]
        detail = "\n".join(detail_lines)
        message = (
            f"[{tag}] Command failed with exit code {result.returncode}: "
            f"{shlex.join(list(argv))}"
        )
        if detail:
            message += f"\nCommand output:\n{detail}"
        raise CommandFailed(message)
    return output


def parse_job_id(sbatch_output: str) -> str:
    text = sbatch_output.strip()
    match = re.search(r"(?m)^\s*(\d+)(?:;[^\s]+)?\s*$", text)
    if not match:
        match = re.search(r"Submitted batch job\s+(\d+)", text)
    if not match:
        raise PipelineError(f"Could not parse Slurm JobID from: {text!r}")
    return match.group(1)


def clean_state(state: str) -> str:
    return state.strip().split()[0].rstrip("+").upper() if state.strip() else ""


def query_squeue_state(job_id: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["squeue", "-h", "-j", job_id, "-o", "%T"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise PipelineError("squeue was not found in PATH") from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise PipelineError(f"squeue -j {job_id} failed: {stderr}")

    for line in (result.stdout or "").splitlines():
        state = clean_state(line)
        if state:
            return state
    return None


def query_sacct_state(job_id: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        result = subprocess.run(
            [
                "sacct",
                "-n",
                "-P",
                "-j",
                job_id,
                "--format=JobID,State,ExitCode",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise PipelineError("sacct was not found in PATH") from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        LOG.warning("sacct -j %s failed: %s", job_id, stderr)
        return None, None

    rows: List[Tuple[str, str, str]] = []
    for line in (result.stdout or "").splitlines():
        parts = line.strip().split("|")
        if len(parts) >= 3 and parts[0]:
            rows.append((parts[0], parts[1], parts[2]))

    exact = [row for row in rows if row[0] == job_id]
    batch = [row for row in rows if row[0] == f"{job_id}.batch"]
    candidates = exact or batch or rows
    if not candidates:
        return None, None

    _, state, exit_code = candidates[0]
    return clean_state(state), exit_code.strip()


def wait_for_job(ctx: Context, t: int, stage: str, job_id: str) -> None:
    """Wait for one specific JobID, never for all jobs owned by the user."""
    LOG.info("Waiting for JobID %s (t=%s, stage=%s)", job_id, t, stage)
    deadline = (
        time.monotonic() + ctx.job_timeout_hours * 3600.0
        if ctx.job_timeout_hours > 0
        else None
    )
    last_state: Optional[str] = None
    last_log_time = 0.0

    while True:
        if deadline is not None and time.monotonic() >= deadline:
            subprocess.run(["scancel", job_id], check=False)
            raise PipelineError(
                f"JobID {job_id} exceeded {ctx.job_timeout_hours:g} hours; "
                "it was cancelled by the orchestrator"
            )

        state = query_squeue_state(job_id)
        if state:
            if state in TERMINAL_FAILURE_STATES:
                raise PipelineError(
                    f"JobID {job_id} reported failure state {state} in squeue"
                )

            if state not in ACTIVE_STATES and state != "COMPLETED":
                LOG.warning("Unexpected squeue state %s for JobID %s", state, job_id)

            if (
                state != last_state
                or time.monotonic() - last_log_time >= JOB_HEARTBEAT_SECONDS
            ):
                LOG.info("JobID %s state: %s (t=%s, stage=%s)", job_id, state, t, stage)
                last_state = state
                last_log_time = time.monotonic()
                ctx.status.update(
                    t,
                    status="running",
                    stage=stage,
                    job_id=job_id,
                    slurm_state=state,
                    message=f"Slurm job {job_id} is {state}",
                )
            time.sleep(ctx.poll_seconds)
            continue

        # The job has left squeue. Accounting may lag briefly, so retry
        # sacct instead of treating an empty result as success.
        final_state: Optional[str] = None
        exit_code: Optional[str] = None
        for _ in range(20):
            final_state, exit_code = query_sacct_state(job_id)
            if final_state:
                break
            time.sleep(1.0)

        if not final_state:
            raise PipelineError(
                f"JobID {job_id} left squeue, but sacct returned no final state"
            )

        ctx.status.update(
            t,
            status="running",
            stage=stage,
            job_id=job_id,
            slurm_state=final_state,
            message=f"Slurm job {job_id} finished as {final_state}",
        )
        LOG.info(
            "JobID %s final state: %s, ExitCode=%s",
            job_id,
            final_state,
            exit_code or "unknown",
        )

        if final_state == "COMPLETED" and (exit_code in (None, "", "0:0")):
            return
        if final_state == "COMPLETED" and exit_code not in (None, "", "0:0"):
            raise PipelineError(
                f"JobID {job_id} reported COMPLETED but ExitCode={exit_code}"
            )
        raise PipelineError(
            f"JobID {job_id} failed with state={final_state}, ExitCode={exit_code}"
        )


def is_transient_sbatch_error(exc: Exception) -> bool:
    """Return True only for submission failures known to be transient."""
    text = str(exc).lower()
    transient_markers = (
        "failed to query the balance",
        "查询余额失败",
        "socket timed out",
        "resource temporarily unavailable",
        "temporarily unable to accept job",
    )
    return any(marker in text for marker in transient_markers)


def submit_and_wait(ctx: Context, t: int, workdir: Path, script: str, stage: str) -> str:
    ensure_file(workdir / script, f"{script} in {workdir}")
    ctx.status.update(
        t,
        status="running",
        stage=stage,
        job_id="",
        slurm_state="",
        message=f"Submitting {script}",
    )

    output = ""
    for attempt in range(1, ctx.sbatch_max_attempts + 1):
        try:
            output = run_command(
                ["sbatch", "--parsable", script], workdir, stage, t=t
            )
            break
        except CommandFailed as exc:
            can_retry = (
                attempt < ctx.sbatch_max_attempts
                and is_transient_sbatch_error(exc)
            )
            if not can_retry:
                raise
            LOG.warning(
                "Transient sbatch failure for t=%s stage=%s "
                "(attempt %s/%s); retrying in %.0f seconds",
                t,
                stage,
                attempt,
                ctx.sbatch_max_attempts,
                ctx.sbatch_retry_seconds,
            )
            ctx.status.update(
                t,
                status="running",
                stage=stage,
                job_id="",
                slurm_state="SUBMIT_RETRY",
                message=(
                    f"sbatch attempt {attempt}/{ctx.sbatch_max_attempts} failed; "
                    f"retrying in {ctx.sbatch_retry_seconds:.0f}s"
                ),
            )
            if ctx.sbatch_retry_seconds:
                time.sleep(ctx.sbatch_retry_seconds)

    job_id = parse_job_id(output)
    LOG.info("Submitted JobID %s for t=%s, stage=%s", job_id, t, stage)
    ctx.status.update(
        t,
        status="running",
        stage=stage,
        job_id=job_id,
        slurm_state="SUBMITTED",
        message=f"Submitted Slurm job {job_id}",
    )
    wait_for_job(ctx, t, stage, job_id)
    return job_id


def clear_out_wg(t: int, target_dir: Path) -> None:
    path = target_dir / "OUT.WG"
    if path.exists():
        path.unlink()
        LOG.info("Removed %s", path)


def mark_stage_done(directory: Path, step: int) -> None:
    marker = directory / f".step{step}.done"
    marker.write_text(now_text() + "\n", encoding="utf-8")
    LOG.info("Wrote checkpoint %s", marker)



def complete_step1_nonscf(ctx: Context, t: int) -> None:
    """Finish step 1 from an already-prepared NON-SCF directory.

    This helper does not create, copy, or back up any gap/dielectric directories.
    """
    target_dir = ctx.gap_dir / str(t)
    ensure_dir(target_dir, f"gap directory for t={t}")
    for fname in ("etot.input", "sub.sh", "gen.kpt"):
        ensure_file(target_dir / fname, f"{fname} for t={t}")

    if not (target_dir / "IN.VR").is_file():
        ensure_file(target_dir / "OUT.VR", f"OUT.VR for t={t}")
        os.replace(target_dir / "OUT.VR", target_dir / "IN.VR")

    if not (target_dir / "IN.KPT").is_file():
        run_command(
            ["split_kp.x", "gen.kpt"],
            target_dir,
            "step1_split_kp",
            t=t,
        )
    ensure_file(target_dir / "IN.KPT", f"IN.KPT for t={t}")

    submit_and_wait(ctx, t, target_dir, "sub.sh", "step1_gap_nonscf")
    output = run_command(
        ["Gap_Read"],
        target_dir,
        "step1_gap_read",
        t=t,
    )
    number = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?"
    gap_match = re.search(rf"Band[\s-]*gap\s*[=:]?\s*({number})", output, re.I)
    fermi_match = re.search(rf"Fermi level\s*[=:]?\s*({number})", output, re.I)
    if not gap_match or not fermi_match:
        raise PipelineError(
            f"Could not parse band gap/Fermi level for t={t}. "
            f"Gap_Read output was: {output!r}"
        )

    band_gap = gap_match.group(1)
    fermi_level = fermi_match.group(1)
    atomic_write_json(
        target_dir / "gap_fermi.json",
        {"band_gap": band_gap, "fermi_level": fermi_level},
    )
    copy_file(target_dir / "REPORT", target_dir / "REPORT_NONSCF")
    clear_out_wg(t, target_dir)
    mark_stage_done(target_dir, 1)
    ctx.status.update(
        t,
        status="step_completed",
        stage="step1",
        slurm_state="COMPLETED",
        message=f"Band gap={band_gap}, Fermi level={fermi_level}",
    )


def run_step1(ctx: Context, t: int) -> None:
    """Band-gap calculation: POTENTIAL -> NONSCF -> split_kp/Gap_Read."""
    target_dir = ctx.gap_dir / str(t)
    ensure_dir(target_dir, f"gap directory for t={t}")
    for fname in ("etot.input", "etot.input2", "gen.kpt", "sub.sh"):
        ensure_file(target_dir / fname, f"{fname} for t={t}")

    ctx.status.update(
        t,
        status="running",
        stage="step1_gap_potential",
        message="Starting band-gap POTENTIAL calculation",
    )
    submit_and_wait(ctx, t, target_dir, "sub.sh", "step1_gap_potential")

    os.replace(target_dir / "etot.input", target_dir / "etot.input1")
    os.replace(target_dir / "etot.input2", target_dir / "etot.input")
    complete_step1_nonscf(ctx, t)


def run_step2(ctx: Context, t: int) -> None:
    """Generate IN.OCC and run the SCF calculation needed by DOS."""
    target_dir = ctx.dielectric_dir / str(t)
    ensure_dir(target_dir, f"dielectric directory for t={t}")
    for fname in ("etot1.input-3-SCF", "sub.sh"):
        ensure_file(target_dir / fname, f"{fname} for t={t}")

    ctx.status.update(
        t,
        status="running",
        stage="step2_generate_occ",
        message="Generating IN.OCC",
    )

    # generate_tddft_occ.x writes a fixed-name IN.OCC in its cwd. Serialize
    # only this tiny local step; all Slurm calculations remain parallel.
    with ctx.generate_occ_lock:
        shared_occ = ctx.base_dir / "IN.OCC"
        if shared_occ.exists():
            shared_occ.unlink()
        run_command(
            ["generate_tddft_occ.x"],
            ctx.base_dir,
            "step2_generate_occ",
            t=t,
            input_text=str(t),
        )
        ensure_file(shared_occ, f"IN.OCC generated for t={t}")
        copy_file(shared_occ, target_dir / "IN.OCC")

    # Match the original workflow: do not remove dielectric OUT.WG here.
    # The DOS step requires OUT.WG -> IN.WG.
    copy_file(target_dir / "etot1.input-3-SCF", target_dir / "etot.input")
    submit_and_wait(ctx, t, target_dir, "sub.sh", "step2_scf")
    copy_file(target_dir / "REPORT", target_dir / "REPORT_SCF")
    ensure_file(target_dir / "OUT.WG", f"SCF OUT.WG for t={t}")
    mark_stage_done(target_dir, 2)

    ctx.status.update(
        t,
        status="step_completed",
        stage="step2",
        slurm_state="COMPLETED",
        message="SCF completed; OUT.WG retained for DOS/RPA",
    )


def run_step3(ctx: Context, t: int) -> None:
    """Run the DOS calculation from the SCF wavefunction."""
    target_dir = ctx.dielectric_dir / str(t)
    ensure_file(target_dir / "OUT.WG", f"SCF OUT.WG for t={t}")
    ensure_file(target_dir / "etot.input-4-DOS", f"DOS input for t={t}")

    ctx.status.update(
        t,
        status="running",
        stage="step3_dos",
        message="Preparing and submitting DOS calculation",
    )
    shutil.copy2(str(target_dir / "OUT.WG"), str(target_dir / "IN.WG"))
    LOG.info("Copied %s -> %s", target_dir / "OUT.WG", target_dir / "IN.WG")
    copy_file(target_dir / "etot.input-4-DOS", target_dir / "etot.input")
    submit_and_wait(ctx, t, target_dir, "sub.sh", "step3_dos")

    # Keep OUT.WG: it is the DOS-modified wavefunction used by RPA_absorb.x.
    mark_stage_done(target_dir, 3)
    ctx.status.update(
        t,
        status="step_completed",
        stage="step3",
        slurm_state="COMPLETED",
        message="DOS completed; OUT.WG retained for RPA",
    )


def update_efermi_in_rpa_input(path: Path, fermi_level: str) -> None:
    ensure_file(path, "RPA.input")
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()

    pattern = re.compile(r"(?m)^(\s*EFERMI\s*=\s*)(\S+)(.*)$")
    if not pattern.search(text):
        raise PipelineError(f"No EFERMI assignment found in {path}")

    replacement = rf"\g<1>{fermi_level}\g<3>"
    new_text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise PipelineError(f"Could not update EFERMI in {path}")

    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(new_text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    LOG.info("Updated EFERMI=%s in %s", fermi_level, path)


def run_step4(ctx: Context, t: int) -> None:
    """Update EFERMI and run the RPA dielectric calculation."""
    gap_json = ctx.gap_dir / str(t) / "gap_fermi.json"
    target_dir = ctx.dielectric_dir / str(t)
    ensure_file(gap_json, f"gap_fermi.json for t={t}")
    ensure_file(target_dir / "RPA.input", f"RPA.input for t={t}")
    ensure_file(target_dir / "RPA.sh", f"RPA.sh for t={t}")

    with open(gap_json, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    fermi_level = str(data.get("fermi_level", "")).strip()
    if not fermi_level:
        raise PipelineError(f"Fermi level is missing from {gap_json}")

    ctx.status.update(
        t,
        status="running",
        stage="step4_rpa_prepare",
        message=f"Updating RPA.input with EFERMI={fermi_level}",
    )
    update_efermi_in_rpa_input(target_dir / "RPA.input", fermi_level)
    submit_and_wait(ctx, t, target_dir, "RPA.sh", "step4_rpa")
    ensure_file(
        target_dir / FINAL_DIELECTRIC_FILE,
        f"{FINAL_DIELECTRIC_FILE} for t={t}",
    )

    # Match the original workflow: dielectric OUT.WG is left in place after
    # RPA. Only --clear-out-wg explicitly removes OUT.WG files.
    mark_stage_done(target_dir, 4)
    ctx.status.update(
        t,
        status="step_completed",
        stage="step4",
        slurm_state="COMPLETED",
        message="RPA calculation completed",
    )


def list_jobs_under_directories(directories: Sequence[Path]) -> List[Tuple[str, str]]:
    """Return (JobID, WorkDir) pairs whose Slurm WorkDir is in the targets."""
    try:
        result = subprocess.run(
            [
                "squeue",
                "-h",
                "-u",
                getpass.getuser(),
                "-o",
                "%i|%Z",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise PipelineError("squeue was not found in PATH") from exc

    if result.returncode != 0:
        raise PipelineError(
            "Could not inspect existing Slurm jobs before cleanup: "
            f"{(result.stderr or '').strip()}"
        )

    target_dirs = {str(path.resolve()) for path in directories}
    matches: List[Tuple[str, str]] = []
    for line in (result.stdout or "").splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        job_id, workdir = parts[0].strip(), parts[1].strip()
        if not job_id or not workdir or workdir == "(null)":
            continue
        try:
            resolved_workdir = str(Path(workdir).resolve())
        except OSError:
            continue
        if resolved_workdir in target_dirs:
            matches.append((job_id, resolved_workdir))
    return matches


def cancel_jobs_for_timepoint(ctx: Context, t: int) -> None:
    """Cancel active jobs previously submitted from this time-point's dirs."""
    directories = (ctx.gap_dir / str(t), ctx.dielectric_dir / str(t))
    jobs = list_jobs_under_directories(directories)
    if not jobs:
        LOG.info("No active Slurm jobs found for t=%s", t)
        return

    for job_id, workdir in jobs:
        LOG.warning(
            "Cancelling leftover JobID %s for t=%s (WorkDir=%s)",
            job_id,
            t,
            workdir,
        )
        result = subprocess.run(
            ["scancel", job_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise PipelineError(
                f"Failed to cancel leftover JobID {job_id} for t={t}: "
                f"{(result.stdout or '').strip()}"
            )

    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        remaining = list_jobs_under_directories(directories)
        if not remaining:
            LOG.info("All leftover Slurm jobs for t=%s are gone", t)
            return
        time.sleep(2.0)

    remaining_ids = ", ".join(job_id for job_id, _ in remaining)
    raise PipelineError(
        f"Leftover Slurm jobs for t={t} did not exit within 60 seconds: "
        f"{remaining_ids}"
    )


def create_timepoint_inputs(ctx: Context, t: int) -> None:
    """Create/repopulate gap/<t> and dielectric/<t> from the source inputs."""
    gap_target = ctx.gap_dir / str(t)
    dielectric_target = ctx.dielectric_dir / str(t)

    for target in (gap_target, dielectric_target):
        if target.exists() or target.is_symlink():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=False)

    for fname in GAP_SOURCE_FILES:
        copy_file(ctx.source_dir / fname, gap_target / fname)
    for fname in DIELECTRIC_SOURCE_FILES:
        copy_file(ctx.source_dir / fname, dielectric_target / fname)

    suffix = scientific_time_label(t)
    rho_src = ctx.tddos_dir / f"OUT.RHO.{suffix}"
    config_src = ctx.tddos_dir / f"restart.config.{suffix}"
    for target in (gap_target, dielectric_target):
        copy_file(rho_src, target / "IN.RHO")
        copy_file(config_src, target / "atom.config")

    LOG.info("Recreated input directories for t=%s", t)


def run_smart_resume_timepoint(ctx: Context, t: int) -> None:
    """Skip complete times; otherwise clean and fully recompute one time."""
    dielectric_target = ctx.dielectric_dir / str(t)
    final_file = dielectric_target / FINAL_DIELECTRIC_FILE

    if final_file.is_file():
        LOG.info(
            "t=%s already has %s; skipping",
            t,
            final_file,
        )
        ctx.status.update(
            t,
            status="completed",
            stage="all",
            slurm_state="COMPLETED",
            message=f"Already complete: {FINAL_DIELECTRIC_FILE} exists",
        )
        return

    preflight_timepoint(ctx, t)
    LOG.warning(
        "t=%s is incomplete because %s is missing; clearing and recomputing",
        t,
        final_file,
    )
    ctx.status.update(
        t,
        status="running",
        stage="resume_cleanup",
        job_id="",
        slurm_state="",
        message="Cancelling leftover jobs and recreating time-point inputs",
    )
    cancel_jobs_for_timepoint(ctx, t)
    create_timepoint_inputs(ctx, t)
    run_full_timepoint(ctx, t)


def run_smart_workspace(ctx: Context, label: str = "smart workflow") -> int:
    """Unified entry point for both fresh calculations and continuation."""
    if not ctx.gap_dir.exists() and not ctx.dielectric_dir.exists():
        LOG.info("Neither gap nor dielectric root exists; starting from scratch")
        prepare_workspace(ctx)
        return run_parallel(
            ctx,
            lambda t: run_full_timepoint(ctx, t),
            label,
        )

    ctx.gap_dir.mkdir(parents=True, exist_ok=True)
    ctx.dielectric_dir.mkdir(parents=True, exist_ok=True)
    return run_parallel(
        ctx,
        lambda t: run_smart_resume_timepoint(ctx, t),
        label,
    )


def run_full_timepoint(ctx: Context, t: int) -> None:
    LOG.info("=" * 72)
    LOG.info("Starting full workflow for t=%s", t)
    try:
        run_step1(ctx, t)
        run_step2(ctx, t)
        run_step3(ctx, t)
        run_step4(ctx, t)
    except Exception as exc:
        LOG.exception("Time point t=%s failed", t)
        ctx.status.update(
            t,
            status="failed",
            message=f"{type(exc).__name__}: {exc}",
        )
        raise
    else:
        ctx.status.update(
            t,
            status="completed",
            stage="all",
            slurm_state="COMPLETED",
            message="All four steps completed",
        )
        LOG.info("Completed all steps for t=%s", t)


def run_step_only(ctx: Context, t: int, step: int) -> None:
    functions = {
        1: run_step1,
        2: run_step2,
        3: run_step3,
        4: run_step4,
    }
    LOG.info("=" * 72)
    LOG.info("Starting step %s for t=%s", step, t)
    try:
        functions[step](ctx, t)
    except Exception as exc:
        LOG.exception("Step %s for t=%s failed", step, t)
        ctx.status.update(t, status="failed", message=f"{type(exc).__name__}: {exc}")
        raise
    else:
        LOG.info("Completed step %s for t=%s", step, t)


def run_parallel(
    ctx: Context,
    worker,
    description: str,
) -> int:
    failures = 0
    max_parallel = max(1, int(ctx.args.max_parallel))
    LOG.info("Configured time points: %s", ctx.times)
    LOG.info(
        "Launching %s for %s time points with max_parallel=%s",
        description,
        len(ctx.times),
        max_parallel,
    )
    ctx.status.set_overall(status="running", message=description)
    with ThreadPoolExecutor(
        max_workers=max_parallel, thread_name_prefix="timepoint"
    ) as executor:
        futures = {executor.submit(worker, t): t for t in ctx.times}
        for future in as_completed(futures):
            t = futures[future]
            try:
                future.result()
            except Exception:
                failures += 1
                LOG.error("Time point t=%s finished with an error", t)

    if failures:
        LOG.error("%s finished with %s failed time point(s)", description, failures)
        ctx.status.set_overall(
            status="failed",
            message=f"{description}: {failures} failed time point(s)",
        )
        return 1
    LOG.info("%s completed successfully for all time points", description)
    ctx.status.set_overall(status="completed", message=description)
    return 0


def clear_all_out_wg(ctx: Context) -> None:
    for directory in (ctx.gap_dir, ctx.dielectric_dir):
        for t in ctx.times:
            target = directory / str(t)
            if target.is_dir():
                clear_out_wg(t, target)


def print_brief_status(data: Dict) -> None:
    times = data.get("times", {})
    if not isinstance(times, dict):
        times = {}

    def sort_key(item: Tuple[str, Dict]) -> int:
        try:
            return int(item[0])
        except (TypeError, ValueError):
            return 10**9

    ordered = sorted(times.items(), key=sort_key)
    counts: Dict[str, int] = {}
    for _, item in ordered:
        status = str(item.get("status", "pending"))
        counts[status] = counts.get(status, 0) + 1

    print(f"Overall : {data.get('status', 'unknown')}")
    print(f"PID     : {data.get('pid', '')}")
    print(f"Updated : {data.get('updated_at', '')}")
    print(f"Total   : {len(ordered)}")
    print(
        "Counts  : "
        + ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    )

    failures = [
        (t, item)
        for t, item in ordered
        if str(item.get("status", "")).lower() == "failed"
    ]
    active = [
        (t, item)
        for t, item in ordered
        if str(item.get("status", "")).lower()
        not in ("completed", "pending", "failed")
    ]
    pending = [
        (t, item)
        for t, item in ordered
        if str(item.get("status", "")).lower() == "pending"
    ]

    def print_items(title: str, items: List[Tuple[str, Dict]]) -> None:
        print("\n" + title)
        if not items:
            print("  none")
            return
        for t, item in items:
            print(
                "  {t:>4}  {status:<14} {stage:<24} "
                "job={job:<10} slurm={slurm:<12} updated={updated}".format(
                    t=t,
                    status=str(item.get("status", "")),
                    stage=str(item.get("stage", ""))[:24],
                    job=str(item.get("job_id", ""))[:10],
                    slurm=str(item.get("slurm_state", ""))[:12],
                    updated=str(item.get("updated_at", "")),
                )
            )
            message = str(item.get("message", ""))
            if message:
                print(f"        {message[:160]}")

    print_items("FAILED", failures)
    print_items("RUNNING / INCOMPLETE", active)
    print_items("NEXT NOT STARTED", pending[:10])
    if len(pending) > 10:
        print(f"  ... and {len(pending) - 10} more not-started times")


def show_status(
    status_file: Path,
    log_file: Path,
    pid_file: Path,
) -> int:
    if not status_file.is_file():
        print(f"Status file does not exist: {status_file}", file=sys.stderr)
        print("", file=sys.stderr)

        if pid_file.is_file():
            pid_text = pid_file.read_text(encoding="utf-8").strip()
            print(f"PID file exists: {pid_file} (PID={pid_text})", file=sys.stderr)
            try:
                os.kill(int(pid_text), 0)
                print("The orchestrator process appears to be alive.", file=sys.stderr)
            except (ValueError, ProcessLookupError):
                print("The recorded PID is stale; the process is not alive.", file=sys.stderr)
            except PermissionError:
                print("The recorded PID belongs to a process you cannot inspect.", file=sys.stderr)
        else:
            print(f"PID file does not exist: {pid_file}", file=sys.stderr)
            print(
                "This usually means the daemon never started or exited before initialization.",
                file=sys.stderr,
            )

        if log_file.is_file():
            print(f"\nLast lines of {log_file}:", file=sys.stderr)
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            for line in lines[-60:]:
                print(line, file=sys.stderr)
        else:
            print(f"Log file does not exist: {log_file}", file=sys.stderr)
        return 1

    with open(status_file, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    print_brief_status(data)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parallel PWMAT dielectric-function workflow for MCloud/Slurm"
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--step",
        type=int,
        choices=[0, 1, 2, 3, 4],
        help="Run one workflow step; step 0 force-prepares fresh directories",
    )
    action.add_argument(
        "--clear-out-wg",
        "--clear_OUT_WG",
        dest="clear_out_wg",
        action="store_true",
        help="Remove OUT.WG files for all selected time points",
    )
    action.add_argument(
        "--show-status",
        action="store_true",
        help="Print the progress file and exit",
    )

    time_selection = parser.add_mutually_exclusive_group()
    time_selection.add_argument("--time", type=int, help="One time point")
    time_selection.add_argument(
        "--times", type=str, help="Comma-separated time points, e.g. 0,4,8"
    )
    time_selection.add_argument(
        "--all", action="store_true", help="Use the CALC_TIMES list configured at the top"
    )

    parser.add_argument("--base-dir", default=DEFAULT_BASE_DIR)
    parser.add_argument("--source-dir", default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--tddos-dir", default=None)
    parser.add_argument("--gap-name", default=DEFAULT_GAP_NAME)
    parser.add_argument("--dielectric-name", default=DEFAULT_DIELECTRIC_NAME)
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Log directory; defaults to DEFAULT_BASE_DIR/logs",
    )
    parser.add_argument("--max-parallel", type=int, default=MAX_PARALLEL)
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=POLL_SECONDS,
        help="Seconds between squeue checks",
    )
    parser.add_argument(
        "--job-timeout-hours",
        type=float,
        default=0.0,
        help="Cancel a single job after this many hours; 0 means unlimited",
    )
    parser.add_argument(
        "--sbatch-max-attempts",
        type=int,
        default=SBATCH_MAX_ATTEMPTS,
        help="Maximum attempts for transient sbatch submission failures",
    )
    parser.add_argument(
        "--sbatch-retry-seconds",
        type=float,
        default=SBATCH_RETRY_SECONDS,
        help="Seconds between transient sbatch submission retries",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Double-fork, detach from the terminal, and append output to the log",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    base_dir = Path(args.base_dir).expanduser().resolve()
    log_dir = (
        Path(args.log_dir).expanduser().resolve()
        if args.log_dir
        else base_dir / "logs"
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    status_file = log_dir / "pwmat_parallel_status.json"
    pid_file = log_dir / "pwmat_parallel.pid"
    log_file = log_dir / "pwmat_parallel.log"

    if args.show_status:
        return show_status(status_file, log_file, pid_file)

    smart_requested = not (
        args.step is not None
        or args.clear_out_wg
        or args.show_status
    )

    if smart_requested:
        if status_file.is_file():
            try:
                with open(status_file, "r", encoding="utf-8") as handle:
                    args.previous_status = json.load(handle)
            except (OSError, ValueError) as exc:
                LOG.warning(
                    "Could not read previous status file %s: %s",
                    status_file,
                    exc,
                )
                args.previous_status = {}
        else:
            args.previous_status = {}

    if args.daemon:
        daemonize(log_file)

    configure_logging(log_file, console=not args.daemon)
    LOG.info("PWMAT parallel orchestrator starting (PID=%s)", os.getpid())
    LOG.info("Command line: %s", shlex.join(sys.argv))

    times = times_from_args(args)

    if not times:
        parser.error("No calculation times were configured")

    with PidLock(pid_file):
        ctx = Context(args, times)
        try:
            if args.step == 0:
                prepare_workspace(ctx)
                return 0

            if args.clear_out_wg:
                clear_all_out_wg(ctx)
                return 0

            if smart_requested:
                return run_smart_workspace(ctx, "smart workflow")

            if args.step == 1:
                return run_parallel(
                    ctx, lambda t: run_step_only(ctx, t, 1), "step 1"
                )
            if args.step == 2:
                return run_parallel(
                    ctx, lambda t: run_step_only(ctx, t, 2), "step 2"
                )
            if args.step == 3:
                return run_parallel(
                    ctx, lambda t: run_step_only(ctx, t, 3), "step 3"
                )
            if args.step == 4:
                return run_parallel(
                    ctx, lambda t: run_step_only(ctx, t, 4), "step 4"
                )

            return run_smart_workspace(ctx, "smart workflow")
        except Exception as exc:
            LOG.exception("Orchestrator failed")
            ctx.status.set_overall(status="failed", message=str(exc))
            return 1
        finally:
            LOG.info("PWMAT parallel orchestrator stopped")


if __name__ == "__main__":
    sys.exit(main())
