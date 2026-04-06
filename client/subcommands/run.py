import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from time import sleep

import typer
from rich import print
from rich.panel import Panel
from rich.table import Table
from typing_extensions import Annotated

from client.helpers.http_client import HTTP_Client

app = typer.Typer(no_args_is_help=True)


def _load_suite(path: Path) -> dict:
    if not path.exists():
        raise typer.BadParameter(f"Suite file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _resolve_job_id(owner: str, name: str) -> int | None:
    response = HTTP_Client().get("/jobs", params={"owner": owner})
    jobs = response.json()
    candidates = [job for job in jobs if job.get("name") == name and job.get("owner") == owner]
    if not candidates:
        return None
    return max(candidate.get("id_", 0) for candidate in candidates)


def _fetch_job(owner: str, job_id: int) -> dict:
    response = HTTP_Client().get(f"/jobs/{job_id}", params={"owner": owner})
    return response.json()


def _run_pre_steps(defaults: dict, job: dict) -> tuple[bool, str]:
    pre_steps = job.get("pre_steps", [])
    if not pre_steps:
        return True, ""

    hdfs_bin = str(defaults.get("hdfs_bin", "hdfs"))
    for step in pre_steps:
        step_type = str(step.get("type", "")).strip()
        step_path = str(step.get("path", "")).strip()
        if not step_type or not step_path:
            return False, "invalid pre_step (missing type/path)"

        if step_type == "hdfs_rm":
            subprocess.run([hdfs_bin, "dfs", "-rm", "-r", "-f", step_path], check=False)
            continue

        if step_type == "hdfs_mkdir":
            try:
                subprocess.run([hdfs_bin, "dfs", "-mkdir", "-p", step_path], check=True)
            except subprocess.CalledProcessError as exc:
                return False, f"pre_step hdfs_mkdir failed for {step_path}: {exc}"
            continue

        return False, f"unsupported pre_step type '{step_type}'"

    return True, ""


@app.command("suite", help="Submit jobs from a JSON suite file.")
def suite(
    file: Annotated[str, typer.Option("--file", "-f", help="Path to suite JSON file.")],
):
    suite_path = Path(file)
    payload = _load_suite(suite_path)

    defaults = payload.get("defaults", {})
    jobs = payload.get("jobs", [])
    if not jobs:
        raise typer.BadParameter("Suite has no jobs.")

    owner = defaults.get("owner") or os.getenv("USER")
    if not owner:
        raise typer.BadParameter("Owner not set. Set defaults.owner in suite or USER env var.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    on_error = str(defaults.get("on_error", "stop")).strip().lower()
    if on_error not in {"stop", "continue"}:
        raise typer.BadParameter("defaults.on_error must be 'stop' or 'continue'.")

    submitted_ok = set()
    submitted_jobs = []
    had_error = False

    table = Table(title=f"Suite Submit: {payload.get('name', suite_path.name)}")
    table.add_column("Suite Job", style="dim")
    table.add_column("Scheduler", style="dim")
    table.add_column("Queue", style="dim")
    table.add_column("Name", style="dim")
    table.add_column("Meta Job ID", style="dim")
    table.add_column("Result", style="dim")

    for index, job in enumerate(jobs, start=1):
        if not job.get("enabled", True):
            continue

        suite_job_id = str(job.get("id", f"job-{index}"))
        deps = job.get("depends_on", [])
        if deps:
            missing = [dep for dep in deps if dep not in submitted_ok]
            if missing:
                msg = f"Skipped due to unmet dependencies: {', '.join(missing)}"
                table.add_row(suite_job_id, "-", "-", "-", "-", msg)
                had_error = True
                if on_error == "stop":
                    break
                continue

        scheduler = str(job.get("scheduler_type", "")).strip().upper()
        if scheduler not in {"S", "H"}:
            msg = f"Invalid scheduler_type '{scheduler}'"
            table.add_row(suite_job_id, scheduler, "-", "-", "-", msg)
            had_error = True
            if on_error == "stop":
                break
            continue

        queue = job.get("queue", defaults.get("queue"))
        if queue is None:
            msg = "Missing queue (job.queue or defaults.queue)"
            table.add_row(suite_job_id, scheduler, "-", "-", "-", msg)
            had_error = True
            if on_error == "stop":
                break
            continue

        path = job.get("path")
        if not path:
            msg = "Missing path"
            table.add_row(suite_job_id, scheduler, str(queue), "-", "-", msg)
            had_error = True
            if on_error == "stop":
                break
            continue

        name_template = str(job.get("name", f"suite-{suite_job_id}-${{ts}}"))
        name = name_template.replace("${ts}", ts)
        options = str(job.get("options", ""))
        qsub_options = str(job.get("qsub_options", "")) if scheduler == "S" else ""

        hadoop_quiet = bool(job.get("hadoop_quiet", defaults.get("hadoop_quiet", False)))
        if scheduler == "H" and hadoop_quiet:
            options = f"{options} --ms-hadoop-quiet".strip()

        request_data = {
            "name": name,
            "queue": int(queue),
            "owner": owner,
            "path": path,
            "options": options,
            "qsub_options": qsub_options,
            "pwd": str(job.get("pwd", os.getcwd())),
            "scheduler_type": scheduler,
        }

        pre_ok, pre_error = _run_pre_steps(defaults, job)
        if not pre_ok:
            table.add_row(suite_job_id, scheduler, str(queue), name, "-", f"ERROR: {pre_error}")
            had_error = True
            if on_error == "stop":
                break
            continue

        response, error = HTTP_Client().post_safe("/jobs", request_data)
        if error:
            table.add_row(suite_job_id, scheduler, str(queue), name, "-", f"ERROR: {error}")
            had_error = True
            if on_error == "stop":
                break
            continue

        meta_job_id = _resolve_job_id(owner, name)
        table.add_row(
            suite_job_id,
            scheduler,
            str(queue),
            name,
            str(meta_job_id) if meta_job_id is not None else "?",
            "submitted",
        )
        submitted_ok.add(suite_job_id)
        submitted_jobs.append(
            {
                "suite_job_id": suite_job_id,
                "scheduler": scheduler,
                "name": name,
                "meta_job_id": meta_job_id,
            }
        )

        delay = float(job.get("delay_after_seconds", defaults.get("delay_after_seconds", 0)))
        if delay > 0:
            sleep(delay)

    print(Panel(table, border_style="green"))

    # Final summary disabled for now.
    # if submitted_jobs:
    #     summary = Table(title=f"Suite Final Summary: {payload.get('name', suite_path.name)}")
    #     summary.add_column("Suite Job", style="dim")
    #     summary.add_column("Meta Job ID", style="dim")
    #     summary.add_column("Scheduler", style="dim")
    #     summary.add_column("Status", style="dim")
    #     summary.add_column("Exec Time (s)", style="dim")
    #     summary.add_column("Started At", style="dim")
    #     summary.add_column("Completed At", style="dim")
    #
    #     status_counts: dict[str, int] = {}
    #     for submitted in submitted_jobs:
    #         meta_job_id = submitted.get("meta_job_id")
    #         if meta_job_id is None:
    #             summary.add_row(
    #                 submitted["suite_job_id"],
    #                 "?",
    #                 submitted["scheduler"],
    #                 "UNKNOWN",
    #                 "-",
    #                 "-",
    #                 "-",
    #             )
    #             status_counts["UNKNOWN"] = status_counts.get("UNKNOWN", 0) + 1
    #             continue
    #
    #         try:
    #             job = _fetch_job(owner, int(meta_job_id))
    #             status = str(job.get("status", "UNKNOWN"))
    #             exec_time = job.get("execution_time_seconds")
    #             started_at = str(job.get("started_at"))
    #             completed_at = str(job.get("completed_at"))
    #             summary.add_row(
    #                 submitted["suite_job_id"],
    #                 str(meta_job_id),
    #                 submitted["scheduler"],
    #                 status,
    #                 f"{exec_time:.2f}" if isinstance(exec_time, (int, float)) else "-",
    #                 started_at,
    #                 completed_at,
    #             )
    #             status_counts[status] = status_counts.get(status, 0) + 1
    #         except Exception:
    #             summary.add_row(
    #                 submitted["suite_job_id"],
    #                 str(meta_job_id),
    #                 submitted["scheduler"],
    #                 "FETCH_ERROR",
    #                 "-",
    #                 "-",
    #                 "-",
    #             )
    #             status_counts["FETCH_ERROR"] = status_counts.get("FETCH_ERROR", 0) + 1
    #
    #     counters = ", ".join(f"{k}={v}" for k, v in sorted(status_counts.items())) or "none"
    #     print(Panel(summary, border_style="cyan"))
    #     print(Panel(f"[bold]Status counters:[/bold] {counters}", border_style="magenta"))

    if had_error:
        raise typer.Exit(1)
