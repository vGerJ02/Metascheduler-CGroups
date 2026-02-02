import io
import os
import sys
from typing_extensions import Annotated
import typer
from rich import print
import time
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.console import Console
from rich.table import Table
from rich.live import Live
from client.subcommands.get import JobStatus, cluster_mode as get_cluster_mode, queues as get_queues, jobs as get_jobs, job as get_job

from client.helpers.http_client import HTTP_Client

app = typer.Typer(no_args_is_help=True)

update_interval = 5
updates_number = sys.maxsize
console = Console()
console.clear()


@app.callback()
def callback(
    interval: Annotated[int, typer.Option(
        help="Interval in seconds to update the information",
        show_default=True,
        min=1,
    )] = 5,
    updates: Annotated[int, typer.Option(
        help="Number of updates to display",
        show_default=True,
    )] = sys.maxsize,
):
    global update_interval
    global updates_number
    update_interval = interval
    updates_number = updates


def show_progress_bar():
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Next update in", total=100)
        while not progress.finished:
            progress.update(task, advance=1)
            time.sleep(update_interval / 100)


@app.command(help="Watch the cluster working mode.")
def cluster_mode():
    for i in range(updates_number):
        console.log("Watching the cluster mode... (Press Ctrl+C to stop)")
        console.log(f"{i}/{updates_number} updates")
        get_cluster_mode()
        show_progress_bar()
        console.clear()


@app.command(help="Watch the cluster queues.")
def queues():
    for i in range(updates_number):
        console.log("Watching the cluster queues... (Press Ctrl+C to stop)")
        console.log(f"{i}/{updates_number} updates")
        get_queues()
        show_progress_bar()
        console.clear()


@app.command(help="Watch the cluster jobs.")
def jobs(status: Annotated[JobStatus, typer.Option(help="Job status.", case_sensitive=False)] = None, queue: Annotated[int, typer.Option(help="Queue ID.")] = None):
    for i in range(updates_number):
        console.log("Watching the cluster jobs... (Press Ctrl+C to stop)")
        console.log(f"{i}/{updates_number} updates")
        get_jobs(status, queue)
        show_progress_bar()
        console.clear()


@app.command(help="Watch a cluster job.")
def job(id: Annotated[int, typer.Argument(help="The Job ID.")]):
    for i in range(updates_number):
        console.log(f"Watching the cluster job {id}... (Press Ctrl+C to stop)")
        console.log(f"{i}/{updates_number} updates")
        get_job(id)
        show_progress_bar()
        console.clear()


def _build_nodes_metrics_table(metrics_raw):
    table = Table(title="Node Metrics", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="dim")
    table.add_column("IP", style="dim")
    table.add_column("Alive", style="dim")
    table.add_column("CPU (%)", style="dim")
    table.add_column("RAM (%)", style="dim")
    table.add_column("Disk (%)", style="dim")
    table.add_column("Load1", style="dim")
    table.add_column("Error", style="dim")

    for metric in metrics_raw:
        cpu = metric.get("cpu_percent")
        ram = metric.get("ram_percent")
        disk = metric.get("disk_percent")
        load1 = metric.get("load1")
        error = metric.get("error") or ""
        table.add_row(
            str(metric.get("id")),
            str(metric.get("ip")),
            str(metric.get("is_alive")),
            f"{cpu:.2f}" if isinstance(cpu, (int, float)) else "-",
            f"{ram:.2f}" if isinstance(ram, (int, float)) else "-",
            f"{disk:.2f}" if isinstance(disk, (int, float)) else "-",
            f"{load1:.2f}" if isinstance(load1, (int, float)) else "-",
            error,
        )
    return table


@app.command("nodes-metrics", help="Watch live metrics for all nodes.")
def nodes_metrics():
    console.log("Watching node metrics... (Press Ctrl+C to stop)")
    updates_remaining = updates_number
    with Live(console=console, refresh_per_second=4) as live:
        while updates_remaining > 0:
            response = HTTP_Client().get('/cluster/nodes/metrics')
            metrics_raw = response.json()
            table = _build_nodes_metrics_table(metrics_raw)
            live.update(table)
            updates_remaining -= 1
            time.sleep(update_interval)
