from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
import os
from typing import Optional
from typing_extensions import Annotated
from requests import Response
import typer
from typing import Optional
from rich import print 
from rich.panel import Panel
from rich.table import Table

from api.constants.scheduler_type import SchedulerType
from client.helpers.http_client import HTTP_Client

app = typer.Typer(no_args_is_help=True)


@dataclass
class NodeResponse:
    id: int
    ip: str
    port: int
    is_alive: bool


@dataclass
class JobResponse:
    id_: int
    queue: int
    name: str
    created_at: datetime
    owner: str
    status: str
    path: str
    options: str
    scheduler_job_id: int
    pwd: str
    scheduler_type: str
    scheduler_job_ref: Optional[str] = None
    quiet: Optional[bool] = None


_JOB_RESPONSE_FIELDS = {field_.name for field_ in fields(JobResponse)}


def _build_job_response(payload: dict) -> JobResponse:
    filtered = {k: v for k, v in payload.items() if k in _JOB_RESPONSE_FIELDS}
    return JobResponse(**filtered)

@dataclass
class JobMetricResponse:
    id: int
    job_id: int
    collected_at: datetime
    cpu_usage: float
    ram_usage: float
    disk_read_bytes: float
    disk_write_bytes: float


@app.command("nodes-metrics", help="Get metrics for all nodes.")
def nodes_metrics():
    response: Response = HTTP_Client().get('/cluster/nodes/metrics')
    metrics_raw = response.json()

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

    panel = Panel(table, border_style="green")
    print(panel)

@dataclass
class JobMetricResponse:
    id: int
    job_id: int
    collected_at: datetime
    cpu_usage: float
    ram_usage: float
    disk_read_bytes: float
    disk_write_bytes: float


class JobStatus(str, Enum):
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'


@app.command(help="Get the cluster working mode.")
def cluster_mode():
    response: Response = HTTP_Client().get('/cluster/mode')
    cluster_mode = response.json()
    panel = Panel(
        f"[bold cyan]Cluster Mode:[/bold cyan]\n{cluster_mode}",
        title="[bold magenta]Cluster Information[/bold magenta]",
        border_style="green"
    )
    print(panel)


@app.command(help="Get the cluster nodes.")
def nodes():
    response: Response = HTTP_Client().get('/cluster/nodes')
    nodes_raw = response.json()
    nodes = [NodeResponse(**node) for node in nodes_raw]
    table = Table(title="Cluster Nodes", show_header=True,
                  header_style="bold magenta")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("IP", style="dim")
    table.add_column("Port", style="dim")
    table.add_column("Is Alive", style="dim")

    for node in nodes:
        table.add_row(str(node.id), node.ip, str(
            node.port), str(node.is_alive))

    panel = Panel(table, border_style="green")
    print(panel)


@app.command("master", help="Get the master node.")
@app.command("master-node", hidden=True, help="Get the master node.")
def master_node():
    response: Response = HTTP_Client().get('/cluster/nodes/master')
    master_node = NodeResponse(**response.json())
    table = Table(title="Master Node", show_header=True,
                  header_style="bold magenta")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("IP", style="dim")
    table.add_column("Port", style="dim")
    table.add_column("Is Alive", style="dim")

    table.add_row(str(master_node.id), master_node.ip, str(
        master_node.port), str(master_node.is_alive))

    panel = Panel(table, border_style="green")
    print(panel)


@app.command(help="Get a node.")
def node(node_id: Annotated[int, typer.Argument(help="Node ID")]):
    response: Response = HTTP_Client().get(f'/cluster/nodes/{node_id}')
    node = NodeResponse(**response.json())
    table = Table(title="Node", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("IP", style="dim")
    table.add_column("Port", style="dim")
    table.add_column("Is Alive", style="dim")

    table.add_row(str(node.id), node.ip, str(node.port), str(node.is_alive))

    panel = Panel(table, border_style="green")
    print(panel)


@app.command(help="Get the queues.")
def queues():
    response: Response = HTTP_Client().get('/queues')
    queues = response.json()
    table = Table(title="Queues", show_header=True,
                  header_style="bold magenta")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("Scheduler Name", style="dim")

    for queue in queues:
        table.add_row(str(queue['id']), queue['scheduler_name'])

    panel = Panel(table, border_style="green")
    print(panel)


@app.command(help="Get the jobs with optional filters.")
def jobs(status: Annotated[JobStatus, typer.Option(help="Job status.", case_sensitive=False)] = None,
         queue: Annotated[int, typer.Option(help="Queue ID.")] = None):
    params = {}
    params["owner"] = os.getenv("USER")
    if status:
        params['status'] = status
    if queue:
        params['queue'] = queue
    response: Response = HTTP_Client().get(
        '/jobs', params)
    jobs_raw = response.json()
    jobs = [_build_job_response(job) for job in jobs_raw]
    table = Table(title="Jobs", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="cyan", width=3)
    table.add_column("Queue", style="dim")
    table.add_column("Name", style="dim")
    table.add_column("Created At", style="dim")
    table.add_column("Owner", style="dim")
    table.add_column("Status", style="dim")
    table.add_column("Path", style="dim")
    table.add_column("Options", style="dim")
    table.add_column("Scheduler Job ID", style="dim")
    table.add_column("PWD", style="dim")
    table.add_column("Type", style="dim")

    for job in jobs:
        scheduler_name = SchedulerType.name_from_code(job.scheduler_type)
        table.add_row(str(job.id_), str(job.queue), job.name, str(job.created_at),
                      job.owner, job.status, job.path, job.options, str(job.scheduler_job_id), job.pwd,
                      scheduler_name)

    panel = Panel(table, border_style="green")
    print(panel)


@app.command(help="Get a specific job.")
def job(id: Annotated[int, typer.Argument(help="The Job ID.")]):
    params = {}
    params["owner"] = os.getenv("USER")
    response: Response = HTTP_Client().get(f'/jobs/{id}', params)
    job = _build_job_response(response.json())
    table = Table(title="Job", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="cyan", width=3)
    table.add_column("Queue", style="dim")
    table.add_column("Name", style="dim")
    table.add_column("Created At", style="dim")
    table.add_column("Owner", style="dim")
    table.add_column("Status", style="dim")
    table.add_column("Path", style="dim")
    table.add_column("Options", style="dim")
    table.add_column("Scheduler Job ID", style="dim")
    table.add_column("PWD", style="dim")
    table.add_column("Type", style="dim")

    scheduler_name = SchedulerType.name_from_code(job.scheduler_type)

    table.add_row(str(job.id_), str(job.queue), job.name, str(job.created_at),
                  job.owner, job.status, job.path, job.options, str(job.scheduler_job_id), job.pwd, scheduler_name)

    panel = Panel(table, border_style="green")
    print(panel)

@app.command("job-metrics", help="Get metrics for a specific job.")
def job_metrics(id: Annotated[int, typer.Argument(help="The Job ID.")]):
    params = {}
    params["owner"] = os.getenv("USER")
    response: Response = HTTP_Client().get(f'/jobs/{id}/metrics', params)
    metrics_raw = response.json()
    metrics = [JobMetricResponse(**metric) for metric in metrics_raw]

    table = Table(title=f"Job Metrics (Job {id})", show_header=True,
                  header_style="bold magenta")
    table.add_column("Collected At", style="dim")
    table.add_column("CPU (%)", style="dim")
    table.add_column("RAM (%)", style="dim")
    table.add_column("Disk Read (B)", style="dim")
    table.add_column("Disk Write (B)", style="dim")

    for metric in metrics:
        table.add_row(
            str(metric.collected_at),
            f"{metric.cpu_usage:.2f}",
            f"{metric.ram_usage:.2f}",
            f"{metric.disk_read_bytes:.0f}",
            f"{metric.disk_write_bytes:.0f}",
        )

    panel = Panel(table, border_style="green")
    print(panel)


@app.command("job-metrics", help="Get metrics for a specific job.")
def job_metrics(id: Annotated[int, typer.Argument(help="The Job ID.")]):
    params = {}
    params["owner"] = os.getenv("USER")
    response: Response = HTTP_Client().get(f'/jobs/{id}/metrics', params)
    metrics_raw = response.json()
    metrics = [JobMetricResponse(**metric) for metric in metrics_raw]

    table = Table(title=f"Job Metrics (Job {id})", show_header=True,
                  header_style="bold magenta")
    table.add_column("Collected At", style="dim")
    table.add_column("CPU (%)", style="dim")
    table.add_column("RAM (%)", style="dim")
    table.add_column("Disk Read (B)", style="dim")
    table.add_column("Disk Write (B)", style="dim")

    for metric in metrics:
        table.add_row(
            str(metric.collected_at),
            f"{metric.cpu_usage:.2f}",
            f"{metric.ram_usage:.2f}",
            f"{metric.disk_read_bytes:.0f}",
            f"{metric.disk_write_bytes:.0f}",
        )

    panel = Panel(table, border_style="green")
    print(panel)


if __name__ == "__main__":
    app()
