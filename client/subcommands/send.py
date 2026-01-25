import os
from dataclasses import dataclass
from time import sleep
from typing import List, Tuple

from typing_extensions import Annotated
import typer
from rich import print
from rich.panel import Panel

from client.helpers.http_client import HTTP_Client

app = typer.Typer(no_args_is_help=True)

DEFAULT_SGE_SCRIPTS = [
    "test_job100000000.sh",
    "test_job1000000000.sh",
    "test_job10000000000.sh",
    "test_job100000000000.sh",
]
DEFAULT_HADOOP_INPUTS = [
    "1000000.txt",
    "5000000.txt",
    "10000000.txt",
    "15000000.txt",
]
DEFAULT_HADOOP_JAR = (
    "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar"
)
DEFAULT_HADOOP_CLASS = "wordcount"


@dataclass
class BenchmarkJob:
    name: str
    queue: int
    path: str
    scheduler_type: str
    options: str = ""


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@app.command(help="Send a job to the metascheduler.")
def job(
    name: Annotated[str, typer.Option(help="Job name.")],
    queue: Annotated[int, typer.Option(help="Job queue.")],
    path: Annotated[str, typer.Option(help="Job path.")],
    scheduler_type: Annotated[
        str, typer.Option(help="Scheduler type: S (SGE), H (Hadoop).")
    ],
    options: Annotated[str, typer.Option(help="Job options.")] = "",
    hadoop_quiet: Annotated[
        bool, typer.Option(help="Reduce Hadoop client logging for this job.")
    ] = False,
):
    scheduler_code = scheduler_type.strip().upper()
    if hadoop_quiet:
        if scheduler_code == "H":
            options = f"{options} --ms-hadoop-quiet".strip()
        else:
            print(
                "[bold yellow]Warning:[/bold yellow] --hadoop-quiet ignored for non-Hadoop jobs."
            )
    request_data = {
        "name": name,
        "queue": queue,
        "owner": os.getenv("USER"),
        "path": path,
        "options": options,
        "pwd": os.getcwd(),
        "scheduler_type": scheduler_code,
    }
    response = HTTP_Client().post("/jobs", request_data)
    response_message = response.json()["message"]
    panel = Panel(
        f"[bold cyan]Response:[/bold cyan]\n{response_message}",
        title="[bold magenta]Create Job[/bold magenta]",
        border_style="green",
    )
    print(panel)


@app.command(help="Send Hadoop and SGE benchmark jobs with predefined configs.")
def benchmarks(
    sge_queue: Annotated[int, typer.Option(help="Queue ID for SGE benchmarks.")] = 1,
    hadoop_queue: Annotated[
        int, typer.Option(help="Queue ID for Hadoop benchmarks.")
    ] = 1,
    sge_scripts: Annotated[
        str, typer.Option(help="Comma-separated list of SGE benchmark scripts.")
    ] = ",".join(DEFAULT_SGE_SCRIPTS),
    hadoop_inputs: Annotated[
        str, typer.Option(help="Comma-separated list of Hadoop input files.")
    ] = ",".join(DEFAULT_HADOOP_INPUTS),
    hadoop_jar: Annotated[
        str, typer.Option(help="Path to the Hadoop examples jar.")
    ] = DEFAULT_HADOOP_JAR,
    hadoop_class: Annotated[
        str, typer.Option(help="Hadoop main class to run.")
    ] = DEFAULT_HADOOP_CLASS,
    name_prefix: Annotated[
        str, typer.Option(help="Prefix for benchmark job names.")
    ] = "bench",
    delay: Annotated[
        float, typer.Option(help="Delay between submissions in seconds.")
    ] = 1.0,
    hadoop_quiet: Annotated[
        bool, typer.Option(help="Reduce Hadoop client logging for benchmark jobs.")
    ] = False,
):
    owner = os.getenv("USER")
    if not owner:
        print("[bold red]Error:[/bold red] USER env var is not set.")
        raise typer.Exit(1)

    sge_script_list = _split_csv(sge_scripts)
    hadoop_input_list = _split_csv(hadoop_inputs)
    if not sge_script_list:
        print("[bold red]Error:[/bold red] No SGE scripts provided.")
        raise typer.Exit(1)
    if not hadoop_input_list:
        print("[bold red]Error:[/bold red] No Hadoop inputs provided.")
        raise typer.Exit(1)

    jobs: List[BenchmarkJob] = []
    for index, script in enumerate(sge_script_list, start=1):
        jobs.append(
            BenchmarkJob(
                name=f"{name_prefix}-sge-{index}",
                queue=sge_queue,
                path=script,
                scheduler_type="S",
            )
        )
    for index, input_file in enumerate(hadoop_input_list, start=1):
        output_dir = f"{name_prefix}-out-{index}"
        options = f"{hadoop_class} {input_file} {output_dir}"
        if hadoop_quiet:
            options = f"{options} --ms-hadoop-quiet"
        jobs.append(
            BenchmarkJob(
                name=f"{name_prefix}-hadoop-{index}",
                queue=hadoop_queue,
                path=hadoop_jar,
                scheduler_type="H",
                options=options,
            )
        )

    errors: List[Tuple[BenchmarkJob, str]] = []
    successes = 0

    for bench_job in jobs:
        request_data = {
            "name": bench_job.name,
            "queue": bench_job.queue,
            "owner": owner,
            "path": bench_job.path,
            "options": bench_job.options,
            "pwd": os.getcwd(),
            "scheduler_type": bench_job.scheduler_type,
        }
        response, error = HTTP_Client().post_safe("/jobs", request_data)
        if error:
            error_details = (
                f"[bold cyan]Job:[/bold cyan] {bench_job.name}\n"
                f"[bold cyan]Queue:[/bold cyan] {bench_job.queue}\n"
                f"[bold cyan]Scheduler:[/bold cyan] {bench_job.scheduler_type}\n"
                f"[bold cyan]Path:[/bold cyan] {bench_job.path}\n"
                f"[bold cyan]Options:[/bold cyan] {bench_job.options or '(none)'}\n\n"
                f"{error}"
            )
            panel = Panel(
                error_details,
                title="[bold red]Benchmark Submission Failed[/bold red]",
                border_style="red",
            )
            print(panel)
            errors.append((bench_job, error))
        else:
            successes += 1
            try:
                response_message = response.json().get("message", "Job created.")
            except ValueError:
                response_message = response.text or "Job created."
            panel = Panel(
                f"[bold cyan]Response:[/bold cyan]\n{response_message}",
                title="[bold magenta]Benchmark Job Created[/bold magenta]",
                border_style="green",
            )
            print(panel)
        if delay > 0:
            sleep(delay)

    summary = (
        f"[bold cyan]Total:[/bold cyan] {len(jobs)}\n"
        f"[bold cyan]Succeeded:[/bold cyan] {successes}\n"
        f"[bold cyan]Failed:[/bold cyan] {len(errors)}"
    )
    summary_panel = Panel(
        summary,
        title="[bold magenta]Benchmark Submission Summary[/bold magenta]",
        border_style="green" if not errors else "red",
    )
    print(summary_panel)

    if errors:
        raise typer.Exit(1)
