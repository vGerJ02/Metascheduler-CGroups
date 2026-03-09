import os
import time
import getpass
import re
from typing import List, Tuple
from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.interfaces.node import Node
from api.interfaces.scheduler import Scheduler
from api.routers.jobs import set_job_scheduler_job_id, update_job_status
import xml.etree.ElementTree as ET

SGE_ROOT = '/usr/local/sge/'
QSTAT = SGE_ROOT + 'bin/lx-amd64/qstat'
QSUB = SGE_ROOT + 'bin/lx-amd64/qsub'
QACCT = SGE_ROOT + 'bin/lx-amd64/qacct'

class SGE(Scheduler):
    """
    SGE Scheduler

    """

    _last_job_list_id: List[int] = []

    def __init__(self) -> None:
        super().__init__()
        self.name = "SGE"
        self.cgroup_path = ""

    def __str__(self) -> str:
        return f"SGE Scheduler: {self.master_node.ip}:{self.master_node.port}"

    @staticmethod
    def _ssh_user() -> str | None:
        return os.getenv("SSH_USER_SGE") or os.getenv("SSH_USER")

    def update_job_list(self, metascheduler_queue: List[Job]):
        """
        Update the internal job list.
        Also update the job status in the database.

        """
        qstat = self._call_qstat()
        jobs_id_state: Tuple[str, int] = self._parse_qstat(qstat)
        current_ids = {job_id for job_id, _ in jobs_id_state}
        actual_jobs: List[Job] = []
        for job_id_state in jobs_id_state:
            job = next(
                (
                    job
                    for job in metascheduler_queue
                    if job.scheduler_job_id == job_id_state[0]
                ),
                None,
            )
            if job is None:
                continue
            actual_jobs.append(job)
            state = str(job_id_state[1])
            if state == "Eqw" or "E" in state:
                update_job_status(job.id_, job.owner, JobStatus.ERROR)
                job.status = JobStatus.ERROR
                self._log_eqw_details(job)
                actual_jobs.remove(job)
                continue
            if "r" in state or "t" in state:
                update_job_status(job.id_, job.owner, JobStatus.RUNNING)
                job.status = JobStatus.RUNNING
                continue
            if "qw" in state:
                update_job_status(job.id_, job.owner, JobStatus.QUEUED)
                job.status = JobStatus.QUEUED

        # Reconcile jobs that disappeared from qstat before being observed as running.
        # This happens for short jobs and would otherwise leave them stuck in QUEUED.
        pending_sge_jobs = [
            job for job in metascheduler_queue
            if getattr(job, "scheduler_type", "") == "S"
            and job.scheduler_job_id
            and job.status in {JobStatus.QUEUED, JobStatus.RUNNING}
            and job.scheduler_job_id not in current_ids
        ]
        for job in pending_sge_jobs:
            accounted_outcome = self._accounted_job_outcome(job)
            if accounted_outcome == JobStatus.COMPLETED:
                update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
                job.status = JobStatus.COMPLETED
            elif accounted_outcome == JobStatus.ERROR:
                update_job_status(job.id_, job.owner, JobStatus.ERROR)
                job.status = JobStatus.ERROR

        ended_jobs_id = [
            job_id
            for job_id in self._last_job_list_id
            if job_id not in [job.scheduler_job_id for job in actual_jobs]
        ]
        ended_jobs = [
            job for job in metascheduler_queue if job.scheduler_job_id in ended_jobs_id
        ]
        for job in ended_jobs:
            update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
            job.status = JobStatus.COMPLETED

        self.running_jobs = actual_jobs
        self._last_job_list_id = [job.scheduler_job_id for job in actual_jobs]

    def get_job_list(self) -> List[Job]:
        """
        Get the list of jobs from the SGE scheduler

        """
        return self.running_jobs

    def _call_qstat(self) -> str:
        """
        Call the qstat command to get the list of jobs

        """
        qstat_xml = self.master_node.send_command(
            f"export SGE_ROOT={SGE_ROOT} && {QSTAT} -xml",
            ssh_user_override=self._ssh_user(),
        )
        return qstat_xml

    def _parse_qstat(self, qstat_output) -> Tuple[int, str]:
        """
        Parse the output of the qstat -xml command

        """
        root = ET.fromstring(qstat_output)
        jobs_queue: Tuple[int, str] = []
        for job_list in root.findall(".//job_list"):
            job_id = job_list.find("JB_job_number").text
            current_job_state = job_list.find("state").text
            jobs_queue.append((int(job_id), current_job_state))
        return jobs_queue

    def queue_job(self, job: Job):
        """
        Queue a job

        """
        try:
            sge_job_id = self._call_qsub(job)
            set_job_scheduler_job_id(job.id_, job.owner, sge_job_id)
            update_job_status(job.id_, job.owner, JobStatus.QUEUED)
            job.scheduler_job_id = sge_job_id
            job.status = JobStatus.QUEUED
            self.running_jobs.append(job)
        except Exception as e:
            print(
                f"[SGE] Failed to submit job id={job.id_} name={job.name} "
                f"owner={job.owner} path={job.path} options='{job.options}': {e}"
            )
            try:
                update_job_status(job.id_, job.owner, JobStatus.ERROR)
            except Exception as status_exc:
                print(
                    f"[SGE] Could not mark job id={job.id_} as ERROR after submission failure: {status_exc}"
                )
            raise e

    def _call_qsub(self, job: Job) -> int:
        """
        Call the qsub command to queue the job
        and return the job id assigned by the scheduler.
        """
        current_user = getpass.getuser()
        target_user = f"sudo -u {job.owner}" if current_user != job.owner else ""
        qsub_options = job.qsub_options.strip() if getattr(job, "qsub_options", "") else ""
        qsub_options_segment = f"{qsub_options} " if qsub_options else ""
        qsub_cmd = (
            f"{target_user} sh -c 'export SGE_ROOT={SGE_ROOT} && cd {job.pwd} "
            f"&& {QSUB} -N {job.name} -o {job.pwd} -e {job.pwd} {qsub_options_segment}{job.path} {job.options}'"
        )
        message = self.master_node.send_command(
            qsub_cmd,
            ssh_user_override=self._ssh_user(),
        )
        try:
            assigned_job_id = self._parse_qsub(message)
            return assigned_job_id
        except Exception as exc:
            print(
                f"[SGE] qsub returned unexpected output for job id={job.id_} name={job.name}: {message.strip()}"
            )
            print(f"[SGE] qsub command was: {qsub_cmd}")
            raise RuntimeError(f"Unable to parse qsub output: {exc}") from exc

    def _log_eqw_details(self, job: Job):
        """Log details when SGE puts a job into Eqw state."""
        current_user = getpass.getuser()
        target_user = f"sudo -u {job.owner}" if current_user != job.owner else ""
        qstat_detail_cmd = (
            f"{target_user} sh -c 'export SGE_ROOT={SGE_ROOT} && {QSTAT} -j {job.scheduler_job_id}'"
        )
        details = self.master_node.send_command(
            qstat_detail_cmd,
            critical=False,
            ssh_user_override=self._ssh_user(),
        ).strip()
        print(
            f"[SGE] Job entered Eqw state: id={job.id_}, scheduler_job_id={job.scheduler_job_id}, "
            f"owner={job.owner}, name={job.name}, path={job.path}, "
            f"qsub_options='{job.qsub_options}', options='{job.options}'"
        )
        if details:
            print(f"[SGE] qstat -j details for {job.scheduler_job_id}:\n{details}")
        print(
            f"[SGE] Check SGE output/error files under {job.pwd} "
            f"(usually {job.name}.o{job.scheduler_job_id} and {job.name}.e{job.scheduler_job_id})."
        )

    def _parse_qsub(self, qsub_output) -> int:
        """
        Parse the output of the qsub command

        """
        return int(qsub_output.split()[2])

    def _call_qacct_for_job(self, job: Job) -> str:
        current_user = getpass.getuser()
        target_user = f"sudo -u {job.owner}" if current_user != job.owner else ""
        qacct_cmd = (
            f"{target_user} sh -c 'export SGE_ROOT={SGE_ROOT} && {QACCT} -j {job.scheduler_job_id}'"
        )
        return self.master_node.send_command(
            qacct_cmd,
            critical=False,
            ssh_user_override=self._ssh_user(),
        )

    @staticmethod
    def _extract_int_field(text: str, field: str) -> int | None:
        match = re.search(rf"^{field}\s+(.+)$", text, re.MULTILINE)
        if not match:
            return None
        value = match.group(1).strip().split()[0]
        try:
            return int(value)
        except ValueError:
            return None

    def _accounted_job_outcome(self, job: Job) -> JobStatus | None:
        qacct_output = self._call_qacct_for_job(job)
        if not qacct_output:
            return None
        lowered = qacct_output.lower()
        if "not found" in lowered or "error:" in lowered and "job" in lowered and "not" in lowered:
            return None

        failed = self._extract_int_field(qacct_output, "failed")
        exit_status = self._extract_int_field(qacct_output, "exit_status")

        if failed is None and exit_status is None:
            return None
        if (failed is not None and failed != 0) or (exit_status is not None and exit_status != 0):
            return JobStatus.ERROR
        return JobStatus.COMPLETED

    def get_all_jobs_info(
        self,
    ) -> List[Tuple[int, int, float, float, str, float, float]]:
        """
        Get the information of all running jobs

        """
        all_jobs_info: List[Tuple[int, int, float, float, str, float, float]] = []
        for node in self.nodes:
            ps_output = node.send_command(
                f"ps -eo pid,comm,nice,%cpu,%mem,ppid,user",
                ssh_user_override=self._ssh_user(),
            )
            job_info = self._get_job_info_from_ps(ps_output)
            if not job_info:
                continue
            io_by_pid = self._get_io_by_pid(node, [info[0] for info in job_info])
            all_jobs_info.extend(
                [
                    (
                        pid,
                        nice,
                        cpu,
                        mem,
                        user,
                        io_by_pid.get(pid, (0.0, 0.0))[0],
                        io_by_pid.get(pid, (0.0, 0.0))[1],
                    )
                    for pid, nice, cpu, mem, user in job_info
                ]
            )
        return all_jobs_info

    def adjust_nice_of_all_jobs(self, new_nice: int):
        """
        Adjust the nice value of all running jobs' processes.

        """

        current_user = getpass.getuser()
        for node in self.nodes:
            ps_output = node.send_command(
                "ps -eo pid,comm,nice,ppid,user",
                ssh_user_override=self._ssh_user(),
            )
            job_processes_pid_nice: Tuple[int, int, str] = (
                self._get_job_processes_from_ps(ps_output)
            )
            for pid, actual_nice, user in job_processes_pid_nice:
                if actual_nice == new_nice:
                    continue

                target_user = f"sudo -u {user}" if current_user != user else ""
                node.send_command(
                    f"{target_user} sh -c 'renice {new_nice} {pid}'",
                    critical=False,
                    ssh_user_override=self._ssh_user(),
                )

    def adjust_nice_of_job(self, job_pid: int, new_nice: int, user: str):
        """
        Adjust the nice value of a running job.

        """

        current_user = getpass.getuser()
        target_user = f"sudo -u {user}" if current_user != user else ""
        for node in self.nodes:

            node.send_command(
                f"{target_user} sh -c 'renice {new_nice} {job_pid}'",
                critical=False,
                ssh_user_override=self._ssh_user(),
            )

    def _get_job_info_from_ps(
        self, ps_output: str
    ) -> List[Tuple[int, int, float, float, str]]:
        """
        Get the list of processes of the running jobs.

        Search for the sge_shepherd process and get the PID of the process and the nice value.

        """
        job_processes_pid_nice_cpu_mem = []
        sge_executors = []
        lines = ps_output.split("\n")[1:]
        for line in lines:
            if "sge_shepherd" in line:
                sge_executors.append(int(line.split()[0]))
        if not sge_executors:
            return job_processes_pid_nice_cpu_mem
        for line in lines:
            if not line:
                continue
            if int(line.split()[5]) in sge_executors:
                job_processes_pid_nice_cpu_mem.append(
                    (
                        int(line.split()[0]),
                        int(line.split()[2]),
                        float(line.split()[3]),
                        float(line.split()[4]),
                        line.split()[6],
                    )
                )
        return job_processes_pid_nice_cpu_mem

    def _get_io_by_pid(
        self, node: Node, pids: List[int]
    ) -> dict[int, tuple[float, float]]:
        if not pids:
            return {}
        pid_list = " ".join(str(pid) for pid in pids)
        cmd = (
            "bash -c '"
            f"for pid in {pid_list}; do "
            "source='unknown'; read_bytes=0; write_bytes=0; "
            "if [ ! -e /proc/$pid/io ]; then source='missing_pid'; "
            "elif read_bytes=$(awk '/^read_bytes/ {print $2}' /proc/$pid/io 2>/dev/null) "
            " && write_bytes=$(awk '/^write_bytes/ {print $2}' /proc/$pid/io 2>/dev/null); then source='direct'; "
            "else source='permission_denied_or_restricted'; fi; "
            "read_bytes=${read_bytes:-0}; write_bytes=${write_bytes:-0}; "
            'echo "$pid $read_bytes $write_bytes $source"; '
            "done'"
        )
        output = node.send_command(
            cmd,
            critical=False,
            ssh_user_override=self._ssh_user(),
        )
        io_by_pid = {}
        for line in output.splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            try:
                pid = int(parts[0])
                read_bytes = float(parts[1])
                write_bytes = float(parts[2])
                source = parts[3]
                io_by_pid[pid] = (read_bytes, write_bytes)
                if source != "direct":
                    print(
                        f"IO read fallback for PID {pid} on node {node.ip}: "
                        f"source={source}, read={read_bytes}, write={write_bytes}"
                    )
            except ValueError:
                continue
        return io_by_pid

    def _get_job_processes_from_ps(self, ps_output: str) -> Tuple[int, int, str]:
        """
        Get the list of processes of the running jobs.

        Search for the sge_shepherd process and get the PID of the process, the nice value and the user.

        """
        job_processes_pid_nice = []
        sge_executors = []
        lines = ps_output.split("\n")[1:]
        for line in lines:
            if "sge_shepherd" in line:
                sge_executors.append(int(line.split()[0]))
        if not sge_executors:
            return job_processes_pid_nice
        for line in lines:
            if not line:
                continue
            if int(line.split()[3]) in sge_executors:
                job_processes_pid_nice.append(
                    (int(line.split()[0]), int(line.split()[2]), line.split()[4])
                )
        return job_processes_pid_nice

    def get_sge_process_tree(self) -> list[str]:
        """
        Retrieves the full process tree associated with Sun Grid Engine (SGE) components on the master node.

        This function executes a Bash script that:
        - Locates core SGE processes (e.g., sge_qmaster, sge_execd, sge_shepherd) using `ps`.
        - Extracts all system processes with their PID and PPID.
        - Recursively builds the tree of child processes starting from the SGE roots.
        - Returns a sorted, unique list of all related PIDs.

        Enables fine-grained resource control using cgroups across the entire SGE execution environment,
        improving fairness and system stability.
        """
        cmd = (
            "bash -c '"
            'ps -eo pid,cmd | grep -E "sge_qmaster|sge_execd|sge_shadowd|sge_shepherd" '
            '| grep -v grep | awk "{print \\$1}" > /tmp/sge_roots.txt && '
            "ps -eo pid,ppid > /tmp/all_procs.txt && "
            "function get_children() { "
            "  local pid=$1; "
            "  echo $pid; "
            '  for child in $(awk -v p=$pid "$2==p {print \\$1}" /tmp/all_procs.txt); do '
            "    get_children $child; "
            "  done; "
            "} ; "
            'ALL=""; '
            "for pid in $(cat /tmp/sge_roots.txt); do "
            '  ALL="$ALL $(get_children $pid)"; '
            "done; "
            'echo $ALL | tr " " "\\n" | sort -n | uniq\''
        )
        output = self.master_node.send_command(
            cmd,
            ssh_user_override=self._ssh_user(),
        )
        return [
            pid.strip() for pid in output.strip().splitlines() if pid.strip().isdigit()
        ]
