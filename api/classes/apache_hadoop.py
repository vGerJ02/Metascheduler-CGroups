import os
import shlex
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.interfaces.node import Node
from api.interfaces.scheduler import Scheduler
from api.routers.jobs import update_job_status, set_job_scheduler_job_ref
import re
import getpass

HADOOP_HOME = "/usr/hdp/2.6.5.0-292/hadoop"
# JAVA_HOME = '/usr/lib/jvm/jre/'
JAVA_HOME = "/usr/lib/jvm/java-8-openjdk"
YARN_APP_ID_GRACE_SECONDS = 60


# export JAVA_HOME=/usr/lib/jvm/jre/ && /opt/hadoop/bin/yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar pi 2 4


class ApacheHadoop(Scheduler):
    """
    Apache Hadoop Scheduler

    """

    def __init__(self) -> None:
        super().__init__()
        self.name = "Apache Hadoop"
        self.cgroup_path = ""

    def __str__(self) -> str:
        return f"Apache Hadoop Scheduler: {self.master_node.ip}:{self.master_node.port}"

    @staticmethod
    def _ssh_user() -> str | None:
        return os.getenv("SSH_USER_HADOOP") or os.getenv("SSH_USER")

    def _init_hdfs_user_dir(self, user: str):
        """
        Initialize the necessary directories in HDFS
        """
        hdfs_user_dir = f"/user/{user}"
        check_cmd = f"export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/hdfs dfs -test -d {hdfs_user_dir}"
        mkdir_cmd = f"export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/hdfs dfs -mkdir -p {hdfs_user_dir}"
        self.master_node.send_command(
            f"{check_cmd} || {mkdir_cmd}",
            ssh_user_override=self._ssh_user(),
        )

    def update_job_list(self, metascheduler_queue: List[Job]):
        """
        Update the job list.
        Also update the job status in the database.

        As Apache Hadoop does not have a scheduler, this method is not implemented.

        """
        if not self.running_jobs:
            return
        still_running: List[Job] = []
        for job in self.running_jobs:
            if job.scheduler_job_ref:
                state, final_state = self._get_application_status(job.scheduler_job_ref)
                if state in {"RUNNING", "ACCEPTED", "SUBMITTED"}:
                    if state == "RUNNING":
                        job.status = JobStatus.RUNNING
                    still_running.append(job)
                    continue
                if state is None and final_state is None:
                    queued_at = getattr(job, "queued_at", None)
                    if queued_at and datetime.utcnow() - queued_at < timedelta(seconds=YARN_APP_ID_GRACE_SECONDS):
                        still_running.append(job)
                        continue
                    response = self._call_yarn_application()
                    if self._is_any_job_running(response):
                        still_running.append(job)
                        continue
                if final_state in {"SUCCEEDED"} or state in {"FINISHED"}:
                    update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
                    job.status = JobStatus.COMPLETED
                elif final_state in {"FAILED", "KILLED"} or state in {"FAILED", "KILLED"}:
                    update_job_status(job.id_, job.owner, JobStatus.ERROR)
                    job.status = JobStatus.ERROR
                else:
                    update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
                    job.status = JobStatus.COMPLETED
                self._reset_java_process_nice()
                continue

            queued_at = getattr(job, "queued_at", None)
            if queued_at and datetime.utcnow() - queued_at < timedelta(seconds=YARN_APP_ID_GRACE_SECONDS):
                still_running.append(job)
                continue

            response = self._call_yarn_application()
            if self._is_any_job_running(response):
                job.status = JobStatus.RUNNING
                still_running.append(job)
            else:
                update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
                job.status = JobStatus.COMPLETED
                self._reset_java_process_nice()
        self.running_jobs = still_running

    def get_job_list(self) -> List[Job]:
        """
        Get the list of jobs from the Apache Hadoop scheduler.

        As Apache Hadoop does not have a scheduler, this method will return the actual running job, if any.

        """
        return self.running_jobs

    def queue_job(self, job: Job):
        """
        Queue a job.

        As Apache Hadoop does not have a scheduler, this method will run the job immediately.

        """
        if self.running_jobs:
            # Refresh tracked running jobs before rejecting new submissions.
            # This avoids stale in-memory state causing false "already running" logs.
            self.update_job_list([])
            if self.running_jobs:
                print(f"There is already a job running. Only one job can run at a time. {self.running_jobs}")
                return
        try:
            self._call_yarn_jar(job)
            job.queued_at = datetime.utcnow()
            self.running_jobs.append(job)
            update_job_status(job.id_, job.owner, JobStatus.RUNNING)
            job.status = JobStatus.RUNNING
        except Exception as e:
            print(f"Error: {e}")
            update_job_status(job.id_, job.owner, JobStatus.ERROR)
            job.status = JobStatus.ERROR

    def _call_yarn_jar(self, job: Job):
        """
        Prepare the HDFS environment and run the Hadoop job.
        """
        quiet_mode = (
            "export HADOOP_ROOT_LOGGER=ERROR,console && "
            "export YARN_ROOT_LOGGER=ERROR,console && "
            "export MAPREDUCE_ROOT_LOGGER=ERROR,console"
            if job.quiet
            else ""
        )

        current_user = getpass.getuser()
        target_user = f"sudo -u {job.owner}" if current_user != job.owner else ""

        print(f"Targeting user: {target_user} current user is: {current_user}")

        self._init_hdfs_user_dir(job.owner)

        env_exports = [f"export JAVA_HOME={JAVA_HOME}"]
        if quiet_mode:
            env_exports.append(quiet_mode)
        env_cmd = " && ".join(env_exports)

        def on_output(line: str, is_stderr: bool):
            app_id = self._extract_application_id(line)
            if app_id and not job.scheduler_job_ref:
                job.scheduler_job_ref = app_id
                try:
                    set_job_scheduler_job_ref(job.id_, job.owner, app_id)
                except Exception as exc:
                    print(f"Error storing scheduler job ref: {exc}")

        self.master_node.send_command_async(
            f"{target_user} sh -c '{env_cmd} && cd {job.pwd} && {HADOOP_HOME}/bin/yarn jar {job.path} {job.options}'",
            on_output=on_output,
            ssh_user_override=self._ssh_user(),
        )

    def _call_yarn_application(self) -> str:
        """
        Call the yarn application -list command to get the list of running jobs.

        """
        response = self._run_yarn_command(
            "application -list -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING"
        )
        print(response)
        return response

    def _run_yarn_command(self, yarn_args: str) -> str:
        exports = [f"export JAVA_HOME={JAVA_HOME}"]
        hadoop_conf_dir = os.getenv("HADOOP_CONF_DIR")
        yarn_conf_dir = os.getenv("YARN_CONF_DIR")
        if hadoop_conf_dir:
            exports.append(f"export HADOOP_CONF_DIR={hadoop_conf_dir}")
        if yarn_conf_dir:
            exports.append(f"export YARN_CONF_DIR={yarn_conf_dir}")
        exports_cmd = " && ".join(exports)
        base_cmd = f"{exports_cmd} && {HADOOP_HOME}/bin/yarn {yarn_args}"
        login_shell_cmd = (
            "source /etc/profile >/dev/null 2>&1 || true; "
            "source ~/.bashrc >/dev/null 2>&1 || true; "
            f"{base_cmd}"
        )
        wrapped_cmd = f"bash -lc {shlex.quote(login_shell_cmd)}"
        yarn_cli_user = os.getenv("HADOOP_CLI_USER") or os.getenv("YARN_CLI_USER")
        ssh_user = os.getenv("SSH_USER")
        if yarn_cli_user and yarn_cli_user != ssh_user:
            return self.master_node.send_command(
                f"sudo -u {yarn_cli_user} {wrapped_cmd}",
                ssh_user_override=self._ssh_user(),
            )
        return self.master_node.send_command(
            wrapped_cmd,
            ssh_user_override=self._ssh_user(),
        )

    def _extract_application_id(self, text: str) -> Optional[str]:
        match = re.search(r"(application_\d+_\d+)", text)
        if match:
            return match.group(1)
        return None

    def _get_application_status(self, application_id: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = self._run_yarn_command(f"application -status {application_id}")
        except Exception as exc:
            response = str(exc)

        state_match = re.search(r"State\s*:\s*(\w+)", response)
        final_match = re.search(r"Final-State\s*:\s*(\w+)", response)
        state = state_match.group(1) if state_match else None
        final_state = final_match.group(1) if final_match else None
        return state, final_state

    def _is_any_job_running(self, response: str) -> bool:
        """
        Check if any job is running, parsing the response from the yarn application -list command.

        """
        match = re.search(r"Total number of applications.*:\s*(\d+)", response)
        if match:
            return int(match.group(1)) > 0
        return False

    def adjust_nice_of_all_jobs(self, new_nice: int):
        for node in self.nodes:
            ps_output = node.send_command(
                f"ps -eo pid,comm,nice",
                ssh_user_override=self._ssh_user(),
            )
            print(ps_output)
            job_processes_pid_nice: Tuple[int, int] = self._get_job_processes_from_ps(
                ps_output
            )
            for pid, actual_nice in job_processes_pid_nice:
                if actual_nice == new_nice:
                    continue
                node.send_command(
                    f"sudo renice {new_nice} {pid}",
                    ssh_user_override=self._ssh_user(),
                )

    def adjust_nice_of_job(self, job_pid: int, new_nice: int):
        """
        Adjust the nice value of a running job.

        """
        for node in self.nodes:
            node.send_command(
                f"renice {new_nice} {job_pid}",
                ssh_user_override=self._ssh_user(),
            )

    def get_all_jobs_info(
        self,
    ) -> List[Tuple[int, int, float, float, str, float, float, str]]:
        """
        Get the information of all running jobs

        """
        all_jobs_info: List[Tuple[int, int, float, float, str, float, float, str]] = []
        for node in self.nodes:
            ps_output = node.send_command(
                f"ps -eo pid,comm,nice,%cpu,%mem,user",
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
                        node.ip,
                    )
                    for pid, nice, cpu, mem, user in job_info
                ]
            )
        return all_jobs_info

    def _get_job_info_from_ps(
        self, ps_output: str
    ) -> List[Tuple[int, int, float, float, str]]:
        """
        Get the list of processes of the running jobs.

        Search for the hadoop processes and get the PID of the process and the nice value.

        """
        job_processes_pid_nice_cpu_mem = []
        lines = ps_output.split("\n")[1:]
        for line in lines:
            if not line:
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            if "java" in parts[1]:
                job_processes_pid_nice_cpu_mem.append(
                    (
                        int(parts[0]),
                        int(parts[2]),
                        float(parts[3]),
                        float(parts[4]),
                        parts[5],
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

    def _get_job_processes_from_ps(self, ps_output: str) -> Tuple[int, int]:
        job_processes_pid_nice = []
        lines = ps_output.split("\n")
        for line in lines:
            if "java" in line:
                job_processes_pid_nice.append(
                    (int(line.split()[0]), int(line.split()[2]))
                )
        return job_processes_pid_nice

    def _reset_java_process_nice(self):
        for node in self.nodes:
            ps_output = node.send_command(
                f"ps -eo pid,comm,nice",
                ssh_user_override=self._ssh_user(),
            )
            job_processes_pid_nice: Tuple[int, int] = self._get_job_processes_from_ps(
                ps_output
            )
            for pid, actual_nice in job_processes_pid_nice:
                if actual_nice == 0:
                    continue
                node.send_command(
                    f"renice 0 {pid}",
                    ssh_user_override=self._ssh_user(),
                )

    def get_hadoop_process_tree(self) -> list[str]:
        """
        Retrieves the full process tree associated with Hadoop components on the master node.

        This function executes a Bash script that:
        - Identifies key Hadoop processes (e.g., NameNode, DataNode, ResourceManager) using `jps`.
        - Extracts all system processes with their PID and PPID.
        - Recursively builds the tree of child processes starting from the Hadoop roots.
        - Returns a sorted, unique list of all related PIDs.

        Useful for applying resource control policies via cgroups to the entire Hadoop workload,
        ensuring consistent isolation and monitoring.
        """
        cmd = (
            "bash -c '"
            'jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager|FsShell|RunJar|MRAppMaster|ApplicationCLI|YarnChild|SecondaryNameNode" | awk "{print \\$1}" > /tmp/hadoop_roots.txt && '
            "ps -eo pid,ppid > /tmp/all_procs.txt && "
            "function get_children() { "
            "  local pid=$1; "
            "  echo $pid; "
            '  for child in $(awk -v p=$pid "$2==p {print \\$1}" /tmp/all_procs.txt); do '
            "    get_children $child; "
            "  done; "
            "} ; "
            'ALL=""; '
            "for pid in $(cat /tmp/hadoop_roots.txt); do "
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
