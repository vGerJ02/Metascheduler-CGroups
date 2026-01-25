from time import sleep
from typing import List, Tuple
from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.interfaces.node import Node
from api.interfaces.scheduler import Scheduler
from api.routers.jobs import update_job_status, set_job_scheduler_job_id
from api.interfaces.node import Node
import re
import os

HADOOP_HOME = '/usr/hdp/2.6.5.0-292/hadoop'
# JAVA_HOME = '/usr/lib/jvm/jre/'
JAVA_HOME = "/usr/lib/jvm/java-8-openjdk"


# export JAVA_HOME=/usr/lib/jvm/jre/ && /opt/hadoop/bin/yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar pi 2 4


class ApacheHadoop(Scheduler):
    '''
    Apache Hadoop Scheduler

    '''

    def __init__(self) -> None:
        super().__init__()
        self.name = 'Apache Hadoop'
        self.cgroup_path = ""

    def __str__(self) -> str:
        return f'Apache Hadoop Scheduler: {self.master_node.ip}:{self.master_node.port}'

    def _init_hdfs_user_dir(self, user: str):
        """
        Initialize the necessary directories in HDFS
        """
        hdfs_user_dir = f"/user/{user}"
        check_cmd = f"export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/hdfs dfs -test -d {hdfs_user_dir}"
        mkdir_cmd = f"export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/hdfs dfs -mkdir -p {hdfs_user_dir}"
        self.master_node.send_command(f"{check_cmd} || {mkdir_cmd}")

    def update_job_list(self, metascheduler_queue: List[Job]):
        '''
        Update the job list.
        Also update the job status in the database.

        As Apache Hadoop does not have a scheduler, this method is not implemented.

        '''
        if not self.running_jobs:
            return
        response = self._call_yarn_application()
        if not self._is_any_job_running(response):
            for job in self.running_jobs:
                update_job_status(job.id_, job.owner, JobStatus.COMPLETED)
                self._reset_java_process_nice()
            self.running_jobs = []

    def get_job_list(self) -> List[Job]:
        '''
        Get the list of jobs from the Apache Hadoop scheduler.

        As Apache Hadoop does not have a scheduler, this method will return the actual running job, if any.

        '''
        return self.running_jobs

    def queue_job(self, job: Job):
        '''
        Queue a job.

        As Apache Hadoop does not have a scheduler, this method will run the job immediately.

        '''
        if self.running_jobs:
            print('There is already a job running. Only one job can run at a time.')
            return
        try:
            self._call_yarn_jar(job)
            self.running_jobs.append(job)
            update_job_status(job.id_, job.owner, JobStatus.RUNNING)
        except Exception as e:
            print(f'Error: {e}')
            update_job_status(job.id_, job.owner, JobStatus.ERROR)

    def _call_yarn_jar(self, job: Job):
        '''
        Prepare the HDFS environment and run the Hadoop job.
        '''
        hdfs_user_dir = f"/user/{job.owner}"
        parts, meta = self._split_hadoop_options(job.options)
        if len(parts) < 3:
            raise ValueError("job.options ha de tenir almenys 3 parts: classe, input_file i output_dir")

        main_class = parts[0]
        input_file = parts[1]
        output_dir = parts[2]

        ssh_user = os.getenv('SSH_USER')
        ssh_home = f"/home/{ssh_user}" if ssh_user else str(job.pwd)
        input_path = f"{ssh_home}/{input_file}"
        check_file_cmd = f"test -f {input_path}"

        try:
            self.master_node.send_command(check_file_cmd)
        except Exception:
            raise FileNotFoundError(f"[ERROR] File '{input_file}' doesn't exist in {ssh_home}")

        self._init_hdfs_user_dir(job.owner)

        env_cmds = [
            f"export JAVA_HOME={JAVA_HOME}",
        ]
        if meta.get("quiet"):
            env_cmds.extend([
                "export HADOOP_ROOT_LOGGER=ERROR,console",
                "export YARN_ROOT_LOGGER=ERROR,console",
                "export MAPREDUCE_ROOT_LOGGER=ERROR,console",
            ])

        cmds = env_cmds + [
            f"{HADOOP_HOME}/bin/hdfs dfs -put -f {input_path} {hdfs_user_dir}/",
            f"{HADOOP_HOME}/bin/hdfs dfs -rm -r -f {hdfs_user_dir}/{output_dir}",
            f"cd {job.pwd} && {HADOOP_HOME}/bin/yarn jar {job.path} {main_class} {hdfs_user_dir}/{input_file} {hdfs_user_dir}/{output_dir}"
        ]

        full_command = f"sudo -u {job.owner} sh -c '{' && '.join(cmds)}'"
        self.master_node.send_command_async(full_command)

    def _split_hadoop_options(self, options: str) -> Tuple[List[str], dict]:
        tokens = options.split()
        meta = {"quiet": False}
        remaining: List[str] = []
        for token in tokens:
            if token == "--ms-hadoop-quiet":
                meta["quiet"] = True
                continue
            remaining.append(token)
        return remaining, meta

    def _call_yarn_application(self) -> str:
        '''
        Call the yarn application -list command to get the list of running jobs.

        '''
        response = self.master_node.send_command(
            f'export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/yarn application -list'
        )
        print(response)
        return response

    def _is_any_job_running(self, response: str) -> bool:
        '''
        Check if any job is running, parsing the response from the yarn application -list command.

        '''
        match = re.search(r"Total number of applications.*:\s*(\d+)", response)
        if match:
            return int(match.group(1)) > 0
        return False

    def adjust_nice_of_all_jobs(self, new_nice: int):
        for node in self.nodes:
            ps_output = node.send_command(f'ps -eo pid,comm,nice')
            print(ps_output)
            job_processes_pid_nice: Tuple[int, int] = self._get_job_processes_from_ps(
                ps_output)
            for pid, actual_nice in job_processes_pid_nice:
                if actual_nice == new_nice:
                    continue
                node.send_command(f'sudo renice {new_nice} {pid}')

    def adjust_nice_of_job(self, job_pid: int, new_nice: int):
        '''
        Adjust the nice value of a running job.

        '''
        for node in self.nodes:
            node.send_command(f'renice {new_nice} {job_pid}')

    def get_all_jobs_info(self) -> List[Tuple[int, int, float, float, str, float, float]]:
        '''
        Get the information of all running jobs

        '''
        node = self.master_node
        ps_output = node.send_command(f'ps -eo pid,comm,nice,%cpu,%mem,user')
        job_info = self._get_job_info_from_ps(ps_output)
        if not job_info:
            return job_info
        io_by_pid = self._get_io_by_pid(node, [info[0] for info in job_info])
        return [
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

    def _get_job_info_from_ps(self, ps_output: str) -> List[Tuple[int, int, float, float, str]]:
        '''
        Get the list of processes of the running jobs.

        Search for the hadoop processes and get the PID of the process and the nice value.

        '''
        job_processes_pid_nice_cpu_mem = []
        lines = ps_output.split('\n')[1:]
        for line in lines:
            if not line:
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            if 'java' in parts[1]:
                job_processes_pid_nice_cpu_mem.append(
                    (int(parts[0]), int(parts[2]), float(parts[3]), float(parts[4]), parts[5]))
        return job_processes_pid_nice_cpu_mem

    def _get_io_by_pid(self, node: Node, pids: List[int]) -> dict[int, tuple[float, float]]:
        if not pids:
            return {}
        pid_list = " ".join(str(pid) for pid in pids)
        cmd = (
            "bash -c '"
            f"for pid in {pid_list}; do "
            "read_bytes=$(sudo -n awk '/^read_bytes/ {print $2}' /proc/$pid/io 2>/dev/null || "
            "awk '/^read_bytes/ {print $2}' /proc/$pid/io 2>/dev/null || echo 0); "
            "write_bytes=$(sudo -n awk '/^write_bytes/ {print $2}' /proc/$pid/io 2>/dev/null || "
            "awk '/^write_bytes/ {print $2}' /proc/$pid/io 2>/dev/null || echo 0); "
            "read_bytes=${read_bytes:-0}; write_bytes=${write_bytes:-0}; "
            "echo \"$pid $read_bytes $write_bytes\"; "
            "done'"
        )
        output = node.send_command(cmd, critical=False)
        io_by_pid = {}
        for line in output.splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            try:
                io_by_pid[int(parts[0])] = (float(parts[1]), float(parts[2]))
            except ValueError:
                continue
        return io_by_pid

    def _get_job_processes_from_ps(self, ps_output: str) -> Tuple[int, int]:
        job_processes_pid_nice = []
        lines = ps_output.split('\n')
        for line in lines:
            if 'java' in line:
                job_processes_pid_nice.append(
                    (int(line.split()[0]), int(line.split()[2])))
        return job_processes_pid_nice

    def _reset_java_process_nice(self):
        for node in self.nodes:
            ps_output = node.send_command(f'ps -eo pid,comm,nice')
            job_processes_pid_nice: Tuple[int, int] = self._get_job_processes_from_ps(
                ps_output)
            for pid, actual_nice in job_processes_pid_nice:
                if actual_nice == 0:
                    continue
                node.send_command(f'renice 0 {pid}')

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
            "jps | grep -E \"NameNode|DataNode|ResourceManager|NodeManager|FsShell|RunJar|MRAppMaster|ApplicationCLI|YarnChild|SecondaryNameNode\" | awk \"{print \\$1}\" > /tmp/hadoop_roots.txt && "
            "ps -eo pid,ppid > /tmp/all_procs.txt && "
            "function get_children() { "
            "  local pid=$1; "
            "  echo $pid; "
            "  for child in $(awk -v p=$pid \"$2==p {print \\$1}\" /tmp/all_procs.txt); do "
            "    get_children $child; "
            "  done; "
            "} ; "
            "ALL=\"\"; "
            "for pid in $(cat /tmp/hadoop_roots.txt); do "
            "  ALL=\"$ALL $(get_children $pid)\"; "
            "done; "
            "echo $ALL | tr \" \" \"\\n\" | sort -n | uniq'"
        )
        output = self.master_node.send_command(cmd)
        return [pid.strip() for pid in output.strip().splitlines() if pid.strip().isdigit()]
