from time import sleep
from typing import List, Tuple
from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.interfaces.scheduler import Scheduler
from api.routers.jobs import update_job_status, set_job_scheduler_job_id
import re

HADOOP_HOME = '/opt/hadoop'
# JAVA_HOME = '/usr/lib/jvm/jre/'
JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"


# export JAVA_HOME=/usr/lib/jvm/jre/ && /opt/hadoop/bin/yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar pi 2 4


class ApacheHadoop(Scheduler):
    '''
    Apache Hadoop Scheduler

    '''

    def __init__(self) -> None:
        super().__init__()
        self.name = 'Apache Hadoop'

    def __str__(self) -> str:
        return f'Apache Hadoop Scheduler: {self.master_node.ip}:{self.master_node.port}'

    def _init_hdfs_user_dir(self, user: str):
        """
        Inicialitza els directoris necessaris al HDFS
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

    """def _call_yarn_jar(self, job: Job):
        '''
        Call the yarn jar command to run the job.

        '''
        self.master_node.send_command_async(
            f'sudo -u {job.owner} sh -c \'export JAVA_HOME={JAVA_HOME} && cd {job.pwd} && {HADOOP_HOME}/bin/yarn jar {job.path} {job.options}\''
        )"""

    def _call_yarn_jar(self, job: Job):
        '''
        Prepara l’entorn HDFS i executa el job Hadoop.
        '''
        hdfs_user_dir = f"/user/{job.owner}"
        parts = job.options.split()
        if len(parts) < 3:
            raise ValueError("job.options ha de tenir almenys 3 parts: classe, input_file i output_dir")

        main_class = parts[0]
        input_file = parts[1]
        output_dir = parts[2]

        check_file_cmd = f"test -f {job.pwd}/{input_file}"
        try:
            self.master_node.send_command(check_file_cmd)
        except Exception:
            raise FileNotFoundError(f"[ERROR] File '{input_file}' doesn't exist in {job.pwd}")

        self._init_hdfs_user_dir(job.owner)

        cmds = [
            f"export JAVA_HOME={JAVA_HOME}",
            f"{HADOOP_HOME}/bin/hdfs dfs -put -f {input_file} {hdfs_user_dir}/",
            f"{HADOOP_HOME}/bin/hdfs dfs -rm -r -f {hdfs_user_dir}/{output_dir}",
            f"cd {job.pwd} && {HADOOP_HOME}/bin/yarn jar {job.path} {main_class} {hdfs_user_dir}/{input_file} {hdfs_user_dir}/{output_dir}"
        ]

        full_command = f"sudo -u {job.owner} sh -c '{' && '.join(cmds)}'"
        print("Executant comanda Hadoop:\n", full_command)

        self.master_node.send_command_async(full_command)

    def _call_yarn_application(self) -> str:
        '''
        Call the yarn application -list command to get the list of running jobs.

        '''
        response = self.master_node.send_command(
            f'export JAVA_HOME={JAVA_HOME} && {HADOOP_HOME}/bin/yarn application -list'
        )
        print("Response function:" + str(response))
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
            job_processes_pid_nice: Tuple[int, int] = self._get_job_processes_from_ps(
                ps_output)
            for pid, actual_nice in job_processes_pid_nice:
                if actual_nice == new_nice:
                    continue
                node.send_command(f'renice {new_nice} {pid}')

    def adjust_nice_of_job(self, job_pid: int, new_nice: int):
        '''
        Adjust the nice value of a running job.

        '''
        for node in self.nodes:
            node.send_command(f'renice {new_nice} {job_pid}')

    def get_all_jobs_info(self) -> List[Tuple[int, int, float, float]]:
        '''
        Get the information of all running jobs

        '''
        node = self.master_node
        ps_output = node.send_command(f'ps -eo pid,comm,nice,%cpu,%mem')
        return self._get_job_info_from_ps(ps_output)

    def _get_job_info_from_ps(self, ps_output: str) -> List[Tuple[int, int, float, float]]:
        '''
        Get the list of processes of the running jobs.

        Search for the sge_shepherd process and get the PID of the process and the nice value.

        '''
        job_processes_pid_nice_cpu_mem = []
        lines = ps_output.split('\n')
        for line in lines:
            if 'java' in line:
                job_processes_pid_nice_cpu_mem.append(
                    (int(line.split()[0]), int(line.split()[2]), float(line.split()[3]), float(line.split()[4])))
        return job_processes_pid_nice_cpu_mem

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
        cmd = (
            "bash -c '"
            "PIDS=$(jps | grep -E \"NameNode|DataNode|ResourceManager|NodeManager|FsShell\" | awk \"{print \\$1}\") && "
            "ps -eo pid,ppid > /tmp/all_procs.txt && "
            "ALL_PIDS=\"\" && "
            "for PID in $PIDS; do "
            "  CHILDREN=$(awk -v p=$PID \"$2==p {print \\$1}\" /tmp/all_procs.txt); "
            "  ALL_PIDS=\"$ALL_PIDS $CHILDREN $PID\"; "
            "done && "
            "echo $ALL_PIDS | tr \" \" \"\\n\" | sort -n | uniq | tac"
            "'"
        )
        output = self.master_node.send_command(cmd)
        return [pid.strip() for pid in output.strip().splitlines() if pid.strip().isdigit()]
