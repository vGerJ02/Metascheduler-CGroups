import threading
import traceback
from time import sleep
from typing import List
from datetime import datetime
from api.config.config import AppConfig
from api.constants.job_status import JobStatus
from api.daemons.policies.planification_policy import PlanificationPolicy
from api.interfaces.job import Job
from api.routers.jobs import read_jobs
from api.utils.database_helper import DatabaseHelper
from api.utils.policy_factory import get_policy_by_name
from api.utils.singleton import Singleton
from rich import print

CYCLE_TIME = 1


def log(message):
    ''' Log messages to the console '''
    prefix = 'DAEMON:'.ljust(10)
    print(f'[cyan]{prefix}[/cyan]{message}')


class JobMonitorDaemon(metaclass=Singleton):
    ''' The Job Monitor Daemon is responsible for monitoring the jobs in the database,
    checking the scheduler queues, and making decisions based on the monitored jobs and queues.

    The daemon runs in a separate thread and is started and stopped by the main application.

    The Job Monitor Daemon is a Singleton class, meaning that only one instance of the class
    can be created. This is useful because we only need one instance of the daemon running
    in the application.
    '''

    config: AppConfig
    metascheduler_queue: List[Job] = []
    to_be_queued_jobs: List[Job] = []
    counter = 0
    planification_policy: PlanificationPolicy = None
    planification_policy_name = None

    def __init__(self):
        self._stop_event = threading.Event()

    def start(self):
        ''' Start the daemon '''
        log('Starting...')
        self.config = AppConfig()
        while not self._stop_event.is_set():
            try:
                self._execute_cycle()
            except Exception as exc:
                log(f'Unhandled daemon cycle error: {exc}')
                traceback.print_exc()
            sleep(CYCLE_TIME)

    def stop(self):
        log(f'Shutting down...')
        self._stop_event.set()

    def _execute_cycle(self):
        self._update_policy_if_needed()
        self._update_jobs_queue()
        self._update_scheduler_queues()
        self._collect_metrics()
        self._make_decisions()

    def _update_policy_if_needed(self):
        ''' Update the policy if needed '''
        if self.planification_policy_name != self.config.get_mode():
            self.planification_policy_name = self.config.get_mode()
            self.planification_policy = get_policy_by_name(
                self.planification_policy_name, PlanificationPolicy(
                    self.config.schedulers, self.config.get_highest_priority()))
            log(f'Using policy: {self.planification_policy_name}')

    def _update_jobs_queue(self):
        ''' Update the jobs queue '''
        log('Monitoring jobs...')
        self.metascheduler_queue = read_jobs(
            owner=None, status=None, queue=None)
        self.to_be_queued_jobs = read_jobs(
            owner=None, status=JobStatus.TO_BE_QUEUED, queue=None)
        log(f'Jobs in all queue: {len(self.metascheduler_queue)}')
        log(f'Jobs to be queued: {len(self.to_be_queued_jobs)}')
        pass

    def _update_scheduler_queues(self):
        ''' Update the scheduler queues '''
        log('Checking queues...')
        for scheduler in self.config.schedulers:
            scheduler.update_job_list(
                self.metascheduler_queue)
            log(f'{scheduler.name}: {len(scheduler.get_job_list())} jobs')
        pass

    def _collect_metrics(self):
        '''Collect CPU/RAM/Disk usage samples for every running job and persist them.'''
        log('Collecting metrics...')
        try:
            db = DatabaseHelper(self.config.schedulers)
            collected_at = datetime.utcnow()
            for scheduler in self.config.schedulers:
                running_jobs = [
                    job for job in scheduler.get_job_list()
                    if job.status == JobStatus.RUNNING
                ]
                if not running_jobs:
                    continue

                processes_info = scheduler.get_all_jobs_info()
                if not processes_info:
                    log(
                        f'No process samples for scheduler {scheduler.name} '
                        f'with {len(running_jobs)} running jobs'
                    )
                    continue

                usage_by_user = self._aggregate_usage_by_user(processes_info)
                usage_by_user_by_node = self._aggregate_usage_by_user_and_node(processes_info)
                if not usage_by_user:
                    log(
                        f'No usage grouped by user for scheduler {scheduler.name}. '
                        f'Raw process samples: {len(processes_info)}'
                    )
                    continue

                for job in running_jobs:
                    cpu, ram, read_bytes, write_bytes = self._usage_for_job(
                        job, running_jobs, usage_by_user)
                    if cpu == 0.0 and ram == 0.0:
                        owners_detected = ", ".join(sorted(usage_by_user.keys())) or "none"
                        log(
                            f'Zero metrics for job {job.id_} ({job.scheduler_type}) owner={job.owner}. '
                            f'Detected process owners: {owners_detected}'
                        )
                    db.insert_job_metric(
                        job.id_, cpu, ram, read_bytes, write_bytes, collected_at)

                    for node_ip, usage_by_user_in_node in usage_by_user_by_node.items():
                        n_cpu, n_ram, n_read_bytes, n_write_bytes = self._usage_for_job(
                            job, running_jobs, usage_by_user_in_node)
                        db.insert_job_node_metric(
                            job.id_, node_ip, n_cpu, n_ram, n_read_bytes, n_write_bytes, collected_at)
        except Exception as exc:
            log(f'Error collecting metrics: {exc}')

    def _make_decisions(self):
        ''' Make decisions based on the monitored jobs and queues '''
        log('Making decisions...')
        self.planification_policy.apply(self.to_be_queued_jobs)
        pass

    def _aggregate_usage_by_user(self, processes_info):
        '''Group CPU, RAM, and disk usage by process owner.'''
        usage = {}
        for process in processes_info:
            if len(process) < 7:
                continue
            user = process[4]
            if not user:
                continue
            if user not in usage:
                usage[user] = {'cpu': 0.0, 'ram': 0.0, 'read': 0.0, 'write': 0.0}
            usage[user]['cpu'] += process[2]
            usage[user]['ram'] += process[3]
            usage[user]['read'] += process[5]
            usage[user]['write'] += process[6]
        return usage

    def _aggregate_usage_by_user_and_node(self, processes_info):
        '''Group CPU, RAM, and disk usage by process owner per node.'''
        usage_by_node = {}
        for process in processes_info:
            if len(process) < 8:
                continue
            user = process[4]
            node_ip = process[7]
            if not user or not node_ip:
                continue
            if node_ip not in usage_by_node:
                usage_by_node[node_ip] = {}
            if user not in usage_by_node[node_ip]:
                usage_by_node[node_ip][user] = {'cpu': 0.0, 'ram': 0.0, 'read': 0.0, 'write': 0.0}
            usage_by_node[node_ip][user]['cpu'] += process[2]
            usage_by_node[node_ip][user]['ram'] += process[3]
            usage_by_node[node_ip][user]['read'] += process[5]
            usage_by_node[node_ip][user]['write'] += process[6]
        return usage_by_node

    def _usage_for_job(self, job: Job, running_jobs: List[Job], usage_by_user: dict[str, dict[str, float]]):
        '''Split the aggregated usage of a user across its running jobs.'''
        user_usage = usage_by_user.get(job.owner)
        if user_usage:
            same_user_jobs = [j for j in running_jobs if j.owner == job.owner]
            divisor = max(1, len(same_user_jobs))
            return (
                user_usage['cpu'] / divisor,
                user_usage['ram'] / divisor,
                user_usage['read'] / divisor,
                user_usage['write'] / divisor,
            )

        # Hadoop containers commonly run as service users (yarn/mapred/hdfs),
        # so owner-based attribution can miss real usage.
        if getattr(job, 'scheduler_type', '') == 'H':
            service_users = ('yarn', 'mapred', 'hdfs')
            service_usage = {'cpu': 0.0, 'ram': 0.0, 'read': 0.0, 'write': 0.0}
            found_service_usage = False
            for user in service_users:
                usage = usage_by_user.get(user)
                if not usage:
                    continue
                found_service_usage = True
                service_usage['cpu'] += usage['cpu']
                service_usage['ram'] += usage['ram']
                service_usage['read'] += usage['read']
                service_usage['write'] += usage['write']

            if found_service_usage:
                hadoop_jobs = [j for j in running_jobs if getattr(j, 'scheduler_type', '') == 'H']
                divisor = max(1, len(hadoop_jobs))
                return (
                    service_usage['cpu'] / divisor,
                    service_usage['ram'] / divisor,
                    service_usage['read'] / divisor,
                    service_usage['write'] / divisor,
                )

        return 0.0, 0.0, 0.0, 0.0
