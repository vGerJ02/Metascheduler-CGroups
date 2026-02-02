import threading
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
            self._execute_cycle()
            sleep(CYCLE_TIME)

    def stop(self):
        log(f'Shutting down...')
        self._stop_event.set()

    def _execute_cycle(self):
        self._update_policy_if_needed()
        self._update_jobs_queue()
        self._collect_metrics()
        self._update_scheduler_queues()
        self._make_decisions()
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
                running_jobs = scheduler.get_job_list()
                if not running_jobs:
                    continue

                processes_info = scheduler.get_all_jobs_info()
                usage_by_user = self._aggregate_usage_by_user(processes_info)
                if not usage_by_user:
                    continue

                for job in running_jobs:
                    cpu, ram, read_bytes, write_bytes = self._usage_for_job(
                        job, running_jobs, usage_by_user)
                    db.insert_job_metric(
                        job.id_, cpu, ram, read_bytes, write_bytes, collected_at)
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

    def _usage_for_job(self, job: Job, running_jobs: List[Job], usage_by_user: dict[str, dict[str, float]]):
        '''Split the aggregated usage of a user across its running jobs.'''
        user_usage = usage_by_user.get(job.owner)
        if not user_usage:
            return 0.0, 0.0, 0.0, 0.0
        same_user_jobs = [j for j in running_jobs if j.owner == job.owner]
        divisor = max(1, len(same_user_jobs))
        return (
            user_usage['cpu'] / divisor,
            user_usage['ram'] / divisor,
            user_usage['read'] / divisor,
            user_usage['write'] / divisor,
        )
