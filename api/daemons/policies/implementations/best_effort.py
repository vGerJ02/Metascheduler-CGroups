from typing import List

from api.classes.cgroups_scheduler import CgroupsScheduler
from api.classes.sge import SGE
from api.config.config import AppConfig
from api.daemons.policies.planification_policy import PlanificationPolicy
from api.interfaces.job import Job
from api.interfaces.node import Node

MINIMUM_NICE_VALUE = 19
DEFAULT_CPU_WEIGHT = 1000
LOW_CPU_WEIGHT = 5000
HIGH_CPU_WEIGHT = 10000


class BestEffortPolicy(PlanificationPolicy):
    ''' The Best Effort Policy is a planification policy that allows all schedulers
    to run jobs concurrently, but prioritizes the jobs of the scheduler with the
    highest priority. This policy is useful for running jobs that require the
    maximum amount of resources available., while still allowing other jobs to
    run concurrently.

    So, if a job from the highest priority scheduler is available, it will be
    queued. If no job from the highest priority scheduler is available, the
    jobs from the other schedulers will be queued, and monitored to reduce the
    priority of them.
    '''
    nodes: List[Node] = []

    def __init__(self, policy: PlanificationPolicy):
        '''

        '''
        super().__init__(policy.schedulers, policy.highest_priority)
        self.nodes = AppConfig().nodes

    def apply(self, to_be_queued_jobs: List[Job]):
        """Queue the next job (if any), and then adjust priorities for all schedulers."""
        if not to_be_queued_jobs:
            print('No jobs to be queued, but adjusting priorities...')
            self._adjust_priorities()
            return

        next_job = to_be_queued_jobs[0]
        next_job_scheduler_index = next_job.queue - 1

        print(f'Queuing job {next_job.id_} from scheduler {next_job_scheduler_index}...')
        self.schedulers[next_job_scheduler_index].queue_job(next_job)

        self._adjust_priorities()

    def _adjust_priorities(self):
        """Adjusts CPU weights (cgroups) or nice values based on priority and job presence."""
        for scheduler in self.schedulers:
            if isinstance(scheduler, CgroupsScheduler):
                if scheduler == self.highest_priority:
                    weight = HIGH_CPU_WEIGHT
                elif len(scheduler.get_job_list()) > 0:
                    weight = LOW_CPU_WEIGHT
                else:
                    weight = DEFAULT_CPU_WEIGHT

                print(f"[BestEffortPolicy] (CGroups) Scheduler '{scheduler.name}' dynamic weight: {weight}")
                scheduler.set_cpu_weight(weight)

            else:
                if scheduler == self.highest_priority:
                    print(f"[BestEffortPolicy] Scheduler '{scheduler.name}' is highest priority (no cgroup)")
                    continue
                if len(scheduler.get_job_list()) > 0:
                    scheduler.adjust_nice_of_all_jobs(MINIMUM_NICE_VALUE)
                    print(f"[BestEffortPolicy] Scheduler '{scheduler.name}' nice set to {MINIMUM_NICE_VALUE}")
