from typing import List

from api.classes.cgroups_scheduler import CgroupsScheduler
from api.daemons.policies.planification_policy import PlanificationPolicy
from api.interfaces.job import Job

MAX_NICE = 19
MIN_NICE_NO_ROOT = 0
CGROUPS_V2_MIN = 1
CGROUPS_V2_MAX = 10000


class SharedPolicy(PlanificationPolicy):
    ''' The Shared Policy is a planification policy that allows all schedulers
    to run jobs concurrently. This policy defines different weights for each
    scheduler, and the jobs' priority is defined by the scheduler's weight.

    The jobs are queued in the order they are received, and the nice value of
    the jobs is adjusted according to the scheduler's weight.

    '''

    def __init__(self, policy: PlanificationPolicy):
        '''

        '''
        super().__init__(policy.schedulers, policy.highest_priority)

    def apply(self, to_be_queued_jobs: List[Job]):
        '''

        '''
        if not to_be_queued_jobs:
            print('No jobs to be queued, but adjusting priorities...')
            self._adjust_priorities()
            return

        next_job = to_be_queued_jobs[0]
        next_job_scheduler_index = next_job.queue - 1

        print(
            f'Queuing job {next_job.id_} from scheduler {next_job_scheduler_index}...')
        self.schedulers[next_job_scheduler_index].queue_job(next_job)

        self._adjust_priorities()

    def _adjust_priorities(self):
        '''
        Adjust job priorities based on either:
        - CPU usage from cgroups (if scheduler is CgroupsScheduler)
        - Static weight (otherwise)
        '''
        for scheduler in self.schedulers:
            if isinstance(scheduler, CgroupsScheduler):
                cpu_usage = scheduler.get_cpu_usage()
                dynamic_weight = self._calculate_weight_from_cpu_usage(cpu_usage)

                print(
                    f'[SharedPolicy] (CGroups) Scheduler {scheduler.name} dynamic weight: {dynamic_weight}'
                )
                scheduler.set_cpu_weight(dynamic_weight)
            else:
                nice_value = self._calculate_nice_from_weight(scheduler.weight)
                print(
                    f'[SharedPolicy] Scheduler {scheduler.name} static weight: {scheduler.weight} → nice: {nice_value}')
                scheduler.adjust_nice_of_all_jobs(nice_value)

    def _calculate_nice_from_weight(self, weight: int) -> int:
        '''
        Calculate the nice value of a job based on the scheduler's weight.
        Assumes that the scheduler's weight is a percentage value, from 0 to 100.
        Assumes that the nice value of a job is a value from 0 to 19.
        Assumes that the sum of all weights is 100.

        '''
        if weight == 0:
            return MAX_NICE
        proportional_cpu_usage = weight / 100
        nice_range = MAX_NICE - MIN_NICE_NO_ROOT
        nice = MIN_NICE_NO_ROOT + (1 - proportional_cpu_usage) * nice_range
        return round(nice)

    def _calculate_weight_from_cpu_usage(self, cpu_usage: float) -> int:
        '''
        Converts CPU usage percentage into a cgroups v2 weight.

        In cgroups v2, CPU weights range from 1 (minimum) to 10000 (maximum),
        where higher weights give more CPU allocation relative to other groups.

        Parameters:
            cpu_usage (float): The CPU usage of the scheduler, in percentage (0–100).

        Returns:
            int: The corresponding cgroup weight, scaled to the 1–10000 range.

        Steps:
        1. Clamp the input CPU usage between 0 and 100 to avoid invalid values.
        2. Scale the percentage into the cgroups weight range (CGROUPS_V2_MIN to CGROUPS_V2_MAX).
        3. Round the result to the nearest integer to get a valid cgroup weight.
        '''
        usage = max(0.0, min(cpu_usage, 100.0))
        weight = CGROUPS_V2_MIN + (usage / 100) * (CGROUPS_V2_MAX - CGROUPS_V2_MIN)
        return round(weight)
