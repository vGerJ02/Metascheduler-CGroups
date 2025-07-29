from typing import List

from api.classes.cgroups_scheduler import CgroupsScheduler
from api.daemons.policies.planification_policy import PlanificationPolicy
from api.interfaces.job import Job
import time

MAX_NICE = 19
MIN_NICE_NO_ROOT = 0


class DynamicPolicy(PlanificationPolicy):
    ''' The Dynamic Policy is a planification policy that allows all schedulers
    to run jobs concurrently. This policy defines different initial weights
    for each scheduler, and the jobs' priority is defined by the scheduler's
    weight, behavior that is identical to the Shared Policy.

    The difference between the Dynamic Policy and the Shared Policy is that the
    Dynamic Policy adjusts the weights of the schedulers dynamically, based on
    the performance of the schedulers and the jobs.

    '''

    def __init__(self, policy: PlanificationPolicy):
        '''

        '''
        super().__init__(policy.schedulers, policy.highest_priority)

    def apply(self, to_be_queued_jobs: List[Job]):
        """Queue the next job if any, and adjust scheduler priorities afterwards."""
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
        """Dynamically adjusts priorities for all schedulers."""
        for scheduler in self.schedulers:
            actual_jobs_info = scheduler.get_all_jobs_info()
            if not actual_jobs_info:
                continue

            if isinstance(scheduler, CgroupsScheduler):
                self._adjust_cgroups_scheduler(scheduler, actual_jobs_info)
            else:
                self._adjust_classic_scheduler(scheduler, actual_jobs_info)

    def _adjust_cgroups_scheduler(self, scheduler, jobs_info):
        """
        Adjusts cgroup parameters dynamically based on job usage:
        - cpu.weight: for CPU share
        - memory.high: for memory limit
        """
        avg_cpu = sum(job[2] for job in jobs_info) / len(jobs_info)
        avg_mem = sum(job[3] for job in jobs_info) / len(jobs_info)

        # 🔧 Adjust CPU weight
        target_cpu_weight = scheduler.weight
        current_cpu_weight = scheduler.get_cpu_weight()
        new_cpu_weight = current_cpu_weight

        if avg_cpu > target_cpu_weight:
            new_cpu_weight = max(1, current_cpu_weight - 10)
        elif avg_cpu < target_cpu_weight:
            new_cpu_weight = min(100, current_cpu_weight + 10)

        print(f'[CgroupsScheduler] Adjusting CPU weight: {current_cpu_weight} → {new_cpu_weight}')
        scheduler.set_cpu_weight(new_cpu_weight)

        # 🔧 Adjust memory limit
        target_mem_limit = scheduler.weight * 10 ** 6  # e.g., weight = 30 ⇒ 30MB
        current_mem_limit = scheduler.get_memory_limit()
        new_mem_limit = current_mem_limit

        if avg_mem > scheduler.weight:
            new_mem_limit = max(10 ** 6, current_mem_limit - 10 * 10 ** 6)
        elif avg_mem < scheduler.weight:
            new_mem_limit = current_mem_limit + 10 * 10 ** 6

        print(f'[CgroupsScheduler] Adjusting MEM limit: {current_mem_limit} → {new_mem_limit}')
        scheduler.set_memory_limit(new_mem_limit)

    def _adjust_classic_scheduler(self, scheduler, jobs_info):
        """Adjusts nice values for classic schedulers based on job CPU/MEM usage and weight."""
        for job_info in jobs_info:
            pid = job_info[0]
            nice = job_info[1]
            cpu = job_info[2]
            mem = job_info[3]
            user = job_info[4]

            self._write_log_in_csv(f'{time.time()},{pid},{nice},{cpu},{mem},{user}')

            if nice == 0:
                scheduler.adjust_nice_of_job(
                    pid, self._calculate_nice_from_weight(scheduler.weight), user)
                continue

            if cpu > float(scheduler.weight) or mem > float(scheduler.weight):
                scheduler.adjust_nice_of_job(pid, min(nice + 1, MAX_NICE), user)
            elif cpu < scheduler.weight or mem < scheduler.weight:
                scheduler.adjust_nice_of_job(pid, max(nice - 1, MIN_NICE_NO_ROOT), user)

    def _write_log_in_csv(self, log: str):
        '''

        '''
        with open('log.csv', 'a') as file:
            file.write(log + '\n')

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
