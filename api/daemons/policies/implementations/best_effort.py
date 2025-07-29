from typing import List

from api.classes.cgroups_scheduler import CgroupsScheduler
from api.config.config import AppConfig
from api.daemons.policies.planification_policy import PlanificationPolicy
from api.interfaces.job import Job
from api.interfaces.node import Node

MINIMUM_NICE_VALUE = 19
DEFAULT_CPU_WEIGHT = 100
LOW_CPU_WEIGHT = 50
HIGH_CPU_WEIGHT = 200


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
        for scheduler in self.schedulers:
            print("Name " + scheduler.name)

            if isinstance(scheduler, CgroupsScheduler):
                if scheduler == self.highest_priority:
                    self._set_cpu_weight(scheduler.sge.cgroup_path, HIGH_CPU_WEIGHT)
                    self._set_cpu_weight(scheduler.hadoop.cgroup_path, HIGH_CPU_WEIGHT)
                elif len(scheduler.get_job_list()) > 0:
                    self._set_cpu_weight(scheduler.sge.cgroup_path, LOW_CPU_WEIGHT)
                    self._set_cpu_weight(scheduler.hadoop.cgroup_path, LOW_CPU_WEIGHT)
                else:
                    self._set_cpu_weight(scheduler.sge.cgroup_path, DEFAULT_CPU_WEIGHT)
                    self._set_cpu_weight(scheduler.hadoop.cgroup_path, DEFAULT_CPU_WEIGHT)
            else:
                if scheduler == self.highest_priority:
                    print(scheduler.name + str(self.highest_priority))
                    continue
                print("Si? 2")
                # 🎚️ Ajustem nice com fins ara
                if len(scheduler.get_job_list()) > 0:
                    scheduler.adjust_nice_of_all_jobs(MINIMUM_NICE_VALUE)

    def _set_cpu_weight(self, cgroup_path: str, weight: int):
        """
        Assigna el pes de CPU a un cgroup concret.
        """
        command = f"echo {weight} | sudo tee {cgroup_path}/cpu.weight"
        print(f"[INFO] Ajustant {cgroup_path}/cpu.weight a {weight}")
        self.nodes[0].send_command(command)

