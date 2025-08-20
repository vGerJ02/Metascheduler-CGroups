import time
from copy import deepcopy
from time import sleep

from api.classes.apache_hadoop import ApacheHadoop
from api.classes.sge import SGE
from typing import List, Tuple
from api.interfaces.job import Job
from api.interfaces.node import Node
from api.interfaces.scheduler import Scheduler


class CgroupsScheduler(Scheduler):
    '''
    Scheduler that encapsulates SGE and Hadoop, and manages cgroups for both.
    '''

    def __init__(self):
        """Initialize the CgroupsScheduler with SGE and Hadoop instances."""
        super().__init__()
        self.name = "Cgroups"
        self.hadoop = ApacheHadoop()
        self.sge = SGE()
        self.parent_cgroup_path = ""
        # Primeres lectures: suposem 0 microsec i ara
        self._last_usage = 0
        self._last_time = time.time()

    def set_master_node(self, node: Node):
        """Assign the master node and configure both SGE and Hadoop with it."""
        self.master_node = node

        # Clone the node for each scheduler (same IP and port, different object)
        sge_node = deepcopy(node)
        hadoop_node = deepcopy(node)

        self.sge.set_master_node(sge_node)
        self.hadoop.set_master_node(hadoop_node)
        time.sleep(0.5)

        print("NODES DE CGROUPS: " + str(self.nodes))

        # Assign persistent Hadoop and SGE processes to their cgroups
        self.assign_pids_to_cgroup(self.hadoop.get_hadoop_process_tree(), "hadoop")
        self.assign_pids_to_cgroup(self.sge.get_sge_process_tree(), "sge")

    def set_nodes(self, nodes: List[Node]):
        """Assign the list of worker nodes to both SGE and Hadoop."""
        self.nodes = nodes
        self.hadoop.set_nodes(nodes)
        self.sge.set_nodes(nodes)

    def set_weight(self, weight: int):
        """Set the weight for both schedulers."""
        self.weight = weight
        self.sge.set_weight(weight)
        self.hadoop.set_weight(weight)

    def queue_job(self, job: Job):
        """Queue a job to the correct scheduler and assign it to a cgroup."""
        if hasattr(job, 'scheduler_type'):
            if job.scheduler_type == "S":
                self.sge.queue_job(job)
                time.sleep(0.5)
                sge_job_pids = self.sge.get_sge_process_tree()
                self.assign_pids_to_cgroup(sge_job_pids, "sge", )

            elif job.scheduler_type == "H":
                self.hadoop.queue_job(job)
                time.sleep(0.5)
                hadoop_job_pids = self.hadoop.get_hadoop_process_tree()
                self.assign_pids_to_cgroup(hadoop_job_pids, "hadoop", )
            else:
                raise ValueError("Unknown scheduler type")
        else:
            raise AttributeError("Job object must have a 'scheduler_type' attribute")

    def update_job_list(self, metascheduler_queue: List[Job]):
        """Update the job list for both schedulers based on the metascheduler queue."""
        self.sge.update_job_list(metascheduler_queue)
        self.hadoop.update_job_list(metascheduler_queue)
        self.running_jobs = self.sge.get_job_list() + self.hadoop.get_job_list()

    def get_job_list(self) -> List[Job]:
        """Return the combined list of running jobs from both schedulers."""
        return self.sge.get_job_list() + self.hadoop.get_job_list()

    def adjust_nice_of_all_jobs(self, new_nice: int):
        """Adjust the nice value for all jobs in both schedulers."""
        self.sge.adjust_nice_of_all_jobs(new_nice)
        self.hadoop.adjust_nice_of_all_jobs(new_nice)

    def adjust_nice_of_job(self, job_pid: int, new_nice: int, user: str):
        """Adjust the nice value for a specific job in both schedulers."""
        self.sge.adjust_nice_of_job(job_pid, new_nice, user)
        self.hadoop.adjust_nice_of_job(job_pid, new_nice)

    def adjust_cpu_weight(self, scheduler_type: str, weight: int):
        """
        Adjust the CPU weight (priority) for the specified scheduler's cgroup.
        :param scheduler_type: 'sge' or 'hadoop'
        :param weight: value from 1 to 10000 (for cgroups v2, default = 100)
        """
        # Find the first active PID to determine the base cgroup
        if scheduler_type == "sge":
            pids = self.sge.get_sge_process_tree()
        elif scheduler_type == "hadoop":
            pids = self.hadoop.get_hadoop_process_tree()
        else:
            raise ValueError("Scheduler type must be 'sge' or 'hadoop'")

        if not pids:
            print(f"⚠️ No active process found for '{scheduler_type}'")
            return

        # Get the cgroup path from the first PID and build the full subcgroup path
        get_cgroup_path_cmd = f"cat /proc/{pids[0]}/cgroup | grep '^0::' | cut -d: -f3"
        cgroup_rel_path = self.master_node.send_command(get_cgroup_path_cmd).strip()
        if not cgroup_rel_path:
            print(f"⚠️ Failed to get cgroup path for PID {pids[0]}")
            return

        full_path = f"/sys/fs/cgroup{cgroup_rel_path}/{scheduler_type}"

        # Set the new CPU weight
        cmd = f"echo {weight} | sudo tee {full_path}/cpu.weight"
        result = self.master_node.send_command(cmd)
        print(f"✅ Assigned cpu.weight={weight} to cgroup '{full_path}'")

    def get_all_jobs_info(self) -> List[Tuple[int, int, float, float, str]]:
        return self.sge.get_all_jobs_info() + self.hadoop.get_all_jobs_info()


    def assign_pids_to_cgroup(self, pids: list[str], sub_cgroup_name: str):
        """Assign a list of PIDs to a specific sub-cgroup ('sge' or 'hadoop')."""
        for pid in pids:
            get_cgroup_path_cmd = f"cat /proc/{pid}/cgroup | grep '^0::' | cut -d: -f3"
            cgroup_rel_path = self.master_node.send_command(get_cgroup_path_cmd).strip()
            if not cgroup_rel_path:
                print(f"⚠️ No cgroup found for PID {pid}")
                continue

            # Avoid duplicating subgroups: skip if sub_cgroup_name already exists in the path
            if sub_cgroup_name in cgroup_rel_path.split('/'):
                target_sub_cgroup = f"/sys/fs/cgroup{cgroup_rel_path}"
            else:
                target_sub_cgroup = f"/sys/fs/cgroup{cgroup_rel_path}/{sub_cgroup_name}"

            base_cgroup_path = f"/sys/fs/cgroup{cgroup_rel_path}"

            if self.parent_cgroup_path == "":
                path_parts = base_cgroup_path.split('/')
                if path_parts[-1] in ["hadoop", "sge"]:
                    self.parent_cgroup_path = '/'.join(path_parts[:-1])
                else:
                    self.parent_cgroup_path = base_cgroup_path
                print(f"📦 Parent cgroup set to: {self.parent_cgroup_path}")

            # 1. Enable controllers in the parent cgroup (to allow child cgroups with cpu.weight)
            enable_ctrls_cmd = (
                f"sudo bash -c \"echo '+cpu +memory +io' > '{base_cgroup_path}/cgroup.subtree_control'\" || true"
            )
            self.master_node.send_command(enable_ctrls_cmd)
            print(f"⚙️ Controllers enabled for {base_cgroup_path}")

            verify_ctrls_cmd = f"cat '{base_cgroup_path}/cgroup.subtree_control'"
            ctrls_result = self.master_node.send_command(verify_ctrls_cmd).strip()
            print(f"🔍 Controllers in parent: {ctrls_result}")

            # 2. Create sub-cgroup if it doesn't exist
            check_cmd = f"test -d '{target_sub_cgroup}' && echo 'EXISTS' || echo 'MISSING'"
            exists = self.master_node.send_command(check_cmd).strip()
            if exists == "MISSING":
                print(f"🆕 Creating subcgroup {target_sub_cgroup}")
                self.master_node.send_command(f"sudo mkdir -p '{target_sub_cgroup}'")
            else:
                print(f"✅ Subcgroup {target_sub_cgroup} already exists")

            # 3. Enable controllers inside the child subcgroup
            enable_ctrls_child_cmd = (
                f"sudo bash -c \"echo '+cpu +memory +io' > '{target_sub_cgroup}/cgroup.subtree_control'\" || true"
            )

            for node in self.nodes:
                try:
                    node.send_command(enable_ctrls_child_cmd)
                except:
                    continue
            print(f"⚙️ Controllers enabled inside {target_sub_cgroup}")

            # 4. Move the PID to the subcgroup
            move_cmd = (
                f"bash -c \"if kill -0 {pid} 2>/dev/null; "
                f"then echo {pid} | sudo tee '{target_sub_cgroup}/cgroup.procs'; "
                f"fi\""
            )

            for node in self.nodes:
                try:
                    node.send_command(move_cmd)
                except:
                    continue
            print(f"🔄 PID {pid} assigned to {target_sub_cgroup}")

            # 5. Save cgroup path if it's the first time
            if sub_cgroup_name == "sge" and self.sge.cgroup_path == "":
                self.sge.cgroup_path = target_sub_cgroup
            elif sub_cgroup_name == "hadoop" and self.hadoop.cgroup_path == "":
                self.hadoop.cgroup_path = target_sub_cgroup

    def get_cpu_weight(self) -> int:
        """Return the current cpu.weight value from the parent cgroup."""
        if not self.parent_cgroup_path:
            print("⚠️ Parent cgroup path is not defined.")
            return 0

        cmd = f"cat '{self.parent_cgroup_path}/cpu.weight'"
        result = self.master_node.send_command(cmd).strip()
        try:
            return int(result)
        except ValueError:
            print(f"⚠️ Could not interpret cpu.weight: {result}")
            return 0

    def get_cpu_usage(self) -> float:
        """
        Estimates CPU usage (%) from cgroup's cpu.stat via send_command().
        """
        try:
            # Llegeix cpu.stat via SSH/exec remot
            stat_file = f"{self.parent_cgroup_path}/cpu.stat"
            cmd = f"cat '{stat_file}'"
            raw = self.master_node.send_command(cmd)

            # Busca la línia usage_usec
            usage_line = next(
                (l for l in raw.splitlines() if l.startswith("usage_usec")),
                None
            )
            if not usage_line:
                print("⚠️ cpu.stat no conté 'usage_usec'")
                return 0.0

            current_usage = int(usage_line.split()[1])
            now = time.time()
            elapsed = now - self._last_time

            # Actualitza per la següent crida
            delta_usage = current_usage - self._last_usage
            self._last_usage = current_usage
            self._last_time = now

            if elapsed <= 0:
                return 0.0

            # 1 core = 1_000_000 usec per segon
            cpu_percent = (delta_usage / (elapsed * 1_000_000)) * 100
            return min(100.0, max(0.0, cpu_percent))

        except Exception as e:
            print(f"⚠️ Error llegint cpu.stat amb send_command(): {e}")
            return 0.0

    def set_cpu_weight(self, weight: int):
        """Set the cpu.weight value in the parent cgroup."""
        if not self.parent_cgroup_path:
            print("⚠️ Parent cgroup path is not defined.")
            return

        cmd = f"sudo bash -c \"echo {weight} > '{self.parent_cgroup_path}/cpu.weight'\""
        self.master_node.send_command(cmd)
        print(f"✅ cpu.weight set to {weight} in {self.parent_cgroup_path}")
