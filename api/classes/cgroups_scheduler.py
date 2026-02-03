import time
from copy import deepcopy
from api.classes.apache_hadoop import ApacheHadoop
from api.classes.sge import SGE
from typing import List, Tuple, Optional
from api.interfaces.job import Job
from api.interfaces.node import Node
from api.interfaces.scheduler import Scheduler

V1_SHARES_MIN = 2
V1_SHARES_MAX = 262144


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
        self.parent_cgroup_paths: dict[str, str] = {}
        self.cgroups_version = "v2"
        self._last_usage = 0
        self._last_time = time.time()
        self._last_usage_v1 = 0
        self._last_time_v1 = time.time()

    def set_cgroups_version(self, version: str):
        normalized = str(version).strip().lower()
        if normalized in {"1", "v1", "legacy"}:
            self.cgroups_version = "v1"
        elif normalized in {"2", "v2", "unified"}:
            self.cgroups_version = "v2"
        else:
            raise ValueError(f"Unknown cgroups version '{version}'. Use 'v1' or 'v2'.")

    def set_master_node(self, node: Node):
        """Assign the master node and configure both SGE and Hadoop with it."""
        self.master_node = node

        # Clone the node for each scheduler (same IP and port, different object)
        sge_node = deepcopy(node)
        hadoop_node = deepcopy(node)

        self.sge.set_master_node(sge_node)
        self.hadoop.set_master_node(hadoop_node)

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
        if self.cgroups_version == "v1":
            cgroup_info = self.master_node.send_command(f"cat /proc/{pids[0]}/cgroup", critical=False)
            cpu_controller, cpu_rel_path = self._parse_v1_cgroup_path(cgroup_info, {"cpu", "cpuacct"})
            if not cpu_rel_path:
                print(f"⚠️ Failed to get cpu cgroup path for PID {pids[0]}")
                return
            cpu_controller = cpu_controller or "cpu"
            base_path = f"/sys/fs/cgroup/{cpu_controller}{cpu_rel_path}"
            full_path = f"{base_path}/{scheduler_type}"
            shares = self._v1_weight_to_shares(weight)
            cmd = f"echo {shares} | sudo tee {full_path}/cpu.shares"
            self.master_node.send_command(cmd, critical=False)
            print(f"✅ Assigned cpu.shares={shares} to cgroup '{full_path}'")
            return

        get_cgroup_path_cmd = f"cat /proc/{pids[0]}/cgroup | grep '^0::' | cut -d: -f3"
        cgroup_rel_path = self.master_node.send_command(get_cgroup_path_cmd, critical=False).strip()
        if not cgroup_rel_path:
            print(f"⚠️ Failed to get cgroup path for PID {pids[0]}")
            return

        full_path = f"/sys/fs/cgroup{cgroup_rel_path}/{scheduler_type}"

        cmd = f"echo {weight} | sudo tee {full_path}/cpu.weight"
        self.master_node.send_command(cmd, critical=False)
        print(f"✅ Assigned cpu.weight={weight} to cgroup '{full_path}'")

    def get_all_jobs_info(self) -> List[Tuple[int, int, float, float, str, float, float]]:
        return self.sge.get_all_jobs_info() + self.hadoop.get_all_jobs_info()


    def assign_pids_to_cgroup(self, pids: list[str], sub_cgroup_name: str):
        """Assign a list of PIDs to a specific sub-cgroup ('sge' or 'hadoop')."""
        if self.cgroups_version == "v1":
            self._assign_pids_to_cgroup_v1(pids, sub_cgroup_name)
            return
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

    def _assign_pids_to_cgroup_v1(self, pids: list[str], sub_cgroup_name: str):
        """Assign PIDs to cgroups v1 (cpu/cpuacct + memory)."""
        for pid in pids:
            cgroup_info = self.master_node.send_command(f"cat /proc/{pid}/cgroup", critical=False)
            cpu_controller, cpu_rel_path = self._parse_v1_cgroup_path(cgroup_info, {"cpu"})
            cpuacct_controller, cpuacct_rel_path = self._parse_v1_cgroup_path(cgroup_info, {"cpuacct"})
            mem_controller, mem_rel_path = self._parse_v1_cgroup_path(cgroup_info, {"memory"})

            if not cpu_rel_path:
                print(f"⚠️ No cpu cgroup found for PID {pid}")
                continue

            cpu_controller = cpu_controller or "cpu"
            cpu_base_path = f"/sys/fs/cgroup/{cpu_controller}{cpu_rel_path}"
            cpu_target = (
                cpu_base_path if sub_cgroup_name in cpu_rel_path.split('/')
                else f"{cpu_base_path}/{sub_cgroup_name}"
            )

            if self.parent_cgroup_path == "":
                path_parts = cpu_base_path.split('/')
                if path_parts[-1] in ["hadoop", "sge"]:
                    self.parent_cgroup_path = '/'.join(path_parts[:-1])
                else:
                self.parent_cgroup_path = cpu_base_path
                self.parent_cgroup_paths["cpu"] = self.parent_cgroup_path
                if cpuacct_rel_path:
                    cpuacct_controller = cpuacct_controller or cpu_controller
                    cpuacct_base = f"/sys/fs/cgroup/{cpuacct_controller}{cpuacct_rel_path}"
                    path_parts = cpuacct_base.split('/')
                    if path_parts[-1] in ["hadoop", "sge"]:
                        cpuacct_base = '/'.join(path_parts[:-1])
                    self.parent_cgroup_paths["cpuacct"] = cpuacct_base
                print(f"📦 Parent cgroup set to: {self.parent_cgroup_path}")

            self._ensure_v1_cgroup(cpu_target)
            self._move_pid_to_v1_cgroup(pid, cpu_target)

            if cpuacct_rel_path:
                cpuacct_controller = cpuacct_controller or cpu_controller
                cpuacct_base_path = f"/sys/fs/cgroup/{cpuacct_controller}{cpuacct_rel_path}"
                cpuacct_target = (
                    cpuacct_base_path if sub_cgroup_name in cpuacct_rel_path.split('/')
                    else f"{cpuacct_base_path}/{sub_cgroup_name}"
                )
                if cpuacct_target != cpu_target:
                    self._ensure_v1_cgroup(cpuacct_target)
                    self._move_pid_to_v1_cgroup(pid, cpuacct_target)

            if mem_rel_path:
                mem_controller = mem_controller or "memory"
                mem_base_path = f"/sys/fs/cgroup/{mem_controller}{mem_rel_path}"
                mem_target = (
                    mem_base_path if sub_cgroup_name in mem_rel_path.split('/')
                    else f"{mem_base_path}/{sub_cgroup_name}"
                )
                if "memory" not in self.parent_cgroup_paths:
                    path_parts = mem_base_path.split('/')
                    if path_parts[-1] in ["hadoop", "sge"]:
                        self.parent_cgroup_paths["memory"] = '/'.join(path_parts[:-1])
                    else:
                        self.parent_cgroup_paths["memory"] = mem_base_path
                self._ensure_v1_cgroup(mem_target)
                self._move_pid_to_v1_cgroup(pid, mem_target)

            if sub_cgroup_name == "sge" and self.sge.cgroup_path == "":
                self.sge.cgroup_path = cpu_target
            elif sub_cgroup_name == "hadoop" and self.hadoop.cgroup_path == "":
                self.hadoop.cgroup_path = cpu_target

    def _parse_v1_cgroup_path(self, cgroup_info: str, controllers: set[str]) -> Tuple[Optional[str], Optional[str]]:
        for line in cgroup_info.splitlines():
            parts = line.split(":")
            if len(parts) != 3:
                continue
            subsystems = set(parts[1].split(","))
            if controllers & subsystems:
                return parts[1], parts[2]
        return None, None

    def _ensure_v1_cgroup(self, path: str):
        check_cmd = f"test -d '{path}' && echo 'EXISTS' || echo 'MISSING'"
        exists = self.master_node.send_command(check_cmd, critical=False).strip()
        if exists == "MISSING":
            self.master_node.send_command(f"sudo mkdir -p '{path}'", critical=False)
        for node in self.nodes:
            try:
                node.send_command(f"sudo mkdir -p '{path}'", critical=False)
            except Exception:
                continue

    def _move_pid_to_v1_cgroup(self, pid: str, path: str):
        move_cmd = (
            f"bash -c \"if kill -0 {pid} 2>/dev/null; "
            f"then echo {pid} | sudo tee '{path}/tasks' > /dev/null; "
            f"fi\""
        )
        for node in self.nodes:
            try:
                node.send_command(move_cmd, critical=False)
            except Exception:
                continue

    def _v1_weight_to_shares(self, weight: int) -> int:
        weight_clamped = max(1, min(10000, int(weight)))
        shares = V1_SHARES_MIN + (weight_clamped - 1) * (V1_SHARES_MAX - V1_SHARES_MIN) / (10000 - 1)
        return max(V1_SHARES_MIN, min(V1_SHARES_MAX, int(round(shares))))

    def _v1_shares_to_weight(self, shares: int) -> int:
        shares_clamped = max(V1_SHARES_MIN, min(V1_SHARES_MAX, int(shares)))
        weight = 1 + (shares_clamped - V1_SHARES_MIN) * (10000 - 1) / (V1_SHARES_MAX - V1_SHARES_MIN)
        return max(1, min(10000, int(round(weight))))

    def get_cpu_weight(self) -> int:
        """Return the current cpu.weight value from the parent cgroup."""
        if not self.parent_cgroup_path:
            print("⚠️ Parent cgroup path is not defined.")
            return 0
        if self.cgroups_version == "v1":
            cmd = f"cat '{self.parent_cgroup_path}/cpu.shares'"
            result = self.master_node.send_command(cmd, critical=False).strip()
            try:
                shares = int(result)
                return self._v1_shares_to_weight(shares)
            except ValueError:
                print(f"⚠️ Could not interpret cpu.shares: {result}")
                return 0

        cmd = f"cat '{self.parent_cgroup_path}/cpu.weight'"
        result = self.master_node.send_command(cmd, critical=False).strip()
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
            if self.cgroups_version == "v1":
                cpuacct_path = self.parent_cgroup_paths.get("cpuacct") or self.parent_cgroup_path
                usage_file = f"{cpuacct_path}/cpuacct.usage"
                raw = self.master_node.send_command(f"cat '{usage_file}'", critical=False).strip()
                current_usage = int(raw)
                now = time.time()
                elapsed = now - self._last_time_v1

                delta_usage = current_usage - self._last_usage_v1
                self._last_usage_v1 = current_usage
                self._last_time_v1 = now

                if elapsed <= 0:
                    return 0.0

                cpu_percent = (delta_usage / (elapsed * 1_000_000_000)) * 100
                return max(0.0, cpu_percent)

            stat_file = f"{self.parent_cgroup_path}/cpu.stat"
            cmd = f"cat '{stat_file}'"
            raw = self.master_node.send_command(cmd, critical=False)

            usage_line = next(
                (l for l in raw.splitlines() if l.startswith("usage_usec")),
                None
            )
            if not usage_line:
                print("⚠️ cpu.stat does not contain 'usage_usec'")
                return 0.0

            current_usage = int(usage_line.split()[1])
            now = time.time()
            elapsed = now - self._last_time

            delta_usage = current_usage - self._last_usage
            self._last_usage = current_usage
            self._last_time = now

            if elapsed <= 0:
                return 0.0

            cpu_percent = (delta_usage / (elapsed * 1_000_000)) * 100
            return min(100.0, max(0.0, cpu_percent))

        except Exception as e:
            print(f"⚠️ Error reading cpu.stat with send_command(): {e}")
            return 0.0

    def set_cpu_weight(self, weight: int):
        """Set the cpu.weight value in the parent cgroup."""
        if not self.parent_cgroup_path:
            print("⚠️ Parent cgroup path is not defined.")
            return
        if self.cgroups_version == "v1":
            shares = self._v1_weight_to_shares(weight)
            cmd = f"sudo bash -c \"echo {shares} > '{self.parent_cgroup_path}/cpu.shares'\""
            for node in self.nodes:
                node.send_command(cmd, critical=False)
            print(f"✅ cpu.shares set to {shares} in {self.parent_cgroup_path}")
            return

        cmd = f"sudo bash -c \"echo {weight} > '{self.parent_cgroup_path}/cpu.weight'\""
        for node in self.nodes:
            node.send_command(cmd, critical=False)
        print(f"✅ cpu.weight set to {weight} in {self.parent_cgroup_path}")
