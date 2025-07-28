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
    Scheduler que encapsula SGE i Hadoop, i gestiona cgroups per ambdós.
    '''

    def __init__(self):
        super().__init__()
        self.name = "Cgroups"
        self.hadoop = ApacheHadoop()
        self.sge = SGE()

    def set_master_node(self, node: Node):
        self.master_node = node

        # Clonar el node per a cada scheduler (mateixa IP i port, objecte diferent)
        sge_node = deepcopy(node)
        hadoop_node = deepcopy(node)

        self.sge.set_master_node(sge_node)
        self.hadoop.set_master_node(hadoop_node)
        time.sleep(0.5)

        # Assignar processos Hadoop i SGE persistents al grup
        self.assign_pids_to_cgroup(self.hadoop.get_hadoop_process_tree(), "hadoop")
        self.assign_pids_to_cgroup(self.sge.get_sge_process_tree(), "sge")

    def set_nodes(self, nodes: List[Node]):
        self.nodes = nodes
        self.hadoop.set_nodes(nodes)
        self.sge.set_nodes(nodes)

    def set_weight(self, weight: int):
        self.weight = weight
        self.sge.set_weight(weight)
        self.hadoop.set_weight(weight)


    def queue_job(self, job: Job):
        if hasattr(job, 'scheduler_type'):
            if job.scheduler_type == "S":
                self.sge.queue_job(job)
                time.sleep(0.5)
                sge_job_pids = self.sge.get_sge_process_tree()
                self.assign_pids_to_cgroup(sge_job_pids, "sge", )

            elif job.scheduler_type == "H":
                self.hadoop.queue_job(job)

                # Assignar els nous processos Hadoop al cgroup
                time.sleep(0.5)
                hadoop_job_pids = self.hadoop.get_hadoop_process_tree()
                self.assign_pids_to_cgroup(hadoop_job_pids, "hadoop", )
            else:
                raise ValueError("Unknown scheduler type")
        else:
            raise AttributeError("Job object must have a 'scheduler_type' attribute")

    def update_job_list(self, metascheduler_queue: List[Job]):
        self.sge.update_job_list(metascheduler_queue)
        self.hadoop.update_job_list(metascheduler_queue)
        self.running_jobs = self.sge.get_job_list() + self.hadoop.get_job_list()

    def get_job_list(self) -> List[Job]:

        return self.sge.get_job_list() + self.hadoop.get_job_list()

    def adjust_nice_of_all_jobs(self, new_nice: int):
        self.sge.adjust_nice_of_all_jobs(new_nice)
        self.hadoop.adjust_nice_of_all_jobs(new_nice)

    def adjust_nice_of_job(self, job_pid: int, new_nice: int, user: str):
        self.sge.adjust_nice_of_job(job_pid, new_nice, user)
        self.hadoop.adjust_nice_of_job(job_pid, new_nice)

    def get_all_jobs_info(self) -> List[Tuple[int, int, float, float, str]]:
        return self.sge.get_all_jobs_info() + self.hadoop.get_all_jobs_info()

    def assign_pids_to_cgroup(self, pids: list[str], sub_cgroup_name: str):
        """
        Assigna una llista de PIDs a un subcgroup específic (p.ex. "sge" o "hadoop").
        Crea el subcgroup si no existeix, activant els controllers necessaris.
        Mostra missatges de debug per a cada pas.
        """
        for pid in pids:
            # 1. Obtenim el path cgroup del PID (cgroup v2)
            get_cgroup_path_cmd = f"cat /proc/{pid}/cgroup | grep '^0::' | cut -d: -f3"
            cgroup_rel_path = self.master_node.send_command(get_cgroup_path_cmd).strip()
            if not cgroup_rel_path:
                print(f"⚠️ No s'ha trobat cgroup per PID {pid}")
                continue

            # Evitar repetir si ja hi és
            if cgroup_rel_path.endswith(f"/{sub_cgroup_name}") or cgroup_rel_path == f"/{sub_cgroup_name}":
                print(f"✅ PID {pid} ja està dins un cgroup amb nom '{sub_cgroup_name}', no es fa res.")
                continue

            base_cgroup_path = f"/sys/fs/cgroup{cgroup_rel_path}"
            target_sub_cgroup = f"{base_cgroup_path}/{sub_cgroup_name}"

            # 2. Crear subcgroup si cal
            check_cmd = f"test -d '{target_sub_cgroup}' && echo 'EXISTS' || echo 'MISSING'"
            exists = self.master_node.send_command(check_cmd).strip()

            if exists == "MISSING":
                cmds = [
                    f"mkdir -p '{target_sub_cgroup}'",
                    f"echo '+cpu +memory +io' | sudo tee '{base_cgroup_path}/cgroup.subtree_control' || true"
                ]
                print(f"🆕 Creant subcgroup {target_sub_cgroup}")
                self.master_node.send_command(f"sudo bash -c \"{' && '.join(cmds)}\"")
            else:
                print(f"✅ Subcgroup {target_sub_cgroup} ja existeix")

            # 3. Assignar el PID
            move_cmd = f"echo {pid} | sudo tee '{target_sub_cgroup}/cgroup.procs'"
            self.master_node.send_command(move_cmd)
            print(f"🔄 PID {pid} assignat a {target_sub_cgroup}")

