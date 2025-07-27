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
        self._create_cgroups()
        time.sleep(0.5)

        # Assignar processos Hadoop persistents al grup
        self.assign_hadoop_to_cgroup(self.hadoop.get_hadoop_process_tree(), "hadoop", )

    def set_nodes(self, nodes: List[Node]):
        self.nodes = nodes
        self.hadoop.set_nodes(nodes)
        self.sge.set_nodes(nodes)
        self.hadoop.set_nodes(nodes)

    def set_weight(self, weight: int):
        self.weight = weight
        self.sge.set_weight(weight)
        self.hadoop.set_weight(weight)

    def _create_cgroups(self):
        '''
        Inicialitza els cgroups SGE i Hadoop dins de la jerarquia cgroups v2,
        sense configurar els recursos.
        '''
        cmds = [
            # Activar controladors per la jerarquia
            "echo '+cpu +memory' | sudo tee /sys/fs/cgroup/cgroup.subtree_control",

            # Crear els cgroups sge i hadoop
            "sudo mkdir -p /sys/fs/cgroup/sge",
            "#sudo mkdir -p /sys/fs/cgroup/hadoop",

            # 3. Activar controladors també dins els subgrups (opcional)
            "sudo sh -c \"echo '+cpu +memory' > /sys/fs/cgroup/sge/cgroup.subtree_control\"",
            "#sudo sh -c \"echo '+cpu +memory' > /sys/fs/cgroup/hadoop/cgroup.subtree_control\"",
        ]

        for cmd in cmds:
            self.master_node.send_command(cmd)

    def queue_job(self, job: Job):
        if hasattr(job, 'scheduler_type'):
            if job.scheduler_type== "S":
                self.sge.queue_job(job)
                pass
            elif job.scheduler_type == "H":
                self.hadoop.queue_job(job)

                # Assignar els nous processos Hadoop al cgroup
                time.sleep(0.5)
                hadoop_job_pids = self.hadoop.get_hadoop_process_tree()
                self.assign_hadoop_to_cgroup(hadoop_job_pids, "hadoop", )
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

    def assign_hadoop_to_cgroup(self, pids: list[str], sub_cgroup_name: str = "hadoop"):
        """
        Per cada PID de Hadoop, troba el seu cgroup path, crea un subcgroup dins d'aquest
        i mou el PID a aquest subcgroup per gestionar-lo.
        """
        for pid in pids:
            # 1. Obtenim el path cgroup del PID (cgroup v2)
            get_cgroup_path_cmd = f"cat /proc/{pid}/cgroup | grep '^0::' | cut -d: -f3"
            cgroup_rel_path = self.master_node.send_command(get_cgroup_path_cmd).strip()
            if not cgroup_rel_path:
                print(f"⚠️ No s'ha trobat cgroup per PID {pid}")
                continue

            base_cgroup_path = f"/sys/fs/cgroup{cgroup_rel_path}"
            print("PAthh" + base_cgroup_path)
            target_sub_cgroup = f"{base_cgroup_path}/{sub_cgroup_name}"

            # 2. Comandes a executar:
            cmds = [
                f"mkdir -p '{target_sub_cgroup}'",
                # activar controllers al cgroup pare per permetre subcgroups
                f"echo '+cpu +memory +io' | sudo tee '{base_cgroup_path}/cgroup.subtree_control' || true",
                # mou el PID al subcgroup
                f"echo {pid} | sudo tee '{target_sub_cgroup}/cgroup.procs'"
            ]

            full_cmd = f"sudo bash -c \"{' && '.join(cmds)}\""
            print(f"Assignant PID {pid} al subcgroup {target_sub_cgroup} amb comanda:\n{full_cmd}")
            self.master_node.send_command(full_cmd)
