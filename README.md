# 🚀 Metascheduler (Cgroups-enhanced)

This is the **next version of the original Python job metascheduler by Pere Muñoz**, extended and improved as part of my thesis. It manages workflows in a **hybrid cluster environment** (Hadoop + SGE) while introducing **fine-grained resource management using cgroups v2**, allowing better control of CPU per scheduler.

---

## ✨ Key Features

- Homogeneous node architecture: Each node runs both SGE and Hadoop, managed under a unified metascheduler.
- CgroupsScheduler: Encapsulates SGE and Hadoop, creating sub-cgroups for each scheduler to control CPU weights and memory limits dynamically.
- Dynamic resource allocation: Supports multiple scheduling policies (Best Effort, Shared, Exclusive, Dynamic) to optimize throughput and fairness in hybrid environments.
- Concurrent HPC + Big Data workloads: Jobs from SGE and Hadoop can coexist on the same nodes with minimal interference.
- Monitoring and adjustment: Resource usage is tracked in real-time, and CPU weights and memory can be adjusted via API and SSH/Fabric commands.

---

## 📁 Repository Structure

- `api/`: Core of the metascheduler. Manages jobs, scheduling, and resource allocation via cgroups.
- `client/`: Typer-based CLI to submit, monitor, and control jobs through the API.
- `local_test_scenario/`: Docker Compose setup for SGE and Hadoop nodes to simulate a hybrid cluster. Includes scripts to generate test jobs and input data.

---

## 🛠️ Installation

Each component has its own installation process. See the README in the respective directories.  
Dependencies are managed with `pipenv`.

---

## 🧪 Usage

1. Set up the API with the master nodes of the cluster frameworks using the configuration file.
2. Use the client CLI to submit jobs, apply scheduling policies, and monitor resource usage.
3. For local testing, use the `local_test_scenario` folder containing Docker Compose setups for SGE and Hadoop.

---

## 🧵 Test Jobs

SGE test jobs: Sum-of-squares or user-defined shell scripts.

Hadoop test jobs: Wordcount problems with different dataset sizes.

Test execution: Use `test.sh` in the `local_test_scenario` folder to submit multiple jobs to the metascheduler queue automatically.

---

## ⚙️ Scheduling Policies

The metascheduler supports different scheduling policies, implemented via `CgroupsScheduler`:

- **Dynamic**: Adjusts CPU weights and memory limits automatically based on job usage, maximizing throughput and fairness.
- **Best Effort**: Jobs take unused CPU and memory resources when available.
- **Shared**: Divides resources evenly between SGE and Hadoop.
- **Exclusive**: Assigns resources exclusively to each scheduler to avoid interference, but may reduce overall efficiency.

`CgroupsScheduler` is responsible for creating sub-cgroups, assigning jobs, and adjusting CPU weights dynamically, enabling fine-grained resource control in hybrid HPC + Big Data workloads.

---

## 📚 References

Original metascheduler by Pere Muñoz:  
https://github.com/peremunoz/metascheduler.git

This version extends the original metascheduler with cgroups-based resource control, hybrid scheduling policies, and improved performance monitoring.
