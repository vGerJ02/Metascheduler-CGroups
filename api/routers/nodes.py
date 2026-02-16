from fastapi import APIRouter, HTTPException
from typing import Optional

from api.config.config import AppConfig


router = APIRouter(
    prefix='/nodes',
    tags=['Nodes'],
    responses={404: {'description': 'Not found'}},
)


@router.get('')
async def read_nodes():
    return [{'id': node.id_, 'ip': node.ip, 'port': node.port, 'is_alive': node.is_alive}
            for node in AppConfig().nodes]


@router.get('/master')
async def read_master_node():
    master_node = AppConfig().master_node
    return {
        'id': master_node.id_,
        'ip': master_node.ip,
        'port': master_node.port,
        'is_alive': master_node.is_alive
    }


_METRICS_CMD = (
    "bash -lc '"
    "read cpu user nice system idle iowait irq softirq steal guest guest_nice < /proc/stat; "
    "total1=$((user+nice+system+idle+iowait+irq+softirq+steal)); idle1=$((idle+iowait)); "
    "sleep 0.3; "
    "read cpu user nice system idle iowait irq softirq steal guest guest_nice < /proc/stat; "
    "total2=$((user+nice+system+idle+iowait+irq+softirq+steal)); idle2=$((idle+iowait)); "
    "cpu=$(awk -v t1=$total1 -v t2=$total2 -v i1=$idle1 -v i2=$idle2 "
    "\"BEGIN {dt=t2-t1; di=i2-i1; if (dt<=0) {print 0} else {printf \\\"%.2f\\\", (100*(dt-di)/dt)} }\"); "
    "mem=$(awk \" /MemTotal/ {t=\\$2} /MemAvailable/ {a=\\$2} "
    "END {if (t>0) printf \\\"%.2f\\\", (t-a)/t*100; else print 0}\" /proc/meminfo); "
    "disk=$(df -P / | awk \"NR==2 {gsub(/%/,\\\"\\\",\\$5); print \\$5}\"); "
    "load=$(awk \"{print \\$1}\" /proc/loadavg); "
    "echo cpu=$cpu; echo mem=$mem; echo disk=$disk; echo load=$load'"
)


def _parse_metrics_output(output: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    metrics: dict[str, float] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        try:
            metrics[key] = float(value)
        except ValueError:
            continue
    return (
        metrics.get("cpu"),
        metrics.get("mem"),
        metrics.get("disk"),
        metrics.get("load"),
    )


@router.get('/metrics')
async def read_nodes_metrics():
    nodes_metrics = []
    for node in AppConfig().nodes:
        # if not node.is_alive:
        #     nodes_metrics.append({
        #         'id': node.id_,
        #         'ip': node.ip,
        #         'port': node.port,
        #         'is_alive': node.is_alive,
        #         'cpu_percent': None,
        #         'ram_percent': None,
        #         'disk_percent': None,
        #         'load1': None,
        #         'error': 'node not reachable',
        #     })
        #     continue
        output = node.send_command(_METRICS_CMD, critical=False)
        cpu, mem, disk, load = _parse_metrics_output(output)
        error = None if cpu is not None else output.strip()[:200] or 'unknown error'
        nodes_metrics.append({
            'id': node.id_,
            'ip': node.ip,
            'port': node.port,
            'is_alive': node.is_alive,
            'cpu_percent': cpu,
            'ram_percent': mem,
            'disk_percent': disk,
            'load1': load,
            'error': error,
        })
    return nodes_metrics


@router.get('/{node_id}')
async def read_node(node_id: int):
    if node_id >= len(AppConfig().nodes):
        raise HTTPException(status_code=404, detail='Node not found')
    node = AppConfig().nodes[node_id]

    return {
        'id': node.id_,
        'ip': node.ip,
        'port': node.port,
        'is_alive': node.is_alive
    }
