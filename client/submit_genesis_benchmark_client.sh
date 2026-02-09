#!/usr/bin/env bash
set -euo pipefail

# Submit a GENESIS benchmark job to metascheduler (SGE path).
# Benchmark reference: https://mdgenesis.org/benchmark/
# The benchmark script must exist on the master node filesystem.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

API_IP="${API_IP:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
QUEUE_ID="${QUEUE_ID:-1}"
PYTHON_BIN="${PYTHON_BIN:-python}"
SUBMIT_USER="${SUBMIT_USER:-${USER:-}}"

JOB_NAME="${JOB_NAME:-genesis-benchmark}"
SCHEDULER_TYPE="${SCHEDULER_TYPE:-S}"
GENESIS_BENCH_DIR="${GENESIS_BENCH_DIR:-/home/${SUBMIT_USER}/genesis_benchmark_input}"
BENCHMARK_SCRIPT="${BENCHMARK_SCRIPT:-${GENESIS_BENCH_DIR}/run_benchmark.sh}"
JOB_OPTIONS="${JOB_OPTIONS:-}"

if [[ -z "${SUBMIT_USER}" ]]; then
  echo "SUBMIT_USER/USER is not set." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/client/main.py" ]]; then
  echo "client/main.py not found under ${ROOT_DIR}" >&2
  exit 1
fi

if [[ "${SCHEDULER_TYPE}" != "S" ]]; then
  echo "SCHEDULER_TYPE must be 'S' for GENESIS benchmark submission via SGE." >&2
  exit 1
fi

if [[ ! -x "${BENCHMARK_SCRIPT}" ]]; then
  echo "Benchmark script not found or not executable: ${BENCHMARK_SCRIPT}" >&2
  echo "Set BENCHMARK_SCRIPT to the executable benchmark runner on the master node." >&2
  exit 1
fi

echo "Submitting GENESIS benchmark to metascheduler:"
echo "  API:             ${API_IP}:${API_PORT}"
echo "  Queue ID:        ${QUEUE_ID}"
echo "  Scheduler type:  ${SCHEDULER_TYPE}"
echo "  Job name:        ${JOB_NAME}"
echo "  Benchmark script:${BENCHMARK_SCRIPT}"
[[ -n "${JOB_OPTIONS}" ]] && echo "  Job options:     ${JOB_OPTIONS}"

CMD=(
  "${PYTHON_BIN}" "${ROOT_DIR}/client/main.py"
  --ip "${API_IP}" --port "${API_PORT}"
  send job
  --name "${JOB_NAME}"
  --queue "${QUEUE_ID}"
  --path "${BENCHMARK_SCRIPT}"
  --scheduler-type "${SCHEDULER_TYPE}"
)

if [[ -n "${JOB_OPTIONS}" ]]; then
  CMD+=(--options "${JOB_OPTIONS}")
fi

PYTHONPATH="${ROOT_DIR}" USER="${SUBMIT_USER}" "${CMD[@]}"

echo "Submitted. Check status with:"
echo "  PYTHONPATH=${ROOT_DIR} USER=${SUBMIT_USER} ${PYTHON_BIN} ${ROOT_DIR}/client/main.py --ip ${API_IP} --port ${API_PORT} get jobs"
