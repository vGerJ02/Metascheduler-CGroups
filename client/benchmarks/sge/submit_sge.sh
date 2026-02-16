#!/usr/bin/env bash
set -euo pipefail

# Submit a simple SGE benchmark job through the metascheduler client.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

API_IP="${API_IP:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
QUEUE_ID="${QUEUE_ID:-1}"
SUBMIT_USER="${SUBMIT_USER:-${USER:-}}"
PYTHON_BIN="${PYTHON_BIN:-python}"

JOB_NAME="${JOB_NAME:-sge-cpu-10s-$(date +%Y%m%d_%H%M%S)}"
JOB_SCRIPT="${JOB_SCRIPT:-${SCRIPT_DIR}/sge_openmp_10s.sh}"
JOB_OPTIONS="${JOB_OPTIONS:-}"
SCHEDULER_TYPE="${SCHEDULER_TYPE:-S}"

if [[ -z "${SUBMIT_USER}" ]]; then
  echo "SUBMIT_USER/USER is not set." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/client/main.py" ]]; then
  echo "client/main.py not found under ${ROOT_DIR}" >&2
  exit 1
fi

if [[ "${SCHEDULER_TYPE}" != "S" ]]; then
  echo "SCHEDULER_TYPE must be 'S' for SGE submission." >&2
  exit 1
fi

if [[ ! -f "${JOB_SCRIPT}" ]]; then
  echo "Benchmark script not found: ${JOB_SCRIPT}" >&2
  exit 1
fi

if [[ ! -x "${JOB_SCRIPT}" ]]; then
  chmod +x "${JOB_SCRIPT}"
fi

echo "Submitting SGE benchmark to metascheduler:"
echo "  API:            ${API_IP}:${API_PORT}"
echo "  Queue ID:       ${QUEUE_ID}"
echo "  Scheduler type: ${SCHEDULER_TYPE}"
echo "  Job name:       ${JOB_NAME}"
echo "  Job script:     ${JOB_SCRIPT}"
[[ -n "${JOB_OPTIONS}" ]] && echo "  Job options:    ${JOB_OPTIONS}"

CMD=(
  "${PYTHON_BIN}" "${ROOT_DIR}/client/main.py"
  --ip "${API_IP}" --port "${API_PORT}"
  send job
  --name "${JOB_NAME}"
  --queue "${QUEUE_ID}"
  --path "${JOB_SCRIPT}"
  --scheduler-type "${SCHEDULER_TYPE}"
)

if [[ -n "${JOB_OPTIONS}" ]]; then
  CMD+=(--options "${JOB_OPTIONS}")
fi

PYTHONPATH="${ROOT_DIR}" USER="${SUBMIT_USER}" "${CMD[@]}"

echo "Submitted. Check status with:"
echo "  PYTHONPATH=${ROOT_DIR} USER=${SUBMIT_USER} ${PYTHON_BIN} ${ROOT_DIR}/client/main.py --ip ${API_IP} --port ${API_PORT} get jobs"

