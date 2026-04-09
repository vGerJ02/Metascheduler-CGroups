#!/usr/bin/env bash
set -euo pipefail

# Example variables (override via env or edit here)
USER_NAME="${USER_NAME:-${USER:-}}"
USER_HOME="/home/${USER:-}"
PYTHONPATH_DIR="${PYTHONPATH_DIR:-..}"
JOB_NAME="${JOB_NAME:-sge_job_1}"
JOB_PATH="${JOB_PATH:-${USER_HOME}/sge_cpu_10s.sh}"
QUEUE_ID="${QUEUE_ID:-2}"
SCHEDULER_TYPE="${SCHEDULER_TYPE:-S}"
JOB_OPTIONS="${JOB_OPTIONS:-sleep input.txt outs}"

export PYTHONPATH="${PYTHONPATH_DIR}"

if [[ ! -d "${PYTHONPATH_DIR}" ]]; then
  echo "PYTHONPATH dir not found: ${PYTHONPATH_DIR}" >&2
  exit 1
fi

if [[ ! -f "main.py" ]]; then
  echo "main.py not found in current directory: $(pwd)" >&2
  exit 1
fi

python3 main.py send job \
  --name "${JOB_NAME}" \
  --path "${JOB_PATH}" \
  --queue "${QUEUE_ID}" \
  --scheduler-type S \
	--options "${JOB_OPTIONS}"
