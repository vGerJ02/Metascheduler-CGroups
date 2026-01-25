#!/usr/bin/env bash
set -euo pipefail

# Example variables (override via env or edit here)
USER_NAME="${USER_NAME:-${USER:-}}"
PYTHONPATH_DIR="${PYTHONPATH_DIR:-..}"
JOB_NAME="${JOB_NAME:-hadoop_job_1}"
JAR_PATH="${JAR_PATH:-/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar}"
QUEUE_ID="${QUEUE_ID:-1}"
SCHEDULER_TYPE="${SCHEDULER_TYPE:-H}"
JOB_OPTIONS="${JOB_OPTIONS:-wordcount 1000000.txt out1}"


# export USER="${USER_NAME}"
export PYTHONPATH="${PYTHONPATH_DIR}"

if [[ ! -d "${PYTHONPATH_DIR}" ]]; then
  echo "PYTHONPATH dir not found: ${PYTHONPATH_DIR}" >&2
  exit 1
fi

# if [[ ! -f "${JAR_PATH}" ]]; then
#   echo "Hadoop JAR not found: ${JAR_PATH}" >&2
#   exit 1
# fi

if [[ ! -f "main.py" ]]; then
  echo "main.py not found in current directory: $(pwd)" >&2
  exit 1
fi

python3 main.py send job \
  --name "${JOB_NAME}" \
  --path "${JAR_PATH}" \
  --queue "${QUEUE_ID}" \
  --scheduler-type "${SCHEDULER_TYPE}" \
  --options "${JOB_OPTIONS}"
