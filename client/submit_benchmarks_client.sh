#!/bin/bash
# Submit the Hadoop benchmark suite to the metascheduler API using the CLI client.
# By default it sends the existing benchmarks/v1/run_benchmarks.sh as a single job.

# Env vars
#   API_IP / API_PORT   - where the API is listening (defaults 0.0.0.0:8000)
#   QUEUE_ID            - queue id to submit to
#   SCHEDULER_TYPE      - scheduler type flag for the API (default: S, to allow arbitrary shell)
#   JOB_NAME            - job name (default: hadoop-benchmarks)
#   JOB_OPTIONS         - extra scheduler options (passed through to qsub when S scheduler is used)
#   BENCHMARK_USER      - user to attach to the job (defaults to current $USER)

set -euo pipefail

ROOT_DIR="/home/gjaimejuan/Metascheduler-CGroups"
BENCHMARK_SCRIPT="${ROOT_DIR}/benchmarks/v1/run_benchmarks.sh"
QUEUE_ID=1
JOB_OPTIONS=
export PYTHONPATH=..

if [[ ! -x "${BENCHMARK_SCRIPT}" ]]; then
  echo "Benchmark script not found or not executable at ${BENCHMARK_SCRIPT}"
  exit 1
fi

API_IP="${API_IP:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
QUEUE_ID="${QUEUE_ID:-}"
SCHEDULER_TYPE="${SCHEDULER_TYPE:-S}"
JOB_NAME="${JOB_NAME:-hadoop-benchmarks}"
JOB_OPTIONS="${JOB_OPTIONS:-}"
SUBMIT_USER="${BENCHMARK_USER:-${USER}}"

# Try to detect the Hadoop queue id if not supplied
if [[ -z "${QUEUE_ID}" ]]; then
    echo "Could not auto-detect queue id. Set QUEUE_ID explicitly and retry."
    exit 1
fi

echo "Submitting Hadoop benchmarks with:"
echo "  API:        ${API_IP}:${API_PORT}"
echo "  Queue ID:   ${QUEUE_ID}"
echo "  Scheduler:  ${SCHEDULER_TYPE}"
echo "  User:       ${SUBMIT_USER}"
echo "  Job name:   ${JOB_NAME}"
[[ -n "${JOB_OPTIONS}" ]] && echo "  Job options: ${JOB_OPTIONS}"

CMD=(pipenv --python ~/.local/share/mamba/envs/metascheduler/bin/python run python "${ROOT_DIR}/client/main.py"
     --ip "${API_IP}" --port "${API_PORT}"
     send job
     --name "${JOB_NAME}"
     --queue "${QUEUE_ID}"
     --path "${BENCHMARK_SCRIPT}"
     --scheduler-type "${SCHEDULER_TYPE}")

if [[ -n "${JOB_OPTIONS}" ]]; then
  CMD+=(--options "${JOB_OPTIONS}")
fi

echo ${CMD}

USER="${SUBMIT_USER}" "${CMD[@]}"

echo "Submitted. Check status with:"
echo "  USER=${SUBMIT_USER} API_IP=${API_IP} API_PORT=${API_PORT} pipenv run python client/main.py get jobs"
