#!/usr/bin/env bash
set -euo pipefail

# Submit core Hadoop benchmarks to metascheduler:
#   - TeraSort (main benchmark)
#   - TestDFSIO (I/O contention)
#   - WordCount (CPU fairness)
#
# Notes:
# - Paths/options are executed on the Hadoop master.
# - This script only submits jobs; it does not create HDFS inputs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

API_IP="${API_IP:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
QUEUE_ID="${QUEUE_ID:-1}"
SUBMIT_USER="${SUBMIT_USER:-${USER:-}}"
PYTHON_BIN="${PYTHON_BIN:-python}"
DELAY="${DELAY:-1}"
HADOOP_QUIET="${HADOOP_QUIET:-1}"

HADOOP_JAR="${HADOOP_JAR:-/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar}"

TERASORT_IN="${TERASORT_IN:-/user/${SUBMIT_USER}/terain}"
TERASORT_OUT="${TERASORT_OUT:-/user/${SUBMIT_USER}/teraout}"

TESTDFSIO_ARGS="${TESTDFSIO_ARGS:--write -nrFiles 8 -fileSize 256MB -resFile /tmp/TestDFSIO_results.txt}"

WORDCOUNT_IN="${WORDCOUNT_IN:-/user/${SUBMIT_USER}/wordcount_in}"
WORDCOUNT_OUT="${WORDCOUNT_OUT:-/user/${SUBMIT_USER}/wordcount_out}"

TS="$(date +%Y%m%d_%H%M%S)"
TERASORT_JOB_NAME="${TERASORT_JOB_NAME:-hadoop-terasort-${TS}}"
TESTDFSIO_JOB_NAME="${TESTDFSIO_JOB_NAME:-hadoop-testdfsio-${TS}}"
WORDCOUNT_JOB_NAME="${WORDCOUNT_JOB_NAME:-hadoop-wordcount-${TS}}"

if [[ -z "${SUBMIT_USER}" ]]; then
  echo "SUBMIT_USER/USER is not set." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/client/main.py" ]]; then
  echo "client/main.py not found under ${ROOT_DIR}" >&2
  exit 1
fi

run_submit() {
  local name="$1"
  local options="$2"

  cmd=(
    "${PYTHON_BIN}" "${ROOT_DIR}/client/main.py"
    --ip "${API_IP}" --port "${API_PORT}"
    send job
    --name "${name}"
    --queue "${QUEUE_ID}"
    --path "${HADOOP_JAR}"
    --scheduler-type H
    --options "${options}"
  )

  if [[ "${HADOOP_QUIET}" == "1" ]]; then
    cmd+=(--hadoop-quiet)
  fi

  PYTHONPATH="${ROOT_DIR}" USER="${SUBMIT_USER}" "${cmd[@]}"
}

echo "Submitting Hadoop benchmark suite to ${API_IP}:${API_PORT} (queue ${QUEUE_ID})"

run_submit "${TERASORT_JOB_NAME}" "terasort ${TERASORT_IN} ${TERASORT_OUT}"
sleep "${DELAY}"

run_submit "${TESTDFSIO_JOB_NAME}" "TestDFSIO ${TESTDFSIO_ARGS}"
sleep "${DELAY}"

run_submit "${WORDCOUNT_JOB_NAME}" "wordcount ${WORDCOUNT_IN} ${WORDCOUNT_OUT}"

echo "Submitted:"
echo "  - ${TERASORT_JOB_NAME}"
echo "  - ${TESTDFSIO_JOB_NAME}"
echo "  - ${WORDCOUNT_JOB_NAME}"
