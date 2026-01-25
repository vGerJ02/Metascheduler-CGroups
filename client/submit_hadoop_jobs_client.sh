#!/usr/bin/env bash
set -euo pipefail

# Submit Hadoop wordcount jobs via the client CLI with minimal local output.
# Input paths are resolved on the master under /home/$SSH_USER, so use relative
# paths (or set INPUT_PREFIX) that exist there.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

API_IP="${API_IP:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
QUEUE_ID="${QUEUE_ID:-1}"
SUBMIT_USER="${SUBMIT_USER:-${USER:-}}"

HADOOP_JAR="${HADOOP_JAR:-/usr/hdp/2.6.5.0-292/hadoop-mapreduce/hadoop-mapreduce-examples-2.7.3.2.6.5.0-292.jar}"
HADOOP_CLASS="${HADOOP_CLASS:-wordcount}"
INPUT_FILES="${INPUT_FILES:-1000000.txt,5000000.txt,10000000.txt,15000000.txt}"
INPUT_PREFIX="${INPUT_PREFIX:-}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-hadoop-bench-out}"
NAME_PREFIX="${NAME_PREFIX:-hadoop-bench}"
DELAY="${DELAY:-1}"
QUIET="${QUIET:-1}"
HADOOP_QUIET="${HADOOP_QUIET:-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ -z "${SUBMIT_USER}" ]]; then
  echo "SUBMIT_USER/USER is not set." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/client/main.py" ]]; then
  echo "client/main.py not found under ${ROOT_DIR}" >&2
  exit 1
fi

if [[ ! -f "${HADOOP_JAR}" ]]; then
  echo "HADOOP_JAR not found: ${HADOOP_JAR}" >&2
  echo "Set HADOOP_JAR to the Hadoop mapreduce examples jar for your cluster." >&2
  exit 1
fi

IFS=',' read -r -a input_list <<< "${INPUT_FILES}"
if [[ "${#input_list[@]}" -eq 0 ]]; then
  echo "No INPUT_FILES provided." >&2
  exit 1
fi

echo "Submitting Hadoop benchmarks to ${API_IP}:${API_PORT} (queue ${QUEUE_ID})"

index=0
for raw_input in "${input_list[@]}"; do
  input="${raw_input#"${raw_input%%[![:space:]]*}"}"
  input="${input%"${input##*[![:space:]]}"}"
  if [[ -z "${input}" ]]; then
    continue
  fi
  index=$((index + 1))
  if [[ -n "${INPUT_PREFIX}" ]]; then
    input_path="${INPUT_PREFIX}${input}"
  else
    input_path="${input}"
  fi
  job_name="${NAME_PREFIX}-${index}"
  output_dir="${OUTPUT_PREFIX}-${index}"

  cmd=(
    "${PYTHON_BIN}" "${ROOT_DIR}/client/main.py"
    --ip "${API_IP}" --port "${API_PORT}"
    send job
    --name "${job_name}"
    --queue "${QUEUE_ID}"
    --path "${HADOOP_JAR}"
    --scheduler-type H
    --options "${HADOOP_CLASS} ${input_path} ${output_dir}"
  )
  if [[ "${HADOOP_QUIET}" == "1" ]]; then
    cmd+=(--hadoop-quiet)
  fi

  if [[ "${QUIET}" == "1" ]]; then
    PYTHONPATH="${ROOT_DIR}" USER="${SUBMIT_USER}" "${cmd[@]}" >/dev/null
  else
    PYTHONPATH="${ROOT_DIR}" USER="${SUBMIT_USER}" "${cmd[@]}"
  fi
  echo "Submitted ${job_name} (${input_path} -> ${output_dir})"

  if [[ "${DELAY}" != "0" ]]; then
    sleep "${DELAY}"
  fi
done

echo "Done."
