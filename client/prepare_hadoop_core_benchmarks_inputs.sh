#!/usr/bin/env bash
set -euo pipefail

# Prepare HDFS inputs for core Hadoop benchmarks:
#   - TeraSort: generates TERAGEN_RECORDS into TERASORT_IN
#   - WordCount: uploads a generated text file into WORDCOUNT_IN
# Optional cleanup:
#   - TERASORT_OUT, WORDCOUNT_OUT, TESTDFSIO_BASE

JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk}"
HADOOP_HOME="${HADOOP_HOME:-/usr/hdp/2.6.5.0-292/hadoop}"
HADOOP_JAR="${HADOOP_JAR:-/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar}"

SUBMIT_USER="${SUBMIT_USER:-${USER:-}}"
if [[ -z "${SUBMIT_USER}" ]]; then
  echo "SUBMIT_USER/USER is not set." >&2
  exit 1
fi

TERAGEN_RECORDS="${TERAGEN_RECORDS:-1000000}"
TERASORT_IN="${TERASORT_IN:-/user/${SUBMIT_USER}/terain}"
TERASORT_OUT="${TERASORT_OUT:-/user/${SUBMIT_USER}/teraout}"

WORDCOUNT_IN="${WORDCOUNT_IN:-/user/${SUBMIT_USER}/wordcount_in}"
WORDCOUNT_OUT="${WORDCOUNT_OUT:-/user/${SUBMIT_USER}/wordcount_out}"
WORDCOUNT_LOCAL_FILE="${WORDCOUNT_LOCAL_FILE:-/tmp/wordcount_input_${SUBMIT_USER}.txt}"
WORDCOUNT_LINES="${WORDCOUNT_LINES:-200000}"

TESTDFSIO_BASE="${TESTDFSIO_BASE:-/benchmarks/TestDFSIO}"

CLEAN_OUTPUTS="${CLEAN_OUTPUTS:-1}"
RECREATE_TERAIN="${RECREATE_TERAIN:-1}"
RECREATE_WORDCOUNT_IN="${RECREATE_WORDCOUNT_IN:-1}"

if [[ ! -x "${HADOOP_HOME}/bin/hdfs" ]]; then
  echo "hdfs CLI not found at ${HADOOP_HOME}/bin/hdfs" >&2
  exit 1
fi

if [[ ! -x "${HADOOP_HOME}/bin/yarn" ]]; then
  echo "yarn CLI not found at ${HADOOP_HOME}/bin/yarn" >&2
  exit 1
fi

if [[ ! -f "${HADOOP_JAR}" ]]; then
  echo "HADOOP_JAR not found: ${HADOOP_JAR}" >&2
  exit 1
fi

export JAVA_HOME

hdfs_cmd() {
  "${HADOOP_HOME}/bin/hdfs" dfs "$@"
}

echo "Preparing HDFS benchmark inputs..."
echo "  User: ${SUBMIT_USER}"
echo "  TeraSort in/out: ${TERASORT_IN} / ${TERASORT_OUT}"
echo "  WordCount in/out: ${WORDCOUNT_IN} / ${WORDCOUNT_OUT}"
echo "  TestDFSIO base: ${TESTDFSIO_BASE}"

# Ensure user home exists in HDFS.
hdfs_cmd -mkdir -p "/user/${SUBMIT_USER}"

if [[ "${CLEAN_OUTPUTS}" == "1" ]]; then
  echo "Cleaning previous benchmark outputs..."
  hdfs_cmd -rm -r -f "${TERASORT_OUT}" >/dev/null 2>&1 || true
  hdfs_cmd -rm -r -f "${WORDCOUNT_OUT}" >/dev/null 2>&1 || true
  hdfs_cmd -rm -r -f "${TESTDFSIO_BASE}" >/dev/null 2>&1 || true
fi

if [[ "${RECREATE_TERAIN}" == "1" ]]; then
  echo "Recreating TeraSort input with teragen (${TERAGEN_RECORDS} records)..."
  hdfs_cmd -rm -r -f "${TERASORT_IN}" >/dev/null 2>&1 || true
  "${HADOOP_HOME}/bin/yarn" jar "${HADOOP_JAR}" teragen "${TERAGEN_RECORDS}" "${TERASORT_IN}"
fi

if [[ "${RECREATE_WORDCOUNT_IN}" == "1" ]]; then
  echo "Generating local WordCount input file: ${WORDCOUNT_LOCAL_FILE}"
  : > "${WORDCOUNT_LOCAL_FILE}"
  for ((i=1; i<=WORDCOUNT_LINES; i++)); do
    printf "metascheduler benchmark line %d for wordcount fairness test\n" "${i}" >> "${WORDCOUNT_LOCAL_FILE}"
  done

  echo "Uploading WordCount input to HDFS: ${WORDCOUNT_IN}"
  hdfs_cmd -rm -r -f "${WORDCOUNT_IN}" >/dev/null 2>&1 || true
  hdfs_cmd -mkdir -p "${WORDCOUNT_IN}"
  hdfs_cmd -put -f "${WORDCOUNT_LOCAL_FILE}" "${WORDCOUNT_IN}/input.txt"
fi

echo "Preparation complete."
