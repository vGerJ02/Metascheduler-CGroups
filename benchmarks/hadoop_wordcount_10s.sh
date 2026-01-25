#!/usr/bin/env bash
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INPUT_LOCAL="${BENCH_DIR}/hadoop_input_200mb.txt"
HDFS_BASE_DIR="/benchmarks/wordcount_10s"
HDFS_INPUT_DIR="${HDFS_BASE_DIR}/input"
HDFS_OUTPUT_DIR="${HDFS_BASE_DIR}/output"

mkdir -p "${BENCH_DIR}"

if [ ! -f "${INPUT_LOCAL}" ]; then
  echo "Generating ~200MB input file at ${INPUT_LOCAL}..."
  yes "metascheduler hadoop benchmark line" | head -c 200000000 > "${INPUT_LOCAL}"
fi

export JAVA_HOME="${JAVA_HOME}"

"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "${HDFS_BASE_DIR}"
"${HADOOP_HOME}/bin/hdfs" dfs -put -f "${INPUT_LOCAL}" "${HDFS_INPUT_DIR}"
"${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f "${HDFS_OUTPUT_DIR}"

"${HADOOP_HOME}/bin/yarn" jar \
  "${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-"*.jar \
  wordcount \
  "${HDFS_INPUT_DIR}" \
  "${HDFS_OUTPUT_DIR}"

echo "Done."
