#!/bin/bash
# Hadoop Benchmark Suite
# Runs common benchmarks (TeraSort, TestDFSIO, NNBench, MRBench)

set -e

# ----------------------------
# Configuration
# ----------------------------
HADOOP_CMD="hadoop"
HDFS_INPUT_DIR="/benchmarks/input"
HDFS_OUTPUT_DIR="/benchmarks/output"
HDFS_BASE_DIR="/benchmarks"
LOCAL_LOG_DIR="./benchmark_logs"

# TeraSort 
TERAGEN_RECORDS=1000000   # 1 million records (~100 MB)

# TestDFSIO
DFSIO_FILES=5
DFSIO_FILE_SIZE=100MB

# MRBench
MRBENCH_RUNS=5

# NNBench 
NNBENCH_FILES=100
NNBENCH_BYTES=1048576  # 1 MB

# Ensure log directory exists
mkdir -p "$LOCAL_LOG_DIR"

# ----------------------------
# Helper functions
# ----------------------------

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

run_and_log() {
  local name=$1
  shift
  local logfile="${LOCAL_LOG_DIR}/${name}.log"
  log "Running ${name}..."
  "$@" 2>&1 | tee "$logfile"
  log "${name} completed. Log saved to ${logfile}"
}

# ----------------------------
# Benchmark 1: TeraSort Suite
# ----------------------------

# Generate data
run_and_log "teragen" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teragen \
  $TERAGEN_RECORDS ${HDFS_BASE_DIR}/terasort-input

# Sort data
run_and_log "terasort" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort \
  ${HDFS_BASE_DIR}/terasort-input ${HDFS_BASE_DIR}/terasort-output

# Validate data
run_and_log "teravalidate" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teravalidate \
  ${HDFS_BASE_DIR}/terasort-output ${HDFS_BASE_DIR}/terasort-validate

# ----------------------------
# Benchmark 2: TestDFSIO
# ----------------------------

run_and_log "testdfsio_write" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO \
  -write -nrFiles $DFSIO_FILES -fileSize $DFSIO_FILE_SIZE -resFile ${HDFS_BASE_DIR}/TestDFSIO_write.log

run_and_log "testdfsio_read" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO \
  -read -nrFiles $DFSIO_FILES -fileSize $DFSIO_FILE_SIZE -resFile ${HDFS_BASE_DIR}/TestDFSIO_read.log

# ----------------------------
# Benchmark 3: NNBench
# ----------------------------

run_and_log "nnbench_create" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar nnbench \
  -operation create_write -maps 2 -reduces 1 \
  -bytesToWrite $NNBENCH_BYTES -numberOfFiles $NNBENCH_FILES \
  -baseDir ${HDFS_BASE_DIR}/nnbench

run_and_log "nnbench_read" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar nnbench \
  -operation open_read -maps 2 -reduces 1 \
  -baseDir ${HDFS_BASE_DIR}/nnbench

# ----------------------------
# Benchmark 4: MRBench
# ----------------------------

run_and_log "mrbench" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar mrbench \
  -numRuns $MRBENCH_RUNS -maps 2 -reduces 1

# ----------------------------
# Benchmark 5: Sort
# ----------------------------

# Make sure input data exists
$HADOOP_CMD fs -mkdir -p $HDFS_INPUT_DIR
$HADOOP_CMD fs -put -f /etc/hosts $HDFS_INPUT_DIR/hosts.txt || true

run_and_log "sort" \
  $HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar sort \
  $HDFS_INPUT_DIR ${HDFS_OUTPUT_DIR}/sort-output

# ----------------------------
# Summary
# ----------------------------

log "All benchmarks completed."
log "Logs saved to ${LOCAL_LOG_DIR}"

