#!/usr/bin/env bash

# hdfs dfs -rm -r /user/gjaimejuan/teraout

# hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar teragen 10000000 /user/gjaimejuan/terain


python main.py send job \
    --name hadoop_terasort \
    --queue 1 \
    --path /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \
    --scheduler-type H \
    --options "terasort /user/gjaimejuan/terain /user/gjaimejuan/teraout" \
		--hadoop-quiet

