#!/usr/bin/env bash

  python3 main.py send job \
    --name hadoop_wordcount_10s \
    --queue 1 \
    --path /home/gjaimejuan/hadoop-mapreduce-examples-2.7.3.jar \
    --scheduler-type H \
    --options "wordcount hadoop_input_200mb.txt wordcount_10s_output"

