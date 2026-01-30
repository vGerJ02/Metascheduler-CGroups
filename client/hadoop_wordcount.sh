#!/usr/bin/env bash

python main.py send job \
    --name hadoop_wordcount_10s \
    --queue 1 \
    --path /home/gjaimejuan/hadoop-mapreduce-examples-2.7.3.jar \
    --scheduler-type H \
    --options "wordcount input.txt out_wordcount" \
		--hadoop-quiet

