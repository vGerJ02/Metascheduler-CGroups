#!/usr/bin/env bash

python main.py send job \
	--name hadoop_pi \
	--queue 1 \
	--path /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \
	--scheduler-type H \
	--options "pi 2 4" \
	--hadoop-quiet

