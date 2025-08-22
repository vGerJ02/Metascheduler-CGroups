#!/bin/bash

# This is the test script used to verify the functionality of the system.
# The script will schedule SGE jobs of different sizes to the system.
# It uses pipenv to ensure the correct Python environment.
# Script per enviar jobs a Hadoop des del client del metascheduler amb pipenv.
# Cada job fa un "wordcount" amb diferents fitxers d'entrada.

cd ../client

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "sge_prova_1" --path "test_job100000000.sh" --scheduler-type S --queue 1'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "sge_prova_2" --path "test_job1000000000.sh" --scheduler-type S --queue 1'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "sge_prova_3" --path "test_job10000000000.sh" --scheduler-type S --queue 1'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "sge_prova_4" --path "test_job100000000000.sh" --scheduler-type S --queue 1'
sleep 3


pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "hadoop_job_1" --path "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar" --queue 1 --scheduler-type H --options "wordcount 1000000.txt out1"'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "hadoop_job_2" --path "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar" --queue 1 --scheduler-type H --options "wordcount 5000000.txt out2"'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "hadoop_job_3" --path "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar" --queue 1 --scheduler-type H --options "wordcount 10000000.txt out3"'
sleep 3

pipenv run sh -c 'export USER=metascheduler && export PYTHONPATH=.. && python3 main.py send job --name "hadoop_job_4" --path "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar" --queue 1 --scheduler-type H --options "wordcount 15000000.txt out4"'


