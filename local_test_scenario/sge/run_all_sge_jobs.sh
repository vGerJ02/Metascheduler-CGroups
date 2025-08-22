#!/bin/bash
#$ -N all_jobs_PROVA       # nom del job
#$ -q all.q                # forçar la cua "all.q"
#$ -o all_jobs_PROVA.o     # fitxer de sortida
#$ -e all_jobs_PROVA.e     # fitxer d'errors

total_time=0

for job in test_job100000000.sh test_job1000000000.sh test_job10000000000.sh test_job100000000000.sh
do
    echo "Executant $job ..."
    start=$(date +%s)
    /home/metascheduler/$job
    end=$(date +%s)
    elapsed=$((end - start))
    echo "$job acabat en $elapsed segons"
    total_time=$((total_time + elapsed))
done

echo "Temps total d'execució: $total_time segons"
