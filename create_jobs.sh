#!/bin/bash

# Hardcoded input and output directories
input_directory="/home/ubuntu/parallel_process/images"
output_directory="/home/ubuntu/parallel_process/processed_images"
num_jobs=2  # Number of jobs

# Create submit files for each job
for ((i=0; i<$num_jobs; i++)); do
    submit_file="job_${i}.sub"
    start_index=$((i * 6250))
    end_index=$((start_index + 6249))

    echo "executable = /home/ubuntu/parallel_process/.venv/bin/python" > $submit_file
    echo "arguments = process_image_multiple.py $(printf "${input_directory}/cat%d.jpg " $(seq $start_index 1 $end_index)) $(printf "${input_directory}/dog%d.jpg " $(seq $start_index 1 $end_index)) $output_directory" >> $submit_file
    echo "output = job_${i}.out" >> $submit_file
    echo "error = job_${i}.err" >> $submit_file
    echo "log = job_${i}.log" >> $submit_file
    echo "request_cpus = 1" >> $submit_file
    echo "queue" >> $submit_file

    echo "Created ${submit_file}"
done

