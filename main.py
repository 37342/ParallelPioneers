import os
import sys
import shutil
import time
from subprocess import run


def make_clear_directory(directory_path):
    """
    Create the specified directory if it does not exist, or clear its contents if it exists.

    Parameters:
    - directory_path: The path of the directory to be created or cleared.
    """
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created.")
        else:
            shutil.rmtree(directory_path)
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' cleared successfully.")
    except Exception as e:
        print(f"Error clearing/creating directory '{directory_path}': {e}")


def get_file_count(directory_path, extension=".jpg"):
    """
    Get the number of files with a specific extension in the specified directory.

    Parameters:
    - directory_path: The path of the directory to be checked.
    - extension: The file extension to count (default is ".jpg").

    Returns:
    - count: The number of files with the specified extension.
    """
    try:
        files = [file for file in os.listdir(directory_path) if file.endswith(extension)]
        count = len(files)
        return count
    except Exception as e:
        print(f"Error counting files: {e}")
        return None


def create_submit_file(job_index, image_input_directory, image_output_directory, images_per_job, remainder):
    """
    Create a HTCondor submit file for the image processing job.

    Parameters:
    - job_index: The index of the job.
    - image_input_directory: The path of the input images' directory.
    - image_output_directory: The path of the processed images' directory.
    - images_per_job: The number of images each job processes.
    - remainder: The remainder images for distribution among jobs.
    """
    submit_file = f"/home/ubuntu/parallel_process/jobs/job_{job_index}.sub"
    start_index = job_index * images_per_job + min(job_index, remainder)
    end_index = start_index + images_per_job - 1 + (1 if job_index < remainder else 0)

    with open(submit_file, "w") as f:
        f.write(f"executable = /home/ubuntu/parallel_process/.venv/bin/python\n")
        f.write(
            f"arguments = /home/ubuntu/parallel_process/scripts/process_image_multiple.py {' '.join([f'{image_input_directory}/cat{i}.jpg' for i in range(start_index, end_index + 1)])} {' '.join([f'{image_input_directory}/dog{i}.jpg' for i in range(start_index, end_index + 1)])} {image_output_directory}\n")
        f.write(f"output = /home/ubuntu/parallel_process/condor_output/job_{job_index}.out\n")
        f.write(f"error = /home/ubuntu/parallel_process/condor_output/job_{job_index}.err\n")
        f.write(f"log = /home/ubuntu/parallel_process/condor_output/job_{job_index}.log\n")
        f.write("request_cpus = 1\n")
        f.write("queue\n")

    print(f"Created {submit_file}")


def wait_until_completion(image_output_directory, target_count, check_interval=0.1):
    """
    Wait until the image processing job is complete.

    Parameters:
    - image_output_directory: The path of the processed images' directory.
    - target_count: The target number of images to wait for.
    - check_interval: Time interval (in seconds) between checks (default is 0.1 seconds).
    """
    while True:
        current_count = len(os.listdir(image_output_directory))

        if current_count >= target_count:
            print(f"{target_count} reached.")
            break

        time.sleep(check_interval)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py num_jobs")
        sys.exit(1)

    num_jobs = int(sys.argv[1])
    image_input_directory = "/home/ubuntu/parallel_process/images"
    image_output_directory = "/home/ubuntu/parallel_process/processed_images"

    total_images = get_file_count(image_input_directory)
    images_per_job = total_images // num_jobs
    remainder = total_images % num_jobs

    make_clear_directory("/home/ubuntu/parallel_process/jobs")
    make_clear_directory("/home/ubuntu/parallel_process/condor_output")
    make_clear_directory(image_output_directory)

    start_time = time.time()

    for i in range(num_jobs):
        create_submit_file(i, image_input_directory, image_output_directory, images_per_job, remainder)

    for i in range(num_jobs):
        submit_file = f"/home/ubuntu/parallel_process/jobs/job_{i}.sub"
        run(["condor_submit", submit_file])
        print(f"Job submission {i + 1}/{num_jobs} completed.")

    wait_until_completion(image_output_directory, total_images)

    end_time = time.time()
    time_taken = end_time - start_time

    print(f"Time taken to process {total_images} across {num_jobs} jobs is {time_taken:.2f} seconds")

    log_file_path = "/home/ubuntu/parallel_process/main.log"
    with open(log_file_path, "a") as log_file:
        log_file.write(f"Time taken to process {total_images} images across {num_jobs} jobs is {time_taken:.2f} seconds\n")
