import argparse
import grpc
import logging
from math import ceil

import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

from tasks.io import find_files_for_task

INPUT_PATH = "./files/inputs/"
INTERMEDIATE_PATH = "./files/intermediate/"
OUTPUT_PATH = "./files/out/"


def run(n_map, n_reduce):
    logging.info(f"run with n_map={n_map}, n_reduce={n_reduce}")
    map_tasks = prepare_map_tasks(n_map, n_reduce)
    reduce_tasks = prepare_reduce_tasks(n_reduce)
    terminate_task = make_terminate_task()

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = wordcount_mr_pb2_grpc.WordCountMRStub(channel)
        for task_type, tasks in (
            ("map", map_tasks), ("reduce", reduce_tasks),
            ("terminate", [terminate_task])
        ):
            for task in tasks:
                logging.info(f"send {task_type} task")
                response = stub.DoTask(task)
                logging.info(f"success: {str(response.success)}")
            logging.info(f"{task_type} tasks are done")


def prepare_map_tasks(n_map, n_reduce):
    file_names = find_files_for_task(INPUT_PATH, only_names=True)
    files_for_map = split_files_for_map(file_names, n_map)
    return [
        make_map_task(job_id, input_file_names, n_buckets=n_reduce) \
        for job_id, input_file_names in enumerate(files_for_map)
    ]


def split_files_for_map(file_names, n_map):
    if len(file_names) <= n_map:
        return [[file_name] for file_name in file_names]
    else:
        files_for_map = list()
        batch_size = ceil(len(file_names) / n_map)
        i = 0
        batch = file_names[i:i+batch_size]
        while batch:
            files_for_map.append(batch)
            i += batch_size
            batch = file_names[i:i+batch_size]
            if len(file_names[i:]) == n_map - len(files_for_map):
                for file_name in file_names[i:]:
                    files_for_map.append([file_name])
                break
        return files_for_map


def prepare_reduce_tasks(n_reduce):
    return [make_reduce_task(job_id) for job_id in range(n_reduce)]


def make_map_task(job_id, input_file_names, n_buckets):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.MAP,
        input_path=INPUT_PATH,
        output_path=INTERMEDIATE_PATH,
        input_file_names=input_file_names,
        job_id=job_id,
        n_buckets=n_buckets
    )


def make_reduce_task(job_id):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.REDUCE,
        input_path=INTERMEDIATE_PATH,
        output_path=OUTPUT_PATH,
        job_id=job_id
    )


def make_terminate_task():
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.TERMINATE,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--n_map", "-N", type=int, nargs="?", const=1, default=1,
        help="number of map tasks"
    )
    parser.add_argument(
        "--n_reduce", "-M", type=int, nargs="?", const=1, default=1,
        help="number of reduce tasks"
    )
    args = parser.parse_args()
    logging.basicConfig(
        filename="./logs/driver.log", level=logging.DEBUG,
        format="%(asctime)s %(message)s", filemode="w"
    )
    run(n_map=args.n_map, n_reduce=args.n_reduce)
