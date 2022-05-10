import argparse
from concurrent import futures
import grpc
from itertools import cycle
import logging
from math import ceil
from os import path
from threading import Event

from proto_tools import make_map_task, make_reduce_task, make_wait_task, \
        make_terminate_task, request_to_string, task_type_to_string
from tasks.io import find_files_for_task
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

INPUT_PATH = "./files/inputs/"
INTERMEDIATE_PATH = "./files/intermediate/"
OUTPUT_PATH = "./files/out/"


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def __init__(
        self, stop_event, n_map, n_reduce,
        input_path=INPUT_PATH, intermediate_path=INTERMEDIATE_PATH,
        output_path=OUTPUT_PATH
    ):
        self.stop_event = stop_event
        self.map_tasks = WordCountMR.prepare_map_tasks(
            n_map, n_reduce, input_path, intermediate_path
        )
        self.reduce_tasks = WordCountMR.prepare_reduce_tasks(
            n_reduce, intermediate_path, output_path
        )
        self.workers = set()
        self.assignments = dict()
        self.complete_job_ids = set()
        self.task_generator = self.make_task_generator()


    def GetTask(self, request, _):
        logging.info("request received")
        logging.info(request_to_string(request))
        worker_id = request.worker_id
        self.workers.add(worker_id)
        if request.worker_id in self.assignments:
            if request.status == wordcount_mr_pb2.TaskRequest.SUCCESS:
                self.complete_job_ids.add(self.assignments[worker_id])
                logging.info(f"worker {worker_id} completed job {self.assignments[worker_id]}")
            else:
                logging.info(f"worker {worker_id} failed job {self.assignments[worker_id]}")
            del self.assignments[worker_id]
        task = next(self.task_generator)
        if task.type in {
            wordcount_mr_pb2.Task.MAP, wordcount_mr_pb2.Task.REDUCE
        }:
            self.assignments[worker_id] = task.job_id
            logging.info(f"{task_type_to_string(task.type)} job {task.job_id} assigned to worker {worker_id}")
        elif task.type == wordcount_mr_pb2.Task.TERMINATE:
            if worker_id in self.workers:
                self.workers.remove(worker_id)
                if not self.workers:
                    print("set")
                    self.stop_event.set()
        elif task.type == wordcount_mr_pb2.Task.WAIT:
            logging.info("idle wait!")
        return task


    def make_task_generator(self):
        yield from self.make_task_generator_stage(self.map_tasks)
        logging.info("map tasks are done")
        yield from self.make_task_generator_stage(self.reduce_tasks)
        logging.info("reduce tasks are done")
        terminate_task = make_terminate_task()
        while True:
            yield terminate_task
        

    def make_task_generator_stage(self, tasks):
        self.complete_job_ids.clear()
        self.assignments.clear()
        tasks_cycled = cycle(tasks)
        task_wait = make_wait_task()
        while True:
            if len(self.complete_job_ids) == len(tasks):
                break
            elif (
                len(self.complete_job_ids) + len(self.assignments)
            ) == len(tasks):
                yield task_wait
            else:
                jobs_to_do = set(range(len(tasks))) - self.complete_job_ids \
                        - set(self.assignments.values())
                while True:
                    task = next(tasks_cycled)
                    if task.job_id in jobs_to_do:
                        yield task
                        break


    @staticmethod
    def prepare_map_tasks(n_map, n_reduce, input_path, output_path):
        file_names = find_files_for_task(input_path, only_names=True)
        files_for_map = WordCountMR.split_files_for_map(
            input_path, file_names, n_map
        )
        return [
            make_map_task(
                input_path, output_path, job_id, input_file_names, n_reduce
            ) for job_id, input_file_names in enumerate(files_for_map)
        ]


    @staticmethod
    def prepare_reduce_tasks(n_reduce, input_path, output_path):
        return [
            make_reduce_task(job_id, input_path, output_path)
            for job_id in range(n_reduce)
        ]


    @staticmethod
    def split_files_for_map(input_path, file_names, n_map):
        file_names.sort(
            key=lambda file_name: path.getsize(input_path + file_name),
            reverse=True
        )
        if len(file_names) == n_map:
            return [[file_name] for file_name in file_names]
        elif len(file_names) < n_map:
            # TODO:
            # file_names = divide_files(input_path, file_names, n_map)
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


def serve(n_map, n_reduce):
    logging.info(f"serve n_map={n_map}, n_reduce={n_reduce}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    stop_event = Event()
    wordcount_mr_pb2_grpc.add_WordCountMRServicer_to_server(
        WordCountMR(stop_event, n_map, n_reduce), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    stop_event.wait()
    logging.info("self-terminate")
    server.stop(grace=True)
    server.wait_for_termination()


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
    serve(n_map=args.n_map, n_reduce=args.n_reduce)
