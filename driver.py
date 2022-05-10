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
INTERMEDIATE_PREFIX = "mr"
OUTPUT_PREFIX = "out"
IGNORE_CASE = False
SORT = False
WAIT_MILLISECONDS = 500


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def __init__(
        self, stop_event, n_map, n_reduce,
        input_path, intermediate_path, output_path,
        intermediate_prefix, output_prefix,
        ignore_case, sort, wait_milliseconds
    ):
        self.input_path = input_path
        self.intermediate_path = intermediate_path
        self.output_path = output_path
        self.stop_event = stop_event
        self.intermediate_prefix = intermediate_prefix
        self.output_prefix = output_prefix
        self.ignore_case = ignore_case
        self.sort = sort
        self.wait_milliseconds = wait_milliseconds
        self.ensure_proper_paths()
        self.map_tasks = self.prepare_map_tasks(n_map, n_reduce)
        self.reduce_tasks = self.prepare_reduce_tasks(n_reduce)
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
            logging.info(f"{task_type_to_string(task.type)} job {task.job_id} is assigned to worker {worker_id}")
        elif task.type == wordcount_mr_pb2.Task.TERMINATE:
            if worker_id in self.workers:
                self.workers.remove(worker_id)
                if not self.workers:
                    self.stop_event.set()
        elif task.type == wordcount_mr_pb2.Task.WAIT:
            logging.info("idle wait task!")
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
        task_wait = make_wait_task(WAIT_MILLISECONDS)
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


    def prepare_map_tasks(self, n_map, n_reduce):
        file_names = find_files_for_task(self.input_path, only_names=True)
        files_for_map = self.split_files_for_map(
            self.input_path, file_names, n_map
        )
        return [
            make_map_task(
                self.input_path, self.intermediate_path,
                job_id, input_file_names, n_reduce,
                self.intermediate_prefix, self.ignore_case, self.sort
            ) for job_id, input_file_names in enumerate(files_for_map)
        ]


    def prepare_reduce_tasks(self, n_reduce):
        return [
            make_reduce_task(
                job_id, self.intermediate_path, self.output_path,
                self.output_prefix, self.sort
            )
            for job_id in range(n_reduce)
        ]


    def ensure_proper_paths(self):
        for path in ("input_path", "intermediate_path", "output_path"):
            if not getattr(self, path).endswith("/"):
                setattr(self, path, getattr(self, path) + "/")


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


def serve(**kwargs):
    logging.info(f"serve {kwargs}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    stop_event = Event()
    kwargs["stop_event"] = stop_event
    wordcount_mr_pb2_grpc.add_WordCountMRServicer_to_server(
        WordCountMR(**kwargs), server
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
    parser.add_argument(
        "--input_path", type=str, nargs="?", const=INPUT_PATH,
        default=INPUT_PATH, help="path to dir with input files"
    )
    parser.add_argument(
        "--intermediate_path", type=str, nargs="?", const=INTERMEDIATE_PATH,
        default=INTERMEDIATE_PATH, help="path to dir for intermediate files"
    )
    parser.add_argument(
        "--output_path", type=str, nargs="?", const=OUTPUT_PATH,
        default=OUTPUT_PATH, help="path to dir for output files"
    )
    parser.add_argument(
        "--intermediate_prefix", type=str, nargs="?", const=INTERMEDIATE_PATH,
        default=INTERMEDIATE_PREFIX, help="prefix for intermediate files"
    )
    parser.add_argument(
        "--output_prefix", type=str, nargs="?", const=OUTPUT_PATH,
        default=OUTPUT_PREFIX, help="prefix for output files"
    )
    parser.add_argument(
        "--ignore_case", "-c", action="store_true",
        default=False, help="ignore case in all tasks"
    )
    parser.add_argument(
        "--sort", "-s", action="store_true",
        default=False, help="add sort between map and reduce"
    )
    parser.add_argument(
        "--wait", "-w", type=int, nargs="?",
        const=WAIT_MILLISECONDS, default=WAIT_MILLISECONDS,
        help="time for wait tasks in milliseconds"
    )
    args = parser.parse_args()
    logging.basicConfig(
        filename="./logs/driver.log", level=logging.DEBUG,
        format="%(asctime)s %(message)s", filemode="w"
    )
    serve(
        n_map=args.n_map, n_reduce=args.n_reduce, input_path=args.input_path,
        intermediate_path=args.intermediate_path, output_path=args.output_path,
        intermediate_prefix=args.intermediate_prefix,
        output_prefix=args.output_prefix, ignore_case=args.ignore_case,
        sort=args.sort, wait_milliseconds=args.wait
    )
