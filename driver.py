import argparse
from concurrent import futures
import grpc
from itertools import chain
import logging
from math import ceil
from threading import Event

import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

from tasks.io import find_files_for_task

INPUT_PATH = "./files/inputs/"
INTERMEDIATE_PATH = "./files/intermediate/"
OUTPUT_PATH = "./files/out/"


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def __init__(self, stop_event, n_map, n_reduce):
        self.stop_event = stop_event
        self.map_tasks = self.prepare_map_tasks(n_map, n_reduce)
        self.reduce_tasks = self.prepare_reduce_tasks(n_reduce)
        self.terminate_task = self.make_terminate_task()
        self.task_generator = self.make_task_generatior()
        self.current_task_type = wordcount_mr_pb2.Task.MAP


    def GetTask(self, request, context):
        logging.info("request received")
        logging.info(WordCountMR.request_to_string(request))
        try:
            task = next(self.task_generator)
        except StopIteration:
            raise AssertionError("no terminate task")
        if task.type != self.current_task_type:
            logging.info(
                f"""{
                    WordCountMR.task_type_to_string(self.current_task_type)
                } tasks are done"""
            )
            self.current_task_type = task.type
            if task.type == wordcount_mr_pb2.Task.TERMINATE:
                self.stop_event.set()
        logging.info(
            f"send {WordCountMR.task_type_to_string(task.type)} task"
        )
        return task


    def make_task_generatior(self):
        for task in chain(
            self.map_tasks, self.reduce_tasks, [self.terminate_task]
        ):
            yield task


    @staticmethod
    def request_to_string(request):
        request_list = [
                f"status: {str(request.status)}",
                f"worker_id: {str(request.worker_id)}",
            ]
        if request.output_file_names:
            request_list.append(
                f"output_file_names: {' '.join(request.output_file_names)}"
            )
        if request.warnings:
            request_list.append(
                f"warnings: {' '.join(request.warnings)}"
            )
        return " | ".join(request_list)


    @staticmethod
    def prepare_map_tasks(n_map, n_reduce):
        file_names = find_files_for_task(INPUT_PATH, only_names=True)
        files_for_map = WordCountMR.split_files_for_map(file_names, n_map)
        return [
            WordCountMR.make_map_task(
                job_id, input_file_names, n_buckets=n_reduce
            ) for job_id, input_file_names in enumerate(files_for_map)
        ]


    @staticmethod
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


    @staticmethod
    def prepare_reduce_tasks(n_reduce):
        return [WordCountMR.make_reduce_task(job_id) for job_id in range(n_reduce)]


    @staticmethod
    def make_map_task(job_id, input_file_names, n_buckets):
        return wordcount_mr_pb2.Task(
            type=wordcount_mr_pb2.Task.MAP,
            input_path=INPUT_PATH,
            output_path=INTERMEDIATE_PATH,
            input_file_names=input_file_names,
            job_id=job_id,
            n_buckets=n_buckets
        )


    @staticmethod
    def make_reduce_task(job_id):
        return wordcount_mr_pb2.Task(
            type=wordcount_mr_pb2.Task.REDUCE,
            input_path=INTERMEDIATE_PATH,
            output_path=OUTPUT_PATH,
            job_id=job_id
        )


    @staticmethod
    def make_terminate_task():
        return wordcount_mr_pb2.Task(
            type=wordcount_mr_pb2.Task.TERMINATE,
        )


    @staticmethod
    def make_wait_task(milliseconds):
        return wordcount_mr_pb2.Task(
            type=wordcount_mr_pb2.Task.WAIT, wait_milliseconds=milliseconds
        )


    @staticmethod
    def task_type_to_string(task_type):
        return ('map', 'reduce', 'wait', 'terminate')[task_type]


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
