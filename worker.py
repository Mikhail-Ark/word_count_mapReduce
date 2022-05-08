import argparse
from concurrent import futures
import logging

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

from tasks.map import word_count_map
from tasks.reduce import word_count_reduce


MAP_DEFAULT_IGNORE_CASE = False
MAP_DEFAULT_N_BUCKETS = 1
MAP_DEFAULT_SORT = False
REDUCE_DEFAULT_MERGE_JOIN = MAP_DEFAULT_SORT
MAP_DEFAULT_OUTPUT_NAME = "mr"
REDUCE_DEFAULT_OUTPUT_NAME = "out"


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def DoTask(self, request, context):
        logging.info("do task")
        task_type, params = self.unpack_params(request)
        if task_type == wordcount_mr_pb2.Task.MAP:
            logging.info(f"task map: {str(params)}")
            word_count_map(**params)
        elif task_type == wordcount_mr_pb2.Task.REDUCE:
            logging.info(f"task reduce: {str(params)}")
            word_count_reduce(**params)
        logging.info("finished")
        return wordcount_mr_pb2.Status(success=True)


    def unpack_params(self, request):
        task_type = request.type
        params = dict()
        params["input_path"] = request.input_path
        params["output_path"] = request.output_path
        params["job_id"] = request.job_id
        if request.input_file_names:
            params["input_file_names"] = request.input_file_names
        if request.HasField("output_file_name"):
            params["output_file_name"] = request.output_file_name
        if task_type == wordcount_mr_pb2.Task.MAP:
            if request.HasField("ignore_case"):
                params["ignore_case"] = request.ignore_case
            else:
                params["ignore_case"] = MAP_DEFAULT_IGNORE_CASE
            if request.HasField("sort"):
                params["sort"] = request.sort
            else:
                params["sort"] = MAP_DEFAULT_SORT
            if request.HasField("n_buckets"):
                params["n_buckets"] = request.n_buckets
            else:
                params["n_buckets"] = MAP_DEFAULT_N_BUCKETS
            if "output_file_name" not in params:
                params["output_file_name"] = MAP_DEFAULT_OUTPUT_NAME
        elif task_type == wordcount_mr_pb2.Task.REDUCE:
            if request.HasField("n_buckets"):
                params["merge_join"] = request.merge_join
            else:
                params["merge_join"] = REDUCE_DEFAULT_MERGE_JOIN
            if "output_file_name" not in params:
                params["output_file_name"] = REDUCE_DEFAULT_OUTPUT_NAME
        else:
            raise AssertionError("Unknown task type")
        return task_type, params


def serve():
    logging.info("run")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    wordcount_mr_pb2_grpc.add_WordCountMRServicer_to_server(
        WordCountMR(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--worker_id", "-i", type=int, nargs="?", const=1, default=1,
        help="worker id"
    )
    args = parser.parse_args()
    logging.basicConfig(
        filename=f"./logs/worker_{args.worker_id}.log", level=logging.DEBUG,
        format="%(asctime)s %(message)s", filemode="w"
    )
    serve()
