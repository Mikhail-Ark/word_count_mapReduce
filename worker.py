import argparse
import logging
from time import sleep

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

from driver import WordCountMR
from tasks.map import word_count_map
from tasks.reduce import word_count_reduce


MAP_DEFAULT_IGNORE_CASE = False
MAP_DEFAULT_N_BUCKETS = 1
MAP_DEFAULT_SORT = False
REDUCE_DEFAULT_MERGE_JOIN = MAP_DEFAULT_SORT
MAP_DEFAULT_OUTPUT_NAME = "mr"
REDUCE_DEFAULT_OUTPUT_NAME = "out"
WAIT_DEFAULT_MILLISECONDS = 500


def run(worker_id):
    logging.info(f"run worker_id {worker_id}")
    task_request = make_init_task_request(worker_id)
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = wordcount_mr_pb2_grpc.WordCountMRStub(channel)
        terminate = False
        while not terminate:
            try:
                task = stub.GetTask(task_request)
                logging.info("task received")
            except grpc._channel._InactiveRpcError:
                logging.info("no server, wait")
                task = WordCountMR.make_wait_task(WAIT_DEFAULT_MILLISECONDS)
            terminate = do_task(task, task_request)
    logging.info("terminated")


def make_init_task_request(worker_id):
    return wordcount_mr_pb2.TaskRequest(
        status=wordcount_mr_pb2.TaskRequest.INIT, worker_id=worker_id
    )


def do_task(task, task_request):
    task_type, params = unpack_params(task)
    if task_type == wordcount_mr_pb2.Task.MAP:
        logging.info(f"map: {str(params)}")
        word_count_map(**params)
        task_request.status = wordcount_mr_pb2.TaskRequest.SUCCESS
    elif task_type == wordcount_mr_pb2.Task.REDUCE:
        logging.info(f"reduce: {str(params)}")
        word_count_reduce(**params)
        task_request.status = wordcount_mr_pb2.TaskRequest.SUCCESS
    elif task_type == wordcount_mr_pb2.Task.WAIT:
        logging.info(f"wait: {str(params)}")
        sleep(params["wait_milliseconds"] / 1000)
    elif wordcount_mr_pb2.Task.TERMINATE:
        logging.info("terminate")
        return True
    logging.info("done")
    return False


def unpack_params(request):
    task_type = request.type
    params = dict()
    if request.input_path:
        params["input_path"] = request.input_path
    if request.output_path:
        params["output_path"] = request.output_path
    if request.job_id:
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
    elif task_type == wordcount_mr_pb2.Task.WAIT:
        if request.HasField("wait_milliseconds"):
            params["wait_milliseconds"] = request.wait_milliseconds
        else:
            params["wait_milliseconds"] = WAIT_DEFAULT_MILLISECONDS
    elif task_type == wordcount_mr_pb2.Task.TERMINATE:
        return task_type, None
    else:
        raise AssertionError("Unknown task type")
    return task_type, params


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
    run(args.worker_id)
