import argparse
import grpc
import logging
from time import sleep

from proto_tools import make_init_task_request, make_wait_task, unpack_params
from tasks.map import word_count_map
from tasks.reduce import word_count_reduce
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc


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
                task = make_wait_task()
            terminate = do_task(task, task_request)
    logging.info("terminated")


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
