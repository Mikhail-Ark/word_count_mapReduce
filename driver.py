import argparse
import logging

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc


def run(n_map, n_reduce):
    logging.info(f"run with n_map={n_map}, n_reduce={n_reduce}")
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = wordcount_mr_pb2_grpc.WordCountMRStub(channel)
        task = make_map_task()
        logging.info("send task")
        response = stub.DoTask(task)
        logging.info(f"success: {str(response.success)}")


def make_map_task():
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.MAP,
        input_path="./files/inputs/testing/",
        output_path="./files/intermediate/testing/",
        input_file_names = ["test_small.txt"],
        job_id = 0,
        ignore_case=False,
        sort=False,
        n_buckets = 1
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
        format="%(asctime)s %(message)s", filemode='w'
    )
    run(n_map=args.n_map, n_reduce=args.n_reduce)
