from codecs import ignore_errors
import logging

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = wordcount_mr_pb2_grpc.WordCountMRStub(channel)
        task = make_map_task()
        response = stub.DoTask(task)
    print("Driver received: " + str(response.success))


def make_map_task():
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.WORDCOUNT_MAP,
        input=["./files/inputs/testing/test_small.txt"],
        output="./files/intermediate/testing/",
        job_id = 0,
        ignore_case=False,
        sort=False,
        n_buckets = 1
    )


if __name__ == '__main__':
    logging.basicConfig()
    run()
