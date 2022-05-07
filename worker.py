from concurrent import futures
import logging

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc

MAP_DEFAULT_IGNORE_CASE = False
MAP_DEFAULT_N_BUCKETS = 1
MAP_DEFAULT_SORT = False
REDUCE_DEFAULT_MERGE_JOIN = MAP_DEFAULT_SORT


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def DoTask(self, request, context):
        task_type, params = self.unpack_params(request)
        print(task_type)
        print(params)
        return wordcount_mr_pb2.Status(success=True)


    def unpack_params(self, request):
        task_type = request.type
        params = dict()
        params["input_file_path"] = request.input
        params["output_path"] = request.output
        params["job_id"] = request.job_id
        if task_type == wordcount_mr_pb2.Task.WORDCOUNT_MAP:
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
        elif task_type == wordcount_mr_pb2.Task.WORDCOUNT_REDUCE:
            if request.HasField("n_buckets"):
                params["merge_join"] = request.merge_join
            else:
                params["merge_join"] = REDUCE_DEFAULT_MERGE_JOIN
        else:
            raise AssertionError("Unknown task type")
        return task_type, params


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    wordcount_mr_pb2_grpc.add_WordCountMRServicer_to_server(WordCountMR(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
