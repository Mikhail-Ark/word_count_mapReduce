from concurrent import futures
import logging

import grpc
import wordcount_mr_pb2
import wordcount_mr_pb2_grpc


class WordCountMR(wordcount_mr_pb2_grpc.WordCountMRServicer):

    def DoTask(self, request, context):
        return wordcount_mr_pb2.Status(success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    wordcount_mr_pb2_grpc.add_WordCountMRServicer_to_server(WordCountMR(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
