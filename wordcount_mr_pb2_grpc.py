# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import wordcount_mr_pb2 as wordcount__mr__pb2


class WordCountMRStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.DoTask = channel.unary_unary(
                '/wordcount_mr.WordCountMR/DoTask',
                request_serializer=wordcount__mr__pb2.Task.SerializeToString,
                response_deserializer=wordcount__mr__pb2.Status.FromString,
                )


class WordCountMRServicer(object):
    """Missing associated documentation comment in .proto file."""

    def DoTask(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WordCountMRServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'DoTask': grpc.unary_unary_rpc_method_handler(
                    servicer.DoTask,
                    request_deserializer=wordcount__mr__pb2.Task.FromString,
                    response_serializer=wordcount__mr__pb2.Status.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'wordcount_mr.WordCountMR', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class WordCountMR(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def DoTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/wordcount_mr.WordCountMR/DoTask',
            wordcount__mr__pb2.Task.SerializeToString,
            wordcount__mr__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)