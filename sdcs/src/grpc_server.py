import os
from concurrent import futures

import grpc
import kvstore_pb2, kvstore_pb2_grpc
from kv_utils import get_target_id, create_stub
from google.protobuf import wrappers_pb2

class MyKVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    """
    分布式 KV 服务端
    支持 str, int, bool, float, list, dict
    list 和 dict 已经在 HTTP 层序列化为 JSON 字符串
    """
    def __init__(self):
        self.__current_id = int(os.environ['Server_id'])
        self.__kv_data = {}  # 本地存储 KV

    def Set(self, request, context):
        """
        设置 KV 值
        """
        target_id = get_target_id(request.key)
        if self.__current_id == target_id:
            # 本节点存储
            self.__kv_data[request.key] = request.value
        else:
            # 转发到目标节点
            stub = create_stub(target_id)
            stub.Set(kvstore_pb2.SetRequest(key=request.key, value=request.value))
        return kvstore_pb2.SetResponse()

    def Get(self, request, context):
        """
        获取 KV 值
        """
        target_id = get_target_id(request.key)
        if self.__current_id == target_id:
            value = self.__kv_data.get(request.key)
            return kvstore_pb2.GetResponse(value=value)
        else:
            stub = create_stub(target_id)
            return stub.Get(kvstore_pb2.GetRequest(key=request.key))

    def Remove(self, request, context):
        """
        删除 KV 值
        """
        target_id = get_target_id(request.key)
        if self.__current_id == target_id:
            success = self.__kv_data.pop(request.key, None) is not None
            return kvstore_pb2.RemoveResponse(success=success)
        else:
            stub = create_stub(target_id)
            return stub.Remove(kvstore_pb2.RemoveRequest(key=request.key))

def serve():
    """
    启动 gRPC 服务
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(MyKVStoreServicer(), server)
    server.add_insecure_port('[::]:50051')
    print(f"gRPC server started on port 50051 (NODE {os.environ['Server_id']})")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
