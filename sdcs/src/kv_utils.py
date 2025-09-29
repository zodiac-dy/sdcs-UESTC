import os
import hashlib
import grpc
import kvstore_pb2_grpc

def get_target_id(key: str) -> int:
    """
    根据 key 计算应该存储的节点 ID。
    使用 MD5 哈希，将哈希值对节点总数取模得到目标节点。
    """
    md5_hash = hashlib.md5(key.encode())
    int_hash = int(md5_hash.hexdigest(), 16)
    total_servers = int(os.environ['Total_servers'])
    target_id = int_hash % total_servers + 1  # 节点编号从 1 开始
    return target_id

def create_stub(target_id: int):
    """
    创建 gRPC stub，用于访问指定节点。
    """
    channel = grpc.insecure_channel(f'cache{target_id}:50051')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)
    return stub
