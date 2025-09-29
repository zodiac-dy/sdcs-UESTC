from flask import Flask, request, jsonify
import grpc
import json
from google.protobuf import any_pb2, wrappers_pb2
import kvstore_pb2, kvstore_pb2_grpc

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

def init_stub():
    """
    初始化 gRPC stub（默认连接到本机 50051）
    """
    channel = grpc.insecure_channel('localhost:50051')
    return kvstore_pb2_grpc.KVStoreStub(channel)

stub = init_stub()

@app.route('/')
def index():
    return 'Simple Distributed Cache System\n', 200

@app.route('/', methods=['POST'])
def set_value():
    """
    设置 KV 值
    支持 str, int, bool, float, list, dict 类型
    """
    data = request.json
    key, value = list(data.items())[0]

    any_value = any_pb2.Any()

    if isinstance(value, str):
        wrappers_value = wrappers_pb2.StringValue(value=value)
        any_value.Pack(wrappers_value)
    elif isinstance(value, int):
        wrappers_value = wrappers_pb2.Int32Value(value=value)
        any_value.Pack(wrappers_value)
    elif isinstance(value, bool):
        wrappers_value = wrappers_pb2.BoolValue(value=value)
        any_value.Pack(wrappers_value)
    elif isinstance(value, float):
        wrappers_value = wrappers_pb2.FloatValue(value=value)
        any_value.Pack(wrappers_value)
    elif isinstance(value, (list, dict)):
        # 使用 JSON 序列化 list 和 dict
        json_str = json.dumps(value)
        wrappers_value = wrappers_pb2.StringValue(value=json_str)
        any_value.Pack(wrappers_value)
    else:
        return "Unsupported type", 400

    stub.Set(kvstore_pb2.SetRequest(key=key, value=any_value))
    return '', 200

@app.route('/<key>', methods=['GET'])
def get_value(key):
    """
    获取 KV 值，自动反序列化 list 和 dict
    """
    response = stub.Get(kvstore_pb2.GetRequest(key=key))
    any_value = response.value

    if any_value is None:
        return '', 404

    # 尝试解包不同类型
    str_value = wrappers_pb2.StringValue()
    if any_value.Unpack(str_value):
        try:
            # 尝试把 JSON 字符串解析回 list 或 dict
            parsed_value = json.loads(str_value.value)
            return jsonify({key: parsed_value}), 200
        except json.JSONDecodeError:
            # 不是 JSON，就返回原始字符串
            return jsonify({key: str_value.value}), 200

    int_value = wrappers_pb2.Int32Value()
    if any_value.Unpack(int_value):
        return jsonify({key: int_value.value}), 200

    bool_value = wrappers_pb2.BoolValue()
    if any_value.Unpack(bool_value):
        return jsonify({key: bool_value.value}), 200

    float_value = wrappers_pb2.FloatValue()
    if any_value.Unpack(float_value):
        return jsonify({key: float_value.value}), 200

    return '', 404

@app.route('/<key>', methods=['DELETE'])
def remove_value(key):
    """
    删除 KV 值
    """
    response = stub.Remove(kvstore_pb2.RemoveRequest(key=key))
    return '1\n' if response.success else '0\n'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
