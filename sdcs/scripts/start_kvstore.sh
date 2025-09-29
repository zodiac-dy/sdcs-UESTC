#!/bin/bash
echo "Starting KVStore Node $CURRENT_ID of $TOTAL_SERVERS..."
python3 /app/src/grpc_server.py &       # 启动 gRPC 服务
python3 /app/src/http_server.py    # 启动 HTTP 接口