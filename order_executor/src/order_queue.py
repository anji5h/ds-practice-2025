import logging
import os
import sys
import grpc
from google.protobuf import empty_pb2

# Configure standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# gRPC stub import setup
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, order_queue_grpc_path)

import order_queue_pb2_grpc as order_queue_grpc

class OrderQueueClient:
    def __init__(self, host='order_queue', port=50054):
        channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = order_queue_grpc.OrderQueueServiceStub(channel)
    
    def process_order(self):
        try:
            response = self.stub.DequeueOrder(empty_pb2.Empty())
            return response
        except grpc.RpcError as e:
            logger.error(f"gRPC error while dequeuing order: error={e.details()}")