import os
import sys
import threading
import heapq
import grpc
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/order_queue")
)
sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc


# Create a class to define the server functions, derived from
# order_queue_pb2_grpc.OrderQueueServiceServicer
class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []

    def _log_queue_state(self):
        print("current queue state: ")
        for idx, (priority, order_id, _) in enumerate(self._queue):
            print(f"idx {idx + 1}: order_id = {order_id}, priority = {priority}")

    def _get_priority(self, order_id: str) -> int:
        first_char = order_id[0].lower()
        if first_char.isdigit():
            return int(first_char)
        else:
            return ord(first_char) - ord("a") + 10

    def EnqueueOrder(self, request, context):
        response = order_queue.EnqueueResponse()
        try:
            with self._lock:
                order_data = request.order_data
                order_id = request.order_id

                priority = self._get_priority(order_id)
                heapq.heappush(self._queue, (priority, order_id, order_data))
                print(f"Order {order_id} enqueued")
                self._log_queue_state()

                response.result = "pass"
                return response
        except:
            response.result = "fail"
            return response

    def DequeueOrder(self, request, context):
        response = order_queue.OrderResponse()
        try:
            with self._lock:
                if not self._queue:
                    response.available = False
                    return response

                _, order_id, order_data = heapq.heappop(self._queue)
                print(f"Order {order_id} dequed")

                response.order_id = order_id
                response.order_data = order_data
                response.available = True

                return response
        except:
            response.available = False
            return response


def serve_queue_service():
    server = grpc.server(futures.ThreadPoolExecutor())
    order_queue_grpc.add_OrderQueueServiceServicer_to_server(
        OrderQueueService(), server
    )
    port = 50054
    server.add_insecure_port(f"[::]:{port}")
    print(f"Order Queue Service started on port {port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve_queue_service()
