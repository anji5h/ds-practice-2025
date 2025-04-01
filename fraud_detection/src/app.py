import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/fraud_detection")
)
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class FraudService(fraud_detection_grpc.FraudServiceServicer):
    def __init__(self, svc_idx=1, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def InitOrder(self, order_id, data):
        # Initialize an order with an empty vector clock for tracking
        self.orders[order_id] = {"data": data, "vc": [0] * self.total_svcs}

    def merge_and_increment(self, local_vc, incoming_vc):
        # Merge the incoming vector clock with the local one and increment the current service's slot
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])  # Merge the clocks
        local_vc[self.svc_idx] += 1  # Increment this service's vector clock slot

    # Create an RPC function to check fraud
    def CheckFraud(self, request, context):
        # Create a FraudResponse object
        response = fraud_detection.FraudResponse()
        # Set the is_fraud of the response object
        response.is_fraud = request.number.startswith("1111")
        # Print the fraud result
        print(f"Fraud check result for card: XXXX-XXXX-XXXX-{request.number[-4:]}")
        # Return the response object
        return response


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    fraud_detection_grpc.add_FraudServiceServicer_to_server(FraudService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
