import json
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
from google.protobuf import empty_pb2


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class FraudService(fraud_detection_grpc.FraudServiceServicer):
    def __init__(self, svc_idx=1, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def InitOrder(self, request, context):
        data = json.loads(request.order_data)
        self.orders[request.order_id] = {"data": data, "vc": [0] * self.total_svcs}
        return empty_pb2.Empty()

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])
        local_vc[self.svc_idx] += 1

    def CheckCreditCard(self, request, context):
        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = fraud_detection.FraudResponse()

        if not order_data:
            response.is_fraud = True
            response.vc = order_data["vc"]
            return response

        credit_card_no: str = order_data["creditCard"]["number"]

        response.is_fraud = credit_card_no.startswith("1111")
        response.vc = order_data["vc"]

        return response

    def CheckUser(self, request, context):
        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = fraud_detection.FraudResponse()

        if not order_data:
            response.is_fraud = True
            response.vc = order_data["vc"]
            return response

        user_email = order_data["user"]["contact"]
        response.is_fraud = not user_email.lower().endswith("@gmail.com")
        response.vc = order_data["vc"]

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
