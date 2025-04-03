import json
import sys
import os
import grpc
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
transaction_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/transaction_verification")
)
sys.path.insert(0, transaction_grpc_path)

import transaction_verification_pb2 as transaction
import transaction_verification_pb2_grpc as transaction_grpc
from google.protobuf import empty_pb2


# Create a class to define the server functions, derived from
# transaction_pb2_grpc.TransactionVerificationServiceServicer
class TransactionVerificationService(
    transaction_grpc.TransactionVerificationServiceServicer
):
    def __init__(self, svc_idx=0, total_svcs=3):
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

    def VerifyUser(self, request, context):
        print(f"Verify user: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = transaction.TransactionVerificationResponse()

        if not order_data:
            response.is_verified = False
            response.vc.extend(order_data["vc"])
            return response

        print(order_data["data"])
        response.is_verified = bool(
            order_data["data"]["user"]["name"] and order_data["data"]["user"]["contact"]
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify user: Response {response.is_verified}\n")

        return response

    def VerifyAddress(self, request, context):
        print(f"Verify Address: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = transaction.TransactionVerificationResponse()

        if not order_data:
            response.is_verified = False
            response.vc.extend(order_data["vc"])
            return response

        response.is_verified = bool(
            order_data["data"]["billingAddress"]["country"] == "USA"
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify Address: Response {response}\n")

        return response

    def VerifyCreditCard(self, request, context):
        print(f"Verify Credit Card: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = transaction.TransactionVerificationResponse()

        if not order_data:
            response.is_verified = False
            response.vc.extend(order_data["vc"])
            return response

        response.is_verified = bool(
            len(order_data["data"]["creditCard"]["number"]) > 10
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify Credit Card: Response {response}\n")

        return response


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add TransactionVerificationService to the server
    transaction_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server
    )

    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep the server running
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
