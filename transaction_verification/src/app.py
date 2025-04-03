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

    def clean_order(self, order_id, local_vc, incoming_vc):
        if local_vc[self.svc_idx] <= incoming_vc[self.svc_idx]:
            self.orders.pop(order_id)
            return True
        else:
            return False

    def VerifyUser(self, request, context):
        print(f"Verify user: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)
        response = transaction.TransactionResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail"
            if order_data["data"]["user"]["name"] == ""
            or order_data["data"]["user"]["contact"] == ""
            else "pass"
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify user: Response {response}\n")
        return response

    def VerifyAddress(self, request, context):
        print(f"Verify Address: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)

        response = transaction.TransactionResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail"
            if order_data["data"]["billingAddress"]["country"] != "USA"
            else "pass"
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify Address: Response {response}\n")

        return response

    def VerifyCreditCard(self, request, context):
        print(f"Verify Credit Card: Received order_id {request.order_id}\n")
        order_data = self.orders.get(request.order_id)

        response = transaction.TransactionResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail" if len(order_data["data"]["creditCard"]["number"]) < 16 else "pass"
        )
        response.vc.extend(order_data["vc"])

        print(f"Verify Credit Card: Response {response}\n")
        return response

    def CleanOrder(self, request, context):
        print(f"cleaning order {request.order_id}")
        order_data = self.orders.get(request.order_id, None)

        response = transaction.TransactionResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        response.result = (
            "fail"
            if not self.clean_order(request.order_id, order_data["vc"], request.vc)
            else "pass"
        )
        response.vc.extend(order_data["vc"])
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
