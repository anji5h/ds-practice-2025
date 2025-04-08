import json
import sys
import os
import grpc
import logging
from concurrent import futures
import threading


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


class TransactionVerificationService(
    transaction_grpc.TransactionVerificationServiceServicer
):
    def __init__(self, svc_idx=0, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}
        self.lock = threading.Lock()

    def InitOrder(self, request, context):
        data = json.loads(request.order_data)
        self.orders[request.order_id] = {"data": data, "vc": [0] * self.total_svcs}
        return empty_pb2.Empty()

    def merge_and_increment(self, local_vc, incoming_vc):
        with self.lock:
            for i in range(self.total_svcs):
                local_vc[i] = max(local_vc[i], incoming_vc[i])
            local_vc[self.svc_idx] += 1

    def clean_order(self, order_id, local_vc, incoming_vc):
        if local_vc[self.svc_idx] <= incoming_vc[self.svc_idx]:
            if order_id in self.orders:
                self.orders.pop(order_id)
            return True
        else:
            return False

    def VerifyUser(self, request, context):
        logger.info(f"Verify user: Received order_id {request.order_id}")
        order_data = self.orders.get(request.order_id)
        response = transaction.TransactionResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail"
            if (
                order_data["data"]["user"]["name"] == ""
                or order_data["data"]["user"]["contact"] == ""
            )
            else "pass"
        )
        response.vc.extend(order_data["vc"])

        logger.info(f"Verify user: Response {response}")
        return response

    def VerifyAddress(self, request, context):
        logger.info(f"Verify Address: Received order_id {request.order_id}")
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

        logger.info(f"Verify Address: Response {response}")
        return response

    def VerifyCreditCard(self, request, context):
        logger.info(f"Verify Credit Card: Received order_id {request.order_id}")
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

        logger.info(f"Verify Credit Card: Response {response}")
        return response

    def CleanOrder(self, request, context):
        logger.info(f"Cleaning order {request.order_id}")
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
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server
    )

    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port 50051.")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
