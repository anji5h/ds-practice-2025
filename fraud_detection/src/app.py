import json
import sys
import os
import grpc
from concurrent import futures

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
from google.protobuf import empty_pb2


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.FraudServiceServicer
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

    def clean_order(self, order_id, local_vc, incoming_vc):
        if local_vc[self.svc_idx] <= incoming_vc[self.svc_idx]:
            self.orders.pop(order_id)
            return True
        else:
            return False

    def CheckCreditCard(self, request, context):
        print(f"checking credit data of order {request.order_id}")
        order_data = self.orders.get(request.order_id, None)

        response = fraud_detection.FraudResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail"
            if order_data["data"]["creditCard"]["number"].startswith("1111")
            else "pass"
        )

        response.vc.extend(order_data["vc"])
        return response

    def CheckUser(self, request, context):
        print(f"checking user data of order {request.order_id}")
        order_data = self.orders.get(request.order_id, None)

        response = fraud_detection.FraudResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        self.merge_and_increment(order_data["vc"], request.vc)

        response.result = (
            "fail"
            if (order_data["data"]["user"]["contact"]).endswith("@example.com")
            else "pass"
        )

        response.vc.extend(order_data["vc"])
        return response

    def CleanOrder(self, request, context):
        print(f"cleaning order {request.order_id}")
        order_data = self.orders.get(request.order_id, None)

        response = fraud_detection.FraudResponse()

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
    # Add FraudService
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
