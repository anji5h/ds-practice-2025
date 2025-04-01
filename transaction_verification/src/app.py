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


# Create a class to define the server functions, derived from
# transaction_pb2_grpc.TransactionVerificationServiceServicer
class TransactionVerificationService(
    transaction_grpc.TransactionVerificationServiceServicer
):
    def __init__(self, svc_idx=0, total_svcs=3):
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

    # Create an RPC function to verify transaction
    def VerifyTransaction(self, request, context):
        # Transaction verification logic
        # Example: approve transactions below a threshold, flag those above
        is_verified = False if request.cvv > 999 else True
        # Create a TransactionVerificationResponse object
        response = transaction.TransactionVerificationResponse()
        # Set the status and message in the response object
        response.is_verified = is_verified
        # Print the status message
        print(f"Sending transaction verification result: {response.is_verified}")
        # Return the response object
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
