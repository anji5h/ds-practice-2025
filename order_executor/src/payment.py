import logging
import os
import sys
import grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
payment_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/payment")
)
sys.path.insert(0, payment_grpc_path)

import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

class PaymentClient:
    def __init__(self, host='payment', port=50058):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = payment_grpc.PaymentServiceStub(self.channel)

    def prepare_update(self, transaction_id):
        try:
            response = self.stub.Prepare(
                payment.PrepareRequest(
                    transaction_id=transaction_id,
                )
            )
            return response.ready
        except grpc.RpcError as e:
            logging.error(f"Payment prepare failed: {e.code()}: {e.details()}")
            return False

    def commit_update(self, transaction_id):
        try:
            response = self.stub.Commit(
                payment.CommitRequest(transaction_id=transaction_id)
            )
            return response.success
        except grpc.RpcError as e:
            logging.error(f"Payment commit failed: {e.code()}: {e.details()}")
            return False

    def abort_update(self, transaction_id):
        try:
            response = self.stub.Abort(
                payment.AbortRequest(transaction_id=transaction_id)
            )
            return response.success
        except grpc.RpcError as e:
            logging.error(f"Payment abort failed: {e.code()}: {e.details()}")
            return False