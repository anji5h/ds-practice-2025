from concurrent import futures
import os
import sys
import grpc
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
suggestion_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/payment")
)
sys.path.insert(0, suggestion_grpc_path)

import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

class PaymentService(payment_grpc.PaymentServiceServicer):
    def __init__(self):
        self.prepared_transactions = set()

    def Prepare(self, request, context):
        self.prepared_transactions.add(request.transaction_id)
        logging.info(f"Payment prepared for {request.transaction_id}")
        return payment.Vote(ready=True)

    def Commit(self, request, context):
        if request.transaction_id in self.prepared_transactions:
            self.prepared_transactions.remove(request.transaction_id)
            logging.info(f"Payment committed for {request.transaction_id}")
            return payment.Ack(success=True)
        return payment.Ack(success=False)

    def Abort(self, request, context):
        if request.transaction_id in self.prepared_transactions:
            self.prepared_transactions.remove(request.transaction_id)
            logging.info(f"Payment aborted for {request.transaction_id}")
            return payment.Ack(success=True)
        return payment.Ack(success=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    port = 50058
    server.add_insecure_port(f'[::]:{port}')
    logging.info(f"server started on port:{port}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()