import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

fraud_detection_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/fraud_detection")
)
transaction_verification_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/transaction_verification")
)
suggestions_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/suggestions")
)
order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/order_queue")
)

sys.path.insert(0, fraud_detection_grpc_path)
sys.path.insert(1, transaction_verification_grpc_path)
sys.path.insert(2, suggestions_grpc_path)
sys.path.insert(3, order_queue_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

import grpc
from google.protobuf.json_format import MessageToDict


class OrchestratorService:
    def __init__(self, total_svcs=3):
        self.fraud_detection_url = "fraud_detection:50051"
        self.transaction_verification_url = "transaction_verification:50052"
        self.suggestions_url = "suggestions:50053"
        self.order_queue_url = "order_queue:50054"
        self.total_svcs = total_svcs
        self.vc = [0] * total_svcs

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])

    def fraud_init(self, order_id, order_data):
        try:
            print(f"Starting fraud init request\n")
            with grpc.insecure_channel(self.fraud_detection_url) as channel:
                stub = fraud_detection_grpc.FraudServiceStub(channel)
                stub.InitOrder(
                    fraud_detection.OrderRequest(
                        order_id=order_id, order_data=order_data
                    )
                )
                print(f"fraud_detection: order {order_id} cached\n")
        except Exception as e:
            print(f"{self.fraud_init.__name__}:{str(e)}")
            raise Exception(f"FRAUD_DETECTION: CACHING FAILED")

    def check_user(self, order_id):
        print(f"Starting check user request, order_id: {order_id}\n")
        with grpc.insecure_channel(self.fraud_detection_url) as channel:
            stub = fraud_detection_grpc.FraudServiceStub(channel)
            response = stub.CheckUser(
                fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
            )
            print(f"fraud_detection: check_user complete\n")
            response_dict = MessageToDict(response)
            if response_dict["result"] == "fail":
                raise Exception(f"FRAUD_DETECTION: CHECK USER FAILED")

            self.merge_and_increment(self.vc, response_dict["vc"])

    def check_credit_card(self, order_id):
        print(f"Starting check credit card request, order_id: {order_id}\n")
        with grpc.insecure_channel(self.fraud_detection_url) as channel:
            stub = fraud_detection_grpc.FraudServiceStub(channel)
            response = stub.CheckCreditCard(
                fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
            )
            print(f"fraud_detection: check_user complete\n")
            response_dict = MessageToDict(response)
            if response_dict["result"] == "fail":
                raise Exception(f"FRAUD_DETECTION: CHECK USER FAILED")

            self.merge_and_increment(self.vc, response_dict["vc"])

    def clean_fraud_order(self, order_id):
        print(f"Starting fraud cleanup request, order_id: {order_id}\n")
        with grpc.insecure_channel(self.fraud_detection_url) as channel:
            stub = fraud_detection_grpc.FraudServiceStub(channel)
            response = stub.CleanOrder(
                fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
            )
            print(f"fraud_detection: order cleanup complete\n")
            response_dict = MessageToDict(response)
            if response_dict["result"] == "fail":
                print(f"FRAUD_DETECTION: CLEANUP FAILED, {order_id}")

    def transaction_init(self, order_id, order_data):
        try:
            print(f"Starting transaction init request\n")
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                stub.InitOrder(
                    transaction_verification.OrderRequest(
                        order_id=order_id, order_data=order_data
                    )
                )
                print(f"transaction_verification: order {order_id} cached\n")
        except Exception as e:
            print(f"{self.transaction_init.__name__}{str(e)}")
            raise Exception(f"TRANSACTION VERIFICATION: CACHING FAILED")

    def verify_user(self, order_id):
        print(f"Starting transaction verify user request\n")
        with grpc.insecure_channel(self.transaction_verification_url) as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                channel
            )
            response = stub.VerifyUser(
                transaction_verification.TransactionRequest(
                    order_id=order_id, vc=self.vc
                )
            )
            print(f"transaction_verification: verify user complete")
            response_dict = MessageToDict(response)

            if response_dict["result"] == "fail":
                raise Exception(f"TRANSACTION VERIFICATION: USER VERIFY FAILED")

            self.merge_and_increment(self.vc, response_dict["vc"])

    def verify_credit_card(self, order_id):
        print(f"Starting transaction verify credit card request\n")
        with grpc.insecure_channel(self.transaction_verification_url) as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                channel
            )
            response = stub.VerifyCreditCard(
                transaction_verification.TransactionRequest(
                    order_id=order_id, vc=self.vc
                )
            )
            print(f"transaction_verification: verify credit card complete\n")
            response_dict = MessageToDict(response)

            if response_dict["result"] == "fail":
                raise Exception(f"TRANSACTION VERIFICATION: CREDIT CARD VERIFY FAILED")

            self.merge_and_increment(self.vc, response_dict["vc"])

    def verify_address(self, order_id):
        print(f"Starting transaction verify address request\n")
        with grpc.insecure_channel(self.transaction_verification_url) as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                channel
            )
            response = stub.VerifyAddress(
                transaction_verification.TransactionRequest(
                    order_id=order_id, vc=self.vc
                )
            )
            print(f"transaction_verification: verify address complete\n")
            response_dict = MessageToDict(response)

            if response_dict["result"] == "fail":
                raise Exception(f"TRANSACTION VERIFICATION: ADDRESS VERIFY FAILED")

            self.merge_and_increment(self.vc, response_dict["vc"])

    def clean_transaction_order(self, order_id):
        print(f"Starting transaction cleanup request\n")
        with grpc.insecure_channel(self.transaction_verification_url) as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                channel
            )
            response = stub.CleanOrder(
                transaction_verification.TransactionRequest(
                    order_id=order_id, vc=self.vc
                )
            )
            print(f"transaction_verification: order cleanup complete\n")
            response_dict = MessageToDict(response)

            if response_dict["result"] == "fail":
                print(f"TRANSACTION VERIFICATION: CLEANUP FAILED, {order_id}")

    def suggestion_init(self, order_id, order_data):
        try:
            print(f"Starting book init request\n")
            with grpc.insecure_channel(self.suggestions_url) as channel:
                stub = suggestions_grpc.SuggestionServiceStub(channel)
                stub.InitOrder(
                    suggestions.OrderRequest(order_id=order_id, order_data=order_data)
                )
                print(f"suggestions: order {order_id} cached\n")
        except Exception as e:
            print(f"{self.suggestion_init.__name__}{str(e)}")
            raise Exception(f"SUGGESTIONS: CACHING FAILED")

    def get_suggestions(self, order_id):
        try:
            print(f"Starting book suggestions request\n")
            with grpc.insecure_channel(self.suggestions_url) as channel:
                stub = suggestions_grpc.SuggestionServiceStub(channel)
                response = stub.GetSuggestions(
                    suggestions.SuggestionRequest(order_id=order_id, vc=self.vc)
                )
                print(f"suggestions: get_suggestions complete\n")

                response_dict = MessageToDict(response)
                self.merge_and_increment(self.vc, response_dict["vc"])

                return response_dict.get("suggestedBooks", [])
        except Exception as e:
            print(f"ERROR in suggestions service: {str(e)}")
            return []

    def clean_suggestion_order(self, order_id):
        print(f"Starting suggestion cleanup request\n")
        with grpc.insecure_channel(self.suggestions_url) as channel:
            stub = suggestions_grpc.SuggestionServiceStub(channel)
            response = stub.CleanOrder(
                suggestions.SuggestionRequest(order_id=order_id, vc=self.vc)
            )
            print(f"suggestions: order cleanup complete\n")

            response_dict = MessageToDict(response)
            if response_dict["result"] == "fail":
                print(f"SUGGESTIONS: CLEANUP FAILED, {order_id}")

    def enqueue_order(self, order_id, order_data):
        print(f"Starting order enqueue request\n")
        with grpc.insecure_channel(self.order_queue_url) as channel:
            stub = order_queue_grpc.OrderQueueServiceStub(channel)
            response = stub.EnqueueOrder(
                order_queue.OrderRequest(order_id=order_id, order_data=order_data)
            )
            print(f"order_queue: enqueue order complete\n")

            response_dict = MessageToDict(response)
            if response_dict["result"] == "fail":
                print(f"ENQUEUE ORDER FAILED, {order_id}")