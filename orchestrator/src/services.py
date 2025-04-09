import sys
import os
import logging
from threading import Event
import threading
import grpc
from google.protobuf.json_format import MessageToDict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# gRPC stub imports (unchanged)
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


class OrchestratorService:
    def __init__(self, total_svcs=3):
        self.fraud_detection_url = "fraud_detection:50051"
        self.transaction_verification_url = "transaction_verification:50052"
        self.suggestions_url = "suggestions:50053"
        self.order_queue_url = "order_queue:50054"
        self.total_svcs = total_svcs
        self.vc = [0] * total_svcs
        self.verify_user_event_status = Event()
        self.verify_credit_card_status = Event()
        self.lock = threading.Lock()

    def merge_and_increment(self, local_vc, incoming_vc):
        with self.lock:
            for i in range(self.total_svcs):
                local_vc[i] = max(local_vc[i], incoming_vc[i])

    def fraud_init(self, order_id, order_data):
        logger.info(f"Fraud - Init order {order_id}")
        try:
            with grpc.insecure_channel(self.fraud_detection_url) as channel:
                stub = fraud_detection_grpc.FraudServiceStub(channel)
                stub.InitOrder(
                    fraud_detection.OrderRequest(
                        order_id=order_id, order_data=order_data
                    )
                )
                logger.info(f"Fraud - Order {order_id} cached")
        except Exception as e:
            logger.error(f"Fraud - Cache failed {order_id}: {e}")
            raise Exception("FRAUD_DETECTION: CACHING FAILED")

    def check_user(self, order_id):
        self.verify_user_event_status.wait()
        logger.info(f"Fraud - Check user {order_id}")
        try:
            with grpc.insecure_channel(self.fraud_detection_url) as channel:
                stub = fraud_detection_grpc.FraudServiceStub(channel)
                response = stub.CheckUser(
                    fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
                )
                response_dict = MessageToDict(response)
                logger.info(
                    f"Fraud - Check_User {order_id} OK, vc={response_dict['vc']}"
                )
                if response_dict["result"] == "fail":
                    raise Exception("FRAUD_DETECTION: CHECK USER FAILED")
                self.merge_and_increment(self.vc, response_dict["vc"])
        except Exception as e:
            raise

    def check_credit_card(self, order_id):
        self.verify_credit_card_status.wait()
        logger.info(f"Fraud - Check card {order_id}")
        try:
            with grpc.insecure_channel(self.fraud_detection_url) as channel:
                stub = fraud_detection_grpc.FraudServiceStub(channel)
                response = stub.CheckCreditCard(
                    fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
                )
                response_dict = MessageToDict(response)
                logger.info(
                    f"Fraud - Check_Card {order_id} OK, vc={response_dict['vc']}"
                )
                if response_dict["result"] == "fail":
                    raise Exception("FRAUD_DETECTION: CHECK CREDIT CARD FAILED")
                self.merge_and_increment(self.vc, response_dict["vc"])
        except Exception as e:
            raise

    def clean_fraud_order(self, order_id):
        logger.info(f"Fraud - Clean order {order_id}")
        try:
            with grpc.insecure_channel(self.fraud_detection_url) as channel:
                stub = fraud_detection_grpc.FraudServiceStub(channel)
                response = stub.CleanOrder(
                    fraud_detection.FraudRequest(order_id=order_id, vc=self.vc)
                )
                response_dict = MessageToDict(response)
                if response_dict["result"] == "fail":
                    logger.warning(f"Fraud - Clean failed {order_id}")
                else:
                    logger.info(f"Fraud - Order {order_id} cleaned")
        except Exception as e:
            logger.error(f"Fraud - Clean error {order_id}: {e}")

    def transaction_init(self, order_id, order_data):
        logger.info(f"Tx - Init order {order_id}")
        try:
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                stub.InitOrder(
                    transaction_verification.OrderRequest(
                        order_id=order_id, order_data=order_data
                    )
                )
                logger.info(f"Tx - Order {order_id} cached")
        except Exception as e:
            logger.error(f"Tx - Cache failed {order_id}: {e}")
            raise Exception("TRANSACTION VERIFICATION: CACHING FAILED")

    def verify_user(self, order_id):
        logger.info(f"Tx - Verify user {order_id}")
        try:
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                response = stub.VerifyUser(
                    transaction_verification.TransactionRequest(
                        order_id=order_id, vc=self.vc
                    )
                )
                response_dict = MessageToDict(response)
                logger.info(f"Tx - Verify_User {order_id} OK, vc={response_dict['vc']}")
                if response_dict["result"] == "fail":
                    raise Exception("TRANSACTION VERIFICATION: USER VERIFY FAILED")
                self.merge_and_increment(self.vc, response_dict["vc"])
        except Exception as e:
            raise
        finally:
            self.verify_user_event_status.set()

    def verify_credit_card(self, order_id):
        logger.info(f"Tx - Verify card {order_id}")
        try:
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                response = stub.VerifyCreditCard(
                    transaction_verification.TransactionRequest(
                        order_id=order_id, vc=self.vc
                    )
                )
                response_dict = MessageToDict(response)
                logger.info(f"Tx - Verify_Card {order_id} OK, vc={response_dict['vc']}")
                if response_dict["result"] == "fail":
                    raise Exception(
                        "TRANSACTION VERIFICATION: CREDIT CARD VERIFY FAILED"
                    )
                self.merge_and_increment(self.vc, response_dict["vc"])
        except Exception as e:
            raise
        finally:
            self.verify_credit_card_status.set()

    def verify_address(self, order_id):
        logger.info(f"Tx - Verify address {order_id}")
        try:
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                response = stub.VerifyAddress(
                    transaction_verification.TransactionRequest(
                        order_id=order_id, vc=self.vc
                    )
                )
                response_dict = MessageToDict(response)
                logger.info(
                    f"Tx - Verify_Address {order_id} OK, vc={response_dict['vc']}"
                )
                if response_dict["result"] == "fail":
                    raise Exception("TRANSACTION VERIFICATION: ADDRESS VERIFY FAILED")
                self.merge_and_increment(self.vc, response_dict["vc"])
        except Exception as e:
            raise

    def clean_transaction_order(self, order_id):
        logger.info(f"Tx - Clean order {order_id}")
        try:
            with grpc.insecure_channel(self.transaction_verification_url) as channel:
                stub = transaction_verification_grpc.TransactionVerificationServiceStub(
                    channel
                )
                response = stub.CleanOrder(
                    transaction_verification.TransactionRequest(
                        order_id=order_id, vc=self.vc
                    )
                )
                response_dict = MessageToDict(response)
                if response_dict["result"] == "fail":
                    logger.warning(f"Tx - Clean failed {order_id}")
                else:
                    logger.info(f"Tx - Order {order_id} cleaned")
        except Exception as e:
            logger.error(f"Tx - Clean error {order_id}: {e}")

    def suggestion_init(self, order_id, order_data):
        logger.info(f"Suggestions - Init order {order_id}")
        try:
            with grpc.insecure_channel(self.suggestions_url) as channel:
                stub = suggestions_grpc.SuggestionServiceStub(channel)
                stub.InitOrder(
                    suggestions.OrderRequest(order_id=order_id, order_data=order_data)
                )
                logger.info(f"Suggesstions - Order {order_id} cached")
        except Exception as e:
            logger.error(f"Suggestions - Cache failed {order_id}: {e}")
            raise Exception("SUGGESTIONS: CACHING FAILED")

    def get_suggestions(self, order_id):
        logger.info(f"Suggestions - Get suggestions {order_id}")
        try:
            with grpc.insecure_channel(self.suggestions_url) as channel:
                stub = suggestions_grpc.SuggestionServiceStub(channel)
                response = stub.GetSuggestions(
                    suggestions.SuggestionRequest(order_id=order_id, vc=self.vc)
                )
                response_dict = MessageToDict(response)
                logger.info(
                    f"Suggestions - Get_Suggestions {order_id} OK, vc={response_dict['vc']}"
                )
                self.merge_and_increment(self.vc, response_dict["vc"])
                return response_dict.get("suggestedBooks", [])
        except Exception as e:
            logger.error(f"Suggestions - Fetch failed {order_id}: {e}")
            return []

    def clean_suggestion_order(self, order_id):
        logger.info(f"Suggestions - Clean order {order_id}")
        try:
            with grpc.insecure_channel(self.suggestions_url) as channel:
                stub = suggestions_grpc.SuggestionServiceStub(channel)
                response = stub.CleanOrder(
                    suggestions.SuggestionRequest(order_id=order_id, vc=self.vc)
                )
                response_dict = MessageToDict(response)
                if response_dict["result"] == "fail":
                    logger.warning(f"Suggestions - Clean failed {order_id}")
                else:
                    logger.info(f"Suggesstions - Order {order_id} cleaned")
        except Exception as e:
            logger.error(f"Suggestions - Clean error {order_id}: {e}")

    def enqueue_order(self, order_id, order_data):
        logger.info(f"Queue - Enqueue order {order_id}")
        try:
            with grpc.insecure_channel(self.order_queue_url) as channel:
                stub = order_queue_grpc.OrderQueueServiceStub(channel)
                response = stub.EnqueueOrder(
                    order_queue.OrderRequest(order_id=order_id, order_data=order_data)
                )
                response_dict = MessageToDict(response)
                if response_dict["result"] == "fail":
                    raise Exception(f"Queue - Order Enqueue failed {order_id}")

                logger.info(f"Queue - Order {order_id} enqueued")
        except Exception as e:
            logger.error(f"Queue - Enqueue failed {order_id}: {e}")
