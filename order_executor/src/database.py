import logging
import os
import sys
import grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# gRPC stub import setup
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
book_database_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/book_database")
)
sys.path.insert(0, book_database_grpc_path)

import book_database_pb2 as books_pb2
import book_database_pb2_grpc as books_pb2_grpc

class DatabaseClient:
    def __init__(self, host='book_database_primary', port=50055):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = books_pb2_grpc.BooksDatabaseStub(self.channel)

    def read_stock(self, name):
        try:
            response = self.stub.Read(books_pb2.ReadRequest(name=name))
            return response.stock
        except grpc.RpcError as e:
            print(f"Read failed: {e.code()}: {e.details()}")
            return -1

    def write_stock(self, name, stock):
        try:
            response = self.stub.Write(
                books_pb2.WriteRequest(name=name, new_stock=stock)
            )
            return response.success
        except grpc.RpcError as e:
            print(f"Write failed: {e.code()}: {e.details()}")
            return False

    def decrement_stock(self, name, quantity):
        try:
            response = self.stub.DecrementStock(
                books_pb2.DecrementRequest(name=name, quantity=quantity)
            )
            return response.success
        except grpc.RpcError as e:
            print(f"Decrement failed: {e.code()}: {e.details()}")
            return False

    def increment_stock(self, name, quantity):
        try:
            response = self.stub.IncrementStock(
                books_pb2.IncrementRequest(name=name, quantity=quantity)
            )
            return response.success
        except grpc.RpcError as e:
            print(f"Increment failed: {e.code()}: {e.details()}")
            return False

    def compare_and_swap(self, name, expected, new_value):
        try:
            response = self.stub.CompareAndSwap(
                books_pb2.CASRequest(
                    name=name,
                    expected_value=expected,
                    new_value=new_value
                )
            )
            return response.success
        except grpc.RpcError as e:
            print(f"CAS failed: {e.code()}: {e.details()}")
            return False
    
    def prepare_update(self, transaction_id, name, quantity):
        try:
            response = self.stub.Prepare(
                books_pb2.PrepareRequest(
                    transaction_id=transaction_id,
                    name=name,
                    quantity=quantity
                )
            )
            return response.ready
        except grpc.RpcError as e:
            print(f"Prepare failed: {e.code()}: {e.details()}")
            return False

    def commit_update(self, transaction_id):
        try:
            response = self.stub.Commit(
                books_pb2.CommitRequest(transaction_id=transaction_id)
            )
            return response.success
        except grpc.RpcError as e:
            print(f"Commit failed: {e.code()}: {e.details()}")
            return False

    def abort_update(self, transaction_id):
        try:
            response = self.stub.Abort(
                books_pb2.AbortRequest(transaction_id=transaction_id)
            )
            return response.success
        except grpc.RpcError as e:
            print(f"Abort failed: {e.code()}: {e.details()}")
            return False
