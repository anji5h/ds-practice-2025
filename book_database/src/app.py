from collections import defaultdict
import os
import sys
import threading
import grpc
import logging
from concurrent import futures

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# gRPC stub import setup
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
book_database_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/book_database")
)
sys.path.insert(0, book_database_grpc_path)

import book_database_pb2 as books_pb2
import book_database_pb2_grpc as books_pb2_grpc


class BooksDatabaseServicer(books_pb2_grpc.BooksDatabaseServicer):
    def __init__(self):
        self.store = {}
        self.prepared_updates = {}
        self.pending_decrements = defaultdict(int)
        self.lock = threading.Lock()
        logger.info("Initialized BooksDatabaseServicer")

    def Read(self, request, context):
        logger.info(f"Read request for book: {request.name}")
        stock = self.store.get(request.name, 0)
        logger.debug(f"Read stock for {request.name}: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        logger.info(
            f"Write request for book: {request.name}, new_stock: {request.new_stock}"
        )
        self.store[request.name] = request.new_stock
        logger.debug(f"Updated stock for {request.name} to {request.new_stock}")
        return books_pb2.WriteResponse(success=True)

    def Prepare(self, request, context):
        logger.info(f"Prepare request for txn {request.transaction_id}")
        with self.lock:
            current = self.store.get(request.name, 0)
            pending = self.pending_decrements.get(request.name, 0)
            available = current - pending

            if available < request.quantity:
                logger.warning(f"Prepare failed: Insufficient stock for {request.name}")
                return books_pb2.Vote(ready=False)

            self.pending_decrements[request.name] += request.quantity
            self.prepared_updates[request.transaction_id] = (
                request.name,
                request.quantity,
            )
            return books_pb2.Vote(ready=True)

    def Commit(self, request, context):
        logger.info(f"Commit request for txn {request.transaction_id}")
        with self.lock:
            if request.transaction_id in self.prepared_updates:
                name, quantity = self.prepared_updates.pop(request.transaction_id)
                self.store[name] = self.store.get(name, 0) - quantity
                self.pending_decrements[name] -= quantity
                if self.pending_decrements[name] == 0:
                    del self.pending_decrements[name]
                logger.info(f"Committed {quantity} decrement for {name}")
                return books_pb2.Ack(success=True)
            return books_pb2.Ack(success=False)

    def Abort(self, request, context):
        logger.info(f"Abort request for txn {request.transaction_id}")
        with self.lock:
            if request.transaction_id in self.prepared_updates:
                name, quantity = self.prepared_updates.pop(request.transaction_id)
                self.pending_decrements[name] -= quantity
                if self.pending_decrements[name] == 0:
                    del self.pending_decrements[name]
                logger.info(f"Aborted {quantity} decrement for {name}")
                return books_pb2.Ack(success=True)
            return books_pb2.Ack(success=False)


class PrimaryReplica(BooksDatabaseServicer):
    def __init__(self, backup_addresses):
        super().__init__()
        self.backups = [self._create_backup_stub(addr) for addr in backup_addresses]
        self.repl_lock = threading.Lock()
        logger.info(
            f"Initialized PrimaryReplica with {len(backup_addresses)} backups: {backup_addresses}"
        )

    def _create_backup_stub(self, address):
        logger.info(f"Creating backup stub for address: {address}")
        channel = grpc.insecure_channel(address)
        return books_pb2_grpc.BooksDatabaseStub(channel)

    def _replicate_write(self, request):
        logger.info(
            f"Replicating write for book: {request.name}, new_stock: {request.new_stock}"
        )
        success_count = 1
        for i, backup in enumerate(self.backups):
            try:
                backup.Write(request)
                logger.debug(f"Successfully replicated to backup {i}")
                success_count += 1
            except grpc.RpcError as e:
                logger.error(f"Replication failed to backup {i}: {e}")
        required_success = (len(self.backups) + 1) // 2 + 1
        logger.info(
            f"Replication complete: {success_count}/{required_success} successful"
        )
        return success_count >= required_success

    def _replicate_2pc(self, request, operation):
        success_count = 1
        for backup in self.backups:
            try:
                if operation == "prepare":
                    backup.Prepare(request)
                elif operation == "commit":
                    backup.Commit(request)
                elif operation == "abort":
                    backup.Abort(request)
                success_count += 1
            except grpc.RpcError as e:
                logger.error(f"Replication failed: {e}")
        required_success = (len(self.backups) + 1) // 2 + 1
        logger.info(
            f"Replication complete: {success_count}/{required_success} successful"
        )
        return success_count >= required_success

    def Prepare(self, request, context):
        logger.info(f"Primary preparing txn {request.transaction_id}")
        result = super().Prepare(request, context)
        if result.ready:
            if not self._replicate_2pc(request, "prepare"):
                logger.error("Prepare replication failed")
                return books_pb2.Vote(ready=False)
        return result

    def Commit(self, request, context):
        logger.info(f"Primary committing txn {request.transaction_id}")
        result = super().Commit(request, context)
        if result.success:
            if not self._replicate_2pc(request, "commit"):
                logger.error("Commit replication failed")
                return books_pb2.Ack(success=False)
        return result

    def Abort(self, request, context):
        logger.info(f"Primary aborting txn {request.transaction_id}")
        result = super().Abort(request, context)
        if result.success:
            if not self._replicate_2pc(request, "abort"):
                logger.error("Abort replication failed")
                return books_pb2.Ack(success=False)
        return result

    def Write(self, request, context):
        logger.info(
            f"Processing Write for book: {request.name}, new_stock: {request.new_stock}"
        )
        self.store[request.name] = request.new_stock
        logger.debug(f"Local write completed for {request.name}")
        success = self._replicate_write(request)
        logger.info(f"Write operation {'succeeded' if success else 'failed'}")
        return books_pb2.WriteResponse(success=success)
    
    def Read(self, request, context):
        logger.info(f"Read request for book: {request.name}")
        stock = self.store.get(request.name, 0)
        logger.debug(f"Read stock for {request.name}: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def DecrementStock(self, request, context):
        logger.info(
            f"DecrementStock request for book: {request.name}, quantity: {request.quantity}"
        )
        with self.lock:
            current = self.store.get(request.name, 0)
            if current < request.quantity:
                logger.warning(
                    f"Insufficient stock for {request.name}: current={current}, requested={request.quantity}"
                )
                return books_pb2.WriteResponse(success=False)
            new_stock = current - request.quantity
            logger.debug(f"Decremented stock for {request.name} to {new_stock}")

        write_request = books_pb2.WriteRequest(name=request.name, new_stock=new_stock)
        return self.Write(write_request, context)

    def IncrementStock(self, request, context):
        logger.info(
            f"IncrementStock request for book: {request.name}, quantity: {request.quantity}"
        )
        with self.lock:
            current = self.store.get(request.name, 0)
            new_stock = current + request.quantity
            logger.debug(f"Incremented stock for {request.name} to {new_stock}")

        write_request = books_pb2.WriteRequest(name=request.name, new_stock=new_stock)
        return self.Write(write_request, context)

    def CompareAndSwap(self, request, context):
        logger.info(
            f"CompareAndSwap request for book: {request.name}, expected: {request.expected_value}, new: {request.new_value}"
        )
        with self.lock:
            current = self.store.get(request.name, 0)
            if current != request.expected_value:
                logger.warning(
                    f"CompareAndSwap failed for {request.name}: current={current}, expected={request.expected_value}"
                )
                return books_pb2.WriteResponse(success=False)
            logger.debug(
                f"CompareAndSwap updated {request.name} to {request.new_value}"
            )

        write_request = books_pb2.WriteRequest(
            name=request.name, new_stock=request.new_value
        )
        return self.Write(write_request, context)


def serve():
    is_primary = os.getenv("IS_PRIMARY", "false").lower() == "true"
    backup_addresses = (
        os.getenv("BACKUP_ADDRESSES", "").split(",") if is_primary else []
    )
    logger.info(
        f"Starting server (is_primary={is_primary}) with backup addresses: {backup_addresses}"
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_pb2_grpc.add_BooksDatabaseServicer_to_server(
        PrimaryReplica(backup_addresses) if is_primary else BooksDatabaseServicer(),
        server,
    )
    port = os.getenv("PORT", "50055")
    server.add_insecure_port(f"[::]:{port}")
    logger.info(f"Server binding to port {port}")
    server.start()
    logger.info(f"Server started successfully on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
