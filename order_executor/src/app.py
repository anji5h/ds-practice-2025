import json
import socket
import time
import random
import uuid
import redis
import grpc
import logging
from typing import Optional
from dataclasses import dataclass
from database import DatabaseClient
from payment import PaymentClient
from order_queue import OrderQueueClient

# Configure standard logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Configuration for the ExecutorService."""

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    GRPC_CHANNEL: str = "order_queue:50054"
    HEARTBEAT_TIMEOUT: int = 10
    ELECTION_LOCK_TTL: int = 5
    CRASH_PROBABILITY: float = 0.01
    POLL_INTERVAL: int = 3
    BOOK_PRICE: float = 110.10


INITIAL_STOCKS = {
    "harry_potter": 10,
    "lord_of_the_ring": 10,
}


class ExecutorService:
    """Service for processing book orders with leader election."""

    def __init__(self, executor_id: str):
        self.config = Config()
        self.executor_id = executor_id
        self.running = True
        self._setup_clients()
        self._setup_keys()
        self._register_executor()
        logger.info(f"Executor initialized: {executor_id}")

    def _setup_clients(self):
        """Initialize Redis and gRPC clients."""
        self.redis = redis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            decode_responses=True,
            retry_on_timeout=True,
        )

        self.db_client = DatabaseClient()
        self.queue_client = OrderQueueClient()
        self.payment_client = PaymentClient()
        self.transaction_log = {}

    def _setup_keys(self):
        """Define Redis key names."""
        self.executor_key = "executor_ids"
        self.heartbeat_key = "leader:heartbeat"
        self.leader_key = "leader_id"
        self.election_key = "election"

    def _register_executor(self):
        """Register executor in Redis."""
        try:
            self.redis.sadd(self.executor_key, self.executor_id)
            logger.info(f"Executor registered: {self.executor_id}")
        except redis.RedisError as e:
            logger.error(f"Failed to register executor {self.executor_id}: {e}")
            raise

    def _unregister_executor(self):
        """Unregister executor from Redis."""
        try:
            self.redis.srem(self.executor_key, self.executor_id)
            logger.info(f"Executor unregistered: {self.executor_id}")
        except redis.RedisError as e:
            logger.error(f"Failed to unregister executor {self.executor_id}: {e}")

    def get_leader(self) -> Optional[str]:
        """Get current leader ID."""
        try:
            leader_id = self.redis.get(self.leader_key)
            logger.debug(f"Retrieved leader: {leader_id}")
            return leader_id
        except redis.RedisError as e:
            logger.error(f"Failed to get leader: {e}")
            return None

    def _set_leader(self, leader_id: str):
        """Set leader ID in Redis."""
        try:
            self.redis.set(self.leader_key, leader_id)
            logger.info(f"Leader set: {leader_id}")
        except redis.RedisError as e:
            logger.error(f"Failed to set leader {leader_id}: {e}")
            raise

    def _update_heartbeat(self):
        """Update leader heartbeat."""
        try:
            self.redis.set(self.heartbeat_key, time.time())
            logger.debug(f"Heartbeat updated: {self.executor_id}")
        except redis.RedisError as e:
            logger.error(f"Failed to update heartbeat: {e}")

    def _get_heartbeat(self):
        """Get last heartbeat timestamp."""
        try:
            heartbeat = self.redis.get(self.heartbeat_key)
            return float(heartbeat) if heartbeat else None
        except redis.RedisError as e:
            logger.error(f"Failed to get heartbeat: {e}")
            return None

    def _try_acquire_election_lock(self) -> bool:
        """Attempt to acquire election lock."""
        try:
            acquired = bool(
                self.redis.set(
                    self.election_key,
                    "locked",
                    nx=True,
                    ex=self.config.ELECTION_LOCK_TTL,
                )
            )
            logger.info(
                f"Election lock attempt: acquired={acquired}, executor_id={self.executor_id}"
            )
            return acquired
        except redis.RedisError as e:
            logger.error(f"Failed to acquire election lock: {e}")
            return False

    def initialize_stocks(self):
        """Initialize book stocks in the database."""
        for name, stock in INITIAL_STOCKS.items():
            success = self.db_client.increment_stock(name=name, quantity=stock)
            logger.info(
                f"Stock initialization: book_name={name}, stock={stock}, success={success}"
            )

    def execute_order(self, order_id: str, order_data: str):
        """Process order using 2PC protocol"""
        try:
            data = json.loads(order_data)

            for book in data["items"]:
                # Phase 1: Prepare
                ready_vote = []
                transaction_id = f"txn_{uuid.uuid4()}"

                db_ready = self.db_client.prepare_update(
                    transaction_id=transaction_id,
                    name=book["name"].lower().replace(" ", "_"),
                    quantity=book["quantity"],
                )

                ready_vote.append(db_ready)

                payment_ready = self.payment_client.prepare_update(
                    transaction_id=transaction_id
                )

                ready_vote.append(payment_ready)

                if all(ready_vote):
                    # Phase 2: Commit
                    self.db_client.commit_update(transaction_id)
                    self.payment_client.commit_update(transaction_id)
                    logger.info(f"Transaction {transaction_id} committed")
                else:
                    # Abort if any service failed
                    logger.warning(f"Aborting transaction {transaction_id}")
                    self.db_client.abort_update(transaction_id)
                    self.payment_client.abort_update(transaction_id)
                    break
        except json.JSONDecodeError as e:
            logger.error(f"Invalid order data: {order_id}, error={e}")
        except Exception as e:
            logger.error(f"Transaction failed: {transaction_id}, error={e}")
            self._handle_transaction_failure(transaction_id)

    def _handle_transaction_failure(self, transaction_id):
        """Basic failure recovery mechanism"""
        logger.warning(f"Attempting recovery for {transaction_id}")
        self.db_client.abort_update(transaction_id)
        self.payment_client.abort_update(transaction_id)

    def _process_order(self):
        """Dequeue and process an order if available."""
        try:
            response = self.queue_client.process_order()
            if response.available:
                logger.info(
                    f"Processing order: order_id={response.order_id}, executor_id={self.executor_id}"
                )
                self.execute_order(response.order_id, response.order_data)
            else:
                logger.debug(f"No orders available: executor_id={self.executor_id}")
        except grpc.RpcError as e:
            logger.error(
                f"gRPC error while dequeuing order: error={e.details()}, executor_id={self.executor_id}"
            )

    def elect_leader(self) -> str:
        """Elect a new leader from available executors."""
        try:
            executors = sorted(self.redis.smembers(self.executor_key), reverse=True)
            if not executors:
                logger.warning("No executors available for election")
                time.sleep(10)
                return self.elect_leader()
            leader_id = executors[0]
            logger.info(f"Leader elected: {leader_id}")
            return leader_id
        except redis.RedisError as e:
            logger.error(f"Failed to elect leader: {e}")
            time.sleep(10)
            return self.elect_leader()

    def _should_crash(self) -> bool:
        """Simulate a crash based on probability."""
        if random.random() < self.config.CRASH_PROBABILITY:
            logger.error(f"Executor crashed: executor_id={self.executor_id}")
            self._unregister_executor()
            self.running = False
            return True
        return False

    def start_election(self):
        """Start a new leader election."""
        leader_id = self.elect_leader()
        self._set_leader(leader_id)
        self._update_heartbeat()
        logger.info(
            f"Election completed: new_leader={leader_id}, executor_id={self.executor_id}"
        )

    def run(self):
        """Main executor loop."""
        logger.info(f"Starting executor loop: executor_id={self.executor_id}")
        while True:
            while self.running:
                if self._should_crash():
                    break

                leader_id = self.get_leader()
                if leader_id == self.executor_id:
                    self._update_heartbeat()
                    self._process_order()
                else:
                    self._monitor_leader(leader_id)

                time.sleep(self.config.POLL_INTERVAL)

            # Recovery after crash
            if not self.running:
                logger.info(f"Recovering from crash: executor_id={self.executor_id}")
                time.sleep(30)
                self._register_executor()
                self.running = True

    def _monitor_leader(self, leader_id: str):
        """Monitor the current leader's heartbeat."""
        heartbeat = self._get_heartbeat()
        if not heartbeat or (time.time() - heartbeat) >= self.config.HEARTBEAT_TIMEOUT:
            if self._try_acquire_election_lock():
                logger.warning(
                    f"Leader unresponsive, starting election: leader_id={leader_id}, "
                    f"executor_id={self.executor_id}"
                )
                self.start_election()
            else:
                logger.info(f"Election in progress: executor_id={self.executor_id}")
        else:
            logger.debug(
                f"Leader active: leader_id={leader_id}, executor_id={self.executor_id}"
            )


def launch_executor():
    """Launch the executor service."""
    executor_id = socket.gethostname()
    try:
        executor = ExecutorService(executor_id=executor_id)
        executor.initialize_stocks()
        executor.start_election()
        executor.run()
    except Exception as e:
        logger.error(f"Executor failed: executor_id={executor_id}, error={e}")
        raise


if __name__ == "__main__":
    launch_executor()
