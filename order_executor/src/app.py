import os
import socket
import sys
import time
import redis
import grpc
import random
import logging
from google.protobuf import empty_pb2
from typing import Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# gRPC stub import setup
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2_grpc as order_queue_grpc

@dataclass
class Config:
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    GRPC_CHANNEL: str = "order_queue:50054"
    HEARTBEAT_TIMEOUT: int = 10
    ELECTION_LOCK_TTL: int = 5
    CRASH_PROBABILITY: float = 0.05
    POLL_INTERVAL: int = 3

class ExecutorService:
    def __init__(self, executor_id: str, stub: order_queue_grpc.OrderQueueServiceStub):
        self.executor_id = executor_id
        self.stub = stub
        self.config = Config()
        self.redis = redis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            decode_responses=True
        )
        self.running = True
        self._setup_keys()
        self._register_executor()

    def _setup_keys(self) -> None:
        self.executor_key = "executor_ids"
        self.heartbeat_key = "leader:heartbeat"
        self.leader_key = "leader_id"
        self.election_key = "election"

    def _register_executor(self) -> None:
        self.redis.sadd(self.executor_key, self.executor_id)
        logger.info(f"Executor {self.executor_id}: Registered")

    def _unregister_executor(self) -> None:
        self.redis.srem(self.executor_key, self.executor_id)
        logger.info(f"Executor {self.executor_id}: Unregistered")

    def get_leader(self) -> Optional[str]:
        return self.redis.get(self.leader_key)

    def _set_leader(self, leader_id: str) -> None:
        self.redis.set(self.leader_key, leader_id)

    def _update_heartbeat(self) -> None:
        self.redis.set(self.heartbeat_key, time.time())

    def _get_heartbeat(self) -> Optional[float]:
        heartbeat = self.redis.get(self.heartbeat_key)
        return float(heartbeat) if heartbeat else None

    def _try_acquire_election_lock(self) -> bool:
        return bool(self.redis.set(
            self.election_key,
            "locked",
            nx=True,
            ex=self.config.ELECTION_LOCK_TTL
        ))

    def _process_order(self) -> None:
        try:
            response = self.stub.DequeueOrder(empty_pb2.Empty())
            if response.available:
                order_id = response.order_id
                logger.info(f"Executor {self.executor_id} (leader): Processing order: {order_id}")
                time.sleep(self.config.POLL_INTERVAL)
                logger.info(f"Executor {self.executor_id} (leader): Order: {order_id} processed")
            else:
                logger.info(f"Executor {self.executor_id} (leader): No orders to process")
        except grpc.RpcError as e:
            logger.error(f"Executor {self.executor_id}: gRPC Error: {e.details()}")

    def elect_leader(self) -> str:
        executors = sorted(self.redis.smembers(self.executor_key), reverse=True)
        if not executors:
            logger.warning("No executors available, retrying...")
            time.sleep(10)
            return self.elect_leader()
        return executors[0]

    def _should_crash(self) -> bool:
        if random.random() < self.config.CRASH_PROBABILITY:
            logger.error(f"Executor {self.executor_id}: Crashed")
            self._unregister_executor()
            self.running = False
            return True
        return False

    def start_election(self) -> None:
        leader_id = self.elect_leader()
        self._set_leader(leader_id)
        self._update_heartbeat()
        logger.info(f"Leader elected: {leader_id}")

    def run(self) -> None:
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
                logger.info("Executor recovering from crash...")
                time.sleep(30)
                self._register_executor()
                self.running = True

    def _monitor_leader(self, leader_id: str) -> None:
        heartbeat = self._get_heartbeat()
        if not heartbeat or (time.time() - heartbeat) >= self.config.HEARTBEAT_TIMEOUT:
            if self._try_acquire_election_lock():
                logger.warning(f"Leader {leader_id} unresponsive. Initiating election")
                self.start_election()
            else:
                logger.info("Election already in progress")
        else:
            logger.info(f"Executor {self.executor_id}: Current leader: {leader_id}")

def launch_executor() -> None:
    with grpc.insecure_channel(Config.GRPC_CHANNEL) as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        executor_id = socket.gethostname()
        executor = ExecutorService(executor_id=executor_id, stub=stub)
        executor.start_election()
        executor.run()

if __name__ == "__main__":
    launch_executor()