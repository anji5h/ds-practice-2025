import os
import socket
import sys
import time
import redis
import grpc
import random
from google.protobuf import empty_pb2

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/order_queue")
)

sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2_grpc as order_queue_grpc


class ExecutorService:
    def __init__(self, executor_id, stub):
        self.executor_id = executor_id
        self.stub = stub
        self.executor_key = "executor_ids"
        self.hearbeat_key = "leader:heartbeat"
        self.leader_key = "leader_id"
        self.election_key = "election"
        self.running = True
        self.redis = redis.Redis(host="redis", port=6379, decode_responses=True)

        self.register_executor()
        time.sleep(5)

    def register_executor(self):
        self.redis.sadd(self.executor_key, self.executor_id)
        print(f"Executor {self.executor_id}: Registered")

    def unregister_executor(self):
        self.redis.srem(self.executor_key, self.executor_id)
        print(f"Executor {self.executor_id}: UnRegistered")

    def get_leader(self):
        return self.redis.get(self.leader_key)

    def set_leader(self, leader_id):
        self.redis.set(self.leader_key, leader_id)

    def set_heartbeat(self):
        self.redis.set(self.hearbeat_key, time.time())

    def get_hearbeat(self):
        return self.redis.get(self.hearbeat_key)

    def set_election_lock(self):
        return self.redis.set(self.election_key, "locked", nx=True, ex=5)

    def poll_queue(self):
        try:
            response = self.stub.DequeueOrder(empty_pb2.Empty())

            if response.available:
                order_id = response.order_id
                print(
                    f"Executor {self.executor_id} (leader): Processing order: {order_id}"
                )
                time.sleep(3)
                print(
                    f"Executor {self.executor_id} (leader): Order: {order_id} processed"
                )
            else:
                print(f"Executor {self.executor_id} (leader): No orders to process.")
        except Exception as e:
            print(f"{self.executor_id}: gRPC Error: {e}")

    def ring_election(self):
        executors = list(self.redis.smembers(self.executor_key))

        if len(executors) <= 0:
            print("No executors found. Waiting for new excecutor.")
            time.sleep(10)
            self.ring_election()

        executors.sort(reverse=True)
        return executors[0]

    def crash_executor(self):
        if random.random() < 0.05:
            print(f"Executor {self.executor_id}: Crashed")
            self.unregister_executor()
            self.running = False
            return True
        return False

    def start_leader_election(self):
        leader_id = self.ring_election()
        self.set_leader(leader_id=leader_id)
        self.set_heartbeat()
        print(f"Leader elected: {leader_id}")

    def run(self):
        while self.running:
            if self.crash_executor():
                continue

            leader_id = self.get_leader()

            if leader_id == self.executor_id:
                self.set_heartbeat()
                self.poll_queue()
            else:
                heartbeat = float(self.get_hearbeat())

                if not heartbeat or (time.time() - heartbeat) >= 6:
                    if self.set_election_lock():
                        print(f"Leader {leader_id} unresponsive. Initiating Re-election")
                        self.start_leader_election()
                    else:
                        print("Election under process")
                else:
                    print(
                        f"Executor {self.executor_id}: Waiting to be leader, current leader: {leader_id}"
                    )

            time.sleep(3)

        if not self.running:
            time.sleep(30)
            self.register_executor()
            self.running = True
            self.run()


def launch_executor():
    with grpc.insecure_channel("order_queue:50054") as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        executor_id = socket.gethostname()
        svc = ExecutorService(executor_id=executor_id, stub=stub)
        svc.start_leader_election()
        svc.run()


if __name__ == "__main__":
    launch_executor()
