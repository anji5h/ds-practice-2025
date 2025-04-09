# System Model

In this distributed online bookshop system, we describe our system model using three dimensions: **Communication Model**, **Architecture Model**, and **Failure Model**.

---

## 1. Communication Model

### Type:
- **Asynchronous RPC** via **gRPC**
- Non-blocking communication where services handle their own logic and synchronization through vector clocks and orchestrator sequencing.

### Interactions:
- **Orchestrator → Backend Services**: Sends order data with a unique `OrderID` and triggers vector clock initialization.
- **Backend Services (indirectly coordinated)**: Events are ordered via vector clocks; no direct RPC between services.
- **Orchestrator ↔ Order Queue**: `EnqueueOrder()` gRPC call after successful validation.
- **Order Executors ↔ Order Queue**: Leader executor calls `DequeueOrder()` to get the next valid order.
- **Order Executors ↔ Each Other**: Use `Election()` and `AnnounceLeader()` RPCs for leader election.
- **Orchestrator → All Services**: Sends `BroadcastClearOrder(VCf)` at the end of processing to clear cached data.

### Message Content:
- **Order Data**: user information, book items, metadata (e.g. priority, shipping method).
- **Vector Clocks**: included in event execution and final broadcast.
- **Result Status**: success/failure per event, propagated through orchestrator.

---

## 2. Architecture Model

### Style:
- **Microservices-based distributed system**
- Stateless services managed via **Docker Compose**

### Components and Roles:

| Service/Component         | Role                                                                 |
|---------------------------|----------------------------------------------------------------------|
| **Orchestrator**          | Coordinates order validation flow, manages event ordering            |
| **Transaction Verification** | Verifies order items, user info, credit card formatting         |
| **Fraud Detection**       | Checks for potential fraud using validated data                      |
| **Book Suggestion**       | Generates suggestions based on current order                         |
| **Order Queue**           | Buffers valid orders; can be simple or priority queue                |
| **Order Executor (N ≥ 2)**| Replicated service that processes orders; only the leader executes   |

### Technologies:
- **gRPC** for all inter-service communication
- **Docker Compose** for service deployment and replication
- **Vector Clocks** for causal ordering of events
- **Leader Election Algorithm**: Ring or Bully algorithm for mutual exclusion

### System Flow:
1. Orchestrator sends order to backend services
2. Services cache data and initialize vector clocks
3. Events execute in partial order (with concurrency)
4. On success, orchestrator enqueues order
5. Order executors perform leader election
6. Leader dequeues and executes the order
7. Orchestrator broadcasts clear command with VCf

---

## 3. Failure Model

### Failure Types:

| Failure Type                  | Description                                                           |
|------------------------------|-----------------------------------------------------------------------|
| **Service Crash**            | Orchestrator detects timeout; cancels the flow                        |
| **Orchestrator Failure**     | Halts coordination; may require restart and state restoration         |
| **Executor Crash**           | Triggers leader re-election; ensures mutual exclusion persists        |
| **Vector Clock Mismatch**    | If local VC > VCf on broadcast, raise error and retain order data     |
| **Queue Service Failure**    | Blocks enqueue/dequeue; represents a critical point of failure        |
| **Network Partition**        | Affects election, message delivery, or event ordering                 |

### Fault Tolerance:

- gRPC **timeouts + retries**
- **Leader Election** for consistent executor access
- **Broadcast VCf validation** to prevent premature data deletion
- **Graceful shutdown hooks** in containers
- **Event logs** or cache for partial recovery

### Recovery Mechanisms:
- **Orchestrator crash**: Requires manual or automatic restart
- **Executor crash**: New leader is elected automatically
- **Backend service crash**: Orchestrator aborts processing and informs the user
- **Queue crash**: Retry or persist queue externally

---
