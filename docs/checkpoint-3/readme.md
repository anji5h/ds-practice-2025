## Seminar 10: Consistency and the Database Module

#### 1. Books Database gRPC Service (`book_database.py`)
- **Purpose**: Implements a distributed key-value database storing book titles (keys) and stock quantities (values), with replication across at least three instances.
- **Key Classes**:
  - **`BooksDatabaseServicer`**:
    - **Role**: Base class for database operations, managing a local in-memory store (`self.store`) and providing thread-safe `Read` and `Write` operations.
    - **How It Works**:
      - **`Read(self, request, context)`**: Retrieves the stock for a given book title from `self.store`. If the title isn’t found, it returns 0. The method logs the request and response for debugging.
        ```python
        def Read(self, request, context):
            logger.info(f"Read request for book: {request.name}")
            stock = self.store.get(request.name, 0)
            return books_pb2.ReadResponse(stock=stock)
        ```
      - **`Write(self, request, context)`**: Updates the stock for a book title in `self.store` with the provided `new_stock`. It returns a `WriteResponse` indicating success.
        ```python
        def Write(self, request, context):
            logger.info(f"Write request for book: {request.name}, new_stock: {request.new_stock}")
            self.store[request.name] = request.new_stock
            return books_pb2.WriteResponse(success=True)
        ```
      - **Thread-Safety**: Uses a threading lock (`self.lock`) to ensure atomicity for concurrent reads and writes, preventing race conditions.
  - **`PrimaryReplica`**:
    - **Role**: Extends `BooksDatabaseServicer` to manage replication to backup instances, implementing a primary-backup protocol with quorum-based consistency.
    - **How It Works**:
      - **`__init__(self, backup_addresses)`**: Initializes the primary with gRPC stubs for backup replicas, created using `backup_addresses`. It also initializes a replication lock (`repl_lock`).
        ```python
        def __init__(self, backup_addresses):
            super().__init__()
            self.backups = [self._create_backup_stub(addr) for addr in backup_addresses]
            self.repl_lock = threading.Lock()
        ```
      - **`_replicate_write(self, request)`**: Propagates `Write` operations to backups, requiring success from a majority of replicas (`(len(self.backups) + 1) // 2 + 1`). It logs successes and failures, ensuring fault-tolerance.
        ```python
        def _replicate_write(self, request):
            success_count = 1  # Count primary as success
            for i, backup in enumerate(self.backups):
                try:
                    backup.Write(request)
                    success_count += 1
                except grpc.RpcError as e:
                    logger.error(f"Replication failed to backup {i}: {e}")
            required_success = (len(self.backups) + 1) // 2 + 1
            return success_count >= required_success
        ```
      - **`Write(self, request, context)`**: Performs a local write and calls `_replicate_write` to propagate to backups. Returns `WriteResponse` based on replication success.
      - **Consistency**: The quorum approach ensures sequential consistency, as a majority of replicas must agree on updates. Reads are served only by the primary, simplifying consistency but potentially creating a read bottleneck.
      - **Bonus Operations**:
        - **`DecrementStock(self, request, context)`**: Atomically decrements stock by a specified quantity, checking availability first. It uses `Write` to update the store and propagate to backups.
          ```python
          def DecrementStock(self, request, context):
              with self.lock:
                  current = self.store.get(request.name, 0)
                  if current < request.quantity:
                      return books_pb2.WriteResponse(success=False)
                  new_stock = current - request.quantity
              write_request = books_pb2.WriteRequest(name=request.name, new_stock=new_stock)
              return self.Write(write_request, context)
          ```
        - **`IncrementStock(self, request, context)`**: Increments stock, also using `Write` for updates.
        - **`CompareAndSwap(self, request, context)`**: Updates stock only if the current value matches an expected value, ensuring atomic updates for concurrent operations.
        - **Concurrency Handling**: These operations use `self.lock` to prevent race conditions, addressing the bonus task of handling concurrent writes (e.g., two orders updating the same book’s stock).

- **Replication**:
  - The system has three instances (one primary, two backups), configured via `backup_addresses`. The primary handles all client requests and replicates writes to backups.
  - Quorum-based replication ensures fault-tolerance, tolerating failures of up to `floor(n/2)` replicas (where `n` is the total number of replicas).
  - The gRPC interface (`ReadRequest`, `ReadResponse`, `WriteRequest`, `WriteResponse`) abstracts the distributed nature, making replication transparent to clients.

#### 2. Order Executor (`executor_service.py`)
- **Purpose**: Processes orders by dequeuing them from a queue, validating stock availability, and updating the database using `DecrementStock`.
- **Key Method**:
  - **`execute_order(self, order_id, order_data)`**:
    - **How It Works**: Parses the order JSON data, iterates through each book, and performs:
      1. **Read Stock**: Calls `db_client.read_stock` to get the current stock for a book.
      2. **Validate**: Checks if the stock is sufficient for the requested quantity.
      3. **Update**: Calls `db_client.decrement_stock` to reduce the stock, which invokes the database’s `DecrementStock` operation.
        ```python
        def execute_order(self, order_id, order_data):
            data = json.loads(order_data)
            for book in data["items"]:
                book_name = book["name"].lower().replace(" ", "_")
                book_quantity = book["quantity"]
                current_stock = self.db_client.read_stock(book_name)
                if current_stock < book_quantity:
                    logger.warning(f"Insufficient stock: book_name={book_name}")
                    continue
                success = self.db_client.decrement_stock(book_name, quantity=book_quantity)
        ```
    - **Integration**: Interacts with the database via a `DatabaseClient` (not shown but assumed to wrap gRPC calls), ensuring stock updates are propagated to all replicas.
    - **Error Handling**: Skips books with insufficient stock and logs errors for invalid JSON or gRPC failures.

- **How It Works**:
  - The executor runs in a loop (`run` method), using leader election (via Redis) to ensure only one executor processes orders at a time.
  - When elected leader, it dequeues orders using `queue_client.process_order` and calls `execute_order` to process them.
  - The `DecrementStock` operation ensures atomic stock updates, leveraging the database’s consistency protocol to maintain data integrity across replicas.

## Seminar 11: Distributed Commitment

#### 1. Books Database gRPC Service (`book_database.py`)
- **Purpose**: Acts as a 2PC participant, supporting `Prepare`, `Commit`, and `Abort` operations to stage and apply stock updates atomically.
- **Key Methods** (in `BooksDatabaseServicer` and `PrimaryReplica`):
  - **`Prepare(self, request, context)`**:
    - **How It Works**: Reserves stock for a transaction by checking available stock (accounting for `pending_decrements`) and storing the transaction in `prepared_updates`. Returns `Vote(ready=True)` if sufficient stock is available.
      ```python
      def Prepare(self, request, context):
          with self.lock:
              current = self.store.get(request.name, 0)
              pending = self.pending_decrements.get(request.name, 0)
              available = current - pending
              if available < request.quantity:
                  return books_pb2.Vote(ready=False)
              self.pending_decrements[request.name] += request.quantity
              self.prepared_updates[request.transaction_id] = (request.name, request.quantity)
              return books_pb2.Vote(ready=True)
      ```
    - **Thread-Safety**: Uses `self.lock` to ensure atomicity, preventing concurrent transactions from over-allocating stock.
  - **`Commit(self, request, context)`**:
    - **How It Works**: Applies the reserved stock decrement from `prepared_updates`, updates `self.store`, and clears `pending_decrements`. Returns `Ack(success=True)`.
      ```python
      def Commit(self, request, context):
          with self.lock:
              if request.transaction_id in self.prepared_updates:
                  name, quantity = self.prepared_updates.pop(request.transaction_id)
                  self.store[name] = self.store.get(name, 0) - quantity
                  self.pending_decrements[name] -= quantity
                  if self.pending_decrements[name] == 0:
                      del self.pending_decrements[name]
                  return books_pb2.Ack(success=True)
              return books_pb2.Ack(success=False)
      ```
  - **`Abort(self, request, context)`**:
    - **How It Works**: Discards the reserved stock by removing the transaction from `prepared_updates` and `pending_decrements`.
  - **Replication** (in `PrimaryReplica`):
    - The primary propagates 2PC operations (`Prepare`, `Commit`, `Abort`) to backups using `_replicate_2pc`, requiring a majority acknowledgment for success.
      ```python
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
          return success_count >= required_success
      ```

- **How It Works**:
  - The database stages stock updates in the `Prepare` phase, reserving stock to prevent over-allocation.
  - In the `Commit` phase, it applies the update, ensuring the stock is decremented only after all participants agree.
  - In the `Abort` phase, it discards the reservation, maintaining consistency.
  - The primary ensures that 2PC operations are replicated, maintaining consistency across all replicas.

#### 2. Payment Service (`payment_service.py`)
- **Purpose**: A single-instance gRPC service acting as a 2PC participant, simulating payment processing with dummy logic.
- **Key Class**:
  - **`PaymentService`**:
    - **Role**: Manages a set of prepared transactions (`self.prepared_transactions`) and implements 2PC operations.
    - **How It Works**:
      - **`Prepare(self, request, context)`**: Adds the transaction ID to `prepared_transactions`, indicating readiness to commit.
        ```python
        def Prepare(self, request, context):
            self.prepared_transactions.add(request.transaction_id)
            logging.info(f"Payment prepared for {request.transaction_id}")
            return payment.Vote(ready=True)
        ```
      - **`Commit(self, request, context)`**: Removes the transaction ID from `prepared_transactions` if present, simulating payment execution.
      - **`Abort(self, request, context)`**: Discards the transaction ID, simulating payment cancellation.
    - **Dummy Logic**: Uses minimal logic as required, focusing on 2PC protocol compliance.

- **How It Works**:
  - The service prepares by marking a transaction as ready, commits by finalizing it, and aborts by discarding it.
  - It integrates with the executor’s 2PC protocol, ensuring payments are executed only after all participants commit.

#### 3. Order Executor (`executor_service.py`)
- **Purpose**: Coordinates 2PC across the database and payment services to process orders atomically, acting as the 2PC coordinator.
- **Key Method**:
  - **`execute_order_2pc(self, order_id, order_data)`**:
    - **How It Works**:
      1. **Parse Order**: Parses the JSON order data to extract book names and quantities.
      2. **Phase 1 (Prepare)**: Generates a unique transaction ID and calls `db_client.prepare_update` for each book and `payment_client.prepare_update` for the payment. Collects votes in `ready_vote`.
      3. **Phase 2 (Commit/Abort)**: If all votes are `ready=True`, calls `commit_update` on both services; otherwise, calls `abort_update`.
        ```python
        def execute_order_2pc(self, order_id, order_data):
            data = json.loads(order_data)
            transaction_id = f"txn_{uuid.uuid4()}"
            ready_vote = []
            for book in data["items"]:
                book_name = book["name"].lower().replace(" ", "_")
                book_quantity = book["quantity"]
                db_ready = self.db_client.prepare_update(transaction_id, book_name, book_quantity)
                ready_vote.append(db_ready)
            payment_ready = self.payment_client.prepare_update(transaction_id)
            ready_vote.append(payment_ready)
            if all(ready_vote):
                self.db_client.commit_update(transaction_id)
                self.payment_client.commit_update(transaction_id)
                logger.info(f"Transaction {transaction_id} committed")
            else:
                logger.warning(f"Aborting transaction {transaction_id}")
                self.db_client.abort_update(transaction_id)
                self.payment_client.abort_update(transaction_id)
        ```
    - **Error Handling**: Catches JSON parsing errors and gRPC failures, triggering `_handle_transaction_failure` to abort the transaction.
  - **`_handle_transaction_failure(self, transaction_id)`** (Bonus Task):
    - **How It Works**: Recovers from participant failures by aborting the transaction on both services, ensuring consistency.
      ```python
      def _handle_transaction_failure(self, transaction_id):
          logger.warning(f"Attempting recovery for {transaction_id}")
          self.db_client.abort_update(transaction_id)
          self.payment_client.abort_update(transaction_id)
      ```

- **How It Works**:
  - The executor runs in a loop, using leader election (via Redis) to ensure only one instance processes orders.
  - When an order is dequeued (`queue_client.process_order`), it calls `execute_order_2pc` to process it atomically.
  - In the `Prepare` phase, the database reserves stock, and the payment service marks the transaction as ready.
  - In the `Commit` phase, both services finalize their operations (stock decrement, payment execution).
  - If any participant fails to prepare, the executor aborts the transaction, discarding all changes.
  - The recovery mechanism ensures that failures (e.g., network issues) result in a consistent state by aborting the transaction.


## General Notes
- **Integration**: The system integrates leader election, order queuing, and 2PC to ensure reliable order execution in a distributed environment.
- **Error Handling**: Comprehensive logging and error handling (e.g., gRPC errors, JSON parsing) ensure robustness.
- **Improvements Needed**:
  - Call `execute_order_2pc` in `_process_order` to fully implement Seminar 11.
  - Add validation for replica count in `PrimaryReplica`.
  - Check for duplicate transaction IDs in `Prepare` methods.
  - Provide a written analysis for coordinator failure to earn bonus points.
- **Bonus Tasks**:
  - **Seminar 10**: Full points for `DecrementStock`, `IncrementStock`, `CompareAndSwap`, and concurrent write handling via locks.
  - **Seminar 11**: Full points for participant failure recovery; coordinator failure analysis missing.

This documentation clarifies the code’s functionality and alignment with the seminar requirements, aiding in understanding and evaluation.