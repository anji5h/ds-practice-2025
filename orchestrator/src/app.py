import uuid
from services import OrchestratorService
from concurrent.futures import ThreadPoolExecutor
import logging
from flask import Flask, request
from flask_cors import CORS
import json
import time
import psutil
from opentelemetry import trace, metrics
from opentelemetry.metrics import Observation
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Set up tracing
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
span_processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint="http://observability:4318/v1/traces")
)
trace.get_tracer_provider().add_span_processor(span_processor)

# Set up metrics
metrics.set_meter_provider(
    MeterProvider(
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint="http://observability:4318/v1/metrics")
            )
        ]
    )
)
meter = metrics.get_meter(__name__)
request_counter = meter.create_counter(
    name="checkout_requests_total",
    description="Total number of checkout requests",
    unit="1",
)
active_requests = meter.create_up_down_counter(
    name="active_checkout_requests",
    description="Number of active checkout requests",
    unit="1",
)
request_latency = meter.create_histogram(
    name="checkout_request_latency",
    description="Latency of checkout requests in milliseconds",
    unit="ms",
)

def memory_usage_callback(options):
    mem = psutil.virtual_memory()
    used_mb = mem.used / (1024 * 1024)
    return [Observation(used_mb, {"resource": "memory"})]

memory_usage_gauge = meter.create_observable_gauge(
    name="memory_usage_mb",
    description="Used system memory in MB",
    unit="MB",
    callbacks=[memory_usage_callback],
)

# Create Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Instrument Flask app
FlaskInstrumentor().instrument_app(app)


def execute_parallel(tasks, task_type):
    with tracer.start_as_current_span(f"execute-{task_type}-tasks") as span:
        span.set_attribute("task_type", task_type)
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(task[0], *task[1]) for task in tasks]
            for i, future in enumerate(futures):
                with tracer.start_as_current_span(f"{task_type}-task-{i}") as subspan:
                    subspan.set_attribute("task_index", i)
                    future.result()


@app.route("/checkout", methods=["POST"])
def checkout():
    service = OrchestratorService()
    active_requests.add(1)
    start_time = time.time()

    with tracer.start_as_current_span("checkout-request") as span:
        span.set_attribute("endpoint", "/checkout")
        try:
            logger.info("Received new checkout request")
            request_counter.add(1, attributes={"endpoint": "/checkout"})

            request_data = request.get_json()
            order_data = json.dumps(request_data)
            order_id = str(uuid.uuid4())
            span.set_attribute("order_id", order_id)

            logger.info("Caching order data")
            with tracer.start_as_current_span("init-tasks"):
                init_tasks = [
                    (service.fraud_init, (order_id, order_data)),
                    (service.transaction_init, (order_id, order_data)),
                    (service.suggestion_init, (order_id, order_data)),
                ]
                execute_parallel(init_tasks, "init")

            logger.info("Order caching completed")

            logger.info("------- RUNNING order Tasks --------")
            with tracer.start_as_current_span("order-tasks"):
                order_tasks = [
                    (service.verify_user, (order_id,)),
                    (service.verify_address, (order_id,)),
                    (service.verify_credit_card, (order_id,)),
                    (service.check_user, (order_id,)),
                    (service.check_credit_card, (order_id,)),
                ]
                execute_parallel(order_tasks, "order")

            logger.info("---- Order Tasks Completed ------")

            with tracer.start_as_current_span("get-suggestions"):
                suggestions = service.get_suggestions(order_id)

            logger.info(f"----- FINAL vector clock ------: {service.vc}")
            span.set_attribute("vector_clock", str(service.vc))

            with tracer.start_as_current_span("enqueue-order"):
                service.enqueue_order(order_id, order_data)

            logger.info("Sending checkout response to user")

            latency_ms = (time.time() - start_time) * 1000
            request_latency.record(latency_ms, attributes={"endpoint": "/checkout"})

            return {
                "orderId": order_id,
                "status": "Order Approved",
                "suggestedBooks": [
                    {
                        "bookId": i,
                        "title": book["title"],
                        "author": book["author"],
                        "link": book["link"],
                    }
                    for i, book in enumerate(suggestions)
                ],
            }

        except Exception as e:
            logger.error(f"Order Rejected: {e}")
            logger.info(f"Current vector clock: {service.vc}")
            span.set_attribute("error", str(e))

            latency_ms = (time.time() - start_time) * 1000
            request_latency.record(
                latency_ms, attributes={"endpoint": "/checkout", "status": "error"}
            )
            request_counter.add(
                1, attributes={"endpoint": "/checkout", "status": "error"}
            )

            return {
                "orderId": order_id,
                "status": f"Order Rejected ({e})",
                "suggestedBooks": [],
            }

        finally:
            active_requests.add(-1)
            logger.info("----- CLEANING order cache ------")
            with tracer.start_as_current_span("cleanup-tasks"):
                cleanup_tasks = [
                    (service.clean_fraud_order, (order_id,)),
                    (service.clean_transaction_order, (order_id,)),
                    (service.clean_suggestion_order, (order_id,)),
                ]
                execute_parallel(cleanup_tasks, "cleanup")


if __name__ == "__main__":
    app.run(host="0.0.0.0")
