import uuid
from services import OrchestratorService
from concurrent.futures import ThreadPoolExecutor
import logging

# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r"/*": {"origins": "*"}})


def execute_parallel(tasks):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(task[0], *task[1]) for task in tasks]
        for future in futures:
            future.result()


@app.route("/checkout", methods=["POST"])
def checkout():
    service = OrchestratorService()

    try:
        logger.info("Received new checkout request")

        request_data = request.get_json()

        order_data = json.dumps(request_data)
        order_id = str(uuid.uuid4())

        logger.info("Caching order data")

        init_tasks = [
            (service.fraud_init, (order_id, order_data)),
            (service.transaction_init, (order_id, order_data)),
            (service.suggestion_init, (order_id, order_data)),
        ]
        execute_parallel(init_tasks)

        logger.info("Order caching completed")

        logger.info("------- RUNNING order Tasks --------")
        order_tasks = [
            (service.verify_user, (order_id,)),
            (service.verify_address, (order_id,)),
            (service.verify_credit_card, (order_id,)),
            (service.check_user, (order_id,)),
            (service.check_credit_card, (order_id,)),
        ]
        execute_parallel(order_tasks)

        logger.info("---- Order Tasks Completed ------")

        suggestions = service.get_suggestions(order_id)

        logger.info(f"----- FINAL vector clock ------: {service.vc}")

        service.enqueue_order(order_id, order_data)

        logger.info("Sending checkout response to user")

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

        return {
            "orderId": order_id,
            "status": f"Order Rejected ({e})",
            "suggestedBooks": [],
        }

    finally:
        logger.info("----- CLEANING order cache ------")
        cleanup_tasks = [
            (service.clean_fraud_order, (order_id,)),
            (service.clean_transaction_order, (order_id,)),
            (service.clean_suggestion_order, (order_id,)),
        ]
        execute_parallel(cleanup_tasks)


if __name__ == "__main__":
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host="0.0.0.0")
