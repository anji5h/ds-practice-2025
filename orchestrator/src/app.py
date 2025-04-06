import uuid
from services import OrchestratorService
from concurrent.futures import ThreadPoolExecutor

# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r"/*": {"origins": "*"}})


def execute_parallel(tasks):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(task[0], *task[1]) for task in tasks]
        for future in futures:
            future.result()


@app.route("/checkout", methods=["POST"])
def checkout():
    service = OrchestratorService()

    try:
        print(f"Received new checkout request\n")

        request_data = request.get_json()

        order_data = json.dumps(request_data)
        order_id = str(uuid.uuid4())

        print(f"Caching order data\n")
        init_tasks = [
            (service.fraud_init, (order_id, order_data)),
            (service.transaction_init, (order_id, order_data)),
            (service.suggestion_init, (order_id, order_data)),
        ]
        execute_parallel(init_tasks)
        print(f"Order caching completed\n")

        print("Verifying order data\n")
        verify_tasks = [
            (service.verify_user, (order_id,)),
            (service.verify_credit_card, (order_id,)),
            (service.verify_address, (order_id,)),
        ]
        execute_parallel(verify_tasks)
        print("Verfying complete\n")
        print(f"Current vector clock: {service.vc}\n")

        print("Checking order data\n")
        fraud_tasks = [
            (service.check_credit_card, (order_id,)),
            (service.check_user, (order_id,)),
        ]
        execute_parallel(fraud_tasks)
        print("Checking complete\n")
        print(f"Current vector clock: {service.vc}\n")

        print("Getting Book Suggestions\n")
        suggestions = service.get_suggestions(order_id)
        print("Getting Book Suggestion Complete\n")
        print(f"Current vector clock: {service.vc}\n")

        service.enqueue_order(order_id, order_data)

        print(f"Sending checkout response to user\n")

        return {
            "orderId": order_id,
            "status": "Order Approved",
            "suggestedBooks": [
                {
                    "bookId": str(i + 1),
                    "title": book["title"],
                    "author": book["author"],
                    "link": book["link"],
                }
                for i, book in enumerate(suggestions)
            ],
        }

    except Exception as e:
        print(f"Order Rejected: {e}")
        print(f"Current vector clock: {service.vc}\n")

        return {
            "orderId": order_id,
            "status": f"Order Rejected ({e})",
            "suggestedBooks": [],
        }

    finally:
        print("Cleaning order data")
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
