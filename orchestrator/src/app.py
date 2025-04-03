import queue
import threading
import uuid
from services import OrchestratorService

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


@app.route("/checkout", methods=["POST"])
def checkout():
    service = OrchestratorService()

    try:
        print(f"Received new checkout request\n")

        request_data = request.get_json()

        order_data = json.dumps(request_data)
        order_id = str(uuid.uuid4())

        results = queue.Queue()

        print(f"Caching order data\n")
        threads = [
            threading.Thread(target=service.fraud_init, args=(order_id, order_data)),
            threading.Thread(
                target=service.transaction_init, args=(order_id, order_data)
            ),
            threading.Thread(
                target=service.suggestion_init, args=(order_id, order_data)
            ),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        print(f"Order caching completed\n")

        print("Verifying order data\n")
        threads = [
            threading.Thread(target=service.verify_user, args=(order_id,)),
            threading.Thread(target=service.verify_credit_card, args=(order_id,)),
            threading.Thread(target=service.verify_address, args=(order_id,)),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        print("Verfying complete\n")
        print(f"Current vector clock: {service.vc}\n")

        print("Checking order data\n")
        threads = [
            threading.Thread(target=service.check_user, args=(order_id,)),
            threading.Thread(target=service.check_credit_card, args=(order_id,)),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        print("Checking complete\n")
        print(f"Current vector clock: {service.vc}\n")

        print("Getting Book Suggestions\n")
        suggestions = service.get_suggestions(order_id)
        print("Getting Book Suggestion Complete\n")
        print(f"Current vector clock: {service.vc}\n")

        print(f"Sending checkout response to user\n")

        return {
            "orderId": "12345",
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
        return {"orderId": order_id, "status": "Order Rejected", "error": str(e)}


if __name__ == "__main__":
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host="0.0.0.0")
