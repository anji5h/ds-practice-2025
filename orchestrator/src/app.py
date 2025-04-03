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
        print(f"Received new checkout request")

        request_data = request.get_json()

        order_data = json.dumps(request_data)
        order_id = str(uuid.uuid4())

        results = queue.Queue()

        print(f"Caching order data")
        print(f"Creating worker threads")
        threads = [
            threading.Thread(
                target=service.fraud_init,
                args=(order_id, order_data),
            ),
            threading.Thread(
                target=service.transaction_init,
                args=(order_id, order_data),
            ),
            threading.Thread(
                target=service.suggestion_init,
                args=(order_id, order_data),
            ),
        ]

        print(f"Starting worker threads")
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        print(f"Threads processing completed")

        results = {}
        while not results.empty():
            key, value = results.get()
            results[key] = value
        print(f"Final results: {results}")

        status = "Order Approved"
        if results.get("is_fraud", False):
            status = "Order Rejected (Fraud detected)"
        elif not results.get("is_verified", False):
            status = "Order Rejected (Transaction verification failed)"

        print(f"Sending checkout response to user")

        return {
            "orderId": "12345",
            "status": status,
            "suggestedBooks": [
                {
                    "bookId": str(i + 1),
                    "title": book["title"],
                    "author": book["author"],
                    "link": book["link"],
                }
                for i, book in enumerate(results.get("suggestions", []))
            ],
        }

    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON received")
        return jsonify({"error": "Invalid JSON"}), 400
    except KeyError as e:
        print(f"ERROR: Missing field {str(e)}")
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host="0.0.0.0")
