from locust import HttpUser, task, between, tag
import random
import json


class OrderUser(HttpUser):
    wait_time = between(0.5, 2)
    host = "http://localhost:8081"

    @tag("single_order")
    @task(1)
    def single_non_fraudulent_order(self):
        """Test a single non-fraudulent order."""
        payload = {
            "user": {"name": "John Doe", "contact": "john.doe@gmail.com"},
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/25",
                "cvv": "123",
            },
            "userComment": "Please handle with care.",
            "items": [{"name": "Harry Potter", "quantity": 1}],
            "billingAddress": {
                "street": "123 Main St",
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "USA",
            },
            "shippingMethod": "Standard",
            "giftWrapping": True,
            "termsAccepted": True,
        }
        with self.client.post(
            "/checkout", json=payload, name="single_order", catch_response=True
        ) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data["status"] == "Order Approved":
                        response.success()
                    else:
                        response.failure(f"Order status not success: {data['status']}")
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Expected 200, got {response.status_code}")

    @tag("multiple_non_conflicting")
    @task(3)
    def multiple_non_conflicting_orders(self):
        """Test multiple non-conflicting orders (different books)."""
        books = ["Harry Potter", "Lord of the Ring", "The Hobbit"]
        book_name = random.choice(books)
        user_id = random.randint(1000, 9999)
        payload = {
            "user": {"name": f"user_{user_id}", "contact": f"user_{user_id}@gmail.com"},
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/25",
                "cvv": "123",
            },
            "userComment": "Test order",
            "items": [{"name": book_name, "quantity": 1}],
            "billingAddress": {
                "street": "123 Main St",
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "USA",
            },
            "shippingMethod": "Standard",
            "giftWrapping": False,
            "termsAccepted": True,
        }
        with self.client.post(
            "/checkout",
            json=payload,
            name="multiple_non_conflicting",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data["status"] == "Order Approved":
                        response.success()
                    else:
                        response.failure(f"Order status not success: {data['status']}")
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Expected 200, got {response.status_code}")

    @tag("mixed_orders")
    @task(2)
    def mixed_fraudulent_non_fraudulent_orders(self):
        """Test a mix of fraudulent and non-fraudulent orders."""
        is_fraudulent = random.choice([True, False])
        user_id = random.randint(1000, 9999)
        user = (
            {"name": "fraud_user", "contact": "fraud@example.com"}
            if is_fraudulent
            else {"name": f"user_{user_id}", "contact": f"user_{user_id}@gmail.com"}
        )
        payload = {
            "user": user,
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/25",
                "cvv": "123",
            },
            "userComment": "Test order",
            "items": [{"name": "Harry Potter", "quantity": 1}],
            "billingAddress": {
                "street": "123 Main St",
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "USA",
            },
            "shippingMethod": "Standard",
            "giftWrapping": False,
            "termsAccepted": True,
        }
        with self.client.post(
            "/checkout", json=payload, name="mixed_orders", catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Expected 200, got {response.status_code}")

    @tag("conflicting_orders")
    @task(1)
    def conflicting_orders(self):
        """Test conflicting orders (same book, high quantity)."""
        user_id = random.randint(1000, 9999)
        payload = {
            "user": {
                "name": f"User {user_id}",
                "contact": f"user{user_id}@gmail.com",
            },
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/25",
                "cvv": "123",
            },
            "userComment": "Test conflicting order",
            "items": [{"name": "Harry Potter", "quantity": 10}],
            "billingAddress": {
                "street": "123 Main St",
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "USA",
            },
            "shippingMethod": "Standard",
            "giftWrapping": False,
            "termsAccepted": True,
        }
        with self.client.post(
            "/checkout", json=payload, name="conflicting_orders", catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Expected 200, got {response.status_code}")
