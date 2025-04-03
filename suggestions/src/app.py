import json
import sys
import os
import grpc
import requests
from concurrent import futures

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
suggestion_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/suggestions")
)
sys.path.insert(0, suggestion_grpc_path)

# Import gRPC generated classes
import suggestions_pb2 as suggestion
import suggestions_pb2_grpc as suggestion_grpc
from google.protobuf import empty_pb2

# Third-party book API (Example: Open Library API)
BOOK_API_URL = "https://openlibrary.org/search.json"


class SuggestionService(suggestion_grpc.SuggestionServiceServicer):
    def __init__(self, svc_idx=2, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def InitOrder(self, request, context):
        data = json.loads(request.order_data)
        self.orders[request.order_id] = {"data": data, "vc": [0] * self.total_svcs}
        return empty_pb2.Empty()

    def merge_and_increment(self, local_vc, incoming_vc):
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])
        local_vc[self.svc_idx] += 1

    def clean_order(self, order_id, local_vc, incoming_vc):
        if local_vc[self.svc_idx] <= incoming_vc[self.svc_idx]:
            self.orders.pop(order_id)
            return True
        else:
            return False

    def GetSuggestions(self, request, context):
        print(f"Received order_id {request.order_id}")

        order_data = self.orders.get(request.order_id)
        self.merge_and_increment(order_data["vc"], request.vc)

        response = suggestion.SuggestionsResponse()

        if not order_data:
            response.suggestedBooks = []
            response.vc.extend(order_data["vc"])
            return response

        query = ";".join([item["name"] for item in order_data["data"]["items"]])
        books = self.fetch_books(query)

        response.suggestedBooks.extend(books)
        response.vc.extend(order_data["vc"])

        print(f"Returning {len(books)} suggestions for query '{query}'")
        return response

    def fetch_books(self, query):
        try:
            print(f"Fetching books from API for query: {query}")
            response = requests.get(BOOK_API_URL, params={"q": query, "limit": 5})
            response.raise_for_status()
            data = response.json()

            books = []
            for doc in data.get("docs", [])[:5]:
                book = suggestion.Book(
                    title=doc.get("title", "Unknown"),
                    author=doc["author_name"][0] if "author_name" in doc else "Unknown",
                    description="N/A",
                    link=f"https://openlibrary.org{doc.get('key', '')}",
                )
                books.append(book)

            print(f"Fetched {len(books)} books for query '{query}'")
            return books
        except Exception as e:
            print(f"Error fetching books: {e}")
            return []

    def CleanOrder(self, request, context):
        print(f"cleaning order {request.order_id}")
        order_data = self.orders.get(request.order_id, None)

        response = suggestion.CleanOrderResponse()

        if not order_data:
            response.result = "fail"
            response.vc.extend(request.vc)
            return response

        response.result = (
            "fail"
            if not self.clean_order(request.order_id, order_data["vc"], request.vc)
            else "pass"
        )
        response.vc.extend(order_data["vc"])
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    suggestion_grpc.add_SuggestionServiceServicer_to_server(SuggestionService(), server)

    port = "50053"
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"Server started. Listening on port {port}.")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
