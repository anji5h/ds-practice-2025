import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestion_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestion_grpc_path)

import suggestions_pb2 as suggestion
import suggestions_pb2_grpc as suggestion_grpc

import grpc
from concurrent import futures

# Create a class to define the server functions, derived from
# suggestion_pb2_grpc.SuggestionServiceServicer
class SuggestionService(suggestion_grpc.SuggestionServiceServicer):
    def __init__(self, svc_idx=2, total_svcs=3):
        self.svc_idx = svc_idx
        self.total_svcs = total_svcs
        self.orders = {}

    def InitOrder(self, order_id, data):
        # Initialize an order with an empty vector clock for tracking
        self.orders[order_id] = {"data": data, "vc": [0] * self.total_svcs}

    def merge_and_increment(self, local_vc, incoming_vc):
        # Merge the incoming vector clock with the local one and increment the current service's slot
        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])  # Merge the clocks
        local_vc[self.svc_idx] += 1  # Increment this service's vector clock slot

    # Create an RPC function to get suggestions
    def GetSuggestions(self, request, context):
        # Assume we have a list of suggestions based on the request input (e.g., a category or query)
        comment = request.comment
        suggestions_list = self.generate_suggestions(comment)

        # Create a SuggestionsResponse object
        response = suggestion.SuggestionsResponse()
        # Add the suggestions to the response object
        response.suggestions.extend(suggestions_list)

        # Print the suggestions
        print(f"Suggestions for '{comment}': {suggestions_list}")
        
        # Return the response object
        return response
    
    def generate_suggestions(self, comment):
        # Example logic to generate suggestions based on the query
        # In a real-world case, this could query a database or an AI model
        if comment == "tech":
            return ["Learn Python", "Master Kubernetes", "Explore AI"]
        elif comment == "health":
            return ["Start yoga", "Eat more vegetables", "Get a workout routine"]
        else:
            return ["Explore more topics"]

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add SuggestionService
    suggestion_grpc.add_SuggestionServiceServicer_to_server(SuggestionService(), server)
    # Listen on port 50051
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
