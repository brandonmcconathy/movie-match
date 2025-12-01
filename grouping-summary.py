import json
import os
import sys
import zmq


# =========================
# Core logic
# =========================

def put_in_groups(request):
    response = {}
    for item in request:
        categories = item["category"]
        for category in categories:
            if category not in response:
                response[category] = [item["id"]]
            else:
                response[category].append(item["id"])
    return response

# =========================
# Request / response helpers
# =========================

def make_error_response(message):
    return {
        "status": "error",
        "error": message
    }


def serialize_response(response_dict):
    """
    Deterministic JSON encoding:
      - sort_keys=True → stable key order
      - separators=(',', ':') → no extra spaces
    """
    return json.dumps(
        response_dict,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")


def parse_request_bytes(raw_bytes):
    """
    Decode raw bytes into a Python dict, or return an error response.
    """
    try:
        request = json.loads(raw_bytes.decode("utf-8"))
        return request, None
    except json.JSONDecodeError:
        return None, make_error_response("Invalid JSON in request body.")

def handle_message(raw_bytes):
    request, parse_error = parse_request_bytes(raw_bytes)
    if parse_error is not None:
        return serialize_response(parse_error)

    response = put_in_groups(request)

    return serialize_response(response)

# =========================
# ZeroMQ server
# =========================

def create_socket(port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")
    return context, socket


def run_server(port="5564"):
    """
    Main server loop.
    """
    context, socket = create_socket(port)
    print(f"[category-summary] Listening on port {port}...", file=sys.stderr)

    try:
        while True:
            raw_request = socket.recv()
            print("Received Request")
            try:
                response_bytes = handle_message(raw_request)
            except Exception as e:
                error_response = make_error_response(f"Internal error: {str(e)}")
                response_bytes = serialize_response(error_response)

            print("Sending Response")
            socket.send(response_bytes)
    except KeyboardInterrupt:
        print("\n[category-summary] Interrupted via keyboard.", file=sys.stderr)
    finally:
        socket.close()
        context.term()


def main():
    port = os.getenv("CHANGE_DIFF_PORT", "5564")
    run_server(port)


if __name__ == "__main__":
    main()