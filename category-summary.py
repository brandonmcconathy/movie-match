import json
import os
import sys
import threading
import zmq


# =========================
# Core Summary Logic
# =========================

def summarize_categories(items):
    """
    Return a dict: category -> count of items in that category.
    """
    category_summary = {}

    for item in items:
        category = item.get("category")
        if isinstance(category, str) and category.strip():
            category_summary[category] = category_summary.get(category, 0) + 1

    return category_summary


def summarize_tags(items):
    """
    Return a dict: tag -> number of items that include that tag.
    Tags are deduplicated per item.
    """
    tag_summary = {}

    for item in items:
        tags = item.get("tags", [])
        if not isinstance(tags, list):
            continue

        unique_tags_for_item = set(
            t for t in tags if isinstance(t, str) and t.strip()
        )

        for tag in unique_tags_for_item:
            tag_summary[tag] = tag_summary.get(tag, 0) + 1

    return tag_summary


def summarize_categories_and_tags(items):
    """
    Combine both summaries for convenience.
    """
    category_summary = summarize_categories(items)
    tag_summary = summarize_tags(items)
    return category_summary, tag_summary


# =========================
# Request / Response Helpers
# =========================

def make_error_response(message):
    """
    Standard error response shape.
    """
    return {
        "status": "error",
        "error": message
    }


def make_success_response(category_summary, tag_summary):
    """
    Standard success response shape.
    """
    return {
        "status": "ok",
        "category_summary": category_summary,
        "tag_summary": tag_summary
    }


def parse_request_bytes(raw_bytes):
    """
    Decode raw bytes → Python dict, or return an error response.
    """
    try:
        request = json.loads(raw_bytes.decode("utf-8"))
        return request, None
    except json.JSONDecodeError:
        return None, make_error_response("Invalid JSON in request body.")


def validate_request(request):
    """
    Validate the top-level request structure.
    Returns (items, error_response_or_none).
    """
    if request.get("request_type") != "category_tag_summary":
        return None, make_error_response(
            "Unsupported request_type. Expected 'category_tag_summary'."
        )

    items = request.get("items")
    if not isinstance(items, list):
        return None, make_error_response("'items' field must be a list.")

    return items, None


def serialize_response(response):
    """
    Serialize response dict to deterministic JSON bytes.

    - sort_keys=True → deterministic key order
    - separators=(',', ':') → no extra spaces
    """
    return json.dumps(
        response,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")


def handle_message(raw_bytes):
    """
    High-level handler: bytes in → bytes out.
    This is easy to unit test without ZeroMQ.
    """
    # 1. Parse
    request, parse_error = parse_request_bytes(raw_bytes)
    if parse_error is not None:
        return serialize_response(parse_error)

    # 2. Validate
    items, validation_error = validate_request(request)
    if validation_error is not None:
        return serialize_response(validation_error)

    # 3. Compute summaries
    category_summary, tag_summary = summarize_categories_and_tags(items)

    # 4. Build success response
    response = make_success_response(category_summary, tag_summary)
    return serialize_response(response)


# =========================
# ZeroMQ Setup & Main Loop
# =========================

def start_quit_listener(stop_event):
    """
    Start a background stdin listener; set stop_event when the user enters 'q'.
    """
    def _wait_for_q():
        print("Press 'q' then Enter to stop the server.", file=sys.stderr)
        try:
            for line in sys.stdin:
                if line.strip().lower() == "q":
                    stop_event.set()
                    break
        except Exception:
            # stdin might be unavailable; ignore and keep running
            pass

    listener = threading.Thread(target=_wait_for_q, daemon=True)
    listener.start()
    return listener

def create_socket(port):
    """
    Create and bind a ZeroMQ REP socket on the given port.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")
    return context, socket


def run_server(port="5559"):
    """
    Main server loop: receive → handle_message → send.
    """
    context, socket = create_socket(port)
    # Allow periodic wake-ups so we can react to a quit request
    socket.RCVTIMEO = 500  # milliseconds

    stop_event = threading.Event()
    start_quit_listener(stop_event)
    print(f"[category-tag-summary] Listening on port {port}...", file=sys.stderr)

    try:
        while not stop_event.is_set():
            try:
                raw_request = socket.recv()
                print("Request Received")
                response_bytes = handle_message(raw_request)
                print("Sending Response")
                socket.send(response_bytes)
            except zmq.Again:
                # Timeout hit; check for stop_event and continue
                continue
            except Exception as e:
                # Internal error; try to send error response but keep server alive
                error_response = make_error_response(f"Internal error: {str(e)}")
                try:
                    socket.send(serialize_response(error_response))
                except Exception:
                    # client might have disconnected; just move on
                    pass
    except KeyboardInterrupt:
        print("\n[category-tag-summary] Shutting down.", file=sys.stderr)
    finally:
        if stop_event.is_set():
            print("\n[category-tag-summary] 'q' pressed, shutting down.", file=sys.stderr)
        socket.close()
        context.term()


def main():
    port = os.getenv("CATEGORY_TAG_PORT", "5559")
    run_server(port)


if __name__ == "__main__":
    main()