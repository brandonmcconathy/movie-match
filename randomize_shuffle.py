import sys
import random
import threading
import zmq


# ---------------------------------------------------------------------------
# Helper: Build error responses
# ---------------------------------------------------------------------------
def make_error(message: str, code: str | None = None) -> dict:
    err = {"message": message}
    if code is not None:
        err["code"] = code
    return {
        "ok": False,
        "error": err
    }


# ---------------------------------------------------------------------------
# Core actions: shuffle and sample
# ---------------------------------------------------------------------------
def handle_shuffle(items, seed=None) -> dict:
    """
    Shuffle the given items.
    - If seed is provided (numeric), use a deterministic shuffle.
    - If seed is omitted, use a normal random shuffle.
    Returns a success response dict.
    """
    # Make a copy so we don't modify caller's list
    shuffled = list(items)

    if seed is not None:
        # Deterministic shuffle with a local Random instance
        try:
            seeded_rng = random.Random(int(seed))
        except (ValueError, TypeError):
            return make_error("Seed must be a numeric value.", "INVALID_SEED")
        seeded_rng.shuffle(shuffled)
    else:
        # Non-deterministic shuffle
        random.shuffle(shuffled)

    return {
        "ok": True,
        "result": {
            "items": shuffled
        }
    }


def handle_sample(items, k) -> dict:
    """
    Sample K unique items from the list without replacement.
    Requirements:
    - 1 <= k <= len(items)
    """
    n = len(items)

    # Validate k
    try:
        k_int = int(k)
    except (ValueError, TypeError):
        return make_error("Field 'k' must be an integer.", "INVALID_K_TYPE")

    if k_int <= 0:
        return make_error("k must be > 0.", "K_TOO_SMALL")
    if k_int > n:
        return make_error("k cannot be greater than the number of items.", "K_TOO_LARGE")

    # Draw k unique items without replacement
    sampled = random.sample(items, k_int)

    return {
        "ok": True,
        "result": {
            "items": sampled
        }
    }


# ---------------------------------------------------------------------------
# Process Request
# ---------------------------------------------------------------------------
def process_request(payload: dict) -> dict:
    """
    Process a JSON request from the client.
    Expected fields:
      - action: "shuffle" or "sample"
      - items: array
      - seed: optional (for shuffle only)
      - k: required for sample
    """
    if not isinstance(payload, dict):
        return make_error("Request must be a JSON object.", "INVALID_PAYLOAD")

    # Validate action
    action = payload.get("action")
    if not isinstance(action, str):
        return make_error("Field 'action' must be a string.", "MISSING_ACTION")

    action = action.lower()

    # Validate items
    items = payload.get("items")
    if not isinstance(items, list):
        return make_error("Field 'items' must be a JSON array.", "INVALID_ITEMS_TYPE")

    # Performance requirement: N <= 1000 should be fast enough with O(N) operations.
    # We don't reject larger inputs here, but our algorithm is still linear-time.

    # Empty array behavior (for shuffle specifically)
    if action == "shuffle":
        seed = payload.get("seed", None)
        # Empty array is allowed and should return empty array
        return handle_shuffle(items, seed=seed)

    elif action == "sample":
        # For sampling, we need k
        if "k" not in payload:
            return make_error("Field 'k' is required for action 'sample'.", "MISSING_K")
        k = payload.get("k")
        return handle_sample(items, k)

    else:
        return make_error(f"Unsupported action '{action}'. Use 'shuffle' or 'sample'.",
                          "UNSUPPORTED_ACTION")


# ---------------------------------------------------------------------------
# Shutdown listener (press 'q' to quit)
# ---------------------------------------------------------------------------
def shutdown_listener(stop_flag):
    """
    Waits for the user to type 'q' then Enter to request shutdown.
    Sets stop_flag[0] = True so the main loop can exit cleanly.
    """
    print("Press 'q' then Enter to stop the microservice...")
    for line in sys.stdin:
        if line.strip().lower() == "q":
            stop_flag[0] = True
            print("Shutdown requested...")
            break


# ---------------------------------------------------------------------------
# Main Server Loop
# ---------------------------------------------------------------------------
def main(port=5555):
    # ZeroMQ REP socket
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    address = f"tcp://*:{port}"
    socket.bind(address)

    print(f"Randomize/Shuffle microservice listening on {address}")

    # Shared flag so the listener thread can tell the loop to stop
    stop_flag = [False]

    # Start a background thread to listen for 'q' to quit
    listener_thread = threading.Thread(
        target=shutdown_listener,
        args=(stop_flag,),
        daemon=True
    )
    listener_thread.start()

    try:
        while not stop_flag[0]:
            # Poll every 1000 ms so we can notice when stop_flag changes
            if socket.poll(timeout=1000):  # timeout in milliseconds
                try:
                    payload = socket.recv_json()
                    print("Received Request")
                except Exception as e:
                    # If JSON is malformed, respond with an error instead of crashing
                    resp = make_error(f"Failed to parse JSON: {e}", "JSON_PARSE_ERROR")
                    socket.send_json(resp)
                    continue

                response = process_request(payload)
                print("Sending randomized list")
                socket.send_json(response)

    except Exception as e:
        print(f"Error in microservice: {e}")

    finally:
        print("Shutting down microservice...")
        socket.close()
        context.term()
        sys.exit(0)


if __name__ == "__main__":
    # Custom port support via command-line argument
    port = 5555  # default port

    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"Invalid port '{sys.argv[1]}', using default 5555 instead.")

    main(port)