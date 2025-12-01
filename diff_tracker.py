import json
import os
import sys
import zmq


# =========================
# Core diff logic
# =========================

def build_index(items, id_field):
    """
    Build a mapping: id_value -> item dict.
    Ignores items missing the id_field.
    """
    index = {}
    for item in items:
        if id_field in item:
            index[item[id_field]] = item
    return index


def compare_field(before_item, after_item, field_name):
    """
    Compare one field between before_item and after_item.
    Returns:
      (changed: bool, before_value, after_value)
    """
    before_value = before_item.get(field_name)
    after_value = after_item.get(field_name)
    changed = (before_value != after_value)
    return changed, before_value, after_value


def compute_diffs(before_list, after_list, id_field, fields_to_compare):
    """
    Main diff engine.

    Returns:
      added_items: list[dict]
      removed_items: list[dict]
      modified_items: list[dict] (with before, after, changed_fields)
    """
    before_index = build_index(before_list, id_field)
    after_index = build_index(after_list, id_field)

    before_ids = set(before_index.keys())
    after_ids = set(after_index.keys())

    added_ids = after_ids - before_ids
    removed_ids = before_ids - after_ids
    candidate_modified_ids = before_ids & after_ids

    # Build lists with deterministic ordering: sort by str(id)
    added_items = [
        after_index[id_val] for id_val in sorted(added_ids, key=lambda x: str(x))
    ]
    removed_items = [
        before_index[id_val] for id_val in sorted(removed_ids, key=lambda x: str(x))
    ]

    modified_items = []
    for id_val in sorted(candidate_modified_ids, key=lambda x: str(x)):
        before_item = before_index[id_val]
        after_item = after_index[id_val]

        changed_fields = {}
        for field in fields_to_compare:
            changed, before_value, after_value = compare_field(before_item, after_item, field)
            if changed:
                changed_fields[field] = {
                    "before": before_value,
                    "after": after_value
                }

        if changed_fields:
            modified_items.append({
                id_field: id_val,
                "before": before_item,
                "after": after_item,
                "changed_fields": changed_fields
            })

    return added_items, removed_items, modified_items


# =========================
# Request / response helpers
# =========================

def make_error_response(message):
    return {
        "status": "error",
        "error": message
    }


def make_success_response(id_field, fields_to_compare,
                          added_items, removed_items, modified_items):
    return {
        "status": "ok",
        "id_field": id_field,
        "fields_to_compare": fields_to_compare,
        "added_items": added_items,
        "removed_items": removed_items,
        "modified_items": modified_items
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


def validate_request(request):
    """
    Validate top-level request structure.
    Returns:
      (id_field, fields_to_compare, before_list, after_list, error_or_none)
    """
    if request.get("request_type") != "change_diff":
        return None, None, None, None, make_error_response(
            "Unsupported request_type. Expected 'change_diff'."
        )

    id_field = request.get("id_field", "id")
    if not isinstance(id_field, str) or not id_field.strip():
        return None, None, None, None, make_error_response(
            "'id_field' must be a non-empty string."
        )

    fields_to_compare = request.get("fields_to_compare")
    if not isinstance(fields_to_compare, list) or not fields_to_compare:
        return None, None, None, None, make_error_response(
            "'fields_to_compare' must be a non-empty list of field names."
        )
    # Ensure all fields are strings
    for f in fields_to_compare:
        if not isinstance(f, str) or not f.strip():
            return None, None, None, None, make_error_response(
                "All entries in 'fields_to_compare' must be non-empty strings."
            )

    before_list = request.get("before")
    after_list = request.get("after")
    if not isinstance(before_list, list) or not isinstance(after_list, list):
        return None, None, None, None, make_error_response(
            "'before' and 'after' must both be lists."
        )

    return id_field, fields_to_compare, before_list, after_list, None


def handle_message(raw_bytes):
    """
    Pure handler: bytes in → bytes out.
    Perfect for integration tests.
    """
    request, parse_error = parse_request_bytes(raw_bytes)
    if parse_error is not None:
        return serialize_response(parse_error)

    id_field, fields_to_compare, before_list, after_list, validation_error = validate_request(request)
    if validation_error is not None:
        return serialize_response(validation_error)

    added_items, removed_items, modified_items = compute_diffs(
        before_list, after_list, id_field, fields_to_compare
    )

    response = make_success_response(
        id_field=id_field,
        fields_to_compare=fields_to_compare,
        added_items=added_items,
        removed_items=removed_items,
        modified_items=modified_items
    )
    return serialize_response(response)


# =========================
# ZeroMQ server
# =========================

def create_socket(port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")
    return context, socket


def run_server(port="5563"):
    """
    Main server loop.
    """
    context, socket = create_socket(port)
    print(f"[change-diff] Listening on port {port}...", file=sys.stderr)

    try:
        while True:
            raw_request = socket.recv()
            print("Received Request")
            try:
                response_bytes = handle_message(raw_request)
            except Exception as e:
                error_response = make_error_response(f"Internal error: {str(e)}")
                response_bytes = serialize_response(error_response)

            print("Sending diff list")
            socket.send(response_bytes)
    except KeyboardInterrupt:
        print("\n[change-diff] Interrupted via keyboard.", file=sys.stderr)
    finally:
        socket.close()
        context.term()


def main():
    port = os.getenv("CHANGE_DIFF_PORT", "5563")
    run_server(port)


if __name__ == "__main__":
    main()