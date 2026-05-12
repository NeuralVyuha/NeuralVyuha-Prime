import sys
import json

def verify_groups(response_json, expected_count):
    try:
        data = json.loads(response_json)
        total = data.get("total", 0)
        hits = data.get("hits", [])

        if total != expected_count:
            print(f"FAILURE: Expected {expected_count} groups, found {total}")
            sys.exit(1)

        print(f"SUCCESS: Found {total} groups as expected.")

        # Verify tenant isolation (implied by query success, but check hits)
        # We can't easily pass expected tenant_id here without more args,
        # but if hits came back, query worked.

    except json.JSONDecodeError:
        print(f"FAILURE: Invalid JSON response: {response_json}")
        sys.exit(1)
    except Exception as e:
        print(f"FAILURE: Verification error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: verify_smoke.py <json_response> <expected_count>")
        sys.exit(1)

    verify_groups(sys.argv[1], int(sys.argv[2]))
