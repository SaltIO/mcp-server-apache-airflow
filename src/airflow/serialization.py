"""JSON serialization helpers for Airflow API responses."""

import json


def _json_default(obj):
    """Handle non-JSON-serializable types from Airflow API responses."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)


def to_json(data) -> str:
    """Serialize a Python dict/list to a JSON string.

    Handles datetime objects and other Airflow API types that are not
    natively JSON-serializable.
    """
    return json.dumps(data, default=_json_default)
