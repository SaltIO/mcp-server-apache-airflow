from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.event_log_api import EventLogApi

from src.airflow.airflow_client import api_client, call_with_token_refresh

event_log_api = EventLogApi(api_client)


def _parse_datetime(value: Any) -> Optional[datetime]:
    """
    Parse a datetime value from string or return datetime object.

    Args:
        value: String (ISO 8601 format) or datetime object

    Returns:
        datetime object or None
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            # Try parsing ISO 8601 format (e.g., "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+00:00")
            # Replace Z at end of string with +00:00 for fromisoformat
            normalized_value = value
            if value.endswith('Z'):
                normalized_value = value[:-1] + '+00:00'

            # Parse ISO format
            dt = datetime.fromisoformat(normalized_value)

            # Ensure timezone-aware (default to UTC if naive)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid datetime format: {value}. Expected ISO 8601 format (e.g., '2024-01-01T00:00:00Z')") from e

    raise TypeError(f"Expected datetime or string, got {type(value).__name__}")


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_event_logs, "get_event_logs", "List log entries from event log", True),
        (get_event_log, "get_event_log", "Get a specific log entry by ID", True),
    ]


async def get_event_logs(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    map_index: Optional[int] = None,
    try_number: Optional[int] = None,
    event: Optional[str] = None,
    owner: Optional[str] = None,
    before: Optional[datetime] = None,
    after: Optional[datetime] = None,
    included_events: Optional[str] = None,
    excluded_events: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    # Convert string dates to datetime objects if needed (from JSON/chat interface)
    before = _parse_datetime(before)
    after = _parse_datetime(after)

    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by
    if dag_id is not None:
        kwargs["dag_id"] = dag_id
    if task_id is not None:
        kwargs["task_id"] = task_id
    if run_id is not None:
        kwargs["run_id"] = run_id
    if map_index is not None:
        kwargs["map_index"] = map_index
    if try_number is not None:
        kwargs["try_number"] = try_number
    if event is not None:
        kwargs["event"] = event
    if owner is not None:
        kwargs["owner"] = owner
    if before is not None:
        kwargs["before"] = before
    if after is not None:
        kwargs["after"] = after
    if included_events is not None:
        kwargs["included_events"] = included_events
    if excluded_events is not None:
        kwargs["excluded_events"] = excluded_events

    response = call_with_token_refresh(event_log_api.get_event_logs, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_event_log(
    event_log_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(event_log_api.get_event_log, event_log_id=event_log_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
