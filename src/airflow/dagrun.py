"""
Airflow 3.x DAG Run API.
"""
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.dag_run_api import DagRunApi
from airflow_client.client.models import (
    DAGRunClearBody,
    DAGRunPatchBody,
    TriggerDAGRunPostBody,
)

from src.airflow.airflow_client import api_client, call_with_token_refresh
from src.envs import AIRFLOW_HOST
from src.airflow.serialization import to_json

dag_run_api = DagRunApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (post_dag_run, "post_dag_run", "Trigger a DAG by ID", False),
        (get_dag_runs, "get_dag_runs", "Get DAG runs by ID", True),
        (get_dag_run, "get_dag_run", "Get a DAG run by DAG ID and DAG run ID", True),
        (patch_dag_run, "patch_dag_run", "Update a DAG run by DAG ID and DAG run ID", False),
        (delete_dag_run, "delete_dag_run", "Delete a DAG run by DAG ID and DAG run ID", False),
        (clear_dag_run, "clear_dag_run", "Clear a DAG run", False),
        (get_upstream_asset_events, "get_upstream_asset_events", "Get asset events for a DAG run", True),
    ]


def get_dag_run_url(dag_id: str, dag_run_id: str) -> str:
    return f"{AIRFLOW_HOST}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"


def _parse_datetime(value: Any) -> Optional[datetime]:
    """Parse a datetime value from string or return datetime object."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            normalized_value = value
            if value.endswith('Z'):
                normalized_value = value[:-1] + '+00:00'
            dt = datetime.fromisoformat(normalized_value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid datetime format: {value}. Expected ISO 8601 format (e.g. 2024-01-15T10:30:00Z). To run immediately, omit this parameter entirely.") from e

    raise TypeError(f"Expected datetime or string, got {type(value).__name__}")


async def post_dag_run(
    dag_id: str,
    dag_run_id: Optional[str] = None,
    logical_date: Optional[str] = None,
    data_interval_start: Optional[str] = None,
    data_interval_end: Optional[str] = None,
    conf: Optional[Dict[str, Any]] = None,
    note: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Trigger a DAG run.

    Args:
        dag_id: The DAG ID to trigger (required).
        dag_run_id: Optional custom run ID. If not provided, Airflow generates one.
        logical_date: Optional execution date in ISO 8601 format (e.g. '2024-01-15T10:30:00Z').
                      OMIT this parameter to run immediately with current time.
        data_interval_start: Optional data interval start in ISO 8601 format. Usually omit.
        data_interval_end: Optional data interval end in ISO 8601 format. Usually omit.
        conf: Optional configuration dictionary to pass to the DAG.
        note: Optional note to attach to the DAG run.
    """
    logical_date = _parse_datetime(logical_date)
    data_interval_start = _parse_datetime(data_interval_start)
    data_interval_end = _parse_datetime(data_interval_end)

    # Build trigger body
    trigger_kwargs = {}
    if dag_run_id is not None:
        trigger_kwargs["dag_run_id"] = dag_run_id
    if logical_date is not None:
        trigger_kwargs["logical_date"] = logical_date
    if data_interval_start is not None:
        trigger_kwargs["data_interval_start"] = data_interval_start
    if data_interval_end is not None:
        trigger_kwargs["data_interval_end"] = data_interval_end
    if conf is not None:
        trigger_kwargs["conf"] = conf
    if note is not None:
        trigger_kwargs["note"] = note

    trigger_body = TriggerDAGRunPostBody(**trigger_kwargs)
    response = call_with_token_refresh(dag_run_api.trigger_dag_run, dag_id=dag_id, trigger_dag_run_post_body=trigger_body)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_dag_runs(
    dag_id: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    logical_date_gte: Optional[str] = None,
    logical_date_lte: Optional[str] = None,
    start_date_gte: Optional[str] = None,
    start_date_lte: Optional[str] = None,
    end_date_gte: Optional[str] = None,
    end_date_lte: Optional[str] = None,
    state: Optional[List[str]] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if logical_date_gte is not None:
        kwargs["logical_date_gte"] = logical_date_gte
    if logical_date_lte is not None:
        kwargs["logical_date_lte"] = logical_date_lte
    if start_date_gte is not None:
        kwargs["start_date_gte"] = start_date_gte
    if start_date_lte is not None:
        kwargs["start_date_lte"] = start_date_lte
    if end_date_gte is not None:
        kwargs["end_date_gte"] = end_date_gte
    if end_date_lte is not None:
        kwargs["end_date_lte"] = end_date_lte
    if state is not None:
        kwargs["state"] = state
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = call_with_token_refresh(dag_run_api.get_dag_runs, dag_id=dag_id, **kwargs)
    response_dict = response.to_dict()

    # Add UI links to each DAG run
    for dag_run in response_dict.get("dag_runs", []):
        dag_run["ui_url"] = get_dag_run_url(dag_id, dag_run["dag_run_id"])

    return [types.TextContent(type="text", text=to_json(response_dict))]


async def get_dag_run(
    dag_id: str, dag_run_id: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(dag_run_api.get_dag_run, dag_id=dag_id, dag_run_id=dag_run_id)
    response_dict = response.to_dict()
    response_dict["ui_url"] = get_dag_run_url(dag_id, dag_run_id)
    return [types.TextContent(type="text", text=to_json(response_dict))]


async def patch_dag_run(
    dag_id: str,
    dag_run_id: str,
    state: Optional[str] = None,
    note: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Update a DAG run (state, note)."""
    update_mask = []
    patch_kwargs = {}

    if state is not None:
        patch_kwargs["state"] = state
        update_mask.append("state")
    if note is not None:
        patch_kwargs["note"] = note
        update_mask.append("note")

    patch_body = DAGRunPatchBody(**patch_kwargs)
    response = call_with_token_refresh(
        dag_run_api.patch_dag_run,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        dag_run_patch_body=patch_body,
        update_mask=update_mask,
    )
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def delete_dag_run(
    dag_id: str, dag_run_id: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    call_with_token_refresh(dag_run_api.delete_dag_run, dag_id=dag_id, dag_run_id=dag_run_id)
    return [types.TextContent(type="text", text=f"DAG run '{dag_run_id}' deleted successfully.")]


async def clear_dag_run(
    dag_id: str, dag_run_id: str, dry_run: Optional[bool] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    clear_kwargs = {}
    if dry_run is not None:
        clear_kwargs["dry_run"] = dry_run

    clear_body = DAGRunClearBody(**clear_kwargs)
    response = call_with_token_refresh(
        dag_run_api.clear_dag_run,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        dag_run_clear_body=clear_body,
    )
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_upstream_asset_events(
    dag_id: str, dag_run_id: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get upstream asset events for a DAG run (formerly dataset events)."""
    response = call_with_token_refresh(dag_run_api.get_upstream_asset_events, dag_id=dag_id, dag_run_id=dag_run_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]
