"""
Airflow 3.x Asset API (formerly Dataset API in Airflow 2.x).

In Airflow 3.0, "Datasets" were renamed to "Assets" per AIP-79.
Tool names are kept as 'dataset' for backward compatibility with existing prompts/capabilities.
"""
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.asset_api import AssetApi

from src.airflow.airflow_client import api_client, call_with_token_refresh

asset_api = AssetApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_datasets, "get_datasets", "List assets (datasets)", True),
        (get_dataset, "get_dataset", "Get an asset (dataset) by ID", True),
        (get_dataset_events, "get_dataset_events", "Get asset (dataset) events", True),
        (create_dataset_event, "create_dataset_event", "Create asset (dataset) event", False),
        (get_dag_dataset_queued_event, "get_dag_dataset_queued_event", "Get a queued asset event for a DAG", True),
        (get_dag_dataset_queued_events, "get_dag_dataset_queued_events", "Get queued asset events for a DAG", True),
        (
            delete_dag_dataset_queued_event,
            "delete_dag_dataset_queued_event",
            "Delete a queued asset event for a DAG",
            False,
        ),
        (
            delete_dag_dataset_queued_events,
            "delete_dag_dataset_queued_events",
            "Delete queued asset events for a DAG",
            False,
        ),
        (get_dataset_queued_events, "get_dataset_queued_events", "Get queued asset events for an asset", True),
        (
            delete_dataset_queued_events,
            "delete_dataset_queued_events",
            "Delete queued asset events for an asset",
            False,
        ),
        # New Airflow 3.x Asset API methods
        (get_asset_aliases, "get_asset_aliases", "List asset aliases", True),
        (get_asset_alias, "get_asset_alias", "Get an asset alias by ID", True),
        (materialize_asset, "materialize_asset", "Trigger materialization of an asset", False),
    ]


async def get_datasets(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    name_pattern: Optional[str] = None,
    uri_pattern: Optional[str] = None,
    dag_ids: Optional[str] = None,
    only_active: Optional[bool] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """List assets (formerly datasets in Airflow 2.x)."""
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by
    if name_pattern is not None:
        kwargs["name_pattern"] = name_pattern
    if uri_pattern is not None:
        kwargs["uri_pattern"] = uri_pattern
    if dag_ids is not None:
        kwargs["dag_ids"] = dag_ids
    if only_active is not None:
        kwargs["only_active"] = only_active

    response = call_with_token_refresh(asset_api.get_assets, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_dataset(
    asset_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get an asset (formerly dataset) by ID."""
    response = call_with_token_refresh(asset_api.get_asset, asset_id=asset_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_dataset_events(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    asset_id: Optional[int] = None,
    source_dag_id: Optional[str] = None,
    source_task_id: Optional[str] = None,
    source_run_id: Optional[str] = None,
    source_map_index: Optional[int] = None,
    timestamp_gte: Optional[str] = None,
    timestamp_gt: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
    timestamp_lt: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get asset (formerly dataset) events."""
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by
    if asset_id is not None:
        kwargs["asset_id"] = asset_id
    if source_dag_id is not None:
        kwargs["source_dag_id"] = source_dag_id
    if source_task_id is not None:
        kwargs["source_task_id"] = source_task_id
    if source_run_id is not None:
        kwargs["source_run_id"] = source_run_id
    if source_map_index is not None:
        kwargs["source_map_index"] = source_map_index
    if timestamp_gte is not None:
        kwargs["timestamp_gte"] = timestamp_gte
    if timestamp_gt is not None:
        kwargs["timestamp_gt"] = timestamp_gt
    if timestamp_lte is not None:
        kwargs["timestamp_lte"] = timestamp_lte
    if timestamp_lt is not None:
        kwargs["timestamp_lt"] = timestamp_lt

    response = call_with_token_refresh(asset_api.get_asset_events, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def create_dataset_event(
    asset_uri: str,
    extra: Optional[Dict[str, Any]] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Create an asset (formerly dataset) event."""
    event_body = {
        "asset_uri": asset_uri,
    }
    if extra is not None:
        event_body["extra"] = extra

    response = call_with_token_refresh(asset_api.create_asset_event, create_asset_events_body=event_body)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_dag_dataset_queued_event(
    dag_id: str,
    asset_id: int,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get a queued asset event for a DAG."""
    kwargs: Dict[str, Any] = {"dag_id": dag_id, "asset_id": asset_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.get_dag_asset_queued_event, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_dag_dataset_queued_events(
    dag_id: str,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get queued asset events for a DAG."""
    kwargs: Dict[str, Any] = {"dag_id": dag_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.get_dag_asset_queued_events, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def delete_dag_dataset_queued_event(
    dag_id: str,
    asset_id: int,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Delete a queued asset event for a DAG."""
    kwargs: Dict[str, Any] = {"dag_id": dag_id, "asset_id": asset_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.delete_dag_asset_queued_event, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def delete_dag_dataset_queued_events(
    dag_id: str,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Delete queued asset events for a DAG."""
    kwargs: Dict[str, Any] = {"dag_id": dag_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.delete_dag_asset_queued_events, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_dataset_queued_events(
    asset_id: int,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get queued asset events for an asset."""
    kwargs: Dict[str, Any] = {"asset_id": asset_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.get_asset_queued_events, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def delete_dataset_queued_events(
    asset_id: int,
    before: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Delete queued asset events for an asset."""
    kwargs: Dict[str, Any] = {"asset_id": asset_id}
    if before is not None:
        kwargs["before"] = before
    response = call_with_token_refresh(asset_api.delete_asset_queued_events, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


# New Airflow 3.x Asset API methods

async def get_asset_aliases(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    name_pattern: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """List asset aliases."""
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by
    if name_pattern is not None:
        kwargs["name_pattern"] = name_pattern

    response = call_with_token_refresh(asset_api.get_asset_aliases, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_asset_alias(
    asset_alias_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get an asset alias by ID."""
    response = call_with_token_refresh(asset_api.get_asset_alias, asset_alias_id=asset_alias_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def materialize_asset(
    asset_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Trigger materialization of an asset."""
    response = call_with_token_refresh(asset_api.materialize_asset, asset_id=asset_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
