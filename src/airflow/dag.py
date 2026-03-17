"""
Airflow 3.x DAG API.
"""
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.dag_api import DAGApi
from airflow_client.client.api.task_api import TaskApi
from airflow_client.client.api.dag_source_api import DagSourceApi
from airflow_client.client.models import DAGPatchBody

from src.airflow.airflow_client import api_client, call_with_token_refresh
from src.envs import AIRFLOW_HOST
from src.airflow.serialization import to_json

dag_api = DAGApi(api_client)
task_api = TaskApi(api_client)
dag_source_api = DagSourceApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_dags, "fetch_dags", "Fetch all DAGs", True),
        (get_dag, "get_dag", "Get a DAG by ID", True),
        (get_dag_details, "get_dag_details", "Get a simplified representation of DAG", True),
        (get_dag_source, "get_dag_source", "Get a source code", True),
        (pause_dag, "pause_dag", "Pause a DAG by ID", False),
        (unpause_dag, "unpause_dag", "Unpause a DAG by ID", False),
        (get_dag_tasks, "get_dag_tasks", "Get tasks for DAG", True),
        (get_task, "get_task", "Get a task by ID", True),
        (get_tasks, "get_tasks", "Get tasks for DAG", True),
        (patch_dag, "patch_dag", "Update a DAG", False),
        (patch_dags, "patch_dags", "Update multiple DAGs", False),
        (delete_dag, "delete_dag", "Delete a DAG", False),
    ]


def get_dag_url(dag_id: str) -> str:
    return f"{AIRFLOW_HOST}/dags/{dag_id}"


async def get_dags(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    tags: Optional[List[str]] = None,
    paused: Optional[bool] = None,
    dag_id_pattern: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """List DAGs from Airflow.

    Args:
        limit: Maximum number of DAGs to return. Default is 100.
        offset: Number of DAGs to skip before returning results.
        order_by: Field name to order results by. Prefix with '-' for descending order.
        tags: Filter DAGs by tags. Example: ["production", "etl"]
        paused: Filter by paused state. None returns all, True returns only paused, False returns only unpaused.
        dag_id_pattern: Filter DAGs by ID substring match. Use simple text, NOT regex.
            Example: "elt" matches "elt_transform", "my_elt_dag", etc.
            Do NOT use regex patterns like ".*elt.*" - use just "elt" instead.

    Returns:
        List of DAGs matching the criteria with their details and UI URLs.
    """
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by
    if tags is not None:
        kwargs["tags"] = tags
    if paused is not None:
        kwargs["paused"] = paused
    if dag_id_pattern is not None:
        kwargs["dag_id_pattern"] = dag_id_pattern

    response = call_with_token_refresh(dag_api.get_dags, **kwargs)
    response_dict = response.to_dict()

    # Add UI links to each DAG
    for dag in response_dict.get("dags", []):
        dag["ui_url"] = get_dag_url(dag["dag_id"])

    return [types.TextContent(type="text", text=to_json(response_dict))]


async def get_dag(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(dag_api.get_dag, dag_id=dag_id)
    response_dict = response.to_dict()
    response_dict["ui_url"] = get_dag_url(dag_id)
    return [types.TextContent(type="text", text=to_json(response_dict))]


async def get_dag_details(
    dag_id: str,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get a simplified representation of a DAG."""
    response = call_with_token_refresh(dag_api.get_dag_details, dag_id=dag_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_dag_source(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get DAG source code."""
    response = call_with_token_refresh(dag_source_api.get_dag_source, dag_id=dag_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def pause_dag(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    patch_body = DAGPatchBody(is_paused=True)
    response = call_with_token_refresh(dag_api.patch_dag, dag_id=dag_id, dag_patch_body=patch_body, update_mask=["is_paused"])
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def unpause_dag(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    patch_body = DAGPatchBody(is_paused=False)
    response = call_with_token_refresh(dag_api.patch_dag, dag_id=dag_id, dag_patch_body=patch_body, update_mask=["is_paused"])
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_dag_tasks(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(task_api.get_tasks, dag_id=dag_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def patch_dag(
    dag_id: str, is_paused: Optional[bool] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    update_mask = []
    patch_body_kwargs = {}

    if is_paused is not None:
        patch_body_kwargs["is_paused"] = is_paused
        update_mask.append("is_paused")

    patch_body = DAGPatchBody(**patch_body_kwargs)
    response = call_with_token_refresh(dag_api.patch_dag, dag_id=dag_id, dag_patch_body=patch_body, update_mask=update_mask)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def patch_dags(
    dag_id_pattern: Optional[str] = None,
    is_paused: Optional[bool] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    update_mask = []
    patch_body_kwargs = {}

    if is_paused is not None:
        patch_body_kwargs["is_paused"] = is_paused
        update_mask.append("is_paused")

    patch_body = DAGPatchBody(**patch_body_kwargs)

    kwargs = {}
    if dag_id_pattern is not None:
        kwargs["dag_id_pattern"] = dag_id_pattern

    response = call_with_token_refresh(dag_api.patch_dags, dag_patch_body=patch_body, update_mask=update_mask, **kwargs)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def delete_dag(dag_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    call_with_token_refresh(dag_api.delete_dag, dag_id=dag_id)
    return [types.TextContent(type="text", text=f"DAG '{dag_id}' deleted successfully.")]


async def get_task(
    dag_id: str, task_id: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(task_api.get_task, dag_id=dag_id, task_id=task_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_tasks(
    dag_id: str, order_by: Optional[str] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    kwargs = {}
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = call_with_token_refresh(task_api.get_tasks, dag_id=dag_id, **kwargs)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]
