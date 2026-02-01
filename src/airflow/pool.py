"""
Airflow 3.x Pool API.
"""
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.pool_api import PoolApi
from airflow_client.client.models import PoolBody, PoolPatchBody

from src.airflow.airflow_client import api_client, call_with_token_refresh

pool_api = PoolApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_pools, "get_pools", "List pools", True),
        (get_pool, "get_pool", "Get a pool by name", True),
        (delete_pool, "delete_pool", "Delete a pool", False),
        (post_pool, "post_pool", "Create a pool", False),
        (patch_pool, "patch_pool", "Update a pool", False),
    ]


async def get_pools(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """List pools."""
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = call_with_token_refresh(pool_api.get_pools, **kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_pool(
    pool_name: str,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Get a pool by name."""
    response = call_with_token_refresh(pool_api.get_pool, pool_name=pool_name)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def delete_pool(
    pool_name: str,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Delete a pool."""
    call_with_token_refresh(pool_api.delete_pool, pool_name=pool_name)
    return [types.TextContent(type="text", text=f"Pool '{pool_name}' deleted successfully.")]


async def post_pool(
    name: str,
    slots: int,
    description: Optional[str] = None,
    include_deferred: Optional[bool] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Create a pool."""
    pool_kwargs = {
        "name": name,
        "slots": slots,
    }
    if description is not None:
        pool_kwargs["description"] = description
    if include_deferred is not None:
        pool_kwargs["include_deferred"] = include_deferred

    pool_body = PoolBody(**pool_kwargs)
    response = call_with_token_refresh(pool_api.post_pool, pool_body=pool_body)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def patch_pool(
    pool_name: str,
    slots: Optional[int] = None,
    description: Optional[str] = None,
    include_deferred: Optional[bool] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """Update a pool."""
    update_mask = []
    patch_kwargs = {}

    if slots is not None:
        patch_kwargs["slots"] = slots
        update_mask.append("slots")
    if description is not None:
        patch_kwargs["description"] = description
        update_mask.append("description")
    if include_deferred is not None:
        patch_kwargs["include_deferred"] = include_deferred
        update_mask.append("include_deferred")

    patch_body = PoolPatchBody(**patch_kwargs)
    response = call_with_token_refresh(
        pool_api.patch_pool,
        pool_name=pool_name,
        pool_patch_body=patch_body,
        update_mask=update_mask,
    )
    return [types.TextContent(type="text", text=str(response.to_dict()))]
