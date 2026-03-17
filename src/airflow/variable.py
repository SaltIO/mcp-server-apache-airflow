"""
Airflow 3.x Variable API.
"""
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.variable_api import VariableApi
from airflow_client.client.models import VariableBody

from src.airflow.airflow_client import api_client, call_with_token_refresh
from src.airflow.serialization import to_json

variable_api = VariableApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (list_variables, "list_variables", "List all variables", True),
        (create_variable, "create_variable", "Create a variable", False),
        (get_variable, "get_variable", "Get a variable by key", True),
        (update_variable, "update_variable", "Update a variable by key", False),
        (delete_variable, "delete_variable", "Delete a variable by key", False),
    ]


async def list_variables(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = call_with_token_refresh(variable_api.get_variables, **kwargs)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def create_variable(
    key: str, value: str, description: Optional[str] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    var_kwargs = {
        "key": key,
        "value": value,
    }
    if description is not None:
        var_kwargs["description"] = description

    variable_body = VariableBody(**var_kwargs)
    response = call_with_token_refresh(variable_api.post_variable, variable_body=variable_body)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_variable(key: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(variable_api.get_variable, variable_key=key)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def update_variable(
    key: str, value: Optional[str] = None, description: Optional[str] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    update_mask = []
    update_kwargs = {}

    if value is not None:
        update_kwargs["value"] = value
        update_mask.append("value")
    if description is not None:
        update_kwargs["description"] = description
        update_mask.append("description")

    variable_body = VariableBody(**update_kwargs)
    response = call_with_token_refresh(
        variable_api.patch_variable,
        variable_key=key,
        variable_body=variable_body,
        update_mask=update_mask,
    )
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def delete_variable(key: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    call_with_token_refresh(variable_api.delete_variable, variable_key=key)
    return [types.TextContent(type="text", text=f"Variable '{key}' deleted successfully.")]
