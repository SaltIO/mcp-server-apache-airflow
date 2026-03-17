"""
Airflow 3.x Connection API.
"""
from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.connection_api import ConnectionApi
from airflow_client.client.models import ConnectionBody

from src.airflow.airflow_client import api_client, call_with_token_refresh
from src.airflow.serialization import to_json

connection_api = ConnectionApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (list_connections, "list_connections", "List all connections", True),
        (create_connection, "create_connection", "Create a connection", False),
        (get_connection, "get_connection", "Get a connection by ID", True),
        (update_connection, "update_connection", "Update a connection by ID", False),
        (delete_connection, "delete_connection", "Delete a connection by ID", False),
        (test_connection, "test_connection", "Test a connection", True),
    ]


async def list_connections(
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

    response = call_with_token_refresh(connection_api.get_connections, **kwargs)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def create_connection(
    conn_id: str,
    conn_type: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None,
    extra: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    conn_kwargs = {
        "connection_id": conn_id,
        "conn_type": conn_type,
    }
    if host is not None:
        conn_kwargs["host"] = host
    if port is not None:
        conn_kwargs["port"] = port
    if login is not None:
        conn_kwargs["login"] = login
    if password is not None:
        conn_kwargs["password"] = password
    if schema is not None:
        conn_kwargs["schema"] = schema
    if extra is not None:
        conn_kwargs["extra"] = extra

    connection_body = ConnectionBody(**conn_kwargs)
    response = call_with_token_refresh(connection_api.post_connection, connection_body=connection_body)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def get_connection(conn_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = call_with_token_refresh(connection_api.get_connection, connection_id=conn_id)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def update_connection(
    conn_id: str,
    conn_type: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None,
    extra: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    update_mask = []
    update_kwargs = {}

    if conn_type is not None:
        update_kwargs["conn_type"] = conn_type
        update_mask.append("conn_type")
    if host is not None:
        update_kwargs["host"] = host
        update_mask.append("host")
    if port is not None:
        update_kwargs["port"] = port
        update_mask.append("port")
    if login is not None:
        update_kwargs["login"] = login
        update_mask.append("login")
    if password is not None:
        update_kwargs["password"] = password
        update_mask.append("password")
    if schema is not None:
        update_kwargs["schema"] = schema
        update_mask.append("schema")
    if extra is not None:
        update_kwargs["extra"] = extra
        update_mask.append("extra")

    connection_body = ConnectionBody(**update_kwargs)
    response = call_with_token_refresh(
        connection_api.patch_connection,
        connection_id=conn_id,
        connection_body=connection_body,
        update_mask=update_mask,
    )
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]


async def delete_connection(conn_id: str) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    call_with_token_refresh(connection_api.delete_connection, connection_id=conn_id)
    return [types.TextContent(type="text", text=f"Connection '{conn_id}' deleted successfully.")]


async def test_connection(
    conn_type: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None,
    extra: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    conn_kwargs = {
        "conn_type": conn_type,
    }
    if host is not None:
        conn_kwargs["host"] = host
    if port is not None:
        conn_kwargs["port"] = port
    if login is not None:
        conn_kwargs["login"] = login
    if password is not None:
        conn_kwargs["password"] = password
    if schema is not None:
        conn_kwargs["schema"] = schema
    if extra is not None:
        conn_kwargs["extra"] = extra

    connection_body = ConnectionBody(**conn_kwargs)
    response = call_with_token_refresh(connection_api.test_connection, connection_body=connection_body)
    return [types.TextContent(type="text", text=to_json(response.to_dict()))]
