"""
Airflow 3.x Monitor API (health and version).
"""
from typing import Callable, List, Union

import mcp.types as types
from airflow_client.client.api.monitor_api import MonitorApi

from src.airflow.airflow_client import api_client, call_with_token_refresh

monitor_api = MonitorApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_health, "get_health", "Get instance status", True),
    ]


async def get_health() -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """
    Get the status of Airflow's metadatabase, triggerer and scheduler.
    It includes info about metadatabase and last heartbeat of scheduler and triggerer.
    """
    response = call_with_token_refresh(monitor_api.get_health)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
