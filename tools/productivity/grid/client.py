"""Client for Grid's Streamable HTTP MCP server.

In Centaur, iron-proxy injects the shared Grid Bearer token for
``grid.tempo.xyz``. For local development only, set ``GRID_MCP_TOKEN``.
"""

import json
from typing import Any

import httpx

from centaur_sdk import secret

MCP_URL = "https://grid.tempo.xyz/api/mcp"
MCP_PROTOCOL_VERSION = "2025-06-18"


class GridClient:
    """Thin JSON-RPC client for Grid's MCP server."""

    def __init__(self, token: str | None = None, timeout: float = 120.0):
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        token = token or secret("GRID_MCP_TOKEN", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.Client(headers=headers, timeout=timeout)
        self._rpc_id = 0
        self._initialized = False

    def close(self) -> None:
        self._http.close()

    def _decode_response(self, response: httpx.Response, rpc_id: int) -> dict[str, Any]:
        response.raise_for_status()
        if not response.content:
            return {}

        content_type = response.headers.get("content-type", "")
        if content_type.startswith("text/event-stream"):
            messages = []
            for line in response.text.splitlines():
                if line.startswith("data:"):
                    messages.append(json.loads(line.removeprefix("data:").strip()))
            message = next((item for item in reversed(messages) if item.get("id") == rpc_id), None)
            if message is None:
                raise RuntimeError("Grid MCP event stream contained no matching JSON-RPC response")
        else:
            message = response.json()

        if "error" in message:
            raise RuntimeError(f"Grid MCP error: {message['error']}")
        return message.get("result", {})

    def _request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._rpc_id += 1
        rpc_id = self._rpc_id
        response = self._http.post(
            MCP_URL,
            json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params or {}},
        )
        session_id = response.headers.get("Mcp-Session-Id")
        if session_id:
            self._http.headers["Mcp-Session-Id"] = session_id
        return self._decode_response(response, rpc_id)

    def _notify(self, method: str) -> None:
        response = self._http.post(MCP_URL, json={"jsonrpc": "2.0", "method": method})
        response.raise_for_status()

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        result = self._request(
            "initialize",
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "centaur-grid", "version": "0.1.0"},
            },
        )
        protocol_version = result.get("protocolVersion", MCP_PROTOCOL_VERSION)
        self._http.headers["MCP-Protocol-Version"] = protocol_version
        self._notify("notifications/initialized")
        self._initialized = True

    def list_tools(self) -> dict[str, Any]:
        """List tools exposed by Grid's MCP server."""
        self._ensure_initialized()
        return self._request("tools/list")

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """Call a Grid MCP tool by name with its JSON-compatible arguments."""
        self._ensure_initialized()
        result = self._request("tools/call", {"name": name, "arguments": arguments or {}})
        if result.get("isError"):
            raise RuntimeError(f"Grid MCP tool {name} failed: {result}")
        return result


def _client() -> GridClient:
    return GridClient()
