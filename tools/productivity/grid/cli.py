"""CLI for Grid's MCP server."""

import json
from typing import Any

import typer
from dotenv import load_dotenv

from .client import GridClient

load_dotenv()

app = typer.Typer(name="grid", help="Discover and call Grid presentation MCP tools")


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


@app.command("health")
def health() -> None:
    """Assert Grid MCP connectivity and authentication."""
    client = GridClient()
    try:
        result = client.list_tools()
        _print({"ok": True, "tool": "grid", "error": None, "details": result})
    except Exception as exc:
        _print({"ok": False, "tool": "grid", "error": str(exc), "details": {}})
        raise typer.Exit(1) from exc
    finally:
        client.close()


@app.command("tools")
def list_tools() -> None:
    """List tools advertised by Grid."""
    client = GridClient()
    try:
        _print(client.list_tools())
    finally:
        client.close()


@app.command("call")
def call_tool(
    name: str = typer.Argument(..., help="Grid MCP tool name"),
    arguments: str = typer.Option("{}", "--arguments", "-a", help="Tool arguments as JSON"),
) -> None:
    """Call a Grid MCP tool."""
    try:
        parsed = json.loads(arguments)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"arguments must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("arguments must decode to a JSON object")

    client = GridClient()
    try:
        _print(client.call_tool(name, parsed))
    finally:
        client.close()


if __name__ == "__main__":
    app()
