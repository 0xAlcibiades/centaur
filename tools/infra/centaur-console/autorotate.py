"""Sandbox-scoped Autorotate account-pool operations."""

from __future__ import annotations

import json

import typer
from rich.console import Console

app = typer.Typer(
    name="autorotate",
    help="Inspect the Codex account pool",
    no_args_is_help=True,
)
console = Console()


@app.callback()
def main() -> None:
    """Inspect the Codex account pool."""


def get_client():
    from .client import ConsoleClient

    # Sandboxes reach Console through iron-proxy, which injects the scoped JWT.
    # This CLI never accepts or handles an Autorotate broker credential.
    return ConsoleClient()


def _print_json(value: dict[str, object]) -> None:
    console.print_json(json.dumps(value, default=str))


@app.command()
def status():
    """Print redacted Codex account-pool health and capacity."""
    with get_client() as client:
        result = client.autorotate_status()
    _print_json(result)


if __name__ == "__main__":
    app()
