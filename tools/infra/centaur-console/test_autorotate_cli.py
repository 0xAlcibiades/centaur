import json

import autorotate
from typer.testing import CliRunner


class FakeClient:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def autorotate_status(self):
        self.calls.append(("status",))
        return {"total": 5, "available": 3, "limited": 1}


def invoke(monkeypatch, args):
    client = FakeClient()
    monkeypatch.setattr(autorotate, "get_client", lambda: client)
    return client, CliRunner().invoke(autorotate.app, args)


def test_status_prints_machine_readable_pool_capacity(monkeypatch):
    client, result = invoke(monkeypatch, ["status"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"total": 5, "available": 3, "limited": 1}
    assert client.calls == [("status",)]


def test_help_has_no_token_options():
    result = CliRunner().invoke(autorotate.app, ["--help"])

    assert result.exit_code == 0
    assert "token" not in result.stdout.lower()
