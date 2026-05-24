from __future__ import annotations

import importlib.util
from pathlib import Path
import tomllib
from types import ModuleType
import uuid


WRAPPER_PY = Path(__file__).resolve().parents[2] / "sandbox" / "codex-app-wrapper.py"


def _load_wrapper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("codex_app_wrapper", WRAPPER_PY)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_configure_laminar_otel_writes_startup_config(monkeypatch, tmp_path) -> None:
    wrapper = _load_wrapper()
    codex_home = tmp_path / ".codex"
    codex_home.mkdir()
    config_path = codex_home / "config.toml"
    config_path.write_text(
        """
model = "gpt-5.5"

[otel]
environment = "old"

[otel.exporter.otlp-http]
endpoint = "http://old/v1/logs"
protocol = "binary"

[projects."/home/agent/workspace"]
trust_level = "trusted"
""".lstrip()
    )

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("CENTAUR_TRACE_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("CENTAUR_THREAD_KEY", "warm-placeholder")
    monkeypatch.setenv("LMNR_BASE_URL", "http://laminar:8000")
    monkeypatch.setenv("LMNR_PROJECT_API_KEY", "lmnr-key")
    monkeypatch.setenv("CODEX_OTEL_ENVIRONMENT", "staging")

    wrapper.configure_laminar_otel_for_startup(
        "00000000-0000-0000-0000-000000000123",
        "slack:C123:1700000000.000100",
    )

    contents = config_path.read_text()
    parsed = tomllib.loads(contents)
    assert parsed["model"] == "gpt-5.5"
    assert parsed["projects"]["/home/agent/workspace"]["trust_level"] == "trusted"
    assert parsed["otel"]["environment"] == "staging"
    assert "exporter" not in parsed["otel"]
    assert (
        parsed["otel"]["trace_exporter"]["otlp-http"]["endpoint"]
        == "http://laminar:8000/v1/traces"
    )
    assert parsed["otel"]["trace_exporter"]["otlp-http"]["protocol"] == "binary"
    assert parsed["otel"]["trace_exporter"]["otlp-http"]["headers"] == {
        "x-trace-id": "00000000-0000-0000-0000-000000000123",
        "x-centaur-thread-key": "slack:C123:1700000000.000100",
        "authorization": "Bearer lmnr-key",
    }
    assert "v1/logs" not in contents


def test_configure_laminar_otel_sets_w3c_trace_context(monkeypatch, tmp_path) -> None:
    wrapper = _load_wrapper()
    codex_home = tmp_path / ".codex"
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("LMNR_BASE_URL", "http://laminar:8000")
    monkeypatch.setattr(
        wrapper.uuid,
        "uuid4",
        lambda: uuid.UUID("11111111-2222-3333-4444-555555555555"),
    )

    wrapper.CURRENT_TRACEPARENT = None
    wrapper.configure_laminar_otel_for_startup(
        "00000000-0000-4000-8000-000000000123",
        "slack:C123:1700000000.000100",
    )

    assert (
        wrapper.CURRENT_TRACEPARENT
        == "00-00000000000040008000000000000123-1111111122223333-01"
    )
    assert (
        wrapper.os.environ["TRACEPARENT"]
        == "00-00000000000040008000000000000123-1111111122223333-01"
    )


def test_configure_trace_context_ignores_invalid_trace_id(monkeypatch) -> None:
    wrapper = _load_wrapper()
    monkeypatch.delenv("TRACEPARENT", raising=False)

    wrapper.CURRENT_TRACEPARENT = None
    wrapper.configure_trace_context("not-a-trace")

    assert wrapper.CURRENT_TRACEPARENT is None
    assert "TRACEPARENT" not in wrapper.os.environ


def test_resume_uses_amp_var(monkeypatch) -> None:
    wrapper = _load_wrapper()
    monkeypatch.delenv("CODEX_CONTINUE_THREAD_ID", raising=False)
    monkeypatch.setenv("AMP_CONTINUE_THREAD_ID", "legacy-thread")
    monkeypatch.setattr(wrapper.os, "getcwd", lambda: "/workspace")
    calls = []

    def fake_request(method, params=None, timeout=30.0):
        calls.append((method, params, timeout))
        return {"thread": {"id": params["threadId"]}}

    monkeypatch.setattr(wrapper, "request", fake_request)
    wrapper.THREAD_ID = None

    assert wrapper.start_or_resume_thread() == "legacy-thread"
    assert calls == [
        (
            "thread/resume",
            {"threadId": "legacy-thread", "cwd": "/workspace"},
            60,
        )
    ]


def test_request_attaches_traceparent(monkeypatch) -> None:
    wrapper = _load_wrapper()
    sent: list[dict] = []
    monkeypatch.setattr(wrapper, "_next_id", lambda: 1)

    def fake_send_raw(payload: dict) -> None:
        sent.append(payload)
        wrapper.RESPONSES[1].put({"id": 1, "result": {"ok": True}})

    monkeypatch.setattr(wrapper, "send_raw", fake_send_raw)

    wrapper.CURRENT_TRACEPARENT = (
        "00-00000000000040008000000000000123-1111111122223333-01"
    )
    result = wrapper.request("thread/start", {"cwd": "/tmp"}, timeout=0.1)

    assert result == {"ok": True}
    assert sent == [
        {
            "id": 1,
            "method": "thread/start",
            "params": {"cwd": "/tmp"},
            "trace": {
                "traceparent": "00-00000000000040008000000000000123-1111111122223333-01"
            },
        }
    ]


def test_main_lazy_starts_app_server_after_input(monkeypatch) -> None:
    wrapper = _load_wrapper()
    requests: list[tuple[str, dict]] = []
    popen_args: list[str] = []
    emitted: list[dict] = []

    class FakeProcess:
        stdin = object()
        stdout = object()
        stderr = object()

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

    class FakeThread:
        def __init__(self, *args, **kwargs) -> None:
            self.target = kwargs.get("target")

        def start(self) -> None:
            if self.target == wrapper.api_stdin_reader:
                wrapper.INPUTS.put(
                    {
                        "type": "user",
                        "trace_id": "00000000-0000-0000-0000-000000000123",
                        "thread_key": "slack:C123:1700000000.000100",
                        "message": {"content": [{"type": "text", "text": "/goal ship"}]},
                    }
                )
                wrapper.INPUTS.put(None)

    def fake_request(method: str, params: dict, timeout: float = 30.0) -> dict:
        requests.append((method, params))
        if method == "initialize":
            return {"codexHome": "/tmp/.codex"}
        if method == "thread/start":
            return {"thread": {"id": "thread-123"}}
        return {}

    def fake_emit(msg: dict) -> None:
        emitted.append(msg)
        if msg.get("type") == "turn.completed":
            wrapper.SHUTTING_DOWN = True

    def fake_popen(args: list[str], *other_args, **kwargs) -> FakeProcess:
        popen_args.extend(args)
        return FakeProcess()

    monkeypatch.setattr(wrapper.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(wrapper.threading, "Thread", FakeThread)
    monkeypatch.setattr(wrapper, "request", fake_request)
    monkeypatch.setattr(wrapper, "notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(wrapper, "emit", fake_emit)
    monkeypatch.setattr(
        wrapper, "configure_laminar_otel_for_startup", lambda *_args, **_kwargs: None
    )
    wrapper.SHUTTING_DOWN = False
    wrapper.APP = None
    wrapper.APP_INITIALIZED = False
    wrapper.THREAD_ID = None
    while not wrapper.INPUTS.empty():
        wrapper.INPUTS.get_nowait()

    wrapper.main()

    assert popen_args == ["codex", "app-server", "--listen", "stdio://"]
    assert requests[0] == (
        "initialize",
        {
            "clientInfo": {
                "name": "centaur",
                "title": "Centaur",
                "version": "0.1.0",
            },
            "capabilities": {"experimentalApi": True},
        },
    )
    assert requests[1][0] == "thread/start"
    assert requests[2] == (
        "thread/goal/set",
        {"threadId": "thread-123", "objective": "ship"},
    )
    assert {"type": "thread.started", "thread_id": "thread-123"} in emitted
    assert {"type": "turn.completed"} in emitted


def test_handle_input_sets_codex_sandbox_policy(monkeypatch) -> None:
    wrapper = _load_wrapper()
    requests: list[tuple[str, dict]] = []

    def fake_request(method: str, params: dict, timeout: float = 30.0) -> dict:
        requests.append((method, params))
        if method == "turn/start":
            return {"turn": {"id": "turn-123"}}
        return {}

    monkeypatch.setattr(
        wrapper, "configure_laminar_otel_for_startup", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(wrapper, "start_app_server", lambda: None)
    monkeypatch.setattr(wrapper, "start_or_resume_thread", lambda: "thread-123")
    monkeypatch.setattr(wrapper, "request", fake_request)

    wrapper.ACTIVE_TURN_ID = None
    while not wrapper.EVENTS.empty():
        wrapper.EVENTS.get_nowait()
    wrapper.EVENTS.put({"method": "turn/completed", "params": {}})

    wrapper.handle_input(
        {
            "type": "user",
            "message": {"content": [{"type": "text", "text": "hello"}]},
        }
    )

    assert requests == [
        (
            "turn/start",
            {
                "threadId": "thread-123",
                "input": [{"type": "text", "text": "hello"}],
                "sandboxPolicy": {"type": "dangerFullAccess"},
            },
        )
    ]
