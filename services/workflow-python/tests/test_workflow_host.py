from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def load_workflow_host():
    module_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
    sys.path.insert(0, str(module_path.parent))
    spec = importlib.util.spec_from_file_location("workflow_host_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeRpc:
    def __init__(self) -> None:
        self.drained = False

    async def drain_notifications(self) -> None:
        self.drained = True


class RequestRpc(FakeRpc):
    def __init__(self) -> None:
        super().__init__()
        self.requests = []

    async def request(self, payload):
        self.requests.append(payload)
        message_type = payload["type"]
        if message_type == "ctx.step.get":
            return {"done": False, "checkpoint_name": "checkpoint-1"}
        if message_type == "ctx.step.put":
            return payload["value"]
        if message_type == "ctx.call_tool":
            return {
                "tool": payload["tool"],
                "method": payload["method"],
                "args": payload["args"],
                "via": "rpc",
            }
        if message_type == "ctx.agent_turn":
            return payload["args"]
        if message_type == "ctx.workflow.start":
            return {
                "workflow_name": payload["workflow_name"],
                "task_id": "task-child",
                "run_id": "run-child",
                "created": True,
            }
        if message_type == "ctx.sleep":
            return {"slept": True}
        if message_type == "ctx.event.wait":
            return {"approved": True}
        raise AssertionError(f"unexpected request {payload}")


class NotificationRpc(FakeRpc):
    def __init__(self) -> None:
        super().__init__()
        self.notifications = []

    def notify(self, payload) -> None:
        self.notifications.append(payload)


@dataclasses.dataclass
class NestedInputForTest:
    name: str


@dataclasses.dataclass
class WorkflowInputForTest:
    nested: NestedInputForTest
    limit: int = 10


class WorkflowHostTests(unittest.TestCase):
    def test_workflow_api_modules_are_importable(self) -> None:
        load_workflow_host()

        from api.runtime_control import ControlPlaneError, canonical_json, decode_jsonb
        from api.workflow_engine import Delivery, WorkflowContext

        self.assertEqual(canonical_json({"b": 1, "a": 2}), '{"a":2,"b":1}')
        self.assertEqual(decode_jsonb('{"ok": true}', {}), {"ok": True})
        self.assertEqual(Delivery().metadata, {})
        self.assertTrue(WorkflowContext)

        error = ControlPlaneError("INVALID", "bad input", 422)
        self.assertEqual(error.to_dict()["status_code"], 422)
        self.assertIn("INVALID", str(error))

    def test_step_accepts_step_kind_and_binds_tool_manager_rpc(self) -> None:
        host = load_workflow_host()
        from api import app as workflow_app

        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        async def run_step():
            async def call_tool():
                manager = workflow_app.get_tool_manager()
                return await manager.call_tool_raw("demo", "method", {"x": 1})

            return await ctx.step("call_tool", call_tool, step_kind="tool_call")

        with patch.object(workflow_app, "resolve_tool_shim", return_value=None):
            result = asyncio.run(run_step())

        self.assertEqual(result["via"], "rpc")
        self.assertEqual(rpc.requests[0]["type"], "ctx.step.get")
        self.assertEqual(rpc.requests[0]["step_kind"], "tool_call")
        self.assertEqual(rpc.requests[-1]["type"], "ctx.step.put")
        self.assertEqual(rpc.requests[-1]["step_kind"], "tool_call")

    def test_sleep_sends_duration_seconds(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        asyncio.run(ctx.sleep("pause", 2.5))

        self.assertEqual(
            rpc.requests,
            [{"type": "ctx.sleep", "step": "pause", "duration_seconds": 2.5}],
        )

    def test_wait_for_event_sends_durable_event_identity_and_timeout(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        result = asyncio.run(
            ctx.wait_for_event("approval", "review", "change:42", timeout=30)
        )

        self.assertEqual(result, {"approved": True})
        self.assertEqual(
            rpc.requests,
            [
                {
                    "type": "ctx.event.wait",
                    "step": "approval",
                    "event_type": "review",
                    "correlation_id": "change:42",
                    "timeout_seconds": 30.0,
                }
            ],
        )

    def test_tools_proxy_calls_tool_manager(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        async def call_tool():
            return await ctx.tools.demo.method(x=1)

        from api import app as workflow_app

        with patch.object(workflow_app, "resolve_tool_shim", return_value=None):
            result = asyncio.run(call_tool())

        self.assertEqual(
            result,
            {"tool": "demo", "method": "method", "args": {"x": 1}, "via": "rpc"},
        )

    def test_run_agent_accepts_positional_step_name_with_text(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        result = asyncio.run(ctx.run_agent("draft_summary", text="summarize this"))

        self.assertEqual(result, {"name": "draft_summary", "text": "summarize this"})

    def test_agent_turn_applies_workflow_agent_defaults(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
            agent_defaults={"model": "claude-opus-4-8", "reasoning": "high"},
        )

        result = asyncio.run(ctx.agent_turn("do the thing"))

        self.assertEqual(
            result,
            {"model": "claude-opus-4-8", "reasoning": "high", "text": "do the thing"},
        )

    def test_agent_turn_per_call_kwargs_override_agent_defaults(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
            agent_defaults={"model": "claude-opus-4-8", "reasoning": "high"},
        )

        result = asyncio.run(ctx.agent_turn("cheap step", reasoning="low"))

        self.assertEqual(
            result,
            {"model": "claude-opus-4-8", "reasoning": "low", "text": "cheap step"},
        )

    def test_start_workflow_enqueues_durable_child_with_idempotency_key(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()
        ctx = host.WorkflowContext(
            rpc,
            run_id="run-123",
            task_id="task-456",
            workflow_name="sample",
        )

        result = asyncio.run(
            ctx.start_workflow(
                "company_context_documents",
                {"scope": "slack_thread"},
                idempotency_key="company-context:slack-thread:42",
            )
        )

        self.assertEqual(result["task_id"], "task-child")
        self.assertEqual(
            rpc.requests,
            [
                {
                    "type": "ctx.workflow.start",
                    "workflow_name": "company_context_documents",
                    "input": {"scope": "slack_thread"},
                    "idempotency_key": "company-context:slack-thread:42",
                }
            ],
        )

    def test_create_pool_retries_transient_connection_failure(self) -> None:
        host = load_workflow_host()
        calls = []
        sleeps = []
        pool = FakePool()

        async def create_pool(database_url):
            calls.append(database_url)
            if len(calls) < 3:
                raise ConnectionRefusedError("postgres is still starting")
            return pool

        async def sleep(delay):
            sleeps.append(delay)

        fake_asyncpg = types.SimpleNamespace(create_pool=create_pool)

        with (
            patch.dict(os.environ, {"DATABASE_URL": "postgresql://example/db"}, clear=False),
            patch.dict(sys.modules, {"asyncpg": fake_asyncpg}),
            patch.object(host.asyncio, "sleep", sleep),
        ):
            result = asyncio.run(host.create_pool())

        self.assertIs(result, pool)
        self.assertEqual(calls, ["postgresql://example/db"] * 3)
        self.assertEqual(sleeps, [0.25, 0.5])

    def test_create_pool_raises_after_all_connection_attempts(self) -> None:
        host = load_workflow_host()
        calls = []
        sleeps = []

        async def create_pool(database_url):
            calls.append(database_url)
            raise ConnectionRefusedError("postgres is unavailable")

        async def sleep(delay):
            sleeps.append(delay)

        fake_asyncpg = types.SimpleNamespace(create_pool=create_pool)

        with (
            patch.dict(os.environ, {"DATABASE_URL": "postgresql://example/db"}, clear=False),
            patch.dict(sys.modules, {"asyncpg": fake_asyncpg}),
            patch.object(host.asyncio, "sleep", sleep),
        ):
            with self.assertRaisesRegex(ConnectionRefusedError, "postgres is unavailable"):
                asyncio.run(host.create_pool())

        self.assertEqual(calls, ["postgresql://example/db"] * 5)
        self.assertEqual(sleeps, [0.25, 0.5, 1.0, 2.0])

    def test_coerce_input_hydrates_nested_dataclasses(self) -> None:
        host = load_workflow_host()

        result = host.coerce_input(
            {"nested": {"name": "digest"}, "limit": 5, "ignored": True},
            WorkflowInputForTest,
        )

        self.assertEqual(
            result,
            WorkflowInputForTest(nested=NestedInputForTest(name="digest"), limit=5),
        )

    def test_discover_workflows_filters_allowlist_and_load_errors(self) -> None:
        host = load_workflow_host()
        with tempfile.TemporaryDirectory() as tmp:
            first_dir = Path(tmp) / "first"
            second_dir = Path(tmp) / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            (first_dir / "allowed.py").write_text(
                "WORKFLOW_NAME = 'allowed'\n"
                "def handler(inp, ctx):\n"
                "    return None\n"
            )
            (second_dir / "filtered.py").write_text(
                "WORKFLOW_NAME = 'filtered'\n"
                "def handler(inp, ctx):\n"
                "    return None\n"
            )
            (second_dir / "broken.py").write_text(
                "WORKFLOW_NAME = 'broken'\n"
                "raise RuntimeError('broken import')\n"
            )
            stderr = io.StringIO()
            with (
                patch.dict(
                    os.environ,
                    {
                        "WORKFLOW_DIRS": f"{first_dir}:{second_dir}",
                        "WORKFLOW_ENABLE_MODE": "allowlist",
                        "WORKFLOW_ALLOWED_NAMES": "allowed",
                    },
                    clear=False,
                ),
                contextlib.redirect_stderr(stderr),
            ):
                workflows = host.discover_workflows()

        self.assertEqual(set(workflows), {"allowed"})
        self.assertIn("workflow_load_error", stderr.getvalue())
        self.assertIn("broken.py", stderr.getvalue())

    def test_discover_workflows_rejects_duplicate_names(self) -> None:
        host = load_workflow_host()
        with tempfile.TemporaryDirectory() as tmp:
            first_dir = Path(tmp) / "first"
            second_dir = Path(tmp) / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            for directory in (first_dir, second_dir):
                (directory / "duplicate.py").write_text(
                    "WORKFLOW_NAME = 'duplicate'\n"
                    "def handler(inp, ctx):\n"
                    "    return None\n"
                )
            with patch.dict(
                os.environ,
                {"WORKFLOW_DIRS": f"{first_dir}:{second_dir}"},
                clear=False,
            ):
                with self.assertRaisesRegex(RuntimeError, "duplicate workflow name 'duplicate'"):
                    host.discover_workflows()

    def test_workflow_result_includes_grouping_identifiers(self) -> None:
        host = load_workflow_host()
        pool = FakePool()
        rpc = FakeRpc()

        async def handler(inp, ctx):
            self.assertEqual(inp, {"input": "value"})
            return {"ok": True, "seen_run_id": ctx.run_id}

        registered = host.RegisteredWorkflow(
            workflow_name="sample_workflow",
            source_path="workflows/sample.py",
            handler=handler,
            input_cls=None,
            webhooks=None,
            schedule=None,
        )

        async def create_pool():
            return pool

        with (
            patch.object(
                host,
                "discover_workflows",
                return_value={"sample_workflow": registered},
            ),
            patch.object(host, "create_pool", create_pool),
        ):
            payload = asyncio.run(
                host.run_workflow(
                    {
                        "type": "workflow.start",
                        "workflow_name": "sample_workflow",
                        "run_id": "run-123",
                        "task_id": "task-456",
                        "input": {"input": "value"},
                    },
                    rpc,
                )
            )

        self.assertEqual(
            payload,
            {
                "type": "workflow.result",
                "workflow_run_id": "run-123",
                "run_id": "run-123",
                "workflow_task_id": "task-456",
                "task_id": "task-456",
                "workflow_name": "sample_workflow",
                "result": {"ok": True, "seen_run_id": "run-123"},
            },
        )
        self.assertTrue(rpc.drained)
        self.assertTrue(pool.closed)

    def test_run_workflow_closes_pool_after_handler_failure(self) -> None:
        host = load_workflow_host()
        pool = FakePool()
        rpc = FakeRpc()

        async def handler(inp, ctx):
            raise RuntimeError("handler failed")

        registered = host.RegisteredWorkflow(
            workflow_name="sample_workflow",
            source_path="workflows/sample.py",
            handler=handler,
            input_cls=None,
            webhooks=None,
            schedule=None,
        )

        async def create_pool():
            return pool

        with (
            patch.object(
                host,
                "discover_workflows",
                return_value={"sample_workflow": registered},
            ),
            patch.object(host, "create_pool", create_pool),
        ):
            with self.assertRaisesRegex(RuntimeError, "handler failed"):
                asyncio.run(
                    host.run_workflow(
                        {
                            "type": "workflow.start",
                            "workflow_name": "sample_workflow",
                            "run_id": "run-123",
                            "task_id": "task-456",
                            "input": {},
                        },
                        rpc,
                    )
                )

        self.assertTrue(rpc.drained)
        self.assertTrue(pool.closed)

    def test_run_workflow_drains_notifications_before_returning_result(self) -> None:
        host = load_workflow_host()
        rpc = NotificationRpc()

        async def handler(inp, ctx):
            ctx.log("daily_digest_started", recipient_count=3)
            return {"ok": True}

        registered = host.RegisteredWorkflow(
            workflow_name="sample_workflow",
            source_path="workflows/sample.py",
            handler=handler,
            input_cls=None,
            webhooks=None,
            schedule=None,
        )

        async def create_pool():
            return None

        with (
            patch.object(
                host,
                "discover_workflows",
                return_value={"sample_workflow": registered},
            ),
            patch.object(host, "create_pool", create_pool),
        ):
            result = asyncio.run(
                host.run_workflow(
                    {
                        "type": "workflow.start",
                        "workflow_name": "sample_workflow",
                        "run_id": "run-123",
                        "task_id": "task-456",
                        "input": {},
                    },
                    rpc,
                )
            )

        self.assertEqual(result["result"], {"ok": True})
        self.assertEqual(
            rpc.notifications,
            [
                {
                    "type": "ctx.log",
                    "message": "daily_digest_started",
                    "fields": {"recipient_count": 3},
                }
            ],
        )
        self.assertTrue(rpc.drained)

    def test_rpc_rejects_response_for_unknown_request(self) -> None:
        host = load_workflow_host()
        rpc = host.RpcClient()

        with self.assertRaisesRegex(host.ProtocolError, "unknown request_id 'missing'"):
            rpc.resolve({"type": "ctx.response", "request_id": "missing", "ok": True})

    def test_run_workflow_threads_agent_defaults_into_context(self) -> None:
        host = load_workflow_host()
        rpc = RequestRpc()

        async def handler(inp, ctx):
            return await ctx.agent_turn("do the thing")

        registered = host.RegisteredWorkflow(
            workflow_name="sample_workflow",
            source_path="workflows/sample.py",
            handler=handler,
            input_cls=None,
            webhooks=None,
            schedule=None,
            agent_defaults={"model": "claude-opus-4-8", "reasoning": "high"},
        )

        async def create_pool():
            return None

        with (
            patch.object(
                host,
                "discover_workflows",
                return_value={"sample_workflow": registered},
            ),
            patch.object(host, "create_pool", create_pool),
        ):
            payload = asyncio.run(
                host.run_workflow(
                    {
                        "type": "workflow.start",
                        "workflow_name": "sample_workflow",
                        "run_id": "run-123",
                        "task_id": "task-456",
                        "input": {},
                    },
                    rpc,
                )
            )

        self.assertEqual(
            payload["result"],
            {"model": "claude-opus-4-8", "reasoning": "high", "text": "do the thing"},
        )

    def test_load_workflow_file_reads_agent_defaults(self) -> None:
        host = load_workflow_host()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "defaults_workflow.py"
            path.write_text(
                "WORKFLOW_NAME = 'defaults_workflow'\n"
                "AGENT_DEFAULTS = {'model': 'claude-opus-4-8', 'reasoning': 'high'}\n"
                "def handler(inp, ctx):\n"
                "    return None\n"
            )
            registered = host.load_workflow_file(path)

        assert registered is not None
        self.assertEqual(
            registered.agent_defaults,
            {"model": "claude-opus-4-8", "reasoning": "high"},
        )

    def test_load_workflow_file_reads_workflow_principal(self) -> None:
        host = load_workflow_host()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "principal_workflow.py"
            path.write_text(
                "WORKFLOW_NAME = 'principal_workflow'\n"
                "WORKFLOW_PRINCIPAL = True\n"
                "def handler(inp, ctx):\n"
                "    return None\n"
            )
            registered = host.load_workflow_file(path)

        assert registered is not None
        self.assertEqual(host.normalize_principal(registered), True)

    def test_failed_workflow_host_exits_even_when_stdin_remains_open(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        with tempfile.TemporaryDirectory() as tmp:
            workflow_path = Path(tmp) / "failing_workflow.py"
            workflow_path.write_text(
                "WORKFLOW_NAME = 'failing_workflow'\n"
                "async def handler(inp, ctx):\n"
                "    raise RuntimeError('boom')\n"
            )

            env = os.environ.copy()
            env["WORKFLOW_DIRS"] = tmp
            proc = subprocess.Popen(
                [sys.executable, str(host_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            try:
                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "workflow.start",
                            "run_id": "run-123",
                            "task_id": "task-456",
                            "workflow_name": "failing_workflow",
                            "input": {},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()

                line = proc.stdout.readline()
                self.assertTrue(line, "workflow host did not emit a response")
                payload = json.loads(line)
                self.assertEqual(payload["type"], "workflow.error")
                self.assertEqual(payload["message"], "boom")

                proc.wait(timeout=2)
                self.assertEqual(proc.returncode, 0)
                assert proc.stderr is not None
                self.assertEqual(proc.stderr.read(), "")
            finally:
                if proc.poll() is None:
                    proc.kill()
                if proc.stdin is not None and not proc.stdin.closed:
                    proc.communicate(timeout=2)
                else:
                    proc.wait(timeout=2)
                    proc.stdout.close()
                    proc.stderr.close()

    def test_workflow_host_returns_result_after_context_response(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        with tempfile.TemporaryDirectory() as tmp:
            workflow_path = Path(tmp) / "agent_workflow.py"
            workflow_path.write_text(
                "WORKFLOW_NAME = 'agent_workflow'\n"
                "async def handler(inp, ctx):\n"
                "    result = await ctx.agent_turn('summarize this')\n"
                "    return {'agent_result': result}\n"
            )

            env = os.environ.copy()
            env["WORKFLOW_DIRS"] = tmp
            proc = subprocess.Popen(
                [sys.executable, str(host_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            assert proc.stderr is not None
            try:
                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "workflow.start",
                            "run_id": "run-123",
                            "task_id": "task-456",
                            "workflow_name": "agent_workflow",
                            "input": {},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()

                request_line = proc.stdout.readline()
                self.assertTrue(request_line, "workflow host did not request an agent turn")
                request = json.loads(request_line)
                self.assertEqual(request["type"], "ctx.agent_turn")

                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "ctx.response",
                            "request_id": request["request_id"],
                            "ok": True,
                            "value": {"text": "daily digest"},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()

                result_line = proc.stdout.readline()
                self.assertTrue(result_line, "workflow host did not emit a result")
                result = json.loads(result_line)
                self.assertEqual(result["type"], "workflow.result")
                self.assertEqual(result["result"], {"agent_result": {"text": "daily digest"}})

                proc.wait(timeout=2)
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(proc.stderr.read(), "")
            finally:
                if proc.poll() is None:
                    proc.kill()
                if proc.stdin is not None and not proc.stdin.closed:
                    proc.communicate(timeout=2)
                else:
                    proc.wait(timeout=2)
                    proc.stdout.close()
                    proc.stderr.close()

    def test_workflow_host_rejects_malformed_input(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        proc = subprocess.Popen(
            [sys.executable, str(host_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdin is not None
        assert proc.stdout is not None
        assert proc.stderr is not None
        try:
            proc.stdin.write("this is not JSON\n")
            proc.stdin.flush()

            line = proc.stdout.readline()
            self.assertTrue(line, "workflow host did not reject malformed input")
            payload = json.loads(line)
            self.assertEqual(payload["type"], "host.error")
            self.assertIn("invalid workflow host input", payload["message"])

            proc.wait(timeout=2)
            self.assertEqual(proc.returncode, 1)
            self.assertEqual(proc.stderr.read(), "")
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.communicate(timeout=2)

    def test_workflow_host_returns_workflow_error_for_failed_context_response(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        with tempfile.TemporaryDirectory() as tmp:
            workflow_path = Path(tmp) / "agent_workflow.py"
            workflow_path.write_text(
                "WORKFLOW_NAME = 'agent_workflow'\n"
                "async def handler(inp, ctx):\n"
                "    return await ctx.agent_turn('summarize this')\n"
            )
            proc = subprocess.Popen(
                [sys.executable, str(host_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "WORKFLOW_DIRS": tmp},
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            assert proc.stderr is not None
            try:
                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "workflow.start",
                            "run_id": "run-123",
                            "task_id": "task-456",
                            "workflow_name": "agent_workflow",
                            "input": {},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()
                request = json.loads(proc.stdout.readline())
                self.assertEqual(request["type"], "ctx.agent_turn")

                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "ctx.response",
                            "request_id": request["request_id"],
                            "ok": False,
                            "error": "agent unavailable",
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()

                response = json.loads(proc.stdout.readline())
                self.assertEqual(response["type"], "workflow.error")
                self.assertEqual(response["message"], "agent unavailable")
                proc.wait(timeout=2)
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(proc.stderr.read(), "")
            finally:
                if proc.poll() is None:
                    proc.kill()
                if proc.stdin is not None and not proc.stdin.closed:
                    proc.stdin.close()
                proc.wait(timeout=2)
                proc.stdout.close()
                proc.stderr.close()

    def test_workflow_host_rejects_concurrent_start(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        with tempfile.TemporaryDirectory() as tmp:
            workflow_path = Path(tmp) / "agent_workflow.py"
            workflow_path.write_text(
                "WORKFLOW_NAME = 'agent_workflow'\n"
                "async def handler(inp, ctx):\n"
                "    return await ctx.agent_turn('summarize this')\n"
            )
            proc = subprocess.Popen(
                [sys.executable, str(host_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "WORKFLOW_DIRS": tmp},
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            assert proc.stderr is not None
            start = {
                "type": "workflow.start",
                "run_id": "run-123",
                "task_id": "task-456",
                "workflow_name": "agent_workflow",
                "input": {},
            }
            try:
                proc.stdin.write(json.dumps(start, separators=(",", ":")) + "\n")
                proc.stdin.flush()
                request = json.loads(proc.stdout.readline())
                self.assertEqual(request["type"], "ctx.agent_turn")

                proc.stdin.write(json.dumps(start, separators=(",", ":")) + "\n")
                proc.stdin.flush()
                rejection = json.loads(proc.stdout.readline())
                self.assertEqual(rejection["type"], "workflow.error")
                self.assertEqual(rejection["message"], "workflow host already has an active workflow")

                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "ctx.response",
                            "request_id": request["request_id"],
                            "ok": True,
                            "value": {"text": "done"},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()
                result = json.loads(proc.stdout.readline())
                self.assertEqual(result["type"], "workflow.result")
                proc.wait(timeout=2)
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(proc.stderr.read(), "")
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.communicate(timeout=2)

    def test_workflow_host_finishes_active_workflow_after_stdin_eof(self) -> None:
        host_path = Path(__file__).resolve().parents[1] / "workflow_host.py"
        with tempfile.TemporaryDirectory() as tmp:
            workflow_path = Path(tmp) / "slow_workflow.py"
            workflow_path.write_text(
                "import asyncio\n"
                "WORKFLOW_NAME = 'slow_workflow'\n"
                "async def handler(inp, ctx):\n"
                "    await asyncio.sleep(0.05)\n"
                "    return {'done': True}\n"
            )
            proc = subprocess.Popen(
                [sys.executable, str(host_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "WORKFLOW_DIRS": tmp},
            )
            assert proc.stdin is not None
            assert proc.stdout is not None
            assert proc.stderr is not None
            try:
                proc.stdin.write(
                    json.dumps(
                        {
                            "type": "workflow.start",
                            "run_id": "run-123",
                            "task_id": "task-456",
                            "workflow_name": "slow_workflow",
                            "input": {},
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                proc.stdin.flush()
                proc.stdin.close()

                result = json.loads(proc.stdout.readline())
                self.assertEqual(result["type"], "workflow.result")
                self.assertEqual(result["result"], {"done": True})
                proc.wait(timeout=2)
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(proc.stderr.read(), "")
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=2)
                proc.stdout.close()
                proc.stderr.close()
if __name__ == "__main__":
    unittest.main()
