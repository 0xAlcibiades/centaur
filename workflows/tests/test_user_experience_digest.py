from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import json
import os
import sys
import types
from pathlib import Path

import httpx


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    api_module = sys.modules.get("api") or types.ModuleType("api")
    runtime_control = types.ModuleType("api.runtime_control")
    runtime_control.decode_jsonb = lambda value, default: (
        json.loads(value)
        if isinstance(value, str)
        else value
        if value is not None
        else default
    )
    workflow_engine = types.ModuleType("api.workflow_engine")
    workflow_engine.WorkflowContext = object
    api_module.runtime_control = runtime_control
    api_module.workflow_engine = workflow_engine
    sys.modules.setdefault("api", api_module)
    sys.modules["api.runtime_control"] = runtime_control
    sys.modules["api.workflow_engine"] = workflow_engine
    return importlib.import_module("workflows.user_experience_digest")


digest = _load_module()


def _result(**overrides):
    value = {
        "problem_detected": True,
        "experience": "bad",
        "severity": "high",
        "user_emotion": "frustrated",
        "agent_contribution": "likely",
        "confidence": 0.92,
        "failure_modes": ["repeated_failure"],
        "evidence_message_ids": ["message-1"],
        "summary": "The agent repeated an unsuccessful answer after a correction.",
    }
    value.update(overrides)
    return value


def test_message_text_keeps_safe_text_and_attachment_name_only():
    text = digest._message_text(
        [
            {"type": "text", "text": "Please inspect this"},
            {
                "type": "attachment",
                "name": "trace.txt",
                "dataBase64": "secret-payload",
            },
        ]
    )

    assert text == "Please inspect this\n[attachment: trace.txt]"
    assert "secret-payload" not in text


def test_validate_result_rejects_evidence_outside_transcript():
    value = _result(evidence_message_ids=["invented-message"])

    try:
        digest._validate_result(value, {"message-1"})
    except ValueError as error:
        assert "evidence_message_ids" in str(error)
    else:
        raise AssertionError("expected invalid evidence to be rejected")


def test_validate_result_requires_none_severity_for_non_problem():
    value = _result(problem_detected=False, experience="good")

    try:
        digest._validate_result(value, {"message-1"})
    except ValueError as error:
        assert "severity none" in str(error)
    else:
        raise AssertionError("expected inconsistent severity to be rejected")


def test_classify_uses_strict_schema_and_store_false():
    requests: list[dict] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"output_text": json.dumps(_result())})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            return await digest._classify(
                client,
                base_url="https://api.openai.test/v1",
                api_key="placeholder",
                model="small-model",
                max_output_tokens=500,
                thread_key="slack:C123:123.456",
                transcript=[
                    {
                        "message_id": "message-1",
                        "role": "user",
                        "created_at": "2026-08-04T12:00:00+00:00",
                        "text": "This still does not work.",
                    }
                ],
                execution_summary={"recent_executions": []},
            )

    result = asyncio.run(run())

    assert result["problem_detected"] is True
    assert requests[0]["store"] is False
    assert requests[0]["model"] == "small-model"
    assert requests[0]["text"]["format"]["strict"] is True
    assert requests[0]["text"]["format"]["schema"] == digest.CLASSIFIER_SCHEMA


def test_claim_reclaims_expired_running_rows():
    class FakePool:
        def __init__(self):
            self.execute_calls = []
            self.query = ""
            self.args = ()

        async def execute(self, query, *args):
            self.execute_calls.append((query, args))
            return "UPDATE 0"

        async def fetch(self, query, *args):
            self.query = query
            self.args = args
            return []

    pool = FakePool()
    asyncio.run(
        digest._claim_scans(
            pool,
            classifier_version="v1",
            model="small-model",
            limit=25,
            max_attempts=3,
            lease_minutes=30,
            run_id="run-1",
        )
    )

    assert "status = 'running'" in pool.query
    assert "updated_at <= NOW()" in pool.query
    assert pool.args == (3, "v1", "small-model", 25, 30, "run-1")
    assert "maximum attempts" in pool.execute_calls[0][0]
    assert pool.execute_calls[0][1] == (3, 30, "run-1")


def test_discovery_uses_scan_rows_as_snapshot_checkpoints():
    class FakePool:
        def __init__(self):
            self.fetch_query = ""
            self.fetch_args = ()
            self.execute_calls = []

        async def fetch(self, query, *args):
            self.fetch_query = query
            self.fetch_args = args
            return [
                {
                    "thread_key": "slack:C123:123.456",
                    "last_message_id": "message-1",
                    "last_message_created_at": dt.datetime(
                        2026, 8, 4, 12, tzinfo=dt.UTC
                    ),
                }
            ]

        async def execute(self, query, *args):
            self.execute_calls.append((query, args))
            return "INSERT 0 1"

    pool = FakePool()
    inserted = asyncio.run(
        digest._discover_candidates(
            pool,
            idle_minutes=60,
            include_direct_messages=False,
            classifier_version="v1",
            model="small-model",
            limit=100,
            run_id="run-1",
        )
    )

    assert inserted == 1
    assert "user_experience_scans existing" in pool.fetch_query
    assert "metadata ->> 'platform'" in pool.fetch_query
    assert "D[^:]*:" in pool.fetch_query
    assert pool.fetch_args == (60, False, "v1", "small-model", 100)
    assert "ON CONFLICT (thread_key, last_message_id" in pool.execute_calls[0][0]


def test_format_digest_lists_problem_threads_without_raw_evidence():
    rows = [
        {
            "thread_key": "slack:T1:C123:123.456",
            "status": "completed",
            "problem_detected": True,
            "severity": "high",
            "confidence": 0.9,
            "failure_modes": ["wrong_answer"],
            "summary": "The response remained incorrect after a correction.",
        },
        {
            "thread_key": "linear:ISSUE-1",
            "status": "completed",
            "problem_detected": False,
            "severity": "none",
            "confidence": 0.8,
            "failure_modes": [],
            "summary": "No problem detected.",
        },
    ]

    report = digest._format_digest(rows, "small-model")

    assert "problems *1*" in report
    assert "https://slack.com/archives/C123/p123456" in report
    assert "wrong_answer" in report
    assert "No problem detected" not in report


def test_handler_discovers_classifies_and_posts_report(monkeypatch):
    scan = digest.Scan(
        scan_id="scan-1",
        thread_key="slack:C123:123.456",
        last_message_id="message-1",
        last_message_created_at=dt.datetime(2026, 8, 4, 12, tzinfo=dt.UTC),
        model="small-model",
        classifier_version="v1",
    )

    async def discover(*_args, **_kwargs):
        return 1

    async def claim(*_args, **_kwargs):
        return [scan]

    async def process(*_args, **_kwargs):
        return {"completed": 1, "failed": 0, "superseded": 0}

    async def results(*_args, **_kwargs):
        return [
            {
                "thread_key": scan.thread_key,
                "status": "completed",
                "problem_detected": True,
                "severity": "high",
                "confidence": 0.9,
                "failure_modes": ["wrong_answer"],
                "summary": "The answer was wrong.",
            }
        ]

    monkeypatch.setattr(digest, "_discover_candidates", discover)
    monkeypatch.setattr(digest, "_claim_scans", claim)
    monkeypatch.setattr(digest, "_process_scans", process)
    monkeypatch.setattr(digest, "_load_run_results", results)
    monkeypatch.setattr(
        digest,
        "_config",
        lambda: {
            "base_url": "https://api.openai.test/v1",
            "model": "small-model",
            "classifier_version": "v1",
            "batch_size": 10,
            "idle_minutes": 60,
            "max_messages": 40,
            "max_attempts": 3,
            "max_output_tokens": 500,
            "timeout_seconds": 20,
            "concurrency": 2,
            "lease_minutes": 30,
            "include_direct_messages": False,
            "slack_channel": "C-REPORT",
        },
    )
    monkeypatch.setenv("OPENAI_API_KEY", "placeholder")

    class FakeContext:
        run_id = "run-1"
        _pool = object()

        def __init__(self):
            self.posts = []
            self.logs = []

        async def step(self, _name, fn):
            return await fn()

        async def post_to_slack(self, channel, text, **kwargs):
            self.posts.append((channel, text, kwargs))
            return {"sent": True}

        def log(self, event, **fields):
            self.logs.append((event, fields))

    context = FakeContext()
    output = asyncio.run(digest.handler(digest.Input(), context))

    assert output["discovered"] == 1
    assert output["completed"] == 1
    assert context.posts[0][0] == "C-REPORT"
    assert "problems *1*" in context.posts[0][1]
    assert context.posts[0][2]["client_msg_id"].startswith("user-experience-digest:")
    assert context.logs[0][0] == "user_experience_digest_completed"


def teardown_module():
    os.environ.pop("OPENAI_API_KEY", None)
