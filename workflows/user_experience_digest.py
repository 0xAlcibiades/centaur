"""Classify completed conversation snapshots and post a daily experience digest.

Each ``user_experience_scans`` row is both a durable work item and the result for
one exact thread snapshot. A new final message or classifier/model version creates
a new row, so scheduled runs do not need a separate global watermark.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from api.runtime_control import decode_jsonb
from api.workflow_engine import WorkflowContext

if TYPE_CHECKING:
    import httpx

WORKFLOW_NAME = "user_experience_digest"

DEFAULT_MODEL = "gpt-5.4-nano"
DEFAULT_CLASSIFIER_VERSION = "v1"
DEFAULT_BATCH_SIZE = 100
DEFAULT_IDLE_MINUTES = 60
DEFAULT_MAX_MESSAGES = 40
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_MAX_OUTPUT_TOKENS = 500
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_CONCURRENCY = 5
DEFAULT_LEASE_MINUTES = 30
FALSE_ENV_VALUES = {"0", "false", "no", "off"}
SLACK_THREAD_KEY_RE = re.compile(
    r"^slack:(?:(?P<team>[^:]+):)?(?P<channel>[CDG][^:]*):(?P<thread_ts>[^:]+)$"
)

LABELS = {"good", "mixed", "bad", "unknown"}

CLASSIFIER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "label": {"type": "string", "enum": sorted(LABELS)},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "evidence_message_ids": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 5,
            "uniqueItems": True,
        },
        "summary": {"type": "string", "maxLength": 1000},
    },
    "required": [
        "label",
        "confidence",
        "evidence_message_ids",
        "summary",
    ],
}

SYSTEM_PROMPT = """Classify the user's experience with an AI agent using one label:
- good: no material agent-caused problem is evident.
- mixed: the agent caused meaningful friction, but the interaction retained value
  or substantially recovered.
- bad: a clear, significant agent-caused or agent-amplified failure was unresolved.
- unknown: the transcript is insufficient to judge.

Judge the interaction, not merely negative emotion about an external problem.
Repeated corrections, ignored instructions, wrong answers, failed tools, timeouts,
missing responses, and unhelpful tone are evidence. Operational failure may make an
experience mixed or bad without an explicit complaint. Do not use mixed or bad
solely because the user describes an upsetting situation. Cite only provided
message IDs, keep the summary factual and under two sentences, and do not quote
secrets or personal data."""


def _positive_int(value: int | str | None, default: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in FALSE_ENV_VALUES


SCHEDULE = {
    "schedule_id": WORKFLOW_NAME,
    "cron": os.getenv("USER_EXPERIENCE_DIGEST_CRON", "0 8 * * *"),
    "timezone": os.getenv("USER_EXPERIENCE_DIGEST_TIMEZONE", "America/Los_Angeles"),
    "enabled": _env_flag("USER_EXPERIENCE_DIGEST_ENABLED"),
    "no_delivery": True,
}


@dataclass
class Input:
    limit: int | None = None
    slack_channel: str | None = None
    post_report: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Scan:
    scan_id: str
    thread_key: str
    last_message_id: str
    last_message_created_at: dt.datetime
    model: str
    classifier_version: str


def _config() -> dict[str, Any]:
    return {
        "base_url": os.getenv(
            "USER_EXPERIENCE_DIGEST_OPENAI_BASE_URL", "https://api.openai.com/v1"
        ).rstrip("/"),
        "model": os.getenv("USER_EXPERIENCE_DIGEST_MODEL", DEFAULT_MODEL).strip()
        or DEFAULT_MODEL,
        "classifier_version": os.getenv(
            "USER_EXPERIENCE_DIGEST_CLASSIFIER_VERSION", DEFAULT_CLASSIFIER_VERSION
        ).strip()
        or DEFAULT_CLASSIFIER_VERSION,
        "batch_size": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_BATCH_SIZE"), DEFAULT_BATCH_SIZE
        ),
        "idle_minutes": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_IDLE_MINUTES"), DEFAULT_IDLE_MINUTES
        ),
        "max_messages": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_MAX_MESSAGES"), DEFAULT_MAX_MESSAGES
        ),
        "max_attempts": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_MAX_ATTEMPTS"), DEFAULT_MAX_ATTEMPTS
        ),
        "max_output_tokens": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_MAX_OUTPUT_TOKENS"),
            DEFAULT_MAX_OUTPUT_TOKENS,
        ),
        "timeout_seconds": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_TIMEOUT_SECONDS"),
            DEFAULT_TIMEOUT_SECONDS,
        ),
        "concurrency": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_CONCURRENCY"), DEFAULT_CONCURRENCY
        ),
        "lease_minutes": _positive_int(
            os.getenv("USER_EXPERIENCE_DIGEST_LEASE_MINUTES"), DEFAULT_LEASE_MINUTES
        ),
        "include_direct_messages": _env_flag(
            "USER_EXPERIENCE_DIGEST_INCLUDE_DIRECT_MESSAGES"
        ),
        "slack_channel": os.getenv("USER_EXPERIENCE_DIGEST_SLACK_CHANNEL", "").strip(),
    }


async def _discover_candidates(
    pool: Any,
    *,
    idle_minutes: int,
    include_direct_messages: bool,
    classifier_version: str,
    model: str,
    limit: int,
    run_id: str,
) -> int:
    rows = await pool.fetch(
        """
        SELECT s.thread_key,
               latest.message_id AS last_message_id,
               latest.created_at AS last_message_created_at
        FROM sessions s
        JOIN LATERAL (
            SELECT m.message_id, m.created_at
            FROM session_messages m
            WHERE m.thread_key = s.thread_key
            ORDER BY m.created_at DESC, m.message_id DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE latest.created_at <= NOW() - ($1::bigint * INTERVAL '1 minute')
          AND COALESCE(s.metadata ->> 'platform', '') = 'slack'
          AND EXISTS (
              SELECT 1 FROM session_messages user_message
              WHERE user_message.thread_key = s.thread_key
                AND user_message.role = 'user'
                AND user_message.metadata @> '{
                    "platform": "slack",
                    "is_mention": true
                }'::jsonb
          )
          AND NOT EXISTS (
              SELECT 1 FROM session_executions active
              WHERE active.thread_key = s.thread_key
                AND active.status IN ('queued', 'running')
          )
          AND ($2::boolean OR s.thread_key !~ '^slack:([^:]+:)?D[^:]*:')
          AND NOT EXISTS (
              SELECT 1 FROM user_experience_scans existing
              WHERE existing.thread_key = s.thread_key
                AND (
                    existing.status = 'baseline'
                    OR (
                        existing.last_message_id = latest.message_id
                        AND existing.classifier_version = $3
                        AND existing.model = $4
                    )
                )
          )
        ORDER BY latest.created_at, latest.message_id
        LIMIT $5
        """,
        idle_minutes,
        include_direct_messages,
        classifier_version,
        model,
        limit,
    )
    inserted = 0
    for row in rows:
        status = await pool.execute(
            """
            INSERT INTO user_experience_scans (
                scan_id, thread_key, last_message_id, last_message_created_at,
                classifier_version, model, status, workflow_run_id, eligible_after
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7,
                    $4 + ($8::bigint * INTERVAL '1 minute'))
            ON CONFLICT (thread_key, last_message_id, classifier_version, model)
            DO NOTHING
            """,
            f"uxs_{uuid.uuid4().hex}",
            str(row["thread_key"]),
            str(row["last_message_id"]),
            row["last_message_created_at"],
            classifier_version,
            model,
            run_id,
            idle_minutes,
        )
        inserted += int(status.endswith(" 1"))
    return inserted


async def _claim_scans(
    pool: Any,
    *,
    classifier_version: str,
    model: str,
    limit: int,
    max_attempts: int,
    lease_minutes: int,
    run_id: str,
) -> list[Scan]:
    await pool.execute(
        """
        UPDATE user_experience_scans
        SET status = 'failed',
            last_error = 'classification lease expired after maximum attempts',
            workflow_run_id = $3,
            updated_at = NOW()
        WHERE status = 'running'
          AND updated_at <= NOW() - ($2::bigint * INTERVAL '1 minute')
          AND attempt_count >= $1
        """,
        max_attempts,
        lease_minutes,
        run_id,
    )
    rows = await pool.fetch(
        """
        WITH claimable AS (
            SELECT scan_id
            FROM user_experience_scans
            WHERE (
                    (status IN ('pending', 'failed') AND eligible_after <= NOW())
                    OR (status = 'running'
                        AND updated_at <= NOW() - ($5::bigint * INTERVAL '1 minute'))
                  )
              AND attempt_count < $1
              AND classifier_version = $2
              AND model = $3
            ORDER BY eligible_after, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $4
        )
        UPDATE user_experience_scans scans
        SET status = 'running',
            attempt_count = scans.attempt_count + 1,
            last_error = '',
            workflow_run_id = $6,
            updated_at = NOW()
        FROM claimable
        WHERE scans.scan_id = claimable.scan_id
        RETURNING scans.scan_id, scans.thread_key, scans.last_message_id,
                  scans.last_message_created_at, scans.model,
                  scans.classifier_version
        """,
        max_attempts,
        classifier_version,
        model,
        limit,
        lease_minutes,
        run_id,
    )
    return [Scan(**dict(row)) for row in rows]


async def _snapshot_is_current(pool: Any, scan: Scan) -> bool:
    row = await pool.fetchrow(
        """
        SELECT latest.message_id,
               EXISTS (
                   SELECT 1 FROM session_executions active
                   WHERE active.thread_key = $1
                     AND active.status IN ('queued', 'running')
               ) AS has_active_execution
        FROM LATERAL (
            SELECT message_id
            FROM session_messages
            WHERE thread_key = $1
            ORDER BY created_at DESC, message_id DESC
            LIMIT 1
        ) latest
        """,
        scan.thread_key,
    )
    return bool(
        row
        and str(row["message_id"]) == scan.last_message_id
        and not row["has_active_execution"]
    )


async def _mark_superseded(pool: Any, scan_id: str) -> None:
    await pool.execute(
        """
        UPDATE user_experience_scans
        SET status = 'superseded', updated_at = NOW()
        WHERE scan_id = $1 AND status = 'running'
        """,
        scan_id,
    )


def _message_text(parts: Any) -> str:
    decoded = decode_jsonb(parts, [])
    if not isinstance(decoded, list):
        return ""
    texts: list[str] = []
    for part in decoded:
        if not isinstance(part, dict):
            continue
        if part.get("type") == "text" and isinstance(part.get("text"), str):
            text = part["text"].strip()
            if text:
                texts.append(text)
        elif part.get("type") == "attachment":
            name = part.get("name") or part.get("filename")
            if isinstance(name, str) and name.strip():
                texts.append(f"[attachment: {name.strip()}]")
    return "\n".join(texts)[:4000]


async def _load_transcript(
    pool: Any, scan: Scan, max_messages: int
) -> list[dict[str, str]]:
    rows = await pool.fetch(
        """
        SELECT message_id, role, parts, created_at
        FROM session_messages
        WHERE thread_key = $1
          AND (created_at, message_id) <= ($2, $3)
        ORDER BY created_at DESC, message_id DESC
        LIMIT $4
        """,
        scan.thread_key,
        scan.last_message_created_at,
        scan.last_message_id,
        max_messages,
    )
    transcript = [
        {
            "message_id": str(row["message_id"]),
            "role": str(row["role"]),
            "created_at": row["created_at"].isoformat(),
            "text": _message_text(row["parts"]),
        }
        for row in reversed(rows)
    ]
    return [message for message in transcript if message["text"]]


async def _execution_summary(pool: Any, thread_key: str) -> dict[str, Any]:
    rows = await pool.fetch(
        """
        SELECT status, started_at, completed_at, error IS NOT NULL AS has_error
        FROM session_executions
        WHERE thread_key = $1
        ORDER BY created_at DESC
        LIMIT 5
        """,
        thread_key,
    )
    return {
        "recent_executions": [
            {
                "status": str(row["status"]),
                "has_error": bool(row["has_error"]),
                "duration_seconds": (
                    max((row["completed_at"] - row["started_at"]).total_seconds(), 0)
                    if row["started_at"] and row["completed_at"]
                    else None
                ),
            }
            for row in rows
        ]
    }


def _response_output_text(value: Any) -> str:
    if isinstance(value, dict) and isinstance(value.get("output_text"), str):
        return value["output_text"]
    parts: list[str] = []
    if not isinstance(value, dict) or not isinstance(value.get("output"), list):
        return ""
    for item in value["output"]:
        if not isinstance(item, dict) or not isinstance(item.get("content"), list):
            continue
        for content in item["content"]:
            if isinstance(content, dict) and isinstance(content.get("text"), str):
                parts.append(content["text"])
    return " ".join(parts)


async def _classify(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    model: str,
    max_output_tokens: int,
    thread_key: str,
    transcript: list[dict[str, str]],
    execution_summary: dict[str, Any],
) -> dict[str, Any]:
    response = await client.post(
        f"{base_url}/responses",
        headers={"authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "instructions": SYSTEM_PROMPT,
            "input": json.dumps(
                {
                    "thread_key": thread_key,
                    "transcript": transcript,
                    **execution_summary,
                },
                separators=(",", ":"),
            ),
            "max_output_tokens": max_output_tokens,
            "reasoning": {"effort": "none"},
            "store": False,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "user_experience_scan",
                    "strict": True,
                    "schema": CLASSIFIER_SCHEMA,
                }
            },
        },
    )
    if not response.is_success:
        raise RuntimeError(f"OpenAI Responses API returned HTTP {response.status_code}")
    value = response.json()
    if isinstance(value, dict) and value.get("incomplete_details"):
        raise RuntimeError("OpenAI Responses API returned an incomplete response")
    output = _response_output_text(value)
    if not output:
        raise RuntimeError("OpenAI Responses API returned no output text")
    parsed = json.loads(output)
    if not isinstance(parsed, dict):
        raise TypeError("classifier output must be a JSON object")
    return parsed


def _validate_result(result: dict[str, Any], message_ids: set[str]) -> dict[str, Any]:
    if result.get("label") not in LABELS:
        raise ValueError("invalid label")
    confidence = result.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise TypeError("confidence must be numeric")
    if not 0 <= float(confidence) <= 1:
        raise ValueError("confidence must be between zero and one")
    evidence = result.get("evidence_message_ids")
    if not isinstance(evidence, list) or any(
        item not in message_ids for item in evidence
    ):
        raise ValueError("evidence_message_ids must reference the supplied transcript")
    summary = result.get("summary")
    if not isinstance(summary, str) or len(summary) > 1000:
        raise ValueError("summary must be a string of at most 1000 characters")
    return result


async def _complete_scan(pool: Any, scan_id: str, result: dict[str, Any]) -> None:
    await pool.execute(
        """
        UPDATE user_experience_scans
        SET status = 'completed',
            label = $2,
            confidence = $3,
            evidence_message_ids = $4,
            summary = $5,
            result = $6::jsonb,
            last_error = '',
            completed_at = NOW(),
            updated_at = NOW()
        WHERE scan_id = $1 AND status = 'running'
        """,
        scan_id,
        result["label"],
        float(result["confidence"]),
        list(dict.fromkeys(result["evidence_message_ids"])),
        result["summary"],
        json.dumps(result, separators=(",", ":")),
    )


def _safe_error(error: Exception) -> str:
    return f"{type(error).__name__}: {error}".replace("\n", " ")[:1000]


async def _fail_scan(pool: Any, scan_id: str, error: Exception) -> None:
    await pool.execute(
        """
        UPDATE user_experience_scans
        SET status = 'failed', last_error = $2, updated_at = NOW()
        WHERE scan_id = $1 AND status = 'running'
        """,
        scan_id,
        _safe_error(error),
    )


async def _process_scan(
    pool: Any,
    client: httpx.AsyncClient,
    scan: Scan,
    config: dict[str, Any],
    api_key: str,
) -> str:
    try:
        if not await _snapshot_is_current(pool, scan):
            await _mark_superseded(pool, scan.scan_id)
            return "superseded"
        transcript = await _load_transcript(pool, scan, config["max_messages"])
        execution_summary = await _execution_summary(pool, scan.thread_key)
        result = await _classify(
            client,
            base_url=config["base_url"],
            api_key=api_key,
            model=scan.model,
            max_output_tokens=config["max_output_tokens"],
            thread_key=scan.thread_key,
            transcript=transcript,
            execution_summary=execution_summary,
        )
        validated = _validate_result(
            result, {message["message_id"] for message in transcript}
        )
        await _complete_scan(pool, scan.scan_id, validated)
        return "completed"
    # A single malformed transcript, database race, or provider failure must not
    # prevent the remaining claimed scans from reaching a terminal state.
    except Exception as error:  # noqa: BLE001
        await _fail_scan(pool, scan.scan_id, error)
        return "failed"


async def _process_scans(
    pool: Any,
    scans: list[Scan],
    config: dict[str, Any],
    api_key: str,
) -> dict[str, int]:
    import httpx

    counts = {"completed": 0, "failed": 0, "superseded": 0}
    semaphore = asyncio.Semaphore(config["concurrency"])
    async with httpx.AsyncClient(
        timeout=config["timeout_seconds"], trust_env=True
    ) as client:

        async def run(scan: Scan) -> str:
            async with semaphore:
                return await _process_scan(pool, client, scan, config, api_key)

        for status in await asyncio.gather(*(run(scan) for scan in scans)):
            counts[status] += 1
    return counts


async def _load_run_results(pool: Any, run_id: str) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        SELECT thread_key, status, label, confidence, summary, model, created_at,
               completed_at
        FROM user_experience_scans
        WHERE workflow_run_id = $1
        ORDER BY completed_at NULLS LAST, created_at
        """,
        run_id,
    )
    return [dict(row) for row in rows]


def _thread_reference(thread_key: str) -> str:
    match = SLACK_THREAD_KEY_RE.match(thread_key)
    if not match:
        return f"`{thread_key}`"
    channel = match.group("channel")
    thread_ts = match.group("thread_ts").replace(".", "")
    return f"<https://slack.com/archives/{channel}/p{thread_ts}|thread>"


def _format_digest(rows: list[dict[str, Any]], model: str) -> str:
    completed = [row for row in rows if row["status"] == "completed"]
    problems = [row for row in completed if row["label"] in {"mixed", "bad"}]
    failed = sum(row["status"] == "failed" for row in rows)
    superseded = sum(row["status"] == "superseded" for row in rows)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    lines = [
        f"*Daily user experience scan — {today}*",
        (
            f"Scanned *{len(completed)}* thread snapshots with `{model}` · "
            f"problems *{len(problems)}* · failed *{failed}* · superseded *{superseded}*"
        ),
    ]
    if not problems:
        if completed:
            lines.append("No problematic experiences were detected in this run.")
        elif failed:
            lines.append("No classifications completed; inspect the failed scan rows.")
        else:
            lines.append("No eligible thread snapshots were available in this run.")
        return "\n\n".join(lines)
    problems.sort(
        key=lambda row: (
            0 if row["label"] == "bad" else 1,
            -float(row["confidence"] or 0),
        )
    )
    lines.append(
        "Labels: "
        + " · ".join(
            f"{label} *{sum(row['label'] == label for row in problems)}*"
            for label in ("bad", "mixed")
            if any(row["label"] == label for row in problems)
        )
    )
    lines.append("*Highest-priority threads*")
    for row in problems[:10]:
        summary = str(row["summary"] or "No summary supplied").replace("\n", " ")
        lines.append(
            f"• *{str(row['label']).upper()}* {_thread_reference(str(row['thread_key']))} "
            f"— {summary}"
        )
    if len(problems) > 10:
        lines.append(f"…and {len(problems) - 10} more problematic threads.")
    return "\n".join(lines)


async def handler(inp: Input, ctx: WorkflowContext) -> dict[str, Any]:
    if ctx._pool is None:
        raise RuntimeError("user experience digest requires DATABASE_URL")
    config = _config()
    limit = _positive_int(inp.limit, config["batch_size"])
    discovered = await ctx.step(
        "discover_thread_snapshots",
        lambda: _discover_candidates(
            ctx._pool,
            idle_minutes=config["idle_minutes"],
            include_direct_messages=config["include_direct_messages"],
            classifier_version=config["classifier_version"],
            model=config["model"],
            limit=limit,
            run_id=ctx.run_id,
        ),
    )
    scans = await _claim_scans(
        ctx._pool,
        classifier_version=config["classifier_version"],
        model=config["model"],
        limit=limit,
        max_attempts=config["max_attempts"],
        lease_minutes=config["lease_minutes"],
        run_id=ctx.run_id,
    )
    if scans:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            error = RuntimeError("OPENAI_API_KEY is not configured")
            for scan in scans:
                await _fail_scan(ctx._pool, scan.scan_id, error)
            counts = {"completed": 0, "failed": len(scans), "superseded": 0}
        else:
            counts = await _process_scans(ctx._pool, scans, config, api_key)
    else:
        counts = {"completed": 0, "failed": 0, "superseded": 0}
    rows = await _load_run_results(ctx._pool, ctx.run_id)
    channel = (inp.slack_channel or config["slack_channel"]).strip()
    if inp.post_report and channel:
        digest_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
        slack = await ctx.post_to_slack(
            channel,
            _format_digest(rows, config["model"]),
            client_msg_id=(
                f"user-experience-digest:{digest_date}:"
                f"{config['classifier_version']}:{config['model']}"
            ),
        )
    else:
        slack = {"sent": False, "reason": "report_disabled_or_no_channel"}
    ctx.log(
        "user_experience_digest_completed",
        discovered=discovered,
        claimed=len(scans),
        **counts,
    )
    return {
        "discovered": discovered,
        "claimed": len(scans),
        **counts,
        "model": config["model"],
        "classifier_version": config["classifier_version"],
        "slack": slack,
    }
