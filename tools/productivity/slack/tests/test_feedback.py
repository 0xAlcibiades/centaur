from __future__ import annotations

from datetime import datetime, timezone

from slack import feedback


def _sample_item(*, category: str = "cli_bug", severity: str = "high") -> feedback.FeedbackItem:
    now = datetime.now(timezone.utc).isoformat()
    return feedback.FeedbackItem(
        id=None,
        slack_channel="test-bot",
        slack_thread_ts=f"12345.{category}",
        permalink="https://slack.com/archives/C123/p12345",
        amp_thread_id=None,
        category=category,
        severity=severity,
        summary=f"{category} summary",
        cli_involved="slack",
        evidence={"bot_error": category == "cli_bug"},
        reporter_user="alice",
        status="new",
        created_at=now,
        updated_at=now,
    )


def test_analyze_thread_signals_does_not_treat_exceptional_as_error():
    messages = [
        {"user": "arjun", "text": "@centaur_ai --invest are L2s still investable or cooked"},
        {
            "user": "centaur_ai",
            "bot_id": "B123",
            "text": "This is an exceptional business with strong unit economics.",
        },
    ]

    signals = feedback.analyze_thread_signals(messages)

    assert signals.has_bot_error is False


def test_classify_feedback_prefers_success_for_positive_follow_up_without_error():
    messages = [
        {"user": "arjun", "text": "@centaur_ai --invest dig into this co"},
        {
            "user": "centaur_ai",
            "bot_id": "B123",
            "text": "This is an exceptional manufacturing business with strong margins.",
            "reactions": [{"name": "thumbsup"}],
        },
        {"user": "arjun", "text": "better than the vanilla thread we got before imo"},
        {"user": "arjun", "text": "could be tighter"},
        {"user": "arjun", "text": "synthesis seems better to me"},
    ]

    signals = feedback.analyze_thread_signals(messages)
    category, severity = feedback.classify_feedback(signals, messages)

    assert signals.repeated_requests is True
    assert signals.has_bot_error is False
    assert category == "success"
    assert severity == "low"
