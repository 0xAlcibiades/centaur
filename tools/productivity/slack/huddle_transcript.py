"""The verbatim transcript of a recorded Slack huddle.

Slack records every huddle and saves the full, speaker-attributed transcript as a
``huddle_transcript`` file. That file is never shared into a channel, so **a bot token cannot
download it, and there is no public API for it** — Slack support confirms this is deliberate.
``files.info`` on the id returns metadata and no words, whatever scopes the bot holds. So the richest
record a company produces about its own decisions — the standup where the priority was set, the
incident call where the cause was found — is the one thing an agent in that workspace cannot read.

The Slack *web client* reads it perfectly well. It calls ``files.info`` on the **workspace host**
(``<workspace>.slack.com``, never ``slack.com``) with ``include_transcription=true``, authenticated by
a user web session: an ``xoxc`` token paired with the ``d`` cookie. This module replays exactly that
call. Nothing here is a scope trick or a permission bypass — a user session sees precisely what that
user could already read by scrolling the huddle in their own Slack client.

Two halves, deliberately separate, because they fail in completely different ways:

* **Discovery** — which huddles exist, and which transcript file belongs to each — runs on the
  ordinary bot token and is stable. It never needs the session.
* **Fetch** — the words themselves — is the session-bound half. When a session lapses it fails
  **loudly** (``needs_reauth``), never by returning an empty transcript. That distinction is the whole
  ballgame: a silent empty read looks exactly like "nobody said anything", and an agent that quietly
  believes a meeting was silent is worse than one that admits it cannot see.

The parsing below is pure and unit-tested against real Slack payloads; the network call is a thin
shell around it.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request

# Current payloads expose canonical ``lines`` entries. Older workspaces also emitted rich-text
# blocks, where Slack marks each spoken turn with a bold ` [mm:ss]: ` between the speaker and words.
_STAMP = re.compile(r"\[(\d{1,2}:\d{2}(?::\d{2})?)\]")

_PAGE = 1000  # turns per page; long huddles paginate
_MAX_PAGES = (
    60  # ~60k turns — a backstop against a pathological response, never hit by a real huddle
)

# The failures that mean "the human must sign in again", as opposed to "this code is wrong". Only
# these set ``needs_reauth``; everything else is a bug and should read like one.
_REAUTH_ERRORS = frozenset(
    {"not_authed", "invalid_auth", "token_revoked", "token_expired", "account_inactive"}
)


class HuddleTranscriptError(RuntimeError):
    """A structured failure that survives stringification through the tool boundary.

    ``needs_reauth=True`` marks the one failure that is not a defect: the user web session expired,
    and only a human signing in again can restore it. Callers should surface that verbatim rather than
    treating it as an empty result.
    """

    def __init__(self, message: str, **detail: object) -> None:
        self.payload = {"error": "huddle_transcript_failed", "message": message, **detail}
        super().__init__(str(self.payload))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A fixed credentialed API call must not carry its headers through a redirect."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


# ── parsing (pure) ──────────────────────────────────────────────────────────────────────────────


def _transcription(files_info: dict) -> dict | None:
    file = files_info.get("file")
    if not isinstance(file, dict):
        return None
    transcription = file.get("huddle_transcription")
    return transcription if isinstance(transcription, dict) else None


def _format_timestamp(value: object) -> str | None:
    if isinstance(value, str):
        candidate = value.strip().strip("[]")
        if re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", candidate):
            return candidate
    if isinstance(value, bool):
        return None
    try:
        milliseconds = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if milliseconds < 0:
        return None
    seconds = milliseconds // 1000
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _line_segments(transcription: dict) -> list[dict]:
    lines = transcription.get("lines")
    if not isinstance(lines, list):
        return []
    out: list[dict] = []
    for line in lines:
        if not isinstance(line, dict):
            raise HuddleTranscriptError("Slack returned a malformed huddle transcript line")
        text = line.get("contents", line.get("text"))
        if text is None:
            continue
        if not isinstance(text, str):
            raise HuddleTranscriptError("Slack returned non-text huddle transcript contents")
        said = text.strip()
        if not said:
            continue
        user_id = line.get("user_id", line.get("speaker_id"))
        if not isinstance(user_id, str) or not user_id:
            user_id = None
        at = _format_timestamp(line.get("start_time_ms", line.get("start_time")))
        out.append({"user_id": user_id, "at": at, "text": said})
    return out


def _rich_text_sections(value: object) -> list[dict]:
    """Normalize both direct and list-wrapped Slack rich-text block shapes."""
    if isinstance(value, list):
        out: list[dict] = []
        for item in value:
            out.extend(_rich_text_sections(item))
        return out
    if not isinstance(value, dict):
        return []
    if value.get("type") == "rich_text_section":
        return [value]
    return _rich_text_sections(value.get("elements"))


def _raw_turn_count(files_info: dict) -> int | None:
    """Count unfiltered Slack entries so noise/empty turns cannot end pagination early."""
    transcription = _transcription(files_info)
    if transcription is None:
        return None
    lines = transcription.get("lines")
    if isinstance(lines, list) and lines:
        return len(lines)
    blocks = transcription.get("blocks")
    if isinstance(lines, list) or isinstance(blocks, (dict, list)):
        return len(_rich_text_sections(blocks))
    return None


def parse_segments(files_info: dict) -> list[dict]:
    """The transcript as ordered turns: ``{"user_id", "at", "text"}``.

    Slack's canonical payload is ``huddle_transcription.lines[]`` with ``user_id``,
    ``start_time_ms``, and ``contents``. The rich-text representation emitted by older workspaces is
    retained as a fallback: each section carries a ``user`` element, a bold timestamp marker, and
    one or more text elements with the words.

    Speakers stay as **Slack ids, never names**. An id round-trips to a real mention and cannot drift;
    a name resolved at parse time silently rots when someone changes their display name.
    """
    transcription = _transcription(files_info)
    if transcription is None:
        return []
    line_segments = _line_segments(transcription)
    if line_segments:
        return line_segments

    out: list[dict] = []
    for section in _rich_text_sections(transcription.get("blocks")):
        user_id: str | None = None
        at: str | None = None
        words: list[str] = []
        elements = section.get("elements")
        if not isinstance(elements, list):
            continue
        for element in elements:
            if not isinstance(element, dict):
                continue
            if element.get("type") == "user":
                candidate = element.get("user_id")
                user_id = candidate if isinstance(candidate, str) else None
            elif element.get("type") == "text":
                text = element.get("text", "")
                if not isinstance(text, str):
                    raise HuddleTranscriptError("Slack returned non-text rich transcript contents")
                stamp = _STAMP.search(text)
                # The bold element that *is* a timestamp is the marker, not speech. A bold word inside
                # the speech itself carries no [mm:ss] and must survive.
                if stamp and (element.get("style") or {}).get("bold"):
                    at = stamp.group(1)
                else:
                    words.append(text)
        said = "".join(words).strip()
        if said:
            out.append({"user_id": user_id, "at": at, "text": said})
    return out


def render(segments: list[dict]) -> str:
    """Turns → a readable transcript, one line each: ``<@U…> [mm:ss]: words``.

    The ``<@U…>`` form is what Slack renders as a real mention, so a verbatim quote names the person
    instead of printing id soup.
    """
    lines = []
    for segment in segments:
        who = f"<@{segment['user_id']}>" if segment.get("user_id") else "someone"
        at = f" [{segment['at']}]" if segment.get("at") else ""
        lines.append(f"{who}{at}: {segment['text']}")
    return "\n".join(lines)


def speakers(segments: list[dict]) -> list[str]:
    """Distinct speaker ids in first-spoke order — an attendee list drawn from who actually *talked*,
    which is not the same as who Slack listed as present."""
    seen: list[str] = []
    for segment in segments:
        uid = segment.get("user_id")
        if uid and uid not in seen:
            seen.append(uid)
    return seen


# ── the session-bound fetch (thin shell) ────────────────────────────────────────────────────────


def _call(host: str, token: str, cookie: str, **params: str) -> dict:
    """One ``files.info`` call against the workspace host.

    The token rides in the ``Authorization`` header and the session in ``Cookie``, and that choice is
    the whole security story. Slack's web client posts the token as a **form field**, and it would be
    the obvious thing to copy — but a request body is the one place iron-proxy cannot look
    (``match_headers`` / ``match_query`` / ``match_path``, and there is no ``match_body``). Copying
    the browser would therefore force a real user web session to sit inside the sandbox, which is
    precisely the exposure the injection model exists to prevent.

    Verified against the live API before choosing: Slack accepts this token in the form body, in the
    query string, **and** in the ``Authorization`` header — all three authenticate. Since they are
    equivalent to Slack, take the one the firewall can reach. Both credentials are then declared as
    header-injected secrets, the sandbox holds only placeholders, and the real session never leaves
    the proxy.
    """
    url = f"https://{host}/api/files.info?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url,
        data=b"",  # Slack wants a POST; the parameters ride in the query string
        headers={"Authorization": f"Bearer {token}", "Cookie": cookie},
    )
    try:
        with _OPENER.open(request, timeout=60) as response:
            body = json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            body = json.load(exc)
        except (UnicodeDecodeError, ValueError):
            raise HuddleTranscriptError(
                "Slack returned an HTTP error", status_code=exc.code
            ) from exc
        if not isinstance(body, dict) or body.get("ok"):
            raise HuddleTranscriptError(
                "Slack returned an HTTP error", status_code=exc.code
            ) from exc
    except urllib.error.URLError as exc:  # network/TLS, not Slack saying no
        raise HuddleTranscriptError(f"could not reach {host}", cause=str(exc)) from exc
    except (UnicodeDecodeError, ValueError) as exc:
        raise HuddleTranscriptError("Slack returned invalid JSON") from exc

    if not isinstance(body, dict):
        raise HuddleTranscriptError("Slack returned a non-object response")

    if not body.get("ok"):
        error = body.get("error")
        if error in _REAUTH_ERRORS:
            raise HuddleTranscriptError(
                "the Slack web session expired — a human must sign in again and refresh the "
                "SLACK_WEB_TOKEN / SLACK_WEB_COOKIE pair; huddle transcripts stay unreadable until "
                "they do",
                slack_error=error,
                needs_reauth=True,
            )
        raise HuddleTranscriptError("Slack returned not-ok", slack_error=error)
    return body


def fetch(file_id: str, *, host: str, token: str, cookie: str) -> dict:
    """The full verbatim transcript for one ``huddle_transcript`` file id.

    Returns ``{"file_id", "speakers", "turns", "text"}``. Raises :class:`HuddleTranscriptError` with
    ``needs_reauth=True`` when the session has lapsed — the loud signal that keeps a stale cookie from
    ever reading as "this huddle had no transcript".

    Only the rendered ``text`` comes back, not the parsed segments as well. They are the same words
    twice, and this is the largest single payload the tool can produce: returning both writes the
    whole huddle into the model's context a second time, and it is then re-read on every request of
    the turn. Callers that need the structure can call :func:`parse_segments` themselves.
    """
    segments: list[dict] = []
    seen_pages: set[str] = set()
    for page in range(1, _MAX_PAGES + 1):
        body = _call(
            host,
            token,
            cookie,
            file=file_id,
            include_transcription="true",
            page=str(page),
            count=str(_PAGE),
        )
        transcription = _transcription(body)
        raw_count = _raw_turn_count(body)
        if transcription is None or raw_count is None:
            raise HuddleTranscriptError(
                "Slack returned no huddle transcription; the file may not be a transcript or it "
                "may not be ready",
                file_id=file_id,
            )
        fingerprint = json.dumps(transcription, sort_keys=True, separators=(",", ":"))
        if fingerprint in seen_pages:
            raise HuddleTranscriptError(
                "Slack transcript pagination did not advance", file_id=file_id, page=page
            )
        seen_pages.add(fingerprint)
        chunk = parse_segments(body)
        segments += chunk
        # Slack's undocumented transcript pagination uses the raw entry count. Parsed turns are the
        # wrong signal because valid pages can contain empty/noise entries that we intentionally drop.
        # A response larger than the requested page size means Slack returned the complete payload.
        if raw_count != _PAGE:
            break
    else:
        raise HuddleTranscriptError(
            "Slack transcript exceeded the pagination safety limit", file_id=file_id
        )
    if not segments:
        raise HuddleTranscriptError(
            "Slack returned an empty huddle transcription; it may not be ready",
            file_id=file_id,
        )
    return {
        "file_id": file_id,
        "speakers": speakers(segments),
        "turns": len(segments),
        "text": render(segments),
    }
