//! Hermes Agent harness — drives `hermes-agent`'s `tui_gateway` JSON-RPC
//! stdio wire as a first-class Centaur harness.
//!
//! Unlike claude/amp (spawn-per-turn stream-json CLIs), Hermes ships a
//! long-lived JSON-RPC gateway (`python -m tui_gateway.entry`) that owns the
//! agent loop, durable session store, skills, persistent memory, background
//! self-improvement reviews, and the cron scheduler. This runtime therefore
//! mirrors the codex runtime shape: one persistent child per sandbox, a
//! handshake (`gateway.ready` → `session.create`), then one `prompt.submit`
//! per turn with events pumped into the shared `CodexTurnNormalizer`.
//!
//! What "first-class" means here (vs. driving `hermes -z` one-shots):
//! - **Session continuity**: one Hermes session per Centaur thread; the
//!   prompt cache and conversation history survive across turns, and
//!   `HERMES_CONTINUE_SESSION_ID` resumes the durable session after a
//!   sandbox restart.
//! - **Learning loop**: the background memory/skill review forks Hermes
//!   spawns after turns run inside the same process and write to the
//!   sandbox-mounted HERMES_HOME, so memories and skills accumulate.
//! - **Crons**: Hermes cron jobs created during a conversation are ticked
//!   by this runtime (`hermes cron tick` every `HERMES_CRON_TICK_SECONDS`,
//!   cross-process file-locked on Hermes's side), so scheduled work fires
//!   for as long as the sandbox lives.
//! - **Interrupts**: Centaur's blocks `interrupt` maps to Hermes's
//!   `session.interrupt` RPC — the turn ends as Interrupted without
//!   killing the child, preserving the session for the next turn.
//!
//! Wire mapping (Hermes event → NormalizedEvent):
//! - `message.delta {text}`         → AgentTextDelta
//! - `reasoning.delta`/`thinking.delta {text}` → ReasoningTextDelta
//! - `tool.start {tool_id,name,args}`  → AssistantMessage(ToolUse)
//! - `tool.complete {tool_id,result}`  → ToolResults
//! - `message.complete {text,status}`  → AssistantMessage(final) + Result

use std::env;
use std::io::{self, BufRead, Write};
use std::process::{Child, ChildStdin, Command as ProcessCommand, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use codex_app_server_protocol::UserInput;
use serde_json::{Value, json};

use crate::server::{BlocksCommand, BlocksState, parse_blocks_line_with_state, write_blocks_error};
use crate::traits::{
    NormalizedContent, NormalizedEvent, NormalizedTokenUsage, NormalizedToolResult,
};
use crate::turn::{BridgeConfig, CodexTurnNormalizer};
use crate::util::write_value;
use crate::{HarnessServerError, Result};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(180);
const DEFAULT_CRON_TICK_SECONDS: u64 = 60;

/// Entry point for `harness-server hermes`.
pub fn run_hermes_blocks_server() -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut hermes: Option<HermesChild> = None;
    let (command_tx, command_rx) = mpsc::channel();
    let (interrupt_tx, interrupt_rx) = mpsc::channel();

    spawn_cron_ticker();

    thread::spawn(move || {
        let stdin = io::stdin();
        let mut blocks_state = BlocksState::default();
        for raw in stdin.lock().lines() {
            let Ok(line) = raw else { break };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match parse_blocks_line_with_state(trimmed, &mut blocks_state) {
                Ok(BlocksCommand::Interrupt) => {
                    if interrupt_tx.send(()).is_err() {
                        break;
                    }
                }
                Ok(command) => {
                    if command_tx.send(Ok(command)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    if command_tx.send(Err(error.to_string())).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut turn_counter = 0u64;
    while let Ok(input) = command_rx.recv() {
        match input {
            Ok(BlocksCommand::User {
                input,
                client_user_message_id,
                model,
                provider: _,
                reasoning,
                trace_context: _,
            }) => {
                turn_counter += 1;
                let result = (|| -> Result<()> {
                    if hermes.is_none() {
                        hermes = Some(HermesChild::start(model.clone())?);
                    }
                    let child = hermes.as_mut().expect("hermes started");
                    run_hermes_turn(
                        child,
                        &mut stdout,
                        input,
                        client_user_message_id,
                        reasoning,
                        turn_counter,
                        &interrupt_rx,
                    )
                })();
                if let Err(error) = result {
                    let thread_id = hermes
                        .as_ref()
                        .map(|child| child.thread_id())
                        .unwrap_or("hermes");
                    eprintln!("Hermes blocks turn failed: {error:#}");
                    write_blocks_error(&mut stdout, thread_id, "turn", error.to_string())?;
                    // A dead child cannot serve the next turn; drop it so the
                    // next user message restarts Hermes and resumes the durable
                    // session via HERMES_CONTINUE_SESSION_ID.
                    if hermes.as_mut().is_some_and(|c| !c.is_alive()) {
                        hermes = None;
                    }
                }
            }
            Ok(BlocksCommand::Interrupt) => {
                eprintln!("Hermes blocks interrupt ignored: no active turn runs");
            }
            Ok(BlocksCommand::AttachmentChunk) => {}
            Err(error) => {
                eprintln!("invalid Hermes blocks input: {error}");
                let thread_id = hermes
                    .as_ref()
                    .map(|child| child.thread_id())
                    .unwrap_or("hermes");
                write_blocks_error(&mut stdout, thread_id, "input", error)?;
            }
        }
        // Drain interrupts that arrived between turns so a stale one cannot
        // instantly cancel the next turn.
        while interrupt_rx.try_recv().is_ok() {}
    }
    Ok(())
}

/// Tick `hermes cron tick` on an interval so cron jobs created inside the
/// conversation fire while the sandbox lives. Hermes serializes ticks
/// cross-process with a file lock, so this is safe alongside any other
/// Hermes process on the same HERMES_HOME. `HERMES_CRON_TICK_SECONDS=0`
/// disables the ticker.
fn spawn_cron_ticker() {
    let interval = env::var("HERMES_CRON_TICK_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CRON_TICK_SECONDS);
    if interval == 0 {
        return;
    }
    let bin = hermes_bin();
    thread::spawn(move || {
        let mut warned = false;
        loop {
            thread::sleep(Duration::from_secs(interval));
            let status = ProcessCommand::new(&bin)
                .args(["cron", "tick"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if let Err(error) = status
                && !warned
            {
                eprintln!("hermes cron ticker disabled: {bin} cron tick failed: {error}");
                warned = true;
            }
        }
    });
}

fn hermes_bin() -> String {
    env::var("HERMES_BIN").unwrap_or_else(|_| "hermes".to_string())
}

fn hermes_gateway_command() -> ProcessCommand {
    // The JSON-RPC gateway is a Python module, not a `hermes` subcommand.
    // HERMES_PYTHON overrides the interpreter (sandbox venvs).
    let python = env::var("HERMES_PYTHON").unwrap_or_else(|_| "python3".to_string());
    let mut command = ProcessCommand::new(python);
    command.args(["-m", "tui_gateway.entry"]);
    command.env("HERMES_QUIET", "1");
    // Centaur owns approval policy at the sandbox boundary (iron-proxy egress,
    // placeholder credentials); inside the sandbox Hermes runs unattended.
    command.env(
        "HERMES_APPROVAL_MODE",
        env::var("HERMES_APPROVAL_MODE").unwrap_or_else(|_| "off".to_string()),
    );
    command
}

struct HermesChild {
    child: Child,
    stdin: ChildStdin,
    stdout: Receiver<io::Result<String>>,
    session_id: String,
    next_rpc_id: i64,
}

impl Drop for HermesChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl HermesChild {
    fn start(model: Option<String>) -> Result<Self> {
        let mut child = hermes_gateway_command()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| HarnessServerError::SpawnHarness {
                cwd: env::current_dir().unwrap_or_default(),
                source,
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or(HarnessServerError::HarnessStdinUnavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HarnessServerError::HarnessStdoutUnavailable)?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or(HarnessServerError::HarnessStderrUnavailable)?;
        thread::spawn(move || {
            let mut parent_stderr = io::stderr();
            let _ = io::copy(&mut stderr, &mut parent_stderr);
        });
        let (stdout_tx, stdout_rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = io::BufReader::new(stdout);
            for raw in reader.lines() {
                let should_stop = raw.is_err();
                if stdout_tx.send(raw).is_err() || should_stop {
                    break;
                }
            }
        });

        let mut this = Self {
            child,
            stdin,
            stdout: stdout_rx,
            session_id: String::new(),
            next_rpc_id: 0,
        };
        this.wait_for_gateway_ready()?;
        this.create_or_resume_session(model)?;
        Ok(this)
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    fn thread_id(&self) -> &str {
        if self.session_id.is_empty() {
            "hermes"
        } else {
            &self.session_id
        }
    }

    fn wait_for_gateway_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        loop {
            let frame = self.read_frame_until(deadline)?;
            if frame.get("method").and_then(Value::as_str) == Some("event")
                && frame.pointer("/params/type").and_then(Value::as_str) == Some("gateway.ready")
            {
                return Ok(());
            }
        }
    }

    /// Create the thread's Hermes session, or resume the durable one after a
    /// sandbox restart (`HERMES_CONTINUE_SESSION_ID`, persisted by the
    /// session runtime from our `thread.started` output — mirrors
    /// CODEX_CONTINUE_THREAD_ID).
    fn create_or_resume_session(&mut self, model: Option<String>) -> Result<()> {
        let resume = env::var("HERMES_CONTINUE_SESSION_ID").unwrap_or_default();
        let resume = resume.trim();
        if !resume.is_empty()
            && let Ok(result) =
                self.rpc("session.resume", json!({"session_id": resume, "cols": 200}))
            && let Some(sid) = result.get("session_id").and_then(Value::as_str)
        {
            self.session_id = sid.to_string();
            return Ok(());
        }

        let mut params = json!({
            "cols": 200,
            "cwd": env::current_dir()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            "title": "Centaur thread",
            "source": "centaur",
        });
        if let Some(model) = model.filter(|value| !value.trim().is_empty()) {
            params["model"] = Value::String(model);
        }
        let result = self.rpc("session.create", params)?;
        self.session_id = result
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                HarnessServerError::Protocol(
                    "session.create response missing session_id".to_string(),
                )
            })?
            .to_string();
        Ok(())
    }

    fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next_rpc_id += 1;
        let id = self.next_rpc_id;
        self.write_frame(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))?;
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        loop {
            let frame = self.read_frame_until(deadline)?;
            if frame.get("id").and_then(Value::as_i64) == Some(id) {
                if let Some(error) = frame.get("error") {
                    return Err(HarnessServerError::Protocol(format!(
                        "hermes {method} failed: {error}"
                    )));
                }
                return Ok(frame.get("result").cloned().unwrap_or(Value::Null));
            }
            // Events and stale responses between request and response are
            // dropped here; per-turn event pumping happens in run_hermes_turn.
        }
    }

    fn write_frame(&mut self, value: &Value) -> Result<()> {
        serde_json::to_writer(&mut self.stdin, value)?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn read_frame_until(&mut self, deadline: Instant) -> Result<Value> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(HarnessServerError::Protocol(
                    "timed out waiting for hermes gateway".to_string(),
                ));
            }
            match self.stdout.recv_timeout(deadline - now) {
                Ok(line) => {
                    let line = line?;
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(value) => return Ok(value),
                        // Hermes keeps its own stdout clean of non-JSON in
                        // quiet mode; tolerate stray lines anyway.
                        Err(_) => continue,
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    return Err(HarnessServerError::Protocol(
                        "timed out waiting for hermes gateway".to_string(),
                    ));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let status = self.child.wait()?;
                    return Err(HarnessServerError::HarnessExited {
                        kind: crate::HarnessKind::Codex,
                        status,
                        stderr: String::new(),
                    });
                }
            }
        }
    }
}

/// Per-turn translation state: Hermes emits deltas + one final
/// `message.complete` whose text REPEATS the streamed content, so the final
/// text must be reconciled (suffix-delta) rather than double-emitted —
/// `CodexTurnNormalizer::emit_agent_text` already handles that when the final
/// AssistantMessage carries the full canonical text.
#[derive(Debug, Default)]
pub struct HermesEventState {
    text_item_id: Option<String>,
    saw_error: Option<String>,
}

/// Translate one Hermes gateway event frame into normalized events.
/// Returns `(events, terminal)`.
pub fn normalize_hermes_frame(
    state: &mut HermesEventState,
    turn: u64,
    frame: &Value,
) -> (Vec<NormalizedEvent>, bool) {
    if frame.get("method").and_then(Value::as_str) != Some("event") {
        return (Vec::new(), false);
    }
    let params = frame.get("params").cloned().unwrap_or_default();
    let kind = params.get("type").and_then(Value::as_str).unwrap_or("");
    let payload = params.get("payload").cloned().unwrap_or_default();

    let text_item = state
        .text_item_id
        .get_or_insert_with(|| format!("hermes-msg-{turn}"))
        .clone();

    match kind {
        "message.delta" => {
            let delta = payload.get("text").and_then(Value::as_str).unwrap_or("");
            if delta.is_empty() {
                return (Vec::new(), false);
            }
            (
                vec![NormalizedEvent::AgentTextDelta {
                    item_id: text_item,
                    delta: delta.to_string(),
                }],
                false,
            )
        }
        "reasoning.delta" | "thinking.delta" => {
            let delta = payload.get("text").and_then(Value::as_str).unwrap_or("");
            if delta.is_empty() {
                return (Vec::new(), false);
            }
            (
                vec![NormalizedEvent::ReasoningTextDelta {
                    item_id: format!("hermes-reasoning-{turn}"),
                    delta: delta.to_string(),
                }],
                false,
            )
        }
        "tool.start" => {
            let tool_id = payload
                .get("tool_id")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            let arguments = payload.get("args").cloned().unwrap_or(json!({}));
            (
                vec![NormalizedEvent::AssistantMessage {
                    partial: false,
                    stop_reason: None,
                    content: vec![NormalizedContent::ToolUse {
                        raw_id: tool_id,
                        tool: name,
                        arguments,
                    }],
                }],
                false,
            )
        }
        "tool.complete" => {
            let tool_id = payload
                .get("tool_id")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            let content = tool_result_text(&payload);
            let is_error = payload
                .get("result")
                .map(|result| {
                    result.get("success").and_then(Value::as_bool) == Some(false)
                        || result.get("error").is_some_and(|e| !e.is_null())
                })
                .unwrap_or(false);
            (
                vec![NormalizedEvent::ToolResults(vec![NormalizedToolResult {
                    tool_use_id: tool_id,
                    content,
                    is_error,
                    exit_code: payload
                        .pointer("/result/exit_code")
                        .and_then(Value::as_i64)
                        .map(|code| code as i32),
                }])],
                false,
            )
        }
        "turn.usage" | "session.usage" => {
            let usage = NormalizedTokenUsage {
                model: payload
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                input_tokens: payload.get("input_tokens").and_then(Value::as_i64),
                output_tokens: payload.get("output_tokens").and_then(Value::as_i64),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: payload.get("cached_tokens").and_then(Value::as_i64),
                reasoning_output_tokens: None,
                total_tokens: payload.get("total_tokens").and_then(Value::as_i64),
            };
            if usage.has_counts() {
                (vec![NormalizedEvent::TokenUsage { usage }], false)
            } else {
                (Vec::new(), false)
            }
        }
        "message.complete" => {
            let status = payload.get("status").and_then(Value::as_str).unwrap_or("");
            let text = payload.get("text").and_then(Value::as_str).unwrap_or("");
            let mut events = Vec::new();
            if status == "error" {
                let error =
                    payload
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or(if text.is_empty() {
                            "hermes turn failed"
                        } else {
                            text
                        });
                state.saw_error = Some(error.to_string());
            } else if !text.is_empty() {
                events.push(NormalizedEvent::AssistantMessage {
                    partial: false,
                    stop_reason: Some("end_turn".to_string()),
                    content: vec![NormalizedContent::AgentText {
                        item_id: text_item,
                        text: text.to_string(),
                    }],
                });
            }
            events.push(NormalizedEvent::Result {
                error: state.saw_error.clone(),
            });
            (events, true)
        }
        _ => (Vec::new(), false),
    }
}

fn tool_result_text(payload: &Value) -> String {
    if let Some(text) = payload.get("result_text").and_then(Value::as_str) {
        return text.to_string();
    }
    match payload.get("result") {
        Some(Value::String(text)) => text.clone(),
        Some(value) if !value.is_null() => serde_json::to_string(value).unwrap_or_default(),
        _ => payload
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    }
}

fn run_hermes_turn<W: Write>(
    child: &mut HermesChild,
    stdout: &mut W,
    input: Vec<UserInput>,
    client_user_message_id: Option<String>,
    reasoning: Option<String>,
    turn: u64,
    interrupt_rx: &Receiver<()>,
) -> Result<()> {
    use crate::wire::notification_to_wire_value;

    let turn_id = format!("turn-{turn}");
    let mut config = BridgeConfig::new(child.thread_id().to_string(), turn_id);
    config.cli_version = "hermes".to_string();
    config.model_provider = "hermes".to_string();
    let mut normalizer = CodexTurnNormalizer::new(config);

    for notification in normalizer.start_notifications(turn == 1)? {
        write_value(stdout, &notification_to_wire_value(&notification)?)?;
    }
    for notification in normalizer.emit_user_message(client_user_message_id, input.clone())? {
        write_value(stdout, &notification_to_wire_value(&notification)?)?;
    }

    let text = user_input_text(&input);
    let mut params = json!({
        "session_id": child.session_id,
        "text": text,
    });
    if let Some(reasoning) = reasoning.filter(|value| !value.trim().is_empty()) {
        params["reasoning_effort"] = Value::String(reasoning);
    }
    child.next_rpc_id += 1;
    let submit_id = child.next_rpc_id;
    child.write_frame(&json!({
        "jsonrpc": "2.0",
        "id": submit_id,
        "method": "prompt.submit",
        "params": params,
    }))?;

    let mut state = HermesEventState::default();
    loop {
        if interrupt_rx.try_recv().is_ok() {
            let session_id = child.session_id.clone();
            let _ = child.rpc("session.interrupt", json!({"session_id": session_id}));
            // Hermes ends the interrupted turn with its own message.complete;
            // keep pumping until it arrives (bounded) so the trailing terminal
            // frame can't leak into the next turn.
            let deadline = Instant::now() + Duration::from_secs(30);
            while let Ok(frame) = child.read_frame_until(deadline) {
                let (_, terminal) = normalize_hermes_frame(&mut state, turn, &frame);
                if terminal {
                    break;
                }
            }
            if let Some(notification) = normalizer.finish_turn_interrupted()? {
                write_value(stdout, &notification_to_wire_value(&notification)?)?;
            }
            return Ok(());
        }

        match child.stdout.recv_timeout(Duration::from_millis(50)) {
            Ok(line) => {
                let line = line?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(frame) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                let (events, terminal) = normalize_hermes_frame(&mut state, turn, &frame);
                for event in events {
                    for notification in normalizer.process_event(&event)? {
                        write_value(stdout, &notification_to_wire_value(&notification)?)?;
                    }
                }
                if terminal {
                    if let Some(notification) = normalizer.finish_turn(state.saw_error.clone())? {
                        write_value(stdout, &notification_to_wire_value(&notification)?)?;
                    }
                    return Ok(());
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                let status = child.child.wait()?;
                return Err(HarnessServerError::HarnessExited {
                    kind: crate::HarnessKind::Codex,
                    status,
                    stderr: String::new(),
                });
            }
        }
    }
}

fn user_input_text(input: &[UserInput]) -> String {
    let mut parts = Vec::new();
    for item in input {
        match item {
            UserInput::Text { text, .. } => parts.push(text.clone()),
            UserInput::Image { url, .. } => parts.push(format!("[image: {url}]")),
            UserInput::LocalImage { path, .. } => {
                parts.push(format!("[image file: {}]", path.display()))
            }
            UserInput::Skill { name, path } => {
                parts.push(format!("[skill: {name} at {}]", path.display()))
            }
            UserInput::Mention { name, path } => parts.push(format!("[mention: {name} at {path}]")),
        }
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::traits::{NormalizedContent, NormalizedEvent};

    use super::{HermesEventState, normalize_hermes_frame};

    fn frame(kind: &str, payload: serde_json::Value) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "method": "event",
            "params": {"type": kind, "session_id": "abc", "payload": payload},
        })
    }

    #[test]
    fn message_delta_becomes_agent_text_delta() {
        let mut state = HermesEventState::default();
        let (events, terminal) = normalize_hermes_frame(
            &mut state,
            1,
            &frame("message.delta", json!({"text": "hi"})),
        );
        assert!(!terminal);
        assert!(matches!(
            &events[..],
            [NormalizedEvent::AgentTextDelta { delta, .. }] if delta == "hi"
        ));
    }

    #[test]
    fn reasoning_delta_becomes_reasoning_text_delta() {
        let mut state = HermesEventState::default();
        let (events, _) = normalize_hermes_frame(
            &mut state,
            1,
            &frame("reasoning.delta", json!({"text": "thinking"})),
        );
        assert!(matches!(
            &events[..],
            [NormalizedEvent::ReasoningTextDelta { delta, .. }] if delta == "thinking"
        ));
    }

    #[test]
    fn tool_start_and_complete_round_trip() {
        let mut state = HermesEventState::default();
        let (start_events, _) = normalize_hermes_frame(
            &mut state,
            1,
            &frame(
                "tool.start",
                json!({"tool_id": "t1", "name": "terminal", "args": {"command": "ls"}}),
            ),
        );
        let [NormalizedEvent::AssistantMessage { content, .. }] = &start_events[..] else {
            panic!("expected assistant message, got {start_events:?}");
        };
        assert!(matches!(
            &content[..],
            [NormalizedContent::ToolUse { raw_id, tool, .. }]
                if raw_id == "t1" && tool == "terminal"
        ));

        let (complete_events, _) = normalize_hermes_frame(
            &mut state,
            1,
            &frame(
                "tool.complete",
                json!({"tool_id": "t1", "name": "terminal", "result": {"output": "ok", "exit_code": 0}}),
            ),
        );
        let [NormalizedEvent::ToolResults(results)] = &complete_events[..] else {
            panic!("expected tool results, got {complete_events:?}");
        };
        assert_eq!(results[0].tool_use_id, "t1");
        assert!(!results[0].is_error);
        assert_eq!(results[0].exit_code, Some(0));
    }

    #[test]
    fn message_complete_finishes_turn_with_canonical_text() {
        let mut state = HermesEventState::default();
        let _ = normalize_hermes_frame(
            &mut state,
            1,
            &frame("message.delta", json!({"text": "par"})),
        );
        let (events, terminal) = normalize_hermes_frame(
            &mut state,
            1,
            &frame("message.complete", json!({"text": "partial then final"})),
        );
        assert!(terminal);
        assert!(matches!(
            &events[..],
            [
                NormalizedEvent::AssistantMessage { partial: false, content, .. },
                NormalizedEvent::Result { error: None },
            ] if matches!(
                &content[..],
                [NormalizedContent::AgentText { text, .. }] if text == "partial then final"
            )
        ));
    }

    #[test]
    fn message_complete_error_becomes_failed_result() {
        let mut state = HermesEventState::default();
        let (events, terminal) = normalize_hermes_frame(
            &mut state,
            1,
            &frame(
                "message.complete",
                json!({"text": "boom", "status": "error", "error": "provider 500"}),
            ),
        );
        assert!(terminal);
        assert!(matches!(
            &events[..],
            [NormalizedEvent::Result { error: Some(error) }] if error == "provider 500"
        ));
    }

    #[test]
    fn tool_complete_marks_failures() {
        let mut state = HermesEventState::default();
        let (events, _) = normalize_hermes_frame(
            &mut state,
            1,
            &frame(
                "tool.complete",
                json!({"tool_id": "t2", "result": {"success": false, "error": "denied"}}),
            ),
        );
        let [NormalizedEvent::ToolResults(results)] = &events[..] else {
            panic!("expected tool results");
        };
        assert!(results[0].is_error);
    }

    #[test]
    fn unknown_events_are_ignored() {
        let mut state = HermesEventState::default();
        let (events, terminal) =
            normalize_hermes_frame(&mut state, 1, &frame("session.info", json!({"model": "x"})));
        assert!(events.is_empty());
        assert!(!terminal);
    }

    #[test]
    fn non_event_frames_are_ignored() {
        let mut state = HermesEventState::default();
        let (events, terminal) = normalize_hermes_frame(
            &mut state,
            1,
            &json!({"jsonrpc": "2.0", "id": 7, "result": {"ok": true}}),
        );
        assert!(events.is_empty());
        assert!(!terminal);
    }

    #[test]
    fn usage_event_maps_token_counts() {
        let mut state = HermesEventState::default();
        let (events, _) = normalize_hermes_frame(
            &mut state,
            1,
            &frame(
                "turn.usage",
                json!({"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}),
            ),
        );
        let [NormalizedEvent::TokenUsage { usage }] = &events[..] else {
            panic!("expected token usage");
        };
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.total_tokens, Some(120));
    }
}
