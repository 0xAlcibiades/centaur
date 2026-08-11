use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use codex_app_server_protocol::UserInput;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    HarnessKind, HarnessServer, HarnessServerError, NormalizedContent, NormalizedEvent,
    NormalizedTokenUsage, NormalizedToolResult, Result, ThreadState, command_from_override,
};

const INITIALIZE_REQUEST_ID: &str = "centaur-hermes-initialize";
const SESSION_REQUEST_ID: &str = "centaur-hermes-session";
const MODE_REQUEST_ID: &str = "centaur-hermes-mode";
const MODEL_REQUEST_ID: &str = "centaur-hermes-model";
const PROMPT_REQUEST_ID: &str = "centaur-hermes-prompt";
const STEER_REQUEST_ID_PREFIX: &str = "centaur-hermes-steer-";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub(crate) struct HermesHarness;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum AcpRequestId {
    String(String),
    Number(i64),
}

impl AcpRequestId {
    fn is(&self, expected: &str) -> bool {
        matches!(self, Self::String(value) if value == expected)
    }

    fn starts_with(&self, prefix: &str) -> bool {
        matches!(self, Self::String(value) if value.starts_with(prefix))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct AcpWireMessage {
    #[serde(default)]
    id: Option<AcpRequestId>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<AcpRpcError>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AcpRpcError {
    code: i64,
    message: String,
    #[serde(default)]
    #[serde(rename = "data")]
    _data: Option<Value>,
}

#[derive(Debug)]
enum AcpResponse {
    Success(Value),
    Error(AcpRpcError),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AcpSessionNotification {
    #[serde(rename = "sessionId")]
    _session_id: String,
    update: AcpSessionUpdate,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "sessionUpdate", rename_all = "snake_case")]
enum AcpSessionUpdate {
    AgentMessageChunk {
        content: AcpContentBlock,
    },
    AgentThoughtChunk {
        content: AcpContentBlock,
    },
    ToolCall {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        title: String,
        #[serde(default)]
        kind: Option<String>,
        #[serde(default, rename = "rawInput")]
        raw_input: Option<Value>,
    },
    ToolCallUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        content: Option<Vec<AcpToolContent>>,
        #[serde(default, rename = "rawOutput")]
        raw_output: Option<Value>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AcpContentBlock {
    Text {
        text: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AcpToolContent {
    Content {
        content: AcpContentBlock,
    },
    Diff {
        path: String,
        #[serde(default, rename = "oldText")]
        old_text: Option<String>,
        #[serde(rename = "newText")]
        new_text: String,
    },
    Terminal {
        #[serde(rename = "terminalId")]
        terminal_id: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AcpPromptResponse {
    stop_reason: String,
    #[serde(default)]
    usage: Option<AcpUsage>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcpUsage {
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    #[serde(default)]
    thought_tokens: Option<i64>,
    #[serde(default)]
    cached_read_tokens: Option<i64>,
    #[serde(default)]
    cached_write_tokens: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcpPermissionRequest {
    options: Vec<AcpPermissionOption>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcpPermissionOption {
    option_id: String,
    kind: String,
}

#[derive(Debug, Clone)]
pub(crate) enum HermesAcpEvent {
    SessionUpdate(AcpSessionNotification),
    PromptResponse(AcpPromptResponse),
    ClientRequest {
        id: AcpRequestId,
        method: String,
        params: Option<Value>,
    },
    RpcError {
        request_id: Option<AcpRequestId>,
        error: AcpRpcError,
    },
    Ignored,
}

#[derive(Debug, Default)]
pub(crate) struct HermesEventNormalizer {
    message_index: usize,
    message_text: String,
    reasoning_text: String,
}

impl HermesEventNormalizer {
    fn message_item_id(&self) -> String {
        format!("hermes-message-{}", self.message_index)
    }

    fn reasoning_item_id(&self) -> String {
        "hermes-reasoning".to_string()
    }

    fn finish_message(&mut self, stop_reason: &str, out: &mut Vec<NormalizedEvent>) {
        if self.message_text.is_empty() {
            return;
        }
        out.push(NormalizedEvent::AssistantMessage {
            partial: false,
            stop_reason: Some(stop_reason.to_string()),
            content: vec![NormalizedContent::AgentText {
                item_id: self.message_item_id(),
                text: std::mem::take(&mut self.message_text),
            }],
        });
        self.message_index += 1;
    }

    fn finish_reasoning(&mut self, out: &mut Vec<NormalizedEvent>) {
        if self.reasoning_text.is_empty() {
            return;
        }
        out.push(NormalizedEvent::AssistantMessage {
            partial: false,
            stop_reason: None,
            content: vec![NormalizedContent::ReasoningText {
                item_id: self.reasoning_item_id(),
                text: std::mem::take(&mut self.reasoning_text),
            }],
        });
    }

    fn normalize(&mut self, event: HermesAcpEvent) -> Vec<NormalizedEvent> {
        let mut out = Vec::new();
        match event {
            HermesAcpEvent::SessionUpdate(notification) => match notification.update {
                AcpSessionUpdate::AgentMessageChunk {
                    content: AcpContentBlock::Text { text },
                } => {
                    self.message_text.push_str(&text);
                    out.push(NormalizedEvent::AgentTextDelta {
                        item_id: self.message_item_id(),
                        delta: text,
                    });
                }
                AcpSessionUpdate::AgentThoughtChunk {
                    content: AcpContentBlock::Text { text },
                } => {
                    self.reasoning_text.push_str(&text);
                    out.push(NormalizedEvent::ReasoningTextDelta {
                        item_id: self.reasoning_item_id(),
                        delta: text,
                    });
                }
                AcpSessionUpdate::ToolCall {
                    tool_call_id,
                    title,
                    kind,
                    raw_input,
                } => {
                    self.finish_message("tool_use", &mut out);
                    let arguments = raw_input.unwrap_or_else(|| json!({}));
                    let tool = tool_name(&title, kind.as_deref());
                    out.push(NormalizedEvent::AssistantMessage {
                        partial: false,
                        stop_reason: Some("tool_use".to_string()),
                        content: vec![NormalizedContent::ToolUse {
                            raw_id: tool_call_id,
                            tool,
                            arguments,
                        }],
                    });
                }
                AcpSessionUpdate::ToolCallUpdate {
                    tool_call_id,
                    status,
                    content,
                    raw_output,
                } if status
                    .as_deref()
                    .is_some_and(|status| matches!(status, "completed" | "failed")) =>
                {
                    out.push(NormalizedEvent::ToolResults(vec![NormalizedToolResult {
                        tool_use_id: tool_call_id,
                        content: tool_result_text(raw_output.as_ref(), content.as_deref()),
                        is_error: status.as_deref() == Some("failed"),
                        exit_code: tool_exit_code(raw_output.as_ref()),
                    }]));
                }
                _ => {}
            },
            HermesAcpEvent::PromptResponse(response) => {
                self.finish_message(&response.stop_reason, &mut out);
                self.finish_reasoning(&mut out);
                if let Some(usage) = response.usage {
                    out.push(NormalizedEvent::TokenUsage {
                        usage: NormalizedTokenUsage {
                            model: None,
                            input_tokens: Some(usage.input_tokens),
                            output_tokens: Some(usage.output_tokens),
                            cache_creation_input_tokens: usage.cached_write_tokens,
                            cache_read_input_tokens: usage.cached_read_tokens,
                            reasoning_output_tokens: usage.thought_tokens,
                            total_tokens: Some(usage.total_tokens),
                        },
                    });
                }
                let error = (response.stop_reason == "refusal")
                    .then(|| "Hermes refused the prompt".to_string());
                out.push(NormalizedEvent::Result { error });
            }
            HermesAcpEvent::RpcError { request_id, error }
                if request_id
                    .as_ref()
                    .is_some_and(|id| id.is(PROMPT_REQUEST_ID)) =>
            {
                out.push(NormalizedEvent::Error {
                    message: format!("Hermes ACP error {}: {}", error.code, error.message),
                });
            }
            HermesAcpEvent::RpcError { request_id, error }
                if request_id
                    .as_ref()
                    .is_some_and(|id| id.starts_with(STEER_REQUEST_ID_PREFIX)) =>
            {
                // turn/steer is acknowledged after the request is written to Hermes. Keep a
                // later native rejection non-terminal for the active turn, but make it visible
                // instead of silently discarding the user's steer.
                eprintln!(
                    "Hermes ACP steer request was rejected ({}): {}",
                    error.code, error.message
                );
            }
            HermesAcpEvent::RpcError { .. } => {}
            HermesAcpEvent::ClientRequest { .. } | HermesAcpEvent::Ignored => {}
        }
        out
    }
}

impl HarnessServer for HermesHarness {
    type Event = HermesAcpEvent;
    type EventNormalizer = HermesEventNormalizer;

    fn kind(&self) -> HarnessKind {
        HarnessKind::Hermes
    }

    fn cli_version(&self) -> &'static str {
        "hermes-acp"
    }

    fn default_model(&self) -> String {
        env::var("HERMES_MODEL").unwrap_or_else(|_| "openrouter/auto".to_string())
    }

    fn default_model_provider(&self) -> &'static str {
        "hermes"
    }

    fn command_for_turn(&self, _state: &ThreadState) -> ProcessCommand {
        if let Some(command) = command_from_override("CENTAUR_HERMES_ACP_COMMAND") {
            return command;
        }
        ProcessCommand::new(env::var("HERMES_ACP_BIN").unwrap_or_else(|_| "hermes-acp".to_string()))
    }

    fn initialize_process(&self, state: &mut ThreadState) -> Result<()> {
        send_request(
            state,
            INITIALIZE_REQUEST_ID,
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": {"readTextFile": false, "writeTextFile": false},
                    "terminal": false,
                    "auth": {"terminal": false}
                },
                "clientInfo": {"name": "centaur-harness-server", "version": env!("CARGO_PKG_VERSION")}
            }),
        )?;
        wait_for_response(state, INITIALIZE_REQUEST_ID)?;

        let mapped = read_session_mapping(&state.id)?;
        let candidate = mapped.or_else(|| {
            state
                .harness_session_id
                .clone()
                .filter(|session_id| session_id != &state.id)
        });
        let session_id = if let Some(session_id) = candidate {
            send_request(
                state,
                SESSION_REQUEST_ID,
                "session/load",
                json!({"cwd": state.cwd, "sessionId": session_id, "mcpServers": []}),
            )?;
            match wait_for_rpc_response(state, SESSION_REQUEST_ID)? {
                // ACP session/load has no required result fields. Hermes versions have returned
                // both null and empty objects here, so any successful response means the native
                // session was restored.
                AcpResponse::Success(_) => session_id,
                // A stale Centaur mapping is expected when Hermes' local session store is wiped
                // or its storage format changes. Recover by replacing the missing native session.
                AcpResponse::Error(error) => {
                    eprintln!(
                        "Hermes ACP could not load native session; creating a replacement ({}): {}",
                        error.code, error.message
                    );
                    create_session(state)?
                }
            }
        } else {
            create_session(state)?
        };
        state.harness_session_id = Some(session_id.clone());
        write_session_mapping(&state.id, &session_id)?;

        send_request(
            state,
            MODE_REQUEST_ID,
            "session/set_mode",
            json!({"sessionId": session_id, "modeId": "dont_ask"}),
        )?;
        wait_for_response(state, MODE_REQUEST_ID)?;

        if !state.model.trim().is_empty() {
            send_request(
                state,
                MODEL_REQUEST_ID,
                "session/set_model",
                json!({"sessionId": session_id, "modelId": state.model}),
            )?;
            wait_for_response(state, MODEL_REQUEST_ID)?;
        }
        Ok(())
    }

    fn stdin_for_turn(&self, _input: &[UserInput]) -> Result<Vec<u8>> {
        Err(HarnessServerError::Protocol(
            "Hermes ACP prompts require initialized thread state".to_string(),
        ))
    }

    fn stdin_for_state_turn(&self, state: &ThreadState, input: &[UserInput]) -> Result<Vec<u8>> {
        prompt_request(state, input, PROMPT_REQUEST_ID, false)
    }

    fn stdin_for_state_steer(&self, state: &ThreadState, input: &[UserInput]) -> Result<Vec<u8>> {
        let request_id = format!("{STEER_REQUEST_ID_PREFIX}{}", Uuid::new_v4().simple());
        prompt_request(state, input, &request_id, true)
    }

    fn stdin_for_interrupt(&self, state: &ThreadState) -> Result<Option<Vec<u8>>> {
        let session_id = state.harness_session_id.as_deref().ok_or_else(|| {
            HarnessServerError::Protocol("Hermes ACP session is not initialized".to_string())
        })?;
        Ok(Some(json_line(&json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id}
        }))?))
    }

    fn wait_for_interrupt(&self, state: &mut ThreadState) -> Result<()> {
        wait_for_response(state, PROMPT_REQUEST_ID).map(|_| ())
    }

    fn parse_stdout_line(&self, line: &str) -> Result<Self::Event> {
        parse_acp_event(line)
    }

    fn response_for_event(&self, event: &Self::Event) -> Result<Option<Vec<u8>>> {
        let HermesAcpEvent::ClientRequest { id, method, params } = event else {
            return Ok(None);
        };
        if method == "session/request_permission" {
            let result = permission_response_result(params.clone())?;
            return Ok(Some(rpc_response_line(id, result)?));
        }
        Ok(Some(json_line(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": format!("unsupported ACP client method: {method}")}
        }))?))
    }

    fn normalize_events(
        &self,
        normalizer: &mut Self::EventNormalizer,
        event: Self::Event,
    ) -> Result<Vec<NormalizedEvent>> {
        Ok(normalizer.normalize(event))
    }
}

fn create_session(state: &mut ThreadState) -> Result<String> {
    send_request(
        state,
        SESSION_REQUEST_ID,
        "session/new",
        json!({"cwd": state.cwd, "mcpServers": []}),
    )?;
    let result = wait_for_response(state, SESSION_REQUEST_ID)?;
    result
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            HarnessServerError::Protocol("Hermes session/new omitted sessionId".to_string())
        })
}

fn send_request(state: &mut ThreadState, id: &str, method: &str, params: Value) -> Result<()> {
    let process = state
        .process
        .as_mut()
        .ok_or(HarnessServerError::HarnessStdinUnavailable)?;
    process.stdin.write_all(&json_line(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params
    }))?)?;
    process.stdin.flush()?;
    Ok(())
}

fn wait_for_response(state: &mut ThreadState, expected_id: &str) -> Result<Value> {
    match wait_for_rpc_response(state, expected_id)? {
        AcpResponse::Success(result) => Ok(result),
        AcpResponse::Error(error) => Err(HarnessServerError::Protocol(format!(
            "Hermes ACP error {}: {}",
            error.code, error.message
        ))),
    }
}

fn wait_for_rpc_response(state: &mut ThreadState, expected_id: &str) -> Result<AcpResponse> {
    let process = state
        .process
        .as_mut()
        .ok_or(HarnessServerError::HarnessStdoutUnavailable)?;
    loop {
        let line = match process.stdout.recv_timeout(HANDSHAKE_TIMEOUT) {
            Ok(line) => line?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(HarnessServerError::Protocol(format!(
                    "timed out waiting for Hermes ACP response `{expected_id}`"
                )));
            }
            Err(RecvTimeoutError::Disconnected) => {
                let status = process.child.wait()?;
                return Err(HarnessServerError::HarnessExited {
                    kind: HarnessKind::Hermes,
                    status,
                    stderr: String::new(),
                });
            }
        };
        let wire: AcpWireMessage = serde_json::from_str(line.trim())?;
        if wire.id.as_ref().is_some_and(|id| id.is(expected_id)) {
            if let Some(error) = wire.error {
                return Ok(AcpResponse::Error(error));
            }
            return Ok(AcpResponse::Success(wire.result.unwrap_or(Value::Null)));
        }
        if wire.method.as_deref() == Some("session/request_permission")
            && let Some(id) = wire.id
        {
            let result = permission_response_result(wire.params)?;
            process.stdin.write_all(&rpc_response_line(&id, result)?)?;
            process.stdin.flush()?;
        }
    }
}

fn permission_response_result(params: Option<Value>) -> Result<Value> {
    let request: AcpPermissionRequest =
        serde_json::from_value(params.unwrap_or_else(|| json!({})))?;
    let selected = request
        .options
        .iter()
        .find(|option| option.option_id == "allow_session")
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| option.kind == "allow_always")
        })
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| option.kind == "allow_once")
        });
    Ok(selected.map_or_else(
        || json!({"outcome": {"outcome": "cancelled"}}),
        |option| json!({"outcome": {"outcome": "selected", "optionId": option.option_id}}),
    ))
}

fn prompt_request(
    state: &ThreadState,
    input: &[UserInput],
    request_id: &str,
    steer: bool,
) -> Result<Vec<u8>> {
    let session_id = state.harness_session_id.as_deref().ok_or_else(|| {
        HarnessServerError::Protocol("Hermes ACP session is not initialized".to_string())
    })?;
    let mut prompt = acp_prompt(input)?;
    if steer {
        let text = prompt
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        prompt = vec![json!({"type": "text", "text": format!("/steer {text}")})];
    }
    json_line(&json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "session/prompt",
        "params": {"sessionId": session_id, "prompt": prompt}
    }))
}

fn acp_prompt(input: &[UserInput]) -> Result<Vec<Value>> {
    input
        .iter()
        .map(|item| match item {
            UserInput::Text { text, .. } => Ok(json!({"type": "text", "text": text})),
            UserInput::LocalImage { path, .. } => Ok(json!({
                "type": "image",
                "data": BASE64_STANDARD.encode(fs::read(path)?),
                "mimeType": image_mime_type(path),
            })),
            UserInput::Image { url, .. } => {
                Ok(json!({"type": "text", "text": format!("[image: {url}]")}))
            }
            UserInput::Skill { name, path } => Ok(json!({
                "type": "text",
                "text": format!("[skill: {name} at {}]", path.display()),
            })),
            UserInput::Mention { name, path } => Ok(json!({
                "type": "text",
                "text": format!("[mention: {name} at {path}]"),
            })),
        })
        .collect()
}

fn image_mime_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) if extension.eq_ignore_ascii_case("png") => "image/png",
        Some(extension) if extension.eq_ignore_ascii_case("webp") => "image/webp",
        Some(extension) if extension.eq_ignore_ascii_case("gif") => "image/gif",
        _ => "image/jpeg",
    }
}

fn parse_acp_event(line: &str) -> Result<HermesAcpEvent> {
    let wire: AcpWireMessage = serde_json::from_str(line)?;
    if let (Some(method), Some(id)) = (wire.method.as_deref(), wire.id.clone()) {
        return Ok(HermesAcpEvent::ClientRequest {
            id,
            method: method.to_string(),
            params: wire.params,
        });
    }
    if wire.method.as_deref() == Some("session/update") {
        let notification = serde_json::from_value(wire.params.unwrap_or_else(|| json!({})))?;
        return Ok(HermesAcpEvent::SessionUpdate(notification));
    }
    if let Some(error) = wire.error {
        return Ok(HermesAcpEvent::RpcError {
            request_id: wire.id,
            error,
        });
    }
    if wire.id.as_ref().is_some_and(|id| id.is(PROMPT_REQUEST_ID)) {
        let response = serde_json::from_value(wire.result.unwrap_or_else(|| json!({})))?;
        return Ok(HermesAcpEvent::PromptResponse(response));
    }
    Ok(HermesAcpEvent::Ignored)
}

fn tool_name(title: &str, kind: Option<&str>) -> String {
    if kind == Some("execute") {
        "shell_command".to_string()
    } else if let Some(kind) = kind.filter(|kind| !kind.is_empty()) {
        kind.to_string()
    } else {
        title.trim().to_string().if_empty("tool")
    }
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

fn tool_result_text(raw_output: Option<&Value>, content: Option<&[AcpToolContent]>) -> String {
    if let Some(Value::String(text)) = raw_output {
        return text.clone();
    }
    if let Some(value) = raw_output {
        return serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"));
    }
    content
        .unwrap_or_default()
        .iter()
        .filter_map(|item| match item {
            AcpToolContent::Content {
                content: AcpContentBlock::Text { text },
            } => Some(text.clone()),
            AcpToolContent::Diff {
                path,
                old_text,
                new_text,
            } => Some(format!(
                "diff {path}\n--- old\n{}\n+++ new\n{new_text}",
                old_text.as_deref().unwrap_or("")
            )),
            AcpToolContent::Terminal { terminal_id } => Some(format!("terminal: {terminal_id}")),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn tool_exit_code(raw_output: Option<&Value>) -> Option<i32> {
    raw_output?
        .get("exitCode")
        .or_else(|| raw_output?.get("exit_code"))
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok())
}

fn json_line(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn rpc_response_line(id: &AcpRequestId, result: Value) -> Result<Vec<u8>> {
    json_line(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn session_mapping_path(thread_id: &str) -> PathBuf {
    let home = env::var_os("HERMES_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".hermes")))
        .unwrap_or_else(|| PathBuf::from("/tmp/hermes"));
    let digest = Sha256::digest(thread_id.as_bytes());
    let filename = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    home.join("centaur-acp-sessions").join(filename)
}

fn read_session_mapping(thread_id: &str) -> Result<Option<String>> {
    let path = session_mapping_path(thread_id);
    match fs::read_to_string(path) {
        Ok(session_id) => Ok(Some(session_id.trim().to_string()).filter(|id| !id.is_empty())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_session_mapping(thread_id: &str, session_id: &str) -> Result<()> {
    let path = session_mapping_path(thread_id);
    let parent = path.parent().ok_or_else(|| {
        HarnessServerError::Protocol("Hermes session mapping path has no parent".to_string())
    })?;
    fs::create_dir_all(parent)?;
    fs::write(path, format!("{session_id}\n"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use codex_app_server_protocol::UserInput;
    use serde_json::{Value, json};

    use crate::{HarnessServer, NormalizedContent, NormalizedEvent, ThreadState};

    use super::{
        HermesAcpEvent, HermesEventNormalizer, HermesHarness, PROMPT_REQUEST_ID,
        STEER_REQUEST_ID_PREFIX, parse_acp_event, permission_response_result,
    };

    fn state() -> ThreadState {
        ThreadState {
            id: "thread-1".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            model: String::new(),
            model_provider: "hermes".to_string(),
            service_tier: None,
            harness_session_id: Some("hermes-session-1".to_string()),
            completed_turns: Vec::new(),
            process: None,
            thread_started_sent: false,
        }
    }

    #[test]
    fn turn_stdin_is_an_acp_prompt_for_the_native_session() {
        let bytes = HermesHarness
            .stdin_for_state_turn(
                &state(),
                &[UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: Vec::new(),
                }],
            )
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["id"], PROMPT_REQUEST_ID);
        assert_eq!(value["method"], "session/prompt");
        assert_eq!(value["params"]["sessionId"], "hermes-session-1");
        assert_eq!(value["params"]["prompt"][0]["text"], "hello");
    }

    #[test]
    fn steer_uses_hermes_active_turn_command() {
        let first = HermesHarness
            .stdin_for_state_steer(
                &state(),
                &[UserInput::Text {
                    text: "use the other file".to_string(),
                    text_elements: Vec::new(),
                }],
            )
            .unwrap();
        let second = HermesHarness
            .stdin_for_state_steer(
                &state(),
                &[UserInput::Text {
                    text: "use the other file".to_string(),
                    text_elements: Vec::new(),
                }],
            )
            .unwrap();
        let first: Value = serde_json::from_slice(&first).unwrap();
        let second: Value = serde_json::from_slice(&second).unwrap();

        assert_eq!(
            first["params"]["prompt"][0]["text"],
            "/steer use the other file"
        );
        assert!(
            first["id"]
                .as_str()
                .unwrap()
                .starts_with(STEER_REQUEST_ID_PREFIX)
        );
        assert_ne!(first["id"], second["id"]);
    }

    #[test]
    fn parses_and_normalizes_message_thought_tool_and_terminal_response() {
        let mut normalizer = HermesEventNormalizer::default();
        let message = parse_acp_event(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Checking."}}}}"#,
        )
        .unwrap();
        let events = normalizer.normalize(message);
        assert!(matches!(
            events.as_slice(),
            [NormalizedEvent::AgentTextDelta { delta, .. }] if delta == "Checking."
        ));

        let thought = parse_acp_event(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"Need inspect."}}}}"#,
        )
        .unwrap();
        assert!(matches!(
            normalizer.normalize(thought).as_slice(),
            [NormalizedEvent::ReasoningTextDelta { delta, .. }] if delta == "Need inspect."
        ));

        let tool = parse_acp_event(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"tc1","title":"Run command","kind":"execute","rawInput":{"command":"pwd"}}}}"#,
        )
        .unwrap();
        let events = normalizer.normalize(tool);
        assert!(matches!(
            &events[0],
            NormalizedEvent::AssistantMessage { stop_reason: Some(reason), content, .. }
                if reason == "tool_use"
                    && matches!(content.as_slice(), [NormalizedContent::AgentText { text, .. }] if text == "Checking.")
        ));
        assert!(matches!(
            &events[1],
            NormalizedEvent::AssistantMessage { content, .. }
                if matches!(content.as_slice(), [NormalizedContent::ToolUse { tool, arguments, .. }]
                    if tool == "shell_command" && arguments["command"] == "pwd")
        ));

        let complete = parse_acp_event(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","toolCallId":"tc1","status":"completed","rawOutput":"/tmp/project"}}}"#,
        )
        .unwrap();
        assert!(matches!(
            normalizer.normalize(complete).as_slice(),
            [NormalizedEvent::ToolResults(results)] if results[0].content == "/tmp/project"
        ));

        let response = parse_acp_event(
            r#"{"jsonrpc":"2.0","id":"centaur-hermes-prompt","result":{"stopReason":"end_turn","usage":{"inputTokens":10,"outputTokens":4,"totalTokens":14}}}"#,
        )
        .unwrap();
        let events = normalizer.normalize(response);
        assert!(events.iter().any(|event| matches!(event, NormalizedEvent::TokenUsage { usage } if usage.total_tokens == Some(14))));
        assert!(matches!(
            events.last(),
            Some(NormalizedEvent::Result { error: None })
        ));
    }

    #[test]
    fn permission_requests_are_approved_for_the_session() {
        let event = parse_acp_event(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"sessionId":"s1","toolCall":{"toolCallId":"p1"},"options":[{"optionId":"allow_once","kind":"allow_once","name":"Once"},{"optionId":"allow_session","kind":"allow_always","name":"Session"}]}}"#,
        )
        .unwrap();
        let response = HermesHarness.response_for_event(&event).unwrap().unwrap();
        let value: Value = serde_json::from_slice(&response).unwrap();

        assert_eq!(value["id"], 7);
        assert_eq!(value["result"]["outcome"]["outcome"], "selected");
        assert_eq!(value["result"]["outcome"]["optionId"], "allow_session");
    }

    #[test]
    fn permission_selection_falls_back_to_once_and_then_cancelled() {
        let once = permission_response_result(Some(json!({
            "options": [{"optionId": "once", "kind": "allow_once"}]
        })))
        .unwrap();
        assert_eq!(once["outcome"]["optionId"], "once");

        let cancelled = permission_response_result(Some(json!({"options": []}))).unwrap();
        assert_eq!(cancelled["outcome"]["outcome"], "cancelled");
    }

    #[test]
    fn ignores_steer_responses_as_non_terminal() {
        let event = parse_acp_event(
            &json!({"jsonrpc": "2.0", "id": "centaur-hermes-steer-123", "result": {"stopReason": "end_turn"}}).to_string(),
        )
        .unwrap();
        assert!(matches!(event, HermesAcpEvent::Ignored));
    }

    #[test]
    fn steer_rpc_errors_are_non_terminal() {
        let event = parse_acp_event(
            &json!({
                "jsonrpc": "2.0",
                "id": "centaur-hermes-steer-123",
                "error": {"code": -32000, "message": "prompt already active"}
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(
            &event,
            HermesAcpEvent::RpcError { request_id: Some(id), .. }
                if id.starts_with(STEER_REQUEST_ID_PREFIX)
        ));
        assert!(HermesEventNormalizer::default().normalize(event).is_empty());
    }
}
