//! Per-session principal registration.
//!
//! When a session starts, [`SessionRegistrar`] upserts the session's principal.
//! Iron-control owns default role assignment for brand-new principals, while
//! existing principals keep their current assignments so operator revocations
//! in Console or ``centaur-perms`` remain sticky. Workflow principals use the
//! same creation behavior. Other principals are derived from the thread key
//! (see [`crate::derive_principal`]).

use serde_json::Value;
use std::collections::BTreeMap;

use crate::IronControlClient;
use crate::error::{IronControlError, Result};
use crate::models::{Principal, SlackChannelPermissionInput};
use crate::principal::{
    derive_principal_with_slack_team, derive_workflow_principal, is_direct_message,
    slack_conversation_id,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SessionPrincipalMetadata<'a> {
    actor_user_id: Option<&'a str>,
    slack_team_id: Option<&'a str>,
    slack_user_email: Option<&'a str>,
    conversation_name: Option<&'a str>,
}

impl<'a> SessionPrincipalMetadata<'a> {
    fn from_session_metadata(metadata: Option<&'a Value>) -> Self {
        let Some(metadata) = metadata else {
            return Self::default();
        };
        Self {
            actor_user_id: metadata
                .get("slack_user_id")
                .or_else(|| metadata.get("aad_object_id"))
                .or_else(|| metadata.get("user_id"))
                .and_then(Value::as_str),
            slack_team_id: metadata.get("slack_team_id").and_then(Value::as_str),
            slack_user_email: metadata.get("slack_user_email").and_then(Value::as_str),
            conversation_name: metadata
                .get("slack_conversation_name")
                .or_else(|| metadata.get("discord_conversation_name"))
                .or_else(|| metadata.get("linear_conversation_name"))
                .or_else(|| metadata.get("teams_conversation_name"))
                .and_then(Value::as_str),
        }
    }
}

/// Registers a session's principal against iron-control at session start.
///
/// Cheap to clone (the inner [`IronControlClient`] shares a connection pool),
/// so it can live on a shared runtime handle.
#[derive(Clone, Debug)]
pub struct SessionRegistrar {
    client: IronControlClient,
    namespace: String,
}

impl SessionRegistrar {
    pub fn new(client: IronControlClient, namespace: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
        }
    }

    /// Upsert the principal for ``thread_key`` using the session metadata the
    /// ingress supplied. Returns the upserted principal record (its ``id`` is
    /// the OID) so callers can bind the session's egress proxy to the same
    /// identity.
    ///
    /// Re-registering an existing channel/user refreshes identity metadata but
    /// leaves its role assignments to iron-control.
    pub async fn register_session(
        &self,
        thread_key: &str,
        metadata: Option<&Value>,
    ) -> Result<Principal> {
        let metadata = SessionPrincipalMetadata::from_session_metadata(metadata);
        let principal = derive_principal_with_slack_team(
            thread_key,
            metadata.actor_user_id,
            metadata.slack_team_id,
            metadata.conversation_name,
        );
        let mut input = principal.to_identity_input(&self.namespace);
        apply_slack_dm_email_label(thread_key, metadata.slack_user_email, &mut input.labels);
        let exists = self.merge_existing_labels(&mut input).await?;
        let slack_permission = slack_permission_for_thread(thread_key, &input.labels);
        let should_upsert_slack_permission = !exists
            || slack_permission
                .as_ref()
                .is_some_and(|permission| is_direct_message(Some(&permission.channel_id)));
        let record = self.client.upsert_principal(&input).await?;
        if should_upsert_slack_permission && let Some(permission) = slack_permission {
            self.client
                .upsert_slack_channel_permission(&record.id, &permission)
                .await?;
        }
        Ok(record)
    }

    /// Ensure the stable principal for ``workflow_name`` exists without
    /// assigning any roles. Workflow discovery and automatic agent turns share
    /// this path so workflow permissions remain operator-managed.
    pub async fn ensure_workflow_principal(&self, workflow_name: &str) -> Result<Principal> {
        let mut input = derive_workflow_principal(workflow_name).to_identity_input(&self.namespace);
        self.merge_existing_labels(&mut input).await?;
        self.client.upsert_principal(&input).await
    }

    async fn merge_existing_labels(&self, input: &mut crate::IdentityInput) -> Result<bool> {
        let existing = match self
            .client
            .get_principal(&self.namespace, &input.foreign_id)
            .await
        {
            Ok(existing) => Some(existing),
            Err(error) if is_status(&error, 404) => None,
            Err(error) => return Err(error),
        };
        let exists = existing.is_some();
        if let Some(existing) = existing {
            let mut labels = existing.labels;
            labels.extend(input.labels.clone());
            input.labels = labels;
        }
        Ok(exists)
    }

    pub async fn get_principal(&self, principal: &str) -> Result<Principal> {
        self.client.get_principal(&self.namespace, principal).await
    }
}

fn slack_permission_for_thread(
    thread_key: &str,
    labels: &BTreeMap<String, String>,
) -> Option<SlackChannelPermissionInput> {
    if let Some(channel_id) = labels.get("slack_channel_id") {
        let channel_id = channel_id.trim();
        return (!is_direct_message(Some(channel_id)))
            .then(|| slack_permission(channel_id.to_owned()));
    }

    if !labels.contains_key("slack_user_id") {
        return None;
    }
    let conversation_id = slack_conversation_id(thread_key)?;
    is_direct_message(Some(conversation_id)).then(|| slack_permission(conversation_id.to_owned()))
}

fn apply_slack_dm_email_label(
    thread_key: &str,
    slack_user_email: Option<&str>,
    labels: &mut BTreeMap<String, String>,
) {
    let Some(email) = slack_user_email
        .map(str::trim)
        .filter(|email| !email.is_empty())
    else {
        return;
    };
    let Some(conversation_id) = slack_conversation_id(thread_key) else {
        return;
    };
    if is_direct_message(Some(conversation_id)) && labels.contains_key("slack_user_id") {
        labels.insert("slack_email".to_owned(), email.to_owned());
    }
}

fn slack_permission(channel_id: String) -> SlackChannelPermissionInput {
    SlackChannelPermissionInput {
        channel_id,
        upload_enabled: true,
        download_enabled: true,
        history_enabled: true,
    }
}

fn is_status(err: &IronControlError, code: u16) -> bool {
    matches!(err, IronControlError::Status { status, .. } if *status == code)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn session_principal_metadata_prefers_slack_user_then_teams_ids() {
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "slack_user_id": "U1",
                "aad_object_id": "aad-user-1",
                "user_id": "teams-user-1"
            })))
            .actor_user_id,
            Some("U1")
        );
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "aad_object_id": "aad-user-1",
                "user_id": "teams-user-1"
            })))
            .actor_user_id,
            Some("aad-user-1")
        );
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "user_id": "teams-user-1"
            })))
            .actor_user_id,
            Some("teams-user-1")
        );
    }

    #[test]
    fn session_principal_metadata_accepts_teams_name() {
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "teams_conversation_name": "Casey Harper"
            })))
            .conversation_name,
            Some("Casey Harper")
        );
    }

    #[test]
    fn session_principal_metadata_carries_slack_team_id() {
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "slack_team_id": "T123"
            })))
            .slack_team_id,
            Some("T123")
        );
    }

    #[test]
    fn session_principal_metadata_carries_slack_user_email() {
        assert_eq!(
            SessionPrincipalMetadata::from_session_metadata(Some(&json!({
                "slack_user_email": "ada@example.com"
            })))
            .slack_user_email,
            Some("ada@example.com")
        );
    }

    #[test]
    fn slack_dm_email_label_applies_only_to_dm_user_principals() {
        let mut dm_labels = BTreeMap::new();
        dm_labels.insert("slack_user_id".to_owned(), "U123".to_owned());
        apply_slack_dm_email_label(
            "slack:T123:D123:1773364194.179929",
            Some(" ada@example.com "),
            &mut dm_labels,
        );
        assert_eq!(
            dm_labels.get("slack_email").map(String::as_str),
            Some("ada@example.com")
        );

        let mut channel_labels = BTreeMap::new();
        channel_labels.insert("slack_channel_id".to_owned(), "C123".to_owned());
        apply_slack_dm_email_label(
            "slack:T123:C123:1773364194.179929",
            Some("ada@example.com"),
            &mut channel_labels,
        );
        assert_eq!(channel_labels.get("slack_email"), None);
    }

    #[tokio::test]
    async fn register_session_leaves_default_roles_to_iron_control() {
        let (base_url, requests, server) = spawn_iron_control_stub(false).await;
        let registrar =
            SessionRegistrar::new(IronControlClient::new(base_url, "test-key"), "default");
        let metadata = json!({
            "slack_user_id": "U123",
            "slack_team_id": "T123",
            "slack_conversation_name": "general"
        });

        registrar
            .register_session("slack:T123:C123:1773364194.179929", Some(&metadata))
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert!(
            requests.contains(
                &"GET /api/v1/principals/lookup/default/slack-channel-t123-c123".to_owned()
            )
        );
        assert!(requests.contains(&"PUT /api/v1/principals/slack-channel-t123-c123".to_owned()));
        assert!(
            requests.contains(
                &"POST /api/v1/principals/prn_channel/slack_channel_permissions".to_owned()
            )
        );
        assert!(
            !requests
                .iter()
                .any(|request| request == "POST /api/v1/principals/prn_channel/roles"),
            "iron-control assigns configured default roles during principal creation"
        );
        server.abort();
    }

    #[tokio::test]
    async fn register_session_does_not_restore_roles_for_existing_principal() {
        let (base_url, requests, server) = spawn_iron_control_stub(true).await;
        let registrar =
            SessionRegistrar::new(IronControlClient::new(base_url, "test-key"), "default");
        let metadata = json!({
            "slack_user_id": "U123",
            "slack_team_id": "T123",
            "slack_conversation_name": "general"
        });

        registrar
            .register_session("slack:T123:C123:1773364194.179929", Some(&metadata))
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert!(
            requests.contains(
                &"GET /api/v1/principals/lookup/default/slack-channel-t123-c123".to_owned()
            )
        );
        assert!(requests.contains(&"PUT /api/v1/principals/slack-channel-t123-c123".to_owned()));
        assert!(
            !requests
                .iter()
                .any(|request| request.ends_with("/slack_channel_permissions")),
            "existing principals must not have Slack permissions reset"
        );
        assert!(
            !requests
                .iter()
                .any(|request| request == "POST /api/v1/principals/prn_channel/roles"),
            "existing principals must not have manually removed roles restored"
        );
        server.abort();
    }

    #[tokio::test]
    async fn register_session_upserts_slack_dm_permission_for_new_user_principal() {
        let (base_url, requests, server) = spawn_iron_control_stub(false).await;
        let registrar =
            SessionRegistrar::new(IronControlClient::new(base_url, "test-key"), "default");
        let metadata = json!({
            "slack_user_id": "U123",
            "slack_team_id": "T123",
            "slack_conversation_name": "Ada Lovelace"
        });

        registrar
            .register_session("slack:T123:D123:1773364194.179929", Some(&metadata))
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert!(requests.contains(&"PUT /api/v1/principals/slack-user-t123-u123".to_owned()));
        assert!(
            requests
                .contains(&"POST /api/v1/principals/prn_user/slack_channel_permissions".to_owned())
        );
        server.abort();
    }

    #[tokio::test]
    async fn register_session_upserts_slack_dm_permission_for_existing_user_principal() {
        let (base_url, requests, server) = spawn_iron_control_stub(true).await;
        let registrar =
            SessionRegistrar::new(IronControlClient::new(base_url, "test-key"), "default");
        let metadata = json!({
            "slack_user_id": "U123",
            "slack_team_id": "T123",
            "slack_conversation_name": "Ada Lovelace"
        });

        registrar
            .register_session("slack:T123:D123:1773364194.179929", Some(&metadata))
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert!(requests.contains(&"PUT /api/v1/principals/slack-user-t123-u123".to_owned()));
        assert!(
            requests
                .contains(&"POST /api/v1/principals/prn_user/slack_channel_permissions".to_owned())
        );
        assert!(
            !requests
                .iter()
                .any(|request| request == "POST /api/v1/principals/prn_user/roles"),
            "existing DM principals must not have manually removed roles restored"
        );
        server.abort();
    }

    #[tokio::test]
    async fn ensure_workflow_principal_preserves_labels_without_seeding_roles() {
        let existing_labels = BTreeMap::from([("operator_label".to_owned(), "keep".to_owned())]);
        let (base_url, requests, server) =
            spawn_workflow_principal_stub(Some(existing_labels)).await;
        let registrar =
            SessionRegistrar::new(IronControlClient::new(base_url, "test-key"), "default");

        registrar
            .ensure_workflow_principal("newsletter_daily_digest")
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        let put = requests
            .iter()
            .find(|request| request.method == "PUT")
            .expect("workflow principal upsert");
        assert_eq!(put.body["data"]["labels"]["operator_label"], "keep");
        assert_eq!(put.body["data"]["labels"]["kind"], "workflow");
        assert_eq!(
            put.body["data"]["labels"]["workflow_name"],
            "newsletter_daily_digest"
        );
        assert!(!requests.iter().any(|request| request.method == "POST"));
        server.abort();
    }

    #[test]
    fn slack_permission_for_thread_skips_dm_channel_fallback_without_user() {
        let mut labels = BTreeMap::new();
        labels.insert("slack_channel_id".to_owned(), "D123".to_owned());

        assert_eq!(slack_permission_for_thread("slack:D123:ts", &labels), None);
    }

    async fn spawn_iron_control_stub(
        principal_exists: bool,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let first_line = request.lines().next().unwrap_or_default();
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap_or_default();
                let path = parts.next().unwrap_or_default();
                seen.lock().unwrap().push(format!("{method} {path}"));

                let (status_line, body) = match (method, path) {
                    ("GET", "/api/v1/principals/lookup/default/slack-channel-t123-c123")
                        if principal_exists =>
                    {
                        ("200 OK", channel_principal_body())
                    }
                    ("GET", "/api/v1/principals/lookup/default/slack-user-t123-u123")
                        if principal_exists =>
                    {
                        ("200 OK", user_principal_body())
                    }
                    ("GET", "/api/v1/principals/lookup/default/slack-channel-t123-c123")
                    | ("GET", "/api/v1/principals/lookup/default/slack-user-t123-u123") => {
                        ("404 Not Found", r#"{"error":"not found"}"#.to_owned())
                    }
                    ("PUT", "/api/v1/principals/slack-channel-t123-c123") => {
                        ("200 OK", channel_principal_body())
                    }
                    ("PUT", "/api/v1/principals/slack-user-t123-u123") => {
                        ("200 OK", user_principal_body())
                    }
                    (
                        "POST",
                        "/api/v1/principals/prn_channel/slack_channel_permissions"
                        | "/api/v1/principals/prn_user/slack_channel_permissions",
                    ) => ("200 OK", r#"{"data":{"ok":true}}"#.to_owned()),
                    ("POST", "/api/v1/principals/prn_channel/roles") => {
                        ("200 OK", r#"{"data":{"ok":true}}"#.to_owned())
                    }
                    _ => (
                        "500 Internal Server Error",
                        r#"{"error":"unexpected"}"#.to_owned(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (base_url, requests, handle)
    }

    fn channel_principal_body() -> String {
        r#"{"data":{"id":"prn_channel","namespace":"default","foreign_id":"slack-channel-t123-c123","name":"Slack Channel #general","labels":{}}}"#.to_owned()
    }

    fn user_principal_body() -> String {
        r#"{"data":{"id":"prn_user","namespace":"default","foreign_id":"slack-user-t123-u123","name":"Slack DM @Ada Lovelace","labels":{}}}"#.to_owned()
    }

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        method: String,
        body: Value,
    }

    async fn spawn_workflow_principal_stub(
        existing_labels: Option<BTreeMap<String, String>>,
    ) -> (
        String,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                let (header_end, content_length) = loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                    let Some(header_end) = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|position| position + 4)
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    break (header_end, content_length);
                };
                while request.len() < header_end + content_length {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }

                let headers = String::from_utf8_lossy(&request[..header_end]);
                let first_line = headers.lines().next().unwrap_or_default();
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_owned();
                let path = parts.next().unwrap_or_default().to_owned();
                let body = serde_json::from_slice(
                    &request[header_end..request.len().min(header_end + content_length)],
                )
                .unwrap_or(Value::Null);
                seen.lock().unwrap().push(RecordedRequest {
                    method: method.clone(),
                    body: body.clone(),
                });

                let workflow_lookup =
                    "/api/v1/principals/lookup/default/workflow-newsletter-daily-digest";
                let workflow_upsert = "/api/v1/principals/workflow-newsletter-daily-digest";
                let (status_line, response_body) = match (method.as_str(), path.as_str()) {
                    ("GET", path) if path == workflow_lookup => match &existing_labels {
                        Some(labels) => ("200 OK", workflow_principal_body(labels.clone())),
                        None => ("404 Not Found", r#"{"error":"not found"}"#.to_owned()),
                    },
                    ("PUT", path) if path == workflow_upsert => {
                        let labels = body["data"]["labels"]
                            .as_object()
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .filter_map(|(key, value)| Some((key, value.as_str()?.to_owned())))
                            .collect();
                        ("200 OK", workflow_principal_body(labels))
                    }
                    _ => (
                        "500 Internal Server Error",
                        r#"{"error":"unexpected"}"#.to_owned(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len(),
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (base_url, requests, handle)
    }

    fn workflow_principal_body(labels: BTreeMap<String, String>) -> String {
        json!({
            "data": {
                "id": "prn_workflow",
                "namespace": "default",
                "foreign_id": "workflow-newsletter-daily-digest",
                "name": "Workflow newsletter_daily_digest",
                "labels": labels,
            }
        })
        .to_string()
    }
}
