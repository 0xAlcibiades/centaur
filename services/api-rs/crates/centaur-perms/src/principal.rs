//! Resolve the CLI `--principal` argument into an iron-control identity.

use std::collections::BTreeMap;

use centaur_iron_control::{PrincipalInput, derive_principal, workflow_principal};
use eyre::{Result, bail};

/// Turn a `--principal` value (plus optional `--slack-user`) into the identity
/// to upsert/look up.
///
/// A value containing `:` is treated as a chat thread key and run through the
/// canonical [`derive_principal`], so the resulting `foreign_id` matches exactly
/// what api-rs writes at session start. Any other value is used verbatim as a
/// principal `foreign_id` (e.g. `slack-channel-t1-c9`), so an operator can name
/// an already-registered principal directly.
pub fn resolve_principal(
    principal: &str,
    slack_user: Option<&str>,
    namespace: &str,
) -> PrincipalInput {
    if principal.contains(':') {
        // The CLI has no resolved conversation name; the synthetic display name
        // is fine for operator-driven lookups.
        derive_principal(principal, slack_user, None).to_principal_input(namespace)
    } else {
        PrincipalInput {
            namespace: namespace.to_owned(),
            foreign_id: principal.to_owned(),
            name: principal.to_owned(),
            labels: BTreeMap::from([("managed-by".to_owned(), "centaur".to_owned())]),
            kind: None,
            slack_user_id: None,
            slack_channel_id: None,
            slack_team_id: None,
            slack_email: None,
        }
    }
}

/// Resolve and validate a server-owned workflow principal for pre-granting.
pub fn resolve_workflow_principal(
    principal: &str,
    workflow_name: &str,
    namespace: &str,
) -> Result<PrincipalInput> {
    let identity = workflow_principal(workflow_name).to_principal_input(namespace);
    if principal != identity.foreign_id {
        bail!(
            "workflow {workflow_name:?} requires principal {:?}, not {principal:?}",
            identity.foreign_id
        );
    }
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_key_is_derived() {
        let id = resolve_principal("slack:T123:C456:1780000000.0001", Some("U1"), "default");
        assert_eq!(id.foreign_id, "slack-channel-t123-c456");
    }

    #[test]
    fn dm_thread_key_keys_on_user() {
        let id = resolve_principal("slack:D9:ts", Some("U07ABC"), "default");
        assert_eq!(id.foreign_id, "slack-user-u07abc");
    }

    #[test]
    fn teams_adapter_thread_key_is_derived() {
        let conversation = "MTk6YWJjMTIzQHRocmVhZC50YWN2Mg";
        let service_url = "aHR0cHM6Ly9zbWJhLnRyYWZmaWNtYW5hZ2VyLm5ldC9hbWVyLw";
        let id = resolve_principal(
            &format!("teams:{conversation}:{service_url}"),
            Some("aad-user-1"),
            "default",
        );
        assert_eq!(id.foreign_id, "teams-conversation-19-abc123-thread-tacv2");
    }

    #[test]
    fn teams_adapter_thread_suffix_does_not_change_the_conversation_principal() {
        let conversation = "MTk6YWJjMTIzQHRocmVhZC50YWN2MjttZXNzYWdlaWQ9cm9vdC1tZXNzYWdlLTE";
        let service_url = "aHR0cHM6Ly9zbWJhLnRyYWZmaWNtYW5hZ2VyLm5ldC9hbWVyLw";
        let id = resolve_principal(
            &format!("teams:{conversation}:{service_url}"),
            Some("aad-user-1"),
            "default",
        );
        assert_eq!(id.foreign_id, "teams-conversation-19-abc123-thread-tacv2");
    }

    #[test]
    fn raw_foreign_id_is_verbatim() {
        let id = resolve_principal("slack-channel-t1-c9", None, "default");
        assert_eq!(id.foreign_id, "slack-channel-t1-c9");
        assert_eq!(id.name, "slack-channel-t1-c9");
    }

    #[test]
    fn workflow_foreign_id_and_identity_labels_are_exact() {
        let id = resolve_workflow_principal(
            "workflow-planetscale-daily-audit",
            "planetscale_daily_audit",
            "default",
        )
        .unwrap();
        assert_eq!(id.name, "Workflow planetscale_daily_audit");
        assert_eq!(id.kind.as_deref(), Some("workflow"));
        assert!(!id.labels.contains_key("kind"));
        assert_eq!(
            id.labels.get("workflow_name").map(String::as_str),
            Some("planetscale_daily_audit")
        );
        assert_eq!(
            id.labels.get("managed-by").map(String::as_str),
            Some("centaur")
        );
    }

    #[test]
    fn workflow_foreign_id_mismatch_fails_closed() {
        let error =
            resolve_workflow_principal("workflow-other", "planetscale_daily_audit", "default")
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("workflow-planetscale-daily-audit")
        );
    }
}
