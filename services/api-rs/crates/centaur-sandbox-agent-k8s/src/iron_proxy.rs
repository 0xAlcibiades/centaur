use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use centaur_iron_proxy::{ProxyFragment, SourceKind, SourcePolicy};
use centaur_sandbox_core::{SandboxError, SandboxId, SandboxResult, SandboxSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, EmptyDirVolumeSource, EnvFromSource,
    EnvVar as K8sEnvVar, EnvVarSource, HTTPGetAction, Pod, PodSpec, Probe, SecretEnvSource,
    SecretKeySelector, SecretVolumeSource, SecurityContext, Service, ServicePort, ServiceSpec,
    Volume, VolumeMount,
};
use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ListParams, Patch, PatchParams, PostParams};
use kube::{Api, Resource};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};
use uuid::Uuid;

use crate::{
    API_SERVER_ENABLED_LABEL, AUXILIARY_GENERATION_ANNOTATION, AUXILIARY_GENERATION_LABEL,
    AgentSandboxBackend, MANAGED_BY_LABEL, MANAGED_BY_VALUE, OBSERVABILITY_ENABLED_LABEL,
    OtlpEgressTarget, SANDBOX_ID_LABEL, auxiliary_delete_params, auxiliary_generation_from_sandbox,
    is_conflict, is_not_found, map_kube_error, object_is_owned_by_sandbox,
};

const IRON_PROXY_LABEL: &str = "centaur.ai/iron-proxy";
const IRON_CONTROL_PROXY_ID_ANNOTATION: &str = "centaur.ai/iron-control-proxy-id";
const WORKFLOW_TASK_ID_LABEL: &str = "centaur.workflow_task_id";
const WORKFLOW_TASK_ID_ANNOTATION: &str = "centaur.ai/workflow-task-id";
const WORKFLOW_TASK_ID_ANNOTATION_PATCH_ATTEMPTS: usize = 3;
const FIREWALL_CA_MOUNT_PATH: &str = "/firewall-certs";
pub(crate) const FIREWALL_CA_CERT_PATH: &str = "/firewall-certs/ca-cert.pem";
const PROXY_MANAGEMENT_PORT: u16 = 9092;
const PROXY_HEALTH_PORT: u16 = 9090;
// Managed-mode proxies carry no rendered config; these local listen/TLS
// settings (everything the control plane does not own) are passed as IRON_*
// env vars instead. The CA paths match where the entrypoint copies the
// mounted CA secret.
const PROXY_TUNNEL_PORT: u16 = 8080;
const PROXY_DNS_LISTEN: &str = ":53";
const PROXY_DNS_PROXY_IP: &str = "127.0.0.1";
const PROXY_TLS_MODE: &str = "mitm";
const PROXY_TLS_CA_CERT_PATH: &str = "/etc/iron-proxy/ca.crt";
const PROXY_TLS_CA_KEY_PATH: &str = "/etc/iron-proxy/ca.key";
const PROXY_UPSTREAM_RESPONSE_HEADER_TIMEOUT: &str = "120s";
const PROXY_UPSTREAM_DENY_CIDRS_ENV: &str = "IRON_PROXY_UPSTREAM_DENY_CIDRS";
const PROXY_LOG_LEVEL: &str = "info";
// iron-control multiplexes every Postgres upstream through a single listener,
// routing by database name; the control plane owns each upstream DSN/role/
// database. api-rs binds one local port (matching the chart's pgPort) and one
// shared client credential (random per sandbox) the sandbox presents on every
// DSN. These are the deploy-level env vars iron-proxy reads for that listener.
const PG_LISTENER_PORT: u16 = 5432;
const CENTAUR_POSTGRES_DSN_ENV: &str = "CENTAUR_POSTGRES_DSN";
const CENTAUR_CONSOLE_URL_ENV: &str = "CENTAUR_CONSOLE_URL";
const PG_LISTEN_ENV: &str = "IRON_PROXY_PG_LISTEN";
const PG_CLIENT_USER_ENV: &str = "IRON_PROXY_PG_CLIENT_USER";
const PG_CLIENT_PASSWORD_ENV: &str = "IRON_PROXY_PG_CLIENT_PASSWORD";
// Managed iron-proxy instances pick up principal/config changes on their next
// /proxy/sync poll (5s cadence upstream). Claiming a warm sandbox must not
// return before the proxy has applied the session principal's config: the
// harness fires its first LLM call within milliseconds of stdin, and an
// un-applied config sends the placeholder credential upstream (observed as
// Anthropic 401s when the first call beat the poll by ~350ms).
//
// The claim barrier asks the proxy directly: POST /v1/sync (immediate
// out-of-band sync), then poll GET /v1/status until the applied principal
// matches. Proxy images without the managed-mode management API never answer
// on the management port; after PROXY_ACK_PROBE_WINDOW of failed probes the
// barrier falls back to the blind delay that covers a full poll interval plus
// apply latency (the pre-barrier behavior).
const PROXY_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_ACK_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PROXY_ACK_PROBE_WINDOW: Duration = Duration::from_secs(2);
const PROXY_REASSIGN_FALLBACK_DELAY: Duration = Duration::from_secs(6);

#[derive(Clone, Debug)]
pub struct IronProxyConfig {
    pub image: String,
    pub image_pull_policy: Option<String>,
    pub fragments: Vec<ProxyFragment>,
    pub source_policy: SourcePolicy,
    pub ca_cert_secret_name: String,
    pub ca_key_secret_name: String,
    pub env_from_secret_names: Vec<String>,
    /// Individual source-authentication keys mounted into a non-environment
    /// proxy. Never put the static infra Secret in `envFrom` for these modes.
    pub secret_env: Vec<IronProxySecretEnv>,
    pub extra_env: BTreeMap<String, String>,
    pub upstream_deny_cidrs: Vec<String>,
    pub op_connect_app_name: String,
    pub op_connect_port: u16,
    pub api_pod_labels: BTreeMap<String, String>,
    pub control_plane_pod_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IronProxySecretEnv {
    pub name: String,
    pub secret_name: String,
    pub secret_key: String,
}

impl IronProxyConfig {
    pub fn new(
        image: impl Into<String>,
        ca_cert_secret_name: impl Into<String>,
        ca_key_secret_name: impl Into<String>,
    ) -> Self {
        Self {
            image: image.into(),
            image_pull_policy: None,
            fragments: Vec::new(),
            source_policy: SourcePolicy::default(),
            ca_cert_secret_name: ca_cert_secret_name.into(),
            ca_key_secret_name: ca_key_secret_name.into(),
            env_from_secret_names: Vec::new(),
            secret_env: Vec::new(),
            extra_env: BTreeMap::new(),
            upstream_deny_cidrs: Vec::new(),
            op_connect_app_name: "onepassword-connect".to_owned(),
            op_connect_port: 8080,
            api_pod_labels: BTreeMap::from([(
                "app.kubernetes.io/component".to_owned(),
                "api".to_owned(),
            )]),
            control_plane_pod_labels: BTreeMap::from([(
                "app.kubernetes.io/component".to_owned(),
                "console".to_owned(),
            )]),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedIronProxy {
    proxy_host: String,
    proxy_pod_name: String,
    proxy_port: u16,
    console_url: String,
    // iron-control principal OID this sandbox's proxy binds to.
    principal_id: String,
    // Labels applied to the iron-control proxy row and used by proxy-specific
    // config rendering in the control plane.
    labels: BTreeMap<String, String>,
    // The single Postgres listener the proxy multiplexes all upstreams through,
    // derived from the principal's effective config. `None` when the principal
    // resolves to no Postgres upstreams. The upstream DSN/role/database are
    // control-plane-owned; api-rs assigns the local listen/client knobs
    // (IRON_PROXY_PG_* + the per-upstream sandbox DSN env vars).
    pg: Option<ResolvedPg>,
    // Replace-secret placeholders the operator granted the principal
    // (`proxy_value` -> same), set as sandbox env so tools send the value the
    // proxy swaps. Infra placeholders are set separately from the known set.
    replace_placeholders: BTreeMap<String, String>,
    // Bearer key for the proxy's management API (/v1/status, /v1/sync),
    // random per proxy pod. The claim barrier reads it back off the live pod
    // env, so it survives api-rs restarts and respects env overrides.
    management_api_key: String,
    observability_enabled: bool,
    api_server_enabled: bool,
}

struct ResolvedIronProxyRuntime {
    pg: Option<ResolvedPg>,
    replace_placeholders: BTreeMap<String, String>,
    observability_enabled: bool,
    api_server_enabled: bool,
}

/// The single Postgres listener the proxy multiplexes every upstream through.
/// iron-control owns each upstream DSN/role/database and routes by database
/// name; api-rs only assigns the local listen port and the shared client
/// credential the sandbox presents (random per sandbox).
#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedPg {
    /// Local listen address the proxy binds (e.g. ``0.0.0.0:5432``).
    listen: String,
    /// Listen port, exposed on the proxy Service and allowed sandbox→proxy.
    port: u16,
    /// Shared client user the sandbox connects as (random per sandbox).
    user: String,
    /// Shared client password (random per sandbox); set on both the proxy
    /// (CLIENT_PASSWORD) and every sandbox DSN so the two agree.
    password: String,
}

/// Env injected into a managed proxy pod so iron-proxy pulls its config from
/// iron-control instead of any local file.
struct ProxySyncEnv {
    proxy_id: String,
    control_url: String,
    token: String,
    config_hash: Option<String>,
}

struct ControlPlaneEgressTarget {
    peer: NetworkPolicyPeer,
    port: u16,
}

impl AgentSandboxBackend {
    pub(crate) async fn resolve_iron_proxy(
        &self,
        id: &SandboxId,
        spec: &SandboxSpec,
    ) -> SandboxResult<Option<ResolvedIronProxy>> {
        if self.config.iron_proxy.is_none() {
            return Ok(None);
        }
        // iron-control is the only mode: the proxy pulls its entire effective
        // config from iron-control over `/proxy/sync`, so no config is rendered
        // locally — the remaining local settings are passed as IRON_* env vars
        // on the pod. The sandbox must carry the principal its proxy binds to.
        if self.config.iron_control.is_none() {
            return Err(SandboxError::InvalidSpec(
                "iron-proxy requires iron-control to be configured".to_owned(),
            ));
        }
        let principal_id = spec.iron_control_principal.clone().ok_or_else(|| {
            SandboxError::InvalidSpec(
                "iron-proxy sandbox spec is missing its iron-control principal".to_owned(),
            )
        })?;
        let pg = self.resolved_pg();
        let replace_placeholders = self.effective_replace_placeholders(&principal_id).await?;
        let labels = spec.iron_control_proxy_labels.clone();

        Ok(Some(self.resolved_iron_proxy_for_principal(
            id,
            principal_id,
            labels,
            ResolvedIronProxyRuntime {
                pg,
                replace_placeholders,
                observability_enabled: spec.capabilities.observability_enabled,
                api_server_enabled: spec.capabilities.api_server_enabled,
            },
        )))
    }

    /// Read the principal's effective config from iron-control for the
    /// replace-secret placeholders set as sandbox env (so tools send the value
    /// the proxy swaps for the real secret). The Postgres DSN catalog is
    /// provided as one fixed local DSN instead — see [`Self::resolved_pg`].
    async fn effective_replace_placeholders(
        &self,
        principal: &str,
    ) -> SandboxResult<BTreeMap<String, String>> {
        let Some(iron_control) = self.config.iron_control.as_ref() else {
            return Ok(BTreeMap::new());
        };
        let effective = iron_control
            .client
            .effective_config(&iron_control.namespace, principal)
            .await
            .map_err(|err| SandboxError::backend_source("iron-control effective_config", err))?;

        Ok(effective
            .secrets
            .iter()
            .filter_map(|secret| secret.replace.as_ref())
            .map(|replace| replace.proxy_value.trim().to_owned())
            .filter(|value| !value.is_empty() && !value.contains('='))
            .map(|value| (value.clone(), value))
            .collect())
    }

    /// Build the single local Postgres listener every managed iron-proxy
    /// exposes. The sandbox always receives one database-less base DSN; tools
    /// choose the database name, and iron-control decides which upstream
    /// credential/role backs that database for the currently assigned
    /// principal.
    fn resolved_pg(&self) -> Option<ResolvedPg> {
        self.config.iron_proxy.as_ref()?;
        Some(ResolvedPg {
            listen: format!("0.0.0.0:{PG_LISTENER_PORT}"),
            port: PG_LISTENER_PORT,
            user: format!("pg-user-{}", uuid::Uuid::new_v4().simple()),
            password: format!("pg-{}", uuid::Uuid::new_v4().simple()),
        })
    }

    /// Resolve the proxy for a resume, where only the sandbox id is known.
    /// Rebinds to the principal stamped on the sandbox at create (read back off
    /// its annotation, so it survives pause and api-rs restarts). Returns `None`
    /// when the sandbox has no proxy or carries no principal annotation.
    pub(crate) async fn resolve_iron_proxy_for_resume(
        &self,
        id: &SandboxId,
    ) -> SandboxResult<Option<ResolvedIronProxy>> {
        if self.config.iron_proxy.is_none() {
            return Ok(None);
        }
        let sandbox = self
            .sandboxes()
            .get(id.as_str())
            .await
            .map_err(|err| map_kube_error("get sandbox for resume", err))?;
        let principal_id = sandbox
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(crate::IRON_CONTROL_PRINCIPAL_ANNOTATION))
            .cloned();
        let Some(principal_id) = principal_id else {
            return Ok(None);
        };
        let pg = self.resolved_pg_for_recreation(Some(&sandbox));
        let replace_placeholders = self.effective_replace_placeholders(&principal_id).await?;
        let observability_enabled = sandbox_observability_enabled(&sandbox, &self.config.container_name)
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    container_name = self.config.container_name.as_str(),
                    "sandbox observability capability env is missing or invalid; defaulting to enabled network policy"
                );
                true
            });
        let api_server_enabled = sandbox_api_server_enabled(&sandbox, &self.config.container_name)
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    container_name = self.config.container_name.as_str(),
                    "sandbox API server capability env is missing or invalid; defaulting to enabled network policy"
                );
                true
            });
        Ok(Some(self.resolved_iron_proxy_for_principal(
            id,
            principal_id,
            BTreeMap::new(),
            ResolvedIronProxyRuntime {
                pg,
                replace_placeholders,
                observability_enabled,
                api_server_enabled,
            },
        )))
    }

    fn resolved_iron_proxy_for_principal(
        &self,
        id: &SandboxId,
        principal_id: String,
        labels: BTreeMap<String, String>,
        runtime: ResolvedIronProxyRuntime,
    ) -> ResolvedIronProxy {
        ResolvedIronProxy {
            proxy_host: iron_proxy_service_name(id),
            proxy_pod_name: new_iron_proxy_pod_name(id),
            proxy_port: PROXY_TUNNEL_PORT,
            console_url: self
                .config
                .iron_control
                .as_ref()
                .map(|settings| settings.control_url.clone())
                .unwrap_or_default(),
            principal_id,
            labels,
            pg: runtime.pg,
            replace_placeholders: runtime.replace_placeholders,
            management_api_key: new_proxy_management_api_key(),
            observability_enabled: runtime.observability_enabled,
            api_server_enabled: runtime.api_server_enabled,
        }
    }

    pub(crate) async fn create_iron_proxy_resources(
        &self,
        id: &SandboxId,
        resolved: Option<&ResolvedIronProxy>,
        generation: &str,
    ) -> SandboxResult<()> {
        let (Some(resolved), Some(iron_proxy)) = (resolved, self.config.iron_proxy.as_ref()) else {
            return Ok(());
        };
        self.delete_iron_proxy_resources(id, generation).await?;
        let sync = self.register_sync_proxy(id, resolved, generation).await?;
        self.services()
            .create(
                &PostParams::default(),
                &build_iron_proxy_service_with_generation(id, resolved, generation),
            )
            .await
            .map_err(|err| map_kube_error("create iron-proxy service", err))?;
        let control_target = control_plane_egress_target(
            &sync.control_url,
            &self.config.namespace,
            iron_proxy.control_plane_pod_labels.clone(),
        );
        for policy in build_iron_proxy_network_policies_with_generation(
            id,
            resolved,
            generation,
            iron_proxy,
            &control_target,
            self.config.otlp_egress.as_ref(),
            resolved.observability_enabled,
        ) {
            self.network_policies()
                .create(&PostParams::default(), &policy)
                .await
                .map_err(|err| map_kube_error("create iron-proxy network policy", err))?;
        }
        self.pods()
            .create(
                &PostParams::default(),
                &build_iron_proxy_pod_with_generation(id, iron_proxy, resolved, &sync, generation),
            )
            .await
            .map_err(|err| map_kube_error("create iron-proxy pod", err))?;
        self.wait_until_proxy_running(resolved).await?;
        self.wait_for_cold_proxy_principal_applied(
            id,
            generation,
            &resolved.principal_id,
            sync.config_hash.as_deref(),
            &resolved.labels,
        )
        .await?;
        Ok(())
    }

    /// Register a per-sandbox proxy in iron-control and return the env (URL +
    /// `iprx_` token) to inject. The proxy OID is recorded so it can be
    /// deregistered on stop.
    async fn register_sync_proxy(
        &self,
        id: &SandboxId,
        resolved: &ResolvedIronProxy,
        generation: &str,
    ) -> SandboxResult<ProxySyncEnv> {
        let iron_control = self.config.iron_control.as_ref().ok_or_else(|| {
            SandboxError::backend("iron-proxy requires iron-control to be configured")
        })?;
        let proxy = iron_control
            .client
            .create_proxy(id.as_str(), &resolved.principal_id, resolved.labels.clone())
            .await
            .map_err(|err| SandboxError::backend_source("iron-control create proxy", err))?;
        let token = proxy
            .token
            .ok_or_else(|| SandboxError::backend("iron-control create proxy returned no token"))?;
        self.proxy_ids.lock().await.insert(
            id.as_str().to_owned(),
            crate::ProxyMapping {
                generation: generation.to_owned(),
                proxy_id: proxy.id.clone(),
            },
        );
        Ok(ProxySyncEnv {
            proxy_id: proxy.id,
            control_url: iron_control.control_url.clone(),
            token,
            config_hash: proxy.config_hash,
        })
    }

    /// Bind the per-sandbox proxy resources (pods, service, network policies)
    /// to the Sandbox CR with ownerReferences so Kubernetes garbage-collects
    /// them when the sandbox is deleted out-of-band (operator cleanup, a
    /// future shutdownPolicy). They are created before the Sandbox CR exists
    /// (the egress policies must precede the pod), so this runs as a separate
    /// patch once the CR is available.
    pub(crate) async fn adopt_iron_proxy_resources(
        &self,
        id: &SandboxId,
        sandbox: &crate::crd::Sandbox,
        generation: &str,
    ) -> SandboxResult<()> {
        let Some(owner_reference) = sandbox_owner_reference(sandbox) else {
            return Ok(());
        };
        let params = PatchParams::default();
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&format!(
                "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
                id.as_str(),
            )))
            .await
            .map_err(|err| map_kube_error("list iron-proxy pods for adoption", err))?;
        for pod in pods.items {
            let Some(name) = pod.metadata.name.as_ref() else {
                continue;
            };
            if auxiliary_delete_params(&pod.metadata, generation)?.is_none() {
                continue;
            }
            let patch = owner_reference_patch(&pod.metadata, &owner_reference, None)?;
            match self.pods().patch(name, &params, &patch).await {
                Ok(_) => {}
                Err(err) if is_not_found(&err) || is_conflict(&err) => {}
                Err(err) => return Err(map_kube_error("adopt iron-proxy pod", err)),
            }
        }
        let service_name = iron_proxy_service_name(id);
        let service_matches = match self.services().get(&service_name).await {
            Ok(service) => Some(auxiliary_delete_params(&service.metadata, generation)?),
            Err(err) if is_not_found(&err) => None,
            Err(err) => return Err(map_kube_error("get iron-proxy service for adoption", err)),
        };
        if let Some(Some(delete_params)) = service_matches {
            let patch =
                owner_reference_patch_from_delete_params(delete_params, &owner_reference, None);
            match self.services().patch(&service_name, &params, &patch).await {
                Ok(_) => {}
                Err(err) if is_not_found(&err) || is_conflict(&err) => {}
                Err(err) => return Err(map_kube_error("adopt iron-proxy service", err)),
            }
        }
        for name in [
            iron_proxy_sandbox_egress_policy_name(id),
            iron_proxy_policy_name(id),
        ] {
            let policy_matches = match self.network_policies().get(&name).await {
                Ok(policy) => Some(auxiliary_delete_params(&policy.metadata, generation)?),
                Err(err) if is_not_found(&err) => None,
                Err(err) => {
                    return Err(map_kube_error(
                        "get iron-proxy network policy for adoption",
                        err,
                    ));
                }
            };
            if let Some(Some(delete_params)) = policy_matches {
                let patch =
                    owner_reference_patch_from_delete_params(delete_params, &owner_reference, None);
                match self.network_policies().patch(&name, &params, &patch).await {
                    Ok(_) => {}
                    Err(err) if is_not_found(&err) || is_conflict(&err) => {}
                    Err(err) => {
                        return Err(map_kube_error("adopt iron-proxy network policy", err));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn adopt_legacy_iron_proxy_resources(
        &self,
        id: &SandboxId,
        sandbox: &crate::crd::Sandbox,
        generation: &str,
        sandbox_pod_isolation_ready: bool,
    ) -> SandboxResult<()> {
        let Some(owner_reference) = sandbox_owner_reference(sandbox) else {
            return Ok(());
        };
        let params = PatchParams::default();
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&format!(
                "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={}",
                id.as_str()
            )))
            .await
            .map_err(|err| map_kube_error("list legacy iron-proxy pods", err))?;
        for pod in pods.items {
            let Some(name) = pod.metadata.name.as_ref() else {
                continue;
            };
            if !legacy_auxiliary_is_adoptable(&pod.metadata, sandbox) {
                continue;
            }
            let patch = owner_reference_patch(&pod.metadata, &owner_reference, Some(generation))?;
            match self.pods().patch(name, &params, &patch).await {
                Ok(_) => {}
                Err(err) if is_not_found(&err) => {}
                Err(err) => return Err(map_kube_error("adopt legacy iron-proxy pod", err)),
            }
        }
        self.adopt_legacy_iron_proxy_service(id, sandbox, &owner_reference, generation)
            .await?;
        for name in [
            iron_proxy_sandbox_egress_policy_name(id),
            iron_proxy_policy_name(id),
        ] {
            if name == iron_proxy_sandbox_egress_policy_name(id) && !sandbox_pod_isolation_ready {
                continue;
            }
            self.adopt_legacy_iron_proxy_network_policy(
                &name,
                sandbox,
                &owner_reference,
                generation,
            )
            .await?;
        }
        Ok(())
    }

    async fn adopt_legacy_iron_proxy_service(
        &self,
        id: &SandboxId,
        sandbox: &crate::crd::Sandbox,
        owner_reference: &Value,
        generation: &str,
    ) -> SandboxResult<()> {
        let api = self.services();
        let name = iron_proxy_service_name(id);
        let service = match api.get(&name).await {
            Ok(service) => service,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => return Err(map_kube_error("get legacy iron-proxy service", err)),
        };
        if !legacy_auxiliary_is_adoptable(&service.metadata, sandbox) {
            return Ok(());
        }
        let patch = service_generation_patch(&service, owner_reference, generation)?;
        match api.patch(&name, &PatchParams::default(), &patch).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error("adopt legacy iron-proxy service", err)),
        }
    }

    async fn adopt_legacy_iron_proxy_network_policy(
        &self,
        name: &str,
        sandbox: &crate::crd::Sandbox,
        owner_reference: &Value,
        generation: &str,
    ) -> SandboxResult<()> {
        let api = self.network_policies();
        let policy = match api.get(name).await {
            Ok(policy) => policy,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => return Err(map_kube_error("get legacy iron-proxy network policy", err)),
        };
        if !legacy_auxiliary_is_adoptable(&policy.metadata, sandbox) {
            return Ok(());
        }
        let patch = network_policy_generation_patch(&policy, owner_reference, generation)?;
        match api.patch(name, &PatchParams::default(), &patch).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error(
                "adopt legacy iron-proxy network policy",
                err,
            )),
        }
    }

    pub(crate) async fn delete_iron_proxy_resources(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<()> {
        // Deliberately not gated on iron_proxy being configured: the resources
        // may exist from a previous configuration, and deleting absent ones is
        // a no-op.
        //
        // Deregister the iron-control proxy first (best-effort): once the pod is
        // gone the token is useless, and a stale proxy row just fails to sync.
        let mapping = self
            .proxy_ids
            .lock()
            .await
            .get(id.as_str())
            .filter(|mapping| mapping.generation == generation)
            .cloned();
        if let Some(mapping) = mapping {
            if let Some(iron_control) = self.config.iron_control.as_ref() {
                let _ = iron_control.client.delete_proxy(&mapping.proxy_id).await;
            }
            let mut proxy_ids = self.proxy_ids.lock().await;
            crate::remove_proxy_mapping_if_generation(&mut proxy_ids, id.as_str(), generation);
        }
        self.delete_iron_proxy_pods_for_sandbox(id, generation)
            .await?;
        self.delete_iron_proxy_service(id, generation).await?;
        for name in [
            iron_proxy_sandbox_egress_policy_name(id),
            iron_proxy_policy_name(id),
        ] {
            self.delete_iron_proxy_network_policy(&name, generation)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn assign_proxy_principal(
        &self,
        id: &SandboxId,
        principal_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        let iron_control = self
            .config
            .iron_control
            .as_ref()
            .ok_or(SandboxError::Unsupported {
                backend: crate::BACKEND_NAME,
                operation: "assign_iron_control_proxy_principal",
            })?;
        let sandbox = self
            .sandboxes()
            .get(id.as_str())
            .await
            .map_err(|err| map_kube_error("get sandbox for proxy assignment", err))?;
        let (_, generation) = self.ensure_auxiliary_generation(id, sandbox).await?;
        let mut proxy_id = self.proxy_id_for_sandbox(id, &generation).await?;
        if proxy_id.is_none()
            || !self
                .has_usable_iron_proxy_resources(id, &generation)
                .await?
        {
            tracing::warn!(
                sandbox_id = id.as_str(),
                principal_id,
                "iron-proxy resources are missing or not running; recreating before assignment"
            );
            proxy_id = Some(
                self.recreate_iron_proxy_resources_for_principal(id, principal_id, labels)
                    .await?,
            );
        }
        let proxy_id = proxy_id.ok_or_else(|| {
            SandboxError::backend(format!(
                "iron-control proxy id for sandbox {} was not found after repair",
                id.as_str()
            ))
        })?;
        let proxy = iron_control
            .client
            .assign_proxy_principal(&proxy_id, principal_id, labels)
            .await
            .map_err(|err| SandboxError::backend_source("iron-control assign proxy", err))?;
        self.proxy_ids.lock().await.insert(
            id.as_str().to_owned(),
            crate::ProxyMapping {
                generation: generation.clone(),
                proxy_id: proxy.id,
            },
        );
        self.patch_iron_control_principal_annotation(id, &generation, principal_id)
            .await?;
        self.patch_proxy_workflow_task_id_annotation(id, &generation, labels)
            .await?;
        self.wait_for_proxy_principal_applied(
            id,
            &generation,
            principal_id,
            proxy.config_hash.as_deref(),
            labels,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn ensure_proxy_resources_for_principal(
        &self,
        id: &SandboxId,
        principal_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        if self.config.iron_proxy.is_none() {
            return Ok(());
        }
        if self.config.iron_control.is_none() {
            return Err(SandboxError::Unsupported {
                backend: crate::BACKEND_NAME,
                operation: "ensure_iron_control_proxy_resources",
            });
        }
        let sandbox = self
            .sandboxes()
            .get(id.as_str())
            .await
            .map_err(|err| map_kube_error("get sandbox for proxy principal check", err))?;
        let (_sandbox, generation) = self.ensure_auxiliary_generation(id, sandbox).await?;
        let proxy_id = self.proxy_id_for_sandbox(id, &generation).await?;
        if let Some(proxy_id) = proxy_id
            && self
                .has_usable_iron_proxy_resources(id, &generation)
                .await?
        {
            let iron_control = self.config.iron_control.as_ref().ok_or_else(|| {
                SandboxError::backend("iron-proxy requires iron-control to be configured")
            })?;
            let proxy = iron_control
                .client
                .assign_proxy_principal(&proxy_id, principal_id, labels)
                .await
                .map_err(|err| SandboxError::backend_source("iron-control assign proxy", err))?;
            self.patch_proxy_workflow_task_id_annotation(id, &generation, labels)
                .await?;
            self.wait_for_proxy_principal_applied(
                id,
                &generation,
                principal_id,
                proxy.config_hash.as_deref(),
                labels,
            )
            .await?;
            return Ok(());
        }

        tracing::warn!(
            sandbox_id = id.as_str(),
            principal_id,
            "iron-proxy resources are missing or not running; recreating before reuse"
        );
        self.recreate_iron_proxy_resources_for_principal(id, principal_id, labels)
            .await?;
        self.patch_iron_control_principal_annotation(id, &generation, principal_id)
            .await?;
        self.patch_proxy_workflow_task_id_annotation(id, &generation, labels)
            .await?;
        Ok(())
    }

    async fn recreate_iron_proxy_resources_for_principal(
        &self,
        id: &SandboxId,
        principal_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<String> {
        if self.config.iron_proxy.is_none() {
            return Err(SandboxError::Unsupported {
                backend: crate::BACKEND_NAME,
                operation: "assign_iron_control_proxy_principal",
            });
        }
        let sandbox = self
            .sandboxes()
            .get(id.as_str())
            .await
            .map_err(|err| map_kube_error("get sandbox for iron-proxy repair", err))?;
        let (sandbox, generation) = self.ensure_auxiliary_generation(id, sandbox).await?;
        let pg = self.resolved_pg_for_recreation(Some(&sandbox));
        let principal_id = principal_id.to_owned();
        let replace_placeholders = self.effective_replace_placeholders(&principal_id).await?;
        let observability_enabled = sandbox_observability_enabled(&sandbox, &self.config.container_name)
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    container_name = self.config.container_name.as_str(),
                    "sandbox observability capability env is missing or invalid during proxy repair; defaulting to enabled network policy"
                );
                true
            });
        let api_server_enabled = sandbox_api_server_enabled(&sandbox, &self.config.container_name)
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    container_name = self.config.container_name.as_str(),
                    "sandbox API server capability env is missing or invalid during proxy repair; defaulting to enabled network policy"
                );
                true
            });
        let resolved = self.resolved_iron_proxy_for_principal(
            id,
            principal_id,
            labels.clone(),
            ResolvedIronProxyRuntime {
                pg,
                replace_placeholders,
                observability_enabled,
                api_server_enabled,
            },
        );
        self.create_iron_proxy_resources(id, Some(&resolved), &generation)
            .await?;
        if let Err(error) = self
            .adopt_iron_proxy_resources(id, &sandbox, &generation)
            .await
        {
            tracing::warn!(
                sandbox_id = id.as_str(),
                %error,
                "failed to set ownerReferences on recreated iron-proxy resources"
            );
        }
        self.proxy_ids
            .lock()
            .await
            .get(id.as_str())
            .filter(|mapping| mapping.generation == generation)
            .map(|mapping| mapping.proxy_id.clone())
            .ok_or_else(|| {
                SandboxError::backend(format!(
                    "iron-control proxy id for sandbox {} was not recorded after repair",
                    id.as_str()
                ))
            })
    }

    /// Reuse the Postgres client credential already stored on an existing
    /// sandbox: recreating only its proxy does not update the sandbox pod spec.
    fn resolved_pg_for_recreation(
        &self,
        sandbox: Option<&crate::crd::Sandbox>,
    ) -> Option<ResolvedPg> {
        let fallback = self.resolved_pg()?;
        sandbox
            .and_then(|sandbox| {
                pg_from_sandbox_env(
                    sandbox,
                    &self.config.container_name,
                    &fallback.listen,
                    fallback.port,
                )
            })
            .or(Some(fallback))
    }

    /// Barrier between reassigning the proxy principal in iron-control and
    /// returning the claimed sandbox: the caller writes stdin (and the harness
    /// fires its first credentialed call) immediately after, so the proxy must
    /// be serving the claimed principal's config by then, not the warm
    /// bootstrap principal's empty one. Pokes the proxy to sync now and waits
    /// until it reports the principal applied; proxy images without the
    /// managed-mode management API fall back to a fixed delay. Never fails the
    /// claim: managed proxies fail closed until synced, so the worst case is a
    /// brief 503 window rather than a failed execution.
    async fn wait_for_proxy_principal_applied(
        &self,
        id: &SandboxId,
        generation: &str,
        principal_id: &str,
        config_hash: Option<&str>,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        let strict = requires_strict_autorotate_proxy_ack(labels);
        let started = Instant::now();
        match self
            .proxy_principal_ack(
                id,
                generation,
                principal_id,
                config_hash,
                strict,
                "claim barrier",
            )
            .await
        {
            Ok(ProxyAck::Applied) => {
                tracing::info!(
                    sandbox_id = id.as_str(),
                    principal_id,
                    barrier = "claim barrier",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "iron-proxy acknowledged the claimed principal's config"
                );
                Ok(())
            }
            Ok(ProxyAck::ManagementUnavailable) => {
                if strict {
                    return Err(SandboxError::NotReady(
                        "autorotate credential pin requires a proxy config acknowledgement"
                            .to_owned(),
                    ));
                }
                tracing::info!(
                    sandbox_id = id.as_str(),
                    "iron-proxy management API is unavailable (image without \
                     managed status support?); using the fixed reassign delay"
                );
                sleep(proxy_fallback_delay_remaining(started.elapsed())).await;
                Ok(())
            }
            Ok(ProxyAck::TimedOut) => {
                if strict {
                    return Err(SandboxError::NotReady(
                        "autorotate credential pin proxy acknowledgement timed out".to_owned(),
                    ));
                }
                // The ack timeout already waited longer than the fixed
                // fallback delay, so do not add another sleep here.
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    principal_id,
                    "iron-proxy did not acknowledge the claimed principal's \
                     config before the deadline; proceeding (managed proxies \
                     fail closed until synced)"
                );
                Ok(())
            }
            Err(error) => {
                if strict {
                    return Err(error);
                }
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    %error,
                    "failed to check the iron-proxy management API for the \
                     claim barrier; using the fixed reassign delay"
                );
                sleep(proxy_fallback_delay_remaining(started.elapsed())).await;
                Ok(())
            }
        }
    }

    /// Cold-created sandboxes do not go through the warm-pool claim barrier,
    /// but the harness can make credentialed calls immediately after create
    /// returns. Ask the proxy to report the requested principal's config before
    /// creating the sandbox pod. If the management API cannot prove readiness,
    /// fall back to the fixed delay instead of failing the sandbox create.
    async fn wait_for_cold_proxy_principal_applied(
        &self,
        id: &SandboxId,
        generation: &str,
        principal_id: &str,
        config_hash: Option<&str>,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        let strict = requires_strict_autorotate_proxy_ack(labels);
        let started = Instant::now();
        match self
            .proxy_principal_ack(
                id,
                generation,
                principal_id,
                config_hash,
                strict,
                "cold create barrier",
            )
            .await
        {
            Ok(ProxyAck::Applied) => {
                tracing::info!(
                    sandbox_id = id.as_str(),
                    principal_id,
                    barrier = "cold create barrier",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "iron-proxy acknowledged the claimed principal's config"
                );
                Ok(())
            }
            Ok(ProxyAck::ManagementUnavailable) => {
                if strict {
                    return Err(SandboxError::NotReady(
                        "autorotate credential pin requires a proxy config acknowledgement"
                            .to_owned(),
                    ));
                }
                tracing::info!(
                    sandbox_id = id.as_str(),
                    "iron-proxy management API is unavailable (image without \
                     managed status support?); using the fixed cold-create delay"
                );
                sleep(proxy_fallback_delay_remaining(started.elapsed())).await;
                Ok(())
            }
            Ok(ProxyAck::TimedOut) => {
                if strict {
                    return Err(SandboxError::NotReady(
                        "autorotate credential pin proxy acknowledgement timed out".to_owned(),
                    ));
                }
                // The ack timeout already waited longer than the fixed
                // fallback delay, so do not add another sleep here.
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    principal_id,
                    "iron-proxy did not acknowledge the cold-created principal's \
                     config before the deadline; proceeding (managed proxies \
                     fail closed until synced)"
                );
                Ok(())
            }
            Err(error) => {
                if strict {
                    return Err(error);
                }
                tracing::warn!(
                    sandbox_id = id.as_str(),
                    %error,
                    "failed to check the iron-proxy management API for the \
                     cold create barrier; using the fixed cold-create delay"
                );
                sleep(proxy_fallback_delay_remaining(started.elapsed())).await;
                Ok(())
            }
        }
    }

    async fn proxy_principal_ack(
        &self,
        id: &SandboxId,
        generation: &str,
        principal_id: &str,
        config_hash: Option<&str>,
        strict_config_hash: bool,
        barrier: &'static str,
    ) -> SandboxResult<ProxyAck> {
        let endpoint = match self.proxy_management_endpoint(id, generation).await {
            Ok(Some(endpoint)) => endpoint,
            Ok(None) => {
                return Err(SandboxError::NotReady(format!(
                    "no running iron-proxy pod found for the {barrier}"
                )));
            }
            Err(error) => return Err(error),
        };
        let Ok(client) = reqwest::Client::builder()
            // Pod-IP call inside the cluster: never route via env-configured
            // HTTP proxies.
            .no_proxy()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .build()
        else {
            return Err(SandboxError::backend(
                "failed to build iron-proxy management client",
            ));
        };
        Ok(wait_for_proxy_ack(
            &client,
            &endpoint,
            principal_id,
            config_hash,
            strict_config_hash,
            PROXY_ACK_TIMEOUT,
            PROXY_ACK_PROBE_WINDOW,
            PROXY_ACK_POLL_INTERVAL,
        )
        .await)
    }

    /// Locate the management API of the sandbox's running proxy pod. The
    /// address (pod IP + IRON_MANAGEMENT_LISTEN port) and bearer key are read
    /// back off the pod itself so the barrier always speaks to what the pod
    /// was actually given.
    async fn proxy_management_endpoint(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<Option<ProxyManagementEndpoint>> {
        let params = ListParams::default().labels(&format!(
            "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
            id.as_str(),
        ));
        let pods = self
            .pods()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list iron-proxy pods", err))?;
        Ok(pods
            .items
            .iter()
            .find_map(proxy_management_endpoint_from_pod))
    }

    async fn proxy_id_for_sandbox(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<Option<String>> {
        if let Some(proxy_id) = self
            .proxy_ids
            .lock()
            .await
            .get(id.as_str())
            .filter(|mapping| mapping.generation == generation)
            .map(|mapping| mapping.proxy_id.clone())
        {
            return Ok(Some(proxy_id));
        }
        let params = ListParams::default().labels(&format!(
            "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
            id.as_str(),
        ));
        let pods = self
            .pods()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list iron-proxy pods", err))?;
        for pod in pods.items {
            if let Some(proxy_id) = pod
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(IRON_CONTROL_PROXY_ID_ANNOTATION))
                .filter(|value| !value.trim().is_empty())
            {
                let proxy_id = proxy_id.to_owned();
                if auxiliary_delete_params(&pod.metadata, generation)?.is_none() {
                    continue;
                }
                self.proxy_ids.lock().await.insert(
                    id.as_str().to_owned(),
                    crate::ProxyMapping {
                        generation: generation.to_owned(),
                        proxy_id: proxy_id.clone(),
                    },
                );
                return Ok(Some(proxy_id));
            }
        }
        Ok(None)
    }

    async fn has_usable_iron_proxy_resources(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<bool> {
        let params = ListParams::default().labels(&format!(
            "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
            id.as_str(),
        ));
        let pods = self
            .pods()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list iron-proxy pods", err))?;
        if !pods.items.iter().any(|pod| {
            auxiliary_delete_params(&pod.metadata, generation)
                .ok()
                .flatten()
                .is_some()
                && pod_running(pod)
        }) {
            return Ok(false);
        }
        match self.services().get(&iron_proxy_service_name(id)).await {
            Ok(service) => {
                if auxiliary_delete_params(&service.metadata, generation)?.is_none()
                    || !service_is_generation_scoped(&service, generation)
                {
                    return Ok(false);
                }
                for name in [
                    iron_proxy_sandbox_egress_policy_name(id),
                    iron_proxy_policy_name(id),
                ] {
                    let policy = match self.network_policies().get(&name).await {
                        Ok(policy) => policy,
                        Err(err) if is_not_found(&err) => return Ok(false),
                        Err(err) => {
                            return Err(map_kube_error("get iron-proxy network policy", err));
                        }
                    };
                    if auxiliary_delete_params(&policy.metadata, generation)?.is_none()
                        || !network_policy_is_generation_scoped(&policy, generation)
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(map_kube_error("get iron-proxy service", err)),
        }
    }

    async fn patch_iron_control_principal_annotation(
        &self,
        id: &SandboxId,
        generation: &str,
        principal_id: &str,
    ) -> SandboxResult<()> {
        let sandbox =
            self.sandboxes().get(id.as_str()).await.map_err(|err| {
                map_kube_error("get sandbox for iron-control principal patch", err)
            })?;
        if auxiliary_generation_from_sandbox(&sandbox)?.as_str() != generation {
            return Ok(());
        }
        let uid = sandbox.metadata.uid.as_deref().ok_or_else(|| {
            SandboxError::backend(
                "sandbox is missing UID required for iron-control principal patch",
            )
        })?;
        let resource_version = sandbox
            .metadata
            .resource_version
            .as_deref()
            .ok_or_else(|| {
                SandboxError::backend(
                    "sandbox is missing resourceVersion required for iron-control principal patch",
                )
            })?;
        let patch = Patch::Merge(json!({
            "metadata": {
                "uid": uid,
                "resourceVersion": resource_version,
                "annotations": {
                    crate::IRON_CONTROL_PRINCIPAL_ANNOTATION: principal_id,
                },
            },
        }));
        self.sandboxes()
            .patch(id.as_str(), &PatchParams::default(), &patch)
            .await
            .map(|_| ())
            .map_err(|err| map_kube_error("patch sandbox iron-control principal", err))
    }

    async fn patch_proxy_workflow_task_id_annotation(
        &self,
        id: &SandboxId,
        generation: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        let workflow_task_id = workflow_task_id_from_proxy_labels(labels);
        for attempt in 0..WORKFLOW_TASK_ID_ANNOTATION_PATCH_ATTEMPTS {
            let params = ListParams::default().labels(&format!(
                "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
                id.as_str(),
            ));
            let pods = self.pods().list(&params).await.map_err(|err| {
                map_kube_error("list iron-proxy pods for workflow task patch", err)
            })?;
            let mut found_current_generation = false;
            let mut retry = false;

            for pod in pods.items {
                let Some(name) = pod.metadata.name.as_deref() else {
                    continue;
                };
                if auxiliary_delete_params(&pod.metadata, generation)?.is_none() {
                    continue;
                }
                found_current_generation = true;
                let Some(patch) = workflow_task_id_annotation_patch(
                    &pod.metadata,
                    generation,
                    workflow_task_id.as_deref(),
                )?
                else {
                    continue;
                };
                match self
                    .pods()
                    .patch(name, &PatchParams::default(), &patch)
                    .await
                {
                    Ok(_) => {}
                    Err(err)
                        if (is_conflict(&err) || is_not_found(&err))
                            && attempt + 1 < WORKFLOW_TASK_ID_ANNOTATION_PATCH_ATTEMPTS =>
                    {
                        retry = true;
                        break;
                    }
                    Err(err) => {
                        return Err(map_kube_error(
                            "patch iron-proxy workflow task annotation",
                            err,
                        ));
                    }
                }
            }
            if retry {
                continue;
            }
            if !found_current_generation {
                return Err(SandboxError::NotReady(format!(
                    "iron-proxy pod for sandbox {} generation {} disappeared before workflow task annotation could be reconciled",
                    id.as_str(),
                    generation,
                )));
            }
            return Ok(());
        }
        unreachable!("workflow task annotation patch returns or retries within the bounded loop")
    }

    fn services(&self) -> Api<Service> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn network_policies(&self) -> Api<NetworkPolicy> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    async fn delete_iron_proxy_pods_for_sandbox(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<()> {
        let params = ListParams::default().labels(&format!(
            "{IRON_PROXY_LABEL}=true,{SANDBOX_ID_LABEL}={},{AUXILIARY_GENERATION_LABEL}={generation}",
            id.as_str(),
        ));
        let pods = self
            .pods()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list iron-proxy pods", err))?;
        for pod in pods.items {
            if let Some(name) = pod.metadata.name.as_ref() {
                let Some(params) = auxiliary_delete_params(&pod.metadata, generation)? else {
                    continue;
                };
                match self.pods().delete(name, &params).await {
                    Ok(_) => {}
                    Err(err) if is_not_found(&err) => {}
                    Err(err) => return Err(map_kube_error("delete iron-proxy pod", err)),
                }
            }
        }
        Ok(())
    }

    async fn delete_iron_proxy_service(
        &self,
        id: &SandboxId,
        generation: &str,
    ) -> SandboxResult<()> {
        let api = self.services();
        let name = iron_proxy_service_name(id);
        let service = match api.get(&name).await {
            Ok(service) => service,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => return Err(map_kube_error("get iron-proxy service for delete", err)),
        };
        let Some(params) = auxiliary_delete_params(&service.metadata, generation)? else {
            return Ok(());
        };
        match api.delete(&name, &params).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error("delete iron-proxy service", err)),
        }
    }

    async fn delete_iron_proxy_network_policy(
        &self,
        name: &str,
        generation: &str,
    ) -> SandboxResult<()> {
        let api = self.network_policies();
        let policy = match api.get(name).await {
            Ok(policy) => policy,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => {
                return Err(map_kube_error(
                    "get iron-proxy network policy for delete",
                    err,
                ));
            }
        };
        let Some(params) = auxiliary_delete_params(&policy.metadata, generation)? else {
            return Ok(());
        };
        match api.delete(name, &params).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error("delete iron-proxy network policy", err)),
        }
    }

    async fn wait_until_proxy_running(&self, resolved: &ResolvedIronProxy) -> SandboxResult<()> {
        let deadline = Instant::now() + self.config.ready_timeout;
        loop {
            match self.pods().get(&resolved.proxy_pod_name).await {
                Ok(pod) if pod_running(&pod) => return Ok(()),
                Ok(pod) if pod_stopped(&pod) => {
                    return Err(SandboxError::NotReady(format!(
                        "iron-proxy pod {} reached terminal state before running",
                        resolved.proxy_pod_name
                    )));
                }
                Ok(pod) if Instant::now() >= deadline => {
                    return Err(SandboxError::NotReady(format!(
                        "iron-proxy pod {} did not become running before timeout; latest phase: {:?}",
                        resolved.proxy_pod_name,
                        pod.status.and_then(|status| status.phase)
                    )));
                }
                Ok(_) => sleep(Duration::from_millis(500)).await,
                Err(err) if is_not_found(&err) && Instant::now() < deadline => {
                    sleep(Duration::from_millis(500)).await;
                }
                Err(err) if is_not_found(&err) => {
                    return Err(SandboxError::NotReady(format!(
                        "iron-proxy pod {} was not created before timeout",
                        resolved.proxy_pod_name
                    )));
                }
                Err(err) => return Err(map_kube_error("wait iron-proxy pod", err)),
            }
        }
    }
}

/// Address + bearer key of a managed proxy's management API.
struct ProxyManagementEndpoint {
    base_url: String,
    api_key: String,
}

/// Applied control-plane state served by the proxy's `GET /v1/status`.
#[derive(serde::Deserialize)]
struct ProxyManagedStatus {
    #[serde(default)]
    config_hash: Option<String>,
    #[serde(default)]
    principal_id: String,
    #[serde(default)]
    synced_once: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum ProxyAck {
    /// The proxy reports the expected principal's config as applied.
    Applied,
    /// The management API never answered within the probe window (proxy image
    /// without managed-mode management support, or listener disabled).
    ManagementUnavailable,
    /// The management API answered but the expected principal's config was
    /// not applied before the deadline.
    TimedOut,
}

fn proxy_fallback_delay_remaining(elapsed: Duration) -> Duration {
    PROXY_REASSIGN_FALLBACK_DELAY.saturating_sub(elapsed)
}

/// Only runtime-owned labels opt into strict acknowledgement. Ordinary proxy
/// reassignment preserves its compatibility fallback; an Autorotate execution
/// must prove the exact config hash before the harness can receive stdin.
fn requires_strict_autorotate_proxy_ack(labels: &BTreeMap<String, String>) -> bool {
    labels
        .get("centaur.autorotate_pin_id")
        .is_some_and(|value| !value.trim().is_empty())
        && labels
            .get("centaur.execution_id")
            .is_some_and(|value| !value.trim().is_empty())
        || labels
            .get("centaur.autorotate_clear")
            .is_some_and(|value| value == "true")
}

/// Poll the proxy's management API until it reports `principal_id`'s config
/// applied. `probe_window` bounds how long an entirely-unresponsive
/// management API is probed before concluding the image predates managed
/// status support; any successful response within the window commits to
/// waiting out the full `ack_timeout`.
async fn wait_for_proxy_ack(
    client: &reqwest::Client,
    endpoint: &ProxyManagementEndpoint,
    principal_id: &str,
    config_hash: Option<&str>,
    strict_config_hash: bool,
    ack_timeout: Duration,
    probe_window: Duration,
    poll_interval: Duration,
) -> ProxyAck {
    let started = Instant::now();
    let mut poked = false;
    let mut management_confirmed = false;
    loop {
        // Poke an immediate out-of-band sync so the barrier does not ride the
        // proxy's 5s poll cadence; retried until it lands (the status poll
        // below still converges without it, just slower).
        if !poked {
            poked = matches!(
                client
                    .post(format!("{}/v1/sync", endpoint.base_url))
                    .bearer_auth(&endpoint.api_key)
                    .send()
                    .await,
                Ok(response) if response.status().is_success()
            );
        }
        let status = client
            .get(format!("{}/v1/status", endpoint.base_url))
            .bearer_auth(&endpoint.api_key)
            .send()
            .await;
        if let Ok(response) = status
            && response.status().is_success()
        {
            management_confirmed = true;
            if let Ok(status) = response.json::<ProxyManagedStatus>().await
                && status.synced_once
                && status.principal_id == principal_id
                && if strict_config_hash {
                    config_hash.is_some() && status.config_hash.as_deref() == config_hash
                } else {
                    status.config_hash.as_deref().is_none_or(|applied_hash| {
                        config_hash.is_none_or(|hash| applied_hash == hash)
                    })
                }
            {
                return ProxyAck::Applied;
            }
        }
        let elapsed = started.elapsed();
        if !management_confirmed && elapsed >= probe_window {
            return ProxyAck::ManagementUnavailable;
        }
        if elapsed >= ack_timeout {
            return ProxyAck::TimedOut;
        }
        sleep(poll_interval).await;
    }
}

/// The management endpoint advertised by a running proxy pod: pod IP plus the
/// IRON_MANAGEMENT_LISTEN port and IRON_MANAGEMENT_API_KEY from the pod's
/// env (so env overrides are respected). `None` for pods that are not
/// running or predate the management env wiring.
fn proxy_management_endpoint_from_pod(pod: &Pod) -> Option<ProxyManagementEndpoint> {
    if !pod_running(pod) {
        return None;
    }
    let pod_ip = pod.status.as_ref()?.pod_ip.as_deref()?;
    let env = pod
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|container| container.name == "iron-proxy")?
        .env
        .as_ref()?;
    let env_value = |name: &str| {
        env.iter()
            .find(|env| env.name == name)
            .and_then(|env| env.value.as_deref())
    };
    let api_key = env_value("IRON_MANAGEMENT_API_KEY")?.to_owned();
    let port = env_value("IRON_MANAGEMENT_LISTEN")
        .and_then(listen_port)
        .unwrap_or(PROXY_MANAGEMENT_PORT);
    let host = if pod_ip.contains(':') {
        format!("[{pod_ip}]")
    } else {
        pod_ip.to_owned()
    };
    Some(ProxyManagementEndpoint {
        base_url: format!("http://{host}:{port}"),
        api_key,
    })
}

/// Port of a `[host]:port` listen address (`":9092"`, `"0.0.0.0:9092"`).
fn listen_port(listen: &str) -> Option<u16> {
    listen.rsplit_once(':')?.1.parse().ok()
}

fn new_proxy_management_api_key() -> String {
    format!("mgmt-{}", uuid::Uuid::new_v4().simple())
}

pub(crate) fn apply_proxy_env(spec: &mut SandboxSpec, resolved: &ResolvedIronProxy) {
    let mut no_proxy_extra = current_env_values(spec, ["NO_PROXY", "no_proxy"]);
    // The harness exports OTLP traces (usage/cost spans) straight to the
    // collector; routing them through iron-proxy fails (plain-HTTP forwards
    // are rejected), so the endpoint host always bypasses the proxy.
    no_proxy_extra.extend(otlp_endpoint_hosts(spec));
    for (name, value) in proxy_env(&resolved.proxy_host, resolved.proxy_port, &no_proxy_extra) {
        set_env(spec, &name, &value);
    }
    // Operator-granted replace placeholders: the sandbox sends the proxy_value
    // and iron-proxy swaps in the real secret. set_missing so infra placeholders
    // (already on the spec from the known set) win.
    for (name, value) in &resolved.replace_placeholders {
        set_missing_env(spec, name, value);
    }
    // The sandbox always gets one local Postgres base DSN. Tools choose the
    // database name they connect to; iron-proxy routes that database to the
    // assigned principal's effective pg_dsn secret.
    if let Some(pg) = &resolved.pg {
        let value = format!(
            "postgresql://{}:{}@{}:{}",
            pg.user, pg.password, resolved.proxy_host, pg.port,
        );
        set_missing_env(spec, CENTAUR_POSTGRES_DSN_ENV, &value);
    }
    if !resolved.console_url.is_empty() {
        set_missing_env(spec, CENTAUR_CONSOLE_URL_ENV, &resolved.console_url);
    }
}

pub(crate) fn sandbox_ca_volume_mount_json() -> Value {
    json!({
        "name": "firewall-ca",
        "mountPath": FIREWALL_CA_MOUNT_PATH,
        "readOnly": true,
    })
}

pub(crate) fn sandbox_ca_volume_json(iron_proxy: &IronProxyConfig) -> Value {
    json!({
        "name": "firewall-ca",
        "secret": {"secretName": iron_proxy.ca_cert_secret_name},
    })
}

#[cfg(test)]
fn build_iron_proxy_pod(
    id: &SandboxId,
    iron_proxy: &IronProxyConfig,
    resolved: &ResolvedIronProxy,
    sync: &ProxySyncEnv,
) -> Pod {
    build_iron_proxy_pod_with_generation(id, iron_proxy, resolved, sync, "test-generation")
}

fn build_iron_proxy_pod_with_generation(
    id: &SandboxId,
    iron_proxy: &IronProxyConfig,
    resolved: &ResolvedIronProxy,
    sync: &ProxySyncEnv,
    generation: &str,
) -> Pod {
    let mut annotations = BTreeMap::from([
        (
            IRON_CONTROL_PROXY_ID_ANNOTATION.to_owned(),
            sync.proxy_id.clone(),
        ),
        (
            crate::IRON_CONTROL_PRINCIPAL_ANNOTATION.to_owned(),
            resolved.principal_id.clone(),
        ),
        (
            AUXILIARY_GENERATION_ANNOTATION.to_owned(),
            generation.to_owned(),
        ),
    ]);
    if let Some(workflow_task_id) = workflow_task_id_from_proxy_labels(&resolved.labels) {
        annotations.insert(WORKFLOW_TASK_ID_ANNOTATION.to_owned(), workflow_task_id);
    }
    Pod {
        metadata: object_meta_with_annotations(
            resolved.proxy_pod_name.clone(),
            iron_proxy_labels(
                id,
                resolved.observability_enabled,
                resolved.api_server_enabled,
                generation,
            ),
            annotations,
        ),
        spec: Some(PodSpec {
            automount_service_account_token: Some(false),
            restart_policy: Some("Never".to_owned()),
            containers: vec![iron_proxy_container(iron_proxy, resolved, sync)],
            volumes: Some(iron_proxy_volumes(iron_proxy)),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn workflow_task_id_from_proxy_labels(labels: &BTreeMap<String, String>) -> Option<String> {
    labels
        .get(WORKFLOW_TASK_ID_LABEL)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| Uuid::parse_str(value).ok())
        .map(|task_id| task_id.to_string())
}

fn workflow_task_id_annotation_patch(
    metadata: &ObjectMeta,
    generation: &str,
    workflow_task_id: Option<&str>,
) -> SandboxResult<Option<Patch<Value>>> {
    let Some(params) = auxiliary_delete_params(metadata, generation)? else {
        return Ok(None);
    };
    let existing = metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(WORKFLOW_TASK_ID_ANNOTATION))
        .map(String::as_str);
    if existing == workflow_task_id {
        return Ok(None);
    }
    let preconditions = params
        .preconditions
        .expect("generation-checked proxy pod always carries preconditions");
    let uid = preconditions
        .uid
        .expect("generation-checked proxy pod always carries a UID");
    let resource_version = preconditions
        .resource_version
        .expect("generation-checked proxy pod always carries a resourceVersion");
    Ok(Some(Patch::Merge(json!({
        "metadata": {
            "uid": uid,
            "resourceVersion": resource_version,
            "annotations": {
                WORKFLOW_TASK_ID_ANNOTATION: workflow_task_id,
            },
        },
    }))))
}

fn iron_proxy_container(
    iron_proxy: &IronProxyConfig,
    resolved: &ResolvedIronProxy,
    sync: &ProxySyncEnv,
) -> Container {
    Container {
        name: "iron-proxy".to_owned(),
        image: Some(iron_proxy.image.clone()),
        image_pull_policy: iron_proxy.image_pull_policy.clone(),
        env: Some(iron_proxy_env_vars(iron_proxy, resolved, sync)),
        env_from: iron_proxy_env_from(iron_proxy),
        ports: Some(container_ports(resolved)),
        readiness_probe: Some(health_probe(Some(5), Some(30))),
        liveness_probe: Some(health_probe(None, None)),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_owned()]),
                ..Default::default()
            }),
            seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                type_: "RuntimeDefault".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        volume_mounts: Some(vec![
            // Writable config dir for the entrypoint's CA copy; no proxy.yaml
            // is rendered in managed mode.
            volume_mount("iron-proxy-config", "/etc/iron-proxy", false),
            volume_mount("iron-proxy-certs", "/certs", false),
            volume_mount("iron-proxy-ca", "/etc/iron-proxy-ca", true),
        ]),
        // Use the image entrypoint directly: it loads the CA and, with
        // IRON_CONTROL_PLANE_URL set, runs iron-proxy with no local config.
        ..Default::default()
    }
}

fn iron_proxy_env_vars(
    iron_proxy: &IronProxyConfig,
    resolved: &ResolvedIronProxy,
    sync: &ProxySyncEnv,
) -> Vec<K8sEnvVar> {
    let mut env = BTreeMap::new();
    env.insert(
        "IRON_MANAGEMENT_API_KEY".to_owned(),
        env_var("IRON_MANAGEMENT_API_KEY", &resolved.management_api_key),
    );
    // Start the managed-mode management API (/v1/status, /v1/sync) so the
    // claim-time principal barrier can verify the applied config. Older proxy
    // images ignore this env and simply never listen; the barrier falls back
    // to a fixed delay.
    env.insert(
        "IRON_MANAGEMENT_LISTEN".to_owned(),
        env_var(
            "IRON_MANAGEMENT_LISTEN",
            &format!(":{PROXY_MANAGEMENT_PORT}"),
        ),
    );
    // iron-proxy pulls its effective config (allowlist, secrets, management)
    // from iron-control using this token; no local config file is rendered.
    // The binary reads the control-plane base URL from IRON_CONTROL_PLANE_URL
    // (distinct from api-rs's own IRON_CONTROL_URL admin-client var); a wrong
    // name makes it fall back to its built-in default endpoint.
    env.insert(
        "IRON_CONTROL_PLANE_URL".to_owned(),
        env_var("IRON_CONTROL_PLANE_URL", &sync.control_url),
    );
    env.insert(
        "IRON_PROXY_TOKEN".to_owned(),
        env_var("IRON_PROXY_TOKEN", &sync.token),
    );
    // The local listen/TLS settings the control plane does not own, passed as
    // env instead of a config file. CA paths match the entrypoint's CA copy.
    for (name, value) in [
        ("IRON_PROXY_TUNNEL_LISTEN", format!(":{PROXY_TUNNEL_PORT}")),
        (
            "IRON_PROXY_UPSTREAM_RESPONSE_HEADER_TIMEOUT",
            PROXY_UPSTREAM_RESPONSE_HEADER_TIMEOUT.to_owned(),
        ),
        ("IRON_DNS_LISTEN", PROXY_DNS_LISTEN.to_owned()),
        ("IRON_DNS_PROXY_IP", PROXY_DNS_PROXY_IP.to_owned()),
        ("IRON_TLS_MODE", PROXY_TLS_MODE.to_owned()),
        ("IRON_TLS_CA_CERT", PROXY_TLS_CA_CERT_PATH.to_owned()),
        ("IRON_TLS_CA_KEY", PROXY_TLS_CA_KEY_PATH.to_owned()),
        ("IRON_LOG_LEVEL", PROXY_LOG_LEVEL.to_owned()),
    ] {
        env.insert(name.to_owned(), env_var(name, &value));
    }
    if !iron_proxy.upstream_deny_cidrs.is_empty() {
        env.insert(
            PROXY_UPSTREAM_DENY_CIDRS_ENV.to_owned(),
            env_var(
                PROXY_UPSTREAM_DENY_CIDRS_ENV,
                &iron_proxy.upstream_deny_cidrs.join(","),
            ),
        );
    }
    for (name, value) in &iron_proxy.extra_env {
        env.insert(name.clone(), env_var(name, value));
    }
    for secret_env in &iron_proxy.secret_env {
        env.insert(secret_env.name.clone(), secret_env_var(secret_env));
    }
    // Single-listener Postgres local config. The control plane owns every
    // upstream DSN + role (the pg_dsn secrets) and multiplexes them through this
    // one listener; api-rs only supplies the bind address and the shared client
    // credential the sandbox presents.
    if let Some(pg) = &resolved.pg {
        for (name, value) in [
            (PG_LISTEN_ENV, pg.listen.as_str()),
            (PG_CLIENT_USER_ENV, pg.user.as_str()),
            (PG_CLIENT_PASSWORD_ENV, pg.password.as_str()),
        ] {
            env.insert(name.to_owned(), env_var(name, value));
        }
    }
    env.into_values().collect()
}

fn secret_env_var(secret_env: &IronProxySecretEnv) -> K8sEnvVar {
    K8sEnvVar {
        name: secret_env.name.clone(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                key: secret_env.secret_key.clone(),
                name: secret_env.secret_name.clone(),
                optional: Some(false),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn iron_proxy_env_from(iron_proxy: &IronProxyConfig) -> Option<Vec<EnvFromSource>> {
    (!iron_proxy.env_from_secret_names.is_empty()).then(|| {
        iron_proxy
            .env_from_secret_names
            .iter()
            .map(|name| EnvFromSource {
                secret_ref: Some(SecretEnvSource {
                    name: name.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect()
    })
}

fn iron_proxy_volumes(iron_proxy: &IronProxyConfig) -> Vec<Volume> {
    vec![
        empty_dir_volume("iron-proxy-config"),
        empty_dir_volume("iron-proxy-certs"),
        Volume {
            name: "iron-proxy-ca".to_owned(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(iron_proxy.ca_key_secret_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        },
    ]
}

#[cfg(test)]
fn build_iron_proxy_service(id: &SandboxId, resolved: &ResolvedIronProxy) -> Service {
    build_iron_proxy_service_with_generation(id, resolved, "test-generation")
}

fn build_iron_proxy_service_with_generation(
    id: &SandboxId,
    resolved: &ResolvedIronProxy,
    generation: &str,
) -> Service {
    let mut ports = vec![service_port("proxy", resolved.proxy_port)];
    if let Some(pg) = &resolved.pg {
        ports.push(service_port("pg", pg.port));
    }
    Service {
        metadata: object_meta_with_annotations(
            iron_proxy_service_name(id),
            iron_proxy_labels(
                id,
                resolved.observability_enabled,
                resolved.api_server_enabled,
                generation,
            ),
            BTreeMap::from([(
                AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                generation.to_owned(),
            )]),
        ),
        spec: Some(ServiceSpec {
            selector: Some(iron_proxy_labels(
                id,
                resolved.observability_enabled,
                resolved.api_server_enabled,
                generation,
            )),
            ports: Some(ports),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
fn build_iron_proxy_network_policies(
    id: &SandboxId,
    resolved: &ResolvedIronProxy,
    iron_proxy: &IronProxyConfig,
    control_target: &ControlPlaneEgressTarget,
    otlp_egress: Option<&OtlpEgressTarget>,
    observability_enabled: bool,
) -> Vec<NetworkPolicy> {
    build_iron_proxy_network_policies_with_generation(
        id,
        resolved,
        "test-generation",
        iron_proxy,
        control_target,
        otlp_egress,
        observability_enabled,
    )
}

fn build_iron_proxy_network_policies_with_generation(
    id: &SandboxId,
    resolved: &ResolvedIronProxy,
    generation: &str,
    iron_proxy: &IronProxyConfig,
    control_target: &ControlPlaneEgressTarget,
    otlp_egress: Option<&OtlpEgressTarget>,
    observability_enabled: bool,
) -> Vec<NetworkPolicy> {
    let sandbox_to_proxy_ports = sandbox_to_proxy_ports(resolved);
    let sandbox_egress = vec![
        egress_to(
            vec![pod_peer(iron_proxy_labels(
                id,
                observability_enabled,
                resolved.api_server_enabled,
                generation,
            ))],
            sandbox_to_proxy_ports.clone(),
        ),
        dns_egress_rule(),
    ];
    vec![
        NetworkPolicy {
            metadata: object_meta_with_annotations(
                iron_proxy_sandbox_egress_policy_name(id),
                sandbox_labels(id, generation),
                BTreeMap::from([(
                    AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                    generation.to_owned(),
                )]),
            ),
            spec: Some(NetworkPolicySpec {
                pod_selector: Some(label_selector(sandbox_labels(id, generation))),
                policy_types: Some(vec!["Egress".to_owned()]),
                egress: Some(sandbox_egress),
                ..Default::default()
            }),
        },
        NetworkPolicy {
            metadata: object_meta_with_annotations(
                iron_proxy_policy_name(id),
                iron_proxy_labels(
                    id,
                    observability_enabled,
                    resolved.api_server_enabled,
                    generation,
                ),
                BTreeMap::from([(
                    AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                    generation.to_owned(),
                )]),
            ),
            spec: Some(NetworkPolicySpec {
                pod_selector: Some(label_selector(iron_proxy_labels(
                    id,
                    observability_enabled,
                    resolved.api_server_enabled,
                    generation,
                ))),
                policy_types: Some(vec!["Ingress".to_owned(), "Egress".to_owned()]),
                ingress: Some(vec![
                    NetworkPolicyIngressRule {
                        from: Some(vec![pod_peer(sandbox_labels(id, generation))]),
                        ports: Some(sandbox_to_proxy_ports),
                    },
                    // api-rs -> proxy management API, for the claim-time
                    // principal barrier (POST /v1/sync + GET /v1/status).
                    NetworkPolicyIngressRule {
                        from: Some(vec![pod_peer(iron_proxy.api_pod_labels.clone())]),
                        ports: Some(vec![network_port(PROXY_MANAGEMENT_PORT)]),
                    },
                ]),
                egress: Some(proxy_egress_rules(
                    iron_proxy,
                    control_target,
                    otlp_egress,
                    observability_enabled,
                )),
            }),
        },
    ]
}

fn sandbox_to_proxy_ports(resolved: &ResolvedIronProxy) -> Vec<NetworkPolicyPort> {
    std::iter::once(network_port(resolved.proxy_port))
        .chain(resolved.pg.as_ref().map(|pg| network_port(pg.port)))
        .collect()
}

fn proxy_egress_rules(
    iron_proxy: &IronProxyConfig,
    control_target: &ControlPlaneEgressTarget,
    otlp_egress: Option<&OtlpEgressTarget>,
    observability_enabled: bool,
) -> Vec<NetworkPolicyEgressRule> {
    // Upstream egress: 443/5432 for normal traffic, plus the iron-control port
    // (deduped) so a sync-mode proxy can reach the control plane. Public
    // upstreams are always constrained away from private/cluster CIDRs; any
    // intra-cluster destination must be added as an explicit rule below.
    let upstream_ports = vec![network_port(443), network_port(5432)];
    let mut rules = vec![dns_egress_rule()];
    rules.push(egress_to(
        vec![control_target.peer.clone()],
        vec![network_port(control_target.port)],
    ));
    rules.push(egress_to(
        vec![all_namespaces_peer()],
        vec![network_port(PG_LISTENER_PORT)],
    ));
    rules.push(egress_to(vec![public_ipv4_peer()], upstream_ports));
    if observability_enabled {
        rules.push(egress_to(
            vec![pod_peer(iron_proxy.api_pod_labels.clone())],
            vec![network_port(8000), network_port(8080)],
        ));
        if let Some(target) = otlp_egress {
            rules.push(egress_to(
                vec![namespace_peer(&target.namespace)],
                vec![network_port(target.port)],
            ));
        }
    }
    if matches!(
        iron_proxy.source_policy.kind,
        SourceKind::OnePasswordConnect
    ) {
        rules.push(egress_to(
            vec![pod_peer(BTreeMap::from([(
                "app".to_owned(),
                iron_proxy.op_connect_app_name.clone(),
            )]))],
            vec![network_port(iron_proxy.op_connect_port)],
        ));
    }
    rules
}

fn dns_egress_rule() -> NetworkPolicyEgressRule {
    egress_to(
        vec![namespace_peer("kube-system")],
        vec![udp_port(53), network_port(53)],
    )
}

fn namespace_peer(namespace: &str) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: Some(label_selector(BTreeMap::from([(
            "kubernetes.io/metadata.name".to_owned(),
            namespace.to_owned(),
        )]))),
        ..Default::default()
    }
}

fn namespace_pod_peer(namespace: &str, labels: BTreeMap<String, String>) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: Some(label_selector(BTreeMap::from([(
            "kubernetes.io/metadata.name".to_owned(),
            namespace.to_owned(),
        )]))),
        pod_selector: Some(label_selector(labels)),
        ..Default::default()
    }
}

fn all_namespaces_peer() -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: Some(LabelSelector::default()),
        ..Default::default()
    }
}

fn public_ipv4_peer() -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        ip_block: Some(IPBlock {
            cidr: "0.0.0.0/0".to_owned(),
            except: Some(vec![
                "0.0.0.0/8".to_owned(),
                "10.0.0.0/8".to_owned(),
                "100.64.0.0/10".to_owned(),
                "127.0.0.0/8".to_owned(),
                "169.254.0.0/16".to_owned(),
                "172.16.0.0/12".to_owned(),
                "192.0.0.0/24".to_owned(),
                "192.0.2.0/24".to_owned(),
                "192.168.0.0/16".to_owned(),
                "198.18.0.0/15".to_owned(),
                "198.51.100.0/24".to_owned(),
                "203.0.113.0/24".to_owned(),
                "224.0.0.0/4".to_owned(),
                "240.0.0.0/4".to_owned(),
            ]),
        }),
        ..Default::default()
    }
}

fn control_plane_egress_target(
    control_url: &str,
    default_namespace: &str,
    control_plane_pod_labels: BTreeMap<String, String>,
) -> ControlPlaneEgressTarget {
    ControlPlaneEgressTarget {
        peer: namespace_pod_peer(default_namespace, control_plane_pod_labels),
        port: url_port(control_url).unwrap_or(443),
    }
}

fn proxy_env(
    proxy_host: &str,
    proxy_port: u16,
    no_proxy_extra: &[String],
) -> BTreeMap<String, String> {
    let proxy_url = format!("http://{proxy_host}:{proxy_port}");
    let no_proxy = no_proxy_value(proxy_host, no_proxy_extra);
    BTreeMap::from([
        ("FIREWALL_HOST".to_owned(), proxy_host.to_owned()),
        ("FIREWALL_PROXY_PORT".to_owned(), proxy_port.to_string()),
        ("HTTP_PROXY".to_owned(), proxy_url.clone()),
        ("HTTPS_PROXY".to_owned(), proxy_url.clone()),
        ("http_proxy".to_owned(), proxy_url.clone()),
        ("https_proxy".to_owned(), proxy_url),
        ("NO_PROXY".to_owned(), no_proxy.clone()),
        ("no_proxy".to_owned(), no_proxy),
        (
            "NODE_EXTRA_CA_CERTS".to_owned(),
            FIREWALL_CA_CERT_PATH.to_owned(),
        ),
        (
            "REQUESTS_CA_BUNDLE".to_owned(),
            FIREWALL_CA_CERT_PATH.to_owned(),
        ),
        (
            "CURL_CA_BUNDLE".to_owned(),
            FIREWALL_CA_CERT_PATH.to_owned(),
        ),
        ("SSL_CERT_FILE".to_owned(), FIREWALL_CA_CERT_PATH.to_owned()),
        (
            "GIT_SSL_CAINFO".to_owned(),
            FIREWALL_CA_CERT_PATH.to_owned(),
        ),
    ])
}

fn no_proxy_value(proxy_host: &str, extra_values: &[String]) -> String {
    let mut hosts = BTreeSet::<String>::from([
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
        proxy_host.to_owned(),
        "victoriametrics".to_owned(),
        "victorialogs".to_owned(),
    ]);
    for value in extra_values {
        hosts.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|host| !host.is_empty())
                .map(ToOwned::to_owned),
        );
    }
    hosts.into_iter().collect::<Vec<_>>().join(",")
}

fn set_missing_env(spec: &mut SandboxSpec, name: &str, value: &str) {
    if env_value(spec, name).is_none() {
        set_env(spec, name, value);
    }
}

fn set_env(spec: &mut SandboxSpec, name: &str, value: &str) {
    if let Some(env) = spec.env.iter_mut().find(|env| env.name == name) {
        env.value = value.to_owned();
    } else {
        spec.env
            .push(centaur_sandbox_core::EnvVar::new(name, value));
    }
}

fn env_value(spec: &SandboxSpec, name: &str) -> Option<String> {
    spec.env
        .iter()
        .find(|env| env.name == name)
        .map(|env| env.value.clone())
}

fn pg_from_sandbox_env(
    sandbox: &crate::crd::Sandbox,
    container_name: &str,
    listen: &str,
    port: u16,
) -> Option<ResolvedPg> {
    let container = sandbox
        .spec
        .pod_template
        .spec
        .containers
        .iter()
        .find(|container| container.name == container_name)
        .or_else(|| sandbox.spec.pod_template.spec.containers.first())?;
    let dsn = container
        .env
        .as_ref()?
        .iter()
        .find(|env| env.name == CENTAUR_POSTGRES_DSN_ENV)
        .and_then(|env| env.value.as_deref())?;
    pg_from_sandbox_dsn(dsn, listen, port)
}

fn sandbox_observability_enabled(
    sandbox: &crate::crd::Sandbox,
    container_name: &str,
) -> Option<bool> {
    sandbox_env_value(
        sandbox,
        "CENTAUR_SANDBOX_OBSERVABILITY_ENABLED",
        container_name,
    )
    .and_then(|value| value.parse().ok())
}

fn sandbox_api_server_enabled(sandbox: &crate::crd::Sandbox, container_name: &str) -> Option<bool> {
    sandbox_env_value(
        sandbox,
        "CENTAUR_SANDBOX_API_SERVER_ENABLED",
        container_name,
    )
    .and_then(|value| value.parse().ok())
}

fn sandbox_env_value(
    sandbox: &crate::crd::Sandbox,
    name: &str,
    fallback_container_name: &str,
) -> Option<String> {
    sandbox
        .spec
        .pod_template
        .spec
        .containers
        .iter()
        .find(|container| container.name == fallback_container_name)
        .or_else(|| sandbox.spec.pod_template.spec.containers.first())?
        .env
        .as_ref()?
        .iter()
        .find(|env| env.name == name)
        .and_then(|env| env.value.clone())
}

fn pg_from_sandbox_dsn(dsn: &str, listen: &str, port: u16) -> Option<ResolvedPg> {
    let rest = dsn
        .strip_prefix("postgresql://")
        .or_else(|| dsn.strip_prefix("postgres://"))?;
    let auth = rest.split_once('@')?.0;
    let (user, password) = auth.split_once(':')?;
    if user.is_empty() || password.is_empty() {
        return None;
    }
    Some(ResolvedPg {
        listen: listen.to_owned(),
        port,
        user: user.to_owned(),
        password: password.to_owned(),
    })
}

fn current_env_values<const N: usize>(spec: &SandboxSpec, names: [&str; N]) -> Vec<String> {
    names
        .into_iter()
        .filter_map(|name| env_value(spec, name))
        .collect()
}

/// Hosts of the spec's OTLP exporter endpoints, mirrored into NO_PROXY (same
/// contract as the Python control plane's `_sandbox_otel_endpoint_hosts`).
fn otlp_endpoint_hosts(spec: &SandboxSpec) -> Vec<String> {
    [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    .into_iter()
    .filter_map(|name| env_value(spec, name))
    .filter_map(host_from_url)
    .collect()
}

/// The authority (`[user@]host[:port]`) of a URL or bare `host:port`, with any
/// scheme and path stripped and surrounding whitespace trimmed.
fn authority(value: &str) -> Option<&str> {
    let without_scheme = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(value);
    let authority = without_scheme.split('/').next()?.trim();
    (!authority.is_empty()).then_some(authority)
}

fn host_from_url(value: String) -> Option<String> {
    let authority = authority(&value)?;
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, host_port)| host_port)
        .unwrap_or(authority);
    let host = host_port
        .split_once(':')
        .map_or(host_port, |(host, _)| host);
    (!host.is_empty()).then(|| host.to_owned())
}

fn url_port(value: &str) -> Option<u16> {
    authority(value)?.rsplit_once(':')?.1.parse().ok()
}

fn pod_running(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .is_some_and(|phase| phase.eq_ignore_ascii_case("running"))
        && pod
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            })
}

fn pod_stopped(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .is_some_and(|phase| {
            phase.eq_ignore_ascii_case("succeeded") || phase.eq_ignore_ascii_case("failed")
        })
}

fn sandbox_owner_reference(sandbox: &crate::crd::Sandbox) -> Option<Value> {
    let name = sandbox.metadata.name.as_ref()?;
    let uid = sandbox.metadata.uid.as_ref()?;
    Some(json!({
        "apiVersion": crate::crd::Sandbox::api_version(&()),
        "kind": crate::crd::Sandbox::kind(&()),
        "name": name,
        "uid": uid,
    }))
}

fn legacy_auxiliary_is_adoptable(metadata: &ObjectMeta, sandbox: &crate::crd::Sandbox) -> bool {
    object_is_owned_by_sandbox(metadata, sandbox)
        && metadata
            .annotations
            .as_ref()
            .is_none_or(|annotations| !annotations.contains_key(AUXILIARY_GENERATION_ANNOTATION))
}

fn owner_reference_patch(
    metadata: &ObjectMeta,
    owner_reference: &Value,
    generation: Option<&str>,
) -> SandboxResult<Patch<Value>> {
    let uid = metadata.uid.as_deref().ok_or_else(|| {
        SandboxError::backend("auxiliary resource is missing UID required for adoption")
    })?;
    let resource_version = metadata.resource_version.as_deref().ok_or_else(|| {
        SandboxError::backend("auxiliary resource is missing resourceVersion required for adoption")
    })?;
    Ok(owner_reference_patch_from_identity(
        uid,
        resource_version,
        owner_reference,
        generation,
    ))
}

fn owner_reference_patch_from_delete_params(
    params: kube::api::DeleteParams,
    owner_reference: &Value,
    generation: Option<&str>,
) -> Patch<Value> {
    let preconditions = params
        .preconditions
        .expect("generation-checked auxiliary delete always carries preconditions");
    owner_reference_patch_from_identity(
        preconditions
            .uid
            .as_deref()
            .expect("generation-checked auxiliary delete always carries UID"),
        preconditions
            .resource_version
            .as_deref()
            .expect("generation-checked auxiliary delete always carries resourceVersion"),
        owner_reference,
        generation,
    )
}

fn owner_reference_patch_from_identity(
    uid: &str,
    resource_version: &str,
    owner_reference: &Value,
    generation: Option<&str>,
) -> Patch<Value> {
    let mut metadata = json!({
        "uid": uid,
        "resourceVersion": resource_version,
        "ownerReferences": [owner_reference],
    });
    if let Some(generation) = generation {
        metadata["annotations"] = json!({ AUXILIARY_GENERATION_ANNOTATION: generation });
        metadata["labels"] = json!({ AUXILIARY_GENERATION_LABEL: generation });
    }
    Patch::Merge(json!({ "metadata": metadata }))
}

fn service_generation_patch(
    service: &Service,
    owner_reference: &Value,
    generation: &str,
) -> SandboxResult<Patch<Value>> {
    let mut patch = patch_value(owner_reference_patch(
        &service.metadata,
        owner_reference,
        Some(generation),
    )?);
    if let Some(spec) = &service.spec {
        let mut spec = serde_json::to_value(spec).map_err(|error| {
            SandboxError::backend_source("serialize legacy iron-proxy service spec", error)
        })?;
        spec["selector"][AUXILIARY_GENERATION_LABEL] = json!(generation);
        patch["spec"] = spec;
    }
    Ok(Patch::Merge(patch))
}

fn network_policy_generation_patch(
    policy: &NetworkPolicy,
    owner_reference: &Value,
    generation: &str,
) -> SandboxResult<Patch<Value>> {
    let mut patch = patch_value(owner_reference_patch(
        &policy.metadata,
        owner_reference,
        Some(generation),
    )?);
    if let Some(spec) = &policy.spec {
        let mut spec = serde_json::to_value(spec).map_err(|error| {
            SandboxError::backend_source("serialize legacy iron-proxy network policy spec", error)
        })?;
        scope_network_policy_selectors(&mut spec, generation);
        patch["spec"] = spec;
    }
    Ok(Patch::Merge(patch))
}

fn patch_value(patch: Patch<Value>) -> Value {
    match patch {
        Patch::Merge(value) => value,
        _ => unreachable!("auxiliary adoption only uses merge patches"),
    }
}

fn scope_network_policy_selectors(value: &mut Value, generation: &str) {
    match value {
        Value::Object(object) => {
            if let Some(Value::Object(selector)) = object.get_mut("podSelector")
                && let Some(Value::Object(labels)) = selector.get_mut("matchLabels")
                && (labels.contains_key(SANDBOX_ID_LABEL) || labels.contains_key(IRON_PROXY_LABEL))
            {
                labels.insert(AUXILIARY_GENERATION_LABEL.to_owned(), json!(generation));
            }
            for value in object.values_mut() {
                scope_network_policy_selectors(value, generation);
            }
        }
        Value::Array(values) => {
            for value in values {
                scope_network_policy_selectors(value, generation);
            }
        }
        _ => {}
    }
}

fn service_is_generation_scoped(service: &Service, generation: &str) -> bool {
    service
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.as_ref())
        .and_then(|selector| selector.get(AUXILIARY_GENERATION_LABEL))
        .map(String::as_str)
        == Some(generation)
}

fn network_policy_is_generation_scoped(policy: &NetworkPolicy, generation: &str) -> bool {
    let Some(spec) = policy.spec.as_ref() else {
        return false;
    };
    let Ok(value) = serde_json::to_value(spec) else {
        return false;
    };
    network_policy_selectors_are_generation_scoped(&value, generation)
}

fn network_policy_selectors_are_generation_scoped(value: &Value, generation: &str) -> bool {
    match value {
        Value::Object(object) => {
            if let Some(Value::Object(selector)) = object.get("podSelector")
                && let Some(Value::Object(labels)) = selector.get("matchLabels")
                && (labels.contains_key(SANDBOX_ID_LABEL) || labels.contains_key(IRON_PROXY_LABEL))
                && labels
                    .get(AUXILIARY_GENERATION_LABEL)
                    .and_then(Value::as_str)
                    != Some(generation)
            {
                return false;
            }
            object
                .values()
                .all(|value| network_policy_selectors_are_generation_scoped(value, generation))
        }
        Value::Array(values) => values
            .iter()
            .all(|value| network_policy_selectors_are_generation_scoped(value, generation)),
        _ => true,
    }
}

fn object_meta_with_annotations(
    name: impl Into<String>,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        labels: Some(labels),
        annotations: (!annotations.is_empty()).then_some(annotations),
        ..Default::default()
    }
}

fn env_var(name: &str, value: &str) -> K8sEnvVar {
    K8sEnvVar {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        ..Default::default()
    }
}

fn container_port(name: impl Into<String>, port: u16) -> ContainerPort {
    ContainerPort {
        name: Some(name.into()),
        container_port: i32::from(port),
        ..Default::default()
    }
}

fn service_port(name: impl Into<String>, port: u16) -> ServicePort {
    let port = i32::from(port);
    ServicePort {
        name: Some(name.into()),
        port,
        target_port: Some(IntOrString::Int(port)),
        protocol: Some("TCP".to_owned()),
        ..Default::default()
    }
}

fn network_port(port: u16) -> NetworkPolicyPort {
    policy_port("TCP", port)
}

fn udp_port(port: u16) -> NetworkPolicyPort {
    policy_port("UDP", port)
}

fn policy_port(protocol: &str, port: u16) -> NetworkPolicyPort {
    NetworkPolicyPort {
        port: Some(IntOrString::Int(i32::from(port))),
        protocol: Some(protocol.to_owned()),
        ..Default::default()
    }
}

fn label_selector(match_labels: BTreeMap<String, String>) -> LabelSelector {
    LabelSelector {
        match_labels: Some(match_labels),
        ..Default::default()
    }
}

fn pod_peer(match_labels: BTreeMap<String, String>) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        pod_selector: Some(label_selector(match_labels)),
        ..Default::default()
    }
}

fn egress_to(to: Vec<NetworkPolicyPeer>, ports: Vec<NetworkPolicyPort>) -> NetworkPolicyEgressRule {
    NetworkPolicyEgressRule {
        to: Some(to),
        ports: Some(ports),
    }
}

fn health_probe(period_seconds: Option<i32>, failure_threshold: Option<i32>) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/healthz".to_owned()),
            port: IntOrString::Int(i32::from(PROXY_HEALTH_PORT)),
            ..Default::default()
        }),
        period_seconds,
        failure_threshold,
        ..Default::default()
    }
}

fn volume_mount(name: &str, mount_path: &str, read_only: bool) -> VolumeMount {
    VolumeMount {
        name: name.to_owned(),
        mount_path: mount_path.to_owned(),
        read_only: read_only.then_some(true),
        ..Default::default()
    }
}

fn empty_dir_volume(name: &str) -> Volume {
    Volume {
        name: name.to_owned(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
    }
}

fn container_ports(resolved: &ResolvedIronProxy) -> Vec<ContainerPort> {
    let mut ports = vec![
        container_port("proxy", resolved.proxy_port),
        container_port("management", PROXY_MANAGEMENT_PORT),
        container_port("health", PROXY_HEALTH_PORT),
    ];
    if let Some(pg) = &resolved.pg {
        ports.push(container_port("pg", pg.port));
    }
    ports
}

fn iron_proxy_service_name(id: &SandboxId) -> String {
    format!("{}-proxy", id.as_str())
}

fn new_iron_proxy_pod_name(id: &SandboxId) -> String {
    format!("{}-proxy-{}", id.as_str(), unique_suffix())
}

fn iron_proxy_sandbox_egress_policy_name(id: &SandboxId) -> String {
    format!("{}-sandbox-egress", id.as_str())
}

fn iron_proxy_policy_name(id: &SandboxId) -> String {
    format!("{}-proxy-net", id.as_str())
}

fn sandbox_labels(id: &SandboxId, generation: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned()),
        (SANDBOX_ID_LABEL.to_owned(), id.as_str().to_owned()),
        (AUXILIARY_GENERATION_LABEL.to_owned(), generation.to_owned()),
    ])
}

fn iron_proxy_labels(
    id: &SandboxId,
    observability_enabled: bool,
    api_server_enabled: bool,
    generation: &str,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        (MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned()),
        (SANDBOX_ID_LABEL.to_owned(), id.as_str().to_owned()),
        (IRON_PROXY_LABEL.to_owned(), "true".to_owned()),
        (AUXILIARY_GENERATION_LABEL.to_owned(), generation.to_owned()),
    ]);
    if observability_enabled {
        labels.insert(OBSERVABILITY_ENABLED_LABEL.to_owned(), "true".to_owned());
    }
    if api_server_enabled {
        labels.insert(API_SERVER_ENABLED_LABEL.to_owned(), "true".to_owned());
    }
    labels
}

fn unique_suffix() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{millis}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved() -> ResolvedIronProxy {
        ResolvedIronProxy {
            proxy_host: "asbx-test-iron-proxy".to_owned(),
            proxy_pod_name: "asbx-test-iron-proxy-1".to_owned(),
            proxy_port: 8080,
            console_url: "http://console:3000".to_owned(),
            principal_id: "principal".to_owned(),
            labels: BTreeMap::new(),
            pg: None,
            replace_placeholders: BTreeMap::new(),
            management_api_key: "test-management-key".to_owned(),
            observability_enabled: true,
            api_server_enabled: true,
        }
    }

    fn resolved_with_capabilities(
        observability_enabled: bool,
        api_server_enabled: bool,
    ) -> ResolvedIronProxy {
        ResolvedIronProxy {
            observability_enabled,
            api_server_enabled,
            ..resolved()
        }
    }

    fn control_target() -> ControlPlaneEgressTarget {
        ControlPlaneEgressTarget {
            peer: namespace_pod_peer(
                "centaur",
                BTreeMap::from([(
                    "app.kubernetes.io/component".to_owned(),
                    "console".to_owned(),
                )]),
            ),
            port: 3000,
        }
    }

    fn peer_namespace(peer: &NetworkPolicyPeer) -> Option<&str> {
        peer.namespace_selector
            .as_ref()?
            .match_labels
            .as_ref()?
            .get("kubernetes.io/metadata.name")
            .map(String::as_str)
    }

    fn peer_component(peer: &NetworkPolicyPeer) -> Option<&str> {
        peer.pod_selector
            .as_ref()?
            .match_labels
            .as_ref()?
            .get("app.kubernetes.io/component")
            .map(String::as_str)
    }

    fn control_peer(target: &ControlPlaneEgressTarget) -> &NetworkPolicyPeer {
        &target.peer
    }

    fn rule_allows_namespace_port(
        rule: &NetworkPolicyEgressRule,
        namespace: &str,
        port: u16,
    ) -> bool {
        rule.to.as_ref().is_some_and(|peers| {
            peers.iter().any(|peer| {
                peer.namespace_selector.as_ref().is_some_and(|selector| {
                    selector.match_labels.as_ref().is_some_and(|labels| {
                        labels
                            .get("kubernetes.io/metadata.name")
                            .map(String::as_str)
                            == Some(namespace)
                    })
                })
            })
        }) && rule.ports.as_ref().is_some_and(|ports| {
            ports
                .iter()
                .any(|policy_port| policy_port.port == Some(IntOrString::Int(i32::from(port))))
        })
    }

    fn rule_allows_all_namespaces_port(rule: &NetworkPolicyEgressRule, port: u16) -> bool {
        rule.to.as_ref().is_some_and(|peers| {
            peers.iter().any(|peer| {
                peer.namespace_selector
                    .as_ref()
                    .is_some_and(|selector| selector.match_labels.is_none())
            })
        }) && rule.ports.as_ref().is_some_and(|ports| {
            ports
                .iter()
                .any(|policy_port| policy_port.port == Some(IntOrString::Int(i32::from(port))))
        })
    }

    fn rule_allows_public_port(rule: &NetworkPolicyEgressRule, port: u16) -> bool {
        rule.to.as_ref().is_some_and(|peers| {
            peers.iter().any(|peer| {
                peer.ip_block
                    .as_ref()
                    .is_some_and(|block| block.cidr == "0.0.0.0/0")
            })
        }) && rule.ports.as_ref().is_some_and(|ports| {
            ports
                .iter()
                .any(|policy_port| policy_port.port == Some(IntOrString::Int(i32::from(port))))
        })
    }

    #[test]
    fn control_plane_egress_target_uses_configured_namespace_and_labels() {
        let target = control_plane_egress_target(
            "http://prod-centaur-console:3000",
            "centaur",
            BTreeMap::from([(
                "app.kubernetes.io/component".to_owned(),
                "console".to_owned(),
            )]),
        );
        assert_eq!(target.port, 3000);
        assert_eq!(peer_namespace(control_peer(&target)), Some("centaur"));
        assert_eq!(peer_component(control_peer(&target)), Some("console"));
    }

    #[test]
    fn iron_proxy_labels_capabilities_when_enabled() {
        let id = SandboxId::new("asbx-test");

        assert_eq!(
            iron_proxy_labels(&id, true, true, "test-generation")
                .get(OBSERVABILITY_ENABLED_LABEL)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            iron_proxy_labels(&id, true, true, "test-generation")
                .get(API_SERVER_ENABLED_LABEL)
                .map(String::as_str),
            Some("true")
        );
        let restricted_labels = iron_proxy_labels(&id, false, false, "test-generation");
        assert!(!restricted_labels.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!restricted_labels.contains_key(API_SERVER_ENABLED_LABEL));
    }

    #[test]
    fn iron_proxy_resources_carry_capability_labels() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let resolved = resolved();
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved, &sync);
        assert_eq!(
            pod.metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
                .map(String::as_str),
            Some("test-generation")
        );
        assert_eq!(
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(AUXILIARY_GENERATION_LABEL))
                .map(String::as_str),
            Some("test-generation")
        );
        assert_eq!(
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );

        let service = build_iron_proxy_service(&id, &resolved);
        assert_eq!(
            service
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
                .map(String::as_str),
            Some("test-generation")
        );
        assert_eq!(
            service
                .spec
                .as_ref()
                .and_then(|spec| spec.selector.as_ref())
                .and_then(|labels| labels.get(AUXILIARY_GENERATION_LABEL))
                .map(String::as_str),
            Some("test-generation")
        );
        assert_eq!(
            service
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            service
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            service
                .spec
                .as_ref()
                .and_then(|spec| spec.selector.as_ref())
                .and_then(|selector| selector.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            service
                .spec
                .as_ref()
                .and_then(|spec| spec.selector.as_ref())
                .and_then(|selector| selector.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );

        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved,
            &iron_proxy,
            &control_target(),
            None,
            true,
        );
        let proxy_policy = &policies[1];
        assert!(policies.iter().all(|policy| {
            policy
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
                .map(String::as_str)
                == Some("test-generation")
        }));
        assert!(policies.iter().all(|policy| {
            policy
                .spec
                .as_ref()
                .and_then(|spec| spec.pod_selector.as_ref())
                .and_then(|selector| selector.match_labels.as_ref())
                .and_then(|labels| labels.get(AUXILIARY_GENERATION_LABEL))
                .map(String::as_str)
                == Some("test-generation")
        }));
        assert_eq!(
            proxy_policy
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            proxy_policy
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            proxy_policy
                .spec
                .as_ref()
                .and_then(|spec| spec.pod_selector.as_ref())
                .and_then(|selector| selector.match_labels.as_ref())
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            proxy_policy
                .spec
                .as_ref()
                .and_then(|spec| spec.pod_selector.as_ref())
                .and_then(|selector| selector.match_labels.as_ref())
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn iron_proxy_pod_carries_only_valid_workflow_task_id_annotation() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let mut resolved = resolved();
        resolved.labels = BTreeMap::from([
            (
                WORKFLOW_TASK_ID_LABEL.to_owned(),
                "018F0054-9A67-7C17-9D26-89C2F0DC45B7".to_owned(),
            ),
            ("centaur.untrusted".to_owned(), "must-not-copy".to_owned()),
        ]);
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved, &sync);
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations
                .get(WORKFLOW_TASK_ID_ANNOTATION)
                .map(String::as_str),
            Some("018f0054-9a67-7c17-9d26-89c2f0dc45b7")
        );
        assert!(!annotations.contains_key("centaur.untrusted"));

        resolved.labels.insert(
            WORKFLOW_TASK_ID_LABEL.to_owned(),
            "not-a-workflow-task-id".to_owned(),
        );
        let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved, &sync);
        assert!(
            pod.metadata
                .annotations
                .as_ref()
                .is_none_or(|annotations| !annotations.contains_key(WORKFLOW_TASK_ID_ANNOTATION))
        );
    }

    #[test]
    fn workflow_task_id_annotation_patch_is_generation_fenced_and_clears_stale_values() {
        let metadata = ObjectMeta {
            annotations: Some(BTreeMap::from([
                (
                    AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                    "current-generation".to_owned(),
                ),
                (
                    WORKFLOW_TASK_ID_ANNOTATION.to_owned(),
                    "018f0054-9a67-7c17-9d26-89c2f0dc45b7".to_owned(),
                ),
            ])),
            uid: Some("proxy-uid".to_owned()),
            resource_version: Some("42".to_owned()),
            ..Default::default()
        };

        let clear_patch = workflow_task_id_annotation_patch(&metadata, "current-generation", None)
            .unwrap()
            .unwrap();
        let clear_value = match clear_patch {
            Patch::Merge(value) => value,
            _ => unreachable!("workflow annotation uses a merge patch"),
        };
        assert_eq!(clear_value["metadata"]["uid"], "proxy-uid");
        assert_eq!(clear_value["metadata"]["resourceVersion"], "42");
        assert!(clear_value["metadata"]["annotations"][WORKFLOW_TASK_ID_ANNOTATION].is_null());

        let set_patch = workflow_task_id_annotation_patch(
            &metadata,
            "current-generation",
            Some("018f0054-9a67-7c17-9d26-89c2f0dc45b8"),
        )
        .unwrap()
        .unwrap();
        let set_value = match set_patch {
            Patch::Merge(value) => value,
            _ => unreachable!("workflow annotation uses a merge patch"),
        };
        assert_eq!(
            set_value["metadata"]["annotations"][WORKFLOW_TASK_ID_ANNOTATION],
            "018f0054-9a67-7c17-9d26-89c2f0dc45b8"
        );
        assert!(
            workflow_task_id_annotation_patch(
                &metadata,
                "current-generation",
                Some("018f0054-9a67-7c17-9d26-89c2f0dc45b7"),
            )
            .unwrap()
            .is_none()
        );

        let mut annotation_absent = metadata.clone();
        annotation_absent
            .annotations
            .as_mut()
            .unwrap()
            .remove(WORKFLOW_TASK_ID_ANNOTATION);
        assert!(
            workflow_task_id_annotation_patch(&annotation_absent, "current-generation", None,)
                .unwrap()
                .is_none()
        );
        assert!(
            workflow_task_id_annotation_patch(&metadata, "old-generation", None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn iron_proxy_uses_narrow_source_key_refs_without_env_from() {
        let id = SandboxId::new("asbx-test");
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        for (name, key) in [
            ("OP_SERVICE_ACCOUNT_TOKEN", "service-account"),
            ("OP_CONNECT_TOKEN", "connect-token"),
        ] {
            let mut iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
            iron_proxy.secret_env = vec![IronProxySecretEnv {
                name: name.to_owned(),
                secret_name: "centaur-iron-proxy-source-auth".to_owned(),
                secret_key: key.to_owned(),
            }];

            let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved(), &sync);
            let spec = pod.spec.as_ref().expect("iron-proxy Pod spec");
            assert_eq!(spec.automount_service_account_token, Some(false));
            assert!(
                spec.volumes
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .all(|volume| volume.projected.is_none()),
                "source auth must not be materialized through a projected volume"
            );
            let secret_volumes = spec
                .volumes
                .as_ref()
                .into_iter()
                .flatten()
                .filter_map(|volume| {
                    volume
                        .secret
                        .as_ref()
                        .and_then(|secret| secret.secret_name.as_deref())
                        .map(|secret_name| (volume.name.as_str(), secret_name))
                })
                .collect::<Vec<_>>();
            assert_eq!(secret_volumes, [("iron-proxy-ca", "ca-key")]);
            assert_eq!(spec.containers.len(), 1);
            let container = &spec.containers[0];
            assert_eq!(container.name, "iron-proxy");
            assert!(container.env_from.is_none());
            let key_ref = container
                .env
                .as_ref()
                .and_then(|env| env.iter().find(|env| env.name == name))
                .and_then(|env| env.value_from.as_ref())
                .and_then(|source| source.secret_key_ref.as_ref())
                .expect("narrow secret key reference");
            assert_eq!(key_ref.name, "centaur-iron-proxy-source-auth");
            assert_eq!(key_ref.key, key);
            assert_eq!(key_ref.optional, Some(false));
        }
    }

    #[test]
    fn legacy_adoption_patch_is_uid_and_resource_version_cas() {
        let mut sandbox = crate::build_agent_sandbox(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test"),
            &crate::AgentSandboxConfig::new("test"),
        )
        .unwrap();
        sandbox.metadata.uid = Some("sandbox-old".to_owned());
        let metadata = ObjectMeta {
            uid: Some("proxy-old".to_owned()),
            resource_version: Some("17".to_owned()),
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "agents.x-k8s.io/v1alpha1".to_owned(),
                    kind: "Sandbox".to_owned(),
                    name: "asbx-test".to_owned(),
                    uid: "sandbox-old".to_owned(),
                    ..Default::default()
                },
            ]),
            ..ObjectMeta::default()
        };
        let owner = sandbox_owner_reference(&sandbox).unwrap();
        let patch = owner_reference_patch(&metadata, &owner, Some("legacy-sandbox-old")).unwrap();
        let value = match patch {
            Patch::Merge(value) => value,
            _ => unreachable!("legacy adoption uses a merge patch"),
        };
        assert_eq!(value["metadata"]["uid"], "proxy-old");
        assert_eq!(value["metadata"]["resourceVersion"], "17");
        assert_eq!(
            value["metadata"]["annotations"][AUXILIARY_GENERATION_ANNOTATION],
            "legacy-sandbox-old"
        );
        assert_eq!(
            value["metadata"]["labels"][AUXILIARY_GENERATION_LABEL],
            "legacy-sandbox-old"
        );
        let mut replacement = sandbox.clone();
        replacement.metadata.uid = Some("sandbox-replacement".to_owned());
        assert!(!legacy_auxiliary_is_adoptable(&metadata, &replacement));
    }

    #[test]
    fn replacement_service_and_policies_exclude_old_generation_pods() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let resolved = resolved();
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };
        let old_proxy = build_iron_proxy_pod_with_generation(
            &id,
            &iron_proxy,
            &resolved,
            &sync,
            "generation-old",
        );
        let replacement_service =
            build_iron_proxy_service_with_generation(&id, &resolved, "generation-replacement");
        let replacement_policies = build_iron_proxy_network_policies_with_generation(
            &id,
            &resolved,
            "generation-replacement",
            &iron_proxy,
            &control_target(),
            None,
            true,
        );
        let old_labels = old_proxy.metadata.labels.as_ref().unwrap();
        let service_selector = replacement_service
            .spec
            .as_ref()
            .and_then(|spec| spec.selector.as_ref())
            .unwrap();
        assert_ne!(
            service_selector.get(AUXILIARY_GENERATION_LABEL),
            old_labels.get(AUXILIARY_GENERATION_LABEL)
        );
        let proxy_policy_selector = replacement_policies[1]
            .spec
            .as_ref()
            .and_then(|spec| spec.pod_selector.as_ref())
            .and_then(|selector| selector.match_labels.as_ref())
            .unwrap();
        assert_ne!(
            proxy_policy_selector.get(AUXILIARY_GENERATION_LABEL),
            old_labels.get(AUXILIARY_GENERATION_LABEL)
        );
        let sandbox_policy_selector = replacement_policies[0]
            .spec
            .as_ref()
            .and_then(|spec| spec.pod_selector.as_ref())
            .and_then(|selector| selector.match_labels.as_ref())
            .unwrap();
        assert_eq!(
            sandbox_policy_selector.get(AUXILIARY_GENERATION_LABEL),
            Some(&"generation-replacement".to_owned())
        );
    }

    #[test]
    fn legacy_adoption_converts_service_and_policy_selectors() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let resolved = resolved();
        let mut sandbox = crate::build_agent_sandbox(
            &id,
            &SandboxSpec::new("agent:test"),
            &crate::AgentSandboxConfig::new("test"),
        )
        .unwrap();
        sandbox.metadata.uid = Some("sandbox-old".to_owned());
        let owner = sandbox_owner_reference(&sandbox).unwrap();
        let owner_reference = k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: "agents.x-k8s.io/v1alpha1".to_owned(),
            kind: "Sandbox".to_owned(),
            name: "asbx-test".to_owned(),
            uid: "sandbox-old".to_owned(),
            ..Default::default()
        };
        let mut service = build_iron_proxy_service(&id, &resolved);
        service.metadata.uid = Some("service-old".to_owned());
        service.metadata.resource_version = Some("10".to_owned());
        service.metadata.owner_references = Some(vec![owner_reference.clone()]);
        service.metadata.annotations = None;
        service
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .remove(AUXILIARY_GENERATION_LABEL);
        service
            .spec
            .as_mut()
            .unwrap()
            .selector
            .as_mut()
            .unwrap()
            .remove(AUXILIARY_GENERATION_LABEL);
        let service_patch =
            patch_value(service_generation_patch(&service, &owner, "legacy-sandbox-old").unwrap());
        assert_eq!(
            service_patch["spec"]["selector"][AUXILIARY_GENERATION_LABEL],
            "legacy-sandbox-old"
        );

        let mut policy = build_iron_proxy_network_policies(
            &id,
            &resolved,
            &iron_proxy,
            &control_target(),
            None,
            true,
        )[1]
        .clone();
        policy.metadata.uid = Some("policy-old".to_owned());
        policy.metadata.resource_version = Some("11".to_owned());
        policy.metadata.owner_references = Some(vec![owner_reference]);
        policy.metadata.annotations = None;
        policy
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .remove(AUXILIARY_GENERATION_LABEL);
        let mut old_spec = serde_json::to_value(policy.spec.as_ref().unwrap()).unwrap();
        remove_generation_selectors(&mut old_spec);
        policy.spec = Some(serde_json::from_value(old_spec).unwrap());
        assert!(!network_policy_is_generation_scoped(
            &policy,
            "legacy-sandbox-old"
        ));
        let policy_patch = patch_value(
            network_policy_generation_patch(&policy, &owner, "legacy-sandbox-old").unwrap(),
        );
        let patched_policy = NetworkPolicy {
            metadata: policy.metadata.clone(),
            spec: Some(serde_json::from_value(policy_patch["spec"].clone()).unwrap()),
        };
        assert!(network_policy_is_generation_scoped(
            &patched_policy,
            "legacy-sandbox-old"
        ));
    }

    #[test]
    fn partial_legacy_conflict_stays_retriable_until_resource_is_converted() {
        let mut sandbox = crate::build_agent_sandbox_with_generation(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test"),
            &crate::AgentSandboxConfig::new("test"),
            Some("legacy-sandbox-old"),
        )
        .unwrap();
        sandbox.metadata.uid = Some("sandbox-old".to_owned());
        let metadata = ObjectMeta {
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "agents.x-k8s.io/v1alpha1".to_owned(),
                    kind: "Sandbox".to_owned(),
                    name: "asbx-test".to_owned(),
                    uid: "sandbox-old".to_owned(),
                    ..Default::default()
                },
            ]),
            ..ObjectMeta::default()
        };
        assert_eq!(
            crate::auxiliary_generation_from_sandbox(&sandbox).unwrap(),
            "legacy-sandbox-old"
        );
        assert!(legacy_auxiliary_is_adoptable(&metadata, &sandbox));
        // A 409 leaves the fetched object unchanged. The next reconciliation
        // must therefore attempt the same ownership-fenced adoption again.
        assert!(legacy_auxiliary_is_adoptable(&metadata, &sandbox));
        let converted = ObjectMeta {
            annotations: Some(BTreeMap::from([(
                AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                "legacy-sandbox-old".to_owned(),
            )])),
            ..metadata
        };
        assert!(!legacy_auxiliary_is_adoptable(&converted, &sandbox));
    }

    fn remove_generation_selectors(value: &mut Value) {
        match value {
            Value::Object(object) => {
                if let Some(Value::Object(selector)) = object.get_mut("podSelector")
                    && let Some(Value::Object(labels)) = selector.get_mut("matchLabels")
                {
                    labels.remove(AUXILIARY_GENERATION_LABEL);
                }
                for value in object.values_mut() {
                    remove_generation_selectors(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    remove_generation_selectors(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn iron_proxy_keeps_env_from_for_environment_source() {
        let id = SandboxId::new("asbx-test");
        let mut iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        iron_proxy.env_from_secret_names = vec![
            "centaur-infra-env".to_owned(),
            "centaur-bootstrap-env".to_owned(),
        ];
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved(), &sync);
        let container = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .expect("iron-proxy container");
        let names = container
            .env_from
            .as_ref()
            .expect("environment-backed proxy secret refs")
            .iter()
            .filter_map(|source| source.secret_ref.as_ref())
            .map(|source| source.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["centaur-infra-env", "centaur-bootstrap-env"]);
    }

    #[test]
    fn iron_proxy_resources_omit_capability_labels_when_disabled() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let resolved = resolved_with_capabilities(false, false);
        let sync = ProxySyncEnv {
            proxy_id: "iprx_test".to_owned(),
            control_url: "http://console:3000".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let pod = build_iron_proxy_pod(&id, &iron_proxy, &resolved, &sync);
        let pod_labels = pod.metadata.labels.as_ref().unwrap();
        assert!(!pod_labels.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!pod_labels.contains_key(API_SERVER_ENABLED_LABEL));

        let service = build_iron_proxy_service(&id, &resolved);
        let service_labels = service.metadata.labels.as_ref().unwrap();
        assert!(!service_labels.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!service_labels.contains_key(API_SERVER_ENABLED_LABEL));
        let service_selector = service.spec.as_ref().unwrap().selector.as_ref().unwrap();
        assert!(!service_selector.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!service_selector.contains_key(API_SERVER_ENABLED_LABEL));

        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved,
            &iron_proxy,
            &control_target(),
            None,
            false,
        );
        let proxy_policy = &policies[1];
        let policy_labels = proxy_policy.metadata.labels.as_ref().unwrap();
        assert!(!policy_labels.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!policy_labels.contains_key(API_SERVER_ENABLED_LABEL));
        let policy_selector = proxy_policy
            .spec
            .as_ref()
            .unwrap()
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert!(!policy_selector.contains_key(OBSERVABILITY_ENABLED_LABEL));
        assert!(!policy_selector.contains_key(API_SERVER_ENABLED_LABEL));
    }

    #[test]
    fn sandbox_egress_policy_does_not_inline_otlp_collector_rule() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let control_target = control_target();
        let target = OtlpEgressTarget {
            namespace: "laminar".to_owned(),
            port: 8000,
        };

        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved(),
            &iron_proxy,
            &control_target,
            Some(&target),
            true,
        );
        let sandbox_egress = policies[0]
            .spec
            .as_ref()
            .unwrap()
            .egress
            .as_ref()
            .unwrap()
            .clone();
        assert!(
            !sandbox_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "laminar", 8000))
        );
        let proxy_egress = policies[1].spec.as_ref().unwrap().egress.as_ref().unwrap();
        assert!(
            proxy_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "laminar", 8000))
        );
        assert!(!proxy_egress.iter().any(|rule| rule.to.is_none()));

        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved(),
            &iron_proxy,
            &control_target,
            None,
            true,
        );
        let sandbox_egress = policies[0]
            .spec
            .as_ref()
            .unwrap()
            .egress
            .as_ref()
            .unwrap()
            .clone();
        assert!(
            !sandbox_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "laminar", 8000))
        );
    }

    #[test]
    fn restricted_sandbox_and_proxy_policies_block_internal_cluster_egress() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let control_target = control_target();
        let target = OtlpEgressTarget {
            namespace: "laminar".to_owned(),
            port: 8000,
        };

        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved(),
            &iron_proxy,
            &control_target,
            Some(&target),
            false,
        );
        let sandbox_egress = policies[0].spec.as_ref().unwrap().egress.as_ref().unwrap();
        assert!(
            !sandbox_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "laminar", 8000))
        );
        assert!(!sandbox_egress.iter().any(|rule| {
            rule.to.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|selector| {
                        selector.match_labels.as_ref() == Some(&iron_proxy.api_pod_labels)
                    })
                })
            })
        }));

        let proxy_egress = policies[1].spec.as_ref().unwrap().egress.as_ref().unwrap();
        assert!(
            !proxy_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "laminar", 8000))
        );
        assert!(!proxy_egress.iter().any(|rule| {
            rule.to.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|selector| {
                        selector.match_labels.as_ref() == Some(&iron_proxy.api_pod_labels)
                    })
                })
            })
        }));
        assert!(proxy_egress.iter().any(|rule| {
            rule.to.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.ip_block.as_ref().is_some_and(|block| {
                        block.cidr == "0.0.0.0/0"
                            && block.except.as_ref().is_some_and(|except| {
                                except.iter().any(|cidr| cidr == "10.0.0.0/8")
                                    && except.iter().any(|cidr| cidr == "172.16.0.0/12")
                                    && except.iter().any(|cidr| cidr == "192.168.0.0/16")
                            })
                    })
                })
            })
        }));
        assert!(
            proxy_egress
                .iter()
                .any(|rule| rule_allows_namespace_port(rule, "centaur", 3000))
        );
        assert!(
            proxy_egress
                .iter()
                .any(|rule| rule_allows_all_namespaces_port(rule, 5432))
        );
        assert!(
            !proxy_egress
                .iter()
                .any(|rule| rule_allows_public_port(rule, 3000))
        );
    }

    #[test]
    fn managed_proxy_env_sets_response_header_timeout() {
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        let sync = ProxySyncEnv {
            proxy_id: "proxy-id".to_owned(),
            control_url: "http://iron-control".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let env = iron_proxy_env_vars(&iron_proxy, &resolved(), &sync);
        let timeout = env
            .iter()
            .find(|var| var.name == "IRON_PROXY_UPSTREAM_RESPONSE_HEADER_TIMEOUT")
            .and_then(|var| var.value.as_deref());

        assert_eq!(timeout, Some("120s"));
    }

    #[test]
    fn managed_proxy_env_sets_upstream_deny_cidrs() {
        let mut iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");
        iron_proxy.upstream_deny_cidrs = vec![
            "169.254.169.254/32".to_owned(),
            "127.0.0.0/8".to_owned(),
            "10.42.0.0/16".to_owned(),
            "10.43.0.0/16".to_owned(),
        ];
        let sync = ProxySyncEnv {
            proxy_id: "proxy-id".to_owned(),
            control_url: "http://iron-control".to_owned(),
            token: "proxy-token".to_owned(),
            config_hash: None,
        };

        let env = iron_proxy_env_vars(&iron_proxy, &resolved(), &sync);
        let deny_cidrs = env
            .iter()
            .find(|var| var.name == PROXY_UPSTREAM_DENY_CIDRS_ENV)
            .and_then(|var| var.value.as_deref());

        assert_eq!(
            deny_cidrs,
            Some("169.254.169.254/32,127.0.0.0/8,10.42.0.0/16,10.43.0.0/16")
        );
    }

    #[test]
    fn pg_recreation_reuses_credentials_from_existing_sandbox_dsn() {
        let dsn = "postgresql://pg-user-original:pg-password-original@asbx-test-iron-proxy:5432";
        let sandbox = crate::build_agent_sandbox(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test").env(CENTAUR_POSTGRES_DSN_ENV, dsn),
            &crate::AgentSandboxConfig::new("test"),
        )
        .unwrap();

        let pg = pg_from_sandbox_env(
            &sandbox,
            crate::DEFAULT_CONTAINER_NAME,
            "0.0.0.0:5432",
            5432,
        )
        .unwrap();

        assert_eq!(pg.listen, "0.0.0.0:5432");
        assert_eq!(pg.port, 5432);
        assert_eq!(pg.user, "pg-user-original");
        assert_eq!(pg.password, "pg-password-original");
    }

    #[test]
    fn pg_recreation_ignores_unparseable_sandbox_dsn() {
        assert!(pg_from_sandbox_dsn("not-a-postgres-dsn", "0.0.0.0:5432", 5432).is_none());
        assert!(pg_from_sandbox_dsn("postgresql://@host:5432", "0.0.0.0:5432", 5432).is_none());
    }

    #[test]
    fn proxy_policy_allows_api_pods_to_management_port() {
        let id = SandboxId::new("asbx-test");
        let iron_proxy = IronProxyConfig::new("proxy:test", "ca-cert", "ca-key");

        let control_target = control_target();
        let policies = build_iron_proxy_network_policies(
            &id,
            &resolved(),
            &iron_proxy,
            &control_target,
            None,
            true,
        );
        let ingress = policies[1]
            .spec
            .as_ref()
            .unwrap()
            .ingress
            .as_ref()
            .unwrap()
            .clone();

        assert!(ingress.iter().any(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|selector| {
                        selector.match_labels.as_ref() == Some(&iron_proxy.api_pod_labels)
                    })
                })
            }) && rule.ports.as_ref().is_some_and(|ports| {
                ports.iter().any(|port| {
                    port.port == Some(IntOrString::Int(i32::from(PROXY_MANAGEMENT_PORT)))
                })
            })
        }));
        // The sandbox-facing rule must not gain the management port.
        assert!(!ingress.iter().any(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|selector| {
                        selector.match_labels.as_ref().is_some_and(|labels| {
                            labels.contains_key(SANDBOX_ID_LABEL)
                                && !labels.contains_key(IRON_PROXY_LABEL)
                        })
                    })
                })
            }) && rule.ports.as_ref().is_some_and(|ports| {
                ports.iter().any(|port| {
                    port.port == Some(IntOrString::Int(i32::from(PROXY_MANAGEMENT_PORT)))
                })
            })
        }));
    }

    fn running_proxy_pod(pod_ip: &str, env: Vec<K8sEnvVar>) -> Pod {
        use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
        Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "iron-proxy".to_owned(),
                    env: Some(env),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".to_owned()),
                pod_ip: Some(pod_ip.to_owned()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_owned(),
                    status: "True".to_owned(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn proxy_management_endpoint_read_back_off_pod_env() {
        let pod = running_proxy_pod(
            "10.1.2.3",
            vec![
                env_var("IRON_MANAGEMENT_API_KEY", "key-123"),
                env_var("IRON_MANAGEMENT_LISTEN", ":9092"),
            ],
        );
        let endpoint = proxy_management_endpoint_from_pod(&pod).unwrap();
        assert_eq!(endpoint.base_url, "http://10.1.2.3:9092");
        assert_eq!(endpoint.api_key, "key-123");

        // Overridden listen port is respected.
        let pod = running_proxy_pod(
            "10.1.2.3",
            vec![
                env_var("IRON_MANAGEMENT_API_KEY", "key-123"),
                env_var("IRON_MANAGEMENT_LISTEN", "0.0.0.0:19092"),
            ],
        );
        let endpoint = proxy_management_endpoint_from_pod(&pod).unwrap();
        assert_eq!(endpoint.base_url, "http://10.1.2.3:19092");

        // A pod without the key (pre-barrier pod) yields no endpoint.
        let pod = running_proxy_pod("10.1.2.3", vec![]);
        assert!(proxy_management_endpoint_from_pod(&pod).is_none());

        // A pod that is not running yields no endpoint.
        let mut pod = running_proxy_pod(
            "10.1.2.3",
            vec![env_var("IRON_MANAGEMENT_API_KEY", "key-123")],
        );
        pod.status.as_mut().unwrap().phase = Some("Pending".to_owned());
        assert!(proxy_management_endpoint_from_pod(&pod).is_none());
    }

    /// Stub of the proxy management API from iron-proxy's managed mode:
    /// `POST /v1/sync` -> 202, `GET /v1/status` -> the bootstrap principal for
    /// the first `mismatches` calls, then the claimed principal.
    async fn spawn_management_stub(
        api_key: &str,
        mismatches: usize,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let sync_calls = Arc::new(AtomicUsize::new(0));
        let status_calls = Arc::new(AtomicUsize::new(0));
        let auth = format!("authorization: bearer {}", api_key.to_lowercase());
        let handle = tokio::spawn({
            let sync_calls = sync_calls.clone();
            async move {
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
                    let request = String::from_utf8_lossy(&request).to_lowercase();
                    let (status_line, body) = if !request.contains(&auth) {
                        ("401 Unauthorized", r#"{"error":"unauthorized"}"#.to_owned())
                    } else if request.starts_with("post /v1/sync") {
                        sync_calls.fetch_add(1, Ordering::SeqCst);
                        ("202 Accepted", r#"{"status":"sync requested"}"#.to_owned())
                    } else if request.starts_with("get /v1/status") {
                        let calls = status_calls.fetch_add(1, Ordering::SeqCst);
                        let principal = if calls < mismatches {
                            "prin_bootstrap"
                        } else {
                            "prin_claimed"
                        };
                        (
                            "200 OK",
                            format!(
                                r#"{{"config_hash":"h","principal_id":"{principal}","principal_status":"active","synced_once":true,"last_sync_at":"2026-06-12T00:00:00Z"}}"#
                            ),
                        )
                    } else {
                        ("404 Not Found", r#"{"error":"not found"}"#.to_owned())
                    };
                    let response = format!(
                        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                }
            }
        });
        (base_url, sync_calls, handle)
    }

    fn barrier_client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn proxy_ack_waits_until_claimed_principal_is_applied() {
        let (base_url, sync_calls, server) = spawn_management_stub("test-key", 2).await;
        let endpoint = ProxyManagementEndpoint {
            base_url,
            api_key: "test-key".to_owned(),
        };

        let ack = wait_for_proxy_ack(
            &barrier_client(),
            &endpoint,
            "prin_claimed",
            None,
            false,
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(ack, ProxyAck::Applied);
        assert!(
            sync_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "the barrier should poke an immediate out-of-band sync"
        );
        server.abort();
    }

    #[tokio::test]
    async fn proxy_ack_times_out_when_principal_never_applies() {
        let (base_url, _sync_calls, server) = spawn_management_stub("test-key", usize::MAX).await;
        let endpoint = ProxyManagementEndpoint {
            base_url,
            api_key: "test-key".to_owned(),
        };

        let ack = wait_for_proxy_ack(
            &barrier_client(),
            &endpoint,
            "prin_claimed",
            None,
            false,
            Duration::from_millis(400),
            Duration::from_millis(200),
            Duration::from_millis(25),
        )
        .await;

        assert_eq!(ack, ProxyAck::TimedOut);
        server.abort();
    }

    #[tokio::test]
    async fn proxy_ack_rejects_matching_principal_with_stale_config_hash() {
        let (base_url, _sync_calls, server) = spawn_management_stub("test-key", 0).await;
        let endpoint = ProxyManagementEndpoint {
            base_url,
            api_key: "test-key".to_owned(),
        };

        let ack = wait_for_proxy_ack(
            &barrier_client(),
            &endpoint,
            "prin_claimed",
            Some("sha256:expected"),
            false,
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(ack, ProxyAck::TimedOut);
        server.abort();
    }

    #[tokio::test]
    async fn proxy_ack_reports_unavailable_management_api() {
        // Bind to grab a free port, then drop the listener so connections are
        // refused — the shape of a proxy image without the management API.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let endpoint = ProxyManagementEndpoint {
            base_url,
            api_key: "test-key".to_owned(),
        };

        let ack = wait_for_proxy_ack(
            &barrier_client(),
            &endpoint,
            "prin_claimed",
            None,
            false,
            Duration::from_secs(2),
            Duration::from_millis(300),
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(ack, ProxyAck::ManagementUnavailable);
    }

    #[test]
    fn apply_proxy_env_does_not_add_api_host_to_no_proxy() {
        let mut spec = SandboxSpec::new("centaur-agent:latest")
            .env("CENTAUR_API_URL", "http://api:8080")
            .env("NO_PROXY", "custom.internal");

        apply_proxy_env(&mut spec, &resolved());

        for name in ["NO_PROXY", "no_proxy"] {
            let value = spec
                .env
                .iter()
                .find(|env| env.name == name)
                .map(|env| env.value.clone())
                .unwrap();
            assert!(
                !value.split(',').any(|host| host == "api"),
                "{name} should not contain the API host: {value}"
            );
            assert!(
                value.split(',').any(|host| host == "custom.internal"),
                "{name} should preserve explicit NO_PROXY extras: {value}"
            );
        }
    }

    #[test]
    fn proxy_fallback_delay_subtracts_elapsed_probe_time() {
        assert_eq!(
            proxy_fallback_delay_remaining(Duration::from_secs(2)),
            Duration::from_secs(4)
        );
        assert_eq!(
            proxy_fallback_delay_remaining(Duration::from_secs(10)),
            Duration::ZERO
        );
    }

    #[test]
    fn autorotate_pin_labels_require_strict_proxy_ack() {
        assert!(!requires_strict_autorotate_proxy_ack(&BTreeMap::new()));
        assert!(!requires_strict_autorotate_proxy_ack(&BTreeMap::from([(
            "centaur.autorotate_pin_id".to_owned(),
            "pin_1".to_owned(),
        )])));
        assert!(requires_strict_autorotate_proxy_ack(&BTreeMap::from([
            ("centaur.autorotate_pin_id".to_owned(), "pin_1".to_owned()),
            ("centaur.execution_id".to_owned(), "exe_1".to_owned()),
        ])));
    }

    #[test]
    fn apply_proxy_env_adds_otlp_endpoint_host_to_no_proxy() {
        let mut spec = SandboxSpec::new("centaur-agent:latest").env(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://laminar-app-server.laminar.svc.cluster.local:8000/v1/traces",
        );

        apply_proxy_env(&mut spec, &resolved());

        for name in ["NO_PROXY", "no_proxy"] {
            let value = spec
                .env
                .iter()
                .find(|env| env.name == name)
                .map(|env| env.value.clone())
                .unwrap();
            assert!(
                value
                    .split(',')
                    .any(|host| host == "laminar-app-server.laminar.svc.cluster.local"),
                "{name} should contain the OTLP endpoint host: {value}"
            );
        }
    }

    #[test]
    fn apply_proxy_env_adds_console_url() {
        let mut spec = SandboxSpec::new("centaur-agent:latest");
        let mut resolved = resolved();
        resolved.console_url = "http://console:3000/".to_owned();

        apply_proxy_env(&mut spec, &resolved);

        let value = spec
            .env
            .iter()
            .find(|env| env.name == CENTAUR_CONSOLE_URL_ENV)
            .map(|env| env.value.as_str());
        assert_eq!(value, Some("http://console:3000/"));
    }
}
