//! Agent Sandbox Kubernetes backend.
//!
//! The Agent Sandbox CRD types are generated from the upstream CRD with
//! `just codegen-agent-sandbox-crd`.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use centaur_iron_control::IronControlClient;
use centaur_sandbox_core::{
    MountKind, ObservedSandbox, SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo,
    SandboxResult, SandboxSpec, SandboxStatus,
};
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{
    AttachParams, DeleteParams, ListParams, LogParams, Patch, PatchParams, PostParams,
    Preconditions,
};
use kube::{Api, Client, Error};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

pub use generated::agents_x_k8s_io as crd;
pub use iron_proxy::{IronProxyConfig, IronProxySecretEnv};
pub use tools::{GitHubTokenRef, ToolSource, ToolsConfig};

pub mod generated;
mod iron_proxy;
mod tools;

const BACKEND_NAME: &str = "agent-sandbox-k8s";
const DEFAULT_CONTAINER_NAME: &str = "agent";
const MANAGED_BY_LABEL: &str = "centaur.ai/managed-by";
const COMPONENT_LABEL: &str = "centaur.ai/component";
const SANDBOX_ID_LABEL: &str = "centaur.ai/sandbox-id";
const OBSERVABILITY_ENABLED_LABEL: &str = "centaur.ai/observability-enabled";
const API_SERVER_ENABLED_LABEL: &str = "centaur.ai/api-server-enabled";
const METADATA_TRACE_ENABLED_LABEL: &str = "centaur.ai/metadata-trace-enabled";
const METADATA_TRACE_CAPABILITY_SIZE_LIMIT: &str = "1Mi";
const METADATA_TRACE_RUNTIME_SIZE_LIMIT: &str = "4Mi";
const METADATA_TRACE_SPOOL_SIZE_LIMIT: &str = "64Mi";
/// Reviewed upstream artifact for the consented metadata-only sidecar. This
/// digest, not a deployment-provided source claim or mutable tag, is the
/// executable trust boundary.
pub const AUTOROTATE_TRACE_AGENT_IMAGE: &str = "us-west1-docker.pkg.dev/snappy-storm-496900-m0/autorotate/autorotate@sha256:d314bb1a420ec6bc2948a4314ee70dd597b6af645a25ff32e6793f044c5f0427";
const MANAGED_BY_VALUE: &str = "api-rs";
// iron-control principal OID the sandbox's proxy binds to, stamped at create
// so resume (which has only the sandbox id) can rebind without the spec or any
// in-memory state. Survives pause and api-rs restarts.
const IRON_CONTROL_PRINCIPAL_ANNOTATION: &str = "centaur.ai/iron-control-principal";
// RFC 3339 instant stamped when the sandbox is paused for idleness and cleared
// on resume. This keeps suspended status observable across api-rs restarts.
const PAUSED_AT_ANNOTATION: &str = "centaur.ai/paused-at";
const RUNNING_FENCE_ANNOTATION: &str = "centaur.ai/running-fence";
/// Immutable token tying every same-name auxiliary object to one Sandbox
/// assignment. It is created before the CR because egress policy objects must
/// exist before the controller creates the pod, then retained on the CR for
/// later exact cleanup.
pub(crate) const AUXILIARY_GENERATION_ANNOTATION: &str = "centaur.ai/auxiliary-generation";
pub(crate) const AUXILIARY_GENERATION_LABEL: &str = "centaur.ai/auxiliary-generation";
const AUXILIARY_CLEANUP_FINALIZER: &str = "centaur.ai/auxiliary-cleanup";

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct AgentSandboxConfig {
    pub namespace: String,
    pub field_manager: String,
    pub container_name: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub image_pull_policy: Option<String>,
    pub image_pull_secrets: Vec<String>,
    pub state_volume: Option<StateVolumeConfig>,
    pub iron_proxy: Option<IronProxyConfig>,
    pub iron_control: Option<IronControlSettings>,
    /// When set, every sandbox gets a `tools-bootstrap` init container that
    /// git-clones the tools repo into the agent's `/app/tools`, and `TOOL_DIRS`
    /// is set so the agent's shim installer finds them.
    pub tools: Option<ToolsConfig>,
    /// In-cluster OTLP collector (e.g. Laminar) used for observability-capable
    /// sandboxes. Sandbox pod egress is granted by chart-level label policy;
    /// the per-sandbox proxy uses this target for its own explicit egress.
    pub otlp_egress: Option<OtlpEgressTarget>,
    /// Opt-in, metadata-only trace sidecar. This never uses the generic OTLP
    /// passthrough path because the sidecar owns its own credentials.
    pub metadata_trace_sidecar: Option<MetadataTraceSidecarConfig>,
    pub ready_timeout: Duration,
}

/// Destination of the sandbox's direct OTLP export, expressed as the target
/// namespace (matched by `kubernetes.io/metadata.name`) and port of the
/// collector service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpEgressTarget {
    pub namespace: String,
    pub port: u16,
}

/// iron-control coordinates for sync-mode egress proxies. When set, a sandbox
/// whose spec carries an `iron_control_principal` gets a per-sandbox proxy
/// registered in iron-control (synced over `IRON_CONTROL_URL` with its
/// `iprx_` token) instead of a rendered static proxy config.
#[derive(Clone, Debug)]
pub struct IronControlSettings {
    /// Admin client used to register/deregister the per-sandbox proxy.
    pub client: IronControlClient,
    /// Base URL injected into the proxy pod as `IRON_CONTROL_URL`.
    pub control_url: String,
    /// iron-control namespace, used to resolve principals by `foreign_id`.
    pub namespace: String,
}

impl AgentSandboxConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            field_manager: "centaur-api-rs".to_owned(),
            container_name: DEFAULT_CONTAINER_NAME.to_owned(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            image_pull_policy: None,
            image_pull_secrets: Vec::new(),
            state_volume: None,
            iron_proxy: None,
            iron_control: None,
            tools: None,
            otlp_egress: None,
            metadata_trace_sidecar: None,
            ready_timeout: Duration::from_secs(60),
        }
    }

    pub fn state_volume(mut self, state_volume: StateVolumeConfig) -> Self {
        self.state_volume = Some(state_volume);
        self
    }

    pub fn iron_proxy(mut self, iron_proxy: IronProxyConfig) -> Self {
        self.iron_proxy = Some(iron_proxy);
        self
    }

    pub fn iron_control(mut self, iron_control: IronControlSettings) -> Self {
        self.iron_control = Some(iron_control);
        self
    }

    pub fn tools(mut self, tools: ToolsConfig) -> Self {
        self.tools = Some(tools);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTraceSidecarConfig {
    pub image: String,
    pub gateway_url: String,
    pub gateway_port: u16,
    pub credential_secret_name: String,
    /// Deployment-controlled Secret generation. Operators update this opaque
    /// value with the referenced Secret; it deliberately never derives from
    /// credential data.
    pub credential_secret_version: String,
    pub bearer_secret_key: String,
    pub pseudonym_key_secret_key: String,
}

impl MetadataTraceSidecarConfig {
    pub fn pinned(
        gateway_url: String,
        gateway_port: u16,
        credential_secret_name: String,
        credential_secret_version: String,
        bearer_secret_key: String,
        pseudonym_key_secret_key: String,
    ) -> Self {
        Self {
            image: AUTOROTATE_TRACE_AGENT_IMAGE.to_owned(),
            gateway_url,
            gateway_port,
            credential_secret_name,
            credential_secret_version,
            bearer_secret_key,
            pseudonym_key_secret_key,
        }
    }

    pub fn fingerprint(&self) -> String {
        // This contains only deployment selectors, never secret contents.
        let material = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            self.image,
            self.gateway_url,
            self.gateway_port,
            self.credential_secret_name,
            self.credential_secret_version,
            self.bearer_secret_key,
            self.pseudonym_key_secret_key,
        );
        hex::encode(Sha256::digest(material))
    }
    pub fn validate(&self) -> SandboxResult<()> {
        if self.image != AUTOROTATE_TRACE_AGENT_IMAGE {
            return Err(SandboxError::InvalidSpec(
                "metadata trace sidecar image must use the reviewed immutable digest".to_owned(),
            ));
        }
        let pinned_digest = self
            .image
            .rsplit_once("@sha256:")
            .map(|(_, digest)| digest)
            .filter(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
        if pinned_digest.is_none() {
            return Err(SandboxError::InvalidSpec(
                "metadata trace sidecar image must be pinned by digest".to_owned(),
            ));
        }
        let gateway = reqwest::Url::parse(&self.gateway_url).ok();
        if !gateway.is_some_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && (url.path().is_empty() || url.path() == "/")
                && url.query().is_none()
                && url.fragment().is_none()
                && url.port_or_known_default() == Some(self.gateway_port)
        }) {
            return Err(SandboxError::InvalidSpec(
                "metadata trace gateway must be an HTTPS origin with the configured port"
                    .to_owned(),
            ));
        }
        if [
            &self.credential_secret_name,
            &self.credential_secret_version,
            &self.bearer_secret_key,
            &self.pseudonym_key_secret_key,
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(SandboxError::InvalidSpec(
                "metadata trace credential Secret name, version, and keys are required".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateVolumeConfig {
    pub mount_path: String,
    pub size: String,
    pub storage_class_name: Option<String>,
}

impl StateVolumeConfig {
    pub fn new(mount_path: impl Into<String>, size: impl Into<String>) -> Self {
        Self {
            mount_path: mount_path.into(),
            size: size.into(),
            storage_class_name: None,
        }
    }

    pub fn storage_class_name(mut self, storage_class_name: impl Into<String>) -> Self {
        self.storage_class_name = Some(storage_class_name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgentSandboxBackend {
    client: Client,
    config: AgentSandboxConfig,
    // sandbox id -> current generation's iron-control proxy OID. This is a
    // recoverable cache, but generation-CAS removal prevents old cleanup from
    // deleting a replacement assignment's mapping.
    proxy_ids: Arc<Mutex<HashMap<String, ProxyMapping>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProxyMapping {
    generation: String,
    proxy_id: String,
}

fn remove_proxy_mapping_if_generation(
    mappings: &mut HashMap<String, ProxyMapping>,
    sandbox_id: &str,
    generation: &str,
) -> Option<ProxyMapping> {
    mappings
        .get(sandbox_id)
        .is_some_and(|mapping| mapping.generation == generation)
        .then(|| mappings.remove(sandbox_id))
        .flatten()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactStopAction {
    AcquireCleanupFinalizer,
    Sandbox,
    Proxy,
    StatePvc,
    ReleaseCleanupFinalizer,
}

fn exact_stop_actions(
    expected_resource_uid: Option<&str>,
    observed_resource_uid: Option<&str>,
) -> SandboxResult<[ExactStopAction; 5]> {
    if let Some(expected_resource_uid) = expected_resource_uid
        && observed_resource_uid != Some(expected_resource_uid)
    {
        return Err(SandboxError::backend(
            "sandbox resource UID changed before exact delete",
        ));
    }
    Ok([
        ExactStopAction::AcquireCleanupFinalizer,
        ExactStopAction::Sandbox,
        ExactStopAction::Proxy,
        ExactStopAction::StatePvc,
        ExactStopAction::ReleaseCleanupFinalizer,
    ])
}

fn new_auxiliary_generation() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sandbox_has_auxiliary_cleanup_finalizer(sandbox: &crd::Sandbox) -> bool {
    sandbox
        .metadata
        .finalizers
        .as_ref()
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .any(|finalizer| finalizer == AUXILIARY_CLEANUP_FINALIZER)
        })
}

fn sandbox_auxiliary_cleanup_finalizer_patch(
    sandbox: &crd::Sandbox,
    present: bool,
) -> SandboxResult<Value> {
    let uid = sandbox.metadata.uid.as_deref().ok_or_else(|| {
        SandboxError::backend("sandbox is missing UID required for cleanup finalizer")
    })?;
    let resource_version = sandbox
        .metadata
        .resource_version
        .as_deref()
        .ok_or_else(|| {
            SandboxError::backend(
                "sandbox is missing resourceVersion required for cleanup finalizer",
            )
        })?;
    let mut finalizers = sandbox.metadata.finalizers.clone().unwrap_or_default();
    finalizers.retain(|finalizer| finalizer != AUXILIARY_CLEANUP_FINALIZER);
    if present {
        finalizers.push(AUXILIARY_CLEANUP_FINALIZER.to_owned());
    }
    Ok(json!({
        "metadata": {
            "uid": uid,
            "resourceVersion": resource_version,
            "finalizers": finalizers,
        },
    }))
}

pub(crate) fn auxiliary_generation_from_sandbox(sandbox: &crd::Sandbox) -> SandboxResult<String> {
    if let Some(generation) = sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
        .filter(|generation| !generation.trim().is_empty())
        .cloned()
    {
        return Ok(generation);
    }
    let uid = sandbox.metadata.uid.as_deref().ok_or_else(|| {
        SandboxError::backend("legacy sandbox is missing UID required for auxiliary adoption")
    })?;
    Ok(format!("legacy-{uid}"))
}

fn sandbox_has_auxiliary_generation(sandbox: &crd::Sandbox) -> bool {
    sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
        .is_some_and(|generation| !generation.trim().is_empty())
}

pub(crate) fn object_is_owned_by_sandbox(metadata: &ObjectMeta, sandbox: &crd::Sandbox) -> bool {
    let Some(sandbox_uid) = sandbox.metadata.uid.as_deref() else {
        return false;
    };
    metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| owners.iter().any(|owner| owner.uid == sandbox_uid))
}

/// Return delete preconditions only for the exact auxiliary generation. Both
/// Kubernetes identity tokens are required: a replacement object's update or
/// recreation turns the delete into a conflict instead of deleting by name.
pub(crate) fn auxiliary_delete_params(
    metadata: &ObjectMeta,
    generation: &str,
) -> SandboxResult<Option<DeleteParams>> {
    if metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
        .map(String::as_str)
        != Some(generation)
    {
        return Ok(None);
    }
    let uid = metadata.uid.clone().ok_or_else(|| {
        SandboxError::backend("auxiliary resource is missing UID required for exact delete")
    })?;
    let resource_version = metadata.resource_version.clone().ok_or_else(|| {
        SandboxError::backend(
            "auxiliary resource is missing resourceVersion required for exact delete",
        )
    })?;
    Ok(Some(DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid),
            resource_version: Some(resource_version),
        }),
        ..DeleteParams::default()
    }))
}

fn exact_sandbox_delete_params(
    metadata: &ObjectMeta,
    expected_resource_uid: &str,
) -> SandboxResult<DeleteParams> {
    if metadata.uid.as_deref() != Some(expected_resource_uid) {
        return Err(SandboxError::backend(
            "sandbox resource UID changed before exact delete",
        ));
    }
    let resource_version = metadata.resource_version.clone().ok_or_else(|| {
        SandboxError::backend("sandbox is missing resourceVersion required for exact delete")
    })?;
    Ok(DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(expected_resource_uid.to_owned()),
            resource_version: Some(resource_version),
        }),
        ..DeleteParams::default()
    })
}

impl AgentSandboxBackend {
    pub fn new(client: Client, config: AgentSandboxConfig) -> Self {
        Self {
            client,
            config,
            proxy_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn try_default(namespace: impl Into<String>) -> SandboxResult<Self> {
        let client = Client::try_default()
            .await
            .map_err(|err| SandboxError::backend_source("create kube client", err))?;
        Ok(Self::new(client, AgentSandboxConfig::new(namespace)))
    }

    fn sandboxes(&self) -> Api<crd::Sandbox> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn persistent_volume_claims(&self) -> Api<PersistentVolumeClaim> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    async fn get_sandbox(&self, id: &SandboxId) -> SandboxResult<Option<crd::Sandbox>> {
        match self.sandboxes().get(id.as_str()).await {
            Ok(sandbox) => Ok(Some(sandbox)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(map_kube_error("get sandbox", err)),
        }
    }

    async fn get_pod(&self, id: &SandboxId) -> SandboxResult<Option<Pod>> {
        match self.pods().get(id.as_str()).await {
            Ok(pod) => Ok(Some(pod)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(map_kube_error("get sandbox pod", err)),
        }
    }

    async fn observed_from_sandbox(
        &self,
        id: &SandboxId,
        sandbox: &crd::Sandbox,
    ) -> SandboxResult<ObservedSandbox> {
        let replicas = sandbox.spec.replicas.unwrap_or(1);
        let pod = self.get_pod(id).await?;
        let status = sandbox_status_with_termination(
            sandbox.metadata.deletion_timestamp.is_some(),
            replicas,
            pod.as_ref(),
        );
        Ok(ObservedSandbox::new(id.clone(), BACKEND_NAME, status)
            .with_component(sandbox_component(sandbox))
            .with_resource_uid(sandbox.metadata.uid.clone())
            .with_created_at(sandbox_creation_time(sandbox))
            .with_suspended_since(sandbox_paused_at(sandbox)))
    }

    async fn patch_sandbox_merge(&self, id: &SandboxId, patch: Value) -> SandboxResult<()> {
        let params = PatchParams::apply(&self.config.field_manager);
        self.sandboxes()
            .patch(id.as_str(), &params, &Patch::Merge(patch))
            .await
            .map(|_| ())
            .map_err(|err| map_kube_error("patch sandbox", err))
    }

    async fn patch_sandbox_exact_merge(
        &self,
        id: &SandboxId,
        resource_version: &str,
        patch: Value,
    ) -> SandboxResult<()> {
        self.patch_sandbox_merge(
            id,
            sandbox_merge_patch_with_resource_version(patch, resource_version)?,
        )
        .await
    }

    async fn ensure_auxiliary_cleanup_finalizer(
        &self,
        id: &SandboxId,
        sandbox: crd::Sandbox,
    ) -> SandboxResult<crd::Sandbox> {
        if sandbox_has_auxiliary_cleanup_finalizer(&sandbox) {
            return Ok(sandbox);
        }
        if sandbox.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::backend(
                "terminating sandbox is missing the auxiliary cleanup finalizer",
            ));
        }
        let patch = sandbox_auxiliary_cleanup_finalizer_patch(&sandbox, true)?;
        self.sandboxes()
            .patch(id.as_str(), &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|err| map_kube_error("install sandbox auxiliary cleanup finalizer", err))
    }

    async fn release_auxiliary_cleanup_finalizer(
        &self,
        id: &SandboxId,
        expected_resource_uid: &str,
    ) -> SandboxResult<()> {
        let Some(current) = self.get_sandbox(id).await? else {
            return Ok(());
        };
        if current.metadata.uid.as_deref() != Some(expected_resource_uid)
            || !sandbox_has_auxiliary_cleanup_finalizer(&current)
        {
            return Ok(());
        }
        let patch = sandbox_auxiliary_cleanup_finalizer_patch(&current, false)?;
        match self
            .sandboxes()
            .patch(id.as_str(), &PatchParams::default(), &Patch::Merge(patch))
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error(
                "release sandbox auxiliary cleanup finalizer",
                err,
            )),
        }
    }

    async fn stop_observed_sandbox(
        &self,
        id: &SandboxId,
        sandbox: crd::Sandbox,
        expected_resource_uid: &str,
    ) -> SandboxResult<()> {
        exact_stop_actions(Some(expected_resource_uid), sandbox.metadata.uid.as_deref())?;
        let (sandbox, generation) = self.ensure_auxiliary_generation(id, sandbox).await?;
        let sandbox = self.ensure_auxiliary_cleanup_finalizer(id, sandbox).await?;
        if sandbox.metadata.deletion_timestamp.is_none() {
            let params = exact_sandbox_delete_params(&sandbox.metadata, expected_resource_uid)?;
            match self.sandboxes().delete(id.as_str(), &params).await {
                Ok(_) => {}
                Err(err) if is_not_found(&err) => {
                    return Err(SandboxError::backend(
                        "sandbox disappeared after its auxiliary cleanup finalizer was installed",
                    ));
                }
                Err(err) => return Err(map_kube_error("delete exact sandbox", err)),
            }
        }
        // A deletionTimestamp makes every reuse path terminal while the
        // finalizer retains the generation needed for retryable exact cleanup.
        self.delete_iron_proxy_resources(id, &generation).await?;
        self.delete_state_pvc(id, &generation).await?;
        self.release_auxiliary_cleanup_finalizer(id, expected_resource_uid)
            .await
    }

    /// Upgrade a pre-generation Sandbox while it still exists. Only resources
    /// already owned by this exact CR UID are stamped, so an old controller
    /// cannot adopt or retire same-name resources precreated for a replacement.
    async fn ensure_auxiliary_generation(
        &self,
        id: &SandboxId,
        sandbox: crd::Sandbox,
    ) -> SandboxResult<(crd::Sandbox, String)> {
        let generation = auxiliary_generation_from_sandbox(&sandbox)?;
        let sandbox = if sandbox_has_auxiliary_generation(&sandbox) {
            sandbox
        } else {
            let resource_version =
                sandbox
                    .metadata
                    .resource_version
                    .as_deref()
                    .ok_or_else(|| {
                        SandboxError::backend(
                            "legacy sandbox is missing resourceVersion required for adoption",
                        )
                    })?;
            let uid = sandbox.metadata.uid.as_deref().ok_or_else(|| {
                SandboxError::backend("legacy sandbox is missing UID required for adoption")
            })?;
            let patch = Patch::Merge(json!({
                "metadata": {
                    "uid": uid,
                    "resourceVersion": resource_version,
                    "annotations": { AUXILIARY_GENERATION_ANNOTATION: generation },
                    "labels": { AUXILIARY_GENERATION_LABEL: generation },
                },
                "spec": {
                    "podTemplate": {
                        "metadata": {
                            "labels": { AUXILIARY_GENERATION_LABEL: generation },
                        },
                    },
                },
            }));
            self.sandboxes()
                .patch(id.as_str(), &PatchParams::default(), &patch)
                .await
                .map_err(|err| map_kube_error("adopt legacy sandbox auxiliary generation", err))?
        };
        // Re-run this reconciliation even after the CR was stamped: a prior
        // CAS conflict may have left one old auxiliary unconverted. Every
        // pass is ownership- and identity-fenced, so retrying cannot adopt a
        // replacement resource.
        let sandbox_pod_isolation_ready = self
            .adopt_legacy_sandbox_pod(id, &sandbox, &generation)
            .await?;
        self.adopt_legacy_iron_proxy_resources(
            id,
            &sandbox,
            &generation,
            sandbox_pod_isolation_ready,
        )
        .await?;
        self.adopt_legacy_state_pvc(id, &sandbox, &generation)
            .await?;
        Ok((sandbox, generation))
    }

    async fn adopt_legacy_sandbox_pod(
        &self,
        id: &SandboxId,
        sandbox: &crd::Sandbox,
        generation: &str,
    ) -> SandboxResult<bool> {
        let api = self.pods();
        let pod = match api.get(id.as_str()).await {
            Ok(pod) => pod,
            Err(err) if is_not_found(&err) => return Ok(true),
            Err(err) => return Err(map_kube_error("get legacy sandbox pod", err)),
        };
        if pod
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(AUXILIARY_GENERATION_LABEL))
            .map(String::as_str)
            == Some(generation)
        {
            return Ok(true);
        }
        if !object_is_owned_by_sandbox(&pod.metadata, sandbox) {
            return Ok(false);
        }
        let uid = pod.metadata.uid.as_deref().ok_or_else(|| {
            SandboxError::backend("legacy sandbox pod is missing UID required for adoption")
        })?;
        let resource_version = pod.metadata.resource_version.as_deref().ok_or_else(|| {
            SandboxError::backend(
                "legacy sandbox pod is missing resourceVersion required for adoption",
            )
        })?;
        let patch = Patch::Merge(json!({
            "metadata": {
                "uid": uid,
                "resourceVersion": resource_version,
                "labels": { AUXILIARY_GENERATION_LABEL: generation },
            },
        }));
        match api
            .patch(id.as_str(), &PatchParams::default(), &patch)
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(map_kube_error("adopt legacy sandbox pod", err)),
        }
    }

    async fn adopt_legacy_state_pvc(
        &self,
        id: &SandboxId,
        sandbox: &crd::Sandbox,
        generation: &str,
    ) -> SandboxResult<()> {
        if self.config.state_volume.is_none() {
            return Ok(());
        }
        let api = self.persistent_volume_claims();
        let name = state_pvc_name(id);
        let pvc = match api.get(&name).await {
            Ok(pvc) => pvc,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => return Err(map_kube_error("get legacy sandbox state pvc", err)),
        };
        if !object_is_owned_by_sandbox(&pvc.metadata, sandbox)
            || pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|annotations| {
                    annotations.contains_key(AUXILIARY_GENERATION_ANNOTATION)
                })
        {
            return Ok(());
        }
        let resource_version = pvc.metadata.resource_version.as_deref().ok_or_else(|| {
            SandboxError::backend("legacy sandbox state pvc is missing resourceVersion")
        })?;
        let uid = pvc
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| SandboxError::backend("legacy sandbox state pvc is missing UID"))?;
        let patch = Patch::Merge(json!({
            "metadata": {
                "uid": uid,
                "resourceVersion": resource_version,
                "annotations": { AUXILIARY_GENERATION_ANNOTATION: generation },
                "labels": { AUXILIARY_GENERATION_LABEL: generation },
            },
        }));
        match api.patch(&name, &PatchParams::default(), &patch).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error("adopt legacy sandbox state pvc", err)),
        }
    }

    async fn delete_state_pvc(&self, id: &SandboxId, generation: &str) -> SandboxResult<()> {
        if self.config.state_volume.is_none() {
            return Ok(());
        }
        let pvc_api = self.persistent_volume_claims();
        let name = state_pvc_name(id);
        let pvc = match pvc_api.get(&name).await {
            Ok(pvc) => pvc,
            Err(err) if is_not_found(&err) => return Ok(()),
            Err(err) => return Err(map_kube_error("get sandbox state pvc for delete", err)),
        };
        let Some(params) = auxiliary_delete_params(&pvc.metadata, generation)? else {
            return Ok(());
        };
        match pvc_api.delete(&name, &params).await {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(map_kube_error("delete sandbox state pvc", err)),
        }
    }

    async fn wait_until_running(&self, id: &SandboxId) -> SandboxResult<()> {
        let deadline = Instant::now() + self.config.ready_timeout;
        loop {
            match self.status(id).await? {
                SandboxStatus::Running => return Ok(()),
                SandboxStatus::Gone | SandboxStatus::Stopped => {
                    return Err(SandboxError::NotReady(format!(
                        "sandbox {} reached terminal state before running",
                        id.as_str()
                    )));
                }
                status if Instant::now() >= deadline => {
                    return Err(SandboxError::NotReady(format!(
                        "sandbox {} did not become running before timeout; latest status: {status:?}",
                        id.as_str()
                    )));
                }
                _ => sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn attach_io(&self, id: &SandboxId) -> SandboxResult<SandboxIo> {
        let sandbox = self
            .get_sandbox(id)
            .await?
            .ok_or_else(|| SandboxError::NotFound(id.as_str().to_owned()))?;
        if sandbox.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::NotReady(format!(
                "agent sandbox {} is terminating",
                id.as_str()
            )));
        }
        let resource_uid = sandbox.metadata.uid.clone().ok_or_else(|| {
            SandboxError::backend("sandbox is missing UID required for io attachment")
        })?;
        let replicas = sandbox.spec.replicas.unwrap_or(1);
        let pod = self.get_pod(id).await?.ok_or_else(|| {
            SandboxError::NotReady(format!("agent sandbox {} has no pod", id.as_str()))
        })?;
        if sandbox_status_from_pod(replicas, Some(&pod)) != SandboxStatus::Running {
            return Err(SandboxError::NotReady(format!(
                "agent sandbox {} is not running",
                id.as_str()
            )));
        }
        if !pod
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|owners| owners.iter().any(|owner| owner.uid == resource_uid))
        {
            return Err(SandboxError::backend(
                "sandbox pod owner UID does not match the attached Sandbox resource",
            ));
        }
        let params = AttachParams::default()
            .container(self.config.container_name.clone())
            .stdin(true)
            .stdout(true)
            .stderr(true)
            .tty(false);
        let mut attached = self
            .pods()
            .attach(id.as_str(), &params)
            .await
            .map_err(|err| map_kube_error("attach sandbox pod", err))?;
        let stdin = attached
            .stdin()
            .map(|stream| Box::pin(stream) as Pin<Box<dyn AsyncWrite + Send>>);
        let stdout = attached
            .stdout()
            .map(|stream| Box::pin(stream) as Pin<Box<dyn AsyncRead + Send>>);
        let stderr = attached
            .stderr()
            .map(|stream| Box::pin(stream) as Pin<Box<dyn AsyncRead + Send>>);
        let stdin = stdin.ok_or_else(|| SandboxError::io("stdin was not attached"))?;
        let stdout = stdout.ok_or_else(|| SandboxError::io("stdout was not attached"))?;
        let stderr = stderr.ok_or_else(|| SandboxError::io("stderr was not attached"))?;
        let still_attached = self.get_sandbox(id).await?.is_some_and(|current| {
            current.metadata.deletion_timestamp.is_none()
                && current.metadata.uid.as_deref() == Some(resource_uid.as_str())
        });
        if !still_attached {
            return Err(SandboxError::backend(
                "sandbox changed while opening io; refusing to bind a stale trace assignment",
            ));
        }
        // Keep kube's attach process alive as long as the returned streams are in use.
        Ok(SandboxIo::with_guard(stdin, stdout, stderr, attached)
            .with_resource_uid(Some(resource_uid)))
    }
}

fn sandbox_merge_patch_with_resource_version(
    mut patch: Value,
    resource_version: &str,
) -> SandboxResult<Value> {
    let metadata = patch
        .as_object_mut()
        .ok_or_else(|| SandboxError::backend("sandbox merge patch must be an object"))?
        .entry("metadata")
        .or_insert_with(|| json!({}));
    metadata
        .as_object_mut()
        .ok_or_else(|| SandboxError::backend("sandbox patch metadata must be an object"))?
        .insert(
            "resourceVersion".to_owned(),
            Value::String(resource_version.to_owned()),
        );
    Ok(patch)
}

#[async_trait]
impl SandboxBackend for AgentSandboxBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    async fn create(&self, spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
        let id = SandboxId::new(next_sandbox_name());
        let auxiliary_generation = new_auxiliary_generation();
        let mut spec = spec;
        let resolved_iron_proxy = self.resolve_iron_proxy(&id, &spec).await?;
        if let Some(resolved) = &resolved_iron_proxy {
            iron_proxy::apply_proxy_env(&mut spec, resolved);
        }
        if let Err(err) = self
            .create_iron_proxy_resources(&id, resolved_iron_proxy.as_ref(), &auxiliary_generation)
            .await
        {
            let _ = self
                .delete_iron_proxy_resources(&id, &auxiliary_generation)
                .await;
            return Err(err);
        }
        let sandbox = build_agent_sandbox_with_generation(
            &id,
            &spec,
            &self.config,
            Some(&auxiliary_generation),
        )?;
        let created = match self
            .sandboxes()
            .create(&PostParams::default(), &sandbox)
            .await
        {
            Ok(created) => created,
            Err(err) => {
                let _ = self
                    .delete_iron_proxy_resources(&id, &auxiliary_generation)
                    .await;
                return Err(map_kube_error("create sandbox", err));
            }
        };
        let resource_uid = created.metadata.uid.clone().ok_or_else(|| {
            SandboxError::backend("created Kubernetes sandbox did not include a resource UID")
        })?;
        // The proxy resources are created before the Sandbox CR (the egress
        // policies must exist before the pod starts), so bind them to it here
        // for cascade deletion. Failure leaves them cleanable by stop() only.
        if let Err(error) = self
            .adopt_iron_proxy_resources(&id, &created, &auxiliary_generation)
            .await
        {
            tracing::warn!(
                sandbox_id = id.as_str(),
                %error,
                "failed to set ownerReferences on iron-proxy resources"
            );
        }
        if let Err(err) = self.wait_until_running(&id).await {
            let _ = self.stop_exact(&id, Some(&resource_uid)).await;
            return Err(err);
        }
        let still_created_resource = self.get_sandbox(&id).await?.is_some_and(|sandbox| {
            sandbox.metadata.deletion_timestamp.is_none()
                && sandbox.metadata.uid.as_deref() == Some(resource_uid.as_str())
        });
        if !still_created_resource {
            let _ = self.stop_exact(&id, Some(&resource_uid)).await;
            return Err(SandboxError::backend(
                "sandbox resource changed before create completed",
            ));
        }
        Ok(SandboxHandle::new(id, BACKEND_NAME).with_resource_uid(Some(resource_uid)))
    }

    async fn open_io(&self, id: &SandboxId) -> SandboxResult<SandboxIo> {
        self.attach_io(id).await
    }

    /// Replays the workload container's stdout from the kubelet's log files.
    /// Unlike an attach stream, this includes output emitted while no reader
    /// was attached, which is what makes orphaned-execution adoption possible.
    async fn read_output_since(
        &self,
        id: &SandboxId,
        since: Option<std::time::SystemTime>,
    ) -> SandboxResult<Vec<String>> {
        let mut params = LogParams {
            container: Some(self.config.container_name.clone()),
            ..LogParams::default()
        };
        if let Some(since) = since {
            params.since_time = Some(
                jiff::Timestamp::try_from(since)
                    .map_err(|error| SandboxError::io_source("invalid log since time", error))?,
            );
        }
        let text = self
            .pods()
            .logs(id.as_str(), &params)
            .await
            .map_err(|err| map_kube_error("read sandbox pod logs", err))?;
        Ok(text.lines().map(str::to_owned).collect())
    }

    async fn status(&self, id: &SandboxId) -> SandboxResult<SandboxStatus> {
        let Some(sandbox) = self.get_sandbox(id).await? else {
            return Ok(SandboxStatus::Gone);
        };
        let replicas = sandbox.spec.replicas.unwrap_or(1);
        let pod = self.get_pod(id).await?;
        Ok(sandbox_status_with_termination(
            sandbox.metadata.deletion_timestamp.is_some(),
            replicas,
            pod.as_ref(),
        ))
    }

    async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
        let Some(sandbox) = self.get_sandbox(id).await? else {
            return Ok(ObservedSandbox::new(
                id.clone(),
                BACKEND_NAME,
                SandboxStatus::Gone,
            ));
        };
        self.observed_from_sandbox(id, &sandbox).await
    }

    async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
        let params =
            ListParams::default().labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"));
        let sandboxes = self
            .sandboxes()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list sandboxes", err))?;
        let mut observed = Vec::with_capacity(sandboxes.items.len());
        for sandbox in sandboxes.items {
            let Some(name) = sandbox.metadata.name.clone() else {
                continue;
            };
            let id = SandboxId::new(name);
            observed.push(self.observed_from_sandbox(&id, &sandbox).await?);
        }
        Ok(observed)
    }

    async fn stop(&self, id: &SandboxId) -> SandboxResult<()> {
        let Some(sandbox) = self.get_sandbox(id).await? else {
            return Ok(());
        };
        let resource_uid = sandbox.metadata.uid.clone().ok_or_else(|| {
            SandboxError::backend("sandbox is missing UID required for exact cleanup")
        })?;
        self.stop_observed_sandbox(id, sandbox, &resource_uid).await
    }

    async fn stop_exact(
        &self,
        id: &SandboxId,
        expected_resource_uid: Option<&str>,
    ) -> SandboxResult<()> {
        if let Some(expected_resource_uid) = expected_resource_uid {
            let Some(current) = self.get_sandbox(id).await? else {
                return Ok(());
            };
            return self
                .stop_observed_sandbox(id, current, expected_resource_uid)
                .await;
        }
        self.stop(id).await
    }

    async fn assign_iron_control_proxy_principal(
        &self,
        id: &SandboxId,
        principal_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        self.assign_proxy_principal(id, principal_id, labels).await
    }

    async fn ensure_iron_control_proxy_resources(
        &self,
        id: &SandboxId,
        principal_id: &str,
        labels: &BTreeMap<String, String>,
    ) -> SandboxResult<()> {
        self.ensure_proxy_resources_for_principal(id, principal_id, labels)
            .await
    }

    async fn pause(&self, id: &SandboxId) -> SandboxResult<()> {
        self.patch_sandbox_merge(id, sandbox_pause_patch(jiff::Timestamp::now()))
            .await
    }

    async fn pause_exact(
        &self,
        id: &SandboxId,
        expected_resource_uid: Option<&str>,
    ) -> SandboxResult<()> {
        if let Some(expected_resource_uid) = expected_resource_uid {
            let Some(current) = self.get_sandbox(id).await? else {
                return Ok(());
            };
            exact_stop_actions(Some(expected_resource_uid), current.metadata.uid.as_deref())?;
            let resource_version =
                current
                    .metadata
                    .resource_version
                    .as_deref()
                    .ok_or_else(|| {
                        SandboxError::backend(
                            "sandbox is missing resourceVersion required for exact pause",
                        )
                    })?;
            return self
                .patch_sandbox_exact_merge(
                    id,
                    resource_version,
                    sandbox_pause_patch(jiff::Timestamp::now()),
                )
                .await;
        }
        self.pause(id).await
    }

    async fn resume(&self, id: &SandboxId) -> SandboxResult<()> {
        self.resume_exact(id, None).await
    }

    async fn resume_exact(
        &self,
        id: &SandboxId,
        expected_resource_uid: Option<&str>,
    ) -> SandboxResult<()> {
        // Resume only has the sandbox id, not the spec, so rebind the proxy to
        // the principal recorded at create rather than re-resolving from spec.
        let resolved_iron_proxy = self.resolve_iron_proxy_for_resume(id).await?;
        let sandbox = self
            .get_sandbox(id)
            .await?
            .ok_or_else(|| SandboxError::backend("sandbox disappeared before proxy resume"))?;
        if sandbox.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::NotReady(
                "sandbox is terminating before resume".to_owned(),
            ));
        }
        exact_stop_actions(expected_resource_uid, sandbox.metadata.uid.as_deref())?;
        let (sandbox, generation) = self.ensure_auxiliary_generation(id, sandbox).await?;
        if let Err(err) = self
            .create_iron_proxy_resources(id, resolved_iron_proxy.as_ref(), &generation)
            .await
        {
            let _ = self.delete_iron_proxy_resources(id, &generation).await;
            return Err(err);
        }
        // The proxy resources were recreated, so re-bind them to the sandbox
        // for cascade deletion.
        if let Err(error) = self
            .adopt_iron_proxy_resources(id, &sandbox, &generation)
            .await
        {
            tracing::warn!(
                sandbox_id = id.as_str(),
                %error,
                "failed to set ownerReferences on resumed iron-proxy resources"
            );
        }
        let current = self
            .get_sandbox(id)
            .await?
            .ok_or_else(|| SandboxError::backend("sandbox disappeared during proxy resume"))?;
        if current.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::NotReady(
                "sandbox began terminating during resume".to_owned(),
            ));
        }
        exact_stop_actions(expected_resource_uid, current.metadata.uid.as_deref())?;
        if expected_resource_uid.is_some() {
            let resource_version =
                current
                    .metadata
                    .resource_version
                    .as_deref()
                    .ok_or_else(|| {
                        SandboxError::backend(
                            "sandbox is missing resourceVersion required for exact resume",
                        )
                    })?;
            self.patch_sandbox_exact_merge(id, resource_version, sandbox_resume_patch())
                .await?;
        } else {
            self.patch_sandbox_merge(id, sandbox_resume_patch()).await?;
        }
        self.wait_until_running(id).await
    }

    async fn ensure_running_exact(
        &self,
        id: &SandboxId,
        expected_resource_uid: &str,
        fence_nonce: &str,
    ) -> SandboxResult<()> {
        let sandbox = self
            .get_sandbox(id)
            .await?
            .ok_or_else(|| SandboxError::backend("sandbox disappeared before running fence"))?;
        if sandbox.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::NotReady(
                "sandbox is terminating before running fence".to_owned(),
            ));
        }
        exact_stop_actions(Some(expected_resource_uid), sandbox.metadata.uid.as_deref())?;
        // Auxiliary reconciliation may mutate the CR. Re-read afterward so
        // the final exact running write carries its current resourceVersion.
        let _ = self.ensure_auxiliary_generation(id, sandbox).await?;
        let current = self
            .get_sandbox(id)
            .await?
            .ok_or_else(|| SandboxError::backend("sandbox disappeared during running fence"))?;
        if current.metadata.deletion_timestamp.is_some() {
            return Err(SandboxError::NotReady(
                "sandbox began terminating during running fence".to_owned(),
            ));
        }
        exact_stop_actions(Some(expected_resource_uid), current.metadata.uid.as_deref())?;
        let resource_version = current
            .metadata
            .resource_version
            .as_deref()
            .ok_or_else(|| {
                SandboxError::backend(
                    "sandbox is missing resourceVersion required for running fence",
                )
            })?;
        self.patch_sandbox_exact_merge(
            id,
            resource_version,
            sandbox_running_fence_patch(fence_nonce),
        )
        .await?;
        self.wait_until_running(id).await
    }
}

fn sandbox_pause_patch(paused_at: jiff::Timestamp) -> Value {
    json!({
        "spec": { "replicas": 0 },
        "metadata": { "annotations": { PAUSED_AT_ANNOTATION: paused_at.to_string() } },
    })
}

fn sandbox_resume_patch() -> Value {
    // A JSON merge patch null removes the annotation.
    json!({
        "spec": { "replicas": 1 },
        "metadata": { "annotations": { PAUSED_AT_ANNOTATION: null } },
    })
}

fn sandbox_running_fence_patch(fence_nonce: &str) -> Value {
    json!({
        "spec": { "replicas": 1 },
        "metadata": { "annotations": {
            PAUSED_AT_ANNOTATION: null,
            RUNNING_FENCE_ANNOTATION: fence_nonce,
        }},
    })
}

fn sandbox_creation_time(sandbox: &crd::Sandbox) -> Option<SystemTime> {
    sandbox
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|time| SystemTime::from(time.0))
}

fn sandbox_paused_at(sandbox: &crd::Sandbox) -> Option<SystemTime> {
    let raw = sandbox
        .metadata
        .annotations
        .as_ref()?
        .get(PAUSED_AT_ANNOTATION)?;
    let timestamp = raw.parse::<jiff::Timestamp>().ok()?;
    Some(SystemTime::from(timestamp))
}

fn sandbox_component(sandbox: &crd::Sandbox) -> Option<String> {
    sandbox
        .metadata
        .labels
        .as_ref()?
        .get(COMPONENT_LABEL)
        .cloned()
}

fn sandbox_status_from_pod(replicas: i32, pod: Option<&Pod>) -> SandboxStatus {
    if replicas == 0 {
        return SandboxStatus::Suspended;
    }
    // Readiness is the Codex container's attach boundary. Optional telemetry
    // must not turn an image pull or credential-projection failure into a
    // session outage.
    let Some(pod) = pod else {
        return SandboxStatus::Created;
    };
    if pod.metadata.deletion_timestamp.is_some() {
        return SandboxStatus::Created;
    }

    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    match phase.as_str() {
        "running" | "pending" if pod_ready(pod) => SandboxStatus::Running,
        "running" | "pending" => SandboxStatus::Created,
        "succeeded" | "failed" => SandboxStatus::Stopped,
        "unknown" => SandboxStatus::Unknown("unknown".to_owned()),
        other => SandboxStatus::Unknown(other.to_owned()),
    }
}

fn sandbox_status_with_termination(
    terminating: bool,
    replicas: i32,
    pod: Option<&Pod>,
) -> SandboxStatus {
    if terminating {
        return SandboxStatus::Gone;
    }
    sandbox_status_from_pod(replicas, pod)
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
        .is_some_and(|statuses| {
            statuses
                .iter()
                .any(|container| container.name == DEFAULT_CONTAINER_NAME && container.ready)
        })
}

#[cfg(test)]
fn build_agent_sandbox(
    id: &SandboxId,
    spec: &SandboxSpec,
    config: &AgentSandboxConfig,
) -> SandboxResult<crd::Sandbox> {
    build_agent_sandbox_with_generation(id, spec, config, None)
}

fn build_agent_sandbox_with_generation(
    id: &SandboxId,
    spec: &SandboxSpec,
    config: &AgentSandboxConfig,
    auxiliary_generation: Option<&str>,
) -> SandboxResult<crd::Sandbox> {
    let mut labels = config.labels.clone();
    labels.extend(spec.labels.clone());
    labels.insert(MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned());
    labels.insert(SANDBOX_ID_LABEL.to_owned(), id.as_str().to_owned());
    if spec.capabilities.observability_enabled {
        labels.insert(OBSERVABILITY_ENABLED_LABEL.to_owned(), "true".to_owned());
    }
    if spec.capabilities.api_server_enabled {
        labels.insert(API_SERVER_ENABLED_LABEL.to_owned(), "true".to_owned());
    }
    let metadata_trace_deadline = spec
        .env
        .iter()
        .find(|env| env.name == "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX")
        .and_then(|env| env.value.parse::<i64>().ok())
        .filter(|deadline| *deadline > 0);
    let metadata_trace = spec.capabilities.metadata_trace_enabled
        && spec
            .labels
            .get("centaur.ai/harness")
            .is_some_and(|harness| harness == "codex")
        && config.metadata_trace_sidecar.is_some()
        && metadata_trace_deadline.is_some();
    if metadata_trace {
        labels.insert(METADATA_TRACE_ENABLED_LABEL.to_owned(), "true".to_owned());
    }
    if let Some(generation) = auxiliary_generation {
        labels.insert(AUXILIARY_GENERATION_LABEL.to_owned(), generation.to_owned());
    }

    let mut pod_labels = labels.clone();
    pod_labels.insert(
        "app.kubernetes.io/name".to_owned(),
        "centaur-sandbox".to_owned(),
    );

    let mut container = json!({
        "name": config.container_name,
        "image": spec.image,
        "stdin": true,
        "stdinOnce": false,
        "tty": false,
    });
    insert_optional(
        &mut container,
        "imagePullPolicy",
        config.image_pull_policy.clone(),
    );
    insert_optional(&mut container, "command", spec.command.clone());
    insert_optional(
        &mut container,
        "args",
        (!spec.args.is_empty()).then(|| spec.args.clone()),
    );
    // Agent container env: spec env + tools wiring (deduped). `TOOL_DIRS`
    // is set deterministically here (not via passthrough) so it always matches
    // the path the bootstrap init container actually populates in this pod.
    let mut agent_env: Vec<(String, String)> = spec
        .env
        .iter()
        .map(|env| (env.name.clone(), env.value.clone()))
        .collect();
    let repo_cache_enabled = spec.capabilities.repo_cache.enabled();
    let scoped_tools = config
        .tools
        .as_ref()
        .filter(|_| repo_cache_enabled)
        .map(|tools| tools.scoped_for_repo_cache_access(&spec.capabilities.repo_cache));
    let repo_cache_tools = scoped_tools.as_ref().filter(|tools| tools.has_sources());
    let baked_base_tools = config.tools.is_some() && repo_cache_tools.is_none();

    if repo_cache_tools.is_some() {
        for (name, value) in tools::agent_env(repo_cache_tools) {
            upsert_env(&mut agent_env, &name, value);
        }
    } else if baked_base_tools {
        for (name, value) in tools::baked_base_agent_env() {
            upsert_env(&mut agent_env, &name, value);
        }
    }
    if metadata_trace {
        // The consented sidecar is the only trace path for this container.
        // In particular, do not let generic api-rs OTEL headers overwrite the
        // loopback-only config the trusted launcher composes at runtime.
        agent_env.retain(|(name, _)| !name.starts_with("OTEL_"));
        upsert_env(
            &mut agent_env,
            "CENTAUR_CODEX_METADATA_TRACE_ADDRESS_FILE",
            "/var/run/autorotate-trace/capability/agent/otlp-endpoint".to_owned(),
        );
        upsert_env(
            &mut agent_env,
            "CENTAUR_CODEX_METADATA_TRACE_WAIT_SECONDS",
            "5".to_owned(),
        );
    }
    insert_optional(
        &mut container,
        "env",
        (!agent_env.is_empty()).then(|| {
            agent_env
                .iter()
                .map(|(name, value)| json!({ "name": name, "value": value }))
                .collect::<Vec<_>>()
        }),
    );
    insert_optional(&mut container, "workingDir", spec.working_dir.clone());
    insert_optional(&mut container, "resources", resources_json(spec));

    let (mut volumes, mut volume_mounts) = mount_json(spec);
    let mut init_containers = Vec::new();
    let mut sidecars = Vec::new();
    if let Some(state_volume) = &config.state_volume {
        volume_mounts.push(json!({
            "name": "state",
            "mountPath": state_volume.mount_path,
        }));
    }
    if let Some(iron_proxy) = &config.iron_proxy {
        volume_mounts.push(iron_proxy::sandbox_ca_volume_mount_json());
        volumes.push(iron_proxy::sandbox_ca_volume_json(iron_proxy));
    }
    // Tool sources are bootstrapped into an emptyDir by an init container and
    // mounted into the agent at the same path `TOOL_DIRS` points at. The mount is
    // writable so `centaur-tools refresh` can fetch and republish the tree.
    if repo_cache_tools.is_some() {
        volume_mounts.extend(tools::agent_volume_mounts_json(repo_cache_tools));
        volumes.extend(tools::volumes_json(repo_cache_tools));
    }
    if metadata_trace {
        volume_mounts.push(json!({
            "name": "metadata-trace-capability",
            "mountPath": "/var/run/autorotate-trace/capability",
            "readOnly": true,
        }));
        volumes.push(metadata_trace_capability_volume_json());
    }
    insert_optional(
        &mut container,
        "volumeMounts",
        (!volume_mounts.is_empty()).then_some(volume_mounts),
    );

    // tools-bootstrap publishes the tools repo into /app/tools.
    if let Some(tools) = repo_cache_tools {
        // The sandbox NetworkPolicy only allows egress to the per-sandbox proxy
        // (plus api-rs and DNS), so when iron-proxy is on the clone must ride it.
        // `apply_proxy_env` ran before this builder, so the resolved proxy URL is
        // on the spec env; absent (proxy disabled/unresolved) the clone goes direct.
        let clone_proxy = config.iron_proxy.as_ref().and_then(|_| {
            spec.env
                .iter()
                .find(|env| env.name == "HTTPS_PROXY")
                .map(|env| tools::CloneProxy {
                    https_proxy: env.value.clone(),
                    ca_cert_path: iron_proxy::FIREWALL_CA_CERT_PATH.to_owned(),
                    ca_volume_mount: iron_proxy::sandbox_ca_volume_mount_json(),
                })
        });
        init_containers.push(tools::tools_init_container_json(
            tools,
            clone_proxy.as_ref(),
        ));
    }
    if metadata_trace {
        let trace = config
            .metadata_trace_sidecar
            .as_ref()
            .expect("metadata trace sidecar checked above");
        trace.validate()?;
        // This is deliberately an ordinary container, not a restartable init
        // container. A bad trace image or unavailable trace Secret must never
        // keep the Codex container from starting.
        sidecars.push(metadata_trace_sidecar_json(
            trace,
            metadata_trace_deadline.expect("checked above"),
        ));
        volumes.push(metadata_trace_credentials_volume_json(trace));
        volumes.push(metadata_trace_runtime_volume_json());
        volumes.push(metadata_trace_spool_volume_json());
    }

    let mut pod_spec = json!({
        "containers": [container],
        "restartPolicy": "Never",
        "automountServiceAccountToken": false,
        "enableServiceLinks": false,
    });
    if !sidecars.is_empty() {
        let containers = pod_spec["containers"]
            .as_array_mut()
            .expect("sandbox containers are an array");
        containers.append(&mut sidecars);
    }
    if repo_cache_tools.is_some() || metadata_trace {
        pod_spec["securityContext"] = tools::pod_security_context_json();
    }
    insert_optional(
        &mut pod_spec,
        "initContainers",
        (!init_containers.is_empty()).then_some(init_containers),
    );
    insert_optional(
        &mut pod_spec,
        "volumes",
        (!volumes.is_empty()).then(|| std::mem::take(&mut volumes)),
    );
    insert_optional(
        &mut pod_spec,
        "imagePullSecrets",
        (!config.image_pull_secrets.is_empty()).then(|| {
            config
                .image_pull_secrets
                .iter()
                .map(|name| json!({ "name": name }))
                .collect::<Vec<_>>()
        }),
    );

    let mut agent_spec = json!({
        "replicas": 1,
        "service": false,
        "shutdownPolicy": "Retain",
        "podTemplate": {
            "metadata": {
                "labels": pod_labels,
                "annotations": config.annotations,
            },
            "spec": pod_spec,
        },
    });
    insert_optional(
        &mut agent_spec,
        "volumeClaimTemplates",
        config
            .state_volume
            .as_ref()
            .map(|state_volume| state_volume_claim_json(state_volume, auxiliary_generation)),
    );

    let mut annotations = config.annotations.clone();
    if let Some(principal) = &spec.iron_control_principal {
        annotations.insert(
            IRON_CONTROL_PRINCIPAL_ANNOTATION.to_owned(),
            principal.clone(),
        );
    }
    if let Some(generation) = auxiliary_generation {
        annotations.insert(
            AUXILIARY_GENERATION_ANNOTATION.to_owned(),
            generation.to_owned(),
        );
    }

    let crd_spec = serde_json::from_value(agent_spec)
        .map_err(|err| SandboxError::InvalidSpec(format!("invalid Agent Sandbox spec: {err}")))?;
    let mut sandbox = crd::Sandbox::new(id.as_str(), crd_spec);
    sandbox.metadata.labels = Some(labels);
    sandbox.metadata.annotations = Some(annotations);
    if auxiliary_generation.is_some() {
        sandbox.metadata.finalizers = Some(vec![AUXILIARY_CLEANUP_FINALIZER.to_owned()]);
    }
    Ok(sandbox)
}

fn mount_json(spec: &SandboxSpec) -> (Vec<Value>, Vec<Value>) {
    let mut volumes = Vec::with_capacity(spec.mounts.len());
    let mut mounts = Vec::with_capacity(spec.mounts.len());
    for (index, mount) in spec.mounts.iter().enumerate() {
        let name = format!("mount-{index}");
        mounts.push(json!({
            "name": name,
            "mountPath": mount.target_path,
            "readOnly": mount.read_only,
        }));
        if let Some(sub_path) = &mount.sub_path
            && let Some(mount_obj) = mounts.last_mut().and_then(Value::as_object_mut)
        {
            mount_obj.insert("subPath".to_owned(), json!(sub_path));
        }
        volumes.push(match &mount.kind {
            MountKind::EmptyDir => json!({
                "name": name,
                "emptyDir": {},
            }),
            MountKind::NamedVolume(claim_name) => json!({
                "name": name,
                "persistentVolumeClaim": {
                    "claimName": claim_name,
                    "readOnly": mount.read_only,
                },
            }),
            MountKind::Bind { source_path } => json!({
                "name": name,
                "hostPath": {
                    "path": source_path,
                },
            }),
        });
    }
    (volumes, mounts)
}

fn resources_json(spec: &SandboxSpec) -> Option<Value> {
    let resources = spec.resources.as_ref()?;
    let mut limits = serde_json::Map::new();
    if let Some(cpu_millis) = resources.cpu_millis {
        limits.insert("cpu".to_owned(), json!(format!("{cpu_millis}m")));
    }
    if let Some(memory_bytes) = resources.memory_bytes {
        limits.insert("memory".to_owned(), json!(format!("{memory_bytes}")));
    }
    (!limits.is_empty()).then(|| json!({ "limits": limits }))
}

fn metadata_trace_capability_volume_json() -> Value {
    json!({
        "name": "metadata-trace-capability",
        "emptyDir": { "sizeLimit": METADATA_TRACE_CAPABILITY_SIZE_LIMIT },
    })
}

fn metadata_trace_runtime_volume_json() -> Value {
    json!({
        "name": "metadata-trace-runtime",
        "emptyDir": { "sizeLimit": METADATA_TRACE_RUNTIME_SIZE_LIMIT },
    })
}

fn metadata_trace_spool_volume_json() -> Value {
    json!({
        "name": "metadata-trace-spool",
        "emptyDir": { "sizeLimit": METADATA_TRACE_SPOOL_SIZE_LIMIT },
    })
}

fn metadata_trace_credentials_volume_json(config: &MetadataTraceSidecarConfig) -> Value {
    json!({
        "name": "metadata-trace-credentials",
        "projected": {
            "defaultMode": 384,
            "sources": [{
                "secret": {
                    "name": config.credential_secret_name,
                    "optional": true,
                    "items": [
                        { "key": config.bearer_secret_key, "path": "bearer", "mode": 384 },
                        { "key": config.pseudonym_key_secret_key, "path": "pseudonym-key", "mode": 384 },
                    ],
                },
            }],
        },
    })
}

fn metadata_trace_sidecar_json(config: &MetadataTraceSidecarConfig, deadline_unix: i64) -> Value {
    json!({
        "name": "metadata-trace",
        "image": config.image,
        "command": ["/bin/sh", "-ec"],
        "args": [
            "umask 077\nendpoint=/var/run/autorotate-trace/capability/agent/otlp-endpoint\ncleanup() { rm -f \"$endpoint\"; }\ntrap cleanup EXIT INT TERM\nwhile :; do\n  cleanup\n  now=$(date +%s)\n  remaining=$((AUTOROTATE_TRACE_CONSENT_EXPIRES_AT_UNIX - now))\n  [ \"$remaining\" -gt 0 ] || exit 0\n  if install -d -m 0700 /var/run/autorotate-trace/capability/agent /var/run/autorotate-trace/runtime/agent/credentials /var/run/autorotate-trace/spool && [ -r /var/run/autorotate-trace/source/bearer ] && [ -r /var/run/autorotate-trace/source/pseudonym-key ] && install -m 0600 /var/run/autorotate-trace/source/bearer /var/run/autorotate-trace/runtime/agent/credentials/bearer && install -m 0600 /var/run/autorotate-trace/source/pseudonym-key /var/run/autorotate-trace/runtime/agent/credentials/pseudonym-key; then\n    /usr/local/bin/autorotate-trace-agent &\n    agent=$!\n    ( sleep \"$remaining\"; cleanup; kill -KILL \"$agent\" 2>/dev/null || true ) &\n    fence=$!\n    wait \"$agent\" || true\n    kill \"$fence\" 2>/dev/null || true\n  fi\n  cleanup\n  sleep 5\ndone"
        ],
        "env": [
            { "name": "AUTOROTATE_TRACE_LISTEN", "value": "127.0.0.1:0" },
            { "name": "AUTOROTATE_TRACE_GATEWAY_URL", "value": config.gateway_url },
            { "name": "AUTOROTATE_TRACE_SOURCE", "value": "bojack" },
            { "name": "AUTOROTATE_TRACE_LISTEN_ADDRESS_FILE", "value": "/var/run/autorotate-trace/capability/agent/otlp-endpoint" },
            { "name": "AUTOROTATE_TRACE_BEARER_TOKEN_FILE", "value": "/var/run/autorotate-trace/runtime/agent/credentials/bearer" },
            { "name": "AUTOROTATE_TRACE_PSEUDONYM_KEY_FILE", "value": "/var/run/autorotate-trace/runtime/agent/credentials/pseudonym-key" },
            { "name": "AUTOROTATE_TRACE_SPOOL_DIR", "value": "/var/run/autorotate-trace/spool" },
            { "name": "AUTOROTATE_TRACE_CONSENT_EXPIRES_AT_UNIX", "value": deadline_unix.to_string() },
        ],
        "volumeMounts": [
            { "name": "metadata-trace-capability", "mountPath": "/var/run/autorotate-trace/capability" },
            { "name": "metadata-trace-runtime", "mountPath": "/var/run/autorotate-trace/runtime" },
            { "name": "metadata-trace-spool", "mountPath": "/var/run/autorotate-trace/spool" },
            { "name": "metadata-trace-credentials", "mountPath": "/var/run/autorotate-trace/source", "readOnly": true },
        ],
        "securityContext": {
            "runAsNonRoot": true,
            "runAsUser": 1001,
            "runAsGroup": 1001,
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] },
            "seccompProfile": { "type": "RuntimeDefault" },
        },
        "resources": {
            "limits": {
                "cpu": "100m",
                "memory": "128Mi",
                "ephemeral-storage": "128Mi",
            },
            "requests": {
                "cpu": "25m",
                "memory": "64Mi",
                "ephemeral-storage": "64Mi",
            },
        },
    })
}

fn state_volume_claim_json(
    state_volume: &StateVolumeConfig,
    auxiliary_generation: Option<&str>,
) -> Vec<Value> {
    let mut pvc_spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": {
            "requests": {
                "storage": state_volume.size,
            },
        },
    });
    insert_optional(
        &mut pvc_spec,
        "storageClassName",
        state_volume.storage_class_name.clone(),
    );
    let mut metadata = json!({"name": "state"});
    if let Some(generation) = auxiliary_generation {
        metadata["annotations"] = json!({
            AUXILIARY_GENERATION_ANNOTATION: generation,
        });
        metadata["labels"] = json!({
            AUXILIARY_GENERATION_LABEL: generation,
        });
    }
    vec![json!({
        "metadata": metadata,
        "spec": pvc_spec,
    })]
}

fn state_pvc_name(id: &SandboxId) -> String {
    format!("state-{}", id.as_str())
}

fn insert_optional<T>(target: &mut Value, key: &str, value: Option<T>)
where
    T: serde::Serialize,
{
    if let Some(value) = value {
        target[key] = json!(value);
    }
}

/// Override-or-append an env entry, so the agent container never emits a
/// duplicate env name when we layer tools/overlay wiring over `spec.env`.
fn upsert_env(env: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some(entry) = env.iter_mut().find(|(existing, _)| existing == name) {
        entry.1 = value;
    } else {
        env.push((name.to_owned(), value));
    }
}

fn next_sandbox_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("asbx-{millis}-{sequence}")
}

fn is_not_found(err: &Error) -> bool {
    matches!(err, Error::Api(api_error) if api_error.code == 404)
}

pub(crate) fn is_conflict(err: &Error) -> bool {
    matches!(err, Error::Api(api_error) if api_error.code == 409)
}

fn map_kube_error(operation: &str, err: Error) -> SandboxError {
    if is_not_found(&err) {
        SandboxError::NotFound(operation.to_owned())
    } else {
        SandboxError::backend_source(operation, err)
    }
}

#[cfg(test)]
mod tests {
    use centaur_sandbox_core::{RepoCacheAccess, ResourceLimits, SandboxCapabilities, SandboxSpec};
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
    };

    use super::*;

    #[test]
    fn builds_agent_sandbox_spec_with_state_volume_and_limits() {
        let spec = SandboxSpec::new("centaur-agent:latest")
            .command(["/bin/sh", "-lc"])
            .args(["cat"])
            .env("CENTAUR_API_URL", "http://api:8000")
            .mount(centaur_sandbox_core::Mount::new(
                MountKind::EmptyDir,
                "/workspace",
            ))
            .resources(
                ResourceLimits::new()
                    .cpu_millis(500)
                    .memory_bytes(512 * 1024 * 1024),
            );
        let config = AgentSandboxConfig::new("centaur")
            .state_volume(StateVolumeConfig::new("/home/agent/state", "10Gi"));

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();

        assert_eq!(sandbox.metadata.name.as_deref(), Some("asbx-test"));
        assert_eq!(sandbox.spec.replicas, Some(1));
        assert_eq!(
            sandbox.spec.shutdown_policy,
            Some(crd::SandboxShutdownPolicy::Retain)
        );
        assert_eq!(
            sandbox.spec.volume_claim_templates.as_ref().unwrap().len(),
            1
        );
        let container = &sandbox.spec.pod_template.spec.containers[0];
        assert_eq!(
            sandbox.spec.pod_template.spec.enable_service_links,
            Some(false)
        );
        assert_eq!(container.image.as_deref(), Some("centaur-agent:latest"));
        assert_eq!(container.stdin, Some(true));
        assert_eq!(container.volume_mounts.as_ref().unwrap().len(), 2);
        assert!(container.resources.as_ref().unwrap().limits.is_some());
    }

    #[test]
    fn labels_observability_enabled_sandboxes_for_chart_policy() {
        let spec = SandboxSpec::new("centaur-agent:latest").capabilities(SandboxCapabilities {
            repo_cache: RepoCacheAccess::All,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
        });
        let config = AgentSandboxConfig::new("centaur");

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();

        assert_eq!(
            sandbox
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            sandbox
                .spec
                .pod_template
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.labels.as_ref())
                .and_then(|labels| labels.get(OBSERVABILITY_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            sandbox
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            sandbox
                .spec
                .pod_template
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.labels.as_ref())
                .and_then(|labels| labels.get(API_SERVER_ENABLED_LABEL))
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn reads_component_label_from_sandbox_metadata() {
        let spec = SandboxSpec::new("centaur-agent:latest").label(COMPONENT_LABEL, "workflow-run");
        let sandbox = build_agent_sandbox(
            &SandboxId::new("asbx-workflow"),
            &spec,
            &AgentSandboxConfig::new("centaur"),
        )
        .unwrap();

        assert_eq!(sandbox_component(&sandbox).as_deref(), Some("workflow-run"));
    }

    #[test]
    fn omits_api_server_label_for_restricted_sandboxes() {
        let spec = SandboxSpec::new("centaur-agent:latest").capabilities(SandboxCapabilities {
            repo_cache: RepoCacheAccess::All,
            observability_enabled: false,
            api_server_enabled: false,
            metadata_trace_enabled: false,
        });
        let config = AgentSandboxConfig::new("centaur");

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();

        assert!(
            sandbox
                .metadata
                .labels
                .as_ref()
                .is_none_or(|labels| !labels.contains_key(OBSERVABILITY_ENABLED_LABEL))
        );
        assert!(
            sandbox
                .spec
                .pod_template
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.labels.as_ref())
                .is_none_or(|labels| !labels.contains_key(OBSERVABILITY_ENABLED_LABEL))
        );
        assert!(
            sandbox
                .metadata
                .labels
                .as_ref()
                .is_none_or(|labels| !labels.contains_key(API_SERVER_ENABLED_LABEL))
        );
        assert!(
            sandbox
                .spec
                .pod_template
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.labels.as_ref())
                .is_none_or(|labels| !labels.contains_key(API_SERVER_ENABLED_LABEL))
        );
    }

    #[test]
    fn tools_clone_rides_iron_proxy_when_enabled() {
        // apply_proxy_env runs before build_agent_sandbox in create(), so the
        // resolved per-sandbox proxy URL arrives on the spec env.
        let spec = SandboxSpec::new("centaur-agent:latest")
            .env("HTTPS_PROXY", "http://asbx-test-iron-proxy:8080");
        let config = AgentSandboxConfig::new("centaur")
            .tools(ToolsConfig::new("paradigmxyz/centaur", "api:test"))
            .iron_proxy(IronProxyConfig::new("proxy:test", "ca-cert", "ca-key"));

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();
        let pod_spec = &sandbox.spec.pod_template.spec;
        let bootstrap = &pod_spec.init_containers.as_ref().unwrap()[0];
        assert_eq!(bootstrap.name, "tools-bootstrap");
        let script = &bootstrap.command.as_ref().unwrap()[2];
        assert!(script.contains("export HTTPS_PROXY=\"http://asbx-test-iron-proxy:8080\""));
        assert!(script.contains("export GIT_SSL_CAINFO=\"/firewall-certs/ca-cert.pem\""));
        assert!(
            bootstrap
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|mount| mount.name == "firewall-ca")
        );

        // Without iron-proxy the clone goes direct: no proxy exports, no CA mount.
        let spec = SandboxSpec::new("centaur-agent:latest");
        let config = AgentSandboxConfig::new("centaur")
            .tools(ToolsConfig::new("paradigmxyz/centaur", "api:test"));
        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();
        let bootstrap = &sandbox
            .spec
            .pod_template
            .spec
            .init_containers
            .as_ref()
            .unwrap()[0];
        let script = &bootstrap.command.as_ref().unwrap()[2];
        assert!(!script.contains("HTTPS_PROXY"));
        assert!(
            !bootstrap
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|mount| mount.name == "firewall-ca")
        );
    }

    #[test]
    fn disabled_repo_cache_uses_baked_base_tools_without_bootstrap() {
        let spec = SandboxSpec::new("centaur-agent:latest").capabilities(SandboxCapabilities {
            repo_cache: RepoCacheAccess::None,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
        });
        let mut tools = ToolsConfig::new("paradigmxyz/centaur", "api:test");
        tools.repo_cache_path = Some("/var/lib/centaur/repos".to_owned());
        let config = AgentSandboxConfig::new("centaur").tools(tools);

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();
        let pod_spec = &sandbox.spec.pod_template.spec;
        assert!(pod_spec.init_containers.as_ref().is_none_or(Vec::is_empty));
        let tool_dirs = pod_spec.containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .find(|env| env.name == "TOOL_DIRS")
            .and_then(|env| env.value.as_deref());
        assert_eq!(tool_dirs, Some("/opt/centaur/tools"));
        assert!(
            pod_spec.containers[0]
                .volume_mounts
                .as_ref()
                .is_none_or(|mounts| {
                    !mounts.iter().any(|mount| {
                        mount.name == "tools-root"
                            || mount.name == "tools-repo-cache"
                            || mount.mount_path == "/app/tools"
                            || mount.mount_path == "/var/lib/centaur/repos"
                    })
                })
        );
        assert!(pod_spec.volumes.as_ref().is_none_or(|volumes| {
            !volumes
                .iter()
                .any(|volume| volume.name == "tools-root" || volume.name == "tools-repo-cache")
        }));
    }

    #[test]
    fn bootstrap_empty_dirs_are_writable_by_agent_uid() {
        let spec = SandboxSpec::new("centaur-agent:latest");
        let config = AgentSandboxConfig::new("centaur")
            .tools(ToolsConfig::new("paradigmxyz/centaur", "api:test"));

        let sandbox = build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();
        let pod_spec = &sandbox.spec.pod_template.spec;

        let security_context = pod_spec.security_context.as_ref().unwrap();
        assert_eq!(security_context.fs_group, Some(1001));
        assert_eq!(
            security_context.fs_group_change_policy.as_deref(),
            Some("OnRootMismatch")
        );
    }

    fn metadata_trace_config() -> MetadataTraceSidecarConfig {
        MetadataTraceSidecarConfig::pinned(
            "https://traces.example:8443".to_owned(),
            8443,
            "consumer-trace-credentials".to_owned(),
            "generation-1".to_owned(),
            "bearer".to_owned(),
            "pseudonym_key".to_owned(),
        )
    }

    #[test]
    fn metadata_trace_sidecar_rejects_any_image_other_than_the_reviewed_digest() {
        let mut config = metadata_trace_config();
        config.image = "registry.example/trace@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned();

        assert!(config.validate().is_err());
    }

    #[test]
    fn metadata_trace_secret_generation_changes_the_sandbox_fingerprint() {
        let config = metadata_trace_config();
        let fingerprint = config.fingerprint();
        let mut rotated = config.clone();
        rotated.credential_secret_version = "generation-2".to_owned();

        assert_ne!(fingerprint, rotated.fingerprint());
    }

    #[test]
    fn metadata_trace_sidecar_is_consent_gated_and_credential_is_file_only() {
        let spec = SandboxSpec::new("centaur-agent:latest")
            .label("centaur.ai/harness", "codex")
            .env(
                "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX",
                "4102444800",
            )
            .env(
                "OTEL_EXPORTER_OTLP_HEADERS",
                "authorization=Bearer not-a-token",
            )
            .capabilities(SandboxCapabilities {
                repo_cache: RepoCacheAccess::All,
                observability_enabled: true,
                api_server_enabled: true,
                metadata_trace_enabled: true,
            });
        let mut config = AgentSandboxConfig::new("centaur");
        config.metadata_trace_sidecar = Some(metadata_trace_config());

        let pod = serde_json::to_value(
            build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap(),
        )
        .unwrap();
        let rendered = pod.to_string();
        let sidecar = &pod["spec"]["podTemplate"]["spec"]["containers"][1];

        assert_eq!(sidecar["command"], json!(["/bin/sh", "-ec"]));
        let launcher = sidecar["args"][0].as_str().unwrap();
        assert!(launcher.contains("install -d -m 0700"));
        assert!(launcher.contains("install -m 0600"));
        assert!(launcher.contains("while :; do"));
        assert!(launcher.contains("cleanup() { rm -f"));
        assert!(launcher.contains("AUTOROTATE_TRACE_CONSENT_EXPIRES_AT_UNIX"));
        assert!(launcher.contains("sleep \"$remaining\"; cleanup; kill -KILL \"$agent\""));
        assert!(launcher.contains("kill -KILL \"$agent\""));
        assert!(launcher.contains("/usr/local/bin/autorotate-trace-agent &"));
        assert!(sidecar.get("restartPolicy").is_none());
        assert_eq!(sidecar["securityContext"]["runAsNonRoot"], true);
        assert_eq!(
            sidecar["securityContext"]["allowPrivilegeEscalation"],
            false
        );
        assert_eq!(sidecar["securityContext"]["readOnlyRootFilesystem"], true);
        assert_eq!(
            sidecar["securityContext"]["capabilities"]["drop"],
            json!(["ALL"])
        );
        assert_eq!(sidecar["resources"]["limits"]["cpu"], "100m");
        assert_eq!(sidecar["resources"]["limits"]["memory"], "128Mi");
        assert_eq!(sidecar["resources"]["limits"]["ephemeral-storage"], "128Mi");
        assert_eq!(
            pod["spec"]["podTemplate"]["spec"]["automountServiceAccountToken"],
            false
        );
        assert_eq!(
            pod["spec"]["podTemplate"]["spec"]["volumes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|volume| volume["name"] == "metadata-trace-credentials")
                .unwrap()["projected"]["sources"][0]["secret"]["items"],
            json!([
                { "key": "bearer", "path": "bearer", "mode": 384 },
                { "key": "pseudonym_key", "path": "pseudonym-key", "mode": 384 },
            ])
        );
        assert_eq!(
            pod["spec"]["podTemplate"]["spec"]["volumes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|volume| volume["name"] == "metadata-trace-credentials")
                .unwrap()["projected"]["sources"][0]["secret"]["optional"],
            true
        );
        for (name, size_limit) in [
            ("metadata-trace-capability", "1Mi"),
            ("metadata-trace-runtime", "4Mi"),
            ("metadata-trace-spool", "64Mi"),
        ] {
            assert_eq!(
                pod["spec"]["podTemplate"]["spec"]["volumes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|volume| volume["name"] == name)
                    .unwrap()["emptyDir"]["sizeLimit"],
                size_limit
            );
        }
        for name in [
            "AUTOROTATE_TRACE_LISTEN_ADDRESS_FILE",
            "AUTOROTATE_TRACE_BEARER_TOKEN_FILE",
            "AUTOROTATE_TRACE_PSEUDONYM_KEY_FILE",
            "AUTOROTATE_TRACE_SPOOL_DIR",
        ] {
            assert!(
                sidecar["env"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|env| env["name"] == name)
            );
        }
        assert!(rendered.contains("\"defaultMode\":384"));
        assert!(!rendered.contains("not-a-token"));
        assert!(!rendered.contains("OTEL_EXPORTER_OTLP_HEADERS"));
        let agent_env = &pod["spec"]["podTemplate"]["spec"]["containers"][0]["env"];
        assert!(
            agent_env
                .as_array()
                .unwrap()
                .iter()
                .any(|env| env["name"] == "CENTAUR_CODEX_METADATA_TRACE_ADDRESS_FILE")
        );
        assert!(agent_env.as_array().unwrap().iter().all(|env| {
            !env["name"]
                .as_str()
                .unwrap_or_default()
                .starts_with("OTEL_")
        }));
        assert!(
            sidecar["volumeMounts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|mount| mount["name"] == "metadata-trace-credentials"),
            "only the trace sidecar receives the projected credential volume"
        );
        assert!(
            pod["spec"]["podTemplate"]["spec"]["containers"][0]["volumeMounts"]
                .as_array()
                .is_none_or(|mounts| mounts
                    .iter()
                    .all(|mount| mount["name"] != "metadata-trace-credentials")),
            "the agent container must never receive trace credentials"
        );
    }

    #[test]
    fn metadata_trace_sidecar_never_attaches_to_non_codex_or_revoked_sandboxes() {
        let mut config = AgentSandboxConfig::new("centaur");
        config.metadata_trace_sidecar = Some(metadata_trace_config());
        for (harness, enabled) in [("claude-code", true), ("codex", false)] {
            let spec = SandboxSpec::new("centaur-agent:latest")
                .label("centaur.ai/harness", harness)
                .capabilities(SandboxCapabilities {
                    repo_cache: RepoCacheAccess::All,
                    observability_enabled: true,
                    api_server_enabled: true,
                    metadata_trace_enabled: enabled,
                });
            let sandbox =
                build_agent_sandbox(&SandboxId::new("asbx-test"), &spec, &config).unwrap();
            let pod = serde_json::to_value(sandbox).unwrap();
            assert!(
                pod["spec"]["podTemplate"]["spec"]["initContainers"]
                    .as_array()
                    .is_none_or(Vec::is_empty)
            );
            assert!(
                pod["spec"]["podTemplate"]["spec"]["containers"][0]["env"]
                    .as_array()
                    .is_none_or(|env| env.iter().all(|entry| {
                        entry["name"] != "CENTAUR_CODEX_METADATA_TRACE_ADDRESS_FILE"
                    }))
            );
        }
    }

    #[test]
    fn metadata_trace_gateway_is_an_https_origin_with_the_rendered_port() {
        let mut config = metadata_trace_config();
        assert!(config.validate().is_ok());
        config.gateway_url = "https://traces.example:8443/v1".to_owned();
        assert!(config.validate().is_err());
        config.gateway_url = "https://traces.example:443".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn maps_agent_sandbox_replicas_and_pod_readiness_to_status() {
        let ready_pod = pod_with_phase_and_ready("Running", true);
        assert_eq!(
            sandbox_status_from_pod(0, Some(&ready_pod)),
            SandboxStatus::Suspended
        );
        assert_eq!(
            sandbox_status_from_pod(1, Some(&ready_pod)),
            SandboxStatus::Running
        );
        assert_eq!(
            sandbox_status_with_termination(true, 1, Some(&ready_pod)),
            SandboxStatus::Gone,
            "an accepted delete is terminal even while its pod remains ready"
        );

        let unready_pod = pod_with_phase_and_ready("Running", false);
        assert_eq!(
            sandbox_status_from_pod(1, Some(&unready_pod)),
            SandboxStatus::Created
        );
        assert_eq!(sandbox_status_from_pod(1, None), SandboxStatus::Created);

        let trace_unavailable_pod = Pod {
            status: Some(PodStatus {
                // Kubernetes keeps Pod phase Pending while a second regular
                // container is in ImagePullBackOff, even after the Codex
                // container has started and is attachable.
                phase: Some("Pending".to_owned()),
                container_statuses: Some(vec![
                    ContainerStatus {
                        name: DEFAULT_CONTAINER_NAME.to_owned(),
                        ready: true,
                        ..ContainerStatus::default()
                    },
                    ContainerStatus {
                        name: "metadata-trace".to_owned(),
                        ready: false,
                        state: Some(ContainerState {
                            waiting: Some(ContainerStateWaiting {
                                reason: Some("ImagePullBackOff".to_owned()),
                                ..ContainerStateWaiting::default()
                            }),
                            ..ContainerState::default()
                        }),
                        ..ContainerStatus::default()
                    },
                ]),
                ..PodStatus::default()
            }),
            ..Pod::default()
        };
        assert_eq!(
            sandbox_status_from_pod(1, Some(&trace_unavailable_pod)),
            SandboxStatus::Running
        );

        let failed_pod = pod_with_phase_and_ready("Failed", false);
        assert_eq!(
            sandbox_status_from_pod(1, Some(&failed_pod)),
            SandboxStatus::Stopped
        );
    }

    #[test]
    fn state_pvc_name_matches_agent_sandbox_template() {
        assert_eq!(
            state_pvc_name(&SandboxId::new("asbx-test")),
            "state-asbx-test"
        );
    }

    #[test]
    fn sandbox_and_state_claim_share_the_auxiliary_generation() {
        let config = AgentSandboxConfig::new("test")
            .state_volume(StateVolumeConfig::new("/home/agent/state", "1Gi"));
        let sandbox = build_agent_sandbox_with_generation(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test"),
            &config,
            Some("generation-test"),
        )
        .unwrap();
        assert_eq!(
            sandbox
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(AUXILIARY_GENERATION_ANNOTATION))
                .map(String::as_str),
            Some("generation-test")
        );
        assert_eq!(
            sandbox.metadata.finalizers.as_deref(),
            Some(&[AUXILIARY_CLEANUP_FINALIZER.to_owned()][..])
        );
        let value = serde_json::to_value(sandbox).unwrap();
        assert_eq!(
            value["spec"]["volumeClaimTemplates"][0]["metadata"]["annotations"]
                [AUXILIARY_GENERATION_ANNOTATION],
            "generation-test"
        );
        assert_eq!(
            value["spec"]["podTemplate"]["metadata"]["labels"][AUXILIARY_GENERATION_LABEL],
            "generation-test"
        );
        assert_eq!(
            value["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]
                [AUXILIARY_GENERATION_LABEL],
            "generation-test"
        );
    }

    #[test]
    fn cleanup_finalizer_patch_is_resource_versioned_and_preserves_other_finalizers() {
        let mut sandbox = build_agent_sandbox(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test"),
            &AgentSandboxConfig::new("test"),
        )
        .unwrap();
        sandbox.metadata.uid = Some("uid-old".to_owned());
        sandbox.metadata.resource_version = Some("17".to_owned());
        sandbox.metadata.finalizers = Some(vec!["example.com/other".to_owned()]);

        let install = sandbox_auxiliary_cleanup_finalizer_patch(&sandbox, true).unwrap();
        assert_eq!(install["metadata"]["uid"], "uid-old");
        assert_eq!(install["metadata"]["resourceVersion"], "17");
        assert_eq!(
            install["metadata"]["finalizers"],
            json!(["example.com/other", AUXILIARY_CLEANUP_FINALIZER])
        );

        sandbox.metadata.finalizers = Some(vec![
            "example.com/other".to_owned(),
            AUXILIARY_CLEANUP_FINALIZER.to_owned(),
        ]);
        let release = sandbox_auxiliary_cleanup_finalizer_patch(&sandbox, false).unwrap();
        assert_eq!(
            release["metadata"]["finalizers"],
            json!(["example.com/other"])
        );
    }

    #[test]
    fn legacy_sandbox_generation_is_uid_derived_and_requires_exact_ownership() {
        let mut sandbox = build_agent_sandbox(
            &SandboxId::new("asbx-test"),
            &SandboxSpec::new("agent:test"),
            &AgentSandboxConfig::new("test"),
        )
        .unwrap();
        sandbox.metadata.uid = Some("sandbox-legacy-uid".to_owned());
        sandbox.metadata.annotations = None;
        assert_eq!(
            auxiliary_generation_from_sandbox(&sandbox).unwrap(),
            "legacy-sandbox-legacy-uid"
        );
        let owned = ObjectMeta {
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "agents.x-k8s.io/v1alpha1".to_owned(),
                    kind: "Sandbox".to_owned(),
                    name: "asbx-test".to_owned(),
                    uid: "sandbox-legacy-uid".to_owned(),
                    ..Default::default()
                },
            ]),
            ..ObjectMeta::default()
        };
        assert!(object_is_owned_by_sandbox(&owned, &sandbox));
        let mut replacement = sandbox.clone();
        replacement.metadata.uid = Some("sandbox-replacement-uid".to_owned());
        assert!(!object_is_owned_by_sandbox(&owned, &replacement));
    }

    #[test]
    fn exact_stop_uid_aba_fences_proxy_and_pvc_cleanup_order() {
        let mut calls = vec!["observe"];
        let conflict = exact_stop_actions(Some("uid-old"), Some("uid-replacement"));
        assert!(conflict.is_err());
        assert_eq!(
            calls,
            vec!["observe"],
            "UID conflict touches no auxiliaries"
        );

        calls.extend(
            exact_stop_actions(Some("uid-old"), Some("uid-old"))
                .unwrap()
                .iter()
                .map(|action| match action {
                    ExactStopAction::AcquireCleanupFinalizer => "install-cleanup-finalizer",
                    ExactStopAction::Sandbox => "delete-sandbox",
                    ExactStopAction::Proxy => "delete-proxy",
                    ExactStopAction::StatePvc => "delete-pvc",
                    ExactStopAction::ReleaseCleanupFinalizer => "release-cleanup-finalizer",
                }),
        );
        assert_eq!(
            calls,
            vec![
                "observe",
                "install-cleanup-finalizer",
                "delete-sandbox",
                "delete-proxy",
                "delete-pvc",
                "release-cleanup-finalizer",
            ]
        );
    }

    #[test]
    fn exact_pause_patch_carries_the_observed_resource_version() {
        let patch = sandbox_merge_patch_with_resource_version(
            sandbox_pause_patch(jiff::Timestamp::now()),
            "17",
        )
        .unwrap();

        assert_eq!(patch["metadata"]["resourceVersion"], "17");
        assert_eq!(patch["spec"]["replicas"], 0);
    }

    #[test]
    fn running_fence_patch_always_changes_the_nonce() {
        let first = sandbox_running_fence_patch("first");
        let second = sandbox_running_fence_patch("second");

        assert_eq!(first["spec"]["replicas"], 1);
        assert_eq!(
            first["metadata"]["annotations"][RUNNING_FENCE_ANNOTATION],
            "first"
        );
        assert_eq!(
            second["metadata"]["annotations"][RUNNING_FENCE_ANNOTATION],
            "second"
        );
        assert_ne!(first, second);
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ResourceVersionedRunningFence {
        resource_version: u64,
        running: bool,
        deleted: bool,
        nonce: Option<String>,
    }

    impl ResourceVersionedRunningFence {
        fn pause(&mut self, expected_resource_version: u64) -> Result<(), ()> {
            if self.resource_version != expected_resource_version {
                return Err(());
            }
            self.running = false;
            self.resource_version += 1;
            Ok(())
        }

        fn fence(&mut self, expected_resource_version: u64, nonce: &str) -> Result<(), ()> {
            if self.resource_version != expected_resource_version {
                return Err(());
            }
            self.running = true;
            self.nonce = Some(nonce.to_owned());
            self.resource_version += 1;
            Ok(())
        }

        fn delete(&mut self, expected_resource_version: u64) -> Result<(), ()> {
            if self.resource_version != expected_resource_version {
                return Err(());
            }
            self.running = false;
            self.deleted = true;
            self.resource_version += 1;
            Ok(())
        }
    }

    #[test]
    fn delayed_pause_after_successor_fence_conflicts_and_cannot_suspend_it() {
        let mut resource = ResourceVersionedRunningFence {
            resource_version: 1,
            running: true,
            deleted: false,
            nonce: None,
        };
        // The old pause has RV1 but its caller timed out/died before the API
        // server applied it. The successor's fenced write wins RV1 first.
        resource.fence(1, "successor").unwrap();
        assert_eq!(resource.pause(1), Err(()));
        assert!(resource.running);
        assert_eq!(resource.nonce.as_deref(), Some("successor"));
    }

    #[test]
    fn delayed_pause_winner_forces_successor_to_refetch_and_fence_new_version() {
        let mut resource = ResourceVersionedRunningFence {
            resource_version: 1,
            running: true,
            deleted: false,
            nonce: None,
        };
        // The delayed pause wins first; a stale fence at RV1 conflicts. A
        // retry after re-GET uses RV2 and is the only path back to running.
        resource.pause(1).unwrap();
        assert_eq!(resource.fence(1, "stale-successor"), Err(()));
        assert!(!resource.running);
        resource.fence(2, "retry-successor").unwrap();
        assert!(resource.running);
        assert_eq!(resource.resource_version, 3);
        assert_eq!(resource.nonce.as_deref(), Some("retry-successor"));
    }

    #[test]
    fn abandoned_pause_request_is_safe_once_successor_fence_wins() {
        let mut resource = ResourceVersionedRunningFence {
            resource_version: 41,
            running: true,
            deleted: false,
            nonce: None,
        };
        // This models a worker crash after submitting pause RV41: no durable
        // result was recorded, but the delayed request is still RV41.
        resource.fence(41, "recovered-successor").unwrap();
        assert_eq!(resource.pause(41), Err(()));
        assert!(resource.running);
    }

    #[test]
    fn delayed_exact_delete_conflicts_after_successor_running_fence() {
        let mut resource = ResourceVersionedRunningFence {
            resource_version: 7,
            running: true,
            deleted: false,
            nonce: None,
        };
        resource.fence(7, "successor").unwrap();
        assert_eq!(resource.delete(7), Err(()));
        assert!(resource.running);
        assert!(!resource.deleted);
    }

    #[test]
    fn exact_delete_winner_prevents_stale_successor_fence() {
        let mut resource = ResourceVersionedRunningFence {
            resource_version: 7,
            running: true,
            deleted: false,
            nonce: None,
        };
        resource.delete(7).unwrap();
        assert_eq!(resource.fence(7, "stale-successor"), Err(()));
        assert!(resource.deleted);
        assert!(!resource.running);
    }

    #[test]
    fn exact_sandbox_delete_requires_uid_and_resource_version() {
        let metadata = ObjectMeta {
            uid: Some("uid-1".to_owned()),
            resource_version: Some("rv-9".to_owned()),
            ..ObjectMeta::default()
        };
        let params = exact_sandbox_delete_params(&metadata, "uid-1").unwrap();
        let preconditions = params.preconditions.unwrap();
        assert_eq!(preconditions.uid.as_deref(), Some("uid-1"));
        assert_eq!(preconditions.resource_version.as_deref(), Some("rv-9"));
        assert!(exact_sandbox_delete_params(&metadata, "uid-replacement").is_err());
    }

    fn auxiliary_metadata(generation: &str, uid: &str, resource_version: &str) -> ObjectMeta {
        ObjectMeta {
            annotations: Some(BTreeMap::from([(
                AUXILIARY_GENERATION_ANNOTATION.to_owned(),
                generation.to_owned(),
            )])),
            uid: Some(uid.to_owned()),
            resource_version: Some(resource_version.to_owned()),
            ..ObjectMeta::default()
        }
    }

    #[test]
    fn auxiliary_cleanup_precedes_cr_delete_and_cannot_clean_replacements() {
        let old_generation = "generation-old";
        let replacement_generation = "generation-replacement";
        let accepted = exact_stop_actions(Some("sandbox-old"), Some("sandbox-old")).unwrap();
        assert_eq!(accepted[0], ExactStopAction::AcquireCleanupFinalizer);
        assert_eq!(accepted[1], ExactStopAction::Sandbox);
        assert_eq!(accepted[2], ExactStopAction::Proxy);
        assert_eq!(accepted[3], ExactStopAction::StatePvc);
        assert_eq!(accepted[4], ExactStopAction::ReleaseCleanupFinalizer);
        let old = [
            auxiliary_metadata(old_generation, "proxy-old", "1"),
            auxiliary_metadata(old_generation, "service-old", "2"),
            auxiliary_metadata(old_generation, "policy-old", "3"),
            auxiliary_metadata(old_generation, "pvc-old", "4"),
        ];
        let replacement = [
            auxiliary_metadata(replacement_generation, "proxy-new", "11"),
            auxiliary_metadata(replacement_generation, "service-new", "12"),
            auxiliary_metadata(replacement_generation, "policy-new", "13"),
            auxiliary_metadata(replacement_generation, "pvc-new", "14"),
        ];
        let mut mappings = HashMap::from([(
            "asbx-test".to_owned(),
            ProxyMapping {
                generation: replacement_generation.to_owned(),
                proxy_id: "proxy-replacement".to_owned(),
            },
        )]);

        // Every auxiliary is selected by the old generation before the CR's
        // UID-preconditioned delete can remove that durable generation handle.
        assert!(old.iter().all(|metadata| {
            auxiliary_delete_params(metadata, old_generation)
                .unwrap()
                .is_some()
        }));
        assert!(replacement.iter().all(|metadata| {
            auxiliary_delete_params(metadata, old_generation)
                .unwrap()
                .is_none()
        }));
        assert!(
            remove_proxy_mapping_if_generation(&mut mappings, "asbx-test", old_generation,)
                .is_none()
        );
        assert_eq!(
            mappings.get("asbx-test").unwrap().proxy_id,
            "proxy-replacement"
        );

        let mut calls = Vec::new();
        for (name, metadata) in [
            ("proxy", &old[0]),
            ("service", &old[1]),
            ("network-policy", &old[2]),
            ("pvc", &old[3]),
        ] {
            let params = auxiliary_delete_params(metadata, old_generation)
                .unwrap()
                .unwrap();
            let preconditions = params.preconditions.unwrap();
            assert_eq!(preconditions.uid, metadata.uid);
            assert_eq!(preconditions.resource_version, metadata.resource_version);
            calls.push(name);
        }
        let mut old_mapping = HashMap::from([(
            "asbx-test".to_owned(),
            ProxyMapping {
                generation: old_generation.to_owned(),
                proxy_id: "proxy-old".to_owned(),
            },
        )]);
        assert!(
            remove_proxy_mapping_if_generation(&mut old_mapping, "asbx-test", old_generation)
                .is_some()
        );
        assert!(old_mapping.is_empty());
        assert_eq!(calls, ["proxy", "service", "network-policy", "pvc"]);
    }

    fn pod_with_phase_and_ready(phase: &str, ready: bool) -> Pod {
        Pod {
            status: Some(PodStatus {
                phase: Some(phase.to_owned()),
                container_statuses: Some(vec![ContainerStatus {
                    name: DEFAULT_CONTAINER_NAME.to_owned(),
                    ready,
                    ..ContainerStatus::default()
                }]),
                ..PodStatus::default()
            }),
            ..Pod::default()
        }
    }
}
