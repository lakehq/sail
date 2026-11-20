import fs from "node:fs";

import { loadAll } from "js-yaml";

const requireImageDigest = process.argv.includes("--require-image-digest");
const inputPaths = process.argv
  .slice(2)
  .filter((argument) => argument !== "--require-image-digest");
if (inputPaths.length === 0) {
  throw new Error(
    "usage: node verify-rendered.mjs [--require-image-digest] <manifest.yaml>...",
  );
}

const documents = [];
for (const inputPath of inputPaths) {
  loadAll(fs.readFileSync(inputPath, "utf8"), (document) => {
    if (document) {
      documents.push(document);
    }
  });
}

const failures = [];
const passes = [];

function check(condition, message) {
  if (condition) {
    passes.push(message);
  } else {
    failures.push(message);
  }
}

function isPositiveInteger(value) {
  if (typeof value !== "string" || !/^[1-9]\d*$/.test(value)) {
    return false;
  }
  const number = Number(value);
  return Number.isSafeInteger(number) && number > 0;
}

function findOne(kind, name) {
  const matches = documents.filter(
    (document) =>
      document.kind === kind && (!name || document.metadata?.name === name),
  );
  check(matches.length === 1, `exactly one ${kind}${name ? `/${name}` : ""}`);
  return matches[0];
}

const deployment = findOne("Deployment", "sail-spark-server");
const service = findOne("Service", "sail-spark-server");
const role = findOne("Role", "sail-server-worker-manager");
const roleBinding = findOne("RoleBinding", "sail-server-worker-manager");
const serverAccount = findOne("ServiceAccount", "sail-server");
const workerAccount = findOne("ServiceAccount", "sail-worker");
const workerTemplateConfig = findOne("ConfigMap", "sail-worker-pod-template");
const settings = findOne("ConfigMap", "sail-production-settings");
const networkPolicy = findOne("NetworkPolicy", "sail-ingress");
const disruptionBudget = findOne("PodDisruptionBudget", "sail-spark-server");
const namespace = findOne(
  "Namespace",
  requireImageDigest ? undefined : "sail-production",
);

const workloadNamespace = deployment?.metadata?.namespace;
check(
  Boolean(workloadNamespace) &&
    [
      service,
      role,
      roleBinding,
      serverAccount,
      workerAccount,
      workerTemplateConfig,
      settings,
      networkPolicy,
      disruptionBudget,
    ]
      .filter(Boolean)
      .every((document) => document.metadata?.namespace === workloadNamespace),
  "all namespaced baseline resources share one explicit namespace",
);

if (deployment) {
  check(
    deployment.spec?.replicas === 1,
    "the baseline has one Spark Connect server replica",
  );
  check(
    deployment.spec?.strategy?.type === "Recreate",
    "Deployment updates use the Recreate strategy",
  );

  const podSpec = deployment.spec?.template?.spec;
  const server = podSpec?.containers?.find(
    (container) => container.name === "server",
  );
  const env = Object.fromEntries(
    (server?.env ?? [])
      .filter((entry) => Object.hasOwn(entry, "value"))
      .map((entry) => [entry.name, String(entry.value)]),
  );
  const rawEnv = Object.fromEntries(
    (server?.env ?? []).map((entry) => [entry.name, entry]),
  );

  check(Boolean(server), "the Spark Connect server container exists");
  check(
    deployment.spec?.template?.metadata?.labels?.[
      "app.kubernetes.io/component"
    ] === "spark-server",
    "the server pod is labeled as a Spark Connect server",
  );
  check(
    server?.ports?.length === 1 &&
      server.ports[0]?.containerPort === 50051 &&
      server.ports[0]?.hostPort === undefined &&
      server.ports[0]?.hostIP === undefined,
    "the server declares Spark Connect without a host port",
  );
  check(
    Boolean(server?.image) &&
      (requireImageDigest
        ? /@sha256:[a-f0-9]{64}$/.test(server.image)
        : !server.image.endsWith(":latest")),
    requireImageDigest
      ? "the server uses a SHA-256 image digest reference"
      : "the local server image does not use the latest tag",
  );
  check(
    server?.image === env.SAIL_KUBERNETES__IMAGE,
    "server and generated workers use the same image reference",
  );
  check(
    isPositiveInteger(env.SAIL_CLUSTER__WORKER_MAX_COUNT),
    "each session has a finite configured worker maximum",
  );
  check(
    env.SAIL_CLUSTER__WORKER_INITIAL_COUNT === "0",
    "the baseline starts no workers before a session needs them",
  );
  check(
    isPositiveInteger(env.SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__WORKER_HEARTBEAT_TIMEOUT_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__WORKER_TASK_SLOTS),
    "worker idle, heartbeat, and task-slot values are explicit and positive",
  );
  check(
    isPositiveInteger(env.SAIL_CLUSTER__TASK_MAX_ATTEMPTS),
    "the per-task attempt maximum is explicit",
  );
  check(
    env.SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE === "flight",
    "the baseline explicitly selects Flight shuffle",
  );
  check(
    isPositiveInteger(env.SAIL_EXECUTION__DEFAULT_PARALLELISM),
    "cluster execution parallelism is explicit",
  );
  check(
    isPositiveInteger(env.SAIL_SPARK__SESSION_TIMEOUT_SECS),
    "the session-activity timeout is explicit",
  );
  check(
    env.SAIL_MODE === "kubernetes-cluster" &&
      env.SAIL_CLUSTER__ENABLE_TLS === "false" &&
      env.SAIL_CLUSTER__DRIVER_LISTEN_HOST === "0.0.0.0" &&
      rawEnv.SAIL_CLUSTER__DRIVER_EXTERNAL_HOST?.valueFrom?.fieldRef
        ?.fieldPath === "status.podIP" &&
      rawEnv.SAIL_KUBERNETES__NAMESPACE?.valueFrom?.fieldRef?.fieldPath ===
        "metadata.namespace" &&
      rawEnv.SAIL_KUBERNETES__DRIVER_POD_NAME?.valueFrom?.fieldRef
        ?.fieldPath === "metadata.name" &&
      env.SAIL_KUBERNETES__WORKER_SERVICE_ACCOUNT_NAME === "sail-worker",
    "the server derives current pod identity and configures Kubernetes cluster mode",
  );
  check(
    isPositiveInteger(env.SAIL_CLUSTER__WORKER_HEARTBEAT_INTERVAL_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__WORKER_HEARTBEAT_TIMEOUT_SECS) &&
      Number(env.SAIL_CLUSTER__WORKER_HEARTBEAT_INTERVAL_SECS) <
        Number(env.SAIL_CLUSTER__WORKER_HEARTBEAT_TIMEOUT_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__WORKER_LAUNCH_TIMEOUT_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__TASK_LAUNCH_TIMEOUT_SECS) &&
      isPositiveInteger(env.SAIL_CLUSTER__TASK_STREAM_BUFFER) &&
      isPositiveInteger(env.SAIL_CLUSTER__TASK_STREAM_CREATION_TIMEOUT_SECS),
    "worker heartbeat, launch, and task-stream bounds are explicit and coherent",
  );
  check(
    env.SAIL_CLUSTER__RPC_RETRY_STRATEGY__TYPE === "fixed" &&
      isPositiveInteger(
        env.SAIL_CLUSTER__RPC_RETRY_STRATEGY__FIXED__MAX_COUNT,
      ) &&
      isPositiveInteger(
        env.SAIL_CLUSTER__RPC_RETRY_STRATEGY__FIXED__DELAY_SECS,
      ),
    "internal RPC retries are explicit and bounded",
  );
  check(
    isPositiveInteger(env.SAIL_SPARK__EXECUTION_HEARTBEAT_INTERVAL_SECS) &&
      Number(env.SAIL_SPARK__EXECUTION_HEARTBEAT_INTERVAL_SECS) <
        Number(env.SAIL_SPARK__SESSION_TIMEOUT_SECS),
    "the Spark Connect execution-heartbeat interval is explicit and below the session threshold",
  );
  check(
    env.SAIL_RUNTIME__MEMORY_POOL__TYPE === "fair" &&
      isPositiveInteger(env.SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE) &&
      env.SAIL_RUNTIME__TEMPORARY_FILES__PATHS === '["/tmp/sail"]' &&
      isPositiveInteger(env.SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE),
    "the server has explicit per-session memory-pool and temporary-file settings",
  );
  check(
    podSpec?.serviceAccountName === "sail-server" &&
      podSpec?.automountServiceAccountToken === true &&
      podSpec?.enableServiceLinks === false,
    "the server uses its own service account",
  );
  check(
    server?.startupProbe?.grpc?.port === 50051 &&
      server.startupProbe.grpc.service === "spark.connect.SparkConnectService",
    "the startup probe checks the named Spark Connect gRPC service",
  );
  check(
    server?.readinessProbe?.grpc?.port === 50051 &&
      server.readinessProbe.grpc.service ===
        "spark.connect.SparkConnectService",
    "the readiness probe checks the named Spark Connect gRPC service",
  );
  check(!server?.livenessProbe, "liveness is not inferred from readiness");
  check(
    Number(podSpec?.terminationGracePeriodSeconds) > 0,
    "the server pod has an explicit termination grace period",
  );
  check(
    !server?.lifecycle?.preStop,
    "server shutdown does not depend on a pre-stop hook",
  );
  check(
    Boolean(server?.resources?.requests?.cpu) &&
      Boolean(server?.resources?.requests?.memory) &&
      Boolean(server?.resources?.requests?.["ephemeral-storage"]) &&
      Boolean(server?.resources?.limits?.cpu) &&
      Boolean(server?.resources?.limits?.memory) &&
      Boolean(server?.resources?.limits?.["ephemeral-storage"]),
    "the server has CPU, memory, and ephemeral-storage requests and limits",
  );
  check(
    podSpec?.volumes?.some(
      (volume) => volume.name === "scratch" && volume.emptyDir?.sizeLimit,
    ) &&
      server?.volumeMounts?.some(
        (mount) => mount.name === "scratch" && mount.mountPath === "/tmp",
      ),
    "the server mounts a size-limited scratch volume at /tmp",
  );
  check(
    podSpec?.securityContext?.runAsNonRoot === true &&
      podSpec?.securityContext?.runAsUser === 10001 &&
      podSpec?.securityContext?.runAsGroup === 10001 &&
      podSpec?.securityContext?.fsGroup === 10001 &&
      podSpec?.securityContext?.seccompProfile?.type === "RuntimeDefault" &&
      podSpec?.hostNetwork !== true &&
      podSpec?.hostPID !== true &&
      podSpec?.hostIPC !== true &&
      server?.securityContext?.privileged !== true &&
      server?.securityContext?.allowPrivilegeEscalation === false &&
      server?.securityContext?.readOnlyRootFilesystem === true &&
      server?.securityContext?.capabilities?.drop?.includes("ALL"),
    "the server manifest contains the intended restricted security fields",
  );
}

if (service) {
  check(
    service.spec?.type === "ClusterIP" &&
      service.spec?.clusterIP !== "None" &&
      service.spec?.publishNotReadyAddresses !== true,
    "Spark Connect uses a non-headless ClusterIP Service",
  );
  check(
    service.spec?.ports?.length === 1 &&
      service.spec.ports[0]?.port === 50051 &&
      service.spec.ports[0]?.targetPort === "spark-connect" &&
      service.spec.ports[0]?.protocol === "TCP",
    "the Service exposes only Spark Connect port 50051",
  );
  check(
    !service.spec?.externalIPs?.length,
    "the Service declares no external IP addresses",
  );
  check(
    service.spec?.selector?.["app.kubernetes.io/name"] === "sail" &&
      service.spec?.selector?.["app.kubernetes.io/component"] ===
        "spark-server",
    "the Service selects the Spark Connect server pod",
  );
}

if (role) {
  const rules = role.rules ?? [];
  check(
    rules.length === 1 &&
      JSON.stringify([...(rules[0]?.apiGroups ?? [])].sort()) ===
        JSON.stringify([""]) &&
      JSON.stringify([...(rules[0]?.resources ?? [])].sort()) ===
        JSON.stringify(["pods"]) &&
      JSON.stringify([...(rules[0]?.verbs ?? [])].sort()) ===
        JSON.stringify(["create", "deletecollection", "get"].sort()) &&
      rules[0]?.resourceNames === undefined &&
      rules[0]?.nonResourceURLs === undefined,
    "server RBAC matches the current worker-manager Kubernetes API calls",
  );
}

check(
  roleBinding?.roleRef?.kind === "Role" &&
    roleBinding?.roleRef?.apiGroup === "rbac.authorization.k8s.io" &&
    roleBinding?.roleRef?.name === "sail-server-worker-manager" &&
    roleBinding?.subjects?.length === 1 &&
    roleBinding.subjects[0]?.kind === "ServiceAccount" &&
    roleBinding.subjects[0]?.name === "sail-server" &&
    roleBinding.subjects[0]?.namespace === workloadNamespace,
  "worker-manager RBAC binds only the server service account",
);

check(
  serverAccount?.automountServiceAccountToken === true,
  "the server service account enables automatic token mounting",
);
check(
  workerAccount?.automountServiceAccountToken === false,
  "the worker service account disables automatic token mounting",
);

if (workerTemplateConfig) {
  let template;
  try {
    template = JSON.parse(workerTemplateConfig.data?.["pod-template.json"]);
  } catch (error) {
    failures.push(`worker pod template is valid JSON: ${error.message}`);
  }
  if (template) {
    const worker = template.spec?.containers?.find(
      (container) => container.name === "worker",
    );
    check(
      template.spec?.containers?.length === 1 && Boolean(worker),
      "the worker template targets only the generated worker container",
    );
    check(
      Object.keys(template.metadata ?? {}).every((key) => key === "labels") &&
        Boolean(template.metadata?.labels?.["app.kubernetes.io/part-of"]),
      "the worker template uses only metadata labels that Sail applies",
    );
    check(
      worker?.image === undefined &&
        worker?.imagePullPolicy === undefined &&
        worker?.command === undefined &&
        worker?.args === undefined &&
        worker?.envFrom === undefined &&
        worker?.ports === undefined &&
        template.spec?.serviceAccount === undefined &&
        template.spec?.serviceAccountName === undefined &&
        template.spec?.restartPolicy === undefined,
      "the worker template does not override generated identity or process fields",
    );
    check(
      !template.spec?.initContainers?.length &&
        !template.spec?.ephemeralContainers?.length &&
        template.spec?.hostNetwork !== true &&
        template.spec?.hostPID !== true &&
        template.spec?.hostIPC !== true &&
        template.spec?.shareProcessNamespace !== true,
      "the worker template does not add privileged pod-level execution surfaces",
    );
    check(
      template.spec?.automountServiceAccountToken === false &&
        template.spec?.enableServiceLinks === false,
      "the worker template disables token mounting",
    );
    check(
      Boolean(worker?.resources?.requests?.cpu) &&
        Boolean(worker?.resources?.requests?.memory) &&
        Boolean(worker?.resources?.requests?.["ephemeral-storage"]) &&
        Boolean(worker?.resources?.limits?.cpu) &&
        Boolean(worker?.resources?.limits?.memory) &&
        Boolean(worker?.resources?.limits?.["ephemeral-storage"]),
      "the worker template has CPU, memory, and ephemeral-storage requests and limits",
    );
    const workerEnv = Object.fromEntries(
      (worker?.env ?? [])
        .filter((entry) => Object.hasOwn(entry, "value"))
        .map((entry) => [entry.name, String(entry.value)]),
    );
    check(
      !(worker?.env ?? []).some((entry) =>
        entry.name?.startsWith("SAIL_CLUSTER__"),
      ),
      "the worker template does not override generated cluster connection settings",
    );
    check(
      workerEnv.SAIL_RUNTIME__MEMORY_POOL__TYPE === "fair" &&
        isPositiveInteger(
          workerEnv.SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE,
        ) &&
        workerEnv.SAIL_RUNTIME__TEMPORARY_FILES__PATHS === '["/tmp/sail"]' &&
        isPositiveInteger(workerEnv.SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE),
      "the worker template has explicit memory-pool and temporary-file settings",
    );
    check(
      template.spec?.volumes?.some(
        (volume) => volume.name === "scratch" && volume.emptyDir?.sizeLimit,
      ) &&
        worker?.volumeMounts?.some(
          (mount) => mount.name === "scratch" && mount.mountPath === "/tmp",
        ),
      "the worker template mounts a size-limited scratch volume at /tmp",
    );
    check(
      template.spec?.securityContext?.runAsNonRoot === true &&
        template.spec?.securityContext?.runAsUser === 10001 &&
        template.spec?.securityContext?.runAsGroup === 10001 &&
        template.spec?.securityContext?.fsGroup === 10001 &&
        template.spec?.securityContext?.seccompProfile?.type ===
          "RuntimeDefault" &&
        worker?.securityContext?.privileged !== true &&
        worker?.securityContext?.allowPrivilegeEscalation === false &&
        worker?.securityContext?.readOnlyRootFilesystem === true &&
        worker?.securityContext?.capabilities?.drop?.includes("ALL"),
      "the worker template contains the intended restricted security fields",
    );
  }
}

if (settings) {
  check(
    settings.data?.image ===
      deployment?.spec?.template?.spec?.containers?.find(
        (container) => container.name === "server",
      )?.image,
    "the recorded image reference matches the deployed image",
  );
  if (requireImageDigest) {
    check(
      /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(
        settings.data?.["source-revision"] ?? "",
      ),
      "the target overlay records a full hexadecimal source revision",
    );
  }
}

if (networkPolicy) {
  const ingressRules = networkPolicy.spec?.ingress ?? [];
  const hasPort = (rule, port) =>
    rule.ports?.length === 1 &&
    Number(rule.ports[0]?.port) === port &&
    rule.ports[0]?.protocol === "TCP" &&
    rule.ports[0]?.endPort === undefined;
  const internalRule = ingressRules.some(
    (rule) =>
      !rule.ports &&
      rule.from?.length === 1 &&
      rule.from[0]?.podSelector?.matchLabels?.["app.kubernetes.io/name"] ===
        "sail" &&
      !rule.from[0]?.namespaceSelector,
  );
  const sameNamespaceClientRule = ingressRules.some(
    (rule) =>
      hasPort(rule, 50051) &&
      rule.from?.length === 1 &&
      rule.from[0]?.podSelector?.matchLabels?.[
        "sail.lakesail.com/spark-connect-client"
      ] === "true" &&
      !rule.from[0]?.namespaceSelector,
  );
  const crossNamespaceClientRule = ingressRules.some(
    (rule) =>
      hasPort(rule, 50051) &&
      rule.from?.length === 1 &&
      rule.from[0]?.podSelector?.matchLabels?.[
        "sail.lakesail.com/spark-connect-client"
      ] === "true" &&
      rule.from[0]?.namespaceSelector?.matchLabels?.[
        "sail.lakesail.com/spark-connect-access"
      ] === "true",
  );
  check(
    ingressRules.length === 3 &&
      networkPolicy.spec?.policyTypes?.length === 1 &&
      networkPolicy.spec.policyTypes[0] === "Ingress" &&
      networkPolicy.spec?.egress === undefined &&
      networkPolicy.spec?.podSelector?.matchLabels?.[
        "app.kubernetes.io/name"
      ] === "sail" &&
      internalRule &&
      sameNamespaceClientRule &&
      crossNamespaceClientRule,
    "ingress policy has the intended Sail and labeled-client selectors",
  );
}

check(
  disruptionBudget?.spec?.minAvailable === 1 &&
    disruptionBudget.spec?.selector?.matchLabels?.["app.kubernetes.io/name"] ===
      "sail" &&
    disruptionBudget.spec?.selector?.matchLabels?.[
      "app.kubernetes.io/component"
    ] === "spark-server",
  "the disruption budget selects the Spark Connect server",
);

check(
  namespace?.metadata?.name === workloadNamespace &&
    namespace?.metadata?.labels?.["pod-security.kubernetes.io/enforce"] ===
      "restricted" &&
    namespace?.metadata?.labels?.["pod-security.kubernetes.io/audit"] ===
      "restricted" &&
    namespace?.metadata?.labels?.["pod-security.kubernetes.io/warn"] ===
      "restricted",
  "the namespace requests Restricted enforcement from Pod Security Admission",
);
if (requireImageDigest) {
  const labels = namespace?.metadata?.labels ?? {};
  const versionPattern = /^v1\.\d+$/;
  check(
    versionPattern.test(
      labels["pod-security.kubernetes.io/enforce-version"] ?? "",
    ) &&
      versionPattern.test(
        labels["pod-security.kubernetes.io/audit-version"] ?? "",
      ) &&
      versionPattern.test(
        labels["pod-security.kubernetes.io/warn-version"] ?? "",
      ),
    "the target namespace pins Restricted policy versions",
  );
}

const serialized = inputPaths
  .map((inputPath) => fs.readFileSync(inputPath, "utf8"))
  .join("\n---\n");
check(
  !/(^|\s)(password|secret|token):\s*[^\s#]/im.test(serialized),
  "the rendered manifest has no inline values under YAML keys named password, secret, or token",
);

for (const message of passes) {
  console.log(`PASS ${message}`);
}

if (failures.length > 0) {
  for (const message of failures) {
    console.error(`FAIL ${message}`);
  }
  process.exit(1);
}

console.log(`PASS ${passes.length} production manifest invariants`);
