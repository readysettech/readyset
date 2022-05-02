# ReadySet

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.1](https://img.shields.io/badge/AppVersion-0.0.1-informational?style=flat-square)

Official ReadySet Server Chart

**Homepage:** <https://readyset.io/>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| ReadySet.io | support@readyset.io |  |

## Requirements

Kubernetes: `>=1.18.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://helm.releases.hashicorp.com/ | consul | 0.41.0 |
| https://prometheus-community.github.io/helm-charts | monitoring(kube-prometheus-stack) | 34.10.0 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| consul | object | Truncated due to length. | Consul Helm chart values using official HashiCorp chart. Ref: https://www.consul.io/docs/k8s/helm |
| consul.affinity | object | `{}` | Affinities to be applied to Consul server pods. |
| consul.client | object | `{"enabled":false}` | Consul chart's daemonset client. Disabled since ReadySet uses the sidecar pattern. Ref: https://www.consul.io/docs/k8s/helm#client |
| consul.client.enabled | bool | `false` | Disable Consul Agent daemonset since we're using sidecars instead. |
| consul.connectInject | object | `{"enabled":false}` | Consul Connect sidecar injection configuration settings. Ref: https://www.consul.io/docs/k8s/helm#connectinject |
| consul.connectInject.enabled | bool | `false` | Toggles Consul Connect sidecar injection. Not currently used by ReadySet. |
| consul.enabled | bool | `true` | Toggles provisioning of the official HashiCorp Consul Helm chart. |
| consul.image | string | `"consul:1.11.4"` | Consul container image and tag. |
| consul.server | object | Truncated due to length. | Consul chart's server configuration block. Ref: https://www.consul.io/docs/k8s/helm#server |
| consul.server.bootstrapExpect | int | `3` | Number of Consul servers to expect before considering the cluster ready. |
| consul.server.enabled | bool | `true` | Toggles provisioning of Consul Server. Ref: https://www.consul.io/docs/k8s/helm#server |
| consul.server.replicas | int | `3` | Number of Consul Server replicas. |
| consul.server.resources | object | `{"limits":{"cpu":"250m","memory":"512Mi"},"requests":{"cpu":"250m","memory":"256Mi"}}` | Resource requests and limits for Consul Server pods. |
| consul.server.storage | string | `"200Gi"` | Storage size for Consul Server state storage devices. |
| consul.server.storageClass | string | `"gp2"` | Storage class for Consul Server state storage devices. Ref: https://kubernetes.io/docs/concepts/storage/storage-classes/ |
| global.debugModeEnabled | bool | `false` | Toggles debug mode logging and more verbose install/upgrade output. Disables parameter checks (<fail "xxxxxxxx" errors) |
| global.fullnameOverride | string | `""` | Allows for complete control over naming prefix. Component names are suffixed to end of provided string. |
| global.imagePullPolicy | string | `"IfNotPresent"` | Pull policy to apply to all resources. |
| global.imagePullSecret | string | `""` | Image pull secret to apply to all resources. |
| global.name | string | `""` | Specifies prefix for named resources, e.g. ${name}-server, ${name}-adapter, etc. |
| global.nameOverride | string | `""` | Names resources as ${release_name}-${component_name}. |
| monitoring | object | Truncated due to length. | https://github.com/prometheus-community/helm-charts/blob/main/charts/kube-prometheus-stack/values.yaml |
| monitoring.alertmanager | object | `{"enabled":false}` | AlertManager configurations. |
| monitoring.alertmanager.enabled | bool | `false` | Toggles alertmanager chart. |
| monitoring.coreDns | object | `{"enabled":false}` | Component scraping for CoreDNS. |
| monitoring.coreDns.enabled | bool | `false` | Toggles scraping for CoreDNS. |
| monitoring.defaultRules | object | `{"create":false}` | Default rules configurations for kube-prometheus-stack. |
| monitoring.defaultRules.create | bool | `false` | Toggles default rules. |
| monitoring.enabled | bool | `true` | Global toggle for kube-prometheus-stack chart. |
| monitoring.fullnameOverride | string | `"readyset-monitor"` | Full name override for kube-prometheus-stack chart. |
| monitoring.grafana | object | Truncated due to length. | Grafana for ReadySet metrics and dashboards. Values Ref: https://github.com/grafana/helm-charts/blob/main/charts/grafana/values.yaml |
| monitoring.grafana.adminPassword | string | `"readyset-monitoring"` | Login phrase. |
| monitoring.grafana.dashboardProviders | object | Truncated due to length. | ReadySet Grafana dashboard providers. |
| monitoring.grafana.dashboards | object | Truncated due to length. | Grafana dashboard configs. @TODO: Move dashboards into a separate ConfigMap. |
| monitoring.grafana.dashboards.readyset | object | Truncated due to length. | ReadySet dashboards. |
| monitoring.grafana.dashboards.readyset.connected | object | Truncated due to length. | ReadySet Connectivity Overview dashboard |
| monitoring.grafana.dashboards.readyset.queryOverview | object | Truncated due to length. | ReadySet Query Overview Dashboard |
| monitoring.grafana.dashboards.readyset.querySpecific | object | Truncated due to length. | ReadySet Query Specific Dashboard |
| monitoring.grafana.dashboards.readyset.readysetExternal | object | Truncated due to length. | ReadySet External Dashboard |
| monitoring.grafana.dashboards.readyset.systemsOverview | object | Truncated due to length. | Systems Overview dashboard |
| monitoring.grafana.enabled | bool | `true` | Toggles deployment of Grafana chart. |
| monitoring.grafana.forceDeployDatasources | bool | `true` | Force redeploy datasources. |
| monitoring.grafana.ingress | object | Truncated due to length. | Ingress configurations for accessing Grafana. |
| monitoring.grafana.sidecar | object | `{"datasources":{"uid":"DS_PROMETHEUS","url":"http://readyset-monitor-prometheus:9090/"}}` | Grafana sidecar configurations. |
| monitoring.grafana.sidecar.datasources | object | `{"uid":"DS_PROMETHEUS","url":"http://readyset-monitor-prometheus:9090/"}` | Data source config for Prometheus. This is the datasource used by ReadySet dashboards, by default. |
| monitoring.kube-state-metrics | object | `{"enabled":false}` | Configuration for kube-state-metrics subchart. |
| monitoring.kube-state-metrics.enabled | bool | `false` | Not needed in this case. |
| monitoring.kubeApiServer | object | `{"enabled":false}` | Component scraping for the Kubernetes API server. |
| monitoring.kubeApiServer.enabled | bool | `false` | Toggles scraping for Kubernetes API server. |
| monitoring.kubeControllerManager | object | `{"enabled":false}` | Component scraping for the Kube Controller Manager. |
| monitoring.kubeControllerManager.enabled | bool | `false` | Toggles scraping of Kube Controller Manager. |
| monitoring.kubeEtcd | object | `{"enabled":false}` | Component scraping for etcd. |
| monitoring.kubeEtcd.enabled | bool | `false` | Toggles scraping for etcd. |
| monitoring.kubeProxy | object | `{"enabled":false}` | Component scraping for kube proxy. |
| monitoring.kubeProxy.enabled | bool | `false` | Toggles scraping for kube proxy. |
| monitoring.kubeScheduler | object | `{"enabled":false}` | Component scraping for kube scheduler. |
| monitoring.kubeScheduler.enabled | bool | `false` | Toggles scraping for kube scheduler. |
| monitoring.kubeStateMetrics | object | `{"enabled":false}` | Component scraping for kube state metrics. |
| monitoring.kubeStateMetrics.enabled | bool | `false` | Toggles scraping of kube-state-metrics. |
| monitoring.kubelet | object | `{"enabled":false}` | Component scraping for Kubelet. |
| monitoring.kubelet.enabled | bool | `false` | Toggles scraping for Kubelet. |
| monitoring.nodeExporter | object | `{"enabled":true}` | Node exporter can be disabled if daemonset already running on all nodes. |
| monitoring.nodeExporter.enabled | bool | `true` | Toggles node exporter DaemonSet |
| monitoring.prometheus | object | Truncated due to length. | Prometheus subchart configurations. |
| monitoring.prometheus.enabled | bool | `true` | Toggles deployment of Prometheus. Can be disabled if hosting your own Prometheus. |
| monitoring.prometheus.prometheusSpec | object | Truncated due to length. | Prometheus specifications for Operator consumption. Ref: https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/api.md#prometheusspec |
| monitoring.prometheus.prometheusSpec.additionalScrapeConfigs | list | Truncated due to length. | Prometheus scrape configurations for gathering metrics. |
| monitoring.prometheus.prometheusSpec.affinity | object | `{}` | Affinities for Prometheus pods. |
| monitoring.prometheus.prometheusSpec.externalUrl | string | `"http://readyset-monitor-prometheus:9090"` | your choosing. |
| monitoring.prometheus.prometheusSpec.ingress | object | `{"enabled":false}` | Ingress deployment specifications for Prometheus. |
| monitoring.prometheus.prometheusSpec.ingress.enabled | bool | `false` | Generally not needed for ReadySet. |
| monitoring.prometheus.prometheusSpec.resources | object | `{"requests":{"cpu":"500m","memory":"1024Mi"}}` | Resource requests and limits for the Prometheus server. |
| monitoring.prometheus.prometheusSpec.resources.requests | object | `{"cpu":"500m","memory":"1024Mi"}` | Resource requests for Prometheus. |
| monitoring.prometheus.prometheusSpec.retention | string | `"14d"` | Retention period for metrics shipped into Prometheus. |
| monitoring.prometheus.prometheusSpec.storageSpec | object | Truncated due to length. | Storage specification for Prometheus pods. |
| monitoring.prometheus.prometheusSpec.storageSpec.volumeClaimTemplate | object | Truncated due to length. | Volume claim template for Prometheus pods. |
| monitoring.prometheus.prometheusSpec.tolerations | list | `[]` | Tolerations for Prometheus workloads. |
| monitoring.prometheusOperator | object | `{"enabled":true}` | Prometheus Operation configurations for kube-prometheus-stack. |
| monitoring.prometheusOperator.enabled | bool | `true` | Toggles PrometheusOperator component of kube-prometheus-stack. |
| readyset.adapter | object | Truncated due to length. | Applications connect to it instead of the RDS instance directly. |
| readyset.adapter.affinity | object | `{}` | Affinities to be applied to ReadySet adapter pods. Ref: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/ |
| readyset.adapter.args | list | `["--prometheus-metrics"]` | Entrypoint arguments for ReadySet adapter containers. |
| readyset.adapter.componentName | string | `"readyset-adapter"` | Component name of adapter. Used to name logs and metrics. |
| readyset.adapter.containerPorts | list | Truncated due to length. | Container ports exposed for ReadySet adapter. |
| readyset.adapter.enabled | bool | `true` | Toggles deployment of ReadySet adapter deployment. |
| readyset.adapter.entrypoint | string | `"/usr/local/bin/docker-entrypoint.sh"` | Entrypoint for ReadySet adapter containers. |
| readyset.adapter.extraEnvironmentVars | object | Truncated due to length. | Static environment variables applied to ReadySet adapter containers. |
| readyset.adapter.extraEnvironmentVars.EXPLICIT_MIGRATIONS | int | `0` | statements. Set to 0 (off) or 1 (on). Conflicts with ASYNC_MIGRATIONS. |
| readyset.adapter.extraEnvironmentVars.METRICS_ADDRESS | string | `"0.0.0.0:6034"` | Listening address and port for health check and metric endpoints. |
| readyset.adapter.extraEnvironmentVars.OUTPUTS_POLLING_INTERVAL | int | `180` | Polling interval in seconds for requesting outputs from the Leader. |
| readyset.adapter.extraEnvironmentVars.PROMETHEUS_METRICS | int | `1` | Toggles prometheus metrics for ReadySet adapter containers. |
| readyset.adapter.extraEnvironmentVars.QUERY_LOG | int | `1` | Toggles query logging. Set to 0 (off) or 1 (on). |
| readyset.adapter.image | object | Truncated due to length. | Container image settings for ReadySet adapter containers. |
| readyset.adapter.image.pullPolicy | string | `""` | Image pull policy to be applied to ReadySet adapter pods. |
| readyset.adapter.image.repository | string | `"305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-adapter"` | Image repository to use for ReadySet adapter containers. |
| readyset.adapter.image.tag | string | `"latest"` | Image tag to use for ReadySet adapter containers. |
| readyset.adapter.labels | object | `{"app":"readyset","component":"adapter"}` | Pod labels for ReadySet adapter pods. |
| readyset.adapter.prometheusScrapePort | int | `6034` | TCP port the ReadySet Prometheus scrape endpoint is listening on. |
| readyset.adapter.rbac | object | Truncated due to length. | RBAC configurations for ReadySet adapter. |
| readyset.adapter.rbac.role | object | `{"bindingName":"","create":true,"name":"","namespace":""}` | Role and role binding configurations for ReadySet adapter. |
| readyset.adapter.rbac.role.bindingName | string | `""` | Name of chart created role binding. Only used if .create is true. |
| readyset.adapter.rbac.role.create | bool | `true` | Toggles creation of ReadySet adapter role. |
| readyset.adapter.rbac.role.name | string | `""` | Name of chart created role for ReadySet adapter. |
| readyset.adapter.rbac.role.namespace | string | `""` | Namespace override for ReadySet adapter role. Defaults to the namespace ReadySet is deployed in. |
| readyset.adapter.rbac.serviceAccount | object | `{"create":true,"name":""}` | Service account configurations for ReadySet adapter. |
| readyset.adapter.rbac.serviceAccount.create | bool | `true` | Toggles creation of ReadySet adapter service account. |
| readyset.adapter.rbac.serviceAccount.name | string | `""` | Name of service account. Allows for bringing your own service account if .create is false. |
| readyset.adapter.replicas | int | `3` | Number of replicas for ReadySet adapter deployments. |
| readyset.adapter.resources | object | `{"requests":{"cpu":"2000m","memory":"4096Mi"}}` | Resource requests and limits for ReadySet adapters. |
| readyset.adapter.securityContext | object | `{"runAsGroup":1000,"runAsUser":1000}` | Security context to be applied to ReadySet adapter containers. Defaults to uid 1000 and gid 1000. Ref: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/ |
| readyset.adapter.service | object | Truncated due to length. | ReadySet adapter k8s service configurations. |
| readyset.adapter.service.annotations | object | `{"service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled":"true","service.beta.kubernetes.io/aws-load-balancer-internal":"true","service.beta.kubernetes.io/aws-load-balancer-ip-address-type":"ipv4","service.beta.kubernetes.io/aws-load-balancer-type":"nlb-ip"}` | Annotations applied to Readyset adapter k8s service. |
| readyset.adapter.service.create | bool | `true` | Toggles provisioning of ReadySet adapter k8s service. |
| readyset.adapter.service.labels | object | `{"app":"readyset","component":"adapter"}` | Labels applied to ReadySet adapter k8s service. |
| readyset.adapter.service.listeners | list | Truncated due to length. | Listener to be created on ReadySet adapter k8s service |
| readyset.adapter.service.nameOverride | string | `""` | Name of service to create or use (if create is false). |
| readyset.adapter.service.type | string | LoadBalancer | Service type applied to ReadySet adapter k8s service. type: ClusterIP - If internal k8s access is all that's needed |
| readyset.adapter.tolerations | list | `[]` | Tolerations to be applied to ReadySet adapter pods. Ref: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| readyset.adapter.topologySpreadConstraints | object | `{}` | Topology spread constraints to apply to the ReadySet adapter pods. Ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/ |
| readyset.common.config | object | Truncated due to length. | Shared/common configuration block for ReadySet applications. |
| readyset.common.config.aggregatorAddressOverride | string | `""` | Log and metric aggregator address for Vector agents to ship to. Default behavior routes metrics to the chart generated Vector aggregator service name. |
| readyset.common.config.authorityAddressOverride | string | `""` | Consul authority address override. Empty value uses chart generated value. |
| readyset.common.config.authorityType | string | `"consul"` | Authority type. Currently only supports Consul. |
| readyset.common.config.cwLogGroupName | string | `"readyset-webapp-20"` | Name of the CloudWatch log group that will contain ReadySet application logs. |
| readyset.common.config.deploymentName | string | `"webapp-15"` | Name of the ReadySet deployment. Should be unique within the context of the chosen Consul cluster to avoid key collisions. |
| readyset.common.config.engine | string | `"mysql"` | Flag to instruct entrypoint script which adapter binary to use. Supported values: mysql, psql |
| readyset.common.config.logFormat | string | `"json"` | Format for ReadySet logs to be emitted to STDOUT/STDERR. |
| readyset.common.config.memoryLimitBytes | int | `0` | Maximum memory (in bytes) each ReadySet server can consume. Unlimited by default, as indicated by a value of 0. |
| readyset.common.config.primaryRegion | string | `"us-east-2"` | The region where the ReadySet controller is hosted. |
| readyset.common.config.quorum | int | `1` | Number of ReadySet server nodes in the cluster. Should match number of server replicas in deploymentConfig.replicas |
| readyset.common.config.region | string | `"us-east-2"` | Required to route view requests to specific regions. |
| readyset.common.config.shards | int | `0` | Number of data shards in the cluster. Not advised to be changed unless you know what you are doing. |
| readyset.common.secrets | object | Truncated due to length. | Secrets shared amongst ReadySet containers deployed in this chart. |
| readyset.common.secrets.replicationUrl | object | `{"pwdSecretKey":"password","secretName":"readyset-db-url","urlSecretKey":"url","userSecretKey":"username"}` | your RDS instance. |
| readyset.common.secrets.replicationUrl.pwdSecretKey | string | `"password"` | Key in the k8s secret for the DB password. |
| readyset.common.secrets.replicationUrl.secretName | string | `"readyset-db-url"` | Name of k8s secret to retrieve DB connection values from. |
| readyset.common.secrets.replicationUrl.urlSecretKey | string | `"url"` | Key in the k8s secret for the DB url value. |
| readyset.common.secrets.replicationUrl.userSecretKey | string | `"username"` | Key in the k8s secret for the DB username. |
| readyset.common.secrets.vectorIamCredentials | object | Truncated due to length. | Kubernetes secret containing IAM credentials for Vector to ship metrics and logs to CloudWatch. If using IRSA and the appropriate annotations, set secretName to a blank string. |
| readyset.consulAgent | object | Truncated due to length. | Consul agent sidecar configurations for ReadySet. |
| readyset.consulAgent.args | list | `["/usr/src/app/entrypoint.sh"]` | Consul agent entrypoint arguments. |
| readyset.consulAgent.containerPorts | list | Truncated due to length. | Container ports to be exposed for the Consul agent container. |
| readyset.consulAgent.entrypoint | string | `"/bin/sh"` | Consul agent entrypoint. |
| readyset.consulAgent.image | string | `"hashicorp/consul"` | Container image repository used for Consul agent sidecars. |
| readyset.consulAgent.labels | object | `{}` | Extra labels applied to Consul agent sidecars. |
| readyset.consulAgent.serverNamespaceOverride | string | `""` | Blank value causes chart behavior to default to current namespace. |
| readyset.consulAgent.tag | string | `"1.11.4"` | Container image tag used for Consul agent sidecars. |
| readyset.server | object | Truncated due to length. | nodes directly. Applications connect to the adapter service. |
| readyset.server.affinity | object | `{}` | Affinities to be applied to ReadySet server pods. Ref: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/ |
| readyset.server.annotations | object | `{}` | Extra annotations applied to ReadySet server pods. |
| readyset.server.args | list | `[]` | Args for ReadySet server container entrypoint. |
| readyset.server.componentName | string | `"readyset-server"` | Component name of ReadySet server. Used for logging and metrics. |
| readyset.server.containerPorts | list | Truncated due to length. | Container ports exposed for ReadySet server. |
| readyset.server.deploymentConfig | object | Truncated due to length. | Deployment configuration options for ReadySet server. |
| readyset.server.deploymentConfig.podManagementPolicy | string | `"Parallel"` | Pod management policy to be used. |
| readyset.server.deploymentConfig.replicas | int | `1` | Number of replicas of ReadySet server to be run. Under normal conditions this should match common.config.quorum. Should be: 1, 3, 5, 7 |
| readyset.server.deploymentConfig.updatePartition | int | `0` | Partition size when performing an update. |
| readyset.server.deploymentConfig.updateStrategyType | string | `"RollingUpdate"` | Update strategy type to use for deployments. |
| readyset.server.enabled | bool | `true` | Toggles creation of ReadySet server components. |
| readyset.server.entrypoint | string | `"/usr/local/bin/readyset-server"` | Entrypoint for ReadySet server containers. |
| readyset.server.extraEnvironmentVars | object | Truncated due to length. | Static environment variables to be applied to ReadySet server containers. |
| readyset.server.extraEnvironmentVars.DB_DIR | string | `"/state"` | Directory to read and write ReadySet datastore to. This path should be on a persistent storage device, e.g. ebs |
| readyset.server.extraEnvironmentVars.FORBID_FULL_MATERIALIZATION | string | `"1"` | Controls materialization. |
| readyset.server.extraEnvironmentVars.LISTEN_ADDRESS | string | `"0.0.0.0"` | Address ranges permitted to connect to ReadySet server. |
| readyset.server.extraEnvironmentVars.MEMORY_CHECK_EVERY | string | `"1"` | Seconds between state size memory limit checks. |
| readyset.server.extraEnvironmentVars.NORIA_MEMORY_BYTES | string | `"0"` | Memory limit (in bytes) for ReadySet server. A value of 0 (default) indicates unlimited memory usage. |
| readyset.server.extraEnvironmentVars.PROMETHEUS_METRICS | string | `"true"` | Toggles Prometheus metric endpoint. Required for Grafana dashboards and metrics. |
| readyset.server.extraLabels | object | `{"app":"readyset","component":"server"}` | Extra labels applied to ReadySet server pods. |
| readyset.server.image | object | Truncated due to length. | Container image settings for ReadySet server. |
| readyset.server.image.repository | string | `"305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-server"` | Container image repository to use for ReadySet server. |
| readyset.server.image.tag | string | `"release-latest"` | Container image tag to use for ReadySet server. |
| readyset.server.priorityClassName | string | `""` | Priority class to be applied to ReadySet server pods. |
| readyset.server.prometheusScrapePort | int | `6033` | TCP port the ReadySet Prometheus scrape endpoint is listening on. |
| readyset.server.rbac | object | Truncated due to length. | RBAC configurations for ReadySet server. |
| readyset.server.rbac.role | object | Truncated due to length. | Role and role binding configurations for ReadySet server. |
| readyset.server.rbac.role.bindingName | string | `""` | If create enabled, uses either the value below or a chart generated name. |
| readyset.server.rbac.role.create | bool | `true` | Toggles creation of role and role binding for ReadySet server. |
| readyset.server.rbac.role.name | string | `""` | If create enabled, uses either the value below or a chart generated name. |
| readyset.server.rbac.role.namespace | string | `""` | Defaults to namespace ReadySet is deployed in. Useful if Consul is running in another namespace |
| readyset.server.rbac.serviceAccount | object | Truncated due to length. | Service account configurations for ReadySet server. |
| readyset.server.rbac.serviceAccount.create | bool | `true` | Toggles creation of ReadySet server service account. |
| readyset.server.rbac.serviceAccount.name | string | `""` | Name of service account for ReadySet server.  Allows for bringing your own service account if .create is false. |
| readyset.server.resources | object | `{"limits":{},"requests":{"cpu":"2000m","memory":"4096Mi"}}` | Defaults to development settings. |
| readyset.server.securityContext | object | `{}` | Security context to be applied to ReadySet server pods. Ref: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/ |
| readyset.server.storageSpec | object | Truncated due to length. | Storage spec for ReadySet server's persistent state. |
| readyset.server.storageSpec.persistentStorageEnabled | bool | `true` | Toggles usage of persistent storage. ReadySet is a database, so persistence is required for production. |
| readyset.server.storageSpec.volumeClaimTemplates | list | Truncated due to length. | Volume claim template for ReadySet server. |
| readyset.server.storageSpec.volumeMounts | list | `[{"mountPath":"/state","name":"state"}]` | Volume mounts for ReadySet server. |
| readyset.server.storageSpec.volumeName | string | `"state"` | Name of volume for ReadySet server state storage. |
| readyset.server.termGracePeriodSec | int | `15` | Seconds to wait before terminating ReadySet server pods. Ref: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/ |
| readyset.server.tolerations | list | `[]` | Tolerations to be applied to ReadySet server pods. Ref: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| readyset.server.topologySpreadConstraints | object | `{}` | Topology spread constraints to apply to the ReadySet server pods. Ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/ |
| readyset.vector | object | Truncated due to length. | Vector agent and aggregator configurations. Ref for Vector: https://vector.dev/ |
| readyset.vector.aggregator | object | Truncated due to length. | Vector aggregator deployment. Used to ship metrics and logs. |
| readyset.vector.aggregator.affinity | object | `{}` | Affinities to be applied to Vector aggregator. |
| readyset.vector.aggregator.annotations | object | `{}` | Annotations to be applied to Vector aggregator resources. |
| readyset.vector.aggregator.args | list | `["--config-dir","/etc/vector/"]` | Entrypoint arguments for Vector aggregator containers. |
| readyset.vector.aggregator.command | list | `[]` | Entrypoint command for Vector aggregator containers. |
| readyset.vector.aggregator.configMaps | object | Truncated due to length. | ConfigMaps for the Vector aggregator deployment. |
| readyset.vector.aggregator.configMaps.config | object | `{"create":true,"nameOverride":""}` | Primary configurations CM for Vector aggregator. |
| readyset.vector.aggregator.configMaps.config.create | bool | `true` | Toggles creation of the Vector aggregator config CM. |
| readyset.vector.aggregator.configMaps.config.nameOverride | string | `""` | Overrides name of configmap created or used (if create = false). If empty, chart will generate a name for the ConfigMap. |
| readyset.vector.aggregator.containerPorts | list | Truncated due to length. | Ports to be exposed on Vector aggregator containers. |
| readyset.vector.aggregator.dnsConfig | object | `{}` | DNS configuration options for Vector aggregator pods. Ref: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-dns-config |
| readyset.vector.aggregator.dnsPolicy | string | `"ClusterFirst"` | DNS policy for Vector aggregator pods. Ref: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy |
| readyset.vector.aggregator.enabled | bool | `true` | Toggles deployment of Vector aggregator. |
| readyset.vector.aggregator.env | list | `[]` | Supplemental environment variables to apply to Vector aggregator. |
| readyset.vector.aggregator.extraVolumeMounts | list | `[]` | Extra volume mounts to apply to Vector aggregator pods. |
| readyset.vector.aggregator.extraVolumes | list | `[]` | Additional volumes to use with Vector aggregator pods. |
| readyset.vector.aggregator.image | object | Truncated due to length. | Container image configurations for Vector aggregator. |
| readyset.vector.aggregator.image.pullPolicy | string | `"IfNotPresent"` | Image pull policy to apply to Vector aggregator pods. |
| readyset.vector.aggregator.image.pullSecrets | list | `[]` | Image pull secrets to be used when pulling Vector aggregator container images. |
| readyset.vector.aggregator.image.repository | string | `"timberio/vector"` | Container image repository for Vector aggregator containers. |
| readyset.vector.aggregator.image.tag | string | `"0.20.0-distroless-libc"` | Container image tag for Vector aggregator containers. |
| readyset.vector.aggregator.initContainers | list | `[]` | Any init containers to apply to Vector aggregator pods. |
| readyset.vector.aggregator.labels | object | Truncated due to length. | Labels to be applied to Vector aggregator resources. |
| readyset.vector.aggregator.livenessProbe | object | Truncated due to length. | Liveness probe configuration for Vector aggregator pods. |
| readyset.vector.aggregator.nameOverride | string | `""` | Name override for Vector aggregator deployment. |
| readyset.vector.aggregator.nodeExporterEndpoint | string | `"http://${NODE_IP}:9100/metrics"` | URL to the node's Prometheus node-exporter metrics endpoint. |
| readyset.vector.aggregator.nodeExporterScrapeInterval | int | `15` | Scrape interval applied when scraping node-exporter. |
| readyset.vector.aggregator.nodeSelector | object | `{}` | Node selector for Vector aggregator pods. |
| readyset.vector.aggregator.persistence | object | Truncated due to length. | Persistent storage configurations for Vector aggregator deployment. |
| readyset.vector.aggregator.persistence.accessModes | list | `["ReadWriteOnce"]` | AccessModes for Vector aggregator PersistentVolumeClaims |
| readyset.vector.aggregator.persistence.enabled | bool | `false` | Toggles usage of PVC. If true, create and use PersistentVolumeClaims |
| readyset.vector.aggregator.persistence.existingClaim | string | `""` | Name of an existing PersistentVolumeClaim to use |
| readyset.vector.aggregator.persistence.finalizers | list | `[]` | Finalizers applied to PersistentVolumeClaims for Vector aggregator. |
| readyset.vector.aggregator.persistence.hostPath | object | `{"path":"/var/lib/vector"}` | HostPath configurations for Vector aggregator's persistent storage |
| readyset.vector.aggregator.persistence.selectors | object | `{}` | Selectors for PersistentVolumeClaims |
| readyset.vector.aggregator.persistence.size | string | `"10Gi"` | Size of PersistentVolumeClaim storage device for Vector aggregator. |
| readyset.vector.aggregator.podPriorityClassName | string | `""` | Set the priorityClassName on Vector aggregator pods. |
| readyset.vector.aggregator.podSecurityContext | object | `{}` | Vector aggregator pod securityContext customizations to apply. |
| readyset.vector.aggregator.readinessProbe | object | Truncated due to length. | Readiness probe configuration for Vector aggregator pods. Ref: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/ |
| readyset.vector.aggregator.replicas | int | `1` | Number of Vector aggregator replicas. |
| readyset.vector.aggregator.resources | object | `{"limits":{"cpu":"1000m","memory":"4096Mi"},"requests":{"cpu":"200m","memory":"256Mi"}}` | Resource requests and limits for the Vector aggregator. |
| readyset.vector.aggregator.rollWorkloadOnConfigChanges | bool | `true` | Triggers rollout restart of Vector aggregator when changes are made to configMap(s). |
| readyset.vector.aggregator.securityContext | object | `{}` | Security Context for Vector aggregator containers. |
| readyset.vector.aggregator.service | object | Truncated due to length. | K8s service configurations for Vector aggregator. |
| readyset.vector.aggregator.service.annotations | object | `{}` | Set annotations on Vector aggregator service. |
| readyset.vector.aggregator.service.enabled | bool | `true` | Toggles deployment of chart created k8s service. |
| readyset.vector.aggregator.service.labels | object | Truncated due to length. | Labels to be applied to the Vector aggregator k8s service created by the chart. Only used if enabled = true. |
| readyset.vector.aggregator.service.nameOverride | string | `"vector-aggregator"` | Name of the service to be created or used. |
| readyset.vector.aggregator.service.ports | list | Truncated due to length. | Ports to be exposed for the Vector aggregator. |
| readyset.vector.aggregator.service.topologyKeys | list | `[]` | TopologyKeys to be applied to Vector aggregator service. Ref: https://kubernetes.io/docs/concepts/services-networking/service-topology/#using-service-topology |
| readyset.vector.aggregator.service.type | string | `"ClusterIP"` | Type of service to be created for the Vector aggregator. |
| readyset.vector.aggregator.serviceAccount | object | Truncated due to length. | Service account for Vector aggregator. |
| readyset.vector.aggregator.serviceAccount.annotations | object | `{}` | Additional annotations to apply to Vector aggregator service account. |
| readyset.vector.aggregator.serviceAccount.automountToken | bool | `true` | Toggles auto-mounting of service account token. |
| readyset.vector.aggregator.serviceAccount.create | bool | `true` | Toggles creation of Vector aggregator service account. |
| readyset.vector.aggregator.serviceAccount.nameOverride | string | `""` | Name of the service account to use. |
| readyset.vector.aggregator.terminationGracePeriodSeconds | int | `60` | Termination grace period seconds for Vector aggregator pods. |
| readyset.vector.aggregator.tolerations | list | `[]` | Allow Vector to schedule on tainted nodes |
| readyset.vector.aggregator.updateStrategy | object | `{}` | UpdateStrategy used when performing Vector aggregator deployment updates. Ref: https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/deployment-v1/ |
| readyset.vector.aggregator.vectorApiListener | string | `"0.0.0.0:8686"` | Vector API listener address and port. |
| readyset.vector.aggregator.vectorDataDir | string | `"/vector-agg-data-dir"` | Directory for Vector aggregator to store its' state. |
| readyset.vector.aggregator.vectorSourceListener | string | `"0.0.0.0:9000"` | Vector aggregator listener address for inbound logs and metrics. |
| readyset.vector.sidecar | object | Truncated due to length. | Vector agent sidecar configurations. Used to consume logs and metrics from ReadySet containers. |
| readyset.vector.sidecar.configMaps | object | Truncated due to length. | ConfigMaps pertaining to vector agent sidecars. Generally used to ship logs & metrics from ReadySet server/adapter containers to CloudWatch and the aggregator service. |
| readyset.vector.sidecar.configMaps.config | object | `{"create":true,"nameOverride":""}` | See templates/00-vector-agent-cm.yml |
| readyset.vector.sidecar.configMaps.config.create | bool | `true` | Toggles creation of configmap. If false, nameOverride CM will be mounted into Vector agents instead. All files in the CM are mounted. |
| readyset.vector.sidecar.configMaps.config.nameOverride | string | `""` | Allows for either overriding the name of the chart created CM or, if create is false, specifies the already existing CM to apply. |
| readyset.vector.sidecar.enabled | bool | `true` | Required for logging and metrics. |
| readyset.vector.sidecar.excludeLogs | list | `["/var/log/containers/*vector-agent-*","/var/log/containers/*kube-system*"]` | List of file exclusion patterns for Vector agent log shipping. |
| readyset.vector.sidecar.image | object | Truncated due to length. | Container image configurations for Vector agent containers. |
| readyset.vector.sidecar.image.pullPolicy | string | `"IfNotPresent"` | Image pull policy for Vector agent containers. |
| readyset.vector.sidecar.image.repository | string | `"timberio/vector"` | Container image repository for Vector agent containers. |
| readyset.vector.sidecar.image.tag | string | `"0.20.0-distroless-libc"` | Container image tag for Vector agent containers. |
| readyset.vector.sidecar.includeLogs | list | `["/var/log/containers/*-readyset*.log"]` | List of file inclusion patterns for Vector agent log shipping. |
| readyset.vector.sidecar.labels | object | Truncated due to length. | Labels applied to Vector agent components e.g. configMaps |
| readyset.vector.sidecar.vectorDataDir | string | `"/vector-data-dir"` | Location in the Vector agent pod to store vector agent data. |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.7.0](https://github.com/norwoodj/helm-docs/releases/v1.7.0)
