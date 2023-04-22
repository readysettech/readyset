{{/*
Container image name and tag for any Readyset server containers used within the chart.
*/}}
{{- define "readyset.server.image" -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}


{{/*
ReadySet server StatefulSet template.
*/}}
{{- define "readyset.server.sts" -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ template "readyset.fullname" .root }}-server
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "readyset.chart.labels" (dict "releaseName" $.Release.Name "componentName" "server") | nindent 4 }}
    {{- if .server.extraLabels }}
      {{- toYaml .server.extraLabels | nindent 4 }}
    {{- end }}
spec:
  serviceName: {{ template "readyset.fullname" .root }}-server
  podManagementPolicy: {{ .server.deploymentConfig.podManagementPolicy }}
  replicas: {{ .server.deploymentConfig.replicas }}
  {{- if (gt (int .server.deploymentConfig.updatePartition) 0) }}
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      partition: {{ .server.deploymentConfig.updatePartition }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "readyset.chart.labels" (dict "releaseName" $.Release.Name "componentName" "server") | nindent 6 }}
  {{- template "readyset.generic.volumeClaimTemplates" .server }}
  template:
    metadata:
      labels:
        {{- include "readyset.chart.labels" (dict "releaseName" $.Release.Name "componentName" "server") | nindent 8 }}
        {{- if .server.extraLabels }}
          {{- toYaml .server.extraLabels | nindent 8 }}
        {{- end }}
        {{- if .server.annotations }}
      annotations:
          {{- toYaml .server.annotations | nindent 8 }}
        {{- end }}
    spec:
    {{- if .server.affinity }}
      affinity:
      {{- toYaml .server.affinity | nindent 8 -}}
    {{- end }}
    {{- if .server.tolerations }}
      tolerations:
      {{- toYaml .server.tolerations | nindent 8 -}}
    {{- end }}
    {{- if .server.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{ toYaml .server.topologySpreadConstraints | indent 8 }}
    {{- end }}
      terminationGracePeriodSeconds: {{ .server.termGracePeriodSec }}
      serviceAccountName: {{ template "readyset.server.serviceAccountName" . }}
      {{- if .server.securityContext }}
      securityContext:
        {{- toYaml .server.securityContext | nindent 8 }}
      {{- end }}
      {{- if .server.priorityClassName }}
      priorityClassName: {{ .server.priorityClassName | quote }}
      {{- end }}
      containers:
        {{- include "readyset.consul.agent.container" (dict "consul" .Values.readyset.consulAgent "Release" .Release) | nindent 8 }}
        - name: readyset-server
          image: {{ template "readyset.server.image" (dict "image" .server.image ) }}
          command: [
            {{ .server.entrypoint | quote }}
          ]
          {{ if .server.args }}
          args:
          {{- range .server.args }}
            - {{ . | quote }}
          {{- end }}
          {{- end -}}
          env:
          {{- include "readyset.server.dynamicEnvironmentVars" (dict "config" .Values.readyset.common.config "secrets" .Values.readyset.server.secrets "csecrets" .Values.readyset.common.secrets) | nindent 12 -}}
          {{- include "readyset.generic.authority.environmentVariables" . | nindent 12 -}}
          {{- include "readyset.generic.extraEnvironmentVars" (dict "extraEnv" .Values.readyset.server.extraEnvironmentVars) | nindent 12 -}}
          {{- include "readyset.server.volumeMounts" (dict "storageSpec" .Values.readyset.server.storageSpec "extraEnv" .Values.readyset.server.extraEnvironmentVars) | nindent 10 -}}
          {{- include "readyset.generic.resources" (dict "resources" .Values.readyset.server.resources ) | nindent 10 }}
          {{- include "readyset.generic.containerPorts" (dict "ports" .Values.readyset.server.containerPorts ) | nindent 10 }}
          {{- include "readyset.server.readiness" . | nindent 10 }}
          {{- include "readyset.server.volumes" . | nindent 6 }}
{{- end -}}

{{/*
Readyset server service account configuration logic.
*/}}
{{- define "readyset.server.serviceAccountName" -}}
{{- if $.Values.readyset.server.rbac.serviceAccount.name -}}
{{- printf "%s" $.Values.readyset.server.rbac.serviceAccount.name -}}
{{- else -}}
{{- printf "%s-server" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
Readyset server SA's role configuration logic.
*/}}
{{- define "readyset.server.role.name" -}}
{{- if $.Values.readyset.server.rbac.role.name -}}
{{- printf "%s" $.Values.readyset.server.rbac.role.name -}}
{{- else -}}
{{- printf "%s-server" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
Readyset server SA's rolebinding configuration logic.
*/}}
{{- define "readyset.server.role.bindingName" -}}
{{- if $.Values.readyset.server.rbac.role.bindingName -}}
{{- printf "%s" $.Values.readyset.server.rbac.role.bindingName -}}
{{- else -}}
{{- printf "%s-server" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
ReadySet server RBAC configurations.
*/}}
{{- define "readyset.server.rbac" -}}
{{ if .rbac.serviceAccount.create }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "readyset.server.serviceAccountName" . }}
  namespace: {{ .Release.Namespace }}
automountServiceAccountToken: true
{{- if .global.imagePullSecret }}
imagePullSecrets:
- name: {{ .global.imagePullSecret }}
{{- end -}}

{{ if .rbac.role.create }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "readyset.server.role.name" . }}
  namespace: {{ .rbac.role.namespace | default .Release.Namespace }}
rules:
  # Required for k8s consul retry-join
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
{{- end }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "readyset.server.role.bindingName" . }}
  namespace: {{ .rbac.role.namespace | default .Release.Namespace }}
subjects:
- kind: ServiceAccount
  name: {{ include "readyset.server.serviceAccountName" . }}
  namespace: {{ .Release.Namespace }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "readyset.server.role.name" . }}
{{- end -}}
{{- end -}}

{{/*
ReadySet server statefulset readiness probe
*/}}
{{- define "readyset.server.readiness" -}}
readinessProbe:
  exec:
    command:
    - /bin/sh
    - -ec
    - |
      curl --fail http://127.0.0.1:6033/metrics
{{- end -}}

{{/*
ReadySet server liveness probe.
*/}}
{{- define "readyset.server.liveness" -}}
livenessProbe:
  exec:
    command:
    - /bin/sh
    - -ec
    - |
      curl --fail http://127.0.0.1:6033/metrics
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 2
{{- end -}}

{{/*
ReadySet server, main configuration envionment variables.
*/}}
{{- define "readyset.server.dynamicEnvironmentVars" -}}
# ReadySet Deployment Name (Unique per Consul Cluster)
- name: DEPLOYMENT
  value: {{ template "readyset.generic.deploymentName" .config.deploymentName | quote }}
# ReadySet Server Configs
- name: NORIA_QUORUM
  value: {{ .config.quorum | quote }}
- name: NORIA_SHARDS
  value: {{ .config.shards | quote }}
- name: NORIA_MEMORY_BYTES
  value: {{ .config.memoryLimitBytes | quote }}
# Await Consul Leader Election Before Launching
- name: INIT_REQUIRE_LEADER
  value: "1"
- name: LOG_LEVEL
  value: "info,readyset_client=trace,readyset_server=trace"
# Database ReadySet is proxying to
- name: UPSTREAM_DB_URL
  valueFrom:
    secretKeyRef:
      name: {{ .csecrets.replicationUrl.secretName }}
      key: {{ .csecrets.replicationUrl.urlSecretKey }}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: VOLUME_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: EXTERNAL_ADDRESS
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: LOG_FORMAT
  value: {{ .config.logFormat | quote }}
# Identify this as a Helm deployment to TelemetryReporter
- name: DEPLOYMENT_ENV
  value: {{ .config.deploymentEnv | quote }}
{{- end -}}

{{/*
ReadySet server volume mounts
*/}}
{{- define "readyset.server.volumeMounts" -}}
volumeMounts:
{{- if .storageSpec.persistentStorageEnabled -}}
  {{ toYaml .storageSpec.volumeMounts | nindent 2 -}}
{{ else }}
{{- with (first .storageSpec.volumeMounts) }}
  - name: {{ .name }}
{{- end }}
    mountPath: {{ .extraEnv.DB_DIR | quote }}
    emptyDir: {}
{{- end -}}
{{- end -}}

{{/*
ReadySet server statefulset pod volumes
*/}}
{{- define "readyset.server.volumes" -}}
volumes:
  - configMap:
      items:
      - key: entrypoint.sh
        path: entrypoint.sh
      name: {{ include "readyset.consul.agent.cm.name" $ }}
    name: init
  - configMap:
      items:
      - key: entrypoint.sh
        path: entrypoint.sh
      name: {{ include "readyset.consul.agent.cm.name" $ }}
    name: state
{{- end -}}
