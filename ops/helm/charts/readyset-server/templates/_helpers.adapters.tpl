{{/*
Image name and tag for Readyset server.
*/}}
{{- define "readyset.adapter.image" -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}

{{/*
ReadySet adapter service account selection template.
*/}}
{{- define "readyset.adapter.serviceAccountName" -}}
{{- if .root.readyset.adapter.rbac.serviceAccount.name -}}
{{- printf "%s" .root.readyset.adapter.rbac.serviceAccount.name -}}
{{- else -}}
{{- printf "%s-adapter" .releaseName -}}
{{- end -}}
{{- end -}}

{{/*
Readyset adapter service account's role configuration logic.
*/}}
{{- define "readyset.adapter.role.name" -}}
{{- if .root.readyset.adapter.rbac.role.name -}}
{{- printf "%s" .root.readyset.adapter.rbac.role.name -}}
{{- else -}}
{{- printf "%s-adapter" .releaseName -}}
{{- end -}}
{{- end -}}

{{/*
Readyset adapter service account's rolebinding configuration logic.
*/}}
{{- define "readyset.adapter.role.bindingName" -}}
{{- if .root.readyset.adapter.rbac.role.bindingName -}}
{{- printf "%s" .root.readyset.adapter.rbac.role.bindingName -}}
{{- else -}}
{{- printf "%s-adapter" .releaseName -}}
{{- end -}}
{{- end -}}

{{/*
ReadySet adapter service's name. Truncated to stay within k8s limits.
*/}}
{{- define "readyset.adapter.service.name" -}}
{{- $nameOverride := "" -}}
{{- $nameOverride = .service.nameOverride }}
{{- if $nameOverride }}{{ $nameOverride }}{{ else }}{{ template "readyset.fullname" .root }}-adapter{{- end -}}
{{- end -}}

{{/*
ReadySet adapter readiness probe.
*/}}
{{- define "readyset.adapter.readiness" -}}
readinessProbe:
  exec:
    command:
    - /bin/sh
    - -ec
    - |
      curl --fail http://127.0.0.1:6034/health
{{- end -}}

{{/*
ReadySet adapter liveness probe.
*/}}
{{- define "readyset.adapter.liveness" -}}
livenessProbe:
  exec:
    command:
    - /bin/sh
    - -ec
    - |
      curl --fail http://localhost:6034/health
  initialDelaySeconds: 5
  periodSeconds: 15
  failureThreshold: 3
{{- end -}}

{{/*
ReadySet adapter pod volumes
*/}}
{{- define "readyset.adapter.volumes" -}}
volumes:
  - configMap:
      items:
      - key: entrypoint.sh
        path: entrypoint.sh
      name: {{ include "readyset.consul.agent.cm.name" $ }}
    name: init
{{- end -}}

{{/*
ReadySet adapter, main configuration envionment variables.
*/}}
{{- define "readyset.adapter.dynamicEnvironmentVars" -}}
- name: ENGINE
  value: {{ .config.engine | quote }}
{{- if eq .config.engine "mysql" }}
- name: LISTEN_ADDRESS
  value: "0.0.0.0:3306"
{{ else if eq .config.engine "psql" }}
- name: LISTEN_ADDRESS
  value: "0.0.0.0:5432"
{{- end -}}
# ReadySet Deployment Name (Unique per Consul Cluster)
- name: NORIA_DEPLOYMENT
  value: {{ template "readyset.generic.deploymentName" .config.deploymentName | quote }}
# Cloud Provider Region
- name: NORIA_REGION
  value: {{ .config.region | quote }}
# Database ReadySet is proxying to
- name: UPSTREAM_DB_URL
  valueFrom:
    secretKeyRef:
      name: {{ .csecrets.replicationUrl.secretName }}
      key: {{ .csecrets.replicationUrl.urlSecretKey }}
# Username permitted to connect to ReadySet
- name: ALLOWED_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .csecrets.replicationUrl.secretName }}
      key: {{ .csecrets.replicationUrl.userSecretKey }}
# Password for user permitted to connect to ReadySet
- name: ALLOWED_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .csecrets.replicationUrl.secretName }}
      key: {{ .csecrets.replicationUrl.pwdSecretKey }}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: EXTERNAL_ADDRESS
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: LOG_FORMAT
  value: {{ .config.logFormat | quote }}
{{- end -}}

{{/*
ReadySet adapter role-based access control (RBAC) configurations.
*/}}
{{- define "readyset.adapter.rbac" -}}
{{ $roleBindingName := include "readyset.adapter.role.bindingName" (dict "root" .root "releaseName" .Release.Name) }}
{{ if .rbac.serviceAccount.create }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "readyset.adapter.serviceAccountName" (dict "root" .root "releaseName" .Release.Name) }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "readyset.generic.labels" (dict "releaseName" .Release.Name "componentLabel" "adapter" "Chart" .Chart) | nindent 4 }}
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
  name: {{ include "readyset.adapter.role.name" (dict "root" .root "releaseName" .Release.Name) }}
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
  name: {{ printf "%s" $roleBindingName }}
  namespace: {{ .rbac.role.namespace | default .Release.Namespace }}
subjects:
- kind: ServiceAccount
  name: {{ include "readyset.adapter.serviceAccountName" (dict "root" .root "releaseName" .Release.Name) }}
  namespace: {{ .Release.Namespace }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "readyset.adapter.role.name" (dict "root" .root "releaseName" .Release.Name) }}
{{- end -}}
{{- end -}}

{{/*
ReadySet adapter deployment template.
*/}}
{{- define "readyset.adapter" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ template "readyset.fullname" .root }}-adapter
  namespace: {{ .Release.Namespace }}
  labels:
  {{- toYaml .adapter.labels | nindent 4 }}
  {{- include "readyset.generic.labels" (dict "releaseName" .Release.Name "componentLabel" "adapter" "Chart" .Chart) | nindent 4 }}
spec:
  replicas: {{ .adapter.replicas }}
  strategy:
    rollingUpdate:
      maxSurge: 50%
      maxUnavailable: 25%
  selector:
    matchLabels:
    {{- include "readyset.chart.labels" (dict "releaseName" .Release.Name "componentName" "adapter") | nindent 6 }}
  template:
    metadata:
      labels:
      {{- toYaml .adapter.labels | nindent 8 }}
        {{- include "readyset.generic.labels" (dict "releaseName" .Release.Name "componentLabel" "adapter" "Chart" .Chart) | nindent 8 }}
        {{- if .adapter.extraLabels }}
          {{- toYaml .adapter.extraLabels | nindent 8 }}
        {{- end }}
        {{- if .adapter.annotations }}
      annotations:
          {{- tpl .adapter.annotations . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "readyset.adapter.serviceAccountName" (dict "root" $.Values "releaseName" .Release.Name) }}
      {{- if .adapter.affinity }}
      affinity:
      {{- toYaml .adapter.affinity | nindent 8 -}}
      {{- end }}
      {{- if .adapter.tolerations }}
      tolerations:
      {{- toYaml .adapter.tolerations | nindent 8 -}}
      {{- end }}
      {{- if .adapter.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{ toYaml .adapter.topologySpreadConstraints | indent 8 }}
      {{- end }}
      containers:
        {{- include "readyset.vector.agent.container" (dict "root" .Values "sidecar" .Values.readyset.vector.sidecar "common" .Values.readyset.common "componentName" .Values.readyset.adapter.componentName "prometheusScrapePort" .Values.readyset.adapter.prometheusScrapePort "releaseName" .Release.Name "Values" .Values) | nindent 8 }}
        {{- include "readyset.consul.agent.container" (dict "consul" .Values.readyset.consulAgent "Release" .Release) | nindent 8 }}
        - name: readyset-adapter
          image: {{ template "readyset.adapter.image" (dict "image" .adapter.image ) }}
          imagePullPolicy: {{ .adapter.image.pullPolicy | default .Values.global.imagePullPolicy }}
          command: [
            {{ .adapter.entrypoint | quote }}
          ]
          {{ if .adapter.args }}
          args:
          {{- range .adapter.args }}
            - {{ . | quote }}
          {{- end }}
          {{- end }}
          {{- if .adapter.securityContext }}
          securityContext:
            {{- toYaml .adapter.securityContext | nindent 12 }}
          {{- end }}
          env:
          {{- include "readyset.adapter.dynamicEnvironmentVars" (dict "config" .Values.readyset.common.config "csecrets" .Values.readyset.common.secrets) | nindent 12 -}}
          {{- include "readyset.generic.authority.environmentVariables" . | nindent 12 -}}
          {{- include "readyset.generic.extraEnvironmentVars" (dict "extraEnv" .adapter.extraEnvironmentVars) | nindent 12 }}
          {{- include "readyset.generic.resources" (dict "resources" .adapter.resources ) | nindent 10 }}
          {{- include "readyset.generic.containerPorts" (dict "ports" .adapter.containerPorts ) | nindent 10 }}
          {{- include "readyset.adapter.readiness" . | nindent 10 -}}
          {{- include "readyset.adapter.liveness" . | nindent 10 -}}
          {{- include "readyset.adapter.volumes" . | nindent 6 -}}
          {{- include "readyset.vector.agent.volumes" (dict "root" .root "sidecar" .Values.readyset.vector.sidecar) | nindent 8 }}
{{- end -}}

{{/*
ReadySet adapter service template.
*/}}
{{- define "readyset.adapter.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ template "readyset.adapter.service.name" (dict "root" . "service" .service) }}
  namespace: {{ .Release.Namespace }}
  labels:
  {{- toYaml .service.labels | nindent 4 }}
  {{- include "readyset.chart.labels" (dict "releaseName" $.Release.Name "componentName" "adapter") | nindent 4 }}
  {{- with .service.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: {{ .service.type }}
  selector:
  {{- include "readyset.chart.labels" (dict "releaseName" $.Release.Name "componentName" "adapter") | nindent 4 }}
  ports:
  {{- range $listener := .service.listeners }}
    - name: {{ $listener.name }}
      port: {{ $listener.port }}
      targetPort: {{ $listener.targetPort }}
      protocol: {{ $listener.protocol }}
  {{- end -}}
{{- end -}}
