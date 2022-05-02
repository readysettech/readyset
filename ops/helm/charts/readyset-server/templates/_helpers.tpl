{{/*
Fullname of ReadySet components. Truncated at 63 characters to stay within bounds of k8s limits.
For more information see: https://stackoverflow.com/questions/50412837/kubernetes-label-name-63-character-limit
*/}}
{{- define "readyset.fullname" -}}
{{- if .Values.global.fullnameOverride -}}
{{- .Values.global.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if .Values.global.name -}}
{{- .Values.global.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name (lower $name)  | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "readyset.chart" -}}
{{- printf "%s" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "readyset.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Allows supplemental environment vars to be passed through
from values.yaml to any ReadySet containers deployed.
*/}}
{{- define "readyset.generic.extraEnvironmentVars" -}}
{{- if .extraEnv -}}
{{- range $key, $value := .extraEnv -}}
- name: {{ $key }}
  value: {{ $value | quote }}
{{ end -}}
{{- end -}}
{{- end -}}

{{/*
Deployment name template to be shared amongst
ReadySet adapter and server pod definitions.
*/}}
{{- define "readyset.generic.deploymentName" -}}
{{ . | replace " " "-" }}
{{- end -}}

{{/*
ReadySet persistent volumeClaimTemplate.
*/}}
{{- define "readyset.generic.volumeClaimTemplates" -}}
{{- if .storageSpec.persistentStorageEnabled }}
  volumeClaimTemplates:
  {{- toYaml .storageSpec.volumeClaimTemplates | nindent 4 -}}
{{- end -}}
{{- end -}}

{{/*
Resource template block for general usage.
*/}}
{{- define "readyset.generic.resources" -}}
{{- if .resources -}}
resources:
{{- toYaml .resources | nindent 2 -}}
{{- end -}}
{{- end -}}

{{/*
ReadySet generic containerPorts template.
*/}}
{{- define "readyset.generic.containerPorts" -}}
{{ if .ports }}
ports:
{{- toYaml .ports | nindent 2 }}
{{- end -}}
{{- end -}}

{{/*
Consul agent sidecar container to connect to Consul server.
*/}}
{{- define "readyset.consul.agent.container" -}}
- name: consul-agent
  image: {{ printf "%s:%s" .consul.image .consul.tag }}
  volumeMounts:
    - mountPath: /usr/src/app/entrypoint.sh
      name: init
      subPath: entrypoint.sh
  command: [
    {{ .consul.entrypoint | quote }}
  ]
  args:
  {{- range .consul.args }}
    - {{ . | quote }}
  {{- end }}
  ports:
  {{- toYaml .consul.containerPorts | nindent 2 }}
  env:
    - name: MY_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: ADVERTISE_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: POD_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: HOST_IP
      valueFrom:
        fieldRef:
          fieldPath: status.hostIP
    - name: NODE
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: CONSUL_SERVER_NAMESPACE
      value: {{ .consul.serverNamespaceOverride | default .Release.Namespace | quote }}
  readinessProbe:
    exec:
      command:
      - /bin/sh
      - -ec
      - |
        curl http://127.0.0.1:8500/v1/health/node/$(hostname) \
        2>/dev/null | grep -E '".+"'
{{- end -}}

{{/*
Fetches either the release namespace or the namespace override, if one is provided.
*/}}
{{- define "readyset.namespace" -}}
  {{- if .namespaceOverride -}}
    {{- .namespaceOverride -}}
  {{- else -}}
    {{- .releaseNs -}}
  {{- end -}}
{{- end -}}

{{/*
List of common labels shared amongst all Readyset chart components.
*/}}
{{- define "readyset.generic.labels" -}}
{{ include "readyset.chart.labels" (dict "releaseName" .releaseName "componentName" .componentLabel) }}
app.kubernetes.io/component: {{ .componentLabel }}
app.kubernetes.io/managed-by: Helm
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "readyset.chart.labels" -}}
app.kubernetes.io/instance: {{ .releaseName | quote }}
app.kubernetes.io/name: {{ .componentName | quote }}
{{- end }}

{{/*
Readyset authority address configuration logic.
*/}}
{{- define "readyset.generic.authority.addr" -}}
{{ if .Values.readyset.common.config.authorityAddressOverride -}}
{{- printf "%s" .Values.readyset.common.config.authorityAddressOverride -}}
{{- else -}}
{{- printf "%s-consul-server:8500" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
Readyset authority type.
*/}}
{{- define "readyset.generic.authority.type" -}}
{{- printf "%s" $.Values.readyset.common.config.authorityType -}}
{{- end -}}

{{/*
Readyset authority environment configuration variables.
*/}}
{{- define "readyset.generic.authority.environmentVariables" -}}
- name: AUTHORITY_ADDRESS
  value: {{ include "readyset.generic.authority.addr" . | quote }}
- name: AUTHORITY
  value: {{ include "readyset.generic.authority.type" . | quote }}
{{- end -}}

{{/*
Readyset Vector aggregator address configuration logic.
*/}}
{{- define "readyset.vector.agg.addr" -}}
{{ if .Values.readyset.common.config.aggregatorAddressOverride -}}
{{- printf "%s" .Values.readyset.common.config.aggregatorAddressOverride -}}
{{ else if .Values.readyset.vector.aggregator.service.nameOverride -}}
{{- printf "%s:9000" .Values.readyset.vector.aggregator.service.nameOverride -}}
{{- else -}}
{{- printf "%s-vector-aggregator:9000" .releaseName -}}
{{- end -}}
{{- end -}}

{{/*
Template representing the Consul Agent configmap's name.
Allows for overriding of chart generated configmap name.
*/}}
{{- define "readyset.consul.agent.cm.name" -}}
{{- $name := $.Values.readyset.consulAgent.configMaps.config.nameOverride -}}
{{ if $name -}}
{{- printf "%s" $name -}}
{{- else -}}
{{- printf "%s-consul-agent-cm" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{- define "readyset.consul.agent.cm" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "readyset.consul.agent.cm.name" $ }}
  namespace: {{ .Release.Namespace }}
  labels:
  {{- include "readyset.generic.labels" (dict "releaseName" $.Release.Name "componentLabel" "consul-agent" "Chart" $.Chart) | nindent 4 }}
  {{ if .Values.readyset.consulAgent.labels }}
    {{- range $k, $v := .Values.readyset.consulAgent.labels }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
  {{- end }}
data:
  entrypoint.sh: |
      #/bin/sh
      set -e
      CONSUL_SERVER_NAMESPACE="${CONSUL_SERVER_NAMESPACE:-consul}"
      exec /usr/local/bin/docker-entrypoint.sh consul agent \
        -advertise="${ADVERTISE_IP}" \
        -bind=0.0.0.0 \
        -client=0.0.0.0 \
        -node-meta=host-ip:${HOST_IP} \
        -node-meta=pod-name:${MY_POD_NAME} \
        -hcl='leave_on_terminate = true' \
        -hcl='ports { grpc = 8502 }' \
        -config-dir=/consul/config \
        -data-dir=/consul/data \
        -retry-join="provider=k8s namespace=${CONSUL_SERVER_NAMESPACE} label_selector=\"app=consul,component=server,release={{ .Release.Name }}\"" \
        -node="${MY_POD_NAME}" \
        -serf-lan-port=8301

{{- end -}}
