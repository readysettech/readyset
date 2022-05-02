{{/*
Vector agent sidecar container. Formed for injection in container spec.
*/}}
{{- define "readyset.vector.agent.container" -}}
{{- if .sidecar.enabled -}}
- name: vector-agent
  image: {{ printf "%s:%s" .sidecar.image.repository .sidecar.image.tag }}
  imagePullPolicy: {{ .sidecar.image.pullPolicy }}
  args:
    - --config-dir
    - /etc/vector/
  env:
    - name: VECTOR_SELF_NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: VECTOR_SELF_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: PROCFS_ROOT
      value: "/host/proc"
    - name: SYSFS_ROOT
      value: "/host/sys"
{{- include "readyset.vector.common.env" (dict "common" .common ) | nindent 4 }}
    # Adapter, server, etc.
    - name: SIDECAR_COMPONENT_NAME
      value: {{ .componentName | quote }}
    - name: PROMETHEUS_SCRAPE_PORT
      value: {{ .prometheusScrapePort | quote }}
    - name: AGGREGATOR_ADDRESS
      value: {{ include "readyset.vector.agg.addr" (dict "releaseName" .releaseName "Values" $.Values) | quote }}
  ports:
    - name: out
      containerPort: 9000
      protocol: TCP
  volumeMounts:
    # Host log directory mount.
    - name: var-log
      mountPath: /var/log/
      readOnly: true
    # Host mount for docker and containerd log file symlinks.
    - name: var-lib
      mountPath: /var/lib
      readOnly: true
    # Vector data dir mount.
    - name: data-dir
      mountPath: {{ .sidecar.vectorDataDir | default "/vector-data-dir" }}
    # Vector config dir mount.
    - name: config-dir
      mountPath: /etc/vector
      readOnly: true
    # Host procsfs mount.
    - name: procfs
      mountPath: /host/proc
      readOnly: true
    # Host sysfs mount.
    - name: sysfs
      mountPath: /host/sys
      readOnly: true
{{- end -}}
{{- end -}}

{{/*
Vector agent sidecar volume mounts. Added to any pods with vector sidecar.
*/}}
{{- define "readyset.vector.agent.volumes" -}}
# Log directory
- name: var-log
  hostPath:
    path: /var/log/
# Docker and containerd log files in Kubernetes are symlinks to this folder.
- name: var-lib
  hostPath:
    path: /var/lib/
# Vector will store its' data here
- name: data-dir
  hostPath:
    path: /var/lib/vector/
# Vector config dir
- name: config-dir
  projected:
    sources:
      - configMap:
          name: {{ include "readyset.vector.agent.configmap.name" (dict "root" .root "sidecar" .sidecar) }}
# Host procsfs
- name: procfs
  hostPath:
    path: /proc
# Host sysfs
- name: sysfs
  hostPath:
    path: /sys
{{- end -}}

{{/*
Vector agent sidecar configmap to use.
Either uses the user provided nameOverride or generates a name based on global name settings.
*/}}
{{- define "readyset.vector.agent.configmap.name" -}}
{{- $nameOverride := "" -}}
{{- $nameOverride = .sidecar.configMaps.config.nameOverride }}
{{- if $nameOverride -}}
{{- printf "%s" $nameOverride -}}
{{- else -}}
{{- printf "%s-vector-agent" .root.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
Readyset Vector agent sidecar configmap.
For configuration option reference:
https://vector.dev/docs/reference/configuration/
*/}}
{{- define "readyset.vector.agent.cm" -}}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "readyset.vector.agent.configmap.name" (dict "root" . "sidecar" .Values.readyset.vector.sidecar) }}
  namespace: {{ .Release.Namespace }}
  {{ if .Values.readyset.vector.sidecar.labels -}}
  labels:
    {{- range $k, $v := .Values.readyset.vector.sidecar.labels }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
  {{- end }}
data:
  "vector.yaml": |
      data_dir: {{ .Values.readyset.vector.sidecar.vectorDataDir }}
      api:
        enabled: false
      sources:
        app_logs:
          type: file
          ignore_older_secs: 600
          {{ if .Values.readyset.vector.sidecar.includeLogs -}}
          include:
          {{- range $k, $v := .Values.readyset.vector.sidecar.includeLogs }}
            - {{ $v }}
          {{ end -}}
          {{- end -}}
          {{ if .Values.readyset.vector.sidecar.excludeLogs -}}
          exclude:
          {{- range $k, $v := .Values.readyset.vector.sidecar.excludeLogs }}
            - {{ $v }}
          {{ end -}}
          {{- end -}}
          read_from: beginning
        # Scrape node metrics from the Node-Exporter daemonset
        node-exporter:
          type: prometheus_scrape
          endpoints:
            - 'http://${NODE_IP}:9100/metrics'
          scrape_interval_secs: 15
        # Scrape prometheus endpoint of ReadySet applications
        prometheus:
          type: prometheus_scrape
          endpoints:
            - 'http://localhost:${PROMETHEUS_SCRAPE_PORT}/prometheus'
          scrape_interval_secs: 15

      transforms:
        metrics:
          type: remap
          inputs:
            - node-exporter
            - prometheus
          source: |2
              .tags.deployment = "${READYSET_DEPLOYMENT_NAME}"
              .tags.job = "${SIDECAR_COMPONENT_NAME}"
              .tags.instance = "${NODE_IP}"
        metadata:
          type: remap
          inputs:
            - app_logs
          source: |2
              server_info = { "host": .host, "app": "${SIDECAR_COMPONENT_NAME}", "deployment": "${READYSET_DEPLOYMENT_NAME}" }
              .event = parse_common_log(.message) ?? parse_json!(parse_json!(.message).log)
              . = merge(., server_info)

      sinks:
        aggregator:
          inputs:
            - metrics
          type: vector
          address: "${AGGREGATOR_ADDRESS}"
        cloudwatch_logs:
          inputs: ["metadata"]
          type: "aws_cloudwatch_logs"
          create_missing_group: true
          create_missing_stream: true
          group_name: "${LOG_GROUP_NAME}"
          compression: "none"
          region: "${AWS_REGION}"
          # Equates to the name of the pod
          stream_name: "{{ "{{ host }}" }}"
          encoding:
            codec: json

{{- end -}}

{{/*
Vector aggregator configmap to use.
Returns nameOverride or generates a name based on global name settings.
*/}}
{{- define "readyset.vector.aggregator.configmap.name" -}}
{{- $nameOverride := "" -}}
{{- $nameOverride = .aggregator.configMaps.config.nameOverride }}
{{- if $nameOverride -}}
{{- printf "%s" $nameOverride -}}
{{- else -}}
{{- printf "%s-vector-aggregator" .root.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
PodSpec definition for Vector aggregator.
*/}}
{{- define "readyset.vector.aggregator.pod" -}}
serviceAccountName: {{ include "readyset.vector.aggregator.serviceAccountName" .root }}
{{- with .aggregator.podSecurityContext }}
securityContext:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .aggregator.podPriorityClassName }}
priorityClassName: {{ . }}
{{- end }}
{{- with .aggregator.dnsPolicy }}
dnsPolicy: {{ . }}
{{- end }}
{{- with .aggregator.dnsConfig }}
dnsConfig:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .aggregator.image.pullSecrets }}
imagePullSecrets:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .aggregator.initContainers }}
initContainers:
{{ toYaml . | indent 2 }}
{{- end }}
containers:
  - name: vector
{{- with .aggregator.securityContext }}
    securityContext:
{{ toYaml . | indent 6 }}
{{- end }}
    image: "{{ printf "%s:%s" .aggregator.image.repository .aggregator.image.tag }}"
    imagePullPolicy: {{ .aggregator.image.pullPolicy }}
{{- with .aggregator.command }}
    command:
    {{- toYaml . | nindent 6 }}
{{- end }}
{{- with .aggregator.args }}
    args:
    {{- toYaml . | nindent 6 }}
{{- end }}
    env:
{{- include "readyset.vector.common.env" (dict "common" .root.Values.readyset.common ) | nindent 4 }}
{{- with .aggregator.env }}
    {{- toYaml . | nindent 6 }}
{{- end }}
    ports:
{{- with .aggregator.containerPorts }}
    {{- toYaml . | nindent 6 }}
{{- end }}
{{- with .aggregator.livenessProbe }}
    livenessProbe:
      {{- toYaml . | trim | nindent 6 }}
{{- end }}
{{- with .aggregator.readinessProbe }}
    readinessProbe:
      {{- toYaml . | trim | nindent 6 }}
{{- end }}
{{- with .aggregator.resources }}
    resources:
{{- toYaml . | nindent 6 }}
{{- end }}
    volumeMounts:
      - name: data
        mountPath: {{ .aggregator.vectorDataDir | quote }}
      - name: config
        mountPath: "/etc/vector/"
        readOnly: true
{{- with .aggregator.extraVolumeMounts }}
{{- toYaml . | nindent 6 }}
{{- end }}
terminationGracePeriodSeconds: {{ .aggregator.terminationGracePeriodSeconds }}
{{- with .aggregator.nodeSelector }}
nodeSelector:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .aggregator.affinity }}
affinity:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with .aggregator.tolerations }}
tolerations:
{{ toYaml . | indent 2 }}
{{- end }}
volumes:
{{- if and .aggregator.persistence.enabled }}
{{- with .aggregator.persistence.existingClaim }}
  - name: data
    persistentVolumeClaim:
      claimName: {{ . }}
{{- end }}
{{- else }}
  - name: data
    emptyDir: {}
{{- end }}
  - name: config
    projected:
      sources:
      - configMap:
          name: {{ include "readyset.vector.aggregator.configmap.name" (dict "root" .root "aggregator" .aggregator) }}
{{- with .aggregator.extraVolumes }}
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Vector aggregator service account selection template.
*/}}
{{- define "readyset.vector.aggregator.serviceAccountName" -}}
{{- $nameOverride := "" -}}
{{- $nameOverride = $.Values.readyset.vector.aggregator.serviceAccount.nameOverride }}
{{- if $nameOverride -}}
{{- printf "%s" $nameOverride -}}
{{- else -}}
{{- printf "%s-vector-aggregator" $.Release.Name -}}
{{- end -}}
{{- end -}}

{{/*
Shared set of environment variables for Readyset Vector containers, including agent sidecars and the aggregator.
*/}}
{{- define "readyset.vector.common.env" -}}
- name: READYSET_DEPLOYMENT_NAME
  value: {{ .common.config.deploymentName | quote }}
- name: LOG_GROUP_NAME
  value: {{ .common.config.cwLogGroupName | quote }}
- name: AWS_REGION
  value: {{ .common.config.primaryRegion | quote }}
{{- if .common.secrets.vectorIamCredentials.secretName }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .common.secrets.vectorIamCredentials.secretName }}
      key: {{ .common.secrets.vectorIamCredentials.accessKey }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .common.secrets.vectorIamCredentials.secretName }}
      key: {{ .common.secrets.vectorIamCredentials.secretKey }}
{{- end }}
- name: VECTOR_SELF_POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: NODE_IP
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
{{- end -}}

{{/*
Vector aggregator configmap for all YAML/TOML configurations.
For configuration option reference:
https://vector.dev/docs/reference/configuration/
*/}}
{{- define "readyset.vector.aggregator.cm" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "readyset.vector.aggregator.configmap.name" (dict "root" .root "aggregator" .Values.readyset.vector.aggregator) }}
  namespace: {{ .Release.Namespace }}
  {{- with .Values.readyset.vector.aggregator }}
  {{ if .labels -}}
  labels:
    {{- range $k, $v := .labels }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
  {{- end }}
  {{- end }}
data:
  "vector.yaml": |
      data_dir: {{ .Values.readyset.vector.aggregator.vectorDataDir }}
      api:
        enabled: true
        address: {{ .Values.readyset.vector.aggregator.vectorApiListener }}
      sources:
        in:
          type: vector
          address: {{ .Values.readyset.vector.aggregator.vectorSourceListener }}
        node-exporter:
          type: prometheus_scrape
          endpoints:
            - {{ .Values.readyset.vector.aggregator.nodeExporterEndpoint }}
          scrape_interval_secs: {{ .Values.readyset.vector.aggregator.nodeExporterScrapeInterval }}

      transforms:
        metrics:
          type: remap
          inputs:
            - node-exporter
          source: |2
              .tags.deployment = "${READYSET_DEPLOYMENT_NAME}"
              .tags.job = "vector-aggregator"
      sinks:
        out:
          inputs:
            - in
          type: console
          target: stdout
          encoding:
            codec: json

        # Send received application logs to CloudWatch Logs
        cloudwatch_logs:
          type: aws_cloudwatch_logs
          inputs:
            - in
          create_missing_group: true
          create_missing_stream: true
          group_name: "${LOG_GROUP_NAME}"
          compression: none
          region: "${AWS_REGION}"
          # Equates to the name of the pod
          stream_name: "{{ "{{ host }}" }}"
          encoding:
            codec: json

        # Aggregate metrics sent from application
        # containers to CW metrics
        cloudwatch_metrics:
          type: aws_cloudwatch_metrics
          inputs:
            - in
            - metrics
          default_namespace: "${VECTOR_SELF_POD_NAMESPACE}"
          region: "${AWS_REGION}"

        # Expose scrape endpoint to Prometheus
        prometheus:
          type: prometheus_exporter
          inputs:
            - in
            - metrics
          address: '0.0.0.0:9090'

{{- end -}}


{{/*
Readyset Vector aggregator resource name configuration logic.
*/}}
{{- define "readyset.vector.aggregator.name" -}}
{{ if .Values.readyset.vector.aggregator.nameOverride -}}
{{- printf "%s" .Values.readyset.vector.aggregator.nameOverride -}}
{{- else -}}
{{- printf "%s-vector-aggregator" .releaseName -}}
{{- end -}}
{{- end -}}
