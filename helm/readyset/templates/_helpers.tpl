{{/*
Expand the name of the chart.
*/}}
{{- define "readyset.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fullname of ReadySet components. Truncated at 63 characters to stay within bounds of k8s limits.
For more information see: https://stackoverflow.com/questions/50412837/kubernetes-label-name-63-character-limit
*/}}
{{- define "readyset.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
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
{{- define "readyset.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ printf "%s" (required "Must pass readyset.deployment as a value" .Values.readyset.deployment) }}
app.kubernetes.io/part-of: {{ include "readyset.name" . }}
app.kubernetes.io/name: readyset
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{ include "readyset.selectorLabels" . }}
{{- end }}

{{/*
Readyset authority address configuration logic.
*/}}
{{- define "readyset.selectorLabels" -}}
{{- if contains "readyset-adapter" .Template.Name -}}
app.kubernetes.io/component: adapter
{{- else if contains "readyset-server" .Template.Name -}}
app.kubernetes.io/component: server
{{- end -}}
{{- end }}

{{/*
Default HTTP port for readyset-adapter
*/}}
{{- define "readyset.adapter.httpPort" -}}
6034
{{- end }}

{{/*
Default HTTP port for readyset-server
*/}}
{{- define "readyset.server.httpPort" -}}
6033
{{- end }}

{{/*
MySQL port number for readyset-adapter
*/}}
{{- define "readyset.mysqlPort" -}}
3306
{{- end }}

{{/*
PostgreSQL port number for readyset-adapter
*/}}
{{- define "readyset.postgresqlPort" -}}
5432
{{- end }}

{{/*
Default storageClass is null, to default to the cluster's default provisioner. Do set this to what works best for your workload.
*/}}
{{- define "readyset.defaultStorageClass" -}}
null
{{- end }}

{{/*
Define the database type we're connecting to
*/}}
{{- define "readyset.query_caching_mode" -}}
{{- $mode := mustRegexFind "async|explicit|in-request-path" .Values.readyset.query_caching_mode }}
{{- if not $mode }}
{{ fail "Must pass either 'async', 'explicit', 'in-request-path' to readyset.query_caching_mode" }}
{{- else -}}
{{- printf "%s" $mode -}}
{{- end -}}
{{- end }}

{{- define "readyset.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "readyset.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}
