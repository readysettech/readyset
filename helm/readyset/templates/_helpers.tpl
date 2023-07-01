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
readyset-adapter image default
*/}}
{{- define "readyset.adapter.imageDefault" -}}
{{- if .Values.readyset -}}
{{- if .Values.readyset.adapter -}}
{{- if .Values.readyset.adapter.imageRepository }}
{{- if .Values.readyset.server.imageTag }}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $imageRepository := default "public.ecr.aws/readyset" .Values.readyset.adapter.imageRepository -}}
{{- $imageTag := default .Chart.AppVersion .Values.readyset.adapter.imageTag -}}
{{- printf "%s/readyset:%s" (lower $imageRepository) (lower $imageTag) -}}
{{- end -}}

{{/*
readyset-server image default
*/}}
{{- define "readyset.server.imageDefault" -}}
{{- if .Values.readyset -}}
{{- if .Values.readyset.server -}}
{{- if .Values.readyset.server.imageRepository }}
{{- if .Values.readyset.server.imageTag }}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $imageRepository := default "public.ecr.aws/readyset" .Values.readyset.server.imageRepository -}}
{{- $imageTag := default .Chart.AppVersion .Values.readyset.server.imageTag -}}
{{- printf "%s/readyset-server:%s" (lower $imageRepository) (lower $imageTag) -}}
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
Logic for adding selector labels based on adapter or server
*/}}
{{- define "readyset.selectorLabels" -}}
{{- if contains "readyset-adapter" .Template.Name -}}
app.kubernetes.io/component: adapter
{{- else if contains "readyset-server" .Template.Name -}}
app.kubernetes.io/component: server
{{- end -}}
{{- end }}

{{/*
readyset.adapter.httpPort -- Port for readyset-adapter
*/}}
{{- define "readyset.adapter.httpPort" -}}
6034
{{- end }}

{{/*
readyset.server.httpPort -- Port for readyset-server
*/}}
{{- define "readyset.server.httpPort" -}}
6033
{{- end }}

{{/*
readyset.mysqlPort -- Port number for readyset-adapter
*/}}
{{- define "readyset.mysqlPort" -}}
3306
{{- end }}

{{/*
readyset.postgresqlPort -- Port number for readyset-adapter
*/}}
{{- define "readyset.postgresqlPort" -}}
5432
{{- end }}

{{/*
readyset.defaultStorageClass is null, to default to the cluster's default provisioner. Do set this to what works best for your workload.
*/}}
{{- define "readyset.defaultStorageClass" -}}
null
{{- end }}

{{/*
readyset.queryCachingMode -- Ensure the value is one of the three currently accepted options
*/}}
{{- define "readyset.queryCachingMode" -}}
{{- $mode := mustRegexFind "async|explicit|in-request-path" .Values.readyset.queryCachingMode }}
{{- if not $mode }}
{{ fail "Must pass either 'async', 'explicit', 'in-request-path' to readyset.queryCachingMode" }}
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
