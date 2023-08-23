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
{{- default 6034 .Values.readyset.adapter.httpPort -}}
{{- end }}

{{/*
readyset.adapter.cpu -- CPUs configuration for readyset-adapter container
*/}}
{{- define "readyset.adapter.cpu" -}}
{{- default "500m" .Values.readyset.adapter.resources.requests.cpu }}
{{- end }}

{{/*
readyset.adapter.memory -- Default memory requirement for readyset-adapter

We configure the container resources and limits to use the same value for memory,
this is to avoid Kubernetes OOM.

*/}}
{{- define "readyset.adapter.memory" -}}
{{ coalesce .Values.readyset.adapter.resources.limits.memory .Values.readyset.adapter.resources.requests.memory "2Gi" }}
{{- end }}

{{/*
readyset.server.cpu -- CPUs configuration for readyset-server container
*/}}
{{- define "readyset.server.cpu" -}}
{{- default 1 .Values.readyset.server.resources.requests.cpu }}
{{- end }}

{{/*
readyset.server.memory -- Memory configuration for readyset-server container

We configure the container resources and limits to use the same value for memory,
this is to avoid Kubernetes OOM.

If .Values.readyset.server.resources.limits.memory is defined it takes presedence,
otherwise we use .Values.readyset.server.requests.memory, else we default to 4Gi.

If the user configures just numbers we default to bytes (Kubernetes' default),
otherwise we calculate the bytes number so we can then pass as env variable
(READYSET_MEMORY_LIMIT) to the container so we can set a memory upper limit.

*/}}
{{- define "readyset.server.memory" -}}
{{- $memory := coalesce .Values.readyset.server.resources.limits.memory .Values.readyset.server.resources.requests.memory "4Gi" }}
{{- $memoryUnit := mustRegexFind "(k|Ki|m|Mi|M|Gi|G|Ti|T|Pi|P|Ei|E)$" $memory }}
{{- $memorySize := mustRegexFind "^([0-9.]+)" $memory }}
{{- $memoryUnitDict := dict "k" "1000" "Ki" "1024" "m" "1000000" "M" "1000000" "Mi" "1048576" "G" "1000000000" "Gi" "1073741824" "T" "1000000000000" "Ti" "1099511627776" "P" "1000000000000000" "Pi" "1125899906842624" "E" "1000000000000000000" "Ei" "1152921504606846976" }}
{{- $memoryUnitInBytes := get $memoryUnitDict $memoryUnit }}
{{- if not $memoryUnit }}
{{- $memorySize -}}
{{- else }}
{{- mulf $memorySize $memoryUnitInBytes | floor | toJson }}
{{- end }}
{{- end }}

{{/*
readyset.server.httpPort -- Port for readyset-server
*/}}
{{- define "readyset.server.httpPort" -}}
{{- default 6033 .Values.readyset.server.httpPort -}}
{{- end }}

{{/*
readyset.adapter.type -- Readyset Adapter type ("postgresql" or "mysql")
*/}}
{{- define "readyset.adapter.type" -}}
{{- $type := mustRegexFind "postgresql|mysql" (default "postgresql" .Values.readyset.adapter.type) }}
{{- if not $type }}
{{ fail "Must pass either 'mysql' or 'postgresql' to readyset.adapter.type" }}
{{- else -}}
{{- printf "%s" $type -}}
{{- end -}}
{{- end }}

{{- define "readyset.adapter.port" -}}
{{- $type := ( include "readyset.adapter.type" . ) }}
{{- if eq $type "postgresql" -}}
{{- default 5432 .Values.readyset.adapter.port -}}
{{- else -}}
{{- default 3306 .Values.readyset.adapter.port -}}
{{- end -}}
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
    {{ default ( include "readyset.fullname" . ) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "readyset.authority_address" -}}
{{- $endpointIncludesPort := mustRegexFind ".*:[0-9]+$" .Values.readyset.authority_address }}
{{- if .Values.consul.enabled -}}
{{ printf "%s-%s:8500" .Chart.Name "consul-server" | trunc 63 }}
{{- else if and .Values.readyset.authority_address $endpointIncludesPort -}}
{{ printf "%s" .Values.readyset.authority_address }}
{{- else if .Values.readyset.authority_address -}}
{{ printf "%s:8500" .Values.readyset.authority_address }}
{{- else -}}
{{ fail "You must configure readyset.authority_address if consul.enabled is false" }}
{{- end -}}
{{- end -}}
