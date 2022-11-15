{{/*
Image name and tag for Readyset benchmark runner's container image.
*/}}
{{- define "benchmark.runner.image" -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}

{{/*
Generates a unique name for the ReadySet benchmark runner job/pod.
*/}}
{{- define "benchmark.runner.name" -}}
{{- printf "%s-%v-%s" .labels.app .labels.buildNumber .labels.rdbms -}}
{{- end -}}

{{/*
Produces environment vars in the format key:value, if populated.
*/}}
{{- define "benchmark.runner.environmentVars" -}}
{{- if len .env }}
env:
{{- end -}}
{{- range $key, $value := .env }}
  - name: {{ $key }}
    value: {{ $value | quote }}
{{- end -}}
{{- end -}}

{{/*
Produces an affinity map to be assigned to benchmark runner pods.
*/}}
{{- define "benchmark.runner.affinity" -}}
{{- if .affinity -}}
affinity:
{{- .affinity | toYaml | nindent 2 -}}
{{- end -}}
{{- end -}}

{{/*
Produces a tolerations map to be assigned to benchmark runner pods.
*/}}
{{- define "benchmark.runner.tolerations" -}}
{{- if .tolerations -}}
tolerations:
{{- .tolerations | toYaml | nindent 2 -}}
{{- end -}}
{{- end -}}


{{/*
Produces a resources map to be assigned to benchmark runner pods.
*/}}
{{- define "benchmark.runner.resources" -}}
{{- if .resources -}}
resources:
{{- .resources | toYaml | nindent 2 -}}
{{- end -}}
{{- end -}}
