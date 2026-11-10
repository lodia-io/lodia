{{- define "lodia.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "lodia.labels" -}}
{{ include "lodia.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "lodia.image" -}}
{{- if .image.digest -}}
{{ .image.repository }}@{{ .image.digest }}
{{- else -}}
{{ .image.repository }}:{{ .image.tag | default .appVersion }}
{{- end -}}
{{- end }}

{{- define "lodia.etcdDruidNamespace" -}}
{{- if .Values.etcdDruid.enabled -}}
{{ .Release.Namespace }}
{{- else -}}
{{ required "etcdDruidNamespace must name the namespace etcd-druid runs in while etcdDruid.enabled is false" .Values.etcdDruidNamespace }}
{{- end -}}
{{- end }}

{{- define "lodia.etcdDruidDeployment" -}}
{{- if .Values.etcdDruid.enabled -}}
etcd-druid
{{- else -}}
{{ required "etcdDruidDeployment must name etcd-druid's Deployment while etcdDruid.enabled is false" .Values.etcdDruidDeployment }}
{{- end -}}
{{- end }}
