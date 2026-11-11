# Every tool runs through `go tool` (see the tool block in go.mod), except
# golangci-lint, helm, promtool and dashboard-linter, which are installed on their own, and ko
# and hydrophone, which run as `go run github.com/google/ko@<version>` and
# `go run sigs.k8s.io/hydrophone@<version>`: their dependency graphs would
# move the operator's own dependencies.

ENVTEST_K8S_VERSION ?= 1.35.0
E2E_CLUSTER ?= lodia-e2e-local
E2E_DIR ?= $(CURDIR)/work/e2e
E2E_HA_TEST ?= TestHighAvailabilityGate
E2E_UPGRADE_TEST ?= TestMinorUpgradeGate
E2E_MAINTENANCE_TEST ?= TestOperatorUpgradeInWindows
CHART = charts/lodia
CONTROLLERS = datastore health lifecycle pki tenant workloads
RACE_PACKAGES = ./internal/controller/health ./internal/metrics
DASHBOARDS = $(wildcard $(CHART)/files/dashboards/*.json)
METRICS = metrics.service.enabled=true,metrics.serviceMonitor.enabled=true,metrics.rules.enabled=true,metrics.dashboards.enabled=true
GENERATED = api/v1alpha1/zz_generated.deepcopy.go api/crds/*.yaml $(CHART)/files/crds/*.yaml $(CHART)/files/rbac/*.yaml
DIGEST = sha256:0000000000000000000000000000000000000000000000000000000000000000

.PHONY: generate
generate: ## regenerate deepcopy functions and CRDs from api/, the chart's copy of the CRDs, and each controller's ClusterRole from its RBAC markers
	go tool controller-gen object:headerFile=hack/boilerplate.go.txt paths=./api/...
	go tool controller-gen crd paths=./api/... output:crd:artifacts:config=api/crds
	rm -f $(CHART)/files/crds/*.yaml && cp api/crds/*.yaml $(CHART)/files/crds/
	rm -f $(CHART)/files/rbac/*.yaml
	for c in $(CONTROLLERS); do go tool controller-gen rbac:roleName=lodia-$$c,fileName=$$c.yaml paths=./internal/controller/$$c output:rbac:artifacts:config=$(CHART)/files/rbac || exit 1; done

.PHONY: lint
lint:
	golangci-lint run ./...
	golangci-lint run --build-tags e2e ./test/e2e/...
	golangci-lint run --build-tags e2e,conformance ./test/e2e/...
	golangci-lint run --build-tags e2e,ha ./test/e2e/...
	golangci-lint run --build-tags e2e,restore ./test/e2e/...
	golangci-lint run --build-tags e2e,gateway ./test/e2e/...
	golangci-lint run --build-tags e2e,hold ./test/e2e/...
	golangci-lint run --build-tags e2e,upgrade ./test/e2e/...
	golangci-lint run --build-tags e2e,maintenance ./test/e2e/...
	cd api && golangci-lint run ./...

.PHONY: test
test: ## unit and envtest tests, those of RACE_PACKAGES with the race detector; envtest runs a real API server and etcd
	# @todo: 60 s until we know what holds an API server past SIGTERM; see todo.md.
	KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test $$(go list ./... | grep -vxF "$$(go list $(RACE_PACKAGES))")
	KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test -race $(RACE_PACKAGES)
	cd api && go test ./...

.PHONY: restore-keys
restore-keys: ## the etcd keys a restore's cleanup deletes, against the API server of each minor Lodia runs
	@minors="$$(go run ./test/restorekeys/minors)" && [ -n "$$minors" ] || { echo "no minors to run"; exit 1; }; \
	for minor in $$minors; do \
		assets="$$(go tool setup-envtest use $$minor -p path)" && \
		KUBEBUILDER_ASSETS="$$assets" RESTORE_KEYS_MINOR="$$minor" go test -count=1 -v ./test/restorekeys || exit 1; \
	done

.PHONY: chart
chart: ## lint and render the chart with image tags, then with image digests; with crds.enabled=false it must still render the policy kubeconfigrequests.lodia.io and its binding; the metrics Service and ServiceMonitor only once their values are on, rendered as the release check, and the ServiceMonitor scraping that Service alone over https, with the labels it is given; the PrometheusRule only once its value is on, with the labels it is given, holding the rule file with every runbook at the release's tag unless appVersion ends in -dev; the dashboards only once their value is on, a ConfigMap each, holding its file, with the labels it is given
	helm dependency build $(CHART)
	helm lint --strict $(CHART)
	helm template lodia $(CHART) --namespace lodia-system > /dev/null
	@policies="$$(helm template lodia $(CHART) --namespace lodia-system --set crds.enabled=false | awk '/^---/ {kind = ""} /^kind: / {kind = $$2} /^  name: kubeconfigrequests\.lodia\.io$$/ {print kind}' | sort | tr '\n' ' ')"; \
	[ "$$policies" = "ValidatingAdmissionPolicy ValidatingAdmissionPolicyBinding " ] || { echo "with crds.enabled=false the chart renders [$$policies] named kubeconfigrequests.lodia.io, want a ValidatingAdmissionPolicy and its binding"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.image.tag=v0.0.0-check | grep -q -- '--etcd-druid-version=v0.0.0-check$$' || { echo "--etcd-druid-version does not follow etcdDruid.image.tag"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidDeployment=etcd-druid > /dev/null 2>&1 || { echo "with etcdDruid.enabled=false the chart renders without etcdDruidVersion"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check > /dev/null 2>&1 || { echo "with etcdDruid.enabled=false the chart renders without etcdDruidDeployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -q 'enableEtcdSpecAutoReconcile: false$$' || { echo "etcd-druid runs with its automatic spec reconcile"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -A1 '^    leaderElection:$$' | grep -q '^      enabled: true$$' || { echo "etcd-druid runs without leader election"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -q -- '--etcd-druid-deployment=etcd-druid$$' || { echo "--etcd-druid-deployment does not name the subchart's Deployment etcd-druid"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | awk '/^---/ {kind = ""; ns = ""} /^kind: / {kind = $$2} /^  namespace: / {ns = $$2} /^    resourceNames: \[etcd-druid\]$$/ && kind == "Role" && ns == "lodia-system" {found = 1} END {exit !found}' || { echo "no Role in the release namespace lets the operator read etcd-druid's Deployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=druid-check | grep -q -- '--etcd-druid-deployment=druid-check$$' || { echo "--etcd-druid-deployment does not follow etcdDruidDeployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=druid-check | awk '/^---/ {kind = ""; ns = ""} /^kind: / {kind = $$2} /^  namespace: / {ns = $$2} /^    resourceNames: \[druid-check\]$$/ && kind == "Role" && ns == "druid" {found = 1} END {exit !found}' || { echo "no Role in etcdDruidNamespace lets the operator read etcdDruidDeployment"; exit 1; }
	helm lint --strict $(CHART) --set image.digest=$(DIGEST),signerImage.digest=$(DIGEST),tunnelImage.digest=$(DIGEST)
	helm template lodia $(CHART) --namespace lodia-system --set image.digest=$(DIGEST),signerImage.digest=$(DIGEST),tunnelImage.digest=$(DIGEST) > /dev/null
	helm template lodia $(CHART) --namespace lodia-system | awk '/^---/ {kind = ""} /^kind: / {kind = $$2} kind == "ServiceMonitor" || kind == "PrometheusRule" || kind == "Service" && /^  name: lodia-metrics$$/ {found = 1} END {exit found}' || { echo "the defaults render the Service lodia-metrics, a ServiceMonitor or a PrometheusRule"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | awk '/^---/ {kind = ""} /^kind: / {kind = $$2} kind == "ConfigMap" && /^    grafana_dashboard:/ {found = 1} END {exit found}' || { echo "the defaults render a ConfigMap labelled grafana_dashboard"; exit 1; }
	helm lint --strict $(CHART) --set $(METRICS)
	helm lint --strict $(CHART) --set $(METRICS),metrics.serviceMonitor.scrapeClass=check,metrics.serviceMonitor.labels.release=check,metrics.rules.labels.release=check,metrics.dashboards.labels.release=check
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) | awk '/^---/ {kind = ""} /^kind: / {kind = $$2} /^  name: / && (kind == "Service" && $$2 == "check-metrics" || kind == "ServiceMonitor" && $$2 == "check" || kind == "PrometheusRule" && $$2 == "check") {found[kind] = 1} END {exit !(found["Service"] && found["ServiceMonitor"] && found["PrometheusRule"])}' || { echo "with $(METRICS) the chart renders no Service check-metrics, no ServiceMonitor check or no PrometheusRule check"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set metrics.serviceMonitor.enabled=true > /dev/null 2>&1 || { echo "the ServiceMonitor renders without the metrics Service"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | grep -qx '      honorLabels: true' || { echo "the ServiceMonitor's endpoint does not honor the scraped labels, so prometheus-operator's namespace and endpoint rename Lodia's"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | grep -qx '      scheme: https' || { echo "the ServiceMonitor's endpoint does not scrape over https, which is all controller-runtime's metrics listener serves"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | grep -qx '        insecureSkipVerify: true' || { echo "the ServiceMonitor's endpoint verifies the serving certificate, which controller-runtime generates for localhost"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS),metrics.serviceMonitor.labels.release=check --show-only templates/servicemonitor.yaml | awk '/^[^ ]/ {top = $$1} /^  [^ ]/ {block = $$1} top == "metadata:" && block == "labels:" && /^    [^ ]/ {print $$1, $$2}' | grep -Fqx 'release: check' || { echo "metrics.serviceMonitor.labels does not reach the labels of the ServiceMonitor, so a Prometheus that selects ServiceMonitors by label never scrapes it"; exit 1; }
	@job="$$(helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | awk '/^  jobLabel: / {print $$2}')"; \
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/metrics-service.yaml | awk '/^[^ ]/ {top = $$1} /^  [^ ]/ {block = $$1} top == "metadata:" && block == "labels:" && /^    [^ ]/ {print $$1, $$2}' | grep -Fqx "$$job: lodia" || { echo "the ServiceMonitor's jobLabel [$$job] names no label of the Service check-metrics that holds lodia, so its job is not lodia"; exit 1; }
	@port="$$(helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | awk '/^    - port: / {print $$3}')"; \
	[ -n "$$port" ] && helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/metrics-service.yaml | awk '/^[^ ]/ {top = $$1} /^  [^ ]/ {block = $$1} top == "spec:" && block == "ports:" && /^    - name: / {print $$3}' | grep -Fqx "$$port" || { echo "the ServiceMonitor scrapes the port [$$port], which the Service check-metrics does not name"; exit 1; }
	@selector="$$(helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | awk '/^    matchLabels:/ {m = 1; next} m && /^      [^ ]/ {print $$1, $$2; next} {m = 0}')"; \
	printf '%s\n' "$$selector" | grep -Fqx 'app.kubernetes.io/component: metrics' && printf '%s\n' "$$selector" | grep -Fqx 'app.kubernetes.io/instance: check' || { echo "the ServiceMonitor's selector [$$selector] lacks app.kubernetes.io/component: metrics or app.kubernetes.io/instance: check"; exit 1; }; \
	matched="$$(helm template check $(CHART) --namespace lodia-system --set $(METRICS) | SELECTOR="$$selector" awk 'function flush(  n, i, ok, pairs) { if (kind == "Service") { ok = 1; n = split(ENVIRON["SELECTOR"], pairs, "\n"); for (i = 1; i <= n; i++) if (!(pairs[i] in labels)) ok = 0; if (ok) print name } split("", labels); kind = name = "" } /^---/ {flush()} /^kind: / {kind = $$2} /^[^ ]/ {top = $$1} /^  [^ ]/ {block = $$1} top == "metadata:" && /^  name: / {name = $$2} top == "metadata:" && block == "labels:" && /^    [^ ]/ {labels[$$1 " " $$2] = 1} END {flush()}' | tr '\n' ' ')"; \
	[ "$$matched" = "check-metrics " ] || { echo "the ServiceMonitor's selector matches the Services [$$matched], want check-metrics alone"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/servicemonitor.yaml | awk '/^      bearerTokenFile: \/var\/run\/secrets\/kubernetes.io\/serviceaccount\/token$$/ {token = 1} /scrapeClass/ {class = 1} END {exit !(token && !class)}' || { echo "by default the ServiceMonitor names a scrape class or sends no token from Prometheus's own file"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS),metrics.serviceMonitor.scrapeClass=check --show-only templates/servicemonitor.yaml | awk '/^  scrapeClass: "check"$$/ {class = 1} /bearerTokenFile/ {token = 1} END {exit !(class && !token)}' || { echo "with metrics.serviceMonitor.scrapeClass the ServiceMonitor names no such class or keeps a token file, which Prometheus refuses beside the class's authorization"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS),metrics.rules.labels.release=check --show-only templates/prometheusrule.yaml | awk '/^[^ ]/ {top = $$1} /^  [^ ]/ {block = $$1} top == "metadata:" && block == "labels:" && /^    [^ ]/ {print $$1, $$2}' | grep -Fqx 'release: check' || { echo "metrics.rules.labels does not reach the labels of the PrometheusRule, so a Prometheus that selects rules by label never loads it"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/prometheusrule.yaml | awk 'spec && $$0 != "" {print substr($$0, 3)} /^spec:$$/ {spec = 1}' | diff $(CHART)/files/rules/lodia.yaml - || { echo "the PrometheusRule of a -dev appVersion is not the rule file $(CHART)/files/rules/lodia.yaml"; exit 1; }
	helm template check $(CHART) --namespace lodia-system --set $(METRICS),metrics.dashboards.labels.release=check --show-only templates/dashboards.yaml | awk '/^kind: ConfigMap$$/ {maps++} /^    grafana_dashboard: "1"$$/ {grafana++} /^    release: check$$/ {release++} END {exit !(maps == $(words $(DASHBOARDS)) && grafana == maps && release == maps)}' || { echo "with $(METRICS) the chart renders other than a ConfigMap for each of $(DASHBOARDS), each with the label grafana_dashboard and the labels of metrics.dashboards.labels"; exit 1; }
	@for file in $(DASHBOARDS); do \
		name="$$(basename $$file .json)"; \
		helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/dashboards.yaml | awk -v want="check-dashboard-$$name" -v key="$$name.json" '/^---/ {config = 0} /^  name: / {config = ($$2 == want)} config && $$0 == "  " key ": |" {data = 1; next} data && /^    / {print substr($$0, 5); next} data {data = 0}' | diff $$file - > /dev/null || { echo "the ConfigMap check-dashboard-$$name does not hold $$file as $$name.json"; exit 1; }; \
	done
	helm package $(CHART) --version 1.2.3 --app-version 1.2.3 --destination work/chart > /dev/null
	helm template check work/chart/lodia-1.2.3.tgz --namespace lodia-system --set $(METRICS) --show-only templates/prometheusrule.yaml | awk 'spec && $$0 != "" {print substr($$0, 3)} /^spec:$$/ {spec = 1}' | sed 's|https://github.com/lodia-io/lodia/blob/v1.2.3/|https://github.com/lodia-io/lodia/blob/main/|' | diff $(CHART)/files/rules/lodia.yaml - || { echo "the PrometheusRule of appVersion 1.2.3 is not the rule file with its runbooks at the tag v1.2.3"; exit 1; }
	! helm template check work/chart/lodia-1.2.3.tgz --namespace lodia-system --set $(METRICS) --show-only templates/prometheusrule.yaml | grep -q 'https://github.com/lodia-io/lodia/blob/main/' || { echo "the PrometheusRule of appVersion 1.2.3 links a runbook at main, not at the tag v1.2.3"; exit 1; }

.PHONY: monitoring
monitoring: ## lint the collector's golden as promtool lints a scrape, check the alert rules and run their tests, and lint the dashboards
	promtool check metrics < internal/metrics/testdata/metrics.prom
	promtool check rules --lint-fatal $(CHART)/files/rules/lodia.yaml
	promtool test rules test/rules/lodia_test.yaml
	for dashboard in $(DASHBOARDS); do dashboard-linter lint --strict $$dashboard || exit 1; done

.PHONY: verify
verify: ## the check that says a change is done
	@before="$$(cat $(GENERATED) | cksum)"; $(MAKE) --no-print-directory generate; \
	[ "$$before" = "$$(cat $(GENERATED) | cksum)" ] || { echo "make generate changed the generated files: review and commit them"; exit 1; }
	go mod tidy -diff
	cd api && go mod tidy -diff
	$(MAKE) --no-print-directory chart lint monitoring test restore-keys

.PHONY: e2e
e2e: ## the e2e suite against E2E_KUBECONFIG, else against a kind cluster of its own that it deletes afterwards
ifdef E2E_KUBECONFIG
	E2E_KUBECONFIG=$(abspath $(E2E_KUBECONFIG)) go test -v -tags e2e -count=1 -timeout 30m ./test/e2e/...
else
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e -count=1 -timeout 30m ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status
endif

.PHONY: e2e-ha
e2e-ha: ## gate G2 against a highly available tenant, on a kind cluster of its own with three workers in three zones, that it deletes afterwards; E2E_HA_TEST=TestHighAvailabilityRemovedAndRestored runs the nightly test instead
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig --config test/e2e/testdata/kind-ha.yaml
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,ha -count=1 -timeout 40m -run '^$(E2E_HA_TEST)$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-restore
e2e-restore: ## gate G3, a Backup of a tenant and a restore from it, against an S3 store in the cluster, on a kind cluster of its own that it deletes afterwards
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,restore -count=1 -timeout 40m -run '^TestRestoreGate$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-gateway
e2e-gateway: ## the gateway gate: tenants behind Envoy Gateway by SNI, a node joined by hostname, pods reaching their API through apiserver-proxy, hostname ownership, on a kind cluster of its own that it deletes afterwards with the DNS container the gate starts
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,gateway -count=1 -timeout 40m -run '^TestGatewayGate$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-dns || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-gateway-g6
e2e-gateway-g6: ## gate G6, what the Gateway and the tunnel add to the API's p99 over a direct path, on a kind cluster of its own that it deletes afterwards with the DNS container the gate starts; results in $(E2E_DIR)/_artifacts
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,gateway -count=1 -timeout 40m -run '^TestGatewayOverhead$$' -artifacts -outputdir $(E2E_DIR) ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-dns || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-hold
e2e-hold: ## gate G4: both nodes of a highly available tenant behind the Gateway cut off for 10 minutes, then one of them, on a kind cluster of its own from kind-ha.yaml that it deletes afterwards with the DNS container the gate starts
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig --config test/e2e/testdata/kind-ha.yaml
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,hold -count=1 -timeout 45m -run '^TestControllerManagerHold$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-tenant-node-2 $(E2E_CLUSTER)-dns || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-upgrade
e2e-upgrade: ## gate G1, a minor upgrade under two loops, on a kind cluster of its own that it deletes afterwards; E2E_UPGRADE_TEST=TestMinorUpgradeChain runs the nightly chain instead
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,upgrade -count=1 -timeout 40m -run '^$(E2E_UPGRADE_TEST)$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-tenant-node-2 $(E2E_CLUSTER)-tenant-node-3 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-upgrade-ha
e2e-upgrade-ha: ## the nightly upgrade of a highly available tenant, on a kind cluster of its own with three workers in three zones, that it deletes afterwards
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig --config test/e2e/testdata/kind-ha.yaml
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,upgrade -count=1 -timeout 45m -run '^TestHighAvailabilityUpgrade$$' ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: e2e-maintenance
e2e-maintenance: ## gate G5, an operator upgrade that re-renders tenants only in their windows, on a kind cluster of its own that it deletes afterwards; E2E_MAINTENANCE_TEST=TestFleetLimitInQueueOrder runs the nightly fleet limit instead
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,maintenance -count=1 -timeout 40m -run '^$(E2E_MAINTENANCE_TEST)$$' ./test/e2e/...; status=$$?; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: conformance-gateway
conformance-gateway: ## the Kubernetes conformance suite through hydrophone against a tenant with two nodes behind the Gateway, on a kind cluster of its own that it deletes afterwards with the DNS container the gate starts; results in $(E2E_DIR)/_artifacts; CONFORMANCE_FOCUS=<regexp> runs only the tests it matches
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,gateway -count=1 -timeout 110m -run '^TestGatewayConformance$$' -artifacts -outputdir $(E2E_DIR) ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-tenant-node-2 $(E2E_CLUSTER)-dns || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status

.PHONY: conformance
conformance: ## the Kubernetes conformance suite through hydrophone against a tenant with two nodes, on a kind cluster of its own that it deletes afterwards; results in $(E2E_DIR)/_artifacts; CONFORMANCE_FOCUS=<regexp> runs only the tests it matches
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,conformance -count=1 -timeout 110m -run '^TestConformance$$' -artifacts -outputdir $(E2E_DIR) ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-tenant-node-2 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status
