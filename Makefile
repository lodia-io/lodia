# Every tool runs through `go tool` (see the tool block in go.mod), except
# golangci-lint, helm, promtool, dashboard-linter, cosign and gh, which are installed on their own,
# and ko, hydrophone, yq and crane, which run as `go run <module>@<version>`:
# their dependency graphs would move the operator's own dependencies.

ENVTEST_K8S_VERSION ?= 1.35.0
E2E_CLUSTER ?= lodia-e2e-local
E2E_DIR ?= $(CURDIR)/work/e2e
E2E_HA_TEST ?= TestHighAvailabilityGate
E2E_RESTORE_TEST ?= TestRestoreGate
E2E_UPGRADE_TEST ?= TestMinorUpgradeGate
E2E_MAINTENANCE_TEST ?= TestOperatorUpgradeInWindows
FLEET_TENANTS ?= 1000
FLEET_HANGING ?= 0.1
FLEET_DIR ?= $(CURDIR)/work/fleet
ETCD_DIR ?= $(CURDIR)/work/etcd
ETCD_BINARY = LODIA_ETCD_BINARY="$$(go run ./test/etcdbinary $(ETCD_DIR))"
CHART = charts/lodia
CONTROLLERS = datastore health lifecycle pki tenant workloads
RACE_PACKAGES = ./internal/controller/health ./internal/metrics
# The tests that count allocations or read the heap, and the three that hold the quiet of the process they share and its guard: make test runs them in a step of their own, in a process with no envtest and no manager, fails it unless each of them passed there, and leaves them out of the step of RACE_PACKAGES.
empty :=
space := $(empty) $(empty)
APART = \
	TestABlockOfTrailersIsMergedIntoAMapOfOneNameInAllocationsEqualToItsParts \
	TestABlockOfTrailersOfNamesNoAllocatorSharesIsReadInAllocationsEqualToItsParts \
	TestACutStringHoldsNoneOfTheStringItWasCutFrom \
	TestAFieldComparedOrSkippedIsNeverDecoded \
	TestAGuardSkipsOutsideTheStepAndFailsInItWhenAnEarlierTestLeftGoroutinesBeside \
	TestAKeyRepeatedByLinesKeepsTheSliceAppendGrewToInTheBytesTheAllocatorTakesForIt \
	TestAMapOfHeadersHoldsAtMostWhatItsTablesTakeAtEverySplitPointAndOneKeyOnEachSideOfIt \
	TestASliceOfPointersTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderOfEightBytes \
	TestASliceOfStringsTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderPastFiveHundredAndTwelve \
	TestAStepStartsABodyOnlyWhenTheGoroutinesOfEarlierTestsHaveEnded \
	TestATallyBuildsNoNameForAnObjectThatDoesNotQualify \
	TestATestOfTheStepWithoutEnvtestRunsBesideNoGoroutineOfTheSuite \
	TestATimeFieldHoldingAnObjectOrAnArrayIsNotATimeWhateverItsSize \
	TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadAndTrailersHTTP1 \
	TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadAndTrailersHTTP1 \
	TestAnHTTP1HeadThatAnnouncesTheNamesOfTheDearestCountWithAFullBlockOfTrailersNoneOfThemAnnouncedIsHeldAtMostHeadAndTrailersHTTP1 \
	TestAnHTTP1HeadWhoseBlockOfTrailersRepeatsKeysIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1 \
	TestAnHTTP1HeadWhoseTrailerValueIsOnSeveralLinesIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1 \
	TestIoReadAllHoldsAtMostItsChunksAndTheSliceItReturnsAtTheCut \
	TestTheHeadTheScanFindsMostWithItsTrailersIsHeldAtMostItsPartsAndTheTerm \
	TestTheLeastAnHTTP1FieldLineAddsToAHeadIsItsCopiesAndAnElementOfTheSliceSinceTheMapGrowsInSteps \
	TestTheMostAHeadOfDistinctNamesAndFieldLinesWithItsTrailersHoldsIsFoundByScanningItsAnnouncedNames \
	TestTheSizeClassesTheMapsAreBuiltFromAreTheAllocators \
	TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform \
	TestTheWorstHintIsTheDearestOfEveryHintATenantCanSetByTheBytesTheAllocatorTakesForTheSlice \
	TestWhatAPassBuildsBesideTheSamplingIsUnderSummariesAtMost \
	TestWhatASamplingKeepsRestsOnItsOwnBoundsAndNotOnWhatTheServerAnswers \
	TestWhatTheTypesKeepAtTheRawBoundsIsUnderKeptAtMost
APART_TESTS = $(subst $(space),|,$(strip $(APART)))
DASHBOARDS = $(wildcard $(CHART)/files/dashboards/*.json)
METRICS = metrics.service.enabled=true,metrics.serviceMonitor.enabled=true,metrics.rules.enabled=true,metrics.dashboards.enabled=true
GENERATED = api/v1alpha1/zz_generated.deepcopy.go api/crds/*.yaml $(CHART)/files/crds/*.yaml $(CHART)/files/rbac/*.yaml
DIGEST = sha256:0000000000000000000000000000000000000000000000000000000000000000
KO = go run github.com/google/ko@v0.19.1
YQ = go run github.com/mikefarah/yq/v4@v4.54.1
CRANE = go run github.com/google/go-containerregistry/cmd/crane@v0.22.1
IMAGE_REGISTRY = ghcr.io/lodia-io
CHART_REGISTRY = ghcr.io/lodia-io/charts
PLATFORMS = linux/amd64,linux/arm64
KO_PUSH ?= true
HELM_PUSH ?= false
RELEASE_DIR ?= work/release
IMAGES_ENV = $(RELEASE_DIR)/images.env
CHART_ENV = $(RELEASE_DIR)/chart.env
CHART_VERSION = $(patsubst v%,%,$(TAG))
GITHUB_REPO = lodia-io/lodia
RELEASE_WORKFLOW = $(GITHUB_REPO)/.github/workflows/release.yml
VERIFY_IDENTITY = --certificate-identity https://github.com/$(RELEASE_WORKFLOW)@refs/tags/$(TAG) --certificate-oidc-issuer https://token.actions.githubusercontent.com
VERIFY_ATTESTATION = -R $(GITHUB_REPO) --signer-workflow $(RELEASE_WORKFLOW) --source-ref refs/tags/$(TAG)
DRUID_SUM = $(CURDIR)/charts/etcd-druid.sha256
SHA256 = $(shell command -v sha256sum > /dev/null && echo sha256sum || echo shasum -a 256)
define check-druid-sum
cd $(1)/charts && $(SHA256) --check --strict $(DRUID_SUM) || { echo "the etcd-druid chart that helm dependency build vendored is not the one charts/etcd-druid.sha256 names"; exit 1; }
endef

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
	cd test/monitoring && golangci-lint run ./...

.PHONY: etcd
etcd: ## fetch etcd's release binary at the version etcd-wrapper runs into ETCD_DIR, after checking it against the SHA-256 versions.EtcdArchives pins, unless it is there, and print its path
	go run ./test/etcdbinary $(ETCD_DIR)

.PHONY: test
test: etcd ## unit and envtest tests, those of RACE_PACKAGES with the race detector, and the test of health that counts a tenant of 5,000 Nodes at the operator's Timeout without it; envtest runs a real API server and etcd, and the tests of Lodia's calls to etcd run the binary that make etcd fetches
	# @todo: 60 s until we know what holds an API server past SIGTERM; see todo.md.
	KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" $(ETCD_BINARY) go test $$(go list ./... | grep -vxF "$$(go list $(RACE_PACKAGES))")
	KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test -race -skip '^($(APART_TESTS))$$' $(RACE_PACKAGES)
	out="$$(mktemp)" && st="$$(mktemp)" && { LODIA_HEALTH_TEST_WITHOUT_ENVTEST=1 go test -race -count=1 -v -run '^($(APART_TESTS))$$' ./internal/controller/health; echo $$? > "$$st"; } 2>&1 | tee "$$out"; \
	status="$$(cat "$$st")"; [ -n "$$status" ] || status=1; \
	for t in $(APART); do grep -q "^--- PASS: $$t (" "$$out" || { echo "$$t did not pass in the step without envtest"; status=1; }; done; \
	rm -f "$$out" "$$st"; exit $$status
	KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test -count=1 -run '^TestATenantOfMaxNodesNodesIsCountedWholeAtTheOperatorsTimeout$$' ./internal/controller/health
	cd api && go test ./...

.PHONY: fleet
fleet: etcd ## the operator against a synthetic fleet of FLEET_TENANTS ControlPlanes on envtest, FLEET_HANGING of whose tenants hang in one phase; the report and the logs go to FLEET_DIR (docs/operations.md, Scale)
	mkdir -p $(FLEET_DIR)
	KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" $(ETCD_BINARY) LODIA_FLEET_TENANTS=$(FLEET_TENANTS) LODIA_FLEET_HANGING=$(FLEET_HANGING) LODIA_FLEET_REPORT=$(FLEET_DIR)/report.txt \
		go test -count=1 -v -timeout 4h -run '^TestFleet$$' ./test/fleet

.PHONY: restore-keys
restore-keys: ## the etcd keys a restore's cleanup deletes, against the API server of each minor Lodia runs
	@minors="$$(go run ./test/restorekeys/minors)" && [ -n "$$minors" ] || { echo "no minors to run"; exit 1; }; \
	for minor in $$minors; do \
		assets="$$(go tool setup-envtest use $$minor -p path)" && \
		KUBEBUILDER_ASSETS="$$assets" RESTORE_KEYS_MINOR="$$minor" go test -count=1 -v ./test/restorekeys || exit 1; \
	done

.PHONY: chart
chart: ## check the vendored etcd-druid chart against charts/etcd-druid.sha256, lint and render the chart with image tags, then with image digests; with crds.enabled=false it must still render the policy kubeconfigrequests.lodia.io and its binding; the metrics Service and ServiceMonitor only once their values are on, rendered as the release check, and the ServiceMonitor scraping that Service alone over https, with the labels it is given; the PrometheusRule only once its value is on, with the labels it is given, holding the rule file with every runbook at the release's tag unless appVersion ends in -dev; the dashboards only once their value is on, a ConfigMap each, holding its file as lodia-<file>, with the labels it is given
	helm dependency build $(CHART)
	$(call check-druid-sum,$(CHART))
	helm lint --strict $(CHART)
	helm template lodia $(CHART) --namespace lodia-system > /dev/null
	@policies="$$(helm template lodia $(CHART) --namespace lodia-system --set crds.enabled=false | awk '/^---/ {kind = ""} /^kind: / {kind = $$2} /^  name: kubeconfigrequests\.lodia\.io$$/ {print kind}' | sort | tr '\n' ' ')"; \
	[ "$$policies" = "ValidatingAdmissionPolicy ValidatingAdmissionPolicyBinding " ] || { echo "with crds.enabled=false the chart renders [$$policies] named kubeconfigrequests.lodia.io, want a ValidatingAdmissionPolicy and its binding"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.image.tag=v0.0.0-check | grep -q -- '--etcd-druid-version=v0.0.0-check$$' || { echo "--etcd-druid-version does not follow etcdDruid.image.tag"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidDeployment=etcd-druid,etcdDruidStatusSyncPeriod=9s > /dev/null 2>&1 || { echo "with etcdDruid.enabled=false the chart renders without etcdDruidVersion"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidStatusSyncPeriod=9s > /dev/null 2>&1 || { echo "with etcdDruid.enabled=false the chart renders without etcdDruidDeployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.operatorConfig.controllers.etcd.etcdStatusSyncPeriod=7s | grep -q -- '--etcd-druid-status-sync-period=7s$$' || { echo "--etcd-druid-status-sync-period does not follow etcdDruid.operatorConfig.controllers.etcd.etcdStatusSyncPeriod"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.operatorConfig.controllers.etcd.etcdStatusSyncPeriod=7s | grep -q 'etcdStatusSyncPeriod: 7s$$' || { echo "etcd-druid does not sync status at etcdDruid.operatorConfig.controllers.etcd.etcdStatusSyncPeriod"; exit 1; }
	! helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=etcd-druid > /dev/null 2>&1 || { echo "with etcdDruid.enabled=false the chart renders without etcdDruidStatusSyncPeriod"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=etcd-druid,etcdDruidStatusSyncPeriod=9s | grep -q -- '--etcd-druid-status-sync-period=9s$$' || { echo "--etcd-druid-status-sync-period does not follow etcdDruidStatusSyncPeriod"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -q 'enableEtcdSpecAutoReconcile: false$$' || { echo "etcd-druid runs with its automatic spec reconcile"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -A1 '^    leaderElection:$$' | grep -q '^      enabled: true$$' || { echo "etcd-druid runs without leader election"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | grep -q -- '--etcd-druid-deployment=etcd-druid$$' || { echo "--etcd-druid-deployment does not name the subchart's Deployment etcd-druid"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system | awk '/^---/ {kind = ""; ns = ""} /^kind: / {kind = $$2} /^  namespace: / {ns = $$2} /^    resourceNames: \[etcd-druid\]$$/ && kind == "Role" && ns == "lodia-system" {found = 1} END {exit !found}' || { echo "no Role in the release namespace lets the operator read etcd-druid's Deployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=druid-check,etcdDruidStatusSyncPeriod=9s | grep -q -- '--etcd-druid-deployment=druid-check$$' || { echo "--etcd-druid-deployment does not follow etcdDruidDeployment"; exit 1; }
	helm template lodia $(CHART) --namespace lodia-system --set etcdDruid.enabled=false,etcdDruidNamespace=druid,etcdDruidVersion=v0.0.0-check,etcdDruidDeployment=druid-check,etcdDruidStatusSyncPeriod=9s | awk '/^---/ {kind = ""; ns = ""} /^kind: / {kind = $$2} /^  namespace: / {ns = $$2} /^    resourceNames: \[druid-check\]$$/ && kind == "Role" && ns == "druid" {found = 1} END {exit !found}' || { echo "no Role in etcdDruidNamespace lets the operator read etcdDruidDeployment"; exit 1; }
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
		helm template check $(CHART) --namespace lodia-system --set $(METRICS) --show-only templates/dashboards.yaml | awk -v want="check-dashboard-$$name" -v key="lodia-$$name.json" '/^---/ {config = 0} /^  name: / {config = ($$2 == want)} config && $$0 == "  " key ": |" {data = 1; next} data && /^    / {print substr($$0, 5); next} data {data = 0}' | diff $$file - > /dev/null || { echo "the ConfigMap check-dashboard-$$name does not hold $$file as lodia-$$name.json"; exit 1; }; \
	done
	helm package $(CHART) --version 1.2.3 --app-version 1.2.3 --destination work/chart > /dev/null
	helm template check work/chart/lodia-1.2.3.tgz --namespace lodia-system --set $(METRICS) --show-only templates/prometheusrule.yaml | awk 'spec && $$0 != "" {print substr($$0, 3)} /^spec:$$/ {spec = 1}' | sed 's|https://github.com/lodia-io/lodia/blob/v1.2.3/|https://github.com/lodia-io/lodia/blob/main/|' | diff $(CHART)/files/rules/lodia.yaml - || { echo "the PrometheusRule of appVersion 1.2.3 is not the rule file with its runbooks at the tag v1.2.3"; exit 1; }
	! helm template check work/chart/lodia-1.2.3.tgz --namespace lodia-system --set $(METRICS) --show-only templates/prometheusrule.yaml | grep -q 'https://github.com/lodia-io/lodia/blob/main/' || { echo "the PrometheusRule of appVersion 1.2.3 links a runbook at main, not at the tag v1.2.3"; exit 1; }

.PHONY: monitoring
monitoring: ## lint the collector's golden as promtool lints a scrape, check the alert rules and run their tests, lint the dashboards, and check that every metric they and the rules read is one Lodia serves
	promtool check metrics < internal/metrics/testdata/metrics.prom
	promtool check rules --lint-fatal $(CHART)/files/rules/lodia.yaml
	promtool test rules test/rules/lodia_test.yaml
	for dashboard in $(DASHBOARDS); do dashboard-linter lint --strict $$dashboard || exit 1; done
	cd test/monitoring && go test -count=1 ./...

.PHONY: release-login
release-login: ## log in to the registry of IMAGE_REGISTRY as $$GITHUB_ACTOR with $$GITHUB_TOKEN, ko for the images and helm for the chart
	@printf '%s' "$$GITHUB_TOKEN" | $(KO) login $(firstword $(subst /, ,$(IMAGE_REGISTRY))) --username "$$GITHUB_ACTOR" --password-stdin
	@printf '%s' "$$GITHUB_TOKEN" | helm registry login $(firstword $(subst /, ,$(CHART_REGISTRY))) --username "$$GITHUB_ACTOR" --password-stdin

.PHONY: release-images
release-images: ## build the three images with ko for PLATFORMS, tagged CHART_VERSION, pushed unless KO_PUSH=false, and write the reference of each by its digest to $(IMAGES_ENV); each digest is the one ko printed for the repository of that image, whatever the order of ko's output
	@[ -n "$(TAG)" ] || { echo "usage: make release-images TAG=vX.Y.Z [KO_PUSH=false]"; exit 1; }
	mkdir -p $(RELEASE_DIR)
	rm -f $(IMAGES_ENV) $(CHART_ENV)
	refs="$$(VERSION=$(TAG) KO_DOCKER_REPO=$(IMAGE_REGISTRY) $(KO) build --push=$(KO_PUSH) --base-import-paths --platform=$(PLATFORMS) --tags=$(CHART_VERSION) ./cmd/lodia ./cmd/lodia-signer ./cmd/lodia-tunnel)" && \
	digest() { printf '%s\n' "$$refs" | awk -v repository="$(IMAGE_REGISTRY)/$$1" '{ at = index($$0, "@"); name = substr($$0, 1, at - 1); sub(/:[^\/]*$$/, "", name); if (name == repository) print substr($$0, at + 1) }'; } && \
	for entry in LODIA_IMAGE:lodia SIGNER_IMAGE:lodia-signer TUNNEL_IMAGE:lodia-tunnel; do \
		digest="$$(digest $${entry#*:})"; \
		[ "$$(printf '%s\n' "$$digest" | grep -c .)" = 1 ] && printf '%s\n' "$$digest" | grep -Eqx 'sha256:[0-9a-f]{64}' || { echo "ko printed [$$digest] for $(IMAGE_REGISTRY)/$${entry#*:}, want one digest" >&2; exit 1; }; \
		echo "$${entry%:*}=$(IMAGE_REGISTRY)/$${entry#*:}@$$digest"; \
	done > $(IMAGES_ENV).tmp && mv $(IMAGES_ENV).tmp $(IMAGES_ENV)

.PHONY: release-chart
release-chart: ## package the chart as version and appVersion CHART_VERSION into $(RELEASE_DIR)/lodia-CHART_VERSION.tgz, with the digests of $(IMAGES_ENV) in its values.yaml, after checking the vendored etcd-druid chart; its values.yaml must differ from the tree's in the three digest lines alone; HELM_PUSH=true pushes it to oci://$(CHART_REGISTRY) and writes $(CHART_ENV) with its reference by the digest helm push prints
	@[ -n "$(TAG)" ] || { echo "usage: make release-chart TAG=vX.Y.Z [HELM_PUSH=true]"; exit 1; }
	@[ -f $(IMAGES_ENV) ] || { echo "$(IMAGES_ENV) is missing: run make release-images first"; exit 1; }
	rm -rf $(RELEASE_DIR)/chart $(CHART_ENV)
	mkdir -p $(RELEASE_DIR)/chart
	cp -R $(CHART) $(RELEASE_DIR)/chart/lodia
	rm -rf $(RELEASE_DIR)/chart/lodia/charts
	helm dependency build $(RELEASE_DIR)/chart/lodia
	$(call check-druid-sum,$(RELEASE_DIR)/chart/lodia)
	. $(IMAGES_ENV) && values=$(RELEASE_DIR)/chart/lodia/values.yaml && \
	for entry in image:$${LODIA_IMAGE##*@} signerImage:$${SIGNER_IMAGE##*@} tunnelImage:$${TUNNEL_IMAGE##*@}; do \
		line="$$($(YQ) ".$${entry%%:*}.digest | line" $$values)" && sed -i.bak "$${line}s|\"\"|\"$${entry#*:}\"|" $$values || exit 1; \
	done && rm $$values.bak
	helm package $(RELEASE_DIR)/chart/lodia --version $(CHART_VERSION) --app-version $(CHART_VERSION) --destination $(RELEASE_DIR)
	. $(IMAGES_ENV) && \
	want="$$(printf '<   digest: ""\n>   digest: "%s"\n' "$${LODIA_IMAGE##*@}" "$${SIGNER_IMAGE##*@}" "$${TUNNEL_IMAGE##*@}" | sort)" && \
	got="$$(tar -xzOf $(RELEASE_DIR)/lodia-$(CHART_VERSION).tgz lodia/values.yaml | diff $(CHART)/values.yaml - | grep '^[<>]' | sort)"; \
	[ "$$got" = "$$want" ] || { printf 'the packaged values.yaml differs from %s/values.yaml in\n%s\nwant\n%s\n' $(CHART) "$$got" "$$want"; exit 1; }
	@if [ "$(HELM_PUSH)" = true ]; then \
		out="$$(helm push $(RELEASE_DIR)/lodia-$(CHART_VERSION).tgz oci://$(CHART_REGISTRY))" && printf '%s\n' "$$out" && \
		pushed="$$(printf '%s\n' "$$out" | sed -n 's/^Pushed: //p')" && digest="$$(printf '%s\n' "$$out" | sed -n 's/^Digest: //p')" && \
		[ "$$pushed" = $(CHART_REGISTRY)/lodia:$(CHART_VERSION) ] && [ "$$(printf '%s\n' "$$digest" | grep -c .)" = 1 ] && printf '%s\n' "$$digest" | grep -Eqx 'sha256:[0-9a-f]{64}' || { echo "helm push printed pushed [$$pushed] and digest [$$digest], want $(CHART_REGISTRY)/lodia:$(CHART_VERSION) and one digest"; exit 1; }; \
		echo "CHART_IMAGE=$(CHART_REGISTRY)/lodia@$$digest" > $(CHART_ENV); \
	fi

.PHONY: release-render-check
release-render-check: ## fail unless the default render of CHART, a packaged chart, names LODIA_IMAGE as the operator container's image, SIGNER_IMAGE as --signer-image and TUNNEL_IMAGE as --tunnel-image, all of $(IMAGES_ENV), and no other ghcr.io/lodia-io reference
	@[ "$(CHART)" != charts/lodia ] || { echo "usage: make release-render-check CHART=<packaged chart>"; exit 1; }
	. $(IMAGES_ENV) && \
	want="$$(printf '%s\n' "image: $$LODIA_IMAGE" "- --signer-image=$$SIGNER_IMAGE" "- --tunnel-image=$$TUNNEL_IMAGE" | sort)" && \
	got="$$(helm template lodia $(CHART) --namespace lodia-system | grep 'ghcr\.io/lodia-io/' | sed 's/^[[:space:]]*//' | sort)"; \
	[ "$$got" = "$$want" ] || { printf 'the default render of %s has the lines\n%s\nwant\n%s\n' $(CHART) "$$got" "$$want"; exit 1; }

.PHONY: release-pull
release-pull: ## pull the three images of $(IMAGES_ENV), every platform of each, and the chart of $(CHART_ENV) into $(RELEASE_DIR)/pull with DOCKER_CONFIG and HELM_REGISTRY_CONFIG both empty, as an anonymous user does, then run release-render-check on the pulled chart
	rm -rf $(RELEASE_DIR)/pull
	mkdir -p $(RELEASE_DIR)/pull/images $(RELEASE_DIR)/pull/chart
	. $(IMAGES_ENV) && . $(CHART_ENV) && \
	export DOCKER_CONFIG="$$(mktemp -d)" HELM_REGISTRY_CONFIG="$$(mktemp -d)/config.json" && \
	for image in $$LODIA_IMAGE $$SIGNER_IMAGE $$TUNNEL_IMAGE; do \
		name="$${image%@*}"; $(CRANE) pull --format oci $$image $(RELEASE_DIR)/pull/images/$${name##*/} || exit 1; \
	done && \
	helm pull oci://$$CHART_IMAGE --destination $(RELEASE_DIR)/pull/chart
	set -- $(RELEASE_DIR)/pull/chart/*.tgz && [ $$# = 1 ] && $(MAKE) --no-print-directory release-render-check CHART=$$1

.PHONY: release-sign
release-sign: ## sign keyless, with the workflow's OIDC token, each image of $(IMAGES_ENV) by its digest, its index and every image the index lists, and the chart of $(CHART_ENV) by its digest
	. $(IMAGES_ENV) && . $(CHART_ENV) && \
	for image in $$LODIA_IMAGE $$SIGNER_IMAGE $$TUNNEL_IMAGE; do cosign sign --yes --recursive $$image || exit 1; done && \
	cosign sign --yes $$CHART_IMAGE

.PHONY: release-verify-signatures
release-verify-signatures: ## run what a user runs for TAG, with DOCKER_CONFIG empty and the signer pinned to release.yml at refs/tags/TAG: cosign verify of each image's index by its digest, of each image that the index lists, and of the chart by its digest
	. $(IMAGES_ENV) && . $(CHART_ENV) && \
	export DOCKER_CONFIG="$$(mktemp -d)" && \
	for image in $$LODIA_IMAGE $$SIGNER_IMAGE $$TUNNEL_IMAGE; do \
		cosign verify $(VERIFY_IDENTITY) $$image || exit 1; \
		digests="$$($(CRANE) manifest $$image | jq -r '.manifests[].digest')" && [ -n "$$digests" ] || exit 1; \
		for digest in $$digests; do cosign verify $(VERIFY_IDENTITY) $${image%@*}@$$digest || exit 1; done; \
	done && \
	cosign verify $(VERIFY_IDENTITY) $$CHART_IMAGE

.PHONY: release-verify-attestations
release-verify-attestations: ## run what a user runs for TAG, with the repository, release.yml and refs/tags/TAG pinned: gh attestation verify of each image and of the chart by its digest; gh reads $$GH_TOKEN
	. $(IMAGES_ENV) && . $(CHART_ENV) && \
	for image in $$LODIA_IMAGE $$SIGNER_IMAGE $$TUNNEL_IMAGE $$CHART_IMAGE; do gh attestation verify oci://$$image $(VERIFY_ATTESTATION) || exit 1; done

# RELEASE_DIR of its own, so that verify never leaves the digests of an image it did not push where a release's release-chart reads them.
.PHONY: verify-release
verify-release: TAG = v0.0.0-verify
verify-release: RELEASE_DIR = work/verify-release
verify-release: ## the release's images built with no push, its chart packaged with their digests and the render check of that chart
	$(MAKE) --no-print-directory release-images TAG=$(TAG) KO_PUSH=false RELEASE_DIR=$(RELEASE_DIR)
	$(MAKE) --no-print-directory release-chart TAG=$(TAG) RELEASE_DIR=$(RELEASE_DIR)
	$(MAKE) --no-print-directory release-render-check CHART=$(RELEASE_DIR)/lodia-$(CHART_VERSION).tgz RELEASE_DIR=$(RELEASE_DIR)

.PHONY: verify
verify: ## the check that says a change is done
	@before="$$(cat $(GENERATED) | cksum)"; $(MAKE) --no-print-directory generate || exit 1; \
	[ "$$before" = "$$(cat $(GENERATED) | cksum)" ] || { echo "make generate changed the generated files: review and commit them"; exit 1; }
	go mod tidy -diff
	cd api && go mod tidy -diff
	cd test/monitoring && go mod tidy -diff
	$(MAKE) --no-print-directory chart verify-release lint monitoring test restore-keys

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
e2e-restore: ## gate G3, a Backup of a tenant and a restore from it, against an S3 store in the cluster, on a kind cluster of its own that it deletes afterwards; E2E_RESTORE_TEST=TestOneMemberRestoredThroughDeltas runs the nightly restore of a one-member tenant instead
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,restore -count=1 -timeout 40m -run '^$(E2E_RESTORE_TEST)$$' ./test/e2e/...; status=$$?; \
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
e2e-maintenance: ## gate G5, an operator upgrade that re-renders tenants only in their windows, on a kind cluster of its own that it deletes afterwards; E2E_MAINTENANCE_TEST=TestFleetLimitInQueueOrder or TestEtcdDruidPatchUpgradeUnderAHeldTenant runs a nightly instead
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

COMMIT ?= HEAD

.PHONY: release-check
release-check: ## refuse TAG before it is pushed, naming every rule it breaks: a semver tag above every published release of its line, COMMIT on origin/main with ci.yml and every nightly green at it, the local tags TAG and api/TAG at COMMIT, the chart at 0.0.0-dev, README.md and docs/README.md naming `--version` of TAG; reads GitHub with $$GITHUB_TOKEN, else with gh's
	@[ -n "$(TAG)" ] || { echo "usage: make release-check TAG=vX.Y.Z [COMMIT=<sha>]"; exit 1; }
	git fetch --quiet --tags origin main
	GITHUB_TOKEN="$${GITHUB_TOKEN:-$$(gh auth token)}" go run ./hack/releasecheck tag --commit="$(COMMIT)" "$(TAG)"
