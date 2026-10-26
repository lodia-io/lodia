# Every tool runs through `go tool` (see the tool block in go.mod), except
# golangci-lint and helm, which are installed on their own, and ko and
# hydrophone, which run as `go run github.com/google/ko@<version>` and
# `go run sigs.k8s.io/hydrophone@<version>`: their dependency graphs would
# move the operator's own dependencies.

ENVTEST_K8S_VERSION ?= 1.35.0
E2E_CLUSTER ?= lodia-e2e-local
E2E_DIR ?= $(CURDIR)/work/e2e
CHART = charts/lodia
CONTROLLERS = datastore health lifecycle pki tenant workloads
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
	cd api && golangci-lint run ./...

.PHONY: test
test: ## unit and envtest tests; envtest runs a real API server and etcd
	KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test ./...
	cd api && go test ./...

.PHONY: chart
chart: ## lint and render the chart with image tags, then with image digests
	helm dependency build $(CHART)
	helm lint --strict $(CHART)
	helm template lodia $(CHART) --namespace lodia-system > /dev/null
	helm lint --strict $(CHART) --set image.digest=$(DIGEST),signerImage.digest=$(DIGEST)
	helm template lodia $(CHART) --namespace lodia-system --set image.digest=$(DIGEST),signerImage.digest=$(DIGEST) > /dev/null

.PHONY: verify
verify: ## the check that says a change is done
	@before="$$(cat $(GENERATED) | cksum)"; $(MAKE) --no-print-directory generate; \
	[ "$$before" = "$$(cat $(GENERATED) | cksum)" ] || { echo "make generate changed the generated files: review and commit them"; exit 1; }
	go mod tidy -diff
	cd api && go mod tidy -diff
	$(MAKE) --no-print-directory chart lint test

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

.PHONY: conformance
conformance: ## the Kubernetes conformance suite through hydrophone against a tenant with two nodes, on a kind cluster of its own that it deletes afterwards; results in $(E2E_DIR)/_artifacts; CONFORMANCE_FOCUS=<regexp> runs only the tests it matches
	mkdir -p $(E2E_DIR)
	go tool kind create cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig
	E2E_KUBECONFIG=$(E2E_DIR)/kubeconfig go test -v -tags e2e,conformance -count=1 -timeout 110m -run '^TestConformance$$' -artifacts -outputdir $(E2E_DIR) ./test/e2e/...; status=$$?; \
	docker rm --force --volumes $(E2E_CLUSTER)-tenant-node-1 $(E2E_CLUSTER)-tenant-node-2 || status=1; \
	go tool kind delete cluster --name $(E2E_CLUSTER) --kubeconfig $(E2E_DIR)/kubeconfig; exit $$status
