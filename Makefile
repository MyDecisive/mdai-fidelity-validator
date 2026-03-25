DOCKER_TAG ?= 0.1.0
CHART_VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
REPO_NAME := $(shell basename -s .git `git config --get remote.origin.url`)
BUILD_PLATFORMS ?= linux/arm64,linux/amd64
GOTOOLCHAIN ?= go1.25.0
GO := CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST := $(GO) test -count=1

docker-login docker-build docker-push kind-load: AWS_ECR_REPO := public.ecr.aws/decisiveai
docker-build docker-push kind-load: DOCKER_IMAGE := $(AWS_ECR_REPO)/$(REPO_NAME):$(DOCKER_TAG)

.PHONY: docker-login
docker-login:
	aws ecr-public get-login-password | docker login --username AWS --password-stdin $(AWS_ECR_REPO)

.PHONY: docker-build
docker-build: tidy vendor
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --load

.PHONY: docker-push
docker-push: tidy vendor docker-login
	docker buildx build --platform $(BUILD_PLATFORMS) -t $(DOCKER_IMAGE) . --push

.PHONY: build
build: tidy vendor
	$(GO) build -trimpath -ldflags="-w -s" -o mdai-fidelity-validator ./cmd/mdai-fidelity-validator

.PHONY: test
test: tidy vendor
	$(GO_TEST) ./...

.PHONY: testv
testv: tidy vendor
	$(GO_TEST) -v ./...

.PHONY: cover
cover: tidy vendor
	$(GO_TEST) -cover ./...

.PHONY: coverv
coverv: tidy vendor
	$(GO_TEST) -v -cover ./...

.PHONY: coverhtml
coverhtml:
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

.PHONY: clean-coverage
clean-coverage:
	@rm -f coverage.out coverage.html

.PHONY: tidy
tidy:
	@$(GO) mod tidy

.PHONY: tidy-check
tidy-check: tidy
	@$(GO) mod tidy -diff

.PHONY: vendor
vendor:
	@$(GO) mod vendor

.PHONY: kind-load
kind-load:
	kind load docker-image $(DOCKER_IMAGE)

.PHONY: restart
restart:
	kubectl rollout -n mdai restart deployment/$(REPO_NAME)

.PHONY: reload
reload: kind-load restart

.PHONY: helm-package
helm-package: CHART_DIR := ./deployment
helm-package:
	@echo "📦 Packaging Helm chart..."
	@helm package -u --version $(CHART_VERSION) --app-version $(CHART_VERSION) $(CHART_DIR) > /dev/null
