.PHONY: build install go-install uninstall test test-ui test-ui-e2e lint lint-go lint-ui typecheck run-daemon e2e frontend-build generate app app-dev package-release

GO_PACKAGES := ./cmd/hadron ./cmd/hadron-app ./cmd/hadrond ./internal/... ./schemas/...
GO_LINT_CACHE_DIR := /tmp/hadron-go-build
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X 'main.version=$(VERSION)' -X 'main.commit=$(COMMIT)' -X 'main.buildDate=$(BUILD_DATE)'

build: frontend-build
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/hadrond ./cmd/hadrond
	go build -ldflags "$(LDFLAGS)" -o bin/hadron ./cmd/hadron
	go build -ldflags "$(LDFLAGS)" -o bin/hadron-app ./cmd/hadron-app

install: build
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 bin/hadrond $(DESTDIR)$(BINDIR)/hadrond
	install -m 0755 bin/hadron $(DESTDIR)$(BINDIR)/hadron
	install -m 0755 bin/hadron-app $(DESTDIR)$(BINDIR)/hadron-app

go-install: frontend-build
	go install ./cmd/hadrond
	go install ./cmd/hadron
	go install ./cmd/hadron-app

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/hadrond $(DESTDIR)$(BINDIR)/hadron $(DESTDIR)$(BINDIR)/hadron-app

test:
	go test $(GO_PACKAGES)

generate:
	go generate ./internal/api

test-ui:
	cd cmd/hadron-app/frontend && npm run test

test-ui-e2e:
	cd cmd/hadron-app/frontend && npm run test:e2e

lint: lint-go lint-ui

lint-go:
	mkdir -p $(GO_LINT_CACHE_DIR)
	GOCACHE=$(GO_LINT_CACHE_DIR) go vet $(GO_PACKAGES)
	GOCACHE=$(GO_LINT_CACHE_DIR) golangci-lint run --max-issues-per-linter=0 --max-same-issues=0
	GOCACHE=$(GO_LINT_CACHE_DIR) staticcheck $(GO_PACKAGES)
	GOCACHE=$(GO_LINT_CACHE_DIR) errcheck $(GO_PACKAGES)
	GOCACHE=$(GO_LINT_CACHE_DIR) govulncheck $(GO_PACKAGES)

lint-ui:
	cd cmd/hadron-app/frontend && npm run lint
	cd cmd/hadron-app/frontend && npm run lint:eslint

typecheck:
	cd cmd/hadron-app/frontend && npm run typecheck

frontend-build:
	cd cmd/hadron-app/frontend && npm run build

run-daemon:
	go run ./cmd/hadrond

e2e: build
	go test -tags e2e -v ./test/e2e/...

app: build

app-dev:
	cd cmd/hadron-app/frontend && npm run dev

package-release: frontend-build
	mkdir -p dist
	for target in darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do \
		os=$${target%/*}; \
		arch=$${target#*/}; \
		stage="dist/hadron_$(VERSION)_$${os}_$${arch}"; \
		archive="dist/hadron_$(VERSION)_$${os}_$${arch}.tar.gz"; \
		rm -rf "$$stage" "$$archive"; \
		mkdir -p "$$stage"; \
		CGO_ENABLED=0 GOOS="$$os" GOARCH="$$arch" go build -ldflags "$(LDFLAGS)" -o "$$stage/hadron" ./cmd/hadron; \
		CGO_ENABLED=0 GOOS="$$os" GOARCH="$$arch" go build -ldflags "$(LDFLAGS)" -o "$$stage/hadrond" ./cmd/hadrond; \
		CGO_ENABLED=0 GOOS="$$os" GOARCH="$$arch" go build -ldflags "$(LDFLAGS)" -o "$$stage/hadron-app" ./cmd/hadron-app; \
		cp README.md LICENSE "$$stage/"; \
		tar -C dist -czf "$$archive" "$$(basename "$$stage")"; \
	done
	cd dist && shasum -a 256 hadron_$(VERSION)_*.tar.gz > checksums.txt
