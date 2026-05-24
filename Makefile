.PHONY: build install go-install uninstall test test-ui lint lint-go lint-ui typecheck run-daemon e2e app app-dev

GO_PACKAGES := ./cmd/hadron ./cmd/hadron-app ./cmd/hadrond ./internal/... ./schemas/...
GO_LINT_CACHE_DIR := /tmp/hadron-go-build
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin

build:
	mkdir -p bin
	go build -o bin/hadrond ./cmd/hadrond
	go build -o bin/hadron ./cmd/hadron

install: build
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 bin/hadrond $(DESTDIR)$(BINDIR)/hadrond
	install -m 0755 bin/hadron $(DESTDIR)$(BINDIR)/hadron

go-install:
	go install ./cmd/hadrond
	go install ./cmd/hadron

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/hadrond $(DESTDIR)$(BINDIR)/hadron

test:
	go test $(GO_PACKAGES)

test-ui:
	cd cmd/hadron-app/frontend && npm run test

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

run-daemon:
	go run ./cmd/hadrond

e2e: build
	go test -tags e2e -v ./test/e2e/...

app:
	cd cmd/hadron-app && wails build

app-dev:
	cd cmd/hadron-app && wails dev
