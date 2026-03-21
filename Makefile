.PHONY: build install test lint run-daemon e2e app app-dev

build:
	go build -o bin/hadrond ./cmd/hadrond
	go build -o bin/hadron ./cmd/hadron

install:
	go install ./cmd/hadrond
	go install ./cmd/hadron

test:
	go test ./...

lint:
	go vet ./...

run-daemon:
	go run ./cmd/hadrond

e2e: build
	go test -tags e2e -v ./test/e2e/...

app:
	cd cmd/hadron-app && wails build

app-dev:
	cd cmd/hadron-app && wails dev
