GO ?= go
VERSION ?= dev

.PHONY: build run test check release
build:
	$(GO) build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/kline-api ./cmd/kline-api
run:
	$(GO) run ./cmd/kline-api
test:
	$(GO) test -race ./...
check:
	$(GO) vet ./...
	test -z "$$($(GO) fmt ./...)"
release:
	GO="$(GO)" VERSION="$(VERSION)" sh scripts/release.sh
