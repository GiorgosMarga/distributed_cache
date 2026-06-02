# Variables
BINARY_NAME=cache-server
PROTO_SRC=api/cachepb/*.proto
GEN_OUT=gen/go
MODULE_NAME=$(shell go list -m)

.PHONY: all proto build build-ui test clean help

all: proto build

## proto: Generate Go code from .proto files
proto:
	@mkdir -p $(GEN_OUT)
	protoc --proto_path=api \
		--go_out=$(GEN_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_SRC)
	@echo "Protobuf generation complete."

## build: Build the server binary
build:
	@echo "Building binary..."
	@mkdir -p bin
	go build -o bin/$(BINARY_NAME) ./cmd/server/main.go

## build-ui: Build the browser UI binary
build-ui:
	@echo "Building UI binary..."
	@mkdir -p bin
	go build -o bin/cache-ui ./cmd/ui/main.go

## test: Run all tests
test:
	go test -v ./internal/...

## clean: Remove generated files and binaries
clean:
	rm -rf gen/go/*
	rm -rf bin/
	@echo "Cleaned up project."

## help: Show available commands
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' |  sed -e 's/^/ /'
