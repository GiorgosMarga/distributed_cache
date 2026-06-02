# Distributed Cache

A small distributed in-memory cache written in Go and exposed over gRPC. The server supports `Get`, `Set`, `Delete`, cluster membership, and basic peer health checks. Keys are assigned with consistent hashing, and the cache supports TTL-based expiration.

## Features

- gRPC cache server with `Get`, `Set`, `Delete`, `Join`, `Leave`, and `IsAlive` RPCs
- Consistent-hash-based key placement
- Peer-to-peer cluster formation and rebalancing
- In-memory storage with TTL expiration
- Command-line client for direct cache operations
- HTTP UI for basic cluster and cache interaction
- Go benchmark entrypoints for server RPC performance

## Repository Layout

- `cmd/server` - gRPC cache server
- `cmd/cli` - CLI client for `Get` and `Set`
- `cmd/client` - simple load generator / client sample
- `cmd/ui` - web UI and cluster controller
- `internal/cache` - in-memory cache implementation
- `internal/cluster` - consistent hash ring
- `internal/transport` - gRPC transport and server logic
- `api/cachepb` - protobuf definitions
- `gen/go/cachepb` - generated Go protobuf code

## Prerequisites

- Go 1.25 or newer
- `protoc` with the Go and gRPC plugins if you need to regenerate protobuf code

## Build

```bash
make build
```

This produces `bin/cache-server`.

## Run the Server

Start a standalone node:

```bash
go run ./cmd/server -address :3000
```

Start additional nodes and connect them to an existing bootstrap node:

```bash
go run ./cmd/server -address :3001 -connectWith :3000
go run ./cmd/server -address :3002 -connectWith :3000
```

Notes:

- `-address` sets the gRPC listen address. The default is `:3000`.
- `-connectWith` accepts a comma-separated list of bootstrap peers.
- The server rebalances keys when peers join or leave.

## Use the CLI

The CLI is a lightweight gRPC client for direct cache operations.

```bash
go run ./cmd/cli set mykey myvalue 60 --addr localhost:3000
go run ./cmd/cli get mykey --addr localhost:3000
```

Flags:

- `--addr`, `-a` - cache node address, default `localhost:3000`
- `--timeout`, `-t` - request timeout, default `5s`

Note: the CLI is a minimal developer tool rather than a polished end-user interface.

## Run the UI

The UI is a separate HTTP process that talks to a cache node and can manage a local cluster.

```bash
go run ./cmd/ui \
  -backend localhost:3000 \
  -listen :8080 \
  -server-bin ./bin/cache-server
```

If `-backend` is not provided, the UI uses `CACHE_BACKEND` or falls back to `localhost:5000`.

If `-server-bin` is not provided, the UI uses `CACHE_SERVER_BIN` or `./bin/cache-server`.

The UI expects a valid server binary path if you want it to launch and manage cluster members locally.

Open:

```text
http://localhost:8080
```

## Protobuf Generation

If you change `api/cachepb/types.proto`, regenerate the Go bindings:

```bash
make proto
```

## Cleaning Up

```bash
make clean
```

This removes generated Go code under `gen/go` and the `bin` directory.

## Notes

- The project uses gRPC for all client-server communication.
- The cache is in-memory only; data is not persisted across restarts.
- The UI is optional and can be ignored if you only need the server and CLI.
