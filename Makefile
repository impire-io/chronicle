.PHONY: fmt tidy build test lint check snapshot

# Format all Go source (gofmt); golangci-lint's formatters also cover goimports.
fmt:
	gofmt -w .

# Keep go.mod/go.sum honest.
tidy:
	go mod tidy

build:
	go build ./...
	go build -o bin/ ./cmd/...

# The guest half of the microsandbox backend: chronicle-workload, static,
# for the microVM's linux/arm64.
workload-linux:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/chronicle-workload-linux-arm64 ./cmd/chronicle-workload

# All tests, no skips; the wire contract runs against a real embedded NATS server.
test:
	go test -race ./...

lint:
	golangci-lint run

# The one gate to run before every commit: everything green.
check: fmt tidy build test lint

# Rehearse the release locally: builds every artifact into dist/ without
# publishing anything. The real release is the v* tag (decision 0017).
snapshot:
	goreleaser release --snapshot --clean
