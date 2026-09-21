#!/usr/bin/env bash
# Runs all unit tests and static checks for the campaign-level sliding
# window rate limiting + pause reason changes. Requires network access
# (or a populated Go module cache) to resolve dependencies.
set -euo pipefail

cd "$(dirname "$0")"

# Use a writable build cache if the default one isn't accessible.
if [ ! -w "$(go env GOCACHE)" ]; then
	export GOCACHE="${TMPDIR:-/tmp}/gocache"
fi

echo "==> go build ./..."
go build ./...

echo "==> go vet ./internal/manager/ ./internal/core/ ./cmd/ ./models/"
go vet ./internal/manager/ ./internal/core/ ./cmd/ ./models/

echo "==> gofmt check"
unformatted=$(gofmt -l internal/manager internal/core cmd models internal/migrations)
if [ -n "$unformatted" ]; then
	echo "gofmt needed on:" >&2
	echo "$unformatted" >&2
	exit 1
fi

echo "==> go test (with race detector) ./internal/manager/"
go test -race -count=1 -v ./internal/manager/

echo "==> go test ./..."
go test -count=1 ./...

echo "All checks passed."
