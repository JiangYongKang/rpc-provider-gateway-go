.PHONY: build run test race vet fmt check

build:
	go build ./...

run:
	go run ./cmd/gateway -config config.example.json

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

check: fmt vet race
