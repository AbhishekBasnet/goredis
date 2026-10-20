BINARY := bin/goredis
PKG    := ./cmd/goredis

.PHONY: build run test race vet bench clean

build:
		go build -o $(BINARY) $(PKG)

run:
		go run $(PKG)

test:
		go test ./...

race:
		go test -race ./...

vet:
		go vet ./...

bench:
		go test -bench=. -benchmem ./...

clean:
		rm -rf bin