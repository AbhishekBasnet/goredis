BINARY := bin/goredis
PKG    := ./cmd/goredis

.PHONY: build run test race vet fmt bench fuzz compat docker demo clean

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

fmt:
		test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

bench:
		go test -run '^$$' -bench=. -benchmem ./...

fuzz:
		go test -run '^$$' -fuzz=FuzzReadCommand -fuzztime=1m ./internal/resp

compat: build
		./scripts/compat.sh

docker:
		docker build -t goredis .

clean:
		rm -rf bin

demo:
		CGO_ENABLED=0 go build -o $(BINARY) $(PKG)
		docker build -t goredis-demo scripts/demo
		docker run --rm -v "$$PWD:/vhs" goredis-demo scripts/demo/demo.tape
		docker run --rm -v "$$PWD/assets:/assets" --entrypoint chown goredis-demo "$$(id -u):$$(id -g)" /assets/demo.gif
