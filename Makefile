.PHONY: build test race chaos chaos-10k bench bench-25k proto docker-up docker-down cluster clean

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

# Randomized fault injection with linearizability checking.
chaos:
	go run ./cmd/raftkvchaos -runs 500

chaos-10k:
	go run ./cmd/raftkvchaos -runs 10000

bench:
	go run ./cmd/raftkvbench -duration 15s

bench-25k:
	go run ./cmd/raftkvbench -duration 15s -rate 25000 -clients 256

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raftpb/raft.proto proto/kvpb/kv.proto proto/docpb/doc.proto

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

cluster:
	./scripts/local-cluster.sh

clean:
	rm -rf bin data chaos-results data-node*.log
