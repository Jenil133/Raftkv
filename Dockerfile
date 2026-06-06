# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN for cmd in raftkvd raftkvctl raftkvbench raftkvchaos; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$cmd ./cmd/$cmd || exit 1; \
    done

FROM alpine:3.20
RUN adduser -D -u 10001 raftkv && mkdir -p /data && chown raftkv /data
COPY --from=build /out/ /usr/local/bin/
USER raftkv
VOLUME /data
# 7000: gRPC (Raft, KV and document APIs). 9100: /metrics, /healthz, /readyz.
EXPOSE 7000 9100
ENTRYPOINT ["raftkvd"]
