# syntax=docker/dockerfile:1
LABEL org.opencontainers.image.source https://github.com/mydecisive/mdai-fidelity-validator
ARG GO_VERSION=1.25
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY --link go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY --link . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s" -o /out/mdai-fidelity-validator ./cmd/mdai-fidelity-validator

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/mdai-fidelity-validator /mdai-fidelity-validator
EXPOSE 8080

ENTRYPOINT ["/mdai-fidelity-validator"]
