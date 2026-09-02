# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26.7
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH

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

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=builder /out/mdai-fidelity-validator /mdai-fidelity-validator
EXPOSE 8080 8888

ENTRYPOINT ["/mdai-fidelity-validator"]
