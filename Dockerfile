FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/mdai-dd-fidelity-validator ./cmd/mdai-dd-fidelity-validator

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/mdai-dd-fidelity-validator /mdai-dd-fidelity-validator
EXPOSE 8080

ENTRYPOINT ["/mdai-dd-fidelity-validator"]
