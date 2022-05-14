FROM --platform=$BUILDPLATFORM golang:1.18-buster as builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=1 go install github.com/mattn/go-sqlite3

COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build

FROM registry.access.redhat.com/ubi8/ubi-minimal
LABEL org.opencontainers.image.created="${BUILD_TIMESTAMP}"
LABEL org.opencontainers.image.authors="Barco Streams <info@barcostreams.com>"
LABEL org.opencontainers.image.documentation=""
LABEL org.opencontainers.image.vendor="Barco Streams"
LABEL org.opencontainers.image.licenses="AGPL"
LABEL org.opencontainers.image.ref.name=""
LABEL org.opencontainers.image.title="Barco Streams"
LABEL org.opencontainers.image.description="Lightweight, elastic, kubernetes-native event streaming system."

WORKDIR /work/
COPY --from=builder /build/barco .
CMD ["/work/barco"]
