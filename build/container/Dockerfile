FROM golang:1.19-buster as builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

ARG TARGETOS TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then apt-get update && apt-get install -y gcc-aarch64-linux-gnu; fi

COPY . .

RUN if [ "$TARGETARCH" = "arm64" ]; \
    then \
        GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags '-s'; \
    else  \
        go build -ldflags '-s'; \
    fi

FROM registry.access.redhat.com/ubi8/ubi-minimal
LABEL org.opencontainers.image.created="${BUILD_TIMESTAMP}"
LABEL org.opencontainers.image.authors="PolarStreams <info@polarstreams.io>"
LABEL org.opencontainers.image.documentation="https://github.com/polarstreams/polar/tree/main/docs"
LABEL org.opencontainers.image.vendor="PolarStreams"
LABEL org.opencontainers.image.licenses="AGPL"
LABEL org.opencontainers.image.title="PolarStreams"
LABEL org.opencontainers.image.description="Lightweight, elastic, kubernetes-native event streaming system."

WORKDIR /work/
COPY --from=builder /build/polar .

RUN mkdir /var/lib/polar

RUN chgrp -R 0 /var/lib/polar && \
    chmod -R g=u /var/lib/polar && \
    chown -R 1001:0 /var/lib/polar

USER 1001

CMD ["/work/polar"]
