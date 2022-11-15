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
LABEL org.opencontainers.image.authors="Barco Streams <info@barcostreams.com>"
LABEL org.opencontainers.image.documentation="https://github.com/barcostreams/barco/tree/main/docs"
LABEL org.opencontainers.image.vendor="Barco Streams"
LABEL org.opencontainers.image.licenses="AGPL"
LABEL org.opencontainers.image.title="Barco Streams"
LABEL org.opencontainers.image.description="Lightweight, elastic, kubernetes-native event streaming system."

WORKDIR /work/
COPY --from=builder /build/barco .

RUN mkdir /var/lib/barco

RUN chgrp -R 0 /var/lib/barco && \
    chmod -R g=u /var/lib/barco && \
    chown -R 1001:0 /var/lib/barco

USER 1001

EXPOSE 9250
EXPOSE 9251
EXPOSE 9252

CMD ["/work/barco"]