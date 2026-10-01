ARG BASE_IMAGE_REGISTRY=cgr.dev
ARG BUILDPLATFORM
FROM --platform=$BUILDPLATFORM $BASE_IMAGE_REGISTRY/chainguard/go:latest AS builder
ARG TARGETARCH

WORKDIR /workspace/ax
# The sibling AX module owns and builds its private Guest runtime.
COPY ax/ /workspace/ax/
RUN --mount=type=cache,target=/root/go/pkg/mod,rw \
    --mount=type=cache,target=/root/.cache/go-build,rw \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -mod=readonly -trimpath -o /kagent-sandbox-guest ./cmd/ax-sandbox-guest

FROM alpine:3.22

RUN apk add --no-cache bash ca-certificates git && \
    mkdir -p /data/workspace /data/guest-logs && \
    chown -R 65532:65532 /data

COPY --from=builder /kagent-sandbox-guest /usr/local/bin/kagent-sandbox-guest
WORKDIR /data/workspace
USER 65532:65532
EXPOSE 80

ARG VERSION
LABEL org.opencontainers.image.source=https://github.com/kagent-dev/kagent
LABEL org.opencontainers.image.description="Process and file guest for standalone kagent sandboxes."
LABEL org.opencontainers.image.version="$VERSION"

ENTRYPOINT ["/usr/local/bin/kagent-sandbox-guest"]
CMD ["--listen=:80", "--workspace=/data/workspace", "--log-dir=/data/guest-logs"]
