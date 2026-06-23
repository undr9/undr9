FROM rust:1.81-bookworm AS builder
WORKDIR /app

COPY . .
RUN cargo build --release -p undr9-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/undr9 undr9
WORKDIR /var/lib/undr9

COPY --from=builder /app/target/release/undr9-cli /usr/local/bin/undr9

ENV UNDR9_ROOT=/var/lib/undr9/data
LABEL org.opencontainers.image.title="undr9" \
      org.opencontainers.image.description="UNDR9 single-node graph database" \
      org.opencontainers.image.source="https://github.com/undr9/undr9-memorydb"
EXPOSE 8080
VOLUME ["/var/lib/undr9/data"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:8080/readyz >/dev/null || exit 1

USER undr9
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["undr9", "serve", "--root", "/var/lib/undr9/data", "--bind", "0.0.0.0:8080"]
