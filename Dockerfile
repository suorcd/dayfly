# syntax=docker/dockerfile:1
# Build the dayfly binary and run `dayfly serve` as a non-root user.
# This image matches the flake container image: the same entrypoint,
# the same bind address, and the same user.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/dayfly /usr/local/bin/dayfly
USER 65534:65534
WORKDIR /
ENV DAYFLY_HTTP_BIND=0.0.0.0:8080
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dayfly"]
CMD ["serve"]
