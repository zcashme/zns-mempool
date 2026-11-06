# Build stage
FROM rust:1.89-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY src ./src
ENV SQLX_OFFLINE=true
RUN cargo build --locked --release

# Runtime stage
FROM debian:bookworm-slim
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/release/zns-mempool /usr/local/bin/zns-mempool
ENTRYPOINT ["/usr/local/bin/zns-mempool"]
