# Build stage
FROM rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ARG DATABASE_URL
ENV DATABASE_URL=$DATABASE_URL
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/release/zns-scanner /usr/local/bin/zns-scanner
ENTRYPOINT ["/usr/local/bin/zns-scanner"]
