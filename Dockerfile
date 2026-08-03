FROM rust:1.88-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/zns-scanner /usr/local/bin/zns-scanner
ENTRYPOINT ["zns-scanner"]