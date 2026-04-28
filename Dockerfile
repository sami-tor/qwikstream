FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/qwhyper /usr/local/bin/qwhyper
ENTRYPOINT ["qwhyper"]
