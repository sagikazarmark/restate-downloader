FROM rust:1.92.0-slim@sha256:973c5191dd2613f5caa07cdfd9f94b8b0b4bbfa10bc21f0073ffc115ff04c701 AS builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./

COPY lib ./lib
COPY bin ./bin

RUN cargo build --release --bin restate-downloader


FROM alpine:3.23.0@sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375

COPY --from=builder /usr/src/app/target/release/restate-downloader /usr/local/bin/restate-downloader

ENV RUST_LOG=info

EXPOSE 9080

CMD ["restate-downloader"]
