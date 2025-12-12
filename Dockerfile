FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0@sha256:c64defb9ed5a91eacb37f96ccc3d4cd72521c4bd18d5442905b95e2226b0e707 AS xx

FROM --platform=$BUILDPLATFORM rust:1.92.0-slim@sha256:973c5191dd2613f5caa07cdfd9f94b8b0b4bbfa10bc21f0073ffc115ff04c701 AS builder

COPY --from=xx / /

RUN apt-get update && apt-get install -y clang lld

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY bin/Cargo.toml ./bin/
COPY lib/Cargo.toml ./lib/

RUN mkdir -p bin/src && echo "fn main() {}" > bin/src/main.rs
RUN mkdir -p lib/src && echo "// dummy" > lib/src/lib.rs

RUN cargo fetch --locked

ARG TARGETPLATFORM

RUN xx-apt-get update && \
    xx-apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    pkg-config

COPY . ./

RUN xx-cargo build --release --bin restate-downloader
RUN xx-verify ./target/$(xx-cargo --print-target-triple)/release/restate-downloader
RUN cp -r ./target/$(xx-cargo --print-target-triple)/release/restate-downloader /usr/local/bin/restate-downloader


FROM alpine:3.23.0@sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375

COPY --from=builder /usr/local/bin/restate-downloader /usr/local/bin/restate-downloader

ENV RUST_LOG=info

EXPOSE 9080

CMD ["restate-downloader"]
