FROM rust:1.35-stretch as builder

# Build deps
RUN mkdir -p /build/src
COPY Cargo.toml Cargo.lock docker/dummy.rs /build/
WORKDIR /build
RUN cp dummy.rs src/main.rs \
        && cargo build --release --locked
RUN rm src/main.rs

# Build app
COPY src /build/src
RUN cargo build --release --locked

FROM debian:stretch-slim
RUN apt update && apt install -y libssl1.1 ca-certificates
COPY --from=builder /build/target/release/sds /usr/local/bin/
CMD /usr/local/bin/sds
