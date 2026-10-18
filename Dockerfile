# syntax=docker/dockerfile:1
# Build: docker build -t gnmic-rs .
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto proto
COPY src src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bins \
    && mkdir -p /out/bin /out/data \
    && cp target/release/gnmic-rs target/release/dialout-sim /out/bin/

# distroless/cc: glibc, CA certificates and tzdata; no shell, runs as non-root.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/bin/ /usr/local/bin/
# Writable data directory for file outputs (uid 65532 = nonroot).
COPY --from=build --chown=65532:65532 /out/data /var/lib/gnmic-rs
EXPOSE 57400
ENTRYPOINT ["/usr/local/bin/gnmic-rs"]
CMD ["--config", "/etc/gnmic-rs/gnmic.yaml"]
