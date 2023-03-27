FROM lukemathwalker/cargo-chef:latest-rust-1.68.0 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin tl2

# We do not need the Rust toolchain to run the binary!
FROM rust:1.58.1 AS runtime

ARG UNAME=appuser
ARG UID=1000
ARG GID=1000

WORKDIR /app

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

COPY --from=builder /app/target/release/tl2 /app/app-bin

RUN chown -R $UNAME:$UNAME /app
USER $UNAME

ENTRYPOINT ["/app/app-bin", "scrape"]
