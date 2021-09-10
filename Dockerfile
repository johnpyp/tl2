FROM rustlang/rust:nightly as planner
WORKDIR app
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN cargo install cargo-chef 
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rustlang/rust:nightly as cacher
WORKDIR app
RUN cargo install cargo-chef
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

FROM rustlang/rust:nightly as builder
WORKDIR app
COPY . .
# Copy over the cached dependencies
COPY --from=cacher /app/target target
COPY --from=cacher $CARGO_HOME $CARGO_HOME
RUN cargo build --release --bin tl2

FROM rustlang/rust:nightly as runtime

ARG UNAME=appuser
ARG UID=1000
ARG GID=1000

WORKDIR /app

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

COPY --from=builder /app/target/release/tl2 /app/app-bin

RUN chown -R $UNAME:$UNAME /app

USER $UNAME

CMD ["/app/app-bin", "scrape"]
