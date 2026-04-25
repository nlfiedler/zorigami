#
# build the application binaries
#
FROM rust:latest AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install clang
WORKDIR /build
COPY Cargo.toml .
COPY database database/
COPY server server/
COPY stores stores/
RUN cargo build --workspace --release

#
# build the healthcheck binary
#
FROM rust:latest AS healthy
WORKDIR /health
COPY healthcheck/Cargo.toml .
COPY healthcheck/src src/
RUN cargo build --release

#
# build the solidjs app
#
FROM debian:latest AS solidjs
ENV DEBIAN_FRONTEND="noninteractive"
# The bun install script itself needs curl to fetch the zip file from
# github.com; while manually installing Bun is not too difficult, the install
# script is sure to work regardless of what changes are made in the future. Set
# BUN_INSTALL so the subsequent stages can find the bun executable.
RUN apt-get -q update && \
    apt-get -q -y install curl unzip
ENV BUN_INSTALL="/usr/local"
RUN curl -fsSL https://bun.com/install | bash
WORKDIR /build
COPY client client
COPY public public
COPY codegen.ts codegen.ts
COPY index.html index.html
COPY package.json package.json
COPY vite.config.ts vite.config.ts
RUN bun install
RUN bun run codegen
RUN bunx vite build

#
# build the final image
#
# rustls needs the CA certs which are not installed by default
#
FROM debian:latest
RUN apt-get -q update && \
    apt-get -q -y install ca-certificates
WORKDIR /zorigami
COPY --from=builder /build/target/release/zorigami zorigami
COPY --from=healthy /health/target/release/healthcheck .
COPY --from=solidjs /build/dist dist/
VOLUME /database
VOLUME /datasets
VOLUME /packstore
ENV DB_PATH "/database/dbase"
ENV ERROR_DB_PATH "/database/errors.db"
ENV HEALTHCHECK_PATH="/liveness"
ENV HOST "0.0.0.0"
ENV PORT 8080
ENV RUST_LOG info
EXPOSE ${PORT}
HEALTHCHECK CMD ./healthcheck
ENTRYPOINT ["./zorigami"]
