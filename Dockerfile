#
# build the application binaries
#
FROM rust:latest AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install clang libgpgme-dev
WORKDIR /build
COPY Cargo.toml .
COPY src src/
RUN cargo build --release

#
# build the healthcheck binary
#
FROM rust:latest AS healthy
WORKDIR /health
COPY healthcheck/Cargo.toml .
COPY healthcheck/src src/
RUN cargo build --release

#
# build the web code
#
# * using BuckleScript requires gcc/make to compile OCaml
# * must install our npm dev dependencies in order to build
#
FROM node:12 AS webster
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install build-essential
RUN npm install -q -g gulp-cli
WORKDIR /webs
COPY bsconfig.json .
COPY graphql_schema.json .
COPY gulpfile.js .
COPY package.json .
COPY src src/
RUN npm install
ENV NODE_ENV production
RUN gulp build

#
# build the final image
#
FROM debian:latest
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install libgpgme11
RUN adduser --disabled-password --gecos '' zorigami
USER zorigami
WORKDIR /zorigami
COPY --from=builder /build/target/release/libzorigami.rlib .
COPY --from=builder /build/target/release/zorigami .
COPY --from=healthy /health/target/release/healthcheck .
COPY public public/
COPY --from=webster /webs/public/javascripts/main.js ./public/javascripts/
VOLUME /database
VOLUME /datasets
VOLUME /packstore
ENV HOST "0.0.0.0"
ENV PORT 8080
EXPOSE ${PORT}
HEALTHCHECK CMD ./healthcheck
ENV RUST_LOG info
ENTRYPOINT ["./zorigami"]
