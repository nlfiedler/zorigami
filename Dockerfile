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
# build the flutter app
#
# For consistency, use the Dart image as a base, add a version of Flutter that
# is known to work via the fvm tool, and then enable the web platform as a build
# target.
#
FROM google/dart AS flutter
ARG BASE_URL=http://localhost:8080
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install unzip
RUN pub global activate fvm
RUN fvm install 1.26.0-1.0.pre
WORKDIR /flutter
COPY fonts fonts/
COPY lib lib/
COPY pubspec.yaml .
COPY web web/
RUN fvm use 1.26.0-1.0.pre
RUN fvm flutter config --enable-web
RUN fvm flutter pub get
ENV BASE_URL ${BASE_URL}
RUN fvm flutter pub run environment_config:generate
RUN fvm flutter build web

#
# build the final image
#
# rustls needs the CA certs which are not installed by default
#
FROM debian:latest
RUN apt-get -q update && \
    apt-get -q -y install ca-certificates
WORKDIR /zorigami
COPY --from=builder /build/target/release/server zorigami
COPY --from=healthy /health/target/release/healthcheck .
COPY --from=flutter /flutter/build/web web/
VOLUME /database
VOLUME /datasets
VOLUME /packstore
ENV HOST "0.0.0.0"
ENV PORT 8080
EXPOSE ${PORT}
HEALTHCHECK CMD ./healthcheck
ENV RUST_LOG info
ENTRYPOINT ["./zorigami"]
