#
# build the application binaries
#
FROM rust:latest AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -q update && \
    apt-get -q -y install clang
WORKDIR /build
COPY Cargo.toml .
COPY src src/
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
# Not really sure what the point of the "beta-web" tag is because it is
# neither set to the beta channel nor is the web enabled, so we end up
# setting everything ourselves anyway.
#
FROM cirrusci/flutter:beta-web AS flutter
ARG BASE_URL
RUN flutter channel beta
RUN flutter upgrade
RUN flutter config --enable-web
WORKDIR /flutter
COPY assets assets/
COPY lib lib/
COPY pubspec.yaml .
COPY web web/
# silly docker and this image do not see eye-to-eye on permissions
# c.f. https://github.com/cirruslabs/docker-images-flutter/issues/12
RUN sudo chown -R cirrus:cirrus /flutter
RUN flutter pub get
ENV BASE_URL ${BASE_URL}
RUN flutter pub run environment_config:generate
RUN flutter build web

#
# build the final image
#
FROM debian:latest
RUN adduser --disabled-password --gecos '' zorigami
USER zorigami
WORKDIR /zorigami
COPY --from=builder /build/target/release/zorigami .
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
