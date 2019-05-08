# zorigami

An application for archiving files and uploading them to cloud storage. Provides
a simple web interface with facilities for controlling what gets backed up and
where it should go.

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2018 edition)
* [Node.js](https://nodejs.org/) LTS

#### Example for macOS

This example assumes you are using [Homebrew](http://brew.sh) to install the
dependencies, which provides up-to-date versions of everything needed. The
`xcode-select --install` is there just because the command-line tools sometimes
get out of date, and some of the dependencies will fail to build without them.

```shell
$ xcode-select --install
$ brew install node
$ brew install gpgme
```

### Building and Testing

```shell
$ cargo clean
$ cargo build
$ cargo test
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the
application (e.g. SFTP). Change to the `test/docker` directory and start the
SFTP server via `docker-compose up -d` (requires Docker Compose).

### dotenv

This application uses [dotenv](https://github.com/dotenv-rs/dotenv) to configure
the tests. For instance, the tests related to SFTP are enabled by the presence
of certain environment variables, which is easily accomplished using dotenv.

## Tools

### License checking

Use the https://github.com/Nemo157/cargo-lichking `cargo` utility. To install:

```shell
$ OPENSSL_ROOT_DIR=`brew --prefix openssl` \
  OPENSSL_LIB_DIR=`brew --prefix openssl`/lib \
  OPENSSL_INCLUDE_DIR=`brew --prefix openssl`/include \
  cargo install cargo-lichking
```

To get the list of licenses, and check for incompatibility:

```shell
$ cargo lichking list
$ cargo lichking check
```

However, need to look for "gpl" manually in the `list` output, as most licenses
that are compatible with MIT/Apache are also compatible with GPL.
