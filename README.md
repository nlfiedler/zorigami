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

These commands will build the backend and run the tests.

```shell
$ cargo clean
$ cargo build
$ cargo test
```

### Building and Running

These commands will build backend and front-end, and then start the server.

```shell
$ gulp build
$ RUST_LOG=info ./target/debug/zorigami
```

For more verbose debugging output, use `RUST_LOG=debug` in the command above.

### Updating the GraphQL PPX schema

The ReasonML support for GraphQL uses a JSON formatted representation of the
schema, which is generated using the following command (after starting a local
server in another window):

```shell
$ npx send-introspection-query http://localhost:8080/graphql
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the
application (e.g. SFTP). From the base directory, start the containers using
`docker-compose up -d` (requires Docker Compose).

### dotenv

This application uses [dotenv](https://github.com/dotenv-rs/dotenv) to configure
the tests. For instance, the tests related to SFTP are enabled by the presence
of certain environment variables, which is easily accomplished using dotenv.

## Tools

### Visual Studio Code and Reason Language Server

When adding new ReasonML dependencies to the project, it may be necessary to
restart the Reason Language Server. Use the VS Code command palette to find
**Restart Reason Language Server** and select it -- now the references to the
new code should be resolved correctly.

### Finding Outdated Crates

Use https://github.com/kbknapp/cargo-outdated and run `cargo outdated`

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

## Origin of the name

A zorigami is a clock possessed by a spirit, as described on the
[Wikipedia](https://en.wikipedia.org/wiki/Tsukumogami) page about
Tsukumogami, which includes zorigami. This has nothing at all do with
this application, accept maybe for the association with time.

## Project History

### July 2014

Started as a project named "akashita", with a basic
[Python](https://www.python.org) implementation that uploaded tarballs to
Glacier. Used ZFS commands to create a snapshot of the dataset, then `tar` and
`split` to produce "pack" files of a reasonable size.

### February 2016

Started the [Erlang](http://www.erlang.org) implementation with all of the
Python code converted to [Go](https://golang.org) and the Erlang application
invoking the Go piece to upload the individual files.

### August 2016

Switched from Amazon Glacier to Google Cloud Storage.

### November 2016

Replaced the Go code with an Erlang
[client](https://github.com/nlfiedler/enenra) for Google Cloud Storage.

### September 2017

Attempted to rewrite the application in [Elixir](https://elixir-lang.org). Spent
a lot of time designing the data model and developing the basic algorithms,
using the Arq data model as a starting point.

### December 2018

Started new project named zorigami that was written using
[Node.js](https://nodejs.org/), again spending a lot of time designing a new
data model based on a key/value store. Settled on using tar format for the pack
files, and [OpenPGP](https://tools.ietf.org/html/rfc4880) for encryption.

### February 2019

Started rewriting the (now [TypeScript](https://www.typescriptlang.org)) code
using [Rust](https://www.rust-lang.org).
