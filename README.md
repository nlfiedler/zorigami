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
```

### Running Automated Tests

These commands will build the backend and run the tests.

```shell
$ cargo update
$ cargo build
$ cargo test
```

### Running the Server

These commands will build backend and front-end, and then start the server.

```shell
$ gulp build
$ RUST_LOG=info ./target/debug/zorigami
```

For more verbose debugging output, use `RUST_LOG=debug` in the command above.
For extremely verbose logging, use `RUST_LOG=trace` which will dump volumes of
output.

### Updating the GraphQL PPX schema

The ReasonML support for GraphQL uses a JSON formatted representation of the
schema, which is generated using the following command (after starting a local
server in another window):

```shell
$ npx send-introspection-query http://localhost:8080/graphql
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the
application (e.g. SFTP). A Docker Compose file is located in the `test/docker`
directory, which describes the services used for testing. With the services
running, and a `.env` file in the base directory, the tests will leverage the
services.

### dotenv

This application uses [dotenv](https://github.com/dotenv-rs/dotenv) to configure
the tests. For instance, the tests related to SFTP are enabled by the presence
of certain environment variables, which is easily accomplished using dotenv.

## Deploying

### Using Docker

The base directory contains a `docker-compose.yml` file which is used to build
the application in stages and produce a relatively small final image.

On the build host:

```shell
$ docker-compose build
$ docker image rm 192.168.1.3:5000/zorigami_app
$ docker image tag zorigami_app 192.168.1.3:5000/zorigami_app
$ docker push 192.168.1.3:5000/zorigami_app
```

On the server, with a production version of the `docker-compose.yml` file:

```shell
$ docker-compose pull
$ docker-compose rm -f -s
$ docker-compose up -d
```

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

## Metrics

### macOS APFS

The directory `node_modules` containing `13,161` files, totaling `331M`. Backup
produced `7` pack files, `32M` in size, totaling `222M`, containing the `9,498`
unique files (verified with `fdupes -rm`). Database files totaling `7.2M` in
size.

### Linux ext4

The directory `node_modules` containing `13,164` files, totaling `333M`. Backup
produced `7` pack files, `32M` in size, totaling `227M`, containing the `9,501`
unique files (verified with `fdupes -rm`). Database files totaling `7.2M` in
size.

### Linux ZFS

* Intel Xeon E3-1220 v5 @ 3.00GHz
* 32 GB RAM
* ZFS 5 disk pool (raidz1)
* 305,567 MB of disk usage
* 123,586 files
* 2.47 MB average file size
* 12,947 duplicates, 69,165 MB (confirmed with `fdupes -rm`)
* 10,015 lines of log output at DEBUG level
* 0 errors

#### Snap-shotting

* 1 hours, 22 minutes, 29 seconds
* 25 files per second
* database: 54 MB
* database overhead: 0.02%

#### Pack-building

* 2 hours, 48 minutes, 10 seconds
* 11 files per second
* USB attached slow disk
* Mostly I/O bound
* 8 MB chunk sizes
* 3,373 pack files, 110,639 files
* pack store: 239,803 MB
* ZFS dataset: 307,992 MB
* Pack file overhead: 0.4%
* Size difference due largely to duplicate files

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

### October 2019

Deployed to server using Docker. Replaced gpgme encryption with libsodium.
