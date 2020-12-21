# zorigami

An application for archiving files and uploading them to cloud storage. Provides
a simple web interface with facilities for controlling what gets backed up and
where it should go.

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2018 edition)
* [Flutter](https://flutter.dev) beta channel
    - Enable the **web** configuration

#### Windows

1. Visual Studio [Code](https://code.visualstudio.com/)
1. [Git](https://git-scm.com/)
1. [Visual Studio](https://visualstudio.microsoft.com/) (to build `vcpkg`)
    * Choose **Desktop development with C++** "workload"
1. [vcpkg](https://github.com/Microsoft/vcpkg) (to install `openssl`)
1. `vcpkg install openssl`
1. `Set-Item -path env:OPENSSL_DIR -value C:\bin\vcpkg\installed\x86-windows`
1. Install [clang](http://clang.llvm.org/) (to build RocksDB)
    * Need 32-bit because other pieces are 32-bit?
1. `Set-Item -path env:LIBCLANG_PATH -value C:\bin\LLVM\bin`

Rather than `Set-Item` it may be better to set the environment variables in the
system settings, then VS Code will be able to build everything.

The `openssl` Rust crate must _not_ be "vendored" otherwise it will attempt to
build OpenSSL from source, which requires Perl in addition to the tools listed
above, _and_ it will likely fail to compile on 32-bit Windows.

### Building, Testing, Starting the Backend

```shell
$ cargo update
$ cargo build --workspace
$ cargo test --workspace
$ RUST_LOG=info cargo run
```

For more verbose debugging output, use `RUST_LOG=debug` in the command above.
For extremely verbose logging, use `RUST_LOG=trace` which will dump large
volumes of output.

To build or run tests for a single package, use the `-p` option, like so:

```shell
$ cargo build -p store_minio
$ cargo test -p store_minio
```

### Building, Testing, Starting the Frontend

```shell
$ flutter pub get
$ flutter pub run environment_config:generate
$ flutter test
$ flutter run -d chrome
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the
application (e.g. SFTP). A Docker Compose file is located in the `server/tests/docker`
directory, which describes the services used for testing. With the services
running, and a `.env` file in the base directory, the tests will leverage the
services.

### dotenv

The backend uses [dotenv](https://github.com/dotenv-rs/dotenv) to configure the
tests. For instance, the tests related to SFTP are enabled by the presence of
certain environment variables, which is easily accomplished using dotenv.

### environment_config

The frontend has some configuration that is set up at build time using the
[environment_config](https://pub.dev/packages/environment_config) package. The
generated file (`lib/environment_config.dart`) is not version controlled, and
the values can be set at build-time using either command-line arguments or
environment variables. See the `pubspec.yaml` for the names and the
`environment_config` README for instructions.

## Deploying

### Using Docker

The base directory contains a `docker-compose.yml` file which is used to build
the application in stages and produce a relatively small final image.

On the build host:

```shell
$ docker-compose build --pull --build-arg BASE_URL=http://192.168.1.1:8080
$ docker image rm 192.168.1.1:5000/zorigami
$ docker image tag zorigami_app 192.168.1.1:5000/zorigami
$ docker push 192.168.1.1:5000/zorigami
```

On the server, with a production version of the `docker-compose.yml` file:

```shell
$ docker-compose down
$ docker-compose up --build -d
```

## Google Cloud Setup

How to create a new project and get the service account credentials file.

1. Create a new project in Google Cloud Platform
1. Navigate to **APIs & Services**
1. Open **Credentials** screen
1. Click _CREATE CREDENTIALS_ and select _Service_ account
1. Enter an account name and optional description
1. Click **CREATE** button
1. Navigate to **IAM & Admin / IAM** and click the **ADD** button
1. Start typing the name of the service account and select the result
1. Add role for _Storage Admin_ under the _Cloud Storage_ category
    * The service account needs to be able to create buckets and objects.
1. Click _Manage service accounts_ link or navigate to **IAM & Admin / Service Accounts**
1. Click on the _Actions_ 3-dot button and select _Create key_
1. Choose *JSON* and click **CREATE** button

## Design

### Clean Architecture

Within the `server` crate the general design of the application conforms to the [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) in which the application is divided into three layers: domain, data, and presentation. The domain layer defines the "policy" or business logic of the application, consisting of entities, use cases, and repositories. The data layer is the interface to the underlying system, defining the data models that are ultimately stored in a database. The presentation layer is what the user generally sees, the web interface, and to some extent, the GraphQL interface.

### Workspace and Packages

The overall application is broken into several crates, or packages as they are
sometimes called. The principle benefit of this is that the overall scale of the
application is easier to manage as it grows. In particular, the dependencies and
build time steadily grow as more store backends are added. These pack store
implementations, and the database implementation as well, are in packages that
are kept separate from the main `server` package. In theory this will help with
build times for the crates, as each crate forms a single compilation unit in
Rust, meaning that a change to any file within that crate will result in the
entire crate being compiled again. Note also that the dependencies among crates
form a directed acyclic graph, meaning there can be no mutual dependencies.

## Tools

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

### Local and SFTP

* Linux ZFS system as above
* Snapshot: 1 hours, 11 minutes, 59 seconds
* 124,079 files
* Packing: 20 hours, 25 minutes, 43 seconds
* 4 MB chunk sizes
* 64 MB pack sizes
* 3,627 packs, 111,130 files
* pack store: 239,880 MB
* ZFS dataset: 307,205 MB
* database: 67 MB

### Local and Minio

* Mostly same as above
* Packing: 9 hours, 56 minutes, 9 seconds

## Origin of the name

A zorigami is a clock possessed by a spirit, as described on the
[Wikipedia](https://en.wikipedia.org/wiki/Tsukumogami) page about
Tsukumogami, which includes zorigami. This has nothing at all do with
this application, accept maybe for the association with time.

## Project History

### July 2014

Started as a project named *akashita*, with a basic
[Python](https://www.python.org) implementation that uploaded tarballs to
Glacier. Used ZFS commands to create a snapshot of the dataset, then `tar` and
`split` to produce "pack" files of a manageable size.

### February 2016

Started the [Erlang](http://www.erlang.org) implementation with all of the
Python code converted to [Go](https://golang.org) and the Erlang application
invoking the Go component to upload the individual files.

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
using [Rust](https://www.rust-lang.org). Web interface written in
[ReasonML](https://reasonml.github.io) during the summer.

### October 2019

Deployed to server using Docker. Replaced gpgme encryption with libsodium.

### February 2020

Replaced the ReasonML web interface with Flutter.

### June/July 2020

Rewrite server code according to Clean Architecture design, breaking database
and pack stores into separate packages within the overall workspace.
