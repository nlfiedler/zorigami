# zorigami

A backup and restore application.

## Features

* Unlimited backup: all files of any size
* Maintains multiple versions, not just the most recent
* Efficiency: compression, de-duplication, block-level incremental backup
* Encryption: all remotely stored data is encrypted with libsodium
* Service agnostic: SFTP, Amazon, Azure, Google, MinIO
* Directory tree and individual file restore
* Restore to dissimilar hardware
* Local and Cloud storage
* Scheduled backups
* Cross Platform: macOS, Windows, Linux
* Amazon Glacier support
* Fault tolerant

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2021 edition)
* [Flutter](https://flutter.dev) **stable** channel

### Initial Setup

Use [fvm](https://pub.dev/packages/fvm) to select a specific version of Flutter
to be installed and used by the application. This is the most reliable method
and produces consistent results when building the application.

```shell
brew install dart
dart pub global activate fvm
fvm install stable
fvm flutter config --enable-macos-desktop
fvm flutter config --enable-web
```

#### Windows

1. [Visual Studio Code](https://code.visualstudio.com/)
1. [Git](https://git-scm.com/)
    * `winget install --id Git.Git -e --source winget`
1. [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
    * Select the following **Individual components**
        - `MSVC ... build tools`, latest version with appropriate architecture
        - `Windows 11 SDK`, or 10 if using Windows 10
        - C++ Clang Compiler for Windows
        - MSBuild support for LLVM toolset
1. [vcpkg](https://github.com/Microsoft/vcpkg) (to install `openssl`)
    * Move the cloned `vcpkg` directory somewhere permanent (e.g. `C:\bin`)
1. `vcpkg install openssl`
1. `Set-Item -path env:OPENSSL_DIR -value C:\bin\vcpkg\installed\x64-windows`

Rather than using `Set-Item` it may be better to set the environment variables in the system settings, then VS Code will be able to build everything.

The `openssl` Rust crate must _not_ be "vendored" otherwise it will attempt to build OpenSSL from source, which requires Perl in addition to the tools listed above.

Note that Visual Studio Installer might install LLVM with multiple architectures (x86 and ARM) and that may result in the Rust compiler attempting to load the wrong version (`libclang.dll could not be opened: LoadLibraryExW failed`) -- if that happens, removing the other architecture directory should help.

### Building, Testing, Starting the Backend

Note that on **Windows** it may be necessary to to run PowerShell as an
administrator since, for the time being, calling `symlink_file()` requires
special privileges. Alternatively, running Windows in "developer mode" should
also work.

```shell
cargo update
cargo build
cargo test
RUST_LOG=info cargo run
```

For more verbose debugging output, use `RUST_LOG=debug` in the command above.
For extremely verbose logging, use `RUST_LOG=trace` which will dump large
volumes of output.

To build or run tests for a single package, use the `-p` option, like so:

```shell
cargo build -p store_minio
cargo test -p store_minio
```

### Building, Testing, Starting the Frontend

```shell
fvm flutter pub get
fvm flutter pub run environment_config:generate
fvm flutter test
fvm flutter run -d chrome
```

### Producing a Local Test Build

```shell
fvm flutter clean
fvm flutter pub get
env BASE_URL=http://localhost:8000 fvm flutter pub run environment_config:generate
fvm flutter build web
cargo build --release
```

Server binary is `target/release/server` and web contents are in `build/web`

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
docker compose build --pull --build-arg BASE_URL=http://192.168.1.2:8080
docker image rm 192.168.1.2:5000/zorigami
docker image tag zorigami-app 192.168.1.2:5000/zorigami
docker push 192.168.1.2:5000/zorigami
```

On the server, with a production version of the `docker-compose.yml` file:

```shell
docker compose down
docker compose up --build -d
```

## Amazon S3 Setup

1. Create user that will act on behalf of zorigami
1. Give specific permissions, not assign to a group
1. Add **AmazonS3FullAccess** permission (search for _s3_)
1. Add **AmazonDynamoDBFullAccess** permission (search for _dynamo_)
1. View the newly created user
1. Find the **Security credentials** tab
1. Add a new **Access key** for this user
1. Select _Application running outside AWS_ when asked
1. Download the `.csv` file of the newly created key

## Azure Blob Storage

How to create a new storage account and get the access key.

1. From the Azure portal, find **Storage accounts** and select it
1. Find and click the **Create** button
1. Create a new resource group, choose a storage account name
1. Select a suitable region
1. Select the lowest cost redundancy (LRS)
1. Click the **Advanced** button
1. Select the _Cool_ option under **Access tier**
1. Click the **Networking** button and review the default selections
1. Click the **Data protection** button and turn off the _soft delete_ options
1. Click the **Encryption** button and review the default selections
1. Click the **Review** button and then click **Create**
1. Once the deployment is done, click the button to view the resource.
1. Find the **Access keys** option on the left panel
1. Copy the _Storage account name_ and _Key_ value from **key1**

## Google Cloud Setup

How to create a new project and get the service account credentials file.

1. Create a new project in Google Cloud Platform
1. Navigate to the **Firestore** page under _DATABASES_
    * Do **not** select _Filestore_ under _STORAGE_, that is a different service
1. Create a _native_ Firestore database (there can be only one)
1. Navigate to **APIs & Services**
1. Open **Credentials** screen
1. Click _CREATE CREDENTIALS_ and select _Service_ account
1. Enter an account name and optional description
1. Click **CREATE** button
1. Navigate to **IAM & Admin / IAM** and click the **GRANT ACCESS** button
1. Under the _Assign roles_ section of the dialog...
1. Start typing the name of the service account and select the result
1. Under the _Cloud Storage_ category and select _Storage Admin_
    * The service account needs to be able to create buckets and objects.
1. Click **ADD ANOTHER ROLE** button
1. Under the _Firebase_ category select _Firebase Admin_
    * The service account needs to be able to create and update documents.
1. Click **SAVE** button
1. Navigate to **IAM & Admin / Service Accounts**
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
OPENSSL_ROOT_DIR=`brew --prefix openssl` \
    OPENSSL_LIB_DIR=`brew --prefix openssl`/lib \
    OPENSSL_INCLUDE_DIR=`brew --prefix openssl`/include \
    cargo install cargo-lichking
```

To get the list of licenses, and check for incompatibility:

```shell
cargo lichking list
cargo lichking check
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

Started as a project named [akashita](https://github.com/nlfiedler/akashita), with a basic [Python](https://www.python.org) implementation that uploaded tarballs to Amazon Glacier. Used ZFS commands to create a snapshot of the dataset, then `tar` and `split` to produce "pack" files of a manageable size.

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

### August 2023

Found and fixed a major bug in the recording of the file and chunk metadata. If
a single-chunk file were to finish filling a pack, it would be recorded as
belonging to the next pack that was created. If no other pack was created, the
file record would not be created. This led to two problems on restore, one in
which the chunk could not be found, and the other in which the file record was
missing. Added a query to track down the missing chunks by searching all pack
archives, and a mutation that inserts a new file record. An unused usecase has
been committed to the repository for gathering the data regarding the chunks of
a file that point to the wrong packs (the first problem).

Tested full backup and restore on the 300+GB shared dataset. Verified that a
30GB file was restored correctly. Symbolic links are also restored correctly.

### May 2024

Switched from the tar file abomination to [EXAF](https://github.com/nlfiedler/exaf-rs) for the pack files and the database archive. Both packs and database are now encrypted using the provided passphrase. This also allows for the database backup and pack files to be fetched and extracted manually if necessary.
