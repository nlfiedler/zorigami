# zorigami

A backup and restore application written in [Rust](https://www.rust-lang.org) and [Flutter](https://flutter.dev) with a [GraphQL](https://graphql.org) wire protocol.

## Features

* Unlimited backup: all files of any size
* Maintains multiple versions of files, not only the most recent
* Efficiency: compression, de-duplication among and within files
* Encryption: all remotely stored data is encrypted using AES256-GCM [AEAD](https://en.wikipedia.org/wiki/Authenticated_encryption)
* Cloud service agnostic: Amazon, Azure, Google, MinIO, SFTP
* Restore entire directory tree as well as individual files
* Local and Cloud storage
* Scheduled backups
* Cross Platform: Linux, macOS, Windows

## Shortcomings

This project has been a work-in-progress since 2014 and there are still some items that have not yet been implemented. The most notable of these are:

* This software is provided "as-is", WITHOUT WARRANTY OF ANY KIND, see the `LICENSE` for details.
* The database schema and pack file formats have changed radically over the years. It may happen again.
* No automatic pruning of the snapshots which would keep the database size to a reasonable limit.
* Object-to-bucket allocation is very primitive and needs improvement.
* Restoring to dissimilar hardware is not yet an easy task.
* No readily available binaries you will need to build and deploy it yourself.
* No QoS control on uploads over the network so a backup can slow everything else to a crawl.
* Backup procedure operates file-by-file (not _point-in-time_) and hence may record changed content incorrectly or inconsistently (such as with database files).

## Building and Testing

### Prerequisites

* [Rust](https://www.rust-lang.org) stable (2021 edition)
* [Flutter](https://flutter.dev) **stable** channel

### Initial Setup

#### macOS

Use [fvm](https://pub.dev/packages/fvm) to select a specific version of Flutter
to be installed and used by the application. This is the most reliable method
and produces consistent results when building the application.

```shell
brew install dart
dart pub global activate fvm
fvm install stable
fvm flutter config --enable-web
```

#### Windows

The application has not been tested on Windows, but building and running the automated tests does work, for the most part (symbolic link tests still fail). As expected, this process is very complex because Windows was not made for this sort of thing.

1. [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
    * Select the following **Individual components**
        - `MSVC ... build tools`, latest version with appropriate architecture
        - `Windows 11 SDK`, or 10 if using Windows 10
1. [vcpkg](https://github.com/Microsoft/vcpkg) (to install `openssl`)
    * Move the cloned `vcpkg` directory somewhere permanent (e.g. `C:\bin`)
1. `vcpkg install openssl`
1. `$Env:OPENSSL_DIR = 'C:\bin\vcpkg\installed\x64-windows'`
1. [LLVM/Clang](https://github.com/llvm/llvm-project/releases)
    * The _windows_ file with the `.tar.xz` extension seems to work.
    * The LLVM/Clang available from the Visual Studio Installer will install multiple architectures and that seems to cause problems.
    * `$Env:LIBCLANG_PATH = 'C:\bin\clang+llvm-18.1.7-x86_64-pc-windows-msvc\bin'`

The `openssl` Rust crate should **not** be _vendored_ otherwise it will attempt to build OpenSSL from source, which requires Perl in addition to the tools listed above.

### Building, Testing, Starting the Backend

Note that on **Windows** it may be necessary to to run PowerShell as an _administrator_ since, for the time being, calling `symlink_file()` requires special privileges. Alternatively, running Windows in _developer mode_ should also work.

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

### Docker

[Docker](https://www.docker.com) is used for testing some features of the application, such as the various remote pack stores. A Docker Compose file is located in the `containers` directory, which describes the services used for testing. With the services running, and an appropriately configured `.env` file in the base directory, the tests will leverage the services.

### environment_config

The frontend has some configuration that is set up at build time using the
[environment_config](https://pub.dev/packages/environment_config) package. The
generated file (`lib/environment_config.dart`) is not version controlled, and
the values can be set at build-time using either command-line arguments or
environment variables. See the `pubspec.yaml` for the names and the
`environment_config` README for instructions.

## Tools

### Finding Outdated Crates

Use https://github.com/kbknapp/cargo-outdated and run `cargo outdated`

## Origin of the name

A zorigami is a clock possessed by a spirit, as described on the
[Wikipedia](https://en.wikipedia.org/wiki/Tsukumogami) page about
Tsukumogami, which includes zorigami. This has nothing at all do with
this application, accept maybe for the association with time.
