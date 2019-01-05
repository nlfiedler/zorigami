# zorigami

An application for archiving files and uploading them to cloud storage. Provides
a simple web interface with facilities for controlling what gets backed up and
where it should go.

## Building and Testing

### Prerequisites

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

### Commands

To start an instance configured for development, run the following command.

```shell
$ npm install
$ npm test
```

### Docker

[Docker](https://www.docker.com) is used for testing some features of the
application (e.g. SFTP). Change to the `test/docker` directory and start the
SFTP server via `docker-compose up -d` (requires Docker Compose).
