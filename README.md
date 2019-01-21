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

### dotenv

This application uses [dotenv](https://github.com/motdotla/dotenv) to configure
the tests. For instance, the tests related to SFTP are enabled by the presence
of certain environment variables, which is easily accomplished using dotenv.

### Mocha and Chai

This application uses [Mocha](https://mochajs.org) and
[Chai](https://www.chaijs.com) for testing. Mocha runs tests in serial which is
very useful for writing tests that build on previous tests in the order in which
they appear. Chai has a very rich API for assertions.

### Why not Jest

[Jest](https://jestjs.io) seems very appealing on paper, but getting started is
not well documented, and there are no obvious, complete examples of how to write
Jest tests in TypeScript. Migrating from Mocha is supposedly very easy, but
again nothing concrete on how to do this. The critical hit, however, was that
Jest runs tests in parallel, which foils our attempts at crafting tests that
build on previous tests.

### Code Coverage

The code coverage requirement is achieved using [c8](https://github.com/bcoe/c8).
Install c8 globally (`npm -g install c8`) and invoke the tests like so:

```shell
$ c8 npm test
```

However, either with TypeScript or the current implementation of c8, the output
is rather inaccurate.
