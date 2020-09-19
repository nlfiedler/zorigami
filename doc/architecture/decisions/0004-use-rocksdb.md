# Use RocksDB

* Status: accepted
* Deciders: Nathan Fiedler
* Date: 2020-08-20

## Context

The application needs to store numerous small records, all of which are addressed using some form of hash digest. Given that data model, a simple key-value store should be adequate. What's more, for the purpose of an application designed to run on the desktop computer, the data store should be embedded (i.e. runs in-process).

## Decision

There are surprisingly few choices when it comes to an embedded database that is accessible from an application written in Rust. One is [SQLite](https://sqlite.org/index.html) which is a relational database that saves everything to a single (ever growing) file. Another is RocksDB, which itself is not written in Rust, but there is a well-maintained Rust wrapper. RocksDB is fast and actively maintained by Facebook.

The choice is **RocksDB**. It is resilient to data-loss, fast, and space efficient for the purpose of this application, which does not have relational data.

## Consequences

RocksDB has been used by this application since February 2019 and it has been working very well without any issues whatsoever.

## Links

* RocksDB [website](https://rocksdb.org)
* rust-rocksdb [GitHub](https://github.com/rust-rocksdb/rust-rocksdb)
