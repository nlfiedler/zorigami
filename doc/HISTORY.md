# Project History

## July 2014

Started as a project named [akashita](https://github.com/nlfiedler/akashita), with a basic [Python](https://www.python.org) implementation that uploaded tarballs to Amazon Glacier. Used ZFS commands to create a snapshot of the dataset, then `tar` and `split` to produce "pack" files of a manageable size.

## February 2016

Started the [Erlang](http://www.erlang.org) implementation with all of the Python code converted to [Go](https://golang.org) and the Erlang application invoking the Go component to upload the individual files.

## August 2016

Switched from Amazon Glacier to Google Cloud Storage.

## November 2016

Replaced the Go code with an Erlang [client](https://github.com/nlfiedler/enenra) for Google Cloud Storage.

## September 2017

Attempted to rewrite the application in [Elixir](https://elixir-lang.org). Spent a lot of time designing the data model and developing the basic algorithms, using the Arq data model as a starting point.

## December 2018

Started new project named zorigami that was written using [Node.js](https://nodejs.org/), again spending a lot of time designing a new data model based on a key/value store. Settled on using tar format for the pack files, and [OpenPGP](https://tools.ietf.org/html/rfc4880) for encryption.

## February 2019

Started rewriting the (now [TypeScript](https://www.typescriptlang.org)) code using [Rust](https://www.rust-lang.org). Web interface written in [ReasonML](https://reasonml.github.io) during the summer.

## October 2019

Deployed to server using Docker. Replaced gpgme encryption with libsodium.

## February 2020

Replaced the ReasonML web interface with Flutter.

## June/July 2020

Rewrite server code according to Clean Architecture design, breaking database and pack stores into separate packages within the overall workspace.

## August 2023

Found and fixed a major bug in the recording of the file and chunk metadata. If a single-chunk file were to finish filling a pack, it would be recorded as belonging to the next pack that was created. If no other pack was created, the file record would not be created. This led to two problems on restore, one in which the chunk could not be found, and the other in which the file record was missing. Added a query to track down the missing chunks by searching all pack archives, and a mutation that inserts a new file record. An unused usecase has been committed to the repository for gathering the data regarding the chunks of a file that point to the wrong packs (the first problem).

Tested full backup and restore on the 300+GB shared dataset. Verified that a 30GB file was restored correctly. Symbolic links are also restored correctly.

## May 2024

Switched from the tar file abomination to [EXAF](https://github.com/nlfiedler/exaf-rs) for the pack files and the database archive. Both packs and database are now encrypted using the provided passphrase with a strong, standards-based encryption algorithm. This also allows for the database backup and pack files to be fetched and extracted manually if necessary.

Replace all use of SHA256 with BLAKE3 for improved performance (the latter is about twice as fast as the former).
