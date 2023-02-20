# Use tar for pack files

* Status: accepted
* Deciders: Nathan Fiedler
* Date: 2023-02-20

## Context

The application collects small files for backup, and chunks from larger files, into what are called "pack" files. These pack files are then uploaded to the pack stores, such as Amazon S3. Initially, the pack file format was simply a [tar](https://en.wikipedia.org/wiki/Tar_(computing)) with gzip-compressed chunks of data. However, the overhead per file was about one kilobyte so it seemed that finding a new format would be a worthwhile endeavor. An effort was made to use [7z](https://en.wikipedia.org/wiki/7z) with the `sevenz-rust` crate. However, it seems that the crate produces corrupted archives (see [issue 12](https://github.com/dyz1990/sevenz-rust/issues/12)).

The options for pack files are:

1. Custom format
2. Zip file
3. tar file
4. Platform or application specific formats

While zip is a very common format, the `zip` crate does not provide a means of tracking the compressed data as it is written to the archive. This makes producing ideally sized packs more difficult since we can only track the size of the input data, not the results after compression.

A custom format would be a poor choice for longevity and maintenance. The other choices are either platform-specific, or suited to certain applications, or just very outdated.

## Decision

The format for pack files will be returning to the original tar file with compressed chunks of data. With this method, we can track the size of the compressed data as it is written, yielding ideally sized pack files. The tar format is very well known and both the `tar` and `flate2` crates are well maintained and generally reliable.

## Consequences

The pack files contain fewer chunks per pack due to the use of DEFLATE compression, however the run time will be reduced when compared to using LZMA2.
