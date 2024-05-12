# Use tar for pack files

* Status: accepted
* Deciders: Nathan Fiedler
* Date: 2024-05-11

## Context

The application collects small files for backup, and chunks from larger files, into what are called "pack" files. These pack files are then uploaded to the pack stores, such as Amazon S3. Initially, the pack file format was simply a [tar](https://en.wikipedia.org/wiki/Tar_(computing)) with gzip-compressed chunks of data. However, the overhead per file was about one kilobyte so it seemed that finding a new format would be a worthwhile endeavor. An effort was made to use [7z](https://en.wikipedia.org/wiki/7z) with the `sevenz-rust` crate. However, it seems that the crate produces corrupted archives (see [issue 12](https://github.com/dyz1990/sevenz-rust/issues/12)). While that bug was fixed, it forced a lot of thought to be put into archiver/compressor solutions.

There are, in fact, several problems with the current solution:

1. Tar has significant overhead per entry
1. sodiumoxide is deprecated and the project is archived (and NaCL itself appears to be dead)
1. The NaCL library (and sodiumoxide in turn) are not standand encryption algorithms
1. Content was compressed piecewise which is not effective (compression works better on larger blocks of data)
1. Overall file, with all that tar overhead, was not compressed so encryption was less than great

While there are quite a few file formats in existence, there are extremely few options for an archiver/compressor that can be invoked from Rust. After learning about the [Pack](https://pack.ac) program, an effort was made to build a new archiver/compressor that suited the needs of this application, and that is [EXAF](https://github.com/nlfiedler/exaf-rs).

EXAF offers several advantages:

1. Very few dependencies
1. Compresses using [Zstandard](http://facebook.github.io/zstd/)
1. Encrypts using the AES256-GCM [AEAD](https://en.wikipedia.org/wiki/Authenticated_encryption) cipher
1. Packs can be extracted using the `exaf-rs` tool and the password (no database required)
1. Overhead per entry is extremely low (tens of bytes)

One drawback of EXAF is that tracking the size of the archive is a bit clumsy as content is not flushed to disk until an internal buffer is filled. That buffer is 16mb in size, so pack sizes that are multiples of 16mb would work best.

## Decision

The format for pack files will be EXAF.

## Consequences
