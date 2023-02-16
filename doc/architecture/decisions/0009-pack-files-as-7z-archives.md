# Use 7z for pack files

* Status: accepted
* Deciders: Nathan Fiedler
* Date: 2023-02-15

## Context

The application collects small files for backup, and chunks from larger files, into what are called "pack" files. These pack files are then uploaded to the pack stores, such as Amazon S3. Initially, the pack file format was simply a [tar](https://en.wikipedia.org/wiki/Tar_(computing)) with gzip-compressed chunks of data. However, the overhead per file was about one kilobyte so it seemed that finding a new format would be a worthwhile endeavor.

The new format should be both compressed and an archive containing multiple entries. It should have minimal overhead per entry, and it should be a free and open format. Similarly, it should be a well-known format that will be supported well into the future. A very limited number of options present themselves:

* Zip: a very popular and well-known format with well-known drawbacks.
* 7z: a relatively recent format with better compression than zip.

The other options are either proprietary, outdated, or application/platform-specific (such as RAR, JAR, StuffIt, DiskImage).

## Decision

The format for pack files going forward will be 7z. While it takes twice as long to write a complete pack file, on average, it offers much better compression than the compressed chunks of data in a tar file. This is due in large part to the difference between the DEFLATE and LZMA2 compression algorithms, where LZMA2 trades speed for higher levels of compression.

## Consequences

The pack files contain more chunks per pack, but it takes twice as long to create them.

## Links

* [7z](https://en.wikipedia.org/wiki/7z)
