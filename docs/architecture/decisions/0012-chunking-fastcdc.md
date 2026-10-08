# Use FastCDC for chunking

- Status: accepted
- Deciders: Nathan Fiedler
- Date: 2026-02-22

## Context

During the backup process, files larger than a given chunk size are split using a content-defined chunking (CDC) algorithm. The alternative to CDC is fixed-size chunking (FSC) which has the drawback of very poor deduplication. The principle goal of this application is to backup file content while using secondary storage efficiently. As such, finding a CDC algorithm that demonstrates good deduplication ratios is paramount.

There are two primary types of content-defined chunking algorithms suitable for deduplication:

1. Hashless: AE, RAM, SeqCDC
1. Hash-based: Rabin, TTTD, FastCDC, UltraCDC

Additionally, some algorithms employ a stream-informed history to skip calculations on repeated fragments, such as [QuickCDC](https://ieeexplore.ieee.org/document/9644788) and [SuperCDC](https://ieeexplore.ieee.org/document/9978509). Such an approach requires storing the gear hash along with additional data and loading that into memory to make it available to the chunking algorithm. Given the large size of the possible data sets, in which there can easily be hundreds of thousands of files, this could be impractical.

Rabin-based CDC generally offers the best deduplication ratio, but it is fairly slow. [FastCDC](https://ieeexplore.ieee.org/document/9055082) is widely recognized as being very fast at chunking, and its deduplication ratios are similar to that of Rabin. [UltraCDC](https://ieeexplore.ieee.org/document/9894295) is designed to handle low-entropy inputs better than other algorithms, but is slower than FastCDC.

The [AE](https://ieeexplore.ieee.org/document/7524782), [RAM](https://www.sciencedirect.com/science/article/abs/pii/S0167739X16305829), and [SeqCDC](https://dl.acm.org/doi/10.1145/3652892.3700766) algorithms do not utilize a hash and instead look for sequences of bytes that have a particular property related to the previously examined bytes. With SeqCDC in particular, there are two approaches, _increasing_ and _decreasing_, and which one works best will depend on the input data. While AE and RAM can be faster than FastCDC for some data sets, they may sacrifice deduplication. Depending on the data set, each algorithm may outperform the others by a small margin.

Other CDC implementations exist, such as [MaxCDC](https://github.com/buildbarn/go-cdc) and [MinCDC](https://github.com/orlp/mincdc) which either have no published deduplication ratio values or perform poorly compared to FastCDC. Both approaches are optimized for minimizing the range of chunk sizes rather than improved deduplication.

## Decision

The chunking algorithm will be FastCDC since it is faster than most and has a good deduplication ratio for most data sets.

Note that this application uses the BLAKE3 digest for recording chunks, rather than the Gear hash due to the higher entropy of BLAKE3 versus the 64-bit Gear hash.

## Consequences

The chunking algorithm has been FastCDC since late 2018 and has worked well since then.

## Links

- [FastCDC](https://crates.io/crates/fastcdc)
