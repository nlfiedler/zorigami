# Self-healing Packs

Reed-Solomon erasure coding allows for stored data to be repaired in the event
of corruption resulting from flaky hardware. The pack files created by this
application make use of the `exaf-rs` crate which now supports ECC by means of
Reed-Solomon erasure coding with customizable values for the number of data and
parity shards. While cloud storage services all provide some level of data
protection, local disk storage does not (and SFTP and Minio). As such, it would
be helpful to allow the user to configure a dataset to make use of ECC when
creating pack files.

## Proposal

Add two new fields to the `Dataset` type to specify the number of data shards
and the number of parity shards. If both values are non-zero then make use of
the ECC support in `exaf-rs` when creating the pack files (call `enable_ecc()`
on `exaf_rs::writer::Writer`).

## Decisions

The shard counts are stored as two `u8` fields on the dataset and surfaced as two
plain number inputs, rather than a set of named levels. Both must be zero
(disabled) or both non-zero, and their sum must not exceed 255; this is validated
in the `update_dataset` use case so a bad configuration is rejected at the point
of entry rather than surfacing much later as a failed backup inside a background
task.

The `ecc` feature of `exaf-rs` is not enabled by default and is required to
*read* an ECC archive, not merely to write one, so it is enabled unconditionally
in `server/Cargo.toml`. This is what makes such packs unreadable by earlier
releases.

Two things beyond the original proposal proved necessary:

**The database archive is protected too.** `create_archive` uses a fixed 10 data
and 2 parity shards, always. The archive is tiny next to the packs it describes
and is the one thing that must be readable to recover anything at all, so there
is nothing to gain from making it optional or configurable.

**The restore path had to stop rejecting repairable packs.** `verify_pack_digest`
hashes the whole pack file and errored on any mismatch *before* `extract_pack`
ever ran, so a single flipped bit aborted the restore even when EXAF could have
repaired it — erasure coding would have bought nothing on the path that matters.
A digest mismatch is now fatal only for a pack without erasure coding; for one
with it, the mismatch is logged as a warning and extraction proceeds, which
repairs the damage or fails with `EccUnrecoverable` if it exceeds the parity
budget.

The pack digest check remains valuable regardless: without erasure coding, the
same corruption causes `extract_pack` to *succeed* while silently yielding a
corrupted chunk, so the digest is the only thing standing between that pack and a
bad restore.
