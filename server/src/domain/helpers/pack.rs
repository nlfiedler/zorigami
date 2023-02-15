//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::Chunk;
use anyhow::{anyhow, Error};
use sevenz_rust::{SevenZArchiveEntry, SevenZWriter};
use std::fs::{self, File};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Builds a tar file one chunk at a time, with each chunk compressed
/// separately, with the overall size being not much larger than a set size.
pub struct PackBuilder {
    /// Preferred size of pack file in bytes.
    target_size: u64,
    /// Compressed bytes written to the pack so far.
    bytes_packed: u64,
    /// 7z file writer.
    builder: Option<SevenZWriter<File>>,
    /// Path of the output file.
    filepath: Option<PathBuf>,
    /// Number of chunks added to the pack.
    chunks_packed: u32,
}

impl PackBuilder {
    /// Construct a builder that will produce a tar file comprised of compressed
    /// chunk data that will ultimately be not much larger than the given size.
    pub fn new(target_size: u64) -> Self {
        Self {
            target_size,
            bytes_packed: 0,
            builder: None,
            filepath: None,
            chunks_packed: 0,
        }
    }

    /// Returns `true` if the builder has been initialized and is ready to
    /// receive chunks.
    pub fn is_ready(&self) -> bool {
        self.builder.is_some()
    }

    /// Returns `true` if there are no chunks in the pack file.
    pub fn is_empty(&self) -> bool {
        self.chunks_packed == 0
    }

    /// Initialize the builder for the given output path.
    pub fn initialize(&mut self, outfile: &Path) -> Result<(), Error> {
        self.filepath = Some(outfile.to_path_buf());
        let file = File::create(outfile)?;
        let builder = SevenZWriter::new(file)?;
        self.builder = Some(builder);
        Ok(())
    }

    /// Write the chunk data in compressed form to the pack file. Returns `true`
    /// if the compressed data has reached the pack size given in `new()`.
    pub fn add_chunk(&mut self, chunk: &Chunk) -> Result<bool, Error> {
        if self.bytes_packed > self.target_size {
            return Err(anyhow!("pack already full"));
        }
        let filepath = chunk
            .filepath
            .as_ref()
            .ok_or_else(|| anyhow!("chunk requires a filepath"))?;
        let mut infile = File::open(filepath)?;
        infile.seek(io::SeekFrom::Start(chunk.offset as u64))?;
        let handle = infile.take(chunk.length as u64);
        let builder = self
            .builder
            .as_mut()
            .ok_or_else(|| anyhow!("must call initialize() first"))?;
        let mut entry: SevenZArchiveEntry = Default::default();
        entry.name = chunk.digest.to_string();
        entry.has_stream = true;
        entry.is_directory = false;
        let result = builder.push_archive_entry(entry, Some(handle))?;
        self.bytes_packed += result.compressed_size;
        self.chunks_packed += 1;
        Ok(self.bytes_packed >= self.target_size)
    }

    /// Flush pending writes and close the pack file.
    pub fn finalize(&mut self) -> Result<PathBuf, Error> {
        self.builder
            .take()
            .ok_or_else(|| anyhow!("must call initialize() first"))?
            .finish()?;
        let filepath = self
            .filepath
            .take()
            .ok_or_else(|| anyhow!("must call initialize() first"))?;
        self.bytes_packed = 0;
        self.chunks_packed = 0;
        Ok(filepath)
    }
}

///
/// Extract the chunks from the given pack file, writing them to the output
/// directory, with the names being the original SHA256 of the chunk (with a
/// "sha256-" prefix).
///
pub fn extract_pack(infile: &Path, outdir: &Path) -> Result<Vec<String>, Error> {
    fs::create_dir_all(outdir)?;
    let results = Mutex::new(Vec::new());
    sevenz_rust::decompress_file_with_extract_fn(infile, outdir, |entry, reader, path| {
        let file = File::create(&path)
            .map_err(|e| sevenz_rust::Error::FileOpen(e, path.to_string_lossy().to_string()))?;
        let mut writer = std::io::BufWriter::new(file);
        std::io::copy(reader, &mut writer).map_err(sevenz_rust::Error::io)?;
        results.lock().unwrap().push(entry.name.clone());
        Ok(true)
    })?;
    Ok(results.into_inner().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::Checksum;
    use tempfile::tempdir;

    #[test]
    fn test_pack_builder_single() -> Result<(), Error> {
        // build a small pack file with small files
        let chunks = [
            Chunk::new(
                Checksum::SHA256(
                    "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f".to_owned(),
                ),
                0,
                3129,
            )
            .filepath(Path::new("../test/fixtures/lorem-ipsum.txt")),
            Chunk::new(
                Checksum::SHA256(
                    "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05".to_owned(),
                ),
                0,
                3375,
            )
            .filepath(Path::new("../test/fixtures/washington-journal.txt")),
            Chunk::new(
                Checksum::SHA256(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
                ),
                0,
                0,
            )
            .filepath(Path::new("../test/fixtures/zero-length.txt")),
        ];
        let mut builder = PackBuilder::new(16384);
        let outdir = tempdir()?;
        let packfile = outdir.path().join("small-pack.tar");
        builder.initialize(&packfile)?;
        let mut chunks_written = 0;
        for chunk in chunks.iter() {
            chunks_written += 1;
            if builder.add_chunk(chunk)? {
                panic!("should not have happened");
            }
        }
        assert_eq!(chunks_written, 3);
        let result = builder.finalize()?;
        assert_eq!(result, packfile);
        // simple validation that works on any platform (checksums of plain text on
        // Windows will vary due to end-of-line characters)
        sevenz_rust::decompress_file_with_extract_fn(packfile, outdir, |entry, _, _| {
            assert_eq!(entry.name.len(), 71);
            assert!(entry.name.starts_with("sha256-"));
            Ok(true)
        })?;
        Ok(())
    }

    #[test]
    fn test_pack_builder_multi() -> Result<(), Error> {
        // build a pack file that becomes too full for more chunks
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let chunks = super::super::find_file_chunks(&infile, 16384)?;
        assert_eq!(chunks.len(), 5);
        let mut builder = PackBuilder::new(65536);
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi-pack.tar");
        assert_eq!(builder.is_ready(), false);
        assert_eq!(builder.is_empty(), true);
        builder.initialize(&packfile)?;
        assert_eq!(builder.is_ready(), true);
        assert_eq!(builder.is_empty(), true);
        let mut chunks_written = 0;
        for chunk in chunks.iter() {
            chunks_written += 1;
            if builder.add_chunk(chunk)? {
                break;
            }
        }
        assert_eq!(chunks_written, 4);
        assert_eq!(builder.is_empty(), false);
        let result = builder.finalize()?;
        assert_eq!(result, packfile);
        assert_eq!(builder.is_ready(), false);
        assert_eq!(builder.is_empty(), true);
        // validate by extracting and checksumming all of the chunks
        let entries: Vec<String> = extract_pack(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 4);
        assert_eq!(
            entries[0],
            "sha256-695429afe5937d6c75099f6e587267065a64e9dd83596a3d7386df3ef5a792c2"
        );
        assert_eq!(
            entries[1],
            "sha256-17119f7abc183375afdb652248aad0c7211618d263335cc4e4ffc9a31e719bcb"
        );
        assert_eq!(
            entries[2],
            "sha256-1545925739c6bfbd6609752a0e6ab61854f14d1fdb9773f08a7f52a13f9362d8"
        );
        assert_eq!(
            entries[3],
            "sha256-bbd5b0b284d4e3c2098e92e8e2897e738c669113d06472560188d99a288872a3"
        );
        let part1sum = Checksum::sha256_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part1sum.to_string(),
            "sha256-695429afe5937d6c75099f6e587267065a64e9dd83596a3d7386df3ef5a792c2"
        );
        let part2sum = Checksum::sha256_from_file(&outdir.path().join(&entries[1]))?;
        assert_eq!(
            part2sum.to_string(),
            "sha256-17119f7abc183375afdb652248aad0c7211618d263335cc4e4ffc9a31e719bcb"
        );
        let part3sum = Checksum::sha256_from_file(&outdir.path().join(&entries[2]))?;
        assert_eq!(
            part3sum.to_string(),
            "sha256-1545925739c6bfbd6609752a0e6ab61854f14d1fdb9773f08a7f52a13f9362d8"
        );
        let part4sum = Checksum::sha256_from_file(&outdir.path().join(&entries[3]))?;
        assert_eq!(
            part4sum.to_string(),
            "sha256-bbd5b0b284d4e3c2098e92e8e2897e738c669113d06472560188d99a288872a3"
        );
        Ok(())
    }
}
