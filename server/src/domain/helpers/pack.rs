//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Chunk;
use anyhow::{anyhow, Context, Error};
use exaf_rs::writer::{Options, Writer};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

/// Builds a compressed archive one chunk at a time.
pub struct PackBuilder {
    /// Preferred size of pack file in bytes.
    target_size: u64,
    /// Optional password to enable encryption of the archive.
    password: Option<String>,
    /// Compressed bytes written to the pack so far.
    bytes_packed: u64,
    /// Archive writer.
    builder: Option<Writer<File>>,
    /// Path of the output file.
    filepath: Option<PathBuf>,
    /// Number of chunks added to the pack.
    chunks_packed: u32,
}

impl PackBuilder {
    /// Construct a builder that will produce a compressed archive up to
    /// approximately `target_size` bytes in length.
    pub fn new(target_size: u64) -> Self {
        Self {
            target_size,
            password: None,
            bytes_packed: 0,
            builder: None,
            filepath: None,
            chunks_packed: 0,
        }
    }

    /// Set the password which will enable encryption of the archive.
    pub fn password<S: Into<String>>(mut self, password: S) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Returns `true` if the builder has been initialized and is ready to
    /// receive chunks.
    pub fn is_ready(&self) -> bool {
        self.builder.is_some()
    }

    /// Returns `true` if the builder has exceeded the target size.
    pub fn is_full(&self) -> bool {
        self.bytes_packed >= self.target_size
    }

    /// Returns `true` if there are no chunks in the pack file.
    pub fn is_empty(&self) -> bool {
        self.chunks_packed == 0
    }

    /// Initialize the builder for the given output path.
    pub fn initialize(&mut self, outfile: &Path) -> Result<(), Error> {
        self.filepath = Some(outfile.to_path_buf());
        let file = File::create(outfile)?;
        // some use cases will need the pack sizes in the archive
        let options = Options::new().file_size(true);
        let mut builder = Writer::with_options(file, options)?;
        if let Some(ref passwd) = self.password {
            builder.enable_encryption(
                exaf_rs::KeyDerivation::Argon2id,
                exaf_rs::Encryption::AES256GCM,
                passwd,
            )?;
        }
        self.builder = Some(builder);
        Ok(())
    }

    /// Write the chunk data in compressed form to the pack file. Returns `true`
    /// if the compressed data has exceeded the pack size given in `new()`.
    pub fn add_chunk(&mut self, chunk: &Chunk) -> Result<bool, Error> {
        if self.bytes_packed > self.target_size {
            return Err(anyhow!("pack already full"));
        }
        let filepath = chunk
            .filepath
            .as_ref()
            .ok_or_else(|| anyhow!("chunk requires a filepath"))?;
        let builder = self
            .builder
            .as_mut()
            .ok_or_else(|| anyhow!("must call initialize() first"))?;
        let filename = chunk.digest.to_string();
        builder.add_file_slice(
            filepath,
            filename,
            None,
            chunk.offset as u64,
            chunk.length as u32,
        )?;
        // Note that bytes_written() is only updated when a manifest/content
        // pair are committed to the exaf archive, as such this will be wrong by
        // a wide margin (~16mb). Suitable for realistic pack sizes that are a
        // multiple of 16mb, but terrible for unit tests with small files.
        //
        // As such, hack the pack size limit to be based on the available data,
        // which is close enough for the purpose of testing the pack behavior.
        if cfg!(test) {
            self.bytes_packed += chunk.length as u64;
        } else {
            self.bytes_packed = builder.bytes_written();
        }
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
pub fn extract_pack(
    infile: &Path,
    outdir: &Path,
    password: Option<&str>,
) -> Result<Vec<String>, Error> {
    fs::create_dir_all(outdir)
        .with_context(|| format!("extract_pack fs::create_dir_all({})", outdir.display()))?;
    let mut results = Vec::new();
    // first get the list of entries from the archive
    let mut reader = exaf_rs::reader::Entries::new(infile)?;
    if let Some(passwd) = password {
        reader.enable_encryption(passwd)?;
    }
    for result in reader {
        let entry = result?;
        results.push(entry.name().to_string());
    }

    // extract the files to the target directory
    let mut reader = exaf_rs::reader::from_file(infile)?;
    if let Some(passwd) = password {
        reader.enable_encryption(passwd)?;
    }
    reader.extract_all(&outdir)?;
    Ok(results)
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
        let packfile = outdir.path().join("small-pack.pack");
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
        let reader = exaf_rs::reader::Entries::new(packfile)?;
        for result in reader {
            let entry = result?;
            let entry_name = entry.name();
            assert_eq!(entry_name.len(), 71);
            assert!(entry_name.starts_with("sha256-"));
        }
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
        let packfile = outdir.path().join("archive.pack");
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
        assert_eq!(chunks_written, 3);
        assert_eq!(builder.is_empty(), false);
        let result = builder.finalize()?;
        assert_eq!(result, packfile);
        assert_eq!(builder.is_ready(), false);
        assert_eq!(builder.is_empty(), true);
        // validate by extracting and checksumming all of the chunks
        let entries: Vec<String> = extract_pack(&packfile, outdir.path(), None)?;
        assert_eq!(entries.len(), 3);
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
        Ok(())
    }

    #[test]
    fn test_pack_builder_jpg() -> Result<(), Error> {
        // build a pack file with a jpeg image
        let chunks = [Chunk::new(
            Checksum::SHA256(
                "aafd64b759b896ceed90c88625c08f215f2a3b0a01ccf47e64239875c5710aa6".to_owned(),
            ),
            0,
            1272254,
        )
        .filepath(Path::new("../test/fixtures/C++98-tutorial.pdf"))];
        let mut builder = PackBuilder::new(4194304);
        let outdir = tempdir()?;
        fs::create_dir_all(&outdir)?;
        let packfile = outdir.path().join("bigger-pack.pack");
        builder.initialize(&packfile)?;
        let mut chunks_written = 0;
        for chunk in chunks.iter() {
            chunks_written += 1;
            if builder.add_chunk(chunk)? {
                panic!("should not have happened");
            }
        }
        assert_eq!(chunks_written, 1);
        let result = builder.finalize()?;
        assert_eq!(result, packfile);
        // validate by extracting and hashing all of the chunks
        let entries: Vec<String> = extract_pack(&packfile, outdir.path(), None)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            "sha256-aafd64b759b896ceed90c88625c08f215f2a3b0a01ccf47e64239875c5710aa6"
        );
        let part4sum = Checksum::sha256_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part4sum.to_string(),
            "sha256-aafd64b759b896ceed90c88625c08f215f2a3b0a01ccf47e64239875c5710aa6"
        );
        Ok(())
    }
}
