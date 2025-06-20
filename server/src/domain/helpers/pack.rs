//
// Copyright (c) 2022 Nathan Fiedler
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

    /// Write the chunk data to the pack file. Returns `true` if the pack file
    /// size has exceeded the value given in [`Self::new()`].
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
        //
        // In practice, creating packs of 64mb for a data set with reasonably
        // sized files (i.e. not all images and videos), the standard deviation
        // of the size of the pack files is about 3 (not counting the very small
        // packs that contained the left over chunks from a backup).
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
/// directory, with the names being the original hash digest of the chunk with
/// the algorithm prefix).
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
                Checksum::BLAKE3(
                    "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128".to_owned(),
                ),
                0,
                3129,
            )
            .filepath(Path::new("../test/fixtures/lorem-ipsum.txt")),
            Chunk::new(
                Checksum::BLAKE3(
                    "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60".to_owned(),
                ),
                0,
                3375,
            )
            .filepath(Path::new("../test/fixtures/washington-journal.txt")),
            Chunk::new(
                Checksum::BLAKE3(
                    "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262".to_owned(),
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
            assert!(entry_name.starts_with("blake3-"));
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
            "blake3-261930e84e14c240210ae8c459acc4bb85dd52f1b91c868f2106dbc1ceb3acca"
        );
        assert_eq!(
            entries[1],
            "blake3-a01747cf21202f0068b8897d2be92aa4479b7ac7207b3baa5057b8ec75fa1c10"
        );
        assert_eq!(
            entries[2],
            "blake3-01e5305fb8f54d214ed2946843ea360fb9bb3f5df66ef3e34fb024d32ebcaee1"
        );
        let part1sum = Checksum::blake3_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part1sum.to_string(),
            "blake3-261930e84e14c240210ae8c459acc4bb85dd52f1b91c868f2106dbc1ceb3acca"
        );
        let part2sum = Checksum::blake3_from_file(&outdir.path().join(&entries[1]))?;
        assert_eq!(
            part2sum.to_string(),
            "blake3-a01747cf21202f0068b8897d2be92aa4479b7ac7207b3baa5057b8ec75fa1c10"
        );
        let part3sum = Checksum::blake3_from_file(&outdir.path().join(&entries[2]))?;
        assert_eq!(
            part3sum.to_string(),
            "blake3-01e5305fb8f54d214ed2946843ea360fb9bb3f5df66ef3e34fb024d32ebcaee1"
        );
        Ok(())
    }

    #[test]
    fn test_pack_builder_jpg() -> Result<(), Error> {
        // build a pack file with a jpeg image
        let chunks = [Chunk::new(
            Checksum::BLAKE3(
                "b740be03e10f454b6f45acdc821822b455aa4ab3721bbe8e3f03923f5cd688b8".to_owned(),
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
            "blake3-b740be03e10f454b6f45acdc821822b455aa4ab3721bbe8e3f03923f5cd688b8"
        );
        let part4sum = Checksum::blake3_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part4sum.to_string(),
            "blake3-b740be03e10f454b6f45acdc821822b455aa4ab3721bbe8e3f03923f5cd688b8"
        );
        Ok(())
    }
}
