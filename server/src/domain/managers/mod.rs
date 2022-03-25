//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Chunk};
use anyhow::{anyhow, Error};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use sodiumoxide::crypto::pwhash::{self, Salt};
use sodiumoxide::crypto::secretstream::{self, Stream, Tag};
use std::fs::{self, File};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::{Duration, SystemTimeError};
use tar::{Archive, Builder, Header};

pub mod backup;
pub mod process;
pub mod restore;
pub mod state;

/// Builds a tar file one chunk at a time, with each chunk compressed
/// separately, with the overall size being not much larger than a set size.
pub struct PackBuilder {
    /// Preferred size of pack file in bytes.
    target_size: u64,
    /// Compressed bytes written to the pack so far.
    bytes_packed: u64,
    /// Tar file builder.
    builder: Option<Builder<File>>,
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
        let builder = Builder::new(file);
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
        let mut handle = infile.take(chunk.length as u64);
        let buffer: Vec<u8> = Vec::new();
        let mut encoder = GzEncoder::new(buffer, flate2::Compression::default());
        io::copy(&mut handle, &mut encoder)?;
        let compressed = encoder.finish()?;
        let compressed_size = compressed.len() as u64;
        let mut header = Header::new_gnu();
        header.set_size(compressed_size);
        // set the date so the tar file produces the same results for the same
        // inputs every time; the date for chunks is completely irrelevant
        header.set_mtime(0);
        header.set_cksum();
        let builder = self
            .builder
            .as_mut()
            .ok_or_else(|| anyhow!("must call initialize() first"))?;
        let filename = chunk.digest.to_string();
        builder.append_data(&mut header, filename, &compressed[..])?;
        self.bytes_packed += compressed_size;
        // Account for the overhead of each tar file entry, which can be
        // significant if there are many (thousands) small files added to a
        // single pack, pushing the pack from the desired size (e.g. 64mb) to
        // something much larger (99mb). The actual overhead for a zero-byte
        // file is more than 1500 bytes but 1024 is closer to the average
        // overhead for a typical file set (about 800 bytes).
        self.bytes_packed += 1024;
        self.chunks_packed += 1;
        Ok(self.bytes_packed >= self.target_size)
    }

    /// Flush pending writes and close the pack file.
    pub fn finalize(&mut self) -> Result<PathBuf, Error> {
        let _output = self
            .builder
            .take()
            .ok_or_else(|| anyhow!("must call initialize() first"))?
            .into_inner()?;
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
pub fn extract_pack(infile: &Path, outdir: &Path) -> io::Result<Vec<String>> {
    fs::create_dir_all(outdir)?;
    let mut results = Vec::new();
    let file = File::open(infile)?;
    let mut ar = Archive::new(file);
    for entry in ar.entries()? {
        let file = entry?;
        let fp = file.path()?;
        // we know the names are valid UTF-8, we created them
        let filename = String::from(fp.to_str().unwrap());
        let mut output_path = outdir.to_path_buf();
        output_path.push(&filename);
        let mut output = File::create(output_path)?;
        let mut decoder = GzDecoder::new(file);
        io::copy(&mut decoder, &mut output)?;
        results.push(filename);
    }
    Ok(results)
}

///
/// Find the chunk boundaries within the given file, using the FastCDC
/// algorithm. The given `size` is the desired average size in bytes for the
/// chunks, but they may be between half and twice that size.
///
pub fn find_file_chunks(infile: &Path, size: u64) -> io::Result<Vec<Chunk>> {
    let file = fs::File::open(infile)?;
    let mmap = unsafe {
        memmap::MmapOptions::new()
            .map(&file)
            .expect("cannot create mmap?")
    };
    let avg_size = size as usize;
    let min_size = avg_size / 2;
    let max_size = avg_size * 2;
    let chunker = fastcdc::FastCDC::new(&mmap[..], min_size, avg_size, max_size);
    let mut results = Vec::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let chksum = Checksum::sha256_from_bytes(&mmap[entry.offset..end]);
        let mut chunk = Chunk::new(chksum, entry.offset, entry.length);
        chunk = chunk.filepath(infile);
        results.push(chunk);
    }
    Ok(results)
}

// Used to avoid initializing the crypto library more than once. Not a
// requirement, but seems sensible and it is easy.
static CRYPTO_INIT: Once = Once::new();

// Initialize the crypto library to improve performance and ensure all of its
// operations are thread-safe.
fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        let _ = sodiumoxide::init();
    });
}

/// Retrieve the user-defined passphrase.
///
/// Returns a default if one has not been defined.
pub fn get_passphrase() -> String {
    std::env::var("PASSPHRASE").unwrap_or_else(|_| "keyboard cat".to_owned())
}

// Hash the given user password using a computationally expensive algorithm.
fn hash_password(passphrase: &str, salt: &Salt) -> Result<secretstream::Key, Error> {
    init_crypto();
    let mut k = secretstream::Key([0; secretstream::KEYBYTES]);
    let secretstream::Key(ref mut kb) = k;
    match pwhash::derive_key(
        kb,
        passphrase.as_bytes(),
        salt,
        pwhash::OPSLIMIT_INTERACTIVE,
        pwhash::MEMLIMIT_INTERACTIVE,
    ) {
        Ok(_) => Ok(k),
        Err(()) => Err(anyhow!("pwhash::derive_key() failed mysteriously")),
    }
}

// Size of the "messages" encrypted with libsodium. We need to read the stream
// back in chunks this size to successfully decrypt.
static CRYPTO_BUFLEN: usize = 8192;

///
/// Encrypt the given file using libsodium stream encryption.
///
/// The passphrase is used with a newly generated salt to produce a secret key,
/// which is then used to encrypt the file. The salt is returned to the caller.
///
pub fn encrypt_file(passphrase: &str, infile: &Path, outfile: &Path) -> Result<Salt, Error> {
    init_crypto();
    let salt = pwhash::gen_salt();
    let key = hash_password(passphrase, &salt)?;
    let attr = fs::symlink_metadata(infile)?;
    let infile_len = attr.len();
    let mut total_bytes_read: u64 = 0;
    let mut buffer = vec![0; CRYPTO_BUFLEN];
    let (mut enc_stream, header) =
        Stream::init_push(&key).map_err(|_| anyhow!("stream init failed"))?;
    let mut input = File::open(infile)?;
    let mut cipher = File::create(outfile)?;
    // Write out a magic/version number for backward compatibility. The magic
    // number is meant to be unique for this file type. The version accounts for
    // any change in the size of the secret stream header, which may change,
    // even if that may be unlikely.
    let version = [b'Z', b'R', b'G', b'M', 0, 0, 0, 1];
    cipher.write_all(&version)?;
    cipher.write_all(header.as_ref())?;
    while total_bytes_read < infile_len {
        let bytes_read = input.read(&mut buffer)?;
        total_bytes_read += bytes_read as u64;
        let tag = if total_bytes_read < infile_len {
            Tag::Message
        } else {
            Tag::Final
        };
        let cipher_text = enc_stream
            .push(&buffer[0..bytes_read], None, tag)
            .map_err(|_| anyhow!("stream push failed"))?;
        cipher.write_all(cipher_text.as_ref())?;
    }
    Ok(salt)
}

///
/// Decrypt the encrypted file using the given passphrase and salt.
///
pub fn decrypt_file(
    passphrase: &str,
    salt: &Salt,
    infile: &Path,
    outfile: &Path,
) -> Result<(), Error> {
    init_crypto();
    let key = hash_password(passphrase, salt)?;
    let mut input = File::open(infile)?;
    // read the magic/version and ensure they match expectations
    let mut version_bytes = [0; 8];
    input.read_exact(&mut version_bytes)?;
    if version_bytes[0..4] != [b'Z', b'R', b'G', b'M'] {
        return Err(anyhow!("pack file missing magic number"));
    }
    if version_bytes[4..8] != [0, 0, 0, 1] {
        return Err(anyhow!("pack file unsupported version"));
    }
    // create a vector with sufficient space to read the header
    let mut header_vec = vec![0; secretstream::HEADERBYTES];
    input.read_exact(&mut header_vec)?;
    let header = secretstream::Header::from_slice(&header_vec)
        .ok_or_else(|| anyhow!("invalid secretstream header"))?;
    // initialize the pull stream
    let mut dec_stream =
        Stream::init_pull(&header, &key).map_err(|_| anyhow!("stream init failed"))?;
    let mut plain = File::create(outfile)?;
    // buffer must be large enough for reading an entire message
    let mut buffer = vec![0; CRYPTO_BUFLEN + secretstream::ABYTES];
    // read the encrypted text until the stream is finalized
    while !dec_stream.is_finalized() {
        let bytes_read = input.read(&mut buffer)?;
        // n.b. this will fail if the read does not get the entire message, but
        // that is unlikely when reading local files
        let (decrypted, _tag) = dec_stream
            .pull(&buffer[0..bytes_read], None)
            .map_err(|_| anyhow!("stream pull failed"))?;
        plain.write_all(decrypted.as_ref())?;
    }
    Ok(())
}

// Return a clear and accurate description of the duration.
pub fn pretty_print_duration(duration: Result<Duration, SystemTimeError>) -> String {
    let mut result = String::new();
    match duration {
        Ok(value) => {
            let mut seconds = value.as_secs();
            if seconds > 3600 {
                let hours = seconds / 3600;
                result.push_str(format!("{} hours ", hours).as_ref());
                seconds -= hours * 3600;
            }
            if seconds > 60 {
                let minutes = seconds / 60;
                result.push_str(format!("{} minutes ", minutes).as_ref());
                seconds -= minutes * 60;
            }
            if seconds > 0 {
                result.push_str(format!("{} seconds", seconds).as_ref());
            } else if result.is_empty() {
                // special case of a zero duration
                result.push_str("0 seconds");
            }
        }
        Err(_) => result.push_str("(error)"),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::Configuration;
    use tempfile::tempdir;

    #[test]
    fn test_file_chunking_16k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 16384)?;
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 22366);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        assert_eq!(results[1].offset, 22366);
        assert_eq!(results[1].length, 8282);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        assert_eq!(results[2].offset, 30648);
        assert_eq!(results[2].length, 16303);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        assert_eq!(results[3].offset, 46951);
        assert_eq!(results[3].length, 18696);
        assert_eq!(
            results[3].digest.to_string(),
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        assert_eq!(results[4].offset, 65647);
        assert_eq!(results[4].length, 32768);
        assert_eq!(
            results[4].digest.to_string(),
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        assert_eq!(results[5].offset, 98415);
        assert_eq!(results[5].length, 11051);
        assert_eq!(
            results[5].digest.to_string(),
            "sha256-a566243537738371133ecff524501290f0621f786f010b45d20a9d5cf82365f8"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_32k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 32768)?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 16408);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-13f6a4c6d42df2b76c138c13e86e1379c203445055c2b5f043a5f6c291fa520d"
        );
        assert_eq!(results[2].offset, 49265);
        assert_eq!(results[2].length, 60201);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-0fe7305ba21a5a5ca9f89962c5a6f3e29cd3e2b36f00e565858e0012e5f8df36"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_64k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 65536)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 76609);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-5420a3bcc7d57eaf5ca9bb0ab08a1bd3e4d89ae019b1ffcec39b1a5905641115"
        );
        Ok(())
    }

    #[test]
    fn test_pack_builder_multi() -> Result<(), Error> {
        // build a pack file that becomes too full for more chunks
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let chunks = find_file_chunks(&infile, 16384)?;
        assert_eq!(chunks.len(), 6);
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
        assert_eq!(chunks_written, 5);
        assert_eq!(builder.is_empty(), false);
        let result = builder.finalize()?;
        assert_eq!(result, packfile);
        assert_eq!(builder.is_ready(), false);
        assert_eq!(builder.is_empty(), true);
        // validate by extracting and checksumming all of the chunks
        let entries: Vec<String> = extract_pack(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 5);
        assert_eq!(
            entries[0],
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        assert_eq!(
            entries[1],
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        assert_eq!(
            entries[2],
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        assert_eq!(
            entries[3],
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        assert_eq!(
            entries[4],
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        let part1sum = Checksum::sha256_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part1sum.to_string(),
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        let part2sum = Checksum::sha256_from_file(&outdir.path().join(&entries[1]))?;
        assert_eq!(
            part2sum.to_string(),
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        let part3sum = Checksum::sha256_from_file(&outdir.path().join(&entries[2]))?;
        assert_eq!(
            part3sum.to_string(),
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        let part4sum = Checksum::sha256_from_file(&outdir.path().join(&entries[3]))?;
        assert_eq!(
            part4sum.to_string(),
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        let part5sum = Checksum::sha256_from_file(&outdir.path().join(&entries[4]))?;
        assert_eq!(
            part5sum.to_string(),
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        Ok(())
    }

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
        let infile = File::open(packfile)?;
        let mut ar = Archive::new(&infile);
        for entry in ar.entries()? {
            let file = entry?;
            let fp = file.path()?;
            let fp_as_str = fp.to_str().unwrap();
            assert_eq!(fp_as_str.len(), 71);
            assert!(fp_as_str.starts_with("sha256-"));
        }
        Ok(())
    }

    #[test]
    fn test_generate_unique_id() {
        let uuid = Configuration::generate_unique_id("charlie", "localhost");
        // UUIDv5 = 747267d5-6e70-5711-8a9a-a40c24c1730f
        assert_eq!(uuid, "dHJn1W5wVxGKmqQMJMFzDw");
    }

    #[test]
    fn test_encryption() -> Result<(), Error> {
        let passphrase = "some passphrase";
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let outdir = tempdir()?;
        let ciphertext = outdir.path().join("SekienAkashita.jpg.enc");
        let salt = encrypt_file(passphrase, infile, &ciphertext)?;
        // cannot do much validation of the encrypted file, it is always
        // going to be different because of random keys and init vectors
        let plaintext = outdir.path().join("SekienAkashita.jpg");
        decrypt_file(passphrase, &salt, &ciphertext, &plaintext)?;
        let plainsum = Checksum::sha256_from_file(&plaintext)?;
        assert_eq!(
            plainsum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_hash_password() -> Result<(), Error> {
        let passwd = "Correct Horse Battery Staple";
        let salt = pwhash::gen_salt();
        let result = hash_password(passwd, &salt)?;
        assert_eq!(result.as_ref().len(), secretstream::KEYBYTES);
        Ok(())
    }

    #[test]
    fn test_pretty_print_duration() {
        let input = Duration::from_secs(0);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "0 seconds");

        let input = Duration::from_secs(5);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "5 seconds");

        let input = Duration::from_secs(65);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 minutes 5 seconds");

        let input = Duration::from_secs(4949);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 hours 22 minutes 29 seconds");

        let input = Duration::from_secs(7300);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 1 minutes 40 seconds");

        let input = Duration::from_secs(10090);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 48 minutes 10 seconds");
    }
}
