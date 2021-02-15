//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Chunk};
use failure::{err_msg, Error};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use log::error;
use rusty_ulid::generate_ulid_string;
use sodiumoxide::crypto::pwhash::{self, Salt};
use sodiumoxide::crypto::secretstream::{self, Stream, Tag};
use std::fs::{self, File};
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Once;
use std::time::{Duration, SystemTimeError};
use tar::{Archive, Builder, Header};

pub mod backup;
pub mod process;
pub mod restore;
pub mod state;

///
/// Write a sequence of chunks into a pack file, returning the SHA256 of the
/// pack file. The chunks will be written in the order they appear in the array.
///
pub fn pack_chunks(chunks: &[Chunk], outfile: &Path) -> io::Result<Checksum> {
    let file = File::create(outfile)?;
    let mut builder = Builder::new(file);
    for chunk in chunks {
        let fp = chunk.filepath.as_ref().expect("chunk requires a filepath");
        let mut infile = File::open(fp)?;
        infile.seek(io::SeekFrom::Start(chunk.offset as u64))?;
        let handle = infile.take(chunk.length as u64);
        let mut header = Header::new_gnu();
        header.set_size(chunk.length as u64);
        // set the date so the tar file produces the same results for the same
        // inputs every time; the date for chunks is completely irrelevant
        header.set_mtime(0);
        header.set_cksum();
        let filename = chunk.digest.to_string();
        builder.append_data(&mut header, filename, handle)?;
    }
    let _output = builder.into_inner()?;
    Checksum::sha256_from_file(outfile)
}

///
/// Extract the chunks from the given pack file, writing them to the output
/// directory, with the names being the original SHA256 of the chunk (with a
/// "sha256-" prefix).
///
pub fn unpack_chunks(infile: &Path, outdir: &Path) -> io::Result<Vec<String>> {
    fs::create_dir_all(outdir)?;
    let mut results = Vec::new();
    let file = File::open(infile)?;
    let mut ar = Archive::new(file);
    for entry in ar.entries()? {
        let mut file = entry?;
        let fp = file.path()?;
        // we know the names are valid UTF-8, we created them
        results.push(String::from(fp.to_str().unwrap()));
        file.unpack_in(outdir)?;
    }
    Ok(results)
}

///
/// Copy the chunk files to the given output location, deleting the chunks as
/// each one is copied.
///
pub fn assemble_chunks(chunks: &[&Path], outfile: &Path) -> io::Result<()> {
    let mut file = File::create(outfile)?;
    for infile in chunks {
        let mut cfile = File::open(infile)?;
        io::copy(&mut cfile, &mut file)?;
        fs::remove_file(infile)?;
    }
    Ok(())
}

///
/// Generate a suitable bucket name, using a ULID and the given unique ID.
///
/// The unique ID is assumed to be a shorted version of the UUID returned from
/// `generate_unique_id()`, and will be converted back to a full UUID for the
/// purposes of generating a bucket name consisting only of lowercase letters.
///
pub fn generate_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => {
            let shorter = uuid.to_simple().to_string();
            let mut ulid = generate_ulid_string();
            ulid.push_str(&shorter);
            ulid.to_lowercase()
        }
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            generate_ulid_string().to_lowercase()
        }
    }
}

///
/// Compress the file at the given path using zlib.
///
pub fn compress_file(infile: &Path, outfile: &Path) -> Result<(), Error> {
    let mut input = File::open(infile)?;
    let output = File::create(outfile)?;
    let mut encoder = ZlibEncoder::new(output, Compression::default());
    io::copy(&mut input, &mut encoder)?;
    Ok(())
}

///
/// Decompress the zlib-encoded file at the given path.
///
pub fn decompress_file(infile: &Path, outfile: &Path) -> Result<(), Error> {
    let input = File::open(infile)?;
    let mut output = File::create(outfile)?;
    let mut decoder = ZlibDecoder::new(input);
    io::copy(&mut decoder, &mut output)?;
    Ok(())
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
        Err(()) => Err(err_msg("pwhash::derive_key() failed mysteriously")),
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
        Stream::init_push(&key).map_err(|_| err_msg("stream init failed"))?;
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
            .map_err(|_| err_msg("stream push failed"))?;
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
        return Err(err_msg("pack file missing magic number"));
    }
    if version_bytes[4..8] != [0, 0, 0, 1] {
        return Err(err_msg("pack file unsupported version"));
    }
    // create a vector with sufficient space to read the header
    let mut header_vec = vec![0; secretstream::HEADERBYTES];
    input.read_exact(&mut header_vec)?;
    let header = secretstream::Header::from_slice(&header_vec)
        .ok_or_else(|| err_msg("invalid secretstream header"))?;
    // initialize the pull stream
    let mut dec_stream =
        Stream::init_pull(&header, &key).map_err(|_| err_msg("stream init failed"))?;
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
            .map_err(|_| err_msg("stream pull failed"))?;
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
    fn test_pack_file_one_chunk() -> io::Result<()> {
        let chunks = [Chunk::new(
            Checksum::SHA256(
                "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f".to_owned(),
            ),
            0,
            3129,
        )
        .filepath(Path::new("../test/fixtures/lorem-ipsum.txt"))];
        let outdir = tempdir()?;
        let packfile = outdir.path().join("pack.tar");
        let digest = pack_chunks(&chunks[..], &packfile)?;
        #[cfg(target_family = "unix")]
        assert_eq!(
            digest.to_string(),
            "sha256-9fd73dfe8b3815ebbf9b0932816306526104336017d9ba308e37e48bce5ab150"
        );
        // line endings differ
        #[cfg(target_family = "windows")]
        assert_eq!(
            digest.to_string(),
            "sha256-b917dfd10f50d2f6eee14f822df5bcca89c0d02d29ed5db372c32c97a41ba837"
        );
        // verify by unpacking
        let entries: Vec<String> = unpack_chunks(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        let sha256 = Checksum::sha256_from_file(&outdir.path().join(&entries[0]))?;
        #[cfg(target_family = "unix")]
        assert_eq!(
            sha256.to_string(),
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        #[cfg(target_family = "windows")]
        assert_eq!(
            sha256.to_string(),
            "sha256-a8ff0257a5fe4fa03ad46d33805b08c7e889a573898d295e0a653cdcdb0250c9"
        );
        Ok(())
    }

    #[test]
    fn test_pack_file_multiple_chunks() -> io::Result<()> {
        let chunks = [
            Chunk::new(
                Checksum::SHA256(
                    "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
                ),
                0,
                40000,
            )
            .filepath(Path::new("../test/fixtures/SekienAkashita.jpg")),
            Chunk::new(
                Checksum::SHA256(
                    "cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f".to_owned(),
                ),
                40000,
                40000,
            )
            .filepath(Path::new("../test/fixtures/SekienAkashita.jpg")),
            Chunk::new(
                Checksum::SHA256(
                    "e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464".to_owned(),
                ),
                80000,
                29466,
            )
            .filepath(Path::new("../test/fixtures/SekienAkashita.jpg")),
        ];
        let outdir = tempdir()?;
        let packfile = outdir.path().join("pack.tar");
        let digest = pack_chunks(&chunks, &packfile)?;
        assert_eq!(
            digest.to_string(),
            "sha256-0715334707315e0b16e1786d0a76ff70929b5671a2081da78970a652431b4a74"
        );
        // verify by unpacking
        let entries: Vec<String> = unpack_chunks(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
        );
        assert_eq!(
            entries[1],
            "sha256-cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f"
        );
        assert_eq!(
            entries[2],
            "sha256-e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464"
        );
        let part1sum = Checksum::sha256_from_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part1sum.to_string(),
            "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
        );
        let part2sum = Checksum::sha256_from_file(&outdir.path().join(&entries[1]))?;
        assert_eq!(
            part2sum.to_string(),
            "sha256-cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f"
        );
        let part3sum = Checksum::sha256_from_file(&outdir.path().join(&entries[2]))?;
        assert_eq!(
            part3sum.to_string(),
            "sha256-e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464"
        );
        // test reassembling the file again
        let outfile = outdir.path().join("SekienAkashita.jpg");
        let part1 = outdir.path().join(&entries[0]);
        let part2 = outdir.path().join(&entries[1]);
        let part3 = outdir.path().join(&entries[2]);
        let parts = [part1.as_path(), part2.as_path(), part3.as_path()];
        assemble_chunks(&parts[..], &outfile)?;
        let allsum = Checksum::sha256_from_file(&outfile)?;
        assert_eq!(
            allsum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_generate_unique_id() {
        let uuid = Configuration::generate_unique_id("charlie", "localhost");
        // UUIDv5 = 747267d5-6e70-5711-8a9a-a40c24c1730f
        assert_eq!(uuid, "dHJn1W5wVxGKmqQMJMFzDw");
    }

    #[test]
    fn test_generate_bucket_name() {
        let uuid = Configuration::generate_unique_id("charlie", "localhost");
        let bucket = generate_bucket_name(&uuid);
        // Ensure the generated name is safe for the "cloud", which so far means
        // Google Cloud Storage and Amazon Glacier. It needs to be reasonably
        // short, must consist only of lowercase letters or digits.
        assert_eq!(bucket.len(), 58, "bucket name is 58 characters");
        for c in bucket.chars() {
            assert!(c.is_ascii_alphanumeric());
            if c.is_ascii_alphabetic() {
                assert!(c.is_ascii_lowercase());
            }
        }
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
