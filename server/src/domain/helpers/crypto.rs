//
// Copyright (c) 2022 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use sodiumoxide::crypto::pwhash::{self, Salt};
use sodiumoxide::crypto::secretstream::{self, Stream, Tag};
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::Path;
use std::sync::Once;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::Checksum;
    use tempfile::tempdir;

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
}
