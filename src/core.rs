//
// Copyright (c) 2019 Nathan Fiedler
//
use crypto_hash::{Algorithm, hex_digest, Hasher};
use hex;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use uuid::Uuid;
use ulid::Ulid;

const BUFFER_SIZE: usize = 65536;

///
/// Generate a type 5 UUID based on the given values.
///
pub fn generate_unique_id(username: &str, hostname: &str) -> String {
    let mut name = String::from(username);
    name.push(':');
    name.push_str(hostname);
    let bytes = name.into_bytes();
    Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes).to_hyphenated().to_string()
}

///
/// Generate a suitable bucket name, using a ULID and the given UUID.
///
pub fn generate_bucket_name(unique_id: &str) -> String {
    let shorter = String::from(unique_id).replace("-", "");
    let mut ulid = Ulid::new().to_string();
    ulid.push_str(&shorter);
    ulid.to_lowercase()
}

///
/// Compute the hash digest of the given data. The algorithm must be either
/// "sha1" or "sha256", anything else will panic.
///
pub fn checksum_data(data: &[u8], algo: &str) -> String {
    let algorithm = match algo {
        "sha1" => Algorithm::SHA1,
        "sha256" => Algorithm::SHA256,
        _ => panic!("invalid digest algorithm {}", algo)
    };
    let digest = hex_digest(algorithm, data);
    let mut result = String::from(algo);
    result.push('-');
    result.push_str(&digest);
    result
}

///
/// Compute the hash digest of the given file. The algorithm must be either
/// "sha1" or "sha256", anything else will panic.
///
pub fn checksum_file(infile: &Path, algo: &str) -> io::Result<String> {
    let algorithm = match algo {
        "sha1" => Algorithm::SHA1,
        "sha256" => Algorithm::SHA256,
        _ => panic!("invalid digest algorithm {}", algo)
    };
    let file = File::open(infile)?;
    let mut hasher = Hasher::new(algorithm);
    let mut reader = io::BufReader::with_capacity(BUFFER_SIZE, file);
    loop {
        let length = {
            let buffer = reader.fill_buf()?;
            hasher.write_all(buffer)?;
            buffer.len()
        };
        if length == 0 { break; }
        reader.consume(length);
    }
    let digest = hasher.finish();
    Ok(checksum_from_bytes(&digest, algo))
}

///
/// Convert hash digest bytes to a hex string with an algo prefix.
///
pub fn checksum_from_bytes(hash: &[u8], algo: &str) -> String {
    let mut result = String::from(algo);
    result.push('-');
    result.push_str(&hex::encode(hash));
    result
}

///
/// Convert a checksum string into the bytes of the hash digest. The checksum
/// value must start with one of the support digest algorithm names, such as
/// "sha1-" or "sha256-", otherwise the function panics.
///
pub fn bytes_from_checksum(value: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if value.starts_with("sha1-") {
        hex::decode(&value[5..])
    } else if value.starts_with("sha256-") {
        hex::decode(&value[7..])
    } else {
        panic!("value does not begin with a supported algorithm name")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unique_id() {
        let uuid = generate_unique_id("charlie", "localhost");
        assert_eq!(uuid, "747267d5-6e70-5711-8a9a-a40c24c1730f");
    }

    #[test]
    fn test_generate_bucket_name() {
        let uuid = generate_unique_id("charlie", "localhost");
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
    #[should_panic]
    fn test_checksum_data_bad_algo() {
        let data = b"crypto-hash";
        checksum_data(data, "md5");
    }

    #[test]
    fn test_checksum_data() {
        let data = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
        let sha1 = checksum_data(data, "sha1");
        assert_eq!(sha1, "sha1-e7505beb754bed863e3885f73e3bb6866bdd7f8c");
        let sha256 = checksum_data(data, "sha256");
        assert_eq!(sha256, "sha256-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433");
    }

    #[test]
    #[should_panic]
    fn test_checksum_file_bad_algo() {
        let infile = Path::new("foobar");
        match checksum_file(&infile, "md5") {
            Ok(_) => unreachable!(),
            Err(_) => unreachable!()
        }
    }

    #[test]
    fn test_checksum_file() -> Result<(), io::Error> {
        // use a file larger than the buffer size used for hashing
        let infile = Path::new("./test/fixtures/SekienAkashita.jpg");
        let sha1 = checksum_file(&infile, "sha1")?;
        assert_eq!(sha1, "sha1-4c009e44fe5794df0b1f828f2a8c868e66644964");
        let sha256 = checksum_file(&infile, "sha256")?;
        assert_eq!(sha256, "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed");
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_bytes_from_checksum_bad_algo() {
        let checksum = "md5-d8e98fb5f0ee8a4af37b14a0c605f17c";
        match bytes_from_checksum(checksum) {
            Ok(_) => unreachable!(),
            Err(_) => unreachable!()
        }
    }

    #[test]
    fn test_checksum_to_bytes_roundtrip() -> Result<(), hex::FromHexError> {
        let checksum = "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed";
        let bytes = bytes_from_checksum(checksum)?;
        let roundtrip = checksum_from_bytes(&bytes, "sha256");
        assert_eq!(roundtrip, checksum);
        Ok(())
    }
}
