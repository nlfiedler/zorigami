//
// Copyright (c) 2019 Nathan Fiedler
//
use crypto_hash::{hex_digest, Algorithm, Hasher};
use fastcdc;
use hex;
use memmap::MmapOptions;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use ulid::Ulid;
use uuid::Uuid;

const BUFFER_SIZE: usize = 65536;

///
/// Generate a type 5 UUID based on the given values.
///
pub fn generate_unique_id(username: &str, hostname: &str) -> String {
    let mut name = String::from(username);
    name.push(':');
    name.push_str(hostname);
    let bytes = name.into_bytes();
    Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes)
        .to_hyphenated()
        .to_string()
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
        _ => panic!("invalid digest algorithm {}", algo),
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
        _ => panic!("invalid digest algorithm {}", algo),
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
        if length == 0 {
            break;
        }
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

/// Some chunk of a file.
pub struct Chunk {
    /// The SHA256 checksum of the chunk, with algo prefix.
    pub digest: String,
    /// The byte offset of this chunk within the file.
    pub offset: usize,
    /// The byte length of this chunk.
    pub length: usize,
}

///
/// Find the chunk boundaries within the given file, using the FastCDC
/// algorithm. The given `size` is the desired average size in bytes for the
/// chunks, but they may be between half and twice that size.
///
pub fn find_file_chunks(infile: &Path, size: u32) -> io::Result<Vec<Chunk>> {
    let file = File::open(infile)?;
    let mmap = unsafe { MmapOptions::new().map(&file).expect("cannot create mmap?") };
    let avg_size = size as usize;
    let min_size = avg_size / 2;
    let max_size = avg_size * 2;
    let chunker = fastcdc::FastCDC::new(&mmap[..], min_size, avg_size, max_size);
    let mut results = Vec::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let mut digest = String::from("sha256-");
        digest.push_str(&hex_digest(Algorithm::SHA256, &mmap[entry.offset..end]));
        results.push(Chunk {
            digest,
            offset: entry.offset,
            length: entry.length,
        })
    }
    Ok(results)
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
        assert_eq!(
            sha256,
            "sha256-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433"
        );
    }

    #[test]
    #[should_panic]
    fn test_checksum_file_bad_algo() {
        let infile = Path::new("foobar");
        match checksum_file(&infile, "md5") {
            Ok(_) => unreachable!(),
            Err(_) => unreachable!(),
        }
    }

    #[test]
    fn test_checksum_file() -> Result<(), io::Error> {
        // use a file larger than the buffer size used for hashing
        let infile = Path::new("./test/fixtures/SekienAkashita.jpg");
        let sha1 = checksum_file(&infile, "sha1")?;
        assert_eq!(sha1, "sha1-4c009e44fe5794df0b1f828f2a8c868e66644964");
        let sha256 = checksum_file(&infile, "sha256")?;
        assert_eq!(
            sha256,
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_bytes_from_checksum_bad_algo() {
        let checksum = "md5-d8e98fb5f0ee8a4af37b14a0c605f17c";
        match bytes_from_checksum(checksum) {
            Ok(_) => unreachable!(),
            Err(_) => unreachable!(),
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

    #[test]
    fn test_file_chunking_16k() -> io::Result<()> {
        let infile = Path::new("./test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(infile, 16384)?;
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 22366);
        assert_eq!(
            results[0].digest,
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        assert_eq!(results[1].offset, 22366);
        assert_eq!(results[1].length, 8282);
        assert_eq!(
            results[1].digest,
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        assert_eq!(results[2].offset, 30648);
        assert_eq!(results[2].length, 16303);
        assert_eq!(
            results[2].digest,
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        assert_eq!(results[3].offset, 46951);
        assert_eq!(results[3].length, 18696);
        assert_eq!(
            results[3].digest,
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        assert_eq!(results[4].offset, 65647);
        assert_eq!(results[4].length, 32768);
        assert_eq!(
            results[4].digest,
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        assert_eq!(results[5].offset, 98415);
        assert_eq!(results[5].length, 11051);
        assert_eq!(
            results[5].digest,
            "sha256-a566243537738371133ecff524501290f0621f786f010b45d20a9d5cf82365f8"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_32k() -> io::Result<()> {
        let infile = Path::new("./test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(infile, 32768)?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest,
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 16408);
        assert_eq!(
            results[1].digest,
            "sha256-13f6a4c6d42df2b76c138c13e86e1379c203445055c2b5f043a5f6c291fa520d"
        );
        assert_eq!(results[2].offset, 49265);
        assert_eq!(results[2].length, 60201);
        assert_eq!(
            results[2].digest,
            "sha256-0fe7305ba21a5a5ca9f89962c5a6f3e29cd3e2b36f00e565858e0012e5f8df36"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_64k() -> io::Result<()> {
        let infile = Path::new("./test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(infile, 65536)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest,
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 76609);
        assert_eq!(
            results[1].digest,
            "sha256-5420a3bcc7d57eaf5ca9bb0ab08a1bd3e4d89ae019b1ffcec39b1a5905641115"
        );
        Ok(())
    }
}
