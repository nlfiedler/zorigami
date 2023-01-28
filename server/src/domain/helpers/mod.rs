//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Chunk};
use fastcdc::v2020::FastCDC;
use memmap2::Mmap;
use std::fs;
use std::io;
use std::path::Path;

pub mod crypto;
pub mod pack;

///
/// Find the chunk boundaries within the given file, using the FastCDC
/// algorithm. The `avg_size` is the desired average size in bytes for the
/// chunks, however the min/max sizes will be 0.25/4 times that size.
///
pub fn find_file_chunks(infile: &Path, avg_size: u32) -> io::Result<Vec<Chunk>> {
    let file = fs::File::open(infile)?;
    let mmap = unsafe { Mmap::map(&file).expect("cannot create memmap?") };
    let min_size = avg_size / 4;
    let max_size = avg_size * 4;
    let chunker = FastCDC::new(&mmap[..], min_size, avg_size, max_size);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_chunking_16k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 16384)?;
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 21325);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-695429afe5937d6c75099f6e587267065a64e9dd83596a3d7386df3ef5a792c2"
        );
        assert_eq!(results[1].offset, 21325);
        assert_eq!(results[1].length, 17140);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-17119f7abc183375afdb652248aad0c7211618d263335cc4e4ffc9a31e719bcb"
        );
        assert_eq!(results[2].offset, 38465);
        assert_eq!(results[2].length, 28084);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-1545925739c6bfbd6609752a0e6ab61854f14d1fdb9773f08a7f52a13f9362d8"
        );
        assert_eq!(results[3].offset, 66549);
        assert_eq!(results[3].length, 18217);
        assert_eq!(
            results[3].digest.to_string(),
            "sha256-bbd5b0b284d4e3c2098e92e8e2897e738c669113d06472560188d99a288872a3"
        );
        assert_eq!(results[4].offset, 84766);
        assert_eq!(results[4].length, 24700);
        assert_eq!(
            results[4].digest.to_string(),
            "sha256-ede34e1a6cb287766e857eb0ed45b9f4b5ad83bb93c597be880c3a2ac91cddbe"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_32k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 32768)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 66549);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-c451d8d136529890c3ecc169177c036029d2b684f796f254bf795c96783fc483"
        );
        assert_eq!(results[1].offset, 66549);
        assert_eq!(results[1].length, 42917);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-b4da74176d97674c78baa2765c77f0ccf4a9602f229f6d2b565cf94447ac7af0"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_64k() -> io::Result<()> {
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 65536)?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 109466);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }
}
