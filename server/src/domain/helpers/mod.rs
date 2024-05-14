//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Chunk};
use fastcdc::v2020::FastCDC;
use memmap2::Mmap;
use std::fs;
use std::io;
use std::path::Path;

pub mod crypto;
pub mod pack;
pub mod thread_pool;

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
        let chksum = Checksum::blake3_from_bytes(&mmap[entry.offset..end]);
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
            "blake3-261930e84e14c240210ae8c459acc4bb85dd52f1b91c868f2106dbc1ceb3acca"
        );
        assert_eq!(results[1].offset, 21325);
        assert_eq!(results[1].length, 17140);
        assert_eq!(
            results[1].digest.to_string(),
            "blake3-a01747cf21202f0068b8897d2be92aa4479b7ac7207b3baa5057b8ec75fa1c10"
        );
        assert_eq!(results[2].offset, 38465);
        assert_eq!(results[2].length, 28084);
        assert_eq!(
            results[2].digest.to_string(),
            "blake3-01e5305fb8f54d214ed2946843ea360fb9bb3f5df66ef3e34fb024d32ebcaee1"
        );
        assert_eq!(results[3].offset, 66549);
        assert_eq!(results[3].length, 18217);
        assert_eq!(
            results[3].digest.to_string(),
            "blake3-fc28c67b6ef846a841452a215bf704058f65cba5c1d78160398d3c2e046642f9"
        );
        assert_eq!(results[4].offset, 84766);
        assert_eq!(results[4].length, 24700);
        assert_eq!(
            results[4].digest.to_string(),
            "blake3-f6996300fce24d3da56c81ea52e5f4f461ce6adb4496f65252996e1082471aac"
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
            "blake3-c3a9c101999bcd14212cbac34a78a5018c6d1548a32c084f43499c254adf07ef"
        );
        assert_eq!(results[1].offset, 66549);
        assert_eq!(results[1].length, 42917);
        assert_eq!(
            results[1].digest.to_string(),
            "blake3-4b5f350ca573fc4f44b0da18d6aef9cdb2bcb7eeab1ad371af82557d0f353454"
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
            "blake3-dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f"
        );
        Ok(())
    }
}
