//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Chunk};
use std::fs;
use std::io;
use std::path::Path;

pub mod crypto;
pub mod pack;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
