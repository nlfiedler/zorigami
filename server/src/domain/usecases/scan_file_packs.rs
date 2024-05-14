//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Checksum;
use crate::domain::helpers::crypto;
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Context, Error};
use log::{error, info};
use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::fs;
use std::str::FromStr;

// Currently unused, but would be useful if there is a file record for which the
// pack references are incorrect. In that case, this usecase would scan all pack
// files to find the chunks that are associated with the given file.

///
/// Scan all packs in the system to find the chunks that belong to a file.
///
/// The presumption is that the metadata in the database is incorrect about the
/// pack digest for the set of chunks belonging to a file. There was a bug in
/// which the wrong pack digest would be recorded for a file, making retrieval
/// of said file impossible. This usecase seeks to track down that missing chunk
/// and report its true location by scanning the entries of each pack file.
///
pub struct ScanPacks {
    repo: Box<dyn RecordRepository>,
}

impl ScanPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl<'a> super::UseCase<Vec<ChunkLocation>, Params<'a>> for ScanPacks {
    fn call(&self, params: Params) -> Result<Vec<ChunkLocation>, Error> {
        // get the dataset and its associated pack stores
        let dataset = self
            .repo
            .get_dataset(&params.dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset_id)))?;
        let stores = self.repo.load_dataset_stores(&dataset)?;
        fs::create_dir_all(&dataset.workspace).context("creating workspace")?;
        // retrieve the file record to get the chunk digests
        let file_rec = self
            .repo
            .get_file(&params.file_digest)?
            .ok_or_else(|| anyhow!(format!("no such file: {}", params.file_digest)))?;
        // get all packs in the entire system
        let all_packs = self.repo.get_all_packs()?;
        info!("ScanPacks: will scan {} packs", all_packs.len());
        let chunk_count = file_rec.chunks.len();
        let mut results: Vec<ChunkLocation> = Vec::new();
        for pack in all_packs.iter() {
            info!("ScanPacks: scanning pack {}", &pack.digest);
            // check the salt before downloading the pack, otherwise we waste
            // time fetching it when we would not be able to decrypt it
            let salt = pack
                .crypto_salt
                .ok_or_else(|| anyhow!(format!("missing pack salt: {:?}", &pack.digest)))?;
            // retrieve and decrypt the pack file
            let archive = tempfile::Builder::new()
                .suffix(".pack")
                .tempfile_in(&dataset.workspace)?;
            let result = stores.retrieve_pack(&pack.locations, archive.path());
            if result.is_err() {
                error!(
                    "ScanPacks: unable to retrieve pack {}: {:?}",
                    &pack.digest, result
                );
                continue;
            }
            // scan the contents of the tar file using an n^2 search, which is
            // acceptable because most pack files have very few chunks
            let reader = exaf_rs::reader::Entries::new(&archive)?;
            reader.enable_encryption(&params.passphrase)?;
            for maybe_entry in reader {
                let entry = maybe_entry?;
                // we know the names are valid UTF-8, we created them
                let digest = Checksum::from_str(entry.name())?;
                if chunk_count == 1 {
                    // for files with a single chunk, compare the file digest
                    // with the current entry from the pack file
                    if digest == params.file_digest {
                        results.push(ChunkLocation {
                            chunk_digest: digest,
                            old_pack_digest: file_rec.chunks[0].1.clone(),
                            new_pack_digest: pack.digest.clone(),
                        });
                    }
                } else {
                    // for files with multiple chunks, compare each chunk digest
                    // with the current entry from the pack file
                    for chunk in file_rec.chunks.iter() {
                        if chunk.1 == digest {
                            // fetch the chunk record to get the wrong pack digest
                            let chunk_rec = self
                                .repo
                                .get_chunk(&chunk.1)?
                                .ok_or_else(|| anyhow!(format!("no such chunk: {}", chunk.1)))?;
                            let pack_digest = chunk_rec.packfile.ok_or_else(|| {
                                anyhow!(format!("chunk missing its pack: {}", chunk.1))
                            })?;
                            results.push(ChunkLocation {
                                chunk_digest: digest,
                                old_pack_digest: pack_digest,
                                new_pack_digest: pack.digest.clone(),
                            });
                            break;
                        }
                    }
                }
            }
            // stop when all chunks are accounted for
            if results.len() == chunk_count {
                break;
            }
        }
        // return the list of chunk/pack digest pairs
        Ok(results)
    }
}

pub struct Params<'a> {
    /// Identifier of the dataset whose packs will be scanned.
    dataset_id: Cow<'a, str>,
    /// Digest of the file whose chunks are to be located.
    file_digest: Checksum,
    /// Pass phrase for decrypting the pack.
    passphrase: Cow<'a, str>,
}

impl<'a> Params<'a> {
    pub fn new<T: Into<String>>(dataset_id: T, file_digest: Checksum, passphrase: T) -> Self {
        Self {
            dataset_id: Cow::from(dataset_id.into()),
            file_digest,
            passphrase: Cow::from(passphrase.into()),
        }
    }
}

impl<'a> fmt::Display for Params<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({}, {})", self.dataset_id, self.file_digest)
    }
}

impl<'a> cmp::PartialEq for Params<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.dataset_id == other.dataset_id && self.file_digest == other.file_digest
    }
}

impl<'a> cmp::Eq for Params<'a> {}

/// The old (wrong) pack digest and the new (correct) pack digest for a chunk.
#[derive(Debug)]
pub struct ChunkLocation {
    pub chunk_digest: Checksum,
    pub old_pack_digest: Checksum,
    pub new_pack_digest: Checksum,
}

impl fmt::Display for ChunkLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChunkLocation({}, {} -> {})",
            self.chunk_digest, self.old_pack_digest, self.new_pack_digest
        )
    }
}

impl cmp::PartialEq for ChunkLocation {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_digest == other.chunk_digest
            && self.old_pack_digest == other.old_pack_digest
            && self.new_pack_digest == other.new_pack_digest
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Checksum, Chunk, Dataset, File, Pack, PackLocation};
    use crate::domain::helpers::{self, crypto, pack};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_scan_packs_missing_dataset() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset().returning(move |_| Ok(None));
        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing dataset"));
    }

    #[test]
    fn test_scan_packs_no_such_file() {
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/scan_packs"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let mock_store = MockPackRepository::new();
            Ok(Box::new(mock_store))
        });
        mock.expect_get_file().returning(|_| Ok(None));
        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no such file: blake3-deadbeef"));
    }

    #[test]
    fn test_scan_packs_not_any_packs() {
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/scan_packs"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let mock_store = MockPackRepository::new();
            Ok(Box::new(mock_store))
        });
        mock.expect_get_all_packs().returning(|| Ok(Vec::new()));
        mock.expect_get_file().returning(|_| {
            Ok(Some(File::new(
                Checksum::BLAKE3("deadbeef".into()),
                0,
                vec![],
            )))
        });
        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_scan_packs_single_chunk() -> Result<(), Error> {
        // build pack file containing a file with one chunk
        let infile = Path::new("../test/fixtures/lorem-ipsum.txt");
        let mut builder = pack::PackBuilder::new(1048576).password("keyboard cat");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("single.pack");
        // chunk1 digest is also the file digest
        let chunk1_sha = "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128";
        builder.initialize(&packfile)?;
        let mut chunk = Chunk::new(Checksum::BLAKE3(chunk1_sha.into()), 0, 3129);
        chunk = chunk.filepath(infile);
        builder.add_chunk(&chunk)?;
        let _result = builder.finalize()?;

        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/scan_packs"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let packfile_path = packfile.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::rename(packfile_path.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_all_packs().returning(move || {
            // this pack digest will be captured as the correct ("new") value
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(vec![Pack::new(pack_sum, locations)])
        });
        mock.expect_get_file().returning(move |_| {
            // intentionally reporting the wrong pack digest for this file's "chunks"
            let file_chunks: Vec<(u64, Checksum)> = vec![(
                0,
                Checksum::SHA1("d535524bd023d0d22a3912a472c5b0f2db111690".into()),
            )];
            Ok(Some(File::new(
                Checksum::BLAKE3(chunk1_sha.into()),
                3129,
                file_chunks,
            )))
        });

        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3(chunk1_sha.into()),
            "keyboard cat",
        );
        let result = usecase.call(params);

        // assert
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        let expected_chunk = Checksum::BLAKE3(chunk1_sha.into());
        let expected_old_pack = Checksum::SHA1("d535524bd023d0d22a3912a472c5b0f2db111690".into());
        let expected_new_pack = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".into());
        assert_eq!(results[0].chunk_digest, expected_chunk);
        assert_eq!(results[0].old_pack_digest, expected_old_pack);
        assert_eq!(results[0].new_pack_digest, expected_new_pack);

        Ok(())
    }

    #[test]
    fn test_scan_packs_multiple_chunks() -> Result<(), Error> {
        // build pack file containing a multi-chunk file
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let file_digest = "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f";
        let chunks = helpers::find_file_chunks(&infile, 32768)?;
        let mut builder = pack::PackBuilder::new(1048576).password("keyboard cat");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi.pack");
        builder.initialize(&packfile)?;
        for chunk in chunks.iter() {
            if builder.add_chunk(chunk)? {
                break;
            }
        }
        let _result = builder.finalize()?;

        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/scan_packs"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let packfile_path = packfile.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::rename(packfile_path.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_all_packs().returning(move || {
            // this pack digest will be captured as the correct ("new") value
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(vec![Pack::new(pack_sum, locations)])
        });
        mock.expect_get_file().returning(move |_| {
            let mut file_chunks: Vec<(u64, Checksum)> = Vec::new();
            let mut chunk_offset: u64 = 0;
            for chunk in chunks.iter() {
                file_chunks.push((chunk_offset, chunk.digest.clone()));
                chunk_offset += chunk.length as u64;
            }
            Ok(Some(File::new(
                Checksum::BLAKE3(file_digest.into()),
                109466,
                file_chunks,
            )))
        });
        let chunk1_sha = "c451d8d136529890c3ecc169177c036029d2b684f796f254bf795c96783fc483";
        let chunk2_sha = "b4da74176d97674c78baa2765c77f0ccf4a9602f229f6d2b565cf94447ac7af0";
        mock.expect_get_chunk()
            .withf(move |c| {
                let expected = Checksum::BLAKE3(chunk1_sha.into());
                c == &expected
            })
            .returning(|_| {
                // intentionally create a chunk record with the wrong pack digest
                let digest = Checksum::BLAKE3(chunk1_sha.into());
                let mut chunk = Chunk::new(digest, 0, 66549);
                chunk.packfile = Some(Checksum::SHA1(
                    "d535524bd023d0d22a3912a472c5b0f2db111690".to_owned(),
                ));
                Ok(Some(chunk))
            });
        mock.expect_get_chunk()
            .withf(move |c| {
                let expected = Checksum::BLAKE3(chunk2_sha.into());
                c == &expected
            })
            .returning(|_| {
                // intentionally create a chunk record with the wrong pack digest
                let digest = Checksum::BLAKE3(chunk2_sha.into());
                let mut chunk = Chunk::new(digest, 66549, 42917);
                chunk.packfile = Some(Checksum::SHA1(
                    "d535524bd023d0d22a3912a472c5b0f2db111690".to_owned(),
                ));
                Ok(Some(chunk))
            });

        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3(file_digest.into()),
            "keyboard cat",
        );
        let result = usecase.call(params);

        // assert
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 2);
        let expected_chunk = Checksum::BLAKE3(chunk1_sha.into());
        let expected_old_pack = Checksum::SHA1("d535524bd023d0d22a3912a472c5b0f2db111690".into());
        let expected_new_pack = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".into());
        assert_eq!(results[0].chunk_digest, expected_chunk);
        assert_eq!(results[0].old_pack_digest, expected_old_pack);
        assert_eq!(results[0].new_pack_digest, expected_new_pack);
        let expected_chunk = Checksum::BLAKE3(chunk2_sha.into());
        assert_eq!(results[1].chunk_digest, expected_chunk);
        assert_eq!(results[1].old_pack_digest, expected_old_pack);
        assert_eq!(results[1].new_pack_digest, expected_new_pack);

        Ok(())
    }
}
