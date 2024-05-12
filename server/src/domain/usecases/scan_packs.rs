//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Checksum;
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Context, Error};
use log::{error, info};
use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::fs;
use std::str::FromStr;

///
/// Scan all packs in the system to find the desired chunk.
///
/// The presumption is that the metadata in the database is incorrect about the
/// pack digest for the given chunk. There was a bug in which a file record was
/// not created and yet the single-chunk file was packed safely. The tree would
/// have the reference to the file, but the record was missing.
///
pub struct ScanPacks {
    repo: Box<dyn RecordRepository>,
}

impl ScanPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl<'a> super::UseCase<Option<Checksum>, Params<'a>> for ScanPacks {
    fn call(&self, params: Params) -> Result<Option<Checksum>, Error> {
        // get the dataset and its associated pack stores
        let dataset = self
            .repo
            .get_dataset(&params.dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset_id)))?;
        let stores = self.repo.load_dataset_stores(&dataset)?;
        fs::create_dir_all(&dataset.workspace).context("creating workspace")?;
        // get all packs in the entire system
        let all_packs = self.repo.get_all_packs()?;
        info!("ScanPacks: will scan {} packs", all_packs.len());
        for pack in all_packs.iter() {
            info!("ScanPacks: scanning pack {}", &pack.digest);
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
            // scan the contents of the tar file to find the chunk digest
            let mut reader = exaf_rs::reader::Entries::new(&archive)?;
            reader.enable_encryption(&params.passphrase)?;
            for maybe_entry in reader {
                let entry = maybe_entry?;
                // we know the names are valid UTF-8, we created them
                let digest = Checksum::from_str(entry.name())?;
                if digest == params.chunk_digest {
                    // found the match, return the pack digest
                    return Ok(Some(pack.digest.clone()));
                }
            }
        }
        // indicate that nothing was found
        Ok(None)
    }
}

pub struct Params<'a> {
    /// Identifier of the dataset whose packs will be scanned.
    dataset_id: Cow<'a, str>,
    /// Digest of the chunk that is to be located.
    chunk_digest: Checksum,
    /// Pass phrase for decrypting the pack.
    passphrase: Cow<'a, str>,
}

impl<'a> Params<'a> {
    pub fn new<T: Into<String>>(dataset_id: T, chunk_digest: Checksum, passphrase: T) -> Self {
        Self {
            dataset_id: Cow::from(dataset_id.into()),
            chunk_digest,
            passphrase: Cow::from(passphrase.into()),
        }
    }
}

impl<'a> fmt::Display for Params<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({}, {})", self.dataset_id, self.chunk_digest)
    }
}

impl<'a> cmp::PartialEq for Params<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.dataset_id == other.dataset_id && self.chunk_digest == other.chunk_digest
    }
}

impl<'a> cmp::Eq for Params<'a> {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Checksum, Chunk, Dataset, File, Pack, PackLocation};
    use crate::domain::helpers::pack;
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
            Checksum::SHA256("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing dataset"));
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
                Checksum::SHA256("deadbeef".into()),
                0,
                vec![],
            )))
        });
        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::SHA256("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(results.is_none());
    }

    #[test]
    fn test_scan_packs_single_chunk() -> Result<(), Error> {
        // build pack file containing a file with one chunk
        let infile = Path::new("../test/fixtures/lorem-ipsum.txt");
        let mut builder = pack::PackBuilder::new(1048576).password("keyboard cat");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("single.pack");
        // chunk1 digest is also the file digest
        let chunk1_sha = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        builder.initialize(&packfile)?;
        let mut chunk = Chunk::new(Checksum::SHA256(chunk1_sha.into()), 0, 3129);
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
                    // rename on Windows fails with permission denied, so do
                    // what the local pack store would do and just copy
                    std::fs::copy(packfile_path.clone(), outfile).unwrap();
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

        // act
        let usecase = ScanPacks::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::SHA256(chunk1_sha.into()),
            "keyboard cat",
        );
        let result = usecase.call(params);

        // assert
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(results.is_some());
        let actual_value = results.unwrap();
        let expected_value = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".into());
        assert_eq!(actual_value, expected_value);

        Ok(())
    }
}
