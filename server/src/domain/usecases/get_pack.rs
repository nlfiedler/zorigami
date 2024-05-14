//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{Checksum, PackEntry, PackFile};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Context, Error};
use log::debug;
use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::fs;

pub struct GetPack {
    repo: Box<dyn RecordRepository>,
}

impl GetPack {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl<'a> super::UseCase<PackFile, Params<'a>> for GetPack {
    fn call(&self, params: Params) -> Result<PackFile, Error> {
        let pack_digest = &params.digest;
        let dataset = self
            .repo
            .get_dataset(&params.dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset_id)))?;
        let stores = self.repo.load_dataset_stores(&dataset)?;
        fs::create_dir_all(&dataset.workspace).context("creating workspace")?;
        let archive = tempfile::Builder::new()
            .suffix(".pack")
            .tempfile_in(&dataset.workspace)?;
        let pack_record = self
            .repo
            .get_pack(pack_digest)?
            .ok_or_else(|| anyhow!(format!("missing pack record: {:?}", pack_digest)))?;
        // retrieve the pack file
        debug!("get-pack: fetching pack {}", pack_digest);
        stores.retrieve_pack(&pack_record.locations, archive.path())?;
        // read the archive file entries
        let mut entries: Vec<PackEntry> = Vec::new();
        let attr = fs::metadata(&archive)?;
        let file_size = attr.len();
        let mut reader = exaf_rs::reader::Entries::new(&archive)?;
        reader.enable_encryption(&params.passphrase)?;
        for maybe_entry in reader {
            let entry = maybe_entry?;
            let path = entry.name().to_string();
            entries.push(PackEntry::new(path, entry.size().unwrap_or(0)));
        }
        Ok(PackFile::new(file_size, entries))
    }
}

pub struct Params<'a> {
    /// Unique identifier of the dataset.
    dataset_id: Cow<'a, str>,
    /// Hash digest of the pack to retrieve.
    digest: Checksum,
    /// Pass phrase for decrypting the pack.
    passphrase: Cow<'a, str>,
}

impl<'a> Params<'a> {
    pub fn new<T: Into<String>>(dataset_id: T, digest: Checksum, passphrase: T) -> Self {
        Self {
            dataset_id: Cow::from(dataset_id.into()),
            digest,
            passphrase: Cow::from(passphrase.into()),
        }
    }
}

impl<'a> fmt::Display for Params<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.digest)
    }
}

impl<'a> cmp::PartialEq for Params<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl<'a> cmp::Eq for Params<'a> {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Dataset, Pack, PackLocation};
    use crate::domain::helpers::{self, pack};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_get_pack_missing_dataset() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset().returning(move |_| Ok(None));
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing dataset"));
    }

    #[test]
    fn test_get_pack_none() {
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let mock_store = MockPackRepository::new();
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| Ok(None));
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing pack record"));
    }

    #[test]
    fn test_get_pack_zero_entries() -> Result<(), Error> {
        // build empty pack file
        let mut builder = pack::PackBuilder::new(65536).password("keyboard cat");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("zero.pack");
        builder.initialize(&packfile)?;
        let _result = builder.finalize()?;
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
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
        mock.expect_get_pack().returning(move |_| {
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(Some(Pack::new(pack_sum, locations)))
        });
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let packfile = result.unwrap();
        assert_eq!(packfile.entries.len(), 0);
        assert_eq!(packfile.length, 38);
        assert_eq!(packfile.content_length, 0);
        assert_eq!(packfile.smallest, 0);
        assert_eq!(packfile.largest, 0);
        assert_eq!(packfile.average, 0);
        Ok(())
    }

    #[test]
    fn test_get_pack_multiple_entries() -> Result<(), Error> {
        // build average pack file
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let chunks = helpers::find_file_chunks(&infile, 32768)?;
        assert_eq!(chunks.len(), 2);
        let mut builder = pack::PackBuilder::new(1048576).password("keyboard cat");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi.pack");
        builder.initialize(&packfile)?;
        for chunk in chunks.iter() {
            builder.add_chunk(chunk)?;
        }
        let _result = builder.finalize()?;
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let packfile_path = packfile.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::copy(packfile_path.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| {
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(Some(Pack::new(pack_sum, locations)))
        });
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let packfile = result.unwrap();
        assert_eq!(packfile.length, 96997);
        assert_eq!(packfile.content_length, 109466);
        assert_eq!(packfile.smallest, 42917);
        assert_eq!(packfile.largest, 66549);
        assert_eq!(packfile.average, 54733);
        assert_eq!(packfile.entries.len(), 2);
        assert_eq!(
            packfile.entries[0].name,
            "blake3-c3a9c101999bcd14212cbac34a78a5018c6d1548a32c084f43499c254adf07ef"
        );
        assert_eq!(packfile.entries[0].size, 66549);
        assert_eq!(
            packfile.entries[1].name,
            "blake3-4b5f350ca573fc4f44b0da18d6aef9cdb2bcb7eeab1ad371af82557d0f353454"
        );
        assert_eq!(packfile.entries[1].size, 42917);
        Ok(())
    }
}
