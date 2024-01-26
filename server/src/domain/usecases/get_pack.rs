//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{Checksum, PackEntry, PackFile};
use crate::domain::helpers::crypto;
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
        let encrypted = tempfile::Builder::new()
            .suffix(".pack")
            .tempfile_in(&dataset.workspace)?;
        let pack_record = self
            .repo
            .get_pack(pack_digest)?
            .ok_or_else(|| anyhow!(format!("missing pack record: {:?}", pack_digest)))?;
        // check the salt before downloading the pack, otherwise we waste
        // time fetching it when we would not be able to decrypt it
        let salt = pack_record
            .crypto_salt
            .ok_or_else(|| anyhow!(format!("missing pack salt: {:?}", pack_digest)))?;
        // retrieve the pack file
        debug!("get-pack: fetching pack {}", pack_digest);
        stores.retrieve_pack(&pack_record.locations, encrypted.path())?;
        // decrypt
        let archive = tempfile::Builder::new()
            .suffix(".tar")
            .tempfile_in(&dataset.workspace)?;
        crypto::decrypt_file(&params.passphrase, &salt, encrypted.path(), archive.path())?;
        // read the archive file entries
        let mut entries: Vec<PackEntry> = Vec::new();
        let attr = fs::metadata(&archive)?;
        let file_size = attr.len();
        let file = fs::File::open(&archive)?;
        let mut ar = tar::Archive::new(file);
        for maybe_entry in ar.entries()? {
            let entry = maybe_entry?;
            // we know the names are valid UTF-8, we created them
            let path = String::from(entry.path()?.to_str().unwrap());
            entries.push(PackEntry::new(path, entry.size()));
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
    fn test_get_pack_missing_salt() {
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let mock_store = MockPackRepository::new();
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| {
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            Ok(Some(Pack::new(pack_sum, vec![])))
        });
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing pack salt"));
    }

    #[test]
    fn test_get_pack_zero_entries() -> Result<(), Error> {
        // build empty pack file
        let mut builder = pack::PackBuilder::new(65536);
        let outdir = tempdir()?;
        let packfile = outdir.path().join("zero.tar");
        builder.initialize(&packfile)?;
        let _result = builder.finalize()?;
        let passphrase = "keyboard cat";
        let encrypted = outdir.path().join("zero.pack");
        let salt = crypto::encrypt_file(passphrase, &packfile, &encrypted)?;
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let encrypted_path = encrypted.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    // rename on Windows fails with permission denied, so do
                    // what the local pack store would do and just copy
                    std::fs::copy(encrypted_path.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| {
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            let mut pack = Pack::new(pack_sum, locations);
            pack.crypto_salt = Some(salt.clone());
            Ok(Some(pack))
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
        assert_eq!(packfile.length, 1024);
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
        let mut builder = pack::PackBuilder::new(1048576);
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi.tar");
        builder.initialize(&packfile)?;
        for chunk in chunks.iter() {
            if builder.add_chunk(chunk)? {
                break;
            }
        }
        let _result = builder.finalize()?;
        let passphrase = "keyboard cat";
        let encrypted = outdir.path().join("multi.pack");
        let salt = crypto::encrypt_file(passphrase, &packfile, &encrypted)?;
        // arrange
        let dataset = Dataset::new(Path::new("tmp/test/get_pack"));
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let encrypted_path = encrypted.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::copy(encrypted_path.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| {
            let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            let mut pack = Pack::new(pack_sum, locations);
            pack.crypto_salt = Some(salt.clone());
            Ok(Some(pack))
        });
        // act
        let usecase = GetPack::new(Box::new(mock));
        let pack_sum = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let params = Params::new("ignored", pack_sum, "keyboard cat");
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let packfile = result.unwrap();
        assert_eq!(packfile.length, 99328);
        assert_eq!(packfile.content_length, 96748);
        assert_eq!(packfile.smallest, 41825);
        assert_eq!(packfile.largest, 54923);
        assert_eq!(packfile.average, 48374);
        assert_eq!(packfile.entries.len(), 2);
        assert_eq!(
            packfile.entries[0].name,
            "sha256-c451d8d136529890c3ecc169177c036029d2b684f796f254bf795c96783fc483"
        );
        assert_eq!(packfile.entries[0].size, 54923);
        assert_eq!(
            packfile.entries[1].name,
            "sha256-b4da74176d97674c78baa2765c77f0ccf4a9602f229f6d2b565cf94447ac7af0"
        );
        assert_eq!(packfile.entries[1].size, 41825);
        Ok(())
    }
}
