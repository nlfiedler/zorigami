//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{Checksum, File};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Context, Error};
use log::info;
use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::fs;
use std::str::FromStr;

///
/// Insert a new file record with the given checksum and pack digest.
///
/// This usecase assumes that the missing file contained a single chunk and as
/// such will verify that no file record exists for that digest.
///
/// This usecase will fetch the pack to verify that the entry exists, get the
/// size from that entry, and then create a file record accordingly.
///
pub struct InsertFile {
    repo: Box<dyn RecordRepository>,
}

impl InsertFile {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl<'a> super::UseCase<bool, Params<'a>> for InsertFile {
    fn call(&self, params: Params) -> Result<bool, Error> {
        // ensure that the file record does not already exist
        if self.repo.get_file(&params.chunk_digest)?.is_some() {
            return Err(anyhow!(format!(
                "file record already exists: {}",
                &params.chunk_digest
            )));
        }
        // get the dataset and its associated pack stores
        let dataset = self
            .repo
            .get_dataset(&params.dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset_id)))?;
        let stores = self.repo.load_dataset_stores(&dataset)?;
        fs::create_dir_all(&dataset.workspace).context("creating workspace")?;
        info!("InsertFile: retrieving pack {}", &params.pack_digest);
        let pack = self
            .repo
            .get_pack(&params.pack_digest)?
            .ok_or_else(|| anyhow!(format!("missing pack: {:?}", params.pack_digest)))?;
        // retrieve and decrypt the pack file
        let archive = tempfile::Builder::new()
            .suffix(".pack")
            .tempfile_in(&dataset.workspace)?;
        stores.retrieve_pack(&pack.locations, archive.path())?;
        // scan the contents of the tar file to verify chunk exists
        let mut reader = exaf_rs::reader::Entries::new(&archive)?;
        reader.enable_encryption(&params.passphrase)?;
        let mut file_size: u64 = 0;
        for maybe_entry in reader {
            let entry = maybe_entry?;
            // we know the names are valid UTF-8, we created them
            let digest = Checksum::from_str(entry.name())?;
            if digest == params.chunk_digest {
                file_size = entry.size().unwrap_or(0);
                break;
            }
        }
        if file_size == 0 {
            Err(anyhow!(format!("pack did not contain chunk")))
        } else {
            // create the file record
            let file_rec = File::new(
                params.chunk_digest.clone(),
                file_size,
                vec![(0, params.pack_digest.clone())],
            );
            self.repo.insert_file(&file_rec).map(|_| true)
        }
    }
}

pub struct Params<'a> {
    /// Identifier of the dataset from which the pack will be retrieved.
    dataset_id: Cow<'a, str>,
    /// Digest of the chunk for which to create a file record.
    chunk_digest: Checksum,
    /// Digest of the pack that is expected to contain the chunk.
    pack_digest: Checksum,
    /// Pass phrase for decrypting the pack.
    passphrase: Cow<'a, str>,
}

impl<'a> Params<'a> {
    pub fn new<T: Into<String>>(
        dataset_id: T,
        chunk_digest: Checksum,
        pack_digest: Checksum,
        passphrase: T,
    ) -> Self {
        Self {
            dataset_id: Cow::from(dataset_id.into()),
            chunk_digest,
            pack_digest,
            passphrase: Cow::from(passphrase.into()),
        }
    }
}

impl<'a> fmt::Display for Params<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Params({}, {} in {})",
            self.dataset_id, self.chunk_digest, self.pack_digest
        )
    }
}

impl<'a> cmp::PartialEq for Params<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.dataset_id == other.dataset_id
            && self.chunk_digest == other.chunk_digest
            && self.pack_digest == other.pack_digest
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
    fn test_insert_file_existing_file() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_file().returning(move |_| {
            Ok(Some(File::new(
                Checksum::BLAKE3("deadbeef".into()),
                0,
                vec![],
            )))
        });
        // act
        let usecase = InsertFile::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            Checksum::BLAKE3("cafebabe".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("file record already exists"));
    }

    #[test]
    fn test_insert_file_missing_dataset() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_file().returning(move |_| Ok(None));
        mock.expect_get_dataset().returning(move |_| Ok(None));
        // act
        let usecase = InsertFile::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            Checksum::BLAKE3("cafebabe".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing dataset"));
    }

    #[test]
    fn test_insert_file_missing_pack_record() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_file().returning(move |_| Ok(None));
        let dataset = Dataset::new(Path::new("tmp/test/insert_file"));
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_load_dataset_stores().returning(move |_| {
            let mock_store = MockPackRepository::new();
            Ok(Box::new(mock_store))
        });
        mock.expect_get_pack().returning(move |_| Ok(None));
        // act
        let usecase = InsertFile::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3("deadbeef".into()),
            Checksum::BLAKE3("cafebabe".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("missing pack"));
    }

    #[test]
    fn test_insert_file_pack_missing_chunk() -> Result<(), Error> {
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
        let mut mock = MockRecordRepository::new();
        mock.expect_get_file().returning(move |_| Ok(None));
        let dataset = Dataset::new(Path::new("tmp/test/insert_file"));
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
        mock.expect_get_pack().returning(move |digest| {
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(Some(Pack::new(digest.to_owned(), locations)))
        });

        // act
        let usecase = InsertFile::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            // intentionally giving the wrong chunk digest
            Checksum::BLAKE3("deadbeef".into()),
            Checksum::BLAKE3("cafebabe".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);

        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("pack did not contain chunk"));

        Ok(())
    }

    #[test]
    fn test_insert_file_success() -> Result<(), Error> {
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
        let mut mock = MockRecordRepository::new();
        mock.expect_get_file().returning(move |_| Ok(None));
        let dataset = Dataset::new(Path::new("tmp/test/insert_file"));
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
        mock.expect_get_pack().returning(move |digest| {
            let locations = vec![PackLocation::new("storeid", "bucketid", "objectid")];
            Ok(Some(Pack::new(digest.to_owned(), locations)))
        });
        mock.expect_insert_file()
            .times(1)
            .withf(|file| file.digest == Checksum::BLAKE3(chunk1_sha.into()))
            .returning(|_| Ok(()));

        // act
        let usecase = InsertFile::new(Box::new(mock));
        let params = Params::new(
            "ignored",
            Checksum::BLAKE3(chunk1_sha.into()),
            Checksum::BLAKE3("deadbeef".into()),
            "keyboard cat",
        );
        let result = usecase.call(params);
        println!("result: {:?}", result);

        // assert
        assert!(result.is_ok());
        assert!(result.unwrap());

        Ok(())
    }
}
