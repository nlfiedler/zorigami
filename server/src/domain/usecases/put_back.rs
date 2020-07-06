//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::Checksum;
use crate::domain::managers::restore;
use crate::domain::repositories::RecordRepository;
use failure::{err_msg, Error};
use std::cmp;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

pub struct PutBack {
    repo: Arc<dyn RecordRepository>,
}

impl PutBack {
    pub fn new(repo: Arc<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<PathBuf, Params> for PutBack {
    fn call(&self, params: Params) -> Result<PathBuf, Error> {
        // rehydrate the dataset from the identifier
        let dataset = self
            .repo
            .get_dataset(&params.dataset)?
            .ok_or_else(|| err_msg(format!("missing dataset: {:?}", params.dataset)))?;
        // assemble the full path of the file to restore
        let mut outfile: PathBuf = dataset.basepath.clone();
        outfile.push(params.filepath);
        let passphrase = crate::domain::managers::get_passphrase();
        // Restore the file back to its original location; technically it is the
        // path determined by the caller, which is easier than walking the
        // snapshot to find the original file path.
        restore::restore_file(
            &self.repo,
            &dataset,
            &passphrase,
            params.digest.clone(),
            &outfile,
        )?;
        Ok(outfile)
    }
}

pub struct Params {
    /// Hash digest of the file to restore.
    digest: Checksum,
    /// Relative path of file to be restored.
    filepath: PathBuf,
    /// Identifier of the dataset containing the snapshot.
    dataset: String,
}

impl Params {
    pub fn new(digest: Checksum, filepath: PathBuf, dataset: String) -> Self {
        Self {
            digest,
            filepath,
            dataset,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.digest)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Chunk, Dataset, File, Pack, PackLocation};
    use crate::domain::managers;
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use tempfile::tempdir;

    #[test]
    fn test_put_back_ok() {
        // arrange
        let relative_path = PathBuf::from("../test/fixtures/lorem-ipsum.txt");
        let file_digest = Checksum::sha256_from_file(&relative_path).unwrap();

        // get dataset
        let tmpdir = tempdir().unwrap();
        let basepath = tmpdir.path();
        let dataset = Dataset::new(basepath);
        let dataset_id = dataset.id.clone();
        let dataset_id_clone = dataset_id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .withf(move |id| id == &dataset_id)
            .returning(move |_| Ok(Some(dataset.clone())));

        // get dataset stores (and create pack file)
        let mut chunk = Chunk::new(file_digest.clone(), 0, 3129);
        chunk = chunk.filepath(&relative_path);
        let chunks = vec![chunk.clone()];
        let mut pack_file = tmpdir.path().to_path_buf();
        pack_file.push("packfile.tar");
        let pack_digest = managers::pack_chunks(&chunks, &pack_file).unwrap();
        chunk = chunk.packfile(pack_digest.clone());
        let mut zipped = pack_file.clone();
        zipped.set_extension("gz");
        managers::compress_file(&pack_file, &zipped).unwrap();
        std::fs::remove_file(&pack_file).unwrap();
        let mut encrypted = pack_file.clone();
        encrypted.set_extension("pack");
        let passphrase = managers::get_passphrase();
        let salt = managers::encrypt_file(&passphrase, &zipped, &encrypted).unwrap();
        std::fs::remove_file(zipped).unwrap();
        let pack_file_path = encrypted.to_string_lossy().into_owned();
        mock.expect_load_dataset_stores().returning(move |_| {
            let pack_file_path_clone = pack_file_path.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::rename(pack_file_path_clone.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });

        // get file
        let file = File::new(file_digest.clone(), 3129, vec![(0, file_digest.clone())]);
        mock.expect_get_file()
            .returning(move |_| Ok(Some(file.clone())));

        // get chunk
        mock.expect_get_chunk()
            .returning(move |_| Ok(Some(chunk.clone())));

        // get pack
        let location = PackLocation::new("store1", "bucket1", "object1");
        let mut pack = Pack::new(pack_digest, vec![location]);
        pack.crypto_salt = Some(salt);
        mock.expect_get_pack()
            .returning(move |_| Ok(Some(pack.clone())));

        // act
        let usecase = PutBack::new(Arc::new(mock));
        let filepath = PathBuf::from("restored.txt");
        let params = Params::new(file_digest.clone(), filepath.clone(), dataset_id_clone);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        let mut expected = basepath.to_path_buf();
        expected.push(filepath);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_put_back_err() {
        // arrange
        let file_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let dataset_id = "coolmura";
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .withf(move |id| id == dataset_id)
            .returning(|_| Err(err_msg("oh no")));
        // act
        let usecase = PutBack::new(Arc::new(mock));
        let filepath = PathBuf::new();
        let params = Params::new(file_sha, filepath, dataset_id.to_owned());
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
