//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{Checksum, TreeReference};
use crate::domain::repositories::{PackRepository, RecordRepository};
use crate::shared::packs;
use anyhow::{Error, anyhow};
use chrono::prelude::*;
use log::{debug, error, info, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::cmp;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Status of a request.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Request is waiting to be processed.
    PENDING,
    /// Request was cancelled before processing began.
    CANCELLED,
    /// Request is being processed.
    RUNNING,
    /// Request processing has completed (successfully or otherwise).
    COMPLETED,
}

/// Request to restore a single file or a tree of files.
#[derive(Clone, Debug)]
pub struct Request {
    /// Unique identifier for this request.
    pub id: String,
    /// Status of this request.
    pub status: Status,
    /// Digest of the tree containing the entry to restore.
    pub tree: Checksum,
    /// Name of the entry within the tree to be restored.
    pub entry: String,
    /// Relative path where file/tree will be restored.
    pub filepath: PathBuf,
    /// Identifier of the dataset containing the data.
    pub dataset: String,
    /// Password text for decrypting the pack files.
    pub passphrase: String,
    /// The date-time when the request processing started.
    pub started: Option<DateTime<Utc>>,
    /// The date-time when the request was completed.
    pub finished: Option<DateTime<Utc>>,
    /// Number of files restored so far during the restoration.
    pub files_restored: u64,
    /// Error messages if anything went wrong during processing.
    pub errors: Vec<String>,
}

impl Request {
    pub fn new(
        tree: Checksum,
        entry: String,
        filepath: PathBuf,
        dataset: String,
        passphrase: String,
    ) -> Self {
        let id = xid::new().to_string();
        Self {
            id,
            status: Status::PENDING,
            tree,
            entry,
            filepath,
            dataset,
            passphrase,
            started: None,
            finished: None,
            files_restored: 0,
            errors: vec![],
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Restore]Request({})", self.id)
    }
}

impl cmp::PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl cmp::Eq for Request {}

///
/// A `Subscriber` receives updates to the progress of a restore operation.
///
#[cfg_attr(test, automock)]
pub trait Subscriber: Send + Sync {
    /// Restore operation has begun to be processed.
    ///
    /// Returns a value for mockall tests.
    fn started(&self, request_id: &str) -> bool;

    /// One or more files have been restored.
    ///
    /// Returns a value for mockall tests.
    fn restored(&self, request_id: &str, addend: u64) -> bool;

    /// An error has occurred while restoring files, directories, or links.
    ///
    /// Returns a value for mockall tests.
    fn error(&self, request_id: &str, error: String) -> bool;

    /// Restore request has been completed.
    ///
    /// Returns a value for mockall tests.
    fn finished(&self, request_id: &str) -> bool;
}

///
/// `Restorer` restores individual files or entires directory trees. Can also
/// restore the database from a recent snapshot.
///
#[cfg_attr(test, automock)]
pub trait Restorer: Send + Sync {
    /// Process a restore request for a file or directory. If the restorer
    /// implementation supports subscribers, they will be notified of progress
    /// during processing.
    fn restore_files(&self, request: Request) -> Result<(), Error>;

    /// Restore the most recent database snapshot from the given pack store.
    fn restore_database(&self, store_id: &str, passphrase: &str) -> Result<(), Error>;
}

///
/// Factory method for constructing a FileRestorer.
///
type FileRestorerFactory = fn(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer>;

///
/// Construct the default file fetcher.
///
fn default_file_fetcher(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    Box::new(FileRestorerImpl::new(dbase))
}

///
/// Basic implementation of `Restorer`.
///
pub struct RestorerImpl {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Factory method for the FileRestorer implementation.
    fetch_factory: FileRestorerFactory,
    // Events related to the restore are sent to the subscriber.
    subscriber: Arc<dyn Subscriber>,
    // If the value is true, the restore process should stop.
    #[allow(dead_code)]
    stop_requested: Arc<RwLock<bool>>,
}

impl RestorerImpl {
    /// Construct a new instance of RestorerImpl.
    pub fn new(
        repo: Arc<dyn RecordRepository>,
        subscriber: Arc<dyn Subscriber>,
        stop_requested: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            dbase: repo,
            fetch_factory: default_file_fetcher,
            subscriber,
            stop_requested,
        }
    }

    /// Construct a RestorerImpl with the given file fetcher factory.
    pub fn with_factory(
        repo: Arc<dyn RecordRepository>,
        fetcher: FileRestorerFactory,
        subscriber: Arc<dyn Subscriber>,

        stop_requested: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            dbase: repo,
            fetch_factory: fetcher,
            subscriber,
            stop_requested,
        }
    }

    fn process_entry(
        &self,
        request: &Request,
        fetcher: &mut Box<dyn FileRestorer>,
    ) -> Result<(), Error> {
        let tree = self
            .dbase
            .get_tree(&request.tree)?
            .ok_or_else(|| anyhow!(format!("missing tree: {:?}", request.tree)))?;
        for entry in tree.entries.iter() {
            if entry.name == request.entry {
                let filepath = request.filepath.clone();
                match &entry.reference {
                    TreeReference::LINK(contents) => {
                        fetcher.restore_link(contents, &filepath)?;
                    }
                    TreeReference::TREE(digest) => {
                        self.process_tree(request, digest.to_owned(), &filepath, fetcher)?;
                    }
                    TreeReference::FILE(digest) => {
                        self.process_file(request, digest.to_owned(), &filepath, fetcher)?;
                    }
                    TreeReference::SMALL(contents) => {
                        fetcher.restore_small(contents, &filepath)?;
                    }
                }
                break;
            }
        }
        Ok(())
    }

    fn process_file(
        &self,
        request: &Request,
        digest: Checksum,
        filepath: &Path,
        fetcher: &mut Box<dyn FileRestorer>,
    ) -> Result<(), Error> {
        // fetch the packs for the file and assemble the chunks
        fetcher.fetch_file(&digest, filepath, &request.passphrase)?;
        // update the count of files restored so far
        self.subscriber.restored(&request.id, 1);
        Ok(())
    }

    fn process_tree(
        &self,
        request: &Request,
        digest: Checksum,
        path: &Path,
        fetcher: &mut Box<dyn FileRestorer>,
    ) -> Result<(), Error> {
        let tree = self
            .dbase
            .get_tree(&digest)?
            .ok_or_else(|| anyhow!(format!("missing tree: {:?}", digest)))?;
        // Errors that occur within this loop will _not_ be passed up the stack
        // but instead simply be logged and collected in the request; the hope
        // is that while some entries may have errors, others will succeed.
        for entry in tree.entries.iter() {
            let mut filepath = path.to_path_buf();
            filepath.push(&entry.name);
            match &entry.reference {
                TreeReference::LINK(contents) => {
                    if let Err(error) = fetcher.restore_link(contents, &filepath) {
                        error!(
                            "process_tree: error restoring link {}: {}",
                            filepath.display(),
                            error
                        );
                        self.subscriber.error(&request.id, error.to_string());
                    }
                }
                TreeReference::TREE(digest) => {
                    if let Err(error) =
                        self.process_tree(request, digest.to_owned(), &filepath, fetcher)
                    {
                        error!(
                            "process_tree: error processing tree {}: {}",
                            filepath.display(),
                            error
                        );
                        self.subscriber.error(&request.id, error.to_string());
                    }
                }
                TreeReference::FILE(digest) => {
                    if let Err(error) =
                        self.process_file(request, digest.to_owned(), &filepath, fetcher)
                    {
                        error!(
                            "process_tree: error processing file {}: {}",
                            filepath.display(),
                            error
                        );
                        self.subscriber.error(&request.id, error.to_string());
                    }
                }
                TreeReference::SMALL(contents) => {
                    if let Err(error) = fetcher.restore_small(contents, &filepath) {
                        error!(
                            "process_tree: error restoring small {}: {}",
                            filepath.display(),
                            error
                        );
                        self.subscriber.error(&request.id, error.to_string());
                    }
                }
            }
        }
        Ok(())
    }
}

impl Restorer for RestorerImpl {
    fn restore_files(&self, request: Request) -> Result<(), Error> {
        // Construct the pack fetcher that will keep track of which pack files
        // have been downloaded to avoid fetching the same one twice.
        let mut fetcher = (self.fetch_factory)(self.dbase.clone());
        info!("processing request {}/{}", request.tree, request.entry);
        self.subscriber.started(&request.id);
        if std::env::var("RESTORE_ALWAYS_PROCESSING").is_ok() {
            // if in test mode, do not really process the request
            self.subscriber.restored(&request.id, 42);
            self.subscriber
                .error(&request.id, "oh no, something went wrong!".into());
            self.subscriber
                .error(&request.id, "something else went wrong!".into());
            self.subscriber
                .error(&request.id, "abandon ship, abandon ship!".into());
        } else {
            match fetcher.load_dataset(&request.dataset) {
                Err(error) => {
                    error!("process_queue: error loading dataset: {}", error);
                    self.subscriber.error(&request.id, error.to_string());
                }
                _ => {
                    if let Err(error) = self.process_entry(&request, &mut fetcher) {
                        error!("process_queue: error processing entry: {}", error);
                        self.subscriber.error(&request.id, error.to_string());
                    }
                }
            }
            self.subscriber.finished(&request.id);
            info!("completed request {}/{}", request.tree, request.entry);
        }
        Ok(())
    }

    fn restore_database(&self, store_id: &str, passphrase: &str) -> Result<(), Error> {
        let result = if let Some(store) = self.dbase.get_store(store_id)? {
            let pack_repo = self.dbase.build_pack_repo(&store)?;
            let config = self.dbase.get_configuration()?;
            let archive_file = tempfile::NamedTempFile::new()?;
            let archive_path = archive_file.into_temp_path();
            info!("retrieving database snapshot from store {}", store.id);
            pack_repo.retrieve_latest_database(&config.computer_id, &archive_path)?;
            info!("restoring database from backup");
            self.dbase.restore_from_backup(&archive_path, passphrase)
        } else {
            Err(anyhow!("pack store not found: {}", store_id))
        };
        if let Err(err) = result {
            error!("database restore failed: {}", err);
            Err(err)
        } else {
            info!("database restore complete");
            Ok(())
        }
    }
}

///
/// Restores individual files and symbolic links. Maintains a list of the pack
/// files that have been downloaded so far and retains chunks to avoid fetching
/// the same pack file multiple times.
///
#[cfg_attr(test, automock)]
pub trait FileRestorer: Send + Sync {
    /// Prepare for restoring files by loading the given dataset.
    fn load_dataset(&mut self, dataset_id: &str) -> Result<(), Error>;

    /// Fetch the necessary packs and restore the given file.
    fn fetch_file(
        &mut self,
        checksum: &Checksum,
        filepath: &Path,
        passphrase: &str,
    ) -> Result<(), Error>;

    /// Restore the named symbolic link given its contents.
    fn restore_link(&self, contents: &[u8], filepath: &Path) -> Result<(), Error>;

    /// Restore the named small file given its contents.
    fn restore_small(&self, contents: &[u8], filepath: &Path) -> Result<(), Error>;
}

pub struct FileRestorerImpl {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Identifier of the loaded data set, if any.
    dataset: Option<String>,
    // Pack repository for retrieving pack files.
    stores: Option<Arc<dyn PackRepository>>,
    // Base path to which files will be restored.
    basepath: Option<PathBuf>,
    // Temporary location where packs and chunks are downloaded.
    packpath: Option<tempfile::TempDir>,
    // Those pack files that have already been fetched.
    downloaded: HashSet<Checksum>,
}

impl FileRestorerImpl {
    /// Construct an instance of FileRestorerImpl.
    pub fn new(dbase: Arc<dyn RecordRepository>) -> Self {
        Self {
            dbase,
            dataset: None,
            stores: None,
            basepath: None,
            packpath: None,
            downloaded: HashSet::new(),
        }
    }

    // Fetch a pack file.
    fn fetch_pack(
        &mut self,
        pack_digest: &Checksum,
        workspace: &Path,
        passphrase: &str,
    ) -> Result<(), Error> {
        if !self.downloaded.contains(pack_digest) {
            let stores = self.stores.as_ref().unwrap();
            let saved_pack = self
                .dbase
                .get_pack(pack_digest)?
                .ok_or_else(|| anyhow!(format!("missing pack record: {:?}", pack_digest)))?;
            // retrieve the pack file
            let mut archive = PathBuf::new();
            archive.push(workspace);
            archive.push(pack_digest.to_string());
            debug!("fetching pack {}", pack_digest);
            stores.retrieve_pack(&saved_pack.locations, &archive)?;
            // unpack the contents
            verify_pack_digest(pack_digest, &archive)?;
            packs::extract_pack(&archive, workspace, Some(passphrase))?;
            debug!("pack extracted");
            fs::remove_file(archive)?;
            // remember this pack as being downloaded
            self.downloaded.insert(pack_digest.to_owned());
        }
        Ok(())
    }
}

impl FileRestorer for FileRestorerImpl {
    fn load_dataset(&mut self, dataset_id: &str) -> Result<(), Error> {
        use anyhow::Context;
        if let Some(id) = self.dataset.as_ref()
            && id == dataset_id
        {
            return Ok(());
        }
        let dataset = self
            .dbase
            .get_dataset(dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", dataset_id)))?;
        self.dataset = Some(dataset_id.to_owned());
        self.stores = Some(Arc::from(self.dbase.load_dataset_stores(&dataset)?));
        fs::create_dir_all(&dataset.workspace).with_context(|| {
            format!(
                "load_dataset fs::create_dir_all({})",
                dataset.workspace.display()
            )
        })?;
        self.packpath = Some(tempfile::TempDir::new_in(dataset.workspace)?);
        self.basepath = Some(dataset.basepath);
        Ok(())
    }

    fn fetch_file(
        &mut self,
        checksum: &Checksum,
        filepath: &Path,
        passphrase: &str,
    ) -> Result<(), Error> {
        use anyhow::Context;
        info!("restoring file from {} to {}", checksum, filepath.display());
        let workspace = self.packpath.as_ref().unwrap().path().to_path_buf();
        fs::create_dir_all(&workspace)
            .with_context(|| format!("fetch_file fs::create_dir_all({})", workspace.display()))?;
        // look up the file record to get chunks
        let saved_file = self
            .dbase
            .get_file(checksum)?
            .ok_or_else(|| anyhow!(format!("missing file: {:?}", checksum)))?;
        if saved_file.chunks.len() == 1 {
            // If the file record contains a single chunk entry then its digest
            // is actually that of the pack record rather than a chunk record.
            let pack_digest = &saved_file.chunks[0].1;
            self.fetch_pack(pack_digest, &workspace, passphrase)?;
            let mut cpath = PathBuf::from(&workspace);
            let filename = &saved_file.digest.to_string();
            cpath.push(filename);
            let mut outfile = self.basepath.clone().unwrap();
            outfile.push(filepath);
            let chunk_paths: Vec<&Path> = vec![&cpath];
            debug!(
                "assembling 1-chunk file {} from {:?}",
                outfile.display(),
                &saved_file
            );
            assemble_chunks(&chunk_paths, &outfile)?;
        } else {
            if saved_file.chunks.len() > 120 {
                // For very large files, give some indication that we will be
                // busy for a while downloading all of the pack files.
                warn!(
                    "retrieving packs for large file {} with {} chunks",
                    filepath.display(),
                    saved_file.chunks.len()
                );
            }
            // look up chunk records to get pack record(s)
            for (_offset, chunk) in &saved_file.chunks {
                let chunk_rec = self
                    .dbase
                    .get_chunk(chunk)?
                    .ok_or_else(|| anyhow!(format!("missing chunk: {:?}", chunk)))?;
                let pack_digest = chunk_rec.packfile.as_ref().unwrap();
                self.fetch_pack(pack_digest, &workspace, passphrase)?;
            }
            // sort the chunks by offset to produce the ordered file list
            let mut chunks = saved_file.chunks;
            chunks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let chunk_bufs: Vec<PathBuf> = chunks
                .iter()
                .map(|c| {
                    let mut cpath = PathBuf::from(&workspace);
                    cpath.push(c.1.to_string());
                    cpath
                })
                .collect();
            let chunk_paths: Vec<&Path> = chunk_bufs.iter().map(|b| b.as_path()).collect();
            let mut outfile = self.basepath.clone().unwrap();
            outfile.push(filepath);
            debug!("assembling N-chunk file {}", outfile.display());
            assemble_chunks(&chunk_paths, &outfile)?;
        }
        Ok(())
    }

    fn restore_link(&self, contents: &[u8], filepath: &Path) -> Result<(), Error> {
        use anyhow::Context;
        info!("restoring symbolic link: {}", filepath.display());
        use os_str_bytes::OsStringBytes;
        // this may panic if the bytes are not valid for this platform
        let target = std::ffi::OsString::assert_from_raw_vec(contents.to_owned());
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        if let Some(parent) = outfile.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("restore_link fs::create_dir_all({})", parent.display())
            })?;
            // ignore any errors removing the file that may or moy not be
            // present, but it definitely has to be gone in order for the
            // symlink call to work
            let _ = fs::remove_file(&outfile);
            // cfg! macro will not work in this OS-specific import case
            {
                #[cfg(target_family = "unix")]
                use std::os::unix::fs;
                #[cfg(target_family = "windows")]
                use std::os::windows::fs;
                #[cfg(target_family = "unix")]
                fs::symlink(target, outfile)?;
                #[cfg(target_family = "windows")]
                fs::symlink_file(target, outfile)?;
            }
            return Ok(());
        }
        Err(anyhow!(format!("no parent for: {:?}", outfile)))
    }

    fn restore_small(&self, contents: &[u8], filepath: &Path) -> Result<(), Error> {
        use anyhow::Context;
        info!("restoring small file: {}", filepath.display());
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        if let Some(parent) = outfile.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("restore_small fs::create_dir_all({})", parent.display())
            })?;
            fs::write(&outfile, contents)?;
            return Ok(());
        }
        Err(anyhow!(format!("no parent for: {:?}", outfile)))
    }
}

impl Drop for FileRestorerImpl {
    fn drop(&mut self) {
        // quietly clean up temporary files
        if let Some(workspace) = self.packpath.take() {
            let _ = fs::remove_dir_all(workspace.path());
            self.downloaded.clear();
        }
    }
}

// Verify the retrieved pack file digest matches the database record.
fn verify_pack_digest(digest: &Checksum, path: &Path) -> Result<(), Error> {
    let actual = Checksum::blake3_from_file(path)?;
    if &actual != digest {
        Err(anyhow!(format!(
            "pack digest does not match: {} != {}",
            &actual, digest
        )))
    } else {
        Ok(())
    }
}

// Copy the chunk files to the given output location. The chunk files are left
// in place and must be removed by the caller.
fn assemble_chunks(chunks: &[&Path], outfile: &Path) -> Result<(), Error> {
    use anyhow::Context;
    if let Some(parent) = outfile.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("assemble_chunks fs::create_dir_all({})", parent.display()))?;
        let mut file = fs::File::create(outfile)
            .with_context(|| format!("assemble_chunks File::create({})", outfile.display()))?;
        for infile in chunks {
            let mut cfile = fs::File::open(infile)
                .with_context(|| format!("assemble_chunks File::open({})", infile.display()))?;
            std::io::copy(&mut cfile, &mut file).context("assemble_chunks io::copy")?;
        }
        return Ok(());
    }
    Err(anyhow!(format!("no parent for: {:?}", outfile)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{
        Configuration, Dataset, PackRetention, Store, StoreType, Tree, TreeEntry,
    };
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use crate::shared::packs;
    use std::collections::HashMap;
    use std::io;
    use std::str::FromStr;

    #[test]
    fn test_assemble_chunks() -> Result<(), Error> {
        let tmpdir = tempfile::tempdir()?;
        let mut outfile = PathBuf::from(tmpdir.path());
        outfile.push("foo");
        outfile.push("bar");
        outfile.push("file.txt");
        assert!(!outfile.exists());
        let chunk = Path::new("../test/fixtures/lorem-ipsum.txt");
        assemble_chunks(&[chunk], &outfile)?;
        assert!(outfile.exists());
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_restorer_enqueue_then_fail() -> io::Result<()> {
        // arrange
        let mock = MockRecordRepository::new();
        let repo = Arc::new(mock);
        fn factory(_dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            let mut restorer = MockFileRestorer::new();
            restorer
                .expect_load_dataset()
                .returning(|_| Err(anyhow!("oh no!")));
            Box::new(restorer)
        }
        let mut submock = MockSubscriber::new();
        submock.expect_started().once().returning(|_| false);
        submock
            .expect_error()
            .withf(|_, err| err.contains("oh no"))
            .returning(|_, _| false);
        submock.expect_finished().once().returning(|_| false);
        let stopper = Arc::new(RwLock::new(false));
        let sut = RestorerImpl::with_factory(repo, factory, Arc::new(submock), stopper);
        // act
        let request = super::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        let result = sut.restore_files(request);
        // assert
        assert!(result.is_ok());
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_restorer_fail_then_succeed() -> io::Result<()> {
        // arrange
        let dataset = Dataset::new(Path::new("/home/base"));
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        let tree = Tree::new(
            vec![TreeEntry::new(
                Path::new("../test/fixtures/lorem-ipsum.txt"),
                TreeReference::FILE(Checksum::BLAKE3(String::from(
                    "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128",
                ))),
            )],
            1,
        );
        mock.expect_get_tree()
            .withf(|digest| digest.to_string() == "sha1-cafebabe")
            .returning(move |_| Ok(Some(tree.clone())));

        let passphrase = packs::get_passphrase();

        //
        // Debugging the mocks can be tricky with the restorer running on a
        // different thread and silently failing. Run the tests like so to get
        // the output that would normally go to the log.
        //
        // RUST_LOG=debug cargo test -p server test_restorer_fail_then_succeed -- --nocapture
        //
        fn factory_fail(_dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            let mut restorer = MockFileRestorer::new();
            restorer
                .expect_load_dataset()
                .returning(|_| Err(anyhow!("oh no!")));
            Box::new(restorer)
        }

        // act/assert with failing request
        let repo = Arc::new(mock);
        let mut submock = MockSubscriber::new();
        submock.expect_started().once().returning(|_| false);
        submock
            .expect_error()
            .withf(|_, err| err.contains("oh no"))
            .returning(|_, _| false);
        submock.expect_finished().once().returning(|_| false);
        let stopper = Arc::new(RwLock::new(false));
        let sut =
            RestorerImpl::with_factory(repo.clone(), factory_fail, Arc::new(submock), stopper);
        let request = super::Request::new(
            Checksum::SHA1("deadbeef".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            dataset_id.clone(),
            passphrase.clone(),
        );
        let result = sut.restore_files(request);
        assert!(result.is_ok());

        // act/assert with successful request
        fn factory_pass(_dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            let mut restorer = MockFileRestorer::new();
            restorer.expect_load_dataset().returning(|_| Ok(()));
            restorer.expect_fetch_file().returning(|_, _, _| Ok(()));
            Box::new(restorer)
        }
        let mut submock = MockSubscriber::new();
        submock.expect_started().once().returning(|_| false);
        submock
            .expect_restored()
            .withf(|_, value| *value == 1)
            .returning(|_, _| false);
        submock.expect_finished().once().returning(|_| false);
        let stopper = Arc::new(RwLock::new(false));
        let sut = RestorerImpl::with_factory(repo, factory_pass, Arc::new(submock), stopper);
        let request = super::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            dataset_id.clone(),
            passphrase.clone(),
        );
        let result = sut.restore_files(request);
        assert!(result.is_ok());
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_restorer_restore_tree() -> io::Result<()> {
        // arrange
        let dataset = Dataset::new(Path::new("/home/town"));
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        let subtree = Tree::new(
            vec![
                TreeEntry::new(
                    Path::new("../test/fixtures/lorem-ipsum.txt"),
                    TreeReference::FILE(Checksum::BLAKE3(String::from(
                        "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128",
                    ))),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/washington-journal.txt"),
                    TreeReference::FILE(Checksum::BLAKE3(String::from(
                        "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60",
                    ))),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/SekienAkashita.jpg"),
                    TreeReference::FILE(Checksum::BLAKE3(String::from(
                        "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f",
                    ))),
                ),
            ],
            3,
        );
        let subtree_sha1 = subtree.digest.clone();
        let subtree_str = subtree_sha1.to_string();
        let subtree_str_clone = subtree_str.clone();
        mock.expect_get_tree()
            .withf(move |digest| digest.to_string() == subtree_str_clone)
            .returning(move |_| Ok(Some(subtree.clone())));
        let subtree_digest: Checksum = FromStr::from_str(&subtree_str).unwrap();
        let roottree = Tree::new(
            vec![TreeEntry::new(
                Path::new("../test/fixtures"),
                TreeReference::TREE(subtree_digest),
            )],
            1,
        );
        let roottree_sha1 = roottree.digest.clone();
        let roottree_str = roottree_sha1.to_string();
        mock.expect_get_tree()
            .withf(move |digest| digest.to_string() == roottree_str)
            .returning(move |_| Ok(Some(roottree.clone())));

        //
        // Debugging the mocks can be tricky with the restorer running on a
        // different thread and silently failing. Run the tests like so to get
        // the output that would normally go to the log.
        //
        // RUST_LOG=debug cargo test -p server test_restorer_restore_tree -- --nocapture
        //
        fn factory(_dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            let mut restorer = MockFileRestorer::new();
            restorer.expect_load_dataset().returning(|_| Ok(()));
            restorer.expect_fetch_file().returning(|_, _, _| Ok(()));
            Box::new(restorer)
        }

        // act/assert
        let repo = Arc::new(mock);
        let mut submock = MockSubscriber::new();
        submock.expect_started().once().returning(|_| false);
        submock
            .expect_restored()
            .withf(|_, value| *value == 1)
            .returning(|_, _| false);
        submock.expect_finished().once().returning(|_| false);
        let stopper = Arc::new(RwLock::new(false));
        let sut = RestorerImpl::with_factory(repo, factory, Arc::new(submock), stopper);
        let passphrase = packs::get_passphrase();
        let request = super::Request::new(
            roottree_sha1,
            String::from("fixtures"),
            PathBuf::from("/home/town"),
            dataset_id.clone(),
            passphrase.clone(),
        );
        let result = sut.restore_files(request);
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_restore_database_ok() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_latest_database()
                .returning(move |_, _| Ok(()));
            Ok(Box::new(mock_store))
        });
        let config: Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_restore_from_backup().returning(|_, _| Ok(()));
        // act
        let repo = Arc::new(mock);
        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let sut = RestorerImpl::new(repo, Arc::new(submock), stopper);
        let passphrase = packs::get_passphrase();
        let result = sut.restore_database("cafebabe", &passphrase);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_restore_database_no_database_err() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_latest_database()
                .returning(move |_, _| Ok(()));
            Ok(Box::new(mock_store))
        });
        let config: Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_restore_from_backup()
            .returning(|_, _| Err(anyhow!("no database archives available")));
        // act
        let repo = Arc::new(mock);
        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let sut = RestorerImpl::new(repo, Arc::new(submock), stopper);
        let passphrase = packs::get_passphrase();
        let result = sut.restore_database("cafebabe", &passphrase);
        // assert
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no database archives available"));
    }

    // TODO: try to get this working with whatever solution is devised for stopping/starting the restore process
    // #[actix_rt::test]
    // #[serial_test::serial]
    // async fn test_restorer_start_stop_restart() -> io::Result<()> {
    //     // arrange
    //     fn factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    //         Box::new(FileRestorerImpl::new(dbase))
    //     }
    //     let mock = MockRecordRepository::new();
    //     let repo = Arc::new(mock);
    //     // start
    //     let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    //     let sut = RestorerImpl::new(state.clone(), factory);
    //     let result = sut.start(repo.clone());
    //     state.wait_for_restorer(RestorerAction::Started);
    //     assert!(result.is_ok());
    //     // stop
    //     let result = sut.stop();
    //     assert!(result.is_ok());
    //     state.wait_for_restorer(RestorerAction::Stopped);
    //     // restart
    //     let result = sut.start(repo);
    //     state.wait_for_restorer(RestorerAction::Started);
    //     assert!(result.is_ok());
    //     Ok(())
    // }
}
