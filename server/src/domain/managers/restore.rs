//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities::{Checksum, TreeReference};
use crate::domain::repositories::{PackRepository, RecordRepository};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::prelude::*;
use log::{debug, error};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::cmp;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

/// Request to restore a single file or a tree of files.
#[derive(Clone, Debug)]
pub struct Request {
    /// Digest of either a file or a tree to restore.
    pub digest: Checksum,
    /// Relative path where file/tree will be restored.
    pub filepath: PathBuf,
    /// Identifier of the dataset containing the data.
    pub dataset: String,
    /// Password text for decrypting the pack files.
    pub passphrase: String,
    /// The datetime when the request was completed.
    pub finished: Option<DateTime<Utc>>,
    /// Number of files restored so far during the restoration.
    pub files_restored: u64,
    /// Error message if request processing failed.
    pub error_msg: Option<String>,
}

impl Request {
    pub fn new(digest: Checksum, filepath: PathBuf, dataset: String, passphrase: String) -> Self {
        Self {
            digest,
            filepath,
            dataset,
            passphrase,
            finished: None,
            files_restored: 0,
            error_msg: None,
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Request({}, {:?})", self.digest, self.filepath)
    }
}

impl cmp::PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest && self.filepath == other.filepath
    }
}

impl cmp::Eq for Request {}

///
/// `Restorer` manages a supervised actor which in turn spawns actors to process
/// file and tree restore requests.
///
#[cfg_attr(test, automock)]
pub trait Restorer: Send + Sync {
    /// Start a supervisor that will run scheduled backups.
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error>;

    /// Add the given request to the queue to be processed.
    fn enqueue(&self, request: Request) -> Result<(), Error>;

    /// Return all pending and recently completed requests.
    fn requests(&self) -> Vec<Request>;

    /// Cancel the pending request.
    ///
    /// Does not cancel the request if it has already begun the restoration
    /// process. Returns true if successfully cancelled.
    fn cancel(&self, request: Request) -> bool;

    /// Signal the supervisor to stop and release the database reference.
    fn stop(&self) -> Result<(), Error>;
}

///
/// Concrete implementation of `Restorer` that uses the actix actor framework to
/// spawn threads and send messages to actors to manage the restore requests.
///
pub struct RestorerImpl {
    // Arbiter manages the supervised actor that initiates backups.
    runner: Mutex<Option<Arbiter>>,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<RestoreSupervisor>>>,
    // Queue of incoming requests to be processed.
    pending: Arc<Mutex<VecDeque<Request>>>,
    // Limited number of recently completed requests.
    completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
}

impl RestorerImpl {
    /// Construct a new instance of RestorerImpl.
    pub fn new() -> Self {
        // create an Arbiter to manage an event loop on a new thread
        Self {
            runner: Mutex::new(None),
            super_addr: Mutex::new(None),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            completed: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
        }
    }

    /// Clear the completed requests list, for testing.
    pub fn reset_completed(&self) {
        let pair = self.completed.clone();
        let (lock, _cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        completed.clear();
    }

    /// Wait for at least one request to be completed, for testing.
    pub fn wait_for_completed(&self) {
        let pair = self.completed.clone();
        let (lock, cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        while completed.is_empty() {
            completed = cvar.wait(completed).unwrap();
        }
    }
}

impl Restorer for RestorerImpl {
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error> {
        let mut runner = self.runner.lock().unwrap();
        if runner.is_none() {
            *runner = Some(Arbiter::new());
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let pending = self.pending.clone();
            let completed = self.completed.clone();
            let addr = actix::Supervisor::start_in_arbiter(
                &runner.as_ref().unwrap().handle(),
                move |_| RestoreSupervisor::new(repo, pending, completed),
            );
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn enqueue(&self, request: Request) -> Result<(), Error> {
        debug!("restore: enqueuing request {}", request.digest);
        let mut queue = self.pending.lock().unwrap();
        queue.push_back(request);
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            fn err_convert(err: SendError<Restore>) -> Error {
                anyhow!(format!("RestorerImpl::enqueue(): {:?}", err))
            }
            addr.try_send(Restore()).map_err(err_convert)
        } else {
            error!("restore: must call start() first");
            Err(anyhow!("must call start() first"))
        }
    }

    fn requests(&self) -> Vec<Request> {
        let mut requests: Vec<Request> = Vec::new();
        let queue = self.pending.lock().unwrap();
        let slices = queue.as_slices();
        requests.extend_from_slice(slices.0);
        requests.extend_from_slice(slices.1);
        let pair = self.completed.clone();
        let (lock, _cvar) = &*pair;
        let completed = lock.lock().unwrap();
        let slices = completed.as_slices();
        requests.extend_from_slice(slices.0);
        requests.extend_from_slice(slices.1);
        requests
    }

    fn cancel(&self, request: Request) -> bool {
        let mut queue = self.pending.lock().unwrap();
        let position = queue.iter().position(|r| r == &request);
        if let Some(idx) = position {
            queue.remove(idx);
            return true;
        }
        false
    }

    fn stop(&self) -> Result<(), Error> {
        fn err_convert(err: SendError<Stop>) -> Error {
            anyhow!(format!("RestorerImpl::stop(): {:?}", err))
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.take() {
            addr.try_send(Stop()).map_err(err_convert)?;
        }
        let mut runner = self.runner.lock().unwrap();
        if let Some(runr) = runner.take() {
            runr.stop();
        }
        Ok(())
    }
}

struct RestoreSupervisor {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Queue of incoming requests to be processed.
    pending: Arc<Mutex<VecDeque<Request>>>,
    // List to which completed tasks are added (in the front).
    completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
}

impl RestoreSupervisor {
    fn new(
        repo: Arc<dyn RecordRepository>,
        pending: Arc<Mutex<VecDeque<Request>>>,
        completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
    ) -> Self {
        Self {
            dbase: repo,
            pending,
            completed,
        }
    }

    fn process_queue(&mut self) -> Result<(), Error> {
        // Construct the pack fetcher that will keep track of which pack files
        // have been downloaded to avoid fetching the same one twice.
        let mut fetcher = FileRestorer::new(self.dbase.clone());
        // Process all of the requests in the queue using the one fetcher, in
        // the hopes that there may be some overlap of the pack files.
        while let Some(request) = self.pop_incoming() {
            debug!("restore: processing request {}", request.digest);
            let mut req = request.clone();
            if let Err(error) = fetcher.load_dataset(&request.dataset) {
                self.set_error(error, &mut req);
            } else {
                if request.digest.is_sha256() {
                    if let Err(error) = self.process_file(
                        &mut req,
                        request.digest.clone(),
                        request.filepath.clone(),
                        &mut fetcher,
                    ) {
                        self.set_error(error, &mut req);
                    }
                } else {
                    if let Err(error) = self.process_tree(
                        &mut req,
                        request.digest.clone(),
                        request.filepath.clone(),
                        &mut fetcher,
                    ) {
                        self.set_error(error, &mut req);
                    }
                }
            }
            self.push_completed(req);
        }
        Ok(())
    }

    fn process_file(
        &self,
        request: &mut Request,
        digest: Checksum,
        filepath: PathBuf,
        fetcher: &mut FileRestorer,
    ) -> Result<(), Error> {
        // fetch the packs for the file and assemble the chunks
        fetcher.fetch_file(&digest, &filepath, &request.passphrase)?;
        // update the count of files restored so far
        request.files_restored += 1;
        Ok(())
    }

    fn process_tree(
        &self,
        request: &mut Request,
        digest: Checksum,
        path: PathBuf,
        fetcher: &mut FileRestorer,
    ) -> Result<(), Error> {
        let tree = self
            .dbase
            .get_tree(&digest)?
            .ok_or_else(|| anyhow!(format!("missing tree: {:?}", digest)))?;
        for entry in tree.entries.iter() {
            let mut filepath = path.clone();
            filepath.push(&entry.name);
            match &entry.reference {
                TreeReference::LINK(contents) => {
                    fetcher.restore_link(contents, filepath)?;
                }
                TreeReference::TREE(digest) => {
                    self.process_tree(request, digest.to_owned(), filepath, fetcher)?;
                }
                TreeReference::FILE(digest) => {
                    self.process_file(request, digest.to_owned(), filepath, fetcher)?;
                }
                TreeReference::SMALL(contents) => {
                    fetcher.restore_small(contents, filepath)?;
                }
            }
        }
        Ok(())
    }

    fn pop_incoming(&self) -> Option<Request> {
        let mut queue = self.pending.lock().unwrap();
        queue.pop_front()
    }

    fn push_completed(&self, request: Request) {
        let mut req = request;
        req.finished = Some(Utc::now());
        let pair = self.completed.clone();
        let (lock, cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        // Push the completed request to the front of the list and truncate the
        // older items to keep the list from growing indefinitely.
        completed.push_front(req);
        completed.truncate(32);
        cvar.notify_all();
    }

    fn set_error(&self, error: Error, request: &mut Request) {
        let err_string = error.to_string();
        request.error_msg = Some(err_string);
    }
}

impl Actor for RestoreSupervisor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!("restore: supervisor started");
    }
}

impl Supervised for RestoreSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<RestoreSupervisor>) {
        debug!("restore: supervisor restarting");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

impl Handler<Stop> for RestoreSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<RestoreSupervisor>) {
        debug!("restore: supervisor received stop message");
        ctx.stop();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Restore();

impl Handler<Restore> for RestoreSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Restore, _ctx: &mut Context<RestoreSupervisor>) {
        debug!("restore: supervisor received restore message");
        if let Err(err) = self.process_queue() {
            error!("supervisor error processing queue: {}", err);
        }
    }
}

struct FileRestorer {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Identifier of the loaded data set, if any.
    dataset: Option<String>,
    // Pack repository for retrieving pack files.
    stores: Option<Box<dyn PackRepository>>,
    // Base path to which files will be restored.
    basepath: Option<PathBuf>,
    // Temporary location where packs and chunks are downloaded.
    packpath: Option<tempfile::TempDir>,
    // Those pack files that have already been fetched.
    downloaded: HashSet<Checksum>,
}

impl FileRestorer {
    fn new(dbase: Arc<dyn RecordRepository>) -> Self {
        Self {
            dbase,
            dataset: None,
            stores: None,
            basepath: None,
            packpath: None,
            downloaded: HashSet::new(),
        }
    }

    fn load_dataset(&mut self, dataset_id: &str) -> Result<(), Error> {
        if let Some(id) = self.dataset.as_ref() {
            if id == dataset_id {
                return Ok(());
            }
        }
        let dataset = self
            .dbase
            .get_dataset(dataset_id)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", dataset_id)))?;
        self.dataset = Some(dataset_id.to_owned());
        self.stores = Some(self.dbase.load_dataset_stores(&dataset)?);
        fs::create_dir_all(&dataset.workspace)?;
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
        let workspace = self.packpath.as_ref().unwrap().path().to_path_buf();
        fs::create_dir_all(&workspace)?;
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
            assemble_chunks(&chunk_paths, &outfile)?;
        } else {
            // look up chunk records to get pack record(s)
            for (_offset, chunk) in &saved_file.chunks {
                let chunk_rec = self
                    .dbase
                    .get_chunk(&chunk)?
                    .ok_or_else(|| anyhow!(format!("missing chunk: {:?}", chunk)))?;
                let pack_digest = chunk_rec.packfile.as_ref().unwrap();
                self.fetch_pack(&pack_digest, &workspace, passphrase)?;
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
            assemble_chunks(&chunk_paths, &outfile)?;
        }
        Ok(())
    }

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
            // check the salt before downloading the pack, otherwise we waste
            // time fetching it when we would not be able to decrypt it
            let salt = saved_pack
                .crypto_salt
                .ok_or_else(|| anyhow!(format!("missing pack salt: {:?}", pack_digest)))?;
            // retrieve the pack file
            let mut encrypted = PathBuf::new();
            encrypted.push(workspace);
            encrypted.push(pack_digest.to_string());
            debug!("restore: fetching pack {}", pack_digest);
            stores.retrieve_pack(&saved_pack.locations, &encrypted)?;
            // decrypt and then unpack the contents
            let mut tarball = encrypted.clone();
            tarball.set_extension("tar");
            super::decrypt_file(passphrase, &salt, &encrypted, &tarball)?;
            fs::remove_file(&encrypted)?;
            verify_pack_digest(pack_digest, &tarball)?;
            super::extract_pack(&tarball, &workspace)?;
            fs::remove_file(tarball)?;
            // remember this pack as being downloaded
            self.downloaded.insert(pack_digest.to_owned());
        }
        Ok(())
    }

    fn restore_link(&self, encoded: &String, filepath: PathBuf) -> Result<(), Error> {
        // The backup procedure ensures that the data collected is able to be
        // converted back, so simply raise errors if anything goes wrong.
        let decoded_raw = base64::decode(encoded)?;
        let target = std::str::from_utf8(&decoded_raw)?;
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        fs::remove_file(&outfile)?;
        // cfg! macro will not work in this OS-specific import case
        {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs;
            #[cfg(target_family = "windows")]
            use std::os::windows::fs;
            #[cfg(target_family = "unix")]
            fs::symlink(&target, &outfile)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file(&target, &outfile)?;
        }
        Ok(())
    }

    fn restore_small(&self, contents: &[u8], filepath: PathBuf) -> Result<(), Error> {
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        fs::write(&outfile, contents)?;
        Ok(())
    }
}

impl Drop for FileRestorer {
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
    let actual = Checksum::sha256_from_file(path)?;
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
    if let Some(parent) = outfile.parent() {
        fs::create_dir_all(parent)?;
        let mut file = fs::File::create(outfile)?;
        for infile in chunks {
            let mut cfile = fs::File::open(infile)?;
            std::io::copy(&mut cfile, &mut file)?;
        }
        return Ok(());
    }
    Err(anyhow!(format!("no parent for: {:?}", outfile)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{Chunk, Dataset, File, Pack, PackLocation, Tree, TreeEntry};
    use crate::domain::managers;
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use std::io;

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
    async fn test_restorer_enqueue_then_fail() -> io::Result<()> {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(|_| Err(anyhow!("oh no!")));
        let repo = Arc::new(mock);
        let sut = RestorerImpl::new();
        // act
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA256("cafebabe".into()),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        ));
        // assert
        assert!(result.is_ok());
        sut.wait_for_completed();
        let requests = sut.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert!(request.error_msg.is_some());
        assert!(request.error_msg.as_ref().unwrap().contains("oh no"));
        Ok(())
    }

    // Simplified pack builder that assumes all chunks will fit in a 64mb pack
    // file. Returns the digest of the resulting file.
    fn pack_chunks(chunks: &[Chunk], outfile: &Path) -> Result<Checksum, Error> {
        let mut builder = managers::PackBuilder::new(67108864);
        builder.initialize(outfile)?;
        for chunk in chunks {
            builder.add_chunk(chunk)?;
        }
        let _output = builder.finalize()?;
        let digest = Checksum::sha256_from_file(outfile)?;
        Ok(digest)
    }

    // A pack with one file containing one chunk.
    struct PackChunkFile {
        pack: Pack,
        packpath: String,
        file: File,
    }

    // create a pack containing one file with a single chunk
    fn create_single_pack(
        passphrase: &str,
        filepath: &Path,
        packpath: &Path,
    ) -> Result<PackChunkFile, Error> {
        let file_digest = Checksum::sha256_from_file(filepath)?;
        let fs_file = fs::File::open(filepath)?;
        let file_len = fs_file.metadata()?.len();
        let mut chunk = Chunk::new(file_digest.clone(), 0, file_len as usize);
        chunk = chunk.filepath(filepath);
        let chunks = vec![chunk.clone()];
        let mut pack_file = packpath.to_path_buf();
        pack_file.push(filepath.file_stem().unwrap());
        pack_file.set_extension("tar");
        let pack_digest = pack_chunks(&chunks, &pack_file)?;
        let mut encrypted = pack_file.clone();
        encrypted.set_extension("nacl");
        let salt = managers::encrypt_file(passphrase, &pack_file, &encrypted)?;
        std::fs::remove_file(pack_file)?;
        let pack_file_path = encrypted.to_string_lossy().into_owned();
        // create file record chose "chunk" digest is actually the pack digest
        let file = File::new(
            file_digest.clone(),
            file_len,
            vec![(0, pack_digest.clone())],
        );
        let object = pack_digest.to_string();
        let location = PackLocation::new("store1", "bucket1", &object);
        let mut pack = Pack::new(pack_digest, vec![location]);
        pack.crypto_salt = Some(salt);
        Ok(PackChunkFile {
            pack,
            packpath: pack_file_path,
            file,
        })
    }

    #[actix_rt::test]
    async fn test_restorer_fail_then_succeed() -> io::Result<()> {
        // arrange
        let tmpdir = tempfile::tempdir()?;
        let dataset = Dataset::new(tmpdir.path());
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_get_file()
            .withf(|digest| digest.to_string() == "sha256-deadbeef")
            .returning(|_| Err(anyhow!("oh no")));

        // create a realistic pack file but mock database records
        let passphrase = managers::get_passphrase();
        let packed1 = create_single_pack(
            &passphrase,
            Path::new("../test/fixtures/lorem-ipsum.txt"),
            tmpdir.path(),
        )
        .unwrap();
        let packed1_file = packed1.file.clone();
        let packed1_pack = packed1.pack.clone();
        mock.expect_load_dataset_stores().returning(move |_| {
            let pack_file_path_clone = packed1.packpath.clone();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |_, outfile| {
                    std::fs::rename(pack_file_path_clone.clone(), outfile).unwrap();
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_file()
            .returning(move |_| Ok(Some(packed1_file.clone())));
        mock.expect_get_pack()
            .returning(move |_| Ok(Some(packed1_pack.clone())));

        // act with failing request
        let repo = Arc::new(mock);
        let sut = RestorerImpl::new();
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA256("deadbeef".into()),
            PathBuf::from("lorem-ipsum.txt"),
            dataset_id.clone(),
            passphrase.clone(),
        ));
        assert!(result.is_ok());
        // assert failure
        sut.wait_for_completed();
        let requests = sut.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert!(request.error_msg.is_some());
        assert!(request.error_msg.as_ref().unwrap().contains("oh no"));

        // act with successful request
        sut.reset_completed();
        let mut filepath = tmpdir.path().to_path_buf();
        filepath.push("lorem-ipsum.txt");
        assert!(!filepath.exists());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA256("cafebabe".into()),
            PathBuf::from("lorem-ipsum.txt"),
            dataset_id.clone(),
            passphrase.clone(),
        ));
        assert!(result.is_ok());
        // assert success
        sut.wait_for_completed();
        let requests = sut.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert!(request.error_msg.is_none());
        let digest_expected = Checksum::SHA256(String::from(
            "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
        ));
        let digest_actual = Checksum::sha256_from_file(&filepath)?;
        assert_eq!(digest_expected, digest_actual);
        Ok(())
    }

    // A pack with one file containing multiple chunks.
    struct PackChunksFile {
        pack: Pack,
        packpath: String,
        chunks: Vec<Chunk>,
        file: File,
    }

    // create a pack containing one file with multiple chunks of 32kb
    fn create_multi_pack(
        passphrase: &str,
        filepath: &Path,
        packpath: &Path,
    ) -> Result<PackChunksFile, Error> {
        let file_digest = Checksum::sha256_from_file(filepath).unwrap();
        let mut chunks: Vec<Chunk> = managers::find_file_chunks(filepath, 32768)?;
        let mut pack_file = packpath.to_path_buf();
        pack_file.push(filepath.file_stem().unwrap());
        pack_file.set_extension("tar");
        let pack_digest = pack_chunks(&chunks, &pack_file).unwrap();
        for chunk in chunks.iter_mut() {
            chunk.packfile = Some(pack_digest.clone());
        }
        let mut encrypted = pack_file.clone();
        encrypted.set_extension("nacl");
        let salt = managers::encrypt_file(passphrase, &pack_file, &encrypted).unwrap();
        std::fs::remove_file(pack_file).unwrap();
        let pack_file_path = encrypted.to_string_lossy().into_owned();
        // construct the list of chunks for the file record
        let mut file_chunks: Vec<(u64, Checksum)> = Vec::new();
        let mut chunk_offset: u64 = 0;
        for chunk in chunks.iter() {
            file_chunks.push((chunk_offset, chunk.digest.clone()));
            chunk_offset += chunk.length as u64;
        }
        let fs_file = fs::File::open(filepath)?;
        let file_len = fs_file.metadata()?.len();
        let file = File::new(file_digest.clone(), file_len, file_chunks);
        let object = pack_digest.to_string();
        let location = PackLocation::new("store1", "bucket1", &object);
        let mut pack = Pack::new(pack_digest, vec![location]);
        pack.crypto_salt = Some(salt);
        Ok(PackChunksFile {
            pack,
            packpath: pack_file_path,
            chunks,
            file,
        })
    }

    #[actix_rt::test]
    async fn test_restorer_restore_tree() -> io::Result<()> {
        // arrange
        let tmpdir = tempfile::tempdir()?;
        let dataset = Dataset::new(tmpdir.path());
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));

        // create realistic pack files but mock database records
        let passphrase = managers::get_passphrase();
        let packed1 = create_single_pack(
            &passphrase,
            Path::new("../test/fixtures/lorem-ipsum.txt"),
            tmpdir.path(),
        )
        .unwrap();
        let packed1_file = packed1.file.clone();
        let packed1_pack = packed1.pack.clone();
        let file1_digest = packed1_file.digest.clone();
        let pack1_digest = packed1_pack.digest.clone();
        let pack1_digest_copy = packed1_pack.digest.clone();
        let packed2 = create_single_pack(
            &passphrase,
            Path::new("../test/fixtures/washington-journal.txt"),
            tmpdir.path(),
        )
        .unwrap();
        let packed2_file = packed2.file.clone();
        let packed2_pack = packed2.pack.clone();
        let file2_digest = packed2_file.digest.clone();
        let pack2_digest = packed2_pack.digest.clone();
        let pack2_digest_copy = packed2_pack.digest.clone();
        let packed3 = create_multi_pack(
            &passphrase,
            Path::new("../test/fixtures/SekienAkashita.jpg"),
            tmpdir.path(),
        )
        .unwrap();
        let packed3_file = packed3.file.clone();
        // The image file should be broken into 3 chunks, which the rest of this
        // test function assumes to be the case, so assert for certainty.
        assert_eq!(packed3.chunks.len(), 3);
        let packed3_chunks = packed3.chunks.clone();
        let packed3_pack = packed3.pack.clone();
        let file3_digest = packed3_file.digest.clone();
        let chunk3_1_digest = packed3_chunks[0].digest.clone();
        let chunk3_2_digest = packed3_chunks[1].digest.clone();
        let chunk3_3_digest = packed3_chunks[2].digest.clone();
        let packed3_chunk_1 = packed3_chunks[0].clone();
        let packed3_chunk_2 = packed3_chunks[1].clone();
        let packed3_chunk_3 = packed3_chunks[2].clone();
        let pack3_digest = packed3_pack.digest.clone();
        let pack3_digest_copy = packed3_pack.digest.clone();
        mock.expect_load_dataset_stores().returning(move |_| {
            let pack1_path = packed1.packpath.clone();
            let packed1_digest = pack1_digest.clone().to_string();
            let pack2_path = packed2.packpath.clone();
            let packed2_digest = pack2_digest.clone().to_string();
            let pack3_path = packed3.packpath.clone();
            let packed3_digest = pack3_digest.clone().to_string();
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_pack()
                .returning(move |locations, outfile| {
                    if locations[0].object == packed1_digest {
                        std::fs::rename(pack1_path.clone(), outfile).unwrap();
                    } else if locations[0].object == packed2_digest {
                        std::fs::rename(pack2_path.clone(), outfile).unwrap();
                    } else if locations[0].object == packed3_digest {
                        std::fs::rename(pack3_path.clone(), outfile).unwrap();
                    }
                    Ok(())
                });
            Ok(Box::new(mock_store))
        });
        mock.expect_get_file()
            .with(eq(file1_digest))
            .returning(move |_| Ok(Some(packed1_file.clone())));
        mock.expect_get_file()
            .with(eq(file2_digest))
            .returning(move |_| Ok(Some(packed2_file.clone())));
        mock.expect_get_file()
            .with(eq(file3_digest))
            .returning(move |_| Ok(Some(packed3_file.clone())));
        mock.expect_get_chunk()
            .with(eq(chunk3_1_digest))
            .returning(move |_| Ok(Some(packed3_chunk_1.clone())));
        mock.expect_get_chunk()
            .with(eq(chunk3_2_digest))
            .returning(move |_| Ok(Some(packed3_chunk_2.clone())));
        mock.expect_get_chunk()
            .with(eq(chunk3_3_digest))
            .returning(move |_| Ok(Some(packed3_chunk_3.clone())));
        mock.expect_get_pack()
            .with(eq(pack1_digest_copy))
            .returning(move |_| Ok(Some(packed1_pack.clone())));
        mock.expect_get_pack()
            .with(eq(pack2_digest_copy))
            .returning(move |_| Ok(Some(packed2_pack.clone())));
        mock.expect_get_pack()
            .with(eq(pack3_digest_copy))
            .returning(move |_| Ok(Some(packed3_pack.clone())));

        let tree = Tree::new(
            vec![
                TreeEntry::new(
                    Path::new("../test/fixtures/lorem-ipsum.txt"),
                    TreeReference::FILE(Checksum::SHA256(String::from(
                        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
                    ))),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/washington-journal.txt"),
                    TreeReference::FILE(Checksum::SHA256(String::from(
                        "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05",
                    ))),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/SekienAkashita.jpg"),
                    TreeReference::FILE(Checksum::SHA256(String::from(
                        "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed",
                    ))),
                ),
            ],
            3,
        );
        let tree_sha1 = tree.digest.clone();
        mock.expect_get_tree()
            .returning(move |_| Ok(Some(tree.clone())));

        // act
        let repo = Arc::new(mock);
        let sut = RestorerImpl::new();
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let mut filepath1 = tmpdir.path().to_path_buf();
        filepath1.push("lorem-ipsum.txt");
        assert!(!filepath1.exists());
        let mut filepath2 = tmpdir.path().to_path_buf();
        filepath2.push("washington-journal.txt");
        assert!(!filepath2.exists());
        let mut filepath3 = tmpdir.path().to_path_buf();
        filepath3.push("SekienAkashita.jpg");
        assert!(!filepath3.exists());
        let result = sut.enqueue(managers::restore::Request::new(
            tree_sha1,
            PathBuf::new(),
            dataset_id.clone(),
            passphrase.clone(),
        ));
        assert!(result.is_ok());

        // assert
        sut.wait_for_completed();
        let requests = sut.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert!(request.error_msg.is_none());
        assert!(filepath1.exists());
        let digest_expected = Checksum::SHA256(String::from(
            "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
        ));
        let digest_actual = Checksum::sha256_from_file(&filepath1)?;
        assert_eq!(digest_expected, digest_actual);
        assert!(filepath2.exists());
        let digest_expected = Checksum::SHA256(String::from(
            "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05",
        ));
        let digest_actual = Checksum::sha256_from_file(&filepath2)?;
        assert_eq!(digest_expected, digest_actual);
        assert!(filepath3.exists());
        let digest_expected = Checksum::SHA256(String::from(
            "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed",
        ));
        let digest_actual = Checksum::sha256_from_file(&filepath3)?;
        assert_eq!(digest_expected, digest_actual);
        Ok(())
    }
}
