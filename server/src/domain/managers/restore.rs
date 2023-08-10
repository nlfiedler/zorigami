//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::{Checksum, TreeReference};
use crate::domain::helpers::{crypto, pack};
use crate::domain::managers::state::{RestorerAction, StateStore};
use crate::domain::repositories::{PackRepository, RecordRepository};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::prelude::*;
use log::{debug, error, info, warn};
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
    /// The datetime when the request was completed.
    pub finished: Option<DateTime<Utc>>,
    /// Number of files restored so far during the restoration.
    pub files_restored: u64,
    /// Error message if request processing failed.
    pub error_msg: Option<String>,
}

impl Request {
    pub fn new(
        tree: Checksum,
        entry: String,
        filepath: PathBuf,
        dataset: String,
        passphrase: String,
    ) -> Self {
        Self {
            tree,
            entry,
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
        write!(f, "Request({}, {})", self.tree, self.entry)
    }
}

impl cmp::PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.tree == other.tree && self.entry == other.entry
    }
}

impl cmp::Eq for Request {}

///
/// `Restorer` manages a supervised actor which in turn spawns actors to process
/// file and tree restore requests.
///
#[cfg_attr(test, automock)]
pub trait Restorer: Send + Sync {
    /// Start a supervisor that will run a file restore process.
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
/// Factory method for constructing a FileRestorer.
///
type FileRestorerFactory = fn(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer>;

///
/// Concrete implementation of `Restorer` that uses the actix actor framework to
/// spawn threads and send messages to actors to manage the restore requests.
///
pub struct RestorerImpl {
    // Arbiter manages the supervised actor that initiates restores.
    runner: Arbiter,
    // Application state to be provided to supervisor and runners.
    state: Arc<dyn StateStore>,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<RestoreSupervisor>>>,
    // Queue of incoming requests to be processed.
    pending: Arc<Mutex<VecDeque<Request>>>,
    // Limited number of recently completed requests.
    completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
    // Factory method for the FileRestorer implementation.
    fetcher: FileRestorerFactory,
}

impl RestorerImpl {
    /// Construct a new instance of RestorerImpl.
    pub fn new(state: Arc<dyn StateStore>, fetcher: FileRestorerFactory) -> Self {
        // create an Arbiter to manage an event loop on a new thread
        Self {
            runner: Arbiter::new(),
            state: state.clone(),
            super_addr: Mutex::new(None),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            completed: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
            fetcher,
        }
    }

    /// Set the file restorer factory, for testing.
    pub fn factory(&mut self, fetcher: FileRestorerFactory) -> Result<(), Error> {
        self.fetcher = fetcher;
        fn err_convert(err: SendError<SetFactory>) -> Error {
            anyhow!(format!("RestorerImpl::factory(): {:?}", err))
        }
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            addr.try_send(SetFactory { fetcher }).map_err(err_convert)?;
        }
        Ok(())
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
        debug!("restorer starting...");
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let state = self.state.clone();
            let pending = self.pending.clone();
            let completed = self.completed.clone();
            let fetcher = self.fetcher;
            let addr = actix::Supervisor::start_in_arbiter(&self.runner.handle(), move |_| {
                RestoreSupervisor::new(repo, state, pending, completed, fetcher)
            });
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn enqueue(&self, request: Request) -> Result<(), Error> {
        info!(
            "enqueue request for {} into {}",
            request.entry,
            request.filepath.display()
        );
        let mut queue = self.pending.lock().unwrap();
        queue.push_back(request);
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            fn err_convert(err: SendError<Restore>) -> Error {
                anyhow!(format!("RestorerImpl::enqueue(): {:?}", err))
            }
            addr.try_send(Restore()).map_err(err_convert)
        } else {
            error!("must call start() first");
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
        info!("cancel request for {}/{}", request.tree, request.entry);
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
            addr.try_send(Stop()).map_err(err_convert)
        } else {
            Ok(())
        }
    }
}

struct RestoreSupervisor {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Application state for signaling changes in restore status.
    state: Arc<dyn StateStore>,
    // Queue of incoming requests to be processed.
    pending: Arc<Mutex<VecDeque<Request>>>,
    // List to which completed tasks are added (in the front).
    completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
    // Factory method for the FileRestorer implementation.
    fetcher: FileRestorerFactory,
}

impl RestoreSupervisor {
    fn new(
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        pending: Arc<Mutex<VecDeque<Request>>>,
        completed: Arc<(Mutex<VecDeque<Request>>, Condvar)>,
        fetcher: FileRestorerFactory,
    ) -> Self {
        Self {
            dbase: repo,
            state,
            pending,
            completed,
            fetcher,
        }
    }

    fn process_queue(&mut self) -> Result<(), Error> {
        // Construct the pack fetcher that will keep track of which pack files
        // have been downloaded to avoid fetching the same one twice.
        let mut fetcher = (self.fetcher)(self.dbase.clone());
        // Process all of the requests in the queue using the one fetcher, in
        // the hopes that there may be some overlap of the pack files.
        while let Some(request) = self.pop_incoming() {
            info!("processing request {}/{}", request.tree, request.entry);
            let mut req = request.clone();
            if let Err(error) = fetcher.load_dataset(&request.dataset) {
                error!("process_queue: error loading dataset: {}", error);
                self.set_error(error, &mut req);
            } else {
                if let Err(error) = self.process_entry(&mut req, &mut fetcher) {
                    error!("process_queue: error processing entry: {}", error);
                    self.set_error(error, &mut req);
                }
            }
            self.push_completed(req);
        }
        Ok(())
    }

    fn process_entry(
        &self,
        request: &mut Request,
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
        request: &mut Request,
        digest: Checksum,
        filepath: &PathBuf,
        fetcher: &mut Box<dyn FileRestorer>,
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
        path: &PathBuf,
        fetcher: &mut Box<dyn FileRestorer>,
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
                    if let Err(error) = fetcher.restore_link(contents, &filepath) {
                        error!(
                            "process_tree: error restoring link {}: {}",
                            filepath.display(),
                            error
                        );
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
                    }
                }
                TreeReference::SMALL(contents) => {
                    if let Err(error) = fetcher.restore_small(contents, &filepath) {
                        error!(
                            "process_tree: error restoring small {}: {}",
                            filepath.display(),
                            error
                        );
                    }
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
        debug!("supervisor started");
        self.state.restorer_event(RestorerAction::Started);
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        debug!("supervisor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        debug!("supervisor stopped");
    }
}

impl Supervised for RestoreSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<RestoreSupervisor>) {
        debug!("supervisor restarting");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetFactory {
    // Factory method for the FileRestorer implementation.
    fetcher: FileRestorerFactory,
}

impl Handler<SetFactory> for RestoreSupervisor {
    type Result = ();

    fn handle(&mut self, msg: SetFactory, _ctx: &mut Context<RestoreSupervisor>) {
        debug!("supervisor received SetFactory message");
        self.fetcher = msg.fetcher;
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

impl Handler<Stop> for RestoreSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<RestoreSupervisor>) {
        debug!("supervisor received Stop message");
        self.state.restorer_event(RestorerAction::Stopped);
        ctx.stop();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Restore();

impl Handler<Restore> for RestoreSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Restore, _ctx: &mut Context<RestoreSupervisor>) {
        debug!("supervisor received Restore message");
        if let Err(err) = self.process_queue() {
            error!("supervisor error processing queue: {}", err);
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
    fn restore_link(&self, contents: &[u8], filepath: &PathBuf) -> Result<(), Error>;

    /// Restore the named small file given its contents.
    fn restore_small(&self, contents: &[u8], filepath: &PathBuf) -> Result<(), Error>;
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
            // check the salt before downloading the pack, otherwise we waste
            // time fetching it when we would not be able to decrypt it
            let salt = saved_pack
                .crypto_salt
                .ok_or_else(|| anyhow!(format!("missing pack salt: {:?}", pack_digest)))?;
            // retrieve the pack file
            let mut encrypted = PathBuf::new();
            encrypted.push(workspace);
            encrypted.push(pack_digest.to_string());
            debug!("fetching pack {}", pack_digest);
            stores.retrieve_pack(&saved_pack.locations, &encrypted)?;
            // decrypt and then unpack the contents
            let mut tarball = encrypted.clone();
            tarball.set_extension("tar");
            crypto::decrypt_file(passphrase, &salt, &encrypted, &tarball)?;
            fs::remove_file(&encrypted)?;
            verify_pack_digest(pack_digest, &tarball)?;
            pack::extract_pack(&tarball, &workspace)?;
            debug!("pack extracted");
            fs::remove_file(tarball)?;
            // remember this pack as being downloaded
            self.downloaded.insert(pack_digest.to_owned());
        }
        Ok(())
    }
}

impl FileRestorer for FileRestorerImpl {
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
        self.stores = Some(Arc::from(self.dbase.load_dataset_stores(&dataset)?));
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
        info!("restoring file from {} to {}", checksum, filepath.display());
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
            debug!("assembling N-chunk file {}", outfile.display());
            assemble_chunks(&chunk_paths, &outfile)?;
        }
        Ok(())
    }

    fn restore_link(&self, contents: &[u8], filepath: &PathBuf) -> Result<(), Error> {
        info!("restoring symbolic link: {}", filepath.display());
        #[cfg(target_family = "unix")]
        use std::os::unix::ffi::OsStringExt;
        #[cfg(target_family = "windows")]
        use std::os::windows::ffi::OsStringExt;
        let target = std::ffi::OsString::from_vec(contents.to_owned());
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        if let Some(parent) = outfile.parent() {
            fs::create_dir_all(parent)?;
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
            return Ok(());
        }
        Err(anyhow!(format!("no parent for: {:?}", outfile)))
    }

    fn restore_small(&self, contents: &[u8], filepath: &PathBuf) -> Result<(), Error> {
        info!("restoring small file: {}", filepath.display());
        let mut outfile = self.basepath.clone().unwrap();
        outfile.push(filepath);
        if let Some(parent) = outfile.parent() {
            fs::create_dir_all(parent)?;
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
    use crate::domain::entities::{Dataset, Tree, TreeEntry};
    use crate::domain::managers;
    use crate::domain::managers::state::StateStoreImpl;
    use crate::domain::repositories::MockRecordRepository;
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
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let sut = RestorerImpl::new(state, factory);
        // act
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
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
                TreeReference::FILE(Checksum::SHA256(String::from(
                    "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
                ))),
            )],
            1,
        );
        mock.expect_get_tree()
            .withf(|digest| digest.to_string() == "sha1-cafebabe")
            .returning(move |_| Ok(Some(tree.clone())));

        let passphrase = crypto::get_passphrase();

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

        // act with failing request
        let repo = Arc::new(mock);
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let mut sut = RestorerImpl::new(state, factory_fail);
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA1("deadbeef".into()),
            String::from("lorem-ipsum.txt"),
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
        fn factory_pass(_dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            let mut restorer = MockFileRestorer::new();
            restorer.expect_load_dataset().returning(|_| Ok(()));
            restorer.expect_fetch_file().returning(|_, _, _| Ok(()));
            Box::new(restorer)
        }
        sut.reset_completed();
        let result = sut.factory(factory_pass);
        assert!(result.is_ok());
        let result = sut.enqueue(managers::restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
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

        // act
        let repo = Arc::new(mock);
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let sut = RestorerImpl::new(state, factory);
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        let passphrase = crypto::get_passphrase();
        let result = sut.enqueue(managers::restore::Request::new(
            roottree_sha1,
            String::from("fixtures"),
            PathBuf::from("/home/town"),
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
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_restorer_start_stop_restart() -> io::Result<()> {
        // arrange
        fn factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
            Box::new(FileRestorerImpl::new(dbase))
        }
        let mock = MockRecordRepository::new();
        let repo = Arc::new(mock);
        // start
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let sut = RestorerImpl::new(state.clone(), factory);
        let result = sut.start(repo.clone());
        state.wait_for_restorer(RestorerAction::Started);
        assert!(result.is_ok());
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_restorer(RestorerAction::Stopped);
        // restart
        let result = sut.start(repo);
        state.wait_for_restorer(RestorerAction::Started);
        assert!(result.is_ok());
        Ok(())
    }
}
