//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities;
use crate::domain::repositories::RecordRepository;
use failure::{err_msg, Error};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

///
/// Restore a single file identified by the given checksum.
///
pub fn restore_file(
    dbase: &Arc<dyn RecordRepository>,
    dataset: &entities::Dataset,
    passphrase: &str,
    checksum: entities::Checksum,
    outfile: &Path,
) -> Result<(), Error> {
    let stores = dbase.load_dataset_stores(&dataset)?;
    // look up the file record to get chunks
    let saved_file = dbase
        .get_file(&checksum)?
        .ok_or_else(|| err_msg(format!("missing file: {:?}", checksum)))?;
    // create an index of all the chunks we want to collect (using strings
    // because the extracted chunks consist of a list of file names)
    let mut desired_chunks: HashSet<String> = HashSet::new();
    for (_offset, chunk) in &saved_file.chunks {
        desired_chunks.insert(chunk.to_string());
    }
    // track pack files that have already been processed
    let mut finished_packs: HashSet<entities::Checksum> = HashSet::new();
    // look up chunk records to get pack record(s)
    for (_offset, chunk) in &saved_file.chunks {
        let chunk_rec = dbase
            .get_chunk(&chunk)?
            .ok_or_else(|| err_msg(format!("missing chunk: {:?}", chunk)))?;
        let pack_digest = chunk_rec.packfile.as_ref().unwrap();
        if !finished_packs.contains(pack_digest) {
            let saved_pack = dbase
                .get_pack(pack_digest)?
                .ok_or_else(|| err_msg(format!("missing pack record: {:?}", pack_digest)))?;
            // check the salt before downloading the pack, otherwise we waste
            // time fetching it when we would not be able to decrypt it
            let salt = saved_pack
                .crypto_salt
                .ok_or_else(|| err_msg(format!("missing pack salt: {:?}", pack_digest)))?;
            // retrieve the pack file
            let encrypted = tempfile::Builder::new()
                .prefix("pack")
                .suffix(".bin")
                .tempfile_in(&dataset.workspace)?;
            stores.retrieve_pack(&saved_pack.locations, encrypted.path())?;
            // decrypt and then decompress before unpacking the contents
            let mut zipped = outfile.to_path_buf();
            zipped.set_extension("gz");
            super::decrypt_file(passphrase, &salt, encrypted.path(), &zipped)?;
            fs::remove_file(encrypted)?;
            super::decompress_file(&zipped, outfile)?;
            fs::remove_file(zipped)?;
            verify_pack_digest(pack_digest, outfile)?;
            let chunk_names = super::unpack_chunks(outfile, &dataset.workspace)?;
            fs::remove_file(outfile)?;
            // remove unrelated chunks to conserve space
            for cname in chunk_names {
                if !desired_chunks.contains(&cname) {
                    let mut chunk_path = PathBuf::from(&dataset.workspace);
                    chunk_path.push(cname);
                    fs::remove_file(&chunk_path)?;
                }
            }
            // remember this pack as being completed
            finished_packs.insert(pack_digest.to_owned());
        }
    }
    // sort the chunks by offset to produce the ordered file list
    let mut chunks = saved_file.chunks;
    chunks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let chunk_bufs: Vec<PathBuf> = chunks
        .iter()
        .map(|c| {
            let mut cpath = PathBuf::from(&dataset.workspace);
            cpath.push(c.1.to_string());
            cpath
        })
        .collect();
    let chunk_paths: Vec<&Path> = chunk_bufs.iter().map(|b| b.as_path()).collect();
    super::assemble_chunks(&chunk_paths, outfile)?;
    Ok(())
}

/// Verify the retrieved pack file digest matches the database record.
fn verify_pack_digest(digest: &entities::Checksum, path: &Path) -> Result<(), Error> {
    let actual = entities::Checksum::sha256_from_file(path)?;
    if &actual != digest {
        Err(err_msg(format!(
            "pack digest does not match: {} != {}",
            &actual, digest
        )))
    } else {
        Ok(())
    }
}
