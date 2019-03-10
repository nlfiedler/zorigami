//
// Copyright (c) 2019 Nathan Fiedler
//
use dotenv::dotenv;
use std::env;
use std::path::Path;
use tempfile::tempdir;
use zorigami::core::*;
use zorigami::store::*;

#[test]
fn test_sftp_roundtrip() {
    // set up the environment and remote connection
    dotenv().ok();
    let addr_var = env::var("SFTP_ADDR");
    if addr_var.is_err() {
        return;
    }
    let address = addr_var.unwrap();
    let username = env::var("SFTP_USER").unwrap();
    let password = env::var("SFTP_PASSWORD").unwrap();
    let basepath = env::var("SFTP_BASEPATH").unwrap();
    let mut store = sftp::SftpStore::new(&address, &username);
    store = store.password(&password);
    store = store.basepath(&basepath);
    run_store_tests(&store);
}

#[test]
fn test_minio_roundtrip() {
    // set up the environment and remote connection
    dotenv().ok();
    let endp_var = env::var("MINIO_ENDPOINT");
    if endp_var.is_err() {
        return;
    }
    let endpoint = endp_var.unwrap();
    let region = env::var("MINIO_REGION").unwrap();
    let store = minio::MinioStore::new(&region, &endpoint);
    run_store_tests(&store);
}

fn run_store_tests(store: &Store) {
    let unique_id = generate_unique_id("charlie", "localhost");
    let bucket = generate_bucket_name(&unique_id);

    // create a pack file with a checksum name
    let chunks = [Chunk::new(
        "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
        0,
        3129,
    )
    .filepath(Path::new("./test/fixtures/lorem-ipsum.txt"))];
    let outdir = tempdir().unwrap();
    let ptmpfile = outdir.path().join("pack.tar");
    let digest = pack_chunks(&chunks[..], &ptmpfile).unwrap();
    let packfile = outdir.path().join(&digest);
    std::fs::rename(&ptmpfile, &packfile).unwrap();

    // store the pack file on the remote side
    let result = store.store_pack(&packfile, &bucket, &digest);
    assert!(result.is_ok());

    // check for bucket(s) being present; may be more from previous runs
    let result = store.list_buckets();
    assert!(result.is_ok());
    let buckets = result.unwrap();
    assert!(!buckets.is_empty());
    assert!(buckets.contains(&bucket));

    // check for object(s) being present
    let result = store.list_objects(&bucket);
    assert!(result.is_ok());
    let listing = result.unwrap();
    assert!(!listing.is_empty());
    assert!(listing.contains(&digest));

    // retrieve the file and verify by checksum
    let result = store.retrieve_pack(&bucket, &digest, &ptmpfile);
    assert!(result.is_ok());
    let sha256 = checksum_file(&ptmpfile);
    assert_eq!(
        sha256.unwrap(),
        "sha256-9fd73dfe8b3815ebbf9b0932816306526104336017d9ba308e37e48bce5ab150"
    );

    // remove all objects from all buckets, and the buckets, too
    for buckette in buckets {
        let result = store.list_objects(&buckette);
        assert!(result.is_ok());
        let objects = result.unwrap();
        for obj in objects {
            store.delete_object(&buckette, &obj).unwrap();
        }
        store.delete_bucket(&buckette).unwrap();
    }
}
