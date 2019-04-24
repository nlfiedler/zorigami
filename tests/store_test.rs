//
// Copyright (c) 2019 Nathan Fiedler
//
#[macro_use]
extern crate serde_json;
use dotenv::dotenv;
use failure::Error;
use std::env;
use std::fs;
use std::path::Path;
use tempfile::tempdir;
use zorigami::core::*;
use zorigami::database::*;
use zorigami::store::*;

#[test]
fn test_store_config() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/store/config/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();

    let config_json = json!({
        "name": "not_default_local",
        "basepath": "some/path/for_local",
    });
    let value = config_json.to_string();
    let mut store = local::LocalStore::default();
    run_config_tests(&value, &mut store, &dbase)?;

    let config_json = json!({
        "name": "some_sftp_name",
        "remote_addr": "localhost:22",
        "username": "joe",
        "password": "secret123",
        "basepath": ".",
    });
    let mut store = sftp::SftpStore::default();
    let value = config_json.to_string();
    run_config_tests(&value, &mut store, &dbase)?;

    let config_json = json!({
        "name": "mister_minio",
        "region": "us-west-1",
        "endpoint": "http://localhost:9000",
    });
    let mut store = minio::MinioStore::default();
    let value = config_json.to_string();
    run_config_tests(&value, &mut store, &dbase)?;

    Ok(())
}

fn run_config_tests(config: &str, store: &mut Store, dbase: &Database) -> Result<(), Error> {
    store.get_config_mut().from_json(config)?;
    save_store(dbase, store)?;
    let stores: Vec<String> = find_stores(dbase)?;
    assert!(!stores.is_empty());
    let type_name = store.get_type().to_string();
    let conf_name = store.get_config().get_name();
    let store_key = format!("store/{}/{}", type_name, conf_name);
    assert!(stores.contains(&store_key));
    let boxster: Box<Store> = load_store(dbase, &store_key)?;
    assert_eq!(boxster.get_type().to_string(), type_name);
    assert_eq!(boxster.get_config().get_name(), conf_name);
    Ok(())
}

#[test]
fn test_local_roundtrip() -> Result<(), Error> {
    let config_json = json!({
        "name": "default",
        "basepath": "tmp/test/local_store",
    });
    let mut store = local::LocalStore::default();
    let value = config_json.to_string();
    store.get_config_mut().from_json(&value)?;
    run_store_tests(&store);
    Ok(())
}

#[test]
fn test_sftp_roundtrip() -> Result<(), Error> {
    // set up the environment and remote connection
    dotenv().ok();
    let addr_var = env::var("SFTP_ADDR");
    if addr_var.is_err() {
        return Ok(());
    }
    let address = addr_var.unwrap();
    let username = env::var("SFTP_USER").unwrap();
    let password = env::var("SFTP_PASSWORD").unwrap();
    let basepath = env::var("SFTP_BASEPATH").unwrap();
    let config_json = json!({
        "name": "default",
        "remote_addr": address,
        "username": username,
        "password": password,
        "basepath": basepath,
    });
    let mut store = sftp::SftpStore::default();
    let value = config_json.to_string();
    store.get_config_mut().from_json(&value)?;
    run_store_tests(&store);
    Ok(())
}

#[test]
fn test_minio_roundtrip() -> Result<(), Error> {
    // set up the environment and remote connection
    dotenv().ok();
    let endp_var = env::var("MINIO_ENDPOINT");
    if endp_var.is_err() {
        return Ok(());
    }
    let endpoint = endp_var.unwrap();
    let region = env::var("MINIO_REGION").unwrap();
    let config_json = json!({
        "name": "default",
        "region": region,
        "endpoint": endpoint,
    });
    let mut store = minio::MinioStore::default();
    let value = config_json.to_string();
    store.get_config_mut().from_json(&value)?;
    run_store_tests(&store);
    Ok(())
}

fn run_store_tests(store: &Store) {
    let unique_id = generate_unique_id("charlie", "localhost");
    let bucket = generate_bucket_name(&unique_id);

    // create a pack file with a checksum name
    let chnksum = Checksum::SHA256(
        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f".to_owned(),
    );
    let chunks =
        [Chunk::new(chnksum, 0, 3129).filepath(Path::new("./test/fixtures/lorem-ipsum.txt"))];
    let outdir = tempdir().unwrap();
    let ptmpfile = outdir.path().join("pack.tar");
    let digest = pack_chunks(&chunks[..], &ptmpfile).unwrap();
    let digest_sum = &digest.to_string();
    let packfile = outdir.path().join(&digest_sum);
    std::fs::rename(&ptmpfile, &packfile).unwrap();

    // store the pack file on the remote side
    let result = store.store_pack(&packfile, &bucket, &digest_sum);
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
    assert!(listing.contains(&digest_sum));

    // retrieve the file and verify by checksum
    let result = store.retrieve_pack(&bucket, &digest_sum, &ptmpfile);
    assert!(result.is_ok());
    let sha256 = checksum_file(&ptmpfile);
    assert_eq!(
        sha256.unwrap().to_string(),
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