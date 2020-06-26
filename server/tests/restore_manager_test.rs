//
// Copyright (c) 2020 Nathan Fiedler
//
mod common;

use common::DBPath;

use failure::Error;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::tempdir;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities::{self, Checksum};
use server::domain::managers::backup::*;
use server::domain::managers::restore::*;
use server::domain::repositories::RecordRepository;

#[test]
fn test_backup_restore() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_restore");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    #[cfg(target_family = "unix")]
    let pack_path = "tmp/test/managers/backup/packs";
    #[cfg(target_family = "windows")]
    let pack_path = "tmp\\test\\managers\\backup\\packs";
    let _ = fs::remove_dir_all(pack_path);

    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert("basepath".to_owned(), pack_path.to_owned());
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
    };
    dbase.put_store(&store)?;

    // create a dataset
    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/managers/backup/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\managers\\backup\\fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mut dataset = entities::Dataset::new(Path::new(basepath));
    dataset = dataset.add_store("local123");
    dataset.pack_size = 65536 as u64;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let dest: PathBuf = [basepath, "zero-length.txt"].iter().collect();
    assert!(fs::write(dest, vec![]).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // perform the second backup
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // perform the third backup
    let dest: PathBuf = [basepath, "washington-journal.txt"].iter().collect();
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // perform the fourth backup with shifted larger file
    let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
    let outfile: PathBuf = [basepath, "SekienShifted.jpg"].iter().collect();
    copy_with_prefix("mary had a little lamb", &infile, &outfile)?;
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // restore the file from the first snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::SHA256(String::from(
        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::SHA256(String::from(
        "1ed890fb1b875a5d7637d54856dc36195bed2e8e40fe6c155a2908b8dd00ebee",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = Checksum::sha256_from_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the second snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = Checksum::sha256_from_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the third snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::SHA256(String::from(
        "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::SHA256(String::from(
        "494cb077670d424f47a3d33929d6f1cbcf408a06d28be11259b2fe90666010dc",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = Checksum::sha256_from_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the fourth snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "b2c67e90a01f5d7aca48835b8ad8f0902ef03288aa4083e742bccbd96d8590a4",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = Checksum::sha256_from_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the zero length file from the first snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let attr = fs::metadata(&restored_file)?;
    assert_eq!(0, attr.len());
    let digest_actual = Checksum::sha256_from_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);
    Ok(())
}

///
/// Copy one file to another, prepending the result with the given text.
///
fn copy_with_prefix(header: &str, infile: &Path, outfile: &Path) -> Result<(), Error> {
    let mut reader: &[u8] = header.as_bytes();
    let mut writer = fs::File::create(outfile)?;
    std::io::copy(&mut reader, &mut writer)?;
    let mut reader = fs::File::open(infile)?;
    std::io::copy(&mut reader, &mut writer)?;
    Ok(())
}
