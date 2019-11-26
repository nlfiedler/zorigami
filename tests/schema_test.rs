//
// Copyright (c) 2019 Nathan Fiedler
//
mod util;

use chrono::{TimeZone, Utc};
use failure::Error;
use juniper::{InputValue, ToInputValue, Variables};
use std::collections::HashMap;
use std::path::Path;
use util::DBPath;
use zorigami::core;
use zorigami::database::*;
use zorigami::schema::*;
use zorigami::store;

#[test]
fn test_store_access() -> Result<(), Error> {
    let db_path = DBPath::new("_test_store_access");
    let ctx = Database::new(&db_path).unwrap();
    let schema = create_schema();

    // make sure there are no stores in the database
    let (res, _errors) = juniper::execute(
        r#"query { stores { key } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("stores").unwrap();
    let res = res.as_list_value().unwrap();
    assert!(res.is_empty());

    // query for a store that does not exist, should return one
    // with default settings
    let (res, _errors) = juniper::execute(
        r#"query { store(key: "store/local/foobar") { kind options } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("store").unwrap();
    let obj = res.as_object_value().unwrap();
    let res = obj.get_field_value("kind").unwrap();
    let res = res.as_scalar_value::<String>().unwrap();
    assert_eq!(res, "local");
    let res = obj.get_field_value("options").unwrap();
    let res = res.as_scalar_value::<String>().unwrap();
    let decoded = base64::decode(&res)?;
    let json = std::str::from_utf8(&decoded)?;
    assert!(json.contains("basepath"));

    // define a new local store with some options
    let mut vars = Variables::new();
    let options = base64::encode(r#"{"label": "foobar", "basepath": "/some/local/path"}"#);
    vars.insert("options".to_owned(), InputValue::scalar(options));
    let (res, _errors) = juniper::execute(
        r#"mutation DefineStore($options: String!) {
            defineStore(typeName: "local", options: $options) {
                key
                label
            }
        }"#,
        Some("DefineStore"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("defineStore").unwrap();
    let obj = res.as_object_value().unwrap();
    let field = obj.get_field_value("key").unwrap();
    let key = field.as_scalar_value::<String>().unwrap();
    assert!(key.starts_with("store/local/"));
    let field = obj.get_field_value("label").unwrap();
    let label = field.as_scalar_value::<String>().unwrap();
    assert_eq!(label, "foobar");

    // call stores query to ensure the new local store is returned
    let (res, _errors) = juniper::execute(
        r#"query { stores { key } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("stores").unwrap();
    let res = res.as_list_value().unwrap();
    assert_eq!(res.len(), 1);
    let res = res[0].as_object_value().unwrap();
    let res = res.get_field_value("key").unwrap();
    let actual = res.as_scalar_value::<String>().unwrap();
    assert!(key.starts_with("store/local/"));
    assert!(key.ends_with(actual));

    // fetch the local store to make sure the options were saved
    let query = format!(r#"query {{ store(key: "{}") {{ options }} }}"#, key);
    let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("store").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("options").unwrap();
    let res = res.as_scalar_value::<String>().unwrap();
    let decoded = base64::decode(&res)?;
    let json = std::str::from_utf8(&decoded)?;
    assert!(json.contains("/some/local/path"));

    // update the store configuration to something different
    let mut vars = Variables::new();
    vars.insert("key".to_owned(), InputValue::scalar(key.to_owned()));
    let options = base64::encode(r#"{"label": "foobar", "basepath": "/totally/different"}"#);
    vars.insert("options".to_owned(), InputValue::scalar(options));
    let (res, _errors) = juniper::execute(
        r#"mutation UpdateStore($key: String!, $options: String!) {
            updateStore(key: $key, options: $options) {
                key
            }
        }"#,
        Some("UpdateStore"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("updateStore").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("key").unwrap();
    let key = res.as_scalar_value::<String>().unwrap();
    assert!(key.starts_with("store/local/"));

    // fetch the local store to make sure the options were saved
    let query = format!(r#"query {{ store(key: "{}") {{ options }} }}"#, key);
    let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("store").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("options").unwrap();
    let res = res.as_scalar_value::<String>().unwrap();
    let decoded = base64::decode(&res)?;
    let json = std::str::from_utf8(&decoded)?;
    assert!(json.contains("/totally/different"));

    // delete the store configuration
    let mut vars = Variables::new();
    vars.insert("key".to_owned(), InputValue::scalar(key.to_owned()));
    let (res, _errors) = juniper::execute(
        r#"mutation DeleteStore($key: String!) {
            deleteStore(key: $key) {
                key
            }
        }"#,
        Some("DeleteStore"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("deleteStore").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("key").unwrap();
    let key = res.as_scalar_value::<String>().unwrap();
    assert!(key.starts_with("store/local/"));

    // delete the store configuration again, should error
    // ... the store is generated on demand, so this will never error

    Ok(())
}

#[test]
fn test_dataset_access() -> Result<(), Error> {
    let db_path = DBPath::new("_test_dataset_access");
    let ctx = Database::new(&db_path).unwrap();
    let schema = create_schema();

    // make sure there are no datasets in the database
    let (res, _errors) = juniper::execute(
        r#"query { datasets { key } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("datasets").unwrap();
    let res = res.as_list_value().unwrap();
    assert!(res.is_empty());

    // query for a dataset that does not exist, should return null
    let (res, _errors) = juniper::execute(
        r#"query { dataset(key: "dataset/foobar") { basepath } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("dataset").unwrap();
    assert!(res.is_null());

    // define a dataset without any stores, should fail
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: "/path".to_owned(),
        schedule: None,
        // workspace: "/path/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec![],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    assert!(res.is_null());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].error().message().contains("at least one store"));

    // define a dataset with an unknown store
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: "/path".to_owned(),
        schedule: None,
        // workspace: "/path/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec!["store/local/i_am_noman".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    assert!(res.is_null());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].error().message().contains("store does not exist"));

    // test defineDataset with non-existent basepath
    let stor = store::load_store(&ctx, "store/local/exists")?;
    store::save_store(&ctx, stor.as_ref())?;
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: "/does_not_exist".to_owned(),
        schedule: None,
        // workspace: "/does_not_exist/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec!["store/local/exists".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    assert!(res.is_null());
    assert_eq!(errors.len(), 1);
    assert!(errors[0].error().message().contains("path does not exist"));

    // dataset with an invalid schedule expression
    let cwd = std::env::current_dir()?;
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: cwd.to_str().unwrap().to_owned(),
        schedule: Some(String::from("1 2 3 2019")),
        // workspace: "/does_not_exist/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec!["store/local/exists".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    assert!(res.is_null());
    assert_eq!(errors.len(), 1);
    assert!(errors[0]
        .error()
        .message()
        .contains("schedule expression could not be parsed"));

    // finally define a valid dataset!
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: cwd.to_str().unwrap().to_owned(),
        schedule: Some(String::from("@daily")),
        // workspace: "/does_not_exist/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec!["store/local/exists".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, _errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("defineDataset").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("key").unwrap();
    let key = res.as_scalar_value::<String>().unwrap();
    assert!(key.len() > 1);

    // create a second dataset
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: None,
        basepath: cwd.to_str().unwrap().to_owned(),
        schedule: Some(String::from("@hourly")),
        // workspace: "/does_not_exist/.tmp".to_owned(),
        pack_size: BigInt::new(42),
        stores: vec!["store/local/exists".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, _errors) = juniper::execute(
        r#"mutation DefineDataset($dataset: InputDataset!) {
            defineDataset(dataset: $dataset) {
                key
            }
        }"#,
        Some("DefineDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("defineDataset").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("key").unwrap();
    let key = res.as_scalar_value::<String>().unwrap();
    assert!(key.len() > 1);

    // check that two datasets are listed
    let (res, _errors) = juniper::execute(
        r#"query { datasets { key } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("datasets").unwrap();
    let res = res.as_list_value().unwrap();
    assert_eq!(res.len(), 2);

    // test updateDataset by changing the pack_size of a dataset
    let mut vars = Variables::new();
    let input_set = InputDataset {
        key: Some(key.to_owned()),
        basepath: cwd.to_str().unwrap().to_owned(),
        schedule: Some(String::from("0 2,17,51 1-3,6,9-11 4,29 2,3,7 Wed")),
        // workspace: "/does_not_exist/.tmp".to_owned(),
        pack_size: BigInt::new(33_554_432),
        stores: vec!["store/local/exists".to_owned()],
    };
    vars.insert("dataset".to_owned(), input_set.to_input_value());
    let (res, _errors) = juniper::execute(
        r#"mutation UpdateDataset($dataset: InputDataset!) {
            updateDataset(dataset: $dataset) {
                packSize
            }
        }"#,
        Some("UpdateDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("updateDataset").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("packSize").unwrap();
    // packSize is a bigint that comes over the wire as a string
    let pack_size = res.as_scalar_value::<String>().unwrap();
    assert_eq!(pack_size, "33554432");

    // fetch dataset and make sure pack_size has been updated
    let query = format!(r#"query {{ dataset(key: "{}") {{ packSize }} }}"#, key);
    let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("dataset").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("packSize").unwrap();
    // packSize is a bigint that comes over the wire as a string
    let pack_size = res.as_scalar_value::<String>().unwrap();
    assert_eq!(pack_size, "33554432");

    // delete the dataset configuration
    let mut vars = Variables::new();
    vars.insert("key".to_owned(), InputValue::scalar(key.to_owned()));
    let (res, _errors) = juniper::execute(
        r#"mutation DeleteDataset($key: String!) {
            deleteDataset(key: $key) {
                packSize
            }
        }"#,
        Some("DeleteDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("deleteDataset").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("packSize").unwrap();
    // packSize is a bigint that comes over the wire as a string
    let pack_size = res.as_scalar_value::<String>().unwrap();
    assert_eq!(pack_size, "33554432");

    // delete the dataset configuration again, should error
    let mut vars = Variables::new();
    vars.insert("key".to_owned(), InputValue::scalar(key.to_owned()));
    let (res, errors) = juniper::execute(
        r#"mutation DeleteDataset($key: String!) {
            deleteDataset(key: $key) {
                key
            }
        }"#,
        Some("DeleteDataset"),
        &schema,
        &vars,
        &ctx,
    )
    .unwrap();
    assert!(res.is_null());
    assert_eq!(errors.len(), 1);
    assert!(errors[0]
        .error()
        .message()
        .contains("Dataset does not exist"));

    Ok(())
}

#[test]
fn test_dataset_snapshot() -> Result<(), Error> {
    let db_path = DBPath::new("_test_dataset_snapshot");
    let ctx = Database::new(&db_path).unwrap();
    let schema = create_schema();

    let unique_id = core::generate_unique_id("charlie", "localhost");
    let mut dataset = core::Dataset::new(&unique_id, Path::new("/path"), "store/local/foobar");
    let tree_sha1 = core::Checksum::SHA1("df74b5ce78c615f29e84081fc7faef4d5a9761f3".to_owned());
    let mut snapshot = core::Snapshot::new(None, tree_sha1);
    snapshot = snapshot.file_count(101);
    let snapsum = snapshot.checksum();
    dataset.latest_snapshot = Some(snapsum.clone());
    ctx.put_dataset(&dataset)?;
    ctx.insert_snapshot(&snapsum, &snapshot)?;

    let (res, _errors) = juniper::execute(
        r#"query { datasets { key latestSnapshot { checksum fileCount } } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("datasets").unwrap();
    let res = res.as_list_value().unwrap();
    assert_eq!(res.len(), 1);
    let res = res[0].as_object_value().unwrap();
    let res = res.get_field_value("latestSnapshot").unwrap();
    let snap_result = res.as_object_value().unwrap();
    let res = snap_result.get_field_value("fileCount").unwrap();
    // fileCount is a bigint that comes over the wire as a string
    let pack_size = res.as_scalar_value::<String>().unwrap();
    assert_eq!(pack_size, "101");

    let res = snap_result.get_field_value("checksum").unwrap();
    let checksum = res.as_scalar_value::<String>().unwrap();
    let query = format!(
        r#"query {{ snapshot(digest: "{}") {{ checksum fileCount }} }}"#,
        checksum
    );
    let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("snapshot").unwrap();
    let snap_result = res.as_object_value().unwrap();
    let res = snap_result.get_field_value("fileCount").unwrap();
    // fileCount is a bigint that comes over the wire as a string
    let pack_size = res.as_scalar_value::<String>().unwrap();
    assert_eq!(pack_size, "101");
    let res = snap_result.get_field_value("checksum").unwrap();
    let actual_checksum = res.as_scalar_value::<String>().unwrap();
    assert_eq!(actual_checksum, checksum);

    Ok(())
}

#[test]
fn test_config_access() -> Result<(), Error> {
    let db_path = DBPath::new("_test_config_access");
    let ctx = Database::new(&db_path).unwrap();
    let schema = create_schema();

    // assert configuration has sensible default values
    let (res, _errors) = juniper::execute(
        r#"query { configuration { computerId } }"#,
        None,
        &schema,
        &Variables::new(),
        &ctx,
    )
    .unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("configuration").unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("computerId").unwrap();
    let actual = res.as_scalar_value::<String>().unwrap();
    let username = whoami::username();
    let hostname = whoami::hostname();
    let expected = core::generate_unique_id(&username, &hostname);
    assert_eq!(actual, &expected);

    Ok(())
}

#[test]
fn test_tree_access() -> Result<(), Error> {
    let db_path = DBPath::new("_test_tree_access");
    let ctx = Database::new(&db_path).unwrap();
    let schema = create_schema();

    let tref1 = core::TreeReference::FILE(core::Checksum::SHA1("cafebabe".to_owned()));
    let entry1 = core::TreeEntry {
        name: String::from("regu"),
        fstype: core::EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: Utc.timestamp(0, 0),
        mtime: Utc.timestamp(0, 0),
        reference: tref1,
        xattrs: HashMap::new(),
    };
    let tref2 = core::TreeReference::FILE(core::Checksum::SHA1("babecafe".to_owned()));
    let entry2 = core::TreeEntry {
        name: String::from("riko"),
        fstype: core::EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: Utc.timestamp(0, 0),
        mtime: Utc.timestamp(0, 0),
        reference: tref2,
        xattrs: HashMap::new(),
    };
    let tref3 = core::TreeReference::FILE(core::Checksum::SHA1("babebabe".to_owned()));
    let entry3 = core::TreeEntry {
        name: String::from("nanachi"),
        fstype: core::EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: Utc.timestamp(0, 0),
        mtime: Utc.timestamp(0, 0),
        reference: tref3,
        xattrs: HashMap::new(),
    };
    let tree = core::Tree::new(vec![entry1, entry2, entry3], 3);
    let treesum = tree.checksum();
    let result = ctx.insert_tree(&treesum, &tree);
    assert!(result.is_ok());

    let query = format!(
        r#"query {{ tree(digest: "{}") {{ entries {{ name reference }} }} }}"#,
        treesum
    );
    let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
    let res = res.as_object_value().unwrap();
    let res = res.get_field_value("tree").unwrap();
    let res = res.as_object_value().unwrap();
    let names = ["nanachi".to_owned(), "regu".to_owned(), "riko".to_owned()];
    let refs = [
        "file-sha1-babebabe".to_owned(),
        "file-sha1-cafebabe".to_owned(),
        "file-sha1-babecafe".to_owned(),
    ];
    let res = res.get_field_value("entries").unwrap();
    let list_result = res.as_list_value().unwrap();
    for (idx, result) in list_result.iter().enumerate() {
        let object = result.as_object_value().unwrap();
        let res = object.get_field_value("name").unwrap();
        let actual = res.as_scalar_value::<String>().unwrap();
        assert_eq!(actual, &names[idx]);
        let res = object.get_field_value("reference").unwrap();
        let actual = res.as_scalar_value::<String>().unwrap();
        assert_eq!(actual, &refs[idx]);
    }

    Ok(())
}
