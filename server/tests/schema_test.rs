//
// Copyright (c) 2020 Nathan Fiedler
//
// mod util;

// use chrono::{TimeZone, Utc};
// use failure::Error;
// use juniper::{InputValue, ToInputValue, Variables};
// use std::collections::HashMap;
// use std::path::Path;
// use util::DBPath;
// use zorigami::core;
// use zorigami::database::*;
// use zorigami::schema::*;
// use zorigami::store;

// #[test]
// fn test_dataset_snapshot() -> Result<(), Error> {
//     let db_path = DBPath::new("_test_dataset_snapshot");
//     let ctx = Database::new(&db_path).unwrap();
//     let schema = create_schema();

//     let unique_id = core::generate_unique_id("charlie", "localhost");
//     let mut dataset = core::Dataset::new(&unique_id, Path::new("/path"), "store/local/foobar");
//     let tree_sha1 = core::Checksum::SHA1("df74b5ce78c615f29e84081fc7faef4d5a9761f3".to_owned());
//     let snapshot = core::Snapshot::new(None, tree_sha1, 101);
//     let snapsum = snapshot.digest.clone();
//     dataset.latest_snapshot = Some(snapsum.clone());
//     ctx.put_dataset(&dataset)?;
//     ctx.insert_snapshot(&snapsum, &snapshot)?;

//     let (res, _errors) = juniper::execute(
//         r#"query { datasets { key latestSnapshot { checksum fileCount } } }"#,
//         None,
//         &schema,
//         &Variables::new(),
//         &ctx,
//     )
//     .unwrap();
//     let res = res.as_object_value().unwrap();
//     let res = res.get_field_value("datasets").unwrap();
//     let res = res.as_list_value().unwrap();
//     assert_eq!(res.len(), 1);
//     let res = res[0].as_object_value().unwrap();
//     let res = res.get_field_value("latestSnapshot").unwrap();
//     let snap_result = res.as_object_value().unwrap();
//     let res = snap_result.get_field_value("fileCount").unwrap();
//     // fileCount is a bigint that comes over the wire as a string
//     let pack_size = res.as_scalar_value::<String>().unwrap();
//     assert_eq!(pack_size, "101");

//     let res = snap_result.get_field_value("checksum").unwrap();
//     let checksum = res.as_scalar_value::<String>().unwrap();
//     let query = format!(
//         r#"query {{ snapshot(digest: "{}") {{ checksum fileCount }} }}"#,
//         checksum
//     );
//     let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
//     let res = res.as_object_value().unwrap();
//     let res = res.get_field_value("snapshot").unwrap();
//     let snap_result = res.as_object_value().unwrap();
//     let res = snap_result.get_field_value("fileCount").unwrap();
//     // fileCount is a bigint that comes over the wire as a string
//     let pack_size = res.as_scalar_value::<String>().unwrap();
//     assert_eq!(pack_size, "101");
//     let res = snap_result.get_field_value("checksum").unwrap();
//     let actual_checksum = res.as_scalar_value::<String>().unwrap();
//     assert_eq!(actual_checksum, checksum);

//     Ok(())
// }

// #[test]
// fn test_tree_access() -> Result<(), Error> {
//     let db_path = DBPath::new("_test_tree_access");
//     let ctx = Database::new(&db_path).unwrap();
//     let schema = create_schema();

//     let tref1 = core::TreeReference::FILE(core::Checksum::SHA1("cafebabe".to_owned()));
//     let entry1 = core::TreeEntry {
//         name: String::from("regu"),
//         fstype: core::EntryType::FILE,
//         mode: Some(0o644),
//         uid: Some(100),
//         gid: Some(100),
//         user: Some(String::from("user")),
//         group: Some(String::from("group")),
//         ctime: Utc.timestamp(0, 0),
//         mtime: Utc.timestamp(0, 0),
//         reference: tref1,
//         xattrs: HashMap::new(),
//     };
//     let tref2 = core::TreeReference::FILE(core::Checksum::SHA1("babecafe".to_owned()));
//     let entry2 = core::TreeEntry {
//         name: String::from("riko"),
//         fstype: core::EntryType::FILE,
//         mode: Some(0o644),
//         uid: Some(100),
//         gid: Some(100),
//         user: Some(String::from("user")),
//         group: Some(String::from("group")),
//         ctime: Utc.timestamp(0, 0),
//         mtime: Utc.timestamp(0, 0),
//         reference: tref2,
//         xattrs: HashMap::new(),
//     };
//     let tref3 = core::TreeReference::FILE(core::Checksum::SHA1("babebabe".to_owned()));
//     let entry3 = core::TreeEntry {
//         name: String::from("nanachi"),
//         fstype: core::EntryType::FILE,
//         mode: Some(0o644),
//         uid: Some(100),
//         gid: Some(100),
//         user: Some(String::from("user")),
//         group: Some(String::from("group")),
//         ctime: Utc.timestamp(0, 0),
//         mtime: Utc.timestamp(0, 0),
//         reference: tref3,
//         xattrs: HashMap::new(),
//     };
//     let tree = core::Tree::new(vec![entry1, entry2, entry3], 3);
//     let treesum = tree.checksum();
//     let result = ctx.insert_tree(&treesum, &tree);
//     assert!(result.is_ok());

//     let query = format!(
//         r#"query {{ tree(digest: "{}") {{ entries {{ name reference }} }} }}"#,
//         treesum
//     );
//     let (res, _errors) = juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
//     let res = res.as_object_value().unwrap();
//     let res = res.get_field_value("tree").unwrap();
//     let res = res.as_object_value().unwrap();
//     let names = ["nanachi".to_owned(), "regu".to_owned(), "riko".to_owned()];
//     let refs = [
//         "file-sha1-babebabe".to_owned(),
//         "file-sha1-cafebabe".to_owned(),
//         "file-sha1-babecafe".to_owned(),
//     ];
//     let res = res.get_field_value("entries").unwrap();
//     let list_result = res.as_list_value().unwrap();
//     for (idx, result) in list_result.iter().enumerate() {
//         let object = result.as_object_value().unwrap();
//         let res = object.get_field_value("name").unwrap();
//         let actual = res.as_scalar_value::<String>().unwrap();
//         assert_eq!(actual, &names[idx]);
//         let res = object.get_field_value("reference").unwrap();
//         let actual = res.as_scalar_value::<String>().unwrap();
//         assert_eq!(actual, &refs[idx]);
//     }

//     Ok(())
// }
