//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core;
use super::database::Database;
use base64::encode;
use failure::{err_msg, Error};
use std::fs;
use std::path::Path;
use std::time::SystemTime;
use xattr;

///
/// Take a snapshot of the directory structure at the given path. The parent, if
/// `Some`, specifies the snapshot that will be recorded as the parent of this
/// new snapshot.
///
pub fn take_snapshot(
    basepath: &Path,
    parent: Option<String>,
    dbase: &Database,
) -> Result<String, Error> {
    let start_time = SystemTime::now();
    let tree = scan_tree(basepath, dbase)?;
    let mut snap = core::Snapshot::new(parent, tree.checksum());
    snap = snap.start_time(start_time);
    snap = snap.file_count(tree.file_count);
    let sha1 = snap.checksum();
    dbase.insert_snapshot(&sha1, &snap)?;
    Ok(sha1)
}

///
/// Read the symbolic link value. This results in an error if the referenced
/// path is not a symbolic link.
///
fn read_link(path: &Path) -> Result<String, Error> {
    let value = fs::read_link(path)?;
    Ok(encode(value.to_str().unwrap()))
}

///
/// Create a `Tree` for the given path, recursively descending into child
/// directories. Any new trees found, as identified by their hash digest, will
/// be inserted into the database. The same is true for any files found, and
/// their extended attributes. The return value itself will also be added to the
/// database. The result will be that everything new will have been added as new
/// records.
///
fn scan_tree(basepath: &Path, dbase: &Database) -> Result<core::Tree, Error> {
    let mut entries: Vec<core::TreeEntry> = Vec::new();
    let mut file_count = 0;
    for entry in fs::read_dir(basepath)? {
        let entry = entry?;
        let file_type = entry.metadata()?.file_type();
        let path = entry.path();
        if file_type.is_dir() {
            let scan = scan_tree(&path, dbase)?;
            file_count += scan.file_count;
            let digest = scan.checksum();
            let ent = process_path(&path, &digest, dbase)?;
            entries.push(ent);
        } else if file_type.is_symlink() {
            let reference = read_link(&path)?;
            let ent = process_path(&path, &reference, dbase)?;
            entries.push(ent);
        } else if file_type.is_file() {
            let digest = core::checksum_file(&path)?;
            let ent = process_path(&path, &digest, dbase)?;
            entries.push(ent);
            file_count += 1;
        }
    }
    let tree = core::Tree::new(entries, file_count);
    let digest = tree.checksum();
    dbase.insert_tree(&digest, &tree)?;
    Ok(tree)
}

///
/// Create a `TreeEntry` record for this path, which may include storing
/// extended attributes in the database.
///
#[allow(dead_code)]
fn process_path(
    fullpath: &Path,
    reference: &str,
    dbase: &Database,
) -> Result<core::TreeEntry, Error> {
    let mut entry = core::TreeEntry::new(fullpath)?;
    entry = entry.reference(reference);
    entry = entry.mode(fullpath);
    entry = entry.owners(fullpath);
    if xattr::SUPPORTED_PLATFORM {
        let xattrs = xattr::list(fullpath)?;
        for name in xattrs {
            let nm = name
                .to_str()
                .ok_or_else(|| err_msg(format!("invalid UTF-8 in filename: {:?}", fullpath)))?;
            let value = xattr::get(fullpath, &name)?;
            if value.is_some() {
                let digest = core::checksum_data_sha1(value.as_ref().unwrap());
                dbase.insert_xattr(&digest, value.as_ref().unwrap())?;
                entry.xattrs.insert(nm.to_owned(), digest);
            }
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[cfg(target_family = "unix")]
    use std::os::unix::fs;
    #[cfg(target_family = "windows")]
    use std::os::windows::fs;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[test]
    fn test_read_link() -> Result<(), Error> {
        let outdir = tempdir()?;
        let link = outdir.path().join("mylink");
        let target = "link_target_is_meaningless";
        // cfg! macro doesn't work for this case it seems so we have this
        // redundant use of the cfg directive instead
        #[cfg(target_family = "unix")]
        fs::symlink(&target, &link)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &link)?;
        let actual = read_link(&link)?;
        assert_eq!(actual, "bGlua190YXJnZXRfaXNfbWVhbmluZ2xlc3M=");
        Ok(())
    }

    #[test]
    fn test_checksum_tree() {
        let entry1 = core::TreeEntry {
            name: String::from("madoka.kaname"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: Some(String::from("sha1-cafebabe")),
            xattrs: HashMap::new(),
        };
        let entry2 = core::TreeEntry {
            name: String::from("homura.akemi"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: Some(String::from("sha1-babecafe")),
            xattrs: HashMap::new(),
        };
        let entry3 = core::TreeEntry {
            name: String::from("sayaka.miki"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: Some(String::from("sha1-babebabe")),
            xattrs: HashMap::new(),
        };
        let tree = core::Tree::new(vec![entry1, entry2, entry3], 2);
        // would look something like this, if we used "now" instead of unix epoch
        // 644 100:100 1552877320 1552877320 sha1-babecafe homura.akemi
        // 644 100:100 1552877320 1552877320 sha1-cafebabe madoka.kaname
        // 644 100:100 1552877320 1552877320 sha1-babebabe sayaka.miki
        let result = format!("{}", tree);
        // results should be sorted lexicographically by filename
        assert!(result.find("homura").unwrap() < result.find("madoka").unwrap());
        assert!(result.find("madoka").unwrap() < result.find("sayaka").unwrap());
        let sum = tree.checksum();
        // because the timestamps are always 0, sha1 is always the same
        assert_eq!(sum, "sha1-5eca657fc20877aa2e75b90b902c55ee69a95139");
    }
}
