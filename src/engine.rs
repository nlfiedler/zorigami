//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core;
use base64::encode;
use failure::Error;
use std::fs;
use std::path::Path;

#[allow(dead_code)]
fn read_link(path: &Path) -> Result<String, Error> {
    let value = fs::read_link(path)?;
    Ok(encode(value.to_str().unwrap()))
}

///
/// Calculate the checksum for the set of tree entries. They will first be
/// sorted (in a separate vector) and then formatted, and the result used to
/// compute the SHA1 digest.
///
pub fn checksum_tree(tree: &[core::TreeEntry]) -> String {
    let formed = format_tree(tree);
    core::checksum_data_sha1(formed.as_bytes())
}

#[allow(dead_code)]
fn format_tree(tree: &[core::TreeEntry]) -> String {
    let mut buf = String::new();
    let mut sorted: Vec<&core::TreeEntry> = Vec::new();
    for e in tree {
        sorted.push(&e)
    }
    sorted.sort_unstable_by_key(|e| &e.name);
    for entry in sorted {
        let formed = format!("{}\n", entry);
        buf.push_str(&formed);
    }
    buf
}

#[allow(dead_code)]
fn process_path(fullpath: &Path, reference: &str) -> Result<core::TreeEntry, Error> {
    let mut entry = core::TreeEntry::new(fullpath)?;
    entry = entry.reference(reference);
    //   const attrs: string[] = await xlist(fullpath)
    //   if (attrs) {
    //     const xattrs: ExtAttr[] = []
    //     for (let name of attrs) {
    //       const value = await xget(fullpath, name)
    //       const hash = core.checksumData(value, 'sha1')
    //       await database.insertExtAttr(hash, value)
    //       xattrs.push({ name, digest: hash })
    //     }
    //     doc.xattrs = xattrs
    //   }
    //   return doc
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        };
        let tree = vec![entry1, entry2, entry3];
        // would look something like this, if we used "now" instead of unix epoch
        // 644 100:100 1552877320 1552877320 sha1-babecafe homura.akemi
        // 644 100:100 1552877320 1552877320 sha1-cafebabe madoka.kaname
        // 644 100:100 1552877320 1552877320 sha1-babebabe sayaka.miki
        let result = format_tree(&tree);
        // results should be sorted lexicographically by filename
        assert!(result.find("homura").unwrap() < result.find("madoka").unwrap());
        assert!(result.find("madoka").unwrap() < result.find("sayaka").unwrap());
        let sum = checksum_tree(&tree);
        // because the timestamps are always 0, sha1 is always the same
        assert_eq!(sum, "sha1-5eca657fc20877aa2e75b90b902c55ee69a95139");
    }
}
