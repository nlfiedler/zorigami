# New Backup

## Features

* Efficiency (de-duplication, large file splitting)
* Encryption (all remotely stored data is encrypted)
* Service agnostic (Amazon, Google, Wasabi, Backblaze, B2, OneDrive, Dropbox, etc)

## Interface

* Web (Node/ReasonML)
* Desktop (Electron)
* Browse by date/time, then folders, then files (those saved at that date/time)
* Browse by folder and file, then date/time (unifying all folders/files over all backups)
* Move through snapshots within a particular tree (a la Time Machine)

## Design

### Open Questions

* How to manage the user password?
    - Web backend waits for initial setup via desktop application
    - Upon startup of desktop app, prompt user for password and configure backend
* How to handle errors when accessing local files during backup?
    - Does this even happen?
    - Fail fast and report?
* How to handle persistent file upload issues?

### Password Hashing Advice

from https://pthree.org/2018/05/23/do-not-use-sha256crypt-sha512crypt-theyre-dangerous/

For hashing passwords, in order of preference, use with an appropriate cost:

* Argon2 or scrypt (CPU and RAM hard)
* bcrypt or PBKDF2 (CPU hard only)

## Data Format

### Overview

* Snapshots record what was saved and when
* Trees describe what files/folders are in the snapshot
    - Trees are nested, a la git
* File content is stored in pack files
    - Large files are spread across multiple pack files
    - Small files are stored in whole
    - Pack files are used to minimize the number of files stored remotely
* Pack files are stored remotely
* PouchDB is used to store metadata
    - PouchDB database is saved just like any other file

### Pack Files

* contains raw file content
* default pack file size of 64MB
    - allow configuring pack file size, since it is inconsequential
    - restrict pack files to between 16 to 256 MB in size
* pack file format
    - pack starts with `P4CK`
    - version number (4 bytes)
    - number of entries (4 bytes)
    - list of entries:
        + byte length (4 bytes)
        + data
    - name is SHA256 of everything above ++ `.pack`
    - name is SHA256 of everything above ++ `.pack.gz` if compressed
* encrypted pack file format
    - header: `C4PX` (rot13 of `P4CK`)
    - version number (4 bytes)
    - HMAC-SHA256 (32 bytes)
    - master init vector (32 bytes)
    - encrypted data init vector and session key
    - encrypted pack data
* compression: use bzip2, gzip, or lzip (but _not_ xz)
    - https://www.nongnu.org/lzip/xz_inadequate.html

#### Pack Creation

1. Input: list of (file path, byte offset, byte length) tuples
    * Offset of zero and length equal to file represents whole file
1. Create `.pack` file with temporary name
1. Construct `:sha256` hasher for pack
1. Write `P4CK`, version, number of entries, to pack file and hasher
1. For each part:
    1. Write length in bytes (4 bytes) to pack and hasher
    1. Init `:sha` hasher for data
    1. Read data in chunks:
        1. Write chunk to pack
        1. Write chunk to data hasher
        1. Write chunk to pack hasher
    1. Save data hasher value to sha1/byte-offset map
        * this is saved to the pack record in PouchDB
1. Name pack file with SHA256 of pack file
1. Write pack details to record in PouchDB
1. Attempt compression of pack file
    * if it is smaller, use that instead, add `.gz` extension
1. Encrypt the pack file (see below)

### PouchDB

* config records (one per computer)
    - managed via web interface
    - key: computer UUID
    - host name
    - user name
    - latest snapshot
    - cloud service provider (e.g. `aws`, `gcp`)
    - cloud service region (e.g. `us-west1`)
    - local path to cloud service credentials file
    - time ranges in which to upload snapshots
    - size of pack files (in MB)
    - default ignore file patterns (applies to all datasets)
    - list of datasets:
        + root local path
        + ignore overrides
* encryption records (one per computer)
    - key: computer UUID
    - random salt (16 bytes)
    - random init vector (16 bytes)
    - HMAC-SHA256 of user password and salt
    - encrypted master keys (x2)
* snapshot records
    - key: SHA1 of snapshot
    - SHA1 of previous snapshot (absent if first snapshot)
    - date/time of snapshot
    - list of root tree entries (sorted by path)
        + base local path
        + tree SHA1
    - deleted (present if this snapshot is marked for removal)
* tree records
    - key: SHA1 of tree data
    - list of entries (sorted by name):
        + mode (also indicates if tree or file, e.g. `drwxr-xr-x`)
        + uid, gid
        + ctime, mtime
        + xattrs
        + checksum (SHA1 for tree, SHA256 for file)
        + entry name
    - unreachable (present if this tree is not reachable)
* file records
    - key: SHA256 of whole file
    - list of parts
        + remote bucket name
        + pack SHA256
        + part SHA1 (`null` if whole file)
        + large files will have all chunks listed every time, even if only one part changed
    - unreachable (present if this file is not reachable)
* pack records
    - key: SHA256 of pack file
    - map of SHA1 to byte offset
        + SHA1 of data part
        + byte offset into pack file
    - unreachable (present if this pack is not referenced)

## Implementation

* Node.js, ReasonML, Electron
* PouchDB
* Google Cloud Storage
    - https://github.com/googleapis/nodejs-storage/
* Amazon Glacier
    - https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/welcome.html

### Procedure

#### Computer UUID

1. Use type 5, with URL namespace
1. The "name" is the computer host name and current user name, separated by slash
    * e.g. `chihiro/nfiedler` yields `f6ce9ef2-3059-5f8d-9f3b-7d532fe15bf8`
    * makes finding the backup records for a computer reproducible

#### Bucket Name

* bucket name will be ULID + computer UUID (without dash separators)
    - e.g. `01arz3ndektsv4rrffq69g5favf6ce9ef230595f8d9f3b7d532fe15bf8`
    - fits within Google's bucket name restrictions
    - should be sufficiently unique despite global bucket namespace
* ULID contains the time, so no need for a timestamp
* UUID makes it easy to find buckets associated with this computer and user

#### Tree SHA1 computation

1. Sort the entries by name
1. Initialize the hash (`:crypto.hash_init/1`)
1. Add data from each field of entry to the hash (`:crypto.hash_update/2`)
1. Finalize the checksum (`:crypto.hash_final/1`)

#### Snapshot SHA1 computation

1. Sort the dataset entries by name
1. Initialize the hash (`:crypto.hash_init/1`)
1. Add data from each field to the hash (`:crypto.hash_update/2`)
1. Finalize the checksum (`:crypto.hash_final/1`)

#### Finding Changes

1. variable: snapshot path
1. variable: local path
1. Walk both trees, starting at root
1. For each tree:
    1. directory/file: if snapshot but not local, local was removed
    1. directory/file: if not snapshot but local, local was added
    1. file: if SHA256 differs, file changed
    1. Descend into common directories
1. Somehow compute new tree SHA1, then recompute all parents

#### Building Pack Files

1. Collect all changed files for a given snapshot
1. Consider using merkle tree of "bytes" to save only changed parts of large files
    - see perkeep.org design/code for an example of dealing with large files
1. Split large files into parts smaller than the configured pack size
1. Use bin packing to spread the files/parts into N pack files, minimizing N
1. Assemble each pack file using the N lists of files/parts

#### Backup

* Strategy for recovering from process crash
    - Build list of changes (trees, files)
    - Store trees in PouchDB immediately
    - Store a "pending" snapshot in PouchDB
    - Store file list in mnesia
    - As pack files are successfully uploaded, update the files list in mnesia
    - Upon process restart, check for pending "snapshot"
        + If present
            * Read files list from mnesia
            * Continue creating and uploading pack files
        + If missing
            * Start over, building changed trees/files list
    - Upon completion, remove the "pending" from snapshot record

1. Scan directory tree looking for files that have not already been saved
    * i.e. their checksum is not in the database
    * only directories and regular files are considered
    * mode, ownership changes are ignored (for now)
1. Create bucket/vault if there are changes to save
1. For each file, handle splitting and packing
    * use a rolling hash function to find changed parts
        - see https://github.com/lemire/rollinghashjava
        - Rabin-Karp slicing seems the most useful
        - need to understand how this works for sending only the changed parts (a la `rsync`)
1. Upload objects/archives
    * Update database for each affected file after each successful upload
1. Upload updated PouchDB database
    * split the file when it gets larger than a typical pack file
    * use the same large file rolling checksum, splitting technique to conserve space
        - how to find the other pieces of the database across old backups?
            + like mercurial; scan backward through time, with periodic full snapshots
1. For Amazon, probably need to store vault ID in S3 with date/time
    * otherwise full recovery is difficult if we can't find the latest snapshot

#### Restore

1. Based on selection, you have the list of SHA256 values of the target file.
1. Use the SHA256 to find the bucket/object to be retrieved.
1. When opening pack file, verify SHA256 of file matches pack file name.
1. Look up the SHA256 in the pack record to get byte offset, length.
1. Read data from pack file at given offset and length.
1. Write the chunk of data to a temporary file.
1. Repeat this procedure for each SHA256 value.
1. When finished, close the file and rename to target filename.

#### Full Recovery

1. Retrieve the most recent PouchDB database
1. Iterate entries in database, fetching packs and extracting files

#### Pruning

* Start by marking the snapshot records as "deleted"
* Use of a bloom filter is probably helpful
* Modest garbage collection:
    - Find trees that are no longer referenced
    - Find pack files that are no longer referenced
    - Remove the pack files from the remote side
    - Remove unreachable tree records
* Aggressive garbage collection
    - Retrieve pack files, remove stale entries, repack, upload
    - Update file records affected by repacking

#### Encryption

* Need to prompt the user for their password when starting up
    - Start a process for each user, holding the master keys in process state
    - Eventually can add code that uses macOS KeyChain, or the like

**Generating Encryption Data**

1. Generate a random 16 byte salt, to be saved in PouchDB.
1. Generate a random 16 byte initialization vector (IV), to be saved in PouchDB.
1. Generate 2 random 32-byte "master keys".
1. Derive encryption key from user provided password and the salt.
    - see https://github.com/riverrun/pbkdf2_elixir for an example
    - use no less than 100,000 rounds
1. Encrypt the master keys with AES/CBC using the the derived key and the IV.
1. Calculate the HMAC-SHA256 of (IV + encrypted master keys) using the derived key.
1. Store everything in the PouchDB encryption record.

**Extracting Master Keys**

1. Retrieve salt from the encryption record.
1. Derive encryption key from user-supplied password using PBKDF2 and the salt.
1. Calculate HMAC-SHA256 of (IV + encrypted master keys) the key.
1. Verify computed HMAC against HMAC-SHA256 in the encryption record.
1. Decrypt the encrypted master keys using the derived key.

**Encrypting Pack Files**

1. Generate a random 16 byte session key (used during a single backup)
1. Generate a random "data IV"
1. Encrypt pack data with AES/CTR using session key and data IV
1. Generate a random "master IV"
1. Encrypt (data IV + session key) with AES/CBC using the first master key from PouchDB and the "master IV"
1. Calculate HMAC-SHA256 of (master IV + "encrypted data IV + session key" + ciphertext) using the second "master key" from PouchDB
1. Write as described in the pack file data format

**Decrypting Pack Files**

1. Calculate HMAC-SHA256 of (master IV + "encrypted data IV + session key" + ciphertext) using the second "master key" from PouchDB
1. Ensure the calculated HMAC-SHA256 matches the value in the object header
1. Decrypt "encrypted data IV + session key" using the first "master key" from PouchDB and the "master IV"
1. Decrypt the ciphertext with AES/CTR using the session key and data IV.
