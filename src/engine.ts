//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as util from 'util'
const posix = require('posix-ext')
const xattr = require('fs-xattr')
import * as core from './core'
import * as database from './database'

const freaddir = util.promisify(fs.readdir)
const flstat = util.promisify(fs.lstat)
const xlist = util.promisify(xattr.list)
const xget = util.promisify(xattr.get)
export const NULL_SHA1 = 'sha1-0000000000000000000000000000000000000000'

/**
 * Get the master keys for encrypting the pack files. They will be loaded from
 * the database, or generated if they are missing.
 *
 * @param password user master password.
 * @returns the master keys.
 */
export async function getMasterKeys(password: string): Promise<core.MasterKeys> {
  let encryptDoc = await database.fetchDocument('encryption')
  let keys = null
  if (encryptDoc === null) {
    keys = core.generateMasterKeys()
    const data = core.newMasterEncryptionData(password, keys)
    encryptDoc = {
      _id: 'encryption',
      salt: data.salt,
      iv: data.iv,
      hmac: data.hmac,
      keys: data.encrypted
    }
    await database.updateDocument(encryptDoc)
  } else {
    const data = {
      salt: encryptDoc.salt,
      iv: encryptDoc.iv,
      hmac: encryptDoc.hmac,
      encrypted: encryptDoc.keys
    }
    keys = core.decryptMasterKeys(data, password)
  }
  return keys
}

/**
 * Create a snapshot record for the given directory tree.
 *
 * @param basepath directory from which to begin scanning.
 * @param parent checksum of the previous snapshot, if known.
 * @returns checksum of the new snapshot.
 */
export async function takeSnapshot(basepath: string, parent = NULL_SHA1): Promise<string> {
  const startTime = Date.now()
  const treescan = await scantree(basepath)
  const treesha1 = checksumTree(treescan.entries)
  // the snapshot has only just begun, so end time is zero
  const endTime = 0
  // produce a deterministic checksum of the snapshot data,
  // _not_ using JSON, since order may not be honored
  const checksumData = [
    parent,
    treesha1,
    startTime.toString(),
    endTime.toString(),
    treescan.fileCount.toString()
  ]
  const checksum = core.checksumData(checksumData.join('\n'), 'sha1')
  await database.insertSnapshot(checksum, {
    parent,
    start_time: startTime,
    end_time: endTime,
    num_files: treescan.fileCount,
    tree: treesha1
  })
  return checksum
}

/**
 * Find those files that were added or changed between the two snapshots. Only
 * files are considered, as changes to directories are already recorded in the
 * database and saved separately. Ignores anything that is not a file or a
 * directory. May return files that were processed earlier, so the caller must
 * filter out files that have record entries in the database.
 *
 * @param snapshot1 earlier snapshot.
 * @param snapshot2 later snapshot.
 * @returns map of file paths to sha256 checksum from the tree entry.
 */
export async function findChangedFiles(
  snapshot1: string,
  snapshot2: string
): Promise<Map<string, string>> {
  const changed: Map<string, string> = new Map()
  // queue entry is (full path, tree1 sha1, tree2 sha1)
  let entry: [string, string, string]
  // use a queue to perform a breadth-first traversal
  const queue: typeof entry[] = []
  // to start, add tree for each snapshot to the queue
  const snap1doc = await database.getSnapshot(snapshot1)
  const snap2doc = await database.getSnapshot(snapshot2)
  queue.push(['.', snap1doc.tree, snap2doc.tree])
  while (queue.length) {
    const entry = queue.shift()
    const tree1 = await database.getTree(entry[1])
    const entries1: TreeEntry[] = tree1.entries
    const tree2 = await database.getTree(entry[2])
    const entries2: TreeEntry[] = tree2.entries
    sortTreeEntryByName(entries1)
    sortTreeEntryByName(entries2)
    // walk through the lists in sorted, merged order
    let index1 = 0
    let index2 = 0
    while (index1 < entries1.length && index2 < entries2.length) {
      const entry1 = entries1[index1]
      const entry2 = entries2[index2]
      if (entry1.name < entry2.name) {
        // file or directory has been removed, nothing to do
        index1++
      } else if (entry1.name > entry2.name) {
        // file or directory has been added
        if (modeToType(entry2.mode) === FileType.DIR) {
          // tree: add every file under it to 'added'
          await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
        } else {
          changed.set(path.join(entry[0], entry2.name), entry2.reference)
        }
        index2++
      } else if (entry1.reference !== entry2.reference) {
        // they have the same name but differ somehow
        const is1dir: boolean = modeToType(entry1.mode) === FileType.DIR
        const is1file: boolean = modeToType(entry1.mode) === FileType.REG
        const is1link: boolean = modeToType(entry1.mode) === FileType.LNK
        const is2dir: boolean = modeToType(entry2.mode) === FileType.DIR
        const is2file: boolean = modeToType(entry2.mode) === FileType.REG
        if (is1dir && is2dir) {
          // tree A & B: add both trees to the queue
          const dirpath = path.join(entry[0], entry1.name)
          queue.push([dirpath, entry1.reference, entry2.reference])
        } else if ((is1file || is1dir || is1link) && is2file) {
          // new file or a changed file
          changed.set(path.join(entry[0], entry2.name), entry2.reference)
        } else if ((is1file || is1link) && is2dir) {
          // now a directory, add everything under it
          await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
        }
        // ignore everything else
        index1++
        index2++
      } else {
        // they are the same
        index1++
        index2++
      }
    }
    // catch everything else in the new snapshot
    while (index2 < entries2.length) {
      const entry2 = entries2[index2]
      // file or directory has been added
      if (modeToType(entry2.mode) === FileType.DIR) {
        // tree: add every file under it to 'added'
        await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
      } else if (modeToType(entry2.mode) === FileType.REG) {
        changed.set(path.join(entry[0], entry2.name), entry2.reference)
      }
      index2++
    }
  }
  return changed
}

/**
 * For the given tree, add every file therein to the `changed` map.
 *
 * @param changed map to which new or changed files are added.
 * @param basepath path prefix for files found under the tree.
 * @param ref database document identifier of the tree record.
 */
async function addAllFilesUnder(changed: Map<string, string>, basepath: string, ref: string) {
  const tree = await database.getTree(ref)
  const entries: TreeEntry[] = tree.entries
  for (let entry of entries) {
    if (modeToType(entry.mode) === FileType.DIR) {
      await addAllFilesUnder(changed, path.join(basepath, entry.name), entry.reference)
    } else if (modeToType(entry.mode) === FileType.REG) {
      changed.set(path.join(basepath, entry.name), entry.reference)
    }
  }
}

function performBackup() {
  // TODO: take snapshot
  // TODO: find changed files
  // TODO: filter out files that we already have records for
}

/**
 * Represents the file type in a convenient form.
 */
const enum FileType {
  FIFO,
  CHR,
  DIR,
  BLK,
  REG,
  LNK,
  SOCK,
  OTHER // did not match any known constant
}

/**
 * Convert the file mode to the `FileType` enum.
 *
 * @param mode file mode as from `stat`.
 * @returns one of the `FileType` values.
 */
function modeToType(mode: number): FileType {
  switch (mode & fs.constants.S_IFMT) {
    case fs.constants.S_IFIFO:
      return FileType.FIFO
    case fs.constants.S_IFCHR:
      return FileType.CHR
    case fs.constants.S_IFDIR:
      return FileType.DIR
    case fs.constants.S_IFBLK:
      return FileType.BLK
    case fs.constants.S_IFREG:
      return FileType.REG
    case fs.constants.S_IFLNK:
      return FileType.LNK
    case fs.constants.S_IFSOCK:
      return FileType.SOCK
    default:
      return FileType.OTHER
  }
}

/**
 * Represents an extended attribute.
 */
interface ExtAttr {
  /** name of the extended attribute */
  name: string,
  /** hash digest of the attribute value */
  hash: string
}

/**
 * Represents a file or directory within a tree object.
 */
export interface TreeEntry {
  /** file or directory name */
  name: string,
  mode: number,
  uid: number,
  user: string,
  gid: number,
  group: string,
  ctime: number,
  mtime: number,
  /** SHA1 for tree, SHA256 for file, base64 encoded link value for symlinks */
  reference: string,
  xattrs?: ExtAttr[]
}

/**
 * Sorts the array of `TreeEntry` objects in place.
 *
 * @param entries array of `TreeEntry` objects to sort.
 */
function sortTreeEntryByName(entries: TreeEntry[]): void {
  entries.sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * Result of calling `scantree()` function.
 */
interface TreeScan {
  entries: TreeEntry[]
  fileCount: number
}

/**
 * Produce tree records for everything under `basepath`. Records are inserted
 * for each (new) tree found along the way.
 *
 * @param basepath directory from which to begin scanning.
 * @returns the result of scanning this directory.
 */
async function scantree(basepath: string): Promise<TreeScan> {
  const dirents = await freaddir(basepath, { withFileTypes: true })
  const entries: TreeEntry[] = []
  let fileCount = 0
  for (let entry of dirents) {
    if (entry.isDirectory()) {
      const fullpath = path.join(basepath, entry.name)
      const scan = await scantree(fullpath)
      fileCount += scan.fileCount
      const checksum = checksumTree(scan.entries)
      entries.push(await processPath(entry.name, fullpath, checksum))
    }
  }
  for (let entry of dirents) {
    if (entry.isSymbolicLink()) {
      const fullpath = path.join(basepath, entry.name)
      const reference = readlink(fullpath)
      entries.push(await processPath(entry.name, fullpath, reference))
    } else if (entry.isFile()) {
      const fullpath = path.join(basepath, entry.name)
      const checksum = await core.checksumFile(fullpath, 'sha256')
      entries.push(await processPath(entry.name, fullpath, checksum))
      fileCount++
    }
  }
  await insertTree(entries)
  return {
    entries,
    fileCount
  }
}

/**
 * Create a `TreeEntry` record for this path, which may include storing extended
 * attributes in the database.
 *
 * @param basename entry name being processed.
 * @param fullpath full path of the entry.
 * @param reference reference (sha1, sha256, symlink) for the entry.
 * @returns object representing this path.
 */
async function processPath(basename: string, fullpath: string, reference: string): Promise<TreeEntry> {
  const stat: fs.Stats = await flstat(fullpath)
  const user = posix.getpwuid(stat.uid)
  const group = posix.getgrgid(stat.gid)
  const doc: TreeEntry = {
    name: basename,
    mode: stat.mode,
    uid: stat.uid,
    user: user.name,
    gid: stat.gid,
    group: group.name,
    ctime: stat.ctimeMs,
    mtime: stat.mtimeMs,
    reference: reference
  }
  const attrs: string[] = await xlist(fullpath)
  if (attrs) {
    const xattrs: ExtAttr[] = []
    for (let name of attrs) {
      const value = await xget(fullpath, name)
      const hash = core.checksumData(value, 'sha1')
      await database.insertExtAttr(hash, value)
      xattrs.push({name, hash})
    }
    doc.xattrs = xattrs
  }
  return doc
}

/**
 * Read the symbolic link value as base64 encoded bytes.
 *
 * @param path full path to the symbolic link.
 * @returns base64 encoded value of the link.
 */
function readlink(path: string): string {
  const buf = fs.readlinkSync(path, { encoding: 'buffer' })
  return buf.toString('base64')
}

/**
 * Produce a checksum for the given tree scan results.
 *
 * @param tree list of entries that make up a single tree record.
 * @returns hash digest of the tree, with algorithm prefix.
 */
function checksumTree(tree: TreeEntry[]): string {
  return core.checksumData(formatTree(tree), 'sha1')
}

/**
 * Produce and insert a record for the tree consisting of the given entries.
 *
 * @param tree list of entries that make up a single tree record.
 */
async function insertTree(tree: TreeEntry[]): Promise<void> {
  const checksum = checksumTree(tree)
  return database.insertTree(checksum, { entries: tree })
}

/**
 * Formats the tree entries in a consistent fashion, suitable for producing a
 * repeatable checksum of the tree.
 *
 * @param entries entries within the tree to be printed.
 * @return formatted tree.
 */
function formatTree(entries: TreeEntry[]): string {
  sortTreeEntryByName(entries)
  const formed = entries.map(e => {
    return `${e.mode} ${e.uid}:${e.gid} ${e.ctime} ${e.mtime} ${e.reference} ${e.name}`
  })
  return formed.join('\n')
}
