//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as util from 'util'
import * as core from './core'
import * as database from './database'

const freaddir = util.promisify(fs.readdir)
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
  queue.push(['.', await getSnapshotTree(snapshot1), await getSnapshotTree(snapshot2)])
  while (queue.length) {
    const entry = queue.shift()
    const tree1 = await database.getTree(entry[1])
    const entries1: TreeEntry[] = tree1.entries
    const tree2 = await database.getTree(entry[2])
    const entries2: TreeEntry[] = tree2.entries
    entries1.sort((a, b) => a.name.localeCompare(b.name))
    entries2.sort((a, b) => a.name.localeCompare(b.name))
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
        if (entry2.mode & fs.constants.S_IFDIR) {
          // tree: add every file under it to 'added'
          await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
        } else {
          changed.set(path.join(entry[0], entry2.name), entry2.reference)
        }
        index2++
      } else if (entry1.reference !== entry2.reference) {
        // they have the same name but differ somehow
        const is1dir = entry1.mode & fs.constants.S_IFDIR
        const is1file = entry1.mode & fs.constants.S_IFREG
        const is2dir = entry2.mode & fs.constants.S_IFDIR
        const is2file = entry2.mode & fs.constants.S_IFREG
        if (is1dir && is2dir) {
          // tree A & B: add both trees to the queue
          const dirpath = path.join(entry[0], entry1.name)
          queue.push([dirpath, entry1.reference, entry2.reference])
        } else if (is1file && is2file || is1dir && is2file) {
          // new file or a changed file
          changed.set(path.join(entry[0], entry2.name), entry2.reference)
        } else if (is1file && is2dir) {
          // now a directory, add everything under it
          await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
        }
        // some other thing (e.g. character device), ignore it for now
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
      if (entry2.mode & fs.constants.S_IFDIR) {
        // tree: add every file under it to 'added'
        await addAllFilesUnder(changed, path.join(entry[0], entry2.name), entry2.reference)
      } else {
        changed.set(path.join(entry[0], entry2.name), entry2.reference)
      }
      index2++
    }
  }
  return changed
}

async function addAllFilesUnder(changed: Map<string, string>, basepath: string, ref: string) {
  const tree = await database.getTree(ref)
  const entries: TreeEntry[] = tree.entries
  for (let entry of entries) {
    if (entry.mode & fs.constants.S_IFDIR) {
      await addAllFilesUnder(changed, path.join(basepath, entry.name), entry.reference)
    } else if (entry.mode & fs.constants.S_IFREG) {
      changed.set(path.join(basepath, entry.name), entry.reference)
    }
  }
}

function performBackup() {
  // TODO: take snapshot
  // TODO: find changed files
  // TODO: filter out files that we already have records for
}

async function getSnapshotTree(snapshot: string): Promise<string> {
  const doc = await database.getSnapshot(snapshot)
  return doc.tree
}

/**
 * Represents a file or directory within a tree object.
 */
interface TreeEntry {
  /** file or directory name */
  name: string,
  mode: number,
  uid: number,
  gid: number,
  ctime: number,
  mtime: number,
  /** SHA1 for tree, SHA256 for file */
  reference: string
}

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
    if (entry.isFile()) {
      const fullpath = path.join(basepath, entry.name)
      const checksum = await core.checksumFile(fullpath, 'sha256')
      entries.push(processPath(entry.name, fullpath, checksum))
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
 * Create a `TreeEntry` record for this path.
 *
 * @param basename entry name being processed.
 * @param fullpath full path of the entry.
 * @param checksum checksum for the entry.
 * @returns object representing this path.
 */
function processPath(basename: string, fullpath: string, checksum: string): TreeEntry {
  const stat = fs.statSync(fullpath)
  return {
    name: basename,
    mode: stat.mode,
    uid: stat.uid,
    gid: stat.gid,
    ctime: stat.ctimeMs,
    mtime: stat.mtimeMs,
    reference: checksum
  }
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
  entries.sort((a, b) => a.name.localeCompare(b.name))
  const formed = entries.map(e => {
    return `${e.mode} ${e.uid}:${e.gid} ${e.ctime} ${e.mtime} ${e.reference} ${e.name}`
  })
  return formed.join('\n')
}
