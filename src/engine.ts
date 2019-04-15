//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as util from 'util'
import * as config from 'config'
import * as fx from 'fs-extra'
const posix = require('posix-ext')
const xattr = require('fs-xattr')
import * as tmp from 'tmp'
import * as core from './core'
import * as database from './database'
import * as store from './store'

const freaddir = util.promisify(fs.readdir)
const flstat = util.promisify(fs.lstat)
const funlink = util.promisify(fs.unlink)
const xlist = util.promisify(xattr.list)
const xget = util.promisify(xattr.get)
export const NULL_SHA1 = 'sha1-0000000000000000000000000000000000000000'
// Chunks will be sized relative to pack files, up to this maximum.
const MAX_CHUNK_SIZE = 4194304

const KEY_BITS: number = config.get('encryption.bits')

/**
 * Get the master keys for encrypting the pack files. They will be loaded from
 * the database, or generated if they are missing.
 *
 * @param userid identifier for the user (added to keys).
 * @param passphrase user pass phrase to (un)lock private key.
 * @returns the master keys.
 */
export async function getMasterKeys(userid: string, passphrase: string): Promise<core.EncryptionKeys> {
  let encryptDoc = await database.fetchDocument('encryption')
  let keys = null
  if (encryptDoc === null) {
    keys = await core.generateEncryptionKeys(userid, passphrase, KEY_BITS)
    encryptDoc = {
      _id: 'encryption',
      public: keys.publicKey,
      private: keys.privateKey
    }
    await database.updateDocument(encryptDoc)
  } else {
    keys = {
      publicKey: encryptDoc.public,
      privateKey: encryptDoc.private
    }
  }
  return keys
}

interface Dataset {
  /** The computer UUID for generating bucket names. */
  uniqueId: string
  /** local base path of dataset to be saved */
  basepath: string
  /**
   * latest snapshot reference; `NULL_SHA1` if not set
   */
  latest: string
  /** path for temporary pack building */
  workspace: string
  // + schedule/frequency overrides
  // + ignore overrides
  /** Target size in bytes for pack files. */
  packSize: number
  // + storage overrides (e.g. `local` vs `aws`)
  /** Name of the store to contain pack files. */
  store: string
}

/**
 * Results of building a single pack file, with information suitable
 * for updating the database.
 */
interface PackBuildResults {
  /** pack file sha256 */
  checksum: string,
  /** the chunks in this pack file */
  chunks: core.Chunk[]
}

/**
 * Keep track of files and their chunks, constructing pack files, tracking which
 * chunks are uploaded, and thereby which files are done.
 */
class PackBuilder {
  private packSize: number
  private chunks: core.Chunk[]
  private chunksSize: number
  /** tracks files and their chunks */
  private fileChunks: Map<string, core.Chunk[]>

  constructor(packSize: number) {
    this.packSize = packSize
    this.chunks = []
    this.chunksSize = 0
    this.fileChunks = new Map<string, core.Chunk[]>()
  }

  async addFile(filepath: string, checksum: string): Promise<void> {
    const stat: fs.Stats = await flstat(filepath)
    let fileChunks = null
    if (stat.size > this.packSize) {
      // Split large files into chunks, add chunks to the list; the file chunk
      // finder produces a fairly wide range of sizes, so aim for chunks being
      // about 1/4 of the pack size. Keep chunks below a reasonable size.
      let desired = Math.min(this.packSize / 4, MAX_CHUNK_SIZE)
      fileChunks = await core.findFileChunks(filepath, desired)
    } else {
      // small files are chunks all by themselves
      const hash = core.bufferFromChecksum(checksum)
      fileChunks = [{ path: filepath, hash, offset: 0, size: stat.size }]
    }
    for (let chunk of fileChunks) {
      const key = core.checksumFromBuffer(chunk.hash, 'sha256')
      const chunkrec = await database.getChunk(key)
      if (chunkrec === null) {
        this.chunks.push(chunk)
        this.chunksSize += chunk.size
      } else {
        // an identical chunk was uploaded previously
        chunk.uploaded = true
      }
    }
    // track _all_ of the chunks in this file, not just the "new" ones
    this.fileChunks.set(checksum, fileChunks)
  }

  hasChunks(): boolean {
    return this.chunks.length > 0
  }

  isFull(): boolean {
    return this.chunksSize > this.packSize
  }

  async buildPack(outfile: string, keys: core.EncryptionKeys): Promise<PackBuildResults> {
    let size = 0
    let index = 0
    while (size < this.packSize && index < this.chunks.length) {
      size += this.chunks[index].size
      index++
    }
    const outgoing = this.chunks.slice(0, index)
    const sha256 = await core.packChunksEncrypted(outgoing, outfile, keys)
    this.chunks = this.chunks.slice(index)
    this.chunksSize -= size
    return { checksum: sha256, chunks: outgoing }
  }

  extractComplete(chunks: core.Chunk[]): Map<string, core.Chunk[]> {
    // get the done chunks in a convenient map by hash with their checksum as a
    // hex string (buffers do not compare automatically)
    const doneChunks = new Map()
    for (let chunk of chunks) {
      doneChunks.set(chunk.hash.toString('hex'), chunk)
    }
    // mark chunks as being uploaded if they are in the done set
    for (let fileChunks of this.fileChunks.values()) {
      for (let chunk of fileChunks) {
        if (doneChunks.has(chunk.hash.toString('hex'))) {
          chunk.uploaded = true
        }
      }
    }
    // extract those files that have been completely uploaded
    const completed: Map<string, core.Chunk[]> = new Map()
    this.fileChunks.forEach((fileChunks, checksum, map) => {
      const allDone = fileChunks.every((e) => e.uploaded)
      if (allDone) {
        completed.set(checksum, fileChunks)
        map.delete(checksum)
      }
    })
    return completed
  }
}

/**
 * Perform a backup for the given dataset.
 *
 * @param dataset the dataset for which to perform a backup.
 * @param keys master keys for encrypting the pack.
 * @returns checksum of the new snapshot.
 */
export async function performBackup(dataset: Dataset, keys: core.EncryptionKeys): Promise<string> {
  fx.ensureDirSync(dataset.workspace)
  const snapshot = await takeSnapshot(dataset.basepath)
  const bucket = core.generateBucketName(dataset.uniqueId)
  const builder = new PackBuilder(dataset.packSize)
  const sendOnePack = async () => {
    const packfile = tmp.fileSync({ dir: dataset.workspace }).name
    const object = path.basename(packfile)
    const results = await builder.buildPack(packfile, keys)
    const remoteObject = await waitForUpload(store.storePack(dataset.store, packfile, bucket, object))
    await recordFinishedChunks(results, bucket, remoteObject)
    const files = builder.extractComplete(results.chunks)
    await recordFinishedFiles(files)
  }
  const handleFile = async (filepath: string, checksum: string) => {
    // ignore files which already have records
    const filerec = await database.getFile(checksum)
    if (filerec === null) {
      await builder.addFile(filepath, checksum)
      // loop until pack builder is below desired size
      // (adding a very large file may require multiple packs)
      while (builder.isFull()) {
        await sendOnePack()
      }
    }
  }
  // if no previous snapshot, visit every file in the new snapshot
  // otherwise, find those files that changed from the previous snapshot
  const fileGenerator = (
    dataset.latest === NULL_SHA1
      ? walkTree(snapshot)
      : findChangedFiles(dataset.latest, snapshot)
  )
  for await (let [filepath, filesha] of fileGenerator) {
    const fullpath = path.join(dataset.basepath, filepath)
    await handleFile(fullpath, filesha)
  }
  // empty the last pack file
  while (builder.hasChunks()) {
    await sendOnePack()
  }
  return snapshot
}

interface FileChunk {
  /** file position for this chunk */
  offset: number,
  /** chunk SHA256 with algo prefix */
  digest: string
}

/**
 * 
 * @param dataset the dataset for which to perform a backup.
 * @param keys master keys for decrypting pack files.
 * @param passphrase pass phrase to unlock private key.
 * @param checksum SHA256 checksum of the file to be restored.
 * @param outfile path to which file will be written.
 */
export async function restoreFile(
  dataset: Dataset,
  keys: core.EncryptionKeys,
  passphrase: string,
  checksum: string,
  outfile: string
): Promise<void> {
  // look up the file record to get chunks
  const filerec = await database.getFile(checksum)
  if (filerec === null) {
    throw new Error('missing file record for ' + checksum)
  }
  // create an index of all the chunks we want to collect
  const desiredChunks: Set<string> = new Set()
  for (let chunk of filerec.chunks) {
    desiredChunks.add(chunk.digest)
  }
  // track pack files that have already been processed
  const finishedPacks: Set<string> = new Set()
  // look up chunk records to get pack record(s)
  for (let chunk of filerec.chunks) {
    const chunkrec = await database.getChunk(chunk.digest)
    if (!finishedPacks.has(chunkrec.pack)) {
      const packrec = await database.getPack(chunkrec.pack)
      // retrieve the pack file
      const packfile = tmp.fileSync({ dir: dataset.workspace }).name
      const emitter = store.retrievePack(dataset.store, packrec.bucket, packrec.object, packfile)
      await store.waitForDone(emitter)
      // extract chunks from pack
      const extractedChunks = await core.unpackChunksEncrypted(packfile, dataset.workspace, keys, passphrase)
      // remove unrelated chunks to conserve space
      for (let ec of extractedChunks) {
        if (!desiredChunks.has(ec)) {
          await funlink(path.join(dataset.workspace, ec))
        }
      }
      await funlink(packfile)
      // remember this pack as being completed
      finishedPacks.add(chunkrec.pack)
    }
  }
  // sort the chunks by offset to produce ordered file list
  filerec.chunks.sort((a: FileChunk, b: FileChunk) => a.offset - b.offset)
  const chunkFiles: string[] = filerec.chunks.map(
    (e: FileChunk) => path.join(dataset.workspace, e.digest)
  )
  return core.assembleChunks(chunkFiles, outfile)
}

/**
 * A generator that yields [filepath, checksum] pairs for files within the given
 * snapshot. Descends into directories in a breadth-first fashion.
 *
 * @param snapshot checksum of the snapshot to process.
 * @returns tuple of file path and SHA256 checksum.
 */
async function* walkTree(snapshot: string): AsyncIterableIterator<[string, string]> {
  // queue entry is (full-path, tree-sha1)
  let queueEntry: [string, string]
  // use a queue to perform a breadth-first traversal
  const queue: typeof queueEntry[] = []
  const snapdoc = await database.getSnapshot(snapshot)
  queue.push(['.', snapdoc.tree])
  while (queue.length) {
    const [basedir, treesha] = queue.shift()
    const tree = await database.getTree(treesha)
    for (let ent of tree.entries) {
      if (modeToType(ent.mode) === FileType.DIR) {
        queue.push([path.join(basedir, ent.name), ent.reference])
      } else if (modeToType(ent.mode) === FileType.REG) {
        yield [path.join(basedir, ent.name), ent.reference]
      }
    }
  }
}

/**
 * Wait for a pack file upload to complete, returning the object name
 * as it is known on the remote end.
 *
 * @param emitter the store event emitter.
 * @returns resolves to the remote object identifier.
 */
function waitForUpload(emitter: store.StoreEmitter): Promise<string> {
  return new Promise((resolve, reject) => {
    let object: string = null
    emitter.on('object', (name) => {
      object = name
    })
    emitter.on('error', (err) => {
      reject(err)
    })
    emitter.on('done', () => {
      resolve(object)
    })
  })
}

/**
 * Record the packs and chunks that were successfully uploaded.
 * 
 * @param results details regarding the pack file.
 * @param bucket name of the remote bucket.
 * @param object name of the remote object (may be archive identifier).
 */
async function recordFinishedChunks(results: PackBuildResults, bucket: string, object: string) {
  // chunk records point to the packs
  for (let chunk of results.chunks) {
    const doc = {
      length: chunk.size,
      pack: results.checksum
    }
    await database.insertChunk(core.checksumFromBuffer(chunk.hash, 'sha256'), doc)
  }
  // pack records provide the remote coordinates
  const doc = {
    bucket,
    object,
    upload_date: Date.now()
  }
  await database.insertPack(results.checksum, doc)
}

/**
 * Record the files that were successfully uploaded in their entirety.
 * 
 * @param files map of file checksums to the chunks within the file.
 */
async function recordFinishedFiles(files: Map<string, core.Chunk[]>) {
  // file records track the file size and all of its chunks
  for (let [checksum, chunks] of files.entries()) {
    let size = 0
    const parts = []
    for (let chunk of chunks) {
      const digest = core.checksumFromBuffer(chunk.hash, 'sha256')
      parts.push({ offset: chunk.offset, digest })
      size += chunk.size
    }
    const doc = {
      length: size,
      chunks: parts
    }
    await database.insertFile(checksum, doc)
  }
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
 * A generator that yields [filepath, checksum] pairs for files that were added
 * or changed between the two snapshots. Only files are considered, as changes
 * to directories are already recorded in the database and saved separately.
 * Ignores anything that is not a file or a directory. May return files that
 * were processed earlier, so the caller must filter out files that have record
 * entries in the database.
 *
 * @param snapshot1 earlier snapshot.
 * @param snapshot2 later snapshot.
 * @returns tuple of file path and the SHA256 checksum from the tree entry.
 */
export async function* findChangedFiles(
  snapshot1: string,
  snapshot2: string
): AsyncIterableIterator<[string, string]> {
  // queue entry is (full path, tree1 sha1, tree2 sha1)
  let entry: [string, string, string]
  // use a queue to perform a breadth-first traversal
  const queue: typeof entry[] = []
  // to start, add tree for each snapshot to the queue
  const snap1doc = await database.getSnapshot(snapshot1)
  const snap2doc = await database.getSnapshot(snapshot2)
  queue.push(['.', snap1doc.tree, snap2doc.tree])
  while (queue.length) {
    const [basedir, tree1sha, tree2sha] = queue.shift()
    const tree1 = await database.getTree(tree1sha)
    const entries1: TreeEntry[] = tree1.entries
    const tree2 = await database.getTree(tree2sha)
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
          yield* addAllFilesUnder(path.join(basedir, entry2.name), entry2.reference)
        } else if (modeToType(entry2.mode) === FileType.REG) {
          yield [path.join(basedir, entry2.name), entry2.reference]
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
          const dirpath = path.join(basedir, entry1.name)
          queue.push([dirpath, entry1.reference, entry2.reference])
        } else if ((is1file || is1dir || is1link) && is2file) {
          // new file or a changed file
          yield [path.join(basedir, entry2.name), entry2.reference]
        } else if ((is1file || is1link) && is2dir) {
          // now a directory, add everything under it
          yield* addAllFilesUnder(path.join(basedir, entry2.name), entry2.reference)
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
        yield* addAllFilesUnder(path.join(basedir, entry2.name), entry2.reference)
      } else if (modeToType(entry2.mode) === FileType.REG) {
        yield [path.join(basedir, entry2.name), entry2.reference]
      }
      index2++
    }
  }
}

/**
 * For the given tree, add every file therein to the `changed` map.
 *
 * @param basepath path prefix for files found under the tree.
 * @param ref database document identifier of the tree record.
 */
async function* addAllFilesUnder(basepath: string, ref: string): AsyncIterableIterator<[string, string]> {
  const tree = await database.getTree(ref)
  const entries: TreeEntry[] = tree.entries
  for (let entry of entries) {
    if (modeToType(entry.mode) === FileType.DIR) {
      yield* addAllFilesUnder(path.join(basepath, entry.name), entry.reference)
    } else if (modeToType(entry.mode) === FileType.REG) {
      yield [path.join(basepath, entry.name), entry.reference]
    }
  }
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
  digest: string
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
      xattrs.push({ name, digest: hash })
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
