//
// Copyright (c) 2018 Nathan Fiedler
//
import * as crypto from 'crypto'
import * as fs from 'fs'
import * as path from 'path'
import * as util from 'util'
import * as PouchDB from 'pouchdb'
import * as fx from 'fs-extra'

const freaddir = util.promisify(fs.readdir)
const dbPath = '/tmp/filesdb/leveldb'
fx.ensureDirSync(dbPath)
const db = new PouchDB(dbPath)

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
      const checksum = checksumData(formatTree(scan.entries), 'sha1')
      entries.push(await processPath(entry.name, fullpath, checksum))
    }
  }
  for (let entry of dirents) {
    if (entry.isFile()) {
      const fullpath = path.join(basepath, entry.name)
      const checksum = await checksumFile(fullpath)
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
 * Produce and insert a record for the tree consisting of the given entries.
 *
 * @param tree list of entries that make up a single tree record.
 */
async function insertTree(tree: TreeEntry[]): Promise<void> {
  const checksum = checksumData(formatTree(tree), 'sha1')
  const doc = {
    _id: checksum,
    entries: tree
  }
  try {
    await db.put(doc)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}


function checksumData(data: string | Buffer, algo: string): string {
  const hash = crypto.createHash(algo)
  hash.update(data)
  return `${algo}-${hash.digest('hex')}`
}

function checksumFile(infile: string): Promise<string> {
  const input = fs.createReadStream(infile)
  const hash = crypto.createHash('sha256')
  return new Promise((resolve, reject) => {
    input.on('readable', () => {
      const data = input.read()
      if (data) {
        hash.update(data)
      } else {
        resolve(`sha256-${hash.digest('hex')}`)
      }
    })
    input.on('error', (err) => {
      input.destroy()
      reject(err)
    })
  })
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

const t1 = Date.now()
scantree('.git').then((results) => {
  console.info(formatTree(results.entries))
  const t2 = Date.now()
  console.info(`processed ${results.fileCount} files in ${(t2 - t1) / 1000} seconds`)
}).catch((err) => {
  console.error(err)
})
