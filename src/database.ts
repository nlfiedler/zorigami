//
// Copyright (c) 2018 Nathan Fiedler
//
import * as config from 'config'
import * as fx from 'fs-extra'
import logger from './logging'
import * as PouchDB from 'pouchdb'

const dbPath: string = config.get('database.path')
fx.ensureDirSync(dbPath)
let db = new PouchDB(dbPath)

declare function emit(value: any, count: number): void

let queriesDefinition = {
  _id: '_design/queries',
  views: {
    all_types: {
      map: function (doc: any) {
        if (doc._id.includes('/')) {
          emit(doc._id.split('/')[0], 1)
        }
      }.toString(),
      reduce: '_count'
    }
  }
}

/**
 * If the schema has changed, update the design document. If it was not yet
 * created, do so now.
 *
 * @param index design document to be inserted/updated.
 * @returns true if index was created, false otherwise.
 */
async function createIndices(index: any): Promise<boolean> {
  let created = false
  try {
    await db.get(index._id)
  } catch (err) {
    if (err.status === 404) {
      await db.put(index)
      created = true
    } else {
      throw err
    }
  }
  // clean up any stale indices from previous versions
  await db.viewCleanup()
  return created
}

/**
 * Perform a query against all of the views to prime the indices.
 *
 * @param index design document to be primed.
 */
async function primeIndices(index: any): Promise<void> {
  for (const view in index.views) {
    await db.query(`queries/${view}`, {
      limit: 0
    })
  }
}

/**
 * Ensure the database is prepared with the necessary design documents.
 *
 * @returns always returns true.
 */
export async function initDatabase(): Promise<boolean> {
  let indexCreated = await createIndices(queriesDefinition)
  if (indexCreated) {
    logger.info('database indices created')
    await primeIndices(queriesDefinition)
    logger.info('database indices primed')
  }
  return true
}

/**
 * Destroy the database and initialize again.
 *
 * @returns whatever `initDatabase` returns.
 */
export async function clearDatabase(): Promise<boolean> {
  await db.destroy()
  db = new PouchDB(dbPath)
  return initDatabase()
}

/**
 * Insert or update a document in the database.
 *
 * @param doc new document
 * @param {string} doc._id document identifier
 * @returns false if inserted, true if updated.
 */
export async function updateDocument(doc: any): Promise<boolean> {
  try {
    let oldDoc = await db.get(doc._id)
    await db.put({ ...doc, _rev: oldDoc._rev })
    logger.info(`updated existing document ${doc._id}`)
    return true
  } catch (err) {
    if (err.status === 404) {
      await db.put(doc)
      logger.info(`inserted new document ${doc._id}`)
      return false
    } else {
      throw err
    }
  }
}

/**
 * Update the `configuration` document with the values provided.
 *
 * @param config configuration values to be saved.
 * @returns false if inserted, true if updated.
 */
export async function updateConfiguration(config: any): Promise<boolean> {
  const doc = {
    ...config,
    _id: 'configuration'
  }
  return updateDocument(doc)
}

/**
 * Retrieve the `configuration` document, if any.
 *
 * @returns document object, or null if not found.
 */
export async function getConfiguration(): Promise<any> {
  return fetchDocument('configuration')
}

/**
 * Ensure the database contains a tree by the given checksum. Conflicts are
 * ignored; if it has the same checksum, it is the same tree.
 *
 * @param checksum hash digest of the tree object.
 * @param doc tree object itself, stored as-is.
 */
export async function insertTree(checksum: string, doc: any): Promise<void> {
  try {
    await db.put({
      ...doc,
      _id: 'tree/' + checksum
    })
    logger.info(`inserted new tree ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the tree record by the given checksum.
 *
 * @param checksum checksum of the desired tree.
 * @returns document object, or null if not found.
 */
export async function getTree(checksum: string): Promise<any> {
  return fetchDocument('tree/' + checksum)
}

/**
 * Ensure the database contains a snapshot by the given checksum. Conflicts are
 * ignored; if it has the same checksum, it is the same snapshot.
 *
 * @param checksum hash digest of the snapshot object.
 * @param doc snapshot object itself, stored as-is.
 */
export async function insertSnapshot(checksum: string, doc: any): Promise<void> {
  try {
    await db.put({
      ...doc,
      _id: 'snapshot/' + checksum
    })
    logger.info(`inserted new snapshot ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the extended attribute value by the given checksum.
 *
 * @param checksum checksum of the desired attribute.
 * @returns attribute data, or null if not found.
 */
export async function getExtAttr(checksum: string): Promise<Buffer> {
  const doc = await fetchDocument('xattr/' + checksum)
  if (doc) {
    return doc.value
  }
  return null
}

/**
 * Ensure the database contains an extended attribute by the given checksum.
 * Conflicts are ignored; if it has the same checksum, it is the same attribute.
 *
 * @param checksum hash digest of the attribute data.
 * @param doc attribute data itself, stored as-is.
 */
export async function insertExtAttr(checksum: string, data: Buffer): Promise<void> {
  try {
    await db.put({
      _id: 'xattr/' + checksum,
      value: data
    })
    logger.info(`inserted new attribute ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the snapshot record by the given checksum.
 *
 * @param checksum checksum of the desired snapshot.
 * @returns document object, or null if not found.
 */
export async function getSnapshot(checksum: string): Promise<any> {
  return fetchDocument('snapshot/' + checksum)
}

/**
 * Ensure the database contains a file by the given checksum. Conflicts are
 * ignored; if it has the same checksum, it is the same file.
 *
 * @param checksum hash digest of the file object.
 * @param doc file object itself, stored as-is.
 */
export async function insertFile(checksum: string, doc: any): Promise<void> {
  try {
    await db.put({
      ...doc,
      _id: 'file/' + checksum
    })
    logger.info(`inserted new file ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the file record by the given checksum.
 *
 * @param checksum checksum of the desired file.
 * @returns document object, or null if not found.
 */
export async function getFile(checksum: string): Promise<any> {
  return fetchDocument('file/' + checksum)
}

/**
 * Ensure the database contains a pack by the given checksum. Conflicts are
 * ignored; if it has the same checksum, it is the same pack.
 *
 * @param checksum hash digest of the pack object.
 * @param doc pack object itself, stored as-is.
 */
export async function insertPack(checksum: string, doc: any): Promise<void> {
  try {
    await db.put({
      ...doc,
      _id: 'pack/' + checksum
    })
    logger.info(`inserted new pack ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the pack record by the given checksum.
 *
 * @param checksum checksum of the desired pack.
 * @returns document object, or null if not found.
 */
export async function getPack(checksum: string): Promise<any> {
  return fetchDocument('pack/' + checksum)
}

/**
 * Ensure the database contains a chunk by the given checksum. Conflicts are
 * ignored; if it has the same checksum, it is the same chunk.
 *
 * @param checksum hash digest of the chunk object.
 * @param doc chunk object itself, stored as-is.
 */
export async function insertChunk(checksum: string, doc: any): Promise<void> {
  try {
    await db.put({
      ...doc,
      _id: 'chunk/' + checksum
    })
    logger.info(`inserted new chunk ${checksum}`)
  } catch (err) {
    if (err.status !== 409) {
      throw err
    }
  }
}

/**
 * Retrieve the chunk record by the given checksum.
 *
 * @param checksum checksum of the desired chunk.
 * @returns document object, or null if not found.
 */
export async function getChunk(checksum: string): Promise<any> {
  return fetchDocument('chunk/' + checksum)
}

/**
 * For any fields of the document that are objects with property `type` that
 * equals `Buffer`, convert the field to a Buffer whose data is that of the
 * `data` field.
 *
 * @param doc document fetched from database, modified in place.
 */
function convertBuffers(doc: any) {
  for (let prop in doc) {
    if (doc[prop].hasOwnProperty('type')) {
      if (doc[prop].type === 'Buffer') {
        doc[prop] = Buffer.from(doc[prop].data)
      }
    }
  }
}

/**
 * Retrieve the document with the given identifier. Automatically converts
 * fields of certain types (e.g. Buffer) for convenience.
 *
 * @param docId identifier of document to retrieve.
 * @returns document object, or null if not found.
 */
export async function fetchDocument(docId: string): Promise<any> {
  try {
    const doc = await db.get(docId)
    convertBuffers(doc)
    return doc
  } catch (err) {
    if (err.status === 404) {
      return null
    } else {
      throw err
    }
  }
}

/**
 * Return the number of chunks in the database.
 *
 * @returns count of `chunk` records.
 */
export async function countChunks(): Promise<number> {
  const res = await db.query('queries/all_types', {
    key: 'chunk',
    group: true
  })
  if (res.rows === undefined || res.rows.length === 0) {
    throw new Error('invalid query response')
  }
  return res['rows'][0].value
}
