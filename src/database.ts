//
// Copyright (c) 2018 Nathan Fiedler
//
import * as config from 'config'
import * as fx from 'fs-extra'
import logger from './logging'
import * as PouchDB from 'pouchdb'

const dbPath: string = config.get('database.path')
fx.ensureDirSync(dbPath)
const db = new PouchDB(dbPath)

/**
 * Ensure the database is prepared with the necessary design documents.
 *
 * @returns always returns true.
 */
export async function initDatabase(): Promise<boolean> {
  // let indexCreated = await createIndices(assetsDefinition)
  // if (indexCreated) {
  //   logger.info('database indices created')
  //   await primeIndices(assetsDefinition)
  //   logger.info('database indices primed')
  // }
  logger.info('database ready')
  return true
}

/**
 * Remove all documents from the database and initialize again.
 *
 * @returns whatever `initDatabase` returns.
 */
export async function clearDatabase(): Promise<boolean> {
  let allDocs = await db.allDocs({ include_docs: true })
  let promises = allDocs.rows.map((row) => db.remove(row.doc))
  let results = await Promise.all(promises)
  logger.info(`removed all ${results.length} documents`)
  // clean up stale indices after removing everything
  // (yes, this is necessary here, otherwise tests fail)
  await db.viewCleanup()
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
 * Retrieve the snapshot record by the given checksum.
 *
 * @param checksum checksum of the desired snapshot.
 * @returns document object, or null if not found.
 */
export async function getSnapshot(checksum: string): Promise<any> {
  return fetchDocument('snapshot/' + checksum)
}

/**
 * For any fields of the document that are objects with property 'type' that
 * equals 'Buffer', convert the field to a Buffer whose data is that of the
 * 'data' field.
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
