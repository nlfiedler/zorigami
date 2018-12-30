//
// Copyright (c) 2018 Nathan Fiedler
//
const config = require('config')
const fs = require('fs-extra')
const logger = require('lib/logging')
const PouchDB = require('pouchdb')

const dbPath = config.get('database.path')
fs.ensureDirSync(dbPath)
const db = new PouchDB(dbPath)

/**
 * Ensure the database is prepared with the necessary design documents.
 *
 * @returns {Promise<boolean>} resolves to true.
 */
async function initDatabase () {
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
 */
async function clearDatabase () {
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
 * @param {object} doc new document
 * @param {string} doc._id document identifier
 * @returns {Promise} resolves to false if inserted, true if updated.
 */
async function updateDocument (doc) {
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
 * For any fields of the document that are objects with property 'type' that
 * equals 'Buffer', convert the field to a Buffer whose data is that of the
 * 'data' field.
 *
 * @param {Object} doc document fetched from database, modified in place.
 */
function convertBuffers (doc) {
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
 * @param {string} docId identifier of document to retrieve.
 * @returns {Promise<Object>} resolves to document object, or null if not found.
 */
async function fetchDocument (docId) {
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

module.exports = {
  initDatabase,
  clearDatabase,
  fetchDocument,
  updateDocument
}
