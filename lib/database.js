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
  return true
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
 * Retrieve the document with the given identifier.
 *
 * @param {string} docId identifier of document to retrieve.
 * @returns {Promise<Object>} resolves to document object, or null if not found.
 */
async function fetchDocument (docId) {
  try {
    return await db.get(docId)
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
  fetchDocument,
  updateDocument
}
