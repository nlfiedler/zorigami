//
// Copyright (c) 2018 Nathan Fiedler
//

const stores = new Map()

// storage implementations define a class that this module expects
// engine reads the configuration and instantiates the store objects
// various storage objects are registered with this module
// operations in this module perform input validation and such
// this module then delegates to the storage implementations
// implement a "local" store that uses a local path
//
// basic API:
// - store a pack using bucket and object name
// - retrieve a pack using bucket and object name
// - list all buckets
// - list all objects in a bucket

/**
 * Add the given store implementation to those used for storing packs.
 *
 * @param {string} key used for selecting the store to which a pack is stored.
 * @param {Store} store an implementation of the Store class.
 */
function registerStore (key, store) {
  stores.set(key, store)
}

/**
 * Remove the store implementation that was registered earlier with key.
 *
 * @param {string} key the key to find the store to be removed.
 */
function unregisterStore (key) {
  stores.delete(key)
}

/**
 * Save the given pack file to the store named by the key.
 *
 * @param {string} key the key for the store to receive the pack.
 * @param {Pack} pack the pack to be stored.
 * @param {Bucket} bucket the bucket to which the pack should be stored.
 * @param {string} objectName preferred name of the remote object for this pack.
 */
function storePack (key, pack, bucket, objectName) {
  // TODO: implement
}

module.exports = {
  registerStore,
  unregisterStore,
  storePack
}
