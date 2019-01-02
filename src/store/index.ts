//
// Copyright (c) 2018 Nathan Fiedler
//

export interface Store {
  // TODO: add functions
}

export interface Pack {
  // TODO: add functions
}

export interface Bucket {
  // TODO: add functions
}

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
 * @param key used for selecting the store to which a pack is stored.
 * @param store an implementation of the Store class.
 */
export function registerStore(key: string, store: Store) {
  stores.set(key, store)
}

/**
 * Remove the store implementation that was registered earlier with key.
 *
 * @param key the key to find the store to be removed.
 */
export function unregisterStore(key: string) {
  stores.delete(key)
}

/**
 * Save the given pack file to the store named by the key.
 *
 * @param key the key for the store to receive the pack.
 * @param pack the pack to be stored.
 * @param bucket the bucket to which the pack should be stored.
 * @param objectName preferred name of the remote object for this pack.
 */
export function storePack(key: string, pack: Pack, bucket: Bucket, objectName: string) {
  // TODO: implement
}
