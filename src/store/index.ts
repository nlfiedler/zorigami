//
// Copyright (c) 2018 Nathan Fiedler
//

export interface Store {
  /**
   * Store the pack file the local disk.
   *
   * @param packfile path of the pack file.
   * @param bucket name of the bucket in which to store the pack.
   * @param object desired name of the pack in the bucket.
   */
  storePack(packfile: string, bucket: string, object: string): void

  /**
   * Retrieve a pack from the given bucket and object name.
   *
   * @param bucket name of the bucket containing the pack.
   * @param object expected name of the pack in the bucket.
   * @param outfile path to which pack will be written.
   */
  retrievePack(bucket: string, object: string, outfile: string): void
  // - list all buckets
  // - list all objects in a bucket
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
export function registerStore(key: string, store: Store): void {
  stores.set(key, store)
}

/**
 * Remove the store implementation that was registered earlier with key.
 *
 * @param key the key to find the store to be removed.
 */
export function unregisterStore(key: string): void {
  stores.delete(key)
}

/**
 * Save the given pack file to the store named by the key.
 *
 * @param key the key for the store to receive the pack.
 * @param packfile the pack to be stored.
 * @param bucket the bucket to which the pack should be stored.
 * @param object preferred name of the remote object for this pack.
 */
export function storePack(key: string, packfile: string, bucket: string, object: string): void {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    store.storePack(packfile, bucket, object)
  } else {
    throw new Error(`no store registered for ${key}`)
  }
}

/**
 * Retrieve the pack file from the given bucket and object.
 *
 * @param key the key for the store to store the pack.
 * @param bucket the bucket from which the pack will be loaded.
 * @param object the object name that refers to the pack.
 * @param outfile path to which pack will be written.
 */
export function retrievePack(key: string, bucket: string, object: string, outfile: string): void {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    store.retrievePack(bucket, object, outfile)
  } else {
    throw new Error(`no store registered for ${key}`)
  }
}
