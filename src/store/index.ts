//
// Copyright (c) 2018 Nathan Fiedler
//
import events = require('events')
import verr = require('verror')

export interface Store {
  /**
   * Store the pack file the local disk.
   *
   * @param packfile path of the pack file.
   * @param bucket name of the bucket in which to store the pack.
   * @param object desired name of the pack in the bucket.
   * @returns emits `progress`, `done`, and `error` events.
   */
  storePack(packfile: string, bucket: string, object: string): events.EventEmitter

  /**
   * Retrieve a pack from the given bucket and object name.
   *
   * @param bucket name of the bucket containing the pack.
   * @param object expected name of the pack in the bucket.
   * @param outfile path to which pack will be written.
   * @returns emits `progress`, `done`, and `error` events.
   */
  retrievePack(bucket: string, object: string, outfile: string): events.EventEmitter

  /**
   * Returns a list of all buckets via `bucket` events. Emits `done`
   * when all buckets have been returned.
   *
   * @returns names of buckets via event emitter.
   */
  listBuckets(): events.EventEmitter

  /**
   * Returns a list of all objects in the named bucket via `object` events.
   * Emits `done` when all objects have been returned.
   *
   * @param bucket name of the bucket to examine.
   * @returns names of objects via event emitter.
   */
  listObjects(bucket: string): events.EventEmitter
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
 * @returns emits `progress`, `done`, and `error` events.
 */
export function storePack(key: string, packfile: string, bucket: string, object: string): events.EventEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.storePack(packfile, bucket, object)
  } else {
    throw new verr.VError({
      name: 'IllegalArgumentError',
      info: { key }
    }, `no store registered for ${key}`)
  }
}

/**
 * Retrieve the pack file from the given bucket and object.
 *
 * @param key the key for the store to store the pack.
 * @param bucket the bucket from which the pack will be loaded.
 * @param object the object name that refers to the pack.
 * @param outfile path to which pack will be written.
 * @returns emits `progress`, `done`, and `error` events.
 */
export function retrievePack(key: string, bucket: string, object: string, outfile: string): events.EventEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.retrievePack(bucket, object, outfile)
  } else {
    throw new verr.VError({
      name: 'IllegalArgumentError',
      info: { key }
    }, `no store registered for ${key}`)
  }
}

/**
 * Returns a list of all buckets.
 *
 * @param key the key for the store to examine.
 * @returns names of buckets via an event emitter.
 */
export function listBuckets(key: string): events.EventEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.listBuckets()
  } else {
    throw new verr.VError({
      name: 'IllegalArgumentError',
      info: { key }
    }, `no store registered for ${key}`)
  }
}

/**
 * Returns a list of all objects in the named bucket.
 *
 * @param key the key for the store to examine.
 * @param bucket name of the bucket to examine.
 * @returns names of objects via an event emitter.
 */
export function listObjects(key: string, bucket: string): events.EventEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.listObjects(bucket)
  } else {
    throw new verr.VError({
      name: 'IllegalArgumentError',
      info: { key }
    }, `no store registered for ${key}`)
  }
}

/**
 * Collect all of the buckets returned by the emitter.
 *
 * @param emitter event emitter as from `listBuckets()`.
 * @returns resolves to list of bucket names.
 */
export function collectBuckets(emitter: events.EventEmitter): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const buckets: string[] = []
    emitter.on('bucket', (name) => {
      buckets.push(name)
    })
    emitter.on('error', (err) => {
      reject(err)
    })
    emitter.on('done', () => {
      resolve(buckets)
    })
  })
}

/**
 * Collect all of the objects returned by the emitter.
 *
 * @param emitter event emitter as from `listObjects()`.
 * @returns resolves to list of object names.
 */
export function collectObjects(emitter: events.EventEmitter): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const objects: string[] = []
    emitter.on('object', (name) => {
      objects.push(name)
    })
    emitter.on('error', (err) => {
      reject(err)
    })
    emitter.on('done', () => {
      resolve(objects)
    })
  })
}

/**
 * Resolve when `done` event emitted, or reject with `error` emitted.
 * Like util.promisify() but without constructing a new function.
 *
 * @param emitter event emitter that yields `done` or `error`.
 * @returns promise.
 */
export function waitForDone(emitter: events.EventEmitter): Promise<any> {
  return new Promise((resolve, reject) => {
    emitter.on('error', (err) => {
      reject(err)
    })
    emitter.on('done', () => {
      resolve()
    })
  })
}
