//
// Copyright (c) 2018 Nathan Fiedler
//
import * as events from 'events'
import * as verr from 'verror'

/**
 * Emitted when there is progress to report on an operation.
 *
 * @asMemberOf Store
 * @event
 * @param value indicates the progress.
 */
declare function progress (value: number): void

/**
 * Emitted for each bucket when iterating over all buckets.
 *
 * @asMemberOf Store
 * @event
 * @param name name of the bucket.
 */
declare function bucket (name: string): void

/**
 * Emitted for each object when iterating over all objects.
 *
 * @asMemberOf Store
 * @event
 * @param name name of the object.
 */
declare function object (name: string): void

/**
 * Emitted when an operation has completed.
 *
 * @asMemberOf Store
 * @event
 */
declare function done (): void

/**
 * Emitted when an error has occurred.
 *
 * @asMemberOf Store
 * @event
 * @param error the error.
 */
declare function error (error: Error): void

export interface StoreEmitter extends events.EventEmitter {
  on(event: 'progress', listener: typeof progress): this
  on(event: 'bucket', listener: typeof bucket): this
  on(event: 'object', listener: typeof object): this
  on(event: 'done', listener: typeof done): this
  on(event: 'error', listener: typeof error): this
}

export interface Store {
  /**
   * Store the pack file the local disk.
   *
   * @param packfile path of the pack file.
   * @param bucket name of the bucket in which to store the pack.
   * @param object desired name of the pack in the bucket.
   * @returns emits `progress`, `object`, `done`, and `error` events.
   */
  storePack(packfile: string, bucket: string, object: string): StoreEmitter

  /**
   * Retrieve a pack from the given bucket and object name.
   *
   * @param bucket name of the bucket containing the pack.
   * @param object expected name of the pack in the bucket.
   * @param outfile path to which pack will be written.
   * @returns emits `progress`, `done`, and `error` events.
   */
  retrievePack(bucket: string, object: string, outfile: string): StoreEmitter

  /**
   * Delete the named object from the given bucket.
   *
   * @param bucket name of the bucket containing the object.
   * @param object expected name of the object in the bucket.
   * @returns emits `done` and `error` events.
   */
  deleteObject(bucket: string, object: string): StoreEmitter

  /**
   * Delete the named bucket. It almost certainly needs to be empty first, so
   * use `listObjects()` and `deleteObject()` to remove the objects.
   *
   * @param bucket name of the bucket to be removed.
   * @returns emits `done` and `error` events.
   */
  deleteBucket(bucket: string): StoreEmitter

  /**
   * Returns a list of all buckets via `bucket` events. Emits `done`
   * when all buckets have been returned.
   *
   * @returns names of buckets via event emitter.
   */
  listBuckets(): StoreEmitter

  /**
   * Returns a list of all objects in the named bucket via `object` events.
   * Emits `done` when all objects have been returned.
   *
   * @param bucket name of the bucket to examine.
   * @returns names of objects via event emitter.
   */
  listObjects(bucket: string): StoreEmitter
}

const stores = new Map()

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
 * Save the given pack file to the store named by the key. The store will emit
 * an `object` event prior to the `done` event, with the name of the object as
 * it is known in the remote store. Some remote stores generate an identifier
 * for the object (e.g. Amazon Glacier).
 *
 * @param key the key for the store to receive the pack.
 * @param packfile the pack to be stored.
 * @param bucket the bucket to which the pack should be stored.
 * @param object preferred name of the remote object for this pack.
 * @returns emits `progress`, `object`, `done`, and `error` events.
 */
export function storePack(key: string, packfile: string, bucket: string, object: string): StoreEmitter {
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
export function retrievePack(key: string, bucket: string, object: string, outfile: string): StoreEmitter {
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
 * Delete the named object from the given bucket.
 *
 * @param key the key for the store to store the pack.
 * @param bucket name of the bucket containing the object.
 * @param object expected name of the object in the bucket.
 * @returns emits `done` and `error` events.
 */
export function deleteObject(key: string, bucket: string, object: string): StoreEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.deleteObject(bucket, object)
  } else {
    throw new verr.VError({
      name: 'IllegalArgumentError',
      info: { key }
    }, `no store registered for ${key}`)
  }
}

/**
 * Delete the named bucket. It almost certainly needs to be empty first, so
 * use `listObjects()` and `deleteObject()` to remove the objects.
 *
 * @param bucket name of the bucket to be removed.
 * @returns emits `done` and `error` events.
 */
export function deleteBucket(key: string, bucket: string): StoreEmitter {
  if (stores.has(key)) {
    const store: Store = stores.get(key)
    return store.deleteBucket(bucket)
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
export function listBuckets(key: string): StoreEmitter {
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
export function listObjects(key: string, bucket: string): StoreEmitter {
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
export function collectBuckets(emitter: StoreEmitter): Promise<string[]> {
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
export function collectObjects(emitter: StoreEmitter): Promise<string[]> {
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
export function waitForDone(emitter: StoreEmitter): Promise<any> {
  return new Promise((resolve, reject) => {
    emitter.on('error', (err) => {
      reject(err)
    })
    emitter.on('done', () => {
      resolve()
    })
  })
}
