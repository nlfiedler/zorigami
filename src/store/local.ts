//
// Copyright (c) 2018 Nathan Fiedler
//
import * as events from 'events'
import * as fs from 'fs'
import * as path from 'path'
import * as fx from 'fs-extra'
import * as verr from 'verror'
import { StoreEmitter } from './index'

/**
 * Local disk implementation of the `Store` interface.
 */
export class LocalStore {
  readonly basepath: string

  /**
   * Construct a new instance of LocalStore.
   *
   * @param basepath location where pack files will be stored.
   */
  constructor(basepath: string) {
    this.basepath = basepath
  }

  storePack(packfile: string, bucket: string, object: string): StoreEmitter {
    if (!fs.existsSync(packfile)) {
      throw new verr.VError({
        name: 'IllegalArgumentError',
        info: {
          path: packfile
        }
      }, `missing pack file: ${packfile}`)
    }
    const emitter = new events.EventEmitter()
    process.nextTick(() => {
      const buckdir = path.join(this.basepath, bucket)
      fx.ensureDir(buckdir, err => {
        if (err) {
          emitter.emit('error', err)
        } else {
          const destfile = path.join(buckdir, object)
          fx.move(packfile, destfile, err => {
            if (err) {
              emitter.emit('error', err)
            } else {
              emitter.emit('done')
            }    
          })
        }
      })
    })
    return emitter
  }

  retrievePack(bucket: string, object: string, outfile: string): StoreEmitter {
    const buckdir = path.join(this.basepath, bucket)
    const packfile = path.join(buckdir, object)
    if (!fs.existsSync(packfile)) {
      throw new verr.VError({
        name: 'RuntimeError',
        info: {
          path: packfile
        }
      }, `missing object file: ${packfile}`)
    }
    const emitter = new events.EventEmitter()
    process.nextTick(() => {
      fx.ensureDir(path.dirname(outfile), err => {
        if (err) {
          emitter.emit('error', err)
        } else {
          fx.copy(packfile, outfile, err => {
            if (err) {
              emitter.emit('error', err)
            } else {
              emitter.emit('done')
            }    
          })
        }
      })
    })
    return emitter
  }

  listBuckets(): StoreEmitter {
    const entries = fs.readdirSync(this.basepath, { withFileTypes: true })
    const dirs = entries.filter((entry) => entry.isDirectory())
    const emitter = new events.EventEmitter()
    process.nextTick(() => {
      for (let entry of dirs) {
        emitter.emit('bucket', entry.name)
      }
      emitter.emit('done')
    })
    return emitter
  }

  listObjects(bucket: string): StoreEmitter {
    const buckdir = path.join(this.basepath, bucket)
    if (!fs.existsSync(buckdir)) {
      throw new verr.VError({
        name: 'RuntimeError',
        info: {
          bucket: bucket
        }
      }, `no such bucket: ${bucket}`)
    }
    const entries = fs.readdirSync(buckdir, { withFileTypes: true })
    const files = entries.filter((entry) => entry.isFile())
    const emitter = new events.EventEmitter()
    process.nextTick(() => {
      for (let entry of files) {
        emitter.emit('object', entry.name)
      }
      emitter.emit('done')
    })
    return emitter
  }
}
