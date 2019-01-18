//
// Copyright (c) 2018 Nathan Fiedler
//
import * as events from 'events'
import * as fs from 'fs'
import * as stream from 'stream'
const Minio = require('minio')
import { StoreEmitter } from './index'

export class MinioStore {
  readonly options: any
  readonly client: any
  readonly region: string

  constructor(options: any, region: string) {
    this.options = options
    this.client = new Minio.Client(options)
    this.region = region
  }

  storePack(packfile: string, bucket: string, object: string): StoreEmitter {
    if (!fs.existsSync(packfile)) {
      throw new Error('missing file: ' + packfile)
    }
    const emitter = new events.EventEmitter()
    this.client.makeBucket(bucket, this.region, (err: Error) => {
      if (err) {
        emitter.emit('error', err)
      } else {
        const metaData = { 'Content-Type': 'application/octet-stream' }
        this.client.fPutObject(bucket, object, packfile, metaData, (err: Error, data: string) => {
          if (err) {
            emitter.emit('error', err)
          } else {
            emitter.emit('object', object)
            emitter.emit('done', data)
          }
        })
      }
    })
    return emitter
  }

  retrievePack(bucket: string, object: string, outfile: string): StoreEmitter {
    const emitter = new events.EventEmitter()
    this.client.fGetObject(bucket, object, outfile, (err: Error) => {
      if (err) {
        emitter.emit('error', err)
      } else {
        emitter.emit('done')
      }
    })
    return emitter
  }

  listBuckets(): StoreEmitter {
    const emitter = new events.EventEmitter()
    this.client.listBuckets((err: Error, buckets: any[]) => {
      if (err) {
        emitter.emit('error', err)
      } else {
        for (let b of buckets) {
          emitter.emit('bucket', b.name)
        }
        emitter.emit('done')
      }
    })
    return emitter
  }

  listObjects(bucket: string): StoreEmitter {
    const emitter = new events.EventEmitter()
    const stream: stream.Readable = this.client.listObjects(bucket)
    stream.on('data', (obj: any) => {
      emitter.emit('object', obj.name)
    })
    stream.on('end', () => {
      emitter.emit('done')
    })
    stream.on('error', (err: Error) => {
      emitter.emit('error', err)
    })
    return emitter
  }
}
