//
// Copyright (c) 2018 Nathan Fiedler
//
import * as events from 'events'
import * as fs from 'fs'
import * as path from 'path'
import * as Client from 'ssh2-sftp-client'
import { StoreEmitter } from './index'

/**
 * SSH/SFTP connection options.
 */
export interface Options {
  /** SFTP port number, defaults to 22 */
  port?: number
  /** User password, if not using key-based authentication. */
  password?: string
  /** Path under which buckets will be stored. */
  basepath?: string
  /** User private key for key-based authentication. */
  privateKey?: Buffer | string
  /** Passphrase to decrypt an encrypted private key. */
  passphrase?: string
}

/**
 * SFTP implementation of the `Store` interface.
 */
export class SecureFtpStore {
  readonly hostname: string
  readonly username: string
  readonly portnum: number
  readonly password: string
  readonly basepath: string
  readonly privateKey: Buffer | string
  readonly passphrase: string

  /**
   * Construct a new instance of SecureFtpStore.
   *
   * @param hostname name of the SFTP host.
   * @param username user for connecting to SFTP host.
   * @param options specifies port and authentication settings.
   */
  constructor(hostname: string, username: string, options?: Options) {
    this.hostname = hostname
    this.username = username
    this.portnum = (options && options.port) ? options.port : 22
    this.password = (options && options.password) ? options.password : undefined
    this.basepath = (options && options.basepath) ? options.basepath : '/'
    this.privateKey = (options && options.privateKey) ? options.privateKey : undefined
    this.passphrase = (options && options.passphrase) ? options.passphrase : undefined
  }

  makeConnectOptions() {
    return {
      host: this.hostname,
      port: this.portnum,
      username: this.username,
      password: this.password,
      privateKey: this.privateKey,
      passphrase: this.passphrase
    }
  }

  storePack(packfile: string, bucket: string, object: string): StoreEmitter {
    if (!fs.existsSync(packfile)) {
      throw new Error('missing file: ' + packfile)
    }
    const emitter = new events.EventEmitter()
    let sftp = new Client()
    sftp.connect(this.makeConnectOptions()).then(() => {
      return sftp.mkdir(path.join(this.basepath, bucket), true)
    }).then(() => {
      return sftp.fastPut(packfile, path.join(this.basepath, bucket, object))
    }).then((data) => {
      sftp.end()
      emitter.emit('done', data)
    }).catch((err) => {
      sftp.end()
      emitter.emit('error', err)
    })
    return emitter
  }

  retrievePack(bucket: string, object: string, outfile: string): StoreEmitter {
    const emitter = new events.EventEmitter()
    let sftp = new Client()
    sftp.connect(this.makeConnectOptions()).then(() => {
      return sftp.fastGet(path.join(this.basepath, bucket, object), outfile)
    }).then((data) => {
      sftp.end()
      emitter.emit('done', data)
    }).catch((err) => {
      sftp.end()
      emitter.emit('error', err)
    })
    return emitter
  }

  listBuckets(): StoreEmitter {
    const emitter = new events.EventEmitter()
    let sftp = new Client()
    sftp.connect(this.makeConnectOptions()).then(() => {
      return sftp.list(this.basepath)
    }).then((data) => {
      sftp.end()
      for (let entry of data) {
        if (entry.type === 'd') {
          emitter.emit('bucket', entry.name)
        }
      }
      emitter.emit('done', data)
    }).catch((err) => {
      sftp.end()
      emitter.emit('error', err)
    })
    return emitter
  }

  listObjects(bucket: string): StoreEmitter {
    const emitter = new events.EventEmitter()
    let sftp = new Client()
    sftp.connect(this.makeConnectOptions()).then(() => {
      return sftp.list(path.join(this.basepath, bucket))
    }).then((data) => {
      sftp.end()
      for (let entry of data) {
        if (entry.type === '-') {
          emitter.emit('object', entry.name)
        }
      }
      emitter.emit('done', data)
    }).catch((err) => {
      sftp.end()
      emitter.emit('error', err)
    })
    return emitter
  }
}
