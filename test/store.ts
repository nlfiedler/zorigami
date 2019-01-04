//
// Copyright (c) 2018 Nathan Fiedler
//
import fs = require('fs')
import path = require('path')
import * as chai from 'chai'
import * as tmp from 'tmp'
import * as core from '../src/core'
import * as store from '../src/store'
import * as local from '../src/store/local'

const assert = chai.assert

//
// Give the database a chance to initialize the database asynchronously.
// A timeout of zero is not sufficient, so this timing is fragile.
//
describe('Store Functionality', () => {
  describe('storing pack files', () => {
    it('should raise on missing pack file', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = () => {
        store.storePack('local', './test/fixtures/does_not_exist', 'bucket', 'object')
      }
      assert.throws(storeFn, Error, 'missing pack file')
    })

    it('should raise on missing object file', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = () => {
        store.retrievePack('local', 'bucket', 'object', 'tmp')
      }
      assert.throws(storeFn, Error, 'missing object file')
    })

    it('should raise on no such bucket', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = () => {
        store.listObjects('local', 'bucket')
      }
      assert.throws(storeFn, Error, 'no such bucket')
    })

    it('should store and retrieve pack files', async () => {
      // create pack file
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          hash: Buffer.from('095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f', 'hex'),
          offset: 0,
          size: 3129
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunks(chunks, packfile)
      assert.equal(digest, 'sha256-ba75dbf315348a3d869fc9cd7e7e0acef28e9de9a9490b2b2901efd700db8c0a')
      // store the pack
      const bucket = core.generateBucketName(core.generateUniqueId('charlie', 'localhost'))
      const basedir = tmp.dirSync().name
      const localStore = new local.LocalStore(basedir)
      store.registerStore('local', localStore)
      const object = path.basename(packfile)
      store.storePack('local', packfile, bucket, object)
      assert.isFalse(fs.existsSync(packfile))
      // check for bucket(s) being present
      const buckets = store.listBuckets('local')
      assert.lengthOf(buckets, 1, 'returned one bucket')
      assert.equal(buckets[0], bucket)
      // check for object(s) being present
      const objects = store.listObjects('local', bucket)
      assert.lengthOf(objects, 1, 'returned one object')
      assert.equal(objects[0], object)
      // retrieve the pack file and verify by unpacking chunks
      store.retrievePack('local', bucket, object, packfile)
      assert.isTrue(fs.existsSync(packfile))
      const outdir = tmp.dirSync().name
      await core.unpackChunks(packfile, outdir)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.lengthOf(entries, 1, 'one chunk unpacked')
      assert.isTrue(entries[0].isFile())
      assert.equal(entries[0].name, 'sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f')
      const chunkDigest = await core.checksumFile(
        path.join(outdir, 'sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f'), 'sha1')
      assert.equal(chunkDigest, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
    })
  })
})
