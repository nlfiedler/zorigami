//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as chai from 'chai'
import * as tmp from 'tmp'
import * as core from '../src/core'
import * as store from '../src/store'
import * as local from '../src/store/local'
import * as minio from '../src/store/minio'
import * as sftp from '../src/store/sftp'

const assert = chai.assert

describe('Store Functionality', function () {
  describe('storing pack files locally', function () {
    it('should raise on missing pack file', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = function () {
        store.storePack('local', './test/fixtures/does_not_exist', 'bucket', 'object')
      }
      assert.throws(storeFn, Error, 'missing pack file')
    })

    it('should raise on missing object file', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = function () {
        store.retrievePack('local', 'bucket', 'object', 'tmp')
      }
      assert.throws(storeFn, Error, 'missing object file')
    })

    it('should raise on no such bucket', function () {
      const localStore = new local.LocalStore('.')
      store.registerStore('local', localStore)
      const storeFn = function () {
        store.listObjects('local', 'bucket')
      }
      assert.throws(storeFn, Error, 'no such bucket')
    })

    it('should store and retrieve pack files', async function () {
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
      await store.waitForDone(store.storePack('local', packfile, bucket, object))
      assert.isFalse(fs.existsSync(packfile))
      // check for bucket(s) being present
      const buckets = await store.collectBuckets(store.listBuckets('local'))
      assert.lengthOf(buckets, 1, 'returned one bucket')
      assert.equal(buckets[0], bucket)
      // check for object(s) being present
      const objects = await store.collectObjects(store.listObjects('local', bucket))
      assert.lengthOf(objects, 1, 'returned one object')
      assert.equal(objects[0], object)
      // retrieve the pack file and verify by unpacking chunks
      await store.waitForDone(store.retrievePack('local', bucket, object, packfile))
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

  describe('storing pack files via SFTP', function () {
    before(function () {
      if (!process.env.SFTP_HOST) {
        this.skip()
      }
    })

    it('should store and retrieve pack files', async function () {
      // the test over SFTP requires more than a couple of seconds to complete
      this.timeout(10000)
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
      const options = {
        port: parseInt(process.env.SFTP_PORT) || 22,
        basepath: process.env.SFTP_BASEPATH,
        password: process.env.SFTP_PASSWORD
      }
      const sftpStore = new sftp.SecureFtpStore(process.env.SFTP_HOST, process.env.SFTP_USER, options)
      store.registerStore('sftp', sftpStore)
      const object = path.basename(packfile)
      await store.waitForDone(store.storePack('sftp', packfile, bucket, object))
      // check for bucket(s) being present; may be more from previous runs
      const buckets = await store.collectBuckets(store.listBuckets('sftp'))
      assert.isAtLeast(buckets.length, 1, 'returned at least one bucket')
      assert.include(buckets, bucket, 'expected bucket is in the list')
      // check for object(s) being present
      const objects = await store.collectObjects(store.listObjects('sftp', bucket))
      assert.lengthOf(objects, 1, 'returned one object')
      assert.equal(objects[0], object)
      // retrieve the pack file and verify by unpacking chunks
      await store.waitForDone(store.retrievePack('sftp', bucket, object, packfile))
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

  describe('storing pack files via Minio', function () {
    before(function () {
      if (!process.env.MINIO_ENDPOINT) {
        this.skip()
      }
    })

    it('should store and retrieve pack files', async function () {
      // the test with Minio requires more than a couple of seconds to complete
      this.timeout(10000)
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
      const options = {
        endPoint: process.env.MINIO_ENDPOINT,
        port: parseInt(process.env.MINIO_PORT) || 9000,
        useSSL: process.env.MINIO_USE_SSL === 'true',
        accessKey: process.env.MINIO_ACCESS_KEY,
        secretKey: process.env.MINIO_SECRET_KEY
      }
      const minioStore = new minio.MinioStore(options, 'us-west-1')
      store.registerStore('minio', minioStore)
      const object = path.basename(packfile)
      await store.waitForDone(store.storePack('minio', packfile, bucket, object))
      // check for bucket(s) being present; may be more from previous runs
      const buckets = await store.collectBuckets(store.listBuckets('minio'))
      assert.isAtLeast(buckets.length, 1, 'returned at least one bucket')
      assert.include(buckets, bucket, 'expected bucket is in the list')
      // check for object(s) being present
      const objects = await store.collectObjects(store.listObjects('minio', bucket))
      assert.lengthOf(objects, 1, 'returned one object')
      assert.equal(objects[0], object)
      // retrieve the pack file and verify by unpacking chunks
      await store.waitForDone(store.retrievePack('minio', bucket, object, packfile))
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
