//
// Copyright (c) 2018 Nathan Fiedler
//
const { assert } = require('chai')
const { describe, it } = require('mocha')
const crypto = require('crypto')
const core = require('lib/core')
const fs = require('fs')
const path = require('path')
const tmp = require('tmp')

describe('Core Functionality', function () {
  describe('unique identifier', function () {
    it('should return a UUIDv5 for user and host', function () {
      const uniqId = core.generateUniqueId('charlie', 'localhost')
      assert.equal(uniqId, '747267d5-6e70-5711-8a9a-a40c24c1730f')
    })
  })

  describe('bucket name', function () {
    it('should generate a 58 character mostly alphanumeric string', function () {
      const uniqId = core.generateUniqueId('charlie', 'localhost')
      const bucket = core.generateBucketName(uniqId)
      assert.equal(bucket.length, 58)
      assert.match(bucket, /\w{58}/, 'bucket name is cloud "safe"')
    })
  })

  describe('master passwords', function () {
    it('should encrypt and decrypt successfully', function () {
      const password = 'keyboard cat'
      const expected = core.generateMasterKeys()
      const { salt, iv, hmac, encrypted } =
        core.newMasterEncryptionData(password, expected.master1, expected.master2)
      const actual = core.decryptMasterKeys(salt, password, iv, encrypted, hmac)
      assert.isTrue(expected.master1.equals(actual.master1))
      assert.isTrue(expected.master2.equals(actual.master2))
    })
  })

  describe('file encryption', function () {
    it('should encrypt and decrypt files', async function () {
      const key = Buffer.alloc(32)
      crypto.randomFillSync(key)
      const iv = Buffer.alloc(16)
      crypto.randomFillSync(iv)
      const infile = './test/fixtures/lorem-ipsum.txt'
      const encrypted = tmp.fileSync().name
      await core.encryptFile(infile, encrypted, key, iv)
      const originalBuf = fs.readFileSync(infile)
      const encryptBuf = fs.readFileSync(encrypted)
      assert.isFalse(originalBuf.equals(encryptBuf),
        'encrypted not equal to original')
      const decrypted = tmp.fileSync().name
      await core.decryptFile(encrypted, decrypted, key, iv)
      const decryptBuf = fs.readFileSync(decrypted)
      assert.isTrue(originalBuf.equals(decryptBuf),
        'original and decrypted match')
    })
  })

  describe('file digests', function () {
    it('should compute the hash digest of a file', async function () {
      const infile = './test/fixtures/lorem-ipsum.txt'
      const sha1 = await core.checksumFile(infile, 'sha1')
      assert.equal(sha1, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
      const sha256 = await core.checksumFile(infile, 'sha256')
      assert.equal(sha256, 'sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f')
    })
  })

  describe('file compression', function () {
    it('should compress and decompress files', async function () {
      const infile = './test/fixtures/lorem-ipsum.txt'
      const compressed = tmp.fileSync().name
      await core.compressFile(infile, compressed)
      const originalBuf = fs.readFileSync(infile)
      const compressBuf = fs.readFileSync(compressed)
      assert.isFalse(originalBuf.equals(compressBuf),
        'compressed not equal to original')
      const decompressed = tmp.fileSync().name
      await core.decompressFile(compressed, decompressed)
      const decompressBuf = fs.readFileSync(decompressed)
      assert.isTrue(originalBuf.equals(decompressBuf),
        'original and decompressed match')
    })
  })

  describe('pack files', function () {
    it('should create a pack with one chunk', async function () {
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 3129
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunks(chunks, packfile)
      assert.equal(digest, 'sha256-5c5ed2868c12085f9ef1ad33cd8e94f806a471eb593c5debe3f8410379739da9')
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackChunks(packfile, 'chunk', outdir)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.equal(entries.length, 1, 'one chunk unpacked')
      assert.isTrue(entries[0].isFile())
      assert.equal(entries[0].name, 'chunk.1')
      const chunkDigest = await core.checksumFile(path.join(outdir, 'chunk.1'), 'sha1')
      assert.equal(chunkDigest, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
    })

    it('should create a pack with multiple chunks', async function () {
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 1000
        },
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 1000,
          length: 1000
        },
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 2000,
          length: 1129
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunks(chunks, packfile)
      assert.equal(digest, 'sha256-66dd6914eaa84902d74109685af95a6651947a6d34aef0fec385b6092007bf95')
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackChunks(packfile, 'chunk', outdir)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 3, 'three files unpacked')
      assert.isTrue(entries.includes('chunk.1'))
      assert.isTrue(entries.includes('chunk.2'))
      assert.isTrue(entries.includes('chunk.3'))
      const chunkFiles = [
        path.join(outdir, 'chunk.1'),
        path.join(outdir, 'chunk.2'),
        path.join(outdir, 'chunk.3')
      ]
      const chunkDigest1 = await core.checksumFile(chunkFiles[0], 'sha1')
      assert.equal(chunkDigest1, 'sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5')
      const chunkDigest2 = await core.checksumFile(chunkFiles[1], 'sha1')
      assert.equal(chunkDigest2, 'sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603')
      const chunkDigest3 = await core.checksumFile(chunkFiles[2], 'sha1')
      assert.equal(chunkDigest3, 'sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576')
      // test reassembling the file again
      const outfile = path.join(outdir, 'lorem-ipsum.txt')
      await core.assembleChunks(chunkFiles, outfile)
      const chunkDigestN = await core.checksumFile(outfile, 'sha1')
      assert.equal(chunkDigestN, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
    })

    it('should create an encrypted pack with one chunk', async function () {
      const { master1, master2 } = core.generateMasterKeys()
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 3129
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunksEncrypted(chunks, packfile, master1, master2)
      assert.equal(digest, 'sha256-5c5ed2868c12085f9ef1ad33cd8e94f806a471eb593c5debe3f8410379739da9')
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackChunksEncrypted(packfile, 'chunk', outdir, master1, master2)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.equal(entries.length, 1, 'one file unpacked')
      assert.isTrue(entries[0].isFile())
      assert.equal(entries[0].name, 'chunk.1')
      const chunkDigest = await core.checksumFile(path.join(outdir, 'chunk.1'), 'sha1')
      assert.equal(chunkDigest, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
    })

    it('should create an encrypted pack with multiple chunks', async function () {
      const { master1, master2 } = core.generateMasterKeys()
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 1000
        },
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 1000,
          length: 1000
        },
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 2000,
          length: 1129
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunksEncrypted(chunks, packfile, master1, master2)
      assert.equal(digest, 'sha256-66dd6914eaa84902d74109685af95a6651947a6d34aef0fec385b6092007bf95')
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackChunksEncrypted(packfile, 'chunk', outdir, master1, master2)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 3, 'three chunks unpacked')
      assert.isTrue(entries.includes('chunk.1'))
      assert.isTrue(entries.includes('chunk.2'))
      assert.isTrue(entries.includes('chunk.3'))
      const chunkDigest1 = await core.checksumFile(path.join(outdir, 'chunk.1'), 'sha1')
      assert.equal(chunkDigest1, 'sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5')
      const chunkDigest2 = await core.checksumFile(path.join(outdir, 'chunk.2'), 'sha1')
      assert.equal(chunkDigest2, 'sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603')
      const chunkDigest3 = await core.checksumFile(path.join(outdir, 'chunk.3'), 'sha1')
      assert.equal(chunkDigest3, 'sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576')
    })

    it('should create an encrypted pack for multiple files', async function () {
      // two different files, and one larger than the stream buffer size
      const { master1, master2 } = core.generateMasterKeys()
      const chunks = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 3129
        },
        {
          path: './test/fixtures/SekienAkashita.jpg',
          offset: 0,
          length: 109466
        }
      ]
      const packfile = tmp.fileSync().name
      const digest = await core.packChunksEncrypted(chunks, packfile, master1, master2)
      assert.equal(digest, 'sha256-5b1d0c1cb9e47828e0bb84ba489f1cf1cab0db63bb07f2baaaacbfd63c12fc60')
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackChunksEncrypted(packfile, 'chunk', outdir, master1, master2)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 2, 'two chunks unpacked')
      assert.isTrue(entries.includes('chunk.1'))
      assert.isTrue(entries.includes('chunk.2'))
      const chunkDigest1 = await core.checksumFile(path.join(outdir, 'chunk.1'), 'sha1')
      assert.equal(chunkDigest1, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
      const chunkDigest2 = await core.checksumFile(path.join(outdir, 'chunk.2'), 'sha1')
      assert.equal(chunkDigest2, 'sha1-4c009e44fe5794df0b1f828f2a8c868e66644964')
    })
  })

  describe('file chunking', function () {
    it('should find chunk boundaries', async function () {
      const infile = './test/fixtures/SekienAkashita.jpg'
      const results = await core.findFileChunks(infile, 32768)
      assert.equal(results.length, 3)
      assert.equal(results[0].hash.toString('hex'), '5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d')
      assert.equal(results[0].offset, 0)
      assert.equal(results[0].size, 32857)
      assert.equal(results[1].hash.toString('hex'), '13f6a4c6d42df2b76c138c13e86e1379c203445055c2b5f043a5f6c291fa520d')
      assert.equal(results[1].offset, 32857)
      assert.equal(results[1].size, 16408)
      assert.equal(results[2].hash.toString('hex'), '0fe7305ba21a5a5ca9f89962c5a6f3e29cd3e2b36f00e565858e0012e5f8df36')
      assert.equal(results[2].offset, 49265)
      assert.equal(results[2].size, 60201)
    })
  })
})
