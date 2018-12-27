//
// Copyright (c) 2018 Nathan Fiedler
//
const { assert } = require('chai')
const { describe, it } = require('mocha')
const crypto = require('crypto')
const core = require('../lib/core')
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
    it('should create a pack with one part', async function () {
      const parts = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 3129
        }
      ]
      const packfile = tmp.fileSync().name
      const results = await core.packParts(parts, packfile)
      assert.equal(results.hash, 'sha256-20bf4683a3bfd2eac935a16cf0745759718971a5f12fe29befa39ae0a22ac6c8')
      assert.equal(results.offsets.size, 1)
      assert.equal(results.offsets.get('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'), 0)
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackParts(packfile, outdir)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.equal(entries.length, 1, 'one file unpacked')
      assert.isTrue(entries[0].isFile())
      assert.equal(entries[0].name, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
    })

    it('should create a pack with multiple parts', async function () {
      const parts = [
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
      const results = await core.packParts(parts, packfile)
      assert.equal(results.hash, 'sha256-23064d1275bb1d4c7fe749a3bc8f7f63538fea7d996ed1d995833ee788d575a4')
      assert.equal(results.offsets.size, 3)
      assert.equal(results.offsets.get('sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5'), 0)
      assert.equal(results.offsets.get('sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603'), 1)
      assert.equal(results.offsets.get('sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576'), 2)
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackParts(packfile, outdir)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 3, 'three files unpacked')
      assert.isTrue(entries.includes('sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5'))
      assert.isTrue(entries.includes('sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603'))
      assert.isTrue(entries.includes('sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576'))
    })

    it('should create an encrypted pack with one part', async function () {
      const { master1, master2 } = core.generateMasterKeys()
      const parts = [
        {
          path: './test/fixtures/lorem-ipsum.txt',
          offset: 0,
          length: 3129
        }
      ]
      const packfile = tmp.fileSync().name
      const results = await core.packPartsEncrypted(parts, packfile, master1, master2)
      assert.equal(results.hash, 'sha256-20bf4683a3bfd2eac935a16cf0745759718971a5f12fe29befa39ae0a22ac6c8')
      assert.equal(results.offsets.size, 1)
      assert.equal(results.offsets.get('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'), 0)
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackPartsEncrypted(packfile, outdir, master1, master2)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.equal(entries.length, 1, 'one file unpacked')
      assert.isTrue(entries[0].isFile())
      assert.equal(entries[0].name, 'sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96')
      // compare file contents for extra assurance
      const originalBuf = fs.readFileSync(parts[0].path)
      const partfileBuf = fs.readFileSync(path.join(outdir, entries[0].name))
      assert.isTrue(originalBuf.equals(partfileBuf),
        'decrytped part file equal to original')
    })

    it('should create an encrypted pack with multiple parts', async function () {
      const { master1, master2 } = core.generateMasterKeys()
      const parts = [
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
      const results = await core.packPartsEncrypted(parts, packfile, master1, master2)
      assert.equal(results.hash, 'sha256-23064d1275bb1d4c7fe749a3bc8f7f63538fea7d996ed1d995833ee788d575a4')
      assert.equal(results.offsets.size, 3)
      assert.equal(results.offsets.get('sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5'), 0)
      assert.equal(results.offsets.get('sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603'), 1)
      assert.equal(results.offsets.get('sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576'), 2)
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackPartsEncrypted(packfile, outdir, master1, master2)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 3, 'three files unpacked')
      assert.isTrue(entries.includes('sha1-824fdcb9fe191e98f0eba2bbb016f3cd95f236c5'))
      assert.isTrue(entries.includes('sha1-7bb96ad562d2b5e99c6d6b4ff87f7380609c5603'))
      assert.isTrue(entries.includes('sha1-418eacb05e0fea53ae7f889ab5aa6a95de049576'))
    })

    it('should create an encrypted pack for multiple files', async function () {
      // two different files, and one larger than the stream buffer size
      const { master1, master2 } = core.generateMasterKeys()
      const parts = [
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
      const results = await core.packPartsEncrypted(parts, packfile, master1, master2)
      assert.equal(results.hash, 'sha256-1be8d7e946e587f90efd45d65885dc9bc7ef922072f4dc75654d5d863e5b3e5d')
      assert.equal(results.offsets.size, 2)
      assert.equal(results.offsets.get('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'), 0)
      assert.equal(results.offsets.get('sha1-4c009e44fe5794df0b1f828f2a8c868e66644964'), 1)
      // verify unpacking
      const outdir = tmp.dirSync().name
      await core.unpackPartsEncrypted(packfile, outdir, master1, master2)
      const entries = fs.readdirSync(outdir)
      assert.equal(entries.length, 2, 'two files unpacked')
      assert.isTrue(entries.includes('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'))
      assert.isTrue(entries.includes('sha1-4c009e44fe5794df0b1f828f2a8c868e66644964'))
    })
  })
})
