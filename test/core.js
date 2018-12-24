//
// Copyright (c) 2018 Nathan Fiedler
//
const { assert } = require('chai')
const { describe, it } = require('mocha')
const crypto = require('crypto')
const core = require('../lib/core')
const fs = require('fs')
const tmp = require('tmp')

describe('Core Functionality', function () {
  describe('unique identifier', function () {
    it('should return a UUIDv5 for user and host', function () {
      const uniqId = core.generateUniqueId('charlie', 'localhost')
      assert.equal(uniqId, '747267d5-6e70-5711-8a9a-a40c24c1730f')
    })
  })

  describe('bucket name', function () {
    it('should generate a 58 character string', function () {
      const uniqId = core.generateUniqueId('charlie', 'localhost')
      const bucket = core.generateBucketName(uniqId)
      assert.equal(bucket.length, 58)
    })
  })

  describe('master passwords', function () {
    it('should encrypt and decrypt successfully', function () {
      const password = 'keyboard cat'
      const expected = core.generateMasterKeys()
      const { salt, iv, hmac, encrypted } =
        core.newMasterEncryptionData(password, expected.master1, expected.master2)
      const actual = core.decryptMasterKeys(salt, password, iv, encrypted, hmac)
      assert.equal(expected.master1.compare(actual.master1), 0)
      assert.equal(expected.master2.compare(actual.master2), 0)
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
      assert.notEqual(originalBuf.compare(encryptBuf), 0,
        'encrypted not equal to original')
      const decrypted = tmp.fileSync().name
      await core.decryptFile(encrypted, decrypted, key, iv)
      const decryptBuf = fs.readFileSync(decrypted)
      assert.equal(originalBuf.compare(decryptBuf), 0,
        'original and decrypted match')
    })
  })

  describe('pack files', function () {
    it('should create files', async function () {
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
      assert.isTrue(results.offsets.has('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'))
      assert.equal(results.offsets.get('sha1-b14c4909c3fce2483cd54b328ada88f5ef5e8f96'), 0)
    })
  })
})
