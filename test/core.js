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
})
