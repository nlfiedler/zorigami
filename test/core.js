//
// Copyright (c) 2018 Nathan Fiedler
//
const { assert } = require('chai')
const { describe, it } = require('mocha')
const core = require('../lib/core')

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
})
