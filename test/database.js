//
// Copyright (c) 2018 Nathan Fiedler
//
const { assert } = require('chai')
const { before, describe, it, run } = require('mocha')
const fs = require('fs-extra')
const config = require('config')

// clean up from previous test runs before starting the server
const dbPath = config.get('database.path')
fs.removeSync(dbPath)
const database = require('lib/database')

//
// Give the database a chance to initialize the database asynchronously.
// A timeout of zero is not sufficient, so this timing is fragile.
//
setTimeout(() => {
  describe('Database Functionality', () => {
    before(async () => {
      await database.initDatabase()
    })

    describe('basic operation', () => {
      it('should insert and fetch a document', async () => {
        let result = await database.fetchDocument('test1')
        assert.isNull(result, 'no document should return null')
        const input = {
          '_id': 'cafebabe',
          'name': 'Madoka',
          'gender': 'Female',
          'ability': 'Hope'
        }
        let wasUpdate = await database.updateDocument(input)
        assert.isFalse(wasUpdate, 'created a new document')
        input.friend = 'Homura'
        wasUpdate = await database.updateDocument(input)
        assert.isTrue(wasUpdate, 'updated an old document')
        result = await database.fetchDocument('cafebabe')
        assert.equal(result.friend, 'Homura')
      })
    })
  })

  run()
}, 500)
