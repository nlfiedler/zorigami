//
// Copyright (c) 2018 Nathan Fiedler
//
import * as chai from 'chai'
import * as database from '../src/database'

const assert = chai.assert

//
// Give the database a chance to initialize the database asynchronously.
// A timeout of zero is not sufficient, so this timing is fragile.
//
setTimeout(function () {
  describe('Database Functionality', function () {
    before(async function () {
      // PouchDB 7.0 takes more than 2 seconds to prime the index
      this.timeout(10000)
      await database.clearDatabase()
    })

    describe('basic operation', function () {
      it('should insert and fetch a document', async function () {
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
        const update = Object.assign({}, { friend: 'Homura' }, input)
        wasUpdate = await database.updateDocument(update)
        assert.isTrue(wasUpdate, 'updated an old document')
        result = await database.fetchDocument('cafebabe')
        assert.equal(result.friend, 'Homura')
      })
    })

    describe('database indices', function () {
      it('should count different record types', async function () {
        const records = [
          { '_id': 'file/cafebabe', 'name': 'readme.txt' },
          { '_id': 'file/babecafe', 'name': 'readyou.txt' },
          { '_id': 'file/facebabe', 'name': 'readus.txt' },
          { '_id': 'file/babeface', 'name': 'readthem.txt' },
          { '_id': 'chunk/babeface', 'name': 'readthem.txt' },
          { '_id': 'chunk/cafebabe', 'name': 'readfrom.txt' },
          { '_id': 'chunk/deadbeef', 'name': 'readahead.txt' },
          { '_id': 'tree/feedface', 'name': 'evergreen.txt' },
          { '_id': 'tree/cafebabe', 'name': 'maple.txt' },
          { '_id': 'tree/cafed00d', 'name': 'birch.txt' }
        ]
        for (let doc of records) {
          await database.updateDocument(doc)
        }
        const chunks = await database.countChunks()
        assert.equal(chunks, 3)
      })
    })
  })

  run()
}, 500)
