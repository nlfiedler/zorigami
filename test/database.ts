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
setTimeout(() => {
  describe('Database Functionality', () => {
    before(async () => {
      await database.clearDatabase()
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
        const update = Object.assign({}, {friend: 'Homura'}, input)
        wasUpdate = await database.updateDocument(update)
        assert.isTrue(wasUpdate, 'updated an old document')
        result = await database.fetchDocument('cafebabe')
        assert.equal(result.friend, 'Homura')
      })
    })
  })

  run()
}, 500)
