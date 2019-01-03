//
// Copyright (c) 2018 Nathan Fiedler
//
import * as chai from 'chai'
import * as database from '../src/database'
import * as engine from '../src/engine'

const assert = chai.assert

//
// Give the database a chance to initialize the database asynchronously.
// A timeout of zero is not sufficient, so this timing is fragile.
//
setTimeout(function () {
  describe('Engine Functionality', function () {
    before(async function () {
      await database.clearDatabase()
    })

    describe('basic operation', function () {
      it('should generate master keys and save to database', async function () {
        // ensure the encryption record does not yet exist
        let result = await database.fetchDocument('encryption')
        assert.isNull(result, 'encryption record does not exist')
        const password = 'keyboard cat'
        // will generate keys if they are missing
        const keys1 = await engine.getMasterKeys(password)
        assert.property(keys1, 'master1', 'has master1 key')
        assert.property(keys1, 'master2', 'has master2 key')
        // a bit of white box testing, but check if db record exists now
        result = await database.fetchDocument('encryption')
        assert.isNotNull(result, 'encryption record now exists')
        // will return the same keys as the first time
        const keys2 = await engine.getMasterKeys(password)
        assert.isTrue(keys2.master1.equals(keys1.master1), 'master1 keys match')
        assert.isTrue(keys2.master2.equals(keys1.master2), 'master2 keys match')
      })
    })
  })

  run()
}, 500)
