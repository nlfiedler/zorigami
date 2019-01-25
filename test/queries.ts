//
// Copyright (c) 2018 Nathan Fiedler
//
import * as chai from 'chai'
import * as database from '../src/database'
import * as request from 'supertest'

const assert = chai.assert

// start the server
import app from '../src/app'

//
// Give the database a chance to initialize the database asynchronously.
// A timeout of zero is not sufficient, so this timing is fragile.
//
setTimeout(function () {
  describe('GraphQL queries', function () {
    before(async function () {
      await database.clearDatabase()
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
    })

    describe('object counts', function () {
      it('should return number of chunks', function (done) {
        request(app)
          .post('/graphql')
          .send({
            query: `query {
              chunkCount
            }`
          })
          .expect(200)
          .expect(res => {
            const result = res.body.data
            assert.equal(result.chunkCount, 3)
          })
          .end(function (err, res) {
            if (err) {
              return done(err)
            }
            done()
          })
      })

      it('should return number of files', function (done) {
        request(app)
          .post('/graphql')
          .send({
            query: `query {
              fileCount
            }`
          })
          .expect(200)
          .expect(res => {
            const result = res.body.data
            assert.equal(result.fileCount, 4)
          })
          .end(function (err, res) {
            if (err) {
              return done(err)
            }
            done()
          })
      })
    })
  })

  run()
}, 500)
