//
// Copyright (c) 2018 Nathan Fiedler
//
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const PouchDB = require('pouchdb')
const fx = require('fs-extra')

const dbPath = '/tmp/filesdb/leveldb'
fx.ensureDirSync(dbPath)
const db = new PouchDB(dbPath)

//
// TODO: need to be resilient to i/o errors, such as EPERM
//
function walk (dir, cb) {
  const entries = fs.readdirSync(dir, { withFileTypes: true })
  const files = entries.filter((entry) => entry.isFile())
  // give all of the files in this directory in one batch
  cb(dir, files).then(() => {
    // process the next level after the promise resolves
    const dirs = entries.filter((entry) => entry.isDirectory())
    for (let entry of dirs) {
      walk(path.join(dir, entry.name), cb)
    }
  }).catch((err) => {
    console.error(err)
  })
}

function checksumFile (infile) {
  const input = fs.createReadStream(infile)
  const hash = crypto.createHash('sha256')
  return new Promise((resolve, reject) => {
    input.on('readable', () => {
      const data = input.read()
      if (data) {
        hash.update(data)
      } else {
        resolve(`sha256-${hash.digest('hex')}`)
      }
    })
    input.on('error', (err) => {
      input.destroy()
      reject(err)
    })
  })
}

let count = 0

walk('/Users/nfiedler', (dir, files) => {
  return Promise.all(files.map((file) => {
    return new Promise((resolve, reject) => {
      const filepath = path.join(dir, file.name)
      const stat = fs.statSync(filepath)
      checksumFile(filepath).then((value) => {
        const doc = {
          _id: value,
          name: filepath,
          size: stat.size
        }
        return db.put(doc)
      }).then((response) => {
        count++
        if (count % 100 === 0) {
          console.info(count)
        }
        resolve(count)
      }).catch((err) => {
        if (err.status !== 409) {
          reject(err)
        }
      })
    })
  }))
})
