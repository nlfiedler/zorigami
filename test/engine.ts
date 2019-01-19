//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as chai from 'chai'
import * as fx from 'fs-extra'
import * as tmp from 'tmp'
const xattr = require('fs-xattr')
import * as core from '../src/core'
import * as database from '../src/database'
import * as engine from '../src/engine'
import * as store from '../src/store'
import * as local from '../src/store/local'

const assert = chai.assert

describe('Engine Functionality', function () {
  describe('basic encryption', function () {
    it('should generate master keys and save to database', async function () {
      await database.clearDatabase()
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

  describe('basic snapshots', function () {
    it('should record a snapshot to database', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fx.copySync('test/fixtures/lorem-ipsum.txt', path.join(basepath, 'lorem-ipsum.txt'))
      // take a snapshot of the test data
      const snapSha1 = await engine.takeSnapshot(basepath)
      const snapshot1 = await database.getSnapshot(snapSha1)
      assert.property(snapshot1, 'parent', 'snapshot has parent property')
      assert.equal(snapshot1.parent, engine.NULL_SHA1, 'first snapshot parent is 0')
      // tree should have entries with user and group fields
      const tree1 = await database.getTree(snapshot1.tree)
      assert.isTrue(tree1.entries.every((e: engine.TreeEntry) => e.user && e.group))
      // make a change to the data set
      fx.copySync('test/fixtures/SekienAkashita.jpg', path.join(basepath, 'SekienAkashita.jpg'))
      xattr.setSync(path.join(basepath, 'SekienAkashita.jpg'), 'me.fiedlers.test', 'foobar')
      // take another snapshot
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const snapshot2 = await database.getSnapshot(snapSha2)
      assert.equal(snapshot2.parent, snapSha1, 'second snapshot parent is first snapshot')
      assert.notEqual(snapSha2, snapSha1, 'created a different snapshot')
      assert.notEqual(snapshot2.tree, snapshot1.tree, 'created a different tree')
      // compute the differences
      const changes = await collectChanges(engine.findChangedFiles(snapSha1, snapSha2))
      // should see new file record
      assert.equal(
        changes.get('SekienAkashita.jpg'),
        'sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed',
        'changed files includes SekienAkashita.jpg'
      )
      // ensure extended attributes are stored in database
      const tree2 = await database.getTree(snapshot2.tree)
      const entryWithAttr = tree2.entries.find((e: engine.TreeEntry) => {
        return e.xattrs && e.xattrs.length && e.xattrs[0].name
      })
      assert.isDefined(entryWithAttr)
      const extattr = await database.getExtAttr(entryWithAttr.xattrs[0].digest)
      assert.isDefined(extattr)
      assert.instanceOf(extattr, Buffer, 'extended attribute is a buffer')
    })

    it('should detect differences with mixed ordering', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fs.mkdirSync(path.join(basepath, 'ccc'))
      fs.mkdirSync(path.join(basepath, 'mmm'))
      fs.mkdirSync(path.join(basepath, 'yyy'))
      fs.writeFileSync(path.join(basepath, 'ccc', 'ccc.txt'), 'cat climbing comfy chairs')
      fs.writeFileSync(path.join(basepath, 'mmm', 'mmm.txt'), 'morose monkey munching muffins')
      fs.writeFileSync(path.join(basepath, 'yyy', 'yyy.txt'), 'yellow yak yodeling')
      const snapSha1 = await engine.takeSnapshot(basepath)
      // add new files, change one file
      fs.mkdirSync(path.join(basepath, 'bbb'))
      fs.mkdirSync(path.join(basepath, 'nnn'))
      fs.mkdirSync(path.join(basepath, 'zzz'))
      fs.writeFileSync(path.join(basepath, 'bbb', 'bbb.txt'), 'baby baboons bathing')
      fs.writeFileSync(path.join(basepath, 'mmm', 'mmm.txt'), 'many mumbling mice in the moonlight')
      fs.writeFileSync(path.join(basepath, 'nnn', 'nnn.txt'), 'nice neanderthals noodling')
      fs.writeFileSync(path.join(basepath, 'zzz', 'zzz.txt'), 'zebras riding on a zephyr')
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const changes1 = await collectChanges(engine.findChangedFiles(snapSha1, snapSha2))
      assert.equal(changes1.size, 4, '4 changed files')
      assert.isTrue(changes1.has('bbb/bbb.txt'), 'bbb.txt has changed')
      assert.isTrue(changes1.has('mmm/mmm.txt'), 'mmm.txt has changed')
      assert.isTrue(changes1.has('nnn/nnn.txt'), 'nnn.txt has changed')
      assert.isTrue(changes1.has('zzz/zzz.txt'), 'zzz.txt has changed')
      // remove some files, change another
      fs.unlinkSync(path.join(basepath, 'bbb', 'bbb.txt'))
      fs.unlinkSync(path.join(basepath, 'yyy', 'yyy.txt'))
      fs.writeFileSync(path.join(basepath, 'zzz', 'zzz.txt'), 'zippy zip ties zooming')
      const snapSha3 = await engine.takeSnapshot(basepath, snapSha2)
      const changes2 = await collectChanges(engine.findChangedFiles(snapSha2, snapSha3))
      assert.equal(changes2.size, 1, '1 changed file')
      assert.isTrue(changes2.has('zzz/zzz.txt'), 'zzz.txt has changed')
    })

    it('should detect entry type changes', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fs.mkdirSync(path.join(basepath, 'mmm'))
      fs.writeFileSync(path.join(basepath, 'mmm', 'mmm.txt'), 'morose monkey munching muffins')
      fs.writeFileSync(path.join(basepath, 'ccc'), 'cat climbing comfy chairs')
      const snapSha1 = await engine.takeSnapshot(basepath)
      // change files to dirs and vice versa
      fx.removeSync(path.join(basepath, 'mmm'))
      fs.writeFileSync(path.join(basepath, 'mmm'), 'many mumbling mice in the moonlight')
      fs.unlinkSync(path.join(basepath, 'ccc'))
      fs.mkdirSync(path.join(basepath, 'ccc'))
      fs.writeFileSync(path.join(basepath, 'ccc', 'ccc.txt'), 'catastrophic catastrophes')
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const changes1 = await collectChanges(engine.findChangedFiles(snapSha1, snapSha2))
      assert.equal(changes1.size, 2, '2 changed files')
      assert.isTrue(changes1.has('mmm'), 'mmm has changed')
      assert.isTrue(changes1.has('ccc/ccc.txt'), 'ccc.txt has changed')
    })
  })

  describe('snapshots and symbolic links', function () {
    it('should encode symbolic links in the tree', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fs.writeFileSync(path.join(basepath, 'mmm.txt'), 'morose monkey munching muffins')
      fs.symlinkSync('mmm.txt', path.join(basepath, 'linky'))
      const snapSha1 = await engine.takeSnapshot(basepath)
      const snapshot1 = await database.getSnapshot(snapSha1)
      const tree1 = await database.getTree(snapshot1.tree)
      assert.isTrue(tree1.entries.some((e: engine.TreeEntry) => e.reference === 'bW1tLnR4dA=='))
    })

    it('should track links becoming files/dirs', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fs.writeFileSync(path.join(basepath, 'mmm.txt'), 'morose monkey munching muffins')
      fs.symlinkSync('mmm.txt', path.join(basepath, 'bbb'))
      fs.symlinkSync('mmm.txt', path.join(basepath, 'ccc'))
      const snapSha1 = await engine.takeSnapshot(basepath)
      // replace the links with files and directories
      fs.unlinkSync(path.join(basepath, 'bbb'))
      fs.writeFileSync(path.join(basepath, 'bbb'), 'bored baby baboons bathing')
      fs.unlinkSync(path.join(basepath, 'ccc'))
      fs.mkdirSync(path.join(basepath, 'ccc'))
      fs.writeFileSync(path.join(basepath, 'ccc', 'ccc.txt'), 'catastrophic catastrophes')
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const changes1 = await collectChanges(engine.findChangedFiles(snapSha1, snapSha2))
      assert.equal(changes1.size, 2, '2 changed files')
      assert.isTrue(changes1.has('bbb'), 'bbb has changed')
      assert.isTrue(changes1.has('ccc/ccc.txt'), 'ccc.txt has changed')
    })

    it('should ignore files/dirs becoming links', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fs.writeFileSync(path.join(basepath, 'bbb'), 'bored baby baboons bathing')
      fs.mkdirSync(path.join(basepath, 'ccc'))
      fs.writeFileSync(path.join(basepath, 'ccc', 'ccc.txt'), 'cuddling cute cucumbers')
      const snapSha1 = await engine.takeSnapshot(basepath)
      // replace the files and directories with links
      fs.writeFileSync(path.join(basepath, 'mmm.txt'), 'morose monkey munching muffins')
      fs.unlinkSync(path.join(basepath, 'bbb'))
      fs.symlinkSync('mmm.txt', path.join(basepath, 'bbb'))
      fx.removeSync(path.join(basepath, 'ccc'))
      fs.symlinkSync('mmm.txt', path.join(basepath, 'ccc'))
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const changes1 = await collectChanges(engine.findChangedFiles(snapSha1, snapSha2))
      assert.equal(changes1.size, 1, '1 changed files')
      assert.isTrue(changes1.has('mmm.txt'), 'mmm.txt has changed')
    })
  })

  describe('basic backup', function () {
    it('should produce pack files for initial backup', async function () {
      await database.clearDatabase()
      const basepath = 'test/tmp/fixtures'
      fx.removeSync(basepath)
      fx.ensureDirSync(basepath)
      fx.copySync('test/fixtures/lorem-ipsum.txt', path.join(basepath, 'lorem-ipsum.txt'))
      // take a snapshot of the test data
      const uniqId = core.generateUniqueId('charlie', 'localhost')
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const packdir = 'test/tmp/packs'
      fx.removeSync(packdir)
      const workdir = 'test/tmp/workspace'
      fx.removeSync(workdir)
      const localStore = new local.LocalStore(packdir)
      store.registerStore('local', localStore)
      const dataset = {
        uniqueId: uniqId,
        basepath,
        latest: engine.NULL_SHA1,
        workspace: workdir,
        packSize: 65536,
        store: 'local'
      }
      const snapSha1 = await engine.performBackup(dataset, keys)
      // verify bucket and object exist
      const buckets = await store.collectBuckets(store.listBuckets('local'))
      assert.lengthOf(buckets, 1, 'returned one bucket')
      assert.typeOf(buckets[0], 'string', 'bucket is a string')
      const objects = await store.collectObjects(store.listObjects('local', buckets[0]))
      assert.lengthOf(objects, 1, 'returned one object')
      assert.typeOf(objects[0], 'string', 'object is a string')
      dataset.latest = snapSha1
      await database.updateConfiguration(dataset)
    })

    it('should produce pack files for second backup', async function () {
      const basepath = 'test/tmp/fixtures'
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // add another file
      fx.copySync('test/fixtures/SekienAkashita.jpg', path.join(basepath, 'SekienAkashita.jpg'))
      // perform another backup
      const snapSha1 = await engine.performBackup(dataset, keys)
      // verify new pack files exist
      const buckets = await store.collectBuckets(store.listBuckets('local'))
      assert.lengthOf(buckets, 2, 'returned two buckets')
      buckets.sort()
      let objects = await store.collectObjects(store.listObjects('local', buckets[0]))
      assert.lengthOf(objects, 1, 'returned one object')
      objects = await store.collectObjects(store.listObjects('local', buckets[1]))
      assert.lengthOf(objects, 2, 'returned two objects')
      dataset.latest = snapSha1
      await database.updateConfiguration(dataset)
    })

    it('should ignore duplicate files', async function () {
      const basepath = 'test/tmp/fixtures'
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // add another file
      fx.copySync('test/fixtures/lorem-ipsum.txt', path.join(basepath, 'lorem-copy.txt'))
      // perform another backup
      const snapSha1 = await engine.performBackup(dataset, keys)
      // verify no new buckets have been created
      const buckets = await store.collectBuckets(store.listBuckets('local'))
      assert.lengthOf(buckets, 2, 'returned two buckets')
      dataset.latest = snapSha1
      await database.updateConfiguration(dataset)
    })

    it('should ignore duplicate chunks within large files', async function () {
      const basepath = 'test/tmp/fixtures'
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // count number of existing chunks in database
      let chunks = await database.countChunks()
      assert.equal(chunks, 7)
      // add another file
      await core.copyFileWithPrefix(
        Buffer.from('mary had a little lamb'),
        'test/fixtures/SekienAkashita.jpg',
        path.join(basepath, 'SekienShifted.jpg')
      )
      // perform another backup
      const snapSha1 = await engine.performBackup(dataset, keys)
      // verify new bucket and objects have been created
      const buckets = await store.collectBuckets(store.listBuckets('local'))
      assert.lengthOf(buckets, 3, 'returned three buckets')
      buckets.sort()
      let objects = await store.collectObjects(store.listObjects('local', buckets[0]))
      assert.lengthOf(objects, 1, 'returned one object')
      objects = await store.collectObjects(store.listObjects('local', buckets[1]))
      assert.lengthOf(objects, 2, 'returned two objects')
      objects = await store.collectObjects(store.listObjects('local', buckets[2]))
      assert.lengthOf(objects, 1, 'returned one object')
      dataset.latest = snapSha1
      await database.updateConfiguration(dataset)
      // ensure only one additional chunk in the database
      chunks = await database.countChunks()
      assert.equal(chunks, 8)
      // verify that only the new chunk was written to the pack file
      const packfile = tmp.fileSync().name
      await store.waitForDone(store.retrievePack('local', buckets[2], objects[0], packfile))
      const outdir = tmp.dirSync().name
      await core.unpackChunksEncrypted(packfile, outdir, keys)
      const entries = fs.readdirSync(outdir, { withFileTypes: true })
      assert.lengthOf(entries, 1, 'one chunk unpacked')
      assert.equal(entries[0].name, 'sha256-7b5b11492f7ea00907fa9afcdacb2c92ec20f3879c6bce1f11a7cff8e1fa34a1')
      const chunkDigest = await core.checksumFile(
        path.join(outdir, 'sha256-7b5b11492f7ea00907fa9afcdacb2c92ec20f3879c6bce1f11a7cff8e1fa34a1'), 'sha256')
      assert.equal(chunkDigest, 'sha256-7b5b11492f7ea00907fa9afcdacb2c92ec20f3879c6bce1f11a7cff8e1fa34a1')
    })

    it('should restore a single chunk file', async function () {
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // restore the lorem-ipsum.txt file using its sha256
      const outfile = tmp.fileSync().name
      const checksum = 'sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f'
      await engine.restoreFile(dataset, keys, checksum, outfile)
      const actual = await core.checksumFile(outfile, 'sha256')
      assert.equal(actual, checksum, 'restored lorem-ipsum.txt file')
    })

    it('should restore a file with multiple chunks', async function () {
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // restore the SekienAkashita.jpg file using its sha256
      const outfile = tmp.fileSync().name
      const checksum = 'sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed'
      await engine.restoreFile(dataset, keys, checksum, outfile)
      const actual = await core.checksumFile(outfile, 'sha256')
      assert.equal(actual, checksum, 'restored SekienAkashita.jpg file')
    })

    it('should restore a file with chunks in different packs', async function () {
      const password = 'keyboard cat'
      const keys = await engine.getMasterKeys(password)
      const dataset = await database.getConfiguration()
      // restore the SekienShifted.jpg file using its sha256
      const outfile = tmp.fileSync().name
      const checksum = 'sha256-b2c67e90a01f5d7aca48835b8ad8f0902ef03288aa4083e742bccbd96d8590a4'
      await engine.restoreFile(dataset, keys, checksum, outfile)
      const actual = await core.checksumFile(outfile, 'sha256')
      assert.equal(actual, checksum, 'restored SekienShifted.jpg file')
    })
  })
})

async function collectChanges(generator: AsyncIterableIterator<[string, string]>): Promise<Map<string, string>> {
  const results: Map<string, string> = new Map()
  for await (let [filepath, filesha] of generator) {
    results.set(filepath, filesha)
  }
  return results
}
