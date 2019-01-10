//
// Copyright (c) 2018 Nathan Fiedler
//
import * as fs from 'fs'
import * as path from 'path'
import * as chai from 'chai'
import * as fx from 'fs-extra'
import * as database from '../src/database'
import * as engine from '../src/engine'

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
      // make a change to the data set
      fx.copySync('test/fixtures/SekienAkashita.jpg', path.join(basepath, 'SekienAkashita.jpg'))
      // take another snapshot
      const snapSha2 = await engine.takeSnapshot(basepath, snapSha1)
      const snapshot2 = await database.getSnapshot(snapSha2)
      assert.equal(snapshot2.parent, snapSha1, 'second snapshot parent is first snapshot')
      assert.notEqual(snapSha2, snapSha1, 'created a different snapshot')
      assert.notEqual(snapshot2.tree, snapshot1.tree, 'created a different tree')
      // compute the differences
      const changes = await engine.findChangedFiles(snapSha1, snapSha2)
      // should see new file record
      assert.equal(
        changes.get('SekienAkashita.jpg'),
        'sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed',
        'changed files includes SekienAkashita.jpg'
      )
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
      const changes1 = await engine.findChangedFiles(snapSha1, snapSha2)
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
      const changes2 = await engine.findChangedFiles(snapSha2, snapSha3)
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
      const changes1 = await engine.findChangedFiles(snapSha1, snapSha2)
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
      const changes1 = await engine.findChangedFiles(snapSha1, snapSha2)
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
      const changes1 = await engine.findChangedFiles(snapSha1, snapSha2)
      assert.equal(changes1.size, 1, '1 changed files')
      assert.isTrue(changes1.has('mmm.txt'), 'mmm.txt has changed')
    })
  })
})
