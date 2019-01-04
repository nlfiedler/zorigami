//
// Copyright (c) 2018 Nathan Fiedler
//
import fs = require('fs')
import path = require('path')
import fx = require('fs-extra')
import verr = require('verror')

/**
 * Local disk implementation of the `Store` interface.
 */
export class LocalStore {
  readonly basepath: string

  /**
   * Construct a new instance of LocalStore.
   *
   * @param basepath location where pack files will be stored.
   */
  constructor(basepath: string) {
    this.basepath = basepath
  }

  storePack(packfile: string, bucket: string, object: string): void {
    if (!fs.existsSync(packfile)) {
      throw new verr.VError({
        name: 'IllegalArgumentError',
        info: {
          path: packfile
        }
      }, `missing pack file: ${packfile}`)
    }
    const buckdir = path.join(this.basepath, bucket)
    fx.ensureDirSync(buckdir)
    const destfile = path.join(buckdir, object)
    fx.moveSync(packfile, destfile)
  }

  retrievePack(bucket: string, object: string, outfile: string): void {
    const buckdir = path.join(this.basepath, bucket)
    fx.ensureDirSync(path.dirname(outfile))
    const packfile = path.join(buckdir, object)
    if (!fs.existsSync(packfile)) {
      throw new verr.VError({
        name: 'RuntimeError',
        info: {
          path: packfile
        }
      }, `missing object file: ${packfile}`)
    }
    fx.copySync(packfile, outfile)
  }

  listBuckets(): string[] {
    const entries = fs.readdirSync(this.basepath, { withFileTypes: true })
    const dirs = entries.filter((entry) => entry.isDirectory())
    return dirs.map((entry) => entry.name)
  }

  listObjects(bucket: string): string[] {
    const buckdir = path.join(this.basepath, bucket)
    if (!fs.existsSync(buckdir)) {
      throw new verr.VError({
        name: 'RuntimeError',
        info: {
          bucket: bucket
        }
      }, `no such bucket: ${bucket}`)
    }
    const entries = fs.readdirSync(buckdir, { withFileTypes: true })
    const files = entries.filter((entry) => entry.isFile())
    return files.map((entry) => entry.name)
  }
}
