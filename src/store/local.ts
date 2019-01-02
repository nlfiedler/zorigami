//
// Copyright (c) 2018 Nathan Fiedler
//
import fs = require('fs')
import path = require('path')
import fx = require('fs-extra')

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
      throw new Error('missing file: ' + packfile)
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
      throw new Error('missing pack file: ' + packfile)
    }
    fx.copySync(packfile, outfile)
  }
}
