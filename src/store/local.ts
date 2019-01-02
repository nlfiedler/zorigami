//
// Copyright (c) 2018 Nathan Fiedler
//
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
        const buckdir = path.join(this.basepath, bucket)
        fx.ensureDirSync(buckdir)
        const destfile = path.join(buckdir, object)
        fx.moveSync(packfile, destfile)
    }

    retrievePack(bucket: string, object: string, outfile: string): void {
        const buckdir = path.join(this.basepath, bucket)
        fx.ensureDirSync(buckdir)
        const packfile = path.join(buckdir, object)
        fx.copySync(packfile, outfile)
    }
}
