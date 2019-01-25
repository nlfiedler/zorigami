//
// Copyright (c) 2019 Nathan Fiedler
//
import * as database from '../database'

export const Query = {
  async chunkCount(obj: any, args: any, context: any, info: any) {
    return await database.countChunks()
  },

  async fileCount(obj: any, args: any, context: any, info: any) {
    return await database.countFiles()
  }
}
