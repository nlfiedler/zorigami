//
// Copyright (c) 2018 Nathan Fiedler
//
import * as core from './core'
import * as database from './database'

/**
 * Get the master keys for encrypting the pack files. They will be loaded from
 * the database, or generated if they are missing.
 *
 * @param password user master password.
 * @returns the master keys.
 */
export async function getMasterKeys(password: string): Promise<core.MasterKeys> {
  let encryptDoc = await database.fetchDocument('encryption')
  let keys = null
  if (encryptDoc === null) {
    keys = core.generateMasterKeys()
    const data = core.newMasterEncryptionData(password, keys)
    encryptDoc = {
      _id: 'encryption',
      salt: data.salt,
      iv: data.iv,
      hmac: data.hmac,
      keys: data.encrypted
    }
    await database.updateDocument(encryptDoc)
  } else {
    const data = {
      salt: encryptDoc.salt,
      iv: encryptDoc.iv,
      hmac: encryptDoc.hmac,
      encrypted: encryptDoc.keys
    }
    keys = core.decryptMasterKeys(data, password)
  }
  return keys
}
