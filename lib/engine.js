//
// Copyright (c) 2018 Nathan Fiedler
//
const core = require('lib/core')
const database = require('lib/database')

/**
 * Get the master keys for encrypting the pack files. They will be loaded from
 * the database, or generated if they are missing.
 *
 * @param {string} password user master password.
 * @returns {Object} contains `master1` and `master2` keys.
 */
async function getMasterKeys (password) {
  let encryptDoc = await database.fetchDocument('encryption')
  let keys = null
  if (encryptDoc === null) {
    keys = core.generateMasterKeys()
    const { salt, iv, hmac, encrypted } = core.newMasterEncryptionData(
      password, keys.master1, keys.master2)
    encryptDoc = {
      _id: 'encryption',
      salt,
      iv,
      hmac,
      keys: encrypted
    }
    await database.updateDocument(encryptDoc)
  } else {
    keys = core.decryptMasterKeys(
      encryptDoc.salt,
      password,
      encryptDoc.iv,
      encryptDoc.keys,
      encryptDoc.hmac
    )
  }
  return keys
}

module.exports = {
  getMasterKeys
}
