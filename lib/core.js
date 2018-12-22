//
// Copyright (c) 2018 Nathan Fiedler
//
const crypto = require('crypto')
const uuidv5 = require('uuid/v5')
const ULID = require('ulid')

/**
 * Generate a type 5 UUID based on the given values.
 *
 * @param {string} username name of the user performing the backup.
 * @param {string} hostname name of the computer being backed up.
 */
function generateUniqueId (username, hostname) {
  return uuidv5(username + ':' + hostname, uuidv5.URL)
}

/**
 * Generate a suitable bucket name, using a ULID and the given UUID.
 *
 * @param {string} uniqueId unique identifier, as from generateUniqueId().
 */
function generateBucketName (uniqueId) {
  return ULID.ulid().toLowerCase() + uniqueId.replace(/-/g, '')
}

/**
 * Generate the two 32 byte master keys, named master1 and master2.
 */
function generateMasterKeys () {
  const master1 = Buffer.alloc(32)
  crypto.randomFillSync(master1)
  const master2 = Buffer.alloc(32)
  crypto.randomFillSync(master2)
  return { master1, master2 }
}

/**
 * Encrypt the given plain text using the key and initialization vector.
 *
 * @param {Buffer} plaintext data to be encrypted.
 * @param {Buffer} key encryption key.
 * @param {Buffer} iv initialization vector.
 * @returns {Buffer} cipher text.
 */
function encrypt (plaintext, key, iv) {
  const cipher = crypto.createCipheriv('aes-256-cbc', key, iv)
  const enc1 = cipher.update(plaintext)
  const enc2 = cipher.final()
  return Buffer.concat([enc1, enc2])
}

/**
 * Decrypt the given cipher text using the key and initialization vector.
 *
 * @param {Buffer} ciphertext data to be decrypted.
 * @param {Buffer} key encryption key.
 * @param {Buffer} iv initialization vector.
 * @returns {Buffer} decrypted data.
 */
function decrypt (ciphertext, key, iv) {
  const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv)
  const dec1 = decipher.update(ciphertext)
  const dec2 = decipher.final()
  return Buffer.concat([dec1, dec2])
}

/**
 * Hash the given user password with the salt value.
 *
 * @param {string} password user password.
 * @param {Buffer} salt random salt value.
 */
function hashPassword (password, salt) {
  return crypto.pbkdf2Sync(password, salt, 160000, 32, 'sha256')
}

/**
 * Compute the HMAC-SHA256 of the given data.
 *
 * @param {Buffer} key hashed user password.
 * @param {Buffer} data buffer to be digested.
 */
function computeHmac (key, data) {
  const hmac = crypto.createHmac('sha256', key)
  hmac.update(data)
  return hmac.digest()
}

/**
 * Use the user password and the two master keys to produce the encryption data
 * that will be stored in the database. This generates a random salt and
 * initialization vector, then derives a key from the user password and salt,
 * encrypts the master keys, computes the HMAC, and returns the results.
 *
 * @param {string} password user-provided master password.
 * @param {Buffer} master1 random master key #1.
 * @param {Buffer} master2 random master key #2.
 * @returns {object} contains salt, iv, hmac, and encrypted master keys.
 */
function newMasterEncryptionData (password, master1, master2) {
  const salt = Buffer.alloc(16)
  crypto.randomFillSync(salt)
  const iv = Buffer.alloc(16)
  crypto.randomFillSync(iv)
  const key = hashPassword(password, salt)
  const masters = Buffer.concat([master1, master2])
  const encrypted = encrypt(masters, key, iv)
  const hmac = computeHmac(key, Buffer.concat([iv, encrypted]))
  return { salt, iv, hmac, encrypted }
}

/**
 * Decrypt the master keys from the data originally produced by
 * newMasterEncryptionData().
 *
 * @param {Buffer} salt random salt value.
 * @param {string} password user password.
 * @param {Buffer} iv initialization vector.
 * @param {Buffer} encrypted encrypted master passwords.
 * @param {Buffer} hmac HMAC-SHA256.
 */
function decryptMasterKeys (salt, password, iv, encrypted, hmac) {
  const key = hashPassword(password, salt)
  const hmac2 = computeHmac(key, Buffer.concat([iv, encrypted]))
  if (hmac.compare(hmac2) !== 0) {
    throw new Error('HMAC does not match records')
  }
  const plaintext = decrypt(encrypted, key, iv)
  const middle = plaintext.length / 2
  const master1 = plaintext.slice(0, middle)
  const master2 = plaintext.slice(middle)
  return { master1, master2 }
}

module.exports = {
  generateBucketName,
  generateUniqueId,
  generateMasterKeys,
  newMasterEncryptionData,
  decryptMasterKeys
}
