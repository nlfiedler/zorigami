//
// Copyright (c) 2018 Nathan Fiedler
//
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

module.exports = {
  generateBucketName,
  generateUniqueId
}
