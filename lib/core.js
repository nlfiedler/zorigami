//
// Copyright (c) 2018 Nathan Fiedler
//
const crypto = require('crypto')
const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const zlib = require('zlib')
const tmp = require('tmp')
const uuidv5 = require('uuid/v5')
const ULID = require('ulid')

const fopen = util.promisify(fs.open)
const fread = util.promisify(fs.read)
const fwrite = util.promisify(fs.write)

const PACK_HEADER_SIZE = 8
const BUFFER_SIZE = 262144

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
  const master1 = Buffer.allocUnsafe(32)
  crypto.randomFillSync(master1)
  const master2 = Buffer.allocUnsafe(32)
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
  const cipher = crypto.createCipheriv('aes-256-ctr', key, iv)
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
  const decipher = crypto.createDecipheriv('aes-256-ctr', key, iv)
  const dec1 = decipher.update(ciphertext)
  const dec2 = decipher.final()
  return Buffer.concat([dec1, dec2])
}

/**
 * Hash the given user password with the salt value using scrypt.
 *
 * @param {string} password user password.
 * @param {Buffer} salt random salt value.
 * @returns {Buffer} 32 byte key value.
 */
function hashPassword (password, salt) {
  return crypto.scryptSync(password, salt, 32)
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
  const salt = Buffer.allocUnsafe(16)
  crypto.randomFillSync(salt)
  const iv = Buffer.allocUnsafe(16)
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

/**
 * Encrypt a file.
 *
 * @param {string} infile path of input file.
 * @param {string} outfile path of output file.
 * @param {Buffer} key encryption key.
 * @param {Buffer} iv initialization vector.
 */
async function encryptFile (infile, outfile, key, iv) {
  const cipher = crypto.createCipheriv('aes-256-ctr', key, iv)
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err) => {
      input.destroy()
      output.destroy()
      reject(err)
    }
    input.on('error', (err) => cleanup(err))
    output.on('error', (err) => cleanup(err))
    output.on('finish', () => resolve())
    input.pipe(cipher).pipe(output)
  })
}

/**
 * Decrypt a file.
 *
 * @param {string} infile path of input file.
 * @param {string} outfile path of output file.
 * @param {Buffer} key encryption key.
 * @param {Buffer} iv initialization vector.
 */
async function decryptFile (infile, outfile, key, iv) {
  const cipher = crypto.createDecipheriv('aes-256-ctr', key, iv)
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err) => {
      input.destroy()
      output.destroy()
      reject(err)
    }
    input.on('error', (err) => cleanup(err))
    output.on('error', (err) => cleanup(err))
    output.on('finish', () => resolve())
    input.pipe(cipher).pipe(output)
  })
}

/**
 * Compress a file using GZip.
 *
 * @param {string} infile path of input file.
 * @param {string} outfile path of output file.
 */
async function compressFile (infile, outfile) {
  const input = fs.createReadStream(infile)
  const zip = zlib.createGzip()
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err) => {
      input.destroy()
      output.destroy()
      reject(err)
    }
    input.on('error', (err) => cleanup(err))
    output.on('error', (err) => cleanup(err))
    output.on('finish', () => resolve())
    input.pipe(zip).pipe(output)
  })
}

/**
 * Decompress a file previously compressed using GZip.
 *
 * @param {string} infile path of input file.
 * @param {string} outfile path of output file.
 */
async function decompressFile (infile, outfile) {
  const input = fs.createReadStream(infile)
  const unzip = zlib.createGunzip()
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err) => {
      input.destroy()
      output.destroy()
      reject(err)
    }
    input.on('error', (err) => cleanup(err))
    output.on('error', (err) => cleanup(err))
    output.on('finish', () => resolve())
    input.pipe(unzip).pipe(output)
  })
}

/**
 * Write a sequence of file parts into a pack file, returning the SHA256 of
 * the pack file and a mapping of part sha1 to part index (zero-based).
 *
 * @param {Object[]} parts list of file parts to be packed.
 * @param {string} parts[].path path of input file.
 * @param {number} parts[].offset byte offset within input file from which to start.
 * @param {number} parts[].length number of bytes to be read from input file.
 * @param {string} outfile path of output file.
 * @returns {Object} `hash` pack digest and `offsets` map to part index.
 */
async function packParts (parts, outfile) {
  const writeAndHash = async (data, fd, hash) => {
    await fwrite(fd, data)
    hash.update(data)
  }
  const outfd = await fopen(outfile, 'w')
  const packHash = crypto.createHash('sha256')
  // Write the pack header: P4CK, version, part count
  await writeAndHash('P4CK', outfd, packHash)
  const buf4 = Buffer.allocUnsafe(4)
  buf4.writeUInt32LE(1, 0)
  await writeAndHash(buf4, outfd, packHash)
  buf4.writeUInt32LE(parts.length, 0)
  await writeAndHash(buf4, outfd, packHash)
  // Write each of the parts into the pack, hashing the overall pack and each
  // individual part, producing a mapping of the part sha1 to the part index.
  const buffer = Buffer.allocUnsafe(BUFFER_SIZE)
  let partNumber = 0
  const hashToIndexMap = new Map()
  for (let { path, offset, length } of parts) {
    buf4.writeUInt32LE(length, 0)
    await writeAndHash(buf4, outfd, packHash)
    const fileHash = crypto.createHash('sha1')
    const infd = await fopen(path, 'r')
    await copyBytes(infd, offset, buffer, outfd, length, (data) => {
      fileHash.update(data)
      packHash.update(data)
    })
    fs.closeSync(infd)
    hashToIndexMap.set('sha1-' + fileHash.digest('hex'), partNumber)
    partNumber++
  }
  fs.closeSync(outfd)
  return { hash: 'sha256-' + packHash.digest('hex'), offsets: hashToIndexMap }
}

/**
 * Extract the file parts from the given pack file, writing them to the output
 * directory, with the names being the SHA1 checksum of each part.
 *
 * @param {string} infile path of pack file to read.
 * @param {string} outdir path to which parts are written.
 */
async function unpackParts (infile, outdir) {
  const infd = await fopen(infile, 'r')
  fs.ensureDirSync(outdir)
  const header = Buffer.allocUnsafe(PACK_HEADER_SIZE)
  await readBytes(infd, header, 0, PACK_HEADER_SIZE, 0)
  const magic = header.toString('utf8', 0, 4)
  if (magic !== 'P4CK') {
    throw new Error(`pack magic number invalid: ${magic}`)
  }
  const version = header.readUInt32LE(4)
  if (version < 1) {
    throw new Error(`pack version invalid: ${version}`)
  }
  if (version === 1) {
    await unpackPartsV1(infd, outdir)
    fs.closeSync(infd)
  } else {
    fs.closeSync(infd)
    throw new Error(`pack version unsupported: ${version}`)
  }
}

/**
 * Unpack the parts to a directory for version 1 of the pack file.
 *
 * @param {number} infd input file descriptor.
 * @param {string} outdir directory to which parts are written.
 */
async function unpackPartsV1 (infd, outdir) {
  const buffer = Buffer.allocUnsafe(BUFFER_SIZE)
  let fpos = PACK_HEADER_SIZE
  await readBytes(infd, buffer, 0, 4, fpos)
  fpos += 4
  const count = buffer.readUInt32LE(0)
  let index = 0
  while (index < count) {
    await readBytes(infd, buffer, 0, 4, fpos)
    fpos += 4
    const partSize = buffer.readUInt32LE(0)
    const outfile = tmp.fileSync({ dir: outdir }).name
    const outfd = await fopen(outfile, 'w')
    const fileHash = crypto.createHash('sha1')
    await copyBytes(infd, fpos, buffer, outfd, partSize, (data) => {
      fileHash.update(data)
    })
    fpos += partSize
    fs.closeSync(outfd)
    const fname = 'sha1-' + fileHash.digest('hex')
    fs.renameSync(outfile, path.join(outdir, fname))
    index++
  }
}

/**
 * Read from the input file into the buffer.
 *
 * @param {number} fd input file descriptor.
 * @param {Buffer} buffer buffer to which data is read.
 * @param {number} offset offset within buffer.
 * @param {number} length number of bytes to be read.
 * @param {number} fpos position from which to read file.
 */
async function readBytes (fd, buffer, offset, length, fpos) {
  let count = 0
  while (count < length) {
    const bytesRead = await fread(fd, buffer, offset + count, length - count, fpos)
    count += bytesRead
    fpos += bytesRead
  }
}

/**
 * Copy length bytes from infd to outfd, using the buffer. The callback
 * is invoked with each block of data that is read.
 *
 * @param {number} infd input file descriptor.
 * @param {number} fpos starting position from which to read input file.
 * @param {Buffer} buffer buffer for copying bytes.
 * @param {number} outfd output file descriptor.
 * @param {number} length number of bytes to be copied.
 * @param {Function} cb callback that receives each block of data.
 */
async function copyBytes (infd, fpos, buffer, outfd, length, cb) {
  let count = 0
  while (count < length) {
    const bytesRead = await fread(infd, buffer, 0, Math.min(length - count, buffer.length), fpos)
    count += bytesRead
    fpos += bytesRead
    const data = buffer.slice(0, bytesRead)
    await fwrite(outfd, data)
    cb(data)
  }
}

module.exports = {
  generateBucketName,
  generateUniqueId,
  generateMasterKeys,
  newMasterEncryptionData,
  decryptMasterKeys,
  encryptFile,
  decryptFile,
  compressFile,
  decompressFile,
  packParts,
  unpackParts
}
