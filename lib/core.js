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
const dedupe = require('@ronomon/deduplication')

const fopen = util.promisify(fs.open)
const fread = util.promisify(fs.read)
const fwrite = util.promisify(fs.write)

// size of the pack file header, which is not expected to change;
// has 'P4CK' (UTF-8, 4 bytes) and the version (4 byte LE integer)
// for plaintext packs, and 'C4PX' and a version with encryption
const PACK_HEADER_SIZE = 8
// use the same buffer size that Node uses for file streams
const BUFFER_SIZE = 65536

/**
 * Generate a type 5 UUID based on the given values.
 *
 * @param {string} username name of the user performing the backup.
 * @param {string} hostname name of the computer being backed up.
 * @returns {string} unique identifier.
 */
function generateUniqueId (username, hostname) {
  return uuidv5(username + ':' + hostname, uuidv5.URL)
}

/**
 * Generate a suitable bucket name, using a ULID and the given UUID.
 *
 * @param {string} uniqueId unique identifier, as from generateUniqueId().
 * @returns {string} unique bucket name.
 */
function generateBucketName (uniqueId) {
  return ULID.ulid().toLowerCase() + uniqueId.replace(/-/g, '')
}

/**
 * Generate the two 32 byte master keys, named master1 and master2.
 *
 * @returns {Object} containing master1 and master2 properties.
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
 * @returns {Buffer} HMAC digest value.
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
  // AES uses 128-bit initialization vectors
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
 * @returns {Object} containing master1 and master2 properties.
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
function encryptFile (infile, outfile, key, iv) {
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
function decryptFile (infile, outfile, key, iv) {
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return decryptStream(input, output, key, iv)
}

/**
 * Decrypt a stream of bytes.
 *
 * @param {string} input stream from which to read.
 * @param {string} output stream to receive decrypted data.
 * @param {Buffer} key encryption key.
 * @param {Buffer} iv initialization vector.
 */
function decryptStream (input, output, key, iv) {
  const cipher = crypto.createDecipheriv('aes-256-ctr', key, iv)
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
 * Compute the hash digest of the given file.
 *
 * @param {string} infile path of file to be processed.
 * @param {string} algo name of digest algorithm (e.g. sha1, sha256).
 * @returns {string} hex string of digest with "<algo>-" prefix.
 */
function checksumFile (infile, algo) {
  const input = fs.createReadStream(infile)
  const hash = crypto.createHash(algo)
  return new Promise((resolve, reject) => {
    input.on('readable', () => {
      const data = input.read()
      if (data) {
        hash.update(data)
      } else {
        resolve(`${algo}-${hash.digest('hex')}`)
      }
    })
    input.on('error', (err) => {
      input.destroy()
      reject(err)
    })
  })
}

/**
 * Compress a file using GZip.
 *
 * @param {string} infile path of input file.
 * @param {string} outfile path of output file.
 */
function compressFile (infile, outfile) {
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
function decompressFile (infile, outfile) {
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
 * Write a sequence of chunks into a pack file, returning the SHA256 of the pack
 * file. The chunks will be written in the order they appear in the array.
 *
 * @param {Object[]} chunks list of chunks to be packed.
 * @param {string} chunks[].path path of input file.
 * @param {number} chunks[].offset byte offset from which to start reading.
 * @param {number} chunks[].length number of bytes to be read.
 * @param {string} outfile path of output file.
 * @returns {string} hex string of pack digest with prefix 'sha256-'.
 */
async function packChunks (chunks, outfile) {
  const writeAndHash = async (data, fd, hash) => {
    await fwrite(fd, data)
    hash.update(data)
  }
  const outfd = await fopen(outfile, 'w')
  const packHash = crypto.createHash('sha256')
  // Write the pack header: P4CK, version, chunk count
  const header = Buffer.allocUnsafe(12)
  header.write('P4CK')
  header.writeUInt32BE(1, 4)
  header.writeUInt32BE(chunks.length, 8)
  await writeAndHash(header, outfd, packHash)
  // Write each of the chunks into the pack, hashing the overall pack.
  const buffer = Buffer.allocUnsafe(BUFFER_SIZE)
  const buf4 = Buffer.allocUnsafe(4)
  for (let { path, offset, length } of chunks) {
    buf4.writeUInt32BE(length, 0)
    await writeAndHash(buf4, outfd, packHash)
    const infd = await fopen(path, 'r')
    await copyBytes(infd, offset, buffer, outfd, length, (data) => {
      packHash.update(data)
    })
    fs.closeSync(infd)
  }
  fs.closeSync(outfd)
  await maybeCompress(outfile)
  return 'sha256-' + packHash.digest('hex')
}

/**
 * Compress the specified file, and if the result is smaller then keep that
 * file, removing the original, and renaming the new one. Otherwise discard the
 * compressed version.
 *
 * @param {string} infile file to be tentatively compressed.
 */
async function maybeCompress (infile) {
  const outfile = tmp.fileSync({ dir: path.dirname(infile) }).name
  await compressFile(infile, outfile)
  const istat = fs.statSync(infile)
  const ostat = fs.statSync(outfile)
  if (istat.size > ostat.size) {
    fs.unlinkSync(infile)
    fs.renameSync(outfile, infile)
  } else {
    fs.unlinkSync(outfile)
  }
}

/**
 * Extract the chunks from the given pack file, writing them to the output
 * directory, with the names being the given prefix plus a numeric suffix
 * extension, starting at one and monotonically increasing (e.g. prefix.1,
 * prefix.2, etc). If the file is compressed, it will be decompressed in place.
 *
 * @param {string} infile path of pack file to read.
 * @param {string} prefix prefix for output file names.
 * @param {string} outdir path to which chunks are written.
 */
async function unpackChunks (infile, prefix, outdir) {
  await maybeDecompress(infile)
  const infd = await fopen(infile, 'r')
  const header = Buffer.allocUnsafe(PACK_HEADER_SIZE)
  await readBytes(infd, header, 0, PACK_HEADER_SIZE, 0)
  const magic = header.toString('utf8', 0, 4)
  if (magic !== 'P4CK') {
    throw new Error(`pack magic number invalid: ${magic}`)
  }
  const version = header.readUInt32BE(4)
  if (version < 1) {
    throw new Error(`pack version invalid: ${version}`)
  }
  fs.ensureDirSync(outdir)
  if (version === 1) {
    await unpackChunksV1(infd, prefix, outdir)
    fs.closeSync(infd)
  } else {
    fs.closeSync(infd)
    throw new Error(`pack version unsupported: ${version}`)
  }
}

/**
 * Check if the specified file is compressed using gzip, and decompress if that
 * is the case, replacing the file in the process.
 *
 * @param {string} infile file that may or may not be compressed.
 */
async function maybeDecompress (infile) {
  const infd = await fopen(infile, 'r')
  const magic = Buffer.allocUnsafe(2)
  await readBytes(infd, magic, 0, 2, 0)
  fs.closeSync(infd)
  const value = magic.readUInt16BE(0)
  if (value === 0x1f8b) {
    const outfile = tmp.fileSync({ dir: path.dirname(infile) }).name
    await decompressFile(infile, outfile)
    fs.unlinkSync(infile)
    fs.renameSync(outfile, infile)
  }
}

/**
 * Unpack the chunks to a directory for version 1 of the pack file.
 *
 * @param {number} infd input file descriptor.
 * @param {string} prefix prefix for output file names.
 * @param {string} outdir directory to which chunks are written.
 */
async function unpackChunksV1 (infd, prefix, outdir) {
  const buffer = Buffer.allocUnsafe(BUFFER_SIZE)
  let fpos = PACK_HEADER_SIZE
  await readBytes(infd, buffer, 0, 4, fpos)
  fpos += 4
  const count = buffer.readUInt32BE(0)
  let index = 0
  while (index < count) {
    await readBytes(infd, buffer, 0, 4, fpos)
    fpos += 4
    const chunkSize = buffer.readUInt32BE(0)
    const outfile = tmp.fileSync({ dir: outdir }).name
    const outfd = await fopen(outfile, 'w')
    await copyBytes(infd, fpos, buffer, outfd, chunkSize)
    fpos += chunkSize
    fs.closeSync(outfd)
    const fname = `${prefix}.${index + 1}`
    fs.renameSync(outfile, path.join(outdir, fname))
    index++
  }
}

/**
 * Write a sequence of chunks into a pack file, encrypting it, returning the
 * SHA256 of the pack file. The chunks will be written in the order they appear
 * in the array.
 *
 * @param {Object[]} chunks list of file chunks to be packed.
 * @param {string} chunks[].path path of input file.
 * @param {number} chunks[].offset byte offset from which to start reading.
 * @param {number} chunks[].length number of bytes to be read.
 * @param {string} outfile path of output file.
 * @returns {string} hex string of pack digest with prefix 'sha256-'.
 */
async function packChunksEncrypted (chunks, outfile, master1, master2) {
  // produce the pack file and encrypt it using a new key and iv
  const sessionKey = Buffer.allocUnsafe(32)
  crypto.randomFillSync(sessionKey)
  // AES uses 128-bit initialization vectors
  const sessionIV = Buffer.allocUnsafe(16)
  crypto.randomFillSync(sessionIV)
  const packfile = tmp.fileSync({ dir: path.dirname(outfile) }).name
  const encfile = tmp.fileSync({ dir: path.dirname(outfile) }).name
  const results = await packChunks(chunks, packfile)
  await encryptFile(packfile, encfile, sessionKey, sessionIV)
  fs.unlinkSync(packfile)

  // prepare to encrypt the new key and iv using yet another new key, then HMAC
  // those values and the entire encrypted file
  const masterIV = Buffer.allocUnsafe(16)
  crypto.randomFillSync(masterIV)
  const encryptedKeys = encrypt(Buffer.concat([sessionIV, sessionKey]), master1, masterIV)
  const hmac = crypto.createHmac('sha256', master2)
  hmac.update(masterIV)
  hmac.update(encryptedKeys)
  const input = fs.createReadStream(encfile)
  const mac = await hmacStream(input, hmac)

  // write the encryption data to the output file followed by the contents of
  // the encrypted pack file
  const header = Buffer.allocUnsafe(PACK_HEADER_SIZE)
  header.write('C4PX')
  header.writeUInt32BE(1, 4)
  const prefix = Buffer.concat([header, mac, masterIV, encryptedKeys])
  await copyFile(prefix, encfile, outfile)
  fs.unlinkSync(encfile)
  return results
}

/**
 * Extract the chunks from the given encrypted pack file, writing them to the
 * output directory, with the names being the given prefix plus a numeric suffix
 * extension, starting at one and monotonically increasing (e.g. prefix.1,
 * prefix.2, etc). The two master keys are used to decrypt the pack file.
 *
 * @param {string} infile path of encrypted pack file.
 * @param {string} prefix prefix for output file names.
 * @param {string} outdir path to contain chunk files.
 * @param {Buffer} master1 first master key.
 * @param {Buffer} master2 second master key.
 */
async function unpackChunksEncrypted (infile, prefix, outdir, master1, master2) {
  const infd = await fopen(infile, 'r')
  const header = Buffer.allocUnsafe(PACK_HEADER_SIZE)
  await readBytes(infd, header, 0, PACK_HEADER_SIZE, 0)
  const magic = header.toString('utf8', 0, 4)
  if (magic !== 'C4PX') {
    throw new Error(`pack magic number invalid: ${magic}`)
  }
  const version = header.readUInt32BE(4)
  if (version < 1) {
    throw new Error(`pack version invalid: ${version}`)
  }
  fs.ensureDirSync(outdir)
  if (version === 1) {
    await unpackChunksEncryptedV1(infd, prefix, outdir, master1, master2)
  } else {
    fs.closeSync(infd)
    throw new Error(`pack version unsupported: ${version}`)
  }
}

/**
 * Unpack the chunks from an encrypted (version 1) pack file.
 *
 * @param {number} infd input file descriptor.
 * @param {string} prefix prefix for output file names.
 * @param {string} outdir path to contain chunk files.
 * @param {Buffer} master1 first master key.
 * @param {Buffer} master2 second master key.
 */
async function unpackChunksEncryptedV1 (infd, prefix, outdir, master1, master2) {
  // read the encrypted pack file header
  const header = Buffer.allocUnsafe(96)
  let fpos = PACK_HEADER_SIZE
  await readBytes(infd, header, 0, header.length, fpos)
  fpos += header.length
  // 32-byte HMAC digest
  const expectedMac = header.slice(0, 32)
  // 16-byte "master" init vector
  const masterIV = header.slice(32, 48)
  // 48-byte encrypted session key and data iv
  const encryptedKeys = header.slice(48, 96)
  // compute the HMAC and compare with the file
  const hmac = crypto.createHmac('sha256', master2)
  hmac.update(masterIV)
  hmac.update(encryptedKeys)
  const input = fs.createReadStream(null, { fd: infd, start: fpos, autoClose: false })
  const actualMac = await hmacStream(input, hmac)
  if (actualMac.equals(expectedMac)) {
    // decrypt the key and iv used to encrypt this pack file, then decrypt and
    // extract the chunks into the output directory
    const decryptedKeys = decrypt(encryptedKeys, master1, masterIV)
    const sessionIV = decryptedKeys.slice(0, 16)
    const sessionKey = decryptedKeys.slice(16)
    const packfile = tmp.fileSync({ dir: outdir }).name
    const input = fs.createReadStream(null, { fd: infd, start: fpos })
    const output = fs.createWriteStream(packfile)
    await decryptStream(input, output, sessionKey, sessionIV)
    await unpackChunks(packfile, prefix, outdir)
    fs.unlinkSync(packfile)
  } else {
    throw new Error('stored HMAC and computed HMAC do not match')
  }
}

/**
 * Compute the HMAC digest of a stream of bytes.
 *
 * @param {string} input stream of bytes to be processed.
 * @param {Hmac} hmac HMAC compute utility.
 */
function hmacStream (input, hmac) {
  return new Promise((resolve, reject) => {
    input.on('error', (err) => {
      input.destroy()
      reject(err)
    })
    input.on('readable', () => {
      const data = input.read()
      if (data) {
        hmac.update(data)
      } else {
        resolve(hmac.digest())
      }
    })
  })
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
    if (cb) {
      cb(data)
    }
  }
}

/**
 * Copy the one file to another, with a prefix.
 *
 * @param {Buffer} header bytes to write to output before copying.
 * @param {string} infile input file path.
 * @param {string} outfile output file path.
 */
function copyFile (header, infile, outfile) {
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
    output.write(header)
    input.pipe(output)
  })
}

/**
 * Find the chunk boundaries within the given file, using the FastCDC algorithm.
 * The returned array contains objects with `hash` as the 32-byte SHA256 buffer,
 * `offset` as the byte position where the chunk starts, and `size` which
 * indicates the length of the chunk. N.B. this function will allocate a buffer
 * that is four times the size of the average value provided.
 *
 * @param {string} infile path of the file to be processed.
 * @param {number} average desired size in bytes for average chunk.
 * @returns {Promise<Array>} resolves to a list of chunk objects.
 */
function findFileChunks (infile, average) {
  const fd = fs.openSync(infile, 'r')
  const minimum = Math.round(average / 2)
  const maximum = average * 2
  const source = Buffer.alloc(maximum * 2)
  const target = Buffer.alloc(dedupe.targetSize(minimum, source.length))

  return new Promise((resolve, reject) => {
    let flags = 0
    const close = (error) => {
      fs.closeSync(fd)
      if (error) {
        // force the loop to exit
        flags = 1
        reject(error)
      }
    }

    let chunks = []
    let fileOffset = 0
    let chunkOffset = 0
    let sourceStart = 0

    while (flags === 0) {
      const length = source.length - sourceStart
      const bytesRead = fs.readSync(fd, source, sourceStart, length, fileOffset)
      fileOffset += bytesRead
      flags = (bytesRead < length) ? 1 : 0
      const sourceSize = sourceStart + bytesRead
      try {
        dedupe.deduplicate(average, minimum, maximum, source, 0, sourceSize, target, 0, flags,
          (error, sourceOffset, targetOffset) => {
            // n.b. the library throws the error, so this is always undefined
            if (error) {
              close(error)
              return
            }
            let offset = 0
            while (offset < targetOffset) {
              const hash = target.slice(offset, offset + 32)
              offset += 32
              const size = target.readUInt32BE(offset)
              offset += 4
              chunks.push({ hash, offset: chunkOffset, size })
              chunkOffset += size
            }
            // Anything remaining in the source buffer should be moved to the
            // beginning of the source buffer, and become the sourceStart for the
            // next read so that we do not read data we have already read:
            sourceStart = sourceSize - sourceOffset
            if (sourceStart > 0) {
              source.copy(source, 0, sourceOffset, sourceOffset + sourceStart)
            }
            if (flags !== 0) {
              // the last block has finished processing
              close()
              resolve(chunks)
            }
          }
        )
      } catch (err) {
        close(err)
      }
    }
  })
}

module.exports = {
  generateBucketName,
  generateUniqueId,
  generateMasterKeys,
  newMasterEncryptionData,
  decryptMasterKeys,
  encryptFile,
  decryptFile,
  checksumFile,
  compressFile,
  decompressFile,
  packChunks,
  unpackChunks,
  packChunksEncrypted,
  unpackChunksEncrypted,
  findFileChunks
}
