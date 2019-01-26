//
// Copyright (c) 2018 Nathan Fiedler
//
import * as crypto from 'crypto'
import * as fs from 'fs'
import * as path from 'path'
import * as stream from 'stream'
import * as util from 'util'
import * as zlib from 'zlib'
import * as fx from 'fs-extra'
import * as tmp from 'tmp'
import * as verr from 'verror'
import * as dedupe from '@ronomon/deduplication'
import * as uuidv5 from 'uuid/v5'
import * as ULID from 'ulid'
const archiver = require('archiver')
const tar = require('tar')

const fopen = util.promisify(fs.open)
const fread = util.promisify(fs.read)
const fwrite = util.promisify(fs.write)

// size of the pack file header, which is not expected to change;
// has 'P4CK' (UTF-8, 4 bytes) and the version (4 byte LE integer)
// for plaintext packs, and 'C4PX' and a version with encryption
const PACK_HEADER_SIZE = 8
// use the same buffer size that Node uses for file streams
const BUFFER_SIZE = 65536

export interface MasterKeys {
  readonly master1: Buffer
  readonly master2: Buffer
}

export interface EncryptionData {
  /** Salt for hasing the user password. */
  readonly salt: Buffer
  /** Initialization vector for encrypting the master keys. */
  readonly iv: Buffer
  /** HMAC-SHA256 of the iv and encrypted master keys. */
  readonly hmac: Buffer
  /** The encrypted master keys. */
  readonly encrypted: Buffer
}

export interface Chunk {
  /** Path of input file, if available. */
  readonly path?: string
  /** SHA256 of chunk, if available. */
  readonly hash?: Buffer,
  /** Byte offset from which to start reading. */
  readonly offset: number
  /** Size in bytes of the chunk. */
  readonly size: number
  /** If true, chunk has been uploaded. */
  uploaded?: boolean
}

/**
 * Generate a type 5 UUID based on the given values.
 *
 * @param username name of the user performing the backup.
 * @param hostname name of the computer being backed up.
 * @returns unique identifier.
 */
export function generateUniqueId(username: string, hostname: string): string {
  return uuidv5(username + ':' + hostname, uuidv5.URL)
}

/**
 * Generate a suitable bucket name, using a ULID and the given UUID.
 *
 * @param uniqueId unique identifier, as from generateUniqueId().
 * @returns unique bucket name.
 */
export function generateBucketName(uniqueId: string): string {
  return ULID.ulid().toLowerCase() + uniqueId.replace(/-/g, '')
}

/**
 * Generate the master keys.
 *
 * @returns the generated master key values.
 */
export function generateMasterKeys(): MasterKeys {
  const master1 = Buffer.allocUnsafe(32)
  crypto.randomFillSync(master1)
  const master2 = Buffer.allocUnsafe(32)
  crypto.randomFillSync(master2)
  return { master1, master2 }
}

/**
 * Encrypt the given plain text using the key and initialization vector.
 *
 * @param plaintext data to be encrypted.
 * @param key encryption key.
 * @param iv initialization vector.
 * @returns cipher text.
 */
function encrypt(plaintext: Buffer, key: Buffer, iv: Buffer): Buffer {
  const cipher = crypto.createCipheriv('aes-256-ctr', key, iv)
  const enc1 = cipher.update(plaintext)
  const enc2 = cipher.final()
  return Buffer.concat([enc1, enc2])
}

/**
 * Decrypt the given cipher text using the key and initialization vector.
 *
 * @param ciphertext data to be decrypted.
 * @param key encryption key.
 * @param iv initialization vector.
 * @returns decrypted data.
 */
function decrypt(ciphertext: Buffer, key: Buffer, iv: Buffer): Buffer {
  const decipher = crypto.createDecipheriv('aes-256-ctr', key, iv)
  const dec1 = decipher.update(ciphertext)
  const dec2 = decipher.final()
  return Buffer.concat([dec1, dec2])
}

/**
 * Hash the given user password with the salt value using scrypt.
 *
 * @param password user password.
 * @param salt random salt value.
 * @returns 32 byte key value.
 */
function hashPassword(password: string, salt: Buffer): Buffer {
  return crypto.scryptSync(password, salt, 32)
}

/**
 * Compute the HMAC-SHA256 of the given data.
 *
 * @param key hashed user password.
 * @param data buffer to be digested.
 * @returns HMAC digest value.
 */
function computeHmac(key: Buffer, data: Buffer): Buffer {
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
 * @param password user-provided master password.
 * @param keys the master keys.
 * @returns the new encryption data.
 */
export function newMasterEncryptionData(password: string, keys: MasterKeys): EncryptionData {
  const salt = Buffer.allocUnsafe(16)
  crypto.randomFillSync(salt)
  // AES uses 128-bit initialization vectors
  const iv = Buffer.allocUnsafe(16)
  crypto.randomFillSync(iv)
  const key = hashPassword(password, salt)
  const masters = Buffer.concat([keys.master1, keys.master2])
  const encrypted = encrypt(masters, key, iv)
  const hmac = computeHmac(key, Buffer.concat([iv, encrypted]))
  return { salt, iv, hmac, encrypted }
}

/**
 * Decrypt the master keys from the data originally produced by
 * newMasterEncryptionData().
 *
 * @param data encryption data.
 * @param password user password.
 * @returns decrypted master key values.
 */
export function decryptMasterKeys(data: EncryptionData, password: string): MasterKeys {
  const key = hashPassword(password, data.salt)
  const hmac2 = computeHmac(key, Buffer.concat([data.iv, data.encrypted]))
  if (data.hmac.compare(hmac2) !== 0) {
    throw new verr.VError('HMAC does not match records')
  }
  const plaintext = decrypt(data.encrypted, key, data.iv)
  const middle = plaintext.length / 2
  const master1 = plaintext.slice(0, middle)
  const master2 = plaintext.slice(middle)
  return { master1, master2 }
}

/**
 * Encrypt a file.
 *
 * @param infile path of input file.
 * @param outfile path of output file.
 * @param key encryption key.
 * @param iv initialization vector.
 */
export function encryptFile(infile: string, outfile: string, key: Buffer, iv: Buffer) {
  const cipher = crypto.createCipheriv('aes-256-ctr', key, iv)
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err: Error) => {
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
 * @param infile path of input file.
 * @param outfile path of output file.
 * @param key encryption key.
 * @param iv initialization vector.
 */
export function decryptFile(infile: string, outfile: string, key: Buffer, iv: Buffer) {
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return decryptStream(input, output, key, iv)
}

/**
 * Decrypt a stream of bytes.
 *
 * @param input stream from which to read.
 * @param output stream to receive decrypted data.
 * @param key encryption key.
 * @param iv initialization vector.
 */
function decryptStream(input: stream.Readable, output: stream.Writable, key: Buffer, iv: Buffer) {
  const cipher = crypto.createDecipheriv('aes-256-ctr', key, iv)
  return new Promise((resolve, reject) => {
    const cleanup = (err: Error) => {
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
 * Compute the hash digest of the given data.
 *
 * @param data data from which to produce a checksum.
 * @param algo digest algorithm to be used (e.g. sha1).
 * @returns hex string of digest with `algo` plus "-" prefix.
 */
export function checksumData(data: string | Buffer, algo: string): string {
  const hash = crypto.createHash(algo)
  hash.update(data)
  return `${algo}-${hash.digest('hex')}`
}

/**
 * Convert a hash digest buffer to a hex string with an algo prefix.
 *
 * @param hash a hash digest in buffer form.
 * @param algo algorithm used to compute the hash.
 * @returns hash digest as hex string with algo prefix (e.g. "sha1-...").
 */
export function checksumFromBuffer(hash: Buffer, algo: string): string {
  return algo + '-' + hash.toString('hex')
}

/**
 * Convert a checksum string into a buffer of the hash digest.
 *
 * @param value hash digest with an algorithm prefix (e.g. 'sha1-').
 * @return buffer with digest bytes.
 */
export function bufferFromChecksum(value: string): Buffer {
  if (value.startsWith('sha1-')) {
    return Buffer.from(value.slice(5), 'hex')
  } else if (value.startsWith('sha256-')) {
    return Buffer.from(value.slice(7), 'hex')
  }
  throw new verr.VError(`checksum unsupported: ${value}`)
}

/**
 * Compute the hash digest of the given file.
 *
 * @param infile path of file to be processed.
 * @param algo name of digest algorithm (e.g. sha1, sha256).
 * @returns hex string of digest with `algo` plus "-" prefix.
 */
export function checksumFile(infile: string, algo: string): Promise<string> {
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
 * @param infile path of input file.
 * @param outfile path of output file.
 */
export function compressFile(infile: string, outfile: string) {
  const input = fs.createReadStream(infile)
  const zip = zlib.createGzip()
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err: Error) => {
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
 * @param infile path of input file.
 * @param outfile path of output file.
 */
export function decompressFile(infile: string, outfile: string) {
  const input = fs.createReadStream(infile)
  const unzip = zlib.createGunzip()
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err: Error) => {
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
 * @param chunks list of chunks to be packed.
 * @param outfile path of output file.
 * @returns hex string of pack digest with prefix 'sha256-'.
 */
export async function packChunks(chunks: Chunk[], outfile: string): Promise<string> {
  for (let chunk of chunks) {
    if (chunk.hash.length !== 32) {
      throw new verr.VError({
        name: 'IllegalArgumentError'
      }, 'chunk has invalid hash length')
    }
  }
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outfile)
    // we need to use archiver because it allows us to append
    // streams, that way we can add portions of larger files
    const archive = archiver('tar')
    output.on('close', () => {
      resolve()
    })
    archive.on('error', (err: Error) => {
      reject(err)
    })
    archive.pipe(output)
    for (let chunk of chunks) {
      const input = fs.createReadStream(chunk.path, {
        start: chunk.offset,
        end: chunk.offset + chunk.size - 1
      })
      const name = checksumFromBuffer(chunk.hash, 'sha256')
      archive.append(input, { name })
    }
    archive.finalize()
  }).then(() => {
    return checksumFile(outfile, 'sha256')
  })
}

/**
 * Extract the chunks from the given pack file, writing them to the output
 * directory, with the names being the original SHA256 of the chunk (with a
 * "sha256-" prefix).
 *
 * @param infile path of pack file to read.
 * @param outdir path to which chunks are written.
 * @returns checksums of all the extracted chunks.
 */
export async function unpackChunks(infile: string, outdir: string): Promise<string[]> {
  fx.ensureDirSync(outdir)
  const results: string[] = []
  // archiver can create tar files, but cannot extract them
  return tar.extract({
    cwd: outdir,
    file: infile,
    onentry: (entry: any) => {
      results.push(path.basename(entry.path))
    }
  }).then((): string[] => {
    return results
  })
}

/**
 * Write a sequence of chunks into a pack file, encrypting it, returning the
 * SHA256 of the pack file. The chunks will be written in the order they appear
 * in the array.
 *
 * @param chunks list of file chunks to be packed.
 * @param outfile path of output file.
 * @param keys master keys for encrypting the pack.
 * @returns hex string of pack digest with prefix 'sha256-'.
 */
export async function packChunksEncrypted(chunks: Chunk[], outfile: string, keys: MasterKeys): Promise<string> {
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
  const encryptedKeys = encrypt(Buffer.concat([sessionIV, sessionKey]), keys.master1, masterIV)
  const hmac = crypto.createHmac('sha256', keys.master2)
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
  await copyFileWithPrefix(prefix, encfile, outfile)
  fs.unlinkSync(encfile)
  return results
}

/**
 * Extract the chunks from the given encrypted pack file, writing them to the
 * output directory, with the names being the original SHA256 of the chunk (with
 * a "sha256-" prefix). The two master keys are used to decrypt the pack file.
 *
 * @param infile path of encrypted pack file.
 * @param outdir path to contain chunk files.
 * @param keys master encryption keys.
 * @returns checksums of all the extracted chunks.
 */
export async function unpackChunksEncrypted(infile: string, outdir: string, keys: MasterKeys): Promise<string[]> {
  const infd = await fopen(infile, 'r')
  const header = Buffer.allocUnsafe(PACK_HEADER_SIZE)
  await readBytes(infd, header, 0, PACK_HEADER_SIZE, 0)
  const magic = header.toString('utf8', 0, 4)
  if (magic !== 'C4PX') {
    throw new verr.VError(`pack magic number invalid: ${magic}`)
  }
  const version = header.readUInt32BE(4)
  if (version < 1) {
    throw new verr.VError(`pack version invalid: ${version}`)
  }
  fx.ensureDirSync(outdir)
  if (version === 1) {
    return unpackChunksEncryptedV1(infd, outdir, keys)
  } else {
    fs.closeSync(infd)
    throw new verr.VError(`pack version unsupported: ${version}`)
  }
}

/**
 * Unpack the chunks from an encrypted (version 1) pack file.
 *
 * @param infd input file descriptor.
 * @param outdir path to contain chunk files.
 * @param keys master encryption keys.
 * @returns checksums of all the extracted chunks.
 */
async function unpackChunksEncryptedV1(infd: number, outdir: string, keys: MasterKeys): Promise<string[]> {
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
  const hmac = crypto.createHmac('sha256', keys.master2)
  hmac.update(masterIV)
  hmac.update(encryptedKeys)
  const input = fs.createReadStream(null, { fd: infd, start: fpos, autoClose: false })
  const actualMac = await hmacStream(input, hmac)
  if (actualMac.equals(expectedMac)) {
    // decrypt the key and iv used to encrypt this pack file, then decrypt and
    // extract the chunks into the output directory
    const decryptedKeys = decrypt(encryptedKeys, keys.master1, masterIV)
    const sessionIV = decryptedKeys.slice(0, 16)
    const sessionKey = decryptedKeys.slice(16)
    const packfile = tmp.fileSync({ dir: outdir }).name
    const input = fs.createReadStream(null, { fd: infd, start: fpos })
    const output = fs.createWriteStream(packfile)
    await decryptStream(input, output, sessionKey, sessionIV)
    const results = await unpackChunks(packfile, outdir)
    fs.unlinkSync(packfile)
    return results
  } else {
    throw new verr.VError('stored HMAC and computed HMAC do not match')
  }
}

/**
 * Compute the HMAC digest of a stream of bytes.
 *
 * @param input stream of bytes to be processed.
 * @param hmac HMAC compute utility.
 */
function hmacStream(input: stream.Readable, hmac: crypto.Hmac): Promise<Buffer> {
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
async function readBytes(fd: number, buffer: Buffer, offset: number, length: number, fpos: number) {
  let count = 0
  while (count < length) {
    const { bytesRead } = await fread(fd, buffer, offset + count, length - count, fpos)
    count += bytesRead
    fpos += bytesRead
  }
}

/**
 * Copy length bytes from infd to outfd, using the buffer. The callback
 * is invoked with each block of data that is read. The file position
 * within the output file is managed by Node automatically.
 *
 * @param infd input file descriptor.
 * @param fpos starting position from which to read input file.
 * @param buffer buffer for copying bytes.
 * @param outfd output file descriptor.
 * @param length number of bytes to be copied.
 * @param cb callback that receives each block of data.
 */
async function copyBytes(infd: number, fpos: number, buffer: Buffer, outfd: number, length: number, cb?: Function) {
  let count = 0
  while (count < length) {
    const { bytesRead } = await fread(infd, buffer, 0, Math.min(length - count, buffer.length), fpos)
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
 * @param header bytes to write to output before copying.
 * @param infile input file path.
 * @param outfile output file path.
 */
export function copyFileWithPrefix(header: Buffer, infile: string, outfile: string) {
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(outfile)
  return new Promise((resolve, reject) => {
    const cleanup = (err: Error) => {
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
export function findFileChunks(infile: string, average: number): Promise<Chunk[]> {
  const fd = fs.openSync(infile, 'r')
  const minimum = Math.round(average / 2)
  const maximum = average * 2
  const source = Buffer.alloc(maximum * 2)
  const target = Buffer.alloc(dedupe.targetSize(minimum, source.length))

  return new Promise((resolve, reject) => {
    let chunks: Chunk[] = []
    let fileOffset = 0
    let chunkOffset = 0

    const close = (error?: Error) => {
      fs.closeSync(fd)
      if (error) {
        reject(error)
      }
    }

    const read = (sourceStart: number) => {
      const length = source.length - sourceStart
      const bytesRead = fs.readSync(fd, source, sourceStart, length, fileOffset)
      fileOffset += bytesRead
      const flags = (bytesRead < length) ? 1 : 0
      write(sourceStart + bytesRead, flags)
    }

    const write = (sourceSize: number, flags: number): void => {
      dedupe.deduplicate(average, minimum, maximum, source, 0, sourceSize, target, 0, flags,
        (error: Error, sourceOffset: number, targetOffset: number) => {
          // if error is defined, a runtime error occurred
          if (error) {
            return close(error)
          }
          let offset = 0
          while (offset < targetOffset) {
            const hash = Buffer.allocUnsafe(32)
            offset += target.copy(hash, 0, offset, offset + 32)
            const size = target.readUInt32BE(offset)
            offset += 4
            chunks.push({ path: infile, hash, offset: chunkOffset, size })
            chunkOffset += size
          }
          // Anything remaining in the source buffer should be moved to the
          // beginning of the source buffer, and become the sourceStart for the
          // next read so that we do not read data we have already read:
          const remaining = sourceSize - sourceOffset
          if (remaining > 0) {
            source.copy(source, 0, sourceOffset, sourceOffset + remaining)
          }
          if (flags === 1) {
            // the last block has finished processing
            close()
            resolve(chunks)
          } else {
            read(remaining)
          }
        }
      )
    }

    read(0)
  })
}

/**
 * Copy the chunk files to the given output location, deleting the chunks as
 * each one is copied.
 *
 * @param chunkFiles list of input files, in order.
 * @param outfile path of output file.
 */
export async function assembleChunks(chunkFiles: string[], outfile: string) {
  const outfd = await fopen(outfile, 'w')
  const buffer = Buffer.allocUnsafe(BUFFER_SIZE)
  for (let infile of chunkFiles) {
    const infd = await fopen(infile, 'r')
    const stat = fs.statSync(infile)
    await copyBytes(infd, 0, buffer, outfd, stat.size)
    fs.closeSync(infd)
    fs.unlinkSync(infile)
  }
  fs.closeSync(outfd)
}
