//
// Copyright (c) 2018 Nathan Fiedler
//
import * as crypto from 'crypto'
import * as fs from 'fs'
import * as path from 'path'
import * as stream from 'stream'
import * as util from 'util'
import * as fx from 'fs-extra'
import * as tmp from 'tmp'
import * as verr from 'verror'
import * as dedupe from '@ronomon/deduplication'
import * as uuidv5 from 'uuid/v5'
import * as ULID from 'ulid'
const archiver = require('archiver')
const openpgp = require('openpgp')
const kbpgp = require('kbpgp')
const tar = require('tar')

const F = kbpgp['const'].openpgp
const fopen = util.promisify(fs.open)
const fread = util.promisify(fs.read)
const fwrite = util.promisify(fs.write)

// use the same buffer size that Node uses for file streams
const BUFFER_SIZE = 65536

export interface EncryptionKeys {
  readonly publicKey: string
  readonly privateKey: string
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
 * Generate the public and private keys for encrypting pack files.
 *
 * @param userid identifier for the user to incorporate into the keys.
 * @param passphrase password for locking and unlocking the private key.
 * @param bits number of bits for the key (e.g. 4096).
 */
export async function generateEncryptionKeys(
  userid: string,
  passphrase: string,
  bits: number
): Promise<EncryptionKeys> {
  //
  // use kbpgp to generate the keys because it is much faster than openpgpjs
  // (see issue https://github.com/openpgpjs/openpgpjs/issues/449)
  // (see issue https://github.com/openpgpjs/openpgpjs/issues/530)
  //
  // TODO: openpgpjs has an encoding issue with userid, should be base64 encoded for safety
  //
  const opts = {
    userid,
    primary: {
      nbits: bits,
      flags: F.certify_keys | F.sign_data | F.auth | F.encrypt_comm | F.encrypt_storage,
      expire_in: 0
    },
    subkeys: [
      {
        nbits: bits / 2,
        flags: F.sign_data,
        expire_in: 0
      }, {
        nbits: bits / 2,
        flags: F.encrypt_comm | F.encrypt_storage,
        expire_in: 0
      }
    ]
  }
  // generate the key manager instance
  const keymgr: any = await new Promise((resolve, reject) => {
    kbpgp.KeyManager.generate(opts, (err: Error, keymgr: any) => {
      if (err) {
        reject(err)
      } else {
        resolve(keymgr)
      }
    })
  })
  // sign it
  await new Promise((resolve, reject) => {
    keymgr.sign({}, (err: Error) => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
  // get the ascii armor version of the private key
  const privateKey: string = await new Promise((resolve, reject) => {
    keymgr.export_pgp_private({ passphrase }, (err: Error, privateKey: string) => {
      if (err) {
        reject(err)
      } else {
        resolve(privateKey)
      }
    })
  })
  // get the public key string
  const publicKey: string = await new Promise((resolve, reject) => {
    keymgr.export_pgp_public({}, (err: Error, publicKey: string) => {
      if (err) {
        reject(err)
      } else {
        resolve(publicKey)
      }
    })
  })
  return { publicKey, privateKey }
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
    // set the date so the tar file produces the same results for the same
    // inputs every time; the date for chunks is completely irrelevant
    const epoch = new Date(0)
    for (let chunk of chunks) {
      const input = fs.createReadStream(chunk.path, {
        start: chunk.offset,
        end: chunk.offset + chunk.size - 1
      })
      const name = checksumFromBuffer(chunk.hash, 'sha256')
      archive.append(input, { name, date: epoch })
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
export async function packChunksEncrypted(chunks: Chunk[], outfile: string, keys: EncryptionKeys): Promise<string> {
  const packfile = tmp.fileSync({ dir: path.dirname(outfile) }).name
  const results = await packChunks(chunks, packfile)
  const input = fs.createReadStream(packfile)
  const output = fs.createWriteStream(outfile)
  const options = {
    message: openpgp.message.fromBinary(input),
    publicKeys: (await openpgp.key.readArmored(keys.publicKey)).keys,
    armor: false,
    compression: openpgp.enums.compression.zlib
  };
  output.on('error', (err: Error) => {
    input.destroy()
    output.destroy()
  })
  const ciphertext = await openpgp.encrypt(options)
  const encrypted = ciphertext.message.packets.write()
  const reader = openpgp.stream.getReader(encrypted)
  await readerToStream(reader, output)
  fs.unlinkSync(packfile)
  return results
}

/**
 * Read from the reader until it is done, writing the data to the output stream.
 *
 * @param reader from which bytes are copied.
 * @param output writable stream to which bytes are written.
 */
async function readerToStream(reader: any, output: stream.Writable): Promise<void> {
  const promisedWrite = (value: any, output: stream.Writable) => {
    return new Promise((resolve, reject) => {
      if (!output.write(value)) {
        output.once('drain', resolve)
      } else {
        resolve()
      }
    })
  }
  while (true) {
    const { done, value } = await reader.read()
    if (done) {
      await new Promise((resolve, reject) => {
        output.end(() => {
          resolve()
        })
      })
      break
    }
    await promisedWrite(value, output)
  }
}

/**
 * Extract the chunks from the given encrypted pack file, writing them to the
 * output directory, with the names being the original SHA256 of the chunk (with
 * a "sha256-" prefix). The two master keys are used to decrypt the pack file.
 *
 * @param infile path of encrypted pack file.
 * @param outdir path to contain chunk files.
 * @param keys master encryption keys.
 * @param passphrase pass phrase to unlock private key.
 * @returns checksums of all the extracted chunks.
 */
export async function unpackChunksEncrypted(
  infile: string,
  outdir: string,
  keys: EncryptionKeys,
  passphrase: string
): Promise<string[]> {
  const privKeyObj = (await openpgp.key.readArmored(keys.privateKey)).keys[0]
  await privKeyObj.decrypt(passphrase)
  const packfile = tmp.fileSync({ dir: outdir }).name
  const input = fs.createReadStream(infile)
  const output = fs.createWriteStream(packfile)
  const options = {
    message: await openpgp.message.read(input),
    privateKeys: [privKeyObj],
    format: 'binary'
  };
  output.on('error', (err: Error) => {
    input.destroy()
    output.destroy()
  })
  const plaintext = await openpgp.decrypt(options)
  const reader = openpgp.stream.getReader(plaintext.data)
  await readerToStream(reader, output)
  const results = await unpackChunks(packfile, outdir)
  fs.unlinkSync(packfile)
  return results
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
