"use strict"

const { promisify } = require('util')
const fs = require('fs')
const open = promisify(fs.open)
const read = promisify(fs.read)
const write = promisify(fs.write)
const close = promisify(fs.close)

const common = require('./common')

// dump Buffer to log as sequence of 8-bit binary strings
const bufToBin = (b, len) => b.reduce((acc, cur, i) => {
  if (i < len) {
    let bin = cur.toString(2)
    bin = '0'.repeat(8 - bin.length) + bin
    acc.push(bin)
  }
  return acc
}, []).join(' ')

// bitwise OR 32 bit int i into Buffer b (big endian)
function orUInt32BE(b, i, o) {
  b.writeUInt32BE((i | b.readUInt32BE(o)) >>> 0, o)
}

async function main() {
  try {
    const wikipath = common.getWikipath()
    const args = common.parseArgs()

    const filepattern = wikipath + args.basename

    const batch = []

    args.do && batch.push(pack_off(filepattern, args.do.off + args.do.rev,
      args.do.off, args.do.rev))
    args.to && batch.push(pack_all_off(filepattern, args.to,
    ))
    args.st && batch.push(pack_all_idx(filepattern, args.st,
    ))

    await Promise.all(batch)

  } catch (e) {
    console.warn("caught", e)
  }
}

async function packedFileCreate(filename, ele_bitlen, which) {
  const pf = {}
  
  const o = open(filename, 'w')

  pf.ele_bitlen = ele_bitlen

  pf.buf_bitlen = common.lcm(64, ele_bitlen)
  pf.buf_bytelen = Math.floor(pf.buf_bitlen / 8)
  pf.buf = new Buffer(pf.buf_bytelen + 7) // add padding so we can write 64 bits when when there's less than 8 bytes left

  pf.buf_offset = {
    bytes: 0,
    bits: 0,
    total: 0
  }

  //console.warn(`offset size in bits: ${offset_size_in_bits}`)
  //console.warn(`revision size in bits: ${revision_size_in_bits}`)
  console.warn(`element size in bits: ${ele_bitlen}`)
  console.warn(`bigbuf bitlen: ${pf.buf_bitlen}`)
  console.warn(`bigbuf bytelen: ${pf.buf_bytelen}`)
  console.warn(`bitlen / 64 = ${pf.buf_bitlen / 64}, bitlen / element size in bits = ${pf.buf_bitlen / ele_bitlen}`)

  pf.wh = which

  pf.fd = await o

  return pf
}

async function packedFileClose(pf) {
  await packedFileFlush(pf)
  await close(pf.fd)
}

async function packedFileFlush(pf) {
  console.warn(pf.wh, pf.buf_offset)
  console.warn(bufToBin(pf.buf, pf.buf_bytelen))

  if (pf.buf_offset.total) {
    const bytesToFlush = pf.buf_offset.bytes + (pf.buf_offset.bits !== 0)
    const { bytesWritten: bytesFlushed } = await write(pf.fd, pf.buf, 0, bytesToFlush)
    console.warn(pf.wh, bytesToFlush, bytesFlushed)
  }
}

async function packedFileWrite(
  pf, ele,
  logThisOne, wh, index, // TODO these should be in a callback
) {
  let h = Math.floor(ele / Math.pow(2, 32))
  let l = ele % Math.pow(2, 32)

  const left = 64 - pf.ele_bitlen - pf.buf_offset.bits

  if (left > 31) {
    h = l << (left - 64) >>> 0
    l = 0
  } else {
    h = ((h << left) | (l >>> (32 - left))) >>> 0
    l = (l << left) >>> 0
  }

  if (!pf.buf_offset.bits)
    pf.buf.writeUInt32BE(h, pf.buf_offset.bytes)
  else
    orUInt32BE(pf.buf, h, pf.buf_offset.bytes)

  pf.buf.writeUInt32BE(l, pf.buf_offset.bytes + 4)

  pf.buf_offset.total += pf.ele_bitlen
  pf.buf_offset.bytes = Math.floor(pf.buf_offset.total / 8)
  pf.buf_offset.bits = pf.buf_offset.total % 8

  // TODO how to get this in a callback?
  if (logThisOne)
    console.warn(`${wh}: ${index.toLocaleString()}\n${bufToBin(pf.buf, pf.buf_bytelen)}`)

  if (pf.buf_offset.total === pf.buf_bitlen) {
    await write(pf.fd, pf.buf, 0, pf.buf_bytelen)

    pf.buf_offset.total = 0
    pf.buf_offset.bytes = 0
    pf.buf_offset.bits = 0

    // zero the buffer since we OR into it
    // for some reason this doesn't work: pf.buf.map(e => 0)
    if (pf.buf_offset.total === 0)
      for (let i = 0; i < pf.buf_bytelen; ++i)
        pf.buf[i] = 0
  }
}

async function pack_off(filepattern, element_size_in_bits,
    offset_size_in_bits, revision_size_in_bits // TODO abstract these out
) {
  const [infd, outpf] = await Promise.all([
    open(filepattern + '-' + 'off' + '.raw', 'r'),
    packedFileCreate(filepattern + '-' + 'off' + '.pck', element_size_in_bits, "do"),
  ])

  const inbuf = new Buffer(12) // TODO abstract this
  let i = 0

  // TODO how to abstract these into a callback etc?
  const f1 = outpf.buf_bitlen / element_size_in_bits * 10000 // show progress when the buffer has been filled this many times
  const f2 = outpf.buf_bitlen / element_size_in_bits - 1 // number of elements in the buffer when it's full

  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_dump_offset(infd, inbuf,
      offset_size_in_bits,   // TODO
      revision_size_in_bits, // TODO
    )

    if (!rap.ok)
      break

    await packedFileWrite(
      outpf, rap.packed,
      logThisOne, "dump offset", i, // TODO this stuff should be in a callback
    )

    ++i
  }

  await Promise.all([close(infd), packedFileClose(outpf)])
}

async function read_and_pack_dump_offset(infd, inbuf,
  offset_size_in_bits,
  revision_size_in_bits,
) {
  //const bytesRead = fs.readSync(infd, inbuf, 0, 12)
  const r = await read(infd, inbuf, 0, 12, null)
  const bytesRead = r.bytesRead

  if (bytesRead === 12) {
    const lower = inbuf.readUInt32LE(0); // lower bytes first - little endian
    const upper = inbuf.readUInt32LE(4); // upper bytes last - little endian
    const extra = inbuf.readUInt32LE(8);

    const offset = upper * Math.pow(2, 32) + lower

    // js can't handle 64 bits but can handle 53 bits
    if (upper >= 1 << (64 - 53)) console.warn(`** do: offset ${offset} (${parseInt(+upper, 16)} : ${parseInt(+lower, 16)}) is greater than 48-bits!`);

    if (offset >= Math.pow(2, offset_size_in_bits)) console.warn(`** offset ${offset} (${parseInt(+offset, 16)}) is greater than the expected ${offset_size_in_bits} bits`)
    if (extra >= Math.pow(2, revision_size_in_bits)) console.warn(`** extra ${extra} (${parseInt(+extra, 16)}) is greater than the expected ${revision_size_in_bits} bits`)

    let packed = offset * Math.pow(2, revision_size_in_bits) + extra

    return { ok: true, packed }
  }

  return { ok: false }
}

async function pack_all_off(filepattern, element_size_in_bits,
) {
  const [infd, outpf] = await Promise.all([
    open(filepattern + '-' + 'all-off' + '.raw', 'r'),
    packedFileCreate(filepattern + '-' + 'all-off' + '.pck', element_size_in_bits, "to"),
  ])

  const inbuf = new Buffer(8) // TODO should also support 4
  let i = 0

  // TODO how to abstract these into a callback etc?
  const f1 = outpf.buf_bitlen / element_size_in_bits * 10000
  const f2 = outpf.buf_bitlen / element_size_in_bits - 1
  
  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_title_offset(infd, inbuf,
      element_size_in_bits, // TODO
    )

    if (!rap.ok)
      break

    await packedFileWrite(
      outpf, rap.packed,
      logThisOne, "title offset", i, // TODO
    )

    ++i
  }

  await Promise.all([close(infd), packedFileClose(outpf)])
}

async function read_and_pack_title_offset(infd, inbuf,
  element_size_in_bits,
) {
  //const bytesRead = fs.readSync(infd, inbuf, 0, 8)
  const r = await read(infd, inbuf, 0, 8, null)
  const bytesRead = r.bytesRead
  
  if (bytesRead === 8) { // TODO should also support 4
    const lower = inbuf.readUInt32LE(0); // lower bytes first - little endian
    const upper = inbuf.readUInt32LE(4); // upper bytes last - little endian

    const offset = upper * Math.pow(2, 32) + lower

    // js can't handle 64 bits but can handle 53 bits
    if (upper >= 1 << (64 - 53)) console.warn(`** to: offset ${offset} (${parseInt(+upper, 16)} : ${parseInt(+lower, 16)}) is greater than 48-bits!`);

    if (offset >= Math.pow(2, element_size_in_bits)) console.warn(`** to: offset ${offset} (${parseInt(+offset, 16)}) is greater than the expected ${element_size_in_bits} bits`)

    let packed = offset
  
    return { ok: true, packed }
  }

  return { ok: false }
}

async function pack_all_idx(filepattern, element_size_in_bits,
) {
  const [infd, outpf] = await Promise.all([
    open(filepattern + '-' + 'all-idx' + '.raw', 'r'),
    packedFileCreate(filepattern + '-' + 'all-idx' + '.pck', element_size_in_bits, "ts"),
  ])

  const inbuf = new Buffer(4) // TODO should also support 8
  let i = 0

  // TODO how to abstract these into a callback etc?
  const f1 = outpf.buf_bitlen / element_size_in_bits * 7500
  const f2 = outpf.buf_bitlen / element_size_in_bits - 1
  
  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_title_index(infd, inbuf,
      element_size_in_bits, // TODO
    )

    if (!rap.ok)
      break

      await packedFileWrite(
        outpf, rap.packed,
        logThisOne, "title index", i, // TODO
      )
  
    ++i
  }

  await Promise.all([close(infd), packedFileClose(outpf)])
}

async function read_and_pack_title_index(infd, inbuf,
  element_size_in_bits,
) {
  const r = await read(infd, inbuf, 0, 4, null)
  const bytesRead = r.bytesRead

  if (bytesRead === 4) { // TODO should also support 8
    const index = inbuf.readUInt32LE(0); // lower bytes first - little endian

    if (index >= Math.pow(2, element_size_in_bits)) console.warn(`** st: offset ${offset} (${parseInt(+offset, 16)}) is greater than the expected ${element_size_in_bits} bits`)

    let packed = index

    return { ok: true, packed }
  }

  return { ok: false }
}

main()
