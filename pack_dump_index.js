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

    args.do && batch.push(pack_off(filepattern, args.do.off + args.do.rev, args.do.off, args.do.rev))
    args.to && batch.push(pack_all_off(filepattern, args.to))
    args.st && batch.push(pack_all_idx(filepattern, args.st))

    await Promise.all(batch)

  } catch (e) {
    console.log("caught", e)
  }
}

async function pack_off(filepattern, element_size_in_bits, offset_size_in_bits, revision_size_in_bits) {
  const [infd, outfd] = await Promise.all([
    open(filepattern + '-' + 'off' + '.raw', 'r'),
    open(filepattern + '-' + 'off' + '.pck', 'w'),
  ])

  const bigbuf_bitlen = common.lcm(64, element_size_in_bits)
  const bigbuf_bytelen = Math.floor(bigbuf_bitlen / 8)
  const bigbuf = new Buffer(bigbuf_bytelen + 7) // add padding so we can write 64 bits when when there's less than 8 bytes left

  console.warn(`offset size in bits: ${offset_size_in_bits}`)
  console.warn(`revision size in bits: ${revision_size_in_bits}`)
  console.warn(`element size in bits: ${element_size_in_bits}`)
  console.warn(`bigbuf bitlen: ${bigbuf_bitlen}`)
  console.warn(`bigbuf bytelen: ${bigbuf_bytelen}`)
  console.warn(`bitlen / 64 = ${bigbuf_bitlen / 64}, bitlen / element size in bits = ${bigbuf_bitlen / element_size_in_bits}`)

  let bigbuf_offset = {
    bytes: 0,
    bits: 0,
    total: 0
  }

  const inbuf = new Buffer(12)
  let bytesRead
  let i = 0

  const f1 = bigbuf_bitlen / element_size_in_bits * 5000
  const f2 = bigbuf_bitlen / element_size_in_bits - 1
 
  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_dump_offset(infd, inbuf,
      offset_size_in_bits,
      revision_size_in_bits,
    )

    if (rap.ok) {
      const packed = rap.packed
      await output_element_via_buffer(outfd, bigbuf, packed,
        element_size_in_bits,
        bigbuf,
        bigbuf_offset,
        bigbuf_bytelen,
        bigbuf_bitlen,
        logThisOne,
        "dump offset",
        i,
      )
    } else {
      console.warn(bigbuf_offset)
      console.warn(bufToBin(bigbuf, bigbuf_bytelen))

      // flush
      if (bigbuf_offset.total) {
        const bytesToFlush = bigbuf_offset.bytes + (bigbuf_offset.bits !== 0)
        const { bytesWritten: bytesFlushed } = await write(outfd, bigbuf, 0, bytesToFlush)
        console.warn("do", bytesToFlush, bytesFlushed)
      }
      break;
    }

    ++i
  }

  await Promise.all([close(infd), close(outfd)])
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

async function pack_all_off(filepattern, element_size_in_bits) {
  const [infd, outfd] = await Promise.all([
    open(filepattern + '-' + 'all-off' + '.raw', 'r'),
    open(filepattern + '-' + 'all-off' + '.pck', 'w'),
  ])

  const bigbuf_bitlen = common.lcm(64, element_size_in_bits)
  const bigbuf_bytelen = Math.floor(bigbuf_bitlen / 8)
  const bigbuf = new Buffer(bigbuf_bytelen + 7) // add padding so we can write 64 bits when when there's less than 8 bytes left

  console.warn(`to: element size in bits: ${element_size_in_bits}`)
  console.warn(`to: bigbuf bitlen: ${bigbuf_bitlen}`)
  console.warn(`to: bigbuf bytelen: ${bigbuf_bytelen}`)
  console.warn(`to: bitlen / 64 = ${bigbuf_bitlen / 64}, bitlen / element size in bits = ${bigbuf_bitlen / element_size_in_bits}`)

  let bigbuf_offset = {
    bytes: 0,
    bits: 0,
    total: 0
  }

  const inbuf = new Buffer(8) // TODO should also support 4
  let bytesRead
  let i = 0

  const f1 = bigbuf_bitlen / element_size_in_bits * 5000
  const f2 = bigbuf_bitlen / element_size_in_bits - 1
  
  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_title_offset(infd, inbuf,
      element_size_in_bits,
    )

    if (rap.ok) {
      const packed = rap.packed
      await output_element_via_buffer(outfd, bigbuf, packed,
        element_size_in_bits,
        bigbuf,
        bigbuf_offset,
        bigbuf_bytelen,
        bigbuf_bitlen,
        logThisOne,
        "title offset",
        i,
      )
    } else {
      console.warn("to", bigbuf_offset)
      console.warn(bufToBin(bigbuf, bigbuf_bytelen))

      // flush
      if (bigbuf_offset.total) {
        const bytesToFlush = bigbuf_offset.bytes + (bigbuf_offset.bits !== 0)
        const { bytesWritten: bytesFlushed } = await write(outfd, bigbuf, 0, bytesToFlush)
        console.warn("to", bytesToFlush, bytesFlushed)
      }
      break;
    }

    ++i
  }

  await Promise.all([close(infd), close(outfd)])
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

async function pack_all_idx(filepattern, element_size_in_bits) {
  const [infd, outfd] = await Promise.all([
    open(filepattern + '-' + 'all-idx' + '.raw', 'r'),
    open(filepattern + '-' + 'all-idx' + '.pck', 'w'),
  ])

  const bigbuf_bitlen = common.lcm(64, element_size_in_bits)
  const bigbuf_bytelen = Math.floor(bigbuf_bitlen / 8)
  const bigbuf = new Buffer(bigbuf_bytelen + 7) // add padding so we can write 64 bits when when there's less than 8 bytes left

  console.warn(`st: element size in bits: ${element_size_in_bits}`)
  console.warn(`st: bigbuf bitlen: ${bigbuf_bitlen}`)
  console.warn(`st: bigbuf bytelen: ${bigbuf_bytelen}`)
  console.warn(`st: bitlen / 64 = ${bigbuf_bitlen / 64}, bitlen / element size in bits = ${bigbuf_bitlen / element_size_in_bits}`)

  let bigbuf_offset = {
    bytes: 0,
    bits: 0,
    total: 0
  }

  const inbuf = new Buffer(4) // TODO should also support 8
  let bytesRead
  let i = 0

  const f1 = bigbuf_bitlen / element_size_in_bits * 5000
  const f2 = bigbuf_bitlen / element_size_in_bits - 1
  
  while (true) {
    const logThisOne = (i % f1) == f2

    const rap = await read_and_pack_title_index(infd, inbuf,
      element_size_in_bits,
    )

    if (rap.ok) {
      const packed = rap.packed
      await output_element_via_buffer(outfd, bigbuf, packed,
        element_size_in_bits,
        bigbuf,
        bigbuf_offset,
        bigbuf_bytelen,
        bigbuf_bitlen,
        logThisOne,
        "title index",
        i,
      )
    } else {
      console.warn("st", bigbuf_offset)
      console.warn(bufToBin(bigbuf, bigbuf_bytelen))

      // flush
      if (bigbuf_offset.total) {
        const bytesToFlush = bigbuf_offset.bytes + (bigbuf_offset.bits !== 0)
        const { bytesWritten: bytesFlushed } = await write(outfd, bigbuf, 0, bytesToFlush)
        console.warn("st", bytesToFlush, bytesFlushed)
      }
      break;
    }

    ++i
  }

  await Promise.all([close(infd), close(outfd)])
}

async function read_and_pack_title_index(infd, inbuf,
  element_size_in_bits,
) {
  //const bytesRead = fs.readSync(infd, inbuf, 0, 4) // TODO should also support 8
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

async function output_element_via_buffer(fd, uint64, ele,
  ele_bitlen,
  bigbuf,
  bigbuf_offset,
  bigbuf_bytelen,
  bigbuf_bitlen,
  logThisOne,
  wh,
  index,
) {
  let h = Math.floor(ele / Math.pow(2, 32))
  let l = ele % Math.pow(2, 32)

  const left = 64 - ele_bitlen - bigbuf_offset.bits

  if (left > 31) {
    h = l << (left - 64) >>> 0
    l = 0
  } else {
    h = ((h << left) | (l >>> (32 - left))) >>> 0
    l = (l << left) >>> 0
  }

  if (!bigbuf_offset.bits)
    uint64.writeUInt32BE(h, bigbuf_offset.bytes)
  else
    orUInt32BE(uint64, h, bigbuf_offset.bytes)

  uint64.writeUInt32BE(l, bigbuf_offset.bytes + 4)

  bigbuf_offset.total += ele_bitlen
  bigbuf_offset.bytes = Math.floor(bigbuf_offset.total / 8)
  bigbuf_offset.bits = bigbuf_offset.total % 8

  if (logThisOne) {
    console.warn(`${wh}: ${index.toLocaleString()}\n${bufToBin(bigbuf, bigbuf_bytelen)}`)
  }

  if (bigbuf_offset.total === bigbuf_bitlen) {
    await write(fd, bigbuf, 0, bigbuf_bytelen)

    bigbuf_offset.total = 0
    bigbuf_offset.bytes = 0
    bigbuf_offset.bits = 0

    // zero the buffer since we OR into it
    // for some reason this doesn't work: bigbuf.map(e => 0)
    if (bigbuf_offset.total === 0)
      for (let i = 0; i < bigbuf_bytelen; ++i)
        bigbuf[i] = 0
  }
}
