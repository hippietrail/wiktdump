"use strict"

const { promisify } = require('util')
const fs = require('fs')
const open = promisify(fs.open)
const read = promisify(fs.read)
const close = promisify(fs.close)

const common = require('./common')

async function main() {
  try {
    const wikipath = common.getWikipath()
    const args = common.parseArgs()

    const offset_size_in_bits = args.do.off
    const revision_size_in_bits = args.do.rev
    const element_size_in_bits = offset_size_in_bits + revision_size_in_bits

    const filepattern = wikipath + args.basename

    const [raw_fd, pck_fd] = await Promise.all([
      open(filepattern + '-' + 'off' + '.raw', 'r'),
      open(filepattern + '-' + 'off' + '.pck', 'r'),
    ])

    let biggest_rev = -Infinity

    for (let index = 0, done = false; !done; ++index) {
      const r = await read_and_compare_entry(raw_fd, pck_fd, element_size_in_bits, index, revision_size_in_bits)
      let prog = false

      if (r.rev > biggest_rev) {
        biggest_rev = r.rev
        prog = true
      }

      if (index % 100000 == 0 || !r.ok)
        prog = true

      if (prog)
        console.log(`index: ${index.toLocaleString()} : ${r.off.toLocaleString()} + ${r.rev}`)

      // TODO distinguish between finishing because we checked all the data
      // TODO versus finishing due to some error
      if (!r.ok) done = true
    }

    close(raw_fd)
    close(pck_fd)
  
  } catch (e) {
    console.log("caught error in main", e)
  }
}

async function read_and_compare_entry(raw_fd, pck_fd, item_size_in_bits, index, revision_size_in_bits) {
  const [raw_item, pck_item] = await Promise.all([
    read_raw_item(raw_fd, 12, index),
    read_pck_item(pck_fd, item_size_in_bits, index, revision_size_in_bits),
  ])

  return {
    ok: raw_item.ok && pck_item.ok    // read a value from both sources ok
      && raw_item.od == pck_item.od   // the offsets match
      && raw_item.rd == pck_item.rd,  // the revisions match
    off: raw_item.od,
    rev: raw_item.rd
  };
}

async function read_raw_item(fd, numbytes, index) {
  const b = new Buffer(numbytes)

  const offset_bytes = numbytes * index
  
  const x = await read(fd, b, 0, numbytes, offset_bytes)

  let l = b.readUInt32LE(0)
  let h = b.readUInt32LE(4)
  let r = b.readUInt32LE(8)
  
  let o = Math.pow(2, 32) * h + l

  return { od:o, rd:r, ok:x.bytesRead == numbytes }
}

async function read_pck_item(fd, numbits, index, revision_size_in_bits) {
  const b = new Buffer(8)

  const offset_bits = numbits * index
  const offset_bytes = Math.floor(offset_bits / 8)
  const offset_mod = offset_bits % 8

  const x = await read(fd, b, 0, 8, offset_bytes)

  // mask off the upper bits that belong to the previous entry
  if (offset_mod) {
    const mask = 0xff >> offset_mod
    b[0] &= mask
  }

  let h = b.readUInt32BE(0)
  let l = b.readUInt32BE(4)

  const rightshift = 64 - numbits - offset_mod
  const leftshift = 32 - rightshift

  if (rightshift >= 32) throw "shifty"

  if (rightshift != 0) {
    const t = (h << leftshift) >>> 0
    h >>>= rightshift
    l >>>= rightshift
    l = (l | t) >>> 0
  }

  const u = unpack(h, l, revision_size_in_bits)

  return { od: u.o, rd: u.r, ok: x.bytesRead == 8 }
}

// unpack offset and revision from packed dump offset struct
function unpack(h, l, revision_size_in_bits) {
  const rightshift = revision_size_in_bits
  const leftshift = 32 - rightshift

  // TODO explain why shifting right by "leftshift" is correct
  // TODO also do we risk using an all-zero mask instead of an all-one mask when rightshift is zero?
  const mask = 0xffffffff >>> leftshift
  const r = (l & mask) >>> 0

  const t = (h << leftshift) >>> 0
  h >>>= rightshift
  l >>>= rightshift
  l = (l | t) >>> 0

  let o = Math.pow(2, 32) * h + l

  return { o, r }
}

// dump Buffer to log as sequence of 8-bit binary strings
const bufToBin = (b, len = 64) => b.reduce((acc, cur, i) => {
  if (typeof len === "undefined" || i < len) {
    let bin = cur.toString(2)
    bin = '0'.repeat(8 - bin.length) + bin
    acc.push(bin)
  }
  return acc
}, []).join(' ')

function to_bits(x, l=32) {
  let b = x.toString(2)
  //console.log({x, l, b, bl:b.length, z:l - b.length})
  let bits = "0".repeat(l/*32*/ - b.length) + b

  let arr = []
  while (true) {
    let x = bits.substr(-8)
    if (x.length) arr.unshift(x)
    if (x.length == 8)
      bits = bits.substring(0, bits.length - 8)
    else
      break
  }

  return arr.join('-')
}

function hl_to_bits(h, l) {
  let b = h.toString(2)
  let bits = "0".repeat(32 - b.length) + b
  b = l.toString(2)
  bits += "-" + "0".repeat(32 - b.length) + b

  return bits
}

main()
