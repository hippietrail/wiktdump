"use strict"

// scans the three index files of a specified dump in their format
// reports the number of bits needed for each field to make packed binary versions of the index files

const { promisify } = require('util')
const fs = require('fs')
const stat = promisify(fs.stat)
const open = promisify(fs.open)
const read = promisify(fs.read)

const common = require('./common')

// minimum number of bits needed to represent the given int
function bits_needed(n) {
  let b = 0
  while (n)
    n = Math.floor(n / 2), ++b
  return b
}

async function getIndexFileMetadata(pathBase) {
  const indeces = [
    { p:'all-idx' }, // st
    { p:'all-off' }, // to
    { p:'off'     }, // do
  ]
  const ob = {
    st: indeces[0], // -all-idx.raw -> -all-idx.pck
    to: indeces[1], // -all-off.raw -> -all-off.pck
    do: indeces[2], // -off.raw     -> -off.pck
  }

  // for some reason you can't just do (await x).forEach() - you have to assign it to something
  // adds a field to each index object representing its file size in bytes
  let _ = (await Promise.all(indeces.map(ind => stat(pathBase + '-' + ind.p + '.raw'))))
          .forEach((v, i) => indeces[i].s = v.size)

  const all_off_int_size = verifyIndexFileMetadata(ob)

  if (!all_off_int_size)
    return { err: "not sane" }

  return { all_off_int_size, ob }
}

function verifyIndexFileMetadata(ob) {
  let all_off_int_size = undefined

  // is dump offset/revision file a multiple of 12 bytes long?
  if (ob.do.s % 12 == 0) {
    // simple ratio between file sizes should be 1:1:3 or 1:2:3
    const lens = Object.values(ob).map(v => v.s)
    const denom = lens.reduce((a, c) => common.gcd(a, c))
    const ratio = lens.map(i => i / denom)

    if (common.arrayCompare(ratio, [1,1,3])) {
      console.log("32-bit title (non-mac)")
      all_off_int_size = 4
    } else if (common.arrayCompare(ratio, [1,2,3])) {
      console.log("64-bit title (mac)")
      all_off_int_size = 8
    }
  }

  return all_off_int_size
}

async function scanIndexFile(whindex, pathBase, getval, track) {
  const elements_per_buf = 2048
  const buffer_size = whindex.element_size * elements_per_buf
  const b = new Buffer(buffer_size)

  const fd = await open(pathBase + '-' + whindex.p + '.raw', 'r')

  const freq = 1000000

  for (let chunk_num = 0, cont = true; cont; ++chunk_num) {
    const { bytesRead } = await read(fd, b, 0, buffer_size, null)

    if (bytesRead) {
      for (let i = 0; i < bytesRead / whindex.element_size; ++i) {
        const val = getval(b, i * whindex.element_size)
        if (track) track(whindex, val)

        const index = chunk_num * elements_per_buf + i
        if (index % freq === 0) console.log(index, whindex.p, val)
      }
    }

    if (bytesRead !== buffer_size) cont = false
  }
}

// all-idx // st // index
function get32BitVal(b, o) {
  const val = b.readUInt32LE(o + 0)
  return val
}

// all-off // to // title offset
function get64BitVal(b, o) {
  const lo = b.readUInt32LE(o + 0), hi = b.readUInt32LE(o + 4)
  return hi * Math.pow(2, 32) + lo
}

// off // do // dump offset
function getValDo(b, o) {
  const lo = b.readUInt32LE(o + 0)
  const hi = b.readUInt32LE(o + 4)
  const rev = b.readUInt32LE(o + 8)
  return [hi * Math.pow(2, 32) + lo, rev]
}

// all-idx // st // index & all-off // to // title offset - each is just an integer
function trackStTo(whindex, val) {
  whindex.min = "min" in whindex ? Math.min(whindex.min, val) : val
  whindex.max = "max" in whindex ? Math.max(whindex.max, val) : val
}

// off // do // dump offset - a packed struct of two integers of different bit widths
function trackDo(whindex, val) {
  whindex.minOff = "minOff" in whindex ? Math.min(whindex.minOff, val[0]) : val[0]
  whindex.maxOff = "maxOff" in whindex ? Math.max(whindex.maxOff, val[0]) : val[0]
  whindex.minRev = "minRev" in whindex ? Math.min(whindex.minRev, val[1]) : val[1]
  whindex.maxRev = "maxRev" in whindex ? Math.max(whindex.maxRev, val[1]) : val[1]
}

async function main() {
  const wikipath = common.getWikipath()
  const baseName = common.parseArgs().basename

  const pathBase = wikipath + baseName
  
  const metadata = await getIndexFileMetadata(pathBase)

  const { ob } = metadata

  ob.st.element_size = 4
  ob.to.element_size = metadata.all_off_int_size;
  ob.do.element_size = 12

  const element_count = ob.st.s / ob.st.element_size
  
  console.log(`element count: ${element_count}`)

  const getValTo = ob.to.element_size == 8 ? get64BitVal : get32BitVal

  await Promise.all([
    scanIndexFile(ob.st, pathBase, get32BitVal, trackStTo),
    scanIndexFile(ob.to, pathBase, getValTo, trackStTo),
    scanIndexFile(ob.do, pathBase, getValDo, trackDo),
  ])

  console.log("st", "min", ob.st.min, "max", ob.st.max, "bits needed", bits_needed(ob.st.max))
  console.log("to", "min", ob.to.min, "max", ob.to.max, "bits needed", bits_needed(ob.to.max))
  console.log("do.off", "min", ob.do.minOff, "max", ob.do.maxOff, "bits needed", bits_needed(ob.do.maxOff))
  console.log("do.rev", "min", ob.do.minRev, "max", ob.do.maxRev, "bits needed", bits_needed(ob.do.maxRev))

  if (ob.st.min == 0, ob.st.max == element_count - 1)
    console.log("the index file does range from zero to the expected number of elements minus one")
}

main()
