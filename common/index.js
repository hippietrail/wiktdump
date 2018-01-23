const fs = require('fs')

// greatest common denominator
const gcd = (a, b) =>
  Number.isNaN(a) || Number.isNaN(b) ? NaN
  : b === 0 ? a
  : gcd(b, a % b)
/*const gcd = (a, b) => {
  console.log(">>", a,b)
  if (Number.isNaN(a) || Number.isNaN(b)) { console.log("==",NaN); return NaN; }
  if (b === 0) { console.log("==", a); return a }
  
  return gcd(b, a % b)
}*/

module.exports = {
  getWikipath: function() {
    let res = ""
    const maxBytesToRead = 1024 // how much of the config file to read in bytes
    const buf = new Buffer(maxBytesToRead)
    try {
      const fd = fs.openSync(process.env[(process.platform == 'win32') ? 'USERPROFILE' : 'HOME'] + "/.wikipath", "r")
      const bytesRead = fs.readSync(fd, buf, 0, maxBytesToRead, null)
      let len = buf.indexOf('\n')
  
      if (len >= 1 && buf[len - 1] == 13) // len >= 1 and previous character is \\r so making it one byte less
        --len
      else if (len === -1 && bytesRead < maxBytesToRead) // len is -1 but we read the whole file so setting it to bytesRead
        len = bytesRead
      if (len !== -1) // len is set so we read .wikipath OK
        res = buf.asciiSlice(0, len)

      fs.closeSync(fd)
    } catch (e) {
      console.warn("**", e)
    }
    return res
  },

  parseArgs: function() {
    const opts = {}
    let lang, proj, date

    for (let i = 2, state = "start"; i < process.argv.length; ++i) {
      const arg = process.argv[i];
      
      if (arg.charAt(0) === '-') {
        let sk, lk, v
        if (arg.charAt(1) === '-') {
          [ lk, v ] = arg.substr(2).split("=")

          if (lk == "all-idx") sk = "st"
          else if (lk == "all-off") sk = "to"
          else if (lk == "debug") sk = "d"
          else if (lk == "dump-offsets") sk = "do"
          else if (lk == "off") sk = "do"
          else if (lk == "sorted-titles") sk = "st"
          else if (lk == "title-offsets") sk = "to"
        } else
          [ sk, v ] = arg.substr(1).split("=");

        if (sk == "d") opts.debug = true
        else if (sk == "do") {
          const m = v.match(/^(\d+):(\d+)$/)
          if (m)
            opts.do = { off: +m[1], rev: +m[2] }
          else
            console.log("** dump offset must be specified as two numbers separated by a colon")
        } else if (sk == "st") opts.st = +v
        else if (sk == "to") opts.to = +v
        else console.log(`** unknown switch: ${lk || sk}`)
      } else {
        if (state == "start") {
          let m

          if ((m = arg.match(/^([a-z][a-z][a-z]??)-?(?:([a-z]*?)-?)?(\d\d\d\d\d\d\d\d)$/))) {
            opts.basename = m[1] + m[3]
            state = "after-lang-proj-date"
          } else if (/^[a-z][a-z][a-z]?$/.test(arg)) {
            lang = arg
            state = "after-lang"
          } else {
            console.log(`unexpected commandline arg: ${arg} (state: ${state})`)
          }
        } else if (state == "after-lang") {
          if (/^\d\d\d\d\d\d\d\d$/.test(arg)) {
            date = arg
            state = "after-lang-and-date"
          } else if (/^[a-z]+$/.test(arg)) {
            proj = arg
            state = "after-lang-and-proj"
          } else {
            console.log(`unexpected commandline arg: ${arg} (state: ${state})`)
          }
        } else if (state == "after-lang-and-proj") {
          if (/^\d\d\d\d\d\d\d\d$/.test(arg)) {
            date = arg
            state = "after-lang-and-proj-and-date"
          } else {
            console.log(`unexpected commandline arg: ${arg} (state: ${state})`)
          }
        } else {
          console.log(`unexpected state: ${state}`)
        }
      }
    }

    if (lang && date) opts.basename = lang + date
    //console.log(opts)

    return opts
  },

  // greatest common denominator
  gcd: gcd,

  // lowest common multiple
  lcm: (a, b) => a * b / gcd(a, b),

  // shallow compare two one-dimensional arrays
  arrayCompare: (a, b) => a.length == b.length && a.every((v,i)=> v === b[i])
}  