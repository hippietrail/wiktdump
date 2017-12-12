'use strict';

const fs = require('fs'),
    util = require('util'),
  Bunzip = require('seek-bzip');

function processCommandline(thenCallback) {
  if (process.argv.length >= 6) {  // 0 is node.exe, 1 is wikibsearch.js

    const opts = {
      wikiPath: "",
      indexPath: "",
    };

    for (let i = 2, a = 2; i < process.argv.length; ++i) {
      const arg = process.argv[i];

      if (arg.charAt(0) === '-') {
        if (arg.charAt(1) === '-') {
          let [ db, val ] = arg.substr(2).split("=");

          if (db === 'page-info') {
            opts.getPageInfo = true;
          } else if (db === 'num-langs') {
            opts.getNumLangs = true;
          } else if (db === 'lang-names') {
            opts.getLangNames = true;
          } else if (db === 'named-langs' || db === 'named-languages') {
            if (val) opts.getNamedLangs = val.split(",");
          } else if (db === 'num-sections') {
            opts.getNumSections = true;
          } else if (db === 'section-names') {
            opts.getSectionNames = true;
          } else if (db === 'named-sections') {
            if (val) opts.getNamedSections = val.split(",");
          } else if (db === 'parse-translations') {
            opts.parseTranslations = true;
          } else if (db === 'named-translations') {
            if (val) opts.getNamedTranslations = val.split(",");
          } else {
            console.log(`unknown double-hyphen switch: ${db}`);
          }
        } else {
          const sw = arg.substr(1);
          if (sw === 'd') {
            opts.debug = true;
          } else {
            console.log(`unknown switch: ${sw}`);
          }
        }
      } else {
        if (a == 2) {
          opts.wikiLang = arg;
        } else if (a == 3) {
          opts.wikiProj = arg;
        } else if (a == 4) {
          opts.wikiDate = arg;
        } else if (a == 5) {
          opts.searchTerm = arg;
        } else {
          console.log(`unknown arg: ${arg}`);
        }
        a++;
      }
    }

    // if there are no other options dump the full raw wikitext
    if (Object.keys(opts).length === 6) {
      opts.fullRaw = true;
    }

    thenCallback(opts);
  } else {
    console.error('usage: node wikibsearch language project dumpdate searchterm');
  }
}

function getPaths(opts, thenCallback) {
  // cross-platform for at least Windows and *nix (but possibly not Mac OS X)
  // http://stackoverflow.com/questions/9080085/node-js-find-home-directory-in-platform-agnostic-way
  var home = process.env[(process.platform == 'win32') ? 'USERPROFILE' : 'HOME'],
      wikipathpath = home + '/.wikipath';

  fs.exists(wikipathpath, x => {
    if (x) {
      var str = fs.createReadStream(wikipathpath, {start: 0, end: 1023});

      // TODO ~/.wikipath only has wikiPath - indexPath hardcoded for now!
      // TODO support multiple wikiPaths and indexPaths
      // TODO  (I have Wiktionaries on C: and Wikipedia on external harddrive)
      if (process.platform === 'sunos') {
        opts.indexPath = '/mnt/user-store/hippietrail/';
      } else {
        opts.indexPath = opts.wikiPath;
      }

      str.on('data', data => {
        opts.wikiPath = data.toString('utf-8').replace(/^\s*(.*?)\s*$/, '$1');
      });

      str.on('end', () => {
        fs.exists(opts.wikiPath, x => {
          let fullDumpPath;

          if (x) {
            opts.debug && console.log('path to wikis "' + opts.wikiPath + '" exists');

            const dumpFileName = opts.wikiLang + opts.wikiProj + '-' + opts.wikiDate + '-pages-articles.xml';
            fullDumpPath = opts.wikiPath + dumpFileName;

            fs.exists(fullDumpPath, x => {
              if (x) {
                thenCallback({ fullDumpPath, type: "xml"});
              } else {
                fs.exists(fullDumpPath + '.bz2', x => {
                  if (x) {
                    thenCallback({ fullDumpPath, type: "bz2"});
                  } else {
                    console.error(`neither ${dumpFileName} nor ${dumpFileName}.bz2 exists`);
                  }
                });
              }
            });
          } else {
            console.error('path to wikis "' + opts.wikiPath + '" doesn\'t exist');
          }
        });
      });

    } else {
      console.error('no ".wikipath" in home');
    }
  });
}

function sanityCheck(opts, dump, searchTerm, thenCallback) {
  let sane = false;
  
  const gotBz = "fd" in dump.files.bz && "byteLen" in dump.files.bz;
  const gotZt = "fd" in dump.files.zt && "byteLen" in dump.files.zt;

  if (dump.type === "xml"|| (dump.type === "bz2" && gotBz && gotZt)) {
    // how it always should be, but isn't on mac
    if (dump.files.to.byteLen / dump.files.st.byteLen === 4 / 4
      && dump.files.to.byteLen % 4 === 0
      && dump.files.do.byteLen / dump.files.to.byteLen === 12 / 4) {

        opts.debug && console.log("sane non-mac index");
      
        sane = true;
        dump.sizeof_txt_told = 4;

    // on macOS sizeof poff 8 roff 4 txt_told 8 index (int) 4
    } else if (
      dump.files.to.byteLen / dump.files.st.byteLen === 8 / 4
        && dump.files.to.byteLen % 8 === 0
        && dump.files.do.byteLen / dump.files.to.byteLen === 12 / 8) {

      opts.debug && console.log("sane mac index");

      sane = true;
      dump.sizeof_txt_told = 8;
    }
  }

  if (sane) {
    thenCallback(opts, dump, searchTerm);
  } else {
    console.log('sanity check fail');
  }
}

function getTitle(dump, indexS, gotTitle) {
  var haystackLen = dump.files.to.byteLen / dump.sizeof_txt_told;
  var indexR = new Buffer(4), offset = new Buffer(dump.sizeof_txt_told), title = new Buffer(256);

  if (indexS < 0 || indexS >= haystackLen) {
    if (indexS === -1) {
      gotTitle('** beginning of list **');
    } else if (indexS === haystackLen) {
      gotTitle('** end of list **');
    } else {
      throw 'sorted index ' + indexS + ' out of range';
    }
  } else {
    // st: sorted titles all-idx.raw
    fs.read(dump.files.st.fd, indexR, 0, 4, indexS * 4, (err, bytesRead, data) => {
      if (!err && bytesRead === 4) {
        indexR = data.readUInt32LE(0);

        if (indexR < 0 || indexR >= haystackLen) throw 'raw index ' + indexR + ' out of range (sorted index ' + indexS + ')';

        // to: title offsets all-off.raw
        fs.read(dump.files.to.fd, offset, 0, dump.sizeof_txt_told, indexR * dump.sizeof_txt_told, (err, bytesRead, data) => {
          if (!err && bytesRead === dump.sizeof_txt_told) {
            const lower = data.readUInt32LE(0);
            const upper = data.readUInt32LE(4);

            offset = upper * Math.pow(2, 32) + lower;

            if (upper > 2097151) console.warn(`** title offset ${offset} (${parseInt(+upper, 16)} : ${parseInt(+lower, 16)}) is greater than 53-bits!`);
            if (offset < 0 || offset >= dump.files.t.byteLen) throw 'title offset ' + offset + ' out of range';

            // t: titles all.txt
            fs.read(dump.files.t.fd, title, 0, 256, offset, (err, bytesRead, data) => {
              if (!err && bytesRead > 0) {
                title = data.toString('utf-8');
                var spl = title.split(/\r?\n/);
                if (spl.length < 2) throw 'didn\'t read a long enough string';

                gotTitle(spl[0]);
              } else {
                console.error(`failed to read title from offset ${offset}`);
              }
            });
          }
        });
      } else { throw ['indexR', err, bytesRead]; }
    });
  }
}

// get the information of the given page other than the latest revision/text
function getPageInfo(dump, indexS, gotPageInfo) {
  var haystackLen = dump.files.to.byteLen / dump.sizeof_txt_told;
  var indexR = new Buffer(4), record = new Buffer(12);

  if (indexS < 0 || indexS >= haystackLen) {
    if (indexS === -1) {
      gotTitle('** beginning of list **');
    } else if (indexS === haystackLen) {
      gotTitle('** end of list **');
    } else {
      throw 'sorted index ' + indexS + ' out of range';
    }
  } else {
    // st: sorted titles all-idx.raw
    fs.read(dump.files.st.fd, indexR, 0, 4, indexS * 4, (err, bytesRead, data) => {
      if (!err && bytesRead === 4) {
        indexR = data.readUInt32LE(0);

        if (indexR < 0 || indexR >= haystackLen) throw 'raw index ' + indexR + ' out of range (sorted index ' + indexS + ')';

        // do: dump offsets off.raw
        fs.read(dump.files.do.fd, record, 0, 12, indexR * 12, (err, bytesRead, data) => {
          if (!err && bytesRead === 12) {
            var lower, upper, offset, revisionOffset;

            lower = data.readUInt32LE(0);
            upper = data.readUInt32LE(4);
            offset = upper * Math.pow(2, 32) + lower;

            if (upper > 2097151) console.warn(`** dump offset ${offset} (${parseInt(+upper, 16)} : ${parseInt(+lower, 16)}) is greater than 53-bits!`);

            if (offset < 0 || offset >= dump.files.d.byteLen) {
              throw 'dump offset ' + offset + ' out of range';
            }

            var info = dump.type === "xml"
              ? { off: offset, xml: { chunk: Buffer(1024) } }
              : { off: offset, bz2: {} };

            var slab = '';
            (function readMore(info) {
              readSome(dump, info, data => {

                slab += data.toString('utf-8');

                var end = slab.indexOf('</page>');

                if (end === -1) {
                  readMore(info);
                } else {
                  // TODO could there be a corner case where a chunk ends right between the > and \n
                  end = slab.indexOf('\n', end);
                  if (end !== -1) {
                    var pageInfo = slab.substring(0, end + 1);
                    var mch = pageInfo.match(/(  <page[^>]*>[\s\S]*)<text[^>]*>[\s\S]*<\/text>\s*([\s\S]*<\/page>)/m);
                    if (mch) {
                      gotPageInfo(mch[1] + mch[2]);
                    } else {
                      throw 'got page but didn\'t extract info';
                    }
                  } else {
                    throw 'didn\'t get \\n';
                  }
                }

              });
            })(info);
          }
        });
      } else { throw ['indexR', err, bytesRead]; }
    });
  }
}

// get the <text> of the most recent <revision> of the page
function getArticle(dump, indexS, gotArticle) {
  var haystackLen = dump.files.to.byteLen / dump.sizeof_txt_told;
  var indexR = new Buffer(4), record = new Buffer(12);

  if (indexS < 0 || indexS >= haystackLen) {
    if (indexS === -1) {
      gotTitle('** beginning of list **');
    } else if (indexS === haystackLen) {
      gotTitle('** end of list **');
    } else {
      throw 'sorted index ' + indexS + ' out of range';
    }
  } else {
    // st: sorted titles all-idx.raw
    fs.read(dump.files.st.fd, indexR, 0, 4, indexS * 4, (err, bytesRead, data) => {
      if (!err && bytesRead === 4) {
        indexR = data.readUInt32LE(0);

        if (indexR < 0 || indexR >= haystackLen) throw 'raw index ' + indexR + ' out of range (sorted index ' + indexS + ')';

        // do: dump offsets off.raw
        fs.read(dump.files.do.fd, record, 0, 12, indexR * 12, (err, bytesRead, data) => {
          if (!err && bytesRead === 12) {
            var lower, upper, offset, revisionOffset;

            lower = data.readUInt32LE(0);
            upper = data.readUInt32LE(4);
            offset = upper * Math.pow(2, 32) + lower;

            if (upper > 2097151) console.warn(`** dump offset ${offset} (${parseInt(+upper, 16)} : ${parseInt(+lower, 16)}) is greater than 53-bits!`);

            revisionOffset = data.readUInt32LE(8);

            // skip to latest <revision>
            offset += revisionOffset;

            if (offset < 0 || offset >= dump.files.d.byteLen) {
              throw 'dump offset ' + offset + ' out of range';
            }

            var info = dump.type === "xml"
              ? { off: offset, xml: { chunk: Buffer(1024) } }
              : { off: offset, bz2: {} };

            var slab = '';
            (function readMore(info) {
              readSome(dump, info, data => {

                slab += data.toString('utf-8');

                var end = slab.indexOf('</revision>');

                if (end === -1) {
                  readMore(info);
                } else {
                  // TODO could there be a corner case where a chunk ends right between the > and \n
                  end = slab.indexOf('\n', end);
                  if (end !== -1) {
                    var revision = slab.substring(0, end + 1);
                    var [, mch] = revision.match(/<text[^>]*>([\s\S]*)<\/text>/m);
                    if (mch) {
                      gotArticle(mch);
                    } else {
                      throw 'got revision but didn\'t extract text';
                    }
                  } else {
                    throw 'didn\'t get \\n';
                  }
                }

              });
            })(info);
          }
        });
      } else { throw ['indexR', err, bytesRead]; }
    });
  }
}

function readSome(dump, info, callback) {
  if ("xml" in info) {
    // d: dump pages-articles.xml
    fs.read(dump.files.d.fd, info.xml.chunk, 0, 1024, info.off, (err, bytesRead, data) => {
      if (!err && bytesRead > 0) {
        info.off += 1024;
        callback(data)
      } else {
        console.log('dump read err, bytesRead', err, bytesRead);
      }
    });
  } else if ("bz2" in info) {
    // first use of seek-bzip2
    if (!("blockNum" in info.bz2)) {
      info.bz2.table = loadTable(dump);
      info.bz2.brs = createBitReaderStream(dump.files.bz.fd);

      info.bz2.blockNum = getBzipBlockNum(info.bz2.table, info.off);
      info.bz2.offsetIntoBlock = info.off - info.bz2.table[info.bz2.blockNum][0];
    } else {
      // subsequent use of seek-bzip2 - due to a bug in seek-bzip2 or a simplistic stream implementation?
      info.bz2.brs.seek(0);
    }

    const dataSlice = Bunzip.decodeBlock(info.bz2.brs, info.bz2.table[info.bz2.blockNum][1]).slice(info.bz2.offsetIntoBlock);

    info.bz2.blockNum += 1;
    info.bz2.offsetIntoBlock = 0;

    callback(dataSlice);
  }
}

// loads the seek-bzip table that makes random access of .bz2 files possible
function loadTable(dump) {
  let { table } = fs.readFileSync(dump.files.zt.fd).toString().split("\n").reduce((acc, line) => {
    if (line) {
      const [ , pos, size ] = line.match(/^(\d+)\t(\d+)$/);

      acc.table.push([+acc.offset, +pos, +size]);
      acc.offset += +size;
  }
    return acc;
  }, {offset: 0, table:[]});

  return table;
}

const createBitReaderStream = function(in_fd) {
  var stat = fs.fstatSync(in_fd);
  var stream = {
      buffer: new Buffer(4096),
      filePos: null,
      pos: 0,
      end: 0,
      _fillBuffer: function() {
          this.end = fs.readSync(in_fd, this.buffer, 0, this.buffer.length,
                  this.filePos);
          this.pos = 0;
          if (this.filePos !== null && this.end > 0) {
              this.filePos += this.end;
          }
      },
      readByte: function() {
          if (this.pos >= this.end) { this._fillBuffer(); }
          if (this.pos < this.end) {
              return this.buffer[this.pos++];
          }
          return -1;
      },
      read: function(buffer, bufOffset, length) {
          if (this.pos >= this.end) { this._fillBuffer(); }
          let bytesRead = 0;
          while (bytesRead < length && this.pos < this.end) {
              buffer[bufOffset++] = this.buffer[this.pos++];
              bytesRead++;
          }
          return bytesRead;
      },
      seek: function(seek_pos) {
          this.filePos = seek_pos;
          this.pos = this.end = 0;
      },
      eof: function() {
          if (this.pos >= this.end) { this._fillBuffer(); }
          return !(this.pos < this.end);
      }
  };
  if (stat.size) {
    stream.size = stat.size;
  }
  return stream;
};

function getBzipBlockNum(table, dumpCharOffset) {
  function bs(t, key, min, max) {
    if (max < min) {
      return max; // exact position is within the block starting at "max"
    } else {
      const mid = Math.floor((min + max) / 2);
      const value = table[mid][0];

      if (value > key) {
        return bs(t, key, min, mid-1);
      } else if (value < key) {
        return bs(t, key, mid+1, max);
      } else {
        return mid; // exact position is "mid"
      }
    }
  }
  return bs(table, dumpCharOffset, 0, table.length - 1);
}

function bsearch(dump, searchTerm, callback) {
  function bs(A, key, imin, imax, cb) {
    // test if array is empty
    if (imax < imin) {
      // set is empty, so return value showing not found
      cb({ok:false, a:imax, b:imin});
    } else {
      // calculate midpoint to cut set in half
      var imid = Math.floor((imin + imax) / 2);

      getTitle(A, imid, Aimid => {

        // three-way comparison
        if (Aimid > key) {
          // key is in lower subset
          bs(A, key, imin, imid-1, cb);
        } else if (Aimid < key) {
          // key is in upper subset
          bs(A, key, imid+1, imax, cb);
        } else {
          // key has been found
          cb({ok:true, a:imid, b:imid});
        }
      });
    }
  }

  bs(dump, searchTerm, 0, dump.files.to.byteLen / dump.sizeof_txt_told - 1, callback);
}

// main
processCommandline(opts => {
  getPaths(opts, (dump) => {
    opts.debug && console.log(`dump "${dump.fullDumpPath}" exists (${dump.type})`);

    // open dump file and index files

    dump.files = {
      d:  { desc: 'dump',            fmt: '%s%s' + opts.wikiProj + '-%d-pages-articles.xml' },
      do: { desc: 'dump offsets',    fmt: '%s%s%d-off.raw' },
      t:  { desc: 'titles',          fmt: '%s%s%d-all.txt' },
      to: { desc: 'title offsets',   fmt: '%s%s%d-all-off.raw' },
      st: { desc: 'sorted titles',   fmt: '%s%s%d-all-idx.raw' },

      bz: { desc: 'compressed dump', fmt: '%s%s' + opts.wikiProj + '-%d-pages-articles.xml.bz2' },
      zt: { desc: 'bzip table',      fmt: '%s%s%d-table.txt' },
    };

    let left = Object.keys(dump.files).length;
    for (var k in dump.files) {
      (e => {
        var p = util.format(e.fmt, opts.wikiPath, opts.wikiLang, opts.wikiDate);

        // TODO these are never closed!
        fs.open(p, 'r', (err, fd) => {
          if (err) {
            //console.error('error opening ' + e.desc + ' file): ' + err);
            opts.debug && console.log('... ' + e.desc + ' file NOPE');
            
            if (--left === 0) {
              sanityCheck(opts, dump, opts.searchTerm, processFiles);
            }
          } else {
            e.fd = fd;
            fs.fstat(fd, (err, stats) => {
              if (err) {
                throw 'fs.fstat() failed';
              } else {
                e.byteLen = stats.size;

                opts.debug && console.log('... ' + e.desc + ' file OK (' + e.byteLen + ')');
              }

              if (--left === 0) {
                sanityCheck(opts, dump, opts.searchTerm, processFiles);
              }
            });
          }
        });
      })(dump.files[k]);
    }
  });
});

function processFiles(opts, dump, searchTerm) {
  bsearch(dump, searchTerm, result => {
    var before, after,
      gotNearby = () => {
        console.log('"' + searchTerm + '" belongs between "' + before + '" and "' + after + '"');
      };

    if (result.a === result.b) {
      getTitle(dump, result.a, t => {
        if (opts.debug)
          console.log('"' + searchTerm + '" found at ' + result.a);
        else
        console.log('"' + searchTerm + '"');

        if ("getPageInfo" in opts) {
          var pageInfo = getPageInfo(dump, result.a, pageInfo => {
            gotPageInfo(opts, pageInfo);
          });
        } else {
          var article = getArticle(dump, result.a, article => {
            gotArticle(opts, article);
          });
        }
      });
    } else {
      getTitle(dump, result.a, t => {
        before = t;
        if (after) gotNearby();
      });
      getTitle(dump, result.b, t => {
        after = t;
        if (before) gotNearby();
      });
    }
  });
}

function gotPageInfo(opts, pageInfo) {
  console.log(pageInfo);
}

function gotArticle(opts, article) {
  let prologAndLangSections;

  if ("getNumLangs" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);

    console.log(`prolog: ${prologAndLangSections.prolog ? "yes" : "no" }, number of language sections: ${prologAndLangSections.langSections.length}`);
  }

  if ("getLangNames" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);
    const ls = prologAndLangSections.langSections;

    const langNames = ls.map(s => {
      const [ , langName ] = s.match(/^==\s*([^=]*)\s*==$/m);
      return langName;
    })
    
    if (prologAndLangSections.prolog) langNames.unshift("prolog");

    console.log(`language sections: ${langNames}`)
  }

  if ("getNamedLangs" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);

    if (opts.getNamedLangs.indexOf("prolog") !== -1) {
      if (prologAndLangSections.prolog) {
        console.log(`"${opts.searchTerm}" : prolog`);
        console.log('-------\n' + prologAndLangSections.prolog + '-------');
      }
    }

    const sections = prologAndLangSections.langSections;

    for (let i = 0; i < sections.length; i++) {
      const langsec = sections[i];

      const [ , langName ] = langsec.match(/^==\s*([^=]*)\s*==$/m);

      if (opts.getNamedLangs.indexOf(langName) !== -1) {
        console.log(`"${opts.searchTerm}" : ${langName}`);
        //console.log('-------\n' + langsec + '-------');
        console.log(langsec);
      }
    }
  }
        
  if ("getNumSections" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);
    
    const sections = prologAndLangSections.langSections;
    
    for (let i = 0; i < sections.length; i++) {
      const langsec = sections[i];
      
      const [ , langName ] = langsec.match(/^==\s*([^=]*)\s*==$/m);

      const mch = langsec.match(/^(===+)[^=]*\1/gm);

      console.log(`"${opts.searchTerm}" / ${langName} / number of sections: ${mch.length}`);
    }
  }

  if ("getSectionNames" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);
    
    const sections = prologAndLangSections.langSections;
    
    for (let i = 0; i < sections.length; i++) {
      const langsec = sections[i];
      
      const [ , langName ] = langsec.match(/^==\s*([^=]*)\s*==$/m);

      const mch = langsec.match(/^(===+)[^=]*\1/gm);

      const sectionNames = mch.map( foo => {
        const [ , eq, name ] = foo.match(/^(===+)\s*([^=]*)\s*\1/m);
        return [ eq.length, name ];
      });

      console.log(`"${opts.searchTerm}" / ${langName} / sections: ${sectionNames}`);
    }
  }

  if ("getNamedSections" in opts) {
    if (!prologAndLangSections) prologAndLangSections = splitArticleIntoPrologAndLangSections(article);

    const sections = prologAndLangSections.langSections;

    for (let i = 0; i < sections.length; i++) {
      const langsec = sections[i];

      const ob = splitLangSectionIntoSections(langsec);

      for (let j = 0; j < ob.sections.length; ++j) {
        const sec = ob.sections[j];

        if (opts.getNamedSections.indexOf(sec.heading) !== -1) {

          if (sec.heading === "Translations" && "parseTranslations" in opts) {
            const parsedTranslations = parseTranslationsSection(sec);

            let filteredTranslations;

            if ("getNamedTranslations" in opts)
              filteredTranslations = filterTranslations(parsedTranslations, opts.getNamedTranslations);
            else
              filteredTranslations = parsedTranslations;

            console.log(JSON.stringify(filteredTranslations, null, "  "))
          } else {
            if (j === 0) console.log("!!!!!!!!!!!!!");
            console.log(`/-------${j}-------\\`);
            console.log(`| •  ${sec.heading}    |`);
            if (sec.body) {
              console.log(`|=======${j}=======|`);
              console.log(sec.body);
            }
            console.log(`\\≡≡≡≡≡≡≡${j}≡≡≡≡≡≡≡/`);
            if (j === 0) console.log("!!!!!!!!!!!!!");
          }
        }
      }
    }
  }

  if ("fullRaw" in opts) {
    console.log(article.replace(/&[^;]*;/g, t => {
      const k = t.substring(1, t.length - 1);

      const x = {
        amp: "&",
        //apos: "'",
        gt: ">",
        lt: "<",
        quot: "\"",
      }

      if (k in x) return x[k];

      console.warn(`** can't ampersand decode ${t}`);

      return t;
    }));
  }
};

function splitArticleIntoPrologAndLangSections(article) {
  let hasArticleProlog = false;
  let articleProlog = null;

  const rawSections = article.split(/^----\n/m);

  for (let i = 0; i < rawSections.length; i++) {
    let [prolog, body] = rawSections[i].split(/^==[^=]*==$/m);

    if (!prolog || /^\s+$/.test(prolog))
      prolog = null;
    if (!body || /^\s+$/.test(body))
      body = null;

    if (prolog) {
      if (i === 0) {
        hasArticleProlog = true;
        articleProlog = prolog;

        rawSections[0] = rawSections[0].substr(articleProlog.length);
      } else
        console.warn(`##${i} unexpected prolog <<<${prolog}>>>`);
    }
    if (!body)
      console.warn(`##${i} body <<<no body!>>>`);
  }

  return {
    prolog: articleProlog,
    langSections: rawSections
  };
}

function splitLangSectionIntoSections(langSection) {
  const [ , langName ] = langSection.match(/^==\s*([^=]*)\s*==$/m);

  const retval = { langName, sections: [] };

  const headings = langSection.match(/^(==+)[^=]*\1/gm).map(h => {
    const [ , name ] = h.match(/^==+\s*([^=]*)\s*==+$/m);
    return name;
  });

  const bodies = langSection.split(/^==+[^=]*==+$/m).map(b => b.trim());

  for (let j = 0; j < bodies.length; j++) {
    const heading = j === 0 ? "prolog" : headings[j-1];
    const body = bodies[j];

    retval.sections.push({ index: j, heading, body });
  }
  return retval;
}

// parse an entire ====Translations==== section
function parseTranslationsSection(section) {
  let tables = section.body.match(/^{{(?:check)?trans-top(?:-also)?(?:\|.*?)?}}[\s\S]*?^{{trans-bottom}}/mg);

  if (!tables) {
    tables = section.body.match(/^{{trans-see\|.*?}}/mg);

    if (!tables) {
      console.log("UNEXPECTED 827", section.body);
    }
  }

  const tablemap = tables.map(table => parseTranslationTable(table));

  return tablemap;
}

// parse a single translation section {{trans-top}} ... {{trans-bottom}}
function parseTranslationTable(table) {
  let mch = table.match(/^{{(check)?trans-top(?:-(also))?(?:\|id=(.*?))?(?:\|(.*?))?}}\n([\s\S]*?)\n{{trans-bottom}}/);

  let check, also, id, gloss, rawList, see;

  let retval = { err: "unfilled retval" }

  if (mch) {
    [ , check, also, id, gloss, rawList ] = mch;
    let list = rawList.replace(/^{{(?:check)?trans-mid}}\n/mg, "") // remove trans-mid
                       //.replace(/^\&lt;.*\&gt;\n/mg, "");        // TODO what's this for??

    const arr = list.match(/^\*.*(\n\*:.*)*/mg);

    // there are tables with glosses but no translations
    if (!arr) {
      retval = { check, also, id, gloss };
    } else {
      const arr2 = arr.map(item => {
        let langName, langItemsRaw, sublangsRaw;

        [, langName, langItemsRaw, sublangsRaw] = item.match(/^\*\s*(.*?)\s*:(?: *(.*))(?:\n([\s\S]*))?/m)

        // lang with direct trans only, no sublangs
        if (langItemsRaw && !sublangsRaw) {
          const tarr = parseTransDirectEntries(langName, langItemsRaw);

          return { name: langName, trans: tarr };
        }

        // lang with sublangs only, no direct trans
        else if (!langItemsRaw && sublangsRaw) {
          const obj = parseTransSublangs(langName, sublangsRaw);

          return { name: langName, subs: obj };
        }

        else if (langItemsRaw && sublangsRaw) {
          const tarr = parseTransDirectEntries(langName, langItemsRaw);
          const subs = parseTransSublangs(langName, sublangsRaw);

          return { name: langName, trans: tarr, subs };
        }

        else return { name: langName, err: "UNEXPECTED neither direct trans entries nor sublangs" }
      });

      // convert array into object using language names as keys
      const obj2 = arr2.reduce((acc, cur) => {
        if (Object.keys(cur).filter(c => ["code", "err", "name", "subs", "trans"].indexOf(c) === -1).length !== 0) console.log("WOAH2", Object.keys(cur));

        const key = cur.name || cur.code;
        let val = {}

        if ("subs" in cur) {
          val.sublangs = cur.subs;
          if ("trans" in cur) {
            val.trans = cur.trans;
          }
        } else if ("trans" in cur) {
          val.trans = cur.trans;
        } else {
          if ("err" in cur) val.err = cur.err;
          else val.err = "no subs or trans but no error either"
        }

        acc[key] = val;

        return acc;
      }, {});
  
      return { check, also, id, gloss, langs: obj2 };
    }
  }
  return retval;
}

function parseTransDirectEntries(langName, langItemsRaw) {
  let arr = langItemsRaw.match(/{{t.*?}}(?: {{[^t].*?}})?/g);

  if (arr) return arr.map(tr => matchTTemplate(tr));
  
  let mch;

  arr = langItemsRaw.match(/\[\[.*?\]\] {{g\|.}} {{qualifier\|.*?}}/g);

  if (arr) return arr.map(tr => matchTNonTemplate(tr));

  mch = langName.match(/^{{ttbc\|([a-z][a-z][a-z]?)}}$/);

  if (mch && mch[1])
    return { code: mch[1], err: langItemsRaw, line: 928 }

  return { name: langName, err: langItemsRaw, line: 930 };
}

function parseTransSublangs(langName, sublangsRaw) {
  const subs = sublangsRaw.match(/^\*.*/mg).map(subitem => {
    const mch = subitem.match(/^\*:\s*(.*?)\s*:\s*(.*)/);

    if (!mch) {
      // TODO this return is not yet handled by the caller
      return { line: 939, subitem };
    }

    const [, lang2, subs] = mch;

    const tarr0 = subs.match(/{{t.*?}}(?: {{[^t].*?}})?/g);

    if (!tarr0)
      return { subname: lang2, subs };

    const tarr = tarr0.map(tr => matchTTemplate(tr));

    if (langName === "Chinese") {
      let tr;

      for (let i = tarr.length - 1; i >= 0; --i) {
        if ("tr" in tarr[i]) {
          tr = tarr[i].tr;
          if (tr.includes("[[") && tr.includes("]]")) {
            tr = tr.substring(2, tr.length-2)
            tarr[i]._tr = tr;
          }
        }
        else if (tr)
          tarr[i]._tr = tr;
      }
    }

    return { subname: lang2, tarr };
  });

  const obj = subs.reduce((acc, cur) => {
    // TODO why does this never happen?
    if (("tarr" in cur) && ("subs" in cur)) console.log("** 976 cur has both tarr and subs")
    acc[cur.subname] = cur.tarr || cur.subs;
    return acc;
  }, {});

  return obj;
}

function matchTTemplate(temp) {
  const [, main, follow] = temp.match(/^{{(.*?)}}(?: {{(.*?)}})?$/);

  const mainspl = parseTTemplate(main);

  const followspl = follow && parseT2Template(follow);

  const retval = { ...mainspl, ...followspl };

  return retval;
}

function parseTTemplate(tt) {
  const p = parseTemplate(tt);
  const o = {};
  let langCode;
  
  Object.entries(p).forEach(e => {
    const [k, v] = e;

    switch (k) {
      case "0":
        if (v === 't-needed')
          o.needed = true;
        else if (!['t', 't-', 't+', 't-needed', 't-check', 't+check'].includes(v))
          o["unexpected 't' template"] = v;
        break;

      case "1":
        if (/[a-z][a-z][a-z]?/.test(v))
          langCode = v;
        else
          o["unexpected language code"] = v;
        break;

      case "2":
        if (/(?:\[\[|\]\])/.test(v)) {
          o.w = v.replace(/\[\[(?:[^\|\]]*\|)?([^\]]*)\]\]/g, "$1");
          o["with-wikilinks"] = v;
        } else
          o.w = v;
        break;

      case "3": case "4":
        if (["m", "f", "n", "c", "p", "m-p", "f-p", "n-p"].indexOf(v) !== -1) {
          if (!("g" in o))
            o.g = [];
          o.g.push(v);
        } else if (["impf", "pf"].indexOf(v) !== -1) {
          if (!("p" in o))
            o.p = [];
          o.p.push(v);
        } else {
          o["unpexpected gender"] = v;
        }
        break;

      case "alt":
        o.alt = v;
        break;

      case "sc":
        if (langCode === "cmn" && v !== "Hani")
          o.sc = v;
        break;

      case "tr":
        if (langCode === "ja") {
          const tr2 = v.split(/,\s*/);
          if (tr2.length === 2) {
            o._kana = tr2[0];
            o._tr = tr2[1];
          } else {
            o.tr = tr2[0]
          }
        } else
          o.tr = v;
        break;

      default:
        o["unpexpected template param"] = [k, v];
    }
  });

  return o;
}

// when there's a second template in a translation entry, it's usually a qualifier
function parseT2Template(t2) {
  const p = parseTemplate(t2);
  const o = {};

  if (p[0] === "qualifier" || p[0] === "q") {
    const v = [];
    for (let i = 1; i in p; ++i) {
      v[i-1] = p[i];
    }
    if (v.length) o.q = v;
  }
  else if (p[0] === "gloss")
    o.g = p[1];
  else
    o["unexpected translation follower"] = p[0];

  return o;
}

function parseTemplate(temp) {
  let t;

  // split is too dumb when there's wikilinks with pipes
  if (/\[.*?\|.*?\]/.test(temp))
    t = temp.match(/(?:[^\[\]\|]|(?:\[\[.*?(?:\|.*?)?\]\]))+/g);
  else
    t = temp.split("|");

  const r = t.map(x => x.split("=")).reduce((acc, item, n) => {
    if (item.length === 1)
      acc.ob[acc.index++] = item[0];
    else
      acc.ob[item[0]] = item[1];
    return acc;
  }, { index: 0, ob: {} });

  return r.ob;
}

// before translations were done with {{templates}} they were done with [[wikilinks]] 
function matchTNonTemplate(nonTemp) {
  const [, w, g, q] = nonTemp.match(/\[\[(.*?)\]\] {{g\|(.)}} {{qualifier\|(.*)?}}/);

  return { w, g: [g], q }
}

function filterTranslations(parsedTranslations, namedTranslations) {
  const filteredTranslations = parsedTranslations.reduce((acc, transTable, idx) => {
    if (!transTable) return { unexpected: { line: 1122, parsedTranslations } }

    if (!("langs" in transTable)) return acc;

    const langOb = {};
    const langAr = Object.keys(transTable.langs).filter(x => namedTranslations.map(n => {
        const sl = n.indexOf("/"), l = (sl === -1) ? n : n.substring(0, sl);
        return l;
      })
      .includes(x)
    );

    if (langAr.length) {
      langAr.forEach(langName => {
        const transLangEntryOb = transTable.langs[langName];
        //let filteredSublangs;

        //filteredSublangs = transLangEntryOb;
        if ("sublangs" in transLangEntryOb)
          /*filteredSublangs*/transLangEntryOb.sublangs = filterSublangs(transLangEntryOb.sublangs, namedTranslations);
        
        langOb[langName] = transLangEntryOb//filteredSublangs;
      });
      acc.push({ ...transTable, langs: langOb });
    }

    return acc;
  }, []);

  return filteredTranslations;
}

function filterSublangs(transSublangEntryOb, namedTranslations) {
  let filteredResult;

  const sublangOb = {};
  const sublangAr = Object.keys(transSublangEntryOb)
                          .filter(x => namedTranslations.map(n => {
                              const sl = n.indexOf("/"), r = n.substring(sl + 1);
                              return r;
                            }).includes(x)
                          );

  if (sublangAr.length) {
    sublangAr.forEach(sublangName => {
      const transSubangEntryOb = transSublangEntryOb[sublangName];

      sublangOb[sublangName] = transSubangEntryOb;
    });
    filteredResult = sublangOb;
  }
  
  return filteredResult;
}
