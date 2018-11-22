/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * This class is not shareable
 *
 * @author Malcolm
 */
public class AStream extends StreamBase {

    public RandomAccessFile file;
    public String filename;
    long length = 0;
    SDict<Long, Long> uids = null; // used for movement of SDbObjects

    @Override
    protected long getLength() {
        return length;
    }

    public AStream(String path, String fn) throws IOException {
        file = new RandomAccessFile(new File(path, fn), "rws");
        filename = fn;
        wbuf = new Buffer(this);
        length = file.length();
        file.seek(0);
    }



    Serialisable Lookup(SDatabase db, long pos) 
    {
        return db.Lookup(Fix(pos));
    }

    long Fix(long pos) 
    {
        if (uids != null && uids.Contains(pos)) {
            pos = uids.Lookup(pos);
        }
        return pos;
    }

    long pos() {
        return length + wbuf.wpos;
    }

    public SDatabase Commit(SDatabase db, SDict<Integer, SDbObject> steps) throws Exception {
        wbuf = new Buffer(this);
        uids = new SDict<Long, Long>(-1L, -1L);
        for (var b = steps.First(); b != null; b = b.Next()) {
            var bs = b.getValue();
            switch (bs.val.type) {
                case Types.STable: {
                    var st = (STable) b.getValue().val;
                    var nt = new STable(st, this);
                    db = db._Add(nt, pos());
                    break;
                }
                case Types.SColumn: {
                    var sc = (SColumn) bs.val;
                    var st = (STable) Lookup(db, Fix(sc.table));
                    var nc = new SColumn(sc, this);
                    db = db._Add(nc, pos());
                    break;
                }
                case Types.SRecord: {
                    var sr = (SRecord) bs.val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SRecord(db, sr, this);
                    db = db._Add(nr, pos());
                    break;
                }
                case Types.SDelete: {
                    var sd = (SDelete) bs.val;
                    var st = (STable) Lookup(db, Fix(sd.table));
                    var nd = new SDelete(sd, this);
                    db = db._Add(nd, pos());
                    break;
                }
                case Types.SUpdate: {
                    var sr = (SUpdate) b.getValue().val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SUpdate(db, sr, this);
                    db = db._Add(nr, pos());
                    break;
                }
                case Types.SAlter: {
                    var sa = new SAlter((SAlter) b.getValue().val, this);
                    db = db._Add(sa, pos());
                    break;
                }
                case Types.SDrop: {
                    var sd = new SDrop((SDrop) b.getValue().val, this);
                    db = db._Add(sd, pos());
                    break;
                }
                case Types.SIndex: {
                    var si = new SIndex((SIndex) b.getValue().val, this);
                    db = db._Add(si, pos());
                    break;
                }
            }
        }
        Flush();
        SDatabase.Install(db);
        return db;
    }

    public void Close() throws IOException {
        file.close();
    }

    protected boolean GetBuf(Buffer b) throws Exception {
        if (b.start > length) {
            return false;
        }
        file.seek(b.start);
        var n = length - b.start;
        if (n > Buffer.Size) {
            n = Buffer.Size;
        }
        b.len = file.read(b.buf, 0, (int) n);
        return b.len > 0;
    }

    protected void PutBuf(Buffer b) throws Exception {
        var p = file.length();
        file.seek(p);
        file.write(b.buf, 0, b.wpos);
        length = p + b.wpos;
        b.wpos = 0;
    }

    public void Flush() throws Exception {
        PutBuf(wbuf);
    }
}
