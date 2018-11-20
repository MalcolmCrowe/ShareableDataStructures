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

    public SDbObject GetOne(SDatabase d) throws Exception {
        synchronized (file) {
            if (position == file.length()) {
                return null;
            }
            rbuf = new Buffer(this, position);
            Serialisable r = _Get(d);
            position = rbuf.start + rbuf.pos;
            return (SDbObject) r;
        }
    }
    /// <summary>
    /// Called from Commit(): file is already locked
    /// </summary>
    /// <param name="tr"></param>
    /// <param name="pos"></param>
    /// <returns></returns>

    public SDbObject[] GetAll(SDatabase d, long pos, long max) throws Exception {
        List<SDbObject> r = new ArrayList<SDbObject>();
        position = pos;
        rbuf = new Buffer(this, pos);
        while (position < max) {
            r.add((SDbObject) _Get(d));
            position = rbuf.start + rbuf.pos;
        }
        return (SDbObject[]) r.toArray(new SDbObject[0]);
    }

    public SysItem Get(SDatabase d, long pos) throws Exception {
        if (pos == length) {
            return null;
        }
        position = pos;
        rbuf = new Buffer(this, position);
        var r = _Get(d);
        return new SysItem(r, rbuf.start + rbuf.pos);
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
        return length + wbuf.pos;
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
        file.write(b.buf, 0, b.pos);
        length = p + b.pos;
        b.pos = 0;
    }

    public void Flush() throws Exception {
        PutBuf(wbuf);
    }
}
