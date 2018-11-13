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
    Buffer rbuf, wbuf;
    long length = 0;
    SDict<Long, Long> uids = null; // used for movement of SDbObjects

    @Override
    protected long getLength() {
        return length;
    }

    public AStream(String path, String fn) throws FileNotFoundException, IOException {
        file = new RandomAccessFile(new File(path, fn), "rws");
        filename = fn;
        rbuf = new Buffer(this);
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

    public Serialisable Get(SDatabase d, long pos) throws Exception {
        synchronized (file) {
            position = pos;
            rbuf = new Buffer(this, position);
            return _Get(d);
        }
    }

    Serialisable Lookup(SDatabase db, long pos) {
        return db.Lookup(Fix(pos));
    }

    long Fix(long pos) {
        if (uids != null && uids.Contains(pos)) {
            pos = uids.Lookup(pos);
        }
        return pos;
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
                    db = db._Add(nt, length);
                    break;
                }
                case Types.SColumn: {
                    var sc = (SColumn) bs.val;
                    var st = (STable) Lookup(db, Fix(sc.table));
                    db = db._Add(new SColumn(sc, this), length);
                    break;
                }
                case Types.SRecord: {
                    var sr = (SRecord) bs.val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    db = db._Add(new SRecord(db, sr, this), length);
                    break;
                }
                case Types.SDelete: {
                    var sd = (SDelete) bs.val;
                    var st = (STable) Lookup(db, Fix(sd.table));
                    db = db._Add(new SDelete(sd, this), length);
                    break;
                }
                case Types.SUpdate: {
                    var sr = (SUpdate) b.getValue().val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SUpdate(db, sr, this);
                    db = db._Add(nr, length);
                    break;
                }
                case Types.SAlter: {
                    var sa = new SAlter((SAlter) b.getValue().val, this);
                    db = db._Add(sa, length);
                    break;
                }
                case Types.SDrop: {
                    var sd = new SDrop((SDrop) b.getValue().val, this);
                    db = db._Add(sd, length);
                    break;
                }
                case Types.SIndex: {
                    var si = new SIndex((SIndex) b.getValue().val, this);
                    db = db._Add(si, length);
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

    void Flush() throws Exception {
        PutBuf(wbuf);
    }
}
