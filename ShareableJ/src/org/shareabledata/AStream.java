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
    Buffer rbuf, wbuf;
    SDict<Long, Long> uids = null; // used for movement of SDbObjects

    public AStream(String fn) throws FileNotFoundException, IOException {
        file = new RandomAccessFile(fn, "rws");
        filename = fn;
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
        return (SDbObject[]) r.toArray();
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
        if (uids.Contains(pos)) {
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
                    db = db.Add(nt, length);
                    break;
                }
                case Types.SColumn: {
                    var sc = (SColumn) bs.val;
                    var st = (STable) Lookup(db, Fix(sc.table));
                    db = db.Add(new SColumn(sc, this), length);
                    break;
                }
                case Types.SRecord: {
                    var sr = (SRecord) bs.val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    db = db.Add(new SRecord(db, sr, this), length);
                    break;
                }
                case Types.SDelete: {
                    var sd = (SDelete) bs.val;
                    var st = (STable) Lookup(db, Fix(sd.table));
                    db = db.Add(new SDelete(sd, this), length);
                    break;
                }
                case Types.SUpdate: {
                    var sr = (SUpdate) b.getValue().val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SUpdate(db, sr, this);
                    db = db.Add(nr, length);
                    break;
                }
                case Types.SAlter: {
                    var sa = new SAlter((SAlter) b.getValue().val, this);
                    db = db.Add(sa, length);
                    break;
                }
                case Types.SDrop: {
                    var sd = new SDrop((SDrop) b.getValue().val, this);
                    db = db.Add(sd, length);
                    break;
                }
                case Types.SIndex: {
                    var si = new SIndex((SIndex) b.getValue().val, this);
                    db = db.Add(si, length);
                    break;
                }
            }
        }
        Flush();
        SDatabase.Install(db);
        return db;
    }

    public int ReadByte() throws IOException {
        return rbuf.GetByte();
    }

    public void WriteByte(byte value) throws IOException {
        wbuf.PutByte(value);
    }

    public void Close() throws IOException {
        file.close();
    }

    public void PutInt(int n) throws IOException {
        for (int j = 24; j >= 0; j -= 8) {
            WriteByte((byte) (n >> j));
        }
    }

    public void PutLong(long t) throws IOException {
        for (int j = 56; j >= 0; j -= 8) {
            WriteByte((byte) (t >> j));
        }
    }

    public void PutString(String s) throws IOException {
        byte[] cs = s.getBytes("UTF-8");
        PutInt(cs.length);
        for (int i = 0; i < cs.length; i++) {
            WriteByte(cs[i]);
        }
    }

    public int GetInt() throws IOException {
        int v = 0;
        for (int j = 0; j < 4; j++) {
            v = (v << 8) + ReadByte();
        }
        return v;
    }

    public long GetLong() throws IOException {
        long v = 0;
        for (int j = 0; j < 8; j++) {
            v = (v << 8) + ReadByte();
        }
        return v;
    }

    public String GetString() throws IOException {
        int n = GetInt();
        byte[] cs = new byte[n];
        for (int j = 0; j < n; j++) {
            cs[j] = (byte) ReadByte();
        }
        return new String(cs, 0, n, "UTF-8");
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
