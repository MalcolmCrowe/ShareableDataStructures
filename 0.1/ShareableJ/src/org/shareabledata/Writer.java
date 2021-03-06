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
public class Writer extends WriterBase {

    public RandomAccessFile file; // shared with Reader(s)
    SDict<Long, Long> uids = null; // used for movement of SDbObjects
    public Writer(RandomAccessFile f)
    {
        file = f;
    }
    protected long length() throws Exception {
        return file.length() + buf.pos;
    }
    Serialisable Lookup(SDatabase db, long pos)
    {
        pos = Fix(pos);
        if (pos>=STransaction._uid)
            return db.objects.Lookup(pos);
        try {
            return new Reader(db,pos)._Get();
        } catch(Exception e)
        {
            throw new Error("invalid log at "+pos);
        }
    }

    long Fix(long pos) 
    {
        if (uids != null && uids.Contains(pos)) {
            pos = uids.Lookup(pos);
        }
        return pos;
    }
  
    public SDatabase Commit(SDatabase db, STransaction tr) throws Exception {
        uids = new SDict<Long, Long>(-1L, -1L);
        // We need two passes: manage a cache of SRecords being deleted or updated
        // before we start writing
        SDict<Long,SRecord> cache = null; 
        for (var b=tr.objects.PositionAt(STransaction._uid);b!=null;b=b.Next())
            switch(b.getValue().val.type) 
            {
                case Types.SUpdate:
                {
                    var su = (SUpdate)b.getValue().val;
                    var u = su.Defpos();
                    var sr = db.Get(u);
                    cache = (cache==null)?new SDict(u,sr):cache.Add(u, sr);
                    break;
                }
                case Types.SDelete:
                {
                    var sd = (SDelete)b.getValue().val;
                    var u = sd.delpos;
                    var sr = db.Get(u);
                    cache = (cache==null)?new SDict(u,sr):cache.Add(u, sr);
                    break;
                }
            }
        if (tr.objects!=null)
        for (var b = tr.objects.PositionAt(STransaction._uid); b != null; b = b.Next()) {
            var bs = b.getValue();
            switch (bs.val.type) {
                case Types.STable: {
                    var st = (STable) b.getValue().val;
                    var nm = tr.Name(st.uid);
                    var nt = new STable(st, nm, this);
                    db = db._Add(nt, nm, length());
                    break;
                }
                case Types.SColumn: {
                    var sc = (SColumn) bs.val;
                    var nm = tr.Name(sc.uid);
                    var nc = new SColumn(sc, nm, this);
                    var tb = (STable)db.objects.get(nc.table);
                    tb = tb.Add(-1,nc,nm);
                    db = db._Add(nc, nm, length())
                            ._Add(tb,db.Name(tb.uid),length())
                            .Add(tb.uid,-1,nc.uid,nm);
                    break;
                }
                case Types.SRecord: {
                    var sr = (SRecord) bs.val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SRecord(db, sr, this);
                    db = db._Add(nr, length());
                    break;
                }
                case Types.SDelete: {
                    var sd = (SDelete) bs.val;
                    var st = (STable) Lookup(db, Fix(sd.table));
                    var nd = new SDelete(sd, this);
                    db = db._Add(nd, length());
                    break;
                }
                case Types.SUpdate: {
                    var su = (SUpdate) b.getValue().val;
                    var st = (STable) Lookup(db, Fix(su.table));
                    var nr = new SUpdate(db, su, this);
                    db = db._Add(nr,length());
                    break;
                }
                case Types.SAlter: {
                    var sa = new SAlter((SAlter) b.getValue().val, this);
                    db = db._Add(sa, length());
                    break;
                }
                case Types.SDrop: {
                    var sd = new SDrop((SDrop) b.getValue().val, this);
                    db = db._Add(sd, length());
                    break;
                }
                case Types.SIndex: {
                    var si = new SIndex(db,(SIndex) b.getValue().val, this);
                    db = db._Add(si, length());
                    break;
                }
                case Types.SDropIndex:
                {
                    var di = new SDropIndex(db, (SDropIndex)b.getValue().val, this);
                    db = db._Add(di, length());
                    break;
                }
            }
        }
        Flush();
        SDatabase.Install(db);
        return db;
    }
    void CommitDone()
    {
        uids = null;
    }
    public void Close() throws IOException {
        file.close();
    }
    @Override
    protected void PutBuf() throws Exception {
        synchronized(file) {
            var p = file.length();
            file.seek(p);
            file.write(buf.buf, 0, buf.pos);
            buf.pos = 0;
        } 
    }
    @Override
    public void WriteByte(byte value) throws Exception
    {
        if (buf.pos>=Buffer.Size)
        {
            PutBuf();
            buf.pos = 0;
        }
        buf.buf[buf.pos++] = value;
    }
    public void Flush() throws Exception {
        PutBuf();
    }
}
