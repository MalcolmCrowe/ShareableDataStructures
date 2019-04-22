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
    SDict<Long,Serialisable> commits = null;
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
        if (commits!=null && commits.Contains(pos))
            return commits.Lookup(pos);
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
        commits = null;
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
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(nt.uid,nt);
                    else
                        commits = commits.Add(nt.uid, nt);
                    break;
                }
                case Types.SColumn: {
                    var sc = (SColumn) bs.val;
                    var nm = tr.Name(sc.uid);
                    var nc = new SColumn(sc, nm, this);
                    var tb = (STable)db.objects.get(nc.table);
                    tb = tb.Add(nc,nm);
                    db = db._Add(nc, nm, length())
                            ._Add(tb,db.Name(tb.uid),length())
                            .Add(tb.uid,nc.uid,nm);
                    if (commits==null)
                        commits = new SDict(nc.uid,nc);
                    else
                        commits = commits.Add(nc.uid, nc);
                    break;
                }
                case Types.SRecord: {
                    var sr = (SRecord) bs.val;
                    var st = (STable) Lookup(db, Fix(sr.table));
                    var nr = new SRecord(db, sr, this);
                    db = db._Add(nr, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(nr.uid,nr);
                    else
                        commits = commits.Add(nr.uid, nr);
                    break;
                }
                case Types.SDelete: {
                    var sd = (SDelete) bs.val;
                    var sr = cache.get(sd.delpos);
                    var st = (STable) Lookup(db, Fix(sd.table));
                    var nd = new SDelete(sd, this);
                    db = db._Add(nd, sr, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(nd.uid,nd);
                    else
                        commits = commits.Add(nd.uid, nd);
                    break;
                }
                case Types.SUpdate: {
                    var su = (SUpdate) b.getValue().val;
                    var sr = cache.get(su.Defpos());
                    var st = (STable) Lookup(db, Fix(su.table));
                    var nr = new SUpdate(db, su, this);
                    db = db._Add(nr,sr, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(nr.uid,nr);
                    else
                        commits = commits.Add(nr.uid, nr);
                    break;
                }
                case Types.SAlter: {
                    var sa = new SAlter((SAlter) b.getValue().val, this);
                    db = db._Add(sa, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(sa.uid,sa);
                    else
                        commits = commits.Add(sa.uid, sa);
                    break;
                }
                case Types.SDrop: {
                    var sd = new SDrop((SDrop) b.getValue().val, this);
                    db = db._Add(sd, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(sd.uid,sd);
                    else
                        commits = commits.Add(sd.uid, sd);
                    break;
                }
                case Types.SIndex: {
                    var si = new SIndex((SIndex) b.getValue().val, this);
                    db = db._Add(si, length());
                    if (commits==null)
                        commits = new SDict<Long,Serialisable>(si.uid,si);
                    else
                        commits = commits.Add(si.uid, si);
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
        commits = null;
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
