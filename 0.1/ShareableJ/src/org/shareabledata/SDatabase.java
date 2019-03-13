/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.*;

/**
 *
 * @author Malcolm
 */
public class SDatabase {

    public final String name;
    public final SDict<Long, SDbObject> objects;
    public final SDict<String, SDbObject> names;
    public final long curpos;
    static Object files = new Object(); // a lock 
    protected static SDict<String, AStream> dbfiles = null;
    protected static SDict<String, SDatabase> databases = null;

    SDatabase getRollback() {
        return this;
    }

    protected boolean getCommitted() {
        return true;
    }

    public static SDatabase Open(String path, String fname) throws Exception {
        if (dbfiles != null && dbfiles.Contains(fname)) {
            var r = databases.Lookup(fname);
            if (r == null) {
                throw new Exception("Database is loading");
            }
            return r;
        }
        var db = new SDatabase(fname);
        var fs = new AStream(path, fname);
        if (dbfiles == null) {
            dbfiles = new SDict<>(fname, fs);
        } else {
            dbfiles = dbfiles.Add(fname, fs);
        }
        db = db.Load();
        Install(db);
        return db;
    }

    synchronized public static void Install(SDatabase db) {
        if (databases == null) {
            databases = new SDict<>(db.name, db);
        } else {
            databases = databases.Add(db.name, db);
        }
    }

    public SRecord Get(long pos) {
        var s = _Get(pos);
        SRecord rc = null;
        if (s != null && s.type == Types.SRecord || s.type == Types.SUpdate) {
            rc = (SRecord) s;
        }
        if (rc == null) {
            throw new Error("Record " + pos + " never defined");
        }
        var tb = (STable)objects.Lookup(rc.table);
        if (tb == null) {
            throw new Error("Table " + rc.table + " has been dropped");
        }
        if (tb.rows == null || !tb.rows.Contains(rc.Defpos())) {
            throw new Error("Record " + pos + " has been dropped");
        }
        return (SRecord) _Get(tb.rows.Lookup(rc.Defpos()));
    }

    public Serialisable _Get(long pos) {
        try {
            return dbfiles.Lookup(name).Lookup(this,pos);
        } catch(Exception e)
        {
            throw new Error("bad log at "+pos);
        }
    }

    SDatabase(String fname) {
        name = fname;
        objects = null;
        names = null;
        curpos = 0;
    }

    protected SDatabase(SDatabase db) {
        name = db.name;
        objects = db.objects;
        names = db.names;
        curpos = db.curpos;
    }

    // CRUD on Records changes indexes as well as table, so we need this
    protected SDatabase(SDatabase db, SDict<Long, SDbObject> obs, 
            SDict<String,SDbObject> nms,long c) {
        name = db.name;
        objects = obs;
        names = nms;
        curpos = c;
    }
    protected SDatabase(SDatabase db,long pos)
    {
        name = db.name;
        objects = db.objects;
        names = db.names;
        curpos = pos;
    }
    SDatabase New(SDict<Long, SDbObject> obs, 
            SDict<String,SDbObject> nms,long c)
    {
        return new SDatabase(this,obs,nms,c);
    }

    protected SDatabase Install(STable t, long c) {
        if (names == null) 
            return New(new SDict<>(t.uid, t),
            new SDict<>(t.name, t),c);
        else
            return New(objects.Add(t.uid, t),
            names.Add(t.name, t),c);
    }
    public SDatabase Install(SColumn c, long p)
    {
        var obs = objects;
        if (c.uid >= STransaction._uid)
            obs = obs.Add(c.uid, c);
        var tb = ((STable)obs.Lookup(c.table)).Add(c);
        return New(obs.Add(c.table,tb), names.Add(tb.name,tb), p);
    }

    protected SDatabase Install(SAlter a, long c) {
        var obs = objects;
        if (a.uid>STransaction._uid)
            obs = obs.Add(a.uid, a);
        if (a.parent == 0) {
            var ot = (STable)obs.Lookup(a.defpos);
            var nt = new STable(ot, a.name);
            return New(obs.Add(a.defpos, nt),
                    names.Remove(ot.name).Add(a.name, nt),c);
        } else {
            var ot = (STable) obs.Lookup(a.parent);
            var oc = (ot.cols == null) ? null : ot.cols.Lookup(a.defpos);
            var nc = new SColumn((SColumn)oc, a.name, a.dataType);
            var nt = ot.Add(nc);
            return New(obs.Add(a.defpos, nt),names.Add(a.name, nt),c);
        }
    }

    protected SDatabase Install(SDrop d, long c) {
        if (d.parent == 0) {
            var ot = (STable)objects.Lookup(d.drpos);
            return New(objects.Remove(d.drpos),names.Remove(ot.name),c);
        } else {
            var ot = (STable)objects.Lookup(d.parent);
            STable nt = ot.Remove(d.drpos);
            return New(objects.Add(d.parent, nt),names,c);
        }
    }

    protected SDatabase Install(SView v, long c) {
        return New(objects.Add(v.uid, v),names.Add(v.name, v),c);
    }

    protected SDatabase Install(SIndex x, long c) throws Exception
    {
        var tb = (STable)objects.Lookup(x.table);
        if (tb.rows != null) {
            for (var b = tb.rows.First(); b != null; b = b.Next()) 
            try {
                x = x.Add(Get(b.getValue().val), b.getValue().val);
            } catch(Exception e){}
        }
        tb = new STable((tb.indexes==null)?new SDict(x.uid,true):tb.indexes.Add(x.uid,true),tb);
        return New(objects.Add(x.uid, x).Add(tb.uid,tb),names.Add(tb.name, tb),c);
    }

    public AStream File() {
        return dbfiles.Lookup(name);
    }

    SDatabase Load() throws Exception {
        var rd = new Reader(dbfiles.Lookup(name), 0);
        var db = this;
        for (var s = rd._Get(this); s != null; s = rd._Get(db)) {
            db = db._Add((SDbObject)s, ((SDbObject)s).uid);
        }
        return new SDatabase(db,rd.getPosition());
    }

    public SDatabase _Add(SDbObject s, long p) throws Exception {
        switch (s.type) {
            case Types.STable:
                return Install((STable) s, p);
            case Types.SColumn:
                return Install((SColumn) s, p);
            case Types.SUpdate:
                return Install((SUpdate) s, p);
            case Types.SRecord:
                return Install((SRecord) s, p);
            case Types.SDelete:
                return Install((SDelete) s, p);
            case Types.SAlter:
                return Install((SAlter) s, p);
            case Types.SDrop:
                return Install((SDrop) s, p);
            //        case Types.SView: return Install((SView)s, p);
            case Types.SIndex:
                return Install((SIndex) s, p);
        }
        return this;
    }
    /// <summary>
    /// Only for testing environments!
    /// </summary>

    public void Close() throws IOException {
        synchronized (files) {
            AStream f = dbfiles.Lookup(name);
            databases = databases.Remove(name);
            dbfiles = dbfiles.Remove(name);
            f.Close();
        }
    }

    protected SDatabase Install(SRecord r, long p) throws Exception {
        var obs = objects;

        if (r.uid>=STransaction._uid)
            obs = obs.Add(r.uid,r);
        var st = ((STable)obs.Lookup(r.table)).Add(r);
        obs = obs.Add(r.table, st);
        var nms = names.Add(st.name,st);
        if (st.indexes!=null)
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
                var x = (SIndex)obs.Lookup(b.getValue().key);
                if (x.table == r.table) {
                    var k = x.uid;
                    var v = x.Add(r, r.uid);
                    obs = obs.Add(k, v);
                }
                if (x.references == r.table && !x.Contains(r))
                    throw new Exception("Referential constraint");
            }
        return New(obs, nms, p);
    }

    protected SDatabase Install(SUpdate u, long c) throws Exception {
        var obs = objects;
        if (u.uid >= STransaction._uid)
            obs = obs.Add(u.uid, u);
        var st = ((STable)obs.Lookup(u.table)).Add(u);
        SRecord sr = null;
        obs = obs.Add(u.table, st);
        var nms = names.Add(st.name, st);
        if (st.indexes!=null)
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
            var x = (SIndex) obs.Lookup(b.getValue().key);
            if (sr == null)
                sr = Get(u.defpos);
            obs = obs.Add(x.uid, x.Update(sr, u, c));
            if (x.references == u.table && !x.Contains(u))
                throw new Exception("Referential constraint");
        }
        return New(obs, nms, c);
    }

    protected SDatabase Install(SDelete d, long p) throws Exception {
        var obs = objects;
        if (d.uid >= STransaction._uid)
            obs = obs.Add(d.uid, d);
        var st = ((STable)obs.Lookup(d.table));
        SRecord sr = null;
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
            var x = (SIndex) obs.Lookup(b.getValue().key);
            if (sr == null)
                sr = Get(d.delpos);
            obs = obs.Add(x.uid, x.Remove(sr, p));
            if (x.references == d.table && x.Contains(sr))
                throw new Exception("Referential constraint");
        }
        var nms = names;
        if (sr!=null)
        {
            st = st.Remove(sr.Defpos());
            obs = obs.Add(d.table, st);
            nms = names.Add(st.name,st);
        }
        return New(obs, nms, p);
    }

    public STransaction Transact(boolean auto) {
        return new STransaction(this, auto);
    }

    public SDatabase MaybeAutoCommit(STransaction tr) throws Exception {
        return tr.autoCommit ? tr.Commit() : tr;
    }

    public SDatabase Rollback() {
        return this;
    }

    STable GetTable(String tn) {
        var ob = (names==null)?null:names.Lookup(tn);
        return (ob != null && ob.type == Types.STable)
                ? (STable) ob : null;
    }

    SIndex GetPrimaryIndex(long t) throws Exception {
        for (var b = objects.First(); b != null; b = b.Next()) {
            if (b.getValue().val.type == Types.SIndex) {
                var x = (SIndex) b.getValue().val;
                if (x.table == t) {
                    return x;
                }
            }
        }
        return null;
    }
}
