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
    public final long curpos;
    public final SRole role;
    static final Object files = new Object(); // a lock 
    protected static SDict<String, RandomAccessFile> dbfiles = null;
    protected static SDict<String, SDatabase> databases = null;
    public static final SDatabase _system = System();

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
        var file = new RandomAccessFile(new File(path,fname),"rws");
        if (dbfiles == null) {
            dbfiles = new SDict<>(fname, file);
        } else {
            dbfiles = dbfiles.Add(fname, file);
        }
        db = db.Load();
        Install(db);
        return db;
    }
    protected static SDatabase System()
    {
        return SysTable.SysTables(new SDatabase());
    }

    synchronized public static void Install(SDatabase db) {
        if (databases == null) {
            databases = new SDict<>(db.name, db);
        } else {
            databases = databases.Add(db.name, db);
        }
    }

    public SRecord Get(Long pos) {
        return (SRecord)_Get(pos);
    }

    public Serialisable _Get(long pos) {
        try {
            return new Reader(this,pos)._Get();
        } catch(Exception e)
        {
            throw new Error("bad log at "+pos);
        }
    }
    public String Name(long uid) throws Exception
    {
        if (uid == -1)
            return "PUBLIC";
        if (!role.defines(uid))
            throw new Exception("Bad long " + SDbObject._Uid(uid));
        return role.uids.get(uid);
    }
    SDatabase(){
        name = "SYSTEM";
        objects = null;
        role = SRole.Public;
        curpos = 0;
    }
    SDatabase(String fname) {
        name = fname;
        objects = _system.objects;
        role = _system.role;
        curpos = 0;
    }

    protected SDatabase(SDatabase db) {
        name = db.name;
        objects = db.objects;
        role = db.role;
        curpos = db.curpos;
    }

    // CRUD on Records changes indexes as well as table, so we need this
    protected SDatabase(SDatabase db, SDict<Long, SDbObject> obs, 
            SRole r,long c) {
        name = db.name;
        objects = obs;
        role = r;
        curpos = c;
    }
    protected SDatabase(SDatabase db,long pos)
    {
        name = db.name;
        objects = db.objects;
        role = db.role;
        curpos = pos;
    }
    SDatabase New(SDict<Long, SDbObject> obs, 
            SRole r,long c)
    {
        return new SDatabase(this,obs,r,c);
    }
    public SDatabase _Add(SDbObject s, String nm, long p) throws Exception
    {
        switch (s.type)
        {
            case Types.STable: return Install((STable)s, nm, p);
            case Types.SColumn: return Install((SColumn)s, nm, p);
//             case Types.SAlter: return Install((SAlter)s, nm, p);
        }
        return this;
    }
    public SDatabase Add(long u,String n)
    {
        return New(objects, role.Add(u,n),curpos);
    }
    public SDatabase Add(long t, long c, String n)
    {
        return New(objects, role.Add(t,c,n), curpos);
    }
    public SDatabase Add(SDbObject ob,long u) throws Exception
    {
        return _Add(ob, u);
    }
    public SDatabase Add(SDbObject ob,String nm,long u) throws Exception
    {
        return _Add(ob, nm, u);
    }

    protected SDatabase Install(STable t, String n,long c) {
        return New((objects==null)?new SDict(t.uid,t):objects.Add(t.uid, t),
                new SRole(role,n,t.uid),c);
    }
    public SDatabase Install(SColumn c, String n, long p) throws Exception
    {
        var obs = objects;
        if (c.uid >= STransaction._uid)
            obs = obs.Add(c.uid, c);
        var tb = ((STable)obs.get(c.table)).Add(c,n);
        if (role.defs!=null && role.defs.Contains(c.table) && 
                role.defs.get(c.table).Contains(n))
            throw new Exception("Table "+role.uids.get(tb.uid)+ 
                    " already has column "+n);
        return New(obs.Add(c.table,tb).Add(c.uid, c), 
                role.Add(c.table,c.uid,n).Add(c.uid,n), p);
    }

    protected SDatabase Install(SAlter a, long c) throws Exception
    {
        var obs = objects;
        if (a.uid>STransaction._uid)
            obs = obs.Add(a.uid, a);
        if (a.col == -1) {
            var ot = (STable)obs.Lookup(a.defpos);
            return New(obs,
                    role.Remove(Name(ot.uid)).Add(Name(a.uid), ot.uid),c);
        } else {
            var ot = (STable) obs.Lookup(a.defpos);
            var nc = new SColumn(a.col, ot.uid, a.dataType);
            var nt = ot.Add(nc,Name(nc.uid));
            return New(obs.Add(a.defpos, nt),role,c);
        }
    }

    protected SDatabase Install(SDrop d, long c) throws Exception
    {
        var obs = objects;
        if (d.uid>=STransaction._uid)
            obs = obs.Add(d.uid, d);
        if (d.parent == -1) {
            var ro = role;
            var ot = objects.Lookup(d.drpos);
            switch(ot.type)
            {
                case Types.STable:
                    ro = ro.Remove(Name(((STable)ot).uid));
                    break;
                case Types.SIndex:
                {
                    var x = (SIndex)ot;
                    var tb = (STable)objects.get(x.table);
                    tb = new STable(tb.indexes.Remove(x.uid),tb);
                    obs = obs.Add(tb.uid,tb);
                    break;
                }
            }
            return New(objects.Remove(d.drpos),ro,c);
        } else {
            var ot = (STable)objects.Lookup(d.parent);
            var nt = ot.Remove(d.drpos);
            return New(objects.Add(d.parent, nt),role,c);
        }
    }

    protected SDatabase Install(SView v, String n, long c) {
        return New(objects.Add(v.uid, v),role.Add(n, v.uid),c);
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
        return New(objects.Add(x.uid, x).Add(tb.uid,tb),role,c);
    }

    public RandomAccessFile File() {
        return dbfiles.Lookup(name);
    }

    SDatabase Load() throws Exception {
        var rd = new Reader(this,curpos);
        var db = this;
        for (var s = (SDbObject)rd._Get(); s != null && s!=Serialisable.Null; s = (SDbObject)rd._Get())
            rd.db = rd.db._Add(s, rd.Position());
        return new SDatabase(rd.db,rd.Position());
    }
    public SDatabase _Add(SDbObject s, long p) throws Exception {
        switch (s.type) {
            case Types.SRecord:
                return Install((SRecord) s, p);
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
    public SDatabase _Add(SDbObject s,SRecord r,long p) throws Exception
    {
       switch (s.type) {
            case Types.SUpdate:
                return Install((SUpdate) s, r, p);
            case Types.SDelete:
                return Install((SDelete) s, r, p);
        }
        return this;        
    }
    /// <summary>
    /// Only for testing environments!
    /// </summary>

    public void Close() throws IOException {
        synchronized (files) {
            var f = dbfiles.Lookup(name);
            databases = databases.Remove(name);
            dbfiles = dbfiles.Remove(name);
        }
    }

    protected SDatabase Install(SRecord r, long p) throws Exception {
        var obs = objects;
        var ro = role;
        if (r.uid>=STransaction._uid)
            obs = obs.Add(r.uid,r);
        var st = ((STable)obs.Lookup(r.table)).Add(r);
        obs = obs.Add(r.table, st);
        if (st.indexes!=null)
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
                var x = (SIndex)obs.Lookup(b.getValue().key);
                x.Check(this,r,false);
                obs = obs.Add(x.uid,x.Add(r,r.uid));
            }
        return New(obs, ro, p);
    }

    public SDatabase Install(SUpdate u, SRecord sr, long c) throws Exception {
        var obs = objects;
        var ro = role;
        if (u.uid >= STransaction._uid)
            obs = obs.Add(u.uid, u);
        var st = ((STable)obs.Lookup(u.table)).Add(u);
        obs = obs.Add(u.table, st);
        if (st.indexes!=null)
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
            var x = (SIndex) obs.Lookup(b.getValue().key);
            var ok = x.Key(sr,x.cols);
            var uk = x.Key(u, x.cols);
            x.Check(this,u,ok.compareTo(uk)==0);
            obs = obs.Add(x.uid, x.Update(sr, ok,u,uk, c));
            if (x.references == u.table && !x.Contains(u))
                throw new Exception("Referential constraint");
        }
        return New(obs, ro, c);
    }

    public SDatabase Install(SDelete d, SRecord sr, long p) throws Exception {
        var obs = objects;
        if (d.uid >= STransaction._uid)
            obs = obs.Add(d.uid, d);
        var st = ((STable)obs.Lookup(d.table));
        if (st.indexes!=null)
        for (var b = st.indexes.First(); b != null; b = b.Next()) {
            var x = (SIndex) obs.Lookup(b.getValue().key);
            obs = obs.Add(x.uid, x.Remove(sr, p));
            if (!x.primary)
                continue;
            var k = x.Key(sr,x.cols);
            for (var ob = obs.PositionAt(0L); ob != null; ob = ob.Next()) // don't bother with system tables
                if (ob.getValue().val instanceof STable)
                {
                    var ot = (STable)ob.getValue().val;
                    for (var ox = ot.indexes.First(); ox != null; ox = ox.Next())
                    {
                        var nx = (SIndex)obs.get(ox.getValue().key);
                        if (nx.references == d.table && nx.rows.Contains(k))
                            throw new Exception("Referential constraint: illegal delete");
                    }
                }
        }
        var ro = role;
        if (sr!=null)
        {
            st = st.Remove(sr.Defpos());
            obs = obs.Add(d.table, st);
        }
        return New(obs, ro, p);
    }

    public STransaction Transact(ReaderBase rdr,boolean auto) {
        return new STransaction(databases.get(name), rdr, auto);
    }

    public SDatabase MaybeAutoCommit(STransaction tr) throws Exception {
        return tr.autoCommit ? tr.Commit() : tr;
    }

    public SDatabase Rollback() {
        return this;
    }

    STable GetTable(String tn) {
        return role.globalNames.Contains(tn)?
                (STable)objects.get(role.globalNames.get(tn)):null;
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
