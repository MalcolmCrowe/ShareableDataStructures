using System;
using System.IO;
namespace Shareable
{
    public class SDatabase
    {
        public readonly string name;
        /// <summary>
        /// The base SDatabase class db.objects contains only committed non-SRecord SDbObjects
        /// (i.e. uids in range 0 to STransaction._uid)
        /// In the STransaction subclass, objects also contains uncommitted SDbObjects of any kind 
        /// (i.e. uids outside this range).
        /// This means that STransaction can lookup any kind of object using objects[]
        /// whereas SDatabase needs to fetch SRecords from the transaction log.
        /// SRecords are not looked up or Installed in the same way as other objects: see _Get and _Add.
        /// </summary>
        public readonly SDict<long, SDbObject> objects;
        public readonly long curpos;
        public readonly SRole role;
        protected static object _lock = new object(); // a lock
        protected static SDict<string, FileStream> dbfiles = SDict<string, FileStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string, SDatabase>.Empty;
        public static readonly SDatabase _system = System();
        public static int commits = 0, rconflicts = 0, wconflicts=0;
        protected virtual bool Committed => true;
        public static SDatabase Open(string path, string fname)
        {
            if (dbfiles.Contains(fname))
                return databases[fname]
                    ?? throw new System.Exception("Database is loading");
            var db = new SDatabase(fname);
            var file = new FileStream(path+fname, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            dbfiles += (fname, file);
            db = db.Load();
            Install(db);
            return db;
        }
        protected static SDatabase System()
        {
           return SysTable.SysTables(new SDatabase());
        }
        public SDict<long, string> uids => role.uids;  
        public static void Install(SDatabase db)
        {
            lock (_lock)
                databases += (db.name, db);
        }
        public FileStream File()
        {
            return dbfiles[name];
        }
        SDatabase()
        {
            name = "SYSTEM";
            objects = SDict<long, SDbObject>.Empty;
            role = SRole.Public;
            curpos = 0;
        }
        SDatabase(string fname)
        {
            name = fname;
            objects = _system.objects;
            role = _system.role;
            curpos = 0;
        }
        protected SDatabase(SDatabase db)
        {
            name = db.name;
            objects = db.objects;
            curpos = db.curpos;
            role = db.role;
        }
        /// <summary>
        /// CRUD on Records changes indexes as well as table, so we need this
        /// </summary>
        /// <param name="db"></param>
        /// <param name="obs"></param>
        /// <param name="c"></param>
        protected SDatabase(SDatabase db, SDict<long, SDbObject> obs, SRole r, long c)
        {
            name = db.name;
            objects = obs;
            curpos = c;
            role = r;
        }
        protected SDatabase(SDatabase db,long pos) :this(db)
        {
            curpos = pos;
        }
        internal SDatabase Load()
        {
            var rd = new Reader(this);
            long last = 0;
            try
            {
                for (var s = rd._Get() as SDbObject; s != null && s!=Serialisable.Null; s = rd._Get() as SDbObject)
                {
                    last = s.uid;
                    rd.db += (s, rd.Position);
                }
            } catch(Exception e)
            {
                Console.WriteLine("Database corrupt after " + last);
                throw e;
            }
            return new SDatabase(rd.db,rd.Position);
        }
        protected virtual Serialisable _Get(long pos)
        {
            return new Reader(this, pos)._Get();
        }
        public SRecord Get(long pos)
        {
            return (SRecord)_Get(pos);
        }
        protected SDatabase _Add(SDbObject s, long p)
        {
            switch (s.type)
            {
                case Types.SRecord: return Install((SRecord)s, p);
                case Types.SAlter: return Install((SAlter)s, p);
                case Types.SDrop: return Install((SDrop)s, p);
                case Types.SIndex: return Install((SIndex)s, p);
                case Types.SDropIndex: return Install((SDropIndex)s,p);
                case Types.SUpdate: return Install((SUpdate)s, p);
                case Types.SDelete: return Install((SDelete)s, p);
            }
            return this;
        }
        protected SDatabase _Add(SDbObject s, string nm, long p)
        {
            switch (s.type)
            {
                case Types.STable: return Install((STable)s, nm, p);
                case Types.SColumn: return Install((SColumn)s, nm, p);
   //             case Types.SAlter: return Install((SAlter)s, nm, p);
            }
            return this;
        }
        public static SDatabase operator+(SDatabase d,(long,string)n)
        {
            return d.New(d.objects, d.role + n,d.curpos);
        }
        public static SDatabase operator +(SDatabase d, (long, int,long, string) n)
        {
            return d.New(d.objects, d.role + n, d.curpos);
        }
        public static SDatabase operator+(SDatabase d,(SDbObject,long) x)
        {
            switch (x.Item1.type)
            {
                case Types.SDelete:
                    {
                        var del = (SDelete)x.Item1;
                        return d._Add(del, x.Item2);
                    }
                case Types.SUpdate:
                    {
                        var upd = (SUpdate)x.Item1;
                        return d._Add(upd,x.Item2);
                    }
            }
            return d._Add(x.Item1, x.Item2);
        }
        public static SDatabase operator+(SDatabase d,(SDbObject,string,long)x)
        {
            return d._Add(x.Item1, x.Item2, x.Item3);
        }
        /// <summary>
        /// Close() is only for testing environments!
        /// </summary>
        public void Close()
        {
            lock (_lock)
            {
                var f = dbfiles[name];
                databases = databases-name;
                dbfiles = dbfiles-name;
                f.Close();
            }
        }
        public string Name(long uid)
        {
            if (uid == -1)
                return "PUBLIC";
            if (!role.defines(uid))
            {
                if (uid < SDbObject.maxAlias)
                    return "$" + (SDbObject.maxAlias - uid);
                throw new StrongException("Bad long " + SDbObject._Uid(uid));
            }
            return role.uids[uid];
        }
        protected virtual SDatabase New(SDict<long,SDbObject> o,SRole r, long c)
        {
            return new SDatabase(this, o, r, c);
        }
        /// <summary>
        /// Tables are only Installed once and the name
        /// is entered in the Role's global namespace.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="n"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public SDatabase Install(STable t, string n, long c)
        {
            return New(objects + (t.uid, t), role+(n,t.uid), c);
        }
        public SDatabase Install(SColumn c, string n, long p)
        {
            var obs = objects;
            if (c.uid >= STransaction._uid)
                obs += (c.uid, c);
            var tb = ((STable)obs[c.table]);
            if (role.subs.Contains(c.table) && role.subs[c.table].defs.Contains(n))
                throw new StrongException("Table " + uids[tb.uid] + " already has column " + n);
            if (tb.rows.Length!=0)
                for (var b=c.constraints.First();b!=null;b=b.Next())
                    switch (b.Value.Item1)
                    {
                        case "NOTNULL": throw new StrongException("Table is not empty");
                    }
            return New(obs + (c.table, tb+(-1,c,n))+(c.uid,c), role+(c.table,-1,c.uid,n)+(c.uid,n), p);
        }
        public SDatabase Install(SRecord r, long c)
        {
            var obs = objects;
            var ro = role;
            var st = ((STable)obs[r.table])+r;
            if (r.uid >= STransaction._uid)
                obs += (r.uid, r);
            obs += (r.table, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)objects[b.Value.Item1];
                x.Check(this,r,false);
                obs += (x.uid, x + (r, r.uid));
            }
            return New(obs, ro, c);
        }
        public SDatabase Install(SUpdate u, long c)
        {
            var obs = objects;
            var ro = role;
            var st = ((STable)obs[u.table])+u;
            if (u.uid >= STransaction._uid)
                obs += (u.uid, u);
            obs += (u.table, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)obs[b.Value.Item1];
                var ok = x.Key(u.oldrec, x.cols);
                var uk = x.Key(u, x.cols);
                x.Check(this, u, ok.CompareTo(uk)==0);
                obs += (x.uid, x.Update(u.oldrec,ok,u,uk,c));
            }
            return New(obs, ro, c);
        }
        public SDatabase Install(SDelete d, long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs += (d.uid, d);
            var st = (STable)obs[d.table];
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var px = (SIndex)obs[b.Value.Item1];
                obs += (px.uid, px - (d.oldrec, c));
                if (!px.primary)
                    continue;
                var k = px.Key(d.oldrec, px.cols);
                for (var ob = obs.PositionAt(0); ob != null; ob = ob.Next()) // don't bother with system tables
                    if (ob.Value.Item2 is STable ot)
                        for (var ox = ot.indexes.First(); ox != null; ox = ox.Next())
                        {
                            var x = (SIndex)obs[ox.Value.Item1];
                            if (x.references == d.table && x.rows.Contains(k))
                                throw new StrongException("Referential constraint: illegal delete");
                        }
            }
            var ro = role;
            if (d.oldrec != null)
            {
                st = st.Remove(d.oldrec.Defpos);
                obs += (d.table, st);
            }
            return New(obs, ro, c);
        }
        public SDatabase Install(SAlter a, long c)
        {
            var obs = objects;
            if (a.uid >= STransaction._uid)
                obs += (a.uid, a);
            if (a.col == -1)
            {
                var ot = (STable)obs[a.defpos];
                return New(obs, role - Name(ot.uid) + (Name(a.uid), ot.uid), c);
            }
            else
            {
                var ot = (STable)objects[a.defpos];
                var nc = new SColumn(ot.uid, a.dataType, a.col);
                var nm = Name(nc.uid);
                var nt = ot + (a.seq,nc,nm);
                return New(obs + (a.defpos, nt),role+(a.defpos,a.seq,a.col,nm),c);
            }
        }
        public SDatabase Install(SDrop d, long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs = obs + (d.uid, d);
            if (d.parent == -1)
            {
                var ro = role;
                var ot = objects[d.drpos];
                switch (ot.type)
                {
                    case Types.STable:
                        ro -= Name(((STable)ot).uid);
                        for (var b = ((STable)ot).indexes.First(); b != null; b = b.Next())
                            obs -= b.Value.Item1;
                        break;
                    case Types.SIndex:
                        {
                            var x = (SIndex)ot;
                            var tb = (STable)objects[x.table];
                            tb = new STable(tb, tb.indexes - x.uid);
                            obs = obs + (tb.uid, tb);
                            break;
                        }
                    default:
                        break;
                }
                return New(obs - d.drpos, ro, c);
            }
            else
            {
                var ot = (STable)objects[d.parent];
                var sc = (SColumn)obs[d.drpos];
                var ss = role.subs[sc.table];
                var sq = ss.props[sc.uid];
                if (d.detail.Length == 0)
                {
                    var nt = ot.Remove(d.drpos);
                    return New(obs + (d.parent, nt), role-(d.parent,sq), c);
                } else
                {
                    var nc = new SColumn(sc.table, sc.dataType, sc.uid, sc.constraints - d.detail);
                    var nt = ot + (sq,nc, Name(nc.uid));
                    var ro = role + (Name(nt.uid),nt.uid);
                    return New(obs + (d.drpos,nc)+(nt.uid,nt),ro,c);
                }
            }
        }
        public SDatabase Install(SView v, string n, long c)
        {
            return New(objects + (v.uid, v),role + (n, v.uid),c);
        }
        public SDatabase Install(SIndex x, long c)
        {
            var tb = (STable)objects[x.table];
            for (var b = tb.rows.First(); b != null; b = b.Next())
                x += (Get(b.Value.Item2), b.Value.Item2);
            tb = new STable(tb, tb.indexes + (x.uid, true));
            return New(objects + (x.uid, x) + (tb.uid, tb),role,c);
        }
        public SDatabase Install(SDropIndex d,long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs = obs + (d.uid, d);
            var tb = (STable)objects[d.table];
            var x = tb.FindIndex(this, d.key);
            tb = new STable(tb, tb.indexes - x.uid);
            return New(obs - x.uid + (tb.uid, tb), role, c);
        }
        public virtual STransaction Transact(ReaderBase rdr,bool auto = true)
        {
            var tr = new STransaction(databases[name], rdr, auto);
            rdr.db = tr;
            return tr;
        }
        public virtual SDatabase MaybeAutoCommit()
        {
            return this;
        }
        public virtual SDatabase Rollback()
        {
            return this;
        }
        /// <summary>
        /// Add in read constraints: a key specifies just one row as the read
        /// Constraint. Otherwise lock the entire table
        /// </summary>
        /// <param name="ix"></param>
        /// <param name="_key"></param>
        /// <returns></returns>
        public virtual SDatabase Rdc(SIndex ix, SCList<Variant> _key)
        {
            return this;
        }
        /// <summary>
        /// Add in a read constraint to lock a table
        /// </summary>
        /// <param name="uid">The table uid</param>
        /// <returns></returns>
        public virtual SDatabase Rdc(long uid)
        {
            return this;
        }
        public virtual STable? GetTable(string tn)
        {
            return role.globalNames.Contains(tn)?(STable)objects[role.globalNames[tn]]:null;
        }
        public virtual SIndex? GetPrimaryIndex(long t)
        {
            for (var b = objects.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SIndex x && x.table == t && x.primary)
                    return x;
            return null;
        }
        internal STable Role(STable tb)
        {
            var ss = role.subs[tb.uid];
            var d = SDict<int, (long, string)>.Empty;
            var cp = SDict<int, Serialisable>.Empty;
            var i = 0;
            for (var b = tb.cols.First(); b != null; b = b.Next(),i++)
            {
                var cu = b.Value.Item1;
                var sc = b.Value.Item2;
                var seq = ss.props[cu];
                d += (seq,(cu,tb.display[i].Item2));
                cp += (seq, sc);
            }
            return new STable(tb, tb.cols, d, cp, tb.refs);
        }
    }
    /// <summary>
    /// Roles are used for naming SDbObjects: each role can have its own name for any 
    /// object. A role also has a set of global names for Tables, Types, and Roles.
    /// </summary>
    public class SRole : SDbObject, ILookup<long,string>
    {
        public class SRObject
        {
            public readonly SDict<long, int> props;
            public readonly SDict<string, int> defs;
            public readonly SDict<int, (long,string)> obs;
            public static readonly SRObject Empty =
                new SRObject(SDict<long, int>.Empty, SDict<string, int>.Empty, 
                    SDict<int, (long,string)>.Empty);
            public SRObject(SDict<long,int>p,SDict<string,int>d,SDict<int,(long,string)>s)
            { props = p; defs = d; obs = s; }
            SRObject Remove(int k)
            {
                if (k < 0 || k>=(obs.Length??0))
                    return this;
                var pr = SDict<long, int>.Empty;
                var df = SDict<string, int>.Empty;
                var os = SDict<int, (long, string)>.Empty;
                var m = 0;
                for (var b=obs.First();b!=null;b=b.Next())
                {
                    var (i,(p,n)) = b.Value;
                    if (i == k)
                        continue;
                    pr += (p, m);
                    df += (n, m);
                    os += (m, (p, n));
                    m++;
                }
                return new SRObject(pr, df, os);
            }
            SRObject Add((int,long,string) e)
            {
                var (i, p, n) = e;
                var k = props.defines(p) ? props[p] : -1;
                if (i < 0)
                    i = (k < 0) ? (props.Length ?? 0) : k;
                var pr = SDict<long, int>.Empty;
                var df = SDict<string, int>.Empty;
                var os = SDict<int, (long, string)>.Empty;
                var m = 0;
                for (var b = obs.First(); b != null; b = b.Next())
                {
                    var (ii, (pp, nn)) = b.Value;
                    if (ii == k)
                        continue;
                    pr += (pp, m);
                    df += (nn, m);
                    os += (m, (pp, nn));
                    m++;
                }
                pr += (p, i);
                df += (n, i);
                os += (i, (p, n));
                return new SRObject(pr, df, os);
            }
            public static SRObject operator+(SRObject r,(int,long,string)e)
            {
                return r.Add(e);
            }
            public static SRObject operator-(SRObject r,int k)
            {
                return r.Remove(k);
            }
        }
        public readonly string name;
        public readonly SDict<long, string> uids;
        public readonly SDict<long, SRObject> subs;
        public readonly SDict<string, long> globalNames;
        public static readonly SRole Public = new SRole("PUBLIC", -1);

        public new string this[long s] => uids[s];

        public SRole(string n,long u) :base(Types.SRole,u)
        {
            name = n; uids = SDict<long, string>.Empty;
            subs = SDict<long, SRObject>.Empty;
            globalNames = SDict<string, long>.Empty;
        }
        public SRole(SRole sr,SDict<long,string>u) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = u;
            subs = sr.subs;
            globalNames = sr.globalNames;
        }
        protected SRole(SRole sr,(long,string)e) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids + e;
            subs = sr.subs;
            globalNames = sr.globalNames;
        }
        protected SRole(SRole sr,(string,long)e): base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids+(e.Item2, e.Item1);
            subs = sr.subs;
            globalNames = sr.globalNames + (e.Item1, e.Item2);
        }
        protected SRole(SRole sr, string s) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids;
            subs = sr.subs;
            globalNames = sr.globalNames - s;
        }
        protected SRole(SRole sr,long p,(int,long,string)e) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids;
            var so = sr.subs.defines(p)?sr.subs[p]:SRObject.Empty;
            subs = sr.subs + (p, so + e);
            globalNames = sr.globalNames;
        }
        protected SRole(SRole sr,long p,int k) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids;
            var so = sr.subs.defines(p) ? sr.subs[p] : SRObject.Empty;
            subs = sr.subs + (p, so - k);
            globalNames = sr.globalNames;
        }
        public static SRole operator+(SRole sr,(string,long)e)
        {
            return new SRole(sr, e);
        }
        public static SRole operator +(SRole sr, (long, string) e)
        {
            return new SRole(sr, e);
        }
        public static SRole operator+(SRole sr,(long,int,long,string)e)
        {
            return new SRole(sr, e.Item1,(e.Item2,e.Item3,e.Item4));
        }
        public static SRole operator-(SRole sr,string s)
        {
            return new SRole(sr, s);
        }
        public static SRole operator-(SRole sr,(long,int) e)
        {
            return new SRole(sr, e.Item1, e.Item2);
        }
        public bool defines(long s)
        {
            return uids.Contains(s);
        }
    }
    public class StrongException : Exception
    {
        public StrongException(string m) : base(m) { }
    }
}
