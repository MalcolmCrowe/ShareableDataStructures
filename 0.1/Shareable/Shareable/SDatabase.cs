using System;
#nullable enable
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
        protected static object files = new object(); // a lock
        protected static SDict<string, AStream> dbfiles = SDict<string, AStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string, SDatabase>.Empty;
        public static readonly SDatabase _system = System(); 
        internal virtual SDatabase _Rollback => this;
        protected virtual bool Committed => true;
        public static SDatabase Open(string path, string fname)
        {
            if (dbfiles.Contains(fname))
                return databases[fname]
                    ?? throw new System.Exception("Database is loading");
            var db = new SDatabase(fname);
            dbfiles += (fname, new AStream(path + fname));
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
            lock(files) databases += (db.name, db);
        }
        public AStream File()
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
            var rd = new Reader(this, curpos);
            for (var s = rd._Get() as SDbObject; s != null; s = rd._Get() as SDbObject)
                rd.db += (s,s.uid);
            return new SDatabase(rd.db,rd.Position);
        }
        protected virtual Serialisable _Get(long pos)
        {
            return dbfiles[name].Lookup(this, pos);
        }
        public SRecord Get(long pos)
        {
            var rc = _Get(pos) as SRecord ??
                throw new Exception("Record " + SDbObject._Uid(pos) + " never defined");
            var tb = objects[rc.table] as STable ??
                throw new Exception("Table " + rc.table + " has been dropped");
            if (!tb.rows.Contains(rc.Defpos))
                throw new Exception("Record " + SDbObject._Uid(pos) + " has been dropped");
            var dp = tb.rows[rc.Defpos];
            if (dp == pos)
                return rc;
            return (SRecord)_Get(dp);
        }
        protected SDatabase _Add(SDbObject s, long p)
        {
            switch (s.type)
            {
                case Types.SUpdate: return Install((SUpdate)s, p);
                case Types.SRecord: return Install((SRecord)s, p);
                case Types.SDelete: return Install((SDelete)s, p);
                case Types.SAlter: return Install((SAlter)s, p);
                case Types.SDrop: return Install((SDrop)s, p);
                case Types.SIndex: return Install((SIndex)s, p);
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
        public static SDatabase operator +(SDatabase d, (long, long, string) n)
        {
            return d.New(d.objects, d.role + n, d.curpos);
        }
        public static SDatabase operator+(SDatabase d,(SDbObject,long) x)
        {
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
            lock (files)
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
                throw new Exception("Bad long " + SDbObject._Uid(uid));
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
            if (role.defs.Contains(c.table) && role.defs[c.table].Contains(n))
                throw new Exception("Table " + uids[tb.uid] + " already has column " + n);
            return New(obs + (c.table, tb+c)+(c.uid,c), role+(c.table,c.uid,n)+(c.uid,n), p);
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
            SRecord? sr = null;
            if (u.uid >= STransaction._uid)
                obs += (u.uid, u);
            obs += (u.table, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)obs[b.Value.Item1];
                if (sr == null)
                    sr = Get(u.defpos);
                var ok = x.Key(sr, x.cols);
                var uk = x.Key(u, x.cols);
                x.Check(this, u, ok.CompareTo(uk)==0);
                obs += (x.uid, x.Update(sr,ok,u,uk,c));
            }
            return New(obs, ro, c);
        }
        public SDatabase Install(SDelete d, long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs += (d.uid, d);
            SRecord? sr = null;
            var st = (STable)obs[d.table];
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var px = (SIndex)obs[b.Value.Item1];
                if (sr == null)
                    sr = Get(d.delpos);
                obs += (px.uid, px - (sr, c));
                if (!px.primary)
                    continue;
                var k = px.Key(sr, px.cols);
                for (var ob = obs.PositionAt(0); ob != null; ob = ob.Next()) // don't bother with system tables
                    if (ob.Value.Item2 is STable ot)
                        for (var ox = ot.indexes.First(); ox != null; ox = ox.Next())
                        {
                            var x = (SIndex)obs[ox.Value.Item1];
                            if (x.references == d.table && x.rows.Contains(k))
                                throw new Exception("Referential constraint: illegal delete");
                        }
            }
            var ro = role;
            if (sr != null)
            {
                st = st.Remove(sr.Defpos);
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
                var ot = (STable)objects[a.col];
                var nc = new SColumn(ot.uid, a.dataType, a.col);
                var nt = ot + nc;
                return New(obs + (a.defpos, nt),role,c);
            }
        }
        public SDatabase Install(SDrop d, long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs = obs + (d.uid, d);
            if (d.parent == 0)
            {
                var ro = role;
                var ot = objects[d.drpos];
                switch (ot.type)
                {
                    case Types.STable:
                        ro -= Name(((STable)ot).uid);
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
                var nt = ot.Remove(d.drpos);
                return New(obs + (d.parent, nt), role, c);
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
        public virtual STransaction Transact(Reader rdr,bool auto = true)
        {
            var tr = new STransaction(this, rdr, auto);
            rdr.db = tr;
            return tr;
        }
        public SDatabase MaybeAutoCommit(STransaction tr)
        {
            return tr.autoCommit ? tr.Commit() : tr;
        }
        public virtual SDatabase Rollback()
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
    }
    /// <summary>
    /// Roles are used for naming SDbObjects: each role can have its own name for any 
    /// object. A role also has a set of global names for Tables, Types, and Roles.
    /// </summary>
    public class SRole : SDbObject, ILookup<long,string>
    {
        public readonly string name;
        public readonly SDict<long, string> uids;
        public readonly SDict<long, SDict<long, string>> props;
        public readonly SDict<long, SDict<string, long>> defs;
        public readonly SDict<string, long> globalNames;
        public static readonly SRole Public = new SRole("PUBLIC", -1);

        public new string this[long s] => uids[s];

        public SRole(string n,long u) :base(Types.SRole,u)
        {
            name = n; uids = SDict<long, string>.Empty;
            props = SDict<long, SDict<long, string>>.Empty;
            defs = SDict<long, SDict<string, long>>.Empty;
            globalNames = SDict<string, long>.Empty;
        }
        public SRole(SRole sr,SDict<long,string>u) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = u;
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames;
        }
        protected SRole(SRole sr,(long,string)e) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids + e;
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames;
        }
        protected SRole(SRole sr,(string,long)e): base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids+(e.Item2, e.Item1);
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames + (e.Item1, e.Item2);
        }
        protected SRole(SRole sr, string s) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids;
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames - s;
        }
        protected SRole(SRole sr,(long,long,string)e) :base(Types.SRole,sr.uid)
        {
            name = sr.name;
            uids = sr.uids;
            var p = sr.props;
            var d = sr.defs;
            if (!p.Contains(e.Item1))
            {
                p += (e.Item1, SDict<long, string>.Empty);
                d += (e.Item1, SDict<string, long>.Empty);
            }
            props = p + (e.Item1, p[e.Item1] + (e.Item2,e.Item3));
            defs = d + (e.Item1, d[e.Item1] + (e.Item3, e.Item2));
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
        public static SRole operator+(SRole sr,(long,long,string)e)
        {
            return new SRole(sr, e);
        }
        public static SRole operator-(SRole sr,string s)
        {
            return new SRole(sr, s);
        }
        public bool defines(long s)
        {
            return uids.Contains(s);
        }
    }
}
