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
        public readonly SDict<string, SDbObject> names;
        public readonly long curpos;
        protected static object files = new object(); // a lock
        protected static SDict<string, AStream> dbfiles = SDict<string, AStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string, SDatabase>.Empty;
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
        public static void Install(SDatabase db)
        {
            lock(files) databases += (db.name, db);
        }
        public AStream File()
        {
            return dbfiles[name];
        }
        SDatabase(string fname)
        {
            name = fname;
            objects = SDict<long, SDbObject>.Empty;
            names = SDict<string, SDbObject>.Empty;
            curpos = 0;
        }
        protected SDatabase(SDatabase db)
        {
            name = db.name;
            objects = db.objects;
            names = db.names;
            curpos = db.curpos;
        }
        /// <summary>
        /// CRUD on Records changes indexes as well as table, so we need this
        /// </summary>
        /// <param name="db"></param>
        /// <param name="obs"></param>
        /// <param name="c"></param>
        protected SDatabase(SDatabase db, SDict<long, SDbObject> obs, SDict<string,SDbObject> nms, long c)
        {
            name = db.name;
            objects = obs;
            names = nms;
            curpos = c;
        }
        protected SDatabase(SDatabase db,long pos) :this(db)
        {
            curpos = pos;
        }
        SDatabase Load()
        {
            var rd = new Reader(dbfiles[name], 0);
            var db = this;
            for (var s = rd._Get(this) as SDbObject; s != null; s = rd._Get(db) as SDbObject)
                db += (s, s.uid);
            return new SDatabase(db,rd.Position);
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
                case Types.STable: return Install((STable)s, p);
                case Types.SColumn: return Install((SColumn)s, p);
                case Types.SUpdate: return Install((SUpdate)s, p);
                case Types.SRecord: return Install((SRecord)s, p);
                case Types.SDelete: return Install((SDelete)s, p);
                case Types.SAlter: return Install((SAlter)s, p);
                case Types.SDrop: return Install((SDrop)s, p);
                case Types.SView: return Install((SView)s, p);
                case Types.SIndex: return Install((SIndex)s, p);
            }
            return this;
        }
        public static SDatabase operator+(SDatabase d,ValueTuple<SDbObject,long> x)
        {
            return d._Add(x.Item1, x.Item2);
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
        protected virtual SDatabase New(SDict<long,SDbObject> o,SDict<string,SDbObject>ns,long c)
        {
            return new SDatabase(this, o, ns, c);
        }
        public SDatabase Install(STable t, long c)
        {
            return New(objects + (t.uid, t),names + (t.name, t),c);
        }
        public SDatabase Install(SColumn c, long p)
        {
            var obs = objects;
            if (c.uid >= STransaction._uid)
                obs += (c.uid, c);
            var tb = ((STable)obs[c.table])+c;
            return New(obs+(c.table,tb), names+(tb.name,tb), p);
        }
        public SDatabase Install(SRecord r, long c)
        {
            var obs = objects;
            var st = ((STable)obs[r.table])+r;
            if (r.uid >= STransaction._uid)
                obs += (r.uid, r);
            obs += (r.table, st);
            var nms = names+(st.name, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)objects[b.Value.Item1];
                obs += (x.uid, x + (r, r.uid));
                if (x.references == r.table && !x.Contains(r))
                    throw new Exception("Referential constraint");
            }
            return New(obs, nms, c);
        }
        public SDatabase Install(SUpdate u, long c)
        {
            var obs = objects;
            var st = ((STable)obs[u.table])+u;
            SRecord? sr = null;
            if (u.uid >= STransaction._uid)
                obs += (u.uid, u);
            obs += (u.table, st);
            var nms = names+(st.name, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)obs[b.Value.Item1];
                if (sr == null)
                    sr = Get(u.defpos);
                obs += (x.uid, x.Update(sr, u, c));
                if (x.references == u.table && !x.Contains(u))
                    throw new Exception("Referential constraint");
            }
            return New(obs, nms, c);
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
                var x = (SIndex)objects[b.Value.Item1];
                if (sr == null)
                    sr = Get(d.delpos);
                obs += (x.uid, x-(sr, c));
                if (x.references == d.table && x.Contains(sr))
                    throw new Exception("Referential constraint");
            }
            var nms = names;
            if (sr != null)
            {
                st = st.Remove(sr.Defpos);
                obs += (d.table, st);
                nms = names + (st.name, st);
            }
            return New(obs, nms, c);
        }
        public SDatabase Install(SAlter a, long c)
        {
            var obs = objects;
            if (a.uid >= STransaction._uid)
                obs += (a.uid, a);
            if (a.parent == 0)
            {
                var ot = (STable)obs[a.defpos];
                var nt = new STable(ot, a.name);
                return New(obs + (a.defpos, nt), names - ot.name + (a.name, nt), c);
            }
            else
            {
                var ot = (STable)objects[a.parent];
                var oc = (SColumn)ot.cols[a.defpos];
                var nc = new SColumn(oc, a.name, a.dataType);
                var nt = ot + nc;
                return New(obs + (a.defpos, nt),names + (a.name, nt),c);
            }
        }
        public SDatabase Install(SDrop d, long c)
        {
            var obs = objects;
            if (d.uid >= STransaction._uid)
                obs = obs + (d.uid, d);
            if (d.parent == 0)
            {
                var nms = names;
                var ot = objects[d.drpos];
                switch (ot.type)
                {
                    case Types.STable:
                        nms = nms - ((STable)ot).name;
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
                return New(obs - d.drpos, nms, c);
            }
            else
            {
                var ot = (STable)objects[d.parent];
                var nt = ot.Remove(d.drpos);
                return New(obs + (d.parent, nt), names, c);
            }
        }
        public SDatabase Install(SView v, long c)
        {
            return New(objects + (v.uid, v),names + (v.name, v),c);
        }
        public SDatabase Install(SIndex x, long c)
        {
            var tb = (STable)objects[x.table];
            for (var b = tb.rows.First(); b != null; b = b.Next())
                x += (Get(b.Value.Item2), b.Value.Item2);
            tb = new STable(tb, tb.indexes + (x.uid, true));
            return New(objects + (x.uid, x) + (tb.uid, tb),names,c);
        }
        public virtual STransaction Transact(bool auto = true)
        {
            return new STransaction(this, auto);
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
            return (STable?)names[tn];
        }
        public virtual SIndex? GetPrimaryIndex(long t)
        {
            for (var b = objects.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SIndex x && x.table == t)
                    return x;
            return null;
        }
    }
}
