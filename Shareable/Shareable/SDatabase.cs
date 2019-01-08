using System;
#nullable enable
namespace Shareable
{
    public class SDatabase
    {
        public readonly string name;
        public readonly SDict<long, SDbObject> objects;
        public readonly SDict<string, SDbObject> names;
        public readonly long curpos;
        protected static object files = new object(); // a lock (not normally ever used)
        protected static SDict<string, AStream> dbfiles = SDict<string, AStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string, SDatabase>.Empty;
        internal virtual SDatabase _Rollback => this;
        protected virtual bool Committed => true;
        public static SDatabase Open(string path, string fname)
        {
            if (dbfiles.Contains(fname))
                return databases.Lookup(fname)
                    ?? throw new System.Exception("Database is loading");
            var db = new SDatabase(fname);
            dbfiles = dbfiles+(fname, new AStream(path + fname));
            db = db.Load();
            Install(db);
            return db;
        }
        public static void Install(SDatabase db)
        {
            databases = databases+(db.name, db);
        }
        public virtual SDbObject Lookup(long pos)
        {
            return objects.Lookup(pos);
        }
        public AStream File()
        {
            return dbfiles.Lookup(name);
        }
        public virtual bool Contains(long pos)
        {
            return objects.Contains(pos);
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
        protected SDatabase(SDatabase db, STable t, long c)
        {
            t.Check(db.Committed);
            name = db.name;
            objects = db.objects+(t.uid, t);
            names = db.names+(t.name, t);
            curpos = c;
        }
        protected SDatabase(SDatabase db, SAlter a, long c)
        {
            name = db.name;
            if (a.parent == 0)
            {
                var ot = (STable)db.Lookup(a.defpos);
                var nt = new STable(ot, a.name);
                objects = db.objects+(a.defpos, nt);
                names = db.names-ot.name+(a.name, nt);
            }
            else
            {
                var ot = (STable)db.Lookup(a.parent);
                var oc = (SColumn)ot.cols.Lookup(a.defpos);
                var nc = new SColumn(oc, a.name, a.dataType);
                var nt = ot + nc;
                objects = db.objects+(a.defpos, nt);
                names = db.names+(a.name, nt);
            }
            curpos = c;
        }
        protected SDatabase(SDatabase db, SDrop d, long c)
        {
            name = db.name;
            var obs = objects;
            if (d.parent == 0)
            {
                var ot = db.Lookup(d.drpos);
                switch(ot.type)
                {
                    case Types.STable:
                        names = db.names-((STable)ot).name;
                        break;
                    case Types.SIndex:
                        {
                            var x = (SIndex)ot;
                            var tb = (STable)objects.Lookup(x.table);
                            tb = new STable(tb, tb.indexes-x.uid);
                            obs = obs+(tb.uid, tb);
                            names = db.names;
                            break;
                        }
                    default:
                        names = db.names;
                        break;
                }
                objects = obs-d.drpos;
            }
            else
            {
                var ot = (STable)db.Lookup(d.parent);
                var nt = ot.Remove(d.drpos);
                objects = db.objects+(d.parent, nt);
                names = db.names;
            }
            curpos = c;
        }
        protected SDatabase(SDatabase db, SView v, long c)
        {
            name = db.name;
            objects = db.objects+(v.uid, v);
            names = db.names+(v.name, v);
            curpos = c;
        }
        protected SDatabase(SDatabase db, SIndex x, long c)
        {
            name = db.name;
            var tb = (STable)db.Lookup(x.table);
            for (var b = tb.rows.First(); b != null; b = b.Next())
                x = x + (db.Get(b.Value.val), b.Value.val);
            tb = new STable(tb, tb.indexes + (x.uid, true));
            objects = db.objects + (x.uid, x) + (tb.uid,tb);
            names = db.names;
            curpos = c;
        }
        SDatabase Load()
        {
            var rd = new Reader(dbfiles.Lookup(name), 0);
            var db = this;
            for (var s = rd._Get(this) as SDbObject; s != null; s = rd._Get(db) as SDbObject)
                db = db + (s, s.uid);
            return db;
        }
        Serialisable _Get(long pos)
        {
            if (pos > STransaction._uid)
                return objects.Lookup(pos);
            return new Reader(dbfiles.Lookup(name), pos)._Get(this);
        }
        public SRecord Get(long pos)
        {
            var rc = _Get(pos) as SRecord ??
                throw new System.Exception("Record " + SDbObject._Uid(pos) + " never defined");
            var tb = Lookup(rc.table) as STable ??
                throw new System.Exception("Table " + rc.table + " has been dropped");
            if (!tb.rows.Contains(rc.Defpos))
                throw new System.Exception("Record " + SDbObject._Uid(pos) + " has been dropped");
            return (SRecord)_Get(tb.rows.Lookup(rc.Defpos));
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
                var f = dbfiles.Lookup(name);
                databases = databases-name;
                dbfiles = dbfiles-name;
                f.Close();
            }
        }
        protected SDatabase Install(STable t, long c)
        {
            return new SDatabase(this, t, c);
        }
        protected SDatabase Install(SColumn c, long p)
        {
            return new SDatabase(this, ((STable)Lookup(c.table))+c, p);
        }
        protected SDatabase Install(SRecord r, long c)
        {
            var obs = objects;
            var st = ((STable)Lookup(r.table))+r;
            if (r.uid > STransaction._uid)
                obs = obs+(r.uid, r);
            obs = obs+(r.table, st);
            var nms = names+(st.name, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)objects.Lookup(b.Value.key);
                obs = obs + (x.uid, x + (r, r.uid));
            }
            return new SDatabase(this, obs, nms, c);
        }
        protected SDatabase Install(SUpdate u, long c)
        {
            var obs = objects;
            var st = ((STable)Lookup(u.table))+u;
            SRecord? sr = null;
            obs = obs+(u.table, st);
            var nms = names+(st.name, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)objects.Lookup(b.Value.key);
                if (sr == null)
                    sr = Get(u.defpos);
                obs = obs+(x.uid, x.Update(sr, u, c));
            }
            return new SDatabase(this, obs, nms, c);
        }
        protected SDatabase Install(SDelete d, long c)
        {
            var obs = objects;
            var st = ((STable)Lookup(d.table)).Remove(d.delpos);
            SRecord? sr = null;
            obs = obs+(d.table, st);
            var nms = names+(st.name, st);
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)objects.Lookup(b.Value.key);
                if (sr == null)
                    sr = Get(d.delpos);
                obs = obs+(x.uid, x-(sr, c));
            }
            return new SDatabase(this, obs, nms, c);
        }
        protected SDatabase Install(SAlter a, long c)
        {
            return new SDatabase(this, a, c);
        }
        protected SDatabase Install(SDrop d, long c)
        {
            return new SDatabase(this, d, c);
        }
        protected SDatabase Install(SView v, long c)
        {
            return new SDatabase(this, v, c);
        }
        protected SDatabase Install(SIndex x, long c)
        {
            return new SDatabase(this, x, c);
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
        public STable? GetTable(string tn)
        {
            return (STable)names.Lookup(tn);
        }
        public SIndex? GetPrimaryIndex(long t)
        {
            for (var b = objects.First(); b != null; b = b.Next())
                if (b.Value.val is SIndex x && x.table == t)
                    return x;
            return null;
        }
    }
}
