using System.IO;
namespace Shareable
{
    public enum Protocol
    {
        EoF = -1, Get = 1, Begin = 2, Commit = 3, Rollback = 4,
        Table = 5, Alter = 6, Drop = 7, Index = 8, Insert = 9,
        Read = 10, Update = 11, Delete = 12, View = 13
    }
    public enum Responses
    {
        Done = 0, Exception = 1
    }
    public class SDatabase
    {
        public readonly string name;
        public readonly SDict<long, SDbObject> objects;
        public readonly SDict<string, SDbObject> names;
        public readonly long curpos;
        protected static object files = new object(); // a lock
        protected static SDict<string,AStream> dbfiles = SDict<string,AStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string,SDatabase>.Empty;
        public static SDatabase Open(string path,string fname)
        {
            if (dbfiles.Contains(fname))
                return databases.Lookup(fname);
            var db = new SDatabase(fname).Load();
            lock (files)
            {
                dbfiles = dbfiles.Add(fname, new AStream(path+fname));
                databases = databases.Add(fname, db);
            }
            return db;
        }
        public static void Install(SDatabase db)
        {
             databases = databases.Add(db.name, db);
        }
        public virtual SDbObject Lookup(long pos)
        {
            return objects.Lookup(pos);
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
        public SDatabase(SDatabase db)
        {
            name = db.name;
            objects = db.objects;
            names = db.names;
            curpos = db.curpos;
        }
        protected SDatabase(SDatabase db,long p)
        {
            name = db.name;
            objects = db.objects.Remove(p);
            names = db.names;
            curpos = db.curpos;
        }
        public SDatabase(SDatabase db,STable t,long c)
        {
            name = db.name;
            objects = db.objects.Add(t.uid, t);
            names = db.names.Add(t.name, t);
            curpos = c;
        }
        public SDatabase(SDatabase db,SAlter a,long c)
        {
            name = db.name;
            if (a.parent==0)
            {
                var ot = (STable)db.Lookup(a.defpos);
                var nt = new STable(ot,a.name);
                objects = db.objects.Add(a.defpos, nt);
                names = db.names.Remove(ot.name).Add(a.name,nt);
            } else
            {
                var ot = (STable)db.Lookup(a.parent);
                var oc = ot.cols.Lookup(a.defpos);
                var nc = new SColumn(oc, a.name, a.dataType);
                var nt = ot.Add(nc);
                objects = db.objects.Add(a.defpos, nt);
                names = db.names.Add(a.name, nt);
            }
            curpos = c;
        }
        public SDatabase(SDatabase db,SDrop d,long c)
        {
            name = db.name;
            if (d.parent == 0)
            {
                var ot = (STable)db.Lookup(d.drpos);
                objects = db.objects.Remove(d.drpos);
                names = names.Remove(ot.name);
            } else { 
                var ot = (STable)db.Lookup(d.parent);
                var nt = ot.Remove(d.drpos);
                objects = db.objects.Add(d.parent, nt);
            }
            curpos = c;
        }
        public SDatabase(SDatabase db,SView v,long c)
        {
            name = db.name;
            objects = objects.Add(v.uid, v);
            names = names.Add(v.name, v);
            curpos = c;
        }
        public SDatabase(SDatabase db,SIndex x,long c)
        {
            name = db.name;
            objects = objects.Add(x.uid, x);
        }
        SDatabase Load()
        {
            var f = dbfiles.Lookup(name);
            var db = this;
            lock (f)
            {
                for (var s = f.GetOne(this); s != null; s = f.GetOne(this))
                    db = db.Add(s,f.Position);
            }
            return db;
        }
        public SRecord Get(long pos)
        {
            var f = dbfiles.Lookup(name);
            lock (f) { 
                var rc = f.Get(this, pos) as SRecord ?? 
                    throw new System.Exception("Record "+pos+" never defined");
                var tb = Lookup(rc.table) as STable ??
                    throw new System.Exception("Table " + rc.table + " has been dropped");
                if (!tb.rows.Contains(rc.Defpos))
                    throw new System.Exception("Record " + pos + " has been dropped");
                return f.Get(this, tb.rows.Lookup(rc.Defpos)) as SRecord;
            }
        }
        public SDatabase Add(Serialisable s,long p)
        {
            switch (s.type)
            {
                case Types.STable: return Install((STable)s, p); 
                case Types.SColumn: return Install((SColumn)s, p); 
                case Types.SUpdate:
                case Types.SRecord: return Install((SRecord)s, p); 
                case Types.SDelete: return Install((SDelete)s, p); 
                case Types.SAlter: return Install((SAlter)s, p); 
                case Types.SDrop: return Install((SDrop)s, p); 
                case Types.SView: return Install((SView)s, p); 
            }
            return this;
        }
        public SDatabase Remove(long p)
        {
            return new SDatabase(this, p);
        }
        /// <summary>
        /// Close() is only for testing environments!
        /// </summary>
        public void Close()
        {
            lock(files)
            {
                var f = dbfiles.Lookup(name);
                databases = databases.Remove(name);
                dbfiles = dbfiles.Remove(name);
                f.Close();
            }
        }
        protected virtual SDatabase Install(STable t,long c)
        {
            return new SDatabase(this, t, c);
        }
        protected virtual SDatabase Install(SColumn c,long p)
        {
            return new SDatabase(this,((STable)Lookup(c.table)).Add(c),p);
        }
        protected virtual SDatabase Install(SRecord r,long c)
        {
            return new SDatabase(this, ((STable)Lookup(r.table)).Add(r),c);
        }
        protected virtual SDatabase Install(SDelete d,long c)
        {
            return new SDatabase(this, ((STable)Lookup(d.table)).Remove(d.delpos),c);
        }
        protected virtual SDatabase Install(SAlter a,long c)
        {
            return new SDatabase(this, a, c);
        }
        protected virtual SDatabase Install(SDrop d, long c)
        {
            return new SDatabase(this, d, c);
        }
        protected virtual SDatabase Install(SView v, long c)
        {
            return new SDatabase(this, v, c);
        }
        protected virtual SDatabase Install(SIndex x,long c)
        {
            return new SDatabase(this, x, c);
        }
        public virtual STransaction Transact(bool auto=true)
        {
            return new STransaction(this,auto);
        }
        public SDatabase MaybeAutoCommit(STransaction tr)
        {
            return tr.autoCommit ? tr.Commit() : tr;
        }
        public virtual SDatabase Rollback()
        {
            return this;
        }
    }
}
