using System.IO;
namespace Shareable
{
    public class SDatabase
    {
        public readonly string name;
        public readonly SDict<long, STable> tables;
        public readonly SDict<string, SDbObject> objects;
        public readonly long curpos;
        static object files = new object(); // a lock
        protected static SDict<string,AStream> dbfiles = SDict<string,AStream>.Empty;
        protected static SDict<string, SDatabase> databases = SDict<string,SDatabase>.Empty;
        public static SDatabase Open(string fname)
        {
            if (dbfiles.Contains(fname))
                return databases.Lookup(fname);
            var db = new SDatabase(fname);
            lock (files)
            {
                dbfiles = dbfiles.Add(fname, new AStream(fname));
                databases = databases.Add(fname, db);
            }
            return db.Load();
        }
        SDatabase(string fname)
        {
            name = fname;
            tables = SDict<long, STable>.Empty;
            objects = SDict<string, SDbObject>.Empty;
            curpos = 0;
        }
        protected SDatabase(SDatabase db)
        {
            name = db.name;
            tables = db.tables;
            objects = db.objects;
        }
        SDatabase(SDatabase db,STable t)
        {
            name = db.name;
            tables = db.tables.Add(t.uid, t);
            objects = db.objects.Add(t.name, t);
        }
        SDatabase Load()
        {
            var f = dbfiles.Lookup(name);
            var db = this;
            lock (f)
            {
                for (var s = f.GetOne(this); s != null; s = f.GetOne(this))
                    switch (s.type)
                    {
                        case Types.STable: db = db.Install((STable)s); break;
                        case Types.SColumn: db = db.Install((SColumn)s); break;
                        case Types.SUpdate:
                        case Types.SRecord: db = db.Install((SRecord)s); break;
                        case Types.SDelete: db = db.Install((SDelete)s); break;
                    }
            }
            return db;
        }
        /// <summary>
        /// Only for testing environments!
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
        SDatabase Install(STable t)
        {
            return new SDatabase(this, t);
        }
        SDatabase Install(SColumn c)
        {
            return new SDatabase(this,tables.Lookup(c.table).Add(c));
        }
        SDatabase Install(SRecord r)
        {
            return new SDatabase(this, tables.Lookup(r.table).Add(r));
        }
        SDatabase Install(SDelete d)
        {
            return new SDatabase(this, tables.Lookup(d.table).Remove(d.delpos));
        }
    }
}
