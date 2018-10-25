using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Shareable
{
    public enum Types
    {
        Serialisable = 0,
        STimestamp = 1,
        SInteger = 2,
        SNumeric = 3,
        SString = 4,
        SDate = 5,
        STimeSpan = 6,
        SBoolean = 7,
        SRow = 8,
        STable = 9,
        SColumn = 10,
        SRecord = 11,
        SUpdate = 12,
        SDelete = 13,
        SAlter = 14,
        SView = 15,
        STransaction = 16,
        SPartial = 17,
        SCompound = 18
    }
    public class Serialisable
    {
        public readonly Types type;
        protected Serialisable(Types t)
        {
            type = t;
        }
        public Serialisable(Types t, AStream f)
        {
            type = t;
        }
        public virtual Serialisable Commit(Transaction tr,AStream f)
        {
            f.WriteByte((byte)type);
            return this;
        }
        public static Serialisable Get(AStream f)
        {
            return new Serialisable(Types.Serialisable);
        }
        public virtual bool Conflicts(Serialisable that)
        {
            return false;
        }
        public override string ToString()
        {
            return "Serialisable (null)";
        }
    }
    public class STimestamp : Serialisable
    {
        public readonly long ticks;
        public STimestamp(DateTime t) : base(Types.STimestamp)
        {
            ticks = t.Ticks;
        }
        STimestamp(AStream f) : base(Types.STimestamp,f)
        {
            ticks = f.GetLong();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutLong(ticks);
            return this;
        }
        public new static STimestamp Get(AStream f)
        {
            return new STimestamp(f);
        }
        public override string ToString()
        {
            return "Timestamp " + new DateTime(ticks).ToString();
        }
    }
    public class SInteger : Serialisable
    {
        public readonly int value;
        public SInteger(int v) : base(Types.SInteger)
        {
            value = v;
        }
        SInteger(AStream f) : base(Types.SInteger, f)
        {
            value = f.GetInt();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutInt(value);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new SInteger(f);
        }
        public override string ToString()
        {
            return "Integer " + value.ToString();
        }
    }
    public class SNumeric : Serialisable
    {
        public readonly long mantissa;
        public readonly int precision;
        public readonly int scale;
        public SNumeric(long m,int p,int s) : base(Types.SNumeric)
        {
            mantissa = m;
            precision = p;
            scale = s;
        }
        SNumeric(AStream f) : base(Types.SNumeric, f)
        {
            mantissa = f.GetLong();
            precision = f.GetInt();
            scale = f.GetInt();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutLong(mantissa);
            f.PutInt(precision);
            f.PutInt(scale);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new SNumeric(f);
        }
        public override string ToString()
        {
            return "Numeric " + ((mantissa * Math.Pow(10.0,-scale)).ToString());
        }
    }
    public class SString : Serialisable
    {
        public readonly string str;
        public SString(string s) :base (Types.SString)
        {
            str = s;
        }
        SString(AStream f) :base(Types.SString, f)
        {
            str = f.GetString();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutString(str);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new SString(f);
        }
        public override string ToString()
        {
            return "String '"+str+"'";
        }
    }
    public class SDate : Serialisable
    {
        public readonly int year;
        public readonly int month;
        public readonly long rest;
        public SDate(DateTime s) : base(Types.SDate)
        {
            year = s.Year;
            month = s.Month;
            rest = (s - new DateTime(year, month, 1)).Ticks;
        }
        SDate(AStream f) : base(Types.SDate, f)
        {
            year = f.GetInt();
            month = f.GetInt();
            rest = f.GetLong();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutLong(rest);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new SDate(f);
        }
        public override string ToString()
        {
            return "Date "+(new DateTime(year,month,1)+new TimeSpan(rest)).ToString();
        }
    }
    public class STimeSpan : Serialisable
    {
        public readonly long ticks;
        public STimeSpan(TimeSpan s) : base(Types.STimeSpan)
        {
            ticks = s.Ticks;
        }
        STimeSpan(AStream f) : base(Types.STimeSpan, f)
        {
            ticks = f.GetLong();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutLong(ticks);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new STimeSpan(f);
        }
        public override string ToString()
        {
            return "TimeSpan "+new TimeSpan(ticks).ToString();
        }
    }
    public enum SBool { Unknown=0, True=1, False=2 }
    public class SBoolean : Serialisable
    {
        public readonly SBool sbool;
        public SBoolean(SBool n) : base(Types.SBoolean)
        {
            sbool = n;
        }
        SBoolean(AStream f) : base(Types.SBoolean, f)
        {
            sbool = (SBool)f.GetInt();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            base.Commit(tr,f);
            f.PutInt((int)sbool);
            return this;
        }
        public new static Serialisable Get(AStream f)
        {
            return new SBoolean(f);
        }
        public override string ToString()
        {
            return "Boolean "+sbool.ToString();
        }
    }
    public class SRow : Serialisable
    {
        public readonly SDict<string, Serialisable> cols;
        public SRow() : base(Types.SRow)
        {
            cols = SDict<string, Serialisable>.Empty;
        }
        public SRow Add(string n, Serialisable v)
        {
            return new SRow(cols.Add(n, v));
        }
        public SRow Remove(string n)
        {
            return new SRow(cols.Remove(n));
        }
        SRow(SDict<string,Serialisable> c) :base(Types.SRow)
        {
            cols = c;
        }
        SRow(SDatabase d,AStream f) :base(Types.SRow)
        {
            var n = f.GetInt();
            var r = SDict<string, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetString();
                var v = f.GetOne(d);
                r = r.Add(k, v);
            }
            cols = r;
        }
        SRow(Transaction tr,SRow s,AStream f) :base(Types.SRow)
        {
            var c = s.cols;
            f.PutInt(s.cols.Count);
            for (var b=s.cols.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                f.PutString(k);
                c.Add(k, f.Commit(tr,b.Value.val)[0]);
            }
            cols = c;
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            return new SRow(tr,this,f);
        }
        public static SRow Get(SDatabase d,AStream f)
        {
            return new SRow(d,f);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("SRow (");
            var cm = "";
            for (var b=cols.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.key);
                sb.Append(":");
                sb.Append(b.Value.val.ToString());
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    public abstract class SDbObject : Serialisable
    {
        /// <summary>
        /// For database objects such as STable, we will want to record 
        /// a unique id based on the actual position in the transaction log,
        /// so the Get and Commit methods will capture the appropriate 
        /// file positions in AStream – this is why the Commit method 
        /// needs to create a new instance of the Serialisable. 
        /// The uid will initially belong to the Transaction. 
        /// Once committed the uid will become the position in the AStream file.
        /// </summary>
        public readonly long uid;
        /// <summary>
        /// For a new database object we add it to the transaction steps
        /// and set the transaction-based uid
        /// </summary>
        /// <param name="t"></param>
        /// <param name="tr"></param>
        protected SDbObject(Types t,Transaction tr) :base(t)
        {
            uid = tr.Add(this);
        }
        /// <summary>
        /// A modified database obejct will keep its uid
        /// </summary>
        /// <param name="s"></param>
        protected SDbObject(SDbObject s) : base(s.type)
        {
            uid = s.uid;
        }
        /// <summary>
        /// A database object got from the file will have
        /// its uid given by the position it is read from
        /// </summary>
        /// <param name="t"></param>
        /// <param name="f"></param>
        protected SDbObject(Types t,AStream f) : base(t)
        {
            uid = f.Position;
        }
        /// <summary>
        /// During commit, database objects are appended to the
        /// file, and we will have a (new) modified database object
        /// with its file position as the uid.
        /// </summary>
        /// <param name="s"></param>
        /// <param name="f"></param>
        protected SDbObject(SDbObject s,AStream f) :base(s.type)
        {
            uid = f.Length;
        }
        internal bool Committed => uid < Transaction._uid;
        internal string Uid()
        {
            return Transaction.Uid(uid);
        }
    }
    public class STable : SDbObject
    {
        public readonly string name;
        public readonly SDict<long,SColumn> cols;
        public readonly SDict<long, long> rows; // defpos->uid of latest update
        public STable(Transaction tr,string n) :base(Types.STable,tr)
        {
            if (tr.objects.Contains(n))
                throw new Exception("Table n already exists");
            name = n;
            cols = SDict<long,SColumn>.Empty;
            rows = SDict<long, long>.Empty;
        }
        public STable Add(SColumn c)
        {
            return new STable(this,cols.Add(c.uid,c));
        }
        public STable Update(SColumn c)
        {
            return new STable(this, cols.Add(c.uid, c));
        }
        public STable Add(SRecord r)
        {
            return new STable(this,rows.Add(r.Defpos, r.uid));
        }
        public STable Remove(long n)
        {
            if (cols.Contains(n))
                return new STable(this, cols.Remove(n));
            else
                return new STable(this, rows.Remove(n));
        }
        STable(STable t,SDict<long,SColumn> c) :base(t)
        {
            name = t.name;
            cols = c;
            rows = t.rows;
        }
        STable(STable t,SDict<long,long> r) : base(t)
        {
            name = t.name;
            cols = t.cols;
            rows = r;
        }
        STable(SDatabase d,AStream f):base(Types.STable,f)
        {
            name = f.GetString();
            cols = SDict<long,SColumn>.Empty;
            rows = SDict<long, long>.Empty;
        }
        STable(Transaction tr,STable t,AStream f) :base(t,f)
        {
            name = t.name;
            // if we already have columns, they need to be updated
            var nc = SDict<long,SColumn>.Empty;
            for (var b = t.cols.First(); b != null; b = b.Next())
                nc = nc.Add(b.Value.key,new SColumn(b.Value.val, uid));
            cols = nc;
            // we also need to update any records or deletions 
            // in the transaction that refer to this table
            for(var i=0;i<tr.steps.Count;i++)
            {
                var s = tr.steps[i];
                switch (s.type)
                {
                    case Types.SUpdate:
                    case Types.SRecord:
                        {
                            var sr = (SRecord)s;
                            if (sr.table == t.uid)
                                tr.steps[i] = sr.Fix(this);
                            break;
                        }
                    case Types.SDelete:
                        {
                            var dl = (SDelete)s;
                            if (dl.table == t.uid)
                                tr.steps[i] = dl.Fix(this);
                            break;
                        }
                }
            }
        }
        /// <summary>
        /// Database objects should only be committed once.
        /// So we only commit a table when it is first mentioned.
        /// Its new columns get committed as they are defined.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            if (Committed) // nothing to do!
                return this;
            var r = new STable(tr,this, f);
            base.Commit(tr,f);
            f.PutString(name);
            return r;
        }
        public static STable Get(SDatabase d,AStream f)
        {
            return new STable(d,f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.STable:
                    return ((STable)that).name.CompareTo(name) == 0;
            }
            return false;
        }
        public override string ToString()
        {
            return "Table "+name+"["+Uid()+"]";
        }
    }
    public class SColumn : SDbObject
    {
        public readonly string name;
        public readonly Types dataType;
        public readonly long table;
        public SColumn(Transaction tr,string n, Types t, long tbl) : base(Types.SColumn,tr)
        {
            name = n; dataType = t; table = tbl;
        }
        internal SColumn(SColumn c,long t) :base (c)
        {
            name = c.name; dataType = c.dataType; 
            table = t;
        }
        SColumn(SDatabase d,AStream f) :base(Types.SColumn,f)
        {
            name = f.GetString();
            dataType = (Types)f.ReadByte();
            table = f.GetLong();
        }
        SColumn(Transaction tr,SColumn c,AStream f):base (c,f)
        {
            name = c.name;
            dataType = c.dataType;
            table = c.table;
        }
        /// <summary>
        /// Database objects are only committed once.
        /// If Alter is implemented it will be committed when it happens.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            if (Committed) // nothing to do!
                return this;
            var r = new SColumn(tr, this, f);
            base.Commit(tr,f);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            return r;
        }
        public static SColumn Get(SDatabase d,AStream f)
        {
            return new SColumn(d,f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SColumn:
                    {
                        var c = (SColumn)that;
                        return c.table == table && c.name.CompareTo(name) == 0;
                    }
            }
            return false;
        }
        public override string ToString()
        {
            return "Column " + name + " [" + Uid() + "]: " + dataType.ToString();
        }
    }
    public class SRecord : SDbObject
    {
        public readonly SDict<string, Serialisable> fields;
        public readonly long table;
        public SRecord(Transaction tr,long t,SDict<string,Serialisable> f) :base(Types.SRecord,tr)
        {
            fields = f;
            table = t;
        }
        public virtual long Defpos => uid;
        public Serialisable Field(string col)
        {
            return fields.Lookup(col);
        }
        public virtual SRecord Fix(STable t)
        {
            return new SRecord(this, t.uid);
        }
        protected SRecord(SRecord r,long tb) :base(r)
        {
            fields = r.fields;
            table = tb;
        }
        protected SRecord(Transaction tr,SRecord r,AStream f) : base(r,f)
        {
            table = r.table;
            f.PutLong(table);
            var tb = tr.tables.Lookup(table);
            var a = SDict<string, Serialisable>.Empty;
            f.PutInt(fields.Count);
            for (var b=fields.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                long p = 0;
                for (var c = tb.cols.First(); c != null; c = c.Next())
                    if (c.Value.val.name == k)
                        p = c.Value.key;
                f.PutLong(p);
                a = a.Add(k, b.Value.val.Commit(tr,f));
            }
            fields = a;
            for (var i=0;i<tr.steps.Count;i++)
            {
                var s = tr.steps[i];
                switch(s.type)
                {
                    case Types.SUpdate:
                        var u = (SUpdate)s;
                        if (u.Defpos==r.Defpos)
                            tr.steps[i] = new SUpdate(u, u.table, Defpos);
                        break;
                    case Types.SDelete:
                        var d = (SDelete)s;
                        if (d.delpos==r.Defpos)
                        tr.steps[i] = new SDelete(d, d.table, Defpos);
                        break;
                }
            }
        }
        protected SRecord(SDatabase d,AStream f) : base(Types.SRecord,f)
        {
            table = f.GetLong();
            var n = f.GetInt();
            var tb = d.tables.Lookup(table);
            var a = SDict<string, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = tb.cols.Lookup(f.GetLong());
                a = a.Add(k.name, f.GetOne(d));
            }
            fields = a;
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            return new SRecord(tr, this, f);
        }
        public static SRecord Get(SDatabase d,AStream f)
        {
            return new SRecord(d,f);
        }
        protected void Append(StringBuilder sb)
        {
            sb.Append(" for "); sb.Append(Uid());
            var cm = "(";
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.key); sb.Append("=");
                sb.Append(b.Value.val.ToString());
            }
            sb.Append(")");
        }
        public override bool Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDelete:
                    return ((SDelete)that).delpos == Defpos;
            }
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Record ");
            sb.Append(Uid());
            Append(sb);
            return sb.ToString();
        }
    }
    public class SUpdate : SRecord
    {
        public readonly long defpos;
        public SUpdate(Transaction tr,SRecord r) : base(tr,r.table,r.fields)
        {
            defpos = r.Defpos;
        }
        public override long Defpos => defpos;
        public override SRecord Fix(STable tb)
        {
            return new SUpdate(this,tb.uid,defpos);
        }
        internal SUpdate(SUpdate u,long tbl,long dp) :base(u,tbl)
        {
            defpos = u.defpos;
        }
        SUpdate(Transaction tr,SUpdate r, AStream f) : base(tr,r,f)
        {
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d,AStream f) : base(d,f)
        {
            defpos = f.GetLong();
        }
        public override Serialisable Commit(Transaction tr,AStream f)
        {
            return new SUpdate(tr,this, f);
        }
        public new static SRecord Get(SDatabase d,AStream f)
        {
            return new SUpdate(d,f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos == Defpos;
            }
            return base.Conflicts(that);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Update ");
            sb.Append(Uid());
            sb.Append(" of "); sb.Append(Transaction.Uid(defpos));
            Append(sb);
            return sb.ToString();
        }
    }
    public class SDelete : SDbObject
    {
        public readonly long table;
        public readonly long delpos;
        public SDelete(Transaction tr, long t, long p) : base(Types.SDelete,tr)
        {
            table = t;
            delpos = p;
        }
        internal SDelete(SDelete u, long tbl, long del) : base(u)
        {
            table = tbl;
            delpos = del;
        }
        SDelete(Transaction tr, SDelete r, AStream f) : base(r,f)
        {
            f.PutLong(table);
            f.PutLong(delpos);
        }
        SDelete(SDatabase d, AStream f) : base(Types.SDelete,f)
        {
            table = f.GetLong();
            delpos = f.GetLong();
        }
        public override Serialisable Commit(Transaction tr, AStream f)
        {
            return new SDelete(tr, this, f);
        }
        public static SDelete Get(SDatabase d, AStream f)
        {
            return new SDelete(d, f);
        }
        internal Serialisable Fix(STable sTable)
        {
            return new SDelete(this,sTable.uid,delpos);
        }
        public override bool Conflicts(Serialisable that)
        { 
            switch(that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos == delpos;
                case Types.SRecord:
                    return ((SRecord)that).Defpos == delpos;
            }
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Delete ");
            sb.Append(Uid());
            sb.Append(" of "); sb.Append(Transaction.Uid(delpos));
            sb.Append("["); sb.Append(Transaction.Uid(table)); sb.Append("]");
            return sb.ToString();
        }
    }
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class AStream : Stream
    {
        /// <summary>
        /// This class is not shareable
        /// </summary>
        class Buffer
        {
            const int Size = 1024;
            public byte[] buf;
            public long start;
            public int len;
            public int pos;
            bool eof;
            AStream fs;
            public Buffer(AStream f)
            {
                buf = new byte[Size];
                pos = 0;
                len = Size;
                start = f.Length;
                eof = false;
                fs = f;
            }
            internal Buffer(AStream f, long s)
            {
                buf = new byte[Size];
                start = s;
                pos = 0;
                f.file.Seek(start, SeekOrigin.Begin);
                len = f.file.Read(buf, 0, Size);
                eof = len < Size;
                fs = f;
            }
            internal int GetByte()
            {
                if (pos >= len)
                {
                    if (eof)
                        return -1;
                    start += len;
                    pos = 0;
                    fs.file.Seek(start, SeekOrigin.Begin);
                    len = fs.file.Read(buf, 0, Size);
                    eof = len < Size;
                }
                return buf[pos++];
            }
            internal void PutByte(byte b)
            {
                if (pos >= len)
                {
                    fs.file.Seek(0, SeekOrigin.End);
                    fs.file.Write(buf, 0, len);
                    start += len;
                    pos = 0;
                }
                buf[pos++] = b;
            }
        }
        public readonly string filename;
        internal FileStream file;
        long position = 0, length = 0;
        Buffer rbuf, wbuf;
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.Open,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            file.Seek(0, SeekOrigin.Begin);
        }
        public Serialisable[] Commit(Transaction tr,params Serialisable[] obs)
        {
            wbuf = new Buffer(this);
            var r = new Serialisable[obs.Length];
            for (var i = 0; i < obs.Length; i++)
                r[i] = obs[i].Commit(tr,this);
            file.Seek(0, SeekOrigin.End);
            file.Write(wbuf.buf, 0, wbuf.pos);
            length += wbuf.pos;
            return r;
        }
        Serialisable _Get(SDatabase d)
        {
            Types tp = (Types)ReadByte();
            Serialisable s = null;
            switch (tp)
            {
                case Types.Serialisable: s = Serialisable.Get(this); break;
                case Types.STimestamp: s = STimestamp.Get(this); break;
                case Types.SInteger: s = SInteger.Get(this); break;
                case Types.SNumeric: s = SNumeric.Get(this); break;
                case Types.SString: s = SString.Get(this); break;
                case Types.SDate: s = SDate.Get(this); break;
                case Types.STimeSpan: s = STimeSpan.Get(this); break;
                case Types.SBoolean: s = SBoolean.Get(this); break;
                case Types.STable: s = STable.Get(d,this); break;
                case Types.SRow: s = SRow.Get(d,this); break;
                case Types.SColumn: s = SColumn.Get(d,this); break;
                case Types.SRecord: s = SRecord.Get(d,this); break;
                case Types.SUpdate: s = SUpdate.Get(d,this); break;
            }
            return s;
        }
        public Serialisable GetOne(SDatabase d)
        {
            lock (file)
            {
                if (position == file.Length)
                    return null;
                rbuf = new Buffer(this, position);
                var r = _Get(d);
                position = rbuf.start + rbuf.pos;
                return r;
            }
        }
        /// <summary>
        /// Called from Commit(): file is already locked
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="pos"></param>
        /// <returns></returns>
        public Serialisable[] GetAll(SDatabase d,long pos)
        {
            var r = new List<Serialisable>();
            position = pos;
            rbuf = new Buffer(this, pos);
            while (position<file.Length)
            {
                r.Add(_Get(d));
                position = rbuf.start + rbuf.pos;
            }
            return r.ToArray();
        }
        public Serialisable Get(SDatabase d,long pos)
        {
            lock (file)
            {
                position = pos;
                rbuf = new Buffer(this, position);
                return _Get(d);
            }
        }
        public override int ReadByte()
        {
            return rbuf.GetByte();
        }
        public override void WriteByte(byte value)
        {
            wbuf.PutByte(value);
        }
        public override void Close()
        {
            file.Close();
            base.Close();
        }
        public void PutInt(int n)
        {
            for (int j = 24; j >= 0; j -= 8)
                WriteByte((byte)(n >> j));
        }
        public void PutLong(long t)
        {
            for (int j = 56; j >= 0; j -= 8)
                WriteByte((byte)(t >> j));
        }
        public void PutString(string s)
        {
            var cs = Encoding.UTF8.GetBytes(s);
            PutInt(cs.Length);
            for (var i = 0; i < cs.Length; i++)
                WriteByte(cs[i]);
        }
        public int GetInt()
        {
            int v = 0;
            for (int j = 0; j < 4; j++)
                v = (v << 8) + ReadByte();
            return v;
        }
        public long GetLong()
        {
            long v = 0;
            for (int j = 0; j < 8; j++)
                v = (v << 8) + ReadByte();
            return v;
        }
        public string GetString()
        {
            int n = GetInt();
            byte[] cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return Encoding.UTF8.GetString(cs, 0, n);
        }
        public override bool CanRead => throw new System.NotImplementedException();

        public override bool CanSeek => throw new System.NotImplementedException();

        public override bool CanWrite => throw new System.NotImplementedException();

        public override long Length => length + (wbuf?.pos)??0;

        public override long Position { get => position; set => throw new System.NotImplementedException(); }

        public override void Flush()
        {
            throw new System.NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new System.NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new System.NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new System.NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new System.NotImplementedException();
        }
    }
}
