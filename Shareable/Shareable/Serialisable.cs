using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

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
        SDrop = 15,
        SView = 16,
        SIndex = 17
    }
    public class Serialisable
    {
        public readonly Types type;
        public readonly static Serialisable Null = new Serialisable(Types.Serialisable);
        protected Serialisable(Types t)
        {
            type = t;
        }
        public Serialisable(Types t, StreamBase f)
        {
            type = t;
        }
        public virtual Serialisable Commit(STransaction tr,AStream f)
        {
            f.WriteByte((byte)type);
            return this;
        }
        public virtual STransaction Commit(STransaction tr,int c,AStream f)
        {
            f.WriteByte((byte)type);
            return (tr.committed==c)? tr:new STransaction(tr,c);
        }
        public static Serialisable Get(StreamBase f)
        {
            return new Serialisable(Types.Serialisable);
        }
        public virtual void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
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
        STimestamp(StreamBase f) : base(Types.STimestamp,f)
        {
            ticks = f.GetLong();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutLong(ticks);
            return r;
        }
        public new static STimestamp Get(StreamBase f)
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
        public static readonly SInteger Zero = new SInteger(0);
        public SInteger(int v) : base(Types.SInteger)
        {
            value = v;
        }
        SInteger(StreamBase f) : base(Types.SInteger, f)
        {
            value = f.GetInt();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutInt(value);
            return r;
        }
        public new static Serialisable Get(StreamBase f)
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
        SNumeric(StreamBase f) : base(Types.SNumeric, f)
        {
            mantissa = f.GetLong();
            precision = f.GetInt();
            scale = f.GetInt();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutLong(mantissa);
            f.PutInt(precision);
            f.PutInt(scale);
            return r;
        }
        public new static Serialisable Get(StreamBase f)
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
        SString(StreamBase f) :base(Types.SString, f)
        {
            str = f.GetString();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutString(str);
            return r;
        }
        public new static Serialisable Get(StreamBase f)
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
        SDate(StreamBase f) : base(Types.SDate, f)
        {
            year = f.GetInt();
            month = f.GetInt();
            rest = f.GetLong();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutLong(rest);
            return r;
        }
        public new static Serialisable Get(StreamBase f)
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
        STimeSpan(StreamBase f) : base(Types.STimeSpan, f)
        {
            ticks = f.GetLong();
        }
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            var r = base.Commit(tr,c,f);
            f.PutLong(ticks);
            return r;
        }
        public new static Serialisable Get(StreamBase f)
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
        SBoolean(StreamBase f) : base(Types.SBoolean, f)
        {
            sbool = (SBool)f.GetInt();
        }
        public override Serialisable Commit(STransaction tr,AStream f)
        {
            f.PutInt((int)sbool);
            return this;
        }
        public new static Serialisable Get(StreamBase f)
        {
            return new SBoolean(f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)sbool);
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
        SRow(SDatabase d, StreamBase f) :base(Types.SRow)
        {
            var n = f.GetInt();
            var r = SDict<string, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetString();
                var v = f._Get(d);
                r = r.Add(k, v);
            }
            cols = r;
        }
        SRow(STransaction tr,SRow s,StreamBase f) :base(Types.SRow,f)
        {
            var c = s.cols;
            f.PutInt(s.cols.Count);
            for (var b=s.cols.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                f.PutString(k);
                c = c.Add(b.Value.key,b.Value.val);
            }
            cols = c;
        }
        public override Serialisable Commit(STransaction tr,AStream f)
        {
            return new SRow(tr,this,f);
        }
        public static SRow Get(SDatabase d,StreamBase f)
        {
            return new SRow(d,f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(cols.Count);
            for (var b = cols.First(); b != null; b = b.Next())
            {
                f.PutString(b.Value.key);
                if (b.Value.val is Serialisable s)
                    s.Put(f);
                else
                    Null.Put(f);
            }
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
        protected SDbObject(Types t,STransaction tr) :base(t)
        {
            uid = tr.uid+1;
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
        protected SDbObject(Types t,StreamBase f) : base(t)
        {
            uid = f.Position;
        }
        protected SDbObject(SDbObject s, AStream f):base(s.type)
        {
            uid = f.Length;
            f.WriteByte((byte)s.type);
        }
        internal bool Committed => uid < STransaction._uid;
        internal string Uid()
        {
            return STransaction.Uid(uid);
        }
    }
    public class STable : SDbObject
    {
        public readonly string name;
        public readonly SList<SColumn> cpos;
        public readonly SDict<long, SColumn> cols;
        public readonly SDict<long, long> rows; // defpos->uid of latest update
        public STable(STransaction tr,string n) :base(Types.STable,tr)
        {
            if (tr.names.Contains(n))
                throw new Exception("Table n already exists");
            name = n;
            cols = SDict<long,SColumn>.Empty;
            cpos = SList<SColumn>.Empty;
            rows = SDict<long, long>.Empty;
        }
        public STable Add(SColumn c)
        {
            return new STable(this,cols.Add(c.uid,c),cpos.InsertAt(c,cpos.Length));
        }
        public STable Update(SColumn o,SColumn c)
        {
            var n = 0;
            var ncp = SList<SColumn>.Empty;
            for (var b = cpos.First(); b != null; b = b.Next())
                if (b.Value.uid == o.uid)
                {
                    ncp = cpos.RemoveAt(n).InsertAt(c, n);
                } else
                    n++;
            if (ncp.Length == 0)
                throw new Exception("Could not find column");
            return new STable(this, cols.Remove(o.uid).Add(c.uid, c),ncp);
        }
        public STable Add(SRecord r)
        {
            return new STable(this,rows.Add(r.Defpos, r.uid));
        }
        public STable Remove(long n)
        {
            if (cols.Contains(n))
            {
                var k = 0;
                var cp = cpos;
                for(var b=cpos.First();b!=null;b=b.Next(),k++)
                    if (b.Value.uid==n)
                    {
                        cp = cp.RemoveAt(k);
                        break;
                    }
                return new STable(this, cols.Remove(n),cp);
            }
            else
                return new STable(this, rows.Remove(n));
        }
        public STable(STable t,string n) :base(t)
        {
            name = n;
            cols = t.cols;
            cpos = t.cpos;
            rows = t.rows;
        }
        STable(STable t,SDict<long,SColumn> c,SList<SColumn> p) :base(t)
        {
            name = t.name;
            cpos = p;
            cols = c;
            rows = t.rows;
        }
        STable(STable t,SDict<long,long> r) : base(t)
        {
            name = t.name;
            cols = t.cols;
            cpos = t.cpos;
            rows = r;
        }
        STable(SDatabase d,StreamBase f):base(Types.STable,f)
        {
            name = f.GetString();
            cols = SDict<long,SColumn>.Empty;
            cpos = SList<SColumn>.Empty;
            rows = SDict<long, long>.Empty;
        }
        public STable(STransaction tr,STable t,AStream f) :base(t,f)
        {
            name = t.name;
            f.PutString(name);
            // if we already have columns, they need to be updated wiith our new uid instead of t's
            var nc = SDict<long, SColumn>.Empty;
            var cp = SList<SColumn>.Empty;
            var n = 0;
            for (var b = t.cpos.First(); b != null; b = b.Next(), n++)
            {
                var c = new SColumn(b.Value, uid);
                nc = nc.Add(c.uid, c);
                cp = cp.InsertAt(c, n);
            }
            cols = nc;
            cpos = cp;
            rows = t.rows;
        }
        /// <summary>
        /// Database objects should only be committed once.
        /// So we only commit a table when it is first mentioned.
        /// Its new columns get committed as they are defined.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            if (Committed) // nothing to do!
                return tr;
            var sn = new STable(tr,this, f);
            f.PutString(name);
            return new STransaction(tr, c, this, sn,f.Length);
        }
        public static STable Get(SDatabase d,StreamBase f)
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
        public SColumn(STransaction tr,string n, Types t, long tbl) : base(Types.SColumn,tr)
        {
            name = n; dataType = t; table = tbl;
        }
        internal SColumn(SColumn c,long t) :base (c)
        {
            name = c.name; dataType = c.dataType; 
            table = t;
        }
        public SColumn(SColumn c,string n,Types d) : base(c)
        {
            name = n; dataType = d; table = c.table;
        }
        SColumn(SDatabase d, StreamBase f) :base(Types.SColumn,f)
        {
            name = f.GetString();
            dataType = (Types)f.ReadByte();
            table = f.GetLong();
        }
        public SColumn(STransaction tr,SColumn c,AStream f):base (c,f)
        {
            name = c.name;
            dataType = c.dataType;
            table = tr.Fix(c.table);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
        }
        /// <summary>
        /// Database objects are only committed once.
        /// If Alter is implemented it will be committed when it happens.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override STransaction Commit(STransaction tr,int c,AStream f)
        {
            if (Committed) // nothing to do!
                return base.Commit(tr,c,f);
            return new STransaction(tr,c,(STable)tr.objects.Lookup(table),this, new SColumn(tr, this, f),f.Length);
        }
        public static SColumn Get(SDatabase d, StreamBase f)
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
                case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return d.drpos == table;
                    }
            }
            return false;
        }
        public override string ToString()
        {
            return "Column " + name + " [" + Uid() + "]: " + dataType.ToString();
        }
    }
    public class SAlter : SDbObject
    {
        public readonly long defpos;
        public readonly long parent;
        public string name;
        public Types dataType;
        public SAlter(STransaction tr,string n,Types d,long o,long p) :base(Types.SAlter,tr)
        {
            defpos = o;  name = n; dataType = d; parent = p;
        }
        internal SAlter(SAlter a,long o,long p) :base(a)
        {
            defpos = o; parent = p; name = a.name; dataType = a.dataType;
        }
        SAlter(SDatabase d, StreamBase f):base(Types.SAlter,f)
        {
            defpos = f.GetLong();
            parent = f.GetLong(); //may be -1
            name = f.GetString();
            dataType = (Types)f.ReadByte();
        }
        public SAlter(STransaction tr,SAlter a,AStream f):base(a,f)
        {
            name = a.name;
            dataType = a.dataType;
            defpos = tr.Fix(a.defpos);
            parent = tr.Fix(a.parent);
            f.PutLong(defpos);
            f.PutLong(parent);
            f.PutString(name);
            f.WriteByte((byte)dataType);
        }
        public override STransaction Commit(STransaction tr, int c, AStream f)
        {
            if (Committed)
                return base.Commit(tr, c, f);
            return new STransaction(tr, c, this, new SAlter(tr, this, f), f.Length);
        }
        public static SAlter Get(SDatabase d, StreamBase f)
        {
            return new SAlter(d, f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SAlter:
                    var a = (SAlter)that;
                    return a.defpos == defpos;
                case Types.SDrop:
                    var d = (SDrop)that;
                    return d.drpos == defpos || d.drpos == parent;
            }
            return false;
        }
        public override string ToString()
        {
            return "Alter " + defpos + ((parent!=0)?"":(" of "+parent)) 
                + name + " " + dataType.ToString();
        }
    }
    public class SDrop: SDbObject
    {
        public readonly long drpos;
        public readonly long parent;
        public SDrop(STransaction tr,long d,long p):base(Types.SDrop,tr)
        {
            drpos = d; parent = p;
        }
        internal SDrop(SDrop d,long pos,long par) : base(d)
        {
            drpos = pos; parent = par; 
        }
        SDrop(SDatabase d, StreamBase f) :base(Types.SDrop,f)
        {
            drpos = f.GetLong();
            parent = f.GetLong();
        }
        public SDrop(STransaction tr,SDrop d,AStream f):base(d,f)
        {
            drpos = tr.Fix(d.drpos);
            parent = tr.Fix(d.parent);
            f.PutLong(drpos);
            f.PutLong(parent);
        }
        public override STransaction Commit(STransaction tr, int c, AStream f)
        {
            if (Committed)
                return base.Commit(tr, c, f);
            return new STransaction(tr, c, this, new SDrop(tr, this, f), f.Length);
        }
        public static SDrop Get(SDatabase d, StreamBase f)
        {
            return new SDrop(d, f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return (d.drpos == drpos && d.parent==parent) || d.drpos==parent || d.parent==drpos;
                    }
                case Types.SColumn:
                    {
                        var c = (SColumn)that;
                        return c.table == drpos || c.uid == drpos;
                    }
                case Types.SAlter:
                    {
                        var a = (SAlter)that;
                        return a.defpos == drpos || a.parent == drpos;
                    }
            }
            return false;
        }
        public override string ToString()
        {
            return "Drop " + drpos + ((parent!=0)?"":(" of "+parent));
        }
    }
    public class SView : SDbObject
    {
        public readonly string name;
        public readonly SDict<int, SColumn> cols;
        public readonly string viewdef;
        public SView(STransaction tr,string n,SDict<int,SColumn> c,string d) :base(Types.SView,tr)
        {
            name = n; cols = c; viewdef = d;
        }
        internal SView(SView v,SDict<int,SColumn>c):base(v)
        {
            cols = c; name = v.name; viewdef = v.viewdef;
        }
        SView(SDatabase d, StreamBase f):base(Types.SView,f)
        {
            name = f.GetString();
            var n = f.GetInt();
            var c = SDict<int, SColumn>.Empty;
            for (var i = 0; i < n; i++)
            {
                var nm = f.GetString();
                c = c.Add(i, new SColumn(null, nm, (Types)f.ReadByte(), 0));
            }
            cols = c;
            viewdef = f.GetString();
        }
        public SView(STransaction tr,SView v,AStream f):base(v,f)
        {
            name = v.name;
            cols = v.cols;
            viewdef = v.viewdef;
            f.PutString(name);
            f.PutInt(cols.Count);
            for (var b=cols.First();b!=null;b=b.Next())
            {
                f.PutString(b.Value.val.name);
                f.WriteByte((byte)b.Value.val.type);
            }
            f.PutString(viewdef);
        }
        public override STransaction Commit(STransaction tr, int c, AStream f)
        {
            if (Committed)
                return base.Commit(tr, c, f);
            return new STransaction(tr, c, this, new SView(tr, this, f), f.Length);
        }
        public static SView Get(SDatabase d, StreamBase f)
        {
            return new SView(d, f);
        }
        public override bool Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SView:
                    {
                        var v = (SView)that;
                        return name.CompareTo(v.name) == 0;
                    }
            }
            return false;
        }
    }
    public class SRecord : SDbObject
    {
        public readonly SDict<string, Serialisable> fields;
        public readonly long table;
        public SRecord(STransaction tr,long t,SDict<string,Serialisable> f) :base(Types.SRecord,tr)
        {
            fields = f;
            table = t;
        }
        public virtual long Defpos => uid;
        protected SRecord(SRecord r,long tb) :base(r)
        {
            fields = r.fields;
            table = tb;
        }
        public SRecord(STransaction tr,SRecord r,AStream f) : base(r,f)
        {
            table = tr.Fix(r.table);
            fields = r.fields;
            f.PutLong(table);
            var tb = (STable)tr.objects.Lookup(table);
            f.PutInt(r.fields.Count);
            for (var b=r.fields.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                long p = 0;
                for (var c = tb.cols.First(); c != null; c = c.Next())
                    if (c.Value.val.name == k)
                        p = c.Value.key;
                f.PutLong(p);
                b.Value.val.Commit(tr,f);
            }
        }
        protected SRecord(SDatabase d, StreamBase f) : base(Types.SRecord,f)
        {
            table = f.GetLong();
            var n = f.GetInt();
            var tb = (STable)d.objects.Lookup(table);
            var a = SDict<string, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = tb.cols.Lookup(f.GetLong());
                a = a.Add(k.name, f._Get(d));
            }
            fields = a;
        }
        public override Serialisable Commit(STransaction tr,AStream f)
        {
            return new SRecord(tr, this, f);
        }
        public static SRecord Get(SDatabase d, StreamBase f)
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
        public SUpdate(STransaction tr,SRecord r) : base(tr,r.table,r.fields)
        {
            defpos = r.Defpos;
        }
        public override long Defpos => defpos;
        internal SUpdate(SUpdate u,long tbl,long dp) :base(u,tbl)
        {
            defpos = u.defpos;
        }
        public SUpdate(STransaction tr,SUpdate r, AStream f) : base(tr,r,f)
        {
            defpos = tr.Fix(defpos);
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d, StreamBase f) : base(d,f)
        {
            defpos = f.GetLong();
        }
        public override Serialisable Commit(STransaction tr,AStream f)
        {
            return new SUpdate(tr,this, f);
        }
        public new static SRecord Get(SDatabase d, StreamBase f)
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
            sb.Append(" of "); sb.Append(STransaction.Uid(defpos));
            Append(sb);
            return sb.ToString();
        }
    }
    public class SDelete : SDbObject
    {
        public readonly long table;
        public readonly long delpos;
        public SDelete(STransaction tr, long t, long p) : base(Types.SDelete,tr)
        {
            table = t;
            delpos = p;
        }
        internal SDelete(SDelete u, long tbl, long del) : base(u)
        {
            table = tbl;
            delpos = del;
        }
        public SDelete(STransaction tr, SDelete r, AStream f) : base(r,f)
        {
            table = tr.Fix(r.table);
            delpos = tr.Fix(r.delpos);
            f.PutLong(table);
            f.PutLong(delpos);
        }
        SDelete(SDatabase d, StreamBase f) : base(Types.SDelete,f)
        {
            table = f.GetLong();
            delpos = f.GetLong();
        }
        public override Serialisable Commit(STransaction tr, AStream f)
        {
            return new SDelete(tr, this, f);
        }
        public static SDelete Get(SDatabase d, StreamBase f)
        {
            return new SDelete(d, f);
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
            sb.Append(" of "); sb.Append(STransaction.Uid(delpos));
            sb.Append("["); sb.Append(STransaction.Uid(table)); sb.Append("]");
            return sb.ToString();
        }
    }
    public class SIndex : SDbObject
    {
        public readonly long table;
        public readonly bool primary;
        public readonly long references;
        public readonly SList<long> cols;
        public readonly SMTree rows;
        /// <summary>
        /// A primary or unique index
        /// </summary>
        /// <param name="t"></param>
        /// <param name="c"></param>
        public SIndex(STransaction tr, long t, bool p, SList<long> c) : base(Types.SIndex, tr)
        {
            table = t;
            primary = p;
            cols = c;
            references = 0;
            rows = new SMTree(Info((STable)tr.objects.Lookup(table), cols));
        }
        internal SIndex(SIndex x, long t, SList<long> c) : base(x)
        {
            table = t; cols = c;
            primary = x.primary;
            references = x.references;
            rows = x.rows;
        }
        SIndex(SDatabase d, StreamBase f) : base(Types.SIndex, f)
        {
            table = f.GetLong();
            var n = f.ReadByte();
            var c = new long[n];
            for (var i = 0; i < n; i++)
                c[i] = f.GetInt();
            references = f.GetLong();
            cols = SList<long>.New(c);
            rows = new SMTree(Info((STable)d.objects.Lookup(table), cols));
        }
        public SIndex(STransaction tr, SIndex x, AStream f) : base(x, f)
        {
            table = tr.Fix(x.table);
            long[] c = new long[x.cols.Length];
            var i = 0;
            for (var b = x.cols.First(); b != null; b = b.Next())
                c[i++] = tr.Fix(b.Value);
            references = tr.Fix(x.references);
            f.PutLong(references);
            cols = SList<long>.New(c);
            rows = x.rows;
        }
        public override STransaction Commit(STransaction tr, int c, AStream f)
        {
            if (Committed)
                return base.Commit(tr,c, f);
            return new STransaction(tr, c, this, new SIndex(tr, this, f), f.Length);
        }
        public static SIndex Get(SDatabase d, StreamBase f)
        {
            return new SIndex(d, f);
        }
        SList<TreeInfo> Info(STable tb, SList<long> cols)
        {
            if (cols == null)
                return SList<TreeInfo>.Empty;
            return Info(tb, cols.next).InsertAt(new TreeInfo(tb.cols.Lookup(cols.element).name, 'D', 'D'), 0);
        }
        public override bool Conflicts(Serialisable that)
        {
            return base.Conflicts(that);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Index " + uid + " [" + table + "] (");
            var cm = "";
            for (var b = cols.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append("" + b.Value);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    public abstract class StreamBase : Stream
    {
        /// <summary>
        /// This class is not shareable
        /// </summary>
        public class Buffer
        {
            public const int Size = 1024;
            public byte[] buf;
            public long start;
            public int len;
            public int pos;
            bool eof;
            StreamBase fs;
            public ManualResetEvent wait = null; // for AsyncStream
            public Buffer(StreamBase f)
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
                f.GetBuf(this);
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
                    fs.GetBuf(this);
                    eof = len < Size;
                }
                return buf[pos++];
            }
            internal void PutByte(byte b)
            {
                if (pos >= len)
                {
                    fs.PutBuf(this);
                    start += len;
                    pos = 0;
                }
                buf[pos++] = b;
            }
        }
        protected Buffer rbuf, wbuf;
        protected StreamBase() { }
        protected abstract void GetBuf(Buffer b);
        protected abstract void PutBuf(Buffer b);
        public override int ReadByte()
        {
            return rbuf.GetByte();
        }
        public override void WriteByte(byte value)
        {
            wbuf.PutByte(value);
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
        public Serialisable _Get(SDatabase d)
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
                case Types.STable: s = STable.Get(d, this); break;
                case Types.SRow: s = SRow.Get(d, this); break;
                case Types.SColumn: s = SColumn.Get(d, this); break;
                case Types.SRecord: s = SRecord.Get(d, this); break;
                case Types.SUpdate: s = SUpdate.Get(d, this); break;
            }
            return s;
        }
    }
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class AStream : StreamBase
    {

        public readonly string filename;
        internal Stream file;
        long position = 0, length = 0;
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.Open,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            file.Seek(0, SeekOrigin.Begin);
        }
        public AStream(AsyncStream asy)
        {
            file = asy;
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
        public STransaction Commit(STransaction tr)
        {
            wbuf = new Buffer(this);
            for (var b=tr.steps.First();b!=null; b=b.Next())
            {
                switch (b.Value.val.type)
                {
                    case Types.STable:
                        {
                            var st = (STable)b.Value.val;
                            tr = new STransaction(tr, b.Value.key, st, new STable(tr,st, this),Length);
                            break;
                        }
                    case Types.SColumn:
                        {
                            var sc = (SColumn)b.Value.val;
                            var st = (STable)tr.objects.Lookup(sc.table);
                            tr = new STransaction(tr, b.Value.key, st, sc, new SColumn(tr, sc, this),Length);
                            break;
                        }
                    case Types.SRecord:
                        {
                            var sr = (SRecord)b.Value.val;
                            var st = (STable)tr.objects.Lookup(sr.table);
                            tr = new STransaction(tr, b.Value.key, st, sr, new SRecord(tr, sr, this),Length);
                            break;
                        }
                    case Types.SDelete:
                        {
                            var sd = (SDelete)b.Value.val;
                            var st = (STable)tr.objects.Lookup(sd.table);
                            tr = new STransaction(tr, b.Value.key, st, sd,Length);
                            break;
                        }
                }
            }
            Flush();
            return tr;
        }
        public override bool CanRead => throw new System.NotImplementedException();

        public override bool CanSeek => throw new System.NotImplementedException();

        public override bool CanWrite => throw new System.NotImplementedException();

        public override long Length => length + (wbuf?.pos)??0;

        public override long Position { get => position; set => throw new System.NotImplementedException(); }
        public override void Close()
        {
            file.Close();
            base.Close();
        }

        public override void Flush()
        {
            PutBuf(wbuf);
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

        protected override void GetBuf(Buffer b)
        {
            file.Seek(b.start, SeekOrigin.Begin);
            b.len = file.Read(b.buf, 0, Buffer.Size);
        }

        protected override void PutBuf(Buffer b)
        {
            file.Seek(0, SeekOrigin.End);
            file.Write(b.buf, 0, b.pos);
            file.Flush();
        }
    }
}
