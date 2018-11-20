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
        SIndex = 17,
        SSearch = 18
    }
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
    public class Serialisable:IComparable
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
        public static Serialisable Get(StreamBase f)
        {
            return Null;
        }
        public virtual void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
        }
        public virtual bool Conflicts(Serialisable that)
        {
            return false;
        }
        public virtual void Append(StringBuilder sb)
        {
            sb.Append(this);
        }
        public virtual int CompareTo(object ob)
        {
            return (ob == Null) ? 0 : -1;
        }
        public override string ToString()
        {
            return "Null";
        }
    }
    public class STimestamp : Serialisable,IComparable
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
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(ticks);
        }
        public new static STimestamp Get(StreamBase f)
        {
            return new STimestamp(f);
        }
        public override string ToString()
        {
            return "Timestamp " + new DateTime(ticks).ToString();
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (STimestamp)obj;
            return ticks.CompareTo(that.ticks);
        }
    }
    public class SInteger : Serialisable, IComparable
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
        public new static Serialisable Get(StreamBase f)
        {
            return new SInteger(f);
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append(value);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(value);
        }
        public override string ToString()
        {
            return "Integer " + value.ToString();
        }
        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SInteger)obj;
            return value.CompareTo(that.value);
        }
    }
    public class SNumeric : Serialisable,IComparable
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
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(mantissa);
            f.PutInt(precision);
            f.PutInt(scale);
        }
        public new static Serialisable Get(StreamBase f)
        {
            return new SNumeric(f);
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append(mantissa * Math.Pow(10.0, -scale));
        }
        public double ToDouble()
        {
            return 1.0 * mantissa * Math.Pow(10.0, scale);
        }
        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SNumeric)obj;
            return ToDouble().CompareTo(that.ToDouble());
        }
        public override string ToString()
        {
            return "Numeric " + ((mantissa * Math.Pow(10.0,-scale)).ToString());
        }
    }
    public class SString : Serialisable,IComparable
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
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(str);
        }
        public new static Serialisable Get(StreamBase f)
        {
            return new SString(f);
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append("'"); sb.Append(str); sb.Append("'");
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SString)obj;
            return str.CompareTo(that.str);
        }
        public override string ToString()
        {
            return "String '"+str+"'";
        }
    }
    public class SDate : Serialisable,IComparable
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
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutLong(rest);
        }
        public new static Serialisable Get(StreamBase f)
        {
            return new SDate(f);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SDate)obj;
            var c = year.CompareTo(that.year);
            if (c == 0)
                c = month.CompareTo(that.month);
            if (c == 0)
                c = rest.CompareTo(that.rest);
            return c;
        }
        public override string ToString()
        {
            return "Date "+(new DateTime(year,month,1)+new TimeSpan(rest)).ToString();
        }
    }
    public class STimeSpan : Serialisable,IComparable
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
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(ticks);
        }
        public new static Serialisable Get(StreamBase f)
        {
            return new STimeSpan(f);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (STimeSpan)obj;
            return ticks.CompareTo(that.ticks);
        }
        public override string ToString()
        {
            return "TimeSpan "+new TimeSpan(ticks).ToString();
        }
    }
    public enum SBool { Unknown=0, True=1, False=2 }
    public class SBoolean : Serialisable,IComparable
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
        public new static Serialisable Get(StreamBase f)
        {
            return new SBoolean(f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)sbool);
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append(sbool);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SBoolean)obj;
            return sbool.CompareTo(that.sbool);
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
        public override void Append(StringBuilder sb)
        {
            sb.Append('(');
            var cm = "";
            for (var b = cols.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.key);
                sb.Append(":");
                sb.Append(b.Value.val.ToString());
            }
            sb.Append(")");
        }
        public override string ToString()
        {
            var sb = new StringBuilder("SRow ");
            Append(sb);
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
        /// We assume that the database file is smaller than 0x40000000.
        /// Other ranges of uids:
        /// Transaction-local uids: 0x40000000-0x7fffffff
        /// System uids (_Log tables etc): 0x90000000-0x80000001
        /// Client session-local uids (disambiguation): 0xffffffff-0x90000001
        /// </summary>
        public readonly long uid;
        /// <summary>
        /// For system tables and columns, with negative uids
        /// </summary>
        /// <param name="t"></param>
        /// <param name="u"></param>
        protected SDbObject(Types t,long u) :base(t)
        {
            uid = u;
        }
        /// <summary>
        /// For a new database object we set the transaction-based uid
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
        /// its uid given by the position it is read from.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="f"></param>
        protected SDbObject(Types t,StreamBase f) : base(t)
        {
            uid = f.Position;
        }
        /// <summary>
        /// During commit, database objects are appended to the
        /// file, and we will have a (new) modified database object
        /// with its file position as the uid.
        /// We remember the correspondence between new and old in the AStream
        /// temporarily (we reinitialise the uids on each Commit)
        /// </summary>
        /// <param name="s"></param>
        /// <param name="f"></param>
        protected SDbObject(SDbObject s, AStream f):base(s.type)
        {
            if (s.uid < STransaction._uid)
                throw new Exception("Internal error - misplaced database object");
            uid = f.Length;
            f.uids = f.uids.Add(s.uid, uid);
            f.WriteByte((byte)s.type);
        }
        /// <summary>
        /// This little routine provides a check on DBMS implementation
        /// </summary>
        /// <param name="committed"></param>
        internal void Check(bool committed)
        {
            if (committed != uid < STransaction._uid)
                throw new Exception("Internal error - Committed check fails");
        }
        public override void Put(StreamBase f)
        {
            throw new NotImplementedException();
        }
        internal string Uid()
        {
            return STransaction.Uid(uid);
        }
    }
    public class STable : SQuery
    {
        public readonly string name;
        public readonly SDict<long, long> rows; // defpos->uid of latest update
        public STable(STransaction tr,string n) :base(Types.STable,tr)
        {
            if (tr.names.Contains(n))
                throw new Exception("Table n already exists");
            name = n;
            rows = SDict<long, long>.Empty;
        }
        public virtual STable Add(SColumn c)
        {
            var t = new STable(this,cols.Add(c.uid,c),cpos.InsertAt(c,cpos.Length),
                names.Add(c.name,c));
            return t;
        }
        public STable Add(SRecord r)
        {
            return new STable(this,rows.Add(r.Defpos, r.uid));
        }
        /// <summary>
        /// This method works for SColumns and SRecords
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public STable Remove(long n)
        {
            if (cols.Contains(n))
            {
                var k = 0;
                var cp = cpos;
                var sc = cols.Lookup(n);
                for(var b=cpos.First();b!=null;b=b.Next(),k++)
                    if (b.Value.uid==n)
                    {
                        cp = cp.RemoveAt(k);
                        break;
                    }
                return new STable(this, cols.Remove(n),cp,names.Remove(sc.name));
            }
            else
                return new STable(this, rows.Remove(n));
        }
        /// <summary>
        /// for system and client table references
        /// </summary>
        /// <param name="n"></param>
        /// <param name="u">will be negative</param>
        public STable(string n, long u)
            : base(Types.STable, u)
        {
            name = n;
            rows = SDict<long, long>.Empty;
        }
        public STable(STable t,string n) :base(t)
        {
            name = n;
            rows = t.rows;
        }
        protected STable(STable t,SDict<long,SSelector> c,SList<SSelector> p,SDict<string,SSelector> n) :base(t,c,p,n)
        {
            name = t.name;
            rows = t.rows;
        }
        protected STable(STable t,SDict<long,long> r) : base(t)
        {
            name = t.name;
            rows = r;
        }
        STable(StreamBase f):base(Types.STable,f)
        {
            name = f.GetString();
            rows = SDict<long, long>.Empty;
        }
        public STable(STable t,AStream f) :base(t,f)
        {
            name = t.name;
            f.PutString(name);
            rows = t.rows;
        }
        public new static STable Get(StreamBase f)
        {
            return new STable(f);
        }
        public override SQuery Lookup(SDatabase db)
        {
            var tb = (name[0] == '_' && SysTable.system.Lookup(name) is SysTable st) ?
                new SysTable(name) :
                db.GetTable(name) ??
                throw new Exception("No such table " + name);
            if (cols.Length == 0)
                return tb;
            var co = SDict<long, SSelector>.Empty;
            var cp = SList<SSelector>.Empty;
            var cn = SDict<string, SSelector>.Empty;
            for (var c = cpos;c!=null && c.Length!=0;c=c.next)
            {
                var tc = tb.names.Lookup(((SColumn)c.element).name);
                co = co.Add(tc.uid, tc);
                cp = cp.InsertAt(tc, cp.Length);
                cn = cn.Add(tc.name, tc);
            }
            return new STable(tb, co, cp, cn);
        }
        public override RowSet RowSet(SDatabase db)
        {
            return new TableRowSet(db, this);
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
        public override void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
            f.PutString(name);
        }
        public override string ToString()
        {
            return "Table "+name+"["+Uid()+"]";
        }
    }
    public class SysTable : STable
    {
        public static long _uid = 0x90000000;
        public static SDict<string, SysTable> system = SDict<string, SysTable>.Empty;
        /// <summary>
        /// System tables are like templates: need to be virtually specialised for a db
        /// </summary>
        /// <param name="n"></param>
        public SysTable(string n) : base(n, --_uid)
        {
        }
        SysTable(SysTable t, SDict<long, SSelector> c, SList<SSelector> p, SDict<string, SSelector> n)
            : base(t, c, p, n)
        {
        }
        static SysTable()
        {
            var t = new SysTable("_Log");
            t = t.Add("Uid", Types.SString);
            t = t.Add("Type", Types.SInteger);
            t = t.Add("Desc", Types.SString);
            system = system.Add(t.name, t);
        }
        public override STable Add(SColumn c)
        {
            return new SysTable(this, cols.Add(c.uid, c), cpos.InsertAt(c, cpos.Length),
                names.Add(c.name, c));
        }
        SysTable Add(string n, Types t)
        {
            return Add(new SysColumn(n, t)) as SysTable;
        }
        public override RowSet RowSet(SDatabase db)
        {
            return new SysRows(db,this);
        }
    }
    internal class SysColumn : SColumn
    {
        internal SysColumn(string n, Types t) : base(n, t, --SysTable._uid)
        { }
    }
    public abstract class SSelector : SDbObject
    {
        public readonly string name;
        public SSelector(Types t, string n, long u) : base(t, u)
        {
            name = n;
        }
        public SSelector(Types t, string n, STransaction tr) : base(t, tr)
        {
            name = n;
        }
        public SSelector(SSelector s, string n) : base(s)
        {
            name = n;
        }
        protected SSelector(Types t, StreamBase f) : base(t, f)
        {
            name = f.GetString();
        }
        protected SSelector(SSelector s,AStream f) : base(s,f)
        {
            name = s.name;
            f.PutString(name);
        }
        public abstract SSelector Lookup(SQuery qry);
    }
    public class SColumn : SSelector
    {
        public readonly Types dataType;
        public readonly long table;
        /// <summary>
        /// For system or client column
        /// </summary>
        /// <param name="n"></param>
        /// <param name="t"></param>
        /// <param name="u"> will be negative</param>
        public SColumn(string n,Types t,long u) :base(Types.SColumn,n,u)
        {
            dataType = t; table = -1;
        }
        public SColumn(STransaction tr,string n, Types t, long tbl) 
            : base(Types.SColumn,n,tr)
        {
            dataType = t; table = tbl;
        }
        public SColumn(SColumn c,string n,Types d) : base(c,n)
        {
            dataType = d; table = c.table;
        }
        SColumn(StreamBase f) :base(Types.SColumn,f)
        {
            dataType = (Types)f.ReadByte();
            table = f.GetLong();
        }
        public SColumn(SColumn c,AStream f):base (c,f)
        {
            dataType = c.dataType;
            table = f.Fix(c.table);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
        }
        public new static SColumn Get(StreamBase f)
        {
            return new SColumn(f);
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
        public override SSelector Lookup(SQuery qry)
        {
            return qry.names.Lookup(name);
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
        public readonly string name;
        public readonly Types dataType;
        public SAlter(STransaction tr,string n,Types d,long o,long p) :base(Types.SAlter,tr)
        {
            defpos = o;  name = n; dataType = d; parent = p;
        }
        SAlter(StreamBase f):base(Types.SAlter,f)
        {
            defpos = f.GetLong();
            parent = f.GetLong(); //may be -1
            name = f.GetString();
            dataType = (Types)f.ReadByte();
        }
        public SAlter(SAlter a,AStream f):base(a,f)
        {
            name = a.name;
            dataType = a.dataType;
            defpos = f.Fix(a.defpos);
            parent = f.Fix(a.parent);
            f.PutLong(defpos);
            f.PutLong(parent);
            f.PutString(name);
            f.WriteByte((byte)dataType);
        }
        public new static SAlter Get(StreamBase f)
        {
            return new SAlter(f);
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
        SDrop(StreamBase f) :base(Types.SDrop,f)
        {
            drpos = f.GetLong();
            parent = f.GetLong();
        }
        public SDrop(SDrop d,AStream f):base(d,f)
        {
            drpos = f.Fix(d.drpos);
            parent = f.Fix(d.parent);
            f.PutLong(drpos);
            f.PutLong(parent);
        }
        public new static SDrop Get(StreamBase f)
        {
            return new SDrop(f);
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
        public readonly SList<SColumn> cols;
        public readonly string viewdef;
        public SView(STransaction tr,string n,SList<SColumn> c,string d) :base(Types.SView,tr)
        {
            name = n; cols = c; viewdef = d;
        }
        internal SView(SView v,SList<SColumn>c):base(v)
        {
            cols = c; name = v.name; viewdef = v.viewdef;
        }
        SView(SDatabase d, StreamBase f):base(Types.SView,f)
        {
            name = f.GetString();
            var n = f.GetInt();
            var c = SList<SColumn>.Empty;
            for (var i = 0; i < n; i++)
            {
                var nm = f.GetString();
                c = c.InsertAt(new SColumn(null, nm, (Types)f.ReadByte(), 0),i);
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
            f.PutInt(cols.Length);
            for (var b=cols.First();b!=null;b=b.Next())
            {
                f.PutString(b.Value.name);
                f.WriteByte((byte)b.Value.type);
            }
            f.PutString(viewdef);
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
        public readonly SDict<long, Serialisable> fields;
        public readonly long table;
        public SRecord(STransaction tr,long t,SDict<long,Serialisable> f) :base(Types.SRecord,tr)
        {
            fields = f;
            table = t;
        }
        public virtual long Defpos => uid;
        public SRecord(SDatabase db,SRecord r,AStream f) : base(r,f)
        {
            table = f.Fix(r.table);
            fields = r.fields;
            f.PutLong(table);
            var tb = (STable)db.Lookup(table);
            f.PutInt(r.fields.Count);
            for (var b=r.fields.First();b!=null;b=b.Next())
            {
                f.PutLong(b.Value.key);
                b.Value.val.Put(f);
            }
        }
        protected SRecord(SDatabase d, StreamBase f) : base(Types.SRecord,f)
        {
            table = f.GetLong();
            var n = f.GetInt();
            var tb = (STable)d.Lookup(table);
            var a = SDict<long, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = f.GetLong();
                a = a.Add(k, f._Get(d));
            }
            fields = a;
        }
        public static SRecord Get(SDatabase d, StreamBase f)
        {
            return new SRecord(d,f);
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append("(_id:");sb.Append(Defpos);
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(","); 
                sb.Append(b.Value.key); sb.Append(":");
                sb.Append(b.Value.val.ToString());
            }
            sb.Append(")");
        }
        public bool Matches(SDict<SSelector,Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (fields.Lookup(b.Value.key.uid).CompareTo(b.Value.val)!=0)
                    return false;
            return true;
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
            sb.Append(" for "); sb.Append(Uid());
            Append(sb);
            return sb.ToString();
        }
    }
    public class SUpdate : SRecord
    {
        public readonly long defpos;
        public SUpdate(STransaction tr,SRecord r,SDict<long,Serialisable>u) : base(tr,r.table,r.fields.Merge(u))
        {
            defpos = r.Defpos;
        }
        public override long Defpos => defpos;
        public SUpdate(SDatabase db,SUpdate r, AStream f) : base(db,r,f)
        {
            defpos = f.Fix(defpos);
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d, StreamBase f) : base(d,f)
        {
            defpos = f.GetLong();
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
            sb.Append(" for "); sb.Append(Uid());
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
        public SDelete(SDelete r, AStream f) : base(r,f)
        {
            table = f.Fix(r.table);
            delpos = f.Fix(r.delpos);
            f.PutLong(table);
            f.PutLong(delpos);
        }
        SDelete(StreamBase f) : base(Types.SDelete,f)
        {
            table = f.GetLong();
            delpos = f.GetLong();
        }
        public new static SDelete Get(StreamBase f)
        {
            return new SDelete(f);
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
        public readonly SMTree<long> rows;
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
            references = -1;
            rows = new SMTree<long>(Info((STable)tr.Lookup(table), cols));
        }
        SIndex(SDatabase d, StreamBase f) : base(Types.SIndex, f)
        {
            table = f.GetLong();
            primary = f.ReadByte()!=0;
            var n = f.GetInt();
            var c = new long[n];
            for (var i = 0; i < n; i++)
                c[i] = f.GetLong();
            references = f.GetLong();
            cols = SList<long>.New(c);
            rows = new SMTree<long>(Info((STable)d.Lookup(table), cols));
        }
        public SIndex(SIndex x, AStream f) : base(x, f)
        {
            table = f.Fix(x.table);
            f.PutLong(table);
            primary = x.primary;
            f.WriteByte((byte)(primary ? 1 : 0));
            long[] c = new long[x.cols.Length];
            f.PutInt(x.cols.Length);
            var i = 0;
            for (var b = x.cols.First(); b != null; b = b.Next())
            {
                c[i] = f.Fix(b.Value);
                f.PutLong(c[i++]);
            }
            references =f.Fix(x.references);
            f.PutLong(references);
            cols = SList<long>.New(c);
            rows = x.rows;
        }
        public SIndex(SIndex x,SMTree<long> nt) :base(x)
        {
            table = x.table;
            primary = x.primary;
            references = x.references;
            cols = x.cols;
            rows = nt;
        }
        public static SIndex Get(SDatabase d, StreamBase f)
        {
            return new SIndex(d, f);
        }
        public bool Contains(SRecord sr)
        {
            return rows.Contains(Key(sr, cols));
        }
        public SIndex Add(SRecord r,long c)
        {
            return new SIndex(this, rows.Add(Key(r, cols), c));
        }
        public SIndex Update(SRecord o,SUpdate u, long c)
        {
            return new SIndex(this, rows.Remove(Key(o,cols),c).Add(Key(u, cols), c));
        }
        public SIndex Remove(SRecord sr,long c)
        {
            return new SIndex(this, rows.Remove(Key(sr, cols),c));
        }
        SList<TreeInfo<long>> Info(STable tb, SList<long> cols)
        {
            if (cols.Length==0)
                return SList<TreeInfo<long>>.Empty;
            return Info(tb, cols.next).InsertAt(new TreeInfo<long>(tb.cols.Lookup(cols.element).uid, 'D', 'D'), 0);
        }
        SCList<Variant> Key(SRecord sr,SList<long> cols)
        {
            if (cols.Length == 0)
                return SCList<Variant>.Empty;
            return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element)), Key(sr, cols.next));
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
            StreamBase fs;
            public Buffer(StreamBase f)
            {
                buf = new byte[Size];
                pos = 0;
                len = Size;
                start = f.Length;
                fs = f;
            }
            internal Buffer(AStream f, long s)
            {
                buf = new byte[Size];
                start = s;
                pos = 0;
                f.GetBuf(this);
                fs = f;
            }
            internal int GetByte()
            {
                if (pos >= len)
                {
                    start += len;
                    pos = 0;
                    if (!fs.GetBuf(this))
                        return -1;
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
        protected Buffer rbuf,wbuf;
        protected StreamBase() { }
        protected abstract bool GetBuf(Buffer b);
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
                case Types.STable: s = STable.Get(this); break;
                case Types.SRow: s = SRow.Get(d, this); break;
                case Types.SColumn: s = SColumn.Get(this); break;
                case Types.SRecord: s = SRecord.Get(d, this); break;
                case Types.SUpdate: s = SUpdate.Get(d, this); break;
                case Types.SDelete: s = SDelete.Get(this); break;
                case Types.SAlter: s = SAlter.Get(this); break;
                case Types.SDrop: s = SDrop.Get(this); break;
                case Types.SIndex: s = SIndex.Get(d, this); break;
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
        long position = 0;
        public long length = 0;
        internal SDict<long, long> uids = null; // used for movement of SDbObjects
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            file.Seek(0, SeekOrigin.Begin);
        }
        public SDbObject GetOne(SDatabase d)
        {
            lock (file)
            {
                if (position == file.Length)
                    return null;
                rbuf = new Buffer(this, position);
                var r = _Get(d);
                position = rbuf.start + rbuf.pos;
                return (SDbObject)r;
            }
        }
        /// <summary>
        /// Called from Transaction.Commit(): file is already locked
        /// </summary>
        /// <param name="d"></param>
        /// <param name="pos"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public SDbObject[] GetAll(SDatabase d,long pos,long max)
        {
            var r = new List<SDbObject>();
            position = pos;
            rbuf = new Buffer(this, pos);
            while (position<max)
            {
                r.Add((SDbObject)_Get(d));
                position = rbuf.start + rbuf.pos;
            }
            return r.ToArray();
        }
        public class SysItem
        {
            public readonly Serialisable item;
            public readonly long next;
            internal SysItem(Serialisable i, long n)
            {
                item = i; next = n;
            }
        }
        public SysItem Get(SDatabase d, long pos) // we are already locked
        {
            if (pos == file.Length)
                return null;
            position = pos;
            rbuf = new Buffer(this, position);
            var r = _Get(d);
            return new SysItem(r, rbuf.start + rbuf.pos);
        }
        Serialisable Lookup(SDatabase db,long pos)
        {
            return db.Lookup(Fix(pos));
        }
        internal long Fix(long pos)
        {
            if (uids.Contains(pos))
                pos = uids.Lookup(pos);
            return pos;
        }
        public SDatabase Commit(SDatabase db,SDict<int,SDbObject> steps)
        {
            wbuf = new Buffer(this);
            uids = SDict<long, long>.Empty;
            for (var b=steps.First();b!=null; b=b.Next())
            {
                switch (b.Value.val.type)
                {
                    case Types.STable:
                        {
                            var st = (STable)b.Value.val;
                            var nt = new STable(st, this);
                            db = db._Add(nt,Length);
                            break;
                        }
                    case Types.SColumn:
                        {
                            var sc = (SColumn)b.Value.val;
                            var st = (STable)Lookup(db,Fix(sc.table));
                            var nc = new SColumn(sc, this);
                            db = db._Add(nc,Length);
                            break;
                        }
                    case Types.SRecord:
                        {
                            var sr = (SRecord)b.Value.val;
                            var st = (STable)Lookup(db,Fix(sr.table));
                            var nr = new SRecord(db, sr, this);
                            db = db._Add(nr,Length);
                            break;
                        }
                    case Types.SDelete:
                        {
                            var sd = (SDelete)b.Value.val;
                            var st = (STable)Lookup(db,Fix(sd.table));
                            var nd = new SDelete(sd, this);
                            db = db._Add(nd,Length);
                            break;
                        }
                    case Types.SUpdate:
                        {
                            var sr = (SUpdate)b.Value.val;
                            var st = (STable)Lookup(db, Fix(sr.table));
                            var nr = new SUpdate(db, sr, this);
                            db = db._Add(nr, Length);
                            break;
                        }
                    case Types.SAlter:
                        {
                            var sa = new SAlter((SAlter)b.Value.val,this);
                            db = db._Add(sa,Length);
                            break;
                        }
                    case Types.SDrop:
                        {
                            var sd = new SDrop((SDrop)b.Value.val, this);
                            db = db._Add(sd, Length);
                            break;
                        }
                    case Types.SIndex:
                        {
                            var si = new SIndex((SIndex)b.Value.val, this);
                            db = db._Add(si, Length);
                            break;
                        }
                }
            }
            Flush();
            SDatabase.Install(db);
            return db;
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

        protected override bool GetBuf(Buffer b)
        {
            if (b.start > length)
                return false;
            file.Seek(b.start, SeekOrigin.Begin);
            var n = length - b.start;
            if (n > Buffer.Size)
                n = Buffer.Size;
            b.len = file.Read(b.buf, 0, (int)n);
            return b.len>0;
        }

        protected override void PutBuf(Buffer b)
        {
            var p = file.Seek(0, SeekOrigin.End);
            file.Write(b.buf, 0, b.pos);
            file.Flush();
            length = p+b.pos;
            b.pos = 0;
        }
    }
}
