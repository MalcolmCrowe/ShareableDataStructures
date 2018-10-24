using System;
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
        public virtual Serialisable Commit(AStream f)
        {
            f.WriteByte((byte)type);
            return this;
        }
        public static Serialisable Get(AStream f)
        {
            return new Serialisable(Types.Serialisable);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        public override Serialisable Commit(AStream f)
        {
            base.Commit(f);
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
        SRow(AStream f) :base(Types.SRow)
        {
            var n = f.GetInt();
            var r = SDict<string, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetString();
                var v = f.GetOne();
                r = r.Add(k, v);
            }
            cols = r;
        }
        SRow(SRow s,AStream f) :base(Types.SRow)
        {
            var c = s.cols;
            f.PutInt(s.cols.Count);
            for (var b=s.cols.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                f.PutString(k);
                c.Add(k, f.Commit(b.Value.val)[0]);
            }
            cols = c;
        }
        public override Serialisable Commit(AStream f)
        {
            return new SRow(this,f);
        }
        public new static SRow Get(AStream f)
        {
            return new SRow(f);
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
    public class STable : Serialisable
    {
        public readonly string name;
        public readonly long uid;
        public readonly SList<SColumn> cols;
        public readonly SDict<long, SRecord> rows;
        public STable(Transaction tr,string n) :base(Types.STable)
        {
            name = n;
            uid = tr.Add(this);
            cols = SList<SColumn>.Empty;
            rows = SDict<long, SRecord>.Empty;
        }
        public STable InsertAt(SColumn c,int n)
        {
            return new STable(this,cols.InsertAt(c,n));
        }
        public STable UpdateAt(SColumn c, int n)
        {
            return new STable(this, cols.UpdateAt(c, n));
        }
        public STable Add(SRecord rec)
        {
            return new STable(this,rows.Add(rec.Defpos, rec));
        }
        public STable Remove(int n)
        {
            return new STable(this, cols.RemoveAt(n));
        }
        STable(STable t,SList<SColumn> c) :base(Types.STable)
        {
            name = t.name;
            uid = t.uid;
            cols = c;
            rows = t.rows;
        }
        STable(STable t,SDict<long,SRecord> r) : base(Types.STable)
        {
            name = t.name;
            uid = t.uid;
            cols = t.cols;
            rows = r;
        }
        STable(AStream f):base(Types.STable)
        {
            uid = f.Position;
            name = f.GetString();
            cols = SList<SColumn>.Empty;
            rows = SDict<long, SRecord>.Empty;
        }
        STable(STable t,AStream f) :base(Types.STable)
        {
            name = t.name;
            uid = f.Length;
            // if we already have columns, they need to be updated
            var nc = SList<SColumn>.Empty;
            for (var b = t.cols.First(); b != null; b = b.Next())
                nc = nc.InsertAt(new SColumn(b.Value, uid), nc.Length);
            cols = nc;
            // if we already have rows, they need to be updated
            var r = SDict<long, SRecord>.Empty;
            for (var b = t.rows.First(); b != null; b = b.Next())
                r = r.Add(b.Value.key, b.Value.val.FixTable(uid));
            rows = r;
        }
        /// <summary>
        /// Database objects should only be committed once.
        /// So we only commit a table when it is first mentioned.
        /// Its new columns get committed as they are defined.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override Serialisable Commit(AStream f)
        {
            if (Transaction.Committed(uid)) // nothing to do!
                return this;
            var r = new STable(this, f);
            base.Commit(f);
            f.PutString(name);
            return r;
        }
        public new static STable Get(AStream f)
        {
            return new STable(f);
        }
        public override string ToString()
        {
            return "Table "+name+"["+Transaction.Pos(uid)+"]";
        }
    }
    public class SColumn : Serialisable
    {
        public readonly string name;
        public readonly Types dataType;
        public readonly long uid;
        public readonly long table;
        public SColumn(Transaction tr,string n, Types t, long tbl) : base(Types.SColumn)
        {
            name = n; dataType = t; uid = tr.Add(this); table = tbl;
        }
        internal SColumn(SColumn c,long t) :base (Types.SColumn)
        {
            name = c.name; dataType = c.dataType; uid = c.uid;
            table = t;
        }
        SColumn(AStream f) :base(Types.SColumn)
        {
            uid = f.Position;
            name = f.GetString();
            dataType = (Types)f.ReadByte();
            table = f.GetLong();
        }
        SColumn(SColumn c,AStream f):base (Types.SColumn)
        {
            uid = f.Position;
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
        public override Serialisable Commit(AStream f)
        {
            if (Transaction.Committed(uid)) // nothing to do!
                return this;
            var r = new SColumn(this, f);
            base.Commit(f);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            return r;
        }
        public new static SColumn Get(AStream f)
        {
            return new SColumn(f);
        }
        public override string ToString()
        {
            return "Column " + name + " [" + Transaction.Pos(uid) + "]: " + dataType.ToString();
        }
    }
    public class SRecord : Serialisable
    {
        public readonly long uid;
        public readonly SDict<long, Serialisable> fields;
        public readonly long table;
        public SRecord(Transaction tr,long t,SDict<long,Serialisable> f) :base(Types.SRecord)
        {
            fields = f;
            table = t;
            uid = tr.Add(this);
        }
        public virtual long Defpos => uid;
        public virtual Serialisable Field(long col)
        {
            return fields.Lookup(col);
        }
        public virtual SRecord FixTable(long tbl)
        {
            return new SRecord(this, tbl);
        }
        protected SRecord(SRecord r,long tb) :base(Types.SRecord)
        {
            uid = r.uid;
            fields = r.fields;
            table = tb;
        }
        protected SRecord(SRecord r,AStream f) : base(Types.SRecord)
        {
            uid = f.Position;
            table = r.table;
            f.PutLong(table);
            var a = SDict<long, Serialisable>.Empty;
            f.PutInt(fields.Count);
            for (var b=fields.First();b!=null;b=b.Next())
            {
                var k = b.Value.key;
                f.PutLong(k);
                a = a.Add(k, b.Value.val.Commit(f));
            }
            fields = a;
        }
        protected SRecord(AStream f) : base(Types.SRecord)
        {
            uid = f.Position;
            table = f.GetLong();
            var n = f.GetInt();
            var a = SDict<long, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = f.GetLong();
                a = a.Add(k, f.GetOne());
            }
            fields = a;
        }
        public override Serialisable Commit(AStream f)
        {
            return new SRecord(this, f);
        }
        public new static SRecord Get(AStream f)
        {
            return new SRecord(f);
        }
        protected void Append(StringBuilder sb)
        {
            sb.Append(" for "); sb.Append(Transaction.Pos(table));
            var cm = "(";
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append("["); sb.Append(Transaction.Pos(b.Value.key)); sb.Append("]");
                sb.Append(b.Value.val.ToString());
            }
            sb.Append(")");
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Record ");
            sb.Append(Transaction.Pos(uid));
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
        public override SRecord FixTable(long tbl)
        {
            return new SUpdate(this,tbl);
        }
        SUpdate(SUpdate u,long tbl) :base(u,tbl)
        {
            defpos = u.defpos;
        }
        SUpdate(SUpdate r, AStream f) : base(r,f)
        {
            f.PutLong(defpos);
        }
        SUpdate(AStream f) : base(f)
        {
            defpos = f.GetLong();
        }
        public override Serialisable Commit(AStream f)
        {
            return new SUpdate(this, f);
        }
        public new static SRecord Get(AStream f)
        {
            return new SUpdate(f);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Update ");
            sb.Append(Transaction.Pos(uid));
            sb.Append(" of "); sb.Append(Transaction.Pos(defpos));
            Append(sb);
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
        FileStream file;
        long position = 0, length = 0;
        Buffer rbuf, wbuf;
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.Open,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            file.Seek(0, SeekOrigin.Begin);
        }
        public Serialisable[] Commit(params Serialisable[] obs)
        {
            lock (file)
            {
                wbuf = new Buffer(this);
                var r = new Serialisable[obs.Length];
                for (var i = 0; i < obs.Length; i++)
                    r[i] = obs[i].Commit(this);
                file.Seek(0, SeekOrigin.End);
                file.Write(wbuf.buf, 0, wbuf.pos);
                length += wbuf.pos;
                return r;
            }
        }
        public Serialisable GetOne()
        {
            lock (file)
            {
                if (position == file.Length)
                    return null;
                var r = _Get(position);
                position = rbuf.start + rbuf.pos;
                return r;
            }
        }
        Serialisable _Get(long pos)
        {
            rbuf = new Buffer(this, position);
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
                case Types.SRow: s = SRow.Get(this); break;
                case Types.SColumn: s = SColumn.Get(this); break;
                case Types.SRecord: s = SRecord.Get(this); break;
                case Types.SUpdate: s = SUpdate.Get(this); break;
            }
            return s;
        }
        public Serialisable Get(long pos)
        {
            lock (file)
            {
                return _Get(pos);
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
