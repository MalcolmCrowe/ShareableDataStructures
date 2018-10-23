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
    public class STable : Serialisable
    {
        static long _uid = 0x80000000;
        public readonly string name;
        public readonly long uid;
        public STable(string n) :base(Types.STable)
        {
            name = n;
            uid = ++_uid;
        }
        STable(AStream f):base(Types.STable)
        {
            uid = f.Position-1;
            name = f.GetString();
        }
        STable(STable t,AStream f) :base(Types.STable)
        {
            name = t.name;
            uid = f.Position;
        }
        public override Serialisable Commit(AStream f)
        {
            var r = new STable(this, f);
            base.Commit(f);
            f.PutString(name);
            return r;
        }
        public new static STable Get(AStream f)
        {
            return new STable(f);
        }
        string Pos()
        {
            if (uid > 0x80000000)
                return "'" + (uid - 0x80000000);
            return "" + uid;
        }
        public override string ToString()
        {
            return "Table "+name+"["+Pos()+"]";
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
            public byte[] buf;
            public readonly long start;
            public readonly int len;
            public int pos;
            public Buffer(AStream f)
            {
                buf = new byte[2048];
                pos = 0;
                len = 2048;
                start = f.Length;
            }
            internal Buffer(AStream f, long s)
            {
                buf = new byte[2048];
                start = s;
                pos = 0;
                f.file.Seek(start, SeekOrigin.Begin);
                len = f.file.Read(buf, 0, buf.Length);
            }
            internal int GetByte()
            {
                if (pos >= len)
                    return -1;
                return buf[pos++];
            }
            internal void PutByte(byte b)
            {
                if (pos >= len)
                    throw new Exception("Buffer overrun");
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
        public Serialisable Create()
        {
            if (position == file.Length)
                return null;
            lock (file)
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
                }
                position = rbuf.start + rbuf.pos;
                return s;
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

        public override long Length => length;

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
