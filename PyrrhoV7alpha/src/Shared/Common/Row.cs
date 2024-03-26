using System.Collections.Specialized;
using System.ComponentModel;
using System.Data.Common;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Xml;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Common
{
    /// <summary>
    /// Data values and local variables can be of structured types 
    /// (row, array, multiset, user-defined, documents). 
    /// In normal use such complex obs can be in a class of TypedValue 
    /// (containing other TypedValues), and can be referred to by a single SqlValue. 
    /// But if during analysis we find we are dealing with SQL references to its internal contents, 
    /// it is built into a structured SqlValue (SqlRow, SqlArray etc) 
    /// whose referenceable elements are SqlValues. 
    /// Evaluation will restore the simple TypedValue structure.
    /// As of 26 April 2021, all TypedValue classes are shareable
    /// </summary>
    public abstract class TypedValue : IComparable
    {
        internal readonly Domain dataType = Domain.Null;
        internal TypedValue(Domain t)
        {
            dataType = t;
        }
        internal virtual TypedValue Next()
        {
            throw new DBException("22009",dataType.kind.ToString());
        }
        internal virtual byte BsonType()
        {
            return dataType.BsonType();
        }
        internal virtual TypedValue? this[string n]
        {
            get { return null; }
            set { }
        }
        internal virtual TypedValue? this[long n]
        {
            get { return null; }
            set { }
        }
        internal TypedValue? this[Ident n] { get { return this[n.ident]; } }
        internal virtual bool? ToBool()
        {
            return null;
        }
        internal virtual int? ToInt()
        {
            return null;
        }
        internal virtual long? ToLong()
        {
            return null;
        }
        internal virtual Integer? ToInteger()
        {
            return null;
        }
        internal virtual double ToDouble()
        {
            return double.NaN;
        }
        internal virtual TypedValue[]? ToArray()
        {
            var r = new TypedValue[1];
            r[0] = this;
            return r;
        }
        internal virtual int Cardinality()
        {
            return 1;
        }
        internal virtual bool Contains(TypedValue e)
        { return false; }
        internal virtual TypedValue Fix(Context cx)
        {
            var dm = (Domain)dataType.Fix(cx);
            return (dm==dataType)?this:dm.Coerce(cx,this);
        }
        internal virtual TypedValue Replaced(Context cx)
        {
            return dataType.Replaced(cx).Coerce(cx,this);
        }
        internal virtual TypedValue Replace(Context cx,DBObject so,DBObject sv)
        {
            return Replaced(cx);
        }
        internal Domain _DataType()
        {
            var dt = dataType;
            if (dt.kind == Sqlx.UNION)
                for (var b = dt.mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain d && d.CanBeAssigned(this))
                        return d;
            return dt;
        }
        internal virtual string ToString(Context cx)
        {
            return ToString();
        }
        public override string ToString()
        {
            return "TypedValue";
        }
        internal virtual string ToString(BList<long?> cs, Context cx)
        {
            return ToString();
        }
        public virtual int CompareTo(object? obj)
        {
            return dataType.Compare(this,obj as TypedValue??TNull.Value);
        }
        internal virtual TypedValue ShallowReplace(Context cx,long was, long now)
        {
            var dm = (Domain)dataType.ShallowReplace(cx,was,now);
            return (dm.dbg != dataType.dbg) ? dm.Coerce(cx,this) : this;
        }
    }
    internal class TNull : TypedValue
    {
        static TNull _Value = new();
        TNull() : base(Domain.Null) { _Value = this; }
        internal static TNull Value => (_Value is null||_Value.dataType is null)?new():_Value;
        public override string ToString()
        {
            if (PyrrhoStart.VerboseMode)
                return base.ToString() + " Null";
            return " Null";
        }
        internal override int Cardinality()
        {
            return 0;
        }
        public override int CompareTo(object? obj)
        {
            if (obj == null || obj is TNull)
                return 0;
            var that = (TypedValue)obj;
            if (that.dataType.nulls == Sqlx.FIRST)
                return 1;
            return -1;
        }
    }
    // shareable
    internal class TInt : TypedValue
    {
        internal readonly long value; // should be 0L for TInteger subclass
        internal TInt(Domain dt, long v) : base(dt.Best(Domain.Int)) { value = v; }
        internal TInt(long v) : this(Domain.Int, v) { }
        internal override TypedValue Next()
        {
            if (value == long.MaxValue)
                return new TInteger(new Integer(value)).Next();
            return base.Next();
        }
        internal override double ToDouble()
        {
            return value * 1.0;
        }
        public override string ToString()
        {
            return value.ToString();
        }
        internal override byte BsonType()
        {
            if (value > int.MinValue && value < int.MaxValue)
                return 16;
            return 18;
        }
        internal override int? ToInt()
        {
            if (value>int.MaxValue || value<int.MinValue)
                return null;
            return (int)value;
        }
        internal override long? ToLong()
        {
            return value;
        }
        internal override Integer? ToInteger()
        {
            return new Integer(value);
        }
    }
    // shareable
    internal class TPosition : TInt
    {
        internal TPosition(long p) : base(Domain.Position, p) { }
        public override string ToString()
        {
            if (value<0)
                return "";
            return DBObject.Uid(value);
        }
    }
    // shareable
    internal class TInteger : TInt
    {
        internal readonly Integer ivalue;
        static readonly Integer longMax = new (long.MaxValue), longMin = new (long.MinValue);
        internal TInteger(Domain dt, Integer i) : base(dt.Best(Domain.Int),0L) { ivalue = i; }
        internal TInteger(Integer i) : this(Domain.Int, i) { }
        internal override TypedValue Next()
        {
            return new TInteger(ivalue.Add(new Integer(1),0));
        }
        internal override byte BsonType()
        {
            if (ivalue?.BitsNeeded() > 64)
                return 19; // Decimal subtype added for Pyrrho
            return base.BsonType();
        }
        internal override int? ToInt()
        {
            if (ivalue < int.MaxValue && ivalue > int.MinValue)
                return (int)ivalue;
            return null;
        }
        internal override long? ToLong()
        {
            if (ivalue < longMax && ivalue > longMin)
                return (long)ivalue;
            return null;
        }
        internal override Integer? ToInteger()
        {
            return ivalue;
        }
        internal override double ToDouble()
        {
            return (double)ivalue;
        }
        public override string ToString()
        {
            return ivalue.ToString();
        }
    }
    // shareable
    internal class TBool : TypedValue
    {
        internal readonly bool? value;
        internal static TBool False = new (false);
        internal static TBool True = new (true);
        internal static TBool Unknown = new(null);
        private TBool(Domain dt, bool? b) : base(dt) { value = b; }
        private TBool(bool? b) : this(Domain.Bool, b) { }
        public override string ToString()
        {
            return value?.ToString()??"Unknown";
        }
        internal override bool? ToBool()
        {
            return value;
        }
        internal static TypedValue For(bool? p)
        {
            return (p ==true)?True : (p==false)? False : Unknown;
        }
    }
    // shareable
    internal class TChar : TypedValue
    {
        internal readonly string value;
        internal static TChar Empty = new ("");
        internal TChar(Domain dt, string s) : base(dt) { value = s; }
        internal TChar(string s) : this(Domain.Char, s) { }
        public override string ToString()
        {
            return value??"null";
        }
        internal override string ToString(BList<long?> cs,Context cx)
        {
            return "'" + value + "'";
        }
    }
    // shareable
    internal class TNumeric : TypedValue
    {
        internal readonly Numeric value;
        internal TNumeric(Domain dt, Numeric n) : base(dt.Best(Domain._Numeric)) { value = n; }
        internal TNumeric(Numeric n) : this(Domain._Numeric, n) { }
        internal override TypedValue Next()
        {
            return new TNumeric(dataType,new Numeric(value.mantissa.Add(new Integer(1),value.scale)));
        }
        internal override double ToDouble()
        {
            return (double)value;
        }
        public override string ToString()
        {
            return value.ToString();
        }
        internal override int? ToInt()
        {
            int r = 0;
            if (value.TryConvert(ref r))
                return r;
            return base.ToInt();
        }
        internal override long? ToLong()
        {
            long r = 0;
            if (value.TryConvert(ref r))
                return r; 
            return base.ToLong();
        }
        internal override Integer? ToInteger()
        {
            Integer r = new (0);
            if (value.TryConvert(ref r))
                return r; 
            return base.ToInteger();
        }
    }
    // shareable
    internal class TReal : TypedValue
    {
        internal readonly double dvalue = double.NaN;
        internal readonly Numeric nvalue = new (0);
        internal TReal(Domain dt, double d) : base(dt) { dvalue = d; }
        internal TReal(Domain dt, Numeric n) : base(dt) { nvalue = n; }
        internal TReal(double d) : this(Domain.Real, d) { }
        internal TReal(Numeric n) : this(Domain.Real, n) { }
        TReal(Domain dt, double d, Numeric n) : base(dt)
        {
            dvalue = d; nvalue = n;
        }
        internal override TypedValue Next()
        {
            if (!double.IsNaN(dvalue))
                return new TReal(dataType, dvalue + 1);
            return new TReal(dataType, new Numeric(nvalue.mantissa.Add(new Integer(1), 0), 0));
        }
        public override string ToString()
        {
            if (!double.IsNaN(dvalue))
                return dvalue.ToString();
            return nvalue.ToString();
        }
        internal override double ToDouble()
        {
            if (double.IsNaN(dvalue))
                return (double)nvalue;
            return dvalue;
        }
    }

    
    internal class TSensitive : TypedValue
    {
        internal readonly TypedValue value;
        internal TSensitive(Domain dt, TypedValue v) : base(dt)
        {
            value = (v is TSensitive st) ? st.value : v;
        }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    // shareable
    internal class TSubType : TypedValue
    {
        internal readonly TypedValue value; // of the supertype
        internal TSubType(UDType dt, TypedValue v) : base(dt)
        {
            value = v;
        }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    // shareable
    internal class TQParam : TypedValue
    {
        internal readonly Iix qid;
        public TQParam(Domain dt,Iix id) :base(dt) { qid = id; }
        internal override TypedValue Fix(Context cx)
        {
            var id = cx.Fix(qid);
            if (id==qid)
                return base.Fix(cx);
            return new TQParam((Domain)dataType.Fix(cx), id);
        }
        public override string ToString()
        {
            return "?" + DBObject.Uid(qid.dp);
        }
    }
    
    internal class TUnion : TypedValue 
    {
        internal readonly TypedValue value = TNull.Value;
        internal TUnion(Domain dt, TypedValue v) : base(dt) { value = v;  }
        internal override TypedValue Fix(Context cx)
        {
            return new TUnion((Domain)dataType.Fix(cx), 
                value.Fix(cx));
        }
        internal override int? ToInt()
        {
            return value.ToInt();
        }
        internal override long? ToLong()
        {
            return value.ToLong();
        }
        internal TypedValue LimitToValue(Context cx,long lp)
        {
            var ts = new List<Domain>();
            for (var b = dataType.unionOf.First(); b != null; b = b.Next())
            {
                var dt = b.key();
                if (dt.HasValue(cx,value))
                    ts.Add(dt);
            }
            if (ts.Count == 0)
                throw new DBException("22000",dataType);
            if (ts.Count == 1)
                return ts[0].Coerce(cx,value);
            return new TUnion(Domain.UnionType(lp,ts.ToArray()),value);
        }
    }
    // shareable
    internal class TDateTime : TypedValue
    {
        internal readonly DateTime value;
        internal TDateTime(Domain dt, DateTime d) : base(dt) { value = d; }
        internal TDateTime(DateTime d) : this(Domain.Timestamp, d) { }
        public override string ToString()
        {
            return value.ToString(Thread.CurrentThread.CurrentUICulture);
        }
        internal override long? ToLong()
        {
            return value.Ticks;
        }
    }
  
    // shareable
    internal class THttpDate : TDateTime
    {
        internal readonly bool milli;
        internal THttpDate(DateTime d,bool m=false) : base(Domain.HttpDate, d)
        {
            milli = m;
        }
        enum DayOfWeek { Sun=0,Mon=1,Tue=2,Wed=3,Thu=4,Fri=5,Sat=6}
        enum Month { Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12}
        static int  White(char[] a,int i)
        {
            // - is white space in RFC 850
            // also treat , as white
            while (i < a.Length && 
                (char.IsWhiteSpace(a[i])||a[i]=='-'||a[i]==','))
                i++;
            return i;
        }
        static (int,int) Digits(string s,char[] a, int i)
        {
            int n;
            for (n = 0; i < a.Length && char.IsDigit(a[i + n]); n++)
                ;
            if (n == 0)
                return (0, i);
            var r = int.Parse(s.Substring(i, n));
            return (r,i+n);
        }
        /// <summary>
        /// Parse a UTC representation according to RFC 7231
        /// </summary>
        /// <param name="r">The string representation</param>
        /// <returns>A THttpDate</returns>
        internal static THttpDate? Parse(string r)
        {
            if (r == null)
                return null;
            // example formats from RFC 7231
            // Sun, 06 Nov 1994 08:49:37 GMT
            // Sunday, 06-Nov-94 08:49:37 GMT
            // Sun Nov  6 08:49:37 1994 
            // allowed by Pyrrho
            // Sun, 06 Nov 1994 08:49:37.123 GMT
            var a = r.ToCharArray();
            var i = 0;
            int M = 1;
            int h = 0;
            int m = 0;
            int s = 0;
            int f = 0;
            int d, y;
            bool milli = false;
            i = White(a, i);
            for (var j = 0; j <= 6; j++)
                if (r.Substring(i, 3) == ((DayOfWeek)j).ToString())
                {
                    i += 3;
                    break;
                }
            i = White(a, i);
            // 06 Nov 1994 08:49:37 GMT
            // 06-Nov-94 08:49:37 GMT
            // Nov  6 08:49:37 1994 
            // 06 Nov 1994 08:49:37.123 GMT
            (d,i) = Digits(r, a, i);
            i = White(a, i);
            // Nov 1994 08:49:37 GMT
            // Nov-94 08:49:37 GMT
            // Nov  6 08:49:37 1994 
            // Nov 1994 08:49:37.123 GMT
            for (var j = 1; j <= 12; j++)
                if (r.Substring(i, 3) == ((Month)j).ToString())
                {
                    i += 3;
                    M = j;
                    break;
                }
            i = White(a, i);
            // 1994 08:49:37 GMT
            // 94 08:49:37 GMT
            // 6 08:49:37 1994 
            // 1994 08:49:37.123 GMT
            (y, i) = Digits(r, a, i);
            if (d == 0)
            {
                d = y;
                y = 0;
            }
            else if (y < 100)
                y += 1900;
            i = White(a, i);
            // 08:49:37 GMT
            // 08:49:37 GMT
            // 08:49:37 1994 
            // 08:49:37.123 GMT
            if (r[i + 2] == ':')
            {
                h = int.Parse(r.Substring(i, 2));
                m = int.Parse(r.Substring(i+3, 2));
                s = int.Parse(r.Substring(i+6, 2));
                i += 8;
            }
            //  GMT
            //  GMT
            //  1994 
            // .123 GMT
            if (r[i] == '.')
            {
                f = int.Parse(r.Substring(i + 1, 3));
                milli = true;
                i += 4;
            }
            i = White(a, i);
            // GMT
            // GMT
            // 1994 
            // GMT
            if (y == 0)
                (y, _) = Digits(r, a, i);
            var dt = new DateTime(y, M, d, h, m, s);
            if (f != 0)
                dt = new DateTime(dt.Ticks + f*10000);
            return new THttpDate(dt,milli);
        }
        /// <summary>
        /// RFC 7231 representation of a timestamp
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            var d = value;
            sb.Append((DayOfWeek)(int)d.DayOfWeek);
            sb.Append(", "); sb.Append(d.Day.ToString("D2"));
            sb.Append(' '); sb.Append((Month)d.Month);
            sb.Append(' '); sb.Append(d.Year);
            sb.Append(' '); sb.Append(d.Hour.ToString("D2"));
            sb.Append(':'); sb.Append(d.Minute.ToString("D2"));
            sb.Append(':'); sb.Append(d.Second.ToString("D2"));
            if (milli)
            {
                var t = d.Ticks
                    -new DateTime(d.Year, d.Month, d.Day, d.Hour, d.Minute, d.Second).Ticks;
                t /= 10000;
                sb.Append('.'); sb.Append(t.ToString("D3")); 
            }
            sb.Append(" GMT");
            return sb.ToString();
        }
    }
    // shareable
    internal class TInterval : TypedValue
    {
        internal readonly Interval value;
        internal TInterval(Domain dt, Interval i) : base(dt) { value = i; }
        internal TInterval(Interval i) : this(Domain.Interval, i) { }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    // shareable
    internal class TTimeSpan : TypedValue
    {
        internal readonly TimeSpan value;
        internal TTimeSpan(Domain dt, TimeSpan t) : base(dt) { value = t; }
        internal TTimeSpan(TimeSpan t) : this(Domain.Timespan, t) { }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    /// <summary>
    /// This is really part of the implementation of multi-level indexes (MTree etc)
    ///     
    /// </summary>
    internal class TMTree: TypedValue
    {
        internal MTree value;
        internal TMTree(MTree m) : base(Domain.MTree) { value = m; }
        internal override int Cardinality()
        {
            return (int)value.count;
        }
    }
    /// <summary>
    /// This is also part of the implementation of multi-level indexes (MTree etc)
    /// </summary>
    internal class TPartial : TypedValue
    {
        internal CTree<long, bool> value;
        internal TPartial(CTree<long, bool> t) : base(Domain.Partial) { value = t; }
        internal override int Cardinality()
        {
            return (int)value.Count;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TPartial(cx.FixTlb(value));
        }
    }
    
    internal class TList : TypedValue
    {
        internal readonly BList<TypedValue> list; 
        internal TList(Domain dt, params TypedValue[] a) 
            : base(new Domain(-1L,Sqlx.ARRAY,dt)) 
        { 
            var ts = BList<TypedValue>.Empty;
            foreach (var x in a)
                ts += x;
            list = ts;
        }
        internal TList(Domain dt, BList<TypedValue> a) : base(dt) { list = a; }
        internal override TypedValue Fix(Context cx)
        {
            return new TList((Domain)dataType.Fix(cx),
                cx.FixBV(list));
        }
        public static TList operator+(TList ar,TypedValue v)
        {
            if (ar.dataType.elType is null || !ar.dataType.elType.CanTakeValueOf(v.dataType))
                throw new DBException("22005", ar.dataType.elType??Domain.Null, v.dataType);
            return new TList(ar.dataType, ar.list + v);
        }
        public static TList operator-(TList ls,int k)
        {
            return new TList(ls.dataType, ls.list - k);
        }
        internal override int Cardinality()
        {
            return Length;
        }
        internal override bool Contains(TypedValue e)
        {
            if (dataType.elType is null || !dataType.elType.CanTakeValueOf(e.dataType))
                throw new DBException("22005", dataType.elType??Domain.Null, e.dataType);
            for (var b = list.First(); b != null; b = b.Next())
                if (b.value().CompareTo(e) == 0)
                    return true;
            return false;
        }
        internal int Length { get { return (int)list.Count; } }
        internal override TypedValue? this[string n]
        {
            get
            {
                if (int.TryParse(n, out int i))
                    return this[i];
                return null;
            }
        }
        internal TypedValue this[int i] => list.PositionAt(i)?.value()??TNull.Value;
        public override int CompareTo(object? obj)
        {
            var that = obj as TList;
            if (that is null)
                return 1;
            var tb = that.list.First();
            var b = list.First();
            for (;  b is not null && tb is not null; b = b.Next(), tb = tb.Next())
            {
                var c = b.value().CompareTo(tb.value());
                if (c!=0) return c;
            }
            if (b is null) return -1;
            if (tb is null) return 1;
            return 0;
        }
        internal override string ToString(Context cx)
        {
            var sb = new StringBuilder("[");
            var cm = "";
            for (var b = list.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value().ToString(cx));
            }
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var cm = "";
            if (list != null)
                for(var b=list.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value().ToString());
                }
            sb.Append(']');
            return sb.ToString();
        }
    }
    internal class TArray : TypedValue
    {
        internal readonly CTree<int,TypedValue> array;
        internal TArray(Domain dt)
            : base(new Domain(-1L, Sqlx.ARRAY, dt))
        {
            array = CTree<int, TypedValue>.Empty;
        }
        internal TArray(Domain dt, CTree<int, TypedValue> a) : base(dt) { array = a; }
        internal override TypedValue Fix(Context cx)
        {
            return new TArray((Domain)dataType.Fix(cx),
                cx.FixTiV(array));
        }
        public static TArray operator +(TArray ar, (int,TypedValue) x)
        {
            var (i, v) = x;
            if (ar.dataType.elType is null || !ar.dataType.elType.CanTakeValueOf(v.dataType))
                throw new DBException("22005", ar.dataType.elType ?? Domain.Null, v.dataType);
            return new TArray(ar.dataType, ar.array + (i,v));
        }
        internal override int Cardinality()
        {
            return Length;
        }
        internal virtual TypedValue? this[int n] => array[n];
        internal override bool Contains(TypedValue e)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is TypedValue f && e.dataType.CompareTo(f)==0)
                    return true;
            return false;
        }
        /// <summary>
        /// Hmm...
        /// </summary>
        internal int Length { get { return (int)array.Count; } }
        public override string ToString()
        {
            var sb = new StringBuilder("(");
            var cm = "";
            if (array != null)
                for (var b = array.First(); b is not null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append('='); sb.Append(b.value().ToString());
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    internal class TTypeSpec : TypedValue
    {
        internal readonly Domain _dataType;
        internal TTypeSpec(Domain t) : base(Domain.TypeSpec)
        {
            _dataType = t;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TTypeSpec((Domain)_dataType.Fix(cx));
        }
        public override string ToString()
        {
            return DBObject.Uid(_dataType.defpos)+" "+_dataType.name;
        }
    }
    internal class TLevel : TypedValue
    {
        internal readonly Level val;
        public static TLevel D = new (Level.D);
        TLevel(Level v) : base(Domain._Level) { val = v; }
        public static TLevel New(Level lv)
        {
            if (lv.Equals(Level.D))
                return D;
            return new TLevel(lv);
        }
        public override string ToString()
        {
            return val.ToString();
        }
    }
    // shareable
    internal class TBlob : TypedValue
    {
        internal readonly byte[] value;
        public int Length => value.Length;
        public byte this[int i] => value[i];
        internal TBlob(Domain dt, byte[] b) : base(dt) { value = b; }
        internal TBlob(byte[] b) : this(Domain.Blob, b) { }

        public override string ToString()
        {
            var sb = new StringBuilder("byte[");
            if (value is not null)
            sb.Append(value.Length);
            sb.Append(']');
            if (value is not null && value.Length>0)
            {
                sb.Append("X'");
                for(int i=0;i<value.Length;i++)
                {
                    sb.Append(value[i].ToString("x2"));
                    if (i>20)
                    {
                        sb.Append("..");
                        break;
                    }
                }
                sb.Append('\'');
            }
            return sb.ToString();
        }
    }
    
    internal class TPeriod : TypedValue
    {
        internal readonly Period value;
        internal TPeriod(Domain dt, Period p) : base(dt) { value = p; }
    }
    /// <summary>
    /// A row-version cookie
    /// </summary>
    internal class TRvv : TypedValue
    {
        internal readonly Rvv rvv = Rvv.Empty;
        internal TRvv(string match) : base (Domain._Rvv)
        {
            rvv = Rvv.Parse(match)??Rvv.Empty;
        }
        internal TRvv(Rvv r) : base(Domain._Rvv)
        {
            rvv = r;
        }
        /// <summary>
        /// Remote data may contain extra columns for Rvv info:
        /// if not, use -1 default indicating no information (disallow updates)
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="vs"></param>
        internal TRvv(Context cx, CTree<long, TypedValue> vs) : base(Domain._Rvv)
        {
            var r = Rvv.Empty;
            var dp = vs[DBObject.Defpos]?.ToLong() ?? -1L;
            var pp = vs[DBObject.LastChange]?.ToLong() ?? -1L;
            if (dp >= 0 && pp >= 0)
                r += (cx.result, (dp, pp));
            rvv = r;
        }
        public override string ToString()
        {
            return rvv.ToString();
        }
    }
    // shareable: no mutators
    internal sealed class TMetadata : TypedValue
    {
        readonly CTree<Sqlx, TypedValue> md;
        public TMetadata(CTree<Sqlx,TypedValue>? m=null) : base(Domain.Metadata)
        {
            md = m ?? CTree<Sqlx,TypedValue>.Empty;
        }
        public override string ToString()
        {
            if (md == CTree<Sqlx, TypedValue>.Empty)
                return "";
            var sb = new StringBuilder();
            var cm = '{';
            for (var b=md.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.key()); sb.Append(':');
                sb.Append(b.value());
            }
            sb.Append('}');
            return sb.ToString();
        }
    }
     /// <summary>
    /// All TypedValues have a domain; TRow also has a BList of uids to give the columns ordering
    /// and a tree of values indexed by uid.
    /// Cursor and RowBookmark are TRows, and rows can be assigned to SqlValues if the columns
    /// match (not the uids).
    /// If the columns don't match then a map is required.
    ///     
    /// </summary>
    internal class TRow : TypedValue
    {
        internal readonly CTree<long, TypedValue> values;
        internal readonly int docCol = -1;  // note the position of a single document column if present
        internal int Length => dataType.Length;
        internal BList<long?> columns => dataType.rowType;
        internal static TRow Empty = new (Domain.Row, CTree<long, TypedValue>.Empty);
        public TRow(Domain dt,CTree<long,TypedValue> vs)
            :base(dt)
        {
            values = vs;
        }
        public TRow(Domain dt, BTree<long, long?>? map, BTree<long, TypedValue> vs)
            : base(dt)
        {
            var v = CTree<long, TypedValue>.Empty;
            for (var b = map?.First(); b != null; b = b.Next())
                if (dt.representation.Contains(b.key()) && b.value() is long p)
                    v += (b.key(), vs[p] ?? TNull.Value);
            values = v;
        }
        public TRow(RowSet rs, Domain dm, TRow rw) : base((dm.Length==0)?rs:dm)
        {
            var vs = CTree<long, TypedValue>.Empty;
            if (dm.Length == 0)
                vs = rw.values;
            else
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        var v = rw[p];
                        if (v == null)
                            for (var c = rs.matching[p]?.First(); c != null && v == null; c = c.Next())
                                v = rw[c.key()];
                        vs += (p, v ?? TNull.Value);
                    }
            values = vs;
        }
        public TRow(Domain dm, Domain cols, TRow rw) : base(dm)
        {
            var vs = CTree<long, TypedValue>.Empty;
            var rb = rw.columns.First();
            var d = (cols == Domain.Row) ? dm : cols;
            for (var b = d.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                if (b.value() is long k && rb.value() is long c)
                    vs += (k, rw[c] ?? TNull.Value);
            values = vs;
        }
        public TRow(TRow rw, Domain dm) : base(dm)
        {
            var v = CTree<long, TypedValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long k)
                    v += (k, rw[b.key()] ?? TNull.Value);
            values = v;
        }
        /// <summary>
        /// Constructor: values by columns
        /// </summary>
         /// <param name="v">The values</param>
        public TRow(Domain dt, params TypedValue[] v) : 
            base(dt)
        {
            var vals = CTree<long, TypedValue>.Empty;
            var i = 0;
            for (var b = dt.rowType.First(); b != null; b = b.Next(), i++)
                if (b.value() is long p)
                {
                    var vi = (i < v.Length) ? v[i] : null;
                    if (vi != null)
                        vals += (p, vi);
                    else if (dt.representation[p] is Domain dd)
                        vals += (p, dd.defaultValue);
                }
            values = vals;
        }
        internal TRow(Domain dm, CList<TypedValue> v) : base(dm)
        {
            var vals = CTree<long, TypedValue>.Empty;
            var tb = v.First();
            for (var b = dm.rowType.First(); b != null && tb is not null; b = b.Next(), tb=tb.Next())
            if (b.value() is long p)
                    vals += (p, tb.value());
            values = vals;
        }
        internal bool IsNull
        {
            get
            {
                var d = dataType.display;
                if (d == 0)
                    d = int.MaxValue;
                for (var b = dataType.rowType.First(); b != null && d>0; b = b.Next(),d--)
                    if (b.value() is long p && values[p] != TNull.Value)
                        return false;
                return true;
            }
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TRow((Domain)dataType.Fix(cx),cx.FixTlV(values));
        }
        public static TRow operator+(TRow rw,(long,TypedValue)x)
        {
            return new TRow(rw.dataType, rw.values + x);
        }
        internal override TypedValue this[long n] => values[n]??TNull.Value;
        internal virtual TypedValue this[int i]
        {
            get {
                if (columns != null && columns[i] is long p)
                    return values[p]??TNull.Value;
                var j = 1;
                for (var b = dataType.rowType.First(); i >= j && b != null; b = b.Next(), j++)
                    if (i == j)
                        return values[b.value()??-1L]??TNull.Value;
                return TNull.Value;
            }
        }
        internal override TypedValue Replace(Context cx, DBObject ov, DBObject nv)
        {
            var dt = (Domain)dataType.Replace(cx, ov, nv);
            var vs = CTree<long,TypedValue>.Empty;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && values[p] is TypedValue v)
                {
                    if (p == ov.defpos)
                        p = nv.defpos;
                    vs += (p, v);
                }
            return new TRow(dt, vs);
        }
        /// <summary>
        /// Make a readable representation of the Row
        /// </summary>
        /// <returns>the representation</returns>
        public override string ToString()
        {
            var str = new StringBuilder();
            var cm = '[';
            for (var b = columns.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    str.Append(cm); cm = ',';
                    str.Append(DBObject.Uid(p)); str.Append('=');
                    str.Append(values[p] ?? TNull.Value);
                }
            if (cm!='[')
                str.Append(']');
            return str.ToString();
        }
        internal override TypedValue[] ToArray()
        {
            var r = new TypedValue[Length];
            for (int i = 0; i < Length; i++)
                r[i] = this[i];
            return r;
        }
        internal CList<TypedValue> ToKey()
        {
            var r = CList<TypedValue>.Empty;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    r += values[p] ?? TNull.Value;
            return r;
        }
    }
    // we implement a TPath as a TRow: its row type is
    // [0=NodeType Array, {uid=TypedValue} ]
    // where uid identifies an unbound identifier X:T in the Match Statement
    // and the TypedValue is T or TArray<T> if X is in a repeating path segment
    // During evaluation of the Match, the TPatch starts off empty and is
    // populated as the path is traversed
    internal class TPath : TRow
    {
        internal readonly long matchAlt; 
        internal TPath(long dp,Context cx) : base(new Domain(-1L,cx,Sqlx.ROW,BList<DBObject>.Empty,0), 
            new CTree<long, TypedValue>(0, new TArray(new Domain(-1L,Sqlx.ARRAY,Domain.NodeType),CTree<int,TypedValue>.Empty))) 
        {
            matchAlt = dp;
        }
        internal TPath(long dp, Domain dt,CTree<long,TypedValue> vs) : base(dt, vs) 
        {
            matchAlt = dp;
        }
        public static TPath operator+(TPath p,TNode n)
        {
            var a = (TArray)p[0L];
            return new TPath(p.matchAlt,p.dataType,p.values + (0L, a+(a.Length,n)));
        }
        public static TPath operator+(TPath p,(Context, long,TypedValue)x)
        {
            var (cx, k, v) = x;
            var dt = p.dataType;
            if (dt.representation.Contains(k))
                return new TPath(p.matchAlt,dt,p.values+(k,v));
            return new TPath(p.matchAlt,
                (Domain)cx.Add(new Domain(dt.defpos,cx,Sqlx.ROW,dt.representation+(k,v.dataType),dt.rowType+k)),
                p.values+(k,v));
        }
        public static TPath operator +(TPath p, (Context, TypedValue, long) x)
        {
            var (cx, v, k) = x;
            var dt = p.dataType;
            if (dt.representation.Contains(k))
            {
                var a = (TArray)p[k];
                return new TPath(p.matchAlt, dt, p.values + (k, a+(a.Length,v)));
            }
            return new TPath(p.matchAlt,
                (Domain)cx.Add(new Domain(dt.defpos, cx, Sqlx.ROW,
                dt.representation + (k, new Domain(-1L,Sqlx.ARRAY, v.dataType)), dt.rowType + k)),
                p.values + (k, v));
        }
        internal override TypedValue this[int i] => ((TArray)this[0L])?[i]??TNull.Value;
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append('('); sb.Append(DBObject.Uid(matchAlt)); sb.Append(',');
            sb.Append(values); sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A set can be placed in a cell of a Table, so is treated as a value type.
    /// Operations of UNION and INTERSECT etc are defined on sets.
    ///     
    /// </summary>
    internal class TSet : TypedValue
    {
        /// <summary>
        /// Implement the set as a tree whose key is V and whose value is bool 
        /// </summary>
        internal readonly CTree<TypedValue, bool> tree;
        /// <summary>
        /// Constructor: a new Set
        /// </summary>
        internal TSet(Domain dt) : base(new Domain(-1L, Sqlx.SET, dt))
        {
            tree = CTree<TypedValue, bool>.Empty;
        }
        internal TSet(Domain dt, CTree<TypedValue, bool> t) : base(dt)
        {
            tree = t;
        }
        public static TSet operator+(TSet a,TypedValue v)
        {
            return a.Add(v);
        }
        internal override int Cardinality()
        {
            return (int)tree.Count;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TSet((Domain)dataType.Fix(cx),tree);
        }
        internal TSet Add(TypedValue a)
        {
            var nt = tree;
            if (dataType.elType is null || !a.dataType.EqualOrStrongSubtypeOf(dataType.elType))
                throw new DBException("2200G", dataType.elType??Domain.Null, a.dataType);
            if (!nt.Contains(a))
                nt += (a, true);
            return new TSet(dataType, nt);
        }
        /// <summary>
        /// Whether an element is already in the set
        /// </summary>
        /// <param name="a">The element</param>
        /// <returns>Whether it is in the set</returns>
        internal override bool Contains(TypedValue a)
        {
            return tree.Contains(a);
        }
        internal SetBookmark? First()
        {
            var tf = tree.First();
            return (tf == null) ? null : new SetBookmark(this, 0, tf);
        }
        internal SetBookmark? Last()
        {
            var tl = tree.Last();
            return (tl == null) ? null : new SetBookmark(this, (int)tree.Count, tl);
        }
        
        internal class SetBookmark : IBookmark<TypedValue>
        {
            readonly TSet _set;
            readonly ABookmark<TypedValue, bool> _bmk;
            readonly long _pos;
            internal SetBookmark(TSet set, long pos,  ABookmark<TypedValue, bool> bmk)
            {
                _set = set; _pos = pos; _bmk = bmk;
            }
            public SetBookmark? Next()
            {
                var bmk = ABookmark<TypedValue, bool>.Next(_bmk, _set.tree);
                if (bmk == null) return null;
                return new SetBookmark(_set,_pos+1, bmk);
            }
            public SetBookmark? Previous()
            {
                var bmk = ABookmark<TypedValue, bool>.Previous(_bmk, _set.tree);
                if (bmk == null) return null;
                return new SetBookmark(_set, _pos + 1, bmk);
            }
            public long Position()
            {
                return _pos;
            }

            public TypedValue Value()
            {
                return _bmk.key();
            }

            IBookmark<TypedValue>? IBookmark<TypedValue>.Next()
            {
                return Next();
            }
        }
        /// <summary>
        /// Mutator: remove object a
        /// </summary>
        /// <param name="a">An object</param>
        internal TSet Remove(TypedValue a)
        {
            if (!tree.Contains(a))
                return this;
            return new TSet(dataType,tree - a);
        }
        /// <summary>
        /// Creator: forms the union of two sets
        /// </summary>
        /// <param name="a">A first set</param>
        /// <param name="b">A second set</param>
        /// <returns>a new Multiset</returns>
        internal static TSet? Union(TSet? a, TSet? b)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Row)
                ae = be;
            if (be != Domain.Row && be != ae)
                throw new DBException("22105").Mix();
            return new TSet(a.dataType,a.tree+b.tree);
        }
        /// <summary>
        /// Creator: forms the intersection of two sets
        /// </summary>
        /// <param name="a">A first multiset</param>
        /// <param name="b">A second multiset</param>
        /// <returns>a new Multiset</returns>
        internal static TSet? Intersect(TSet? a, TSet? b)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Row)
                ae = be;
            if (be != Domain.Row && be != ae)
                throw new DBException("22105").Mix();
            var t = a.tree;
            for (var d = t.First(); d != null; d = d.Next())
                if (d.key() is TypedValue v && !b.Contains(v))
                    t -= v;
            return new TSet(a.dataType, t);
        }
        /// <summary>
        /// Creator: forms the difference of two sets
        /// </summary>
        /// <param name="a">A first set</param>
        /// <param name="b">A second set</param>
        /// <returns>a new Multiset</returns>
        internal static TSet Except(TSet a, TSet b)
        {
            if (a == null)
                return b;
            if (b == null)
                return a;
            var t = a.tree;
            for (var d = t.First(); d != null; d = d.Next())
                if (d.key() is TypedValue v && b.Contains(v))
                    t -= v;
            return new TSet(a.dataType, t);
        }
        /// <summary>
        /// Construct a string repreesntation of the set
        /// </summary>
        /// <returns>a string</returns>
        public override string ToString()
        {
            var sb = new StringBuilder("SET(");
            var cm = "";
            for (var b = tree.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key());
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Multiset can be placed in a cell of a Table, so is treated as a value type.
    /// Operations of UNION and INTERSECT etc are defined on Multisets.
    ///     
    /// </summary>
    internal class TMultiset : TypedValue
    {
        /// <summary>
        /// Implement the multiset as a tree whose key is V and whose value is long 
        /// (the multiplicity of the key V as a member of the multiset).
        /// While this looks like MTree (which is TypedValue[] to long) it doesn't work the same way
        /// </summary>
        internal readonly BTree<TypedValue,long?> tree; 
        /// <summary>
        /// Accessor
        /// </summary>
        internal long Count { get { return count; } }
        /// <summary>
        /// Whether duplicates are distinguished
        /// </summary>
        internal bool distinct = false;
        internal long count = 0;
        /// <summary>
        /// Constructor: a new Multiset
        /// </summary>
        /// <param name="tr">The transaction</param>
        internal TMultiset(Domain dt) : base (new Domain(-1L,Sqlx.MULTISET,dt))
        {
            tree = BTree<TypedValue,long?>.Empty;
            count = 0;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(TMultiset tm) : base(tm.dataType)
        {
            tree = BTree<TypedValue, long?>.Empty;
            count = tm.count;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(Domain dt,BTree<TypedValue,long?>t,long ct) :base(dt)
        {
            tree = t; count = ct;
        }
        internal override int Cardinality()
        {
            return (int)count;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TMultiset((Domain)dataType.Fix(cx),
                cx.FixTVl(tree),count);
        }
        /// <summary>
        /// Mutator: Add n copies of object a
        /// </summary>
        /// <param name="a">An object</param>
        /// <param name="n">a multiplicity</param>
        internal TMultiset Add(TypedValue a, long n)
        {
             if (dataType.elType is null || !dataType.elType.CanTakeValueOf(a.dataType))
                throw new DBException("22005", dataType.elType??Domain.Null, a).ISO();
            var nt = tree;
            if (!nt.Contains(a))
                nt+=(a, n);
            else if (nt[a] is long o)
                nt+=(a, o + n);
            var nc = count + n;
            return new TMultiset(dataType, nt, nc);
        }
        /// <summary>
        /// Mutator: Add object a
        /// </summary>
        /// <param name="a">An object</param>
        internal TMultiset Add(TypedValue a)
        {
            return Add(a, 1L);
        }
        /// <summary>
        /// Whether an element is already in the multiset
        /// </summary>
        /// <param name="a">The element</param>
        /// <returns>Whether it is in the set</returns>
        internal override bool Contains(TypedValue a)
        {
            return tree.Contains(a);
        }
        internal MultisetBookmark? First()
        {
            var tf = tree.First();
            return (tf==null)?null:new MultisetBookmark(this,0,tf);
        }
        internal MultisetBookmark? Last()
        {
            var tl = tree.Last();
            return (tl==null)?null:new MultisetBookmark(this, count-1, tl);
        }
        
        internal class MultisetBookmark : IBookmark<TypedValue>
        {
            readonly TMultiset _set;
            readonly ABookmark<TypedValue, long?> _bmk;
            readonly long _pos;
            readonly long? _rep;
            internal MultisetBookmark(TMultiset set, long pos, 
                ABookmark<TypedValue, long?> bmk, long? rep = null)
            {
                _set = set; _pos = pos; _bmk = bmk; _rep = rep;
            }
            public MultisetBookmark? Next()
            {
                var bmk = _bmk;
                var rep = _rep;
                for (; ; )
                {
                    if (rep is not null && rep>0)
                            return new MultisetBookmark(_set, _pos + 1, bmk, rep - 1);
                    bmk = ABookmark<TypedValue, long?>.Next(bmk, _set.tree);
                    if (bmk == null)
                        return null;
                    rep = bmk.value();
                }
            }
            public MultisetBookmark? Previous()
            {
                var bmk = _bmk;
                var rep = _rep;
                for (; ; )
                {
                    if (rep is not null && rep>0)
                            return new MultisetBookmark(_set, _pos - 1, bmk, rep - 1);
                    bmk = ABookmark<TypedValue, long?>.Previous(bmk, _set.tree);
                    if (bmk == null)
                        return null;
                    rep = bmk.value();
                }
            }
            public long Position()
            {
                return _pos;
            }

            public TypedValue Value()
            {
                return _bmk.key();
            }

            IBookmark<TypedValue>? IBookmark<TypedValue>.Next()
            {
                return Next();
            }
        }
        /// <summary>
        /// Mutator: remove n copies of object a
        /// </summary>
        /// <param name="a">An object</param>
        /// <param name="n">A multiplicity</param>
        internal TMultiset Remove(TypedValue a, long n)
        {
            var o = tree[a];
            if (o == null)
                return this; // was DBException 22103
            long m = (long)o;
            var nt = tree;
            if (m <= n)
                nt -= a;
            else
                nt+=(a, m - n);
            var nc = count - n;
            return new TMultiset(dataType, nt, nc);
        }
        /// <summary>
        /// Mutator: remove object a
        /// </summary>
        /// <param name="a">An object</param>
        internal TMultiset Remove(TypedValue a)
        {
            return Remove(a, 1);
        }
        /// <summary>
        /// Creator: A Multiset of the distinct objects of this
        /// </summary>
        /// <returns>A new Multiset</returns>
        internal TMultiset Set() // return a multiset with same values but no duplicates
        {
            TMultiset m = new (this);
            for (var b = m.tree.First();b is not null;b=b.Next())
                m.Add(b.key());
            return m;
        }
        /// <summary>
        /// Creator: forms the union of two Multisets, optionally removing duplicates
        /// </summary>
        /// <param name="a">A first multiset</param>
        /// <param name="b">A second multiset</param>
        /// <param name="all">true if duplicates are not to be removed</param>
        /// <returns>a new Multiset</returns>
        internal static TMultiset? Union(TMultiset? a, TMultiset? b, bool all)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Row)
                ae = be;
            if (be != Domain.Row && be != ae)
                throw new DBException("22105").Mix();
            TMultiset r = new (a);
            if (all)
            {
                for (var d = b.tree.First(); d != null; d = d.Next())
                    if (d.value() is long p)
                        r = r.Add(d.key(), p);
            }
            else
            {
                for (var d = a.tree.First(); d != null; d = d.Next())
                    r = r.Add(d.key());
                for (var d = b.tree.First(); d != null; d = d.Next())
                    if (!a.tree.Contains(d.key()))
                        r = r.Add(d.key());
            }
            return r;
        }
        /// <summary>
        /// Creator: forms the intersection of two Multisets, optionally removing duplicates
        /// </summary>
        /// <param name="a">A first multiset</param>
        /// <param name="b">A second multiset</param>
        /// <param name="all">true if duplicates are not to be removed</param>
        /// <returns>a new Multiset</returns>
        internal static TMultiset? Intersect(TMultiset? a, TMultiset? b, bool all)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Row)
                ae = be;
            if (be != Domain.Row && be != ae)
                throw new DBException("22105").Mix();
            TMultiset r = new (a);
            for(var d = a.tree.First();d is not null;d=d.Next())
            {
                TypedValue v = d.key();
                if (!b.tree.Contains(v))
                    r.Remove(v);
                else if (all && d.value() is long m && b.tree[v] is long n)
                    r.Add(v, (m<n) ? m : n);
                else
                    r.Add(v);
            }
            return r;
        }
        /// <summary>
        /// Creator: forms the difference of two Multisets, optionally removing duplicates
        /// </summary>
        /// <param name="a">A first multiset</param>
        /// <param name="b">A second multiset</param>
        /// <param name="all">true if duplicates are not to be removed</param>
        /// <returns>a new Multiset</returns>
        internal static TMultiset Except(TMultiset a, TMultiset b, bool all)
        {
            if (a == null)
                return b;
            if (b == null)
                return a;
            TMultiset r = new (a.dataType);
            for (var d = a.tree.First(); d != null; d = d.Next())
            if (d.value() is long m && d.key() is TypedValue v ){
                if (all && b.tree[v] is long n)
                {
                    if (m > n)
                        r.Add(v, m - n);
                }
                else 
                    r.Add(v);
            }
            return r;
        }
        /// <summary>
        /// Construct a string repreesntation of the Multiset 
        /// </summary>
        /// <returns>a string</returns>
        public override string ToString()
        {
            var sb = new StringBuilder("MULTISET(");
            var cm = "";
            for (var b = tree.First(); b != null; b = b.Next())
                if (b.value() is long n && n>0)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key());
                    if (n!=1L)
                    {
                        sb.Append('(');sb.Append(n); sb.Append(')');
                    }    
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    // shareable
    internal class Adapters
    {
        readonly BTree<long, BTree<long, long?>> list; // if long? >0 it is defpos of invertible adapter function
        internal Adapters() : this(BTree<long, BTree<long, long?>>.Empty) { }
        Adapters(BTree<long, BTree<long, long?>> e)
        {
            list = e;
        }
        Adapters _Add(long a, long b, long atob)
        {
            var r = list;
            var ab = list[a];
            if (ab == null)
                r+=(a, new BTree<long, long?>(b, atob));
            else if (!ab.Contains(b))
            {
                ab+=(b, atob);
                r+=(a, ab);
            }
            return new Adapters(r);
        }
        internal Adapters Add(long a, long b, long atob, long btoa)
        {
            return _Add(a, b, btoa)._Add(b, a, atob);
        }
        internal long? Match(long a, long b)
        {
            if (a == b)
                return 0;
            var ab = list[a];
            if (ab == null)
                return null;
            return ab[b];
        }
    }
    /// <summary>
    /// The OrderCategory enumeration. Row of this type are placed in the database so the following values cannot be changed.
    /// </summary>
    [Flags]
    internal enum OrderCategory 
    { None = 0, Equals = 1, Full = 2, Relative = 4, Map = 8, State = 16, Primitive = 32 };
    /// <summary>
    /// A set of DBObject references
    /// </summary>
    internal class TMeta : TypedValue
    {
        public readonly long defpos;
        public readonly Sqlx kind;
        public readonly CTree<string, TypedValue> props;
        public readonly DBObject? dbo;
        public TMeta(long dp,Sqlx k,Context cx) : base(Domain.Content) 
        {
            defpos = dp;
            kind = k;
            var r = CTree<string,TypedValue>.Empty;
            if (cx.db.objects[dp] is DBObject ob)
            {
                dbo = ob;
                r += ("Pos", new TInt(dp));
                r += ("Type", new TChar(ob.GetType().Name));
                r += ("Domain", new TChar(ob.domain.kind.ToString()));
                if (ob.definer>0)
                    r += ("Definer", new TInt(ob.definer));
                if (ob.infos[cx.role.defpos] is ObInfo oi && oi.name is string no)
                    r += ("Name", new TChar(no));
            }
            props = r;
        }
    }
}
