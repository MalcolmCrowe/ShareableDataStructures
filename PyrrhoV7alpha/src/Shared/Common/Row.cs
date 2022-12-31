using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
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
        internal abstract TypedValue New(Domain t);
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
            return null;
        }
        internal virtual TypedValue Fix(Context cx)
        {
            var dm = (Domain)dataType.Fix(cx);
            return (dm==dataType)?this:New(dm);
        }
        internal virtual TypedValue Replaced(Context cx)
        {
            return New(dataType.Replaced(cx));
        }
        internal virtual TypedValue Replace(Context cx,DBObject so,DBObject sv)
        {
            return Replaced(cx);
        }
        internal virtual TypedValue Relocate(Context cx)
        {
            if (dataType.defpos < 0)
                return this;
            return New((Domain)dataType.Relocate(cx));
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
        public override string ToString()
        {
            return "TypedValue";
        }
        internal virtual string ToString(CList<long> cs, Context cx)
        {
            return ToString();
        }
        public virtual int CompareTo(object? obj)
        {
            return dataType.Compare(this,(obj as TypedValue)??TNull.Value);
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
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
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
        internal override TypedValue New(Domain t)
        {
            return new TInt(t, value);
        }
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
            if (ivalue <long.MaxValue && ivalue>long.MinValue)
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
        internal readonly bool value;
        internal static TBool False = new (false);
        internal static TBool True = new (true);
        private TBool(Domain dt, bool b) : base(dt) { value = b; }
        private TBool(bool b) : this(Domain.Bool, b) { }
        internal override TypedValue New(Domain t)
        {
            return new TBool(t, value);
        }
        public override string ToString()
        {
            return value.ToString();
        }
        internal override bool? ToBool()
        {
            return value;
        }
        internal static TypedValue For(bool p)
        {
            return p ? True : False;
        }
    }
    // shareable
    internal class TChar : TypedValue
    {
        internal readonly string value;
        internal static TChar Empty = new ("");
        internal TChar(Domain dt, string s) : base(dt) { value = s; }
        internal TChar(string s) : this(Domain.Char, s) { }
        internal TChar(Ident n) : this((n == null) ? "" : n.ident) { }
        internal override TypedValue New(Domain t)
        {
            return new TChar(t, value);
        }
        public override string ToString()
        {
            return value??"null";
        }
        internal override string ToString(CList<long> cs,Context cx)
        {
            return "'" + value + "'";
        }
    }
    // shareable
    internal class TNumeric : TypedValue
    {
        internal readonly Numeric value;
        internal TNumeric(Domain dt, Numeric n) : base(dt.Best(Domain.Numeric)) { value = n; }
        internal TNumeric(Numeric n) : this(Domain.Numeric, n) { }
        internal override TypedValue New(Domain t)
        {
            return new TNumeric(t, value);
        }
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
        internal override TypedValue New(Domain t)
        {
            return new TReal(t, dvalue, nvalue);
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

    // shareable as of 26 April 2021
    internal class TSensitive : TypedValue
    {
        internal readonly TypedValue value;
        internal TSensitive(Domain dt, TypedValue v) : base(dt)
        {
            value = (v is TSensitive st) ? st.value : v;
        }
        internal override TypedValue New(Domain t)
        {
            return new TSensitive(t, value);
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
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
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
        internal override TypedValue New(Domain t)
        {
            return t.defaultValue;
        }
        public override string ToString()
        {
            return "?" + DBObject.Uid(qid.dp);
        }
    }
    // shareable as of 26 April 2021
    internal class TUnion : TypedValue 
    {
        internal readonly TypedValue value = TNull.Value;
        internal TUnion(Domain dt, TypedValue v) : base(dt) { value = v;  }
        internal override TypedValue New(Domain t)
        {
            return new TUnion(t,value); // approximate: use Relocate
        }
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
        internal override TypedValue New(Domain t)
        {
            return new TDateTime(t, value);
        }
        public override string ToString()
        {
            return value.ToString();
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
        internal override TypedValue New(Domain t)
        {
            return new TInterval(t, value);
        }
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
        internal override TypedValue New(Domain t)
        {
            return new TTimeSpan(t, value);
        }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    /// <summary>
    /// This is really part of the implementation of multi-level indexes (MTree etc)
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class TMTree: TypedValue
    {
        internal MTree value;
        internal TMTree(MTree m) : base(Domain.MTree) { value = m; }
        internal override TypedValue New(Domain t)
        {
            return this; // approximate: use Relocate
        }
    }
    /// <summary>
    /// This is also part of the implementation of multi-level indexes (MTree etc)
    /// </summary>
    internal class TPartial : TypedValue
    {
        internal CTree<long, bool> value;
        internal TPartial(CTree<long, bool> t) : base(Domain.Partial) { value = t; }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TPartial(cx.FixTlb(value));
        }
    }
    // shareable as of 26 April 2021
    internal class TArray : TypedValue
    {
        internal readonly BList<TypedValue> list; 
        internal TArray(Domain dt, params TypedValue[] a) 
            : base(new Domain(-1L,Sqlx.ARRAY,dt)) 
        { 
            var ts = BList<TypedValue>.Empty;
            foreach (var x in a)
                ts += x;
            list = ts;
        }
        internal TArray(Domain dt, BList<TypedValue> a) : base(dt) { list = a; }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TArray((Domain)dataType.Fix(cx),
                cx.FixBV(list));
        }
        public static TArray operator+(TArray ar,TypedValue v)
        {
            if (!ar.dataType.elType.CanTakeValueOf(v.dataType))
                throw new DBException("22005", ar.dataType.elType, v.dataType);
            return new TArray(ar.dataType, ar.list + v);
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
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var cm = "";
            if (list != null)
                for(var b=list.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value().ToString());
                }
            sb.Append(']');
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class TTypeSpec : TypedValue
    {
        readonly Domain _dataType;
        internal TTypeSpec(Domain t) : base(Domain.TypeSpec)
        {
            _dataType = t;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TTypeSpec((Domain)_dataType.Fix(cx));
        }
    }
    internal class TLevel : TypedValue
    {
        internal readonly Level val;
        public static TLevel D = new (Level.D);

        TLevel(Level v) : base(Domain._Level) { val = v; }
        internal override TypedValue New(Domain t)
        {
            return this;
        }
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
        internal override TypedValue New(Domain t)
        {
            return new TBlob(t,value);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("byte[");
            if (value!=null)
            sb.Append(value.Length);
            sb.Append(']');
            if (value!=null && value.Length>0)
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
    // shareable as of 26 April 2021
    internal class TPeriod : TypedValue
    {
        internal readonly Period value;
        internal TPeriod(Domain dt, Period p) : base(dt) { value = p; }
        internal override TypedValue New(Domain t)
        {
            return new TPeriod(t, value);
        }
    }
    /// <summary>
    /// A row-version cookie
    /// </summary>
    internal class TRvv : TypedValue
    {
        internal readonly Rvv rvv = Rvv.Empty;
        internal TRvv(string match) : base (Domain.Rvv)
        {
            rvv = Rvv.Parse(match)??Rvv.Empty;
        }
        internal TRvv(Rvv r) : base(Domain.Rvv)
        {
            rvv = r;
        }
        /// <summary>
        /// Remote data may contain extra columns for Rvv info:
        /// if not, use -1 default indicating no information (disallow updates)
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="vs"></param>
        internal TRvv(Context cx, CTree<long, TypedValue> vs) : base(Domain.Rvv)
        {
            var r = Rvv.Empty;
            var dp = vs[DBObject.Defpos]?.ToLong() ?? -1L;
            var pp = vs[DBObject.LastChange]?.ToLong() ?? -1L;
            if (dp >= 0 && pp >= 0)
                r += (cx.result, (dp, pp));
            rvv = r;
        }
        internal override TypedValue New(Domain t)
        {
            return this;
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
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
        }
        public override string ToString()
        {
            if (md == CTree<Sqlx, TypedValue>.Empty)
                return "";
            var sb = new StringBuilder();
            var cm = '{';
            for (var b=md.First();b!=null;b=b.Next())
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
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class TRow : TypedValue
    {
        internal readonly CTree<long, TypedValue> values;
        internal readonly int docCol = -1;  // note the position of a single document column if present
        internal int Length => dataType.Length;
        internal CList<long> columns => dataType.rowType;
        internal static TRow Empty = new (Domain.Row, CTree<long, TypedValue>.Empty);
        public TRow(Domain dt,CTree<long,TypedValue> vs)
            :base(dt)
        {
            values = vs;
        }
        public TRow(Domain dt, BTree<long, long>? map, BTree<long, TypedValue> vs)
            : base(dt)
        {
            var v = CTree<long, TypedValue>.Empty;
            for (var b = map?.First(); b != null; b = b.Next())
                if (dt.representation.Contains(b.key()))
                    v += (b.key(), vs[b.value()] ?? TNull.Value);
            values = v;
        }
        public TRow(RowSet rs,Domain dm,TRow rw) : base(dm)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var v = rw[p];
                if (v == null)
                    for (var c = rs.matching[p]?.First(); c != null && v == null; c = c.Next())
                        v = rw[c.key()];
                vs += (p, v ?? TNull.Value);
            }
            values = vs;
        }
        public TRow(Domain dm,Domain cols,TRow rw) :base(dm)
        {
            var vs = CTree<long,TypedValue>.Empty;
            var rb = rw.columns.First();
            var d = (cols == Domain.Row) ? dm : cols;
            for (var b = d.First(); b != null && rb!=null; b = b.Next(),rb=rb.Next())
                vs += (b.value(), rw[rb.value()]);
            values = vs;
        }
        public TRow(TRow rw, Domain dm) :base(dm)
        {
            var v = CTree<long, TypedValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                v += (b.value(), rw[b.key()] ?? TNull.Value);
            values = v;
        }
        /// <summary>
        /// Constructor: values by columns
        /// </summary>
         /// <param name="v">The values</param>
        public TRow(Context cx,Domain dt, params TypedValue[] v) : 
            base(dt)
        {
            var vals = CTree<long, TypedValue>.Empty;
            var i = 0;
            for (var b = dt.rowType.First(); b != null; b = b.Next(), i++)
            {
                var p = b.value();
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
            for (var b = dm.rowType.First(); b != null && tb!=null; b = b.Next(), tb=tb.Next())
            {
                var p = b.value();
                vals += (p, tb.value());
            }
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
                    if (values[b.value()] != TNull.Value)
                        return false;
                return true;
            }
        }
        internal override TypedValue New(Domain t)
        {
            var vs = CTree<long, TypedValue>.Empty;
            var nb = t.rowType.First();
            for (var b = dataType.rowType.First(); b != null && nb != null; b = b.Next(), nb = nb.Next())
                if (values[b.value()] is TypedValue v)
                vs += (nb.value(), v);
            return new TRow(t, vs);
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
        internal TypedValue this[int i]
        {
            get {
                if (columns != null && columns.Contains(i))
                    return values[columns[i]]??TNull.Value;
                var j = 1;
                for (var b = dataType.representation.First(); i >= j && b != null; b = b.Next(), j++)
                    if (i == j)
                        return values[b.key()]??TNull.Value;
                return TNull.Value;
            }
        }
        internal override TypedValue Replace(Context cx, DBObject ov, DBObject nv)
        {
            var dt = (Domain)dataType.Replace(cx, ov, nv);
            if (dt == dataType)
                return this;
            var vs = new TypedValue[dt.Length];
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (values[b.value()] is TypedValue v)
                    vs[b.key()] = v;
            return new TRow(cx, dt, vs);
        }
        /// <summary>
        /// Make a readable representation of the Row
        /// </summary>
        /// <returns>the representation</returns>
        public override string ToString()
        {
            var str = new StringBuilder();
            var cm = '(';
            for (var b=columns.First();b!=null;b=b.Next())
            {
                str.Append(cm); cm = ',';
                var p = b.value();
                str.Append(DBObject.Uid(p)); str.Append('=');
                str.Append(values[p]?? TNull.Value);
            }
            str.Append(')');
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
                r += values[b.value()]??TNull.Value;
            return r;
        }
    }
    /// <summary>
    /// A Multiset can be placed in a cell of a Table, so is treated as a value type.
    /// Operations of UNION and INTERSECT etc are defined on Multisets.
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class TMultiset : TypedValue
    {
        /// <summary>
        /// Implement the multiset as a tree whose key is V and whose value is long 
        /// (the multiplicity of the key V as a member of the multiset).
        /// While this looks like MTree (which is TypedValue[] to long) it doesn't work the same way
        /// </summary>
        internal CTree<TypedValue,long> tree; 
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
        internal TMultiset(Domain dt) : base (dt)
        {
            tree = CTree<TypedValue,long>.Empty;
            count = 0;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(TMultiset tm) : base(tm.dataType)
        {
            tree = CTree<TypedValue, long>.Empty;
            count = tm.count;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(Domain dt,CTree<TypedValue,long>t,long ct) :base(dt)
        {
            tree = t; count = ct;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
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
        internal void Add(TypedValue a, long n)
        {
             if (!dataType.elType.CanTakeValueOf(a.dataType))
                throw new DBException("22005", dataType.elType, a).ISO();
            if (!tree.Contains(a))
                tree+=(a, n);
            else if (distinct)
                return;
            else
            {
                long o = tree[a];
                tree+=(a, o + n);
            }
            count += n;
        }
        /// <summary>
        /// Mutator: Add object a
        /// </summary>
        /// <param name="a">An object</param>
        internal void Add(TypedValue a)
        {
            Add(a, 1L);
        }
        /// <summary>
        /// Whether an element is already in the multiset
        /// </summary>
        /// <param name="a">The element</param>
        /// <returns>Whether it is in the set</returns>
        internal bool Contains(TypedValue a)
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
        // shareable as of 26 April 2021
        internal class MultisetBookmark : IBookmark<TypedValue>
        {
            readonly TMultiset _set;
            readonly ABookmark<TypedValue, long> _bmk;
            readonly long _pos;
            readonly long? _rep;
            internal MultisetBookmark(TMultiset set, long pos, 
                ABookmark<TypedValue, long> bmk, long? rep = null)
            {
                _set = set; _pos = pos; _bmk = bmk; _rep = rep;
            }
            public MultisetBookmark? Next()
            {
                var bmk = _bmk;
                var rep = _rep;
                for (; ; )
                {
                    if (rep!=null && rep>0)
                            return new MultisetBookmark(_set, _pos + 1, bmk, rep - 1);
                    bmk = ABookmark<TypedValue, long>.Next(bmk, _set.tree);
                    if (bmk == null)
                        return null;
                }
            }
            public MultisetBookmark? Previous()
            {
                var bmk = _bmk;
                var rep = _rep;
                for (; ; )
                {
                    if (rep!=null && rep>0)
                            return new MultisetBookmark(_set, _pos - 1, bmk, rep - 1);
                    bmk = ABookmark<TypedValue, long>.Previous(bmk, _set.tree);
                    if (bmk == null)
                        return null;
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
        internal void Remove(TypedValue a, long n)
        {
            object o = tree[a];
            if (o == null)
                return; // was DBException 22103
            long m = (long)o;
            if (m <= n)
                tree -= a;
            else
                tree+=(a, m - n);
            count -= n;
        }
        /// <summary>
        /// Mutator: remove object a
        /// </summary>
        /// <param name="a">An object</param>
        internal void Remove(TypedValue a)
        {
            Remove(a, 1);
        }
        /// <summary>
        /// Creator: A Multiset of the distinct objects of this
        /// </summary>
        /// <returns>A new Multiset</returns>
        internal TMultiset Set() // return a multiset with same values but no duplicates
        {
            TMultiset m = new (this);
            for (var b = m.tree.First();b!=null;b=b.Next())
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
                r.tree = a.tree;
                r.count = a.count;
                for (var d=b.tree.First();d!=null;d=d.Next())
                    r.Add(d.key(), d.value());
            }
            else
            {
                for (var d =a.tree.First();d!=null;d=d.Next())
                    r.Add(d.key());
                for (var d = b.tree.First(); d != null; d = d.Next())
                    if (!a.tree.Contains(d.key()))
                        r.Add(d.key());
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
            for(var d = a.tree.First();d!=null;d=d.Next())
            {
                TypedValue v = d.key();
                if (!b.tree.Contains(v))
                    r.Remove(v);
                else if (all)
                {
                    long m = d.value();
                    long n = b.tree[v];
                    r.Add(v, (m<n) ? m : n);
                }
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
            {
                TypedValue v = d.key();
                long m = d.value();
                long n = 0;
                object o = b.tree[v];
                if (all)
                {
                    if (o != null)
                        n = (long)o;
                    if (m > n)
                        r.Add(v, m - n);
                }
                else if (o == null)
                    r.Add(v);
            }
            return r;
        }
        /// <summary>
        /// Construct a string repreesntation of the Multiset (for debugging)
        /// </summary>
        /// <returns>a string</returns>
        public override string ToString()
        {
            string str = "MULTISET(";
            bool first = true;
            for (var b = tree.First(); b != null; b = b.Next())
            {
                if (!first)
                    str += ",";
                for (var i = 0; i < b.value(); i++)
                    str += b.key().ToString();
            }
            return str + ")";
        }
    }
    /// <summary>
    /// The model for Xml values is that element ordering and repetition is important,
    /// but attributes are not ordered and have unique names.
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class TXml : TypedValue
    {
        internal readonly string? name;
        internal readonly CTree<string, TypedValue> attributes = CTree<string, TypedValue>.Empty;
        internal readonly string content = "";
        internal readonly CList<TXml> children = CList<TXml>.Empty;
        internal static TXml Null = new (null);
        internal TXml(string? n) : base(Domain.XML) { name = n; }
        TXml(string? n,CTree<string,TypedValue>a,string c,CList<TXml> ch)
            : base(Domain.XML)
        {
            name = n; attributes = a; content = c; children = ch;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TXml(name,cx.FixTsV(attributes),content,cx.FixLX(children));
        }
        public static TXml operator+(TXml t,(string,TypedValue)a)
        {
            return new TXml(t.name, t.attributes + a, t.content, t.children);
        }
        public static TXml operator+(TXml t,TXml c)
        {
            return new TXml(t.name,t.attributes,t.content,t.children + c);
        }
        public static TXml operator +(TXml t, string c)
        {
            return new TXml(t.name, t.attributes, c, t.children);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("<" + name);
            for (var b=attributes.First();b!=null;b=b.Next())
                sb.Append(" " + b.key() + "=\"" + b.value().ToString() + "\"");
            if (content != "")
                sb.Append(">" + content + "</" + name + ">");
            else if (children.Count > 0)
            {
                sb.Append('>');
                for (var b=children.First();b!=null;b=b.Next())
                    sb.Append(b.value().ToString());
                sb.Append("</" + name + ">");
            }
            else
                sb.Append("/>");
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
}
