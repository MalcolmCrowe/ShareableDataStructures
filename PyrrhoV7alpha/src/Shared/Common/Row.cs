using System;
using System.IO;
using System.Text;
using System.Xml;
using System.Collections;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
    /// In normal use such complex data can be in a class of TypedValue 
    /// (containing other TypedValues), and can be referred to by a single SqlValue. 
    /// But if during analysis we find we are dealing with SQL references to its internal contents, 
    /// it is built into a structured SqlValue (SqlRow, SqlArray etc) 
    /// whose referenceable elements are SqlValues. 
    /// Evaluation will restore the simple TypedValue structure.
    /// </summary>
    public abstract class TypedValue : ITypedValue,IComparable
    {
        internal readonly Domain dataType = Domain.Null;
        internal TypedValue(Domain t)
        {
            if (t == null)
                throw new PEException("PE666");
            dataType = t;
        }
        internal abstract TypedValue New(Domain t);
        internal abstract object Val();
        internal virtual TypedValue Next()
        {
            throw new DBException("22009",dataType.kind.ToString());
        }
        internal virtual SqlValue Build(long dp,Context cx,Domain td)
        {
            if (td.kind == Sqlx.CONTENT)
                td = null;
            return new SqlLiteral(dp, cx, this, td);
        }
        internal virtual byte BsonType()
        {
            return dataType.BsonType();
        }
        public abstract int _CompareTo(object obj);
        internal virtual TypedValue this[string n]
        {
            get { return null; }
            set { }
        }
        internal virtual TypedValue this[long n]
        {
            get { return null; }
            set { }
        }
        internal TypedValue this[Ident n] { get { return this[n.ident]; } }
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
        internal virtual Integer ToInteger()
        {
            return null;
        }
        internal virtual double ToDouble()
        {
            return double.NaN;
        }
        internal virtual TypedValue[] ToArray()
        {
            return null;
        }
        public abstract bool IsNull
        {
            get;
        }
        internal virtual TypedValue Relocate(Context cx)
        {
            return New((Domain)dataType._Relocate(cx));
        }
        internal virtual TypedValue Relocate(Writer wr)
        {
            return New((Domain)dataType._Relocate(wr));
        }
        public override string ToString()
        {
            return "TypedValue";
        }
        internal TypedValue NotNull()
        {
            return IsNull ? TNull.Value : this;
        }
        public int CompareTo(object obj)
        {
            return dataType.Compare(this,(TypedValue)obj);
        }
    }
    internal class TNull : TypedValue
    {
        static TNull _Value;
        TNull() : base(Domain.Null) { _Value = this; }
        internal static TNull Value => _Value??new TNull();
        internal override TypedValue New(Domain t)
        {
            return _Value;
        }
        internal override object Val()
        {
            return null;
        }
        public override string ToString()
        {
            return "Null";
        }
        public override int _CompareTo(object obj)
        {
            // other cases dealt with in Compare
            return -1;
        }
        public override bool IsNull
        {
            get
            {
                return true;
            }
        }
    }
    internal class TInt : TypedValue
    {
        internal readonly long? value;
        internal TInt(Domain dt, long? v) : base(dt) { value = v; }
        internal TInt(long? v) : this(Domain.Int, v) { }
        internal override TypedValue New(Domain t)
        {
            return new TInt(t, value);
        }
        internal override object Val()
        {
            return value;
        }
        internal override TypedValue Next()
        {
            if (value.HasValue)
                return new TInt(dataType, value.Value + 1);
            return base.Next();
        }
        public override string ToString()
        {
            return value.ToString();
        }
        public override int _CompareTo(object obj)
        {
            var nt = obj as Integer;
            if (nt != null && value.HasValue)
                return -nt.CompareTo(new Integer(value.Value));
            var that = (TInt)obj;
            if (value == that.value)
                return 0;
            if (!value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1;
            if (!that.value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1;
            return value.Value.CompareTo(that.value.Value);
        }
        internal override byte BsonType()
        {
            if (value > int.MinValue && value < int.MaxValue)
                return 16;
            return 18;
        }
        internal override int? ToInt()
        {
            return (int?)value;
        }
        internal override long? ToLong()
        {
            return value;
        }
        internal override Integer ToInteger()
        {
            return value.HasValue?new Integer(value.Value):null;
        }
        public override bool IsNull
        {
            get
            {
                return !value.HasValue;
            }
        }
    }
    internal class TInteger : TInt
    {
        internal readonly Integer ivalue;
        internal TInteger(Domain dt, Integer i) : base(dt,0) { ivalue = i; }
        internal TInteger(Integer i) : this(Domain.Int, i) { }
        internal override object Val()
        {
            return ivalue;
        }
        internal override TypedValue Next()
        {
            if (ivalue != null)
                return new TInteger(ivalue.Add(new Integer(1),0));
            return base.Next();
        }
        public override int _CompareTo(object obj)
        {
            Integer v = null;
            if (obj is TInt)
                v = ((TInt)obj).ToInteger();
            else if (ivalue!=null)
                v = ((TInteger)obj).ivalue;
            if (ivalue == v)
                return 0;
            if (ivalue == null && v != null)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (v == null)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1;
            return ivalue.CompareTo(v);
        }
        internal override byte BsonType()
        {
            if (ivalue.BitsNeeded() > 64)
                return 19; // Decimal subtype added for Pyrrho
            return base.BsonType();
        }
        internal override int? ToInt()
        {
            if (ivalue == null)
                return null;
            return (int)ivalue;
        }
        internal override long? ToLong()
        {
            if (ivalue == null)
                return null;
            return ivalue;
        }
        internal override Integer ToInteger()
        {
            return ivalue;
        }
        public override bool IsNull
        {
            get
            {
                return ivalue == null;
            }
        }
        public override string ToString()
        {
            if (ivalue == null)
                return "null";
            return ivalue.ToString();
        }
    }
    internal class TBool : TypedValue
    {
        internal readonly bool? value;
        internal static TBool False = new TBool(false);
        internal static TBool True = new TBool(true);
        internal static TBool Null = new TBool(null);
        private TBool(Domain dt, bool? b) : base(dt) { value = b; }
        private TBool(bool? b) : this(Domain.Bool, b) { }
        internal override TypedValue New(Domain t)
        {
            return new TBool(t, value);
        }
        public override int _CompareTo(object obj)
        {
            var that = (TBool)obj;
            if (value == that.value)
                return 0;
            if (!value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (!that.value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.Value.CompareTo(that.value.Value);
        }
        public override string ToString()
        {
            return value.ToString();
        }
        internal override object Val()
        {
            return value;
        }
        internal override bool? ToBool()
        {
            return value;
        }
        public override bool IsNull
        {
            get
            {
                return !value.HasValue;
            }
        }
        internal static TypedValue For(bool p)
        {
            return p ? True : False;
        }
    }
    internal class TChar : TypedValue
    {
        internal readonly string value;
        internal static TChar Empty = new TChar("");
        internal TChar(Domain dt, string s) : base(dt) { value = s; }
        internal TChar(string s) : this(Domain.Char, s) { }
        internal TChar(DBObject n,Database d) : this((n == null) ? "" : 
            ((ObInfo)d.role.infos[n.defpos]).name) { }
        internal TChar(Ident n) : this((n == null) ? "" : n.ident) { }
        internal override TypedValue New(Domain t)
        {
            return new TChar(t, value);
        }
        public override int _CompareTo(object obj)
        {
            var that = obj as TChar;
            if (that == null && obj != null)
                that = new TChar(obj.ToString());
            if (value == that.value)
                return 0;
            if (value == null)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (that.value == null)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.CompareTo(that.value);
        }
        public override string ToString()
        {
            return value;
        }
        internal override object Val()
        {
            return value;
        }
        public override bool IsNull
        {
            get
            {
                return value==null;
            }
        }
    }
    internal class TNumeric : TypedValue
    {
        internal readonly Numeric value;
        internal TNumeric(Domain dt, Numeric n) : base(dt) { value = n; }
        internal TNumeric(Numeric n) : this(Domain.Numeric, n) { }
        internal override TypedValue New(Domain t)
        {
            return new TNumeric(t, value);
        }
        public override int _CompareTo(object obj)
        {
            var that = (TNumeric)obj;
            if (value == that.value)
                return 0;
            if (value == null)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (that.value == null)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.CompareTo(that.value);
        }
        internal override TypedValue Next()
        {
            if (value!=null)
                return new TNumeric(dataType,new Numeric(value.mantissa.Add(new Integer(1),value.scale)));
            return base.Next();
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
        internal override Integer ToInteger()
        {
            Integer r = new Integer(0);
            if (value.TryConvert(ref r))
                return r; 
            return base.ToInteger();
        }
        internal override object Val()
        {
            return value;
        }
        public override bool IsNull
        {
            get
            {
                return (object)value==null;
            }
        }
    }
    internal class TReal : TypedValue
    {
        internal readonly double dvalue = double.NaN;
        internal readonly Numeric nvalue = null;
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
        public override int _CompareTo(object obj)
        {
            var that = (TReal)obj;
            if (((object)nvalue) != null)
            {
                if (((object)that.nvalue) != null)
                    return nvalue.CompareTo(that.nvalue);
                else
                    return nvalue.CompareTo(new Numeric(that.dvalue));
            }
            if (((object)that.nvalue) == null)
                return dvalue.CompareTo(that.dvalue);
            return new Numeric(dvalue).CompareTo(that.nvalue);
        }
        internal override TypedValue Next()
        {
            if (nvalue != null)
                return new TReal(dataType, new Numeric(nvalue.mantissa.Add(new Integer(1), 0), 0));
            if (dvalue != double.NaN)
                return new TReal(dataType, dvalue + 1);
            return base.Next();
        }
        public override string ToString()
        {
            if ((object)nvalue != null)
                return nvalue.ToString();
            return dvalue.ToString();
        }
        internal override double ToDouble()
        {
            if (((object)nvalue) != null)
                return (double)nvalue;
            return dvalue;
        }
        internal override object Val()
        {
            if ((object)nvalue == null)
                return dvalue;
            return nvalue;
        }
        public override bool IsNull
        {
            get
            {
                return dvalue==double.NaN && nvalue==null;
            }
        }
    }
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
        public override int _CompareTo(object obj)
        {
            object that = (obj is TSensitive st) ? st.value : obj;
            return value._CompareTo(that);
        }

        internal override object Val()
        {
            return value.Val();
        }
        public override bool IsNull => value.IsNull;
        public override string ToString()
        {
            return value.ToString();
        }
    }
    internal class TUnion : TypedValue 
    {
        internal TypedValue value = null;
        internal TUnion(Domain dt, TypedValue v) : base(dt) { value = v;  }
        internal override TypedValue New(Domain t)
        {
            return new TUnion(t,value); // approximate: use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TUnion((Domain)dataType._Relocate(cx), 
                value.Relocate(cx));
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TUnion((Domain)dataType._Relocate(wr),
                value.Relocate(wr));
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal override object Val()
        {
            return value;
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
                var dt = b.value();
                if (dt.HasValue(cx,value))
                    ts.Add(dt);
            }
            if (ts.Count == 0)
                throw new Exception("22000");
            if (ts.Count == 1)
                return ts[0].Coerce(cx,value);
            return new TUnion(Domain.UnionType(lp,ts.ToArray()),value);
        }
        public override bool IsNull
        {
            get
            {
                return value!=null;
            }
        }
    }
    internal class TDateTime : TypedValue
    {
        internal readonly DateTime? value;
        internal TDateTime(Domain dt, DateTime? d) : base(dt) { value = d; }
        internal TDateTime(DateTime d) : this(Domain.Timestamp, d) { }
        /// <summary>
        /// Don't use this yourself: use Relocate.
        /// (It should be protected but Domain is internal)
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        internal override TypedValue New(Domain t)
        {
            return new TDateTime(t, value);
        }
        public override int _CompareTo(object obj)
        {
            var that = (TDateTime)obj;
            if (value == that.value)
                return 0;
            if (!value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (!that.value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.Value.CompareTo(that.value.Value);
        }
        public override string ToString()
        {
            return value.ToString();
        }
        internal override long? ToLong()
        {
            return value.Value.Ticks;
        }
        internal override object Val()
        {
            return value;
        }
        public override bool IsNull
        {
            get
            {
                return !value.HasValue;
            }
        }
    }
    internal class TInterval : TypedValue
    {
        internal readonly Interval value;
        internal TInterval(Domain dt, Interval i) : base(dt) { value = i; }
        internal TInterval(Interval i) : this(Domain.Interval, i) { }
        internal override TypedValue New(Domain t)
        {
            return new TInterval(t, value);
        }
        /// <summary>
        /// Fake: Intervals are strictly speaking not comparable
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override int _CompareTo(object obj)
        {
            var that = (TInterval)obj;
            if (value == that.value)
                return 0;
            if (value == null)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (that.value == null)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.ToString().CompareTo(that.value.ToString());
        }
        internal override object Val()
        {
            return value;
        }
        public override string ToString()
        {
            return value.ToString();
        }
        public override bool IsNull
        {
            get
            {
                return value==null;
            }
        }
    }
    internal class TTimeSpan : TypedValue
    {
        internal readonly TimeSpan? value;
        internal TTimeSpan(Domain dt, TimeSpan? t) : base(dt) { value = t; }
        internal TTimeSpan(TimeSpan t) : this(Domain.Timespan, t) { }
        internal override TypedValue New(Domain t)
        {
            return new TTimeSpan(t, value);
        }
        public override int _CompareTo(object obj)
        {
            var that = (TTimeSpan)obj;
            if (value == that.value)
                return 0;
            if (!value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (!that.value.HasValue)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return value.Value.CompareTo(that.value.Value);
        }
        internal override object Val()
        {
            return value;
        }
        public override string ToString()
        {
            return value.ToString();
        }
        public override bool IsNull
        {
            get
            {
                return !value.HasValue;
            }
        }
    }
    /// <summary>
    /// This is really part of the implementation of multi-level indexes (MTree etc)
    /// </summary>
    internal class TMTree: TypedValue
    {
        internal MTree value;
        internal TMTree(MTree m) : base(Domain.MTree) { value = m; }
        internal override TypedValue New(Domain t)
        {
            return this; // approximate: use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TMTree((MTree)value.Relocate(cx));
        }
        internal override object Val()
        {
            return value;
        }
        public override int _CompareTo(object obj)
        {
            var that = (TMTree)obj;
            if (value == that.value)
                return 0;
            if (value == null)
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (that.value == null)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            var e = value.First();
            var f = that.value.First();
            for (;e!=null &&f!=null;e=e.Next(),f=f.Next())
            {
                var c = e.key()._CompareTo(f.key());
                if (c != 0)
                    return c;
                c = e.Value().Value.CompareTo(f.Value().Value);
                if (c != 0)
                    return c;
            }
            return (e!=null) ? 1 : (f!=null) ? -1 : 0;
        }
        public override bool IsNull
        {
            get
            {
                return value==null;
            }
        }
    }
    /// <summary>
    /// This is also part of the implementation of multi-level indexes (MTree etc)
    /// </summary>
    internal class TPartial : TypedValue
    {
        internal BTree<long, bool> value;
        internal TPartial(BTree<long, bool> t) : base(Domain.Partial) { value = t; }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TPartial(cx.Fix(value));
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TPartial(wr.Fix(value));
        }
        internal override object Val()
        {
            return value;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        public override bool IsNull
        {
            get
            {
                return value==null;
            }
        }
    }
    internal class TArray : TypedValue
    {
        internal readonly CList<TypedValue> list; 
        internal TArray(Domain dt, params TypedValue[] a) : base(dt) 
        { 
            var ts = CList<TypedValue>.Empty;
            foreach (var x in a)
                ts += x;
            list = ts;
        }
        internal TArray(Domain dt, CList<TypedValue> a) : base(dt) { list = a; }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TArray((Domain)dataType._Relocate(cx),
                cx.Fix(list));
        }
        internal override object Val()
        {
            return list;
        }
        public static TArray operator+(TArray ar,TypedValue v)
        {
            if (!ar.dataType.elType.CanTakeValueOf(v.dataType))
                throw new DBException("22005", ar.dataType.elType, v.dataType);
            return new TArray(ar.dataType, ar.list + v);
        }
        internal int Length { get { return (int)list.Count; } }
        internal override TypedValue this[string n]
        {
            get
            {
                int i;
                if (int.TryParse(n, out i))
                    return this[i];
                return null;
            }
            set
            {
                int i;
                if (int.TryParse(n, out i))
                    this[i] = value;
            }
        }
        public override int _CompareTo(object obj)
        {
            var that = (TArray)obj;
            if (list == null || list.Count == 0)
            {
                if (that.list == null || that.list.Count == 0)
                    return 0;
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            }
            if (that.list == null || that.list.Count==0)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            int j = 0;
            for(;j<list.Count&&j<that.list.Count;j++)
            {
                var a = list[j];
                var b = that.list[j];
                if (a == null && b == null)
                    continue;
                if (a == null)
                    return -1;
                if (b == null)
                    return 1;
                var c = a._CompareTo(b);
                if (c != 0)
                    return c;
            }
            if (j < list.Count)
                return 1;
            if (j < that.list.Count)
                return -1;
            return 0;
        }
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
            sb.Append("]");
            return sb.ToString();
        }
        public override bool IsNull
        {
            get
            {
                return list==null || list.Count==0;
            }
        }
    }
    internal class TTypeSpec : TypedValue
    {
        Domain _dataType;
        internal TTypeSpec(Domain t) : base(Domain.TypeSpec)
        {
            _dataType = t;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TTypeSpec((Domain)_dataType._Relocate(cx));
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TTypeSpec((Domain)_dataType._Relocate(wr));
        }
        internal override object Val()
        {
            return _dataType;
        }
        public override int _CompareTo(object obj)
        {
            var that = (TTypeSpec)obj;
            if (_dataType == that._dataType)
                return 0;
            if (_dataType == null)
                return (_dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            if (that._dataType == null)
                return (_dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            return _dataType.CompareTo(that._dataType);
        }
        public override bool IsNull
        {
            get
            {
                return _dataType==null;
            }
        }
    }
    internal class TLevel : TypedValue
    {
        Level val;
        public static TLevel D = new TLevel(Level.D);

        public override bool IsNull => false;

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
        public override int _CompareTo(object obj)
        {
            var that = (TLevel)obj;
            if (val.Equals(that.val))
                return 0;
            if (val == null && that.val == null)
                return 0;
            if (val == null)
                return -1;
            if (that.val == null)
                return 1;
            return val.CompareTo(that.val);
        }
        internal override object Val()
        {
            return val;
        }
        public override string ToString()
        {
            return val.ToString();
        }
    }
    internal class TBlob : TypedValue
    {
        internal byte[] value;
        internal TBlob(Domain dt, byte[] b) : base(dt) { value = b; }
        internal TBlob(byte[] b) : this(Domain.Blob, b) { }
        internal override TypedValue New(Domain t)
        {
            return new TBlob(t,value);
        }
        internal override object Val()
        {
            return value;
        }
        public override int _CompareTo(object obj)
        {
            var that = (TBlob)obj;
            if (value==null || value.Length==0)
            {
                if (that.value == null || that.value.Length == 0)
                    return 0;
                return (dataType.nulls == Sqlx.LAST) ? 1 : -1; 
            }
            if (that.value == null || that.value.Length == 0)
                return (dataType.nulls == Sqlx.LAST) ? -1 : 1; 
            var m = value.Length;
            var n = that.value.Length;
            for (int i=0;i<n && i<m;i++)
            {
                var c = value[i].CompareTo(that.value[i]);
                if (c != 0)
                    return c;
            }
            return (m < n) ? -1 : (m > n) ? 1 : 0;
        }
        public override bool IsNull
        {
            get
            {
                return value==null || value.Length==0;
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("byte[");
            if (value!=null)
            sb.Append(value.Length);
            sb.Append("]");
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
                sb.Append("'");
            }
            return sb.ToString();
        }
    }
    internal class TPeriod : TypedValue
    {
        internal Period value;
        internal TPeriod(Domain dt, Period p) : base(dt) { value = p; }
        internal override TypedValue New(Domain t)
        {
            return new TPeriod(t, value);
        }
        internal override object Val()
        {
            return value;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        public override bool IsNull
        {
            get
            {
                return value==null;
            }
        }
    }
    /// <summary>
    /// A row-version cookie
    /// </summary>
    internal class TRvv : TypedValue
    {
        internal Rvv value;
        internal TRvv(Rvv ck) : base (Domain.Char)
        { value = ck; }
        internal override TypedValue New(Domain t)
        {
            return this;
        }
        internal override object Val()
        {
            return value;
        }
        public override bool IsNull
        {
            get { return false; }
        }
        public override int _CompareTo(object obj)
        {
            var that = obj as TRvv;
            return value.off.CompareTo(that.value.off);
        }
        public override string ToString()
        {
            return value.ToString();
        }
    }
    /// <summary>
    /// Column is a convenience class for named values: has a name, segpos, data type, value.
    /// Should really only be used in system tables and Documents. Should not be stored anywhere.
    /// </summary>
    internal sealed class Column
    {
        internal readonly string name;
        internal readonly long segpos;
        internal readonly TypedValue typedValue;
        internal Column(string n, long p, Domain d)
        { name = n; segpos = p;  typedValue = d.defaultValue;  }
        internal Column(string n, long p, TypedValue tv)
        { name = n; segpos = p;  typedValue = tv; }
        internal Domain DataType { get { return typedValue.dataType; } }
        internal object Val() { return typedValue.Val(); }
        public override string ToString()
        {
            if (typedValue == null)
                return "<null>";
            if (typedValue.dataType.kind == Sqlx.CHAR)
                return "\"" + typedValue.ToString() + "\"";
            return typedValue.ToString();
        }
    }
    /// This primitive form of TRow is a linked=list suitable for MTree indexes
    /// </summary>
    internal class PRow : ITypedValue
    {
        internal readonly TypedValue _head;
        internal readonly PRow _tail;
        public PRow(TypedValue head, PRow tail = null)
        {
            _head = head; _tail = tail;
        }
        PRow(TRow r,int off,int len)
        {
            _head = r[off];
            _tail = (off < len - 1) ? new PRow(r, off + 1, len) : null;
        }
        public PRow(TRow r) : this(r, 0, r.Length)
        { }
        public PRow(BList<TypedValue> v,int off=0)
        {
            if (off < v.Count)
                _head = v[off];
            else
                _head = null;
            _tail = null;
            if (++off < v.Count)
                _tail = new PRow(v,off);
        }
        public PRow(int off, params Column[] v)
        {
            if (off < v.Length)
                _head = v[off].typedValue;
            else
                _head = null;
            _tail = null;
            if (++off < v.Length)
                _tail = new PRow(off, v);
        }
        internal PRow Reverse()
        {
            PRow r = null;
            for (var i = Length - 1; i >= 0; i--)
                r = new PRow(this[i], r);
            return r;
        }
        public TypedValue this[int i]
        {
            get { if (i == 0) return _head; return _tail?[i - 1]; }
        }
        public int Length { get { if (_tail == null) return 1; return 1 + _tail.Length; } }
        public int _CompareTo(object ob)
        {
            var p = ob as PRow;
            if (_head == null)
                return -1;
            if (p == null)
                return 1;
            var c = _head.CompareTo(p._head);
            if (c != 0)
                return c;
            if (_tail != null)
                return _tail._CompareTo(p._tail);
            return (p._tail == null) ? 0 : -1;
        }
        public bool IsNull { get { return false; } }
        public static PRow Key(BList<long> cols,BTree<long,TypedValue> vals)
        {
            PRow r = null;
            for (var b = cols?.Last(); b != null; b = b.Previous())
                r = new PRow(vals[b.value()], r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "(";
            for (var r = this; r != null && r._head!=null; r = r._tail)
            {
                sb.Append(cm); cm = ",";
                if (r._head != TNull.Value)
                    sb.Append(r._head.ToString());
            }
            sb.Append(")");
            return sb.ToString();
        }

        public int CompareTo(object obj)
        {
            PRow that = obj as PRow;
            if (that == null)
                return 1;
            if (_head == null && that._head == null)
                return 0;
            if (_head == null)
                return -1;
            if (that._head == null)
                return 1;
            var c = _head.CompareTo(that._head);
            if (c != 0)
                return c;
            if (_tail == null)
                return (that._tail != null) ? -1 : 0;
            return _tail.CompareTo(that?._tail);
        }
    }
    /// <summary>
    /// All TypedValues have a domain; TRow also has a BList of uids to give the columns ordering
    /// and a tree of values indexed by uid.
    /// Cursor and RowBookmark are TRows, and rows can be assigned to SqlValues if the columns
    /// match (not the uids).
    /// If the columns don't match then a map is required.
    /// </summary>
    internal class TRow : TypedValue
    {
        internal readonly BTree<long, TypedValue> values;
        internal readonly int docCol = -1;  // note the position of a single document column if present
        internal int Length => dataType.Length;
        internal CList<long> columns => dataType.rowType;
        public TRow(Domain dt,BTree<long,TypedValue> vs)
            :base(dt)
        {
            values = vs;
        }
        /// <summary>
        /// The following two methods assume that the given value list is from a RowSet source,
        /// and delivers the corresponding TRow for the rowSet.
        /// </summary>
        /// <param name="oi"></param>
        /// <param name="map"></param>
        /// <param name="vs"></param>
        public TRow(Domain dt, BTree<long, RowSet.Finder> map, BTree<long, TypedValue> vs)
            : base(dt)
        {
            var v = BTree<long, TypedValue>.Empty;
            for (var b = map.First(); b != null; b = b.Next())
                v += (b.key(), vs[b.value().col] ?? TNull.Value);
            values = v;
        }
        public TRow(RowSet rs, TRow rw) : base(rs.domain) 
        {
            var v = BTree<long, TypedValue>.Empty;
            for (var b = rs.domain.rowType.First(); b != null; b = b.Next())
                v += (b.value(), rw[b.key()] ?? TNull.Value);
            values = v;
        }
        public TRow(ObInfo oi, TRow rw) : base(oi.domain)
        {
            var v = BTree<long, TypedValue>.Empty;
            for (var b = oi.domain.rowType.First(); b != null; b = b.Next())
                v += (b.value(), rw[b.key()] ?? TNull.Value);
            values = v;
        }
        public TRow(SqlValue sv, BTree<long, TypedValue> vs) : this(sv.domain, vs) { }
        public TRow(ObInfo oi, BTree<long, TypedValue> vs) : this(oi.domain, vs) { }
        /// <summary>
        /// Constructor: values by columns
        /// </summary>
         /// <param name="v">The values</param>
        public TRow(Domain dt, params TypedValue[] v) : 
            base(dt)
        {
            var vals = BTree<long, TypedValue>.Empty;
            var i = 0;
            for (var b = dt.rowType.First(); i < v.Length && b != null; b = b.Next(), i++)
            {
                var p = b.value();
                vals += (p, v[i] ?? dt.representation[p].defaultValue);
            }
            values = vals;
        }
        public TRow(RowSet rs, params TypedValue[] v) : this(rs.domain, v) { }
        public TRow(ObInfo oi, params TypedValue[] v) : this(oi.domain, v) { }
        internal TRow(Domain dm, PRow v) : base(dm)
        {
            var vals = BTree<long, TypedValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next(), v=v._tail)
            {
                var p = b.value();
                vals += (p, v._head);
            }
            values = vals;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TRow((Domain)dataType._Relocate(cx),
                cx.Fix(values));
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TRow((Domain)dataType._Relocate(wr),
                wr.Fix(values));
        }
        public static TRow operator+(TRow rw,(long,TypedValue)x)
        {
            return new TRow(rw.dataType, rw.values + x);
        }
        internal override object Val()
        {
            return this;
        }
        internal override TypedValue this[long n] => values[n];
        internal TypedValue this[BTree<long,bool> c] => values[c];
        internal TypedValue this[int i]
        {
            get {
                if (columns != null)
                    return values[columns[i]];
                var j = 1;
                for (var b = dataType.representation.First(); i >= j && b != null; b = b.Next(), j++)
                    if (i == j)
                        return values[b.key()];
                return null;
            }
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
        public override int _CompareTo(object obj)
        {
            var that = (TRow)obj;
            if (Length != that.Length)
                goto bad;
            try
            {
                for (int i = 0; i < Length; i++)
                {
                    var c = this[i]?.CompareTo(that[i]) ?? -1;
                    if (c != 0)
                        return c;
                }
                return 0;
            }
            catch { }
        bad:
            throw new DBException("22000").ISO();
        }
        internal override TypedValue[] ToArray()
        {
            var r = new TypedValue[Length];
            for (int i = 0; i < Length; i++)
                r[i] = this[i];
            return r;
        }
        public override bool IsNull
        {
            get 
            {
                for (var b = values.First(); b != null; b = b.Next())
                {
                    var v = b.value();
                    if (v != null && !v.IsNull)
                        return false;
                }
                return true;
            }
        }
    }
    /// <summary>
    /// A Multiset can be placed in a cell of a Table, so is treated as a value type.
    /// Operations of UNION and INTERSECT etc are defined on Multisets.
    /// </summary>
    internal class TMultiset : TypedValue
    {
        /// <summary>
        /// Implement the multiset as a tree whose key is V and whose value is long 
        /// (the multiplicity of the key V as a member of the multiset).
        /// While this looks like MTree (which is TypedValue[] to long) it doesn't work the same way
        /// </summary>
        internal CTree<TypedValue,long?> tree; 
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
        /// <param name="et">The element type</param>
        internal TMultiset(Context cx,Domain et) : base (new Domain(Sqlx.MULTISET,et))
        {
            tree = CTree<TypedValue,long?>.Empty;
            count = 0;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(TMultiset tm) : base(tm.dataType)
        {
            tree = CTree<TypedValue, long?>.Empty;
            count = tm.count;
            // Disallow not Allow for duplicates (see below)
        }
        internal TMultiset(Domain dt) : base(dt) { }
        internal TMultiset(Domain dt,CTree<TypedValue,long?>t,long ct) :base(dt)
        {
            tree = t; count = ct;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TMultiset((Domain)dataType._Relocate(cx),
                cx.Fix(tree),count);
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TMultiset((Domain)dataType._Relocate(wr),
                wr.Fix(tree),count);
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
                long o = tree[a].Value;
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
        internal MultisetBookmark First()
        {
            return new MultisetBookmark(this);
        }
        internal class MultisetBookmark : IBookmark<TypedValue>
        {
            readonly TMultiset _set;
            readonly ABookmark<TypedValue, long?> _bmk;
            readonly long _pos;
            readonly long? _rep;
            internal MultisetBookmark(TMultiset set, long pos = 0, ABookmark<TypedValue, long?> bmk = null, long? rep = null)
            {
                _set = set; _pos = pos; _bmk = bmk; _rep = rep;
            }
            public MultisetBookmark Next()
            {
                var bmk = _bmk;
                var rep = _rep;
                for (; ; )
                {
                    if (rep.HasValue)
                    {
                        var rp = _rep.Value;
                        if (rp > 0)
                            return new MultisetBookmark(_set, _pos + 1, bmk, rp - 1);
                    }
                    bmk = ABookmark<TypedValue, long?>.Next(bmk, _set.tree);
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

            IBookmark<TypedValue> IBookmark<TypedValue>.Next()
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
            TMultiset m = new TMultiset(this);
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
        internal static TMultiset Union(TMultiset a, TMultiset b, bool all)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Null)
                ae = be;
            if (be != Domain.Null && be != ae)
                throw new DBException("22105").Mix();
            TMultiset r = new TMultiset(a);
            if (all)
            {
                r.tree = a.tree;
                r.count = a.count;
                for (var d=b.tree.First();d!=null;d=d.Next())
                    if (d.value()!=null)
                    r.Add(d.key(), d.value().Value);
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
        internal static TMultiset Intersect(TMultiset a, TMultiset b, bool all)
        {
            if (a == null || b == null)
                return null;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae == Domain.Null)
                ae = be;
            if (be != Domain.Null && be != ae)
                throw new DBException("22105").Mix();
            TMultiset r = new TMultiset(a);
            for(var d = a.tree.First();d!=null;d=d.Next())
            {
                TypedValue v = d.key();
                object o = b.tree[v];
                if (d.value()==null || o == null)
                    continue;
                if (all)
                {
                    long m = d.value().Value;
                    long n = (long)o;
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
            Domain tp = a.dataType.elType;
            TMultiset r = new TMultiset(a.dataType);
            for (var d = a.tree.First(); d != null; d = d.Next())
                if (d.value() != null)
                {
                    TypedValue v = d.key();
                    long m = d.value().Value;
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
                if (b.value() != null)
                {
                    if (!first)
                        str += ",";
                    for (var i = 0; i < b.value().Value; i++)
                        str += b.key().ToString();
                }
            return str + ")";
        }
        public override int _CompareTo(object obj)
        {
            var that = (TMultiset)obj;
            if (dataType.kind != that.dataType.kind)
                throw new DBException("22000").ISO();
            var e = tree.First();
            var f = that.tree.First();
            var et = dataType.elType;
            for (; e!=null && f!=null; e = e.Next(), f = f.Next())
            {
                var c = et.Compare(e.key(),f.key());
                if (c != 0)
                    return c;
            }
            return (e!=null) ? 1 : (f!=null) ? -1 : 0;
        }
        internal override object Val()
        {
            return tree;
        }
        public override bool IsNull
        {
            get { return tree == null || tree.Count == 0; }
        }
    }
    /// <summary>
    /// The model for Xml values is that element ordering and repetition is important,
    /// but attributes are not ordered and have unique neames.
    /// </summary>
    internal class TXml : TypedValue
    {
        internal readonly string name;
        internal readonly BTree<string, TypedValue> attributes = BTree<string, TypedValue>.Empty;
        internal readonly string content = "";
        internal readonly BList<TXml> children = BList<TXml>.Empty;
        internal static TXml Null = new TXml(null);
        internal TXml(string n) : base(Domain.XML) { name = n; }
        TXml(string n,BTree<string,TypedValue>a,string c,BList<TXml> ch)
            : base(Domain.XML)
        {
            name = n; attributes = a; content = c; children = ch;
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException(); // use Relocate
        }
        internal override TypedValue Relocate(Context cx)
        {
            return new TXml(name,cx.Fix(attributes),content,cx.Fix(children));
        }
        internal override TypedValue Relocate(Writer wr)
        {
            return new TXml(name, wr.Fix(attributes), content, wr.Fix(children));
        }
        internal override object Val()
        {
            return ToString();
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
        public override int _CompareTo(object obj)
        {
            return ToString().CompareTo(((TypedValue)obj).ToString());
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
                sb.Append(">");
                for (var b=children.First();b!=null;b=b.Next())
                    sb.Append(b.value().ToString());
                sb.Append("</" + name + ">");
            }
            else
                sb.Append("/>");
            return sb.ToString();
        }
        public override bool IsNull
        {
            get { return this==Null;  }
        }
    }
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
    internal enum OrderCategory { None = 0, Equals = 1, Full = 2, Relative = 4, Map = 8, State = 16 };
}
