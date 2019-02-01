using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
#nullable enable
namespace Shareable
{
    public enum Types
    {
        Serialisable = 0,
        STimestamp = 1, // unused
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
        SSearch = 18,
        SBegin = 19,
        SRollback = 20,
        SCommit = 21,
        SCreateTable = 22,
        SCreateIndex = 23,
        SUpdateSearch = 24,
        SDeleteSearch = 25,
        SAlterStatement = 26,
        SDropStatement = 27,
        SInsert = 28,
        SSelect = 29,
        EoF = 30,
        Get = 31,
        Insert = 32,
        Read = 33,
        Done = 34,
        Exception = 35,
        SExpression = 36,
        SFunction = 37,
        SValues = 38,
        SOrder = 39,
        SBigInt = 40,
        SInPredicate = 41,
        DescribedGet = 42,
        SGroupQuery = 43,
        STableExp = 44,
        SAliasedTable = 45,
        SJoin = 46
    }
    public interface ILookup<K,V> where K:IComparable
    {
        bool defines(K s);
        V this[K s] { get; }
    }
    public class Context 
    {
        public readonly ILookup<string, Serialisable> head;
        public readonly ILookup<long, Serialisable> ags;
        public readonly Context? next;
        public static readonly Context Empty =
             new Context(SDict<string, Serialisable>.Empty, SDict<long, Serialisable>.Empty, null); 
        public Context(ILookup<string, Serialisable> h, ILookup<long, Serialisable> a, Context? n)
        {
            head = h; ags = a; next = n;
        }
        public Context(RowBookmark b,Context? c)
        {
            head = b; ags = b._ags; next = c;
        }
        public Context(ILookup<string, Serialisable> h, Context? n)
        {
            head = h; ags = SDict<long,Serialisable>.Empty; next = n;
        }
        public Context(ILookup<long, Serialisable> a, Context? n)
        {
            head = SDict<string, Serialisable>.Empty; ags = a; next = n;
        }
        public Serialisable this[string s] => (this==Empty)?Serialisable.Null:
            head.defines(s)?head[s]:next?[s]??Serialisable.Null;
        public Serialisable this[long f] => (this == Empty) ? Serialisable.Null :
            ags.defines(f) ? ags[f] : next?[f] ?? Serialisable.Null;
        public bool defines(string s)
        {
            return (this==Empty) ? false : head.defines(s) || (next?.defines(s) ?? false);
        }
        public bool defines(long f)
        {
            return (this == Empty) ? false : ags.defines(f) || (next?.defines(f) ?? false);
        }
    }
    public class Serialisable:IComparable
    {
        public readonly Types type;
        public readonly static Serialisable Null = new Serialisable(Types.Serialisable);
        public Serialisable(Types t)
        {
            type = t;
        }
        public Serialisable(Types t, Reader f)
        {
            type = t;
        }
        public static Serialisable Get(Reader f)
        {
            return Null;
        }
        public virtual string Alias(int n)
        {
            return "col" + n;
        }
        public virtual SDict<long,SFunction> Aggregates(SDict<long,SFunction> a,Context cx)
        {
            return a;
        }
        public virtual bool isValue => true;
        public virtual void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
        }
        public virtual bool Conflicts(Serialisable that)
        {
            return false;
        }
        public virtual void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append(this);
        }
        public virtual int CompareTo(object ob)
        {
            return (ob == Null) ? 0 : -1;
        }
        public static string DataTypeName(Types t)
        {
            switch(t)
            {
                case Types.SInteger: return "integer";
                case Types.SString: return "string";
                case Types.SNumeric: return "numeric";
                case Types.SBoolean: return "boolean";
                case Types.SDate: return "date";
                case Types.STimeSpan: return "timespan";
      //          case Types.STimestamp: return "timestamp";
            }
            throw new Exception("Unknown data type");
        }
        public static Serialisable New(Types t,object v)
        {
            switch (t)
            {
                case Types.SString: return new SString((string)v);
                case Types.SInteger: return new SInteger((int)v);
            }
            return Null;
        }
        /// <summary>
        /// For aggregation functions
        /// </summary>
        /// <param name="rb"></param>
        /// <returns></returns>
        public virtual Serialisable StartCounter(Serialisable v)
        {
            return Null;
        }
        public virtual Serialisable AddIn(Serialisable a,Serialisable v)
        {
            return a;
        }
        /// <summary>
        /// During Analysis:
        /// We have been mentioned in a Serialisable. We might be able to improve it using a known list
        /// of selectors.
        /// After Analysis:
        /// The names argument is a set of Values.
        /// </summary>
        /// <param name="nms">The information for associating strings to SColumns</param>
        /// <returns></returns>
        public virtual Serialisable Lookup(Context cx)
        {
            return this;
        }
        public virtual Serialisable this[string col]
        { get { throw new NotImplementedException(); } }
        public override string ToString()
        {
            return "Serialisable " +type;
        }
    }
    public class SExpression : Serialisable
    {
        public readonly Serialisable left, right;
        public readonly Op op;
        public SExpression(SDatabase db,Serialisable lf,Op o,Reader f) : base(Types.SExpression)
        {
            left = lf;
            op = o;
            right = f._Get(db);
        }
        public SExpression(Serialisable lf,Op o,Serialisable rt) : base(Types.SExpression)
        {
            left = lf; right = rt; op = o;
        }
        public override bool isValue => false;
        public enum Op { Plus, Minus, Times, Divide, Eql, NotEql, Lss, Leq, Gtr, Geq, Dot, And, Or, UMinus, Not };
        internal static SExpression Get(SDatabase db,Reader f)
        {
            var lf = f._Get(db);
            return new SExpression(db, lf, (Op)f.ReadByte(), f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            left.Put(f);
            f.WriteByte((byte)op);
            right.Put(f);
        }
        public override SDict<long,SFunction> Aggregates(SDict<long,SFunction> ags,Context cx)
        {
            if (left != null)
                ags = left.Aggregates(ags, cx);
            if (right != null)
                ags = right.Aggregates(ags, cx);
            return ags;
        }
        public override Serialisable Lookup(Context cx)
        {
            if (op == Op.Dot)
            {
                if (cx.head is RowBookmark rb)
                {
                    var ls = ((SString)left).str;
                    if (!cx.defines(ls))
                        return this;
                    var rs = ((SString)right).str;
                    if (ls.CompareTo(rb._rs._qry.Alias) == 0)
                        return cx.defines(rs)?cx[rs]:this;
                    var rw = (SRow)rb._ob[ls];
                    return rw.defines(rs)?rw.vals[rs].Lookup(cx):Null;
                }
                return this;
            }
            var lf = left.Lookup(cx);
            var rg = right.Lookup(cx);
            if (!(lf.isValue && rg.isValue))
                return new SExpression(lf, op, rg);
            switch (op)
            {
                 case Op.Plus:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv + ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv + getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) + ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv + ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv + getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) + ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv + new Numeric(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SNumeric(lv + new Numeric(getbig(rg), 0));
                                        case Types.SNumeric:
                                            return new SNumeric(lv + ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Minus:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv - ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv - getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) - ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv - ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv - getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) - ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv - new Numeric(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SNumeric(lv - new Numeric(getbig(rg), 0));
                                        case Types.SNumeric:
                                            return new SNumeric(lv - ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Times:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv * ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv * getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) * ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv * ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv * getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) * ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv * new Numeric(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SNumeric(lv * new Numeric(getbig(rg), 0));
                                        case Types.SNumeric:
                                            return new SNumeric(lv * ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Divide:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv / ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv / getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Numeric(new Integer(lv), 0) / ((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv / ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv / getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Numeric(new Integer(lv), 0) / ((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(new Numeric(lv / new Numeric(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SNumeric(new Numeric(lv / new Numeric(getbig(rg), 0)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv / ((SNumeric)rg).num));
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Eql:
                case Op.NotEql:
                case Op.Gtr:
                case Op.Geq:
                case Op.Leq:
                case Op.Lss:
                    {
                        int c = 0;
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.CompareTo(((SInteger)rg).value); break;
                                        case Types.SBigInt:
                                            c = lv.CompareTo(getbig(rg)); break;
                                        case Types.SNumeric:
                                            c = new Numeric(new Integer(lv), 0).CompareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.CompareTo(((SInteger)rg).value); break;
                                        case Types.SBigInt:
                                            c = lv.CompareTo(getbig(rg)); break;
                                        case Types.SNumeric:
                                            c = new Numeric(new Integer(lv), 0).CompareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.CompareTo(new Numeric(((SInteger)rg).value)); break;
                                        case Types.SBigInt:
                                            c = lv.CompareTo(new Numeric(getbig(rg), 0)); break;
                                        case Types.SNumeric:
                                            c = lv.CompareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                        }
                        bool r = true;
                        switch (op)
                        {
                            case Op.Eql: r = c == 0; break;
                            case Op.NotEql: r = c != 0; break;
                            case Op.Leq: r = c <= 0; break;
                            case Op.Lss: r = c < 0; break;
                            case Op.Geq: r = c >= 0; break;
                            case Op.Gtr: r = c > 0; break;
                        }
                        return SBoolean.For(r);
                    }
                case Op.UMinus:
                    switch (left.type)
                    {
                        case Types.SInteger: return new SInteger(-((SInteger)left).value);
                        case Types.SBigInt: return new SInteger(-getbig(lf));
                        case Types.SNumeric: return new SNumeric(-((SNumeric)left).num);
                    }
                    break;
                case Op.Not:
                    {
                        if (lf is SBoolean lb) return For(!lb.sbool);
                        break;
                    }
                case Op.And:
                    {
                        if (lf is SBoolean lb && rg is SBoolean rb)
                            return For(lb.sbool && rb.sbool);
                        break;
                    }
                case Op.Or:
                    {
                        if (lf is SBoolean lb && rg is SBoolean rb)
                            return For(lb.sbool|| rb.sbool);
                        break;
                    }
                case Op.Dot:
                    {
                        var ls = ((SString)left).str;
                        var a = cx.defines(ls) ? cx[ls]  : left;
                        return a.Lookup(cx);
                    }
            }
            throw new Exception("Bad computation");
        }
        Integer getbig(Serialisable x)
        {
            return ((SInteger)x).big ?? throw new Exception("No Value?");
        }
        SBoolean For(bool v)
        {
            return v ? SBoolean.True : SBoolean.False;
        }
    }
    public class SFunction : Serialisable
    {
        public readonly Serialisable arg; // probably an SQuery
        public readonly Func func;
        static long _fid = 0;
        public readonly long fid = ++_fid; // we will have a list of function expressions
        public SFunction(SDatabase db,Func fn,Reader f) : base(Types.SFunction)
        {
            func = fn;
            arg = f._Get(db);
        }
        public SFunction(Func fn, Serialisable a) : base(Types.SFunction)
        {
            func = fn;
            arg = a;
        }
        public override bool isValue => false;
        public enum Func { Sum, Count, Max, Min, Null };
        internal static SFunction Get(SDatabase db,Reader f)
        {
            return new SFunction(db, (Func)f.ReadByte(), f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)func);
            arg.Put(f);
        }
        public bool IsAgg => (func!=Func.Null);
        public override SDict<long, SFunction> Aggregates(SDict<long, SFunction> a, Context cx)
        {
            return IsAgg? a + (fid, this) : a;
        }
        public override Serialisable Lookup(Context cx)
        {
            if (cx.ags==SDict<long,Serialisable>.Empty)
                return this;
            var x = arg.Lookup(cx);
            if (func == Func.Null)
                return SBoolean.For(x == Null);
            return cx.defines(fid) ? cx[fid] : Null;
        }
        public override Serialisable StartCounter(Serialisable v)
        {
            switch (func)
            {
                case Func.Count:
                    return SInteger.One;
                case Func.Max:
                case Func.Min:
                case Func.Sum:
                    return v;
            }
            return Null;
        }
        public override Serialisable AddIn(Serialisable a,Serialisable v)
        {
            switch (func)
            {
                case Func.Count:
                    return new SInteger(((SInteger)a).value + 1);
                case Func.Max:
                    return (a.CompareTo(v) > 0) ? a : v;
                case Func.Min:
                    return (a.CompareTo(v) < 0) ? a : v;
                case Func.Sum:
                    switch (a.type)
                    {
                        case Types.SInteger:
                            {
                                var lv = ((SInteger)a).value;
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SInteger(lv + ((SInteger)v).value);
                                    case Types.SBigInt:
                                        return new SInteger(lv + getbig(v));
                                    case Types.SNumeric:
                                        return new SNumeric(new Numeric(new Integer(lv), 0) + ((SNumeric)v).num);
                                }
                                break;
                            }
                        case Types.SBigInt:
                            {
                                var lv = getbig(a);
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SInteger(lv + ((SInteger)v).value);
                                    case Types.SBigInt:
                                        return new SInteger(lv + getbig(v));
                                    case Types.SNumeric:
                                        return new SNumeric(new Numeric(new Integer(lv), 0) + ((SNumeric)v).num);
                                }
                                break;
                            }
                        case Types.SNumeric:
                            {
                                var lv = ((SNumeric)a).num;
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SNumeric(lv + new Numeric(((SInteger)v).value));
                                    case Types.SBigInt:
                                        return new SNumeric(lv + new Numeric(getbig(v), 0));
                                    case Types.SNumeric:
                                        return new SNumeric(lv + ((SNumeric)v).num);
                                }
                                break;
                            }

                    }
                    break;
            }
            return v;
        }
        Integer getbig(Serialisable x)
        {
            return ((SInteger)x).big ?? throw new Exception("No Value?");
        }
    }
#nullable enable
    public class SInPredicate : Serialisable
    {
        public readonly Serialisable arg;
        public readonly Serialisable list;
        public SInPredicate(Serialisable a,Serialisable r):base(Types.SInPredicate)
        {
            arg = a; list = r;
        }
        public override bool isValue => false;
        public static SInPredicate Get(SDatabase db,Reader f)
        {
            var a = f._Get(db);
            return new SInPredicate(a, f._Get(db));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            arg.Put(f);
            list.Put(f);
        }
        public override Serialisable Lookup(Context cx)
        {
            return new SInPredicate(arg.Lookup(cx), list.Lookup(cx));
        }
    }
/*    public class STimestamp : Serialisable,IComparable
    {
        public readonly long ticks;
        public STimestamp(DateTime t) : base(Types.STimestamp)
        {
            ticks = t.Ticks;
        }
        STimestamp(Reader f) : base(Types.STimestamp,f)
        {
            ticks = f.GetLong();
        }
        public override bool isValue => true;
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(ticks);
        }
        public new static STimestamp Get(Reader f)
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
    } */
    public class SInteger : Serialisable, IComparable
    {
        public readonly int value;
        public readonly Integer? big;
        public static readonly SInteger Zero = new SInteger(0);
        public static readonly SInteger One = new SInteger(1);
        public SInteger(int v) : base(Types.SInteger)
        {
            value = v; big = null;
        }
        public SInteger(Integer b) : base((b<int.MaxValue&&b>int.MinValue)?Types.SInteger:Types.SBigInt)
        {
            if (b < int.MaxValue && b > int.MinValue)
            {
                value = b; big = null;
            }
            else
            {
                value = 0; big = b;
            }
        }
        SInteger(Reader f) : this(f.GetInt())
        {
        }
        public override bool isValue => true;
        public new static Serialisable Get(Reader f)
        {
            return new SInteger(f);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
             sb.Append(big??value);
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
        public readonly Numeric num;
        public SNumeric(Numeric n) :base (Types.SNumeric)
        {
            num = n;
        }
        public SNumeric(long m,int p,int s) : base(Types.SNumeric)
        {
            num = new Numeric(new Integer(m), s, p);
        }
        SNumeric(Reader f) : base(Types.SNumeric, f)
        {
            var mantissa = f.GetInteger();
            var precision = f.GetInt();
            var scale = f.GetInt();
            num = new Numeric(mantissa, scale, precision);
        }
        public override bool isValue => true;
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(num.mantissa);
            f.PutInt(num.precision);
            f.PutInt(num.scale);
        }
        public new static Serialisable Get(Reader f)
        {
            return new SNumeric(f);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append(num.mantissa * Math.Pow(10.0, -num.scale));
        }
        public double ToDouble()
        {
            return 1.0 * num.mantissa * Math.Pow(10.0, num.scale);
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
            return "Numeric " + num.ToString();
        }
    }
    /// <summary>
    /// Care is needed. Because we use ' as a string delimiter
    /// we need to do something when we embed a string with ' .
    /// For now we use the SQL standard's solution of replacing ' by ''
    /// within embedded strings. But serialisation to streams is okay!
    /// We support embedding here in the append routines.
    /// When de-embedding a string, e.g. in StrongLink.Document or 
    /// StrongLink.Parser.Lexer classes, we need to act appropriately on ''
    /// </summary>
    public class SString : Serialisable,IComparable
    {
        public readonly string str; // may contain single '
        public SString(string s) :base (Types.SString)
        {
            str = s;
        }
        SString(Reader f) :base(Types.SString, f)
        {
            str = f.GetString();
        }
        public override bool isValue => true;
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(str);
        }
        public new static Serialisable Get(Reader f)
        {
            return new SString(f);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append("'"); sb.Append(str.Replace("'","''")); sb.Append("'");
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
        public SDate(int y,int m,long r) : base(Types.SDate)
        {
            year = y; month = m; rest = r;
        }
        SDate(Reader f) : base(Types.SDate, f)
        {
            year = f.GetInt();
            month = f.GetInt();
            rest = f.GetLong();
        }
        public override bool isValue => true;
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutLong(rest);
        }
        public new static Serialisable Get(Reader f)
        {
            return new SDate(f);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (SDate)obj;
            var dt = new DateTime(year, month, 1) + new TimeSpan(rest);
            var tdt = new DateTime(that.year, that.month, 1) + new TimeSpan(that.rest);
            return dt.Ticks.CompareTo(tdt.Ticks);
        }
        public override string ToString()
        {
            var dt = new DateTime(year, month, 1) + new TimeSpan(rest);
            var sb = new StringBuilder();
            sb.Append('\''); sb.Append(dt.Year);
            sb.Append('-'); sb.Append(dt.Month.ToString("D2"));
            sb.Append('-'); sb.Append(dt.Day.ToString("D2"));
            if (dt.TimeOfDay.Ticks!=0)
            {
                sb.Append('T'); sb.Append(dt.TimeOfDay);
            }
            sb.Append('\'');
            return sb.ToString();
        }
    }
    public class STimeSpan : Serialisable,IComparable
    {
        public readonly int years;
        public readonly int months;
        public readonly TimeSpan ts;
        public STimeSpan(TimeSpan s) : base(Types.STimeSpan)
        {
            ts = s;
            years = 0;
            months = 0;
        }
        public STimeSpan(int y,int m) :base(Types.STimeSpan)
        {
            years = y;
            months = m;
            ts = new TimeSpan(0);
        }
        STimeSpan(Reader f) : base(Types.STimeSpan, f)
        {
            ts = new TimeSpan(f.GetInteger());
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(ts.Ticks);
        }
        public override bool isValue => true;
        public new static Serialisable Get(Reader f)
        {
            return new STimeSpan(f);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            var that = (STimeSpan)obj;
            var c = years.CompareTo(that.years);
            if (c != 0)
                return c;
            c = months.CompareTo(that.months);
            if (c != 0)
                return c;
            return ts.CompareTo(that.ts);
        }
        public override string ToString()
        {
            if (years != 0 || months!=0)
                return "'" + years + "Y" + months + "M'";
            return "'"+ ts.ToString() + "'";
        }
    }
    public class SBoolean : Serialisable,IComparable
    {
        public readonly bool sbool;
        public static readonly SBoolean True = new SBoolean(true);
        public static readonly SBoolean False = new SBoolean(false);
        SBoolean(bool n) : base(Types.SBoolean)
        {
            sbool = n;
        }
        public override bool isValue => true;
        public new static Serialisable Get(Reader f)
        {
            return For(f.ReadByte() == 1);
        }
        public static SBoolean For(bool r)
        {
            return r ? True : False;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)(sbool?1:0));
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append(this);
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
            return sbool ? "\"true\"" : "\"false\"";
        }
    }
    public class SRow : Serialisable,ILookup<string,Serialisable>
    {
        public readonly SDict<int, string> names;
        public readonly SDict<int, Serialisable> cols;
        public readonly SDict<string, Serialisable> vals;
        public readonly bool isNull;
        public readonly SRecord? rec;
        public SRow() : base(Types.SRow)
        {
            names = SDict<int,string>.Empty;
            cols = SDict<int, Serialisable>.Empty;
            vals = SDict<string, Serialisable>.Empty;
            isNull = true;
            rec = null;
        }
        public override bool isValue => true;
        protected SRow Add(string n, Serialisable v)
        {
            return new SRow(names+(names.Length??0,n),cols+(cols.Length??0,v),
                vals+(n,v),rec);
        }
        public static SRow operator+(SRow s,ValueTuple<string,Serialisable>v)
        {
            return s.Add(v.Item1, v.Item2);
        }
        SRow(SDict<int,string> n,SDict<int,Serialisable> c,SDict<string,Serialisable> v,SRecord? r) 
            :base(Types.SRow)
        {
            names = n;
            cols = c;
            vals = v;
            rec = r;
            isNull = false;
        }
        public SRow(SList<string> a, SList<Serialisable> s) :base(Types.SRow)
        {
            var cn = SDict<int, string>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            var k = 0;
            var isn = true;
            for (;s.Length!=0;s=s.next,a=a.next) // not null
            {
                var n = a.element;
                cn += (k, n);
                r += (k++, s.element);
                vs += (n, s.element);
                if (s.element != Null)
                    isn = false;
            }
            names = cn;
            cols = r;
            vals = vs;
            rec = null;
            isNull = isn;
        }
        SRow(SDatabase d, Reader f) :base(Types.SRow)
        {
            var n = f.GetInt();
            var cn = SDict<int, string>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetString();
                cn += (i, k);
                var v = f._Get(d);
                r += (i, v);
                vs += (k, v);
            }
            names = cn;
            cols = r;
            vals = vs;
            isNull = false;
            rec = null;
        }
        public SRow(SDatabase db,SRecord r) :base(Types.SRow)
        {
            var tb = (STable)db.objects[r.table];
            var cn = SDict<int, string>.Empty;
            var co = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            var k = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SColumn sc)
                {
                    var v = r.fields.Lookup(sc.uid)??Null;
                    co += (k, v);
                    cn += (k++, sc.name);
                    vs += (sc.name, v);
                }
                else
                    throw new Exception("Unimplemented selector");
            names = cn;
            cols = co;
            vals = vs;
            rec = r;
            isNull = false;
        }
        public SRow(SSelectStatement ss, Context cx) : base(Types.SRow)
        {
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            var isn = true;
            var cb = ss.cpos.First();
            for (var b = ss.display.First(); cb != null && b != null; b = b.Next(), cb = cb.Next())
            {
                var v = cb.Value.Item2.Lookup(cx) ?? Null;
                if (v is SRow sr && sr.cols.Length == 1)
                    v = sr.cols.Lookup(0) ?? Null;
                if (v != null)
                {
                    r += (b.Value.Item1, v);
                    vs += (b.Value.Item2, v);
                }
                if (v != Null)
                    isn = false;
            }
            names = ss.display;
            cols = r;
            vals = vs;
            rec = ((RowBookmark)cx.head)._ob.rec;
            isNull = isn;
        }
        public static SRow Get(SDatabase d,Reader f)
        {
            return new SRow(d,f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(names.Length);
            var cb = cols.First();
            for (var b = names.First(); cb!=null && b != null; b = b.Next(),cb=cb.Next())
            {
                f.PutString(b.Value.Item2);
                if (cb.Value.Item2 is Serialisable s)
                    s.Put(f);
                else
                    Null.Put(f);
            }
        }
        public override Serialisable this[string col] => vals.Lookup(col);
        public Serialisable this[int col] => cols.Lookup(col);
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append('{');
            var cm = "";
            var nb = names.First();
            for (var b = cols.First(); nb!=null && b != null; b = b.Next(),nb=nb.Next())
                if (b.Value.Item2 is Serialisable s && s!=Null)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(nb.Value.Item2);
                    sb.Append(":");
                    s.Append(db, sb);
                }
            sb.Append("}");
        }
        public override Serialisable Lookup(Context cx)
        {
            var v = SDict<int, Serialisable>.Empty;
            var r = SDict<string, Serialisable>.Empty;
            var nb = names.First();
            for (var b = cols.First(); nb != null && b != null; nb = nb.Next(), b = b.Next())
            {
                var e = b.Value.Item2.Lookup(cx);
                v += (b.Value.Item1, e);
                r += (nb.Value.Item2, e);
            }
            return new SRow(names, v, r, (cx.head as RowBookmark)?._ob.rec);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("SRow ");
            Append(null,sb);
            return sb.ToString();
        }

        public bool defines(string s)
        {
            return vals.Contains(s);
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
        /// Transaction-local uids: 0x4000000000000000-0x7fffffffffffffff
        /// System uids (_Log tables etc): 0x9000000000000000-0x8000000000000001
        /// Client session-local objects are given negative uids with default -1
        /// </summary>
        public readonly long uid;
        /// <summary>
        /// For system tables and columns, with negative uids
        /// </summary>
        /// <param name="t"></param>
        /// <param name="u"></param>
        protected SDbObject(Types t,long u=-1) :base(t)
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
        /// For AStream we subtract 1 to account for the Types byte.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="f"></param>
        protected SDbObject(Types t,Reader f) : base(t)
        {
            uid = (f is SocketReader)?f.GetLong():f.Position-1;
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
            f.uids = f.uids + (s.uid, uid);
            f.WriteByte((byte)s.type);
        }
        public override bool isValue => false;
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(uid);
        }
        internal string Uid()
        {
            return _Uid(uid);
        }
        internal static string _Uid(long uid)
        {
            if (uid > STransaction._uid)
                return "'" + (uid - STransaction._uid);
            if (uid < 0 && uid > -0x7000000000000000)
                return "#" + (-uid);
            if (uid <= -0x7000000000000000)
                return "@" + (0x7000000000000000 + uid);
            return "" + uid;
        }
        public override string ToString()
        {
            return "SDbObject";
        }
    }
    public class STable : SQuery
    {
        public readonly string name;
        public readonly SDict<long, SSelector> cols;
        public readonly SDict<long, long> rows; // defpos->uid of latest update
        public readonly SDict<long,bool> indexes;
        /// <summary>
        /// A newly defined empty table
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="n"></param>
        public STable(STransaction tr,string n) :base(Types.STable,tr)
        {
            if (tr.names.Contains(n))
                throw new Exception("Table n already exists");
            name = n;
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        protected virtual STable Add(SColumn c)
        {
            var t = new STable(this,cols + (c.uid,c),
                display+(display.Length??0,c.name),
                cpos + (cpos.Length??0,c),
                names + (c.name,c));
            return t;
        }
        public static STable operator+(STable t,SColumn c)
        {
            return t.Add(c);
        }
        protected STable Add(SRecord r)
        {
            return new STable(this,rows + (r.Defpos, r.uid));
        }
        public static STable operator+(STable t,SRecord r)
        {
            return t.Add(r);
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
                var cp = SDict<int, Serialisable>.Empty;
                var di = SDict<int, string>.Empty;
                var sc = cols.Lookup(n);
                var db = display.First();
                for (var b = cpos.First();db!=null && b != null; db=db.Next(), b = b.Next())
                    if (b.Value.Item2 is SColumn c && c.uid != n)
                    {
                        di += (k, db.Value.Item2);
                        cp += (k++, c);
                    }
                return new STable(this,cols-n,di,cp,names-sc.name);
            }
            else
                return new STable(this, rows-n);
        }
        /// <summary>
        /// for system and client table references
        /// </summary>
        /// <param name="n"></param>
        /// <param name="u">will be negative</param>
        public STable(string n, long u=-1)
            : base(Types.STable, u)
        {
            name = n;
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        public STable(Types t,STable tb)  : base(t,tb)
        {
            name = tb.name;
            cols = tb.cols;
            rows = tb.rows;
            indexes = tb.indexes;
        }
        public STable(STable t,string n) :base(t)
        {
            name = n;
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        protected STable(STable t, SDict<long, SSelector> co, 
            SDict<int,string> a,
            SDict<int,Serialisable> cp, SDict<string,Serialisable> na) 
            :base(t,a,cp,na)
        {
            name = t.name;
            cols = co;
            rows = t.rows;
            indexes = t.indexes;
        }
        protected STable(STable t,SDict<long,long> r) : base(t)
        {
            name = t.name;
            cols = t.cols;
            rows = r;
            indexes = t.indexes;
        }
        /// <summary>
        /// When we commit a table, the newly committed table should
        /// not have any columns or rows. Preserve only the name.
        /// </summary>
        /// <param name="t">The current state of the table in the transaction</param>
        /// <param name="f"></param>
        public STable(STable t,AStream f) :base(t,f)
        {
            name = t.name;
            f.PutString(name);
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long,long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        STable(Reader f) :base(Types.STable,f)
        {
            name = f.GetString();
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        protected STable(Types t,Reader f) : base(t, f)
        {
            name = f.GetString();
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        internal STable(STable t,SDict<long,bool> x) :base(t)
        {
            name = t.name;
            cols = t.cols;
            rows = t.rows;
            indexes = x;
        }
        public static STable Get(SDatabase db,Reader f)
        {
            var tb = new STable(f);
            var n = tb.name;
            return (tb.uid >= 0)?
                tb : (n[0] == '_' && SysTable.system.Lookup(n) is SysTable st) ?
                st :
                db.GetTable(n) ??
                throw new Exception("No such table " + n);
        }
        public override RowSet RowSet(STransaction tr,SQuery top,SDict<long,SFunction>ags,Context cx)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)tr.objects[b.Value.Item1];
                if (x.references < 0)
                    return new IndexRowSet(tr, this, x, SCList<Variant>.Empty, SList<Serialisable>.Empty);
            }
            return new TableRowSet(tr, this);
        }
        public override Serialisable Lookup(Context cx)
        {
            if (cx.head is RowBookmark rb)
                return rb._ob;
            return this;
        }
        public override Serialisable Lookup(string a)
        {
            if (a.CompareTo(name) == 0)
                return this;
            return base.Lookup(a);
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
            base.Put(f);
            f.PutString(name);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append("Table "); sb.Append(name);
        }
        public override string Alias => name;

        public SDbObject ob => this;

        public override string ToString()
        {
            return "Table "+name+"["+Uid()+"]";
        }
    }
    public class SCreateTable : Serialisable
    {
        public readonly string tdef;
        public readonly SList<SColumn> coldefs;
        public SCreateTable(string tn,SList<SColumn> c):base(Types.SCreateTable)
        { tdef = tn; coldefs = c; }
        protected SCreateTable(Reader f):base(Types.SCreateTable,f)
        {
            tdef = f.GetString();
            var n = f.GetInt();
            var c = SList<SColumn>.Empty;
            for (var i = 0; i < n; i++)
            {
                var co = SColumn.Get(f);
                c += (new SColumn(co,co.name,(Types)f.ReadByte()), i);
            }
            coldefs = c;
        }
        public new static SCreateTable Get(Reader f)
        {
            return new SCreateTable(f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(tdef);
            f.PutInt(coldefs.Length);
            for (var b = coldefs.First(); b != null; b = b.Next())
            {
                f.PutString(b.Value.name);
                f.WriteByte((byte)b.Value.dataType);
            }
        }
        public override string ToString()
        {
            return "CreateTable "+tdef+" "+coldefs.ToString();
        }
    }
    public class SCreateIndex :Serialisable
    {
        public readonly SString index;
        public readonly SString table;
        public readonly SBoolean primary;
        public readonly Serialisable references; // SString or Null
        public readonly SList<SSelector> cols;
        public SCreateIndex(SString i,SString t,SBoolean b,Serialisable r,SList<SSelector>c)
            : base(Types.SCreateIndex)
        { index = i; table = t; primary = b; references = r; cols = c; }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(table.str);
            var refer = references as SString;
            f.WriteByte((byte)(((refer?.str.Length??0) == 0) ? 2 : (primary.sbool) ? 0 : 1));
            f.PutString(refer?.str??"");
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                f.PutString(b.Value.name);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Create ");
            if (primary.sbool)
                sb.Append("primary ");
            sb.Append("index ");
            sb.Append(index.str); sb.Append(" for ");
            sb.Append(table.str);
            var refer = references as SString;
            if (refer?.str.Length>0)
            {
                sb.Append("references ");sb.Append(refer?.str); 
            }
            sb.Append('(');
            var cm = "";
            for (var b=cols.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.name);
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    public class SysTable : STable
    {
        public static long _uid = -0x7000000000000000;
        public static SDict<string, SysTable> system = SDict<string, SysTable>.Empty;
        /// <summary>
        /// System tables are like templates: need to be virtually specialised for a db
        /// </summary>
        /// <param name="n"></param>
        public SysTable(string n) : base(n, --_uid)
        {
        }
        SysTable(SysTable t, SDict<long, SSelector> c, SDict<int,string> d,
            SDict<int,Serialisable> p, SDict<string, Serialisable> n)
            : base(t, c, d, p, n)
        {
        }
        static void Add(string name,params ValueTuple<string,Types>[] ss)
        {
            var st = new SysTable(name);
            for (var i = 0; i < ss.Length; i++)
                st = (SysTable)st.Add(new SColumn(ss[i].Item1, ss[i].Item2));
            system = system + (st.name, st);
        }
        static SysTable()
        {
            Add("_Log",
            new ValueTuple<string,Types>("Uid", Types.SString),
            new ValueTuple<string,Types>("Type", Types.SInteger),
            new ValueTuple<string,Types>("Desc", Types.SString));
            Add("_Tables",
            new ValueTuple<string, Types>("Name", Types.SString),
            new ValueTuple<string, Types>("Cols", Types.SInteger),
            new ValueTuple<string, Types>("Rows", Types.SInteger));
        }
        protected override STable Add(SColumn c)
        {
            return new SysTable(this, cols + (c.uid, c), display+(display.Length??0,c.name), 
                cpos + (cpos.Length??0,c),
                names + (c.name, c));
        }
        SysTable Add(string n, Types t)
        {
            return (SysTable)Add(new SysColumn(n, t));
        }
        public override RowSet RowSet(STransaction tr,SQuery top, SDict<long,SFunction>ags,Context cx)
        {
            return new SysRows(tr,this);
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
        protected SSelector(Types t, Reader f) : base(t, f)
        {
            name = f.GetString();
        }
        protected SSelector(SSelector s,AStream f) : base(s,f)
        {
            name = s.name;
            f.PutString(name);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(name);
        }
        public override Serialisable Lookup(Context cx)
        {
            return cx[name];
        }
        public override string Alias(int n)
        {
            return name;
        }
        public override string ToString()
        {
            return "SSelector";
        }
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
        public SColumn(string n,Types t=Types.Serialisable,long u = -1) :base(Types.SColumn,n,u)
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
        SColumn(Reader f) :base(Types.SColumn,f)
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
        public new static SColumn Get(Reader f)
        {
            return new SColumn(f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
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
        public override Serialisable Lookup(Context cx)
        {
            return cx.defines(name) ? cx[name] : this;
        }
        public override string ToString()
        {
            return "Column " + name + " [" + Uid() + "] for "+_Uid(table)+": " + dataType.ToString();
        }
    }
    public class SAlterStatement : Serialisable
    {
        public readonly string id;
        public readonly string col;
        public readonly string name;
        public readonly Types dataType;
        public SAlterStatement (string i, string c, string n, Types d)
            :base(Types.SAlterStatement)
        {
            id = i; col = c; name = n; dataType = d;
        }
        public static STransaction Obey(STransaction tr,Reader rdr)
        {
            var tn = rdr.GetString(); // table name
            var tb = (STable)tr.names.Lookup(tn) ??
                throw new Exception("Table " + tn + " not found");
            var cn = rdr.GetString(); // column name or ""
            var nm = rdr.GetString(); // new name
            var dt = (Types)rdr.ReadByte();
            if (cn.Length == 0)
                return (STransaction)tr.Install(new SAlter(tr, nm, Types.STable, tb.uid, 0), tr.curpos);
            else if (dt == Types.Serialisable)
                return (STransaction)tr.Install(new SAlter(tr, nm, Types.SColumn, tb.uid,
                        (tb.names.Lookup(cn) as SSelector)?.uid ??
                        throw new Exception("Column " + cn + " not found")), tr.curpos);
            else 
             return (STransaction)tr.Install(new SColumn(tr, nm, dt, tb.uid),tr.curpos);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(id);
            f.PutString(col);
            f.PutString(name);
            f.WriteByte((byte)dataType);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Alter table ");
            sb.Append(id);
            if (col.Length > 0)
            {
                sb.Append(" alter "); sb.Append(col);
            }
            else
                sb.Append(" add ");
            sb.Append(name);
            sb.Append(' ');
            sb.Append(DataTypeName(dataType));
            return sb.ToString();
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
        SAlter(Reader f):base(Types.SAlter,f)
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
        public new static SAlter Get(Reader f)
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
                + name + " " + Serialisable.DataTypeName(dataType);
        }
    }
    public class SDropStatement: Serialisable
    {
        public readonly string drop;
        public readonly string table;
        public SDropStatement(string d,string t) :base(Types.SDropStatement)
        { drop = d; table = t; }
        public static STransaction Obey(STransaction tr, Reader rdr)
        {
            var nm = rdr.GetString(); // object name
            var pt = tr.names.Lookup(nm) ??
                throw new Exception("Object " + nm + " not found");
            var cn = rdr.GetString();
            return (STransaction)tr.Install(
                (cn.Length == 0) ?
                    new SDrop(tr, pt.uid, -1) :
                    new SDrop(tr,
                        (((STable)pt).names.Lookup(cn) as SSelector)?.uid ??
                        throw new Exception("Column " + cn + " not found"),
                    pt.uid),tr.curpos
                );
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(drop);
            f.PutString(table);
        }
        public override string ToString()
        {
            return "Drop " + drop + ((table.Length>0)?(" from "+table):"");
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
        SDrop(Reader f) :base(Types.SDrop,f)
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
        public new static SDrop Get(Reader f)
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
        SView(SDatabase d, Reader f):base(Types.SView,f)
        {
            name = f.GetString();
            var n = f.GetInt();
            var c = SList<SColumn>.Empty;
            for (var i = 0; i < n; i++)
            {
                var nm = f.GetString();
                c = c+(new SColumn(nm, (Types)f.ReadByte(), 0),i);
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
        public static SView Get(SDatabase d, Reader f)
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
    public class SInsertStatement : Serialisable
    {
        public readonly string tb;
        public readonly SList<SSelector> cols;
        public readonly Serialisable vals;
        public SInsertStatement(string t,SList<SSelector> c,Serialisable v)
            : base(Types.SInsert)
        {
            tb = t; cols = c; vals = v;
        }
        public STransaction Obey(STransaction tr)
        {
            var tb = (STable)tr.names.Lookup(this.tb); 
            var n = cols.Length; // # named cols
            var cs = SList<long>.Empty;
            Exception? ex = null;
            var i = 0;
            for (var b=cols.First();b!=null;b=b.Next())
            {
                if (tb.names.Lookup(b.Value.name) is SColumn sc)
                    cs += (sc.uid, i++);
                else
                    ex = new Exception("Column " + b.Value.name + " not found");
            }
            if (vals is SValues svs)
            {
                var nc = svs.vals.Length??0;
                if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc))
                    ex = new Exception("Wrong number of columns");
                var f = SDict<long, Serialisable>.Empty;
                var c = svs.vals;
                if (n == 0)
                    for (var b = tb.cpos.First(); c.Length!=0 && b!=null; b = b.Next(), c = c.next) // not null
                        f += (((SSelector)b.Value.Item2).uid, c.element.Lookup(Context.Empty));
                else
                    for (var b = cs; c.Length!=0 && b.Length != 0; b = b.next, c = c.next) // not null
                        f += (b.element, c.element);
                tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f),tr.curpos);
            }
            if (ex != null)
                throw ex;
            return tr;
        }
        public static SInsertStatement Get(SDatabase db,Reader f)
        {
            var t = f.GetString();
            var n = f.GetInt();
            var c = SList<SSelector>.Empty;
            for (var i = 0; i < n; i++)
                c += ((SSelector)f._Get(db), i);
            return new SInsertStatement(t, c, f._Get(db));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutString(tb);
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                b.Value.Put(f);
            vals.Put(f);
        }
    }
    public class SValues : Serialisable
    {
        public readonly SList<Serialisable> vals;
        public SValues(SList<Serialisable> c) : base(Types.SValues)
        {
            vals = c;
        }
        public SValues(SDatabase db,Reader f) : base(Types.SValues)
        {
            var n = f.GetInt();
            var nr = f.GetInt();
            vals = SList<Serialisable>.Empty;
            for (var i = 0; i < n; i++)
                vals += (f._Get(db), i);
        }
        public override bool isValue => true;
        public static SValues Get(SDatabase db,Reader f)
        {
            var n = f.GetInt();
            var nr = f.GetInt();
            var v = SList<Serialisable>.Empty;
            for (var i = 0; i < n; i++)
                v += (f._Get(db), i);
            return new SValues(v);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(vals.Length);
            f.PutInt(1);
            for (var b = vals.First(); b != null; b = b.Next())
                b.Value.Put(f);
        }
    }
    public class SRecord : SDbObject
    {
        public readonly SDict<long, Serialisable> fields;
        public readonly long table;
        public SRecord(STransaction tr, long t, SDict<long, Serialisable> f) : this(Types.SRecord, tr, t, f) { }
        protected SRecord(Types ty,STransaction tr,long t,SDict<long,Serialisable> f):base(ty,tr)
        {
            fields = f;
            table = t;
        }
        public virtual long Defpos => uid;
        public SRecord(SDatabase db, SRecord r, AStream f) : base(r,f)
        {
            table = f.Fix(r.table);
            var fs = r.fields;
            f.PutLong(table);
            var tb = (STable)db.objects[table];
            f.PutInt(r.fields.Length);
            for (var b=fs.First();b!=null;b=b.Next())
            {
                var oc = b.Value.Item1;
                var v = b.Value.Item2;
                var c = oc;
                if (f.uids.Contains(c))
                {
                    c = f.uids.Lookup(c);
                    fs = fs-oc+(c, v);
                }
                f.PutLong(c);
                v.Put(f);
            }
            fields = fs;
        }
        protected SRecord(Types t,SDatabase d, Reader f) : base(t,f)
        {
            table = f.GetLong();
            var n = f.GetInt();
            var tb = (STable)d.objects[table];
            var a = SDict<long, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = f.GetLong();
                a += (k, f._Get(d));
            }
            fields = a;
        }
        public static SRecord Get(SDatabase d, Reader f)
        {
            return new SRecord(Types.SRecord, d,f);
        }
        public override void Append(SDatabase? db, StringBuilder sb)
        {
            sb.Append("{");
            if (Defpos<STransaction._uid)
            {
                sb.Append("_id:"); sb.Append(Defpos); sb.Append(",");
            }
            var tb = db?.objects[table] as STable;
            sb.Append("_table:");
            sb.Append('"'); sb.Append(tb?.name ?? _Uid(table)); sb.Append('"');
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(",");
                var c = tb?.cols.Lookup(b.Value.Item1);
                sb.Append(c?.name ?? _Uid(b.Value.Item1)); sb.Append(":");
                b.Value.Item2.Append(db,sb);
            }
            sb.Append("}");
        }
        public bool Matches(RowBookmark rb,SList<Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value is SExpression x && x.Lookup(new Context(rb,null)) is SBoolean e 
                    && !e.sbool)
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
            sb.Append(" for "); sb.Append(_Uid(table));
            Append(null,sb);
            return sb.ToString();
        }
    }
    public class SUpdateSearch : Serialisable
    {
        public readonly SQuery qry;
        public readonly SDict<string, Serialisable> assigs;
        public SUpdateSearch(SQuery q,SDict<string,Serialisable> a)
            : base(Types.SUpdateSearch)
        {
            qry = q; assigs = a;
        }
        public STransaction Obey(STransaction tr,Context cx)
        {
            for (var b = qry.RowSet(tr,qry,SDict<long,SFunction>.Empty, cx).First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var u = SDict<string, Serialisable>.Empty;
                for (var c = assigs.First(); c != null; c = c.Next())
                    u += (c.Value.Item1, c.Value.Item2.Lookup(new Context(b,null)));
                tr = b.Update(tr,u);
            }
            return tr;
        }
        public static SUpdateSearch Get(SDatabase db,Reader f)
        {
            var q = (SQuery)f._Get(db);
            var n = f.GetInt();
            var a = SDict<string, Serialisable>.Empty;
            for (var i=0;i<n;i++)
            {
                var s = f.GetString();
                if (q.Lookup(s) is SColumn sc)
                    a += (sc.name, f._Get(db));
                else
                    throw new Exception("Column " + s + " not found");
            }
            return new SUpdateSearch(q, a);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
            f.PutInt(assigs.Length);
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                f.PutString(b.Value.Item1); b.Value.Item2.Put(f);
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Update ");
            qry.Append(null,sb);
            sb.Append(" set ");
            var cm = "";
            for (var b = assigs.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.Item1);sb.Append('=');sb.Append(b.Value.Item2);
            }
            return sb.ToString();
        }
    }
    public class SUpdate : SRecord
    {
        public readonly long defpos;
        public SUpdate(STransaction tr,SRecord r,SDict<string,Serialisable>u) 
            : base(Types.SUpdate,tr,r.table,_Merge(tr,r,u))
        {
            defpos = r.Defpos;
        }
        public override long Defpos => defpos;
        public SUpdate(SDatabase db,SUpdate r, AStream f) : base(db,r,f)
        {
            defpos = f.Fix(r.defpos);
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d, Reader f) : base(Types.SUpdate,d,f)
        {
            defpos = f.GetLong();
        }
        static SDict<long,Serialisable> _Merge(STransaction tr,SRecord r,
            SDict<string,Serialisable> us)
        {
            var tb = (STable)tr.objects[r.table];
            var u = SDict<long, Serialisable>.Empty;
            for (var b=us.First();b!=null;b=b.Next())
                u += (((SColumn)(tb.names[b.Value.Item1] ??
                    throw new Exception("No column " + b.Value.Item1))).uid,
                    b.Value.Item2);
            return r.fields.Merge(u);
        }
        public new static SRecord Get(SDatabase d, Reader f)
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
            Append(null,sb);
            return sb.ToString();
        }
    }
    public class SDeleteSearch : Serialisable
    {
        public readonly SQuery qry;
        public SDeleteSearch(SQuery q) :base(Types.SDeleteSearch) { qry = q; }
        public STransaction Obey(STransaction tr,Context cx)
        {
            for (var b = qry.RowSet(tr,qry,SDict<long,SFunction>.Empty, cx).First() as RowBookmark; 
                b != null; b = b.Next() as RowBookmark)
            {
                var rc = b._ob.rec ?? throw new System.Exception("??");// not null
                tr = (STransaction)tr.Install(new SDelete(tr, rc.table, rc.uid),tr.curpos); 
            }
            return tr;
        }
        public static SDeleteSearch Get(SDatabase db,Reader f)
        {
            return new SDeleteSearch((SQuery)f._Get(db));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Delete ");
            qry.Append(null,sb);
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
        SDelete(Reader f) : base(Types.SDelete,f)
        {
            table = f.GetLong();
            delpos = f.GetLong();
        }
        public new static SDelete Get(Reader f)
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
        public readonly long refindex;
        public readonly SList<long> cols;
        public readonly SMTree<long> rows;
        /// <summary>
        /// A primary or unique index
        /// </summary>
        /// <param name="t"></param>
        /// <param name="c"></param>
        public SIndex(STransaction tr, long t, bool p, long r, SList<long> c) : base(Types.SIndex, tr)
        {
            table = t;
            primary = p;
            cols = c;
            references = r;
            if (r >= 0)
            {
                var rx = tr.GetPrimaryIndex(r) ??
                    throw new Exception("referenced table has no primary index");
                refindex = rx.uid;
            }
            else
                refindex = -1;
            rows = new SMTree<long>(Info((STable)tr.objects[table], cols,refindex>=0));
        }
        SIndex(SDatabase d, Reader f) : base(Types.SIndex, f)
        {
            table = f.GetLong();
            primary = f.ReadByte()!=0;
            var n = f.GetInt();
            var c = new long[n];
            for (var i = 0; i < n; i++)
                c[i] = f.GetLong();
            references = f.GetLong();
            refindex =  (references<0)?-1:d.GetPrimaryIndex(references)?.uid ??
                throw new Exception("internal error");
            cols = SList<long>.New(c);
            rows = new SMTree<long>(Info((STable)d.objects[table], cols,references>=0));
        }
        public SIndex(SIndex x, AStream f) : base(x, f)
        {
            table = f.Fix(x.table);
            f.PutLong(table);
            primary = x.primary;
            f.WriteByte((byte)(primary ? 1 : 0));
            long[] c = new long[x.cols.Length??0];
            f.PutInt(x.cols.Length);
            var i = 0;
            for (var b = x.cols.First(); b != null; b = b.Next())
            {
                c[i] = f.Fix(b.Value);
                f.PutLong(c[i++]);
            }
            references =f.Fix(x.references);
            refindex = f.Fix(x.refindex);
            f.PutLong(references);
            cols = SList<long>.New(c);
            rows = x.rows;
        }
        public SIndex(SIndex x,SMTree<long> nt) :base(x)
        {
            table = x.table;
            primary = x.primary;
            references = x.references;
            refindex = x.refindex;
            cols = x.cols;
            rows = nt;
        }
        public static SIndex Get(SDatabase d, Reader f)
        {
            return new SIndex(d, f);
        }
        public bool Contains(SRecord sr)
        {
            return rows.Contains(Key(sr, cols));
        }
        protected SIndex Add(SRecord r,long c)
        {
            return new SIndex(this, rows+(Key(r, cols), c));
        }
        public static SIndex operator+(SIndex x,ValueTuple<SRecord,long> t)
        {
            return x.Add(t.Item1, t.Item2);
        }
        public SIndex Update(SRecord o,SUpdate u, long c)
        {
            return new SIndex(this, rows-(Key(o,cols),o.uid)+(Key(u, cols), u.uid));
        }
        protected SIndex Remove(SRecord sr,long c)
        {
            return new SIndex(this, rows-(Key(sr, cols),c));
        }
        public static SIndex operator-(SIndex x,ValueTuple<SRecord,long> a)
        {
            return x.Remove(a.Item1, a.Item2);
        }
        SList<TreeInfo<long>> Info(STable tb, SList<long> cols,bool fkey)
        {
            if (cols.Length==0)
                return SList<TreeInfo<long>>.Empty;
            return Info(tb, cols.next,fkey)+(new TreeInfo<long>( // not null
                tb.cols.Lookup(cols.element).uid, (cols.Length!=1 || fkey)?'A':'D', 'D'), 0);
        }
        SCList<Variant> Key(SRecord sr,SList<long> cols)
        {
            if (cols.Length == 0)
                return SCList<Variant>.Empty;
            return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element)), Key(sr, cols.next)); // not null
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Index " + _Uid(uid) + " [" + _Uid(table) + "] (");
            var cm = "";
            for (var b = cols.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(_Uid(b.Value));
            }
            sb.Append(")");
            if (refindex >= 0)
                sb.Append(" ref index " + refindex);
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
            public readonly bool input;
            public long start;
            public byte[] buf;
            public int len;
            public int wpos;
            internal StreamBase fs;
            public Buffer(StreamBase f)
            {
                buf = new byte[Size];
                wpos = 0;
                len = Size;
                start = f.Length;
                fs = f;
            }
            public Buffer(StreamBase f,long s)
            {
                buf = new byte[Size];
                wpos = 0;
                len = Size;
                start = s;
                f.GetBuf(this);
                fs = f;
            }
            internal void PutByte(byte b)
            {
                if (wpos >= len)
                {
                    fs.PutBuf(this);
                    start += len;
                }
                buf[wpos++] = b;
            }
        }
        protected Buffer? wbuf = null;
        protected StreamBase() { }
        public abstract bool GetBuf(Buffer b);
        protected abstract void PutBuf(Buffer b);
        public override void WriteByte(byte value)
        {
            wbuf?.PutByte(value);
        }
        public void PutInt(int? n)
        {
            if (n == null)
                throw new Exception("Null PutInt");
            PutInt(new Integer(n.Value));
        }
        public void PutInt(Integer b)
        {
            var m = b.bytes.Length;
            WriteByte((byte)m);
            for (int j = 0; j<m ; j++)
                WriteByte(b.bytes[j]);
        }
        public void PutLong(long n)
        {
            PutInt(new Integer(n));
        }
        public void PutString(string s)
        {
            var cs = Encoding.UTF8.GetBytes(s);
            PutInt(cs.Length);
            for (var i = 0; i < cs.Length; i++)
                WriteByte(cs[i]);
        }
 /*       public void PutStrings(SList<string> ss)
        {
            PutInt(ss?.Length??0);
            for (var b = ss.First(); b != null; b = b.Next())
                PutString(b.Value);
        } */
    }
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class Reader
    {
        public StreamBase.Buffer buf;
        public int pos = 0;
        internal Reader(StreamBase f)
        {
            buf = new StreamBase.Buffer(f);
        }
        internal Reader(StreamBase f, long s)
        {
            buf = new StreamBase.Buffer(f, s);
        }
        internal long Position => buf.start + pos;
        public virtual int ReadByte()
        {
            if (pos >= buf.len)
            {
                buf = new StreamBase.Buffer(buf.fs, buf.start + buf.len);
                pos = 0;
            }
            return (buf.len == 0) ? -1 : buf.buf[pos++];
        }
        public Integer GetInteger()
        {
            var n = ReadByte();
            var cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return new Integer(cs);
        }
        public int GetInt()
        {
            return GetInteger();
        }
        public long GetLong()
        {
            return GetInteger();
        }
        public string GetString()
        {
            int n = GetInt();
            byte[] cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return Encoding.UTF8.GetString(cs, 0, n);
        }
 /*       public SList<string> GetStrings()
        {
            int n = GetInt();
            var r = SList<string>.Empty;
            for (var i = 0; i < n; i++)
                r = r + (GetString(), i);
            return r;
        } */
        public Serialisable _Get(SDatabase d)
        {
            Types tp = (Types)ReadByte();
            Serialisable s;
            switch (tp)
            {
                case Types.Serialisable: s = Serialisable.Get(this); break;
 //               case Types.STimestamp: s = STimestamp.Get(this); break;
                case Types.SBigInt:
                case Types.SInteger: s = SInteger.Get(this); break;
                case Types.SNumeric: s = SNumeric.Get(this); break;
                case Types.SString: s = SString.Get(this); break;
                case Types.SDate: s = SDate.Get(this); break;
                case Types.STimeSpan: s = STimeSpan.Get(this); break;
                case Types.SBoolean: s = SBoolean.Get(this); break;
                case Types.STable: s = STable.Get(d,this); break;
                case Types.SRow: s = SRow.Get(d, this); break;
                case Types.SColumn: s = SColumn.Get(this); break;
                case Types.SRecord: s = SRecord.Get(d, this); break;
                case Types.SUpdate: s = SUpdate.Get(d, this); break;
                case Types.SDelete: s = SDelete.Get(this); break;
                case Types.SAlter: s = SAlter.Get(this); break;
                case Types.SDrop: s = SDrop.Get(this); break;
                case Types.SIndex: s = SIndex.Get(d, this); break;
                case Types.SUpdateSearch: s = SUpdateSearch.Get(d, this); break;
                case Types.SDeleteSearch: s = SDeleteSearch.Get(d, this); break;
                case Types.SSearch: s = SSearch.Get(d,this); break;
                case Types.SSelect: s = SSelectStatement.Get(d, this); break;
                case Types.SValues: s = SValues.Get(d, this); break;
                case Types.SExpression: s = SExpression.Get(d, this); break;
                case Types.SFunction: s = SFunction.Get(d, this); break;
                case Types.SOrder: s = SOrder.Get(d, this); break;
                case Types.SInPredicate: s = SInPredicate.Get(d, this); break;
                case Types.SAliasedTable: s = SAliasedTable.Get(d, this); break;
                case Types.SGroupQuery: s = SGroupQuery.Get(d, this); break;
                case Types.SJoin: s = SJoin.Get(d, this); break;
                default: s = Serialisable.Null; break;
            }
            return s;
        }
        /// <summary>
        /// Called from Transaction.Commit()
        /// </summary>
        /// <param name="d"></param>
        /// <param name="pos"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public SDbObject[] GetAll(SDatabase d,long max)
        {
            var r = new List<SDbObject>();
            while (Position < max)
                r.Add((SDbObject)_Get(d));
            return r.ToArray();
        }
        Serialisable Lookup(SDatabase db, long pos)
        {
            return db.objects[((AStream)buf.fs).Fix(pos)];
        }
    }
    /// <summary>
    /// this class is not shareable
    /// </summary>
    public class SocketReader : Reader
    {
        public SocketReader(StreamBase f) : base(f)
        {
            pos = 2;
        }
        public override int ReadByte()
        {
            if (pos >= buf.len)
            {
                if (!buf.fs.GetBuf(buf))
                    return -1;
                pos = 2;
            }
            return (buf.len == 0) ? -1 : buf.buf[pos++];
        }
    }
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class AStream : StreamBase
    {
        public readonly string filename;
        internal Stream file;
        long wposition = 0;
        public long length = 0;
        internal SDict<long, long> uids = SDict<long,long>.Empty; // used for movement of SDbObjects
        SDict<long, Serialisable> commits = SDict<long, Serialisable>.Empty;
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            wposition = length;
            file.Seek(0, SeekOrigin.Begin);
        }
        public SDatabase Commit(SDatabase db,STransaction tr)
        {
            commits = SDict<long, Serialisable>.Empty;
            wbuf = new Buffer(this);
            uids = SDict<long, long>.Empty;
            for (var b=tr.objects.PositionAt(STransaction._uid);b!=null; b=b.Next())
            {
                switch (b.Value.Item2.type)
                {
                    case Types.STable:
                        {
                            var st = (STable)b.Value.Item2;
                            var nt = new STable(st, this);
                            db += (nt,Length);
                            commits += (nt.uid, nt);
                            break;
                        }
                    case Types.SColumn:
                        {
                            var sc = (SColumn)b.Value.Item2;
                            var nc = new SColumn(sc, this);
                            db += (nc,Length);
                            commits += (nc.uid, nc);
                            break;
                        }
                    case Types.SRecord:
                        {
                            var sr = (SRecord)b.Value.Item2;
                            var nr = new SRecord(db, sr, this);
                            if (sr.uid>STransaction._uid)
                                db += (nr,Length);
                            commits += (nr.uid, nr);
                            break;
                        }
                    case Types.SDelete:
                        {
                            var sd = (SDelete)b.Value.Item2;
                            var nd = new SDelete(sd, this);
                            if (sd.uid > STransaction._uid)
                                db += (nd, Length);
                            commits += (nd.uid, nd);
                            break;
                        }
                    case Types.SUpdate:
                        {
                            var sr = (SUpdate)b.Value.Item2;
                            var nr = new SUpdate(db, sr, this);
                            if (sr.uid > STransaction._uid)
                                db += (nr, Length);
                            commits += (nr.uid, nr);
                            break;
                        }
                    case Types.SAlter:
                        {
                            var sa = new SAlter((SAlter)b.Value.Item2, this);
                            db += (sa,Length);
                            commits += (sa.uid, sa);
                            break;
                        }
                    case Types.SDrop:
                        {
                            var sd = new SDrop((SDrop)b.Value.Item2, this);
                            db += (sd, Length);
                            commits += (sd.uid, sd);
                            break;
                        }
                    case Types.SIndex:
                        {
                            var si = new SIndex((SIndex)b.Value.Item2, this);
                            db += (si, Length);
                            commits += (si.uid, si);
                            break;
                        }
                }
            }
            var len = Length;
            Flush();
            SDatabase.Install(db);
            return db;
        }
        internal Serialisable Lookup(SDatabase db, long pos)
        {
            pos = Fix(pos);
            if (pos >= STransaction._uid)
                return db.objects[pos];
            if (pos >= wposition)
                return commits[pos];
            return new Reader(this, pos)._Get(db);
        }
        internal long Fix(long pos)
        {
            return (uids.Contains(pos))?uids[pos]:pos;
        }
        public override bool CanRead => throw new System.NotImplementedException();

        public override bool CanSeek => throw new System.NotImplementedException();

        public override bool CanWrite => throw new System.NotImplementedException();

        public override long Length => length + (wbuf?.wpos)??0;

        public override long Position { get => file.Position; set => throw new System.NotImplementedException(); }
        public override void Close()
        {
            file.Close();
            base.Close();
        }

        public override void Flush()
        {
            if (wbuf!=null)
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

        public override bool GetBuf(Buffer b)
        {
            lock (file)
            {
                if (b.start > wposition)
                    throw new Exception("File overrun attempt");
                  //  return false;
                file.Seek(b.start, SeekOrigin.Begin);
                var n = length - b.start;
                if (n > Buffer.Size)
                    n = Buffer.Size;
                b.len = file.Read(b.buf, 0, (int)n);
                return b.len > 0;
            }
        }

        protected override void PutBuf(Buffer b)
        {
            lock (file)
            {
                var p = file.Seek(0, SeekOrigin.End);
                file.Write(b.buf, 0, b.wpos);
                file.Flush();
                length = p + b.wpos;
                wposition = length;
                b.wpos = 0;
            }
        }
    }
}
