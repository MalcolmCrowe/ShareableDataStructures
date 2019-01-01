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
        SInPredicate = 41
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
                case Types.STimestamp: return "timestamp";
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
        /// We have been mentioned in a Serialisable. We might be able to improve it using a known list
        /// of selectors
        /// </summary>
        /// <param name="nms">The information for associating strings to SColumns</param>
        /// <returns></returns>
        internal virtual Serialisable Lookup(SDict<string, Serialisable> nms)
        {
            return this;
        }
        public virtual Serialisable this[string col]
        { get { throw new NotImplementedException(); } }
        /// <summary>
        /// Evaluate a Serialisable
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        public virtual Serialisable Eval(RowBookmark rs)
        {
            return this;
        }
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
        internal override Serialisable Lookup(SDict<string, Serialisable> nms)
        {
            return new SExpression(left.Lookup(nms),op,right.Lookup(nms));
        }
#nullable disable
        public override Serialisable Eval(RowBookmark rs)
        {
            var lf = left.Eval(rs);
            var rg = right.Eval(rs);
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
                                            return new SInteger(lv + ((SInteger)rg).big);
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) + ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = ((SInteger)lf).big;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv + ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv + ((SInteger)rg).big);
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
                                            return new SNumeric(lv + new Numeric(((SInteger)rg).big,0));
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
                                            return new SInteger(lv - ((SInteger)rg).big);
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) - ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = ((SInteger)lf).big;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv - ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv - ((SInteger)rg).big);
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
                                            return new SNumeric(lv - new Numeric(((SInteger)rg).big, 0));
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
                                            return new SInteger(lv * ((SInteger)rg).big);
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Integer(lv), 0) * ((SNumeric)rg).num);
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = ((SInteger)lf).big;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv * ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv * ((SInteger)rg).big);
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
                                            return new SNumeric(lv * new Numeric(((SInteger)rg).big, 0));
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
                                            return new SInteger(lv / ((SInteger)rg).big);
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Numeric(new Integer(lv), 0) / ((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = ((SInteger)lf).big;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv / ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(lv / ((SInteger)rg).big);
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
                                            return new SNumeric(new Numeric(lv / new Numeric(((SInteger)rg).big, 0)));
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
                                            c = lv.CompareTo(((SInteger)rg).big); break;
                                        case Types.SNumeric:
                                            c = new Numeric(new Integer(lv), 0).CompareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = ((SInteger)lf).big;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.CompareTo(((SInteger)rg).value); break;
                                        case Types.SBigInt:
                                            c = lv.CompareTo(((SInteger)rg).big); break;
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
                                            c = lv.CompareTo(new Numeric(((SInteger)rg).big, 0)); break;
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
                        return new SBoolean(r);
                    }
                case Op.UMinus:
                    switch (left.type)
                    {
                        case Types.SInteger: return new SInteger(-((SInteger)left).value);
                        case Types.SBigInt: return new SInteger(-((SInteger)left).big);
                        case Types.SNumeric: return new SNumeric(-((SNumeric)left).num);
                    }
                    break;
                case Op.Not:
                    {
                        if (left is SBoolean lb) return new SBoolean(lb.sbool == SBool.True ? SBool.False : SBool.True);
                        break;
                    }
                case Op.And:
                    {
                        if (left is SBoolean lb && right is SBoolean rb)
                            return new SBoolean(lb.sbool == SBool.True && rb.sbool == SBool.True);
                        break;
                    }
                case Op.Or:
                    {
                        if (left is SBoolean lb && right is SBoolean rb)
                            return new SBoolean(lb.sbool == SBool.True || rb.sbool == SBool.True);
                        break;
                    }
            }
            throw new Exception("Bad computation");
        }
    }
    public class SFunction : Serialisable
    {
        public readonly Serialisable arg; // probably an SQuery
        public readonly Func func;
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
        internal override Serialisable Lookup(SDict<string, Serialisable> nms)
        {
            return new SFunction(func,(nms.Length==0)?arg:arg.Lookup(nms));
        }
        /// <summary>
        /// rb gives the group if grouping is implemented
        /// </summary>
        /// <param name="rb"></param>
        /// <returns></returns>
        public override Serialisable Eval(RowBookmark rb)
        {
            if (func == Func.Null)
                return new SBoolean(base.Eval(rb) == Null);
            var t = Types.Serialisable;
            var empty = false;
            Integer ai = Integer.Zero;
            Numeric an = Numeric.Zero;
            string ac = "";
            int ic = 0;
            for (var b = rb._rs.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
                if (b.SameGroupAs(rb))
                {
                    var a = arg.Eval(b);
                    t = a.type;
                    switch (func)
                    {
                        case Func.Count:
                            if (a!=Null)
                                ic++;
                            break;
                        case Func.Sum:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Integer(((SInteger)a).value);
                                    ai = ai + xi; break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = ai + xb; break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = an + xn; break;
                            }
                            break;
                        case Func.Max:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Integer(((SInteger)a).value);
                                    ai = (empty || xi > ai) ? xi : ai;
                                    break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = (empty || xb > ai) ? xb : ai;
                                    break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = (empty || xn > an) ? xn : an;
                                    break;
                                case Types.SString:
                                    var xc = ((SString)a).str;
                                    ac = (empty || xc.CompareTo(ac) > 1)? xc : ac;
                                    break;
                            }
                            empty = false;
                            break;
                        case Func.Min:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Integer(((SInteger)a).value);
                                    ai = (empty || xi < ai) ? xi : ai;
                                    break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = (empty || xb < ai) ? xb : ai;
                                    break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = (empty || xn < an) ? xn : an;
                                    break;
                                case Types.SString:
                                    var xc = ((SString)a).str;
                                    ac = (empty || xc.CompareTo(ac) < 1) ? xc : ac;
                                    break;
                            }
                            empty = false;
                            break;
                    }
                }
            if (func == Func.Count)
                return new SInteger(ic);
            switch(t)
            {
                case Types.SInteger: 
                case Types.SBigInt: return new SInteger(ai);
                case Types.SNumeric: return new SNumeric(an);
                case Types.SString: return new SString(ac);
                default: return Null;
            }
            throw new Exception("Unimplemented function");
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
        internal override Serialisable Lookup(SDict<string, Serialisable> nms)
        {
            return new SInPredicate(arg.Lookup(nms), list.Lookup(nms));
        }
    }
    public class STimestamp : Serialisable,IComparable
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
    }
    public class SInteger : Serialisable, IComparable
    {
        public readonly int value;
        public readonly Integer? big;
        public static readonly SInteger Zero = new SInteger(0);
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
        public new static Serialisable Get(Reader f)
        {
            return new SInteger(f);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
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
    public class SString : Serialisable,IComparable
    {
        public readonly string str;
        public SString(string s) :base (Types.SString)
        {
            str = s;
        }
        SString(Reader f) :base(Types.SString, f)
        {
            str = f.GetString();
        }
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
        SDate(Reader f) : base(Types.SDate, f)
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
        public new static Serialisable Get(Reader f)
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
        STimeSpan(Reader f) : base(Types.STimeSpan, f)
        {
            ticks = f.GetLong();
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(ticks);
        }
        public new static Serialisable Get(Reader f)
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
        public SBoolean(bool b) :base(Types.SBoolean)
        {
            sbool = b ? SBool.True : SBool.False;
        }
        SBoolean(Reader f) : base(Types.SBoolean, f)
        {
            sbool = (SBool)f.GetInt();
        }
        public new static Serialisable Get(Reader f)
        {
            return new SBoolean(f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)sbool);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
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
        public readonly SDict<int, string> names;
        public readonly SDict<int, Serialisable> cols;
        public readonly SDict<string, Serialisable> vals;
        public readonly SRecord? rec;
        public SRow(SRecord? r=null) : base(Types.SRow)
        {
            names = SDict<int,string>.Empty;
            cols = SDict<int, Serialisable>.Empty;
            vals = SDict<string, Serialisable>.Empty;
            rec = r;
        }
        public SRow Add(string n, Serialisable v)
        {
            return new SRow(names.Add(names.Length.Value,n),cols.Add(cols.Length.Value,v),
                vals.Add(n,v),rec);
        }
        SRow(SDict<int,string> n,SDict<int,Serialisable> c,SDict<string,Serialisable> v,SRecord? rec) 
            :this(rec)
        {
            names = n;
            cols = c;
            vals = v;
        }
        public SRow(SList<Serialisable> s) :this()
        {
            var cn = SDict<int, string>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            var k = 0;
            for (;s.Length.Value!=0;s=s.next) // not null
            {
                var n = "col" + (k + 1);
                cn = cn.Add(k, n);
                r = r.Add(k, s.element);
                vs = vs.Add(n, s.element);
            }
        }
        SRow(SDatabase d, Reader f) :this()
        {
            var n = f.GetInt();
            var cn = SDict<int, string>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetString();
                cn = cn.Add(i, k);
                var v = f._Get(d);
                r = r.Add(i, v);
                vs = vs.Add(k, v);
            }
            names = cn;
            cols = r;
            vals = vs;
        }
        public SRow(SDatabase db,SRecord rec) :this(rec)
        {
            var tb = (STable)db.Lookup(rec.table);
            var cn = SDict<int, string>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            var k = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value.val is SColumn sc)
                {
                    r = r.Add(k, rec.fields.Lookup(sc.uid));
                    cn = cn.Add(k++, sc.name);
                }
                else
                    throw new Exception("Unimplemented selector");
            names = cn;
            cols = r;
            vals = vs;
        }
        public SRow(SSelectStatement ss,RowBookmark bm):this(bm._ob.rec)
        {
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<string, Serialisable>.Empty;
            if (ss.als.Length.Value > 0)
            {
                var cb = ss.cpos.First();
                for (var b = ss.als.First(); cb!=null && b != null; b = b.Next(), cb = cb.Next())
                {
                    var v = cb.Value.val.Eval(bm);
                    if (v == Null)
                        continue;
                    r = r.Add(b.Value.key, v);
                    vs = vs.Add(b.Value.val, v);
                }
            } 
            names = ss.als;
            cols = r;
            vals = vs;
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
                f.PutString(b.Value.val);
                if (cb.Value.val is Serialisable s)
                    s.Put(f);
                else
                    Null.Put(f);
            }
        }
        public override Serialisable this[string col] => vals.Lookup(col);
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sb.Append('{');
            var cm = "";
            var nb = names.First();
            for (var b = cols.First(); nb!=null && b != null; b = b.Next(),nb=nb.Next())
                if (b.Value.val is Serialisable s && s!=Null)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(nb.Value.val);
                    sb.Append(":");
                    s.Append(db, sb);
                }
            sb.Append("}");
        }
        public override string ToString()
        {
            var sb = new StringBuilder("SRow ");
            Append(null,sb);
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
        /// Transaction-local uids: 0x4000000000000000-0x7fffffffffffffff
        /// System uids (_Log tables etc): 0x9000000000000000-0x8000000000000001
        /// Client session-local objects are given negative uids with default -1
        /// </summary>
        public readonly long uid;
        static long _dbg = 0;
        long dbg = ++_dbg;
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
            f.uids = f.uids.Add(s.uid, uid);
            f.WriteByte((byte)s.type);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(uid);
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
        public STable(STransaction tr,string n) :base(Types.STable,tr)
        {
            if (tr.names.Contains(n))
                throw new Exception("Table n already exists");
            name = n;
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        public virtual STable Add(SColumn c)
        {
            var t = new STable(this,cols.Add(c.uid,c),cpos.Add(cpos.Length.Value,c),
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
                var cp = SDict<int, Serialisable>.Empty;
                var sc = cols.Lookup(n);
                for (var b = cpos.First(); b != null; b = b.Next(), k++)
                    if (b.Value.val is SColumn c && c.uid != n)
                        cp = cp.Add(k++, c);
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
        public STable(string n, long u=-1)
            : base(Types.STable, u)
        {
            name = n;
            cols = SDict<long, SSelector>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        public STable(STable t,string n) :base(t)
        {
            name = n;
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        protected STable(STable t, SDict<long, SSelector> co, SDict<int,Serialisable> cp, 
            SDict<string, Serialisable> cn) :base(t,cp,cn)
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
        public STable(STable t,AStream f) :base(t,f)
        {
            name = t.name;
            f.PutString(name);
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        STable(Reader f) :base(Types.STable,f)
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
        public override RowSet RowSet(SDatabase db)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)db.objects.Lookup(b.Value.key);
                if (x.references < 0)
                    return new IndexRowSet(db, this, x, SList<Serialisable>.Empty);
            }
            return new TableRowSet(db, this);
        }
        public override SRow Eval(RowBookmark rb)
        {
            return rb._ob;
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
                c = coldefs.InsertAt(new SColumn(co,co.name,(Types)f.ReadByte()), i);
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
            f.WriteByte((byte)(((refer?.str.Length??0) == 0) ? 2 : (primary.sbool == SBool.True) ? 0 : 1));
            f.PutString(refer?.str??"");
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                f.PutString(b.Value.name);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Create ");
            if (primary.sbool == SBool.True)
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
        SysTable(SysTable t, SDict<long, SSelector> c, SDict<int,Serialisable> p, SDict<string, Serialisable> n)
            : base(t, c, p, n)
        {
        }
        static void Add(string name,params SSlot<string,Types>[] ss)
        {
            var st = new SysTable(name);
            for (var i = 0; i < ss.Length; i++)
                st = (SysTable)st.Add(new SColumn(ss[i].key, ss[i].val));
            system = system.Add(st.name, st);
        }
        static SysTable()
        {
            Add("_Log",
            new SSlot<string,Types>("Uid", Types.SString),
            new SSlot<string,Types>("Type", Types.SInteger),
            new SSlot<string,Types>("Desc", Types.SString));
            Add("_Tables",
            new SSlot<string, Types>("Name", Types.SString),
            new SSlot<string, Types>("Cols", Types.SInteger),
            new SSlot<string, Types>("Rows", Types.SInteger));
        }
        public override STable Add(SColumn c)
        {
            return new SysTable(this, cols.Add(c.uid, c), cpos.Add(cpos.Length.Value,c),
                names.Add(c.name, c));
        }
        SysTable Add(string n, Types t)
        {
            return (SysTable)Add(new SysColumn(n, t));
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
        internal override Serialisable Lookup(SDict<string, Serialisable> nms)
        {
            return nms.Lookup(name);
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
        public override Serialisable Eval(RowBookmark rs)
        {
            if (rs._ob.rec is SRecord rec)
            {
                var tb = (STable)rs._rs._db.objects.Lookup(rec.table);
                return rec.fields.Lookup(
                    ((SColumn)(tb.names.Lookup(name) ??
                    throw new Exception("no column " + name))).uid) ??
                    Null;
            }
            return rs._ob[name] ??
                        throw new Exception("no column " + name);
        }
        public override string ToString()
        {
            return "Column " + name + " [" + Uid() + "]: " + dataType.ToString();
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
        public static void Obey(STransaction tr,Reader rdr)
        {
            var tn = rdr.GetString(); // table name
            var tb = (STable)tr.names.Lookup(tn) ??
                throw new Exception("Table " + tn + " not found");
            var cn = rdr.GetString(); // column name or ""
            var nm = rdr.GetString(); // new name
            var dt = (Types)rdr.ReadByte();
            tr = tr.Add(
                (cn.Length == 0) ?
                    new SAlter(tr, nm, Types.STable, tb.uid, 0) :
                    (dt == Types.Serialisable) ?
                    (SDbObject)new SAlter(tr, nm, Types.SColumn, tb.uid,
                        (tb.names.Lookup(cn) as SSelector)?.uid ??
                        throw new Exception("Column " + cn + " not found")) :
                        new SColumn(tr, nm, dt, tb.uid)
                    );
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
        public static void Obey(STransaction tr, Reader rdr)
        {
            var nm = rdr.GetString(); // object name
            var pt = tr.names.Lookup(nm) ??
                throw new Exception("Object " + nm + " not found");
            var cn = rdr.GetString();
            tr = tr.Add(
                (cn.Length == 0) ?
                    new SDrop(tr, pt.uid, -1) :
                    new SDrop(tr,
                        (((STable)pt).names.Lookup(cn) as SSelector)?.uid ??
                        throw new Exception("Column " + cn + " not found"),
                    pt.uid)
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
                c = c.InsertAt(new SColumn(nm, (Types)f.ReadByte(), 0),i);
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
                    cs = cs.InsertAt(sc.uid, i++);
                else
                    ex = new Exception("Column " + b.Value.name + " not found");
            }
            if (vals is SValues svs)
            {
                var nc = svs.vals.Length.Value;
                if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc))
                    ex = new Exception("Wrong number of columns");
                var f = SDict<long, Serialisable>.Empty;
                var c = svs.vals;
                if (n == 0)
                    for (var b = tb.cpos.First(); c.Length!=0 && b!=null; b = b.Next(), c = c.next) // not null
                        f = f.Add(((SSelector)b.Value.val).uid, c.element);
                else
                    for (var b = cs; c.Length!=0 && b.Length != 0; b = b.next, c = c.next) // not null
                        f = f.Add(b.element, c.element);
                tr = tr.Add(new SRecord(tr, tb.uid, f));
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
                c = c.InsertAt((SSelector)f._Get(db), i);
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
                vals = vals.InsertAt(f._Get(db), i);
        }
        public static SValues Get(SDatabase db,Reader f)
        {
            var n = f.GetInt();
            var nr = f.GetInt();
            var v = SList<Serialisable>.Empty;
            for (var i = 0; i < n; i++)
                v = v.InsertAt(f._Get(db), i);
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
        public SRecord(STransaction tr,long t,SDict<long,Serialisable> f) :base(Types.SRecord,tr)
        {
            fields = f;
            table = t;
        }
        public virtual long Defpos => uid;
        public SRecord(SDatabase db,SRecord r,AStream f) : base(r,f)
        {
            table = f.Fix(r.table);
            var fs = r.fields;
            f.PutLong(table);
            var tb = (STable)db.Lookup(table);
            f.PutInt(r.fields.Length);
            for (var b=fs.First();b!=null;b=b.Next())
            {
                var oc = b.Value.key;
                var v = b.Value.val;
                var c = oc;
                if (f.uids.Contains(c))
                {
                    c = f.uids.Lookup(c);
                    fs = fs.Remove(oc).Add(c, v);
                }
                f.PutLong(c);
                v.Put(f);
            }
            fields = fs;
        }
        protected SRecord(SDatabase d, Reader f) : base(Types.SRecord,f)
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
        public static SRecord Get(SDatabase d, Reader f)
        {
            return new SRecord(d,f);
        }
        public override void Append(SDatabase? db, StringBuilder sb)
        {
            sb.Append("{");
            if (Defpos<STransaction._uid)
            {
                sb.Append("_id:"); sb.Append(Defpos); sb.Append(",");
            }
            var tb = db?.objects.Lookup(table) as STable;
            sb.Append("_table:");
            sb.Append('"'); sb.Append(tb?.name ?? ("" + table)); sb.Append('"');
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(",");
                var c = tb?.cols.Lookup(b.Value.key);
                sb.Append(c?.name ?? ("" + b.Value.key)); sb.Append(":");
                b.Value.val.Append(db,sb);
            }
            sb.Append("}");
        }
        public bool Matches(RowBookmark rb,SList<Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value is SExpression x && x.Eval(rb) is SBoolean e 
                    && e.sbool!=SBool.True)
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
        public readonly SDict<SSelector, Serialisable> assigs;
        public SUpdateSearch(SQuery q,SDict<SSelector,Serialisable> a)
            : base(Types.SUpdateSearch)
        {
            qry = q; assigs = a;
        }
        public static void Obey(STransaction tr,Reader rdr)
        {
            var qry = (SQuery)rdr._Get(tr);
            var n = rdr.GetInt(); // # cols updated
            var f = SDict<long, Serialisable>.Empty;
            Exception? ex = null;
            for (var i = 0; i < n; i++)
            {
                var cn = rdr.GetString();
                if (qry.names.Lookup(cn) is SColumn sc)
                    f = f.Add(sc.uid, rdr._Get(tr));
                else
                    ex = new Exception("Column " + cn + " not found");
            }
            if (ex != null)
                throw (ex);
            for (var b = qry.RowSet(tr).First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
                tr = tr.Add(new SUpdate(tr, b._ob.rec ?? throw new System.Exception("??"), f)); // not null
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
            f.PutInt(assigs.Length);
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                b.Value.key.Put(f); b.Value.val.Put(f);
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
                sb.Append(b.Value.key);sb.Append('=');sb.Append(b.Value.val);
            }
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
        SUpdate(SDatabase d, Reader f) : base(d,f)
        {
            defpos = f.GetLong();
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
        public static void Obey(STransaction tr,Reader rdr)
        {
            var qry = (SQuery)rdr._Get(tr);
            for (var b = qry.RowSet(tr).First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var rc = b._ob.rec ?? throw new System.Exception("??");// not null
                tr = tr.Add(new SDelete(tr, rc.table, rc.uid)); 
            }
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
            rows = new SMTree<long>(Info((STable)tr.Lookup(table), cols));
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
            rows = new SMTree<long>(Info((STable)d.Lookup(table), cols));
        }
        public SIndex(SIndex x, AStream f) : base(x, f)
        {
            table = f.Fix(x.table);
            f.PutLong(table);
            primary = x.primary;
            f.WriteByte((byte)(primary ? 1 : 0));
            long[] c = new long[x.cols.Length.Value];
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
            return Info(tb, cols.next).InsertAt(new TreeInfo<long>( // not null
                tb.cols.Lookup(cols.element).uid, (cols.Length!=1 || !primary)?'A':'D', 'D'), 0);
        }
        SCList<Variant> Key(SRecord sr,SList<long> cols)
        {
            if (cols.Length == 0)
                return SCList<Variant>.Empty;
            return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element)), Key(sr, cols.next)); // not null
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
        public Serialisable _Get(SDatabase d)
        {
            Types tp = (Types)ReadByte();
            Serialisable s;
            switch (tp)
            {
                case Types.Serialisable: s = Serialisable.Get(this); break;
                case Types.STimestamp: s = STimestamp.Get(this); break;
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
                case Types.SSearch: s = SSearch.Get(d,this); break;
                case Types.SSelect: s = SSelectStatement.Get(d, this); break;
                case Types.SValues: s = SValues.Get(d, this); break;
                case Types.SExpression: s = SExpression.Get(d, this); break;
                case Types.SFunction: s = SFunction.Get(d, this); break;
                case Types.SOrder: s = SOrder.Get(d, this); break;
                case Types.SInPredicate: s = SInPredicate.Get(d, this); break;
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
            return db.Lookup(((AStream)buf.fs).Fix(pos));
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
        public AStream(string fn)
        {
            filename = fn;
            file = new FileStream(fn,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
            length = file.Seek(0, SeekOrigin.End);
            file.Seek(0, SeekOrigin.Begin);
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
        internal Serialisable Lookup(SDatabase db, long pos)
        {
            return new Reader(this, pos)._Get(db);
        }
        internal long Fix(long pos)
        {
            return (uids.Contains(pos))?uids.Lookup(pos):pos;
        }
        public override bool CanRead => throw new System.NotImplementedException();

        public override bool CanSeek => throw new System.NotImplementedException();

        public override bool CanWrite => throw new System.NotImplementedException();

        public override long Length => length + (wbuf?.wpos)??0;

        public override long Position { get => wposition; set => throw new System.NotImplementedException(); }
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
                if (b.start > length)
                    return false;
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
            var p = file.Seek(0, SeekOrigin.End);
            file.Write(b.buf, 0, b.wpos);
            file.Flush();
            length = p+b.wpos;
            b.wpos = 0;
        }
    }
}
