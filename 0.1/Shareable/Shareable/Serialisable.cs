using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
        SCreateColumn = 23,
        SUpdateSearch = 24,
        SDeleteSearch = 25,
        SAlterStatement = 26,
        SInsert = 27,
        SSelect = 28,
        EoF = 29,
        Get = 30,
        Insert = 31,
        Read = 32,
        Done = 33,
        Exception = 34,
        SExpression = 35,
        SFunction = 36,
        SValues = 37,
        SOrder = 38,
        SBigInt = 39,
        SInPredicate = 40,
        DescribedGet = 41,
        SGroupQuery = 42,
        STableExp = 43,
        SAlias = 44,
        SSelector = 45,
        SArg = 46,
        SRole = 47,
        SUser = 48,
        SName = 49,
        SNames = 50,
        SQuery = 51, // only used for "STATIC"
        SSysTable = 52,
        SCreateView = 53
    }
    public interface ILookup<K, V> where K : IComparable
    {
        bool defines(K s);
        V this[K s] { get; }
    }
    public class Context 
    {
        public readonly ILookup<long, Serialisable> refs;
        public readonly Context? next;
        public static readonly Context Empty =
             new Context(SDict<long, Serialisable>.Empty); 
        public Context(ILookup<long, Serialisable> a,Context? n=null)
        {
            refs = a; next = n; 
        }
        public static Context Append(Context a,Context b)
        {
            if (a.next == null)
                return new Context(a.refs, b);
            return new Context(a.refs, Append(a.next, b));
        }
        public SRow Row()
        {
            return (refs is SRow r) ? r : next?.Row() ?? throw new Exception("PE05");
        }
        public SDict<long,Serialisable> Ags()
        {
            if (refs is SDict<long,Serialisable> r)
            {
                var f = r.First();
                if (f == null || f.Value.Item2.type!=Types.SRow)
                    return r;
            }
            return next?.Ags() ?? throw new Exception("PE25");
        }
        public Serialisable this[long f] => (this == Empty) ? Serialisable.Null :
        refs.defines(f) ? refs[f] : next?[f] ?? Serialisable.Null;
        public bool defines(long f)
        {
            return (this == Empty) ? false : refs.defines(f) || (next?.defines(f) ?? false);
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
        /// <summary>
        /// Prepare is used by the server to make a transformed version of the Serialisable that
        /// replaces all client-side uids with server-side uids apart from aliases.
        /// This speeds things up in stored procs etc.
        /// With single-statement execution it is called before Obey.
        /// </summary>
        /// <param name="db">the transaction</param>
        /// <param name="pt">a parser lookup table (client side uids to server uids)</param>
        /// <returns></returns>
        public virtual Serialisable Prepare(STransaction db,SDict<long,long> pt)
        {
            return this;
        }
        /// <summary>
        /// The UseAliases machinery is used by the server when a viewdefinition is the target of a query
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ta"></param>
        /// <returns></returns>
        public virtual Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            return this;
        }
        /// <summary>
        /// The UpdateAliases machinery is used by the client at the end of parsing a SelectStatement
        /// </summary>
        /// <param name="uids"></param>
        /// <returns></returns>
        public virtual Serialisable UpdateAliases(SDict<long, string> uids)
        {
            return this;
        }
        /// <summary>
        /// Obey is used in the server to make changes to the transaction
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="cx"></param>
        /// <returns></returns>
        public virtual STransaction Obey(STransaction tr,Context cx)
        {
            return tr;
        }
        /// <summary>
        /// Aggregates is used by the server in query processing
        /// </summary>
        /// <param name="ags"></param>
        /// <returns></returns>
        public virtual SDict<long,Serialisable> Aggregates(SDict<long,Serialisable> ags)
        {
            return ags;
        }
        public virtual bool isValue => true;
        /// <summary>
        /// Put is used in serialisation by client and server
        /// </summary>
        /// <param name="f"></param>
        public virtual void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
        }
        /// <summary>
        /// Conflicts is used by the server in commit validation 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tr"></param>
        /// <param name="that"></param>
        /// <returns></returns>
        public virtual bool Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            return false;
        }
        /// <summary>
        /// Append is used by the server when preparing results to send to the client
        /// </summary>
        /// <param name="db"></param>
        /// <param name="sb"></param>
        public virtual void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append(this);
        }
        /// <summary>
        /// Append is used to create readable versions of database objects
        /// </summary>
        /// <param name="sb"></param>
        public virtual void Append(StringBuilder sb)
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
        public Serialisable Coerce(Types t)
        {
            if (type == t || type==Types.Serialisable)
                return this;
            switch (t)
            {
                case Types.SInteger:
                    switch(type)
                    {
                        case Types.SBigInt:
                            return this;
                        default: throw new Exception("Expected integer got " + type);
                    }
                case Types.SNumeric:
                    switch (type)
                    {
                        case Types.SInteger:
                            var n = (SInteger)this;
                            if (n.big is Integer b)
                                return new SNumeric(b, 12, 0);
                            return new SNumeric(n.value, 12, 0);
                        default:
                            throw new Exception("Expected numeric got " + type);
                    }
            }
            throw new Exception("Expected " + t + " got " + type);
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
        public virtual Serialisable Fix(AStream f)
        {
            return this;
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
        public virtual Serialisable Lookup(STransaction tr,Context cx)
        {
            return this;
        }
        /// <summary>
        /// For readConstraint checking
        /// </summary>
        /// <param name=""></param>
        /// <returns></returns>
        public virtual bool Check(SDict<long,bool> rdC)
        {
            return false;
        }
        public virtual Serialisable this[long col]
        { get { throw new NotImplementedException(); } }
        public override string ToString()
        {
            return "NULL";
        }
    }
    public class SExpression : SDbObject
    {
        public readonly Serialisable left, right;
        public readonly Op op;
        public SExpression(Serialisable lf,Op o,Reader f, long u) : base(Types.SExpression,u)
        {
            left = lf;
            op = o;
            right = f._Get();
        }
        public SExpression(Serialisable lf,Op o,Serialisable rt) : base(Types.SExpression)
        {
            left = lf; right = rt; op = o;
        }
        public override bool isValue => false;
        public enum Op { Plus, Minus, Times, Divide, Eql, NotEql, Lss, Leq, Gtr, Geq, Dot, And, Or, UMinus, Not };
        internal new static SExpression Get(Reader f)
        {
            var u = f.GetLong();
            var lf = f._Get();
            return new SExpression(lf, (Op)f.ReadByte(), f, u);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            if (op == Op.Dot)
                return new SExpression(left.UseAliases(db,ta), op, right);
            return new SExpression(left.UseAliases(db, ta),op,right.UseAliases(db,ta));
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var lf = left.Prepare(db, pt);
            if (op == Op.Dot && lf is SDbObject ob && db.objects[ob.uid] is SQuery qq)
                pt = qq.Names(db,pt);
            return new SExpression(lf, op, right.Prepare(db, pt));
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var lf = left.UpdateAliases(uids);
            var rg = right.UpdateAliases(uids);
            return (lf == left && rg == right) ?
                this : new SExpression(lf, op, rg);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            left.Put(f);
            f.WriteByte((byte)op);
            right.Put(f);
        }
        public override Serialisable Fix(AStream f)
        {
            return new SExpression(left.Fix(f),op,right.Fix(f));
        }
        public override SDict<long,Serialisable> Aggregates(SDict<long, Serialisable> ags)
        {
            if (left != null)
                ags = left.Aggregates(ags);
            if (right != null)
                ags = right.Aggregates(ags);
            return ags;
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            var lf = left.Lookup(tr, cx);
            if (op == Op.Dot && right is SDbObject rn)
            {
                if (lf is SRow rw && rw.defines(rn.uid))
                            return rw[rn.uid];
                if (lf is SDbObject ln && cx.defines(ln.uid) && 
                    cx[ln.uid] is SRow sr && sr.defines(rn.uid))
                            return sr[rn.uid];
                return this;
            }
            var rg = right.Lookup(tr,cx);
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
                                            return new SInteger(new Integer(lv) + getbig(rg));
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
                                            return new SInteger(lv + new Integer(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SInteger(lv + getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0) + ((SNumeric)rg).num);
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
                                            return new SInteger(new Integer(lv) - getbig(rg));
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
                                            return new SInteger(lv - new Integer(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SInteger(lv - getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0) - ((SNumeric)rg).num);
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
                                            return new SInteger(new Integer(lv) * getbig(rg));
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
                                            return new SInteger(lv * new Integer(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SInteger(lv * getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0) * ((SNumeric)rg).num);
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
                                            return new SInteger(new Integer(lv) / getbig(rg));
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
                                            return new SInteger(lv / new Integer(((SInteger)rg).value));
                                        case Types.SBigInt:
                                            return new SInteger(lv / getbig(rg));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Numeric(lv, 0) / ((SNumeric)rg).num));
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
                case Op.Eql: return SBoolean.For(lf.CompareTo(rg) == 0); 
                case Op.NotEql: return SBoolean.For(lf.CompareTo(rg) != 0);
                case Op.Leq: return SBoolean.For(lf.CompareTo(rg) <= 0);
                case Op.Lss: return SBoolean.For(lf.CompareTo(rg) < 0);
                case Op.Geq: return SBoolean.For(lf.CompareTo(rg) >= 0);
                case Op.Gtr: return SBoolean.For(lf.CompareTo(rg) > 0);
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
                        var ls = (ILookup<long,Serialisable>)left.Lookup(tr,cx);
                        if (ls != null)
                            return ls[((SDbObject)right).uid];
                        break;
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
        public override string ToString()
        {
            return left.ToString()+" "+op.ToString()+" "+right.ToString();
        }
    }
    /// <summary>
    /// SArg is a reference to a value pushed onto the Context.
    /// For constraints the target is unnamed (SValue gives SArg.Value),
    /// and is found in the context by the uid of the constrained object or proc.
    /// The value might be a Row (a set of named parameters or local variables)
    /// </summary>
    public class SArg : Serialisable
    {
        public readonly SDbObject target;
        public static SArg Value = new SArg();
        SArg() : base(Types.SArg) { target = SRole.Public; }
        public SArg(Reader f) : base(Types.SArg)
        {
            target = f.context;
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            return cx.refs[target.uid];
        }
        public override bool isValue => false;
    }
    public class SFunction : Serialisable
    {
        public readonly Serialisable arg; // probably an SQuery
        public readonly Func func;
        public readonly long fid = --SysTable._uid; // we will have a list of function expressions
        public SFunction(Func fn,Reader f) : base(Types.SFunction)
        {
            func = fn;
            arg = f._Get();
        }
        public SFunction(Func fn, Serialisable a) : base(Types.SFunction)
        {
            func = fn;
            arg = a;
        }
        public override Serialisable Fix(AStream f)
        {
            return new SFunction(func,arg.Fix(f));
        }
        public override bool isValue => false;
        public enum Func { Sum, Count, Max, Min, Null, NotNull, Constraint, Default, Generated };
        internal new static SFunction Get(Reader f)
        {
            return new SFunction((Func)f.ReadByte(), f);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            return new SFunction(func,arg.UseAliases(db, ta));
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            return new SFunction(func,arg.UpdateAliases(uids));
        }
        public override Serialisable Prepare(STransaction db, SDict<long,long> pt)
        {
            return new SFunction(func, arg.Prepare(db, pt));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)func);
            arg.Put(f);
        }
        public bool IsAgg => (func!=Func.Null);
        public override SDict<long, Serialisable> Aggregates(SDict<long, Serialisable> ags)
        {
            return (func == Func.Constraint || IsAgg) ? ags + (fid, this) : ags;
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            if (cx.refs==SDict<long,Serialisable>.Empty)
                return this;
            var x = arg.Lookup(tr,cx);
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
                    if (v == Null)
                        return a;
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
                                        return new SInteger(new Integer(lv) + getbig(v));
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
                                        return new SInteger(lv + new Integer(((SInteger)v).value));
                                    case Types.SBigInt:
                                        return new SInteger(lv + getbig(v));
                                    case Types.SNumeric:
                                        return new SNumeric(new Numeric(lv, 0) + ((SNumeric)v).num);
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
        public override string ToString()
        {
            return func.ToString() + "(" + arg + ")";
        }
    }
    public class SInPredicate : Serialisable
    {
        public readonly Serialisable arg;
        public readonly Serialisable list;
        public SInPredicate(Serialisable a,Serialisable r):base(Types.SInPredicate)
        {
            arg = a; list = r;
        }
        public override bool isValue => false;
        public new static SInPredicate Get(Reader f)
        {
            var a = f._Get();
            return new SInPredicate(a, f._Get());
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var a = arg.Prepare(db,pt);
            if (list is SQuery q)
                pt = q.Names(db, pt);
            return new SInPredicate(a,list.Prepare(db,pt));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            arg.Put(f);
            list.Put(f);
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            var a = arg.Lookup(tr,cx);
            var ls = list.Lookup(tr,cx);
            switch(ls.type)
            {
                case Types.SValues:
                    for (var b = ((SValues)ls).vals.First(); b != null; b = b.Next())
                        if (b.Value.CompareTo(a) == 0)
                            return SBoolean.True;
                    break;
                case Types.SRow:
                    if (list.type == Types.SSelect)
                        goto case Types.SSelect;
                    for (var b = ((SRow)ls).cols.First(); b != null; b = b.Next())
                        if (b.Value.Item2.CompareTo(a) == 0)
                            return SBoolean.True;
                    break;
                case Types.SSelect:
                    {
                        var ss = (SSelectStatement)list;
                        for (var b = ss.RowSet(tr,ss,Context.Empty).First(); b != null; b = b.Next())
                            if (b.Value.CompareTo(a) == 0)
                                return SBoolean.True;
                    }
                    break;
            }
            return SBoolean.False;
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
        public readonly long value;
        public readonly Integer? big;
        public static readonly SInteger Zero = new SInteger(0);
        public static readonly SInteger One = new SInteger(1);
        public SInteger(long v) : base(Types.SInteger)
        {
            value = v; big = null;
        }
        public SInteger(Integer b) : base(b<new Integer(long.MaxValue)&&
            b>new Integer(long.MinValue)?Types.SInteger:Types.SBigInt)
        {
            switch (type)
            {
                case Types.SInteger:
                    value = (long)b; big = null; break;
                case Types.SBigInt:
                    value = 0; big = b; break;
            }
        }
        SInteger(Reader f) : this(f.GetInteger())
        {
        }
        public override bool isValue => true;
        public new static Serialisable Get(Reader f)
        {
            return new SInteger(f);
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            switch (type)
            {
                case Types.SInteger:
                    sb.Append(value);
                    break;
                case Types.SBigInt:
                    sb.Append(big?.ToString() ?? "");
                    break;
            }
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            switch(type)
            {
                case Types.SInteger:
                    f.PutLong(value);
                    break;
                case Types.SBigInt:
                    f.PutInteger(big ?? Integer.Zero);
                    break;
            }
        }
        public override string ToString()
        {
            return "Integer " + ((type==Types.SInteger)?value.ToString():big?.ToString()??"");
        }
        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2 ?? Null);
            var rg = (Serialisable)obj;
            if (type == Types.SInteger)
                switch (rg.type)
                {
                    case Types.SInteger:
                        return value.CompareTo(((SInteger)rg).value);
                    case Types.SBigInt:
                        return value.CompareTo(((SInteger)rg).big);
                    case Types.SNumeric:
                        return new Numeric(new Integer(value), 0).CompareTo(((SNumeric)rg).num);
                }
            else
            {
                var bg = big ?? throw new Exception("PE09");
                switch (rg.type)
                {
                    case Types.SInteger:
                        return bg.CompareTo(((SInteger)rg).value);
                    case Types.SBigInt:
                        return bg.CompareTo(((SInteger)rg).big ?? throw new Exception("PE10"));
                    case Types.SNumeric:
                        return new Numeric(bg, 0).CompareTo(((SNumeric)rg).num);
                }
            }
            throw new Exception("PE08");
        }
    }
    public class SNumeric : Serialisable,IComparable
    {
        public readonly Numeric num;
        public SNumeric(Numeric n) :base (Types.SNumeric)
        {
            num = n;
        }
        public SNumeric(Integer m,int p,int s) : base(Types.SNumeric)
        {
            num = new Numeric(m, s, p);
        }
        public SNumeric(long m, int p, int s) : base(Types.SNumeric)
        {
            num = new Numeric(m, s, p);
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
            f.PutInteger(num.mantissa);
            f.PutInt(num.precision);
            f.PutInt(num.scale);
        }
        public new static Serialisable Get(Reader f)
        {
            return new SNumeric(f);
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append((double)num.mantissa * Math.Pow(10.0, -num.scale));
        }
        public double ToDouble()
        {
            return (double)num.mantissa * Math.Pow(10.0, num.scale);
        }
        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2 ?? Null);
            var rg = (Serialisable)obj;
            switch (rg.type)
            {
                case Types.SInteger:
                    return num.CompareTo(new Numeric(((SInteger)rg).value)); 
                case Types.SBigInt:
                    return num.CompareTo(new Numeric(
                        ((SInteger)rg).big??throw new Exception("PE11"), 0));
                case Types.SNumeric:
                    return num.CompareTo(((SNumeric)rg).num);
            }
            throw new Exception("PE12");
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
        public override void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append("'"); sb.Append(str.Replace("'","''")); sb.Append("'");
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2 ?? Null);
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
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2??Null);
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
            ts = new TimeSpan((long)f.GetInteger());
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
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2 ?? Null);
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
        public override void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append(this);
        }

        public override int CompareTo(object obj)
        {
            if (obj == Null)
                return 1;
            if (obj is SRow sr && sr.cols.Length == 1)
                return CompareTo(sr.vals.First()?.Value.Item2 ?? Null);
            var that = (SBoolean)obj;
            return sbool.CompareTo(that.sbool);
        }
        public override string ToString()
        {
            return sbool ? "\"true\"" : "\"false\"";
        }
    }
    public class SRow : Serialisable,ILookup<long,Serialisable>,IComparable
    {
        public readonly SDict<int, (long,string)> names;
        public readonly SDict<int, Serialisable> cols;
        public readonly SDict<long, Serialisable> vals;
        public readonly bool isNull;
        public readonly SRecord? rec;
        public SRow() : base(Types.SRow)
        {
            names = SDict<int,(long,string)>.Empty;
            cols = SDict<int, Serialisable>.Empty;
            vals = SDict<long, Serialisable>.Empty;
            isNull = true;
            rec = null;
        }
        public override bool isValue
        {
            get
            {
                for (var b = cols.First(); b != null; b = b.Next())
                    if (!b.Value.Item2.isValue)
                        return false;
                return true;
            }
        }
        protected SRow Add(((long,string),Serialisable) v)
        {
            return new SRow(names+(names.Length??0,v.Item1),cols+(cols.Length??0,v.Item2),
                vals+(v.Item1.Item1,v.Item2),rec);
        }
        public static SRow operator+(SRow s,((long,string),Serialisable)v)
        {
            return s.Add(v);
        }
        SRow(SDict<int,(long,string)> n,SDict<int,Serialisable> c,SDict<long,Serialisable> v,SRecord? r) 
            :base(Types.SRow)
        {
            names = n;
            cols = c;
            vals = v;
            rec = r;
            isNull = false;
        }
        public SRow(SList<(long,string)> a, SList<Serialisable> s) :base(Types.SRow)
        {
            var cn = SDict<int, (long,string)>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<long, Serialisable>.Empty;
            var k = 0;
            var isn = true;
            for (;s.Length!=0;s=s.next,a=a.next) // not null
            {
                var n = a.element;
                cn += (k, n);
                r += (k++, s.element);
                vs += (n.Item1, s.element);
                if (s.element != Null)
                    isn = false;
            }
            names = cn;
            cols = r;
            vals = vs;
            rec = null;
            isNull = isn;
        }
        SRow(Reader f) :base(Types.SRow)
        {
            var n = f.GetInt();
            var cn = SDict<int, (long,string)>.Empty;
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<long, Serialisable>.Empty;
            for(var i=0;i<n;i++)
            {
                var k = f.GetLong();
                cn += (i, (k,f.db.Name(k)));
                var v = f._Get();
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
            var cn = SDict<int, (long,string)>.Empty;
            var co = SDict<int, Serialisable>.Empty;
            var vs = SDict<long, Serialisable>.Empty;
            var k = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SColumn sc)
                {
                    var v = r.fields.Lookup(sc.uid)??Null;
                    co += (k, v);
                    cn += (k++, (sc.uid,db.Name(sc.uid)));
                    vs += (sc.uid, v);
                }
                else
                    throw new Exception("Unimplemented selector");
            names = cn;
            cols = co;
            vals = vs;
            rec = r;
            isNull = false;
        }
        public SRow(STransaction tr,SSelectStatement ss, Context cx) : base(Types.SRow)
        {
            var r = SDict<int, Serialisable>.Empty;
            var vs = SDict<long, Serialisable>.Empty;
            var isn = true;
            var cb = ss.cpos.First();
            for (var b = ss.display.First(); cb != null && b != null; b = b.Next(), cb = cb.Next())
            {
                var v = cb.Value.Item2.Lookup(tr,cx) ?? Null;
                if (v is SRow sr && sr.cols.Length == 1)
                    v = sr.cols.Lookup(0) ?? Null;
                if (v != null)
                {
                    r += (b.Value.Item1, v);
                    vs += (b.Value.Item2.Item1, v);
                }
                if (v != Null)
                    isn = false;
            }
            names = ss.display;
            cols = r;
            vals = vs;
            rec = (cx.refs as SRow)?.rec;
            isNull = isn;
        }
        public new static SRow Get(Reader f)
        {
            return new SRow(f);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var ns = SList<(long,string)>.Empty;
            var vs = SList<Serialisable>.Empty;
            for (var i=0;i<names.Length;i++)
            {
                var n = names[i];
                ns += (ta.Contains(n.Item1)?(ta[n.Item1],n.Item2):n, i);
                vs += (cols[i].UseAliases(db, ta), i);
            }
            return new SRow(ns, vs);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var nms = SDict<int, (long,string)>.Empty;
            var cls = SDict<int, Serialisable>.Empty;
            var vls = SDict<long, Serialisable>.Empty;
            var nb = names.First();
            for (var b=cols.First();nb!=null&&b!=null;b=b.Next(),nb=nb.Next())
            {
                var n = nb.Value;
                var u = SDbObject.Prepare(n.Item2.Item1, pt);
                var id = (u, n.Item2.Item2);
                var v = b.Value.Item2.Prepare(db, pt);
                nms += (n.Item1,id);
                cls += (b.Value.Item1, v);
                vls += (u, v);
            }
            return new SRow(nms,cls,vls,rec);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(names.Length);
            var cb = cols.First();
            for (var b = names.First(); cb!=null && b != null; b = b.Next(),cb=cb.Next())
            {
                f.PutLong(b.Value.Item2.Item1);
                if (cb.Value.Item2 is Serialisable s)
                    s.Put(f);
                else
                    Null.Put(f);
            }
        }
        public override Serialisable this[long col] => vals[col];
        public Serialisable this[int j] => cols[j];
        public override int CompareTo(object ob)
        {
            if (ob is SRow sr && sr.cols.Length==cols.Length)
            {
                var ab = sr.vals.First();
                for (var b = vals.First(); ab != null && b != null; ab = ab.Next(), b=b.Next())
                {
                    var c = b.Value.Item2.CompareTo(ab.Value.Item2);
                    if (c != 0)
                        return c;
                }
                return 0;
            }
            if (cols.Length == 1)
                return (vals?.First()?.Value.Item2 ?? Null).CompareTo(ob);
            throw new Exception("PE19");
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append('{');
            var cm = "";
            var nb = names.First();
            for (var b = cols.First(); nb!=null && b != null; b = b.Next(),nb=nb.Next())
                if (b.Value.Item2 is Serialisable s && s!=Null)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(nb.Value.Item2.Item2);
                    sb.Append(":");
                    s.Append(db, sb);
                }
            sb.Append("}");
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append('{');
            var cm = "";
            var nb = names.First();
            for (var b = cols.First(); nb != null && b != null; b = b.Next(), nb = nb.Next())
                if (b.Value.Item2 is Serialisable s && s != Null)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(nb.Value.Item2.Item2);
                    sb.Append(":");
                    s.Append(sb);
                }
            sb.Append("}");
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            var v = SDict<int, Serialisable>.Empty;
            var r = SDict<long, Serialisable>.Empty;
            var nb = names.First();
            for (var b = cols.First(); nb != null && b != null; nb = nb.Next(), b = b.Next())
            {
                var e = b.Value.Item2.Lookup(tr,cx);
                v += (b.Value.Item1, e);
                r += (nb.Value.Item2.Item1, e);
            }
            return new SRow(names, v, r, (cx.refs as RowBookmark)?._ob.rec);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("SRow ");
            Append(sb);
            return sb.ToString();
        }

        public bool defines(long s)
        {
            return vals.Contains(s);
        }
    }
    public class SDbObject : Serialisable
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
        /// Transaction-local uids: long.MaxValue>>2 to long.MaxValue
        /// System uids (_Log tables etc): -long.MinValue+1- to -2000001
        /// -1 is a special uid for the PUBLIC SRole
        /// Client session-local objects are given negative uids -1000000 to -2.
        /// Aliases for a query have negative uids below -1000001
        /// </summary>
        public readonly long uid;
        public static readonly long maxAlias = -1000001;
        /// <summary>
        /// For system tables and columns, with negative uids
        /// </summary>
        /// <param name="t"></param>
        /// <param name="u"></param>
        public SDbObject(Types t,long u=-1) :base(t)
        {
            uid = u;
        }
        /// <summary>
        /// For a new database object we set the transaction-based uid.
        /// (Install will give a new transaction with a new uid.)
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
        protected SDbObject(Types t, Reader f) : base(t)
        {
            if (t == Types.SName)
                uid = f.GetLong();
            else  // a new object is being defined
                if (f is SocketReader)
                {
                    var u = f.GetLong();

                    uid = ((STransaction)f.db).uid + 1;
                    if (u != -1) // keep track of the client-side name
                        f.db += (uid, f.db.role[u]);
                }
                else // file position is uid
                    uid = f.Position - 1;
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
        public virtual long Affects => uid;
        public new static SDbObject Get(Reader f)
        {
            return new SDbObject(Types.SName,f);
        }
        public override Serialisable Prepare(STransaction tr, SDict<long, long> pt)
        {
            if (uid < maxAlias || uid >= 0)
                return this;
            if (!pt.Contains(uid))
                throw new Exception("Could not find " + tr.Name(uid));
            return tr.objects[pt[uid]];
        }
        public override Serialisable UseAliases(SDatabase db,SDict<long,long> ta)
        {
            return (ta.Contains(uid)) ?
                new SDbObject(Types.SName, ta[uid]) : this;
        }
        public override Serialisable UpdateAliases(SDict<long,string> uids)
        {
            return (uids.Contains(uid-1000000))?
                new SDbObject(Types.SName,uid-1000000):this;
        }
        public static long Prepare(long u, SDict<long, long> pt)
        {
            if (u >= 1 || u < maxAlias)
                return u;
            if (!pt.Contains(u))
                throw new Exception("Could not find " + _Uid(u));
            return pt[u];
        }
        public static (long,string) Prepare((long,string) n,SDict<long,long> pt)
        {
            var u = n.Item1;
            if (u>=1 || u < maxAlias)
                return n;
            if (!pt.Contains(u))
                throw new Exception("Could not find " + n.Item2);
            return (pt[u],n.Item2);
        }
        public static (long,long) Prepare((long,long) pair,SDict<long,long> pt)
        {
            return (Prepare(pair.Item1, pt), Prepare(pair.Item2, pt));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(uid);
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            if (cx.defines(uid))
                return cx[uid];
            return base.Lookup(tr,cx);
        }
        internal string Uid()
        {
            return _Uid(uid);
        }
        internal static string _Uid(long uid)
        {
            if (uid > STransaction._uid)
                return "'" + (uid - STransaction._uid);
            if (uid < 0 && uid > -1000000)
                return "#" + (-uid);
            if (uid < -1000000 && uid > -0x7000000000000000)
                return "$" + (-1000000-uid);
            if (uid <= -0x7000000000000000)
                return "@" + (0x7000000000000000 + uid);
            return "" + uid;
        }
        public override string ToString()
        {
            return "SName "+_Uid(uid);
        }
    }
    public class STable : SQuery
    {
        public readonly SDict<long, SColumn> cols; // keys are uids for long column names
        public readonly SDict<long, long> rows; // defpos->uid of latest update
        public readonly SDict<long,bool> indexes;
        /// <summary>
        /// A newly defined empty table: it will immediately get installed in the transaction
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="n"></param>
        public STable(STransaction tr) :base(Types.STable,tr)
        {
            cols = SDict<long, SColumn>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        protected virtual STable Add(SColumn c,string s)
        {
            var t = new STable(this,cols + (c.uid,c),
                display+(display.Length??0,(c.uid,s)),
                cpos + (cpos.Length??0,c),
                refs + (c.uid,c));
            return t;
        }
        public static STable operator+(STable t,(SColumn,string) c)
        {
            return t.Add(c.Item1,c.Item2);
        }
        protected STable Add(SRecord r)
        {
            return new STable(this,rows + (r.Defpos, r.uid));
        }
        public static STable operator+(STable t,SRecord r)
        {
            return t.Add(r);
        }
        public SColumn FindForRole(SDatabase db,string nm)
        {
            return (SColumn)db.objects[db.role.defs[uid][nm]];
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
                var di = SDict<int, (long,string)>.Empty;
                var sc = cols[n];
                var db = display.First();
                for (var b = cpos.First();db!=null && b != null; db=db.Next(), b = b.Next())
                    if (b.Value.Item2 is SColumn c && c.uid != n)
                    {
                        di += (k, db.Value.Item2);
                        cp += (k++, c);
                    }
                return new STable(this,cols-n,di,cp,refs-sc.uid);
            }
            else
                return new STable(this, rows-n);
        }
        public STable(long u = -1) : this(Types.STable, u) { }
        public STable(Types t,long u=-1)
            : base(t, u)
        {
            cols = SDict<long, SColumn>.Empty;
            rows = SDict<long, long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        public STable(Types t,STable tb)  : base(t,tb)
        {
            cols = tb.cols;
            rows = tb.rows;
            indexes = tb.indexes;
        }
        protected STable(STable t) :base(t)
        {
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        protected STable(STable t, SDict<long, SColumn> co, 
            SDict<int,(long,string)> a,
            SDict<int,Serialisable> cp, SDict<long,Serialisable> na) 
            :base(t,a,cp,na)
        {
            cols = co;
            rows = t.rows;
            indexes = t.indexes;
        }
        protected STable(STable t,SDict<long,long> r) : base(t)
        {
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
        public STable(STable t,string nm, AStream f) :base(t,f)
        {
            f.PutString(nm);
            cols = SDict<long, SColumn>.Empty;
            rows = SDict<long,long>.Empty;
            indexes = SDict<long, bool>.Empty;
        }
        internal STable(STable t,SDict<long,bool> x) :base(t)
        {
            cols = t.cols;
            rows = t.rows;
            indexes = x;
        }
        /// <summary>
        /// If f is a SocketReader, this is a reference in a query to an existing table.
        /// Otherwise we are loading a CreateTable.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public new static STable Get(Reader f)
        {
            var db = f.db;
            if (f is SocketReader)
            {
                var u = f.GetLong();
                var nm = db.role.uids[u];
                if (!db.role.globalNames.Contains(nm))
                    throw new Exception("No table " + nm);
                return (STable)db.objects[db.role.globalNames[nm]];
            }
            var c = f.Position - 1;
            var tb = new STable(c);
            var tn = f.GetString();
            f.db = db.Install(tb,tn,c);
            return tb;
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            return this;
        }
        public override Serialisable UseAliases(SDatabase db,SDict<long, long> ta)
        {
            if (ta.Contains(uid))
                return new SAlias(this, ta[uid], uid);
            return base.UseAliases(db,ta);
        }
        public override RowSet RowSet(STransaction tr,SQuery top,Context cx)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)tr.objects[b.Value.Item1];
                if (x.references < 0)
                    return new IndexRowSet(tr, this, x, SCList<Variant>.Empty,
                        SExpression.Op.NotEql,SList<Serialisable>.Empty, cx);
            }
            return new TableRowSet(tr, this, cx);
        }
        public SRecord Check(STransaction tr,SRecord rc)
        {
            // check primary/unique key for nulls
            for (var b=indexes.First();b!=null;b=b.Next())
            {
                var x = (SIndex)tr.objects[b.Value.Item1];
                var k = x.Key(rc, x.cols);
                var i = 0;
                for (var kb=k.First();kb!=null;kb=kb.Next(),i++)
                    if (kb.Value.ob==null)
                    {
                        if (x.primary && i == x.cols.Length - 1)
                        { 
                            long cu=0;
                            var j = 0;
                            var mb = x.rows.PositionAt(k);
                            for (var cb = x.cols.First(); j <= i && cb != null; cb = cb.Next(), j++)
                            {
                                cu = cb.Value;
                                if (j<i)
                                    mb = mb._inner;
                            }
                            var sc = (SColumn)tr.objects[cu];
                            if (sc.dataType == Types.SInteger)
                            {
                                var bu = mb._outer;
                                while (bu._parent != null)
                                    bu = bu._parent;
                                var v = new SInteger(((SInteger)bu._bucket.Last().ob).value + 1);
                                var f = rc.fields;
                                f += (cu,v);
                                return new SRecord(tr, rc.table, f);
                            }
                        }
                        throw new Exception("Illegal null value in primary key");
                    }
            }
            return rc;
        }
        public override bool Conflicts(SDatabase db, STransaction tr, Serialisable that)
        {
            return that.type == Types.STable &&
                db.Name(uid).CompareTo(tr.Name(((STable)that).uid)) == 0;
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            sb.Append("Table ");
            if (db != null)
                sb.Append(db.Name(uid));
            else
                sb.Append(_Uid(uid));
        }
        public override long Alias => uid;

        public SDbObject ob => this;

        public override string ToString()
        {
            return "Table "+Uid();
        }
    }
    public class SCreateTable : Serialisable
    {
        public readonly long tdef;
        public readonly SList<SColumn> coldefs;
        public readonly SList<SIndex> constraints;
        public SCreateTable(long tn,SList<SColumn> c, SList<SIndex> cs)
            :base(Types.SCreateTable)
        { tdef = tn; coldefs = c; constraints = cs; }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(tdef);
            f.PutInt(coldefs.Length);
            for (var b = coldefs.First(); b != null; b = b.Next())
                b.Value.PutColDef(f);
            f.PutInt(constraints.Length);
            for (var b = constraints.First(); b != null; b = b.Next())
                b.Value.Put(f);
        }
        public new static Serialisable Get(Reader f)
        {
            var db = f.db;
            var tn = db.role[f.GetLong()];
            if (db.role.globalNames.Contains(tn))
                throw new Exception("Table " + tn + " already exists");
            db = db.Install(new STable((STransaction)db), tn, db.curpos);
            var n = f.GetInt();
            for (var i = 0; i < n; i++)
            {
                var sc = (SColumn)f._Get();
                db = db.Install(sc, db.role[sc.uid], db.curpos);
            }
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                db = db.Install((SIndex)f._Get(), db.curpos);
            f.db = db;
            return Null;
        }
        public override string ToString()
        {
            return "CreateTable "+SDbObject._Uid(tdef)+" "+coldefs.ToString();
        }
    }
    public class SysTable : STable
    {
        public static readonly long _SysUid = -0x7000000000000000;
        public static long _uid = _SysUid;
        public SysTable() : base(Types.SSysTable,--_uid)
        {
        }
        SysTable(SysTable t, SDict<long, SColumn> c, SDict<int,(long,string)> d,
            SDict<int,Serialisable> p, SDict<long, Serialisable> n)
            : base(t, c, d, p, n)
        {
        }
        protected override STable Add(SColumn c,string s)
        {
            var t = new SysTable(this, cols + (c.uid, c),
                display + (display.Length ?? 0, (c.uid,s)),
                cpos + (cpos.Length ?? 0, c),
                refs + (c.uid, c));
            return t;
        }
        static SDatabase Add(SDatabase d,string name,params (string,Types)[] ss)
        {
            var st = new SysTable();
            d = d.Install(st,name,0);
            for (var i = 0; i < ss.Length; i++)
                d = d.Install(new SColumn(--_uid,st.uid,ss[i].Item2),ss[i].Item1,0);
            return d;
        }
        internal static SDatabase SysTables(SDatabase d)
        {
            d=Add(d,"_Log",("Uid", Types.SString),("Type", Types.SString),("Desc", Types.SString),
                ("Id",Types.SString),("Affects",Types.SString));
            d=Add(d,"_Columns",("Table",Types.SString),("Name",Types.SString),
                ("Type",Types.SString),("Constraints",Types.SInteger), ("Uid", Types.SString));
            d=Add(d,"_Constraints", ("Table", Types.SString), ("Column", Types.SString),
                ("Check", Types.SString), ("Expression", Types.SString));
            d=Add(d,"_Indexes", ("Table", Types.SString), ("Type", Types.SString),
                ("Cols", Types.SString), ("References", Types.SString));
            d=Add(d,"_Tables",("Name", Types.SString),("Cols", Types.SInteger),
                ("Rows", Types.SInteger),("Indexes",Types.SInteger), ("Uid", Types.SString));
            return d;
        }
        public override RowSet RowSet(STransaction tr,SQuery top, Context cx)
        {
            return new SysRows(tr,this);
        }
        public override string ToString()
        {
            switch(_SysUid-uid)
            {
                case 1: return "_Log";
                case 8: return "_Columns";
                case 13: return "_Constraints";
                case 18: return "_Indexes";
                case 23: return "_Tables";
            }
            throw new Exception("PE11");
        }
    }
    /// <summary>
    /// Selectors are names of SRow columns: some are table columns
    /// and others are aliases used in queries. When we receive a
    /// query from a client all column name references are SSelectors
    /// </summary>
    public class SSelector : SDbObject
    {
        protected SSelector(long u) : base(Types.SSelector, u) { }
        public SSelector(Types tc, long u) : base(tc, u) { }
        protected SSelector(STransaction tr,Types tc) : base(tc,tr) { }
        protected SSelector(SSelector s) : base(s) { }
        protected SSelector(SSelector s, AStream f) : base(s, f) { }
        protected SSelector(Types tc, Reader f) : base(tc, f) { }
        public new static Serialisable Get(Reader f)
        {
            var x = f.GetLong(); // a client-side uid
            var ro = f.db.role;
            var n = ro[x];// client-side name
            if (f.context is SQuery)
            {
                if (ro.defs[f.context.uid].defines(n)) //it's a ColumnDef
                {
                    var sc = ((STable)f.context).cols[ro.defs[f.context.uid][n]];
                    f.db += (sc, sc.uid);
                    return sc;
                }
            }
            else if (ro.globalNames.defines(n)) // it's a table or stored query
                return f.db.objects[ro.globalNames[n]];
            throw new Exception("Unknown " + n);
        }
    }
    /// <summary>
    /// Some selectors are tablecolumn definitions (just as some queries are tables)
    /// </summary>
    public class SColumn : SSelector
    {
        public readonly Types dataType;
        public readonly long table;
        public readonly SDict<string,SFunction> constraints;
        /// <summary>
        /// For system or client column
        /// </summary>
        /// <param name="t">columns data type, default Serialisable (Null)</param>
        /// <param name="u"> will be negative</param>
        public SColumn(long u, long tbl, Types t=Types.Serialisable)
            : base(Types.SColumn, u)
        {
            dataType = t; table = tbl; constraints = SDict<string, SFunction>.Empty;
        }
        public SColumn(long tbl,Types t, long u, SDict<string,SFunction> cs) : base(Types.SColumn, u)
        {
            dataType = t; table = tbl; constraints = cs;
        }
        public SColumn(long tbl, Types t, long u) : base(Types.SColumn, u)
        { 
            dataType = t; table = tbl; constraints = SDict<string,SFunction>.Empty;
        }
        public SColumn(STransaction tr, long tbl,Types t, SDict<string, SFunction> c)
            : base(tr,Types.SColumn)
        {
            dataType = t; table = tbl; constraints = c;
        }
        protected SColumn(SColumn c, Types d, SDict<string, SFunction> e) : base(c)
        {
            dataType = d; table = c.table; constraints = e;
        }
        SColumn(Reader f) :base(Types.SColumn,f)
        {
            var db = f.db;
            var ro = db.role;
            var cn = (f is SocketReader)?ro[uid]:f.GetString(); 
            var oc = f.context;
            f.context = this; 
            dataType = (Types)f.ReadByte();
            var ut = f.GetLong();
            var tn = ro[ut];
            table = ro.globalNames[tn];
            var n = f.GetInt();
            var c = SDict<string,SFunction>.Empty;
            for (var i = 0; i < n; i++)
            {
                cn = f.GetString();
                c += (cn,f._Get() as SFunction ?? throw new Exception("Constraint expected"));
            }
            constraints = c;
            f.context = oc;
            if (!(f is SocketReader) && !f.db.uids.Contains(uid))
                f.db = f.db.Install(this, cn, f.Position);
        }
        /// <summary>
        /// Creates a new Column definition with a new file-position uid
        /// </summary>
        /// <param name="c"></param>
        /// <param name="f"></param>
        public SColumn(SColumn c,string nm,AStream f)  : base(c, f)
        {
            f.PutString(nm);
            dataType = c.dataType;
            table = f.Fix(c.table);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            f.PutInt(c.constraints.Length ?? 0);
            for (var b = c.constraints.First(); b != null; b = b.Next())
            {
                f.PutString(b.Value.Item1);
                b.Value.Item2.Fix(f).Put(f);
            }
            constraints = c.constraints;
        }
        public new static SColumn Get(Reader f)
        {
            return new SColumn(f);
        }
        public void PutColDef(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            f.PutInt(constraints.Length ?? 0);
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                f.PutString(b.Value.Item1);
                var c = b.Value.Item2;
                if (f is AStream af)
                    c = (SFunction)c.Fix(af);
                c.Put(f);
            }
        }
        // a column reference
        public override void Put(StreamBase f)
        {
            f.WriteByte((byte)Types.SName);
            f.PutLong(uid);
        }
        public override Serialisable UseAliases(SDatabase db,SDict<long, long> ta)
        {
            if (ta.Contains(uid))
                return new SExpression(db.objects[ta[table]], SExpression.Op.Dot, this);
            return base.UseAliases(db,ta);
        }
        public override Serialisable Prepare(STransaction tr, SDict<long, long> pt)
        {
            var cs = SDict<string, SFunction>.Empty;
            for (var b = constraints.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, (SFunction)b.Value.Item2.Prepare(tr,pt));
            return new SColumn(this, dataType, cs);
        }
        /// <summary>
        /// Fix a Column reference
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public override Serialisable Fix(AStream f)
        {
            return (f.uids.Contains(uid)) ? new SColumn(f.uids[table], dataType, f.uids[uid]) : this;
        }
        public override bool Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SColumn:
                    {
                        var c = (SColumn)that;
                        return c.table == table && db.Name(c.uid).CompareTo(tr.Name(uid)) == 0;
                    }
                case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return d.drpos == table;
                    }
            }
            return false;
        }
        public override Serialisable Lookup(STransaction tr,Context cx)
        {
            var r = cx.defines(uid) ? cx[uid] : Null;
            if (r == Null && !(cx.refs is RowBookmark))
                return this;
            return r;
        }
        public Serialisable Check(STransaction tr,Serialisable v,Context cx)
        {
            v = v.Coerce(dataType);
            for (var b=constraints.First();b!=null;b=b.Next())
                switch (b.Value.Item1)
                {
                    case "NOTNULL":
                        if (v.type==Types.Serialisable)
                            throw new Exception("Illegal null value");
                        break;
                    case "GENERATED":
                        if (v.type!=Types.Serialisable)
                            throw new Exception("Illegal value for generated column");
                        return b.Value.Item2.arg.Lookup(tr,cx);
                    case "DEFAULT":
                        if (v.type == Types.Serialisable)
                            return b.Value.Item2.arg;
                        break;
                    default:
                        cx = new Context(new SDict<long, Serialisable>(SArg.Value.target.uid, v), cx);
                        if (b.Value.Item2.arg.Lookup(tr,cx) != SBoolean.True)
                            throw new Exception("Column constraint " + b.Value.Item1 + " fails");
                        break;
                }
            return v;
        }
        public override SDict<long,Serialisable> Aggregates(SDict<long,Serialisable> ags)
        {
            for (var b = constraints.First(); b != null; b = b.Next())
                ags += ((b.Value.Item2).fid, b.Value.Item2);
            return base.Aggregates(ags);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(_Uid(table));
            sb.Append("[");
            sb.Append(Uid());
            sb.Append("] ");
            sb.Append(dataType.ToString());
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                sb.Append(' ');
                if (b.Value.Item2.func == SFunction.Func.Constraint)
                {
                    sb.Append(b.Value.Item1);
                    sb.Append('=');
                }
                b.Value.Item2.Append(sb);
            }
            return sb.ToString();
        }
    }
    public class SAlter : SDbObject
    {
        public readonly long defpos;
        public readonly long col;
        public readonly string name;
        public readonly Types dataType;
        public readonly SDict<string, SFunction> constraints;
        public SAlter(STransaction tr,string n,Types d,long o,long p,
            SDict<string,SFunction> cs) :base(Types.SAlter,tr)
        {
            defpos = o;  name = n; dataType = d; col = p; constraints = cs; 
        }
        public SAlter(string n, Types d, long o, long p,
            SDict<string, SFunction> cs) : base(Types.SAlter)
        {
            defpos = o; name = n; dataType = d; col = p; constraints = cs;
        }
        SAlter(Reader f):base(Types.SAlter,f)
        {
            defpos = f.GetLong();
            col = f.GetLong(); //may be -1
            name = f.GetString();
            dataType = (Types)f.ReadByte();
            var cs = SDict<string, SFunction>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
            {
                var id = f.GetString();
                cs += (id, f._Get() as SFunction ?? throw new Exception("Constraint expected"));
            }
            constraints = cs;
        }
        public SAlter(SAlter a,AStream f):base(a,f)
        {
            name = a.name;
            dataType = a.dataType;
            defpos = f.Fix(a.defpos);
            col = f.Fix(a.col);
            f.PutLong(defpos);
            f.PutLong(col);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            var cs = SDict<string, SFunction>.Empty;
            f.PutInt(a.constraints.Length ?? 0);
            for (var b=a.constraints.First();b!=null;b=b.Next())
            {
                f.PutString(b.Value.Item1);
                var cf = (SFunction)b.Value.Item2.Fix(f);
                cs += (b.Value.Item1, cf);
                cf.Put(f);
            }
            constraints = cs;
        }
        public override long Affects => defpos;
        public new static SAlter Get(Reader f)
        {
            return new SAlter(f);
        }
        public override STransaction Obey(STransaction tr, Context cx)
        {
            if (col != -1)
                return (STransaction)tr.Install(new SAlter(tr, name, dataType, defpos, col, constraints), tr.curpos);
            else if (dataType == Types.Serialisable)
                return (STransaction)tr.Install(new SAlter(tr, name, Types.Serialisable, defpos, -1,
                    constraints), tr.curpos);
            else
                return (STransaction)tr.Install(new SColumn(tr, defpos, dataType, constraints), name, tr.curpos);
        }
        public override bool Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch(that.type)
            {
                case Types.SAlter:
                    var a = (SAlter)that;
                    return a.defpos == defpos;
                case Types.SDrop:
                    var d = (SDrop)that;
                    return d.drpos == defpos || d.drpos == col;
            }
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Alter ");
            sb.Append(_Uid(defpos));
            sb.Append((col == -1) ? "" : (" column " + _Uid(col)));
            sb.Append(name);
            sb.Append((dataType!=Types.Serialisable)?(" " + DataTypeName(dataType)):"");
            for(var b=constraints.First();b!=null;b=b.Next())
            {
                sb.Append(" ");sb.Append(b.Value.Item1);
                b.Value.Item2.Append(sb);
            }
            return sb.ToString();
        }
    }
    public class SDrop: SDbObject
    {
        public readonly long drpos;
        public readonly long parent;
        public readonly string detail;
        public SDrop(long d, long p, string s) : base(Types.SDrop)
        {
            drpos = d; parent = p; detail = s;
        }
        public SDrop(STransaction tr,long d,long p,string s):base(Types.SDrop,tr)
        {
            drpos = d; parent = p; detail = s;
        }
        protected SDrop(Reader f) :base(Types.SDrop, f)
        {
            drpos = f.GetLong();
            parent = f.GetLong();
            detail = f.GetString();
        }
        public SDrop(SDrop d,AStream f):base(d,f)
        {
            drpos = f.Fix(d.drpos);
            parent = f.Fix(d.parent);
            detail = d.detail;
            f.PutLong(drpos);
            f.PutLong(parent);
            f.PutString(detail);
        }
        public override long Affects => drpos;
        public new static SDrop Get(Reader f)
        {
            return new SDrop(f);
        }
        public override bool Conflicts(SDatabase db,STransaction tr,Serialisable that)
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
                        return a.defpos == drpos || a.col == drpos;
                    }
            }
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Drop ");
            sb.Append("" + drpos);
            sb.Append((parent!=0)?"":(" of "+parent));
            sb.Append(detail);
            return sb.ToString();
        }
    }
    /// When view processing is added, we will often find multiple table occurrences
    /// in the resulting query. These will need to be fully aliased 
    /// with their columns as the view is encountered during the construction
    /// of the top-level query (the view aliases will need to be added to it).
    /// As a precaution let us do this for all its tables and columns as routine.
    public class SView : SDbObject
    {
        public readonly SDict<long,bool> viewobs;
        public readonly SQuery viewdef;
        public SView(SQuery vwd) :base(Types.SView)
        {
            viewobs = SDict<long, bool>.Empty;
            viewdef = vwd;
        }
        public SView(STransaction tr,SQuery d) :base(Types.SView,tr)
        {
            viewdef = d;
            viewobs = ViewObs(tr,d);
        }
        public SView(STransaction tr,SView v,AStream f):base(v,f)
        {
            base.Put(f);
            viewdef = (SQuery)v.viewdef.Fix(f);
            viewobs = ViewObs(tr, viewdef);
            viewdef.Put(f);
        }
        static SDict<long, bool> ViewObs(SDatabase db, SQuery d)
        {
            var pt = d.Names(db, SDict<long, long>.Empty);
            var qt = SDict<long, bool>.Empty;
            for (var b = pt.First(); b != null; b = b.Next())
                qt += (b.Value.Item2, true);
            return qt;
        }
        public new static SView Get(Reader f)
        {
            var vw = (SView)f.db.objects[f.GetLong()];
            // Construct new aliases in tr for all objects used in the view definition including aliases.
            var ta = SDict<long, long>.Empty;
            for (var b = vw.viewobs.First(); b != null; b = b.Next())
                ta = Aliases(f, b.Value.Item1, ta);
            // Transform vw to use these new aliases for all its objects and return it.
            vw = (SView)vw.UseAliases(f.db,ta);
            return vw;
        }
        /// <summary>
        /// Add aliases for the given object and its subobjects (e.g. columns)
        /// </summary>
        /// <param name="f"></param>
        /// <param name="u"></param>
        /// <param name="ta"></param>
        /// <returns></returns>
        static SDict<long,long> Aliases(Reader f,long u,SDict<long,long> ta)
        {
            var a = --f.lastAlias;
            var n = "$" + (maxAlias - a);
            f.db += (a, n);
            ta += (u, a);
            if (f.db.role.props.Contains(u))
                for (var b = f.db.role.props[u].First(); b != null; b = b.Next())
                    ta = Aliases(f, b.Value.Item1, ta);
            return ta;
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            return new SView((SQuery)viewdef.Prepare(db,pt));
        }
        public override void Put(StreamBase f)
        {
            viewdef.Put(f);
        }
        public override bool Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SView:
                    {
                        var v = (SView)that;
                        return db.Name(uid).CompareTo(tr.Name(v.uid)) == 0;
                    }
            }
            return false;
        }
    }
    public class SInsert : Serialisable
    {
        public readonly long table;
        public readonly SList<long> cols;
        public readonly Serialisable vals;
        public SInsert(long t,SList<long> c,Serialisable v)
            : base(Types.SInsert)
        {
            table = t; cols = c; vals = v;
        }
        public SInsert(long t,SList<long> c,Reader r) :base(Types.SInsert)
        {
            var tr = (STransaction)r.db;
            table = t;
            cols = c;
            vals = r._Get();
        }
        public override Serialisable Prepare(STransaction tr, SDict<long, long> pt)
        {
            var t = table;
            if (t < 0)
            {
                var tn = tr.role.uids[t];
                if (!tr.role.globalNames.Contains(tn))
                    throw new Exception("Table " + tn + " not found");
                t = tr.role.globalNames[tn];
            }
            var cs = SList<long>.Empty;
            var rt = tr.role.defs[t];
            var i = 0;
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var u = b.Value;
                if (u < 0) // it is a client-side uid to be looked up in the SNames contribution to tr
                {
                    var cn = tr.role.uids[u];
                    if (!rt.Contains(cn))
                        throw new Exception("Column " + cn + " not found");
                    u = rt[cn];
                }
                cs += (u, i++);
            }
            switch(vals.type)
            {
                case Types.SValues:
                    {
                        var svs = (SValues)vals;
                        var tb = (STable)tr.objects[t];
                        var nc = svs.vals.Length ?? 0;
                        if ((i == 0 && nc != tb.cpos.Length) || (i != 0 && i != nc))
                            throw new Exception("Wrong number of columns");
                        var vs = SList<Serialisable>.Empty;
                        i = 0;
                        for (var b = svs.vals.First(); b != null; b = b.Next())
                            vs += (b.Value.Prepare(tr, tb.Names(tr,pt)), i++);
                        return new SInsert(t, cs, new SValues(vs));
                    }
                case Types.SSelect:
                    {
                        var ss = (SSelectStatement)vals;
                        var tb = (STable)tr.objects[t];
                        ss = (SSelectStatement)ss.Prepare(tr, ss.Names(tr,pt));
                        var nc = ss.Display.Length;
                        if ((i == 0 && nc != tb.cpos.Length) || (i != 0 && i != nc))
                            throw new Exception("Wrong number of columns");
                        return new SInsert(t, cs, ss);
                    }
            }
            throw new Exception("Unkown insert syntax "+vals.type.ToString());
        }
        public override STransaction Obey(STransaction tr,Context cx)
        {
            var tb = (STable)tr.objects[table];
            switch (vals.type)
            {
                case Types.SValues:
                    {
                        var svs = (SValues)vals;
                        var f = SDict<long, Serialisable>.Empty;
                        var c = svs.vals;
                        if (cols.Length == 0)
                            for (var b = tb.cpos.First(); c.Length != 0 && b != null; b = b.Next(), c = c.next)
                            {
                                var sc = (SColumn)b.Value.Item2;
                                var v = sc.Check(tr,c.element.Lookup(tr,cx), cx);
                                f += (sc.uid, v);
                            }
                        else
                            for (var b = cols; c.Length != 0 && b.Length != 0; b = b.next, c = c.next)
                            {
                                var sc = (SColumn)tr.objects[b.element];
                                var v = sc.Check(tr,c.element.Lookup(tr,cx), cx);
                                f += (b.element, v);
                            }
                        tr = (STransaction)tr.Install(tb.Check(tr,new SRecord(tr, table, f)), tr.curpos);
                        break;
                    }
                case Types.SSelect:
                    {
                        var ss = (SSelectStatement)vals;
                        var rs = ss.RowSet(tr, ss, Context.Empty);
                        for (var rb = (RowBookmark?)rs.First();rb!=null;rb=(RowBookmark?)rb.Next())
                        {
                            var f = SDict<long, Serialisable>.Empty;
                            var c = rb._ob.vals.First();
                            if (cols.Length == 0)
                                for (var b = tb.cpos.First(); c!= null && b != null; b = b.Next(), c = c.Next())
                                {
                                    var sc = (SColumn)b.Value.Item2;
                                    var v = sc.Check(tr,c.Value.Item2.Lookup(tr,cx), cx);
                                    f += (sc.uid, v);
                                }
                            else
                                for (var b = cols; c != null && b.Length != 0; b = b.next, c = c.Next())
                                {
                                    var sc = (SColumn)tr.objects[b.element];
                                    var v = sc.Check(tr,c.Value.Item2.Lookup(tr,cx), cx);
                                    f += (b.element, v);
                                }
                            tr = (STransaction)tr.Install(new SRecord(tr, table, f), tr.curpos);
                        }
                        break;
                    }
            }
            return tr;
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            return new SInsert(table,cols,vals.UpdateAliases(uids));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(table);
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                f.PutLong(b.Value);
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
        public SValues(Reader f) : base(Types.SValues)
        {
            var n = f.GetInt();
            var nr = f.GetInt();
            vals = SList<Serialisable>.Empty;
            for (var i = 0; i < n; i++)
                vals += (f._Get(), i);
        }
        public override bool isValue => true;
        public new static SValues Get(Reader f)
        {
            return new SValues(f);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var vs = SList<Serialisable>.Empty;
            var i = 0;
            for (var b=vals.First();b!=null;b=b.Next(),i++)
                vs += (b.Value.UseAliases(db, ta), i);
            return new SValues(vs);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var vs = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = vals.First(); b != null; b = b.Next())
                vs += (b.Value.Prepare(db, pt), n++);
            return new SValues(vs);
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
            var tb = (STable)tr.objects[t];
            var a = tb.Aggregates(SDict<long,Serialisable>.Empty);
            var cx = new Context(a,null);
            var db = tb.Display.First();
            for (var b = tb.cols.First(); b != null && db!=null; b = b.Next(), db=db.Next())
            {
                var sc = b.Value.Item2;
                var cn = db.Value.Item2.Item1;
                for (var c = sc.constraints.First(); c != null; c = c.Next())
                {
                    var fn = c.Value.Item2;
                    switch (fn.func)
                    {
                        case SFunction.Func.Default:
                            if ((!f.Contains(cn)) || f[cn] == Null)
                                f += (cn, fn.arg);
                            break;
                        case SFunction.Func.NotNull:
                            if ((!f.Contains(cn)) || f[cn] == Null)
                                throw new Exception("Value of "+tr.Name(cn)+" cannot be null");
                            break;
                        case SFunction.Func.Constraint:
                            {
                                var cf = new Context(f, cx);
                                if (fn.arg.Lookup(tr,cf) == SBoolean.False)
                                    throw new Exception("Constraint violation");
                                break;
                            }
                        case SFunction.Func.Generated:
                            {
                                var cf = new Context(f, cx);
                                if (f.Contains(cn) && f[cn] != Null)
                                    throw new Exception("Value cannot be supplied for column " + tr.Name(cn));
                                f += (cn, fn.arg.Lookup(tr,cf));
                            }
                            break;
                    }
                }
            }
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
        protected SRecord(Types t,Reader f) : base(t,f)
        {
            table = f.GetLong();
            var n = f.GetInt();
            var tb = (STable)f.Lookup(table);
            var a = SDict<long, Serialisable>.Empty;
            for(var i = 0;i< n;i++)
            {
                var k = f.GetLong();
                a += (k, f._Get());
            }
            fields = a;
        }
        public new static SRecord Get(Reader f)
        {
            return new SRecord(Types.SRecord, f);
        }
        public override void Append(SDatabase db, StringBuilder sb)
        {
            sb.Append("{");
            if (Defpos<STransaction._uid)
            {
                sb.Append("_id:"); sb.Append(Defpos); sb.Append(",");
            }
            var tb = (STable)db.objects[table];
            sb.Append("_table:");
            sb.Append('"');
            sb.Append(db.Name(tb.uid));
            sb.Append('"');
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(",");
                var c = tb.cols.Lookup(b.Value.Item1);
                sb.Append(db.Name(c.uid));
                sb.Append(":");
                b.Value.Item2.Append(db,sb);
            }
            sb.Append("}");
        }
        public override void Append(StringBuilder sb)
        {
            sb.Append("{");
            if (Defpos < STransaction._uid)
            {
                sb.Append("_id:"); sb.Append(Defpos); sb.Append(",");
            }
            sb.Append("_table:");
            sb.Append('"');
            sb.Append(_Uid(table));
            sb.Append('"');
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.Append(",");
                sb.Append( _Uid(b.Value.Item1));
                sb.Append(":");
                b.Value.Item2.Append(sb);
            }
            sb.Append("}");
        }
        public bool Matches(RowBookmark rb,SList<Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value is SExpression x && x.Lookup(rb._rs._tr,rb._cx) is SBoolean e 
                    && !e.sbool)
                    return false;
            return true;
        }
        public bool EqualMatches(RowBookmark rb, SList<Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value is SExpression x && x.op==SExpression.Op.Eql 
                    && x.Lookup(rb._rs._tr, rb._cx) is SBoolean e && !e.sbool)
                    return false;
            return true;
        }
        public override bool Check(SDict<long, bool> rdC)
        {
            return rdC.Contains(Defpos) || rdC.Contains(table);
        }
        public override bool Conflicts(SDatabase db, STransaction tr,Serialisable that)
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
            Append(sb);
            return sb.ToString();
        }
    }
    public class SUpdateSearch : Serialisable
    {
        public readonly SQuery qry;
        public readonly SDict<long, Serialisable> assigs;
        public SUpdateSearch(SQuery q,SDict<long,Serialisable> a)
            : base(Types.SUpdateSearch)
        {
            qry = q; assigs = a;
        }
        public override STransaction Obey(STransaction tr,Context cx)
        {
            for (var b = qry.RowSet(tr,qry,Context.Empty).First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var u = SDict<long, Serialisable>.Empty;
                for (var c = assigs.First(); c != null; c = c.Next())
                    u += (c.Value.Item1, c.Value.Item2.Lookup(tr,b._cx));
                tr = b.Update(tr,u);
            }
            return tr;
        }
        public new static SUpdateSearch Get(Reader f)
        {
            var q = (SQuery)f._Get();
            var n = f.GetInt();
            var a = SDict<long, Serialisable>.Empty;
            for (var i=0;i<n;i++)
            {
                var s = f._Get(); 
                if (s is SDbObject sc)
                    a += (sc.uid, f._Get());
                else
                    throw new Exception("Column " + s + " not found");
            }
            return new SUpdateSearch(q, a);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var a = SDict<long, Serialisable>.Empty;
            for (var b = assigs.First(); b != null; b = b.Next())
                a += (SDbObject.Prepare(b.Value.Item1, pt), 
                    b.Value.Item2.Prepare(db, pt));
            return new SUpdateSearch((SQuery)qry.Prepare(db,pt),a);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
            f.PutInt(assigs.Length);
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                f.WriteByte((byte)Types.SName); f.PutLong(b.Value.Item1);
                b.Value.Item2.Put(f);
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Update ");
            qry.Append(sb);
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
        public SUpdate(STransaction tr,SRecord r,SDict<long,Serialisable>u) 
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
        SUpdate(Reader f) : base(Types.SUpdate,f)
        {
            defpos = f.GetLong();
        }
        static SDict<long,Serialisable> _Merge(STransaction tr,SRecord r,
            SDict<long,Serialisable> us)
        {
            var tb = (STable)tr.objects[r.table];
            var u = SDict<long, Serialisable>.Empty;
            for (var b=us.First();b!=null;b=b.Next())
                u += b.Value;
            return r.fields+u;
        }
        public override long Affects => defpos;
        public new static SUpdate Get(Reader f)
        {
            return new SUpdate(f);
        }
        public override bool Conflicts(SDatabase db, STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos == Defpos;
            }
            return base.Conflicts(db,tr,that);
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
    public class SDeleteSearch : Serialisable
    {
        public readonly SQuery qry;
        public SDeleteSearch(SQuery q) :base(Types.SDeleteSearch) { qry = q; }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            return new SDeleteSearch((SQuery)qry.Prepare(db, pt));
        }
        public override STransaction Obey(STransaction tr,Context cx)
        {
            for (var b = qry.RowSet(tr,qry,cx).First() as RowBookmark; 
                b != null; b = b.Next() as RowBookmark)
            {
                var rc = b._ob.rec ?? throw new System.Exception("PE14");// not null
                tr = (STransaction)tr.Install(new SDelete(tr, rc.table, rc.uid),tr.curpos); 
            }
            return tr;
        }
        public new static SDeleteSearch Get(Reader f)
        {
            return new SDeleteSearch((SQuery)f._Get());
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Delete ");
            qry.Append(sb);
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
        public override long Affects => delpos;
        public override bool Check(SDict<long, bool> rdC)
        {
            return rdC.Contains(delpos) || rdC.Contains(table);
        }
        public override bool Conflicts(SDatabase db, STransaction tr,Serialisable that)
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
        public readonly SMTree<Serialisable> rows;
        /// <summary>
        /// A primary or unique index
        /// </summary>
        /// <param name="t"></param>
        /// <param name="c"></param>
        public SIndex(long t, bool p, long r, SList<long> c) : base(Types.SIndex)
        {
            table = t;
            primary = p;
            cols = c;
            references = r;
            rows = new SMTree<Serialisable>(SList<TreeInfo<Serialisable>>.Empty);
            refindex = -1;
        }
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
            rows = new SMTree<Serialisable>(Info((STable)tr.objects[table], cols,refindex>=0));
        }
        SIndex(Reader f) : base(Types.SIndex, f)
        {
            var ro = f.db.role;
            var tn = ro[f.GetLong()];
            if (!ro.globalNames.Contains(tn))
                throw new Exception("Table " + tn + " not found");
            table = ro.globalNames[tn];
            primary = f.ReadByte()!=0;
            var n = f.GetInt();
            var rt = ro.defs[table];
            var c = new long[n];
            for (var i = 0; i < n; i++)
            {
                var cn = ro[f.GetLong()];
                if (!rt.Contains(cn))
                    throw new Exception("Column " + cn + " not found");
                c[i] = rt[cn];
            }
            var ru = f.GetLong();
            var rn = (ru == -1) ? "" : ro[ru];
            var rx = -1L;
            if (ru != -1)
            {
                if (!ro.globalNames.Contains(rn))
                    throw new Exception("Ref table " + rn + " not found");
                ru = ro.globalNames[rn];
                if (f.db.GetPrimaryIndex(ru) is SIndex x)
                    rx = x.uid;
                else
                    throw new Exception("Ref table " + rn + " has no primary index");
            }
            references = ru;
            refindex = rx;
            cols = SList<long>.New(c);
            rows = new SMTree<Serialisable>(Info((STable)f.db.objects[table], cols,references>=0));
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
        public SIndex(SIndex x,SMTree<Serialisable> nt) :base(x)
        {
            table = x.table;
            primary = x.primary;
            references = x.references;
            refindex = x.refindex;
            cols = x.cols;
            rows = nt;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutLong(table);
            f.WriteByte((byte)(primary ? 1 : 0));
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                f.PutLong(b.Value);
            f.PutLong(references);
        }
        public new static SIndex Get(Reader f)
        {
            return new SIndex(f);
        }
        public void Check(SDatabase db,SRecord r,bool updating)
        {
            var k = Key(r, cols);
            if ((!updating) && refindex == -1 && rows.Contains(k))
                throw new Exception("Duplicate Key constraint violation");
            if (refindex != -1)
            {
                var rx = (SIndex)db.objects[refindex];
                if (!rx.rows.Contains(k))
                    throw new Exception("Referential constraint violation");
            }
        }
        protected SIndex Add(SRecord r,long c)
        {
            return new SIndex(this, rows+(Key(r, cols), c));
        }
        public static SIndex operator+(SIndex x,(SRecord,long) t)
        {
            return x.Add(t.Item1, t.Item2);
        }
        public SIndex Update(SRecord o,SCList<Variant> ok,SUpdate u, SCList<Variant>uk, long c)
        {
            return new SIndex(this, rows-(ok,o.uid)+(uk, u.uid));
        }
        protected SIndex Remove(SRecord sr,long c)
        {
            return new SIndex(this, rows-(Key(sr, cols),c));
        }
        public static SIndex operator-(SIndex x,(SRecord,long) a)
        {
            return x.Remove(a.Item1, a.Item2);
        }
        SList<TreeInfo<Serialisable>> Info(STable tb, SList<long> cols,bool fkey)
        {
            if (cols.Length==0)
                return SList<TreeInfo<Serialisable>>.Empty;
            return Info(tb, cols.next,fkey)+(new TreeInfo<Serialisable>( // not null
                tb.cols.Lookup(cols.element), (cols.Length!=1 || fkey)?'A':'D', 'D'), 0);
        }
        internal SCList<Variant> Key(SRecord sr,SList<long> cols)
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
            if (primary)
                sb.Append(" primary ");
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
            PutInteger(new Integer(n.Value));
        }
        public void PutInteger(Integer b)
        {
            var m = b.bytes.Length;
            WriteByte((byte)m);
            for (int j = 0; j<m ; j++)
                WriteByte(b.bytes[j]);
        }
        public void PutLong(long n)
        {
            PutInteger(new Integer(n));
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
        public Context ctx = Context.Empty;
        public SDbObject context = SRole.Public; // set a function or object being defined
        public long lastAlias = SDbObject.maxAlias;
        public SDatabase db;   // a copy, updatable during Get, Load
        internal Reader(SDatabase d)
        {
            db = d;
            buf = new StreamBase.Buffer(d.File());
        }
        internal Reader(StreamBase s)
        {
            db = SDatabase._system;
            buf = new StreamBase.Buffer(s);
        }
        internal Reader(SDatabase d,long s)
        {
            db = d;
            buf = new StreamBase.Buffer(d.File(),s);
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
        public static long pe13;
        public int GetInt()
        {
            pe13 = Position;
            return (int)GetInteger();
        }
        public long GetLong()
        {
            return (long)GetInteger();
        }
        public string GetString()
        {
            int n = GetInt();
            byte[] cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return Encoding.UTF8.GetString(cs, 0, n);
        }
        public Serialisable _Get()
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
                case Types.STable: s = STable.Get(this); break;
                case Types.SRow: s = SRow.Get(this); break;
                case Types.SColumn: s = SColumn.Get(this); break;
                case Types.SRecord: s = SRecord.Get(this); break;
                case Types.SUpdate: s = SUpdate.Get(this); break;
                case Types.SDelete: s = SDelete.Get(this); break;
                case Types.SAlter: s = SAlter.Get(this); break;
                case Types.SDrop: s = SDrop.Get(this); break;
                case Types.SIndex: s = SIndex.Get(this); break;
                case Types.SCreateTable: s = SCreateTable.Get(this); break;
                case Types.SUpdateSearch: s = SUpdateSearch.Get(this); break;
                case Types.SDeleteSearch: s = SDeleteSearch.Get(this); break;
                case Types.SSearch: s = SSearch.Get(this); break;
                case Types.SSelect: s = SSelectStatement.Get(this); break;
                case Types.SValues: s = SValues.Get(this); break;
                case Types.SExpression: s = SExpression.Get(this); break;
                case Types.SFunction: s = SFunction.Get(this); break;
                case Types.SOrder: s = SOrder.Get(this); break;
                case Types.SInPredicate: s = SInPredicate.Get(this); break;
                case Types.SAlias: s = SAlias.Get(this); break;
                case Types.SSelector: s = SSelector.Get(this); break;
                case Types.SGroupQuery: s = SGroupQuery.Get(this); break;
                case Types.STableExp: s = SJoin.Get(this); break;
                case Types.SName: s = SDbObject.Get(this); break;
                case Types.SArg: s = new SArg(this); break;
                default: s = Serialisable.Null; break;
            }
            return s;
        }
        public virtual STable GetTable()
        {
            var tb = new STable(Position - 1);
            var nm = GetString();
            db = db.Install(tb, nm, Position); // will have moved on
            return tb;
        }
        /// <summary>
        /// Called from Transaction.Commit()
        /// </summary>
        /// <param name="d"></param>
        /// <param name="pos"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public SDbObject[] GetAll(long max)
        {
            var r = new List<SDbObject>();
            while (Position < max)
                r.Add((SDbObject)_Get());
            return r.ToArray();
        }
        public Serialisable Lookup(long pos)
        {
            return db.objects[((AStream)buf.fs).Fix(pos)];
        }
    }
    /// <summary>
    /// this class is not shareable
    /// </summary>
    public class SocketReader : Reader
    {
        public SocketReader(StreamBase s) : base(s)
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
        public override STable GetTable()
        {
            var un = GetLong();
            var nm = db.role[un];
            if (db.role.globalNames.defines(nm))
            {
                var tb = (STable)db.objects[db.role.globalNames[nm]];
                context = tb;
                return tb;
            }
            throw new Exception("No such table " + nm);
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
                            var nm = tr.Name(st.uid);
                            var nt = new STable(st, nm, this);
                            db += (nt,nm,Length);
                            commits += (nt.uid, nt);
                            break;
                        }
                    case Types.SColumn:
                        {
                            var sc = (SColumn)b.Value.Item2;
                            var nm = tr.Name(sc.uid);
                            var nc = new SColumn(sc, nm, this);
                            var tb = (STable)db.objects[nc.table];
                            tb += (nc,nm); 
                            db = db + (nc,nm,Length) + (tb,db.Name(tb.uid),Length)
                                + (tb.uid,nc.uid,nm);
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
            if (commits.Contains(pos))
                return commits[pos];
            return new Reader(db, pos)._Get();
        }
        internal long Fix(long pos)
        {
            return (uids.Contains(pos))?uids[pos]:pos;
        }
        internal void CommitDone()
        {
            uids = SDict<long, long>.Empty;
            commits = SDict<long, Serialisable>.Empty;
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
            var p = file.Seek(0, SeekOrigin.End);
            file.Write(b.buf, 0, b.wpos);
            file.Flush();
            length = p + b.wpos;
            wposition = length;
            b.wpos = 0;
        }
    }
}
