using System.Text;
using System.Text.Json.Serialization.Metadata;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// IMMUTABLE SHAREABLE
    /// Executable statements can be used in stored procedures. 
    /// This class provides machinery to support control statements in stored procedures.
    /// Control statements (blocks) generally ovveride this basic infrastructure.
    /// The behaviour of condition handlers and loops requires some infrastructure here.
    /// By default a compound statement executes its tree of statements in sequence. But this ordering can be disturbed,
    /// and the transaction.breakto if not null will pop blocks off the stack until we catch the break.
    /// For example RETURN will set breakto to the previous stack (dynLink), as will the execution of an EXIT handler.
    /// CONTINUE will set breakto to the end of the enlcosing looping construct.
    /// BREAK will set breakto to just outside the current looping construct.
    /// BREAK nnn will set breakto just outside the named looping construct.
    /// An UNDO handler will restore the state to the defining point of the handler, and then EXIT.
    /// </summary>
	internal class Executable : DBObject
	{
        internal const long
            Label = -92, // string
            Result = -93, // long Domain
            Schema = -171, // long (Graph)Schema
            UseGraph = -481; // long Graph
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal string? label => (string?)mem[Label];
        internal long result => (long)(mem[Result] ?? -1L);
        internal long schema => (long)(mem[Schema] ?? -1L);
        internal long graph => (long)(mem[UseGraph]??-1L);
        internal static Executable None = new();
        Executable() : base(--_uid, BTree<long, object>.Empty) { }
        internal Executable(long dp, BTree<long, object>? m=null) 
            : base(dp, m??BTree<long, object>.Empty) { }
        /// <summary>
        /// Support execution of a tree of Executables, in an Activation.
        /// With break behaviour
        /// </summary>
        /// <param name="e">The Executables</param>
        /// <param name="tr">The transaction</param>
		protected static Context ObeyList(BList<long?> e, Context cx)
        {
            if (e == null)
                throw new DBException("42173");
            Context nx = cx;
            Activation a = (Activation)cx;
            for (var b = e.First(); b != null && nx == cx
                && ((Activation)cx).signal == null; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is Executable x)
                    try
                    {
                        nx = x._Obey(a);
                        if (a == nx && a.signal != null)
                            a.signal.Throw(a);
                        if (cx != nx)
                            break;
                    }
                    catch (DBException ex)
                    {
                        a.signal = new Signal(cx.cxid, ex);
                    }
            return nx;
        }
       internal Context Obey(Context cx)
        {
            if (cx.db.objects[schema] is Schema sc)
                cx.schema = sc;
            if (cx.db.objects[graph] is Graph g)
                cx.graph = g;
            if (cx is Activation ax)
                return _Obey(ax);
            else
            {
                var a = new Activation(cx,"");
                var r = _Obey(a);
                cx = a.SlideDown();
                return r;
            }
        }
        /// <summary>
        /// _Obey the Executable for the given Activation.
        /// All non-CRUD Executables should have a shortcut override.
        /// The base class implementation is for DDL execution in stored procedures.
        /// It parses the string and executes it in the current context
        /// </summary>
        /// <param name="cx">The context</param>
		public virtual Context _Obey(Context cx)
        {
            return cx;
        }
        internal static bool Calls(BList<long?> ss,long defpos,Context cx)
        {
            for (var b = ss?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p]?.Calls(defpos, cx) == true)
                    return true;
            return false;
        }
        public override string ToString()
        {
            var nm = GetType().Name;
            var sb = new StringBuilder(nm);
            if (mem.Contains(Label)) { sb.Append(' '); sb.Append(label); }
            if (result > 0)
            { sb.Append(" Result: "); sb.Append(Uid(result)); }
            if (mem.Contains(ObInfo.Name)) { sb.Append(' '); sb.Append(mem[ObInfo.Name]); }
            sb.Append(' ');sb.Append(Uid(defpos));
            return sb.ToString();
        }

        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new Executable(dp,mem+m);
        }
    }
    internal class ExecuteList : BList<Executable>
    {
        internal new static ExecuteList Empty = new();
        ExecuteList() : base() { }
        ExecuteList(ExecuteList es, Executable e) : base(es, e) { }
        internal static ExecuteList New(DBObject? ob)
        {
            return (ob is Executable e) ? Empty + e : Empty;
        }
        public static ExecuteList operator +(ExecuteList a, DBObject? ob)
        {
            return (ob is Executable e) ? new(a, e) : a;
        }
        public static ExecuteList operator +(ExecuteList a, ExecuteList b)
        {
            for (var c = b.First(); c != null; c = c.Next())
                a += c.value();
            return a;
        }
        internal Executable? Top()
        {
            return Last()?.value();
        }
    }
    internal class CommitStatement : Executable
    {
        internal CommitStatement(long dp) : base(dp)
        {
        }

        public override Context _Obey(Context cx)
        {
            cx.db.Commit(cx);
            return cx.parent ?? cx;
        }
        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
    }
    internal class RollbackStatement(long dp) : Executable(dp)
    {
        public override Context _Obey(Context cx)
        {
            cx.db.Rollback();
            throw new DBException("40000").ISO();
        }
        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// A Select Statement can be used in a stored procedure so is a subclass of Executable
    /// 
    /// </summary>
    internal class SelectStatement : Executable
    {
        internal const long
            Union = -196; // long RowSet
        /// <summary>
        /// The QueryExpression 
        /// </summary>
        public long union => (long)(mem[Union] ?? -1L);
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The cursor specification</param>
        public SelectStatement(long dp, RowSet r)
            : base(dp, new BTree<long, object>(Union, r.defpos)+(_Domain,r)) 
        { }
        protected SelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectStatement operator +(SelectStatement et, (long, object) x)
        {
            return new SelectStatement(et.defpos, et.mem + x);
        }
        public static SelectStatement operator +(SelectStatement rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > rs.depth)
                    m += (_Depth, d);
            }
            if (p == Union)
                m += (_Domain, cx._Dom(p));
            return (SelectStatement)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SelectStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(union);
            if (nc != union)
                r += (Union, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[union]?.Calls(defpos, cx)??false;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectStatement)base._Replace(cx, so, sv);
            var nu = ((RowSet?)cx.obs[union])?.Replace(cx, so, sv)?.defpos;
            if (nu!=union && nu is not null)
                r += (cx,Union, nu);
            return r;
        }
        public override Context _Obey(Context cx)
        {
            cx.result = union;
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Union="); sb.Append(Uid(union));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Compound Statement for the SQL procedure language
    /// 
    /// </summary>
    internal class CompoundStatement : Executable
    {
        internal const long
             Stms = -96; // BList<long?> Executable
        /// <summary>
        /// The contained tree of Executables
        /// </summary>
		public BList<long?> stms =>
            (BList<long?>?)mem[Stms] ?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: create a compound statement
        /// </summary>
        /// <param name="n">The label for the compound statement</param>
		internal CompoundStatement(long dp, string n)
            : base(dp, new BTree<long, object>(Label, n))
        { }
        public CompoundStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CompoundStatement operator +(CompoundStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (CompoundStatement)et.New(m + x);
        }

        public static CompoundStatement operator +(CompoundStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == Stms)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (CompoundStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CompoundStatement(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new CompoundStatement(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.FixLl(stms);
            if (nc != stms)
                r += (Stms, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(stms, defpos, cx);
        }
        /// <summary>
        /// _Obey a Compound Statement.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context _Obey(Context cx)
        {
            cx.exec = this;
            var act = new Activation(cx, label ?? "") { binding=cx.binding};
            act = (Activation)ObeyList(stms, act);
            act.signal?.Throw(cx);
            cx.db = act.db;
            return act.SlideDown();
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (CompoundStatement)base._Replace(cx, so, sv);
            var ns = cx.ReplacedLl(stms);
            if (ns!=stms)
                r += (cx, Stms, ns);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "(";
            for (var b = stms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    
    internal class PreparedStatement : Executable
    {
        internal const long
            QMarks = -396,  // BList<long?> QlValue
            Target = -397; // Executable
        internal BList<long?> qMarks =>
           (BList<long?>?)mem[QMarks] ?? BList<long?>.Empty;
        internal Executable? target => (Executable?)mem[Target];
        public PreparedStatement(Context cx,long nst)
            : base(cx.GetUid(), _Mem(cx) + (QMarks, cx.qParams) + (_Framing, new Framing(cx,nst)))
        { }
        protected PreparedStatement(long dp,BTree<long,object> m)
            :base(dp,m)
        { }
        static BTree<long,object> _Mem(Context cx)
        {
            var r = BTree<long, object>.Empty;
            if (cx.exec != null)
                r += (Target, cx.exec);
            return r;
        }
        public static PreparedStatement operator +(PreparedStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (PreparedStatement)et.New(m + x);
        }

        public static PreparedStatement operator +(PreparedStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == QMarks)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (PreparedStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PreparedStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new PreparedStatement(dp, m);
        }
        public override Context _Obey(Context cx)
        {
            if (target == null)
                return cx;
            return ((Executable)target.Instance(defpos,cx))._Obey(cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (PreparedStatement)base._Replace(cx, so, sv);
            var nq = cx.ReplacedLl(qMarks);
            if (nq!=qMarks)
                r +=(cx, QMarks, nq);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");sb.Append(target);
            var cm = "";
            sb.Append(" Params: ");
            for (var b = qMarks.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A local variable declaration.
    /// 
    /// </summary>
	internal class LocalVariableDec : Executable
    {
        internal const long
            Init = -97; // long QlValue
        /// <summary>
        /// Default initialiser
        /// </summary>
        public long init => (long)(mem[Init]??-1L);
        public long vbl => (long)(mem[AssignmentStatement.Vbl]??-1L);
        /// <summary>
        /// Constructor: a new local variable
        /// </summary>
        public LocalVariableDec(long dp, QlValue v, BTree<long,object>?m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Label, v.name??"")
          + (AssignmentStatement.Vbl, v.defpos))
        { }
        protected LocalVariableDec(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LocalVariableDec operator +(LocalVariableDec et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (LocalVariableDec)et.New(m + x);
        }

        public static LocalVariableDec operator +(LocalVariableDec e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (LocalVariableDec)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LocalVariableDec(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new LocalVariableDec(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nb = cx.Fix(vbl);
            if (nb != vbl)
                r += (AssignmentStatement.Vbl, nb);
            var ni = cx.Fix(init);
            if (init != ni)
                r += (Init, ni);
            return r;
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.FindCx(defpos).values[defpos]??TNull.Value;
        }
        /// <summary>
        /// Execute the local variable declaration, by adding the local variable to the activation (overwrites any previous)
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var vb = (QlValue)(cx.obs[vbl] ?? throw new PEException("PE1101"));
            TypedValue tv = cx.obs[init]?.Eval(cx)??vb.domain.defaultValue;
            a.bindings += (defpos, CTree<long,TypedValue>.Empty); // local variables need special handling
            cx.AddValue(vb, tv); // We expect a==ac, but if not, tv will be copied to a later
            return cx;
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LocalVariableDec)base._Replace(cx, so, sv);
            var ni = ((QlValue?)cx.obs[init])?.Replace(cx,so,sv)?.defpos ;
            if (ni!=init && ni is not null)
                r += (Init, ni);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(label); sb.Append(' '); sb.Append(Uid(defpos));
            return sb.ToString();
        }
	}
    /// <summary>
    /// A procedure formal parameter has mode and result info
    /// 
    /// </summary>
    internal class FormalParameter : QlValue
    {
        internal const long
            ParamMode = -98, // Qlx
            Result = -99; // Qlx
        public long val => (long)(mem[AssignmentStatement.Val] ?? -1L);
        /// <summary>
        /// The mode of the parameter: IN, OUT or INOUT
        /// </summary>
		public Qlx paramMode => (Qlx)(mem[ParamMode] ?? Qlx.IN);
        /// <summary>
        /// The result mode of the parameter: RESULT or NO
        /// </summary>
		public Qlx result => (Qlx)(mem[Result] ?? Qlx.NO);
        /// <summary>
        /// Constructor: a procedure formal parameter from the parser
        /// </summary>
        /// <param name="m">The mode</param>
		public FormalParameter(long vp, Qlx m, string n,Domain dt)
            : base(vp, new BTree<long, object>(ParamMode, m)+(ObInfo.Name,n)
                  +(AssignmentStatement.Val,vp)+(_Domain,dt))
        { }
        protected FormalParameter(long dp,BTree<long, object> m) : base(dp,m) { }

        public static FormalParameter operator +(FormalParameter et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (FormalParameter)et.New(m + x);
        }

        public static FormalParameter operator +(FormalParameter e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (FormalParameter)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FormalParameter(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new FormalParameter(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var vl = cx.Fix(val);
            if (vl != val)
                r += (AssignmentStatement.Val, vl);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient=false)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient=false)
        {
            return true;
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.values[defpos]??TNull.Value;
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        internal override string ToString(string sg,Remotes rf,BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            return Eval(cx).ToString();
        }
        /// <summary>
        /// A readable version of the ProcParameter
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(ParamMode)) { sb.Append(' '); sb.Append(paramMode); }
            if (mem.Contains(Result)) { sb.Append(' '); sb.Append(result); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A local cursor
    /// 
    /// </summary>
    internal class CursorDeclaration : LocalVariableDec
    {
        internal const long
            CS = FetchStatement.Cursor; // long SqlCursor
        /// <summary>
        /// The specification for the cursor
        /// </summary>
        public long cs => (long)(mem[CS] ?? -1L);
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="i">The name</param>
        /// <param name="c">The cursor specification</param>
        public CursorDeclaration(long dp, SqlCursor sc,RowSet c) 
            : base(dp,sc, new BTree<long, object>(CS,c.defpos)+(_Domain,c)) 
        { }
        protected CursorDeclaration(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CursorDeclaration operator +(CursorDeclaration et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (CursorDeclaration)et.New(m + x);
        }

        public static CursorDeclaration operator +(CursorDeclaration e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (CursorDeclaration)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CursorDeclaration(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new CursorDeclaration(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(cs);
            if (nc!=cs)
                r += (CS, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[cs]?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Instantiate the cursor
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var cu = cx.obs[cs];
            if (cu is not null)
                cx.AddValue(cu, cu.Eval(cx));
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (CursorDeclaration)base._Replace(cx, so, sv);
            var nc = cx.obs[cs]?.Replace(cx, so, sv)?.defpos;
            if (nc != cs && nc is not null)
                r +=(cx, CS, nc);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" cs=");sb.Append(Uid(cs));
            return sb.ToString();
        }

    }
    /// <summary>
    /// An Exception handler for a stored procedure
    /// 
    /// </summary>
	internal class HandlerStatement : Executable
	{
        internal const long
            Action = -100, // long Executable
            Conds = -101, // BList<string>
            HType = -102; // Qlx
        /// <summary>
        /// The handler type: CONTINUE, EXIT, or UNDO
        /// </summary>
		public Qlx htype => (Qlx)(mem[HType]??Qlx.EXIT);
        /// <summary>
        /// A tree of condition names, SQLSTATE codes, "SQLEXCEPTION", "SQLWARNING", or "NOT_FOUND"
        /// </summary>
		public BList<string> conds => 
            (BList<string>?)mem[Conds]?? BList<string>.Empty;
        /// <summary>
        /// The statement to execute when one of these conditions happens
        /// </summary>
        public long action => (long)(mem[Action]??-1L);
        /// <summary>
        /// Constructor: a handler statement for a stored procedure
        /// </summary>
        /// <param name="t">CONTINUE, EXIT or UNDO</param>
		public HandlerStatement(long dp, Qlx t, string n)
            : base(dp, BTree<long, object>.Empty + (HType, t) + (ObInfo.Name, n)) { }
        protected HandlerStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static HandlerStatement operator +(HandlerStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (HandlerStatement)et.New(m + x);
        }

        public static HandlerStatement operator +(HandlerStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (HandlerStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new HandlerStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new HandlerStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var na = cx.Fix(action);
            if (na!=action)
            r += (Action, na);
            return r;
        }
        /// <summary>
        /// Obeying the handler declaration means installing the handler for each condition
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context _Obey(Context cx)
		{
            cx.exec = this;
            var a = (Activation)cx;
            a.saved = new ExecState(cx,cx.tr??throw new PEException("PE49204"));
            for (var c =conds.First();c is not null;c=c.Next())
                if (c.value() is string h)
				a.exceptions+=(h,new Handler(this,a));
            return cx;
		}
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[action]?.Calls(defpos, cx)??false;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (HandlerStatement)base._Replace(cx, so, sv);
            var na = ((Executable?)cx.obs[action])?.Replace(cx, so, sv)?.defpos;
            if (na != action && na is not null)
                r +=(cx, Action, na);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(htype.ToString());
            for (var b=conds.First();b is not null;b=b.Next())
            {
                sb.Append(' '); sb.Append(b.value());
            }
            sb.Append(" Action=");sb.Append(Uid(action));
            return sb.ToString();
        }

    }
    /// <summary>
    /// A Handler helps implementation of exception handling
    /// 
    /// </summary>
	internal class Handler : Executable
	{
        internal const long
            HDefiner = -103, // long Activation
            Hdlr = -104; // HandlerStatement
        /// <summary>
        /// The shared handler statement
        /// </summary>
		HandlerStatement? hdlr => (HandlerStatement?)mem[Hdlr];
        /// <summary>
        /// The activation that defined this
        /// </summary>
        long hdefiner =>(long)(mem[HDefiner]??-1L);
        /// <summary>
        /// Constructor: a Handler instance
        /// </summary>
        /// <param name="h">The HandlerStatement</param>
		public Handler(HandlerStatement h, Activation ad) 
            : base(ad.GetUid(), BTree<long, object>.Empty
            + (Hdlr, h) + (HDefiner, h.definer) + (Dependents,h.dependents))
        { }
        protected Handler(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Handler operator +(Handler et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (Handler)et.New(m + x);
        }

        public static Handler operator +(Handler e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (Handler)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Handler(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Handler(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nh = hdlr?.Fix(cx);
            if (hdlr != nh && nh != null)
                r += (Hdlr, nh);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return hdlr?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Execute the action part of the Handler Statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
                Activation? definer = null;
                for (Context? p = cx; definer == null && p != null; p = p.next)
                    if (p.cxid == hdefiner)
                        definer = p as Activation;
                if (hdlr?.htype == Qlx.UNDO)
                {
                    CompoundStatement? cs = null;
                    for (Context? p = cx; cs == null && p != null; p = p.next)
                        cs = p.exec as CompoundStatement;
                    if (cs != null && definer?.saved is ExecState es)
                    {
                        cx.db = es.mark;
                        cx.next = es.stack;
                    }
                }
                ((Executable?)cx.obs[hdlr?.action??-1L])?._Obey(cx);
                if (hdlr?.htype == Qlx.EXIT && definer?.next is Context nx)
                    return nx;
                var a = (Activation)cx;
                a.signal?.Throw(cx);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Hdef=");sb.Append(Uid(hdefiner));
            if (hdlr != null)
            {
                sb.Append(" Hdlr="); sb.Append(Uid(hdlr.defpos));
            }
            return sb.ToString();
        }

    }
    /// <summary>
    /// A Break statement for a stored procedure
    /// 
    /// </summary>
	internal class BreakStatement : Executable
	{
        /// <summary>
        /// Constructor: A break statement
        /// </summary>
        /// <param name="n">The label to break to</param>
		public BreakStatement(long dp,string? n) 
            : base(dp,(n==null)?BTree<long,object>.Empty:(BTree<long, object>.Empty+(Label,n)))
		{ }
        protected BreakStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BreakStatement operator +(BreakStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (BreakStatement)et.New(m + x);
        }

        public static BreakStatement operator +(BreakStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (BreakStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BreakStatement(defpos,m);
        }
        internal override DBObject New(long dp,BTree<long, object> m)
        {
            return new BreakStatement(dp, m);
        }
        /// <summary>
        /// Execute a break statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            cx.exec = this;
            if (label == "")
                return cx.SlideDown();
            for (Context? c = cx.next; c?.next != null; c = c.next)
                if (c is Activation ac && ac.label == label)
                {
                    cx = c;
                    break;
                }
            return cx;
        }
	}
    /// <summary>
    /// An assignment statement for a stored procedure
    /// 
    /// </summary>
	internal class AssignmentStatement : Executable
    {
        internal const long
            Val = -105, // long QlValue
            Vbl = -106; // long QlValue
        /// <summary>
        /// The left hand side of the assignment, checked for assignability
        /// </summary>
        public long vbl => (long)(mem[Vbl]??-1L);
        /// <summary>
        /// The right hand side of the assignment
        /// </summary>
		public long val => (long)(mem[Val]??-1L);
        /// <summary>
        /// Constructor: An assignment statement from the parser
        /// </summary>
		public AssignmentStatement(long dp,DBObject vb,QlValue va) 
            : base(dp,BTree<long, object>.Empty+(Vbl,vb.defpos)+(Val,va.defpos)
                  +(Dependents,CTree<long,bool>.Empty+(vb.defpos,true)+(va.defpos,true)))
        { }
        protected AssignmentStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static AssignmentStatement operator +(AssignmentStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (AssignmentStatement)et.New(m + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new AssignmentStatement(defpos,m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new AssignmentStatement(dp,m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var na = cx.Fix(val);
            if (na!=val)
                r += (Val, na);
            var nb = cx.Fix(vbl);
            if (nb!=vbl)
                r += (Vbl, nb);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return (cx.obs[vbl]?.Calls(defpos, cx)??false) || 
                (cx.obs[val]?.Calls(defpos,cx)??false);
        }
        public override Context _Obey(Context cx)
        {
            cx.exec = this;
            var vb = cx._Ob(vbl);
            var dm = vb?.domain??Domain.Content;
            if (cx.obs[val] is DBObject va && va.Eval(cx) is TypedValue tv)
            {
                if (vb is SqlValueExpr se && se.op == Qlx.DOT && cx._Ob(se.left)?.Eval(cx) is TNode tl
                    && cx._Ob(se.right) is QlInstance sc && tl.dataType is Table st 
                    && st.representation[sc.sPos] is Domain cd
                    && cd.Coerce(cx, tv) is TypedValue v1)
                    cx.Add(new Update(tl.tableRow, st.defpos, new CTree<long, TypedValue>(sc.sPos, v1),
                        cx.db.nextPos, cx));
                else if (vb is SqlField sf && cx._Ob(sf.from)?.Eval(cx) is TNode nf
                    && nf.dataType is Table tf && tf._PathDomain(cx).infos[cx.role.defpos] is ObInfo fi
                    && cx._Ob(fi.names[sf.name??""]) is TableColumn tc
                    && tc.domain.Coerce(cx, tv) is TypedValue v3)
                    cx.Add(new Update(nf.tableRow, tf.defpos, new CTree<long, TypedValue>(tc.defpos, v3),
                        cx.db.nextPos, cx));
                else if (dm != Domain.Content && dm != Domain.Null && dm.Coerce(cx, tv) is TypedValue v2)
                    cx.values += (vb?.defpos ?? -1L, v2);
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (AssignmentStatement)base._Replace(cx, so, sv);
            var nb = cx.obs[vbl]?.Replace(cx, so, sv)?.defpos;
            if (nb != vbl && nb is not null)
                r+=(Vbl, nb);
            var na = cx.obs[val]?.Replace(cx, so, sv)?.defpos;
            if (na != val && na is not null)
                r +=(Val, na);  
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (vbl != -1L && val != -1L)
            {
                sb.Append(' '); sb.Append(Uid(vbl)); 
                sb.Append('='); sb.Append(Uid(val)); 
            } 
            return sb.ToString();
        }
	}
    /// <summary>
    /// A multiple assignment statement for a stored procedure.
    /// The right hand side must be row valued, and the left hand side is a
    /// tree of variable identifiers.
    /// 
    /// </summary>
    internal class MultipleAssignment : Executable
    {
        internal const long
            LhsType = -107, // Domain
            List = -108, // BList<long?> QlValue
            Rhs = -109; // long QlValue
        /// <summary>
        /// The tree of identifiers
        /// </summary>
        internal BList<long?> list => (BList<long?>?)mem[List]??BList<long?>.Empty;
        /// <summary>
        /// The row type of the lefthand side, used to coerce the given value 
        /// </summary>
        Domain lhsType => (Domain)(mem[LhsType]??Domain.Null);
        /// <summary>
        /// The row-valued right hand side
        /// </summary>
        public long rhs => (long)(mem[Rhs]??-1L);
        /// <summary>
        /// Constructor: A new multiple assignment statement from the parser
        /// </summary>
        public MultipleAssignment(long dp,Context cx,BList<Ident>ls,QlValue rh) : 
            base(dp,_Mem(cx,ls,rh))
        { }
        protected MultipleAssignment(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,BList<Ident> lh,QlValue rg)
        {
            var dm = rg.domain ?? Domain.Null;
            var r = new BTree<long, object>(Rhs, rg.defpos);
            var ls = BList<long?>.Empty;
            for (var b = lh.First(); b != null; b = b.Next())
                if (b.value() is Ident id && cx.obs[id.uid] is QlValue v
                            && v.domain is Domain vd)
                {
                    dm = dm.Constrain(cx, id.uid, vd);
                    ls += v.defpos;
                }
            return r +(List,ls)+(LhsType,rg);
        }
        public static MultipleAssignment operator +(MultipleAssignment et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (MultipleAssignment)et.New(m + x);
        }

        public static MultipleAssignment operator +(MultipleAssignment e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == List)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (MultipleAssignment)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MultipleAssignment(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new MultipleAssignment(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object> m)
        {
            var r = base._Fix(cx,m);
            var nt = (Domain)lhsType.Fix(cx);
            if (nt != lhsType)
                r += (LhsType, nt);
            var nl = cx.FixLl(list);
            if (nl != list)
                r += (List, nl);
            var nr = cx.Fix(rhs);
            if (nr != rhs)
                r += (Rhs, nr);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[rhs]?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Execute the multiple assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            if (cx.obs[rhs] is QlValue sv)
            {
                TRow r = (TRow)sv.Eval(cx);
                for (int j = 0; j < r.Length; j++)
                    if (list[j] is long p)
                        cx.values += (p, r[j] ?? TNull.Value);
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (MultipleAssignment)base._Replace(cx, so, sv);
            var nd = (Domain)lhsType.Replace(cx, so, sv);
            if (nd != lhsType && nd is not null)
                r += (LhsType, nd);
            var nl = cx.ReplacedLl(list);
            if (nl != list)
                r+=(cx, List, nl);
            var na = ((QlValue?)cx.obs[rhs])?.Replace(cx, so, sv)?.defpos;
            if (na != rhs && na is not null)
                r+= (cx, Rhs, na);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" LhsType: "); sb.Append(lhsType.rowType);
            var cm = " Lhs: ";
            for (var b = list.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(" Rhs="); sb.Append(Uid(rhs));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A treament statement. Changing the domain of the current row (from _From)
    /// will move the row into a subtype table. nd must be a subtype of _From.
    /// </summary>
    internal class TreatAssignment : Executable
    {
        /// <summary>
        /// Constructor: A new treat assignment statement from the parser
        /// </summary>
        public TreatAssignment(long dp, UDType nd) :
            base(dp, new BTree<long,object>(_Domain,nd))
        { }
        protected TreatAssignment(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TreatAssignment operator +(TreatAssignment et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (TreatAssignment)et.New(m + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TreatAssignment(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new TreatAssignment(dp, m);
        }
        /// <summary>
        /// Execute the treatment: reparse the values of this row and add it to the new domain table.
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = cx; 
            a.exec = this;
            return cx;
        }
    }
    /// <summary>
    /// A return statement for a stored procedure or function
    /// 
    /// </summary>
	internal class ReturnStatement : Executable
    {
        internal const long
            Ret = -110; // long QlValue
        /// <summary>
        /// The return value
        /// </summary>
		public long ret => (long)(mem[Ret] ?? -1L);
    //    public long result => (long)(mem[SqlInsert.Value] ??-1L);
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(long dp,QlValue v) : base(dp, 
            new BTree<long, object>(Ret,v.defpos))
        { }
        protected ReturnStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ReturnStatement operator +(ReturnStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ReturnStatement)et.New(m + x);
        }

        public static ReturnStatement operator +(ReturnStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (ReturnStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ReturnStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ReturnStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nr = cx.Fix(ret);
            if (nr != ret)
                r += (Ret, nr);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[ret]?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Execute the return statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            a.val = cx.obs[ret]?.Eval(cx) ?? TNull.Value;
            cx = a.SlideDown();
            if (cx.obs[cx.result] is ExplicitRowSet es && es.CanTakeValueOf(a.val.dataType))
                cx.obs += (cx.result,es+(cx.GetUid(), a.val));
            cx.lastret = defpos;
            return cx;
		}
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ReturnStatement)base._Replace(cx, so, sv);
            var nr = ((QlValue?)cx.obs[ret])?.Replace(cx, so, sv)?.defpos;
            if (nr != (cx.done[ret]?.defpos ?? ret) && nr is not null)
                r+=(cx, Ret, nr);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" -> ");sb.Append(Uid(ret));
            if (result>0)
            {
                sb.Append(" Result ");sb.Append(Uid(result));
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Case statement for a stored procedure
    /// 
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        internal const long
            _Operand = -112, // long QlValue
            Whens = -113; // BList<long?> WhenPart
        /// <summary>
        /// The test expression
        /// </summary>
        public long operand => (long)(mem[_Operand]??-1L);
        /// <summary>
        /// A tree of when parts
        /// </summary>
        public BList<long?> whens => (BList<long?>?)mem[Whens]?? BList<long?>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
        public BList<long?> els => (BList<long?>?)mem[IfThenElse.Else]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a case statement from the parser
        /// </summary>
        public SimpleCaseStatement(long dp,QlValue op,BList<WhenPart> ws,
            BList<long?> ss) : 
            base(dp,_Mem(op,ws,ss))
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(QlValue op, BList<WhenPart> ws,
            BList<long?> ss)
        {
            var r = new BTree<long, object>(_Operand, op.defpos);
            var wl = BList<long?>.Empty;
            for (var b = ws.First(); b != null; b = b.Next())
                if (b.value() is WhenPart w)
                    wl += w.defpos;
            r += (IfThenElse.Else, ss);
            return r + (Whens, wl);
        }
        public static SimpleCaseStatement operator +(SimpleCaseStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SimpleCaseStatement)et.New(m + x);
        }

        public static SimpleCaseStatement operator +(SimpleCaseStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == IfThenElse.Else || p == Whens)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SimpleCaseStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SimpleCaseStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SimpleCaseStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.FixLl(els);
            if (ne!=els)
            r += (IfThenElse.Else, ne);
            var no = cx.Fix(operand);
            if (no!=operand)
            r += (_Operand, no);
            var nw = cx.FixLl(whens);
            if (nw!=whens)
            r += (Whens, nw);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value() is long p && (cx.obs[p]?.Calls(defpos, cx)??false))
                    return true;
            return Calls(els,defpos,cx) || (cx.obs[operand]?.Calls(defpos,cx)??false);
        }
        /// <summary>
        /// Execute the case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for(var c = whens.First();c is not null; c=c.Next())
                if (cx.obs[operand] is QlValue sv && sv.Matches(cx)==true
                    && c.value() is long p && cx.obs[p] is WhenPart w)
                    return ObeyList(w.stms, cx);
            return ObeyList(els, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SimpleCaseStatement)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[operand])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[operand]?.defpos ?? operand) && no is not null)
                r+=(cx, _Operand, no);
            var nw = cx.ReplacedLl(whens);
            if (nw != whens)
                r +=(cx, Whens, nw);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r+=(cx, IfThenElse.Else, ne);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand: "); sb.Append(Uid(operand));
            var cm = " Whens: ";
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A searched case statement
    /// 
    /// </summary>
	internal class SearchedCaseStatement : Executable
	{
        /// <summary>
        /// A tree of when parts
        /// </summary>
		public BList<long?> whens => (BList<long?>?)mem[SimpleCaseStatement.Whens]??BList<long?>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
		public BList<long?> els => (BList<long?>?)mem[IfThenElse.Else]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(long dp,BList<WhenPart>ws,BList<long?>ss) 
            : base(dp,_Mem(ws,ss))
        {  }
        protected SearchedCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(BList<WhenPart>ws,BList<long?>ss)
        {
            var r = BTree<long, object>.Empty;
            if (ss != BList<long?>.Empty)
                r += (IfThenElse.Else, ss);
            var wl = BList<long?>.Empty;
            for (var b = ws.First(); b != null; b = b.Next())
                if (b.value() is WhenPart w)
                    wl += w.defpos;
            return r + (SimpleCaseStatement.Whens,wl);
        }
        public static SearchedCaseStatement operator +(SearchedCaseStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SearchedCaseStatement)et.New(m + x);
        }

        public static SearchedCaseStatement operator +(SearchedCaseStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SearchedCaseStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SearchedCaseStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SearchedCaseStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.FixLl(els);
            if (ne != els)
                r += (IfThenElse.Else, ne);
            var nw = cx.FixLl(whens);
            if (nw != whens)
                r += (SimpleCaseStatement.Whens, nw);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value() is long p && (cx.obs[p]?.Calls(defpos, cx)??false))
                    return true;
            return Calls(els, defpos, cx);
        }
        /// <summary>
        /// Execute a searched case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// 
        public override Context _Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for (var c = whens.First(); c != null; c = c.Next())
                if (c.value() is long p && cx.obs[p] is WhenPart w
                    && ((QlValue?)cx.obs[w.cond])?.Matches(cx) == true)
                        return ObeyList(w.stms, cx);
			return ObeyList(els,cx);
		}
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SearchedCaseStatement)base._Replace(cx, so, sv);
            var nw = cx.ReplacedLl(whens);
            if (nw != whens)
                r+=(cx, SimpleCaseStatement.Whens, nw);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r+=(cx, IfThenElse.Else, ne);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " Whens: ";
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A when part for a searched case statement or trigger action
    /// 
    /// </summary>
	internal class WhenPart :Executable
	{
        internal const long
            Cond = -114; // long QlValue or -1L
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public long cond => (long)(mem[Cond]??-1L);
        /// <summary>
        /// a tree of statements
        /// </summary>
		public BList<long?> stms =>(BList<long?>?)mem[CompoundStatement.Stms]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A tree of statements for this when</param>
        public WhenPart(long dp,QlValue? v, BList<long?> s) 
            : base(dp, ((v is null)?BTree<long, object>.Empty:new BTree<long,object>(Cond,v.defpos))
                  +(CompoundStatement.Stms,s))
        { }
        protected WhenPart(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhenPart operator +(WhenPart et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (WhenPart)et.New(m + x);
        }

        public static WhenPart operator +(WhenPart e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == CompoundStatement.Stms)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (WhenPart)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WhenPart(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new WhenPart(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(cond);
            if (cond != nc)
                r += (Cond, nc);
            var ns = cx.FixLl(stms);
            if (ns != stms)
                r += (CompoundStatement.Stms, ns);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(stms,defpos,cx) || (cx.obs[cond]?.Calls(defpos, cx)??false);
        }
        public override Context _Obey(Context cx)
        {
            var a = cx;
            if (cond == -1L || ((QlValue?)cx.obs[cond])?.Matches(cx)==true)
            {
                a.val = TBool.True;
                return ObeyList(stms, cx);
            }
            a.val = TBool.False;
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WhenPart)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[cond])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cond]?.defpos ?? cond) && no is not null)
                r+=(cx, Cond, no);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r+=(cx, CompoundStatement.Stms, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (cond!=-1L)
                sb.Append(" Cond: ");sb.Append(Uid(cond));
            sb.Append(" Stms: ");
            var cm = "(";
            for (var b = stms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// An if-then-else statement for a stored proc/func
    /// 
    /// </summary>
	internal class IfThenElse : Executable
	{
        internal const long
            Else = -116, // BList<long?> Executable
            Elsif = -117, // BList<long?> Executable
            Search = -118, // long QlValue
            Then = -119; // BList<long?> Executable
        /// <summary>
        /// The test condition
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
        /// <summary>
        /// The then statements
        /// </summary>
		public BList<long?> then => (BList<long?>?)mem[Then]?? BList<long?>.Empty;
        /// <summary>
        /// The elsif parts
        /// </summary>
		public BList<long?> elsif => (BList<long?>?)mem[Elsif] ?? BList<long?>.Empty;
        /// <summary>
        /// The else part
        /// </summary>
		public BList<long?> els => (BList<long?>?)mem[Else] ?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: an if-then-else statement from the parser
        /// </summary>
		public IfThenElse(long dp,QlValue se,BList<long?>th,BList<long?>ei,BList<long?> el) 
            : base(dp,new BTree<long, object>(Search,se.defpos) +(Then,th) +(Elsif,ei) + (Else,el))
		{}
        protected IfThenElse(long dp, BTree<long, object> m) : base(dp, m) { }
        public static IfThenElse operator +(IfThenElse et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (IfThenElse)et.New(m + x);
        }

        public static IfThenElse operator +(IfThenElse e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == Elsif || p==Then || p==Else)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (IfThenElse)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new IfThenElse(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new IfThenElse(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(search);
            if (ns != search)
                r += (Search, ns);
            var nt = cx.FixLl(then);
            if (nt != then)
                r += (Then, nt);
            var ne = cx.FixLl(els);
            if (els != ne)
                r += (Else, ne);
            var ni = cx.FixLl(elsif);
            if (ni != elsif)
                r += (Elsif, ni);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(then,defpos, cx)||Calls(elsif,defpos,cx)||Calls(els,defpos,cx);
        }
        /// <summary>
        /// _Obey an if-then-else statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx; 
            a.exec = this;
            if (((QlValue?)cx.obs[search])?.Matches(cx)==true)
                return ObeyList(then, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (g.value() is long p && cx.obs[p] is IfThenElse f 
                    && ((QlValue?)cx.obs[f.search])?.Matches(cx)==true)
                    return ObeyList(f.then, cx);
            return ObeyList(els, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (IfThenElse)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no is not null)
                r+=(cx, Search, no);
            var nt = cx.ReplacedLl(then);
            if (nt != then)
                r+=(cx, Then, nt);
            var ni = cx.ReplacedLl(elsif);
            if (ni != elsif)
                r+=(cx, Elsif, ni);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r+=(cx, Else, ne);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand="); sb.Append(Uid(search));
            var cm = " Then: ";
            for (var b = then.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            cm = " ElsIf: ";
            for (var b = elsif.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    
    internal class XmlNameSpaces : Executable
    {
        internal const long
            Nsps = -120; // CTree<string,string>
        /// <summary>
        /// A tree of namespaces to be added
        /// </summary>
        public CTree<string, string> nsps => (CTree<string,string>?)mem[Nsps]
            ??CTree<string,string>.Empty;
        /// <summary>
        /// Constructor
        /// </summary>
        public XmlNameSpaces(long dp) : base(dp,BTree<long, object>.Empty) { }
        protected XmlNameSpaces(long dp, BTree<long, object> m) : base(dp, m) { }
        public static XmlNameSpaces operator +(XmlNameSpaces et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (XmlNameSpaces)et.New(m + x);
        }

        public static XmlNameSpaces operator +(XmlNameSpaces e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (XmlNameSpaces)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new XmlNameSpaces(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new XmlNameSpaces(dp, m);
        }
        /// <summary>
        /// Add the namespaces
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            for(var b=nsps.First();b!= null;b=b.Next())
                if (b.value() is string s)
                cx.nsps+=(b.key(),s);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " ";
            for (var b = nsps.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key()); sb.Append('=');
                sb.Append(b.value());
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A while statement for a stored proc/func
    /// 
    /// </summary>
	internal class WhileStatement : Executable
	{
        internal const long
            What = -123; // BList<long?> Executable
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public long search => (long)(mem[IfThenElse.Search]??-1L);
        /// <summary>
        /// The statements to execute
        /// </summary>
		public BList<long?> what => (BList<long?>?)mem[What]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a while statement from the parser
        /// </summary>
        /// <param name="n">The label for the while</param>
		public WhileStatement(long dp,string n) : base(dp,new BTree<long,object>(Label,n)) 
        {  }
        protected WhileStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhileStatement operator +(WhileStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (WhileStatement)et.New(m + x);
        }

        public static WhileStatement operator +(WhileStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == What)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (WhileStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WhileStatement(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new WhileStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(search);
            if (ns != search)
                r += (IfThenElse.Search, ns);
            var nw = cx.FixLl(what);
            if (nw != what)
                r += (What, nw);
            return r;
        }
        /// <summary>
        /// Execute a while statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx; 
            a.exec = this;
            var na = cx;
            while (na==cx && a.signal == null && ((QlValue?)cx.obs[search])?.Matches(cx)==true)
            {
                var lp = new Activation(cx, label ?? "") { cont = a, brk = a };
                na = ObeyList(what, lp);
                if (na == lp)
                    na = cx;
                a = (Activation)na.SlideDown();
                a.signal = lp.signal;
            }
            return a;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(what,defpos,cx) || (cx.obs[search]?.Calls(defpos, cx)??false);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WhileStatement)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no is not null)
                r+=(cx, IfThenElse.Search, no);
            var nw = cx.ReplacedLl(what);
            if (nw != what)
                r+=(cx, What, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand="); sb.Append(Uid(search));
            var cm = " What: ";
            for (var b = what.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A repeat statement for a stored proc/func
    /// 
    /// </summary>
	internal class RepeatStatement : Executable
	{
        /// <summary>
        /// The condition for stopping
        /// </summary>
		public long search => (long)(mem[IfThenElse.Search]??-1L);
        /// <summary>
        /// The tree of statements to execute at least once
        /// </summary>
		public BList<long?> what => (BList<long?>?)mem[WhileStatement.What]??BList<long?>.Empty;
         /// <summary>
        /// Constructor: a repeat statement from the parser
        /// </summary>
        /// <param name="n">The label</param>
        public RepeatStatement(long dp,string n) : base(dp,new BTree<long,object>(Label,n)) 
        {  }
        protected RepeatStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RepeatStatement operator +(RepeatStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (RepeatStatement)et.New(m + x);
        }

        public static RepeatStatement operator +(RepeatStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == WhileStatement.What)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (RepeatStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RepeatStatement(defpos, m);
        }
       internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new RepeatStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(search);
            if (ns != search)
                r += (IfThenElse.Search, ns);
            var nw = cx.FixLl(what);
            if (nw != what)
                r += (WhileStatement.What, nw);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(what, defpos, cx) || (cx.obs[search]?.Calls(defpos,cx)??false);
        }
        /// <summary>
        /// Execute the repeat statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var act = new Activation(cx,label??"");
            for (; ;)
            {
                var na = ObeyList(what, act);
                if (na != act)
                    break;
                act.signal?.Throw(act);
                if (((QlValue?)cx.obs[search])?.Matches(act)!=false)
                    break;
            }
            cx = act.SlideDown(); 
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RepeatStatement)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no is not null)
                r+=(cx,IfThenElse.Search, no);
            var nw = cx.ReplacedLl(what);
            if (nw != what)
                r +=(cx, WhileStatement.What, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " What: ";
            for (var b = what.First(); b != null; b = b.Next())
            if (b.value() is long p){
                sb.Append(cm); cm = ";";
                sb.Append(Uid(p));
            }
            sb.Append(" Operand="); sb.Append(Uid(search));
            return sb.ToString();
        }
    }
    /// <summary>
    /// An Iterate (like C continue;) statement for a stored proc/func 
    /// 
    /// </summary>
	internal class IterateStatement : Executable
	{
        /// <summary>
        /// Constructor: an iterate statement from the parser
        /// </summary>
        /// <param name="n">The name of the iterated SQL-statement</param>
		public IterateStatement(long dp,string n):base(dp,BTree<long, object>.Empty
            +(Label,n))
		{ }
        protected IterateStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static IterateStatement operator +(IterateStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (IterateStatement)et.New(m + x);
        }

        public static IterateStatement operator +(IterateStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (IterateStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new IterateStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new IterateStatement(dp, m);
        }
        /// <summary>
        /// Execute the iterate statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx; // from the top of the stack each time
            a.exec = this;
            return a.cont ?? cx;
		}
	}
    /// <summary>
    /// A Loop statement for a stored proc/func
    /// 
    /// </summary>
	internal class LoopStatement : Executable
	{
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<long?> stms => (BList<long?>?)mem[CompoundStatement.Stms]??BList<long?>.Empty;
        /// <summary>
        /// Constructor: a loop statement from the parser
        /// </summary>
        /// <param name="s">The statements</param>
        /// <param name="n">The loop identifier</param>
		public LoopStatement(long dp,string n):base(dp,new BTree<long,object>(Label,n))
		{ }
        protected LoopStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LoopStatement operator +(LoopStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (LoopStatement)et.New(m + x);
        }

        public static LoopStatement operator +(LoopStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == CompoundStatement.Stms)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (LoopStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LoopStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new LoopStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.FixLl(stms);
            if (ns != stms)
                r += (CompoundStatement.Stms, ns);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(stms,defpos, cx);
        }
        /// <summary>
        /// Execute the loop statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = (Activation)cx; // from the top of the stack each time
            a.exec = this;
            var act = new Activation(cx,label??"");
            var lp = new Activation(act,"");
            var na = lp;
            while(na==lp)
            {
                lp.brk = a;
                lp.cont = act;
                na = (Activation)ObeyList(stms, lp);
                if (na==lp)
                    lp.signal?.Throw(a);
            }
            if (na == lp)
            {
                act.signal?.Throw(a);
                act = (Activation)lp.SlideDown();
            }
            return act.SlideDown();
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LoopStatement)base._Replace(cx, so, sv);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r += (cx, CompoundStatement.Stms, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " Stms: ";
            for (var b = stms.First(); b != null; b = b.Next())
            if (b.value() is long p){
                sb.Append(cm); cm = ";";
                sb.Append(Uid(p));
            }
            return sb.ToString();
        }
    }
    internal class ForStatement : Executable
    {
        internal const long
            CountCol = -142; // long QlValue
        internal long vbl => (long)(mem[AssignmentStatement.Vbl] ?? -1L);
        internal long list => (long)(mem[AssignmentStatement.Val] ?? -1L);
        internal long col => (long)(mem[CountCol] ?? -1L);
        internal Qlx op => (Qlx)(mem[SqlValueExpr.Op] ?? Qlx.NO);
        internal long stm => (long)(mem[Procedure.Body] ?? -1L);
        internal ForStatement(long dp, QlValue f, QlValue v, Qlx o, long c, Executable x, Context cx)
            : this(dp, BTree<long, object>.Empty + (Procedure.Body,x.defpos)
                  + (_Domain,x.domain) + (CountCol,c) + (SqlValueExpr.Op,o)
                  + (AssignmentStatement.Vbl, f.defpos) + (AssignmentStatement.Val, v.defpos))
        { }
        internal ForStatement(long dp, BTree<long, object>? m = null) : base(dp, m)
        { }
        public static ForStatement operator+(ForStatement fs,(long,object)x)
        {
            return new ForStatement(fs.defpos, fs.mem + x);
        }
        public override Context _Obey(Context cx)
        {
            var ls = cx._Ob(list) as SqlValueArray??throw new DBException("PE10601");
            var vb = cx._Ob(vbl) as QlValue ?? throw new DBException("PE10602");
            var dl = cx._Ob(ls.domain.defpos) as Domain ?? throw new DBException("PE10603");
            var et = cx._Ob(dl.elType?.defpos ?? -1L) as Domain ?? throw new DBException("PE10604");
            var vl = ls.Eval(cx) as TList;
            var fe = vl?[0];
            if (vb.domain.kind != Qlx.CONTENT || fe is null) throw new DBException("42000").Add(Qlx.FOR_STATEMENT); // should be unbound
            var rt = new BList<long?>(vbl);
            var rs = new CTree<long, Domain>(vbl, dl);
            if (op!=Qlx.NO)
            {
                rt += col; rs += (col,Domain.Int);
            }
            var rd = (rt.Length==0)?Domain.TableType:new Domain(-1L, cx, Qlx.TABLE, rs, rt);
            var bs = BindingRowSet.Get(cx, new TRow(rd, fe));
            for (var i = 0; i < (vl?.Length ?? 0); i++)
            {
                var vs = new CTree<long, TypedValue>(vbl, vl?[i]??TNull.Value);
                switch(op)
                {
                    case Qlx.ORDINALITY: vs += (col, new TInt(i + 1)); break; 
                    case Qlx.OFFSET: vs += (col, new TInt(i)); break;
                }
                bs += (cx, new TRow(rd, vs));
            }
            cx.result = bs.defpos;
            if (cx._Ob(stm) is Executable st)
                cx = st.Obey(cx);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(Uid(vbl)); sb.Append(" IN "); sb.Append(Uid(list));
            if (col>0)
            { sb.Append(' '); sb.Append(op); sb.Append(' '); sb.Append(Uid(col)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A for statement for a stored proc/func
    /// From SQL PSM: all the conventions are different (e.g. forvn is not an ordinary QlValue)
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        internal const long
            ForVn = -125, // string
            Sel = -127; // long RowSet
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public long sel => (long)(mem[Sel]??-1L);
        /// <summary>
        /// The identifier in the AS part
        /// </summary>
		public string? forvn => (string?)mem[ForVn];
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<long?> stms => (BList<long?>?)mem[CompoundStatement.Stms]??BList<long?>.Empty;
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
        public ForSelectStatement(long dp, string n,Ident vn, 
            RowSet rs,BList<long?>ss ) 
            : base(dp,BTree<long, object>.Empty+ (Sel,rs.defpos) + (CompoundStatement.Stms,ss)
                  +(Label,n)+(ForVn,vn.ident))
		{ }
        protected ForSelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ForSelectStatement operator +(ForSelectStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ForSelectStatement)et.New(m + x);
        }

        public static ForSelectStatement operator +(ForSelectStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == CompoundStatement.Stms)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (ForSelectStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ForSelectStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ForSelectStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(sel);
            if (ns != sel)
                r += (Sel, ns);
            var nn = cx.FixLl(stms);
            if (nn != stms)
                r += (CompoundStatement.Stms, nn);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return (cx.obs[sel]?.Calls(defpos, cx)??false) || Calls(stms,defpos,cx);
        }
        /// <summary>
        /// Execute a FOR statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            cx.exec = this;
            if (cx.obs[sel] is RowSet qs)
            {
                var ac = new Activation(cx, label ?? "");
                ac.Add(qs);
                for (var rb = qs.First(ac); rb != null; rb = rb.Next(ac))
                {
                    cx.cursors += (qs.defpos, rb);
                    if (cx.cursors[qs.defpos] is Cursor cu)
                        ac.values += cu.values;
                    ac.brk = cx as Activation;
                    ac.cont = ac;
                    ac = (Activation)ObeyList(stms, ac);
                    ac.signal?.Throw(cx);
                }
                return ac.SlideDown();
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ForSelectStatement)base._Replace(cx, so, sv);
            var no = ((RowSet?)cx.obs[sel])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[sel]?.defpos??sel) && no is not null)
                r+=(cx, Sel, no);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r+=(cx, CompoundStatement.Stms, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (forvn != null)
            { sb.Append(" Var="); sb.Append(forvn); }
            sb.Append(" Sel="); sb.Append(Uid(sel));
            var cm = " Stms: ";
            for (var b = stms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ";";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// An Open statement for a cursor
    /// 
    /// </summary>
	internal class OpenStatement : Executable
	{
        long cursor => (long)(mem[FetchStatement.Cursor]??-1L);
        /// <summary>
        /// Constructor: an open statement from the parser
        /// </summary>
        /// <param name="n">the cursor name</param>
		public OpenStatement(long dp, SqlCursor c) : base(dp,BTree<long, object>.Empty
            +(FetchStatement.Cursor,c.defpos))
		{ }
        protected OpenStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static OpenStatement operator +(OpenStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (OpenStatement)et.New(m + x);
        }

        public static OpenStatement operator +(OpenStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (OpenStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new OpenStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new OpenStatement(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(cursor);
            if (nc != cursor)
                r += (FetchStatement.Cursor, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return ((SqlCursor?)cx.obs[cursor])?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (OpenStatement)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cursor]?.defpos ?? cursor) && no is not null)
                r+=(cx, FetchStatement.Cursor, no);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Cursor: "); sb.Append(cursor);
            return sb.ToString();
        }

    }
    /// <summary>
    /// A Close statement for a cursor
    /// 
    /// </summary>
	internal class CloseStatement : Executable
	{
        public long cursor => (long)(mem[FetchStatement.Cursor]??-1L);
        /// <summary>
        /// Constructor: a close statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
		public CloseStatement(long dp, SqlCursor c) : base(dp, BTree<long, object>.Empty
            + (FetchStatement.Cursor, c.defpos) + (_Domain, c.domain))
        { }
        protected CloseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CloseStatement operator +(CloseStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (CloseStatement)et.New(m + x);
        }

        public static CloseStatement operator +(CloseStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (CloseStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CloseStatement(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new CloseStatement(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(cursor);
            if (nc!=cursor)
                r += (FetchStatement.Cursor, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return ((SqlCursor?)cx.obs[cursor])?.Calls(defpos, cx)??false;
        }
        /// <summary>
        /// Execute the close statement
        /// </summary>
        public override Context _Obey(Context cx)
        {
            cx.Add(new EmptyRowSet(defpos,cx,Domain.Null));
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (CloseStatement)base._Replace(cx, so, sv);
            var no = ((QlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cursor]?.defpos ?? cursor) && no is not null)
                r +=(cx, FetchStatement.Cursor, no);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Cursor: "); sb.Append(cursor);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A fetch statement for a stored proc/func
    /// 
    /// </summary>
	internal class FetchStatement : Executable
	{
        internal const long
            Cursor = -129, // long SqlCursor
            How = -130, // Qlx
            Outs = -131, // BList<long?> QlValue
            Where = -132; // long QlValue
        long cursor =>(long)(mem[Cursor]??-1L);
        /// <summary>
        /// The behaviour of the Fetch
        /// </summary>
        public Qlx how => (Qlx)(mem[How]??Qlx.ALL);
        /// <summary>
        /// The given absolute or relative position if specified
        /// </summary>
        public long where => (long)(mem[Where]??-1L);
        /// <summary>
        /// The tree of assignable expressions to receive values
        /// </summary>
		public BList<long?> outs => (BList<long?>?)mem[Outs]?? BList<long?>.Empty; 
        /// <summary>
        /// Constructor: a fetch statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
        /// <param name="h">The fetch behaviour: ALL, NEXT, LAST PRIOR, ABSOLUTE, RELATIVE</param>
        /// <param name="w">The output variables</param>
        public FetchStatement(long dp, SqlCursor n, Qlx h, QlValue? w)
        : base(dp, BTree<long, object>.Empty + (Cursor, n.defpos) + (How, h) + (Where, w?.defpos??-1L))
        { }
        protected FetchStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static FetchStatement operator +(FetchStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (FetchStatement)et.New(m + x);
        }

        public static FetchStatement operator +(FetchStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == Outs)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (FetchStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FetchStatement(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new FetchStatement(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(cursor);
            if (nc != cursor)
                r += (Cursor, nc);
            var no = cx.FixLl(outs);
            if (outs != no)
                r += (Outs, no);
            var nw = cx.Fix(where);
            if (where != nw)
                r += (Where, nw);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return (cx.obs[cursor]?.Calls(defpos, cx)??false) || 
                (cx.obs[where]?.Calls(defpos,cx)??false) || Calls(outs,defpos,cx);
        }
        /// Execute a fetch
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context _Obey(Context cx)
        {
            if (cx.obs[cursor] is SqlCursor cu && cx.obs[cu.spec] is RowSet cs)
            {
                // position the cursor as specified
                var rqpos = 0L;
                var rb = cx.values[cu.defpos] as Cursor;
                if (rb != null)
                    rqpos = rb._pos + 1;
                switch (how)
                {
                    case Qlx.NEXT: break; // default case
                    case Qlx.PRIOR: // reposition so we can refecth
                        rqpos -= 2;
                        break;
                    case Qlx.FIRST:
                        rqpos = 1;
                        break;
                    case Qlx.LAST:
                        {
                            int n = 0;
                            for (var e = ((RowSet?)cx.obs[cs.defpos])?.First(cx); e != null;
                                e = e.Next(cx))
                                n++;
                            rqpos = n - 1;
                            break;
                        }
                    case Qlx.ABSOLUTE:
                        rqpos = cx.obs[where]?.Eval(cx)?.ToLong()??-1L;
                        break;
                    case Qlx.RELATIVE:
                        rqpos += (cx.obs[where]?.Eval(cx)?.ToLong()??-1L);
                        break;
                }
                if (rb == null || rqpos == 0)
                    rb = ((RowSet?)cx.obs[cs.defpos])?.First(cx);
                while (rb != null && rqpos != rb._pos)
                    rb = rb.Next(cx);
                if (rb != null)
                {
                    cx.values += (cu.defpos, rb);
                    for (int i = 0; i < cs.rowType.Length; i++)
                        if (rb[i] is TypedValue c && outs[i] is long ou 
                            &&  cx.obs[ou] is QlValue sv)
                                cx.AddValue(sv, c);
 
                }
                else
                    cx = new Signal(defpos, "02000", "No obs").Obey(cx);
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (FetchStatement)base._Replace(cx, so, sv);
            var nc = ((QlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (nc != (cx.done[cursor]?.defpos??cursor) && nc is not null)
                r+=(cx, Cursor, nc);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r+=(cx, Outs, no);
            var nw = ((QlValue?)cx.obs[where])?.Replace(cx, so, sv)?.defpos;
            if (nw != (cx.done[where]?.defpos ?? where) && nw is not null)
                r+=(cx, Where, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (how != Qlx.ALL)
            { sb.Append(' '); sb.Append(how); }
            if (cursor != -1)
            { sb.Append(" Cursor: "); sb.Append(Uid(cursor)); }
            var cm = " Into: ";
            for (var b = outs.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            if (where!=-1L)
            { sb.Append(" Where= "); sb.Append(Uid(where)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A call statement for a stored proc/func
    /// 
    /// </summary>
	internal class CallStatement : Executable
    {
        internal const long
             Call = -335; // long SqlCall
        public long call => (long)(mem[Call] ?? -1L);
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long dp, SqlCall c, BTree<long, object>? m = null)
    : base(dp, m ?? BTree<long, object>.Empty
          + (_Domain, c.domain)
          + (Call, c.defpos) + (Dependents, new CTree<long, bool>(c.defpos, true))
          + (ObInfo.Name, c.name ?? ""))
        { }
        protected CallStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CallStatement operator +(CallStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (CallStatement)et.New(m + x);
        }

        public static CallStatement operator +(CallStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (CallStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CallStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new CallStatement(dp,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[call]?._Rdc(cx) ?? CTree<long, bool>.Empty;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var nc = cx.Fix(call);
            if (nc != call)
                r += (Call, nc);
            return r;
        }
        /// <summary>
        /// Execute a proc/method call
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context _Obey(Context cx)
        {
            cx.exec = this;
            if (cx.obs[call] is not SqlCall sc)
                throw new DBException("42108");
            var proc = (Procedure)(cx.db.objects[sc.procdefpos] ?? throw new DBException("42108"));
            var ac = new Context(cx, cx._Ob(proc.definer) as Role ?? throw new DBException("42108"),
                cx.user ?? throw new DBException("42108"));
            var a = proc?.Exec(ac, sc.parms) ?? cx;
            return a.SlideDown();
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (CallStatement)base._Replace(cx, so, sv);
            var ca = cx.ObReplace(call, so, sv);
            if (ca != call)
                r+=(cx, Call, ca);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return (cx.obs[call] is SqlCall sc) && sc.Calls(defpos, cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(name);
            sb.Append(' '); sb.Append(Uid(call));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A signal statement for a stored proc/func
    /// </summary>
    internal class SignalStatement : Executable
    {
        internal const long
            Objects = -137, // BList<string>
            _Signal = -138, // string
            SetList = -139, // BTree<Qlx,long?>
            SType = -140; // Qlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Qlx stype => (Qlx)(mem[SType] ?? Qlx.NO); // RAISE or RESIGNAL
        internal string? signal => (string?)mem[_Signal];
        internal BList<string> objects => (BList<string>?)mem[Objects]??BList<string>.Empty;
        internal BTree<Qlx, long?> setlist =>
            (BTree<Qlx, long?>?)mem[SetList] ?? BTree<Qlx, long?>.Empty;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public SignalStatement(long dp, string n, params string[] obs) 
            : base(dp, _Mem(obs) +(_Signal, n))
        { }
        protected SignalStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(string[] obs)
        {
            var r = BList<string>.Empty;
            foreach (var o in obs)
                r += o;
            return new BTree<long, object>(Objects, r);
        }
        public static SignalStatement operator +(SignalStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SignalStatement)et.New(m + x);
        }

        public static SignalStatement operator +(SignalStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == SetList)
                m += (_Depth, cx._DepthTXV((BTree<Qlx,long?>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SignalStatement)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SignalStatement(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new SignalStatement(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(setlist);
            if (ns != setlist)
                r += (SetList, ns);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p && (cx.obs[p]?.Calls(defpos, cx)??false))
                    return true;
            return base.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute a signal
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context _Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (cx.tr == null)
                return cx;
            if (stype == Qlx.RESIGNAL && !cx.tr.diagnostics.Contains(Qlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            if (signal != null && cx.db is not null)
            {
                string? sclass = signal[0..2];
                var dia = cx.tr.diagnostics;
                dia += (Qlx.RETURNED_SQLSTATE, new TChar(signal));
                for (var s = setlist.First(); s != null; s = s.Next())
                    if (s.value() is long p && cx.obs[p] is QlValue sv)
                        dia += (s.key(), sv.Eval(cx));
                cx.db += (Transaction.Diagnostics, dia);
                Handler? h = null;
                Activation? cs;
                for (cs = a; h == null && cs != null;)
                {
                    h = cs.exceptions[signal];
                    if (h == null && char.IsDigit(signal[0]))
                        h = cs.exceptions[sclass + "000"];
                    h ??= cs.exceptions["SQLEXCEPTION"];
                    if (h == null)
                    {
                        var c = cs.next;
                        while (c != null && c is not Activation)
                            c = c.next;
                        cs = c as Activation;
                    }
                }
                if (h == null || sclass == "25" || sclass == "40" || sclass == "2D") // no handler or uncatchable transaction errors
                {
                    for (; cs != null && a != cs; a = cx.GetActivation())
                        cx = a;
                    a.signal = new Signal(defpos, signal, objects);
                }
                else
                {
                    a.signal = null;
                    cx = h._Obey(cx);
                }
            }
            return cx;
        }
        /// <summary>
        /// Throw this signal
        /// </summary>
        /// <param name="cx">the context</param>
        public void Throw(Context cx)
        {
            var e = new DBException(signal ?? "", objects);
            for (var x = setlist.First(); x != null; x = x.Next())
                if (x.value() is long p)
                    e.info += (x.key(), cx.obs[p]?.Eval(cx) ?? TNull.Value);
            throw e;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SignalStatement)base._Replace(cx, so, sv);
            var sl = BTree<Qlx, long?>.Empty;
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    sl += (b.key(), cx.uids[p] ?? b.value());
            r+=(cx, SetList, sl);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (stype != Qlx.NO)
            { sb.Append(' '); sb.Append(stype); }
            sb.Append(' '); sb.Append(signal);
            var cs = " Set: ";
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cs); cs = ";";
                    sb.Append(b.key()); sb.Append('=');
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A signal for an activation
    /// </summary>
	internal class Signal
    {
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal long defpos;
        internal string signal;
        internal Qlx stype = Qlx.SIGNAL;
        internal BList<string> objects = BList<string>.Empty;
        internal Exception? exception;
        internal BTree<Qlx, long?> setlist = BTree<Qlx, long?>.Empty;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public Signal(long dp, string n,params object[] obs)
        {
            defpos = dp;
            signal = n;
            foreach (var o in obs)
                if (o.ToString() is string s)
                    objects += s;
        }
        public Signal(long dp, DBException e)
        {
            defpos = dp;
            signal = e.signal;
            exception = e;
            foreach (var o in e.objects)
                if (o.ToString() is string s)
                objects += s;
        }
        /// <summary>
        /// Execute a signal
        /// </summary>
        /// <param name="tr">the transaction</param>
        public Context Obey(Context cx)
        {
            if (cx.tr == null)
                return cx;
            var a = cx.GetActivation(); // from the top of the stack each time
            if (stype == Qlx.RESIGNAL && !cx.tr.diagnostics.Contains(Qlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            string sclass = signal[0..2];
            var dia = cx.tr.diagnostics;
            dia += (Qlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    if (b.value() is TypedValue v)
                        dia += (b.key(), v);
                dia += (Qlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
            }
            for (var s = setlist.First(); s != null; s = s.Next())
                if (s.value() is long p)
                    dia += (s.key(), cx.obs[p]?.Eval(cx)??TNull.Value);
            if (cx.db is not null)
                cx.db += (Transaction.Diagnostics, dia);
            Handler? h = null;
            Activation? cs;
            for (cs = a; h == null && cs != null;)
            {
                h = cs.exceptions[signal];
                if (h == null && char.IsDigit(signal[0]))
                    h = cs.exceptions[sclass + "000"];
                h ??= cs.exceptions["SQLEXCEPTION"];
                if (h == null)
                {
                    var c = cs.next;
                    while (c != null && c is not Activation)
                        c = c.next;
                    cs = c as Activation;
                }
            }
            if (h == null || sclass == "25" || sclass == "40" || sclass == "2D") // no handler or uncatchable transaction errors
            {
                for (; cs != null && a != cs; a = cx.GetActivation())
                    cx = a;
                a.signal = this;
            }
            else
            {
                a.signal = null;
                cx = h._Obey(cx);
            }
            return cx;
        }
        /// <summary>
        /// Throw this signal
        /// </summary>
        /// <param name="cx">the context</param>
        public void Throw(Context cx)
        {
            if (exception is not DBException e)
            {
                e = new DBException(signal, objects);
                for (var x = setlist.First(); x != null; x = x.Next())
                    if (x.value() is long p)
                    e.info += (x.key(), cx.obs[p]?.Eval(cx)??TNull.Value);
            }
            throw e;
        }
    }
    /// <summary>
    /// A GetDiagnostics statement for a routine
    /// 
    /// </summary>
    internal class GetDiagnostics : Executable
    {
        internal const long
            List = -141; // CTree<long,Qlx>
        internal CTree<long, Qlx> list =>
            (CTree<long, Qlx>?)mem[List] ?? CTree<long, Qlx>.Empty;
        internal GetDiagnostics(long dp) : base(dp, BTree<long, object>.Empty) { }
        protected GetDiagnostics(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GetDiagnostics operator +(GetDiagnostics et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (GetDiagnostics)et.New(m + x);
        }

        public static GetDiagnostics operator +(GetDiagnostics e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == List)
                m += (_Depth, cx._DepthTVX((CTree<long, Qlx>)o, d));
            else if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (GetDiagnostics)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GetDiagnostics(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new GetDiagnostics(dp, m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var nl = cx.Fix(list);
            if (nl != list)
                r += (List, nl);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = list.First(); b != null; b = b.Next())
                if (cx.obs[b.key()]?.Calls(defpos, cx)??false)
                    return true;
            return base.Calls(defpos, cx);
        }
        /// <summary>
        /// _Obey a GetDiagnostics statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context _Obey(Context cx)
        {
            cx.exec = this;
            for (var b = list.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue s && b.value() is Qlx t
                    && cx.tr is not null && cx.tr.diagnostics[t] is TypedValue v)
                cx.AddValue(s, v);
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (GetDiagnostics)base._Replace(cx, so, sv);
            var ls = CTree<long, Qlx>.Empty;
            for (var b = list.First(); b != null; b = b.Next())
                ls += (cx.uids[b.key()] ?? b.key(), b.value());
            r+=(cx, List, ls);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "  ";
            for (var b = list.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = "; ";
                sb.Append(b.value());
                sb.Append("->");
                sb.Append(Uid(b.key())); 
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Select statement: single row
    /// 
    /// </summary>
	internal class SelectSingle : Executable
    {
        /// <summary>
        /// The query
        /// </summary>
		public long sel => (long)(mem[ForSelectStatement.Sel] ?? -1L);
        /// <summary>
        /// The output tree
        /// </summary>
		public BList<long?> outs => (BList<long?>?)mem[FetchStatement.Outs] ?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a select statement: single row from the parser
        /// </summary>
        /// <param name="s">The select statement</param>
        /// <param name="sv">The tree of variables to receive the values</param>
		public SelectSingle(long dp) : base(dp, BTree<long, object>.Empty)
        { }
        protected SelectSingle(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectSingle operator +(SelectSingle et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SelectSingle)et.New(m + x);
        }

        public static SelectSingle operator +(SelectSingle e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == FetchStatement.Outs)
                m += (_Depth, cx._DepthBV((BList<long?>)o, d));
            else
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SelectSingle)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectSingle(defpos, m);
        }
        internal override DBObject New(long dp,BTree<long,object> m)
        {
            return new SelectSingle(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(sel);
            if (ns != sel)
                r += (ForSelectStatement.Sel, ns);
            var no = cx.FixLl(outs);
            if (no != outs)
                r += (FetchStatement.Outs, no);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(outs, defpos, cx);
        }
        /// <summary>
        /// Execute a select statement: single row
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context _Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (((RowSet?)cx.obs[sel])?.First(cx) is Cursor rb)
            {
                a.AddValue(this, rb);
                for (var b = outs.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue sv)
                        a.AddValue(sv, rb[b.key()]);
            }
            else
                a.NoData();
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectSingle)base._Replace(cx, so, sv);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r+=(cx, FetchStatement.Outs, no);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Sel=");sb.Append(Uid(sel));
            var cm = " Outs: ";
            for (var b = outs.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }

    }
    /// <summary>
    /// The Insert columns tree if provided is an ordered tree of some or all of the from's columns.
    /// This will match the row type of the data rowset. 
    /// If present the data rowset needs to have its columns reordered and expanded by nulls to
    /// match the from row set.
    /// shareable
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal const long
            InsCols = -241, // Domain
            Value = -156; // long RowSet
        internal long source => (long)(mem[RowSet._Source] ?? -1L);
        public long value => (long)(mem[Value] ?? -1L);
        public Domain insCols => (Domain)(mem[InsCols]??Domain.Row); // tablecolumns (should be specified)
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="fm">The rowset with target info</param>
        /// <param name="v">The uid of the data rowset</param>
        public SqlInsert(long dp, RowSet fm, long v, Domain iC)
           : base(dp, _Mem(fm,v,iC))
        { }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(RowSet fm,long v, Domain iC)
        {
            var r = BTree<long, object>.Empty + (RowSet._Source, fm.defpos)
                 + (Value, v);
            if (iC != null)
                r += (InsCols, iC);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlInsert(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlInsert(dp,m);
        }
        public static SqlInsert operator +(SqlInsert et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlInsert)et.New(m + x);
        }

        public static SqlInsert operator +(SqlInsert e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SqlInsert)e.New(m + (p, o));
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlInsert)base._Replace(cx, so, sv);
            var tg = cx.ObReplace(source, so, sv);
            if (tg != source)
                r+=(cx, RowSet._Source, tg);
            return r;
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var tg = cx.Fix(source);
            if (tg != source)
                r = cx.Add(r, RowSet._Source, tg);
            var nv = cx.Fix(value);
            if (nv != value)
                r = cx.Add(r, Value, nv);
            return r;
        }
        public override Context _Obey(Context cx)
        {
            cx.result = 0;
            if (cx.obs[source] is RowSet tg && cx.obs[value] is RowSet data)
            {
                var ts = BTree<long, TargetActivation>.Empty;
                for (var it = tg.rsTargets.First(); it != null; it = it.Next())
                    if (it.value() is long p && cx.obs[p] is RowSet tb)
                        ts += tb.Insert(cx, data, insCols);
                for (var ib = data.First(cx); ib != null; ib = ib.Next(cx))
                    for (var it = ts.First(); it != null; it = it.Next())
                        if (it.value() is TargetActivation ta)
                        {
                            ta.db = cx.db;
                            ta.cursors = cx.cursors;
                            ta.cursors += (ta._fm.defpos, ib);
                            ta.EachRow(ib._pos);
                            cx.db = ta.db;
                            if (ta is TableActivation at && cx.db.objects[at.table.defpos] is Table tt)
                                cx.obs+=(tt.defpos,tt);
                            ts += (it.key(), ta);
                        }
                for (var c = ts.First(); c != null; c = c.Next())
                    if (c.value() is TargetActivation ta)
                    {
                        ta.db = cx.db;
                        ta.Finish();
                        ts += (c.key(), ta);
                        cx.affected ??= Rvv.Empty;
                        if (ta.affected != null)
                            cx.affected += ta.affected;
                    }
            }
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");
            sb.Append(Uid(source));
            sb.Append(" Value: "); sb.Append(Uid(value));
            if (insCols.rowType!=BList<long?>.Empty)
            {
                sb.Append(" Columns: [");
                var cm = "";
                for (var b = insCols.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(']');
            }
            return sb.ToString();
        }
    }
    internal class DeleteNode : Executable
    {
        internal long what => (long)(mem[WhileStatement.What] ?? -1L);
        internal DeleteNode(long dp,QlValue v)
            : base(dp,new BTree<long,object>(WhileStatement.What,v.defpos)) 
        { }
        protected DeleteNode(long dp, BTree<long, object>? m = null) : base(dp, m)
        {
        }
        public override Context _Obey(Context cx)
        {
            var n = cx.obs[what]?.Eval(cx) as TNode??throw new DBException("22004");
            cx.Add(new Delete1(n.tableRow, cx.db.nextPos, cx));
            return cx;
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal long source => (long)(mem[RowSet._Source] ?? -1L);
        internal bool detach => mem.Contains(Index.IndexConstraint);
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        internal QuerySearch(long dp, RowSet f, CTree<UpdateAssignment, bool>? ua = null)
            : base(dp, _Mem(ua) + (RowSet._Source, f.defpos))
        { }
        protected QuerySearch(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(CTree<UpdateAssignment, bool>? ua)
        {
            var r = BTree<long, object>.Empty;
            if (ua is not null)
                r += (RowSet.Assig, ua);
            return r;
        }
        public static QuerySearch operator +(QuerySearch et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (QuerySearch)et.New(m + x);
        }

        public static QuerySearch operator +(QuerySearch e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (QuerySearch)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySearch(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new QuerySearch(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            r += (RowSet._Source, cx.Fix(source));
            return r;
        }
        public override Context _Obey(Context cx)
        {
            if (detach)
                cx.parse |= ExecuteStatus.Detach;
            if (cx.obs[source] is RowSet tg)
            {
                var ts = BTree<long, TargetActivation>.Empty;
                for (var it = tg.rsTargets.First(); it != null; it = it.Next())
                if (it.value() is long p && cx.obs[p] is RowSet tb)
                    ts += tb.Delete(cx, tg);
                for (var ib = tg.First(cx); ib != null; ib = ib.Next(cx))
                    for (var it = ts.First(); it != null; it = it.Next())
                        if (it.value() is TargetActivation ta)
                        {
                            ta.db = cx.db;
                            ta.cursors = cx.cursors;
                            ta.cursors += (ta._fm.defpos, ib);
                            ta.EachRow(ib._pos);
                            cx.db = ta.db;
                            ts += (it.key(), ta);
                        }
                for (var c = ts.First(); c != null; c = c.Next())
                    if (c.value() is TargetActivation ta)
                    {
                        ta.db = cx.db;
                        ta.Finish();
                        ts += (c.key(), ta);
                        cx.affected ??= Rvv.Empty;
                        if (ta.affected is not null)
                            cx.affected += ta.affected;
                    }
            }
            return cx;
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: "); sb.Append(Uid(source));
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement a searched UPDATE statement as a kind of QuerySearch
    /// (QuerySearch itself is for Delete)
    /// </summary>
    internal class UpdateSearch : QuerySearch
    {
        /// <summary>
        /// Constructor: A searched UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        public UpdateSearch(long dp, RowSet f)
            : base(dp, f)
        { }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator +(UpdateSearch et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (UpdateSearch)et.New(m + x);
        }

        public static UpdateSearch operator +(UpdateSearch e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (UpdateSearch)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateSearch(defpos, m);
        }
        public override Context _Obey(Context cx)
        {
            cx.result = 0;
            if (cx.obs[source] is RowSet tg)
            {
                var ts = BTree<long, TargetActivation>.Empty;
                for (var it = tg.rsTargets.First(); it != null; it = it.Next())
                    if (it.value() is long p && cx.obs[p] is RowSet tb)
                        ts += tb.Update(cx, tg);
                for (var ib = tg.First(cx); ib != null; ib = ib.Next(cx))
                    for (var it = ts.First(); it != null; it = it.Next())
                        if (it.value() is TargetActivation ta)
                        {
                            ta.db = cx.db;
                            ta.cursors = cx.cursors;
                            ta.cursors += (ta._fm.defpos, ib);
                            ta.values += ib.values;
                            ta.EachRow(ib._pos);
                            cx.db = ta.db;
                            ts += (it.key(), ta);
                        }
                for (var c = ts.First(); c != null; c = c.Next())
                    if (c.value() is TargetActivation ta)
                    {
                        ta.db = cx.db;
                        ta.Finish();
                        ts += (c.key(), ta);
                        cx.affected ??= Rvv.Empty;
                        if (ta.affected != null)
                            cx.affected += ta.affected;
                    }
            }
            return cx;
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new UpdateSearch(dp, m);
        }
    }
    internal class GraphInsertStatement : Executable
    {
        internal const long
            AtSchemaLevel = -333, // bool
            GraphExps = -307; // CList<CList<GqlNode>> GqlNode (alternately with SqlEdges)
        // In MatchStatement we can also have SqlNodes that are SqlPaths 
        internal CList<CList<GqlNode>> graphExps =>
            (CList<CList<GqlNode>>)(mem[GraphExps] ?? CList<CList<GqlNode>>.Empty);
        internal BList<long?> stms => 
            (BList<long?>?)mem[IfThenElse.Then] ?? BList<long?>.Empty;
        internal bool atSchemaLevel => (bool)(mem[AtSchemaLevel] ?? false);
        public GraphInsertStatement(long dp, bool sch, CList<CList<GqlNode>> ge, BList<long?> th)
            : base(dp, new BTree<long, object>(GraphExps, ge) + (IfThenElse.Then, th) +(AtSchemaLevel,sch))
        { }
        public GraphInsertStatement(long dp, BTree<long, object>? m = null) : base(dp, m)
        { }
        public static GraphInsertStatement operator +(GraphInsertStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (GraphInsertStatement)et.New(m + x);
        }

        public static GraphInsertStatement operator +(GraphInsertStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            else
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (GraphInsertStatement)e.New(m + (p, o));
        }

        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GraphInsertStatement(dp,m);
        }
        /// <summary>
        /// GraphInsertStatement contains a CTree of GqlNode in lexical sequence.
        /// In _Obey() we ensure that ac.values has a corresponding sequence of TNode.
        /// We create whatever nodee, edges, nodetypes, edgetypes are needed.
        /// </summary>
        /// <param name="cx">the context</param>
        /// <returns>the same context</returns>
        /// <exception cref="DBException"></exception>
        public override Context _Obey(Context cx)
        {
            for (var b = graphExps.First(); b != null; b = b.Next())
                if (b.value() is CList<GqlNode> ge)
                {
                    // Do the nodes first and then the edges
                    for (var gb = ge.First(); gb != null; gb = gb.Next())
                        if (gb.value() is GqlNode nd && nd is not GqlEdge && nd is not GqlReference
                            && cx.db.objects[nd.domain.defpos] is NodeType nt)
                            nd.Create(cx, nt, defpos);
                    TNode? bn = null;
                    GqlEdge? ed = null;
                    for (var gb = ge.First(); gb != null; gb = gb.Next())
                        if (gb.value() is GqlNode g)
                        {
                            if (g is GqlEdge edge)
                                ed = edge;
                            else if (ed is not null && bn is not null && g.Eval(cx) is TNode nn)
                            {
                                var el = cx.obs[ed.label.defpos] as Domain ?? ed.label;
                                var nm = (el.kind == Qlx.EDGETYPE) ?
                                    (el is GqlLabel) ? (el.name ?? el.domain.name) : el.ToString() : "";
                                var ln = (ed.tok == Qlx.ARROWBASE) ? bn : nn;
                                var an = (ed.tok == Qlx.ARROWBASE) ? nn : bn;
                                var pn = CTree<string, bool>.Empty;
                                for (var pb = ed.docValue.First(); pb != null; pb = pb.Next())
                                    pn += (pb.key(), true);
                                EdgeType? et = null;
                                if (nm != "" && cx.db.objects[cx.role.edgeTypes[nm] ?? -1L] is Domain ew)
                                {
                                    if (ew is EdgeType ev)
                                        et = ev;
                                    else if (ew.kind == Qlx.UNION)
                                    {
                                        for (var c = ew.unionOf.First(); c != null; c = c.Next())
                                            if (cx.obs[c.key().defpos] is EdgeType ex 
                                                && ln.dataType.defpos==ex.leavingType && an.dataType.defpos==ex.arrivingType)
                                            {
                                                et = ex;
                                                goto found;
                                            }
                                        throw new DBException("22G0W", nm);
                                    }
                                    else throw new PEException("PE20902");
                                    found:;
                                }
                                if (nm == "" && cx.db.objects[cx.role.unlabelledEdgeTypesInfo[pn] ?? -1L] is EdgeType eu)
                                    et = eu;
                                et ??= (EdgeType)ed._NodeType(cx, Domain.EdgeType);
                                ed.Create(cx, et, defpos);
                            }
                            else
                                bn = g.Eval(cx) as TNode;
                            if (atSchemaLevel)
                                ed?.InsertSchema(cx);
                        }
                }
            return cx;
        }
        internal CTree<long,bool> GraphTypes(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = graphExps.First(); b != null; b = b.Next())
                if (b.value() is CList<GqlNode> nl)
                    for (var c = nl.First(); c != null; c = c.Next())
                        if (c.value() is GqlNode sn)
                            r += (sn.domain.defpos, true);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (atSchemaLevel)
                sb.Append(" SCHEMA");
            string cm;
            sb.Append('['); sb.Append(graphExps);sb.Append(']');
            if (stms!=BList<long?>.Empty)
            {
                cm = " THEN [";
                for (var b = stms.First(); b != null; b = b.Next())
                    if (b.value() is long p) {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                if (cm == ",") sb.Append(']');
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The lhs of each assignment should be an unbound identifier
    /// </summary>
    /// <param name="dp"></param>
    /// <param name="m"></param>
    class LetStatement(long dp, BTree<long, object> m) : Executable(dp, m)
    {
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>)(mem[RowSet.Assig] ?? CTree<UpdateAssignment, bool>.Empty);
        internal long stm => (long)(mem[Procedure.Body] ?? -1L);
        public static LetStatement operator+(LetStatement ls,(long,object)x)
        {
            return new LetStatement(ls.defpos, ls.mem + x);
        }
        public override Context _Obey(Context cx)
        {
            var vs = CTree<long, TypedValue>.Empty;
            var ls = BList<DBObject>.Empty;
            for (var c = assig.First(); c != null; c = c.Next())
            {
                var ua = c.key();
                if (cx.obs[ua.val] is QlValue sv)
                {
                    ls += sv;
                    vs += (ua.vbl, sv.Eval(cx));
                }
                cx.binding += vs;
            }
            var dm = new Domain(Qlx.TABLE, cx, ls);
            cx.result = BindingRowSet.Get(cx, new TRow(dm, vs)).defpos;
            if (cx._Ob(stm) is Executable st)
                cx = st.Obey(cx);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(domain);
            var cm = "[";
            for (var b=assig.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(Uid(b.key().vbl));sb.Append('=');
                sb.Append(Uid(b.key().val));
            }
            sb.Append(']');
            return sb.ToString();
        }
    }
    class OrderAndPageStatement(long dp, BTree<long, object> m) : Executable(dp, m)
    {
        public static OrderAndPageStatement operator +(OrderAndPageStatement ls, (long, object) x)
        {
            return new OrderAndPageStatement(ls.defpos, ls.mem + x);
        }
        public override Context _Obey(Context cx)
        {
            var bt = cx.obs[cx.result] as ExplicitRowSet
                ?? new ExplicitRowSet(cx.GetUid(), cx, Domain.Row, new BList<(long, TRow)>((cx.GetUid(), TRow.Empty)));
            var nr = BList<(long,TRow)>.Empty;
            var od = (mem[RowSet.RowOrder] as Domain)?? Domain.Row;
            var rs = bt.Sort(cx, od, false);
            var ff = (int)(mem[RowSetSection.Offset] ?? 0);
            var lm = (int)(mem[RowSetSection.Size] ?? 0);
            if (ff!=0 || lm!=0)
                rs = new RowSetSection(cx, rs,ff,lm);
            for (var b = rs.First(cx); b != null; b = b.Next(cx))
                nr += (b._pos, b);
            var nb = new ExplicitRowSet(bt.defpos, cx, bt, nr) + (TableRowSet._Index, bt.index);
            cx.Add(nb);
            cx.result = nb.defpos;
            return cx;
        }
    }
    class FilterStatement(long dp, BTree<long, object> m) : Executable(dp, m)
    {
        public static FilterStatement operator +(FilterStatement ls, (long, object) x)
        {
            return new FilterStatement(ls.defpos, ls.mem + x);
        }
        public override Context _Obey(Context cx)
        {
            var bt = cx.obs[cx.result] as ExplicitRowSet
                ?? new ExplicitRowSet(cx.GetUid(), cx, Domain.Row, new BList<(long, TRow)>((cx.GetUid(), TRow.Empty)));
            var nr = BList<(long, TRow)>.Empty;
            if (mem[RowSet._Where] is CTree<long, bool> wh)
            {
                bt += (RowSet._Where, wh);
                cx.Add(bt);
            }
            for (var b = bt.First(cx); b != null; b = b.Next(cx))
            {
                if (!b.Matches(cx))
                    break;
                nr += (b._pos, b);
            }
            var nb = new ExplicitRowSet(bt.defpos, cx, bt, nr) + (TableRowSet._Index, bt.index);
            cx.Add(nb);
            cx.result = nb.defpos;
            return cx;
        }
    }
    /// <summary>
    /// The Match syntax consists of a graph expression, an optional where condition and an optional action part.
    /// Parsing of the graph expression results in a collection of previously unbound identifiers(GDefs) 
    /// and a collection of constraints(boolean SqlValueExpr). By default, the resulting domain is an ExplicitRowSet
    /// whose columns match the bindings CList<TGParam>(an ordering of GDefs). Whether or not it is the final result,
    /// this bindingtable is used during the match process to eleminate duplicate rows in the result.
    /// If a return (or Yield) statement is present, a rowset built from its rows becomes the Match result: 
    /// a SelectRowSet, built directly by the MatchStatement. Such a RETURN statement is just one 
    /// of the statement types that can be dependent on the Match: it is very common for the dependent statement 
    /// to be CREATE. Any dependent statement(s) are executed by AddRow in the EndStep of the match process, 
    /// using the binding values it has found.
    /// Like the CREATE syntax, a graph expression consists of a tree of node-edge-node chains. A path pattern 
    /// also has this form but has the effect of an edge, since the starting and ending nodes are merged with the
    /// adjacent nodes of the enclosing expression. If either part of this structural description is missing
    /// in the given match expression, blank nodes are inserted so that the MatchStatement obeys these rules.
    /// The graph expression cannot contain subtype references or SqlExpressions and the match process
    /// does not construct any nodes: all identifiers are either unbound or constant identfiers 
    /// or values that are used for matching subgraphs.
    /// During the steps of the evaluation, unbound identiers are progressively bound to particular values, and their
    /// binding values become additional constraints. 
    /// Evaluation of the match statement traverses all of the nodes/edges in the database: each full traversal
    /// building a row of the bindingtable. The set of nodes and edges matched in that row is a graph
    /// and in this way a match statement defines a graph (the union of the graphs of its rows).
    /// If there is a dependent statement, it is obeyed for each row of the binding table.
    /// During pattern matching, the pattern is traversed: (a) on a node expression the database is examined for nodes
    /// that match it, and where it satisfies all constraints on that node, the bindings are noted.
    /// (b) For each matching node, if the next match expression is an edge, the database is examined for suitable edges, and so on.
    /// In this way the process has (a) ExpSteps and (b) NodeSteps: there are also 
    /// (c) PathSteps for the start of the next repetition of a path pattern, 
    /// (d) GraphSteps for the next graph pattern in the MatchStatement, and
    /// (e) an EndStep to record the resulting row in the bindingtable, and execute the dependent statement.
    /// Each step apart from the end step has a next step, which is obeyed on success.
    /// On completion of the match statement, or on failure at any point, we unbind the last binding made
    /// and take the next choice from the previous step if any (backtracking).
    /// </summary>
    internal class MatchStatement : Executable
    {
        internal const long
            BindingTable = -490, // long ExplicitRowSet
            GDefs = -210,   // CTree<long,TGParam>
            MatchFlags = -496, // Bindings=1,Body=2,Return=4,Schema=8
            MatchList = -491, // BList<long?> GqlMatchAlt
            Truncating = -492; // BTree<long,(long,long)> EdgeType (or 0L), lm QlValue, ord QlValue
        internal CTree<long, TGParam> gDefs =>
            (CTree<long, TGParam>)(mem[GDefs] ?? CTree<long, TGParam>.Empty);
        internal BList<long?> matchList =>
            (BList<long?>)(mem[MatchList] ?? BList<long?>.Empty);
        internal CTree<long, bool> where =>
            (CTree<long, bool>)(mem[RowSet._Where] ?? CTree<long, bool>.Empty);
        internal long body => (long)(mem[Procedure.Body] ?? -1L);
        internal BList<long?> then => (BList<long?>)(mem[IfThenElse.Then] ?? BList<long?>.Empty);
        internal BTree<long, (long, long)> truncating =>
            (BTree<long, (long, long)>)(mem[Truncating] ?? BTree<long, (long, long)>.Empty);
        internal int size => (int?)mem[RowSetSection.Size] ?? -1;
        internal long bindings => (long)(mem[BindingTable] ?? -1L);
        [Flags]
        internal enum Flags { None = 0, Bindings = 1, Body = 2, Return = 4, Schema = 8 }
        internal Flags flags => (Flags)(mem[MatchFlags] ?? Flags.None);
        /// <summary>
        /// This private field is modified only at the start of _Obey
        /// </summary>
        BTree<long,(int,Domain)> truncator = BTree<long,(int,Domain)>.Empty;
        public MatchStatement(Context cx, BTree<long, (long, long)>? tg, CTree<long, TGParam> gs, BList<long?> ge,
            BTree<long, object> m, long st, long re)
            : this(cx.GetUid(), cx, tg, gs, ge, m, st, re)
        {
            cx.Add(this);
        }
        MatchStatement(long dp, Context cx, BTree<long, (long, long)>? tg, CTree<long, TGParam> gs, BList<long?> ge,
    BTree<long, object> m, long st, long re)
            : base(dp, _Mem(dp, cx, m, tg, gs, ge, st, re) + (Procedure.Body, st))
        {
            cx.Add(this);
        }
        public MatchStatement(long dp, BTree<long, object>? m = null) : base(dp, m)
        { }
        static BTree<long, object> _Mem(long dp, Context cx, BTree<long, object> m, BTree<long, (long, long)>? tg,
            CTree<long, TGParam> gs, BList<long?> ge, long st, long re)
        {
            var ch = false;
            for (var b = gs.First(); b != null; b = b.Next())
            {
                cx.undefined -= b.key();
                ch = true;
            }
            m += (GDefs, gs);
            if (tg is not null)
                m += (Truncating, tg);
            var f = (Flags)(m[MatchFlags] ?? Flags.None);
            for (var b = gs.First(); b != null && f == Flags.None; b = b.Next())
                if (b.value().value != "")
                    f |= Flags.Bindings;
            if (cx.obs[st] is Executable e)
            {
                f |= Flags.Body;
                if (e.domain.rowType != BList<long?>.Empty)
                    f |= Flags.Return;
                m += (Procedure.Body, st);
            }
            m += (MatchFlags, f);
            m += (ReturnStatement.Ret, re);
            m += (MatchList, ge);
            for (var b = ge.First(); b != null; b = b.Next())
                if (cx.obs[b.value() ?? -1L] is GqlMatch sm)
                    for (var c = sm.matchAlts.First(); c != null; c = c.Next())
                        if (cx.obs[c.value() ?? -1L] is GqlMatchAlt sa && gs[sa.pathId] is TGParam p
                                && cx.obs[p.uid] is QlValue sv)
                            sv.AddFrom(cx, sa.defpos);
            if (ch)
                cx.NowTry();
            return m;
        }
        public static MatchStatement operator +(MatchStatement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (MatchStatement)et.New(m + x);
        }

        public static MatchStatement operator +(MatchStatement e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (MatchStatement)e.New(m + (p, o));
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new MatchStatement(dp, m);
        }
        public bool Done(Context cx)
        {
            return size >= 0 && cx.obs[bindings] is BindingRowSet se && se.rows.Count >= size;
        }
        /// <summary>
        /// We traverse the given match graphs in the order given, matching with possible database nodes as we move.
        /// The match graphs and the set of TGParams are in this MatchStatement.
        /// The database graph is in ac.db.graphs and ac.db.nodeids.
        /// The handling of a binding variable x defined within a path pattern is quite tricky: 
        /// during the path matching process x stands for a simple value 
        /// (a single node, or an expression evalated from its properties),
        /// But, as a field of the path value p, p.x is an array of the values constructed so far.
        /// In result rowset, RETURN, WHERE, or the body statement of the MATCH, x will be an array
        /// for the current binding set.
        /// Accordingly, the MatchStatement defines such x as array types, except within the body of DbNode.
        /// At the end of a pattern, in PathStep, we change the binding to contain the array values instead,
        /// getting the values for earlier pattern matching from ac.paths.
        /// This means if ps is a PathStep, the datatype of the binding associated with x will change in ps.Next
        /// from (T) to (T array) for all non-path x named in xn.state for some xn in ps.sp.pattern.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <returns></returns>
        public override Context _Obey(Context cx)
        {
            for (var b = cx.undefined.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlReview sr)
                {
                    var ns = cx.names;
                    DBObject v = sr;
                    string? nm = null;
                    long? t = null;
                    for (var c = sr.chain?.First(); c != null; c = c.Next())
                        if (c.value() is Ident id && ns[id.ident] is long cp
                            && cx._Ob(cp) is DBObject nv)
                        {
                            t ??= cp;
                            v = nv;
                            nm = id.ident;
                            ns = nv.domain.infos[cx.role.defpos]?.names ?? Names.Empty;
                        }
                    var tg = t ?? -1L;
                    if (v is TableColumn tc)
                        v = new SqlField(sr.defpos, nm??"", tc.seq, tg, tc.domain,tg);
                    if (sr.alias != null)
                        v += (_Alias, sr.alias);
                    cx.undefined -= sr.defpos;
                    cx.Replace(sr, v);
                } 
            // Graph expression and Database agree on the set of NodeType and EdgeTypes
            // Traverse the given graphs, binding as we go
            var ac
                = new Activation(cx, "Match")
                {
                    binding = cx.binding,
                    result = domain.defpos
                };

            // Parse any truncating expressions
            for (var b = truncating.First(); b != null; b = b.Next())
            {
                var (i, d) = b.value();
                if ((ac.obs[i] as QlValue)?.Eval(cx) is not TInt lm || (ac.obs[d] as QlValue)?.Eval(cx) is not TChar qs) throw new PEException("PE60801");
                var il = lm.ToInt() ?? throw new PEException("PE60802");
                var pd = new Parser(cx, qs.ToString());
                if (cx.db.objects[b.key()] is NodeType nt)
                    pd.cx.AddDefs(nt);
                var rt = BList<DBObject>.Empty;
                while (pd.tok == Qlx.Id)
                {
                    rt += ac._Ob(pd.ParseOrderItem(false)) ?? SqlNull.Value;
                    if (pd.tok == Qlx.COMMA)
                        pd.Next();
                    else
                        break;
                }
                var od = (rt.Length == 0) ? Domain.Content : new Domain(ac.GetUid(), ac, Qlx.ROW, rt, rt.Length);
                truncator += (b.key(), (il, od));
            }
            var pre = ((Transaction)ac.db).physicals.Count;
            _step = 0;
            var gf = matchList.First();
            if (ac.obs[gf?.value() ?? -1L] is GqlMatch sm)
                for (var b = sm.matchAlts.First(); b != null; b = b.Next())
                    if (ac.obs[b.value() ?? -1L] is GqlMatchAlt sa)
                    {
                        ac.paths += (sa.defpos, new TPath(sa.defpos, ac));
                        var xf = sa.matchExps.First();
                        if (gf is not null && xf is not null)
                            ExpNode(ac, new ExpStep(sa, xf, new GraphStep(gf.Next(), new EndStep(this))), Qlx.Null, null, null);
                    }
            var ps = ((Transaction)ac.db).physicals;
            var changes = false;
            // Alas, MatchStatement can have side effects such as creating ghostly NodeTypes for 
            // BindingTable entries. For now we simply take them out and pretend they aren't there
            for (var b = ps.PositionAt(pre); b != null; b = b.Next())
                if (b.value() is PNodeType ph && names.Contains(ph.name))
                    ac.db += (Transaction.Physicals, ps - b.key());
                else
                    changes = true;
            if (!changes)
            {
                cx.result = bindings;
                if (ac.obs[bindings] is RowSet brs)
                    cx.Add(brs);
            }
            else
                cx.result = -1L;
            if (ac.obs[body] is SelectStatement ss && ac.obs[ss.union] is RowSet su)
            {
                ac.result = ss.union;
                ac.Add(su);
            }
            if (ac.obs[ac.result] is RowSet rs)
            {
                if (mem[RowSet.RowOrder] is Domain ord)
                {
                    cx.Add(rs);
                    rs = rs.Sort(cx, ord, false);
                }
                if (mem[RowSetSection.Size] is int ct)
                {
                    cx.Add(rs);
                    rs = new RowSetSection(cx, rs, 0, ct);
                }
                ac.result = rs.defpos;
                ac.obs += (ac.result, rs);
            } 
            else if (gDefs == CTree<long, TGParam>.Empty)
                cx.result = TrueRowSet.OK(cx).defpos;
            else if (cx.obs[cx.result] is RowSet rrs)
                cx.obs += (cx.result, rrs); 
            if (then != BList<long?>.Empty)
            {
                var ta = new Activation(ac, "" + defpos)
                {
                    result = cx.result
                };
                ObeyList(then, ta);
                ta.SlideDown();
                ac.values += ta.values;
                ac.db = ta.db;
            }
            ac.SlideDown();
            cx.db = ac.db;
            if (ac.obs[bindings] is RowSet bs)
                cx.obs += (bindings, bs);
            var aff = ac.db.AffCount(ac);
            if (aff > 0)
                cx.result = -1L;            
            return cx;
        }
        /// <summary>
        /// The Match implementation uses continuations as in Scheme or Haskell.
        /// Step is a linked list of things to do.
        /// At any step in the match, one of the matching methods
        /// ExpNode, DbNode or PathNode
        /// is called, with a continuation parameter (a Step) specifying what to do if the match succeeds.
        /// Step.Next says what to do in that case: and may call the next matching method if any,
        /// AddRow, or if neither of these is appropriate, will do nothing.
        /// Of course, when calling the next matching method, we set up its Step.
        /// As a result, on each success the stack grows, and corresponds to the trail.
        /// </summary>
        internal abstract class Step(MatchStatement m)
        {
            public MatchStatement ms = m; // all continuations know the MatchStatement

            /// <summary>
            /// The parameters are (maybe) a Step, and the current end state of the match 
            /// </summary>
            /// <param name="cx">The context</param>
            /// <param name="cn">A continuation if provided</param>
            /// <param name="tok">A token indicating the match state</param>
            /// <param name="pd">The current last matched node if any</param>
            public abstract void Next(Context cx, Step? cn, Qlx tok, GqlNode? px, TNode? pd);
            public virtual Step? Cont => null;
            protected static string Show(ABookmark<int, long?>? b)
            {
                var sb = new StringBuilder();
                var cm = '[';
                for (; b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ',';
                    sb.Append(Uid(b.value() ?? -1L));
                }
                if (cm == '[') sb.Append(cm); sb.Append(']');
                return sb.ToString();
            }
        }
        /// <summary>
        /// EndStep is the final entry in any continuation and includes computation of the (source row of) RETURN.
        /// In a complex situation there may be several possibly alternative RETURNS, 
        /// and all such must be placed in the binding table.
        /// </summary>
        internal class EndStep(MatchStatement m) : Step(m)
        {
            /// <summary>
            /// On success we simply call AddRow, which does the work!
            /// </summary>
            public override void Next(Context cx, Step? cn, Qlx tok, GqlNode? px, TNode? pd)
            {
                for (var b = ms.where.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValueExpr se && se.Eval(cx) != TBool.True)
                        return;
                ms.AddRow(cx);
            }

            public override string ToString()
            {
                return "End[" + Uid(ms.defpos) + "]";
            }
        }
        /// <summary>
        /// GraphStep contains the remaining alternative matchexpressions in the MatchStatement.
        /// A GraphStep precedes the final EndStep, but there may be others?
        /// </summary>
        internal class GraphStep(ABookmark<int, long?>? graphs, Step n) : Step(n.ms)
        {
            internal readonly ABookmark<int, long?>? matchAlts = graphs; // the current place in the MatchStatement
            readonly Step next = n; // the continuation

            /// <summary>
            /// On Success we go on to the next matchexpression if any.
            /// Otherwise we have succeeded and call next.Next.
            /// </summary>
            public override void Next(Context cx, Step? cn, Qlx tok, GqlNode? px, TNode? pd)
            {
                if ((!ms.Done(cx)) && cx.obs[matchAlts?.value() ?? -1L] is GqlMatch sm)
                {
                    for (var b = sm.matchAlts.First(); b != null; b = b.Next())
                        if (cx.obs[b.value() ?? -1L] is GqlMatchAlt sa)
                        {
                            cx.paths += (sa.defpos, new TPath(sa.defpos, cx));
                            cx.binding -= sa.pathId;
                            ms.ExpNode(cx, new ExpStep(sa, sa.matchExps.First(),
                                new GraphStep(matchAlts?.Next(), next)), Qlx.Null, null, null);
                        }
                }
                else 
                    next.Next(cx, cn, tok, px, pd);
            }
            public override Step? Cont => next;
            public override string ToString()
            {
                var sb = new StringBuilder("Graph");
                sb.Append(Show(matchAlts));
                sb.Append(','); sb.Append(next.ToString());
                return sb.ToString();
            }
        }
        /// <summary>
        /// This is the most common matching method, matches.value() if not null should be a
        /// GqlNode, GqlEdge, or GqlPath 
        /// (if ExpNode finds it is a GqlPath it sets up PathNode to follow it).
        /// </summary>
        internal class ExpStep(GqlMatchAlt sa, ABookmark<int, long?>? m, MatchStatement.Step n) : Step(n.ms)
        {
            public ABookmark<int, long?>? matches = m; // the current place in the matchExp
            public GqlMatchAlt alt = sa; // the current match alternative
            public Step next = n; // the continuation

            /// <summary>
            /// On Success we go on to the next element in the match expression if any.
            /// Otherwise the expression has succeeded and we call next.Next.
            /// </summary>
            public override void Next(Context cx, Step? cn, Qlx tok, GqlNode? pg, TNode? pd)
            {
                if ((!ms.Done(cx)) && matches != null)
                    ms.ExpNode(cx, new ExpStep(alt, matches, cn ?? next), tok, pg, pd);
                else
                    next.Next(cx, null, Qlx.WITH, pg, pd);
            }
            public override Step? Cont => next;
            public override string ToString()
            {
                var sb = new StringBuilder("Exp");
                sb.Append(Show(matches));
                sb.Append(','); sb.Append(next.ToString());
                return sb.ToString();
            }
        }
        /// <summary>
        /// PathStep contains details of a path pattern, and may insert a new copy
        /// of itself in the continuation if a further repeat is possible.
        /// </summary>
        internal class PathStep(GqlMatchAlt sa, GqlPath s, int i, GqlNode? pg, MatchStatement.Step n) : Step(n.ms)
        {
            public GqlMatchAlt alt = sa; // the current match alternative
            public GqlPath sp = s; // the repeating pattern spec
            public CTree<long, TGParam> state = CTree<long, TGParam>.Empty;
            public int im = i; // the iteration count
            public Step next = n; // the continuation
            public GqlNode? xn = pg;
            /// <summary>
            /// The quantifier specifies minimum and maximum for the iteration count.
            /// Depending on this value we may recommence the pattern.
            /// When this is done (backtracking) we announce success of the repeating pattern
            /// and call next.Next()
            /// </summary>
            public override void Next(Context cx, Step? cn, Qlx tok, GqlNode? px, TNode? pd)
            {
                if (ms.Done(cx))
                {
                    next.Next(cx, cn, tok, px, pd);
                    return;
                }
                var i = im + 1;
                // this is where we need to promote the local bindings to arrays
                var ls = CTree<long, TGParam>.Empty;
                for (var b = sp.pattern.First(); b != null; b = b.Next())
                    if (cx.obs[b.value() ?? -1L] is GqlNode n)
                        for (var c = n.state.First(); c != null; c = c.Next())
                            if (!c.value().type.HasFlag(TGParam.Type.Path))
                                ls += (c.value().uid, c.value());
                if (cx.paths[alt.defpos] is TPath pa)
                {
                    for (var b = ls.First(); b != null; b = b.Next())
                        if (cx.binding[b.key()] is TArray ta && ta[im] is TypedValue tv)
                            pa += (cx, tv, b.key());
                    cx.paths += (alt.defpos, pa);
                    cx.binding += (alt.pathId, pa[0L]);
                }
                // now see if we need to repeat the match process for this path pattern
                if ((i < sp.quantifier.Item2 || sp.quantifier.Item2 < 0) && pd is not null)
                    ms.PathNode(cx, new PathStep(alt, sp, i, px, next), pd);
                if (i >= sp.quantifier.Item1 && pd is not null)
                    next.Next(cx, cn, tok, px, pd);
            }
            public override Step? Cont => next;
            public override string ToString()
            {
                var sb = new StringBuilder((alt.mode == Qlx.NONE) ? "Path" : alt.mode.ToString());
                sb.Append(im);
                sb.Append(Show(sp?.pattern.First()));
                sb.Append(','); sb.Append(next.ToString());
                return sb.ToString();
            }
        }
        /// <summary>
        /// NodeStep receives a list of possible database nodes from ExpNode, and
        /// checks these one by one.
        /// </summary>
        internal class NodeStep(GqlMatchAlt sa, GqlNode x, ABookmark<long, TableRow>? no,Step n) 
            : Step(n.ms)
        {
            public GqlMatchAlt alt = sa;
            public ABookmark<long, TableRow>? nodes = no;
            public GqlNode xn = x;
            public Step next = n;

            /*           public NodeStep(NodeStep s, ABookmark<long, TableRow>? ns) : base(s.ms)
                       {
                           alt = s.alt; nodes = ns; xn = s.xn; next = s.next;
                       } */
            /// <summary>
            /// On each success we call the continuation
            /// </summary>
            public override void Next(Context cx, Step? cn, Qlx tok, GqlNode? pg, TNode? pd)
            {
                ms.ExpNode(cx, (ExpStep)next, tok, pg, pd);
            }
            public override Step? Cont => next;
            public override string ToString()
            {
                var sb = new StringBuilder("Node");
                sb.Append(Uid(xn.defpos)); sb.Append(':');
                var cm = '[';
                for (var b = nodes; b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ',';
                    sb.Append(Uid(b.value().defpos));
                }
                if (cm == '[') sb.Append(cm); sb.Append(']');
                sb.Append(','); sb.Append(next.ToString());
                return sb.ToString();
            }
        }
        /// <summary>
        /// A the end of the matchstatement, all bindings have been achieved, and we
        /// add a row to the MatchStatement's rowSet. If there are executable statements
        /// appended to the Match, we execute them for the current row of the binding table.
        /// </summary>
        /// <param name="cx">The context</param>
        void AddRow(Context cx)
        {
            // everything has been checked
            if (flags == Flags.None)
                cx.val = TBool.True;
            if ((flags.HasFlag(Flags.Bindings) || flags.HasFlag(Flags.Schema))
                && cx.obs[bindings] is BindingRowSet ers)
            {
                var uid = cx.GetUid();
                var rr = new TRow(ers, cx.binding);
                ers += (cx, rr);
                cx.Add(ers);
                cx.val = TNull.Value;
                cx.values += cx.binding;
                var ro = cx.result;
                if (cx.obs[body] is Executable bd && cx.binding != CTree<long, TypedValue>.Empty)
                {
                    cx = bd._Obey(cx);
                    if (bd is SelectStatement ss)
                        ro = ss.union;
                }
                if (flags == Flags.None)
                    cx.val = TBool.True;
                cx.result = ro;
            }
        }
        static int _step = 0; // debugging assistant
        /// <summary>
        /// We work through the given graph expression in the order given. 
        /// For each expression node xn, there is at most one next node nx to move to. 
        /// In ExpNode we will compute a set ds of database nodes that can correspond with xn.
        /// If xn is not a Path, then we calculate ds and call DbNode for the first of ds.
        /// If xn is a Path, we push it on the path stack with a count of 0, and use the previous ds.
        /// There is a set gDefs(xn) of TGParams defined at xn, which can be referenced later.
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <param name="be">The Step: current state</param>
        /// <param name="tok">A direction token if an edge</param>
        /// <param name="pd">The previous database node if any</param>
        void ExpNode(Context cx, ExpStep be, Qlx tok, GqlNode? gp, TNode? pd)
        {
            if (cx.obs[be.matches?.value() ?? -1L] is not GqlNode xn)
            {
                be.next.Next(cx, null, tok, gp, pd);
                return;
            }
            cx.conn.Awake();
            var step = ++_step;
            if (xn is GqlPath sp && sp.pattern.First() is ABookmark<int, long?> ma && pd is not null)
            {
                // additions to the path binding happen in PathStep.Next()
                // here we just ensure that the required arrays have been initialised
                var pa = cx.paths[be.alt.defpos] ?? new TPath(be.alt.defpos, cx);
                pa = PathInit(cx, pa, ma, 0);
                cx.paths += (pa.matchAlt, pa);
                cx.binding -= be.alt.pathId;
                PathNode(cx, new PathStep(be.alt, sp, 0, gp,
                    new ExpStep(be.alt, be.matches?.Next(), be.next)), pd);
            }
            var ds = BTree<long, TableRow>.Empty; // the set of database nodes that can match with xn
            // We have a current node xn, but no current dn yet. Initialise the set of possible d to empty. 
            if (tok == Qlx.WITH && pd is not null)
                ds += (xn.defpos, pd.tableRow);
            //    else if (xn.Eval(ac) is TNode nn)
            //        ds += (xn.defpos, nn.tableRow);
            else if (pd is TEdge && cx.db.joinedNodes[pd.defpos] is CTree<Domain, bool> dj)
            {
                for (var b = dj.First(); b != null; b = b.Next())
                    if (b.key() is EdgeType je  && ((tok == Qlx.ARROWBASE) ?
                        (cx.db.objects[je.arrivingType] as NodeType)?.GetS(cx, pd.tableRow.vals[je.arriveCol] as TInt)
                      : (cx.db.objects[je.leavingType] as NodeType)?.GetS(cx, pd.tableRow.vals[je.leaveCol] as TInt))// this node will match with xn
                                       is TableRow jn)
                        ds += (jn.defpos, jn);
            } 
            else if (pd is not null && pd.dataType is EdgeType pe && pd.defpos != pd.dataType.defpos
                && ((tok == Qlx.ARROWBASE) ?
                (cx.db.objects[pe.arrivingType] as NodeType)?.GetS(cx, pd.tableRow.vals[pe.arriveCol] as TInt)
                : (cx.db.objects[pe.leavingType] as NodeType)?.GetS(cx, pd.tableRow.vals[pe.leaveCol] as TInt))// this node will match with xn
               is TableRow tn)
                ds += (tn.defpos, tn);
            else if (pd is not null && pd.defpos == pd.dataType.defpos) // schema case
            {
                if (pd.dataType is EdgeType et &&
                    cx.db.objects[(tok == Qlx.ARROWBASE) ? et.leavingType : et.arrivingType] is NodeType pn
                    && pn.Schema(cx) is TableRow ts)
                    ds += (ts.defpos, ts);
                else if (pd.dataType is NodeType pg)
                {
                    for (var b = pg.sindexes.First(); b != null; b = b.Next())
                        if ((cx.db.objects[b.key()] as EdgeType)?.Schema(cx) is TableRow tq)
                            ds += (tq.defpos, tq);
                    for (var b = pg.rindexes.First(); b != null; b = b.Next())
                        if ((cx.db.objects[b.key()] as EdgeType)?.Schema(cx) is TableRow tq)
                            ds += (tq.defpos, tq);
                }
            }
            else if (pd is not null && pd.dataType is NodeType pn && pn is not EdgeType) // an edge attached to the TNode pd
            {
                var ctr = CTree<Domain, int>.Empty;
                var tg = truncator != CTree<long, (int, Domain)>.Empty;
                // case 1: pn has a primary index
                for (var b = pn.rindexes.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is EdgeType rt)
                    {
                        if (pd.defpos == pd.dataType.defpos) // schmema flag
                        {
                            ds += (rt.defpos, rt.Schema(cx));
                            continue;
                        }
                        if (xn.domain.defpos >= 0 && xn.domain.name != rt.name)
                                continue;
                        var lm = truncator.Contains(rt.defpos) ? truncator[rt.defpos].Item1 : int.MaxValue;
                        var ic = (xn.tok == Qlx.ARROWBASE) ? rt.leaveCol : rt.arriveCol;
                        var xp = (xn.tok == Qlx.ARROWBASE) ? rt.leaveIx : rt.arriveIx;
                        for (var g = b.value().First(); g != null; g = g.Next())
                            if (g.key()[0] == ic)
                            {
                                if (cx.db.objects[xp] is Index rx
                                && pd.tableRow.vals[pn.idCol] is TInt ti
                                && rx.rows?.impl?[ti] is TPartial tp)
                                {
                                    if (lm < tp.value.Count && truncator[rt.defpos].Item2 is Domain dm
                                        && dm.Length > 0)
                                        ds = Trunc(ds, rt, tp.value, lm, dm);
                                    else
                                        for (var c = tp.value.First(); c != null; c = c.Next())
                                            if (rt.tableRows[c.key()] is TableRow tr)
                                            {
                                                if (tg)
                                                {
                                                    if (AllTrunc(ctr))
                                                        goto alldone;
                                                    if (Trunc(ctr, rt))
                                                        goto rtdone;
                                                    ctr = AddIn(ctr, rt);
                                                }
                                                ds += (tr.defpos, tr);
                                            }
                                }
                            rtdone:;
                            }
                    }
                // case 2: pn has no primary index: follow the above logic for sysRefIndexes instead
                var la = truncator.Contains(Domain.EdgeType.defpos) ? truncator[Domain.EdgeType.defpos].Item1 : int.MaxValue;
                for (var b = pn.sindexes[pd.tableRow.defpos]?.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is TableColumn cc
                        && cx.db.objects[cc.tabledefpos] is EdgeType rt
                        && rt.NameFor(cx) is string ne && xn.domain.NameFor(cx) is string nx
                        && (ne == "" || nx == "" || ne == nx)
                        && b.value() is CTree<long, bool> pt)
                        for (var c = pt.First(); c != null; c = c.Next())
                        {
                            var lm = truncator.Contains(rt.defpos) ? truncator[rt.defpos].Item1 : int.MaxValue;
                            if (pd.defpos == pd.dataType.defpos && lm-- > 0 && la-- > 0)  // schema flag
                            {
                                ds += (rt.defpos, rt.Schema(cx));
                                continue;
                            }
                            if (!xn.label.Match(cx, new CTree<long, bool>(rt.defpos, true), Qlx.EDGETYPE))
                                continue;
                            if (rt.tableRows[c.key()] is TableRow dr && lm-- > 0 && la-- > 0)
                                ds += (dr.defpos, dr);
                        }
                    alldone:;
            }
            else // use Label, Label expression, xn's domain, or all node/edge types, and the properties specified
                ds = xn.For(cx, this, xn, ds);
            var df = ds.First();
            if (df != null && !Done(cx))
                DbNode(cx, new NodeStep(be.alt, xn, df, new ExpStep(be.alt, be.matches?.Next(), be.next)),
                     (xn is GqlEdge && xn is not GqlPath) ? xn.tok : tok, pd);
        }
        static CTree<Domain,int> AddIn(CTree<Domain,int> ctr,EdgeType rt)
        {
            ctr = AddIn1(ctr, rt);
            ctr += (Domain.EdgeType, ctr[Domain.EdgeType]+1);
            return ctr;
        }
        static CTree<Domain,int> AddIn1(CTree<Domain,int> ctr,Table tb)
        {
            ctr += (tb, ctr[tb] + 1);
            for (var b = tb.super.First(); b != null; b = b.Next())
                if (b.key() is Table t)
                    ctr = AddIn1(ctr, t);
            return ctr;
        }
        static BTree<long,TableRow> Trunc(BTree<long,TableRow> ds,EdgeType rt,CTree<long,bool> t,int lm,Domain dm)
        {
            var mt = new MTree(dm, TreeBehaviour.Ignore, 0);
            for (var b = t.First(); b != null; b = b.Next())
            if (rt.tableRows[b.key()] is TableRow tr){
                var vs = tr.vals;
                var k = CList<TypedValue>.Empty;
                for (var c = dm.rowType.First(); c != null; c = c.Next())
                    if (c.value() is long p && vs[p] is TypedValue v)
                        k += v;
                mt += (k, 0, 0);
            }
            var ct = 0;
            for (var b = mt.First(); b != null && ct < lm; b=b.Next())
                if (b.Value() is long p && rt.tableRows[p] is TableRow tr)
                    ds += (p, tr);
            return ds;
        }
        bool Trunc(CTree<Domain,int> ctr,EdgeType rt)
        {
            if (Trunc1(ctr, rt))
                return true;
            if (truncator.Contains(Domain.EdgeType.defpos) 
                && (ctr[Domain.EdgeType]>= truncator[Domain.EdgeType.defpos].Item1))
                    return true;
            return false;
        }
        bool Trunc1(CTree<Domain,int> ctr,Table st)
        {
            if (truncator.Contains(st.defpos) && ctr[st] >= truncator[st.defpos].Item1)
                return true;
            for (var b = st.super.First(); b != null; b = b.Next())
                if (b.key() is Table t)
                    return Trunc1(ctr, t);
            return false;
        }
        bool AllTrunc(CTree<Domain, int> ctr)
        {
            if (truncator.Contains(Domain.EdgeType.defpos)
                && (ctr[Domain.EdgeType] >= truncator[Domain.EdgeType.defpos].Item1))
                return true;
            return false;
        }
        /// <summary>
        /// For each dn in ds:
        /// If xn's specific properties do not match dx's then backtrack.
        /// We bind each t in t(xn), using the values in dn.
        /// If DbNode matches, it calls ExpNode for the next expression, 
        /// or the next expression from the Pop(), or AddRow if there is none.
        /// After AddRow if there is a Path in progress, try a further repeat of the paterrn.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="bn">Step gives current state of the match</param>
        /// <param name="pd">If not null, the previous matching node</param> 
        void DbNode(Context cx,NodeStep bn, Qlx tok, TNode? pd)
        {
            int step;
            var ob = cx.binding;
            var ov = cx.values;
            var pa = cx.paths[bn.alt.defpos];
            var ot = pa;
            TNode? dn = null;
            var ns = bn.nodes;
            ABookmark<Domain, bool>? db = null;
            while (ns is not null)
            {
                step = ++_step;
                if (ns?.value() is not TableRow tr)
                    goto backtrack;
                if (flags.HasFlag(Flags.Schema))
                {
                    for (var b = (cx.db.objects[tr.tabledefpos] as NodeType)?.nodeTypes.First(); b != null; b = b.Next())
                        if (b.key() is NodeType dm)
                        {
                            if (dm is EdgeType et && pd is not null &&
                                ((bn.xn.tok == Qlx.ARROWBASE) ? et.leavingType : et.arrivingType) != pd.dataType.defpos)
                                continue;
                            cx.binding += (bn.xn.defpos, new TRow(dm, tr.vals));
                            dn = dm.Node(cx, tr);
                            goto next;
                        }
                    goto backtrack;
                }
                else
                {
                    var dt = cx.db.objects[tr.tabledefpos] as NodeType??throw new PEException("PE70704");
                    db = cx.db.joinedNodes[tr.defpos]?.First();
                    var oc = cx.binding;
                LoopB:
                    if (db?.key() is NodeType dj)
                        dt = dj;
                    dn = dt.Node(cx, tr);
                    if (dn.tableRow is TableRow rw && rw.tabledefpos != dt.defpos
                        && cx.db.joinedNodes[tr.defpos]?.Contains(dt) == true)
                        dn = new TNode(cx,new TableRow(rw.defpos, rw.ppos, dt.defpos, rw.vals));
                    if (dn is null)
                        goto another;
                    if (bn.alt.mode == Qlx.TRAIL && dn is TEdge && pa?.HasNode(dn) == true)
                        goto another;
                    if (bn.alt.mode == Qlx.ACYCLIC && dn is not null && pa?.HasNode(dn) == true)
                        goto another;
                    if (bn.alt.mode == Qlx.SHORTEST && !Shortest(bn, cx))
                        goto another;
                    if (bn.alt.mode == Qlx.LONGEST && !Longest(bn, cx))
                        goto another;
                    if (dn is not null)
                    {
                        if (pa is not null && dn != pd)
                        {
                            cx.paths += (bn.alt.defpos, pa + (cx, dn));
                            cx.binding += (bn.alt.pathId, pa[0L]);
                        }
                        DoBindings(cx, bn.alt.defpos, bn.xn, dn);
                        if (!bn.xn.CheckProps(cx, dn))
                            goto another;
                        cx.values += (bn.xn.defpos, dn);
                        if (dn is TEdge de && pd is not null
                            && ((bn.xn.tok == Qlx.ARROWBASE) ? de.leaving : de.arriving) is TInt pv
                            && pv.ToLong()?.CompareTo(pd.tableRow.defpos) != 0 && pv.CompareTo(pd.id)!=0)
                            goto another;
                    }
                    goto next;
                another:
                    db = db?.Next();
                    cx.binding = oc;
                    if (db!= null)
                        goto LoopB;
                    goto backtrack;
                }
            next:
                bn.next.Next(cx, null, (tok == Qlx.WITH) ? Qlx.Null : tok, bn.xn, dn);
            backtrack:
                if (ot is not null)
                {
                    cx.paths += (bn.alt.defpos, ot);
                    cx.binding += (bn.alt.pathId, ot[0]);
                }
                cx.binding = ob; // unbind all the bindings from this recursion step
                if (Done(cx))
                    break;
                ns = ns?.Next();
            }
            if (ot is not null)
            {
                cx.paths += (bn.alt.defpos, ot);
                cx.binding += (bn.alt.pathId, ot[0]);
            }
            cx.values = ov;
            cx.binding = ob;
        }
        static TPath PathInit(Context cx, TPath pa, ABookmark<int, long?> ma, int d)
        {
            for (var c = ma; c != null; c = c.Next())
                if (cx.obs[c.value() ?? -1L] is GqlNode cn)
                {
                    if (cn is GqlPath sp && sp.pattern.First() is ABookmark<int, long?> sm)
                        pa = PathInit(cx, pa, sm, d + 1);
                    else
                        for (var b = cn.state.First(); b != null; b = b.Next())
                            if (b.value() is TGParam tg && b.key() >= 0L && cx.obs[tg.uid] is QlValue sv
                                    && sv.defpos >= ma.value() && !pa.values.Contains(sv.defpos))
                            {
                                var ta = new TArray(sv.domain);
                                for (var e = 0; e < d; e++)
                                    ta = new TArray(ta.dataType);
                                pa += (cx, sv.defpos, ta);
                            }
                }
            return pa;
        }
        /// <summary>
        /// Remember that ExpNode continues to the end of the graph when called, with 
        /// successful rows added in by AddRow.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="bp">The Step current state and continuation</param>
        /// <param name="dn">The most recent TNode</param>
        void PathNode(Context cx, PathStep bp, TNode dn)
        {
            var step = ++_step;
            var ob = cx.binding;
            var ot = cx.paths[bp.alt.defpos];
            if (cx.obs[bp.sp.pattern.First()?.value() ?? -1] is not GqlNode xi || !xi.CheckProps(cx, dn))
                goto backtrack;
            DoBindings(cx, bp.alt.defpos, xi, dn);
            if (bp.sp.pattern.First() is not ABookmark<int, long?> fn) goto backtrack;
            if (cx.obs[fn.value() ?? -1L] is GqlNode xn && bp.sp is GqlPath sp
                && (bp.im < sp.quantifier.Item2 || sp.quantifier.Item2<0))
                ExpNode(cx, new ExpStep(bp.alt,fn.Next(), bp), xn.tok, bp.xn, dn); // use ordinary ExpNode for the internal pattern
                                                                                   // dn must be an edge
                                                                                   // and xn must be an GqlEdge
            backtrack:
            if (ot is not null)
            {
                cx.paths += (bp.alt.defpos, ot);
                cx.binding += (bp.alt.pathId, ot[0]);
            }
            cx.binding = ob; // unbind all the bindings from this recursion step
        }
        /// <summary>
        /// When we match a database node we process the binding information in the GqlNode:
        /// All local bindings are simple values at this stage.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ap">The current alt defpos</param>
        /// <param name="xn">The GqlNode or GqlEdge or GqlPath</param>
        /// <param name="dn">The current database node (TNode or TEdge)</param>
        void DoBindings(Context cx, long ap, GqlNode xn, TNode dn)
        {
            if (xn.state != CTree<long, TGParam>.Empty && dn.dataType is NodeType nt)
            {
                var bi = cx.binding;
                if (nt.infos[cx.role.defpos]?.names is Names ns)
                    for (var b = xn.docValue?.First(); b != null; b = b.Next())
                        if (gDefs[b.value().defpos] is TGParam tg
                            && b.key() is string n
                            && ns?[n] is long np
                            && (dn.tableRow.vals[np]??cx._Ob(np)?.Eval(cx)) is TypedValue tv)
                        {
                            if (tg.type.HasFlag(TGParam.Type.Group))
                            {
                                var ta = bi[tg.uid] as TArray ?? new TArray(tv.dataType);
                                bi += (tg.uid, ta + (cx,ta.Length, tv));
                            }
                            else
                                bi += (tg.uid, tv);
                        }
                for (var b = xn.state.First(); b != null; b = b.Next())
                {
                    TypedValue tv = TNull.Value;
                    if (b.value() is TGParam tg)
                   {
                        switch (b.key())
                        {
                            case -(long)Qlx.Id: tv = dn; break;
                            case -(long)Qlx.RARROW:
                                {
                                    var te = cx.GType(nt.leavingType);
                                    var er = te?.Get(cx, dn.tableRow.vals[nt.leaveCol] as TInt);
                                    tv = (te is null || er is null) ? TNull.Value : te.Node(cx, er);
                                    break;
                                }
                            case -(long)Qlx.ARROW:
                                {
                                    var te = cx.GType(nt.arrivingType);
                                    var er = te?.Get(cx, dn.tableRow.vals[nt.arriveCol] as TInt);
                                    tv = (te is null || er is null) ? TNull.Value : te.Node(cx, er);
                                    break;
                                }
                            case -(long)Qlx.TYPE: //tv = new TChar(nt.name); break;
                                {
                                    tv = new TChar(SubType(cx, nt, dn).name);
                                    break;
                                }
                            default:
                                {
                                    if (tg.type.HasFlag(TGParam.Type.Type))
                                        tv = new TChar(SubType(cx, nt, dn).name); 
                                    else if (b.key() == xn.defpos)
                                        tv = dn;
                                    break;
                                }
                        }
                        if (tv != TNull.Value && !cx.role.dbobjects.Contains(tg.value))
                        {
                            if (tg.type.HasFlag(TGParam.Type.Group))
                            {
                                var ta = bi[tg.uid] as TArray ?? new TArray(tv.dataType);
                                bi += (tg.uid, ta + (cx, ta.Length, tv));
                            }
                            else
                                bi += (tg.uid, tv);
                        }
                    }
                }
                cx.binding = bi;
            }
        }
        static NodeType SubType(Context cx,NodeType nt,TNode dn)
        {
            for (var b = nt.subtypes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is NodeType st && st.tableRows.Contains(dn.defpos))
                    return SubType(cx, st, dn);
            return nt;
        }
        // implement an algorithn for SHORTEST
        bool Shortest(Step st,Context cx)
        {
            ABookmark<int, long?>? gb = null;
            for (var s = st;s!=null;s=s.Cont)
                if (s is GraphStep gs)
                {
                    gb = gs.matchAlts?.Previous()??gs.ms.matchList.Last();
                    break;
                }
            if (cx.obs[gb?.value() ?? -1L] is GqlMatchAlt sm
                   && (sm.mode == Qlx.SHORTEST || sm.mode==Qlx.LONGEST)
                   && sm.pathId >= 0
                   && cx.obs[cx.result] is ExplicitRowSet ers)
            {
                var rws = ers.explRows;
                if (rws.Length == 0) return true;
                var (ol,ov) = rws[ers.Length - 1];
                var cp = cx.paths[defpos] ?? throw new PEException("PE030803");
                if (ov[sm.pathId] is not TList op)
                    return true; // ??
                if (op.Length > cp.Length)
                {
                    ers += (ExplicitRowSet.ExplRows, rws - (ers.Length-1));
                    cx.Add(ers);
                    return true;
                }
                return false;
            }
            return true;
        }
        bool Longest(Step st, Context cx)
        {
            ABookmark<int, long?>? gb = null;
            for (var s = st; s != null; s = s.Cont)
                if (s is GraphStep gs)
                {
                    gb = gs.matchAlts?.Previous() ?? gs.ms.matchList.Last();
                    break;
                }
            if (cx.obs[gb?.value() ?? -1L] is GqlMatchAlt sm
                   && (sm.mode == Qlx.LONGEST)
                   && sm.pathId >= 0
                   && cx.obs[cx.result] is ExplicitRowSet ers)
            {
                var rws = ers.explRows;
                if (rws.Length == 0) return true;
                var (ol, ov) = rws[ers.Length - 1];
                var cp = cx.paths[defpos] ?? throw new PEException("PE030803");
                if (ov[sm.pathId] is not TList op)
                    return true; // ??
                if (op.Length < cp.Length)
                {
                    ers += (ExplicitRowSet.ExplRows, rws - (ers.Length - 1));
                    cx.Add(ers);
                    return true;
                }
                return false;
            }
            return true;
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (MatchStatement)base._Replace(cx, was, now);
            var ch = false;
            var ls = BList<long?>.Empty;
            for (var b=matchList.First();b!=null;b=b.Next())
                if (cx.obs[b.value()??-1L] is GqlMatchAlt sa)
                {
                    var a = sa.Replace(cx, was, now);
                    if (a != sa)
                        ch = true;
                    ls += a.defpos;
                }
            var tg = BTree<long, (long, long)>.Empty;
            for (var b=truncating.First();b!=null;b=b.Next())
            {
                var (i, d) = b.value();
                var k = b.key();
                long nk = k;
                if (cx.obs[k] is EdgeType)
                    nk = cx.done[k]?.defpos ?? k;
                var nd = cx.Replaced(d);
                tg += (k, (i, d));
            }
            return ch?cx.Add(r+(MatchList,ls)):r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" GDefs (");
            sb.Append(gDefs);
            sb.Append(')');
            sb.Append(" Graphs (");
            var cm = "";
            for (var b=matchList.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value() ?? -1L));
            }
            sb.Append(')');
            if (where != CTree<long, bool>.Empty)
            {
                sb.Append(" Where "); sb.Append(where);
            }
            if (body>=0)
            {
                sb.Append(" Body "); sb.Append(Uid(body));
            }
            if (bindings>=0)
            {
                sb.Append(" Bindings "); sb.Append(Uid(bindings));
            }
            if (then!=BList<long?>.Empty)
            {
                sb.Append(" THEN ["); cm = "";
                for (var b = then.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value() ?? -1L));
                }
                sb.Append(']');
            }
            return sb.ToString();
        } 
    }
}