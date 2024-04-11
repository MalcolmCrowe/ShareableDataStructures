using System.Runtime.Intrinsics.X86;
using System.Text;
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
            Stmt = -93; // string
        public string? stmt => (string?)mem[Stmt];
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal string? label => (string?)mem[Label];
        internal Executable(long dp, BTree<long, object>? m=null) 
            : base(dp, m??BTree<long, object>.Empty) { }
        internal Executable(long dp, string s) : base(dp,new BTree<long,object>(Stmt,s)) { }
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
                        nx = x.Obey(a);
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
        /// <summary>
        /// Obey the Executable for the given Activation.
        /// All non-CRUD Executables should have a shortcut override.
        /// The base class implementation is for DDL execution in stored procedures.
        /// It parses the string and executes it in the current context
        /// </summary>
        /// <param name="cx">The context</param>
		public virtual Context Obey(Context cx)
        {
            Context nc = new(cx) { parse = cx.parse };
            var db = new Parser(nc).ParseSql(stmt ?? "", Domain.Content);
            cx.db = db;
            cx.affected += nc.affected;
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
            if (mem.Contains(ObInfo.Name)) { sb.Append(' '); sb.Append(mem[ObInfo.Name]); }
            sb.Append(' ');sb.Append(Uid(defpos));
            if (mem.Contains(Stmt)) { sb.Append(" Stmt:"); sb.Append(stmt); }
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
        public CommitStatement(long dp) : base(dp) { }
        public override Context Obey(Context cx)
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
    internal class RollbackStatement : Executable
    {
        public RollbackStatement(long dp) : base(dp) { }
        public override Context Obey(Context cx)
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
            : base(dp, new BTree<long, object>(Union, r.defpos)) 
        { }
        protected SelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectStatement operator +(SelectStatement et, (long, object) x)
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
            return (SelectStatement)et.New(m + x);
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
        /// <summary>
        /// Obey the executable
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cx.result = union;
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectStatement)base._Replace(cx, so, sv);
            var nu = ((RowSet?)cx.obs[union])?.Replace(cx, so, sv)?.defpos;
            if (nu!=union && nu is not null)
                r += (cx,Union, nu);
            return r;
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
		public CompoundStatement(long dp, string n)
            : base(dp, new BTree<long, object>(Label, n))
        { }
        protected CompoundStatement(long dp, BTree<long, object> m) : base(dp, m) { }
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
        /// Obey a Compound Statement.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context Obey(Context cx)
        {
            cx.exec = this;
            var act = new Activation(cx, label ?? "") { binding=cx.binding};
            act = (Activation)ObeyList(stms, act);
            act.signal?.Throw(cx);
            cx.db = act.db;
            return act.SlideDown();
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            QMarks = -396,  // BList<long?> SqlValue
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
        public override Context Obey(Context cx)
        {
            if (target == null)
                return cx;
            return ((Executable)target.Instance(defpos,cx)).Obey(cx);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            Init = -97; // long SqlValue
        /// <summary>
        /// Default initialiser
        /// </summary>
        public long init => (long)(mem[Init]??-1L);
        public long vbl => (long)(mem[AssignmentStatement.Vbl]??-1L);
        /// <summary>
        /// Constructor: a new local variable
        /// </summary>
        public LocalVariableDec(long dp, SqlValue v, BTree<long,object>?m=null)
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
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var vb = (SqlValue)(cx.obs[vbl] ?? throw new PEException("PE1101"));
            TypedValue tv = cx.obs[init]?.Eval(cx)??vb.domain.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.AddValue(vb, tv); // We expect a==cx, but if not, tv will be copied to a later
            return cx;
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LocalVariableDec)base._Replace(cx, so, sv);
            var ni = ((SqlValue?)cx.obs[init])?.Replace(cx,so,sv)?.defpos ;
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
    internal class FormalParameter : SqlValue
    {
        internal const long
            ParamMode = -98, // Sqlx
            Result = -99; // Sqlx
        public long val => (long)(mem[AssignmentStatement.Val] ?? -1L);
        /// <summary>
        /// The mode of the parameter: IN, OUT or INOUT
        /// </summary>
		public Sqlx paramMode => (Sqlx)(mem[ParamMode] ?? Sqlx.IN);
        /// <summary>
        /// The result mode of the parameter: RESULT or NO
        /// </summary>
		public Sqlx result => (Sqlx)(mem[Result] ?? Sqlx.NO);
        /// <summary>
        /// Constructor: a procedure formal parameter from the parser
        /// </summary>
        /// <param name="m">The mode</param>
		public FormalParameter(long vp, Sqlx m, string n,Domain dt)
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
        public override Context Obey(Context cx)
        {
            var cu = cx.obs[cs];
            if (cu is not null)
                cx.AddValue(cu, cu.Eval(cx));
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            HType = -102; // Sqlx
        /// <summary>
        /// The handler type: CONTINUE, EXIT, or UNDO
        /// </summary>
		public Sqlx htype => (Sqlx)(mem[HType]??Sqlx.EXIT);
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
		public HandlerStatement(long dp, Sqlx t, string n)
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
		public override Context Obey(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        public override Context Obey(Context cx)
        {
                Activation? definer = null;
                for (Context? p = cx; definer == null && p != null; p = p.next)
                    if (p.cxid == hdefiner)
                        definer = p as Activation;
                if (hdlr?.htype == Sqlx.UNDO)
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
                ((Executable?)cx.obs[hdlr?.action??-1L])?.Obey(cx);
                if (hdlr?.htype == Sqlx.EXIT && definer?.next is Context nx)
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
        public override Context Obey(Context cx)
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
            Val = -105, // long SqlValue
            Vbl = -106; // long SqlValue
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
		public AssignmentStatement(long dp,DBObject vb,SqlValue va) 
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
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var vb = cx._Ob(vbl);
            var dm = vb?.domain??Domain.Content;
            if (cx.obs[val] is DBObject va && va.Eval(cx) is TypedValue tv)
            {
                if (vb is SqlValueExpr se && se.op == Sqlx.DOT && cx._Ob(se.left)?.Eval(cx) is TNode tl
                    && cx._Ob(se.right) is SqlCopy sc && tl.dataType is Table st 
                    && st.pathDomain.representation[sc.copyFrom] is Domain cd
                    && cd.Coerce(cx, tv) is TypedValue v1)
                    cx.Add(new Update(tl.tableRow, new CTree<long,bool>(st.defpos,true), new CTree<long, TypedValue>(sc.copyFrom, v1),
                        cx.db.nextPos, cx));
                else if (vb is SqlField sf && cx._Ob(sf.from)?.Eval(cx) is TNode nf
                    && nf.dataType is Table tf && tf._PathDomain(cx).infos[cx.role.defpos] is ObInfo fi
                    && cx._Ob(fi.names[sf.name ?? "?"].Item2 ?? -1L) is TableColumn tc
                    && tc.domain.Coerce(cx, tv) is TypedValue v3)
                    cx.Add(new Update(nf.tableRow, new CTree<long,bool>(tf.defpos,true), new CTree<long, TypedValue>(tc.defpos, v3),
                        cx.db.nextPos, cx));
                else if (dm != Domain.Content && dm != Domain.Null && dm.Coerce(cx, tv) is TypedValue v2)
                    cx.values += (vb?.defpos ?? -1L, v2);
            }
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            List = -108, // BList<long?> SqlValue
            Rhs = -109; // long SqlValue
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
        public MultipleAssignment(long dp,Context cx,BList<Ident>ls,SqlValue rh) : 
            base(dp,_Mem(cx,ls,rh))
        { }
        protected MultipleAssignment(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,BList<Ident> lh,SqlValue rg)
        {
            var dm = rg.domain ?? Domain.Null;
            var r = new BTree<long, object>(Rhs, rg.defpos);
            var ls = BList<long?>.Empty;
            for (var b = lh.First(); b != null; b = b.Next())
                if (b.value() is Ident id && cx.obs[id.iix.dp] is SqlValue v
                            && v.domain is Domain vd)
                {
                    dm = dm.Constrain(cx, id.iix.lp, vd);
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
        public override Context Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            if (cx.obs[rhs] is SqlValue sv)
            {
                TRow r = (TRow)sv.Eval(cx);
                for (int j = 0; j < r.Length; j++)
                    if (list[j] is long p)
                        cx.values += (p, r[j] ?? TNull.Value);
            }
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (MultipleAssignment)base._Replace(cx, so, sv);
            var nd = (Domain)lhsType.Replace(cx, so, sv);
            if (nd != lhsType && nd is not null)
                r += (LhsType, nd);
            var nl = cx.ReplacedLl(list);
            if (nl != list)
                r+=(cx, List, nl);
            var na = ((SqlValue?)cx.obs[rhs])?.Replace(cx, so, sv)?.defpos;
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
        public override Context Obey(Context cx)
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
            Ret = -110; // long SqlValue
        /// <summary>
        /// The return value
        /// </summary>
		public long ret => (long)(mem[Ret] ?? -1L);
        public long result => (long)(mem[SqlInsert.Value] ??-1L);
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(long dp,SqlValue v) : base(dp, 
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
        public override Context Obey(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ReturnStatement)base._Replace(cx, so, sv);
            var nr = ((SqlValue?)cx.obs[ret])?.Replace(cx, so, sv)?.defpos;
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
            _Operand = -112, // long SqlValue
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
        public SimpleCaseStatement(long dp,SqlValue op,BList<WhenPart> ws,
            BList<long?> ss) : 
            base(dp,_Mem(op,ws,ss))
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(SqlValue op, BList<WhenPart> ws,
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
        public override Context Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for(var c = whens.First();c is not null; c=c.Next())
                if (cx.obs[operand] is SqlValue sv && sv.Matches(cx)==true
                    && c.value() is long p && cx.obs[p] is WhenPart w)
                    return ObeyList(w.stms, cx);
            return ObeyList(els, cx);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SimpleCaseStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[operand])?.Replace(cx, so, sv)?.defpos;
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
        public override Context Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for (var c = whens.First(); c != null; c = c.Next())
                if (c.value() is long p && cx.obs[p] is WhenPart w
                    && ((SqlValue?)cx.obs[w.cond])?.Matches(cx) == true)
                        return ObeyList(w.stms, cx);
			return ObeyList(els,cx);
		}
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            Cond = -114; // long SqlValue
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
        public WhenPart(long dp,SqlValue v, BList<long?> s) 
            : base(dp, BTree<long, object>.Empty+(Cond,v.defpos)+(CompoundStatement.Stms,s))
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
        public override Context Obey(Context cx)
        {
            var a = cx;
            if (cond == -1L || ((SqlValue?)cx.obs[cond])?.Matches(cx)==true)
            {
                a.val = TBool.True;
                return ObeyList(stms, cx);
            }
            a.val = TBool.False;
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WhenPart)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[cond])?.Replace(cx, so, sv)?.defpos;
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
            Search = -118, // long SqlValue
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
		public IfThenElse(long dp,SqlValue se,BList<long?>th,BList<long?>ei,BList<long?> el) 
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
        /// Obey an if-then-else statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx; 
            a.exec = this;
            if (((SqlValue?)cx.obs[search])?.Matches(cx)==true)
                return ObeyList(then, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (g.value() is long p && cx.obs[p] is IfThenElse f 
                    && ((SqlValue?)cx.obs[f.search])?.Matches(cx)==true)
                    return ObeyList(f.then, cx);
            return ObeyList(els, cx);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (IfThenElse)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
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
        public override Context Obey(Context cx)
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
            Search = -122, // long SqlValue
            What = -123; // BList<long?> Executable
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
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
                r += (Search, ns);
            var nw = cx.FixLl(what);
            if (nw != what)
                r += (What, nw);
            return r;
        }
        /// <summary>
        /// Execute a while statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx; 
            a.exec = this;
            var na = cx;
            while (na==cx && a.signal == null && ((SqlValue?)cx.obs[search])?.Matches(cx)==true)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WhileStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no is not null)
                r+=(cx, Search, no);
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
		public long search => (long)(mem[WhileStatement.Search]??-1L);
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
                r += (WhileStatement.Search, ns);
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
        public override Context Obey(Context cx)
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
                if (((SqlValue?)cx.obs[search])?.Matches(act)!=false)
                    break;
            }
            cx = act.SlideDown(); 
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RepeatStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no is not null)
                r+=(cx,WhileStatement.Search, no);
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
        public override Context Obey(Context cx)
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
        public override Context Obey(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
    /// <summary>
    /// A for statement for a stored proc/func
    /// 
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
        public override Context Obey(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        public override Context Obey(Context cx)
        {
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (OpenStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
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
        public override Context Obey(Context cx)
        {
            cx.Add(new EmptyRowSet(defpos,cx,Domain.Null));
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (CloseStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
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
            How = -130, // Sqlx
            Outs = -131, // BList<long?> SqlValue
            Where = -132; // long SqlValue
        long cursor =>(long)(mem[Cursor]??-1L);
        /// <summary>
        /// The behaviour of the Fetch
        /// </summary>
        public Sqlx how => (Sqlx)(mem[How]??Sqlx.ALL);
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
        public FetchStatement(long dp, SqlCursor n, Sqlx h, SqlValue? w)
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
        public override Context Obey(Context cx)
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
                    case Sqlx.NEXT: break; // default case
                    case Sqlx.PRIOR: // reposition so we can refecth
                        rqpos -= 2;
                        break;
                    case Sqlx.FIRST:
                        rqpos = 1;
                        break;
                    case Sqlx.LAST:
                        {
                            int n = 0;
                            for (var e = ((RowSet?)cx.obs[cs.defpos])?.First(cx); e != null;
                                e = e.Next(cx))
                                n++;
                            rqpos = n - 1;
                            break;
                        }
                    case Sqlx.ABSOLUTE:
                        rqpos = cx.obs[where]?.Eval(cx)?.ToLong()??-1L;
                        break;
                    case Sqlx.RELATIVE:
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
                            &&  cx.obs[ou] is SqlValue sv)
                                cx.AddValue(sv, c);
 
                }
                else
                    cx = new Signal(defpos, "02000", "No obs").Obey(cx);
            }
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (FetchStatement)base._Replace(cx, so, sv);
            var nc = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (nc != (cx.done[cursor]?.defpos??cursor) && nc is not null)
                r+=(cx, Cursor, nc);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r+=(cx, Outs, no);
            var nw = ((SqlValue?)cx.obs[where])?.Replace(cx, so, sv)?.defpos;
            if (nw != (cx.done[where]?.defpos ?? where) && nw is not null)
                r+=(cx, Where, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (how != Sqlx.ALL)
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
        public override Context Obey(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            SetList = -139, // BTree<Sqlx,long?>
            SType = -140; // Sqlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype => (Sqlx)(mem[SType] ?? Sqlx.NO); // RAISE or RESIGNAL
        internal string? signal => (string?)mem[_Signal];
        internal BList<string> objects => (BList<string>?)mem[Objects]??BList<string>.Empty;
        internal BTree<Sqlx, long?> setlist =>
            (BTree<Sqlx, long?>?)mem[SetList] ?? BTree<Sqlx, long?>.Empty;
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
                m += (_Depth, cx._DepthTXV((BTree<Sqlx,long?>)o, d));
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
        public override Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (cx.tr == null)
                return cx;
            if (stype == Sqlx.RESIGNAL && !cx.tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            if (signal != null && cx.db is not null)
            {
                string? sclass = signal[0..2];
                var dia = cx.tr.diagnostics;
                dia += (Sqlx.RETURNED_SQLSTATE, new TChar(signal));
                for (var s = setlist.First(); s != null; s = s.Next())
                    if (s.value() is long p && cx.obs[p] is SqlValue sv)
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
                    cx = h.Obey(cx);
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SignalStatement)base._Replace(cx, so, sv);
            var sl = BTree<Sqlx, long?>.Empty;
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    sl += (b.key(), cx.uids[p] ?? b.value());
            r+=(cx, SetList, sl);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (stype != Sqlx.NO)
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
        internal Sqlx stype = Sqlx.SIGNAL;
        internal BList<string> objects = BList<string>.Empty;
        internal Exception? exception;
        internal BTree<Sqlx, long?> setlist = BTree<Sqlx, long?>.Empty;
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
            if (stype == Sqlx.RESIGNAL && !cx.tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            string sclass = signal[0..2];
            var dia = cx.tr.diagnostics;
            dia += (Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    if (b.value() is TypedValue v)
                        dia += (b.key(), v);
                dia += (Sqlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
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
                cx = h.Obey(cx);
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
            List = -141; // CTree<long,Sqlx>
        internal CTree<long, Sqlx> list =>
            (CTree<long, Sqlx>?)mem[List] ?? CTree<long, Sqlx>.Empty;
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
                m += (_Depth, cx._DepthTVX((CTree<long, Sqlx>)o, d));
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
        /// Obey a GetDiagnostics statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            for (var b = list.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s && b.value() is Sqlx t
                    && cx.tr is not null && cx.tr.diagnostics[t] is TypedValue v)
                cx.AddValue(s, v);
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (GetDiagnostics)base._Replace(cx, so, sv);
            var ls = CTree<long, Sqlx>.Empty;
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
        internal const long
            Outs = -142; // BList<long?> SqlValue
        /// <summary>
        /// The query
        /// </summary>
		public long sel => (long)(mem[ForSelectStatement.Sel] ?? -1L);
        /// <summary>
        /// The output tree
        /// </summary>
		public BList<long?> outs => (BList<long?>?)mem[Outs] ?? BList<long?>.Empty;
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
            if (p == Outs)
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
                r += (Outs, no);
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
		public override Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (((RowSet?)cx.obs[sel])?.First(cx) is Cursor rb)
            {
                a.AddValue(this, rb);
                for (var b = outs.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue sv)
                        a.AddValue(sv, rb[b.key()]);
            }
            else
                a.NoData();
            return cx;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectSingle)base._Replace(cx, so, sv);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r+=(cx, Outs, no);
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        public override Context Obey(Context cx)
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
        internal DeleteNode(long dp,SqlValue v)
            : base(dp,new BTree<long,object>(WhileStatement.What,v.defpos)) 
        { }
        protected DeleteNode(long dp, BTree<long, object>? m = null) : base(dp, m)
        {
        }
        protected DeleteNode(long dp, string s) : base(dp, s)
        {
        }
        public override Context Obey(Context cx)
        {
            var n = cx.obs[what]?.Eval(cx) as TNode??throw new DBException("22004");
            cx.Add(new Delete(n.tableRow, cx.db.nextPos));
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
        public override Context Obey(Context cx)
        {
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
        public override Context Obey(Context cx)
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
            GraphExps = -307; // CList<CList<SqlNode>> SqlNode (alternately with SqlEdges)
        // In MatchStatement we can also have SqlNodes that are SqlPaths 
        internal CList<CList<SqlNode>> graphExps =>
            (CList<CList<SqlNode>>)(mem[GraphExps] ?? CList<CList<SqlNode>>.Empty);
        internal BList<long?> stms => 
            (BList<long?>?)mem[IfThenElse.Then] ?? BList<long?>.Empty;
        public GraphInsertStatement(long dp, CList<CList<SqlNode>> ge, BList<long?> th)
            : base(dp, new BTree<long, object>(GraphExps, ge) + (IfThenElse.Then, th))
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
        /// GraphInsertStatement contains a CTree of SqlNode in lexical sequence.
        /// In Obey() we ensure that cx.values has a corresponding sequence of TNode.
        /// We create whatever nodee, edges, nodetypes, edgetypes are needed.
        /// </summary>
        /// <param name="cx">the context</param>
        /// <returns>the same context</returns>
        /// <exception cref="DBException"></exception>
        public override Context Obey(Context cx)
        {
            for (var b = graphExps.First(); b != null; b = b.Next())
                if (b.value() is CList<SqlNode> ge)
                {
                    //  Do the nodes first and then the edges!
                    for (var gb = ge.First(); gb != null; gb = gb.Next())
                        if (gb.value() is SqlNode nd && (!cx.values.Contains(nd.defpos))
                            && !cx.binding.Contains(nd.idValue)
                            && nd is not SqlEdge)
                        {
                            var nt = (NodeType)nd.domain;
                            if (nt.defpos < 0 && nt.labels.Count == 1)
                                for (var c = nd.labelSet.First(); c != null; c = c.Next())
                                {
                                    if (cx.obs[c.key()] is SqlLiteral sl
                                            && sl.Eval(cx) is TChar tc
                                            && cx._Od(cx.role.nodeTypes[tc.value] ?? -1L) is NodeType nt0)
                                        nt = nt0;
                                }
                            nd.Create(cx, nt);
                        }
                    SqlNode? ln = null;
                    SqlEdge? ed = null;
                    NodeType? et = null;
                    for (var gb = ge.First(); gb!=null && gb.value() is SqlNode n; gb = gb.Next())
                    {
                        if (n is SqlEdge && cx._Od(n.domain.defpos) is NodeType e)
                        {
                            ed = (SqlEdge)n;
                            et = e;
                        }
                        else if (ed is not null && ln is not null && et is not null)
                        {
                            ed += (SqlEdge.LeavingValue, (ed.tok == Sqlx.ARROWBASE) ? ln.defpos : n.defpos);
                            ed += (SqlEdge.ArrivingValue, (ed.tok == Sqlx.ARROWBASE) ? n.defpos : ln.defpos);
                            ed = (SqlEdge)cx.Add(ed);
                            ed.Create(cx, et, ln.labelSet.Count<=1);
                            ed = null;
                            ln = n;
                        }
                        else
                            ln = n;
                    }
                    if (ed!=null)
                        throw new DBException("22G0L",ed.ToString());
                }
            return cx;
        }
        internal CTree<long,bool> GraphTypes(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = graphExps.First(); b != null; b = b.Next())
                if (b.value() is CList<SqlNode> nl)
                    for (var c = nl.First(); c != null; c = c.Next())
                        if (c.value() is SqlNode sn)
                            r += (sn.domain.defpos, true);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
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
                if (cm == ",") sb.Append("]");
            }
            return sb.ToString();
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
            MatchList = -491, // BList<long?> SqlMatchAlt
            Truncating = -492; // BTree<long,(int,Domain)> EdgeType (or 0L)
        internal CTree<long, TGParam> gDefs =>
            (CTree<long, TGParam>)(mem[GDefs] ?? CTree<long, TGParam>.Empty);
        internal BList<long?> matchList =>
            (BList<long?>)(mem[MatchList] ?? BList<long?>.Empty);
        internal CTree<long, bool> where =>
            (CTree<long, bool>)(mem[RowSet._Where] ?? CTree<long, bool>.Empty);
        internal long body => (long)(mem[Procedure.Body] ?? -1L);
        internal BList<long?> then => (BList<long?>)(mem[IfThenElse.Then] ?? BList<long?>.Empty);
        internal BTree<long, (int, Domain)> truncating =>
            (BTree<long, (int, Domain)>)(mem[Truncating] ?? BTree<long, (int, Domain)>.Empty);
        internal Domain sort => (Domain?)mem[RowSet.RowOrder] ?? Domain.Null;
        internal int size => (int?)mem[RowSetSection.Size] ?? -1;
        internal long ret => (long)(mem[ReturnStatement.Ret] ?? -1L);
        internal BTree<string, (int, long?)> names =>
            (BTree<string, (int, long?)>?)mem[ObInfo.Names] ?? BTree<string, (int, long?)>.Empty;
        internal long bindings => (long)(mem[BindingTable] ?? -1L);
        [Flags]
        internal enum Flags { None = 0, Bindings = 1, Body = 2, Return = 4, Schema = 8 }
        internal Flags flags => (Flags)(mem[MatchFlags] ?? Flags.None);
        public MatchStatement(Context cx, BTree<long, (int, Domain)>? tg, CTree<long, TGParam> gs, BList<long?> ge,
            BTree<long, object> m, long st, long re)
            : this(cx.GetUid(), cx, tg, gs, ge, m, st, re)
        {
            cx.Add(this);
        }
        MatchStatement(long dp, Context cx, BTree<long, (int, Domain)>? tg, CTree<long, TGParam> gs, BList<long?> ge,
    BTree<long, object> m, long st, long re)
            : base(dp, _Mem(dp, cx, m, tg, gs, ge, st, re) + (Procedure.Body, st))
        {
            cx.Add(this);
        }
        public MatchStatement(long dp, BTree<long, object>? m = null) : base(dp, m)
        { }
        static BTree<long, object> _Mem(long dp, Context cx, BTree<long, object> m, BTree<long, (int, Domain)>? tg,
            CTree<long, TGParam> gs, BList<long?> ge, long st, long re)
        {
            for (var b = gs.First(); b != null; b = b.Next())
                cx.undefined -= b.key();
            m += (GDefs, gs);
            if (tg is not null)
                m += (Truncating, tg);
            var f = (Flags)(m[MatchFlags]??Flags.None);
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
                if (cx.obs[b.value() ?? -1L] is SqlMatch sm)
                    for (var c = sm.matchAlts.First(); c != null; c = c.Next())
                        if (cx.obs[c.value() ?? -1L] is SqlMatchAlt sa && gs[sa.pathId] is TGParam p
                                && cx.obs[p.uid] is SqlValue sv)
                            sv.AddFrom(cx, sa.defpos);
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
        /// <summary>
        /// We traverse the given match graphs in the order given, matching with possible database nodes as we move.
        /// The match graphs and the set of TGParams are in this MatchStatement.
        /// The database graph is in cx.db.graphs and cx.db.nodeids.
        /// The handling of a binding variable x defined within a path pattern is quite tricky: 
        /// during the path matching process x stands for a simple value 
        /// (a single node, or an expression evalated from its properties),
        /// But, as a field of the path value p, p.x is an array of the values constructed so far.
        /// In result rowset, RETURN, WHERE, or the body statement of the MATCH, x will be an array
        /// for the current binding set.
        /// Accordingly, the MatchStatement defines such x as array types, except within the body of DbNode.
        /// At the end of a pattern, in PathStep, we change the binding to contain the array values instead,
        /// getting the values for earlier pattern matching from cx.paths.
        /// This means if ps is a PathStep, the datatype of the binding associated with x will change in ps.Next
        /// from (T) to (T array) for all non-path x named in xn.state for some xn in ps.sp.pattern.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <returns></returns>
        public override Context Obey(Context _cx)
        {
            // Graph expression and Database agree on the set of NodeType and EdgeTypes
            // Traverse the given graphs, binding as we go
            var cx = new Context(_cx)
            {
                binding = _cx.binding,
                result = domain.defpos
            };
            _step = 0;
            var gf = matchList.First();
            if (cx.obs[gf?.value() ?? -1L] is SqlMatch sm)
                for (var b = sm.matchAlts.First(); b != null; b = b.Next())
                    if (cx.obs[b.value() ?? -1L] is SqlMatchAlt sa)
                    {
                        cx.paths += (sa.defpos, new TPath(sa.defpos, cx));
                        var xf = sa.matchExps.First();
                        if (gf is not null && xf is not null)
                            ExpNode(cx, new ExpStep(sa, xf, new GraphStep(gf.Next(), new EndStep(this))), Sqlx.Null, null);
                    }
            // aggregations
            if (domain is SelectRowSet srs && srs.aggs != CTree<long, bool>.Empty)
            { // code copied from SelectRowSet.Build
                var rws = CList<TRow>.Empty;
                var re = (ReturnStatement?)cx.obs[ret];
                var fd = cx.funcs[re?.ret ?? -1L];
                for (var b = fd?.First(); b != null; b = b.Next())
                    if (b.value() != BTree<long, Register>.Empty)
                    {
                        // Remember the aggregating SqlValues are probably not just aggregation SqlFunctions
                        // Seed the keys in cx.values
                        var vs = b.key().values;
                        cx.values += vs;
                        for (var d = srs.matching.First(); d != null; d = d.Next())
                            if (cx.values[d.key()] is TypedValue v)
                                for (var c = d.value().First(); c != null; c = c.Next())
                                    if (!vs.Contains(c.key()))
                                        vs += (c.key(), v);
                        // and the aggregate function accumulated values
                        for (var c = srs.aggs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlValue v)
                            {
                                if (v is SqlFunction fr && fr.op == Sqlx.RESTRICT)
                                    cx.values += (fr.val, fr.Eval(cx));
                                else
                                    cx.values += (v.defpos, v.Eval(cx));
                            }
                        // compute the aggregation expressions from these seeds
                        for (var c = srs.rowType.First(); c != null; c = c.Next())
                            if (c.value() is long p && cx.obs[p] is SqlValue sv
                                && sv.IsAggregation(cx, srs.aggs) != CTree<long, bool>.Empty)
                                vs += (sv.defpos, sv.Eval(cx));
                        // add in any exposed RESTRICT values
                        for (var c = srs.aggs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlFunction fr && fr.op == Sqlx.RESTRICT)
                                vs += (fr.val, fr.Eval(cx));
                        // for the having calculation to work we must ensure that
                        // having uses the uids that are in aggs
                        for (var h = srs.having.First(); h != null; h = h.Next())
                            if (cx.obs[h.key()]?.Eval(cx) != TBool.True)
                                goto skip;
                        rws += new TRow(srs, vs);
                    skip:;
                    }
                if (rws == CList<TRow>.Empty)
                    rws += new TRow(srs, CTree<long, TypedValue>.Empty);
                cx.Add((RowSet)srs.New(srs.mem + (RowSet._Rows, rws) + (RowSet._Built, true) + (MatchFlags, true)
                    - Index.Tree + (RowSet.Groupings, srs.groupings)));
                cx.result = srs.defpos;
            }
            if (cx.obs[cx.result] is RowSet rs)
            {
                if (mem[RowSet.RowOrder] is Domain ord)
                {
                    _cx.Add(rs);
                    rs = rs.Sort(_cx, ord, false);
                }
                if (mem[RowSetSection.Size] is int ct)
                {
                    _cx.Add(rs);
                    rs = new RowSetSection(_cx, rs, 0, ct);
                }
                _cx.result = rs.defpos;
                _cx.obs += (_cx.result, rs);
            }
            var aff = cx.db.AffCount(cx);
            if (aff > 0)
                _cx.result = -1L;
            else if (gDefs == CTree<long, TGParam>.Empty)
                _cx.result = TrueRowSet.OK(_cx).defpos;
            else if (cx.obs[_cx.result] is RowSet rrs)
                _cx.obs += (_cx.result, rrs);
            if (then != BList<long?>.Empty)
            {
                var ac = new Activation(cx, "" + defpos);
                ObeyList(then, ac);
                ac.SlideDown();
                cx.values += ac.values;
                cx.db = ac.db;
            }
            cx.SlideDown();
            _cx.db = cx.db;
            return _cx;
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
        internal abstract class Step
        {
            public MatchStatement ms; // all continuations know the MatchStatement
            protected Step(MatchStatement m)
            { ms = m; }
            /// <summary>
            /// The parameters are (maybe) a Step, and the current end state of the match 
            /// </summary>
            /// <param name="cx">The context</param>
            /// <param name="cn">A continuation if provided</param>
            /// <param name="tok">A token indicating the match state</param>
            /// <param name="pd">The current last matched node if any</param>
            public abstract void Next(Context cx, Step? cn, Sqlx tok, TNode? pd);
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
        internal class EndStep : Step
        {
            public EndStep(MatchStatement m) : base(m)
            { }
            /// <summary>
            /// On success we simply call AddRow, which does the work!
            /// </summary>
            public override void Next(Context cx, Step? cn, Sqlx tok, TNode? pd)
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
        internal class GraphStep : Step
        {
            internal readonly ABookmark<int, long?>? matchAlts; // the current place in the MatchStatement
            readonly Step next; // the continuation
            public GraphStep(ABookmark<int, long?>? graphs, Step n) : base(n.ms)
            { matchAlts = graphs; next = n; }
            /// <summary>
            /// On Success we go on to the next matchexpression if any.
            /// Otherwise we have succeeded and call next.Next.
            /// </summary>
            public override void Next(Context cx, Step? cn, Sqlx tok, TNode? pd)
            {
                if (cx.obs[matchAlts?.value() ?? -1L] is SqlMatch sm)
                {
                    for (var b = sm.matchAlts.First(); b != null; b = b.Next())
                        if (cx.obs[b.value() ?? -1L] is SqlMatchAlt sa)
                        {
                            cx.paths += (sa.defpos, new TPath(sa.defpos, cx));
                            ms.ExpNode(cx, new ExpStep(sa, sa.matchExps.First(),
                                new GraphStep(matchAlts?.Next(), next)), Sqlx.Null, null);
                        }
                }
                else
                    next.Next(cx, cn, tok, pd);
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
        /// SqlNode, SqlEdge, or SqlPath 
        /// (if ExpNode finds it is a SqlPath it sets up PathNode to follow it).
        /// </summary>
        internal class ExpStep : Step
        {
            public ABookmark<int, long?>? matches; // the current place in the matchExp
            public SqlMatchAlt alt; // the current match alternative
            public Step next; // the continuation
            public ExpStep(SqlMatchAlt sa, ABookmark<int, long?>? m, Step n)
                : base(n.ms)
            { alt = sa; matches = m; next = n; }
            /// <summary>
            /// On Success we go on to the next element in the match expression if any.
            /// Otherwise the expression has succeeded and we call next.Next.
            /// </summary>
            public override void Next(Context cx, Step? cn, Sqlx tok, TNode? pd)
            {
                if (matches != null)
                    ms.ExpNode(cx, new ExpStep(alt, matches, cn ?? next), tok, pd);
                else
                    next.Next(cx, null, Sqlx.WITH, pd);
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
        internal class PathStep : Step
        {
            public SqlMatchAlt alt; // the current match alternative
            public SqlPath sp; // the repeating pattern spec
            public CTree<long, TGParam> state = CTree<long, TGParam>.Empty;
            public int im; // the iteration count
            public Step next; // the continuation
            public PathStep(SqlMatchAlt sa, SqlPath s, int i, Step n) : base(n.ms)
            { alt = sa; sp = s; im = i; next = n; }
            /// <summary>
            /// The quantifier specifies minimum and maximum for the iteration count.
            /// Depending on this value we may recommence the pattern.
            /// When this is done (backtracking) we announce success of the repeating pattern
            /// and call next.Next()
            /// </summary>
            public override void Next(Context cx, Step? cn, Sqlx tok, TNode? pd)
            {
                var i = im + 1;
                // this is where we need to promote the local bindings to arrays
                var ls = CTree<long, TGParam>.Empty;
                for (var b = sp.pattern.First(); b != null; b = b.Next())
                    if (cx.obs[b.value() ?? -1L] is SqlNode n)
                        for (var c = n.state.First(); c != null; c = c.Next())
                            if (c.value().type != TGParam.Type.Path)
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
                    ms.PathNode(cx, new PathStep(alt, sp, i, next), pd);
                if (i >= sp.quantifier.Item1 && pd is not null)
                    next.Next(cx, cn, tok, pd);
            }
            public override Step? Cont => next;
            public override string ToString()
            {
                var sb = new StringBuilder((alt.mode == Sqlx.NONE) ? "Path" : alt.mode.ToString());
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
        internal class NodeStep : Step
        {
            public SqlMatchAlt alt;
            public ABookmark<long, TableRow>? nodes;
            public SqlNode xn;
            public Step next;
            public NodeStep(SqlMatchAlt sa, SqlNode x, ABookmark<long, TableRow>? no,
                Step n) : base(n.ms)
            { alt = sa; xn = x; nodes = no; next = n; }
 /*           public NodeStep(NodeStep s, ABookmark<long, TableRow>? ns) : base(s.ms)
            {
                alt = s.alt; nodes = ns; xn = s.xn; next = s.next;
            } */
            /// <summary>
            /// On each success we call the continuation
            /// </summary>
            public override void Next(Context cx, Step? cn, Sqlx tok, TNode? pd)
            {
                ms.ExpNode(cx, (ExpStep)next, tok, pd);
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
        /// add a row to the MatchStatement's rowSet.
        /// </summary>
        /// <param name="cx">The context</param>
        void AddRow(Context cx)
        {
            // everything has been checked
            if (flags == Flags.None)
                cx.val = TBool.True;
            if ((flags.HasFlag(Flags.Bindings)||flags.HasFlag(Flags.Schema))
                && cx.obs[bindings] is ExplicitRowSet ers
                && cx.obs[ers.index] is Index ex && ex.MakeKey(cx.binding) is CList<TypedValue> k
                && ex.rows?.Contains(k) != true)
            {
                var uid = cx.GetUid();
                ex += (k, uid);
                ers += (cx, ExplicitRowSet.ExplRows, ers.explRows + (uid, new TRow(ers, cx.binding)));
                cx.Add(ex); cx.Add(ers);
                cx.val = TNull.Value;
                cx.values += cx.binding;
                if (cx.obs[body] is Executable bd && cx.binding != CTree<long, TypedValue>.Empty)
                {
                    cx = bd.Obey(cx);
                    if (cx.val is TRow rr)
                    {
                        if (domain.defpos != bindings && cx.obs[domain.defpos] is ExplicitRowSet es)
                        {
                            var ur = cx.GetUid();
                            es += (cx, ExplicitRowSet.ExplRows, es.explRows + (ur, rr));
                            cx.obs += (es.defpos, es);
                        }
                        if (domain.defpos != bindings && cx.obs[domain.defpos] is SelectRowSet srs)
                        {
                            if (srs.aggs != CTree<long, bool>.Empty)
                            // This code is largely copied from SelectRowSet.Build
                            {
                                cx.values += rr.values;
                                if (!cx.funcs.Contains(defpos))
                                    cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty);
                                if (srs.groupings.Count == 0)
                                    for (var b0 = srs.aggs.First(); b0 != null; b0 = b0.Next())
                                    {
                                        if (cx.obs[b0.key()] is SqlFunction sf0)
                                            sf0.AddIn(TRow.Empty, cx);
                                    }
                                else for (var g = srs.groupings.First(); g != null; g = g.Next())
                                        if (g.value() is long p && cx.obs[p] is Grouping gg)
                                        {
                                            var vals = CTree<long, TypedValue>.Empty;
                                            for (var gb = gg.keys.First(); gb != null; gb = gb.Next())
                                                if (gb.value() is long gp && cx.obs[gp] is SqlValue v)
                                                    vals += (gp, v.Eval(cx));
                                            var key = new TRow(srs.groupCols, vals);
                                            for (var b1 = srs.aggs.First(); b1 != null; b1 = b1.Next())
                                                if (cx.obs[b1.key()] is SqlFunction sf1)
                                                    sf1.AddIn(key, cx);
                                        }
                            }
                        }
                    }
                    /*              if (flags.HasFlag(Flags.Body) && cx._Ob(body) is Executable e)
                                  {
                                      var ob = cx.binding;
                                      cx.nodes += gDefs;
                                      var ac = new Activation(cx, "" + defpos);
                                      ac.values += ob;
                                      e.Obey(ac);
                                      ac.SlideDown();
                                      cx.values += ac.values;
                                      cx.binding = ob;
                                      cx.db = ac.db;
                                  } */
                }
                else if (flags == Flags.None)
                    cx.val = TBool.True;
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
        void ExpNode(Context cx, ExpStep be, Sqlx tok, TNode? pd)
        {
            if (cx.obs[be.matches?.value() ?? -1L] is not SqlNode xn)
            {
                be.next.Next(cx, null, tok, pd);
                return;
            }
            cx.conn.Awake();
            var step = ++_step;
            if (xn is SqlPath sp && sp.pattern.First() is ABookmark<int, long?> ma && pd is not null)
            {
                // additions to the path binding happen in PathStep.Next()
                // here we just ensure that the required arrays have been initialised
                var pa = cx.paths[be.alt.defpos] ?? new TPath(be.alt.defpos, cx);
                pa = PathInit(cx, pa, ma, 0);
                cx.paths += (pa.matchAlt, pa);
                PathNode(cx, new PathStep(be.alt, sp, 0,
                    new ExpStep(be.alt, be.matches?.Next(), be.next)), pd);
            }
            var ds = BTree<long, TableRow>.Empty; // the set of database nodes that can match with xn
            // We have a current node xn, but no current dn yet. Initialise the set of possible d to empty. 
            if (tok == Sqlx.WITH && pd is not null)
                ds += (xn.defpos, pd.tableRow);
            //    else if (xn.Eval(cx) is TNode nn)
            //        ds += (xn.defpos, nn.tableRow);
            else
            if (pd is not null && pd.dataType is EdgeType pe && pd.defpos != pd.dataType.defpos
                && ((tok == Sqlx.ARROWBASE) ?
                (cx.db.objects[pe.arrivingType] as NodeType)?.GetS(cx, pd.tableRow.vals[pe.arriveCol] as TInt)
                : (cx.db.objects[pe.leavingType] as NodeType)?.GetS(cx, pd.tableRow.vals[pe.leaveCol] as TInt))// this node will match with xn
               is TableRow tn)
                ds += (tn.defpos, tn);
            else if (pd is not null && pd.defpos == pd.dataType.defpos) // schema case
            {
                if (pd.dataType is EdgeType et &&
                    cx.db.objects[(tok == Sqlx.ARROWBASE) ? et.leavingType : et.arrivingType] is NodeType pn
                    && pn.Schema(cx) is TableRow ts)
                    ds += (ts.defpos, ts);
                else if (pd.dataType is NodeType pg)
                {
                    for (var b = pg.sindexes.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is TableColumn tc && tc.toType == pd.defpos
                            && tc.flags.HasFlag((xn.tok == Sqlx.RARROW) ? PColumn.GraphFlags.ArriveCol : PColumn.GraphFlags.LeaveCol)
                            && (cx.db.objects[tc.tabledefpos] as EdgeType)?.Schema(cx) is TableRow tq)
                            ds += (tq.defpos, tq);
                    for (var b= pg.rindexes.First();b!=null;b=b.Next())
                        if ((cx.db.objects[b.key()] as EdgeType)?.Schema(cx) is TableRow tq)
                            ds += (tq.defpos, tq);
                }
            }
            else if (pd is not null && pd.dataType is NodeType pn) // an edge attached to the TNode pd
            {
                var ctr = CTree<Domain, int>.Empty;
                var tg = truncating != CTree<long, (int, Domain)>.Empty;
                // case 1: pn has a primary index
                for (var b = pn.rindexes.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is EdgeType rt)
                    {
                        if (pd.defpos == pd.dataType.defpos) // schmema flag
                        {
                            ds += (rt.defpos, rt.Schema(cx));
                            continue;
                        }
                        //          if (xn.domain.defpos >= 0 && xn.domain.name != rt.name)
                        //              continue;
                        var lm = truncating.Contains(rt.defpos) ? truncating[rt.defpos].Item1 : int.MaxValue;
                        var ic = (xn.tok == Sqlx.ARROWBASE) ? rt.leaveCol : rt.arriveCol;
                        var xp = (xn.tok == Sqlx.ARROWBASE) ? rt.leaveIx : rt.arriveIx;
                        for (var g = b.value().First(); g != null; g = g.Next())
                            if (g.key()[0] == ic)
                            {
                                if (cx.db.objects[xp] is Index rx
                                && pd.tableRow.vals[pn.idCol] is TInt ti
                                && rx.rows?.impl?[ti] is TPartial tp)
                                {
                                    if (lm < tp.value.Count && truncating[rt.defpos].Item2 is Domain dm
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
                var la = truncating.Contains(Domain.EdgeType.defpos) ? truncating[Domain.EdgeType.defpos].Item1 : int.MaxValue;
                for (var b = pn.sindexes.First(); b != null; b = b.Next())
                    if (cx._Ob(b.key()) is TableColumn tc
                        && cx._Ob(tc.tabledefpos) is EdgeType rt
                        && (xn.domain.defpos < 0 || xn.domain.defpos == rt.defpos)
                        && b.value() is CTree<long, CTree<long, bool>> pt) {
                        if (pd.defpos == pd.dataType.defpos)  // schema flag
                        {
                            ds += (rt.defpos, rt.Schema(cx));
                            continue;
                        }
                        var pv = pd.tableRow.defpos;
                        if (cx._Ob(tc.toType) is NodeType tt && pd.tableRow.vals[tt.idCol]?.ToLong() is long vp)
                            pv = vp;
                        if (pt[pv] is CTree<long, bool> ct)
                        {
                            var lm = truncating.Contains(rt.defpos) ? truncating[rt.defpos].Item1 : int.MaxValue;
                            for (var c = ct.First(); c != null && lm-- > 0 && la-- > 0; c = c.Next())
                                if (rt.tableRows[c.key()] is TableRow tr)
                                    ds += (tr.defpos, tr);
                            if (lm <= 0 || la <= 0)
                                break;
                        }
                    }
                alldone:;
            }
            else // use Label, Label expression, xn's domain, or all node/edge types, and the properties specified
                ds = (cx._Ob(xn.label) ?? xn.domain).For(cx, this, xn, ds);
            var df = ds.First();
            if (df != null)
                DbNode(cx, new NodeStep(be.alt, xn, df, new ExpStep(be.alt, be.matches?.Next(), be.next)),
                     (xn is SqlEdge && xn is not SqlPath) ? xn.tok : tok, pd);
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
            if (truncating.Contains(Domain.EdgeType.defpos) 
                && (ctr[Domain.EdgeType]>= truncating[Domain.EdgeType.defpos].Item1))
                    return true;
            return false;
        }
        bool Trunc1(CTree<Domain,int> ctr,Table st)
        {
            if (truncating.Contains(st.defpos) && ctr[st] >= truncating[st.defpos].Item1)
                return true;
            for (var b = st.super.First(); b != null; b = b.Next())
                if (b.key() is Table t)
                    return Trunc1(ctr, t);
            return false;
        }
        bool AllTrunc(CTree<Domain, int> ctr)
        {
            if (truncating.Contains(Domain.EdgeType.defpos)
                && (ctr[Domain.EdgeType] >= truncating[Domain.EdgeType.defpos].Item1))
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
        void DbNode(Context cx,NodeStep bn, Sqlx tok, TNode? pd)
        {
            int step;
            var ob = cx.binding;
            var pa = cx.paths[bn.alt.defpos];
            var ot = pa;
            TNode? dn = null;
            var ns = bn.nodes;
            while (ns is not null)
            {
                step = ++_step;
                if (ns?.value() is not TableRow tr)
                    goto backtrack;
                if (flags.HasFlag(Flags.Schema))
                {
                    for (var b = tr.tabledefpos.First(); b != null; b = b.Next())
                        if (cx._Ob(b.key()) is NodeType dm)
                        {
                            if (dm is EdgeType et && pd is not null &&
                                ((bn.xn.tok == Sqlx.ARROWBASE) ? et.leavingType : et.arrivingType) != pd.dataType.defpos)
                                continue;
                            cx.binding += (bn.xn.defpos, new TRow(dm, tr.vals));
                            dn = new TNode(dm, tr);
                            goto next;
                        }
                    goto backtrack;
                }
                else
                {
                    NodeType? dt = null;
                    for (var b = tr.tabledefpos.First(); b != null; b = b.Next())
                    {
                        dt = cx.db.objects[b.key()] as NodeType;
                        if (bn.xn.domain.defpos < 0)
                            break;
                        if (dt is not null && dt.EqualOrStrongSubtypeOf(bn.xn.domain))
                            break;
                    }
                    if (dt is null || (bn.xn.domain.defpos > 0 && !dt.EqualOrStrongSubtypeOf(bn.xn.domain)))
                        goto backtrack;
                    dn = (dt is EdgeType et) ? new TEdge(cx, et, tr) : new TNode(cx, dt, tr);
                    if (bn.alt.mode == Sqlx.TRAIL && dn is TEdge && pa?.Contains(dn) == true)
                        goto backtrack;
                    if (bn.alt.mode == Sqlx.ACYCLIC && pa?.Contains(dn) == true)
                        goto backtrack;
                    if ((bn.alt.mode == Sqlx.SHORTEST || bn.alt.mode == Sqlx.SHORTESTPATH)
                            && !Shortest(bn, cx))
                        goto backtrack;
                    if (pa is not null && dn != pd)
                        cx.paths += (bn.alt.defpos, pa + dn);
                    DoBindings(cx, bn.alt.defpos, bn.xn, dn);
                    if (!bn.xn.CheckProps(cx, dn))
                        goto backtrack;
                    if (dn is TEdge de && pd is not null
                        && ((bn.xn.tok == Sqlx.ARROWBASE) ? de.leaving : de.arriving).CompareTo(pd.id) != 0)
                        goto backtrack;
                }
            next:
                bn.next.Next(cx, null, (tok == Sqlx.WITH) ? Sqlx.Null : tok, dn);
            backtrack:
                if (ot is not null)
                    cx.paths += (bn.alt.defpos, ot);
                cx.binding = ob; // unbind all the bindings from this recursion step
                ns = ns?.Next();
            }
            if (ot is not null)
                cx.paths += (bn.alt.defpos,ot);
            cx.binding = ob;
        }
        static TPath PathInit(Context cx, TPath pa, ABookmark<int, long?> ma, int d)
        {
            for (var c = ma; c != null; c = c.Next())
                if (cx.obs[c.value() ?? -1L] is SqlNode cn)
                {
                    if (cn is SqlPath sp && sp.pattern.First() is ABookmark<int, long?> sm)
                        pa = PathInit(cx, pa, sm, d + 1);
                    else
                        for (var b = cn.state.First(); b != null; b = b.Next())
                            if (b.value() is TGParam tg && b.key() >= 0L && cx.obs[tg.uid] is SqlValue sv
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
            if (cx.obs[bp.sp.pattern.First()?.value() ?? -1] is not SqlNode xi || !xi.CheckProps(cx, dn))
                goto backtrack;
            DoBindings(cx, bp.alt.defpos, xi, dn);
            if (bp.sp.pattern.First() is not ABookmark<int, long?> fn) goto backtrack;
            if (cx.obs[fn.value() ?? -1L] is SqlNode xn && bp.sp is SqlPath sp
                && (bp.im < sp.quantifier.Item2 || sp.quantifier.Item2<0))
                ExpNode(cx, new ExpStep(bp.alt,fn.Next(),bp), xn.tok, dn); // use ordinary ExpNode for the internal pattern
                                                                // dn must be an edge
                                                                // and xn must be an SqlEdge
        backtrack:
            if (ot is not null)
                cx.paths += (bp.alt.defpos,ot);
            cx.binding = ob; // unbind all the bindings from this recursion step
        }
        /// <summary>
        /// When we match a database node we process the binding information in the SqlNode:
        /// All local bindings are simple values at this stage.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ap">The current alt defpos</param>
        /// <param name="xn">The SqlNode or SqlEdge or SqlPath</param>
        /// <param name="dn">The current database node (TNode or TEdge)</param>
        void DoBindings(Context cx, long ap, SqlNode xn, TNode dn)
        {
            if (xn.state != CTree<long, TGParam>.Empty && dn.dataType is NodeType nt)
            {
                var bi = cx.binding;
                if (nt.infos[cx.role.defpos]?.names is BTree<string, (int, long?)> ns)
                    for (var b = xn.docValue?.First(); b != null; b = b.Next())
                        if (gDefs[b.value() ?? -1L] is TGParam tg
                            && cx.GName(b.key()) is string n
                            && ns?[n].Item2 is long np
                            && dn.tableRow.vals[np] is TypedValue tv)
                        {
                            if (tg.type.HasFlag(TGParam.Type.Group))
                            {
                                var ta = bi[tg.uid] as TArray ?? new TArray(tv.dataType);
                                bi += (tg.uid, ta + (ta.Length, tv));
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
                            case -(int)Sqlx.Id: tv = dn; break;
                            case -(int)Sqlx.RARROW:
                                {
                                    var te = cx.GType(nt.leavingType);
                                    var er = te?.Get(cx, dn.tableRow.vals[nt.leaveCol] as TInt);
                                    tv = (te is null || er is null) ? TNull.Value : new TNode(cx, te, er);
                                    break;
                                }
                            case -(int)Sqlx.ARROW:
                                {
                                    var te = cx.GType(nt.arrivingType);
                                    var er = te?.Get(cx, dn.tableRow.vals[nt.arriveCol] as TInt);
                                    tv = (te is null || er is null) ? TNull.Value : new TNode(cx, te, er);
                                    break;
                                }
                            case -(int)Sqlx.TYPE: tv = new TChar(nt.name); break;
                        }
                        if (tv != TNull.Value)
                        {
                            if (tg.type.HasFlag(TGParam.Type.Group))
                            {
                                var ta = bi[tg.uid] as TArray ?? new TArray(tv.dataType);
                                bi += (tg.uid, ta + (ta.Length, tv));
                            }
                            else
                                bi += (tg.uid, tv);
                        }
                    }
                }
                cx.binding = bi;
            }
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
            if (cx.obs[gb?.value() ?? -1L] is SqlMatchAlt sm
                   && (sm.mode == Sqlx.SHORTEST || sm.mode == Sqlx.SHORTESTPATH)
                   && sm.pathId >= 0
                   && cx.obs[cx.result] is ExplicitRowSet ers)
            {
                var rws = ers.explRows;
                if (rws.Length == 0) return true;
                var (ol,ov) = rws[ers.Length - 1];
                var op = ov[sm.pathId] as TList;
                var cp = cx.paths[defpos]??throw new PEException("PE030803");
                if (op is null)
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
        protected override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (SqlValueExpr)base._Replace(cx, was, now);
            var ch = false;
            var ls = BList<long?>.Empty;
            for (var b=matchList.First();b!=null;b=b.Next())
                if (cx.obs[b.value()??-1L] is SqlMatchAlt sa)
                {
                    var a = sa.Replace(cx, was, now);
                    if (a != sa)
                        ch = true;
                    ls += a.defpos;
                }
            var tg = BTree<long, (int, Domain)>.Empty;
            for (var b=truncating.First();b!=null;b=b.Next())
            {
                var (i, d) = b.value();
                var k = b.key();
                long nk = k;
                if (cx.obs[k] is EdgeType)
                    nk = cx.done[k]?.defpos ?? k;
                var nd = cx.done[d.defpos] as Domain?? d;
                if (k != nk || d.defpos != nd.defpos)
                    ch = true;
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