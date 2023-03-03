using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Security.AccessControl;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// IMMUTABLE SHAREABLE
    /// Executable statements can be used in stored procedures. 
    /// This class provides machinery to support control statements in stored procedures.
    /// Control statements (blocks) generally ovveride this basic infrastructure.
    /// The behaviour of condition handlers and loops requires some infrastructure here.
    /// By default a compound statement executes its list of statements in sequence. But this ordering can be disturbed,
    /// and the transaction.breakto if not null will pop blocks off the stack until we catch the break.
    /// For example RETURN will set breakto to the previous stack (dynLink), as will the execution of an EXIT handler.
    /// CONTINUE will set breakto to the end of the enlcosing looping construct.
    /// BREAK will set breakto to just outside the current looping construct.
    /// BREAK nnn will set breakto just outside the named looping construct.
    /// An UNDO handler will restore the state to the defining point of the handler, and then EXIT.
    /// </summary>
	internal abstract class Executable : DBObject
	{
        internal const long
            Label = -92, // string
            Stmt = -93; // string
        public string? stmt => (string?)mem[Stmt];
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal string? label => (string?)mem[Label];
        protected Executable(long dp, BTree<long, object>? m=null) 
            : base(dp, m??BTree<long, object>.Empty) { }
        /// <summary>
        /// Support execution of a list of Executables, in an Activation.
        /// With break behaviour
        /// </summary>
        /// <param name="e">The Executables</param>
        /// <param name="tr">The transaction</param>
		protected Context ObeyList(BList<long?> e, Context cx)
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
        /// </summary>
        /// <param name="cx">The context</param>
		public virtual Context Obey(Context cx)
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
            if (mem.Contains(Label)) { sb.Append(" "); sb.Append(label); }
            if (mem.Contains(ObInfo.Name)) { sb.Append(" "); sb.Append(mem[ObInfo.Name]); }
            sb.Append(" ");sb.Append(Uid(defpos));
            if (mem.Contains(Stmt)) { sb.Append(" Stmt:"); sb.Append(stmt); }
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class RollbackStatement : Executable
    {
        public RollbackStatement(long dp) : base(dp) { }
        public override Context Obey(Context cx)
        {
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SelectStatement : Executable
    {
        internal const long
            SourceSQL = -195, //string
            Union = -196; // long RowSet
        /// <summary>
        /// The source string
        /// </summary>
        public string? _source => (string?)mem[SourceSQL];
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
        public static SelectStatement operator+(SelectStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SelectStatement)s.New(s.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var nu = ((RowSet?)cx.obs[union])?.Replace(cx, so, sv)?.defpos;
            if (nu!=union && nu!=null)
                r += (Union, nu);
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class CompoundStatement : Executable
    {
        internal const long
             Stms = -96; // BList<long?> Executable
        /// <summary>
        /// The contained list of Executables
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
        public static CompoundStatement operator +(CompoundStatement c, (long, object) x)
        {
            var (dp, ob) = x;
            if (c.mem[dp] == ob)
                return c;
            return new CompoundStatement(c.defpos, c.mem + x);
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
            var act = new Activation(cx, label ?? "");
            act = (Activation)ObeyList(stms, act);
            if (act.signal != null)
                act.signal.Throw(cx);
            return act.SlideDown();
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long,object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ns = cx.ReplacedLl(stms);
            if (ns!=stms)
                r += (Stms, ns);
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
            sb.Append(")");
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
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
        public static PreparedStatement operator+(PreparedStatement pr,(long,object)x)
        {
            var (dp, ob) = x;
            if (pr.mem[dp] == ob)
                return pr;
            return (PreparedStatement)pr.New(pr.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var nq = cx.ReplacedLl(qMarks);
            if (nq!=qMarks)
                r += (QMarks,nq);
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
    /// // shareable as of 26 April 2021
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
        public LocalVariableDec(long dp, Context cx, SqlValue v, BTree<long,object>?m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Label, v.name??"")
          + (AssignmentStatement.Vbl, v.defpos)+(_Domain,v.domain))
        { }
        protected LocalVariableDec(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LocalVariableDec operator+(LocalVariableDec s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (LocalVariableDec)s.New(s.mem + x);
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
        internal override TypedValue Eval(Context cx)
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
            var dm = cx._Dom(vb) ?? throw new DBException("PE1102");
            TypedValue tv = cx.obs[init]?.Eval(cx)??dm.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.AddValue(vb, tv); // We expect a==cx, but if not, tv will be copied to a later
            return cx;
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ni = ((SqlValue?)cx.obs[init])?.Replace(cx,so,sv)?.defpos ;
            if (ni!=init && ni!=null)
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
    /// // shareable as of 26 April 2021
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
            : base(vp,new BTree<long, object>(ParamMode, m)+(ObInfo.Name,n)+(AssignmentStatement.Val,vp)
                  +(_Domain,dt.defpos))
        { }
        protected FormalParameter(long dp,BTree<long, object> m) : base(dp,m) { }
        public static FormalParameter operator +(FormalParameter s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new FormalParameter(s.defpos,s.mem + x);
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
        internal override TypedValue Eval(Context cx)
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
            if (mem.Contains(ParamMode)) { sb.Append(" "); sb.Append(paramMode); }
            if (mem.Contains(Result)) { sb.Append(" "); sb.Append(result); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A local cursor
    /// // shareable as of 26 April 2021
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
        public CursorDeclaration(long dp, Context cx,SqlCursor sc,RowSet c) 
            : base(dp,cx,sc,new BTree<long,object>(CS,c.defpos)
                  +(_Domain,c.domain)) 
        { }
        protected CursorDeclaration(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CursorDeclaration operator+(CursorDeclaration c,(long,object)x)
        {
            var (dp, ob) = x;
            if (c.mem[dp] == ob)
                return c;
            return (CursorDeclaration)c.New(c.mem + x);
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
            if (cu!=null)
                cx.AddValue(cu, cu.Eval(cx));
            return cx;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var nc = ((SqlValue?)cx.obs[cs])?.Replace(cx, so, sv)?.defpos;
            if (nc != cs && nc!=null)
                r += (CS, nc);
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
    /// // shareable as of 26 April 2021
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
        /// A list of condition names, SQLSTATE codes, "SQLEXCEPTION", "SQLWARNING", or "NOT_FOUND"
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
        public static HandlerStatement operator+(HandlerStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new HandlerStatement(s.defpos, s.mem + x);
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
            for (var c =conds.First();c!=null;c=c.Next())
                if (c.value() is string h)
				a.exceptions+=(h,new Handler(this,a));
            return cx;
		}
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[action]?.Calls(defpos, cx)??false;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var na = ((Executable?)cx.obs[action])?.Replace(cx, so, sv)?.defpos;
            if (na != action && na!=null)
                r += (Action, na);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ");sb.Append(htype.ToString());
            for (var b=conds.First();b!=null;b=b.Next())
            {
                sb.Append(" "); sb.Append(b.value());
            }
            sb.Append(" Action=");sb.Append(Uid(action));
            return sb.ToString();
        }

    }
    /// <summary>
    /// A Handler helps implementation of exception handling
    /// // shareable as of 26 April 2021
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
        public static Handler operator+(Handler h,(long,object)x)
        {
            var (dp, ob) = x;
            if (h.mem[dp] == ob)
                return h;
            return new Handler(h.defpos, h.mem + x);
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
                if (a.signal != null)
                    a.signal.Throw(cx);
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
    /// // shareable as of 26 April 2021
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
        public static BreakStatement operator+(BreakStatement b,(long,object)x)
        {
            var (dp, ob) = x;
            if (b.mem[dp] == ob)
                return b;
            return new BreakStatement(b.defpos, b.mem + x);
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
    /// // shareable as of 26 April 2021
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
                  +(Dependents,CTree<long,bool>.Empty+(vb.defpos,true)+(va.defpos,true))
                  +(_Depth,Math.Max(va.depth+1,vb.depth+1)))
        { }
        protected AssignmentStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static AssignmentStatement operator+(AssignmentStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new AssignmentStatement(s.defpos, s.mem + x);
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
            var vb = cx.obs[vbl] ?? (DBObject?) cx.db.objects[vbl];
            var dm = cx._Dom(vb);
            if (vb!=null && dm != null && cx.obs[val] is DBObject va && va.Eval(cx) is TypedValue tv)
            {
                var v = dm.Coerce(cx,tv);
                cx.values += (vbl, v);
            }
            return cx;
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var nb = ((SqlValue?)cx.obs[vbl])?.Replace(cx, so, sv)?.defpos;
            if (nb != vbl && nb!=null)
                r += (Vbl, nb);
            var na = ((SqlValue?)cx.obs[val])?.Replace(cx, so, sv)?.defpos;
            if (na != val && na!=null)
                r += (Val, na);  
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (vbl != -1L && val != -1L)
            {
                sb.Append(" "); sb.Append(Uid(vbl)); 
                sb.Append("="); sb.Append(Uid(val)); 
            } 
            return sb.ToString();
        }
	}
    /// <summary>
    /// A multiple assignment statement for a stored procedure.
    /// The right hand side must be row valued, and the left hand side is a
    /// list of variable identifiers.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class MultipleAssignment : Executable
    {
        internal const long
            LhsType = -107, // long Domain
            List = -108, // BList<long?> SqlValue
            Rhs = -109; // long SqlValue
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal BList<long?> list => (BList<long?>?)mem[List]??BList<long?>.Empty;
        /// <summary>
        /// The row type of the lefthand side, used to coerce the given value 
        /// </summary>
        long lhsType => (long)(mem[LhsType]??-1L);
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
            var d = rg.depth + 1;
            var dm = cx._Dom(rg)??Domain.Null;
            var r = new BTree<long,object>(Rhs,rg.defpos);
            var ls = BList<long?>.Empty;
            for (var b = lh.First(); b != null; b = b.Next())
                if (b.value() is Ident id && cx.obs[id.iix.dp] is SqlValue v
                            && cx._Dom(v) is Domain vd)
                {
                    dm = dm.Constrain(cx, id.iix.lp, vd);
                    d = Math.Max(d, v.depth + 1);
                    ls += v.defpos;
                }
            return r + (_Depth,d)+(List,ls)+(LhsType,dm.defpos);
        }
        public static MultipleAssignment operator+(MultipleAssignment s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new MultipleAssignment(s.defpos, s.mem + x);
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
            var nt = cx.Fix(lhsType);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var nd = ((Domain?)cx.obs[lhsType])?.Replace(cx, so, sv)?.defpos;
            if (nd != lhsType && nd!=null)
                r += (LhsType, nd);
            var nl = cx.ReplacedLl(list);
            if (nl != list)
                r += (List, nl);
            var na = ((SqlValue?)cx.obs[rhs])?.Replace(cx, so, sv)?.defpos;
            if (na != rhs && na!=null)
                r += (Rhs, na);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" LhsType: "); sb.Append(Uid(lhsType));
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
    /// A return statement for a stored procedure or function
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class ReturnStatement : Executable
    {
        internal const long
            Ret = -110; // long
        /// <summary>
        /// The return value
        /// </summary>
		public long ret => (long)(mem[Ret] ?? -1L);
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(long dp,SqlValue v) : base(dp, 
            new BTree<long, object>(Ret,v.defpos)+(_Depth,v.depth+1))
        { }
        protected ReturnStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ReturnStatement operator +(ReturnStatement s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new ReturnStatement(s.defpos, s.mem + x);
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
			a.val = cx.obs[ret]?.Eval(cx)??TNull.Value;
            cx = a.SlideDown();
            return cx;
		}
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var nr = ((SqlValue?)cx.obs[ret])?.Replace(cx, so, sv)?.defpos;
            if (nr != (cx.done[ret]?.defpos ?? ret) && nr!=null)
                r += (Ret,nr);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" -> ");sb.Append(Uid(ret));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Case statement for a stored procedure
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        internal const long
            Else = -111, // BList<long?> Executable
            _Operand = -112, // long SqlValue
            Whens = -113; // BList<long?> WhenPart
        /// <summary>
        /// The test expression
        /// </summary>
        public long operand => (long)(mem[_Operand]??-1L);
        /// <summary>
        /// A list of when parts
        /// </summary>
        public BList<long?> whens => (BList<long?>?)mem[Whens]?? BList<long?>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
        public BList<long?> els => (BList<long?>?)mem[Else]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a case statement from the parser
        /// </summary>
        public SimpleCaseStatement(long dp,Context cx,SqlValue op,BList<WhenPart> ws,
            BList<long?> ss) : 
            base(dp,_Mem(cx,op,ws,ss))
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, SqlValue op, BList<WhenPart> ws,
            BList<long?> ss)
        {
            var r = new BTree<long, object>(_Operand, op.defpos);
            var d = op.depth + 1;
            var wl = BList<long?>.Empty;
            for (var b = ws.First(); b != null; b = b.Next())
                if (b.value() is WhenPart w)
                {
                    d = Math.Max(d, w.depth + 1);
                    wl += w.defpos;
                }
            for (var b = ss.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                    d = Math.Max(d, ob.depth + 1);
            r += (Else, ss);
            return r + (Whens, wl) + (_Depth, d);
        }
        public static SimpleCaseStatement operator+(SimpleCaseStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SimpleCaseStatement(s.defpos, s.mem + x);
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
            r += (Else, ne);
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
            for(var c = whens.First();c!=null; c=c.Next())
                if (cx.obs[operand] is SqlValue sv && sv.Matches(cx)==true
                    && c.value() is long p && cx.obs[p] is WhenPart w)
                    return ObeyList(w.stms, cx);
            return ObeyList(els, cx);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var no = ((SqlValue?)cx.obs[operand])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[operand]?.defpos ?? operand) && no!=null)
                r += (_Operand, no);
            var nw = cx.ReplacedLl(whens);
            if (nw != whens)
                r += (Whens, nw);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r += (Else, ne);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class SearchedCaseStatement : Executable
	{
        /// <summary>
        /// A list of when parts
        /// </summary>
		public BList<long?> whens => (BList<long?>?)mem[SimpleCaseStatement.Whens]??BList<long?>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
		public BList<long?> els => (BList<long?>?)mem[SimpleCaseStatement.Else]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(long dp,Context cx,BList<WhenPart>ws,BList<long?>ss) 
            : base(dp,_Mem(cx,ws,ss))
        {  }
        protected SearchedCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,BList<WhenPart>ws,BList<long?>ss)
        {
            var d = 1;
            var r = BTree<long, object>.Empty;
            if (ss != BList<long?>.Empty)
            {
                for (var b = ss.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is DBObject ob)
                        d = Math.Max(d, ob.depth + 1);
                r += (SimpleCaseStatement.Else, ss);
            }
            var wl = BList<long?>.Empty;
            for (var b = ws.First(); b != null; b = b.Next())
                if (b.value() is WhenPart w)
                {
                    d = Math.Max(d, w.depth + 1);
                    wl += w.defpos;
                }
            return r + (SimpleCaseStatement.Whens,wl)+(_Depth, d);
        }
        public static SearchedCaseStatement operator+(SearchedCaseStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SearchedCaseStatement(s.defpos, s.mem + x);
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
                r += (SimpleCaseStatement.Else, ne);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var nw = cx.ReplacedLl(whens);
            if (nw != whens)
                r += (SimpleCaseStatement.Whens, nw);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r += (SimpleCaseStatement.Else, ne);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class WhenPart :Executable
	{
        internal const long
            Cond = -114, // long SqlValue
            Stms = -115; // BList<long?> Executable
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public long cond => (long)(mem[Cond]??-1L);
        /// <summary>
        /// a list of statements
        /// </summary>
		public BList<long?> stms =>(BList<long?>?)mem[Stms]?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A list of statements for this when</param>
        public WhenPart(long dp,SqlValue v, BList<long?> s) 
            : base(dp, BTree<long, object>.Empty+(Cond,v.defpos)+(Stms,s))
        { }
        protected WhenPart(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhenPart operator+(WhenPart s,(long,object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new WhenPart(s.defpos, s.mem + x);
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
                r += (Stms, ns);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object> m)
        {
            var r = base._Replace(cx, so, sv, m);
            var no = ((SqlValue?)cx.obs[cond])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cond]?.defpos ?? cond) && no!=null)
                r += (Cond, no);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r += (Stms, nw);
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
            sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// An if-then-else statement for a stored proc/func
    /// // shareable as of 26 April 2021
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
		public IfThenElse(long dp,Context cx,SqlValue se,BList<long?>th,BList<long?>ei,BList<long?> el) 
            : base(dp,_Mem(cx,se,th,ei,el))
		{}
        protected IfThenElse(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue se, BList<long?> th, BList<long?> ei,
            BList<long?> el)
        {
            var r = new BTree<long, object>(Search,se.defpos) +(Then,th) +(Elsif,ei)
                + (Else,el);
            var d = se.depth + 1;
            for (var b = th.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                d = Math.Max(d, ob.depth + 1);
            for (var b = ei.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                    d = Math.Max(d, ob.depth + 1); 
            for (var b = el.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                    d = Math.Max(d, ob.depth + 1); 
            return r + (_Depth,d);
        }
        public static IfThenElse operator+(IfThenElse s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new IfThenElse(s.defpos, s.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no!=null)
                r += (Search, no);
            var nt = cx.ReplacedLl(then);
            if (nt != then)
                r += (Then, nt);
            var ni = cx.ReplacedLl(elsif);
            if (ni != elsif)
                r += (Elsif, ni);
            var ne = cx.ReplacedLl(els);
            if (ne != els)
                r += (Else, ne);
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
    // shareable as of 26 April 2021
    internal class XmlNameSpaces : Executable
    {
        internal const long
            Nsps = -120; // CTree<string,string>
        /// <summary>
        /// A list of namespaces to be added
        /// </summary>
        public CTree<string, string> nsps => (CTree<string,string>?)mem[Nsps]
            ??CTree<string,string>.Empty;
        /// <summary>
        /// Constructor
        /// </summary>
        public XmlNameSpaces(long dp) : base(dp,BTree<long, object>.Empty) { }
        protected XmlNameSpaces(long dp, BTree<long, object> m) : base(dp, m) { }
        public static XmlNameSpaces operator+(XmlNameSpaces s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new XmlNameSpaces(s.defpos, s.mem + x);
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
                sb.Append(b.key()); sb.Append("=");
                sb.Append(b.value());
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A while statement for a stored proc/func
    /// // shareable as of 26 April 2021
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
        public static WhileStatement operator+(WhileStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new WhileStatement(s.defpos, s.mem + x);
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
                var lp = new Activation(cx,label??"");
                lp.cont = a;
                lp.brk = a;
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no!=null)
                r += (Search, no);
            var nw = cx.ReplacedLl(what);
            if (nw != what)
                r += (What, nw);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class RepeatStatement : Executable
	{
        /// <summary>
        /// The condition for stopping
        /// </summary>
		public long search => (long)(mem[WhileStatement.Search]??-1L);
        /// <summary>
        /// The list of statements to execute at least once
        /// </summary>
		public BList<long?> what => (BList<long?>?)mem[WhileStatement.What]??BList<long?>.Empty;
         /// <summary>
        /// Constructor: a repeat statement from the parser
        /// </summary>
        /// <param name="n">The label</param>
        public RepeatStatement(long dp,string n) : base(dp,new BTree<long,object>(Label,n)) 
        {  }
        protected RepeatStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RepeatStatement operator+(RepeatStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new RepeatStatement(s.defpos, s.mem + x);
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
            Context na = act;
            for (; ;)
            {
                na = ObeyList(what, act);
                if (na != act)
                    break;
                act.signal?.Throw(act);
                if (((SqlValue?)cx.obs[search])?.Matches(act)!=false)
                    break;
            }
            cx = act.SlideDown(); 
            return cx;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var no = ((SqlValue?)cx.obs[search])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[search]?.defpos ?? search) && no!=null)
                r += (WhileStatement.Search, no);
            var nw = cx.ReplacedLl(what);
            if (nw != what)
                r += (WhileStatement.What, nw);
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
    /// // shareable as of 26 April 2021
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
        public static IterateStatement operator+(IterateStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new IterateStatement(s.defpos, s.mem + x);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class LoopStatement : Executable
	{
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<long?> stms => (BList<long?>?)mem[WhenPart.Stms]??BList<long?>.Empty;
        /// <summary>
        /// Constructor: a loop statement from the parser
        /// </summary>
        /// <param name="s">The statements</param>
        /// <param name="n">The loop identifier</param>
		public LoopStatement(long dp,string n,long i):base(dp,new BTree<long,object>(Label,n))
		{ }
        protected LoopStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LoopStatement operator+(LoopStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new LoopStatement(s.defpos, s.mem + x);
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
                r += (WhenPart.Stms, ns);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r += (WhenPart.Stms, nw);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        internal const long
            ForVn = -125, // string
            Sel = -127, // long RowSet
            Stms = -128; // BList<long?> Executable
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
		public BList<long?> stms => (BList<long?>?)mem[Stms]??BList<long?>.Empty;
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
        public ForSelectStatement(long dp, Context cx, string n,Ident vn, 
            RowSet rs,BList<long?>ss ) 
            : base(dp,_Mem(cx,rs,ss) +(Label,n)+(ForVn,vn.ident))
		{ }
        protected ForSelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,RowSet rs,BList<long?>ss)
        {
            var r = BTree<long, object>.Empty;
            var d = rs.depth+1;
            for (var b = ss.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                d = Math.Max(d, ob.depth + 1);
            return r + (Sel,rs.defpos) + (Stms,ss) + (_Depth, d);
        }
        public static ForSelectStatement operator+(ForSelectStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new ForSelectStatement(s.defpos, s.mem + x);
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
                r += (Stms, nn);
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
                    if (ac.signal != null)
                        ac.signal.Throw(cx);
                }
                return ac.SlideDown();
            }
            return cx;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var no = ((RowSet?)cx.obs[sel])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[sel]?.defpos??sel) && no!=null)
                r += (Sel, no);
            var nw = cx.ReplacedLl(stms);
            if (nw != stms)
                r += (Stms, nw);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class OpenStatement : Executable
	{
        long cursor => (long)(mem[FetchStatement.Cursor]??-1L);
        /// <summary>
        /// Constructor: an open statement from the parser
        /// </summary>
        /// <param name="n">the cursor name</param>
		public OpenStatement(long dp, SqlCursor c,long i) : base(dp,BTree<long, object>.Empty
            +(FetchStatement.Cursor,c.defpos))
		{ }
        protected OpenStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static OpenStatement operator+(OpenStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new OpenStatement(s.defpos, s.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var no = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cursor]?.defpos ?? cursor) && no!=null)
                r += (FetchStatement.Cursor, no);
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class CloseStatement : Executable
	{
        public long cursor => (long)(mem[FetchStatement.Cursor]??-1L);
        /// <summary>
        /// Constructor: a close statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
		public CloseStatement(long dp, SqlCursor c,long i) : base(dp,BTree<long, object>.Empty
            +(FetchStatement.Cursor,c.defpos)+(_Domain,c.domain))
		{ }
        protected CloseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CloseStatement operator+(CloseStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new CloseStatement(s.defpos, s.mem + x);
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
            cx.Add(new EmptyRowSet(defpos,cx,domain));
            return cx;
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long,object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var no = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (no != (cx.done[cursor]?.defpos ?? cursor) && no!=null)
                r += (FetchStatement.Cursor, no);
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
    /// // shareable as of 26 April 2021
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
        /// The list of assignable expressions to receive values
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
        public static FetchStatement operator+(FetchStatement s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new FetchStatement(s.defpos, s.mem + x);
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
            if (cx.obs[cursor] is SqlCursor cu && cx.obs[cu.spec] is RowSet cs
                && cx._Dom(cs) is Domain cd)
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
                        rqpos = rqpos - 2;
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
                        rqpos = rqpos + (cx.obs[where]?.Eval(cx)?.ToLong()??-1L);
                        break;
                }
                if (rb == null || rqpos == 0)
                    rb = ((RowSet?)cx.obs[cs.defpos])?.First(cx);
                while (rb != null && rqpos != rb._pos)
                    rb = rb.Next(cx);
                if (rb != null)
                {
                    cx.values += (cu.defpos, rb);
                    for (int i = 0; i < cd.rowType.Length; i++)
                        if (rb[i] is TypedValue c && outs[i] is long ou 
                            &&  cx.obs[ou] is SqlValue sv)
                                cx.AddValue(sv, c);
 
                }
                else
                    cx = new Signal(defpos, "02000", "No obs").Obey(cx);
            }
            return cx;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var nc = ((SqlValue?)cx.obs[cursor])?.Replace(cx, so, sv)?.defpos;
            if (nc != (cx.done[cursor]?.defpos??cursor) && nc!=null)
                r += (Cursor, nc);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r += (Outs, no);
            var nw = ((SqlValue?)cx.obs[where])?.Replace(cx, so, sv)?.defpos;
            if (nw != (cx.done[where]?.defpos ?? where) && nw!=null)
                r += (Where, nw);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (how != Sqlx.ALL)
            { sb.Append(" "); sb.Append(how); }
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
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class CallStatement : Executable
    {
        internal const long
            Parms = -133, // BList<long?> SqlValue
            ProcDefPos = -134, // long Procedure
            Var = -135; // long SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public long var => (long)(mem[Var] ?? -1L);
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public long procdefpos => (long)(mem[ProcDefPos]??-1L);
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public BList<long?> parms =>
            (BList<long?>?)mem[Parms] ?? BList<long?>.Empty;
        public CTree<long,bool> aggs =>
            (CTree<long,bool>?)mem[Domain.Aggs]??CTree<long,bool>.Empty;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long lp, Context cx,Procedure pr, string pn, 
            BList<long?> acts, SqlValue? tg = null)
         : this(lp,cx, (Procedure)cx.Add(pr.Instance(lp,cx)), pn, acts, 
               (tg == null) ? null : 
               new BTree<long, object>(Var, tg.defpos) + (_Domain,pr.domain))
        { }
        public CallStatement(long lp, Context cx, string pn, BList<long?> acts, SqlValue? tg = null)
         : this(lp, cx, null, pn, acts,
               (tg == null) ? null :
               new BTree<long, object>(Var, tg.defpos) + (_Domain, Domain.Content.defpos))
        { }
        protected CallStatement(long dp, Context cx,Procedure? pr, string pn, BList<long?> acts, 
            BTree<long, object>? m = null)
         : base(dp, _Mem(cx,pr,acts,m) + (Parms, acts) + (ObInfo.Name, pn))
        { }
        protected CallStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, Procedure? pr, BList<long?> acts,
            BTree<long, object>? m)
        {
            m = m ?? BTree<long, object>.Empty;
            var dm = Domain.Content;
            if (pr != null)
            {
                m += (ProcDefPos, pr.defpos);
                dm = cx._Dom(pr) ?? Domain.Content;
            }
            var ag = CTree<long, bool>.Empty;
            for (var b = acts.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue pa)
                    ag += pa.IsAggregation(cx);
            if (ag != dm.aggs)
            {
                dm += (Domain.Aggs, ag);
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static CallStatement operator +(CallStatement s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new CallStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CallStatement(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new CallStatement(dp,m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var np = cx.Fix(procdefpos);
            if (np != procdefpos)
                r += (ProcDefPos, np);
            var ns = cx.FixLl(parms);
            if (parms != ns)
                r += (Parms, ns);
            var va = cx.Fix(var);
            if (var != va)
                r += (Var, va);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (var != -1L && (cx.obs[var]?.Calls(defpos, cx)??false))
                return true;
            return procdefpos == defpos || Calls(parms, defpos, cx);
        }
        /// <summary>
        /// Execute a proc/method call
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var proc = (Procedure)(cx.db.objects[procdefpos]??throw new DBException("42108"));
            var ac = new Context(cx, cx._Ob(proc.definer) as Role??throw new DBException("42108"), 
                cx.user??throw new DBException("42108"));
            var a = proc?.Exec(ac, parms) ?? cx;
            return a.SlideDown();
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var nv = cx.ObReplace(var, so, sv);
            if (nv != var)
                r += (Var, nv);
            var np = parms;
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var a = cx.ObReplace(p, so, sv);
                    if (a != b.value())
                        np += (b.key(), a);
                }
            if (np != parms)
                r += (Parms, np);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (var != -1L)
            {
                sb.Append(" Var="); sb.Append(Uid(var));
            }
            sb.Append(" "); sb.Append(name);
            sb.Append(" "); sb.Append(Uid(procdefpos));
            sb.Append(" (");
            var cm = "";
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(")");
            return sb.ToString();
        }

    }
    /// <summary>
    /// A signal statement for a stored proc/func
    /// shareable as of 26 April 2021
    /// </summary>
    internal class SignalStatement : Executable
    {
        internal const long
            Objects = -137, // BList<string>
            _Signal = -138, // string
            SetList = -139, // CTree<Sqlx,long?>
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
        public static SignalStatement operator +(SignalStatement s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SignalStatement(s.defpos, s.mem + x);
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
            if (signal != null && cx.db!=null)
            {
                string? sclass = signal.Substring(0, 2);
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
                    if (h == null)
                        h = cs.exceptions["SQLEXCEPTION"];
                    if (h == null)
                    {
                        var c = cs.next;
                        while (c != null && !(c is Activation))
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
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object> m)
        {
            var r = base._Replace(cx, so, sv,m);
            var sl = BTree<Sqlx, long?>.Empty;
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    sl += (b.key(), cx.uids[p] ?? b.value());
            r += (SetList, sl);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (stype != Sqlx.NO)
            { sb.Append(" "); sb.Append(stype); }
            sb.Append(" "); sb.Append(signal);
            var cs = " Set: ";
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cs); cs = ";";
                    sb.Append(b.key()); sb.Append("=");
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
            string sclass = signal.Substring(0, 2);
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
            if (cx.db!=null)
                cx.db += (Transaction.Diagnostics, dia);
            Handler? h = null;
            Activation? cs;
            for (cs = a; h == null && cs != null;)
            {
                h = cs.exceptions[signal];
                if (h == null && char.IsDigit(signal[0]))
                    h = cs.exceptions[sclass + "000"];
                if (h == null)
                    h = cs.exceptions["SQLEXCEPTION"];
                if (h == null)
                {
                    var c = cs.next;
                    while (c != null && !(c is Activation))
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
            var e = exception as DBException;
            if (e == null)
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class GetDiagnostics : Executable
    {
        internal const long
            List = -141; // CTree<long,Sqlx>
        internal CTree<long, Sqlx> list =>
            (CTree<long, Sqlx>?)mem[List] ?? CTree<long, Sqlx>.Empty;
        internal GetDiagnostics(long dp) : base(dp, BTree<long, object>.Empty) { }
        protected GetDiagnostics(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GetDiagnostics operator +(GetDiagnostics s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new GetDiagnostics(s.defpos, s.mem + x);
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
                    && cx.tr!=null && cx.tr.diagnostics[t] is TypedValue v)
                cx.AddValue(s, v);
            return cx;
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object> m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ls = CTree<long, Sqlx>.Empty;
            for (var b = list.First(); b != null; b = b.Next())
                ls += (cx.uids[b.key()] ?? b.key(), b.value());
            r += (List, ls);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " ";
            for (var b = list.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(" "); sb.Append(b.value());
                sb.Append("->");
                sb.Append(Uid(b.key())); 
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Select statement: single row
    /// // shareable as of 26 April 2021
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
        /// The output list
        /// </summary>
		public BList<long?> outs => (BList<long?>?)mem[Outs] ?? BList<long?>.Empty;
        /// <summary>
        /// Constructor: a select statement: single row from the parser
        /// </summary>
        /// <param name="s">The select statement</param>
        /// <param name="sv">The list of variables to receive the values</param>
		public SelectSingle(long dp) : base(dp, BTree<long, object>.Empty)
        { }
        protected SelectSingle(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectSingle operator +(SelectSingle s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SelectSingle(s.defpos, s.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var no = cx.ReplacedLl(outs);
            if (no != outs)
                r += (Outs, no);
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
    /// The Insert columns list if provided is an ordered list of some or all of the from's columns.
    /// This will match the row type of the data rowset. 
    /// If present the data rowset needs to have its columns reordered and expanded by nulls to
    /// match the from row set.
    /// shareable
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal const long
            InsCols = -241, // BList<long?>? SqlValue
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
        public static SqlInsert operator +(SqlInsert s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlInsert(s.defpos, s.mem + x);
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object> m)
        {
            var r = base._Replace(cx, so, sv,m);
            var tg = cx.ObReplace(source, so, sv);
            if (tg != source)
                r += (RowSet._Source, tg);
            return r;
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var tg = cx.Fix(source);
            if (tg != source)
                r += (RowSet._Source, tg);
            var nv = cx.Fix(value);
            if (nv != value)
                r += (Value, nv);
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
                        cx.affected = cx.affected ?? Rvv.Empty;
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
                sb.Append("]");
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal long source => (long)(mem[RowSet._Source] ?? -1L);
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        internal QuerySearch(long dp, Context cx, RowSet f, DBObject tb,
            Grant.Privilege how, CTree<UpdateAssignment, bool>? ua = null)
            : base(dp, _Mem(cx,f,tb,ua) + (RowSet._Source, f.defpos))
        { }
        protected QuerySearch(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, RowSet f,DBObject td,CTree<UpdateAssignment, bool>? ua)
        {
            var d = Math.Max(td.depth, f.depth);
            for (var b = ua?.First(); b != null; b = b.Next())
                if (cx.obs[b.key().val] is SqlValue sa &&
                    cx.obs[b.key().vbl] is SqlValue sb)
                d = Math.Max(d, Math.Max(sa.depth,sb.depth));
            var r = new BTree<long, object>(_Depth,d+1);
            if (ua!=null)
                r += (RowSet.Assig, ua);
            return r;
        }
        public static QuerySearch operator +(QuerySearch q, (long, object) x)
        {
            var (dp, ob) = x;
            if (q.mem[dp] == ob)
                return q;
            return (QuerySearch)q.New(q.mem + x);
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
                        cx.affected = cx.affected ?? Rvv.Empty;
                        if (ta.affected!=null)
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class UpdateSearch : QuerySearch
    {
        /// <summary>
        /// Constructor: A searched UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        public UpdateSearch(long dp, Context cx, RowSet f, DBObject tb,
            Grant.Privilege how)
            : base(dp, cx, f, tb, how)
        { }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator +(UpdateSearch u, (long, object) x)
        {
            var (dp, ob) = x;
            if (u.mem[dp] == ob)
                return u;
            return (UpdateSearch)u.New(u.mem + x);
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
                        cx.affected = cx.affected ?? Rvv.Empty;
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
    /// <summary>
    /// The Match syntax consists of a graph expression, an optional where condition and an optional action part.
    /// Parsing of the grap expression results in a collection of unbound identifiers and a collection
    /// of constraints.
    /// Like the CREATE syntax, a graph expression consists of a list of node-edge-node chains.
    /// The graph expression cannot contain subtype references or SqlExpressions and does not construct any nodes:
    /// all identifiers are either unbound starting with _ (_ on its own means don't care)
    /// or constant identfiers or values that are used for matching subgraphs.
    /// Unbound identiers (other than _ itself) are progressively bound to particular values, and their
    /// bindings are added to the constraints. 
    /// Evaluation of the match expression traverses all of the nodes/edges in the database 
    /// building a binding list. This maximal graph object is maintained for the Database
    /// Traversal occurs for each possible set of bindings, in lexicographic order of the identifiers
    /// and constraints in the graph expression. Each line of the graph expression must begin with a node
    /// so at the start of a line there may be a new unbound (or don't care) node for which there are a 
    /// lot of possible nodes: if the next thing is an edge, there may be a number of possible edges, and so on.
    /// Every time we succeed in binding all of the unbound identifiers, we record this
    /// binding collection and then backtrack, unbinding the most recent binding 
    /// removing the associated additional constraints, and continue from the current search position. 
    /// We continue in this way until we have traversed all of the graph.
    /// </summary>
    internal class MatchStatement : Executable
    {
        internal const long
            GDefs = -470;   // <CTree<long,TGParam>
        internal CTree<long,TGParam> gDefs =>
            (CTree<long,TGParam>)(mem[GDefs] ?? CTree<long,TGParam>.Empty);
        internal CTree<TGraph, bool> graphs =>
            (CTree<TGraph, bool>)(mem[Database.Graphs] ?? CTree<TGraph, bool>.Empty);
        internal CTree<long,bool> where =>
            (CTree<long, bool>)(mem[RowSet._Where]??CTree<long,bool>.Empty);
        public MatchStatement(long dp, CTree<TGraph, bool> gr, CTree<long, TGParam> ub,
            CTree<long,bool> wh,long st)
            : base(dp, new BTree<long,object>(GDefs,ub) + (Database.Graphs,gr)
                  +(RowSet._Where,wh)+(Stmt,st))
        { }
        public MatchStatement(long dp, BTree<long, object>? m = null) : base(dp, m)
        { }
        /// <summary>
        /// We traverse the given graphs in the order given, matching with possible database nodes as we move.
        /// The state consists of
        ///     The current expression graph node as two bookmarks (for chains, and nodes in the chain)
        ///     A saved mapping from nodes to the set of matching database nodes (depends on bindings)
        ///     The current matching database node as a bookmark in this mapping
        ///     The saved binding state (the current binding state for TGParams is in cx.binding)
        /// This state is implemented as a tuple, and saved in a BList for this tuple.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        public override Context Obey(Context cx)
        {
            var rt = BList<long?>.Empty;
            var re = CTree<long, Domain>.Empty;
            for (var b = gDefs.First(); b != null && b.key()<Transaction.Executables; b = b.Next())
                if (b.value() is TGParam g && g.id!="_"){
                    rt += g.uid;
                    re += (g.uid, Domain.Char);
                }
            var dt = new Domain(cx.GetUid(), cx, Sqlx.ROW, re, rt, rt.Length);
            cx.Add(dt);
            var ers = new ExplicitRowSet(cx.GetUid(), cx, dt, BList<(long,TRow)>.Empty)
                +(RowSet._Where,where);
            cx.Add(ers);
            // define a state for the graph traversal (see above description)
         //   var stk = BList<(ABookmark<TGraph, bool>?, ABookmark<long, TNode>?,
         //       CTree<TNode,bool>, ABookmark<TNode,bool>?, CTree<TGParam, TypedValue>)>.Empty;
            // Graph expression and Database agree on the set of NodeType and EdgeTypes
            // Traverse the given graphs, binding as we go
            cx.binding = CTree<TGParam, TypedValue>.Empty;
            cx.result = ers.defpos;
            var gp = graphs.First();
            if (gp?.key() is TGraph tg)
            {
                var xb = tg.nodes.First();
                ExpNode(cx, //ref stk,
                           gp, xb, null, null);
            }
            cx.result = ers.defpos;
            return cx;
        }
        void AddRow(Context cx)
        {
            if (cx.obs[cx.result] is ExplicitRowSet ers && cx._Dom(ers) is Domain dt)
            {
                var vs = CTree<long,TypedValue>.Empty;
                for (var b = cx.binding.First(); b != null; b = b.Next())
                    if (b.key() is TGParam tg && b.value() is TypedValue tv)
                        vs += (tg.uid, tv);
                ers += (ExplicitRowSet.ExplRows,ers.explRows+(cx.GetUid(),new TRow(dt, vs)));
                cx.Add(ers);
            }
        }
        /// <summary>
        /// We work through the given graph expression in the order given. 
        /// For each expression node xn, there is at most one next node nx to move to. 
        /// We will remember our previous node px.
        /// There is a set gDefs(xn) of TGParams defined at xn. (The TGParam can be referenced later.)
        /// In ExpNode we will compute a set ds of database nodes that can correspond with xn.
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <param name="stk">The save state for backtracking</param>
        /// <param name="gp">The position in graphs</param>
        /// <param name="xb">The position in the current graph</param>
        /// <param name="px">The previous expression node if any</param>
        void ExpNode(Context cx, // ref BList<(ABookmark<TGraph, bool>?, ABookmark<long, TNode>?,
                                 // CTree<TNode,bool>, ABookmark<TNode, bool>?, CTree<TGParam, TypedValue>)> stk,
                ABookmark<TGraph, bool>? gp, ABookmark<long, TNode>? xb, TNode? px, TNode? pd)
        {
            if (xb?.value() is not TMatch xn)
                return;
            // We have a current node xn, but no current dn yet. Initialise the set of possible d to empty. 
            var ds = CTree<TNode, bool>.Empty; // the set of database nodes that can match with xn
            if (pd is TEdge pe) // there is only one possible database node that can match with xn
            {
                if (cx.db.nodeIds[pe.arriving] is TNode n)
                    ds += (n, true);
            } else if (!xn.id.StartsWith('_')) // then there is only one possible matching database node
            {
                if (cx.db.nodeIds[xn.id] is TNode n) // we will check properties match in DbNode
                    ds += (n, true);
            } else if (xn.dataType != Domain.NodeType && xn.dataType != Domain.EdgeType) // has a specific type
            {
                if (cx.db.objects[xn.dataType.structure] is Table tb)
                    for (var b = tb.tableRows.First(); b != null; b = b.Next())
                        if (b.value().vals[xn.dataType.rowType[0] ?? -1L] is TChar c
                            && cx.db.nodeIds[c.ToString()] is TNode n)
                        {
                            if (pd?.dataType is NodeType nt && n.dataType is EdgeType et
                                && cx._Ob(et.leavingType) is Domain lt
                                && !nt.EqualOrStrongSubtypeOf(lt))
                                continue;
                            ds += (n, true);
                        }
            } else for(var b = cx.db.nodeIds.First();b!=null;b=b.Next())
                if (b.value() is TNode n)
                        ds += (n, true);
            var df = ds.First();
 //           var j = stk.Length;
 //           stk += (gp,xb,ds,df,cx.binding);
            while (df!=null)
                df = DbNode(cx, // ref stk,
                               gp, xb, xn, df, px, pd);
 //           while (stk.Length > j)
 //               stk -= stk.Length - 1;
        }
        /// <summary>
        /// For each dn in ds:
        /// If xn's specific properties do not match dx's then backtrack.
        /// We bind each t in t(xn), using the char values in dn.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="stk"></param>
        /// <param name="xb"></param>
        /// <param name="xn"></param>
        /// <param name="df"></param>
        /// <returns></returns>
        ABookmark<TNode, bool>? DbNode(Context cx, // ref BList<(ABookmark<TGraph, bool>?, ABookmark<long, TNode>?,
                                                   // CTree<TNode, bool>, ABookmark<TNode, bool>?, CTree<TGParam, TypedValue>)> stk,
            ABookmark<TGraph, bool>? gp, ABookmark<long, TNode>? xb, TMatch xn, ABookmark<TNode, bool> df,
            TNode? px, TNode? pd)
        {
            var ob = cx.binding;
            var bi = cx.binding;
            if (df.key() is not TNode dn || !xn.CheckProps(cx,dn))
                goto backtrack;
            if (dn is TEdge de && pd != null && pd.id != de.leaving)
                goto backtrack;
            var ns = CTree<string, TGParam>.Empty;
            for (var b = xn.tgs.First(); b != null; b = b.Next())
                if (b.value() is TGParam tg)
                {
                    switch (tg.kind)
                    {
                        case Sqlx.RARROWBASE:
                        case Sqlx.ARROW:
                        case Sqlx.LPAREN: bi += (tg, new TChar(dn.id)); break;
                        case Sqlx.COLON: bi += (tg, new TChar(dn.dataType.name)); break;
                    }
                    ns += (tg.id, tg);
                }
            var ps = CTree<string, TGParam>.Empty;
            for (var b = xn.props.First(); b != null; b = b.Next())
                if (b.value() is TGParam tg && ns.Contains(tg.id))
                    ps += (b.key(), tg);
            for (var c = xn.columns.First(); c != null; c = c.Next())
                if (c.value() is long p && cx.NameFor(p) is string nn
                    && ps[nn] is TGParam tg1 && dn.values[p] is TypedValue tv)
                            bi += (tg1, tv);
            cx.binding = bi;
            xb = xb?.Next();
            TNode? xx = xn; // we don't pass in xn if we are starting a new graph
            TNode? dx = dn;
            if (xb==null)  // we have reached the end of a graph in the match
            {
                xx = null;
                dx = null;
                gp = gp?.Next(); // start the next graph
                if (gp?.key() is TGraph g)
                    xb = g.nodes.First();
            }
            // stk +=(gp,xb,)
            if (xb != null) // go on to the next node in the expression graph
                ExpNode(cx, // ref stk,
                            gp, xb, xx, dx);
            else   // we have finished the expression and should add a row
                AddRow(cx);
        backtrack:
            cx.binding = ob; // unbind all the bindings we made
            return df.Next(); // try another node
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
    }
}