using System;
using System.Configuration;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
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
        public string stmt => (string)mem[Stmt];
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal string label => (string)mem[Label];
        protected Executable(long dp, BTree<long, object> m=null) 
            : base(dp, m??BTree<long,object>.Empty) { }
        /// <summary>
        /// Support execution of a list of Executables, in an Activation.
        /// With break behaviour
        /// </summary>
        /// <param name="e">The Executables</param>
        /// <param name="tr">The transaction</param>
		protected Context ObeyList(CList<long> e,Context cx)
		{
            if (e == null)
                throw new DBException("42173");
            Context nx = cx;
            Activation a = (Activation)cx;
            for (var b = e.First();b!=null && nx==cx 
                && ((Activation)cx).signal==null;b=b.Next())
            {
                try
                {
                    var x = (Executable)cx._Ob(b.value());
                    nx = x.Obey(a);
                    if (a==nx && a.signal != null)
                        a.signal.Throw(a);
                    if (cx != nx)
                        break;
                }
                catch (DBException ex)
                {
                    a.signal = new Signal(cx.cxid,ex);
                }
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
        internal static bool Calls(CList<long> ss,long defpos,Context cx)
        {
            for (var b = ss?.First(); b != null; b = b.Next())
                if (cx.obs[b.value()].Calls(defpos, cx))
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
            throw cx.db.Exception("40000").ISO();
        }
        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
        internal override DBObject Relocate(long dp)
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
        public string _source => (string)mem[SourceSQL];
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
            return (SelectStatement)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SelectStatement)base._Relocate(cx);
            r += (Union, cx.Fix(union));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SelectStatement)base._Fix(cx);
            var nc = cx.Fix(union);
            if (nc != union)
                r += (Union, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[union].Calls(defpos, cx);
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SelectStatement)base._Replace(cx, so, sv);
            var nu = ((RowSet)cx.obs[r.union])._Replace(cx, so, sv).defpos;
            if (nu!=r.union)
                r += (Union, nu);
            cx.done += (defpos, r);
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
             Stms = -96; // CList<long> Executable
        /// <summary>
        /// The contained list of Executables
        /// </summary>
		public CList<long> stms =>
            (CList<long>)mem[Stms] ?? CList<long>.Empty;
        /// <summary>
        /// Constructor: create a compound statement
        /// </summary>
        /// <param name="n">The label for the compound statement</param>
		public CompoundStatement(long dp,string n) 
            : base(dp,new BTree<long,object>(Label,n))
		{ }
        protected CompoundStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CompoundStatement operator +(CompoundStatement c, (long, object) x)
        {
            return new CompoundStatement(c.defpos, c.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CompoundStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CompoundStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CompoundStatement)base._Relocate(cx);
            r += (Stms,cx.FixLl(stms));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (CompoundStatement)base._Fix(cx);
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
            var act = new Activation(cx,label);
            try
            {
                act = (Activation)ObeyList(stms, act);
                if (act.signal != null)
                    act.signal.Throw(cx);
            }
            catch (Exception e) { throw e; }
            return act.SlideDown();
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CompoundStatement)base._Replace(cx, so, sv);
            var ns = cx.ReplacedLl(r.stms);
            if (ns!=r.stms)
                r += (Stms, ns);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "(";
            for (var b = stms.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class PreparedStatement : Executable
    {
        internal const long
            QMarks = -396,  // CList<long> SqlValue
            Target = -397; // Executable
        internal CList<long> qMarks =>
           (CList<long>)mem[QMarks] ?? CList<long>.Empty;
        internal Executable target => (Executable)mem[Target];
        public PreparedStatement(Context cx,long nst)
            : base(cx.GetUid(), BTree<long, object>.Empty
                 + (Target, cx.exec) + (QMarks, cx.qParams) + (_Framing, new Framing(cx,nst)))
        { }
        protected PreparedStatement(long dp,BTree<long,object> m)
            :base(dp,m)
        { }
        public static PreparedStatement operator+(PreparedStatement pr,(long,object)x)
        {
            return (PreparedStatement)pr.New(pr.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PreparedStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new PreparedStatement(dp,mem);
        }
        public override Context Obey(Context cx)
        {
            return ((Executable)target.Instance(defpos,cx)).Obey(cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PreparedStatement)base._Replace(cx, so, sv);
            var nq = cx.ReplacedLl(r.qMarks);
            if (nq!=r.qMarks)
                r += (QMarks,nq);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");sb.Append(target);
            var cm = "";
            sb.Append(" Params: ");
            for (var b = qMarks.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
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
        public LocalVariableDec(long dp, Context cx, SqlValue v, BTree<long,object>m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Label, v.name)
          + (AssignmentStatement.Vbl, v.defpos)+(_Domain,v.domain))
        { }
        protected LocalVariableDec(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LocalVariableDec operator+(LocalVariableDec s,(long,object)x)
        {
            return (LocalVariableDec)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LocalVariableDec(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new LocalVariableDec(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (LocalVariableDec)base._Relocate(cx);
            r += (AssignmentStatement.Vbl, cx.Fix(vbl));
            r += (Init, cx.Fix(init));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (LocalVariableDec)base._Fix(cx);
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
            return cx.FindCx(defpos).values[defpos];
        }
        /// <summary>
        /// Execute the local variable declaration, by adding the local variable to the activation (overwrites any previous)
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var vb = (SqlValue)cx.obs[vbl];
            var dm = cx._Dom(vb);
            TypedValue tv = cx.obs[init]?.Eval(cx)??dm.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.AddValue(vb, tv); // We expect a==cx, but if not, tv will be copied to a later
            return cx;
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LocalVariableDec)base._Replace(cx, so, sv);
            var ni = ((SqlValue)cx.obs[r.init])._Replace(cx,so,sv).defpos ;
            if (ni!=r.init)
                r += (Init, ni);
            cx.done += (defpos, r);
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
            return new FormalParameter(s.defpos,s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FormalParameter(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new FormalParameter(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (FormalParameter)base._Relocate(cx);
            return r+(AssignmentStatement.Val,cx.Fix(val));
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (FormalParameter)base._Fix(cx);
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
            return cx.values[defpos];
        }
        internal override CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false; // might be the target of an assignment in the method body
        }
        internal override string ToString(string sg,Remotes rf,CList<long> cs,
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
            return (CursorDeclaration)c.New(c.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CursorDeclaration(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CursorDeclaration(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (DBObject)base._Relocate(cx);
            r += (CS, cx.Fix(cs));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (CursorDeclaration)base._Fix(cx);
            var nc = cx.Fix(cs);
            if (nc!=cs)
                r += (CS, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[cs].Calls(defpos, cx);
        }
        /// <summary>
        /// Instantiate the cursor
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var cu = cx.obs[cs];
            cx.AddValue(cu, cu.Eval(cx));
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CursorDeclaration)base._Replace(cx, so, sv);
            var nc = ((SqlValue)cx.obs[r.cs])._Replace(cx, so, sv).defpos;
            if (nc != r.cs)
                r += (CS, nc);
            cx.done += (defpos, r);
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
        public string name => (string)mem[ObInfo.Name] ?? "";
        /// <summary>
        /// A list of condition names, SQLSTATE codes, "SQLEXCEPTION", "SQLWARNING", or "NOT_FOUND"
        /// </summary>
		public BList<string> conds => 
            (BList<string>)mem[Conds]?? BList<string>.Empty;
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
            return new HandlerStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new HandlerStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new HandlerStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (HandlerStatement)base._Relocate(cx);
            r += (Action, cx.Fix(action));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (HandlerStatement)base._Fix(cx);
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
            a.saved = new ExecState(cx);
            for (var c =conds.First();c!=null;c=c.Next())
				a.exceptions+=(c.value(),new Handler(this,a));
            return cx;
		}
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[action].Calls(defpos, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (HandlerStatement)base._Replace(cx, so, sv);
            var na = ((Executable)cx.obs[r.action])._Replace(cx, so, sv).defpos;
            if (na != r.action)
                r += (Action, na);
            cx.done += (defpos, r);
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
		HandlerStatement hdlr => (HandlerStatement)mem[Hdlr];
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
            return new Handler(h.defpos, h.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Handler(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Handler(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Handler)base._Relocate(cx);
            r += (Hdlr, hdlr.Relocate(cx));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (Handler)base._Fix(cx);
            var nh = hdlr.Fix(cx);
            if (hdlr!=nh)
            r += (Hdlr, nh);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return hdlr.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute the action part of the Handler Statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            try
            {
                Activation definer = null;
                for (Context p = cx; definer == null && p != null; p = p.next)
                    if (p.cxid == hdefiner)
                        definer = p as Activation;
                if (hdlr.htype == Sqlx.UNDO)
                {
                    CompoundStatement cs = null;
                    for (Context p = cx; cs == null && p != null; p = p.next)
                        cs = p.exec as CompoundStatement;
                    if (cs != null)
                    {
                        cx.db = definer.saved.mark;
                        cx.next = definer.saved.stack;
                    }
                }
                ((Executable)cx.obs[hdlr.action]).Obey(cx);
                if (hdlr.htype == Sqlx.EXIT)
                    return definer.next;
                var a = (Activation)cx;
                if (a.signal != null)
                    a.signal.Throw(cx);
            }
            catch (Exception e) { throw e; }
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Hdef=");sb.Append(Uid(hdefiner));
            sb.Append(" Hdlr=");sb.Append(Uid(hdlr.defpos));
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
		public BreakStatement(long dp,string n) : base(dp,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected BreakStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BreakStatement operator+(BreakStatement b,(long,object)x)
        {
            return new BreakStatement(b.defpos, b.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BreakStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new BreakStatement(dp,mem);
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
            for (cx = cx.next; cx.next != null; cx = cx.next)
                if (cx is Activation ac && ac.label == label)
                    break;
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
        public AssignmentStatement(long dp,long vb,long va,int d)
            : base(dp, BTree<long, object>.Empty + (Vbl, vb) + (Val, va)
                  +(_Depth,d))
        { }
        protected AssignmentStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static AssignmentStatement operator+(AssignmentStatement s,(long,object)x)
        {
            return new AssignmentStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new AssignmentStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new AssignmentStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (AssignmentStatement)base._Relocate(cx);
            r += (Val, cx.Fix(val));
            r += (Vbl, cx.Fix(vbl));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (AssignmentStatement)base._Fix(cx);
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
            return cx.obs[vbl].Calls(defpos, cx) || cx.obs[val].Calls(defpos,cx);
        }
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var vb = cx.obs[vbl] ?? (DBObject) cx.db.objects[vbl];
            var dm = cx._Dom(vb);
            if (val != -1L)
            {
                var v = dm.Coerce(cx,cx.obs[val].Eval(cx));
                cx.values += (vbl, v);
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (AssignmentStatement)base._Replace(cx, so, sv);
            var nb = ((SqlValue)cx.obs[vbl])._Replace(cx, so, sv).defpos;
            if (nb != r.vbl)
                r += (Vbl, nb);
            var na = ((SqlValue)cx.obs[val])._Replace(cx, so, sv).defpos;
            if (na != r.val)
                r += (Val, na);  
            cx.done += (defpos, r);
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
            List = -108, // CList<long> SqlValue
            Rhs = -109; // long SqlValue
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal CList<long> list => (CList<long>)mem[List];
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
            var dm = cx._Dom(rg);
            var r = new BTree<long,object>(Rhs,rg.defpos);
            var ls = CList<long>.Empty;
            for (var b=lh.First();b!=null;b=b.Next())
            {
                var id = b.value();
                var v = (SqlValue)cx.obs[id.iix.dp];
                dm = dm.Constrain(cx, id.iix.lp, cx._Dom(v));
                d = Math.Max(d, v.depth+1);
                ls += v.defpos;
            }
            return r + (_Depth,d)+(List,ls)+(LhsType,dm.defpos);
        }
        public static MultipleAssignment operator+(MultipleAssignment s,(long,object)x)
        {
            return new MultipleAssignment(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MultipleAssignment(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new MultipleAssignment(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (MultipleAssignment)base._Relocate(cx);
            r += (LhsType, cx.Fix(lhsType));
            r +=(List, cx.FixLl(list));
            r += (Rhs, cx.Fix(rhs));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (MultipleAssignment)base._Fix(cx);
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
            return cx.obs[rhs].Calls(defpos, cx);
        }
        /// <summary>
        /// Execute the multiple assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            TRow r = (TRow)cx.obs[rhs].Eval(cx);
            for (int j = 0; j < r.Length; j++)
                cx.values+=(list[j],r[j]);
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MultipleAssignment)base._Replace(cx, so, sv);
            var nd = ((Domain)cx.obs[lhsType])._Replace(cx, so, sv).defpos;
            if (nd != r.lhsType)
                r += (LhsType, nd);
            var nl = cx.ReplacedLl(r.list);
            if (nl != r.list)
                r += (List, nl);
            var na = ((SqlValue)cx.obs[rhs])._Replace(cx, so, sv).defpos;
            if (na != r.rhs)
                r += (Rhs, na);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" LhsType: "); sb.Append(Uid(lhsType));
            var cm = " Lhs: ";
            for (var b=list.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
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
            return new ReturnStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ReturnStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ReturnStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ReturnStatement)base._Relocate(cx);
            r += (Ret, cx.Fix(ret));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (ReturnStatement)base._Fix(cx);
            var nr = cx.Fix(ret);
            if (nr != ret)
                r += (Ret, nr);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[ret].Calls(defpos, cx);
        }
        /// <summary>
        /// Execute the return statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
			a.val = cx.obs[ret].Eval(cx);
            cx = a.SlideDown();
            return cx;
		}
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (ReturnStatement)base._Replace(cx, so, sv);
            var nr = ((SqlValue)cx.obs[ret])._Replace(cx, so, sv).defpos;
            if (nr != r.ret)
                r += (Ret,nr);
            cx.done += (defpos, r);
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
            Else = -111, // CList<long> Executable
            _Operand = -112, // long SqlValue
            Whens = -113; // CList<long> WhenPart
        /// <summary>
        /// The test expression
        /// </summary>
        public long operand => (long)(mem[_Operand]??-1L);
        /// <summary>
        /// A list of when parts
        /// </summary>
        public CList<long> whens => (CList<long>)mem[Whens]?? CList<long>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
        public CList<long> els => (CList<long>)mem[Else]?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a case statement from the parser
        /// </summary>
        public SimpleCaseStatement(long dp,Context cx,SqlValue op,BList<WhenPart> ws,
            CList<long> ss) : 
            base(dp,_Mem(cx,op,ws,ss))
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue op, BList<WhenPart> ws,
            CList<long> ss)
        {
            var r = new BTree<long,object>(_Operand,op.defpos);
            var d = op.depth + 1;
            var wl = CList<long>.Empty;
            for (var b=ws.First();b!=null;b=b.Next())
            {
                var w = b.value();
                d = Math.Max(d, w.depth + 1);
                wl += w.defpos;
            }
            if (ss!=CList<long>.Empty)
            {
                for (var b = ss.First(); b != null; b = b.Next())
                    d = Math.Max(d, cx.obs[b.value()].depth + 1);
                r += (Else, ss);
            }
            return r + (Whens,wl) + (_Depth,d);
        }
        public static SimpleCaseStatement operator+(SimpleCaseStatement s,(long,object)x)
        {
            return new SimpleCaseStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SimpleCaseStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SimpleCaseStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SimpleCaseStatement)base._Relocate(cx);
            r += (Else, cx.FixLl(els));
            r += (_Operand, cx.Fix(operand));
            r += (Whens, cx.FixLl(whens));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SimpleCaseStatement)base._Fix(cx);
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
                if (cx.obs[b.value()].Calls(defpos, cx))
                    return true;
            return Calls(els,defpos,cx) || cx.obs[operand].Calls(defpos,cx);
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
                if (((SqlValue)cx.obs[operand]).Matches(cx)==true)
                    return ObeyList(((WhenPart)cx.obs[c.value()]).stms, cx);
            return ObeyList(els, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SimpleCaseStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.operand])._Replace(cx, so, sv).defpos;
            if (no != r.operand)
                r += (_Operand, no);
            var nw = cx.ReplacedLl(r.whens);
            if (nw != r.whens)
                r += (Whens, nw);
            var ne = cx.ReplacedLl(r.els);
            if (ne != r.els)
                r += (Else, ne);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand: "); sb.Append(Uid(operand));
            var cm =" Whens: ";
            for (var b=whens.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
		public CList<long> whens => (CList<long>)mem[SimpleCaseStatement.Whens];
        /// <summary>
        /// An else part
        /// </summary>
		public CList<long> els => (CList<long>)mem[SimpleCaseStatement.Else]?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(long dp,Context cx,BList<WhenPart>ws,CList<long>ss) 
            : base(dp,_Mem(cx,ws,ss))
        {  }
        protected SearchedCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,BList<WhenPart>ws,CList<long>ss)
        {
            var d = 1;
            var r = BTree<long, object>.Empty;
            if (ss != CList<long>.Empty)
            {
                for (var b = ss.First(); b != null; b = b.Next())
                    d = Math.Max(d, cx.obs[b.value()].depth + 1);
                r += (SimpleCaseStatement.Else,ss);
            }
            var wl = CList<long>.Empty;
            for (var b = ws.First(); b != null; b = b.Next())
            {
                var w = b.value();
                d = Math.Max(d, w.depth + 1);
                wl += w.defpos;
            }
            return r + (SimpleCaseStatement.Whens,wl)+(_Depth, d);
        }
        public static SearchedCaseStatement operator+(SearchedCaseStatement s,(long,object)x)
        {
            return new SearchedCaseStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SearchedCaseStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SearchedCaseStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SearchedCaseStatement)base._Relocate(cx);
            r += (SimpleCaseStatement.Else,cx.FixLl(els));
            r += (SimpleCaseStatement.Whens, cx.FixLl(whens));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SearchedCaseStatement)base._Fix(cx);
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
                if (cx.obs[b.value()].Calls(defpos, cx))
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
            {
                var w = (WhenPart)cx.obs[c.value()];
                if (((SqlValue)cx.obs[w.cond]).Matches(cx)==true)
                    return ObeyList(w.stms, cx);
            }
			return ObeyList(els,cx);
		}
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SearchedCaseStatement)base._Replace(cx, so, sv);
            var nw = cx.ReplacedLl(r.whens);
            if (nw != r.whens)
                r += (SimpleCaseStatement.Whens, nw);
            var ne = cx.ReplacedLl(r.els);
            if (ne != r.els)
                r += (SimpleCaseStatement.Else, ne);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " Whens: ";
            for (var b = whens.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
            Stms = -115; // CList<long> Executable
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public long cond => (long)(mem[Cond]??-1L);
        /// <summary>
        /// a list of statements
        /// </summary>
		public CList<long> stms =>(CList<long>)mem[Stms]?? CList<long>.Empty;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A list of statements for this when</param>
        public WhenPart(long dp,SqlValue v, CList<long> s) 
            : base(dp, BTree<long,object>.Empty+(Cond,v.defpos)+(Stms,s))
        { }
        protected WhenPart(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhenPart operator+(WhenPart s,(long,object) x)
        {
            return new WhenPart(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WhenPart(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WhenPart(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (WhenPart)base._Relocate(cx);
            r += (Cond, cx.Fix(cond));
            r += (Stms, cx.FixLl(stms));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (WhenPart)base._Fix(cx);
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
            return Calls(stms,defpos,cx) || cx.obs[cond].Calls(defpos, cx);
        }
        public override Context Obey(Context cx)
        {
            var a = cx;
            if (cond == -1L || ((SqlValue)cx.obs[cond]).Matches(cx)==true)
            {
                a.val = TBool.True;
                return ObeyList(stms, cx);
            }
            a.val = TBool.False;
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (WhenPart)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.cond])._Replace(cx, so, sv).defpos;
            if (no != r.cond)
                r += (Cond, no);
            var nw = cx.ReplacedLl(r.stms);
            if (nw != r.stms)
                r += (Stms, nw);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (cond!=-1L)
                sb.Append(" Cond: ");sb.Append(Uid(cond));
            sb.Append(" Stms: ");
            var cm = "(";
            for (var b=stms.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(Uid(b.value()));
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
            Else = -116, // CList<long> Executable
            Elsif = -117, // CList<long> Executable
            Search = -118, // long SqlValue
            Then = -119; // CList<long> Executable
        /// <summary>
        /// The test condition
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
        /// <summary>
        /// The then statements
        /// </summary>
		public CList<long> then => (CList<long>)mem[Then]?? CList<long>.Empty;
        /// <summary>
        /// The elsif parts
        /// </summary>
		public CList<long> elsif => (CList<long>)mem[Elsif] ?? CList<long>.Empty;
        /// <summary>
        /// The else part
        /// </summary>
		public CList<long> els => (CList<long>)mem[Else] ?? CList<long>.Empty;
        /// <summary>
        /// Constructor: an if-then-else statement from the parser
        /// </summary>
		public IfThenElse(long dp,Context cx,SqlValue se,CList<long>th,CList<long>ei,CList<long> el) 
            : base(dp,_Mem(cx,se,th,ei,el))
		{}
        protected IfThenElse(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue se, CList<long> th, CList<long> ei,
            CList<long> el)
        {
            var r = new BTree<long, object>(Search,se.defpos) +(Then,th) +(Elsif,ei)
                + (Else,el);
            var d = se.depth + 1;
            for (var b = th.First(); b != null; b = b.Next())
                d = Math.Max(d, cx.obs[b.value()].depth + 1);
            for (var b = ei.First(); b != null; b = b.Next())
                d = Math.Max(d, cx.obs[b.value()].depth + 1); 
            for (var b = el.First(); b != null; b = b.Next())
                d = Math.Max(d, cx.obs[b.value()].depth + 1);
            return r + (_Depth,d);
        }
        public static IfThenElse operator+(IfThenElse s,(long,object)x)
        {
            return new IfThenElse(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new IfThenElse(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new IfThenElse(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (IfThenElse)base._Relocate(cx);
            r += (Search, cx.Fix(search));
            r += (Then,cx.FixLl(then));
            r += (Else, cx.FixLl(els)); 
            r += (Elsif, cx.FixLl(elsif)); 
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (IfThenElse)base._Fix(cx);
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
            if (((SqlValue)cx.obs[search]).Matches(cx)==true)
                return ObeyList(then, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (cx.obs[g.value()] is IfThenElse f 
                    && ((SqlValue)cx.obs[f.search]).Matches(cx)==true)
                    return ObeyList(f.then, cx);
            return ObeyList(els, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (IfThenElse)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.search])._Replace(cx, so, sv).defpos;
            if (no != r.search)
                r += (Search, no);
            var nt = cx.ReplacedLl(r.then);
            if (nt != r.then)
                r += (Then, nt);
            var ni = cx.ReplacedLl(r.elsif);
            if (ni != r.elsif)
                r += (Elsif, ni);
            var ne = cx.ReplacedLl(r.els);
            if (ne != r.els)
                r += (Else, ne);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand="); sb.Append(Uid(search));
            var cm = " Then: ";
            for (var b = then.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            cm = " ElsIf: ";
            for (var b = elsif.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            cm = " Else: ";
            for (var b = els.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
        public CTree<string, string> nsps => (CTree<string,string>)mem[Nsps];
        /// <summary>
        /// Constructor
        /// </summary>
        public XmlNameSpaces(long dp) : base(dp,BTree<long,object>.Empty) { }
        protected XmlNameSpaces(long dp, BTree<long, object> m) : base(dp, m) { }
        public static XmlNameSpaces operator+(XmlNameSpaces s,(long,object)x)
        {
            return new XmlNameSpaces(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new XmlNameSpaces(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new XmlNameSpaces(dp,mem);
        }
        /// <summary>
        /// Add the namespaces
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            for(var b=nsps.First();b!= null;b=b.Next())
                cx.nsps+=(b.key(),b.value());
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
            What = -123; // CList<long> Executable
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
        /// <summary>
        /// The statements to execute
        /// </summary>
		public CList<long> what => (CList<long>)mem[What]?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a while statement from the parser
        /// </summary>
        /// <param name="n">The label for the while</param>
		public WhileStatement(long dp,string n) : base(dp,new BTree<long,object>(Label,n)) 
        {  }
        protected WhileStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhileStatement operator+(WhileStatement s,(long,object)x)
        {
            return new WhileStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WhileStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WhileStatement(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (WhileStatement)base._Relocate(cx);
            r += (Search, cx.Fix(search));
            r += (What, cx.FixLl(what));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (WhileStatement)base._Fix(cx);
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
            while (na==cx && a.signal == null && ((SqlValue)cx.obs[search]).Matches(cx)==true)
            {
                var lp = new Activation(cx,label);
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
            return Calls(what,defpos,cx) || cx.obs[search].Calls(defpos, cx);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (WhileStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.search])._Replace(cx, so, sv).defpos;
            if (no != r.search)
                r += (Search, no);
            var nw = cx.ReplacedLl(r.what);
            if (nw != r.what)
                r += (What, nw);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Operand="); sb.Append(Uid(search));
            var cm = " What: ";
            for (var b = what.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
		public CList<long> what => (CList<long>)mem[WhileStatement.What];
         /// <summary>
        /// Constructor: a repeat statement from the parser
        /// </summary>
        /// <param name="n">The label</param>
        public RepeatStatement(long dp,string n) : base(dp,new BTree<long,object>(Label,n)) 
        {  }
        protected RepeatStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RepeatStatement operator+(RepeatStatement s,(long,object)x)
        {
            return new RepeatStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RepeatStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RepeatStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RepeatStatement)base._Relocate(cx);
            r += (WhileStatement.Search, cx.Fix(search));
            r += (WhileStatement.What, cx.FixLl(what));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RepeatStatement)base._Fix(cx);
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
            return Calls(what, defpos, cx) || cx.obs[search].Calls(defpos,cx);
        }
        /// <summary>
        /// Execute the repeat statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var act = new Activation(cx,label);
            Context na = act;
            for (; ;)
            {
                na = ObeyList(what, act);
                if (na != act)
                    break;
                act.signal?.Throw(act);
                if (((SqlValue)cx.obs[search]).Matches(act)!=false)
                    break;
            }
            cx = act.SlideDown(); 
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RepeatStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.search])._Replace(cx, so, sv).defpos;
            if (no != r.search)
                r += (WhileStatement.Search, no);
            var nw = cx.ReplacedLl(r.what);
            if (nw != r.what)
                r += (WhileStatement.What, nw);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " What: ";
            for (var b = what.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
		public IterateStatement(long dp,string n):base(dp,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected IterateStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static IterateStatement operator+(IterateStatement s,(long,object)x)
        {
            return new IterateStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new IterateStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new IterateStatement(dp, mem);
        }
        /// <summary>
        /// Execute the iterate statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx; // from the top of the stack each time
            a.exec = this;
            return a.cont;
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
		public CList<long> stms => (CList<long>)mem[WhenPart.Stms];
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
            return new LoopStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LoopStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new LoopStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (LoopStatement)base._Relocate(cx);
            r += (WhenPart.Stms, cx.FixLl(stms));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (LoopStatement)base._Fix(cx);
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
            var act = new Activation(cx,label);
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
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LoopStatement)base._Replace(cx, so, sv);
            var nw = cx.ReplacedLl(r.stms);
            if (nw != r.stms)
                r += (WhenPart.Stms, nw);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " Stms: ";
            for (var b = stms.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
            Stms = -128; // CList<long> Executable
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public long sel => (long)(mem[Sel]??-1L);
        /// <summary>
        /// The identifier in the AS part
        /// </summary>
		public string forvn => (string)mem[ForVn];
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public CList<long> stms => (CList<long>)mem[Stms];
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
        public ForSelectStatement(long dp, Context cx, string n,Ident vn, 
            RowSet rs,CList<long>ss ) 
            : base(dp,_Mem(cx,rs,ss) +(Label,n)+(ForVn,vn.ident))
		{ }
        protected ForSelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,RowSet rs,CList<long>ss)
        {
            var r = BTree<long,object>.Empty;
            var d = rs.depth+1;
            for (var b = ss.First(); b != null; b = b.Next())
                d = Math.Max(d, cx.obs[b.value()].depth + 1);
            return r + (Sel,rs.defpos) + (Stms,ss) + (_Depth, d);
        }
        public static ForSelectStatement operator+(ForSelectStatement s,(long,object)x)
        {
            return new ForSelectStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ForSelectStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ForSelectStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ForSelectStatement)base._Relocate(cx);
            r += (Sel, cx.Fix(sel));
            r += (Stms, cx.FixLl(stms));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (ForSelectStatement)base._Fix(cx);
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
            return cx.obs[sel].Calls(defpos, cx) || Calls(stms,defpos,cx);
        }
        /// <summary>
        /// Execute a FOR statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var qs = (RowSet)cx.obs[sel];
            var ac = new Activation(cx,label);
            for (var rb = qs.First(ac); rb != null; rb = rb.Next(ac))
            {
                ac.Add(qs);
                cx.cursors += (qs.defpos, rb);
                ac.values += cx.cursors[qs.defpos].values;
                ac.brk = cx as Activation;
                ac.cont = ac;
                ac = (Activation)ObeyList(stms, ac);
                if (ac.signal != null)
                    ac.signal.Throw(cx);
            }
            return ac.SlideDown();
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (ForSelectStatement)base._Replace(cx, so, sv);
            var no = ((RowSet)cx.obs[r.sel])._Replace(cx, so, sv).defpos;
            if (no != r.sel)
                r += (Sel, no);
            var nw = cx.ReplacedLl(r.stms);
            if (nw != r.stms)
                r += (Stms, nw);
            cx.done += (defpos, r);
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
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
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
		public OpenStatement(long dp, SqlCursor c,long i) : base(dp,BTree<long,object>.Empty
            +(FetchStatement.Cursor,c.defpos))
		{ }
        protected OpenStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static OpenStatement operator+(OpenStatement s,(long,object)x)
        {
            return new OpenStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new OpenStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new OpenStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (OpenStatement)base._Relocate(cx);
            r += (FetchStatement.Cursor, cx.Fix(cursor));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (OpenStatement)base._Fix(cx);
            var nc = cx.Fix(cursor);
            if (nc != cursor)
                r += (FetchStatement.Cursor, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            var c = (SqlCursor)cx.obs[cursor];
            return c.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (OpenStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.cursor])._Replace(cx, so, sv).defpos;
            if (no != r.cursor)
                r += (FetchStatement.Cursor, no);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Cursor: "); sb.Append(Uid(cursor));
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
		public CloseStatement(long dp, SqlCursor c,long i) : base(dp,BTree<long,object>.Empty
            +(FetchStatement.Cursor,c.defpos)+(_Domain,c.domain))
		{ }
        protected CloseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CloseStatement operator+(CloseStatement s,(long,object)x)
        {
            return new CloseStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CloseStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CloseStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CloseStatement)base._Relocate(cx);
            r += (FetchStatement.Cursor, cx.Fix(cursor));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (CloseStatement)base._Fix(cx);
            var nc = cx.Fix(cursor);
            if (nc!=cursor)
                r += (FetchStatement.Cursor, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            var c = (SqlCursor)cx.obs[cursor];
            return c.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute the close statement
        /// </summary>
        public override Context Obey(Context cx)
        {
            cx.Add(new EmptyRowSet(defpos,cx,domain));
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CloseStatement)base._Replace(cx, so, sv);
            var no = ((SqlValue)cx.obs[r.cursor])._Replace(cx, so, sv).defpos;
            if (no != r.cursor)
                r += (FetchStatement.Cursor, no);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Cursor: "); sb.Append(Uid(cursor));
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
            Outs = -131, // CList<long> SqlValue
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
		public CList<long> outs => (CList<long>)mem[Outs]?? CList<long>.Empty; 
        /// <summary>
        /// Constructor: a fetch statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
        /// <param name="h">The fetch behaviour: ALL, NEXT, LAST PRIOR, ABSOLUTE, RELATIVE</param>
        /// <param name="w">The output variables</param>
        public FetchStatement(long dp, SqlCursor n, Sqlx h, SqlValue w)
        : base(dp, BTree<long, object>.Empty + (Cursor, n.defpos) + (How, h) + (Where, w?.defpos??-1L))
        { }
        protected FetchStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static FetchStatement operator+(FetchStatement s,(long,object)x)
        {
            return new FetchStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FetchStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new FetchStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (FetchStatement)base._Relocate(cx);
            r += (Cursor, cx.Fix(cursor));
            r += (Outs, cx.FixLl(outs));
            r += (Where, cx.Fix(where));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (FetchStatement)base._Fix(cx);
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
            return cx.obs[cursor].Calls(defpos, cx) || 
                cx.obs[where].Calls(defpos,cx) || Calls(outs,defpos,cx);
        }
        /// Execute a fetch
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var cu = (SqlCursor)cx.obs[cursor];
            var cs = (RowSet)cx.obs[cu.spec];
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
                        for (var e = ((RowSet)cx.obs[cs.defpos]).First(cx); e != null; 
                            e = e.Next(cx))
                            n++;
                        rqpos = n - 1;
                        break;
                    }
                case Sqlx.ABSOLUTE:
                    rqpos = (long)cx.obs[where].Eval(cx).Val();
                    break;
                case Sqlx.RELATIVE:
                    rqpos = rqpos + (long)cx.obs[where].Eval(cx).Val();
                    break;
            }
            if (rb == null || rqpos == 0)
                rb = ((RowSet)cx.obs[cs.defpos]).First(cx);
            while (rb != null && rqpos != rb._pos)
                rb = rb.Next(cx);
            cx.values += (cu.defpos, rb);
            if (rb == null)
                cx = new Signal(defpos, "02000", "No obs").Obey(cx);
            else
            {
                var dt = cx._Dom(cs).rowType;
                for (int i = 0; i < dt.Length; i++)
                {
                    var c = rb[i];
                    if (c != null)
                    {
                        var ou = outs[i];
                        if (ou != -1L)
                            cx.AddValue((SqlValue)cx.obs[ou], c);
                    }
                }
            }
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (FetchStatement)base._Replace(cx, so, sv);
            var nc = ((SqlValue)cx.obs[r.cursor])._Replace(cx, so, sv).defpos;
            if (nc != r.cursor)
                r += (Cursor, nc);
            var no = cx.ReplacedLl(r.outs);
            if (no != r.outs)
                r += (Outs, no);
            cx.done += (defpos, r);
            var nw = ((SqlValue)cx.obs[r.where])._Replace(cx, so, sv).defpos;
            if (nw != r.where)
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
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
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
            Parms = -133, // CList<long> SqlValue
            ProcDefPos = -134, // long Procedure
            Var = -135; // long SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public long var => (long)(mem[Var] ?? -1L);
        /// <summary>
        /// We need to allow the Call to have a name because we may not have resolved the target object
        /// </summary>
        public string name => (string)mem[ObInfo.Name];
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public long procdefpos => (long)mem[ProcDefPos];
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public CList<long> parms =>
            (CList<long>)mem[Parms] ?? CList<long>.Empty;
        public CTree<long,bool> aggs =>
            (CTree<long,bool>)mem[Domain.Aggs]??CTree<long,bool>.Empty;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long lp, Context cx,Procedure pr, string pn, 
            CList<long> acts, SqlValue tg = null)
         : this(lp,cx, (Procedure)cx.Add(pr?.Instance(lp,cx)), pn, acts, 
               (tg == null) ? null : 
               new BTree<long, object>(Var, tg.defpos) + (_Domain,pr?.domain??Domain.Content.defpos))
        { }
        protected CallStatement(long dp, Context cx,Procedure pr, string pn, CList<long> acts, 
            BTree<long, object> m = null)
         : base(dp, _Mem(cx,pr,acts,m) + (Parms, acts) + (ProcDefPos, pr?.defpos ?? -1L)
               + (ObInfo.Name, pr?.infos[cx.role.defpos]?.name ?? pn))
        { }
        protected CallStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,Procedure pr,CList<long> acts,BTree<long,object>m)
        {
            m = m ?? BTree<long, object>.Empty;
            var dm = cx._Dom(pr)??Domain.Content;
            var ag = CTree<long, bool>.Empty;
            for (var b=acts.First();b!=null;b=b.Next())
            {
                var pa = (SqlValue)cx.obs[b.value()];
                ag += pa.IsAggregation(cx);
            }
            if (ag!=dm.aggs)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static CallStatement operator +(CallStatement s, (long, object) x)
        {
            return new CallStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CallStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CallStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CallStatement)base._Relocate(cx);
            r += (ProcDefPos, cx.Fix(procdefpos));
            r += (Parms, cx.FixLl(parms));
            r += (Var, cx.Fix(var));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (CallStatement)base._Fix(cx);
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
            if (var != -1L && cx.obs[var].Calls(defpos, cx))
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
            var proc = (Procedure)cx.db.objects[procdefpos];
            return proc.Exec(cx, parms);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var nv = cx.ObReplace(var, so, sv);
            if (nv != var)
                r += (Var, nv);
            var np = r.parms;
            for (var b = parms.First(); b != null; b = b.Next())
            {
                var a = cx.ObReplace(b.value(), so, sv);
                if (a != b.value())
                    np += (b.key(), a);
            }
            if (np != r.parms)
                r += (Parms, np);
            if (r!=this)
                r = (CallStatement)New(cx, r.mem);
            cx.done += (defpos, r);
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
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
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
            SetList = -139, // CTree<Sqlx,long>
            SType = -140; // Sqlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype => (Sqlx)(mem[SType] ?? Sqlx.NO); // RAISE or RESIGNAL
        internal string signal => (string)mem[_Signal];
        internal BList<string> objects => (BList<string>)mem[Objects];
        internal CTree<Sqlx, long> setlist =>
            (CTree<Sqlx, long>)mem[SetList] ?? CTree<Sqlx, long>.Empty;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public SignalStatement(long dp, string n, params string[] obs) 
            : base(dp, _Mem(obs) +(_Signal, n))
        { }
        protected SignalStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(string[] obs)
        {
            var r = BList<string>.Empty;
            foreach (var o in obs)
                r += o;
            return new BTree<long, object>(Objects, r);
        }
        public static SignalStatement operator +(SignalStatement s, (long, object) x)
        {
            return new SignalStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SignalStatement(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SignalStatement(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SignalStatement)base._Relocate(cx);
            r += (SetList, cx.Fix(setlist));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SignalStatement)base._Fix(cx);
            var ns = cx.Fix(setlist);
            if (ns != setlist)
                r += (SetList, ns);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = setlist.First(); b != null; b = b.Next())
                if (cx.obs[b.value()].Calls(defpos, cx))
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
            if (stype == Sqlx.RESIGNAL && !cx.tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            string sclass = signal.Substring(0, 2);
            var dia = cx.tr.diagnostics;
            dia += (Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            for (var s = setlist.First(); s != null; s = s.Next())
                dia += (s.key(), cx.obs[s.value()].Eval(cx));
            cx.db += (Transaction.Diagnostics, dia);
            Handler h = null;
            Activation cs = null;
            for (cs = a; h == null && cs != null;)
            {
                h = cs.exceptions[signal];
                if (h == null && Char.IsDigit(signal[0]))
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
                a.signal = new Signal(defpos,signal,objects);
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
            var e = new DBException(signal, objects);
                for (var x = setlist.First(); x != null; x = x.Next())
                    e.info += (x.key(), cx.obs[x.value()].Eval(cx));
            throw e;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SignalStatement)base._Replace(cx, so, sv);
            var sl = CTree<Sqlx, long>.Empty;
            for (var b = r.setlist.First(); b != null; b = b.Next())
                sl += (b.key(), cx.uids[b.value()] ?? b.value());
            r += (SetList, sl);
            cx.done += (defpos, r);
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
            {
                sb.Append(cs); cs = ";";
                sb.Append(b.key()); sb.Append("=");
                sb.Append(Uid(b.value()));
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
        internal Exception exception;
        internal CTree<Sqlx, long> setlist = CTree<Sqlx, long>.Empty;
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
                objects += o.ToString();
        }
        public Signal(long dp, DBException e)
        {
            defpos = dp;
            signal = e.signal;
            exception = e;
            foreach (var o in e.objects)
                objects += o.ToString();
        }
        /// <summary>
        /// Execute a signal
        /// </summary>
        /// <param name="tr">the transaction</param>
        public Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            if (stype == Sqlx.RESIGNAL && !cx.tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                throw new DBException("0K000").ISO();
            string sclass = signal.Substring(0, 2);
            var dia = cx.tr.diagnostics;
            dia += (Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    dia += (b.key(), b.value());
                dia += (Sqlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
            }
            for (var s = setlist.First(); s != null; s = s.Next())
                dia += (s.key(), cx.obs[s.value()].Eval(cx));
            cx.db += (Transaction.Diagnostics, dia);
            Handler h = null;
            Activation cs;
            for (cs = a; h == null && cs != null;)
            {
                h = cs.exceptions[signal];
                if (h == null && Char.IsDigit(signal[0]))
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
                    e.info += (x.key(), cx.obs[x.value()].Eval(cx));
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
            (CTree<long, Sqlx>)mem[List] ?? CTree<long, Sqlx>.Empty;
        internal GetDiagnostics(long dp) : base(dp, BTree<long, object>.Empty) { }
        protected GetDiagnostics(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GetDiagnostics operator +(GetDiagnostics s, (long, object) x)
        {
            return new GetDiagnostics(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GetDiagnostics(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new GetDiagnostics(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (List, cx.Fix(list));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (GetDiagnostics)base._Fix(cx);
            var nl = cx.Fix(list);
            if (nl != list)
                r += (List, nl);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = list.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Calls(defpos, cx))
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
                cx.AddValue(cx.obs[b.key()], cx.tr.diagnostics[b.value()]);
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (GetDiagnostics)base._Replace(cx, so, sv);
            var ls = CTree<long, Sqlx>.Empty;
            for (var b = r.list.First(); b != null; b = b.Next())
                ls += (cx.uids[b.key()] ?? b.key(), b.value());
            r += (List, ls);
            cx.done += (defpos, r);
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
            Outs = -142; // CList<long> SqlValue
        /// <summary>
        /// The query
        /// </summary>
		public long sel => (long)(mem[ForSelectStatement.Sel] ?? -1L);
        /// <summary>
        /// The output list
        /// </summary>
		public CList<long> outs => (CList<long>)mem[Outs] ?? CList<long>.Empty;
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
            return new SelectSingle(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectSingle(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectSingle(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SelectSingle)base._Relocate(cx);
            r += (ForSelectStatement.Sel, cx.Fix(sel));
            r += (Outs, cx.FixLl(outs));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SelectSingle)base._Fix(cx);
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
            var rb = ((RowSet)cx.obs[sel]).First(cx);
            a.AddValue(this, rb);
            if (rb != null)
                for (var b = outs.First(); b != null; b = b.Next())
                    a.AddValue((SqlValue)cx.obs[b.value()], rb[b.key()]);
            else
                a.NoData();
            return cx;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SelectSingle)base._Replace(cx, so, sv);
            var no = cx.ReplacedLl(r.outs);
            if (no != r.outs)
                r += (Outs, no);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Sel=");sb.Append(Uid(sel));
            var cm = " Outs: ";
            for (var b = outs.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
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
            InsCols = -241, // CList<long> SqlValue
            Provenance = -155, //string
            Value = -156; // long RowSet
        internal long source => (long)(mem[RowSet._Source] ?? -1L);
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        public long value => (long)(mem[Value] ?? -1L);
        public CList<long> insCols => (CList<long>)mem[InsCols]; // tablecolumns if specified
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="fm">The rowset with target info</param>
        /// <param name="v">The uid of the data rowset</param>
        public SqlInsert(long dp, RowSet fm, string prov, long v, CList<long>iC)
           : base(dp, BTree<long, object>.Empty + (RowSet._Source, fm.defpos)
                 + (Provenance, prov) + (Value, v) + (InsCols, iC))
        { }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlInsert(defpos, m);
        }
        public static SqlInsert operator +(SqlInsert s, (long, object) x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx, so, sv);
            var tg = cx.ObReplace(source, so, sv);
            if (tg != source)
                r += (RowSet._Source, tg);
            if (r != this)
                r = (SqlInsert)New(cx, r.mem);
            cx.done += (defpos, r);
            return cx.Add(r);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlInsert(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlInsert)base._Relocate(cx);
            r += (RowSet._Source, cx.Fix(source));
            r += (Value, cx.Fix(value));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlInsert)base._Fix(cx);
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
            var tg = (RowSet)cx.obs[source];
            var data = (RowSet)cx.obs[value];
            if (provenance!=null)
                cx._Add(cx._Dom(data) + (Domain.Provenance, provenance));
            var ts = BTree<long, TargetActivation>.Empty;
            for (var it = tg.rsTargets.First(); it != null; it = it.Next())
            {
                var tb = cx.obs[it.value()];
                ts += tb.Insert(cx, data, insCols);
            }
            for (var ib = data.First(cx); ib != null; ib = ib.Next(cx))
                for (var it = ts.First(); it != null; it = it.Next())
                {
                    var ta = it.value();
                    ta.db = cx.db;
                    ta.cursors = cx.cursors;
                    ta.cursors += (ta._fm.defpos, ib);
                    ta.EachRow(ib._pos);
                    cx.db = ta.db;
                    ts += (it.key(), ta);
                }
            for (var c = ts.First(); c != null; c = c.Next())
            {
                var ta = c.value();
                ta.db = cx.db;
                ta.Finish();
                ts += (c.key(), ta);
                cx.affected += ta.affected;
            }
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");
            sb.Append(Uid(source));
            sb.Append(" Value: "); sb.Append(Uid(value));
            if (provenance != null)
            { sb.Append(" Provenance: "); sb.Append(provenance); }
            if (insCols!=null)
            {
                sb.Append(" Columns: [");
                var cm = "";
                for (var b=insCols.First(); b != null; b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
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
            Grant.Privilege how, CTree<UpdateAssignment, bool> ua = null)
            : base(dp, BTree<long, object>.Empty
                  + (RowSet._Source, f.defpos)
                  + (_Depth, f.depth + 1) + (RowSet.Assig, ua))
        {
            if (cx._Dom(f).rowType.Length == 0)
                throw new DBException("2E111", cx.db.user, dp).Mix();
        }
        protected QuerySearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QuerySearch operator +(QuerySearch q, (long, object) x)
        {
            return (QuerySearch)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySearch(defpos, m);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuerySearch)base._Replace(cx, so, sv);
            if (r != this)
                r = (QuerySearch)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuerySearch(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (QuerySearch)base._Relocate(cx);
            r += (RowSet._Source, cx.Fix(source));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (QuerySearch)base._Fix(cx);
            r += (RowSet._Source, cx.Fix(source));
            return r;
        }
        public override Context Obey(Context cx)
        {
            var tg = (RowSet)cx.obs[source];
            var ts = BTree<long, TargetActivation>.Empty;
            for (var it = tg.rsTargets.First(); it != null; it = it.Next())
            {
                var tb = cx.obs[it.value()];
                ts += tb.Delete(cx, tg);
            }
            for (var ib = tg.First(cx); ib != null; ib = ib.Next(cx))
                for (var it = ts.First(); it != null; it = it.Next())
                {
                    var ta = it.value();
                    ta.db = cx.db;
                    ta.cursors = cx.cursors;
                    ta.cursors += (ta._fm.defpos, ib);
                    ta.EachRow(ib._pos);
                    cx.db = ta.db;
                    ts += (it.key(), ta);
                }
            for (var c = ts.First(); c != null; c = c.Next())
            {
                var ta = c.value();
                ta.db = cx.db;
                ta.Finish();
                ts += (c.key(), ta);
                cx.affected += ta.affected;
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
            return (UpdateSearch)u.New(u.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateSearch(defpos, m);
        }
        public override Context Obey(Context cx)
        {
            cx.result = 0;
            var tg = (RowSet)cx.obs[source];
            var ts = BTree<long, TargetActivation>.Empty;
            for (var it = tg.rsTargets.First(); it != null; it = it.Next())
            {
                var tb = cx.obs[it.value()];
                ts += tb.Update(cx, tg);
            }
            for (var ib = tg.First(cx); ib != null; ib = ib.Next(cx))
                for (var it = ts.First(); it != null; it = it.Next())
                {
                    var ta = it.value();
                    ta.db = cx.db;
                    ta.cursors = cx.cursors;
                    ta.cursors += (ta._fm.defpos, ib);
                    ta.EachRow(ib._pos);
                    cx.db = ta.db;
                    ts += (it.key(), ta);
                }
            for (var c = ts.First(); c != null; c = c.Next())
            {
                var ta = c.value();
                ta.db = cx.db;
                ta.Finish();
                ts += (c.key(), ta);
                cx.affected += ta.affected;
            }
            return cx;
        }
        internal override DBObject Relocate(long dp)
        {
            return new UpdateSearch(dp, mem);
        }
    }

}