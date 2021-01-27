using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
	internal class Executable : DBObject
	{
        public enum Type
        {
            // cf SQL2011-2 Table 32
            NoType = 0,
            AlterDomain = 3,
            AlterTable = 4,
            Call = 7,
            Clearance = 8,
            CloseCursor = 9,
            CreateRoutine = 14,
            AlterRoutine = 17,
            DeleteWhere = 19,
            Select = 21,
            CreateDomain = 23,
            DropDomain = 27,
            DropRole = 29,
            DropRoutine = 30,
            DropTable = 32,
            DropTrigger = 34,
            Fetch = 45,
            GetDescriptor = 47,
            Grant = 48,
            GrantRole = 49,
            Insert = 50,
            Open = 53,
            Return = 58,
            Revoke = 59,
            AlterType = 60,
            CreateRole = 61,
            RollbackWork = 62,
            SelectSingle = 65,
            SetRole = 73,
            SetSessionAuthorization = 76,
            CreateTable = 77,
            CreateTrigger = 80,
            UpdateWhere = 82,
            CreateType = 83,
            CreateView = 84,
            SetSessionCharacteristics = 109,
            StartTransaction = 111,
            CreateOrdering = 114,
            DropOrdering = 115,
            RevokeRole = 129,
            // additional command codes
            Signal = 200,
            HttpPost = 201,
            HttpPut = 202,
            HttpDelete = 203,
            Compound = 204,
            VariableDec = 205,
            HandlerDef = 206,
            Break = 207,
            Assignment = 208,
            Case = 209,
            IfThenElse = 210,
            While = 211,
            Repeat = 212,
            Iterate = 213,
            Loop = 214,
            ForSelect = 215,
            Namespace = 216,
            Drop = 217,
            Constraint = 218,
            Column = 219,
            Period = 220,
            Rename = 223,
            When = 230,
            Commit = 231
        }

        internal const long
            Label = -92, // string
            Stmt = -93, // string
            _Type = -94; // Executable.Type
        public string name => (string)mem[Name] ?? "";
        public string stmt => (string)mem[Stmt];
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal string label => (string)mem[Label];
        internal Type type => (Type)(mem[_Type]??Type.NoType);
        /// <summary>
        /// Constructor: define an Executable of a given type.
        /// Procedure statements are subclasses of Executable
        /// </summary>
        /// <param name="t">The Executable.Type</param>
        public Executable(long dp, Type t) : this(dp,new BTree<long,object>(_Type,t))
        { }
        protected Executable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Executable operator+(Executable e,(long,object)x)
        {
            return new Executable(e.defpos, e.mem + x);
        }
        /// <summary>
        /// Support execution of a list of Executables, in an Activation.
        /// With break behaviour
        /// </summary>
        /// <param name="e">The Executables</param>
        /// <param name="tr">The transaction</param>
		protected Context ObeyList(BList<long> e,Context cx)
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
                    var x = (Executable)cx.obs[b.value()];
                    nx = x.Obey(cx);
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
            if (type == Type.RollbackWork)
                throw cx.db.Exception("40000").ISO();
            return cx;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Executable(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return (Executable)New(mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = ((DBObject)base._Relocate(wr)).Relocate(wr.Fix(defpos));
            if (mem.Contains(TableExpression.Nuid))
            { 
                var nu = (long)mem[TableExpression.Nuid];
                var nn = nu;
                if (wr.rss.Contains(nu))
                    nn = wr.rss[nu].defpos;
                else if (wr.uids.Contains(nu))
                    nn = wr.uids[nu];
                if (nn != nu)
                    r += (TableExpression.Nuid, nn);
            }
            return r;
        }
        internal static bool Calls(BList<long> ss,long defpos,Context cx)
        {
            for (var b = ss?.First(); b != null; b = b.Next())
                if (cx.obs[b.value()].Calls(defpos, cx))
                    return true;
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (mem.Contains(Label)) { sb.Append(" "); sb.Append(label); }
            sb.Append(" ");sb.Append(Uid(defpos));
            if (mem.Contains(TableExpression.Nuid))
            { sb.Append(" Nuid="); sb.Append(Uid((long)mem[TableExpression.Nuid])); }
            if (mem.Contains(Stmt)) { sb.Append(" Stmt:"); sb.Append(stmt); }
            return sb.ToString();
        }
    }
    internal class CommitStatement : Executable
    {
        public CommitStatement(long dp) : base(dp, Type.Commit) { }
    }
    /// <summary>
    /// A Select Statement can be used in a stored procedure so is a subclass of Executable
    /// </summary>
    internal class SelectStatement : Executable
    {
        internal const long
             CS = -95; //long CursorSpecification
        /// <summary>
        /// The cusorspecification (a Query) for this executable
        /// </summary>
        public long cs=>(long)(mem[CS]??-1L);
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The cursor specification</param>
        public SelectStatement(long dp, CursorSpecification c)
            : base(dp, new BTree<long, object>(CS, c.defpos) + (Dependents, c.dependents)
                  +(Depth,1+c.depth))
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SelectStatement)base._Relocate(wr);
            r += (CS, wr.Fixed(cs).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SelectStatement)base.Fix(cx);
            var nc = cx.obuids[cs] ?? cs;
            if (nc != cs)
                r += (CS, nc);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[cs].Calls(defpos, cx);
        }
        /// <summary>
        /// Obey the executable
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
        //    if (!cx.results.Contains(cs))
        //        ((Query)cx.obs[cs]).RowSets(cx, BTree<long,RowSet.Finder>.Empty);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" CS="); sb.Append(Uid(cs));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Compound Statement for the SQL procedure language
    /// </summary>
    internal class CompoundStatement : Executable
    {
        internal const long
             Stms = -96; // BList<long> Executable
        /// <summary>
        /// The contained list of Executables
        /// </summary>
		public BList<long> stms =>
            (BList<long>)mem[Stms] ?? BList<long>.Empty;
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (CompoundStatement)base._Relocate(wr);
            r += (Stms, wr.Fix(stms));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CompoundStatement)base.Fix(cx);
            var nc = cx.Fix(stms);
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
    internal class PreparedStatement : Executable
    {
        internal const long
            QMarks = -396,  // CList<long> SqlValue
            Target = -397; // Executable
        internal CList<long> qMarks =>
            (CList<long>)mem[QMarks] ?? CList<long>.Empty;
        internal Executable target => (Executable)mem[Target];
        public PreparedStatement(Context cx) 
            :base(_Unheap(cx,cx.exec.defpos-1),_Mem(cx)) // actually will call UnLex
        {  }
        static long _Unheap(Context cx,long dp)
        {
            cx.ObUnLex(dp);
            return cx.obuids[dp]??dp;
        }
        static BTree<long,object> _Mem(Context cx)
        {
            if (cx.exec is SelectStatement ss)
                ((Query)cx.obs[ss.cs]).RowSets(cx, BTree<long, RowSet.Finder>.Empty);
            cx.unLex = true;
            var f = new Framing(cx);
            f.Relocate(cx);
            cx.Scan(cx.qParams);
            f = (Framing)f.Fix(cx);
            cx._Add((Executable)cx.exec.Fix(cx));
            var p = cx.exec.defpos;
            var r = new BTree<long, object>(_Framing, f) 
                + (Target,f.obs[cx.obuids[p]??p])
                + (QMarks,cx.Fix(cx.qParams));
            f.Install(cx);
            cx.unLex = false;
            return r;
        }
        public override Context Obey(Context cx)
        {
            cx.Install2(framing);
            return target.Obey(cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");sb.Append(target);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A local variable declaration.
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (LocalVariableDec)base._Relocate(wr);
            r += (AssignmentStatement.Vbl, wr.Fixed(vbl).defpos);
            r += (Init, wr.Fixed(init)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (LocalVariableDec)base.Fix(cx);
            var nb = cx.obuids[vbl] ?? vbl;
            if (nb != vbl)
                r += (AssignmentStatement.Vbl, nb);
            var ni = cx.obuids[init] ?? init;
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
            TypedValue tv = cx.obs[init]?.Eval(cx)??vb.domain.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.AddValue(vb, tv); // We expect a==cx, but if not, tv will be copied to a later
            return cx;
        }
        internal override BTree<long, RowSet.Finder> Needs(Context context, RowSet rs)
        {
            return BTree<long, RowSet.Finder>.Empty;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(label); sb.Append(' '); sb.Append(Uid(defpos));
            sb.Append(' ');  sb.Append(domain.kind);
            return sb.ToString();
        }
	}
    /// <summary>
    /// A procedure formal parameter has mode and result info
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
            : base(vp,new BTree<long, object>(ParamMode, m)+(Name,n)+(AssignmentStatement.Val,vp)
                  +(_Domain,dt))
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (FormalParameter)base._Relocate(wr);
            return r+(AssignmentStatement.Val,wr.Fixed(val).defpos);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (FormalParameter)base.Fix(cx);
            var nv = cx.obuids[val] ?? val;
            if (nv != val)
                r += (AssignmentStatement.Val, nv);
            return r;
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.FindCx(defpos).values[defpos];
        }
        internal override BTree<long, RowSet.Finder> Needs(Context context, RowSet rs)
        {
            return BTree<long, RowSet.Finder>.Empty;
        }
        /// <summary>
        /// A readable version of the ProcParameter
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(domain);
            if (mem.Contains(ParamMode)) { sb.Append(" "); sb.Append(paramMode); }
            if (mem.Contains(Result)) { sb.Append(" "); sb.Append(result); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A local cursor
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
        public CursorDeclaration(long dp, Context cx,SqlCursor sc,CursorSpecification c) 
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (CS, wr.Fixed(cs).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CursorDeclaration)base.Fix(cx);
            var nc = cx.obuids[cs] ?? cs;
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
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" cs=");sb.Append(Uid(cs));
            return sb.ToString();
        }

    }
    /// <summary>
    /// An Exception handler for a stored procedure
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
            : base(dp, BTree<long, object>.Empty + (HType, t) + (Name, n)) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (HandlerStatement)base._Relocate(wr);
            r += (Action, wr.Fixed(action).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (HandlerStatement)base.Fix(cx);
            var na = cx.obuids[action] ?? action;
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
    /// </summary>
	internal class Handler : Executable
	{
        internal const long
            HDefiner = -103, // Activation
            Hdlr = -104; // HandlerStatement
        /// <summary>
        /// The shared handler statement
        /// </summary>
		HandlerStatement hdlr => (HandlerStatement)mem[Hdlr];
        /// <summary>
        /// The activation that defined this
        /// </summary>
        Activation hdefiner =>(Activation)mem[HDefiner];
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Handler)base._Relocate(wr);
            r += (Hdlr, hdlr.Relocate(wr));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Handler)base.Fix(cx);
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
                if (hdlr.htype == Sqlx.UNDO)
                {
                    CompoundStatement cs = null;
                    for (Context p = cx; cs == null && p != null; p = p.next)
                        cs = p.exec as CompoundStatement;
                    if (cs != null)
                    {
                        cx.db = hdefiner.saved.mark;
                        cx.next = hdefiner.saved.stack;
                    }
                }
                ((Executable)cx.obs[hdlr.action]).Obey(cx);
                if (hdlr.htype == Sqlx.EXIT)
                    return hdefiner.next;
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
            sb.Append(" Hdef=");sb.Append(Uid(hdefiner.cxid));
            sb.Append(" Hdlr=");sb.Append(Uid(hdlr.defpos));
            return sb.ToString();
        }

    }
    /// <summary>
    /// A Break statement for a stored procedure
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
		public AssignmentStatement(long dp,SqlValue vb,SqlValue va) 
            : base(dp,BTree<long, object>.Empty+(Vbl,vb.defpos)+(Val,va.defpos)
                  +(Dependents,CTree<long,bool>.Empty+(vb.defpos,true)+(va.defpos,true)))
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (AssignmentStatement)base._Relocate(wr);
            r += (Val, wr.Fixed(val).defpos);
            r += (Vbl, wr.Fixed(vbl).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (AssignmentStatement)base.Fix(cx);
            var na = cx.obuids[val] ?? val;
            if (na!=val)
                r += (Val, na);
            var nb = cx.obuids[vbl] ?? vbl;
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
            var vb = cx.obs[vbl];
            var t = vb.domain;
            if (val != -1L)
            {
                var v = t.Coerce(cx,cx.obs[val].Eval(cx));
                cx.AddValue(vb, v);
            }
            return cx;
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
    /// </summary>
    internal class MultipleAssignment : Executable
    {
        internal const long
            LhsType = -107, // long DBObject
            List = -108, // BList<long> SqlValue
            Rhs = -109; // long SqlValue
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal BList<long> list => (BList<long>)mem[List];
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
        public MultipleAssignment(long dp) : base(dp,BTree<long,object>.Empty)
        { }
        protected MultipleAssignment(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (MultipleAssignment)base._Relocate(wr);
            r += (LhsType, wr.Fixed(lhsType).defpos);
            r +=(List, wr.Fix(list));
            r += (Rhs, wr.Fixed(rhs).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (MultipleAssignment)base.Fix(cx);
            var nt = cx.obuids[lhsType] ?? lhsType;
            if (nt != lhsType)
                r += (LhsType, nt);
            var nl = cx.Fix(list);
            if (nl != list)
                r += (List, nl);
            var nr = cx.obuids[rhs] ?? rhs;
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
        public ReturnStatement(long dp) : base(dp, BTree<long, object>.Empty)
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (ReturnStatement)base._Relocate(wr);
            r += (Ret, wr.Fixed(ret).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (ReturnStatement)base.Fix(cx);
            var nr = cx.obuids[ret] ?? ret;
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
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" -> ");sb.Append(Uid(ret));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Case statement for a stored procedure
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        internal const long
            Else = -111, // BList<long> Executable
            Operand = -112, // long SqlValue
            Whens = -113; // BList<long> WhenPart
        /// <summary>
        /// The test expression
        /// </summary>
        public long operand => (long)(mem[Operand]??-1L);
        /// <summary>
        /// A list of when parts
        /// </summary>
        public BList<long> whens => (BList<long>)mem[Whens]?? BList<long>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
        public BList<long> els => (BList<long>)mem[Else]?? BList<long>.Empty;
        /// <summary>
        /// Constructor: a case statement from the parser
        /// </summary>
        public SimpleCaseStatement(long dp) : base(dp,BTree<long,object>.Empty)
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SimpleCaseStatement)base._Relocate(wr);
            r += (Else, wr.Fix(els));
            r += (Operand, wr.Fixed(operand).defpos);
            r += (Whens, wr.Fix(whens));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SimpleCaseStatement)base.Fix(cx);
            var ne = cx.Fix(els);
            if (ne!=els)
            r += (Else, ne);
            var no = cx.obuids[operand] ?? operand;
            if (no!=operand)
            r += (Operand, no);
            var nw = cx.Fix(whens);
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
    /// </summary>
	internal class SearchedCaseStatement : Executable
	{
        /// <summary>
        /// A list of when parts
        /// </summary>
		public BList<long> whens => (BList<long>)mem[SimpleCaseStatement.Whens];
        /// <summary>
        /// An else part
        /// </summary>
		public BList<long> els => (BList<long>)mem[SimpleCaseStatement.Else]?? BList<long>.Empty;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(long dp) : base(dp,BTree<long,object>.Empty)
        {  }
        protected SearchedCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SearchedCaseStatement)base._Relocate(wr);
            r += (SimpleCaseStatement.Else,wr.Fix(els));
            r += (SimpleCaseStatement.Whens, wr.Fix(whens));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SearchedCaseStatement)base.Fix(cx);
            var ne = cx.Fix(els);
            if (ne != els)
                r += (SimpleCaseStatement.Else, ne);
            var nw = cx.Fix(whens);
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
    /// </summary>
	internal class WhenPart :Executable
	{
        internal const long
            Cond = -114, // long SqlValue
            Stms = -115; // BList<long> Executable
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public long cond => (long)(mem[Cond]??-1L);
        /// <summary>
        /// a list of statements
        /// </summary>
		public BList<long> stms =>(BList<long>)mem[Stms]?? BList<long>.Empty;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A list of statements for this when</param>
        public WhenPart(long dp,SqlValue v, BList<long> s) 
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (WhenPart)base._Relocate(wr);
            r += (Cond, wr.Fixed(cond)?.defpos??-1L);
            r += (Stms, wr.Fix(stms));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (WhenPart)base.Fix(cx);
            var nc = cx.obuids[cond] ?? cond;
            if (cond != nc)
                r += (Cond, nc);
            var ns = cx.Fix(stms);
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
    /// </summary>
	internal class IfThenElse : Executable
	{
        internal const long
            Else = -116, // BList<long> Executable
            Elsif = -117, // BList<long> Executable
            Search = -118, // long SqlValue
            Then = -119; // BList<long> Executable
        /// <summary>
        /// The test condition
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
        /// <summary>
        /// The then statements
        /// </summary>
		public BList<long> then => (BList<long>)mem[Then]?? BList<long>.Empty;
        /// <summary>
        /// The elsif parts
        /// </summary>
		public BList<long> elsif => (BList<long>)mem[Elsif] ?? BList<long>.Empty;
        /// <summary>
        /// The else part
        /// </summary>
		public BList<long> els => (BList<long>)mem[Else] ?? BList<long>.Empty;
        /// <summary>
        /// Constructor: an if-then-else statement from the parser
        /// </summary>
		public IfThenElse(long dp) : base(dp,BTree<long,object>.Empty)
		{}
        protected IfThenElse(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (IfThenElse)base._Relocate(wr);
            r += (Search, wr.Fixed(search).defpos);
            r += (Then,wr.Fix(then));
            r += (Else, wr.Fix(els)); 
            r += (Elsif, wr.Fix(elsif)); 
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (IfThenElse)base.Fix(cx);
            var ns = cx.obuids[search] ?? search;
            if (ns != search)
                r += (Search, ns);
            var nt = cx.Fix(then);
            if (nt != then)
                r += (Then, nt);
            var ne = cx.Fix(els);
            if (els != ne)
                r += (Else, ne);
            var ni = cx.Fix(elsif);
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
    /// </summary>
	internal class WhileStatement : Executable
	{
        internal const long
            Loop = -121, // long Executable
            Search = -122, // long SqlValue
            What = -123; // BList<long> Executable
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public long search => (long)(mem[Search]??-1L);
        /// <summary>
        /// The statements to execute
        /// </summary>
		public BList<long> what => (BList<long>)mem[What]?? BList<long>.Empty;
        public long loop => (long)(mem[Loop] ?? 0); 
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (WhileStatement)base._Relocate(wr);
            r += (Loop, wr.Fixed(loop).defpos);
            r += (Search, wr.Fixed(search).defpos);
            r += (What, wr.Fix(what));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (WhileStatement)base.Fix(cx);
            var nl = cx.obuids[loop] ?? loop;
            if (nl != loop)
                r += (Loop, nl);
            var ns = cx.obuids[search] ?? search;
            if (ns != search)
                r += (Search, ns);
            var nw = cx.Fix(what);
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
            if (loop != -1L)
            { sb.Append(" Loop: "); sb.Append(Uid(loop)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A repeat statement for a stored proc/func
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
		public BList<long> what => (BList<long>)mem[WhileStatement.What];
        public long loop => (long)(mem[WhileStatement.Loop]??0L);
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (RepeatStatement)base._Relocate(wr);
            r += (WhileStatement.Loop, wr.Fix(loop));
            r += (WhileStatement.Search, wr.Fixed(search).defpos);
            r += (WhileStatement.What, wr.Fix(what));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RepeatStatement)base.Fix(cx);
            var nl = cx.obuids[loop] ?? loop;
            if (loop != nl)
                r += (WhileStatement.Loop, nl);
            var ns = cx.obuids[search] ?? search;
            if (ns != search)
                r += (WhileStatement.Search, ns);
            var nw = cx.Fix(what);
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
            if (loop != -1L)
            { sb.Append(" Loop: "); sb.Append(Uid(loop)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// An Iterate (like C continue;) statement for a stored proc/func 
    /// </summary>
	internal class IterateStatement : Executable
	{
        /// <summary>
        /// Constructor: an iterate statement from the parser
        /// </summary>
        /// <param name="n">The name of the iterator</param>
		public IterateStatement(long dp,string n,long i):base(dp,BTree<long,object>.Empty
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
    /// </summary>
	internal class LoopStatement : Executable
	{
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<long> stms => (BList<long>)mem[WhenPart.Stms];
        public long loop => (long)(mem[WhileStatement.Loop]??0);
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (LoopStatement)base._Relocate(wr);
            r += (WhileStatement.Loop, wr.Fix(loop));
            r += (WhenPart.Stms, wr.Fix(stms));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (LoopStatement)base.Fix(cx);
            var nl = cx.obuids[loop] ?? loop;
            if (nl != loop)
                r += (WhileStatement.Loop, nl);
            var ns = cx.Fix(stms);
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
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " Stms: ";
            for (var b = stms.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            if (loop != -1L)
            { sb.Append(" Loop: "); sb.Append(Uid(loop)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A for statement for a stored proc/func
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        internal const long
            Cursor = -124, // long SqlCursor
            ForVn = -125, // string
            Loop = -126, // long
            Sel = -127, // long CursorSpecification
            Stms = -128; // BList<long> Executable
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public long sel => (long)(mem[Sel]??-1L);
        /// <summary>
        /// The SqlCursor
        /// </summary>
		public long cursor => (long)mem[Cursor];
        /// <summary>
        /// The identifier in the AS part
        /// </summary>
		public string forvn => (string)mem[ForVn];
        /// <summary>
        /// The FOR loop
        /// </summary>
        public long loop => (long)(mem[Loop] ?? -1L);
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<long> stms => (BList<long>)mem[Stms];
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
        public ForSelectStatement(long dp, string n) : base(dp,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected ForSelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (ForSelectStatement)base._Relocate(wr);
            r += (WhileStatement.Loop, wr.Fixed(loop)?.defpos??-1L);
            r += (Cursor, wr.Fix(cursor));
            r += (Sel, wr.Fix(sel));
            r += (Stms, wr.Fix(stms));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (ForSelectStatement)base.Fix(cx);
            var nl = cx.obuids[loop] ?? loop;
            if (nl != loop)
                r += (WhileStatement.Loop, nl);
            var nc = cx.obuids[cursor] ?? cursor;
            if (nc != cursor)
                r += (Cursor, nc);
            var ns = cx.obuids[sel] ?? sel;
            if (ns != sel)
                r += (Sel, ns);
            var nn = cx.Fix(stms);
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
            var da = ((Query)cx.obs[sel]).RowSets(cx,BTree<long,RowSet.Finder>.Empty);
            if (da == null)
                return cx;
            cx.data += (sel, da);
            var cs = (CursorSpecification)cx.obs[sel];
            var qe = (QueryExpression)cx.obs[cs.union];
            var qs = (QuerySpecification)cx.obs[qe.left];
            var ac = new Activation(cx,label);
            for (var rb = da.First(ac); rb != null; rb = rb.Next(ac))
            {
                ac.Add(qs);
                ac.finder = cx.data[qs.defpos].finder;
                ac.brk = cx as Activation;
                ac.cont = ac;
                ac = (Activation)ObeyList(stms, ac);
                if (ac.signal != null)
                    ac.signal.Throw(cx);
            }
            return ac.SlideDown();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (forvn != null)
            { sb.Append(" Var="); sb.Append(forvn); }
            if (cursor != -1L)
            { sb.Append(" Cursor="); sb.Append(cursor); }
            sb.Append(" Sel="); sb.Append(Uid(sel));
            var cm = " Stms: ";
            for (var b = stms.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ";";
                sb.Append(Uid(b.value()));
            }
            if (loop != -1L)
            { sb.Append(" Loop: "); sb.Append(Uid(loop)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// An Open statement for a cursor
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (OpenStatement)base._Relocate(wr);
            r += (FetchStatement.Cursor, wr.Fix(cursor));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (OpenStatement)base.Fix(cx);
            var nc = cx.obuids[cursor] ?? cursor;
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
            var c = (SqlCursor)cx.obs[cursor];
            var rs = ((Query)cx.obs[c.spec]).RowSets(cx,BTree<long, RowSet.Finder>.Empty);
            cx.data += (c.spec, rs);
            return cx;
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
    /// </summary>
	internal class CloseStatement : Executable
	{
        public long cursor => (long)(mem[FetchStatement.Cursor]??-1L);
        /// <summary>
        /// Constructor: a close statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
		public CloseStatement(long dp, SqlCursor c,long i) : base(dp,BTree<long,object>.Empty
            +(FetchStatement.Cursor,c.defpos))
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (CloseStatement)base._Relocate(wr);
            r += (FetchStatement.Cursor, wr.Fix(cursor));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CloseStatement)base.Fix(cx);
            var nc = cx.obuids[cursor] ?? cursor;
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
            cx.data += (defpos,new EmptyRowSet(defpos,cx,cx.data[defpos]?.domain));
            return cx;
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
    /// </summary>
	internal class FetchStatement : Executable
	{
        internal const long
            Cursor = -129, // long SqlCursor
            How = -130, // Sqlx
            Outs = -131, // BList<long> SqlValue
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
		public BList<long> outs => (BList<long>)mem[Outs]?? BList<long>.Empty; 
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (FetchStatement)base._Relocate(wr);
            r += (Cursor, wr.Fixed(cursor).defpos);
            r += (Outs, wr.Fix(outs));
            r += (Where, wr.Fixed(where)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (FetchStatement)base.Fix(cx);
            var nc = cx.obuids[cursor] ?? cursor;
            if (nc != cursor)
                r += (Cursor, nc);
            var no = cx.Fix(outs);
            if (outs != no)
                r += (Outs, no);
            var nw = cx.obuids[where] ?? where;
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
            var cs = (Query)cx.obs[cu.spec];
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
                        for (var e = cx.data[cs.defpos].First(cx); e != null; e = e.Next(cx))
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
                rb = cx.data[cs.defpos].First(cx);
            while (rb != null && rqpos != rb._pos)
                rb = rb.Next(cx);
            cx.values += (cu.defpos, rb);
            if (rb == null)
                cx = new Signal(defpos, "02000", "No data").Obey(cx);
            else
            {
                var dt = cs.rowType;
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
    /// </summary>
	internal class CallStatement : Executable
	{
        internal const long
            Parms = -133, // CList<long> SqlValue
            ProcDefPos = -134, // long
            Var = -135; // long SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public long var => (long)(mem[Var]??-1L);
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public long procdefpos => (long)mem[ProcDefPos];
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public CList<long> parms =>
            (CList<long>)mem[Parms]?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long dp, Procedure pr, string pn, CList<long> acts, SqlValue tg=null)
         : this(dp, pr, pn, acts, (tg==null)?null: new BTree<long, object>(Var,tg.defpos))
        { }
        protected CallStatement(long dp, Procedure pr, string pn, CList<long> acts, BTree<long,object> m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Parms, acts) + (ProcDefPos, pr?.defpos??-1L)
               +(_Domain,pr?.domain??Domain.Content) + (Name,pn))
        {
   //         if (pr == null)
   //             throw new DBException("42108", pn);
        }
        protected CallStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CallStatement operator+(CallStatement s,(long,object)x)
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (CallStatement)base._Relocate(wr);
            r += (ProcDefPos, wr.Fix(procdefpos));
            r += (Parms, wr.Fix(parms));
            r += (Var, wr.Fix(var));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CallStatement)base.Fix(cx);
            var np = cx.obuids[procdefpos] ?? procdefpos;
            if (np!=procdefpos)
                r += (ProcDefPos, np);
            var ns = cx.Fix(parms);
            if (parms!=ns)
                r += (Parms, ns);
            var va = cx.obuids[var] ?? var;
            if (var!=va)
                r += (Var, va);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (var != -1L && cx.obs[var].Calls(defpos, cx))
                return true;
            return procdefpos==defpos || Calls(parms,defpos, cx);
        }
        internal void Grouped(Context cx,GroupSpecification gs)
        {
            ((SqlValue)cx.obs[var])?.Grouped(cx, gs);
            gs.Grouped(cx, parms);
        }
        /// <summary>
        /// Execute a proc/method call
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
		{
            cx.exec = this;
            var proc = (Procedure)cx.db.objects[procdefpos];
            return proc.Exec(cx,parms);
		}
        internal override DBObject _Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var nv = cx.Replace(var,so, sv);
            if (nv != var)
                r += (Var, nv);
            var np = r.parms;
            for (var b=parms.First();b!=null;b=b.Next())
            {
                var a = cx.Replace(b.value(), so, sv);
                if (a != b.value())
                    np += (b.key(), a);
            }
            if (np != r.parms)
                r += (Parms, np);
            r = (CallStatement)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (var!=-1L)
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
    /// </summary>
	internal class Signal : Executable
    {
        internal const long
            Exception = -136, // Exception
            Objects = -137, // BList<object>
            _Signal = -138, // string
            SetList = -139, // CTree<Sqlx,long>
            SType = -140; // Sqlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype => (Sqlx)(mem[SType] ?? Sqlx.NO); // RAISE or RESIGNAL
        internal string signal => (string)mem[_Signal];
        internal BList<object> objects => (BList<object>)mem[Objects];
        internal Exception exception => (Exception)mem[Exception];
        internal CTree<Sqlx, long> setlist =>
            (CTree<Sqlx, long>)mem[SetList] ?? CTree<Sqlx, long>.Empty;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public Signal(long dp, string n, params object[] m) : base(dp, _Mem(m) + (_Signal, n))
        { }
        public Signal(long dp, DBException e) : base(dp, _Mem(e.objects)
            + (_Signal, e.signal) + (Exception, e))
        { }
        protected Signal(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(object[] obs)
        {
            var r = BList<object>.Empty;
            foreach (var o in obs)
                r += o;
            return new BTree<long, object>(Objects, r);
        }
        public static Signal operator +(Signal s, (long, object) x)
        {
            return new Signal(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Signal(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Signal(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Signal)base._Relocate(wr);
            r += (SetList, wr.Fix(setlist));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Signal)base.Fix(cx);
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
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (stype!=Sqlx.NO)
            { sb.Append(" "); sb.Append(stype); }
            sb.Append(" "); sb.Append(signal);
            if (mem.Contains(Exception))
            {
                sb.Append(" Exception: "); sb.Append(exception);
                var cm = " ";
                for (var b = objects.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = " ";
                    sb.Append(b.value());
                } 
            }
            var cs = " Set: ";
            for (var b=setlist.First();b!=null;b=b.Next())
            {
                sb.Append(cs); cs = ";";
                sb.Append(b.key());sb.Append("=");
                sb.Append(Uid(b.value()));
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A GetDiagnostics statement for a routine
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (List, wr.Fix(list));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Executable)base.Fix(cx);
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
    /// </summary>
	internal class SelectSingle : Executable
    {
        internal const long
            Outs = -142; // BList<long> SqlValue
        /// <summary>
        /// The query
        /// </summary>
		public long sel => (long)(mem[ForSelectStatement.Sel] ?? -1L);
        /// <summary>
        /// The output list
        /// </summary>
		public BList<long> outs => (BList<long>)mem[Outs] ?? BList<long>.Empty;
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SelectSingle)base._Relocate(wr);
            r += (ForSelectStatement.Sel, wr.Fixed(sel).defpos);
            r += (Outs, wr.Fix(outs));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SelectSingle)base.Fix(cx);
            var ns = cx.obuids[sel] ?? sel;
            if (ns != sel)
                r += (ForSelectStatement.Sel, ns);
            var no = cx.Fix(outs);
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
            var sr = ((Query)cx.obs[sel]).RowSets(cx, BTree<long, RowSet.Finder>.Empty);
            var rb = sr.First(cx);
            a.AddValue(this, rb);
            if (rb != null)
                for (var b = outs.First(); b != null; b = b.Next())
                    a.AddValue((SqlValue)cx.obs[b.value()], rb[b.key()]);
            else
                a.NoData();
            return cx;
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
}