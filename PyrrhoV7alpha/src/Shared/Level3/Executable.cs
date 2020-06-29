using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
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
            return ((DBObject)base._Relocate(wr)).Relocate(wr.Fix(defpos));
        }
        internal override Basis _Relocate(Context cx)
        {
            return ((DBObject)base._Relocate(cx)).Relocate(cx.Unheap(defpos));
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
            return Uid(defpos)+" "+GetType().Name;
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
            : base(dp, new BTree<long, object>(CS, c.defpos) + (Dependents, c.dependents))
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
            var r = (SelectStatement)base._Relocate(wr);
            var c = (CursorSpecification)wr.Fixed(cs);
            if (c.defpos != cs)
                r += (CS, c.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SelectStatement)base._Relocate(cx);
            var c = (CursorSpecification)cx.Fixed(cs);
            if (c.defpos != cs)
                r += (CS, c.defpos);
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
            ((Query)cx.obs[cs]).RowSets(cx, BTree<long,RowSet.Finder>.Empty);
            return cx;
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
            var r = (CompoundStatement)base._Relocate(wr);
            var ss = BList<long>.Empty;
            var ch = false;
            for (var b=stms.First();b!=null;b=b.Next())
            {
                var s = (Executable)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                ss += s.defpos;
            }
            if (ch)
                r += (Stms, ss);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CompoundStatement)base._Relocate(cx);
            var ss = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var s = (Executable)cx.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                ss += s.defpos;
            }
            if (ch)
                r += (Stms, ss);
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
        internal readonly BList<SqlValue> qMarks;
        internal readonly Executable target;
        public PreparedStatement(Executable e,BList<SqlValue> q) 
            :base(e.defpos-1,BTree<long,object>.Empty)
        {
            target = e;
            qMarks = q;
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
          + (AssignmentStatement.Vbl, v.defpos))
        { }
        protected LocalVariableDec(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LocalVariableDec operator+(LocalVariableDec s,(long,object)x)
        {
            return new LocalVariableDec(s.defpos, s.mem + x);
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
            var r = (LocalVariableDec)base._Relocate(wr);
            var v = (SqlValue)wr.Fixed(vbl);
            if (v.defpos != vbl)
                r += (AssignmentStatement.Vbl, v.defpos);
            if (wr.Fixed(init) is Executable e && e.defpos != init)
                r += (Init, e.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (LocalVariableDec)base._Relocate(cx);
            var v = (SqlValue)cx.Fixed(vbl);
            if (v.defpos != vbl)
                r += (AssignmentStatement.Vbl, v.defpos);
            if (cx.Fixed(init) is Executable e && e.defpos != init)
                r += (Init, e.defpos);
            return r;
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
    internal class ParamInfo : DBObject
    {
        internal const long
            ParamMode = -98, // Sqlx
            Result = -99; // Sqlx
        public long val => (long)(mem[AssignmentStatement.Val] ?? -1L);
        public string name => (string)mem[Name] ?? "";
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
		public ParamInfo(long vp, Sqlx m, string n,Domain dt)
            : base(vp,new BTree<long, object>(ParamMode, m)+(Name,n)+(AssignmentStatement.Val,vp)
                  +(DBObject._Domain,dt))
        { }
        protected ParamInfo(long dp,BTree<long, object> m) : base(dp,m) { }
        public static ParamInfo operator +(ParamInfo s, (long, object) x)
        {
            return new ParamInfo(s.defpos,s.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ParamInfo(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (ParamInfo)base._Relocate(wr);
            var v = (SqlValue)wr.Fixed(val);
            return r+(AssignmentStatement.Val,v.defpos);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ParamInfo)base._Relocate(cx);
            var v = (SqlValue)cx.Fixed(val);
            return r + (AssignmentStatement.Val, v.defpos);
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

        internal override Basis New(BTree<long, object> m)
        {
            return new ParamInfo(defpos,m);
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
        public CursorDeclaration(long dp, Context cx,string n,CursorSpecification c) 
            : base(dp,cx,new SqlCursor(dp,c,n),new BTree<long,object>(CS,c.defpos)) 
        { }
        protected CursorDeclaration(long dp, BTree<long, object> m) : base(dp, m) { }
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
            var r = base._Relocate(wr);
            var c = (CursorSpecification)wr.Fixed(cs);
            if (c.defpos != cs)
                r += (CS, c.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            var c = (CursorSpecification)cx.Fixed(cs);
            if (c.defpos != cs)
                r += (CS, c.defpos);
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
            var r = (HandlerStatement)base._Relocate(wr);
            var a = (Executable)wr.Fixed(action);
            if (a.defpos != action)
                r += (Action, a.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (HandlerStatement)base._Relocate(cx);
            var a = (Executable)cx.Fixed(action);
            if (a.defpos != action)
                r += (Action, a.defpos);
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
            var r = (Handler)base._Relocate(wr);
            var h = hdlr.Relocate(wr);
            if (h != hdlr)
                r += (Hdlr, h);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Handler)base._Relocate(cx);
            var h = hdlr.Relocate(cx);
            if (h != hdlr)
                r += (Hdlr, h);
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
                  +(Dependents,BTree<long,bool>.Empty+(vb.defpos,true)+(va.defpos,true)))
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
            var r = (AssignmentStatement)base._Relocate(wr);
            var va = (SqlValue)wr.Fixed(val);
            if (va.defpos != val)
                r += (Val, va.defpos);
            var vb = (SqlValue)wr.Fixed(vbl);
            if (vb.defpos != vbl)
                r += (Vbl, vb.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (AssignmentStatement)base._Relocate(cx);
            var va = (SqlValue)cx.Fixed(val);
            if (va.defpos != val)
                r += (Val, va.defpos);
            var vb = (SqlValue)cx.Fixed(vbl);
            if (vb.defpos != vbl)
                r += (Vbl, vb.defpos);
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
                var v = t.Coerce(cx,cx.obs[val].Eval(cx)?.NotNull());
                cx.AddValue(vb, v);
            }
            return cx;
        }
        public override string ToString()
        {
            if (vbl!=-1L && val!=-1L)
                return Uid(vbl) + "=" + Uid(val);
            return base.ToString();
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
            var r = (MultipleAssignment)base._Relocate(wr);
            var lt = wr.cx.obs[lhsType].Relocate(wr);
            if (lt.defpos != lhsType)
                r += (LhsType, lt.defpos);
            var ls = BList<long>.Empty;
            var ch = false;
            for(var b=list.First();b!=null;b=b.Next())
            {
                var s = (SqlValue)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                ls += s.defpos;
            }
            if (ch)
                r +=(List, ls);
            var rh = (SqlValue)wr.Fixed(rhs);
            if (rh.defpos != rhs)
                r += (Rhs, rh.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (MultipleAssignment)base._Relocate(cx);
            var o = cx.obs[lhsType];
            var lt = o.Relocate(cx);
            if (lt.defpos != lhsType)
                r += (LhsType, lt.defpos);
            var ls = BList<long>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var so = cx.obs[b.value()];
                var s = (SqlValue)so.Relocate(cx);
                ch = ch || s.defpos != b.value();
                ls += s.defpos;
            }
            if (ch)
                r += (List, ls);
            o = cx.obs[rhs];
            var rh = (SqlValue)o.Relocate(cx);
            if (rh.defpos != rhs)
                r += (Rhs, rh.defpos);
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
		public long ret => (long)(mem[Ret]??-1L);
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(long dp) : base (dp,BTree<long,object>.Empty)
        { }
        protected ReturnStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ReturnStatement operator+(ReturnStatement s,(long,object)x)
        {
            return new ReturnStatement(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ReturnStatement(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ReturnStatement(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (ReturnStatement)base._Relocate(wr);
            var rt = (ReturnStatement)wr.Fixed(ret);
            if (rt.defpos != ret)
                r += (Ret, rt.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ReturnStatement)base._Relocate(cx);
            var nr = (ReturnStatement)cx.Fixed(ret);
            if (nr.defpos != ret)
                r += (Ret, nr.defpos);
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
            var r = (SimpleCaseStatement)base._Relocate(wr);
            var e = BList<long>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                e += s.defpos;
            }
            if (ch)
                r += (Else, e);
            var op = (SqlValue)wr.Fixed(operand);
            if (op.defpos != operand)
                r += (Operand, op.defpos);
            var wh = BList<long>.Empty;
            ch = false;
            for (var b=whens.First();b!=null;b=b.Next())
            {
                var w = (WhenPart)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (Whens, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SimpleCaseStatement)base._Relocate(cx);
            var e = BList<long>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var o = cx.obs[b.value()];
                var s = (Executable)o.Relocate(cx);
                ch = ch || s.defpos != b.value();
                e += s.defpos;
            }
            if (ch)
                r += (Else, e);
            var oo = cx.obs[operand];
            var op = (SqlValue)oo.Relocate(cx);
            if (op.defpos != operand)
                r += (Operand, op.defpos);
            var wh = BList<long>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var o = cx.obs[b.value()];
                var w = (WhenPart)o.Relocate(cx);
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (Whens, wh);
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
                if (((SqlValue)cx.obs[operand]).Matches(cx))
                    return ObeyList(((WhenPart)cx.obs[c.value()]).stms, cx);
            return ObeyList(els, cx);
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
            var r = (SearchedCaseStatement)base._Relocate(wr);
            var e = BList<long>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                e += s.defpos;
            }
            if (ch)
                r += (SimpleCaseStatement.Else, e);
            var wh = BList<long>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var w = (WhenPart)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (SimpleCaseStatement.Whens, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SearchedCaseStatement)base._Relocate(cx);
            var e = BList<long>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)cx.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                e += s.defpos;
            }
            if (ch)
                r += (SimpleCaseStatement.Else, e);
            var wh = BList<long>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var w = (WhenPart)cx.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (SimpleCaseStatement.Whens, wh);
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
                if (((SqlValue)cx.obs[w.cond]).Matches(cx))
                    return ObeyList(w.stms, cx);
            }
			return ObeyList(els,cx);
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
            var r = base._Relocate(wr);
            if (wr.Fixed(cond) is SqlValue c && c.defpos != cond)
                r += (Cond, c.defpos);
            var ss = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var s = (Executable)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                ss += s.defpos;
            }
            if (ch)
                r += (Stms, ss);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            if (cx.Fixed(cond) is SqlValue c && c.defpos != cond)
                r += (Cond, c.defpos);
            var ss = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var s = (Executable)cx.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                ss += s.defpos;
            }
            if (ch)
                r += (Stms, ss);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(stms,defpos,cx) || cx.obs[cond].Calls(defpos, cx);
        }
        public override Context Obey(Context cx)
        {
            var a = cx;
            if (cond == -1L || ((SqlValue)cx.obs[cond]).Matches(cx))
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
            var r = (IfThenElse)base._Relocate(wr);
            var se = (SqlValue)wr.Fixed(search);
            if (se.defpos != search)
                r += (Search, se.defpos);
            var th = BList<long>.Empty;
            var ch = false;
            for (var b=then.First();b!=null;b=b.Next())
            {
                var t = (Executable)wr.Fixed(b.value());
                ch = ch || t.defpos != b.value();
                th += t.defpos;
            }
            if (ch)
                r += (Then,th);
            var el = BList<long>.Empty;
            ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var e = (Executable)wr.Fixed(b.value());
                ch = ch || e.defpos != b.value();
                el += e.defpos;
            }
            if (ch)
                r += (Else, el); 
            var ei = BList<long>.Empty;
            ch = false;
            for (var b = elsif.First(); b != null; b = b.Next())
            {
                var e = (Executable)wr.Fixed(b.value());
                ch = ch || e.defpos != b.value();
                ei += e.defpos;
            }
            if (ch)
                r += (Elsif, ei); 
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (IfThenElse)base._Relocate(cx);
            var se = (SqlValue)cx.Fixed(search);
            if (se.defpos != search)
                r += (Search, se.defpos);
            var th = BList<long>.Empty;
            var ch = false;
            for (var b = then.First(); b != null; b = b.Next())
            {
                var t = (Executable)cx.Fixed(b.value());
                ch = ch || t.defpos != b.value();
                th += t.defpos;
            }
            if (ch)
                r += (Then, th);
            var el = BList<long>.Empty;
            ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var e = (Executable)cx.Fixed(b.value());
                ch = ch || e.defpos != b.value();
                el += e.defpos;
            }
            if (ch)
                r += (Else, el);
            var ei = BList<long>.Empty;
            ch = false;
            for (var b = elsif.First(); b != null; b = b.Next())
            {
                var e = (Executable)cx.Fixed(b.value());
                ch = ch || e.defpos != b.value();
                ei += e.defpos;
            }
            if (ch)
                r += (Elsif, ei);
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
            if (((SqlValue)cx.obs[search]).Matches(cx))
                return ObeyList(then, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (cx.obs[g.value()] is IfThenElse f 
                    && ((SqlValue)cx.obs[f.search]).Matches(cx))
                    return ObeyList(f.then, cx);
            return ObeyList(els, cx);
        }
	}
    internal class XmlNameSpaces : Executable
    {
        internal const long
            Nsps = -120; // BTree<string,string>
        /// <summary>
        /// A list of namespaces to be added
        /// </summary>
        public BTree<string, string> nsps => (BTree<string,string>)mem[Nsps];
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
    }
    /// <summary>
    /// A while statement for a stored proc/func
    /// </summary>
	internal class WhileStatement : Executable
	{
        internal const long
            Loop = -121, // long
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
            var r = (WhileStatement)base._Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (Loop, lp);
            var se = (SqlValue)wr.Fixed(search);
            if (se.defpos != search)
                r += (Search, se);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b=what.First();b!=null;b=b.Next())
            {
                var w = (Executable)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (What, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (WhileStatement)base._Relocate(cx);
            var lp = cx.Unheap(loop);
            if (lp != loop)
                r += (Loop, lp);
            var se = (SqlValue)cx.Fixed(search);
            if (se.defpos != search)
                r += (Search, se);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)cx.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (What, wh);
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
            while (na==cx && a.signal == null && ((SqlValue)cx.obs[search]).Matches(cx))
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
            var r = (RepeatStatement)base._Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = (SqlValue)wr.Fixed(search);
            if (se.defpos != search)
                r += (WhileStatement.Search, se.defpos);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhileStatement.What, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RepeatStatement)base._Relocate(cx);
            var lp = cx.Unheap(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = (SqlValue)cx.Fixed(search);
            if (se.defpos != search)
                r += (WhileStatement.Search, se.defpos);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)cx.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhileStatement.What, wh);
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
                if (na != cx)
                    break;
                act.signal?.Throw(act);
                if (!((SqlValue)cx.obs[search]).Matches(act))
                    break;
            }
            cx = act.SlideDown(); 
            return cx;
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
            var r = (LoopStatement)base._Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (LoopStatement)base._Relocate(cx);
            var lp = cx.Unheap(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)cx.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
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
	}
    /// <summary>
    /// A for statement for a stored proc/func
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        internal const long
            Cursor = -124, // string
            ForVn = -125, // string
            Loop = -126, // long
            Sel = -127, // long CursorSpecification
            Stms = -128; // BList<long> Executable
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public long sel => (long)(mem[Sel]??-1L);
        /// <summary>
        /// The name of the cursor
        /// </summary>
		public string cursor => (string)mem[Cursor];
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
            var r = (ForSelectStatement)base._Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = (Query)wr.Fixed(sel);
            if (se.defpos != sel)
                r += (Cursor, se.defpos);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)wr.Fixed(b.value());
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ForSelectStatement)base._Relocate(cx);
            var lp = cx.Unheap(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = (Query)cx.obs[sel].Relocate(cx);
            if (se.defpos != sel)
                r += (Cursor, se.defpos);
            var wh = BList<long>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)cx.obs[b.value()].Relocate(cx);
                ch = ch || w.defpos != b.value();
                wh += w.defpos;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
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
            var cs = (CursorSpecification)cx.obs[sel];
            var qe = (QueryExpression)cx.obs[cs.union];
            var qs = (QuerySpecification)cx.obs[qe.left];
            var ac = new Activation(cx,label);
            for (var rb = da.First(ac); rb != null; rb = rb.Next(ac))
            {
                ac.Add(qs);
                ac.brk = cx as Activation;
                ac.cont = ac;
                ac = (Activation)ObeyList(stms, ac);
                if (ac.signal != null)
                    ac.signal.Throw(cx);
            }
            return ac.SlideDown();
        }
	}
    /// <summary>
    /// An Open statement for a cursor
    /// </summary>
	internal class OpenStatement : Executable
	{
        SqlCursor cursor => (SqlCursor)mem[FetchStatement.Cursor];
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
            var r = (OpenStatement)base._Relocate(wr);
            var c = cursor.Relocate(wr);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (OpenStatement)base._Relocate(cx);
            var c = cursor.Relocate(cx);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cursor.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            ((Query)cx.obs[cursor.spec]).RowSets(cx,BTree<long, RowSet.Finder>.Empty);
            return cx;
        }
    }
    /// <summary>
    /// A Close statement for a cursor
    /// </summary>
	internal class CloseStatement : Executable
	{
        public SqlCursor cursor => (SqlCursor)mem[FetchStatement.Cursor];
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
            var r = (CloseStatement)base._Relocate(wr);
            var c = cursor.Relocate(wr);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CloseStatement)base._Relocate(cx);
            var c = cursor.Relocate(cx);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cursor.Calls(defpos, cx);
        }
        /// <summary>
        /// Execute the close statement
        /// </summary>
        public override Context Obey(Context cx)
        {
            cx.data += (defpos,EmptyRowSet.Value);
            return cx;
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
        : base(dp, BTree<long, object>.Empty + (Cursor, n.defpos) + (How, h) + (Where, w.defpos))
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
            var r = (FetchStatement)base._Relocate(wr);
            var c = (SqlCursor)wr.Fixed(cursor);
            if (c.defpos != cursor)
                r += (Cursor, c.defpos);
            var os = BList<long>.Empty;
            var ch = false;
            for (var b=outs.First();b!=null;b=b.Next())
            {
                var ou = (SqlValue)wr.Fixed(b.value());
                ch = ch || ou.defpos != b.value();
                os += ou.defpos;
            }
            if (ch)
                r += (Outs, os);
            var w = (SqlValue)wr.Fixed(where);
            if (w.defpos != where)
                r += (Where, w.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (FetchStatement)base._Relocate(cx);
            var c = (SqlCursor)cx.Fixed(cursor);
            if (c.defpos != cursor)
                r += (Cursor, c.defpos);
            var os = BList<long>.Empty;
            var ch = false;
            for (var b = outs.First(); b != null; b = b.Next())
            {
                var ou = (SqlValue)cx.Fixed(b.value());
                ch = ch || ou.defpos != b.value();
                os += ou.defpos;
            }
            if (ch)
                r += (Outs, os);
            var w = (SqlValue)cx.Fixed(where);
            if (w.defpos != where)
                r += (Where, w.defpos);
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
 	}
    /// <summary>
    /// A call statement for a stored proc/func
    /// </summary>
	internal class CallStatement : Executable
	{
        internal const long
            Parms = -133, // BList<long> SqlValue
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
		public BList<long> parms =>
            (BList<long>)mem[Parms]?? BList<long>.Empty;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long dp, Procedure pr, string pn, BList<long> acts, SqlValue tg=null)
         : this(dp, pr, pn, acts, (tg==null)?null: new BTree<long, object>(Var,tg))
        { }
        protected CallStatement(long dp, Procedure pr, string pn, BList<long> acts, BTree<long,object> m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Parms, acts) + (ProcDefPos, pr?.defpos??-1L)
               +(_Domain,pr?.domain??Domain.Content) + (Name,pn))
        { }
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
            var r = (CallStatement)base._Relocate(wr);
            var pp = wr.Fixed(procdefpos);
            if (pp.defpos != procdefpos)
                r += (ProcDefPos, pp.defpos);
            var ps = BList<long>.Empty;
            var ch = false;
            for (var b=parms.First();b!=null;b=b.Next())
            {
                var p = (SqlValue)wr.Fixed(b.value());
                ch = ch||p.defpos != b.value();
                ps += p.defpos;
            }
            if (ch)
                r += (Parms, ps);
            var vr = (SqlValue)wr.Fixed(var);
            if (vr.defpos != var)
                r += (Var, vr.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (CallStatement)base._Relocate(cx);
            var pp = cx.Fixed(procdefpos);
            if (pp.defpos != procdefpos)
                r += (ProcDefPos, pp.defpos);
            var ps = BList<long>.Empty;
            var ch = false;
            for (var b = parms.First(); b != null; b = b.Next())
            {
                var p = (SqlValue)cx.Fixed(b.value());
                ch = ch || p.defpos != b.value();
                ps += p.defpos;
            }
            if (ch)
                r += (Parms, ps);
            var vr = (SqlValue)cx.Fixed(var);
            if (vr.defpos != var)
                r += (Var, vr.defpos);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (var != -1L && cx.obs[var].Calls(defpos, cx))
                return true;
            return procdefpos==defpos || Calls(parms,defpos, cx);
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
            cx.done += (defpos, r);
            return r;
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
            SetList = -139, // BTree<Sqlx,long>
            SType = -140; // Sqlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype => (Sqlx)(mem[SType] ?? Sqlx.NO); // RAISE or RESIGNAL
        internal string signal => (string)mem[_Signal];
        internal BList<object> objects => (BList<object>)mem[Objects];
        internal Exception exception => (Exception)mem[Exception];
        internal BTree<Sqlx, long> setlist =>
            (BTree<Sqlx, long>)mem[SetList] ?? BTree<Sqlx, long>.Empty;
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
        protected Signal(long dp, BTree<long, object> m) :base(dp, m) {}
        static BTree<long,object> _Mem(object[] obs)
        {
            var r = BList<object>.Empty;
            foreach (var o in obs)
                r += o;
            return new BTree<long,object>(Objects,r);
        }
        public static Signal operator+(Signal s,(long,object)x)
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
            var r = (Signal)base._Relocate(wr);
            var sl = BTree<Sqlx, long>.Empty;
            var ch = false;
            for(var b=setlist.First();b!=null;b=b.Next())
            {
                var s = (SqlValue)wr.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                sl += (b.key(),s.defpos);
            }
            if (ch)
                r += (SetList, sl);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Signal)base._Relocate(cx);
            var sl = BTree<Sqlx, long>.Empty;
            var ch = false;
            for (var b = setlist.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)cx.Fixed(b.value());
                ch = ch || s.defpos != b.value();
                sl += (b.key(), s.defpos);
            }
            if (ch)
                r += (SetList, sl);
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
            if (stype==Sqlx.RESIGNAL && !cx.tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                    throw new DBException("0K000").ISO();
            string sclass = signal.Substring(0, 2);
            var dia = cx.tr.diagnostics;
            dia +=(Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    dia+=(b.key(), b.value());
                dia+=(Sqlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
            }
            for (var s = setlist.First();s!= null;s=s.Next())
                dia+=(s.key(), cx.obs[s.value()].Eval(cx));
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
                for (var x = setlist.First();x!= null;x=x.Next())
                    e.info +=(x.key(), cx.obs[x.value()].Eval(cx));
            }
            throw e;
        }
	}
    /// <summary>
    /// A GetDiagnostics statement for a routine
    /// </summary>
    internal class GetDiagnostics : Executable
    {
        internal const long
            List = -141; // BTree<long,Sqlx>
        internal BTree<long,Sqlx> list => 
            (BTree<long,Sqlx>)mem[List]??BTree<long, Sqlx>.Empty;
        internal GetDiagnostics(long dp) : base(dp,BTree<long,object>.Empty) { }
        protected GetDiagnostics(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GetDiagnostics operator+(GetDiagnostics s,(long,object)x)
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
            var r = base._Relocate(wr);
            var sl = BTree<long, Sqlx>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)wr.cx.obs[b.key()].Relocate(wr);
                ch = ch || s.defpos != b.key();
                sl += (s.defpos,b.value());
            }
            if (ch)
                r += (List, sl);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            var sl = BTree<long, Sqlx>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)cx.obs[b.key()].Relocate(cx);
                ch = ch || s.defpos != b.key();
                sl += (s.defpos, b.value());
            }
            if (ch)
                r += (List, sl);
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
		public long sel => (long)(mem[ForSelectStatement.Sel]??-1L);
        /// <summary>
        /// The output list
        /// </summary>
		public BList<long> outs => (BList<long>)mem[Outs] ?? BList<long>.Empty;
        /// <summary>
        /// Constructor: a select statement: single row from the parser
        /// </summary>
        /// <param name="s">The select statement</param>
        /// <param name="sv">The list of variables to receive the values</param>
		public SelectSingle(long dp) : base(dp,BTree<long,object>.Empty)
		{ }
        protected SelectSingle(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectSingle operator+(SelectSingle s,(long,object)x)
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
            var r = (SelectSingle)base._Relocate(wr);
            var se = (Query)wr.Fixed(sel);
            if (se.defpos != sel)
                r += (ForSelectStatement.Sel, se.defpos);
            var os = BList<long>.Empty;
            var ch = false;
            for (var b=outs.First();b!=null;b=b.Next())
            {
                var ou = (SqlValue)wr.Fixed(b.value());
                ch = ch || ou.defpos != b.value();
                os += ou.defpos;
            }
            if (ch)
                r += (Outs, os);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SelectSingle)base._Relocate(cx);
            var se = (Query)cx.Fixed(sel);
            if (se.defpos != sel)
                r += (ForSelectStatement.Sel, se.defpos);
            var os = BList<long>.Empty;
            var ch = false;
            for (var b = outs.First(); b != null; b = b.Next())
            {
                var ou = (SqlValue)cx.Fixed(b.value());
                ch = ch || ou.defpos != b.value();
                os += ou.defpos;
            }
            if (ch)
                r += (Outs, os);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return Calls(outs,defpos, cx);
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
            a.AddValue(this,rb);
            if (rb != null)
                for (var b = outs.First(); b != null; b = b.Next())
                    a.AddValue((SqlValue)cx.obs[b.value()], rb[b.key()]);
            else
                a.NoData();
            return cx;
        }
	}
    /// <summary>
    /// An executable for an HTTP REST request from the parser
    /// </summary>
    internal class HttpREST : Executable
    {
        internal const long
            CredPw = -143, //long SqlValue
            CredUs = -144, // long SqlValue
            Mime = -145, // string
            Posted = -146, // long SqlValue
            Url = -147, // long SqlValue
            Verb = -148, // string
            Where = -149; //long SqlValue
        /// <summary>
        /// The url provided
        /// </summary>
        internal long url => (long)(mem[Url]??-1L);
        /// <summary>
        /// The posted data provided
        /// </summary>
        internal long data => (long)(mem[Posted]??-1L);
        /// <summary>
        /// The requested mime data type
        /// </summary>
        internal string mime => (string)mem[Mime]??"";
        /// <summary>
        /// The credentials if supplied (null means use default credentials)
        /// </summary>
        internal long us => (long)(mem[CredUs]??-1L);
        internal long pw => (long)(mem[CredPw]??-1L);
        internal long wh => (long)(mem[Where]??-1L);
        internal string verb => (string)mem[Verb];
        /// <summary>
        /// Constructor: a HTTP REST request
        /// </summary>
        public HttpREST(long dp, string v, SqlValue u, SqlValue p)
        : base(dp, BTree<long, object>.Empty + (Verb, v) + (CredUs, u.defpos) + (CredPw, p.defpos))
        { }
        protected HttpREST(long dp, BTree<long, object> m) : base(dp, m) { }
        public static HttpREST operator+(HttpREST s,(long,object)x)
        {
            return new HttpREST(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new HttpREST(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new HttpREST(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (HttpREST)base._Relocate(wr);
            if (wr.Fixed(pw) is SqlValue cp && cp.defpos != pw)
                    r += (CredPw, cp.defpos);
            if (wr.Fixed(us) is SqlValue cu && cu.defpos != us)
                    r += (CredUs, cu.defpos);
            if (wr.Fixed(data) is SqlValue cd && cd.defpos != data)
                    r += (Posted, cd.defpos);
            if (wr.Fixed(wh) is SqlValue cw && cw.defpos != wh)
                    r += (Where, cw.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (HttpREST)base._Relocate(cx);
            if (cx.Fixed(pw) is SqlValue cp && cp.defpos != pw)
                    r += (CredPw, cp.defpos);
            if (cx.Fixed(us) is SqlValue cu && cu.defpos != us)
                    r += (CredUs, cu.defpos);
            if (cx.Fixed(data) is SqlValue cd && cd.defpos != data)
                    r += (Posted, cd.defpos);
            if (cx.Fixed(wh) is SqlValue cw && cw.defpos != wh)
                    r += (Where, cw.defpos);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[data]?.Calls(defpos, cx) == true || 
                cx.obs[url]?.Calls(defpos, cx) == true
                || cx.obs[wh]?.Calls(defpos, cx) == true;
        }
        /// <summary>
        /// Obey the HTTP request
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var s = cx.obs[url].Eval(cx)?.ToString();
            if (s == null)
                return cx;
            // Okay, use HTTP
            var rq = SqlHttp.GetRequest(cx, cx.obs[url].Eval(cx).ToString());
            rq.UserAgent = "Pyrrho 7.0 http://" + PyrrhoStart.host + "/" 
                + cx.tr.startTime + "/" + cx.db.loadpos;
            rq.ContentType = mime ?? "application/tcc+json, application/json";
            rq.Method = verb;
            if (verb!="DELETE")
            {
                var rst = new System.IO.StreamWriter(rq.GetRequestStream());
                rst.WriteLine(cx.data.ToString());
                rst.Close();
            }
            var rr = new System.IO.StreamReader(rq.GetResponse().GetResponseStream());
            cx.val = new TChar(rr.ReadToEnd());
            return cx;
        }
    }
}