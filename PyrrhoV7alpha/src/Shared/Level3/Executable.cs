using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

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
		protected Context ObeyList(BList<Executable> e,Context cx)
		{
            if (e == null)
                throw new DBException("42173", (cx as Activation)?.label??"");
            Context nx = cx;
            Activation a = (Activation)cx;
            for (var b = e.First();b!=null && nx==cx 
                && ((Activation)cx).signal==null;b=b.Next())
            {
                try
                {
                    var x = b.value();
                    nx = x.Obey(cx);
                    if (a==nx && a.signal != null)
                        a.signal.Throw(a);
                    cx.SlideDown(nx);
                }
                catch (DBException ex)
                {
                    a.signal = new Signal(cx.cxid,ex);
                }
            }
            return cx;
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
            return new Executable(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            return ((DBObject)base.Relocate(wr)).Relocate(wr.Fix(defpos));
        }
        internal static bool Calls(BList<Executable> ss,long defpos,Database d)
        {
            for (var b = ss?.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, d))
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
             CS = -95; //CursorSpecification
        /// <summary>
        /// The cusorspecification (a Query) for this executable
        /// </summary>
        public CursorSpecification cs=>(CursorSpecification)mem[CS];
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The cursor specification</param>
        public SelectStatement(long dp, CursorSpecification c)
            : base(dp, new BTree<long, object>(CS, c) + (Dependents, c.dependents))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SelectStatement)base.Relocate(wr);
            var c = cs.Relocate(wr);
            if (c != cs)
                r += (CS, c);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SelectStatement)base.Frame(cx);
            var c = cs.Frame(cx);
            if (c != cs)
                r += (CS, c);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return cs.Calls(defpos, db);
        }
        /// <summary>
        /// Obey the executable
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cs.RowSets(cx);
            return cx;
        }

    }
    /// <summary>
    /// A Compound Statement for the SQL procedure language
    /// </summary>
    internal class CompoundStatement : Executable
    {
        internal const long
             Stms = -96; // BList<Executable>
        /// <summary>
        /// The contained list of Executables
        /// </summary>
		public BList<Executable> stms =>
            (BList<Executable>)mem[Stms] ?? BList<Executable>.Empty;
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
        internal override DBObject Frame(Context cx)
        {
            var r = (CompoundStatement)base.Frame(cx);
            var ss = BList<Executable>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var s = (Executable)b.value().Frame(cx);
                ch = ch || s != b.value();
                ss += s;
            }
            if (ch)
                r += (Stms, ss);
            return cx.Add(r,true);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (CompoundStatement)base.Relocate(wr);
            var ss = BList<Executable>.Empty;
            var ch = false;
            for (var b=stms.First();b!=null;b=b.Next())
            {
                var s = (Executable)b.value().Relocate(wr);
                ch = ch || s != b.value();
                ss += s;
            }
            if (ch)
                r += (Stms, ss);
            return r;
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(stms, defpos, db);
        }
        /// <summary>
        /// Obey a Compound Statement.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context Obey(Context cx)
        {
            cx.exec = this;
            var act = new Activation(cx, label);
            try
            {
                act = (Activation)ObeyList(stms, act);
                if (act.signal != null)
                    act.signal.Throw(cx);
            }
            catch (Exception e) { throw e; }
            cx.SlideDown(act);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(stms);
            return sb.ToString();
        }
    }
    internal class PreparedStatement : Executable
    {
        internal readonly BList<SqlValue> qMarks;
        internal readonly Executable target;
        public PreparedStatement(Executable e,BList<SqlValue> q) :base(e.defpos-1,BTree<long,object>.Empty)
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
            Init = -97; // SqlValue
        /// <summary>
        /// The declared data type for the variable
        /// </summary>
        public Domain dataType => (Domain)mem[_Domain] ?? Domain.Null;
        /// <summary>
        /// Default initialiser
        /// </summary>
        public SqlValue init => (SqlValue)mem[Init];
        public SqlValue vbl => (SqlValue)mem[AssignmentStatement.Vbl];
        public ObInfo info => (ObInfo)mem[SqlValue.Info] ?? ObInfo.Any;
        /// <summary>
        /// Constructor: a new local variable
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// <param name="n">The name of the variable</param>
        /// <param name="dt">The data type</param>
        public LocalVariableDec(long dp, string n, Domain dt, SqlValue v=null)
            : base(dp,BTree<long,object>.Empty+(Label,n)+(_Domain,dt)
                  +(AssignmentStatement.Vbl,v??(new SqlValue(dp,n,dt))))
        { }
        public LocalVariableDec(long dp, string n, Domain dt, ObInfo oi, 
            SqlValue v = null,BTree<long,object>m=null)
    : base(dp, (m??BTree<long, object>.Empty) + (Label, n) + (_Domain, dt) + (SqlValue.Info,oi) 
          + (AssignmentStatement.Vbl, (v??new SqlValue(dp, n, dt))))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (LocalVariableDec)base.Relocate(wr);
            var d = dataType?.Relocate(wr);
            if (d != dataType)
                r += (_Domain, d);
            var oi = info?.Relocate(wr);
            if (oi != info)
                r += (SqlValue.Info, d);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (LocalVariableDec)base.Frame(cx);
            var d = dataType?.Frame(cx);
            if (d != dataType)
                r += (_Domain, d);
            var oi = info?.Frame(cx);
            if (oi != info)
                r += (SqlValue.Info, d);
            return cx.Add(r,true);
        }
        /// <summary>
        /// Execute the local variable declaration, by adding the local variable to the activation (overwrites any previous)
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            TypedValue tv = init?.Eval(cx)??dataType.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.AddValue(vbl, tv); // We expect a==cx, but if not, tv will be copied to a later
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
    /// A procedure formal parameter is a dynamically initialised local variable
    /// </summary>
	internal class ProcParameter : SqlValue
    {
        internal const long
            ParamMode = -98, // Sqlx
            Result = -99; // Sqlx
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
		public ProcParameter(long dp, Sqlx m, string n, ObInfo dt) 
            : base(dp, n, dt.domain, new BTree<long, object>(ParamMode, m)+(Info,dt))
        { }
        protected ProcParameter(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ProcParameter operator +(ProcParameter s, (long, object) x)
        {
            return new ProcParameter(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ProcParameter(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ProcParameter(dp, mem);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
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
    /// </summary>
    internal class CursorDeclaration : LocalVariableDec
    {
        internal const long
            CS = FetchStatement.Cursor; // SqlCursor
        /// <summary>
        /// The specification for the cursor
        /// </summary>
        public SqlCursor cs => (SqlCursor)mem[CS];
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="i">The name</param>
        /// <param name="c">The cursor specification</param>
        public CursorDeclaration(long dp,string n,CursorSpecification c) 
            : this(dp,n,c.rowType,new SqlCursor(c.defpos,c,n)) 
        { }
        CursorDeclaration(long dp,string n,Selection rt,SqlCursor c)
            : base(dp,n,Domain.Row,rt.info,c,new BTree<long,object>(CS,c))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var c = cs.Relocate(wr);
            if (c != cs)
                r += (CS, c);
            return r;
        }
        internal override bool Calls(long defpos, Database db)
        {
            return cs.Calls(defpos, db);
        }
        /// <summary>
        /// Instantiate the cursor
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cx.AddValue(cs, cs.Eval(cx));
            return cx;
        }
    }
    /// <summary>
    /// An Exception handler for a stored procedure
    /// </summary>
	internal class HandlerStatement : Executable
	{
        internal const long
            Action = -100, // Executable
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
        public Executable action => (Executable)mem[Action];
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (HandlerStatement)base.Relocate(wr);
            var a = action.Relocate(wr);
            if (a != action)
                r += (Action, a);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (HandlerStatement)base.Frame(cx);
            var a = action.Frame(cx);
            if (a != action)
                r += (Action, a);
            return cx.Add(r,true);
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
        internal override bool Calls(long defpos, Database db)
        {
            return action.Calls(defpos, db);
        }
    }
    /// <summary>
    /// A Handler helps implementation of exception handling
    /// </summary>
	internal class Handler : Executable
	{
        internal const long
            HDefiner = -103, // Activation
            Hdlr = -104; //HandlerStatement
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
            : base(ad.nextHeap++, BTree<long, object>.Empty
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (Handler)base.Relocate(wr);
            var h = hdlr.Relocate(wr);
            if (h != hdlr)
                r += (Hdlr, h);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (Handler)base.Frame(cx);
            var h = hdlr.Frame(cx);
            if (h != hdlr)
                r += (Hdlr, h);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return hdlr.Calls(defpos, db);
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
                cx = hdlr.action.Obey(cx);
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
            return cx.Ctx(defpos);
        }
	}
    /// <summary>
    /// An assignment statement for a stored procedure
    /// </summary>
	internal class AssignmentStatement : Executable
    {
        internal const long
            Val = -105, //SqlValue
            Vbl = -106; // SqlValue
        /// <summary>
        /// The left hand side of the assignment, checked for assignability
        /// </summary>
        public SqlValue vbl => (SqlValue)mem[Vbl];
        /// <summary>
        /// The right hand side of the assignment
        /// </summary>
		public SqlValue val => (SqlValue)mem[Val];
        /// <summary>
        /// Constructor: An assignment statement from the parser
        /// </summary>
		public AssignmentStatement(long dp,SqlValue vb,SqlValue va) 
            : base(dp,BTree<long, object>.Empty+(Vbl,vb)+(Val,va)
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (AssignmentStatement)base.Relocate(wr);
            var va = val.Relocate(wr);
            if (va != val)
                r += (Val, va);
            var vb = vbl.Relocate(wr);
            if (vb != vbl)
                r += (Vbl, vb);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (AssignmentStatement)base.Frame(cx);
            var va = val.Frame(cx);
            if (va != val)
                r += (Val, va);
            var vb = vbl.Frame(cx);
            if (vb != vbl)
                r += (Vbl, vb);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return vbl.Calls(defpos, db) || val.Calls(defpos,db);
        }
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var t = vbl.domain;
            if (val != null)
            {
                var v = t.Coerce(val.Eval(cx)?.NotNull());
                cx.AddValue(vbl, v);
            }
            return cx;
        }
        public override string ToString()
        {
            if (vbl!=null && val!=null)
                return vbl.ToString() + "=" + val.ToString();
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
            LhsType = -107, // DBObject
            List = -108, // BList<long>
            Rhs = -109; // SqlValue
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal BList<long> list => (BList<long>)mem[List];
        /// <summary>
        /// The row type of the lefthand side, used to coerce the given value 
        /// </summary>
        DBObject lhsType => (DBObject)mem[LhsType]??Domain.Content;
        /// <summary>
        /// The row-valued right hand side
        /// </summary>
        public SqlValue rhs => (SqlValue)mem[Rhs];
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (MultipleAssignment)base.Relocate(wr);
            var lt = lhsType.Relocate(wr);
            if (lt != lhsType)
                r += (LhsType, lt);
            var ls = BList<long>.Empty;
            var ch = false;
            for(var b=list.First();b!=null;b=b.Next())
            {
                var s = wr.Fix(b.value());
                ch = ch || s != b.value();
                ls += s;
            }
            if (ch)
                r +=(List, ls);
            var rh = rhs.Relocate(wr);
            if (rh != rhs)
                r += (Rhs, rh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (MultipleAssignment)base.Frame(cx);
            var lt = lhsType.Frame(cx);
            if (lt != lhsType)
                r += (LhsType, lt);
            var ls = BList<long>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var s = cx.Fix(b.value());
                ch = ch || s != b.value();
                ls += s;
            }
            if (ch)
                r += (List, ls);
            var rh = rhs.Frame(cx);
            if (rh != rhs)
                r += (Rhs, rh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return rhs.Calls(defpos, db);
        }
        /// <summary>
        /// Execute the multiple assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            TRow r = (TRow)rhs.Eval(cx);
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
            Ret = -110; // SqlValue
        /// <summary>
        /// The return value
        /// </summary>
		public SqlValue ret => (SqlValue)mem[Ret];
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(long dp) : base (dp,BTree<long,object>.Empty)
        {  }
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (ReturnStatement)base.Relocate(wr);
            var rt = ret.Relocate(wr);
            if (rt != ret)
                r += (Ret, rt);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (ReturnStatement)base.Frame(cx);
            var rt = ret.Frame(cx);
            if (rt != ret)
                r += (Ret, rt);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return ret.Calls(defpos, db);
        }
        /// <summary>
        /// Execute the return statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
			a.val = ret.Eval(cx);
            cx.SlideDown(a);
            return cx;
		}
	}
    /// <summary>
    /// A Case statement for a stored procedure
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        internal const long
            Else = -111, // BList<Executable> 
            Operand = -112, // SqlValue
            Whens = -113; // BList<WhenPart>
        /// <summary>
        /// The test expression
        /// </summary>
        public SqlValue operand => (SqlValue)mem[Operand];
        /// <summary>
        /// A list of when parts
        /// </summary>
        public BList<WhenPart> whens =>
            (BList<WhenPart>)mem[Whens]?? BList<WhenPart>.Empty;
        /// <summary>
        /// An else part
        /// </summary>
        public BList<Executable> els =>
            (BList<Executable>)mem[Else]?? BList<Executable>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SimpleCaseStatement)base.Relocate(wr);
            var e = BList<Executable>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)b.value().Relocate(wr);
                ch = ch || s != b.value();
                e += s;
            }
            if (ch)
                r += (Else, e);
            var op = operand.Relocate(wr);
            if (op != operand)
                r += (Operand, op);
            var wh = BList<WhenPart>.Empty;
            ch = false;
            for (var b=whens.First();b!=null;b=b.Next())
            {
                var w = (WhenPart)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (Whens, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SimpleCaseStatement)base.Frame(cx);
            var e = BList<Executable>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)b.value().Frame(cx);
                ch = ch || s != b.value();
                e += s;
            }
            if (ch)
                r += (Else, e);
            var op = operand.Frame(cx);
            if (op != operand)
                r += (Operand, op);
            var wh = BList<WhenPart>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var w = (WhenPart)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (Whens, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return Calls(els,defpos,db) || operand.Calls(defpos, db);
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
                if (operand.Matches(cx))
                    return ObeyList(c.value().stms, cx);
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
		public BList<WhenPart> whens => (BList<WhenPart>)mem[SimpleCaseStatement.Whens];
        /// <summary>
        /// An else part
        /// </summary>
		public BList<Executable> els => 
            (BList<Executable>)mem[SimpleCaseStatement.Else]?? BList<Executable>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SearchedCaseStatement)base.Relocate(wr);
            var e = BList<Executable>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)b.value().Relocate(wr);
                ch = ch || s != b.value();
                e += s;
            }
            if (ch)
                r += (SimpleCaseStatement.Else, e);
            var wh = BList<WhenPart>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var w = (WhenPart)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (SimpleCaseStatement.Whens, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SearchedCaseStatement)base.Frame(cx);
            var e = BList<Executable>.Empty;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var s = (Executable)b.value().Frame(cx);
                ch = ch || s != b.value();
                e += s;
            }
            if (ch)
                r += (SimpleCaseStatement.Else, e);
            var wh = BList<WhenPart>.Empty;
            ch = false;
            for (var b = whens.First(); b != null; b = b.Next())
            {
                var w = (WhenPart)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (SimpleCaseStatement.Whens, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            for (var b = whens.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return Calls(els, defpos, db);
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
			for(var c = whens.First();c!=null;c=c.Next())
				if (c.value().cond.Matches(cx))
					return ObeyList(c.value().stms,cx);
			return ObeyList(els,cx);
		}
	}
    /// <summary>
    /// A when part for a searched case statement or trigger action
    /// </summary>
	internal class WhenPart :Executable
	{
        internal const long
            Cond = -114, // SqlValue
            Stms = -115; // BList<Executable>
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public SqlValue cond => (SqlValue)mem[Cond];
        /// <summary>
        /// a list of statements
        /// </summary>
		public BList<Executable> stms =>
            (BList<Executable>)mem[Stms]?? BList<Executable>.Empty;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A list of statements for this when</param>
        public WhenPart(long dp,SqlValue v, BList<Executable> s) 
            : base(dp, BTree<long,object>.Empty+(Cond,v)+(Stms,s))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var c = cond?.Relocate(wr);
            if (c != cond)
                r += (Cond, c);
            var ss = BList<Executable>.Empty;
            var ch = false;
            for (var b=stms.First();b!=null;b=b.Next())
            {
                var s = (Executable)b.value().Relocate(wr);
                ch = ch || s != b.value();
                ss += s;
            }
            if (ch)
                r += (Stms, ss);
            return r;
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(stms,defpos,db) || cond.Calls(defpos, db);
        }
        public override Context Obey(Context cx)
        {
            var a = cx;
            if (cond == null || cond.Matches(cx))
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
            if (cond!=null)
                sb.Append(" Cond: ");sb.Append(cond);
            sb.Append(" Stms: ");sb.Append(stms);
            return sb.ToString();
        }
    }
    /// <summary>
    /// An if-then-else statement for a stored proc/func
    /// </summary>
	internal class IfThenElse : Executable
	{
        internal const long
            Else = -116, // BList<Executable>
            Elsif = -117, // BList<Executable>
            Search = -118, // SqlValue
            Then = -119; // BList<Executable>
        /// <summary>
        /// The test condition
        /// </summary>
		public SqlValue search => (SqlValue)mem[Search];
        /// <summary>
        /// The then statements
        /// </summary>
		public BList<Executable> then =>
            (BList<Executable>)mem[Then]?? BList<Executable>.Empty;
        /// <summary>
        /// The elsif parts
        /// </summary>
		public BList<Executable> elsif =>
            (BList<Executable>)mem[Elsif] ?? BList<Executable>.Empty;
        /// <summary>
        /// The else part
        /// </summary>
		public BList<Executable> els =>
            (BList<Executable>)mem[Else] ?? BList<Executable>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (IfThenElse)base.Relocate(wr);
            var se = search.Relocate(wr);
            if (se != search)
                r += (Search, se);
            var th = BList<Executable>.Empty;
            var ch = false;
            for (var b=then.First();b!=null;b=b.Next())
            {
                var t = (Executable)b.value().Relocate(wr);
                ch = ch || t != b.value();
                th += t;
            }
            if (ch)
                r += (Then,th);
            var el = BList<Executable>.Empty;
            ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var e = (Executable)b.value().Relocate(wr);
                ch = ch || e != b.value();
                el += e;
            }
            if (ch)
                r += (Else, el); 
            var ei = BList<Executable>.Empty;
            ch = false;
            for (var b = elsif.First(); b != null; b = b.Next())
            {
                var e = (Executable)b.value().Relocate(wr);
                ch = ch || e != b.value();
                ei += e;
            }
            if (ch)
                r += (Elsif, ei); 
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (IfThenElse)base.Frame(cx);
            var se = search.Frame(cx);
            if (se != search)
                r += (Search, se);
            var th = BList<Executable>.Empty;
            var ch = false;
            for (var b = then.First(); b != null; b = b.Next())
            {
                var t = (Executable)b.value().Frame(cx);
                ch = ch || t != b.value();
                th += t;
            }
            if (ch)
                r += (Then, th);
            var el = BList<Executable>.Empty;
            ch = false;
            for (var b = els.First(); b != null; b = b.Next())
            {
                var e = (Executable)b.value().Frame(cx);
                ch = ch || e != b.value();
                el += e;
            }
            if (ch)
                r += (Else, el);
            var ei = BList<Executable>.Empty;
            ch = false;
            for (var b = elsif.First(); b != null; b = b.Next())
            {
                var e = (Executable)b.value().Frame(cx);
                ch = ch || e != b.value();
                ei += e;
            }
            if (ch)
                r += (Elsif, ei);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(then,defpos, db)||Calls(elsif,defpos,db)||Calls(els,defpos,db);
        }
        /// <summary>
        /// Obey an if-then-else statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx; 
            a.exec = this;
            if (search.Matches(cx))
                return ObeyList(then, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (g.value() is IfThenElse f && f.search.Matches(cx))
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
            Search = -122, // SqlValue
            What = -123; // BList<Executable>
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public SqlValue search => (SqlValue)mem[Search];
        /// <summary>
        /// The statements to execute
        /// </summary>
		public BList<Executable> what => 
            (BList<Executable>)mem[What]?? BList<Executable>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (WhileStatement)base.Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (Loop, lp);
            var se = search.Relocate(wr);
            if (se != search)
                r += (Search, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b=what.First();b!=null;b=b.Next())
            {
                var w = (Executable)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (What, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (WhileStatement)base.Frame(cx);
            var se = search.Frame(cx);
            if (se != search)
                r += (Search, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (What, wh);
            return cx.Add(r,true);
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
            while (na==cx && a.signal == null && search.Matches(cx))
            {
                var lp = new Activation(cx, label);
                lp.cont = a;
                lp.brk = a;
                na = ObeyList(what, lp);
                if (na == lp)
                    na = cx;
                a.SlideDown(lp);
                a.signal = lp.signal;
            }
            return a;
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(what,defpos,db) || search.Calls(defpos, db);
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
		public SqlValue search => (SqlValue)mem[WhileStatement.Search];
        /// <summary>
        /// The list of statements to execute at least once
        /// </summary>
		public BList<Executable> what => (BList<Executable>)mem[WhileStatement.What];
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (RepeatStatement)base.Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = search.Relocate(wr);
            if (se != search)
                r += (WhileStatement.Search, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhileStatement.What, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (RepeatStatement)base.Frame(cx);
            var lp = cx.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = search.Frame(cx);
            if (se != search)
                r += (WhileStatement.Search, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = what.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhileStatement.What, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(what, defpos, db) || search.Calls(defpos, db);
        }
        /// <summary>
        /// Execute the repeat statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var act = new Activation(cx, label);
            Context na = act;
            for (; ;)
            {
                na = ObeyList(what, act);
                if (na != cx)
                    break;
                act.signal?.Throw(act);
                if (!search.Matches(act))
                    break;
            }
            cx.SlideDown(act); 
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
		public BList<Executable> stms => (BList<Executable>)mem[WhenPart.Stms];
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (LoopStatement)base.Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (LoopStatement)base.Frame(cx);
            var lp = cx.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(stms,defpos, db);
        }
        /// <summary>
        /// Execute the loop statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx; // from the top of the stack each time
            a.exec = this;
            var act = new Activation(cx, label);
            var lp = new Activation(act, null);
            var na = lp;
            while(na==lp)
            {
                lp.brk = a;
                lp.cont = act;
                na = (Activation)ObeyList(stms, lp);
                if (na==lp)
                    lp.signal?.Throw(a);
            }
            if (na == act)
            {
                act.signal?.Throw(a);
                act.SlideDown(lp);
            }
            cx.SlideDown(act);
            return cx;
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
            Sel = -127, // CursorSpecification
            Stms = -128; // BList<Executable>
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public CursorSpecification sel => (CursorSpecification)mem[Sel];
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
		public BList<Executable> stms => (BList<Executable>)mem[Stms];
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (ForSelectStatement)base.Relocate(wr);
            var lp = wr.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = sel.Relocate(wr);
            if (se != sel)
                r += (Cursor, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Relocate(wr);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (ForSelectStatement)base.Frame(cx);
            var lp = cx.Fix(loop);
            if (lp != loop)
                r += (WhileStatement.Loop, lp);
            var se = sel.Frame(cx);
            if (se != sel)
                r += (Cursor, se);
            var wh = BList<Executable>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
            {
                var w = (Executable)b.value().Frame(cx);
                ch = ch || w != b.value();
                wh += w;
            }
            if (ch)
                r += (WhenPart.Stms, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return sel.Calls(defpos, db) || Calls(stms,defpos,db);
        }
        /// <summary>
        /// Execute a FOR statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            var da = sel.RowSets(cx);
            if (da == null)
                return cx;
            var qs = sel.union.left;
            var ac = new Activation(cx, label);
            ac.Add(qs);
            var dt = sel.rowType;
            for (var rb = da.First(ac); rb != null; rb = rb.Next(ac))
            {
                ac.Add(qs);
                ac.brk = cx as Activation;
                ac.cont = ac;
                ac = (Activation)ObeyList(stms, ac);
                if (ac.signal != null)
                    ac.signal.Throw(cx);
            }
            cx.SlideDown(ac);
            return cx;
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
            +(FetchStatement.Cursor,c))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (OpenStatement)base.Relocate(wr);
            var c = cursor.Relocate(wr);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (OpenStatement)base.Frame(cx);
            var c = cursor.Frame(cx);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return cursor.Calls(defpos, db);
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            cursor.spec.RowSets(cx);
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
            +(FetchStatement.Cursor,c))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (CloseStatement)base.Relocate(wr);
            var c = cursor.Relocate(wr);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (CloseStatement)base.Frame(cx);
            var c = cursor.Frame(cx);
            if (c != cursor)
                r += (FetchStatement.Cursor, c);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return cursor.Calls(defpos, db);
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
            Cursor = -129, // SqlCursor
            How = -130, // Sqlx
            Outs = -131, // BList<SqlValue>
            Where = -132; // SqlValue
        SqlCursor cursor =>(SqlCursor)mem[Cursor];
        /// <summary>
        /// The behaviour of the Fetch
        /// </summary>
        public Sqlx how => (Sqlx)(mem[How]??Sqlx.ALL);
        /// <summary>
        /// The given absolute or relative position if specified
        /// </summary>
        public SqlValue where => (SqlValue)mem[Where];
        /// <summary>
        /// The list of assignable expressions to receive values
        /// </summary>
		public BList<SqlValue> outs => 
            (BList<SqlValue>)mem[Outs]?? BList<SqlValue>.Empty; 
        /// <summary>
        /// Constructor: a fetch statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
        /// <param name="h">The fetch behaviour: ALL, NEXT, LAST PRIOR, ABSOLUTE, RELATIVE</param>
        /// <param name="w">The output variables</param>
        public FetchStatement(long dp, SqlCursor n, Sqlx h, SqlValue w)
        : base(dp, BTree<long, object>.Empty + (Cursor, n) + (How, h) + (Where, w))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (FetchStatement)base.Relocate(wr);
            var c = cursor.Relocate(wr);
            if (c != cursor)
                r += (Cursor, c);
            var os = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=outs.First();b!=null;b=b.Next())
            {
                var ou = (SqlValue)b.value().Relocate(wr);
                ch = ch || ou != b.value();
                os += ou;
            }
            if (ch)
                r += (Outs, os);
            var w = where.Relocate(wr);
            if (w != where)
                r += (Where, w);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (FetchStatement)base.Frame(cx);
            var c = cursor.Frame(cx);
            if (c != cursor)
                r += (Cursor, c);
            var os = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = outs.First(); b != null; b = b.Next())
            {
                var ou = (SqlValue)b.value().Frame(cx);
                ch = ch || ou != b.value();
                os += ou;
            }
            if (ch)
                r += (Outs, os);
            var w = where?.Frame(cx);
            if (w != where)
                r += (Where, w);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return cursor.Calls(defpos, db) || where.Calls(defpos,db) || Calls(outs,defpos,db);
        }
        /// Execute a fetch
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var cs = cursor.spec.Refresh(cx);
            // position the cursor as specified
            var rqpos = 0L;
            var rb = cx.values[cursor.defpos] as Cursor;
            if (rb!=null)
                rqpos= rb._pos+1;
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
                    rqpos = (long)where.Eval(cx).Val();
                    break;
                case Sqlx.RELATIVE:
                    rqpos = rqpos + (long)where.Eval(cx).Val();
                    break;
            }
            if (rb == null || rqpos == 0)
                rb = cx.data[cs.defpos].First(cx);
            while (rb!= null && rqpos != rb._pos)
                rb = rb.Next(cx);
            if (rb == null)
                cx = new Signal(defpos,"02000", "No data").Obey(cx);
            else
            {
                var dt = cs.rowType;
                for (int i = 0; i < dt.Length; i++)
                {
                    var c = rb[i];
                    if (c != null)
                    {
                        var ou = outs[i];
                        if (ou != null)
                            cx.AddValue(ou, c);
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
            Parms = -133, // BList<SqlValue>
            ProcDefPos = -134, // long
            Var = -135; // SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public SqlValue var => (SqlValue)mem[Var];
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public long procdefpos => (long)mem[ProcDefPos];
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public BList<SqlValue> parms =>
            (BList<SqlValue>)mem[Parms]?? BList<SqlValue>.Empty;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
        public CallStatement(long dp, Procedure pr, string pn, BList<SqlValue> ps,SqlValue tg=null)
         : this(dp, pr, pn, ps, (tg==null)?null: new BTree<long, object>(Var,tg))
        { }
        protected CallStatement(long dp, Procedure pr, string pn, BList<SqlValue> ps, BTree<long,object> m=null)
         : base(dp, (m??BTree<long, object>.Empty) + (Parms, ps) + (ProcDefPos, pr?.defpos??-1L)
               +(_Domain,pr?.domain??Domain.Content) +(Procedure.RetType,pr?.retType?? ObInfo.Any) + (Name,pn))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (CallStatement)base.Relocate(wr);
            var ps = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=parms.First();b!=null;b=b.Next())
            {
                var p = (SqlValue)b.value().Relocate(wr);
                ch = ch||p != b.value();
                ps += p;
            }
            if (ch)
                r += (Parms, ps);
            var vr = var.Relocate(wr);
            if (vr != var)
                r += (Var, vr);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (CallStatement)base.Frame(cx);
            var ps = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = parms.First(); b != null; b = b.Next())
            {
                var p = (SqlValue)b.value().Frame(cx);
                ch = ch || p != b.value();
                ps += p;
            }
            if (ch)
                r += (Parms, ps);
            var vr = var?.Frame(cx);
            if (vr != var)
                r += (Var, vr);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            if (var != null && var.Calls(defpos, db))
                return true;
            return procdefpos==defpos || Calls(parms,defpos, db);
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
            var nv = var._Replace(cx,so, sv);
            if (nv != var)
                r += (Var, nv);
            var np = r.parms;
            for (var b=parms.First();b!=null;b=b.Next())
            {
                var a = b.value()._Replace(cx, so, sv);
                if (a != b.value())
                    np += (b.key(), (SqlValue)a);
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
            Objects = -137, // object[]
            _Signal = -138, // string
            SetList = -139, // BTree<Sqlx,SqlValue>
            SType = -140; // Sqlx RAISE or RESIGNAL
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype => (Sqlx)(mem[SType] ?? Sqlx.NO); // RAISE or RESIGNAL
        internal string signal => (string)mem[_Signal];
        internal object[] objects => (object[])mem[Objects] ?? new object[0];
        internal Exception exception => (Exception)mem[Exception];
        internal BTree<Sqlx, SqlValue> setlist =>
            (BTree<Sqlx, SqlValue>)mem[SetList] ?? BTree<Sqlx, SqlValue>.Empty;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public Signal(long dp, string n, params object[] m) : base(dp, BTree<long, object>.Empty
            + (_Signal, n) + (Objects, m))
        { }
        public Signal(long dp, DBException e) : base(dp, BTree<long, object>.Empty
            + (_Signal, e.signal) + (Objects, e.objects) + (Exception, e))
        { }
        protected Signal(long dp, BTree<long, object> m) :base(dp, m) {}
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (Signal)base.Relocate(wr);
            var sl = BTree<Sqlx, SqlValue>.Empty;
            var ch = false;
            for(var b=setlist.First();b!=null;b=b.Next())
            {
                var s = (SqlValue)b.value().Relocate(wr);
                ch = ch || s != b.value();
                sl += (b.key(),s);
            }
            if (ch)
                r += (SetList, sl);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (Signal)base.Frame(cx);
            var sl = BTree<Sqlx, SqlValue>.Empty;
            var ch = false;
            for (var b = setlist.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)b.value().Frame(cx);
                ch = ch || s != b.value();
                sl += (b.key(), s);
            }
            if (ch)
                r += (SetList, sl);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            for (var b = setlist.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return base.Calls(defpos, db);
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
                dia+=(s.key(), s.value().Eval(cx));
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
                    e.info +=(x.key(), x.value().Eval(cx));
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
            List = -141; // BTree<Sqlvalue,Sqlx>
        internal BTree<SqlValue,Sqlx> list => 
            (BTree<SqlValue,Sqlx>)mem[List]??BTree<SqlValue, Sqlx>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var sl = BTree<SqlValue, Sqlx>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)b.key().Relocate(wr);
                ch = ch || s != b.key();
                sl += (s,b.value());
            }
            if (ch)
                r += (List, sl);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = base.Frame(cx);
            var sl = BTree<SqlValue, Sqlx>.Empty;
            var ch = false;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)b.key().Frame(cx);
                ch = ch || s != b.key();
                sl += (s, b.value());
            }
            if (ch)
                r += (List, sl);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            for (var b = list.First(); b != null; b = b.Next())
                if (b.key().Calls(defpos, db))
                    return true;
            return base.Calls(defpos, db);
        }
        /// <summary>
        /// Obey a GetDiagnostics statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Context Obey(Context cx)
        {
            cx.exec = this;
            for (var b = list.First(); b != null; b = b.Next())
                cx.AddValue(b.key(), cx.tr.diagnostics[b.value()]);
            return cx;
        }
    }
    /// <summary>
    /// A Select statement: single row
    /// </summary>
	internal class SelectSingle : Executable
	{
        internal const long
            Outs = -142; // BList<SqlValue>
        /// <summary>
        /// The query
        /// </summary>
		public CursorSpecification sel => (CursorSpecification)mem[ForSelectStatement.Sel];
        /// <summary>
        /// The output list
        /// </summary>
		public BList<SqlValue> outs =>
            (BList<SqlValue>)mem[Outs] ?? BList<SqlValue>.Empty;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SelectSingle)base.Relocate(wr);
            var se = sel.Relocate(wr);
            if (se != sel)
                r += (ForSelectStatement.Sel, se);
            var os = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=outs.First();b!=null;b=b.Next())
            {
                var ou = (SqlValue)b.value().Relocate(wr);
                ch = ch || ou != b.value();
                os += ou;
            }
            if (ch)
                r += (Outs, os);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SelectSingle)base.Frame(cx);
            var se = sel.Frame(cx);
            if (se != sel)
                r += (ForSelectStatement.Sel, se);
            var os = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = outs.First(); b != null; b = b.Next())
            {
                var ou = (SqlValue)b.value().Frame(cx);
                ch = ch || ou != b.value();
                os += ou;
            }
            if (ch)
                r += (Outs, os);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return Calls(outs,defpos, db);
        }
        /// <summary>
        /// Execute a select statement: single row
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Context Obey(Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            var sr = sel.RowSets(cx);
            var rb = sr.First(cx);
            a.AddValue(this,rb);
            if (rb != null)
                for (var b = outs.First(); b != null; b = b.Next())
                    a.AddValue(b.value(), rb[b.key()]);
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
            CredPw = -143, //SqlValue
            CredUs = -144, // SqlValue
            Mime = -145, // string
            Posted = -146, // SqlValue
            Url = -147, // SqlValue
            Verb = -148, // SqlValue
            Where = -149; //SqlValue
        /// <summary>
        /// The url provided
        /// </summary>
        internal SqlValue url => (SqlValue)mem[Url];
        /// <summary>
        /// The posted data provided
        /// </summary>
        internal SqlValue data => (SqlValue)mem[Posted];
        /// <summary>
        /// The requested mime data type
        /// </summary>
        internal string mime => (string)mem[Mime]??"";
        /// <summary>
        /// The credentials if supplied (null means use default credentials)
        /// </summary>
        internal SqlValue us => (SqlValue)mem[CredUs];
        internal SqlValue pw => (SqlValue)mem[CredPw];
        internal SqlValue wh => (SqlValue)mem[Where];
        internal SqlValue verb => (SqlValue)mem[Verb];
        /// <summary>
        /// Constructor: a HTTP REST request
        /// </summary>
        public HttpREST(long dp, string v, SqlValue u, SqlValue p)
        : base(dp, BTree<long, object>.Empty + (Verb, v) + (CredUs, u) + (CredPw, p))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (HttpREST)base.Relocate(wr);
            var cp = pw.Relocate(wr);
            if (cp != pw)
                r += (CredPw, cp);
            var cu = us.Relocate(wr);
            if (cu != us)
                r += (CredUs, cu);
            var cd = data.Relocate(wr);
            if (cd != data)
                r += (Posted, cd);
            var cw = wh.Relocate(wr);
            if (cw != wh)
                r += (Where, wh); 
            var cv = verb.Relocate(wr);
            if (cv != verb)
                r += (Verb, cv);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (HttpREST)base.Frame(cx);
            var cp = pw.Frame(cx);
            if (cp != pw)
                r += (CredPw, cp);
            var cu = us.Frame(cx);
            if (cu != us)
                r += (CredUs, cu);
            var cd = data.Frame(cx);
            if (cd != data)
                r += (Posted, cd);
            var cw = wh.Frame(cx);
            if (cw != wh)
                r += (Where, wh);
            var cv = verb.Frame(cx);
            if (cv != verb)
                r += (Verb, cv);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return data?.Calls(defpos, db) == true || url?.Calls(defpos, db) == true
                || wh?.Calls(defpos, db) == true;
        }
        /// <summary>
        /// Obey the HTTP request
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Context Obey(Context cx)
        {
            var a = (Activation)cx;
            a.exec = this;
            var s = url.Eval(cx)?.ToString();
            if (s == null)
                return cx;
            // Okay, use HTTP
            var rq = SqlHttp.GetRequest(cx, url.Eval(cx).ToString());
            rq.UserAgent = "Pyrrho 7.0 http://" + PyrrhoStart.host + "/" 
                + cx.tr.startTime + "/" + cx.db.loadpos;
            rq.ContentType = mime ?? "application/tcc+json, application/json";
            var vb =verb.Eval(cx).ToString();
            rq.Method = vb;
            if (vb!="DELETE")
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