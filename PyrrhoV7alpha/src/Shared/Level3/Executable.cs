using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
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
            Label = -95, // string
            Stmt = -96, // string
            _Type = -97; // Executable.Type
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
		public Executable(Lexer cx,BTree<long,object> m) :base(cx.offset+cx.start,m)
        {  }
        public Executable(Lexer cx, Type t) : this(cx,new BTree<long,object>(_Type,t))
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
		protected Transaction ObeyList(BList<Executable> e,Transaction tr,Context cnx)
		{
            if (e == null)
                throw new DBException("42173", (cnx as Activation)?.label??"");
            for (var b = e.First();b!=null;b=b.Next())
            {
                var a = cnx.GetActivation(); // always access top of stack (don't pass as a parameter
                if (a.signal != null)
                    tr = a.signal.Obey(tr,cnx);
                if (cnx.breakto == a)
                    cnx.breakto = null;
                if (cnx.breakto != null || a.signal!=null)
                    return tr;
                try
                {
                    var x = b.value();
                    tr = x.Obey(tr,cnx);
                    if (a.signal != null)
                        a.signal.Throw(tr,a);
                }
                catch (DBException ex)
                {
                    a.signal = new Signal(cnx.cxid,ex);
                }
            }
            return tr;
		}
        /// <summary>
        /// Obey the Executable for the given Activation.
        /// All non-CRUD Executables should have a shortcut override.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public virtual Transaction Obey(Transaction tr,Context cx)
        {
            if (type == Type.RollbackWork)
                throw tr.Exception("40000").ISO();
            throw new NotImplementedException();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Executable(defpos,m);
        }
    }
    internal class CommitStatement : Executable
    {
        public CommitStatement(Lexer lx) : base(lx, Type.Commit) { }
    }
    /// <summary>
    /// A Select Statement can be used in a stored procedure so is a subclass of Executable
    /// </summary>
    internal class SelectStatement : Executable
    {
        internal const long
                        CS = -98; //CursorSpecification
        /// <summary>
        /// The cusorspecification (a Query) for this executable
        /// </summary>
        public CursorSpecification cs=>(CursorSpecification)mem[CS];
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The cursor specification</param>
        public SelectStatement(Lexer cx,CursorSpecification c) 
            : base (cx,new BTree<long,object>(CS,c))
        { }
        /// <summary>
        /// Obey the executable
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            cx.data = cs.RowSets(tr, cx);
            return tr;
        }
    }
    /// <summary>
    /// A Compound Statement for the SQL procedure language
    /// </summary>
    internal class CompoundStatement : Executable
    {
        internal const long
                        Stms = -99; // BList<Executable>
        /// <summary>
        /// The contained list of Executables
        /// </summary>
		public BList<Executable> stms =>
            (BList<Executable>)mem[Stms] ?? BList<Executable>.Empty;
        /// <summary>
        /// Constructor: create a compound statement
        /// </summary>
        /// <param name="n">The label for the compound statement</param>
		public CompoundStatement(Lexer cx,string n) 
            : base(cx,new BTree<long,object>(Label,n))
		{ }
        protected CompoundStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CompoundStatement operator +(CompoundStatement c, (long, object) x)
        {
            return new CompoundStatement(c.defpos, c.mem + x);
        }
        /// <summary>
        /// Obey a Compound Statement.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation();// use the top of the stack every time
            a.exec = this;
            var act = new Activation(cx, label);
            try
            {
                tr = ObeyList(stms, tr,act);
                if (cx.breakto == a)
                    cx.breakto = null;
                if (act.signal != null)
                    act.signal.Throw(tr,a);
            }
            catch (Exception e) { throw e; }
            return tr;
        }
	}
#if JAVASCRIPT
    /// <summary>
    /// For JavaScript
    /// </summary>
    internal class LocalVariables : Executable
    {
        public LocalVariableDec var;
        public LocalVariables next;
        internal LocalVariables(LocalVariableDec v, LocalVariables n): base(Type.VariableDec)
        {
            var = v;
            next = n;
        }
        public override void Obey(Transaction tr)
        {
            var.Obey(tr);
            if (next != null)
                next.Obey(tr);
        }
    }
#endif
    /// <summary>
    /// A local variable declaration.
    /// </summary>
	internal class LocalVariableDec : Executable
    {
        internal const long
            DataType = -100, // Domain
            Init = -101; // TypedValue
        /// <summary>
        /// The declared data type for the variable
        /// </summary>
        public Domain dataType => (Domain)mem[DataType] ?? Domain.Null;
        /// <summary>
        /// Default initialiser
        /// </summary>
        public TypedValue init => (TypedValue)mem[Init];
        /// <summary>
        /// Constructor: a new local variable
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// <param name="n">The name of the variable</param>
        /// <param name="dt">The data type</param>
        public LocalVariableDec(Lexer lx, string n, Domain dt, BTree<long,object> m=null)
            : base(lx,(m??BTree<long,object>.Empty)+(Label,n)+(DataType,dt))
        { }
        protected LocalVariableDec(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LocalVariableDec operator+(LocalVariableDec s,(long,object)x)
        {
            return new LocalVariableDec(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the local variable declaration, by adding the local variable to the activation (overwrites any previous)
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation();
            a.exec = this;
            TypedValue tv = init??dataType.defaultValue;
            a.locals += (defpos, true); // local variables need special handling
            cx.values += (defpos, tv); // We expect a==cx, but if not, tv will be copied to a later
            return tr;
        }
	}
    /// <summary>
    /// A procedure formal parameter is a dynamically initialised local variable
    /// </summary>
	internal class ProcParameter : LocalVariableDec
	{
        internal const long
            ParamMode = -102, // Sqlx
            Result = -103; // Sqlx
        /// <summary>
        /// The mode of the parameter: IN, OUT or INOUT
        /// </summary>
		public Sqlx paramMode => (Sqlx)(mem[ParamMode]??Sqlx.IN);
        /// <summary>
        /// The result mode of the parameter: RESULT or NO
        /// </summary>
		public Sqlx result => (Sqlx)(mem[Result]??Sqlx.NO);
        /// <summary>
        /// Constructor: a procedure formal parameter from the parser
        /// </summary>
        /// <param name="m">The mode</param>
		public ProcParameter(Lexer cx, Sqlx m,string n,Domain dt) : base(cx,n,dt,
            new BTree<long,object>(ParamMode,m))
		{ }
        protected ProcParameter(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ProcParameter operator+(ProcParameter s,(long,object)x)
        {
            return new ProcParameter(s.defpos, s.mem + x);
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
            CS = SelectStatement.CS; // SqlCursor
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
        public CursorDeclaration(Lexer cx,string n,CursorSpecification c) 
            : base(cx,n,c.rowType,new BTree<long,object>(CS,c)) 
        { }
        protected CursorDeclaration(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// Instantiate the cursor
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            cx.values += (defpos, cs.Eval(tr, cx));
            return tr;
        }
    }
    /// <summary>
    /// An Exception handler for a stored procedure
    /// </summary>
	internal class HandlerStatement : Executable
	{
        internal const long
            Action = -104, // Executable
            Conds = -105, // BList<string>
            HType = -106; // Sqlx
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
		public HandlerStatement(Lexer ctx, Sqlx t, string n)
            : base(ctx, BTree<long, object>.Empty + (HType, t) + (Name, n)) { }
        protected HandlerStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static HandlerStatement operator+(HandlerStatement s,(long,object)x)
        {
            return new HandlerStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Obeying the handler declaration means installing the handler for each condition
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Transaction Obey(Transaction tr,Context cx)
		{
            var a = cx.GetActivation(); // get the top of stack each time
            a.exec = this;
            a.saved = new ExecState(tr,cx);
            for (var c =conds.First();c!=null;c=c.Next())
				a.exceptions+=(c.value(),new Handler(this,a,cx.cxid));
            return tr;
		}
    }
    /// <summary>
    /// A Handler helps implementation of execption handling
    /// </summary>
	internal class Handler : Executable
	{
        internal const long
            HDefiner = -107, // Activation
            Hdlr = -108; //HandlerStatement
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
		public Handler(HandlerStatement h, Activation a, long cd) : base(cd, BTree<long, object>.Empty
            + (Hdlr, h) + (HDefiner, a))
        { }
        protected Handler(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Handler operator+(Handler h,(long,object)x)
        {
            return new Handler(h.defpos, h.mem + x);
        }
        /// <summary>
        /// Execute the action part of the Handler Statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cnx)
        {
            var a = cnx.GetActivation(); // from the top of the stack each time
            try
            {
                if (hdlr.htype == Sqlx.UNDO)
                {
                    CompoundStatement cs = null;
                    for (Context p = a; cs == null && p != null; p = p.next)
                        cs = p.exec as CompoundStatement;
                    if (cs != null)
                    {
                        tr = hdefiner.saved.mark;
                        cnx.next = hdefiner.saved.stack;
                    }
                    a.breakto = null;
                }
                tr = hdlr.action.Obey(tr,cnx);
                if (hdlr.htype == Sqlx.EXIT)
                    a.breakto = hdefiner.next;
                if (a.signal != null)
                    a.signal.Throw(tr,a);
            }
            catch (Exception e) { throw e; }
            return tr;
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
		public BreakStatement(Lexer cx,string n) : base(cx,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected BreakStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BreakStatement operator+(BreakStatement b,(long,object)x)
        {
            return new BreakStatement(b.defpos, b.mem + x);
        }
        /// <summary>
        /// Execute a break statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            a.breakto = cx.Ctx(defpos) ?? a.brk;
            return tr;
        }
	}
    /// <summary>
    /// An assignment statement for a stored procedure
    /// </summary>
	internal class AssignmentStatement : Executable
    {
        internal const long
            Val = -109, //SqlValue
            Vbl = -110; // SqlValue
        /// <summary>
        /// The left hand side of the assignment, checked for assignability
        /// </summary>
        public SqlValue vbl;
        /// <summary>
        /// The right hand side of the assignment
        /// </summary>
		public SqlValue val = null;
        /// <summary>
        /// Constructor: An assignment statement from the parser
        /// </summary>
		public AssignmentStatement(Lexer cx) : base(cx, BTree<long, object>.Empty)
        { }
        protected AssignmentStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static AssignmentStatement operator+(AssignmentStatement s,(long,object)x)
        {
            return new AssignmentStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx; // top of the stack
            a.exec = this;
            var t = vbl.nominalDataType;
            if (val != null)
            {
                var v = t.Coerce(val.Eval(tr,cx)?.NotNull());
                cx.values += (vbl.defpos, v);
            }
            return tr;
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
            LhsType = -111, // Domain
            List = -112, // BList<long>
            Rhs = -113; // SqlValue
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal BList<long> list => (BList<long>)mem[List];
        /// <summary>
        /// The row type of the lefthand side, used to coerce the given value 
        /// </summary>
        Domain lhsType => (Domain)mem[LhsType]??Domain.Content;
        /// <summary>
        /// The row-valued right hand side
        /// </summary>
        public SqlValue rhs => (SqlValue)mem[Rhs];
        /// <summary>
        /// Constructor: A new multiple assignment statement from the parser
        /// </summary>
        public MultipleAssignment(Lexer cx) : base(cx,BTree<long,object>.Empty)
        { }
        protected MultipleAssignment(long dp, BTree<long, object> m) : base(dp, m) { }
        public static MultipleAssignment operator+(MultipleAssignment s,(long,object)x)
        {
            return new MultipleAssignment(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the multiple assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            TRow r = (TRow)rhs.Eval(tr,cx);
            for (int j = 0; j < r.dataType.Length; j++)
                cx.values+=(list[j],r[j]);
            return tr;
        }
    }
    /// <summary>
    /// A return statement for a stored procedure or function
    /// </summary>
	internal class ReturnStatement : Executable
    {
        internal const long
            Ret = -115; // SqlValue
        /// <summary>
        /// The return value
        /// </summary>
		public SqlValue ret => (SqlValue)mem[Ret];
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(Lexer cx) : base (cx,BTree<long,object>.Empty)
        {  }
        protected ReturnStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ReturnStatement operator+(ReturnStatement s,(long,object)x)
        {
            return new ReturnStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the return statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
			a.ret = ret.Eval(tr,cx);
            return tr;
		}
	}
    /// <summary>
    /// A Case statement for a stored procedure
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        internal const long
            Else = -116, // BList<Executable> 
            Operand = -117, // SqlValue
            Whens = -118; // BTree<int,WhenPart>
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
        public SimpleCaseStatement(Lexer cx) : base(cx,BTree<long,object>.Empty)
        { }
        protected SimpleCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SimpleCaseStatement operator+(SimpleCaseStatement s,(long,object)x)
        {
            return new SimpleCaseStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for(var c = whens.First();c!=null; c=c.Next())
                if (operand.Matches(tr,cx))
                    return ObeyList(c.value().stms, tr,cx);
            return ObeyList(els, tr,cx);
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
		public BTree<int, WhenPart> whens => (BTree<int, WhenPart>)mem[SimpleCaseStatement.Whens];
        /// <summary>
        /// An else part
        /// </summary>
		public BList<Executable> els => 
            (BList<Executable>)mem[SimpleCaseStatement.Else]?? BList<Executable>.Empty;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(Lexer cx) : base(cx,BTree<long,object>.Empty)
        {  }
        protected SearchedCaseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SearchedCaseStatement operator+(SearchedCaseStatement s,(long,object)x)
        {
            return new SearchedCaseStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a searched case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// 
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
			for(var c = whens.First();c!=null;c=c.Next())
				if (c.value().cond.Matches(tr,cx))
					return ObeyList(c.value().stms,tr,cx);
			return ObeyList(els,tr,cx);
		}
	}
    /// <summary>
    /// A when part for a searched case statement or trigger action
    /// </summary>
	internal class WhenPart :Executable
	{
        internal const long
            Cond = -119, // SqlValue
            Stms = -120; // BList<Executable>
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
        public WhenPart(Lexer cx,SqlValue v, BList<Executable> s) : base(cx, BTree<long,object>.Empty
            +(Cond,v)+(Stms,s))
        { }
        protected WhenPart(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhenPart operator+(WhenPart s,(long,object) x)
        {
            return new WhenPart(s.defpos, s.mem + x);
        }
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx;
            if (cond == null || cond.Matches(tr, cx))
            {
                a.ret = TBool.True;
                return ObeyList(stms, tr,cx);
            }
            a.ret = TBool.False;
            return tr;
        }
    }
    /// <summary>
    /// An if-then-else statement for a stored proc/func
    /// </summary>
	internal class IfThenElse : Executable
	{
        internal const long
            Search = -121, // SqlValue
            Then = -122, // BList<Executable>
            Else = -123, // BList<Executable>
            Elsif = -124; // BList<Executable>
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
		public IfThenElse(Lexer cx) : base(cx,BTree<long,object>.Empty)
		{}
        protected IfThenElse(long dp, BTree<long, object> m) : base(dp, m) { }
        public static IfThenElse operator+(IfThenElse s,(long,object)x)
        {
            return new IfThenElse(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Obey an if-then-else statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr, Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (search.Matches(tr, cx))
                return ObeyList(then, tr, cx);
            for (var g = elsif.First(); g != null; g = g.Next())
                if (g.value() is IfThenElse f && f.search.Matches(tr, cx))
                    return ObeyList(f.then, tr, cx);
            return ObeyList(els, tr, cx);
        }
	}
    internal class XmlNameSpaces : Executable
    {
        internal const long
            Nsps = -125; // BTree<string,string>
        /// <summary>
        /// A list of namespaces to be added
        /// </summary>
        public BTree<string, string> nsps => (BTree<string,string>)mem[Nsps];
        /// <summary>
        /// Constructor
        /// </summary>
        public XmlNameSpaces(Lexer cx) : base(cx,BTree<long,object>.Empty) { }
        protected XmlNameSpaces(long dp, BTree<long, object> m) : base(dp, m) { }
        public static XmlNameSpaces operator+(XmlNameSpaces s,(long,object)x)
        {
            return new XmlNameSpaces(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Add the namespaces
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            for(var b=nsps.First();b!= null;b=b.Next())
                cx.nsps+=(b.key(),b.value());
            return tr;
        }
    }
    /// <summary>
    /// A while statement for a stored proc/func
    /// </summary>
	internal class WhileStatement : Executable
	{
        internal const long
            Loop = -126, // long
            Search = -127, // SqlValue
            What = -128; // BList<Executable>
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
		public WhileStatement(Lexer cx,string n) : base(cx,new BTree<long,object>(Label,n)) 
        {  }
        protected WhileStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static WhileStatement operator+(WhileStatement s,(long,object)x)
        {
            return new WhileStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a while statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Transaction Obey(Transaction tr, Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            while (a.signal == null && search.Matches(tr, cx))
            {
                if (cx.breakto != null)
                    break;
                var lp = new Activation(cx, label);
                lp.cont = a;
                lp.brk = a;
                tr = ObeyList(what, tr, lp);
                a.SlideDown(lp);
                a.signal = lp.signal;
            }
            return tr;
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
        public long loop => (long)(mem[WhileStatement.Loop]??0);
        /// <summary>
        /// Constructor: a repeat statement from the parser
        /// </summary>
        /// <param name="n">The label</param>
        public RepeatStatement(Lexer cx,string n) : base(cx,new BTree<long,object>(Label,n)) 
        {  }
        protected RepeatStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RepeatStatement operator+(RepeatStatement s,(long,object)x)
        {
            return new RepeatStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the repeat statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Transaction Obey(Transaction tr, Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            var first = true;
            var act = new Activation(cx, label);
            while (act.signal == null && (first || !search.Matches(tr, act)))
            {
                first = false;
                if (act.breakto == act)
                    act.breakto = null;
                if (act.breakto != null)
                    break;
                var lp = new Activation(act, null);
                lp.brk = a;
                lp.cont = act;
                tr = ObeyList(what, tr, lp);
                act.SlideDown(lp);
                act.signal = lp.signal;
                if (lp.breakto != null && lp.breakto != act)
                    act.breakto = lp.breakto;
                if (act.breakto == act)
                    act.breakto = null;
            }
            a.SlideDown(act);
            if (a.breakto == a)
                a.breakto = null;
            return tr;
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
        /// <param name="n">The name f the iterator</param>
		public IterateStatement(Lexer cx,string n,long i):base(cx,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected IterateStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static IterateStatement operator+(IterateStatement s,(long,object)x)
        {
            return new IterateStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the iterate statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            a.breakto = a.cont;
            return tr;
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
		public LoopStatement(Lexer cx,string n,long i):base(cx,new BTree<long,object>(Label,n))
		{ }
        protected LoopStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LoopStatement operator+(LoopStatement s,(long,object)x)
        {
            return new LoopStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the loop statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr, Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            var act = new Activation(cx, label);
            var lp = new Activation(act, null);
            for (; ; )
            {
                if (lp.breakto == act)
                    lp.breakto = null;
                if (lp.breakto != null)
                    break;
                lp.brk = a;
                lp.cont = act;
                tr = ObeyList(stms, tr,lp);
                act.SlideDown(lp);
                if (lp.signal != null)
                    lp.signal.Throw(tr,a);
            }
            if (act.breakto == a)
                act.breakto = null;
            if (act.signal != null)
                act.signal.Throw(tr,a);
            cx.SlideDown(act);
            return tr;
        }
	}
    /// <summary>
    /// A for statement for a stored proc/func
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        internal const long
            Sel = -129, // CursorSpecification
            Cursor = -130, // string
            ForVn = -131, // string
            Loop = -132, // long
            Stms = -133; // BList<Executable>
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
        public long loop => (long)(mem[Loop] ?? -1);
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public BList<Executable> stms => (BList<Executable>)mem[Stms];
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
        public ForSelectStatement(Lexer cx,string n) : base(cx,BTree<long,object>.Empty
            +(Label,n))
		{ }
        protected ForSelectStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ForSelectStatement operator+(ForSelectStatement s,(long,object)x)
        {
            return new ForSelectStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a FOR statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            cx.data = sel.RowSets(tr,cx);
            if (cx.data == null)
                return tr;
            var qs = sel.union.left;
            var ac = new Activation(cx, label);
            ac.Add(qs);
            var dt = sel.rowType;
            for (var rb = cx.data.First(cx); rb != null; rb = rb.Next(cx))
            {
                var lp = new Activation(ac, null);
                lp.Add(qs);
                lp.brk = a;
                lp.cont = ac;
                tr = ObeyList(stms, tr,lp);
                if (lp.signal != null)
                    lp.signal.Throw(tr,a);
                ac.SlideDown(lp);
            }
            a.SlideDown(ac);
            return tr;
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
		public OpenStatement(Lexer cx,SqlCursor c,long i) : base(cx,BTree<long,object>.Empty
            +(FetchStatement.Cursor,c))
		{ }
        protected OpenStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static OpenStatement operator+(OpenStatement s,(long,object)x)
        {
            return new OpenStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            cx.data = cursor.spec.RowSets(tr, cx);
            return tr;
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
		public CloseStatement(Lexer cx, SqlCursor c,long i) : base(cx,BTree<long,object>.Empty
            +(FetchStatement.Cursor,c))
		{ }
        protected CloseStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CloseStatement operator+(CloseStatement s,(long,object)x)
        {
            return new CloseStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute the close statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            cx.data = EmptyRowSet.Value;
            return tr;
        }
	}
    /// <summary>
    /// A fetch statement for a stored proc/func
    /// </summary>
	internal class FetchStatement : Executable
	{
        internal const long
            Cursor = -134, // SqlCursor
            How = -135, // Sqlx
            Outs = -136, // BList<SqlValue>
            Where = -137; // SqlValue
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
        public FetchStatement(Lexer cx, SqlCursor n, Sqlx h, SqlValue w)
        : base(cx, BTree<long, object>.Empty + (Cursor, n) + (How, h) + (Where, w))
        { }
        protected FetchStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static FetchStatement operator+(FetchStatement s,(long,object)x)
        {
            return new FetchStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a fetch
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var cs = (Query)cx.obs[cursor.defpos];
            // position the cursor as specified
            var rqpos = 0L;
            if (cx.rb!=null)
                rqpos= cx.rb._pos+1;
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
                        for (var e = cx.data.First(cx); e != null; e = e.Next(cx))
                            n++;
                        rqpos = n - 1;
                        break;
                    }
                case Sqlx.ABSOLUTE:
                    rqpos = (long)where.Eval(tr,cx).Val();
                    break;
                case Sqlx.RELATIVE:
                    rqpos = rqpos + (long)where.Eval(tr,cx).Val();
                    break;
            }
            if (cx.rb == null || rqpos == 1)
                cx.rb = cx.data.First(cx);
            while (cx.rb != null && rqpos != cx.rb._pos)
                cx.rb = cx.rb.Next(cx);
            if (cx.rb == null)
                tr = new Signal(defpos,"02000", "No data").Obey(tr,cx);
            else
            {
                var dt = cs.rowType;
                for (int i = 0; i < dt.Length; i++)
                {
                    var c = cx.rb.row[i];
                    if (c != null)
                    {
                        var ou = outs[i];
                        if (ou != null)
                            cx.values += (ou.defpos, c);
                    }
                }
            }
            return tr;
        }
 	}
    /// <summary>
    /// A call statement for a stored proc/func
    /// </summary>
	internal class CallStatement : Executable
	{
        internal const long
            Parms = -138, // BList<SqlValue>
            OwningType = -139, // Domain
            Proc = -140, // Procedure
            RetType = -141, // Domain
            Var = -142; // SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public SqlValue var => (SqlValue)mem[Var];
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public Procedure proc => (Procedure)mem[Proc];
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public BList<SqlValue> parms =>
            (BList<SqlValue>)mem[Parms]?? BList<SqlValue>.Empty;
        /// <summary>
        /// Required result type (Null=void)
        /// </summary>
        public Domain returnType => (Domain)mem[RetType]??Domain.Null;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
		public CallStatement(long dp) : base(dp,BTree<long,object>.Empty)
		{ }
        protected CallStatement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static CallStatement operator+(CallStatement s,(long,object)x)
        {
            return new CallStatement(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a proc/method call
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
		{
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            return proc.Exec(tr,cx,parms);
		}
        internal override DBObject Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var nv = var.Replace(cx,so, sv);
            if (nv != var)
                r += (Var, nv);
            var np = r.parms;
            for (var b=parms.First();b!=null;b=b.Next())
            {
                var a = b.value().Replace(cx, so, sv);
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
            Exception = -143, // Exception
            Objects = -144, // object[]
            _Signal = -145, // string
            SetList = -146, // BTree<Sqlx,SqlValue>
            SType = -147; // Sqlx RAISE or RESIGNAL
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
		public Signal(Lexer cx, string n, params object[] m) : base(cx, BTree<long, object>.Empty
            + (_Signal, n) + (Objects, m))
        { }
        public Signal(long dp,string n, params object[] m) : base(dp, BTree<long, object>.Empty
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
        /// <summary>
        /// Execute a signal
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (stype==Sqlx.RESIGNAL && !tr.diagnostics.Contains(Sqlx.RETURNED_SQLSTATE))
                    throw new DBException("0K000").ISO();
            string sclass = signal.Substring(0, 2);
            var dia = tr.diagnostics;
            dia +=(Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    dia+=(b.key(), b.value());
                dia+=(Sqlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
            }
            for (var s = setlist.First();s!= null;s=s.Next())
                dia+=(s.key(), s.value().Eval(tr,cx));
            tr += (Transaction.Diagnostics, dia);
            Handler h = null;
            Activation cs = null;
            for (cs = a; h == null && cs != null;)
            {
                h = cs.exceptions[signal];
                if (h == null && Char.IsDigit(signal[0]))
                    h = cs.exceptions[sclass + "000"];
                if (h == null)
                    h = cs.exceptions["SQLEXCEPTION"];
            }
            if (h == null || sclass == "25" || sclass == "40" || sclass == "2D") // no handler or uncatchable transaction errors
            {
                for (; cs != null && a != cs; a = cx.GetActivation())
                    cx = a;
                a.signal = this;
                cx.breakto = cs;
            }
            else
            {
                cx.breakto = null;
                a.signal = null;
                tr = h.Obey(tr,cx);
            }
            return tr;
		}
        /// <summary>
        /// Throw this signal
        /// </summary>
        /// <param name="cx">the context</param>
        public void Throw(Transaction tr,Context cx)
        {
            var e = exception as DBException;
            if (e == null)
            {
                e = new DBException(signal, objects);
                for (var x = setlist.First();x!= null;x=x.Next())
                    e.info +=(x.key(), x.value().Eval(tr, cx));
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
            List = -148; // BTree<Sqlvalue,Sqlx>
        internal BTree<SqlValue,Sqlx> list => 
            (BTree<SqlValue,Sqlx>)mem[List]??BTree<SqlValue, Sqlx>.Empty;
        internal GetDiagnostics(Lexer cx) : base(cx,BTree<long,object>.Empty) { }
        protected GetDiagnostics(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GetDiagnostics operator+(GetDiagnostics s,(long,object)x)
        {
            return new GetDiagnostics(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Obey a GetDiagnostics statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx; // from the top of the stack each time
            a.exec = this;
            for (var b = list.First(); b != null; b = b.Next())
                a.values += (b.key().defpos, tr.diagnostics[b.value()]);
            return tr;
        }
    }
    /// <summary>
    /// A Select statement: single row
    /// </summary>
	internal class SelectSingle : Executable
	{
        internal const long
            Outs = -149; // BList<SqlValue>
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
		public SelectSingle(Lexer cx) : base(cx,BTree<long,object>.Empty)
		{ }
        protected SelectSingle(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SelectSingle operator+(SelectSingle s,(long,object)x)
        {
            return new SelectSingle(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Execute a select statement: single row
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            cx.data = sel.RowSets(tr, cx);
            a.rb = cx.data.First(cx);
            if (a.rb != null)
                for (var b = outs.First(); b != null; b = b.Next())
                    a.values += (b.value().defpos, a.rb.row[b.key()]);
            else
                a.NoData(tr);
            return tr;
        }
	}
    /// <summary>
    /// An executable for an HTTP REST request from the parser
    /// </summary>
    internal class HttpREST : Executable
    {
        internal const long
            CredPw = -150, //SqlValue
            CredUs = -151, // SqlValue
            Mime = -152, // string
            Posted = -153, // SqlValue
            Url = -154, // SqlValue
            Verb = -155, // SqlValue
            Where = -156; //SqlValue
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
        public HttpREST(Lexer cx, string v, SqlValue u, SqlValue p)
        : base(cx, BTree<long, object>.Empty + (Verb, v) + (CredUs, u) + (CredPw, p))
        { }
        protected HttpREST(long dp, BTree<long, object> m) : base(dp, m) { }
        public static HttpREST operator+(HttpREST s,(long,object)x)
        {
            return new HttpREST(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Obey the HTTP request
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override Transaction Obey(Transaction tr,Context cx)
        {
            var a = cx.GetActivation(); // from the top of the stack each time
            a.exec = this;
            var s = url.Eval(tr,cx)?.ToString();
            if (s == null)
                return tr;
            // Okay, use HTTP
            var rq = SqlHttp.GetRequest(tr, url.Eval(tr,cx).ToString());
            rq.UserAgent = "Pyrrho 5.7 http://" + PyrrhoStart.host + "/" + tr.startTime + "/" + tr.loadpos;
            rq.ContentType = mime ?? "application/tcc+json, application/json";
            var vb =verb.Eval(tr,cx).ToString();
            rq.Method = vb;
            if (vb!="DELETE")
            {
                var rst = new System.IO.StreamWriter(rq.GetRequestStream());
                rst.WriteLine(cx.data.ToString());
                rst.Close();
            }
            var rr = new System.IO.StreamReader(rq.GetResponse().GetResponseStream());
            cx.ret = new TChar(rr.ReadToEnd());
            return tr;
        }
    }
}