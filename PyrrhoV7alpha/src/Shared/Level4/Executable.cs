using System;
using Pyrrho.Common;
using PyrrhoBase;
using Pyrrho.Level1;
using Pyrrho.Level2;
using Pyrrho.Level3;
#if !SILVERLIGHT
using System.IO;
#endif
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
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
	internal class Executable
	{
        static int _dbg = 0;
        public int dbg = ++_dbg;
        public string stmt = null;
        public string blockid; // e.g. dbix||defpos
        /// <summary>
        /// The Executable.Type enumeration includes all the SQL2011 possibilities.
        /// Some of them make no sense in Pyrrho.
        /// </summary>
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
            When = 230
#if MONGO|| JAVASCRIPT
            ,Function = 224, // JavaScript
            For = 225, // JavaScript
            Continue = 226, // JavaScript
            With = 227, // JavaScript
            Expression = 228 // JavaScript
#endif
#if MONGO
            ,MongoIndex = 229 // Mongo
#endif
        }
        /// <summary>
        /// The label for the Executable
        /// </summary>
        internal readonly Ident label;
        /// <summary>
        /// Many Executables create an Activation for condition handling and control statements.
        /// They are entered into the transaction stack so that their local variables can be referenced.
        /// </summary>
        public Activation act = null;
        /// <summary>
        /// Diagnostics area has the concept of nested executions
        /// </summary>
        public Executable outer = null;
        /// <summary>
        /// Constructor: define an Executable of a given type.
        /// Procedure statements are subclasses of Executable
        /// </summary>
        /// <param name="t">The Executable.Type</param>
		public Executable(Type t,string i,Ident n = null)
        {
            type = t;
            blockid = i;
            label = n;
        }
        protected Executable(Executable e, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            type = e.type;
            blockid = e.blockid;
            label = e.label;
            stmt = e.stmt;
            act = e.act?.Copy(ref cs,ref vs);
        }
        internal virtual Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new Executable(this, ref cs,ref vs);
        }
        /// <summary>
        /// The Executable.Type of the Executable
        /// </summary>
		public Type type;
        /// <summary>
        /// Support execution of a list of Executables, in an Activation.
        /// With break behaviour
        /// </summary>
        /// <param name="e">The Executables</param>
        /// <param name="tr">The transaction</param>
		protected void ObeyList(Executable[] e,Context cnx)
		{
            if (e == null)
                throw new DBException("42173", cnx.GetActivation().staticLink.alias?.ident??"???");
            for (int i = 0; i < e.Length; i++)
            {
                var a = cnx.GetActivation(); // always access top of stack (don't pass as a parameter
                if (a.signal != null)
                    a.signal.Obey(cnx);
                if (cnx.breakto == a)
                    cnx.breakto = null;
                if (cnx.breakto != null || a.signal!=null)
                    return;
                try
                {
                    var x = e[i];
                    x.Obey(cnx);
                    if (a.signal != null)
                        a.signal.Throw(a);
                }
                catch (DBException ex)
                {
                    a.signal = new Signal(blockid,ex);
                }
            }
		}
        /// <summary>
        /// Obey the Executable for the given Activation.
        /// All non-CRUD Executables should have a shortcut override.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public virtual void Obey(Transaction tr)
        {
            if (type == Type.RollbackWork)
                throw tr.Exception("40000").ISO();
            new Parser(tr, blockid).ParseProcedureStatement(stmt);
        }
        /// <summary>
        /// Push an Executable into the context, saving the current context;
        /// This machinery is used only for the COMMAND_FUNCTION diagnostic field
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public void SPush(Context cx)
        {
            var context = cx.context;
            outer = context?.exec;
            if (context != null)
                context.exec = this; // context = cx now of course
        }
        /// <summary>
        /// Pop the current executable out of the context
        /// </summary>
        /// <returns></returns>
        public Executable SPop(Context cx)
        {
            var context = cx.context;
            var r = context?.exec;
            if (r != null)
            {
                if (r.dbg != dbg)
                    throw new PEException("Unreasonable static link stack");
                context.exec = r.outer;
                return r;
            }
            return null;
        }
	}
    /// <summary>
    /// A Select Statement can be used in a stored procedure so is a subclass of Executable
    /// </summary>
    internal class SelectStatement : Executable
    {
        /// <summary>
        /// The cusorspecification (a Query) for this executable
        /// </summary>
        public CursorSpecification cs;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The cursor specification</param>
        public SelectStatement(CursorSpecification c,string i) : base (Type.Select,i)
        {
            cs = c;
        }
        protected SelectStatement(SelectStatement s, ref ATree<string, Context> css, ref ATree<long, SqlValue> vs) 
            :base(s,ref css,ref vs)
        {
            cs = (CursorSpecification)s.cs.Copy(ref css,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new SelectStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Obey the executable
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            cs.Analyse(tr);
        }
    }
    /// <summary>
    /// A Compound Statement for the SQL procedure language
    /// </summary>
    internal class CompoundStatement : Executable
	{
        /// <summary>
        /// The contained list of Executables
        /// </summary>
		public Executable[] stms = null;
        /// <summary>
        /// Constructor: create a compound statement
        /// </summary>
        /// <param name="n">The label for the compound statement</param>
		public CompoundStatement(Ident n, string i) : base(Type.Compound, i, n)
		{
		}
        protected CompoundStatement(CompoundStatement c, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(c,ref cs, ref vs)
        {
            stms = new Executable[c.stms.Length];
            for (var i = 0; i < stms.Length; i++)
                stms[i] = c.stms[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new CompoundStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Obey a Compound Statement.
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation();// use the top of the stack every time
            a.exec = this;
            act = new Activation(tr, label, blockid);
            act.Push(tr);
            try
            {
                ObeyList(stms, tr);
                if (tr.breakto == a)
                    tr.breakto = null;
                if (act.signal != null)
                    act.signal.Throw(a);
            }
            catch (Exception e) { throw e; }
            finally { act.Pop(tr); }
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
        internal override void SetupValues(Context cx)
        {
            var.SetupValues(cx);
            if (next != null)
                next.SetupValues(cx);
        }
    }
#endif
    /// <summary>
    /// A local variable declaration.
    /// </summary>
	internal class LocalVariableDec : Executable
	{
        /// <summary>
        /// The declared data type for the variable
        /// </summary>
        public Domain dataType;
        public Ident name;
        /// <summary>
        /// Default initialiser
        /// </summary>
        public string init = null;
        /// <summary>
        /// Constructor: a new local variable
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// <param name="n">The name of the variable</param>
        /// <param name="dt">The data type</param>
        public LocalVariableDec(Transaction tr, Ident n, Domain dt)
            : base(Type.VariableDec, tr.context.blockid,n)
        {
            dataType = dt;
            name = n;
            if (dataType.kind == Sqlx.TABLE) // make it look like a real table
            {
                var db = tr.db;
                var tt = new Table(db);
                var ro = db._Role;
                var dfs = ro.defs;
                var rr = new RoleObject(tt.defpos, n);
                BTree<long, RoleObject>.Add(ref dfs, tt.defpos, rr);
                ro.defs = dfs;
                int sq = 0;
                foreach (var c in dataType.columns)
                {
                    var tc = new TableColumn(db, sq, c.kind, db.NextPos(), new BTree<long, Grant.Privilege>(db.Transrole, Grant.Privilege.Usage))
                    { sensitive = dt.kind == Sqlx.SENSITIVE };
                    tt.AddColumn(tr, tc.defpos, db, tc, dataType.names[sq]);
                    sq++;
                }
            }
            if (tr.context is Activation ac)
                Ident.Tree<Activation.Variable>.Add(ref ac.vars,name,new Activation.Variable(tr, dt, name) { blockid = ac.blockid });
        }
        protected LocalVariableDec(LocalVariableDec d, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(d,ref cs,ref vs)
        {
            dataType = d.dataType;
            name = d.name;
            init = d.init;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new LocalVariableDec(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the local variable declaration, by adding the local variable to the activation (overwrites any previous)
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation() ;// use the top of the stack every time
            a.exec = this;
            TypedValue tv = dataType.New(tr);
            if (init != null)
                tv = new Parser(tr,blockid).ParseSqlValue(init, dataType).Eval(tr,tr.GetRowSet());
            a.Define(tr,dataType,name).Set(tr,tv);
        }
	}
    /// <summary>
    /// A procedure formal parameter is a dynamically initialised local variable
    /// </summary>
	internal class ProcParameter : LocalVariableDec
	{
        /// <summary>
        /// The mode of the parameter: IN, OUT or INOUT
        /// </summary>
		public Sqlx paramMode = Sqlx.IN;
        /// <summary>
        /// The result mode of the parameter: RESULT or NO
        /// </summary>
		public Sqlx result = Sqlx.NO;
        /// <summary>
        /// Constructor: a procedure formal parameter
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// <param name="m">The mode</param>
        /// <param name="i">The name</param>
        /// <param name="dt">The data type</param>
		public ProcParameter(Transaction tr, Sqlx m,Ident i,Domain dt) : base(tr, i, dt)
		{
			paramMode = m;
        }
        protected ProcParameter(ProcParameter p, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(p,ref cs, ref vs)
        {
            paramMode = p.paramMode;
            result = p.result;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new ProcParameter(this,ref cs,ref vs);
        }
        /// <summary>
        /// A readable version of the ProcParameter
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ((paramMode==Sqlx.IN)?"":paramMode.ToString()+" ") + 
                ((result==Sqlx.NO)?"":result.ToString()+" ") + base.ToString();
        }
	}
    /// <summary>
    /// A local cursor
    /// </summary>
    internal class CursorDeclaration : LocalVariableDec
    {
        /// <summary>
        /// The specification for the cursor
        /// </summary>
        public SqlCursor cs;
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="i">The name</param>
        /// <param name="c">The cursor specification</param>
        public CursorDeclaration(Transaction tr,Ident i,CursorSpecification c) 
            : base(tr,i,c.nominalDataType) 
        {
            cs = new SqlCursor(tr,c,i);
            if (tr.context is Activation ac)
                Ident.Tree<Activation.Variable>.Update(ref ac.vars, i, cs);
        }
        protected CursorDeclaration(CursorDeclaration c, ref ATree<string, Context> css, ref ATree<long, SqlValue> vs) 
            :base(c,ref css, ref vs)
        {
            cs = (SqlCursor)c.cs.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new CursorDeclaration(this,ref cs,ref vs);
        }
        /// <summary>
        /// Instantiate the cursor
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            if (tr.context is Activation ac)
            {
                var vs = BTree<long, SqlValue>.Empty;
                var nc = (SqlCursor)cs.Copy(ref vs);
                nc.Set(tr, cs.Eval(tr, null));
               Ident.Tree<Activation.Variable>.Add(ref ac.vars, cs.name, nc);
            }
        }
    }
    /// <summary>
    /// An Exception handler for a stored procedure
    /// </summary>
	internal class HandlerStatement : Executable
	{
        /// <summary>
        /// The handler type: CONTINUE, EXIT, or UNDO
        /// </summary>
		public Sqlx htype = Sqlx.EXIT;
        /// <summary>
        /// A list of condition names, SQLSTATE codes, "SQLEXCEPTION", "SQLWARNING", or "NOT_FOUND"
        /// </summary>
		public string[] conds = null;
        /// <summary>
        /// The statement to execute when one of these conditions happens
        /// </summary>
        public Executable action = null;
        /// <summary>
        /// Constructor: a handler statement for a stored procedure
        /// </summary>
        /// <param name="t">CONTINUE, EXIT or UNDO</param>
		public HandlerStatement(Context ctx, Sqlx t,string i): base(Executable.Type.HandlerDef,i)
		{
			htype = t;
		}
        protected HandlerStatement(HandlerStatement h, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(h,ref cs,ref vs)
        {
            htype = h.htype;
            conds = h.conds;
            action = h.action.Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new HandlerStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Obeying the handler declaration means installing the handler for each condition
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override void Obey(Transaction tr)
		{
            var a = tr.GetActivation(); // get the top of stack each time
            a.exec = this;
            a.saved = new ExecState(tr);
            foreach (var c in conds)
				BTree<string,Handler>.Add(ref a.exceptions,c,new Handler(this,a,action.blockid));
		}
    }
    /// <summary>
    /// A Handler helps implementation of execption handling
    /// </summary>
	internal class Handler : Executable
	{
        /// <summary>
        /// The shared handler statement
        /// </summary>
		HandlerStatement hdlr = null;
        /// <summary>
        /// The activation that defined this
        /// </summary>
        Activation definer;
        /// <summary>
        /// Constructor: a Handler instance
        /// </summary>
        /// <param name="h">The HandlerStatement</param>
		public Handler(HandlerStatement h,Activation a,string i) : base(Type.HandlerDef,i)
		{
			hdlr = h; definer = a;
		}
        /// <summary>
        /// Execute the action part of the Handler Statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Context cnx)
        {
            var tr = cnx as Transaction;
            var a = cnx.GetActivation(); // from the top of the stack each time
            if (act == null)
                act = new Activation(cnx, label, blockid)
                {
                    exec = this
                };
            act.Push(tr);
            try
            {
                if (hdlr.htype == Sqlx.UNDO)
                {
                    CompoundStatement cs = null;
                    for (var p = a; cs == null && p != null; p = p.dynLink)
                        cs = p.exec as CompoundStatement;
                    if (cs != null)
                        definer.saved.Restore(tr);
                    tr.breakto = null;
                }
                hdlr.action.Obey(tr);
                if (hdlr.htype == Sqlx.EXIT)
                    tr.breakto = definer.dynLink??(definer.staticLink as Activation);
                if (act.signal != null)
                    act.signal.Throw(a);
            }
            catch (Exception e) { throw e; }
            finally { act.Pop(tr); }
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
		public BreakStatement(Ident n,string i) : base(Type.Break,i, n)
		{
		}
        protected BreakStatement(BreakStatement b, ref ATree<string, Context> cs,ref ATree<long,SqlValue> vs) 
            : base(b, ref cs,ref vs)
        {
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new BreakStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a break statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            tr.breakto = tr.FindActivation(blockid) ?? a.brk;
        }
	}
#if MONGO || JAVASCRIPT
    /// <summary>
    /// A Continue statement for JavaScript
    /// </summary>
    internal class ContinueStatement : Executable
    {
        /// <summary>
        /// Constructor: A continue statement
        /// </summary>
        public ContinueStatement()
            : base(Executable.Type.Continue)
        {
        }
        /// <summary>
        /// Execute a continue statement
        /// </summary>
        /// <param name="a">The Activation</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            a.breakto = new Ident("continue");
        }
    } 
#endif
    /// <summary>
    /// An assignment statement for a stored procedure
    /// </summary>
	internal class AssignmentStatement : Executable
	{
        /// <summary>
        /// The left hand side of the assignment, checked for assignability
        /// </summary>
        public Target vbl;
        /// <summary>
        /// The right hand side of the assignment
        /// </summary>
		public SqlValue val = null;
        /// <summary>
        /// Constructor: An assignment statement from the parser
        /// </summary>
		public AssignmentStatement(string i) : base(Type.Assignment,i)
		{
		}
        protected AssignmentStatement(AssignmentStatement a, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(a, ref cs, ref vs)
        {
            vbl = a.vbl.Copy(ref vs);
            val = a.val.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new AssignmentStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.context; // top of the stack
            a.exec = this;
            var t = vbl.dataType;
            if (val != null)
            {
                var v = t.Coerce(tr, val.Eval(tr,tr.GetRowSet())?.NotNull());
#if MONGO
                if (t.kind == Sqlx.DOCUMENT) // Pyrrho v5.1 special case
                {
                    var w = vbl.Eval(a) as TDocument;
                    if (w==null)
                        w = new TDocument(a);
                    v = w.Update(a.transaction, v as TDocument);
                }
#endif
                vbl.Assign(tr,v);
            }
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
        /// <summary>
        /// The list of identifiers
        /// </summary>
        internal Ident[] list = null;
        /// <summary>
        /// The row type of the lefthand side, used to coerce the given value 
        /// </summary>
        Domain lhsType = Domain.Content;
        /// <summary>
        /// The row-valued right hand side
        /// </summary>
        public SqlValue rhs = null;
        /// <summary>
        /// Constructor: A new multiple assignment statement from the parser
        /// </summary>
        public MultipleAssignment(string i) : base(Type.Assignment,i)
        {
        }
        protected MultipleAssignment(MultipleAssignment m, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(m,ref cs,ref vs)
        {
            list = m.list;
            lhsType = m.lhsType;
            rhs = m.rhs.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new MultipleAssignment(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the multiple assignment
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.context; // from the top of the stack each time
            a.exec = this;
            TRow r = (TRow)rhs.Eval(tr,tr.GetRowSet());
            for (int j = 0; j < r.dataType.Length; j++)
            {
                var n = a.MustFind(tr,list[j]).LVal(tr);
                var p = r[j];
                var dt = n.dataType;
                if (!dt.HasValue(tr, p))
                    throw tr.Exception("22005Q", dt.kind, p.dataType).ISO()
                        .AddType(dt).AddValue(p.dataType);
                n.Assign(tr,p);
            }
            tr.context = a;// don't forget to update
        }
    }
    /// <summary>
    /// A return statement for a stored procedure or function
    /// </summary>
	internal class ReturnStatement : Executable
	{
        /// <summary>
        /// The return value
        /// </summary>
		public SqlValue ret = null;
        public Domain typ = Domain.Null;
        /// <summary>
        /// Constructor: a return statement from the parser
        /// </summary>
        public ReturnStatement(string i) : base (Type.Return,i)
        {  }
        protected ReturnStatement(ReturnStatement r, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(r,ref cs,ref vs)
        {
            ret = r.ret.Copy(ref vs);
            typ = r.typ;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new ReturnStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the return statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
			a.ret = ret.Eval(tr,tr.GetRowSet());
		}
	}
    /// <summary>
    /// A Case statement for a stored procedure
    /// </summary>
    internal class SimpleCaseStatement : Executable
    {
        /// <summary>
        /// The test expression
        /// </summary>
        public SqlValue operand = null;
        /// <summary>
        /// A list of when parts
        /// </summary>
        public WhenPart[] whens = null;
        /// <summary>
        /// An else part
        /// </summary>
        public Executable[] els = null;
        /// <summary>
        /// Constructor: a case statement from the parser
        /// </summary>
        public SimpleCaseStatement(string i) : base(Type.Case, i)
        {
        }
        protected SimpleCaseStatement(SimpleCaseStatement s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(s,ref cs,ref vs)
        {
            operand = s.operand.Copy(ref vs);
            whens = new WhenPart[s.whens.Length];
            for (var i = 0; i < whens.Length; i++)
                whens[i] = (WhenPart)s.whens[i].Copy(ref cs,ref vs);
            els = new Executable[s.els.Length];
            for (var i = 0; i < els.Length; i++)
                els[i] = s.els[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new SimpleCaseStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            foreach(var c in whens)
            {
                if (operand.Matches(tr,tr.GetRowSet()))
                {
                    ObeyList(c.stms, tr);
                    return;
                }
            }
            ObeyList(els, tr);
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
		public WhenPart[] whens = null;
        /// <summary>
        /// An else part
        /// </summary>
		public Executable[] els = null;
        /// <summary>
        /// Constructor: a searched case statement from the parser
        /// </summary>
		public SearchedCaseStatement(string i) : base(Type.Case, i)
        {
        }
        protected SearchedCaseStatement(SearchedCaseStatement s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
            :base(s,ref cs,ref vs)
        {
            whens = new WhenPart[s.whens.Length];
            for (var i = 0; i < whens.Length; i++)
                whens[i] = (WhenPart)s.whens[i].Copy(ref cs,ref vs);
            els = new Executable[s.els.Length];
            for (var i = 0; i < els.Length; i++)
                els[i] = s.els[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new SearchedCaseStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a searched case statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        /// 
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
			foreach (var c in whens)
			{
				if (c.cond.Matches(tr,tr.GetRowSet()))
				{
					ObeyList(c.stms,tr);
					return;
				}
			}
			ObeyList(els,tr);
		}
	}
    /// <summary>
    /// A when part for a searched case statement or trigger action
    /// </summary>
	internal class WhenPart :Executable
	{
        /// <summary>
        /// A search condition for the when part
        /// </summary>
		public SqlValue cond = null;
        /// <summary>
        /// a list of statements
        /// </summary>
		public Executable[] stms = null;
        /// <summary>
        /// Whether the statement has just fired
        /// </summary>
        public bool fired = false;
        /// <summary>
        /// Constructor: A searched when part from the parser
        /// </summary>
        /// <param name="v">A search condition</param>
        /// <param name="s">A list of statements for this when</param>
        public WhenPart(SqlValue v, Executable[] s, string i = "") : base(Type.When, i)
        {
            cond = v;
            stms = s;
        }
        protected WhenPart(WhenPart w, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(w,ref cs,ref vs)
        {
            cond = w.cond.Copy(ref vs);
            stms = new Executable[w.stms.Length];
            for (var i = 0; i < stms.Length; i++)
                stms[i] = w.stms[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new WhenPart(this,ref cs,ref vs);
        }
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation();
            if (cond == null || cond.Matches(tr, tr.GetRowSet()))
            {
                fired = true;
                ObeyList(stms, tr);
            }
        }
    }
    /// <summary>
    /// An if-then-else statement for a stored proc/func
    /// </summary>
	internal class IfThenElse : Executable
	{
        /// <summary>
        /// The test condition
        /// </summary>
		public SqlValue search = null;
        /// <summary>
        /// The then statements
        /// </summary>
		public Executable[] then = null;
        /// <summary>
        /// The elsif parts
        /// </summary>
		public Executable[] elsif = null;
        /// <summary>
        /// The else part
        /// </summary>
		public Executable[] els = null;
        /// <summary>
        /// Constructor: an if-then-else statement from the parser
        /// </summary>
		public IfThenElse(string i) : base(Type.IfThenElse,i)
		{
		}
        protected IfThenElse(IfThenElse i, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(i,ref cs,ref vs)
        {
            search = i.search.Copy(ref vs);
            then = new Executable[i.then.Length];
            for (var j = 0; j < then.Length; j++)
                then[j] = i.then[j].Copy(ref cs,ref vs);
            elsif = new Executable[i.elsif.Length];
            for (var j = 0; j < elsif.Length; j++)
                elsif[j] = i.elsif[j].Copy(ref cs,ref vs);
            els = new Executable[i.els.Length];
            for (var j = 0; j < els.Length; j++)
                els[j] = i.els[j].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new IfThenElse(this,ref cs,ref vs);
        }
        /// <summary>
        /// Obey an if-then-else statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            var r = tr.GetRowSet();
            a.exec = this;
			if (search.Matches(tr,r))
				ObeyList(then,tr);
			else 
			{
				foreach(var g in elsif)
					if (g is IfThenElse f && f.search.Matches(tr,r))
					{
						ObeyList(f.then,tr);
						return;
					}
				ObeyList(els,tr);
			}
		}
	}
#if MONGO || JAVASCRIPT
    /// <summary>
    /// for JavaScript
    /// </summary>
    internal class ExpressionStatement : Executable
    {
        SqlValue exp;
        public ExpressionStatement(SqlValue e)
            : base(Executable.Type.Expression)
        {
            exp = e;
        }
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.ret = exp.Eval(tr.context);
        }
        internal override void SetupValues(Context cx)
        {
            SqlValue.Setup(exp,Domain.Null);
        }
    }
    /// <summary>
    /// for JavaScript
    /// </summary>
    internal class WithStatement : Executable
    {
        SqlValue val;
        Executable body;
        public WithStatement(SqlValue v, Executable e):base(Executable.Type.With)
        {
            val = v; body = e;
        }
        internal override void SetupValues(Context cx)
        {
            SqlValue.Setup(val,Domain.Null);
            body.SetupValues(cx);
        }
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            var na = new StructuredActivation(a, val.nominalDataType);
            if (na.Push())
            {
                body.Obey(tr);
                na.Pop();
            }
        }
    }
#endif
    internal class XmlNameSpaces : Executable
    {
        /// <summary>
        /// A list of namespaces to be added
        /// </summary>
        public ATree<string, string> nsps;
        /// <summary>
        /// Constructor
        /// </summary>
        public XmlNameSpaces(string i) : base(Type.Namespace,i) { }
        protected XmlNameSpaces(XmlNameSpaces x, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(x,ref cs, ref vs)
        {
            nsps = x.nsps;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new XmlNameSpaces(this,ref cs,ref vs);
        }
        /// <summary>
        /// Add the namespaces
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            for(var b=nsps.First();b!= null;b=b.Next())
                ATree<string,string>.Add(ref tr.context.nsps,b.key(),b.value());
        }
    }
    /// <summary>
    /// A while statement for a stored proc/func
    /// </summary>
	internal class WhileStatement : Executable
	{
        /// <summary>
        /// The search condition for continuing
        /// </summary>
		public SqlValue search = null;
        /// <summary>
        /// The statements to execute
        /// </summary>
		public Executable[] what = null;
        Activation loop = null;
        /// <summary>
        /// Constructor: a while statement from the parser
        /// </summary>
        /// <param name="n">The label for the while</param>
		public WhileStatement(Ident n,string i) : base(Type.While, i, n) 
        {
        }
        protected WhileStatement(WhileStatement w, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(w,ref cs,ref vs)
        {
            search = w.search.Copy(ref vs);
            what = new Executable[what.Length];
            for (var i = 0; i < what.Length; i++)
                what[i] = w.what[i].Copy(ref cs,ref vs);
            loop = w.loop?.Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new WhileStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a while statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (act == null)
            {
                act = new Activation(tr, label,blockid+"A");
                loop = new Activation(tr, null, blockid+"B");
            }
            act.Push(tr);
            var r = tr.GetRowSet();
            try
            {
                while (a.signal == null && search.Matches(tr,r))
                {
                    if (tr.breakto == act)
                        tr.breakto = null;
                    if (tr.breakto != null)
                        break;
                    loop.Push(tr);
                    try
                    {
                        loop.cont = act;
                        loop.brk = a;
                        ObeyList(what, tr);
                        a.signal = loop.signal;
                    }
                    catch (Exception e) { throw e; }
                    finally { loop.Pop(tr); }
                }
            }
            catch (Exception e) { throw e; }
            finally { act.Pop(tr); }
            tr.context = a; // don't forget to update
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
		public SqlValue search = null;
        /// <summary>
        /// The list of statements to execute at least once
        /// </summary>
		public Executable[] what = null;
        Activation loop = null;
        /// <summary>
        /// Constructor: a repeat statement from the parser
        /// </summary>
        /// <param name="n">The label</param>
        public RepeatStatement(Ident n,string i) : base(Type.Repeat, i,n) 
        {
        }
        protected RepeatStatement(RepeatStatement r, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(r,ref cs,ref vs)
        {
            search = r.search.Copy(ref vs);
            what = new Executable[r.what.Length];
            for (var i = 0; i < what.Length; i++)
                what[i] = r.what[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new RepeatStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the repeat statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (act == null)
            {
                act = new Activation(tr, label,blockid+"A");
                loop = new Activation(tr, null,blockid+"B");
            }
            var first = true;
            act.Push(tr);
            var r = tr.GetRowSet();
            try
            {
                while (a.signal == null && (first || !search.Matches(tr,r)))
                {
                    first = false;
                    if (tr.breakto == act)
                        tr.breakto = null;
                    if (tr.breakto != null)
                        break;
                    loop.Push(tr);
                    try
                    {
                        loop.brk = a;
                        loop.cont = act;
                        ObeyList(what, tr);
                        a.signal = loop.signal;
                    }
                    catch (Exception e) { throw e; }
                    finally { loop.Pop(tr); }
                    if (tr.breakto == act)
                        tr.breakto = null;
                }
            }
            catch (Exception e) { throw e; }
            finally { act.Pop(tr); }
            if (tr.breakto == a)
                tr.breakto = null;
        }
	}
    /// <summary>
    /// An Iterate (like C continue;) statement for a stored proc/func 
    /// </summary>
	internal class IterateStatement : Executable
	{
        /// <summary>
        /// The label for the iterate
        /// </summary>
		public Ident id;
        /// <summary>
        /// Constructor: an iterate statement from the parser
        /// </summary>
        /// <param name="n">The name f the iterator</param>
		public IterateStatement(Ident n,string i):base(Type.Iterate,i)
		{
			id = n;
		}
        protected IterateStatement(IterateStatement i, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(i,ref cs,ref vs)
        {
            id = i.id;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new IterateStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the iterate statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            tr.breakto = a.cont;
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
		public Executable[] stms = null;
        Activation loop = null;
        /// <summary>
        /// Constructor: a loop statement from the parser
        /// </summary>
        /// <param name="s">The statements</param>
        /// <param name="n">The loop identifier</param>
		public LoopStatement(Ident n,string i):base(Type.Loop,i, n)
		{
		}
        protected LoopStatement(LoopStatement s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(s,ref cs,ref vs)
        {
            stms = new Executable[s.stms.Length];
            for (var i = 0; i < stms.Length; i++)
                stms[i] = s.stms[i].Copy(ref cs,ref vs);
            loop = s.loop?.Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new LoopStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute the loop statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (act == null)
            {
                act = new Activation(tr, label,blockid+"A");
                loop = new Activation(tr, null,blockid+"B");
            }
            act.Push(tr);
            try
            {
                for (; ; )
                {
                    if (tr.breakto == act)
                        tr.breakto = null;
                    if (tr.breakto != null)
                        break;
                    loop.Push(tr);
                    try
                    {
                        loop.brk = a;
                        loop.cont = act;
                        ObeyList(stms, tr);
                    }
                    catch (Exception e) { throw e; }
                    finally
                    {
                        loop.Pop(tr);
                    }
                    if (tr.breakto == act)
                        tr.breakto = null;
                    if (loop.signal != null)
                        loop.signal.Throw(a);
                }

                if (tr.breakto == a)
                    tr.breakto = null;
                if (act.signal != null)
                    act.signal.Throw(a);
            }
            catch (Exception e) { throw e; }
            finally { act.Pop(tr); }
        }
	}
#if MONGO || JAVASCRIPT
    internal class ForStatement: Executable
    {
        public LocalVariables var = null; // controlled variables
        public SqlValue search = null; // condition, may be null
        public SqlValue onIter = null; // statements to execute before iterations, may be null
        public Executable body = null; // the body of the loop, may be null
        Activation act = null;
        public ForStatement() : base(Executable.Type.For) { }
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (act==null)
                act = new Activation(a, lebel); 
            var.Obey(tr);
            for (a = tr.GetActivation(); (!Iterate(a)) && (search == null) ? true : (bool)search.Eval(tr.context).Val(); a = tr.GetActivation())
            {
                if (body != null)
                {
                    if (act.Push())
                    try {
                        body.Obey(tr);
                    } catch (Exception e) { throw e; }
                    } finally {
                        act.Pop();
                    }
                    a = tr.GetActivation();
                }
                if (onIter != null)
                    onIter.Eval(tr.context);
            }
        }
        internal override void SetupValues(Context cx)
        {
            var.SetupValues(cx);
            SqlValue.Setup(search, Domain.Bool);
            SqlValue.Setup(onIter, Domain.Null);
            if (body != null)
                body.SetupValues(cx);
        }
    }
#endif
    /// <summary>
    /// A for statement for a stored proc/func
    /// </summary>
	internal class ForSelectStatement : Executable
	{
        /// <summary>
        /// The query for the FOR
        /// </summary>
		public CursorSpecification sel = null;
        /// <summary>
        /// The name of the cursor
        /// </summary>
		public Ident cursor = null;
        /// <summary>
        /// The identifier in the AS part
        /// </summary>
		public Ident forvn = null;
        /// <summary>
        /// The FOR loop
        /// </summary>
        public Activation loop = null;
        /// <summary>
        /// The statements in the loop
        /// </summary>
		public Executable[] pb;
        /// <summary>
        /// Constructor: a for statement from the parser
        /// </summary>
        /// <param name="n">The label for the FOR</param>
		public ForSelectStatement(Ident n,string i) : base(Type.ForSelect, i,n)
		{
		}
        protected ForSelectStatement(ForSelectStatement f, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(f,ref cs,ref vs)
        {
            sel = (CursorSpecification)f.sel.Copy(ref cs,ref vs);
            cursor = f.cursor;
            forvn = f.forvn;
            loop = f.loop?.Copy(ref cs,ref vs);
            pb = new Executable[f.pb.Length];
            for (var i = 0; i < pb.Length; i++)
                pb[i] = f.pb[i].Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new ForSelectStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a FOR statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            tr.Execute(sel, false);
            if (sel.rowSet == null)
                return;
            var qs = sel.union.left;
            var ac = new Activation(tr, label, act.blockid);
            ac.Push(tr);
            ATree<string, Context>.Add(ref ac.contexts, qs.blockid, qs);
            try
            {
                var dt = sel.nominalDataType;
                for (var rb = sel.rowSet.First(); rb != null; rb = rb.Next())
                {
                    var lp = new Activation(tr, null, loop.blockid);
                    lp.Push(tr);
                    lp.Define(tr,qs.nominalDataType,cursor).Set(tr, new TContext(qs)); 
                    ATree<string, Context>.Add(ref tr.context.contexts, qs.blockid, qs);
                    try
                    {
                        lp.brk = a;
                        lp.cont = ac;
                        ObeyList(pb, tr);
                        if (lp.signal != null)
                            lp.signal.Throw(a);
                    }
                    catch (Exception e) { throw e; }
                    finally { lp.Pop(tr); }
                }
            }
            catch (Exception e) { throw e; }
            finally { ac.Pop(tr); }
            if (tr.breakto == a)
                tr.breakto = null;
            sel.rowSet = null;
        }
	}
    /// <summary>
    /// An Open statement for a cursor
    /// </summary>
	internal class OpenStatement : Executable
	{
        SqlCursor cursor;
        /// <summary>
        /// Constructor: an open statement from the parser
        /// </summary>
        /// <param name="n">the cursor name</param>
		public OpenStatement(SqlCursor c,string i) : base(Type.Open,i)
		{
            cursor = c;
		}
        protected OpenStatement(OpenStatement o, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(o,ref cs,ref vs)
        {
            cursor = (SqlCursor)o.cursor.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new OpenStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute an open statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            ((cursor.Eval(tr,tr.GetRowSet()) as TContext).ctx as Query).Analyse(tr);
        }
    }
    /// <summary>
    /// A Close statement for a cursor
    /// </summary>
	internal class CloseStatement : Executable
	{
        Activation.Variable cursor;
        /// <summary>
        /// Constructor: a close statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
		public CloseStatement(Activation.Variable c,string i) : base(Executable.Type.CloseCursor,i)
		{
            cursor = c;
        }
        protected CloseStatement(CloseStatement c, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(c, ref cs, ref vs)
        {
            cursor = (Activation.Variable)c.cursor.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new CloseStatement(this, ref cs, ref vs);
        }
        /// <summary>
        /// Execute the close statement
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            cursor.GetCursor(tr).rowSet = null;
        }
	}
    /// <summary>
    /// A fetch statement for a stored proc/func
    /// </summary>
	internal class FetchStatement : Executable
	{
        SqlCursor cursor;
        /// <summary>
        /// The behaviour of the Fetch
        /// </summary>
        public Sqlx how = Sqlx.ALL;
        /// <summary>
        /// The given absolute or relative position if specified
        /// </summary>
        public SqlValue where = null;
        /// <summary>
        /// The list of assignable expressions to receive values
        /// </summary>
		public SqlValue[] outs = null; 
        /// <summary>
        /// Constructor: a fetch statement from the parser
        /// </summary>
        /// <param name="n">The name of the cursor</param>
        /// <param name="h">The fetch behaviour: ALL, NEXT, LAST PRIOR, ABSOLUTE, RELATIVE</param>
        /// <param name="w">The output variables</param>
		public FetchStatement(SqlCursor n,Sqlx h,SqlValue w):base(Type.Fetch,n.spec.blockid)
		{
            cursor = n;
            how = h;
            where = w;
        }
        protected FetchStatement(FetchStatement f, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(f,ref cs,ref vs)
        {
            how = f.how;
            cursor = (SqlCursor)f.cursor.Copy(ref vs);
            where = f.where?.Copy(ref vs);
            outs = new SqlValue[f.outs.Length];
            for (var i = 0; i < outs.Length; i++)
                outs[i] = f.outs[i].Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new FetchStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a fetch
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Transaction tr)
        {
            var cs = (cursor.Eval(tr,tr.GetRowSet()) as TContext).ctx as Query;
            // position the cursor as specified
            var rqpos = 0L;
            if (cs.row!=null)
                rqpos= cs.row._pos+1;
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
                        for (var e = cs.rowSet.First(); e != null; e = e.Next())
                            n++;
                        rqpos = n - 1;
                        break;
                    }
                case Sqlx.ABSOLUTE:
                    rqpos = (long)where.Eval(tr,cs.rowSet).Val(tr);
                    break;
                case Sqlx.RELATIVE:
                    rqpos = rqpos + (long)where.Eval(tr,cs.rowSet).Val(tr);
                    break;
            }
            if (cs.row == null || rqpos == 1)
                cs.row = cs.rowSet.First();
            if (cs.row != null && rqpos != cs.row._pos)
                cs.row = cs.row.PositionAt(rqpos);
            if (cs.row == null)
                new Signal("02000", "No data").Obey(tr);
            else
            {
                var dt = cs.nominalDataType;
                for (int i = 0; i < dt.Length; i++)
                {
                    var c = cs.row.Get(dt.names[i]);
                    if (c != null)
                    {
                        var ou = outs[i];
                        if (ou != null)
                        {
                            SqlValue.Setup(tr,cs,ou, dt[i]);
                            var lv = ou.LVal(tr);
                            if (lv != null)
                                lv.Assign(tr,c);
                        }
                    }
                }
            }
        }
 	}
    /// <summary>
    /// A call statement for a stored proc/func
    /// </summary>
	internal class CallStatement : Executable
	{
        /// <summary>
        /// The target object (for a method)
        /// </summary>
		public SqlValue var = null;
        /// <summary>
        /// The method name + arity
        /// </summary>
		public Ident pname = null;
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public Procedure proc = null;
		public int database = 0;
        /// <summary>
        /// The list of actual parameters
        /// </summary>
		public SqlValue[] parms = new SqlValue[0];
        /// <summary>
        /// Required result type (Null=void)
        /// </summary>
        public Domain returnType = Domain.Null;
        /// <summary>
        /// Constructor: a procedure/function call
        /// </summary>
		public CallStatement(string i) : base(Type.Call,i)
		{
		}
        protected CallStatement(CallStatement c, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(c, ref cs,ref vs)
        {
            var = c.var?.Copy(ref vs);
            pname = c.pname;
            proc = c.proc;
            parms = new SqlValue[c.parms.Length];
            for (var i = 0; i < parms.Length; i++)
                parms[i] = c.parms[i].Copy(ref vs);
            returnType = c.returnType;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new CallStatement(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a proc/method call
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override void Obey(Transaction tr)
		{
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            proc.Exec(tr,database,pname,parms);
		}
        internal CallStatement Replace(Transaction tr,Context cx,SqlValue so,SqlValue sv,ref ATree<long,SqlValue>map)
        {
            var nv = var.Replace(tr, cx, so, sv, ref map);
            var changed = nv!=var;
            var np = new SqlValue[parms.Length];
            for (var i=0;i<parms.Length;i++)
            {
                np[i] = parms[i].Replace(tr, cx, so, sv, ref map);
                if (np[i] != parms[i])
                    changed = true;
            }
            if (!changed)
                return this;
            return new CallStatement(blockid) { var=nv, proc=proc,pname=pname,parms = np };
        }
	}
    /// <summary>
    /// A signal statement for a stored proc/func
    /// </summary>
	internal class Signal : Executable
	{
        /// <summary>
        /// The signal to raise
        /// </summary>
        internal Sqlx stype; // RAISE or RESIGNAL
        internal string signal;
        internal object[] objects;
        internal Exception exception = null;
        internal ATree<Sqlx, SqlValue> setlist = BTree<Sqlx, SqlValue>.Empty;
        Transaction trans = null;
        /// <summary>
        /// Constructor: a signal statement from the parser
        /// </summary>
        /// <param name="n">The signal name</param>
        /// <param name="m">The signal information items</param>
		public Signal(string n,string i,params object[] m) : base(Type.Signal,i)
		{
			signal = n;
            objects = m;
		}
        public Signal(string i,DBException e) : base(Type.Signal,i)
        {
            signal = e.signal;
            objects = e.objects;
            exception = e;
        }
        protected Signal(Signal s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(s,ref cs,ref vs)
        {
            stype = s.stype;
            signal = s.signal;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new Signal(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a signal
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override void Obey(Transaction tr)
        {
            trans = tr;
            var a = tr.GetActivation(); // from the top of the stack each time
            a.exec = this;
            if (stype==Sqlx.RESIGNAL && tr.diagnostics[Sqlx.RETURNED_SQLSTATE]==null)
                    throw tr.Exception("0K000").ISO();
            string sclass = signal.Substring(0, 2);
            tr.Put(Sqlx.RETURNED_SQLSTATE, new TChar(signal));
            if (exception is DBException dbex)
            {
                for (var b = dbex.info.First(); b != null; b = b.Next())
                    tr.Put(b.key(), b.value());
                tr.Put(Sqlx.MESSAGE_TEXT, new TChar(Resx.Format(dbex.signal, dbex.objects)));
            }
            for (var s = setlist.First();s!= null;s=s.Next())
                tr.Put(s.key(), s.value().Eval(tr,tr.GetRowSet()));
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
                    cs = cs.staticLink as Activation ?? cs.dynLink;
            }
            if (h == null || sclass == "25" || sclass == "40" || sclass == "2D") // no handler or uncatchable transaction errors
            {
                for (; cs != null && a != cs; a = tr.GetActivation())
                    a.Pop(tr);
                a.signal = this;
                tr.breakto = cs;
            }
            else
            {
                tr.breakto = null;
                a.signal = null;
                if (act == null)
                    act = new Activation(tr, label,blockid+"S");
                act.Push(tr);
                try
                {
                    h.Obey(tr);
                }
                catch (Exception e) { throw e; }
                finally { act.Pop(tr); }
            }
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
                    ATree<Sqlx, TypedValue>.Add(ref e.info, x.key(), x.value().Eval(trans, trans.GetRowSet()));
            }
            throw e;
        }
	}
    /// <summary>
    /// A GetDiagnostics statement for a routine
    /// </summary>
    internal class GetDiagnostics : Executable
    {
        ATree<SqlValue,Sqlx> list = BTree<SqlValue, Sqlx>.Empty;
        internal GetDiagnostics(string i) : base(Type.GetDescriptor,i) { }
        public void Add(SqlValue t, Sqlx w)
        {
            ATree<SqlValue,Sqlx>.Add(ref list,t,w);
        }
        protected GetDiagnostics(GetDiagnostics g, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            :base(g,ref cs,ref vs)
        {
            list = g.list;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new GetDiagnostics(this,ref cs,ref vs);
        }
        /// <summary>
        /// Obey a GetDiagnostics statement
        /// </summary>
        /// <param name="tr">the transaction</param>
        public override void Obey(Transaction tr)
        {
            var a = tr.context; // from the top of the stack each time
            a.exec = this;
            for (var b = list.First(); b != null; b = b.Next())
            {
                var d = tr.Get(b.value());
                var v = b.key().LVal(tr);
                v.Assign(tr, d);
            }
        }
    }
    /// <summary>
    /// A Select statement: single row
    /// </summary>
	internal class SelectSingle : Executable
	{
        /// <summary>
        /// The query
        /// </summary>
		public CursorSpecification sel = null;
        /// <summary>
        /// The output list
        /// </summary>
		public SqlValue[] outs = null;
        /// <summary>
        /// Constructor: a select statement: single row from the parser
        /// </summary>
        /// <param name="s">The select statement</param>
        /// <param name="sv">The list of variables to receive the values</param>
		public SelectSingle(string i) : base(Type.SelectSingle,i)
		{
		}
        protected SelectSingle(SelectSingle s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(s,ref cs,ref vs)
        {
            sel = (CursorSpecification)s.sel.Copy(ref cs,ref vs);
            outs = new SqlValue[s.outs.Length];
            for (var i = 0; i < outs.Length; i++)
                outs[i] = s.outs[i].Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new SelectSingle(this,ref cs,ref vs);
        }
        /// <summary>
        /// Execute a select statement: single row
        /// </summary>
        /// <param name="tr">The transaction</param>
		public override void Obey(Transaction tr)
        {
            var a = tr.GetActivation(); // from the top of the stack each time
            (a as Context).exec = this;
            if ((tr as Transaction).Execute(sel,false) is RowSet rs)
            {
                var b = rs.First();
                if (b != null)
                {
                    var dt = rs.rowType.TypeOf(tr, b.Value());
                    for (int i = 0; i < dt.Length; i++)
                    {
                        var lv = outs[i].LVal(tr);
                        if (lv != null)
                            lv.Assign(tr, b.Get(dt.names[0]));
                    }
                }
                else
                    a.NoData(tr as Transaction);
            } 
        }
	}
    /// <summary>
    /// An executable for an HTTP REST request from the parser
    /// </summary>
    internal class HttpREST : Executable
    {
        /// <summary>
        /// The url provided
        /// </summary>
        internal SqlValue url = null;
        /// <summary>
        /// The posted data provided
        /// </summary>
        internal SqlValue data = null;
        /// <summary>
        /// The requested mime data type
        /// </summary>
        internal string mime = "";
        /// <summary>
        /// The credentials if supplied (null means use default credentials)
        /// </summary>
        internal SqlValue us, pw, wh = null;
        /// <summary>
        /// Constructor: a HTTP REST request
        /// </summary>
        public HttpREST(Type t, SqlValue u, SqlValue p, string i)
            : base(t, i)
        {
            us = u; pw = p;
        }
        protected HttpREST(HttpREST h, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(h,ref cs,ref vs)
        {
            url = h.url.Copy(ref vs);
            mime = h.mime;
            us = h.us.Copy(ref vs);
            pw = h.pw.Copy(ref vs);
            wh = h.wh.Copy(ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new HttpREST(this,ref cs,ref vs);
        }
        /// <summary>
        /// The REST verb
        /// </summary>
        /// <returns>a string</returns>
        internal string Verb()
        {
            switch (type)
            {
                case Type.HttpDelete: return "DELETE";
                case Type.HttpPost: return "POST";
                case Type.HttpPut: return "POST";
                default: return "??";
            }
        }
#if !SILVERLIGHT && !WINDOWS_PHONE
        /// <summary>
        /// Obey the HTTP request
        /// </summary>
        /// <param name="tr">The transaction</param>
        public override void Obey(Context cx)
        {
            if (cx is Transaction tr)
            {
                var a = tr.GetActivation(); // from the top of the stack each time
                a.exec = this;
                var s = url.Eval(tr,tr.GetRowSet())?.ToString();
                if (s == null)
                    return;
#if !EMBEDDED && !LOCAL
            // v5.5 first check the URL is not a Pyrrho server
            // With Pyrrho will be of form scheme:[//[user:password@]host:port][/]db/ro
            var ss = s.Split(':','/','@');
            if (ss.Length > 2)
            {
                var db = "";
                var pw = "";
                var us = PyrrhoStart.user;
                var ho = "localhost";
                var po = 80;
                var rl = "";
                if (s.Contains("//"))
                {
                    if (s.Contains("@"))
                    {
                        us = ss[3]; pw = ss[4]; ho = ss[5]; int.TryParse(ss[6],out po); db = ss[7]; rl = ss[8];
                    }
                    else
                    {
                        ho = ss[3]; int.TryParse(ss[4],out po); db = ss[5]; rl = ss[6];
                    }
                } else if (s!=null && s!="" && s[0]=='/')
                {
                    db = ss[0]; rl = ss[1];
                }
                else
                {
                    db = ss[1]; rl = ss[2];
                }
                var c = tr.ConnectionFor(ho, po, db, us, pw, rl);
                if (c != null)
                {
                    var asy = c.Async as AsyncStream;
                    var lk = asy.GetLock();
                    lock (lk)
                    {
                        lk.OnLock(false, "HttpREST", asy.Conn());
                        asy.Write(Protocol.Rest);
                        asy.PutString(Verb());
                        asy.PutString(s);
                        asy.PutData(a, data.Eval(tr.context));
                        asy.ReadResponse();
                        lk.Unlock(true);
                    }
                    return;
                }
            }
#endif
                // Okay, use HTTP
                var rq = SqlHttp.GetRequest(tr.db, (string)url.Val(tr));
#if !EMBEDDED
                rq.UserAgent = "Pyrrho 5.7 http://" + PyrrhoStart.cfg.hp + "/" + tr.starttime + "/" + tr.db.posAtStart;
#endif
                rq.ContentType = mime ?? "application/tcc+json, application/json";
                rq.Method = Verb();
                if (type != Type.HttpDelete)
                {
                    var rst = new StreamWriter(rq.GetRequestStream());
                    rst.WriteLine(data.Val(tr) as string);
                    rst.Close();
                }
                var wr = rq.GetResponse();
            }
#endif
        }
    }
}