using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
using System.ComponentModel.Design;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Reflection.Emit;
using System.Runtime.ExceptionServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// An LL(1) Parser deals with all Sql statements from various sources.
    /// 
    /// Entries to the Parser from other files are marked internal or public
    /// and can be distinguished by the fact that they create a new Lexer()
    /// 
    /// Most of the grammar below comes from the SQL standard
    /// the comments are extracts from Pyrrho.doc synopsis of syntax
    /// 
    /// Many SQL statements parse to Executables (can be placed in stored procedures etc)
    /// Can then be executed imediately depending on parser settings.
    /// 
    /// Some constructs get parsed during database Load(): these should never try to change the schema
    /// or make other changes. parse should only call _Obey within a transaction.
    /// This means that (for now at least) stored executable code (triggers, procedures) should
    /// never attempt schema changes. 
    /// </summary>
	internal class Parser
    {
        /// <summary>
        /// cx.obs contains DBObjects currently involved in the query (except ad-hoc Domains).
        /// Domains are mostly unknown during query analysis: their "defining position"
        /// will be the lexical position of a DBObject being constructed in the parse.
        /// Any identifiers in the parse will be collected in cx.defs: if unknown their
        /// domain will usually be Domain.Content.
        /// </summary>
        public Context cx; // updatable: the current state of the parse
        /// <summary>
        /// The lexer breaks the input into tokens for us. During parsing
        /// lxr.val is the object corresponding to the current token,
        /// lxr.start and lxr.pos delimit the current token
        /// </summary>
		internal Lexer lxr;
        /// <summary>
        /// The current token
        /// </summary>
		internal Qlx tok;
        public Parser(Database da, Connection con)
        {
            cx = new Context(da, con)
            {
                db = da.Transact(da.nextId, con)
            };
            lxr = new Lexer(cx, "");
        }
        public Parser(Context c)
        {
            cx = c;
            lxr = new Lexer(cx, "");
        }
        /// <summary>
        /// Create a Parser for Constraint definition
        /// </summary>
        /// <param name="_cx"></param>
        /// <param name="src"></param>
        /// <param name="infos"></param>
        public Parser(Context _cx, Ident src)
        {
            cx = _cx.ForConstraintParse();
            cx.parse = ExecuteStatus.Parse;
            lxr = new Lexer(cx, src);
            tok = lxr.tok;
        }
        public Parser(Context _cx, string src)
        {
            cx = _cx.ForConstraintParse();
            lxr = new Lexer(cx, new Ident(src, cx.Ix(Transaction.Analysing, cx.db.nextStmt)));
            tok = lxr.tok;
        }
        /// <summary>
        /// Create a Parser for Constraint definition
        /// </summary>
        /// <param name="rdr"></param>
        /// <param name="scr"></param>
        /// <param name="tb"></param>
        public Parser(Reader rdr, Ident scr)
            : this(rdr.context, scr)
        { }
        internal Iix LexPos()
        {
            var lp = lxr.Position;
            return cx.parse switch
            {
                ExecuteStatus.Graph or
                ExecuteStatus.GraphType or
                ExecuteStatus.Obey => cx.Ix(lp, lp),
                ExecuteStatus.Prepare => new Iix(lp, cx, cx.nextHeap++),
                _ => new Iix(lp, cx, cx.GetUid()),
            };
        }
        internal long LexDp()
        {
            return cx.parse switch
            {
                ExecuteStatus.Graph or
                ExecuteStatus.GraphType or
                ExecuteStatus.Obey => lxr.Position,
                ExecuteStatus.Prepare => cx.nextHeap++,
                _ => cx.GetUid(),
            };
        }
        /// <summary>
        /// Move to the next token
        /// </summary>
        /// <returns></returns>
		internal Qlx Next()
        {
            tok = lxr.Next();
            return tok;
        }
        /// <summary>
        /// Match any of a set of token types
        /// </summary>
        /// <param name="s">the tree of token types</param>
        /// <returns>whether the current token matched any of the set</returns>
		bool Match(params Qlx[] s)
        {
            string a = "";
            if (tok == Qlx.Id)
            {

                a = lxr.val.ToString().ToUpper();
            }
            for (int j = 0; j < s.Length; j++)
            {
                if (tok == s[j])
                    return true;
                if (lxr.doublequoted)
                    return false;
                if (a.CompareTo(s[j].ToString()) == 0)
                {
                    lxr.tok = tok = s[j];
                    return true;
                }
            }
            return false;
        }
        /// <summary>
        /// Raise a syntax error if the current token does not match a given set
        /// </summary>
        /// <param name="t">the tree of token types</param>
        /// <returns>the token that matched</returns>
		internal Qlx Mustbe(params Qlx[] t)
        {
            int j;
            string s = "";
            if (tok == Qlx.Id)
                s = lxr.val.ToString().ToUpper();
            for (j = 0; j < t.Length; j++)
            {
                if (tok == t[j])
                    break;
                if (lxr.doublequoted)
                    continue;
                var a = tok == Qlx.Id;
                var b = s == t[j].ToString();
                if (a && b)
                {
                    tok = t[j];
                    break;
                }
            }
            if (j >= t.Length)
            {
                string str = "";
                for (int k = 0; k < t.Length; k++)
                {
                    if (k > 0)
                        str += ", ";
                    str += t[k].ToString();
                }
                string ctx = (lxr.pos >= lxr.input.Length) ? "EOF" : new string(lxr.input, lxr.start, lxr.pos - lxr.start);
                throw new DBException("42161", str, ctx).Mix();
            }
            Next();
            return t[j];
        }
        /// <summary>
        /// Parse Sql input
        ///     Sql = SqlStatement [‘;’] .
        /// The type of database modification that may occur is determined by db.parse.
        /// </summary>
        /// <param name="sql">the input</param>
        /// <param name="xp">the expected result type (default is Domain.Content)</param>
        /// <returns>The modified Database and the new uid highwatermark </returns>
        public Database ParseSql(string sql, Domain xp)
        {
            if (PyrrhoStart.ShowPlan)
                Console.WriteLine(sql);
            lxr = new Lexer(cx, sql, cx.db.lexeroffset);
            for (; ; )
            {
                tok = lxr.tok;
                ParseStatement(xp);
                for (var b = cx.forReview.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue k)
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is RowSet rs)
                                rs.Apply(new BTree<long, object>(RowSet._Where, new CTree<long, bool>(k.defpos, true)),
                                    cx);
                if (Match(Qlx.SEMICOLON,Qlx.NEXT))
                    Next();
                else break;
            }
            if (tok != Qlx.EOF)
            {
                string ctx = new(lxr.input, lxr.start, lxr.pos - lxr.start);
                throw new DBException("42000", ctx).ISO();
            }
            if (cx.undefined != CTree<long, int>.Empty)
                throw new DBException("42112", cx.obs[cx.undefined.First()?.key() ?? -1L]?.mem[ObInfo.Name] ?? "?");
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return cx.db;
        }
        public Database ParseQl(string ql)
        {
            if (PyrrhoStart.ShowPlan)
                Console.WriteLine(ql);
            lxr = new Lexer(cx, ql, cx.db.lexeroffset);
            tok = lxr.tok;
            do
            {
                ParseStatement(Domain.TableType);
                //      cx.result = e.defpos;
                if (tok == Qlx.SEMICOLON)
                    Next();
            } while (tok != Qlx.EOF);
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + ql.Length);
            return cx.db;
        }
        /// <summary>
        /// Add the parameters to a prepared statement
        /// </summary>
        /// <param name="pre">The object with placeholders</param>
        /// <param name="s">The parameter strings concatenated by |</param>
        /// <returns>The modified database and the new uid highwatermark</returns>
        public Database ParseSql(PreparedStatement pre, string s)
        {
            cx.Add(pre);
            cx.Add(pre.framing);
            lxr = new Lexer(cx, s, cx.db.lexeroffset, true);
            tok = lxr.tok;
            var b = cx.FixLl(pre.qMarks).First();
            for (; b != null && tok != Qlx.EOF; b = b.Next())
                if (b.value() is long p)
                {
                    var v = lxr.val;
                    var lp = LexPos();
                    if (Match(Qlx.DATE, Qlx.TIME, Qlx.TIMESTAMP, Qlx.INTERVAL))
                    {
                        Qlx tk = tok;
                        Next();
                        v = lxr.val;
                        if (tok == Qlx.CHARLITERAL)
                        {
                            Next();
                            v = new SqlDateTimeLiteral(lp.dp, cx,
                                new Domain(lp.dp, tk, BTree<long, object>.Empty), v.ToString()).Eval(cx);
                        }
                    }
                    else
                        Mustbe(Qlx.BLOBLITERAL, Qlx.NUMERICLITERAL, Qlx.REALLITERAL,
                            // Qlx.DOCUMENTLITERAL,
                            Qlx.CHARLITERAL, Qlx.INTEGERLITERAL);
                    cx.values += (p, v);
                    Mustbe(Qlx.SEMICOLON);
                }
            if (!(b == null && tok == Qlx.EOF))
                throw new DBException("33001");
            cx.QParams(); // parm SqlLiterals that are QParams with actuals
            cx = pre.target?._Obey(cx) ?? cx;
            return cx.db;
        }
        /// <summary>  
        /// </summary>
        /// <param name="xp">A Domain or ObInfo for the expected result of the Executable</param>
        /// <param name="procbody">If true, we are parsing a routine body (SQl-style RETURN)</param>
        /// <returns>The Executable results of the Parse</returns>
        public Executable ParseStatement(Domain xp,bool procbody = false)
        {
            var lp = lxr.start;
            Match(Qlx.ALTER, Qlx.AT, Qlx.BEGIN, Qlx.BINDING, Qlx.BREAK, Qlx.CALL, Qlx.CASE, Qlx.CREATE,
                Qlx.CLOSE, Qlx.DECLARE, Qlx.FETCH, Qlx.GET, Qlx.GRANT,
                Qlx.GRAPH, Qlx.ITERATE, Qlx.LEAVE, Qlx.LOOP, Qlx.OPEN, Qlx.PROPERTY, 
                Qlx.REPEAT, Qlx.RESIGNAL, Qlx.REVOKE, Qlx.SIGNAL, Qlx.TABLE,
                Qlx.UPDATE, Qlx.VALUES, Qlx.WHILE); // watch for these non-reserved words
            switch (tok)
            {
                case Qlx.ALTER: ParseAlter(); return Executable.None;
                case Qlx.AT:
                    {
                        Next();
                        var sn = new Ident(this);
                        Mustbe(Qlx.Id);
                        var sc = cx.db.objects[(sn.ident == "HOME_SCHEMA")?cx.role.home_schema
                            : cx.role.schemas[sn.ident] ?? -1L] as Schema
                            ?? throw new DBException("42107", sn.ident);
                        var sa = ParseStatement(xp,procbody);
                        return (Executable)cx.Add(sa + (Executable.Schema, sc));
                    }
                case Qlx.BEGIN: return ParseCompoundStatement(xp, "");
                case Qlx.BINDING:
                    {
                        Next();
                        ParseBindingVariableDefinitions();
                        return ParseStatement(xp,procbody);
                    }
                case Qlx.BREAK: return ParseBreakLeave();
                case Qlx.CALL: ParseCallStatement(); return Executable.None;  // some GQL TBD
                case Qlx.CASE: return ParseCaseStatement(xp);
                case Qlx.CLOSE: return ParseCloseStatement();
                case Qlx.COMMIT:
                    Next();
                    if (Match(Qlx.WORK))
                        Next();
                    if (cx.parse == ExecuteStatus.Obey)
                        cx.db.Commit(cx);
                    else
                        throw new DBException("2D000", "Commit");
                    break;
                case Qlx.CREATE: return ParseCreateClause(); // some GQL TBD
                case Qlx.DECLARE: return ParseDeclaration();  // might be for a handler
                case Qlx.DELETE: return ParseSqlDelete();
                case Qlx.DROP: ParseDropStatement(); return Executable.None; // some GQL TBD
                case Qlx.EOF: return new Executable(LexPos().lp);
                case Qlx.FETCH: return ParseFetchStatement();
                case Qlx.FILTER: return ParseFilter();
                case Qlx.FOR: return ParseForStatement(xp, null);
                case Qlx.GET: return ParseGetDiagnosticsStatement();
                case Qlx.GRANT: return ParseGrant();
                case Qlx.GRAPH: // TBD
                case Qlx.Id: return ParseLabelledStatement(xp);
                case Qlx.IF: return ParseIfThenElse(xp);
                case Qlx.INSERT: return ParseSqlInsert();
                case Qlx.ITERATE: return ParseIterate();
                case Qlx.LBRACE: goto case Qlx.BEGIN;
                case Qlx.LEAVE: return ParseBreakLeave();
                case Qlx.LET: return ParseLet();
                case Qlx.LIMIT: goto case Qlx.ORDER;
                case Qlx.LOOP: return ParseLoopStatement(xp, null);
                case Qlx.MATCH: return ParseMatchStatement();
                case Qlx.OFFSET: goto case Qlx.ORDER;
                case Qlx.OPEN: return ParseOpenStatement();
                case Qlx.OPTIONAL: // TBD
                    Next();
                    if (tok == Qlx.CALL) goto case Qlx.CALL;
                    if (tok == Qlx.MATCH) goto case Qlx.MATCH;
                    break;
                case Qlx.ORDER: return ParseOrderAndPage();
                case Qlx.PROPERTY:
                    Next();
                    if (tok == Qlx.GRAPH) goto case Qlx.GRAPH;
                    break;
                case Qlx.REPEAT: return ParseRepeat(xp, null);
                case Qlx.REMOVE: goto case Qlx.DELETE; // some GQL TBD
                case Qlx.RESIGNAL: return ParseSignal();
                case Qlx.RETURN:
                    if (procbody)
                        return ParseReturn(xp); // some GQL TBD
                    return ParseCursorSpecification(xp,true);
                case Qlx.REVOKE: return ParseRevoke();
                case Qlx.ROLLBACK:
                    Next();
                    if (Match(Qlx.WORK))
                        Next();
                    var e = new RollbackStatement(LexDp());
                    cx.exec = e;
                    if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction)
                        cx = new Context(cx.db.Rollback(), cx.conn);
                    else
                        cx.Add(e);
                    cx.exec = e;
                    return e;
                case Qlx.SELECT: return ParseCursorSpecification(xp); // single TBD
                                                                       //  return ParseSelectSingle(new Ident(this), xp);
                case Qlx.SET: return ParseAssignment();  // some GQL TBD
                                                     //  return ParseAssignment();
                case Qlx.SIGNAL: return ParseSignal();
                case Qlx.SKIP: goto case Qlx.ORDER;
                //GQL case Qlx.START
                case Qlx.TABLE: return ParseCursorSpecification(xp); // some GQL TBD
                case Qlx.UPDATE: (cx, var ue) = ParseSqlUpdate(); return ue;
                case Qlx.USE:
                    {
                        Next();
                        var sn = new Ident(this);
                        Mustbe(Qlx.Id);
                        var gg = cx.db.objects[(sn.ident == "HOME_GRAPH") ? cx.role.home_graph
                            : cx.role.graphs[sn.ident] ?? -1L] as Graph
                            ?? throw new DBException("42107", sn.ident);
                        var sa = ParseStatement(xp,procbody);
                        return (Executable)cx.Add(sa + (Executable.UseGraph, gg));
                    }
                case Qlx.VALUE: goto case Qlx.BINDING;
                case Qlx.VALUES: return ParseCursorSpecification(xp);
                case Qlx.WHILE: return ParseSqlWhile(xp, null);
                case Qlx.WITH: goto case Qlx.MATCH; // wow
            }
            throw new DBException("42000");
        }
        byte MustBeLevelByte()
        {
            byte lv;
            if (tok == Qlx.Id && lxr.val is TChar tc && tc.value.Length == 1)
                lv = (byte)('D' - tc.value[0]);
            else
                throw new DBException("4211A", tok.ToString());
            Next();
            return lv;
        }
        Level MustBeLevel()
        {
            Mustbe(Qlx.LEVEL);
            var min = MustBeLevelByte();
            var max = min;
            if (tok == Qlx.MINUS)
                max = MustBeLevelByte();
            var gps = BTree<string, bool>.Empty;
            if (Match(Qlx.GROUPS))
            {
                Next();
                while (tok == Qlx.Id && lxr.val is not null)
                {
                    gps += (lxr.val.ToString(), true);
                    Next();
                }
            }
            var rfs = BTree<string, bool>.Empty;
            if (Match(Qlx.REFERENCES))
            {
                Next();
                while (tok == Qlx.Id && lxr.val is not null)
                {
                    rfs += (lxr.val.ToString(), true);
                    Next();
                }
            }
            return new Level(min, max, gps, rfs);
        }
        void YieldClause()
        {
            Next();
            for (; ; )
            {
                if (Match(Qlx.Id))
                    Next();
                if (Match(Qlx.AS))
                {
                    Next();
                    Mustbe(Qlx.Id);
                }
                if (tok == Qlx.COMMA)
                    Next();
                else
                    break;
            }
        }
        /// <summary>
		/// Grant  = 	GRANT Privileges TO GranteeList [ WITH GRANT OPTION ] 
		/// |	GRANT Role_id { ',' Role_id } TO GranteeList [ WITH ADMIN OPTION ] 
        /// |   GRANT SECURITY Level TO User_id .
        /// </summary>
        Executable ParseGrant()
        {
            var lp = lxr.start;
            Next();
            if (Match(Qlx.SECURITY))
            {
                Next();
                var lv = MustBeLevel();
                Mustbe(Qlx.TO);
                var nm = lxr.val?.ToString() ?? throw new DBException("42135");
                Mustbe(Qlx.Id);
                var usr = cx.db.objects[cx.db.roles[nm] ?? -1L] as User
                    ?? throw new DBException("42135", nm.ToString());
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    cx.Add(new Clearance(usr.defpos, lv, tr.nextPos));
            }
            else if (Match(Qlx.PASSWORD))
            {
                TypedValue pwd = new TChar("");
                Role? irole = null;
                Next();
                if (!Match(Qlx.FOR) && !Match(Qlx.TO))
                {
                    pwd = lxr.val;
                    Next();
                }
                if (Match(Qlx.FOR))
                {
                    Next();
                    var rid = new Ident(this);
                    Mustbe(Qlx.Id);
                    irole = cx.GetObject(rid.ident) as Role ??
                        throw new DBException("42135", rid);
                }
                Mustbe(Qlx.TO);
                var grantees = ParseGranteeList(BList<PrivNames>.Empty);
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                {
                    var irolepos = -1L;
                    if (irole != null && irole.name is not null)
                    {
                        tr.AccessRole(cx, true, new CList<string>(irole.name), grantees, false);
                        irolepos = irole.defpos;
                    }
                    for (var b = grantees.First(); b is not null; b = b.Next())
                        if (b.value() is DBObject us)
                            cx.Add(new Authenticate(us.defpos, pwd.ToString(), irolepos, tr.nextPos));
                }
            }
            Match(Qlx.OWNER, Qlx.USAGE);
            if (Match(Qlx.ALL, Qlx.SELECT, Qlx.INSERT, Qlx.DELETE, Qlx.UPDATE, Qlx.REFERENCES, 
                Qlx.OWNER, Qlx.TRIGGER, Qlx.USAGE, Qlx.EXECUTE, Qlx.GRAPH, Qlx.SCHEMA))
            {
                var priv = ParsePrivileges();
                if (Match(Qlx.ON))
                    Next();
                var (ob, _) = ParseObjectName();
                long pob = ob?.defpos ?? 0;
                Mustbe(Qlx.TO);
                var grantees = ParseGranteeList(priv);
                bool opt = ParseGrantOption();
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    tr.AccessObject(cx, true, priv, pob, grantees, opt);
            }
            else
            {
                var roles = ParseRoleNameList();
                Mustbe(Qlx.TO);
                var grantees = ParseGranteeList(new BList<PrivNames>(new PrivNames(Qlx.USAGE)));
                bool opt = ParseAdminOption();
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    tr.AccessRole(cx, true, roles, grantees, opt);
            }
            return new Executable(lp);
        }
        /// <summary>
        /// ObjectName is used in GRANT and ALTER ROLE
		/// ObjectName = 	TABLE id
		/// 	|	DOMAIN id
		///     |	TYPE id
		///     |	Routine
		///     |	VIEW id 
        ///     |   ENTITY id .
        /// </summary>
        /// <param name="db">the connected database affected</param>
        /// <returns>the object that has been specified</returns>
		(DBObject, string) ParseObjectName()
        {
            Qlx kind = Qlx.TABLE;
            Match(Qlx.INSTANCE, Qlx.CONSTRUCTOR, Qlx.OVERRIDING, Qlx.VIEW, Qlx.TYPE,
                Qlx.PROCEDURE,Qlx.FUNCTION,Qlx.METHOD);
            if (tok != Qlx.Id || lxr.val.ToString() == "INDEX")
            {
                kind = tok;
                Next();
                if (kind == Qlx.INSTANCE || kind == Qlx.STATIC || kind == Qlx.OVERRIDING || kind == Qlx.CONSTRUCTOR)
                    Mustbe(Qlx.METHOD);
            }
            var n = lxr.val.ToString() ?? "?";
            Mustbe(Qlx.Id);
            DBObject? ob = null;
            switch (kind)
            {
                case Qlx.TABLE:
                case Qlx.DOMAIN:
                case Qlx.VIEW:
                case Qlx.ENTITY:
                case Qlx.TYPE:
                    ob = cx.GetObject(n) ??
                        throw new DBException("42135", n);
                    break;
                case Qlx.CONSTRUCTOR:
                case Qlx.FUNCTION:
                case Qlx.PROCEDURE:
                    {
                        var a = ParseSignature();
                        ob = cx.GetProcedure(LexPos().dp, n, a) ??
                            throw new DBException("42108", n);
                        break;
                    }
                case Qlx.INSTANCE:
                case Qlx.STATIC:
                case Qlx.OVERRIDING:
                case Qlx.METHOD:
                    {
                        var a = ParseSignature();
                        Mustbe(Qlx.FOR);
                        var tp = lxr.val.ToString();
                        Mustbe(Qlx.Id);
                        var oi = ((cx.role.dbobjects[tp] is long p) ? cx._Ob(p) : null)?.infos[cx.role.defpos] ??
                            throw new DBException("42119", tp);
                        ob = (DBObject?)oi.mem[oi.methodInfos[n]?[a] ?? -1L] ??
                            throw new DBException("42108", n);
                        break;
                    }
                case Qlx.TRIGGER:
                    {
                        Mustbe(Qlx.ON);
                        var tn = lxr.val.ToString();
                        Mustbe(Qlx.Id);
                        var tb = cx.GetObject(tn) as Table ?? throw new DBException("42135", tn);
                        for (var b = tb.triggers.First(); ob == null && b != null; b = b.Next())
                            for (var c = b.value().First(); ob == null && c != null; c = c.Next())
                                if (cx._Ob(c.key()) is Trigger tg && tg.name == n)
                                    ob = tg;
                        if (ob == null)
                            throw new DBException("42107", n);
                        break;
                    }
                case Qlx.DATABASE:
                    ob = SqlNull.Value;
                    break;
                default:
                    throw new DBException("42115", kind).Mix();
            }
            if (ob == null) throw new PEException("00083");
            return (cx.Add(ob), n);
        }
        /// <summary>
        /// used in ObjectName. 
        /// '('Type, {',' Type }')'
        /// </summary>
        /// <returns>the number of parameters</returns>
		CList<Domain> ParseSignature()
        {
            CList<Domain> fs = CList<Domain>.Empty;
            if (tok == Qlx.LPAREN)
            {
                Next();
                if (tok == Qlx.RPAREN)
                {
                    Next();
                    return CList<Domain>.Empty;
                }
                fs += ParseDataType();
                while (tok == Qlx.COMMA)
                {
                    Next();
                    fs += ParseDataType();
                }
                Mustbe(Qlx.RPAREN);
            }
            return fs;
        }
        /// <summary>
		/// ObjectPrivileges = ALL PRIVILEGES | Action { ',' Action } .
        /// </summary>
        /// <returns>The tree of privileges</returns>
		BList<PrivNames> ParsePrivileges()
        {
            var r = BList<PrivNames>.Empty;
            if (tok == Qlx.ALL)
            {
                Next();
                Mustbe(Qlx.PRIVILEGES);
                return r;
            }
            r += ParsePrivilege();
            while (tok == Qlx.COMMA)
            {
                Next();
                r += ParsePrivilege();
            }
            return r;
        }
        /// <summary>
		/// Action = 	SELECT [ '(' id { ',' id } ')' ]
		/// 	|	DELETE
		/// 	|	INSERT  [ '(' id { ',' id } ')' ]
		/// 	|	UPDATE  [ '(' id { ',' id } ')' ]
		/// 	|	REFERENCES  [ '(' id { ',' id } ')' ]
		/// 	|	USAGE
        /// 	|   TRIGGER
		/// 	|	EXECUTE 
        /// 	|   OWNER .
        /// </summary>
        /// <returns>A singleton privilege (tree of one item)</returns>
		PrivNames ParsePrivilege()
        {
            Match(Qlx.UPDATE,Qlx.REFERENCES, Qlx.USAGE, Qlx.TRIGGER, Qlx.EXECUTE, Qlx.OWNER);
            var r = new PrivNames(tok);
            Mustbe(Qlx.SELECT, Qlx.DELETE, Qlx.INSERT, Qlx.UPDATE,
                Qlx.REFERENCES, Qlx.USAGE, Qlx.TRIGGER, Qlx.EXECUTE, Qlx.OWNER);
            if ((r.priv == Qlx.UPDATE || r.priv == Qlx.REFERENCES || r.priv == Qlx.SELECT || r.priv == Qlx.INSERT) && tok == Qlx.LPAREN)
            {
                Next();
                r.cols += (lxr.val.ToString(), true);
                Mustbe(Qlx.Id);
                while (tok == Qlx.COMMA)
                {
                    Next();
                    r.cols += (lxr.val.ToString(), true);
                    Mustbe(Qlx.Id);
                }
                Mustbe(Qlx.RPAREN);
            }
            return r;
        }
        /// <summary>
		/// GranteeList = PUBLIC | Grantee { ',' Grantee } .
        /// </summary>
        /// <param name="priv">the tree of privieges to grant</param>
        /// <returns>the updated database objects</returns>
		BList<DBObject> ParseGranteeList(BList<PrivNames> priv)
        {
            var r = new BList<DBObject>(ParseGrantee(priv));
            while (tok == Qlx.COMMA)
            {
                Next();
                r += ParseGrantee(priv);
            }
            return r;
        }
        /// <summary>
        /// helper for non-reserved words
        /// </summary>
        /// <returns>if we match a method mode</returns>
        bool MethodModes()
        {
            return Match(Qlx.INSTANCE, Qlx.OVERRIDING, Qlx.CONSTRUCTOR);
        }
        internal Role? GetRole(string n)
        {
            return (Role?)cx.db.objects[cx.db.roles[n] ?? -1L];
        }
        /// <summary>
		/// Grantee = 	[USER] id
		/// 	|	ROLE id . 
        /// </summary>
        /// <param name="priv">the tree of privileges</param>
        /// <returns>the updated grantee</returns>
		DBObject ParseGrantee(BList<PrivNames> priv)
        {
            Qlx kind = Qlx.USER;
            if (Match(Qlx.PUBLIC))
            {
                Next();
                return (Role?)cx.db.objects[Database.Guest] ?? throw new PEException("PE2400");
            }
            if (Match(Qlx.USER))
                Next();
            else if (Match(Qlx.ROLE))
            {
                kind = Qlx.ROLE;
                Next();
            }
            var n = lxr.val.ToString();
            Mustbe(Qlx.Id);
            DBObject? ob;
            switch (kind)
            {
                case Qlx.USER:
                    {
                        ob = GetRole(n);
                        if ((ob == null || ob.defpos == -1) && cx.db is Transaction tr)
                            ob = cx.Add(new PUser(n, tr.nextPos, cx));
                        break;
                    }
                case Qlx.ROLE:
                    {
                        ob = GetRole(n) ?? throw new DBException("28102", n);
                        if (ob.defpos >= 0)
                        { // if not PUBLIC we need to have privilege to change the grantee role
                            var ri = ob.infos[cx.role.defpos];
                            if (ri == null || !ri.priv.HasFlag(Role.admin))
                                throw new DBException("42105").Add(Qlx.ADMIN);
                        }
                    }
                    break;
                default: throw new DBException("28101").Mix();
            }
            if (ob == SqlNull.Value && (priv == null || priv.Length != 1 || priv[0]?.priv != Qlx.OWNER))
                throw new DBException("28102", kind, n).Mix();
            if (ob == null)
                throw new PEException("PE2401");
            return cx.Add(ob);
        }
        /// <summary>
        /// [ WITH GRANT OPTION ] 
        /// </summary>
        /// <returns>whether WITH GRANT OPTION was specified</returns>
		bool ParseGrantOption()
        {
            if (tok == Qlx.WITH)
            {
                Next();
                Mustbe(Qlx.GRANT);
                Mustbe(Qlx.OPTION);
                return true;
            }
            return false;
        }
        /// <summary>
        /// [ WITH ADMIN OPTION ] 
        /// </summary>
        /// <returns>whether WITH ADMIN OPTION was specified</returns>
		bool ParseAdminOption()
        {
            if (tok == Qlx.WITH)
            {
                Next();
                Mustbe(Qlx.ADMIN);
                Mustbe(Qlx.OPTION);
                return true;
            }
            return false;
        }
        /// <summary>
        /// Role_id { ',' Role_id }
        /// </summary>
        /// <returns>The tree of Roles</returns>
		CList<string> ParseRoleNameList()
        {
            var r = CList<string>.Empty;
            if (tok == Qlx.Id)
            {
                r += lxr.val.ToString();
                Next();
                while (tok == Qlx.COMMA)
                {
                    Next();
                    r += lxr.val.ToString();
                    Mustbe(Qlx.Id);
                }
            }
            return r;
        }
        /// <summary>
		/// Revoke = 	REVOKE [GRANT OPTION FOR] Privileges FROM GranteeList
		/// 	|	REVOKE [ADMIN OPTION FOR] Role_id { ',' Role_id } FROM GranteeList .
        /// Privileges = ObjectPrivileges ON ObjectName .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseRevoke()
        {
            var lp = lxr.start;
            Next();
            Qlx opt = ParseRevokeOption();
            if (tok == Qlx.Id)
            {
                var priv = ParseRoleNameList();
                Mustbe(Qlx.FROM);
                var grantees = ParseGranteeList(BList<PrivNames>.Empty);
                if (opt == Qlx.GRANT)
                    throw new DBException("42116").Mix();
                if (cx.db is Transaction tr)
                    tr.AccessRole(cx, false, priv, grantees, opt == Qlx.ADMIN);
            }
            else
            {
                if (opt == Qlx.ADMIN)
                    throw new DBException("42117").Mix();
                var priv = ParsePrivileges();
                Mustbe(Qlx.ON);
                var (ob, _) = ParseObjectName();
                Mustbe(Qlx.FROM);
                var grantees = ParseGranteeList(priv);
                if (cx.db is Transaction tr)
                    tr.AccessObject(cx, false, priv, ob.defpos, grantees, (opt == Qlx.GRANT));
            }
            return new Executable(lp);
        }
        /// <summary>
        /// [GRANT OPTION FOR] | [ADMIN OPTION FOR]
        /// </summary>
        /// <returns>GRANT or ADMIN or NONE</returns>
		Qlx ParseRevokeOption()
        {
            Qlx r = Qlx.NONE;
            if (Match(Qlx.GRANT, Qlx.ADMIN))
            {
                r = tok;
                Next();
                Mustbe(Qlx.OPTION);
                Mustbe(Qlx.FOR);
            }
            return r;
        }
        /// <summary>
		/// Call = 		CALL Procedure_id '(' [  TypedValue { ','  TypedValue } ] ')' 
		/// 	|	MethodCall .
        /// </summary>
        /// <param name="ob">The target object of the method call if any</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseCallStatement()
        {
            var lp = lxr.start;
            Next();
            Executable e = ParseProcedureCall();
            if (cx.parse != ExecuteStatus.Parse && cx.db is Transaction tr)
                cx = tr.Execute(e, cx);
            cx.Add(e);
            return e;
        }
        /// <summary>
		/// Create =	CREATE ROLE id [string]
		/// |	CREATE DOMAIN id [AS] DomainDefinition {Metadata}
		/// |	CREATE FUNCTION id '(' Parameters ')' RETURNS Type Body 
        /// | CREATE [OR REPLACE] [PROPERTY] GRAPH [IF NOT EXISTS] GraphDetails 
        /// | CREATE [OR REPLACE] [PROPERTY] GRAPH TYPE [IF NOT EXISTS] GraphTypeDetails
        /// |   CREATE ORDERING FOR id Order
		/// |	CREATE PROCEDURE id '(' Parameters ')' Body
		/// |	CREATE Method Body
        /// | CREATE SCHEMA [IF NOT EXISTS] Path_Value id
		/// |	CREATE TABLE id TableContents [UriType] {Metadata}
		/// |	CREATE TRIGGER id (BEFORE|AFTER) Event ON id [ RefObj ] Trigger
		/// |	CREATE TYPE id [UNDER id] AS Representation [ Method {',' Method} ] {Metadata}
		/// |	CREATE ViewDefinition 
        /// |   CREATE XMLNAMESPACES NamespaceList
        /// |   CREATE (Node) {-[Edge]->(Node)|<-[Edge]-(Node)}
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        Executable ParseCreateClause(bool parm = false)
        {
            var lp = lxr.start;
            if (cx.role is Role dr
                && dr.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole) != true
                && dr.defpos != -502)
                throw new DBException("42105").Add(Qlx.ROLE);
            Next();
            MethodModes();
            if (Match(Qlx.ORDERING))
                ParseCreateOrdering();
            else if (Match(Qlx.PROCEDURE,Qlx.FUNCTION))
            {
                bool func = tok == Qlx.FUNCTION;
                Next();
                ParseProcedureClause(func, Qlx.CREATE);
            }
            else if (Match(Qlx.OVERRIDING, Qlx.INSTANCE, Qlx.STATIC, Qlx.CONSTRUCTOR, Qlx.METHOD))
                ParseMethodDefinition();
            else if (Match(Qlx.TABLE,Qlx.TEMPORARY))
            {
                if (tok == Qlx.TEMPORARY)
                {
                    Role au = GetRole("Temp") ?? throw new DBException("3D001", "Temp").Mix();
                    cx = new Context(cx, au, cx.db.user ?? throw new DBException("42105").Add(Qlx.TEMPORARY));
                    Next();
                }
                Mustbe(Qlx.TABLE);
                ParseCreateTable();
            }
            else if (Match(Qlx.TRIGGER))
            {
                Next();
                ParseTriggerDefClause();
            }
            else if (Match(Qlx.DOMAIN))
            {
                Next();
                ParseDomainDefinition();
            }
            else if (Match(Qlx.TYPE))
            {
                Next();
                ParseTypeClause();
            }
            else if (Match(Qlx.ROLE))
            {
                Next();
                var id = lxr.val.ToString();
                Mustbe(Qlx.Id);
                TypedValue o = new TChar("");
                if (Match(Qlx.CHARLITERAL))
                {
                    o = lxr.val;
                    Next();
                }
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    cx.Add(new PRole(id, o.ToString(), tr.nextPos, cx));
            }
            else if (Match(Qlx.VIEW))
                ParseViewDefinition();
            else if (tok == Qlx.OR)
            {
                Next();
                Mustbe(Qlx.REPLACE);
                return ParseCreateClause(true);
            }
            else if (Match(Qlx.PROPERTY))
                return ParseCreateClause(parm);
            else if (Match(Qlx.GRAPH))
            {
                Next();
                if (Match(Qlx.TYPE))
                {
                    Next();
                    ParseCreateGraphType(parm);
                }
                else
                    ParseCreateGraph(parm);
            }
            else if (Match(Qlx.IF))
            {
                Next();
                Mustbe(Qlx.NOT);
                Mustbe(Qlx.EXISTS);
                ParseCreateClause(true);
            }
            else if (tok == Qlx.SCHEMA)
            {
                Next();
                ParseCreateSchema(parm);
            }
            else if (tok == Qlx.LPAREN)
                return ParseInsertGraph();
            return new Executable(lp);
        }

        static (string,string) Schema(string iri)
        {
            var ix = iri.LastIndexOf('/');
            if (ix < 0)
                return (".", iri);
            return (iri[..ix],iri[(ix + 1)..]);
        }
        void ParseCreateSchema(bool ifnotexists = false)
        {
            var cr = ParseCatalogReference();
            if (cr is null || cr == "")
                throw new DBException("42000").Add(Qlx.CREATE_SCHEMA_STATEMENT,new TChar(cr??"??"));
            if (cx.role.schemas.Contains(cr))
            {
                if (!ifnotexists)
                    throw new DBException("42104",cr.ToString()).Add(Qlx.CREATE_SCHEMA_STATEMENT, new TChar(cr ?? "??"));
            } else  if (cx.parse == ExecuteStatus.Obey)
                cx.Add(new PSchema(cx.db.nextPos, cr));
        }
        void ParseCreateGraph(bool replace = false)
        {
            bool ifnotexists = tok == Qlx.IF;
            if (ifnotexists) { Next(); Mustbe(Qlx.NOT); Mustbe(Qlx.EXISTS); }
            var cr = ParseCatalogReference();
            var (sd,nm) = Schema(cr);
            if (((sd==".")?Level5.Schema.Empty:cx._Ob(cx.role.schemas[sd] ?? -1L)) is not Schema sc 
                || nm is null || nm == "")
                throw new DBException("42000", nm ?? "??").Add(Qlx.CREATE_GRAPH_STATEMENT, new TChar(nm ?? "??"));
            if (cx.role.graphs.Contains(cr.ToString()))
            {
                if (ifnotexists)
                    return;
                if (!replace)
                    throw new DBException("42104",cr.ToString());
            }
            var oi = sc.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.SCHEMA);
            var gr = cx._Ob(oi.names[nm].Item2 ?? -1L) as Graph;
            if (gr is not null)
            {
                if (ifnotexists)
                    return;
                if (!replace)
                    throw new DBException("42104", cr.ToString());
            }
            var ts = CTree<long, bool>.Empty;
            var ns = CTree<long, TNode>.Empty;
            if (Match(Qlx.DOUBLECOLON, Qlx.TYPED))
                Next();
            if (tok==Qlx.ANY)
            {
                Next();
                if (Match(Qlx.PROPERTY)) Next();
                if (Match(Qlx.GRAPH)) Next();
            }
            else if (tok == Qlx.LIKE)
            {
                var op = cx.parse;
                cx.parse = ExecuteStatus.GraphType;
                var cs = (GraphInsertStatement)ParseInsertGraph();
                cs._Obey(cx);
                ts = cs.GraphTypes(cx);
                cx.parse = op;
            } 
            else if (tok==Qlx.Id)
            {
                var id = new Ident(this);
                Mustbe(Qlx.Id);
                if (cx._Ob(cx.role.graphs[id.ident] ?? -1L) is not Graph g)
                    throw new DBException("42107", id.ident);
                ts = g.graphTypes;
                for (var b = g.nodes.First(); b != null; b = b.Next())
                    if (b.value() is TNode n)
                        ns += (n.defpos, n);
            } else
            {
                if (Match(Qlx.PROPERTY))
                    Next();
                if (Match(Qlx.GRAPH))
                    Next();
                Mustbe(Qlx.LBRACE);
                ts += (ParseElementTypeSpec(ts),true);
                while (tok == Qlx.COMMA)
                {
                    Next();
                    ts += (ParseElementTypeSpec(ts), true);
                }
                Mustbe(Qlx.RBRACE);
            }
            if (tok == Qlx.AS)
            {
                Next(); Mustbe(Qlx.COPY); Next(); Mustbe(Qlx.OF);
                var cs = ParseInsertGraph();
                cs._Obey(cx);
            }
            if (cx.parse == ExecuteStatus.Obey)
                cx.Add(new PGraph(cx.db.nextPos, cr,ts,ns));
        }
        void ParseCreateGraphType(bool replace = false)
        {
            bool ifnotexists = tok == Qlx.IF;
            if (ifnotexists) { Next(); Mustbe(Qlx.NOT); Mustbe(Qlx.EXISTS); }
            var cr = ParseCatalogReference();
            var (pr, nm) = Schema(cr);
            if (cx._Ob(cx.role.schemas[pr]??-1L) is not Schema sc || nm is null || nm == "")
                throw new DBException("42107","Schema");
            var oi = sc.infos[cx.role.defpos]??throw new DBException("42105").Add(Qlx.SCHEMA);
            var gt = cx._Ob(oi.names[nm].Item2??-1L) as GraphType;
            var ts = CTree<long, bool>.Empty;
            if (gt is not null)
            {
                if (ifnotexists)
                    return;
                if (!replace)
                    throw new DBException("42104", cr.ToString());
            }
            if (tok == Qlx.AS)
                Next();
            if (tok == Qlx.COPY)
            {
                Next(); Mustbe(Qlx.OF);
                var id = new Ident(this);
                var og = cx._Ob(cx.db.catalog[id.ident] ?? -1L) as GraphType
                    ?? throw new DBException("42107", id.ident);
                ts = og.constraints;
            }
            else if (tok == Qlx.LIKE)
            {
                var op = cx.parse;
                cx.parse = ExecuteStatus.GraphType;
                var cs = (GraphInsertStatement)ParseInsertGraph();
                cs._Obey(cx);
                ts = cs.GraphTypes(cx);
                cx.parse = op;
            }
            else 
                ts = ParseNestedGraphTypeSpecification(ts);
            if (cx.parse == ExecuteStatus.Obey)
                cx.Add(new PGraphType(cx.db.nextPos, cr.ToString(), ts));
        }
        CTree<long,bool> ParseNestedGraphTypeSpecification(CTree<long,bool> ts)
        {
            Mustbe(Qlx.LBRACE);
            ts += (ParseElementTypeSpec(ts), true);
            while (tok == Qlx.COMMA)
            {
                Next();
                ts += (ParseElementTypeSpec(ts), true);
            }
            Mustbe(Qlx.RBRACE);
            return ts;
        }
        CTree<Qlx, bool> SpecWord(CTree<Qlx,bool> wds, params Qlx[] tks)
        {
            foreach (var b in tks)
                if (Match(b))
                {
                    wds += (b, true);
                    Next();
                    break;
                }
            return wds;
        }
        /// <summary>
        /// ElementTypeSpecification = 
        ///   [(NODE|VERTEX) [TYPE] id] * ElementDetails
        /// | [(DIRECTED | UNDIRECTED) (EDGE|RELATIONSHIP) [TYPE] id] * ElementDetails £ ArcType ElementDetails
        /// | [(DIRECTED | UNDIRECTED) (EDGE|RELATIONSHIP) [TYPE] id] * ElementDetails  £ EndPointPhrase
        /// ArcType =
        ///   ARROWBASE ElementDetails ARROW 
        /// | RARROW ElementDetails RARROWBASE
        /// | ARROWBASETILDE ElementDetails RBRACKTILDE
        /// | ARROWTILDE ElementDetails RARROWTILDE  (Editors note 35)
        /// EndpointPhrase = CONNECTING LPAREN id (RARROW|TILDE|ARROW|TO) id RPAREN
        /// At * we must have one of LABEL, LABELS, CONNECTING, COLON, DOUBLEARROW, IMPLIES, LPAREN, LBRACE, AS, COMMA, RBRACE
        /// At £ we must have one of CONNECTING, ARROWBASE, RARROW, ARROWBASETILDE, AS, COMMA, RBRACE
        /// At the end we must have COMMA or RBRACE and a node or edge type.
        /// There are many syntax rules in 18.2 and 18.3 which limit the optionality of these tokens.
        /// </summary>
        /// <param name="ts"></param>
        /// <returns></returns>
        /// <exception cref="DBException"></exception>
        long ParseElementTypeSpec(CTree<long,bool> ts)
        {
            var wds = CTree<Qlx, bool>.Empty;
            var lp = lxr.Position;
            var lv = lxr.val;
            wds = SpecWord(wds, Qlx.NODE, Qlx.VERTEX);
            wds = SpecWord(wds, Qlx.DIRECTED, Qlx.UNDIRECTED);
            wds = SpecWord(wds, Qlx.EDGE, Qlx.RELATIONSHIP);
            wds = SpecWord(wds, Qlx.TYPE);
            var id = new Ident("", new Iix(lp));
            var al = id;
            if (tok == Qlx.Id)
            {
                id = new Ident(this); // <node type name>
                cx.defs += (id, 0);
                Next();
            }
            wds = SpecWord(wds, Qlx.LPAREN); // <node type pattern> (if not: <node type phrase>)
            var eg = Match(Qlx.CONNECTING, Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE);
            if (tok==Qlx.Id)
            {
                al = new Ident(this); // <node type alias>
                cx.defs += (al, 0);
                Next();
            }
            if (id.ident == "")
                id = al;
            var (dm,ps) = ElementDetails(id,Domain.NodeType);
            if (wds.Contains(Qlx.LPAREN))
                Mustbe(Qlx.RPAREN);
            var nd = new Ident(dm.name, new Iix(dm.defpos));
            cx.AddDefs(nd, dm, al.ident);
            cx.defs += (id, nd.iix);
            id = nd;
            var fk = tok;
            eg = eg || Match(Qlx.CONNECTING, Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE);
            if (eg && dm.kind == Qlx.NODETYPE && cx.db is Transaction tr)
                cx.db = tr + (Transaction.Physicals, tr.physicals - dm.defpos);
            var kind = (wds.Contains(Qlx.DIRECTED) || wds.Contains(Qlx.UNDIRECTED)
                     || wds.Contains(Qlx.EDGE) || wds.Contains(Qlx.RELATIONSHIP) 
                     || eg) ? Qlx.EDGETYPE
                : Qlx.NODETYPE;
            if (kind == Qlx.EDGETYPE)
            {
                Ident? ai = null;
                string? an = null;
                Domain? st = null;
                var sp = BList<(Ident, Domain)>.Empty;
                Ident? li = null;
                string? ln = null;
                Domain? ft = null;
                var fp = BList<(Ident, Domain)>.Empty;
                Domain? lt = null;
                Domain? at = null;
                al = new Ident(this);
                if (Match(Qlx.Id))
                {
                    cx.defs += (al, 0);
                    Next();
                }
                if (Match(Qlx.CONNECTING))
                {
                    Next();
                    Mustbe(Qlx.LPAREN);
                    var fi = new Ident(this);
                    var iix = cx.defs[fi];
                    ft = cx.obs[(iix != Iix.None) ? iix.dp: cx.role.nodeTypes[fi.ident] ?? -1L] as NodeType;
                    Mustbe(Qlx.Id);
                    var ar = tok;
                    Next();
                    var se = new Ident(this);
                    iix = cx.defs[se];
                    st = cx.obs[(iix != Iix.None) ? iix.dp : cx.role.nodeTypes[se.ident] ?? -1L] as NodeType;
                    Mustbe(Qlx.Id);
                    switch (ar)
                    {
                        case Qlx.TILDE:
                        case Qlx.TO:
                            wds += (Qlx.UNDIRECTED, true);
                            goto case Qlx.ARROWR;
                        case Qlx.ARROWR:
                            li = fi; ln = fi.ident; lt = (NodeType?)ft;
                            ai = se; an = se.ident; at = (NodeType?)st;
                            break;
                        case Qlx.ARROWL:
                            li = se; ln = se.ident; lt = (NodeType?)st;
                            ai = fi; an = fi.ident; at = (NodeType?)ft;
                            break;
                    }
                    Mustbe(Qlx.RPAREN);
                }
                else
                {
                    /*Mustbe(Qlx.LPAREN);
                     var fi = new Ident(this);
                     var fn = fi.ident;
                     if (tok == Qlx.Id)
                         Next();
                     else if (Match(Qlx.LABEL, Qlx.LABELS, Qlx.COLON, Qlx.IS, Qlx.Id, Qlx.LBRACE))
                         (ft, fp) = ElementDetails(fi);
                     FindOrCreateElementType(fi, (NodeType)(ft ?? Domain.NodeType), fp);
                     ft = cx.obs[cx.role.nodeTypes[fn] ?? -1L] as NodeType; 
                     Mustbe(Qlx.RPAREN); */
                    Next(); // ARROWBASE, RARROW, ARROWBASETILDE
                    var fi = id;
                    var fn = id.ident;
                    var iix = cx.defs[id]; 
                    ft = cx.obs[(iix != Iix.None) ? iix.dp: cx.role.nodeTypes[fn] ?? -1L] as Domain;
                    fp = ps;
                    var mi = new Ident(this);
                    var mn = mi.ident;
                    dm = Domain.EdgeType;
                    lp = lxr.Position;
                    if (tok == Qlx.Id)
                    {
                        cx.Add(new QlValue(mi, BList<Ident>.Empty, cx, Domain.EdgeType));
                        cx.AddDefs(mi, Domain.EdgeType);
                        Next();
                    }
                    else if (Match(Qlx.LABEL, Qlx.LABELS, Qlx.COLON, Qlx.IS, Qlx.Id, Qlx.LBRACE))
                    {
                        (dm, ps) = ElementDetails(mi,Domain.EdgeType);
                        id = new Ident(dm.name, new Iix(lp));
                    }
                    var sk = tok;
                    Next();
                    Mustbe(Qlx.LPAREN);
                    var si = new Ident(this);
                    var sn = fi.ident;
                    if (tok == Qlx.Id)
                        Next();
                    else if (Match(Qlx.LABEL, Qlx.LABELS, Qlx.COLON, Qlx.IS))
                        (st, sp) = ElementDetails(si,Domain.NodeType);
                    FindOrCreateElementType(si, st, sp);
                    st = cx.obs[cx.role.nodeTypes[sn] ?? -1L] as NodeType;
                    Mustbe(Qlx.RPAREN);
                    switch (fk)
                    {
                        case Qlx.ARROWBASETILDE:
                            wds += (Qlx.UNDIRECTED, true);
                            goto case Qlx.ARROWBASE;
                        case Qlx.ARROWBASE:
                            if (sk != Qlx.ARROW) throw new DBException("42161", Qlx.ARROW, sk);
                            lt = ft; ln = fn; at = st; an = sn;
                            break;
                        case Qlx.RARROW:
                            if (sk != Qlx.RARROWBASE) throw new DBException("42161", Qlx.ARROWBASE, sk);
                            lt = st; ln = sn; at = ft; an = fn;
                            break;
                    }
                }
                ps += (new Ident("LEAVING", new Iix(cx.GetUid())), Domain.Position);
                ps += (new Ident("ARRIVING", new Iix(cx.GetUid())), Domain.Position);
                return FindOrCreateElementType(id, null, ps, null, lt, at, wds).defpos;
            }
            else if (Match(Qlx.AS))
            {
                Next();
                al = new Ident(this);
                Mustbe(Qlx.Id);
            }
            var r = FindOrCreateElementType(id, null, ps, al).defpos;
            return r;
        }
        /// <summary>
        /// Given some of : an id, label expression, property set, and ends, first look to see 
        /// if we have such an element type.
        /// If we have an id or label expressions, we may hope to find the properties among existing node types.
        /// If we don't, we need to look at the property lists of unlabelledNodeTypes.
        /// Either way, we may should end up with a set P of existing/required properties.
        /// Existing properties will reference tablecolumns in cx.db; others will be qlvalues in cx.obs.
        /// Then we want to include the former and construct the latter.
        /// Note that if existing nodetypes give too many columns we need to construct supertypes as below.
        /// (the following stage numbers are very approximate!)
        /// 1: If there is a label, look at the oninsert set: If there is more than one node 
        /// return the joinednodetype if it has the properties we want, or error.
        /// If no label, check unlabelled for matches on properties.
        /// 2: If a single type is found with the required properties p, 
        /// (if id is not "", alter it to have the right name, 
        /// and if ends don't match construct a new variant) goto step 5.
        /// 3: Otherwise for each type X found 
        /// For each such X, make a supertype Y if needed so that we have p(Y)= p(X) intersect p, 
        /// and then we want a new nodetype that has these X's as supertypes 
        /// and contributes the rest of p.
        /// 4: Finally look at ends.
        /// INVARIANT: for any two existing nodeTypes A and B with
        /// p(A) intersect p(B) a nonempty set q, there exists C with 
        /// C superof A and C superof B and p(C)= q.
        /// </summary>
        /// <param name="id">The required name for the node type</param>
        /// <param name="dm">The label or label expression</param>
        /// <param name="ps">The required propertry list</param>
        /// <param name="al">A local alias</param>
        /// <param name="lt">For an edge, the type of the source node</param>
        /// <param name="at">For an edge, the type of the destination node</param>
        /// <param name="wds">The keywords collected from the element type specification</param>
        /// <returns>A (possibly joined) node type that meets these requirements</returns>
        /// <exception cref="DBException"></exception>
        NodeType FindOrCreateElementType(Ident id, Domain? dm, BList<(Ident, Domain)> ps, Ident? al = null,
            Domain? lt = null, Domain? at = null, CTree<Qlx, bool>? wds = null)
        {
            if (lt is null && at is null && cx.db.objects[cx.role.nodeTypes[id.ident] ?? -1L] is NodeType ne
                && ne.Length == ps.Length)
                return ne;
            if (lt is not null && at is not null && cx.db.objects[cx.role.edgeTypes[id.ident] ?? -1L] is NodeType ee
                && ee.Length == ps.Length)
                return ee;
            var un = CTree<Domain, bool>.Empty; // relevant node types
            var ep = CTree<long, bool>.Empty; // properties found
            var pn = BTree<string, long?>.Empty; // required properties: names and qlvalue pos
            var rp = CTree<long, bool>.Empty; // properties required (by qlvalue pos)
            for (var b = ps.First(); b != null; b = b.Next())
            {
                var i = b.value().Item1;
                pn += (i.ident, i.iix.dp);
                rp += (i.iix.dp, true);
            }
            var op = rp;
            // 1: Construct the set of relevant (super)types
            if (dm is GqlLabel lb)
                un = lb.OnInsert(cx);
            if (un.Count == 0 && dm?.defpos >= 0)
                un += (dm, true);
            else if (ps.Length > 0) // watch out for existing unlabelled node type
            {
                for (var b = cx.db.unlabelledNodeTypes.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.value() ?? -1L] is NodeType t)
                        for (var c = b.key().First(); c != null; c = c.Next())
                            if (cx.NameFor(c.key()) is string n && pn[n] is long q)
                            {
                                ep += (c.key(), true);
                                un += (t, true);
                                rp -= q;
                            }
            }
            // 2: check for required properties
            var ix = cx.defs[id];
            if (lt is GqlLabel ll)
                lt = cx.db.objects[cx.role.nodeTypes[ll.name] ?? -1L] as NodeType;
            if (at is GqlLabel la)
                at = cx.db.objects[cx.role.nodeTypes[la.name] ?? -1L] as NodeType;
            if (rp.Count==0 && un.Count==0 && ((ix!=Iix.None)?ix.dp:cx.role.nodeTypes[id.ident]) is long np 
                && cx.db.objects[np] is NodeType nt)
            {
                if (lt is null && at is null) return nt;
                var e = (EdgeType)nt;
                if (cx.db.objects[cx.db.edgeEnds[e.defpos]?[lt?.defpos ?? -1L]?[at?.defpos ?? -1L]??-1L] is EdgeType er)
                    return er;
                goto Define;
            }
            if (rp.Count == 0 && un.Count > 1L)
                return new JoinedNodeType(cx.GetUid(),id.ident,Domain.NodeType,new BTree<long,object>(Domain.NodeTypes,un),cx);
            // 3: If we get to here, we may need to construct suitable supertypes.
            if (un.Count > 1)
            {
                for (var b = un.First(); b != null; b = b.Next())
                    if (b.key() is NodeType ub)
                    {
                        var q = CTree<long, bool>.Empty;
                        for (var c = ub.representation.First(); c != null; c = c.Next())
                            if (op.Contains(c.key()))
                                q += (c.key(), true);
                        if (q.Count != ub.Length)
                        {
                                un -= b.key();
                            if (ub.SuperWith(cx, q) is NodeType nn)
                                un += (nn, true);
                            else
                            {
                                var rt = BList<long?>.Empty;
                                var rs = CTree<long, Domain>.Empty;
                                for (var c=q.First();c!=null;c=c.Next())
                                if (ub.representation[c.key()] is Domain d){ 
                                    rt += c.key();
                                    rs += (c.key(), d);
                                }
                                var nsp = cx.GetUid();
                                var nsn = DBObject.Uid(nsp);
                                var nsk = (ub.kind == Qlx.NODETYPE) ? Domain.NodeType : Domain.EdgeType;
                                var nsm = ub.mem + (Domain.RowType, rt) + (Domain.Representation, rs);
                                var nst = new NodeType(nsp,nsn,nsk,nsm,cx);
                                nst = (NodeType?)cx.Add(new PNodeType(nsn, nsk, ub.super, -1L, cx.db.nextPos, cx))
                                    ?? throw new DBException("42105");
                                un += (nst, true);
                            }
                        }
                    }
            }
            // 4: Finally build the required node or edge type.
            Define:;
            var tp = (lt is not null && at is not null) ?
                new PEdgeType(id.ident, (EdgeType)Domain.EdgeType.Relocate(id.iix.dp), un, -1L,
            lt.defpos, at.defpos, cx.db.nextPos, cx)
                : new PNodeType(id.ident, (NodeType)Domain.NodeType.Relocate(id.iix.dp), un, -1L, cx.db.nextPos, cx);
            var ut = (NodeType)(cx.Add(tp) ?? throw new DBException("42105"));
            var us = CTree<string, Domain>.Empty;
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value() ?? -1L] is TableColumn tc && cx.NameFor(tc.defpos) is string tn)
                    us += (tn, tc.domain);
            for (var b = ps.First(); b != null; b = b.Next())
            {
                var (cn, cd) = b.value();
                if (us.Contains(cn.ident))
                {
                    if (us[cn.ident]?.CompareTo(cd) == 0)
                        continue;
                    throw new DBException("42104", cn);
                }
                var pc = new PColumn3(ut, cn.ident, -1, cd, PColumn.GraphFlags.None, -1L, -1L, cx.db.nextPos, cx);
                if (cn.ident == "LEAVING" && lt is not null)
                {
                    pc.flags = PColumn.GraphFlags.LeaveCol;
                    pc.toType = lt.defpos;
                }
                if (cn.ident == "ARRIVING" && at is not null)
                {
                    pc.flags = PColumn.GraphFlags.ArriveCol;
                    pc.toType = at.defpos;
                }
                cx.Add(pc);
                if (pc.toType >= 0 && cx.db.objects[pc.defpos] is TableColumn tc)
                {
                    var px = new PIndex(cn.ident, (Table)tp.dataType,
                        new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(tc), 1),
                    PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate, -1, cx.db.nextPos);
                    cx.Add(px);
                }
            }
            if (wds is not null && wds.Contains(Qlx.UNDIRECTED) && cx._Ob(ut.defpos) is EdgeType et)
            {
                et += (QuantifiedPredicate.Between, true);
                cx.Add(et);
            }
            return cx.db.objects[ut.defpos] as NodeType ?? throw new DBException("42105");
        }
        (Domain,BList<(Ident,Domain)>) ElementDetails(Ident id,NodeType xp)
        {
            var ix = cx.defs[id];
            if (ix != Iix.None && cx.obs[ix.dp] is Domain n)
                return (n, BList<(Ident, Domain)>.Empty);
            Domain le = GqlLabel.Empty;
            if (Match(Qlx.LABEL,Qlx.LABELS,Qlx.COLON,Qlx.DOUBLEARROW,Qlx.IMPLIES))
                le = ParseLabelExpression(xp);
            var lp = lxr.Position;
            if (Match(Qlx.DOUBLEARROW, Qlx.IMPLIES)) // we prefer DOUBLEARROW to the keyword
            {
                Next();
                var lf = ParseLabelExpression(xp);
                cx.Add(lf);
                le = (Domain)cx.Add(new GqlLabel(lp,le.defpos,lf.defpos,
                    new BTree<long,object>(Domain.Kind,Qlx.DOUBLEARROW)));
            }
            var ps = BList<(Ident, Domain)>.Empty;
            if (Match(Qlx.LBRACE))
            {
                Next();
                cx.IncSD(id);
                var pn = new Ident(this);
                Mustbe(Qlx.Id);
                if (Match(Qlx.COLON, Qlx.DOUBLECOLON, Qlx.TYPED)) // allow colon because of JSON option
                    Next();
                var dt = ParseDataType();
                ps += (pn, dt);
                while (tok == Qlx.COMMA)
                {
                    Next();
                    pn = new Ident(this);
                    Mustbe(Qlx.Id);
                    dt = ParseDataType();
                    ps += (pn, dt);
                }
                Mustbe(Qlx.RBRACE);
                cx.DecSDClear();
            }
            if (le.kind == Qlx.NO)
            {
                if (xp.kind == Qlx.NODETYPE)
                    le = FindOrCreateElementType(id, Domain.NodeType, ps);
                else
                    le = FindOrCreateElementType(id, Domain.EdgeType, ps, null,
                        GqlLabel.Empty, GqlLabel.Empty);
            }
            return (le,ps);
        }
 /*       (CTree<long,bool>,string) ParseLabelSet()
        {
            var sb = new StringBuilder();
            var r = CTree<long, bool>.Empty;
            if (Match(Qlx.COLON,Qlx.LABELS,Qlx.LABEL,Qlx.TYPED))
                Next();
            var id = new Ident(this);
            Mustbe(Qlx.Id);
            cx.Add(new GqlLabel(id));
            cx.defs += (id, cx.sD);
            r += (id.iix.dp, true);
            sb.Append(id.ident);
            while (tok==Qlx.AMPERSAND)
            {
                Next();
                id = new Ident(this);
                Mustbe(Qlx.Id);
                cx.Add(new GqlLabel(id));
                cx.defs += (id, cx.sD);
                r += (id.iix.dp, true);
                sb.Append('&'); sb.Append(id.ident);
            }
            return (r,sb.ToString());
        } */
        internal string ParseCatalogReference()
        {
            lxr.cat = true;
            lxr.Rescan();
            Next();
            var s = lxr.val.ToString();
            lxr.cat = false;
            Next();
            return s;
        }
        internal BList<long?> ParseBindingVariableDefinitions()
        {
            var r = BList<long?>.Empty;
            while (Match(Qlx.BINDING, Qlx.GRAPH, Qlx.TABLE, Qlx.VALUE))
            {
                var lp = LexPos();
                if (Match(Qlx.BINDING))
                {
                    Next();
                    Match(Qlx.BINDING, Qlx.GRAPH, Qlx.TABLE, Qlx.VALUE);
                }
                var tp = tok;
                Mustbe(Qlx.GRAPH, Qlx.TABLE, Qlx.VALUE);
                var id = new Ident(this);
                Mustbe(Qlx.Id);
                switch (tp)
                {
                    case Qlx.GRAPH:
                        if (Match(Qlx.DOUBLECOLON, Qlx.TYPED, Qlx.ANY, Qlx.BINDING, Qlx.GRAPH))
                        {
                            var ts = CTree<long, bool>.Empty;
                            if (Match(Qlx.DOUBLECOLON, Qlx.TYPED))
                                Next();
                            if (tok == Qlx.ANY)
                            {
                                Next();
                                if (tok == Qlx.BINDING)
                                    Next();
                                Mustbe(Qlx.GRAPH);
                            }
                            else
                            {
                                if (tok == Qlx.BINDING)
                                    Next();
                                Mustbe(Qlx.GRAPH);
                                ts = ParseNestedGraphTypeSpecification(ts);
                            }
                            if (tok == Qlx.NOT)
                            {
                                Next();
                                Mustbe(Qlx.NULL);
                            }
                            var gt = new GraphType(id.iix.dp, new BTree<long, object>(Domain.Constraints, ts));
                            gt += (ObInfo.Name, id.ident);
                            cx.Add(gt);
                            r += lp.dp;
                        }
                        Mustbe(Qlx.EQL);
                        ParseSqlValue(Domain.GraphSpec);
                        break;
                    case Qlx.TABLE:
                        if (Match(Qlx.DOUBLECOLON, Qlx.TYPED, Qlx.BINDING, Qlx.TABLE))
                        {
                            var ts = CTree<long, bool>.Empty;
                            if (Match(Qlx.DOUBLECOLON, Qlx.TYPED))
                                Next();
                            if (tok == Qlx.ANY)
                            {
                                Next();
                                if (Match(Qlx.BINDING))
                                    Next();
                                Mustbe(Qlx.GRAPH);
                            }
                            else
                            {
                                if (Match(Qlx.BINDING))
                                    Next();
                                Mustbe(Qlx.GRAPH);
                                ParseNestedGraphTypeSpecification(ts);
                            }
                            if (tok == Qlx.NOT)
                            {
                                Next();
                                Mustbe(Qlx.NULL);
                            }
                            Mustbe(Qlx.EQL);
                            var gs = ParseSqlValue(Domain.GraphSpec);
                            gs += (ObInfo.Name, id.ident);
                            cx.Add(gs);
                            r += gs.defpos;
                        }
                        break;
                    case Qlx.VALUE:
                        {
                            var dt = Domain.Content;
                            if (Match(Qlx.DOUBLECOLON, Qlx.TYPED)|| StartDataType())
                            {
                                if (Match(Qlx.DOUBLECOLON, Qlx.TYPED))
                                    Next();
                                dt = ParseDataType();
                            }
                            if (tok == Qlx.NOT)
                            {
                                Next();
                                Mustbe(Qlx.NULL);
                            }
                            Mustbe(Qlx.EQL);
                            var sv = ParseSqlValue(dt);
                            sv += (ObInfo.Name, id.ident);
                            cx.Add(sv);
                            r += sv.defpos;
                        }
                        break;
                }
            }
            return r;
        }
        /// <summary>
        /// Create: CREATE GraphPattern {',' GraphPattern } [THEN Statements END].
        /// A graph fragment results in the addition of at least one Record
        /// in a node type: the node type definition comprising a table and UDT may 
        /// be created and/or altered on the fly, and may extend to further edge and edge types.
        /// The given input is a sequence of clauses that contain graph expressions
        /// with internal and external references to existing and new nodes, edges and their types.
        /// Values are not always constants either, so the graph expressions must be
        /// SqlValueGraph rather than TGraphs.
        /// This routine 
        /// 1. constructs a tree of SqlValueGraphs corresponding to the given input, 
        /// with their internal and external references. Many of the node and edge references
        /// will be unbound at this stage because some types may be new or to be modified, and
        /// expressions have not been evaluated.
        /// 2. analyses the referenced types and expressions for consistency. (recursive, semi-interactive)
        /// 3. generates Physicals to update the set of types.
        /// 4. evaluates the SqlValueGraphs to generate Records and Updates (recursive)
        ///    Binds the (uncomitted) nodes
        /// 5. adds the Graph and Node->Edge associations to the database
        /// </summary>
        internal Executable ParseInsertGraph()
        {
            var sch = false;
            if (Match(Qlx.SCHEMA))
            {
                sch = true; 
                Next();
                if (tok == Qlx.DIVIDE)
                    ParseCreateSchema();
            }
            // New nodes without Id keys should be assigned cx.db.nextPos.ToString(), and this is fixed
            // on Commit, see Record(Record,Writer): the NodeOrEdge flag is added in Record()
            var ge = ParseInsertGraphList(sch);
            var st = BList<long?>.Empty;
            var cs = (GraphInsertStatement)cx.Add(new GraphInsertStatement(cx.GetUid(), sch, ge, st));
            if (cx.parse == ExecuteStatus.Obey && ((!sch)||cs.graphExps[0]?[0]?.domain.kind==Qlx.EDGETYPE))
                cs._Obey(cx);
            if (tok == Qlx.THEN)
            {
                Next();
                var e = BList<long?>.Empty;
                for (; ; )
                {
                    if (ParseStatement(Domain.Content) is Executable x)
                    {
                        cx.Add(x);
                        e += x.defpos;
                    }
                    if (tok != Qlx.SEMICOLON)
                        break;
                    Next();
                }
                Mustbe(Qlx.END);
                cs += (IfThenElse.Then, e);
            }
            cx.Add(cs);
            return cs;
        }
        /// <summary>
        /// Match: MatchMode  [id'='] MatchNode {'|'Match }.
        /// MatchMode: [TRAIL|ACYCLIC|SIMPLE][SHORTEST|SHORTESTPATH|ALL|ANY].
        /// </summary>
        /// <returns></returns>
        (CTree<long, TGParam>, BList<long?>) ParseSqlMatchList()
        {
            // Step M10
            var svgs = BList<long?>.Empty;
            var tgs = CTree<long, TGParam>.Empty;
            Ident? pi = null;
            // the current token is LPAREN
            while (Match(Qlx.LPAREN, Qlx.USING, Qlx.TRAIL, Qlx.ACYCLIC, Qlx.SIMPLE,
                Qlx.SHORTESTPATH, Qlx.SHORTEST, Qlx.ALL, Qlx.ANY))
            {
                // state M11
                cx.IncSD(new Ident(this)); // ident part if possibly unhelpful
                var dp = cx.GetUid();
                var alts = BList<long?>.Empty;
                var mo = Qlx.NONE;
                var sh = Qlx.NONE;
                if (tok != Qlx.LPAREN)
                {
                    if (Match(Qlx.TRAIL, Qlx.ACYCLIC, Qlx.SIMPLE))
                    {
                        mo = tok;
                        Next();
                    }
                    if (Match(Qlx.SHORTEST, Qlx.SHORTESTPATH, Qlx.ALL, Qlx.ANY))
                    {
                        sh = tok;
                        Next();
                    }
                    pi = new Ident(this);
                    if (tok == Qlx.Id)
                    {
                        Next();
                        if (lxr.tgs[pi.iix.dp] is TGParam gp)
                        {
                            gp = new TGParam(gp.uid, gp.value, Domain.PathType, TGParam.Type.Path, dp);
                            lxr.tgs += (pi.iix.dp, gp);
                            cx.Add(new QlValue(pi, BList<Ident>.Empty, cx, Domain.PathType)
                                +(DBObject._From,dp));
                            cx.defs += (pi, cx.sD);
                            cx.locals += (pi.iix.dp, true);
                        }
                        Mustbe(Qlx.EQL);
                    }
                }
                // state M12
                (tgs, var s) = ParseGqlMatch(dp, pi, tgs);
                // state M13
                alts += cx.Add(new GqlMatchAlt(dp, cx, mo, sh, s, pi?.iix.dp ?? -1L)).defpos;
                // state M14
                while (tok == Qlx.VBAR)
                {
                    Next();
                    // state M15
                    dp = cx.GetUid();
                    (tgs, s) = ParseGqlMatch(dp, pi, tgs);
                    // state M16
                    var ns = BTree<string, TGParam>.Empty;
                    alts += cx.Add(new GqlMatchAlt(dp, cx, mo, sh, s, pi?.iix.dp ?? -1L)).defpos;
                    // goto state M14
                }
                // state M17
                svgs += cx.Add(new GqlMatch(cx, alts)).defpos;
                cx.DecSD();
                if (tok == Qlx.COMMA)
                    Next();
                // goto state M11
                else break;
            };
            // state M18
            // we now can define pi properly.
            return (tgs, svgs);
        }
        // we have just passed TRUNCATING or , in
        // Truncation = TRUNCATING TruncationSpec{',' TruncationSpec} .
        // we are starting on 
        // TruncationSpec = [EdgeType_id][{OrderSpec {',' OrderSpec}] '=' int .
        BTree<long, (int, Domain)> ParseTruncation()
        {
            var r = BTree<long, (int, Domain)>.Empty;
            // state M2
            while (true)
            {
                // state M3
                var k = Domain.EdgeType.defpos;
                if (tok == Qlx.Id)
                {
                    var ei = lxr.val.ToString();
                    Next();
                    var et = cx.db.objects[cx.role.dbobjects[ei] ?? -1L] as EdgeType
                        ?? throw new DBException("42161", Qlx.EDGETYPE);
                    cx.AddDefs(et);
                    k = et.defpos;
                }
                // state M4
                Mustbe(Qlx.LPAREN);
                var rt = BList<DBObject>.Empty;
                // state M5
                while (tok == Qlx.Id)
                {
                    rt += cx._Ob(ParseOrderItem(false)) ?? SqlNull.Value;
                    if (tok == Qlx.COMMA)
                        Next(); 
                    else
                        break; 
                    // state M6
                }
                Mustbe(Qlx.RPAREN);
                // state M7
                var od = (rt.Length == 0) ? Domain.Content : new Domain(cx.GetUid(), cx, Qlx.ROW, rt, rt.Length);
                Mustbe(Qlx.EQL);
                var lm = lxr.val.ToInt() ?? throw new DBException("42161", Qlx.INT);
                Next();
                r += (k, (lm, od));
                if (tok == Qlx.COMMA)
                    Next();
                else
                    break;
                // state M8: goto state M3
            }
            // state M9
            return r;
        }
        CList<CList<GqlNode>> ParseInsertGraphList(bool sch)
        {
            var svgs = CList<CList<GqlNode>>.Empty;
            // the current token is LPAREN
            while (tok == Qlx.LPAREN || tok==Qlx.LBRACK)
            {
                svgs += ParseInsertGraphStep(sch);
                if (tok == Qlx.COMMA)
                    Next();
            };
            return svgs;
        }
        CList<GqlNode> ParseInsertGraphStep(bool sch)
        {
            cx.IncSD(new Ident(this));
            // the current token is LPAREN or LBRACK
            var svg = CList<GqlNode>.Empty;
            (var n, svg) = ParseInsertGraphItem(svg, sch);
            while (tok == Qlx.RARROW || tok == Qlx.ARROWBASE)
                (n, svg) = ParseInsertGraphItem(svg, sch, n);
            cx.DecSD();
            return svg;
        }
        // state M19
        // This will give us a pattern of SqlNodes svg and will update tgs and cx.defs (including pi)
        (CTree<long, TGParam>, BList<long?>) ParseGqlMatch(long f, Ident? pi, CTree<long, TGParam> tgs)
        {
            // the current token is LPAREN
            var svg = BList<long?>.Empty;
            (var n, svg, tgs) = ParseMatchExp(svg, pi, tgs, f);
            // state M20
            tgs += n.state;
            lxr.tgs = CTree<long, TGParam>.Empty;
            return (tgs, svg);
        }
        /// <summary>
        /// GraphPattern: Node Path .
        /// Path: { Edge Node }.
        /// Node: '(' GraphItem ')'.
        /// Edge: '-[' GraphItem ']->' | '<-[' GraphItem ']-'.
        /// GraphItem: [Value][Label][doc]. // labels are in role.dbobjects or made PTypes, become longs
        /// Label: ':' (id|Value){(':'|'&')(id|Value)}. // in this version that is the limit of complexity
        /// </summary>
        /// <param name="svg">The graph fragments so far</param>
        /// <param name="dt">The standard NODETYPE or EDGETYPE</param>
        /// <param name="ln">The node to attach the new edge</param>
        /// <returns>An GqlNode for the new node or edge and the tree of all the graph fragments</returns>
        (GqlNode, CList<GqlNode>) ParseInsertGraphItem(CList<GqlNode> svg, bool sch = false, GqlNode? ln = null)
        {
            var og = lxr.tgs;
            lxr.tgs = CTree<long, TGParam>.Empty;
            var ro = cx.role;
            var ab = tok; // LPAREN, LBRACK, ARROWBASE or RARROW
            var pi = new Ident(this);
            if (tok == Qlx.Id)
                Next();
            Mustbe(Qlx.LPAREN, Qlx.LBRACK, Qlx.ARROWBASE, Qlx.RARROW);
            var b = new Ident(this);
            var bound = cx.locals.Contains(cx.defs[b].dp);
            var nb = b;
            long id = -1L;
            var lp = lxr.Position;
            NodeType? dm = null;
            if (tok == Qlx.Id)
            {
                if (cx.role.dbobjects.Contains(b.ident))
                    throw new DBException("42104", b.ident);
                var ix = cx.defs[b];
                if (ix == Iix.None)
                {
                    id = lp;
                    cx.defs += (b, b.iix);
                }
                else
                {
                    id = ix.dp;
                    bound = true;
                }
                Next();
            }
            Domain lb = GqlLabel.Empty;
            Qlx tk = tok;
            if (tok == Qlx.COLON)
            {
                if (bound)
                    throw new DBException("42104", b.ident);
                Next();
                lb = ParseLabelExpression((ab == Qlx.LPAREN) ? Domain.NodeType : Domain.EdgeType);
            }
            else if (!bound)
                throw new DBException("42107", b.ident);
            cx.IncSD(b);
            var dc = CTree<string, QlValue>.Empty;
            CTree<long, bool>? wh = null;
            if (tok == Qlx.LBRACE)
            {
                Next();
                while (tok != Qlx.RBRACE)
                {
                    var (n, v) = GetDocItem(lp,lb,sch);
                    if (lb.name is not null)
                    {
                        var px = cx.Ix(b.iix.lp, n.iix.dp);
                        var lx = cx.Ix(lb.defpos, n.iix.dp);
                        var ic = new Ident(n.ident,px,null);
                        var it = new Ident(lb.name,lx,null);
                        var iq = new Ident(b, ic);
                        var iu = new Ident(it, ic);
                        cx.defs += (iu, ic.iix);
                        cx.defs += (iq, ic.iix);
                        cx.defs += (ic, ic.iix);
                    }
                    dc += (n.ident, v);
                    if (tok == Qlx.COMMA)
                        Next();
                }
                Mustbe(Qlx.RBRACE);
            }
            GqlNode? an = null;
            var ahead = CList<GqlNode>.Empty;
            if (ln is not null)
            {
                var ba = (ab == Qlx.ARROWBASE) ? Qlx.ARROW : Qlx.RARROWBASE;
                Mustbe(ba);
                (an, ahead) = ParseInsertGraphItem(ahead,sch);
            }
            else
                Mustbe(Qlx.RPAREN,Qlx.RBRACK);
            var m = BTree<long, object>.Empty + (GqlNode._Label, lb) + (SqlValueExpr.Op,tk);
            if (ln is not null && an is not null)
            {
                m = m + (EdgeType.LeavingType, ln.domain.defpos) + (EdgeType.ArrivingType, an.domain.defpos);
                if (sch) //??
                    m = m + (GqlEdge.LeavingValue, ln.defpos) + (GqlEdge.ArrivingValue, an.defpos);
            }
            if (cx.obs[id] is GqlNode r)
                r = new GqlReference(lp, r);
            else if (lb.defpos >= 0 && lb.rowType == BList<long?>.Empty && lb is NodeType zt)
                r = new GqlReference(lp, zt);
            else
                r = ab switch
            {
                Qlx.LPAREN => new GqlNode(nb, BList<Ident>.Empty, cx, id, dc, lxr.tgs, dm, m),
                Qlx.LBRACK => new GqlEdge(nb, BList<Ident>.Empty, cx, Qlx.Null, -1L, 
                -1L, -1L, dc, lxr.tgs, dm, m),
                Qlx.ARROWBASE => new GqlEdge(nb, BList<Ident>.Empty, cx, ab, id,
                ln?.defpos ?? -1L, an?.defpos ?? -1L, dc, lxr.tgs, dm, m),
                Qlx.RARROW => new GqlEdge(nb, BList<Ident>.Empty, cx, ab, id,
                an?.defpos ?? -1L, ln?.defpos ?? -1L, dc, lxr.tgs, dm, m),
                _ => throw new DBException("42000", ab).Add(Qlx.INSERT_STATEMENT,new TChar(ab.ToString()))
            };
            if (wh is not null)
                r += (RowSet._Where, wh);
            cx.Add(r);
            cx.defs += (b, new Iix(id, cx, r.defpos));
            cx.DecSD(null,null,lxr.tgs);
            svg += r;
            if (an != null)
            {
                svg += ahead;
                r = an;
            }
            lxr.tgs = og;
            return (r, svg);
        }

        /// <summary>
        /// MatchNode: '(' MatchItem ')' { (MatchEdge|MatchPath) MatchNode } .
        /// MatchEdge: '-[' MatchItem ']->' | '<-[' MatchItem ']-'.
        /// MatchItem: [Value][Label][doc | WhereClause ].
        /// MatchPath: '[' Match ']' MatchQuantifier .
        /// Label: ':' (id|Value)[Label].
        /// MatchQuanitifier : '?' | '*' | '+' | '{'int','[int]'}' .
        /// </summary>
        /// <param name="svg">The graph fragments so far</param>
        /// <param name="ln">The node to attach the new edge</param>
        /// <returns>An GqlNode for the new node or edge, the list of all the pattern fragments, and TGParams</returns>
        (GqlNode, BList<long?>, CTree<long, TGParam>) ParseMatchExp(BList<long?> svg, Ident? pi, CTree<long, TGParam> tgs, long f,
            Ident? ln = null)
        {
            // state M21
            var st = CTree<long, TGParam>.Empty; // for match
            var ab = tok; // LPAREN, ARROWBASE, RARROW, LBRACK
            lxr.tga = f;
            cx.IncSD(new Ident(this)); // ident part is possibly unhelpful
     //       lxr.tgt = BTree<string,(int,long?)>.Empty;
            var pgg = lxr.tgg;
            var og = lxr.tgs;
            lxr.tgs = CTree<long, TGParam>.Empty;
            if (tok == Qlx.LBRACK)
                lxr.tgg = TGParam.Type.Group;
            Mustbe(Qlx.LPAREN, Qlx.ARROWBASE, Qlx.RARROW, Qlx.LBRACK);
            GqlNode? r = null;
            GqlNode? an = null;
            var b = new Ident(this);
            long id = -1L;
            var ahead = BList<long?>.Empty;
            if (ab == Qlx.LBRACK)
            {
                // state M22
                var (tgp, svp) = ParseGqlMatch(f, pi, lxr.tgs);
                Mustbe(Qlx.RBRACK);
                // state M23
                lxr.tgg = pgg;
                var qu = (-1, 0);
                if (Match(Qlx.QMARK, Qlx.TIMES, Qlx.PLUS, Qlx.LBRACE))
                    qu = ParseMatchQuantifier();
                // state M24
                tgs += tgp;
                (var sa, ahead, tgs) = ParseMatchExp(ahead, pi, tgs, f, ln);
                r = new GqlPath(cx, svp, qu, ln?.iix.dp ?? -1L, sa.defpos);
                // to state M34
            }
            else
            {
                // state M25
                var lp = lxr.Position;
                NodeType? dm = null;
                if (tok == Qlx.Id)
                {
                    var ix = cx.defs[b];
                    if (lxr.tgs[lp] is TGParam ig)
                        st += (-(int)Qlx.Id, ig);
                    if (ix == Iix.None)
                    {
                        id = lp;
                        cx.locals += (b.iix.dp, true);
                        cx.defs += (b, b.iix);
                    }
                    else
                        id = ix.dp;
                    Next();
                }
                // state M26
                DBObject lb = GqlLabel.Empty;
                if (tok == Qlx.COLON)
                {
                    lxr.tex = true; // expect a Type
                    Next();
                    var s = cx.defs[b];
                    lb = ParseLabelExpression((ab==Qlx.LPAREN)?Domain.NodeType:Domain.EdgeType,b.ident);
                    if (cx.locals.Contains(b.iix.dp)) // yuk
                        cx.defs += (b, s);
                    if (lxr.tgs[lb.defpos] is TGParam qg)
                        st += (-(int)Qlx.TYPE, qg);
                    // state M28
                    if (lb is GqlLabel sl && sl.domain is NodeType nt && nt.defpos > 0)
                        dm = nt;
                    if (lb is Domain ld && (ld.kind == Qlx.NODETYPE||ld.kind==Qlx.EDGETYPE) && ld is NodeType)
                        dm = (NodeType)ld;
                    else if (lb is QlValue gl && gl.Eval(cx) is TTypeSpec tt)
                        dm = tt._dataType as NodeType;
                    var pg = (dm is not null) ? dm.kind switch
                    {
                        Qlx.NODETYPE => TGParam.Type.Node,
                        Qlx.EDGETYPE => TGParam.Type.Edge,
                        Qlx.ARRAY => TGParam.Type.Path,
                        _ => TGParam.Type.None
                    } : TGParam.Type.Maybe;
                    if (lxr.tgs[lp] is TGParam ig && dm is not null)
                    {
                        Domain nd = dm;
                        if (ig.type.HasFlag(TGParam.Type.Group))
                        {
                            nd = new Domain(-1L, Qlx.ARRAY, dm);
                            pg |= TGParam.Type.Group;
                        }
                        lxr.tgs += (lp, new TGParam(lp, ig.value, nd, pg, f));
                    }
                }
                // state M29
                var dc = CTree<string,QlValue>.Empty;
                CTree<long, bool>? wh = null;
                if (tok == Qlx.LBRACE)
                {
                    lxr.tex = false; // expect a Value
                    Next();
                    while (tok != Qlx.RBRACE)
                    {
                        var (n, v) = GetDocItem(lp,(Domain)lb);
                        dc += (n.ident, v);
                        if (tok == Qlx.COMMA)
                            Next();
                    }
                    Mustbe(Qlx.RBRACE);
                }
                // state M30
                if (tok == Qlx.WHERE)
                {
                    var cd = cx.defs;
             //       if (dm is not null)
             //           cx.AddDefs(dm);
                    var ot = lxr.tgs;
                    var od = dm ?? Domain.NodeType;
                    cx.Add(new GqlNode(b, BList<Ident>.Empty, cx, -1L, CTree<string, QlValue>.Empty,
                        CTree<long, TGParam>.Empty, od));
                    cx.defs += (b, b.iix);
                    wh = ParseWhereClause();
                    cx.defs = cd;
                    cx.obs -= b.iix.dp; // wow
                    if (ot is not null) // why is this required?
                        lxr.tgs = ot;
                }
                st += lxr.tgs;
                // state M31
                Qlx ba = Qlx.RPAREN;
                if (ln is not null && ab != Qlx.LBRACK && ab != Qlx.LPAREN)
                {
                    ba = (ab == Qlx.ARROWBASE) ? Qlx.ARROW : Qlx.RARROWBASE;
                    if (ln.ident != "COLON" && st[ln.iix.dp] is TGParam lg)
                        st += (-(int)ab, lg);
                    if (lxr.tgs != null)
                        lxr.tgs = CTree<long, TGParam>.Empty;
                }
                else if (ab == Qlx.LBRACK)
                    ba = Qlx.RBRACK;
                Mustbe(ba);
                var m = BTree<long, object>.Empty + (SqlFunction._Val, lb.defpos);
                r = ab switch
                {
                    Qlx.LPAREN => new GqlNode(b, BList<Ident>.Empty, cx, id, dc, st, dm, m),
                    Qlx.ARROWBASE => new GqlEdge(b, BList<Ident>.Empty, cx, ab, id, ln?.iix.dp ?? -1L, -1L, dc, st, dm, m),
                    Qlx.RARROW => new GqlEdge(b, BList<Ident>.Empty, cx, ab, id, -1L, ln?.iix.dp ?? -1L, dc, st, dm, m),
                    _ => throw new DBException("42000", ab).Add(Qlx.MATCH_STATEMENT, new TChar(ab.ToString()))
                };
                r += (GqlNode._Label, lb);
                if (wh is not null)
                    r += (RowSet._Where, wh);
            }
            cx.Add(r);
            cx.DecSD();
            // state M32
            if (Match(Qlx.LPAREN, Qlx.ARROWBASE, Qlx.RARROW, Qlx.LBRACK))
                (an, ahead, tgs) = ParseMatchExp(ahead, pi, tgs, f, b);
            // state M33
            if (r is null)
                throw new DBException("42000", "MatchExp").Add(Qlx.MATCH_STATEMENT, new TChar(an?.name??"??"));
            if (r is GqlEdge)
                r = r.Add(cx, an, st);
            cx.Add(r);
            tgs += r.state;
            if (id > 0)
                cx.defs += (b, new Iix(id));
            svg += r.defpos;
            svg += ahead;
            cx.DecSD();
            lxr.tgs = og;
            return (r, svg, tgs);
        }
        /// <summary>
        /// Don't attempt to interpret the labels at this point
        /// (e.g. for Match they may be unbound variables)
        /// </summary>
        /// <returns></returns>
        Domain ParseLabelExpression(NodeType dm, string? a=null, NodeType? lt = null, NodeType? at = null)
        {
            var lp = lxr.Position;
            var neg = false;
            if (tok == Qlx.EXCLAMATION)
            {
                neg = true;
                Next();
            }
            if (tok == Qlx.COLON)
                Next();
            var c1 = new Ident(this);
            Mustbe(Qlx.Id);
            var left = cx.db.objects[cx.role.dbobjects[c1.ident] ?? -1L] as Domain 
                ?? (Domain)cx.Add(new GqlLabel(c1,cx,lt,at,new BTree<long,object>(Domain.Kind,dm.kind)));
            cx.defs += (c1, c1.iix);
            cx.AddDefs(c1,left.rowType,a);
            cx.Add(left);
            if (neg)
            {
                left = new GqlLabel(lp, -1L, left.defpos, new BTree<long,object>(Domain.Kind,Qlx.EXCLAMATION));
                cx.Add(left);
            }
            while (Match(Qlx.VBAR,Qlx.COLON,Qlx.AMPERSAND,Qlx.DOUBLEARROW))
            {
                lp = lxr.Position;
                var op = tok;
                Next();
                if (Match(Qlx.COLON))
                    Next();
                var right = ParseLabelExpression(Domain.NodeType,c1.ident,lt,at);
                cx.Add(right);
                left = new GqlLabel(lp, left.defpos, right.defpos, new BTree<long,object>(Domain.Kind,op)); // leave name empty for now
                cx.Add(left);
            }
            return left;
        }
        /// <summary>
        /// we have just matched MATCH|WITH
        /// MatchStatement: Match {',' Match} [WhereClause] [[THEN] Statement].
        /// Match: MatchMode MatchPattern
        /// </summary>
        /// <returns></returns>
        /// <exception cref="DBException"></exception>
        internal Executable ParseMatchStatement()
        {
            var olddefs = cx.defs; // we will remove any defs introduced by the Match below
                                   // while allowing existing defs to be updated by the Match parser
            var oldlocals = cx.locals;
            var flags = MatchStatement.Flags.None;
            Next();
            cx.IncSD(new Ident(this)); // TBD: the ident part is possibly unhelpful
            if (tok == Qlx.SCHEMA)
            {
                flags |= MatchStatement.Flags.Schema;
                Next();
            }
            // State M0
            BTree<long, (int, Domain)>? tg = null;
            if (Match(Qlx.TRUNCATING))
            {
                // State M1
                Next();
                // State M2
                tg = ParseTruncation();
            }
            lxr.tgs = CTree<long, TGParam>.Empty;
            lxr.ParsingMatch = true;
            var (tgs, svgs) = ParseSqlMatchList();
            var xs = CTree<long, bool>.Empty;
            for (var b = svgs.First(); b != null; b = b.Next())
                if (cx.obs[b.value() ?? -1] is GqlMatch ss)
                    for (var c = ss.matchAlts.First(); c != null; c = c.Next())
                        if (cx.obs[c.value() ?? -1L] is GqlMatchAlt sa)
                            for (var dd = sa.matchExps.First(); dd != null; dd = dd.Next())
                                if (dd.value() is long ep)
                                    xs += (ep, true);
            // state M18
            lxr.ParsingMatch = false;
            var (ers, ns) = BindingTable(cx, tgs);
            var m = ers.mem;
            m += (ObInfo.Names, ns);
            m += (MatchStatement.MatchFlags, flags);
            m += (MatchStatement.BindingTable, ers.defpos);
            m += (DBObject._Domain, ers);
            var wh = ParseWhereClause() ?? CTree<long, bool>.Empty;
            m += (RowSet._Where, wh);
            long e = -1L;
            var pe = -1L;
            Domain rd = Domain.Row + (Domain.Nodes, xs);
            Match(Qlx.FETCH);
            if (tok != Qlx.EOF && tok != Qlx.END && tok != Qlx.RPAREN)
            {
                var op = cx.parse;
                cx.parse = ExecuteStatus.Graph;
                if (tok != Qlx.ORDER && tok != Qlx.FETCH)
                {
                    var xe = ParseStatement(Domain.Content + (Domain.Nodes, xs));
                    if (xe is SelectStatement ss && cx.obs[ss.union] is RowSet su)
                    {
                        cx.Add(su + (RowSet._Source, ers.defpos));
                        if (su is RowSetSection sc)
                            m += (RowSetSection.Size, sc.size);
                        pe = xe.defpos;
                    }
                    else
                    {
                        e = cx.lastret;
                        rd = cx.obs[(cx.obs[e] as ReturnStatement)?.result ?? -1L] as RowSet ?? rd;
                        pe = xe.defpos;
                    }
                }
                if (tok == Qlx.ORDER && rd is RowSet rr)
                    m += (RowSet.RowOrder, ParseOrderClause(rr, Domain.Row, false));// limit to requirements specified here
                if (Match(Qlx.FETCH))
                    m += (RowSetSection.Size, FetchFirstClause());
                if (cx.obs[cx.lastret] is ReturnStatement rs && cx.obs[rs.ret]?.domain is Domain d && d != Domain.Null)
                {
                    RowSet es = (RowSet)(cx.obs[(long)(rs.mem[SqlInsert.Value] ?? -1L)] ??
                        throw new DBException("42000").Add(Qlx.MATCH_STATEMENT, new TChar("Return")));
                    m += (DBObject._Domain, es);
                    cx.result = es.defpos;
                }
                cx.parse = op;
            }
            if (tgs is null)
                throw new DBException("PE60201");
            cx.Add(ers);
            cx.result = ers.defpos;
            var ms = new MatchStatement(cx, tg, tgs, svgs, m, pe, e);
            cx.DecSD();
            if (cx.parse == ExecuteStatus.Obey)
                ms._Obey(cx);
            for (var b = cx.defs.First(); b != null; b = b.Next())
                if (!olddefs.Contains(b.key()))
                    cx.defs -= b.key();
            cx.locals = oldlocals;
            return ms;
        }
        internal static (BindingRowSet, BTree<string, (int, long?)>) BindingTable(Context cx, CTree<long, TGParam> gs)
        {
            var rt = BList<long?>.Empty;
            var re = CTree<long, Domain>.Empty;
            var ns = BTree<string, (int, long?)>.Empty;
            var j = 0;
            for (var b = gs.First(); b != null; b = b.Next())
                if (b.value() is TGParam g && g.value != ""
                    && cx.obs[g.uid] is DBObject sn && !re.Contains(sn.defpos) && g.IsBound(cx) is null)
                {
                    var dr = sn.domain;
                    if (g.type.HasFlag(TGParam.Type.Type))
                        dr = Domain.Char;
                    if (g.type.HasFlag(TGParam.Type.Group) && dr.kind != Qlx.ARRAY)
                        dr = new Domain(-1L, Qlx.ARRAY, dr);
                    re += (sn.defpos, dr);
                    rt += sn.defpos;
                    if (g.value != null)
                        ns += (g.value, (j++, g.uid));
                }
            if (rt.Count == 0)
            {
                var rc = new SqlLiteral(cx.GetUid(), "Match", TBool.True);
                rt += rc.defpos;
                re += (rc.defpos, Domain.Bool);
            }
            var nd = new Domain(-1L, cx, Qlx.TABLE, re, rt, rt.Length) + (ObInfo.Names, ns)
                +(MatchStatement.GDefs,gs);
            var ers = new BindingRowSet(cx, cx.GetUid(), nd);
            return ((BindingRowSet)cx.Add(ers), ns);
        }
        (int, int) ParseMatchQuantifier()
        {
            var mm = tok;
            Next();
            int l = 0, h = -1; // -1 indicates unbounded
            switch (mm)
            {
                case Qlx.QMARK: h = 1; break;
                case Qlx.TIMES: break;
                case Qlx.PLUS: l = 1; break;
                case Qlx.LBRACE:
                    if (lxr.val is TInt tl)
                        l = (int)tl.value;
                    Mustbe(Qlx.INTEGERLITERAL);
                    Mustbe(Qlx.COMMA);
                    if (lxr.val is TInt th)
                        h = (int)th.value;
                    if (tok == Qlx.INTEGERLITERAL)
                        Next();
                    Mustbe(Qlx.RBRACE);
                    break;
            }
            return (l, h);
        }
        /// <summary>
        /// GET option here is Pyrrho shortcut, needs third syntax for ViewDefinition
        /// ViewDefinition = id [ViewSpecification] AS (QueryExpression|GET) {TableMetadata} .
        /// ViewSpecification = Cols 
        ///       | OF id 
        ///       | OF '(' id Type {',' id Type} ')' .
        /// </summary>
        /// <returns>maybe a RowSet</returns>
        internal RowSet? ParseViewDefinition(string? id = null)
        {
            var op = cx.parse;
            var lp = LexPos();
            var sl = lxr.start;
            if (id == null)
            {
                Next();
                id = lxr.val.ToString();
                Mustbe(Qlx.Id);
                if (cx.db.role.dbobjects.Contains(id) == true)
                    throw new DBException("42104", id);
            }
            // CREATE VIEW always creates a new Compiled object,
            // whose columns and datatype are recorded in the framing part.
            // For a normal view the columns are SqlCopies that refer to a derived table
            // to be defined in the AS CursorSpecification part of the syntax:
            // so that initially they will have the undefined Content datatype.
            // If it is a RestView the column datatypes are specified inline
            // and constitute a VirtualTable which will have a defining position
            // and maybe have associated VirtualIndexes also with defining positions.
            // In all cases there will be objects defined in the Framing: 
            // these accumulate naturally during parsing.
            // The usage of these framing objects is different:
            // normal views are always instanced, while restviews are not.
            Domain dm = Domain.TableType;
            cx.defs = Ident.Idents.Empty;
            var nst = cx.db.nextStmt;
            /*        BList<Physical> rest = null;  // if rest, virtual table and indexes */
            Table? us = null;  // Show the USING table of a RestViewUsing
            var ts = BTree<long, ObInfo>.Empty;
            if (Match(Qlx.LPAREN))
            {
                Next();
                for (var i = 0; ; i++)
                {
                    var n = lxr.val.ToString();
                    var np = LexPos();
                    Mustbe(Qlx.Id);
                    ts += (np.dp, new ObInfo(n));
                    if (Mustbe(Qlx.COMMA, Qlx.RPAREN) == Qlx.RPAREN)
                        break;
                }
            }
            else if (Match(Qlx.OF))
            {
                cx.parse = ExecuteStatus.Compile;
                Next();
                lp = LexPos();
                sl = lxr.start;
                if (Match(Qlx.LPAREN)) // inline type def (RestView only)
                    dm = (Domain)cx.Add(ParseRowTypeSpec(Qlx.VIEW));
                else
                {
                    var tn = lxr.val.ToString();
                    Mustbe(Qlx.Id);
                    dm = ((cx.db.role.dbobjects[tn] is long p) ? cx._Dom(p) : null) ??
                        throw new DBException("42119", tn, "").Mix();
                }
            }
            Mustbe(Qlx.AS);
            var rest = Match(Qlx.GET);
            RowSet? ur = null;
            RowSet? cs = null;
            if (!rest)
            {
                cx.parse = ExecuteStatus.Compile;
                cs = _ParseCursorSpecification(Domain.TableType);
                if (ts != BTree<long, ObInfo>.Empty)
                {
                    var ub = cs.rowType.First();
                    for (var b = ts.First(); b != null && ub != null; b = b.Next(), ub = ub.Next())
                        if (ub.value() is long u && cx.obs[u] is QlValue v && b.value().name is string nn)
                            cx.Add(v + (DBObject._Alias, nn));
                }
                cx.Add(new SelectStatement(cx.GetUid(), cs));
                cs = (RowSet?)cx.obs[cs.defpos] ?? throw new PEException("PE1802");
                var nb = ts.First();
                for (var b = cs.rowType.First(); b != null; b = b.Next(), nb = nb?.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue v)
                    {
                        if (v.domain.kind == Qlx.CONTENT || v.defpos < 0) // can't simply use WellDefined
                            throw new DBException("42112", v.NameFor(cx));
                        if (nb != null && nb.value().name is string bn)
                            cx.Add(v + (DBObject._Alias, bn));
                    }
                for (var b = cx.forReview.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue k)
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is RowSet rs && rs is not SelectRowSet)
                                rs.Apply(new BTree<long, object>(RowSet._Where, new CTree<long, bool>(k.defpos, true)), cx);
                cx.parse = op;
            }
            else
            {
                Next();
                if (Match(Qlx.USING))
                {
                    Next();
                    ur = ParseTableReferenceItem(lp.lp, Domain.TableType);
                    us = (Table?)cx.obs[ur.target] ?? throw new DBException("42107");
                }
            }
            PView? pv;
            if (rest)
            {
                if (us == null)
                    pv = new PRestView(id, dm, nst, cx.db.nextPos, cx);
                else
                    pv = new PRestView2(id, dm, nst,
                        ur ?? throw new PEException("PE2500"),
                        cx.db.nextPos, cx);
            }
            else
            {
                cx.Add(cs ?? throw new DBException("22204"));
                pv = new PView(id, new string(lxr.input, sl, lxr.pos - sl),
                    cs, nst, cx.db.nextPos, cx);
            }
            pv.framing = new Framing(cx, nst);
            var vw = (View)(cx.Add(pv) ?? throw new DBException("42105").Add(Qlx.VIEW));
            if (StartMetadata(Qlx.VIEW))
            {
                var m = ParseMetadata(Qlx.VIEW);
                if (vw != null && m != null)
                    cx.Add(new PMetadata(id, -1, vw, m, cx.db.nextPos));
            }
            cx.result = -1L;
            return cs; // cs is null for PRestViews
        }
        /// <summary>
        /// Parse the Create ordering syntax:
        /// FOR Domain_id (EQUALS|ORDER FULL) BY (STATE|(MAP|RELATIVE) WITH Func_id)  
        /// </summary>
        private void ParseCreateOrdering()
        {
            Next();
            Mustbe(Qlx.FOR);
            var n = new Ident(this);
            Mustbe(Qlx.Id);
            Domain ut = ((cx.role.dbobjects[n.ident] is long p) ? cx.db.objects[p] as Domain : null)
                ?? throw new DBException("42133", n).Mix();
            OrderCategory fl;
            if (Match(Qlx.EQUALS))
            {
                fl = OrderCategory.Equals;
                Next();
                Mustbe(Qlx.ONLY);
            }
            else
            {
                fl = OrderCategory.Full;
                Mustbe(Qlx.ORDER); Mustbe(Qlx.FULL);
            }
            Mustbe(Qlx.BY);
            Qlx smr = Mustbe(Qlx.STATE, Qlx.MAP, Qlx.RELATIVE);
            if (smr == Qlx.STATE)
            {
                fl |= OrderCategory.State;
                cx.Add(new Ordering(ut, -1L, fl, cx.db.nextPos, cx));
            }
            else
            {
                fl |= ((smr == Qlx.RELATIVE) ? OrderCategory.Relative : OrderCategory.Map);
                Mustbe(Qlx.WITH);
                var (fob, nf) = ParseObjectName();
                var func = fob as Procedure ?? throw new DBException("42000", "Adapter");
                if (smr == Qlx.RELATIVE && func.arity != 2)
                    throw new DBException("42154", nf).Mix();
                cx.Add(new Ordering(ut, func.defpos, fl, cx.db.nextPos, cx));
            }
        }
        /// <summary>
        /// Cols =		'('id { ',' id } ')'.
        /// </summary>
        /// <returns>a tree of Ident</returns>
        BList<Ident> ParseIDList()
        {
            bool b = (tok == Qlx.LPAREN);
            if (b)
                Next();
            var r = BList<Ident>.Empty;
            r += ParseIdent();
            while (tok == Qlx.COMMA)
            {
                Next();
                r += ParseIdent();
            }
            if (b)
                Mustbe(Qlx.RPAREN);
            return r;
        }
        /// <summary>
		/// Cols =		'('ColRef { ',' ColRef } ')'.
        /// </summary>
        /// <returns>a tree of coldefpos: returns null if input is (SELECT</returns>
		Domain? ParseColsList(Domain ob, Qlx s = Qlx.TABLE)
        {
            var r = BList<DBObject>.Empty;
            bool b = tok == Qlx.LPAREN;
            if (b)
                Next();
            if (tok == Qlx.SELECT)
                return null;
            r += ParseColRef(ob);
            while (tok == Qlx.COMMA)
            {
                Next();
                r += ParseColRef(ob);
            }
            if (b)
                Mustbe(Qlx.RPAREN);
            return new Domain(cx.GetUid(), cx, s, r, r.Length);
        }
        /// <summary>
        /// ColRef = id { '.' id } .
        /// </summary>
        /// <returns>+- seldefpos</returns>
        DBObject ParseColRef(DBObject ta)
        {
            if (tok == Qlx.PERIOD)
            {
                Next();
                var pn = lxr.val;
                Mustbe(Qlx.Id);
                var tb = ta as Table ?? throw new DBException("42162", pn).Mix();
                if (cx.db.objects[tb.applicationPS] is not PeriodDef pd || pd.NameFor(cx) != pn.ToString())
                    throw new DBException("42162", pn).Mix();
                return (PeriodDef)cx.Add(pd);
            }
            // We will raise an exception if the column does not exist
            var id = new Ident(this);
            var p = ta.ColFor(cx, id.ident);
            var tc = cx.obs[p] ?? cx.db.objects[p] as DBObject ??
                throw new DBException("42112", id.ident).Mix();
            Mustbe(Qlx.Id);
            // We will construct paths as required for any later components
            while (tok == Qlx.DOT)
            {
                Next();
                var pa = new Ident(this);
                Mustbe(Qlx.Id);
                if (tc is TableColumn c)
                    tc = new ColumnPath(pa.iix.dp, pa.ident, c, cx.db); // returns a (child)TableColumn for non-documents
                long dm = -1;
                if (tok == Qlx.AS)
                {
                    Next();
                    tc = (TableColumn)tc.New(cx.GetUid(), tc.mem + ParseDataType().mem);
                }
                if (cx.db.objects[ta.ColFor(cx, pa.ident)] is TableColumn cc
                    && cx.db is Transaction tr) // create a new path 
                    cx.Add(new PColumnPath(cc.defpos, pa.ToString(), dm, tr.nextPos, cx));
            }
            return cx.Add(tc);
        }
        /// <summary>
        /// id [UNDER id] AS Representation [ Method {',' Method} ] Metadata
		/// Representation = | StandardType 
        ///             | (' Member {',' Member }')' .
        /// </summary>
        void ParseTypeClause()
        {
            var typename = new Ident(lxr.val.ToString(), cx.Ix(lxr.start, cx.GetPos()));
            var supers = CTree<Domain,bool>.Empty;
            UDType dt = Domain.TypeSpec;
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            var pp = cx.db.nextPos;
            if (cx.tr == null)
                throw new DBException("2F003");
            Mustbe(Qlx.Id);
            if (cx.role.dbobjects.Contains(typename.ident))
                throw new DBException("42104", typename.ident);
            if (Match(Qlx.UNDER))
            while (tok==Qlx.UNDER||tok==Qlx.COMMA) // COMMA is allowed for graph types
            {
                Next();
                var undername = new Ident(this);
                if (!cx.role.dbobjects.Contains(undername.ident))
                    throw new DBException("42107", undername.ident);
                Mustbe(Qlx.Id, Qlx.NUMERIC, Qlx.INTEGER, Qlx.CHAR, Qlx.REAL);
                if ((cx.GetObject(undername.ident) ??
                    StandardDataType.Get(dt.kind) ??
                    throw cx.db.Exception("42119", undername).Pyrrho()) is Domain udm)
                    supers += ((udm as UDType) 
                        ?? (UDType)cx.Add(dt.New(udm.defpos, udm.mem + (DBObject._Domain, udm))), true);
            }
            if (tok == Qlx.AS)
            {
                Next();
                if (tok == Qlx.LPAREN)
                    dt = (UDType)ParseRowTypeSpec(dt.kind, typename, supers);
                else
                {
                    var d = ParseStandardDataType() ??
                        throw new DBException("42161", "StandardType", lxr.val.ToString()).Mix();
                    if (d == Domain.Null)
                        throw new DBException("42000", tok, " following AS");
                    dt = new UDType(d.defpos, d.mem);
                }
            }
            else
            {
                var dd = (UDType?)supers.First()?.key();
                if (dd is not null)
                    dd = dd.kind switch
                    {
                        Qlx.NODETYPE => Domain.NodeType,
                        Qlx.EDGETYPE => Domain.EdgeType,
                        _ => Domain.TypeSpec,
                    };
                dd ??= Domain.TypeSpec;
                var pt = (supers.Count <= 1L)?
                    new PType(typename.ident, dd, supers, -1L, cx.db.nextPos, cx)
                    :new PType2(typename.ident, dd, supers, -1L, cx.db.nextPos, cx);
                cx.Add(pt);
            }
            if (Match(Qlx.RDFLITERAL))
            {
                RdfLiteral rit = (RdfLiteral)lxr.val;
                dt += (ObInfo.Name, rit.val as string ?? "");
                Next();
            }
            // UNDER may have specified a node or edge type. If so, we change the 
            // user-defined type dt created by lines 1583/1597/1603 above into a NodeType or EdgeType
            // respectively.
            // (If it is still a UDType it may get changed to one of these
            // by metadata. We will deal with that if it happens below.)
            if (supers.First()?.key() is EdgeType e1)
            {
                if (dt is not EdgeType et)
                    et = new EdgeType(cx.GetUid(), typename.ident, Domain.EdgeType, 
                        new BTree<long, object>(Domain.Under, new CTree<Domain, bool>(e1, true)), cx);
                dt = et.NewNodeType(cx, typename.ident, 'V');
            }
            else if (supers.First()?.key() is NodeType n1)
            {
                if (dt is not NodeType nt)
                    nt = new NodeType(cx.GetUid(), typename.ident, Domain.NodeType, 
                        new BTree<long,object>(Domain.Under,new CTree<Domain,bool>(n1,true)), cx);
                dt = nt.NewNodeType(cx, typename.ident, 'V');
            }
            else
            {
                if (((Transaction)cx.db).physicals[pp] is PType qt && dt.framing.obs.Count > 0)
                {
                    qt.framing = dt.framing;
                    cx.Add(qt);
                }
                dt = (UDType)(cx.db.objects[pp] ?? Domain.TypeSpec);
            }
            // Ensure the context is aware of everything so far and check for method headers
            cx.Add(dt);
            MethodModes();
            if (Match(Qlx.OVERRIDING, Qlx.INSTANCE, Qlx.STATIC, Qlx.CONSTRUCTOR, Qlx.METHOD))
            {
                cx.obs = ObTree.Empty;
                dt = ParseMethodHeader(dt) ?? throw new PEException("PE42140");
                while (tok == Qlx.COMMA)
                {
                    Next();
                    cx.obs = ObTree.Empty;
                    dt = ParseMethodHeader(dt) ?? throw new PEException("PE42141");
                }
            }
            // it is a syntax error to add NODETPE/EDGETYPE metadata to a node type:
            // but it is okay to add edgetype metadata to an edgetype
            if ((dt is not NodeType || dt is EdgeType) && StartMetadata(Qlx.TYPE))
                ParseTypeMetadata(typename, dt);
            cx.parse = op;
        }
        void ParseTypeMetadata(Ident typename, UDType dt)
        {
            // BuildNodeType is also used for the CREATE NODE syntax, which is
            // part of the DML, and so any new properties added here are best prepared with
            // SqlValues/SqlLiterals instead of Domains/TableColumns.
            // We prepare a useful tree of all the columns we know about in case their names
            // occur in the metadata.
            var ls = CTree<string, QlValue>.Empty;
            var nn = CTree<string, long>.Empty;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is TableColumn tc
                        && tc.infos[cx.role.defpos] is ObInfo ti && ti.name is string cn)
                {
                    ls += (cn, new SqlLiteral(p, cn, tc.domain.defaultValue, tc.domain));
                    nn += (cn, tc.defpos);
                }
            var m = ParseMetadata(Qlx.TYPE);
            // The metadata contains aliases for special columns etc
                var ll = m[Qlx.RARROW] as TList?? new TList(Domain.TypeSpec,new TTypeSpec(Domain.NodeType));
                var al = m[Qlx.ARROW] as TList ?? new TList(Domain.TypeSpec, new TTypeSpec(Domain.NodeType));
            if (m.Contains(Qlx.NODETYPE) || ll?.Length*al?.Length>1)
            {
                var nt = new NodeType(typename.iix.dp, dt.mem + (Domain.Kind, Qlx.NODETYPE));
                nt = nt.FixNodeType(cx, typename);
                // Process ls and m 
                dt = nt.Build(cx, null, new BTree<long, object>(Domain.NodeTypes, nt.label.OnInsert(cx))+(GqlNode.DocValue,ls), m);
                ls = CTree<string, QlValue>.Empty;
                // and fix the PType to be a PNodeType
            }
            var odt = dt;
            if (m.Contains(Qlx.EDGETYPE))
            {
                if (((Transaction)cx.db).physicals[typename.iix.dp] is not PType pt)
                    throw new PEException("PE50501");
                var np = (dt is NodeType)?cx.db.nextPos:dt.defpos;
                var un = CTree<Domain, bool>.Empty;
                if (dt is NodeType)
                    un += (dt, true);
                for (var bl = ll?.list.First(); bl != null; bl = bl.Next())
                    for (var ba = al?.list.First(); ba != null; ba = ba.Next())
                        if (bl.value() is TypedValue ln && ba.value() is TypedValue an)
                        {
                            m += (Qlx.RARROW, ln);
                            m += (Qlx.ARROW, an);
                            var lv = cx.role.dbobjects[ln.ToString()]??Domain.NodeType.defpos;
                            var av = cx.role.dbobjects[an.ToString()]??Domain.NodeType.defpos;
                            dt = odt;
                            // try to find a specific edgeType for this combination
                            var d = cx.db.objects[cx.role.edgeTypes[typename.ident]??-1L] as EdgeType;
                            if (cx.db.objects[cx.db.edgeEnds[d?.defpos??-1L]?[lv]?[av]??-1L] is not EdgeType et)
                            {
                                var pe = new PEdgeType(typename.ident, Domain.EdgeType, un, -1L, lv, av, np, cx);
                                pt = pe;
                                et = new EdgeType(np, typename.ident, dt, new BTree<long, object>(Domain.Under, un), cx, m);
                                pt.dataType = et;
                            }
                            et = et.FixEdgeType(cx,pt);
                            dt = et.Build(cx, null, new BTree<long,object>(Domain.NodeTypes,et.label.OnInsert(cx))
                                +(GqlNode.DocValue, ls)+(EdgeType.LeavingType,lv)+(EdgeType.ArrivingType,av), m);
                            np = cx.db.nextPos;
                        }
                        else throw new PEException("PE60703");
            }
            else if (m != CTree<Qlx, TypedValue>.Empty)
                cx.Add(new PMetadata(typename.ident, -1, dt, m, cx.db.nextPos));
        }
        /// <summary>
        /// Method =  	MethodType METHOD id '(' Parameters ')' [RETURNS Type] [FOR id].
        /// MethodType = 	[ OVERRIDING | INSTANCE | STATIC | CONSTRUCTOR ] .
        /// </summary>
        /// <returns>the methodname parse class</returns>
        MethodName ParseMethod(Domain? xp = null)
        {
            MethodName mn = new()
            {
                methodType = PMethod.MethodType.Instance
            };
            switch (tok)
            {
                case Qlx.OVERRIDING: Next(); mn.methodType = PMethod.MethodType.Overriding; break;
                case Qlx.INSTANCE: Next(); break;
                case Qlx.STATIC: Next(); mn.methodType = PMethod.MethodType.Static; break;
                case Qlx.CONSTRUCTOR: Next(); mn.methodType = PMethod.MethodType.Constructor; break;
            }
            Mustbe(Qlx.METHOD);
            mn.name = new Ident(lxr.val.ToString(), cx.Ix(cx.db.nextPos));
            Mustbe(Qlx.Id);
            int st = lxr.start;
            if (mn.name is not Ident nm)
                throw new DBException("42000", "Method name");
            mn.ins = ParseParameters(mn.name, xp);
            mn.mname = new Ident(nm.ident, nm.iix);
            for (var b = mn.ins.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var pa = (FormalParameter?)cx.obs[p] ?? throw new PEException("PE1621");
                    cx.defs += (new Ident(mn.mname, new Ident(pa.name ?? "", new Iix(pa.defpos))),
                        mn.mname.iix);
                }
            mn.retType = ParseReturnsClause(mn.mname);
            mn.signature = new string(lxr.input, st, lxr.start - st);
            if (tok == Qlx.FOR)
            {
                Next();
                var tname = new Ident(this);
                var ttok = tok;
                Mustbe(Qlx.Id, Qlx.NUMERIC, Qlx.CHAR, Qlx.INTEGER, Qlx.REAL);
                xp = (Domain?)cx.db.objects[cx.role.dbobjects[tname.ident] ?? -1L] ??
                    StandardDataType.Get(ttok);
            }
            else if (mn.methodType == PMethod.MethodType.Constructor)
                xp = (Domain?)cx.db.objects[cx.role.dbobjects[mn.name.ident] ?? -1L];
            mn.type = xp as UDType ?? throw new DBException("42000", "Constructor?").ISO();
            return mn;
        }
        /// <summary>
        /// Define a new method header (calls ParseMethod)
        /// </summary>
        /// <param name="xp">the UDTtype if we are creating a Type</param>
        /// <returns>maybe a type</returns>
		UDType? ParseMethodHeader(Domain? xp = null)
        {
            MethodName mn = ParseMethod(xp);
            if (mn.name is not Ident nm || mn.retType == null || mn.type == null)
                throw new DBException("42000", "Method header");
            var r = new PMethod(nm.ident, mn.ins,
                mn.retType, mn.methodType, mn.type, null,
                new Ident(mn.signature, nm.iix), cx.db.nextStmt, cx.db.nextPos, cx);
            cx.Add(r);
            return (xp is not null) ? (UDType?)cx._Ob(xp.defpos) : null;
        }
        /// <summary>
        /// Create a method body (called when parsing CREATE METHOD or ALTER METHOD)
        /// </summary>
        /// <returns>the executable</returns>
		void ParseMethodDefinition()
        {
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            MethodName mn = ParseMethod(); // don't want to create new things for header
            if (mn.name is not Ident nm || mn.retType == null || mn.type == null)
                throw new DBException("42000", "Method def");
            var ut = mn.type;
            var oi = ut.infos[cx.role.defpos];
            var meth = cx.db.objects[oi?.methodInfos[nm.ident]?[Context.Signature(mn.ins)] ?? -1L] as Method ??
    throw new DBException("42132", nm.ToString(), oi?.name ?? "??").Mix();
            var lp = LexPos();
            int st = lxr.start;
            var nst = cx.db.nextStmt;
            cx.obs = ObTree.Empty;
            cx.Add(meth.framing); // for formals from meth
                                  //            var nst = meth.framing.obs.First()?.key()??cx.db.nextStmt;
            cx.defs = Ident.Idents.Empty;
            for (var b = meth.ins.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var pa = (FormalParameter?)cx.obs[p] ?? throw new DBException("42000", "Method");
                    var px = cx.Ix(pa.defpos);
                    cx.defs += (new Ident(pa.name ?? "", Iix.None), px);
                }
            ut.Defs(cx);
            meth += (Procedure.Body,
                (ParseStatement(mn.retType,true) ?? throw new DBException("42000", "MethodBody")).defpos);
            Ident ss = new(new string(lxr.input, st, lxr.start - st), lp);
            cx.parse = op;
            // we really should check the signature here
            var md = new Modify(meth.defpos, meth, ss, nst, cx.db.nextPos, cx);
            cx.Add(md);
            cx.result = -1L;
        }
        /// <summary>
        /// DomainDefinition = id [AS] StandardType [DEFAULT TypedValue] { CheckConstraint } Collate.
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        void ParseDomainDefinition()
        {
            var colname = new Ident(this);
            Mustbe(Qlx.Id);
            if (tok == Qlx.AS)
                Next();
            var type = ParseDataType();
            if (Match(Qlx.DEFAULT))
            {
                Next();
                int st = lxr.start;
                var dv = ParseSqlValue(type);
                Next();
                var ds = new string(lxr.input, st, lxr.start - st);
                type = type + (Domain.Default, dv) + (Domain.DefaultString, ds);
            }
            PDomain pd;
            if (type.name != "")
                pd = new PDomain1(colname.ident, type, cx.db.nextPos, cx);
            else
                pd = new PDomain(colname.ident, type, cx.db.nextPos, cx);
            var a = new List<Physical>();
            while (Match(Qlx.NOT, Qlx.CONSTRAINT, Qlx.CHECK))
                if (ParseCheckConstraint(pd.dataType) is PCheck ck)
                    a.Add(ck);
            if (Match(Qlx.COLLATE))
                pd.domain += (Domain.Culture, new CultureInfo(ParseCollate()));
            cx.Add(pd);
        }
        /// <summary>
        /// Parse a collation indication
        /// </summary>
        /// <returns>The collation name</returns>
        string ParseCollate()
        {
            Next();
            var collate = lxr.val;
            Mustbe(Qlx.Id);
            return collate.ToString();
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] QlValue ')' .
        /// </summary>
        /// <param name="pd">the domain</param>
        /// <returns>the PCheck object resulting from the parse</returns>
        PCheck? ParseCheckConstraint(Domain dm)
        {
            var oc = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            var o = new Ident(this);
            Ident n;
            if (Match(Qlx.CONSTRAINT))
            {
                Next();
                n = o;
                Mustbe(Qlx.Id);
            }
            else
                n = new Ident(this);
            Mustbe(Qlx.CHECK);
            Mustbe(Qlx.LPAREN);
            var nst = cx.db.nextStmt;
            var st = lxr.start;
            var se = ParseSqlValue(Domain.Bool).Reify(cx);
            se.Validate(cx);
            Mustbe(Qlx.RPAREN);
            var pc = new PCheck(dm, n.ident, se,
                    new string(lxr.input, st, lxr.start - st), nst, cx.db.nextPos, cx);
            cx.parse = oc;
            return pc;
        }

        /// <summary>
        /// id TableContents [UriType] [Classification] [Enforcement] {Metadata} 
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        void ParseCreateTable()
        {
            var name = new Ident(this);
            Mustbe(Qlx.Id);
            if (cx.db.schema.dbobjects.Contains(name.ident) || cx.role.dbobjects.Contains(name.ident))
                throw new DBException("42104", name);
            var pt = new PTable(name.ident, new Domain(-1L, cx, Qlx.TABLE, BList<DBObject>.Empty),
                cx.db.nextPos, cx);
            var tb = cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.TABLE);
            tb = ParseTableContentsSource((Table)tb);
            if (tok == Qlx.RDFLITERAL &&
                lxr.val is RdfLiteral rit && rit.val is string ri)
            {
                tb += (ObInfo.Name, ri);
                tb = (Table)cx.Add(tb); // FIX this
                Next();
            }
            if (Match(Qlx.SECURITY))
            {
                Next();
                if (Match(Qlx.LEVEL))
                    tb = ParseClassification(tb);
                if (Match(Qlx.SCOPE))
                    tb = ParseEnforcement((Table)tb);
            }
            if (StartMetadata(Qlx.TABLE))
            {
                var dp = LexDp();
                var md = ParseMetadata(Qlx.TABLE);
                cx.Add(new PMetadata(name.ident, dp, tb, md, cx.db.nextPos));
            }
        }
        DBObject ParseClassification(DBObject ob)
        {
            var lv = MustBeLevel();
            var pc = new Classify(ob.defpos, lv, cx.db.nextPos);
            ob = cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.SECURITY);
            return ob;
        }
        /// <summary>
        /// Enforcement = SCOPE [READ] [INSERT] [UPDATE] [DELETE]
        /// </summary>
        /// <returns></returns>
        Table ParseEnforcement(Table tb)
        {
            if (cx.db is null || cx.db.user is null || cx.db.user.defpos != cx.db.owner)
                throw new DBException("42105").Add(Qlx.ENFORCED);
            Mustbe(Qlx.SCOPE);
            Grant.Privilege r = Grant.Privilege.NoPrivilege;
            while (Match(Qlx.READ, Qlx.INSERT, Qlx.UPDATE, Qlx.DELETE))
            {
                switch (tok)
                {
                    case Qlx.READ: r |= Grant.Privilege.Select; break;
                    case Qlx.INSERT: r |= Grant.Privilege.Insert; break;
                    case Qlx.UPDATE: r |= Grant.Privilege.Update; break;
                    case Qlx.DELETE: r |= Grant.Privilege.Delete; break;
                }
                Next();
            }
            var pe = new Enforcement(tb, r, cx.db.nextPos);
            tb = (Table)(cx.Add(pe) ?? throw new DBException("42105").Add(Qlx.ENFORCED));
            return tb;
        }
        /// <summary>
        /// TebleContents = '(' TableClause {',' TableClause } ')' { VersioningClause }
        /// | OF Type_id ['(' TypedTableElement { ',' TypedTableElement } ')'] .
        /// VersioningClause = WITH (SYSTEM|APPLICATION) VERSIONING .
        /// </summary>
        /// <param name="ta">The newly defined Table</param>
        /// <returns>The updated Table</returns>
        Table ParseTableContentsSource(Table tb)
        {
            switch (tok)
            {
                case Qlx.LPAREN:
                    Next();
                    tb = ParseTableItem(tb);
                    while (tok == Qlx.COMMA)
                    {
                        Next();
                        tb = ParseTableItem(tb);
                    }
                    Mustbe(Qlx.RPAREN);
                    while (Match(Qlx.WITH))
                        tb = ParseVersioningClause(tb, false);
                    break;
                case Qlx.OF:
                    {
                        Next();
                        var id = ParseIdent();
                        var udt = cx.db.objects[cx.role.dbobjects[id.ident] ?? -1L] as Domain ??
                            throw new DBException("42133", id.ToString()).Mix();
                        var tr = cx.db as Transaction ?? throw new DBException("2F003");
                        for (var cd = udt.rowType.First(); cd != null; cd = cd.Next())
                            if (cd.value() is long p && cx.db.objects[p] is TableColumn tc &&
                                tc.infos[cx.role.defpos] is ObInfo ci && ci.name is not null)
                            {
                                var pc = new PColumn2(tb, ci.name, cd.key(), tc.domain,
                                    tc.generated.gfs ?? tc.domain.defaultValue?.ToString() ?? "",
                                    tc.domain.defaultValue ?? TNull.Value, tc.domain.notNull,
                                    tc.generated, tr.nextPos, cx);
                                tb = (Table?)cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN);
                            }
                        if (Match(Qlx.LPAREN))
                        {
                            for (; ; )
                            {
                                Next();
                                if (Match(Qlx.UNIQUE, Qlx.PRIMARY, Qlx.FOREIGN))
                                    tb = ParseTableConstraintDefin(tb);
                                else
                                {
                                    id = ParseIdent();
                                    var se = (TableColumn?)cx.db.objects[udt.ColFor(cx, id.ident)]
                                        ?? throw new DBException("42112", id.ident);
                                    ParseColumnOptions(tb, se);
                                    tb = (Table?)cx.obs[tb.defpos] ?? throw new PEException("PE1711");
                                }
                                if (!Match(Qlx.COMMA))
                                    break;
                            }
                            Mustbe(Qlx.RPAREN);
                        }
                        break;
                    }
                default: throw new DBException("42161", "(, AS, or OF", tok.ToString()).Mix();
            }
            return tb;
        }
        /// <summary>
        /// Parse the table versioning clause:
        /// (SYSTEM|APPLICATION) VERSIONING
        /// </summary>
        /// <param name="ta">the table</param>
        /// <param name="drop">whether we are dropping an existing versioning</param>
        /// <returns>the updated Table object</returns>
        private Table ParseVersioningClause(Table tb, bool drop)
        {
            Next();
            var sa = tok;
            Mustbe(Qlx.SYSTEM, Qlx.APPLICATION);
            var vi = (sa == Qlx.SYSTEM) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var pi = tb.FindPrimaryIndex(cx) ?? throw new DBException("42000", "PrimaryIndex");
            if (drop)
            {
                var fl = (vi == 0) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
                Physical? ph = null;
                for (var e = tb.indexes.First(); e != null; e = e.Next())
                    for (var c = e.value().First(); c != null; c = c.Next())
                        if (cx.db.objects[c.key()] is Level3.Index px &&
                                    px.tabledefpos == tb.defpos && (px.flags & fl) == fl)
                            ph = new Drop(px.defpos, tr.nextPos);
                if (sa == Qlx.SYSTEM && cx.db.objects[tb.systemPS] is PeriodDef pd)
                    ph = new Drop(pd.defpos, tr.nextPos);
                Mustbe(Qlx.VERSIONING);
                if (ph is not null)
                    tb = (Table)(cx.Add(ph) ?? throw new DBException("42105").Add(Qlx.TABLE));
                return tb;
            }
            var ti = tb.infos[cx.role.defpos];
            if (ti == null || sa == Qlx.APPLICATION)
                throw new DBException("42164", tb.NameFor(cx)).Mix();
            pi = (Level3.Index)(cx.Add(new PIndex("", tb, pi.keys,
                    PIndex.ConstraintType.PrimaryKey | vi, -1L, tr.nextPos))
                ?? throw new DBException("42105").Add(Qlx.PRIMARY));
            Mustbe(Qlx.VERSIONING);
            var ixs = tb.indexes;
            var iks = ixs[pi.keys] ?? CTree<long, bool>.Empty;
            iks += (pi.defpos, true);
            ixs += (pi.keys, iks);
            return (Table)cx.Add(tb + (Table.Indexes, tb.indexes + ixs));
        }
        /// <summary>
        /// TypedTableElement = id WITH OPTIONS '(' ColumnOption {',' ColumnOption} ')'.
        /// ColumnOption = (SCOPE id)|(DEFAULT TypedValue)|ColumnConstraint .
        /// </summary>
        /// <param name="tb">The table being created</param>
        /// <param name="tc">The column being optioned</param>
        void ParseColumnOptions(Table tb, TableColumn tc)
        {
            Mustbe(Qlx.WITH);
            Mustbe(Qlx.OPTIONS);
            Mustbe(Qlx.LPAREN);
            for (; ; )
            {
                switch (tok)
                {
                    case Qlx.SCOPE:
                        Next();
                        Mustbe(Qlx.Id); // TBD
                        break;
                    case Qlx.DEFAULT:
                        {
                            Next();
                            int st = lxr.start;
                            var dv = ParseSqlValue(tc.domain);
                            var ds = new string(lxr.input, st, lxr.start - st);
                            tc = tc + (Domain.Default, dv) + (Domain.DefaultString, ds);
                            cx.db += tc;
                            break;
                        }
                    default:
                        tb = ParseColumnConstraint(tb, tc);
                        break;
                }
                if (Match(Qlx.COMMA))
                    Next();
                else
                    break;
            }
            Mustbe(Qlx.RPAREN);
        }
        /// <summary>
        /// TableClause =	ColumnDefinition | TableConstraintDef | TablePeriodDefinition .
        /// </summary>
        Table ParseTableItem(Table tb)
        {
            Match(Qlx.PRIMARY, Qlx.UNIQUE, Qlx.FOREIGN, Qlx.CHECK);
            if (Match(Qlx.PERIOD))
                tb = AddTablePeriodDefinition(tb);
            else if (tok == Qlx.Id)
                tb = ParseColumnDefin(tb);
            else
                tb = ParseTableConstraintDefin(tb);
            return tb;
        }
        /// <summary>
        /// TablePeriodDefinition = PERIOD FOR PeriodName '(' id, id ')' .
        /// PeriodName = SYSTEM_TIME | id .
        /// </summary>
        /// <returns>the TablePeriodDefinition</returns>
        TablePeriodDefinition ParseTablePeriodDefinition()
        {
            var r = new TablePeriodDefinition();
            Next();
            Mustbe(Qlx.FOR);
            if (Match(Qlx.SYSTEM_TIME))
                Next();
            else
            {
                r.periodname = new Ident(this);
                Mustbe(Qlx.Id);
                r.pkind = Qlx.APPLICATION;
            }
            Mustbe(Qlx.LPAREN);
            r.col1 = ParseIdent();
            Mustbe(Qlx.COMMA);
            r.col2 = ParseIdent();
            Mustbe(Qlx.RPAREN);
            return r;
        }
        /// <summary>
        /// Add columns for table period definition
        /// </summary>
        /// <param name="tb"></param>
        /// <returns>the updated table</returns>
        Table AddTablePeriodDefinition(Table tb)
        {
            var ptd = ParseTablePeriodDefinition();
            if (ptd.col1 == null || ptd.col2 == null)
                throw new DBException("42105").Add(Qlx.COLUMN);
            var c1 = (TableColumn?)cx.db.objects[tb.ColFor(cx, ptd.col1.ident)];
            var c2 = (TableColumn?)cx.db.objects[tb.ColFor(cx, ptd.col2.ident)];
            if (c1 is null)
                throw new DBException("42112", ptd.col1).Mix();
            if (c1.domain.kind != Qlx.DATE && c1.domain.kind != Qlx.TIMESTAMP)
                throw new DBException("22005R", "DATE or TIMESTAMP", c1.ToString()).ISO()
                    .AddType(Domain.UnionDate).AddValue(c1.domain);
            if (c2 == null)
                throw new DBException("42112", ptd.col2).Mix();
            if (c2.domain.kind != Qlx.DATE && c2.domain.kind != Qlx.TIMESTAMP)
                throw new DBException("22005R", "DATE or TIMESTAMP", c2.ToString()).ISO()
                    .AddType(Domain.UnionDate).AddValue(c1.domain);
            if (c1.domain.CompareTo(c2) != 0)
                throw new DBException("22005S", c1.ToString(), c2.ToString()).ISO()
                    .AddType(c1.domain).AddValue(c2.domain);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var pd = new PPeriodDef(tb, ptd.periodname.ident, c1.defpos, c2.defpos, tr.nextPos, cx);
            return (Table)(cx.Add(pd) ?? throw new DBException("42105").Add(Qlx.PERIOD));
        }
        /// <summary>
		/// ColumnDefinition = id Type [DEFAULT TypedValue] {ColumnConstraint|CheckConstraint} Collate {Metadata}
		/// |	id Type GENERATED ALWAYS AS '('Value')'
        /// |   id Type GENERATED ALWAYS AS ROW (START|NEXT|END).
        /// Type = ...|	Type ARRAY
        /// |	Type MULTISET 
        /// </summary>
        /// <param name="tb">the table</param>
        /// <returns>the updated table</returns>
        Table ParseColumnDefin(Table tb)
        {
            var type = Domain.Null;
            var dom = Domain.Null;
            if (Match(Qlx.COLUMN))
                Next();
            var colname = new Ident(this);
            var lp = colname.iix;
            Mustbe(Qlx.Id);
            if (StartDataType())
                type = ParseDataType();
            else if (tok == Qlx.Id)
            {
                var op = cx.db.role.dbobjects[new Ident(this).ident];
                type = cx.db.objects[op ?? -1L] as Domain
                    ?? throw new DBException("42119", lxr.val.ToString());
                Next();
            }
            dom = type;
            if (Match(Qlx.ARRAY, Qlx.SET, Qlx.MULTISET))
            {
                dom = (Domain)cx.Add(new Domain(cx.GetUid(), tok, type));
                cx.Add(dom);
                Next();
            }
            var ua = CTree<UpdateAssignment, bool>.Empty;
            var gr = GenerationRule.None;
            var dfs = "";
            var nst = cx.db.nextStmt;
            TypedValue dv = TNull.Value;
            if (Match(Qlx.DEFAULT))
            {
                Next();
                int st = lxr.start;
                dv = lxr.val;
                dfs = new string(lxr.input, st, lxr.pos - st-1);
                Next();
            }
            else if (Match(Qlx.GENERATED))
            {
                dv = dom.defaultValue ?? TNull.Value;
                // Set up the information for parsing the generation rule
                // The table domain and cx.defs should contain the columns so far defined
                var oc = cx;
                cx = cx.ForConstraintParse();
                gr = ParseGenerationRule(lp.dp, dom);
                dfs = gr.gfs;
                oc.DoneCompileParse(cx);
                cx = oc;
            }
            if (dom == null)
                throw new DBException("42120", colname.ident);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var pc = new PColumn3(tb, colname.ident, -1, dom,
                dfs, dv, "", ua, false, gr, PColumn.GraphFlags.None, -1L, -1L, tr.nextPos, cx);
            cx.Add(pc);
            tb = (Table?)cx.obs[tb.defpos] ?? tb;
            var tc = (TableColumn)(cx.db.objects[pc.defpos] ?? throw new PEException("PE50100"));
            if (gr.exp >= 0)
                tc = cx.Modify(pc, tc);
            while (Match(Qlx.NOT, Qlx.REFERENCES, Qlx.CHECK, Qlx.UNIQUE, Qlx.PRIMARY, Qlx.CONSTRAINT,
                Qlx.SECURITY))
            {
                var oc = cx;
                nst = cx.db.nextStmt;
                cx = cx.ForConstraintParse();
                tb = ParseColumnConstraintDefin(tb, pc, tc);
                tb = (Table?)cx.obs[tb.defpos] ?? throw new PEException("PE1570");
                oc.DoneCompileParse(cx);
                cx = oc;
                tc = cx.Modify(pc, (TableColumn)(cx.obs[tc.defpos] ?? throw new PEException("PE15070")));
                tb = cx.Modify(tb, nst); // maybe the framing changes since nst are irrelevant??
            }
            var od = dom;
            if (type != null && Match(Qlx.COLLATE))
                tc += (Domain.Culture, new CultureInfo(ParseCollate()));
            if (StartMetadata(Qlx.TYPE) && cx.NameFor(tc.defpos) is string tn)
            {
                var md = ParseMetadata(Qlx.TYPE);
                var pm = new PMetadata(tn, 0, tc, md, cx.db.nextPos);
                cx.Add(pm);
                var oi = tc.infos[cx.role.defpos] ?? new ObInfo(tn);
                oi += (ObInfo._Metadata, md);
                tc += (DBObject.Infos, tc.infos + (cx.role.defpos, oi));
            }
            if (dom != od)
            {
                var nd = (Domain)dom.Relocate(cx.GetUid());
                if (nd is EdgeType ne && nd.defpos != dom.defpos)
                    ne.Fix(cx);
                dom = nd;
                cx.Add(dom);
                tc += (DBObject._Domain, dom);
                cx.Add(tc);
            }
            return tb;
        }
        /// <summary>
        /// Detect start of TableMetadata or ColumnMetatdata
        /// TableMetadata = ENTITY | PIE | HISTGORAM | LEGEND | LINE | POINTS | REFERRED |string | iri
        /// ColumnMetadata = ATTRIBUTE | X | Y | CAPTION |string | iri | REFERS
        /// </summary>
        /// <param name="kind">the kind of object</param>
        /// <returns>wheteher metadata follows</returns>
        bool StartMetadata(Qlx kind)
        {
            switch (kind)
            {
                case Qlx.TABLE:
                    return Match(Qlx.ENTITY, Qlx.PIE, Qlx.HISTOGRAM, Qlx.LEGEND, Qlx.LINE,
                    Qlx.NODE, Qlx.VERTEX, Qlx.POINTS, Qlx.DROP, Qlx.JSON, Qlx.CSV, Qlx.CHARLITERAL, Qlx.RDFLITERAL,
                    Qlx.REFERRED, Qlx.ETAG, Qlx.SECURITY);
                case Qlx.COLUMN:
                    return Match(Qlx.ATTRIBUTE, Qlx.X, Qlx.Y, Qlx.CAPTION, Qlx.DROP,
                    Qlx.CHARLITERAL, Qlx.RDFLITERAL, Qlx.REFERS, Qlx.SECURITY, Qlx.MULTIPLICITY);
                case Qlx.FUNCTION:
                    return Match(Qlx.ENTITY, Qlx.PIE, Qlx.HISTOGRAM, Qlx.LEGEND,
                    Qlx.LINE, Qlx.POINTS, Qlx.DROP, Qlx.JSON, Qlx.CSV, Qlx.INVERTS, Qlx.MONOTONIC);
                case Qlx.VIEW:
                    return Match(Qlx.ENTITY, Qlx.URL, Qlx.MIME, Qlx.SQLAGENT, Qlx.USER,
                    Qlx.PASSWORD, Qlx.CHARLITERAL, Qlx.RDFLITERAL, Qlx.ETAG, Qlx.MILLI);
                case Qlx.TYPE:
                    return Match(Qlx.PREFIX, Qlx.SUFFIX, Qlx.NODETYPE, Qlx.EDGETYPE, Qlx.SCHEMA,
                    Qlx.SENSITIVE, Qlx.CARDINALITY, Qlx.MULTIPLICITY);
                case Qlx.ANY:
                    Match(Qlx.DESC, Qlx.URL, Qlx.MIME, Qlx.SQLAGENT, Qlx.USER, Qlx.PASSWORD,
                        Qlx.ENTITY, Qlx.PIE, Qlx.HISTOGRAM, Qlx.LEGEND, Qlx.LINE, Qlx.POINTS, Qlx.REFERRED,
                        Qlx.ETAG, Qlx.ATTRIBUTE, Qlx.X, Qlx.Y, Qlx.CAPTION, Qlx.REFERS, Qlx.JSON, Qlx.CSV,
                        Qlx.INVERTS, Qlx.MONOTONIC, Qlx.PREFIX, Qlx.SUFFIX);
                    return !Match(Qlx.EOF, Qlx.RPAREN, Qlx.COMMA, Qlx.RBRACK, Qlx.RBRACE);
                default: return Match(Qlx.CHARLITERAL, Qlx.RDFLITERAL);
            }
        }
        internal CTree<Qlx, TypedValue> ParseMetadata(string s, int off, Qlx kind)
        {
            lxr = new Lexer(cx, s, off);
            return ParseMetadata(kind);
        }
        /// <summary>
        /// Parse ([ADD]|DROP) Metadata
        /// </summary>
        /// <param name="tr">the database</param>
        /// <param name="ob">the object the metadata is for</param>
        /// <param name="kind">the metadata</param>
        internal CTree<Qlx, TypedValue> ParseMetadata(Qlx kind)
        {
            var drop = false;
            if (Match(Qlx.ADD, Qlx.DROP))
            {
                drop = tok == Qlx.DROP;
                Next();
            }
            var m = CTree<Qlx, TypedValue>.Empty;
            TypedValue ds = TNull.Value;
            TypedValue iri = TNull.Value;
            long iv = -1;
            var lp = tok == Qlx.LPAREN;
            if (lp)
                Next();
            while (StartMetadata(kind))
            {
                var o = lxr.val;
                switch (tok)
                {
                    case Qlx.CHARLITERAL:
                        ds = drop ? new TChar("") : o;
                        break;
                    case Qlx.INVERTS:
                        {
                            Next();
                            if (tok == Qlx.EQL)
                                Next();
                            var x = cx.GetObject(o.ToString()) ??
                                throw new DBException("42108", lxr.val.ToString()).Pyrrho();
                            iv = x.defpos;
                            break;
                        }
                    case Qlx.DESC:
                    case Qlx.URL:
                        {
                            if (drop)
                                break;
                            var t = tok;
                            Next();
                            if (tok == Qlx.EQL)
                                Next();
                            if (tok == Qlx.CHARLITERAL || tok == Qlx.RDFLITERAL)
                                m += (t, lxr.val);
                            break;
                        }
                    case Qlx.HISTOGRAM:
                    case Qlx.LINE:
                    case Qlx.PIE:
                    case Qlx.POINTS:
                        {
                            if (drop)
                                o = new TChar("");
                            m += (tok, o);
                            Next();
                            if (tok == Qlx.LPAREN)
                            {
                                Next();
                                m += (Qlx.X, lxr.val);
                                Mustbe(Qlx.Id);
                                Mustbe(Qlx.COMMA);
                                m += (Qlx.Y, lxr.val);
                                Mustbe(Qlx.Id);
                            }
                            else
                                continue;
                            break;
                        }
                    case Qlx.VERTEX:
                    case Qlx.NODE:
                        {
                            m += (tok, o);
                            Next();
                            break;
                        }
                    case Qlx.PREFIX:
                    case Qlx.SUFFIX:
                        {
                            var tk = tok;
                            Next();
                            if (drop)
                                o = new TChar("");
                            else
                            {
                                o = lxr.val;
                                Mustbe(Qlx.Id);
                            }
                            m += (tk, o);
                            break;
                        }
                    case Qlx.RDFLITERAL:
                        Next();
                        iri = drop ? new TChar("") : lxr.val;
                        break;
                    // NODETYPE ['(' Identity_id ')']
                    // + NODETYPE  + NODE=id
                    case Qlx.NODETYPE:
                        {
                            m += (tok, o);
                            Next();
                            if (tok == Qlx.LPAREN)
                            {
                                Next();
                                var id = lxr.val;
                                Mustbe(Qlx.Id);
                                m += (Qlx.NODE, id);
                                if (Match(Qlx.CHARACTER, Qlx.CHAR, Qlx.VARCHAR, Qlx.STRING))
                                {
                                    Next();
                                    m += (Qlx.CHAR, TChar.Empty);
                                }
                                Mustbe(Qlx.RPAREN);
                            }
                            break;
                        }
                    // EDGETYPE   '('[(IdCol_id [CHAR|STRING])':')                              // ^1 ^2
                    //                                   //  md=md+ EDGETYPE + ?1EDGE=id + ?2CHAR
                    //                 [Leaving_id '=']  NodeType_id {'|' NodeType_id} [SET]',' // ^3 ^4 ^5        
                    //                                   // md=md+ ?3LPAREN=id + ^4RARROW=TList(id) + ?5ARROWBASE
                    //                 [Arriving_id '='] NodeType_id {'|' NodeType_id} [SET]')' // ^6 ^7 ^8
                    //                                   // md=md+ ?6RPAREN=id + ^7ARROW=TList(id) + ?8RARROWBASE
                    case Qlx.EDGETYPE:
                        {
                            m += (tok, o);
                            Next();
                            if (tok == Qlx.LPAREN)
                            {
                                Next();
                                var ll = new TList(Domain.Char);
                                var ln = lxr.val; // ^1 ^3 or ^4, don't know yet
                                Mustbe(Qlx.Id);
                                if (tok != Qlx.EQL) // ln is ^1 or ^3
                                {
                                    if (Match(Qlx.CHAR,Qlx.STRING))
                                    {
                                        Next();
                                        m += (Qlx.CHAR, TChar.Empty); // ^2
                                    }
                                    if (tok == Qlx.COLON) // ln is ^1
                                    {
                                        Next();
                                        m += (Qlx.EDGE, ln); // ^1
                                        ln = lxr.val;  // ln is ^3 or ^4
                                        Mustbe(Qlx.Id);
                                    }
                                }
                                if (tok == Qlx.EQL) // ln is ^3
                                {
                                    Next();
                                    m += (Qlx.LPAREN, ln); //^3
                                    ln = lxr.val;  // ln is ^4
                                    Mustbe(Qlx.Id);
                                }
                                while (true) // ln is ^4
                                {
                                    if (!cx.role.dbobjects.Contains(ln.ToString()))
                                        throw new DBException("42161", "NodeType", ln);
                                    ll += ln;
                                    if (tok != Qlx.VBAR)
                                        break;
                                    Next();
                                    ln = lxr.val; // ^+4
                                    Mustbe(Qlx.Id);
                                }
                                if (tok == Qlx.SET) // ^5
                                {
                                    m += (Qlx.ARROWBASE, TChar.Empty); // ^5
                                    Next();
                                }
                                m += (Qlx.RARROW, ll); // ^4
                                Mustbe(Qlx.COMMA);
                                var al = new TList(Domain.Char);
                                var an = lxr.val;  // an is ^5 or ^6
                                Mustbe(Qlx.Id);
                                if (tok == Qlx.EQL)  // ^5
                                {
                                    m += (Qlx.RPAREN, an); // ^5
                                    Next();
                                    an = lxr.val;  // ^6
                                    Mustbe(Qlx.Id);
                                }
                                while (true) // ln is ^6
                                {
                                    if (!cx.role.dbobjects.Contains(an.ToString()))
                                        throw new DBException("42161", "NodeType", an);
                                    al += an;
                                    if (tok != Qlx.VBAR)
                                        break;
                                    Next();
                                    an = lxr.val; // ^+5
                                    Mustbe(Qlx.Id);
                                }
                                if (tok == Qlx.SET) // ^7
                                {
                                    m += (Qlx.RARROWBASE, TChar.Empty); // ^7
                                    Next();
                                }
                                m += (Qlx.ARROW, al); // ^6
                                Mustbe(Qlx.RPAREN);
                            }
                            break;
                        }
                    case Qlx.CARDINALITY:
                        {
                            Next();
                            Mustbe(Qlx.LPAREN);
                            var lw = lxr.val;
                            Mustbe(Qlx.INTEGERLITERAL);
                            m += (Qlx.MIN, lw);
                            if (tok == Qlx.TO)
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Qlx.INTEGERLITERAL, Qlx.TIMES);
                                m += (Qlx.MAX, lw);
                            }
                            break;
                        }
                    case Qlx.MULTIPLICITY:
                        {
                            Next();
                            Mustbe(Qlx.LPAREN);
                            var lw = lxr.val;
                            Mustbe(Qlx.INTEGERLITERAL);
                            m += (Qlx.MINVALUE, lw);
                            if (tok == Qlx.TO)
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Qlx.INTEGERLITERAL, Qlx.TIMES);
                                m += (Qlx.MAXVALUE, lw);
                            }
                            break;
                        }
                    default:
                        if (drop)
                            o = new TChar("");
                        m += (tok, o);
                        iv = -1L;
                        break;
                    case Qlx.RPAREN:
                        break;
                }
                Next();
            }
            if (ds != TNull.Value && !m.Contains(Qlx.DESC))
                m += (Qlx.DESC, ds);
            if (iv != -1L)
                m += (Qlx.INVERTS, new TInt(iv));
            if (iri != TNull.Value)
                m += (Qlx.IRI, iri);
            return m;
        }
        /// <summary>
        /// GenerationRule =  GENERATED ALWAYS AS '('Value')' [ UPDATE '(' Assignments ')' ]
        /// |   GENERATED ALWAYS AS ROW (START|END) .
        /// </summary>
        /// <param name="rt">The expected type</param>
        GenerationRule ParseGenerationRule(long tc, Domain xp)
        {
            var gr = GenerationRule.None;
            var ox = cx.parse;
            if (Match(Qlx.GENERATED))
            {
                Next();
                Mustbe(Qlx.ALWAYS);
                Mustbe(Qlx.AS);
                if (Match(Qlx.ROW))
                {
                    Next();
                    gr = tok switch
                    {
                        Qlx.START => new GenerationRule(Generation.RowStart),
                        Qlx.END => new GenerationRule(Generation.RowEnd),
                        _ => throw new DBException("42161", "START or END", tok.ToString()).Mix(),
                    };
                    Next();
                }
                else
                {
                    var st = lxr.start;
                    var oc = cx.parse;
                    cx.parse = ExecuteStatus.Compile;
                    var nst = cx.db.nextStmt;
                    var gnv = ParseSqlValue(xp).Reify(cx);
                    var s = new string(lxr.input, st, lxr.start - st);
                    gr = new GenerationRule(Generation.Expression, s, gnv, tc, nst);
                    cx.Add(gnv);
                    cx.parse = oc;
                }
            }
            cx.parse = ox;
            return gr;
        }
        /// <summary>
        /// Parse a columnconstraintdefinition
        /// ColumnConstraint = [ CONSTRAINT id ] ColumnConstraintDef
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="pc">the column (Level2)</param>
        /// <returns>the updated table</returns>
		Table ParseColumnConstraintDefin(Table tb, PColumn2 pc, TableColumn tc)
        {
            //     Ident name = null;
            if (Match(Qlx.CONSTRAINT))
            {
                Next();
                //      name = new Ident(this);
                Mustbe(Qlx.Id);
            }
            if (tok == Qlx.NOT)
            {
                Next();
                if (pc != null)
                {
                    pc.notNull = true;
                    var dm = tc.domain;
                    if (!dm.notNull && cx.tr is not null)
                    {
                        dm = (Domain)dm.Relocate(cx.GetUid());
                        dm += (Domain.NotNull, true);
                        tc += (Domain.NotNull, true);
                        dm = (Domain)cx.Add(dm);
                        pc.dataType = dm;
                        cx.db += (Transaction.Physicals, cx.tr.physicals + (pc.defpos, pc));
                    }
                    tc += (DBObject._Domain, dm);
                    tc = (TableColumn)cx.Add(tc);
                    tb += (Domain.Representation, tb.representation + (tc.defpos, dm));
                    tb = (Table)cx.Add(tb);
                    cx.db += (tb.defpos, tb);
                    cx.db += (tc.defpos, tc);
                }
                Mustbe(Qlx.NULL);
            }
            else
                tb = ParseColumnConstraint(tb, tc);
            cx.db += (tc.defpos, tc);
            return tb;
        }
        /// <summary>
        /// ColumnConstraintDef = 	NOT NULL
        ///     |	PRIMARY KEY 
        ///     |	REFERENCES id [ Cols ] { ReferentialAction }
        /// 	|	UNIQUE 
        ///     |   CHECK SearchCondition .
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="tc">the table column (Level3)</param>
        /// <param name="name">the name of the constraint</param>
        /// <returns>the updated table</returns>
		Table ParseColumnConstraint(Table tb, TableColumn tc)
        {
            if (cx.tr == null) throw new DBException("42105").Add(Qlx.TRANSACTION);
            var key = new Domain(cx.GetUid(), cx, Qlx.ROW, new BList<DBObject>(tc), 1);
            string nm = "";
            switch (tok)
            {
                case Qlx.SECURITY:
                    Next();
                    ParseClassification(tc);
                    break;
                case Qlx.REFERENCES:
                    {
                        Next();
                        var rn = lxr.val.ToString();
                        var ta = cx.GetObject(rn);
                        var rt = ta as Table;
                        if (rt == null && ta is RestView rv)
                            rt = rv.super.First()?.key() as Table??throw new DBException("42105").Add(Qlx.REFERENCES);
                        if (rt == null) throw new DBException("42107", rn).Mix();
                        var cols = Domain.Row;
                        Mustbe(Qlx.Id);
                        if (tok == Qlx.LPAREN)
                            cols = ParseColsList(rt) ?? throw new DBException("42000").Add(Qlx.REFERENCING);
                        string afn = "";
                        if (tok == Qlx.USING)
                        {
                            Next();
                            int st = lxr.start;
                            if (tok == Qlx.Id)
                            {
                                Next();
                                var ni = new Ident(this);
                                var pr = cx.GetProcedure(LexPos().dp, ni.ident, new CList<Domain>(tb))
                                     ?? throw new DBException("42108", ni.ident);
                                afn = "\"" + pr.defpos + "\"";
                            }
                            else
                            {
                                Mustbe(Qlx.LPAREN);
                                ParseSqlValueList(Domain.Content);
                                Mustbe(Qlx.RPAREN);
                                afn = new string(lxr.input, st, lxr.start - st);
                            }
                        }
                        var ct = ParseReferentialAction();
                        tb = cx.tr.ReferentialConstraint(cx, tb, "", key, rt, cols, ct, afn);
                        break;
                    }
                case Qlx.CONSTRAINT:
                    {
                        Next();
                        nm = new Ident(this).ident;
                        Mustbe(Qlx.Id);
                        if (tok != Qlx.CHECK)
                            throw new DBException("42161", "CHECK", tok);
                        goto case Qlx.CHECK;
                    }
                case Qlx.CHECK:
                    {
                        Next();
                        ParseColumnCheckConstraint(tb, tc, nm);
                        break;
                    }
                case Qlx.DEFAULT:
                    {
                        Next();
                        cx.Add(tc + (Domain.Default, ParseSqlValue(tc.domain).Eval(cx) ?? TNull.Value));
                        break;
                    }
                case Qlx.UNIQUE:
                    {
                        Next();
                        var tr = cx.db as Transaction ?? throw new DBException("2F003");
                        cx.Add(new PIndex(nm, tb, key, PIndex.ConstraintType.Unique, -1L,
                            tr.nextPos));
                        break;
                    }
                case Qlx.PRIMARY:
                    {
                        var tn = tb.NameFor(cx);
                        if (tb.FindPrimaryIndex(cx) is not null)
                            throw new DBException("42147", tn).Mix();
                        Next();
                        Mustbe(Qlx.KEY);
                        cx.Add(new PIndex(tn, tb, key, PIndex.ConstraintType.PrimaryKey,
                            -1L, cx.db.nextPos));
                        break;
                    }
            }
            return tb;
        }
        /// <summary>
        /// TableConstraint = [CONSTRAINT id ] TableConstraintDef .
		/// TableConstraintDef = UNIQUE Cols
		/// |	PRIMARY KEY  Cols
		/// |	FOREIGN KEY Cols REFERENCES Table_id [ Cols ] { ReferentialAction } .
        /// </summary>
        /// <param name="tb">the table</param>
		Table ParseTableConstraintDefin(Table tb)
        {
            Ident? name = null;
            if (Match(Qlx.CONSTRAINT))
            {
                Next();
                name = new Ident(this);
                Mustbe(Qlx.Id);
            }
            else if (tok == Qlx.Id)
                name = new Ident(this);
            Qlx s = Mustbe(Qlx.UNIQUE, Qlx.PRIMARY, Qlx.FOREIGN, Qlx.CHECK);
            switch (s)
            {
                case Qlx.UNIQUE: tb = ParseUniqueConstraint(tb, name); break;
                case Qlx.PRIMARY: (tb) = ParsePrimaryConstraint(tb, name); break;
                case Qlx.FOREIGN: (tb) = ParseReferentialConstraint(tb, name); break;
                case Qlx.CHECK: (tb) = ParseTableConstraint(tb, name); break;
            }
            cx.result = -1L;
            return (Table)cx.Add(tb);
        }
        /// <summary>
        /// construct a unique constraint
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="name">the constraint name</param>
        /// <returns>the updated table</returns>
        Table ParseUniqueConstraint(Table tb, Ident? name)
        {
            var tr = cx.db as Transaction ?? throw new DBException("42105").Add(Qlx.TRANSACTION);
            if (ParseColsList(tb, Qlx.ROW) is not Domain ks) throw new DBException("42161", "cols");
            var px = new PIndex(name?.ident ?? "", tb, ks, PIndex.ConstraintType.Unique, -1L, tr.nextPos);
            return (Table)(cx.Add(px) ?? throw new DBException("42105").Add(Qlx.UNIQUE));
        }
        /// <summary>
        /// construct a primary key constraint
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="cl">the ident</param>
        /// <param name="name">the constraint name</param>
        Table ParsePrimaryConstraint(Table tb, Ident? name)
        {
            var tr = cx.db as Transaction ?? throw new DBException("42105").Add(Qlx.TRANSACTION);
            Mustbe(Qlx.KEY);
            if (ParseColsList(tb, Qlx.ROW) is not Domain ks) throw new DBException("42161", "cols");
            if (tb.FindPrimaryIndex(cx) is not null)
            {
                var up = -1L;
                for (var b = tb.indexes[ks]?.First(); up < 0L && b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Level3.Index x && x.flags.HasFlag(PIndex.ConstraintType.Unique))
                        up = x.defpos;
                if (up < 0L)
                {
                    var pp = new PIndex(name?.ident ?? "", tb, ks, PIndex.ConstraintType.Unique, -1L, tr.nextPos);
                    if (cx.Add(pp) is null)
                        throw new DBException("42105").Add(Qlx.PRIMARY);
                    up = pp.defpos;
                }
                var ax = new AlterIndex(up, cx.db.nextPos);
                return (Table)(cx.Add(ax) ?? throw new DBException("42105").Add(Qlx.ALTER));
            }
            Physical px = new PIndex(name?.ident ?? "", tb, ks,
                PIndex.ConstraintType.PrimaryKey, -1L, tr.nextPos);
            return (Table)(cx.Add(px) ?? throw new DBException("42105").Add(Qlx.CONSTRAINT));
        }
        /// <summary>
        /// construct a referential constraint
        /// id [ Cols ] { ReferentialAction }
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="name">the constraint name</param>
        /// <returns>the updated table</returns>
        Table ParseReferentialConstraint(Table tb, Ident? name)
        {
            var tr = cx.db as Transaction ?? throw new DBException("42105").Add(Qlx.TRANSACTION);
            Mustbe(Qlx.KEY);
            var cols = ParseColsList(tb) ?? throw new DBException("42161", "cols");
            Mustbe(Qlx.REFERENCES);
            var refname = new Ident(this);
            var rt = cx.GetObject(refname.ident) as Table ??
                throw new DBException("42107", refname).Mix();
            Mustbe(Qlx.Id);
            var refs = Domain.Row;
            PIndex.ConstraintType ct = PIndex.ConstraintType.ForeignKey;
            if (tok == Qlx.LPAREN)
                refs = ParseColsList(rt) ?? Domain.Null;
            string afn = "";
            if (Match(Qlx.USING))
            {
                Next();
                int st = lxr.start;
                if (tok == Qlx.Id)
                {
                    var ic = new Ident(this);
                    Next();
                    var pr = cx.GetProcedure(LexPos().dp, ic.ident, Context.Signature(refs))
                        ?? throw new DBException("42108", ic.ident);
                    afn = "\"" + pr.defpos + "\"";
                }
                else
                {
                    Mustbe(Qlx.LPAREN);
                    ParseSqlValueList(Domain.Content);
                    Mustbe(Qlx.RPAREN);
                    afn = new string(lxr.input, st, lxr.start - st);
                }
            }
            if (Match(Qlx.ON))
                ct |= ParseReferentialAction();
            tb = tr.ReferentialConstraint(cx, tb, name?.ident ?? "", cols, rt, refs, ct, afn);
            return (Table)(cx.Add(tb) ?? throw new DBException("42105").Add(Qlx.ACTION));
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] QlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="name">the name of the constraint</param
        /// <returns>the new PCheck</returns>
        Table ParseTableConstraint(Table tb, Ident? name)
        {
            int st = lxr.start;
            var nst = cx.db.nextStmt;
            Mustbe(Qlx.LPAREN);
            var se = ParseSqlValue(Domain.Bool);
            Mustbe(Qlx.RPAREN);
            var n = name ?? new Ident(this);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            PCheck r = new(tb, n.ident, se, new string(lxr.input, st, lxr.start - st), nst, tr.nextPos, cx);
            tb = (Table)(cx.Add(r) ?? throw new DBException("42105").Add(Qlx.CHECK));
            if (tb.defpos < Transaction.TransPos)
            {
                var trs = tb.RowSets(new Ident("", Iix.None), cx, tb, -1L);
                if (trs.First(cx) != null)
                    throw new DBException("44000", n.ident).ISO();
            }
            return tb;
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] QlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="pc">the column constrained</param>
        /// <param name="name">the name of the constraint</param>
        /// <returns>the new TableColumn</returns>
        TableColumn ParseColumnCheckConstraint(Table tb, TableColumn tc, string nm)
        {
            var oc = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            int st = lxr.start;
            Mustbe(Qlx.LPAREN);
            // Set up the information for parsing the column check constraint
            var ix = cx.Ix(tb.defpos);
            cx.defs += (new Ident(tb.NameFor(cx), ix), ix);
            for (var b = cx.obs.PositionAt(tb.defpos)?.Next(); b != null; b = b.Next())
                if (b.value() is TableColumn x)
                    cx.defs += (x.NameFor(cx), cx.Ix(x.defpos), Ident.Idents.Empty);
            var nst = cx.db.nextStmt;
            var se = ParseSqlValue(Domain.Bool);
            Mustbe(Qlx.RPAREN);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var pc = new PCheck2(tb, tc, nm, se,
                new string(lxr.input, st, lxr.start - st), nst, tr.nextPos, cx);
            cx.parse = oc;
            return (TableColumn)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.CHECK));
        }
        /// <summary>
		/// ReferentialAction = {ON (DELETE|UPDATE) (CASCADE| SET DEFAULT|RESTRICT)} .
        /// </summary>
        /// <returns>constraint type flags</returns>
		PIndex.ConstraintType ParseReferentialAction()
        {
            PIndex.ConstraintType r = PIndex.Reference;
            while (Match(Qlx.ON))
            {
                Next();
                Qlx when = Mustbe(Qlx.UPDATE, Qlx.DELETE);
                Qlx what = Mustbe(Qlx.RESTRICT, Qlx.CASCADE, Qlx.SET, Qlx.NO);
                if (what == Qlx.SET)
                    what = Mustbe(Qlx.DEFAULT, Qlx.NULL);
                else if (what == Qlx.NO)
                {
                    Mustbe(Qlx.ACTION);
                    throw new DBException("42123").Mix();
                }
                if (when == Qlx.UPDATE)
                {
                    r &= ~PIndex.Updates;
                    switch (what)
                    {
                        case Qlx.CASCADE: r |= PIndex.ConstraintType.CascadeUpdate; break;
                        case Qlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultUpdate; break;
                        case Qlx.NULL: r |= PIndex.ConstraintType.SetNullUpdate; break;
                        case Qlx.RESTRICT: r |= PIndex.ConstraintType.RestrictUpdate; break;
                    }
                }
                else
                {
                    r &= ~PIndex.Deletes;
                    switch (what)
                    {
                        case Qlx.CASCADE: r |= PIndex.ConstraintType.CascadeDelete; break;
                        case Qlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultDelete; break;
                        case Qlx.NULL: r |= PIndex.ConstraintType.SetNullDelete; break;
                        case Qlx.RESTRICT: r |= PIndex.ConstraintType.RestrictDelete; break;
                    }
                }
            }
            return r;
        }
        /// <summary>
        /// Called from ALTER and CREATE parsers. This is the first time we see the proc-clause,
        /// and we must parse it to add any physicals needed to declare Domains and Types 
        /// before we add the PProcedure/Alter physical.
        /// CREATE (PROCEDURE|FUNCTION) id '(' Parameters ')' [RETURNS Type] Body
        /// ALTER (PROCEDURE|FUNCTION) id '(' Parameters ')' [RETURNS Type] AlterBody
        /// </summary>
        /// <param name="func">whether it is a function</param>
        /// <param name="create">whether it is CREATE</param>
        /// <returns>the new Executable</returns>
        public void ParseProcedureClause(bool func, Qlx create)
        {
            var op = cx.parse;
            var nst = cx.db.nextStmt;
            var n = new Ident(lxr.val.ToString(),
                new Iix(lxr.Position, cx, cx.db.nextPos)); // n.iix.dp will match pp.ppos
            cx.parse = ExecuteStatus.Compile;
            Mustbe(Qlx.Id);
            int st = lxr.start;
            var ps = ParseParameters(n);
            var a = Context.Signature(ps);
            var pi = new ObInfo(n.ident,
                Grant.Privilege.Owner | Grant.Privilege.Execute | Grant.Privilege.GrantExecute);
            var rdt = func ? ParseReturnsClause(n) : Domain.Null;
            if (Match(Qlx.EOF) && create == Qlx.CREATE)
                throw new DBException("42000", "EOF").Mix();
            var pr = cx.GetProcedure(LexPos().dp, n.ident, a);
            PProcedure? pp = null;
            if (pr == null)
            {
                if (create == Qlx.CREATE)
                {
                    // create a preliminary version of the PProcedure without parsing the body
                    // in case the procedure is recursive (the body is parsed below)
                    pp = new PProcedure(n.ident, ps,
                        rdt, pr, new Ident(lxr.input.ToString() ?? "", n.iix), nst, cx.db.nextPos, cx);
                    pr = new Procedure(pp.defpos, cx, ps, rdt,
                        new BTree<long, object>(DBObject.Definer, cx.role.defpos)
                        + (DBObject.Infos, new BTree<long, ObInfo>(cx.role.defpos, pi)));
                    pr = (Procedure)(cx.Add(pp) ?? pr);
                    pp.dataType = pr.domain;
                    cx.db += pr;
                }
                else
                    throw new DBException("42108", n.ToString()).Mix();
            }
            else
                if (create == Qlx.CREATE)
                throw new DBException("42167", n.ident, ps.Length).Mix();
            if (create == Qlx.ALTER && tok == Qlx.TO)
            {
                Next();
                Mustbe(Qlx.Id);
                cx.db += pr;
            }
            else if (create == Qlx.ALTER && (StartMetadata(Qlx.FUNCTION) || Match(Qlx.ALTER, Qlx.ADD, Qlx.DROP)))
                ParseAlterBody(pr);
            else
            {
                var lp = LexPos();
                if (StartMetadata(Qlx.FUNCTION))
                {
                    var m = ParseMetadata(Qlx.FUNCTION);
                    var pm = new PMetadata3(n.ident, 0, pr, m, cx.db.nextPos);
                    cx.Add(pm);
                }
                var s = new Ident(new string(lxr.input, st, lxr.start - st), lp);
                if (tok != Qlx.EOF && tok != Qlx.SEMICOLON)
                {
                    cx.AddParams(pr);
                    cx.Add(pr);
                    var bd = ParseStatement(pr.domain,true) ?? throw new DBException("42000", "Statement");
                    cx.Add(pr);
                    var ns = cx.db.nextStmt;
                    var fm = new Framing(cx, pp?.nst ?? ns);
                    s = new Ident(new string(lxr.input, st, lxr.start - st), lp);
                    if (pp != null)
                    {
                        pp.source = s;
                        pp.proc = bd?.defpos ?? throw new DBException("42000", "Body missing");
                        pp.framing = fm;
                    }
                    pr += (DBObject._Framing, fm);
                    pr += (Procedure.Clause, s.ident);
                    pr += (Procedure.Body, bd.defpos);
                    if (pp is not null)
                        cx.Add(pp);
                    cx.Add(pr);
                    cx.result = -1L;
                    cx.parse = op;
                }
                if (create == Qlx.CREATE)
                    cx.db += pr;
                var cix = cx.Ix(pr.defpos);
                cx.defs += (n, cix);
                if (pp == null)
                {
                    var pm = new Modify(pr.defpos, pr, s, nst, cx.db.nextPos, cx);
                    cx.Add(pm); // finally add the Modify
                }
            }
            cx.result = -1L;
            cx.parse = op;
        }
        internal (Domain, Domain) ParseProcedureHeading(Ident pn)
        {
            var ps = Domain.Null;
            var oi = Domain.Null;
            if (tok != Qlx.LPAREN)
                return (ps, Domain.Null);
            ps = ParseParameters(pn);
            LexPos(); // for synchronising with CREATE
            if (Match(Qlx.RETURNS))
            {
                Next();
                oi = ParseDataType(pn);
            }
            return (ps, oi);
        }
        /// <summary>
        /// Function metadata is used to control display of output from table-valued functions
        /// </summary>
        /// <param name="pr"></param>
        void ParseAlterBody(Procedure pr)
        {
            if (pr.domain.kind != Qlx.Null)
                return;
            if (pr.domain.Length == 0)
                return;
            ParseAlterOp(pr.domain);
            while (tok == Qlx.COMMA)
            {
                Next();
                ParseAlterOp(pr.domain);
            }
        }
        /// <summary>
        /// Parse a parameter tree
        /// </summary>
        /// <param name="pn">The proc/method name</param>
        /// <param name="xp">The UDT if we are in CREATE TYPE (null if in CREATE/ALTER METHOD or if no udt)</param>
        /// <returns>the tree of formal procparameters</returns>
		internal Domain ParseParameters(Ident pn, Domain? xp = null)
        {
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            Mustbe(Qlx.LPAREN);
            var r = BList<DBObject>.Empty;
            while (tok != Qlx.RPAREN)
            {
                r += ParseProcParameter(pn, xp);
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            Mustbe(Qlx.RPAREN);
            cx.parse = op;
            return new Domain(Qlx.ROW, cx, r);
        }
        internal Domain ParseReturnsClause(Ident pn)
        {
            if (!Match(Qlx.RETURNS))
                return Domain.Null;
            Next();
            StartDataType();
            if (tok == Qlx.Id)
            {
                var s = lxr.val.ToString();
                Next();
                var ob = (Domain)(cx._Ob(cx.role.dbobjects[s] ?? -1L)
                    ?? throw new DBException("42119", s));
                return ob;
            }
            var dm = ParseDataType(pn);
            cx.Add(dm);
            return dm;
        }
        /// <summary>
        /// parse a formal parameter
        /// </summary>
        /// <param name="pn">The procedure or method</param>
        /// <param name="xp">The UDT if in CREATE TYPE</param>
        /// <returns>the procparameter</returns>
		FormalParameter ParseProcParameter(Ident pn, Domain? xp = null)
        {
            Qlx pmode = Qlx.IN;
            if (Match(Qlx.IN, Qlx.OUT, Qlx.INOUT))
            {
                pmode = tok;
                Next();
            }
            var n = new Ident(this);
            Mustbe(Qlx.Id);
            var p = new FormalParameter(n.iix.dp, pmode, n.ident, ParseDataType(n))
                + (DBObject._From, pn.iix.dp);
            cx.Add(p);
            if (xp == null) // prepare to parse a body
                cx.defs += (new Ident(pn, n), pn.iix);
            if (Match(Qlx.RESULT))
            {
                p += (FormalParameter.Result, true);
                cx.Add(p);
                Next();
            }
            return p;
        }
        /// <summary>
		/// Declaration = 	DECLARE id { ',' id } Type
		/// |	DECLARE id CURSOR FOR QueryExpression [ FOR UPDATE [ OF Cols ]] 
        /// |   HandlerDeclaration .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseDeclaration()
        {
            var lp = LexPos();
            Next();
            if (Match(Qlx.CONTINUE, Qlx.EXIT, Qlx.UNDO))
                return (Executable)cx.Add(ParseHandlerDeclaration());
            var n = new Ident(this);
            Mustbe(Qlx.Id);
            LocalVariableDec lv;
            if (Match(Qlx.CURSOR))
            {
                Next();
                Mustbe(Qlx.FOR);
                var cs = ParseCursorSpecification(Domain.TableType);
                var cu = (RowSet?)cx.obs[cs.union] ?? throw new PEException("PE1557");
                var sc = new SqlCursor(n.iix.dp, cu, n.ident);
                cx.result = -1L;
                cx.Add(sc);
                lv = new CursorDeclaration(lp.dp, sc, cu);
            }
            else
            {
                var ld = ParseDataType();
                var vb = new QlValue(n, BList<Ident>.Empty, cx, ld);
                cx.Add(vb);
                lv = new LocalVariableDec(lp.dp, vb);
                if (Match(Qlx.EQL, Qlx.DEFAULT))
                {
                    Next();
                    var iv = ParseSqlValue(ld);
                    cx.Add(iv);
                    lv += (LocalVariableDec.Init, iv.defpos);
                }
            }
            var cix = cx.Ix(lv.vbl);
            cx.defs += (n, cix);
            cx.Add(lv);
            return lv;
        }
        /// <summary>
        /// |	DECLARE HandlerType HANDLER FOR ConditionList Statement .
        /// HandlerType = 	CONTINUE | EXIT | UNDO .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
        HandlerStatement ParseHandlerDeclaration()
        {
            var hs = new HandlerStatement(LexDp(), tok, new Ident(this).ident);
            Mustbe(Qlx.CONTINUE, Qlx.EXIT, Qlx.UNDO);
            Mustbe(Qlx.HANDLER);
            Mustbe(Qlx.FOR);
            hs += (HandlerStatement.Conds, ParseConditionValueList());
            ParseStatement(Domain.Content,true);
            if (cx.exec is not Executable a) throw new DBException("42161", "handler");
            cx.Add(a);
            hs = hs + (HandlerStatement.Action, a.defpos) + (DBObject.Dependents, a.dependents);
            cx.Add(hs);
            return hs;
        }
        /// <summary>
		/// ConditionList =	Condition { ',' Condition } .
        /// </summary>
        /// <returns>the tree of conditions</returns>
        BList<string> ParseConditionValueList()
        {
            var r = new BList<string>(ParseConditionValue());
            while (tok == Qlx.COMMA)
            {
                Next();
                r += ParseConditionValue();
            }
            return r;
        }
        /// <summary>
		/// Condition =	Condition_id | SQLSTATE string | SQLEXCEPTION | SQLWARNING | (NOT FOUND) .
        /// </summary>
        /// <returns>a string</returns>
        string ParseConditionValue()
        {
            var n = lxr.val;
            if (Match(Qlx.SQLSTATE))
            {
                Next();
                if (Match(Qlx.VALUE))
                    Next();
                n = lxr.val;
                Mustbe(Qlx.CHARLITERAL);
                switch (n.ToString()[..2])
                { // handlers are not allowed to defeat the transaction machinery
                    case "25": throw new DBException("2F003").Mix();
                    case "40": throw new DBException("2F003").Mix();
                    case "2D": throw new DBException("2F003").Mix();
                }
            }
            else if (Match(Qlx.SQLEXCEPTION, Qlx.SQLWARNING, Qlx.NOT))
            {
                if (tok == Qlx.NOT)
                {
                    Next();
                    Mustbe(Qlx.FOUND);
                    n = new TChar("NOT_FOUND");
                }
                else
                {
                    n = new TChar(tok.ToString());
                    Next();
                }
            }
            else
                Mustbe(Qlx.Id);
            return n.ToString();
        }
        /// <summary>
		/// CompoundStatement = Label BEGIN [XMLDec] Statements END .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="f">The procedure whose body is being defined if any</param>
        /// <returns>the Executable result of the parse</returns>
		CompoundStatement ParseCompoundStatement(Domain xp, string n)
        {
            var cs = new CompoundStatement(LexDp(), n);
            var st = Mustbe(Qlx.BEGIN,Qlx.LBRACE);
            var et = (st == Qlx.BEGIN) ? Qlx.END : Qlx.RBRACE;
            if (Match(Qlx.TRANSACTION))
                throw new DBException("22G01", "Nested transactions are not supported").ISO();
            var r = BList<long?>.Empty;
            while (tok != Qlx.END && ParseStatement(xp,true) is Executable a)
            {
                r += cx.Add(a).defpos;
                if (tok == et)
                    break;
                Mustbe(Qlx.SEMICOLON,Qlx.NEXT);
            }
            Mustbe(et);
            cs += (cx, CompoundStatement.Stms, r);
            cx.Add(cs);
            return cs;
        }
        /// <summary>
        /// GetDiagnostics = GET DIAGMOSTICS Target = ItemName { , Target = ItemName }
        /// </summary>
        GetDiagnostics ParseGetDiagnosticsStatement()
        {
            Next();
            Mustbe(Qlx.DIAGNOSTICS);
            var r = new GetDiagnostics(LexDp());
            for (; ; )
            {
                var t = ParseSqlValueEntry(Domain.Content, false);
                cx.Add(t);
                Mustbe(Qlx.EQL);
                Match(Qlx.NUMBER, Qlx.MORE, Qlx.COMMAND_FUNCTION, Qlx.COMMAND_FUNCTION_CODE,
                    Qlx.DYNAMIC_FUNCTION, Qlx.DYNAMIC_FUNCTION_CODE, Qlx.ROW_COUNT,
                    Qlx.TRANSACTIONS_COMMITTED, Qlx.TRANSACTIONS_ROLLED_BACK,
                    Qlx.TRANSACTION_ACTIVE, Qlx.CATALOG_NAME,
                    Qlx.CLASS_ORIGIN, Qlx.COLUMN_NAME, Qlx.CONDITION_NUMBER,
                    Qlx.CONNECTION_NAME, Qlx.CONSTRAINT_CATALOG, Qlx.CONSTRAINT_NAME,
                    Qlx.CONSTRAINT_SCHEMA, Qlx.CURSOR_NAME, Qlx.MESSAGE_LENGTH,
                    Qlx.MESSAGE_OCTET_LENGTH, Qlx.MESSAGE_TEXT, Qlx.PARAMETER_MODE,
                    Qlx.PARAMETER_NAME, Qlx.PARAMETER_ORDINAL_POSITION,
                    Qlx.RETURNED_SQLSTATE, Qlx.ROUTINE_CATALOG, Qlx.ROUTINE_NAME,
                    Qlx.ROUTINE_SCHEMA, Qlx.SCHEMA_NAME, Qlx.SERVER_NAME, Qlx.SPECIFIC_NAME,
                    Qlx.SUBCLASS_ORIGIN, Qlx.TABLE_NAME, Qlx.TRIGGER_CATALOG,
                    Qlx.TRIGGER_NAME, Qlx.TRIGGER_SCHEMA,
                    Qlx.SESSION_SET_SCHEMA_COMMAND, Qlx.SESSION_SET_TIME_ZONE_COMMAND,
                    Qlx.SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND,
                    Qlx.SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND,
                    Qlx.SESSION_SET_VALUE_PARAMETER_COMMAND, Qlx.SESSION_RESET_COMMAND,
                    Qlx.SESSION_CLOSE_COMMAND, Qlx.START_TRANSACTION_COMMAND,
                    Qlx.ROLLBACK_COMMAND, Qlx.COMMIT_COMMAND, Qlx.CREATE_SCHEMA_STATEMENT,
                    Qlx.DROP_SCHEMA_STATEMENT, Qlx.CREATE_GRAPH_STATEMENT,
                    Qlx.DROP_GRAPH_STATEMENT,Qlx.CREATE_GRAPH_TYPE_STATEMENT,
                    Qlx.DROP_GRAPH_TYPE_STATEMENT,Qlx.INSERT_STATEMENT,
                    Qlx.SET_STATEMENT,Qlx.REMOVE_STATEMENT,Qlx.DELETE_STATEMENT,
                    Qlx.MATCH_STATEMENT, Qlx.FILTER_STATEMENT, Qlx.LET_STATEMENT,
                    Qlx.FOR_STATEMENT,Qlx.ORDER_BY_AND_PAGE_STATEMENT,
                    Qlx.RETURN_STATEMENT,Qlx.SELECT_STATEMENT,
                    Qlx.CALL_PROCEDURE_STATEMENT);
                r += (cx, GetDiagnostics.List, r.list + (t.defpos, tok));
                Mustbe(Qlx.NUMBER, Qlx.MORE, Qlx.COMMAND_FUNCTION, Qlx.COMMAND_FUNCTION_CODE,
                    Qlx.DYNAMIC_FUNCTION, Qlx.DYNAMIC_FUNCTION_CODE, Qlx.ROW_COUNT,
                    Qlx.TRANSACTIONS_COMMITTED, Qlx.TRANSACTIONS_ROLLED_BACK,
                    Qlx.TRANSACTION_ACTIVE, Qlx.CATALOG_NAME,
                    Qlx.CLASS_ORIGIN, Qlx.COLUMN_NAME, Qlx.CONDITION_NUMBER,
                    Qlx.CONNECTION_NAME, Qlx.CONSTRAINT_CATALOG, Qlx.CONSTRAINT_NAME,
                    Qlx.CONSTRAINT_SCHEMA, Qlx.CURSOR_NAME, Qlx.MESSAGE_LENGTH,
                    Qlx.MESSAGE_OCTET_LENGTH, Qlx.MESSAGE_TEXT, Qlx.PARAMETER_MODE,
                    Qlx.PARAMETER_NAME, Qlx.PARAMETER_ORDINAL_POSITION,
                    Qlx.RETURNED_SQLSTATE, Qlx.ROUTINE_CATALOG, Qlx.ROUTINE_NAME,
                    Qlx.ROUTINE_SCHEMA, Qlx.SCHEMA_NAME, Qlx.SERVER_NAME, Qlx.SPECIFIC_NAME,
                    Qlx.SUBCLASS_ORIGIN, Qlx.TABLE_NAME, Qlx.TRIGGER_CATALOG,
                    Qlx.TRIGGER_NAME, Qlx.TRIGGER_SCHEMA,
                    Qlx.SESSION_SET_SCHEMA_COMMAND, Qlx.SESSION_SET_TIME_ZONE_COMMAND,
                    Qlx.SESSION_SET_PROPERTY_GRAPH_PARAMETER_COMMAND,
                    Qlx.SESSION_SET_BINDING_TABLE_PARAMETER_COMMAND,
                    Qlx.SESSION_SET_VALUE_PARAMETER_COMMAND, Qlx.SESSION_RESET_COMMAND,
                    Qlx.SESSION_CLOSE_COMMAND, Qlx.START_TRANSACTION_COMMAND,
                    Qlx.ROLLBACK_COMMAND, Qlx.COMMIT_COMMAND, Qlx.CREATE_SCHEMA_STATEMENT,
                    Qlx.DROP_SCHEMA_STATEMENT, Qlx.CREATE_GRAPH_STATEMENT,
                    Qlx.DROP_GRAPH_STATEMENT, Qlx.CREATE_GRAPH_TYPE_STATEMENT,
                    Qlx.DROP_GRAPH_TYPE_STATEMENT, Qlx.INSERT_STATEMENT,
                    Qlx.SET_STATEMENT, Qlx.REMOVE_STATEMENT, Qlx.DELETE_STATEMENT,
                    Qlx.MATCH_STATEMENT, Qlx.FILTER_STATEMENT, Qlx.LET_STATEMENT,
                    Qlx.FOR_STATEMENT, Qlx.ORDER_BY_AND_PAGE_STATEMENT,
                    Qlx.RETURN_STATEMENT, Qlx.SELECT_STATEMENT,
                    Qlx.CALL_PROCEDURE_STATEMENT);
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            return (GetDiagnostics)cx.Add(r);
        }
        /// <summary>
        /// Label =	[ label ':' ] .
        /// Some procedure statements have optional labels. We deal with these here
        /// </summary>
        /// <param name="xp">the expected ob type if any</param>
		Executable ParseLabelledStatement(Domain xp)
        {
            Ident sc = ParseIdentChain(false).Item1;
            var lp = lxr.start;
            var cp = LexPos();
            // OOPS: according to SQL 2003 there MUST follow a colon for a labelled statement
            if (tok == Qlx.COLON)
            {
                Next();
                var s = sc.ident;
                var e = tok switch
                {
                    Qlx.BEGIN => ParseCompoundStatement(xp, s),
                    Qlx.FOR => ParseForStatement(xp, s),
                    Qlx.LOOP => ParseLoopStatement(xp, s),
                    Qlx.REPEAT => ParseRepeat(xp, s),
                    Qlx.WHILE => ParseSqlWhile(xp, s),
                    _ => throw new DBException("26000", s).ISO(),
                };
                return (Executable)cx.Add(e);
            }
            // OOPS: but we'q better allow a procedure call here for backwards compatibility
            else if (tok == Qlx.LPAREN)
            {
                Next();
                cp = LexPos();
                var ps = ParseSqlValueList(Domain.Content);
                Mustbe(Qlx.RPAREN);
                var a = cx.Signature(ps);
                var pr = cx.GetProcedure(cp.dp, sc.ident, a) ??
                    throw new DBException("42108", sc.ident);
                var c = new SqlProcedureCall(cp.dp, cx, pr, ps);
                return (Executable)cx.Add(new CallStatement(cx.GetUid(), c));
            }
            // OOPS: and a simple assignment for backwards compatibility
            else if (Identify(sc, new BList<Ident>(sc), Domain.Content) is DBObject vb)
            {
                if (vb is SqlCopy vc && cx.db.objects[vc.copyFrom] is TableColumn tc)
                    vb = tc;
                Mustbe(Qlx.EQL);
                var va = ParseSqlValue(vb.domain);
                var sa = new AssignmentStatement(cp.dp, vb, va);
                return (Executable)cx.Add(sa);
            }
            return new Executable(lp);
        }
        /// <summary>
		/// Assignment = 	SET Target '='  TypedValue { ',' Target '='  TypedValue }
		/// 	|	SET '(' Target { ',' Target } ')' '='  TypedValue .
        /// </summary>
		Executable ParseAssignment()
        {
            var lp = LexPos();
            Next();
            if (tok == Qlx.LPAREN)
                return ParseMultipleAssignment();
            if (Match(Qlx.AUTHORIZATION, Qlx.ROLE, Qlx.TIMEOUT,Qlx.TABLE, Qlx.DOMAIN,Qlx.TYPE,
                Qlx.PROCEDURE, Qlx.FUNCTION, Qlx.TRIGGER, Qlx.METHOD,
                Qlx.STATIC, Qlx.INSTANCE, Qlx.OVERRIDING, Qlx.CONSTRUCTOR))
            { 
                ParseSqlSet(); 
                return Executable.None; 
            }
            var vb = ParseVarOrColumn(Domain.Content);
            cx.Add(vb);
            if (Match(Qlx.COLON,Qlx.IS) && cx.binding[vb.defpos] is TNode tn)
            {
                Next();
                var ln = new Ident(this);
                Mustbe(Qlx.Id);
                if (cx.db.objects[cx.role.dbobjects[ln.ident] ?? -1L] is not Table nt)
                    throw new DBException("42107", ln.ident);
                if (cx.parse == ExecuteStatus.Obey)
                {
                    nt += (Table.TableRows, nt.tableRows + (tn.defpos, tn.tableRow));
                    nt += (Table.LastData, cx.db.nextPos); // this needs more work
                    cx.db += nt;
                }
                return Executable.None;
            }
            Mustbe(Qlx.EQL);
            var va = ParseSqlValue(vb.domain);
            var sa = new AssignmentStatement(lp.dp, vb, va);
            return (Executable)cx.Add(sa);
        }
        /// <summary>
        /// 	|	SET '(' Target { ',' Target } ')' '='  TypedValue .
        /// Target = 	id { '.' id } .
        /// </summary>
		Executable ParseMultipleAssignment()
        {
            Mustbe(Qlx.EQL);
            var ids = ParseIDList();
            var v = ParseSqlValue(Domain.Content);
            cx.Add(v);
            var ma = new MultipleAssignment(LexDp(), cx, ids, v);
            if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction)
                cx = ma._Obey(cx);
            return (Executable)cx.Add(ma);
        }
        /// <summary>
        /// |	RETURN TypedValue
        /// </summary>
		Executable ParseReturn(Domain xp)
        {
            Next();
            QlValue re;
            var dp = cx.GetUid();
            var ep = -1L;
            var dm = xp;
            if (xp.kind == Qlx.CONTENT)
            {
                ep = cx.GetUid();
                dm = ParseSelectList(-1L, xp) + (Domain.Kind, Qlx.ROW);
                if (dm.aggs == CTree<long, bool>.Empty)
                    new ExplicitRowSet(ep,cx,dm,BList<(long,TRow)>.Empty);
                else 
                {
                    var ii = cx.GetIid();
                    var sd = dm.SourceRow(cx, dp); // this is what we will need
                    RowSet sr = new SelectRowSet(ii, cx, dm, new ExplicitRowSet(ep, cx, sd, BList<(long,TRow)>.Empty));
                    if (xp.mem[Domain.Nodes] is CTree<long,bool> xs) // passed to us for MatchStatement Return handling
                        sr += (Domain.Nodes, xs); 
                    sr = ParseSelectRowSet((SelectRowSet)sr); // this is what we will do with it
                    ep = sr.defpos;
                    dm = sd;
                }
                re = new SqlRow(dp, cx, dm);
            }
            else
                re = ParseSqlValue(xp);
            cx.Add(re);
            var rs = new ReturnStatement(cx.GetUid(), re);
            cx.lastret = rs.defpos;
            if (dm != Domain.Content)
                rs += (DBObject._Domain, dm);
            if (ep >= 0)
                rs += (SqlInsert.Value, ep);
            return (Executable)cx.Add(rs);
        }
        /// <summary>
		/// CaseStatement = 	CASE TypedValue { WHEN TypedValue THEN Statements }+ [ ELSE Statements ] END CASE
		/// |	CASE { WHEN QlValue THEN Statements }+ [ ELSE Statements ] END CASE .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseCaseStatement(Domain xp)
        {
            Next();
            if (tok == Qlx.WHEN)
            {
                var ws = ParseWhenList(xp);
                var ss = BList<long?>.Empty;
                if (tok == Qlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList(xp);
                }
                var e = new SearchedCaseStatement(LexDp(), ws, ss);
                Mustbe(Qlx.END);
                Mustbe(Qlx.CASE);
                cx.Add(e);
                return e;
            }
            else
            {
                var op = ParseSqlValue(Domain.Content);
                var ws = ParseWhenList(op.domain);
                var ss = BList<long?>.Empty;
                if (tok == Qlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList(xp);
                }
                Mustbe(Qlx.END);
                Mustbe(Qlx.CASE);
                var e = new SimpleCaseStatement(LexDp(), op, ws, ss);
                cx.Add(e);
                cx.exec = e;
                return e;
            }
        }
        /// <summary>
        /// { WHEN QlValue THEN Statements }
        /// </summary>
        /// <returns>the tree of Whenparts</returns>
		BList<WhenPart> ParseWhenList(Domain xp)
        {
            var r = BList<WhenPart>.Empty;
            var dp = LexDp();
            while (tok == Qlx.WHEN)
            {
                Next();
                var c = ParseSqlValue(xp);
                Mustbe(Qlx.THEN);
                r += new WhenPart(dp, c, ParseStatementList(xp));
            }
            return r;
        }
        LetStatement ParseLet()
        {
            var lp = LexPos();
            var r = BList<long?>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var sg = CTree<UpdateAssignment,bool>.Empty;
            while (tok == Qlx.LET || tok == Qlx.COMMA)
            {
                Next();
                if (tok == Qlx.VALUE)
                    r += ParseBindingVariableDefinitions(); // we expect just one
                else
                {
                    var id = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (cx.defs.Contains(id.ident))
                        throw new DBException("42104", id.ident);
                    var vb = new QlValue(id, BList<Ident>.Empty, cx, Domain.Content);
                    cx.defs += (id, id.iix);
                    Mustbe(Qlx.EQL);
                    var sv = ParseSqlValue(Domain.Content);
                    cx.Add(sv);
                    var sa = new UpdateAssignment(vb.defpos, sv.defpos);
                    vb += (DBObject._Domain, sv.domain);
                    cx.Add(vb);
                    r += vb.defpos;
                    rs += (vb.defpos, sv.domain);
                    sg += (sa, true);
                }
            }
            var dm = new Domain(-1L,cx,Qlx.TABLE,rs,r);
            var ls = new LetStatement(lp.dp, new BTree<long, object>(RowSet.Assig,sg)+(DBObject._Domain,dm));
     /*       var es = ParseStatement(Domain.Content);
            cx.Add(es);
            ls += (Procedure.Body, es.defpos); */
            cx.Add(ls);
            if (cx.parse == ExecuteStatus.Obey)
                cx = ls._Obey(cx);
            return ls;
        }
        OrderAndPageStatement ParseOrderAndPage()
        {
            var lp = LexPos();
            var m = BTree<long, object>.Empty;
            Domain dm = (cx.obs[cx.result] as ExplicitRowSet)??Domain.Row;
            if (tok == Qlx.ORDER)
            {
                dm = ParseOrderClause(dm);
                m += (RowSet.RowOrder, dm);
            }
            if (Match(Qlx.OFFSET,Qlx.SKIP))
            {
                Next();
                var so = ParseSqlValue(Domain.Int);
                cx.Add(so);
                m += (RowSetSection.Offset, so.Eval(cx).ToInt() ?? 0);
            }
            if (Match(Qlx.LIMIT))
            {
                Next();
                var so = ParseSqlValue(Domain.Int);
                cx.Add(so);
                var ln = so.Eval(cx).ToInt() ?? 0;
                if (ln < 0) throw new DBException("22G02").ISO();
                m += (RowSetSection.Size, ln);
            }
            var ls = new OrderAndPageStatement(lp.dp, m);
            cx.Add(ls);
            if (cx.parse == ExecuteStatus.Obey)
                cx = ls._Obey(cx);
            return ls;
        }
        FilterStatement ParseFilter()
        {
            var lp = LexPos();
            var m = BTree<long, object>.Empty;
            Domain dm = (cx.obs[cx.result] as ExplicitRowSet) ?? Domain.Row; 
            if (ParseWhereClause() is CTree<long,bool> wh)
                m += (RowSet._Where, wh);
            var ls = new FilterStatement(lp.dp, m);
            cx.Add(ls);
            if (cx.parse == ExecuteStatus.Obey)
                cx = ls._Obey(cx);
            return ls;
        }
        /// <summary>
		/// ForStatement =	Label FOR [ For_id AS ][ id CURSOR FOR ] QueryExpression DO Statements END FOR [Label_id] .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseForStatement(Domain xp, string? n)
        {
            var lp = LexDp();
            Next();
            Ident c = new(DBObject.Uid(lp), cx.Ix(lp));
            var d = 1; // depth
            if (tok != Qlx.SELECT)
            {
                c = new Ident(this);
                Mustbe(Qlx.Id);
                cx.defs += (c, c.iix);
                if (tok==Qlx.IN)
                {
                    Next();
                    var fv = new QlValue(c, BList<Ident>.Empty, cx, Domain.Content);
                    var fl = ParseSqlValue(Domain.TableType);
                    var fd = ((fl.domain.kind == Qlx.ARRAY) ? fl.domain.elType : fl.domain)??Domain.Content;
                    fv += (DBObject._Domain, fd);
                    cx.Add(fv);
                    var cc = -1L;
                    var op = Qlx.NO;
                    if (tok==Qlx.WITH)
                    {
                        Next();
                        op = Mustbe(Qlx.ORDINALITY, Qlx.OFFSET);
                        c = new Ident(this);
                        Mustbe(Qlx.Id);
                        cx.defs += (c, c.iix);
                        cc = cx.Add(new QlValue(c, BList<Ident>.Empty, cx, Domain.Int)).defpos;
                    }
                    var xf = ParseStatement(Domain.Content);
                    var r = new ForStatement(lp, fv, fl, op, cc, xf, cx);
                    if (cx.parse == ExecuteStatus.Obey)
                        r.Obey(cx);
                    return r;
                }
                if (Match(Qlx.CURSOR))
                {
                    Next();
                    Mustbe(Qlx.FOR);
                }
                else
                {
                    Mustbe(Qlx.AS);
                    if (tok != Qlx.SELECT)
                    {
                        Mustbe(Qlx.Id);
                        Mustbe(Qlx.CURSOR);
                        Mustbe(Qlx.FOR);
                    }
                }
            }
            var ss = ParseCursorSpecification(Domain.TableType, true); // use ambient declarations
            d = Math.Max(d, ss.depth + 1);
            var cs = (RowSet?)cx.obs[ss.union] ?? throw new DBException("42000", "CursorSpec");
            Mustbe(Qlx.DO);
            var xs = ParseStatementList(xp);
            var fs = new ForSelectStatement(lp, n ?? "", c, cs, xs) + (DBObject._Depth, d);
            Mustbe(Qlx.END);
            Mustbe(Qlx.FOR);
            if (tok == Qlx.Id)
            {
                if (n != null && n != lxr.val.ToString())
                    throw new DBException("42157", lxr.val.ToString(), n).Mix();
                Next();
            }
            return (Executable)cx.Add(fs);
        }
        /// <summary>
		/// IfStatement = 	IF BooleanExpr THEN Statements { ELSEIF BooleanExpr THEN Statements } [ ELSE Statements ] END IF .
        /// </summary>
        /// <param name="xp">The type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseIfThenElse(Domain xp)
        {
            var lp = LexDp();
            var old = cx;
            Next();
            var se = ParseSqlValue(Domain.Bool);
            cx.Add(se);
            Mustbe(Qlx.THEN);
            var th = ParseStatementList(xp);
            var ei = BList<long?>.Empty;
            while (Match(Qlx.ELSEIF))
            {
                var d = LexDp();
                Next();
                var s = ParseSqlValue(Domain.Bool);
                cx.Add(s);
                Mustbe(Qlx.THEN);
                Next();
                var t = ParseStatementList(xp);
                var e = new IfThenElse(d, s, t, BList<long?>.Empty, BList<long?>.Empty);
                cx.Add(e);
                ei += e.defpos;
            }
            var el = BList<long?>.Empty;
            if (tok == Qlx.ELSE)
            {
                Next();
                el = ParseStatementList(xp);
            }
            Mustbe(Qlx.END);
            Mustbe(Qlx.IF);
            var ife = new IfThenElse(lp, se, th, ei, el);
            cx = old;
            var r = (Executable)cx.Add(ife);
            if (cx.parse == ExecuteStatus.Obey)
                r.Obey(cx);
            return r;
        }
        /// <summary>
		/// Statements = 	Statement { ';' Statement } .
        /// </summary>
        /// <param name="xp">The obs type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		BList<long?> ParseStatementList(Domain xp)
        {
            if (ParseStatement(xp, true) is not Executable a)
                throw new DBException("42161", "statement");
            var r = BList<long?>.Empty + cx.Add(a).defpos;
            while (tok == Qlx.SEMICOLON)
            {
                Next();
                if (ParseStatement(xp,true) is not Executable b)
                    throw new DBException("42161", "statement");
                r += cx.Add(b).defpos;
            }
            return r;
        }
        /// <summary>
		/// SelectSingle =	[DINSTINCT] SelectList INTO VariableRef { ',' VariableRef } TableExpression .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSelectSingle(Ident id, Domain xp)
        {
            cx.IncSD(id);
            Next();
            //     var ss = new SelectSingle(LexPos());
            //     var qe = new RowSetExpr(lp+1);
            //     var s = new RowSetSpec(lp+2,cx,xp) + 
            //                  (QueryExpression._Distinct, ParseDistinctClause())
            var d = ParseDistinctClause();
            var dm = ParseSelectList(id.iix.dp, xp);
            Mustbe(Qlx.INTO);
            var ts = ParseTargetList();
            //      cs = cs + ;
            //      qe+=(RowSetExpr._Left,cx.Add(s).defpos);
            //     ss += (SelectSingle.Outs,ts);
            if (ts.Count != dm.rowType.Length)
                throw new DBException("22007").ISO();
            //      s+=(RowSetSpec.TableExp,ParseTableExpression(s).defpos);
            RowSet te = ParseTableExpression(id.iix, dm);
            if (d)
                te = new DistinctRowSet(cx, te);
            var cs = new SelectStatement(id.iix.dp, te);
            var ss = new SelectSingle(id.iix.dp) + (ForSelectStatement.Sel, cs);
            cx.DecSD();
            cx.exec = ss;
            return (Executable)cx.Add(ss);
        }
        /// <summary>
        /// traverse a comma-separated variable tree
        /// </summary>
        /// <returns>the tree</returns>
		BList<long?> ParseTargetList()
        {
            bool b = (tok == Qlx.LPAREN);
            if (b)
                Next();
            var r = BList<long?>.Empty;
            for (; ; )
            {
                var v = ParseVarOrColumn(Domain.Content);
                cx.Add(v);
                r += v.defpos;
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            if (b)
                Mustbe(Qlx.RPAREN);
            return r;
        }
        /// <summary>
        /// parse a dotted identifier chain. Watch for pseudo TableColumns
        /// CHECK ROW PARTITION VERSIONING PROVENANCE TYPE_URI SYSTEM_TIME
        /// The result will get classified as variable or ident
        /// during the Analysis stage Selects when things get setup
        /// </summary>
        /// <param name="ppos">the lexer position</param>
        /// <returns>an sqlName </returns>
        QlValue ParseVarOrColumn(Domain xp)
        {
            Match(Qlx.SYSTEM_TIME, Qlx.SECURITY);
            if (tok == Qlx.SECURITY)
            {
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER);
                var sp = LexPos();
                Next();
                return (QlValue)cx.Add(new SqlFunction(sp.dp, cx, Qlx.SECURITY, null, null, null, Qlx.NO));
            }
            if (Match(Qlx.PARTITION, Qlx.POSITION, Qlx.VERSIONING, Qlx.CHECK,
                Qlx.SYSTEM_TIME, Qlx.LAST_DATA))
            {
                QlValue ps = new SqlFunction(LexPos().dp, cx, tok, null, null, null, Qlx.NO);
                Next();
                if (tok == Qlx.LPAREN && ((SqlFunction)ps).domain.kind == Qlx.VERSIONING)
                {
                    var vp = LexPos();
                    Next();
                    if (tok == Qlx.SELECT)
                    {
                        var cs = ParseCursorSpecification(Domain.Null).union;
                        Mustbe(Qlx.RPAREN);
                        var sv = (QlValue)cx.Add(new SqlValueSelect(vp.dp, cx,
                            (RowSet?)cx.obs[cs] ?? throw new DBException("42000", "Version"), xp));
                        ps += (cx, SqlFunction._Val, sv.defpos);
                    }
                    else
                        Mustbe(Qlx.RPAREN);
                }
                return (QlValue)cx.Add(ps);
            }
            var ttok = tok;
            var (ic, il) = ParseIdentChain(true);
            var lp = LexPos();
            if (tok == Qlx.LPAREN)
            {
                Next();
                var ps = BList<long?>.Empty;
                if (tok != Qlx.RPAREN)
                    ps = ParseSqlValueList(Domain.Content);
                Mustbe(Qlx.RPAREN);
                var n = cx.Signature(ps);
                if (ic.Length == 0 || ic[ic.Length - 1] is not Ident pn)
                    throw new DBException("42000", "Signature");
                if (ic.Length == 1)
                {
                    var pr = cx.GetProcedure(LexPos().dp, pn.ident, n);
                    if (pr == null && (cx.db.objects[cx.role.dbobjects[pn.ident] ?? -1L]
                        ?? StandardDataType.Get(ttok)) is Domain ut && ut != Domain.Content)
                    {
                        cx.Add(ut);
                        var oi = ut.infos[cx.role.defpos];
                        if (cx.db.objects[oi?.methodInfos[pn.ident]?[n] ?? -1L] is Method me)
                            return (QlValue)cx.Add(new SqlConstructor(lp.dp, cx, me, ps));
                        if (Context.CanCall(cx.Signature(ut.rowType), n) || ttok != Qlx.Id || ut.rowType == BList<long?>.Empty)
                            return (QlValue)cx.Add(new SqlDefaultConstructor(pn.iix.dp, cx, ut, ps));
                    }
                    if (pr == null)
                        throw new DBException("42108", ic.ident);
                    return (QlValue)cx.Add(new SqlProcedureCall(lp.dp, cx, pr, ps));
                }
                else if (ic.Prefix(ic.Length - 2) is Ident pf)
                {
                    var vr = (QlValue)Identify(pf, il, Domain.Null);
                    cx.undefined += (lp.dp, cx.sD);
                    return (QlValue)cx.Add(new SqlMethodCall(lp.dp, cx, ic.sub?.ident ?? "", ps, vr));
                }
            }
            var ob = Identify(ic, il, xp);
            if (ob is not QlValue r)
                throw new DBException("42112", ic.ToString());
            return r;
        }
        DBObject Identify(Ident ic, BList<Ident> il, Domain xp)
        {
            if (cx.user == null)
                throw new DBException("42105").Add(Qlx.USER);
            // See SourceIntro.docx section 6.1.2
            // we look up the identifier chain ic
            // and perform 6.1.2 (2) if we find anything
            var len = ic.Length;
            var (pa, sub) = cx.Lookup(LexPos().dp, ic, len);
            // pa is the object that was found, or null
            var m = sub?.Length ?? 0;
            if (pa is SqlCopy sc && cx.db.objects[sc.copyFrom] is TableColumn tc
                && cx.db.objects[tc.tabledefpos] is NodeType nt && sub != null)
            {
                cx.Add(pa);
                cx.AddDefs(nt);
                var sb = Identify(sub, il - 0, xp);
                sb += (DBObject._From, pa.defpos);
                return sb;
            }
            if (pa is QlValue pp && pp.domain.kind == Qlx.PATH && sub is not null)
            {
                var pf = (GqlNode)Identify(sub, BList<Ident>.Empty, Domain.Content);
                var pc = new SqlValueExpr(sub.iix.dp, cx, Qlx.PATH, pp, pf, Qlx.NO);
                cx.Add(pf);
                return cx.Add(pc);
            }
            if (pa is QlValue sv && sv.domain.infos[cx.role.defpos] is ObInfo si && ic.sub is not null
                && si.names[ic.sub.ident].Item2 is long sp && cx.db.objects[sp] is TableColumn tc1)
            {
                var co = new SqlCopy(ic.sub.iix.dp, cx, ic.sub.ident, sv.defpos, tc1);
                var nc = new SqlValueExpr(ic.iix.dp, cx, Qlx.DOT, sv, co, Qlx.NO);
                cx.Add(co);
                return cx.Add(nc);
            }
            if (pa is QlValue sv1 && sv1.domain is NodeType && sub is not null)
            {
                var co = new SqlField(sub.iix.dp, sub.ident, -1, sv1.defpos, Domain.Content, sv1.defpos);
                return cx.Add(co);
            }
            if (cx.locals.Contains(ic.iix.dp)) // a binding or local variable
                return new QlValue(ic.iix.dp, cx.obs[ic.iix.dp]?.mem??BTree<long,object>.Empty+(ObInfo.Name,ic.ident));
            // if sub is non-zero there is a new chain to construct
            var nm = len - m;
            DBObject ob;
            // nm is the position  of the first such in the chain ic
            // create the missing components if any (see 6.1.2 (1))
            for (var i = nm; i < len; i++)
                if (ic[i] is Ident c)
                {// the ident of the component to create
                    if (i == len - 1)
                    {
                        ob = new SqlReview(c, ic, il, cx, xp) ?? throw new PEException("PE1561");
                        cx.Add(ob);
                        // cx.defs enables us to find these objects again
                        cx.defs += (c, 1);
                        cx.defs += (ic, ic.Length);
                        if (!lxr.ParsingMatch) // flag as undefined unless we are parsing a MATCH
                            cx.undefined += (ob.defpos, cx.sD);
                        else if (ic[i - 1] is Ident ip && cx.defs[ip.ident]?[cx.sD].Item1 is Iix px)
                        {
                            var pb = cx.obs[px.dp] ?? new QlValue(new Ident(ip.ident, px), il, cx, xp);
                            cx.Add(pb);
                            ob = new SqlField(ob.defpos, c.ident, -1, pb.defpos, xp, pb.defpos);
                            cx.Add(ob);
                        }
                        pa = ob;
                    }
                    else
                        new ForwardReference(c, il, cx);
                }
            if (pa == null)
                throw new PEException("PE1562");
            if (pa.defpos >= Transaction.Executables && ic.iix.dp < Transaction.Executables)
            {
                var nv = pa.Relocate(ic.iix.dp);
                cx.Replace(pa, nv);
                return nv;
            }
            return pa;
        }
        /// <summary>
        /// Parse an identifier
        /// </summary>
        /// <returns>the Ident</returns>
        Ident ParseIdent(bool stdfns = false)
        {
            var c = new Ident(this);
            if (stdfns && StartStdFunctionRefs())
                Next();
            else
                Mustbe(Qlx.Id, Qlx.PARTITION, Qlx.POSITION, Qlx.VERSIONING, Qlx.CHECK,
                    Qlx.SYSTEM_TIME);
            return c;
        }
        /// <summary>
        /// Parse a IdentChain
        /// </summary>
        /// <returns>the Ident</returns>
		(Ident, BList<Ident>) ParseIdentChain(bool stdfns=false, BList<Ident>? il = null)
        {
            il ??= BList<Ident>.Empty;
            var left = ParseIdent(stdfns);
            il += left;
            if (tok == Qlx.DOT)
            {
                Next();
                if (!Match(Qlx.Id)) // allow VERSIONING etc to follow - see  ParseVarOrColum
                    return (left, il);
                (var sub, il) = ParseIdentChain(false,il);
                left = new Ident(left, sub);
            }
            return (left, il);
        }
        /// <summary>
		/// LoopStatement =	Label LOOP Statements END LOOP .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The  type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseLoopStatement(Domain xp, string? n)
        {
            var ls = new LoopStatement(LexDp(), n ?? "");
            Next();
            ls += (CompoundStatement.Stms, ParseStatementList(xp));
            Mustbe(Qlx.END);
            Mustbe(Qlx.LOOP);
            if (tok == Qlx.Id && n != null && n == lxr.val.ToString())
                Next();
            return (Executable)cx.Add(ls);
        }
        /// <summary>
		/// While =		Label WHILE QlValue DO Statements END WHILE .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseSqlWhile(Domain xp, string? n)
        {
            var ws = new WhileStatement(LexDp(), n ?? "");
            var old = cx; // new SaveContext(lxr, ExecuteStatus.Parse);
            Next();
            var s = ParseSqlValue(Domain.Bool);
            cx.Add(s);
            ws += (IfThenElse.Search, s.defpos);
            Mustbe(Qlx.DO);
            ws += (cx, WhileStatement.What, ParseStatementList(xp));
            Mustbe(Qlx.END);
            Mustbe(Qlx.WHILE);
            if (tok == Qlx.Id && n != null && n == lxr.val.ToString())
                Next();
            cx = old; // old.Restore(lxr);
            cx.exec = ws;
            return (Executable)cx.Add(ws);
        }
        /// <summary>
		/// Repeat =		Label REPEAT Statements UNTIL BooleanExpr END REPEAT .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The obs type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseRepeat(Domain xp, string? n)
        {
            var rs = new RepeatStatement(LexDp(), n ?? "");
            Next();
            rs += (cx, WhileStatement.What, ParseStatementList(xp));
            Mustbe(Qlx.UNTIL);
            var s = ParseSqlValue(Domain.Bool);
            cx.Add(s);
            rs += (IfThenElse.Search, s.defpos);
            Mustbe(Qlx.END);
            Mustbe(Qlx.REPEAT);
            if (tok == Qlx.Id && n != null && n == lxr.val.ToString())
                Next();
            cx.exec = rs;
            return (Executable)cx.Add(rs);
        }
        /// <summary>
        /// Parse a break or leave statement
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseBreakLeave()
        {
            Match(Qlx.BREAK, Qlx.LEAVE);
            Qlx s = tok;
            Ident? n = null;
            Next();
            if (s == Qlx.LEAVE && tok == Qlx.Id)
            {
                n = new Ident(this);
                Next();
            }
            return (Executable)cx.Add(new BreakStatement(LexDp(), n?.ident));
        }
        /// <summary>
        /// Parse an iterate statement
        /// </summary>
        /// <param name="f">The procedure whose body is being defined if any</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseIterate()
        {
            Next();
            var n = new Ident(this);
            Mustbe(Qlx.Id);
            return (Executable)cx.Add(new IterateStatement(LexDp(), n.ident));
        }
        /// <summary>
        /// |	SIGNAL (id|SQLSTATE [VALUE] string) [SET item=Value {,item=Value} ]
        /// |   RESIGNAL [id|SQLSTATE [VALUE] string] [SET item=Value {,item=Value} ]
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSignal()
        {
            Qlx s = tok;
            TypedValue n;
            Next();
            if (tok == Qlx.Id)
            {
                n = lxr.val;
                Next();
            }
            else
            {
                Mustbe(Qlx.SQLSTATE);
                if (tok == Qlx.VALUE)
                    Next();
                n = lxr.val;
                Mustbe(Qlx.CHARLITERAL);
            }
            var r = new SignalStatement(LexDp(), n.ToString()) + (SignalStatement.SType, s);
            if (tok == Qlx.SET)
            {
                Next();
                for (; ; )
                {
                    Match(Qlx.CLASS_ORIGIN, Qlx.SUBCLASS_ORIGIN, Qlx.CONSTRAINT_CATALOG,
                        Qlx.CONSTRAINT_SCHEMA, Qlx.CONSTRAINT_NAME, Qlx.CATALOG_NAME,
                        Qlx.SCHEMA_NAME, Qlx.TABLE_NAME, Qlx.COLUMN_NAME, Qlx.CURSOR_NAME,
                        Qlx.MESSAGE_TEXT);
                    var k = tok;
                    Mustbe(Qlx.CLASS_ORIGIN, Qlx.SUBCLASS_ORIGIN, Qlx.CONSTRAINT_CATALOG,
                        Qlx.CONSTRAINT_SCHEMA, Qlx.CONSTRAINT_NAME, Qlx.CATALOG_NAME,
                        Qlx.SCHEMA_NAME, Qlx.TABLE_NAME, Qlx.COLUMN_NAME, Qlx.CURSOR_NAME,
                        Qlx.MESSAGE_TEXT);
                    Mustbe(Qlx.EQL);
                    var sv = ParseSqlValue(Domain.Content);
                    cx.Add(sv);
                    r += (cx, SignalStatement.SetList, r.setlist + (k, sv.defpos));
                    if (tok != Qlx.COMMA)
                        break;
                    Next();
                }
            }
            cx.exec = r;
            return (Executable)cx.Add(r);
        }
        /// <summary>
		/// Open =		OPEN id .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
 		Executable ParseOpenStatement()
        {
            Next();
            var o = new Ident(this);
            Mustbe(Qlx.Id);
            return (Executable)cx.Add(new OpenStatement(LexDp(),
                cx.Get(o, Domain.TableType) as SqlCursor
                ?? throw new DBException("34000", o.ToString())));
        }
        /// <summary>
		/// Close =		CLOSE id .
        /// </summary>
        /// <returns>The Executable result of the parse</returns>
		Executable ParseCloseStatement()
        {
            Next();
            var o = new Ident(this);
            Mustbe(Qlx.Id);
            return (Executable)cx.Add(new CloseStatement(LexDp(),
                cx.Get(o, Domain.TableType) as SqlCursor
                ?? throw new DBException("34000", o.ToString())));
        }
        /// <summary>
		/// Fetch =		FETCH Cursor_id INTO VariableRef { ',' VariableRef } .
        /// </summary>
        /// <returns>The Executable result of the parse</returns>
        Executable ParseFetchStatement()
        {
            Next();
            var dp = LexDp();
            var how = Qlx.NEXT;
            QlValue? where = null;
            if (Match(Qlx.NEXT, Qlx.PRIOR, Qlx.FIRST,
                Qlx.LAST, Qlx.ABSOLUTE, Qlx.RELATIVE))
            {
                how = tok;
                Next();
            }
            if (how == Qlx.ABSOLUTE || how == Qlx.RELATIVE)
                where = ParseSqlValue(Domain.Int);
            if (tok == Qlx.FROM)
                Next();
            var o = new Ident(this);
            Mustbe(Qlx.Id);
            var fs = new FetchStatement(dp,
                cx.Get(o, Domain.TableType) as SqlCursor
                ?? throw new DBException("34000", o.ToString()),
                how, where);
            Mustbe(Qlx.INTO);
            fs += (FetchStatement.Outs, ParseTargetList());
            return (Executable)cx.Add(fs);
        }
        /// <summary>
        /// [ TriggerCond ] (Call | (BEGIN ATOMIC Statements END)) .
		/// TriggerCond = WHEN '(' QlValue ')' .
        /// </summary>
        /// <returns>a TriggerAction</returns>
        internal long ParseTriggerDefinition(PTrigger trig)
        {
            long oldStart = cx.parseStart; // safety
            cx.parse = ExecuteStatus.Parse;
            cx.parseStart = LexPos().lp;
            var op = cx.parse;
            var tn = cx.NameFor(trig.target) ?? throw new DBException("42000");
            var fn = new Ident(tn, LexPos());
            var tb = cx.db.objects[trig.target] as Table
                ?? throw new PEException("PE1562");
            tb = (Table)cx.Add(tb);
            cx.Add(tb.framing);
            var fm = tb.RowSets(fn, cx, tb, fn.iix.dp);
            trig.from = fm.defpos;
            trig.dataType = fm;
            var tg = new Trigger(trig, cx.role);
            cx.Add(tg); // incomplete version for parsing
            if (trig.oldTable != null)
            {
                var tt = (TransitionTable)cx.Add(new TransitionTable(trig.oldTable, true, cx, fm, tg));
                var nix = new Iix(trig.oldTable.iix, tt.defpos);
                cx.defs += (trig.oldTable, nix);
            }
            if (trig.oldRow != null)
            {
                cx.Add(new SqlOldRow(trig.oldRow, cx, fm));
                cx.defs += (trig.oldRow, trig.oldRow.iix);
            }
            if (trig.newTable != null)
            {
                var tt = (TransitionTable)cx.Add(new TransitionTable(trig.newTable, true, cx, fm, tg));
                var nix = new Iix(trig.newTable.iix, tt.defpos);
                cx.defs += (trig.newTable, nix);
            }
            if (trig.newRow != null)
            {
                cx.Add(new SqlNewRow(trig.newRow, cx, fm));
                cx.defs += (trig.newRow, trig.newRow.iix);
            }
            for (var b = trig.dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string n)
                {
                    var px = new Iix(fn.iix, p);
                    cx.defs += (n, px, Ident.Idents.Empty);
                }
            QlValue? when = null;
            Executable? act;
            if (tok == Qlx.WHEN)
            {
                Next();
                when = ParseSqlValue(Domain.Bool);
            }
            if (Match(Qlx.BEGIN))
            {
                Next();
                if (new CompoundStatement(LexDp(), "") is not CompoundStatement cs)
                    throw new DBException("42161", "CompoundStatement");
                var ss = BList<long?>.Empty;
                Mustbe(Qlx.ATOMIC);
                while (!Match(Qlx.END))
                {
                    if (ParseStatement(Domain.Content,true) is not Executable a)
                        throw new DBException("42161", "statement");
                    ss += cx.Add(a).defpos;
                    if (Match(Qlx.END))
                        break;
                    Mustbe(Qlx.SEMICOLON);
                }
                Next();
                cs += (cx, CompoundStatement.Stms, ss);
                act = cs;
            }
            else
                act = ParseStatement(Domain.Content,true) ??
                    throw new DBException("42161", "statement");
            cx.Add(act);
            var r = (WhenPart)cx.Add(new WhenPart(LexDp(), when, new BList<long?>(act.defpos)));
            trig.def = r.defpos;
            trig.framing = new Framing(cx, trig.nst);
            cx.Add(tg + (Trigger.Action, r.defpos));
            cx.parseStart = oldStart;
            cx.result = -1L;
            cx.parse = op;
            return r.defpos;
        }
        /// <summary>
        /// |	CREATE TRIGGER id (BEFORE|AFTER) Event ON id [ RefObj ] Trigger
        /// RefObj = REFERENCING  { (OLD|NEW)[ROW|TABLE][AS] id } .
        /// Trigger = FOR EACH ROW ...
        /// </summary>
        /// <returns>the executable</returns>
        void ParseTriggerDefClause()
        {
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            var nst = cx.db.nextStmt;
            var trig = new Ident(this);
            Mustbe(Qlx.Id);
            PTrigger.TrigType tgtype = 0;
            var w = Mustbe(Qlx.BEFORE, Qlx.INSTEAD, Qlx.AFTER);
            switch (w)
            {
                case Qlx.BEFORE: tgtype |= PTrigger.TrigType.Before; break;
                case Qlx.AFTER: tgtype |= PTrigger.TrigType.After; break;
                case Qlx.INSTEAD: Mustbe(Qlx.OF); tgtype |= PTrigger.TrigType.Instead; break;
            }
            tgtype = ParseTriggerHow(tgtype);
            var cls = BList<Ident>.Empty;
            var upd = (tgtype & PTrigger.TrigType.Update) == PTrigger.TrigType.Update;
            if (upd && Match(Qlx.OF))
            {
                Next();
                cls += new Ident(this);
                Mustbe(Qlx.Id);
                while (tok == Qlx.COMMA)
                {
                    Next();
                    cls += new Ident(this);
                    Mustbe(Qlx.Id);
                }
            }
            Mustbe(Qlx.ON);
            var tabl = new Ident(this);
            Mustbe(Qlx.Id);
            var tb = cx.GetObject(tabl.ident) as Table ?? throw new DBException("42107", tabl.ToString()).Mix();
            Ident? or = null, nr = null, ot = null, nt = null;
            if (Match(Qlx.REFERENCING))
            {
                Next();
                while (Match(Qlx.OLD,Qlx.NEW))
                {
                    if (tok == Qlx.OLD)
                    {
                        if ((tgtype & PTrigger.TrigType.Insert) == PTrigger.TrigType.Insert)
                            throw new DBException("42146", "OLD", "INSERT").Mix();
                        Next();
                        if (Match(Qlx.TABLE))
                        {
                            Next();
                            if (ot != null)
                                throw new DBException("42143", "OLD").Mix();
                            if (tok == Qlx.AS)
                                Next();
                            ot = new Ident(this);
                            Mustbe(Qlx.Id);
                            continue;
                        }
                        if (Match(Qlx.ROW))
                            Next();
                        if (or != null)
                            throw new DBException("42143", "OLD ROW").Mix();
                        if (tok == Qlx.AS)
                            Next();
                        or = new Ident(this);
                        Mustbe(Qlx.Id);
                    }
                    else
                    {
                        Mustbe(Qlx.NEW);
                        if ((tgtype & PTrigger.TrigType.Delete) == PTrigger.TrigType.Delete)
                            throw new DBException("42146", "NEW", "DELETE").Mix();
                        if (tok == Qlx.TABLE)
                        {
                            Next();
                            if (nt != null)
                                throw new DBException("42143", "NEW").Mix();
                            nt = new Ident(lxr.val.ToString(), tabl.iix);
                            Mustbe(Qlx.Id);
                            continue;
                        }
                        if (Match(Qlx.ROW))
                            Next();
                        if (nr != null)
                            throw new DBException("42143", "NEW ROW").Mix();
                        if (tok == Qlx.AS)
                            Next();
                        nr = new Ident(lxr.val.ToString(), tabl.iix);
                        Mustbe(Qlx.Id);
                    }
                }
            }
            if (tok == Qlx.FOR)
            {
                Next();
                Mustbe(Qlx.EACH);
                if (Match(Qlx.ROW))
                {
                    Next();
                    tgtype |= PTrigger.TrigType.EachRow;
                }
                else
                {
                    Mustbe(Qlx.STATEMENT);
                    if (Match(Qlx.DEFERRED))
                    {
                        tgtype |= PTrigger.TrigType.Deferred;
                        Next();
                    }
                    tgtype |= PTrigger.TrigType.EachStatement;
                }
            }
            if ((tgtype & PTrigger.TrigType.EachRow) != PTrigger.TrigType.EachRow)
            {
                if (nr != null || or != null)
                    throw new DBException("42148").Mix();
            }
            var st = lxr.start;
            var cols = BList<long?>.Empty;
            for (var b = cls?.First(); b is not null; b = b.Next())
                if (cx.defs[b.value()] is Iix xi)
                    cols += xi.dp;
            var np = cx.db.nextPos;
            var pt = new PTrigger(trig.ident, tb.defpos, (int)tgtype, cols,
                    or, nr, ot, nt,
                    new Ident(new string(lxr.input, st, lxr.input.Length - st),
                        cx.Ix(st)),
                    nst, cx, np);
            var ix = LexPos();
            ParseTriggerDefinition(pt);
            pt.src = new Ident(new string(lxr.input, st, lxr.pos - st), ix);
            cx.parse = op;
            pt.framing = new Framing(cx, nst);
            cx.Add(pt);
            cx.result = -1L;
        }
        /// <summary>
        /// Event = 	INSERT | DELETE | (UPDATE [ OF id { ',' id } ] ) .
        /// </summary>
        /// <param name="type">ref: the trigger type</param>
		PTrigger.TrigType ParseTriggerHow(PTrigger.TrigType type)
        {
            if (tok == Qlx.INSERT)
            {
                Next();
                type |= PTrigger.TrigType.Insert;
            }
            else if (Match(Qlx.UPDATE))
            {
                Next();
                type |= PTrigger.TrigType.Update;
            }
            else
            {
                Mustbe(Qlx.DELETE);
                type |= PTrigger.TrigType.Delete;
            }
            return type;
        }
        /// <summary>
		/// Alter =		ALTER DOMAIN id AlterDomain { ',' AlterDomain } 
		/// |	ALTER FUNCTION id '(' Parameters ')' RETURNS Type Statement
		/// |	ALTER PROCEDURE id '(' Parameters ')' Statement
		/// |	ALTER Method Statement
        /// |   ALTER TABLE id TO id
		/// |	ALTER TABLE id AlterTable { ',' AlterTable } 
        /// |	ALTER TYPE id AlterType { ',' AlterType } 
        /// |   ALTER VIEW id AlterView { ',' AlterView } 
        /// </summary>
        /// <returns>A tree of Executable references</returns>
		void ParseAlter()
        {
            if (cx.role.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole) == false)
                throw new DBException("42105").Add(Qlx.ADMIN);
            Next();
            MethodModes();
            Match(Qlx.CONSTRUCTOR, Qlx.DOMAIN, Qlx.FUNCTION, Qlx.INSTANCE, Qlx.METHOD,
                Qlx.OVERRIDING, Qlx.PROCEDURE, Qlx.STATIC, Qlx.TABLE, Qlx.TYPE, Qlx.ROLE, Qlx.VIEW);
            switch (tok)
            {
                case Qlx.CONSTRUCTOR: ParseMethodDefinition(); break;
                case Qlx.DOMAIN: ParseAlterDomain(); break;
                case Qlx.FUNCTION: ParseAlterProcedure(); break;
                case Qlx.INSTANCE: ParseMethodDefinition(); break;
                case Qlx.METHOD: ParseMethodDefinition(); break;
                case Qlx.OVERRIDING: ParseMethodDefinition(); break;
                case Qlx.PROCEDURE: ParseAlterProcedure(); break;
                case Qlx.STATIC: ParseMethodDefinition(); break;
                case Qlx.TABLE: ParseAlterTable(); break;
                case Qlx.TYPE: ParseAlterType(); break;
                case Qlx.VIEW: ParseAlterView(); break;
                default:
                    throw new DBException("42125", tok).Mix();
            }
        }
        /// <summary>
        /// id AlterTable { ',' AlterTable } 
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        void ParseAlterTable()
        {
            Next();
            Table tb;
            var o = new Ident(this);
            Mustbe(Qlx.Id);
            tb = cx.GetObject(o.ident) as Table ??
                throw new DBException("42107", o).Mix();
            ParseAlterTableOps(tb);
        }
        /// <summary>
        /// AlterView = SET (INSERT|UPDATE|DELETE) SqlStatement
        ///     |   TO id
        ///     |   SET SOURCE To QueryExpression
        ///     |   [DROP]TableMetadata
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        void ParseAlterView()
        {
            Next();
            var nm = new Ident(this);
            Mustbe(Qlx.Id);
            Domain vw = (Domain)(cx.GetObject(nm.ident) ??
                throw new DBException("42107", nm).Mix());
            ParseAlterOp(vw);
            while (tok == Qlx.COMMA)
            {
                Next();
                ParseAlterOp(vw);
            }
        }
        /// <summary>
        /// Parse an alter operation
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="ob">the object to be affected</param>
        /// <returns>A tree of Executable references</returns>
        void ParseAlterOp(Domain ob)
        {
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var kind = (ob is View) ? Qlx.VIEW : Qlx.FUNCTION;
            long np;
            if (tok == Qlx.SET && ob is View)
            {
                Next();
                int st;
                Ident s;
                var lp = LexPos();
                Mustbe(Qlx.SOURCE);
                Mustbe(Qlx.TO);
                st = lxr.start;
                RowSet qe;
                qe = ParseRowSetSpec(ob);
                s = new Ident(new string(lxr.input, st, lxr.start - st), lp);
                cx.Add(new Modify("Source", ob.defpos, qe, s, cx.db.nextPos, cx));
            }
            else if (tok == Qlx.TO)
            {
                Next();
                var nm = lxr.val;
                Mustbe(Qlx.Id);
                cx.Add(new Change(ob.defpos, nm.ToString(), cx.db.nextPos, cx));
            }
            else if (Match(Qlx.ALTER))
            {
                Next();
                var ic = new Ident(this);
                Mustbe(Qlx.Id);
                ob = (Domain)(cx.GetObject(ic.ident) ??
                    throw new DBException("42135", ic.ident));
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Add(Qlx.METADATA).Mix();
                var m = ParseMetadata(Qlx.COLUMN);
                cx.Add(new PMetadata(ic.ident, 0, ob, m, cx.db.nextPos));
            }
            if (StartMetadata(kind) || Match(Qlx.ADD))
            {
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Add(Qlx.METADATA).Mix();
                if (Match(Qlx.ALTER))
                    Next();
                var m = ParseMetadata(kind);
                np = tr.nextPos;
                cx.Add(new PMetadata(ob.NameFor(cx), -1, ob, m, np));
            }
        }
        /// <summary>
        /// id AlterDomain { ',' AlterDomain } 
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        BList<long?> ParseAlterDomain()
        {
            Next();
            var c = ParseIdent();
            var es = BList<long?>.Empty;
            if (cx.GetObject(c.ident) is not Domain d)
                throw new DBException("42161", "domain id");
            es += ParseAlterDomainOp(d);
            while (tok == Qlx.COMMA)
            {
                Next();
                es += ParseAlterDomainOp(d);
            }
            return es;
        }
        /// <summary>
        /// AlterDomain =  SET DEFAULT TypedValue 
        /// |	DROP DEFAULT
        /// |	TYPE Type
        /// |	AlterCheck .
        /// AlterCheck =	ADD CheckConstraint 
        /// |	DROP CONSTRAINT id .
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="d">the domain object</param>
        /// <returns>A tree of Executable references</returns>
        BList<long?> ParseAlterDomainOp(Domain d)
        {
            var es = BList<long?>.Empty;
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            if (tok == Qlx.SET)
            {
                Next();
                Mustbe(Qlx.DEFAULT);
                int st = lxr.start;
                var dv = ParseSqlValue(Domain.For(d.kind));
                string ds = new(lxr.input, st, lxr.start - st);
                es += cx.Add(new Edit(d, d.name, d + (Domain.Default, dv) + (Domain.DefaultString, ds),
                    cx.db.nextPos, cx))?.defpos;
            }
            else if (Match(Qlx.ADD))
            {
                Next();
                Ident id;
                if (Match(Qlx.CONSTRAINT))
                {
                    Next();
                    id = new Ident(this);
                    Mustbe(Qlx.Id);
                }
                else
                    id = new Ident(this);
                Mustbe(Qlx.CHECK);
                Mustbe(Qlx.LPAREN);
                var nst = cx.db.nextStmt;
                int st = lxr.pos;
                var sc = ParseSqlValue(Domain.Bool).Reify(cx);
                string source = new(lxr.input, st, lxr.pos - st - 1);
                Mustbe(Qlx.RPAREN);
                var pc = new PCheck(d, id.ident, sc, source, nst, tr.nextPos, cx);
                es += cx.Add(pc)?.defpos;
            }
            else if (tok == Qlx.DROP)
            {
                Next();
                var dp = cx.db.Find(d)?.defpos ?? -1L;
                if (Match(Qlx.DEFAULT))
                {
                    Next();
                    es += cx.Add(new Edit(d, d.name, d, tr.nextPos, cx))?.defpos;
                }
                else if (StartMetadata(Qlx.DOMAIN) || Match(Qlx.ADD, Qlx.DROP))
                {
                    if (tr.role.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Add(Qlx.METADATA).Mix();
                    var m = ParseMetadata(Qlx.DOMAIN);
                    es += cx.Add(new PMetadata(d.name, -1, d, m, dp))?.defpos;
                }
                else
                {
                    Mustbe(Qlx.CONSTRAINT);
                    var n = new Ident(this);
                    Mustbe(Qlx.Id);
                    Drop.DropAction s = ParseDropAction();
                    var ch = (Check?)cx.GetObject(n.ident) ?? throw new DBException("42135", n.ident);
                    es += cx.Add(new Drop1(ch.defpos, s, tr.nextPos))?.defpos;
                }
            }
            else if (StartMetadata(Qlx.DOMAIN) || Match(Qlx.ADD, Qlx.DROP))
            {
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Add(Qlx.METADATA).Mix();
                es += cx.Add(new PMetadata(d.name, 0, d, ParseMetadata(Qlx.DOMAIN),
                    tr.nextPos))?.defpos;
            }
            else
            {
                Mustbe(Qlx.TYPE);
                var dt = ParseDataType();
                es += cx.Add(new Edit(d, d.name, dt, tr.nextPos, cx))?.defpos;
            }
            return es;
        }
        /// <summary>
        /// DropAction = | RESTRICT | CASCADE .
        /// </summary>
        /// <returns>RESTRICT (default) or CASCADE</returns>
		Drop.DropAction ParseDropAction()
        {
            Match(Qlx.RESTRICT, Qlx.CASCADE);
            Drop.DropAction r = 0;
            switch (tok)
            {
                case Qlx.CASCADE: r = Drop.DropAction.Cascade; Next(); break;
                case Qlx.RESTRICT: r = Drop.DropAction.Restrict; Next(); break;
            }
            return r;
        }
        /// <summary>
        /// |   ALTER TABLE id AlterTable { ',' AlterTable }
        /// </summary>
        /// <param name="tb">the database</param>
        /// <param name="tb">the table</param>
        /// <returns>A tree of Executable references</returns>
        void ParseAlterTableOps(Table tb)
        {
            cx.AddDefs(tb);
            ParseAlterTable(tb);
            while (tok == Qlx.COMMA)
            {
                Next();
                ParseAlterTable(tb);
            }
        }
        /// <summary>
        /// AlterTable =   TO id
        /// |   Enforcement
        /// |   ADD ColumnDefinition
        ///	|	ALTER [COLUMN] id AlterColumn { ',' AlterColumn }
        /// |	DROP [COLUMN] id DropAction
        /// |	(ADD|DROP) (TableConstraintDef |VersioningClause)
        /// |   SET TableConstraintDef ReferentialAction
        /// |   ADD TablePeriodDefinition [ AddPeriodColumnList ]
        /// |   ALTER PERIOD id To id
        /// |   DROP TablePeriodDefinition
        /// |   [DROP] Metadata
        /// |	AlterCheck .
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tb">the Table object</param>
        /// <returns>A tree of Executable references</returns>
        void ParseAlterTable(Table tb)
        {
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            cx.Add(tb.framing);
            if (Match(Qlx.TO))
            {
                Next();
                var o = lxr.val;
                Mustbe(Qlx.Id);
                cx.Add(new Change(tb.defpos, o.ToString(), tr.nextPos, cx));
                return;
            }
            if (Match(Qlx.LEVEL))
                ParseClassification(tb);
            if (Match(Qlx.SCOPE))
                ParseEnforcement(tb);
            Match(Qlx.ADD,Qlx.PRIMARY, Qlx.FOREIGN, Qlx.UNIQUE, Qlx.PERIOD, Qlx.CONSTRAINT, Qlx.ALTER);
            switch (tok)
            {
                case Qlx.CONSTRAINT:
                    ParseCheckConstraint(tb);
                    return;
                case Qlx.DROP:
                    {
                        Next();
                        Match(Qlx.PRIMARY, Qlx.FOREIGN, Qlx.UNIQUE, Qlx.PERIOD, Qlx.CONSTRAINT);
                        switch (tok)
                        {
                            case Qlx.CONSTRAINT:
                                {
                                    Next();
                                    var name = new Ident(this);
                                    Mustbe(Qlx.Id);
                                    Drop.DropAction act = ParseDropAction();
                                    if (cx.GetObject(name.ident) is Check ck)
                                        cx.Add(new Drop1(ck.defpos, act, tr.nextPos));
                                    return;
                                }
                            case Qlx.PRIMARY:
                                {
                                    Next();
                                    Mustbe(Qlx.KEY);
                                    Drop.DropAction act = ParseDropAction();
                                    if (ParseColsList(tb) is not Domain cols)
                                        throw new DBException("42161", "cols");
                                    cols += (Domain.Kind, Qlx.ROW);
                                    Level3.Index x = (tb).FindIndex(cx.db, cols)?[0]
                                        ?? throw new DBException("42164", tb.NameFor(cx));
                                    if (x != null)
                                        cx.Add(new Drop1(x.defpos, act, tr.nextPos));
                                    return;
                                }
                            case Qlx.FOREIGN:
                                {
                                    Next();
                                    Mustbe(Qlx.KEY);
                                    if (ParseColsList(tb) is not Domain cols)
                                        throw new DBException("42161", "cols");
                                    Mustbe(Qlx.REFERENCES);
                                    var n = new Ident(this);
                                    Mustbe(Qlx.Id);
                                    var rt = cx.GetObject(n.ident) as Table ??
                                        throw new DBException("42107", n).Mix();
                                    var st = lxr.pos;
                                    if (tok == Qlx.LPAREN && ParseColsList(rt) is null)
                                        throw new DBException("42161", "rcols");
                                    var x = (tb).FindIndex(cx.db, cols)?[0];
                                    if (x != null)
                                    {
                                        cx.Add(new Drop(x.defpos, tr.nextPos));
                                        return;
                                    }
                                    throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                                }
                            case Qlx.UNIQUE:
                                {
                                    Next();
                                    var st = lxr.pos;
                                    if (ParseColsList(tb) is not Domain cols)
                                        throw new DBException("42161", "cols");
                                    var x = tb.FindIndex(cx.db, cols)?[0];
                                    if (x != null)
                                    {
                                        cx.Add(new Drop(x.defpos, tr.nextPos));
                                        return;
                                    }
                                    throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                                }
                            case Qlx.PERIOD:
                                {
                                    var ptd = ParseTablePeriodDefinition();
                                    var pd = (ptd.pkind == Qlx.SYSTEM_TIME) ? tb.systemPS : tb.applicationPS;
                                    if (pd > 0)
                                        cx.Add(new Drop(pd, tr.nextPos));
                                    return;
                                }
                            case Qlx.WITH:
                                ParseVersioningClause(tb, true);
                                return;
                            default:
                                {
                                    if (StartMetadata(Qlx.TABLE))
                                    {
                                        if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                                            throw new DBException("42105").Add(Qlx.METADATA).Mix();
                                        cx.Add(new PMetadata(tb.NameFor(cx), 0, tb,
                                                ParseMetadata(Qlx.TABLE), tr.nextPos));
                                        return;
                                    }
                                    if (Match(Qlx.COLUMN))
                                        Next();
                                    var name = new Ident(this);
                                    Mustbe(Qlx.Id);
                                    Drop.DropAction act = ParseDropAction();
                                    var tc = (TableColumn?)tr.objects[tb.ColFor(cx, name.ident)]
                                       ?? throw new DBException("42135", name);
                                    if (tc != null)
                                        cx.Add(new Drop1(tc.defpos, act, tr.nextPos));
                                    return;
                                }
                        }
                    }
                case Qlx.ADD:
                    {
                        Next();
                        if (Match(Qlx.PERIOD))
                            AddTablePeriodDefinition(tb);
                        else if (tok == Qlx.WITH)
                            ParseVersioningClause(tb, false);
                        else if (Match(Qlx.CONSTRAINT,Qlx.UNIQUE,Qlx.PRIMARY,Qlx.FOREIGN,Qlx.CHECK))
                            ParseTableConstraintDefin(tb);
                        else
                            ParseColumnDefin(tb);
                        break;
                    }
                case Qlx.ALTER:
                    {
                        Next();
                        if (Match(Qlx.COLUMN))
                            Next();
                        var o = new Ident(this);
                        Mustbe(Qlx.Id);
                        var col = (TableColumn?)cx.db.objects[tb.ColFor(cx, o.ident)]
                                ?? throw new DBException("42112", o.ident);
                        while (StartMetadata(Qlx.COLUMN) || Match(Qlx.TO, Qlx.POSITION, Qlx.SET, Qlx.DROP, Qlx.ADD, Qlx.TYPE))
                            tb = ParseAlterColumn(tb, col, o.ident);
                        return;
                    }
                case Qlx.PERIOD:
                    {
                        if (Match(Qlx.Id))
                        {
                            var pid = lxr.val;
                            Next();
                            Mustbe(Qlx.TO);
                            if (cx.db.objects[tb.applicationPS] is not PeriodDef pd)
                                throw new DBException("42162", pid).Mix();
                            pid = lxr.val;
                            Mustbe(Qlx.Id);
                            cx.Add(new Change(pd.defpos, pid.ToString(), tr.nextPos, cx));
                        }
                        tb = AddTablePeriodDefinition(tb);
                        return;
                    }
                case Qlx.SET:
                    {
                        Next();
                        if (ParseColsList(tb) is not Domain cols)
                            throw new DBException("42161", "cols");
                        Mustbe(Qlx.REFERENCES);
                        var n = new Ident(this);
                        Mustbe(Qlx.Id);
                        var rt = cx.GetObject(n.ident) as Table ??
                            throw new DBException("42107", n).Mix();
                        var st = lxr.pos;
                        if (tok == Qlx.LPAREN && ParseColsList(rt) is null)
                            throw new DBException("42161", "cols");
                        PIndex.ConstraintType ct = 0;
                        if (Match(Qlx.ON))
                            ct = ParseReferentialAction();
                        cols += (Domain.Kind, Qlx.ROW);
                        if (tb.FindIndex(cx.db, cols, PIndex.ConstraintType.ForeignKey)?[0] is Level3.Index x)
                        {
                            cx.Add(new RefAction(x.defpos, ct, tr.nextPos));
                            return;
                        }
                        throw new DBException("42135", new string(lxr.input, st, lxr.pos - st)).Mix();
                    }
                default:
                    if (StartMetadata(Qlx.TABLE) || Match(Qlx.ADD, Qlx.DROP))
                        if (tb.Denied(cx, Grant.Privilege.Metadata))
                            throw new DBException("42105").Add(Qlx.METADATA);
                    cx.Add(new PMetadata(tb.NameFor(cx), 0, tb, ParseMetadata(Qlx.TABLE),
                        tr.nextPos));
                    return;
            }
        }
        /// <summary>
        /// <summary>
        /// AlterColumn = 	TO id
        /// |   POSITION int
        /// |	(SET|DROP) ColumnConstraint 
        /// |	AlterDomain
        /// |	SET GenerationRule.
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tb">the table object</param>
        /// <param name="tc">the table column object</param>
        Table ParseAlterColumn(Table tb, TableColumn tc, string nm)
        {
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            TypedValue o;
            if (Match(Qlx.TO))
            {
                Next();
                var n = new Ident(this);
                Mustbe(Qlx.Id);
                var pc = new Change(tc.defpos, n.ident, tr.nextPos, cx);
                tb = (Table)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.ALTER));
                return tb;
            }
            if (Match(Qlx.POSITION))
            {
                Next();
                o = lxr.val;
                Mustbe(Qlx.INTEGERLITERAL);
                if (o.ToInt() is not int n)
                    throw new DBException("42161", "INTEGER");
                var pa = new Alter3(tc.defpos, nm, n, tb, tc.domain,
                    tc.generated.gfs ?? cx.obs?.ToString() ?? "",
                    tc.domain.defaultValue ?? TNull.Value,
                    "", tc.update, tc.domain.notNull, tc.generated,
                    tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                tb = (Table)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.POSITION));
                return tb;
            }
            if (Match(Qlx.ADD))
            {
                Next();
                while (StartMetadata(Qlx.COLUMN))
                {
                    if (tb.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Add(Qlx.METADATA).Mix();
                    var pm = new PMetadata(nm, 0, tc, ParseMetadata(Qlx.COLUMN), tr.nextPos);
                    tc = (TableColumn)(cx.Add(pm) ?? throw new DBException("42105").Add(Qlx.METADATA));
                }
                if (Match(Qlx.CONSTRAINT))
                    Next();
                var n = new Ident(this);
                Mustbe(Qlx.Id);
                Mustbe(Qlx.CHECK);
                Mustbe(Qlx.LPAREN);
                int st = lxr.pos;
                var nst = cx.db.nextStmt;
                var se = ParseSqlValue(Domain.Bool).Reify(cx);
                string source = new(lxr.input, st, lxr.pos - st - 1);
                Mustbe(Qlx.RPAREN);
                var pc = new PCheck(tc, n.ident, se, source, nst, tr.nextPos, cx);
                tc = (TableColumn)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.CHECK));
            }
            else if (tok == Qlx.SET)
            {
                Next();
                if (Match(Qlx.DEFAULT))
                {
                    Next();
                    int st = lxr.start;
                    var dv = lxr.val;
                    Next();
                    var ds = new string(lxr.input, st, lxr.start - st);
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, tc.domain, ds, dv, "",
                        CTree<UpdateAssignment, bool>.Empty, false,
                        GenerationRule.None, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.DEFAULT));
                }
                else if (Match(Qlx.GENERATED))
                {
                    Domain type = Domain.Row;
                    var oc = cx;
                    cx = cx.ForConstraintParse();
                    var nst = cx.db.nextStmt;
                    var gr = ParseGenerationRule(tc.defpos, tc.domain) + (DBObject._Framing, new Framing(cx, nst));
                    oc.DoneCompileParse(cx);
                    cx = oc;
                    tc.ColumnCheck(tr, true);
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm), tb, tc.domain,
                        gr.gfs ?? type.defaultValue?.ToString() ?? "", type.defaultValue ?? TNull.Value,
                        "", CTree<UpdateAssignment, bool>.Empty, tc.domain.notNull, gr,
                        tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.GENERATED));
                }
                else if (Match(Qlx.NOT))
                {
                    Next();
                    Mustbe(Qlx.NULL);
                    tc.ColumnCheck(tr, false);
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, tc.domain, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                        true, tc.generated, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.NULLS));
                }
                return ParseColumnConstraint(tb, tc);
            }
            else if (tok == Qlx.DROP)
            {
                Next();
                if (StartMetadata(Qlx.COLUMN))
                {
                    if (tb.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Add(Qlx.METADATA,new TChar(tc.NameFor(cx))).Mix();
                    var pm = new PMetadata(nm, 0, tc, ParseMetadata(Qlx.COLUMN), tr.nextPos);
                    tc = (TableColumn)(cx.Add(pm) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                }
                else
                {
                    if (!Match(Qlx.DEFAULT,Qlx.NOT,Qlx.PRIMARY,Qlx.REFERENCES,Qlx.UNIQUE,Qlx.CONSTRAINT)
                        &&!StartMetadata(Qlx.COLUMN))
                        throw new DBException("42000", lxr.Diag).ISO();
                    if (tok == Qlx.DEFAULT)
                    {
                        Next();
                        var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                            tb, tc.domain, "", TNull.Value, tc.updateString ?? "", tc.update, tc.domain.notNull,
                            GenerationRule.None, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                        tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.DEFAULT));
                    }
                    else if (tok == Qlx.NOT)
                    {
                        Next();
                        Mustbe(Qlx.NULL);
                        var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                            tb, tc.domain, tc.domain.defaultString, tc.domain.defaultValue,
                            tc.updateString ?? "", tc.update, false,
                            tc.generated, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                        tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.NULLS));
                    }
                    else if (tok == Qlx.PRIMARY)
                    {
                        Next();
                        Mustbe(Qlx.KEY);
                        Drop.DropAction act = ParseDropAction();
                        if (tb.FindPrimaryIndex(cx) is Level3.Index x)
                        {
                            if (x.keys.Length != 1 || x.keys[0] != tc.defpos)
                                throw new DBException("42158", tb.NameFor(cx), tc.NameFor(cx)).Mix()
                                    .Add(Qlx.TABLE_NAME, new TChar(tb.NameFor(cx)))
                                    .Add(Qlx.COLUMN_NAME, new TChar(tc.NameFor(cx)));
                            var pd = new Drop1(x.defpos, act, tr.nextPos);
                            cx.Add(pd);
                        }
                    }
                    else if (tok == Qlx.REFERENCES)
                    {
                        Next();
                        var n = new Ident(this);
                        Mustbe(Qlx.Id);
                        if (tok == Qlx.LPAREN)
                        {
                            Next();
                            Mustbe(Qlx.Id);
                            Mustbe(Qlx.RPAREN);
                        }
                        Level3.Index? dx = null;
                        for (var p = tb.indexes.First(); dx == null && p != null; p = p.Next())
                            for (var c = p.value().First(); dx == null && c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is Level3.Index x && x.keys.Length == 1 && x.keys[0] == tc.defpos &&
                                    cx.db.objects[x.reftabledefpos] is Table rt && rt.NameFor(cx) == n.ident)
                                    dx = x;
                        if (dx == null)
                            throw new DBException("42159", nm, n.ident).Mix()
                                .Add(Qlx.TABLE_NAME, new TChar(n.ident))
                                .Add(Qlx.COLUMN_NAME, new TChar(nm));
                        var pd = new Drop(dx.defpos, tr.nextPos);
                        cx.Add(pd);
                    }
                    else if (tok == Qlx.UNIQUE)
                    {
                        Next();
                        Level3.Index? dx = null;
                        for (var p = tb.indexes.First(); dx == null && p != null; p = p.Next())
                            for (var c = p.value().First(); dx == null && c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is Level3.Index x && x.keys.Length == 1 &&
                                        x.keys[0] == tc.defpos &&
                                    (x.flags & PIndex.ConstraintType.Unique) == PIndex.ConstraintType.Unique)
                                    dx = x;
                        if (dx == null)
                            throw new DBException("42160", nm).Mix()
                                .Add(Qlx.TABLE_NAME, new TChar(nm));
                        var pd = new Drop(dx.defpos, tr.nextPos);
                        cx.Add(pd);
                    }
                    else if (tok == Qlx.CONSTRAINT)
                    {
                        var n = new Ident(this);
                        Mustbe(Qlx.Id);
                        Drop.DropAction s = ParseDropAction();
                        var ch = cx.GetObject(n.ident) as Check ?? throw new DBException("42135", n.ident);
                        var pd = new Drop1(ch.defpos, s, tr.nextPos);
                        cx.Add(pd);
                    }
                }
            }
            else if (Match(Qlx.TYPE))
            {
                Next();
                Domain? type;
                if (tok == Qlx.Id)
                {
                    var domain = new Ident(this);
                    Next();
                    type = (Domain?)cx.GetObject(domain.ident);
                    if (type == null)
                        throw new DBException("42119", domain.ident, cx.db.name).Mix()
                            .Add(Qlx.CATALOG_NAME, new TChar(cx.db.name))
                            .Add(Qlx.TYPE, new TChar(domain.ident));
                }
                else if (StartDataType())
                {
                    type = ParseDataType() + (Domain.Default, tc.domain.defaultValue)
                        + (Domain.DefaultString, tc.domain.defaultString);
                    type = (Domain)cx.Add(type);
                    if (!tc.domain.CanTakeValueOf(type))
                        throw new DBException("2200G");
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, type,
                        type.defaultString, type.defaultValue, tc.updateString ?? "", tc.update,
                        tc.domain.notNull, tc.generated, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105").Add(Qlx.DOMAIN));
                }
            }
            if (StartMetadata(Qlx.COLUMN))
            {
                if (tb.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Add(Qlx.METADATA).Mix();
                var md = ParseMetadata(Qlx.COLUMN);
                var pm = new PMetadata(nm, 0, tc, md, tr.nextPos);
                cx.Add(pm);
            }
            return tb;
        }
        /// <summary>
		/// AlterType = TO id
        ///     |   ADD ( Member | Method )
		/// 	|	SET Member_id To id
        /// 	|   SET UNDER id
		/// 	|	DROP ( Member_id | (MethodType Method_id '('Type{','Type}')')) DropAction
        /// 	|   DROP UNDER id
		/// 	|	ALTER Member_id AlterMember { ',' AlterMember } .
        /// </summary>
        void ParseAlterType()
        {
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            Next();
            var id = new Ident(this);
            Mustbe(Qlx.Id);
            if (cx.role is not Role ro || (!ro.dbobjects.Contains(id.ident)) ||
            cx._Ob(ro.dbobjects[id.ident] ?? -1L) is not UDType tp || tp.infos[ro.defpos] is not ObInfo oi)
                throw new DBException("42133", id.ident).Mix()
                    .Add(Qlx.TYPE, new TChar(id.ident));
            if (tok == Qlx.TO)
            {
                Next();
                id = new Ident(this);
                Mustbe(Qlx.Id);
                cx.Add(new Change(ro.dbobjects[id.ident] ?? -1L, id.ident, tr.nextPos, cx));
            }
            else if (tok == Qlx.SET)
            {
                Next();
                if (Match(Qlx.UNDER))
                {
                    Next();
                    var ui = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (cx.role.dbobjects.Contains(ui.ident)
                        && cx.db.objects[cx.role.dbobjects[ui.ident] ?? -1L] is UDType tu)
                        cx.Add(new EditType(id.ident, tp, tp, new CTree<Domain,bool>(tu,true), cx.db.nextPos, cx));
                    else
                        throw new DBException("42107", ui.ident);
                }
                else
                {
                    id = new Ident(this);
                    Mustbe(Qlx.Id);
                    var sq = tp.PosFor(cx, id.ident);
                    var ts = tp.ColFor(cx, id.ident);
                    if (cx.db.objects[ts] is not TableColumn tc)
                        throw new DBException("42133", id).Mix()
                            .Add(Qlx.TYPE, new TChar(id.ident));
                    Mustbe(Qlx.TO);
                    id = new Ident(this);
                    Mustbe(Qlx.Id);
                    new Alter3(tc.defpos, id.ident, sq,
                        (Table?)cx.db.objects[tc.tabledefpos] ?? throw new DBException("42105").Add(Qlx.TABLE),
                        tc.domain, tc.domain.defaultString,
                        tc.domain.defaultValue, tc.updateString ?? "",
                        tc.update, tc.domain.notNull, tc.generated, tc.flags, tc.index, tc.toType, tr.nextPos, cx);
                }
            }
            else if (tok == Qlx.DROP)
            {
                Next();
                if (Match(Qlx.CONSTRAINT,Qlx.COLUMN))
                {
                    ParseDropStatement();
                    return;
                }
                if (Match(Qlx.UNDER))
                {
                    Next();
                    var st = tp.super?.First()?.key() as UDType ?? throw new PEException("PE92612");
                    cx.Add(new EditType(id.ident, tp, st, CTree<Domain,bool>.Empty, cx.db.nextPos, cx));
                }
                else
                {
                    id = new Ident(this);
                    if (MethodModes())
                    {
                        MethodName mn = ParseMethod(Domain.Null);
                        if (mn.name is not Ident nm
                            || cx.db.objects[oi?.methodInfos?[nm.ident]?[Context.Signature(mn.ins)] ?? -1L] is not Method mt)
                            throw new DBException("42133", tp).Mix().
                                Add(Qlx.TYPE, new TChar(tp.name));
                        ParseDropAction();
                        new Drop(mt.defpos, tr.nextPos);
                    }
                    else
                    {
                        if (cx.db.objects[tp.ColFor(cx, id.ident)] is not TableColumn tc)
                            throw new DBException("42133", id).Mix()
                                .Add(Qlx.TYPE, new TChar(id.ident));
                        ParseDropAction();
                        new Drop(tc.defpos, tr.nextPos);
                    }
                }
            }
            else if (Match(Qlx.ADD))
            {
                Next();
                if (Match(Qlx.CONSTRAINT))
                {
                    cx.AddDefs(tp);
                    ParseCheckConstraint(tp);
                    return;
                }
                if (Match(Qlx.COLUMN))
                {
                    ParseColumnDefin(tp);
                    return;
                }
                MethodModes();
                if (Match(Qlx.INSTANCE, Qlx.STATIC, Qlx.CONSTRUCTOR, Qlx.OVERRIDING, Qlx.METHOD))
                {
                    ParseMethodHeader(tp);
                    return;
                }
                if ((tp is not NodeType || tp is EdgeType) && StartMetadata(Qlx.TYPE))
                    ParseTypeMetadata(id, tp);
                else
                {
                    var (nm, dm, md) = ParseMember(id);
                    var c = new PColumn2(tp, nm.ident, -1, dm, dm.defaultString, dm.defaultValue,
                            false, GenerationRule.None, tr.nextPos, cx);
                    cx.Add(c);
                    var tc = (TableColumn)(cx.obs[c.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN));
                    if (md != CTree<Qlx, TypedValue>.Empty)
                        cx.Add(new PMetadata(nm.ident, 0, tc, md, tr.nextPos));
                }
            }
            else if (Match(Qlx.ALTER))
            {
                Next();
                id = new Ident(this);
                Mustbe(Qlx.Id);
                if (cx.db.objects[tp.ColFor(cx, id.ident)] is not TableColumn tc)
                    throw new DBException("42133", id).Mix()
                        .Add(Qlx.TYPE, new TChar(id.ident));
                ParseAlterMembers(tc);
            }
        }
        /// <summary>
        /// AlterMember =	TYPE Type
        /// 	|	SET DEFAULT TypedValue
        /// 	|	DROP DEFAULT .
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tc">The UDType member</param>
        TableColumn ParseAlterMembers(TableColumn tc)
        {
            if (tc.infos[cx.role.defpos] is not ObInfo ci)
                throw new DBException("42105").Add(Qlx.COLUMN);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            TypedValue dv = tc.domain.defaultValue;
            var ds = "";
            for (; ; )
            {
                if (tok == Qlx.TO)
                {
                    Next();
                    var n = new Ident(this);
                    Mustbe(Qlx.Id);
                    tc = (TableColumn)(cx.Add(new Change(tc.defpos, n.ident, tr.nextPos, cx))
                        ?? throw new DBException("42105").Add(Qlx.NAME));
                    goto skip;
                }
                else if (Match(Qlx.TYPE))
                {
                    Next();
                    tc = (TableColumn)tc.New(tc.mem + ParseDataType().mem);
                }
                else if (tok == Qlx.SET)
                {
                    Next();
                    if (tok == Qlx.ID) // special cases for the typed graph model
                    {
                        var k = lxr.val.ToString();
                        if (k == "ID")
                        {

                        }
                        else if (k == "LEAVING")
                        { }
                        else if (k == "ARRIVING")
                        { }
                        else throw new DBException("42161", "ID,LEAVING or ARRIVING");
                    }
                    Mustbe(Qlx.DEFAULT);
                    var st = lxr.start;
                    dv = lxr.val;
                    Next();
                    ds = new string(lxr.input, st, lxr.start - st);
                    tc += (Domain.DefaultString, ds);
                    tc += (Domain.Default, dv);
                }
                else if (tok == Qlx.DROP)
                {
                    Next();
                    Mustbe(Qlx.DEFAULT);
                    dv = TNull.Value;
                    tc += (Domain.Default, dv);
                }
                if (cx._Ob(tc.tabledefpos) is Domain td && ci.name is not null)
                    tc = (TableColumn)(cx.Add(new Alter3(tc.defpos, ci.name, td.PosFor(cx, ci.name),
                         (Table?)cx.db.objects[tc.tabledefpos] ?? throw new DBException("42105").Add(Qlx.DOMAIN),
                         tc.domain, ds, dv, tc.updateString ?? "", tc.update,
                         tc.domain.notNull, GenerationRule.None,
                         tc.flags, tc.index, tc.toType, tr.nextPos, cx)) ?? throw new DBException("42105"));
                skip:
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            return tc;
        }
        /// <summary>
        /// FUNCTION id '(' Parameters ')' RETURNS Type Statement
        /// PROCEDURE id '(' Parameters ')' Statement
        /// </summary>
        /// <returns>the executable</returns>
		void ParseAlterProcedure()
        {
            bool func = Match(Qlx.FUNCTION);
            Next();
            ParseProcedureClause(func, Qlx.ALTER);
        }
        /// <summary>
		/// DropStatement = 	DROP DropObject DropAction .
		/// DropObject = 	ORDERING FOR id
		/// 	|	ROLE id
		/// 	|	TRIGGER id
		/// 	|	ObjectName .
        /// </summary>
        /// <returns>the executable</returns>
		void ParseDropStatement()
        {
            if (cx.role == null || cx.role.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole) == false)
                throw new DBException("42105").Add(Qlx.ADMIN);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            Next();
            if (Match(Qlx.ORDERING))
            {
                Next(); Mustbe(Qlx.FOR);
                var o = new Ident(this);
                Mustbe(Qlx.Id);
                ParseDropAction(); // ignore if present
                var tp = cx.db.objects[cx.role.dbobjects[o.ident] ?? -1L] as Domain ??
                    throw new DBException("42133", o.ToString()).Mix();
                cx.Add(new Ordering(tp, -1L, OrderCategory.None, tr.nextPos, cx));
            }
            else
            {
                var (ob, _) = ParseObjectName();
                var a = ParseDropAction();
                ob.Cascade(cx, a);
            }
        }
        bool StartStdFunctionRefs()
        {
            return Match(Qlx.COLLECT, Qlx.CURRENT, Qlx.DESCRIBE, Qlx.ELEMENT, Qlx.ELEMENTID, Qlx.EVERY,
    Qlx.EXTRACT, Qlx.FIRST_VALUE, Qlx.FUSION, Qlx.GROUPING, Qlx.HTTP, Qlx.ID,
    Qlx.LABELS, Qlx.LAST_VALUE, Qlx.LAST_DATA, Qlx.PARTITION,
    Qlx.CHAR_LENGTH, Qlx.WITHIN, Qlx.POSITION, Qlx.ROW_NUMBER, Qlx.SOME,
    Qlx.SPECIFICTYPE, Qlx.SUBSTRING, Qlx.COLLECT, Qlx.INTERSECTION, Qlx.ROWS,

#if OLAP
                Sqlx.CORR, Sqlx.COVAR_POP, Sqlx.COVAR_SAMP, Sqlx.CUME_DIST, Sqlx.DENSE_RANK,
                Sqlx.PERCENT_RANK, Sqlx.PERCENTILE_CONT, Sqlx.PERCENTILE_DISC,
                Sqlx.POSITION_REGEX, Sqlx.RANK, Sqlx.REGR_COUNT, Sqlx.REGR_AVGX, Sqlx.REGR_AVGY,
                Sqlx.REGR_INTERCEPT, Sqlx.REGR_R2, Sqlx.REGR_SLOPE, Sqlx.REGR_SXX, Sqlx.REGR_SXY,
                Sqlx.REGR_SYY, Sqlx.VAR_POP, Sqlx.VAR_SAMP,
#endif
#if SIMILAR
                Sqlx.OCCURRENCES_REGEX, Sqlx.SUBSTRING_REGEX, Sqlx.TRANSLATE_REGEX,
#endif
    Qlx.VERSIONING);
        }
        bool StartDataType()
        {
            return Match(Qlx.CHARACTER, Qlx.CHAR, Qlx.VARCHAR, Qlx.NATIONAL, Qlx.NCHAR, Qlx.STRING,
                Qlx.BOOLEAN, Qlx.NUMERIC, Qlx.DECIMAL,
                Qlx.DEC, Qlx.FLOAT, Qlx.REAL, Qlx.DOUBLE,
                Qlx.INT, Qlx.INTEGER, Qlx.BIGINT, Qlx.SMALLINT, Qlx.PASSWORD,
                Qlx.FLOAT16, Qlx.FLOAT32, Qlx.FLOAT64, Qlx.FLOAT128, Qlx.FLOAT256,
                Qlx.DOUBLE, Qlx.INT, Qlx.BIGINT, Qlx.INTEGER, Qlx.SMALLINT, //Qlx.LONG,
                Qlx.INT8, Qlx.INT16, Qlx.INT32, Qlx.INT64, Qlx.INTEGER128, Qlx.INT256,
                Qlx.SIGNED, Qlx.UNSIGNED, Qlx.BINARY, Qlx.BLOB, Qlx.NCLOB,
                Qlx.UINT, Qlx.UINT8, Qlx.UINT16, Qlx.UINT32, Qlx.UINT64, Qlx.UINT128, Qlx.UINT256,
                Qlx.BINARY, Qlx.BLOB, Qlx.NCLOB, Qlx.CLOB,
                Qlx.DATE, Qlx.TIME, Qlx.TIMESTAMP, Qlx.INTERVAL,
                Qlx.DOCUMENT, Qlx.DOCARRAY, Qlx.CHECK,
                Qlx.ROW, Qlx.TABLE, Qlx.ROW, Qlx.ARRAY, Qlx.SET, Qlx.MULTISET, Qlx.LIST,
                Qlx.REF);
        }
        /// <summary>
		/// Type = 		StandardType | DefinedType | DomainName | REF(TableReference) .
		/// DefinedType = 	ROW  Representation
		/// 	|	TABLE Representation
        /// 	|   ( Type {, Type }) 
        ///     |   Type UNION Type { UNION Type }.
        /// </summary>
        /// <param name="pn">Parent Id (Type, or Procedure)</param>
		Domain ParseDataType(Ident? pn = null)
        {
            StartDataType();
            Domain? r = null;
            Qlx tp = tok;
            if (Match(Qlx.TABLE, Qlx.ROW, Qlx.TYPE, Qlx.LPAREN))// anonymous row type
            {
                if (Match(Qlx.TABLE, Qlx.ROW, Qlx.TYPE))
                    Next();
                else
                    tp = Qlx.TYPE;
                if (tok == Qlx.LPAREN)
                    r = ParseRowTypeSpec(tp, pn, CTree<Domain,bool>.Empty); // pn is needed for tp==TYPE case
            }
            else
            {
                var cn = new Ident(this);
                if (tok == Qlx.Id && cx.GetObject(cn.ident) is UDType ut)
                {
                    Next();
                    ut.Defs(cx);
                    r = ut;
                }
                else
                    r = ParseStandardDataType();
            }
            if (r != null && Match(Qlx.ARRAY, Qlx.SET, Qlx.MULTISET))
            {
                r = new Domain(-1L, tok, r);
                Next();
            }
            if (r == null || r == Domain.Null || r == Domain.Content)
            {
                var o = new Ident(this);
                Next();
                r = (Domain)(cx.db.objects[cx.role.dbobjects[o.ident] ?? -1L] ?? Domain.Content);
            }
            if (Match(Qlx.SENSITIVE))
            {
                r = (Domain)cx.Add(new Domain(cx.GetUid(), tok, r));
                Next();
            }
            return r;
        }
        /// <summary>
        /// StandardType = 	BooleanType | CharacterType | FloatType | IntegerType | LobType | NumericType | DateTimeType | IntervalType | XMLType .
        /// BooleanType = 	BOOLEAN .
        /// CharacterType = (([NATIONAL] CHARACTER) | CHAR | NCHAR | VARCHAR) [VARYING] ['('int ')'] [CHARACTER SET id ] Collate .
        /// Collate 	=	[ COLLATE id ] .
        /// There is no need to specify COLLATE UNICODE, since this is the default collation. COLLATE UCS_BASIC is supported but deprecated. Show the tree of available collations, see .NET documentation.
        /// FloatType =	(FLOAT|REAL) ['('int','int')'] .
        /// IntegerType = 	INT | INTEGER .
        /// LobType = 	BLOB | CLOB | NCLOB .
        /// CLOB is a synonym for CHAR in Pyrrho (both represent unbounded string). Similarly NCLOB is a synonym for NCHAR.
        /// NumericType = 	(NUMERIC|DECIMAL|DEC) ['('int','int')'] .
        /// DateTimeType =  (DATE | TIME | TIMESTAMP) ([IntervalField [ TO IntervalField ]] | ['(' int ')']).
        /// The use of IntervalFields when declaring DateTimeType  is an addition to the SQL standard.
        /// XMLType =	XML .
        /// </summary>
        /// <returns>the obs type</returns>
        Domain ParseStandardDataType()
        {
            Domain r = Domain.Null;
            Domain r0 = Domain.Null;
            var sg = Match(Qlx.SIGNED);
            var us = Match(Qlx.UNSIGNED);
            if (sg || us)
                Next();
            if (Match(Qlx.CHARACTER, Qlx.CHAR, Qlx.VARCHAR, Qlx.STRING, Qlx.CLOB, Qlx.NCLOB))
            {
                r = r0 = Domain.Char;
                Next();
                if (Match(Qlx.LARGE))
                {
                    Next();
                    Mustbe(Qlx.OBJECT); // CLOB is CHAR in Pyrrho
                }
                else if (Match(Qlx.VARYING))
                    Next();
                r = ParsePrecPart(r);
                if (Match(Qlx.CHARACTER))
                {
                    Next();
                    Mustbe(Qlx.SET);
                    var o = new Ident(this);
                    Mustbe(Qlx.Id);
                    r += (Domain.Charset, (Common.CharSet)Enum.Parse(typeof(Common.CharSet), o.ident, false));
                }
                if (Match(Qlx.COLLATE))
                    r += (Domain.Culture, new CultureInfo(ParseCollate()));
            }
            else if (Match(Qlx.NATIONAL, Qlx.NCHAR))
            {
                if (Match(Qlx.NATIONAL))
                {
                    Next();
                    Mustbe(Qlx.CHARACTER);
                }
                else
                    Next();
                r = r0 = Domain.Char;
                if (Match(Qlx.LARGE))
                {
                    Next();
                    Mustbe(Qlx.OBJECT); // NCLOB is NCHAR in Pyrrho
                }
                r = ParsePrecPart(r);
            }
            else if (Match(Qlx.NUMERIC, Qlx.DECIMAL, Qlx.DEC))
            {
                r = r0 = Domain._Numeric;
                Next();
                r = ParsePrecScale(r);
            }
            else if (Match(Qlx.FLOAT, Qlx.FLOAT16, Qlx.FLOAT32, Qlx.FLOAT64, Qlx.FLOAT128,
                Qlx.FLOAT256, Qlx.REAL, Qlx.DOUBLE))
            {
                r = r0 = Domain.Real;
                if (tok == Qlx.DOUBLE)
                    Mustbe(Qlx.PRECISION);
                Next();
                r = ParsePrecPart(r);
            }
            else if (Match(Qlx.INT, Qlx.INT8, Qlx.INT16, Qlx.INT32, Qlx.INT64, Qlx.INT128, Qlx.INT256,
                Qlx.INTEGER,Qlx.INTEGER8, Qlx.INTEGER16, Qlx.INTEGER32, Qlx.INTEGER64, Qlx.INTEGER128, 
                Qlx.INTEGER256, Qlx.BIGINT, Qlx.SMALLINT))
            {
                r = r0 = Domain.Int;
                Next();
                r = ParsePrecPart(r);
            }
            else if (Match(Qlx.BINARY))
            {
                Next();
                Mustbe(Qlx.LARGE);
                Mustbe(Qlx.OBJECT);
                r = r0 = Domain.Blob;
            }
            else if (Match(Qlx.BOOLEAN))
            {
                r = r0 = Domain.Bool;
                Next();
            }
            else if (Match(Qlx.CLOB, Qlx.NCLOB))
            {
                r = r0 = Domain.Char;
                Next();
            }
            else if (Match(Qlx.BLOB))
            {
                r = r0 = Domain.Blob;
                Next();
            }
            else if (Match(Qlx.DATE, Qlx.TIME, Qlx.TIMESTAMP, Qlx.INTERVAL))
            {
                Domain dr = r0 = Domain.Timestamp;
                switch (tok)
                {
                    case Qlx.DATE: dr = Domain.Date; break;
                    case Qlx.TIME: dr = Domain.Timespan; break;
                    case Qlx.TIMESTAMP: dr = Domain.Timestamp; break;
                    case Qlx.INTERVAL: dr = Domain.Interval; break;
                }
                Next();
                if (Match(Qlx.YEAR, Qlx.DAY, Qlx.MONTH, Qlx.HOUR, Qlx.MINUTE, Qlx.SECOND))
                    dr = ParseIntervalType();
                r = dr;
            }
            else if (Match(Qlx.PASSWORD))
            {
                r = r0 = Domain.Password;
                Next();
            }
            else if (Match(Qlx.POSITION))
            {
                r = r0 = Domain.Position;
                Next();
            }
            else if (Match(Qlx.DOCUMENT))
            {
                r = r0 = Domain.Document;
                Next();
            }
            else if (Match(Qlx.DOCARRAY))
            {
                r = r0 = Domain.DocArray;
                Next();
            }
            else if (Match(Qlx.CHECK))
            {
                r = r0 = Domain._Rvv;
                Next();
            }
            else if (Match(Qlx.OBJECT))
            {
                r = r0 = Domain.ObjectId;
                Next();
            }
            if (r == Domain.Null)
                return Domain.Null; // not a standard type
            if (r == r0)
                return r0; // completely standard
            // see if we know this type
            if (cx.db.objects[cx.db.Find(r)?.defpos ?? -1L] is Domain nr
                && r.CompareTo(nr) == 0)
                return (Domain)cx.Add(nr);
            if (cx.newTypes.Contains(r) && cx.obs[cx.newTypes[r] ?? -1L] is Domain ns)
                return (Domain)cx.Add(ns);
            var pp = new PDomain(r, cx.db.nextPos, cx);
            cx.Add(pp);
            return (Domain)cx.Add(pp.domain);
        }
        /// <summary>
		/// IntervalType = 	INTERVAL IntervalField [ TO IntervalField ] .
		/// IntervalField = 	YEAR | MONTH | DAY | HOUR | MINUTE | SECOND ['(' int ')'] .
        /// </summary>
        /// <param name="q">The Domain being specified</param>
        /// <returns>the modified obs type</returns>
        Domain ParseIntervalType()
        {
            Qlx start = Mustbe(Qlx.YEAR, Qlx.DAY, Qlx.MONTH, Qlx.HOUR, Qlx.MINUTE, Qlx.SECOND);
            var d = Domain.Interval;
            var m = d.mem + (Domain.Start, start);
            if (tok == Qlx.LPAREN)
            {
                Next();
                var p1 = lxr.val;
                Mustbe(Qlx.INTEGERLITERAL);
                m += (Domain.Scale, p1.ToInt() ?? 0);
                if (start == Qlx.SECOND && tok == Qlx.COMMA)
                {
                    Next();
                    var p2 = lxr.val;
                    Mustbe(Qlx.INTEGERLITERAL);
                    m += (Domain.Precision, p2.ToInt() ?? 0);
                }
                Mustbe(Qlx.RPAREN);
            }
            if (Match(Qlx.TO))
            {
                Next();
                Qlx end = Mustbe(Qlx.YEAR, Qlx.DAY, Qlx.MONTH, Qlx.HOUR, Qlx.MINUTE, Qlx.SECOND);
                m += (Domain.End, end);
                if (end == Qlx.SECOND && tok == Qlx.LPAREN)
                {
                    Next();
                    var p2 = lxr.val;
                    Mustbe(Qlx.INTEGERLITERAL);
                    m += (Domain.Precision, p2.ToInt() ?? 0);
                    Mustbe(Qlx.RPAREN);
                }
            }
            return (Domain)d.New(m);
        }
        /// <summary>
        /// Handle ROW type or TABLE type in Type specification.
        /// </summary>
        /// <returns>The RowTypeSpec</returns>
        internal Domain ParseRowTypeSpec(Qlx k, Ident? pn = null, CTree<Domain,bool>? under=null)
        {
            under ??= CTree<Domain,bool>.Empty;
            var dt = Domain.Null;
            if (tok == Qlx.Id)
            {
                var id = new Ident(this);
                Next();
                if (cx.GetObject(id.ident) is not Domain ob)
                    throw new DBException("42107", id.ident).Mix();
                return ob;
            }
            var lp = LexPos();
            var ns = BList<(Ident, Domain, CTree<Qlx, TypedValue>)>.Empty;
            // sm is also used for the RestView case
            var sm = BTree<string, (int, long?)>.Empty;
            for (var b = under.First(); b != null; b = b.Next())
            {
                sm += (b.key() as UDType)?.HierarchyCols(cx) ?? BTree<string, (int, long?)>.Empty;
                if (b.key() is EdgeType)
                    k = Qlx.EDGETYPE;
                if (b.key() is NodeType && k!=Qlx.EDGETYPE)
                    k = Qlx.NODETYPE;
            }
            var sl = lxr.start;
            Mustbe(Qlx.LPAREN);
            for (var n = 0; ; n++)
            {
                var mi = ParseMember(pn);
                if (sm.Contains(mi.Item1.ident))
                    throw new DBException("42104", mi.Item1.ident);
                ns += mi;
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            Mustbe(Qlx.RPAREN);
            var ic = new Ident(new string(lxr.input, sl, lxr.start - sl), lp);
            var m = new BTree<long, object>(DBObject.Definer, cx.role.defpos);
            var oi = new ObInfo(ic.ident, Grant.AllPrivileges);
            m += (DBObject.Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            var st = cx.db.nextPos;
            var nst = cx.db.nextStmt;
            string tn = (pn is not null) ? pn.ident : ic.ident;
            if (k == Qlx.VIEW)
                dt = (Domain)cx.Add(new Domain(st, cx, Qlx.VIEW, BList<DBObject>.Empty));
            else if (k == Qlx.TABLE)
                dt = (Domain)cx.Add(new Table(lp.dp, m));
            else if (pn is not null)
            {
                dt = k switch
                {
                    Qlx.NODETYPE => Domain.NodeType,
                    Qlx.EDGETYPE => Domain.EdgeType,
                    _ => Domain.TypeSpec,
                };
                dt = ((UDType)dt).New(pn, under, st, cx) ?? throw new PEException("PE40407");
            }
            if (k != Qlx.ROW)
                pn ??= new Ident("", lp);
            var ms = CTree<long, Domain>.Empty;
            var rt = BList<long?>.Empty;
            var ls = CTree<long, string>.Empty;
            var j = 0;
            for (var b = ns.First(); b != null; b = b.Next(), j++)
            {
                var (nm, dm, _) = b.value();
                if ((k == Qlx.TYPE || k == Qlx.NODETYPE || k == Qlx.EDGETYPE) && pn != null)
                {
                    var np = cx.db.nextPos;
                    var pc = new PColumn3((Table)dt, nm.ident, -1, dm,
                        "", dm.defaultValue, "", CTree<UpdateAssignment, bool>.Empty,
                        false, GenerationRule.None, PColumn.GraphFlags.None, -1L, -1L, np, cx);
                    cx.Add(pc);
                    ms += (pc.defpos, dm);
                    sm += (nm.ident, (j, pc.defpos));
                    rt += pc.defpos;
                    var cix = cx.Ix(pc.defpos);
                    cx.defs += (new Ident(pn, nm), cix);
                }
                else if (pn != null && pn.ident != "")
                {
                    var se = new SqlElement(nm, BList<Ident>.Empty, cx, pn, dm);
                    cx.Add(se);
                    ms += (se.defpos, dm);
                    sm += (nm.ident, (j, se.defpos));
                    rt += se.defpos;
                    cx.defs += (new Ident(pn, nm), pn.iix);
                }
                else // RestView
                {
                    var sp = nm.iix.dp;
                    rt += sp;
                    ms += (sp, dm);
                    sm += (nm.ident, (b.key(), sp));
                    ls += (sp, nm.ident);
                }
            }
            dt = cx.obs[dt.defpos] as Domain ?? throw new DBException("42105").Add(Qlx.DOMAIN);
            oi += (ObInfo.Names, sm);
            var r = (Domain)dt.New(st, BTree<long, object>.Empty
                + (ObInfo.Name, tn) + (DBObject.Definer, cx.role.defpos)
                + (DBObject.Infos, new BTree<long, ObInfo>(cx.role.defpos, oi))
                + (Domain.Representation, ms) + (Domain.RowType, rt));
            for (var a = under?.First(); a != null; a = a.Next())
                if (cx.db.objects[a.key().defpos] is Table tu)
                {
                    for (var b = tu.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.db.objects[c.key()] is Level3.Index x && dt is Table t)
                                r = (Domain)(cx.Add(new PIndex(t.name + "." + x.name, t, b.key(), x.flags,
                                    x.refindexdefpos, cx.db.nextPos)) ?? dt);
                    r += (Domain.Constraints, tu.constraints);
                }
            if (pn == null || pn.ident == "") // RestView
                r = r + (ObInfo.Names, sm) + (RestView.NamesMap, ls);
            cx.Add(r);
            if (dt is Table)
            {
                r += (DBObject._Framing, new Framing(cx, -1L));
                cx.Add(r);
                cx.Add(r.framing);
            }
            else
                cx.Add(new Framing(cx, nst));
            return r;
        }
        /// <summary>
        /// Member = id Type [DEFAULT TypedValue] Collate .
        /// </summary>
        /// <param name="pn">The parent object being defined (except for anonymous row type)</param>
        /// <returns>The RowTypeColumn</returns>
		(Ident, Domain, CTree<Qlx, TypedValue>) ParseMember(Ident? pn)
        {
            Ident? n = null;
            StartDataType();
            if (tok == Qlx.Id)
            {
                n = new Ident(this);
                Next();
            }
            var dm = ParseDataType(pn); // dm is domain of the member
            if (tok == Qlx.Id && n == null)
                throw new DBException("42000", dm);
            if (Match(Qlx.DEFAULT))
            {
                int st = lxr.start;
                var dv = ParseSqlValue(dm);
                var ds = new string(lxr.input, st, lxr.start - st);
                dm = dm + (Domain.Default, dv) + (Domain.DefaultString, ds);
            }
            if (Match(Qlx.COLLATE))
                dm += (Domain.Culture, ParseCollate());
            var md = CTree<Qlx, TypedValue>.Empty;
            if (StartMetadata(Qlx.COLUMN))
                md = ParseMetadata(Qlx.COLUMN);
            if (n == null || dm == null || md == null)
                throw new DBException("42000", "Member");
            return (n, dm, md);
        }
        /// <summary>
        /// Parse a precision
        /// </summary>
        /// <param name="r">The SqldataType</param>
        /// <returns>the updated obs type</returns>
		Domain ParsePrecPart(Domain r)
        {
            if (tok == Qlx.LPAREN)
            {
                Next();
                if (lxr.val is TInt it)
                {
                    int prec = (int)it.value;
                    r += (Domain.Precision, prec);
                }
                Mustbe(Qlx.INTEGERLITERAL);
                Mustbe(Qlx.RPAREN);
            }
            return r;
        }
        /// <summary>
        /// Parse a precision and scale
        /// </summary>
        /// <param name="r">The sqldatatype</param>
        /// <returns>the updated obs type</returns>
		Domain ParsePrecScale(Domain r)
        {
            if (tok == Qlx.LPAREN)
            {
                Next();
                if (lxr.val is TInt it && it is TInt i)
                {
                    int prec = (int)i.value;
                    r += (Domain.Precision, prec);
                }
                Mustbe(Qlx.INTEGERLITERAL);
                if (tok == Qlx.COMMA)
                {
                    Next();
                    if (lxr.val is TInt jt && jt is TInt j)
                    {
                        int scale = (int)j.value;
                        r += (Domain.Scale, scale);
                    }
                    Mustbe(Qlx.INTEGERLITERAL);
                }
                Mustbe(Qlx.RPAREN);
            }
            return r;
        }
        /// <summary>
        /// Rename =SET ObjectName TO id .
        /// </summary>
        /// <returns>the executable</returns>
		void ParseSqlSet()
        {
            if (Match(Qlx.AUTHORIZATION))
            {
                Next();
                Mustbe(Qlx.EQL);
                Mustbe(Qlx.CURATED);
                if (cx.db is not Transaction) throw new DBException("2F003");
                if (cx.db.user == null || cx.db.user.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER).Mix();
                if (cx.parse == ExecuteStatus.Obey)
                {
                    var pc = new Curated(cx.db.nextPos);
                    cx.Add(pc);
                }
            }
            else if (Match(Qlx.TIMEOUT))
            {
                Next();
                Mustbe(Qlx.EQL);
                if (cx.db.user == null || cx.db.user.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER).Mix();
                Mustbe(Qlx.INTEGERLITERAL);
                // ignore for now
            }
            else
            {
                // Rename
                Ident? n;
                Match(Qlx.DOMAIN, Qlx.ROLE, Qlx.VIEW, Qlx.TYPE);
                MethodModes();
                DBObject? ob;
                if (Match(Qlx.TABLE, Qlx.DOMAIN, Qlx.ROLE, Qlx.VIEW, Qlx.TYPE))
                {
                    Next();
                    n = new Ident(this);
                    Mustbe(Qlx.Id);
                    ob = cx._Ob(cx.role.dbobjects[n.ident] ?? -1L)
                        ?? cx._Ob(cx.db.roles[n.ident] ?? -1L)
                        ?? throw new DBException("42107", n.ident);
                    var oi = ob.infos[cx.db.role.defpos] ?? ob.infos[ob.definer]
                        ?? throw new DBException("42105").Add(Qlx.ROLE);
                    ob += (DBObject.Infos, oi + (ObInfo.Name, n.ident));
                    cx.Add(ob);
                }
                else
                {
                    bool meth = false;
                    PMethod.MethodType mt = PMethod.MethodType.Instance;
                    if (Match(Qlx.OVERRIDING, Qlx.STATIC, Qlx.INSTANCE, Qlx.CONSTRUCTOR))
                    {
                        switch (tok)
                        {
                            case Qlx.OVERRIDING: mt = PMethod.MethodType.Overriding; break;
                            case Qlx.STATIC: mt = PMethod.MethodType.Static; break;
                            case Qlx.CONSTRUCTOR: mt = PMethod.MethodType.Constructor; break;
                        }
                        Next();
                        Mustbe(Qlx.METHOD);
                        meth = true;
                    }
                    else if (Match(Qlx.METHOD))
                        meth = true;
                    else if (!Match(Qlx.PROCEDURE, Qlx.FUNCTION))
                        throw new DBException("42126").Mix();
                    Next();
                    n = new Ident(this);
                    var nid = n.ident;
                    Mustbe(Qlx.Id);
                    var a = CList<Domain>.Empty;
                    if (tok == Qlx.LPAREN)
                    {
                        Next();
                        a += ParseDataType();
                        while (tok == Qlx.COMMA)
                        {
                            Next();
                            a += ParseDataType();
                        }
                        Mustbe(Qlx.RPAREN);
                    }
                    if (meth)
                    {
                        Ident? type = null;
                        if (mt == PMethod.MethodType.Constructor)
                            type = new Ident(nid, cx.Ix(0));
                        if (tok == Qlx.FOR)
                        {
                            Next();
                            type = new Ident(this);
                            Mustbe(Qlx.Id);
                        }
                        if (type == null)
                            throw new DBException("42134").Mix();
                        if (cx.role is not Role ro ||
                            cx.GetObject(type.ident) is not DBObject ot ||
                            ot.infos[ro.defpos] is not ObInfo oi)
                            throw new DBException("42105").Add(Qlx.OBJECT);
                        ob = (Method?)cx.db.objects[oi.methodInfos[n.ident]?[a] ?? -1L];
                    }
                    else
                        ob = cx.GetProcedure(LexPos().dp, n.ident, a);
                    if (ob == null)
                        throw new DBException("42135", n.ident).Mix();
                    Mustbe(Qlx.TO);
                    var nm = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    {
                        var pc = new Change(ob.defpos, nm.ident, tr.nextPos, cx);
                        cx.Add(pc);
                    }
                }
            }
        }
        /// <summary>
		/// CursorSpecification = [ XMLOption ] QueryExpression  .
        /// </summary>
        /// <param name="xp">The result expected (default Domain.Content)</param>
        /// <returns>A CursorSpecification</returns>
		internal SelectStatement ParseCursorSpecification(Domain xp, bool ambient = false)
        {
            RowSet un = _ParseCursorSpecification(xp, ambient);
            var s = new SelectStatement(cx.GetUid(), un);
            cx.exec = s;
            return (SelectStatement)cx.Add(s);
        }
        internal RowSet _ParseCursorSpecification(Domain xp, bool ambient = false)
        {
            var inced = false;
            if (!ambient) // can't test for prevtok==SELECT here!
            {
                inced = true;
                cx.IncSD(new Ident(this));
            }
            RowSet qe;
            qe = ParseRowSetSpec(xp, ambient);
            cx.result = qe.defpos;
            cx.Add(qe);
            if (inced)
                cx.DecSD();
            return qe;
        }
        /// <summary>
        /// Start the parse for a QueryExpression (called from View)
        /// </summary>
        /// <param name="sql">The ql string</param>
        /// <param name="xp">The expected result type</param>
        /// <returns>a RowSet</returns>
		public RowSet ParseQueryExpression(Ident sql, Domain xp)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return ParseRowSetSpec(xp);
        }
        /// <summary>
        /// RowSetSpec = RowSetSpecBody [OrderByClause] [FetchFirstClause] .
		/// RowSetSpecBody = RowSetTerm 
		/// | RowSetSpecBody ( UNION | EXCEPT ) [ ALL | DISTINCT ] QueryTerm .
		/// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification 
        /// in which the RowSetExpr is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="xp">the expected result type</param>
        /// <returns>Updated result type, and a RowSet</returns>
		RowSet ParseRowSetSpec(Domain xp, bool ambient = false)
        {
            RowSet left, right;
            left = ParseRowSetTerm(xp, ambient);
            while (Match(Qlx.UNION, Qlx.EXCEPT))
            {
                Qlx op = tok;
                Next();
                Qlx md = Qlx.DISTINCT;
                if (Match(Qlx.ALL, Qlx.DISTINCT))
                {
                    md = tok;
                    Next();
                }
                right = ParseRowSetTerm(xp, ambient);
                left = new MergeRowSet(cx.GetUid(), cx, xp, left, right, md == Qlx.DISTINCT, op);
                if (md == Qlx.DISTINCT)
                    left += (RowSet.Distinct, true);
            }
            var ois = left.ordSpec;
            var nis = ParseOrderClause(left, ois, true);
            left = (RowSet)(cx.obs[left.defpos] ?? throw new PEException("PE20701"));
            if (ois.CompareTo(nis) != 0)
                left = left.Sort(cx, nis, false);
            var n = FetchFirstClause();
            if (n > 0)
                left = new RowSetSection(cx, left, 0, n);
            return (RowSet)cx.Add(left);
        }
        internal int FetchFirstClause()
        {
            int n = -1;
            if (Match(Qlx.FETCH))
            {
                n = 1;
                Next();
                Mustbe(Qlx.FIRST);
                var o = lxr.val;
                if (tok == Qlx.INTEGERLITERAL)
                {
                    n = o.ToInt() ?? 1;
                    Next();
                    Mustbe(Qlx.ROWS);
                }
                else
                    Mustbe(Qlx.ROW);
                Mustbe(Qlx.ONLY);
            }
            return n;
        }
        /// <summary>
		/// QueryTerm = QueryPrimary | QueryTerm INTERSECT [ ALL | DISTINCT ] QueryPrimary .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification 
        /// in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the RowSet</returns>
		RowSet ParseRowSetTerm(Domain xp, bool ambient = false)
        {
            RowSet left, right;
            left = ParseQueryPrimary(xp, ambient);
            while (Match(Qlx.INTERSECT))
            {
                var lp = LexPos();
                Next();
                Qlx m = Qlx.DISTINCT;
                if (Match(Qlx.ALL, Qlx.DISTINCT))
                {
                    m = tok;
                    Next();
                }
                right = ParseQueryPrimary(xp, ambient);
                left = new MergeRowSet(lp.dp, cx, xp, left, right, m == Qlx.DISTINCT, Qlx.INTERSECT);
                if (m == Qlx.DISTINCT)
                    left += (RowSet.Distinct, true);
            }
            return (RowSet)cx.Add(left);
        }
        /// <summary>
		/// QueryPrimary = QuerySpecification |  TypedValue | TABLE id .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification in which the 
        /// QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the updated result type and the RowSet</returns>
		RowSet ParseQueryPrimary(Domain xp, bool ambient = false)
        {
            var lp = LexPos();
            RowSet qs;
            switch (tok)
            {
                case Qlx.LPAREN:
                    Next();
                    qs = ParseRowSetSpec(xp, ambient);
                    Mustbe(Qlx.RPAREN);
                    break;
                case Qlx.RETURN:
                case Qlx.SELECT: // query specification
                    qs = ParseQuerySpecification(xp, ambient);
                    break;
                case Qlx.WITH:
                case Qlx.MATCH:
                    ParseMatchStatement();
                    qs = (RowSet)(cx.obs[cx.result] ?? new TrivialRowSet(cx, Domain.Row));
                    break;
                case Qlx.VALUES:
                    var v = BList<long?>.Empty;
                    Qlx sep = Qlx.COMMA;
                    while (sep == Qlx.COMMA)
                    {
                        Next();
                        var llp = LexPos();
                        Mustbe(Qlx.LPAREN);
                        var x = ParseSqlValueList(xp);
                        Mustbe(Qlx.RPAREN);
                        v += cx.Add(new SqlRow(llp.dp, cx, xp, x)).defpos;
                        sep = tok;
                    }
                    qs = (RowSet)cx.Add(new SqlRowSet(lp.dp, cx, xp, v));
                    break;
                case Qlx.TABLE:
                    Next();
                    Ident ic = new(this);
                    Mustbe(Qlx.Id);
                    var tb = cx.GetObject(ic.ident) as Table;
                    if (tb is null && long.TryParse(ic.ident, out var tp))
                        tb = cx.db.objects[tp] as Table;
                    if (tb is null)
                        throw new DBException("42107", ic.ident);
                    qs = tb.RowSets(ic, cx, tb, ic.iix.dp, Grant.Privilege.Select);
                    break;
                default:
                    throw new DBException("42127").Mix();
            }
            return (RowSet)cx.Add(qs);
        }
        /// <summary>
		/// OrderByClause = ORDER BY BList<long?> { ',' BList<long?> } .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the tree of OrderItems</returns>
		Domain ParseOrderClause(RowSet rs, Domain ord, bool wfok)
        {
            if (tok != Qlx.ORDER)
                return ord;
            cx.IncSD(new Ident(this)); // order by columns will be in the foregoing cursor spec
            Next();
            Mustbe(Qlx.BY);
            var bs = BList<DBObject>.Empty;
            for (var b = ord.rowType.First(); b != null; b = b.Next())
                bs += cx._Ob(b.value() ?? -1L) ?? SqlNull.Value;
            var oi = (QlValue?)cx._Ob(ParseOrderItem(wfok)) ?? SqlNull.Value;
            bs += Simplify(cx,oi,rs);
            while (tok == Qlx.COMMA)
            {
                Next();
                oi = (QlValue?)cx._Ob(ParseOrderItem(wfok)) ?? SqlNull.Value;
                bs += Simplify(cx,oi,rs);
            }
            cx.DecSD();
            return new Domain(cx.GetUid(), cx, Qlx.ROW, bs, bs.Length);
        }
        static QlValue Simplify(Context cx,QlValue oi,RowSet rs)
        {
            if (oi is SqlTreatExpr te)
            {
                var v = (QlValue?)cx._Ob(te.val)??SqlNull.Value;
                var w = Simplify(cx, v, rs);
                if (v != w)
                    return (QlValue)cx.Add(new SqlTreatExpr(te.defpos, w, te.domain));
            }
            for (var b = rs.rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value() ?? -1L) is QlValue e && oi._MatchExpr(cx, e, rs))
                    oi = e;
            return oi;
        }
        /// <summary>
        /// This version is for WindowSpecifications
        /// </summary>
        /// <param name="ord"></param>
        /// <returns></returns>
        Domain ParseOrderClause(Domain ord)
        {
            if (tok != Qlx.ORDER)
                return ord;
            Next();
            Mustbe(Qlx.BY);
            var bs = BList<DBObject>.Empty;
            for (var b = ord.rowType.First(); b != null; b = b.Next())
                bs += cx._Ob(b.value() ?? -1L) ?? SqlNull.Value;
            bs += cx._Ob(ParseOrderItem(false)) ?? SqlNull.Value;
            while (tok == Qlx.COMMA)
            {
                Next();
                bs += cx._Ob(ParseOrderItem(false)) ?? SqlNull.Value;
            }
            return new Domain(cx.GetUid(), cx, Qlx.ROW, bs, bs.Length) - Domain.Aggs;
        }
        /// <summary>
		/// BList<long?> =  TypedValue [ ASC | DESC ] [ NULLS ( FIRST | LAST )] .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>an OrderItem</returns>
		long ParseOrderItem(bool wfok)
        {
            var v = ParseSqlValue(Domain.Content, wfok);
            var dt = v.domain;
            var a = Qlx.ASC;
            var n = Qlx.NULL;
            if (Match(Qlx.ASC))
                Next();
            else if (Match(Qlx.DESC))
            {
                a = Qlx.DESC;
                Next();
            }
            if (Match(Qlx.NULLS))
            {
                Next();
                if (Match(Qlx.FIRST))
                    Next();
                else if (tok == Qlx.LAST)
                {
                    n = Qlx.LAST;
                    Next();
                }
            }
            if (a == v.domain.AscDesc && n == v.domain.nulls)
                return v.defpos;
            if (dt.defpos < Transaction.Analysing)
                dt = (Domain)dt.Relocate(cx.GetUid());
            dt += (Domain.Descending, a);
            dt += (Domain.NullsFirst, n);
            cx.Add(dt);
            return cx.Add(new SqlTreatExpr(cx.GetUid(), v, dt)).defpos;
        }
        /// <summary>
		/// RowSetSpec = SELECT [ALL|DISTINCT] SelectList [INTO Targets] TableExpression .
        /// Many identifiers in the selectList will be resolved in the TableExpression.
        /// This select tree and tableExpression may both contain queries.
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>The RowSetSpec</returns>
		RowSet ParseQuerySpecification(Domain xp, bool ambient = false)
        {
            var id = new Ident(this);
            var inced = false;
            Mustbe(Qlx.SELECT, Qlx.RETURN);
            if ((!ambient) && lxr.prevtok == Qlx.SELECT)
            {
                cx.IncSD(id);
                inced = true;
            }
            var d = ParseDistinctClause();
            var dm = ParseSelectList(id.iix.dp, xp);
            cx.Add(dm);
            RowSet te = ParseTableExpression(id.iix, dm);
            if (Match(Qlx.FOR))
            {
                Next();
                Mustbe(Qlx.UPDATE);
            }
            if (inced)
                cx.DecSD(dm, te);
            te = (SelectRowSet?)cx.obs[te.defpos] ?? throw new PEException("PE1967");
            if (d)
                te = new DistinctRowSet(cx, te);
            return te;
        }
        /// <summary>
        /// [DISTINCT|ALL]
        /// </summary>
        /// <returns>whether DISTINCT has been specified</returns>
		bool ParseDistinctClause()
        {
            bool r = false;
            if (tok == Qlx.DISTINCT)
            {
                Next();
                r = true;
            }
            else if (tok == Qlx.ALL)
                Next();
            return r;
        }
        /// <summary>
		/// SelectList = '*' | SelectItem { ',' SelectItem } .
        /// </summary>
        /// <param name="dp">The position of the SELECT keyword</param>
        /// <param name="xp">the expected result type, or Domain.Content</param>
		Domain ParseSelectList(long dp, Domain xp)
        {
            QlValue v;
            var j = 0;
            var vs = BList<DBObject>.Empty;
            v = ParseSelectItem(dp, xp, j++);
            if (v is not null) // star items do not have a value to add at this stage
                vs += v;
            while (tok == Qlx.COMMA)
            {
                Next();
                v = ParseSelectItem(dp, xp, j++);
                if (v is not null)
                    vs += v;
            }
            return (Domain)cx.Add(new Domain(cx.GetUid(), cx, Qlx.TABLE, vs, vs.Length));
        }
        QlValue ParseSelectItem(long q, Domain xp, int pos)
        {
            Domain dm = Domain.Content;
            if (xp.rowType.Length > pos)
                dm = xp.representation[xp[pos] ?? -1L] ?? throw new PEException("PE1675");
            return ParseSelectItem(q, dm);
        }
        /// <summary>
		/// SelectItem = * | (Scalar [AS id ]) | (RowValue [.*] [AS IdList]) .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <param name="t">the expected obs type for the query</param>
        /// <param name="pos">The position in the SelectList</param>
        QlValue ParseSelectItem(long q, Domain xp)
        {
            Ident alias;
            QlValue v;
            if (tok == Qlx.TIMES)
            {
                var lp = LexPos();
                Next();
                v = new SqlStar(lp.dp, cx, -1L);
            }
            else
            {
                v = ParseSqlValue(xp, true);
                if (q >= 0)
                    v = (QlValue)v.AddFrom(cx, q);
            }
            if (tok == Qlx.AS)
            {
                Next();
                alias = new Ident(this);
                var n = v.name;
                var nv = v;
                if (n == "")
                    nv += (ObInfo.Name, alias.ident);
                else
                    nv += (DBObject._Alias, alias.ident);
                if (cx.defs.Contains(alias.ident) && cx.defs[alias.ident]?[alias.iix.sd].Item1 is Iix ob
                    && cx.obs[ob.dp] is QlValue ov)
                {
                    var v0 = nv;
                    nv = (QlValue)nv.Relocate(ov.defpos);
                    cx.Replace(v0, nv);
                }
                else
                    cx.Add(nv);
                cx.defs += (alias, new Iix(v.defpos, cx, v.defpos));
                cx.Add(nv);
                Mustbe(Qlx.Id);
                v = nv;
            }
            else
                cx.Add(v);
   /*         if (v.domain.kind == Qlx.TABLE)
            {
                // we want a scalar from this
                var dm = cx.obs[v.domain[0] ?? -1L] as Domain ?? Domain.Content;
                cx.Add(v + (DBObject._Domain, dm));
            } */
            return v;
        }
        /// <summary>
		/// TableExpression = FromClause [ WhereClause ] [ GroupByClause ] [ HavingClause ] [WindowClause] .
        /// The ParseFromClause is called before this
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="t">the expected obs type</param>
        /// <returns>The TableExpression</returns>
		SelectRowSet ParseTableExpression(Iix lp, Domain d)
        {
            RowSet fm = ParseFromClause(lp.lp, d);
            if (cx.obs[d.defpos] is not Domain dm)
                throw new PEException("PE50310");
            var m = fm.mem;
            for (var b = fm.SourceProps.First(); b is not null; b = b.Next())
                if (b.value() is long p)
                    m -= p;
            m += (RowSet._Source, fm.defpos);
            var vs = BList<DBObject>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue sv
                    && sv.Verify(cx))
                {
                    (var ls, m) = sv.Resolve(cx, lp.dp, m);
                    vs += ls;
                }
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var ob = cx.obs[k];
                if (ob is QlValue sv)
                    sv.Resolve(cx, k, BTree<long, object>.Empty);
                else if (ob?.id is Ident ic && ob is ForwardReference fr
                    && cx.defs[ic.ident] is BTree<int, (Iix, Ident.Idents)> tt
                    && tt.Contains(cx.sD))
                {
                    var (iix, _) = tt[cx.sD];
                    if (cx.obs[iix.dp] is RowSet nb)
                    {
                        for (var c = fr.subs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is QlValue su && su.name is not null
                                && nb.names[su.name].Item2 is long sp
                                && cx.obs[sp] is QlValue ru)
                            {
                                cx.Replace(su, ru);
                                cx.undefined -= su.defpos;
                            }
                        cx.Replace(ob, nb);
                        if (nb.alias is not null)
                            cx.UpdateDefs(ic, nb, nb.alias);
                        cx.undefined -= k;
                        cx.NowTry();
                    }
                    for (var c = fr.subs.First(); c != null; c = c.Next())
                        if (cx.obs[c.key()] is Domain os && os.id != null)
                        {
                            var (iiy, _) = cx.defs[(os.id.ident, cx.sD)];
                            if (cx.obs[iiy.dp] is Domain oy)
                            {
                                cx.Replace(os, oy);
                                cx.undefined -= c.key();
                                cx.NowTry();
                            }
                        }
                }
            }
            fm = (RowSet)(cx.obs[fm.defpos] ?? throw new PEException("PE1666"));
            var ds = vs.Length;
            for (var b = fm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !fm.representation.Contains(p))
                    vs += v;
            dm = new Domain(dm.defpos, cx, Qlx.TABLE, vs, ds);
            cx.Add(dm);
            fm = (RowSet)(cx.obs[fm.defpos] ?? throw new PEException("PE2001"));
            return ParseSelectRowSet(new SelectRowSet(lp, cx, dm, fm, m));
        }
        SelectRowSet ParseSelectRowSet(SelectRowSet r)
        {
            var m = BTree<long, object>.Empty;
            var gc = Domain.Null;
            var gg = new GroupSpecification(cx.GetUid(), BTree<long, object>.Empty);
            if (r.aggs != CTree<long, bool>.Empty)
                m += (Domain.Aggs, r.aggs);
            if (tok == Qlx.WHERE)
            {
                var wc = ParseWhereClause() ?? throw new DBException("42161", "condition");
                var wh = new BTree<long, object>(RowSet._Where, wc);
                m += wh;
                ((RowSet)(cx.obs[r.source] ?? throw new PEException("PE2002"))).Apply(wh, cx);
            }
            if (tok == Qlx.GROUP)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "GROUP");
                if (ParseGroupClause(r) is GroupSpecification gs)
                {
                    gc = gs.Cols(cx, r);
                    gg = gs;
                    m += (RowSet.Group, gs.defpos);
                    m += (RowSet.GroupCols, gc);
                }
            }
            if (tok == Qlx.HAVING)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "HAVING");
                m += (RowSet.Having, ParseHavingClause(r));
            }
            if (Match(Qlx.WINDOW))
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "WINDOW");
                m += (RowSet.Windows, ParseWindowClause());
            }
            if (r.aggs.Count > 0 && cx.conn._tcp is not null)
            {
                // check for agged or grouped
                var os = CTree<long, bool>.Empty;
                var na = r.aggs;
                var gd = CTree<long, bool>.Empty;
                for (var b = na.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue x)
                        gd += x.Operands(cx);
                if (r.mem[Domain.Nodes] is CTree<long, bool> xs) // passed to us for KnownBy help
                {
                    gd += xs;
                    m += (Domain.Nodes, xs);
                    cx.Add(r + (Domain.Nodes, xs)); // do this before Apply!
                }
                for (var b = r.rowType.First(); b != null && b.key() < r.display; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue x)
                        os += x.ExposedOperands(cx, gd, gc);
                for (var b = r.where.First(); b != null && b.key() < r.display; b = b.Next())
                    if (cx.obs[b.key()] is QlValue x)
                        os += x.ExposedOperands(cx, gd, gc);
                for (var b = r.having.First(); b != null && b.key() < r.display; b = b.Next())
                    if (cx.obs[b.key()] is QlValue x)
                        os += x.ExposedOperands(cx, gd, gc);
                for (var b = r.window.First(); b != null && b.key() < r.display; b = b.Next())
                    if (cx.obs[b.key()] is QlValue x)
                        os += x.ExposedOperands(cx, gd, gc);
                if (os.Count > 0)
                {
            //                var dp = cx.GetUid();
            //                var nf = (SqlFunction)cx.Add(new SqlFunction(dp, cx, Qlx.RESTRICT, v, null, null, Qlx.NO));
            //                na += (nf.defpos, true);
                    if (gc.defpos < 0)
                        gc = (Domain)gc.Relocate(cx.GetUid());
                    var rt = gc.rowType;
                    var rs = gc.representation;
                    var gi = cx.obs[gg.sets.First()?.value() ?? -1L] as Grouping
                        ?? new Grouping(cx);
                    var ms = gi.members;
                    for (var b = os.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()] is QlValue v)
                        {
                            rt += v.defpos;
                            rs += (v.defpos, v.domain);
                            ms += (v.defpos, (int)ms.Count);
                        }
                    gc = gc + (Domain.RowType, rt) + (Domain.Representation, rs);
                    cx.Add(gc);
                    gi += (Grouping.Members, ms);
                    gi += (Level3.Index.Keys, gc);
                    cx.Add(gi);
                    gg += (GroupSpecification.Sets, new BList<long?>(gi.defpos));
                    cx.Add(gg);
                    m += (RowSet.Groupings, new BList<long?>(gi.defpos));
                    m += (RowSet.Group, gg.defpos);
                    m += (RowSet.GroupCols, gc);
                }
                if (na != r.aggs)
                    m += (Domain.Aggs, na);
            }
            r = (SelectRowSet)(cx.obs[r.defpos] ?? throw new PEException("PE20100"));
            r = (SelectRowSet)r.Apply(m, cx);
            return r;
        }
        /// <summary>
		/// FromClause = 	FROM TableReference { ',' TableReference } .
        /// (before WHERE, GROUP, etc).
        /// </summary>
        /// <param name="dp">The position for the selectrowset being constructed</param>
        /// <param name="dm">the selectlist </param>
        /// <returns>The resolved select domain and table expression</returns>
		RowSet ParseFromClause(long dp, Domain dm)
        {
            if (tok == Qlx.FROM)
            {
                Next();
                return (RowSet)cx.Add(ParseTableReference(dp, dm));
            }
            else
                return new TrivialRowSet(cx,dm);
        }
        /// <summary>
		/// TableReference = TableFactor Alias | JoinedTable .
        /// </summary>
        /// <param name="st">the future selectrowset defining position</param>
        /// <returns>and the new table reference item</returns>
        RowSet ParseTableReference(long st, Domain dm)
        {
            RowSet a;
            a = ParseTableReferenceItem(st, dm);
            cx.Add(a);
            while (Match(Qlx.COMMA, Qlx.CROSS, Qlx.NATURAL, Qlx.JOIN, Qlx.INNER, Qlx.LEFT, Qlx.RIGHT, Qlx.FULL, Qlx.ON))
            {
                var lp = LexPos();
                a = ParseJoinPart(lp.dp, a.Apply(new BTree<long, object>(DBObject._From, lp.dp), cx), dm);
            }
            return a;
        }
        /// <summary>
		/// TableFactor = 	Table_id [TimePeriodSpecification]
		/// | 	View_id 
		/// | 	Table_FunctionCall 
        /// |   Subquery
        /// |   ROWS '(' int [',' int] ')'
		/// | 	'(' TableReference ')'
		/// | 	TABLE '('  Value ')' 
		/// | 	UNNEST '('  Value ')'  (should allow a comma separated tree of array values)
        /// |   STATIC
        /// |   '[' docs ']' .
        /// Subquery = '(' QueryExpression ')' .
        /// </summary>
        /// <param name="st">the defining position of the selectrowset being constructed</param>
        /// <returns>the rowset for this table reference</returns>
		RowSet ParseTableReferenceItem(long st, Domain dm)
        {
            RowSet rf;
            var lp = new Iix(st, cx, LexPos().dp);
            if (Match(Qlx.ROWS)) // Pyrrho specific
            {
                Next();
                Mustbe(Qlx.LPAREN);
                var v = ParseSqlValue(Domain.Position);
                QlValue w = SqlNull.Value;
                if (tok == Qlx.COMMA)
                {
                    Next();
                    w = ParseSqlValue(Domain.Position);
                }
                Mustbe(Qlx.RPAREN);
                Match(Qlx.COMMA, Qlx.CROSS, Qlx.NATURAL, Qlx.JOIN, Qlx.INNER, Qlx.LEFT, Qlx.RIGHT, Qlx.FULL);
                if (tok == Qlx.Id || tok == Qlx.AS)
                {
                    if (tok == Qlx.AS)
                        Next();
                    new Ident(this);
                    Mustbe(Qlx.Id);
                }
                RowSet rs;
                if (w != SqlNull.Value)
                    rs = new LogRowColRowSet(lp.dp, cx,
                        Domain.Int.Coerce(cx, v.Eval(cx)).ToLong() ?? -1L,
                        Domain.Int.Coerce(cx, w.Eval(cx)).ToLong() ?? -1L);
                else
                    rs = new LogRowsRowSet(lp.dp, cx,
                        Domain.Int.Coerce(cx, v.Eval(cx)).ToLong() ?? -1L);
                cx.Add(rs);
                rf = rs;
            }
            // this syntax should allow multiple array/multiset arguments and ORDINALITY
            else if (Match(Qlx.UNNEST))
            {
                Next();
                Mustbe(Qlx.LPAREN);
                QlValue sv = ParseSqlValue(Domain.Content);
                cx.Add(sv);
                if (sv.domain.elType?.kind != Qlx.ROW)
                    throw new DBException("42161", sv);
                if (sv.domain.kind == Qlx.ARRAY)
                    rf = new ArrayRowSet(cx.GetUid(), cx, sv);
                else if (sv.domain.kind == Qlx.SET)
                    rf = new SetRowSet(cx.GetUid(), cx, sv);
                else if (sv.domain.kind == Qlx.MULTISET)
                    rf = new MultisetRowSet(cx.GetUid(), cx, sv);
                else throw new DBException("42161", sv);
                Mustbe(Qlx.RPAREN);
            }
            else if (Match(Qlx.TABLE))
            {
                Next();
                var cp = LexPos();
                Mustbe(Qlx.LPAREN); // SQL2003-2 7.6 required before table valued function
                Ident n = new(this);
                Mustbe(Qlx.Id);
                var r = BList<long?>.Empty;
                Mustbe(Qlx.LPAREN);
                if (tok != Qlx.RPAREN)
                    for (; ; )
                    {
                        r += cx.Add(ParseSqlValue(Domain.Content)).defpos;
                        if (tok == Qlx.RPAREN)
                            break;
                        Mustbe(Qlx.COMMA);
                    }
                Next();
                Mustbe(Qlx.RPAREN); // another: see above
                var proc = cx.GetProcedure(LexPos().dp, n.ident, cx.Signature(r))
                    ?? throw new DBException("42108", n.ident);
                ParseCorrelation(proc.domain);
                var ca = new SqlProcedureCall(cp.dp, cx, proc, r);
                cx.Add(ca);
                rf = ca.RowSets(n, cx, proc.domain, n.iix.dp);
            }
            else if (Match(Qlx.LPAREN,Qlx.LBRACE)) // subquery
            {
                var mt = tok;
                Next();
                cx.IncSD(new Ident("", LexPos()));
                rf = ParseRowSetSpec(Domain.TableType);
                cx.DecSD();
                Mustbe((mt==Qlx.LPAREN)?Qlx.RPAREN:Qlx.RBRACE);
                if (tok == Qlx.Id)
                {
                    var a = lxr.val.ToString();
                    var rx = cx.Ix(rf.defpos);
                    var ia = new Ident(a, rx);
                    for (var b = cx.defs[a]?.Last()?.value().Item2.First(); b != null; b = b.Next())
                        if (cx.obs[b.value()?.Last()?.value().Item1.dp ?? -1L] is QlValue lv
                            && (lv.domain.kind == Qlx.CONTENT || lv is SqlReview)
                            && lv.name != null
                            && rf.names.Contains(lv.name) && cx.obs[rf.names[lv.name].Item2 ?? -1L] is QlValue uv)
                        {
                            var nv = (Domain)uv.Relocate(lv.defpos);
                            cx.Replace(lv, nv);
                            cx.Replace(uv, nv);
                        }
                    cx.defs += (ia, rx);
                    cx.AddDefs(ia, rf);
                    Next();
                }
            }
            else if (Match(Qlx.STATIC))
            {
                Next();
                rf = new TrivialRowSet(cx, dm);
            }
            else if (tok == Qlx.LBRACK)
                rf = new TrivialRowSet(cx, dm) + (cx, RowSet.Target, ParseSqlDocArray().defpos);
            else // ordinary table, view, OLD/NEW TABLE id, or parameter
            {
                Ident ic = new(this);
                Mustbe(Qlx.Id);
                string? a = null;
                Match(Qlx.COMMA, Qlx.CROSS, Qlx.DO, Qlx.NATURAL, Qlx.JOIN, Qlx.INNER, Qlx.LEFT, 
                    Qlx.RIGHT, Qlx.FULL,Qlx.VALUES, Qlx.ON);
                if (tok == Qlx.Id || tok == Qlx.AS)
                {
                    if (tok == Qlx.AS)
                        Next();
                    a = lxr.val.ToString();
                    Mustbe(Qlx.Id);
                }
                var ob = (cx.GetObject(ic.ident) ?? cx.obs[cx.defs[ic].dp]);
                if (ob is null && long.TryParse(ic.ident, out var tp))
                    ob = cx.db.objects[tp] as DBObject;
                if (ob is null)
                    throw new DBException("42107", ic.ToString());
                if (ob is QlValue o && (ob.domain.kind != Qlx.TABLE || o.from < 0))
                    throw new DBException("42000", ob.domain.kind);
                if (ob is RowSet f)
                {
                    rf = f;
                    ob = cx.obs[f.target] as Table;
                }
                else
                {
                    cx.DefineForward(ic.ident);
                    cx.DefineForward(a);
                    rf = _From(ic, ob, dm, Grant.Privilege.Select, a);
                    cx.DefineStructures(ob as Table, rf);
                }
                if (Match(Qlx.FOR))
                {
                    var ps = ParsePeriodSpec();
                    var tb = ob as Table ?? throw new DBException("42000", "PeriodSpec");
                    rf += (cx, RowSet.Periods, rf.periods + (tb.defpos, ps));
                    long pp = (ps.periodname == "SYSTEM_TIME") ? tb.systemPS : tb.applicationPS;
                    if (pp < 0)
                        throw new DBException("42162", ps.periodname).Mix();
                    rf += (cx, RowSet.Periods, rf.periods + (tb.defpos, ps));
                }
                var rx = cx.Ix(rf.defpos);
            }
            return rf;
        }
        /// <summary>
        /// We are about to call the From constructor, which may
        /// Resolve undefined expressions in the SelectList 
        /// </summary>
        /// <param name="dp">The occurrence of this table reference</param>
        /// <param name="ob">The table or view referenced</param>
        /// <param name="q">The expected result for the enclosing query</param>
        /// <returns></returns>
        RowSet _From(Ident ic, DBObject ob, Domain dm, Grant.Privilege pr, string? a = null)
        {
            var dp = ic.iix.dp;
            if (ob != null)
            {
                if (ob is View ov)
                    ob = ov.Instance(dp, cx);
                ob._Add(cx);
            }
            if (ob == null)
                throw new PEException("PE2003");
            var ff = ob.RowSets(ic, cx, dm, ic.iix.dp, pr, a);
            for (var b = ff.rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value() ?? -1L] is QlValue sv && sv.domain is UDType ut
                    && sv.name is not null)
                {
                    var id = new Ident(sv.name, new Iix(sv.defpos));
                    cx.AddDefs(id, ut.rowType, sv.alias);
                }
            var un = CTree<long, bool>.Empty;
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                if (b.key() is long k && cx.obs[k] is DBObject uo)
                {
                    var (ix, ids) = cx.defs[(uo.name, cx.sD)];
                    for (var c = ids.First(); c != null; c = c.Next())
                        for (var d = c.value().First(); d != null; d = d.Next())
                            un += (d.value().Item1.dp, true);
                    if (uo is SqlReview sv &&
                        (b.value() == cx.sD - 1 || b.value() == cx.sD) &&
                        cx.obs[ix.dp] is QlValue ts
                        && !un.Contains(k))
                    {
                        cx.undefined -= k;
                        var nv = (QlValue)cx.Add(ts.Relocate(k));
                        if (sv.alias is not null)
                            nv += (DBObject._Alias, sv.alias);
                        cx.Add(nv);
                        cx.Replace(ts, nv); // looks like it should be sv, but this is correct
                        cx.NowTry();
                    }
                    if (uo is SqlMethodCall um && um.procdefpos < 0
                        && cx.obs[um.var] is QlValue su && su.domain is UDType ut
                        && ut.infos[cx.role.defpos] is ObInfo ui && um.name != null
                        && ui.methodInfos[um.name] is BTree<CList<Domain>, long?> st)
                    {
                        var dl = CList<Domain>.Empty;
                        for (var c = um.parms.First(); c != null; c = c.Next())
                            if (c.value() is long q && cx.obs[q] is QlValue av)
                                dl += av.domain;
                        for (var c = st.First(); c != null; c = c.Next())
                            if (c.key().Length == dl.Length)
                            {
                                var db = dl.First();
                                for (var d = c.key().First(); d != null && db != null;
                                    d = d.Next(), db = db.Next())
                                    if (!db.value().CanTakeValueOf(d.value()))
                                        goto next;
                                if (c.value() is long m && cx.db.objects[m] is Method me)
                                {
                                    cx.undefined -= k;
                                    var nm = um + (SqlCall.ProcDefPos, me.defpos);
                                    cx.Add(nm);
                                    cx.Replace(um, nm);
                                    break;
                                }
                            next:;
                            }
                    }
                }
            }
            return (RowSet)(cx.obs[ff.defpos] ?? throw new PEException("PE20720"));
        }
        /// <summary>
        /// TimePeriodSpec = 
        ///    |    AS OF TypedValue
        ///    |    BETWEEN(ASYMMETRIC|SYMMETRIC)  TypedValue AND TypedValue
        ///    |    FROM TypedValue TO TypedValue .
        /// </summary>
        /// <returns>The periodSpec</returns>
        PeriodSpec ParsePeriodSpec()
        {
            string pn = "SYSTEM_TIME";
            Qlx kn;
            QlValue? t1 = null, t2 = null;
            Next();
            if (tok == Qlx.Id)
                pn = lxr.val.ToString();
            Mustbe(Qlx.SYSTEM_TIME, Qlx.Id);
            kn = tok;
            switch (tok)
            {
                case Qlx.AS:
                    Next();
                    Mustbe(Qlx.OF);
                    t1 = ParseSqlValue(Domain.UnionDate);
                    break;
                case Qlx.BETWEEN:
                    Next();
                    kn = Qlx.ASYMMETRIC;
                    if (Match(Qlx.ASYMMETRIC))
                        Next();
                    else if (Match(Qlx.SYMMETRIC))
                    {
                        Next();
                        kn = Qlx.SYMMETRIC;
                    }
                    t1 = ParseSqlValueTerm(Domain.UnionDate, false);
                    Mustbe(Qlx.AND);
                    t2 = ParseSqlValue(Domain.UnionDate);
                    break;
                case Qlx.FROM:
                    Next();
                    t1 = ParseSqlValue(Domain.UnionDate);
                    Mustbe(Qlx.TO);
                    t2 = ParseSqlValue(Domain.UnionDate);
                    break;
                default:
                    kn = Qlx.NO;
                    break;
            }
            return new PeriodSpec(pn, kn, t1, t2);
        }
        /// <summary>
        /// Alias = 		[[AS] id [ Cols ]] .
        /// Creates a new ObInfo for the derived table.
        /// </summary>
        /// <returns>The correlation info</returns>
		ObInfo? ParseCorrelation(Domain xp)
        {
            if (tok == Qlx.Id || tok == Qlx.AS)
            {
                if (tok == Qlx.AS)
                    Next();
                var cs = BList<long?>.Empty;
                var rs = CTree<long, Domain>.Empty;
                var tablealias = new Ident(this);
                Mustbe(Qlx.Id);
                if (tok == Qlx.LPAREN)
                {
                    Next();
                    var ids = ParseIDList();
                    if (ids.Length != xp.Length)
                        throw new DBException("22000", xp);
                    var ib = ids.First();
                    for (var b = xp.rowType.First(); ib != null && b != null; b = b.Next(), ib = ib.Next())
                        if (b.value() is long oc)
                        {
                            var cp = ib.value().iix.dp;
                            var cd = xp.representation[oc] ?? throw new PEException("PE47169");
                            cs += cp;
                            rs += (cp, cd);
                        }
                    xp = new Domain(cx.GetUid(), cx, Qlx.TABLE, rs, cs);
                    cx.Add(xp);
                    return new ObInfo(tablealias.ident, Grant.Privilege.Execute);
                }
                else
                    return new ObInfo(tablealias.ident, Grant.Privilege.Execute);
            }
            return null;
        }
        /// <summary>
		/// JoinType = 	INNER | ( LEFT | RIGHT | FULL ) [OUTER] .
        /// </summary>
        /// <param name="v">The JoinPart being parsed</param>
		Qlx ParseJoinType()
        {
            Qlx r = Qlx.INNER;
            if (Match(Qlx.INNER))
                Next();
            else if (Match(Qlx.LEFT,Qlx.RIGHT,Qlx.FULL))
            {
                r = tok;
                Next();
            }
            if (r != Qlx.INNER && Match(Qlx.OUTER))
                Next();
            return r;
        }
        /// <summary>
		/// JoinedTable = 	TableReference CROSS JOIN TableFactor 
		/// 	|	TableReference NATURAL [JoinType] JOIN TableFactor
		/// 	|	TableReference [JoinType] JOIN TableReference ON QlValue .
        /// </summary>
        /// <param name="q">The eexpected domain q</param>
        /// <param name="fi">The RowSet so far</param>
        /// <returns>the updated query</returns>
        RowSet ParseJoinPart(long dp, RowSet fi, Domain dm)
        {
            var left = fi;
            Qlx jkind;
            RowSet right;
            var m = BTree<long, object>.Empty;
            if (Match(Qlx.COMMA))
            {
                jkind = Qlx.CROSS;
                Next();
                right = ParseTableReferenceItem(dp, dm);
            }
            else if (Match(Qlx.CROSS))
            {
                jkind = Qlx.CROSS;
                Next();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, dm);
            }
            else if (Match(Qlx.NATURAL))
            {
                m += (JoinRowSet.Natural, tok);
                Next();
                jkind = ParseJoinType();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, dm);
            }
            else
            {
                jkind = ParseJoinType();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, dm);
                if (tok == Qlx.USING)
                {
                    m += (JoinRowSet.Natural, tok);
                    Next();
                    var ns = ParseIDList();
                    var sd = cx.sD;
                    var (_, li) = (left.alias is not null) ? cx.defs[(left.alias, sd)] : cx.defs[(left.name, sd)];
                    var (_, ri) = (right.alias is not null) ? cx.defs[(right.alias, sd)] : cx.defs[(right.name, sd)];
                    var cs = BTree<long, long?>.Empty;
                    for (var b = ns.First(); b != null; b = b.Next())
                        cs += (ri[b.value()].dp, li[b.value()].dp);
                    m += (JoinRowSet.JoinUsing, cs);
                }
                else
                {
                    Mustbe(Qlx.ON);
                    var oc = ParseSqlValue(Domain.Bool).Disjoin(cx);
                    var on = BTree<long, long?>.Empty;
                    var wh = CTree<long, bool>.Empty;
                    left = (RowSet)(cx.obs[left.defpos] ?? throw new PEException("PE2005"));
                    right = (RowSet)(cx.obs[right.defpos] ?? throw new PEException("PE2006"));
                    var ls = CList<QlValue>.Empty;
                    var rs = CList<QlValue>.Empty;
                    var lm = cx.Map(left.rowType);
                    var rm = cx.Map(right.rowType);
                    for (var b = oc.First(); b != null; b = b.Next())
                    {
                        if (cx.obs[b.key()] is not SqlValueExpr se || se.domain.kind != Qlx.BOOLEAN)
                            throw new DBException("42151");
                        var lf = se.left;
                        var rg = se.right;
                        if (cx.obs[lf] is QlValue sl && cx.obs[rg] is QlValue sr && se.op == Qlx.EQL)
                        {
                            var rev = !lm.Contains(lf);
                            if (rev)
                            {
                                if ((!rm.Contains(lf))
                                    || (!lm.Contains(rg)))
                                    throw new DBException("42151");
                                oc += (cx.Add(new SqlValueExpr(se.defpos, cx, Qlx.EQL,
                                    sr, sl, Qlx.NO)).defpos, true);
                                ls += sr;
                                rs += sl;
                                on += (rg, lf);
                            }
                            else
                            {
                                if (!rm.Contains(rg))
                                    throw new DBException("42151");
                                ls += sl;
                                rs += sr;
                                on += (lf, rg);
                            }
                        }
                        else
                        {
                            oc -= se.defpos;
                            wh += (se.defpos, true);
                        }
                    }
                    if (oc != CTree<long, bool>.Empty)
                        m += (JoinRowSet.JoinCond, oc);
                    if (on != BTree<long, long?>.Empty)
                        m += (JoinRowSet.OnCond, on);
                    if (wh != CTree<long, bool>.Empty)
                        m += (RowSet._Where, wh);
                }
            }
            left = (RowSet)(cx.obs[left.defpos] ?? throw new PEException("PE207030"));
            right = (RowSet)(cx.obs[right.defpos] ?? throw new PEException("PE207031"));
            var r = new JoinRowSet(dp, cx, left, jkind, right, m);
            return (JoinRowSet)cx.Add(r);
        }
        /// <summary>
		/// GroupByClause = GROUP BY [DISTINCT|ALL] GroupingElement { ',' GroupingElement } .
        /// GroupingElement = GroupingSet | (ROLLUP|CUBE) '('GroupingSet {',' GroupingSet} ')'  
        ///     | GroupSetsSpec | '('')' .
        /// GroupingSet = Col | '(' Col {',' Col } ')' .
        /// GroupingSetsSpec = GROUPING SETS '(' GroupingElement { ',' GroupingElement } ')' .
        /// </summary>
        /// <returns>The GroupSpecification</returns>
        GroupSpecification? ParseGroupClause(RowSet rs)
        {
            if (tok != Qlx.GROUP)
                return null;
            Next();
            var lp = LexPos();
            Mustbe(Qlx.BY);
            bool d = false;
            if (tok == Qlx.ALL)
                Next();
            else if (tok == Qlx.DISTINCT)
            {
                Next();
                d = true;
            }
            bool simple = true;
            GroupSpecification r = new(lp.dp, cx, BTree<long, object>.Empty
                + (GroupSpecification.DistinctGp, d));
            r = ParseGroupingElement(r, rs, ref simple);
            while (tok == Qlx.COMMA)
            {
                Next();
                r = ParseGroupingElement(r, rs, ref simple);
            }
            // simplify: see SQL2003-02 7.9 SR 10 .
            if (simple && r.sets.Count > 1)
            {
                var ms = CTree<long, int>.Empty;
                var i = 0;
                for (var g = r.sets.First(); g != null; g = g.Next())
                    if (g.value() is long gp)
                        for (var h = ((Grouping?)cx.obs[gp])?.members.First(); h != null; h = h.Next())
                            ms += (h.key(), i++);
                var gn = new Grouping(cx, new BTree<long, object>(Grouping.Members, ms));
                cx.Add(gn);
                r += (GroupSpecification.Sets, new BList<long?>(gn.defpos));
            }
            return (GroupSpecification)cx.Add(r);
        }
        /// <summary>
        /// A grouping element
        /// </summary>
        /// <param name="g">the group specification</param>
        /// <param name="simple">whether it is simple</param>
        /// <returns>whether it is simple</returns>
        GroupSpecification ParseGroupingElement(GroupSpecification g, RowSet rs, ref bool simple)
        {
            if (Match(Qlx.Id))
            {
                var c = ParseSqlValue(Domain.Content);
                for (var b = rs.rowType.First(); b != null; b = b.Next())
                    if (cx.obs[b.value() ?? -1L] is QlValue sb && sb._MatchExpr(cx, c, rs))
                        c = sb;
                var ls = new Grouping(cx, BTree<long, object>.Empty + (Grouping.Members,
                    new CTree<long, int>(c.defpos, 0)));
                cx.Add(ls);
                g += (cx, GroupSpecification.Sets, g.sets + ls.defpos);
                simple = true;
                return (GroupSpecification)cx.Add(g);
            }
            simple = false;
            if (Match(Qlx.LPAREN))
            {
                var lp = LexPos();
                Next();
                if (tok == Qlx.RPAREN)
                {
                    Next();
                    g += (cx, GroupSpecification.Sets, g.sets + cx.Add(new Grouping(cx)).defpos);
                    return (GroupSpecification)cx.Add(g);
                }
                g += (cx, GroupSpecification.Sets, g.sets + cx.Add(ParseGroupingSet()).defpos);
                return (GroupSpecification)cx.Add(g);
            }
#if OLAP
            if (Match(Sqlx.GROUPING))
            {
#else
            Mustbe(Qlx.GROUPING);
#endif
            Next();
            Mustbe(Qlx.SETS);
            Mustbe(Qlx.LPAREN);
            g = ParseGroupingElement(g, rs, ref simple);
            while (tok == Qlx.COMMA)
            {
                Next();
                g = ParseGroupingElement(g, rs, ref simple);
            }
            Mustbe(Qlx.RPAREN);
#if OLAP
        }
            var rc = tok;
            Mustbe(Sqlx.ROLLUP, Sqlx.CUBE);
            Mustbe(Sqlx.LPAREN);
            g += (GroupSpecification.Sets,g.sets + ParseGroupingSet(rc));
            while (Match(Sqlx.COMMA))
            {
                Next();
                g +=(GroupSpecification.Sets,g.sets + ParseGroupingSet(rc));
            }
#endif
            return (GroupSpecification)cx.Add(g);
        }
        /// <summary>
        /// a grouping set
        /// </summary>
        /// <returns>the grouping</returns>
        Grouping ParseGroupingSet()
        {
            var cn = ParseIdent();
            var t = new Grouping(cx, BTree<long, object>.Empty
                + (Grouping.Members, new CTree<long, int>(cn.iix.dp, 0)));
            var i = 1;
            while (Match(Qlx.COMMA))
            {
                cn = ParseIdent();
                t += (Grouping.Members, t.members + (cn.iix.dp, i++));
            }
            Mustbe(Qlx.RPAREN);
            return (Grouping)cx.Add(t);
        }
        /// <summary>
		/// HavingClause = HAVING BooleanExpr .
        /// </summary>
        /// <returns>The QlValue (Boolean expression)</returns>
		CTree<long, bool> ParseHavingClause(Domain dm)
        {
            var r = CTree<long, bool>.Empty;
            if (tok != Qlx.HAVING)
                return r;
            Next();
            var lp = LexPos();
            r = ParseSqlValueDisjunct(Domain.Bool, false, dm);
            if (tok != Qlx.OR)
                return r;
            var left = Disjoin(r);
            while (tok == Qlx.OR)
            {
                Next();
                left = (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Qlx.OR, left,
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool, false, dm)), Qlx.NO));
            }
            r += (left.defpos, true);
            //      lxr.context.cur.Needs(left.alias ?? left.name, RowSet.Need.condition);
            return r;
        }
        /// <summary>
		/// WhereClause = WHERE BooleanExpr .
        /// </summary>
        /// <returns>The QlValue (Boolean expression)</returns>
		CTree<long, bool>? ParseWhereClause()
        {
            cx.done = ObTree.Empty;
            if (tok != Qlx.WHERE)
                return null;
            Next();
            var r = ParseSqlValueDisjunct(Domain.Bool, false);
            if (tok != Qlx.OR)
                return cx.FixTlb(r);
            var left = Disjoin(r);
            while (tok == Qlx.OR)
            {
                var lp = LexPos();
                Next();
                left = (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Qlx.OR, left,
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool, false)), Qlx.NO));
                left = (QlValue)cx.Add(left);
            }
            r += (left.defpos, true);
            //       lxr.context.cur.Needs(left.alias ?? left.name,RowSet.Need.condition);
            return cx.FixTlb(r);
        }
        /// <summary>
		/// WindowClause = WINDOW WindowDef { ',' WindowDef } .
        /// </summary>
        /// <returns>the window set as a tree by window names</returns>
        BTree<string, WindowSpecification> ParseWindowClause()
        {
            if (tok != Qlx.WINDOW)
                throw new DBException("42000", tok);
            Next();
            var tree = BTree<string, WindowSpecification>.Empty; // of WindowSpecification
            ParseWindowDefinition(ref tree);
            while (tok == Qlx.COMMA)
            {
                Next();
                ParseWindowDefinition(ref tree);
            }
            return tree;
        }
        /// <summary>
		/// WindowDef = id AS '(' WindowDetails ')' .
        /// </summary>
        /// <param name="tree">ref: the tree of windowdefs</param>
        void ParseWindowDefinition(ref BTree<string, WindowSpecification> tree)
        {
            var id = lxr.val;
            Mustbe(Qlx.Id);
            Mustbe(Qlx.AS);
            WindowSpecification r = ParseWindowSpecificationDetails();
            if (r.orderWindow != null)
            {
                if (tree[r.orderWindow] is not WindowSpecification ow)
                    throw new DBException("42135", r.orderWindow).Mix();
                if (ow.order != Domain.Row && r.order != Domain.Row)
                    throw new DBException("42000", "7.11 SR10d").ISO();
                if (ow.order != Domain.Row)
                    throw new DBException("42000", "7.11 SR10c").ISO();
                if (ow.units != Qlx.NO || ow.low != null || ow.high != null)
                    throw new DBException("42000", "7.11 SR10e").ISO();
            }
            tree += (id.ToString(), r);
        }
        /// <summary>
        /// An SQL insert statement
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="sql">the ql</param>
        /// <returns>the SqlInsert</returns>
        internal void ParseSqlInsert(string sql)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            ParseSqlInsert();
        }
        /// <summary>
		/// Insert = INSERT INTO Table_id [ Cols ]  TypedValue [Classification]
        ///        |  INSERT GraphPattern.
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseSqlInsert()
        {
            var lp = LexPos();
            Next();
            if (Match(Qlx.INTO))
                Next();
            else return ParseInsertGraph();
            Ident ic = new(this);
            cx.IncSD(ic);
            var fm = ParseTableReference(ic.iix.dp, Domain.TableType);
            cx.Add(fm);
            if (fm is not TableRowSet && !cx.defs.Contains(ic.ident))
                cx.defs += (ic, ic.iix);
            cx.AddDefs(ic, fm);
            Domain? cs = null;
            // Ambiguous syntax here: (Cols) or (Subquery) or other possibilities
            if (tok == Qlx.LPAREN)
            {
                if (ParseColsList(fm) is Domain cd)
                {
                    fm = (RowSet)fm.New(cx.GetUid(), fm.mem + (Domain.Representation, cd.representation)
                        + (Domain.RowType, cd.rowType) + (Domain.Display, cd.Length));
                    cx.Add(fm);
                    cs = cd;
                }
                else
                    tok = lxr.PushBack(Qlx.LPAREN);
            }
            QlValue sv;
            cs ??= new Domain(cx.GetUid(), cx, Qlx.ROW, fm.representation, fm.rowType, fm.Length);
            var vp = cx.GetUid();
            if (Match(Qlx.DEFAULT))
            {
                Next();
                Mustbe(Qlx.VALUES);
                sv = SqlNull.Value;
            }
            else
                // care: we might have e.g. a subquery here
                sv = ParseSqlValue(fm);
            if (sv is SqlRow) // tolerate a single value without the VALUES keyword
                sv = new SqlRowArray(vp, cx, sv.domain, new BList<long?>(sv.defpos));
            var sce = sv.RowSetFor(vp, cx, fm.rowType, fm.representation) + (cx, RowSet.RSTargets, fm.rsTargets)
                + (RowSet.Asserts, RowSet.Assertions.AssignTarget);
            cx._Add(sce);
            SqlInsert s = new(lp.dp, fm, sce.defpos, cs);
            cx.Add(s);
            cx.result = s.value;
            if (Match(Qlx.SECURITY))
            {
                Next();
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER);
                s += (DBObject.Classification, MustBeLevel());
            }
            cx.DecSD();
            if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                cx = tr.Execute(s, cx);
            cx.exec = s;
            return (SqlInsert)cx.Add(s);
        }
        /// <summary>
		/// Delete = DELETE Node_Value | FROM Table_id [ WhereClause] .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseSqlDelete()
        {
            var lp = LexPos();
            Next();
            if (!Match(Qlx.WITH, Qlx.FROM))
            {
                var n = ParseSqlValue(Domain.NodeType) ?? throw new DBException("42161", "Node or Edge");
                return (Executable)cx.Add(new DeleteNode(cx.GetUid(), n));
            }
            Mustbe(Qlx.FROM);
            Ident ic = new(this);
            cx.IncSD(ic);
            Mustbe(Qlx.Id);
            if (tok == Qlx.AS)
                Next();
            if (tok == Qlx.Id)
            {
                new Ident(this);
                Next();
            }
            var ob = cx.GetObject(ic.ident);
            if (ob == null && cx.defs.Contains(ic.ident))
                ob = cx.obs[cx.defs[(ic.ident, lp.sd)].Item1.dp];
            if (ob is null && long.TryParse(ic.ident, out var tp))
                ob = cx.db.objects[tp] as DBObject;
            if (ob == null)
                throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob as Domain ?? Domain.Null, Grant.Privilege.Delete));
            QuerySearch qs = new(lp.dp, f);
            cx.defs += (ic, lp);
            cx.GetUid();
            cx.Add(qs);
            var rs = (RowSet?)cx.obs[qs.source] ?? throw new PEException("PE2006");
            if (ParseWhereClause() is CTree<long, bool> wh)
            {
                rs = (RowSet?)cx.obs[rs.defpos] ?? throw new PEException("PE2007");
                rs = rs.Apply(RowSet.E + (RowSet._Where, rs.where + wh), cx);
            }
            cx._Add(rs);
            cx.result = rs.defpos;
            if (tok != Qlx.EOF)
                throw new DBException("42000", tok);
            cx.DecSD();
            if (cx.parse == ExecuteStatus.Obey)
                cx = ((Transaction)cx.db).Execute(qs, cx);
            cx.result = -1L;
            cx.exec = qs;
            return (Executable)cx.Add(qs);
        }
        /// <summary>
        /// the update statement
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="sql">the ql</param>
        /// <returns>the updatesearch</returns>
        internal Context ParseSqlUpdate(Context cx, string sql)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return ParseSqlUpdate().Item1;
        }
        /// <summary>
		/// Update = UPDATE [XMLOption] Table_id Assignment [WhereClause] .
        /// </summary>
        /// <returns>The UpdateSearch</returns>
		(Context, Executable) ParseSqlUpdate()
        {
            var st = LexPos().dp;
            Next();
            Ident ic = new(this);
            cx.IncSD(ic);
            Mustbe(Qlx.Id);
            Mustbe(Qlx.SET);
            var ob = cx.GetObject(ic.ident) as Domain;
            if (ob == null && cx.defs.Contains(ic.ident))
                ob = cx.obs[cx.defs[(ic.ident, ic.iix.sd)].Item1.dp] as Domain;
            if (ob == null)
                throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob, Grant.Privilege.Update));
            cx.AddDefs(ic, f);
            for (var b = f.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue c && c.name is not null)
                {
                    var dp = cx.Ix(p);
                    cx.defs += (new Ident(c.name, dp), dp);
                }
            UpdateSearch us = new(st, f);
            cx.Add(us);
            var ua = ParseAssignments();
            var rs = (RowSet)(cx.obs[us.source] ?? throw new DBException("PE2009"));
            rs = rs.Apply(new BTree<long, object>(RowSet.Assig, ua), cx);
            if (ParseWhereClause() is CTree<long, bool> wh)
            {
                rs = (RowSet)(cx.obs[rs.defpos] ?? throw new DBException("PE2010"));
                rs = rs.Apply(new BTree<long, object>(RowSet._Where, wh), cx);
            }
            cx.result = rs.defpos;
            if (cx.parse == ExecuteStatus.Obey)
                cx = ((Transaction)cx.db).Execute(us, cx);
            us = (UpdateSearch)cx.Add(us);
            cx.exec = us;
            cx.DecSD();
            return (cx, us);
        }
        internal CTree<UpdateAssignment, bool> ParseAssignments(string sql)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            return ParseAssignments();
        }
        /// <summary>
        /// Assignment = 	SET Target '='  TypedValue { ',' Target '='  TypedValue }
        /// </summary>
        /// <returns>the tree of assignments</returns>
		CTree<UpdateAssignment, bool> ParseAssignments()
        {
            var r = CTree<UpdateAssignment, bool>.Empty + (ParseUpdateAssignment(), true);
            while (tok == Qlx.COMMA)
            {
                Next();
                r += (ParseUpdateAssignment(), true);
            }
            return r;
        }
        /// <summary>
        /// Target '='  TypedValue
        /// </summary>
        /// <returns>An updateAssignmentStatement</returns>
		UpdateAssignment ParseUpdateAssignment()
        {
            QlValue vbl;
            QlValue val;
            Match(Qlx.SECURITY);
            if (tok == Qlx.SECURITY)
            {
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER);
                vbl = (QlValue)cx.Add(new SqlSecurity(LexPos().dp, cx));
                Next();
            }
            else vbl = ParseVarOrColumn(Domain.Content);
            Mustbe(Qlx.EQL);
            val = ParseSqlValue(vbl.domain);
            return new UpdateAssignment(vbl.defpos, val.defpos);
        }
        /// <summary>
        /// Parse an SQL Value
        /// </summary>
        /// <param name="s">The string to parse</param>
        /// <param name="t">the expected obs type if any</param>
        /// <returns>the QlValue</returns>
        internal QlValue ParseSqlValue(string s, Domain xp)
        {
            lxr = new Lexer(cx, s);
            tok = lxr.tok;
            return ParseSqlValue(xp);
        }
        internal QlValue ParseSqlValue(Ident ic, Domain xp)
        {
            lxr = new Lexer(cx, ic.ident, ic.iix.lp);
            tok = lxr.tok;
            return ParseSqlValue(xp);
        }
        /// <summary>
        /// Alas the following informal syntax is not a good guide to the way LL(1) has to go...
		///  Value = 		Literal
        /// |   Id ':'  TypedValue
		/// |	Value BinaryOp TypedValue 
		/// | 	'-'  TypedValue 
		/// |	'('  TypedValue ')'
		/// |	Value Collate 
		/// | 	Value '['  Value ']'
		/// |	ColumnRef  
		/// | 	VariableRef
        /// |   PeriodName
        /// |   PERIOD '('  Value,  Value ')'
		/// |	VALUE 
        /// |   ROW
		/// |	Value '.' Member_id
		/// |	MethodCall
		/// |	NEW MethodCall 
		/// | 	FunctionCall 
		/// |	VALUES  '('  Value { ','  Value } ')' { ',' '('  Value { ','  Value } ')' }
		/// |	Subquery
        /// |   ARRAY Subquery
		/// |	(MULTISET|ARRAY) '['  Value { ','  Value } ']'
        /// |   ROW '(' Value { ',' Value ')'
		/// | 	TABLE '('  Value ')' 
		/// |	TREAT '('  Value AS Sub_Type ')'  .
        /// PeriodName = SYSTEM_TIME | id .
        /// </summary>
        /// <param name="t">a constraint on usage</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the QlValue</returns>
        internal QlValue ParseSqlValue(Domain xp, bool wfok = false)
        {
            if (Match(Qlx.PERIOD))
            {
                Next();
                Mustbe(Qlx.LPAREN);
                var op1 = ParseSqlValue(Domain.UnionDate);
                Mustbe(Qlx.COMMA);
                var op2 = ParseSqlValue(Domain.UnionDate);
                Mustbe(Qlx.RPAREN);
                var r = new SqlValueExpr(LexPos().dp, cx, Qlx.PERIOD, op1, op2, Qlx.NO);
                return (QlValue)cx.Add(r);
            }
            QlValue left;
            if (xp.kind == Qlx.BOOLEAN || xp.kind == Qlx.CONTENT)
            {
                left = Disjoin(ParseSqlValueDisjunct(xp, wfok));
                while (left.domain.kind == Qlx.BOOLEAN && tok == Qlx.OR)
                {
                    Next();
                    left = new SqlValueExpr(LexPos().dp, cx, Qlx.OR, left,
                        Disjoin(ParseSqlValueDisjunct(xp, wfok)), Qlx.NO);
                }
            }
            else if (xp.kind == Qlx.TABLE || xp.kind == Qlx.VIEW || xp.kind == Qlx.TYPE || xp is NodeType)
            {
                if (Match(Qlx.TABLE))
                    Next();
                left = ParseSqlTableValue(xp);
                while (Match(Qlx.UNION, Qlx.EXCEPT, Qlx.INTERSECT))
                {
                    var lp = LexPos();
                    var op = tok;
                    var m = Qlx.NO;
                    Next();
                    if ((op == Qlx.UNION || op == Qlx.EXCEPT)
                        && Match(Qlx.ALL, Qlx.DISTINCT))
                    {
                        m = tok;
                        Next();
                    }
                    var right = ParseSqlTableValue(xp);
                    left = new SqlValueExpr(lp.dp, cx, op, left, right, m);
                }
            }
            else if (xp.kind == Qlx.TYPE && Match(Qlx.LPAREN))
            {
                Next();
                if (Match(Qlx.SELECT))
                {
                    var cs = ParseCursorSpecification(xp).union;
                    left = new SqlValueSelect(cx.GetUid(), cx,
                        (RowSet)(cx.obs[cs] ?? throw new DBException("PE2011")), xp);
                }
                else
                    left = ParseSqlValue(xp);
                Mustbe(Qlx.RPAREN);
            }
            else
                left = ParseSqlValueExpression(xp, wfok);
            return ((QlValue)cx.Add(left));
        }
        QlValue ParseSqlTableValue(Domain xp)
        {
            if (tok == Qlx.LPAREN)
            {
                Next();
                if (tok == Qlx.SELECT)
                {
                    var cs = ParseCursorSpecification(xp).union;
                    Mustbe(Qlx.RPAREN);
                    return (QlValue)cx.Add(new SqlValueSelect(cx.GetUid(), cx,
                        (RowSet)(cx.obs[cs] ?? throw new DBException("PE2012")), xp));
                }
            }
            if (Match(Qlx.SELECT))
                return (QlValue)cx.Add(new SqlValueSelect(cx.GetUid(), cx,
                    (RowSet)(cx.obs[ParseCursorSpecification(xp).union] ?? throw new DBException("PE2013")), xp));
            if (Match(Qlx.VALUES))
            {
                var lp = LexPos();
                Next();
                var v = ParseSqlValueList(xp);
                return (QlValue)cx.Add(new SqlRowArray(lp.dp, cx, xp, v));
            }
            if (Match(Qlx.MATCH,Qlx.WITH))
            {
                ParseMatchStatement();
                var rs = cx.obs[cx.result] as RowSet ?? new TrivialRowSet(cx, Domain.Bool);
                return (QlValue)cx.Add(new SqlValueSelect(cx.GetUid(), cx, rs, xp));
            }
            if (Match(Qlx.TABLE))
                Next();
            return ParseSqlValueItem(xp, false);
        }
        QlValue Disjoin(CTree<long, bool> s) // s is not empty
        {
            var rb = s.Last();
            var rp = rb?.key() ?? -1L;
            var right = (QlValue?)cx.obs[rp] ?? SqlNull.Value;
            for (rb = rb?.Previous(); rb is not null; rb = rb.Previous())
                if (cx.obs[rb.key()] is QlValue lf)
                    right = (QlValue)cx.Add(new SqlValueExpr(cx.GetUid(), cx, Qlx.AND,
                        lf, right, Qlx.NO));
            return (QlValue)cx.Add(right);
        }
        /// <summary>
        /// Parse a possibly boolean expression
        /// </summary>
        /// <param name="xp"></param>
        /// <param name="wfok"></param>
        /// <param name="dm">A select tree to the left of a Having clause, or null</param>
        /// <returns>A disjunction of expressions</returns>
        CTree<long, bool> ParseSqlValueDisjunct(Domain xp, bool wfok, Domain? dm = null)
        {
            var left = ParseSqlValueConjunct(xp, wfok, dm);
            var r = new CTree<long, bool>(left.defpos, true);
            while ((left.domain.kind == Qlx.BOOLEAN|| left.domain.kind==Qlx.Null) && Match(Qlx.AND))
            {
                Next();
                left = ParseSqlValueConjunct(xp, wfok, dm);
                r += (left.defpos, true);
            }
            return r;
        }
        QlValue ParseSqlValueConjunct(Domain xp, bool wfok, Domain? dm)
        {
            var left = ParseSqlValueConjunct(xp, wfok);
            return (dm == null) ? left : left.Having(cx, dm);
        }
        QlValue ParseSqlValueConjunct(Domain xp, bool wfok)
        {
            var left = ParseSqlValueExpression(Domain.Content, wfok);
            if (Match(Qlx.EQL, Qlx.NEQ, Qlx.LSS, Qlx.GTR, Qlx.LEQ, Qlx.GEQ))
            {
                var op = tok;
                var lp = LexPos();
                if (lp.dp == left.defpos)
                    lp = cx.GetIid();
                Next();
                return (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx,
                    op, left, ParseSqlValueExpression(left.domain, wfok), Qlx.NO));
            }
            if (xp.kind != Qlx.CONTENT)
            {
                var nd = left.domain.LimitBy(cx, left.defpos, xp);
                if (nd != left.domain && nd != null)
                    left += (DBObject._Domain, nd);
            }
            return (QlValue)cx.Add(left);
        }
        QlValue ParseSqlValueExpression(Domain xp, bool wfok)
        {
            var left = ParseSqlValueTerm(xp, wfok);
            while ((Domain.UnionDateNumeric.CanTakeValueOf(left.domain)
                || left is SqlReview)
                && Match(Qlx.PLUS, Qlx.MINUS))
            {
                var op = tok;
                var lp = LexPos();
                Next();
                var x = ParseSqlValueTerm(xp, wfok);
                left = (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx, op, left, x, Qlx.NO));
            }
            return (QlValue)cx.Add(left);
        }
        /// <summary>
        /// |   NOT TypedValue
        /// |	Value BinaryOp TypedValue 
        /// |   PeriodPredicate
		/// BinaryOp =	'+' | '-' | '*' | '/' | '||' | MultisetOp | AND | OR | LT | GT | LEQ | GEQ | EQL | NEQ |':'. 
		/// MultisetOp = MULTISET ( UNION | INTERSECT | EXCEPT ) ( ALL | DISTINCT ) .
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
        QlValue ParseSqlValueTerm(Domain xp, bool wfok)
        {
            bool sign = false, not = false;
            var lp = LexPos();
            if (tok == Qlx.PLUS)
                Next();
            else if (tok == Qlx.MINUS)
            {
                Next();
                sign = true;
            }
            else if (tok == Qlx.NOT)
            {
                Next();
                not = true;
            }
            var left = ParseSqlValueFactor(xp, wfok);
            if (sign)
                left = new SqlValueExpr(lp.dp, cx, Qlx.MINUS, null, left, Qlx.NO)
                    .Constrain(cx, Domain.UnionNumeric);
            else if (not)
                left = left.Invert(cx);
            var imm = Qlx.NO;
            if (Match(Qlx.IMMEDIATELY))
            {
                Next();
                imm = Qlx.IMMEDIATELY;
            }
            if (Match(Qlx.CONTAINS, Qlx.OVERLAPS, Qlx.EQUALS, Qlx.PRECEDES, Qlx.SUCCEEDS))
            {
                var op = tok;
                lp = LexPos();
                Next();
                return (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx,
                    op, left, ParseSqlValueFactor(left.domain, wfok), imm));
            }
            while (Match(Qlx.TIMES, Qlx.DIVIDE, Qlx.MULTISET))
            {
                Qlx op = tok;
                lp = LexPos();
                switch (op)
                {
                    case Qlx.TIMES:
                        break;
                    case Qlx.DIVIDE: goto case Qlx.TIMES;
                    case Qlx.MULTISET:
                        {
                            Next();
                            if (Match(Qlx.INTERSECT))
                                op = tok;
                            else
                            {
                                tok = lxr.PushBack(Qlx.MULTISET);
                                return (QlValue)cx.Add(left);
                            }
                        }
                        break;
                }
                Qlx m = Qlx.NO;
                if (Match(Qlx.ALL, Qlx.DISTINCT))
                {
                    m = tok;
                    Next();
                }
                Next();
                if (left.domain.kind == Qlx.TABLE)
                    left += (Domain.Kind, Qlx.CONTENT); // must be scalar
                left = (QlValue)cx.Add(new SqlValueExpr(lp.dp, cx, op, left,
                    ParseSqlValueFactor(left.domain, wfok), m));
            }
            return (QlValue)cx.Add(left);
        }
        /// <summary>
        /// |	Value '||'  TypedValue 
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the QlValue</returns>
		QlValue ParseSqlValueFactor(Domain xp, bool wfok)
        {
            var left = ParseSqlValueEntry(xp, wfok);
            while (Match(Qlx.CONCATENATE, Qlx.COLON))
            {
                Qlx op = tok;
                Next();
                var right = ParseSqlValueEntry(left.domain, wfok);
                left = new SqlValueExpr(LexPos().dp, cx, op, left, right, Qlx.NO);
                cx.Add(left);
            }
            return left;
        }
        /// <summary>
        /// | 	Value '['  TypedValue ']'
        /// |	ColumnRef  
        /// | 	VariableRef 
        /// |   TypedValue
        /// |   PeriodRef | SYSTEM_TIME
        /// |   id ':'  Value
        /// |	Value '.' Member_id
        /// |   Value IN '(' RowSet | (Value {',' Value })')'
        /// |   Value IS [NOT] NULL
        /// |   Value IS [NOT] MEMBER OF Value
        /// |   Value IS [NOT] BETWEEN Value AND Value
        /// |   Value IS [NOT] OF '(' [ONLY] id {',' [ONLY] id } ')'
        /// |   Value IS [NOT] LIKE Value [ESCAPE TypedValue ]
        /// |   Value COLLATE id
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
		QlValue ParseSqlValueEntry(Domain xp, bool wfok)
        {
            var left = ParseSqlValueItem(xp, wfok);
            bool invert = false;
            var lp = LexPos();
            while (tok == Qlx.DOT || tok == Qlx.LBRACK)
                if (tok == Qlx.DOT)
                {
                    // could be table alias, block id, instance id etc
                    Next();
                    if (tok == Qlx.TIMES)
                    {
                        lp = LexPos();
                        Next();
                        return new SqlStar(lp.dp, cx, left.defpos);
                    }
                    var n = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (tok == Qlx.LPAREN)
                    {
                        var ps = BList<long?>.Empty;
                        Next();
                        if (tok != Qlx.RPAREN)
                            ps = ParseSqlValueList(xp);
                        cx.Add(left);
                        var ut = left.domain; // care, the methodInfos may be missing some later methods
                        if (cx.db.objects[ut.defpos] is not Domain u || u.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42105").Add(Qlx.TYPE);
                        var ar = cx.Signature(ps);
                        var pr = cx.db.objects[oi.methodInfos[n.ident]?[ar] ?? -1L] as Method
                            ?? throw new DBException("42173", n);
                        left = new SqlMethodCall(lp.dp, cx, pr, ps, left);
                        Mustbe(Qlx.RPAREN);
                        left = (QlValue)cx.Add(left);
                    }
                    else
                    {
                        var oi = left.infos[cx.role.defpos];
                        if (oi is null || oi.names == BTree<string, (int, long?)>.Empty)
                            oi = left.domain.infos[cx.role.defpos];
                        var cp = oi?.names[n.ident].Item2 ?? -1L;
                        var el = (QlValue)cx.Add(new SqlCopy(n.iix.dp, cx, n.ident, lp.dp, cp));
                        left = (QlValue)cx.Add(new SqlValueExpr(cx.GetUid(), cx, Qlx.DOT, left, el, Qlx.NO));
                    }
                }
                else // tok==Qlx.LBRACK
                {
                    Next();
                    left = new SqlValueExpr(lp.dp, cx, Qlx.LBRACK, left,
                        ParseSqlValue(Domain.Int), Qlx.NO);
                    Mustbe(Qlx.RBRACK);
                }

            if (tok == Qlx.IS)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                bool b = true;
                if (tok == Qlx.NOT)
                {
                    Next();
                    b = false;
                }
                if (tok == Qlx.OF)
                {
                    Next();
                    Mustbe(Qlx.LPAREN);
                    var r = BList<Domain>.Empty;
                    var es = BList<long?>.Empty;
                    var t1 = ParseDataType();
                    lp = LexPos();
                    r += t1;
                    while (tok == Qlx.COMMA)
                    {
                        Next();
                        t1 = ParseDataType();
                        lp = LexPos();
                        r += t1;
                    }
                    Mustbe(Qlx.RPAREN);
                    return (QlValue)cx.Add(new TypePredicate(lp.dp, left, b, r));
                }
                Mustbe(Qlx.NULL);
                return (QlValue)cx.Add(new NullPredicate(lp.dp, left, b));
            }
            var savestart = lxr.start;
            if (tok == Qlx.NOT)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                invert = true;
            }
            if (tok == Qlx.BETWEEN)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                var od = left.domain;
                var lw = ParseSqlValueTerm(od, false);
                Mustbe(Qlx.AND);
                var hi = ParseSqlValueTerm(od, false);
                return (QlValue)cx.Add(new BetweenPredicate(lp.dp, cx, left, !invert, lw, hi));
            }
            if (tok == Qlx.LIKE)
            {
                if (!(xp.CanTakeValueOf(Domain.Bool) &&
                    Domain.Char.CanTakeValueOf(left.domain)))
                    throw new DBException("42000", lxr.pos);
                Next();
                LikePredicate k = new(lp.dp, cx, left, !invert, ParseSqlValue(Domain.Char), null);
                if (Match(Qlx.ESCAPE))
                {
                    Next();
                    k += (cx, LikePredicate.Escape, ParseSqlValueItem(Domain.Char, false)?.defpos ?? -1L);
                }
                return (QlValue)cx.Add(k);
            }
#if SIMILAR
            if (Match(Sqlx.LIKE_REGEX))
            {
                            if (!cx._Domain(tp).CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                SimilarPredicate k = new SimilarPredicate(left, !invert, ParseSqlValue(), null, null);
                if (Match(Sqlx.FLAG))
                {
                    Next();
                    k.flag = ParseSqlValue(-1);
                }
                return (SqlValue)cx.Add(k);
            }
            if (Match(Sqlx.SIMILAR))
            {
                            if (!cx._Domain(tp).CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Sqlx.TO);
                SimilarPredicate k = new SimilarPredicate(left, !invert, ParseSqlValue(), null, null);
                if (Match(Sqlx.ESCAPE))
                {
                    Next();
                    k.escape = ParseSqlValue(Domain.Char.defpos);
                }
                return (SqlValue)cx.Add(k);
            }
#endif
            if (tok == Qlx.IN)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                InPredicate n = new InPredicate(lp.dp, cx, left) +
                    (QuantifiedPredicate.Found, !invert);
                var tp = tok;
                if (tok == Qlx.LPAREN || tok==Qlx.LBRACK)
                {
                    Next();
                    if (Match(Qlx.SELECT, Qlx.TABLE, Qlx.VALUES))
                    {
                        RowSet rs = ParseQuerySpecification(Domain.TableType);
                        cx.Add(rs);
                        n += (cx, QuantifiedPredicate._Select, rs.defpos);
                    }
                    else
                        n += (cx, QuantifiedPredicate.Vals, ParseSqlValueList(left.domain));
                    Mustbe((tp==Qlx.LPAREN)?Qlx.RPAREN:Qlx.RBRACK);
                }
                else
                    n += (cx, SqlFunction._Val, ParseSqlValue(
                        (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.COLLECT, left.domain))).defpos);
                return (QlValue)cx.Add(n);
            }
            if (Match(Qlx.MEMBER))
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Qlx.OF);
                var dm = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.MULTISET, xp));
                return (QlValue)cx.Add(new MemberPredicate(LexPos().dp, cx, left,
                    !invert, ParseSqlValue(dm)));
            }
            if (invert)
            {
                tok = lxr.PushBack(Qlx.NOT);
                lxr.pos = lxr.start - 1;
                lxr.start = savestart;
            }
            else
            if (Match(Qlx.COLLATE))
                left = ParseCollateExpr(left);
            return (QlValue)cx.Add(left);
        }
        /// <summary>
        /// |	Value Collate 
        /// </summary>
        /// <param name="e">The QlValue</param>
        /// <returns>The collated QlValue</returns>
        QlValue ParseCollateExpr(QlValue e)
        {
            Next();
            var o = lxr.val;
            Mustbe(Qlx.Id);
            var ci = new CultureInfo(o.ToString());
            e += (Domain.Culture, ci);
            return (QlValue)cx.Add(e);
        }
        /// <summary>
        ///  Value= [NEW} MethodCall
        /// | 	FunctionCall 
        /// |	VALUES  '('  Value { ','  Value } ')' { ',' '('  Value { ','  Value } ')' }
        /// |	Subquery
        /// |   TypedValue
        /// |   ARRAY Subquery
        /// |	( MULTISET | ARRAY ) '['  Value { ','  Value } ']'
        /// |   ROW '(' Value { ',' Value } ')'
        /// | 	TABLE '('  Value ')' 
        /// |	TREAT '('  Value AS Sub_Type ')'  
        /// |   '[' DocArray ']'
        /// |   '{' Document '}'
        /// |   '$lt;' Xml '$gt;' 
        /// Predicate = Any | At | Between | Comparison | QuantifiedComparison | Current | Every | Exists | In | Like | Member | Null | Of 
        /// | Some | Unique .
        /// Any = ANY '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
        /// At = ColumnRef AT TypedValue .
        /// Between =  TypedValue [NOT] BETWEEN [SYMMETRIC|ASYMMETRIC]  TypedValue AND TypedValue .
        /// Comparison =  TypedValue CompOp TypedValue .
        /// CompOp = '=' | '<>' | '<' | '>' | '<=' | '>=' .
        /// QuantifiedComparison =  TypedValue CompOp (ALL|SOME|ANY) Subquery .
        /// Current = CURRENT '(' ColumnRef ')'.
        /// Current and At can be used on default temporal TableColumns of temporal tables. See section 7.12.
        /// Every = EVERY '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
        /// Exists = EXISTS QueryExpression .
        /// FuncOpt = [FILTER '(' WHERE SearchCondition ')'] [OVER WindowSpec] .
        /// The presence of the OVER keyword makes a window function. In accordance with SQL2003-02 section 4.15.3, window functions can only be used in the select tree of a RowSetSpec or SelectSingle or the order by clause of a “simple table query” as defined in section 7.5 above. Thus window functions cannot be used within expressions or as function arguments.
        /// In =  TypedValue [NOT] IN '(' QueryExpression | (  TypedValue { ','  TypedValue } ) ')' .
        /// Like =  TypedValue [NOT] LIKE string .
        /// Member =  TypedValue [ NOT ] MEMBER OF TypedValue .
        /// Null =  TypedValue IS [NOT] NULL .
        /// Of =  TypedValue IS [NOT] OF '(' [ONLY] Type {','[ONLY] Type } ')' .
        /// Some = SOME '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
        /// Unique = UNIQUE QueryExpression .
        /// VariableRef =	{ Scope_id '.' } Variable_id .
        /// ColumnRef =	[ TableOrAlias_id '.' ]  Column_id 
        /// 	| TableOrAlias_id '.' (ROW | PARTITION | VERSIONING | CHECK) .
		/// FunctionCall = NumericValueFunction | StringValueFunction | DateTimeFunction | SetFunctions | XMLFunction | UserFunctionCall | MethodCall .
		/// NumericValueFunction = AbsolutValue | Avg | Cast | Ceiling | Coalesce | Correlation | Count | Covariance | Exponential | Extract | Floor | Grouping | Last | LengthExpression | Maximum | Minimum | Modulus 
        ///     | NaturalLogarithm | Next | Nullif | Percentile | Position | PowerFunction | Rank | Regression | RowNumber | SquareRoot | StandardDeviation | Sum | Variance | HttpGet .
		/// AbsolutValue = ABS '('  TypedValue ')' .
		/// Avg = AVG '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Cast = CAST '('  TypedValue AS Type ')' .
		/// Ceiling = (CEIL|CEILING) '('  TypedValue ')' .
		/// Coalesce = COALESCE '('  TypedValue {','  TypedValue } ')'
		/// Corelation = CORR '('  TypedValue ','  TypedValue ')' FuncOpt .
		/// Count = COUNT '(' '*' ')'
		/// | COUNT '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Covariance = (COVAR_POP|COVAR_SAMP) '('  TypedValue ','  TypedValue ')' FuncOpt .
        /// Schema = SCHEMA '(' ObjectName ')' . 
		/// WindowSpec = Window_id | '(' WindowDetails ')' .
		/// Exponential = EXP '('  TypedValue ')' .
		/// Extract = EXTRACT '(' ExtractField FROM TypedValue ')' .
		/// ExtractField =  YEAR | MONTH | DAY | HOUR | MINUTE | SECOND.
		/// Floor = FLOOR '('  TypedValue ')' .
		/// Grouping = GROUPING '(' ColumnRef { ',' ColumnRef } ')' .
		/// Last = LAST ['(' ColumnRef ')' OVER WindowSpec ] .
		/// LengthExpression = (CHAR_LENGTH|CHARACTER_LENGTH|OCTET_LENGTH) '('  TypedValue ')' .
		/// Maximum = MAX '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Minimum = MIN '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Modulus = MOD '('  TypedValue ','  TypedValue ')' .
		/// NaturalLogarithm = LN '('  TypedValue ')' .
		/// Next = NEXT ['(' ColumnRef ')' OVER WindowSpec ] .
		/// Nullif = NULLIF '('  TypedValue ','  TypedValue ')' .
		/// Percentile = (PERCENTILE_CONT|PERCENTILE_DISC) '('  TypedValue ')' WithinGroup .
		/// Position = POSITION ['('Value IN TypedValue ')'] .
		/// PowerFunction = POWER '('  TypedValue ','  TypedValue ')' .
		/// Rank = (CUME_DIST|DENSE_RANK|PERCENT_RANK|RANK) '('')' OVER WindowSpec 
		///   | (DENSE_RANK|PERCENT_RANK|RANK|CUME_DIST) '('  TypedValue {','  TypedValue } ')' WithinGroup .
		/// Regression = (REGR_SLOPE|REGR_INTERCEPT|REGR_COUNT|REGR_R2|REGR_AVVGX| REGR_AVGY|REGR_SXX|REGR_SXY|REGR_SYY) '('  TypedValue ','  TypedValue ')' FuncOpt .
		/// RowNumber = ROW_NUMBER '('')' OVER WindowSpec .
		/// SquareRoot = SQRT '('  TypedValue ')' .
		/// StandardDeviation = (STDDEV_POP|STDDEV_SAMP) '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Sum = SUM '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// DateTimeFunction = CURRENT_DATE | CURRENT_TIME | LOCALTIME | CURRENT_TIMESTAMP | LOCALTIMESTAMP .
		/// StringValueFunction = Substring | Fold | Transcode | Transliterate | Trim | Overlay | Normalise | TypeUri | XmlAgg .
		/// Substring = SUBSTRING '('  TypedValue FROM TypedValue [ FOR TypedValue ] ')' .
		/// Fold = (UPPER|LOWER) '('  TypedValue ')' .
		/// Trim = TRIM '(' [[LEADING|TRAILING|BOTH] [character] FROM]  TypedValue ')' .
		/// Variance = (VAR_POP|VAR_SAMP) '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
        /// TypeUri = TYPE_URI '('  TypedValue ')' .
		/// XmlAgg = XMLAGG '('  TypedValue ')' .
		/// SetFunction = Cardinality | Collect | Element | Fusion | Intersect | Set .
		/// Collect = CARDINALITY '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt  .
		/// Fusion = FUSION '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt  .
		/// Intersect = INTERSECT '(' [DISTINCT|ALL]  TypedValue) ')' FuncOpt .
		/// Cardinality = CARDINALITY '('  TypedValue ')' .
		/// Element = ELEMENT '('  TypedValue ')' .
		/// Set = SET '('  TypedValue ')' .
		/// XMLFunction = 	XMLComment | XMLConcat | XMLElement | XMLForest | XMLParse | XMLProc | XMLRoot | XMLAgg | XPath .
		/// XPath is not in the SQL2003 standard but has become popular. See section 5.9.
		/// XMLComment = XMLCOMMENT '('  TypedValue ')' .
		/// XMLConcat = XMLCONCAT '('  TypedValue {','  TypedValue } ')' .
		/// XMLElement = XMLELEMENT '(' NAME id [ ',' Namespace ] [',' AttributeSpec ]{ ','  TypedValue } ')' .
		/// Namespace = XMLNAMESPACES '(' NamespaceDefault |( string AS id {',' string AS id }) ')' .
		/// NamespaceDefault = (DEFAULT string) | (NO DEFAULT) .
		/// AttributeSpec = XMLATTRIBUTES '(' NamedValue {',' NamedValue }')' .
		/// NamedValue =  TypedValue [ AS id ] .
		/// XMLForest = XMLFOREST '(' [ Namespace ','] NamedValue { ',' NamedValue } ')' .
		/// XMLParse = XMLPARSE '(' CONTENT TypedValue ')' .
		/// XMLProc = XMLPI '(' NAME id [','  TypedValue ] ')' .
		/// XMLRoot = XMLROOT '('  TypedValue ',' VERSION (TypedValue | NO VALUE) [','STANDALONE (YES|NO|NO VALUE)] ')' .
		/// NO VALUE is the default for the standalone property.
		/// XPath = XMLQUERY '('  TypedValue ',' xml ')' .
        /// HttpGet = HTTP GET url_Value [AS mime_string] .
        /// Level = LEVEL id ['-' id] GROUPS { id } REFERENCES { id }.
		/// MethodCall = 	Value '.' Method_id  [ '(' [  TypedValue { ','  TypedValue } ] ')']
		/// 	|	'('  TypedValue AS Type ')' '.' Method_id  [ '(' [  TypedValue { ','  TypedValue } ] ')']
		///     |	Type'::' Method_id [ '(' [  TypedValue { ','  TypedValue } ] ')' ] .
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <param name="wfok">whether a window function is allowed</param>
        /// <returns>the ql value</returns>
        internal QlValue ParseSqlValueItem(Domain xp, bool wfok)
        {
            QlValue r;
            var lp = LexPos();
            if (tok == Qlx.QMARK && cx.parse == ExecuteStatus.Prepare)
            {
                Next();
                var qm = new SqlLiteral(lp.dp, new TQParam(Domain.Content, lp));
                cx.qParams += qm.defpos;
                return qm;
            }
            if (Match(Qlx.LEVEL))
            {
                return (QlValue)cx.Add(new SqlLiteral(LexPos().dp, TLevel.New(MustBeLevel())));
            }
            Match(Qlx.SCHEMA, Qlx.LABELS, Qlx.ELEMENTID, Qlx.ID, Qlx.TYPE, // we may get these non-reserved words
                Qlx.TABLE, Qlx.EXTRACT, Qlx.FIRST_VALUE, Qlx.GROUPING, Qlx.HTTP, Qlx.LAST_VALUE, Qlx.LAST_DATA,Qlx.RANK,
    Qlx.CHAR_LENGTH, Qlx.WITHIN, Qlx.POSITION, Qlx.ROW_NUMBER, Qlx.SUBSTRING, Qlx.COLLECT, Qlx.INTERSECTION,
    Qlx.ELEMENT, Qlx.USER, Qlx.VERSIONING, Qlx.VALUES);
            StartStdFunctionRefs();
            if (Match(Qlx.Id, Qlx.FIRST, Qlx.NEXT, Qlx.LAST, Qlx.CHECK, Qlx.TYPE_URI)) // Id or pseudo ident
            {
                QlValue vr = ParseVarOrColumn(xp);
                if (tok == Qlx.DOUBLECOLON)
                {
                    Next();
                    if (vr.name == null || cx.db.objects[cx.role.dbobjects[vr.name] ?? -1L] is not Domain ut
                        || ut.infos[cx.role.defpos] is not ObInfo oi)
                        throw new DBException("42139", vr.name ?? "??").Mix();
                    var name = new Ident(this);
                    Mustbe(Qlx.Id);
                    lp = LexPos();
                    Mustbe(Qlx.LPAREN);
                    var ps = ParseSqlValueList(xp);
                    Mustbe(Qlx.RPAREN);
                    var n = cx.Signature(ps);
                    var m = cx.db.objects[oi.methodInfos[name.ident]?[n] ?? -1L] as Method
                        ?? throw new DBException("42132", name.ident, ut.name).Mix();
                    if (m.methodType != PMethod.MethodType.Static)
                        throw new DBException("42140").Mix();
                    var fc = new SqlMethodCall(lp.dp, cx, m, ps, vr);
                    return (QlValue)cx.Add(fc);
                }
                return (QlValue)cx.Add(vr);
            }
            if (Match(Qlx.EXISTS, Qlx.UNIQUE))
            {
                Qlx op = tok;
                Next();
                Mustbe(Qlx.LPAREN);
                RowSet g = ParseRowSetSpec(Domain.Null);
                Mustbe(Qlx.RPAREN);
                if (op == Qlx.EXISTS)
                    return (QlValue)cx.Add(new ExistsPredicate(LexPos().dp, cx, g));
                else
                    return (QlValue)cx.Add(new UniquePredicate(LexPos().dp, cx, g));
            }
            if (Match(Qlx.RDFLITERAL, Qlx.CHARLITERAL,
                Qlx.INTEGERLITERAL, Qlx.NUMERICLITERAL, Qlx.NULL,
            Qlx.REALLITERAL, Qlx.BLOBLITERAL, Qlx.BOOLEANLITERAL))
            {
                r = new SqlLiteral(LexDp(), lxr.val);
                Next();
                return (QlValue)cx.Add(r);
            }
            // pseudo functions
            StartDataType();
            switch (tok)
            {
                case Qlx.ARRAY:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                        {
                            lp = LexPos();
                            Next();
                            if (tok == Qlx.SELECT)
                            {
                                var st = lxr.start;
                                var cs = ParseCursorSpecification(Domain.Null).union;
                                Mustbe(Qlx.RPAREN);
                                return (QlValue)cx.Add(new SqlValueSelect(lp.dp, cx,
                                    (RowSet)(cx.obs[cs] ?? throw new DBException("42000", "Array")), xp));
                            }
                            throw new DBException("22204");
                        }
                        Mustbe(Qlx.LBRACK);
                        var et = (xp.kind == Qlx.CONTENT) ? xp
                            : xp.elType ?? throw new DBException("42000", lxr.pos);
                        var v = ParseSqlValueList(et);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueArray(lp.dp, cx, xp, v));
                    }
                case Qlx.SCHEMA:
                    {
                        Next();
                        Mustbe(Qlx.LPAREN);
                        var (ob, n) = ParseObjectName();
                        if (Match(Qlx.COLUMN))
                        {
                            Next();
                            var cn = lxr.val;
                            Mustbe(Qlx.Id);
                            if (ob is not Table tb)
                                throw new DBException("42107", n).Mix();
                            if (cx._Dom(tb.defpos) is not Domain ft ||
                                cx.db.objects[ft.ColFor(cx, cn.ToString())] is not DBObject oc)
                                throw new DBException("42112", cn.ToString());
                            ob = oc;
                        }
                        r = new SqlLiteral(lp.dp, new TInt(ob.lastChange));
                        Mustbe(Qlx.RPAREN);
                        return (QlValue)cx.Add(r);
                    }
                case Qlx.CURRENT_DATE:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, Qlx.CURRENT_DATE,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_ROLE:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, Qlx.CURRENT_ROLE,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_TIME:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, Qlx.CURRENT_TIME,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_TIMESTAMP:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, Qlx.CURRENT_TIMESTAMP,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.USER:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, Qlx.USER,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.DATE: // also TIME, TIMESTAMP, INTERVAL
                    {
                        Qlx tk = tok;
                        Next();
                        var o = lxr.val;
                        lp = LexPos();
                        if (tok == Qlx.CHARLITERAL)
                        {
                            Next();
                            return new SqlDateTimeLiteral(lp.dp, cx, Domain.For(tk), o.ToString());
                        }
                        else
                            return (QlValue)cx.Add(new SqlLiteral(lp.dp, o));
                    }
                case Qlx.INTERVAL:
                    {
                        Next();
                        var o = lxr.val;
                        Mustbe(Qlx.CHARLITERAL);
                        Domain di = ParseIntervalType();
                        return (QlValue)cx.Add(new SqlDateTimeLiteral(lp.dp, cx, di, o.ToString()));
                    }
                case Qlx.LPAREN:// subquery
                    {
                        Next();
                        if (tok == Qlx.SELECT)
                        {
                            var st = lxr.start;
                            var cs = ParseCursorSpecification(xp).union;
                            Mustbe(Qlx.RPAREN);
                            return (QlValue)cx.Add(new SqlValueSelect(cx.GetUid(),
                                cx, (RowSet)(cx.obs[cs] ?? throw new PEException("PE2010")), xp));
                        }
                        Domain et = Domain.Null;
                        switch (xp.kind)
                        {
                            case Qlx.ARRAY:
                            case Qlx.MULTISET:
                                et = xp.elType ?? Domain.Null;
                                break;
                            case Qlx.CONTENT:
                                et = Domain.Content;
                                break;
                            case Qlx.ROW:
                                break;
                            default:
                                var v = ParseSqlValue(xp);
                                if (v is SqlLiteral sl)
                                    v = (QlValue)cx.Add(new SqlLiteral(lp.dp, xp.Coerce(cx, sl.val)));
                                Mustbe(Qlx.RPAREN);
                                return v;
                        }
                        var fs = BList<DBObject>.Empty;
                        for (var i = 0; ; i++)
                        {
                            var it = ParseSqlValue(et ??
                                xp.representation[xp[i] ?? -1L] ?? Domain.Content);
                            if (tok == Qlx.AS)
                            {
                                lp = LexPos();
                                Next();
                                var ic = new Ident(this);
                                Mustbe(Qlx.Id);
                                it += (DBObject._Alias, ic.ToString());
                                cx.Add(it);
                            }
                            fs += it;
                            if (tok != Qlx.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Qlx.RPAREN);
                        if (fs.Length == 1 && fs[0] is QlValue w)
                            return (QlValue)cx.Add(w);
                        return (QlValue)cx.Add(new SqlRow(lp.dp, cx, fs));
                    }
                case Qlx.MULTISET:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                            return ParseSqlValue(xp);
                        Mustbe(Qlx.LBRACK);
                        var v = ParseSqlValueList(xp);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueMultiset(lp.dp, cx, xp, v));
                    }
                case Qlx.SET:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                            return ParseSqlValue(xp);
                        Mustbe(Qlx.LBRACK);
                        var v = ParseSqlValueList(xp);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueSet(lp.dp, cx, xp, v));
                    }
                case Qlx.NEW:
                    {
                        Next();
                        var o = new Ident(this);
                        Mustbe(Qlx.Id);
                        lp = LexPos();
                        if (cx.db.objects[cx.role.dbobjects[o.ident] ?? -1L] is not Domain ut
                            || ut.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42142").Mix();
                        Mustbe(Qlx.LPAREN);
                        var ps = ParseSqlValueList(ut);
                        var n = cx.Signature(ps);
                        Mustbe(Qlx.RPAREN);
                        if (cx.db.objects[oi.methodInfos[o.ident]?[n] ?? -1L] is not Method m)
                        {
                            if (ut.Length != 0 && ut.Length != (int)n.Count)
                                throw new DBException("42142").Mix();
                            return (QlValue)cx.Add(new SqlDefaultConstructor(o.iix.dp, cx, ut, ps));
                        }
                        if (m.methodType != PMethod.MethodType.Constructor)
                            throw new DBException("42142").Mix();
                        return (QlValue)cx.Add(new SqlProcedureCall(o.iix.dp, cx, m, ps));
                    }
                case Qlx.ROW:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                        {
                            lp = LexPos();
                            Next();
                            var v = ParseSqlValueList(xp);
                            Mustbe(Qlx.RPAREN);
                            return (QlValue)cx.Add(new SqlRow(lp.dp, cx, xp, v));
                        }
                        throw new DBException("42135", "ROW").Mix();
                    }
                /*       case Qlx.SELECT:
                           {
                               var sc = new SaveContext(trans, ExecuteStatus.Parse);
                               RowSet cs = ParseCursorSpecification(t).stmt as RowSet;
                               sc.Restore(tr);
                               return (QlValue)cx.Add(new SqlValueSelect(cs, t));
                           } */
                case Qlx.TABLE: // allowed by 6.39
                    {
                        Next();
                        var lf = ParseSqlValue(Domain.TableType);
                        return (QlValue)cx.Add(lf);
                    }

                case Qlx.TIME: goto case Qlx.DATE;
                case Qlx.TIMESTAMP: goto case Qlx.DATE;
                case Qlx.TREAT:
                    {
                        Next();
                        Mustbe(Qlx.LPAREN);
                        var v = ParseSqlValue(Domain.Content);
                        Mustbe(Qlx.RPAREN);
                        Mustbe(Qlx.AS);
                        var dt = ParseDataType();
                        return (QlValue)cx.Add(new SqlTreatExpr(lp.dp, v, dt));//.Needs(v);
                    }
                case Qlx.CASE:
                    {
                        Next();
                        QlValue? v = null;
                        Domain cp = Domain.Bool;
                        Domain rd = Domain.Content;
                        if (tok != Qlx.WHEN)
                        {
                            v = ParseSqlValue(xp);
                            cx.Add(v);
                            cp = v.domain;
                        }
                        var cs = BList<(long, long)>.Empty;
                        var wh = BList<long?>.Empty;
                        while (Mustbe(Qlx.WHEN, Qlx.ELSE) == Qlx.WHEN)
                        {
                            var w = ParseSqlValue(cp);
                            cx.Add(w);
                            wh += w.defpos;
                            while (v != null && tok == Qlx.COMMA)
                            {
                                Next();
                                w = ParseSqlValue(cp);
                                cx.Add(w);
                                wh += w.defpos;
                            }
                            Mustbe(Qlx.THEN);
                            var x = ParseSqlValue(xp);
                            cx.Add(x);
                            rd = rd.Constrain(cx, lp.dp, x.domain);
                            for (var b = wh.First(); b != null; b = b.Next())
                                if (b.value() is long p)
                                    cs += (p, x.defpos);
                        }
                        var el = ParseSqlValue(xp);
                        cx.Add(el);
                        Mustbe(Qlx.END);
                        return (QlValue)cx.Add((v == null) ? (QlValue)new SqlCaseSearch(lp.dp, cx, rd, cs, el.defpos)
                            : new SqlCaseSimple(lp.dp, cx, rd, v, cs, el.defpos));
                    }
                case Qlx.VALUE:
                    {
                        Next();
                        QlValue vbl = new(new Ident("VALUE", lp), BList<Ident>.Empty, cx, xp);
                        return (QlValue)cx.Add(vbl);
                    }
                case Qlx.VALUES:
                    {
                        Next();
                        var v = ParseSqlValueList(xp);
                        return (QlValue)cx.Add(new SqlRowArray(lp.dp, cx, xp, v));
                    }
                case Qlx.LBRACE:
                    {
                        var v = BList<DBObject>.Empty;
                        Next();
                        if (tok != Qlx.RBRACE)
                        {
                            var (n, sv) = GetDocItem(lp.dp,xp);
                            v += (Domain)cx.Add(sv + (ObInfo.Name, n));
                        }
                        while (tok == Qlx.COMMA)
                        {
                            Next();
                            var (n, sv) = GetDocItem(lp.dp,xp);
                            v += (Domain)cx.Add(sv + (ObInfo.Name, n));
                        }
                        Mustbe(Qlx.RBRACE);
                        return (QlValue)cx.Add(new SqlRow(cx.GetUid(), cx, v));
                    }
                case Qlx.LBRACK:
                    {
                        if (xp.kind is Qlx.SET)
                        {
                            Next();
                            var v = ParseSqlValueList(xp);
                            if (v.Length == 0)
                                throw new DBException("22103").ISO();
                            Mustbe(Qlx.RBRACK);
                            return (QlValue)cx.Add(new SqlValueSet(lp.dp, cx, xp, v));
                        }
                        return (QlValue)cx.Add(ParseSqlDocArray());
                    }
            }
            // "SQLFUNCTIONS"
            Qlx kind;
            QlValue? val = null;
            QlValue? op1 = null;
            QlValue? op2 = null;
            CTree<long, bool>? filter = null;
            Qlx mod = Qlx.NO;
            WindowSpecification? ws = null;
            Ident? windowName = null;
            lp = LexPos();
            StartStdFunctionRefs();
            switch (tok)
            {
                case Qlx.ABS:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.UnionNumeric);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.ANY: goto case Qlx.COUNT;
                case Qlx.AVG: goto case Qlx.COUNT;
                case Qlx.CARDINALITY:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.Collection);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.CAST:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.Content);
                        Mustbe(Qlx.AS);
                        op1 = (QlValue)cx.Add(new SqlTypeExpr(cx.GetUid(), ParseDataType()));
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.CEIL: goto case Qlx.ABS;
                case Qlx.CEILING: goto case Qlx.ABS;
                case Qlx.CHAR_LENGTH:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.CHARACTER_LENGTH: goto case Qlx.CHAR_LENGTH;
                case Qlx.COALESCE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        op1 = ParseSqlValue(xp);
                        Mustbe(Qlx.COMMA);
                        op2 = ParseSqlValue(xp);
                        while (tok == Qlx.COMMA)
                        {
                            Next();
                            op1 = new SqlCoalesce(LexPos().dp, cx, op1, op2);
                            op2 = ParseSqlValue(xp);
                        }
                        Mustbe(Qlx.RPAREN);
                        return (QlValue)cx.Add(new SqlCoalesce(lp.dp, cx, op1, op2));
                    }
                case Qlx.COLLECT: goto case Qlx.COUNT;
#if OLAP
                case Sqlx.CORR: goto case Sqlx.COVAR_POP;
#endif
                case Qlx.COUNT: // actually a special case: but deal with all ident-arg aggregates here
                    {
                        kind = tok;
                        mod = Qlx.NO; // harmless default value
                        Next();
                        Mustbe(Qlx.LPAREN);
                        if (kind == Qlx.COUNT && tok == Qlx.TIMES)
                        {
                            val = (QlValue)cx.Add(new SqlLiteral(LexPos().dp, new TInt(1L))
                                + (ObInfo.Name, "*"));
                            Next();
                            mod = Qlx.TIMES;
                        }
                        else
                        {
                            if (tok == Qlx.ALL)
                                Next();
                            else if (tok == Qlx.DISTINCT)
                            {
                                mod = tok;
                                Next();
                            }
                            val = ParseSqlValue(Domain.Content);
                        }
                        Mustbe(Qlx.RPAREN);
                        if (tok == Qlx.FILTER)
                        {
                            Next();
                            Mustbe(Qlx.LPAREN);
                            if (tok == Qlx.WHERE)
                                filter = ParseWhereClause();
                            Mustbe(Qlx.RPAREN);
                        }
                        if (Match(Qlx.OVER) && wfok)
                        {
                            Next();
                            if (tok == Qlx.Id)
                            {
                                windowName = new Ident(this);
                                Next();
                            }
                            else
                            {
                                ws = ParseWindowSpecificationDetails();
                                ws += (ObInfo.Name, "U" + DBObject.Uid(cx.GetUid()));
                            }
                        }
                        var m = BTree<long, object>.Empty;
                        if (filter != null && filter != CTree<long, bool>.Empty)
                            m += (SqlFunction.Filter, filter);
                        if (ws != null)
                            m += (SqlFunction.Window, ws.defpos);
                        if (windowName is not null)
                            m += (SqlFunction.WindowId, windowName);
                        var sf = new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, m);
                        return (QlValue)cx.Add(sf);
                    }
#if OLAP
                case Sqlx.COVAR_POP:
                    {
                        QuerySpecification se = cx as QuerySpecification;
                        if (se != null)
                            se.aggregates = true;
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        op1 = ParseSqlValue(tp);
                        Mustbe(Sqlx.COMMA);
                        op2 = ParseSqlValue(tp);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.COVAR_SAMP: goto case Sqlx.COVAR_POP;
                case Sqlx.CUME_DIST: goto case Sqlx.RANK;
#endif
                case Qlx.CURRENT: // OF cursor --- delete positioned and update positioned
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.OF);
                        val = (QlValue?)cx.Get(ParseIdentChain(false).Item1, xp);
                        break;
                    }
#if OLAP
                case Sqlx.DENSE_RANK: goto case Sqlx.RANK;
#endif
                case Qlx.ELEMENT: goto case Qlx.CARDINALITY;
                case Qlx.ELEMENTID:
                    {
                        var ov = lxr.val;
                        kind = tok;
                        Next();
                        if (tok == Qlx.LPAREN)
                            Next();
                        else
                        {
                            tok = lxr.PushBack(Qlx.Id);
                            lxr.val = ov;
                            return (QlValue)cx.Add(ParseVarOrColumn(xp));
                        }
                        val = ParseSqlValue(Domain.Content);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.EVERY: goto case Qlx.COUNT;
                case Qlx.EXP: goto case Qlx.ABS;
                case Qlx.EXTRACT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        mod = tok;
                        Mustbe(Qlx.YEAR, Qlx.MONTH, Qlx.DAY, Qlx.HOUR, Qlx.MINUTE, Qlx.SECOND);
                        Mustbe(Qlx.FROM);
                        val = ParseSqlValue(Domain.UnionDate);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.FLOOR: goto case Qlx.ABS;
                case Qlx.FUSION: goto case Qlx.COUNT;
                case Qlx.GROUPING:
                    {
                        Next();
                        return (QlValue)cx.Add(new ColumnFunction(lp.dp, ParseIDList()));
                    }
                case Qlx.ID: goto case Qlx.ELEMENTID;
                case Qlx.INTERSECT: goto case Qlx.COUNT;
                case Qlx.LABELS: goto case Qlx.ELEMENTID;
                case Qlx.LN: goto case Qlx.ABS;
                case Qlx.LOWER: goto case Qlx.SUBSTRING;
                case Qlx.MAX:
                case Qlx.MIN:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.UnionDateNumeric);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.MOD: goto case Qlx.NULLIF;
                case Qlx.NULLIF:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        op1 = ParseSqlValue(Domain.Content);
                        Mustbe(Qlx.COMMA);
                        op2 = ParseSqlValue(op1.domain);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
#if SIMILAR
                case Sqlx.OCCURRENCES_REGEX:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        var pat = ParseSqlValue(Domain.Char.defpos);
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char.defpos);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue();
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.CHARACTERS, Sqlx.OCTETS))
                        {
                            rep.units = tok;
                            Next();
                        }
                        op2 = rep;
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
#endif
                case Qlx.OCTET_LENGTH: goto case Qlx.CHAR_LENGTH;
                case Qlx.OF:
                    {
                        kind = tok;
                        Next();
                        TChar n;
                        var ns = new TList(Domain.Char); // happens to be suitable
                        while (tok != Qlx.RPAREN)
                        {
                            Next();
                            n = lxr.val as TChar ?? throw new DBException("42000", "OF");
                            ns += n;
                            Mustbe(Qlx.Id);
                        }
                        Mustbe(Qlx.RPAREN);
                        val = (QlValue)cx.Add(new SqlLiteral(cx.GetUid(), ns));
                        break;
                    }
                case Qlx.PARTITION:
                    {
                        kind = tok;
                        Next();
                        break;
                    }
#if OLAP
                case Sqlx.PERCENT_RANK: goto case Sqlx.RANK;
                case Sqlx.PERCENTILE_CONT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        op1 = ParseSqlValue(Domain.UnionNumeric.defpos);
                        Mustbe(Sqlx.RPAREN);
                        WindowSpecification ws = ParseWithinGroupSpecification();
                        window = ws;
                        if (ws.order == null || ws.partition == ws.order.Length)
                            throw new DBException("42128").Mix();
                        var oi = ws.order[ws.partition];
                        val = oi.what;
                        ws.name = tr.local.genuid(0);
                        break;
                    }
                case Sqlx.PERCENTILE_DISC: goto case Sqlx.PERCENTILE_CONT;
#endif
                case Qlx.POSITION:
                    {
                        kind = tok;
                        Next();
                        if (tok == Qlx.LPAREN)
                        {
                            Next();
                            op1 = ParseSqlValue(Domain.Int);
                            Mustbe(Qlx.IN);
                            op2 = ParseSqlValue(Domain.Content);
                            Mustbe(Qlx.RPAREN);
                        }
                        break;
                    }
#if OLAP
                case Sqlx.POSITION_REGEX:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        mod = Sqlx.AFTER;
                        if (Match(Sqlx.START, Sqlx.AFTER))
                            mod = tok;
                        var pat = ParseSqlValue(Domain.Char.defpos);
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char.defpos);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(rt);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.WITH))
                        {
                            Next();
                            rep.with = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.CHARACTERS, Sqlx.OCTETS))
                        {
                            rep.units = tok;
                            Next();
                        }
                        if (Match(Sqlx.OCCURRENCE))
                        {
                            Next();
                            rep.occurrence = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.GROUP))
                        {
                            Next();
                            rep.group = ParseSqlValue(Domain.Char.defpos);
                        }
                        Mustbe(Sqlx.RPAREN);
                        op2 = rep;
                        break;
                    }
#endif
                case Qlx.POWER: goto case Qlx.MOD;
                case Qlx.RANK: goto case Qlx.ROW_NUMBER;
#if OLAP
                case Sqlx.REGR_COUNT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_AVGX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_AVGY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_INTERCEPT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_R2: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SLOPE: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SYY: goto case Sqlx.COVAR_POP;
#endif
                case Qlx.ROW_NUMBER:
                    {
                        kind = tok;
                        Next();
                        lp = LexPos();
                        Mustbe(Qlx.LPAREN);
                        if (tok == Qlx.RPAREN)
                        {
                            Next();
                            Mustbe(Qlx.OVER);
                            if (tok == Qlx.Id)
                            {
                                windowName = new Ident(this);
                                Next();
                            }
                            else
                            {
                                ws = ParseWindowSpecificationDetails();
                                ws += (ObInfo.Name, "U" + cx.db.uid);
                            }
                            var m = BTree<long, object>.Empty;
                            if (filter != null)
                                m += (SqlFunction.Filter, filter);
                            if (ws != null)
                                m += (SqlFunction.Window, ws.defpos);
                            if (windowName != null)
                                m += (SqlFunction.WindowId, windowName);
                            return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, m));
                        }
                        var v = new BList<long?>(cx.Add(ParseSqlValue(xp)).defpos);
                        for (var i = 1; tok == Qlx.COMMA; i++)
                        {
                            Next();
                            v += ParseSqlValue(xp).defpos;
                        }
                        Mustbe(Qlx.RPAREN);
                        val = new SqlRow(LexPos().dp, cx, xp, v);
                        var f = new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, BTree<long, object>.Empty
                            + (SqlFunction.Window, ParseWithinGroupSpecification().defpos)
                            + (SqlFunction.WindowId, "U" + cx.db.uid));
                        return (QlValue)cx.Add(f);
                    }
                case Qlx.ROWS: // Pyrrho (what is this?)
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        if (Match(Qlx.TIMES))
                        {
                            mod = Qlx.TIMES;
                            Next();
                        }
                        else
                            val = ParseSqlValue(Domain.Int);
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.SOME: goto case Qlx.COUNT;
                case Qlx.DESCRIBE:
                case Qlx.SPECIFICTYPE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        Mustbe(Qlx.RPAREN);
                        return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, null, null, null, Qlx.NO));
                    }
                case Qlx.SQRT: goto case Qlx.ABS;
                case Qlx.STDDEV_POP: goto case Qlx.COUNT;
                case Qlx.STDDEV_SAMP: goto case Qlx.COUNT;
                case Qlx.SUBSTRING:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        if (kind == Qlx.SUBSTRING)
                        {
#if SIMILAR
                            if (Match(Sqlx.SIMILAR))
                            {
                                mod = Sqlx.REGULAR_EXPRESSION;
                                Next();
                                var re = ParseSqlValue();
                                Mustbe(Sqlx.ESCAPE);
                                op1 = new RegularExpression(cx, re, null, ParseSqlValue());
                            }
                            else
#endif
                            {
                                Mustbe(Qlx.FROM);
                                op1 = ParseSqlValue(Domain.Int);
                                if (tok == Qlx.FOR)
                                {
                                    Next();
                                    op2 = ParseSqlValue(Domain.Int);
                                }
                            }
                        }
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
#if SIMILAR
                case Sqlx.SUBSTRING_REGEX:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        var pat = ParseSqlValue(Domain.Char.defpos);
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char.defpos);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(Domain.Char.defpos);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.CHARACTERS,Sqlx.OCTETS))
                        {
                            rep.units = tok;
                            Next();
                        }
                        if (Match(Sqlx.OCCURRENCE))
                        {
                            Next();
                            rep.occurrence = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.GROUP))
                        {
                            Next();
                            rep.group = ParseSqlValue(Domain.Int.defpos);
                        }
                        op2 = rep;
                        break;
                    }
#endif
                case Qlx.SUM: goto case Qlx.COUNT;
#if SIMILAR
                case Sqlx.TRANSLATE_REGEX:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        var pat = ParseSqlValue();
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char.defpos);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(t);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.WITH))
                        {
                            Next();
                            rep.with = ParseSqlValueDomain.Char.defpos();
                        }
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int.defpos);
                        }
                        if (Match(Sqlx.OCCURRENCE))
                        {
                            Next();
                            if (tok == Sqlx.ALL)
                            {
                                Next();
                                rep.all = true;
                            }
                            else
                                rep.occurrence = ParseSqlValue(Domain.Int.defpos);
                        }
                        op2 = rep;
                        break;
                    }
#endif
                case Qlx.TRIM:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        if (Match(Qlx.LEADING, Qlx.TRAILING, Qlx.BOTH))
                        {
                            mod = tok;
                            Next();
                        }
                        val = ParseSqlValue(Domain.Char);
                        if (tok == Qlx.FROM)
                        {
                            Next();
                            op1 = val; // trim character
                            val = ParseSqlValue(Domain.Char);
                        }
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.TYPE: goto case Qlx.ELEMENTID;
                case Qlx.UPPER: goto case Qlx.SUBSTRING;
#if OLAP
                case Sqlx.VAR_POP: goto case Sqlx.COUNT;
                case Sqlx.VAR_SAMP: goto case Sqlx.COUNT;
#endif
                case Qlx.VERSIONING:
                    kind = tok;
                    Next();
                    break;
                default:
                    {
                        var fc = (CallStatement)ParseProcedureCall();
                        return (SqlProcedureCall)(cx.obs[fc.call] ?? throw new DBException("42000", "Call"));
                    }
            }
            return (QlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod));
        }

        /// <summary>
        /// WithinGroup = WITHIN GROUP '(' OrderByClause ')' .
        /// </summary>
        /// <returns>A WindowSpecification</returns>
        WindowSpecification ParseWithinGroupSpecification()
        {
            WindowSpecification r = new(LexDp());
            Mustbe(Qlx.WITHIN);
            Mustbe(Qlx.GROUP);
            Mustbe(Qlx.LPAREN);
            if (r.order != Domain.Row)
                r += (cx, WindowSpecification.Order, ParseOrderClause(r.order));
            Mustbe(Qlx.RPAREN);
            return r;
        }
        /// <summary>
		/// WindowDetails = [Window_id] [ PartitionClause] [ OrderByClause ] [ WindowFrame ] .
		/// PartitionClause =  PARTITION BY  OrdinaryGroup .
		/// WindowFrame = (ROWS|RANGE) (WindowStart|WindowBetween) [ Exclusion ] .
		/// WindowStart = ((TypedValue | UNBOUNDED) PRECEDING) | (CURRENT ROW) .
		/// WindowBetween = BETWEEN WindowBound AND WindowBound .
        /// </summary>
        /// <returns>The WindowSpecification</returns>
		WindowSpecification ParseWindowSpecificationDetails()
        {
            Mustbe(Qlx.LPAREN);
            WindowSpecification w = new(LexDp());
            Match(Qlx.PARTITION,Qlx.ROWS,Qlx.RANGE);
            if (tok == Qlx.Id)
            {
                w += (WindowSpecification.OrderWindow, lxr.val.ToString());
                Next();
            }
            var dm = Domain.Row;
            if (Match(Qlx.PARTITION))
            {
                Next();
                Mustbe(Qlx.BY);
                dm = (Domain)dm.Relocate(cx.GetUid());
                var rs = dm.representation;
                var rt = dm.rowType;
                var d = 1;
                for (var b = ParseSqlValueList(Domain.Content).First(); b != null; b = b.Next())
                    if (b.value() is long p && cx._Dom(p) is Domain dp)
                    {
                        rt += p; rs += (p, dp);
                        d = Math.Max(d, dp.depth + 1);
                    }
                dm = dm + (Domain.RowType, rt) + (Domain.Representation, rs) + (DBObject._Depth, d);
                w += (cx, WindowSpecification.PartitionType, dm);
            }
            if (tok == Qlx.ORDER)
            {
                var oi = ParseOrderClause(dm);
                oi = (Domain)cx.Add(oi);
                w += (cx, WindowSpecification.Order, oi);
            }
            if (Match(Qlx.ROWS, Qlx.RANGE))
            {
                w += (WindowSpecification.Units, tok);
                Next();
                if (tok == Qlx.BETWEEN)
                {
                    Next();
                    w += (WindowSpecification.Low, ParseWindowBound());
                    Mustbe(Qlx.AND);
                    w += (WindowSpecification.High, ParseWindowBound());
                }
                else
                    w += (WindowSpecification.Low, ParseWindowBound());
                if (Match(Qlx.EXCLUDE))
                {
                    Next();
                    if (Match(Qlx.CURRENT))
                    {
                        w += (WindowSpecification.Exclude, tok);
                        Next();
                        Mustbe(Qlx.ROW);
                    }
                    else if (Match(Qlx.TIES))
                    {
                        w += (WindowSpecification.Exclude, Qlx.EQL);
                        Next();
                    }
                    else if (Match(Qlx.NO))
                    {
                        Next();
                        Mustbe(Qlx.OTHERS);
                    }
                    else
                    {
                        w += (WindowSpecification.Exclude, tok);
                        Mustbe(Qlx.GROUP);
                    }
                }
            }
            Mustbe(Qlx.RPAREN);
            cx.Add(w);
            return w;
        }
        /// <summary>
		/// WindowBound = WindowStart | ((TypedValue | UNBOUNDED) FOLLOWING ) .
        /// </summary>
        /// <returns>The WindowBound</returns>
        WindowBound ParseWindowBound()
        {
            bool prec = false, unbd = true;
            TypedValue d = TNull.Value;
            if (Match(Qlx.CURRENT))
            {
                Next();
                Mustbe(Qlx.ROW);
                return new WindowBound();
            }
            if (Match(Qlx.UNBOUNDED))
                Next();
            else if (tok == Qlx.INTERVAL)
            {
                Next();
                var o = lxr.val;
                var lp = LexPos();
                Mustbe(Qlx.CHAR);
                Domain di = ParseIntervalType();
                d = di.Parse(new Scanner(lp.dp, o.ToString().ToCharArray(), 0, cx));
                unbd = false;
            }
            else
            {
                d = lxr.val;
                Mustbe(Qlx.INTEGERLITERAL, Qlx.NUMERICLITERAL);
                unbd = false;
            }
            if (Match(Qlx.PRECEDING))
            {
                Next();
                prec = true;
            }
            else
                Mustbe(Qlx.FOLLOWING);
            if (unbd)
                return new WindowBound() + (WindowBound.Preceding, prec);
            return new WindowBound() + (WindowBound.Preceding, prec) + (WindowBound.Distance, d);
        }
        /// <summary>
        /// For the REST service there may be an explicit procedure call
        /// </summary>
        /// <param name="sql">a call statement to parse</param>
        /// <returns>the CallStatement</returns>
        internal CallStatement ParseProcedureCall(string sql)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length); // not really needed here
            var n = new Ident(this);
            Mustbe(Qlx.Id);
            var ps = BList<long?>.Empty;
            var lp = LexPos();
            if (tok == Qlx.LPAREN)
            {
                Next();
                ps = ParseSqlValueList(Domain.Content);
            }
            var arity = cx.Signature(ps);
            Mustbe(Qlx.RPAREN);
            var pp = cx.role.procedures[n.ident]?[arity] ?? -1;
            var pr = cx.db.objects[pp] as Procedure
                ?? throw new DBException("42108", n).Mix();
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (CallStatement)cx.Add(new CallStatement(lp.dp, fc));
        }
        /// <summary>
		/// UserFunctionCall = Id '(' [  TypedValue {','  TypedValue}] ')' .
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the Executable</returns>
        Executable ParseProcedureCall()
        {
            var id = new Ident(this);
            Mustbe(Qlx.Id);
            Mustbe(Qlx.LPAREN);
            var ps = ParseSqlValueList(Domain.Content);
            var a = cx.Signature(ps);
            Mustbe(Qlx.RPAREN);
            if (cx.role.procedures[id.ident]?[a] is not long pp ||
                cx.db.objects[pp] is not Procedure pr)
                throw new DBException("42108", id.ident).Mix();
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (Executable)cx.Add(new CallStatement(id.iix.dp, fc));
        }
        /// <summary>
        /// Parse a tree of Sql values
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the List of QlValue</returns>
        BList<long?> ParseSqlValueList(Domain xp)
        {
            var r = BList<long?>.Empty;
            Domain ei;
            switch (xp.kind)
            {
                case Qlx.ARRAY:
                case Qlx.SET:
                case Qlx.MULTISET:
                    ei = xp.elType ?? throw new PEException("PE50710");
                    break;
                case Qlx.CONTENT:
                    for (; ; )
                    {
                        var v = ParseSqlValue(xp);
                        cx.Add(v);
                        r += v.defpos;
                        if (tok == Qlx.COMMA)
                            Next();
                        else break;
                    }
                    return r;
                default:
                    ei = xp;
                    break;
            }
            for (; ; )
            {
                var v = (ei.Length > 0) ?
                    ParseSqlRow(ei) :
                    ParseSqlValue(ei);
                cx.Add(v);
                if (tok == Qlx.AS)
                {
                    Next();
                    var d = ParseDataType();
                    v = new SqlTreatExpr(LexPos().dp, v, d); //.Needs(v);
                    cx.Add(v);
                }
                r += v.defpos;
                if (tok == Qlx.COMMA)
                    Next();
                else
                    break;
            }
            return r;
        }
        public SqlRow ParseSqlRow(Domain xp)
        {
            var llp = LexPos();
            Mustbe(Qlx.LPAREN);
            var lk = BList<long?>.Empty;
            var i = 0;
            for (var b = xp.rowType.First(); b != null && i < xp.display; b = b.Next(), i++)
                if (b.value() is long p && xp.representation[p] is Domain dt)
                {
                    if (i > 0)
                        Mustbe(Qlx.COMMA);
                    var v = ParseSqlValue(dt);
                    cx.Add(v);
                    lk += v.defpos;
                }
            Mustbe(Qlx.RPAREN);
            return (SqlRow)cx.Add(new SqlRow(llp.dp, cx, xp, lk));
        }
        /// <summary>
        /// Parse an SqlRow
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="sql">The string to parse</param>
        /// <param name="result">the expected obs type</param>
        /// <returns>the SqlRow</returns>
        public QlValue ParseSqlValueList(string sql, Domain xp)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (tok == Qlx.LPAREN)
                return ParseSqlRow(xp);
            return ParseSqlValueEntry(xp, false);
        }
        /// <summary>
        /// Get a document item for ParseGraphExp
        /// </summary>
        /// <param name="lb">The label for the item</param>
        /// <returns>(ColRef,Value)</returns>
        (Ident, QlValue) GetDocItem(long pa,Domain lb,bool sch=false)
        {
            Ident k = new(this);
            var ns = lb.infos[cx.role.defpos]?.names??BTree<string,(int,long?)>.Empty;
            if (lxr.caseSensitive && k.ident == "id")
                k = new Ident("ID", k.iix);
            if (!cx.defs.Contains(k.ident))
            {
                cx.locals += (k.iix.dp, true);
                cx.defs += (k, k.iix);
            }
            var ip = lxr.Position;
            Mustbe(Qlx.Id);
            lxr.docValue = true;
            Mustbe(Qlx.COLON,Qlx.DOUBLECOLON,Qlx.TYPED); // GQL extra options
            if (sch)
                return (k, (QlValue)cx.Add(new SqlLiteral(k.iix.dp, ParseDataType())));
            Ident q = new(this);
            if (tok == Qlx.Id && !cx.defs.Contains(q.ident))
            {
                cx.locals += (q.iix.dp, true);
                cx.defs += (q, q.iix);
            }
            lxr.docValue = false;
            var kc = new QlValue(k, BList<Ident>.Empty, cx, Domain.Char);
            var eq = (lxr.val is TChar ec) ? ec.value : q.ident;
            if (lxr.caseSensitive && eq == "localDateTime")
            {
                Next();
                Mustbe(Qlx.LPAREN);
                Ident tv = new(this);
                if (Domain.Timestamp.Parse(0, tv.ident, cx) is not TDateTime v)
                    throw new DBException("42161", "Timestamp");
                Next();
                Mustbe(Qlx.RPAREN);
                return (k,(QlValue)cx.Add(new SqlLiteral(q.iix.dp, v)));
            }
            if (lxr.caseSensitive && eq == "date")
            {
                Next();
                Mustbe(Qlx.LPAREN);
                Ident tv = new(this);
                if (Domain.Date.Parse(0, tv.ident, cx) is not TDateTime v)
                    throw new DBException("42161", "Date");
                Next();
                Mustbe(Qlx.RPAREN);
                return (k,(QlValue)cx.Add(new SqlLiteral(q.iix.dp, v)));
            }
            if (lxr.caseSensitive && eq == "toInteger")
            {
                Next();
                Mustbe(Qlx.LPAREN);
                if (lxr.val is not TInt v)
                    throw new DBException("42161", "Integer");
                Next();
                Mustbe(Qlx.RPAREN);
                return (k,(QlValue)cx.Add(new SqlLiteral(q.iix.dp, v)));
            }
            if (tok == Qlx.Id && !cx.defs.Contains(eq))
            {
                var vv = new SqlField(q.iix.dp,q.ident,-1,-1L,kc.domain,pa);
                cx.Add(vv);
                cx.defs+=(q.ident, q.iix, Ident.Idents.Empty);
                lxr.tgs += (q.iix.dp, new TGParam(q.iix.dp,q.ident,kc.domain,lxr.tgg|TGParam.Type.Value,pa));
            }
            var dm = lb.representation[ns[k.ident].Item2??-1L]??Domain.Content;
      //      if (lxr.tgg.HasFlag(TGParam.Type.Group))
      //          dm = new Domain(-1L, Qlx.ARRAY, dm);
            var va = ParseSqlValue(dm);
            return (k,(QlValue)cx.Add(va));
        }
        /// <summary>
        /// Parse a document array
        /// </summary>
        /// <returns>the SqlDocArray</returns>
        public QlValue ParseSqlDocArray()
        {
            var dp = LexPos().dp;
            var v = new SqlRowArray(dp, BTree<long, object>.Empty);
            Next();
            if (tok != Qlx.RBRACK)
            {
                if (tok != Qlx.LPAREN)
                {
                    var ls = ParseSqlValueList(Domain.Content);
                    Mustbe(Qlx.RBRACK);
                    var xp = Domain.Content;
                    if (ls != BList<long?>.Empty && cx.obs[ls[0] ?? -1L] is QlValue vl)
                        xp = new Domain(-1L, Qlx.ARRAY, vl.domain);
                    return (QlValue)cx.Add(new SqlValueArray(dp, cx, xp, ls));
                }
                v += ParseSqlRow(Domain.Content);
            }
            while (tok == Qlx.COMMA)
            {
                Next();
                v += ParseSqlRow(Domain.Content);
            }
            Mustbe(Qlx.RBRACK);
            return (QlValue)cx.Add(v);
        }
    }
 }