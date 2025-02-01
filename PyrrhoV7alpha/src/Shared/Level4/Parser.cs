using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
using System.Data.SqlTypes;
using System.Globalization;
using System.Net.Sockets;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
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
    /// Some constructs get parsed during database Load(): these should never try to change the rowType
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
            lxr = new Lexer(cx, new Ident(src, cx.db.nextStmt));
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
        internal long LexDp()
        {
            return (cx.parse.HasFlag(ExecuteStatus.Graph) || cx.parse.HasFlag(ExecuteStatus.GraphType)
                || cx.parse.HasFlag(ExecuteStatus.Obey)) ? lxr.Position
                : (cx.parse.HasFlag(ExecuteStatus.Prepare)) ? cx.nextHeap++
                : cx.GetUid();
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
        /// The tye of database modification that may occur is determined by db.parse.
        /// </summary>
        /// <param name="sql">the input</param>
        /// <param name="xp">the expected valueType tye (default is Domain.Content)</param>
        /// <returns>The modified Database and the new uid highwatermark </returns>
        public Database ParseSql(string sql, Domain xp)
        {
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
            if (PyrrhoStart.ShowPlan)
                Console.WriteLine(sql);
            // first check for a JSON procedure call
            if (sql.StartsWith('{'))
                return ParseJSONCall(sql);
            // Normal SQL client command
            lxr = new Lexer(cx, sql, cx.db.lexeroffset);
            Executable e = EmptyStatement.Empty;
            tok = lxr.tok;
            while (StartStatement())
            {
                e = ParseStatementList();
                for (var b = cx.forReview.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue k)
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is RowSet rs)
                                rs.Apply(new BTree<long, object>(RowSet._Where, new CTree<long, bool>(k.defpos, true)),
                                    cx);
            }
            if (cx.parse.HasFlag(ExecuteStatus.Obey))
            {
                var ac = new Activation(cx, "");
                var na = e._Obey(ac, null);
                if (na == ac && ac.signal != null)
                    ac.signal.Throw(ac);
                cx.obs = na.obs;
                cx = na.SlideDown();
            }
            if (tok == Qlx.SEMICOLON) // tolerate a final ;
                Next();
            if (tok != Qlx.EOF)
            {
                string ctx = new(lxr.input, lxr.start, lxr.pos - lxr.start);
                throw new DBException("42000", ctx).ISO();
            }
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return cx.db;
        }
        Database ParseJSONCall(string jd)
        {
            var d = new TDocument(jd);
            if (d[0] is TDocument ps && ps.First() is ABookmark<int,(string,TypedValue)> bm)
            {
                var (pn, pr) = bm.value();
                if (pr is TDocument pd && cx.role.procedures[pn] is BTree<CList<Domain>, long?> bp)
                {
                    for (var b = bp.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.value() ?? -1L] is Procedure proc)
                        {
                            var qr = CList<long>.Empty;
                            var oc = cx.values;
                            var ac = new Activation(cx, proc.name ?? DBObject.Uid(proc.defpos));
                            for (var c = proc.ins.First(); c != null; c = c.Next())
                                if (proc.framing.obs[c.key()] is FormalParameter fp
                                        && fp.name is string fn && pd[fn] is TypedValue v)
                                    qr += ac.Add(new SqlLiteral(cx.GetUid(), v)).defpos;
                                else goto tryanother;
                            var r = proc.Exec(ac, qr).db;
                            cx.values = oc;
                            return r;
                                tryanother:;
                        }
                    throw new DBException("42108", pn);
                }
            }
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
                ParseStatement();
                //      cx.valueType = e.defpos;
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
            cx.binding = CTree<long, TypedValue>.Empty;
            cx.result = null;
            cx.Add(pre);
            cx.Add(pre.framing);
            lxr = new Lexer(cx, s, cx.db.lexeroffset, true);
            tok = lxr.tok;
            var b = cx.FixLl(pre.qMarks).First();
            for (; b != null && tok != Qlx.EOF; b = b.Next())
                if (b.value() is long p)
                {
                    var v = lxr.val;
                    var lp = LexDp();
                    if (Match(Qlx.DATE, Qlx.TIME, Qlx.TIMESTAMP, Qlx.INTERVAL))
                    {
                        Qlx tk = tok;
                        Next();
                        v = lxr.val;
                        if (tok == Qlx.CHARLITERAL)
                        {
                            Next();
                            v = new SqlDateTimeLiteral(lp, cx,
                                new Domain(lp, tk, BTree<long, object>.Empty), v.ToString()).Eval(cx);
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
        /// <param name="xp">A Domain or ObInfo for the expected valueType of the Executable</param>
        /// <param name="procbody">If true, we are parsing a routine body (SQl-style RETURN)</param>
        /// <returns>The Executable results of the Parse</returns>
        public Executable ParseStatement(params (long, object)[] m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Null;
            var lp = LexDp();
            if (StartStatement())
                switch (tok)
                {
                    case Qlx.ALTER: ParseAlter(); return Executable.None;
                    case Qlx.AT:
                        {
                            Next();
                            var sn = new Ident(this);
                            Mustbe(Qlx.Id);
                            var sc = cx.db.objects[cx.role.schemas[sn.ident] ?? -1L] as Schema
                                ?? throw new DBException("42107", sn.ident);
                            var sa = ParseStatement();
                            return (Executable)cx.Add(sa + (Executable.ValueType, sc));
                        }
                    case Qlx.BEGIN: return ParseNestedStatement(m);
                    case Qlx.BINDING:
                        {
                            Next();
                            ParseBindingVariableDefinitions();
                            return ParseStatement();
                        }
                    case Qlx.BREAK: return ParseBreakLeave();
                    case Qlx.CALL: return ParseCallStatement(m); //return Executable.None;  // some GQL TBD
                    case Qlx.CASE: return ParseCaseStatement(m);
                    case Qlx.CLOSE: return ParseCloseStatement();
                    case Qlx.COMMIT:
                        Next();
                        if (Match(Qlx.WORK))
                            Next();
                        if (cx.parse.HasFlag(ExecuteStatus.Obey))
                            cx.db.Commit(cx);
                        else
                            throw new DBException("2D000", "Commit");
                        break;
                    case Qlx.CREATE: return ParseCreateClause(); // some GQL TBD
                    case Qlx.DECLARE: return ParseDeclaration();  // might be for a handler
                    case Qlx.DELETE: return ParseSqlDelete();
                    case Qlx.DETACH: goto case Qlx.DELETE;
                    case Qlx.DROP: ParseDropStatement(); return Executable.None; // some GQL TBD
                    case Qlx.EOF: return new Executable(LexDp());
                    case Qlx.EXCEPT: return ParseCompositeQueryStatement();
                    case Qlx.FETCH: Next();
                        if (Match(Qlx.FIRST))
                            goto case Qlx.ORDER;
                        return ParseFetchStatement();
                    case Qlx.FILTER: return ParseFilter();
                    case Qlx.FINISH: return ParseReturn();
                    case Qlx.FOR: return ParseForStatement();
                    case Qlx.GET: return ParseGetDiagnosticsStatement();
                    case Qlx.GRANT: return ParseGrant();
                    case Qlx.GRAPH: // TBD
                    case Qlx.Id: return ParseLabelledStatement(m);
                    case Qlx.IF: return ParseIfThenElseStmt(m);
                    case Qlx.INSERT: return ParseSqlInsert();
                    case Qlx.INTERSECT: return ParseCompositeQueryStatement(m);
                    case Qlx.ITERATE: return ParseIterate();
                    case Qlx.LBRACE: goto case Qlx.BEGIN;
                    case Qlx.LEAVE: return ParseBreakLeave();
                    case Qlx.LET: return ParseLet();
                    case Qlx.LIMIT: goto case Qlx.ORDER;
                    case Qlx.LOOP: return ParseLoopStatement();
                    case Qlx.MATCH: return ParseMatchStatement();
                    case Qlx.NODETACH: goto case Qlx.DELETE;
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
                    case Qlx.REPEAT: return ParseRepeat(m);
                    case Qlx.REMOVE: goto case Qlx.DELETE; // some GQL TBD
                    case Qlx.RESIGNAL: return ParseSignal();
                    case Qlx.RETURN:
                        if (cx.bindings.Count==0 && mm[Procedure.ProcBody] is bool b && b == true)
                            return ParseReturn(m); 
                        if (cx.bindings.Count > 0)
                            cx.anames += cx.names;
                        return ParseSelectStatement(m);
                    case Qlx.REVOKE: return ParseRevoke();
                    case Qlx.ROLLBACK:
                        Next();
                        if (Match(Qlx.WORK))
                            Next();
                        var e = new RollbackStatement(LexDp());
                        cx.exec = e;
                        if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction)
                            cx = new Context(cx.db.Rollback(), cx.conn);
                        else
                            cx.Add(e);
                        cx.exec = e;
                        return e;
                    case Qlx.SELECT:
                        return ParseSelectStatement(m); // single TBD
                    case Qlx.SET:
                        return ParseAssignment();  // and see ParseSqlSet, some GQL TBD
                    case Qlx.SIGNAL: return ParseSignal();
                    case Qlx.SKIP: goto case Qlx.ORDER;
                    //GQL case Qlx.START
                    case Qlx.TABLE:
                        {
                            Next();
                            var tn = new Ident(this);
                            Mustbe(Qlx.Id);
                            var tb = cx.db.objects[cx.role.dbobjects[tn.ident] ?? -1L] as Table
                                ?? throw new DBException("42107", tn);
                            return (Executable)cx.Add(new QueryStatement(lp,
                                new BTree<long, object>(QueryStatement.Result, tb.RowSets(tn, cx, tb, -1L, 0L).defpos)));
                        }
                    case Qlx.UNION: return ParseCompositeQueryStatement(m);
                    case Qlx.UPDATE: (cx, var ue) = ParseSqlUpdate(); return ue;
                    case Qlx.USE:
                        {
                            Next();
                            var sn = new Ident(this);
                            Mustbe(Qlx.Id);
                            var gg = cx.db.objects[(sn.ident == "HOME_GRAPH") ? cx.role.home_graph
                                : cx.role.graphs[sn.ident] ?? -1L] as Graph
                                ?? throw new DBException("42107", sn.ident);
                            var sa = ParseStatement((DBObject._Domain, xp));
                            return (Executable)cx.Add(sa + (Executable.UseGraph, gg));
                        }
                    case Qlx.VALUE: goto case Qlx.BINDING;
                    case Qlx.VALUES: return ParseSelectStatement(m);
                    case Qlx.WHILE: return ParseSqlWhile();
                    case Qlx.WHEN: return ParseConditionalStmt(m);
                    case Qlx.WHERE: goto case Qlx.FILTER;
                    case Qlx.WITH: goto case Qlx.MATCH; // wow
                }
            throw new DBException("42000");
        }
        bool StartStatement()
        {
            return Match(Qlx.ALTER, Qlx.AT, Qlx.BEGIN, Qlx.BINDING, Qlx.BREAK, Qlx.CALL, 
                Qlx.CASE, Qlx.CREATE, Qlx.CLOSE, Qlx.COMMIT, Qlx.CREATE, Qlx.DECLARE, 
                Qlx.DELETE, Qlx.DETACH,Qlx.DROP, Qlx.EXCEPT,
                Qlx.FETCH, Qlx.FILTER, Qlx.FINISH, Qlx.FOR,
                Qlx.GET, Qlx.GRANT, Qlx.GRAPH, Qlx.IF, Qlx.INSERT, Qlx.INTERSECT, Qlx.ITERATE, 
                Qlx.LBRACE, Qlx.LEAVE, Qlx.LET,  Qlx.LIMIT, Qlx.LOOP, Qlx.MATCH, Qlx.NODETACH,
                Qlx.OFFSET, Qlx.OPEN, Qlx.OPTIONAL, Qlx.ORDER, Qlx.PROPERTY, Qlx.REPEAT,
                Qlx.REMOVE, Qlx.RESIGNAL, Qlx.RETURN, Qlx.REVOKE, Qlx.ROLLBACK, Qlx.SELECT,
                Qlx.SET, Qlx.SIGNAL, Qlx.SKIP, Qlx.TABLE, Qlx.UNION, Qlx.UPDATE, Qlx.USE, Qlx.VALUE,
                Qlx.VALUES, Qlx.WHEN, Qlx.WHERE, Qlx.WHILE, Qlx.WITH) || Match(Qlx.Id);
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
                if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
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
                if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
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
                if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
                    tr.AccessObject(cx, true, priv, pob, grantees, opt);
            }
            else
            {
                var roles = ParseRoleNameList();
                Mustbe(Qlx.TO);
                var grantees = ParseGranteeList(new BList<PrivNames>(new PrivNames(Qlx.USAGE)));
                bool opt = ParseAdminOption();
                if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
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
                        ob = cx.GetProcedure(LexDp(), n, a) ??
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
        /// <returns>the Executable valueType of the parse</returns>
		Executable ParseCallStatement(params (long, object)[] m)
        {
            var lp = lxr.start;
            Next();
            Executable e = ParseProcedureCall(m);
            if (cx.parse.HasFlag(ExecuteStatus.Obey|ExecuteStatus.Http) && cx.db is Transaction tr)
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
                if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
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
            } else  if (cx.parse.HasFlag(ExecuteStatus.Obey))
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
            var gr = cx._Ob(oi.names[nm]) as Graph;
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
                cx.parse |= ExecuteStatus.GraphType;
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
            }
            if (cx.parse.HasFlag(ExecuteStatus.Obey))
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
            var gt = cx._Ob(oi.names[nm]) as GraphType;
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
                cx.parse |= ExecuteStatus.GraphType;
                var cs = (GraphInsertStatement)ParseInsertGraph();
                cs._Obey(cx);
                ts = cs.GraphTypes(cx);
                cx.parse = op;
            }
            else 
                ts = ParseNestedGraphTypeSpecification(ts);
            if (cx.parse.HasFlag(ExecuteStatus.Obey))
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
        /// We are parsing left to right and this is tricky for the ElementTypeSpecification syntax! All of
        /// the helpful keywords are optional (although subject to complex restrictions),
        /// so they only serve to make the code here more confusing.
        /// We also allow alternative node tye references and multiplicity metadata.
        /// ElementTypeSpecification = 
        ///   [(NODE|VERTEX) [TYPE] id] * ElementDetails 
        /// | [(DIRECTED | UNDIRECTED) (EDGE|RELATIONSHIP) [TYPE] id] ( * ElementDetails ) ArcType ( ElementDetails )
        /// | (DIRECTED | UNDIRECTED) (EDGE|RELATIONSHIP) [TYPE] * ElementDetails CONNECTING EndPointPhrase
        /// ArcType =
        ///   ARROWBASE ElementDetails ARROW 
        /// | RARROW ElementDetails RARROWBASE
        /// | ARROWBASETILDE ElementDetails RBRACKTILDE
        /// | ARROWTILDE ElementDetails RARROWTILDE  (Editors note 35)
        /// EndpointPhrase = LPAREN Refs (RARROW|TILDE|ARROW|TO) Refs RPAREN
        /// At * we must have one of LABEL, LABELS, CONNECTING, COLON, DOUBLEARROW, IMPLIES, 
        ///    LBRACE, AS, COMMA, RBRACE
        /// At £ we must have one of CONNECTING, ARROWBASE, RARROW, ARROWBASETILDE, AS, COMMA, RBRACE
        /// At the end we must have COMMA or RBRACE and a node or edge tye.
        /// There are many syntax rules in 18.2 and 18.3 which limit the optionality of these tokens.
        /// </summary>
        /// <param name="ts"></param>
        /// <returns></returns>
        /// <exception cref="DBException"></exception>
        long ParseElementTypeSpec(CTree<long,bool> ts)
        {
            NodeType? r = null;
            var wds = CTree<Qlx, bool>.Empty;
            var lp = lxr.Position;
            var lv = lxr.val;
            wds = SpecWord(wds, Qlx.NODE, Qlx.VERTEX);
            wds = SpecWord(wds, Qlx.DIRECTED, Qlx.UNDIRECTED);
            wds = SpecWord(wds, Qlx.EDGE, Qlx.RELATIONSHIP);
            wds = SpecWord(wds, Qlx.TYPE);
            var id = new Ident("", lp);
            var al = id;
            if (tok == Qlx.Id)
            {
                id = new Ident(this); // <node type name>
                cx.dnames += (id.ident, id.uid);
                Next();
            }
            var tps = BList<(Ident, Domain)>.Empty; // the new node tye properties
            if (tok == Qlx.LPAREN) // <node/edge tye pattern> 
            {
                /// expecting ( ElementDetails ) ArcType ( ElementDetails )
                /// ArcType =
                ///   ARROWBASE ElementDetails ARROW 
                /// | RARROW ElementDetails RARROWBASE
                /// | ARROWBASETILDE ElementDetails RBRACKTILDE
                /// | ARROWTILDE ElementDetails RARROWTILDE  (Editors note 35)
                Next();
                var eg = Match(Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE);
                if (tok == Qlx.Id)
                {
                    al = new Ident(this); // <node type alias>
                    cx.dnames += (al.ident, al.uid);
                    Next();
                }
                if (id.ident == "")
                    id = al;
                var td = (wds.Contains(Qlx.EDGE) || wds.Contains(Qlx.RELATIONSHIP)) ? Domain.EdgeType : Domain.NodeType;
                var (dm, fa, ps) = ElementDetails(id, td);
                Mustbe(Qlx.RPAREN);
                var nd = new Ident(dm.name, dm.defpos);
                cx.dnames += (id.ident, nd.uid);
                id = nd;
                var fk = tok;
                eg = eg || Match(Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE);
                if (eg && dm.kind == Qlx.NODETYPE && cx.db is Transaction tr)
                    cx.db = tr + (Transaction.Physicals, tr.physicals - dm.defpos);
                var kind = (wds.Contains(Qlx.DIRECTED) || wds.Contains(Qlx.UNDIRECTED)
                         || wds.Contains(Qlx.EDGE) || wds.Contains(Qlx.RELATIONSHIP)
                         || eg) ? Qlx.EDGETYPE
                    : Qlx.NODETYPE;
                if (kind == Qlx.EDGETYPE)
                {
                    /// expecting ArcType ( ElementDetails )
                    /// ArcType =
                    ///   ARROWBASE ElementDetails ARROW 
                    /// | RARROW ElementDetails RARROWBASE
                    /// | ARROWBASETILDE ElementDetails RBRACKTILDE
                    /// | ARROWTILDE ElementDetails RARROWTILDE  (Editors note 35)
                    string? an = null;
                    Domain? st = null;
                    var sp = BList<(Ident, Domain)>.Empty;
                    string? ln = null;
                    Domain? ft = null;
                    var fp = BList<(Ident, Domain)>.Empty;
                    var lt = BList<Domain?>.Empty;
                    var at = BList<Domain?>.Empty;
                    var sa = BList<(Ident, TMetadata)>.Empty;
                    var la = BList<(Ident, TMetadata)>.Empty;
                    var aa = BList<(Ident, TMetadata)>.Empty;
                    al = new Ident(this);
                    if (Match(Qlx.Id))
                    {
                        cx.dnames += (al.ident, al.uid);
                        Next();
                    }
                    Next(); // ARROWBASE, RARROW, ARROWBASETILDE
                    var fi = id;
                    var fn = id.ident;
                    var ix = cx.dnames[fn];
                    ft = cx.obs[(ix >= 0L) ? ix : cx.role.dbobjects[fn] ?? -1L] as Domain;
                    fp = ps;
                    var mi = new Ident(this);
                    var mn = mi.ident;
                    dm = Domain.EdgeType;
                    lp = lxr.Position;
                    if (tok == Qlx.Id)
                    {
                        cx.Add(new QlValue(mi, BList<Ident>.Empty, cx, Domain.EdgeType));
                        Next();
                    }
                    else if (Match(Qlx.LABEL, Qlx.LABELS, Qlx.COLON, Qlx.IS, Qlx.Id, Qlx.LBRACE))
                    {
                        (dm, _, ps) = ElementDetails(mi, Domain.EdgeType);
                        id = new Ident(dm.name, lp);
                    }
                    var sk = tok;
                    Next();
                    Mustbe(Qlx.LPAREN);
                    var si = new Ident(this);
                    var sn = fi.ident;
                    if (tok == Qlx.Id)
                        Next();
                    else if (Match(Qlx.LABEL, Qlx.LABELS, Qlx.COLON, Qlx.IS))
                        (st, sa, sp) = ElementDetails(si, Domain.NodeType);
                    FindOrCreateElementType(si, st, sp);
                    st = cx.obs[cx.role.dbobjects[sn] ?? -1L] as NodeType;
                    Mustbe(Qlx.RPAREN);
                    switch (fk)
                    {
                        case Qlx.ARROWBASETILDE:
                            wds += (Qlx.UNDIRECTED, true);
                            goto case Qlx.ARROWBASE;
                        case Qlx.ARROWBASE:
                            if (sk != Qlx.ARROW) throw new DBException("42161", Qlx.ARROW, sk);
                            lt += ft; ln = fn; la = fa; at += st; an = sn; aa = sa;
                            break;
                        case Qlx.RARROW:
                            if (sk != Qlx.RARROWBASE) throw new DBException("42161", Qlx.ARROWBASE, sk);
                            lt += st; ln = sn; la = sa; at += ft; an = fn; aa = fa;
                            break;
                    }
                    for (var b = la.First()?.Next(); b != null; b = b.Next())
                        if (b.value().Item1 is Ident i)
                        {
                            ix = cx.names[i.ident];
                            if (cx.obs[(ix > 0L) ? ix : cx.role.dbobjects[i.ident]??-1L] is not NodeType t)
                                throw new DBException("42107", i.ident);
                            lt += t;
                        }
                    for (var b = aa.First()?.Next(); b != null; b = b.Next())
                        if (b.value().Item1 is Ident i)
                        {
                            ix = cx.names[i.ident];
                            if (cx.obs[(ix > 0L) ? ix : cx.role.dbobjects[i.ident] ?? -1L] is not NodeType t)
                                throw new DBException("42107", i.ident);
                            at += t;
                        }
                    r = FindOrCreateElementType(id, null, ps, null, lt, at, wds);
                }
            }
            else // <node/edge tye phrase>
            {
                /// expecting ElementDetails CONNECTING LPAREN Refs (RARROW|TILDE|ARROW|TO) Refs RPAREN
                /// Refs = Ref {'|' Ref}
                /// Ref = id {Metadata}
                var eg = Match(Qlx.CONNECTING, Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE); // is this needed?
                if (tok == Qlx.Id)
                {
                    al = new Ident(this); // <node tye alias>
                    cx.dnames += (al.ident, al.uid);
                    Next();
                }
                if (id.ident == "")
                    id = al;
                var td = (wds.Contains(Qlx.EDGE) || wds.Contains(Qlx.RELATIONSHIP)) ? Domain.EdgeType : Domain.NodeType;
                var (dm, fa, ps) = ElementDetails(id, td);
                var nd = new Ident(dm.name, dm.defpos);
                cx.dnames += (id.ident, nd.uid);
                id = nd;
                var fk = tok;
                eg = eg || Match(Qlx.CONNECTING, Qlx.ARROWBASE, Qlx.RARROW, Qlx.ARROWBASETILDE);
                if (eg && dm.kind == Qlx.NODETYPE && cx.db is Transaction tr)
                    cx.db = tr + (Transaction.Physicals, tr.physicals - dm.defpos);
                Ident? ai = null;
                string? an = null;
                Domain? st = null;
                var sp = BList<(Ident, Domain)>.Empty;
                Ident? li = null;
                string? ln = null;
                Domain? ft = null;
                var fp = BList<(Ident, Domain)>.Empty;
                var lt = BList<Domain?>.Empty;
                var at = BList<Domain?>.Empty;
                var sa = BList<(Ident, TMetadata)>.Empty;
                var la = BList<(Ident, TMetadata)>.Empty;
                var aa = BList<(Ident, TMetadata)>.Empty;
                al = new Ident(this);
                if (Match(Qlx.Id))
                {
                    cx.dnames += (al.ident, al.uid);
                    Next();
                }
                if (eg || dm.kind ==Qlx.EDGETYPE)
                {
                    Mustbe(Qlx.CONNECTING);
                    fa = BList<(Ident, TMetadata)>.Empty;
                    Mustbe(Qlx.LPAREN);
                    var fi = new Ident(this);
                    var ix = cx.dnames[fi.ident];
                    ft = cx.obs[(ix>0L) ? ix : cx.role.dbobjects[fi.ident] ?? -1L] as NodeType;
                    fa += (fi, ParseMetadata(Qlx.TYPE));
                    Mustbe(Qlx.Id);
                    while (Match(Qlx.VBAR))
                    {
                        Next();
                        var ij = new Ident(this);
                        Mustbe(Qlx.Id);
                        fa += (ij, ParseMetadata(Qlx.TYPE));
                    }
                    Match(Qlx.TO);
                    var ar = tok;
                    Next();
                    var se = new Ident(this);
                    ix = cx.dnames[se.ident];
                    st = cx.obs[(ix>0L) ? ix : cx.role.dbobjects[se.ident] ?? -1L] as NodeType;
                    sa += (se, ParseMetadata(Qlx.TYPE));
                    Mustbe(Qlx.Id);
                    while (Match(Qlx.VBAR))
                    {
                        Next();
                        var ik = new Ident(this);
                        Mustbe(Qlx.Id);
                        sa += (ik, ParseMetadata(Qlx.TYPE));
                    }
                    switch (ar)
                    {
                        case Qlx.TO:
                            //                if (wds.Contains(Qlx.UNDIRECTED)) goto case Qlx.TILDE;
                            goto case Qlx.ARROWR;
                        case Qlx.TILDE:
                            wds += (Qlx.UNDIRECTED, true);
                            goto case Qlx.ARROWR;
                        case Qlx.ARROWR:
                            li = fi; ln = fi.ident; lt += ft; la = fa;
                            ai = se; an = se.ident; at += st; aa = sa;
                            break;
                        case Qlx.ARROWL:
                            li = se; ln = se.ident; lt += st; la = sa;
                            ai = fi; an = fi.ident; at += ft; aa = fa;
                            break;
                    }
                    Mustbe(Qlx.RPAREN);
                    for (var b = la.First()?.Next(); b != null; b = b.Next())
                        if (b.value().Item1 is Ident i)
                        {
                            ix = cx.dnames[i.ident];
                            if (cx.obs[(ix>0L) ? ix : cx.role.dbobjects[i.ident] ?? -1L] is not NodeType t)
                                throw new DBException("42107", i.ident);
                            lt += t;
                        }
                    for (var b = aa.First()?.Next(); b != null; b = b.Next())
                        if (b.value().Item1 is Ident i)
                        {
                            ix = cx.dnames[i.ident];
                            if (cx.obs[(ix>0L) ? ix : cx.role.dbobjects[i.ident] ?? -1L] is not NodeType t)
                                throw new DBException("42107", i.ident);
                            at += t;
                        }
                    r = FindOrCreateElementType(id, null, ps, null, lt, at, wds);
                }
            }
            r ??= FindOrCreateElementType(id, null, tps, al);
            if (StartMetadata(Qlx.TYPE)) 
            {
                var m = ParseMetadata(Qlx.TYPE);
                if (r != null && m != null)
                    cx.Add(new PMetadata(id.ident, -1, r, m, cx.db.nextPos));
            }
            return r?.defpos ??throw new PEException("PE40201");
        }
        /// <summary>
        /// Given some of : an id, label expression, property set, and ends, first look to see 
        /// if we have such an element tye.
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
        /// 3: Otherwise for each tye X found 
        /// For each such X, make a supertype Y if needed so that we have p(Y)= p(X) intersect p, 
        /// and then we want a new nodetype that has these X's as supertypes 
        /// and contributes the rest of p.
        /// 4: Look at ends.
        /// 5: Finally, if there is a property called "ID", make it the primary key for the new type.
        /// INVARIANT: for any two existing nodeTypes A and B with
        /// p(A) intersect p(B) a nonempty set q, there exists C with 
        /// C superof A and C superof B and p(C)= q.
        /// </summary>
        /// <param name="id">The required name for the node tye</param>
        /// <param name="dm">The label or label expression</param>
        /// <param name="ps">The required propertry list</param>
        /// <param name="a">A local alias</param>
        /// <param name="la">For an edge, the possible types of the source node</param>
        /// <param name="aa">For an edge, the possible types of the destination node</param>
        /// <param name="wds">The keywords collected from the element tye specification</param>
        /// <returns>A (possibly joined) node tye that meets these requirements</returns>
        /// <exception cref="DBException"></exception>
        NodeType FindOrCreateElementType(Ident id, Domain? dm, BList<(Ident, Domain)> ps, Ident? a = null,
            BList<Domain?>? la = null, BList<Domain?>? aa = null, CTree<Qlx, bool>? wds = null)
        {
            if (la is null && aa is null && cx.db.objects[cx.role.nodeTypes[id.ident] ?? -1L] is NodeType ne
                && ne.Length == ps.Length)
                return ne;
            if (la is not null && aa is not null && cx.db.objects[cx.role.edgeTypes[id.ident] ?? -1L] is NodeType ee
                && ee.Length == ps.Length)
                return ee;
            var un = dm?.super ?? CTree<Domain, bool>.Empty; // relevant node types
            var ep = CTree<long, bool>.Empty; // properties found
            var pn = BTree<string, long?>.Empty; // required properties: names and qlvalue pos
            var rp = CTree<long, bool>.Empty; // properties required (by qlvalue pos)
            for (var b = ps.First(); b != null; b = b.Next())
            {
                var i = b.value().Item1;
                pn += (i.ident, i.uid);
                rp += (i.uid, true);
            }
            var op = rp;
            // 1: Construct the set of relevant (super)types
            if (id.ident != dm?.name)
            {
                if (dm is GqlLabel lb)
                    un = lb.OnInsert(cx, 0L);
                if (un.Count == 0 && dm?.defpos >= 0)
                    un += (dm, true);
            }
            if (ps.Length > 0) // watch out for existing unlabelled node tye
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
            var ix = cx.role.dbobjects[id.ident];
            NodeType? lt = null;
            NodeType? at = null;
            if (la?[0] is GqlLabel ll)
                lt = cx.db.objects[cx.role.nodeTypes[ll.name] ?? -1L] as NodeType;
            if (aa?[0] is GqlLabel al)
                at = cx.db.objects[cx.role.nodeTypes[al.name] ?? -1L] as NodeType;
            if (rp.Count==0 && un.Count==0 && ((ix>=0L)?ix:cx.role.nodeTypes[id.ident]) is long np 
                && cx.db.objects[np] is NodeType nt)
            {
                if (lt is null && at is null) return nt;
                goto Define;
            }
            if (rp.Count == 0 && un.Count == 0 && ((ix>=0L) ? ix : cx.role.edgeTypes[id.ident]) is long pe
    && cx.db.objects[pe] is Domain pd)
            {
                if (pd is EdgeType pf && pf.leavingType == lt?.defpos && pf.arrivingType == at?.defpos) return pf;
                EdgeType? te = null;
                if (pd.kind == Qlx.UNION)
                    for (var c = pd.unionOf.First(); te is null && c != null; c = c.Next())
                        if (cx.db.objects[c.key().defpos] is EdgeType tf
                            && tf.leavingType == lt?.defpos && tf.arrivingType == at?.defpos)
                            te = tf;
                if (te is not null)
                    return te;
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
                                var rt = CList<long>.Empty;
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
        // 4: Build the required node or edge tye.
        Define:;
            NodeType? ut = null;
            var ops = ps;
            if (la == null || aa == null) // It is a node tye
            {
                var tp = new PNodeType(id.ident, (NodeType)Domain.NodeType.Relocate(id.uid), un, -1L, cx.db.nextPos, cx);
                ut = (NodeType)(cx.Add(tp) ?? throw new DBException("42105"));
                var us = CTree<string, Domain>.Empty;
                for (var b = dm?.rowType.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.value()] is TableColumn tc && cx.NameFor(tc.defpos) is string tn)
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
                    cx.Add(pc);
                }
            }
            else  // it is an edge tye
                for (var bl = la.First(); bl != null; bl = bl.Next())
                    for (var ba = aa.First(); ba != null; ba = ba.Next())
                    {
                        ps = ops;
                        lt = bl.value() as NodeType; at = ba.value() as NodeType;
                        if (lt is null || at is null) continue;
                        var lx = lt.FindPrimaryIndex(cx);
                        var ax = at.FindPrimaryIndex(cx);
                        ps += (new Ident("LEAVING", cx.GetUid()),
                            (lx is null)?Domain.Position:lt.idColDomain);
                        ps += (new Ident("ARRIVING", cx.GetUid()), 
                            (ax is null)?Domain.Position:at.idColDomain);
                        var tp = new PEdgeType(id.ident, (EdgeType)Domain.EdgeType.Relocate(id.uid), un, -1L,
                            lt.defpos, at.defpos, cx.db.nextPos, cx);
                        ut = (NodeType)(cx.Add(tp) ?? throw new DBException("42105"));
                        cx.dnames += (ut.name, ut.defpos);
                        var us = CTree<string, Domain>.Empty;
                        for (var b = dm?.rowType.First(); b != null; b = b.Next())
                            if (cx.db.objects[b.value()] is TableColumn tc && cx.NameFor(tc.defpos) is string tn)
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
                            var rx = -1L;
                            if (cn.ident == "LEAVING" && lt is not null)
                            {
                                rx = lx?.defpos ?? -1L;
                                pc.flags = PColumn.GraphFlags.LeaveCol;
                                pc.toType = lt.defpos;
                            }
                            if (cn.ident == "ARRIVING" && at is not null)
                            {
                                rx = ax?.defpos ?? -1L;
                                pc.flags = PColumn.GraphFlags.ArriveCol;
                                pc.toType = at.defpos;
                            }
                            cx.Add(pc);
                            if (pc.toType >= 0 && cx.db.objects[pc.defpos] is TableColumn tc)
                            {
                                var px = new PIndex(cn.ident, (Table)tp.dataType,
                                    new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(tc), 1),
                                PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate
                                | PIndex.ConstraintType.CascadeDelete,
                                    rx,cx.db.nextPos);
                                cx.Add(px);
                            }
                        }
                        if (wds is not null && wds.Contains(Qlx.UNDIRECTED) && cx._Ob(ut.defpos) is EdgeType et)
                        {
                            et += (QuantifiedPredicate.Between, true);
                            cx.Add(et);
                        }
                    }
            var nu = cx.db.objects[ut?.defpos??-1L] as NodeType??
                new EdgeType(id.uid,id.ident,(la==null)?Domain.NodeType:Domain.EdgeType,
                    BTree<long,object>.Empty,cx);
            // 5: If there is a property called ID make it the primary key
            if (nu?.infos[cx.role.defpos] is ObInfo ui
                && ui.names.Contains("ID") && cx.obs[ui.names["ID"]] is TableColumn pt)
                cx.Add(new PIndex("ID", nu, new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(pt), 1),
                    PIndex.ConstraintType.PrimaryKey, -1, cx.db.nextPos));
            return nu ?? throw new DBException("42105");
        }
        /// <summary>
        /// When this is called id is the current Lexer Ident
        /// </summary>
        /// <param name="id"></param>
        /// <param name="xp"></param>
        /// <returns></returns>
        (Domain,BList<(Ident,TMetadata)>,BList<(Ident,Domain)>) ElementDetails(Ident id,NodeType xp)
        {
            var la = new BList<(Ident, TMetadata)>((id,TMetadata.Empty));
            var ix = cx.dnames[id.ident];
            while (Match(Qlx.VBAR))
            {
                Next();
                var ia = new Ident(this);
                Mustbe(Qlx.Id);
                la += (ia, TMetadata.Empty+ParseMetadata(Qlx.TYPE));
            }
            if (ix>=0L && cx.obs[ix] is Domain n)
                return (n, la, BList<(Ident, Domain)>.Empty);
            Domain le = GqlLabel.Empty;
            if (Match(Qlx.LABEL,Qlx.LABELS,Qlx.COLON,Qlx.DOUBLEARROW,Qlx.IMPLIES))
                le = ParseNodeLabelExpression(xp,id.ident);
            var lp = lxr.Position;
            if (Match(Qlx.DOUBLEARROW, Qlx.IMPLIES)) // we prefer DOUBLEARROW to the keyword
            {
                Next();
                var lf = ParseNodeLabelExpression(xp);
                cx.Add(lf);
                le = (Domain)cx.Add(new GqlLabel(lp,cx,le.defpos,lf.defpos,
                    new BTree<long,object>(Domain.Kind,Qlx.DOUBLEARROW)));
            }
            var ps = BList<(Ident, Domain)>.Empty;
            if (Match(Qlx.LBRACE))
            {
                Next();
    //            cx.IncSD(id);
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
                    if (Match(Qlx.COLON, Qlx.DOUBLECOLON, Qlx.TYPED)) // allow colon because of JSON option
                        Next();
                    dt = ParseDataType();
                    ps += (pn, dt);
                }
                Mustbe(Qlx.RBRACE);
                cx.defs -= cx.sD-1;
            }
            if (le.kind == Qlx.NO)
            {
                if (xp.kind == Qlx.NODETYPE)
                    le = FindOrCreateElementType(id, Domain.NodeType, ps);
                else
                    le = FindOrCreateElementType(id, Domain.EdgeType, ps, null,
                        BList<Domain?>.Empty, BList<Domain?>.Empty);
            }
            else if (le.defpos > 0L && !cx.role.dbobjects.Contains(le.name))
                le = FindOrCreateElementType(id, le, ps);
            lxr.tgs = CTree<long, TGParam>.Empty;
            return (le, la, ps);
        }
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
        internal CList<long> ParseBindingVariableDefinitions()
        {
            var r = CList<long>.Empty;
            while (Match(Qlx.BINDING, Qlx.GRAPH, Qlx.TABLE, Qlx.VALUE))
            {
                var lp = LexDp();
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
                            var gt = new GraphType(id.uid, new BTree<long, object>(Domain.Constraints, ts));
                            gt += (ObInfo.Name, id.ident);
                            cx.Add(gt);
                            r += lp;
                        }
                        Mustbe(Qlx.EQL);
                        ParseSqlValue((DBObject._Domain,Domain.GraphSpec));
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
                            var gs = ParseSqlValue((DBObject._Domain, Domain.GraphSpec));
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
                            var sv = ParseSqlValue((DBObject._Domain,dt));
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
        /// in a node tye: the node tye definition comprising a table and UDT may 
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
            var st = CList<long>.Empty;
            var cs = (GraphInsertStatement)cx.Add(new GraphInsertStatement(cx.GetUid(), sch, ge, st));
            return cs;
        }
        /// <summary>
        /// Match: MatchMode  [id'='] MatchNode {'|'Match }.
        /// MatchMode: [TRAIL|ACYCLIC|SIMPLE][SHORTEST|LONGEST|ALL|ANY].
        /// </summary>
        /// <returns></returns>
        (CTree<long, TGParam>, CList<long>) ParseGqlMatchList()
        {
            // Step M10
            var svgs = CList<long>.Empty;
            var tgs = CTree<long, TGParam>.Empty;
            Ident? pi = null;
            while (Match(Qlx.LPAREN, Qlx.USING, Qlx.TRAIL, Qlx.ACYCLIC, Qlx.SIMPLE,
                Qlx.LONGEST, Qlx.SHORTEST, Qlx.ALL, Qlx.ANY))
            {
                // state M11
                var dp = cx.GetUid();
                var alts = CList<long>.Empty;
                var mo = Qlx.NONE;
                var sh = Qlx.NONE;
                if (tok != Qlx.LPAREN)
                {
                    if (Match(Qlx.TRAIL, Qlx.ACYCLIC, Qlx.SIMPLE))
                    {
                        mo = tok;
                        Next();
                    }
                    if (Match(Qlx.SHORTEST, Qlx.LONGEST, Qlx.ALL, Qlx.ANY))
                    {
                        sh = tok;
                        cx.inclusionMode = sh;
                        Next();
                    }
                    pi = new Ident(this);
                    if (tok == Qlx.Id)
                    {
                        Next();
                        if (lxr.tgs[pi.uid] is TGParam gp)
                        {
                            gp = new TGParam(pi.uid, gp.value, Domain.PathType, TGParam.Type.Path, dp);
                            lxr.tgs += (pi.uid, gp);
                            var qi = cx.Add(new QlValue(pi, BList<Ident>.Empty, cx, Domain.PathType)
                                +(DBObject._From,dp));
                            cx.Add(pi.ident,qi);
                            cx.bindings += (pi.uid, qi.domain);
                            tgs = lxr.tgs;
                        }
                        Mustbe(Qlx.EQL);
                    }
                }
                // state M12
                (tgs, var s) = ParseGqlMatch(dp, pi, tgs);
                // state M13
                alts += cx.Add(new GqlMatchAlt(dp, cx, mo, sh, s, pi?.uid?? -1L)).defpos;
                // state M14
                while (tok == Qlx.VBAR)
                {
                    Next();
                    // state M15
                    dp = cx.GetUid();
                    (tgs, s) = ParseGqlMatch(dp, pi, tgs);
                    // state M16
                    var ns = BTree<string, TGParam>.Empty;
                    alts += cx.Add(new GqlMatchAlt(dp, cx, mo, sh, s, pi?.uid ?? -1L)).defpos;
                    // goto state M14
                }
                // state M17
                svgs += cx.Add(new GqlMatch(cx, alts)).defpos;
                if (tok == Qlx.COMMA)
                    Next();
                // goto state M11
                else break;
            };
            // state M18
            // we now can define pi properly.
            return (tgs, svgs);
        }
        // we have just passed '(' or ',' in
        // Truncation = TRUNCATING '('TruncationSpec{',' TruncationSpec} ')'.
        // we are starting on 
        // TruncationSpec = [EdgeType_id]['(']Value[')']'=' int .
        BTree<long, (long, long)> ParseTruncation()
        {
            var r = BTree<long, (long, long)>.Empty;
            // state M2
            while (true)
            {
                // state M3
                var k = Domain.EdgeType.defpos;
                Domain? et = null;
                if (tok == Qlx.Id)
                {
                    var ei = lxr.val.ToString();
                    Next();
                    et = cx.db.objects[cx.role.dbobjects[ei] ?? -1L] as Domain
                        ?? throw new DBException("42161", Qlx.EDGETYPE);
                    cx.AddDefs(et);
                    k = et.defpos;
                }
                // state M4
                var pa = Match(Qlx.LPAREN);
                if (pa)
                    Next();
                var ot = cx.Add(ParseSqlValue((DBObject._Domain,Domain.Char))).defpos;
                if (pa)
                    Mustbe(Qlx.RPAREN);
                // state M7
                Mustbe(Qlx.EQL);
                var lm = cx.Add(ParseSqlValue((DBObject._Domain,Domain.Int))).defpos;
                if (et?.kind == Qlx.UNION)
                    for (var b = et.unionOf.First(); b != null; b = b.Next())
                        r += (b.key().defpos, (lm, ot));
                else
                    r += (k, (lm, ot));
                if (tok == Qlx.COMMA)
                    Next();
                else
                    break;
                // state M8: goto state M3
            }
            Mustbe(Qlx.RPAREN);
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
  //          cx.IncSD(new Ident(this));
            // the current token is LPAREN or LBRACK
            var svg = CList<GqlNode>.Empty;
            (var n, svg) = ParseInsertGraphItem(svg, sch);
            while (tok == Qlx.RARROW || tok == Qlx.ARROWBASE)
                (n, svg) = ParseInsertGraphItem(svg, sch, n);
  //          cx.DecSD();
            return svg;
        }
        // state M19
        // This will give us a pattern of SqlNodes svg and will update tgs and cx.defs (including pi)
        (CTree<long, TGParam>, CList<long>) ParseGqlMatch(long f, Ident? pi, CTree<long, TGParam> tgs)
        {
            // the current token is LPAREN
            var svg = CList<long>.Empty;
            (var n, svg, tgs) = ParseMatchExp(svg, pi, tgs, f, tok);
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
            var bound = cx.bindings.Contains(cx.names[b.ident]);
            var nb = b;
            long id = -1L;
            var lp = lxr.Position;
            GqlNode? nd = null;
            Domain? dm = ab switch
            { Qlx.LPAREN or Qlx.RARROW => Domain.NodeType, Qlx.LBRACK => Domain.EdgeType, _ => null };
            if (tok == Qlx.Id)
            {
                if (cx.role.dbobjects.Contains(b.ident))
                    throw new DBException("42104", b.ident);
                var ix = cx.names[b.ident];
                if (ix<0L)
                {
                    id = lp;
                    if (cx._Ob(b.uid) is DBObject bo)
                        cx.Add(b.ident, bo);
                    else
                        cx.names+=(b.ident,b.uid);
                }
                else
                {
                    id = ix;
                    bound = true;
                    nd = cx.obs[id] as GqlNode ?? throw new PEException("PE80201");
                }
                Next();
            }
            Domain lb = GqlLabel.Empty;
            Qlx tk = tok;
            if (tok == Qlx.COLON)
            {
                if (bound && b.ident!="COLON")
                    throw new DBException("42104", b.ident);
                Next();
                lb = ParseNodeLabelExpression((ab == Qlx.LPAREN) ? Domain.NodeType : Domain.EdgeType,
                    b.ident,(NodeType?)((ab==Qlx.LPAREN)?null:ln?.domain));
            }
            else if (!bound)
                throw new DBException("42107", b.ident);
   //         cx.IncSD(b);
            var dc = CTree<string, QlValue>.Empty;
            CTree<long, bool>? wh = null;
            if (tok == Qlx.LBRACE)
            {
                Next();
                while (tok != Qlx.RBRACE)
                {
                    var (n, v) = GetDocItem(lp, lb, sch);
                    if (lb.name is not null && cx._Ob(b.uid) is DBObject bo)
                    {
                        var px = b.uid;
                        var ic = new Ident(n.ident, px, null);
                        var it = new Ident(lb.name, px, null);
                        var iq = new Ident(b, ic);
                        var iu = new Ident(it, ic);
                        cx.Add(iu.ident, bo); // does cx.names += (iu.ident,px)  and children
                        cx.names += (iq.ident, px);
                        cx.names += (ic.ident, px);
                    }
                    dc += (n.ident, v);
                    if (tok == Qlx.COMMA)
                        Next();
                }
                Mustbe(Qlx.RBRACE);
                if (lb == GqlLabel.Empty && dm?.FindType(cx, dc) is CTree<Domain,bool> du && du.Count==1 && du.First()?.key() is Domain cd)
                    dm = cd;
            } else 
                if (dm is not null && nd?.FindType(cx, dm) is NodeType rt)
                dm = rt;
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
                r = new GqlReference(cx, lp, (dm is null) ? r : r + (DBObject._Domain, dm));
            else if (lb.defpos >= 0 && lb.rowType == CList<long>.Empty && lb is NodeType zt
                && (zt.kind != Qlx.EDGETYPE || dc.Count == 0L))
                r = new GqlReference(lp, zt);
            else
                r = ab switch
            {
                Qlx.LPAREN => new GqlNode(nb, BList<Ident>.Empty, cx, id, dc, lxr.tgs, dm, m),
                Qlx.LBRACK => new GqlEdge(nb, BList<Ident>.Empty, cx, Qlx.Null, -1L,
                -1L, -1L, dc, lxr.tgs, dm, m),
                Qlx.ARROWBASE => new GqlEdge(nb, BList<Ident>.Empty, cx, ab, id,
                ((ln is GqlReference lr) ? lr.id?.uid : ln?.defpos) ?? -1L,
                ((an is GqlReference ar) ? ar.id?.uid : an?.defpos) ?? -1L, dc, lxr.tgs, dm, m),
                Qlx.RARROW => new GqlEdge(nb, BList<Ident>.Empty, cx, ab, id,
                ((an is GqlReference ar) ? ar.id?.uid : an?.defpos) ?? -1L,
                ((ln is GqlReference lr) ? lr.id?.uid : ln?.defpos) ?? -1L, dc, lxr.tgs, dm, m),
                _ => throw new DBException("42000", ab).Add(Qlx.INSERT_STATEMENT, new TChar(ab.ToString()))
            };
            if (wh is not null)
                r += (RowSet._Where, wh);
            cx.Add(r);
            cx.Add(b.ident, r);
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
        (GqlNode, CList<long>, CTree<long, TGParam>) ParseMatchExp(CList<long> svg, Ident? pi, CTree<long, TGParam> tgs, long f,
            Qlx lt, Ident? ln = null)
        {
            // state M21
            var st = CTree<long, TGParam>.Empty; // for match
            var ab = tok; // LPAREN, ARROWBASE, RARROW, LBRACK
            lxr.tga = f;
            var ei = new Ident(this);
     //       lxr.tgt = Names.Empty;
            var pgg = lxr.tgg;
            var og = lxr.tgs;
            lxr.tgs = CTree<long, TGParam>.Empty;
            if (tok == Qlx.LBRACK)
                lxr.tgg |= TGParam.Type.Group;
            Mustbe(Qlx.LPAREN, Qlx.ARROWBASE, Qlx.RARROW, Qlx.LBRACK);
            GqlNode? r = null;
            GqlNode? an = null;
            var b = new Ident(this);
            if (cx.obs[cx.names[b.ident]] is GqlNode nb)
                r = new GqlReference(cx,b.uid,nb);
            long id = -1L;
            var ahead = CList<long>.Empty;
            if (ab == Qlx.LBRACK)
            {
                if (r is not null)
                    throw new DBException("42002", b.ident);
                // state M22
                var pl = cx.GetUid();
                var (tgp, svp) = ParseGqlMatch(f, pi, lxr.tgs);
                Mustbe(Qlx.RBRACK);
                // state M23
                lxr.tgg = pgg;
                var qu = (-1, 0);
                if (Match(Qlx.QMARK, Qlx.TIMES, Qlx.PLUS, Qlx.LBRACE))
                    qu = ParseMatchQuantifier();
                // state M24
                tgs += tgp;
                (var sa, ahead, tgs) = ParseMatchExp(ahead, pi, tgs, f, ab, ln);
                r = new GqlPath(cx.GetUid(), cx, svp, qu, ln?.uid ?? -1L, sa.defpos);
                // to state M34
            }
            else
            {
                // state M25
                var lp = lxr.Position;
                NodeType? dm = null;
                if (tok == Qlx.Id)
                {
                    var ix = cx.names[b.ident];
                    if (lxr.tgs[lp] is TGParam ig)
                        st += (-(int)Qlx.Id, ig);
                    if (ix <0L)
                    {
                        id = lp;
                        cx.names += (b.ident, b.uid);
                    }
                    else
                        id = ix;
                    Next();
                }
                // state M26
                DBObject lb = GqlLabel.Empty;
                if (tok == Qlx.COLON)
                {
                    if (r is not null)
                        throw new DBException("42002", b.ident);
                    lxr.tex = true; // expect a Type
                    Next();
                    var s = cx.names[b.ident];
                    if (s == 0L) throw new DBException("42000");
                    lb = ParseNodeLabelExpression((ab==Qlx.LPAREN)?Domain.NodeType:Domain.EdgeType,b.ident);
                    if (cx.bindings.Contains(b.uid)) // yuk
                        cx.names += (b.ident, s);
                    if (lxr.tgs[lb.defpos] is TGParam qg)
                        st += (-(int)Qlx.TYPE, qg);
                    // state M28
                    if (lb is GqlLabel sl && sl.domain is NodeType nt && nt.defpos > 0)
                        dm = nt;
                    if (lb is Domain ld && (ld.kind == Qlx.NODETYPE||ld.kind==Qlx.EDGETYPE) && ld is NodeType tye)
                        dm = tye;
                    else if (lb is QlValue gl && gl.Eval(cx) is TTypeSpec tt)
                        dm = tt._dataType as NodeType;
                    cx.bindings += (b.uid, dm??Domain.NodeType);
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
                        lxr.tgs += (lp, new TGParam(ig.uid, ig.value, nd, pg, f));
                    }
                }
                // state M29
                var dc = CTree<string,QlValue>.Empty;
                if (lb is Domain dd)
                {
                    cx.AddDefs(dd);
                    cx.Add(lb.name, dd);
                }
                CTree<long, bool>? wh = null;
                if (tok == Qlx.LBRACE)
                {
                    if (r is not null)
                        throw new DBException("42002", b.ident);
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
                    if (r is not null)
                        throw new DBException("42002", b.ident);
                    var cd = cx.names;
                    var ot = lxr.tgs;
                    var od = dm ?? Domain.NodeType;
                    cx.names += lb.names;
                    cx.Add(new GqlNode(b, BList<Ident>.Empty, cx, -1L, CTree<string, QlValue>.Empty,
                        CTree<long, TGParam>.Empty, od));
                    cx.names += (b.ident, b.uid);
                    wh = ParseWhereClause();
                    cx.names = cd;
                    cx.obs -= b.uid; // wow
                    if (ot is not null) // why is this required?
                        lxr.tgs = ot;
                }
         //       cx.defs -= cx.sD-1;
                st += lxr.tgs;
                // state M31
                Qlx ba = Qlx.RPAREN;
                if (ln is not null && ab != Qlx.LBRACK && ab != Qlx.LPAREN)
                {
                    ba = (ab == Qlx.ARROWBASE) ? Qlx.ARROW : Qlx.RARROWBASE;
                    lt = ab;
                    if (ln.ident != "COLON" && st[ln.uid] is TGParam lg)
                        st += (-(int)ab, lg);
                    if (lxr.tgs != null)
                        lxr.tgs = CTree<long, TGParam>.Empty;
                }
                else if (ab == Qlx.LBRACK)
                    ba = Qlx.RBRACK;
                Mustbe(ba);
                r ??= cx.obs[id] as GqlReference;
                var le = (ln != null && cx.names[ln.ident] is long pl && pl >= 0L) ? pl : ln?.uid?? -1L;
                if (r is null)
                {
                    var m = BTree<long, object>.Empty + (SqlFunction._Val, lb.defpos) + (GqlNode._Label, lb);
                    r = ab switch
                    {
                        // for GqlNode, use available type information from the previous node 
                        Qlx.LPAREN => new GqlNode(b, BList<Ident>.Empty, cx, id, dc, st,
                                    cx.db.objects[((lt == Qlx.RARROW) ? LastEdge(ln, tgs)?.leavingType : LastEdge(ln, tgs)?.arrivingType) ?? -1L] as Domain, m),
                        Qlx.ARROWBASE => new GqlEdge(b, BList<Ident>.Empty, cx, ab, id, le, -1L, dc, st, dm, m),
                        Qlx.RARROW => new GqlEdge(b, BList<Ident>.Empty, cx, ab, id, -1L, le, dc, st, dm, m),
                        _ => throw new DBException("42000", ab).Add(Qlx.MATCH_STATEMENT, new TChar(ab.ToString()))
                    };
                    r += (GqlNode._Label, lb);
                    if (wh is not null)
                        r += (RowSet._Where, wh);
                }
            }
            cx.Add(r);
            // state M32
            if (Match(Qlx.LPAREN, Qlx.ARROWBASE, Qlx.RARROW, Qlx.LBRACK))
                (an, ahead, tgs) = ParseMatchExp(ahead, pi, tgs, f, lt, b);
            // state M33
            if (r is null)
                throw new DBException("42000", "MatchExp").Add(Qlx.MATCH_STATEMENT, new TChar(an?.name??"??"));
            if (r is GqlEdge)
                r = r.Add(cx, an, st);
            cx.Add(r);
            tgs += r.state;
            svg += r.defpos;
            svg += ahead;
   //         cx.DecSD();
            lxr.tgs = og;
            return (r, svg, tgs);
        }
        EdgeType? LastEdge(Ident? ln,CTree<long,TGParam> tgs)
        {
            if (cx.obs[ln?.uid ?? -1L] is GqlEdge le && le.domain.defpos >= 0
                    && le.domain is EdgeType lt)
                return lt;
            for (var b = tgs.Last(); b != null; b = b.Previous())
                if (cx.db.objects[b.key()] is EdgeType t)
                    return t;
            return null;
        }
        Domain ParseNodeLabelExpression(NodeType dm, string? a=null, NodeType? lt = null, NodeType? at = null)
        {
            var lp = lxr.Position;
            var neg = false;
            if (tok == Qlx.EXCLAMATION)
            {
                neg = true;
                Next();
            }
            var ab = tok;
            if (Match(Qlx.COLON,Qlx.DOUBLEARROW,Qlx.IMPLIES))
                Next();
            var c1 = new Ident(this);
            Mustbe(Qlx.Id);
            var left = cx._Ob(cx.role.dbobjects[c1.ident] ?? -1L) as Domain 
                ?? (Domain)cx.Add(new GqlLabel(c1,cx,lt,at,
                new BTree<long,object>(Domain.Kind,dm.kind)));
            if (ab == Qlx.DOUBLEARROW || ab == Qlx.IMPLIES)
                left = new GqlLabel(c1=new Ident(a ?? throw new DBException("42000"),lp),cx,
                    (NodeType)left, null, 
                    new BTree<long, object>(Domain.Kind, left.kind)
                    +(Domain.Under,new CTree<Domain,bool>(left,true)));
            else if (left.kind==Qlx.UNION && (lt is not null || at is not null))
            {
                var un = CTree<Domain, bool>.Empty;
                EdgeType? ee = null;
                for (var b = left.unionOf.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key().defpos] is EdgeType e
                        && ((e.leavingType == lt?.defpos && at is null)
                        || (e.arrivingType == at?.defpos && lt is null)
                        || (e.leavingType == lt?.defpos && e.arrivingType == at?.defpos)))
                    {
                        ee = e;
                        un += (e, true);
                    }
                if (un.Count == 1 && ee is not null)
                    left = ee;
                else if (un.Count < left.unionOf.Count)
                    left = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.UNION, un));
            }
            if (left is GqlLabel)
                cx.bindings += (c1.uid, left);
            cx.Add(left);
            cx.Add(c1.ident, left);
            if (neg)
            {
                left = new GqlLabel(lp, cx, -1L, left.defpos, new BTree<long,object>(Domain.Kind,Qlx.EXCLAMATION));
                cx.Add(left);
            }
            while (Match(Qlx.VBAR,Qlx.COLON,Qlx.AMPERSAND,Qlx.DOUBLEARROW))
            {
                lp = lxr.Position;
                ab = tok;
                Next();
                if (Match(Qlx.COLON,Qlx.DOUBLEARROW))
                    Next();
                var right = ParseNodeLabelExpression(Domain.NodeType,c1.ident,lt,at);
                cx.Add(right);
                left = new GqlLabel(lp, cx, left.defpos, right.defpos, new BTree<long,object>(Domain.Kind,ab)); // leave name empty for now
                cx.Add(left);
            }
            var ns = left.names;
            if (ns.Count==0)
                ns = left.infos[cx.role.defpos]?.names ?? Names.Empty;
            if (ns.Count > 0)
                left += (ObInfo._Names, ns);
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
            var olddefs = cx.defs; 
            var oldlocals = cx.bindings;
            var flags = MatchStatement.Flags.None;
            Next();
            if (tok == Qlx.SCHEMA)
            {
                flags |= MatchStatement.Flags.Schema;
                Next();
            }
            // State M0
            BTree<long, (long, long)>? tg = null;
            if (Match(Qlx.TRUNCATING))
            {
                // State M1
                Next();
                // State M2
                Mustbe(Qlx.LPAREN);
                tg = ParseTruncation();
            }
            lxr.tgs = CTree<long, TGParam>.Empty;
            cx.ParsingMatch = true;
            var (tgs, svgs) = ParseGqlMatchList();
            var xs = CTree<long, bool>.Empty;
            for (var b = svgs.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is GqlMatch ss)
                    for (var c = ss.matchAlts.First(); c != null; c = c.Next())
                        if (cx.obs[c.value()] is GqlMatchAlt sa)
                            for (var dd = sa.matchExps.First(); dd != null; dd = dd.Next())
                                if (dd.value() is long ep)
                                    xs += (ep, true);
            // state M18
            cx.ParsingMatch = false;
            var (ers, ns) = BindingTable(cx, tgs, svgs);
            var m = ers.mem;
            m += (ObInfo._Names, ns);
            m += (MatchStatement.MatchFlags, flags);
            m += (MatchStatement.BindingTable, ers.defpos);
            m += (DBObject._Domain, ers);
            if (Match(Qlx.WHERE) && ParseWhereClause() is CTree<long, bool> wh) // GQL-169
                m += (RowSet._Where, wh);
            BindingRowSet rr = ers;
            cx.Add(ers);
            cx.result = ers;
            var ms = new MatchStatement(cx, tg, tgs, svgs, m);
            cx.Add(ms);
            return ms;
        }
        internal static (BindingRowSet, Names) BindingTable(Context cx, CTree<long, TGParam> gs, CList<long> svgs)
        {
            var rt = CList<long>.Empty;
            var re = CTree<long, Domain>.Empty;
            var ns = Names.Empty;
            var ds = CTree<long, Domain>.Empty;
            for (var a = svgs.First(); a != null; a = a.Next())
                if (cx.obs[a.value()] is GqlMatch gm)
                    for (var b = gm.matchAlts.First(); b != null; b = b.Next())
                        if (cx.obs[b.value()] is GqlMatchAlt ga)
                            for (var c = ga.matchExps.First(); c != null; c = c.Next())
                                if (cx.obs[c.value()] is GqlNode gn)
                                {
                                    ds += (gn.defpos, gn.domain);
                                    for (var d = gn.docValue.First(); d != null; d = d.Next())
                                        if (cx.obs[gn.domain.names[d.key()]] is TableColumn tc
                                            && d.value() is QlValue q)
                                        {
                                            ds += (q.defpos, tc.domain);
                                            if (q.domain.kind==Qlx.CONTENT)
                                                cx.Add(q + (DBObject._Domain, tc.domain));
                                        }
                                }
            for (var b = gs.First(); b != null; b = b.Next())
                if (b.value() is TGParam g && g.value != "" && b.key()>=Transaction.TransPos
                    && cx.obs[g.uid] is DBObject sn && !re.Contains(sn.defpos) && g.IsBound(cx) is null)
                {
                    var dr = (sn is Domain d)?d:sn.domain;
                    var gp = g.uid;
                    if (g.type.HasFlag(TGParam.Type.Type))
                        dr = Domain.Char;
                    if (g.type.HasFlag(TGParam.Type.Group) && dr.kind != Qlx.ARRAY)
                        dr = new Domain(-1L, Qlx.ARRAY, dr);
                    re += (gp, dr);
                    rt += gp;
                    if (g.value != null)
                        ns += (g.value,gp);
                }
            if (rt.Count == 0)
            {
                var rc = new SqlLiteral(cx.GetUid(), "Match", TBool.True);
                rt += rc.defpos;
                re += (rc.defpos, Domain.Bool);
            }
            var nd = ((rt.Length==0)?Domain.Row:new Domain(-1L, cx, Qlx.TABLE, re, rt, rt.Length))
                + (ObInfo._Names, ns) +(MatchStatement.GDefs,gs);
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
        internal View? ParseViewDefinition(string? id = null,long dp = -1L)
        {
            var op = cx.parse;
            var lp = LexDp();
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
            // For a normal view the columns are QlInstances that refer to a derived table
            // to be defined in the AS QueryStatement part of the syntax:
            // so that initially they will have the undefined Content datatype.
            // If it is a RestView the column datatypes are specified inline
            // and constitute a RestRowSet which will have a defining position.
            // In all cases there will be objects defined in the Framing: 
            // these accumulate naturally during parsing.
            // The usage of these framing objects is different:
            // normal views are always instanced, while restviews are not.
            Domain dm = Domain.TableType;
            cx.parse = ExecuteStatus.Compile;
            cx.defs = BTree<long, Names>.Empty;
            var ns = Names.Empty;
            var nst = cx.db.nextStmt;
            Table? us = null;  // Show the USING table of a RestViewUsing
            var ts = BTree<long, ObInfo>.Empty;
            if (Match(Qlx.LPAREN))
            {
                Next();
                for (var i = 0; ; i++)
                {
                    var n = lxr.val.ToString();
                    var np = LexDp();
                    Mustbe(Qlx.Id);
                    ts += (np, new ObInfo(n));
                    ns += (n, np);
                    if (Mustbe(Qlx.COMMA, Qlx.RPAREN) == Qlx.RPAREN)
                        break;
                }
                cx.names = ns;
            }
            else if (Match(Qlx.OF))
            {
                Next();
                lp = LexDp();
                sl = lxr.start;
                if (Match(Qlx.LPAREN)) // inline tye def (RestView only)
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
            Executable? cs = null;
            if (!rest)
            {
                cx.parse = ExecuteStatus.Compile;
                cs = ParseStatementList();
                ur = cx.result as RowSet ?? throw new DBException("42000");
                if (ts != BTree<long, ObInfo>.Empty) // the view definition has explicit column names
                {
                    var ub = cx.result?.rowType.First();
                    for (var b = ts.First(); b != null && ub != null; b = b.Next(), ub = ub.Next())
                        if (ub.value() is long p && cx.obs[p] is QlValue v && b.value()?.name is string nn)
                        {
                            if (v.domain.kind == Qlx.CONTENT || v.defpos < 0) // can't simply use WellDefined
                                throw new DBException("42112", nn);
                            ns += (nn, p);
                        }
                }
                else
                    for (var b = ur.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && cx.obs[p] is QlValue v && cx.NameFor(p) is string n)
                        {
                            if (v.domain.kind == Qlx.CONTENT || v.defpos < 0) // can't simply use WellDefined
                                throw new DBException("42112", n);
                            ns += (n, p);
                        }
                cx.Add(ur + (ObInfo._Names, ns) + (ObInfo.Name, ur.alias ?? ""));
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
                    ur = ParseTableReferenceItem(lp, (DBObject._Domain, Domain.TableType));
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
                pv = new PView(id, new string(lxr.input, sl, lxr.pos - sl),
                    ur??throw new DBException("42000"), nst, cx.db.nextPos, cx);
            pv.framing = new Framing(cx, nst+1);
            var vi = pv.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.VIEW);
            pv.infos += (cx.role.defpos, vi + (ObInfo._Names, ns));
            var vw = (View)(cx.Add(pv) ?? throw new DBException("42105").Add(Qlx.VIEW));
            if (StartMetadata(Qlx.VIEW))
            {
                var m = ParseMetadata(Qlx.VIEW);
                if (vw != null && m != null)
                    cx.Add(new PMetadata(id, -1, vw, m, cx.db.nextPos));
            }
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
            return vw; // cs is null for PRestViews
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
                    tc = new ColumnPath(pa.uid, pa.ident, c, cx.db); // returns a (child)TableColumn for non-documents
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
            var typename = new Ident(lxr.val.ToString(), cx.GetPos());
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
            // UNDER may have specified a node or edge tye. If so, we change the 
            // user-defined tye dt created by lines 1583/1597/1603 above into a NodeType or EdgeType
            // respectively.
            // (If it is still a UDType it may get changed to one of these
            // by metadata. We will deal with that if it happens below.)
            if (supers.First()?.key() is EdgeType e1)
            {
                if (dt is not EdgeType et)
                    et = new EdgeType(cx.GetUid(), typename.ident, Domain.EdgeType, 
                        new BTree<long, object>(Domain.Under, new CTree<Domain, bool>(e1, true)), cx);
                dt = (UDType)cx.Add(et);
            }
            else if (supers.First()?.key() is NodeType n1)
            {
                if (dt is not NodeType nt)
                    nt = new NodeType(cx.GetUid(), typename.ident, Domain.NodeType, 
                        new BTree<long,object>(Domain.Under,new CTree<Domain,bool>(n1,true)), cx);
                dt = (UDType)cx.Add(nt);
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
            // it is a syntax error to add NODETPE/EDGETYPE metadata to a node tye:
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
            var nn = Names.Empty;
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
                var nt = new NodeType(typename.uid, dt.mem + (Domain.Kind, Qlx.NODETYPE));
                nt = nt.FixNodeType(cx, typename);
                // Process ls and m 
                dt = nt.Build(cx, null, 0L, new BTree<long, object>(Domain.NodeTypes, nt.label.OnInsert(cx,0L))+(GqlNode.DocValue,ls), m);
                ls = CTree<string, QlValue>.Empty;
                // and fix the PType to be a PNodeType
            }
            var odt = dt;
            if (m.Contains(Qlx.EDGETYPE))
            {
                if (((Transaction)cx.db).physicals[typename.uid] is not PType pt)
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
                            var d = cx.db.objects[cx.role.edgeTypes[typename.ident]??-1L] as Domain;
                            EdgeType? et = null;
                            if (d is EdgeType de && de.leavingType == lv && de.arrivingType == av)
                                et = de;
                            if (d?.kind == Qlx.UNION)
                                for (var c = d.unionOf.First(); et is null && c != null; c = c.Next())
                                    if (cx.db.objects[c.key().defpos] is EdgeType df
                                        && df.leavingType == lv && df.arrivingType == av)
                                        et = df;
                            if (et is null)
                            {
                                var pe = new PEdgeType(typename.ident, Domain.EdgeType, un, -1L, lv, av, np, cx);
                                pt = pe;
                                et = new EdgeType(np, typename.ident, dt, new BTree<long, object>(Domain.Under, un), cx, m);
                                pt.dataType = et;
                            } 
                            et = et.FixEdgeType(cx,pt);
                            dt = et.Build(cx, null, 0L, new BTree<long,object>(Domain.NodeTypes,et.label.OnInsert(cx,0L))
                                +(GqlNode.DocValue, ls)+(EdgeType.LeavingType,lv)+(EdgeType.ArrivingType,av), m);
                            np = cx.db.nextPos;
                        }
                        else throw new PEException("PE60703");
            }
            else if (m != TMetadata.Empty)
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
            mn.name = new Ident(lxr.val.ToString(), cx.db.nextPos);
            Mustbe(Qlx.Id);
            int st = lxr.start;
            if (mn.name is not Ident nm)
                throw new DBException("42000", "Method name");
            mn.ins = ParseParameters(mn.name, xp);
            mn.mname = new Ident(nm.ident, nm.uid);
            for (var b = mn.ins.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var pa = (FormalParameter?)cx.obs[p] ?? throw new PEException("PE1621");
                    cx.names += (mn.mname.ident, mn.mname.uid);
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
        /// <returns>maybe a tye</returns>
		UDType? ParseMethodHeader(Domain? xp = null)
        {
            MethodName mn = ParseMethod(xp);
            if (mn.name is not Ident nm || mn.retType == null || mn.type == null)
                throw new DBException("42000", "Method header");
            var r = new PMethod(nm.ident, mn.ins,
                mn.retType, mn.methodType, mn.type, null,
                new Ident(mn.signature, nm.uid), cx.db.nextStmt, cx.db.nextPos, cx);
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
            var meth = cx.db.objects[oi?.methodInfos[nm.ident]?[Database.Signature(mn.ins)] ?? -1L] as Method ??
    throw new DBException("42132", nm.ToString(), oi?.name ?? "??").Mix();
            var lp = LexDp();
            int st = lxr.start;
            var nst = cx.db.nextStmt;
            cx.obs = ObTree.Empty;
            cx.Add(meth.framing); // for formals from meth
                                  //            var nst = meth.framing.obs.First()?.key()??cx.db.nextStmt
            cx.names = cx.anames;
            for (var b = meth.ins.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var pa = (FormalParameter?)cx.obs[p] ?? throw new DBException("42000", "Method");
                    cx.Add(pa.name??"",pa);
                }
            cx.Add(meth.names);
            cx.Add(nm.ident, meth);
            var tn = ut.infos[cx.role.defpos]?.names ?? Names.Empty;
            cx.Add(tn);
            cx.defs += (meth.defpos, tn + meth.names);
            cx.anames = cx.names;
            var oa = cx.anames;
            var od = cx.defs;
            meth += (Procedure.Body,
                (ParseStatement((DBObject._Domain,mn.retType),(NestedStatement.WfOK,true), (Procedure.ProcBody, true)) 
                ?? throw new DBException("42000", "MethodBody")).defpos);
            Ident ss = new(new string(lxr.input, st, lxr.start - st), lp);
            cx.parse = op;
            // we really should check the signature here
            var md = new Modify(meth.defpos, meth, ss, nst, cx.db.nextPos, cx);
            cx.Add(md);
            cx.defs = od;
            cx.anames = oa;
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
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
                var dv = ParseSqlValue((DBObject._Domain,type));
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
            var se = ParseSqlValue((DBObject._Domain, Domain.Bool));
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
            var pt = new PTable(name.ident, Domain.TableType,cx.db.nextPos, cx);
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
                            var dv = ParseSqlValue((DBObject._Domain, tc.domain));
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
            var lp = colname.uid;
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
                gr = ParseGenerationRule(lp, tb, dom);
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
                tc += (ObInfo._Names, oi.names);
                tc += (ObInfo._Metadata, oi.metadata);
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
                    Qlx.SENSITIVE, Qlx.CARDINALITY);
                case Qlx.ANY:
                    Match(Qlx.DESC, Qlx.URL, Qlx.MIME, Qlx.SQLAGENT, Qlx.USER, Qlx.PASSWORD,
                        Qlx.ENTITY, Qlx.PIE, Qlx.HISTOGRAM, Qlx.LEGEND, Qlx.LINE, Qlx.POINTS, Qlx.REFERRED,
                        Qlx.ETAG, Qlx.ATTRIBUTE, Qlx.X, Qlx.Y, Qlx.CAPTION, Qlx.REFERS, Qlx.JSON, Qlx.CSV,
                        Qlx.INVERTS, Qlx.MONOTONIC, Qlx.PREFIX, Qlx.SUFFIX, Qlx.CARDINALITY);
                    return !Match(Qlx.EOF, Qlx.RPAREN, Qlx.COMMA, Qlx.RBRACK, Qlx.RBRACE);
                default: return Match(Qlx.CHARLITERAL, Qlx.RDFLITERAL);
            }
        }
        internal TMetadata ParseMetadata(string s, int off, Qlx kind)
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
        internal TMetadata ParseMetadata(Qlx kind)
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
                                if (Match(Qlx.SET)) // ^5
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
                                if (Match(Qlx.SET)) // ^7
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
                            if (Match(Qlx.TO))
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Qlx.INTEGERLITERAL, Qlx.TIMES, Qlx.NULL);
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
                            if (Match(Qlx.TO))
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Qlx.INTEGERLITERAL, Qlx.TIMES, Qlx.NULL);
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
            return new TMetadata(m);
        }
        /// <summary>
        /// GenerationRule =  GENERATED ALWAYS AS '('Value')' [ UPDATE '(' Assignments ')' ]
        /// |   GENERATED ALWAYS AS ROW (START|END) .
        /// </summary>
        /// <param name="rt">The expected tye</param>
        GenerationRule ParseGenerationRule(long tc, Table tb, Domain xp)
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
                    for (var b = tb.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && cx.db.objects[p] is TableColumn co)
                        {
                            var nm = co.NameFor(cx) ?? "";
                            var rv = new QlInstance(cx.GetUid(), cx, nm, -1L, co.defpos);
                            cx.Add(rv);
                            cx.Add(nm, rv);
                        }
                    var gnv = ParseSqlValue((DBObject._Domain, xp));
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
                                var pr = cx.GetProcedure(LexDp(), ni.ident, new CList<Domain>(tb))
                                     ?? throw new DBException("42108", ni.ident);
                                afn = "\"" + pr.defpos + "\"";
                            }
                            else
                            {
                                Mustbe(Qlx.LPAREN);
                                ParseSqlValueList();
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
                        cx.Add(tc + (Domain.Default, ParseSqlValue((DBObject._Domain,tc.domain)).Eval(cx) ?? TNull.Value));
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
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
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
                    var pr = cx.GetProcedure(LexDp(), ic.ident, Database.Signature(refs))
                        ?? throw new DBException("42108", ic.ident);
                    afn = "\"" + pr.defpos + "\"";
                }
                else
                {
                    Mustbe(Qlx.LPAREN);
                    ParseSqlValueList();
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
            var se = ParseSqlValue((DBObject._Domain,Domain.Bool));
            Mustbe(Qlx.RPAREN);
            var n = name ?? new Ident(this);
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            PCheck r = new(tb, n.ident, se, new string(lxr.input, st, lxr.start - st), nst, tr.nextPos, cx);
            tb = (Table)(cx.Add(r) ?? throw new DBException("42105").Add(Qlx.CHECK));
            if (tb.defpos < Transaction.TransPos)
            {
                var trs = tb.RowSets(new Ident("", -1L), cx, tb, -1L, 0L);
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
            var ix = tb.defpos;
       //     cx.Add(tb.NameFor(cx), tb);
            var nst = cx.db.nextStmt;
            var se = ParseSqlValue((DBObject._Domain,Domain.Bool));
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
        /// <returns>constraint tye flags</returns>
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
            var n = new Ident(lxr.val.ToString(),cx.db.nextPos); // n.uid will match pp.ppos
            cx.parse = ExecuteStatus.Compile;
            Mustbe(Qlx.Id);
            int st = lxr.start;
            var ps = ParseParameters(n);
            var a = Database.Signature(ps);
            var ap = LexDp();
            var pi = new ObInfo(n.ident,
                Grant.Privilege.Owner | Grant.Privilege.Execute | Grant.Privilege.GrantExecute);
            var rdt = func ? ParseReturnsClause(n) : Domain.Null;
            if (Match(Qlx.EOF) && create == Qlx.CREATE)
                throw new DBException("42000", "EOF").Mix();
            var pr = cx.GetProcedure(LexDp(), n.ident, a);
            PProcedure? pp = null;
            if (pr == null)
            {
                if (create == Qlx.CREATE)
                {
                    // create a preliminary version of the PProcedure without parsing the body
                    // in case the procedure is recursive (the body is parsed below)
                    pp = new PProcedure(n.ident, ps,
                        rdt, pr, new Ident(lxr.input.ToString() ?? "", n.uid), nst, cx.db.nextPos, cx);
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
            var oa = cx.anames;
            var od = cx.defs;
            cx.anames += (n.ident, pr.defpos);
            cx.defs += (pr.defpos, ps.names);
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
                var lp = LexDp();
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
                    var bd = ParseStatementList((Procedure.ProcBody,true),(DBObject.Scope,ap),
                        (DBObject._Domain,pr.domain),(NestedStatement.WfOK,true)) ?? throw new DBException("42000", "Statement");
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
                    cx.result = null;
                    cx.binding = CTree<long, TypedValue>.Empty;
                    cx.parse = op;
                }
                if (create == Qlx.CREATE)
                    cx.db += pr;
                var cix = pr.defpos;
                if (pp == null)
                {
                    var pm = new Modify(pr.defpos, pr, s, nst, cx.db.nextPos, cx);
                    cx.Add(pm); // finally add the Modify
                }
            }
            cx.anames = oa;
            cx.defs = od;
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
            cx.parse = op;
        }
        internal (Domain, Domain) ParseProcedureHeading(Ident pn)
        {
            var ps = Domain.Null;
            var oi = Domain.Null;
            if (tok != Qlx.LPAREN)
                return (ps, Domain.Null);
            ps = ParseParameters(pn);
            LexDp(); // for synchronising with CREATE
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
                var pp = ParseProcParameter(pn, xp);
                r += pp;
                if (pp.name is not null)
                    cx.names += (pp.name, pp.defpos);
                if (tok != Qlx.COMMA)
                    break;
                Next();
            }
            Mustbe(Qlx.RPAREN);
            cx.parse = op;
            return (r.Length>0)?new Domain(Qlx.ROW, cx, r):Domain.Null;
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
            var p = new FormalParameter(n.uid, pmode, n.ident, ParseDataType(n))
                + (DBObject._From, pn.uid);

            cx.Add(p);
    //        if (xp == null) // prepare to parse a body
     //           cx.names += (pn.ident, pn.uid);
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
        /// <returns>the Executable valueType of the parse</returns>
		Executable ParseDeclaration()
        {
            var lp = LexDp();
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
                var cs = ParseSelectStatement();
                var cu = (RowSet?)cx.obs[cs.result] ?? throw new PEException("PE1557");
                var sc = new SqlCursor(n.uid, cu, n.ident);
                cx.result = null;
                cx.Add(sc);
                lv = new CursorDeclaration(lp, sc, cu);
                cx.Add(n.ident, sc);
            }
            else
            {
                var ld = ParseDataType();
                var vb = new QlValue(n, BList<Ident>.Empty, cx, ld);
                cx.Add(vb);
                lv = new LocalVariableDec(lp, vb);
                if (Match(Qlx.EQL, Qlx.DEFAULT))
                {
                    Next();
                    var iv = ParseSqlValue((DBObject._Domain,ld));
                    cx.Add(iv);
                    lv += (LocalVariableDec.Init, iv.defpos);
                }
                cx.Add(n.ident, vb);
            }
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
            ParseStatement((NestedStatement.WfOK,true));
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
		/// NestedStatement = Label BEGIN [XMLDec] Statements END .
        /// </summary>
        /// <param name="n">the label</param>
        /// <returns>the Executable valueType of the parse</returns>
		NestedStatement ParseNestedStatement(params (long, object)[] m)
        {
            var cs = new NestedStatement(LexDp(), cx, CList<CList<long>>.Empty, m);
            var st = Mustbe(Qlx.BEGIN,Qlx.LBRACE);
            var et = (st == Qlx.BEGIN) ? Qlx.END : Qlx.RBRACE;
            var lp = LexDp();
            if (Match(Qlx.TRANSACTION))
                throw new DBException("22G01", "Nested transactions are not supported").ISO();
            var r = CList<CList<long>>.Empty;
            var rr = CList<long>.Empty;
            while (tok != et && StartStatement() && ParseStatement(m) is Executable a)
            {
                rr += cx.Add(a).defpos;
                if (Match(Qlx.SEMICOLON, Qlx.NEXT))
                {
                    r += rr;
                    // unbind any intervening GqlNode names
                    for (var b = cx.names.First(); b != null; b = b.Next())
                    {
                        if (b.value() is long bp && cx.obs[bp] is GqlNode n
                            && n.name?[0] != '#')
                        {
                            if (n is GqlReference nr)
                                bp = nr.refersTo;
                            if (bp>lp)
                                cx.names -= b.key();
                        }
                    }
                    rr = CList<long>.Empty;
                    Next();
                }
            }
            r += rr;
            Mustbe(et);
            cs += (cx, NestedStatement.Stms, r);
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
                var t = ParseSqlValueEntry();
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
        /// <param name="xp">the expected ob tye if any</param>
		Executable ParseLabelledStatement(params (long, object)[] m)
        {
            var (sc,il) = ParseIdentChain(false);
            var lp = lxr.start;
            var cp = LexDp();
            // OOPS: according to SQL 2003 there MUST follow a colon for a labelled statement
            if (tok == Qlx.COLON)
            {
                Next();
                var s = sc.ident;
                var e = tok switch
                {
                    Qlx.BEGIN => ParseNestedStatement((Executable.Label,s)),
                    Qlx.FOR => ParseForStatement((Executable.Label, s)),
                    Qlx.LOOP => ParseLoopStatement((Executable.Label, s)),
                    Qlx.REPEAT => ParseRepeat((Executable.Label, s)),
                    Qlx.WHILE => ParseSqlWhile((Executable.Label, s)),
                    _ => throw new DBException("26000", s).ISO(),
                };
                return (Executable)cx.Add(e);
            }
            // OOPS: but we'q better allow a procedure call here for backwards compatibility
            else if (tok == Qlx.LPAREN)
            {
                Next();
                cp = LexDp();
                var ps = ParseSqlValueList();
                Mustbe(Qlx.RPAREN);
                var a = cx.db.Signature(cx,ps);
                var pr = cx.GetProcedure(cp, sc.ident, a) ??
                    throw new DBException("42108", sc.ident);
                var c = new SqlProcedureCall(cp, cx, pr, ps);
                return (Executable)cx.Add(new CallStatement(cx.GetUid(), c));
            }
            // OOPS: and a simple assignment for backwards compatibility
            else if (Identify(sc, il, Domain.Content) is DBObject vb)
            {
                if (vb is QlInstance vc && cx.db.objects[vc.sPos] is TableColumn tc)
                    vb = tc;
                Mustbe(Qlx.EQL);
                var va = ParseSqlValue((DBObject._Domain, vb.domain));
                var sa = new AssignmentStatement(cp, vb, va);
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
            var lp = LexDp();
            Next();
            if (tok == Qlx.LPAREN)
                return ParseMultipleAssignment();
            if (Match(Qlx.AUTHORIZATION, Qlx.ROLE, Qlx.TIMEOUT,Qlx.TABLE, Qlx.DOMAIN,Qlx.TYPE,
                Qlx.PROCEDURE, Qlx.FUNCTION, Qlx.TRIGGER, Qlx.METHOD, Qlx.REFERENCING,
                Qlx.STATIC, Qlx.INSTANCE, Qlx.OVERRIDING, Qlx.CONSTRUCTOR))
            { 
                ParseSqlSet(); 
                return Executable.None; 
            }
            var vb = ParseVarOrColumn((DBObject.Scope,LexDp()));
            cx.Add(vb);
            if (Match(Qlx.COLON,Qlx.IS) && cx.binding[vb.defpos] is TNode tn)
            {
                Next();
                var ln = new Ident(this);
                Mustbe(Qlx.Id);
                if (cx.db.objects[cx.role.dbobjects[ln.ident] ?? -1L] is not Table nt)
                    throw new DBException("42107", ln.ident);
                if (cx.parse.HasFlag(ExecuteStatus.Obey))
                {
                    nt += (Table.TableRows, nt.tableRows + (tn.defpos, tn.tableRow));
                    nt += (Table.LastData, cx.db.nextPos); // this needs more work
                    cx.db += nt;
                }
                return Executable.None;
            }
            Mustbe(Qlx.EQL);
            var va = ParseSqlValue((DBObject._Domain,vb.domain));
            var sa = new AssignmentStatement(lp, vb, va);
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
            var v = ParseSqlValue();
            cx.Add(v);
            var ma = new MultipleAssignment(LexDp(), cx, ids, v);
            return (Executable)cx.Add(ma);
        }
        /// <summary>
        /// |	RETURN TypedValue
        /// |   FINISH
        /// </summary>
		Executable ParseReturn(params (long,object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain?? Domain.Null;
            if (Match(Qlx.FINISH))
            {
                Next();
                cx.result = new FinishRowSet(cx);
                return EmptyStatement.Empty;
            }
            Next();
            QlValue re;
            var dp = cx.GetUid();
            var ep = -1L;
            var dm = xp;
            if (xp.kind == Qlx.CONTENT)
            {
                var on = cx.names;
                cx.names = cx.anames;
                ep = cx.GetUid();
                dm = ParseSelectList(-1L,
                    (DBObject.Scope, LexDp()), (DBObject._Domain, xp + (Domain.Kind, Qlx.ROW)));
                cx.names = on + cx.names;
                if (dm.aggs == CTree<long, bool>.Empty)
                    new ExplicitRowSet(ep, cx, dm, BList<(long, TRow)>.Empty);
                else
                {
                    var ii = cx.GetUid();
                    var sd = dm.SourceRow(cx, dp); // this is what we will need
                    RowSet sr = new SelectRowSet(ii, cx, dm, new ExplicitRowSet(ep, cx, sd, BList<(long, TRow)>.Empty));
                    if (xp.mem[Domain.Nodes] is CTree<long, bool> xs) // passed to us for MatchStatement Return handling
                        sr += (Domain.Nodes, xs);
                    sr = ParseSelectRowSet((SelectRowSet)sr); // this is what we will do with it
                    ep = sr.defpos;
                    dm = sd;
                }
                re = new SqlRow(dp, cx, dm);
            }
            else
            {
                if (xp.kind == Qlx.TYPE)
                    m = Add(m, (RowSet._Scalar, true));
                re = ParseSqlValue(m);
            }
            cx.Add(re);
            var rs = new ReturnStatement(cx.GetUid(), re);
            if (cx.obs[cx.undefined.First()?.key()??-1L] is SqlReview x)
                throw new DBException("42108",x.name??"");
            cx.lastret = cx.obs[rs.defpos] as RowSet;
            if (dm != Domain.Content)
                rs += (DBObject._Domain, dm);
            if (ep >= 0)
                rs += (SqlInsert.Value, ep);
            else
                rs += (SqlInsert.Value, re.defpos);
            return (Executable)cx.Add(rs);
        }
        /// <summary>
		/// CaseStatement = 	CASE TypedValue { WHEN TypedValue THEN Statements }+ [ ELSE Statements ] END CASE
		/// |	CASE { WHEN QlValue THEN Statements }+ [ ELSE Statements ] END CASE .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseCaseStatement(params (long, object)[]m)
        {
            Next();
            if (tok == Qlx.WHEN)
            {
                var ws = ParseWhenList();
                var ss = EmptyStatement.Empty;
                if (tok == Qlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList();
                }
                var e = new SearchedCaseStatement(LexDp(), ws, ss);
                Mustbe(Qlx.END);
                Mustbe(Qlx.CASE);
                cx.Add(e);
                return e;
            }
            else
            {
                var op = ParseSqlValue();
                var ws = ParseWhenList((DBObject._Domain, op.domain));
                var ss = EmptyStatement.Empty;
                if (tok == Qlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList();
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
		BList<WhenPart> ParseWhenList(params (long, object)[]m)
        {
            var r = BList<WhenPart>.Empty;
            var dp = LexDp();
            while (tok == Qlx.WHEN)
            {
                Next();
                var c = ParseSqlValue((DBObject._Domain,Domain.Bool));
                Mustbe(Qlx.THEN);
                r += new WhenPart(dp, c, ParseStatementList(m));
            }
            return r;
        }
        LetStatement ParseLet()
        {
            var lp = LexDp();
            var r = CList<long>.Empty;
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
                    if (cx.Known(id.ident))
                        throw new DBException("42104", id.ident);
                    var vb = new QlValue(id, BList<Ident>.Empty, cx, Domain.Content);
                    cx.Add(id.ident, vb);
                    Mustbe(Qlx.EQL);
                    var sv = ParseSqlValue();
                    cx.Add(sv);
                    var sa = new UpdateAssignment(vb.defpos, sv.defpos);
                    vb += (DBObject._Domain, sv.domain);
                    cx.Add(vb);
                    r += vb.defpos;
                    rs += (vb.defpos, sv.domain);
                    sg += (sa, true);
                }
            }
            var dm = (r.Length==0)?Domain.TableType:new Domain(-1L,cx,Qlx.TABLE,rs,r);
            var ls = new LetStatement(lp, new BTree<long, object>(RowSet.Assig,sg)+(DBObject._Domain,dm));
            cx.Add(ls);
            return ls;
        }
        Executable ParseOrderAndPage()
        {
            var lp = LexDp();
            var m = BTree<long, object>.Empty;
            Domain dm = (cx.result as RowSet)??Domain.Row;
            m += (QueryStatement.Result, dm.defpos);
            if (tok == Qlx.ORDER)
            {
                dm = ParseOrderClause(dm);
                m += (RowSet.RowOrder, dm);
            }
            if (Match(Qlx.OFFSET,Qlx.SKIP))
            {
                Next();
                var so = ParseSqlValue((DBObject._Domain, Domain.Int));
                cx.Add(so);
                m += (RowSetSection.Offset, so.Eval(cx).ToInt() ?? 0);
            }
            if (Match(Qlx.FETCH))
                Next();
            if (Match(Qlx.LIMIT))
            {
                Next();
                var so = ParseSqlValue((DBObject._Domain, Domain.Int));
                cx.Add(so);
                var ln = so.Eval(cx).ToInt() ?? 0;
                if (ln < 0) throw new DBException("22G02").ISO();
                m += (RowSetSection.Size, ln);
            }
            else if (Match(Qlx.FIRST))
                m += (RowSetSection.Size, FetchFirstClause());
            var ls = new OrderAndPageStatement(lp, m);
            cx.Add(ls);
            return ls;
        }
        FilterStatement ParseFilter()
        {
            if (Match(Qlx.FILTER))
                Next();
            var lp = LexDp();
            var m = BTree<long, object>.Empty;
            Domain dm = cx.result ?? Domain.Row; 
            if (ParseWhereClause() is CTree<long,bool> wh)
                m += (RowSet._Where, wh);
            var ls = new FilterStatement(lp, m);
            cx.Add(ls);
            return ls;
        }
        /// <summary>
		/// ForStatement =	Label FOR [ For_id AS ][ id CURSOR FOR ] QueryExpression DO Statements END FOR [Label_id] .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The tye of the test expression</param>
        /// <returns>the Executable valueType of the parse</returns>
        Executable ParseForStatement(params (long, object)[] m)
        {
            var mm = BTree<long, object>.New(m);
            var n = mm[Executable.Label] as string;
            var lp = LexDp();
            Next();
            Ident c = new(DBObject.Uid(lp), lp);
            var d = 1; // depth
            if (tok != Qlx.SELECT) // GQL for statement
            {
                c = new Ident(this);
                Mustbe(Qlx.Id);
                if (cx.names.Contains(c.ident))
                    throw new DBException("42014", c.ident);
                cx.names += (c.ident, c.uid);
                if (tok==Qlx.IN)
                {
                    Next();
                    var fv = new QlValue(c, BList<Ident>.Empty, cx, Domain.Content);
                    var fl = ParseSqlValue();
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
                        cx.names += (c.ident, c.uid);
                        cc = cx.Add(new QlValue(c, BList<Ident>.Empty, cx, Domain.Int)).defpos;
                    }
                    var xf = ParseStatement();
                    var r = (Executable)cx.Add(new ForStatement(lp, fv, fl, op, cc, xf, cx));
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
            var ss = ParseSelectStatement(); // use ambient declarations
            d = Math.Max(d, ss.depth + 1);
            var cs = (RowSet?)cx.obs[ss.result] ?? throw new DBException("42000", "CursorSpec");
            var sawDo = Match(Qlx.DO);
            if (sawDo) Next();
            var xs = ParseStatements();
            mm += (ForSelectStatement.ForVn, c.ident);
            mm += (ForSelectStatement.Sel, cs.defpos);
            mm += (ConditionalStatement.Then, xs);
            var fs = new ForSelectStatement(lp, mm) + (DBObject._Depth, d);
            if (sawDo) { Mustbe(Qlx.END); Mustbe(Qlx.FOR); }
            if (tok == Qlx.Id)
            {
                if (n != null && n != lxr.val.ToString())
                    throw new DBException("42157", lxr.val.ToString(), n).Mix();
                Next();
            }
            return (Executable)cx.Add(fs);
        }
        Executable ParseCompositeQueryStatement(params (long, object)[]m)
        {
            var lp = LexDp();
            var lf = cx.result?.defpos ?? throw new DBException("42000", tok);
            var tk = Mustbe(Qlx.EXCEPT,Qlx.INTERSECT,Qlx.UNION);
            var rg = (ParseStatement(m) as QueryStatement??throw new DBException("42000")).defpos;
            return (Executable)cx.Add(new CompositeQueryStatement(lp, cx, tk, lf, rg));
        }
        /// <summary>
        /// IfStatement = 	IF BooleanExpr THEN Statements { ELSEIF BooleanExpr THEN Statements } [ ELSE Statements ] END IF .
        /// </summary>
        /// <param name="xp">The tye of the test expression</param>
        /// <returns>the Executable valueType of the parse</returns>
        Executable ParseIfThenElseStmt(params (long, object)[]m)
        {
            var lp = LexDp();
            var old = cx;
            Next();
            var se = ParseSqlValue((DBObject._Domain, Domain.Bool));
            cx.Add(se);
            Mustbe(Qlx.THEN);
            var th = ParseStatements(m);
            var ei = CList<long>.Empty;
            while (Match(Qlx.ELSEIF))
            {
                var d = LexDp();
                Next();
                var s = ParseSqlValue((DBObject._Domain, Domain.Bool));
                cx.Add(s);
                Mustbe(Qlx.THEN);
                Next();
                var t = ParseStatements(m);
                var e = new ConditionalStatement(d, s, t, CList<long>.Empty, EmptyStatement.Empty);
                cx.Add(e);
                ei += new CList<long>(e.defpos);
            }
            var el = EmptyStatement.Empty;
            if (tok == Qlx.ELSE)
            {
                Next();
                el = ParseStatements(m);
            }
            Mustbe(Qlx.END);
            Mustbe(Qlx.IF);
            var ife = new ConditionalStatement(lp, se, th, ei, el);
            cx = old;
            var r = (Executable)cx.Add(ife);
            return r;
        }
        Executable ParseConditionalStmt(params (long, object)[]m)
        {
            var lp = LexDp();
            var old = cx;
            Next();
            var se = ParseSqlValue((DBObject._Domain, Domain.Bool));
            cx.Add(se);
            Mustbe(Qlx.THEN);
            var th = ParseStatement(m);
            var w030 = th.gql;
            m = Add(m, (DBObject.Gql,th.gql)); // for WG3:GYD-030 now apply this value to all branches
            var ei = CList<long>.Empty;
            while (Match(Qlx.ELSEIF))
            {
                var d = LexDp();
                Next();
                var s = ParseSqlValue((DBObject._Domain, Domain.Bool));
                cx.Add(s);
                Mustbe(Qlx.THEN);
                Next();
                var t = ParseStatement(m).Check(w030);
                var e = new ConditionalStatement(d, s, t, CList<long>.Empty, EmptyStatement.Empty);
                cx.Add(e);
                ei += e.defpos;
            }
            Executable el = EmptyStatement.Empty;
            if (tok == Qlx.ELSE)
            {
                Next();
                el = ParseStatement(m);
            }
            if (Match(Qlx.END))
            {
                Next();
                Mustbe(Qlx.IF);
            }
            var ife = new ConditionalStatement(lp, se, th, ei, el);
            cx = old;
            var r = (Executable)cx.Add(ife);
            return r;
        }
        (long, object)[] Add((long, object)[] m,(long,object)x)
        {
            var n = m.Length;
            var r = new (long, object)[n + 1];
            for (var j = 0; j < n; j++)
                r[j] = m[j];
            r[n] = x;
            return r;
        }
        /// <summary>
        /// Statements = 	Statement { [';'|NEXT|] Statement } .
        /// Semicolons|NEXT separate sequences, and discard the bindings from the previous sequence
        /// </summary>
        /// <returns>a list of lists of the statements found</returns>
        Executable ParseStatements(params (long, object)[] m)
        {
            var lp = LexDp();
            var r = CList<CList<long>>.Empty;
            var s = CList<long>.Empty;
            var ox = cx.result;
            for (var b = cx.names.First(); b != null; b = b.Next())
                if (b.value() is long p && p > 0 && cx.obs[p]?.Defined()==true)
                    cx.anames += (b.key(), b.value());
            while (StartStatement() && !Match(Qlx.UNTIL))
            {
                if (ParseStatement(m) is not Executable b)
                    throw new DBException("42161", "statement");
                s += ((Executable)cx.Add(b)).defpos;
                cx.result = b.domain;
                if (Match(Qlx.SEMICOLON, Qlx.NEXT))
                {
                    Next();
                    cx.result = ox;
                    r += s;
                    s = CList<long>.Empty;
                }
            }
            var ns = new NestedStatement(cx.GetUid(), cx, r + s);
            if (cx.result is not null)
                ns += (QueryStatement.Result, cx.result?.defpos ?? -1L);
            return (Executable)cx.Add(ns);
        }
		internal Executable ParseStatementList(params (long, object)[] m)
        {
            var mm = BTree<long, object>.New(m);
            var ap = (long)(mm[DBObject.Scope] ?? -1L);
            var lp = LexDp();
            var s = CList<long>.Empty;
            var ox = cx.result;
            for (var b = cx.names.First(); b != null; b = b.Next())
                if (b.value() is long p && p>0 && (cx.obs[p]?.Defined()!=false) 
                    && (p<ap || p>Transaction.HeapStart))
                    cx.anames += (b.key(), b.value());
            while (StartStatement())
            {
                if (ParseStatement(m) is not Executable b)
                    throw new DBException("42161", "statement");
                s += ((Executable)cx.Add(b)).defpos;
                cx.result = b.domain;
            }
            var ns = new AccessingStatement(cx.GetUid(), new BTree<long,object>(AccessingStatement.GqlStms,s));
            if (cx.result is not null)
                ns += (QueryStatement.Result,cx.result?.defpos??-1L);
            return (Executable)cx.Add(ns);
        }
        /// <summary>
        /// traverse a comma-separated variable tree
        /// </summary>
        /// <returns>the tree</returns>
		CList<long> ParseTargetList(params (long, object)[] m)
        {
            bool b = (tok == Qlx.LPAREN);
            if (b)
                Next();
            var r = CList<long>.Empty;
            for (; ; )
            {
                var v = ParseVarOrColumn(m);
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
        /// The valueType will get classified as variable or ident
        /// during the Analysis stage Selects when things get setup
        /// </summary>
        /// <param name="ppos">the lexer position</param>
        /// <returns>an sqlName </returns>
        QlValue ParseVarOrColumn(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Content;
            Match(Qlx.SYSTEM_TIME, Qlx.SECURITY);
            if (tok == Qlx.SECURITY)
            {
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER);
                var sp = LexDp();
                Next();
                return (QlValue)cx.Add(new SqlFunction(sp, cx, Qlx.SECURITY, null, null, null, Qlx.NO));
            }
            if (Match(Qlx.PARTITION, Qlx.POSITION, Qlx.VERSIONING, Qlx.CHECK,
                Qlx.SYSTEM_TIME, Qlx.LAST_DATA))
            {
                QlValue ps = new SqlFunction(LexDp(), cx, tok, null, null, null, Qlx.NO);
                Next();
                if (tok == Qlx.LPAREN && ((SqlFunction)ps).domain.kind == Qlx.VERSIONING)
                {
                    var vp = LexDp();
                    Next();
                    if (tok == Qlx.SELECT)
                    {
                        var cs = ParseSelectStatement();
                        Mustbe(Qlx.RPAREN);
                        var sv = (QlValue)cx.Add(new QlValueQuery(vp, cx,
                            (RowSet?)cx.obs[cs.result] ?? throw new DBException("42000", "Version"), 
                            xp, new CList<long>(cs.defpos)));
                        ps += (cx, SqlFunction._Val, sv.defpos);
                    }
                    else
                        Mustbe(Qlx.RPAREN);
                }
                return (QlValue)cx.Add(ps);
            }
            var ttok = tok;
            var (ic, il) = ParseIdentChain(true);
            var lp = LexDp();
            if (tok == Qlx.LPAREN)
            {
                Next();
                var ps = CList<long>.Empty;
                if (tok != Qlx.RPAREN)
                    ps = ParseSqlValueList();
                Mustbe(Qlx.RPAREN);
                var n = cx.db.Signature(cx,ps);
                if (ic.Length == 0 || ic[ic.Length - 1] is not Ident pn)
                    throw new DBException("42000", "Signature");
                if (ic.Length == 1)
                {
                    var pr = cx.GetProcedure(LexDp(), pn.ident, n);
                    if (pr == null && (cx.db.objects[cx.role.dbobjects[pn.ident] ?? -1L]
                        ?? StandardDataType.Get(ttok)) is Domain ut && ut != Domain.Content)
                    {
                        cx.Add(ut);
                        var oi = ut.infos[cx.role.defpos];
                        var ml = oi?.methodInfos[pn.ident];
                        if (cx.db.objects[ml?[n] ?? -1L] is Method me)
                            return (QlValue)cx.Add(new SqlConstructor(lp, cx, me, ps));
                        if (ut.Length == ps.Length)
                            return (QlValue)cx.Add(new SqlDefaultConstructor(pn.uid, cx, ut, ps));
                        throw new DBException((ps.Length < ut.Length) ? "42109" : "42110", pn.ident);
                    }
                    if (pr == null) // must be a constructor for a type we know
                        throw new DBException("42107", ic.ident);
                    return (QlValue)cx.Add(new SqlProcedureCall(lp, cx, pr, ps));
                }
                else if (ic.Prefix(ic.Length - 2) is Ident pf)
                {
                    var vr = (QlValue)Identify(pf, il, Domain.Null);
                    cx.undefined += (lp, cx.sD);
                    return (QlValue)cx.Add(new SqlMethodCall(lp, cx, ic.sub?.ident ?? "", ps, vr));
                }
            }
            var ob = Identify(ic, il, xp);
            if (ob is not QlValue)
            {
                if (ob is TableColumn tc)
                    ob = new QlInstance(ic, cx, lp, tc.defpos);
                else if (cx.bindings.Contains(ob.defpos))
                    ob = new QlInstance(ic, cx, lp, ob.defpos);
                else
                    throw new DBException("42112", ic.ident);
            }
            return (QlValue)ob;
        }
        /// <summary>
        /// In graph dotted expressions we can't have more than one (top) unknown/unbound identifier:
        /// anything else will be field names (when eventually matched) or already known. 
        /// When we get to field subtypes this could be found at match time. 
        /// In LET expressions, the top can be a new unbound variable, 
        /// then all must be field names of known or to be matched types. 
        /// Everything on the right hand side must be field names of visible nodes or of the top identifier. 
        /// The above RHS considerations apply to everything in WHERE conditions or field constraints, 
        /// while the LHS rules apply to binding variables.
        /// So: during parsing, we limit the number of unbound fields that can occur in an identifier chain; 
        /// (SQL unbounded, GQL 0 or 1), and have an expectation of what sort of thing can match: 
        /// Content: Sql case: no expectations on identifier chains,
	    /// QlValue: a value of a column in a given(ambient or specified) row type: String -> SqlField -> TypedValue,
	    /// GqlLabel: a name of a given type or one of its subtypes: String -> Domain -> TSubType, 
	    /// TableColumn: a column name in a given type: String -> TChar, 
	    /// Domain: an ordering domain for a given row type: String -> TTypeSpec.
        /// </summary>
        /// <param name="ic">The identifier chain</param>
        /// <param name="il">The identifier chain as a list</param>
        /// <param name="xp">The ambient or expected type with SqlFunction.Mod to distinguish the above cases</param>
        /// <returns>The new object</returns>
        /// <exception cref="PEException"></exception>
        DBObject Identify(Ident ic, BList<Ident> il, Domain xp)
        {
            if (cx.user == null)
                throw new DBException("42105").Add(Qlx.USER);
            // See SourceIntro.docx section 6.1.2
            // we look up the identifier chain ic
            // and perform 6.1.2 (2) if we find anything
            var len = ic.Length;
            var (pa, sub) = cx.Lookup(ic);
            // pa is the object that was found, or null
            if (pa is not null && sub is null)
                return pa;
            var m = sub?.Length ?? 0;
            if (pa is QlInstance sc && cx.db.objects[sc.sPos] is TableColumn tc
                && cx.db.objects[tc.tabledefpos] is NodeType nt && sub != null)
            {
                cx.AddDefs(nt);
                var sb = Identify(sub, il - 0, xp);
                sb += (DBObject._From, pa.defpos);
                return sb;
            }
            if (pa is QlValue pp && pp.domain.kind == Qlx.PATH && sub is not null)
            {
                var pf = (GqlNode)Identify(sub, BList<Ident>.Empty, Domain.Content);
                return new SqlValueExpr(sub, cx, Qlx.PATH, pp, pf, Qlx.NO);
            }
            if (pa is QlValue sv && sv.domain.infos[cx.role.defpos] is ObInfo si && ic.sub is not null
                && cx.db.objects[si.names[ic.sub.ident]] is TableColumn tc1)
            {
                var co = new QlInstance(ic.sub, cx, sv.defpos, tc1);
                var nc = new SqlValueExpr(ic, cx, Qlx.DOT, sv, co, Qlx.NO);
                return nc;
            }
            if (pa is QlValue sv1 && sv1.domain is NodeType && sub is not null)
            {
                var co = new SqlField(sub, cx, -1, sv1.defpos, Domain.Content, sv1.defpos);
                return cx.Add(co);
            }
            if (cx.bindings.Contains(ic.uid)) // a binding or local variable
                return new QlValue(ic.uid, cx.obs[ic.uid]?.mem??BTree<long,object>.Empty+(ObInfo.Name,ic.ident));
            // if sub is non-zero there is a new chain to construct
            var nm = len - m;
            DBObject? ob = null;
            // nm is the position  of the first such in the chain ic
            // create the missing components if any (see 6.1.2 (1))
            for (var i = nm; i < len; i++)
                if (ic[i] is Ident c)
                {// the ident of the component to create
                    if (i == len - 1)
                    {
                        if (!cx.ParsingMatch) // flag as undefined unless we are parsing a MATCH
                        {
                            for (var b = cx.bindings.First(); b != null && ob is null; b = b.Next())
                                if (cx.obs[b.key()] is GqlNode g && g.name == ic[i-1]?.ident
                                    && g.domain.names[c.ident] is long pb && pb > 0
                                    && cx.db.objects[pb] is DBObject po)
                                    ob = new SqlField(ic.uid, c.ident, -1, b.key(), po.domain, b.key());
                            ob ??= new SqlReview(c, ic, il, cx, xp) ?? throw new PEException("PE1561");
                            cx.Add(ob);
                        }
                        else if (ic[i - 1] is Ident ip && cx.names[ip.ident] is long px && px>0)
                        {
                            var pb = cx.obs[px] ?? new QlValue(new Ident(ip.ident, px), il, cx, xp);
                            cx.Add(pb);
                            ob = new SqlField(ic.uid, c.ident, -1, pb.defpos, xp, pb.defpos);
                            cx.Add(ob);
                        }
                        else
                        {
                            ob = cx.Add(new QlValue(c, il, cx, 
                                lxr.tgg.HasFlag(TGParam.Type.Group)?new Domain(-1L,Qlx.ARRAY,xp):xp));
                            cx.bindings += (c.uid, ob.domain);
                        }
                        pa = ob;
                    }
                    else
                        new ForwardReference(c, il, cx);
                }
            if (pa == null)
                throw new PEException("PE1562");
            if (pa.defpos >= Transaction.Executables && ic.uid < Transaction.Executables)
            {
                var nv = pa.Relocate(ic.uid);
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
        /// <param name="xp">The  tye of the test expression</param>
        /// <returns>the Executable valueType of the parse</returns>
		Executable ParseLoopStatement(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var ls = new LoopStatement(LexDp(), mm);
            Next();
            ls += (NestedStatement.Stms, ParseStatementList(m));
            Mustbe(Qlx.END);
            Mustbe(Qlx.LOOP);
            var n = mm[Executable.Label] as string;
            if (tok == Qlx.Id && n != null && n == lxr.val.ToString())
                Next();
            return (Executable)cx.Add(ls);
        }
        /// <summary>
		/// While =		Label WHILE QlValue DO Statements END WHILE .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The tye of the test expression</param>
        /// <returns>the Executable valueType of the parse</returns>
        Executable ParseSqlWhile(params (long, object)[] m)
        {
            var xp = cx.result ?? Domain.Null;
            var mm = BTree<long, object>.New(m);
            var ws = new WhileStatement(LexDp(),mm);
            var old = cx; // new SaveContext(lxr, ExecuteStatus.Parse);
            Next();
            var s = ParseSqlValue((DBObject._Domain, Domain.Bool));
            cx.Add(s);
            ws += (ConditionalStatement.Search, s.defpos);
            Mustbe(Qlx.DO);
            ws += (cx, WhileStatement.What, ParseStatementList(m));
            Mustbe(Qlx.END);
            Mustbe(Qlx.WHILE);
            var n = mm[Executable.Label] as string;
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
        /// <param name="xp">The obs tye of the test expression</param>
        /// <returns>the Executable valueType of the parse</returns>
        Executable ParseRepeat(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var rs = new RepeatStatement(LexDp(),mm);
            Next();
            rs += (cx, WhileStatement.What, ParseStatements(m).defpos);
            Mustbe(Qlx.UNTIL);
            var s = ParseSqlValue((DBObject._Domain, Domain.Bool));
            cx.Add(s);
            rs += (ConditionalStatement.Search, s.defpos);
            Mustbe(Qlx.END);
            Mustbe(Qlx.REPEAT);
            var n = mm[Executable.Label] as string ?? "";
            if (tok == Qlx.Id && n != null && n == lxr.val.ToString())
                Next();
            cx.exec = rs;
            return (Executable)cx.Add(rs);
        }
        /// <summary>
        /// Parse a break or leave statement
        /// </summary>
        /// <returns>the Executable valueType of the parse</returns>
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
        /// <returns>the Executable valueType of the parse</returns>
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
        /// <returns>the Executable valueType of the parse</returns>
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
                    var sv = ParseSqlValue();
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
        /// <returns>the Executable valueType of the parse</returns>
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
        /// <returns>The Executable valueType of the parse</returns>
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
        /// <returns>The Executable valueType of the parse</returns>
        Executable ParseFetchStatement(params (long, object)[]m)
        {
            if (Match(Qlx.FETCH))
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
                where = ParseSqlValue((DBObject._Domain,Domain.Int));
            if (tok == Qlx.FROM)
                Next();
            var o = new Ident(this);
            Mustbe(Qlx.Id);
            var fs = new FetchStatement(dp,
                cx.Get(o, Domain.TableType) as SqlCursor
                ?? throw new DBException("34000", o.ToString()),
                how, where);
            Mustbe(Qlx.INTO);
            fs += (FetchStatement.Outs, ParseTargetList(m));
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
            cx.parseStart = LexDp();
            var op = cx.parse;
            var tn = cx.NameFor(trig.target) ?? throw new DBException("42000");
            var fn = new Ident(tn, LexDp());
            var tb = cx.db.objects[trig.target] as Table
                ?? throw new PEException("PE1562");
            tb = (Table)cx.Add(tb);
            cx.Add(tb.framing);
            var fm = tb.RowSets(fn, cx, tb, fn.uid, 0L);
            trig.from = fm.defpos;
            trig.dataType = fm;
            var tg = new Trigger(trig, cx.role);
            cx.Add(tg); // incomplete version for parsing
            var ro = cx.role;
            var oa = cx.anames;
            if (trig.oldTable != null)
            {
                var tt = (TransitionTable)cx.Add(new TransitionTable(trig.oldTable, true, cx, fm, tg));
                var nix = tt.defpos;
                cx.anames += (trig.oldTable.ident, nix);
                cx.db += tt;
            }
            if (trig.oldRow != null)
                cx.Add(new SqlOldRow(trig.oldRow, cx, fm));
            if (trig.newTable != null)
            {
                var tt = (TransitionTable)cx.Add(new TransitionTable(trig.newTable, true, cx, fm, tg));
                var nix = tt.defpos;
                cx.anames += (trig.newTable.ident, nix);
                cx.db += tt;
            }
            if (trig.newRow != null)
                cx.Add(new SqlNewRow(trig.newRow, cx, fm));
            var on = cx.names;
            for (var b = trig.dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string n)
                {
                    var px = p;
                    cx.names += (n, px);
                }
            QlValue? when = null;
            Executable? act;
            if (tok == Qlx.WHEN)
            {
                Next();
                when = ParseSqlValue((DBObject._Domain,Domain.Bool));
            }
            if (Match(Qlx.BEGIN, Qlx.LBRACE))
            {
                var st = tok;
                Next();
                Mustbe(Qlx.ATOMIC);
                var cs = new NestedStatement(LexDp(), cx, CList<CList<long>>.Empty);
                var et = (st == Qlx.BEGIN) ? Qlx.END : Qlx.RBRACE;
                if (Match(Qlx.TRANSACTION))
                    throw new DBException("22G01", "Nested transactions are not supported").ISO();
                var ss = CList<CList<long>>.Empty;
                var rr = CList<long>.Empty;
                while (tok != et && StartStatement() && ParseStatement() is Executable a)
                {
                    rr += cx.Add(a).defpos;
                    if (Match(Qlx.SEMICOLON, Qlx.NEXT))
                    {
                        ss += rr;
                        rr = CList<long>.Empty;
                        Next();
                    }
                }
                ss += rr;
                Mustbe(et);
                cs += (cx, NestedStatement.Stms, ss);
                act = cs;
            }
            else
                act = ParseStatement((Procedure.ProcBody,true)) ??
                    throw new DBException("42161", "statement");
            cx.Add(act);
            var r = (WhenPart)cx.Add(new WhenPart(LexDp(), when, act));
            trig.def = r.defpos;
            trig.framing = new Framing(cx, trig.nst);
            cx.Add(tg + (Trigger.Action, r.defpos));
            cx.parseStart = oldStart;
            cx.names = on;
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
            cx.parse = op;
            cx.anames = oa;
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
                            nt = new Ident(lxr.val.ToString(), tabl.uid);
                            Mustbe(Qlx.Id);
                            continue;
                        }
                        if (Match(Qlx.ROW))
                            Next();
                        if (nr != null)
                            throw new DBException("42143", "NEW ROW").Mix();
                        if (tok == Qlx.AS)
                            Next();
                        nr = new Ident(lxr.val.ToString(), tabl.uid);
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
            var cols = CList<long>.Empty;
            for (var b = cls?.First(); b is not null; b = b.Next())
                if (cx.names[b.value().ident] is long xi && xi > 0)
                    cols += xi;
            var np = cx.db.nextPos;
            var pt = new PTrigger(trig.ident, tb.defpos, (int)tgtype, cols,
                    or, nr, ot, nt,
                    new Ident(new string(lxr.input, st, lxr.input.Length - st),st),
                    nst, cx, np);
            var ix = LexDp();
            ParseTriggerDefinition(pt);
            pt.src = new Ident(new string(lxr.input, st, lxr.pos - st), ix);
            cx.parse = op;
            pt.framing = new Framing(cx, nst);
            cx.Add(pt);
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
        }
        /// <summary>
        /// Event = 	INSERT | DELETE | (UPDATE [ OF id { ',' id } ] ) .
        /// </summary>
        /// <param name="type">ref: the trigger tye</param>
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
                var lp = LexDp();
                Mustbe(Qlx.SOURCE);
                Mustbe(Qlx.TO);
                st = lxr.start;
                var qe = ParseStatementList();
                if (qe is not NestedStatement ne || ne.stms.Count!=1)
                    throw new DBException("42000");
                s = new Ident(new string(lxr.input, st, lxr.start - st), lp);
                cx.Add(new Modify("Source",lp, new QueryStatement(lp, ob, ne.stms[0]??CList<long>.Empty), s, cx.db.nextPos, cx));
                cx.obs -= ne.defpos;
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
        CList<long> ParseAlterDomain()
        {
            Next();
            var c = ParseIdent();
            var es = CList<long>.Empty;
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
        CList<long> ParseAlterDomainOp(Domain d)
        {
            var es = CList<long>.Empty;
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            if (tok == Qlx.SET)
            {
                Next();
                Mustbe(Qlx.DEFAULT);
                int st = lxr.start;
                var dv = ParseSqlValue((DBObject._Domain,Domain.For(d.kind)));
                string ds = new(lxr.input, st, lxr.start - st);
                if (cx.Add(new Edit(d, d.name, d + (Domain.Default, dv) + (Domain.DefaultString, ds),
                    cx.db.nextPos, cx)) is DBObject ef)
                    es += ef.defpos;
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
                var sc = ParseSqlValue((DBObject._Domain, Domain.Bool));
                string source = new(lxr.input, st, lxr.pos - st - 1);
                Mustbe(Qlx.RPAREN);
                if (cx.Add(new PCheck(d, id.ident, sc, source, nst, tr.nextPos, cx)) is DBObject co)
                    es += co.defpos;
            }
            else if (tok == Qlx.DROP)
            {
                Next();
                var dp = cx.db.Find(d)?.defpos ?? -1L;
                if (Match(Qlx.DEFAULT))
                {
                    Next();
                    if (cx.Add(new Edit(d, d.name, d, tr.nextPos, cx)) is DBObject ee)
                        es += ee.defpos;
                }
                else if (StartMetadata(Qlx.DOMAIN) || Match(Qlx.ADD, Qlx.DROP))
                {
                    if (tr.role.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Add(Qlx.METADATA).Mix();
                    var m = ParseMetadata(Qlx.DOMAIN);
                    if (cx.Add(new PMetadata(d.name, -1, d, m, dp)) is DBObject me)
                        es += me.defpos;
                }
                else
                {
                    Mustbe(Qlx.CONSTRAINT);
                    var n = new Ident(this);
                    Mustbe(Qlx.Id);
                    Drop.DropAction s = ParseDropAction();
                    var ch = (Check?)cx.GetObject(n.ident) ?? throw new DBException("42135", n.ident);
                    if (cx.Add(new Drop1(ch.defpos, s, tr.nextPos)) is DBObject ed)
                        es += ed.defpos;
                }
            }
            else if (StartMetadata(Qlx.DOMAIN) || Match(Qlx.ADD, Qlx.DROP))
            {
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Add(Qlx.METADATA).Mix();
                if (cx.Add(new PMetadata(d.name, 0, d, ParseMetadata(Qlx.DOMAIN),tr.nextPos)) is DBObject eo)
                    es += eo.defpos;
            }
            else
            {
                Mustbe(Qlx.TYPE);
                var dt = ParseDataType();
                if (cx.Add(new Edit(d, d.name, dt, tr.nextPos, cx)) is DBObject ee)
                    es += ee.defpos;
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
                var se = ParseSqlValue((DBObject._Domain, Domain.Bool));
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
                    var gr = ParseGenerationRule(tc.defpos, tb, tc.domain) + (DBObject._Framing, new Framing(cx, nst));
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
                            || cx.db.objects[oi?.methodInfos?[nm.ident]?[Database.Signature(mn.ins)] ?? -1L] is not Method mt)
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
                    var c = new PColumn2(tp, nm.ident, tp.Length, dm, dm.defaultString, dm.defaultValue,
                            false, GenerationRule.None, tr.nextPos, cx);
                    cx.Add(c);
                    var tc = (TableColumn)(cx.obs[c.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN));
                    if (md != TMetadata.Empty)
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
    Qlx.LABELS, Qlx.LAST_VALUE, Qlx.LAST_DATA, Qlx.PARTITION, Qlx.UNNEST,
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
            if (Match(Qlx.TABLE, Qlx.ROW, Qlx.TYPE, Qlx.LPAREN))// anonymous row tye
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
                    //ut.Defs(cx);
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
        /// <returns>the obs tye</returns>
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
            else if (Match(Qlx.NUMERIC, Qlx.DECIMAL, Qlx.DEC, Qlx.FLOAT, Qlx.FLOAT16, Qlx.FLOAT32, 
                Qlx.FLOAT64, Qlx.FLOAT128, Qlx.FLOAT256, Qlx.REAL, Qlx.DOUBLE))
            {
                r = r0 = Domain.Real;
                if (tok == Qlx.DOUBLE)
                    Mustbe(Qlx.PRECISION);
                Next();
                r = ParsePrecScale(r);
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
            else if (Match(Qlx.DATE, Qlx.TIME, Qlx.TIMESTAMP, Qlx.INTERVAL, Qlx.DATETIME))
            {
                Domain dr = r0 = Domain.Timestamp;
                switch (tok)
                {
                    case Qlx.DATE: dr = Domain.Date; break;
                    case Qlx.TIME: dr = Domain.Timespan; break;
                    case Qlx.DATETIME:
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
                return Domain.Null; // not a standard tye
            if (r == r0)
                return r0; // completely standard
            // see if we know this tye
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
        /// <returns>the modified obs tye</returns>
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
        /// Handle ROW tye or TABLE tye in Type specification.
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
            var lp = LexDp();
            var ns = BList<(Ident, Domain, TMetadata)>.Empty;
            // sm is also used for the RestView case
            var sm = Names.Empty;
            for (var b = under.First(); b != null; b = b.Next())
            {
                var hm = (b.key() as UDType)?.HierarchyCols(cx) ?? Names.Empty;
                sm += hm;
                for (var c = hm.First(); c != null; c = c.Next())
                    if (c.value() is long mp && cx.obs[mp] is DBObject om
                        && om.infos[cx.role.defpos] is ObInfo mm)
                    {
                        var im = new Ident(c.key(), mp);
                        ns += (im, om.domain, mm.metadata);
                    }
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
                dt = (Domain)cx.Add(new Table(lp, m));
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
            var rt = CList<long>.Empty;
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
                    sm += (nm.ident, pc.defpos);
                    rt += pc.defpos;
                    var cix = pc.defpos;
                    cx.names += (pn.ident, cix);
                }
                else if (pn != null && pn.ident != "")
                {
                    var se = new SqlElement(nm, BList<Ident>.Empty, cx, pn, dm);
                    cx.Add(se);
                    ms += (se.defpos, dm);
                    sm += (nm.ident, se.defpos);
                    rt += se.defpos;
                    cx.names += (pn.ident, pn.uid);
                }
                else // RestView
                {
                    var sp = nm.uid;
                    rt += sp;
                    ms += (sp, dm);
                    sm += (nm.ident, sp);
                    ls += (sp, nm.ident);
                }
            }
            dt = cx.obs[dt.defpos] as Domain ?? throw new DBException("42105").Add(Qlx.DOMAIN);
            oi += (ObInfo._Names, sm);
            var r = (Domain)dt.New(st, new BTree<long, object>(Domain.Kind,k)
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
                r = r + (ObInfo._Names, sm) + (RestView.NamesMap, ls);
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
        /// <param name="pn">The parent object being defined (except for anonymous row tye)</param>
        /// <returns>The RowTypeColumn</returns>
		(Ident, Domain, TMetadata) ParseMember(Ident? pn)
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
                var dv = ParseSqlValue((DBObject._Domain,dm));
                var ds = new string(lxr.input, st, lxr.start - st);
                dm = dm + (Domain.Default, dv) + (Domain.DefaultString, ds);
            }
            if (Match(Qlx.COLLATE))
                dm += (Domain.Culture, ParseCollate());
            var md = TMetadata.Empty;
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
        /// <returns>the updated obs tye</returns>
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
        /// <returns>the updated obs tye</returns>
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
                if (cx.parse.HasFlag(ExecuteStatus.Obey))
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
            else if (Match(Qlx.REFERENCING))
            {
                cx.conn.refIdsToPos = true;
                Next();
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
                            type = new Ident(nid, 0L);
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
                        ob = cx.GetProcedure(LexDp(), n.ident, a);
                    if (ob == null)
                        throw new DBException("42135", n.ident).Mix();
                    Mustbe(Qlx.TO);
                    var nm = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (cx.parse.HasFlag(ExecuteStatus.Obey) && cx.db is Transaction tr)
                    {
                        var pc = new Change(ob.defpos, nm.ident, tr.nextPos, cx);
                        cx.Add(pc);
                    }
                }
            }
        }
		internal QueryStatement ParseSelectStatement(params (long, object)[] m)
        {
            var on = cx.names;
            cx.names = cx.anames;
            var mm = BTree<long, object>.New(m);
            var xp = (Domain)(mm[DBObject._Domain] ?? Domain.Null);
            var lp = LexDp();
            RowSet left, right;
            m = Add(m, (DBObject.Scope, lp));
            left = ParseRowSetTerm(m);
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
                right = ParseRowSetTerm(m);
                left = new CompositeRowSet(cx.GetUid(), cx, xp, left, right, md == Qlx.DISTINCT, op);
                if (md == Qlx.DISTINCT)
                    left += (RowSet.Distinct, true);
            }
            var ois = left.ordSpec;
            var nis = ParseOrderClause(left, ois, m);
            left = (RowSet)(cx.obs[left.defpos] ?? throw new PEException("PE20701"));
            if (ois.CompareTo(nis) != 0)
                left = left.Sort(cx, nis, false);
            if (Match(Qlx.FETCH, Qlx.FIRST))
            {
                var n = FetchFirstClause();
                if (n > 0)
                    left = new RowSetSection(cx, left, 0, n);
            }
            var qs = new QueryStatement(cx.GetUid(),
                mm+(QueryStatement.Result,left.defpos)+(DBObject._Domain,left));
            cx.Add(qs);
            cx.result = left;
            cx.names += on;
            return qs;
        }
        internal int FetchFirstClause()
        {
            if (Match(Qlx.FETCH))
                Next();
            var n = 1;
            Mustbe(Qlx.FIRST);
            var o = lxr.val;
            Match(Qlx.ROW);
            if (tok == Qlx.INTEGERLITERAL)
            {
                n = o.ToInt() ?? 1;
                Next();
                Match(Qlx.ROWS);
                Mustbe(Qlx.ROWS);
            }
            else
                Mustbe(Qlx.ROW);
            Match(Qlx.ONLY);
            Mustbe(Qlx.ONLY);
            return n;
        }
        /// <summary>
		/// QueryTerm = QueryPrimary | QueryTerm INTERSECT [ ALL | DISTINCT ] QueryPrimary .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification 
        /// in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected obs tye</param>
        /// <returns>the RowSet</returns>
		RowSet ParseRowSetTerm(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Null;
            RowSet left, right;
            left = ParseQueryPrimary(m);
            while (Match(Qlx.INTERSECT))
            {
                var lp = LexDp();
                Next();
                Qlx d = Qlx.DISTINCT;
                if (Match(Qlx.ALL, Qlx.DISTINCT))
                {
                    d = tok;
                    Next();
                }
                right = ParseQueryPrimary(m);
                left = new CompositeRowSet(lp, cx, xp, left, right, d == Qlx.DISTINCT, Qlx.INTERSECT);
                if (d == Qlx.DISTINCT)
                    left += (RowSet.Distinct, true);
            }
            return (RowSet)cx.Add(left);
        }
		RowSet ParseQueryPrimary(params (long, object)[] m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Null;
            var lp = LexDp();
            RowSet qs;
            switch (tok)
            {
                case Qlx.LPAREN:
                    Next();
                    qs = ParseSelectStatement(m).domain as RowSet??throw new DBException("42000");
                    Mustbe(Qlx.RPAREN);
                    break;
                case Qlx.RETURN:
                case Qlx.SELECT: // query specification
                    qs = ParseQueryExpression(m);
                    break;
                case Qlx.WITH:
                case Qlx.MATCH:
                    ParseMatchStatement();
                    qs = (RowSet)(cx.result ?? new TrivialRowSet(cx, Domain.Row));
                    break;
                case Qlx.VALUES:
                    var v = CList<long>.Empty;
                    Qlx sep = Qlx.COMMA;
                    while (sep == Qlx.COMMA)
                    {
                        Next();
                        var llp = LexDp();
                        Mustbe(Qlx.LPAREN);
                        var x = ParseSqlValueList(m);
                        Mustbe(Qlx.RPAREN);
                        v += cx.Add(new SqlRow(llp, cx, xp, x)).defpos;
                        sep = tok;
                    }
                    qs = (RowSet)cx.Add(new SqlRowSet(lp, cx, xp, v));
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
                    var ap = (long)(mm[DBObject.Scope]?? 0L);
                    qs = tb.RowSets(ic, cx, tb, ic.uid, ap, Grant.Privilege.Select);
                    break;
                default:
                    throw new DBException("42127").Mix();
            }
            return (RowSet)cx.Add(qs);
        }
		Domain ParseOrderClause(RowSet rs, Domain ord, params (long, object)[]m)
        {
            if (tok != Qlx.ORDER)
                return ord;
            //cx.IncSD(new Ident(this)); // order by columns will be in the foregoing cursor spec
            var on = cx.names;
            var lp = lxr.Position;
            cx.defs += (lp,cx.names);
            Next();
            Mustbe(Qlx.BY);
            var bs = BList<DBObject>.Empty;
            for (var b = ord.rowType.First(); b != null; b = b.Next())
                bs += cx._Ob(b.value()) ?? SqlNull.Value;
            var lb = Match(Qlx.LPAREN);
            if (lb)
                Next();
            var mi = Add(m, (DBObject._Domain, Domain.Content));
            var oi = (QlValue?)cx._Ob(ParseOrderItem(mi))??throw new DBException("42000");
            bs += Simplify(cx,oi,rs);
            while (tok == Qlx.COMMA)
            {
                Next();
                oi = (QlValue?)cx._Ob(ParseOrderItem(mi)) ?? throw new DBException("42000");
                bs += Simplify(cx,oi,rs);
            }
            if (lb)
                Mustbe(Qlx.RPAREN);
            //cx.DecSD();
            cx.defs -= lp;
            cx.names = on;
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
                if (cx._Ob(b.value()) is QlValue e && oi._MatchExpr(cx, e, rs))
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
                bs += cx._Ob(b.value()) ?? SqlNull.Value;
            bs += cx._Ob(ParseOrderItem((NestedStatement.WfOK,false))) ?? SqlNull.Value;
            while (tok == Qlx.COMMA)
            {
                Next();
                bs += cx._Ob(ParseOrderItem((NestedStatement.WfOK, false))) ?? SqlNull.Value;
            }
            return new Domain(cx.GetUid(), cx, Qlx.ROW, bs, bs.Length) - Domain.Aggs;
        }
        /// <summary>
		/// CList<long> =  TypedValue [ ASC | DESC ] [ NULLS ( FIRST | LAST )] .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>an OrderItem</returns>
		internal long ParseOrderItem(params (long, object)[]m)
        {
            var v = ParseSqlValue(m);
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
        /// <param name="t">the expected obs tye</param>
        /// <returns>The RowSetSpec</returns>
		RowSet ParseQueryExpression(params (long, object)[] m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Content;
            var id = new Ident(this);
            Mustbe(Qlx.SELECT, Qlx.RETURN);
            var on = cx.names;
            var lp = LexDp();
            var d = ParseDistinctClause();
            var dm = ParseSelectList(id.uid, Add(m, (DBObject.Scope, lp)));
            cx.Add(dm);
            cx.names += on;
            RowSet te = ParseTableExpression(id.uid, (DBObject._Domain,dm), (DBObject.Scope,id.uid));
            if (Match(Qlx.FOR))
            {
                Next();
                Mustbe(Qlx.UPDATE);
            }
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
        /// <param name="xp">the expected valueType tye, or Domain.Content</param>
        /// <returns>a "Domain" whose rowtype may contain SqlReviews for local columns</returns> 
		Domain ParseSelectList(long dp, params (long, object)[]m)
        {
            QlValue v;
            var j = 0;
            var vs = BList<DBObject>.Empty;
            v = ParseSelectItem(dp, j++, m);
            if (v is not null) // star items do not have a value to add at this stage
                vs += v;
            while (tok == Qlx.COMMA)
            {
                Next();
                v = ParseSelectItem(dp, j++, m);
                if (v is not null)
                    vs += v;
            }
            return (Domain)cx.Add(new Domain(cx.GetUid(), cx, Qlx.TABLE, vs, vs.Length));
        }
        QlValue ParseSelectItem(long q, int pos, params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var dm = mm[DBObject._Domain] as Domain??Domain.Content;
            if (dm.rowType.Length > pos)
                dm = dm.representation[dm[pos] ?? -1L] ?? throw new PEException("PE1675");
            return ParseSelectItem(q, Add(m,(DBObject._Domain,dm)));
        }
        /// <summary>
		/// SelectItem = * | (Scalar [AS id ]) | (RowValue [.*] [AS IdList]) .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <param name="t">the expected obs tye for the query</param>
        /// <param name="pos">The position in the SelectList</param>
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        QlValue ParseSelectItem(long q, params (long, object)[]m)
        {
            Ident alias;
            QlValue v;
            if (tok == Qlx.TIMES)
            {
                var lp = LexDp();
                Next();
                v = new SqlStar(lp, cx, -1L);
            }
            else
            {
                v = ParseSqlValue(Add(m,(NestedStatement.WfOK,true)));
                if (v is SqlReview)
                    cx.undefined += (v.defpos,cx.sD);
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
                if (cx.Lookup(alias).Item1 is QlValue ov)
                {
                    var v0 = nv;
                    nv = (QlValue)nv.Relocate(ov.defpos);
                    cx.Replace(v0, nv);
                }
                else
                    cx.Add(nv);
                cx.Add(alias.ident, v);
                cx.Add(nv);
                Mustbe(Qlx.Id);
                v = nv;
            }
            else
                cx.Add(v);
            return v;
        }
        /// <summary>
		/// TableExpression = FromClause [ WhereClause ] [ GroupByClause ] [ HavingClause ] [WindowClause] .
        /// The ParseFromClause is called before this
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="t">the expected obs tye</param>
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        /// <returns>The TableExpression</returns>
		SelectRowSet ParseTableExpression(long lp, params(long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var d = mm[DBObject._Domain] as Domain ?? Domain.TableType;
            RowSet fm = ParseFromClause(lp, m);
            if (cx.obs[d.defpos] is not Domain dm)
                throw new PEException("PE50310");
            var mf = fm.mem;
            for (var b = fm.SourceProps.First(); b is not null; b = b.Next())
                if (b.value() is long p)
                    mf -= p;
            mf += (RowSet._Source, fm.defpos);
            var vs = BList<DBObject>.Empty;
            var ns = dm.names;
            var ap = (long)(mm[DBObject.Scope] ?? 0L);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue sv
                    && sv.Verify(cx))
                {
                    (var ls, mf) = sv.Resolve(cx, fm, mf, ap);
                    vs += ls;
                    if (sv.NameFor(cx) is string sn)
                        ns += (sn, p);
                } 
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var ob = cx.obs[k];
                if (ob is QlValue sv && k>=ap && fm is RowSet rf)
                    sv.Resolve(cx, rf, BTree<long, object>.Empty, ap);
                else if (ob?.id is Ident ic && ob is ForwardReference fr
                    && cx.Lookup(ic.ident) is DBObject tt)
                {
                    if (tt is RowSet nb)
                    {
                        for (var c = fr.subs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is QlValue su && su.name is not null
                                && cx.obs[nb.names[su.name]] is QlValue ru)
                            {
                                cx.Replace(su, ru);
                                cx.undefined -= su.defpos;
                            }
                        cx.Replace(ob, nb);
                        cx.undefined -= k;
                        cx.NowTry();
                    }
                    for (var c = fr.subs.First(); c != null; c = c.Next())
                        if (cx.obs[c.key()] is Domain os && os.id != null)
                        {
                            var (iiy, _) = cx.Lookup(os.id);
                            if (iiy is Domain oy)
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
                {
                    if (v.NameFor(cx) is string sn && dm.names.Contains(sn))
                        continue;
                    vs += v;
                } 
            dm = new Domain(dm.defpos, cx, Qlx.TABLE, vs, ds);
            cx.Add(dm);
            fm = (RowSet)(cx.obs[fm.defpos] ?? throw new PEException("PE2001"));
            return ParseSelectRowSet(new SelectRowSet(lp, cx, dm, fm, mf));
        }
        SelectRowSet ParseSelectRowSet(SelectRowSet r)
        {
            var m = BTree<long, object>.Empty;
            var gc = Domain.Null;
            var gg = new GroupSpecification(cx.GetUid(), BTree<long, object>.Empty);
            if (r.aggs != CTree<long, bool>.Empty)
                m += (Domain.Aggs, r.aggs);
            if (tok == Qlx.WHERE) // oddly, these clauses are part of GQL syntax
            {
                var wc = ParseWhereClause() ?? throw new DBException("42161", "condition");
                var wh = new BTree<long, object>(RowSet._Where, wc);
                m += wh;
                ((RowSet)(cx.obs[r.source] ?? throw new PEException("PE2002"))).Apply(wh, cx);
            }
            // tolerate a group specification in non-aggregating queries
            // e.g. where SUM etc are just for path binding arrays
            if (tok == Qlx.GROUP)
            {
                if (ParseGroupClause(r) is GroupSpecification gs)
                {
                    gc = gs.Cols(cx, r);
                    gg = gs;
                    if (r.aggs.Count != 0)
                    {
                        m += (RowSet.Group, gs.defpos);
                        m += (RowSet.GroupCols, gc);
                    }
                }
            }
            if (tok == Qlx.HAVING)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "HAVING");
                if (r.aggs.Count != 0)
                    m += (RowSet.Having, ParseHavingClause(r));
                else
                {
                    var wh = r.where + (CTree<long, bool>)(m[RowSet._Where] ?? CTree<long, bool>.Empty)
                        + ParseHavingClause(r);
                    m += (RowSet._Where, wh);
                }
            }
            // a following order and page statement can handle <order by clause, offset clause and limit clause
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
                    gg += (GroupSpecification.Sets, new CList<long>(gi.defpos));
                    cx.Add(gg);
                    m += (RowSet.Groupings, new CList<long>(gi.defpos));
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
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        /// <returns>The resolved select domain and table expression</returns>
		RowSet ParseFromClause(long dp, params (long, object)[]m)
        {
            if (tok == Qlx.FROM)
            {
                Next();
                return (RowSet)cx.Add(ParseTableReference(dp, m));
            }
            else if (cx.result is BindingRowSet bs)
                return bs;
            else
            {
                var mm = BTree<long, object>.New(m);
                var dm = mm[DBObject._Domain] as Domain ?? Domain.TableType;
                return new TrivialRowSet(cx, dm);
            }
        }
        /// <summary>
		/// TableReference = TableFactor Alias | JoinedTable .
        /// </summary>
        /// <param name="st">the future selectrowset defining position</param>
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        /// <returns>and the new table reference item</returns>
        RowSet ParseTableReference(long st, params (long, object)[]m)
        {
            RowSet a = ParseTableReferenceItem(st, m);
            cx.Add(a);
            while (Match(Qlx.COMMA, Qlx.CROSS, Qlx.NATURAL, Qlx.JOIN, Qlx.INNER, Qlx.LEFT, Qlx.RIGHT, Qlx.FULL, Qlx.ON))
            {
                var lp = LexDp();
                a = ParseJoinPart(lp, a.Apply(new BTree<long, object>(DBObject._From, lp), cx), m);
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
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        /// <returns>the rowset for this table reference</returns>
		RowSet ParseTableReferenceItem(long st, params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var dm = mm[DBObject._Domain] as Domain ?? Domain.TableType;
            RowSet rf;
            var lp = LexDp();
            if (Match(Qlx.ROWS)) // Pyrrho specific
            {
                Next();
                Mustbe(Qlx.LPAREN);
                var v = ParseSqlValue((DBObject._Domain,Domain.Position));
                QlValue w = SqlNull.Value;
                if (tok == Qlx.COMMA)
                {
                    Next();
                    w = ParseSqlValue((DBObject._Domain, Domain.Position));
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
                    rs = new LogRowColRowSet(lp, cx,
                        Domain.Int.Coerce(cx, v.Eval(cx)).ToLong() ?? -1L,
                        Domain.Int.Coerce(cx, w.Eval(cx)).ToLong() ?? -1L);
                else
                    rs = new LogRowsRowSet(lp, cx,
                        Domain.Int.Coerce(cx, v.Eval(cx)).ToLong() ?? -1L);
                cx.Add(rs);
                rf = rs;
            }
            // this syntax should allow multiple array/multiset arguments and ORDINALITY
            else if (Match(Qlx.UNNEST))
            {
                Next();
                Mustbe(Qlx.LPAREN);
                QlValue sv = ParseSqlValue();
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
                var cp = LexDp();
                Mustbe(Qlx.LPAREN); // SQL2003-2 7.6 required before table valued function
                Ident n = new(this);
                Mustbe(Qlx.Id);
                var r = CList<long>.Empty;
                Mustbe(Qlx.LPAREN);
                if (tok != Qlx.RPAREN)
                    for (; ; )
                    {
                        r += cx.Add(ParseSqlValue()).defpos;
                        if (tok == Qlx.RPAREN)
                            break;
                        Mustbe(Qlx.COMMA);
                    }
                Next();
                Mustbe(Qlx.RPAREN); // another: see above
                var proc = cx.GetProcedure(LexDp(), n.ident, cx.db.Signature(cx, r))
                    ?? throw new DBException("42108", n.ident);
                ParseCorrelation(proc.domain);
                var ca = new SqlProcedureCall(cp, cx, proc, r);
                cx.Add(ca);
                var ap = (long)(mm[DBObject.Scope] ?? -1L);
                rf = ca.RowSets(n, cx, proc.domain, n.uid, ap);
            }
            else if (Match(Qlx.LPAREN,Qlx.LBRACE)) // subquery
            {
                var mt = tok;
                Next();
                var on = cx.names;
                var sp = lxr.Position;
                cx.defs += (sp,cx.names);
                var sl = ParseStatementList((DBObject._Domain, Domain.TableType),
                    (DBObject.Scope, sp));
                rf = ((sl is NestedStatement ns && ns.LastOf(cx) is Executable ne) ? 
                     (ne.domain as RowSet)??(cx.obs[(ne as QueryStatement)?.result??-1L] as RowSet):
                    (sl.domain as RowSet)??  (cx.obs[(long)(sl.mem[QueryStatement.Result]??-1L)] as RowSet))
                    ?? throw new DBException("42000");
                cx.Add(rf);
                cx.Add(sl + (QueryStatement.Result, rf.defpos));
                cx.defs -= sp;
                cx.names = on;
                Mustbe((mt==Qlx.LPAREN)?Qlx.RPAREN:Qlx.RBRACE);
                if (tok == Qlx.Id)
                {
                    var a = lxr.val.ToString();
                    var rx = rf.defpos;
                    var ia = new Ident(a, rx);
                    if (cx.Lookup(ia).Item1 is QlValue lv
                            && (lv.domain.kind == Qlx.CONTENT || lv is SqlReview)
                            && lv.name != null
                            && rf.names.Contains(lv.name) && cx.obs[rf.names[lv.name]] is QlValue uv)
                        {
                            var nv = (Domain)uv.Relocate(lv.defpos);
                            cx.Replace(lv, nv);
                            cx.Replace(uv, nv);
                        }
                    cx.Add(ia.ident, rf);
                    Next();
                }
                cx.names += on;
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
                    Qlx.RIGHT, Qlx.FULL,Qlx.VALUES, Qlx.ON, Qlx.FETCH);
                if (tok == Qlx.Id || tok == Qlx.AS)
                {
                    if (tok == Qlx.AS)
                        Next();
                    a = lxr.val.ToString();
                    Mustbe(Qlx.Id);
                }
                var ob = (cx.GetObject(ic.ident) ?? cx.Lookup(ic).Item1);
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
                    for (var b = cx.undefined.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()] is SqlReview sr &&
                            cx.obs[cx.defs[f.defpos]?[sr.name ?? ""] ?? -1L] is QlValue qv)
                        {
                            cx.undefined -= sr.defpos;
                            cx.Replace(sr, qv);
                            cx.NowTry();
                        }
                }
                else
                {
                    cx.DefineForward(ic.ident);
                    cx.DefineForward(a);
                    var ap = (long)(mm[DBObject.Scope] ?? 0L);
                    rf = _From(ic, ob, dm, Grant.Privilege.Select, ap, a);
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
            }
            return rf;
        }
        /// <summary>
        /// We are about to call the From constructor, which may
        /// Resolve undefined expressions in the SelectList 
        /// </summary>
        /// <param name="dp">The occurrence of this table reference</param>
        /// <param name="ob">The table or view referenced</param>
        /// <param name="q">The expected valueType for the enclosing query</param>
        /// <returns></returns>
        RowSet _From(Ident ic, DBObject ob, Domain dm, Grant.Privilege pr, long ap, string? a = null)
        {
            var dp = ic.uid;
            if (ob != null)
            {
                if (ob is View ov)
                {
                    var ut = cx.db.objects[(ov as RestView)?.usingTable ?? -1L] as Table;
                    RowSet? ur = (ut is null) ? null : _From(new Ident(ut.NameFor(cx), cx.GetUid()), ut,
                        dm, Grant.AllPrivileges, ap);
                    ob = ov.Instance(dp, cx, ur);
                }
                ob._Add(cx);
            }
            if (ob == null)
                throw new PEException("PE2003");
            var ff = ob.RowSets(ic, cx, dm, ic.uid, ap, pr, a);
            cx.Add(ff.names);
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                if (b.key() is long k && cx.obs[k] is DBObject uo)
                {
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
                                    cx.NowTry();
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
                    t1 = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
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
                    t1 = ParseSqlValueTerm((DBObject._Domain, Domain.UnionDate));
                    Mustbe(Qlx.AND);
                    t2 = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
                    break;
                case Qlx.FROM:
                    Next();
                    t1 = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
                    Mustbe(Qlx.TO);
                    t2 = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
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
                var cs = CList<long>.Empty;
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
                            var cp = ib.value().uid;
                            var cd = xp.representation[oc] ?? throw new PEException("PE47169");
                            cs += cp;
                            rs += (cp, cd);
                        }
                    xp = (cs.Length==0)?Domain.TableType:new Domain(cx.GetUid(), cx, Qlx.TABLE, rs, cs);
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
        /// <param name="ap">ambient limit in names: e.g. 0L allow all, lxr.Position allow none</param>
        /// <returns>the updated query</returns>
        RowSet ParseJoinPart(long dp, RowSet fi, params (long, object)[]m)
        {
            var left = fi;
            Qlx jkind;
            RowSet right;
            var jm = BTree<long, object>.Empty;
            if (Match(Qlx.COMMA))
            {
                jkind = Qlx.CROSS;
                Next();
                right = ParseTableReferenceItem(dp, m);
            }
            else if (Match(Qlx.CROSS))
            {
                jkind = Qlx.CROSS;
                Next();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, m);
            }
            else if (Match(Qlx.NATURAL))
            {
                jm += (JoinRowSet.Natural, tok);
                Next();
                jkind = ParseJoinType();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, m);
            }
            else
            {
                jkind = ParseJoinType();
                Mustbe(Qlx.JOIN);
                right = ParseTableReferenceItem(dp, m);
                if (tok == Qlx.USING)
                {
                    jm += (JoinRowSet.Natural, tok);
                    Next();
                    var ns = ParseIDList();
                    var cs = BTree<long, long?>.Empty;
                    for (var b = ns.First(); b != null; b = b.Next())
                    {
                        var lo = (left.alias is not null) ? cx.Lookup(left.alias) : cx.Lookup(left.name ?? "");
                        var ro = (right.alias is not null) ? cx.Lookup(right.alias) : cx.Lookup(right.name ?? "");
                        if (ro is not null && lo is not null)
                            cs += (ro.defpos, lo.defpos);
                    }
                    jm += (JoinRowSet.JoinUsing, cs);
                }
                else
                {
                    Mustbe(Qlx.ON);
                    var oc = ParseSqlValue((DBObject._Domain,Domain.Bool)).Disjoin(cx);
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
                        jm += (JoinRowSet.JoinCond, oc);
                    if (on != BTree<long, long?>.Empty)
                        jm += (JoinRowSet.OnCond, on);
                    if (wh != CTree<long, bool>.Empty)
                        jm += (RowSet._Where, wh);
                }
            }
            left = (RowSet)(cx.obs[left.defpos] ?? throw new PEException("PE207030"));
            right = (RowSet)(cx.obs[right.defpos] ?? throw new PEException("PE207031"));
            var r = new JoinRowSet(dp, cx, left, jkind, right, jm);
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
            var lp = LexDp();
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
            GroupSpecification r = new(lp, cx, BTree<long, object>.Empty
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
                r += (GroupSpecification.Sets, new CList<long>(gn.defpos));
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
                var c = ParseSqlValue();
                for (var b = rs.rowType.First(); b != null; b = b.Next())
                    if (cx.obs[b.value()] is QlValue sb && sb._MatchExpr(cx, c, rs))
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
                var lp = LexDp();
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
                + (Grouping.Members, new CTree<long, int>(cn.uid, 0)));
            var i = 1;
            while (Match(Qlx.COMMA))
            {
                cn = ParseIdent();
                t += (Grouping.Members, t.members + (cn.uid, i++));
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
            r = ParseSqlValueDisjunct((DBObject._Domain, Domain.Bool),(DBObject.HavingDom,dm));
            if (tok != Qlx.OR)
                return r;
            var left = Disjoin(r);
            while (tok == Qlx.OR)
            {
                var lp = LexDp();
                Next();
                left = (QlValue)cx.Add(new SqlValueExpr(lp, cx, Qlx.OR, left,
                    Disjoin(ParseSqlValueDisjunct((DBObject._Domain, Domain.Bool), (DBObject.HavingDom, dm))), Qlx.NO));
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
            if (!Match(Qlx.WHERE))
                return null;
            Next();
            return _ParseWhereClause();
        }
        CTree<long, bool>? _ParseWhereClause()
        { 
            cx.done = ObTree.Empty;
            var left = ParseSqlValue((DBObject._Domain,Domain.Bool));
            return cx.FixTlb(Disjoin(left.defpos)); 
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
            var lp = LexDp();
            Next();
            if (Match(Qlx.INTO))
                Next();
            else return ParseInsertGraph();
            Ident ic = new(this);
            //  cx.IncSD(ic);
            var on = cx.names;
            cx.defs += (lp, cx.names);
            cx.names = cx.anames;
            var fm = ParseTableReference(ic.uid, (DBObject._Domain, Domain.TableType), (DBObject.Scope,lp));
            cx.Add(fm);
            if (fm is not TableRowSet && !cx.names.Contains(ic.ident))
                cx.Add(ic.ident, fm);
           // cx.AddDefs(ic, fm);
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
            cs ??= (fm.Length==0)?Domain.Row:new Domain(cx.GetUid(), cx, Qlx.ROW, fm.representation, fm.rowType, fm.Length);
            var vp = cx.GetUid();
            if (Match(Qlx.DEFAULT))
            {
                Next();
                Mustbe(Qlx.VALUES);
                sv = SqlNull.Value;
            }
            else
                // care: we might have e.g. a subquery here
                sv = ParseSqlValue((DBObject._Domain,fm));
            if (sv is SqlRow) // tolerate a single value without the VALUES keyword
                sv = new SqlRowArray(vp, cx, sv.domain, new CList<long>(sv.defpos));
            var sce = sv.RowSetFor(vp, cx, fm.rowType, fm.representation) + (cx, RowSet.RSTargets, fm.rsTargets)
                + (RowSet.Asserts, RowSet.Assertions.AssignTarget);
            cx._Add(sce);
            SqlInsert s = new(lp, fm, sce.defpos, cs);
            cx.Add(s);
            cx.result = cx.obs[s.defpos] as RowSet;
            if (Match(Qlx.SECURITY))
            {
                Next();
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105").Add(Qlx.OWNER);
                s += (DBObject.Classification, MustBeLevel());
            }
            // cx.DecSD();
            cx.defs -= lp;
            cx.names += on;
            if (cx.parse.HasFlag(ExecuteStatus.Obey|ExecuteStatus.Http) && cx.db is Transaction tr)
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
            var lp = LexDp();
            var cc = Match(Qlx.DETACH);
            if (Match(Qlx.DETACH, Qlx.NODETACH))
                Next();
            Mustbe(Qlx.DELETE);
            if (!Match(Qlx.WITH, Qlx.FROM))
            {
                var de = DeleteNode(cc);
                if (!Match(Qlx.COMMA))
                    return de;
                var ds = new CList<long>(de.defpos);
                while (Match(Qlx.COMMA))
                {
                    Next();
                    ds += DeleteNode(cc).defpos;
                }
                return (Executable)cx.Add(new AccessingStatement(cx.GetUid(),
                    new BTree<long, object>(AccessingStatement.GqlStms, ds)));
            }
            Mustbe(Qlx.FROM);
            Ident ic = new(this);
            // cx.IncSD(ic);
            var on = cx.names;
            cx.defs += (lp, cx.names);
            cx.names = cx.anames;
            Mustbe(Qlx.Id);
            if (tok == Qlx.AS)
                Next();
            if (tok == Qlx.Id)
            {
                new Ident(this);
                Next();
            }
            var ob = cx.GetObject(ic.ident) ?? cx.Lookup(ic).Item1;
            if (ob is null && long.TryParse(ic.ident, out var tp))
                ob = cx.db.objects[tp] as DBObject;
            if (ob == null)
                throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob as Domain ?? Domain.Null, Grant.Privilege.Delete, 0L));
            QuerySearch qs = new(lp, f);
            if (cc)
                qs += (Level3.Index.IndexConstraint, PIndex.ConstraintType.CascadeDelete);
            cx.Add(ic.ident, f);
            cx.GetUid();
            cx.Add(qs);
            var rs = (RowSet?)cx.obs[qs.source] ?? throw new PEException("PE2006");
            if (ParseWhereClause() is CTree<long, bool> wh)
            {
                rs = (RowSet?)cx.obs[rs.defpos] ?? throw new PEException("PE2007");
                rs = rs.Apply(RowSet.E + (RowSet._Where, rs.where + wh), cx);
            }
            cx._Add(rs);
            cx.result = rs;
            if (tok != Qlx.EOF)
                throw new DBException("42000", tok);
            // cx.DecSD();
            cx.defs -= lp;
            cx.names += on;
            if (cx.parse.HasFlag(ExecuteStatus.Obey|ExecuteStatus.Http))
                cx = ((Transaction)cx.db).Execute(qs, cx);
            cx.exec = qs;
            return (Executable)cx.Add(qs);
        }
        Executable DeleteNode(bool detach)
        {
            var n = lxr.val;
            Mustbe(Qlx.Id);
            return (Executable)cx.Add(new DeleteNode(cx.GetUid(),
                    cx.obs[cx.names[n.ToString()]] as QlValue
                    ?? throw new DBException("42161", "Node or edgde"),detach));
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
            var st = LexDp();
            Next();
            Ident ic = new(this);
            var on = cx.names;
            Mustbe(Qlx.Id);
            Mustbe(Qlx.SET);
            var ob = (cx.GetObject(ic.ident) as Domain ?? cx.Lookup(ic).Item1 as Domain) ?? throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob, Grant.Privilege.Update, st));
            cx.defs += (ic.uid, f.names);
            cx.Add(f.names);
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
            cx.result = rs;
            if (cx.parse.HasFlag(ExecuteStatus.Obey | ExecuteStatus.Http) && cx.db is Transaction tr)
                cx = tr.Execute(us, cx); 
            us = (UpdateSearch)cx.Add(us);
            cx.exec = us;
            // cx.DecSD();
            cx.defs -= st;
            cx.names += on;
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
                vbl = (QlValue)cx.Add(new SqlSecurity(LexDp(), cx));
                Next();
            }
            else vbl = ParseVarOrColumn((DBObject.Scope,LexDp()));
            Mustbe(Qlx.EQL);
            val = ParseSqlValue((DBObject._Domain,vbl.domain));
            return new UpdateAssignment(vbl.defpos, val.defpos);
        }
        /// <summary>
        /// Parse an SQL Value
        /// </summary>
        /// <param name="s">The string to parse</param>
        /// <param name="t">the expected obs tye if any</param>
        /// <returns>the QlValue</returns>
        internal QlValue ParseSqlValue(string s, params (long, object)[]m)
        {
            lxr = new Lexer(cx, s);
            tok = lxr.tok;
            return ParseSqlValue(m);
        }
        internal QlValue ParseSqlValue(Ident ic, params (long, object)[]m)
        {
            lxr = new Lexer(cx, ic.ident, ic.uid);
            tok = lxr.tok;
            return ParseSqlValue(m);
        }
        internal QlValue ParseSqlValue(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Content;
            if (Match(Qlx.MATCH))
                return (QlValue)cx.Add(new QlMatchValue(cx,ParseMatchStatement(),xp));
            if (Match(Qlx.PERIOD))
            {
                Next();
                Mustbe(Qlx.LPAREN);
                var op1 = ParseSqlValue((DBObject._Domain,Domain.UnionDate));
                Mustbe(Qlx.COMMA);
                var op2 = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
                Mustbe(Qlx.RPAREN);
                var r = new SqlValueExpr(LexDp(), cx, Qlx.PERIOD, op1, op2, Qlx.NO);
                return (QlValue)cx.Add(r);
            }
            QlValue left;
            if (xp.kind == Qlx.BOOLEAN || xp.kind == Qlx.CONTENT)
            {
                left = Disjoin(ParseSqlValueDisjunct(m));
                while (left.domain.kind == Qlx.BOOLEAN && tok == Qlx.OR)
                {
                    var lp = LexDp();
                    Next();
                    left = new SqlValueExpr(lp, cx, Qlx.OR, left,
                        Disjoin(ParseSqlValueDisjunct(m)), Qlx.NO);
                }
            }
            else if (xp.kind == Qlx.TABLE || xp.kind == Qlx.VIEW || xp is NodeType)
            {
                if (Match(Qlx.TABLE))
                    Next();
                left = ParseSqlTableValue(m);
                while (Match(Qlx.UNION, Qlx.EXCEPT, Qlx.INTERSECT))
                {
                    var lp = LexDp();
                    var op = tok;
                    var md = Qlx.NO;
                    Next();
                    if ((op == Qlx.UNION || op == Qlx.EXCEPT)
                        && Match(Qlx.ALL, Qlx.DISTINCT))
                    {
                        md = tok;
                        Next();
                    }
                    var right = ParseSqlTableValue(m);
                    left = new SqlValueExpr(lp, cx, op, left, right, md);
                }
            }
            else if (xp.kind == Qlx.TYPE && Match(Qlx.LPAREN))
            {
                Next();
                if (Match(Qlx.SELECT))
                {
                    var cs = ParseSelectStatement(m);
                    left = new QlValueQuery(cx.GetUid(), cx,
                        (RowSet)(cx.obs[cs.result] ?? throw new DBException("PE2011")), 
                        xp, new CList<long>(cs.defpos));
                }
                else
                    left = ParseSqlValue(m);
                Mustbe(Qlx.RPAREN);
            }
            else
                left = ParseSqlValueExpression(m);
            return ((QlValue)cx.Add(left));
        }
        QlValue ParseSqlTableValue(params (long, object)[]m)
        {
            cx.names = cx.anames;
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Content;
            if (tok == Qlx.LPAREN)
            {
                Next();
                if (Match(Qlx.SELECT, Qlx.MATCH))
                {
                    var es = ParseStatementList(m);
                    Mustbe(Qlx.RPAREN);
                    var dr = cx.result as RowSet ?? throw new DBException("42000");
                    return (QlValue)cx.Add(new QlValueQuery(cx.GetUid(), cx, dr, xp,
                        (es as AccessingStatement)?.gqlStms??CList<long>.Empty));
                }
            }
            if (Match(Qlx.SELECT))
            {
                var cs = ParseSelectStatement(m);
                return (QlValue)cx.Add(new QlValueQuery(cx.GetUid(), cx,
                    (RowSet)(cx.obs[cs.result] ?? throw new DBException("PE2013")), 
                    xp, new CList<long>(cs.defpos)));
            }
            if (Match(Qlx.VALUES))
            {
                var lp = LexDp();
                Next();
                var v = ParseSqlValueList(m);
                return (QlValue)cx.Add(new SqlRowArray(lp, cx, xp, v));
            }
            if (Match(Qlx.TABLE))
                Next();
            return ParseSqlValueItem(m);
        }
        CTree<long,bool> Disjoin(long e) 
        {
            if (cx.obs[e] is SqlValueExpr se && se.op == Qlx.AND)
                return Disjoin(se.left) + Disjoin(se.right);
            return new (e,true);
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
        CTree<long, bool> ParseSqlValueDisjunct(params (long, object)[]m)
        {
            var left = ParseSqlValueConjunct(m);
            var r = new CTree<long, bool>(left.defpos, true);
            while ((left.domain.kind == Qlx.BOOLEAN|| left.domain.kind==Qlx.Null) && Match(Qlx.AND))
            {
                Next();
                left = ParseSqlValueConjunct(m);
                r += (left.defpos, true);
            }
            return r;
        }
        QlValue ParseSqlValueConjunct(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var wfok = (bool)(mm[NestedStatement.WfOK] ?? false);
            var left = ParseSqlValueExpression((NestedStatement.WfOK, wfok));
            if (Match(Qlx.EQL, Qlx.NEQ, Qlx.LSS, Qlx.GTR, Qlx.LEQ, Qlx.GEQ))
            {
                var op = tok;
                var lp = LexDp();
                if (lp == left.defpos)
                    lp = cx.GetUid();
                Next();
                left = (QlValue)cx.Add(new SqlValueExpr(lp, cx, op, left, 
                    ParseSqlValueExpression((DBObject._Domain,left.domain), (NestedStatement.WfOK, wfok)),
                    Qlx.NO));
            } else
            if (mm[DBObject._Domain] is Domain xp)
            {
                var nd = left.domain.LimitBy(cx, left.defpos, xp);
                if (nd != left.domain && nd != null)
                    left += (DBObject._Domain, nd);
            }
            if (mm[DBObject.HavingDom] is Domain dm)
                left = left.Having(cx, dm);
            return (QlValue)cx.Add(left);
        }
        QlValue ParseSqlValueExpression(params (long, object)[]m)
        {
            var left = ParseSqlValueTerm(m);
            while ((Domain.UnionDateNumeric.CanTakeValueOf(left.domain)
                || left is SqlReview)
                && Match(Qlx.PLUS, Qlx.MINUS))
            {
                var op = tok;
                var lp = LexDp();
                Next();
                var x = ParseSqlValueTerm(m);
                left = (QlValue)cx.Add(new SqlValueExpr(lp, cx, op, left, x, Qlx.NO));
            }
            return (QlValue)cx.Add(left);
        }
        /// <summary>
        /// |   NOT TypedValue
        /// |	Value BinaryOp TypedValue 
        /// |   PeriodPredicate
		/// BinaryOp =	'+' | '-' | '*' | '/' | '||' | SetOp | AND | OR | LT | GT | LEQ | GEQ | EQL | NEQ |':'. 
		/// SetOp = (MULTISET|SET) ( UNION | INTERSECT | EXCEPT ) ( ALL | DISTINCT ) .
        /// </summary>
        /// <param name="t">the expected obs tye</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
        QlValue ParseSqlValueTerm(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var wfok = (bool)(mm[NestedStatement.WfOK] ?? false);
            bool sign = false, not = false;
            var lp = LexDp();
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
            var left = ParseSqlValueFactor(m);
            if (sign)
                left = new SqlValueExpr(lp, cx, Qlx.MINUS, null, left, Qlx.NO)
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
                lp = LexDp();
                Next();
                return (QlValue)cx.Add(new SqlValueExpr(lp, cx,
                    op, left, ParseSqlValueFactor((DBObject._Domain,left.domain),(NestedStatement.WfOK,wfok)), imm));
            }
            while (Match(Qlx.TIMES, Qlx.DIVIDE, Qlx.MULTISET, Qlx.SET))
            {
                Qlx op = tok;
                lp = LexDp();
                switch (op)
                {
                    case Qlx.DIVIDE: goto case Qlx.TIMES;
                    case Qlx.MULTISET:
                        {
                            var sm = tok;
                            Next();
                            if (Match(Qlx.INTERSECT,Qlx.UNION,Qlx.EXCEPT))
                                op = tok;
                            else
                            {
                                tok = lxr.PushBack(sm);
                                return (QlValue)cx.Add(left);
                            }
                        }
                        break; 
                    case Qlx.SET:goto case Qlx.MULTISET;
                    case Qlx.TIMES:
                        break;
                }
                Qlx md = Qlx.NO;
                if (Match(Qlx.ALL, Qlx.DISTINCT))
                {
                    md = tok;
                    Next();
                }
                Next();
                if (left.domain.kind == Qlx.TABLE)
                    left += (Domain.Kind, Qlx.CONTENT); // must be scalar
                left = (QlValue)cx.Add(new SqlValueExpr(lp, cx, op, left,
                    ParseSqlValueFactor((DBObject._Domain, left.domain), (NestedStatement.WfOK, wfok)), md));
            }
            return (QlValue)cx.Add(left);
        }
        /// <summary>
        /// |	Value '||'  TypedValue 
        /// </summary>
        /// <param name="t">the expected obs tye</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the QlValue</returns>
		QlValue ParseSqlValueFactor(params (long, object)[]m)
        {
            var left = ParseSqlValueEntry(m);
            while (Match(Qlx.CONCATENATE, Qlx.COLON))
            {
                Qlx op = tok;
                Next();
                var mm = BTree<long, object>.New(m);
                var wfok = mm[NestedStatement.WfOK] ?? false;
                var right = ParseSqlValueEntry((DBObject._Domain,left.domain), (NestedStatement.WfOK,wfok));
                left = new SqlValueExpr(LexDp(), cx, op, left, right, Qlx.NO);
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
        /// <param name="t">the expected obs tye</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
		QlValue ParseSqlValueEntry(params (long, object)[]m)
        {
            var left = ParseSqlValueItem(m);
            bool invert = false;
            var lp = LexDp();
            while (tok == Qlx.DOT || tok == Qlx.LBRACK)
                if (tok == Qlx.DOT)
                {
                    // could be table alias, block id, instance id etc
                    Next();
                    if (tok == Qlx.TIMES)
                    {
                        lp = LexDp();
                        Next();
                        return new SqlStar(lp, cx, left.defpos);
                    }
                    var n = new Ident(this);
                    Mustbe(Qlx.Id);
                    if (tok == Qlx.LPAREN)
                    {
                        var ps = CList<long>.Empty;
                        Next();
                        if (tok != Qlx.RPAREN)
                            ps = ParseSqlValueList(m);
                        cx.Add(left);
                        var ut = left.domain; // care, the methodInfos may be missing some later methods
                        if (cx.db.objects[ut.defpos] is not Domain u || u.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42105").Add(Qlx.TYPE);
                        var ar = cx.db.Signature(cx,ps);
                        var pr = cx.db.objects[oi.methodInfos[n.ident]?[ar] ?? -1L] as Method
                            ?? throw new DBException("42173", n);
                        left = new SqlMethodCall(lp, cx, pr, ps, left);
                        Mustbe(Qlx.RPAREN);
                        left = (QlValue)cx.Add(left);
                    }
                    else
                    {
                        var oi = left.infos[cx.role.defpos];
                        if (oi is null || oi.names == Names.Empty)
                            oi = left.domain.infos[cx.role.defpos];
                        var cp = oi?.names[n.ident] ?? -1L;
                        if (cp == 0L) throw new DBException("42000");
                        var el = new QlInstance(n, cx, lp, cp);
                        left = (QlValue)cx.Add(new SqlValueExpr(cx.GetUid(), cx, Qlx.DOT, left, el, Qlx.NO));
                    }
                }
                else // tok==Qlx.LBRACK
                {
                    Next();
                    left = new SqlValueExpr(lp, cx, Qlx.LBRACK, left,
                        ParseSqlValue((DBObject._Domain,Domain.Int)), Qlx.NO);
                    Mustbe(Qlx.RBRACK);
                }
            var mm = BTree<long, object>.New(m);
            var xp = (Domain)(mm[DBObject._Domain] ?? Domain.Content);
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
                    var es = CList<long>.Empty;
                    var t1 = ParseDataType();
                    lp = LexDp();
                    r += t1;
                    while (tok == Qlx.COMMA)
                    {
                        Next();
                        t1 = ParseDataType();
                        lp = LexDp();
                        r += t1;
                    }
                    Mustbe(Qlx.RPAREN);
                    return (QlValue)cx.Add(new TypePredicate(lp, left, b, r));
                }
                Mustbe(Qlx.NULL);
                return (QlValue)cx.Add(new NullPredicate(lp, left, b));
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
                var lw = ParseSqlValueTerm((DBObject._Domain,od));
                Mustbe(Qlx.AND);
                var hi = ParseSqlValueTerm((DBObject._Domain, od));
                return (QlValue)cx.Add(new BetweenPredicate(lp, cx, left, !invert, lw, hi));
            }
            if (tok == Qlx.LIKE)
            {
                if (!(xp.CanTakeValueOf(Domain.Bool) &&
                    Domain.Char.CanTakeValueOf(left.domain)))
                    throw new DBException("42000", lxr.pos);
                Next();
                LikePredicate k = new(lp, cx, left, !invert, ParseSqlValue((DBObject._Domain,Domain.Char)), null);
                if (Match(Qlx.ESCAPE))
                {
                    Next();
                    k += (cx, LikePredicate.Escape, ParseSqlValueItem((DBObject._Domain, Domain.Char))?.defpos ?? -1L);
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
                InPredicate n = new InPredicate(lp, cx, left) +
                    (QuantifiedPredicate.Found, !invert);
                var tp = tok;
                if (tok == Qlx.LPAREN || tok == Qlx.LBRACK)
                {
                    Next();
                    if (Match(Qlx.SELECT, Qlx.TABLE, Qlx.VALUES))
                    {
                        RowSet rs = ParseQueryExpression((DBObject._Domain,Domain.TableType));
                        cx.Add(rs);
                        n += (cx, QuantifiedPredicate._Select, rs.defpos);
                    }
                    else
                        n += (cx, QuantifiedPredicate.Vals, 
                            ParseSqlValueList((DBObject._Domain,left.domain)));
                    Mustbe((tp == Qlx.LPAREN) ? Qlx.RPAREN : Qlx.RBRACK);
                }
                else
                {
                    var vl = ParseSqlValue(
                        (DBObject._Domain, cx.Add(new Domain(cx.GetUid(), Qlx.COLLECT, left.domain))));
                    n += (cx, SqlFunction._Val, vl.defpos);
                }
                return (QlValue)cx.Add(n);
            }
            if (Match(Qlx.MEMBER))
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Qlx.OF);
                var dm = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.MULTISET, xp));
                return (QlValue)cx.Add(new MemberPredicate(LexDp(), cx, left,
                    !invert, ParseSqlValue((DBObject._Domain,dm))));
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
        /// RowType = SCHEMA '(' ObjectName ')' . 
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
        /// <param name="t">the expected obs tye</param>
        /// <param name="wfok">whether a window function is allowed</param>
        /// <returns>the ql value</returns>
        internal QlValue ParseSqlValueItem(params (long, object)[]m)
        {
            QlValue r;
            var lp = LexDp();
            if (tok == Qlx.QMARK && cx.parse.HasFlag(ExecuteStatus.Prepare))
            {
                Next();
                var qm = new SqlLiteral(lp, new TQParam(Domain.Content, lp));
                cx.qParams += qm.defpos;
                return qm;
            }
            if (Match(Qlx.LEVEL))
            {
                return (QlValue)cx.Add(new SqlLiteral(LexDp(), TLevel.New(MustBeLevel())));
            }
            Match(Qlx.SCHEMA, Qlx.LABELS, Qlx.ELEMENTID, Qlx.ID, Qlx.TYPE, // we may get these non-reserved words
                Qlx.TABLE, Qlx.EXTRACT, Qlx.FIRST_VALUE, Qlx.GROUPING, Qlx.HTTP, Qlx.LAST_VALUE, Qlx.LAST_DATA,Qlx.RANK,
    Qlx.CHAR_LENGTH, Qlx.WITHIN, Qlx.POSITION, Qlx.ROW_NUMBER, Qlx.SUBSTRING, Qlx.COLLECT, Qlx.INTERSECTION,
    Qlx.ELEMENT, Qlx.USER, Qlx.VERSIONING, Qlx.VALUES);
            StartStdFunctionRefs();
            if (Match(Qlx.Id, Qlx.FIRST, Qlx.NEXT, Qlx.LAST, Qlx.CHECK, Qlx.TYPE_URI)) // Id or pseudo ident
            {
                QlValue vr = ParseVarOrColumn(m);
                if (tok == Qlx.DOUBLECOLON)
                {
                    Next();
                    if (vr.name == null || cx.db.objects[cx.role.dbobjects[vr.name] ?? -1L] is not Domain ut
                        || ut.infos[cx.role.defpos] is not ObInfo oi)
                        throw new DBException("42139", vr.name ?? "??").Mix();
                    var name = new Ident(this);
                    Mustbe(Qlx.Id);
                    lp = LexDp();
                    Mustbe(Qlx.LPAREN);
                    var ps = ParseSqlValueList(m);
                    Mustbe(Qlx.RPAREN);
                    var n = cx.db.Signature(cx,ps);
                    var me = cx.db.objects[oi.methodInfos[name.ident]?[n] ?? -1L] as Method
                        ?? throw new DBException("42132", name.ident, ut.name).Mix();
                    if (me.methodType != PMethod.MethodType.Static)
                        throw new DBException("42140").Mix();
                    var fc = new SqlMethodCall(lp, cx, me, ps, vr);
                    return (QlValue)cx.Add(fc);
                }
                return (QlValue)cx.Add(vr);
            }
            if (Match(Qlx.EXISTS, Qlx.UNIQUE))
            {
                Qlx op = tok;
                Next();
                var lb = Mustbe(Qlx.LPAREN,Qlx.LBRACE);
                var de = ParseStatementList();
                if (de is not AccessingStatement ae) throw new DBException("PE70821");
                Mustbe((lb==Qlx.LPAREN)?Qlx.RPAREN:Qlx.RBRACE);
                if (op == Qlx.EXISTS)
                    return (QlValue)cx.Add(new ExistsPredicate(LexDp(), cx, 
                        cx.result as RowSet??EmptyRowSet.Empty));
                else
                    return (QlValue)cx.Add(new UniquePredicate(LexDp(), cx, 
                        cx.result as RowSet ?? EmptyRowSet.Empty));
            }
            var mm = BTree<long, object>.New(m);
            var xp = (mm[DBObject._Domain] as Domain) ?? Domain.Content;
            if (Match(Qlx.RDFLITERAL, Qlx.CHARLITERAL,
                Qlx.INTEGERLITERAL, Qlx.NUMERICLITERAL, Qlx.NULL,
            Qlx.REALLITERAL, Qlx.BLOBLITERAL, Qlx.BOOLEANLITERAL))
            {
                r = new SqlLiteral(LexDp(), xp.Coerce(cx,lxr.val));
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
                            lp = LexDp();
                            Next();
                            if (tok == Qlx.SELECT)
                            {
                                var st = lxr.start;
                                var cs = ParseSelectStatement((DBObject._Domain,Domain.Null));
                                Mustbe(Qlx.RPAREN);
                                return (QlValue)cx.Add(new QlValueQuery(lp, cx,
                                    (RowSet)(cx.obs[cs.result] ?? throw new DBException("42000", "Array")), 
                                    xp, new CList<long>(cs.defpos)));
                            }
                            throw new DBException("22204");
                        }
                        Mustbe(Qlx.LBRACK);
                        var et = (xp.kind == Qlx.CONTENT) ? xp
                            : xp.elType ?? throw new DBException("42000", lxr.pos);
                        var v = ParseSqlValueList((DBObject._Domain,et));
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueArray(lp, cx, xp, v));
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
                        r = new SqlLiteral(lp, new TInt(ob.lastChange));
                        Mustbe(Qlx.RPAREN);
                        return (QlValue)cx.Add(r);
                    }
                case Qlx.CURRENT_DATE:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, Qlx.CURRENT_DATE,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_ROLE:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, Qlx.CURRENT_ROLE,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_TIME:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, Qlx.CURRENT_TIME,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.CURRENT_TIMESTAMP:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, Qlx.CURRENT_TIMESTAMP,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.USER:
                    {
                        Next();
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, Qlx.USER,
                            null, null, null, Qlx.NO));
                    }
                case Qlx.DATE: // also TIME, TIMESTAMP, INTERVAL
                    {
                        Qlx tk = tok;
                        Next();
                        var o = lxr.val;
                        lp = LexDp();
                        if (tok == Qlx.CHARLITERAL)
                        {
                            Next();
                            return new SqlDateTimeLiteral(lp, cx, Domain.For(tk), o.ToString());
                        }
                        else
                            return (QlValue)cx.Add(new SqlLiteral(lp, o));
                    }
                case Qlx.INTERVAL:
                    {
                        Next();
                        var o = lxr.val;
                        Mustbe(Qlx.CHARLITERAL);
                        Domain di = ParseIntervalType();
                        return (QlValue)cx.Add(new SqlDateTimeLiteral(lp, cx, di, o.ToString()));
                    }
                case Qlx.LPAREN:// subquery
                    {
                        Next();
                        if (Match(Qlx.SELECT, Qlx.MATCH))
                        {
                            var es = ParseStatementList(m);
                            Mustbe(Qlx.RPAREN);
                            var dm = cx.result as RowSet ?? throw new DBException("42000");
                            return (QlValue)cx.Add(new QlValueQuery(cx.GetUid(), cx, dm, xp,
                                (es as AccessingStatement)?.gqlStms ?? CList<long>.Empty));
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
                                var v = ParseSqlValue(m);
                                if (v is SqlLiteral sl)
                                    v = (QlValue)cx.Add(new SqlLiteral(lp, xp.Coerce(cx, sl.val)));
                                Mustbe(Qlx.RPAREN);
                                return v;
                        }
                        var fs = BList<DBObject>.Empty;
                        for (var i = 0; ; i++)
                        {
                            var it = ParseSqlValue((DBObject._Domain,et ??
                                xp.representation[xp[i] ?? -1L] ?? Domain.Content));
                            if (tok == Qlx.AS)
                            {
                                lp = LexDp();
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
                        return (QlValue)cx.Add(new SqlRow(lp, cx, fs));
                    }
                case Qlx.MULTISET:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                            return ParseSqlValue(m);
                        Mustbe(Qlx.LBRACK);
                        var v = ParseSqlValueList(m);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueMultiset(lp, cx, xp, v));
                    }
                case Qlx.SET:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                            return ParseSqlValue(m);
                        Mustbe(Qlx.LBRACK);
                        var v = ParseSqlValueList(m);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Qlx.RBRACK);
                        return (QlValue)cx.Add(new SqlValueSet(lp, cx, xp, v));
                    }
                case Qlx.NEW:
                    {
                        Next();
                        var o = new Ident(this);
                        Mustbe(Qlx.Id);
                        lp = LexDp();
                        if (cx.db.objects[cx.role.dbobjects[o.ident] ?? -1L] is not Domain ut
                            || ut.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42142").Mix();
                        Mustbe(Qlx.LPAREN);
                        var ps = ParseSqlValueList((DBObject._Domain,ut));
                        var n = cx.db.Signature(cx, ps);
                        Mustbe(Qlx.RPAREN);
                        if (cx.db.objects[oi.methodInfos[o.ident]?[n] ?? -1L] is not Method me)
                        {
                            if (ut.Length != 0 && ut.Length != (int)n.Count)
                                throw new DBException("42142").Mix();
                            return (QlValue)cx.Add(new SqlDefaultConstructor(o.uid, cx, ut, ps));
                        }
                        if (me.methodType != PMethod.MethodType.Constructor)
                            throw new DBException("42142").Mix();
                        return (QlValue)cx.Add(new SqlProcedureCall(o.uid, cx, me, ps));
                    }
                case Qlx.ROW:
                    {
                        Next();
                        if (Match(Qlx.LPAREN))
                        {
                            lp = LexDp();
                            Next();
                            var v = ParseSqlValueList(m);
                            Mustbe(Qlx.RPAREN);
                            return (QlValue)cx.Add(new SqlRow(lp, cx, xp, v));
                        }
                        throw new DBException("42135", "ROW").Mix();
                    }
                case Qlx.TABLE: // allowed by 6.39
                    {
                        Next();
                        var lf = ParseSqlValue((DBObject._Domain,Domain.TableType));
                        return (QlValue)cx.Add(lf);
                    }

                case Qlx.TIME: goto case Qlx.DATE;
                case Qlx.TIMESTAMP: goto case Qlx.DATE;
                case Qlx.TREAT:
                    {
                        Next();
                        Mustbe(Qlx.LPAREN);
                        var v = ParseSqlValue();
                        Mustbe(Qlx.RPAREN);
                        Mustbe(Qlx.AS);
                        var dt = ParseDataType();
                        return (QlValue)cx.Add(new SqlTreatExpr(lp, v, dt));//.Needs(v);
                    }
                case Qlx.CASE:
                    {
                        Next();
                        QlValue? v = null;
                        var cp = (DBObject._Domain,Domain.Bool);
                        Domain rd = Domain.Content;
                        if (tok != Qlx.WHEN)
                        {
                            v = ParseSqlValue(m);
                            cx.Add(v);
                            cp.Item2 = v.domain;
                        }
                        var cs = BList<(long, long)>.Empty;
                        var wh = CList<long>.Empty;
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
                            var x = ParseSqlValue(m);
                            cx.Add(x);
                            rd = rd.Constrain(cx, lp, x.domain);
                            for (var b = wh.First(); b != null; b = b.Next())
                                if (b.value() is long p)
                                    cs += (p, x.defpos);
                        }
                        var el = ParseSqlValue(m);
                        cx.Add(el);
                        rd = rd.Constrain(cx, lp, el.domain);
                        Mustbe(Qlx.END);
                        return (QlValue)cx.Add((v == null) ? (QlValue)new SqlCaseSearch(lp, cx, rd, cs, el.defpos)
                            : new SqlCaseSimple(lp, cx, rd, v, cs, el.defpos));
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
                        var v = ParseSqlValueList(m);
                        return (QlValue)cx.Add(new SqlRowArray(lp, cx, xp, v));
                    }
                case Qlx.LBRACE:
                    {
                        var v = BList<DBObject>.Empty;
                        Next();
                        if (tok != Qlx.RBRACE)
                        {
                            var (n, sv) = GetDocItem(lp,xp);
                            v += (Domain)cx.Add(sv + (ObInfo.Name, n));
                        }
                        while (tok == Qlx.COMMA)
                        {
                            Next();
                            var (n, sv) = GetDocItem(lp,xp);
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
                            var v = ParseSqlValueList(m);
                            if (v.Length == 0)
                                throw new DBException("22103").ISO();
                            Mustbe(Qlx.RBRACK);
                            return (QlValue)cx.Add(new SqlValueSet(lp, cx, xp, v));
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
            lp = LexDp();
            StartStdFunctionRefs();
            switch (tok)
            {
                case Qlx.ABS:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue((DBObject._Domain,Domain.UnionNumeric));
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
                        val = ParseSqlValue((DBObject._Domain,Domain.Collection));
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.CAST:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue();
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
                        val = ParseSqlValue((DBObject._Domain,Domain.Char));
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.CHARACTER_LENGTH: goto case Qlx.CHAR_LENGTH;
                case Qlx.COALESCE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        op1 = ParseSqlValue(m);
                        Mustbe(Qlx.COMMA);
                        op2 = ParseSqlValue(m);
                        while (tok == Qlx.COMMA)
                        {
                            Next();
                            op1 = new SqlCoalesce(LexDp(), cx, op1, op2);
                            op2 = ParseSqlValue(m);
                        }
                        Mustbe(Qlx.RPAREN);
                        return (QlValue)cx.Add(new SqlCoalesce(lp, cx, op1, op2));
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
                            val = (QlValue)cx.Add(new SqlLiteral(LexDp(), new TInt(1L))
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
                            val = ParseSqlValue();
                        }
                        Mustbe(Qlx.RPAREN);
                        if (tok == Qlx.FILTER)
                        {
                            Next();
                            Mustbe(Qlx.LPAREN); // Function syntax here
                            filter = ParseWhereClause();
                            Mustbe(Qlx.RPAREN);
                        }
                        if (Match(Qlx.OVER) && (bool)(mm[NestedStatement.WfOK]??false)) // extension to GQL v1
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
                        var mf = BTree<long, object>.Empty;
                        if (filter != null && filter != CTree<long, bool>.Empty)
                            mf += (SqlFunction.Filter, filter);
                        if (ws != null)
                            mf += (SqlFunction.Window, ws.defpos);
                        if (windowName is not null)
                            mf += (SqlFunction.WindowId, windowName);
                        var sf = new SqlFunction(lp, cx, kind, val, op1, op2, mod, mf);
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
                            return (QlValue)cx.Add(ParseVarOrColumn(m));
                        }
                        val = ParseSqlValue();
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
                        val = ParseSqlValue((DBObject._Domain, Domain.UnionDate));
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.FLOOR: goto case Qlx.ABS;
                case Qlx.FUSION: goto case Qlx.COUNT;
                case Qlx.GROUPING:
                    {
                        Next();
                        return (QlValue)cx.Add(new ColumnFunction(lp, ParseIDList()));
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
                        val = ParseSqlValue((DBObject._Domain, Domain.Comparable));
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.MOD: goto case Qlx.NULLIF;
                case Qlx.NULLIF:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        op1 = ParseSqlValue();
                        Mustbe(Qlx.COMMA);
                        op2 = ParseSqlValue((DBObject._Domain,op1.domain));
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
                            op1 = ParseSqlValue((DBObject._Domain,Domain.Int));
                            Mustbe(Qlx.IN);
                            op2 = ParseSqlValue();
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
                        lp = LexDp();
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
                            var mf = BTree<long, object>.Empty;
                            if (filter != null)
                                mf += (SqlFunction.Filter, filter);
                            if (ws != null)
                                mf += (SqlFunction.Window, ws.defpos);
                            if (windowName != null)
                                mf += (SqlFunction.WindowId, windowName);
                            return (QlValue)cx.Add(new SqlFunction(lp, cx, kind, val, op1, op2, mod, mf));
                        }
                        var v = new CList<long>(cx.Add(ParseSqlValue(m)).defpos);
                        for (var i = 1; tok == Qlx.COMMA; i++)
                        {
                            Next();
                            v += ParseSqlValue(m).defpos;
                        }
                        Mustbe(Qlx.RPAREN);
                        val = new SqlRow(LexDp(), cx, xp, v);
                        var f = new SqlFunction(lp, cx, kind, val, op1, op2, mod, BTree<long, object>.Empty
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
                            val = ParseSqlValue((DBObject._Domain,Domain.Int));
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
                        return (QlValue)cx.Add(new SqlFunction(lp, cx, kind, null, null, null, Qlx.NO));
                    }
                case Qlx.SQRT: goto case Qlx.ABS;
                case Qlx.STDDEV_POP: goto case Qlx.COUNT;
                case Qlx.STDDEV_SAMP: goto case Qlx.COUNT;
                case Qlx.SUBSTRING:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Qlx.LPAREN);
                        val = ParseSqlValue((DBObject._Domain, Domain.Char));
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
                                op1 = ParseSqlValue((DBObject._Domain, Domain.Int));
                                if (tok == Qlx.FOR)
                                {
                                    Next();
                                    op2 = ParseSqlValue((DBObject._Domain, Domain.Int));
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
                        val = ParseSqlValue((DBObject._Domain, Domain.Char));
                        if (tok == Qlx.FROM)
                        {
                            Next();
                            op1 = val; // trim character
                            val = ParseSqlValue((DBObject._Domain, Domain.Char));
                        }
                        Mustbe(Qlx.RPAREN);
                        break;
                    }
                case Qlx.TYPE: goto case Qlx.ELEMENTID;
                case Qlx.UNNEST: goto case Qlx.CARDINALITY;
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
            return (QlValue)cx.Add(new SqlFunction(lp, cx, kind, val, op1, op2, mod));
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
                for (var b = ParseSqlValueList().First(); b != null; b = b.Next())
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
                var lp = LexDp();
                Mustbe(Qlx.CHAR);
                Domain di = ParseIntervalType();
                d = di.Parse(new Scanner(lp, o.ToString().ToCharArray(), 0, cx));
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
            var ps = CList<long>.Empty;
            var lp = LexDp();
            if (tok == Qlx.LPAREN)
            {
                Next();
                ps = ParseSqlValueList();
            }
            var arity = cx.db.Signature(cx,ps);
            Mustbe(Qlx.RPAREN);
            var pp = cx.role.procedures[n.ident]?[arity] ?? -1;
            var pr = cx.db.objects[pp] as Procedure
                ?? throw new DBException("42108", n).Mix();
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (CallStatement)cx.Add(new CallStatement(lp, fc));
        }
        /// <summary>
		/// UserFunctionCall = Id '(' [  TypedValue {','  TypedValue}] ')' .
        /// </summary>
        /// <returns>the Executable</returns>
        Executable ParseProcedureCall(params (long, object)[] m)
        {
            var id = new Ident(this);
            Mustbe(Qlx.Id);
            Mustbe(Qlx.LPAREN);
            var ps = (tok==Qlx.RPAREN)?CList<long>.Empty:ParseSqlValueList(m);
            var a = cx.db.Signature(cx, ps);
            Mustbe(Qlx.RPAREN);
            Procedure pr;
            if (cx.role.procedures[id.ident]?[a] is long pp &&
                cx.db.objects[pp] is Procedure pa)
                pr = pa;
            else
            {
                var ar = CList<Domain>.Empty;
                for (var b = a.First(); b != null; b = b.Next())
                    ar += (b.value()?.kind == Qlx.NUMERIC) ? Domain.Real : b.value();
                if (cx.role.procedures[id.ident]?[ar] is long pq
                    && cx.db.objects[pq] is Procedure pb)
                    pr = pb;
                else
                    throw new DBException("42108", id.ident).Mix();
            }
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (Executable)cx.Add(new CallStatement(id.uid, fc));
        }
        /// <summary>
        /// Parse a tree of Sql values
        /// </summary>
        /// <param name="t">the expected obs tye</param>
        /// <returns>the List of QlValue</returns>
        CList<long> ParseSqlValueList(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = mm[DBObject._Domain] as Domain ?? Domain.Content;
            var r = CList<long>.Empty;
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
                        var v = ParseSqlValue(m);
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
                var v = (ei.Length > 0 && xp is not NodeType) ?
                    ParseSqlRow((DBObject._Domain,ei)) :
                    ParseSqlValue((DBObject._Domain,ei));
                cx.Add(v);
                if (tok == Qlx.AS)
                {
                    Next();
                    var d = ParseDataType();
                    v = new SqlTreatExpr(LexDp(), v, d); //.Needs(v);
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
        public SqlRow ParseSqlRow(params (long, object)[]m)
        {
            var mm = BTree<long, object>.New(m);
            var xp = (mm[DBObject._Domain] as Domain) ?? Domain.Row;
            var llp = LexDp();
            Mustbe(Qlx.LPAREN);
            var lk = CList<long>.Empty;
            var i = 0;
            for (var b = xp.rowType.First(); b != null && i < xp.rowType.Length
                && (xp.display==0||i<xp.display); b = b.Next(), i++)
                if (b.value() is long p && xp.representation[p] is Domain dt)
                {
                    if (i > 0)
                        Mustbe(Qlx.COMMA);
                    var v = ParseSqlValue((DBObject._Domain,dt));
                    cx.Add(v);
                    lk += v.defpos;
                }
            Mustbe(Qlx.RPAREN);
            return (SqlRow)cx.Add(new SqlRow(llp, cx, xp, lk));
        }
        /// <summary>
        /// Parse an SqlRow
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="sql">The string to parse</param>
        /// <param name="result">the expected obs tye</param>
        /// <returns>the SqlRow</returns>
        public QlValue ParseSqlValueList(string sql,params (long, object)[]m)
        {
            lxr = new Lexer(cx, sql);
            tok = lxr.tok;
            if (tok == Qlx.LPAREN)
                return ParseSqlRow();
            return ParseSqlValueEntry();
        }
        /// <summary>
        /// Get a document item for ParseGraphExp.
        /// caseSensitive is a connection option for JavaScript users
        /// </summary>
        /// <param name="lb">The label for the item</param>
        /// <returns>(ColRef,Value)</returns>
        (Ident, QlValue) GetDocItem(long pa,Domain lb,bool sch=false)
        {
            if (!Match(Qlx.Id))
                throw new DBException("42161", "Identifier");
            Ident k = new(this);
            var ns = lb.infos[cx.role.defpos]?.names??Names.Empty;
            if (lxr.caseSensitive && k.ident == "id")
                k = new Ident("ID", k.uid);
            if (!cx.Known(k.ident))
            {
                cx.bindings += (k.uid, Domain.Null);
                cx.names += (k.ident, k.uid);
            }
            var xd = (cx.db.objects[lb.infos[cx.role.defpos]?.names[k.ident]?? -1L] as TableColumn)?.domain ?? Domain.Char;
            var ip = lxr.Position;
            Mustbe(Qlx.Id);
            lxr.docValue = true;
            Mustbe(Qlx.COLON,Qlx.DOUBLECOLON,Qlx.TYPED); // GQL extra options
            if (sch)
                return (k, (QlValue)cx.Add(new SqlLiteral(k.uid, ParseDataType())));
            Ident q = new(lxr.val.ToString(), LexDp()); // capture instance reference
            QlValue? r = null;
            if (tok == Qlx.Id && !cx.Known(q.ident) && q.uid >= Transaction.Analysing)
            {
                cx.bindings += (q.uid, Domain.Null);
                cx.names += (q.ident, q.uid);
                if (cx.obs[lb.names[k.ident]] is TableColumn dc)
                    xd = dc.domain;
                if (cx.Lookup(q.ident) is QlValue qi)
                {
                    q = new(q.ident, qi.defpos);
                    r = qi;
                }
                r = new QlValue(q, BList<Ident>.Empty, cx, Domain.Content);
                Next();
            } 
            lxr.docValue = false;
            if (lxr.tgs[lxr.Position - q.ident.Length]?.type.HasFlag(TGParam.Type.Group) == true)
            {
                xd = new Domain(-1L, Qlx.ARRAY, xd);
                r += (DBObject._Domain, xd);
                cx.Add(r);
            }
            var eq = (lxr.val is TChar ec) ? ec.value : q.ident;
            if (lxr.caseSensitive && eq == "localDateTime") // allow JavaScript pseudos
            {
                Next();
                Mustbe(Qlx.LPAREN);
                Ident tv = new(this);
                if (Domain.Timestamp.Parse(0, tv.ident, cx) is not TDateTime v)
                    throw new DBException("42161", "Timestamp");
                Next();
                Mustbe(Qlx.RPAREN);
                return (q, (QlValue)cx.Add(new SqlLiteral(q.uid, v)));
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
                return (q, (QlValue)cx.Add(new SqlLiteral(q.uid, v)));
            }
            if (lxr.caseSensitive && eq == "toInteger") // alow JavaScript cast
            {
                Next();
                Mustbe(Qlx.LPAREN);
                if (lxr.val is not TInt v)
                    throw new DBException("42161", "Integer");
                Next();
                Mustbe(Qlx.RPAREN);
                return (q, (QlValue)cx.Add(new SqlLiteral(q.uid, v)));
            }
            if (tok == Qlx.Id && !cx.Known(eq))
            {
                var vv = new SqlField(q.uid, q.ident, -1, -1L, xd, pa);
                cx.Add(vv);
                cx.names += (q.ident, q.uid);
                lxr.tgs += (q.uid, new TGParam(q.uid, q.ident, xd, lxr.tgg | TGParam.Type.Value, pa));
            }
            var dm = lb.representation[ns[k.ident]] ?? Domain.Content; // don't use xd: might be a Group/TArray
            r ??= ParseSqlValue((DBObject._Domain,dm));
            return (k, (QlValue)cx.Add(r));

        }
        /// <summary>
        /// Parse a document array
        /// </summary>
        /// <returns>the SqlDocArray</returns>
        public QlValue ParseSqlDocArray()
        {
            var dp = LexDp();
            var v = new SqlRowArray(dp, BTree<long, object>.Empty);
            Next();
            if (tok != Qlx.RBRACK)
            {
                if (tok != Qlx.LPAREN)
                {
                    var ls = ParseSqlValueList();
                    Mustbe(Qlx.RBRACK);
                    var xp = Domain.Content;
                    if (ls != CList<long>.Empty && cx.obs[ls[0]] is QlValue vl)
                        xp = new Domain(-1L, Qlx.ARRAY, vl.domain);
                    return (QlValue)cx.Add(new SqlValueArray(dp, cx, xp, ls));
                }
                v += ParseSqlRow();
            }
            while (tok == Qlx.COMMA)
            {
                Next();
                v += ParseSqlRow();
            }
            Mustbe(Qlx.RBRACK);
            return (QlValue)cx.Add(v);
        }
    }
 }