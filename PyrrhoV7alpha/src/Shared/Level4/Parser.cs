using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
using System.ComponentModel;
using System.Globalization;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
    /// or make other changes. parse should only call Obey within a transaction.
    /// This means that (for now at least) stored executable code (triggers, procedures) should
    /// never attempt schema changes. 
    /// </summary>
	internal class Parser
    {
        /// <summary>
        /// cx.obs contains DBObjects currently involved in the query (other than Domains).
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
		internal Sqlx tok;
        public Parser(Database da,Connection con)
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
            lxr = new Lexer(cx,"");
        }
        /// <summary>
        /// Create a Parser for Constraint definition
        /// </summary>
        /// <param name="_cx"></param>
        /// <param name="src"></param>
        /// <param name="infos"></param>
        public Parser(Context _cx,Ident src)
        {
            cx = _cx.ForConstraintParse();
            cx.parse = ExecuteStatus.Parse;
            lxr = new Lexer(cx,src);
            tok = lxr.tok;
        }
        public Parser(Context _cx, string src)
        {
            cx = _cx.ForConstraintParse();
            lxr = new Lexer(cx,new Ident(src, cx.Ix(Transaction.Analysing,cx.db.nextStmt)));
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
        {  }
        internal Iix LexPos()
        {
            var lp = lxr.Position;
            return cx.parse switch
            {
                ExecuteStatus.Obey => cx.Ix(lp, lp),
                ExecuteStatus.Prepare => new Iix(lp, cx, cx.nextHeap++),
                _ => new Iix(lp, cx, cx.GetUid()),
            };
        }
        internal long LexDp()
        {
            return cx.parse switch
            {
                ExecuteStatus.Obey => lxr.Position,
                ExecuteStatus.Prepare => cx.nextHeap++,
                _ => cx.GetUid(),
            };
        }
        /// <summary>
        /// Move to the next token
        /// </summary>
        /// <returns></returns>
		internal Sqlx Next()
        {
            tok = lxr.Next();
            return tok;
        }
        /// <summary>
        /// Match any of a set of token types
        /// </summary>
        /// <param name="s">the tree of token types</param>
        /// <returns>whether the current token matched any of the set</returns>
		bool Match(params Sqlx[] s)
        {
            string a = "";
            if (tok == Sqlx.ID)
                a = lxr.val.ToString().ToUpper();
            for (int j = 0; j < s.Length; j++)
            {
                if (tok == s[j])
                    return true;
                if (tok == Sqlx.ID && a.CompareTo(s[j].ToString())==0)
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
		internal Sqlx Mustbe(params Sqlx[] t)
        {
            int j;
            string s = "";
            if (tok == Sqlx.ID)
                s = lxr.val.ToString().ToUpper();
            for (j = 0; j < t.Length; j++)
            {
                if (tok == t[j])
                    break;
                var a = tok == Sqlx.ID;
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
                string ctx = (lxr.pos>=lxr.input.Length)?"EOF":new string(lxr.input, lxr.start, lxr.pos - lxr.start);
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
            tok = lxr.tok;
            ParseSqlStatement(xp);
            if (tok == Sqlx.SEMICOLON)
                Next();
            for (var b = cx.forReview.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue k)
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx.obs[c.key()] is RowSet rs)
                            rs.Apply(new BTree<long, object>(RowSet._Where, new CTree<long, bool>(k.defpos, true)),
                                cx);
            if (tok != Sqlx.EOF)
            {
                string ctx = new (lxr.input, lxr.start, lxr.pos - lxr.start);
                throw new DBException("42000", ctx).ISO();
            }
            if (cx.undefined != CTree<long, int>.Empty)
                throw new DBException("42112", cx.obs[cx.undefined.First()?.key()??-1L]?.mem[ObInfo.Name] ?? "?");
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return cx.db;
        }
        public Database ParseSql(string sql)
        {
            if (PyrrhoStart.ShowPlan)
                Console.WriteLine(sql);
            lxr = new Lexer(cx,sql, cx.db.lexeroffset);
            tok = lxr.tok;
            do
            {
                ParseSqlStatement(Domain.TableType);
                //      cx.result = e.defpos;
                if (tok == Sqlx.SEMICOLON)
                    Next();
            } while (tok != Sqlx.EOF);
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return cx.db;
        }
        /// <summary>
        /// Add the parameters to a prepared statement
        /// </summary>
        /// <param name="pre">The object with placeholders</param>
        /// <param name="s">The parameter strings concatenated by |</param>
        /// <returns>The modified database and the new uid highwatermark</returns>
        public Database ParseSql(PreparedStatement pre,string s)
        {
            cx.Add(pre);
            cx.Add(pre.framing);
            lxr = new Lexer(cx, s, cx.db.lexeroffset, true);
            tok = lxr.tok;
            var b = cx.FixLl(pre.qMarks).First();
            for (; b != null && tok != Sqlx.EOF; b = b.Next())
                if (b.value() is long p)
                {
                    var v = lxr.val;
                    var lp = LexPos();
                    if (Match(Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL))
                    {
                        Sqlx tk = tok;
                        Next();
                        v = lxr.val;
                        if (tok == Sqlx.CHARLITERAL)
                        {
                            Next();
                            v = new SqlDateTimeLiteral(lp.dp, cx,
                                new Domain(lp.dp, tk, BTree<long, object>.Empty), v.ToString()).Eval(cx);
                        }
                    }
                    else
                        Mustbe(Sqlx.BLOBLITERAL, Sqlx.NUMERICLITERAL, Sqlx.REALLITERAL,
                            // Sqlx.DOCUMENTLITERAL,
                            Sqlx.CHARLITERAL, Sqlx.INTEGERLITERAL);
                    cx.values += (p, v);
                    Mustbe(Sqlx.SEMICOLON);
                }
            if (!(b == null && tok == Sqlx.EOF))
                throw new DBException("33001");
            cx.QParams(); // replace SqlLiterals that are QParams with actuals
            cx = pre.target?.Obey(cx)??cx;
            return cx.db;
        }
        /// <summary>
        ///SqlStatement =	Alter
        /// 	|	BEGIN TRANSACTION
        ///     |	Call
        ///     |	COMMIT [WORK]
        /// 	|	CreateClause
        /// 	|	CursorSpecification
        /// 	|	DeleteSearched
        /// 	|	DropClause
        /// 	|	Grant
        /// 	|	Insert
        /// 	|	Rename
        /// 	|	Revoke
        /// 	|	ROLLBACK [WORK]
        /// 	|	UpdateSearched .       
        /// </summary>
        /// <param name="rt">A Domain or ObInfo for the expected result of the Executable</param>
        /// <returns>The Executable results of the Parse</returns>
        public Executable ParseSqlStatement(Domain xp)
        {
            var lp = lxr.start;
            switch (tok)
            {
                case Sqlx.ALTER: ParseAlter(); break;
                case Sqlx.CALL: ParseCallStatement(); break;
                case Sqlx.COMMIT:
                    Next();
                    if (Match(Sqlx.WORK))
                        Next();
                    if (cx.parse == ExecuteStatus.Obey)
                        cx.db.Commit(cx);
                    else
                        throw new DBException("2D000", "Commit");
                    break;
                case Sqlx.CREATE: return ParseCreateClause(); 
                case Sqlx.DELETE: return ParseSqlDelete(); 
                case Sqlx.DROP: ParseDropStatement(); break;
                case Sqlx.GRANT: return ParseGrant(); 
                case Sqlx.INSERT: return ParseSqlInsert();
                case Sqlx.WITH: // wow
                case Sqlx.MATCH: return ParseSqlMatchStatement();
                case Sqlx.REVOKE: return ParseRevoke();
                case Sqlx.ROLLBACK:
                    Next();
                    if (Match(Sqlx.WORK))
                        Next();
                    var e = new RollbackStatement(LexDp());
                    cx.exec = e;
                    if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction)
                        cx = new Context(cx.db.Rollback(), cx.conn);
                    else
                        cx.Add(e);
                    cx.exec = e;
                    return e;
                case Sqlx.RETURN: // wow
                case Sqlx.SELECT: return ParseCursorSpecification(xp);
                case Sqlx.SET: ParseSqlSet(); break;
                case Sqlx.TABLE: return ParseCursorSpecification(xp);
                case Sqlx.UPDATE: (cx, var ue) = ParseSqlUpdate(); return ue;
                case Sqlx.VALUES: return ParseCursorSpecification(xp);
                //    case Sqlx.WITH: e = ParseCursorSpecification(); break;
                case Sqlx.EOF: return new Executable(LexPos().lp); // whole input is a comment
            }
            object ob = lxr.val;
            if (ob == null || ob is TNull)
                ob = new string(lxr.input, lxr.start, lxr.pos - lxr.start);
            return new Executable(lp, ob.ToString()??"");
        }
        byte MustBeLevelByte()
        {
            byte lv;
            if (tok == Sqlx.ID && lxr.val is TChar tc && tc.value.Length == 1)
                lv = (byte)('D' - tc.value[0]);
            else
                throw new DBException("4211A", tok.ToString());
            Next();
            return lv;
        }
        Level MustBeLevel()
        {
            Mustbe(Sqlx.LEVEL);
            var min = MustBeLevelByte();
            var max = min;
            if (tok == Sqlx.MINUS)
                max = MustBeLevelByte();
            var gps = BTree<string, bool>.Empty;
            if (tok==Sqlx.GROUPS)
            {
                Next();
                while (tok==Sqlx.ID && lxr.val is not null)
                {
                    gps +=(lxr.val.ToString(), true);
                    Next();
                }
            }
            var rfs = BTree<string, bool>.Empty;
            if (tok == Sqlx.REFERENCES)
            {
                Next();
                while (tok == Sqlx.ID && lxr.val is not null)
                {
                    rfs +=(lxr.val.ToString(), true);
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
            if (Match(Sqlx.SECURITY))
            {
                Next();
                var lv = MustBeLevel();
                Mustbe(Sqlx.TO);
                var nm = lxr.val?.ToString()??throw new DBException("42135");
                Mustbe(Sqlx.ID);
                var usr = cx.db.objects[cx.db.roles[nm]??-1L] as User
                    ?? throw new DBException("42135", nm.ToString());
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                   cx.Add(new Clearance(usr.defpos, lv, tr.nextPos));
            }
            else if (Match(Sqlx.PASSWORD))
            {
                TypedValue pwd = new TChar("");
                Role? irole = null;
                Next();
                if (!Match(Sqlx.FOR) && !Match(Sqlx.TO))
                {
                    pwd = lxr.val;
                    Next();
                }
                if (Match(Sqlx.FOR))
                {
                    Next();
                    var rid = new Ident(this);
                    Mustbe(Sqlx.ID);
                    irole = cx.GetObject(rid.ident) as Role??
                        throw new DBException("42135", rid);
                }
                Mustbe(Sqlx.TO);
                var grantees = ParseGranteeList(BList<PrivNames>.Empty);
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                {
                    var irolepos = -1L;
                    if (irole != null && irole.name is not null)
                    {
                        tr.AccessRole(cx,true, new CList<string> ( irole.name ), grantees, false);
                        irolepos = irole.defpos;
                    }
                    for (var b=grantees.First();b is not null;b=b.Next())
                        if (b.value() is DBObject us)
                        cx.Add(new Authenticate(us.defpos, pwd.ToString(), irolepos,tr.nextPos));
                }
            }
            Match(Sqlx.OWNER, Sqlx.USAGE);
            if (Match(Sqlx.ALL, Sqlx.SELECT, Sqlx.INSERT, Sqlx.DELETE, Sqlx.UPDATE, Sqlx.REFERENCES, Sqlx.OWNER, Sqlx.TRIGGER, Sqlx.USAGE, Sqlx.EXECUTE))
            {
                var priv = ParsePrivileges();
                Mustbe(Sqlx.ON);
                var (ob,_) = ParseObjectName();
                long pob = ob?.defpos ?? 0;
                Mustbe(Sqlx.TO);
                var grantees = ParseGranteeList(priv);
                bool opt = ParseGrantOption();
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    tr.AccessObject(cx,true, priv, pob, grantees, opt);
            }
            else
            {
                var roles = ParseRoleNameList();
                Mustbe(Sqlx.TO);
                var grantees = ParseGranteeList(new BList<PrivNames>( new PrivNames(Sqlx.USAGE) ));
                bool opt = ParseAdminOption();
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    tr.AccessRole(cx,true, roles, grantees, opt);
            }
            return new Executable(lp, new string(lxr.input, lp, lxr.start - lp));
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
		(DBObject,string) ParseObjectName()
        {
            Sqlx kind = Sqlx.TABLE;
            Match(Sqlx.INSTANCE, Sqlx.CONSTRUCTOR, Sqlx.OVERRIDING, Sqlx.VIEW, Sqlx.TYPE);
            if (tok != Sqlx.ID || lxr.val.ToString() == "INDEX")
            {
                kind = tok;
                Next();
                if (kind == Sqlx.INSTANCE || kind == Sqlx.STATIC || kind == Sqlx.OVERRIDING || kind == Sqlx.CONSTRUCTOR)
                    Mustbe(Sqlx.METHOD);
            }
            var n = lxr.val.ToString()??"?";
            Mustbe(Sqlx.ID);
            DBObject? ob=null;
            switch (kind)
            {
                case Sqlx.TABLE: 
                case Sqlx.DOMAIN:
                case Sqlx.VIEW:
                case Sqlx.ENTITY:
                case Sqlx.TYPE: ob = cx.GetObject(n) ??
                        throw new DBException("42135",n);
                    break;
                case Sqlx.CONSTRUCTOR: 
                case Sqlx.FUNCTION: 
                case Sqlx.PROCEDURE:
                    {
                        var a = ParseSignature();
                        ob = cx.GetProcedure(LexPos().dp,n, a)??
                            throw new DBException("42108",n);
                        break;
                    }
                case Sqlx.INSTANCE: 
                case Sqlx.STATIC: 
                case Sqlx.OVERRIDING: 
                case Sqlx.METHOD:
                    {
                        var a = ParseSignature();
                        Mustbe(Sqlx.FOR);
                        var tp = lxr.val.ToString();
                        Mustbe(Sqlx.ID);
                        var oi = ((cx.role.dbobjects[tp] is long p)?cx._Ob(p):null)?.infos[cx.role.defpos] ??
                            throw new DBException("42119", tp);
                        ob = (DBObject?)oi.mem[oi.methodInfos[n]?[a]??-1L]??
                            throw new DBException("42108",n);
                        break;
                    }
                case Sqlx.TRIGGER:
                    {
                        Mustbe(Sqlx.ON);
                        var tn = lxr.val.ToString();
                        Mustbe(Sqlx.ID);
                        var tb = cx.GetObject(tn) as Table?? throw new DBException("42135", tn);
                        for (var b = tb.triggers.First(); ob == null && b != null; b = b.Next())
                            for (var c = b.value().First(); ob == null && c != null; c = c.Next())
                                if (cx._Ob(c.key()) is Trigger tg && tg.name == n)
                                    ob = tg;
                        if (ob==null)
                            throw new DBException("42107", n);
                        break;
                    }
                case Sqlx.DATABASE:
                    ob = SqlNull.Value;
                    break;
                default:
                    throw new DBException("42115", kind).Mix();
            }
            if (ob == null) throw new PEException("00083");
            return (cx.Add(ob),n);
        }
        /// <summary>
        /// used in ObjectName. 
        /// '('Type, {',' Type }')'
        /// </summary>
        /// <returns>the number of parameters</returns>
		CList<Domain> ParseSignature()
        {
            CList<Domain> fs = CList<Domain>.Empty;
            if (tok == Sqlx.LPAREN)
            {
                Next();
                if (tok == Sqlx.RPAREN)
                {
                    Next();
                    return CList<Domain>.Empty;
                }
                fs += ParseSqlDataType();
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    fs += ParseSqlDataType();
                }
                Mustbe(Sqlx.RPAREN);
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
            if (tok == Sqlx.ALL)
            {
                Next();
                Mustbe(Sqlx.PRIVILEGES);
                return r;
            }
            r+=ParsePrivilege();
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParsePrivilege();
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
            var r = new PrivNames(tok);
            Mustbe(Sqlx.SELECT, Sqlx.DELETE, Sqlx.INSERT, Sqlx.UPDATE,
                Sqlx.REFERENCES, Sqlx.USAGE, Sqlx.TRIGGER, Sqlx.EXECUTE, Sqlx.OWNER);
            if ((r.priv == Sqlx.UPDATE || r.priv == Sqlx.REFERENCES || r.priv == Sqlx.SELECT || r.priv == Sqlx.INSERT) && tok == Sqlx.LPAREN)
            {
                Next();
                r.cols += (lxr.val.ToString(),true);
                Mustbe(Sqlx.ID);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    r.cols += (lxr.val.ToString(),true);
                    Mustbe(Sqlx.ID);
                }
                Mustbe(Sqlx.RPAREN);
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
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParseGrantee(priv);
            }
            return r;
        }
        /// <summary>
        /// helper for non-reserved words
        /// </summary>
        /// <returns>if we match a method mode</returns>
        bool MethodModes()
        {
            return Match(Sqlx.INSTANCE, Sqlx.OVERRIDING, Sqlx.CONSTRUCTOR);
        }
        internal Role? GetRole(string n)
        {
            return (Role?)cx.db.objects[cx.db.roles[n]??-1L];
        }
        /// <summary>
		/// Grantee = 	[USER] id
		/// 	|	ROLE id . 
        /// </summary>
        /// <param name="priv">the tree of privileges</param>
        /// <returns>the updated grantee</returns>
		DBObject ParseGrantee(BList<PrivNames> priv)
        {
            Sqlx kind = Sqlx.USER;
            if (Match(Sqlx.PUBLIC))
            {
                Next();
                return (Role?)cx.db.objects[Database.Guest]??throw new PEException("PE2400");
            }
            if (Match(Sqlx.USER))
                Next();
            else if (Match(Sqlx.ROLE))
            {
                kind = Sqlx.ROLE;
                Next();
            }
            var n = lxr.val.ToString();
            Mustbe(Sqlx.ID);
            DBObject? ob;
            switch (kind)
            {
                case Sqlx.USER:
                    {
                        ob = GetRole(n);
                        if ((ob == null || ob.defpos == -1) && cx.db is Transaction tr)
                            ob = cx.Add(new PUser(n, tr.nextPos, cx));
                        break;
                    }
                case Sqlx.ROLE: 
                    {
                        ob = GetRole(n)??throw new DBException("28102",n);
                        if (ob.defpos>=0)
                        { // if not PUBLIC we need to have privilege to change the grantee role
                            var ri = ob.infos[cx.role.defpos];
                            if (ri == null || !ri.priv.HasFlag(Role.admin))
                                throw new DBException("42105");
                        }
                    }
                    break;
                default: throw new DBException("28101").Mix();
            }
            if (ob == SqlNull.Value && (priv == null || priv.Length != 1 || priv[0]?.priv != Sqlx.OWNER))
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
            if (tok == Sqlx.WITH)
            {
                Next();
                Mustbe(Sqlx.GRANT);
                Mustbe(Sqlx.OPTION);
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
            if (tok == Sqlx.WITH)
            {
                Next();
                Mustbe(Sqlx.ADMIN);
                Mustbe(Sqlx.OPTION);
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
            if (tok == Sqlx.ID)
            {
                r += lxr.val.ToString();
                Next();
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    r += lxr.val.ToString();
                    Mustbe(Sqlx.ID);
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
            Sqlx opt = ParseRevokeOption();
            if (tok == Sqlx.ID)
            {
                var priv = ParseRoleNameList();
                Mustbe(Sqlx.FROM);
                var grantees = ParseGranteeList(BList<PrivNames>.Empty);
                if (opt == Sqlx.GRANT)
                    throw new DBException("42116").Mix();
                if (cx.db is Transaction tr)
                    tr.AccessRole(cx,false, priv, grantees, opt == Sqlx.ADMIN);
            }
            else
            {
                if (opt == Sqlx.ADMIN)
                    throw new DBException("42117").Mix();
                var priv = ParsePrivileges();
                Mustbe(Sqlx.ON);
                var (ob,_) = ParseObjectName();
                Mustbe(Sqlx.FROM);
                var grantees = ParseGranteeList(priv);
                if (cx.db is Transaction tr)
                    tr.AccessObject(cx,false, priv, ob.defpos, grantees, (opt == Sqlx.GRANT));
            }
            return new Executable(lp, new string(lxr.input, lp, lxr.start - lp));
        }
        /// <summary>
        /// [GRANT OPTION FOR] | [ADMIN OPTION FOR]
        /// </summary>
        /// <returns>GRANT or ADMIN or NONE</returns>
		Sqlx ParseRevokeOption()
        {
            Sqlx r = Sqlx.NONE;
            if (Match(Sqlx.GRANT, Sqlx.ADMIN))
            {
                r = tok;
                Next();
                Mustbe(Sqlx.OPTION);
                Mustbe(Sqlx.FOR);
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
                cx = tr.Execute(e,cx);
            cx.Add(e);
            return new Executable(lp, new string(lxr.input, lp, lxr.start - lp));
        }
        /// <summary>
		/// Create =	CREATE ROLE id [string]
		/// |	CREATE DOMAIN id [AS] DomainDefinition {Metadata}
		/// |	CREATE FUNCTION id '(' Parameters ')' RETURNS Type Body 
        /// |   CREATE ORDERING FOR id Order
		/// |	CREATE PROCEDURE id '(' Parameters ')' Body
		/// |	CREATE Method Body
		/// |	CREATE TABLE id TableContents [UriType] {Metadata}
		/// |	CREATE TRIGGER id (BEFORE|AFTER) Event ON id [ RefObj ] Trigger
		/// |	CREATE TYPE id [UNDER id] AS Representation [ Method {',' Method} ] {Metadata}
		/// |	CREATE ViewDefinition 
        /// |   CREATE XMLNAMESPACES NamespaceList
        /// |   CREATE (Node) {-[Edge]->(Node)|<-[Edge]-(Node)}
        /// </summary>
        /// <returns>A tree of Executable references</returns>
        Executable ParseCreateClause()
        {
            var lp = lxr.start;
            if (cx.role is Role dr 
                && dr.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole)!=true
                && dr.defpos!= -502)
                throw new DBException("42105");
            Next();
            MethodModes();
            Match(Sqlx.TEMPORARY, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE, Sqlx.DOMAIN);
            if (Match(Sqlx.ORDERING))
                ParseCreateOrdering();
            else if (Match(Sqlx.XMLNAMESPACES))
                ParseCreateXmlNamespaces();
            else if (tok == Sqlx.PROCEDURE || tok == Sqlx.FUNCTION)
            {
                bool func = tok == Sqlx.FUNCTION;
                Next();
                ParseProcedureClause(func, Sqlx.CREATE);
            }
            else if (Match(Sqlx.OVERRIDING, Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.METHOD))
                ParseMethodDefinition();
            else if (tok == Sqlx.TABLE || tok == Sqlx.TEMPORARY)
            {
                if (tok == Sqlx.TEMPORARY)
                {
                    Role au = GetRole("Temp") ?? throw new DBException("3D001", "Temp").Mix();
                    cx = new Context(cx, au, cx.db.user ?? throw new DBException("42105"));
                    Next();
                }
                Mustbe(Sqlx.TABLE);
                ParseCreateTable();
            }
            else if (tok == Sqlx.TRIGGER)
            {
                Next();
                ParseTriggerDefClause();
            }
            else if (tok == Sqlx.DOMAIN)
            {
                Next();
                ParseDomainDefinition();
            }
            else if (tok == Sqlx.TYPE)
            {
                Next();
                ParseTypeClause();
            }
            else if (tok == Sqlx.ROLE)
            {
                Next();
                var id = lxr.val.ToString();
                Mustbe(Sqlx.ID);
                TypedValue o = new TChar("");
                if (Match(Sqlx.CHARLITERAL))
                {
                    o = lxr.val;
                    Next();
                }
                if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                    cx.Add(new PRole(id, o.ToString(), tr.nextPos, cx));
            }
            else if (tok == Sqlx.VIEW)
                ParseViewDefinition();
            else if (tok == Sqlx.LPAREN)
                return ParseCreateGraph();
            return new Executable(lp, new string(lxr.input, lp, lxr.start - lp));
        }
        /// <summary>
        /// Create: CREATE Graph {',' Graph } [THEN Statements END].
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
        /// 5. adds the TGraph and Node->Edge associations to the database
        /// </summary>
        internal Executable ParseCreateGraph()
        {
            // New nodes without ID keys should be assigned cx.db.nextPos.ToString(), and this is fixed
            // on Commit, see Record(Record,Writer): the NodeOrEdge flag is added in Record()
            var ge = ParseSqlGraphList();
            var st = BList<long?>.Empty;
            var lp = cx.GetPos();
            var cs = (CreateStatement)cx.Add(new CreateStatement(lp, ge, st));
            if (cx.parse == ExecuteStatus.Obey)
                cs.Obey(cx);
            if (tok == Sqlx.THEN)
            {
                Next();
                if (ParseProcedureStatement(Domain.Content) is Executable e)
                {
                    cx.Add(e);
                    cs += (IfThenElse.Then, e);
                    cx.Add(cs);
                }
            }
            return cs;
        }
        (CTree<long,TGParam>,BList<long?>) ParseSqlMatchList()
        {
            var svgs = BList<long?>.Empty;
            var tgs = CTree<long, TGParam>.Empty;
            Ident? pi = null;
            // the current token is LPAREN
            while (Match(Sqlx.LPAREN,Sqlx.USING,Sqlx.TRAIL,Sqlx.ACYCLIC,Sqlx.SIMPLE,Sqlx.SHORTEST,Sqlx.ALL,Sqlx.ANY))
            {
                var mo = Sqlx.NONE;
                if (tok != Sqlx.LPAREN)
                {
                    if (Match(Sqlx.TRAIL, Sqlx.ACYCLIC, Sqlx.SIMPLE, Sqlx.SHORTEST, Sqlx.ALL, Sqlx.ANY))
                    {
                        mo = tok;
                        Next();
                    }
                    pi = new Ident(this);
                    if (tok == Sqlx.ID)
                    {
                        Next();
                        if (lxr.tgs[pi.iix.dp] is TGParam gp)
                        {
                            gp = new TGParam(gp.uid, gp.value, new Domain(-1L,Sqlx.ARRAY,Domain.NodeType), TGParam.Type.Path);
                            lxr.tgs += (pi.iix.dp, gp);
                            cx.Add(new SqlValue(pi,cx,Domain.Char));
                            cx.defs += (pi, cx.sD);
                        }
                        Mustbe(Sqlx.EQL);
                    }
                }
                (tgs,var s) = ParseSqlMatch(tgs);
                svgs += cx.Add(new SqlMatch(cx, mo, s, pi?.iix.dp??-1L)).defpos;
                if (tok==Sqlx.COMMA)
                    Next();
            };
            return (tgs,svgs);
        }
        CList<CList<SqlNode>> ParseSqlGraphList()
        {
            var svgs = CList<CList<SqlNode>>.Empty;
            // the current token is LPAREN
            while (tok==Sqlx.LPAREN)
            {
                svgs += ParseSqlGraph();
                if (tok == Sqlx.COMMA)
                    Next();
            };
            return svgs;
        }
        CList<SqlNode> ParseSqlGraph()
        {
            // the current token is LPAREN
            var svg = CList<SqlNode>.Empty;
            (var n, svg) = ParseGraphItem(svg);
            while (tok == Sqlx.RARROW || tok == Sqlx.ARROWBASE)
                (n, svg) = ParseGraphItem(svg, n);
            return svg;
        }
        (CTree<long,TGParam>,BList<long?>) ParseSqlMatch(CTree<long,TGParam> tgs)
        {
            // the current token is LPAREN
            var svg = BList<long?>.Empty;
            (var n,svg,tgs) = ParseMatchExp(svg,tgs);
            tgs += n.state;
            lxr.tgs = CTree<long, TGParam>.Empty;
            return (tgs,svg);
        }
        /// <summary>
        /// Graph: Node Path .
        /// Path: { Edge Node }.
        /// Node: '(' GraphItem ')'.
        /// Edge: '-[' GraphItem ']->' | '<-[' GraphItem ']-'.
        /// GraphItem: [Value][Label][doc].
        /// Label: ':' (id|Value)[Label].
        /// </summary>
        /// <param name="svg">The graph fragments so far</param>
        /// <param name="dt">The standard NODETYPE or EDGETYPE</param>
        /// <param name="ln">The node to attach the new edge</param>
        /// <returns>An SqlNode for the new node or edge and the tree of all the graph fragments</returns>
        (SqlNode, CList<SqlNode>) ParseGraphItem(CList<SqlNode> svg, SqlNode? ln = null)
        {
            var ab = tok; // LPAREN, ARROWBASE or RARROW
            var pi = new Ident(this);
            if (tok == Sqlx.ID)
                Next();
            Mustbe(Sqlx.LPAREN, Sqlx.ARROWBASE, Sqlx.RARROW);
            var b = new Ident(this);
            long id = -1L;
            var lp = lxr.Position;
            NodeType? dm = null;
            if (tok == Sqlx.ID)
            {
                var ix = cx.defs[b];
                if (ix == Iix.None)
                {
                    id = lp;
                    cx.defs += (b, b.iix);
                }
                else
                    id = ix.dp;
                Next();
            }
            var lb = BList<long?>.Empty;
            if (tok == Sqlx.COLON)
            {
                Next();
                var a = new Ident(this);
                Mustbe(Sqlx.ID);
                var lv = (cx.obs[cx.defs[a]?.dp ?? -1L] is SqlValue od && od.domain is UDType) ? od :
                    (cx.role.dbobjects[a.ident] is long ap && cx.db.objects[ap] is NodeType nt) ?
                    new SqlLiteral(a.iix.dp, new TTypeSpec(nt))
                    : new SqlValue(a, cx, Domain.TypeSpec);
                cx.defs += (a, a.iix);
                lb += lv.defpos;
                cx.Add(lv);
                while (tok == Sqlx.COLON)
                {
                    Next();
                    var c1 = new Ident(this);
                    Mustbe(Sqlx.ID);
                    var l1 = (cx.obs[cx.defs[c1]?.dp ?? -1L] is SqlValue o1 && o1.domain is UDType) ? o1 :
                        (cx.role.dbobjects[c1.ident] is long a1 && cx.db.objects[a1] is NodeType n1) ?
                        new SqlLiteral(c1.iix.dp, new TTypeSpec(n1))
                        : new SqlValue(c1, cx, Domain.TypeSpec);
                    cx.defs += (c1, c1.iix);
                    lb += l1.defpos;
                    cx.Add(l1);
                }
                if (lb.Last()?.value() is long xt && cx.obs[xt] is SqlValue gl && gl.Eval(cx) is TTypeSpec tt)
                    dm = tt._dataType as NodeType;
            }
            var dc = BTree<long, long?>.Empty;
            CTree<long, bool>? wh = null;
            if (tok == Sqlx.LBRACE)
            {
                Next();
                while (tok != Sqlx.RBRACE)
                {
                    var (n, v) = GetDocItem();
                    dc += (n.defpos, v.defpos);
                    if (tok == Sqlx.COMMA)
                        Next();
                }
                Mustbe(Sqlx.RBRACE);
            }
            SqlNode? an = null;
            var ahead = CList<SqlNode>.Empty;
            if (ln is not null)
            {
                var ba = (ab == Sqlx.ARROWBASE) ? Sqlx.ARROW : Sqlx.RARROWBASE;
                Mustbe(ba);
                (an, ahead) = ParseGraphItem(ahead);
            }
            else
                Mustbe(Sqlx.RPAREN);
            var r = cx.obs[id] as SqlNode;
            if (r == null)
            {
                r = ab switch
                {
                    Sqlx.LPAREN => new SqlNode(b, cx, id, lb, dc, lxr.tgs, dm),
                    Sqlx.ARROWBASE => new SqlEdge(b, cx, ab, id, ln?.defpos ?? -1L, an?.defpos ?? -1L, lb, dc, lxr.tgs, dm),
                    Sqlx.RARROW => new SqlEdge(b, cx, ab, id, an?.defpos ?? -1L, ln?.defpos ?? -1L, lb, dc, lxr.tgs, dm),
                    _ => throw new DBException("42000")
                };
                if (wh is not null)
                    r += (RowSet._Where, wh);
                cx.Add(r);
                cx.defs += (b, new Iix(id));
            }
            svg += r;
            if (an != null)
                svg += ahead;
            return (r, svg);
        }

        /// <summary>
        /// Match: MatchMode [id'='] MatchNode .
        /// MatchNode: '(' MatchItem ')' { (MatchEdge|MatchPath) MatchNode } .
        /// MatchEdge: '-[' MatchItem ']->' | '<-[' MatchItem ']-'.
        /// MatchItem: [Value][Label][doc | WhereClause ].
        /// MatchPath: '[' Match ']' MatchQuantifier .
        /// Label: ':' (id|Value)[Label].
        /// MatchMode: [TRAIL|ACYCLIC|SIMPLE|SHORTEST|ALL|ANY].
        /// MatchQuanitifier : '?' | '*' | '+' | '{'int','[int]'}' .
        /// </summary>
        /// <param name="svg">The graph fragments so far</param>
        /// <param name="dt">The standard NODETYPE or EDGETYPE</param>
        /// <param name="ln">The node to attach the new edge</param>
        /// <returns>An SqlNode for the new node or edge and the tree of all the graph fragments</returns>
        (SqlNode, BList<long?>, CTree<long,TGParam>) ParseMatchExp(BList<long?> svg, CTree<long,TGParam> tgs, Ident? ln = null)
        {
            var st = CTree<long, TGParam>.Empty; // for match
            var ab = tok; // LPAREN, ARROWBASE, RARROW, LBRACK
            var pgg = lxr.tgg;
            if (tok==Sqlx.LBRACK)
                lxr.tgg = TGParam.Type.Group;
            Mustbe(Sqlx.LPAREN, Sqlx.ARROWBASE, Sqlx.RARROW, Sqlx.LBRACK);
            SqlNode? r = null;
            SqlNode? an = null;
            var b = new Ident(this);
            long id = -1L;
            var ahead = BList<long?>.Empty;
            if (ab == Sqlx.LBRACK)
            {
                var (tgp, svp) = ParseSqlMatch(lxr.tgs);
                Mustbe(Sqlx.RBRACK);
                // promote tgp references to arrays
                // we will update cx.defs but not the rest of cx.obs!
                var od = cx.done;
                cx.uids = BTree<long, long?>.Empty;
                for (var tb = tgp.First(); tb != null; tb = tb.Next())
                    if (tb.value() is TGParam g && cx.obs[g.uid] is SqlValue so 
                        && g.type.HasFlag(TGParam.Type.Group))
                    {
                        var nd = (so.domain.defpos < 0) ? g.dataType :
                            cx.Add(new Domain(-1L, Sqlx.ARRAY, so.domain));
                        var no = cx.Add(so.Relocate(cx.GetUid())+(DBObject._Domain, nd));
                        cx.done += (so.defpos,no);
                    }
                cx.defs = cx.defs.ApplyDone(cx);
                cx.done = od;
                lxr.tgg = pgg;
                var qu = (-1, 0);
                if (Match(Sqlx.QMARK, Sqlx.TIMES, Sqlx.PLUS, Sqlx.LBRACE))
                    qu = ParseMatchQuantifier();
                tgs += tgp;
                (var sa, ahead, tgs) = ParseMatchExp(ahead, tgs, ln);
                svp -= (svp.Length - 1); // drop the empty node at the end of the pattern
                r = new SqlPath(cx, svp, qu, ln?.iix.dp ?? -1L, sa.defpos);
            }
            else
            {
                var lp = lxr.Position;
                NodeType? dm = null;
                if (tok == Sqlx.ID)
                {
                    var ix = cx.defs[b];
                    if (lxr.tgs[lp] is TGParam ig)
                        st += (-(int)Sqlx.ID, ig);
                    if (ix == Iix.None)
                    {
                        id = lp;
                        cx.defs += (b, b.iix);
                    }
                    else
                        id = ix.dp;
                    Next();
                }
                var lb = BList<long?>.Empty;
                if (tok == Sqlx.COLON)
                {
                    Next();
                    var a = new Ident(this);
                    Mustbe(Sqlx.ID);
                    var lv = (cx.obs[cx.defs[a]?.dp ?? -1L] is SqlValue od && od.domain is UDType) ? od :
                        (cx.role.dbobjects[a.ident] is long ap && cx.db.objects[ap] is NodeType nt) ?
                        new SqlLiteral(a.iix.dp, new TTypeSpec(nt))
                        : new SqlValue(a, cx, Domain.TypeSpec);
                    cx.defs += (a, a.iix);
                    lb += lv.defpos;
                    if (lxr.tgs[lv.defpos] is TGParam qg)
                        st += (-(int)Sqlx.TYPE, qg);
                    cx.Add(lv);
                    while (tok == Sqlx.COLON)
                    {
                        Next();
                        var c1 = new Ident(this);
                        Mustbe(Sqlx.ID);
                        var l1 = (cx.obs[cx.defs[c1]?.dp ?? -1L] is SqlValue o1 && o1.domain is UDType) ? o1 :
                            (cx.role.dbobjects[c1.ident] is long a1 && cx.db.objects[a1] is NodeType n1) ?
                            new SqlLiteral(c1.iix.dp, new TTypeSpec(n1))
                            : new SqlValue(c1, cx, Domain.TypeSpec);
                        cx.defs += (c1, c1.iix);
                        lb += l1.defpos;
                        cx.Add(l1);
                    }
                    if (lb.Last()?.value() is long xt && cx.obs[xt] is SqlValue gl && gl.Eval(cx) is TTypeSpec tt)
                    {
                        dm = tt._dataType as NodeType;
                        var pg = (dm is not null) ? dm.kind switch
                        {
                            Sqlx.NODETYPE => TGParam.Type.Node,
                            Sqlx.EDGETYPE => TGParam.Type.Edge,
                            Sqlx.ARRAY => TGParam.Type.Path,
                            _ => TGParam.Type.None
                        } : TGParam.Type.Maybe;
                        if (lxr.tgs[lp] is TGParam ig && dm is not null)
                            lxr.tgs += (lp, new TGParam(lp, ig.value, dm, pg));
                    }
                }
                var dc = BTree<long, long?>.Empty;
                CTree<long, bool>? wh = null;
                if (tok == Sqlx.LBRACE)
                {
                    Next();
                    while (tok != Sqlx.RBRACE)
                    {
                        var (n, v) = GetDocItem();
                        dc += (n.defpos, v.defpos);
                        if (tok == Sqlx.COMMA)
                            Next();
                    }
                    Mustbe(Sqlx.RBRACE);
                }
                else if (tok == Sqlx.WHERE)
                {
                    var cd = cx.defs;
                    var ot = lxr.tgs;
                    var od = dm ?? Domain.NodeType;
                    cx.Add(new SqlNode(b, cx, -1L, BList<long?>.Empty, BTree<long, long?>.Empty,
                        CTree<long, TGParam>.Empty, od));
                    cx.defs += (b, b.iix);
                    wh = ParseWhereClause();
                    cx.defs = cd;
                    cx.obs -= b.iix.dp; // wow
                    if (ot is not null) // why is this required?
                        lxr.tgs = ot;
                }
                st += lxr.tgs;
                Sqlx ba = Sqlx.RPAREN;
                if (ln is not null && ab != Sqlx.LBRACK && ab != Sqlx.LPAREN)
                {
                    ba = (ab == Sqlx.ARROWBASE) ? Sqlx.ARROW : Sqlx.RARROWBASE;
                    if (ln.ident != "COLON" && st[ln.iix.dp] is TGParam lg)
                        st += (-(int)ab, lg);
                    if (lxr.tgs != null)
                        lxr.tgs = CTree<long, TGParam>.Empty;
                }
                else if (ab == Sqlx.LBRACK)
                    ba = Sqlx.RBRACK;
                Mustbe(ba);
                r = ab switch
                {
                    Sqlx.LPAREN => new SqlNode(b, cx, id, lb, dc, st, dm),
                    Sqlx.ARROWBASE => new SqlEdge(b, cx, ab, id, ln?.iix.dp ?? -1L, -1L, lb, dc, st, dm),
                    Sqlx.RARROW => new SqlEdge(b, cx, ab, id, -1L, ln?.iix.dp ?? -1L, lb, dc, st, dm),
                    _ => throw new DBException("42000")
                };
                if (wh is not null)
                    r += (RowSet._Where, wh);
            }
            if (Match(Sqlx.LPAREN, Sqlx.ARROWBASE, Sqlx.RARROW, Sqlx.LBRACK))
                (an, ahead, tgs) = ParseMatchExp(ahead, tgs, b);
            if (r is null)
                throw new DBException("42000");
            if (r is SqlEdge)
                r = r.Add(cx, an, st);
            else
                cx.Add(r);
            tgs += r.state;
            if (id > 0)
                cx.defs += (b, new Iix(id));
                svg += r.defpos;
            svg += ahead;
            return (r, svg, tgs);
        }
        /// <summary>
        /// MatchStatement: MATCH Match {',' Match} [WhereClause] [[THEN] Statement].
        /// Match: MatchMode Graph
        /// </summary>
        /// <returns></returns>
        /// <exception cref="DBException"></exception>
        internal Executable ParseSqlMatchStatement()
        {
            var olddefs = cx.defs; // we will remove any defs introduced by the Match below
                                   // while allowing existing defs to be updated by the Match parser
            Next();
            lxr.tgs = CTree<long, TGParam>.Empty;
            lxr.ParsingMatch = true;
            var (tgs,svgs) = ParseSqlMatchList();
            lxr.ParsingMatch = false;
            var wh = ParseWhereClause() ?? CTree<long, bool>.Empty;
            long e = -1L;
            if (tok != Sqlx.EOF && tok != Sqlx.END && tok != Sqlx.RPAREN)
            {
                var op = cx.parse;
                cx.parse = ExecuteStatus.Parse;
                e = (ParseProcedureStatement(Domain.Content) is Executable ex) ? ex.defpos : -1L;
                cx.parse = op;
            }
            if (tgs is null)
                throw new DBException("PE60201");
            var ms = new MatchStatement(cx, tgs, svgs, wh, e);
            ms = (MatchStatement)cx.Add(ms);
            if (cx.parse == ExecuteStatus.Obey)
                ms.Obey(cx);
            if (tok == Sqlx.THEN)
            {
                Next();
                if (ParseProcedureStatement(Domain.Content) is Executable te)
                {
                    cx.Add(te + (DBObject._From,ms.defpos));
                    ms += (IfThenElse.Then, te.defpos);
                    cx.Add(ms);
                }
                Mustbe(Sqlx.END);
            }
            for (var b = cx.defs.First(); b != null; b = b.Next())
                if (!olddefs.Contains(b.key()))
                    cx.defs -= b.key();
            return ms;
        }
        (int,int) ParseMatchQuantifier()
        {
            var mm = tok;
            Next(); 
            int l = 0, h = -1; // -1 indicates unbounded
            switch (mm)
            {
                case Sqlx.QMARK: h = 1; break;
                case Sqlx.TIMES: break;
                case Sqlx.PLUS: l = 1; break;
                case Sqlx.LBRACE:
                    if (lxr.val is TInt tl)
                        l = (int)tl.value;
                    Mustbe(Sqlx.INTEGERLITERAL);
                    Mustbe(Sqlx.COMMA);
                    if (lxr.val is TInt th)
                        h = (int)th.value;
                    if (tok == Sqlx.INTEGERLITERAL)
                        Next();
                    Mustbe(Sqlx.RBRACE);
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
                Mustbe(Sqlx.ID);
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
            if (Match(Sqlx.LPAREN))
            {
                Next();
                for (var i = 0; ; i++)
                {
                    var n = lxr.val.ToString();
                    var np = LexPos();
                    Mustbe(Sqlx.ID);
                    ts += (np.dp, new ObInfo(n));
                    if (Mustbe(Sqlx.COMMA, Sqlx.RPAREN) == Sqlx.RPAREN)
                        break;
                }
            }
            else if (Match(Sqlx.OF))
            {
                cx.parse = ExecuteStatus.Compile;
                Next();
                lp = LexPos();
                sl = lxr.start;
                if (Match(Sqlx.LPAREN)) // inline type def (RestView only)
                    dm = (Domain)cx.Add(ParseRowTypeSpec(Sqlx.VIEW));
                else
                {
                    var tn = lxr.val.ToString();
                    Mustbe(Sqlx.ID);
                    dm = ((cx.db.role.dbobjects[tn] is long p) ? cx._Dom(p) : null) ??
                        throw new DBException("42119", tn, "").Mix();
                }
            }
            Mustbe(Sqlx.AS);
            var rest = Match(Sqlx.GET);
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
                        if (ub.value() is long u && cx.obs[u] is SqlValue v && b.value().name is string nn)
                            cx.Add(v + (DBObject._Alias, nn));
                }
                cx.Add(new SelectStatement(cx.GetUid(), cs));
                cs = (RowSet?)cx.obs[cs.defpos] ?? throw new PEException("PE1802");
                var nb = ts.First();
                for (var b = cs.rowType.First(); b != null; b = b.Next(), nb = nb?.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                    {
                        if (v.domain.kind == Sqlx.CONTENT || v.defpos < 0) // can't simply use WellDefined
                            throw new DBException("42112", v.NameFor(cx));
                        if (nb != null && nb.value().name is string bn)
                            cx.Add(v + (DBObject._Alias, bn));
                    }
                for (var b = cx.forReview.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValue k)
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is RowSet rs && rs is not SelectRowSet)
                                rs.Apply(new BTree<long, object>(RowSet._Where, new CTree<long, bool>(k.defpos, true)), cx);
                cx.parse = op;
            }
            else
            {
                Next();
                if (tok == Sqlx.USING)
                {
                    Next();
                    ur = ParseTableReferenceItem(lp.lp,Domain.TableType);
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
            var vw = (View)(cx.Add(pv) ?? throw new DBException("42105"));
            if (StartMetadata(Sqlx.VIEW))
            {
                var m = ParseMetadata(Sqlx.VIEW);
                if (vw != null && m != null)
                    cx.Add(new PMetadata(id, -1, vw, m, cx.db.nextPos));
            }
            cx.result = -1L;
            return cs; // cs is null for PRestViews
        }
        /// <summary>
        /// Parse the CreateXmlNamespaces syntax
        /// </summary>
        private void ParseCreateXmlNamespaces()
        {
            Next();
            var ns = (XmlNameSpaces)ParseXmlNamespaces();
            cx.nsps += (ns.nsps, false);
            for (var s = ns.nsps.First(); s != null; s = s.Next())
                cx.Add(new Namespace(s.key(), s.value(), cx.db.nextPos));
        }
        /// <summary>
        /// Parse the Create ordering syntax:
        /// FOR Domain_id (EQUALS|ORDER FULL) BY (STATE|(MAP|RELATIVE) WITH Func_id)  
        /// </summary>
        private void ParseCreateOrdering()
        {
            Next();
            Mustbe(Sqlx.FOR);
            var n = new Ident(this);
            Mustbe(Sqlx.ID);
            Domain ut = ((cx.role.dbobjects[n.ident]is long p)?cx.db.objects[p] as Domain:null)
                ?? throw new DBException("42133", n).Mix();
            OrderCategory fl;
            if (Match(Sqlx.EQUALS))
            {
                fl = OrderCategory.Equals;
                Next();
                Mustbe(Sqlx.ONLY);
            }
            else
            {
                fl = OrderCategory.Full;
                Mustbe(Sqlx.ORDER); Mustbe(Sqlx.FULL);
            }
            Mustbe(Sqlx.BY);
            Sqlx smr = Mustbe(Sqlx.STATE, Sqlx.MAP, Sqlx.RELATIVE);
            if (smr == Sqlx.STATE)
            {
                fl |= OrderCategory.State;
                cx.Add(new Ordering(ut, -1L, fl, cx.db.nextPos, cx));
            }
            else
            {
                fl |= ((smr == Sqlx.RELATIVE) ? OrderCategory.Relative : OrderCategory.Map);
                Mustbe(Sqlx.WITH);
                var (fob, nf) = ParseObjectName();
                var func = fob as Procedure ?? throw new DBException("42000");
                if (smr == Sqlx.RELATIVE && func.arity != 2)
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
            bool b = (tok == Sqlx.LPAREN);
            if (b)
                Next();
            var r = BList<Ident>.Empty;
            r += ParseIdent();
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParseIdent();
            }
            if (b)
                Mustbe(Sqlx.RPAREN);
            return r;
        }
        /// <summary>
		/// Cols =		'('ColRef { ',' ColRef } ')'.
        /// </summary>
        /// <returns>a tree of coldefpos: returns null if input is (SELECT</returns>
		Domain? ParseColsList(Domain ob)
        {
            var r = BList<DBObject>.Empty;
            bool b = tok == Sqlx.LPAREN;
            if (b)
                Next();
            if (tok == Sqlx.SELECT)
                return null;
            r+=ParseColRef(ob);
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParseColRef(ob);
            }
            if (b)
                Mustbe(Sqlx.RPAREN);
            return new Domain(cx.GetUid(), cx, Sqlx.TABLE, r, r.Length);
        }
        /// <summary>
        /// ColRef = id { '.' id } .
        /// </summary>
        /// <returns>+- seldefpos</returns>
        DBObject ParseColRef(DBObject ta)
        {
            if (tok == Sqlx.PERIOD)
            {
                Next();
                var pn = lxr.val;
                Mustbe(Sqlx.ID);
                var tb = ((ta is NodeType et) ? et.super : ta) as Table
                    ?? throw new DBException("42162", pn).Mix();
                if (cx.db.objects[tb.applicationPS] is not PeriodDef pd || pd.NameFor(cx) != pn.ToString())
                    throw new DBException("42162", pn).Mix();
                return (PeriodDef)cx.Add(pd);
            }
            // We will raise an exception if the column does not exist
            var id = new Ident(this);
            var p = ta.ColFor(cx, id.ident);
            var tc = cx.obs[p]??cx.db.objects[p] as DBObject??
                throw new DBException("42112", id.ident).Mix();
            Mustbe(Sqlx.ID);
            // We will construct paths as required for any later components
            while (tok == Sqlx.DOT)
            {
                Next();
                var pa = new Ident(this);
                Mustbe(Sqlx.ID);
                if (tc is TableColumn c)
                    tc = new ColumnPath(pa.iix.dp,pa.ident,c,cx.db); // returns a (child)TableColumn for non-documents
                long dm = -1;
                if (tok == Sqlx.AS)
                {
                    Next();
                    tc = (TableColumn)tc.New(cx.GetUid(),tc.mem+ParseSqlDataType().mem);
                }
                if (cx.db.objects[ta.ColFor(cx,pa.ident)] is TableColumn cc
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
            Ident? undername = null;
            UDType? under = null;
            UDType dt = Domain.TypeSpec;
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            var pp = cx.db.nextPos;
            if (cx.tr == null)
                throw new DBException("2F003");
            Mustbe(Sqlx.ID);
            if (cx.role.dbobjects.Contains(typename.ident))
                throw new DBException("42104", typename.ident);
            if (Match(Sqlx.UNDER))
            {
                Next();
                undername = new Ident(this);
                if (!cx.role.dbobjects.Contains(undername.ident))
                    throw new DBException("42107", undername.ident);
                Mustbe(Sqlx.ID,Sqlx.NUMERIC,Sqlx.INTEGER,Sqlx.CHAR,Sqlx.REAL);
            }
            if (undername != null)
            {
                if ((cx.GetObject(undername.ident)??
                    StandardDataType.Get(dt.kind) ??
                    throw cx.db.Exception("42119", undername).Pyrrho()) is Domain udm)
                {
                    if (udm is UDType type)
                        under = type;
                    else
                        under = (UDType)cx.Add(dt.New(udm.defpos, udm.mem + (Table.PathDomain, udm)));
                }
            }
            if (tok == Sqlx.AS)
            {
                Next();
                if (tok == Sqlx.LPAREN)
                    dt = (UDType)ParseRowTypeSpec(dt.kind, typename, under);
                else
                {
                    var d = ParseStandardDataType() ??
                        throw new DBException("42161", "StandardType", lxr.val.ToString()).Mix();
                    if (d == Domain.Null)
                        throw new DBException("42000", tok, " following AS");
                    dt = new UDType(d.defpos,d.mem);
                }
            } else
            {
                var pt = new PType(typename.ident, Domain.TypeSpec, under, -1L, cx.db.nextPos, cx);
                cx.Add(pt);
                dt = (UDType)pt.dataType;
            }
            if (Match(Sqlx.RDFLITERAL))
            {
                RdfLiteral rit = (RdfLiteral)lxr.val;
                dt += (ObInfo.Name, rit.val as string??"");
                Next();
            }
            // Here we have the first change from the introduction of the typed graph model
            // UNDER may have specified a node or edge type. If so, we change the 
            // user-defined type dt created by lines 1583/1597/1603 above into a NodeType or EdgeType
            // respectively.
            // (If it is still a UDType it may get changed to one of these
            // by metadata. We will deal with that if it happens below.)
            if (under is EdgeType e1)
            {
                if (dt is not EdgeType et)
                    et = new EdgeType(cx.GetUid(), typename.ident, Domain.EdgeType, e1, cx);
                dt = et.FixEdgeType(cx,typename);
            }
            else if (under is NodeType n1)
            {
                if (dt is not NodeType nt)
                    nt = new NodeType(cx.GetUid(), typename.ident, Domain.NodeType, n1, cx);
                dt = nt.FixNodeType(cx,typename);
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
            if (Match(Sqlx.OVERRIDING, Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.METHOD))
            {
                cx.obs = ObTree.Empty;
                dt = ParseMethodHeader(dt) ?? throw new PEException("PE42140");
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    cx.obs = ObTree.Empty;
                    dt = ParseMethodHeader(dt) ?? throw new PEException("PE42141");
                }
            }
            // it is a syntax error to add NODETPE/EDGETYPE metadata to something that is already
            // a node type or edge type
            if (dt is not NodeType && StartMetadata(Sqlx.TYPE))
            {
                // BuildNodeType is also used for the CREATE NODE syntax, which is
                // part of the DML, and so any new properties added here are best prepared with
                // SqlValues/SqlLiterals instead of Domains/TableColumns.
                // We prepare a useful tree of all the columns we know about in case their names
                // occur in the metadata.
                var ls = CTree<string,SqlValue>.Empty;
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is TableColumn tc 
                            && tc.infos[cx.role.defpos] is ObInfo ti && ti.name is string cn)
                        ls += (cn,new SqlLiteral(p, cn, tc.domain.defaultValue, tc.domain));
                var m = ParseMetadata(Sqlx.TYPE);
                // The metadata contains aliases for special columns etc
                if (m.Contains(Sqlx.NODETYPE))
                {
                    var nt = new NodeType(typename.iix.dp,dt.mem);
                    nt = nt.FixNodeType(cx,typename);
                    // Process ls and m 
                    (dt,ls) = nt.Build(cx,nt, ls, m);
                    // and fix the PType to be a PNodeType
                }
                else if (m.Contains(Sqlx.EDGETYPE) && dt is not NodeType) // or EdgeType
                {
                    if (dt is not EdgeType et)
                        et = new EdgeType(dt.defpos, typename.ident, dt, null, cx, m);
                    et = et.FixEdgeType(cx,typename);
                    (dt,ls) = et.Build(cx, et, ls, m);
                }
                else if (m != CTree<Sqlx, TypedValue>.Empty)
                    cx.Add(new PMetadata(typename.ident, -1, dt, m, cx.db.nextPos)); 
            }
            cx.parse = op;
        }
        /// <summary>
        /// Method =  	MethodType METHOD id '(' Parameters ')' [RETURNS Type] [FOR id].
        /// MethodType = 	[ OVERRIDING | INSTANCE | STATIC | CONSTRUCTOR ] .
        /// </summary>
        /// <returns>the methodname parse class</returns>
        MethodName ParseMethod(Domain? xp=null)
        {
            MethodName mn = new()
            {
                methodType = PMethod.MethodType.Instance
            };
            switch (tok)
            {
                case Sqlx.OVERRIDING: Next(); mn.methodType = PMethod.MethodType.Overriding; break;
                case Sqlx.INSTANCE: Next(); break;
                case Sqlx.STATIC: Next(); mn.methodType = PMethod.MethodType.Static; break;
                case Sqlx.CONSTRUCTOR: Next(); mn.methodType = PMethod.MethodType.Constructor; break;
            }
            Mustbe(Sqlx.METHOD);
            mn.name = new Ident(lxr.val.ToString(), cx.Ix(cx.db.nextPos));
            Mustbe(Sqlx.ID);
            int st = lxr.start;
            if (mn.name is not Ident nm)
                throw new DBException("42000");
            mn.ins = ParseParameters(mn.name,xp);
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
            if (tok == Sqlx.FOR)
            {
                Next();
                var tname = new Ident(this);
                var ttok = tok;
                Mustbe(Sqlx.ID,Sqlx.NUMERIC,Sqlx.CHAR,Sqlx.INTEGER,Sqlx.REAL);
                xp = (Domain?)cx.db.objects[cx.role.dbobjects[tname.ident]??-1L]??
                    StandardDataType.Get(ttok);
            } else if (mn.methodType==PMethod.MethodType.Constructor) 
                xp = (Domain?)cx.db.objects[cx.role.dbobjects[mn.name.ident]??-1L];
            mn.type = xp as UDType ?? throw new DBException("42000", "Constructor?").ISO();
            return mn;
        }
        /// <summary>
        /// Define a new method header (calls ParseMethod)
        /// </summary>
        /// <param name="xp">the UDTtype if we are creating a Type</param>
        /// <returns>maybe a type</returns>
		UDType? ParseMethodHeader(Domain? xp=null)
        {
            MethodName mn = ParseMethod(xp);
            if (mn.name is not Ident nm || mn.retType==null || mn.type==null)
                throw new DBException("42000");
            var r = new PMethod(nm.ident, mn.ins,
                mn.retType, mn.methodType, mn.type, null,
                new Ident(mn.signature, nm.iix), cx.db.nextStmt, cx.db.nextPos, cx);
            cx.Add(r);
            return (xp is not null)?(UDType?)cx._Ob(xp.defpos):null;
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
                throw new DBException("42000");
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
                    var pa = (FormalParameter?)cx.obs[p] ?? throw new DBException("42000");
                    var px = cx.Ix(pa.defpos);
                    cx.defs += (new Ident(pa.name ?? "", Iix.None), px);
                }
            ut.Defs(cx);
            meth += (Procedure.Body, 
                (ParseProcedureStatement(mn.retType) ?? throw new DBException("42000")).defpos);
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
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.AS)
                Next();
            var type = ParseSqlDataType();
            if (tok == Sqlx.DEFAULT)
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
            while (Match(Sqlx.NOT, Sqlx.CONSTRAINT, Sqlx.CHECK))
                if (ParseCheckConstraint(pd.dataType) is PCheck ck)
                     a.Add(ck);
            if (tok == Sqlx.COLLATE)
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
            Mustbe(Sqlx.ID);
            return collate.ToString();
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] SqlValue ')' .
        /// </summary>
        /// <param name="pd">the domain</param>
        /// <returns>the PCheck object resulting from the parse</returns>
        PCheck? ParseCheckConstraint(Domain dm)
        {
            var oc = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            var o = new Ident(this);
            Ident n;
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
                n = o;
                Mustbe(Sqlx.ID);
            }
            else
                n = new Ident(this);
            Mustbe(Sqlx.CHECK);
            Mustbe(Sqlx.LPAREN);
            var nst = cx.db.nextStmt;
            var st = lxr.start;
            var se = ParseSqlValue(Domain.Bool).Reify(cx);
            se.Validate(cx);
            Mustbe(Sqlx.RPAREN);
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
            Mustbe(Sqlx.ID);
            if (cx.db.schema.dbobjects.Contains(name.ident) || cx.role.dbobjects.Contains(name.ident))
                throw new DBException("42104", name);
            var pt = new PTable(name.ident, new Domain(-1L, cx, Sqlx.TABLE, BList<DBObject>.Empty), 
                cx.db.nextPos, cx);
            var tb = cx.Add(pt)??throw new DBException("42105");
            tb = ParseTableContentsSource((Table)tb);
            if (tok == Sqlx.RDFLITERAL && 
                lxr.val is RdfLiteral rit && rit.val is string ri)
            {
                tb += (ObInfo.Name, ri);
                tb = (Table)cx.Add(tb); // FIX this
                Next();
            }
            if (Match(Sqlx.SECURITY))
            {
                Next();
                if (Match(Sqlx.LEVEL))
                    tb = ParseClassification(tb);
                if (tok == Sqlx.SCOPE)
                    tb = ParseEnforcement((Table)tb);
            }
            if (StartMetadata(Sqlx.TABLE))
            {
                var dp = LexDp();
                var md = ParseMetadata(Sqlx.TABLE);
                cx.Add(new PMetadata(name.ident,dp,tb,md,cx.db.nextPos));
            }
        }
        DBObject ParseClassification(DBObject ob)
        {
            var lv = MustBeLevel();
            var pc = new Classify(ob.defpos, lv, cx.db.nextPos);
            ob = cx.Add(pc) ?? throw new DBException("42105");
            return ob;
        }
        /// <summary>
        /// Enforcement = SCOPE [READ] [INSERT] [UPDATE] [DELETE]
        /// </summary>
        /// <returns></returns>
        Table ParseEnforcement(Table tb)
        {
            if (cx.db  is null || cx.db.user is null || cx.db.user.defpos != cx.db.owner)
                throw new DBException("42105");
            Mustbe(Sqlx.SCOPE);
            Grant.Privilege r = Grant.Privilege.NoPrivilege;
            while (Match(Sqlx.READ, Sqlx.INSERT, Sqlx.UPDATE, Sqlx.DELETE))
            {
                switch (tok)
                {
                    case Sqlx.READ: r |= Grant.Privilege.Select; break;
                    case Sqlx.INSERT: r |= Grant.Privilege.Insert; break;
                    case Sqlx.UPDATE: r |= Grant.Privilege.Update; break;
                    case Sqlx.DELETE: r |= Grant.Privilege.Delete; break;
                }
                Next();
            }
            var pe = new Enforcement(tb, r, cx.db.nextPos);
            tb = (Table)(cx.Add(pe) ?? throw new DBException("42105"));
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
                case Sqlx.LPAREN:
                    Next();
                    tb = ParseTableItem(tb);
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        tb = ParseTableItem(tb);
                    }
                    Mustbe(Sqlx.RPAREN);
                    while (Match(Sqlx.WITH))
                        tb = ParseVersioningClause(tb, false);
                    break;
                case Sqlx.OF:
                    {
                        Next();
                        var id = ParseIdent();
                        var udt = cx.db.objects[cx.role.dbobjects[id.ident]??-1L] as Domain??
                            throw new DBException("42133", id.ToString()).Mix();
                        var tr = cx.db as Transaction?? throw new DBException("2F003");
                        for (var cd = udt.rowType.First(); cd != null; cd = cd.Next())
                            if (cd.value() is long p && cx.db.objects[p] is TableColumn tc &&
                                tc.infos[cx.role.defpos] is ObInfo ci && ci.name is not null)
                            {
                                var pc = new PColumn2(tb, ci.name, cd.key(), tc.domain,
                                    tc.generated.gfs ?? tc.domain.defaultValue?.ToString() ?? "",
                                    tc.domain.defaultValue ?? TNull.Value, tc.domain.notNull,
                                    tc.generated, tr.nextPos, cx);
                                tb  = (Table?)cx.Add(pc)?? throw new DBException("42105");
                            }
                        if (Match(Sqlx.LPAREN))
                        {
                            for (; ; )
                            {
                                Next();
                                if (Match(Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.FOREIGN))
                                    tb = ParseTableConstraintDefin(tb);
                                else
                                {
                                    id = ParseIdent();
                                    var se = (TableColumn?)cx.db.objects[udt.ColFor(cx,id.ident)]
                                        ??throw new DBException("42112",id.ident);
                                    ParseColumnOptions(tb, se);
                                    tb = (Table?)cx.obs[tb.defpos] ?? throw new PEException("PE1711");
                                }
                                if (!Match(Sqlx.COMMA))
                                    break;
                            }
                            Mustbe(Sqlx.RPAREN);
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
            Mustbe(Sqlx.SYSTEM, Sqlx.APPLICATION);
            var vi = (sa == Sqlx.SYSTEM) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            var pi = tb.FindPrimaryIndex(cx) ?? throw new DBException("42000");
            if (drop)
            {
                var fl = (vi == 0) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
                Physical? ph=null;
                for (var e = tb.indexes.First(); e != null; e = e.Next())
                    for (var c = e.value().First(); c != null; c = c.Next())
                        if (cx.db.objects[c.key()] is Level3.Index px &&
                                    px.tabledefpos == tb.defpos && (px.flags & fl) == fl)
                            ph = new Drop(px.defpos, tr.nextPos);
                if (sa == Sqlx.SYSTEM && cx.db.objects[tb.systemPS] is PeriodDef pd)
                    ph = new Drop(pd.defpos, tr.nextPos);
                Mustbe(Sqlx.VERSIONING);
                if (ph is not null)
                    tb = (Table)(cx.Add(ph) ?? throw new DBException("42105"));
                return tb;
            }
            var ti = tb.infos[cx.role.defpos];
            if (ti==null || sa == Sqlx.APPLICATION)
                throw new DBException("42164", tb.NameFor(cx)).Mix();
            pi = (Level3.Index)(cx.Add(new PIndex("", tb, pi.keys,
                    PIndex.ConstraintType.PrimaryKey | vi,-1L, tr.nextPos))
                ?? throw new DBException("42105"));
            Mustbe(Sqlx.VERSIONING);
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
            var es = ExecuteList.Empty;
            Mustbe(Sqlx.WITH);
            Mustbe(Sqlx.OPTIONS);
            Mustbe(Sqlx.LPAREN);
            for (; ; )
            {
                switch (tok)
                {
                    case Sqlx.SCOPE: Next();
                        Mustbe(Sqlx.ID); // TBD
                        break;
                    case Sqlx.DEFAULT:
                        {
                            Next();
                            int st = lxr.start;
                            var dv = ParseSqlValue(tc.domain);
                            var ds = new string(lxr.input, st, lxr.start - st);
                            tc = tc + (Domain.Default, dv) + (Domain.DefaultString, ds);
                            cx.db += (tc, cx.db.loadpos);
                            break;
                        }
                    default: tb = ParseColumnConstraint(tb, tc);
                        break;
                }
                if (Match(Sqlx.COMMA))
                    Next();
                else
                    break;
            }
            Mustbe(Sqlx.RPAREN);
        }
        /// <summary>
        /// TableClause =	ColumnDefinition | TableConstraintDef | TablePeriodDefinition .
        /// </summary>
        Table ParseTableItem(Table tb)
        {
            if (Match(Sqlx.PERIOD))
                tb = AddTablePeriodDefinition(tb);
            else if (tok == Sqlx.ID)
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
            Mustbe(Sqlx.FOR);
            if (Match(Sqlx.SYSTEM_TIME))
                Next();
            else
            {
                r.periodname = new Ident(this);
                Mustbe(Sqlx.ID);
                r.pkind = Sqlx.APPLICATION;
            }
            Mustbe(Sqlx.LPAREN);
            r.col1 = ParseIdent();
            Mustbe(Sqlx.COMMA);
            r.col2 = ParseIdent();
            Mustbe(Sqlx.RPAREN);
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
            if (ptd.col1==null || ptd.col2==null)
                throw new DBException("42105");
            var c1 = (TableColumn?)cx.db.objects[tb.ColFor(cx,ptd.col1.ident)];
            var c2 = (TableColumn?)cx.db.objects[tb.ColFor(cx,ptd.col2.ident)];
            if (c1 is null)
                throw new DBException("42112", ptd.col1).Mix();
            if (c1.domain.kind != Sqlx.DATE && c1.domain.kind != Sqlx.TIMESTAMP)
                throw new DBException("22005R", "DATE or TIMESTAMP", c1.ToString()).ISO()
                    .AddType(Domain.UnionDate).AddValue(c1.domain);
            if (c2 == null)
                throw new DBException("42112", ptd.col2).Mix();
            if (c2.domain.kind != Sqlx.DATE && c2.domain.kind != Sqlx.TIMESTAMP)
                throw new DBException("22005R", "DATE or TIMESTAMP", c2.ToString()).ISO()
                    .AddType(Domain.UnionDate).AddValue(c1.domain);
            if (c1.domain.CompareTo(c2)!=0)
                throw new DBException("22005S", c1.ToString(), c2.ToString()).ISO()
                    .AddType(c1.domain).AddValue(c2.domain);
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            var pd = new PPeriodDef(tb, ptd.periodname.ident, c1.defpos, c2.defpos, tr.nextPos, cx);
            return (Table)(cx.Add(pd) ?? throw new DBException("42105"));
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
            if (Match(Sqlx.COLUMN))
                Next();
            var colname = new Ident(this);
            var lp = colname.iix;
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.ID)
            {
                var op = cx.db.role.dbobjects[new Ident(this).ident];
                type = cx.db.objects[op??-1L] as Domain
                    ?? throw new DBException("42119", lxr.val.ToString());
                Next();
            }
            else if (Match(Sqlx.CHARACTER, Sqlx.CHAR, Sqlx.VARCHAR, Sqlx.NATIONAL, Sqlx.NCHAR,
                Sqlx.BOOLEAN, Sqlx.NUMERIC, Sqlx.DECIMAL,
                Sqlx.DEC, Sqlx.FLOAT, Sqlx.REAL, Sqlx.DOUBLE,
                Sqlx.INT, Sqlx.INTEGER, Sqlx.BIGINT, Sqlx.SMALLINT, Sqlx.PASSWORD,
                Sqlx.BINARY, Sqlx.BLOB, Sqlx.NCLOB, Sqlx.CLOB, Sqlx.XML,
                Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL,
                Sqlx.DOCUMENT, Sqlx.DOCARRAY, Sqlx.CHECK,
#if MONGO
                Sqlx.OBJECT, // v5.1
#endif
                Sqlx.ROW, Sqlx.TABLE, Sqlx.REF))
                type = ParseSqlDataType();
            dom = type;
            if (Match(Sqlx.ARRAY,Sqlx.SET,Sqlx.MULTISET))
            {
                dom = (Domain)cx.Add(new Domain(cx.GetUid(),tok, type));
                cx.Add(dom);
                Next();
            }
            var ua = CTree<UpdateAssignment,bool>.Empty;
            var gr = GenerationRule.None;
            var dfs = "";
            var nst = cx.db.nextStmt;
            TypedValue dv = TNull.Value;
            if (tok == Sqlx.DEFAULT)
            {
                Next();
                int st = lxr.start;
                dv = lxr.val;
                dfs = new string(lxr.input, st, lxr.pos - st);
                Next();
            }
            else if (Match(Sqlx.GENERATED))
            {
                dv = dom.defaultValue ?? TNull.Value;
                // Set up the information for parsing the generation rule
                // The table domain and cx.defs should contain the columns so far defined
                var oc = cx;
                cx = cx.ForConstraintParse();
                gr = ParseGenerationRule(lp.dp,dom);
                dfs = gr.gfs;
                oc.DoneCompileParse(cx);
                cx = oc;
            }
            if (dom == null)
                throw new DBException("42120", colname.ident);
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            var pc = new PColumn3(tb, colname.ident, -1, dom,
                dfs, dv, "", ua, false, gr, tr.nextPos, cx);
            cx.Add(pc);
            tb = (Table?)cx.obs[tb.defpos] ?? tb;
            var tc = (TableColumn)(cx.db.objects[pc.defpos]??throw new PEException("PE50100"));
            if (gr.exp >= 0)
                tc = cx.Modify(pc, tc);
            while (Match(Sqlx.NOT, Sqlx.REFERENCES, Sqlx.CHECK, Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.CONSTRAINT,
                Sqlx.SECURITY))
            {
                var oc = cx;
                nst = cx.db.nextStmt;
                cx = cx.ForConstraintParse();
                tb = ParseColumnConstraintDefin(tb, pc, tc);
                tb = (Table?)cx.obs[tb.defpos] ?? throw new PEException("PE1570");
                oc.DoneCompileParse(cx);
                cx = oc;
                tc = cx.Modify(pc,(TableColumn)(cx.obs[tc.defpos]??throw new PEException("PE15070")));
                tb = cx.Modify(tb, nst); // maybe the framing changes since nst are irrelevant??
            }
            var od = dom;
            if (type != null && tok == Sqlx.COLLATE)
                tc += (Domain.Culture,new CultureInfo(ParseCollate()));
            if (StartMetadata(Sqlx.TYPE))
            {
                var md = ParseMetadata(Sqlx.TYPE);
                var tn = cx.NameFor(tc.defpos);
                var pm = new PMetadata(tn, 0, tc, md, cx.db.nextPos);
                cx.Add(pm);
                var oi = tc.infos[cx.role.defpos] ?? new ObInfo(tn);
                oi += (ObInfo._Metadata,md);
                tc += (DBObject.Infos, tc.infos + (cx.role.defpos, oi));
            }
            if (dom != od)
            {
                dom = (Domain)dom.Relocate(cx.GetUid());
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
        bool StartMetadata(Sqlx kind)
        {
            switch (kind)
            {
                case Sqlx.TABLE: return Match(Sqlx.ENTITY, Sqlx.PIE, Sqlx.HISTOGRAM, Sqlx.LEGEND, Sqlx.LINE, 
                    Sqlx.POINTS, Sqlx.DROP,Sqlx.JSON, Sqlx.CSV, Sqlx.CHARLITERAL, Sqlx.RDFLITERAL, Sqlx.REFERRED, 
                    Sqlx.ETAG, Sqlx.SECURITY);
                case Sqlx.COLUMN: return Match(Sqlx.ATTRIBUTE, Sqlx.X, Sqlx.Y, Sqlx.CAPTION, Sqlx.DROP, 
                    Sqlx.CHARLITERAL, Sqlx.RDFLITERAL,Sqlx.REFERS, Sqlx.SECURITY,Sqlx.MULTIPLICITY);
                case Sqlx.FUNCTION: return Match(Sqlx.ENTITY, Sqlx.PIE, Sqlx.HISTOGRAM, Sqlx.LEGEND,
                    Sqlx.LINE, Sqlx.POINTS, Sqlx.DROP, Sqlx.JSON, Sqlx.CSV, Sqlx.INVERTS, Sqlx.MONOTONIC);
                case Sqlx.VIEW: return Match(Sqlx.ENTITY, Sqlx.URL, Sqlx.MIME, Sqlx.SQLAGENT, Sqlx.USER, 
                    Sqlx.PASSWORD,Sqlx.CHARLITERAL,Sqlx.RDFLITERAL,Sqlx.ETAG,Sqlx.MILLI);
                case Sqlx.TYPE: return Match(Sqlx.PREFIX, Sqlx.SUFFIX, Sqlx.NODETYPE, Sqlx.EDGETYPE, 
                    Sqlx.SENSITIVE, Sqlx.CARDINALITY);
                case Sqlx.ANY:
                    Match(Sqlx.DESC, Sqlx.URL, Sqlx.MIME, Sqlx.SQLAGENT, Sqlx.USER, Sqlx.PASSWORD,
                        Sqlx.ENTITY,Sqlx.PIE,Sqlx.HISTOGRAM,Sqlx.LEGEND,Sqlx.LINE,Sqlx.POINTS,Sqlx.REFERRED,
                        Sqlx.ETAG,Sqlx.ATTRIBUTE,Sqlx.X,Sqlx.Y,Sqlx.CAPTION,Sqlx.REFERS,Sqlx.JSON,Sqlx.CSV,
                        Sqlx.INVERTS,Sqlx.MONOTONIC, Sqlx.PREFIX, Sqlx.SUFFIX);
                    return !Match(Sqlx.EOF,Sqlx.RPAREN,Sqlx.COMMA,Sqlx.RBRACK,Sqlx.RBRACE);
                default: return Match(Sqlx.CHARLITERAL, Sqlx.RDFLITERAL);
            }
        }
        internal CTree<Sqlx,TypedValue> ParseMetadata(string s,int off,Sqlx kind)
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
        internal CTree<Sqlx, TypedValue> ParseMetadata(Sqlx kind)
        {
            var drop = false;
            if (Match(Sqlx.ADD, Sqlx.DROP))
            {
                drop = tok == Sqlx.DROP;
                Next();
            }
            var m = CTree<Sqlx, TypedValue>.Empty;
            TypedValue ds = TNull.Value;
            TypedValue iri = TNull.Value;
            long iv = -1;
            var lp = tok == Sqlx.LPAREN;
            if (lp)
                Next();
            while (StartMetadata(kind))
            {
                var o = lxr.val;
                switch (tok)
                {
                    case Sqlx.CHARLITERAL:
                        ds = drop ? new TChar("") : o;
                        break;
                    case Sqlx.INVERTS:
                        {
                            Next();
                            if (tok == Sqlx.EQL)
                                Next();
                            var x = cx.GetObject(o.ToString()) ??
                                throw new DBException("42108", lxr.val.ToString()).Pyrrho();
                            iv = x.defpos;
                            break;
                        }
                    case Sqlx.DESC:
                    case Sqlx.URL:
                        {
                            if (drop)
                                break;
                            var t = tok;
                            Next();
                            if (tok == Sqlx.EQL)
                                Next();
                            if (tok == Sqlx.CHARLITERAL || tok == Sqlx.RDFLITERAL)
                                m += (t, lxr.val);
                            break;
                        }
                    case Sqlx.HISTOGRAM:
                    case Sqlx.LINE:
                    case Sqlx.PIE:
                    case Sqlx.POINTS:
                        {
                            if (drop)
                                o = new TChar("");
                            m += (tok, o);
                            Next();
                            if (tok==Sqlx.LPAREN)
                            {
                                Next();
                                m += (Sqlx.X, lxr.val);
                                Mustbe(Sqlx.ID);
                                Mustbe(Sqlx.COMMA);
                                m += (Sqlx.Y, lxr.val);
                                Mustbe(Sqlx.ID);
                             } else
                                continue;
                            break;
                        }
                    case Sqlx.PREFIX:
                    case Sqlx.SUFFIX:
                        {
                            var tk = tok;
                            Next();
                            if (drop)
                                o = new TChar("");
                            else 
                            {
                                o = lxr.val;
                                Mustbe(Sqlx.ID);
                            }
                            m += (tk, o);
                            break;
                        }   
                    case Sqlx.RDFLITERAL:
                        Next();
                        iri = drop ? new TChar("") : lxr.val;
                        break;
                    // NODETYPE ['(' Identity_id ')']
                    // + NODETYPE  + NODE=id
                    case Sqlx.NODETYPE:
                        {
                            m += (tok, o);
                            Next();
                            if (tok==Sqlx.LPAREN)
                            {
                                Next();
                                var id = lxr.val;
                                Mustbe(Sqlx.ID);
                                m += (Sqlx.NODE, id);
                                Mustbe(Sqlx.RPAREN);
                            }
                            break;
                        }
                    // EDGETYPE ['(' Identity_id ')'] '('[Leaving_id '='] [SET] NodeType_id ',' [Arriving_id '='] [SET] NodeType_id ')'
                    // + EDGETYPE   + EDGE=id             + LPAREN=id      N  + RARROW=id       +RPAREN id        N  + ARROW=id
                    //                                                     Y  + ARROWBASE=id                      Y  + RARROWBASE=id
                    case Sqlx.EDGETYPE:
                        {
                            m += (tok, o);
                            Next();
                            var ei = lxr.val;
                            if (tok==Sqlx.ID)
                            {
                                Next();
                                m += (Sqlx.EDGE, ei);
                            }
                            Mustbe(Sqlx.LPAREN);
                            var ln = lxr.val;
                            Mustbe(Sqlx.ID);
                            if (tok==Sqlx.EQL)
                            {
                                m += (Sqlx.LPAREN, ln);
                                Next();
                                ln = lxr.val;
                                Mustbe(Sqlx.ID);
                            }
                            if (tok == Sqlx.SET)
                            {
                                m += (Sqlx.ARROWBASE, TChar.Empty);
                                Next();
                            }
                            if (!cx.role.dbobjects.Contains(ln.ToString()))
                                throw new DBException("42161", "NodeType", ln);
                            m += (Sqlx.RARROW, ln);
                            Mustbe(Sqlx.COMMA);
                            var an = lxr.val;
                            Mustbe(Sqlx.ID);
                            if (tok == Sqlx.EQL)
                            {
                                m += (Sqlx.RPAREN, an);
                                Next();
                                an = lxr.val;
                                Mustbe(Sqlx.ID);
                            }
                            if (tok == Sqlx.SET)
                            {
                                m += (Sqlx.RARROWBASE, TChar.Empty);
                                Next();
                            }
                            if (!cx.role.dbobjects.Contains(an.ToString()))
                                throw new DBException("42161", "NodeType", ln);
                            m += (Sqlx.ARROW, an);
                            Mustbe(Sqlx.RPAREN);
                            break;
                        }
                    case Sqlx.CARDINALITY:
                        {
                            Next();
                            Mustbe(Sqlx.LPAREN);
                            var lw = lxr.val;
                            Mustbe(Sqlx.INTEGERLITERAL);
                            m += (Sqlx.MIN, lw);
                            if (tok==Sqlx.TO)
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Sqlx.INTEGERLITERAL, Sqlx.TIMES);
                                m += (Sqlx.MAX, lw);
                            }
                            break;
                        }
                    case Sqlx.MULTIPLICITY:
                        {
                            Next();
                            Mustbe(Sqlx.LPAREN);
                            var lw = lxr.val;
                            Mustbe(Sqlx.INTEGERLITERAL);
                            m += (Sqlx.MINVALUE, lw);
                            if (tok == Sqlx.TO)
                            {
                                Next();
                                lw = lxr.val;
                                Mustbe(Sqlx.INTEGERLITERAL, Sqlx.TIMES);
                                m += (Sqlx.MAXVALUE, lw);
                            }
                            break;
                        }
                    default:
                        if (drop)
                            o = new TChar("");
                        m += (tok, o);
                        iv = -1L;
                        break;
                    case Sqlx.RPAREN:
                        break;
                }
                Next();
            }
            if (ds != TNull.Value && !m.Contains(Sqlx.DESC))
                m += (Sqlx.DESC, ds);
            if (iv != -1L)
                m += (Sqlx.INVERTS, new TInt(iv));
            if (iri != TNull.Value)
                m += (Sqlx.IRI, iri);
            return m;
        }
        /// <summary>
        /// GenerationRule =  GENERATED ALWAYS AS '('Value')' [ UPDATE '(' Assignments ')' ]
        /// |   GENERATED ALWAYS AS ROW (START|END) .
        /// </summary>
        /// <param name="rt">The expected type</param>
        GenerationRule ParseGenerationRule(long tc,Domain xp)
        {
            var gr = GenerationRule.None;
            var ox = cx.parse;
            if (Match(Sqlx.GENERATED))
            {
                Next();
                Mustbe(Sqlx.ALWAYS);
                Mustbe(Sqlx.AS);
                if (Match(Sqlx.ROW))
                {
                    Next();
                    gr = tok switch
                    {
                        Sqlx.START => new GenerationRule(Generation.RowStart),
                        Sqlx.END => new GenerationRule(Generation.RowEnd),
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
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
          //      name = new Ident(this);
                Mustbe(Sqlx.ID);
            }
            if (tok == Sqlx.NOT)
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
                        dm = (Domain)cx.Add(dm);
                        pc.dataType = dm;
                        cx.db += (Transaction.Physicals, cx.tr.physicals+(pc.defpos, pc));
                    }
                    tc += (DBObject._Domain,dm);
                    tc = (TableColumn)cx.Add(tc);
                    tb += (Domain.Representation, tb.representation + (tc.defpos, dm));
                    tb = (Table)cx.Add(tb);
                    cx.db += (tb.defpos, tb);
                    cx.db += (tc.defpos, tc);
                }
                Mustbe(Sqlx.NULL);
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
            if (cx.tr == null) throw new DBException("42105");
            var key = new Domain(cx.GetUid(),cx,Sqlx.ROW,new BList<DBObject>(tc),1);
            string nm = "";
            switch (tok)
            {
                case Sqlx.SECURITY:
                    Next();
                    tc = (TableColumn)ParseClassification(tc);
                    break;
                case Sqlx.REFERENCES:
                    {
                        Next();
                        var rn = lxr.val.ToString();
                        var ta = cx.GetObject(rn);
                        var rt = ((ta is NodeType et) ? et.super :ta) as Table;
                        if (rt == null && ta is RestView rv)
                            rt = (Table?)rv.super;
                        if (rt==null) throw new DBException("42107", rn).Mix();
                        var cols = Domain.Row;
                        Mustbe(Sqlx.ID);
                        if (tok == Sqlx.LPAREN)
                            cols = ParseColsList(rt)??throw new DBException("42000");
                        string afn = "";
                        if (tok == Sqlx.USING)
                        {
                            Next();
                            int st = lxr.start;
                            if (tok == Sqlx.ID)
                            {
                                Next();
                                var ni = new Ident(this);
                                var pr = cx.GetProcedure(LexPos().dp, ni.ident, new CList<Domain>(tb))
                                     ?? throw new DBException("42108", ni.ident);
                                afn = "\"" + pr.defpos + "\"";
                            }
                            else
                            {
                                Mustbe(Sqlx.LPAREN);
                                ParseSqlValueList(Domain.Content);
                                Mustbe(Sqlx.RPAREN);
                                afn = new string(lxr.input,st, lxr.start - st);
                            }
                        }
                        var ct = ParseReferentialAction();
                        tb = cx.tr.ReferentialConstraint(cx,tb, "", key, rt, cols, ct, afn);
                        break;
                    }
                case Sqlx.CONSTRAINT:
                    {
                        Next();
                        nm = new Ident(this).ident;
                        Mustbe(Sqlx.ID);
                        if (tok != Sqlx.CHECK)
                            throw new DBException("42161", "CHECK",tok);
                        goto case Sqlx.CHECK;
                    }
                case Sqlx.CHECK:
                    {
                        Next();
                        tc = ParseColumnCheckConstraint(tb, tc, nm);
                        break;
                    }
                case Sqlx.DEFAULT:
                    {
                        Next();
                        tc = (TableColumn)cx.Add(
                            tc+(Domain.Default,ParseSqlValue(tc.domain).Eval(cx)??TNull.Value));
                        break;
                    }
                case Sqlx.UNIQUE:
                    {
                        Next();
                        var tr = cx.db as Transaction?? throw new DBException("2F003");
                        cx.Add(new PIndex(nm, tb, key, PIndex.ConstraintType.Unique, -1L,
                            tr.nextPos));
                        break;
                    }
                case Sqlx.PRIMARY:
                    {
                        var tn = tb.NameFor(cx);
                        if (tb.FindPrimaryIndex(cx) is not null)
                            throw new DBException("42147", tn).Mix();
                        Next();
                        Mustbe(Sqlx.KEY);
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
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
                name = new Ident(this);
                Mustbe(Sqlx.ID);
            }
            else if (tok==Sqlx.ID)
                name = new Ident(this);
            Sqlx s = Mustbe(Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.FOREIGN, Sqlx.CHECK);
            switch (s)
            {
                case Sqlx.UNIQUE: tb = ParseUniqueConstraint(tb, name); break;
                case Sqlx.PRIMARY: (tb) = ParsePrimaryConstraint(tb, name); break;
                case Sqlx.FOREIGN: (tb) = ParseReferentialConstraint(tb, name); break;
                case Sqlx.CHECK: (tb) = ParseTableConstraint(tb, name); break;
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
            var tr = cx.db as Transaction ?? throw new DBException("42105");
            if (ParseColsList(tb) is not Domain ks) throw new DBException("42161", "cols");
            var px = new PIndex(name?.ident ?? "", tb, ks, PIndex.ConstraintType.Unique, -1L, tr.nextPos);
            return (Table)(cx.Add(px) ?? throw new DBException("42105"));
        }
        /// <summary>
        /// construct a primary key constraint
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="cl">the ident</param>
        /// <param name="name">the constraint name</param>
        Table ParsePrimaryConstraint(Table tb, Ident? name)
        {
            var tr = cx.db as Transaction ?? throw new DBException("42105");
            if (tb.FindPrimaryIndex(cx) is Level3.Index x)
                throw new DBException("42147", x.NameFor(cx)).Mix();
            Mustbe(Sqlx.KEY);
            if (ParseColsList(tb) is not Domain ks) throw new DBException("42161", "cols");
            var px = new PIndex(name?.ident ?? "", tb, ks, PIndex.ConstraintType.PrimaryKey, -1L, tr.nextPos);
            return (Table)(cx.Add(px)?? throw new DBException("42105"));
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
            var tr = cx.db as Transaction ?? throw new DBException("42105");
            Mustbe(Sqlx.KEY);
            var cols = ParseColsList(tb) ?? throw new DBException("42161","cols");
            Mustbe(Sqlx.REFERENCES);
            var refname = new Ident(this);
            var rt = cx.GetObject(refname.ident) as Table??
                throw new DBException("42107", refname).Mix();
            Mustbe(Sqlx.ID);
            var refs = Domain.Row;
            PIndex.ConstraintType ct = PIndex.ConstraintType.ForeignKey;
            if (tok == Sqlx.LPAREN)
                refs = ParseColsList(rt)??Domain.Null;
            string afn = "";
            if (tok == Sqlx.USING)
            {
                Next();
                int st = lxr.start;
                if (tok == Sqlx.ID)
                {
                    var ic = new Ident(this);
                    Next();
                    var pr = cx.GetProcedure(LexPos().dp, ic.ident,Context.Signature(refs))
                        ??throw new DBException("42108",ic.ident);
                    afn = "\"" + pr.defpos + "\"";
                }
                else
                {
                    Mustbe(Sqlx.LPAREN);
                    ParseSqlValueList(Domain.Content);
                    Mustbe(Sqlx.RPAREN);
                    afn = new string(lxr.input, st, lxr.start - st);
                }
            }
            if (tok == Sqlx.ON)
                ct |= ParseReferentialAction();
            tb = tr.ReferentialConstraint(cx, tb, name?.ident ?? "", cols, rt, refs, ct, afn);
            return (Table)(cx.Add(tb) ?? throw new DBException("42105"));
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] SqlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="name">the name of the constraint</param
        /// <returns>the new PCheck</returns>
        Table ParseTableConstraint(Table tb, Ident? name)
        {
            int st = lxr.start;
            var nst = cx.db.nextStmt;
            Mustbe(Sqlx.LPAREN);
            var se = ParseSqlValue(Domain.Bool);
            Mustbe(Sqlx.RPAREN);
            var n = name ?? new Ident(this);
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            PCheck r = new(tb, n.ident, se, new string(lxr.input, st, lxr.start - st), nst, tr.nextPos, cx);
            tb = (Table)(cx.Add(r) ?? throw new DBException("42105"));
            if (tb.defpos < Transaction.TransPos)
            {
                var trs = tb.RowSets(new Ident("", Iix.None),cx, tb,-1L);
                if (trs.First(cx) != null)
                    throw new DBException("44000", n.ident).ISO();
            }
            return tb;
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] SqlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="pc">the column constrained</param>
        /// <param name="name">the name of the constraint</param>
        /// <returns>the new TableColumn</returns>
        TableColumn ParseColumnCheckConstraint(Table tb, TableColumn tc,string nm)
        {
            var oc = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            int st = lxr.start;
            Mustbe(Sqlx.LPAREN);
            // Set up the information for parsing the column check constraint
            var ix = cx.Ix(tb.defpos);
            cx.defs += (new Ident(tb.NameFor(cx), ix), ix);
            for (var b = cx.obs.PositionAt(tb.defpos)?.Next(); b != null; b = b.Next())
                if (b.value() is TableColumn x)
                    cx.defs += (x.NameFor(cx), cx.Ix(x.defpos), Ident.Idents.Empty);
            var nst = cx.db.nextStmt;
            var se = ParseSqlValue(Domain.Bool);
            Mustbe(Sqlx.RPAREN);
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            var pc = new PCheck2(tb, tc, nm, se,
                new string(lxr.input, st, lxr.start - st), nst, tr.nextPos, cx);
            cx.parse = oc;
            return (TableColumn)(cx.Add(pc) ?? throw new DBException("42105"));
        }
        /// <summary>
		/// ReferentialAction = {ON (DELETE|UPDATE) (CASCADE| SET DEFAULT|RESTRICT)} .
        /// </summary>
        /// <returns>constraint type flags</returns>
		PIndex.ConstraintType ParseReferentialAction()
        {
            PIndex.ConstraintType r = PIndex.Reference;
            while (tok == Sqlx.ON)
            {
                Next();
                Sqlx when = Mustbe(Sqlx.UPDATE, Sqlx.DELETE);
                Sqlx what = Mustbe(Sqlx.RESTRICT, Sqlx.CASCADE, Sqlx.SET, Sqlx.NO);
                if (what == Sqlx.SET)
                    what = Mustbe(Sqlx.DEFAULT, Sqlx.NULL);
                else if (what == Sqlx.NO)
                {
                    Mustbe(Sqlx.ACTION);
                    throw new DBException("42123").Mix();
                }
                if (when == Sqlx.UPDATE)
                {
                    r &= ~PIndex.Updates;
                    switch (what)
                    {
                        case Sqlx.CASCADE: r |= PIndex.ConstraintType.CascadeUpdate; break;
                        case Sqlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultUpdate; break;
                        case Sqlx.NULL: r |= PIndex.ConstraintType.SetNullUpdate; break;
                        case Sqlx.RESTRICT: r |= PIndex.ConstraintType.RestrictUpdate; break;
                    }
                }
                else
                {
                    r &= ~PIndex.Deletes;
                    switch (what)
                    {
                        case Sqlx.CASCADE: r |= PIndex.ConstraintType.CascadeDelete; break;
                        case Sqlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultDelete; break;
                        case Sqlx.NULL: r |= PIndex.ConstraintType.SetNullDelete; break;
                        case Sqlx.RESTRICT: r |= PIndex.ConstraintType.RestrictDelete; break;
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
        public void ParseProcedureClause(bool func, Sqlx create)
        {
            var op = cx.parse;
            var nst = cx.db.nextStmt;
            var n = new Ident(lxr.val.ToString(),
                new Iix(lxr.Position,cx,cx.db.nextPos)); // n.iix.dp will match pp.ppos
            cx.parse = ExecuteStatus.Compile;
            Mustbe(Sqlx.ID);
            int st = lxr.start;
            var ps = ParseParameters(n);
            var a = Context.Signature(ps);
            var pi = new ObInfo(n.ident,
                Grant.Privilege.Owner | Grant.Privilege.Execute | Grant.Privilege.GrantExecute);
            var rdt = func ? ParseReturnsClause(n) : Domain.Null;
            if (Match(Sqlx.EOF) && create == Sqlx.CREATE)
                throw new DBException("42000", "EOF").Mix();
            var pr = cx.GetProcedure(LexPos().dp,n.ident, a);
            PProcedure? pp = null;
            if (pr == null)
            {
                if (create == Sqlx.CREATE)
                {
                    // create a preliminary version of the PProcedure without parsing the body
                    // in case the procedure is recursive (the body is parsed below)
                    pp = new PProcedure(n.ident, ps,
                        rdt, pr, new Ident(lxr.input.ToString()??"", n.iix), nst, cx.db.nextPos, cx);
                    pr = new Procedure(pp.defpos, cx, ps, rdt,
                        new BTree<long,object>(DBObject.Definer,cx.role.defpos)
                        +(DBObject.Infos,new BTree<long,ObInfo>(cx.role.defpos,pi)));
                    pr = (Procedure)(cx.Add(pp)??pr);
                    pp.dataType = pr.domain;
                    cx.db += (pr,cx.db.loadpos);
                }
                else
                    throw new DBException("42108", n.ToString()).Mix();
            }
            else
                if (create == Sqlx.CREATE)
                throw new DBException("42167", n.ident, ps.Length).Mix();
            if (create == Sqlx.ALTER && tok == Sqlx.TO)
            {
                Next();
                Mustbe(Sqlx.ID);
                cx.db += (pr,cx.db.loadpos);
            }
            else if (create == Sqlx.ALTER && (StartMetadata(Sqlx.FUNCTION) || Match(Sqlx.ALTER, Sqlx.ADD, Sqlx.DROP)))
                ParseAlterBody(pr);
            else
            {
                var lp = LexPos();
                if (StartMetadata(Sqlx.FUNCTION))
                {
                    var m = ParseMetadata(Sqlx.FUNCTION);
                    var pm = new PMetadata3(n.ident, 0, pr, m, cx.db.nextPos);
                    cx.Add(pm);
                }
                var s = new Ident(new string(lxr.input, st, lxr.start - st),lp);
                if (tok != Sqlx.EOF && tok != Sqlx.SEMICOLON)
                {
                    cx.AddParams(pr);
                    cx.Add(pr);
                    var bd = ParseProcedureStatement(pr.domain)??throw new DBException("42000");
                    cx.Add(pr);
                    var ns = cx.db.nextStmt;
                    var fm = new Framing(cx,pp?.nst??ns);
                    s = new Ident(new string(lxr.input, st, lxr.start - st),lp);
                    if (pp != null)
                    {
                        pp.source = s;
                        pp.proc = bd?.defpos??throw new DBException("42000");
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
                if (create == Sqlx.CREATE)
                    cx.db += (pr,cx.db.loadpos);
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
        internal (Domain,Domain) ParseProcedureHeading(Ident pn)
        {
            var ps = Domain.Null;
            var oi = Domain.Null;
            if (tok != Sqlx.LPAREN)
                return (ps, Domain.Null);
            ps = ParseParameters(pn);
            LexPos(); // for synchronising with CREATE
            if (tok == Sqlx.RETURNS)
            {
                Next();
                oi = ParseSqlDataType(pn);
            }
            return (ps, oi);
        }
        /// <summary>
        /// Function metadata is used to control display of output from table-valued functions
        /// </summary>
        /// <param name="pr"></param>
        void ParseAlterBody(Procedure pr)
        {
            if (pr.domain.kind != Sqlx.Null)
                return;
            if (pr.domain.Length==0)
                return;
            ParseAlterOp(pr.domain);
            while (tok == Sqlx.COMMA)
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
		internal Domain ParseParameters(Ident pn,Domain? xp = null)
		{
            var op = cx.parse;
            cx.parse = ExecuteStatus.Compile;
            Mustbe(Sqlx.LPAREN);
            var r = BList<DBObject>.Empty;
			while (tok!=Sqlx.RPAREN)
			{
                r+= ParseProcParameter(pn,xp);
				if (tok!=Sqlx.COMMA)
					break;
				Next();
			}
			Mustbe(Sqlx.RPAREN);
            cx.parse = op;
			return new Domain(Sqlx.ROW,cx,r);
		}
		internal Domain ParseReturnsClause(Ident pn)
		{
			if (tok!=Sqlx.RETURNS)
				return Domain.Null;
			Next();
            if (tok == Sqlx.ID)
            {
                var s = lxr.val.ToString();
                Next();
                var ob = (Domain)(cx._Ob(cx.role.dbobjects[s] ?? -1L)
                    ?? throw new DBException("42119", s));
                return ob;
            }
			var dm = ParseSqlDataType(pn);
            cx.Add(dm);
            return dm;
		}
        /// <summary>
        /// parse a formal parameter
        /// </summary>
        /// <param name="pn">The procedure or method</param>
        /// <param name="xp">The UDT if in CREATE TYPE</param>
        /// <returns>the procparameter</returns>
		FormalParameter ParseProcParameter(Ident pn,Domain? xp=null)
		{
            var es = ExecuteList.Empty;
			Sqlx pmode = Sqlx.IN;
			if (Match(Sqlx.IN,Sqlx.OUT,Sqlx.INOUT))
			{
				pmode = tok;
				Next();
			}
			var n = new Ident(this);
			Mustbe(Sqlx.ID);
            var p = new FormalParameter(n.iix.dp, pmode,n.ident, ParseSqlDataType(n))
                +(DBObject._From,pn.iix.dp);
            cx.Add(p);
            if (xp == null) // prepare to parse a body
                cx.defs += (new Ident(pn, n), pn.iix);
			if (Match(Sqlx.RESULT))
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
 			if (Match(Sqlx.CONTINUE,Sqlx.EXIT,Sqlx.UNDO))
				return (Executable)cx.Add(ParseHandlerDeclaration());
			var n = new Ident(this);
			Mustbe(Sqlx.ID);
            LocalVariableDec lv;
            if (tok == Sqlx.CURSOR)
            {
                Next();
                Mustbe(Sqlx.FOR);
                var cs = ParseCursorSpecification(Domain.TableType);
                var cu = (RowSet?)cx.obs[cs.union]??throw new PEException("PE1557");
                var sc = new SqlCursor(n.iix.dp, cu, n.ident);
                cx.result = -1L;
                cx.Add(sc);
                lv = new CursorDeclaration(lp.dp,sc, cu);
            }
            else
            {
                var ld = ParseSqlDataType();
                var vb = new SqlValue(n, cx, ld);
                cx.Add(vb);
                lv = new LocalVariableDec(lp.dp, vb);
                if (Match(Sqlx.EQL, Sqlx.DEFAULT))
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
        Executable ParseHandlerDeclaration()
        {
            var hs = new HandlerStatement(LexDp(), tok, new Ident(this).ident);
            Mustbe(Sqlx.CONTINUE, Sqlx.EXIT, Sqlx.UNDO);
            Mustbe(Sqlx.HANDLER);
            Mustbe(Sqlx.FOR);
            hs+=(HandlerStatement.Conds,ParseConditionValueList());
            ParseProcedureStatement(Domain.Content);
            if (cx.exec is not Executable a) throw new DBException("42161", "handler");
            cx.Add(a);
            hs= hs+(HandlerStatement.Action,a.defpos)+(DBObject.Dependents,a.dependents);
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
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParseConditionValue();
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
			if (tok==Sqlx.SQLSTATE)
			{
				Next();
                if (tok == Sqlx.VALUE)
                    Next();
				n = lxr.val;
				Mustbe(Sqlx.CHARLITERAL);
                switch (n.ToString()[..2])
                { // handlers are not allowed to defeat the transaction machinery
                    case "25": throw new DBException("2F003").Mix();
                    case "40": throw new DBException("2F003").Mix();
                    case "2D": throw new DBException("2F003").Mix();
                }
			} 
			else if (Match(Sqlx.SQLEXCEPTION,Sqlx.SQLWARNING,Sqlx.NOT))
			{
				if (tok==Sqlx.NOT)
				{
					Next();
					Mustbe(Sqlx.FOUND);
					n = new TChar("NOT_FOUND");
				}
				else
				{
					n = new TChar(tok.ToString());
					Next();
				}
			}
			else
				Mustbe(Sqlx.ID);
            return n.ToString();
        }
        /// <summary>
		/// CompoundStatement = Label BEGIN [XMLDec] Statements END .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="f">The procedure whose body is being defined if any</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseCompoundStatement(Domain xp,string n)
        {
            var cs = new CompoundStatement(LexDp(), n);
            Mustbe(Sqlx.BEGIN);
            if (tok == Sqlx.TRANSACTION)
                throw new DBException("25001", "Nested transactions are not supported").ISO();
            if (tok == Sqlx.XMLNAMESPACES)
            {
                Next();
                cx.Add(ParseXmlNamespaces());
            }
            var r =BList<long?>.Empty;
            while (tok != Sqlx.END && ParseProcedureStatement(xp) is Executable a)
            {
                r += cx.Add(a).defpos; 
                if (tok == Sqlx.END)
                    break;
                Mustbe(Sqlx.SEMICOLON);
            }
            Mustbe(Sqlx.END);
            cs+=(cx,CompoundStatement.Stms,r);
            cs += (DBObject.Dependents, _Deps(r));
            cx.Add(cs);
            return cs;
        }
        /// <summary>
		/// Statement = 	Assignment
		/// |	Call
		/// |	CaseStatement 
		/// |	Close
		/// |	CompoundStatement
		/// |	BREAK
		/// |	Declaration
		/// |	DeletePositioned
		/// |	DeleteSearched
		/// |	Fetch
		/// |	ForStatement
        /// |   GetDiagnostics
		/// |	IfStatement
		/// |	Insert
		/// |	ITERATE label
		/// |	LEAVE label
		/// |	LoopStatement
		/// |	Open
		/// |	Repeat
		/// |	RETURN TypedValue
		/// |	SelectSingle
		/// |	Raise
        /// |   Resignal
		/// |	Sparql
		/// |	UpdatePositioned
		/// |	UpdateSearched
		/// |	While .
        /// </summary>
        /// <param name="xp">the expected domain</param>
        /// <returns>the Executable resulting from the parse</returns>
		internal Executable ParseProcedureStatement(Domain xp)
		{
            var lp = lxr.start;
            Match(Sqlx.BREAK,Sqlx.MATCH);
 			switch (tok)
			{
                case Sqlx.EOF: return new Executable(lp);
                case Sqlx.ID: return ParseLabelledStatement(xp);
                case Sqlx.ALTER: ParseAlter(); break;
                case Sqlx.BEGIN: return ParseCompoundStatement(xp, "");
                case Sqlx.CALL: return ParseCallStatement(); 
                case Sqlx.CASE: return ParseCaseStatement(xp); 
                case Sqlx.CLOSE: return ParseCloseStatement();
                case Sqlx.COMMIT: throw new DBException("2D000").ISO(); // COMMIT not allowed inside SQL routine
                case Sqlx.CREATE: return ParseCreateClause();
                case Sqlx.BREAK: return ParseBreakLeave(); 
                case Sqlx.DECLARE: return ParseDeclaration();  // might be for a handler
                case Sqlx.DELETE: return ParseSqlDelete(); 
                case Sqlx.DROP: ParseDropStatement(); break;
                case Sqlx.FETCH: return ParseFetchStatement(); 
                case Sqlx.GRANT: return ParseGrant(); 
                case Sqlx.FOR: return ParseForStatement(xp, null);
                case Sqlx.GET: return ParseGetDiagnosticsStatement(); 
                case Sqlx.IF: return ParseIfThenElse(xp); 
                case Sqlx.INSERT: return ParseSqlInsert();
                case Sqlx.ITERATE: return ParseIterate(); 
                case Sqlx.LEAVE: return ParseBreakLeave();
                case Sqlx.MATCH: return ParseSqlMatchStatement(); 
                case Sqlx.LOOP: return ParseLoopStatement(xp, null);
                case Sqlx.OPEN: return ParseOpenStatement(); 
                case Sqlx.REPEAT: return ParseRepeat(xp, null);
                case Sqlx.REVOKE: return ParseRevoke(); 
                case Sqlx.ROLLBACK: cx.Add(new RollbackStatement(LexDp())); break;
                case Sqlx.RETURN: return ParseReturn(xp); 
                case Sqlx.SELECT: return ParseSelectSingle(new Ident(this),xp);
                case Sqlx.SET: return ParseAssignment(); 
                case Sqlx.SIGNAL: return ParseSignal(); 
                case Sqlx.RESIGNAL: return ParseSignal(); 
                case Sqlx.UPDATE: (cx,var e) = ParseSqlUpdate(); return e;
                case Sqlx.WHILE: return ParseSqlWhile(xp, null); 
				default: throw new DBException("42000",lxr.Diag).ISO();
			}
            return new Executable(lp);
		}
        /// <summary>
        /// GetDiagnostics = GET DIAGMOSTICS Target = ItemName { , Target = ItemName }
        /// </summary>
        Executable ParseGetDiagnosticsStatement()
        {
            Next();
            Mustbe(Sqlx.DIAGNOSTICS);
            var r = new GetDiagnostics(LexDp());
            for (; ; )
            {
                var t = ParseSqlValueEntry(Domain.Content, false);
                cx.Add(t);
                Mustbe(Sqlx.EQL);
                Match(Sqlx.NUMBER, Sqlx.MORE, Sqlx.COMMAND_FUNCTION, Sqlx.COMMAND_FUNCTION_CODE,
                    Sqlx.DYNAMIC_FUNCTION, Sqlx.DYNAMIC_FUNCTION_CODE, Sqlx.ROW_COUNT,
                    Sqlx.TRANSACTIONS_COMMITTED, Sqlx.TRANSACTIONS_ROLLED_BACK,
                    Sqlx.TRANSACTION_ACTIVE, Sqlx.CATALOG_NAME,
                    Sqlx.CLASS_ORIGIN, Sqlx.COLUMN_NAME, Sqlx.CONDITION_NUMBER,
                    Sqlx.CONNECTION_NAME, Sqlx.CONSTRAINT_CATALOG, Sqlx.CONSTRAINT_NAME,
                    Sqlx.CONSTRAINT_SCHEMA, Sqlx.CURSOR_NAME, Sqlx.MESSAGE_LENGTH,
                    Sqlx.MESSAGE_OCTET_LENGTH, Sqlx.MESSAGE_TEXT, Sqlx.PARAMETER_MODE,
                    Sqlx.PARAMETER_NAME, Sqlx.PARAMETER_ORDINAL_POSITION,
                    Sqlx.RETURNED_SQLSTATE, Sqlx.ROUTINE_CATALOG, Sqlx.ROUTINE_NAME,
                    Sqlx.ROUTINE_SCHEMA, Sqlx.SCHEMA_NAME, Sqlx.SERVER_NAME, Sqlx.SPECIFIC_NAME,
                    Sqlx.SUBCLASS_ORIGIN, Sqlx.TABLE_NAME, Sqlx.TRIGGER_CATALOG,
                    Sqlx.TRIGGER_NAME, Sqlx.TRIGGER_SCHEMA);
                r+=(cx,GetDiagnostics.List,r.list+(t.defpos,tok));
                Mustbe(Sqlx.NUMBER, Sqlx.MORE, Sqlx.COMMAND_FUNCTION, Sqlx.COMMAND_FUNCTION_CODE,
                    Sqlx.DYNAMIC_FUNCTION, Sqlx.DYNAMIC_FUNCTION_CODE, Sqlx.ROW_COUNT,
                    Sqlx.TRANSACTIONS_COMMITTED, Sqlx.TRANSACTIONS_ROLLED_BACK,
                    Sqlx.TRANSACTION_ACTIVE, Sqlx.CATALOG_NAME,
                    Sqlx.CLASS_ORIGIN, Sqlx.COLUMN_NAME, Sqlx.CONDITION_NUMBER,
                    Sqlx.CONNECTION_NAME, Sqlx.CONSTRAINT_CATALOG, Sqlx.CONSTRAINT_NAME,
                    Sqlx.CONSTRAINT_SCHEMA, Sqlx.CURSOR_NAME, Sqlx.MESSAGE_LENGTH,
                    Sqlx.MESSAGE_OCTET_LENGTH, Sqlx.MESSAGE_TEXT, Sqlx.PARAMETER_MODE,
                    Sqlx.PARAMETER_NAME, Sqlx.PARAMETER_ORDINAL_POSITION,
                    Sqlx.RETURNED_SQLSTATE, Sqlx.ROUTINE_CATALOG, Sqlx.ROUTINE_NAME,
                    Sqlx.ROUTINE_SCHEMA, Sqlx.SCHEMA_NAME, Sqlx.SERVER_NAME, Sqlx.SPECIFIC_NAME,
                    Sqlx.SUBCLASS_ORIGIN, Sqlx.TABLE_NAME, Sqlx.TRIGGER_CATALOG,
                    Sqlx.TRIGGER_NAME, Sqlx.TRIGGER_SCHEMA);
                if (tok != Sqlx.COMMA)
                    break;
                Next();
            }
            return (Executable)cx.Add(r);
        }
        /// <summary>
        /// Label =	[ label ':' ] .
        /// Some procedure statements have optional labels. We deal with these here
        /// </summary>
        /// <param name="xp">the expected ob type if any</param>
		Executable ParseLabelledStatement(Domain xp)
        {
            Ident sc = new(this);
            var lp = lxr.start;
            var cp = LexPos();
            Mustbe(Sqlx.ID);
            // OOPS: according to SQL 2003 there MUST follow a colon for a labelled statement
            if (tok == Sqlx.COLON)
            {
                Next();
                var s = sc.ident;
                var e = tok switch
                {
                    Sqlx.BEGIN => ParseCompoundStatement(xp, s),
                    Sqlx.FOR => ParseForStatement(xp, s),
                    Sqlx.LOOP => ParseLoopStatement(xp, s),
                    Sqlx.REPEAT => ParseRepeat(xp, s),
                    Sqlx.WHILE => ParseSqlWhile(xp, s),
                    _ => throw new DBException("26000", s).ISO(),
                };
                return (Executable)cx.Add(e);
            }
            // OOPS: but we'q better allow a procedure call here for backwards compatibility
            else if (tok == Sqlx.LPAREN)
            {
                Next();
                cp = LexPos();
                var ps = ParseSqlValueList(Domain.Content);
                Mustbe(Sqlx.RPAREN);
                var a = cx.Signature(ps);
                var pr = cx.GetProcedure(cp.dp, sc.ident, a) ??
                    throw new DBException("42108", sc.ident);
                var c = new SqlProcedureCall(cp.dp,cx,pr,ps);
                return (Executable)cx.Add(new CallStatement(cx.GetUid(),c));
            }
            // OOPS: and a simple assignment for backwards compatibility
            else if (cx.defs[sc] is Iix vp && cx.db.objects[vp.dp] is DBObject vb)
            {
                Mustbe(Sqlx.EQL);
                var va = ParseSqlValue(vb.domain);
                var sa = new AssignmentStatement(cp.dp,vb,va);
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
            if (tok == Sqlx.LPAREN)
                return ParseMultipleAssignment();
            var vb = ParseVarOrColumn(Domain.Content);
            cx.Add(vb);
            Mustbe(Sqlx.EQL);
            var va = ParseSqlValue(vb.domain);
            var sa = new AssignmentStatement(lp.dp,vb,va);
            return (Executable)cx.Add(sa);
        }
        /// <summary>
        /// 	|	SET '(' Target { ',' Target } ')' '='  TypedValue .
        /// Target = 	id { '.' id } .
        /// </summary>
		Executable ParseMultipleAssignment()
        {
            Mustbe(Sqlx.EQL);
            var ids = ParseIDList();
            var v = ParseSqlValue(Domain.Content);
            cx.Add(v);
            var ma = new MultipleAssignment(LexDp(),cx,ids,v);
            if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction)
                cx = ma.Obey(cx);
            return (Executable)cx.Add(ma);
        }
        /// <summary>
        /// |	RETURN TypedValue
        /// </summary>
		Executable ParseReturn(Domain xp)
        {
            Next();
            SqlValue re;
            var ag = -1L;
            var dp = cx.GetUid();
            if (xp == Domain.Content)
            {
                var dm = ParseSelectList(-1L, xp) + (Domain.Kind,Sqlx.ROW);
                if (dm.aggs != CTree<long,bool>.Empty)
                {
                    // we don't really know yet what rowset the enclosing syntax will provide
                    // for now we sort out the requirements
                    var ii = cx.GetIid();
                    var sd = dm.Source(cx,dp); // this is what we will need
                    var sr = new SelectRowSet(ii, cx, dm,new TrivialRowSet(cx,sd));
                    sr = (SelectRowSet)ParseSelectRowSet(sr); // this is what we will do with it
                    ag = sr.defpos;
                    dm = sd;
                }
                re = new SqlRow(dp, cx, dm);
            }
            else
                re = ParseSqlValue(xp);
            cx.Add(re);
            var rs = new ReturnStatement(cx.GetUid(), re);
            if (ag >= 0)
                rs+=(ReturnStatement.Aggregator, ag);
            return (Executable)cx.Add(rs);
        }
        /// <summary>
		/// CaseStatement = 	CASE TypedValue { WHEN TypedValue THEN Statements }+ [ ELSE Statements ] END CASE
		/// |	CASE { WHEN SqlValue THEN Statements }+ [ ELSE Statements ] END CASE .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseCaseStatement(Domain xp)
        {
            Next();
            if (tok == Sqlx.WHEN)
            {
                var ws = ParseWhenList(xp);
                var ss = BList<long?>.Empty;
                if (tok == Sqlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList(xp);
                }
                var e = new SearchedCaseStatement(LexDp(),ws,ss);
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.CASE);
                cx.Add(e);
                return e;
            }
            else
            {
                var op = ParseSqlValue(Domain.Content);
                var ws = ParseWhenList(op.domain);
                var ss = BList<long?>.Empty;
                if (tok == Sqlx.ELSE)
                {
                    Next();
                    ss = ParseStatementList(xp);
                }
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.CASE);
                var e = new SimpleCaseStatement(LexDp(),op,ws,ss);
                cx.Add(e);
                cx.exec = e;
                return e;
            }
        }
        /// <summary>
        /// { WHEN SqlValue THEN Statements }
        /// </summary>
        /// <returns>the tree of Whenparts</returns>
		BList<WhenPart> ParseWhenList(Domain xp)
		{
            var r = BList<WhenPart>.Empty;
            var dp = LexDp();
			while (tok==Sqlx.WHEN)
			{
				Next();
                var c = ParseSqlValue(xp);
				Mustbe(Sqlx.THEN);
                r+=new WhenPart(dp, c, ParseStatementList(xp));
			}
			return r;
		}
        /// <summary>
		/// ForStatement =	Label FOR [ For_id AS ][ id CURSOR FOR ] QueryExpression DO Statements END FOR [Label_id] .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseForStatement(Domain xp,string? n)
        {
            var lp = LexDp();
            Next();
            Ident c = new(DBObject.Uid(lp), cx.Ix(lp));
            var d = 1; // depth
            if (tok != Sqlx.SELECT)
            {
                c = new Ident(this);
                Mustbe(Sqlx.ID);
                if (tok == Sqlx.CURSOR)
                {
                    Next();
                    Mustbe(Sqlx.FOR);
                }
                else
                {
                    Mustbe(Sqlx.AS);
                    if (tok != Sqlx.SELECT)
                    {
                        Mustbe(Sqlx.ID);
                        Mustbe(Sqlx.CURSOR);
                        Mustbe(Sqlx.FOR);
                    }
                }
            }
            var ss = ParseCursorSpecification(Domain.TableType,true); // use ambient declarations
            d = Math.Max(d, ss.depth + 1);
            var cs = (RowSet?)cx.obs[ss.union]??throw new DBException("42000");
            Mustbe(Sqlx.DO);
            var xs = ParseStatementList(xp);
            var fs = new ForSelectStatement(lp,n??"",c,cs,xs)+(DBObject._Depth,d);
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.FOR);
            if (tok == Sqlx.ID)
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
            Mustbe(Sqlx.THEN);
            var th = ParseStatementList(xp);
            var ei = BList<long?>.Empty;
            while (Match(Sqlx.ELSEIF))
            {
                var d = LexDp();
                Next();
                var s = ParseSqlValue(Domain.Bool);
                cx.Add(s);
                Mustbe(Sqlx.THEN);
                Next();
                var t = ParseStatementList(xp);
                var e = new IfThenElse(d, s, t, BList<long?>.Empty, BList<long?>.Empty);
                cx.Add(e);
                ei += e.defpos;
            }
            var el = BList<long?>.Empty;
            if (tok == Sqlx.ELSE)
            {
                Next();
                el = ParseStatementList(xp);
            }
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.IF);
            var ife = new IfThenElse(lp,se, th,ei,el);
            cx = old;
            return (Executable)cx.Add(ife);
        }
        /// <summary>
		/// Statements = 	Statement { ';' Statement } .
        /// </summary>
        /// <param name="xp">The obs type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		BList<long?> ParseStatementList(Domain xp)
		{
            if (ParseProcedureStatement(xp) is not Executable a)
                throw new DBException("42161", "statement");
            var r = BList<long?>.Empty + cx.Add(a).defpos;
            while (tok==Sqlx.SEMICOLON)
			{
				Next();
                if (ParseProcedureStatement(xp) is not Executable b)
                    throw new DBException("42161", "statement");
                r +=cx.Add(b).defpos;
			}
			return r;
		}
        /// <summary>
		/// SelectSingle =	[DINSTINCT] SelectList INTO VariableRef { ',' VariableRef } TableExpression .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSelectSingle(Ident id,Domain xp)
        {
            cx.IncSD(id);
            Next();
            //     var ss = new SelectSingle(LexPos());
            //     var qe = new RowSetExpr(lp+1);
            //     var s = new RowSetSpec(lp+2,cx,xp) + 
            //                  (QueryExpression._Distinct, ParseDistinctClause())
            var d = ParseDistinctClause();
            var dm = ParseSelectList(id.iix.dp,xp);
            Mustbe(Sqlx.INTO);
            var ts = ParseTargetList();
      //      cs = cs + ;
      //      qe+=(RowSetExpr._Left,cx.Add(s).defpos);
       //     ss += (SelectSingle.Outs,ts);
            if (ts.Count != dm.rowType.Length)
                throw new DBException("22007").ISO();
            //      s+=(RowSetSpec.TableExp,ParseTableExpression(s).defpos);
            RowSet te = ParseTableExpression(id.iix,dm);
            if (d)
                te = new DistinctRowSet(cx, te);
            var cs = new SelectStatement(id.iix.dp, te);
            var ss = new SelectSingle(id.iix.dp)+(ForSelectStatement.Sel,cs);
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
			bool b = (tok==Sqlx.LPAREN);
                if (b)
                    Next();
            var r = BList<long?>.Empty;
            for (; ; )
            {
                var v = ParseVarOrColumn(Domain.Content);
                cx.Add(v);
                r += v.defpos;
                if (tok != Sqlx.COMMA)
                    break;
                Next();
            }
			if (b)
				Mustbe(Sqlx.RPAREN);
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
        SqlValue ParseVarOrColumn(Domain xp)
        {
            Match(Sqlx.SYSTEM_TIME, Sqlx.SECURITY);
            if (tok == Sqlx.SECURITY)
            {
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105");
                var sp = LexPos();
                Next();
                return (SqlValue)cx.Add(new SqlFunction(sp.dp, cx, Sqlx.SECURITY, null, null, null, Sqlx.NO));
            }
            if (Match(Sqlx.PARTITION, Sqlx.POSITION, Sqlx.VERSIONING, Sqlx.CHECK, 
                Sqlx.SYSTEM_TIME, Sqlx.LAST_DATA))
            {
                SqlValue ps = new SqlFunction(LexPos().dp, cx, tok, null, null, null, Sqlx.NO);
                Next();
                if (tok == Sqlx.LPAREN && ((SqlFunction)ps).domain.kind == Sqlx.VERSIONING)
                {
                    var vp = LexPos();
                    Next();
                    if (tok == Sqlx.SELECT)
                    {
                        var cs = ParseCursorSpecification(Domain.Null).union;
                        Mustbe(Sqlx.RPAREN);
                        var sv = (SqlValue)cx.Add(new SqlValueSelect(vp.dp, cx, 
                            (RowSet?)cx.obs[cs]??throw new DBException("42000"), xp));
                        ps += (cx, SqlFunction._Val, sv.defpos);
                    } else
                        Mustbe(Sqlx.RPAREN);
                } 
                return (SqlValue)cx.Add(ps);
            }
            var ttok = tok;
            Ident ic = ParseIdentChain();
            var lp = LexPos();
            if (tok == Sqlx.LPAREN)
            {
                Next();
                var ps = BList<long?>.Empty;
                if (tok != Sqlx.RPAREN)
                    ps = ParseSqlValueList(Domain.Content);
                Mustbe(Sqlx.RPAREN);
                var n = cx.Signature(ps);
                if (ic.Length == 0 || ic[ic.Length - 1] is not Ident pn)
                    throw new DBException("42000");
                if (ic.Length == 1)
                {
                    var pr = cx.GetProcedure(LexPos().dp, pn.ident, n);
                    if (pr == null && (cx.db.objects[cx.role.dbobjects[pn.ident]??-1L]
                        ?? StandardDataType.Get(ttok))is Domain ut && ut!=Domain.Content)
                    {
                        cx.Add(ut);
                        var oi = ut.infos[cx.role.defpos];
                        if (cx.db.objects[oi?.methodInfos[pn.ident]?[n] ?? -1L] is Method me)
                            return (SqlValue)cx.Add(new SqlConstructor(lp.dp, cx, me, ps));
                        if (Context.CanCall(cx.Signature(ut.rowType),n) || ttok!=Sqlx.ID || ut.rowType==BList<long?>.Empty)
                            return (SqlValue)cx.Add(new SqlDefaultConstructor(pn.iix.dp, cx, ut, ps));
                    }
                    if (pr == null)
                        throw new DBException("42108", ic.ident);
                    return (SqlValue)cx.Add(new SqlProcedureCall(lp.dp, cx, pr, ps));
                }
                else if (ic.Prefix(ic.Length-2) is Ident pf)
                {
                    var vr = (SqlValue)Identify(pf, Domain.Null);
                    cx.undefined += (lp.dp, cx.sD);
                    return (SqlValue)cx.Add(new SqlMethodCall(lp.dp, cx, ic.sub?.ident??"", ps, vr));
                }
            }
            var ob = Identify(ic, xp);
            if (ob is not SqlValue r)
                throw new DBException("42112", ic.ToString());
            return r;
        }
        DBObject Identify(Ident ic, Domain xp)
        {
            if (cx.user == null)
                throw new DBException("42105");
            // See SourceIntro.docx section 6.1.2
            // we look up the identifier chain ic
            // and perform 6.1.2 (2) if we find anything
            var len = ic.Length;
            var (pa, sub) = cx.Lookup(LexPos().dp, ic, len);
            // pa is the object that was found, or null
            var m = sub?.Length ?? 0;
            if (pa is SqlCopy sc && cx.db.objects[sc.copyFrom] is TableColumn tc 
                && cx.db.objects[tc.tabledefpos] is NodeType nt && sub!=null)
            {
                cx.Add(pa);
                cx.AddDefs(nt);
                var sb = Identify(sub, xp);
                sb += (DBObject._From, pa.defpos);
                return sb;
            }
            if (pa is SqlValue sv && sv.domain.infos[cx.role.defpos] is ObInfo si && sub is not null
                && si.names[sub.ident].Item2 is long sp && cx.db.objects[sp] is TableColumn tc1)
            {
                var co = new SqlCopy(sub.iix.dp, cx, sub.ident, sv.defpos, tc1);
                var nc = new SqlValueExpr(ic.iix.dp, cx, Sqlx.DOT, sv, co, Sqlx.NO);
                cx.Add(co);
                return cx.Add(nc);
            }
            if (pa is SqlValue sv1 && sv1.domain is NodeType && sub is not null)
            {
                var co = new SqlField(sub.iix.dp, sub.ident, -1, sv1.defpos, Domain.Content, sv1.defpos);
                return cx.Add(co);
            }
            // if sub is non-zero there is a new chain to construct
            var nm = len - m;
            DBObject ob;
            // nm is the position  of the first such in the chain ic
            // create the missing components if any (see 6.1.2 (1))
            for (var i = len - 1; i >= nm; i--)
                if (ic[i] is Ident c)
                {// the ident of the component to create
                    if (i == len - 1)
                    {
                        ob = new SqlValue(c, cx, xp) ?? throw new PEException("PE1561");
                        cx.Add(ob);
                        // cx.defs enables us to find these objects again
                        cx.defs += (c, 1);
                        cx.defs += (ic, ic.Length);
                        if (!lxr.ParsingMatch) // flag as undefined unless we are parsing a MATCH
                            cx.undefined += (ob.defpos, cx.sD);
                        else if (ic[i-1] is Ident ip && cx.defs[ip.ident]?[cx.sD].Item1 is Iix px)
                        {
                            var pb = cx.obs[px.dp]??new SqlValue(new Ident(ip.ident,px), cx, xp);
                            cx.Add(pb);
                            ob = new SqlField(ob.defpos, c.ident, -1, pb.defpos, xp, pb.defpos);
                            cx.Add(ob);
                        }
                        pa = ob;
                    }
                    else
                        new ForwardReference(c, cx);
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
       Ident ParseIdent()
        {
            var c = new Ident(this);
            Mustbe(Sqlx.ID, Sqlx.PARTITION, Sqlx.POSITION, Sqlx.VERSIONING, Sqlx.CHECK,
                Sqlx.SYSTEM_TIME);
            return c;
        }
        /// <summary>
        /// Parse a IdentChain
        /// </summary>
        /// <returns>the Ident</returns>
		Ident ParseIdentChain() 
		{
            var left = ParseIdent();
			if (tok==Sqlx.DOT)
			{
				Next();
                if (!Match(Sqlx.ID)) // allow VERSIONING etc to follow - see  ParseVarOrColum
                    return left;
                left = new Ident(left, ParseIdentChain());
			}
			return left;
		}
        /// <summary>
		/// LoopStatement =	Label LOOP Statements END LOOP .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The  type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseLoopStatement(Domain xp, string? n)
        {
            var ls = new LoopStatement(LexDp(), n??"");
                Next();
                ls+=(WhenPart.Stms,ParseStatementList(xp));
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.LOOP);
                if (tok == Sqlx.ID && n != null && n == lxr.val.ToString())
                    Next();
            return (Executable)cx.Add(ls);
        }
        /// <summary>
		/// While =		Label WHILE SqlValue DO Statements END WHILE .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="xp">The type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseSqlWhile(Domain xp, string? n)
        {
            var ws = new WhileStatement(LexDp(), n??"");
            var old = cx; // new SaveContext(lxr, ExecuteStatus.Parse);
            Next();
            var s = ParseSqlValue(Domain.Bool);
            cx.Add(s);
            ws+=(WhileStatement.Search,s.defpos);
            Mustbe(Sqlx.DO);
            ws+=(cx,WhileStatement.What,ParseStatementList(xp));
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.WHILE);
            if (tok == Sqlx.ID && n != null && n == lxr.val.ToString())
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
            var rs = new RepeatStatement(LexDp(), n??"");
            Next();
            rs+=(cx,WhileStatement.What,ParseStatementList(xp));
            Mustbe(Sqlx.UNTIL);
            var s = ParseSqlValue(Domain.Bool);
            cx.Add(s);
            rs+=(WhileStatement.Search,s.defpos);
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.REPEAT);
            if (tok == Sqlx.ID && n != null && n == lxr.val.ToString())
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
			Sqlx s = tok;
			Ident? n = null;
			Next();
			if (s==Sqlx.LEAVE && tok==Sqlx.ID)
			{
				n = new Ident(this);
				Next();
			}
			return (Executable)cx.Add(new BreakStatement(LexDp(),n?.ident)); 
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
			Mustbe(Sqlx.ID);
			return (Executable)cx.Add(new IterateStatement(LexDp(), n.ident)); 
		}
        /// <summary>
        /// |	SIGNAL (id|SQLSTATE [VALUE] string) [SET item=Value {,item=Value} ]
        /// |   RESIGNAL [id|SQLSTATE [VALUE] string] [SET item=Value {,item=Value} ]
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSignal()
        {
            Sqlx s = tok;
            TypedValue n;
            Next();
            if (tok == Sqlx.ID)
            {
                n = lxr.val;
                Next();
            }
            else
            {
                Mustbe(Sqlx.SQLSTATE);
                if (tok == Sqlx.VALUE)
                    Next();
                n = lxr.val;
                Mustbe(Sqlx.CHARLITERAL);
            }
            var r = new SignalStatement(LexDp(), n.ToString()) + (SignalStatement.SType, s);
            if (tok == Sqlx.SET)
            {
                Next();
                for (; ; )
                {
                    Match(Sqlx.CLASS_ORIGIN, Sqlx.SUBCLASS_ORIGIN, Sqlx.CONSTRAINT_CATALOG,
                        Sqlx.CONSTRAINT_SCHEMA, Sqlx.CONSTRAINT_NAME, Sqlx.CATALOG_NAME,
                        Sqlx.SCHEMA_NAME, Sqlx.TABLE_NAME, Sqlx.COLUMN_NAME, Sqlx.CURSOR_NAME,
                        Sqlx.MESSAGE_TEXT);
                    var k = tok;
                    Mustbe(Sqlx.CLASS_ORIGIN, Sqlx.SUBCLASS_ORIGIN, Sqlx.CONSTRAINT_CATALOG,
                        Sqlx.CONSTRAINT_SCHEMA, Sqlx.CONSTRAINT_NAME, Sqlx.CATALOG_NAME,
                        Sqlx.SCHEMA_NAME, Sqlx.TABLE_NAME, Sqlx.COLUMN_NAME, Sqlx.CURSOR_NAME,
                        Sqlx.MESSAGE_TEXT);
                    Mustbe(Sqlx.EQL);
                    var sv = ParseSqlValue(Domain.Content);
                    cx.Add(sv);
                    r += (cx,SignalStatement.SetList, r.setlist + (k, sv.defpos));
                    if (tok != Sqlx.COMMA)
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
			Mustbe(Sqlx.ID);
			return (Executable)cx.Add(new OpenStatement(LexDp(),
                cx.Get(o, Domain.TableType) as SqlCursor
                ?? throw new DBException("34000",o.ToString())));
		}
        /// <summary>
		/// Close =		CLOSE id .
        /// </summary>
        /// <returns>The Executable result of the parse</returns>
		Executable ParseCloseStatement()
		{
			Next();
			var o = new Ident(this);
			Mustbe(Sqlx.ID);
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
            var how = Sqlx.NEXT;
            SqlValue? where = null;
            if (Match(Sqlx.NEXT, Sqlx.PRIOR, Sqlx.FIRST, 
                Sqlx.LAST, Sqlx.ABSOLUTE, Sqlx.RELATIVE))
            {
                how = tok;
                Next();
            }
            if (how == Sqlx.ABSOLUTE || how == Sqlx.RELATIVE)
                where = ParseSqlValue(Domain.Int);
            if (tok == Sqlx.FROM)
                Next();
            var o = new Ident(this);
            Mustbe(Sqlx.ID);
            var fs = new FetchStatement(dp, 
                cx.Get(o, Domain.TableType) as SqlCursor 
                ?? throw new DBException("34000", o.ToString()),
                how, where);
            Mustbe(Sqlx.INTO);
            fs+=(FetchStatement.Outs,ParseTargetList());
            return (Executable)cx.Add(fs);
        }
        /// <summary>
        /// [ TriggerCond ] (Call | (BEGIN ATOMIC Statements END)) .
		/// TriggerCond = WHEN '(' SqlValue ')' .
        /// </summary>
        /// <returns>a TriggerAction</returns>
        internal long ParseTriggerDefinition(PTrigger trig)
        {
            long oldStart = cx.parseStart; // safety
            cx.parse = ExecuteStatus.Parse;
            cx.parseStart = LexPos().lp;
            var op = cx.parse;
            var fn = new Ident(cx.NameFor(trig.target), LexPos());
            var tb = cx.db.objects[trig.target] as Table 
                ?? throw new PEException("PE1562");
            tb = (Table)cx.Add(tb);
            cx.Add(tb.framing);
            var fm = tb.RowSets(fn, cx, tb,fn.iix.dp);
            trig.from = fm.defpos;
            trig.dataType = fm;
            var tg = new Trigger(trig,cx.role);
            cx.Add(tg); // incomplete version for parsing
            if (trig.oldTable != null)
            {
                var tt = (TransitionTable)cx.Add(new TransitionTable(trig.oldTable, true, cx, fm, tg));
                var nix = new Iix(trig.oldTable.iix, tt.defpos);
                cx.defs += (trig.oldTable,nix);
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
                cx.defs += (trig.newTable,nix);
            }
            if (trig.newRow != null)
            {
                cx.Add(new SqlNewRow(trig.newRow, cx, fm));
                cx.defs += (trig.newRow, trig.newRow.iix);
            }
            for (var b = trig.dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var px = new Iix(fn.iix, p);
                    cx.defs += (cx.NameFor(p), px, Ident.Idents.Empty);
                }
            SqlValue? when = null;
            Executable? act;
            if (tok == Sqlx.WHEN)
            {
                Next();
                when = ParseSqlValue(Domain.Bool);
            }
            if (tok == Sqlx.BEGIN)
            {
                Next();
                if (new CompoundStatement(LexDp(),"") is not CompoundStatement cs)
                    throw new DBException("42161","CompoundStatement");
                var ss = BList<long?>.Empty;
                Mustbe(Sqlx.ATOMIC);
                while (tok != Sqlx.END)
                {
                    if (ParseProcedureStatement(Domain.Content) is not Executable a)
                        throw new DBException("42161", "statement");
                    ss+=cx.Add(a).defpos; 
                    if (tok == Sqlx.END)
                        break;
                    Mustbe(Sqlx.SEMICOLON);
                }
                Next();
                cs+=(cx,CompoundStatement.Stms,ss);
                cs += (DBObject.Dependents, _Deps(ss));
                act = cs;
            }
            else
                act = ParseProcedureStatement(Domain.Content)??
                    throw new DBException("42161","statement");
            cx.Add(act);
            var r = (WhenPart)cx.Add(new WhenPart(LexDp(), 
                when??SqlNull.Value, new BList<long?>(act.defpos)));
            trig.def = r.defpos;
            trig.framing = new Framing(cx,trig.nst);
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
            Mustbe(Sqlx.ID);
            PTrigger.TrigType tgtype = 0;
            var w = Mustbe(Sqlx.BEFORE, Sqlx.INSTEAD, Sqlx.AFTER);
            switch (w)
            {
                case Sqlx.BEFORE: tgtype |= PTrigger.TrigType.Before; break;
                case Sqlx.AFTER: tgtype |= PTrigger.TrigType.After; break;
                case Sqlx.INSTEAD: Mustbe(Sqlx.OF); tgtype |= PTrigger.TrigType.Instead; break;
            }
            tgtype = ParseTriggerHow(tgtype);
            var cls = BList<Ident>.Empty;
            var upd = (tgtype & PTrigger.TrigType.Update) == PTrigger.TrigType.Update;
            if (upd && tok == Sqlx.OF)
            {
                Next();
                cls += new Ident(this);
                Mustbe(Sqlx.ID);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    cls += new Ident(this);
                    Mustbe(Sqlx.ID);
                }
            }
            Mustbe(Sqlx.ON);
            var tabl = new Ident(this);
            Mustbe(Sqlx.ID);
            var tb = cx.GetObject(tabl.ident) as Table ?? throw new DBException("42107", tabl.ToString()).Mix();
            Ident? or = null, nr = null, ot = null, nt = null;
            if (tok == Sqlx.REFERENCING)
            {
                Next();
                while (tok == Sqlx.OLD || tok == Sqlx.NEW)
                {
                    if (tok == Sqlx.OLD)
                    {
                        if ((tgtype & PTrigger.TrigType.Insert) == PTrigger.TrigType.Insert)
                            throw new DBException("42146", "OLD", "INSERT").Mix();
                        Next();
                        if (tok == Sqlx.TABLE)
                        {
                            Next();
                            if (ot != null)
                                throw new DBException("42143", "OLD").Mix();
                            if (tok == Sqlx.AS)
                                Next();
                            ot = new Ident(this);
                            Mustbe(Sqlx.ID);
                            continue;
                        }
                        if (tok == Sqlx.ROW)
                            Next();
                        if (or != null)
                            throw new DBException("42143", "OLD ROW").Mix();
                        if (tok == Sqlx.AS)
                            Next();
                        or = new Ident(this);
                        Mustbe(Sqlx.ID);
                    }
                    else
                    {
                        Mustbe(Sqlx.NEW);
                        if ((tgtype & PTrigger.TrigType.Delete) == PTrigger.TrigType.Delete)
                            throw new DBException("42146", "NEW", "DELETE").Mix();
                        if (tok == Sqlx.TABLE)
                        {
                            Next();
                            if (nt != null)
                                throw new DBException("42143", "NEW").Mix();
                            nt = new Ident(lxr.val.ToString(), tabl.iix);
                            Mustbe(Sqlx.ID);
                            continue;
                        }
                        if (tok == Sqlx.ROW)
                            Next();
                        if (nr != null)
                            throw new DBException("42143", "NEW ROW").Mix();
                        if (tok == Sqlx.AS)
                            Next();
                        nr = new Ident(lxr.val.ToString(), tabl.iix);
                        Mustbe(Sqlx.ID);
                    }
                }
            }
            if (tok == Sqlx.FOR)
            {
                Next();
                Mustbe(Sqlx.EACH);
                if (tok == Sqlx.ROW)
                {
                    Next();
                    tgtype |= PTrigger.TrigType.EachRow;
                }
                else
                {
                    Mustbe(Sqlx.STATEMENT);
                    if (tok == Sqlx.DEFERRED)
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
            for (var b=cls?.First(); b is not null; b=b.Next())
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
			if (tok==Sqlx.INSERT)
			{
				Next();
				type |= PTrigger.TrigType.Insert;
			} 
			else if (tok==Sqlx.UPDATE)
			{
				Next();
				type |= PTrigger.TrigType.Update;
			} 
			else
			{
				Mustbe(Sqlx.DELETE);
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
            if (cx.role.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole)==false)
                throw new DBException("42105");
            Next();
            MethodModes();
            Match(Sqlx.DOMAIN,Sqlx.TYPE,Sqlx.ROLE,Sqlx.VIEW);
			switch (tok)
			{
                case Sqlx.TABLE: ParseAlterTable(); break;
                case Sqlx.DOMAIN: ParseAlterDomain(); break;
                case Sqlx.TYPE: ParseAlterType(); break;
                case Sqlx.FUNCTION: ParseAlterProcedure(); break;
                case Sqlx.PROCEDURE: ParseAlterProcedure(); break;
                case Sqlx.OVERRIDING: ParseMethodDefinition(); break;
                case Sqlx.INSTANCE: ParseMethodDefinition(); break;
                case Sqlx.STATIC: ParseMethodDefinition(); break;
                case Sqlx.CONSTRUCTOR: ParseMethodDefinition(); break;
                case Sqlx.METHOD: ParseMethodDefinition(); break;
                case Sqlx.VIEW: ParseAlterView(); break;
                default:
					throw new DBException("42125",tok).Mix();
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
            Mustbe(Sqlx.ID);
            tb = cx.GetObject(o.ident) as Table??
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
            Mustbe(Sqlx.ID);
            Domain vw = (Domain)(cx.GetObject(nm.ident) ??
                throw new DBException("42107", nm).Mix());
            ParseAlterOp(vw);
            while (tok == Sqlx.COMMA)
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
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            var kind = (ob is View)?Sqlx.VIEW:Sqlx.FUNCTION;
            long np;
            if (tok == Sqlx.SET && ob is View)
            {
                Next();
                int st;
                Ident s;
                var lp = LexPos();
                Mustbe(Sqlx.SOURCE);
                Mustbe(Sqlx.TO);
                st = lxr.start;
                RowSet qe;
                qe = ParseQueryExpression(ob);
                s = new Ident(new string(lxr.input, st, lxr.start - st), lp);
                cx.Add(new Modify("Source", ob.defpos, qe, s, cx.db.nextPos, cx));
            }
            else if (tok == Sqlx.TO)
            {
                Next();
                var nm = lxr.val;
                Mustbe(Sqlx.ID);
                cx.Add(new Change(ob.defpos, nm.ToString(), cx.db.nextPos, cx));
            }
            else if (tok == Sqlx.ALTER)
            {
                Next();
                var ic = new Ident(this);
                Mustbe(Sqlx.ID);
                ob = (Domain)(cx.GetObject(ic.ident) ??
                    throw new DBException("42135", ic.ident));
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Mix();
                var m = ParseMetadata(Sqlx.COLUMN);
                cx.Add(new PMetadata(ic.ident, 0, ob, m, cx.db.nextPos));
            }
            if (StartMetadata(kind) || Match(Sqlx.ADD))
            {
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Mix();
                if (tok == Sqlx.ALTER)
                    Next();
                var m = ParseMetadata(kind);
                np = tr.nextPos;
                cx.Add(new PMetadata(ob.NameFor(cx), -1, ob,m,np));
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
            while (tok == Sqlx.COMMA)
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
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            if (tok == Sqlx.SET)
            {
                Next();
                Mustbe(Sqlx.DEFAULT);
                int st = lxr.start;
                var dv = ParseSqlValue(Domain.For(d.kind));
                string ds = new(lxr.input, st, lxr.start - st);
                es+= cx.Add(new Edit(d, d.name, d + (Domain.Default, dv) + (Domain.DefaultString, ds),
                    cx.db.nextPos, cx))?.defpos;
            }
            else if (Match(Sqlx.ADD))
            {
                Next();
                Ident id;
                if (tok == Sqlx.CONSTRAINT)
                {
                    Next();
                    id = new Ident(this);
                    Mustbe(Sqlx.ID);
                }
                else
                    id = new Ident(this);
                Mustbe(Sqlx.CHECK);
                Mustbe(Sqlx.LPAREN);
                var nst = cx.db.nextStmt;
                int st = lxr.pos;
                var sc = ParseSqlValue(Domain.Bool).Reify(cx);
                string source = new(lxr.input, st, lxr.pos - st - 1);
                Mustbe(Sqlx.RPAREN);
                var pc = new PCheck(d, id.ident, sc, source, nst, tr.nextPos, cx);
                es += cx.Add(pc)?.defpos;
            }
            else if (tok == Sqlx.DROP)
            {
                Next();
                var dp = cx.db.Find(d)?.defpos ?? -1L;
                if (tok == Sqlx.DEFAULT)
                {
                    Next();
                   es += cx.Add(new Edit(d, d.name, d, tr.nextPos, cx))?.defpos;
                }
                else if (StartMetadata(Sqlx.DOMAIN) || Match(Sqlx.ADD, Sqlx.DROP))
                {
                    if (tr.role.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Mix();
                    var m = ParseMetadata(Sqlx.DOMAIN);
                    es += cx.Add(new PMetadata(d.name, -1, d, m, dp))?.defpos;
                }
                else
                {
                    Mustbe(Sqlx.CONSTRAINT);
                    var n = new Ident(this);
                    Mustbe(Sqlx.ID);
                    Drop.DropAction s = ParseDropAction();
                    var ch = (Check?)cx.GetObject(n.ident) ?? throw new DBException("42135", n.ident);
                    es += cx.Add(new Drop1(ch.defpos, s, tr.nextPos))?.defpos;
                }
            }
            else if (StartMetadata(Sqlx.DOMAIN) || Match(Sqlx.ADD, Sqlx.DROP))
            {
                if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Mix();
                es += cx.Add(new PMetadata(d.name, 0, d, ParseMetadata(Sqlx.DOMAIN),
                    tr.nextPos))?.defpos;
            }
            else
            {
                Mustbe(Sqlx.TYPE);
                var dt = ParseSqlDataType();
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
            Match(Sqlx.RESTRICT, Sqlx.CASCADE);
            Drop.DropAction r = 0;
			switch (tok)
			{
                case Sqlx.CASCADE: r = Drop.DropAction.Cascade; Next(); break;
                case Sqlx.RESTRICT: r = Drop.DropAction.Restrict; Next(); break;
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
			while (tok==Sqlx.COMMA)
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
            var tr = cx.db as Transaction?? throw new DBException("2F003");
            cx.Add(tb.framing);
            if (tok == Sqlx.TO)
            {
                Next();
                var o = lxr.val;
                Mustbe(Sqlx.ID);
                cx.Add(new Change(tb.defpos, o.ToString(), tr.nextPos, cx));
            }
            if (tok == Sqlx.LEVEL)
                ParseClassification(tb);
            if (tok == Sqlx.SCOPE)
                ParseEnforcement(tb);
            Match(Sqlx.ADD);
            switch (tok)
            {
                case Sqlx.CONSTRAINT:
                    ParseCheckConstraint(tb);
                    return;
                case Sqlx.DROP:
                    {
                        Next();
                        switch (tok)
                        {
                            case Sqlx.CONSTRAINT:
                                {
                                    Next();
                                    var name = new Ident(this);
                                    Mustbe(Sqlx.ID);
                                    Drop.DropAction act = ParseDropAction();
                                    if (cx.GetObject(name.ident) is Check ck)
                                        cx.Add(new Drop1(ck.defpos, act, tr.nextPos));
                                    return;
                                }
                            case Sqlx.PRIMARY:
                                {
                                    Next();
                                    Mustbe(Sqlx.KEY);
                                    Drop.DropAction act = ParseDropAction();
                                    if (ParseColsList(tb) is not Domain cols)
                                        throw new DBException("42161", "cols");
                                    cols += (Domain.Kind, Sqlx.ROW);
                                    Level3.Index x = (tb).FindIndex(cx.db, cols)?[0]
                                        ?? throw new DBException("42164", tb.NameFor(cx));
                                    if (x != null)
                                        cx.Add(new Drop1(x.defpos, act, tr.nextPos));
                                    return;
                                }
                            case Sqlx.FOREIGN:
                                {
                                    Next();
                                    Mustbe(Sqlx.KEY);
                                    if (ParseColsList(tb) is not Domain cols)
                                        throw new DBException("42161", "cols");
                                    Mustbe(Sqlx.REFERENCES);
                                    var n = new Ident(this);
                                    Mustbe(Sqlx.ID);
                                    var rt = cx.GetObject(n.ident) as Table ??
                                        throw new DBException("42107", n).Mix();
                                    var st = lxr.pos;
                                    if (tok == Sqlx.LPAREN && ParseColsList(rt) is null)
                                        throw new DBException("42161", "rcols");
                                    var x = (tb).FindIndex(cx.db, cols)?[0];
                                    if (x != null)
                                    {
                                        cx.Add(new Drop(x.defpos, tr.nextPos));
                                        return;
                                    }
                                    throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                                }
                            case Sqlx.UNIQUE:
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
                            case Sqlx.PERIOD:
                                {
                                    var ptd = ParseTablePeriodDefinition();
                                    var pd = (ptd.pkind == Sqlx.SYSTEM_TIME) ? tb.systemPS : tb.applicationPS;
                                    if (pd > 0)
                                        cx.Add(new Drop(pd, tr.nextPos));
                                    return;
                                }
                            case Sqlx.WITH:
                                ParseVersioningClause(tb, true);
                                return;
                            default:
                                {
                                    if (StartMetadata(Sqlx.TABLE))
                                    {
                                        if (cx.role.Denied(cx, Grant.Privilege.Metadata))
                                            throw new DBException("42105").Mix();
                                        cx.Add(new PMetadata(tb.NameFor(cx), 0, tb,
                                                ParseMetadata(Sqlx.TABLE), tr.nextPos));
                                        return;
                                    }
                                    if (tok == Sqlx.COLUMN)
                                        Next();
                                    var name = new Ident(this);
                                    Mustbe(Sqlx.ID);
                                    Drop.DropAction act = ParseDropAction();
                                    var tc = (TableColumn?)tr.objects[tb.ColFor(cx, name.ident)]
                                       ?? throw new DBException("42135", name);
                                    if (tc != null)
                                        cx.Add(new Drop1(tc.defpos, act, tr.nextPos));
                                    return;
                                }
                        }
                    }
                case Sqlx.ADD:
                    {
                        Next();
                        if (tok == Sqlx.PERIOD)
                            tb = AddTablePeriodDefinition(tb);
                        else if (tok == Sqlx.WITH)
                            tb = ParseVersioningClause(tb, false);
                        else if (tok == Sqlx.CONSTRAINT || tok == Sqlx.UNIQUE || tok == Sqlx.PRIMARY || tok == Sqlx.FOREIGN || tok == Sqlx.CHECK)
                            tb = ParseTableConstraintDefin(tb);
                        else
                            tb = ParseColumnDefin(tb);
                        break;
                    }
                case Sqlx.ALTER:
                    {
                        Next();
                        if (tok == Sqlx.COLUMN)
                            Next();
                        var o = new Ident(this);
                        Mustbe(Sqlx.ID);
                        var col = (TableColumn?)cx.db.objects[tb.ColFor(cx, o.ident)]
                                ?? throw new DBException("42112", o.ident);
                        while (StartMetadata(Sqlx.COLUMN) || Match(Sqlx.TO, Sqlx.POSITION, Sqlx.SET, Sqlx.DROP, Sqlx.ADD, Sqlx.TYPE))
                            tb = ParseAlterColumn(tb, col, o.ident);
                        return;
                    }
                case Sqlx.PERIOD:
                    {
                        if (Match(Sqlx.ID))
                        {
                            var pid = lxr.val;
                            Next();
                            Mustbe(Sqlx.TO);
                            if (cx.db.objects[tb.applicationPS] is not PeriodDef pd)
                                throw new DBException("42162", pid).Mix();
                            pid = lxr.val;
                            Mustbe(Sqlx.ID);
                            cx.Add(new Change(pd.defpos, pid.ToString(), tr.nextPos, cx));
                        }
                        tb = AddTablePeriodDefinition(tb);
                        return;
                    }
                case Sqlx.SET:
                    {
                        Next();
                        if (ParseColsList(tb) is not Domain cols)
                            throw new DBException("42161", "cols");
                        Mustbe(Sqlx.REFERENCES);
                        var n = new Ident(this);
                        Mustbe(Sqlx.ID);
                        var rt = cx.GetObject(n.ident) as Table ??
                            throw new DBException("42107", n).Mix();
                        var st = lxr.pos;
                        if (tok == Sqlx.LPAREN && ParseColsList(rt) is null)
                            throw new DBException("42161", "cols");
                        PIndex.ConstraintType ct = 0;
                        if (tok == Sqlx.ON)
                            ct = ParseReferentialAction();
                        cols += (Domain.Kind, Sqlx.ROW);
                        if (tb.FindIndex(cx.db, cols, PIndex.ConstraintType.ForeignKey)?[0] is Level3.Index x)
                        {
                            cx.Add(new RefAction(x.defpos, ct, tr.nextPos));
                            return;
                        }
                        throw new DBException("42135", new string(lxr.input, st, lxr.pos - st)).Mix();
                    }
                default:
                    if (StartMetadata(Sqlx.TABLE) || Match(Sqlx.ADD, Sqlx.DROP))
                        if (tb.Denied(cx, Grant.Privilege.Metadata))
                            throw new DBException("42105");
                    cx.Add(new PMetadata(tb.NameFor(cx), 0, tb, ParseMetadata(Sqlx.TABLE),
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
            var tr = cx.db as Transaction?? throw new DBException("2F003");
			TypedValue o;
            if (tok == Sqlx.TO)
            {
                Next();
                var n = new Ident(this);
                Mustbe(Sqlx.ID);
                var pc = new Change(tc.defpos, n.ident, tr.nextPos, cx);
                tb = (Table)(cx.Add(pc) ?? throw new DBException("42105"));
                return tb;
            }
            if (tok == Sqlx.POSITION)
            {
                Next();
                o = lxr.val;
                Mustbe(Sqlx.INTEGERLITERAL);
                if (o.ToInt() is not int n)
                    throw new DBException("42161", "INTEGER");
                var pa = new Alter3(tc.defpos, nm, n, tb, tc.domain,
                    tc.generated.gfs ?? cx.obs?.ToString() ?? "",
                    tc.domain.defaultValue ?? TNull.Value,
                    "", tc.update, tc.domain.notNull, tc.generated, tr.nextPos, cx);
                tb = (Table)(cx.Add(pa)??throw new DBException("42105"));
                return tb;
            }
            if (Match(Sqlx.ADD))
            {
                Next();
                while (StartMetadata(Sqlx.COLUMN))
                {
                    if (tb.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105").Mix();
                    var pm = new PMetadata(nm, 0, tc, ParseMetadata(Sqlx.COLUMN), tr.nextPos);
                    tc = (TableColumn)(cx.Add(pm) ?? throw new DBException("42105"));
                }
                if (tok == Sqlx.CONSTRAINT)
                    Next();
                var n = new Ident(this);
                Mustbe(Sqlx.ID);
                Mustbe(Sqlx.CHECK);
                Mustbe(Sqlx.LPAREN);
                int st = lxr.pos;
                var nst = cx.db.nextStmt;
                var se = ParseSqlValue(Domain.Bool).Reify(cx);
                string source = new(lxr.input, st, lxr.pos - st - 1);
                Mustbe(Sqlx.RPAREN);
                var pc = new PCheck(tc, n.ident, se, source, nst, tr.nextPos, cx);
                tc = (TableColumn)(cx.Add(pc) ?? throw new DBException("42105"));
            }
            else if (tok == Sqlx.SET)
            {
                Next();
                if (tok == Sqlx.DEFAULT)
                {
                    Next();
                    int st = lxr.start;
                    var dv = lxr.val;
                    Next();
                    var ds = new string(lxr.input, st, lxr.start - st);
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, tc.domain, ds, dv, "",
                        CTree<UpdateAssignment, bool>.Empty, false,
                        GenerationRule.None, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105"));
                }
                else if (Match(Sqlx.GENERATED))
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
                        "", CTree<UpdateAssignment, bool>.Empty, tc.domain.notNull, gr, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105"));
                }
                else if (Match(Sqlx.NOT))
                {
                    Next();
                    Mustbe(Sqlx.NULL);
                    tc.ColumnCheck(tr, false);
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, tc.domain, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                        true, tc.generated, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105"));
                }
                return ParseColumnConstraint(tb, tc);
            }
            else if (tok == Sqlx.DROP)
            {
                Next();
                if (StartMetadata(Sqlx.COLUMN))
                {
                    if (tb.Denied(cx, Grant.Privilege.Metadata))
                        throw new DBException("42105", tc.NameFor(cx)).Mix();
                    var pm = new PMetadata(nm, 0, tc, ParseMetadata(Sqlx.COLUMN), tr.nextPos);
                    tc = (TableColumn)(cx.Add(pm) ?? throw new DBException("42105"));
                }
                else
                {
                    if (tok != Sqlx.DEFAULT && tok != Sqlx.NOT && tok != Sqlx.PRIMARY && tok != Sqlx.REFERENCES && tok != Sqlx.UNIQUE && tok != Sqlx.CONSTRAINT && !StartMetadata(Sqlx.COLUMN))
                        throw new DBException("42000", lxr.Diag).ISO();
                    if (tok == Sqlx.DEFAULT)
                    {
                        Next();
                        var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                            tb, tc.domain, "", TNull.Value, tc.updateString ?? "", tc.update, tc.domain.notNull,
                            GenerationRule.None, tr.nextPos, cx);
                        tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105"));
                    }
                    else if (tok == Sqlx.NOT)
                    {
                        Next();
                        Mustbe(Sqlx.NULL);
                        var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                            tb, tc.domain, tc.domain.defaultString, tc.domain.defaultValue,
                            tc.updateString ?? "", tc.update, false,
                            tc.generated, tr.nextPos, cx);
                        tc = (TableColumn)(cx.Add(pa) ?? throw new DBException("42105"));
                    }
                    else if (tok == Sqlx.PRIMARY)
                    {
                        Next();
                        Mustbe(Sqlx.KEY);
                        Drop.DropAction act = ParseDropAction();
                        if (tb.FindPrimaryIndex(cx) is Level3.Index x)
                        {
                            if (x.keys.Length != 1 || x.keys[0] != tc.defpos)
                                throw new DBException("42158", tb.NameFor(cx), tc.NameFor(cx)).Mix()
                                    .Add(Sqlx.TABLE_NAME, new TChar(tb.NameFor(cx)))
                                    .Add(Sqlx.COLUMN_NAME, new TChar(tc.NameFor(cx)));
                            var pd = new Drop1(x.defpos, act, tr.nextPos);
                            cx.Add(pd);
                        }
                    }
                    else if (tok == Sqlx.REFERENCES)
                    {
                        Next();
                        var n = new Ident(this);
                        Mustbe(Sqlx.ID);
                        if (tok == Sqlx.LPAREN)
                        {
                            Next();
                            Mustbe(Sqlx.ID);
                            Mustbe(Sqlx.RPAREN);
                        }
                        Level3.Index? dx = null;
                        for (var p = tb.indexes.First(); dx == null && p != null; p = p.Next())
                            for (var c = p.value().First(); dx == null && c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is Level3.Index x && x.keys.Length == 1 && x.keys[0] == tc.defpos &&
                                    cx.db.objects[x.reftabledefpos] is Table rt && rt.NameFor(cx) == n.ident)
                                    dx = x;
                        if (dx == null)
                            throw new DBException("42159", nm, n.ident).Mix()
                                .Add(Sqlx.TABLE_NAME, new TChar(n.ident))
                                .Add(Sqlx.COLUMN_NAME, new TChar(nm));
                        var pd = new Drop(dx.defpos, tr.nextPos);
                        cx.Add(pd);
                    }
                    else if (tok == Sqlx.UNIQUE)
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
                                .Add(Sqlx.TABLE_NAME, new TChar(nm));
                        var pd = new Drop(dx.defpos, tr.nextPos);
                        cx.Add(pd);
                    }
                    else if (tok == Sqlx.CONSTRAINT)
                    {
                        var n = new Ident(this);
                        Mustbe(Sqlx.ID);
                        Drop.DropAction s = ParseDropAction();
                        var ch = cx.GetObject(n.ident) as Check ?? throw new DBException("42135", n.ident);
                        var pd = new Drop1(ch.defpos, s, tr.nextPos);
                        cx.Add(pd);
                    }
                }
            }
            else if (Match(Sqlx.TYPE))
            {
                Next();
                Domain? type;
                if (tok == Sqlx.ID)
                {
                    var domain = new Ident(this);
                    Next();
                    type = (Domain?)cx.GetObject(domain.ident);
                    if (type == null)
                        throw new DBException("42119", domain.ident, cx.db.name).Mix()
                            .Add(Sqlx.CATALOG_NAME, new TChar(cx.db.name))
                            .Add(Sqlx.TYPE, new TChar(domain.ident));
                }
                else if (Match(Sqlx.CHARACTER, Sqlx.CHAR, Sqlx.VARCHAR, Sqlx.NATIONAL, Sqlx.NCHAR,
                    Sqlx.BOOLEAN, Sqlx.NUMERIC, Sqlx.DECIMAL,
                    Sqlx.DEC, Sqlx.FLOAT, Sqlx.REAL, // Sqlx.LONG,Sqlx.DOUBLE,
                    Sqlx.INT, // Sqlx.BIGINT,
                    Sqlx.INTEGER,// Sqlx.SMALLINT,
                    Sqlx.BINARY, Sqlx.BLOB, Sqlx.NCLOB,
                    Sqlx.CLOB, Sqlx.DATE, Sqlx.TIME, Sqlx.ROW, Sqlx.TABLE))
                {
                    type = ParseSqlDataType() + (Domain.Default, tc.domain.defaultValue)
                        + (Domain.DefaultString, tc.domain.defaultString);
                    type = (Domain)cx.Add(type);
                    if (!tc.domain.CanTakeValueOf(type))
                        throw new DBException("2200G");
                    var pa = new Alter3(tc.defpos, nm, tb.PosFor(cx, nm),
                        tb, type,
                        type.defaultString, type.defaultValue, tc.updateString ?? "", tc.update,
                        tc.domain.notNull, tc.generated, tr.nextPos, cx);
                    tc = (TableColumn)(cx.Add(pa)??throw new DBException("42105"));
                }
            }
            if (StartMetadata(Sqlx.COLUMN))
            {
                if (tb.Denied(cx, Grant.Privilege.Metadata))
                    throw new DBException("42105").Mix();
                var md = ParseMetadata(Sqlx.COLUMN);
                var pm = new PMetadata(nm, 0, tc, md, tr.nextPos);
                tc = (TableColumn)(cx.Add(pm) ?? throw new DBException("42105"));
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
            Mustbe(Sqlx.ID);
            if (cx.role is not Role ro || (!ro.dbobjects.Contains(id.ident)) ||
            cx._Ob(ro.dbobjects[id.ident]??-1L) is not UDType tp || tp.infos[ro.defpos] is not ObInfo oi) 
                throw new DBException("42133", id.ident).Mix()
                    .Add(Sqlx.TYPE, new TChar(id.ident)); 
            if (tok == Sqlx.TO)
            {
                Next();
                id = new Ident(this);
                Mustbe(Sqlx.ID);
                cx.Add(new Change(ro.dbobjects[id.ident]??-1L, id.ident, tr.nextPos, cx));
            }
            else if (tok == Sqlx.SET)
            {
                Next();
                if (Match(Sqlx.UNDER))
                {
                    Next();
                    var ui = new Ident(this);
                    Mustbe(Sqlx.ID);
                    if (cx.role.dbobjects.Contains(ui.ident)
                        && cx.db.objects[cx.role.dbobjects[ui.ident] ?? -1L] is UDType tu)
                        cx.Add(new EditType(id.ident, tp, tp, tu, cx.db.nextPos, cx));
                }
                else
                {
                    id = new Ident(this);
                    Mustbe(Sqlx.ID);
                    var sq = tp.PosFor(cx, id.ident);
                    var ts = tp.ColFor(cx, id.ident);
                    if (cx.db.objects[ts] is not TableColumn tc)
                        throw new DBException("42133", id).Mix()
                            .Add(Sqlx.TYPE, new TChar(id.ident));
                    Mustbe(Sqlx.TO);
                    id = new Ident(this);
                    Mustbe(Sqlx.ID);
                    new Alter3(tc.defpos, id.ident, sq,
                        (Table?)cx.db.objects[tc.tabledefpos] ?? throw new DBException("42105"),
                        tc.domain, tc.domain.defaultString,
                        tc.domain.defaultValue, tc.updateString ?? "",
                        tc.update, tc.domain.notNull, tc.generated, tr.nextPos, cx);
                }
            }
            else if (tok == Sqlx.DROP)
            {
                Next();
                if (tok == Sqlx.CONSTRAINT || tok==Sqlx.COLUMN)
                {
                    ParseDropStatement();
                    return;
                }
                if (tok == Sqlx.UNDER)
                {
                    Next();
                    var st = tp.super ?? throw new PEException("PE92612");
                    cx.Add(new EditType(id.ident, tp, st, null, cx.db.nextPos, cx));
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
                                Add(Sqlx.TYPE, new TChar(tp.name));
                        ParseDropAction();
                        new Drop(mt.defpos, tr.nextPos);
                    }
                    else
                    {
                        if (cx.db.objects[tp.ColFor(cx, id.ident)] is not TableColumn tc)
                            throw new DBException("42133", id).Mix()
                                .Add(Sqlx.TYPE, new TChar(id.ident));
                        ParseDropAction();
                        new Drop(tc.defpos, tr.nextPos);
                    }
                }
            }
            else if (Match(Sqlx.ADD))
            {
                Next();
                if (tok==Sqlx.CONSTRAINT)
                {
                    cx.AddDefs(tp);
                    ParseCheckConstraint(tp);
                    return;
                }
                if (tok==Sqlx.COLUMN)
                {
                    ParseColumnDefin(tp);
                    return;
                }
                MethodModes();
                if (Match(Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.OVERRIDING, Sqlx.METHOD))
                {
                    ParseMethodHeader(tp);
                    return;
                }
                var (nm,dm,md) = ParseMember(id);
                var c = new PColumn2(tp,nm.ident, -1, dm, dm.defaultString, dm.defaultValue,
                        false, GenerationRule.None, tr.nextPos, cx);
                cx.Add(c);
                var tc = (TableColumn)(cx.obs[c.defpos]?? throw new DBException("42105"));
                if (md!=CTree<Sqlx,TypedValue>.Empty)
                    cx.Add(new PMetadata(nm.ident, 0, tc, md, tr.nextPos));
            }
            else if (tok == Sqlx.ALTER)
            {
                Next();
                id = new Ident(this);
                Mustbe(Sqlx.ID);
                if (cx.db.objects[tp.ColFor(cx, id.ident)] is not TableColumn tc)
                    throw new DBException("42133", id).Mix()
                        .Add(Sqlx.TYPE, new TChar(id.ident));
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
                throw new DBException("42105");
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            TypedValue dv = tc.domain.defaultValue;
            var ds = "";
            for (; ; )
            {
                if (tok == Sqlx.TO)
                {
                    Next();
                    var n = new Ident(this);
                    Mustbe(Sqlx.ID);
                    tc = (TableColumn)(cx.Add(new Change(tc.defpos, n.ident, tr.nextPos, cx))
                        ?? throw new DBException("42105"));
                    goto skip;
                }
                else if (Match(Sqlx.TYPE))
                {
                    Next();
                    tc = (TableColumn)tc.New(tc.mem + ParseSqlDataType().mem);
                }
                else if (tok == Sqlx.SET)
                {
                    Next();
                    Mustbe(Sqlx.DEFAULT);
                    var st = lxr.start;
                    dv = lxr.val;
                    Next();
                    ds = new string(lxr.input, st, lxr.start - st);
                    tc += (Domain.DefaultString, ds);
                    tc += (Domain.Default, dv);
                }
                else if (tok == Sqlx.DROP)
                {
                    Next();
                    Mustbe(Sqlx.DEFAULT);
                    dv = TNull.Value;
                    tc += (Domain.Default, dv);
                }
                if (cx._Ob(tc.tabledefpos) is Domain td && ci.name is not null)
                    tc = (TableColumn)(cx.Add(new Alter3(tc.defpos, ci.name, td.PosFor(cx,ci.name), 
                         (Table?)cx.db.objects[tc.tabledefpos]??throw new DBException("42105"),
                         tc.domain, ds, dv, tc.updateString??"", tc.update,
                         tc.domain.notNull, GenerationRule.None, tr.nextPos, cx))??throw new DBException("42105"));
            skip:
                if (tok != Sqlx.COMMA)
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
            bool func = tok == Sqlx.FUNCTION;
            Next();
            ParseProcedureClause(func, Sqlx.ALTER);
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
            if (cx.role==null || cx.role.infos[cx.role.defpos]?.priv.HasFlag(Grant.Privilege.AdminRole)==false)
                throw new DBException("42105");
            var tr = cx.db as Transaction ?? throw new DBException("2F003");
            Next();
            if (Match(Sqlx.ORDERING))
            {
                Next(); Mustbe(Sqlx.FOR);
                var o = new Ident(this);
                Mustbe(Sqlx.ID);
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
        /// <summary>
		/// Type = 		StandardType | DefinedType | DomainName | REF(TableReference) .
		/// DefinedType = 	ROW  Representation
		/// 	|	TABLE Representation
        /// 	|   ( Type {, Type }) 
        ///     |   Type UNION Type { UNION Type }.
        /// </summary>
        /// <param name="pn">Parent ID (Type, or Procedure)</param>
		Domain ParseSqlDataType(Ident? pn=null)
        {
            Domain r;
            Sqlx tp = tok;
            if (Match(Sqlx.TABLE, Sqlx.ROW, Sqlx.TYPE, Sqlx.LPAREN))// anonymous row type
            {
                if (Match(Sqlx.TABLE, Sqlx.ROW, Sqlx.TYPE))
                    Next();
                else
                    tp = Sqlx.TYPE;
                if (tok == Sqlx.LPAREN)
                   return ParseRowTypeSpec(tp,pn); // pn is needed for tp==TYPE case
            }
            var cn = new Ident(this);
            if (tok==Sqlx.ID && cx.GetObject(cn.ident) is UDType ut)
            {
                Next();
                ut.Defs(cx);
                return ut;
            }
            r = ParseStandardDataType();
            if (r == Domain.Null || r==Domain.Content)
            {
                var o = new Ident(this);
                Next();
                r = (Domain)(cx.db.objects[cx.role.dbobjects[o.ident]??-1L]??Domain.Content);
            } 
            if (tok == Sqlx.SENSITIVE)
            {
                r = (Domain)cx.Add(new Domain(cx.GetUid(),tok, r));
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
            if (Match(Sqlx.CHARACTER, Sqlx.CHAR, Sqlx.VARCHAR))
            {
                r = r0 = Domain.Char;
                Next();
                if (tok == Sqlx.LARGE)
                {
                    Next();
                    Mustbe(Sqlx.OBJECT); // CLOB is CHAR in Pyrrho
                }
                else if (tok == Sqlx.VARYING)
                    Next();
                r = ParsePrecPart(r);
                if (tok == Sqlx.CHARACTER)
                {
                    Next();
                    Mustbe(Sqlx.SET);
                    var o = new Ident(this);
                    Mustbe(Sqlx.ID);
                    r += (Domain.Charset, (Common.CharSet)Enum.Parse(typeof(Common.CharSet), o.ident, false));
                }
                if (tok == Sqlx.COLLATE)
                    r += (Domain.Culture, new CultureInfo(ParseCollate()));
            }
            else if (Match(Sqlx.NATIONAL, Sqlx.NCHAR))
            {
                if (tok == Sqlx.NATIONAL)
                {
                    Next();
                    Mustbe(Sqlx.CHARACTER);
                }
                else
                    Next();
                r = r0 = Domain.Char;
                if (tok == Sqlx.LARGE)
                {
                    Next();
                    Mustbe(Sqlx.OBJECT); // NCLOB is NCHAR in Pyrrho
                }
                r = ParsePrecPart(r);
            }
            else if (Match(Sqlx.NUMERIC, Sqlx.DECIMAL, Sqlx.DEC))
            {
                r = r0 = Domain._Numeric;
                Next();
                r = ParsePrecScale(r);
            }
            else if (Match(Sqlx.FLOAT, Sqlx.REAL, Sqlx.DOUBLE))
            {
                r = r0 = Domain.Real;
                if (tok == Sqlx.DOUBLE)
                    Mustbe(Sqlx.PRECISION);
                Next();
                r = ParsePrecPart(r);
            }
            else if (Match(Sqlx.INT, Sqlx.INTEGER, Sqlx.BIGINT, Sqlx.SMALLINT))
            {
                r = r0 = Domain.Int;
                Next();
                r = ParsePrecPart(r);
            }
            else if (Match(Sqlx.BINARY))
            {
                Next();
                Mustbe(Sqlx.LARGE);
                Mustbe(Sqlx.OBJECT);
                r = r0 = Domain.Blob;
            }
            else if (Match(Sqlx.BOOLEAN))
            {
                r = r0 = Domain.Bool;
                Next();
            }
            else if (Match(Sqlx.CLOB, Sqlx.NCLOB))
            {
                r = r0 = Domain.Char;
                Next();
            }
            else if (Match(Sqlx.BLOB, Sqlx.XML))
            {
                r = r0 = Domain.Blob;
                Next();
            }
            else if (Match(Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL))
            {
                Domain dr = r0 = Domain.Timestamp;
                switch (tok)
                {
                    case Sqlx.DATE: dr = Domain.Date; break;
                    case Sqlx.TIME: dr = Domain.Timespan; break;
                    case Sqlx.TIMESTAMP: dr = Domain.Timestamp; break;
                    case Sqlx.INTERVAL: dr = Domain.Interval; break;
                }
                Next();
                if (Match(Sqlx.YEAR, Sqlx.DAY, Sqlx.MONTH, Sqlx.HOUR, Sqlx.MINUTE, Sqlx.SECOND))
                    dr = ParseIntervalType();
                r = dr;
            }
            else if (Match(Sqlx.PASSWORD))
            {
                r = r0 = Domain.Password;
                Next();
            }
            else if (Match(Sqlx.POSITION))
            {
                r = r0 = Domain.Position;
                Next();
            }
            else if (Match(Sqlx.DOCUMENT))
            {
                r = r0 = Domain.Document;
                Next();
            }
            else if (Match(Sqlx.DOCARRAY))
            {
                r = r0 = Domain.DocArray;
                Next();
            }
            else if (Match(Sqlx.CHECK))
            {
                r = r0 = Domain._Rvv;
                Next();
            }
            else if (Match(Sqlx.OBJECT))
            {
                r = r0 = Domain.ObjectId;
                Next();
            }
            if (r == Domain.Null)
                return Domain.Null; // not a standard type
            if (r == r0)
                return r0; // completely standard
            // see if we know this type
            if (cx.db.objects[cx.db.Find(r)?.defpos??-1L] is Domain nr 
                && r.CompareTo(nr)==0)
                return (Domain)cx.Add(nr);
            if (cx.newTypes.Contains(r) && cx.obs[cx.newTypes[r]??-1L] is Domain ns)
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
			Sqlx start = Mustbe(Sqlx.YEAR,Sqlx.DAY,Sqlx.MONTH,Sqlx.HOUR,Sqlx.MINUTE,Sqlx.SECOND);
            var d = Domain.Interval;
            var m = d.mem+(Domain.Start, start);
			if (tok==Sqlx.LPAREN)
			{
				Next();
				var p1 = lxr.val;
				Mustbe(Sqlx.INTEGERLITERAL);
				m+=(Domain.Scale,p1.ToInt()??0);
				if (start==Sqlx.SECOND && tok==Sqlx.COMMA)
				{
					Next();
					var p2 = lxr.val;
					Mustbe(Sqlx.INTEGERLITERAL);
					m+=(Domain.Precision,p2.ToInt()??0);
				}
				Mustbe(Sqlx.RPAREN);
			}
			if (tok==Sqlx.TO)
			{
				Next();
				Sqlx end = Mustbe(Sqlx.YEAR,Sqlx.DAY,Sqlx.MONTH,Sqlx.HOUR,Sqlx.MINUTE,Sqlx.SECOND);
                m += (Domain.End, end);
				if (end==Sqlx.SECOND && tok==Sqlx.LPAREN)
				{
					Next();
					var p2 = lxr.val;
					Mustbe(Sqlx.INTEGERLITERAL);
					m+=(Domain.Precision,p2.ToInt()??0);
					Mustbe(Sqlx.RPAREN);
				}
			}
            return (Domain)d.New(m);
		}
        /// <summary>
        /// Handle ROW type or TABLE type in Type specification.
        /// </summary>
        /// <returns>The RowTypeSpec</returns>
        internal Domain ParseRowTypeSpec(Sqlx k, Ident? pn = null, Domain? under = null)
        {
            if (under is not null)
                k = under.kind;
            var dt = Domain.Null;
            if (tok == Sqlx.ID)
            {
                var id = new Ident(this);
                Next();
                if (cx.GetObject(id.ident) is not Domain ob)
                    throw new DBException("42107", id.ident).Mix();
                return ob;
            }
            var lp = LexPos();
            var ns = BList<(Ident, Domain, CTree<Sqlx, TypedValue>)>.Empty;
            // sm is also used for the RestView case
            var sm = (under as UDType)?.HierarchyCols(cx) ?? BTree<string, (int, long?)>.Empty;
            var sl = lxr.start;
            Mustbe(Sqlx.LPAREN);
            for (var n = 0; ; n++)
            {
                var mi = ParseMember(pn);
                if (sm.Contains(mi.Item1.ident))
                    throw new DBException("42104", mi.Item1.ident);
                ns += mi;
                if (tok != Sqlx.COMMA)
                    break;
                Next();
            }
            Mustbe(Sqlx.RPAREN);
            var ic = new Ident(new string(lxr.input, sl, lxr.start - sl), lp);
            var m = new BTree<long, object>(DBObject.Definer, cx.role.defpos);
            var oi = new ObInfo(ic.ident, Grant.AllPrivileges);
            m += (DBObject.Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            var st = cx.db.nextPos;
            var nst = cx.db.nextStmt;
            string tn = (pn is not null) ? pn.ident : ic.ident;
            if (k == Sqlx.VIEW)
                dt = (Domain)cx.Add(new Domain(st, cx, Sqlx.VIEW, BList<DBObject>.Empty));
            else if (k == Sqlx.TABLE)
                dt = (Domain)cx.Add(new Table(lp.dp, m));
            else if (pn is not null)
            {
                dt = k switch
                {
                    Sqlx.NODETYPE => Domain.NodeType,
                    Sqlx.EDGETYPE => Domain.EdgeType,
                    _ => Domain.TypeSpec,
                };
                dt = ((UDType)dt).New(pn, under, st, cx) ?? throw new PEException("PE40407");
            }
            if (k != Sqlx.ROW)
                pn ??= new Ident("", lp);
            var ms = CTree<long, Domain>.Empty;
            var rt = BList<long?>.Empty;
            var ls = CTree<long, string>.Empty;
            var j = 0;
            for (var b = ns.First(); b != null; b = b.Next(), j++)
            {
                var (nm, dm, _) = b.value();
                if ((k == Sqlx.TYPE || k==Sqlx.NODETYPE || k==Sqlx.EDGETYPE) && pn != null)
                {
                    var np = cx.db.nextPos;
                    var pc = new PColumn3((Table)dt, nm.ident, dm.Length, dm,
                        "", dm.defaultValue, "", CTree<UpdateAssignment, bool>.Empty,
                        false, GenerationRule.None, np, cx);
                    cx.Add(pc);
                    ms += (pc.defpos, dm);
                    sm += (nm.ident, (j, pc.defpos));
                    rt += pc.defpos;
                    var cix = cx.Ix(pc.defpos);
                    cx.defs += (new Ident(pn, nm), cix);
                }
                else if (pn!=null && pn.ident!="")
                {
                    var se = new SqlElement(nm, cx, pn, dm);
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
            dt = cx.obs[dt.defpos] as Domain??throw new DBException("42105");
            oi += (ObInfo.Names, sm);
            var r = (Domain)dt.New(st, BTree<long, object>.Empty
                + (ObInfo.Name, tn) + (DBObject.Definer,cx.role.defpos)
                + (DBObject.Infos,new BTree<long,ObInfo>(cx.role.defpos,oi))
                + (Domain.Representation, ms) + (Domain.RowType, rt));
            if (under is not null)
            {
                for (var b = (under as Table)?.indexes.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx.db.objects[c.key()] is Level3.Index x && dt is Table t)
                            r = (Domain)(cx.Add(new PIndex(t.name+"."+x.name, t, b.key(), x.flags, 
                                x.refindexdefpos, cx.db.nextPos))??dt);
                r += (Domain.Constraints, under.constraints);
                r += (Domain.Under, under);
            }
            if (pn == null || pn.ident=="") // RestView
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
		(Ident,Domain,CTree<Sqlx,TypedValue>) ParseMember(Ident? pn)
		{
            Ident? n = null;
            if (tok == Sqlx.ID)
            {
                n = new Ident(this);
                Next();
            }
            var dm = ParseSqlDataType(pn); // dm is domain of the member
            if (tok == Sqlx.ID && n == null)
                throw new DBException("42000",dm);
			if (tok==Sqlx.DEFAULT)
			{
				int st = lxr.start;
				var dv = ParseSqlValue(dm);
                var ds = new string(lxr.input, st, lxr.start - st);
				dm = dm + (Domain.Default,dv) + (Domain.DefaultString,ds);
			}
            if (tok == Sqlx.COLLATE)
                dm+= (Domain.Culture,ParseCollate());
            var md = CTree<Sqlx, TypedValue>.Empty;
            if (StartMetadata(Sqlx.COLUMN))
                md = ParseMetadata(Sqlx.COLUMN);
            if (n == null || dm == null || md == null)
                throw new DBException("42000");
            return (n,dm,md);
		}
        /// <summary>
        /// Parse a precision
        /// </summary>
        /// <param name="r">The SqldataType</param>
        /// <returns>the updated obs type</returns>
		Domain ParsePrecPart(Domain r)
		{
			if (tok==Sqlx.LPAREN)
			{
				Next();
                if (lxr.val is TInt it)
                {
                    int prec = (int)it.value;
                    r += (Domain.Precision, prec);
                }
                Mustbe(Sqlx.INTEGERLITERAL);
				Mustbe(Sqlx.RPAREN);
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
			if (tok==Sqlx.LPAREN)
			{
				Next();
                if (lxr.val is TInt it && it is TInt i)
                {
                    int prec = (int)i.value;
                    r += (Domain.Precision, prec);
                }
				Mustbe(Sqlx.INTEGERLITERAL);
				if (tok==Sqlx.COMMA)
				{
					Next();
                    if (lxr.val is TInt jt && jt is TInt j)
                    {
                        int scale = (int)j.value;
                        r+=(Domain.Scale,scale);
                    }
					Mustbe(Sqlx.INTEGERLITERAL);
				}
				Mustbe(Sqlx.RPAREN);
			}
            return r;
		}
        /// <summary>
        /// Rename =SET ObjectName TO id .
        /// </summary>
        /// <returns>the executable</returns>
		void ParseSqlSet()
        {
            Next();
            if (Match(Sqlx.AUTHORIZATION))
            {
                Next();
                Mustbe(Sqlx.EQL);
                Mustbe(Sqlx.CURATED);
                if (cx.db is not Transaction) throw new DBException("2F003");
                if (cx.db.user == null || cx.db.user.defpos != cx.db.owner)
                    throw new DBException("42105").Mix();
                if (cx.parse == ExecuteStatus.Obey)
                {
                    var pc = new Curated(cx.db.nextPos);
                    cx.Add(pc);
                }
            }
            else if (Match(Sqlx.PROFILING))
            {
                Next();
                Mustbe(Sqlx.EQL);
                if (cx.db.user == null || cx.db.user.defpos != cx.db.owner)
                    throw new DBException("42105").Mix();
                Mustbe(Sqlx.BOOLEANLITERAL);
                // ignore for now
            }
            else if (Match(Sqlx.TIMEOUT))
            {
                Next();
                Mustbe(Sqlx.EQL);
                if (cx.db.user == null || cx.db.user.defpos != cx.db.owner)
                    throw new DBException("42105").Mix();
                Mustbe(Sqlx.INTEGERLITERAL);
                // ignore for now
            }
            else
            {
                // Rename
                Ident? n;
                Match(Sqlx.DOMAIN, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE);
                MethodModes();
                DBObject? ob;
                if (Match(Sqlx.TABLE, Sqlx.DOMAIN, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE))
                {
                    Next();
                    n = new Ident(this);
                    Mustbe(Sqlx.ID);
                    ob = cx._Ob(cx.role.dbobjects[n.ident] ?? -1L)
                        ?? cx._Ob(cx.db.roles[n.ident] ?? -1L)
                        ?? throw new DBException("42107", n.ident);
                    var oi = ob.infos[cx.db.role.defpos] ?? ob.infos[ob.definer]
                        ?? throw new DBException("42105");
                    ob += (DBObject.Infos, oi + (ObInfo.Name, n.ident));
                    cx.Add(ob);
                }
                else
                {
                    bool meth = false;
                    PMethod.MethodType mt = PMethod.MethodType.Instance;
                    if (Match(Sqlx.OVERRIDING, Sqlx.STATIC, Sqlx.INSTANCE, Sqlx.CONSTRUCTOR))
                    {
                        switch (tok)
                        {
                            case Sqlx.OVERRIDING: mt = PMethod.MethodType.Overriding; break;
                            case Sqlx.STATIC: mt = PMethod.MethodType.Static; break;
                            case Sqlx.CONSTRUCTOR: mt = PMethod.MethodType.Constructor; break;
                        }
                        Next();
                        Mustbe(Sqlx.METHOD);
                        meth = true;
                    }
                    else if (tok == Sqlx.METHOD)
                        meth = true;
                    else if (!Match(Sqlx.PROCEDURE, Sqlx.FUNCTION))
                        throw new DBException("42126").Mix();
                    Next();
                    n = new Ident(this);
                    var nid = n.ident;
                    Mustbe(Sqlx.ID);
                    var a = CList<Domain>.Empty;
                    if (tok == Sqlx.LPAREN)
                    {
                        Next();
                        a += ParseSqlDataType();
                        while (tok == Sqlx.COMMA)
                        {
                            Next();
                            a += ParseSqlDataType();
                        }
                        Mustbe(Sqlx.RPAREN);
                    }
                    if (meth)
                    {
                        Ident? type = null;
                        if (mt == PMethod.MethodType.Constructor)
                            type = new Ident(nid, cx.Ix(0));
                        if (tok == Sqlx.FOR)
                        {
                            Next();
                            type = new Ident(this);
                            Mustbe(Sqlx.ID);
                        }
                        if (type == null)
                            throw new DBException("42134").Mix();
                        if (cx.role is not Role ro ||
                            cx.GetObject(type.ident) is not DBObject ot ||
                            ot.infos[ro.defpos] is not ObInfo oi)
                            throw new DBException("42105"); ;
                        ob = (Method?)cx.db.objects[oi.methodInfos[n.ident]?[a] ?? -1L];
                    }
                    else
                        ob = cx.GetProcedure(LexPos().dp, n.ident, a);
                    if (ob == null)
                        throw new DBException("42135", n.ident).Mix();
                    Mustbe(Sqlx.TO);
                    var nm = new Ident(this);
                    Mustbe(Sqlx.ID);
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
		internal SelectStatement ParseCursorSpecification(Domain xp, bool ambient=false)
        {
            RowSet un = _ParseCursorSpecification(xp,ambient);
            var s = new SelectStatement(cx.GetUid(), un);
            cx.exec = s;
            return (SelectStatement)cx.Add(s);
        }
        internal RowSet _ParseCursorSpecification(Domain xp,bool ambient=false)
        {
            if (!ambient)
                cx.IncSD(new Ident(this));
            ParseXmlOption(false);
            RowSet qe;
            qe = ParseQueryExpression(xp,ambient);
            cx.result = qe.defpos;
            cx.Add(qe);
            if (!ambient)
               cx.DecSD();
            return qe;
        }
        /// <summary>
        /// Start the parse for a QueryExpression (called from View)
        /// </summary>
        /// <param name="sql">The sql string</param>
        /// <param name="xp">The expected result type</param>
        /// <returns>a RowSet</returns>
		public RowSet ParseQueryExpression(Ident sql,Domain xp)
		{
			lxr = new Lexer(cx,sql);
			tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
			return ParseQueryExpression(xp);
		}
        /// <summary>
        /// QueryExpression = QueryExpressionBody [OrderByClause] [FetchFirstClause] .
		/// QueryExpressionBody = QueryTerm 
		/// | QueryExpressionBody ( UNION | EXCEPT ) [ ALL | DISTINCT ] QueryTerm .
		/// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification 
        /// in which the RowSetExpr is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="xp">the expected result type</param>
        /// <returns>Updated result type, and a RowSet</returns>
		RowSet ParseQueryExpression(Domain xp,bool ambient=false)
        {
            RowSet left,right;
            left = ParseQueryTerm(xp,ambient);
            while (Match(Sqlx.UNION, Sqlx.EXCEPT))
            {
                Sqlx op = tok;
                Next();
                Sqlx md = Sqlx.DISTINCT;
                if (Match(Sqlx.ALL, Sqlx.DISTINCT))
                {
                    md = tok;
                    Next();
                }
                right = ParseQueryTerm(xp,ambient);
                left = new MergeRowSet(cx.GetUid(), cx, xp,left, right,md==Sqlx.DISTINCT,op);
                if (md == Sqlx.DISTINCT)
                    left += (RowSet.Distinct, true);
            }
            var ois = left.ordSpec;
            var nis = ParseOrderClause(ois, true);
            left = (RowSet)(cx.obs[left.defpos]??throw new PEException("PE20701"));
            if (ois.CompareTo(nis)!=0)
                left = left.Sort(cx, nis, false);
            if (Match(Sqlx.FETCH))
            {
                Next();
                Mustbe(Sqlx.FIRST);
                var o = lxr.val;
                var n = 1;
                if (tok == Sqlx.INTEGERLITERAL)
                {
                    n = o.ToInt()??1;
                    Next();
                    Mustbe(Sqlx.ROWS);
                }
                else
                    Mustbe(Sqlx.ROW);
                left = new RowSetSection(cx, left, 0, n);
                Mustbe(Sqlx.ONLY);
            }
            return (RowSet)cx.Add(left);
        }
        /// <summary>
		/// QueryTerm = QueryPrimary | QueryTerm INTERSECT [ ALL | DISTINCT ] QueryPrimary .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification 
        /// in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the RowSet</returns>
		RowSet ParseQueryTerm(Domain xp,bool ambient = false)
		{
            RowSet left,right;
            left = ParseQueryPrimary(xp,ambient);
			while (Match(Sqlx.INTERSECT))
			{
                var lp = LexPos();
				Next();
				Sqlx m = Sqlx.DISTINCT;
				if (Match(Sqlx.ALL,Sqlx.DISTINCT))
				{
					m = tok;
					Next();
				}
                right = ParseQueryPrimary(xp,ambient);
				left = new MergeRowSet(lp.dp, cx, xp, left,right,m==Sqlx.DISTINCT,Sqlx.INTERSECT);
                if (m == Sqlx.DISTINCT)
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
                case Sqlx.LPAREN:
                    Next();
                    qs = ParseQueryExpression(xp,ambient);
                    Mustbe(Sqlx.RPAREN);
                    break;
                case Sqlx.RETURN:
                case Sqlx.SELECT: // query specification
                     qs = ParseQuerySpecification(xp,ambient);
                     break;
                case Sqlx.MATCH:
                     ParseSqlMatchStatement();
                     qs = (RowSet)(cx.obs[cx.result]??new TrivialRowSet(cx,Domain.Row));
                     break;
                case Sqlx.VALUES:
                    var v = BList<long?>.Empty;
                    Sqlx sep = Sqlx.COMMA;
                    while (sep == Sqlx.COMMA)
                    {
                        Next();
                        var llp = LexPos();
                        Mustbe(Sqlx.LPAREN);
                        var x = ParseSqlValueList(xp);
                        Mustbe(Sqlx.RPAREN);
                        v += cx.Add(new SqlRow(llp.dp, cx, xp, x)).defpos;
                        sep = tok;
                    }
                    qs = (RowSet)cx.Add(new SqlRowSet(lp.dp, cx, xp, v));
                    break;
                case Sqlx.TABLE:
                    Next();
                    Ident ic = new(this);
                    Mustbe(Sqlx.ID);
                    var tb = cx.GetObject(ic.ident) as Table ??
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
		Domain ParseOrderClause(Domain ord,bool wfok)
		{
			if (tok!=Sqlx.ORDER)
				return ord;
            cx.IncSD(new Ident(this)); // order by columns will be in the foregoing cursor spec
			Next();
			Mustbe(Sqlx.BY);
            var bs = BList<DBObject>.Empty;
            for (var b = ord.rowType.First(); b != null; b = b.Next())
                bs += cx._Ob(b.value() ?? -1L) ?? SqlNull.Value;
			bs+=cx._Ob(ParseOrderItem(wfok))??SqlNull.Value;
			while (tok==Sqlx.COMMA)
			{
				Next();
                bs += cx._Ob(ParseOrderItem(wfok)) ?? SqlNull.Value;
			}
            cx.DecSD();
            return new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length);
		}
        /// <summary>
        /// This version is for WindowSpecifications
        /// </summary>
        /// <param name="ord"></param>
        /// <returns></returns>
        Domain ParseOrderClause(Domain ord)
        {
            if (tok != Sqlx.ORDER)
                return ord;
            Next();
            Mustbe(Sqlx.BY);
            var bs = BList<DBObject>.Empty;
            for (var b = ord.rowType.First(); b != null; b = b.Next())
                bs += cx._Ob(b.value() ?? -1L) ?? SqlNull.Value;
            bs += cx._Ob(ParseOrderItem(false)) ?? SqlNull.Value;
            while (tok == Sqlx.COMMA)
            {
                Next();
                bs += cx._Ob(ParseOrderItem(false)) ?? SqlNull.Value;
            }
            return new Domain(cx.GetUid(), cx, Sqlx.ROW, bs, bs.Length);
        }
        /// <summary>
		/// BList<long?> =  TypedValue [ ASC | DESC ] [ NULLS ( FIRST | LAST )] .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>an OrderItem</returns>
		long ParseOrderItem(bool wfok)
		{
            var v = ParseSqlValue(Domain.Content,wfok);
            var dt = v.domain;
            var a = Sqlx.ASC;
            var n = Sqlx.NULL;
            if (Match(Sqlx.ASC))
				Next();
			else if (Match(Sqlx.DESC))
			{
				a = Sqlx.DESC;
				Next();
			}
			if (Match(Sqlx.NULLS))
			{
				Next();
				if (Match(Sqlx.FIRST))
					Next();
				else if (tok==Sqlx.LAST)
				{
					n = Sqlx.LAST;
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
            if (!ambient)
                cx.IncSD(id);
            Mustbe(Sqlx.SELECT,Sqlx.RETURN);
            var d = ParseDistinctClause();
            var dm = ParseSelectList(id.iix.dp, xp);
            cx.Add(dm);
            var te = ParseTableExpression(id.iix, dm);
            if (Match(Sqlx.FOR))
            {
                Next();
                Mustbe(Sqlx.UPDATE);
            }
            if (!ambient)
                cx.DecSD(dm,te);
            te = (RowSet?)cx.obs[te.defpos]??throw new PEException("PE1967");
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
			if (tok==Sqlx.DISTINCT)
			{
				Next();
				r = true;
			} 
			else if (tok==Sqlx.ALL)
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
            SqlValue v;
            var j = 0;
            var vs = BList<DBObject>.Empty;
            v = ParseSelectItem(dp, xp, j++);
            if (v is not null) // star items do not have a value to add at this stage
                vs += v;
            while (tok == Sqlx.COMMA)
            {
                Next();
                v = ParseSelectItem(dp, xp, j++);
                if (v is not null)
                    vs += v;
            }
            return (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs, vs.Length));
        }
        SqlValue ParseSelectItem(long q,Domain xp,int pos)
        {
            Domain dm = Domain.Content;
            if (xp.rowType.Length>pos)
                dm = xp.representation[xp[pos]??-1L]??throw new PEException("PE1675");
            return ParseSelectItem(q,dm);
        }
        /// <summary>
		/// SelectItem = * | (Scalar [AS id ]) | (RowValue [.*] [AS IdList]) .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <param name="t">the expected obs type for the query</param>
        /// <param name="pos">The position in the SelectList</param>
        SqlValue ParseSelectItem(long q,Domain xp)
        {
            Ident alias;
            SqlValue v;
            if (tok == Sqlx.TIMES)
            {
                var lp = LexPos();
                Next();
                v = new SqlStar(lp.dp, cx, -1L);
            }
            else
            {
                v = ParseSqlValue(xp, true);
                if (q>=0)
                    v = (SqlValue)v.AddFrom(cx, q);
            }
            if (tok == Sqlx.AS)
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
                    && cx.obs[ob.dp] is SqlValue ov)
                {
                    var v0 = nv;
                    nv = (SqlValue)nv.Relocate(ov.defpos);
                    cx.Replace(v0, nv);
                }
                else
                    cx.Add(nv);
                cx.defs += (alias, new Iix(v.defpos, cx, v.defpos));
                cx.Add(nv);
                Mustbe(Sqlx.ID);
                v = nv;
            }
            else
                cx.Add(v);
            if (v.domain.kind==Sqlx.TABLE)
            {
                // we want a scalar from this
                var dm = cx.obs[v.domain[0]??-1L] as Domain??Domain.Content;
                cx.Add(v + (DBObject._Domain,dm));
            }
            return v;
        }
        /// <summary>
		/// TableExpression = FromClause [ WhereClause ] [ GroupByClause ] [ HavingClause ] [WindowClause] .
        /// The ParseFromClause is called before this
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="t">the expected obs type</param>
        /// <returns>The TableExpression</returns>
		RowSet ParseTableExpression(Iix lp, Domain d)
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
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                {
                    (var ls, m) = sv.Resolve(cx, lp.dp, m);
                    vs += ls;
                }
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var ob = cx.obs[k];
                if (ob is SqlValue sv)
                    sv.Resolve(cx, k, BTree<long, object>.Empty);
                else if (ob?.id is Ident ic && ob is ForwardReference fr
                    && cx.defs[ic.ident] is BTree<int, (Iix, Ident.Idents)> tt
                    && tt.Contains(cx.sD))
                {
                    var (iix, _) = tt[cx.sD];
                    if (cx.obs[iix.dp] is RowSet nb)
                    {
                        for (var c = fr.subs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlValue su && su.name is not null
                                && nb.names[su.name].Item2 is long sp
                                && cx.obs[sp] is SqlValue ru)
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
                if (b.value() is long p && cx.obs[p] is SqlValue v && !fm.representation.Contains(p))
                    vs += v;
            dm = new Domain(dm.defpos, cx, Sqlx.TABLE, vs, ds);
            cx.Add(dm);
            fm = (RowSet)(cx.obs[fm.defpos] ?? throw new PEException("PE2001"));
            return ParseSelectRowSet(new SelectRowSet(lp, cx, dm, fm, m));
        }
        RowSet ParseSelectRowSet(SelectRowSet r)
        { 
            var m = BTree<long, object>.Empty;
            if (r.aggs != CTree<long, bool>.Empty) 
                m += (Domain.Aggs, r.aggs);
            if (tok == Sqlx.WHERE)
            {
                var wc = ParseWhereClause() ?? throw new DBException("42161", "condition");
                var wh = new BTree<long,object>(RowSet._Where, wc);
                m += wh;
                ((RowSet)(cx.obs[r.source]??throw new PEException("PE2002"))).Apply(wh,cx);
            }
            if (tok == Sqlx.GROUP)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "GROUP");
                m += (RowSet.Group, ParseGroupClause()?.defpos ?? -1L);
            }
            if (tok == Sqlx.HAVING)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "HAVING");
                m += (RowSet.Having, ParseHavingClause(r));
            }
            if (tok == Sqlx.WINDOW)
            {
                if (r.aggs == CTree<long, bool>.Empty)
                    throw new DBException("42128", "WINDOW");
                m += (RowSet.Windows, ParseWindowClause());
            }
            r = (SelectRowSet)(cx.obs[r.defpos]??throw new PEException("PE20100"));
            r = (SelectRowSet) r.Apply(m, cx);
            if (r.aggs.Count > 0)
            {
                var vw = true;
                for (var b = r.aggs.First(); b != null; b = b.Next())
                    if ((b.key() >= Transaction.TransPos && b.key() < Transaction.Executables)
                        || b.key() >= Transaction.HeapStart)
                        vw = false;
                if (!vw)
                {
                    // check for agged or grouped
                    var os = CTree<long, bool>.Empty;
                    for (var b = r.rowType.First(); b != null && b.key() < r.display; b = b.Next())
                        if (b.value() is long p && cx.obs[p] is SqlValue x)
                            os += x.Operands(cx);
                    for (var b = r.having.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()] is SqlValue x)
                        os += x.Operands(cx);
                    for (var b = os.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()] is SqlValue v && !v.AggedOrGrouped(cx, r))
                            throw new DBException("42170", v.alias ?? v.name??"??");
                }
            }
            return r;
        }
        /// <summary>
		/// FromClause = 	FROM TableReference { ',' TableReference } .
        /// (before WHERE, GROUP, etc).
        /// </summary>
        /// <param name="dp">The position for the selectrowset being constructed</param>
        /// <param name="dm">the selectlist </param>
        /// <returns>The resolved select domain and table expression</returns>
		RowSet ParseFromClause(long dp,Domain dm)
		{
            if (tok == Sqlx.FROM)
            {
                Next();
                return (RowSet)cx.Add(ParseTableReference(dp,dm));
            }
            else
                return new TrivialRowSet(cx,dm);
		}
        /// <summary>
		/// TableReference = TableFactor Alias | JoinedTable .
        /// </summary>
        /// <param name="st">the future selectrowset defining position</param>
        /// <returns>and the new table reference item</returns>
        RowSet ParseTableReference(long st,Domain dm)
        {
            RowSet a;
            a = ParseTableReferenceItem(st,dm);
            cx.Add(a);
            while (Match(Sqlx.COMMA, Sqlx.CROSS, Sqlx.NATURAL, Sqlx.JOIN, Sqlx.INNER, Sqlx.LEFT, Sqlx.RIGHT, Sqlx.FULL))
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
		RowSet ParseTableReferenceItem(long st,Domain dm)
        {
            RowSet rf;
            var lp = new Iix(st,cx,LexPos().dp);
            if (tok == Sqlx.ROWS) // Pyrrho specific
            {
                Next();
                Mustbe(Sqlx.LPAREN);
                var v = ParseSqlValue(Domain.Position);
                SqlValue w = SqlNull.Value;
                if (tok == Sqlx.COMMA)
                {
                    Next();
                    w = ParseSqlValue(Domain.Position);
                }
                Mustbe(Sqlx.RPAREN);
                if (tok == Sqlx.ID || tok == Sqlx.AS)
                {
                    if (tok == Sqlx.AS)
                        Next();
                    new Ident(this);
                    Mustbe(Sqlx.ID);
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
            else if (tok == Sqlx.UNNEST)
            {
                Next();
                Mustbe(Sqlx.LPAREN);
                SqlValue sv = ParseSqlValue(Domain.Content);
                cx.Add(sv);
                if (sv.domain.elType?.kind != Sqlx.ROW)
                    throw new DBException("42161", sv);
                if (sv.domain.kind == Sqlx.ARRAY)
                    rf = new ArrayRowSet(cx.GetUid(), cx, sv);
                else if (sv.domain.kind == Sqlx.SET)
                    rf = new SetRowSet(cx.GetUid(), cx, sv);
                else if (sv.domain.kind == Sqlx.MULTISET)
                    rf = new MultisetRowSet(cx.GetUid(), cx, sv);
                else throw new DBException("42161", sv);
                Mustbe(Sqlx.RPAREN);
            }
            else if (tok == Sqlx.TABLE)
            {
                Next();
                var cp = LexPos();
                Mustbe(Sqlx.LPAREN); // SQL2003-2 7.6 required before table valued function
                Ident n = new(this);
                Mustbe(Sqlx.ID);
                var r = BList<long?>.Empty;
                Mustbe(Sqlx.LPAREN);
                if (tok != Sqlx.RPAREN)
                    for (; ; )
                    {
                        r += cx.Add(ParseSqlValue(Domain.Content)).defpos;
                        if (tok == Sqlx.RPAREN)
                            break;
                        Mustbe(Sqlx.COMMA);
                    }
                Next();
                Mustbe(Sqlx.RPAREN); // another: see above
                var proc = cx.GetProcedure(LexPos().dp, n.ident, cx.Signature(r))
                    ?? throw new DBException("42108", n.ident);
                ParseCorrelation(proc.domain);
                var ca = new SqlProcedureCall(cp.dp, cx, proc, r);
                cx.Add(ca);
                rf = ca.RowSets(n, cx, proc.domain, n.iix.dp);
            }
            else if (tok == Sqlx.LPAREN) // subquery
            {
                Next();
                cx.IncSD(new Ident("",LexPos()));
                rf = ParseQueryExpression(Domain.TableType);
                cx.DecSD();
                Mustbe(Sqlx.RPAREN);
                if (tok == Sqlx.ID)
                {
                    var a = lxr.val.ToString();
                    var rx = cx.Ix(rf.defpos);
                    var ia = new Ident(a, rx);
                    for (var b = cx.defs[a]?.Last()?.value().Item2.First(); b != null; b = b.Next())
                        if (cx.obs[b.value()?.Last()?.value().Item1.dp ?? -1L] is SqlValue lv
                            && (lv.domain.kind==Sqlx.CONTENT || lv.GetType().Name == "SqlValue")
                            && lv.name != null
                            && rf.names.Contains(lv.name) && cx.obs[rf.names[lv.name].Item2??-1L] is SqlValue uv)
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
            else if (tok == Sqlx.STATIC)
            {
                Next();
                rf = new TrivialRowSet(cx,dm);
            }
            else if (tok == Sqlx.LBRACK)
                rf = new TrivialRowSet(cx,dm) + (cx,RowSet.Target, ParseSqlDocArray().defpos);
            else // ordinary table, view, OLD/NEW TABLE id, or parameter
            {
                Ident ic = new(this);
                Mustbe(Sqlx.ID);
                string? a = null;
                if (tok == Sqlx.ID || tok == Sqlx.AS)
                {
                    if (tok == Sqlx.AS)
                        Next();
                    a = lxr.val.ToString();
                    Mustbe(Sqlx.ID);
                }
                var ob = (cx.GetObject(ic.ident) ?? cx.obs[cx.defs[ic].dp]) ??
                    throw new DBException("42107", ic.ToString());
                if (ob is SqlValue o && (ob.domain.kind != Sqlx.TABLE || o.from < 0))
                    throw new DBException("42000");
                if (ob is RowSet f)
                {
                    rf = f;
                    ob = cx.obs[f.target] as Table;
                }
                else
                    rf = _From(ic, ob, dm, Grant.Privilege.Select, a);
                if (Match(Sqlx.FOR))
                {
                    var ps = ParsePeriodSpec();
                    var tb = ob as Table ?? throw new DBException("42000");
                    rf += (cx,RowSet.Periods, rf.periods + (tb.defpos, ps));
                    long pp = (ps.periodname == "SYSTEM_TIME") ? tb.systemPS : tb.applicationPS;
                    if (pp < 0)
                        throw new DBException("42162", ps.periodname).Mix();
                    rf += (cx,RowSet.Periods, rf.periods + (tb.defpos, ps));
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
            var un = CTree<long, bool>.Empty;
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                if (b.key() is long k && cx.obs[k] is DBObject uo)
                {
                    var (ix, ids) = cx.defs[(uo.name, cx.sD)];
                    for (var c = ids.First(); c != null; c = c.Next())
                        for (var d = c.value().First(); d != null; d = d.Next())
                            un += (d.value().Item1.dp, true);
                    if (uo is SqlValue sv && sv.GetType().Name=="SqlValue" &&
                        (b.value() == cx.sD - 1 || b.value() == cx.sD) &&
                        cx.obs[ix.dp] is SqlValue ts  
                        && !un.Contains(k))
                    {
                        cx.undefined -= k;
                        var nv = (SqlValue)cx.Add(ts.Relocate(k));
                        if (sv.alias is not null)
                            nv += (DBObject._Alias, sv.alias);
                        cx.Add(nv);
                        cx.Replace(ts, nv); // looks like it should be sv, but this is correct
                        cx.NowTry();
                    }
                    if (uo is SqlMethodCall um && um.procdefpos < 0
                        && cx.obs[um.var] is SqlValue su && su.domain is UDType ut
                        && ut.infos[cx.role.defpos] is ObInfo ui && um.name != null
                        && ui.methodInfos[um.name] is BTree<CList<Domain>, long?> st)
                    {
                        var dl = CList<Domain>.Empty;
                        for (var c = um.parms.First(); c != null; c = c.Next())
                            if (c.value() is long q && cx.obs[q] is SqlValue av)
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
            return (RowSet)(cx.obs[ff.defpos]??throw new PEException("PE20720"));
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
            Sqlx kn;
            SqlValue? t1 = null, t2 = null;
            Next();
            if (tok == Sqlx.ID)
                pn = lxr.val.ToString();
            Mustbe(Sqlx.SYSTEM_TIME,Sqlx.ID);
            kn = tok;
            switch (tok)
            {
                case Sqlx.AS: Next();
                    Mustbe(Sqlx.OF);
                    t1 = ParseSqlValue(Domain.UnionDate);
                    break;
                case Sqlx.BETWEEN: Next();
                    kn = Sqlx.ASYMMETRIC;
                    if (Match(Sqlx.ASYMMETRIC))
                        Next();
                    else if (Match(Sqlx.SYMMETRIC))
                    {
                        Next();
                        kn = Sqlx.SYMMETRIC;
                    }
                    t1 = ParseSqlValueTerm(Domain.UnionDate, false);
                    Mustbe(Sqlx.AND);
                    t2 = ParseSqlValue(Domain.UnionDate);
                    break;
                case Sqlx.FROM: Next();
                    t1 = ParseSqlValue(Domain.UnionDate);
                    Mustbe(Sqlx.TO);
                    t2 = ParseSqlValue(Domain.UnionDate);
                    break;
                default:
                    kn  =Sqlx.NO;
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
            if (tok == Sqlx.ID || tok == Sqlx.AS)
			{
				if (tok==Sqlx.AS)
					Next();
                var cs = BList<long?>.Empty;
                var rs = CTree<long, Domain>.Empty;
                var tablealias = new Ident(this);
				Mustbe(Sqlx.ID);
				if (tok==Sqlx.LPAREN)
				{
					Next();
                    var ids = ParseIDList();
                    if (ids.Length != xp.Length)
                        throw new DBException("22000",xp);
                    var ib = ids.First();
                    for (var b = xp.rowType.First(); ib != null && b != null; b = b.Next(), ib = ib.Next())
                        if (b.value() is long oc)
                        {
                            var cp = ib.value().iix.dp;
                            var cd = xp.representation[oc] ?? throw new PEException("PE47169");
                            cs += cp;
                            rs += (cp, cd);
                        }
                    xp = new Domain(cx.GetUid(),cx, Sqlx.TABLE, rs, cs);
                    cx.Add(xp);
                    return new ObInfo(tablealias.ident, Grant.Privilege.Execute);
				} else
                    return new ObInfo(tablealias.ident, Grant.Privilege.Execute);
			}
            return null;
		}
        /// <summary>
		/// JoinType = 	INNER | ( LEFT | RIGHT | FULL ) [OUTER] .
        /// </summary>
        /// <param name="v">The JoinPart being parsed</param>
		Sqlx ParseJoinType()
		{
            Sqlx r = Sqlx.INNER;
			if (tok==Sqlx.INNER)
				Next();
			else if (tok==Sqlx.LEFT||tok==Sqlx.RIGHT||tok==Sqlx.FULL)
			{
                r = tok;
                Next();
			}
			if (r!=Sqlx.INNER && tok==Sqlx.OUTER)
				Next();
            return r;
		}
        /// <summary>
		/// JoinedTable = 	TableReference CROSS JOIN TableFactor 
		/// 	|	TableReference NATURAL [JoinType] JOIN TableFactor
		/// 	|	TableReference [JoinType] JOIN TableReference ON SqlValue .
        /// </summary>
        /// <param name="q">The eexpected domain q</param>
        /// <param name="fi">The RowSet so far</param>
        /// <returns>the updated query</returns>
        RowSet ParseJoinPart(long dp, RowSet fi,Domain dm)
        {
            var left = fi;
            Sqlx jkind;
            RowSet right;
            var m = BTree<long, object>.Empty;
            if (Match(Sqlx.COMMA))
            {
                jkind = Sqlx.CROSS;
                Next();
                right = ParseTableReferenceItem(dp,dm);
            }
            else if (Match(Sqlx.CROSS))
            {
                jkind = Sqlx.CROSS;
                Next();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(dp,dm);
            }
            else if (Match(Sqlx.NATURAL))
            {
                m += (JoinRowSet.Natural, tok);
                Next();
                jkind = ParseJoinType();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(dp,dm);
            }
            else
            {
                jkind = ParseJoinType();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(dp,dm);
                if (tok == Sqlx.USING)
                {
                    m += (JoinRowSet.Natural, tok);
                    Next();
                    var ns = ParseIDList();
                    var sd = cx.sD;
                    var (_, li) = (left.alias is not null)?cx.defs[(left.alias,sd)] : cx.defs[(left.name,sd)];
                    var (_, ri) = (right.alias is not null)? cx.defs[(right.alias,sd)] : cx.defs[(right.name,sd)];
                    var cs = BTree<long, long?>.Empty;
                    for (var b = ns.First(); b != null; b = b.Next())
                        cs += (ri[b.value()].dp, li[b.value()].dp);
                    m += (JoinRowSet.JoinUsing, cs);
                }
                else
                {
                    Mustbe(Sqlx.ON);
                    var oc = ParseSqlValue(Domain.Bool).Disjoin(cx);
                    var on = BTree<long, long?>.Empty;
                    var wh = CTree<long, bool>.Empty;
                    left = (RowSet)(cx.obs[left.defpos]??throw new PEException("PE2005"));
                    right = (RowSet)(cx.obs[right.defpos]??throw new PEException("PE2006"));
                    var ls = CList<SqlValue>.Empty;
                    var rs = CList<SqlValue>.Empty;
                    var lm = cx.Map(left.rowType);
                    var rm = cx.Map(right.rowType);
                    for (var b = oc.First(); b != null; b = b.Next())
                    { 
                        if (cx.obs[b.key()] is not SqlValueExpr se || se.domain.kind != Sqlx.BOOLEAN)
                            throw new DBException("42151");
                        var lf = se.left;
                        var rg = se.right;
                        if (cx.obs[lf] is SqlValue sl && cx.obs[rg] is SqlValue sr && se.op == Sqlx.EQL)
                        {
                            var rev = !lm.Contains(lf);
                            if (rev)
                            {
                                if ((!rm.Contains(lf))
                                    || (!lm.Contains(rg)))
                                    throw new DBException("42151");
                                oc += (cx.Add(new SqlValueExpr(se.defpos, cx, Sqlx.EQL,
                                    sr, sl, Sqlx.NO)).defpos, true);
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
                    if (oc!=CTree<long,bool>.Empty)
                        m += (JoinRowSet.JoinCond, oc);
                    if (on != BTree<long, long?>.Empty)
                        m += (JoinRowSet.OnCond, on);
                    if (wh != CTree<long,bool>.Empty)
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
        GroupSpecification? ParseGroupClause()
        {
            if (tok != Sqlx.GROUP)
                return null;
            Next();
            var lp = LexPos();
            Mustbe(Sqlx.BY);
            bool d = false;
            if (tok == Sqlx.ALL)
                Next();
            else if (tok == Sqlx.DISTINCT)
            {
                Next();
                d = true;
            }
            bool simple = true;
            GroupSpecification r = new(lp.dp,cx,BTree<long, object>.Empty
                + (GroupSpecification.DistinctGp, d));
            r = ParseGroupingElement(r,ref simple);
            while (tok == Sqlx.COMMA)
            {
                Next();
                r = ParseGroupingElement(r,ref simple);
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
        GroupSpecification ParseGroupingElement(GroupSpecification g, ref bool simple)
        {
             if (Match(Sqlx.ID))
            {
                var cn = ParseIdent();
                var c = cx.Get(cn,Domain.Content)??throw new DBException("42112",cn.ident);
                var ls = new Grouping(cx, BTree<long, object>.Empty + (Grouping.Members,
                    new CTree<long, int>(c.defpos, 0)));
                cx.Add(ls);
                g += (cx,GroupSpecification.Sets,g.sets+ls.defpos);
                simple = true;
                return (GroupSpecification)cx.Add(g);
            }
            simple = false;
            if (Match(Sqlx.LPAREN))
            {
                var lp = LexPos();
                Next();
                if (tok == Sqlx.RPAREN)
                {
                    Next();
                    g += (cx,GroupSpecification.Sets,g.sets+cx.Add(new Grouping(cx)).defpos);
                    return (GroupSpecification)cx.Add(g);
                }
                g +=(cx,GroupSpecification.Sets,g.sets+cx.Add(ParseGroupingSet()).defpos);
                return (GroupSpecification)cx.Add(g);
            }
#if OLAP
            if (Match(Sqlx.GROUPING))
            {
#else
                Mustbe(Sqlx.GROUPING);
#endif
                Next();
                Mustbe(Sqlx.SETS);
                Mustbe(Sqlx.LPAREN);
                g = ParseGroupingElement(g, ref simple);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    g = ParseGroupingElement(g, ref simple);
                }
                Mustbe(Sqlx.RPAREN);
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
            var t = new Grouping(cx,BTree<long, object>.Empty
                +(Grouping.Members,new CTree<long,int>(cn.iix.dp,0)));
            var i = 1;
            while (Match(Sqlx.COMMA))
            {
                cn = ParseIdent();
                t+=(Grouping.Members,t.members+(cn.iix.dp,i++));
            }
            Mustbe(Sqlx.RPAREN);
            return (Grouping)cx.Add(t);
        }
        /// <summary>
		/// HavingClause = HAVING BooleanExpr .
        /// </summary>
        /// <returns>The SqlValue (Boolean expression)</returns>
		CTree<long,bool> ParseHavingClause(Domain dm)
        {
            var r = CTree<long,bool>.Empty;
            if (tok != Sqlx.HAVING)
                return r;
            Next();
            var lp = LexPos();
            r = ParseSqlValueDisjunct(Domain.Bool, false, dm);
            if (tok != Sqlx.OR)
                return r;
            var left = Disjoin(r);
            while (tok == Sqlx.OR)
            {
                Next();
                left = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Sqlx.OR, left, 
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool,false, dm)), Sqlx.NO));
            }
            r +=(left.defpos, true);
      //      lxr.context.cur.Needs(left.alias ?? left.name, RowSet.Need.condition);
            return r;
        }
        /// <summary>
		/// WhereClause = WHERE BooleanExpr .
        /// </summary>
        /// <returns>The SqlValue (Boolean expression)</returns>
		CTree<long,bool>? ParseWhereClause()
		{
            cx.done = ObTree.Empty;
            if (tok != Sqlx.WHERE)
                return null;
			Next();
            var r = ParseSqlValueDisjunct(Domain.Bool, false);
            if (tok != Sqlx.OR)
                return cx.FixTlb(r);
            var left = Disjoin(r);
            while (tok == Sqlx.OR)
            {
                var lp = LexPos();
                Next();
                left = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Sqlx.OR, left, 
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool,false)), Sqlx.NO));
                left = (SqlValue)cx.Add(left);
            }
            r +=(left.defpos, true);
     //       lxr.context.cur.Needs(left.alias ?? left.name,RowSet.Need.condition);
            return cx.FixTlb(r);
		}
        /// <summary>
		/// WindowClause = WINDOW WindowDef { ',' WindowDef } .
        /// </summary>
        /// <returns>the window set as a tree by window names</returns>
        BTree<string,WindowSpecification> ParseWindowClause()
        {
            if (tok != Sqlx.WINDOW)
                throw new DBException("42000");
            Next();
            var tree = BTree<string,WindowSpecification>.Empty; // of WindowSpecification
            ParseWindowDefinition(ref tree);
            while (tok == Sqlx.COMMA)
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
        void ParseWindowDefinition(ref BTree<string,WindowSpecification> tree)
        {
            var id = lxr.val;
            Mustbe(Sqlx.ID);
            Mustbe(Sqlx.AS);
            WindowSpecification r = ParseWindowSpecificationDetails();
            if (r.orderWindow != null)
            {
                if (tree[r.orderWindow] is not WindowSpecification ow)
                    throw new DBException("42135", r.orderWindow).Mix();
                if (ow.order!=Domain.Row && r.order!=Domain.Row)
                    throw new DBException("42000", "7.11 SR10d").ISO();
                if (ow.order!=Domain.Row)
                    throw new DBException("42000", "7.11 SR10c").ISO();
                if (ow.units != Sqlx.NO || ow.low != null || ow.high != null)
                    throw new DBException("42000", "7.11 SR10e").ISO();
            }
            tree+= (id.ToString(), r);
        }
        /// <summary>
        /// An SQL insert statement
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="sql">the sql</param>
        /// <returns>the SqlInsert</returns>
        internal void ParseSqlInsert(string sql)
        {
            lxr = new Lexer(cx,sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            ParseSqlInsert();
        }
        /// <summary>
		/// Insert = INSERT [WITH string][XMLOption] INTO Table_id [ Cols ]  TypedValue [Classification].
        /// </summary>
        /// <returns>the executable</returns>
        SqlInsert ParseSqlInsert()
        {
            bool with = false;
            var lp = LexPos();
            Next();
            if (tok == Sqlx.WITH)
            {
                Next();
                with = true;
            }
            ParseXmlOption(with);
            Mustbe(Sqlx.INTO);
            Ident ic = new(this);
            cx.IncSD(ic);
            var fm = ParseTableReference(ic.iix.dp,Domain.TableType);
            cx.Add(fm);
            if (fm is not TableRowSet && !cx.defs.Contains(ic.ident))
                cx.defs += (ic, ic.iix);
            cx.AddDefs(ic, fm);
            Domain? cs = null;
            // Ambiguous syntax here: (Cols) or (Subquery) or other possibilities
            if (tok == Sqlx.LPAREN)
            {
                if (ParseColsList(fm) is Domain cd)
                {
                    fm = (RowSet)fm.New(cx.GetUid(), fm.mem+ (Domain.Representation,cd.representation)
                        + (Domain.RowType,cd.rowType) + (Domain.Display, cd.Length));
                    cx.Add(fm);
                    cs = cd;
                }
                else
                    tok = lxr.PushBack(Sqlx.LPAREN);
            }
            SqlValue sv;
            cs ??= new Domain(cx.GetUid(), cx, Sqlx.ROW, fm.representation, fm.rowType, fm.Length); 
            var vp = cx.GetUid();
            if (tok == Sqlx.DEFAULT)
            {
                Next();
                Mustbe(Sqlx.VALUES);
                sv = SqlNull.Value;
            }
            else
            // care: we might have e.g. a subquery here
                sv = ParseSqlValue(fm);
            if (sv is SqlRow) // tolerate a single value without the VALUES keyword
                sv = new SqlRowArray(vp, cx, sv.domain, new BList<long?>(sv.defpos));
            var sce = sv.RowSetFor(vp, cx,fm.rowType,fm.representation) + (cx,RowSet.RSTargets, fm.rsTargets) 
                + (RowSet.Asserts,RowSet.Assertions.AssignTarget);
            cx._Add(sce);
            SqlInsert s = new(lp.dp, fm, sce.defpos, cs); 
            cx.Add(s);
            cx.result = s.value;
            if (Match(Sqlx.SECURITY))
            {
                Next();
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105");
                s += (DBObject.Classification, MustBeLevel());
            }
            cx.DecSD();
            if (cx.parse == ExecuteStatus.Obey && cx.db is Transaction tr)
                cx = tr.Execute(s, cx);
            cx.exec = s;
            return (SqlInsert)cx.Add(s);
        }
        /// <summary>
		/// DeleteSearched = DELETE [XMLOption] FROM Table_id [ WhereClause] .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseSqlDelete()
        {
            var lp = LexPos();
            Next();
            ParseXmlOption(false);
            Mustbe(Sqlx.FROM);
            Ident ic = new(this);
            cx.IncSD(ic);
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.AS)
                Next();
            if (tok == Sqlx.ID)
            {
                new Ident(this);
                Next();
            }
            var ob = cx.GetObject(ic.ident);
            if (ob == null && cx.defs.Contains(ic.ident))
                ob = cx.obs[cx.defs[(ic.ident,lp.sd)].Item1.dp];
            if (ob == null)
                throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob as Domain??Domain.Null, Grant.Privilege.Delete));
            QuerySearch qs = new(lp.dp, f);
            cx.defs += (ic, lp);
            cx.GetUid();
            cx.Add(qs);
            var rs = (RowSet?)cx.obs[qs.source]??throw new PEException("PE2006");
            if (ParseWhereClause() is CTree<long, bool> wh)
            {
                rs = (RowSet?)cx.obs[rs.defpos]??throw new PEException("PE2007");
                rs = rs.Apply(RowSet.E + (RowSet._Where, rs.where + wh),cx);
            }
            cx._Add(rs);
            cx.result = rs.defpos;
            if (tok != Sqlx.EOF)
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
        /// <param name="sql">the sql</param>
        /// <returns>the updatesearch</returns>
        internal Context ParseSqlUpdate(Context cx, string sql)
        {
            lxr = new Lexer(cx,sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length);
            return ParseSqlUpdate().Item1;
        }
        /// <summary>
		/// UpdateSearched = UPDATE [XMLOption] Table_id Assignment [WhereClause] .
        /// </summary>
        /// <returns>The UpdateSearch</returns>
		(Context,Executable) ParseSqlUpdate()
        {
            var st = LexPos().dp;
            Next();
            ParseXmlOption(false);
            Ident ic = new(this);
            cx.IncSD(ic);
            Mustbe(Sqlx.ID);
            Mustbe(Sqlx.SET);
            var ob = cx.GetObject(ic.ident) as Domain;
            if (ob == null && cx.defs.Contains(ic.ident))
                ob = cx.obs[cx.defs[(ic.ident,ic.iix.sd)].Item1.dp] as Domain;
            if (ob==null)
                throw new DBException("42107", ic.ident);
            var f = (RowSet)cx.Add(_From(ic, ob, ob, Grant.Privilege.Update));
            cx.AddDefs(ic, f);
            for (var b = f.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue c && c.name is not null)
                {
                    var dp = cx.Ix(p);
                    cx.defs += (new Ident(c.name, dp), dp);
                }
            UpdateSearch us = new(st, f);
            cx.Add(us);
            var ua = ParseAssignments();
            var rs = (RowSet)(cx.obs[us.source]??throw new DBException("PE2009"));
            rs = rs.Apply(new BTree<long,object>(RowSet.Assig, ua),cx);
            if (ParseWhereClause() is CTree<long, bool> wh)
            {
                rs = (RowSet)(cx.obs[rs.defpos]??throw new DBException("PE2010"));
                rs = rs.Apply(new BTree<long, object>(RowSet._Where, wh),cx);
            }
            cx.result = rs.defpos;
            if (cx.parse == ExecuteStatus.Obey)
                cx = ((Transaction)cx.db).Execute(us, cx);
            us = (UpdateSearch)cx.Add(us);
            cx.exec = us;
            cx.DecSD();
            return (cx,us);
        }
        internal CTree<UpdateAssignment,bool> ParseAssignments(string sql)
        {
            lxr = new Lexer(cx,sql);
            tok = lxr.tok;
            return ParseAssignments();
        }
        /// <summary>
        /// Assignment = 	SET Target '='  TypedValue { ',' Target '='  TypedValue }
        /// </summary>
        /// <returns>the tree of assignments</returns>
		CTree<UpdateAssignment,bool> ParseAssignments()
		{
            var r = CTree<UpdateAssignment,bool>.Empty + (ParseUpdateAssignment(),true);
            while (tok==Sqlx.COMMA)
			{
				Next();
				r+=(ParseUpdateAssignment(),true);
			}
			return r;
		}
        /// <summary>
        /// Target '='  TypedValue
        /// </summary>
        /// <returns>An updateAssignmentStatement</returns>
		UpdateAssignment ParseUpdateAssignment()
        {
            SqlValue vbl;
            SqlValue val;
            Match(Sqlx.SECURITY);
            if (tok == Sqlx.SECURITY)
            {
                if (cx.db.user?.defpos != cx.db.owner)
                    throw new DBException("42105");
                vbl = (SqlValue)cx.Add(new SqlSecurity(LexPos().dp,cx));
                Next();
            }
            else vbl = ParseVarOrColumn(Domain.Content);
            Mustbe(Sqlx.EQL);
            val = ParseSqlValue(vbl.domain);
            return new UpdateAssignment(vbl.defpos,val.defpos);
        }
        /// <summary>
        /// Parse an SQL Value
        /// </summary>
        /// <param name="s">The string to parse</param>
        /// <param name="t">the expected obs type if any</param>
        /// <returns>the SqlValue</returns>
        internal SqlValue ParseSqlValue(string s,Domain xp)
        {
            lxr = new Lexer(cx,s);
            tok = lxr.tok;
            return ParseSqlValue(xp);
        }
        internal SqlValue ParseSqlValue(Ident ic, Domain xp)
        {
            lxr = new Lexer(cx,ic.ident,ic.iix.lp);
            tok = lxr.tok;
            return ParseSqlValue(xp);
        }
        /// <summary>
        /// Alas the following informal syntax is not a good guide to the way LL(1) has to go...
		///  Value = 		Literal
        /// |   ID ':'  TypedValue
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
        /// <returns>the SqlValue</returns>
        internal SqlValue ParseSqlValue(Domain xp, bool wfok = false)
        {
            if (tok == Sqlx.PERIOD)
            {
                Next();
                Mustbe(Sqlx.LPAREN);
                var op1 = ParseSqlValue(Domain.UnionDate);
                Mustbe(Sqlx.COMMA);
                var op2 = ParseSqlValue(Domain.UnionDate);
                Mustbe(Sqlx.RPAREN);
                var r = new SqlValueExpr(LexPos().dp, cx, Sqlx.PERIOD, op1, op2, Sqlx.NO);
                return (SqlValue)cx.Add(r);
            }
            SqlValue left;
            if (xp.kind == Sqlx.BOOLEAN || xp.kind == Sqlx.CONTENT)
            {
                left = Disjoin(ParseSqlValueDisjunct(xp, wfok));
                while (left.domain.kind == Sqlx.BOOLEAN && tok == Sqlx.OR)
                {
                    Next();
                    left = new SqlValueExpr(LexPos().dp, cx, Sqlx.OR, left,
                        Disjoin(ParseSqlValueDisjunct(xp, wfok)), Sqlx.NO);
                }
            }
            else if (xp.kind == Sqlx.TABLE || xp.kind == Sqlx.VIEW || xp.kind==Sqlx.TYPE || xp is NodeType)
            {
                if (Match(Sqlx.TABLE))
                    Next();
                left = ParseSqlTableValue(xp);
                while (Match(Sqlx.UNION, Sqlx.EXCEPT, Sqlx.INTERSECT))
                {
                    var lp = LexPos();
                    var op = tok;
                    var m = Sqlx.NO;
                    Next();
                    if ((op == Sqlx.UNION || op == Sqlx.EXCEPT)
                        && Match(Sqlx.ALL, Sqlx.DISTINCT))
                    {
                        m = tok;
                        Next();
                    }
                    var right = ParseSqlTableValue(xp);
                    left = new SqlValueExpr(lp.dp, cx, op, left, right, m);
                }
            }
            else if (xp.kind == Sqlx.TYPE && Match(Sqlx.LPAREN))
            {
                Next();
                if (Match(Sqlx.SELECT))
                {
                    var cs = ParseCursorSpecification(xp).union;
                    left = new SqlValueSelect(cx.GetUid(), cx,
                        (RowSet)(cx.obs[cs] ?? throw new DBException("PE2011")), xp);
                }
                else
                    left = ParseSqlValue(xp);
                Mustbe(Sqlx.RPAREN);
            }
            else
                left = ParseSqlValueExpression(xp, wfok);
            return ((SqlValue)cx.Add(left));
        }
        SqlValue ParseSqlTableValue(Domain xp)
        {
            if (tok == Sqlx.LPAREN)
            {
                Next();
                if (tok == Sqlx.SELECT)
                {
                    var cs = ParseCursorSpecification(xp).union;
                    Mustbe(Sqlx.RPAREN);
                    return (SqlValue)cx.Add(new SqlValueSelect(cx.GetUid(), cx,
                        (RowSet)(cx.obs[cs]??throw new DBException("PE2012")),xp));
                }
            }
            if (Match(Sqlx.SELECT))
                return (SqlValue)cx.Add(new SqlValueSelect(cx.GetUid(),cx,
                    (RowSet)(cx.obs[ParseCursorSpecification(xp).union]??throw new DBException("PE2013")),xp));
            if (Match(Sqlx.VALUES))
            {
                var lp = LexPos();
                Next();
                var v = ParseSqlValueList(xp);
                return (SqlValue)cx.Add(new SqlRowArray(lp.dp, cx, xp, v));
            }
            if (Match(Sqlx.MATCH))
            {
                ParseSqlMatchStatement();
                var rs = cx.obs[cx.result] as RowSet ?? throw new DBException("42000");
                return (SqlValue)cx.Add(new SqlValueSelect(cx.GetUid(), cx, rs,xp));
            }
            if (Match(Sqlx.TABLE))
                Next();
            return ParseSqlValueItem(xp,false);
        }
        SqlValue Disjoin(CTree<long,bool> s) // s is not empty
        {
            var rb = s.Last();
            var rp = rb?.key() ?? -1L;
            var right = (SqlValue?)cx.obs[rp]??SqlNull.Value;
            for (rb=rb?.Previous();rb is not null;rb=rb.Previous())
                if (cx.obs[rb.key()] is SqlValue lf)
                    right = (SqlValue)cx.Add(new SqlValueExpr(LexPos().dp, cx, Sqlx.AND, 
                        lf, right, Sqlx.NO));
            return (SqlValue)cx.Add(right);
        }
        /// <summary>
        /// Parse a possibly boolean expression
        /// </summary>
        /// <param name="xp"></param>
        /// <param name="wfok"></param>
        /// <param name="dm">A select tree to the left of a Having clause, or null</param>
        /// <returns>A disjunction of expressions</returns>
        CTree<long,bool> ParseSqlValueDisjunct(Domain xp,bool wfok, Domain? dm=null)
        {
            var left = ParseSqlValueConjunct(xp, wfok, dm);
            var r = new CTree<long, bool>(left.defpos, true);
            while (left.domain.kind==Sqlx.BOOLEAN && Match(Sqlx.AND))
            {
                Next();
                left = ParseSqlValueConjunct(xp,wfok, dm);
                r += (left.defpos, true);
            }
            return r;
        }
        SqlValue ParseSqlValueConjunct(Domain xp,bool wfok,Domain? dm)
        {
            var left = ParseSqlValueConjunct(xp, wfok);
            return (dm == null) ? left : left.Having(cx,dm);
        }
        SqlValue ParseSqlValueConjunct(Domain xp,bool wfok)
        {
            var left = ParseSqlValueExpression(Domain.Content,wfok);
            if (Match(Sqlx.EQL, Sqlx.NEQ, Sqlx.LSS, Sqlx.GTR, Sqlx.LEQ, Sqlx.GEQ))
            {
                var op = tok;
                var lp = LexPos();
                Next();
                return (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx,
                    op, left, ParseSqlValueExpression(left.domain,wfok), Sqlx.NO));
            }
            if (xp.kind != Sqlx.CONTENT)
            {
                var nd = left.domain.LimitBy(cx, left.defpos, xp);
                if (nd != left.domain && nd != null)
                    left += (DBObject._Domain, nd);
            }
            return (SqlValue)cx.Add(left);
        }
        SqlValue ParseSqlValueExpression(Domain xp,bool wfok)
        {
            var left = ParseSqlValueTerm(xp,wfok);
            while ((Domain.UnionDateNumeric.CanTakeValueOf(left.domain)
                ||left.GetType().Name=="SqlValue") 
                && Match(Sqlx.PLUS, Sqlx.MINUS))
            {
                var op = tok;
                var lp = LexPos();
                Next();
                var x = ParseSqlValueTerm(xp, wfok);
                left = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, op, left, x, Sqlx.NO));
            }
            return (SqlValue)cx.Add(left);
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
        SqlValue ParseSqlValueTerm(Domain xp,bool wfok)
        {
            bool sign = false, not = false;
            var lp = LexPos();
            if (tok == Sqlx.PLUS)
                Next();
            else if (tok == Sqlx.MINUS)
            {
                Next();
                sign = true;
            }
            else if (tok == Sqlx.NOT)
            {
                Next();
                not = true;
            }	
            var left = ParseSqlValueFactor(xp,wfok);
            if (sign)
                left = new SqlValueExpr(lp.dp, cx, Sqlx.MINUS, null, left, Sqlx.NO)
                    .Constrain(cx,Domain.UnionNumeric);
            else if (not)
                left = left.Invert(cx);
            var imm = Sqlx.NO;
            if (Match(Sqlx.IMMEDIATELY))
            {
                Next();
                imm = Sqlx.IMMEDIATELY;
            }
            if (Match(Sqlx.CONTAINS, Sqlx.OVERLAPS, Sqlx.EQUALS, Sqlx.PRECEDES, Sqlx.SUCCEEDS))
            {
                var op = tok;
                lp = LexPos();
                Next();
                return (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx,
                    op, left, ParseSqlValueFactor(left.domain,wfok), imm));
            }
            while (Match(Sqlx.TIMES, Sqlx.DIVIDE,Sqlx.MULTISET))
            {
                Sqlx op = tok;
                lp = LexPos();
                switch (op)
                {
                    case Sqlx.TIMES:
                        break;
                    case Sqlx.DIVIDE: goto case Sqlx.TIMES;
                    case Sqlx.MULTISET:
                        {
                            Next();
                            if (Match(Sqlx.INTERSECT))
                                op = tok;
                            else
                            {
                                tok = lxr.PushBack(Sqlx.MULTISET);
                                return (SqlValue)cx.Add(left);
                            }
                        }
                        break;
                }
                Sqlx m = Sqlx.NO;
                if (Match(Sqlx.ALL, Sqlx.DISTINCT))
                {
                    m = tok;
                    Next();
                }
                Next();
                if (left.domain.kind == Sqlx.TABLE)
                    left += (Domain.Kind,Sqlx.CONTENT); // must be scalar
                left = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, op, left, 
                    ParseSqlValueFactor(left.domain,wfok), m));
            }
            return (SqlValue)cx.Add(left);
        }
        /// <summary>
        /// |	Value '||'  TypedValue 
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the SqlValue</returns>
		SqlValue ParseSqlValueFactor(Domain xp,bool wfok)
		{
			var left = ParseSqlValueEntry(xp,wfok);
			while (Match(Sqlx.CONCATENATE,Sqlx.COLON))
			{
				Sqlx op = tok;
				Next();
				var right = ParseSqlValueEntry(left.domain,wfok);
				left = new SqlValueExpr(LexPos().dp, cx, op,left,right,Sqlx.NO);
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
		SqlValue ParseSqlValueEntry(Domain xp,bool wfok)
        {
            var left = ParseSqlValueItem(xp,wfok);
            bool invert = false;
            var lp = LexPos();
            while (tok==Sqlx.DOT || tok==Sqlx.LBRACK)
                if (tok==Sqlx.DOT)
                {
                    // could be table alias, block id, instance id etc
                    Next();
                    if (tok == Sqlx.TIMES)
                    {
                        lp = LexPos();
                        Next();
                        return new SqlStar(lp.dp, cx, left.defpos);
                    }
                    var n = new Ident(this);
                    Mustbe(Sqlx.ID);
                    if (tok == Sqlx.LPAREN)
                    {
                        var ps = BList<long?>.Empty;
                        Next();
                        if (tok != Sqlx.RPAREN)
                            ps = ParseSqlValueList(xp);
                        cx.Add(left);
                        var ut = left.domain; // care, the methodInfos may be missing some later methods
                        if (cx.db.objects[ut.defpos] is not Domain u || u.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42105");
                        var ar = cx.Signature(ps);
                        var pr = cx.db.objects[oi.methodInfos[n.ident]?[ar] ?? -1L] as Method
                            ?? throw new DBException("42173", n);
                        left = new SqlMethodCall(lp.dp, cx, pr, ps, left);
                        Mustbe(Sqlx.RPAREN);
                        left = (SqlValue)cx.Add(left);
                    }
                    else
                    {
                        var oi = left.infos[cx.role.defpos];
                        if (oi is null || oi.names == BTree<string, (int, long?)>.Empty)
                            oi = left.domain.infos[cx.role.defpos];
                        var cp = oi?.names[n.ident].Item2?? -1L;
                        var el = (SqlValue)cx.Add(new SqlCopy(n.iix.dp, cx,n.ident,lp.dp,cp));
                        left = new SqlValueExpr(lp.dp, cx, Sqlx.DOT, left, el,Sqlx.NO);
                    }
                } else // tok==Sqlx.LBRACK
                {
                    Next();
                    left = new SqlValueExpr(lp.dp, cx, Sqlx.LBRACK, left,
                        ParseSqlValue(Domain.Int), Sqlx.NO);
                    Mustbe(Sqlx.RBRACK);
                }

            if (tok == Sqlx.IS)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                bool b = true;
                if (tok == Sqlx.NOT)
                {
                    Next();
                    b = false;
                }
                if (tok == Sqlx.OF)
                {
                    Next();
                    Mustbe(Sqlx.LPAREN);
                    var r = BList<Domain>.Empty;
                    var es = BList<long?>.Empty;
                    var t1 = ParseSqlDataType();
                    lp = LexPos();
                    r+=t1;
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        t1 = ParseSqlDataType();
                        lp = LexPos();
                        r+=t1;
                    }
                    Mustbe(Sqlx.RPAREN);
                    return (SqlValue)cx.Add(new TypePredicate(lp.dp,left, b, r));
                }
                Mustbe(Sqlx.NULL);
                return (SqlValue)cx.Add(new NullPredicate(lp.dp,left, b));
            }
            var savestart = lxr.start;
            if (tok == Sqlx.NOT)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                invert = true;
            }
            if (tok == Sqlx.BETWEEN)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                var od = left.domain;
                var lw = ParseSqlValueTerm(od, false);
                Mustbe(Sqlx.AND);
                var hi = ParseSqlValueTerm(od, false);
                return (SqlValue)cx.Add(new BetweenPredicate(lp.dp, cx, left, !invert, lw, hi));
            }
            if (tok == Sqlx.LIKE)
            {
                if (!(xp.CanTakeValueOf(Domain.Bool) && 
                    Domain.Char.CanTakeValueOf(left.domain)))
                    throw new DBException("42000", lxr.pos);
                Next();
                LikePredicate k = new (lp.dp,cx, left, !invert,ParseSqlValue(Domain.Char), null);
                if (tok == Sqlx.ESCAPE)
                {
                    Next();
                    k+=(cx,LikePredicate.Escape,ParseSqlValueItem(Domain.Char, false)?.defpos??-1L);
                }
                return (SqlValue)cx.Add(k);
            }
#if SIMILAR
            if (tok == Sqlx.LIKE_REGEX)
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
            if (tok == Sqlx.SIMILAR)
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
            if (tok == Sqlx.IN)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                InPredicate n = new InPredicate(lp.dp, cx, left)+
                    (QuantifiedPredicate.Found, !invert);
                if (tok == Sqlx.LPAREN)
                {
                    Next();
                    if (Match(Sqlx.SELECT, Sqlx.TABLE, Sqlx.VALUES))
                    {
                        RowSet rs = ParseQuerySpecification(Domain.TableType);
                        cx.Add(rs);
                        n += (cx,QuantifiedPredicate._Select, rs.defpos);
                    }
                    else
                        n += (cx,QuantifiedPredicate.Vals, ParseSqlValueList(left.domain));
                    Mustbe(Sqlx.RPAREN);
                }
                else
                    n += (cx,SqlFunction._Val, ParseSqlValue(
                        (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.COLLECT, left.domain))).defpos);
                return (SqlValue)cx.Add(n);
            }
            if (tok == Sqlx.MEMBER)
            {
                if (!xp.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Sqlx.OF);
                var dm = (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.MULTISET, xp));
                return (SqlValue)cx.Add(new MemberPredicate(LexPos().dp, cx,left,
                    !invert, ParseSqlValue(dm)));
            }
            if (invert)
            {
                tok = lxr.PushBack(Sqlx.NOT);
                lxr.pos = lxr.start-1;
                lxr.start = savestart;
            }
            else
            if (tok == Sqlx.COLLATE)
                left = ParseCollateExpr(left);
            return (SqlValue)cx.Add(left);
		}
        /// <summary>
        /// |	Value Collate 
        /// </summary>
        /// <param name="e">The SqlValue</param>
        /// <returns>The collated SqlValue</returns>
        SqlValue ParseCollateExpr(SqlValue e)
        {
            Next();
            var o = lxr.val;
            Mustbe(Sqlx.ID);
            var ci = new CultureInfo(o.ToString());
            e += (Domain.Culture,ci);
            return (SqlValue)cx.Add(e);
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
        /// <returns>the sql value</returns>
        internal SqlValue ParseSqlValueItem(Domain xp,bool wfok)
        {
            SqlValue r;
            var lp = LexPos();
            if (tok == Sqlx.QMARK && cx.parse == ExecuteStatus.Prepare)
            {
                Next();
                var qm = new SqlLiteral(lp.dp, new TQParam(Domain.Content,lp));
                cx.qParams += qm.defpos;
                return qm;
            }
            if (Match(Sqlx.LEVEL))
            {
                return (SqlValue)cx.Add(new SqlLiteral(LexPos().dp, TLevel.New(MustBeLevel())));
            }
            Match(Sqlx.SCHEMA); // for Pyrrho 5.1 most recent schema change
            if (Match(Sqlx.ID,Sqlx.FIRST,Sqlx.NEXT,Sqlx.LAST,Sqlx.CHECK,Sqlx.TYPE_URI)) // ID or pseudo ident
            {
                SqlValue vr = ParseVarOrColumn(xp);
                if (tok == Sqlx.DOUBLECOLON)
                {
                    Next();
                    if (vr.name==null || cx.db.objects[cx.role.dbobjects[vr.name]??-1L] is not Domain ut
                        || ut.infos[cx.role.defpos] is not ObInfo oi)
                        throw new DBException("42139",vr.name??"??").Mix();
                    var name = new Ident(this);
                    Mustbe(Sqlx.ID);
                    lp = LexPos();
                    Mustbe(Sqlx.LPAREN);
                    var ps = ParseSqlValueList(xp);
                    Mustbe(Sqlx.RPAREN);
                    var n = cx.Signature(ps);
                    var m = cx.db.objects[oi.methodInfos[name.ident]?[n]??-1L] as Method
                        ?? throw new DBException("42132",name.ident,ut.name).Mix();
                    if (m.methodType != PMethod.MethodType.Static)
                        throw new DBException("42140").Mix();
                    var fc = new SqlMethodCall(lp.dp, cx, m, ps, vr);
                    return (SqlValue)cx.Add(fc);
                }
                return (SqlValue)cx.Add(vr);
            }
            if (Match(Sqlx.EXISTS,Sqlx.UNIQUE))
            {
                Sqlx op = tok;
                Next();
                Mustbe(Sqlx.LPAREN);
                RowSet g = ParseQueryExpression(Domain.Null);
                Mustbe(Sqlx.RPAREN);
                if (op == Sqlx.EXISTS)
                    return (SqlValue)cx.Add(new ExistsPredicate(LexPos().dp, cx, g));
                else
                    return (SqlValue)cx.Add(new UniquePredicate(LexPos().dp, cx, g));
            }
            if (Match(Sqlx.RDFLITERAL, Sqlx.DOCUMENTLITERAL, Sqlx.CHARLITERAL, 
                Sqlx.INTEGERLITERAL, Sqlx.NUMERICLITERAL, Sqlx.NULL,
            Sqlx.REALLITERAL, Sqlx.BLOBLITERAL, Sqlx.BOOLEANLITERAL))
            {
                r = new SqlLiteral(LexDp(), lxr.val);
                Next();
                return (SqlValue)cx.Add(r);
            }
            // pseudo functions
            switch (tok)
            {
                case Sqlx.ARRAY:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                        {
                            lp = LexPos();
                            Next();
                            if (tok == Sqlx.SELECT)
                            {
                                var st = lxr.start;
                                var cs = ParseCursorSpecification(Domain.Null).union;
                                Mustbe(Sqlx.RPAREN);
                                return (SqlValue)cx.Add(new SqlValueSelect(lp.dp, cx, 
                                    (RowSet)(cx.obs[cs]??throw new DBException("42000")),xp)); 
                            }
                            throw new DBException("22204");
                        }
                        Mustbe(Sqlx.LBRACK);
                        var et = (xp.kind == Sqlx.CONTENT) ? xp 
                            : xp.elType?? throw new DBException("42000", lxr.pos);
                        var v = ParseSqlValueList(et);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Sqlx.RBRACK);
                        return (SqlValue)cx.Add(new SqlValueArray(lp.dp, cx, xp, v));
                    }
                 case Sqlx.SCHEMA:
                    {
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        var (ob,n) = ParseObjectName();
                        if (Match(Sqlx.COLUMN))
                        {
                            Next();
                            var cn = lxr.val;
                            Mustbe(Sqlx.ID);
                            if (ob is not Table tb)
                                throw new DBException("42107", n).Mix();
                            if (cx._Dom(tb.defpos) is not Domain ft ||
                                cx.db.objects[ft.ColFor(cx, cn.ToString())] is not DBObject oc)
                                    throw new DBException("42112", cn.ToString());
                            ob = oc;
                        }
                        r = new SqlLiteral(lp.dp, new TInt(ob.lastChange));
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(r);
                    } 
                case Sqlx.CURRENT_DATE: 
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx,Sqlx.CURRENT_DATE, 
                            null, null,null,Sqlx.NO));
                    }
                case Sqlx.CURRENT_ROLE:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx,Sqlx.CURRENT_ROLE, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.CURRENT_TIME:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx,Sqlx.CURRENT_TIME, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.CURRENT_TIMESTAMP: 
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx,Sqlx.CURRENT_TIMESTAMP, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.USER:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx,Sqlx.USER, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.DATE: // also TIME, TIMESTAMP, INTERVAL
                    {
                        Sqlx tk = tok;
                        Next();
                        var o = lxr.val;
                        lp = LexPos();
                        if (tok == Sqlx.CHARLITERAL)
                        {
                            Next();
                            return new SqlDateTimeLiteral(lp.dp,cx,Domain.For(tk), o.ToString());
                        }
                        else
                            return (SqlValue)cx.Add(new SqlLiteral(lp.dp,o));
                    }
                case Sqlx.INTERVAL:
                    {
                        Next();
                        var o = lxr.val;
                        Mustbe(Sqlx.CHARLITERAL);
                        Domain di = ParseIntervalType();
                        return (SqlValue)cx.Add(new SqlDateTimeLiteral(lp.dp, cx,di, o.ToString()));
                    }
                case Sqlx.LPAREN:// subquery
                    {
                        Next();
                        if (tok == Sqlx.SELECT)
                        {
                            var st = lxr.start;
                            var cs = ParseCursorSpecification(xp).union;
                            Mustbe(Sqlx.RPAREN);
                            return (SqlValue)cx.Add(new SqlValueSelect(cx.GetUid(), 
                                cx,(RowSet)(cx.obs[cs]??throw new PEException("PE2010")),xp));
                        }
                        Domain et = Domain.Null;
                        switch(xp.kind)
                        {
                            case Sqlx.ARRAY:
                            case Sqlx.MULTISET:
                                et = xp.elType??Domain.Null;
                                break;
                            case Sqlx.CONTENT:
                                et = Domain.Content;
                                break;
                            case Sqlx.ROW:
                                break;
                            default:
                                var v = ParseSqlValue(xp);
                                if (v is SqlLiteral sl)
                                    v = (SqlValue)cx.Add(new SqlLiteral(lp.dp, xp.Coerce(cx, sl.val)));
                                Mustbe(Sqlx.RPAREN);
                                return v;
                        }
                        var fs = BList<DBObject>.Empty;
                        for (var i = 0; ; i++)
                        {
                            var it = ParseSqlValue(et??
                                xp.representation[xp[i]??-1L]??Domain.Content);
                            if (tok == Sqlx.AS)
                            {
                                lp = LexPos();
                                Next();
                                var ic = new Ident(this);
                                Mustbe(Sqlx.ID);
                                it += (DBObject._Alias, ic.ToString());
                                cx.Add(it);
                            }
                            fs += it;
                            if (tok != Sqlx.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sqlx.RPAREN);
                        if (fs.Length==1 && fs[0] is SqlValue w)
                            return (SqlValue)cx.Add(w);
                        return (SqlValue)cx.Add(new SqlRow(lp.dp,cx,fs)); 
                    }
                case Sqlx.MULTISET:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                            return ParseSqlValue(xp);
                        Mustbe(Sqlx.LBRACK);
                        var v = ParseSqlValueList(xp);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Sqlx.RBRACK);
                        return (SqlValue)cx.Add(new SqlValueMultiset(lp.dp, cx, xp, v));
                    }
                case Sqlx.SET:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                            return ParseSqlValue(xp);
                        Mustbe(Sqlx.LBRACK);
                        var v = ParseSqlValueList(xp);
                        if (v.Length == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Sqlx.RBRACK);
                        return (SqlValue)cx.Add(new SqlValueSet(lp.dp, cx, xp, v));
                    }
                case Sqlx.NEW:
                    {
                        Next();
                        var o = new Ident(this);
                        Mustbe(Sqlx.ID);
                        lp = LexPos();
                        if (cx.db.objects[cx.role.dbobjects[o.ident]??-1L] is not Domain ut
                            || ut.infos[cx.role.defpos] is not ObInfo oi)
                            throw new DBException("42142").Mix();
                        Mustbe(Sqlx.LPAREN);
                        var ps = ParseSqlValueList(ut);
                        var n = cx.Signature(ps);
                        Mustbe(Sqlx.RPAREN);
                        if (cx.db.objects[oi.methodInfos[o.ident]?[n] ?? -1L] is not Method m)
                        {
                            if (ut.Length != 0 && ut.Length != (int)n.Count)
                                throw new DBException("42142").Mix();
                            return (SqlValue)cx.Add(new SqlDefaultConstructor(o.iix.dp, cx, ut, ps));
                        }
                        if (m.methodType != PMethod.MethodType.Constructor)
                            throw new DBException("42142").Mix();
                        return (SqlValue)cx.Add(new SqlProcedureCall(o.iix.dp, cx, m, ps));
                    }
                case Sqlx.ROW:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                        {
                            lp = LexPos();
                            Next();
                            var v = ParseSqlValueList(xp);
                            Mustbe(Sqlx.RPAREN);
                            return (SqlValue)cx.Add(new SqlRow(lp.dp,cx,xp,v));
                        }
                        throw new DBException("42135", "ROW").Mix();
                    }
                /*       case Sqlx.SELECT:
                           {
                               var sc = new SaveContext(trans, ExecuteStatus.Parse);
                               RowSet cs = ParseCursorSpecification(t).stmt as RowSet;
                               sc.Restore(tr);
                               return (SqlValue)cx.Add(new SqlValueSelect(cs, t));
                           } */
                case Sqlx.TABLE: // allowed by 6.39
                    {
                        Next();
                        var lf = ParseSqlValue(Domain.TableType);
                        return (SqlValue)cx.Add(lf);
                    }

                case Sqlx.TIME: goto case Sqlx.DATE;
                case Sqlx.TIMESTAMP: goto case Sqlx.DATE;
                case Sqlx.TREAT:
                    {
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        var v = ParseSqlValue(Domain.Content);
                        Mustbe(Sqlx.RPAREN);
                        Mustbe(Sqlx.AS);
                        var dt = ParseSqlDataType();
                        return (SqlValue)cx.Add(new SqlTreatExpr(lp.dp, v, dt));//.Needs(v);
                    }
                case Sqlx.CASE:
                    {
                        Next();
                        SqlValue? v = null;
                        Domain cp = Domain.Bool;
                        Domain rd = Domain.Content;
                        if (tok != Sqlx.WHEN)
                        {
                            v = ParseSqlValue(xp);
                            cx.Add(v);
                            cp = v.domain;
                        }
                        var cs = BList<(long, long)>.Empty;
                        var wh = BList<long?>.Empty;
                        while (Mustbe(Sqlx.WHEN, Sqlx.ELSE) == Sqlx.WHEN)
                        {
                            var w = ParseSqlValue(cp);
                            cx.Add(w);
                            wh += w.defpos;
                            while (v != null && tok == Sqlx.COMMA)
                            {
                                Next();
                                w = ParseSqlValue(cp);
                                cx.Add(w);
                                wh += w.defpos;
                            }
                            Mustbe(Sqlx.THEN);
                            var x = ParseSqlValue(xp);
                            cx.Add(x);
                            rd = rd.Constrain(cx, lp.dp, x.domain);
                            for (var b = wh.First(); b != null; b = b.Next())
                                if (b.value() is long p)
                                    cs += (p, x.defpos);
                        }
                        var el = ParseSqlValue(xp);
                        cx.Add(el);
                        Mustbe(Sqlx.END);
                        return (SqlValue)cx.Add((v == null) ? (SqlValue)new SqlCaseSearch(lp.dp, cx, rd, cs, el.defpos)
                            : new SqlCaseSimple(lp.dp, cx, rd, v, cs, el.defpos));
                    }
                case Sqlx.VALUE:
                    {
                        Next();
                        SqlValue vbl = new(new Ident("VALUE",lp),cx,xp);
                        return (SqlValue)cx.Add(vbl);
                    }
                case Sqlx.VALUES:
                    {
                        Next();
                        var v = ParseSqlValueList(xp);
                        return (SqlValue)cx.Add(new SqlRowArray(lp.dp, cx, xp, v));
                    }
                case Sqlx.LBRACE:
                    {
                        var v = BList<DBObject>.Empty;
                        Next();
                        if (tok != Sqlx.RBRACE)
                        {
                            var (n,sv) = GetDocItem();
                            v += (Domain)cx.Add(sv+(ObInfo.Name,n));
                        }
                        while (tok==Sqlx.COMMA)
                        {
                            Next();
                            var (n,sv) = GetDocItem();
                            v += (Domain)cx.Add(sv + (ObInfo.Name, n));
                        }
                        Mustbe(Sqlx.RBRACE);
                        return (SqlValue)cx.Add(new SqlRow(cx.GetUid(),cx,v));
                    }
                case Sqlx.LBRACK:
                    {
                        if (xp.kind is Sqlx.SET)
                        {
                            Next();
                            var v = ParseSqlValueList(xp);
                            if (v.Length == 0)
                                throw new DBException("22103").ISO();
                            Mustbe(Sqlx.RBRACK);
                            return (SqlValue)cx.Add(new SqlValueSet(lp.dp, cx, xp, v));
                        }
                        return (SqlValue)cx.Add(ParseSqlDocArray());
                    }
                case Sqlx.LSS:
                    return (SqlValue)cx.Add(ParseXmlValue());
            }
            // "SQLFUNCTIONS"
            Sqlx kind;
            SqlValue? val = null;
            SqlValue? op1 = null;
            SqlValue? op2 = null;
            CTree<long,bool>? filter = null;
            Sqlx mod = Sqlx.NO;
            WindowSpecification? ws = null;
            Ident? windowName = null;
            lp = LexPos();
            switch (tok)
            {
                case Sqlx.ABS:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.UnionNumeric);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.ANY: goto case Sqlx.COUNT;
                case Sqlx.AVG: goto case Sqlx.COUNT;
                case Sqlx.CARDINALITY: // multiset arg functions
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Collection);
                        if (kind != Sqlx.MULTISET)
                            throw new DBException("42113", kind).Mix();
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.CAST:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Content);
                        Mustbe(Sqlx.AS);
                        op1 = (SqlValue)cx.Add(new SqlTypeExpr(cx.GetUid(),ParseSqlDataType()));
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.CEIL: goto case Sqlx.ABS;
                case Sqlx.CEILING: goto case Sqlx.ABS;
                case Sqlx.CHAR_LENGTH:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.COALESCE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        op1 = ParseSqlValue(xp);
                        Mustbe(Sqlx.COMMA);
                        op2 = ParseSqlValue(xp);
                        while (tok == Sqlx.COMMA)
                        {
                            Next();
                            op1 = new SqlCoalesce(LexPos().dp, cx,op1,op2);
                            op2 = ParseSqlValue(xp);
                        }
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(new SqlCoalesce(lp.dp, cx, op1, op2));
                    }
                case Sqlx.COLLECT: goto case Sqlx.COUNT;
#if OLAP
                case Sqlx.CORR: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.COUNT: // actually a special case: but deal with all ident-arg aggregates here
                    {
                        kind = tok;
                        mod = Sqlx.NO; // harmless default value
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        if (kind == Sqlx.COUNT && tok == Sqlx.TIMES)
                        {
                            val = (SqlValue)cx.Add(new SqlLiteral(LexPos().dp,new TInt(1L))
                                +(ObInfo.Name,"*"));
                            Next();
                            mod = Sqlx.TIMES;
                        }
                        else
                        {
                            if (tok == Sqlx.ALL)
                                Next();
                            else if (tok == Sqlx.DISTINCT)
                            {
                                mod = tok;
                                Next();
                            }
                            val = ParseSqlValue(Domain.Content);
                        }
                        Mustbe(Sqlx.RPAREN);
                        if (tok == Sqlx.FILTER)
                        {
                            Next();
                            Mustbe(Sqlx.LPAREN);
                            if (tok == Sqlx.WHERE)
                                filter = ParseWhereClause();
                            Mustbe(Sqlx.RPAREN);
                        }
                        if (tok == Sqlx.OVER && wfok)
                        {
                            Next();
                            if (tok == Sqlx.ID)
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
                        if (filter != null &&  filter !=CTree<long,bool>.Empty)
                            m += (SqlFunction.Filter, filter);
                        if (ws != null)
                            m += (SqlFunction.Window, ws.defpos);
                        if (windowName is not null)
                            m += (SqlFunction.WindowId, windowName);
                        var sf = new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, m);
                        return (SqlValue)cx.Add(sf);
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
                case Sqlx.CURRENT: // OF cursor --- delete positioned and update positioned
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.OF);
                        val = (SqlValue?)cx.Get(ParseIdentChain(),xp);
                        break;
                    }
#if OLAP
                case Sqlx.DENSE_RANK: goto case Sqlx.RANK;
#endif
                case Sqlx.ELEMENT: goto case Sqlx.CARDINALITY;
                case Sqlx.EVERY: goto case Sqlx.COUNT;
                case Sqlx.EXP: goto case Sqlx.ABS;
                case Sqlx.EXTRACT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        mod = tok;
                        Mustbe(Sqlx.YEAR, Sqlx.MONTH, Sqlx.DAY, Sqlx.HOUR, Sqlx.MINUTE, Sqlx.SECOND);
                        Mustbe(Sqlx.FROM);
                        val = ParseSqlValue(Domain.UnionDate);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.FLOOR: goto case Sqlx.ABS;
                case Sqlx.FUSION: goto case Sqlx.COUNT;
                case Sqlx.GROUPING:
                    {
                        Next();
                        return (SqlValue)cx.Add(new ColumnFunction(lp.dp, ParseIDList()));
                    }
                case Sqlx.INTERSECT: goto case Sqlx.COUNT;
                case Sqlx.LN: goto case Sqlx.ABS;
                case Sqlx.LOWER: goto case Sqlx.SUBSTRING;
                case Sqlx.MAX: 
                case Sqlx.MIN:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.UnionDateNumeric);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.MOD: goto case Sqlx.NULLIF;
                case Sqlx.NULLIF:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        op1 = ParseSqlValue(Domain.Content);
                        Mustbe(Sqlx.COMMA);
                        op2 = ParseSqlValue(op1.domain);
                        Mustbe(Sqlx.RPAREN);
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
                case Sqlx.OCTET_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.OF:
                    {
                        kind = tok;
                        Next();
                        TChar n;
                        var ns = new TList(Domain.Char); // happens to be suitable
                        while (tok != Sqlx.RPAREN)
                        {
                            Next();
                            n = lxr.val as TChar ?? throw new DBException("42000");
                            ns += n;
                            Mustbe(Sqlx.ID);
                        }
                        Mustbe(Sqlx.RPAREN);
                        val = (SqlValue)cx.Add(new SqlLiteral(cx.GetUid(), ns));
                        break;
                    }
                case Sqlx.PARTITION:
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
                case Sqlx.POSITION:
                    {
                        kind = tok;
                        Next();
                        if (tok == Sqlx.LPAREN)
                        {
                            Next();
                            op1 = ParseSqlValue(Domain.Int.defpos);
                            Mustbe(Sqlx.IN);
                            op2 = ParseSqlValue(Domain.Content.defpos);
                            Mustbe(Sqlx.RPAREN);
                        }
                        break;
                    }
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
                case Sqlx.POWER: goto case Sqlx.MOD;
                case Sqlx.RANK: goto case Sqlx.ROW_NUMBER;
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
                case Sqlx.ROW_NUMBER:
                    {
                        kind = tok;
                        Next();
                        lp = LexPos();
                        Mustbe(Sqlx.LPAREN);
                        if (tok == Sqlx.RPAREN)
                        {
                            Next();
                            Mustbe(Sqlx.OVER);
                            if (tok == Sqlx.ID)
                            {
                                windowName = new Ident(this);
                                Next();
                            }
                            else
                            {
                                ws = ParseWindowSpecificationDetails();
                                ws+=(ObInfo.Name,"U"+ cx.db.uid);
                            }
                            var m = BTree<long, object>.Empty;
                            if (filter != null)
                                m += (SqlFunction.Filter, filter);
                            if (ws != null)
                                m += (SqlFunction.Window, ws.defpos);
                            if (windowName != null)
                                m += (SqlFunction.WindowId, windowName);
                            return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, m));
                        }
                        var v = new BList<long?>(cx.Add(ParseSqlValue(xp)).defpos);
                        for (var i=1; tok == Sqlx.COMMA;i++)
                        {
                            Next();
                            v += ParseSqlValue(xp).defpos;
                        }
                        Mustbe(Sqlx.RPAREN);
                        val = new SqlRow(LexPos().dp, cx, xp, v);
                        var f = new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod, BTree<long, object>.Empty
                            + (SqlFunction.Window, ParseWithinGroupSpecification().defpos)
                            + (SqlFunction.WindowId,"U"+ cx.db.uid));
                        return (SqlValue)cx.Add(f);
                    }
                case Sqlx.ROWS: // Pyrrho (what is this?)
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        if (Match(Sqlx.TIMES))
                        {
                            mod = Sqlx.TIMES;
                            Next();
                        }
                        else
                            val = ParseSqlValue(Domain.Int);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.SOME: goto case Sqlx.COUNT;
                case Sqlx.DESCRIBE:
                case Sqlx.SPECIFICTYPE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, null, null, null, Sqlx.NO));
                    }
                case Sqlx.SQRT: goto case Sqlx.ABS;
                case Sqlx.STDDEV_POP: goto case Sqlx.COUNT;
                case Sqlx.STDDEV_SAMP: goto case Sqlx.COUNT;
                case Sqlx.SUBSTRING:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        if (kind == Sqlx.SUBSTRING)
                        {
#if SIMILAR
                            if (tok == Sqlx.SIMILAR)
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
                                Mustbe(Sqlx.FROM);
                                op1 = ParseSqlValue(Domain.Int);
                                if (tok == Sqlx.FOR)
                                {
                                    Next();
                                    op2 = ParseSqlValue(Domain.Int);
                                }
                            }
                        }
                        Mustbe(Sqlx.RPAREN);
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
                case Sqlx.SUM: goto case Sqlx.COUNT;
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
                case Sqlx.TRIM:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        if (Match(Sqlx.LEADING, Sqlx.TRAILING, Sqlx.BOTH))
                        {
                            mod = tok;
                            Next();
                        }
                        val = ParseSqlValue(Domain.Char);
                        if (tok == Sqlx.FROM)
                        {
                            Next();
                            op1 = val; // trim character
                            val = ParseSqlValue(Domain.Char);
                        }
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.UPPER: goto case Sqlx.SUBSTRING;
#if OLAP
                case Sqlx.VAR_POP: goto case Sqlx.COUNT;
                case Sqlx.VAR_SAMP: goto case Sqlx.COUNT;
#endif
                case Sqlx.VERSIONING:
                    kind = tok;
                    Next();
                    break;
                case Sqlx.XMLAGG: goto case Sqlx.COUNT;
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }
                case Sqlx.XMLCONCAT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Char);
                        while (tok == Sqlx.COMMA)
                        {
                            Next();
                            val = (SqlValue)cx.Add(new SqlValueExpr(LexPos().dp, cx, Sqlx.XMLCONCAT, 
                                val, ParseSqlValue(Domain.Char), Sqlx.NO));
                        }
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(val);
                    }
                case Sqlx.XMLELEMENT:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        Mustbe(Sqlx.NAME);
                        var name = lxr.val;
                        Mustbe(Sqlx.ID);
                        bool namespaces = false, attributes = false;
                        int n = 0;
                        while (tok == Sqlx.COMMA)
                        {
                            Next();
                            var llp = LexPos();
                            if (n == 0 && (!namespaces) && (!attributes) && tok == Sqlx.XMLNAMESPACES)
                            {
                                Next();
                                ParseXmlNamespaces();
                                namespaces = true;
                            }
                            else if (n == 0 && (!attributes) && tok == Sqlx.XMLATTRIBUTES)
                            {
                                Next();
                                Mustbe(Sqlx.LPAREN);
                                var doc = new SqlRow(llp.dp,BTree<long, object>.Empty);
                                var v = ParseSqlValue(Domain.Char);
                                var j = 0;
                                var a = new Ident("Att"+(++j), cx.Ix(0));
                                if (tok == Sqlx.AS)
                                {
                                    Next();
                                    a = new Ident(this);
                                    Mustbe(Sqlx.ID);
                                }
                                doc += (cx,v+(ObInfo.Name,a.ident));
                                a = new Ident("Att" + (++j), cx.Ix(0));
                                while (tok == Sqlx.COMMA)
                                {
                                    Next();
                                    var w = ParseSqlValue(Domain.Char);
                                    if (tok == Sqlx.AS)
                                    {
                                        Next();
                                        a = new Ident(this);
                                        Mustbe(Sqlx.ID);
                                    }
                                }
                                doc += (cx,v + (ObInfo.Name, a.ident));
                                v = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Sqlx.XMLATTRIBUTES, v, null, Sqlx.NO));
                                Mustbe(Sqlx.RPAREN);
                                op2 = v;
                            }
                            else
                            {
                                val = (SqlValue)cx.Add(new SqlValueExpr(lp.dp, cx, Sqlx.XML, val, 
                                    ParseSqlValue(Domain.Char), Sqlx.NO));
                                n++;
                            }
                        }
                        Mustbe(Sqlx.RPAREN);
                        op1 = (SqlValue)cx.Add(new SqlLiteral(LexPos().dp,name));
                        break;
                    }
                case Sqlx.XMLFOREST:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        if (tok == Sqlx.XMLNAMESPACES)
                        {
                            Next();
                            ParseXmlNamespaces();
                        }
                        val = ParseSqlValue(Domain.Char);
                        while (tok == Sqlx.COMMA)
                        {
                            var llp = LexPos();
                            Next();
                            val = (SqlValue)cx.Add(new SqlValueExpr(llp.dp, cx, Sqlx.XMLCONCAT, val, 
                                ParseSqlValue(Domain.Char), Sqlx.NO));
                        }
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(val);
                    }
                case Sqlx.XMLPARSE:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        Mustbe(Sqlx.CONTENT);
                        val = ParseSqlValue(Domain.Char);
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(val);
                    }
                case Sqlx.XMLQUERY:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        op1 = ParseSqlValue(Domain.Char);
                        if (tok != Sqlx.COMMA)
                            throw new DBException("42000", tok).Mix();
                        lxr.XmlNext(')');
                        op2 = (SqlValue)cx.Add(new SqlLiteral(LexPos().dp, new TChar(lxr.val.ToString())));
                        Next();
                        break;
                    }
                case Sqlx.XMLPI:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        Mustbe(Sqlx.NAME);
                        val = (SqlValue)cx.Add(new SqlLiteral(LexPos().dp, new TChar( lxr.val.ToString())));
                        Mustbe(Sqlx.ID);
                        if (tok == Sqlx.COMMA)
                        {
                            Next();
                            op1 = ParseSqlValue(Domain.Char);
                        }
                        Mustbe(Sqlx.RPAREN);
                        break;
                    }

                default:
                    {
                        var fc = (CallStatement)ParseProcedureCall();
                        return (SqlProcedureCall)(cx.obs[fc.call]??throw new DBException("42000"));
                    }
            }
            return (SqlValue)cx.Add(new SqlFunction(lp.dp, cx, kind, val, op1, op2, mod));
        }

        /// <summary>
        /// WithinGroup = WITHIN GROUP '(' OrderByClause ')' .
        /// </summary>
        /// <returns>A WindowSpecification</returns>
        WindowSpecification ParseWithinGroupSpecification()
        {
            WindowSpecification r = new(LexDp());
            Mustbe(Sqlx.WITHIN);
            Mustbe(Sqlx.GROUP);
            Mustbe(Sqlx.LPAREN);
            if (r.order!=Domain.Row)
                r+=(cx,WindowSpecification.Order,ParseOrderClause(r.order,false));
            Mustbe(Sqlx.RPAREN);
            return r;
        }
        /// <summary>
		/// XMLOption = WITH XMLNAMESPACES '(' XMLNDec {',' XMLNDec } ')' .
        /// </summary>
        void ParseXmlOption(bool donewith)
		{
            if (!donewith)
            {
                if (tok != Sqlx.WITH)
                    return;
                Next();
            }
			Mustbe(Sqlx.XMLNAMESPACES);
			ParseXmlNamespaces();
		}
        /// <summary>
		/// XMLNDec = (string AS id) | (DEFAULT string) | (NO DEFAULT) .
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseXmlNamespaces()
		{
            var pn = new XmlNameSpaces(LexDp());
			Mustbe(Sqlx.LPAREN);
			if (tok==Sqlx.NO)
			{
				Next();
				Mustbe(Sqlx.DEFAULT);
			} 
			else if (tok==Sqlx.DEFAULT)
			{
				Next();
				var o = lxr.val;
				Mustbe(Sqlx.CHARLITERAL);
                pn += (XmlNameSpaces.Nsps,pn.nsps+("", o.ToString()));
			}
			else
				for (Sqlx sep = Sqlx.COMMA;sep==Sqlx.COMMA;sep=tok)
				{
					var s = lxr.val;
					Mustbe(Sqlx.CHARLITERAL);
					Mustbe(Sqlx.AS);
					var p = lxr.val;
					Mustbe(Sqlx.ID);
					pn +=(XmlNameSpaces.Nsps,pn.nsps+(s.ToString(),p.ToString()));
				}
			Mustbe(Sqlx.RPAREN);
            return (Executable)cx.Add(pn);
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
			Mustbe(Sqlx.LPAREN);
			WindowSpecification w = new(LexDp());
            if (tok == Sqlx.ID)
            {
                w+=(WindowSpecification.OrderWindow,lxr.val.ToString());
                Next();
            }
            var dm = Domain.Row;
            if (tok==Sqlx.PARTITION)
			{
				Next();
				Mustbe(Sqlx.BY);
                dm = (Domain)dm.Relocate(cx.GetUid());
                var rs = dm.representation;
                var rt = dm.rowType;
                var d = 1;
                for (var b = ParseSqlValueList(Domain.Content).First(); b != null; b = b.Next())
                    if (b.value() is long p && cx._Dom(p) is Domain dp) {
                        rt += p; rs += (p, dp);
                        d = Math.Max(d, dp.depth + 1);
                    }
                dm = dm +(Domain.RowType,rt)+(Domain.Representation,rs)+(DBObject._Depth,d);
                w += (cx, WindowSpecification.PartitionType, dm);
			}
            if (tok == Sqlx.ORDER)
            {
                var oi = ParseOrderClause(dm);
                oi = (Domain)cx.Add(oi);
                w += (cx, WindowSpecification.Order, oi);
            }
			if (Match(Sqlx.ROWS,Sqlx.RANGE))
			{
				w+=(WindowSpecification.Units,tok);
				Next();
                if (tok == Sqlx.BETWEEN)
                {
                    Next();
                    w+=(WindowSpecification.Low,ParseWindowBound());
                    Mustbe(Sqlx.AND);
                    w+=(WindowSpecification.High,ParseWindowBound());
                }
                else
                    w += (WindowSpecification.Low, ParseWindowBound());
                if (Match(Sqlx.EXCLUDE))
                {
                    Next();
                    if (Match(Sqlx.CURRENT))
                    {
                        w+=(WindowSpecification.Exclude,tok);
                        Next();
                        Mustbe(Sqlx.ROW);
                    }
                    else if (Match(Sqlx.TIES))
                    {
                        w += (WindowSpecification.Exclude, Sqlx.EQL);
                        Next();
                    }
                    else if (Match(Sqlx.NO))
                    {
                        Next();
                        Mustbe(Sqlx.OTHERS);
                    }
                    else
                    {
                        w += (WindowSpecification.Exclude, tok);
                        Mustbe(Sqlx.GROUP);
                    }
                }
			}
			Mustbe(Sqlx.RPAREN);
            cx.Add(w);
			return w;
		}
        /// <summary>
		/// WindowBound = WindowStart | ((TypedValue | UNBOUNDED) FOLLOWING ) .
        /// </summary>
        /// <returns>The WindowBound</returns>
        WindowBound ParseWindowBound()
        {
            bool prec = false,unbd = true;
            TypedValue d = TNull.Value;
            if (Match(Sqlx.CURRENT))
            {
                Next();
                Mustbe(Sqlx.ROW);
                return new WindowBound();
            }
            if (Match(Sqlx.UNBOUNDED))
                Next();
            else if (tok == Sqlx.INTERVAL)
            {
                Next();
                var o=lxr.val;
                var lp = LexPos();
                Mustbe(Sqlx.CHAR);
                Domain di = ParseIntervalType();
                d = di.Parse(new Scanner(lp.dp,o.ToString().ToCharArray(),0,cx));
                unbd = false;
            }
            else
            {
                d = lxr.val;
                Mustbe(Sqlx.INTEGERLITERAL, Sqlx.NUMERICLITERAL);
                unbd = false;
            }
            if (Match(Sqlx.PRECEDING))
            {
                Next();
                prec = true;
            }
            else
                Mustbe(Sqlx.FOLLOWING);
            if (unbd)
                return new WindowBound()+(WindowBound.Preceding,prec);
            return new WindowBound()+(WindowBound.Preceding,prec)+(WindowBound.Distance,d);
        }
        /// <summary>
        /// Show the REST service, we can have a value, maybe a procedure call:
        /// </summary>
        /// <param name="sql">an expression string to parse</param>
        /// <returns>the SqlValue</returns>
        internal SqlValue ParseSqlValueItem(string sql,Domain xp)
        {
            lxr = new Lexer(cx,sql);
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId + sql.Length); // not really needed here
            return ParseSqlValueItem(xp,false);
        }
        /// <summary>
        /// Show the REST service there may be an explicit procedure call
        /// </summary>
        /// <param name="sql">a call statement to parse</param>
        /// <returns>the CallStatement</returns>
        internal CallStatement ParseProcedureCall(string sql)
        {
            lxr = new Lexer(cx,sql); 
            tok = lxr.tok;
            if (LexDp() > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextIid
                cx.db += (Database.NextId, cx.db.nextId +sql.Length); // not really needed here
            var n = new Ident(this);
            Mustbe(Sqlx.ID);
            var ps = BList<long?>.Empty;
            var lp = LexPos();
            if (tok == Sqlx.LPAREN)
            {
                Next();
                ps = ParseSqlValueList(Domain.Content);
            }
            var arity = cx.Signature(ps);
            Mustbe(Sqlx.RPAREN);
            var pp = cx.role.procedures[n.ident]?[arity] ?? -1;
            var pr = cx.db.objects[pp] as Procedure
                ?? throw new DBException("42108", n).Mix();
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (CallStatement)cx.Add(new CallStatement(lp.dp,fc));
        }
        /// <summary>
		/// UserFunctionCall = Id '(' [  TypedValue {','  TypedValue}] ')' .
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the Executable</returns>
        Executable ParseProcedureCall()
        {
            var id = new Ident(this);
            Mustbe(Sqlx.ID);
            Mustbe(Sqlx.LPAREN);
            var ps = ParseSqlValueList(Domain.Content);
            var a = cx.Signature(ps);
            Mustbe(Sqlx.RPAREN);
            if (cx.role.procedures[id.ident]?[a] is not long pp || 
                cx.db.objects[pp] is not Procedure pr)
                throw new DBException("42108", id.ident).Mix();
            var fc = new SqlProcedureCall(cx.GetUid(), cx, pr, ps);
            cx.Add(fc);
            return (Executable)cx.Add(new CallStatement(id.iix.dp,fc));
        }
        /// <summary>
        /// Parse a tree of Sql values
        /// </summary>
        /// <param name="t">the expected obs type</param>
        /// <returns>the List of SqlValue</returns>
        BList<long?> ParseSqlValueList(Domain xp)
        {
            var r = BList<long?>.Empty;
            Domain ei;
            switch (xp.kind)
            {
                case Sqlx.ARRAY:
                case Sqlx.SET:
                case Sqlx.MULTISET:
                    ei = xp.elType??throw new PEException("PE50710");
                    break;
                case Sqlx.CONTENT:
                    for (; ; )
                    {
                        var v = ParseSqlValue(xp);
                        cx.Add(v);
                        r += v.defpos;
                        if (tok == Sqlx.COMMA)
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
                var v = (ei.Length>0)?
                    ParseSqlRow(ei) :
                    ParseSqlValue(ei);
                cx.Add(v);
                if (tok == Sqlx.AS)
                {
                    Next();
                    var d = ParseSqlDataType();
                    v = new SqlTreatExpr(LexPos().dp, v, d); //.Needs(v);
                    cx.Add(v);
                }
                r += v.defpos;
                if (tok == Sqlx.COMMA)
                    Next();
                else
                    break;
            }
            return r;
        }
        public SqlRow ParseSqlRow(Domain xp)
        {
            var llp = LexPos();
            Mustbe(Sqlx.LPAREN);
            var lk = BList<long?>.Empty;
            var i = 0;
            for (var b = xp.rowType.First(); b != null && i < xp.display; b = b.Next(), i++)
                if (b.value() is long p && xp.representation[p] is Domain dt)
                {
                    if (i > 0)
                        Mustbe(Sqlx.COMMA);
                    var v = ParseSqlValue(dt);
                    cx.Add(v);
                    lk += v.defpos;
                }
            Mustbe(Sqlx.RPAREN);
            return (SqlRow)cx.Add(new SqlRow(llp.dp, cx, xp, lk));
        }
        /// <summary>
        /// Parse an SqlRow
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="sql">The string to parse</param>
        /// <param name="result">the expected obs type</param>
        /// <returns>the SqlRow</returns>
        public SqlValue ParseSqlValueList(string sql,Domain xp)
        {
            lxr = new Lexer(cx,sql);
            tok = lxr.tok;
            if (tok == Sqlx.LPAREN)
                return ParseSqlRow(xp);         
            return ParseSqlValueEntry(xp,false);
        }
        /// <summary>
        /// Get a document item for ParseGraphExp
        /// </summary>
        /// <param name="v">The document being constructed</param>
        (SqlValue,SqlValue) GetDocItem()
        {
            Ident k = new(this);
            var ip = lxr.Position;
            var vo = (lxr.val is TChar tc) ? tc.value : k.ident;
            Mustbe(Sqlx.ID);
            Mustbe(Sqlx.COLON);
            Ident q = new(this);
            var eq = (lxr.val is TChar ec) ? ec.value : q.ident;
            if (tok == Sqlx.ID && !cx.defs.Contains(eq))
                cx.Add(new SqlValue(q, cx, Domain.Char));
            return ((SqlValue)cx.Add(new SqlValue(k,cx,Domain.Char)),
                (SqlValue)cx.Add(ParseSqlValue(Domain.Content)));
        }
        /// <summary>
        /// Parse a document array
        /// </summary>
        /// <returns>the SqlDocArray</returns>
        public SqlValue ParseSqlDocArray()
        {
            var v = new SqlRowArray(LexPos().dp,BTree<long, object>.Empty);
            cx.Add(v);
            Next();
            if (tok != Sqlx.RBRACK)
                v += ParseSqlRow(Domain.Content);
            while (tok == Sqlx.COMMA)
            {
                Next();
                v+=ParseSqlRow(Domain.Content);
            }
            Mustbe(Sqlx.RBRACK);
            return (SqlValue)cx.Add(v);
        }
        /// <summary>
        /// Parse an XML value
        /// </summary>
        /// <returns>the SqlValue</returns>
        public SqlValue ParseXmlValue()
        {
            Mustbe(Sqlx.LSS);
            var e = GetName();
            var v = new SqlXmlValue(LexPos().dp,e,SqlNull.Value,BTree<long, object>.Empty);
            cx.Add(v);
            while (tok!=Sqlx.GTR && tok!=Sqlx.DIVIDE)
            {
                var a = GetName();
                v+=(a, (SqlValue)cx.Add(ParseSqlValue(Domain.Char)));
            }
            if (tok != Sqlx.DIVIDE)
            {
                Next(); // GTR
                if (tok == Sqlx.ID)
                {
                    var st = lxr.start;
                    Next();
                    if (tok == Sqlx.ID || tok != Sqlx.LSS)
                    {
                        while (tok != Sqlx.LSS)
                            Next();
                        v+=(SqlXmlValue._Content,new SqlLiteral(LexPos().dp,
                            new TChar(new string(lxr.input, st, lxr.start - st))));
                    }
                    else
                    {
                        lxr.PushBack(Sqlx.ANY);
                        lxr.pos = lxr.start;
                        v += (SqlXmlValue._Content, ParseSqlValueItem(Domain.Char, false));
                    }
                }
                else
                    while (tok != Sqlx.DIVIDE) // tok should Sqlx.LSS
                        v+=(SqlXmlValue)cx.Add(ParseXmlValue());
                Mustbe(Sqlx.DIVIDE);
                var ee = GetName();
                if (e.prefix != ee.prefix || e.keyname != ee.keyname)
                    throw new DBException("2200N", ee).ISO();
            }
            Mustbe(Sqlx.GTR);
            return (SqlValue)cx.Add(v);
        }
        /// <summary>
        /// Parse an XML name
        /// </summary>
        /// <returns>the XmlName</returns>
        public SqlXmlValue.XmlName GetName()
        {
            var e = new SqlXmlValue.XmlName(new string(lxr.input, lxr.start, lxr.pos - lxr.start));
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.COLON)
            {
                Next();
                e=new SqlXmlValue.XmlName(new string(lxr.input, lxr.start, lxr.pos - lxr.start),
                    e.keyname);
                Mustbe(Sqlx.ID);
            }
            return e;
        }
        static CTree<long, bool> _Deps(BList<long?> bl)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = bl.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    r += (p, true);
            return r;
        }
/*        Executable ParseInteractStatement()
        {
            var m = (SqlValue)cx.Add(ParseSqlValue(Domain.Char));
            Mustbe(Sqlx.SET);
            var id = new Ident(this);
            Mustbe(Sqlx.ID);
            Executable? e=null;
            if (tok==Sqlx.BEGIN)
                e = (Executable)cx.Add(ParseCompoundStatement(Domain.Null, ""));
            return new InteractStatement(cx, m.defpos, id.iix.dp, e?.defpos??-1L);
        } */
    }
}