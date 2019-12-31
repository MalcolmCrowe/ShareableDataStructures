using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
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
    /// An LL(1) Parser deals with all Sql statements from various sources.
    /// 
    /// Entries to the Parser from other files are marked internal or public
    /// and can be distinguished by the fact that they create a new Scanner()
    /// 
    /// Most of the grammar below comes from SQL2003
    /// the comments are extracts from Pyrrho.doc synopsis of syntax
    /// 
    /// Many SQL statements parse to Executables (can be placed in stored procedures etc)
    /// Can then be executed imediately depending on parser settings.
    /// 
    /// Some constructs get parsed during database Load(): these should never try to change the schema
    /// or make other changes. db.parse should only be Obey within a transaction.
    /// This means that (for now at least) stored executable code (triggers, procedures) should
    /// never attempt schema changes. 
    /// </summary>
	internal class Parser
    {
        /// <summary>
        /// Both tr and cx have their idea of role. Use tr.role to get information
        /// about Tables and cx.role to store and get information about TabelReferences
        /// </summary>
        public Database db; // immutable shareable: we will update this and return new version
        public Context cx; // updatable: has volatile query rowTypes.
        /// <summary>
        /// The lexer breaks the input into tokens for us. During parsing
        /// lxr.val is the object corresponding to the current token,
        /// lxr.start and lxr.pos delimit the current token
        /// </summary>
		Lexer lxr;
        /// <summary>
        /// The current token
        /// </summary>
		Sqlx tok;
        /// <summary>
        /// Identifier for parsed entities: e.g. dbix:defpos or cmd
        /// </summary>
        string id;
        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="t">The transaction</param>
        public Parser(Database db,Context c=null)
        {
            this.db = db.Transact(db.nextTid, db.source);
            cx = c??new Context(db);
            id = ""+cx?.cxid;
        }
        public Parser(Database d,Ident src)
        {
            db = d+(Database._ExecuteStatus,ExecuteStatus.Parse);
            cx = new Context(db);
            lxr = new Lexer(src.ident, src.iix);
            tok = lxr.tok;
            id = db.name+db.dbg;
        }
        /// <summary>
        /// Move to the next token
        /// </summary>
        /// <returns></returns>
		Sqlx Next()
        {
            tok = lxr.Next();
            return tok;
        }
        /// <summary>
        /// Match any of a set of token types
        /// </summary>
        /// <param name="s">the list of token types</param>
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
        /// <param name="t">the list of token types</param>
        /// <returns>the token that matched</returns>
		Sqlx Mustbe(params Sqlx[] t)
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
        /// The type of database maodification that may occur is determined by tr.parse.
        /// </summary>
        /// <param name="sql">the input</param>
        /// <param name="rt">the desired result type (default is Domain.Null)</param>
        /// <returns>The modified Database and the new uid highwatermark </returns>
        public (Database,long) ParseSql(string sql,DBObject rt=null)
        {
            lxr = new Lexer(sql,db.lexeroffset);
            tok = lxr.tok;
            var e = ParseSqlStatement(rt);
            cx.result = e.defpos;
            if (e.type == Executable.Type.Commit)
                return db.Commit(cx);
            if (tok == Sqlx.SEMICOLON)
                Next();
            if (tok != Sqlx.EOF)
            {
                string ctx = new string(lxr.input, lxr.start, lxr.pos - lxr.start);
                throw new DBException("42000", ctx).ISO();
            }
            if (lxr.Position>Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid,db.nextTid+sql.Length);
            return (db, db.nextTid);
        }
        /// <summary>
        /// Add the parameters to a prepared statement
        /// </summary>
        /// <param name="pre">The object with placeholders</param>
        /// <param name="s">The parameter strings concatenated by |</param>
        /// <returns>The modified database and the new uid highwatermark</returns>
        public (Database,long) ParseSql(PreparedStatement pre,string s)
        {
            cx.Add(pre);
            cx.top = pre.defpos;
            lxr = new Lexer(s, db.lexeroffset);
            tok = lxr.tok;
            var b = pre.qMarks.First();
            for (;b!=null && tok!=Sqlx.EOF;b=b.Next())
            {
                var v = lxr.val;
                var lp = lxr.Position;
                if (Match(Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL))
                {
                    Sqlx tk = tok;
                    Next();
                    if (tok == Sqlx.CHARLITERAL)
                    {
                        Next();
                        v = new SqlDateTimeLiteral(lp,
                            new Domain(lp, tk, BTree<long, object>.Empty), v.ToString()).val;
                    }
                }
                else
                    Mustbe(Sqlx.BLOBLITERAL, Sqlx.NUMERICLITERAL, Sqlx.REALLITERAL,
                        Sqlx.CHARLITERAL, Sqlx.INTEGERLITERAL, Sqlx.DOCUMENTLITERAL);
                cx.values += (b.value().defpos, v);
                Mustbe(Sqlx.SEMICOLON);
            }
            if (!(b == null && tok == Sqlx.EOF))
                throw new DBException("33001");
            db = pre.target.Obey((Transaction)db, cx);
            return (db, db.nextTid);
        }
        /// <summary>
        ///SqlStatement = 	Rdf
        ///     |	Alter
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
        /// <returns>The Executable result of the Parse</returns>
        public Executable ParseSqlStatement(DBObject rt=null)
        {
            int st = lxr.start;
            var oi = rt as ObInfo ?? ObInfo.Any;
            Executable e = null;
            //            Match(Sqlx.RDF);
            switch (tok)
            {
                case Sqlx.ALTER: e = ParseAlter(); break;
                case Sqlx.CALL: e = ParseCallStatement(); break;
                case Sqlx.COMMIT: e = ParseCommit();  break;
                case Sqlx.CREATE: e = ParseCreateClause(); break;
                case Sqlx.DELETE: e = ParseSqlDelete(); break;
                case Sqlx.DROP: e = ParseDropStatement(); break;
                case Sqlx.GRANT: e = ParseGrant(); break;
                case Sqlx.HTTP: e = ParseHttpREST(rt); break;
                case Sqlx.INSERT: e = ParseSqlInsert(); break;
                case Sqlx.REVOKE: e = ParseRevoke(); break;
                case Sqlx.ROLLBACK:
                    Next();
                    if (Match(Sqlx.WORK))
                        Next();
                    db.Rollback(new DBException("0000").ISO());
                    break;
                case Sqlx.SELECT: e = ParseCursorSpecification(oi); break;
                case Sqlx.SET: e = ParseSqlSet(); break;
                case Sqlx.TABLE: e = ParseCursorSpecification(oi); break;
                case Sqlx.UPDATE: e = ParseSqlUpdate(); break;
                case Sqlx.VALUES: e = ParseCursorSpecification(oi); break;
                //    case Sqlx.WITH: e = ParseCursorSpecification(); break;
                case Sqlx.EOF: return null; // whole input is a comment
                default:
                    object ob = lxr.val;
                    if (ob == null || ((TypedValue)ob).IsNull)
                        ob = new string(lxr.input, lxr.start, lxr.pos - lxr.start);
                    throw new DBException("42000", ob).ISO();
            }
            return cx.exec = (Executable)cx.Add(e);
        }
        Executable ParseCommit()
        {
            Next();
            if (tok == Sqlx.WORK)
                Next();
            return new CommitStatement(lxr.Position);
        }
        private Executable ParseHttpREST(DBObject rt)
        {
            Sqlx t = Next();
            SqlValue us = null;
            SqlValue pw = null;
            if (!Match(Sqlx.ADD, Sqlx.UPDATE, Sqlx.DELETE))
            {
                us = ParseSqlValue(Domain.Char);
                if (Match(Sqlx.COLON))
                {
                    Next();
                    pw = ParseSqlValue(Domain.Char);
                }
            }
            HttpREST h = null;
            var lp = lxr.Position;
            switch (t)
            {
                case Sqlx.ADD: h = new HttpREST(lp,"POST", us, pw); break;
                case Sqlx.UPDATE: h = new HttpREST(lp, "UPDATE", us, pw); break;
                case Sqlx.DELETE: h = new HttpREST(lp,"DELETE", us, pw); break;
            }
                Next();
                h+= (HttpREST.Url,ParseSqlValue(Domain.Char).Eval((Transaction)db,cx));
            cx = new Context(cx, h);
            try
            { 
                if (t != Sqlx.DELETE)
                {
                    var e = ParseProcedureStatement(Domain.Content);
                    cx=cx.next;
                    return (Executable)cx.Add(e);
                }
                h +=(HttpREST.Mime,"text/xml");
                if (tok == Sqlx.AS)
                {
                    Next();
                    h += (HttpREST.Mime,lxr.val.ToString());
                    Mustbe(Sqlx.CHARLITERAL);
                }
                if (Match(Sqlx.WHERE))
                {
                    Next();
                    h+=(HttpREST.Where, ParseSqlValue(Domain.Bool));
                }
            }
            catch (Exception e) { throw e; }
            finally { cx = cx.next; }
            return (Executable)cx.Add(h);
        }
        byte MustBeLevelByte()
        {
            byte lv = 0;
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
                while (tok==Sqlx.ID)
                {
                    gps +=(((TChar)lxr.val).value, true);
                    Next();
                }
            }
            var rfs = BTree<string, bool>.Empty;
            if (tok == Sqlx.REFERENCES)
            {
                Next();
                while (tok == Sqlx.ID)
                {
                    rfs +=(((TChar)lxr.val).value, true);
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
        /// <returns>the executable type</returns>
        public Executable ParseGrant()
        {
            Executable e = null;
            Next();
            if (Match(Sqlx.SECURITY))
            {
                Next();
                var lv = MustBeLevel();
                Mustbe(Sqlx.TO);
                var nm = lxr.val;
                var lp = lxr.Position;
                Mustbe(Sqlx.ID);
                var usr = db.GetObject(nm.ToString()) as User ??
                    throw new DBException("42135", nm.ToString());
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    tr += new Clearance(usr.defpos, lv, tr);
                return e; // still null
            }
            else if (Match(Sqlx.PASSWORD))
            {
                TypedValue pwd = new TChar("");
                Role irole = null;
                Next();
                if (!Match(Sqlx.FOR) && !Match(Sqlx.TO))
                {
                    pwd = lxr.val;
                    Next();
                }
                if (Match(Sqlx.FOR))
                {
                    Next();
                    var rid = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    irole = db.GetObject(rid.ident) as Role??
                        throw new DBException("42135", rid);
                }
                Mustbe(Sqlx.TO);
                var lp = lxr.Position;
                var grantees = ParseGranteeList(null);
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                {
                    var irolepos = -1L;
                    if (irole != null)
                    {
                        tr = tr.AccessRole(true, new string[] { irole.name }, grantees, false);
                        irolepos = irole.defpos;
                    }
                    for (var i = 0; i < grantees.Length; i++)
                    {
                        var us = grantees[i];
                        tr += new Authenticate(us.defpos, pwd.ToString(), irolepos, tr);
                    }
                    db = tr;
                }
                return e;// e.SPop(lxr); // e is still null of course
            }
            Match(Sqlx.OWNER, Sqlx.USAGE);
            if (Match(Sqlx.ALL, Sqlx.SELECT, Sqlx.INSERT, Sqlx.DELETE, Sqlx.UPDATE, Sqlx.REFERENCES, Sqlx.OWNER, Sqlx.TRIGGER, Sqlx.USAGE, Sqlx.EXECUTE))
            {
                e = new Executable(lxr.Position, Executable.Type.Grant);
                var priv = ParsePrivileges();
                Mustbe(Sqlx.ON);
                var (ob,n) = ParseObjectName();
                long pob = ob?.defpos ?? 0;
                Mustbe(Sqlx.TO);
                var grantees = ParseGranteeList(priv);
                bool opt = ParseGrantOption();
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    tr.AccessObject(true, priv, pob, grantees, opt);
            }
            else
            {
                e = new Executable(lxr.Position, Executable.Type.GrantRole);
                var roles = ParseRoleNameList();
                Mustbe(Sqlx.TO);
                var grantees = ParseGranteeList(new PrivNames[] { new PrivNames(Sqlx.USAGE) });
                bool opt = ParseAdminOption();
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr.AccessRole(true, roles, grantees, opt);
            }
            return (Executable)cx.Add(e);
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
            var n = lxr.val.ToString();
            Mustbe(Sqlx.ID);
            DBObject ob;
            switch (kind)
            {
                case Sqlx.TABLE: 
                case Sqlx.DOMAIN:
                case Sqlx.VIEW:
                case Sqlx.ENTITY:
                case Sqlx.TRIGGER:
                case Sqlx.TYPE: ob = db.GetObject(n) ??
                        throw new DBException("42135",n);
                    break;
                case Sqlx.CONSTRUCTOR: 
                case Sqlx.FUNCTION: 
                case Sqlx.PROCEDURE:
                    {
                        var a = ParseArity();
                        ob = db.GetProcedure(n, a) ??
                        throw new DBException("42108", n+"$"+a);
                        break;
                    }
                case Sqlx.INSTANCE: 
                case Sqlx.STATIC: 
                case Sqlx.OVERRIDING: 
                case Sqlx.METHOD:
                    {
                        int arity = ParseArity();
                        Mustbe(Sqlx.FOR);
                        var tp = lxr.val.ToString();
                        Mustbe(Sqlx.ID);
                        var ut = db.GetObject(tp) as UDType ??
                            throw new DBException("42119", tp);
                        ob = ut.methods[n][arity];
                        break;
                    }
                default:
                    throw new DBException("42115", kind).Mix();
            }
            if (ob == null && kind != Sqlx.DATABASE)
                throw new DBException("42135", n).Mix();
            return (cx.Add(ob),n);
        }
        /// <summary>
        /// used in ObjectName. All that is needed here is the number of parameters
        /// because SQL is not strongly typed enough to distinguish actual parameter types
        /// '('Type, {',' Type }')'
        /// </summary>
        /// <returns>the number of parameters</returns>
		int ParseArity()
        {
            int n = 0;
            List<Domain> fs = new List<Domain>();
            if (tok == Sqlx.LPAREN)
            {
                Next();
                if (tok == Sqlx.RPAREN)
                {
                    Next();
                    return 0;
                }
                fs.Add(ParseSqlDataType().domain);
                n++;
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    fs.Add(ParseSqlDataType().domain);
                    n++;
                }
                Mustbe(Sqlx.RPAREN);
            }
            return n;
        }
        /// <summary>
		/// ObjectPrivileges = ALL PRIVILEGES | Action { ',' Action } .
        /// </summary>
        /// <returns>The list of privileges</returns>
		PrivNames[] ParsePrivileges()
        {
            var r = new List<PrivNames>();
            if (tok == Sqlx.ALL)
            {
                Next();
                Mustbe(Sqlx.PRIVILEGES);
                return null;
            }
            r.Add(ParsePrivilege());
            while (tok == Sqlx.COMMA)
            {
                Next();
                r.Add(ParsePrivilege());
            }
            return r.ToArray();
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
        /// <returns>A singleton privilege (list of one item)</returns>
		PrivNames ParsePrivilege()
        {
            var r = new PrivNames(tok);
            var s = new List<string>();
            Mustbe(Sqlx.SELECT, Sqlx.DELETE, Sqlx.INSERT, Sqlx.UPDATE,
                Sqlx.REFERENCES, Sqlx.USAGE, Sqlx.TRIGGER, Sqlx.EXECUTE, Sqlx.OWNER);
            if ((r.priv == Sqlx.UPDATE || r.priv == Sqlx.REFERENCES || r.priv == Sqlx.SELECT || r.priv == Sqlx.INSERT) && tok == Sqlx.LPAREN)
            {
                Next();
                s.Add(lxr.val.ToString());
                Mustbe(Sqlx.ID);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    s.Add(lxr.val.ToString());
                    Mustbe(Sqlx.ID);
                }
                Mustbe(Sqlx.RPAREN);
            }
            r.names = s.ToArray();
            return r;
        }
        /// <summary>
		/// GranteeList = PUBLIC | Grantee { ',' Grantee } .
        /// </summary>
        /// <param name="priv">the list of privieges to grant</param>
        /// <returns>the updated database objects</returns>
		DBObject[] ParseGranteeList(PrivNames[] priv)
        {
            if (Match(Sqlx.PUBLIC))
            {
                Next();
                return null;
            }
            var r = new List<DBObject>
            {
                ParseGrantee(priv)
            };
            while (tok == Sqlx.COMMA)
            {
                Next();
                r.Add(ParseGrantee(priv));
            }
            return r.ToArray();
        }
        /// <summary>
        /// helper for non-reserved words
        /// </summary>
        /// <returns>if we match a method mode</returns>
        bool MethodModes()
        {
            return Match(Sqlx.INSTANCE, Sqlx.OVERRIDING, Sqlx.CONSTRUCTOR);
        }
        internal Role GetRole(string n)
        {
            return (db.roles.Contains(n)) ? (Role)db.objects[db.roles[n]] : null;
        }
        /// <summary>
		/// Grantee = 	[USER] id
		/// 	|	ROLE id . 
        /// </summary>
        /// <param name="priv">the list of privileges</param>
        /// <returns>the updated grantee</returns>
		DBObject ParseGrantee(PrivNames[] priv)
        {
            Sqlx kind = Sqlx.USER;
            if (Match(Sqlx.USER))
                Next();
            else if (Match(Sqlx.ROLE))
            {
                kind = Sqlx.ROLE;
                Next();
            }
            var n = lxr.val.ToString();
            var lp = lxr.Position;
            Mustbe(Sqlx.ID);
            DBObject ob;
            switch (kind)
            {
                case Sqlx.USER:
                    {
                        ob = GetRole(n) as User;
                        if ((ob == null || ob.defpos == -1) && db is Transaction tr)
                        {
                            db = tr+new PUser(n, tr);
                            ob = GetRole(n);
                        }
                        break;
                    }
                case Sqlx.ROLE: ob = GetRole(n); break;
                default: throw new DBException("28101").Mix();
            }
            if (ob == null && (priv == null || priv.Length != 1 || priv[0].priv != Sqlx.OWNER))
                throw new DBException("28102", kind, n).Mix();
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
        /// <returns>The list of Roles</returns>
		string[] ParseRoleNameList()
        {
            var r = new List<string>();
            if (tok == Sqlx.ID)
            {
                r.Add(lxr.val.ToString());
                Next();
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    r.Add(lxr.val.ToString());
                    Mustbe(Sqlx.ID);
                }
            }
            return r.ToArray();
        }
        /// <summary>
		/// Revoke = 	REVOKE [GRANT OPTION FOR] Privileges FROM GranteeList
		/// 	|	REVOKE [ADMIN OPTION FOR] Role_id { ',' Role_id } FROM GranteeList .
        /// Privileges = ObjectPrivileges ON ObjectName .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseRevoke()
        {
            var e = new Executable(lxr.Position, Executable.Type.Revoke);
            Next();
            Sqlx opt = ParseRevokeOption();
            if (tok == Sqlx.ID)
            {
                var er = new Executable(lxr.Position, Executable.Type.RevokeRole);
                var priv = ParseRoleNameList();
                Mustbe(Sqlx.FROM);
                var grantees = ParseGranteeList(new PrivNames[0]);
                if (opt == Sqlx.GRANT)
                    throw new DBException("42116").Mix();
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db =tr.AccessRole(false, priv, grantees, opt == Sqlx.ADMIN);
            }
            else
            {
                if (opt == Sqlx.ADMIN)
                    throw new DBException("42117").Mix();
                var priv = ParsePrivileges();
                Mustbe(Sqlx.ON);
                var (ob,n) = ParseObjectName();
                Mustbe(Sqlx.FROM);
                var grantees = ParseGranteeList(priv);
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    tr.AccessObject(false, priv, ob.defpos, grantees, (opt == Sqlx.GRANT));
            }
            return (Executable)cx.Add(e);
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
		Executable ParseCallStatement(DBObject ob=null)
        {
            Next();
            Executable e = ParseProcedureCall(ob);
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(e,cx);
            return (Executable)cx.Add(e);
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
        /// |   CREATE INDEX Table_id id [UNIQUE] Cols.
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseCreateClause()
        {
            Next();
            MethodModes();
            Match(Sqlx.TEMPORARY, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE, Sqlx.DOMAIN);
            if (Match(Sqlx.ORDERING))
                return (Executable)cx.Add(ParseCreateOrdering());
            if (Match(Sqlx.XMLNAMESPACES))
                return (Executable)cx.Add(ParseCreateXmlNamespaces());
            if (tok == Sqlx.PROCEDURE || tok == Sqlx.FUNCTION)
            {
                bool func = tok == Sqlx.FUNCTION;
                Next();
                return (Executable)cx.Add(ParseProcedureClause(func, Sqlx.CREATE));
            }
            else if (Match(Sqlx.OVERRIDING, Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.METHOD))
                return (Executable)cx.Add(ParseMethodDefinition());
            else if (tok == Sqlx.TABLE || tok == Sqlx.TEMPORARY)
            {
                if (tok == Sqlx.TEMPORARY)
                {
                    Role au = GetRole("Temp");
                    if (au == null)
                        throw new DBException("3D001", "Temp").Mix();
                    cx = new Context(cx, au, db.user);
                    Next();
                }
                Mustbe(Sqlx.TABLE);
                return (Executable)cx.Add(ParseCreateTable());
            }
            else if (tok == Sqlx.TRIGGER)
            {
                Next();
                return (Executable)cx.Add(ParseTriggerDefClause());
            }
            else if (tok == Sqlx.DOMAIN)
            {
                Next();
                return (Executable)cx.Add(ParseDomainDefinition());
            }
            else if (tok == Sqlx.TYPE)
            {
                Next();
                return (Executable)cx.Add(ParseTypeClause());
            }
            else if (tok == Sqlx.ROLE)
            {
                var cr = new Executable(lxr.Position, Executable.Type.CreateRole);
                Next();
                var id = lxr.val.ToString();
                var lp = lxr.Position;
                Mustbe(Sqlx.ID);
                TypedValue o = new TChar("");
                if (Match(Sqlx.CHARLITERAL))
                {
                    o = lxr.val;
                    Next();
                }
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr +new PRole(id, o.ToString(), tr);
                return (Executable)cx.Add(cr);
            }
            else if (tok == Sqlx.VIEW)
                return (Executable)cx.Add(ParseViewDefinition());
            throw new DBException("42118", tok).Mix();
        }
        /// <summary>
        /// GET option here is Pyrrho shortcut, needs third syntax for ViewDefinition
        /// ViewDefinition = id [ViewSpecification] AS (QueryExpression|GET) {TableMetadata} .
        /// ViewSpecification = Cols 
        ///       | OF id 
        ///       | OF '(' id Type {',' id Type} ')' .
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseViewDefinition()
        {
            var ct = new Executable(lxr.Position, Executable.Type.CreateTable);
            Next();
            var id = lxr.val.ToString();
            var lp = lxr.Position;
            Mustbe(Sqlx.ID);
            var t = ObInfo.Any;
            long structdef = -1;
            string tn = null; // explicit view type
            Table ut = null;
            bool schema = false; // will record whether a schema for GET follows
            if (Match(Sqlx.LPAREN))
            {
                Next();
                var cd = BList<SqlValue>.Empty;
                for (var i=0;;i++ )
                {
                    var n = lxr.val.ToString();
                    Mustbe(Sqlx.ID);
                    cd += new SqlValue(lxr.offset + lxr.start, n);
                    if (Mustbe(Sqlx.COMMA, Sqlx.RPAREN) == Sqlx.RPAREN)
                        break;
                }
                t +=(ObInfo.Columns,cd);
            }
            if (Match(Sqlx.OF))
            {
                Next();
                if (Match(Sqlx.LPAREN)) // inline type def
                    t = ParseRowTypeSpec(Sqlx.VIEW, lxr.start);
                else
                {
                    tn = lxr.val.ToString();
                    lp = lxr.Position;
                    Mustbe(Sqlx.ID);
                    t = db.objects[db.role.dbobjects[tn]] as ObInfo??
                        throw new DBException("42119", tn, "").Mix();
                }
                schema = true;
            }
            Mustbe(Sqlx.AS);
            Context old = cx;
            cx = new Context(cx);
            int st = lxr.start;
            var rest = Match(Sqlx.GET);
            QueryExpression qe = null;
            if (!rest)
                qe = ParseQueryExpression(ObInfo.Any);
            else
            {
                if (!schema)
                    throw new DBException("42168").Mix();
                Next();
                if (tok == Sqlx.USING)
                {
                    Next();
                    var idu = lxr.val.ToString();
                    Mustbe(Sqlx.ID);
                    ut = db.GetObject(idu) as Table ??
                        throw new DBException("42107", idu);
                }
            }
            cx = old;
            PView pv = null;
            var tr = db as Transaction;
            if (db.parse == ExecuteStatus.Obey)
            {
                if (rest)
                {
                    if (ut == null)
                        pv = new PRestView(id, structdef, tr);
                    else
                        pv = new PRestView2(id, structdef, ut.defpos, tr);
                }
                else
                    pv = new PView(id, qe, tr);
                tr+= pv;
            }
            var ob = (DBObject)db.objects[db.loadpos];
            lp = lxr.Position;
            var md = ParseMetadata(db, ob, -1, Sqlx.ADD);
            if (md != null && db.parse == ExecuteStatus.Obey)
                tr+=new PMetadata(id, -1, pv.ppos, md.description,
                    md.iri,md.seq+1,md.flags, tr);
            db = tr??db;
            return (Executable)cx.Add(ct);
        }
        /// <summary>
        /// Parse the CreateXmlNamespaces syntax
        /// </summary>
        /// <returns>the executable</returns>
        private Executable ParseCreateXmlNamespaces()
        {
            Next();
            var lp = lxr.Position;
            var ns = (XmlNameSpaces)ParseXmlNamespaces();
            cx.nsps += ns.nsps;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
            {
                for (var s = ns.nsps.First(); s != null; s = s.Next())
                    db = tr + new Namespace(s.key(), s.value(), tr);
            }
            return (Executable)cx.Add(ns);
        }
        /// <summary>
        /// Parse the Create ordering syntax:
        /// FOR Domain_id (EQUALS|ORDER FULL) BY (STATE|(MAP|RELATIVE) WITH Func_id)  
        /// </summary>
        /// <returns>the executable</returns>
        private Executable ParseCreateOrdering()
        {
            var co = new Executable(lxr.Position, Executable.Type.CreateOrdering);
            Next();
            Mustbe(Sqlx.FOR);
            var n = new Ident(lxr);
            Mustbe(Sqlx.ID);
            UDType ut = db.GetObject(n.ident) as UDType??
                throw new DBException("42133", n).Mix();
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
                fl = fl | OrderCategory.State;
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr + new Ordering(ut.defpos, -1L, fl, tr);
            }
            else
            {
                fl = fl | ((smr == Sqlx.RELATIVE) ? OrderCategory.Relative : OrderCategory.Map);
                Mustbe(Sqlx.WITH);
                var lp = lxr.Position;
                var (fob,nf) = ParseObjectName();
                Procedure func = fob as Procedure;
                if (smr == Sqlx.RELATIVE && func.arity != 2)
                    throw new DBException("42154", nf).Mix();
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr + new Ordering(ut.defpos, func.defpos, fl, tr);
            }
            return (Executable)cx.Add(co);
        }
        /// <summary>
        /// Cols =		'('id { ',' id } ')'.
        /// </summary>
        /// <returns>a list of Ident</returns>
        Ident[] ParseIDList()
        {
            bool b = (tok == Sqlx.LPAREN);
            if (b)
                Next();
            var r = new List<Ident>
            {
                ParseIdent()
            };
            while (tok == Sqlx.COMMA)
            {
                Next();
                r.Add(ParseIdent());
            }
            if (b)
                Mustbe(Sqlx.RPAREN);
            return r.ToArray();
        }
        /// <summary>
		/// Cols =		'('ColRef { ',' ColRef } ')'.
        /// </summary>
        /// <returns>a list of coldefpos</returns>
		CList<TableColumn> ParseColsList(Table tb)
        {
            var r = CList<TableColumn>.Empty;
            bool b = (tok == Sqlx.LPAREN);
            if (b)
                Next();
            r+=ParseColRef(tb);
            while (tok == Sqlx.COMMA)
            {
                Next();
                r+=ParseColRef(tb);
            }
            if (b)
                Mustbe(Sqlx.RPAREN);
            return r;
        }
        /// <summary>
        /// ColRef = id { '.' id } .
        /// </summary>
        /// <returns>+- seldefpos</returns>
        TableColumn ParseColRef(Table tb)
        {
            if (tok == Sqlx.PERIOD)
            {
                Next();
                var pn = lxr.val;
                Mustbe(Sqlx.ID);
                var pd = db.objects[tb.applicationPS] as PeriodDef;
                var pi = db.role.obinfos[pd.defpos] as ObInfo;
                if (pd == null || pi.name != pn.ToString())
                    throw new DBException("42162", pn).Mix();
                return (PeriodDef)cx.Add(pd);
            }
            // We will raise an exception if the first one does not exist
            var id = new Ident(lxr);
            var rt = db.role.obinfos[tb.defpos] as ObInfo;
            var sc = rt.ColFor(id.ident) as SqlCol ??
                throw new DBException("42112", id.ident).Mix();
            var tc = sc.tableCol;
            Mustbe(Sqlx.ID);
            // We will construct paths as required for any later components
            while (tok == Sqlx.DOT)
            {
                Next();
                var pa = new Ident(lxr);
                Mustbe(Sqlx.ID);
                tc = new ColumnPath(pa.iix,pa.ident,tc,db); // returns a (child)TableColumn for non-documents
                long dm = -1;
                if (tok == Sqlx.AS)
                {
                    Next();
                    var at = ParseSqlDataType().domain;
                    if (db.types.Contains(at))
                        at = db.types[at];
                    else if (db is Transaction tr0)
                        db = tr0 + at;
                    tc += (DBObject._Domain, at);
                }
                if (!rt.map.Contains(pa.ident) && db is Transaction tr) // create a new path 
                {
                    var pp = new PColumnPath(sc.defpos, pa.ToString(), dm, tr);
                    db = tr+pp;
                }
            }
            return (TableColumn)cx.Add(tc);
        }
        /// <summary>
        /// id [UNDER id] AS Representation [ Method {',' Method} ]
		/// Representation = | StandardType 
        ///             | (' Member {',' Member }')' .
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseTypeClause()
        {
            var ct = new Executable(lxr.Position, Executable.Type.CreateType);
            var typename = new Ident(lxr);
            Ident undername = null;
            var tr = db as Transaction?? throw new DBException("2F003");
            Mustbe(Sqlx.ID);
            if (Match(Sqlx.UNDER))
            {
                Next();
                undername = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            long underdef = -1L;
            if (undername != null)
            {
                var udm = db.GetObInfo(undername.ident)?.domain ??
                    throw db.Exception("42119", undername).Pyrrho();
                underdef = udm.defpos;
            }
            if (tok == Sqlx.AS)
            {
                Next();
                var dt = Domain.Null;
                if (tok == Sqlx.LPAREN)
                {
                    var lp = lxr.start;
                    tr +=ParseRowTypeSpec(Sqlx.TYPE, lp);
                    dt = new Domain(lp, Sqlx.TYPE, BTree<long, object>.Empty);
                }
                else
                    dt = ParseStandardDataType() ??
                        throw new DBException("42161", "StandardType", lxr.val.ToString()).Mix();
                if (Match(Sqlx.RDFLITERAL))
                {
                    RdfLiteral rit = (RdfLiteral)lxr.val;
                    dt += (Domain.Iri,rit.dataType.iri);
                    Next();
                }
                PType pt = null;
                if (db.parse == ExecuteStatus.Obey)
                {
                    if (dt.iri != null)
                        pt = new PType1(typename.ident, underdef, dt, tr);
                    else
                        pt = new PType(typename.ident, underdef, dt, tr);
                    tr += pt;
                }
                while (Match(Sqlx.CHECK, Sqlx.CONSTRAINT))
                {
                    PCheck pc = ParseCheckConstraint(pt);
                    if (db.parse == ExecuteStatus.Obey)
                        tr+=pc;
                }
            }
            db = tr;
            MethodModes();
            if (Match(Sqlx.OVERRIDING, Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.METHOD))
            {
                var tb = (Domain)db.GetObject(typename.ident) ??
                    throw db.Exception("42133", typename.ident);
                ParseMethodHeader(typename);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    ParseMethodHeader(typename);
                }
            }
            return (Executable)cx.Add(ct);
        }
        /// <summary>
		/// Method =  	MethodType METHOD id '(' Parameters ')' [RETURNS Type] [FOR id].
		/// MethodType = 	[ OVERRIDING | INSTANCE | STATIC | CONSTRUCTOR ] .
        /// </summary>
        /// <returns>the methodname parse class</returns>
        MethodName ParseMethod(Ident type)
        {
            MethodName mn = new MethodName
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
            mn.mname = new Ident(lxr);
            mn.name = mn.mname.ident; // without the $ which we are just about to add
            Mustbe(Sqlx.ID);
            int st = lxr.start;
            mn.arity = 0;
            mn.ins = ParseParameters(mn.mname);
            mn.arity = (int)mn.ins.Count;
            mn.retType = ParseReturnClause(mn.mname);
            mn.mname.ident = mn.mname.ident + "$" + mn.arity;
            mn.signature = new string(lxr.input,st, lxr.start - st);
            if (tok == Sqlx.FOR)
            {
                Next();
                type = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            mn.tname = type ?? throw new DBException("42000", "Method").ISO();
            return mn;
        }
        /// <summary>
        /// Define a new method header (called when parsing CREATE TYPE)(calls ParseMethod)
        /// </summary>
        /// <param name="type">the type name</param>
        /// <returns>the methodname parse class</returns>
		MethodName ParseMethodHeader(Ident type)
        {
            MethodName mn = ParseMethod(type);
            var tdef = type.iix;
            if (tdef < 0)
                tdef = (db.GetObject(type.ident) as UDType ?? 
                    throw new DBException("42133", mn.tname).Mix()).defpos;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr + new PMethod(mn.name, mn.arity, mn.retType.defpos, mn.methodType, 
                    tdef, mn.signature, tr);
            return mn;
        }
        /// <summary>
        /// Create a method body (called when parsing CREATE METHOD or ALTER METHOD)
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseMethodDefinition()
        {
            var cr = new Executable(lxr.Position, Executable.Type.CreateRoutine);
            MethodName mn = ParseMethod(null);
            int st = lxr.start;
            UDType ut = db.GetObject(mn.tname.ident) as UDType??
                throw new DBException("42133", mn.tname).Mix();
            var ti = db.role.obinfos[ut.defpos] as ObInfo;
            Method meth = ut.methods[mn.mname.ident]?[mn.arity]??
                throw new DBException("42132", mn.mname.ToString(), ti.name).Mix();
            meth +=(Procedure.Params, mn.ins);
            meth += (Procedure.Body, ParseProcedureStatement(mn.retType));
            string ss = new string(lxr.input,st, lxr.start - st);
            // we really should check the signature here
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr + new Modify(mn.mname.ident, meth.defpos, mn.signature + " " + ss, 
                    meth,tr);
            return (Executable)cx.Add(cr);
        }
        /// <summary>
        /// DomainDefinition = id [AS] StandardType [DEFAULT TypedValue] { CheckConstraint } Collate.
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseDomainDefinition()
        {
            var cd = new Executable(lxr.Position, Executable.Type.CreateDomain);
            var colname = new Ident(lxr);
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.AS)
                Next();
            var type = ParseSqlDataType().domain;
            if (tok == Sqlx.DEFAULT)
            {
                Next();
                int st = lxr.start;
                var dv = lxr.val;
                Next();
                var ds = new string(lxr.input, st, lxr.start - st);
                type = type + (Domain.Default, dv) + (Domain.DefaultString, ds);
            }
            PDomain pd = null;
            var tr = db as Transaction?? throw new DBException("2F003");
            if (type.iri != null)
                pd = new PDomain1(colname.ident, type, tr);
            else
                pd = new PDomain(colname.ident, type, tr);
            // pd gets added to the database 10 lines below
            var a = new List<Physical>();
            while (Match(Sqlx.NOT, Sqlx.CONSTRAINT, Sqlx.CHECK))
                a.Add(ParseCheckConstraint(pd));
            if (tok == Sqlx.COLLATE)
                pd.culture = new CultureInfo(ParseCollate());
            if (db.parse == ExecuteStatus.Obey)
            {
                tr+=pd;
                for (int j = 0; j < a.Count; j++)
                    tr+=a[j];
            }
            db = tr;
            return (Executable)cx.Add(cd);
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
        PCheck ParseCheckConstraint(PDomain pd)
        {
            int st = lxr.start;
            var o = new Ident(lxr);
            Ident n = null;
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
                n = o;
                Mustbe(Sqlx.ID);
            }
            else
                n = new Ident(lxr);
            Mustbe(Sqlx.CHECK);
            Mustbe(Sqlx.LPAREN);
            st = lxr.start;
            ParseSqlValue(Domain.Bool);
            Mustbe(Sqlx.RPAREN);
            PCheck pc = null;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                pc = new PCheck(pd.defpos, n.ident, 
                    new string(lxr.input,st, lxr.start - st), tr);
            return pc;
        }
        /// <summary>
        /// id TableContents [UriType] [Clasasification] [Enforcement] {Metadata} 
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseCreateTable()
        {
            var ct = new Executable(lxr.Position, Executable.Type.CreateTable);
            var name = new Ident(lxr);
            PTable tb = null;
            Mustbe(Sqlx.ID);
            if (db.schemaRole.dbobjects.Contains(name.ident) || db.role.dbobjects.Contains(name.ident))
                throw new DBException("42104", name);
            var tr = db as Transaction?? throw new DBException("2F003");
            tb = new PTable(name.ident, tr);
            db = tr+tb;
            var ta = (Table)db.objects[tb.defpos];
            ParseTableContentsSource(ta);
            ta = (Table)db.objects[tb.defpos];
            if (tok == Sqlx.RDFLITERAL)
            {
                RdfLiteral rit = (RdfLiteral)lxr.val;
                ta += (DBObject._Domain, ta.domain+(Domain.Iri,rit.dataType.iri));
                Next();
            }
            if (Match(Sqlx.SECURITY))
            {
                Next();
                if (Match(Sqlx.LEVEL))
                    ParseClassification(ta.defpos);
                if (tok == Sqlx.SCOPE)
                    ParseEnforcement(ta.defpos);
            }
            return (Executable)cx.Add(ct);
        }
        void ParseClassification(long pt)
        {
            var lp = lxr.Position;
            var lv = MustBeLevel();
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr+ new Classify(pt, lv, tr);
        }
        /// <summary>
        /// Enforcement = SCOPE [READ] [INSERT] [UPDATE] [DELETE]
        /// </summary>
        /// <returns></returns>
        void ParseEnforcement(long pt)
        {
            if (db.user.defpos != db.owner)
                throw new DBException("42105");
            Mustbe(Sqlx.SCOPE);
            var lp = lxr.Position;
            Grant.Privilege r = Grant.Privilege.NoPrivilege;
            while (Match(Sqlx.READ,Sqlx.INSERT,Sqlx.UPDATE,Sqlx.DELETE))
            {
                switch(tok)
                {
                    case Sqlx.READ: r |= Grant.Privilege.Select; break;
                    case Sqlx.INSERT: r |= Grant.Privilege.Insert; break;
                    case Sqlx.UPDATE: r |= Grant.Privilege.Update; break;
                    case Sqlx.DELETE: r |= Grant.Privilege.Delete; break;
                }
                Next();
            }
            if (db.parse==ExecuteStatus.Obey && db is Transaction tr)
                db = tr + new Enforcement(pt, r, tr);
        }
        /// <summary>
        /// TebleContents = '(' TableClause {',' TableClause } ')' { VersioningClause }
        /// | OF Type_id ['(' TypedTableElement { ',' TypedTableElement } ')'] .
        /// VersioningClause = WITH (SYSTEM|APPLICATION) VERSIONING .
        /// </summary>
        /// <param name="ta">The newly defined Table</param>
        /// <returns>The iri or null</returns>
        void ParseTableContentsSource(Table ta,Ident tn=null)
        {
            switch (tok)
            {
                case Sqlx.LPAREN:
                    Next();
                    ta = ParseTableItem(ta,tn);
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        ta = ParseTableItem(ta,tn);
                    }
                    Mustbe(Sqlx.RPAREN);
                    while (Match(Sqlx.WITH))
                        ParseVersioningClause(ta, false);
                    break;
                case Sqlx.OF:
                    {
                        Next();
                        var id = ParseIdent();
                        var udt = db.GetObject(id.ident) as UDType??
                            throw new DBException("42133", id.ToString()).Mix();
                        var ui = db.role.obinfos[udt.defpos] as ObInfo;
                        var tr = db as Transaction?? throw new DBException("2F003");
                        for (var cd = ui.columns.First(); cd != null; cd = cd.Next())
                        {
                            var se = cd.value() as SqlValue;
                            var tc = db.objects[se.defpos] as TableColumn;
                            var pc = new PColumn2(ta.defpos, se.name, cd.key(), tc.domain.defpos, 
                                tc.generated.gfs??tc.domain.defaultValue?.ToString()??"", 
                                tc.domain.defaultValue, tc.notNull, 
                                tc.generated, db);
                            tr += pc;
                        }
                        db = tr;
                        ta = (Table)db.objects[ta.defpos];
                        var rt = db.role.obinfos[ta.defpos] as ObInfo;
                        if (Match(Sqlx.LPAREN))
                        {
                            for (; ; )
                            {
                                Next();
                                if (Match(Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.FOREIGN))
                                    ParseTableConstraintDefin(ta);
                                else
                                {
                                    id = ParseIdent();
                                    var se = ui.ColFor(id.ident) as SqlValue;
                                    if (se == null)
                                        throw new DBException("42112", id.ToString()).Mix();
                                    ParseColumnOptions(ta, se);
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
        }
        /// <summary>
        /// Parse the table versioning clause:
        /// (SYSTEM|APPLICATION) VERSIONING
        /// </summary>
        /// <param name="ta">the table</param>
        /// <param name="drop">whether we are dropping an existing versioning</param>
        /// <returns>the updated Table object</returns>
        private Table ParseVersioningClause(Table ta, bool drop)
        {
            Next();
            var sa = tok;
            var lp = lxr.Position;
            Mustbe(Sqlx.SYSTEM, Sqlx.APPLICATION);
            var vi = (sa == Sqlx.SYSTEM) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
            PIndex pi = null;
            var tr = db as Transaction?? throw new DBException("2F003");
            if (drop)
            {
                var fl = (vi == 0) ? PIndex.ConstraintType.SystemTimeIndex : PIndex.ConstraintType.ApplicationTimeIndex;
                for (var e = ta.indexes.First(); e != null; e = e.Next())
                {
                    var px = db.objects[e.value()] as PIndex;
                    if (px.tabledefpos == ta.defpos && (px.flags & fl) == fl)
                        tr += new Drop(px.defpos, tr);
                }
                if (sa == Sqlx.SYSTEM)
                {
                    var pd = db.objects[ta.systemPS] as PeriodDef;
                    tr+=new Drop(pd.defpos, tr);
                }
                Mustbe(Sqlx.VERSIONING);
                return (Table)cx.Add(ta);
            }
            if (sa == Sqlx.SYSTEM)
            {
                // ??
            }
            Index ix = ta.FindPrimaryIndex(db);
            var ti = db.role.obinfos[ta.defpos] as ObInfo;
            if (pi == null && sa == Sqlx.APPLICATION)
                throw new DBException("42164", ti.name).Mix();
            if (pi != null)
                tr+=new PIndex("",ta.defpos, pi.columns,
                    PIndex.ConstraintType.PrimaryKey | vi,
                    -1L, tr);
            Mustbe(Sqlx.VERSIONING);
            db = tr;
            return (Table)cx.Add(ta);
        }
        /// <summary>
        /// TypedTableElement = id WITH OPTIONS '(' ColumnOption {',' ColumnOption} ')'.
        /// ColumnOption = (SCOPE id)|(DEFAULT TypedValue)|ColumnConstraint .
        /// </summary>
        /// <param name="tb">The table define created</param>
        /// <param name="tc">The column beging optioned</param>
        void ParseColumnOptions(Table tb, SqlValue se)
        {
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
                            var dfv = se.domain.Parse(lxr.Position,new string(lxr.input, st, lxr.start - st));
                            db = ((Transaction)db)+ 
                                (se + (DBObject._Domain, se.domain + (Domain.Default, dfv)));
                            break;
                        }
                    default: ParseColumnConstraint(tb, se);
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
        /// <param name="tb">the table</param>
        /// <returns>the updated Table</returns>
        Table ParseTableItem(Table tb,Ident tn=null)
        {
            if (Match(Sqlx.PERIOD))
                tb = AddTablePeriodDefinition(tb);
            else if (tok == Sqlx.ID)
                tb = ParseColumnDefin(tb,tn);
            else
                tb = ParseTableConstraintDefin(tb);
            return (Table)cx.Add(tb);
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
                r.periodname = new Ident(lxr);
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
            var rt = db.role.obinfos[tb.defpos] as ObInfo;
            var c1 = rt.ColFor(ptd.col1.ident);
            var c2 = rt.ColFor(ptd.col2.ident);
            var c1t = c1?.domain;
            var c2t = c2?.domain;
            if (c1 == null)
                throw new DBException("42112", ptd.col1).Mix();
            if (c1t.kind != Sqlx.DATE && c2t.kind != Sqlx.TIMESTAMP)
                throw new DBException("22005R", "DATE or TIMESTAMP", c1t.ToString()).ISO()
                    .AddType(Domain.UnionDate).AddValue(c1t);
            if (c1 == null)
                throw new DBException("42112", ptd.col2).Mix();
            if (c1t.CompareTo(c2t)!=0)
                throw new DBException("22005S", c1t.ToString(), c2t.ToString()).ISO()
                    .AddType(c1t).AddValue(c2t);
            var tr = db as Transaction?? throw new DBException("2F003");
            var ps = new PPeriodDef(tb.defpos, ptd.periodname.ident, c1.defpos, c2.defpos, tr);
            db = tr+ps;
            return (Table)cx.Add((Table)db.objects[tb.defpos]);
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
        Table ParseColumnDefin(Table tb,Ident tn=null)
        {
            DBObject type = null;
            SqlValue rc = null;
            var colname = new Ident(lxr);
            long lp = lxr.Position;
            Mustbe(Sqlx.ID);
            Domain dm = null;
            if (tok == Sqlx.ID)
            {
                var domain = db.GetObInfo(new Ident(lxr).ident)?.domain ??
                    throw new DBException("42119", lxr.val.ToString());
                Next();
            }
            else if (Match(Sqlx.CHARACTER, Sqlx.CHAR, Sqlx.VARCHAR, Sqlx.NATIONAL, Sqlx.NCHAR,
                Sqlx.BOOLEAN, Sqlx.NUMERIC, Sqlx.DECIMAL,
                Sqlx.DEC, Sqlx.FLOAT, Sqlx.REAL, Sqlx.DOUBLE,
                Sqlx.INT, Sqlx.INTEGER, Sqlx.BIGINT, Sqlx.SMALLINT, Sqlx.PASSWORD,
                Sqlx.BINARY, Sqlx.BLOB, Sqlx.NCLOB, Sqlx.CLOB, Sqlx.XML,
                Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL,
                Sqlx.DOCUMENT, Sqlx.DOCARRAY,
#if MONGO
                Sqlx.OBJECT, // v5.1
#endif
                Sqlx.ROW, Sqlx.TABLE, Sqlx.REF))
                type = ParseSqlDataType();
            if (Match(Sqlx.ARRAY))
            {
                type = new Domain(lp,Sqlx.ARRAY, type);
                Next();
            }
            else if (Match(Sqlx.MULTISET))
            {
                type = new Domain(lp,Sqlx.MULTISET, type);
                Next();
            }
            /*           if (tok == Sqlx.RDFLITERAL)
                       {
                           if (lxr.val is RdfLiteral owl)
                           {
                               Domain.GetOrCreateDomain(lxr,lxr.db, ref type);
                               type = type.Copy(owl.dataType.iri);
                               if (owl.val != null && owl.ToString() != "")
                                   type.abbrev = owl.ToString();
                               //                if (owl.type.iri ==IriRef.BSON)
                               //                  Bson.Parse(trans,owl);
                           }
                           Next();
                       } */
            var ua = BList<UpdateAssignment>.Empty;
            var gr = GenerationRule.None;
            var dfs = "";
            TypedValue dv = TNull.Value;
            if (tok == Sqlx.DEFAULT)
            {
                Next();
                int st = lxr.start;
                dv = lxr.val;
                dfs = new string(lxr.input, st, lxr.pos - st);
                Next();
            }
            else
            {
                var dn = type as Domain;
                dv = dn?.defaultValue??TNull.Value;
                dfs = dn?.defaultString??"";
                var oi = (ObInfo)db.role.obinfos[tb.defpos];
                for (var b = oi.columns.First(); b != null; b = b.Next())
                {
                    var co = (SqlCol)b.value();
                    var cn = new Ident(co.name, co.defpos);
                    if (tn != null)
                        cx.defs += (new Ident(tn, cn), co);
                    cx.defs += (cn, co);
                }
                if (Match(Sqlx.GENERATED))
                    gr = ParseGenerationRule(tb);
            }
            if (dm == null)
                dm = type as Domain;
            if (dm == null)
                throw new DBException("42120", colname.ident);
            var tr = db as Transaction?? throw new DBException("2F003");
            PColumn3 pc = null;
            TableColumn tc = null;
            int k = 0;
            var dt = (ObInfo)db.role.obinfos[tb.defpos];
            k = (int)dt.columns.Count;
            pc = new PColumn3(tb.defpos, colname.ident, k, dm.defpos, gr.gfs??dfs, dv, 
                "",ua,false, gr, db);
            tr +=pc;
            db = tr;
            tb = db.objects[tb.defpos] as Table;
            tc = db.objects[pc.ppos] as TableColumn;
            var se = new SqlValue(tc.defpos,colname.ident);
            var ri = (ObInfo)db.role.obinfos[tb.defpos];
            if (rc != null)
                tr += new PCheck(tc.defpos, ""+db.uid,
                    "value in (select \"" + rc.name + "\" from \"" + ri.name + "\")",
                    tr);
            while (Match(Sqlx.NOT, Sqlx.REFERENCES, Sqlx.CHECK, Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.CONSTRAINT,
                Sqlx.SECURITY))
            {
                tb = ParseColumnConstraintDefin(tb, se, pc);
                tr = (Transaction)db; // !!
            }
            if (type != null && tok == Sqlx.COLLATE)
                dm = new Domain(pc.ppos,type.kind,type.mem+(Domain.Culture,new CultureInfo(ParseCollate())));
            if (pc != null && pc.domdefpos != dm.defpos)
            {
                pc.domdefpos = dm.defpos;
                tr += pc;
                tc += (DBObject._Domain, dm);
                tr += tc;
            }
            db = tr;
            cx.Add(tc);
            var sc = new SqlCol(tc.defpos, colname.ident, tc);
            if (tn != null)
                cx.defs += (new Ident(tn, colname), sc);
            else
                cx.defs += (colname, sc);
            return (Table)cx.Add((Table)db.objects[tb.defpos]);
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
                case Sqlx.TABLE: return Match(Sqlx.ENTITY, Sqlx.PIE, Sqlx.HISTOGRAM, Sqlx.LEGEND, Sqlx.LINE, Sqlx.POINTS, Sqlx.DROP,
                    Sqlx.JSON, Sqlx.CSV, Sqlx.CHARLITERAL, Sqlx.RDFLITERAL, Sqlx.REFERRED);
                case Sqlx.COLUMN: return Match(Sqlx.ATTRIBUTE, Sqlx.X, Sqlx.Y, Sqlx.CAPTION, Sqlx.DROP, Sqlx.CHARLITERAL, Sqlx.RDFLITERAL,
                    Sqlx.REFERS);
                case Sqlx.FUNCTION: return Match(Sqlx.ENTITY, Sqlx.PIE, Sqlx.HISTOGRAM, Sqlx.LEGEND,
                    Sqlx.LINE, Sqlx.POINTS, Sqlx.DROP, Sqlx.JSON, Sqlx.CSV, Sqlx.INVERTS, Sqlx.MONOTONIC);
                default: return Match(Sqlx.CHARLITERAL, Sqlx.RDFLITERAL);
            }
        }
        /// <summary>
        /// Parse ([ADD]|DROP) Metadata
        /// </summary>
        /// <param name="tr">the database</param>
        /// <param name="ob">the object the metadata is for</param>
        /// <param name="seq">the position of the metadata</param>
        /// <param name="kind">the metadata</param>
        Metadata ParseMetadata(Database tr, DBObject ob, int seq, Sqlx kind)
        {
            var drop = false;
            if (Match(Sqlx.ADD))
                Next();
            else if (Match(Sqlx.DROP))
            {
                drop = true;
                Next();
            }
            return ParseMetadata(tr, ob, seq, kind, drop);
        }
        /// <summary>
        /// Parse Metadata
        /// </summary>
        /// <param name="tr">the database</param>
        /// <param name="ob">the object to be affected</param>
        /// <param name="seq">the position in the metadata</param>
        /// <param name="kind">the metadata</param>
        /// <param name="drop">whether drop</param>
        /// <returns>the metadata</returns>
        Metadata ParseMetadata(Database tr, DBObject ob, int seq, Sqlx kind, bool drop)
        {
            Metadata m = null;
            var checkedRole = 0;
            var pr = ((ObInfo)tr.role.obinfos[ob.defpos]).priv;
            while (StartMetadata(kind))
            {
                if (checkedRole++ == 0 && (tr.role == null || !pr.HasFlag(Grant.Privilege.AdminRole)))
                    throw new DBException("42105");
                if (m == null)
                    m = new Metadata();
                var o = lxr.val;
                switch (tok)
                {
                    case Sqlx.CHARLITERAL: m.description = drop ? "" : o.ToString(); break;
                    case Sqlx.RDFLITERAL:
                        if (!pr.HasFlag(Grant.Privilege.Owner))
                            throw new DBException("42105");
                        m.iri = drop ? "" : o.ToString();
                        break;
                    case Sqlx.ID:
                        {
                            var ifc = new Ident(lxr);
                            var nm = ifc.Suffix(seq);
                            var x = tr.GetObject(nm.ident)??
                                throw new DBException("42108", lxr.val.ToString()).Pyrrho();
                            m.refpos = x.defpos;
                            break;
                        }
                    default:
                        var ic = new Ident(lxr);
                        if (drop)
                            m.Drop(tok);
                        else
                            m.Add(tok);
                        break;
                }
                Next();
            }
            return m;
        }
        /// <summary>
        /// GenerationRule =  GENERATED ALWAYS AS '('Value')' [ UPDATE '(' Assignments ')' ]
        /// |   GENERATED ALWAYS AS ROW (START|END) .
        /// </summary>
        /// <param name="rt">The expected type</param>
        GenerationRule ParseGenerationRule(DBObject rt)
        {
            var gr = GenerationRule.None;
            if (Match(Sqlx.GENERATED))
            {
                Next();
                Mustbe(Sqlx.ALWAYS);
                Mustbe(Sqlx.AS);
                if (Match(Sqlx.ROW))
                {
                    Next();
                    switch (tok)
                    {
                        case Sqlx.START: gr = new GenerationRule(Generation.RowStart); break;
                        case Sqlx.END: gr = new GenerationRule(Generation.RowEnd); break;
                        default: throw new DBException("42161", "START or END", tok.ToString()).Mix();
                    }
                    Next();
                }
                else
                {
                    int st = lxr.start;
                    var gnv = ParseSqlValue(rt);
                    var s = new string(lxr.input, st, lxr.start - st);
                    gr = new GenerationRule(Generation.Expression, s, gnv);
                }
            }
            return gr;
        }
        /// <summary>
        /// Parse a columnconstrintdefinition
        /// ColumnConstraint = [ CONSTRAINT id ] ColumnConstraintDef
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="tc">the column (Level3)</param>
        /// <param name="pc">the column (Level2)</param>
        /// <returns>the updated table</returns>
		Table ParseColumnConstraintDefin(Table tb, SqlValue se, PColumn2 pc)
        {
            Ident name = null;
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
                name = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            else
                name = new Ident(lxr);
            if (tok == Sqlx.NOT)
            { 
                Next();
                if (pc != null)
                    pc.notNull = true;
                Mustbe(Sqlx.NULL);
                // adding NOT NULL at this stage is ridiculously tricky
                var tc = db.objects[se.defpos] as TableColumn;
                var ro = db.role;
                var ti = (ObInfo)ro.obinfos[tb.defpos];
                var iq = ti.map[pc.name];
                tc+=(Domain.NotNull,true);
                ti += (ObInfo.Columns, ti.columns + (iq.Value, new SqlCol(pc.ppos, pc.name, tc)));
                ro += ti;
                var tr = (Transaction)db;
                tr += pc;
                tr += ro;
                db = tr;
            }
            else
                tb = ParseColumnConstraint(tb, se);
            return (Table)cx.Add((Transaction)db,tb.defpos);
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
		Table ParseColumnConstraint(Table tb, SqlValue se)
        {
            var tc = db.objects[se.defpos] as TableColumn;
            if (tc.generated != GenerationRule.None)
                throw new DBException("42163", se.name).Mix();
            var key = CList<TableColumn>.Empty;
            if (se is SqlCol sc)
                key += sc.tableCol;
            else if (se is SqlValue sv && db.objects[sv.defpos] is TableColumn co)
                key += co;
            else throw new DBException("42112", se.name);
            var o = lxr.val;
            var lp = lxr.Position;
            switch (tok)
            {
                case Sqlx.SECURITY:
                    Next();
                    ParseClassification(tc.defpos); break;
                case Sqlx.REFERENCES:
                    {
                        Next();
                        var refname = new Ident(lxr);
                        var rt = db.GetObject(refname.ident) as Table??
                            throw new DBException("42107", refname).Mix();
                        CList<TableColumn> cols = null;
                        Mustbe(Sqlx.ID);
                        if (tok == Sqlx.LPAREN)
                            cols = ParseColsList(rt);
                        string afn = "";
                        if (tok == Sqlx.USING)
                        {
                            Next();
                            int st = lxr.start;
                            if (tok == Sqlx.ID)
                            {
                                Next();
                                var nm = new Ident(lxr);
                                var pr = db.GetProcedure(nm.ident,1) ??
                                    throw new DBException("42108", nm).Pyrrho();
                                afn = "\"" + pr.defpos + "\"";
                            }
                            else
                            {
                                Mustbe(Sqlx.LPAREN);
                                ParseSqlValueList(new ObInfo(st,cols,(Transaction)db));
                                Mustbe(Sqlx.RPAREN);
                                afn = new string(lxr.input,st, lxr.start - st);
                            }
                        }
                        var ct = ParseReferentialAction();
                        db = ((Transaction)db).AddReferentialConstraint(lp,tb, new Ident("",0), key, rt, cols, ct, afn);
                        break;
                    }
                case Sqlx.CHECK:
                    {
                        Next();
                        ParseColumnCheckConstraint(tb, se);
                        break;
                    }
                case Sqlx.UNIQUE:
                    {
                        Next();
                        var tr = db as Transaction?? throw new DBException("2F003");
                        db= tr+new PIndex(se.name, tb.defpos, key, PIndex.ConstraintType.Unique, -1L,
                            tr);
                        break;
                    }
                case Sqlx.PRIMARY:
                    {
                        Index x = tb.FindPrimaryIndex(db);
                        var ti = db.role.obinfos[tb.defpos] as ObInfo;
                        if (x != null)
                            throw new DBException("42147", ti.name).Mix();
                        Next();
                        Mustbe(Sqlx.KEY);
                        var tr = db as Transaction?? throw new DBException("2F003");
                        db = tr+new PIndex(ti.name, tb.defpos, key, PIndex.ConstraintType.PrimaryKey, 
                            -1L, tr);
                        break;
                    }
            }
            return (Table)cx.Add((Transaction)db,tb.defpos);
        }
        /// <summary>
        /// TableConstraint = [CONSTRAINT id ] TableConstraintDef .
		/// TableConstraintDef = UNIQUE Cols
		/// |	PRIMARY KEY  Cols
		/// |	FOREIGN KEY Cols REFERENCES Table_id [ Cols ] { ReferentialAction } .
        /// </summary>
        /// <param name="tb">the table</param>
        /// <returns>the updated table</returns>
		Table ParseTableConstraintDefin(Table tb)
        {
            Ident name = null;
            if (tok == Sqlx.CONSTRAINT)
            {
                Next();
                name = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            else
                name = new Ident(lxr);
            Sqlx s = Mustbe(Sqlx.UNIQUE, Sqlx.PRIMARY, Sqlx.FOREIGN, Sqlx.CHECK);
            switch (s)
            {
                case Sqlx.UNIQUE: tb = ParseUniqueConstraint(tb, name); break;
                case Sqlx.PRIMARY: tb = ParsePrimaryConstraint(tb, name); break;
                case Sqlx.FOREIGN: tb = ParseReferentialConstraint(tb, name); break;
                case Sqlx.CHECK: ParseTableConstraint(tb, name); break;
            }
            return (Table)cx.Add((Transaction)db,tb.defpos);
        }
        /// <summary>
        /// construct a unique constraint
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="name">the constraint name</param>
        /// <returns>the updated table</returns>
        Table ParseUniqueConstraint(Table tb, Ident name)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            db = tr + new PIndex(name.ident, tb.defpos, ParseColsList(tb), 
                PIndex.ConstraintType.Unique, -1L, tr);
            return (Table)cx.Add(tr,tb.defpos);
        }
        /// <summary>
        /// construct a primary key constraint
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="cl">the ident</param>
        /// <param name="name">the constraint name</param>
        Table ParsePrimaryConstraint(Table tb, Ident name)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            Index x = tb.FindPrimaryIndex(db);
            var ti = db.role.obinfos[tb.defpos] as ObInfo;
            if (x != null)
                throw new DBException("42147", ti.name).Mix();
            Mustbe(Sqlx.KEY);
            db=tr+new PIndex(name.ident, tb.defpos, ParseColsList(tb), 
                PIndex.ConstraintType.PrimaryKey, -1L, tr);
            return (Table)cx.Add(tr,tb.defpos);
        }
        /// <summary>
        /// construct a referential constraint
        /// id [ Cols ] { ReferentialAction }
        /// </summary>
        /// <param name="tb">the table</param>
        /// <param name="name">the constraint name</param>
        /// <returns>the updated table</returns>
        Table ParseReferentialConstraint(Table tb, Ident name)
        {
            var tr = db as Transaction?? throw new DBException("2F003");
            Mustbe(Sqlx.KEY);
            var cols = ParseColsList(tb);
            var lp = lxr.Position;
            Mustbe(Sqlx.REFERENCES);
            var refname = new Ident(lxr);
            var rt = db.GetObject(refname.ident) as Table??
                throw new DBException("42107", refname).Mix();
            Mustbe(Sqlx.ID);
            var refs = CList<TableColumn>.Empty;
            PIndex.ConstraintType ct = PIndex.ConstraintType.ForeignKey;
            if (tok == Sqlx.LPAREN)
                refs = ParseColsList(rt);
            string afn = "";
            if (tok == Sqlx.USING)
            {
                Next();
                int st = lxr.start;
                if (tok == Sqlx.ID)
                {
                    var ic = new Ident(lxr);
                    Next();
                    var pr = db.GetProcedure(ic.ident,(int)refs.Count)??
                        throw new DBException("42108", ic.ident+"$"+refs.Count).Pyrrho();
                    afn = "\"" + pr.defpos + "\"";
                }
                else
                {
                    Mustbe(Sqlx.LPAREN);
                    ParseSqlValueList(new ObInfo(st,cols,tr));
                    Mustbe(Sqlx.RPAREN);
                    afn = new string(lxr.input,st, lxr.start - st);
                }
            }
            if (tok == Sqlx.ON)
                ct |= ParseReferentialAction();
            db=tr.AddReferentialConstraint(lp,tb, name, cols, rt, refs, ct, afn);
            return (Table)cx.Add(tr,tb.defpos);
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] SqlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="name">the name of the constraint</param
        /// <returns>the new PCheck</returns>
        void ParseTableConstraint(Table tb, Ident name)
        {
            int st = lxr.start;
            Mustbe(Sqlx.LPAREN);
            ParseSqlValue(Domain.Bool);
            Mustbe(Sqlx.RPAREN);
            var n = name ?? new Ident(lxr);
            var tr = db as Transaction?? throw new DBException("2F003");
            PCheck r = new PCheck(tb.defpos, n.ident,
                new string(lxr.input,st, lxr.start - st), tr);
            tr+=r;
            if (tb.defpos < Transaction.TransPos)
                new From(-1,tb, (ObInfo)db.role.obinfos[tb.defpos]).TableCheck(tr,r);
            db = tr;
        }
        /// <summary>
        /// CheckConstraint = [ CONSTRAINT id ] CHECK '(' [XMLOption] SqlValue ')' .
        /// </summary>
        /// <param name="tb">The table</param>
        /// <param name="pc">the column constrained</param>
        /// <param name="name">the name of the constraint</param>
        /// <returns>the new PCheck</returns>
        void ParseColumnCheckConstraint(Table tb, SqlValue se)
        {
            int st = lxr.start;
            var lp = lxr.Position;
            Mustbe(Sqlx.LPAREN);
            var cx = new Context(db);
            ParseSqlValue(Domain.Bool);
            Mustbe(Sqlx.RPAREN);
            var tr = db as Transaction?? throw new DBException("2F003");
            PCheck2 r = new PCheck2(tb.defpos, se.defpos,se.name,
                new string(lxr.input,st, lxr.start - st), tr);
            db = tr+r;
        }
        /// <summary>
		/// ReferentialAction = {ON (DELETE|UPDATE) (CASCADE| SET DEFAULT|RESTRICT)} .
        /// </summary>
        /// <returns>constraint type flags</returns>
		PIndex.ConstraintType ParseReferentialAction()
        {
            PIndex.ConstraintType r = PIndex.ConstraintType.ForeignKey;
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
                    switch (what)
                    {
                        case Sqlx.CASCADE: r |= PIndex.ConstraintType.CascadeUpdate; break;
                        case Sqlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultUpdate; break;
                        case Sqlx.NULL: r |= PIndex.ConstraintType.SetNullUpdate; break;
                    }
                else
                    switch (what)
                    {
                        case Sqlx.CASCADE: r |= PIndex.ConstraintType.CascadeDelete; break;
                        case Sqlx.DEFAULT: r |= PIndex.ConstraintType.SetDefaultDelete; break;
                        case Sqlx.NULL: r |= PIndex.ConstraintType.SetNullDelete; break;
                    }
            }
            return r;
        }
        public Procedure ParseProcedureBody(Procedure pr,string sql)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            lxr = new Lexer(sql,pr.defpos-1);
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                tr += (Database.NextTid, db.nextTid + sql.Length);
            var (ps,dm) = ParseProcedureHeading(new Ident(pr.name,pr.defpos));
            pr = pr + (Procedure.Params, ps) + (Procedure.RetType, dm);
            db = tr+pr;
            var oc = cx;
            cx = new Context(db);
            for (var b = ps.First(); b != null; b = b.Next())
            {
                var pp = b.value();
                cx.defs += (new Ident(pp.name,0),pp);
                cx.Add(pp);
            }
            var bd = ParseProcedureStatement(dm);
            cx = oc;
            return pr + (Procedure.Body, bd);
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
        public Executable ParseProcedureClause(bool func, Sqlx create)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            var op = db.parse;
            db += (Database._ExecuteStatus, ExecuteStatus.Parse);
            var cr = new Executable(lxr.Position, (create == Sqlx.CREATE) ? Executable.Type.CreateRoutine : Executable.Type.AlterRoutine);
            var n = new Ident(lxr);
            Mustbe(Sqlx.ID);
            int st = lxr.start;
            var ps = ParseParameters(n);
            var arity = (int)ps.Count;
            var pn = n.Suffix(arity);
            DBObject rt = Domain.Null;
            if (func)
                rt = ParseReturnClause(n);
            if (Match(Sqlx.EOF) && create == Sqlx.CREATE)
                throw new DBException("42000", "EOF").Mix();
            PProcedure pp = null;
            Domain rdt = null;
            var pr = db.GetProcedure(n.ident, arity) as Procedure;
            if (pr == null)
            {
                if (create == Sqlx.CREATE)
                {
                    pp = new PProcedure(n.ident, arity, rt.defpos, "", db);
                    pr = new Procedure(pp, db, new BTree<long, object>(Procedure.Params, ps));
                    tr += pp;
                    tr += pr;
                    if (cx.dbformat<51)// provide for possible recursive reference
                        cx.digest += (n.iix, (n.ident, n.iix)); 
                }
                else
                    throw new DBException("42108", n.ToString()).Mix();
            }
            else
                if (create == Sqlx.CREATE)
                throw new DBException("42167", n.ident, (int)ps.Count).Mix();
            if (create == Sqlx.ALTER && tok == Sqlx.TO)
            {
                Next();
                var nn = new Ident(lxr);
                Mustbe(Sqlx.ID);
                pr = pr + (Basis.Name, n.ident) + (Procedure.Arity, (int)ps.Count);
                tr += pr;
            }
            else if (create == Sqlx.ALTER && (StartMetadata(Sqlx.FUNCTION) || Match(Sqlx.ALTER, Sqlx.ADD, Sqlx.DROP)))
                ParseAlterBody(pr);
            else
            {
                var sq = 1;
                if (ParseMetadata(db, pr, sq, Sqlx.FUNCTION, false) is Metadata md)
                    tr += new PMetadata3(n.ident, md.seq, pr.defpos, md.description,
                        md.iri, pr.defpos, md.flags, tr);
                pr = pr + (Procedure.RetType, rdt) + (Procedure.Params, ps);
                var s = new string(lxr.input, st, lxr.start - st);
                if (tok != Sqlx.EOF && tok != Sqlx.SEMICOLON)
                {
                    pr += (Procedure.Body, ParseProcedureStatement(pr.retType));
                    s = new string(lxr.input, st, lxr.start - st);
                }
                if (create == Sqlx.CREATE)
                {
                    tr += pp;
                    tr += pr;
                }
                //  pr.forSchema = db.lastSchemaPos;
                if (pp != null)
                {
                    pp.proc = pr;
                    pp.proc_clause = s;
                    tr += pr;
                    cx.defs += (n, pr);
                    if (db.format < 51)
                        pp.digested = cx.digest;
                }
                else
                    tr += new Modify(n.ident, pr.defpos, s, pr, tr); // finally add the Modify
            }
            tr += (Database._ExecuteStatus, op);
            db = tr;
            return (Executable)cx.Add(cr);
        }
        (BList<ProcParameter>,DBObject) ParseProcedureHeading(Ident pn)
        {
            var ps = BList<ProcParameter>.Empty;
            var dm = Domain.Null;
            if (tok != Sqlx.LPAREN)
                return (ps, dm);
            ps = ParseParameters(pn);
            if (tok == Sqlx.RETURNS)
            {
                Next();
                dm = ParseSqlDataType(pn).domain;
            }
            return (ps, dm);
        }
        /// <summary>
        /// Function metadata is used to control display of output from table-valued functions
        /// </summary>
        /// <param name="pr"></param>
        Procedure ParseAlterBody(Procedure pr)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            if (!pr.mem.Contains(Procedure.RetType))
                return pr;
            var dt = pr.retType;
            var ri = db.role.obinfos[dt.defpos] as ObInfo;
            if (ri.Length==0)
                return pr;
            pr = (Procedure)ParseAlterOp(pr);
            while (tok == Sqlx.COMMA)
            {
                Next();
                pr = (Procedure)ParseAlterOp(pr);
            }
            return (Procedure)cx.Add(tr, pr.defpos);
        }
        /// <summary>
        /// Parse a parameter list
        /// </summary>
        /// <returns>the list of formal procparameters</returns>
		BList<ProcParameter> ParseParameters(Ident pn)
		{
            Mustbe(Sqlx.LPAREN);
            var r = BList<ProcParameter>.Empty;
            var ac = cx as CalledActivation;
            var svs = ac?.locals;
			while (tok!=Sqlx.RPAREN)
			{
                r+= ParseProcParameter(pn);
				if (tok!=Sqlx.COMMA)
					break;
				Next();
			}
            if (ac != null)
                ac.locals = svs;
			Mustbe(Sqlx.RPAREN);
			return r;
		}
        /// <summary>
        /// Parse the return clause 
        /// </summary>
        /// <returns>the data type required (Domain or ObInfo)</returns>
		DBObject ParseReturnClause(Ident pn)
		{
			if (tok!=Sqlx.RETURNS)
				return Domain.Null;
			Next();
            if (tok == Sqlx.ID)
            {
                var s = lxr.val.ToString();
                return db.GetObject(s);
            }
            var st = lxr.pos;
            var lp = lxr.Position;
			var dm = ParseSqlDataType(pn);
            if (cx.dbformat < 51)
                cx.digest += (lp, (new string(lxr.input,st,lxr.start - st), 
                    (dm.domain.structure>0)?dm.domain.structure:dm.defpos));
            return dm;
		}
        /// <summary>
        /// parse a formal parameter
        /// </summary>
        /// <returns>the procparameter</returns>
		ProcParameter ParseProcParameter(Ident pn)
		{
			Sqlx pmode = Sqlx.NONE;
			if (Match(Sqlx.IN,Sqlx.OUT,Sqlx.INOUT))
			{
				pmode = tok;
				Next();
			}
			var n = new Ident(lxr);
			Mustbe(Sqlx.ID);
            var p = new ProcParameter(lxr.Position, pmode,n.ident,ParseSqlDataType().domain);
            if (pn!=null)
                cx.defs += (new Ident(pn, n), p);
			if (Match(Sqlx.RESULT))
			{
                p += (ProcParameter.Result, true);
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
			Next();
 			if (Match(Sqlx.CONTINUE,Sqlx.EXIT,Sqlx.UNDO))
				return (Executable)cx.Add(ParseHandlerDeclaration());
			int st = lxr.start;
			var n = new Ident(lxr);
			Mustbe(Sqlx.ID);
            LocalVariableDec lv = null;
            DBObject ld;
            if (tok == Sqlx.CURSOR)
            {
                Next();
                Mustbe(Sqlx.FOR);
                var e = ParseCursorSpecification(ObInfo.Any) as SelectStatement;
                lv = new CursorDeclaration(lxr.Position, cx, n.ident, e.cs);
                ld = e.cs.rowType;
            }
            else
            {
                ld = ParseSqlDataType();
                lv = new LocalVariableDec(lxr.Position, n.ident,
                     ld as Domain ?? throw new DBException("42000", lxr.pos));
            }
            if (Match(Sqlx.EQL, Sqlx.DEFAULT))
            {
                Next();
                st = lxr.start;
                var iv = ParseSqlValue(ld);
                lv+=(LocalVariableDec.Init, iv);
            }
            cx.defs += (n, lv.vbl);
            cx.Add(lv.vbl);
            return lv;
		}
        /// <summary>
        /// |	DECLARE HandlerType HANDLER FOR ConditionList Statement .
        /// HandlerType = 	CONTINUE | EXIT | UNDO .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
        Executable ParseHandlerDeclaration()
        {
            var hs = new HandlerStatement(lxr.Position, tok, new Ident(lxr).ident);
            Mustbe(Sqlx.CONTINUE, Sqlx.EXIT, Sqlx.UNDO);
            Mustbe(Sqlx.HANDLER);
            Mustbe(Sqlx.FOR);
            var ac = new Activation(cx, hs.label);
            hs+=(HandlerStatement.Conds,ParseConditionValueList());
            var st = lxr.pos;
            var a = ParseProcedureStatement(Domain.Content);
            hs= hs+(HandlerStatement.Action,a)+(DBObject.Dependents,a.dependents);
            return (Executable)cx.Add(hs);
        }
        /// <summary>
		/// ConditionList =	Condition { ',' Condition } .
        /// </summary>
        /// <returns>the list of conditions</returns>
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
                switch (n.ToString().Substring(0,2))
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
        /// <returns>the Executable result of the parse</returns>
		Executable ParseCompoundStatement(DBObject rt,Ident n)
        {
            var eid = id + lxr.start;
            var cs = new CompoundStatement(lxr.Position, n?.ident);
            BList<Executable> r = BList<Executable>.Empty;
            Mustbe(Sqlx.BEGIN);
            var ac = new Activation(cx, n?.ident);
            if (tok == Sqlx.TRANSACTION)
                throw new DBException("25001", "Nested transactions are not supported").ISO();
            if (tok == Sqlx.XMLNAMESPACES)
            {
                Next();
                r+=ParseXmlNamespaces();
            }
            while (tok != Sqlx.END)
            {
                r+=((int)r.Count,ParseProcedureStatement(rt));
                if (tok == Sqlx.END)
                    break;
                Mustbe(Sqlx.SEMICOLON);
            }
            Mustbe(Sqlx.END);
            cs+=(CompoundStatement.Stms,r);
            cs += (DBObject.Dependents, _Deps(r));
            return (Executable)cx.Add(cs);
        }
        internal (Database,Executable) ParseProcedureStatement(string sql,DBObject rt)
        {
            lxr = new Lexer(sql, db.lexeroffset);
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid + sql.Length);
            var b = ParseProcedureStatement(rt);
            return (db,(b!=null)?(Executable)cx.Add(b):null);
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
        /// <param name="p">the procedure return type</param>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseProcedureStatement(DBObject rt)
		{
            Match(Sqlx.BREAK);
            Executable r = null;
            int st = lxr.start;
 			switch (tok)
			{
                case Sqlx.ID: r= ParseLabelledStatement(rt); break;
                case Sqlx.BEGIN: r = ParseCompoundStatement(rt,null); break;
                case Sqlx.CALL: r = ParseCallStatement(rt); break;
                case Sqlx.CASE: r = ParseCaseStatement(rt); break;
                case Sqlx.CLOSE: r = ParseCloseStatement(); break;
                case Sqlx.COMMIT: throw new DBException("2D000").ISO(); // COMMIT not allowed inside SQL routine
                case Sqlx.BREAK: r = ParseBreakLeave(); break;
                case Sqlx.DECLARE: r = ParseDeclaration(); break;
                case Sqlx.DELETE: r = ParseSqlDelete(); break;
                case Sqlx.EOF: r = new CompoundStatement(lxr.Position, id + lxr.start); break; // okay if it's a partially defined method
                case Sqlx.FETCH: r = ParseFetchStatement(); break;
                case Sqlx.FOR: r = ParseForStatement(rt,null); break;
                case Sqlx.GET: r = ParseGetDiagnosticsStatement(); break;
                case Sqlx.HTTP: r = ParseHttpREST(rt); break;
                case Sqlx.IF: r = ParseIfThenElse(rt); break;
                case Sqlx.INSERT: r = ParseSqlInsert(); break;
                case Sqlx.ITERATE: r = ParseIterate(); break;
                case Sqlx.LEAVE: r = ParseBreakLeave(); break;
                case Sqlx.LOOP: r = ParseLoopStatement(rt,null); break;
                case Sqlx.OPEN: r = ParseOpenStatement(); break;
                case Sqlx.REPEAT: r = ParseRepeat(rt,null); break;
                case Sqlx.ROLLBACK: r = new Executable(lxr.Position, Executable.Type.RollbackWork); break;
                case Sqlx.RETURN: r = ParseReturn(rt); break;
                case Sqlx.SELECT: r = ParseSelectSingle(rt); break;
                case Sqlx.SET: r = ParseAssignment(); break;
                case Sqlx.SIGNAL: r = ParseSignal(); break;
                case Sqlx.RESIGNAL: r = ParseSignal(); break;
                case Sqlx.UPDATE: r = ParseSqlUpdate(); break;
                case Sqlx.WHILE: r = ParseSqlWhile(rt,null); break;
				default: throw new DBException("42000",lxr.Diag).ISO();
			}
            return (Executable)cx.Add(r);
		}
        /// <summary>
        /// GetDiagnostics = GET DIAGMOSTICS Target = ItemName { , Target = ItemName }
        /// </summary>
        /// <returns>The executable</returns>
        private Executable ParseGetDiagnosticsStatement()
        {
            Next();
            Mustbe(Sqlx.DIAGNOSTICS);
            var r = new GetDiagnostics(lxr.Position);
            for (; ; )
            {
                var t = ParseSqlValueEntry(Domain.Content,false);
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
                r+=(GetDiagnostics.List,r.list+(t,tok));
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
        /// <param name="rt">the expected data type if any</param>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseLabelledStatement(DBObject rt)
        {
            Ident sc = new Ident(lxr);
            var lp = lxr.Position;
            Mustbe(Sqlx.ID);
            // OOPS: according to SQL 2003 there MUST follow a colon for a labelled statement
            if (tok == Sqlx.COLON)
            {
                Next();
                var s = sc;
                switch (tok)
                {
                    case Sqlx.BEGIN: return (Executable)cx.Add(ParseCompoundStatement(rt,s));
                    case Sqlx.FOR: return (Executable)cx.Add(ParseForStatement(rt,s));
                    case Sqlx.LOOP: return (Executable)cx.Add(ParseLoopStatement(rt,s));
                    case Sqlx.REPEAT: return (Executable)cx.Add(ParseRepeat(rt,s));
                    case Sqlx.WHILE: return (Executable)cx.Add(ParseSqlWhile(rt,s));
                    default:
                        throw new DBException("26000", s).ISO();
                }
            }
            // OOPS: but we'q better allow a procedure call here for backwards compatibility
            else if (tok == Sqlx.LPAREN)
            {
                Next();
                var cp = lxr.Position;
                var ps = ParseSqlValueList(ObInfo.Any);
                Mustbe(Sqlx.RPAREN);
                var arity = (int)ps.Count;
                var pn = sc.Suffix(arity);
                var pr = db.GetProcedure(sc.ident, arity) ??
                    throw new DBException("42108", pn);
                var cs = new CallStatement(cp,pr,ps);
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr.Execute(cs,cx);
                return (Executable)cx.Add(cs);
            }
            // OOPS: and a simple assignment for backwards compatibility
            else
            {
                var vb = (SqlValue)cx.Get(sc);
                Mustbe(Sqlx.EQL);
                var va = ParseSqlValue(rt);
                var sa = new AssignmentStatement(lp,vb,va);
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr.Execute(sa,cx);
                return (Executable)cx.Add(sa);
            }
        }
        /// <summary>
		/// Assignment = 	SET Target '='  TypedValue { ',' Target '='  TypedValue }
		/// 	|	SET '(' Target { ',' Target } ')' '='  TypedValue .
        /// </summary>
        /// <returns>The executable resulting from the parse</returns>
		Executable ParseAssignment()
        {
            var lp = lxr.Position;
            Next();
            if (tok == Sqlx.LPAREN)
                return (Executable)cx.Add(ParseMultipleAssignment());
            var ic = ParseIdentChain();
            var vb = (SqlValue)cx.Get(ic) ?? throw new DBException("42112", ic.ToString());
            Mustbe(Sqlx.EQL);
            var va = ParseSqlValue(vb.domain);
            var sa = new AssignmentStatement(lp,vb,va);
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(sa,cx);
            return (Executable)cx.Add(sa);
        }
        /// <summary>
        /// 	|	SET '(' Target { ',' Target } ')' '='  TypedValue .
        /// Target = 	id { '.' id } .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseMultipleAssignment()
        {
            var ma = new MultipleAssignment(lxr.Position);
            Mustbe(Sqlx.EQL);
            ma = ma + (MultipleAssignment.List,ParseIDList())
                +(MultipleAssignment.Rhs,ParseSqlValue(Domain.Content));
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                ma.Obey(tr,cx);
            return (Executable)cx.Add(ma);
        }
        /// <summary>
        /// |	RETURN TypedValue
        /// </summary>
        /// <returns>the executable resulting from the parse</returns>
		Executable ParseReturn(DBObject rt)
        {
            var rs = new ReturnStatement(lxr.Position);
            Next();
            rs += (ReturnStatement.Ret, ParseSqlValue(rt));
            return (Executable)cx.Add(rs);
        }
        /// <summary>
		/// CaseStatement = 	CASE TypedValue { WHEN TypedValue THEN Statements }+ [ ELSE Statements ] END CASE
		/// |	CASE { WHEN SqlValue THEN Statements }+ [ ELSE Statements ] END CASE .
        /// </summary>
        /// <returns>the Executable resulting from the parse</returns>
		Executable ParseCaseStatement(DBObject rt)
        {
            var old = cx;
            Next();
            Executable e = null;
            if (tok == Sqlx.WHEN)
            {
                e = new SearchedCaseStatement(lxr.Position);
                e+=(SimpleCaseStatement.Whens, ParseWhenList(Domain.Bool,rt));
                if (tok == Sqlx.ELSE)
                {
                    Next();
                    e+= (SimpleCaseStatement.Else,ParseStatementList(rt));
                }
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.CASE);
            }
            else
            {
                var op = ParseSqlValue(Domain.Content);
                e = new SimpleCaseStatement(lxr.Position);
                e+=(SimpleCaseStatement.Operand,op);
                e+=(SimpleCaseStatement.Whens,ParseWhenList(op.domain, rt));
                if (tok == Sqlx.ELSE)
                {
                    Next();
                    e +=(SimpleCaseStatement.Else,ParseStatementList(rt));
                }
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.CASE);
            }
            cx = old;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(e,cx);
            return (Executable)cx.Add(e);
        }
        /// <summary>
        /// { WHEN SqlValue THEN Statements }
        /// </summary>
        /// <returns>the list of Whenparts</returns>
		BList<WhenPart> ParseWhenList(Domain od,DBObject rt)
		{
            var r = BList<WhenPart>.Empty;
			while (tok==Sqlx.WHEN)
			{
				Next();
                var c = ParseSqlValue(od);
				Mustbe(Sqlx.THEN);
                r+=new WhenPart(lxr.Position, c, ParseStatementList(rt));
			}
			return r;
		}
        /// <summary>
		/// ForStatement =	Label FOR [ For_id AS ][ id CURSOR FOR ] QueryExpression DO Statements END FOR [Label_id] .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseForStatement(DBObject rt,Ident n)
        {
            var fs = new ForSelectStatement(lxr.Position, n.ident);
            var old = cx;
            Next();
            Ident c = null;
            if (tok != Sqlx.SELECT)
            {
                c = new Ident(lxr);
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
                        fs+=(ForSelectStatement.ForVn,c);
                        c = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                        Mustbe(Sqlx.CURSOR);
                        Mustbe(Sqlx.FOR);
                    }
                }
            }
            fs = fs + (ForSelectStatement.Sel, ParseCursorSpecification(ObInfo.Any).cs)
            + (ForSelectStatement.Cursor, (c ?? new Ident(lxr)).ident);
            Mustbe(Sqlx.DO);
            fs += (ForSelectStatement.Stms,ParseStatementList(rt));
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.FOR);
            if (tok == Sqlx.ID)
            {
                if (n != null && n.ident != lxr.val.ToString())
                    throw new DBException("42157", lxr.val.ToString(), n).Mix();
                Next();
            }
            cx = old;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(fs,cx);
            return (Executable)cx.Add(fs);
        }
        /// <summary>
		/// IfStatement = 	IF BooleanExpr THEN Statements { ELSEIF BooleanExpr THEN Statements } [ ELSE Statements ] END IF .
        /// </summary>
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseIfThenElse(DBObject rt)
        {
            var ife = new IfThenElse(lxr.Position);
            var old = cx;
            Next();
            ife += (IfThenElse.Search, ParseSqlValue(Domain.Bool));
            Mustbe(Sqlx.THEN);
            ife += (IfThenElse.Then, ParseStatementList(rt));
            var ei = BList<Executable>.Empty;
            while (Match(Sqlx.ELSEIF))
            {
                var eif = new IfThenElse(lxr.Position);
                Next();
                eif += (IfThenElse.Search, ParseSqlValue(rt));
                Mustbe(Sqlx.THEN);
                Next();
                eif += (IfThenElse.Then, ParseStatementList(rt));
                ei += eif;
            }
            ife += (IfThenElse.Elsif, ei);
            var el = BList<Executable>.Empty;
            if (tok == Sqlx.ELSE)
            {
                Next();
                el = ParseStatementList(rt);
            }
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.IF);
            ife += (IfThenElse.Else, el);
            cx = old;
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(ife, cx);
            return (Executable)cx.Add(ife);
        }
        /// <summary>
		/// Statements = 	Statement { ';' Statement } .
        /// </summary>
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		BList<Executable> ParseStatementList(DBObject rt)
		{
            var r = new BList<Executable>(ParseProcedureStatement(rt));
            while (tok==Sqlx.SEMICOLON)
			{
				Next();
                r+=ParseProcedureStatement(rt);
			}
			return r;
		}
        /// <summary>
		/// SelectSingle =	[DINSTINCT] SelectList INTO VariableRef { ',' VariableRef } TableExpression .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSelectSingle(DBObject rt)
        {
            Next();
            var lp = lxr.Position;
            var ss = new SelectSingle(lxr.Position);
            var cs = new CursorSpecification(lp,BTree<long,object>.Empty);
            var qe = new QueryExpression(lp+1,false, BTree<long, object>.Empty);
            var s = new QuerySpecification(lp+2, BTree<long, object>.Empty
                + (QueryExpression._Distinct, ParseDistinctClause()));
            cs+=(CursorSpecification.Union,qe);
            s = ParseSelectList(s,rt as ObInfo);
            s += (Query.Display,s.Size(cx));
            qe+=(QueryExpression._Left,s);
            Mustbe(Sqlx.INTO);
            ss += (SelectSingle.Outs,ParseTargetList());
            if (ss.outs.Count != s.rowType.columns.Count)
                throw new DBException("22007").ISO();
            s+=(QuerySpecification.TableExp,ParseTableExpression(s));
            s += (DBObject.Depth, Math.Max(s.depth, 1 + s.tableExp.depth));
            ss+=(ForSelectStatement.Sel,cs);
            return (Executable)cx.Add(ss);
        }
        /// <summary>
        /// traverse a comma-separated variable list
        /// </summary>
        /// <returns>the list</returns>
		BList<SqlValue> ParseTargetList()
		{
			bool b = (tok==Sqlx.LPAREN);
                if (b)
                    Next();
            var r = BList<SqlValue>.Empty;
            for (; ; )
            {
                r += ParseVarOrColumn(Domain.Content);
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
        SqlValue ParseVarOrColumn(DBObject rt)
        {
            Match(Sqlx.PROVENANCE, Sqlx.TYPE_URI, Sqlx.SYSTEM_TIME, Sqlx.SECURITY);
            if (tok == Sqlx.SECURITY)
            {
                Next();
                return (SqlValue)cx.Add(new SqlFunction(lxr.Position, Sqlx.SECURITY, null, null, null, Sqlx.NO));
            }
            Ident ic = ParseIdentChain();
            var pseudoTok = Sqlx.NO;
            if (Match(Sqlx.PARTITION, Sqlx.POSITION, Sqlx.VERSIONING, Sqlx.CHECK, 
                Sqlx.PROVENANCE, Sqlx.TYPE_URI, Sqlx.SYSTEM_TIME))
            {
                pseudoTok = tok;
                Next();
            }
            var lp = lxr.Position;
            if (tok == Sqlx.LPAREN)
            {
                var (tg,pn) = cx.defs.Split(ic);
                Next();
                var ps = BList<SqlValue>.Empty;
                if (tok!=Sqlx.RPAREN)
                    ps = ParseSqlValueList(rt);
                Mustbe(Sqlx.RPAREN);
                int n = (int)ps.Count;
                var pr = db.GetProcedure(pn, n);
                if (pr==null && (tg==null || tg.domain.kind!=Sqlx.CONTENT)) 
                    throw new DBException("42108", ic.ident);
                var cs = new CallStatement(lp, pr, ps, (SqlValue)tg);
                return (SqlValue)cx.Add(new SqlProcedureCall(ic.iix, cs));
                // notreached
            }
            SqlValue r = null;
            if (ic != null)
            {
                r = cx.Get(ic) as SqlValue;
                if (r == null)
                {
                    var nd = cx.defs;
                    for (var id = ic; nd != null && id != null; id = id.sub)
                    {
                        DBObject ob = null;
                        if (char.IsDigit(id.ident[0])) // cx.dbformat<51 compatibility
                        {
                            ob = (DBObject)db.objects[long.Parse(id.ident)];
                            r = ob.ToSql(id,db);
                        }
                        else if (nd.Contains(id.ident))
                        {
                            (ob, nd) = cx.defs[id.ident];
                            r = ob?.ToSql(id,db);
                        }
                        if (ob == null)
                        {
                            var fm = (r as SqlTable)?.from;
                            if (fm?.rowType is ObInfo ri && ri.map.Contains(id.ident))
                            {
                                var dp = id.iix - 1;
                                var fo = fm;
                                var sr = ri.columns[ri.map[id.ident].Value];
                                if (sr is SqlCol sc && sc.defpos < db.uid)
                                {
                                    //             sr = new SqlValueExpr(dp,Sqlx.DOT,r,sc,Sqlx.NO);
                                    sr = new SqlCol(dp, sc.name, sc.tableCol);
                                    // Modify the from rowType and and enclosing join types.
                                    cx.Replace(sc, sr);
                                }
                                fm = (From)fm.Refresh(cx);
                                if (fm != fo)
                                {
                                    r += (SqlValue._From, fm);
                                    cx.Add(r);
                                    cx.defs += (new Ident(r.name, r.defpos), r);
                                }
                                r = sr;
                            }
                            else
                            {
                                var v = new SqlValue(id.iix, id.ident);
                                r = (r == null) ? v :
                                    new SqlValueExpr(id.iix - 1, Sqlx.DOT, r, v, Sqlx.NO);
                            }
                        }
                    }
                    cx.defs += (ic, r);
                }
                else if (r.defpos > db.loadpos)
                    r = new SqlCopy(ic.iix, r);
                else if (r is SqlOldRowCol sn)
                    r = new SqlOldRowCol(ic.iix, sn.trs, sn);
                else if (r is SqlCol sc)
                    r = new SqlCol(ic.iix, sc.name, sc.tableCol);
                cx.Add(r);
            }
            if (pseudoTok != Sqlx.NO)
                r = new SqlFunction(lxr.Position, pseudoTok, r, null, null, Sqlx.NO);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Parse an identifier
        /// </summary>
        /// <returns>the Ident</returns>
        Ident ParseIdent()
        {
            var c = new Ident(lxr);
            Mustbe(Sqlx.ID);
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
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseLoopStatement(DBObject rt, Ident n)
        {
            var ls = new LoopStatement(lxr.Position, n.ident,cx.cxid);
                Next();
                ls+=(WhenPart.Stms,ParseStatementList(rt));
                Mustbe(Sqlx.END);
                Mustbe(Sqlx.LOOP);
                if (tok == Sqlx.ID && n != null && n?.ident == lxr.val.ToString())
                    Next();
            return (Executable)cx.Add(ls);
        }
        /// <summary>
		/// While =		Label WHILE SqlValue DO Statements END WHILE .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseSqlWhile(DBObject rt, Ident n)
        {
            var ws = new WhileStatement(lxr.Position, n.ident);
            var old = cx; // new SaveContext(lxr, ExecuteStatus.Parse);
            Next();
            ws+=(WhileStatement.Search,ParseSqlValue(Domain.Bool));
            Mustbe(Sqlx.DO);
            ws+=(WhileStatement.What,ParseStatementList(rt));
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.WHILE);
            if (tok == Sqlx.ID && n != null && n?.ident == lxr.val.ToString())
                Next();
            cx = old; // old.Restore(lxr);
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(ws,cx);
            return (Executable)cx.Add(ws);
        }
        /// <summary>
		/// Repeat =		Label REPEAT Statements UNTIL BooleanExpr END REPEAT .
        /// </summary>
        /// <param name="n">the label</param>
        /// <param name="rt">The data type of the test expression</param>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseRepeat(DBObject rt, Ident n)
        {
            var rs = new RepeatStatement(lxr.Position, n?.ident);
            Next();
            rs+=(WhileStatement.What,ParseStatementList(rt));
            Mustbe(Sqlx.UNTIL);
            rs+=(WhileStatement.Search,ParseSqlValue(Domain.Bool));
            Mustbe(Sqlx.END);
            Mustbe(Sqlx.REPEAT);
            if (tok == Sqlx.ID && n != null && n?.ident == lxr.val.ToString())
                Next();
            return (Executable)cx.Add(rs);
        }
        /// <summary>
        /// Parse a break or leave statement
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseBreakLeave()
		{
			Sqlx s = tok;
			Ident n = null;
			Next();
			if (s==Sqlx.LEAVE && tok==Sqlx.ID)
			{
				n = new Ident(lxr);
				Next();
			}
			return (Executable)cx.Add(new BreakStatement(lxr.Position,n.ident)); 
		}
        /// <summary>
        /// Parse an iterate statement
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
        Executable ParseIterate()
		{
			Next();
			var n = new Ident(lxr);
			Mustbe(Sqlx.ID);
			return (Executable)cx.Add(new IterateStatement(lxr.Position, n.ident,cx.cxid)); 
		}
        /// <summary>
        /// |	SIGNAL (id|SQLSTATE [VALUE] string) [SET item=Value {,item=Value} ]
        /// |   RESIGNAL [id|SQLSTATE [VALUE] string] [SET item=Value {,item=Value} ]
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
		Executable ParseSignal()
        {
            Sqlx s = tok;
            TypedValue n = null;
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
            var r = new Signal(lxr.Position, n.ToString()) + (Signal.SType, s);
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
                    r+=(Signal.SetList, r.setlist+(k, ParseSqlValue(Domain.Content)));
                    if (tok != Sqlx.COMMA)
                        break;
                    Next();
                }
            }
            return (Executable)cx.Add(r);
        }
        /// <summary>
		/// Open =		OPEN id .
        /// </summary>
        /// <returns>the Executable result of the parse</returns>
 		Executable ParseOpenStatement()
		{
			Next();
			var o = new Ident(lxr);
			Mustbe(Sqlx.ID);
			return (Executable)cx.Add(new OpenStatement(lxr.Position,
                cx.Get(o) as SqlCursor?? throw new DBException("34000",o.ToString()),
                cx.cxid));
		}
        /// <summary>
		/// Close =		CLOSE id .
        /// </summary>
        /// <returns>The Executable result of the parse</returns>
		Executable ParseCloseStatement()
		{
			Next();
			var o = new Ident(lxr);
			Mustbe(Sqlx.ID);
			return (Executable)cx.Add(new CloseStatement(lxr.Position,
                cx.Get(o) as SqlCursor?? throw new DBException("34000", o.ToString()),
                cx.cxid));
		}
        /// <summary>
		/// Fetch =		FETCH Cursor_id INTO VariableRef { ',' VariableRef } .
        /// </summary>
        /// <returns>The Executable result of the parse</returns>
        Executable ParseFetchStatement()
        {
            Next();
            var how = Sqlx.NEXT;
            SqlValue where = null;
            if (Match(Sqlx.NEXT, Sqlx.PRIOR, Sqlx.FIRST, Sqlx.LAST, Sqlx.ABSOLUTE, Sqlx.RELATIVE))
            {
                how = tok;
                Next();
            }
            if (how == Sqlx.ABSOLUTE || how == Sqlx.RELATIVE)
                where = ParseSqlValue(Domain.Int);
            if (tok == Sqlx.FROM)
                Next();
            var o = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var fs = new FetchStatement(lxr.Position, 
                cx.Get(o) as SqlCursor 
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
        internal WhenPart ParseTriggerDefinition(PTrigger trig)
        {
            var fm = trig.from;
            var ti = fm.rowType;
            cx.AddOldTable(trig.oldTable, fm);
            cx.AddOldRow(trig.oldRow,ti);
            cx.AddTable(trig.newTable, fm);
            cx.AddRow(trig.newRow, ti);
            var needed = BTree<SqlValue, Ident>.Empty; //ignored
            SqlValue when = null;
            Executable act = null;
            if (tok == Sqlx.WHEN)
            {
                Next();
                when = ParseSqlValue(Domain.Bool);
            }
            if (tok == Sqlx.BEGIN)
            {
                Next();
                var cs = new CompoundStatement(lxr.Position,"");
                var ss = BList<Executable>.Empty;
                Mustbe(Sqlx.ATOMIC);
                while (tok != Sqlx.END)
                {
                    ss+=ParseProcedureStatement(Domain.Content); 
                    if (tok == Sqlx.END)
                        break;
                    Mustbe(Sqlx.SEMICOLON);
                }
                Next();
                cs+=(CompoundStatement.Stms,ss);
                cs += (DBObject.Dependents, _Deps(ss));
                act = cs;
            }
            else
                act = ParseProcedureStatement(Domain.Content);
            return (WhenPart)cx.Add(new WhenPart(lxr.Position, when, new BList<Executable>(act)));
        }
        public Executable ParseTriggerDefClause(string sql)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            return ParseTriggerDefClause();
        }
        /// <summary>
        /// |	CREATE TRIGGER id (BEFORE|AFTER) Event ON id [ RefObj ] Trigger
        /// RefObj = REFERENCING  { (OLD|NEW)[ROW|TABLE][AS] id } .
        /// Trigger = FOR EACH ROW ...
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseTriggerDefClause()
        {
            var ct = new Executable(lxr.Position, Executable.Type.CreateTrigger);
            var trig = new Ident(lxr);
            Mustbe(Sqlx.ID);
            PTrigger.TrigType tgtype = (PTrigger.TrigType)0;
            var w = Mustbe(Sqlx.BEFORE, Sqlx.INSTEAD, Sqlx.AFTER);
            switch (w)
            {
                case Sqlx.BEFORE: tgtype |= PTrigger.TrigType.Before; break;
                case Sqlx.AFTER: tgtype |= PTrigger.TrigType.After; break;
                case Sqlx.INSTEAD: Mustbe(Sqlx.OF); tgtype |= PTrigger.TrigType.Instead; break;
            }
            var qp = lxr.Position;
            tgtype = ParseTriggerHow(tgtype);
            var cls = BList<Ident>.Empty;
            if ((tgtype & PTrigger.TrigType.Update) == PTrigger.TrigType.Update && tok == Sqlx.OF)
            {
                Next();
                cls += new Ident(lxr);
                Mustbe(Sqlx.ID);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    cls += new Ident(lxr);
                    Mustbe(Sqlx.ID);
                }
            }
            Mustbe(Sqlx.ON);
            var tabl = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var tb = db.GetObject(tabl.ident) as Table??
                throw new DBException("42107", tabl.ToString()).Mix();
            Ident or = null, nr = null, ot = null, nt = null;
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
                            ot = new Ident(lxr);
                            Mustbe(Sqlx.ID);
                            cx.defs += (ot, new FromOldTable(ot, 
                                new From(ot.iix,tb,(ObInfo)db.role.obinfos[tb.defpos])));
                            continue;
                        }
                        if (tok == Sqlx.ROW)
                            Next();
                        if (or != null)
                            throw new DBException("42143", "OLD ROW").Mix();
                        if (tok == Sqlx.AS)
                            Next();
                        or = new Ident(lxr);
                        cx.defs += (or, new ObInfoOldRow((ObInfo)db.role.obinfos[tb.defpos],tb.defpos));
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
                            nt = new Ident(lxr);
                            Mustbe(Sqlx.ID);
                            continue;
                        }
                        if (tok == Sqlx.ROW)
                            Next();
                        if (nr != null)
                            throw new DBException("42143", "NEW ROW").Mix();
                        if (tok == Sqlx.AS)
                            Next();
                        nr = new Ident(lxr);
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
                    tgtype |= PTrigger.TrigType.EachStatement;
                }
            }
            if ((tgtype & PTrigger.TrigType.EachRow) != PTrigger.TrigType.EachRow)
            {
                if (nr != null || or != null)
                    throw new DBException("42148").Mix();
            }
            var old = cx; // new SaveContext(lxr, ExecuteStatus.Parse);
            int st = lxr.start;
            var fm = _From(tabl.iix,tb,null,QuerySpecification.Default);
            if (tb == null)
                tb = db.objects[fm.target] as Table;
            fm +=(Query.SimpleQuery,fm);
            if (ot != null)
                cx.defs += (ot, tb);
            if (nt != null)
                cx.defs += (nt, tb);
            if (or != null)
                cx.defs += (or, fm.rowType);
            if (nr != null)
                cx.defs += (nr, fm.rowType);
            st = lxr.start;
            var cols = new long[cls.Count];
            for (int i = 0; i < cols.Length; i++)
                cols[i] = fm.rowType.ColFor(cls[i].ident).defpos; 
            var pt = new PTrigger(trig.ident, fm, (int)tgtype, cols,
                    or, nr, ot, nt, null,
                    new Ident(new string(lxr.input, st, lxr.input.Length - st), st),
                        cx, db);
            var op = db.parse;
            db += (Database._ExecuteStatus, ExecuteStatus.Parse);
            pt.def = ParseTriggerDefinition(pt);
            pt.src = new Ident(new string(lxr.input, st, lxr.pos - st), st);
            db += (Database._ExecuteStatus, op);
            cx = old;
            if (tb != null && db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr + pt;
            return (Executable)cx.Add(ct);
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
        /// <returns>the Executable</returns>
		Executable ParseAlter()
		{
			Next();
            MethodModes();
            Match(Sqlx.DOMAIN,Sqlx.TYPE,Sqlx.ROLE,Sqlx.VIEW);
			switch (tok)
			{
				case Sqlx.TABLE: return ParseAlterTable();
				case Sqlx.DOMAIN: return ParseAlterDomain();
				case Sqlx.TYPE: return ParseAlterType();
				case Sqlx.FUNCTION: return ParseAlterProcedure();
				case Sqlx.PROCEDURE: return ParseAlterProcedure();
				case Sqlx.OVERRIDING: return ParseMethodDefinition();
				case Sqlx.INSTANCE: return ParseMethodDefinition();
				case Sqlx.STATIC: return ParseMethodDefinition();
				case Sqlx.CONSTRUCTOR: return ParseMethodDefinition();
				case Sqlx.METHOD: return ParseMethodDefinition();
                case Sqlx.VIEW: return ParseAlterView();
				default:
					throw new DBException("42125",tok).Mix();
			}
		}
        /// <summary>
        /// id AlterTable { ',' AlterTable } 
        /// </summary>
        /// <returns>the Executable</returns>
        Executable ParseAlterTable()
        {
            var at = new Executable(lxr.Position, Executable.Type.AlterTable);
            Next();
            Table tb = null;
            var o = new Ident(lxr);
            Mustbe(Sqlx.ID);
            tb = db.GetObject(o.ident) as Table??
                throw new DBException("42107", o).Mix();
            ParseAlterTableOps(tb);
            return (Executable)cx.Add(at);
        }
        /// <summary>
        /// AlterView = SET (INSERT|UPDATE|DELETE) SqlStatement
        ///     |   TO id
        ///     |   SET SOURCE To QueryExpression
        ///     |   [DROP]TableMetadata
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseAlterView()
        {
            var at = new Executable(lxr.Position, Executable.Type.AlterTable);
            Next();
            var nm = new Ident(lxr);
            Mustbe(Sqlx.ID);
            DBObject vw = db.GetObject(nm.ident) ??
                throw new DBException("42107", nm).Mix();
            ParseAlterOp(vw);
            while (tok == Sqlx.COMMA)
            {
                Next();
                ParseAlterOp(vw);
            }
            return (Executable)cx.Add(at);
        }
        /// <summary>
        /// Parse an alter operation
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="ob">the object to be affected</param>
        DBObject ParseAlterOp(DBObject ob)
        {
            var tr = db as Transaction?? throw new DBException("2F003");
            var kind = (ob is View)?Sqlx.VIEW:Sqlx.FUNCTION;
            var oi = db.role.obinfos[ob.defpos] as ObInfo;
            if (tok == Sqlx.SET && ob is View)
            {
                Next();
                int st;
                string s;
                long lp;
                if (Match(Sqlx.SOURCE))
                {
                    Next();
                    Mustbe(Sqlx.TO);
                    st = lxr.start;
                    lp = lxr.Position;
                    var qe = ParseQueryExpression(ObInfo.Any);
                    s = new string(lxr.input,st, lxr.start - st);
                    db = tr + new Modify("Source", ob.defpos, s, qe, tr);
                    return cx.Add(tr,ob.defpos);
                }
                var t = tok;
                Mustbe(Sqlx.INSERT, Sqlx.UPDATE, Sqlx.DELETE);
                Mustbe(Sqlx.TO);
                st = lxr.start;
                lp = lxr.Position;
                ParseSqlStatement();
                s = new string(lxr.input,st, lxr.start - st);
                string n = "";
                switch (t)
                {
                    case Sqlx.INSERT: n = "Insert"; break;
                    case Sqlx.UPDATE: n = "Update"; break;
                    case Sqlx.DELETE: n = "Delete"; break;
                }
                tr +=new Modify(n, ob.defpos, s, null, tr);
            }
            else if (tok == Sqlx.TO)
            {
                Next();
                var nm = lxr.val;
                var lp = lxr.Position;
                Mustbe(Sqlx.ID);
                tr +=new Change(ob.defpos, nm.ToString(), tr);
            }
            else if (tok == Sqlx.ALTER)
            {
                Next();
                var ic = new Ident(lxr);
                Mustbe(Sqlx.ID);
                ob = db.GetObject(ic.ident) ??
                    throw new DBException("42135", ic.ident);
                    if (tr.role.Denied(tr, Grant.Privilege.AdminRole))
                        throw new DBException("42105", db.role.name??"").Mix();
                    var m = ParseMetadata(db, ob, -1, Sqlx.COLUMN);
                    tr +=new PMetadata(ic.ident, m, ob.defpos, m.refpos, tr);
            }
            if (StartMetadata(kind) || Match(Sqlx.ADD))
            {
                var lp = lxr.Position;
                if (tr.role.Denied(tr, Grant.Privilege.AdminRole))
                    throw new DBException("42105", db.role.name ?? "").Mix();
                if (tok == Sqlx.ALTER)
                    Next();
                var m = ParseMetadata(db, ob, -1, kind);
                tr+=new PMetadata(oi.name, m, -1, ob.defpos, tr);
            }
            db = tr;
            return cx.Add(tr, ob.defpos);
        }
        /// <summary>
        /// id AlterDomain { ',' AlterDomain } 
        /// </summary>
        /// <returns>the Executable</returns>
        Executable ParseAlterDomain()
        {
            var ad = new Executable(lxr.Position, Executable.Type.AlterDomain);
                Next();
                var c = ParseIdent();
                var d = db.GetObInfo(c.ident).domain;
                ParseAlterDomainOp(d);
                while (tok == Sqlx.COMMA)
                {
                    Next();
                    ParseAlterDomainOp(d);
                }
            return (Executable)cx.Add(ad);
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
        void ParseAlterDomainOp(Domain d)
		{
            var tr = db as Transaction?? throw new DBException("2F003");
            var oi = db.role.obinfos[d.defpos] as ObInfo;
            if (tok == Sqlx.SET)
			{
				Next();
				Mustbe(Sqlx.DEFAULT);
				int st = lxr.start;
                var lp = lxr.Position;
				var dv = ParseSqlValue(d);
				string ds = new string(lxr.input,st,lxr.start-st);
                if (db.parse == ExecuteStatus.Obey)
                    tr+=new Edit(d, oi.name, d+(Domain.Default,dv)+(Domain.DefaultString,ds), tr);
			} 
			else if (Match(Sqlx.ADD))
			{
				Next();
                Ident id;
                if (tok == Sqlx.CONSTRAINT)
                {
                    Next();
                    id = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                }
                else 
                    id = new Ident(lxr);
				Mustbe(Sqlx.CHECK);
				Mustbe(Sqlx.LPAREN);
                int st = lxr.pos;
                var sc = ParseSqlValue(Domain.Bool);
                string source = new string(lxr.input,st, lxr.pos - st - 1);
				Mustbe(Sqlx.RPAREN);
                if (tr.parse == ExecuteStatus.Obey)
                {
                    var ck = new PCheck(d.defpos, id.ident, source, tr);
                    tr+=ck;
                }
			}
            else if (tok == Sqlx.DROP)
            {
                Ident n = null;
                var lp = lxr.Position;
                Next();
                if (tok == Sqlx.DEFAULT)
                {
                    Next();
                    if (tr.parse == ExecuteStatus.Obey)
                        tr+=new Edit(d,oi.name, d, tr);
                }
                else if (StartMetadata(Sqlx.DOMAIN) || Match(Sqlx.ADD, Sqlx.DROP))
                {
                    if (tr.role == null || 
                        tr.role.Denied(tr, Grant.Privilege.AdminRole))
                        throw new DBException("42105", tr.role.name ?? "").Mix();
                   tr += new PMetadata(oi.name, ParseMetadata(db, d, -1, Sqlx.DOMAIN), -1, 
                        d.defpos, tr);
                }
                else
                {
                    Mustbe(Sqlx.CONSTRAINT);
                    n = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    Drop.DropAction s = ParseDropAction();
                    var ch = db.GetObject(n.ident) as Check;
                    if (db.parse == ExecuteStatus.Obey)
                        tr += new Drop1(ch.defpos, s, tr);
                }
            }
            else if (StartMetadata(Sqlx.DOMAIN) || Match(Sqlx.ADD, Sqlx.DROP))
            {
                var lp = lxr.Position;
                if (tr.role.Denied(tr, Grant.Privilege.AdminRole))
                    throw new DBException("42105", db.role.name??"").Mix();
                tr+=new PMetadata(oi.name,ParseMetadata(db, d, -1, Sqlx.DOMAIN), -1, d.defpos,
                    tr);
            }
            else
            {
                Mustbe(Sqlx.TYPE);
                var lp = lxr.Position;
                Domain dt = ParseSqlDataType().domain;
                if (tr.parse == ExecuteStatus.Obey)
                    tr+=new Edit(d, oi.name, dt, tr);
            }
            db = tr;
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
                case Sqlx.CASCADE: r = Drop.DropAction.Cascade; Next();
                    db += (Database.Cascade, true);
                    break;
                case Sqlx.RESTRICT: r = Drop.DropAction.Restrict; Next(); break;
            }
            return r;
		}
        /// <summary>
        /// |   ALTER TABLE id AlterTable { ',' AlterTable }
        /// </summary>
        /// <param name="tb">the database</param>
        /// <param name="tb">the table</param>
        void ParseAlterTableOps(Table tb)
		{
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
        void ParseAlterTable(Table tb)
        {
            var tr = db as Transaction?? throw new DBException("2F003");
            if (tok == Sqlx.TO)
            {
                Next();
                var o = lxr.val;
                var lp = lxr.Position;
                Mustbe(Sqlx.ID);
                if (tr.parse == ExecuteStatus.Obey)
                    db = tr+new Change(tb.defpos, o.ToString(), tr);
                return;
            } 
            if (tok==Sqlx.LEVEL)
            {
                ParseClassification(tb.defpos);
                return;
            }
            if (tok==Sqlx.SCOPE)
            {
                ParseEnforcement(tb.defpos);
                return;
            }
            Match(Sqlx.ADD);
            switch (tok)
            {
                case Sqlx.DROP:
                    {
                        Next();
                        switch (tok)
                        {
                            case Sqlx.CONSTRAINT:
                                {
                                    Next();
                                    var name = new Ident(lxr);
                                    Mustbe(Sqlx.ID);
                                    Drop.DropAction act = ParseDropAction();
                                    if (tb != null)
                                    {
                                        var ck = db.GetObject(name.ident) as Check;
                                        if (ck != null && db.parse == ExecuteStatus.Obey)
                                        {
                                            db = tr+new Drop1(ck.defpos, act, tr);
                                            return;
                                        }
                                    }
                                    else
                                        return;
                                    break;
                                }
                            case Sqlx.PRIMARY:
                                {
                                    Next();
                                    Mustbe(Sqlx.KEY);
                                    var lp = lxr.Position;
                                    Drop.DropAction act = ParseDropAction();
                                    var cols = ParseColsList(tb);
                                    if (tb != null)
                                    {
                                        Index x = tb.FindIndex(db,cols);
                                        if (x!=null && db.parse == ExecuteStatus.Obey)
                                        {
                                            db =tr+ new Drop1(x.defpos, act, tr);
                                            return;
                                        }
                                    }
                                    else
                                        return;
                                    break;
                                }
                            case Sqlx.FOREIGN:
                                {
                                    Next();
                                    Mustbe(Sqlx.KEY);
                                    var cols = ParseColsList(tb);
                                    Mustbe(Sqlx.REFERENCES);
                                    var n = new Ident(lxr);
                                    Mustbe(Sqlx.ID);
                                    var rt = db.GetObject(n.ident) as Table??
                                        throw new DBException("42107", n).Mix();
                                    var rcols = CList<TableColumn>.Empty;
                                    var st = lxr.pos;
                                    if (tok == Sqlx.LPAREN)
                                        rcols = ParseColsList(rt);
                                    if (tr.parse == ExecuteStatus.Obey)
                                    {
                                        var x = tb.FindIndex(db,cols);
                                        if (x != null)
                                        {
                                            db = tr+new Drop(x.defpos, tr);
                                            return;
                                        }
                                        throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                                    }
                                    else
                                        return;
                                }
                            case Sqlx.UNIQUE:
                                {
                                    Next();
                                    var st = lxr.pos;
                                    var lp = lxr.Position;
                                    var cols = ParseColsList(tb);
                                    if (db.parse == ExecuteStatus.Obey)
                                    {
                                        var x = tb.FindIndex(db,cols);
                                        if (x != null)
                                        {
                                            db=tr+new Drop(x.defpos, tr);
                                            return;
                                        }
                                        throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                                    }
                                    else
                                        return;
                                }
                            case Sqlx.PERIOD:
                                {
                                    var lp = lxr.Position;
                                    var ptd = ParseTablePeriodDefinition();
                                    var pd = (ptd.pkind==Sqlx.SYSTEM_TIME)?tb.systemPS:tb.applicationPS;
                                    if (pd >0)
                                        db= tr+new Drop(pd, tr);
                                    return;
                                }
                            case Sqlx.WITH:
                                tb = ParseVersioningClause(tb, true); return;
                            default:
                                {
                                    if (StartMetadata(Sqlx.TABLE))
                                    {
                                        var lp = lxr.Position;
                                        var ti = tr.role.obinfos[tb.defpos] as ObInfo;
                                        if (tr.role.Denied(tr, Grant.Privilege.AdminRole))
                                            throw new DBException("42105", db.role.name ?? "").Mix();
                                        db=tr+new PMetadata(ti.name, ParseMetadata(tr, tb, -1, 
                                            Sqlx.TABLE,true), -1, tb.defpos, tr);
                                        return;
                                    }
                                    if (tok == Sqlx.COLUMN)
                                        Next();
                                    var name = new Ident(lxr);
                                    Mustbe(Sqlx.ID);
                                    Drop.DropAction act = ParseDropAction();
                                    if (tb != null)
                                    {
                                        var ft = tr.role.obinfos[tb.defpos] as ObInfo;
                                        var tc = ft.ColFor(name.ident);
                                        if (tc != null && tr.parse == ExecuteStatus.Obey)
                                        {
                                            db=tr+new Drop1(tc.defpos, act, tr);
                                            return;
                                        }
                                    }
                                    break;
                                }
                        }
                        break;
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
                        {
                            tb = ParseColumnDefin(tb);
                            tr = (Transaction)db; //!!
                            if (tr.physicals.Last()?.value() is PColumn pc 
                                && pc.notNull && tb.tableRows.PositionAt(0) != null)
                                    throw new DBException("42130");
                        }
                        break;
                    }
                case Sqlx.ALTER:
                    {
                        Next();
                        if (tok == Sqlx.COLUMN)
                            Next();
                        SqlValue col = null;
                        var o = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                        if (tb != null)
                        {
                            var ft = db.role.obinfos[tb.defpos] as ObInfo;
                            col = ft.ColFor(o.ident) as SqlValue;
                        }
                        if (col == null)
                            throw new DBException("42112", o.ident).Mix();
                        while (StartMetadata(Sqlx.COLUMN) || Match(Sqlx.TO,Sqlx.POSITION,Sqlx.SET,Sqlx.DROP,Sqlx.ADD,Sqlx.TYPE))
                            ParseAlterColumn(tb, col);
                        break;
                    }
                case Sqlx.PERIOD:
                    {
                        if (Match(Sqlx.ID))
                        {
                            var pid = lxr.val;
                            Next();
                            Mustbe(Sqlx.TO);
                            var pd = db.objects[tb.applicationPS] as PeriodDef;
                            if (pd == null)
                                throw new DBException("42162", pid).Mix();
                            pid = lxr.val;
                            var lp = lxr.Position;
                            Mustbe(Sqlx.ID);
                            if (db.parse == ExecuteStatus.Obey)
                                db=tr+new Change(pd.defpos, pid.ToString(), tr);
                        }
                        tb = AddTablePeriodDefinition(tb);
                        break;
                    }
                case Sqlx.SET:
                    {
                        Next();
                        var cols = ParseColsList(tb);
                        Mustbe(Sqlx.REFERENCES);
                        var n = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                        var rt = db.GetObject(n.ident) as Table ??
                            throw new DBException("42107", n).Mix();
                        var rcols = CList<TableColumn>.Empty;
                        var st = lxr.pos;
                        if (tok == Sqlx.LPAREN)
                            rcols = ParseColsList(rt);
                        PIndex.ConstraintType ct=0;
                        if (tok == Sqlx.ON)
                            ct = ParseReferentialAction();
                        if (db.parse == ExecuteStatus.Obey)
                        {
                            var x = tb.FindIndex(db, cols);
                            if (x != null)
                            {
                                db = tr+new RefAction(x.defpos, ct, tr);
                                return;
                            }
                            throw new DBException("42135", new string(lxr.input, st, lxr.pos)).Mix();
                        }
                        else
                            return;
                    }
                default:
                    if (StartMetadata(Sqlx.TABLE) || Match(Sqlx.ADD, Sqlx.DROP))
                        if (db.parse == ExecuteStatus.Obey)
                        {
                            var lp = lxr.Position;
                            var oi = tr.role.obinfos[tb.defpos] as ObInfo;
                            if (tb.Denied(tr, Grant.Privilege.AdminRole))
                                throw new DBException("42015",oi.name);
                            db=tr+new PMetadata(oi.name, ParseMetadata(db, tb, -1, Sqlx.TABLE), 
                                -1, tb.defpos, tr);
                        }
                    break;
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
        /// <param name="col">the table column object</param>
        void ParseAlterColumn(Table tb, SqlValue col)
		{
            var tr = db as Transaction?? throw new DBException("2F003");
			TypedValue o = null;
            var ti = db.role.obinfos[tb.defpos] as ObInfo;
            var tc = db.objects[col.defpos] as TableColumn;
            string nm = col.name;
            if (tok == Sqlx.TO)
            {
                Next();
                var n = new Ident(lxr);
                Mustbe(Sqlx.ID);
                if (db.parse == ExecuteStatus.Obey)
                    db= tr+new Change(col.defpos, n.ident, tr);
                return;
            }
            if (tok == Sqlx.POSITION)
            {
                Next();
                o = lxr.val;
                var lp = lxr.Position;
                Mustbe(Sqlx.INTEGERLITERAL);
                if (db.parse == ExecuteStatus.Obey)
                    db = tr+new Alter3(col.defpos, nm, o.ToInt().Value, tc.tabledefpos, 
                        col.domain.defpos, 
                        tc.generated.gfs??tc.domain.defaultValue?.ToString()??"",
                        tc.domain.defaultValue,
                        "",tc.update,tc.notNull, tc.generated, tr);
                return;
            }
            if (Match(Sqlx.ADD))
            {
                Next();
                if (StartMetadata(Sqlx.COLUMN))
                {
                    var lp = lxr.Position;
                    if (tb.Denied(tr, Grant.Privilege.AdminRole))
                        throw new DBException("42105", ti.name).Mix();
                    db = tr+new PMetadata(col.name, 
                        ParseMetadata(db, col, -1, Sqlx.COLUMN, false), 0, col.defpos, tr);
                    return;
                }
                if (tok == Sqlx.CONSTRAINT)
                    Next();
                var n = new Ident(lxr);
                Mustbe(Sqlx.ID);
                Mustbe(Sqlx.CHECK);
                Mustbe(Sqlx.LPAREN);
                int st = lxr.pos;
                ParseSqlValue(Domain.Bool);
                string source = new string(lxr.input,st, lxr.pos - st - 1);
                Mustbe(Sqlx.RPAREN);
                if (db.parse == ExecuteStatus.Obey)
                    db=tr+new PCheck(col.defpos, n.ident, source, tr);
                return;
            }
            if (tok == Sqlx.SET)
            {
                Next();
                if (tok == Sqlx.DEFAULT)
                {
                    Next();
                    int st = lxr.start;
                    var lp = lxr.Position;
                    var dv = lxr.val;
                    Next();
                    var ds = new string(lxr.input, st, lxr.start - st);
                    if (db.parse == ExecuteStatus.Obey)
                        db=tr+new Alter3(col.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, 
                            tc.domain.defpos,ds,dv,"",
                            BList<UpdateAssignment>.Empty, tc.notNull, GenerationRule.None, 
                            tr);
                    return;
                }
                if (Match(Sqlx.GENERATED))
                {
                    Domain type = null;
                    var lp = lxr.Position;
                    var gr = ParseGenerationRule(tb);
                    if (db.parse == ExecuteStatus.Obey)
                    {
                        tc.ColumnCheck(tr, true);
                        db=tr+new Alter3(tc.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, 
                            tc.domain.defpos,
                            gr.gfs??type.defaultValue?.ToString()??"",
                            type.defaultValue, 
                            "",BList<UpdateAssignment>.Empty, tc.notNull, gr, 
                            tr);
                    }
                    return;
                }
                if (Match(Sqlx.NOT))
                {
                    Next();
                    var lp = lxr.Position;
                    Mustbe(Sqlx.NULL);
                    if (db.parse == ExecuteStatus.Obey)
                    {
                        tc.ColumnCheck(tr, false);
                        db=tr+new Alter3(tc.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, tc.domain.defpos,
                            "",TNull.Value, "", BList<UpdateAssignment>.Empty, true, 
                            tc.generated, tr);
                    }
                    return;
                }
                tb = ParseColumnConstraint(tb, col);
                return;
            }
            else if (tok == Sqlx.DROP)
            {
                Next();
                if (StartMetadata(Sqlx.COLUMN))
                {
                    var lp = lxr.Position;
                    if (tb.Denied(tr, Grant.Privilege.AdminRole))
                        throw new DBException("42105", ti.name ?? "").Mix();
                    db=tr+new PMetadata(col.name, 
                            ParseMetadata(db, col, -1, Sqlx.COLUMN, true), 0,
                        col.defpos, tr);
                    return;
                }
                if (tok != Sqlx.DEFAULT && tok != Sqlx.NOT && tok != Sqlx.PRIMARY && tok != Sqlx.REFERENCES && tok != Sqlx.UNIQUE && tok != Sqlx.CONSTRAINT && !StartMetadata(Sqlx.COLUMN))
                    throw new DBException("42000", lxr.Diag).ISO();
                if (tok == Sqlx.DEFAULT)
                {
                    var lp = lxr.Position;
                    Next();
                    if (db.parse == ExecuteStatus.Obey)
                        db=tr+new Alter3(col.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, 
                            tc.domain.defpos,"", null, tc.updateString, tc.update, tc.notNull, 
                            GenerationRule.None, tr);
                    return;
                }
                else if (tok == Sqlx.NOT)
                {
                    Next();
                    var lp = lxr.Position;
                    Mustbe(Sqlx.NULL);
                    if (db.parse == ExecuteStatus.Obey)
                        db=tr+new Alter3(col.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, tc.domain.defpos,
                            tc.domain.defaultString,tc.domain.defaultValue, 
                            tc.updateString, tc.update, false, 
                            tc.generated, tr);
                    return;
                }
                else if (tok == Sqlx.PRIMARY)
                {
                    Next();
                    var lp = lxr.Position;
                    Mustbe(Sqlx.KEY);
                    Drop.DropAction act = ParseDropAction();
                    if (db.parse == ExecuteStatus.Obey)
                    {
                        Index x = tb.FindPrimaryIndex(db);
                        if (x.keys.Count != 1 
                            || x.keys[0].defpos != col.defpos)
                            throw new DBException("42158", col.name, ti.name).Mix()
                                .Add(Sqlx.TABLE_NAME,new TChar(ti.name))
                                .Add(Sqlx.COLUMN_NAME,new TChar(col.name));
                        db=tr+ new Drop1(x.defpos, act, tr);
                    }
                    return;
                }
                else if (tok == Sqlx.REFERENCES)
                {
                    Next();
                    var n = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    Ident k = null;
                    if (tok == Sqlx.LPAREN)
                    {
                        Next();
                        k = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                        Mustbe(Sqlx.RPAREN);
                    }
                    Index dx = null;
                    Table rt = null;
                    ObInfo ri = null;
                    for (var p = tb.indexes.First();p!= null;p=p.Next())
                    {
                        var x = (Index)db.objects[p.value()];
                        if (x.keys.Count != 1 || x.keys[0].defpos != col.defpos)
                            continue;
                        rt = db.objects[x.reftabledefpos] as Table;
                        ri = db.role.obinfos[rt.defpos] as ObInfo;
                        if (ri.name == n.ident)
                        {
                            dx = x;
                            break;
                        }
                    }
                    if (dx == null)
                        throw new DBException("42159", col.name, n.ident).Mix()
                            .Add(Sqlx.TABLE_NAME,new TChar(n.ident))
                            .Add(Sqlx.COLUMN_NAME,new TChar(col.name));
                    if (db.parse == ExecuteStatus.Obey)
                        db = tr+new Drop(dx.defpos, tr);
                    return;
                }
                else if (tok == Sqlx.UNIQUE)
                {
                    var lp = lxr.Position;
                    Next();
                    Index dx = null;
                    for (var p =tb.indexes.First();p!= null;p=p.Next())
                    {
                        var x = (Index)db.objects[p.value()];
                        if (x.keys.Count != 1 
                            || x.keys[0].defpos != col.defpos)
                            continue;
                        if ((x.flags & PIndex.ConstraintType.Unique) == PIndex.ConstraintType.Unique)
                        {
                            dx = x;
                            break;
                        }
                    }
                    if (dx == null)
                        throw new DBException("42160", nm).Mix()
                            .Add(Sqlx.TABLE_NAME,new TChar(nm));
                    if (db.parse == ExecuteStatus.Obey)
                        db=tr+new Drop(dx.defpos, tr);
                    return;
                }
                else if (tok == Sqlx.CONSTRAINT)
                {
                    var n = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    Drop.DropAction s = ParseDropAction();
                    var ch = db.GetObject(n.ident) as Check;
                    if (db.parse == ExecuteStatus.Obey)
                        db= tr+new Drop1(ch.defpos, s, tr);
                    return;
                }
            }
            else if (Match(Sqlx.TYPE))
            {
                Next();
                long dm = -1L;
                var lp = lxr.Position;
                Domain type = null;
                if (tok == Sqlx.ID)
                {
                    var domain = new Ident(lxr);
                    Next();
                    type = db.GetObInfo(domain.ident).domain;
                    if (type == null)
                        throw new DBException("42119", domain.ident, db.name).Mix()
                            .Add(Sqlx.CATALOG_NAME, new TChar(db.name))
                            .Add(Sqlx.TYPE, new TChar(domain.ident));
                    dm = type.defpos;
                }
                else if (Match(Sqlx.CHARACTER, Sqlx.CHAR, Sqlx.VARCHAR, Sqlx.NATIONAL, Sqlx.NCHAR,
                    Sqlx.BOOLEAN, Sqlx.NUMERIC, Sqlx.DECIMAL,
                    Sqlx.DEC, Sqlx.FLOAT, Sqlx.REAL, // Sqlx.LONG,Sqlx.DOUBLE,
                    Sqlx.INT, // Sqlx.BIGINT,
                    Sqlx.INTEGER,// Sqlx.SMALLINT,
                    Sqlx.BINARY, Sqlx.BLOB, Sqlx.NCLOB,
                    Sqlx.CLOB, Sqlx.DATE, Sqlx.TIME, Sqlx.ROW, Sqlx.TABLE))
                {
                    type = ParseSqlDataType().domain+(Domain.Default,tc.domain.defaultValue)
                        +(Domain.DefaultString,tc.domain.defaultString);
                    var xx = Domain.Create(db,type);
                    db = (Transaction)xx.Item1;
                    type = xx.Item2;
                    dm = type.defpos;
                }
                if (db.parse == ExecuteStatus.Obey)
                {
                    if (!tc.domain.CanTakeValueOf(type))
                        throw new DBException("2200G");
                    db=tr+new Alter3(col.defpos, nm, ti.map[col.name].Value, tc.tabledefpos, dm, 
                        type.defaultString,type.defaultValue,tc.updateString, tc.update,
                        tc.notNull, tc.generated, tr);
                }
                return;
            }
            if (StartMetadata(Sqlx.COLUMN))
            {
                var lp = lxr.Position;
                if (tb.Denied(tr, Grant.Privilege.AdminRole))
                    throw new DBException("42105", ti.name ?? "").Mix();
                var md = ParseMetadata(db, col, -1, Sqlx.COLUMN);
                db=tr+new PMetadata(nm, md, -1, col.defpos, tr);
            }
		}
        /// <summary>
		/// AlterType = TO id
        ///     |   ADD ( Member | Method )
		/// 	|	SET Member_id To id
		/// 	|	DROP ( Member_id | (MethodType Method_id '('Type{','Type}')')) DropAction
		/// 	|	ALTER Member_id AlterMember { ',' AlterMember } .
        /// </summary>
        /// <returns>the executable</returns>
        Executable ParseAlterType()
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            var at = new Executable(lxr.Position, Executable.Type.AlterType);
            Next();
            var id = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var tp = db.GetObject(id.ident) as UDType??
                throw new DBException("42133", id.ident).Mix()
                    .Add(Sqlx.TYPE, new TChar(id.ident));
            var ti = db.role.obinfos[tp.defpos] as ObInfo;
            if (tok == Sqlx.TO)
            {
                Next();
                id = new Ident(lxr);
                Mustbe(Sqlx.ID);
                if (db.parse == ExecuteStatus.Obey)
                    tr+=new Change(tp.defpos, id.ident,tr);
            }
            else if (tok == Sqlx.SET)
            {
                id = new Ident(lxr);
                Mustbe(Sqlx.ID);
                var sq = ti.map[id.ident] ?? -1;
                var ts = ti.columns[sq] as SqlValue;
                var tc = db.objects[ts?.defpos ?? -1L] as TableColumn;
                if (tc == null)
                    throw new DBException("42133", id).Mix()
                        .Add(Sqlx.TYPE, new TChar(id.ident));
                Mustbe(Sqlx.TO);
                id = new Ident(lxr);
                Mustbe(Sqlx.ID);
                if (db.parse == ExecuteStatus.Obey)
                    tr+= new Alter3(tc.defpos, id.ident, sq, tc.tabledefpos, 
                        tc.domain.defpos, tc.domain.defaultString, 
                        tc.domain.defaultValue, tc.updateString,
                       tc.update, tc.notNull, tc.generated, tr);
            }
            else if (tok == Sqlx.DROP)
            {
                Next();
                id = new Ident(lxr);
                if (MethodModes())
                {
                    MethodName mn = ParseMethod(null);
                    Method mt = tp.methods[mn.mname.ident]?[mn.arity];
                    if (mt == null)
                        throw new DBException("42133", tp).Mix().
                            Add(Sqlx.TYPE, new TChar(ti.name));
                    if (db.parse == ExecuteStatus.Obey)
                    {
                        ParseDropAction();
                        tr += new Drop(mt.defpos, tr);
                    }
                }
                else
                {
                    var tc = ti.columns[ti.map[id.ident].Value] as SqlValue;
                    if (tc == null)
                        throw new DBException("42133", id).Mix()
                            .Add(Sqlx.TYPE, new TChar(id.ident));
                    if (db.parse == ExecuteStatus.Obey)
                    {
                        ParseDropAction();
                        tr += new Drop(tc.defpos, tr);
                    }
                }
            }
            else if (Match(Sqlx.ADD))
            {
                Next();
                var lp = lxr.Position;
                MethodModes();
                if (Match(Sqlx.INSTANCE, Sqlx.STATIC, Sqlx.CONSTRUCTOR, Sqlx.OVERRIDING, Sqlx.METHOD))
                {
                    ParseMethodHeader(id);
                    return cx.exec;
                }
                var nc = (int)ti.columns.Count;
                var c = ParseMember(nc);
                tp += (ObInfo.Columns, ti.columns + (nc, c));
                var (d,dm) = Domain.Create(db,tp);
                db = (Transaction)d;
                tp = (UDType)dm;
                if (tp.tabledefpos>0 && db.parse == ExecuteStatus.Obey)
                    tr+=new PColumn2(tp.tabledefpos, c.name, nc,
                        dm.defpos, c.domain.defaultString, c.domain.defaultValue, 
                        false, GenerationRule.None, tr);
            }
            else if (tok == Sqlx.ALTER)
            {
                Next();
                id = new Ident(lxr);
                Mustbe(Sqlx.ID);
                var tc = ti.ColFor(id.ident) as SqlValue;
                if (tc == null)
                    throw new DBException("42133", id).Mix()
                        .Add(Sqlx.TYPE, new TChar(id.ident));
                ParseAlterMembers(tc);
            }
            db = tr;
            return at;
        }
        /// <summary>
        /// AlterMember =	TYPE Type
        /// 	|	SET DEFAULT TypedValue
        /// 	|	DROP DEFAULT .
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tc">The UDType member</param>
        void ParseAlterMembers(SqlValue tc)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            TypedValue dv = tc.domain.defaultValue;
            var co = db.objects[tc.defpos] as TableColumn;
            var ti = db.role.obinfos[co.tabledefpos] as ObInfo;
            var dt = tc.domain;
            var ds = "";
            for (; ; )
            {
                var lp = lxr.Position;
                if (tok == Sqlx.TO)
                {
                    Next();
                    var n = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    if (db.parse == ExecuteStatus.Obey)
                        tr+=new Change(tc.defpos, n.ident, tr);
                    goto skip;
                }
                else if (Match(Sqlx.TYPE))
                {
                    Next();
                    var xx = Domain.Create(db, ParseSqlDataType().domain);
                    db = (Transaction)xx.Item1;
                    dt = xx.Item2;
                }
                else if (tok == Sqlx.SET)
                {
                    Next();
                    Mustbe(Sqlx.DEFAULT);
                    var st = lxr.start;
                    dv = lxr.val;
                    Next();
                    ds = new string(lxr.input, st, lxr.start - st);
                }
                else if (tok == Sqlx.DROP)
                {
                    Next();
                    Mustbe(Sqlx.DEFAULT);
                    dv = TNull.Value;
                }
                if (db.parse == ExecuteStatus.Obey)
                    tr+=new Alter3(tc.defpos, tc.name, ti.map[tc.name].Value, co.tabledefpos,
                         co.domain.defpos, ds, dv, co.updateString, co.update, 
                         co.notNull, GenerationRule.None, tr);
            skip:
                if (tok != Sqlx.COMMA)
                    break;
                Next();
            }
            db = tr;
        }
        /// <summary>
        /// FUNCTION id '(' Parameters ')' RETURNS Type Statement
        /// PROCEDURE id '(' Parameters ')' Statement
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseAlterProcedure()
        {
            var ar = new Executable(lxr.Position, Executable.Type.AlterRoutine);
            bool func = tok == Sqlx.FUNCTION;
            Next();
            ParseProcedureClause(func, Sqlx.ALTER);
            return ar;
        }
        /// <summary>
		/// DropStatement = 	DROP DropObject DropAction .
		/// DropObject = 	ORDERING FOR id
		/// 	|	ROLE id
		/// 	|	TRIGGER id
		/// 	|	ObjectName .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseDropStatement()
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            Next();
            if (Match(Sqlx.ORDERING))
            {
                var dr = new Executable(lxr.Position, Executable.Type.DropOrdering);
                Next(); Mustbe(Sqlx.FOR);
                var o = new Ident(lxr);
                Mustbe(Sqlx.ID);
                ParseDropAction(); // ignore if present
                var tp = db.GetObject(o.ident) as UDType ??
                    throw new DBException("42133", o.ToString()).Mix();
                if (db.parse == ExecuteStatus.Obey)
                    tr += new Ordering(tp.defpos, -1L, OrderCategory.None, tr);
                return dr;
            }
            else
            {
                var dt = new Executable(lxr.Position, Executable.Type.DropTable);
                var lp = lxr.Position;
                var (ob, n) = ParseObjectName();
                var a = ParseDropAction();
                var (d, ro) = ob.Cascade(tr, tr, db.role, a);
                d += (ro, d.loadpos);
                tr = (Transaction)d;
                if (db.parse == ExecuteStatus.Obey)
                    tr += new Drop1(ob.defpos, a, tr);
                db = tr;
                return dt;
            }
        }
        /// <summary>
        /// used in ObjectName. All that is needed here is the number of parameters
        /// because SQL is not strongly typed enough to distinguish actual parameter types
        /// '('Type, {',' Type }')'
        /// </summary>
        /// <returns>the number of parameters</returns>
        BList<Domain> ParseDataTypeList()
        {
            int n = 0;
            var anons = BList<Domain>.Empty;
            var lp = lxr.Position;
            if (tok == Sqlx.LPAREN)
            {
                Next();
                if (tok != Sqlx.RPAREN)
                {
                    Next();
                    anons += ParseSqlDataType().domain;
                    n++;
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        anons += ParseSqlDataType().domain;
                        n++;
                    }
                    Mustbe(Sqlx.RPAREN);
                }
            }
            return anons;
        }
        /// <summary>
		/// Type = 		StandardType | DefinedType | DomainName | REF(TableReference) .
		/// DefinedType = 	ROW  Representation
		/// 	|	TABLE Representation
        /// 	|   ( Type {, Type }) 
        ///     |   Type UNION Type { UNION Type }.
        /// </summary>
        /// <returns>The specified Domain or ObInfo</returns>
		DBObject ParseSqlDataType(Ident pn=null)
        {
            var lp = lxr.Position;
            DBObject r = null;
            Sqlx tp = tok;
            if (Match(Sqlx.ROW, Sqlx.TYPE, Sqlx.LPAREN))// anonymous row type
            {
                if (Match(Sqlx.ROW, Sqlx.TYPE))
                    Next();
                else
                    tp = Sqlx.TYPE;
                if (tok == Sqlx.LPAREN)
                {
                    var oi = ParseRowTypeSpec(Sqlx.TYPE, lxr.start,pn);
                    r = oi;
                    if (db is Transaction tr)
                        db = tr+ oi; // must be + ObInfo, so not += r
                }
            }
            if (Match(Sqlx.REF))
            {
                Next();
                var llp = lxr.Position;
                Mustbe(Sqlx.LPAREN);
                var q = new TableExpression(llp,BTree<long,object>.Empty);
                var t = ParseTableReference(null);
                q += (TableExpression.From, t);
                Mustbe(Sqlx.RPAREN);
                return new Domain(lp,Sqlx.REF, q.rowType.mem);
            }
            r = ParseStandardDataType();
            if (r == null)
            {
                var tr = db as Transaction ?? throw new DBException("2F003");
                var o = new Ident(lxr);
                if (tok == Sqlx.TABLE) // ad hoc table type
                {
                    Next();
                    var pt = new PTable("",tr);
                    tr += pt;
                    var pd = new PDomain(Physical.Type.PDomain, "", Sqlx.TABLE, 0, 
                        0, CharSet.UCS, "", "", pt.ppos, tr);
                    db = tr + pd;
                    var ta = (Table)db.objects[lp-1];
                    if (pn != null)
                        ta += (Basis.Name, pn);
                    ParseTableContentsSource(ta,pn);
                    r = (Domain)db.objects[lp];
                }
                else
                {
                    Mustbe(Sqlx.ID);
                    var oi = db.GetObInfo(o.ident)??
                        throw new DBException("42119", o.ident, "").Mix();
                    for (var b = oi.columns.First(); b != null; b = b.Next())
                    {
                        var sc = b.value();
                        cx.defs += (new Ident(pn, new Ident(sc.name, sc.defpos)), sc);
                    }
                    r = oi;
                }
            }
            if (tok==Sqlx.SENSITIVE)
            {
                r = new Domain(lp,tok, r);
                Next();
            }
            if (tok == Sqlx.UNION)
            {
                var ts = new List<DBObject>
                {
                    r
                };
                while (tok == Sqlx.UNION)
                {
                    Next();
                    ts.Add(ParseSqlDataType());
                }
                Domain[] ta = new Domain[ts.Count];
                for (int j = 0; j < ts.Count; j++)
                    ta[j] = ts[j] as Domain ??
                        throw new DBException("42133");
                r = Domain.UnionType(lp,ta);
            }
            if (r is Domain dm)
            {
                var xx = Domain.Create(db, dm);
                db = (Transaction)xx.Item1;
                return xx.Item2;
            }
            else
                return r;
        }
        /// <summary>
        /// StandardType = 	BooleanType | CharacterType | FloatType | IntegerType | LobType | NumericType | DateTimeType | IntervalType | XMLType .
        /// BooleanType = 	BOOLEAN .
        /// CharacterType = (([NATIONAL] CHARACTER) | CHAR | NCHAR | VARCHAR) [VARYING] ['('int ')'] [CHARACTER SET id ] Collate .
        /// Collate 	=	[ COLLATE id ] .
        /// There is no need to specify COLLATE UNICODE, since this is the default collation. COLLATE UCS_BASIC is supported but deprecated. For the list of available collations, see .NET documentation.
        /// FloatType =	(FLOAT|REAL) ['('int','int')'] .
        /// IntegerType = 	INT | INTEGER .
        /// LobType = 	BLOB | CLOB | NCLOB .
        /// CLOB is a synonym for CHAR in Pyrrho (both represent unbounded string). Similarly NCLOB is a synonym for NCHAR.
        /// NumericType = 	(NUMERIC|DECIMAL|DEC) ['('int','int')'] .
        /// DateTimeType =  (DATE | TIME | TIMESTAMP) ([IntervalField [ TO IntervalField ]] | ['(' int ')']).
        /// The use of IntervalFields when declaring DateTimeType  is an addition to the SQL standard.
        /// XMLType =	XML .
        /// </summary>
        /// <returns>the data type</returns>
        Domain ParseStandardDataType()
        {
            Domain r = null;
            Domain r0 = null;
            var lp = lxr.Position;
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
                    var o = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    r+=(Domain.Charset,(CharSet)Enum.Parse(typeof(CharSet), o.ident, false));
                }
                if (tok == Sqlx.COLLATE)
                    r+=(Domain.Culture, new CultureInfo(ParseCollate()));
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
                r = r0 = Domain.Numeric;
                Next();
                r = ParsePrecScale(r);
            }
            else if (Match(Sqlx.FLOAT, Sqlx.REAL ,Sqlx.DOUBLE))
            {
                r = r0 = Domain.Real;
                if (tok == Sqlx.DOUBLE)
                    Mustbe(Sqlx.PRECISION);
                Next();
                r = ParsePrecPart(r);
            }
            else if (Match(Sqlx.INT, Sqlx.INTEGER ,Sqlx.BIGINT,Sqlx.SMALLINT))
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
            else if (Match(Sqlx.BLOB,Sqlx.XML))
            {
                r = r0 = Domain.Blob;
                Next();
            }
            else if (Match(Sqlx.DATE, Sqlx.TIME, Sqlx.TIMESTAMP, Sqlx.INTERVAL))
            {
                Domain dr = r0 = Domain.Timestamp;
                switch(tok)
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
            else if (Match(Sqlx.OBJECT))
            {
                r = r0 = Domain.ObjectId;
                Next();
            }
            if (r == r0) 
                return r0; // completely standard
            return new Domain(lp,r); // set it to a positive defpos
        }
        /// <summary>
		/// IntervalType = 	INTERVAL IntervalField [ TO IntervalField ] .
		/// IntervalField = 	YEAR | MONTH | DAY | HOUR | MINUTE | SECOND ['(' int ')'] .
        /// </summary>
        /// <param name="q">The Domain being specified</param>
        /// <returns>the modified data type</returns>
        Domain ParseIntervalType()
		{
            var lp = lxr.Position;
			Sqlx start = Mustbe(Sqlx.YEAR,Sqlx.DAY,Sqlx.MONTH,Sqlx.HOUR,Sqlx.MINUTE,Sqlx.SECOND);
            var d = new Domain(lp,Domain.Interval)+(Domain.Start, start);
			if (tok==Sqlx.LPAREN)
			{
				Next();
				var p1 = lxr.val;
				Mustbe(Sqlx.INTEGERLITERAL);
				d+=(Domain.Scale,p1.ToInt().Value);
				if (start==Sqlx.SECOND && tok==Sqlx.COMMA)
				{
					Next();
					var p2 = lxr.val;
					Mustbe(Sqlx.INTEGERLITERAL);
					d+=(Domain.Precision,p2.ToInt().Value);
				}
				Mustbe(Sqlx.RPAREN);
			}
			if (tok==Sqlx.TO)
			{
				Next();
				Sqlx end = Mustbe(Sqlx.YEAR,Sqlx.DAY,Sqlx.MONTH,Sqlx.HOUR,Sqlx.MINUTE,Sqlx.SECOND);
                d += (Domain.End, end);
				if (end==Sqlx.SECOND && tok==Sqlx.LPAREN)
				{
					Next();
					var p2 = lxr.val;
					Mustbe(Sqlx.INTEGERLITERAL);
					d+=(Domain.Precision,p2.ToInt().Value);
					Mustbe(Sqlx.RPAREN);
				}
			}
            return d;
		}
        /// <summary>
        /// Handle ROW type or TABLE type in Type specification
        /// </summary>
        /// <param name="d">The type of type spec (ROW or TABLE)</param>
        /// <param name="tdp">ref: the typedefpos</param>
        /// <returns>The RowTypeSpec</returns>
        ObInfo ParseRowTypeSpec(Sqlx d, int st, Ident pn = null)
        {
            var tr = db as Transaction ?? throw new DBException("2F003");
            var ic = new Ident(lxr);
            if (tok == Sqlx.ID)
            {
                Next();
                var tb = db.GetObject(ic.ident) as Table ??
                    throw new DBException("42107", ic.ident).Mix();
                return db.role.obinfos[tb.defpos] as ObInfo;
            }
            var mems = BList<SqlValue>.Empty;
            var lps = BList<long>.Empty;
            Mustbe(Sqlx.LPAREN);
            for (var n = 0; ; n++)
            {
                lps += lxr.Position;
                mems += (n, ParseMember(n, pn));
                if (tok != Sqlx.COMMA)
                    break;
                Next();
            }
            Mustbe(Sqlx.RPAREN);
            var oi = new ObInfo(ic.iix + 1, Domain.Row, mems) + (Basis.Name, ic.ident);
            tr += (Database.Roles, db.role + oi);
            var pt = new PTable(ic.ident, tr);
            tr += pt;
            for (int j = 0; j < oi.columns.Count; j++)
            {
                var dt = oi.columns[j].domain;
                var pc = new PColumn3(pt.defpos, oi.columns[j].name, j, dt.defpos,
                    "", dt.defaultValue, "", BList<UpdateAssignment>.Empty,
                    false, GenerationRule.None, db);
                tr += pc;
                oi += new SqlValue(lps[j], oi.columns[j].name);
            }
            tr += oi;
            tr += new Domain(ic.iix, d, BTree<long, object>.Empty);
            db = tr;
            return oi;
        }
        /// <summary>
        /// Member = id Type [DEFAULT TypedValue] Collate .
        /// </summary>
        /// <returns>The RowTypeColumn</returns>
		SqlValue ParseMember(int seq,Ident pn=null)
		{
            Ident n = null;
            if (tok == Sqlx.ID)
            {
                n = new Ident(lxr);
                Next();
            }
            DBObject dt = ParseSqlDataType();
			if (tok==Sqlx.DEFAULT)
			{
				int st = lxr.start;
                long lp = lxr.Position;
				ParseSqlValue(dt);
				dt += (Domain.Default,dt.Parse(new Scanner(lp,lxr.input,st)));
			}
            if (tok == Sqlx.COLLATE)
                dt+= (Domain.Culture,ParseCollate());
            var r = new SqlValue(n.iix,n?.ident);
            if (pn != null)
                cx.defs += (new Ident(pn, n), r);
            ObInfo oi = new ObInfo(n.iix, n.ident);
            if (StartMetadata(Sqlx.COLUMN))
                r +=(SqlValue.Info,ParseMetadata(db,null,-1, Sqlx.COLUMN));
            return (SqlValue)cx.Add(r);
		}
        /// <summary>
        /// Parse a precision
        /// </summary>
        /// <param name="r">The SqldataType</param>
        /// <returns>the updated data type</returns>
		Domain ParsePrecPart(Domain r)
		{
			if (tok==Sqlx.LPAREN)
			{
				Next();
                if (lxr.val is TInt)
                {
                    int prec = lxr.val.ToInt().Value;
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
        /// <returns>the updated data type</returns>
		Domain ParsePrecScale(Domain r)
		{
			if (tok==Sqlx.LPAREN)
			{
				Next();
                if (lxr.val is TInt)
                {
                    int prec = lxr.val.ToInt().Value;
                    r += (Domain.Precision, prec);
                }
				Mustbe(Sqlx.INTEGERLITERAL);
				if (tok==Sqlx.COMMA)
				{
					Next();
                    if (lxr.val is TInt)
                    {
                        int scale = lxr.val.ToInt().Value;
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
		Executable ParseSqlSet()
        {
            Next();
            if (Match(Sqlx.ROLE))
            {
                var sr = new Executable(lxr.Position, Executable.Type.SetRole);
                Next();
                ParseSetRole();
                return (Executable)cx.Add(sr);
            }
            if (Match(Sqlx.AUTHORIZATION))
            {
                var ss = new Executable(lxr.Position, Executable.Type.SetSessionAuthorization);
                Next();
                Mustbe(Sqlx.EQL);
                Mustbe(Sqlx.CURATED);
                var tr = db as Transaction ?? throw new DBException("2F003");
                if (db.user == null || db.user.defpos != db.owner)
                    throw new DBException("42105").Mix();
                if (db.parse == ExecuteStatus.Obey)
                    db = tr+ new Curated(tr);
                return (Executable)cx.Add(ss);
            }
            if (Match(Sqlx.PROFILING))
            {
                var sc = new Executable(lxr.Position, Executable.Type.SetSessionCharacteristics);
                Next();
                Mustbe(Sqlx.EQL);
                if (db.user == null || db.user.defpos != db.owner)
                    throw new DBException("42105").Mix();
                var o = lxr.val;
                Mustbe(Sqlx.BOOLEANLITERAL);
                // ignore for now
                return (Executable)cx.Add(sc);
            }
            if (Match(Sqlx.TIMEOUT))
            {
                var sc = new Executable(lxr.Position, Executable.Type.SetSessionCharacteristics);
                Next();
                Mustbe(Sqlx.EQL);
                if (db.user == null || db.user.defpos != db.owner)
                    throw new DBException("42105").Mix();
                var o2 = lxr.val;
                Mustbe(Sqlx.INTEGERLITERAL);
                // ignore for now
                return (Executable)cx.Add(sc);
            }
            // Rename
            Ident n = null;
            Match(Sqlx.DOMAIN, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE);
            MethodModes();
            Sqlx kind = tok;
            DBObject ob = null;
            Executable at = null;
            bool meth = false;
            if (Match(Sqlx.TABLE, Sqlx.DOMAIN, Sqlx.ROLE, Sqlx.VIEW, Sqlx.TYPE))
            {
                Next();
                n = new Ident(lxr);
                Mustbe(Sqlx.ID);
                ob = db.GetObject(n.ident) ??
                    throw new DBException("42135", n.ident);
                switch (kind)
                {
                    case Sqlx.TABLE:
                        at = new Executable(lxr.Position, Executable.Type.AlterTable);
                        break;
                    case Sqlx.DOMAIN:
                        at = new Executable(lxr.Position, Executable.Type.AlterDomain);
                        break;
                    case Sqlx.ROLE:
                        at = new Executable(lxr.Position, Executable.Type.CreateRole);
                        break;
                    case Sqlx.VIEW:
                        at = new Executable(lxr.Position, Executable.Type.AlterTable);
                        break;
                    case Sqlx.TYPE:
                        at = new Executable(lxr.Position, Executable.Type.AlterType);
                        break;
                }
            }
            else
            {
                at = new Executable(lxr.Position, Executable.Type.AlterRoutine);
                meth = false;
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
                n = new Ident(lxr);
                var nid = n.ident;
                Mustbe(Sqlx.ID);
                int arity = 0;
                if (tok == Sqlx.LPAREN)
                {
                    Next();
                    ParseSqlDataType();
                    arity++;
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        ParseSqlDataType();
                        arity++;
                    }
                    Mustbe(Sqlx.RPAREN);
                }
                if (meth)
                {
                    Ident type = null;
                    if (mt == PMethod.MethodType.Constructor)
                        type = new Ident(nid, 0);
                    if (tok == Sqlx.FOR)
                    {
                        Next();
                        type = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                    }
                    if (type == null)
                        throw new DBException("42134").Mix();
                    ob = (db.GetObject(type.ident) as UDType)?.methods[n.ident]?[arity];
                }
                else
                    ob = db.GetProcedure(n.ident,arity);
                if (ob == null)
                    throw new DBException("42135", "Object not found").Mix();
                Mustbe(Sqlx.TO);
                var nm = new Ident(lxr);
                Mustbe(Sqlx.ID);
                if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                    db = tr+ new Change(ob.defpos, nm.ident, tr);
            }
            return (Executable)cx.Add(at);
        }
        /// <summary>
        /// 	|	SET ROLE id | NONE
        /// 	This needs to change properties of the connection, so that it applies to the session
        /// </summary>
		void ParseSetRole()
        {
            var n = new Ident(lxr);
            var dn = new Ident("", 0);
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.FOR)
            {
                Next();
                Mustbe(Sqlx.DATABASE);
                dn = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            var a = db.GetObject(n.ident) as Role;
            var dm = (a != null) ? (ObInfo)db.role.obinfos[a.defpos] : null;
            if (dm == null || (dm.priv&Grant.Privilege.UseRole)==0)
                throw new DBException("42135", n.ident);
            db += (Transaction.Role, a);
            if (cx.next == null)
                cx = new Context(db);
            else
                cx = new Context(cx, a, cx.user);
        }
        /// <summary>
        /// Start the parse for a Query
        /// </summary>
        /// <param name="sql">The sql to parse</param>
        /// <returns>A CursorSpecification</returns>
		public SelectStatement ParseCursorSpecification(string sql,ObInfo rt=null)
		{
            rt = rt ?? ObInfo.Any;
			lxr = new Lexer(sql, db.lexeroffset);
			tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid+sql.Length);
			return ParseCursorSpecification(rt);
		}
        /// <summary>
		/// CursorSpecification = [ XMLOption ] QueryExpression  .
        /// </summary>
        /// <returns>A CursorSpecification</returns>
		internal SelectStatement ParseCursorSpecification(ObInfo rt,bool defer = false)
        {
            rt = rt ?? ObInfo.Any;
            var lp = lxr.Position;
            int st = lxr.start;
            CursorSpecification r = new CursorSpecification(lxr.Position,BTree<long,object>.Empty);
            ParseXmlOption(false);
            var qe = ParseQueryExpression(rt);
            r += (CursorSpecification.Union,qe);
            r += (Query.Display, qe.display);
            r += (Query.OrdSpec, qe.ordSpec);
            r += (CursorSpecification._Source,new string(lxr.input, st, lxr.start - st));
            r = (CursorSpecification)cx.Add(r);
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
            {
                var da = r.RowSets(tr, cx);
                if (da != null)
                    cx.data += (r.defpos, da);
                cx.rb = da?.First(cx);
                cx.data += (lp, da);
            }
            return (SelectStatement)cx.Add(new SelectStatement(lp, r));
        }
        /// <summary>
        /// Start the parse for a QueryExpression (called from View)
        /// </summary>
        /// <param name="sql">The sql string</param>
        /// <returns>A QueryExpression</returns>
		public QueryExpression ParseQueryExpression(string sql,ObInfo rt=null)
		{
            rt = rt ?? ObInfo.Any;
			lxr = new Lexer(sql);
			tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid + sql.Length);
			return ParseQueryExpression(rt);
		}
        /// <summary>
        /// QueryExpression = QueryExpressionBody [OrderByClause] [FetchFirstClause] .
		/// QueryExpressionBody = QueryTerm 
		/// | QueryExpressionBody ( UNION | EXCEPT ) [ ALL | DISTINCT ] QueryTerm .
		/// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>A QueryExpression</returns>
		QueryExpression ParseQueryExpression(ObInfo rt)
        {
            QueryExpression left = ParseQueryTerm(rt);
            while (Match(Sqlx.UNION, Sqlx.EXCEPT))
            {
                left += (Query.SimpleQuery, null);
                Sqlx op = tok;
                var lp = lxr.Position;
                Next();
                Sqlx m = Sqlx.DISTINCT;
                if (Match(Sqlx.ALL, Sqlx.DISTINCT))
                {
                    m = tok;
                    Next();
                }
                left = new QueryExpression(lp + 1, cx, left, op, ParseQueryTerm(rt));
                if (m == Sqlx.DISTINCT)
                    left += (QueryExpression._Distinct, true);
            }
            var ois = left.ordSpec?.items ?? BList<SqlValue>.Empty;
            var nis = ParseOrderClause(ois, left.defpos,left.simpletablequery);
            if (ois != nis)
                left += (Query.OrdSpec, new OrderSpec(nis));
            if (Match(Sqlx.FETCH))
            {
                Next();
                Mustbe(Sqlx.FIRST);
                var o = lxr.val;
                if (tok == Sqlx.INTEGERLITERAL)
                {
                    left += (Query.FetchFirst, o.ToInt().Value);
                    Next();
                }
                else
                    left += (Query.FetchFirst, 1);
                Mustbe(Sqlx.ROW, Sqlx.ROWS);
                Mustbe(Sqlx.ONLY);
            }
            return (QueryExpression)cx.Add(left);
        }
        /// <summary>
		/// QueryTerm = QueryPrimary | QueryTerm INTERSECT [ ALL | DISTINCT ] QueryPrimary .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>the QueryExpression</returns>
		QueryExpression ParseQueryTerm(ObInfo rt)
		{
			QueryExpression left = ParseQueryPrimary(rt);
			while (Match(Sqlx.INTERSECT))
			{
                left+=(Query.SimpleQuery,false);
                var lp = lxr.Position;
				Next();
				Sqlx m = Sqlx.DISTINCT;
				if (Match(Sqlx.ALL,Sqlx.DISTINCT))
				{
					m = tok;
					Next();
				}
				left = new QueryExpression(lp+1, cx, left,Sqlx.INTERSECT,ParseQueryPrimary(rt));
                if (m == Sqlx.DISTINCT)
                    left += (QueryExpression._Distinct, true);
			}
			return (QueryExpression)cx.Add(left);
		}
        /// <summary>
		/// QueryPrimary = QuerySpecification |  TypedValue | TABLE id .
        /// A simple table query is defined (SQL2003-02 14.1SR18c) as a CursorSpecification in which the QueryExpression is a QueryTerm that is a QueryPrimary that is a QuerySpecification.
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>the QueryExpression</returns>
		QueryExpression ParseQueryPrimary(ObInfo rt)
		{
            var lp = lxr.Position;
            var qe = new QueryExpression(lp+1, true);
            switch (tok)
            {
                case Sqlx.LPAREN:
                    Next();
                    qe = ParseQueryExpression(rt);
                    Mustbe(Sqlx.RPAREN);
                    break;
                case Sqlx.SELECT: // query specification
                    {
                        var qs = ParseQuerySpecification(rt);
                        qe = qe + (QueryExpression._Left, qs)
                            +(Query.RowType,(ObInfo)qs.rowType.Relocate(qe.defpos));
                        break;
                    }
                case Sqlx.VALUES:
                    var v = BList<SqlValue>.Empty;
                    Sqlx sep = Sqlx.COMMA;
                    while (sep == Sqlx.COMMA)
                    {
                        Next();
                        v += ParseSqlValue(rt);
                        sep = tok;
                    }
                    qe+=(QueryExpression._Left,new SqlRow(lp,v));
                    break;
                case Sqlx.TABLE:
                    Next();
                    lp = lxr.Position;
                    Ident ic = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    var tb = db.GetObject(ic.ident) as Table ??
                        throw new DBException("42107", ic.ident);
                    var fm = _From(lp, tb, null, QuerySpecification.Default);
                    var ft = fm.rowType.For(db, tb, Grant.Privilege.Select);
                    qe = qe+(QueryExpression._Left,fm+(Query.RowType,ft))
                        + (Query.RowType,(ObInfo)ft.Relocate(qe.defpos));
                    break;
                default:
                    throw new DBException("42127").Mix();
            }
            qe += (Query.RowType, (ObInfo)qe.left.rowType.Relocate(qe.defpos));
            qe += (Query.Display, qe.left.display);
            qe += (DBObject.Depth, 1 + qe.left.depth);
            return (QueryExpression)cx.Add(qe);
		}
        /// <summary>
		/// OrderByClause = ORDER BY OrderSpec { ',' OrderSpec } .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the list of OrderItems</returns>
		BList<SqlValue> ParseOrderClause(BList<SqlValue> ord,long curq,bool wfok)
		{
			if (tok!=Sqlx.ORDER)
				return ord;
			Next();
			Mustbe(Sqlx.BY);
			ord+=ParseOrderItem(curq,wfok);
			while (tok==Sqlx.COMMA)
			{
				Next();
				ord+=ParseOrderItem(curq,wfok);
			}
            return ord;
		}
        /// <summary>
		/// OrderSpec =  TypedValue [ ASC | DESC ] [ NULLS ( FIRST | LAST )] .
        /// </summary>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>an OrderItem</returns>
		SqlValue ParseOrderItem(long curq,bool wfok)
		{
            var v = ParseSqlValue(Domain.Content,curq,wfok);
            var dt = v.domain;
            var a = Sqlx.ASC;
            var n = Sqlx.FIRST;
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
			return v+(DBObject._Domain,dt+(a,n));
		}
        /// <summary>
		/// QuerySpecification = SELECT [ALL|DISTINCT] SelectList [INTO Targets] TableExpression .
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>The QuerySpecification</returns>
		QuerySpecification ParseQuerySpecification(ObInfo rt)
        {
            Mustbe(Sqlx.SELECT);
            var lp = lxr.Position;
            QuerySpecification r = new QuerySpecification(lp-1,BTree<long, object>.Empty
                + (QuerySpecification.Distinct, ParseDistinctClause()));
            r = ParseSelectList(r,ObInfo.Any); // not rt
            var te = ParseTableExpression(r);
            r = (QuerySpecification)r.Refresh(cx);
            r = r + (QuerySpecification.TableExp,te) 
                + (Query.Display,(int)r.rowType.columns.Count)
                + (DBObject._Domain,Domain.TableType)
                + (DBObject.Depth, Math.Max(Depth(r),1+te.depth));
            r = (QuerySpecification)cx.Add(r);
            if (Match(Sqlx.FOR))
            {
                Next();
                Mustbe(Sqlx.UPDATE);
            }
            if (rt!=ObInfo.Any && !rt.CanBeAssigned(r.rowType))
                throw new DBException("42000", lxr.pos);
            return (QuerySpecification)r.Refresh(cx); // no chances
        }
        int Depth(QuerySpecification q)
        {
            var d = q.tableExp.depth;
            for (var b = q.rowType.columns.First(); b != null; b = b.Next())
                if (b.value() is SqlValue v)
                d = Math.Max(d, v.depth);
            return d + 1;
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
        /// <param name="t">the expected data type</param>
		QuerySpecification ParseSelectList(QuerySpecification q,ObInfo rt)
        {
            int j = 0;
            q = ParseSelectItem(q, rt, j++);
            while (tok == Sqlx.COMMA)
            {
                Next();
                q = ParseSelectItem(q, rt, j++);
            }
            return (QuerySpecification)cx.Add(q);
        }
        /// <summary>
		/// SelectItem = * | (Scalar [AS id ]) | (RowValue [.*] [AS IdList]) .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <param name="t">the expected data type for the query</param>
        /// <param name="pos">The position in the SelectList</param>
        QuerySpecification ParseSelectItem(QuerySpecification q,ObInfo rt,int pos)
        {
            Ident alias = null;
            if (tok==Sqlx.TIMES)
            {
                Next();
                var sa = new SqlStar(lxr.Position, null);
                cx.Add(sa);
                return (QuerySpecification)cx.Add(q + sa);
            }
            var dt = (rt == ObInfo.Any) ? Domain.Content : rt.columns[pos].domain;
            var v = ParseSqlValue(dt,q.defpos,true);
            cx.Add(v);
            if (v.aggregates())
                q += (Query._Aggregates, true);
            if (tok == Sqlx.AS)
            {
                Next();
                if (v is SqlStar sa && tok == Sqlx.LPAREN)
                {
                    var r = new Correlation();
                    Next();
                    var ids = ParseIDList();
                    for (var i = 0; i < ids.Length; i++)
                        r.cols += ids[i].ident;
                    v += (SqlStar.Prefix, sa.prefix);
                }
                else
                {
                    alias = new Ident(lxr);
                    v += (DBObject._Alias, alias.ident);
                    cx.defs += (alias, v);
                    cx.Add(v);
                    Mustbe(Sqlx.ID);
                }
            }
            if (v.domain.kind == Sqlx.TABLE)
                throw new DBException("42171");
            q += v;
            q += (Query.Display, q.rowType.Length);
            if (alias != null)
            {
                var qt = q.rowType;
                q += (Query.RowType, qt + (ObInfo.Map, qt.map + (alias.ident, pos)));
            }
            return (QuerySpecification)cx.Add(q);
        }
        /// <summary>
		/// TableExpression = FromClause [ WhereClause ] [ GroupByClause ] [ HavingClause ] [WindowClause] .
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="t">the expected data type</param>
        /// <returns>The TableExpression</returns>
		TableExpression ParseTableExpression(QuerySpecification q)
		{
            var lp = lxr.Position;
            var fm = ParseFromClause(q); // query rewriting occurs during these steps
            var wh = ParseWhereClause(fm);
            fm = fm.Refresh(cx);
            var gp = ParseGroupClause(fm);
            var ha = ParseHavingClause();
            var wi = ParseWindowClause();
            fm = fm.Refresh(cx);
            // we build the tablexpression after the context has settled down
            var r = new TableExpression(lp, BTree<long, object>.Empty
                + (Query.RowType, (ObInfo)fm.rowType.Relocate(lp))
                + (DBObject._Domain, fm.domain)
                + (TableExpression.From, fm)
                + (TableExpression.Group, gp)
                + (TableExpression.Having, ha)
                + (TableExpression.Windows, wi));
            r = (TableExpression)cx.Add(r);
            r = (TableExpression)r.AddCondition(cx,Query.Where, wh);
            r = (TableExpression)r.AddCondition(cx,TableExpression.Having, ha);
            var ds = (r.from is QuerySpecification sq) ? sq.display : r.from.rowType.Length;
            r+=(Query.Display,ds);
            r = (TableExpression)cx.Add(r);
            q = (QuerySpecification)q.Refresh(cx); // may have changed!
            q += (QuerySpecification.TableExp, r);
            cx.Add(q);
            return (TableExpression)r.Conditions(cx);
        }
        /// <summary>
		/// FromClause = 	FROM TableReference { ',' TableReference } .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <returns>The table expression</returns>
		Query ParseFromClause(QuerySpecification q)
		{
            if (tok == Sqlx.FROM)
            {
                Next();
                var st = lxr.start - 1; // yuk
                var rt = ParseTableReference(q);
                q = (QuerySpecification)q.Refresh(cx);
                while (tok == Sqlx.COMMA)
                {
                    var lp = lxr.Position;
                    Next();
                    var te = ParseTableReference(q);
                    q = (QuerySpecification)q.Refresh(cx);
                    var jp = new JoinPart(lp, BTree<long, object>.Empty
                    + (JoinPart.JoinKind, Sqlx.CROSS) + (JoinPart.LeftOperand, rt)
                    + (JoinPart.RightOperand, te));
                    rt = jp.Selects(cx,q);
                }
                return (Query)cx.Add(rt);
            }
            else
                return (Query)cx.Add(From._static);
		}
        /// <summary>
		/// TableReference = TableFactor Alias | JoinedTable .
        /// </summary>
        /// <param name="q">the query being parsed</param>
        /// <returns>The table expression</returns>
        Query ParseTableReference(QuerySpecification q)
        {
            var st = lxr.start;
            var a = ParseTableReferenceItem(q);
            var lp = lxr.Position;
            while (Match(Sqlx.CROSS, Sqlx.NATURAL, Sqlx.JOIN, Sqlx.INNER, Sqlx.LEFT, Sqlx.RIGHT, Sqlx.FULL))
            {
                q = (QuerySpecification)q.Refresh(cx);
                a = ParseJoinPart(lp, a, q);
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
		/// | 	TABLE '('  TypedValue ')' 
		/// | 	UNNEST '('  TypedValue ')' 
        /// |   STATIC
        /// |   '[' docs ']' .
        /// Subquery = '(' QueryExpression ')' .
        /// </summary>
        /// <param name="qs">the table expression</param>
        /// <returns>the table expression</returns>
		Query ParseTableReferenceItem(QuerySpecification q)
		{
            Query rf;
            if (tok == Sqlx.ROWS) // Pyrrho specific
            {
                var tr = db as Transaction ?? throw new DBException("2F003");
                Next();
                Mustbe(Sqlx.LPAREN);
                var v = lxr.val;
                Mustbe(Sqlx.INTEGERLITERAL);
                TypedValue w = new TInt(0);
                if (tok == Sqlx.COMMA)
                {
                    Next();
                    w = lxr.val;
                    Mustbe(Sqlx.INTEGERLITERAL);
                }
                Mustbe(Sqlx.RPAREN);
                Ident a = null;
                if (tok == Sqlx.ID || tok == Sqlx.AS)
                {
                    if (tok == Sqlx.AS)
                        Next();
                    a = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                }
                if (w.ToInt() != 0)
                    rf = (Query)cx.Add(new LogRowColTable(tr, cx, (long)v.Val(), (long)w.Val(), 
                        a?.ident??""));
                else
                    rf = (Query)cx.Add(new LogRowTable(tr, cx, (long)v.Val(), a?.ident ?? ""));
            }
            else if (tok == Sqlx.UNNEST)
            {
                Next();
                var lp = lxr.Position;
                Mustbe(Sqlx.LPAREN);
                SqlValue sv = null;
                if (tok == Sqlx.LBRACK) // allow [ ] for doc array
                {
                    var da = new SqlRowArray(lxr.Position, BTree<long, object>.Empty);
                    Next();
                    if (tok != Sqlx.RBRACK)
                        while (true)
                        {
                            da += ParseSqlRow(Domain.Document);
                            if (tok != Sqlx.COMMA)
                                break;
                            Next();
                        }
                    Mustbe(Sqlx.RBRACK);
                    sv = da;
                }
                else
                    sv = ParseSqlValue(Domain.TableType); //  unnest forces table value
                rf = new Query(lp,new BTree<long, object>(From.Target, sv.defpos));
                Mustbe(Sqlx.RPAREN);
            }
            else if (tok == Sqlx.TABLE)
            {
                Next();
                Mustbe(Sqlx.LPAREN); // SQL2003-2 7.6 required before table valued function
                var st = lxr.start;
                Ident n = new Ident(lxr);
                Mustbe(Sqlx.ID);
                var r = BList<SqlValue>.Empty;
                Mustbe(Sqlx.LPAREN);
                if (tok != Sqlx.RPAREN)
                    for (; ; )
                    {
                        r += ParseSqlValue(Domain.Content);
                        if (tok == Sqlx.RPAREN)
                            break;
                        Mustbe(Sqlx.COMMA);
                    }
                Next();
                Mustbe(Sqlx.RPAREN); // another: see above
                var cr = ParseCorrelation();
                var proc = db.GetProcedure(n.ident, (int)r.Count)
                    ?? throw new DBException("42108", n.ident + "$" + r.Count).Mix();
                if (db.parse==ExecuteStatus.Obey && db is Transaction tr)
                    proc.Exec(tr, cx, r);
                var ri = (ObInfo)db.role.obinfos[proc.retType.defpos];
                rf = (Query)cx.Add(new Query(n.iix, BTree<long, object>.Empty
                    + (Basis.Name, cr.tablealias))
                    +(Query.RowType,cr.Apply(ri).Relocate(n.iix)));
            }
            else if (tok == Sqlx.LPAREN) // subquery
            {
                var lp = lxr.Position;
                Next();
                QueryExpression qe = ParseQueryExpression(q.rowType);
                Mustbe(Sqlx.RPAREN);
                var r = ParseCorrelation();
                var rs = qe.RowSets(db, cx);
                rf = new Query(lp, BTree<long, object>.Empty + (Basis.Name, r.tablealias))
                    +(Query.RowType,(ObInfo)r.Pick(rs.qry.rowType).Relocate(lp));
            }
            else if (tok == Sqlx.STATIC)
            {
                var lp = lxr.Position;
                Next();
                rf = From._static;
            }
            else if (tok == Sqlx.LBRACK)
            {
                var lp = lxr.Position;
                rf = new Query(lp,BTree<long, object>.Empty
                    + (From.Target, ParseSqlDocArray().defpos));
            }
            else // ordinary table
            {
                Ident ic = new Ident(lxr);
                var lp = lxr.Position;
                Mustbe(Sqlx.ID);
                string a = null;
                if (tok == Sqlx.ID)
                {
                    a = lxr.val.ToString();
                    Next();
                }
                Table tb = null;
                long tabledefpos;
                var st = cx.Get(ic) as From; 
                if (st != null)
                    tabledefpos = st.target;
                else
                {
                    tb = db.GetObject(ic.ident) as Table
                        ?? cx.Get(ic) as Table
                        ?? throw new DBException("42107", ic);
                    tabledefpos = tb.defpos;
                }              
                rf = _From(lp, tb, a, q, st); // at least one of tb and st?.from is nonnull
                if (Match(Sqlx.FOR))
                {
                    var ps = ParsePeriodSpec(rf);
                    rf += (Query.Periods, rf.periods + (tb.defpos, ps));
                    long pp = (ps.periodname == "SYSTEM_TIME") ? tb.systemPS : tb.applicationPS;
                    if (pp<0)
                        throw new DBException("42162", ps.periodname).Mix();
                    rf += (Query.Periods, rf.periods + (tabledefpos, ps));
                }
                cx.defs += (ic, rf);
                if (cx.dbformat < 51)
                    cx.defs += (new Ident(rf.defpos.ToString(), rf.defpos), rf);
                if (a != null)
                    cx.defs += (new Ident(a, rf.defpos), rf);
            }
            rf = (Query)cx.Add(rf);
            var dt = rf.rowType;
            for (var i = 0; i < dt.Length; i++)
            {
                var co = dt.columns[i];
                cx.defs += (new Ident(dt.columns[i].name, co.defpos), co);
                if (cx.dbformat < 51)
                    cx.defs += (new Ident(co.defpos.ToString(),co.defpos), co);
                cx.obs += (co.defpos, co);
            }
            return rf.Refresh(cx); // but check anyway
        }
        /// <summary>
        /// This is tricky. We want to ensure that the From rowtype is different
        /// for each occurrence of a table in the query.
        /// If there are no stars in the select list, then we will find which columns
        /// are needed in the select list, and these will have unique uids.
        /// If there is a star in the select list that might be for this table,
        /// we append a list of all columns to the query and construct a rowType from
        /// that.
        /// We access tr.role to find a Table's rowType when we encounter a reference to the table. 
        /// Then in cx.role we have the rowTypes for the different instances during parsing.
        /// Both tr.role and cx.role allow looking up a rowType by its name: cx.role also by alias.
        /// </summary>
        /// <param name="dp">The occurrence of this table</param>
        /// <param name="tb">The table</param>
        /// <param name="a">The alias or null</param>
        /// <param name="q">The query with the select list</param>
        /// <returns></returns>
        From _From(long dp,Table tb,string a,QuerySpecification q,From fm=null)
        {
            var qs = BList<SqlValue>.Empty;
            var ch = false;
            var i = 0;
            ObInfo ti = fm?.rowType;
            if (fm == null)
            {
                ti = (ObInfo)((ObInfo)db.role.obinfos[tb.defpos]).Relocate(dp);
                if (a != null)
                    cx.defs += (new Ident(a, 0), ti);
                i = 0;
                fm = new From(dp, tb, ti);
            }
            if (a != "" && a != null)
                fm += (DBObject._Alias, a);
            for (var b = q.rowType.columns?.First();ti!=null && b!=null;b=b.Next())
            {
                var c = b.value();
                if (c is SqlStar sa)
                {
                    if (sa.prefix?.Uses(tb.defpos)==true)
                    {
                        ch = true;
                        DoStar(tb, ref ti, ref qs, ref i);
                    }
                    else if (sa.prefix == null)
                        q += (SqlStar.Prefix, null);
                }
                else 
                {
                    var nc = c?.Resolve(cx, fm, ref ti);
                    var s = nc.ToString();
                    if (c != nc)
                    {
                        ch = true;
                        cx.Replace(c, nc);
                    }
                    qs += (i++, nc);
                }
            }

            if (q.mem.Contains(SqlStar.Prefix))
            {
                ch = true;
                DoStar(tb, ref ti, ref qs, ref i);
            }
  //          if (ti != null)
  //              for (var j = ti.Length - 1; j > qs.Count - 1; j--)
  //                  ti += (ObInfo.Columns,ti.columns-j);
            if (ch)
                cx.Add(q + (Query.Display, (int)qs.Count)
                    +(Query.RowType,new ObInfo(q.defpos, Domain.TableType,qs)));
            fm += (Query.RowType,ti);
            if (a != null)
                fm += (DBObject._Alias, a);
            var r = (From)cx.Add(fm);
            cx.TableRef(r);
            return (From)cx.Add(r);
        }
        /// <summary>
        /// We need to create SqlCols for this From
        /// Transaction uses additional space above tr.nextTid
        /// Database uses the heap
        /// </summary>
        /// <param name="tb"></param>
        /// <param name="ti"></param>
        /// <param name="qs"></param>
        /// <param name="i"></param>
        void DoStar(Table tb,ref ObInfo ti,ref BList<SqlValue> qs, ref int i)
        {

            var ce = new StringBuilder(db.source);
            var off = db.uid; // used if db is Transaction
            var cm = ';';
            var j = 0;
            for (var bb = ti.columns.First(); bb != null; bb = bb.Next())
            {
                var bc = (SqlCol)bb.value();
                ce.Append(cm); cm = ',';
                ce.Append(bc.name);
                long cp; 
                if (db is Transaction)
                    cp = off + ce.Length;
                else
                {
                    cp = db.nextHeap;
                    db += (Database.NextHeap, cp + 1);
                }
                var nb = new SqlCol(cp, bc.name, bc.tableCol)
                    + (DBObject._Domain, bc.tableCol.domain);
                cx.Add(nb);
                ti += (ObInfo.Columns,ti.columns+(j++, nb));
                qs += (i++, nb);
            }
            db += (CursorSpecification._Source, ce.ToString());
        }
        /// <summary>
        /// TimePeriodSpec = 
        ///    |    AS OF TypedValue
        ///    |    BETWEEN(ASYMMETRIC|SYMMETRIC)  TypedValue AND TypedValue
        ///    |    FROM TypedValue TO TypedValue .
        /// </summary>
        /// <returns>The periodSpec</returns>
        PeriodSpec ParsePeriodSpec(Query q)
        {
            var r = new PeriodSpec();
            Next();
            if (tok == Sqlx.ID)
                r.periodname = lxr.val.ToString();
            Mustbe(Sqlx.SYSTEM_TIME,Sqlx.ID);
            r.kind = tok;
            switch (tok)
            {
                case Sqlx.AS: Next();
                    Mustbe(Sqlx.OF);
                    r.time1 = ParseSqlValue(Domain.UnionDate,q.defpos);
                    break;
                case Sqlx.BETWEEN: Next();
                    r.kind = Sqlx.ASYMMETRIC;
                    if (Match(Sqlx.ASYMMETRIC))
                        Next();
                    else if (Match(Sqlx.SYMMETRIC))
                    {
                        Next();
                        r.kind = Sqlx.SYMMETRIC;
                    }
                    r.time1 = ParseSqlValueTerm(Domain.UnionDate,false);
                    Mustbe(Sqlx.AND);
                    r.time2 = ParseSqlValue(Domain.UnionDate);
                    break;
                case Sqlx.FROM: Next();
                    r.time1 = ParseSqlValue(Domain.UnionDate);
                    Mustbe(Sqlx.TO);
                    r.time2 = ParseSqlValue(Domain.UnionDate);
                    break;
                default:
                    r.kind=Sqlx.NO;
                    break;
            }
            return r;
        }
        /// <summary>
        /// Alias = 		[[AS] id [ Cols ]] .
        /// </summary>
        /// <returns>The correlation info</returns>
		Correlation ParseCorrelation()
		{
			var r = new Correlation();
            if (tok == Sqlx.ID || tok == Sqlx.AS)
			{
				if (tok==Sqlx.AS)
					Next();
                r.tablealias = new Ident(lxr).ident;
				Mustbe(Sqlx.ID);
				if (tok==Sqlx.LPAREN)
				{
					Next();
                    var ids = ParseIDList();
                    for (var i = 0; i < ids.Length; i++)
                        r.cols += ids[i].ident;
				}
			}
            return r;
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
        /// <param name="r">The Query so far</param>
        /// <returns>the updated query</returns>
        Query ParseJoinPart(long dp,Query fi,QuerySpecification q)
        {
            var left = fi;
            var jkind = Sqlx.INNER;
            Query right;
            JoinPart v = new JoinPart(dp, BTree<long, object>.Empty);
            if (Match(Sqlx.CROSS))
            {
                jkind = tok;
                Next();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(q);
            }
            else if (Match(Sqlx.NATURAL))
            {
                v += (JoinPart.Natural, tok);
                Next();
                jkind = ParseJoinType();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(q);
            }
            else
            {
                jkind = ParseJoinType();
                Mustbe(Sqlx.JOIN);
                right = ParseTableReferenceItem(q);
                if (tok == Sqlx.USING)
                {
                    v += (JoinPart.Natural, tok);
                    Next();
                    var cs = BList<long>.Empty;
                    var ids = ParseIDList();
                    for (var i = 0; i < ids.Length; i++)
                        cs += ids[i].iix;
                    v += (JoinPart.NamedCols, cs);
                }
                else
                {
                    Mustbe(Sqlx.ON);
                    var lo = BList<SqlValue>.Empty;
                    var ro = BList<SqlValue>.Empty;
                    var oc = ParseSqlValue(Domain.Bool).Disjoin();
                    var lm = left.rowType.map;
                    var rm = right.rowType.map;
                    for (var b = oc.First(); b != null; b = b.Next())
                    {
                        var se = b.value() as SqlValueExpr;
                        if (se == null || se.kind != Sqlx.EQL) throw new DBException("42151");
                        var lf = se.left;
                        var rg = se.right;
                        if (lf == null || rg == null) throw new DBException("42151");
                        var rev = !lm.Contains(lf.name);
                        if (rev)
                        {
                            if ((!rm.Contains(lf.name))
                                || (!lm.Contains(rg.name)))
                                throw new DBException("42151");
                            oc += (b.key(), new SqlValueExpr(se.defpos, Sqlx.EQL, rg, lf, Sqlx.NO));
                            lo += rg;
                            ro += lf;
                        }
                        else
                        {
                            if (!rm.Contains(rg.name))
                                throw new DBException("42151");
                            lo += lf;
                            ro += rg;
                        }
                    }
                    v += (JoinPart.JoinCond,oc);
                    left += (Query.OrdSpec, new OrderSpec(lo));
                    right += (Query.OrdSpec, new OrderSpec(ro));
                }
            }
            v += (JoinPart.JoinKind, jkind);
            v += (JoinPart.LeftOperand, left);
            v += (JoinPart.RightOperand, right);
            //       for (var b = v.joinCond.First(); b != null; b = b.Next())
            //         r.Needs(b.value().alias ?? b.value().name, Query.Need.joined);
            q = (QuerySpecification)q.Refresh(cx);
            return (Query)cx.Add(v.Selects(cx,q).Conditions(cx).Orders(db as Transaction,cx,q.ordSpec));
        }
        /// <summary>
		/// GroupByClause = GROUP BY [DISTINCT|ALL] GroupingElement { ',' GroupingElement } .
        /// GroupingElement = GroupingSet | (ROLLUP|CUBE) '('GroupingSet {',' GroupingSet} ')'  
        ///     | GroupSetsSpec | '('')' .
        /// GroupingSet = Col | '(' Col {',' Col } ')' .
        /// GroupingSetsSpec = GROUPING SETS '(' GroupingElement { ',' GroupingElement } ')' .
        /// </summary>
        /// <returns>The GroupSpecification</returns>
        GroupSpecification ParseGroupClause(Query q)
        {
            if (tok != Sqlx.GROUP)
                return null;
            Next();
            var lp = lxr.Position;
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
            GroupSpecification r = new GroupSpecification(lp,BTree<long, object>.Empty
                + (GroupSpecification.DistinctGp, d));
            r = ParseGroupingElement(r,ref simple);
            while (tok == Sqlx.COMMA)
            {
                Next();
                r = ParseGroupingElement(r,ref simple);
            }
            lp = lxr.Position;
            // simplify: see SQL2003-02 7.9 SR 10 .
            if (simple && r.sets.Count > 1)
            {
                Grouping gn = new Grouping(lp);
                var i = 0;
                for (var g = r.sets.First();g!=null;g=g.Next())
                    for (var h = g.value().members.First(); h!=null;h=h.Next())
                        gn += (Grouping.Members,gn.members+(h.key(),i++));
                r += (GroupSpecification.Sets, new BList<Grouping>(gn));
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
                var ls = new Grouping(cn.iix,BTree<long,object>.Empty+
                    (Grouping.Members,new BTree<SqlValue,int>((SqlValue)cx.Get(cn), 0)));
                g += (GroupSpecification.Sets,g.sets+ls);
                simple = true;
                return (GroupSpecification)cx.Add(g);
            }
            simple = false;
            if (Match(Sqlx.LPAREN))
            {
                var lp = lxr.Position;
                Next();
                if (tok == Sqlx.RPAREN)
                {
                    Next();
                    g += (GroupSpecification.Sets,g.sets+new Grouping(lp));
                    return (GroupSpecification)cx.Add(g);
                }
                g +=(GroupSpecification.Sets,g.sets+ParseGroupingSet());
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
            var r = cx.obs[cn.iix];
            var t = new Grouping(cn.iix,BTree<long,object>.Empty
                +(Grouping.Members,new BTree<SqlValue,int>((SqlValue)r,0)));
            var i = 1;
            while (Match(Sqlx.COMMA))
            {
                cn = ParseIdent();
                r = cx.obs[cn.iix];
                t+=(Grouping.Members,t.members+((SqlValue)r,i++));
            }
            Mustbe(Sqlx.RPAREN);
            return (Grouping)cx.Add(t);
        }
        /// <summary>
		/// HavingClause = HAVING BooleanExpr .
        /// </summary>
        /// <returns>The SqlValue (Boolean expression)</returns>
		BTree<long,SqlValue> ParseHavingClause()
        {
            var r = BTree<long,SqlValue>.Empty;
            if (tok != Sqlx.HAVING)
                return r;
            Next();
            var lp = lxr.Position;
            var d = ParseSqlValueDisjunct(Domain.Bool,false);
            if (tok != Sqlx.OR)
            {
                foreach (var s in d)
                {
                    r +=(s.defpos, s);
       //             lxr.context.cur.Needs(s.alias ?? s.name, Query.Need.condition);
                }
                return r;
            }
            var left = Disjoin(d);
            while (tok == Sqlx.OR)
            {
                Next();
                left = new SqlValueExpr(lp, Sqlx.OR, left, 
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool,false)), Sqlx.NO);
            }
            r +=(left.defpos, left);
      //      lxr.context.cur.Needs(left.alias ?? left.name, Query.Need.condition);
            return r;
        }
        /// <summary>
		/// WhereClause = WHERE BooleanExpr .
        /// </summary>
        /// <returns>The SqlValue (Boolean expression)</returns>
		BTree<long,SqlValue> ParseWhereClause(Query q)
		{
            var r = BTree<long,SqlValue>.Empty;
            if (tok != Sqlx.WHERE)
                return r;
			Next();
            var d = ParseSqlValueDisjunct(Domain.Bool,false);
            if (tok != Sqlx.OR)
            {
                foreach (var s in d)
                    r +=(s.defpos, s);
                //              lxr.context.cur.Needs(s.alias ?? s.name, Query.Need.condition);
                return r;
            }
            var left = Disjoin(d);
            while (tok == Sqlx.OR)
            {
                var lp = lxr.Position;
                Next();
                left = new SqlValueExpr(lp, Sqlx.OR, left, 
                    Disjoin(ParseSqlValueDisjunct(Domain.Bool,false)), Sqlx.NO);
                left = (SqlValue)cx.Add(left);
            }
            r +=(left.defpos, left);
     //       lxr.context.cur.Needs(left.alias ?? left.name,Query.Need.condition);
            return r;
		}
        /// <summary>
		/// WindowClause = WINDOW WindowDef { ',' WindowDef } .
        /// </summary>
        /// <returns>the window set as a tree by window names</returns>
        BTree<string,WindowSpecification> ParseWindowClause()
        {
            if (tok != Sqlx.WINDOW)
                return null;
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
                WindowSpecification ow = tree[r.orderWindow];
                if (ow==null)
                    throw new DBException("42135", r.orderWindow).Mix();
                if (r.partition != 0)
                    throw new DBException("42000", "7.11 SR10c").ISO();
                if (ow.order != null && r.order != null)
                    throw new DBException("42000", "7.11 SR10d").ISO();
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
        internal SqlInsert ParseSqlInsert(Context cx,string sql,bool autoKey=true)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid + sql.Length);
            return ParseSqlInsert();
        }
        /// <summary>
		/// Insert = INSERT [WITH (PROVENANCE|TYPE_URI) string][XMLOption] INTO Table_id [ Cols ]  TypedValue [Classification].
        /// </summary>
        /// <returns>the executable</returns>
        SqlInsert ParseSqlInsert()
        {
            string prov = null;
            bool with = false;
            var lp = lxr.Position;
            Next();
            if (tok == Sqlx.WITH)
            {
                Next();
                if (Match(Sqlx.PROVENANCE, Sqlx.TYPE_URI))
                {
                    Next();
                    prov = lxr.val.ToString();
                    Mustbe(Sqlx.CHARLITERAL);
                }
                else
                    with = true;
            }
            ParseXmlOption(with);
            Mustbe(Sqlx.INTO);
            Ident ic = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var oi = db.GetObInfo(ic.ident)??
                throw new DBException("42107", ic.ident);
            var idlist = false;
            Correlation r = null;
            if (tok == Sqlx.LPAREN)
            {
                Next();
                idlist = (tok == Sqlx.ID);
                tok = lxr.PushBack(Sqlx.LPAREN);
            }
            if (idlist)
                r = new Correlation(ParseIDList());
            SqlValue v;
            var vp = lxr.Position;
            if (tok == Sqlx.DEFAULT)
            {
                Next();
                Mustbe(Sqlx.VALUES);
                v = SqlNull.Value;
            }
            else
                // care: we might have e.g. a subquery here
                v = ParseSqlValue(oi);
            if (v is SqlRow) // tolerate a single value without the VALUES keyword
                v = new SqlRowArray(vp, BTree<long, object>.Empty
                    + (SqlRowArray.Rows, new BList<SqlRow>(v as SqlRow)));
            SqlInsert s = new SqlInsert(lp,db, cx, ic, r, prov, v);
            if (Match(Sqlx.SECURITY))
            {
                Next();
                if (db.user.defpos != db.owner)
                    throw new DBException("42105");
                s += (DBObject.Classification, MustBeLevel());
            }
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = tr.Execute(s, cx);
            return (SqlInsert)cx.Add(s);
        }
        /// <summary>
		/// DeleteSearched = DELETE [XMLOption] FROM Table_id [ WhereClause] .
        /// </summary>
        /// <returns>the executable</returns>
		Executable ParseSqlDelete()
        {
            var lp = lxr.Position;
            Next();
            ParseXmlOption(false);
            Mustbe(Sqlx.FROM);
            Ident a = null;
            Ident ic = new Ident(lxr);
            Mustbe(Sqlx.ID);
            if (tok == Sqlx.AS)
                Next();
            if (tok == Sqlx.ID)
            {
                a = new Ident(lxr);
                Next();
            }
            QuerySearch r = new QuerySearch(lp, db, cx, ic, (a!=null)?new Correlation(a.ident):null, 
                Grant.Privilege.Delete);
            var fm = r.table;
            ObInfo dt = fm.rowType;
            for (var i = 0; i < dt.Length; i++)
            {
                var co = dt.columns[i];
                cx.defs += (new Ident(dt.columns[i].name, co.defpos), co);
                cx.obs += (co.defpos, co);
            }
            r += (SqlInsert._Table, 
                fm.AddCols(cx,fm).AddCondition(cx,Query.Where, ParseWhereClause(fm)));
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = r.table.Delete(tr, cx, BTree<string, bool>.Empty, new Adapters());
            return (Executable)cx.Add(r);
        }
        /// <summary>
        /// the update statement
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="sql">the sql</param>
        /// <returns>the updatesearch</returns>
        internal UpdateSearch ParseSqlUpdate(Context cx, string sql)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid + sql.Length);
            return ParseSqlUpdate() as UpdateSearch;
        }
        /// <summary>
		/// UpdateSearched = UPDATE [XMLOption] Table_id Assignment [WhereClause] .
        /// </summary>
        /// <returns>The UpdateSearch</returns>
		Executable ParseSqlUpdate()
        {
            var lp = lxr.Position; 
            Next();
            ParseXmlOption(false);
            Ident ic = new Ident(lxr);

            Ident a = null;
            Mustbe(Sqlx.ID);
            if (tok != Sqlx.SET)
            {
                if (tok == Sqlx.AS)
                    Next();
                a = new Ident(lxr);
                Mustbe(Sqlx.ID);
            }
            Mustbe(Sqlx.SET);
            UpdateSearch r = new UpdateSearch(lp, db, cx, ic, (a==null)?null:new Correlation(a.ident), Grant.Privilege.Update);
            var fm = r.table;
            ObInfo dt = fm.rowType;
            for (var i = 0; i < dt.Length; i++)
            {
                var co = dt.columns[i];
                cx.defs += (new Ident(dt.columns[i].name, co.defpos), co);
                cx.obs += (co.defpos, co);
            }
            fm = (From)fm.AddCols(cx,fm);
            fm += (From.Assigns, ParseAssignments(fm.rowType));
            fm = (From)fm.AddCondition(cx,Query.Where, ParseWhereClause(fm));
            r += (SqlInsert._Table, fm);
            if (db.parse == ExecuteStatus.Obey && db is Transaction tr)
                db = r.table.Update(tr, cx, BTree<string, bool>.Empty, new Adapters(), new List<RowSet>());
            return (Executable)cx.Add(r);
        }
        internal BList<UpdateAssignment> ParseAssignments(string sql,ObInfo rt)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            return ParseAssignments(rt);
        }
        /// <summary>
        /// Assignment = 	SET Target '='  TypedValue { ',' Target '='  TypedValue }
        /// </summary>
        /// <returns>the list of assignments</returns>
		BList<UpdateAssignment> ParseAssignments(ObInfo rt)
		{
            var r = BList<UpdateAssignment>.Empty + ParseUpdateAssignment(rt);
            while (tok==Sqlx.COMMA)
			{
				Next();
				r+=ParseUpdateAssignment(rt);
			}
			return r;
		}
        /// <summary>
        /// Target '='  TypedValue
        /// </summary>
        /// <returns>An updateAssignmentStatement</returns>
		UpdateAssignment ParseUpdateAssignment(ObInfo rt)
        {
            SqlValue vbl;
            SqlValue val;
            Match(Sqlx.SECURITY);
 /*           if (tok == Sqlx.SECURITY)
            {
                if (tr.user.defpos != tr.owner)
                    throw new DBException("42105");
                sa.vbl = new LevelTarget(lxr);
                Next();
            }
            else */
            {
                var ic = new Ident(lxr);
                Mustbe(Sqlx.ID);
                vbl = (SqlValue)cx.Get(ic,rt)
                    ?? throw new DBException("42112", ic.ToString()).Mix();
            }
            Mustbe(Sqlx.EQL);
            val = ParseSqlValue(vbl.domain);
            return new UpdateAssignment(vbl,val);
        }
        /// <summary>
        /// Parse an SQL TypedValue
        /// </summary>
        /// <param name="s">The string to parse</param>
        /// <param name="t">the expected data type if any</param>
        /// <returns>the SqlValue</returns>
        internal SqlValue ParseSqlValue(string s,DBObject rt)
        {
            lxr = new Lexer(s);
            tok = lxr.tok;
            return ParseSqlValue(rt);
        }
        /// <summary>
        /// Alas the following informal syntax is not a good guide to the way LL(1) has to go...
		///  Value = 		Literal
        /// |   ID ':'  TypedValue
		/// |	Value BinaryOp TypedValue 
		/// | 	'-'  TypedValue 
		/// |	'('  TypedValue ')'
		/// |	Value Collate 
		/// | 	Value '['  TypedValue ']'
		/// |	ColumnRef  
		/// | 	VariableRef
        /// |   PeriodName
        /// |   PERIOD '('  TypedValue,  TypedValue ')'
		/// |	VALUE 
        /// |   ROW
		/// |	Value '.' Member_id
		/// |	MethodCall
		/// |	NEW MethodCall 
		/// | 	FunctionCall 
		/// |	VALUES  '('  TypedValue { ','  TypedValue } ')' { ',' '('  TypedValue { ','  TypedValue } ')' }
		/// |	Subquery
		/// |	( MULTISET | ARRAY | ROW) '('  TypedValue { ','  TypedValue } ')'
		/// | 	TABLE '('  TypedValue ')' 
		/// |	TREAT '('  TypedValue AS Sub_Type ')'  .
        /// PeriodName = SYSTEM_TIME | id .
        /// </summary>
        /// <param name="t">a constraint on usage</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the SqlValue</returns>
        SqlValue ParseSqlValue(DBObject rt,long curq= -1L,bool wfok=false)
        {
            if (tok == Sqlx.NULL)
            {
                Next();
                return SqlNull.Value;
            }
            if (tok == Sqlx.PERIOD)
            {
                Next();
                Mustbe(Sqlx.LPAREN);
                var op1 = ParseSqlValue(Domain.UnionDate,curq);
                Mustbe(Sqlx.COMMA);
                var op2 = ParseSqlValue(Domain.UnionDate,curq);
                Mustbe(Sqlx.RPAREN);
                return (SqlValue)cx.Add(new SqlValueExpr(lxr.Position, Sqlx.PERIOD, op1, op2, Sqlx.NO));
            }
            SqlValue left = null;
            left = Disjoin(ParseSqlValueDisjunct(rt,wfok));
            while (left.domain.kind==Sqlx.BOOLEAN && tok==Sqlx.OR)
            {
                Next();
                left = new SqlValueExpr(lxr.Position, Sqlx.OR, left, 
                    Disjoin(ParseSqlValueDisjunct(rt,wfok)), Sqlx.NO);
            }
            while (Match(Sqlx.UNION, Sqlx.EXCEPT, Sqlx.INTERSECT))
            {
                var lp = lxr.Position;
                var op = tok;
                var m = Sqlx.NO;
                Next();
                if ((op == Sqlx.UNION || op == Sqlx.EXCEPT) && Match(Sqlx.ALL, Sqlx.DISTINCT))
                {
                    m = tok;
                    Next();
                }
                var right = ParseSqlValueTerm(rt,wfok);
                left = new SqlValueExpr(lp, op, left, right, Sqlx.NO);
            }
            ObInfo ti = null;
            return ((SqlValue)cx.Add(left)).Resolve(cx,null,ref ti);
        }
        SqlValue Disjoin(List<SqlValue> s)
        {
            var right = s[s.Count - 1];
            for (var i = s.Count - 2; i >= 0; i--)
                right = new SqlValueExpr(lxr.Position, Sqlx.AND, s[i], right, Sqlx.NO);
            return (SqlValue)cx.Add(right);
        }
        List<SqlValue> ParseSqlValueDisjunct(DBObject rt,bool wfok)
        {
            var r = new List<SqlValue>();
            var left = ParseSqlValueConjunct(rt,wfok);
            r.Add(left);
            while (left.domain.kind==Sqlx.BOOLEAN && Match(Sqlx.AND))
            {
                Next();
                left = ParseSqlValueConjunct(rt,wfok);
                r.Add(left);
            }
            return r;
        }
        SqlValue ParseSqlValueConjunct(DBObject rt,bool wfok)
        {
            var left = ParseSqlValueExpression(Domain.Content,wfok);
            if (Match(Sqlx.EQL, Sqlx.NEQ, Sqlx.LSS, Sqlx.GTR, Sqlx.LEQ, Sqlx.GEQ))
            {
                var op = tok;
                var lp = lxr.Position;
                Next();
                return (SqlValue)cx.Add(new SqlValueExpr(lp, 
                    op, left, ParseSqlValueExpression(left.domain,wfok), Sqlx.NO));
            }
            return (SqlValue)cx.Add(left);
        }
        SqlValue ParseSqlValueExpression(DBObject rt,bool wfok)
        {
            var left = ParseSqlValueTerm(rt,wfok);
            while (Domain.UnionDateNumeric.CanTakeValueOf(left.domain) 
                && Match(Sqlx.PLUS, Sqlx.MINUS))
            {
                var op = tok;
                var lp = lxr.Position;
                Next();
                left = new SqlValueExpr(lp, op, left, 
                    ParseSqlValueTerm(rt,wfok), Sqlx.NO);
            }
            return (SqlValue)cx.Add(left);
        }
        /// <summary>
        /// |   NOT TypedValue
        /// |	Value BinaryOp TypedValue 
        /// |   PeriodPredicate
		/// BinaryOp =	'+' | '-' | '*' | '/' | '||' | MultisetOp | AND | OR | LT | GT | LEQ | GEQ | EQL | NEQ. 
		/// MultisetOp = MULTISET ( UNION | INTERSECT | EXCEPT ) ( ALL | DISTINCT ) .
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
        SqlValue ParseSqlValueTerm(DBObject rt,bool wfok)
        {
            bool sign = false, not = false;
            var lp = lxr.Position;
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
            var left = ParseSqlValueFactor(Domain.Content,wfok);
            if (sign)
                left = new SqlValueExpr(lp, Sqlx.MINUS, null, left, Sqlx.NO);
            else if (not)
                left = left.Invert();
            var imm = Sqlx.NO;
            if (Match(Sqlx.IMMEDIATELY))
            {
                Next();
                imm = Sqlx.IMMEDIATELY;
            }
            if (Match(Sqlx.CONTAINS, Sqlx.OVERLAPS, Sqlx.EQUALS, Sqlx.PRECEDES, Sqlx.SUCCEEDS))
            {
                var op = tok;
                lp = lxr.Position;
                Next();
                return (SqlValue)cx.Add(new SqlValueExpr(lp, 
                    op, left, ParseSqlValueFactor(left.domain,wfok), imm));
            }
//            if (Match(Sqlx.AND))
//            {
//                while (Match(Sqlx.AND))
//                {
//                    lp = lxr.Position;
//                    Next();
//                    left = new SqlValueExpr(lp, Sqlx.AND, left, ParseSqlValueTerm(wfok),Sqlx.NO);
//                }
//                return (SqlValue)cx.Add(left);
//            }
            while (Match(Sqlx.TIMES, Sqlx.DIVIDE,Sqlx.MULTISET))
            {
                Sqlx op = tok;
                lp = lxr.Position;
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
                left = new SqlValueExpr(lp, op, left, ParseSqlValueFactor(left.domain,wfok), m);
            }
            if (!rt.domain.CanTakeValueOf(left.domain))
                throw new DBException("42000", lxr.pos);
            return (SqlValue)cx.Add(left);
        }
        /// <summary>
        /// |	Value '||'  TypedValue 
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the SqlValue</returns>
		SqlValue ParseSqlValueFactor(DBObject rt,bool wfok)
		{
			var left = ParseSqlValueEntry(rt,wfok);
			while (left.domain.kind==Sqlx.CHAR && Match(Sqlx.CONCATENATE))
			{
				Sqlx op = tok;
				Next();
				var right = ParseSqlValueEntry(Domain.Char,wfok);
				left = new SqlValueExpr(lxr.Position, op,left,right,Sqlx.NO);
			}
			return (SqlValue)cx.Add(left);
		}
        /// <summary>
        /// | 	Value '['  TypedValue ']'
        /// |	ColumnRef  
        /// | 	VariableRef 
        /// |   TypedValue
        /// |   PeriodRef | SYSTEM_TIME
        /// |   id ':'  Value
        /// |	Value '.' Member_id
        /// |   Value IN '(' Query | (Value {',' Value })')'
        /// |   Value IS [NOT] NULL
        /// |   Value IS [NOT] MEMBER OF Value
        /// |   Value IS [NOT] BETWEEN Value AND Value
        /// |   Value IS [NOT] OF '(' [ONLY] id {',' [ONLY] id } ')'
        /// |   Value IS [NOT] LIKE Value [ESCAPE TypedValue ]
        /// |   Value COLLATE id
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <param name="wfok">whether to allow a window function</param>
        /// <returns>the sqlValue</returns>
		SqlValue ParseSqlValueEntry(DBObject rt,bool wfok)
        {
            var left = ParseSqlValueItem(Domain.Content,wfok);
            bool invert = false;
            var lp = lxr.Position;
            if (left is SqlValue && tok == Sqlx.COLON)
            {
                var fl = left as SqlValue;
                if (fl == null)
                    throw new DBException("42000", left.ToString()).ISO();
                Next();
                left = ParseSqlValueItem(rt,wfok);
                // ??
            }
            while (tok==Sqlx.DOT || tok==Sqlx.LBRACK)
                if (tok==Sqlx.DOT)
                {
                    // could be table alias, block id, instance id etc
                    Next();
                    if (tok == Sqlx.TIMES)
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlStar(lxr.Position, left.from));
                    }
                    var n = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    if (tok == Sqlx.LPAREN)
                    {
                        Next();
                        var ps = ParseSqlValueList(rt);
                        var pr = (left.domain as UDType)?.methods[n.ident][(int)ps.Count]
                            ?? throw new DBException("42173", n);
                        var cs = new CallStatement(lp, pr, ps, left);
                        Mustbe(Sqlx.RPAREN);
                        left = new SqlMethodCall(n.iix, cs);
                    }
                    else
                    {
                        left = new SqlValueExpr(lp, Sqlx.DOT, left,
                            new SqlValue(lxr.Position, n.ident), Sqlx.NO);
                        if (left.mem.Contains(SqlValue.Info))
                            left += (SqlValue.Info,left.info);
                    }
                } else // tok==Sqlx.LBRACK
                {
                    Next();
                    left = new SqlValueExpr(lp, Sqlx.LBRACK, left,
                        ParseSqlValue(Domain.Int), Sqlx.NO);
                    Mustbe(Sqlx.RBRACK);
                }

            if (tok == Sqlx.IS)
            {
                if (!rt.domain.CanTakeValueOf(Domain.Bool))
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
                    bool only = (tok == Sqlx.ONLY);
                    if (only)
                        Next();
                    var r = BList<Domain>.Empty;
                    var t1 = ParseSqlDataType().domain;
                    lp = lxr.Position;
                    if (only)
                        t1 = new Domain(lp,Sqlx.ONLY,new BTree<long,object>(Domain.Under,t1));
                    r+=t1;
                    while (tok == Sqlx.COMMA)
                    {
                        Next();
                        only = (tok == Sqlx.ONLY);
                        if (only)
                            Next();
                        t1 = ParseSqlDataType().domain;
                        lp = lxr.Position;
                        if (only)
                            t1 = new Domain(lp,Sqlx.ONLY, new BTree<long, object>(Domain.Under, t1));
                        r+=t1;
                    }
                    Mustbe(Sqlx.RPAREN);
                    return (SqlValue)cx.Add(new TypePredicate(lp,left, b, r));
                }
                Mustbe(Sqlx.NULL);
                return (SqlValue)cx.Add(new NullPredicate(lp,left, b));
            }
            var savestart = lxr.start;
            if (tok == Sqlx.NOT)
            {
                if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                invert = true;
            }
            if (tok == Sqlx.BETWEEN)
            {
                if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                BetweenPredicate b = new BetweenPredicate(lp, left, !invert, 
                    ParseSqlValueTerm(left.domain,false), null);
                Mustbe(Sqlx.AND);
                b+=(QuantifiedPredicate.High,ParseSqlValue(left.domain));
                return (SqlValue)cx.Add(b);
            }
            if (tok == Sqlx.LIKE)
            {
                if (!(rt.domain.CanTakeValueOf(Domain.Bool) && 
                    Domain.Char.CanTakeValueOf(left.domain)))
                    throw new DBException("42000", lxr.pos);
                Next();
                LikePredicate k = new LikePredicate(lp,left, !invert, 
                    ParseSqlValue(Domain.Char), null);
                if (tok == Sqlx.ESCAPE)
                {
                    Next();
                    k+=(LikePredicate.Escape,ParseSqlValueItem(Domain.Char,false));
                }
                return (SqlValue)cx.Add(k);
            }
#if SIMILAR
            if (tok == Sqlx.LIKE_REGEX)
            {
                            if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                SimilarPredicate k = new SimilarPredicate(left, !invert, ParseSqlValue(), null, null);
                if (Match(Sqlx.FLAG))
                {
                    Next();
                    k.flag = ParseSqlValue();
                }
                return (SqlValue)cx.Add(k);
            }
            if (tok == Sqlx.SIMILAR)
            {
                            if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Sqlx.TO);
                SimilarPredicate k = new SimilarPredicate(left, !invert, ParseSqlValue(), null, null);
                if (Match(Sqlx.ESCAPE))
                {
                    Next();
                    k.escape = ParseSqlValue(Domain.Char);
                }
                return (SqlValue)cx.Add(k);
            }
#endif
            if (tok == Sqlx.IN)
            {
                if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                var oi = new ObInfo(-1, Domain.Row, new BList<SqlValue>(left));
                Next();
                InPredicate n = new InPredicate(lxr.Position, left)+
                    (QuantifiedPredicate.Found, !invert);
                Mustbe(Sqlx.LPAREN);
                if (Match(Sqlx.SELECT, Sqlx.TABLE, Sqlx.VALUES))
                    n+=(QuantifiedPredicate.Select,ParseQuerySpecification(oi));
                else
                    n+=(QuantifiedPredicate.Vals,ParseSqlValueList(left.domain));
                Mustbe(Sqlx.RPAREN);
                return (SqlValue)cx.Add(n);
            }
            if (tok == Sqlx.MEMBER)
            {
                if (!rt.domain.CanTakeValueOf(Domain.Bool))
                    throw new DBException("42000", lxr.pos);
                Next();
                Mustbe(Sqlx.OF);
                return (SqlValue)cx.Add(new MemberPredicate(lxr.Position,left,
                    !invert, ParseSqlValue(Domain.Multiset)));
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
            return (SqlValue)cx.Add(new SqlValueExpr(lxr.Position, 
                Sqlx.COLLATE, e, o.Build(e.defpos), Sqlx.NO));
        }
        /// <summary>
        ///  TypedValue= [NEW} MethodCall
        /// | 	FunctionCall 
        /// |	VALUES  '('  TypedValue { ','  TypedValue } ')' { ',' '('  TypedValue { ','  TypedValue } ')' }
        /// |	Subquery
        /// |	( MULTISET | ARRAY | ROW) '('  TypedValue { ','  TypedValue } ')'
        /// | 	TABLE '('  TypedValue ')' 
        /// |	TREAT '('  TypedValue AS Sub_Type ')'  
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
        /// The presence of the OVER keyword makes a window function. In accordance with SQL2003-02 section 4.15.3, window functions can only be used in the select list of a QuerySpecification or SelectSingle or the order by clause of a “simple table query” as defined in section 7.5 above. Thus window functions cannot be used within expressions or as function arguments.
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
        /// Provenance = PROVENANCE|TYPE_URI .
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
        /// <param name="t">the expected data type</param>
        /// <param name="wfok">whether a window function is allowed</param>
        /// <returns>the sql value</returns>
        SqlValue ParseSqlValueItem(DBObject rt,bool wfok)
        {
            SqlValue r;
            if (tok == Sqlx.QMARK && db.parse == ExecuteStatus.Prepare)
            {
                Next();
                var qm = new SqlValueExpr(lxr.Position, Sqlx.QMARK, null, null, Sqlx.NO);
                cx.qParams += qm;
                return qm;
            }
            if (Match(Sqlx.LEVEL))
            {
                return (SqlValue)cx.Add(new SqlLiteral(lxr.Position, TLevel.New(MustBeLevel())));
            }
            if (Match(Sqlx.HTTP))
            {
                Next();
                if (!Match(Sqlx.GET))
                { // supplying user and password this way is no longer supported
                    ParseSqlValue(Domain.Char);
                    if (Match(Sqlx.COLON))
                    {
                        Next();
                        ParseSqlValue(Domain.Char);
                    }
                }
                Mustbe(Sqlx.GET);
                var x = ParseSqlValue(rt);
                string m = "application/json";
                if (Match(Sqlx.AS))
                {
                    Next();
                    m = lxr.val.ToString();
                    Mustbe(Sqlx.CHARLITERAL);
                }
                var wh = BTree<long,SqlValue>.Empty;
                if (Match(Sqlx.WHERE))
                {
                    Next();
                    wh = ParseSqlValue(Domain.Bool).Disjoin();
        //            for (var b = wh.First(); b != null; b = b.Next())
       //                 lxr.context.cur.Needs(b.value().alias ?? b.value().name,Query.Need.condition);
                }
                return (SqlValue)cx.Add(new SqlHttp(lxr.Position, null, x, m, ObInfo.Any, wh, "*"));
            }
            var lp = lxr.Position;
            Match(Sqlx.SCHEMA); // for Pyrrho 5.1 most recent schema change
            if (Match(Sqlx.ID,Sqlx.NEXT,Sqlx.LAST,Sqlx.CHECK,Sqlx.PROVENANCE,Sqlx.TYPE_URI)) // ID or pseudo ident
            {
                bool tp = tok == Sqlx.TYPE_URI;
                SqlValue vr = ParseVarOrColumn(rt);
                if (tok == Sqlx.DOUBLECOLON)
                {
                    Next();
                    var ut = db.GetObject(vr.name) as UDType
                        ?? throw new DBException("42139",vr.name).Mix();
                    var ui = db.role.obinfos[vr.defpos] as ObInfo;
                    var name = new Ident(lxr);
                    Mustbe(Sqlx.ID);
                    lp = lxr.Position;
                    Mustbe(Sqlx.LPAREN);
                    var ps = ParseSqlValueList(rt);
                    Mustbe(Sqlx.RPAREN);
                    int n = (int)ps.Count;
                    var m = ut.methods[name.ident]?[n] 
                        ?? throw new DBException("42132",name.ident,ui.name).Mix();
                    if (m.methodType != PMethod.MethodType.Static)
                        throw new DBException("42140").Mix();
                    var fc = new CallStatement(lp, m, ps, vr);
                    return (SqlValue)cx.Add(new SqlProcedureCall(name.iix, fc));
                }
                return (SqlValue)cx.Add(vr);
            }
            if (Match(Sqlx.EXISTS,Sqlx.UNIQUE))
            {
                Sqlx op = tok;
                Next();
                Mustbe(Sqlx.LPAREN);
                QueryExpression g = ParseQueryExpression(ObInfo.Any);
                Mustbe(Sqlx.RPAREN);
                if (op == Sqlx.EXISTS)
                    return (SqlValue)cx.Add(new ExistsPredicate(lxr.Position, g));
                else
                    return (SqlValue)cx.Add(new UniquePredicate(lxr.Position, g));
            }
            if (Match(Sqlx.RDFLITERAL, Sqlx.DOCUMENTLITERAL, Sqlx.CHARLITERAL, 
                Sqlx.INTEGERLITERAL, Sqlx.NUMERICLITERAL,
            Sqlx.REALLITERAL, Sqlx.BLOBLITERAL, Sqlx.BOOLEANLITERAL))
            {
                r = lxr.val.Build(lxr.Position);
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
                            Next();
                            if (tok == Sqlx.SELECT)
                            {
                                var st = lxr.start;
                                var cs = ParseCursorSpecification(ObInfo.Any,true).cs;
                                Mustbe(Sqlx.RPAREN);
                                return (SqlValue)cx.Add(new SqlValueSelect(lp+1, cs, 
                                    new string(lxr.input, st, lxr.start - st)));
                            }
                            throw new DBException("22204");
                        }
                        Mustbe(Sqlx.LBRACK);
                        var et = (rt == Domain.Content) ? rt :
                            (rt as Domain)?.elType ??
                            throw new DBException("42000", lxr.pos);
                        var v = ParseSqlValueList(et);
                        if (v.Count == 0)
                            throw new DBException("22103").ISO();
                        Mustbe(Sqlx.RBRACK);
                        return (SqlValue)cx.Add(new SqlValueArray(lp, v));
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
                            var tb = ob as Table;
                            if (tb == null)
                                throw new DBException("42107", n).Mix();
                            var ft = db.role.obinfos[tb.defpos] as ObInfo;
                            ob = ft.ColFor(cn.ToString());
                        }
                        r = new SqlLiteral(lp, new TInt(ob.lastChange));
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(r);
                    } 
                case Sqlx.CURRENT_DATE: 
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp,Sqlx.CURRENT_DATE, 
                            null, null,null,Sqlx.NO));
                    }
                case Sqlx.CURRENT_ROLE:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp, Sqlx.CURRENT_ROLE, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.CURRENT_TIME:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp, Sqlx.CURRENT_TIME, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.CURRENT_TIMESTAMP: 
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp, Sqlx.CURRENT_TIMESTAMP, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.CURRENT_USER:
                    {
                        Next();
                        return (SqlValue)cx.Add(new SqlFunction(lp, Sqlx.CURRENT_USER, 
                            null, null, null, Sqlx.NO));
                    }
                case Sqlx.DATE: // also TIME, TIMESTAMP, INTERVAL
                    {
                        Sqlx tk = tok;
                        Next();
                        var o = lxr.val;
                        lp = lxr.Position;
                        if (tok == Sqlx.CHARLITERAL)
                        {
                            Next();
                            return (SqlValue)cx.Add(new SqlDateTimeLiteral(lp, 
                                new Domain(lp,tk,BTree<long,object>.Empty), o.ToString()));
                        }
                        else
                            return (SqlValue)cx.Add(new SqlLiteral(lp, o));
                    }
                case Sqlx.INTERVAL:
                    {
                        Next();
                        var o = lxr.val;
                        Mustbe(Sqlx.CHARLITERAL);
                        Domain di = ParseIntervalType();
                        return (SqlValue)cx.Add(new SqlDateTimeLiteral(lp, di, o.ToString()));
                    }
                case Sqlx.LPAREN:
                    {
                        Mustbe(Sqlx.LPAREN);
                        if (tok == Sqlx.SELECT)
                        {
                            var st = lxr.start;
                            var cs = ParseCursorSpecification(ObInfo.Any,true).cs;
                            Mustbe(Sqlx.RPAREN);
                            return (SqlValue)cx.Add(new SqlValueSelect(lp, cs, 
                                new string(lxr.input,st,lxr.start-st)));
                        }
                        var fs = BList<SqlValue>.Empty;
                        var oi = (rt==Domain.Content)?ObInfo.Any :
                            rt as ObInfo ?? throw new DBException("42000", lxr.pos);
                        for (var i = 0; ; i++)
                        {
                            var it = ParseSqlValue(oi.columns[i]?.domain ?? Domain.Content);
                            if (tok == Sqlx.AS)
                            {
                                lp = lxr.Position;
                                Next();
                                var ic = new Ident(lxr);
                                Mustbe(Sqlx.ID);
                                it += (DBObject._Alias, ic.ToString());
                            }
                            fs += it;
                            if (tok != Sqlx.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sqlx.RPAREN);
                        if (fs.Count == 1)
                            return (SqlValue)cx.Add(fs[0]);
                        return (SqlValue)cx.Add(new SqlRow(lp,fs));
                    }
                case Sqlx.MULTISET:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                            return ParseSqlValue(rt);
                        Mustbe(Sqlx.LBRACK);
                        var v = ParseSqlValueList(rt);
                        if (v.Count == 0)
                            throw new DBException("22103").ISO();
                        Domain dt = v[0].domain;
                        Mustbe(Sqlx.RBRACK);
                        return (SqlValue)cx.Add(new SqlValueArray(lp, v));
                    }
                case Sqlx.NEW:
                    {
                        Next();
                        var o = new Ident(lxr);
                        Mustbe(Sqlx.ID);
                        lp = lxr.Position;
                        var ut = db.GetObject(o.ident) as UDType??
                            throw new DBException("42142").Mix();
                        Mustbe(Sqlx.LPAREN);
                        var ps = ParseSqlValueList((ObInfo)db.role.obinfos[ut.tabledefpos]);
                        int n = (int)ps.Count;
                        Mustbe(Sqlx.RPAREN);
                        Method m = ut.methods[o.ident]?[n];
                        var ui = db.role.obinfos[ut.defpos] as ObInfo;
                        if (m == null)
                        {
                            if (ui.columns != null && (int)ui.columns.Count != n)
                                throw new DBException("42142").Mix();
                            return (SqlValue)cx.Add(new SqlDefaultConstructor(o.iix, ut, ps));
                        }
                        if (m.methodType != PMethod.MethodType.Constructor)
                            throw new DBException("42142").Mix();
                        var fc = new CallStatement(lp, m, ps);
                        return (SqlValue)cx.Add(new SqlProcedureCall(o.iix, fc));
                    }
                case Sqlx.ROW:
                    {
                        Next();
                        if (Match(Sqlx.LPAREN))
                        {
                            lp = lxr.Position;
                            Next();
                            var v = ParseSqlValueList(rt);
                            Mustbe(Sqlx.RPAREN);
                            return (SqlValue)cx.Add(new SqlRow(lp,v));
                        }
                        throw new DBException("42135", "ROW").Mix();
                    }
                /*       case Sqlx.SELECT:
                           {
                               var sc = new SaveContext(trans, ExecuteStatus.Parse);
                               Query cs = ParseCursorSpecification(t).stmt as Query;
                               sc.Restore(tr);
                               return (SqlValue)cx.Add(new SqlValueSelect(cs, t));
                           } */
                case Sqlx.TABLE: // allowed by 6.39
                    {
                        Next();
                        return (SqlValue)cx.Add(ParseSqlValue(rt));
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
                        DBObject dt = ParseSqlDataType();
                        return (SqlValue)cx.Add(new SqlTreatExpr(lp, v, dt.domain, cx));//.Needs(v);
                    }
                case Sqlx.VALUE:
                    {
                        Next();
                        SqlValue vbl = new SqlValue(lp,"VALUE");
                        return (SqlValue)cx.Add(vbl);
                    }
                case Sqlx.VALUES:
                    {
                        var v = BList<SqlRow>.Empty;
                        Sqlx sep = Sqlx.COMMA;
                        while (sep == Sqlx.COMMA)
                        {
                            Next();
                            var llp = lxr.Position;
                            Mustbe(Sqlx.LPAREN);
                            var x = ParseSqlValueList(rt);
                            Mustbe(Sqlx.RPAREN);
                            v+=new SqlRow(llp,x);
                            sep = tok;
                        }
                        return (SqlValue)cx.Add(new SqlRowArray(lp, v));
                    }
                case Sqlx.LBRACE:
                    {
                        var v = new SqlRow(lp,BTree<long,object>.Empty);
                        Next();
                        if (tok != Sqlx.RBRACE)
                            GetDocItem(v);
                        while (tok==Sqlx.COMMA)
                        {
                            Next();
                            GetDocItem(v);
                        }
                        Mustbe(Sqlx.RBRACE);
                        return (SqlValue)cx.Add(v);
                    }
                case Sqlx.LBRACK:
                        return (SqlValue)cx.Add(ParseSqlDocArray());
                case Sqlx.LSS:
                    return (SqlValue)cx.Add(ParseXmlValue());
            }
            // "SQLFUNCTIONS"
            Sqlx kind;
            SqlValue val = null;
            SqlValue op1 = null;
            SqlValue op2 = null;
            SqlValue filter = null;
            Sqlx mod = Sqlx.NO;
            WindowSpecification window = null;
            Ident windowName = null;
            lp = lxr.Position;
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
                case Sqlx.CASE:
                    {
                        kind = tok;
                        Next();
                        if (tok == Sqlx.WHEN) // searched case
                        {
                            while (tok == Sqlx.WHEN)
                            {
                                kind = tok;
                                Next();
                                var llp = lxr.Position;
                                op1 = ParseSqlValue(Domain.Bool);
                                Mustbe(Sqlx.THEN);
                                val = ParseSqlValue(rt);
                                if (tok == Sqlx.WHEN)
                                    op2 = new SqlFunction(llp, kind,val, op1, op2, mod);
                            }
                            if (tok == Sqlx.ELSE)
                            {
                                Next();
                                op2 = ParseSqlValue(rt);
                            }
                            Mustbe(Sqlx.END);
                            return (SqlValue)cx.Add(new SqlFunction(lp,kind,val,op1,op2,mod));
                        }
                        var val1 = ParseSqlValue(Domain.Content); // simple case
                        if (tok != Sqlx.WHEN)
                            throw new DBException("42129").Mix();
                        while (tok == Sqlx.WHEN)
                        {
                            Next();
                            var llp = lxr.Position;
                            op1 = ParseSqlValue(val1.domain);
                            var c = new List<SqlValue>();
                            while (tok == Sqlx.COMMA)
                            {
                                Next();
                                c.Add(ParseSqlValue(val1.domain));
                            }
                            Mustbe(Sqlx.THEN);
                            val = ParseSqlValue(rt);
                            foreach(var cv in c)
                                op2 = new SqlFunction(llp, Sqlx.WHEN,val,cv,op2,mod);
                        }
                        if (tok == Sqlx.ELSE)
                            op2 = ParseSqlValue(rt);
                        val = val1;
                        Mustbe(Sqlx.END);
                        break;
                    }
                case Sqlx.CAST:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        val = ParseSqlValue(Domain.Content);
                        Mustbe(Sqlx.AS);
                        op1 = new SqlTypeExpr(lp,ParseSqlDataType().domain,cx);
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
                        op1 = ParseSqlValue(rt);
                        Mustbe(Sqlx.COMMA);
                        op2 = ParseSqlValue(rt);
                        while (tok == Sqlx.COMMA)
                        {
                            Next();
                            op1 = new SqlCoalesce(lxr.Position, op1,op2);
                            op2 = ParseSqlValue(rt);
                        }
                        Mustbe(Sqlx.RPAREN);
                        return (SqlValue)cx.Add(new SqlCoalesce(lp, op1, op2));
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
                            Mustbe(Sqlx.WHERE);
                            filter = ParseSqlValue(Domain.Bool);
                            Mustbe(Sqlx.RPAREN);
                        }
                        if (tok == Sqlx.OVER && wfok)
                        {
                            Next();
                            if (tok == Sqlx.ID)
                            {
                                windowName = new Ident(lxr);
                                Next();
                            }
                            else
                            {
                                window = ParseWindowSpecificationDetails();
                                window += (Basis.Name, "U" + db.uid);
                            }
                        }
                        return (SqlValue)cx.Add(new SqlFunction(lp, kind, val, op1, op2, mod, BTree<long,object>.Empty
                            +(SqlFunction.Filter,filter)+(SqlFunction.Window,window)
                            +(SqlFunction.WindowId,windowName)));
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
                        op1 = ParseSqlValue();
                        Mustbe(Sqlx.COMMA);
                        op2 = ParseSqlValue();
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
                        val = (SqlValue)cx.Get(ParseIdentChain());
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
                        var cs = BList<long>.Empty;
                        var ids = ParseIDList();
                        for (var i = 0; i < ids.Length; i++)
                            cs += ids[i].iix;
                        return (SqlValue)cx.Add(new ColumnFunction(lp, cs));
                    }
                case Sqlx.INTERSECT: goto case Sqlx.COUNT;
                case Sqlx.LN: goto case Sqlx.ABS;
                case Sqlx.LOWER: goto case Sqlx.SUBSTRING;
                case Sqlx.MAX: goto case Sqlx.COUNT;
                case Sqlx.MIN: goto case Sqlx.COUNT;
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
                        var pat = ParseSqlValue(Domain.Char);
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue();
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int);
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
                        op1 = ParseSqlValue(Domain.UnionNumeric);
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
                            op1 = ParseSqlValue(Domain.Int);
                            Mustbe(Sqlx.IN);
                            op2 = ParseSqlValue(Domain.Content);
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
                        var pat = ParseSqlValue(Domain.Char);
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue(Domain.Char);
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(rt);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.WITH))
                        {
                            Next();
                            rep.with = ParseSqlValue(Domain.Int);
                        }
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue(Domain.Int);
                        }
                        if (Match(Sqlx.CHARACTERS, Sqlx.OCTETS))
                        {
                            rep.units = tok;
                            Next();
                        }
                        if (Match(Sqlx.OCCURRENCE))
                        {
                            Next();
                            rep.occurrence = ParseSqlValue(Domain.Int);
                        }
                        if (Match(Sqlx.GROUP))
                        {
                            Next();
                            rep.group = ParseSqlValue(Domain.Char);
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
                        lp = lxr.Position;
                        Mustbe(Sqlx.LPAREN);
                        if (tok == Sqlx.RPAREN)
                        {
                            Next();
                            Mustbe(Sqlx.OVER);
                            if (tok == Sqlx.ID)
                            {
                                windowName = new Ident(lxr);
                                Next();
                            }
                            else
                            {
                                window = ParseWindowSpecificationDetails();
                                window+=(Basis.Name,"U"+db.uid);
                            }
                            return (SqlValue)cx.Add(new SqlFunction(lp, kind, val, op1, op2, mod, BTree<long,object>.Empty
                            +(SqlFunction.Filter,filter)+(SqlFunction.Window,window)
                             +(SqlFunction.WindowId,windowName)));
                        }
                        var oi = rt as ObInfo;
                        var v = new BList<SqlValue>(ParseSqlValue(oi?.columns[0]?.domain??
                            Domain.Content));
                        for (var i=1; tok == Sqlx.COMMA;i++)
                        {
                            Next();
                            v += ParseSqlValue(oi?.columns[i]?.domain ??
                            Domain.Content);
                        }
                        Mustbe(Sqlx.RPAREN);
                        val = new SqlRow(lxr.Position, v);
                        var f = new SqlFunction(lp, kind, val, op1, op2, mod, BTree<long, object>.Empty
                            + (SqlFunction.Window, ParseWithinGroupSpecification(lxr.Position))
                            + (SqlFunction.WindowId,"U"+db.uid));
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
                case Sqlx.SET: goto case Sqlx.CARDINALITY;
                case Sqlx.SOME: goto case Sqlx.COUNT;
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
                        var pat = ParseSqlValue();
                        SqlValue flg = null;
                        if (Match(Sqlx.FLAG))
                        {
                            Next();
                            flg = ParseSqlValue();
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(t);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue();
                        }
                        if (Match(Sqlx.CHARACTERS,Sqlx.OCTETS))
                        {
                            rep.units = tok;
                            Next();
                        }
                        if (Match(Sqlx.OCCURRENCE))
                        {
                            Next();
                            rep.occurrence = ParseSqlValue();
                        }
                        if (Match(Sqlx.GROUP))
                        {
                            Next();
                            rep.group = ParseSqlValue();
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
                            flg = ParseSqlValue();
                        }
                        op1 = new RegularExpression(cx, pat, flg, null);
                        Mustbe(Sqlx.IN);
                        val = ParseSqlValue(t);
                        var rep = new RegularExpressionParameters(cx);
                        if (Match(Sqlx.WITH))
                        {
                            Next();
                            rep.with = ParseSqlValue();
                        }
                        if (Match(Sqlx.FROM))
                        {
                            Next();
                            rep.startPos = ParseSqlValue();
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
                                rep.occurrence = ParseSqlValue();
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
                            val = new SqlValueExpr(lxr.Position, Sqlx.XMLCONCAT, 
                                val, ParseSqlValue(Domain.Char), Sqlx.NO);
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
                            var llp = lxr.Position;
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
                                var doc = new SqlRow(llp,BTree<long,object>.Empty);
                                var v = ParseSqlValue(Domain.Char);
                                var j = 0;
                                var a = new Ident("Att"+(++j),0);
                                if (tok == Sqlx.AS)
                                {
                                    Next();
                                    a = new Ident(lxr);
                                    Mustbe(Sqlx.ID);
                                }
                                doc += v+(Basis.Name,a.ident);
                                a = new Ident("Att" + (++j),0);
                                while (tok == Sqlx.COMMA)
                                {
                                    Next();
                                    var w = ParseSqlValue(Domain.Char);
                                    if (tok == Sqlx.AS)
                                    {
                                        Next();
                                        a = new Ident(lxr);
                                        Mustbe(Sqlx.ID);
                                    }
                                }
                                doc += v + (Basis.Name, a.ident);
                                v = new SqlValueExpr(lp, Sqlx.XMLATTRIBUTES, v, null, Sqlx.NO);
                                Mustbe(Sqlx.RPAREN);
                                op2 = v;
                            }
                            else
                            {
                                val = new SqlValueExpr(lp, Sqlx.XML, val, ParseSqlValue(Domain.Char), Sqlx.NO);
                                n++;
                            }
                        }
                        Mustbe(Sqlx.RPAREN);
                        op1 = new SqlLiteral(lxr.Position,name);
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
                            var llp = lxr.Position;
                            Next();
                            val = new SqlValueExpr(llp, Sqlx.XMLCONCAT, val, ParseSqlValue(Domain.Char), Sqlx.NO);
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
                        op2 = new SqlLiteral(lxr.Position, new TChar(lxr.val.ToString()));
                        Next();
                        break;
                    }
                case Sqlx.XMLPI:
                    {
                        kind = tok;
                        Next();
                        Mustbe(Sqlx.LPAREN);
                        Mustbe(Sqlx.NAME);
                        val = new SqlLiteral(lxr.Position, new TChar( lxr.val.ToString()));
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
                    return (SqlValue)cx.Add(new SqlProcedureCall(lp,
                        (CallStatement)ParseProcedureCall(rt)));
            }
            return (SqlValue)cx.Add(new SqlFunction(lp, kind, val, op1, op2, mod));
        }

        /// <summary>
        /// WithinGroup = WITHIN GROUP '(' OrderByClause ')' .
        /// </summary>
        /// <returns>A WindowSpecification</returns>
        WindowSpecification ParseWithinGroupSpecification(long t)
        {
            WindowSpecification r = new WindowSpecification(t);
            Mustbe(Sqlx.WITHIN);
            Mustbe(Sqlx.GROUP);
            Mustbe(Sqlx.LPAREN);
            r+=(WindowSpecification.Order,OrderSpec.Empty
                +(OrderSpec.Items,ParseOrderClause(r.order.items,t,false)));
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
            var pn = new XmlNameSpaces(lxr.Position);
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
			WindowSpecification w = new WindowSpecification(lxr.Position);
            if (tok == Sqlx.ID)
            {
                w+=(WindowSpecification.OrderWindow,lxr.val.ToString());
                Next();
            }
            var oi = BList<SqlValue>.Empty;
            var pt = 0;
			if (tok==Sqlx.PARTITION)
			{
				Next();
				Mustbe(Sqlx.BY);
                oi = ParseSqlValueList(Domain.Char);
                pt = (int)oi.Count;
                w += (WindowSpecification.Order, OrderSpec.Empty + (OrderSpec.Items, oi));
                w += (WindowSpecification.Partition, pt);
			}
			if (tok==Sqlx.ORDER)
				w +=(WindowSpecification.Order,OrderSpec.Empty
                    +(OrderSpec.Items,ParseOrderClause(BList<SqlValue>.Empty,w.defpos,false)));
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
			return w;
		}
        /// <summary>
		/// WindowBound = WindowStart | ((TypedValue | UNBOUNDED) FOLLOWING ) .
        /// </summary>
        /// <returns>The WindowBound</returns>
        WindowBound ParseWindowBound()
        {
            bool prec = false,unbd = true;
            Domain tp = Domain.Int;
            TypedValue d = null;
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
                var lp = lxr.Position;
                Mustbe(Sqlx.CHAR);
                Domain di = ParseIntervalType();
                d = di.Parse(new Scanner(lp,o.ToString().ToCharArray(),0));
                tp = di;
                unbd = false;
            }
            else
            {
                d = lxr.val;
                Sqlx tk = Mustbe(Sqlx.INTEGERLITERAL, Sqlx.NUMERICLITERAL);
                tp = Domain.Predefined(tk);
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
        /// For the REST service, we can have a value, maybe a procedure call:
        /// </summary>
        /// <param name="sql">an expression string to parse</param>
        /// <returns>the SqlValue</returns>
        internal SqlValue ParseSqlValueItem(string sql,DBObject rt)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid + sql.Length); // not really needed here
            return ParseSqlValueItem(rt,false);
        }
        /// <summary>
        /// For the REST service there may be an explicit procedure call
        /// </summary>
        /// <param name="sql">a call statement to parse</param>
        /// <returns>the CallStatement</returns>
        internal CallStatement ParseProcedureCall(string sql,DBObject ob)
        {
            lxr = new Lexer(sql); 
            tok = lxr.tok;
            if (lxr.Position > Transaction.TransPos)  // if sce is uncommitted, we need to make space above nextTid
                db += (Database.NextTid, db.nextTid +sql.Length); // not really needed here
            var n = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var ps = BList<SqlValue>.Empty;
            var lp = lxr.Position;
            if (tok == Sqlx.LPAREN)
            {
                Next();
                ps = ParseSqlValueList(Domain.Content);
            }
            var arity = (int)ps.Count;
            Mustbe(Sqlx.RPAREN);
            var pp = db.role.procedures[n.ident]?[arity] ?? -1;
            var pr = db.objects[pp] as Procedure
                ?? throw new DBException("42108", n).Mix();
            var fc = new CallStatement(lp, pr, ps);
            return (CallStatement)cx.Add(fc);
        }
        /// <summary>
		/// UserFunctionCall = Id '(' [  TypedValue {','  TypedValue}] ')' .
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>the Executable</returns>
        Executable ParseProcedureCall(DBObject ob)
        {
            var id = new Ident(lxr);
            Mustbe(Sqlx.ID);
            var lp = lxr.Position;
            Mustbe(Sqlx.LPAREN);
            var ps = ParseSqlValueList(Domain.Content);
            var a = (int)ps.Count;
            Mustbe(Sqlx.RPAREN);
            var pp = db.role.procedures[id.ident]?[a] ??
                throw new DBException("42108", id.ident).Mix();
            var pr = (Procedure)db.objects[pp];
            var fc = new CallStatement(lp, pr, ps);
            return (Executable)cx.Add(fc);
        }
        /// <summary>
        /// Parse a list of Sql values
        /// </summary>
        /// <param name="t">the expected data type</param>
        /// <returns>the List of SqlValue</returns>
        BList<SqlValue> ParseSqlValueList(DBObject rt)
        {
            var r = BList<SqlValue>.Empty;
            for (; ;)
            {
                var v = ParseSqlValue(rt);
                if (tok == Sqlx.AS)
                {
                    Next();
                    var d = ParseSqlDataType().domain;
                    v = new SqlTreatExpr(lxr.Position, v, d, cx); //.Needs(v);
                    cx.Add(v);
                }
                r += v;
                if (tok == Sqlx.COMMA)
                    Next();
                else
                    break;
            }
            return r;
        }
        public SqlRow ParseSqlRow(DBObject rt)
        {
            Mustbe(Sqlx.LPAREN);
            var lk = ParseSqlValueList(rt as ObInfo ??
                 throw new DBException("42000", lxr.pos));
            Mustbe(Sqlx.RPAREN);
            return (SqlRow)cx.Add(new SqlRow(lxr.Position, lk));
        }
        /// <summary>
        /// Parse an SqlRow
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="sql">The string to parse</param>
        /// <param name="result">the expected data type</param>
        /// <returns>the SqlRow</returns>
        public SqlValue ParseSqlValueList(string sql,DBObject rt)
        {
            lxr = new Lexer(sql);
            tok = lxr.tok;
            if (tok == Sqlx.LPAREN)
                return ParseSqlRow(rt);         
            return ParseSqlValueEntry(rt,false);
        }
        /// <summary>
        /// Get a document item
        /// </summary>
        /// <param name="v">The document being constructed</param>
        SqlRow GetDocItem(SqlRow v)
        {
            Ident k = new Ident(lxr);
            Mustbe(Sqlx.ID);
            Mustbe(Sqlx.COLON);
            return v + (ParseSqlValue(Domain.Content,v.defpos)+(Basis.Name,k.ident));
        }
        /// <summary>
        /// Parse a document array
        /// </summary>
        /// <returns>the SqlDocArray</returns>
        public SqlValue ParseSqlDocArray()
        {
            var v = new SqlRowArray(lxr.Position,BTree<long,object>.Empty);
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
            var v = new SqlXmlValue(lxr.Position,new BTree<long,object>(SqlXmlValue.Element,e));
            cx.Add(v);
            while (tok!=Sqlx.GTR && tok!=Sqlx.DIVIDE)
            {
                var a = GetName();
                v+=(SqlXmlValue.Attrs,v.attrs+(a, ParseSqlValue(Domain.Char)));
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
                        v+=(SqlXmlValue.Content,new SqlLiteral(lxr.Position, 
                            new TChar(new string(lxr.input, st, lxr.start - st))));
                    }
                    else
                    {
                        lxr.PushBack(Sqlx.ANY);
                        lxr.pos = lxr.start;
                        v += (SqlXmlValue.Content, ParseSqlValueItem(Domain.Char,false));
                    }
                }
                else
                    while (tok != Sqlx.DIVIDE) // tok should Sqlx.LSS
                        v+=(SqlXmlValue.Children,v.children+((SqlXmlValue)ParseXmlValue()));
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
                e.prefix = e.keyname;
                e.keyname = new string(lxr.input, lxr.start, lxr.pos - lxr.start);
                Mustbe(Sqlx.ID);
            }
            return e;
        }
        BTree<long,bool> _Deps(BList<Executable> bl)
        {
            var r = BTree<long, bool>.Empty;
            for (var b = bl.First(); b != null; b = b.Next())
                r += (b.value().defpos, true);
            return r;
        }
    }
}