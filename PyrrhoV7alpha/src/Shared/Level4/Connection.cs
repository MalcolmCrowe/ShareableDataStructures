using System;
using System.Text;
using System.Net.Sockets;
using System.Collections.Generic;
using System.Threading.Tasks;
#if WINDOWS && !OSP
using System.Security.Principal;
#endif
using PyrrhoBase;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.IO;
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
    /// Connection maintains a list of connection strings, to enable Transaction to build
    /// its list of databases. Note that Transaction is a subclass of Connection.
    /// We ensure that all of the databases mentioned are in the server's list of databses
    /// (including remote databases!). User and password information is used to create a
    /// new database or connect to a remote database that is not configured.
    /// </summary>
    internal class Connection : IConnection
    {
        internal class Results
        {
            internal RowSet rowSet;
            internal RowBookmark bmk = null;
            internal ATree<string, Activation> acts;
            internal string name;
            internal string[] names;
        }
        internal Context context;
        public Transaction icontext {  get { return context; } set { context = value as Context; } }
        internal Database db;
        internal string user,role;
        internal bool authenticated = true;
        /// <summary>
        /// Support for iterate and breaks
        /// </summary>
        internal Activation breakto = null;
        internal Evaluation _authority = null;
        /// <summary>
        /// CaseSensitive is false for SQL2003.
        /// true for Java.
        /// </summary>
        public bool caseSensitiveIds = false;
        /// <summary>
        /// Standard SQL diagnostics area
        /// </summary>
        internal ATree<Sqlx, TypedValue> diagnostics = BTree<Sqlx, TypedValue>.Empty;
        /// <summary>
        /// the parsing state: Parse, Obey, Drop, Rename
        /// </summary>
        internal ExecuteStatus parse = ExecuteStatus.Obey; // set to Parse e.g. by ParseCreateClause
        /// <summary>
        /// Transaction stats for this connection
        /// </summary>
        internal int commits = 0, rollbacks = 0;
        /// <summary>
        /// Try to track the rows affected by a transaction (includes triggers etc)
        /// </summary>
        internal List<Rvv> affected = new List<Rvv>();
        /// <summary>
        /// rowCount does not include triggers: just the rows directly affected
        /// </summary>
        internal int rowCount = 0;
        /// <summary>
        /// The current activation stack
        /// </summary>
        internal ATree<string, Activation> stack = BTree<string, Activation>.Empty;
        /// <summary>
        /// We may be monitoring transaction conflict information
        /// </summary>
        internal TransactionProfile profile = null;
        /// <summary>
        /// Index of remote transaction coordinator in connectionList if >=0
        /// </summary>
        internal int coordinator = -1;
        internal List<Lock> locks = new List<Lock>();
        /// <summary>
        /// Record the transaction results here
        /// </summary>
        internal Results result = null;
        internal bool lookAheadDone = true, more = true;
        internal List<DBException> warnings = new List<DBException>();
        internal ATree<long, bool> strategyDone = BTree<long, bool>.Empty;
        static long _cid = 0;
        internal readonly long cid = ++_cid;
        /// <summary>
        /// 5.0 Improved interactive error handling
        /// </summary>
        internal Ident.Tree<Participant> mark = Ident.Tree<Participant>.Empty;
        /// <summary>
        /// Constructor: a new single database connection, eg embedded, configuration, REST.
        /// </summary>
        /// <param name="n">The database name</param>
        internal Connection (Ident n) 
        {
            db = Database.databases?[n];
        }
        /// <summary>
        /// Constructor: called for a new Transaction
        /// </summary>
        /// <param name="c">The connection</param>
        protected Connection(Connection c)
        {
            db = c.db; // snapshot
            user = c.user;
            authenticated = c.authenticated;
            affected = c.affected;
            commits = c.commits;
            rollbacks = c.rollbacks;
            context = c.context;
            profile = c.profile;
            coordinator = c.coordinator;
            result = c.result;
        }
        internal Activation GetActivation()
        {
            for (var c = context; c != null; c = c.staticLink)
                if (c is Activation ac)
                    return ac;
            return null;
        }
        internal Query GetQueryContext()
        {
            if (context.cur is Query cr)
                return cr;
            for (var c = context; c != null; c = c.staticLink)
                if (c is Query q)
                    return q;
            return result?.rowSet.qry;
        }
        internal RowSet GetRowSet()
        {
            if ((result?.rowSet ?? (context?.cur??context as Query)?.rowSet) is RowSet r)
                return r;
            for (var c = context; c != null; c = c.staticLink)
                if (c is Query q && q.rowSet is RowSet rs)
                    return rs;
            return null;
        }
        internal Participant Mark()
        {
            return db.Mark();
        }
        internal virtual void Restore(Participant mark)
        {
            db = mark;
        }
        internal virtual Database Db(int i)
        {
            throw new PEException("PE001");
        }
        internal virtual int DbCount { get => 0; }
        /// <summary>
        /// Add an entry for the SQL standard diagnostics area
        /// </summary>
        /// <param name="w">A diagnostic identifier</param>
        /// <param name="v">A value</param>
        internal void Put(Sqlx w, TypedValue t)
        {
            ATree<Sqlx, TypedValue>.Add(ref diagnostics, w, t);
        }
        /// <summary>
        /// Retrieve an SQL standard diagnostics entry
        /// </summary>
        /// <param name="w">A diagnostic identifier</param>
        /// <returns>the value found or computed</returns>
        internal virtual TypedValue Get(Sqlx w)
        {
            switch (w) // deal with some special cases
            {
                case Sqlx.MESSAGE_LENGTH:
                    if (diagnostics[Sqlx.MESSAGE_TEXT] == null)
                        return new TInt(0);
                    return new TInt(diagnostics[Sqlx.MESSAGE_TEXT].ToString().Length);
                case Sqlx.MESSAGE_OCTET_LENGTH:
                    if (diagnostics[Sqlx.MESSAGE_TEXT] == null)
                        return new TInt(0);
                    return new TInt(System.Text.Encoding.UTF8.GetByteCount(diagnostics[Sqlx.MESSAGE_TEXT].ToString()));
            }
            return diagnostics[w];
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
        /// <summary>
        /// Construct a Warning
        /// </summary>
        /// <param name="sig">the signal</param>
        /// <param name="obs">extra information</param>
        public void Warning(string sig, params object[] obs)
        {
            warnings.Add(Exception(sig, obs));
        }
        /// <summary>
        /// Find the topmost activation with a given blockid
        /// </summary>
        /// <param name="hd">The desired headlabel</param>
        /// <returns>The topmost Activation with this blockid, or null if not found</returns>
        internal Activation FindActivation(string blockid)
        {
            if (blockid == null)
                return null;
            for (var c = context; c != null; c = c.staticLink ?? (c as Activation)?.dynLink)
                if (c is Activation a && a.blockid == blockid)
                    return a;
            return null;
        }
        /// <summary>
        /// Get the current role
        /// </summary>
        /// <param name="db">the database</param>
        /// <returns>the defpos of the current role or -1</returns>
        internal virtual Role _Role(Database db)
        {
            var rp = _authority?.GetRole(db) ?? -1;
            return db.GetObject(rp) as Role ?? throw new PEException("PE002");
        }
        internal virtual User _User(Database db)
        {
            var rp = _authority?.GetUser(db) ?? -1;
            return db.GetObject(rp) as User ?? throw new PEException("PE002");
        }
        internal virtual Role _Role(int dbix)
        {
            throw new NotImplementedException();
        }
        internal virtual User _User(int dbix)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Set the given user and role (this is in a stack)
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="role">The role</param>
        /// <param name="user">The user</param>
        /// <returns>The previous Authority (for use with PopTo)</returns>
        internal Evaluation PushRole(Database db, long role, long user)
        {
            var r = _authority;
            _authority = new Evaluation(db.name, role, user, db.dbix, _authority);
            return r;
        }
        /// <summary>
        /// Restore a previous Authority i.e. user and role (from PushRole)
        /// </summary>
        /// <param name="a">The Authority to restore</param>
        internal void PopTo(Evaluation a)
        {
            if (a == null || _authority == null)
                throw new PEException("unreasonable authority state");
            _authority = _authority.PopTo(a);
        }
        /// <summary>
        /// safely compare two TypedValues for equality.
        /// (They should really use TNull.Value and never be null) 
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        internal bool Match(TypedValue a, TypedValue b)
        {
            return (a == null && b == null) || !(a == null || a.CompareTo(this, b) != 0);
        }
        /// <summary>
        /// Lowest-level Pyrrho protocol has a concept of primitive type
        /// </summary>
        /// <param name="kind">One of the primitive types</param>
        /// <returns>The Typecode used in the data file</returns>
        internal static int Typecode(Sqlx kind)
        {
            switch (kind)
            {
                case Sqlx.NULL: return 0;
                case Sqlx.INTEGER: return 1;
                case Sqlx.NUMERIC: return 2;
                case Sqlx.REAL: return 8;
                case Sqlx.NCHAR: return 3;
                case Sqlx.LEVEL:
                case Sqlx.CHAR: return 3;
                case Sqlx.DOCUMENT: return 5;
                case Sqlx.DOCARRAY: return 5;
                case Sqlx.OBJECT: return 3;
                case Sqlx.PASSWORD: return 3;
                case Sqlx.TIMESTAMP: return 4;
                case Sqlx.DATE: return 13;
                case Sqlx.BLOB: return 5;
                case Sqlx.REF:
                case Sqlx.ROW: return 6;
                case Sqlx.ARRAY: return 7;
                case Sqlx.MULTISET: return 15;
                case Sqlx.TABLE: return 14;
                case Sqlx.TYPE: return 12;
                case Sqlx.BOOLEAN: return 9;
                case Sqlx.INTERVAL: return 10;
                case Sqlx.TIME: return 11;
                case Sqlx.UNION: return 1; // only happens with INTEGER
                case Sqlx.XML: return 3;
            }
            return 0;
        }
        /// <summary>
        /// Execute a Select statement
        /// </summary>
        /// <param name="s">the CursorSpecification</param>
        internal virtual RowSet Execute(CursorSpecification s, bool result)
        {
            switch (parse)
            {
                case ExecuteStatus.Parse: break;
                case ExecuteStatus.Obey:
                    s.Analyse(this);
                    return s.rowSet;
            }
            return null;
        }
        internal void SetResults(RowSet r)
        {
            if (r == null || (result != null && result.acts.Count < stack.Count))
                return;
            result = new Results()
            {
                rowSet = r,
                acts = stack,
                name = (r.qry is From fm) ? fm.target?.NameInSession(r.tr.Db(fm.target.dbix))?.ToString() : "",
                names = new string[r.rowType.Length]
            };
            var ns = r.rowType.names;
            for (var i = 0; i < ns.Length; i++)
            {
                var id = ns[i];
                if ((Char.IsDigit(id.ident[0])||id.ident[0]=='\'') && Database.Get(r.tr,id) is Database d &&
                    d.GetObject(id.Defpos()) is DBObject ob)
                    id = ob.NameInSession(d);
                result.names[i] = id.ToString();
            }
        }
        /// <summary>
        /// Allow client to reset a DataReader
        /// </summary>
        internal void ResetReader()
        {
            result.bmk=result.rowSet.First();
        }
        /// <summary>
        /// A Secondary connection is to a fixed physbase image
        /// </summary>
        public bool secondary = false;
        /// <summary>
        /// Obey the given command (called from various places)
        /// </summary>
        /// <param name="cmd">The command to execute</param>
        /// <returns>The transaction with its results</returns>
        internal Connection Execute(string sql)
        {
            var tr = RequireTransaction();
            // clear the context
            if (tr.context is Context cx)
            {
                cx.contexts = BTree<string, Context>.Empty;
                cx.defs = Ident.Tree<SqlValue>.Empty;
                cx.lookup = BTree<Ident, ATree<int, ATree<long, SqlValue>>>.Empty;
                cx.cur = null;
            }
            // and now proceed
            new Parser(tr).ParseSql(sql);
            if (tr.result != null)
            {
                tr.stack = tr.result.rowSet.qry.acts;
#if !EMBEDDED
                if (PyrrhoStart.StrategyMode)
                    tr.ShowStrategy();
#endif
                var b = tr.result.rowSet.First();// don't ask me why but combining these two lines into a simple assignment gives null
                tr.result.bmk = b;
            } 
            result = tr.result;
            return tr;
        }
        internal virtual void Execute(Executable e)
        {
            if (parse != ExecuteStatus.Obey)
                return;
            Activation a = new Activation(this, null);
            a.Push(this);
            try
            {
                e.Obey(this); // Obey must not call the Parser!
                if (a.signal != null)
                {
                    var ex = Exception(a.signal.signal, a.signal.objects);
                    for (var s = a.signal.setlist.First(); s != null; s = s.Next())
                        ex.Add(s.key(), s.value().Eval(this,null));
                    throw ex;
                }
            }
            catch (Exception ex) { throw ex; }
            finally { a.Pop(this); }
        }
        /// <summary>
        /// For REST service: do what we should according to the path, mime type and posted data
        /// </summary>
        /// <param name="method">GET/PUT/POST/DELETE</param>
        /// <param name="path">The URL</param>
        /// <param name="mime">The mime type in the header</param>
        /// <param name="data">The posted data if any</param>
        internal Connection Execute(string method, string id,string[] path, string mime, string data, string etag)
        {
            var tr = RequireTransaction();
            var db = tr.db;
            var ro = db.GetRole(db.defRole);
            if (etag != null)
            {
                var ss = etag.Split(';');
                if (ss.Length > 1)
                    db.CheckRdC(ss[1]);
            }
            var au = PushRole(db, ro.defpos, -1);
            if (path.Length > 2)
            {
                switch (method)
                {
                    case "GET":
                        db.Execute(ro, id+".",path, 2, etag);
                        break;
                    case "DELETE":
                        db.Execute(ro, id+".", path, 2, etag);
                        db.Delete(tr.result.rowSet);
                        break;
                    case "PUT":
                        db.Execute(ro, id+".",path, 2, etag);
                        db.Put(tr.result.rowSet, data);
                        var rvr = tr.result.rowSet as RvvRowSet;
                        tr.SetResults(rvr._rs);
                        break;
                    case "POST":
                        db.Execute(ro, id+".", path, 2, etag);
                        tr.stack = tr.result?.acts ?? BTree<string, Activation>.Empty;
                        db.Post(tr.result?.rowSet, data);
                        break;
                }
            }
            else
            {
                switch (method)
                {
                    case "GET":
                        var f = new From(tr, id+"G", new Ident("Role$Class", 0,Ident.IDType.NoInput), null, Grant.Privilege.Select);
                        f.Analyse(tr);
                        SetResults(f.rowSet);
                        break;
                    case "POST":
                        new Parser(tr).ParseProcedureStatement(data);
                        break;
                }
                tr.result = result;
            }
            PopTo(au);
            return tr;
        }
        internal void AddContext(string k, Context c)
        {
            if (k != null && k != "")
                ATree<string, Context>.Add(ref context.contexts, k, c);
        }
        internal void AddContext(Query q)
        {
            AddContext(q.alias?.ident, q);
            AddContext(q.blockid, q);
            for (var b = q.contexts.First(); b != null; b = b.Next())
                AddContext(b.key(), b.value());
        }
        internal Context Ctx(string blockid)
        {
            return stack[blockid] ?? context.contexts[blockid] ??
                                ((context.blockid == blockid) ? context :
                (context.cur is Query q && q.blockid == blockid) ? context.cur : null);
        }
        /// <summary>
        /// The current value of an Ident.
        /// This method is used when name comes from a Type definition
        /// and so name does not specify a context.
        /// It will return null if the Ident has not been computed yet.
        /// </summary>
        /// <param name="tr">The connection</param>
        /// <param name="name">The Ident to find</param>
        /// <returns>The value found</returns>
        internal TypedValue Eval(Ident name)
        {
            if (result?.rowSet.qry.row?.Get(name)?.NotNull() is TypedValue tv)
                return tv;
            return (context.cur ?? context).Lookup(this, name).Eval(this, GetRowSet());
        }
#if !EMBEDDED && !LOCAL
        /// <summary>
        /// Get a DumpTable cursor
        /// </summary>
        /// <param name="tbdefpos">The table to dump</param>
        /// <param name="match">A set of equality conditions</param>
        internal long Execute(long tbdefpos, ATree<long,  TypedValue> match)
        {
            var tr = this as Transaction;
            var db = tr.db as Participant;
            return db.DumpTableCursor(tbdefpos, match);
        }
        /// <summary>
        /// PartitionedIndex lookup
        /// </summary>
        /// <param name="indexpos"></param>
        /// <param name="data"></param>
        internal long Execute(ATree<long,  TypedValue> match, long indexpos)
        {
            var tr = this as Transaction;
            var db = tr.db as Participant;
            return db.PartitionedIndexCursor(indexpos, match);
        }
#endif
        /// <summary>
        /// Start a transaction (from client), join a transaction (from server)
        /// </summary>
        /// <returns>The Transaction</returns>
        internal virtual Connection BeginTransaction()
        {
            var tr = new Transaction(this)
            {
                autoCommit = false
            };
            //          Console.WriteLine("Explicit transaction " + tr.tid);
            return tr;
        }
        /// <summary>
        /// Commit a transaction
        /// </summary>
        /// <returns>The underlying connection</returns>
        internal virtual Connection Commit()
        {
            return this;
        }
        /// <summary>
        /// Roll back a transaction
        /// </summary>
        /// <param name="ex">The exception condition</param>
        /// <returns>The underlying connection</returns>
        internal virtual Connection Rollback(DBException ex)
        {
            return this;
        }
        /// <summary>
        /// Ensure we have a Transaction
        /// </summary>
        /// <returns>The transaction</returns>
        internal virtual Transaction RequireTransaction()
        {
            affected.Clear();
            return new Transaction(this);
        }
        internal virtual Domain GetDomain(Ident name,out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Domain GetDomain(Domain dt, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Table GetTable(Ident name, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Index GetIndex(Ident name, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual View GetView(Ident name, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Procedure GetProcedure(Ident nameAndArity, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Method GetMethod(Ident tname, Ident mname, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Trigger GetTrigger(Ident name, out Database db)
        {
            throw new PEException("PE001");
        }
        internal virtual Database DbFor(Domain dt)
        {
            throw new PEException("PE001");
        }
#if !EMBEDDED && !LOCAL
        /// <summary>
        /// Connect up the REST service
        /// </summary>
        /// <param name="host">The hostname</param>
        /// <param name="port">The TCP port</param>
        /// <param name="db">The database name</param>
        /// <param name="us">The user name</param>
        /// <param name="pw">The password</param>
        /// <param name="rl">The role</param>
        /// <returns></returns>
        internal Connection ConnectionFor(string host, int port, string db, string us, string pw, string rl)
        {
            var sv = host + ":" + port;
            foreach (var cc in connectionList)
                if (cc.remoteServer == sv && cc.Name == db)
                {
                    if (cc.user != us)
                        throw new DBException("2E105", db).Mix();
                    return cc;
                }
            var c = new Connection(this, db, sv, ServerRole.Undefined, us, pw, rl);
            try
            {
                c.Open();
                var asy = c.Async as AsyncStream;
                var lk = asy.GetLock();
                lock (lk)
                {
                    lk.OnLock(false, "ConnectionFor", this);
                    asy.Write(Protocol.GetMaster);
                    switch (asy.ReadResponse())
                    {
                        default: c = null; break;
                        case Responses.NoMaster: break;
                        case Responses.Master:
                            asy.GetString();
                            asy.GetString();
                            asy.GetString();
                            break;
                    }
                    lk.Unlock(true);
                }
                return c;
            }
            catch (Exception)
            {
                return null;
            }
        }
#endif
#if MONGO
#if !EMBEDDED
        /// <summary>
        /// For MongoDB, the collection name may be the name of a database,
        /// or have .$cmd appended. 
        /// Here we ensure that we place the database at databases[0], before we
        /// start a transaction. Otherwise we get our bearings by finding or
        /// creating suitable table and column for the service.
        /// </summary>
        /// <param name="databaseNameAndSuffix"></param>
        /// <returns></returns>
        internal void MongoConnect(MongoService ms, string databaseNameAndSuffix)
        {
            Console.WriteLine("For Collection " + databaseNameAndSuffix);
            var ss = databaseNameAndSuffix.Split('.');
            var ssn = ss.Length;
            var cmdmode = false;
            if (ssn > 0 && ss[ssn - 1] == "$cmd")
            {
                cmdmode = true;
                ssn--;
            }
            if (ssn == 0)
                throw new DBException("3D001", "not specified");
            // first see if we have the database and move it to position 0
            int ix;
            for (ix = 0; ix < connectionList.Count; ix++)
                if (connectionList[ix].name == ss[0])
                    break;
            if (ix < connectionList.Count)
            {
                if (ix != 0)
                {
                    var t = connectionList[0];
                    connectionList[0] = connectionList[ix];
                    connectionList[ix] = t;
                }
            }
            else
            {
                var cs = new ConnectedConfiguration(this, ss[0], ss[0], BTree<string, string>.Empty);
                cs.user = user;
                if (connectionList.Count > 0)
                {
                    connectionList.Add(connectionList[0]);
                    connectionList[0] = cs;
                }
                else
                    connectionList.Add(cs);
                var dfm = "true";
                cs.Open(ref dfm);
            }
            ms.tr = new Transaction(this);
            ms.ctx.transaction = ms.tr;
            // see what table we will be using
            ms.tabname = "tbl";
            if (ssn > 1)
                ms.tabname = ss[1];
            // see what column we will be using
            ms.colname = "doc";
            ms.tr.mongocmd = cmdmode;
        }
#endif
#endif
        /// <summary>
        /// Close a DataReader
        /// </summary>
        internal virtual Connection RdrClose()
        {
            result = null;
            context = null;
            affected.Clear();
            return this;
        }
        /// <summary>
        /// Check for Transaction conflicts
        /// </summary>
        /// <value>Whether a conflict has occurred</value>
        internal virtual bool Conflict { get { return false; } }
         public void Close()
        {
#if !LOCAL && !EMBEDDED
            if (PyrrhoStart.DebugMode)
                Console.WriteLine("Closing connection " + cid);
            foreach( var cd in connectionList)
                if (cd._async!=null)
                {
                    for (var b = Push.pushConnections.First(); b != null; b = b.Next())
                        if (b.value() == cd._async)
                            goto skip;
                    cd.CloseStream();
                    skip:;
                }
#endif
        }

        public string Blockid()
        {
            return "B"+ ++_cid;
        }
        void ShowStrategy()
        {
            var sb = new StringBuilder();
            sb.Append("Transaction ");
            sb.Append(cid);
            result.rowSet.qry.ToString1(sb, this);
            Console.WriteLine(sb.ToString());
            result.rowSet.Strategy(0);
        }
    }
    /// <summary>
    /// Lightweight IConnection for Level 2 parsing: create when needed
    /// </summary>
    internal class SingleConnection : Connection
    {
        internal Database database;
        public SingleConnection(Database db) :base(db.name)
        {
            database = db;
            PushRole(db, -1, -1);
        }
        internal override Database Db(int ix)
        {
            return database;
        }
        internal override Role _Role(Database db)
        {
            return database.GetObject(_authority.role) as Role;
        }
        internal override User _User(Database db)
        {
            return database.GetObject(_authority.user) as User;
        }
        internal override Role _Role(int ix)
        {
            return database.GetObject(_authority.role) as Role;
        }
        internal override User _User(int ix)
        {
            return database.GetObject(_authority.user) as User;
        }
        internal override int DbCount { get => 1; }
        internal override Domain GetDomain(Ident name, out Database db)
        {
            db = database;
            return database.GetDomain(name);
        }
        internal override Domain GetDomain(Domain dt, out Database db)
        {
            db = database;
            return database.GetDomain(dt);
        }
        internal override Table GetTable(Ident name, out Database db)
        {
            db = database;
            return database.GetTable(name);
        }
        internal override Index GetIndex(Ident name, out Database db)
        {
            db = database;
            return database.GetIndex(name);
        }
        internal override View GetView(Ident name, out Database db)
        {
            db = database;
            return database.GetView(name);
        }
        internal override Procedure GetProcedure(Ident nameAndArity, out Database db)
        {
            db = database;
            return database.GetProcedure(nameAndArity);
        }
        internal override Method GetMethod(Ident tname, Ident mname, out Database db)
        {
            db = database;
            return database.GetMethod(tname, mname);
        }
        internal override Trigger GetTrigger(Ident name, out Database db)
        {
            db = database;
            return database.GetTrigger(name);
        }
        internal override Database DbFor(Domain dt)
        {
            return database;
        }
    }
}