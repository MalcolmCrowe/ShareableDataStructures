using System.Text;
using System.Net;
using System.Net.Sockets;
using Pyrrho.Level2; // for Record
using Pyrrho.Level3; // for Database
using Pyrrho.Level4; // for Select
using Pyrrho.Level1; // for DataFile option
using Pyrrho.Common;
using System.Globalization;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho
{
    internal enum ServerStatus { Open, Store, Master, Server };
    /// <summary>
    /// The Pyrrho DBMS Server process: deals with a single connection from a client
    /// and exits when the connection is closed. 
    /// There is a private PyrrhoServer instance for each thread.
    /// Communication is by asynchronous TCP transport, from a PyrrhoLink client or another server. 
    /// For HTTP communication see HttpService.cs
    /// </summary>
    internal class PyrrhoServer
    {
        /// <summary>
        /// The client socket
        /// </summary>
        readonly Socket client;
        /// <summary>
        /// Contection: connection string details, prepared statements
        /// </summary>
        readonly Connection conn;
        /// <summary>
        /// the Pyrrho protocol stream for this connection
        /// </summary>
		internal readonly TCPStream tcp;
        /// <summary>
        /// the database for this connection
        /// </summary>
        Database? db = null;
        /// <summary>
        /// An internal unique identifier for this connection
        /// </summary>
        static int _cid = 0;
        readonly int cid = _cid++;        /// <summary>
        /// Remaining local variables are volatile within protocol steps
        /// </summary>
        Context? cx = null;
        Cursor? rb = null;
        internal bool lookAheadDone = true, more = true;
        private int nextCol = 0;
        private TypedValue? nextCell = null;
        /// <summary>
        /// Constructor: called on Accept
        /// </summary>
        /// <param name="c">the newly connected Client socket</param>
		public PyrrhoServer(Socket c)
        {
            client = c;
            tcp = new() { client = client };
            conn = new Connection(tcp,GetConnectionString(tcp));
        }
        /// <summary>
        /// The main routine started in the thread for this client. This contains a protcol loop
        /// </summary>
        public void Server()
        {
            // process the connection string
            var fn = conn.props["Files"];
            var user = conn.props["User"];
            var log = PyrrhoStart.validationLog;
            Thread.CurrentThread.CurrentUICulture = new CultureInfo(conn.props["Locale"]??"");
            int p = -1;
            bool recovering = false;
            try
            {
                if (fn == null || user==null)
                    throw new DBException("2E208");
                db = Database.Get(conn.props);
                if (db == null)
                {
                    var fp = PyrrhoStart.path + fn;
                    if (!File.Exists(fp))
                    {
                        var fs = new FileStream(fp,
                        FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                        var wr = new Writer(Context._system, fs);
                        wr.PutInt(777);
                        wr.PutInt(52); // v7.02 and greater: format is 52
                        wr.PutBuf();
                        if (user != Environment.UserDomainName + "\\" + Environment.UserName)
                        {
                            db = new Database(fn, fp, fs);
                            var _cx = new Context(db, conn);
                            wr = new Writer(_cx, db.df);
                            new PRole(fn, "Default Role", wr.Length, _cx).Commit(wr, null);
                            new PUser(user, wr.Length, _cx).Commit(wr, null);
                            wr.PutBuf();
                        }
                        if (PyrrhoStart.VerboseMode)
                            Console.WriteLine("Database file created " + fp);
                        fs.Close();
                    }
                    db = new Database(fn, fp, new FileStream(fp,
                        FileMode.Open, FileAccess.ReadWrite, FileShare.None));
                    if (PyrrhoStart.VerboseMode && db.role is not null)
                        Console.WriteLine("Server " + cid + " " + user
                            + " " + fn + " " + db.role.name);
                    db = db.Load();
                }
                cx = new Context(db,conn);
                tcp.Write(Responses.Primary);
                tcp.Flush();
            }
            catch (DBException e)
            {
                try
                {
                    tcp.StartException();
                    tcp.Write(Responses.Exception);
                    tcp.PutString(e.signal);
                    tcp.PutInt(e.objects.Length);
                    foreach (var o in e.objects)
                        tcp.PutString(o?.ToString()??"");
                    for (var i = e.info.First(); i != null; i = i.Next())
                    {
                        tcp.PutString(i.key().ToString());
                        tcp.PutString(i.value().ToString());
                    }
                    tcp.Flush();
                    tcp.Close();
                }
                catch (Exception) { }
                goto _return;
            }
            catch (Exception e)
            {
                try
                {
                    tcp.Write(Responses.FatalError);
                    Console.WriteLine("Internal error " + e.Message);
                    tcp.PutString(e.Message);
                    tcp.Flush();
                }
                catch (Exception) { }
                goto _return;
            }
            //       lock (PyrrhoStart.path)
            //           Console.WriteLine("Connection " + cid + " started");
            for (; ; )
            {
                try
                {
                    p = tcp.ReadByte();
                    if ((Protocol)p != Protocol.ReaderData)
                        recovering = false;
             //       lock (PyrrhoStart.path)
             //             Console.WriteLine("Connection " + cid + " " + (Protocol)p);
                }
                catch (Exception)
                {
                    p = -1;
                }
                if (p < 0)
                {
                    goto _return;
                }
                try
                {
                    switch ((Protocol)p)
                    {
                        case Protocol.ExecuteNonQuery: //  SQL service
                            {
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                long t = 0;
                                cx = new Context(db, cx??throw new PEException("PE14001"));
                                db = new Parser(cx).ParseSql(cmd, Domain.Content);
                                log?.WriteLine("" + (++PyrrhoStart.validationStep));
                                log?.WriteLine(cmd); log?.Flush();
                                cx.db = (Transaction)db;
                                cx.done = ObTree.Empty;
                                var tn = DateTime.Now.Ticks;
                                if (PyrrhoStart.DebugMode && tn > t)
                                    Console.WriteLine("" + (tn - t));
                                tcp.PutWarnings(cx);
                                db = db.RdrClose(ref cx);
                                tcp.Write(Responses.Done);
                                tcp.PutInt(db.AffCount(cx));
                                var r = cx.rdC;
                                cx = new(db, conn) { rdC = r };
                                break;
                            }
                        case Protocol.ExecuteNonQueryTrace: //  SQL service with trace
                            {
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                long t = 0;
                                cx = new Context(db, cx ?? throw new PEException("PE14002"));
                                var ts = db.length;
                                db = new Parser(cx).ParseSql(cmd, Domain.Content);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                var tn = DateTime.Now.Ticks;
                                if (PyrrhoStart.DebugMode && tn > t)
                                    Console.WriteLine("" + (tn - t));
                                tcp.PutWarnings(cx);
                                db = db.RdrClose(ref cx);
                                tcp.Write(Responses.DoneTrace);
                                tcp.PutLong(ts);
                                tcp.PutLong(db.length);
                                tcp.PutInt(db.AffCount(cx));
                                var r = cx.rdC;
                                cx = new(db, conn) { rdC = r };
                                break;
                            }
                        // close the reader
                        case Protocol.CloseReader:
                            {
                                if (cx is not null)
                                    db = db.RdrClose(ref cx);
                                rb = null;
                                break;
                            }
                        // start a new transaction
                        case Protocol.BeginTransaction:
                            {
                                db = db.Transact(db.nextId, conn, false);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Begin Transaction " + db.uid);
                                cx = new Context(db, conn);
                                break;
                            }
                        // commit
                        case Protocol.Commit:
                            {
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var tr = db;
                                db = db.Commit(cx ?? throw new PEException("PE14003"));
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Commit Transaction " + tr.uid);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.CommitTrace:
                            {
                                var ts = db.length;
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var tr = db;
                                db = db.Commit(cx ?? throw new PEException("PE14004"));
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Commit Transaction " + tr.uid);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.DoneTrace);
                                tcp.PutInt(db.AffCount(cx));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.length);
                                tcp.Flush();
                                cx = new Context(db, conn);
                                break;
                            }
                        // rollback
                        case Protocol.Rollback:
                            if (db is Transaction && PyrrhoStart.DebugMode)
                                Console.WriteLine("Rollback on Request " + db.uid);
                            db = db.Rollback();
                            cx = new Context(db, conn);
                            tcp.Write(Responses.Done);
                            break;
                        // close the connection
                        case Protocol.CloseConnection:
                            Close();
                            cx = null;
                            goto _return;
                        // Get names of local databases
                        case Protocol.GetFileNames:
                            {
                                var bf = tcp.PutFileNames(TCPStream.BBuf.Empty);
                                tcp.Write(bf, 0, int.MaxValue);
                            }
                            break;
                        // set the current reader
                        case Protocol.ResetReader:
                            rb = ((RowSet?)cx?.result)?.First(cx);
                            tcp.Write(Responses.Done);
                            tcp.Flush();
                            break;
                        case Protocol.ReaderData:
                            if (recovering)
                                continue;
                            ReaderData();
                            tcp.Flush();
                            break;
                        case Protocol.TypeInfo:
                            {
                                string dts = "";
                                db = db.Transact(db.nextId, conn);
                                try
                                {
                                    var dp = db.role?.dbobjects[tcp.GetString()]??Domain.Content.defpos;
                                    dts = ((Domain?)cx?.db?.objects[dp]??Domain.Content).ToString();
                                }
                                catch (Exception) { }
                                tcp.PutString(dts);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Prepare: // v7 Prepared statement API
                            {
                                var nm = tcp.GetString();
                                var sql = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                cx = new(db, conn) {
                                    parse = ExecuteStatus.Prepare
                                };
                                if (nm.Contains('('))
                                {
                                    // install an edge rename intervention
                                    cx.conn.Add(nm, sql);
                                    break;
                                }
                                var nst = cx.db.nextStmt;
                                db = new Parser(cx).ParseSql(sql, Domain.Content);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                cx.done = ObTree.Empty;
                                conn.Add(nm, new PreparedStatement(cx,nst));
                                cx.result = null;
                                cx.binding = CTree<long, TypedValue>.Empty;
                                db = db.RdrClose(ref cx);
                                tcp.Write(Responses.Done);
                                break;
                            }
                        case Protocol.Execute: // v7 Prepared statement API
                            {
                                var nm = tcp.GetString();
                                var n = tcp.GetInt();
                                var sb = new StringBuilder();
                                for (var i = 0; i < n; i++)
                                {
                                    sb.Append(tcp.GetString());
                                    sb.Append(';');
                                }
                                var ps = conn.prepared[nm]??
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                db = db.Transact(db.nextId, conn);
                                cx = new Context(db, cx ?? throw new PEException("PE14005"));
                                db = new Parser(cx).ParseSql(ps, cmp);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                if (cx.result is null)
                                {
                                    db = db.RdrClose(ref cx);
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt(db.AffCount(cx));
                                }
                                else
                                {
                                    tcp.PutRowType(cx);
                                    rb = ((RowSet?)cx.result)?.First(cx);
                                    while (rb != null && rb.IsNull)
                                        rb = rb.Next(cx);
                                    nextCol = 0;
                                }
                                break;
                            }
                        case Protocol.ExecuteTrace: // v7 Prepared statement API
                            {
                                var st = DateTime.Now;
                                var nm = tcp.GetString();
                                var n = tcp.GetInt();
                                var sb = new StringBuilder();
                                for (var i = 0; i < n; i++)
                                {
                                    sb.Append(tcp.GetString());
                                    sb.Append(';');
                                }
                                var ps = conn.prepared[nm] ??
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                db = db.Transact(db.nextId, conn);
                                var ts = db.length;
                                cx = new(db, cx ?? throw new PEException("PE14006"));
                                db = new Parser(cx).ParseSql(ps, cmp);
                                cx.db = (Transaction)db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                if (cx.result is null)
                                {
                                    db = db.RdrClose(ref cx);
                                    tcp.Write(Responses.DoneTrace);
                                    tcp.PutLong(ts);
                                    tcp.PutLong(db.length);
                                    tcp.PutInt(db.AffCount(cx));
                                }
                                else
                                    tcp.PutRowType(cx);
                                break;
                            }
                        case Protocol.ExecuteReader: // ExecuteReader
                            {
                                if (rb != null)
                                    throw new DBException("2E202").Mix();
                                nextCol = 0; // discard anything left over from ReaderData
                                var cmd = tcp.GetString();
                                log?.WriteLine("" + (++PyrrhoStart.validationStep));
                                log?.WriteLine(cmd);
                                var tr = db.Transact(db.nextId, conn);
                                var pl = tr.physicals.Count;
                                db = tr;
                                cx = new (db, cx??throw new PEException("PE14007"));
                                //           Console.WriteLine(cmd);
                                db = new Parser(cx).ParseSql(cmd, Domain.TableType);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                var tn = DateTime.Now.Ticks;
                                tcp.PutWarnings(cx);
                                // ignore ad hoc domains if no other Physicals
                                for (var b = ((Transaction)db).physicals.PositionAt(pl); b != null; b = b.Next())
                                    if (b.value() is PDomain)
                                        pl++;
                                if (cx.result is not null && ((Transaction)db).physicals.Count<=pl)
                                {
                                    if (cx.result is RowSet)
                                        cx.result = CheckPathGrouping(cx, (RowSet)cx.result);
                                    tcp.PutRowType(cx);
                                    rb = null;
                                    if (cx.result is RowSet res)
                                    {
                                        if (PyrrhoStart.ShowPlan)
                                            res.ShowPlan(cx);
                                        log?.WriteLine(new TDocArray(new Context(cx), res));
                                        cx.rdC += res.dependents;
                                        rb = res.First(cx);
                                        while (rb != null && rb.IsNull)
                                            rb = rb.Next(cx);
                                    }
                                    db = cx.db;
                                }
                                else
                                {
                                    var ocx = cx;
                                    db = db.RdrClose(ref cx);
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt(db.AffCount(ocx));
                                }
                                break;
                            }
                        case Protocol.ExecuteMatch: // ExecuteMatch
                            {
                                if (rb != null)
                                    throw new DBException("2E202").Mix();
                                nextCol = 0; // discard anything left over from ReaderData
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                cx = new(db, cx ?? throw new PEException("PE14008"));
                                //           Console.WriteLine(cmd);
                                db = new Parser(cx).ParseSql(cmd, Domain.TableType);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                var tn = DateTime.Now.Ticks;
                                var c = db.AffCount(cx);
                                tcp.PutWarnings(cx);
                                if (c>0)
                                {
                                    tcp.Write(Responses.MatchDone);
                                    tcp.PutInt(c);   
                                    db = db.RdrClose(ref cx);
                                    var r = cx.rdC;
                                    cx = new(db, conn) { rdC = r };
                                }
                                else
                                {
                                    if (cx.result is RowSet)
                                        cx.result = CheckPathGrouping(cx, (RowSet)cx.result);
                                    tcp.Write(Responses.TableData);
                                    tcp.PutRowType(cx);
                                    rb = null;
                                    if (cx.result is RowSet rs)
                                    {
                                        log?.WriteLine(new TDocArray(cx, rs));
                                        cx.rdC += rs.dependents;
                                        rb = rs.First(cx);
                                    }
                                    db = cx.db;
                                }
                                break;
                            }
                        case Protocol.Get: // GET rurl
                            {
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                string[] path = tcp.GetString().Split('/');
                                var tr = db.Transact(db.nextId, conn);
                                db = tr;
                                cx = new(db, cx??throw new PEException("PE14009")) { versioned = true };
                                tr.Execute(cx, k, "GET", db.name, path, "", "", "");
                                tcp.PutWarnings(cx);
                                if (cx.result is not null)
                                {
                                    tcp.PutRowType(cx);
                                    rb = ((RowSet?)cx.result)?.First(cx);
                                }
                                else
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(ref cx);
                                }
                                break;
                            }
                        case Protocol.Get2: // GET rurl version for weakly-typed languages
                            {
                                string[] path = tcp.GetString().Split('/');
                                var tr = db.Transact(db.nextId, conn);
                                db = tr;
                                cx = new Context(db, cx??throw new PEException("PE14010"));
                                tr.Execute(cx, 0L, "GET",  db.name, path, "", "", "");
                                tcp.PutWarnings(cx);
                                if (cx.result is RowSet rs)
                                {
                                    tcp.PutSchema1(cx, rs);
                                    rb = rs.First(cx);
                                }
                                else
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(ref cx);
                                }
                                break;
                            }
                        case Protocol.GetInfo: // for a table or structured type name for database[0]
                            {
                                string tname = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                var tr = (Transaction)db;
                                tcp.PutWarnings(cx ?? throw new PEException("PE14011"));
                                if (cx.GetObject(tname) is not Table tb)
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(ref cx);
                                }
                                else
                                    tcp.PutColumns(cx, tb);
                                break;
                            }
                        case Protocol.Post:
                            {   // we go to a lot of trouble to set things up like SqlInsert._Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                cx = new (db, cx ?? throw new PEException("PE14012"));
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("POST " + s);
                                var ss = s.Split('/');
                                if (ss.Length < 3)
                                    throw new DBException("Protocol error");
                                var t = long.Parse(ss[1]);
                                var tb = (Table?)db.objects[t]??throw new DBException("42105").Add(Qlx.TABLE);
                                var ti = tb.infos[db.role.defpos];
                                var f = new TableRowSet(1L, cx, t, 0L);
                                BTree<long, TargetActivation>? ans = null;
                                CTree<long, TypedValue> old=CTree<long,TypedValue>.Empty, vs;
                                vs = f.Parse(cx, ss[2]);
                                var data = new TrivialRowSet(0L,cx.GetUid(), cx, new TRow(f, vs));
                                ans = f.Insert(cx, data, f);
                                if (data.First(cx) is Cursor ib)
                                {
                                    old = CTree<long, TypedValue>.Empty;
                                    for (var b = f.rowType.First(); b != null; b = b.Next())
                                        if (b.value() is long bp && f.iSMap[bp] is long fp &&
                                            vs[p] is TypedValue tt)
                                            old += (fp, tt);
                                    if (ans?.First()?.value() is TableActivation ta)
                                    {
                                        ta.cursors += (ta._fm.defpos, ib);
                                        ta.EachRow(cx, ib._pos);
                                        cx.db = ta.db;
                                        ta.Finish(cx);
                                        vs = ta.newRow ?? CTree<long, TypedValue>.Empty;
                                        if (cx.affected != null && ta.affected != null)
                                            cx.affected += ta.affected;
                                    }
                                }
                                var oc = cx;
                                db = db.RdrClose(ref cx);
                                if (cx?.affected is Rvv rv && rv.Contains(t))
                                {
                                    var ep = rv[t]?.Last()?.value()??-1L;
                                    var en = 0;
                                    for (var b = tb?.rowType.First(); b != null; b = b.Next())
                                        if (b.value() is long a)
                                        {
                                            var dt = tb?.representation[a] ?? Domain.Content;
                                            if (dt.Compare(old[a] ?? TNull.Value, vs[a] ?? TNull.Value) != 0)
                                                en++;
                                        }
                                    tcp.Write(Responses.Entity);
                                    tcp.PutInt(en);
                                    for (var b = tb?.rowType.First(); b != null; b = b.Next())
                                        if (b.value() is long a)
                                        {
                                            var dt = tb?.representation[a] ?? Domain.Content;
                                            if (dt.Compare(old[a] ?? TNull.Value, vs[a] ?? TNull.Value) != 0
                                                && cx.NameFor(a) is string an)
                                            {
                                                tcp.PutString(an);
                                                tcp.PutInt(dt.Typecode());
                                                var bb = TCPStream.BBuf.Empty + (cx, dt, vs[a] ?? TNull.Value);
                                                tcp.Write(bb, 0, bb.m.Length);
                                            }
                                        }
                                    tcp.PutLong(ep);
                                    tcp.Write(Responses.Done);
                                }
                                else
                                    tcp.Write(Responses.NoData);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Put:
                            {   // we go to a lot of trouble to set things up like UpdateSearch._Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                cx = new Context(db, conn);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("PUT " + s);
                                var ss = s.Split('/');
                                if (ss.Length < 5)
                                    throw new DBException("Protocol error");
                                if (!long.TryParse(ss[1], out long t))
                                    throw new DBException("42161", "entity", ss[1]);
                                var tb = (Table?)db.objects[t];
                                var ro = db.role;
                                if (tb == null)
                                    throw new DBException("42105").Add(Qlx.TABLE);
                                var ti = tb.infos[ro.defpos];
                                var f = new TableRowSet(1L, cx, t, 0L);
                                BTree<long, TargetActivation>? ans = null;
                                CTree<long, TypedValue>? old = null, vs = null;
                                if (long.TryParse(ss[2], out long dp) && long.TryParse(ss[3], out long pp)
                                    && tb.tableRows[dp] is TableRow tr && tr.ppos == pp)
                                    if (TableRowSet.TableCursor.New(cx, f, dp) is Cursor ib)
                                    {
                                        old = tr.vals;
                                        vs = f.Parse(cx, ss[4]);
                                        var us = BTree<long, UpdateAssignment>.Empty;
                                        for (var b = f.rowType.First(); b != null; b = b.Next())
                                            if (b.value() is long c)
                                            {
                                                var ov = ib.values[c] ?? TNull.Value;
                                                var nv = vs[c] ?? TNull.Value;
                                                var dt = f.representation[c] ?? Domain.Content;
                                                if (dt.Compare(ov, nv) != 0)
                                                {
                                                    var ns = (QlValue)cx.Add(new SqlLiteral(cx.GetUid(), nv));
                                                    us += (c, new UpdateAssignment(c, ns.defpos));
                                                }
                                            }
                                        ans = f.Update(cx, f);
                                        if (ans?.First()?.value() is TableActivation ta)
                                        {
                                            ta.updates = us;
                                            ta.cursors += (ta._fm.defpos, ib);
                                            ta.EachRow(cx, ib._pos);
                                            cx.db = ta.db;
                                            ta.Finish(cx);
                                            vs = ta.newRow;
                                            if (cx.affected is not null && ta.affected is not null)
                                                cx.affected += ta.affected;
                                        }
                                    }
                                var oc = cx;
                                if (cx.db is not null)
                                    db = cx.db.RdrClose(ref cx);
                                if (cx.role != null && cx.affected is Rvv rv && rv.Contains(t))
                                {
                                    var ep = rv[t]?.Last()?.value();
                                    var en = 0;
                                    for (var b = tb.rowType.First(); b != null; b = b.Next())
                                        if (b.value() is long a)
                                        {
                                            var dt = tb.representation[a] ?? Domain.Content;
                                            if (vs?.Contains(a) == true && dt.Compare(old?[a] ?? TNull.Value, vs?[a] ?? TNull.Value) != 0)
                                                en++;
                                        }
                                    tcp.Write(Responses.Entity);
                                    tcp.PutInt(en);
                                    for (var b = tb.rowType.First(); b != null; b = b.Next())
                                        if (b.value() is long a && tb.representation[a] is Domain dt &&
                                                vs?.Contains(a) == true &&
                                                dt.Compare(old?[a] ?? TNull.Value, vs?[a] ?? TNull.Value) != 0
                                                && cx.NameFor(a) is string an)
                                        {
                                            tcp.PutString(an);
                                            tcp.PutInt(dt.Typecode());
                                            var bb = TCPStream.BBuf.Empty + (cx, dt, vs?[a] ?? TNull.Value);
                                            tcp.Write(bb,0,bb.m.Length);
                                        }
                                    tcp.PutLong(ep ?? -1L);
                                    tcp.Write(Responses.Done);
                                }
                                else
                                    tcp.Write(Responses.NoData);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Delete:
                            {   // we go to a lot of trouble to set things up like QuerySearch._Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, conn);
                                cx = new Context(db, conn);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("DELETE " + s);
                                var ss = s.Split('/');
                                if (ss.Length < 4)
                                    throw new DBException("Protocol error");
                                if (!long.TryParse(ss[1], out long t))
                                    throw new DBException("42161", "entity", ss[2]);
                                var tb = (Table?)db.objects[t];
                                var ro = db.role;
                                if (tb == null || ro == null)
                                    throw new DBException("42105").Add(Qlx.TABLE);
                                var ti = tb.infos[ro.defpos];
                                var f = new TableRowSet(1L, cx, t, 0L);
                                BTree<long, TargetActivation>? ans = null;
                                if (long.TryParse(ss[2], out long dp) 
                                    && long.TryParse(ss[3], out long pp)
                                    && tb.tableRows[dp] is TableRow tr && tr.ppos == pp)
                                {
                                    var r = new TRow(f, f.iSMap, tr.vals);
                                    var ib = TableRowSet.TableCursor.New(cx, f, dp);
                                    ans = f.Delete(cx, f);
                                    if (ib is not null && ans?.First()?.value() is TableActivation ta)
                                    {
                                        ta.cursors += (ta._fm.defpos, ib);
                                        ta.EachRow(cx, ib._pos);
                                        cx.db = ta.db;
                                        ta.Finish(cx);
                                        if (cx.affected is not null && ta.affected is not null)
                                            cx.affected += ta.affected;
                                    }
                                }
                                if (cx.db is not null)
                                db = cx.db.RdrClose(ref cx);
                                if (ans != BTree<long, TargetActivation>.Empty && cx.affected is not null)
                                {
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt((int)cx.affected.Count);
                                }
                                else
                                    tcp.Write(Responses.NoData);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Get1:
                            {
                                db = db.Transact(db.nextId, conn);
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                goto case Protocol.Get;
                            }
                        case Protocol.Rest:
                            {
                                var tr = db.Transact(db.nextId, conn);
                                db = tr;
                                cx = new(db, cx??throw new PEException("PE14013"));
                                var vb = tcp.GetString();
                                var url = tcp.GetString();
                                var jo = tcp.GetString();
                                tr.Execute(cx, 0L, vb, db.name, url.Split('/'), "", "application/json", jo);
                                tcp.PutWarnings(cx);
                                var ocx = cx;
                                db = db.RdrClose(ref cx);
                                rb = null;
                                tcp.PutRowType(ocx);
                                break;
                            }
                        case Protocol.CommitAndReport:
                            {
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx ?? throw new PEException("PE14014"));
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReport);
                                PutReport(cx);
                                cx = new Context(db,conn);
                                break;
                            }
                        case Protocol.CommitAndReport1:
                            {
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx ?? throw new PEException("PE14015"));
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReport);
                                tcp.PutInt(db.AffCount(cx));
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.CommitAndReportTrace:
                            {
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var ts = db.length;
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx ?? throw new PEException("PE14016"));
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutLong(ts);
                                tcp.PutLong(db.length);
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.CommitAndReportTrace1:
                            {
                                if (db is not Transaction)
                                    throw new DBException("25000").Mix();
                                var ts = db.length;
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx ?? throw new PEException("PE14017"));
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutInt(db.AffCount(cx));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.length);
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.Authority:
                            {
                                var rn = tcp.GetString();
                                if (rn.Length != 0)
                                {
                                    rn = rn[0] == '"' && rn.Length > 1 && rn[^1] == '"' ? 
                                        rn[1..^1] : rn.ToUpper();
                                }
                                if (!db.roles.Contains(rn))
                                    throw new DBException("42105").Add(Qlx.ROLE);
                                conn.props += ("Role", rn);
                                if (db.roles[rn] is not long rp || db.objects[rp] is not Role ro
                                    || cx==null)
                                    throw new DBException("42105").Add(Qlx.ROLE);
                                db += ro;
                                db += (Database.Role, ro);
                                cx.db = db;
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.GraphInfo:
                            {
                                if (cx is null)
                                    throw new DBException("3D001");
                                if (cx.role?.dbobjects["Role$GraphInfo"] is not long tp
                                    || db.objects[tp] is not SystemTable st)
                                    throw new DBException("42105").Add(Qlx.ROLE);
                                tcp.Write(Responses.GraphInfo);
                                for (var b = new SystemRowSet(cx, st, 0L).First(cx); b != null; b = b.Next(cx))
                                {
                                    tcp.PutString(b[0].ToString());
                                    tcp.PutInt(b[1].ToInt()?? 0);
                                }
                                tcp.PutString("");
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                break;
                            }
                        case 0: goto case Protocol.EoF;
                        case Protocol.EoF:
                            Close();
                            goto _return; // eof on stream
                        default: throw new DBException("3D005").ISO();
                    }
                }
                catch (DBException e)
                {
                    try
                    {
                        db = db.Rollback();
                        cx = new Context(db,cx?.conn);
                        rb = null;
                        tcp.StartException();
                        tcp.Write(Responses.Exception);
                        tcp.PutString(e.Message);
                        tcp.PutInt(e.objects.Length);
                        foreach (var o in e.objects)
                            if (o != null)
                                tcp.PutString(o?.ToString() ?? "");
                        for (var ii = e.info.First(); ii != null; ii = ii.Next())
                        {
                            tcp.PutString(ii.key().ToString());
                            tcp.PutString(ii.value().ToString());
                        }
                        tcp.Flush();
                        recovering = true;
                        if (PyrrhoStart.DebugMode || PyrrhoStart.TutorialMode)
                        {
                            Console.Write("Exception " + e.Message);
                            foreach (var o in e.objects)
                                Console.Write(" " + (o?.ToString()??"$Null"));
                            Console.WriteLine();
                        }
                    }
                    catch (Exception) { }
                }
                catch (SocketException)
                {
                    db = db.Rollback();
                    cx = new Context(db);
                    goto _return;
                }
                catch (ThreadAbortException)
                {
                    db = db.Rollback();
                    cx = new Context(db);
                    goto _return;
                }
                catch (Exception e)
                {
                    try
                    {
                        db = db.Rollback();
                        rb = null;
                        cx = new Context(db);
                        tcp.StartException();
                        tcp.Write(Responses.FatalError);
                        Console.WriteLine("Internal Error " + e.Message);
                        var s = e.StackTrace;
                        while (s?.Length > 0)
                        {
                            var i = s.IndexOf('\r');
                            if (i < 0)
                                break;
                            Console.WriteLine(s[..i]);
                            s = s[(i + 2)..];
                        }
                        Console.WriteLine(s);
                        tcp.PutString(e.Message);
                    }
                    catch (Exception)
                    {
                        goto _return;
                    }
                    db = db.Rollback();
                    cx = new Context(db);
                }
            }
        _return: if (PyrrhoStart.TutorialMode)
                Console.WriteLine("(" + cid + ") Ends with " + p);
            tcp?.Close();
        }
        RowSet CheckPathGrouping(Context cx, RowSet rs)
        {
            var rp = rs.representation;
            for (var b = rs.First(); b != null; b = b.Next())
                if (b.value() is long p && rs.representation[p] is Domain dm &&
                    cx.obs[p] is GqlNode g && g.state[p]?.type.HasFlag(TGParam.Type.Group) == true)
                        rp += (p,new Domain(-1L, Qlx.ARRAY, dm));
            if (rp != rs.representation)
                return rs + (Domain.Representation, rp);
            return rs;
        }
        static BTree<string, string> GetConnectionString(TCPStream tcp)
        {
            var dets = BTree<string, string>.Empty;
            var t = DateTime.Now.Ticks;
            try
            {
                tcp.PutLong(t);
                tcp.crypt.key = t;
                int n = tcp.ReadByte(); // should be 0
                if (n != 0)
                    return dets;
                for (; ; )
                {
                    string? str = null;
                    int b = tcp.crypt.ReadByte();
                    if (b < (int)Connecting.Password || b > (int)Connecting.CaseSensitive)
                        throw new DBException("42105").Add(Qlx.CONNECTION);
                    switch ((Connecting)b)
                    {
                        case Connecting.AllowAsk: str = "AllowAsk"; break;
                        case Connecting.Done: return dets;
                        case Connecting.Password: str = "Password"; break;
                        case Connecting.User: str = "User"; break;
                        case Connecting.Files: str = "Files"; break;
                        case Connecting.Role: str = "Role"; break;
                        case Connecting.Stop: str = "Stop"; break;
                        case Connecting.Host: str = "Host"; break;
                        case Connecting.Key: str = "Key"; break;
                        case Connecting.Base: str = "Base"; break;
                        case Connecting.Coordinator: str = "Coordinator"; break;
                        case Connecting.BaseServer: str = "BaseServer"; break;
                        case Connecting.Modify: str = "Modify"; break;
                        case Connecting.Length: str = "Length"; break;
                        case Connecting.Culture: str = "Locale"; break;
                        case Connecting.CaseSensitive: str = "CaseSensitive"; break;
                        case Connecting.Schema: str = "Schema"; break;
                        case Connecting.Graph: str = "Graph"; break;
                        default:
                            throw new DBException("42105").Add(Qlx.CONNECTION);
                    }
                    dets += (str, tcp.crypt.GetString());
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Bad client connection attempt");
            }
            return dets;
        }
        /// <summary>
        /// Close the connection
        /// </summary>
		void Close()
        {
            if (db != null)
            {
                db.Rollback();
                cx = new Context(db);
            }
            tcp.Close();
            rb = null;
            client.Close();
        }
        /// <summary>
        /// Send a block of data as part of a stream of cells.
        /// We send as many cells as will fit in a 2048-byte block, 
        /// we will prefix the cells by 4 bytes saying how many cells are in the block.
        /// Any cell that is larger than 2048 bytes is sent in smaller pieces.
        /// For other cells we use class BBuf to collect as many cells as will fit.
        /// </summary>
        internal void ReaderData()
        {
            if (cx == null)
                return;
            if ((!lookAheadDone) && nextCol == 0)
            {
                for (rb = rb?.Next(cx); rb != null; rb = rb.Next(cx))
                    ;
                lookAheadDone = true;
                nextCell = null;
            }
            // The protocol response byte is either ReaderData or NoData
            if (rb is null)
            {
                tcp.Write(Responses.NoData);
                return;
            }
            tcp.Write(Responses.ReaderData);
            tcp.ncells = 1;
            var domains = BTree<int, Domain>.Empty;
            var i = 0;
            if (rb.columns is CList<long> co)
            {
                for (var b = co.First(); b != null; b = b.Next(), i++)
                    if (b.value() is long p)
                        domains += (i, rb._dom.representation[p] ?? Domain.Content);
            }
            else
                for (var b = rb.dataType.representation.First(); b != null; b = b.Next(), i++)
                    domains += (i, b.value() ?? Domain.Content);
            var dc = domains[nextCol] ?? throw new PEException("PE1401");
            var ds = rb.display;
            if (ds == 0)
                ds = rb.Length;
            nextCell = rb[nextCol++];
            if (nextCol == ds)
                lookAheadDone = false;
            var (rv, rc) = TCPStream.Check(cx, rb);
            var bbuf = TCPStream.BBuf.Empty;
            bbuf += (cx, dc, nextCell, rv ?? "", rc ?? "");
            for (; ; )
            {
                if (nextCol == ds)
                {
                    if (!lookAheadDone)
                        for (rb = rb.Next(cx); rb != null && rb.IsNull; rb = rb.Next(cx))
                            ;
                    more = rb != null;
                    lookAheadDone = true;
                    nextCol = 0;
                    if (rb == null)
                        break;
                    (rv, rc) = TCPStream.Check(cx, rb);
                }
                nextCell = rb[nextCol] ?? throw new PEException("PE0110");
                dc = domains[nextCol] ?? throw new PEException("PE1405");
                var nbuf = bbuf + (cx, dc, nextCell, rv ?? "", rc ?? "");
                if (nbuf.m.Length + 7 >= TCPStream.bSize)
                    break;
                bbuf = nbuf;
                if (++nextCol == ds)
                    lookAheadDone = false;
                tcp.ncells++;
            }
            tcp.wcount = 3;
            tcp.PutInt(tcp.ncells);
            // Finally add BBuf's bytes to the Buffer
            tcp.Write(bbuf, 0, bbuf.m.Length);
        }
        int DataLength(Context cx, TypedValue tv, string? rv = null, string? rc = null)
        {
            var lc = 0;
            if (rv != null)
                lc += 1 + StringLength(rv);
            if (rc != null)
                lc += 1 + StringLength(rc);
            if (tv == TNull.Value)
                return lc + 1;
            object o = tv;
            switch (tv.dataType.kind)
            {
                case Qlx.BOOLEAN: return 5;
                case Qlx.INTEGER: break;
                case Qlx.NUMERIC:
                    if (tv is TNumeric nv)
                        o = nv.value;
                    break;
                case Qlx.REAL:
                    return lc + 1 + StringLength(tv.ToDouble());
                case Qlx.DATE:
                    return lc + 9; // 1+long
                case Qlx.TIME:
                    return lc + 9;
                case Qlx.TIMESTAMP:
                    return lc + 9;
                case Qlx.BLOB:
                    return lc + 5 + ((byte[])o).Length;
                case Qlx.ROW:
                    return lc + 1 + RowLength(cx, (TRow)tv);
                case Qlx.ARRAY:
                    return lc + 1 + ArrayLength(cx, tv);
                case Qlx.SET:
                    return lc + 1 + SetLength(cx, (TSet)o);
                case Qlx.MULTISET:
                    return lc + 1 + MultisetLength(cx, (TMultiset)o);
                case Qlx.TABLE:
                    if (o is Cursor c && cx.obs[c._rowsetpos] is RowSet rs)
                        return lc + 1 + TableLength(cx, rs);
                    if (o is RowSet ro)
                        return lc + 1 + TableLength(cx, ro);
                    break;
                case Qlx.INTERVAL:
                    return lc + 10; // 1+ 1byte + (1long or 2xint)
                case Qlx.TYPE:
                    {
                        if (tv.dataType is UDType ut)
                        {
                            if (ut.prefix != null)
                                return lc + 1 + StringLength(ut.prefix) + StringLength(o);
                            if (ut.suffix != null)
                                return lc + 1 + StringLength(o) + StringLength(ut.suffix);
                            if (tv is TTypeSpec tt)
                                return lc + 1 + StringLength(tt._dataType.name);
                        }
                        var tn = tv.dataType.name;
                        return lc + 1 + tn.Length + ((TRow)o).Length;
                    }
                case Qlx.NODETYPE:
                    if (tv.dataType is NodeType nt && tv is not TNode)
                        return lc + 1 + StringLength(nt.Describe(cx));
                    return lc+1 + StringLength(((TNode)tv).ToString(cx));
                case Qlx.EDGETYPE:
                    if (tv.dataType is EdgeType et && tv is not TNode)
                        return lc + 1 + StringLength(et.Describe(cx));
                    return lc + 1 + StringLength(((TNode)tv).ToString(cx));
            }
            return lc + 1 + StringLength(o);
        }
        static int StringLength(object? o)
        {

            return o == null ? 6 : 4 + Encoding.UTF8.GetBytes(o.ToString()??"").Length;
        }
        static int TypeLength(Domain t)
        {
            return 5 + StringLength(t.ToString());
        }
        int RowLength(Context cx, Cursor r)
        {
            int len = 4;
            var dt = r.dataType;
            int n = dt.Length;
            for (int i = 0; i < n; i++)
            {
                var c = r[i];
                len += StringLength(r.NameFor(cx, i)) + TypeLength(c.dataType)
                    + DataLength(cx, c);
            }
            return len;
        }
        int RowLength(Context cx, TRow v)
        {
            int len = 4;
            for (var b = v.columns.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var i = b.key();
                    len += StringLength(Domain.NameFor(cx, p, i))
                        + TypeLength(v.dataType.representation[p] ?? Domain.Content)
                        + DataLength(cx, v[i]);
                }
            return len;

        }
        int ArrayLength(Context cx, TypedValue a)
        {
            int len = 4 + StringLength("ARRAY") + TypeLength(a.dataType.elType ??Domain.Content);
            if (a is TArray ta)
            for (var b = ta.array.First(); b != null; b = b.Next())
                len += 1 + DataLength(cx, b.value());
            else if (a is TList tl)
                for (var b = tl.list.First(); b != null; b = b.Next())
                    len += 1 + DataLength(cx, b.value());
            return len;
        }
        int MultisetLength(Context cx, TMultiset m)
        {
            int len = 4 + StringLength("MULTISET") + TypeLength(m.dataType.elType ?? Domain.Content);
            for (var e = m.First(); e != null; e = e.Next())
                len += 1 + DataLength(cx, e.Value());
            return len;
        }
        int SetLength(Context cx,TSet s)
        {
            int len = 4 + StringLength("SET") + TypeLength(s.dataType.elType ?? Domain.Content);
            for (var e = s.First(); e != null; e = e.Next())
                len += 1 + DataLength(cx, e.Value());
            return len;
        }
        int TableLength(Context _cx, RowSet r)
        {
            int len = 4 + StringLength("TABLE") + SchemaLength(_cx, r);
            for (var e = r.First(_cx); e != null; e = e.Next(_cx))
                len += RowLength(_cx, e);
            return len;
        }
        static int SchemaLength(Context cx, RowSet r)
        {
            int len = 5;
            int m = r.display;
            if (m > 0 && cx.role is not null)
            {
                len += StringLength(cx.obs[r.defpos]?.infos[cx.role.defpos]?.name??"");
                int[] flags = new int[m];
                r.Schema(cx, flags);
                var j = 0;
                for (var b = r.representation.First(); b != null; b = b.Next(), j++)
                {
                    var d = b.value() ??Domain.Content;
                    len += StringLength(cx.obs[b.key()]?.name??"")
                        + TypeLength(d);
                }
            }
            return len;
        }
        /// <summary>
        /// Send the transaction report to the client
        /// </summary>
        /// <param name="tr"></param>
        internal void PutReport(Context cx)
        {
            tcp.PutLong(0L);
            tcp.PutInt((cx?.affected==null)?0:(int)cx.affected.Count);
            for (var tb = cx?.affected?.First(); tb != null; tb = tb.Next())
                for (var b = tb.value().First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        tcp.PutString(DBObject.Uid(tb.key()));
                        tcp.PutLong(b.key());
                        tcp.PutLong(p);
                    }
        }
    }
	/// <summary>
	/// The Client Listener for the PyrrhoDBMS.
    /// The Main entry point is here
	/// </summary>
	class PyrrhoStart
	{
        /// <summary>
        /// the default database folder
        /// </summary>
        internal static string path = "";
#if WINDOWS
        internal static FileSecurity arule;
#endif
        /// <summary>
        /// The identity that started the service
        /// </summary>
        internal static string domain="";
        /// <summary>
        /// the name of the hp image
        /// </summary>
		internal static string image = "PyrrhoSvr.exe";
        internal static ServerStatus state = ServerStatus.Open;
        /// <summary>
        /// a TCP listener for the Pyrrho service
        /// </summary>
		static TcpListener? tcp;
        public static HttpClient htc = new();
        public static string host = "::1";
        public static string hostname = "localhost";
        public static int port = 5433;
        internal static bool VerboseMode = false, TutorialMode = false, DebugMode = false, 
            HTTPFeedbackMode = false,ShowPlan = false, ValidationMode = false;
        internal static int validationStep = 0;
        internal static StreamWriter? validationLog = null;
        /// <summary>
        /// The main service loop of the Pyrrho DBMS is here
        /// </summary>
        internal static void Run()
        {
            var ad = IPAddress.Parse(host);
            var i = 0;
            while (tcp == null && i++ < 100)
            {
                try
                {
                    tcp = new TcpListener(ad, port);
                    tcp.Start();
                }
                catch (Exception)
                {
                    port++;
                    tcp = null;
                }
            }
            if (tcp == null)
                throw new Exception("Cannot open a port on "+host);
            Console.WriteLine("PyrrhoDBMS protocol on "+host+":" + port);
            if (path!="")
                Console.WriteLine("Database folder " + path);
            int cid = 0;
            for (; ; )
                try
                {
                    Socket client = tcp.AcceptSocket();
                    var t = new Thread(new ThreadStart(new PyrrhoServer(client).Server))
                    {
                        Name = "T" + (++cid)
                    };
                    t.Start();
                }
                catch (Exception)
                { }
        }
        /// <summary>
        /// The main entry point for the application. Process arguments and create the main service loop
        /// </summary>
        [STAThread]
		static void Main(string[] args)
		{
            foreach (var s in args)
                Console.Write(s + " ");
            Console.Write("Enter to start up");
            Console.ReadLine();
            for (int j = 0; j < Version.Length; j++)
                switch (j)
                {
                    case 1:
                    case 2:
                        Console.Write(Version[j]);
                        Console.Write(' ');
                        break;
                    default:
                        Console.WriteLine(Version[j]);
                        break;
                }
			int k = 0;
            int httpport = 0;
            int httpsport = 0;
            for (; args.Length > k; k++)
                if (args[k][0] == '-')
                    switch (args[k][1])
                    {
                        case 'p': port = int.Parse(args[k][3..]); break;
                        case 'h': host = args[k][3..]; break;
                        case 'n': hostname = args[k][3..]; break;
                        case 'd':
                            path = args[k][3..];
                            FixPath();
                            break;
                        case 'D': DebugMode = true; break;
                        case 'H': HTTPFeedbackMode = true; break;
                        case 'V': VerboseMode = true; break;
                        case 'T': TutorialMode = true; break;
                        default: Usage(); return;
                    }
                else if (args[k][0] == '+')
                {
                    int p = -1;
                    if (args[k].Length > 2)
                        p = int.Parse(args[k][3..]);
                    switch (args[k][1])
                    {
                        case 'R': ShowPlan = true; break;
                        case 's': httpport = (p < 0) ? 8180 : p; break;
                        case 'S': httpsport = (p < 0) ? 8133 : p; break;
                        case 'V': ValidationMode = true; validationStep = (p < 0) ? 0 : p; break;
                    }
                }
            if (ValidationMode)
            {
                validationLog = new StreamWriter("log.txt");
                validationLog.AutoFlush = true;
            }
#if WINDOWS
            arule = new FileSecurity();
            var administrators = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);
            var everyone = new SecurityIdentifier(WellKnownSidType.WorldSid, null);
            arule.AddAccessRule(new FileSystemAccessRule(everyone, FileSystemRights.FullControl,
                AccessControlType.Deny));
            arule.AddAccessRule(new FileSystemAccessRule(administrators, FileSystemRights.FullControl,
                AccessControlType.Allow));
            arule.AddAccessRule(new FileSystemAccessRule(WindowsIdentity.GetCurrent().User,
                FileSystemRights.FullControl, AccessControlType.Allow));
#endif
            if (httpport > 0 || httpsport > 0)
                new Thread(new ThreadStart(new HttpService(hostname, httpport, httpsport).Run)).Start();
            Run();
		}
        static void FixPath()
        {
            if (path.Contains('/') && !path.EndsWith('/'))
                path += "/";
            else if (path.Contains('\\') && !path.EndsWith('\\'))
                path += "\\";
#if WINDOWS
            var acl = Directory.GetAccessControl(path);
            var acr = acl.GetAccessRules(true, true, typeof(SecurityIdentifier));
            if (acr == null)
                goto bad;
            foreach (FileSystemAccessRule r in acr)
                if ((FileSystemRights.Write&r.FileSystemRights)==FileSystemRights.Write
                    && r.AccessControlType==AccessControlType.Allow)
                    return;
            bad: throw new Exception("Cannot access path " + path);
#endif
        }
        /// <summary>
        /// Provide help about the command line options
        /// </summary>
        static void Usage()
		{
            string serverName = "PyrrhoSvr";
            Console.WriteLine("Usage: "+serverName+" [-d:path] [-h:host] [-n:hostname] [-p:port] [-t:nn] [+s[:http]] [+S[:https]] {-flag}");
            Console.WriteLine("Parameters:");
            Console.WriteLine("   -d  Use the given folder for database storage");
            Console.WriteLine("   -h  Use the given host address. Default is ::1");
            Console.WriteLine("   -n  Use the given host name. Default is localhost");
			Console.WriteLine("   -p  Listen on the given port. Default is 5433");
            Console.WriteLine("   +s[:port]  Start HTTP REST service on the given port (default 8180).");
            Console.WriteLine("   -t  Limit the number of connections to nnn");
            Console.WriteLine("   +S[:port]  Start HTTPS REST service on the given port (default 8133).");
            Console.WriteLine("Flags:");
            Console.WriteLine("   -D  Debug mode");
            Console.WriteLine("   +G  Show graph");
            Console.WriteLine("   -H  Show feedback on HTTP RESTView operations");
            Console.WriteLine("   +R  Show evaluation plan");
            Console.WriteLine("   -V  Verbose mode");
            Console.WriteLine("   -T  Tutorial mode");
		}
        /// <summary>
        /// Version information
        /// </summary>
 		internal static string[] Version =
        [
            "Pyrrho DBMS (c) 2025 Malcolm Crowe and University of the West of Scotland",
            "7.09alpha","(20 March 2025)", "http://www.pyrrhodb.com"
        ];
	}
}