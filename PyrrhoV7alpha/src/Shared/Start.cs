using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Text;
using System.Net;
using System.Net.Sockets;
using Pyrrho.Level2; // for Record
using Pyrrho.Level3; // for Database
using Pyrrho.Level4; // for Select
using Pyrrho.Level1; // for DataFile option
using Pyrrho.Common;
#if WINDOWS
using System.Security.AccessControl;
#endif

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
        Socket client;
        /// <summary>
        /// Contection: connection string details, prepared statements
        /// </summary>
        Connection conn;
        /// <summary>
        /// the Pyrrho protocol stream for this connection
        /// </summary>
		internal readonly TCPStream tcp;
        /// <summary>
        /// the database for this connection
        /// </summary>
        Database db = null;
        /// <summary>
        /// An internal unique identifier for this connection
        /// </summary>
        static int _cid = 0;
        int cid = _cid++;        /// <summary>
        /// Remaining local variables are volatile within protocol steps
        /// </summary>
        Context cx = null;
        Cursor rb = null;
        internal bool lookAheadDone = true, more = true;
        private int nextCol = 0;
        private TypedValue nextCell = null;
        /// <summary>
        /// Constructor: called on Accept
        /// </summary>
        /// <param name="c">the newly connected Client socket</param>
		public PyrrhoServer(Socket c)
        {
            client = c;
            tcp = new TCPStream();
            tcp.client = client;
            conn = new Connection();
            conn.props = GetConnectionString(tcp);
        }
        /// <summary>
        /// The main routine started in the thread for this client. This contains a protcol loop
        /// </summary>
        public void Server()
        {
            // process the connection string
            var fn = conn.props["Files"];
            int p = -1;
            bool recovering = false;
            try
            {
                db = Database.Get(conn.props);
                if (db == null)
                {
                    var fp = PyrrhoStart.path + fn;
                    var user = conn.props["User"];
                    if (!File.Exists(fp))
                    {
                        var fs = new FileStream(fp,
                        FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                        var wr = new Writer(null, fs);
                        wr.PutInt(777);
                        wr.PutInt(52);
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
                    if (PyrrhoStart.VerboseMode)
                        Console.WriteLine("Server " + cid + " " + user
                            + " " + fn + " " + db.role.name);
                    db = db.Load();
                }
                cx = new Context(db);
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
                        tcp.PutString(o.ToString());
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
                p = -1;
                try
                {
                    p = tcp.ReadByte();
                    if ((Protocol)p != Protocol.ReaderData)
                        recovering = false;
                    //              lock (PyrrhoStart.path)
                    //                  Console.WriteLine("Connection " + cid + " " + (Protocol)p);
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
                                db = db.Transact(db.nextId, cmd, conn);
                                long t = 0;
                                cx = new Context(db, cx);
                                db = new Parser(cx).ParseSql(cmd, Domain.Content);
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
                                cx = new Context(db, conn);
                                cx.rdC = r;
                                break;
                            }
                        case Protocol.ExecuteNonQueryTrace: //  SQL service with trace
                            {
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId, cmd, conn);
                                long t = 0;
                                cx = new Context(db, cx);
                                var ts = db.loadpos;
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
                                tcp.PutLong(db.loadpos);
                                tcp.PutInt(db.AffCount(cx));
                                var r = cx.rdC;
                                cx = new Context(db, conn);
                                cx.rdC = r;
                                break;
                            }
                        // close the reader
                        case Protocol.CloseReader:
                            {
                                db = db.RdrClose(ref cx);
                                rb = null;
                                break;
                            }
                        // start a new transaction
                        case Protocol.BeginTransaction:
                            {
                                db = db.Transact(db.nextId, "", conn, false);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Begin Transaction " + db.uid);
                                cx = new Context(db, conn);
                                break;
                            }
                        // commit
                        case Protocol.Commit:
                            {
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var tr = db;
                                db = db.Commit(cx);
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
                                var ts = db.loadpos;
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var tr = db;
                                db = db.Commit(cx);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Commit Transaction " + tr.uid);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.DoneTrace);
                                tcp.PutInt(db.AffCount(cx));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
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
                            tcp.PutFileNames(); break;
                        // set the current reader
                        case Protocol.ResetReader:
                            rb = ((RowSet)cx.obs[cx.result]).First(cx);
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
                                db = db.Transact(db.nextId, "", conn);
                                try
                                {
                                    var dm = db.role.dbobjects[tcp.GetString()];
                                    dts = dm.ToString();
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
                                db = db.Transact(db.nextId, sql, conn);
                                cx = new Context(db, conn);
                                cx.parse = ExecuteStatus.Prepare;
                                var nst = cx.db.nextStmt;
                                db = new Parser(cx).ParseSql(sql, Domain.Content);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                cx.done = ObTree.Empty;
                                conn.Add(nm, new PreparedStatement(cx,nst));
                                cx.result = -1L;
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
                                if (!conn.prepared.Contains(nm))
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                db = db.Transact(db.nextId, cmp, conn);
                                cx = new Context(db, cx);
                                db = new Parser(cx).ParseSql(conn.prepared[nm], cmp);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                if (cx.result <= 0L)
                                {
                                    db = db.RdrClose(ref cx);
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt(db.AffCount(cx));
                                }
                                else
                                {
                                    tcp.PutSchema(cx);
                                    rb = ((RowSet)cx.obs[cx.result]).First(cx);
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
                                if (!conn.prepared.Contains(nm))
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                db = db.Transact(db.nextId, cmp, conn);
                                var ts = db.loadpos;
                                cx = new Context(db, cx);
                                db = new Parser(cx).ParseSql(conn.prepared[nm], cmp);
                                cx.db = (Transaction)db;
                                cx.done = ObTree.Empty;
                                tcp.PutWarnings(cx);
                                if (cx.result < 0L)
                                {
                                    db = db.RdrClose(ref cx);
                                    tcp.Write(Responses.DoneTrace);
                                    tcp.PutLong(ts);
                                    tcp.PutLong(db.loadpos);
                                    tcp.PutInt(db.AffCount(cx));
                                }
                                else
                                    tcp.PutSchema(cx);
                                Console.WriteLine(nm + " " + (DateTime.Now.Ticks - st.Ticks));
                                break;
                            }
                        case Protocol.ExecuteReader: // ExecuteReader
                            {
                                if (rb != null)
                                    throw new DBException("2E202").Mix();
                                nextCol = 0; // discard anything left over from ReaderData
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId, cmd, conn);
                                cx = new Context(db, cx);
                                //           Console.WriteLine(cmd);
                                db = new Parser(cx).ParseSql(cmd, Domain.TableType);
                                cx.db = db;
                                cx.done = ObTree.Empty;
                                var tn = DateTime.Now.Ticks;
                                tcp.PutWarnings(cx);
                                if (cx.result > 0L)
                                {
                                    tcp.PutSchema(cx);
                                    rb = null;
                                    if (cx.obs[cx.result] is RowSet res)
                                    {
                                        if (PyrrhoStart.ShowPlan)
                                            res.ShowPlan(cx);
                                        var s = cx.obs.ToString();
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
                        case Protocol.Get: // GET rurl
                            {
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                string[] path = tcp.GetString().Split('/');
                                var tr = db.Transact(db.nextId, "", conn);
                                db = tr;
                                cx = new Context(db, cx);
                                cx.versioned = true;
                                tr.Execute(cx, k, "GET", db.name, path, "", "", "");
                                tcp.PutWarnings(cx);
                                if (cx.result > 0)
                                {
                                    tcp.PutSchema(cx);
                                    rb = ((RowSet)cx.obs[cx.result]).First(cx);
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
                                var tr = db.Transact(db.nextId, "", conn);
                                db = tr;
                                cx = new Context(db, cx);
                                tr.Execute(cx, 0L, "GET",  db.name, path, "", "", "");
                                tcp.PutWarnings(cx);
                                if (cx.result > 0)
                                {
                                    var rs = (RowSet)cx.obs[cx.result];
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
                                db = db.Transact(db.nextId, "", conn);
                                var tr = (Transaction)db;
                                tcp.PutWarnings(cx);
                                var tb = tr.GetObject(tname) as Table;
                                if (tb == null)
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(ref cx);
                                }
                                else
                                    tcp.PutColumns(cx, cx._Dom(tb));
                                break;
                            }
                        case Protocol.Post:
                            {   // we go to a lot of trouble to set things up like SqlInsert.Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, "", conn);
                                cx = new Context(db, cx);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("POST " + s);
                                var ss = s.Split('/');
                                if (ss.Length < 3)
                                    throw new DBException("Protocol error");
                                var t = long.Parse(ss[1]);
                                var tb = (Table)db.objects[t];
                                var ti = tb.infos[db.role.defpos];
                                var f = new TableRowSet(1L, cx, t);
                                BTree<long, TargetActivation> ans = null;
                                CTree<long, TypedValue> old, vs;
                                var dm = cx._Dom(f);
                                vs = dm.Parse(cx, ss[2]);
                                var data = new TrivialRowSet(cx.GetUid(), cx, new TRow(dm, vs));
                                ans = f.Insert(cx, data, dm.rowType);
                                var ib = data.First(cx);
                                old = CTree<long, TypedValue>.Empty;
                                for (var b = dm.rowType.First(); b != null; b = b.Next())
                                    old += (f.iSMap[b.value()], vs[b.value()]);
                                var ta = (TableActivation)ans.First().value();
                                ta.cursors += (ta._fm.defpos, ib);
                                ta.EachRow(ib._pos);
                                cx.db = ta.db;
                                ta.Finish();
                                vs = ta.newRow;
                                cx.affected += ta.affected;
                                var oc = cx;
                                db = db.RdrClose(ref cx);
                                if (cx?.affected is Rvv rv && rv.Contains(t))
                                {
                                    var ep = rv[t].Last().value();
                                    var en = 0;
                                    var td = cx._Dom(tb);
                                    for (var b = td.rowType.First(); b != null; b = b.Next())
                                    {
                                        var a = b.value();
                                        var dt = td.representation[a];
                                        if (dt.Compare(old[a], vs[a]) != 0)
                                            en++;
                                    }
                                    tcp.Write(Responses.Entity);
                                    tcp.PutInt(en);
                                    for (var b = td.rowType.First(); b != null; b = b.Next())
                                    {
                                        var a = b.value();
                                        var dt = td.representation[a];
                                        if (dt.Compare(old[a], vs[a]) != 0)
                                        {
                                            tcp.PutString(cx.NameFor(a));
                                            tcp.PutInt(dt.Typecode());
                                            tcp.PutData(cx, vs[a]);
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
                            {   // we go to a lot of trouble to set things up like UpdateSearch.Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, s, conn);
                                cx = new Context(db, conn);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("PUT " + s);
                                var ss = s.Split('/');
                                long t = 0;
                                if (ss.Length < 5)
                                    throw new DBException("Protocol error");
                                if (!long.TryParse(ss[1], out t))
                                    throw new DBException("42161", "entity", ss[1]);
                                var tb = (Table)db.objects[t];
                                var ro = db.role;
                                var ti = tb.infos[ro.defpos];
                                var f = new TableRowSet(1L, cx, t);
                                var dm = cx._Dom(f);
                                long dp = 0, pp = 0;
                                BTree<long, TargetActivation> ans = null;
                                CTree<long, TypedValue> old = null, vs = null;
                                if (long.TryParse(ss[2], out dp) && long.TryParse(ss[3], out pp)
                                    && tb.tableRows[dp] is TableRow tr && tr.ppos == pp)
                                {
                                    old = tr.vals;
                                    var ib = TableRowSet.TableCursor.New(cx,f,dp);
                                    vs = dm.Parse(cx, ss[4]);
                                    var us = BTree<long,UpdateAssignment>.Empty;
                                    for (var b=dm.rowType.First();b!=null;b=b.Next())
                                    {
                                        var c = b.value();
                                        var ov = ib.values[c];
                                        var nv = vs[c];
                                        var dt = dm.representation[c];
                                        if (dt.Compare(ov, nv) != 0)
                                        {
                                            var ns = (SqlValue)cx.Add(new SqlLiteral(cx.GetUid(), cx, nv));
                                            us += (c,new UpdateAssignment(c, ns.defpos));
                                        }
                                    }
                                    ans = f.Update(cx, f);
                                    var ta = (TableActivation)ans.First().value();
                                    ta.updates = us;
                                    ta.cursors += (ta._fm.defpos, ib);
                                    ta.EachRow(ib._pos);
                                    cx.db = ta.db;
                                    ta.Finish();
                                    vs = ta.newRow;
                                    cx.affected += ta.affected;
                                }
                                var oc = cx;
                                db = cx.db.RdrClose(ref cx);
                                var td = cx._Dom(tb);
                                if (cx?.affected is Rvv rv && rv.Contains(t))
                                {
                                    var ep = rv[t].Last().value();
                                    var en = 0;
                                    for (var b = td.rowType.First(); b != null; b = b.Next())
                                    {
                                        var a = b.value();
                                        var dt = td.representation[a];
                                        if (vs.Contains(a) && dt.Compare(old[a], vs[a]) != 0)
                                            en++;
                                    }
                                    tcp.Write(Responses.Entity);
                                    tcp.PutInt(en);
                                    for (var b = td.rowType.First(); b != null; b = b.Next())
                                    {
                                        var a = b.value();
                                        var dt = td.representation[a];
                                        if (vs.Contains(a) && dt.Compare(old[a], vs[a]) != 0)
                                        {
                                            var ci = cx._Ob(a).infos[cx.db.role.defpos];
                                            tcp.PutString(ci.name);
                                            tcp.PutInt(dt.Typecode());
                                            tcp.PutData(cx, vs[a]);
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
                        case Protocol.Delete:
                            {   // we go to a lot of trouble to set things up like QuerySearch.Obey()
                                // so that the trigger and cascade machinery gets called if necessary
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                db = db.Transact(db.nextId, s, conn);
                                cx = new Context(db, conn);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("DELETE " + s);
                                var ss = s.Split('/');
                                long t = 0;
                                if (ss.Length < 4)
                                    throw new DBException("Protocol error");
                                if (!long.TryParse(ss[1], out t))
                                    throw new DBException("42161", "entity", ss[2]);
                                var tb = (Table)db.objects[t];
                                var ro = db.role;
                                var ti = tb.infos[ro.defpos];
                                var f = new TableRowSet(1L, cx, t);
                                long dp = 0, pp = 0;
                                BTree<long, TargetActivation> ans = null;
                                var dm = cx._Dom(f);
                                if (long.TryParse(ss[2], out dp) && long.TryParse(ss[3], out pp)
                                    && tb.tableRows[dp] is TableRow tr && tr.ppos == pp)
                                {
                                    var r = new TRow(dm, f.iSMap, tr.vals);
                                    var ib = TableRowSet.TableCursor.New(cx, f, dp);
                                    ans = f.Delete(cx, f);
                                    var ta = (TableActivation)ans.First().value();
                                    ta.cursors += (ta._fm.defpos, ib);
                                    ta.EachRow(ib._pos);
                                    cx.db = ta.db;
                                    ta.Finish();
                                    cx.affected += ta.affected;
                                }
                                db = cx.db.RdrClose(ref cx);
                                if (ans != BTree<long, TargetActivation>.Empty)
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
                                db = db.Transact(db.nextId, "",conn);
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                goto case Protocol.Get;
                            }
                        case Protocol.Rest:
                            {
                                var tr = db.Transact(db.nextId, "",conn);
                                db = tr;
                                cx = new Context(db,cx);
                                var vb = tcp.GetString();
                                var url = tcp.GetString();
                                var jo = tcp.GetString();
                                tr.Execute(cx, 0L, vb, db.name, url.Split('/'), "", "application/json", jo);
                                tcp.PutWarnings(cx);
                                var ocx = cx;
                                db = db.RdrClose(ref cx);
                                rb = null;
                                tcp.PutSchema(ocx);
                                break;
                            }
                        case Protocol.CommitAndReport:
                            {
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReport);
                                PutReport(cx);
                                cx = new Context(db,conn);
                                break;
                            }
                        case Protocol.CommitAndReport1:
                            {
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReport);
                                tcp.PutInt(db.AffCount(cx));
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.CommitAndReportTrace:
                            {
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var ts = db.loadpos;
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.CommitAndReportTrace1:
                            {
                                if (!(db is Transaction))
                                    throw new DBException("25000").Mix();
                                var ts = db.loadpos;
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(cx);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutInt(db.AffCount(cx));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                PutReport(cx);
                                cx = new Context(db, conn);
                                break;
                            }
                        case Protocol.Authority:
                            {
                                var rn = tcp.GetString();
                                if (rn.Length != 0)
                                {
                                    if (rn[0] == '"' && rn.Length > 1 && rn[rn.Length - 1] == '"')
                                        rn = rn.Substring(1, rn.Length - 2);
                                    else
                                        rn = rn.ToUpper();
                                }
                                if (!db.roles.Contains(rn))
                                    throw new DBException("42105");
                                conn.props += ("Role", rn);
                                var rp = db.roles[rn];
                                db += ((Role)db.objects[rp], db.loadpos);
                                db += (Database._Role, rp);
                                cx.db = db;
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
                        cx = new Context(db);
                        rb = null;
                        tcp.StartException();
                        tcp.Write(Responses.Exception);
                        tcp.PutString(e.Message);
                        tcp.PutInt(e.objects.Length);
                        foreach (var o in e.objects)
                            if (o != null)
                                tcp.PutString(o.ToString());
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
                        while (s.Length > 0)
                        {
                            var i = s.IndexOf('\r');
                            if (i < 0)
                                break;
                            Console.WriteLine(s.Substring(0,i));
                            s = s.Substring(i + 2);
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
 /*       static DateTime startTrace;
        internal static bool tracing = false;
        internal static void Debug(int a, string m) // a=0 start, 1-continue, 2=stop
        {
            TimeSpan t;
            switch (a)
            {
                case 0:
                    tracing = true;
                    startTrace = DateTime.Now;
                    Console.WriteLine("Start " + m);
                    break;
                case 1:
                    if (!tracing)
                        return;
                    t = DateTime.Now - startTrace;
                    Console.WriteLine(m + " " + t.TotalMilliseconds);
                    break;
                case 2:
                    tracing = false;
                    t = DateTime.Now - startTrace;
                    Console.WriteLine(m + " " + t.TotalMilliseconds + " Stop " + m);
                    break;
            }
        } */
        BTree<string, string> GetConnectionString(TCPStream tcp)
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
                    string str = null;
                    int b = tcp.crypt.ReadByte();
                    if (b < (int)Connecting.Password || b > (int)Connecting.Modify)
                        return null;
                    switch ((Connecting)b)
                    {
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
                        default:
                            return null;
                    }
                    dets += (str, tcp.crypt.GetString());
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Only OSP clients can connect to this server. A Pyrho DBMS client tried to do so!");
            }
            return dets;
        }
        /// <summary>
        /// Close the connection
        /// </summary>
		void Close()
        {
            if (db != null)
                db.Rollback();
            cx = new Context(db);
            tcp.Close();
            rb = null;
            client.Close();
        }
        /// <summary>
        /// Send a block of obs as part of a stream of rows
        /// </summary>
        internal void ReaderData()
        {
            if ((!lookAheadDone) && nextCol == 0)
            {
                for (rb = rb.Next(cx); rb != null && rb.IsNull; rb = rb.Next(cx))
                    ;
                lookAheadDone = true;
                nextCell = null;
            }
            more = rb != null && !rb.IsNull;
            if (!more)
            {
                tcp.Write(Responses.NoData);
                return;
            }
            tcp.Write(Responses.ReaderData);
            int ncells = 1; // we will very naughtily poke this into the write buffer later (at offset 3)
            // for now we announce that we will send one cell: we always send at least one cell
            tcp.PutInt(1);
            var domains = BTree<int, Domain>.Empty;
            var i = 0;
            if (rb.columns is CList<long> co)
                for (var b = co.First(); b != null; b = b.Next(), i++)
                    domains += (i, rb._dom.representation[b.value()]);
            else
                for (var b = rb.dataType.representation.First(); b != null; b = b.Next(), i++)
                    domains += (i, cx._Dom(b.value()));
            var dc = domains[nextCol];
            var ds = rb.display;
            if (ds == 0)
                ds = rb.Length;
            nextCell = rb[nextCol++];
            if (nextCol == ds)
                lookAheadDone = false;
            var (rv,rc) = tcp.Check(cx, rb);
            tcp.PutCell(cx, dc, nextCell, rv, rc);
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
                    if (!more)
                        break;
                    (rv,rc) = tcp.Check(cx, rb);
                }
                nextCell = rb[nextCol];
                int len = DataLength(cx, nextCell,rv,rc);
                dc = domains[nextCol];
                if (nextCell != null && dc.CompareTo(nextCell.dataType)!=0)
                {
                    var nm = rb.NameFor(cx, nextCol);
                    if (nm == null)
                        nm = nextCell.dataType.ToString();
                    len += 4 + StringLength(nm);
                }
                if (tcp.wcount + len + 1 >= TCPStream.bSize)
                    break;
                tcp.PutCell(cx, dc, nextCell, rv, rc);
                if (++nextCol == ds)
                    lookAheadDone = false;
                ncells++;
            }
            // naughty naughty: update ncells
            if (ncells != 1)
            {
                int owc = tcp.wcount;
                tcp.wcount = 3;
                tcp.PutInt(ncells);
                tcp.wcount = owc;
            }
        }
        int DataLength(Context cx, TypedValue tv, string rv = null, string rc=null)
        {

            var lc = 0;
            if (rv != null)
                lc += 1 + StringLength(rv);
            if (rc != null)
                lc += 1 + StringLength(rc);
            if (tv == null || tv==TNull.Value)
                return lc+1; 
            object o = tv.Val();
            switch (tv.dataType.kind)
            {
                case Sqlx.BOOLEAN: return 5;
                case Sqlx.INTEGER: break;
                case Sqlx.NUMERIC:
                    if (o is long)
                        o = new Common.Numeric((long)o);
                    else if (o is double)
                        o = new Common.Numeric((double)o);
                    break;
                case Sqlx.REAL:
                    if (o is Common.Numeric)
                        return lc+ 1 + StringLength(o);
                    return 1 + StringLength(new Common.Numeric((double)o).DoubleFormat());
                case Sqlx.DATE:
                    return lc+9; // 1+long
                case Sqlx.TIME:
                    return lc+9;
                case Sqlx.TIMESTAMP:
                    return lc+9;
                case Sqlx.BLOB:
                    return lc+5 + ((byte[])o).Length;
                case Sqlx.ROW:
                    {
                        if (o is TRow r)
                            return lc+1 + RowLength(cx, r);
                        return lc+1 + RowLength(cx, ((RowSet)o).First(cx));
                    }
                case Sqlx.ARRAY:
                    return lc+1 + ArrayLength(cx, (TArray)tv);
                case Sqlx.MULTISET:
                    return lc+1 + MultisetLength(cx, (TMultiset)o);
                case Sqlx.TABLE:
                    if (o is Cursor c)
                        return lc+1 + TableLength(cx, (RowSet)cx.obs[c._rowsetpos]);
                    return lc+1 + TableLength(cx, (RowSet)o);
                case Sqlx.INTERVAL:
                    return lc+10; // 1+ 1byte + (1long or 2xint)
                case Sqlx.TYPE:
                    {
                        if (tv.dataType is UDType ut)
                        {
                            if (ut.prefix!=null)
                                return lc + 1 + StringLength(ut.prefix) + StringLength(o);
                            if (ut.suffix!=null)
                                return lc + 1 + StringLength(o) + StringLength(ut.suffix);
                        }
                        var tn = tv.dataType.name;
                        return lc+1 + tn.Length + ((TRow)o).Length;
                    }
                case Sqlx.XML: break;
            }
            return lc+1 + StringLength(o);
        }
        int StringLength(object o)
        {
            if (o == null)
                return 6;
            return 4 + Encoding.UTF8.GetBytes(o.ToString()).Length;
        }
        int TypeLength(Domain t)
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
            {
                var i = b.key();
                var p = b.value();
                len += StringLength(v.dataType.NameFor(cx, p, i))
                    + TypeLength(v.dataType.representation[p])
                    + DataLength(cx, v[i]);
            }
            return len;

        }
        int ArrayLength(Context cx, TArray a)
        {
            int len = 4 + StringLength("ARRAY") + TypeLength(cx._Dom(a.dataType.elType));
            for (var b = a.list.First(); b != null; b = b.Next())
                len += 1 + DataLength(cx, b.value());
            return len;
        }
        int MultisetLength(Context cx, TMultiset m)
        {
            int len = 4 + StringLength("MULTISET") + TypeLength(cx._Dom(m.dataType.elType));
            for (var e = m.First(); e != null; e = e.Next())
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
        int SchemaLength(Context cx, RowSet r)
        {
            int len = 5;
            var dm = cx._Dom(r);
            int m = dm.display;
            if (m > 0)
            {
                len += StringLength(cx.obs[r.defpos]?.infos[cx.role.defpos]?.name);
                int[] flags = new int[m];
                r.Schema(cx, flags);
                var j = 0;
                for (var b = dm.representation.First(); b != null; b = b.Next(), j++)
                {
                    var d = cx._Dom(b.value());
                    len += StringLength(((SqlValue)cx.obs[b.key()]).name)
                        + TypeLength(d);
                }
            }
            return len;
        }
        /// <summary>
        /// Send a row of POST results to the client
        /// </summary>
        internal void PutCur(TableRow rec, Domain dt)
        {
            tcp.Write(Responses.CellData);
            for (var b = dt.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var d = cx._Dom(b.value());
                if (rec?.vals[p] is TypedValue c)
                    tcp.PutCell(cx, c.dataType, c);
                else
                    tcp.PutCell(cx, d, TNull.Value);
            }
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
                {
                    tcp.PutString(DBObject.Uid(tb.key()));
                    tcp.PutLong(b.key());
                    tcp.PutLong(b.value());
                }
        }
    }
	/// <summary>
	/// The Client Listener for the PyrrhoDBMS.
    /// The Main entry point is here
	/// </summary>
	class PyrrhoStart
	{
        internal static List<PyrrhoServer> connections = new List<PyrrhoServer>();
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
        /// <summary>
        /// During configure the service must be running, but it is not ready for all messages
        /// </summary>
        internal static ServerStatus state = ServerStatus.Open;
        /// <summary>
        /// a TCP listener for the Pyrrho service
        /// </summary>
		static TcpListener tcp;
        public static string host = "::1";
        public static string hostname = "localhost";
        public static int port = 5433;
        internal static bool VerboseMode = false, TutorialMode = false, DebugMode = false, 
            HTTPFeedbackMode = false,ShowPlan = false;
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
                        case 'p': port = int.Parse(args[k].Substring(3)); break;
                        case 'h': host = args[k].Substring(3); break;
                        case 'n': hostname = args[k].Substring(3); break;
                        case 'd':
                            path = args[k].Substring(3);
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
                        p = int.Parse(args[k].Substring(3));
                    switch (args[k][1])
                    {
                        case 's': httpport = (p < 0) ? 8180 : p; break;
                        case 'S': httpsport = (p < 0) ? 8133 : p; break;
                        case 'R': ShowPlan = true; break;
                    }
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
            if (path.Contains("/") && !path.EndsWith("/"))
                path += "/";
            else if (path.Contains("\\") && !path.EndsWith("\\"))
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
            Console.WriteLine("   -H  Show feedback on HTTP RESTView operations");
            Console.WriteLine("   +R  Show evaluation plan");
            Console.WriteLine("   -V  Verbose mode");
            Console.WriteLine("   -T  Tutorial mode");
		}
        /// <summary>
        /// Version information
        /// </summary>
 		internal static string[] Version = new string[]
        {
            "Pyrrho DBMS (c) 2022 Malcolm Crowe and University of the West of Scotland",
            "7.01alpha","(18 Nov 2022)", "http://www.pyrrhodb.com"
        };
	}
}