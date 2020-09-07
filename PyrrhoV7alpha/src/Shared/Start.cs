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
using System.Security.Principal;
using System.Security.AccessControl;
using System.Diagnostics;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho
{
#if !EMBEDDED
    internal enum ServerStatus { Open, Store, Master, Server };
#endif
    /// <summary>
    /// The Pyrrho DBMS Server process: deals with a single connection from a client
    /// and exits when the connection is closed. 
    /// There is a private PyrrhoServer instance for each thread.
    /// Communication is by asynchronous TCP transport, from a PyrrhoLink client or another server. 
    /// For HTTP communication see HttpService.cs
    /// </summary>
    internal class PyrrhoServer
	{
#if !EMBEDDED
        /// <summary>
        /// The client socket
        /// </summary>
        Socket client;
        /// <summary>
        /// the Pyrrho protocol stream for this client
        /// </summary>
		internal TCPStream tcp;
		Database db;
        Context cx;
        Cursor rb; 
        internal bool lookAheadDone = true, more = true;
        public DateTime lastop = DateTime.Now;
        public Thread myThread = null;
        internal const long Preparing = 0x7000000000000000;
        private int nextCol = 0;
        private TypedValue nextCell = null;
        // uid range for prepared statements is 0x7000000000000000-0x7fffffffffffffff
        private BTree<string, PreparedStatement> prepared = BTree<string, PreparedStatement>.Empty;
        static int _cid = 0;
        int cid = _cid++;
        static long CheckProtocol = 0;
        /// <summary>
        /// Constructor: called on Accept
        /// </summary>
        /// <param name="c">the newly connected Client socket</param>
		public PyrrhoServer(Socket c)
		{
			client = c;
		}
        /// <summary>
        /// The main routine started in the thread for this client. This contains a protcol loop
        /// </summary>
        public void Server()
        {
            //     client.Blocking = false;
            // process the connection string
            tcp = new TCPStream();
            myThread = Thread.CurrentThread;
            tcp.client = client;
            string user = null;
            int p = -1;
            try
            {
                var cs = GetConnectionString(tcp);
                var fn = cs["Files"];
                user = cs["User"];
                db = Database.Get(fn);
                if (db == null)
                {
                    var fp = PyrrhoStart.path + fn;
                    if (!File.Exists(fp))
                    {
                        var fs = new FileStream(fp,
                        FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                        var wr = new Writer(null, fs);
                        wr.PutInt(777);
                        wr.PutInt(51);
                        wr.PutBuf();
                        fs.Close();
                        //                  File.SetAccessControl(fp, PyrrhoStart.arule);
                    }
                    db = new Database(fn, new FileStream(fp,
                        FileMode.Open, FileAccess.ReadWrite, FileShare.None));
                }
                db = db.Load();
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
                    for (var i = e.info.First();i!= null;i=i.Next())
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
                lastop = DateTime.Now;
                try
                {
                    switch ((Protocol)p)
                    {
                        case Protocol.ExecuteNonQuery: //  SQL service
                            {
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId,cmd);
                                var tr = db;
                                long t=0;
                                cx = new Context(db);
                                db = new Parser(cx).ParseSql(cmd,Domain.Content);
                                cx.db = (Transaction)db;
                                var tn = DateTime.Now.Ticks;
                                if (PyrrhoStart.DebugMode && tn>t)
                                    Console.WriteLine(""+(tn- t));
                                if (db is Transaction td) // the SQL might or might not have been a Commit
                                {
                                    db = td + (Transaction.ReadConstraint, cx.rdC);// +(Transaction.Domains, cx.db.types);
                                    tcp.PutWarnings(td);
                                }
                                tr = db;
                                db = db.RdrClose(cx);
                                tcp.Write(Responses.Done);
                                tcp.PutInt(db.AffCount(tr.Affected()));
                                break;
                            }
                        case Protocol.ExecuteNonQueryTrace: //  SQL service with trace
                            {
                                var cmd = tcp.GetString();
                                db = db.Transact(db.nextId,cmd);
                                var tr = db;
                                long t = 0;
                                cx = new Context(db);
                                var ts = db.loadpos;
                                db = new Parser(db).ParseSql(cmd,Domain.Content);
                                cx.db = (Transaction)db;
                                var tn = DateTime.Now.Ticks;
                                if (PyrrhoStart.DebugMode && tn > t)
                                    Console.WriteLine("" + (tn - t));

                                if (db is Transaction td) // the SQL might or might not have been a Commit
                                {
                                    db = td + (Transaction.ReadConstraint, cx.rdC);// + (Transaction.Domains,cx.db.types);
                                    tcp.PutWarnings(td);
                                }
                                tr = db;
                                db = db.RdrClose(cx);
                                tcp.Write(Responses.DoneTrace);
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                tcp.PutInt(db.AffCount(tr.Affected()));
                                break;
                            }
                        // close the reader
                        case Protocol.CloseReader:
                            {
                                db = db.RdrClose(cx);
                                rb = null;
                                break;
                            }
                        // start a new transaction
                        case Protocol.BeginTransaction:
                            {
                                var tr = db.Transact(db.nextId,"",false);
                                db = tr;
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Begin Transaction " + (db as Transaction).uid);
                                break;
                            }
                        // commit
                        case Protocol.Commit:
                            {
                                var tr = db as Transaction;
                                if (tr == null)
                                    throw new DBException("25000").Mix();
                                db = db.Commit(cx);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Commit Transaction " + tr.uid);
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.CommitTrace:
                            {
                                var tr = db as Transaction;
                                var ts = db.loadpos;
                                if (tr == null)
                                    throw new DBException("25000").Mix();
                                db = db.Commit(cx);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("Commit Transaction " + tr.uid);
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.DoneTrace);
                                tcp.PutInt(db.AffCount(tr.Affected()));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                tcp.Flush();
                                break;
                            }
                        // rollback
                        case Protocol.Rollback: 
                            if (PyrrhoStart.DebugMode)
                                Console.WriteLine("Rollback on Request " + (db as Transaction).uid);
                            db = db.Rollback(new DBException("40000").ISO());
                            tcp.Write(Responses.Done);
                            break;
                        // close the connection
                        case Protocol.CloseConnection: 
                            Close();
                            goto _return;
                        // Get names of local databases
                        case Protocol.GetFileNames:
                            tcp.PutFileNames(); break;
                        // set authority for the connection
                        case Protocol.Authority:
                            {
                                var au = tcp.GetString();
                                tcp.Write(Responses.Done); tcp.Flush();
                                break;
                            }                        // set the current reader
                        case Protocol.ResetReader:
                            rb = cx.result.First(cx);
                            tcp.Write(Responses.Done);
                            tcp.Flush(); break;
                        case Protocol.ReaderData:
                            ReaderData();
                            tcp.Flush(); break;
                        case Protocol.TypeInfo:
                            {
                                string dts = "";
                                db = db.Transact(db.nextId,"");
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
                                var tr = db.Transact(db.nextId, sql);
                                tr+=(Database._ExecuteStatus,ExecuteStatus.Prepare);
                                cx = new Context(tr);
                                db = new Parser(cx).ParseSql(sql,Domain.Content);
                                cx.db = (Transaction)db;
                                tcp.PutWarnings(tr);
                                cx.unLex = true;
                                prepared += (nm, new PreparedStatement(cx));
                                db += (Database.NextPrep, cx.nextHeap);
                                cx.result = null;
                                db = db.RdrClose(cx);
                                tcp.Write(Responses.Done);
                                break;
                            }
                        case Protocol.Execute: // v7 Prepared statement API
                            {
                                var nm = tcp.GetString();
                                var n = tcp.GetInt();
                                var sb = new StringBuilder();
                                for (var i=0;i<n;i++)
                                {
                                    sb.Append(tcp.GetString());
                                    sb.Append(';');
                                }
                                if (!prepared.Contains(nm))
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                var tr = db.Transact(db.nextId, cmp);
                                cx = new Context(tr);
                                db = new Parser(cx).ParseSql(prepared[nm],cmp);
                                cx.db = (Transaction)db;
                                tr = (Transaction)db;
                                tcp.PutWarnings(tr);
                                if (cx.result == null)
                                {
                                    db = db.RdrClose(cx);
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt(db.AffCount(tr.Affected()));
                                }
                                else
                                {
                                    tcp.PutSchema(cx);
                                    rb = cx.result.First(cx);
                                    while (rb != null && rb.IsNull)
                                        rb = rb.Next(cx);
                                    nextCol = 0;
                                }
                                break;
                            }
                        case Protocol.ExecuteTrace: // v7 Prepared statement API
                            {
                                var nm = tcp.GetString();
                     //           Debug(0, "Execute");
                                var n = tcp.GetInt();
                                var sb = new StringBuilder();
                                for (var i = 0; i < n; i++)
                                {
                                    sb.Append(tcp.GetString());
                                    sb.Append(';');
                                }
                                if (!prepared.Contains(nm))
                                    throw new DBException("33000", nm);
                                var cmp = sb.ToString();
                                var tr = db.Transact(db.nextId, cmp);
                                var ts = db.loadpos;
                                cx = new Context(tr);
                                db = new Parser(cx).ParseSql(prepared[nm], cmp);
                                cx.db = (Transaction)db;
                                tcp.PutWarnings(tr);
                                if (cx.result == null)
                                {
                                    tr = (Transaction)db;
                                    db = db.RdrClose(cx);
                                    tcp.Write(Responses.DoneTrace);
                                    tcp.PutLong(ts);
                                    tcp.PutLong(db.loadpos);
                                    tcp.PutInt(db.AffCount(tr.Affected()));
                                }
                                else
                                    tcp.PutSchema(cx);
                                if (tracing)
                                    Debug(2, "Done");
                                break;
                            }
                        case Protocol.ExecuteReader: // ExecuteReader
                            {
                                if (rb != null)
                                    throw new DBException("2E202").Mix();
                                nextCol = 0; // discard anything left over from ReaderData
                                var cmd = tcp.GetString();
                                var tr = db.Transact(db.nextId,cmd);
                                cx = new Context(tr);
                     //           Console.WriteLine(cmd);
                                db = new Parser(cx).ParseSql(cmd,Domain.Content);
                                cx.db = (Transaction)db;
                                var tn = DateTime.Now.Ticks;
                                //                if (PyrrhoStart.DebugMode && tn>t)
                                //                    Console.WriteLine(""+(tn- t));
                                tr = (Transaction)db + (Transaction.ReadConstraint, cx.rdC);// +(Transaction.Domains,cx.db.types);
                                tcp.PutWarnings(tr);
                                if (cx.result is RowSet rs)
                                {
                                    tcp.PutSchema(cx);
                                    rb = rs.First(cx);
                                    while (rb != null && rb.IsNull)
                                        rb = rb.Next(cx);
                                }
                                else
                                {
                   //                 Console.WriteLine("no data");
                                    db = db.RdrClose(cx);
                                    tcp.Write(Responses.Done);
                                    tcp.PutInt(db.AffCount(tr.Affected()));
                                }
                                break;
                            }

                        // 5.0 allow continue after interactive error
                        case Protocol.Mark:
                            {
                                db = db.Transact(db.nextId,"");
                                var t = db as Transaction;
                                if (t!=null)
                                    t+=(Transaction._Mark, t);
                            }
                            break;

                        case Protocol.Get: // GET rurl
                            {
                                string[] path = tcp.GetString().Split('/');
                                db = db.Transact(db.nextId,"");
                                cx = new Context(db);
                                db.Execute(db.role, "G",path, 1, "");
                                var tr = (Transaction)db;
                                tcp.PutWarnings(tr);
                                if (cx.result is RowSet rs)
                                {
                                    tcp.PutSchema(cx);
                                    rb = rs.First(cx);
                                }
                                else
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(cx);
                                }
                                break;
                            }
                        case Protocol.Get2: // GET rurl version for weakly-typed languages
                            {
                                string[] path = tcp.GetString().Split('/');
                                db = db.Transact(db.nextId,"");
                                var tr = (Transaction)db;
                                cx = new Context(tr);
                                db.Execute(tr.role, "G", path, 1, "");
                                tcp.PutWarnings(tr);
                                if (cx.result is RowSet rs)
                                {
                                    tcp.PutSchema1(cx,rs);
                                    rb = rs.First(cx);
                                }
                                else
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(cx);
                                }
                                break;
                            }
                        case Protocol.GetInfo: // for a table or structured type name for database[0]
                            {
                                string tname = tcp.GetString();
                                db = db.Transact(db.nextId,"");
                                var tr = (Transaction)db;
                                tcp.PutWarnings(tr);
                                var tb = tr.GetObject(tname) as Table;
                                if (tb == null)
                                {
                                    rb = null;
                                    tcp.Write(Responses.NoData);
                                    db = db.RdrClose(cx);
                                }
                                else
                                {
                                    var rt = tr.role.infos[tb.defpos] as ObInfo;
                                    tcp.PutColumns(db,rt.domain);
                                }
                                break;
                            }
                        case Protocol.Post:
                            {
                                var tr = db.Transact(db.nextId,"");
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                tr+=(Database.SchemaKey,k);
                                var s = tcp.GetString();
                                cx = new Context(tr);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("POST "+s);
                                cx = new Parser(tr).ParseSqlInsert(cx, s);
                                tr = cx.tr;
                                var trs = cx.result as TransitionRowSet;
                                tcp.PutWarnings(tr);
                                tr = cx.tr;
                                tcp.PutSchema(cx);
                                var rec = rb.Rec();
                                var dt = rb.dataType;
                                rb = null;
                                db = tr.RdrClose(cx);
                                PutCur(rec,dt);
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Put:
                            {
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                var s = tcp.GetString();
                                var tr = db.Transact(db.nextId, s)+(Database.SchemaKey,k);
                                cx = new Context(tr);
                                if (PyrrhoStart.DebugMode)
                                    Console.WriteLine("PUT "+s);
                                cx = new Parser(tr).ParseSqlUpdate(cx, s);
                                tr = cx.tr;
                                var rs = cx.result;
                                if (rs != null)
                                    rb = rs.First(cx);
                                tcp.PutWarnings(tr);
                                tcp.PutSchema(cx);
                                var rec = rb.Rec();
                                var dt = rb.dataType;
                                db = tr.RdrClose(cx);
                                PutCur(rec,dt);
                                rb = null;
                                tcp.Write(Responses.Done);
                                tcp.Flush();
                                break;
                            }
                        case Protocol.Get1:
                            {
                                var tr = db.Transact(db.nextId,"");
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                tr += (Database.SchemaKey,k);
                                db = tr;
                                goto case Protocol.Get;
                            }
                        case Protocol.Delete:
                            {
                                var tr = db.Transact(db.nextId,"");
                                var k = tcp.GetLong();
                                if (k == 0) k = 1; // someone chancing it?
                                tr+=(Database.SchemaKey,k);
                                db = tr;
                                goto case Protocol.ExecuteNonQuery;
                            }
                        case Protocol.Rest:
                            {
                                var tr = db.Transact(db.nextId,"");
                                cx = new Context(tr);
                                var vb = tcp.GetString();
                                var url = tcp.GetString();
                                var jo = tcp.GetString();
                                tr.Execute(cx,vb, "R", url.Split('/'), "application/json", jo, "");
                                tcp.PutWarnings(tr);
                                db = tr.RdrClose(cx);
                                rb = null;
                                tcp.PutSchema(cx);
                                break;
                            }
                        case Protocol.CommitAndReport:
                            {
                                var tr = db as Transaction;
                                if (tr == null)
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.TransactionReport);
                                PutReport(tr);
                                break;
                            }
                        case Protocol.CommitAndReport1:
                            {
                                var tr = db as Transaction ??
                                    throw new DBException("25000").Mix();
                                var n = tcp.GetInt();
                                for (int i = 0; i < n; i++)
                                {
                                    var pa = tcp.GetString();
                                    var d = tcp.GetLong();
                                    var o = tcp.GetLong();
                                }
                                db = db.Commit(cx);
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.TransactionReport);
                                tcp.PutInt(db.AffCount(tr.Affected()));
                                PutReport(tr);
                                break;
                            }
                        case Protocol.CommitAndReportTrace:
                            {
                                var tr = db as Transaction ??
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
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                PutReport(tr);
                                break;
                            }
                        case Protocol.CommitAndReportTrace1:
                            {
                                var tr = db as Transaction;
                                if (db as Transaction == null)
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
                                tcp.PutWarnings(tr);
                                tcp.Write(Responses.TransactionReportTrace);
                                tcp.PutInt(db.AffCount(tr.Affected()));
                                tcp.PutLong(ts);
                                tcp.PutLong(db.loadpos);
                                CheckProtocol = db.loadpos;
                                PutReport(tr);
                                break;
                            }
                        case 0: goto case Protocol.EoF;
                        case Protocol.EoF: Close();
                            goto _return; // eof on stream
                        default: throw new DBException("3D005").ISO();
                    }
                }
                catch (DBException e)
                {
                    try
                    {
                        db = db.Rollback(e);
                        cx.data = BTree<long,RowSet>.Empty;
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
                        if (PyrrhoStart.DebugMode || PyrrhoStart.TutorialMode)
                        {
                            Console.Write("Exception " + e.Message);
                            foreach (var o in e.objects)
                                Console.Write(" " + o.ToString());
                            Console.WriteLine();
                        }
                    }
                    catch (Exception) { }
                }
                catch (SocketException e)
                {
                    db = db.Rollback(new DBException("00003", e.Message).Pyrrho());
                    goto _return;
                }
                catch (ThreadAbortException e)
                {
                    db = db.Rollback(new DBException("00004", e.Message).Pyrrho());
                    goto _return;
                }
                catch (Exception e)
                {
                    try
                    {
                        db = db.Rollback(e);
                        rb = null;
                        cx.data = BTree<long,RowSet>.Empty;
                        tcp.StartException();
                        tcp.Write(Responses.FatalError);
                        Console.WriteLine("Internal Error "+e.Message);
                        Console.WriteLine(e.StackTrace.Substring(0,80));
                        tcp.PutString(e.Message);
                    }
                    catch (Exception)
                    {
                        goto _return;
                    }
                    db = db.Rollback(new DBException("00005", e.Message).Pyrrho());
                }
            }
        _return: if (PyrrhoStart.TutorialMode)
                Console.WriteLine("(" + cid + ") Ends with " + p);
            tcp?.Close();
        }
        static DateTime startTrace;
        internal static bool tracing = false;
        internal static void Debug(int a,string m) // a=0 start, 1-continue, 2=stop
        {
            TimeSpan t; 
            switch(a)
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
                    Console.WriteLine(m + " " + t.TotalMilliseconds+" Stop " + m);
                    break;
            }
        }
        BTree<string,string> GetConnectionString(TCPStream tcp)
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
                    dets+=(str, tcp.crypt.GetString());
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
            if (db!=null)
				db.Rollback(new DBException("00000").ISO());
			tcp.Close();
            cx.data = BTree<long,RowSet>.Empty;
            rb = null;
			client.Close();
		}
        /// <summary>
        /// Send a block of data as part of a stream of rows
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
            if (rb.columns is BList<long> co)
                for (var b = co.First(); b != null; b = b.Next(), i++)
                    domains += (i, rb.dataType.representation[b.value()]);
            else
                for (var b = rb.dataType.representation.First(); b != null; b = b.Next(), i++)
                    domains += (i, b.value());
            var dc = domains[nextCol];
            nextCell = rb[nextCol++];
            if (nextCol == rb.Length)
                lookAheadDone = false;
      //      tcp.PutCheck(db);
            tcp.PutCell(cx,dc,nextCell);
            var dt = rb.dataType;
            for (; ; )
            {
                var lc = 0;
                if (nextCol == rb.display)
                {
                    if (!lookAheadDone)
                        for (rb = rb.Next(cx); rb != null && rb.IsNull; rb = rb.Next(cx))
                            ;
                    more = rb!=null;
                    lookAheadDone = true;
                    nextCol = 0;
                    if (!more)
                        break;
                }
                nextCell = rb[nextCol];
                int len = lc+DataLength(cx,nextCell);
                dc = domains[nextCol];
                if (nextCell != null && !dc.Equals(nextCell.dataType))
                {
                    var nm = rb.NameFor(cx,nextCol);
                    if (nm == null)
                        nm = nextCell.dataType.ToString();
                    len += 4+StringLength(nm); 
                }
                if (tcp.wcount + len + 1 >= TCPStream.bSize)
                    break;
                tcp.PutCell(cx,dc,nextCell);
                if (++nextCol == rb.display)
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
         int DataLength(Context cx,TypedValue tv)
        {
            if (tv == null)
                return 1;
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
                        return 1 + StringLength(o);
                    return 1 + StringLength(new Common.Numeric((double)o).DoubleFormat());
                case Sqlx.DATE:
                    return 9; // 1+long
                case Sqlx.TIME:
                    return 9;
                case Sqlx.TIMESTAMP:
                    return 9;
                case Sqlx.BLOB:
                    return 5+((byte[])o).Length;
                case Sqlx.ROW:
                    {
                        if (o is TRow r)
                            return 1 + RowLength(cx, r);
                        return 1 + RowLength(cx, ((RowSet)o).First(cx));
                    }
                case Sqlx.ARRAY:
                    return 1 + ArrayLength(cx,(TArray)tv);
                case Sqlx.MULTISET:
                    return 1 + MultisetLength(cx,(TMultiset)o);
                case Sqlx.TABLE:
                    return 1 + TableLength(cx,(RowSet)o);
                case Sqlx.INTERVAL:
                    return 10; // 1+ 1byte + (1long or 2xint)
                case Sqlx.TYPE:
                    {
                        var tn = tv.dataType.name;
                        return 1 + tn.Length + ((TRow)o).Length;
                    }
                  case Sqlx.XML: break;
            }
            return 1+StringLength(o);
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
        int RowLength(Context cx,Cursor r)
        {
            int len = 4;
            var dt = r.dataType;
            int n = dt.Length;
            for (int i = 0; i < n; i++)
            {
                var c = r[i];
                len += StringLength(r.NameFor(cx,i)) + TypeLength(c.dataType) 
                    + DataLength(cx,c);
            }
            return len;
        }
        int RowLength(Context cx,TRow v)
        {
            int len = 4;
            for (var b=v.columns.First();b!=null;b=b.Next())
            {
                var i = b.key();
                var p = b.value();
                len += StringLength(v.dataType.NameFor(cx, p, i)) 
                    + TypeLength(v.dataType.representation[p]) 
                    + DataLength(cx, v[i]);
            }
            return len;

        }
        int ArrayLength(Context cx,TArray a)
        {
            int len = 4 + StringLength("ARRAY") + TypeLength(a.dataType.elType);
            for(var b=a.list.First();b!=null;b=b.Next())
                len += 1+DataLength(cx,b.value());
            return len;
        }
        int MultisetLength(Context cx, TMultiset m)
        {
            int len = 4 + StringLength("MULTISET") + TypeLength(m.dataType.elType);
            for (var e = m.First(); e != null; e = e.Next())
                len += 1 + DataLength(cx, e.Value());
            return len;
        }
        int TableLength(Context _cx, RowSet r)
        {
            int len = 4 + StringLength("TABLE") + SchemaLength(_cx,r);
            for (var e = r.First(_cx); e != null; e = e.Next(_cx))
                len += RowLength(_cx, e);
            return len;
        }
        int SchemaLength(Context cx,RowSet r)
        {
            int len = 5;
            int m = r.display;
            if (m > 0)
            {
                len += StringLength(cx.obs[r.defpos]?.mem[Basis.Name]??"");
                int[] flags = new int[m];
                r.Schema(cx, flags);
                var j = 0;
                for (var b = r.domain.representation.First(); b != null; b = b.Next(),j++)
                {
                    var d = b.value();
                    len += StringLength(((SqlValue)cx.obs[b.key()]).name)
                        + TypeLength(d);
                }
            }
            return len;
        }
        /// <summary>
        /// Send a row of POST results to the client
        /// Here we use the fact that db.affected is a mutable List!
        /// </summary>
        internal void PutCur(TableRow rec,Domain dt)
        {
            var tr = (Transaction)db;
            tcp.Write(Responses.CellData);
            // TBD
            for (var b=dt.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var d = b.value();
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
        internal void PutReport(Transaction tr)
        {
            tcp.PutLong(tr.schemaKey);
            var aff = tr.Affected();
            tcp.PutInt((int)aff.Count);
            var dl = tr.name;
            for (var tb = aff.First(); tb != null; tb = tb.Next())
                for (var b = tb.value().First(); b != null; b = b.Next())
                {
                    tcp.PutLong(b.key());
                    tcp.PutLong(b.value());
                }
        }
#endif
        }
#if !EMBEDDED
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
        internal static FileSecurity arule;
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
        public static int port = 5433;
        internal static bool RepairMode = false, TutorialMode = false, FileMode = false, CheckMode = false,
            DebugMode = false, VerifyMode = false, StrategyMode = false, HTTPFeedbackMode = false;
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
#if !LOCAL
            int conns = 0;
#endif
            int cid = 0;
            for (; ; )
                try
                {
#if !LOCAL
                     while (connections.Count >= cfg.maxConnections)
                    {
                        conns = connections.Count;
                        Thread.Sleep(500);
                    }
#endif
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
#if !LOCAL
        internal static void Terminator()
        {
            for (;;)
            {
                Thread.Sleep(20000);
                DateTime when = DateTime.Now - cfg.timeout;
                lock (connections)
                    foreach (PyrrhoServer p in connections)
                        if (p.lastop < when)
                        {
                            Console.WriteLine("A connection timeout occurred");
                            p.myThread.Abort();
                        }
            }
        }
#endif
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
                if (j == 1 || j==2)
                    Console.Write(Version[j]);
                else
				    Console.WriteLine(Version[j]);
			int k = 0;
            int httpport = 0;
            int httpsport = 0;
            for (; args.Length > k; k++)
                if (args[k][0] == '-')
                    switch (args[k][1])
                    {
                        case 'p': port = int.Parse(args[k].Substring(3)); break;
                        case 'h': host = args[k].Substring(3); break;
                        case 'd':
                            path = args[k].Substring(3);
                            FixPath();
                            break;
#if !LOCAL
                    case 't': cfg.maxConnections = int.Parse(args[k].Substring(3)); break;
#endif
                        case 'R': RepairMode = true; break;
                        case 'T': TutorialMode = true; break;
                        case 'E': StrategyMode = true; break;
                        case 'F': FileMode = true; break;
                        case 'C': CheckMode = true; break;
                        case 'D': DebugMode = true; break;
                        case 'H': HTTPFeedbackMode = true; break;
                        case 'V': VerifyMode = true; break;
                        default: Usage(); return;
                    }
                else if (args[k][0] == '+')
                {
                    int p = -1;
                    if (args[k].Length > 2)
                        p = int.Parse(args[k].Substring(3));
                    if (args[k][1] == 's')
                        httpport = (p < 0) ? 8080 : p;
                    if (args[k][1] == 'S')
                        httpsport = (p < 0) ? 8133 : p;
                }
            arule = new FileSecurity();
            var administrators = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);
            var everyone = new SecurityIdentifier(WellKnownSidType.WorldSid, null);
            arule.AddAccessRule(new FileSystemAccessRule(everyone, FileSystemRights.FullControl,
                AccessControlType.Deny));
            arule.AddAccessRule(new FileSystemAccessRule(administrators, FileSystemRights.FullControl,
                AccessControlType.Allow));
            arule.AddAccessRule(new FileSystemAccessRule(WindowsIdentity.GetCurrent().User,
                FileSystemRights.FullControl, AccessControlType.Allow));
            if (RepairMode)
            {
                state = ServerStatus.Server;
                Console.WriteLine("Repair mode");
            }
            if (httpport > 0 || httpsport > 0)
                new Thread(new ThreadStart(new HttpService(host, httpport, httpsport).Run)).Start();
#if MONGO
            if (MongoService.port!=0)
                new Thread(new ThreadStart(MongoService.Run)).Start(); 
#endif
#if !LOCAL
            var te = new Thread(new ThreadStart(Terminator))
            {
                Name = "Terminator"
            };
            te.Start();
#endif
            Run();
		}
        static void FixPath()
        {
            if (path == "")
                return;
            if (path.Contains("/") && !path.EndsWith("/"))
                path += "/";
            else if (!path.EndsWith("\\"))
                path += "\\";
#if !MONO
            var acl = Directory.GetAccessControl(path);
            if (acl == null)
                goto bad;
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
            Console.WriteLine("Usage: "+serverName+" [-d:path] [-h:host] [-p:port] [-t:nn] [+s[:http]] [+S[:https]] {-flag}");
#endif
            Console.WriteLine("Parameters:");
            Console.WriteLine("   -d  Use the given folder for database storage");
            Console.WriteLine("   -h  Use the given host address. Default is 127.0.0.1.");
			Console.WriteLine("   -p  Listen on the given port. Default is 5433");
            Console.WriteLine("   +s[:port]  Start HTTP REST service on the given port (default 8080).");
            Console.WriteLine("   -t  Limit the number of connections to nnn");
            Console.WriteLine("   +S[:port]  Start HTTPS REST service on the given port (default 8133).");
            Console.WriteLine("Flags:");
#if !APPEND
            Console.WriteLine("   -C  Check mode for checksum verification");
#endif
            Console.WriteLine("   -E  Explain strategy for rowset evaluation");
            Console.WriteLine("   -F  Tutorial mode showing file operations");
            Console.WriteLine("   -H  Show feedback on HTTP RESTView operations");
#if (!LOCAL) && (!EMBEDDED)
            Console.WriteLine("   -R  Repair mode, disables automatic features of the configuration database");
            Console.WriteLine("   -T  Tutorial mode, showing server-server messages, pauses in execution");
#endif
            Console.WriteLine("   -V  Show progress of compiling SQL");
		}
        /// <summary>
        /// Version information
        /// </summary>
 		internal static string[] Version = new string[]
        {
            "Pyrrho DBMS (c) 2020 Malcolm Crowe and University of the West of Scotland",
            "7.0 alpha"," (7 September 2020)", " www.pyrrhodb.com https://pyrrhodb.uws.ac.uk"
        };
	}
}
