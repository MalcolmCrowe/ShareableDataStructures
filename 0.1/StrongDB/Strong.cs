using System;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Security.Principal;
using System.Security.AccessControl;
using Shareable;
#nullable enable

namespace StrongDB
{
    class StrongServer
    {
        /// <summary>
        /// The client socket
        /// </summary>
        Socket client;
        /// <summary>
        /// the Strong protocol stream for this client
        /// </summary>
		internal ServerStream asy;
        SDatabase db;
        static int _cid = 0;
        int cid = _cid++;
        public DateTime lastop = DateTime.Now;
        public static string path= "";
        /// <summary>
        /// Constructor: called on Accept
        /// </summary>
        /// <param name="c">the newly connected Client socket</param>
        public StrongServer(Socket c)
        {
            client = c;
        }
        /// <summary>
        /// The main routine started in the thread for this client. This contains a protcol loop
        /// </summary>
        public void Server()
        {
            // client.Blocking = false;
            // process the connection string
            asy = new ServerStream(client);
            var rdr = asy.rdr;
            var wtr = asy.wtr;
            int p = -1;
            try
            {
                var fn = rdr.GetString();
                db = SDatabase.Open(path, fn);
                wtr.Write(Types.Done);
                wtr.PutLong(0);
                wtr.PutLong(0);
                asy.Flush();
            }
            catch (IOException)
            {
                asy.Close();
                return;
            }
            catch (Exception e)
            {
                try
                {
                    asy.StartException();
                    wtr.Write(Types.Exception);
                    wtr.PutString(e.Message);
                    asy.Flush();
                }
                catch (Exception) { }
                goto _return;
            }
            // start a Strong protocol service
            for (; ; )
            {
                p = -1;
                try
                {
                    p = rdr.ReadByte();
                } catch (Exception)
                {
                    p = -1;
                }
                if (p < 0)
                    goto _return;
                try
                {
         //           Console.WriteLine("[" + cid + "] Start " + ((Types)p).ToString());
                    switch ((Types)p)
                    {
                        case Types.SNames:
                            {
                                var tr = db.Transact(rdr);
                                var us = db.uids;
                                for (var b = us.PositionAt(SysTable._SysUid); b != null && b.Value.Item1 < 0; b = b.Next())
                                    us -= b.Value.Item1;
                                var n = rdr.GetInt();
                                for (var i = 0; i < n; i++)
                                {
                                    var u = rdr.GetLong();
                                    var s = rdr.GetString();
                                    if (u < rdr.lastAlias)
                                        rdr.lastAlias = u;
                                    us += (u, s);
                                }
                                db = new STransaction(tr,new SRole(tr.role,us));
                                break;
                            }
                        case Types.DescribedGet:
                        case Types.Get:
                            {
                                var tr = db.Transact(rdr);
                                SQuery? qy = null;
                                try
                                {
                                    qy = rdr._Get() as SQuery;
                                    tr = (STransaction)rdr.db;
                                } catch (Exception e)
                                {
                                    rdr.buf.len = 0;
                                    throw e;
                                }
                                if (qy == null)
                                    throw new StrongException("Bad query");
                                qy = (SQuery)qy.Prepare(tr, qy.Names(tr,SDict<long,long>.Empty));
                                RowSet rs = qy.RowSet(tr, qy, Context.Empty);
                                var sb = new StringBuilder("[");
                                var cm = "";
                                for (var b = rs?.First(); b != null; b = b.Next())
                                    if (((RowBookmark)b)._ob is SRow sr && sr.isValue)
                                    {
                                        sb.Append(cm); cm = ",";
                                        sr.Append(rs._tr, sb);
                                    }
                                sb.Append(']');
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(rs._tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                if ((Types)p == Types.DescribedGet)
                                {
                                    var dp = rs._qry.Display;
                                    wtr.PutInt(dp.Length ?? 0);
                                    for (var b = dp.First(); b != null; b = b.Next())
                                        wtr.PutString(b.Value.Item2.Item2);
                                }
                                wtr.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Types.SCreateTable:
                            {
                                var tr = db.Transact(rdr);
                                var tn = db.role[rdr.GetLong()];// table name
                                if (db.role.globalNames.Contains(tn))
                                    throw new StrongException("Duplicate table name " + tn);
                                tr = (STransaction)tr.Install(new STable(tr), tn, tr.curpos);
                                rdr.db = tr;
                                var n = rdr.GetInt();
                                for (var i = 0; i < n; i++)
                                    CreateColumn(rdr);
                                n = rdr.GetInt();
                                for (var i = 0; i < n; i++)
                                {
                                    rdr.ReadByte();
                                    CreateIndex(rdr);
                                }
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit((STransaction)rdr.db);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SCreateColumn:
                            {
                                var tr = db.Transact(rdr);
                                rdr.db = tr;
                                CreateColumn(rdr);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit((STransaction)rdr.db);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SInsert:
                            {
                                var tr = db.Transact(rdr);
                                var tn = tr.role[rdr.GetLong()];
                                if (!db.role.globalNames.Contains(tn))
                                    throw new StrongException("Table " + tn + " not found");
                                var tb = (STable)db.objects[db.role.globalNames[tn]];
                                rdr.context = tb;
                                var n = rdr.GetInt();
                                var c = SList<long>.Empty;
                                for (var i = 0; i < n; i++)
                                    c += (rdr.GetLong(), i);
                                var ins = new SInsert(tb.uid, c, rdr);
                                tr = ins.Prepare(tr,tb.Names(tr,SDict<long,long>.Empty)).Obey(tr,Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.Insert:
                            {
                                var tr = db.Transact(rdr);
                                var tn = tr.role[rdr.GetLong()];
                                if (!db.role.globalNames.Contains(tn))
                                    throw new StrongException("Table " + tn + " not found");
                                var tb = (STable)db.objects[db.role.globalNames[tn]];
                                rdr.context = tb;
                                var n = rdr.GetInt(); // # named cols
                                var cs = SList<SColumn>.Empty;
                                Exception? ex = null;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = db.role[rdr.GetLong()];
                                    var ss = db.role.subs[tb.uid];
                                    if (ss.defs.Contains(cn))
                                        cs += ((SColumn)db.objects[ss.obs[ss.defs[cn]].Item1], i);
                                    else
                                        ex = new StrongException("Column " + cn + " not found");
                                }
                                var nc = rdr.GetInt(); // #cols
                                if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc))
                                    ex = new StrongException("Wrong number of columns");
                                var nr = rdr.GetInt(); // #records
                                for (var i = 0; i < nr; i++)
                                {
                                    var f = SDict<long, Serialisable>.Empty;
                                    if (n == 0)
                                        for (var b = tb.cpos.First(); b != null; b = b.Next())
                                        {
                                            if (b.Value.Item2 is SColumn sc)
                                                f += (sc.uid, rdr._Get());
                                        }
                                    else
                                        for (var b = cs; b.Length != 0; b = b.next)
                                            f += (b.element.uid, rdr._Get());
                                    tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f), tr.curpos);
                                }
                                if (ex != null)
                                    throw ex;
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SAlter:
                            {
                                var tr = db.Transact(asy.rdr);
                                rdr.db = tr;
                                var at = SAlter.Get(rdr);
                                tr = (STransaction)rdr.db;
                                tr = at.Prepare(tr, SDict<long, long>.Empty)
                                    .Obey(tr, Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SDrop:
                            {
                                var tr = db.Transact(asy.rdr);
                                var dr = SDrop.Get(rdr).Prepare(tr,SDict<long,long>.Empty);
                                tr = dr.Obey(tr, Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SIndex:
                            {
                                var tr = db.Transact(asy.rdr);
                                rdr.db = tr;
                                CreateIndex(rdr);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit((STransaction)rdr.db);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SDropIndex:
                            {
                                var tr = db.Transact(rdr);
                                rdr.db = tr;
                                var dr = new SDropIndex(rdr);
                                tr = (STransaction)rdr.db;
                                tr = dr.Prepare(tr, SDict<long, long>.Empty)
                                    .Obey(tr, Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.Read:
                            {
                                var id = rdr.GetLong();
                                var sb = new StringBuilder();
                                db.Get(id).Append(db,sb);
                                wtr.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Types.SUpdateSearch:
                            {
                                var tr = db.Transact(rdr);
                                var u = SUpdateSearch.Get(rdr);
                                tr = u.Prepare(tr,u.qry.Names(tr,SDict<long,long>.Empty)).Obey(tr,Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SUpdate:
                            {
                                var tr = db.Transact(rdr);
                                var id = rdr.GetLong();
                                var rc = db.Get(id);
                                var tb = (STable)tr.objects[rc.table]; 
                                var n = rdr.GetInt(); // # cols updated
                                var f = SDict<long, Serialisable>.Empty;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = rdr.GetLong();
                                    f += (cn, rdr._Get());
                                }
                                tr = (STransaction)tr.Install(new SUpdate(tr, rc, f),tr.curpos);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit((STransaction)rdr.db);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SDeleteSearch:
                            {
                                var tr = db.Transact(rdr);
                                var dr = SDeleteSearch.Get(rdr);
                                tr = dr.Prepare(tr,dr.qry.Names(tr,SDict<long,long>.Empty)).Obey(tr,Context.Empty);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SDelete:
                            {
                                var tr = db.Transact(rdr);
                                var id = rdr.GetLong();
                                var rc = db.Get(id) as SRecord ??
                                    throw new StrongException("Record " + id + " not found");
                                tr = (STransaction)tr.Install(new SDelete(tr, rc.table,rc.uid),rc,tr.curpos);
                                var ts = db.curpos;
                                db = db.MaybeAutoCommit(tr);
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        case Types.SBegin:
                            db = db.Transact(rdr, false);
                            wtr.Write(Types.Done);
                            wtr.PutLong(db.curpos);
                            wtr.PutLong(db.curpos);
                            asy.Flush();
                            break;
                        case Types.SRollback:
                            db = db.Rollback();
                            wtr.Write(Types.Done);
                            wtr.PutLong(db.curpos);
                            wtr.PutLong(db.curpos);
                            asy.Flush();
                            break;
                        case Types.SCommit:
                            {
                                var tr = db as STransaction ??
                                    throw new StrongException("No transaction to commit");
                                var ts = db.curpos;
                                db = tr.Commit();
                                wtr.Write(Types.Done);
                                wtr.PutLong(ts);
                                wtr.PutLong(db.curpos);
                                asy.Flush();
                                break;
                            }
                        default:
                            throw new StrongException("Protocol error");
                    }
         //           Console.WriteLine("[" + cid + "] End " + ((Types)p).ToString());
                }
                catch (SocketException)
                {
                    return;
                }
                catch (StrongException e)
                {
                    try
                    {
                        db = db.Rollback();
           //             db.result = null;
                        asy.StartException();
           //             Console.WriteLine(""+cid+" Reporting Exception: " + e.Message);
                        wtr.Write(Types.Exception);
                        wtr.PutString(e.Message);
                         asy.Flush();
                    }
                    catch (Exception) { }
                }
                catch (Exception e)
                {
                    try
                    {
                        db = db.Rollback();
                        //             db.result = null;
                        asy.StartException();
                        //             Console.WriteLine(""+cid+" Reporting Exception: " + e.Message);
                        wtr.Write(Types.Exception);
                        wtr.PutString(e.Message);
                        asy.Flush();
                    }
                    catch (Exception) { }
                }
            }
        _return:;
        }
        void CreateColumn(ReaderBase rdr)
        {
            var sc = (SColumn)rdr._Get();
            var db = (STransaction)rdr.db;
            sc = (SColumn)sc.Prepare(db, ((STable)db.objects[sc.table]).Names(db,SDict<long,long>.Empty));
            var cn = db.role[sc.uid];
            rdr.db = db.Install(new SColumn(db,sc.table,sc.dataType,sc.constraints), cn, db.curpos);
        }
        void CreateIndex(ReaderBase rdr)
        {
            var db = (STransaction)rdr.db;
            rdr.db = db.Install((SIndex)SIndex.Get(rdr).Prepare(db,SDict<long,long>.Empty), db.curpos);
        }
    }
        /// <summary>
        /// The Client Listener for the StrongDBMS.
        /// The Main entry point is here
        /// </summary>
    class StrongStart
    {
        internal static string host = "127.0.0.1";
        internal static int port = 50433;
        /// <summary>
        /// a TCP listener for the Strong service
        /// </summary>
		static TcpListener tcp;
        /// <summary>
        /// The main service loop of the StrongDBMS is here
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
                throw new StrongException("Cannot open a port on " + host);
            Console.WriteLine("StrongDBMS protocol on " + host + ":" + port);
            if (StrongServer.path != "")
                Console.WriteLine("Database folder " + StrongServer.path);
            int cid = 0;
            for (; ; )
                try
                {
                    Socket client = tcp.AcceptSocket();
                    var t = new Thread(new ThreadStart(new StrongServer(client).Server))
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
                if (j == 1 || j == 2)
                    Console.Write(Version[j]);
                else
                    Console.WriteLine(Version[j]);
            int k = 0;
            while (args.Length > k && args[k][0] == '-')
            {
                switch (args[k][1])
                {
                    case 'p': port = int.Parse(args[k].Substring(3)); break;
                    case 'h': host = args[k].Substring(3); break;
                    case 'd':
                        StrongServer.path = args[k].Substring(3);
                        FixPath();
                        break;
                    default: Usage(); return;
                }
                k++;
            }
            Run();
        }
        static void FixPath()
        {
            if (StrongServer.path == "")
                return;
            if (StrongServer.path.Contains("/") && !StrongServer.path.EndsWith("/"))
                StrongServer.path += "/";
            else if (!StrongServer.path.EndsWith("\\"))
                StrongServer.path += "\\";
            var acl = Directory.GetAccessControl(StrongServer.path);
            if (acl == null)
                goto bad;
            var acr = acl.GetAccessRules(true, true, typeof(SecurityIdentifier));
            if (acr == null)
                goto bad;
            foreach (FileSystemAccessRule r in acr)
                if ((FileSystemRights.Write & r.FileSystemRights) == FileSystemRights.Write
                    && r.AccessControlType == AccessControlType.Allow)
                    return;
                bad: throw new StrongException("Cannot access path " + StrongServer.path);
        }
        /// <summary>
        /// Provide help about the command line options
        /// </summary>
        static void Usage()
        {
            string serverName = "StrongDBMS";
            Console.WriteLine("Usage: " + serverName + " [-d:path] [-h:host] [-p:port] [-s:http] [-t:nn] [-S:https] {-flag}");
            Console.WriteLine("Parameters:");
            Console.WriteLine("   -d  Use the given folder for database storage");
            Console.WriteLine("   -h  Use the given host address. Default is 127.0.0.1.");
            Console.WriteLine("   -p  Listen on the given port. Default is 5433");
        }
        /// <summary>
        /// Version information
        /// </summary>
 		internal static string[] Version = new string[]
{
    "Strong DBMS (c) 2019 Malcolm Crowe and University of the West of Scotland",
    "0.1"," (1 May 2019)", " github.com/MalcolmCrowe/ShareableDataStructures"
};
    }
    public class ServerStream :Stream
    {
        internal Socket client;
        internal int rx = 0;
        internal ServerReader rdr;
        internal ServerWriter wtr;
        bool exception = false;
        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override long Length => 0;

        public override long Position { get => 0; set => throw new NotImplementedException(); }

        internal ServerStream(Socket c)
        {
            client = c;
            rdr = new ServerReader(client);
            wtr = new ServerWriter(client);
            rdr.buf.pos = 2;
            rdr.buf.len = 0;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
        internal void StartException()
        {
            rdr.buf.pos = rdr.buf.len;
            wtr.buf.pos = 4;
            wtr.exception = true;
        }
        public void Write(Types p)
        {
            WriteByte((byte)p);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            for (var i = 0; i < count; i++)
            {
                var b = ReadByte();
                if (b < 0)
                    return i;
                buffer[i] = (byte)b;
            }
            return count;
        }
        public override void Flush()
        {
            wtr.PutBuf();
            wtr.buf.pos = 2;
        }
    }
    public class ServerWriter : SocketWriter
    {
        public bool exception = false;
        public ServerWriter(Socket c) : base(c) { }
        public override void PutBuf()
        {
            if (buf.pos == 2)
                return;
            // now always send bSize bytes (not wcount)
            if (!exception) {// version 2.0
                base.PutBuf();
                return;
            }
            unchecked
            {
                exception = false;
                buf.buf[0] = (byte)((Buffer.Size - 1) >> 7);
                buf.buf[1] = (byte)((Buffer.Size - 1) & 0x7f);
                buf.pos -= 4;
                buf.buf[2] = (byte)(buf.pos >> 7);
                buf.buf[3] = (byte)(buf.pos & 0x7f);
            }
            try
            {
                client.Send(buf.buf, 0);
                buf.len = 0;
            }
            catch (Exception)
            {
                Console.WriteLine("Socket Exception reported on Flush");
            }
        }
    }
    public class ServerReader : SocketReader
    {
        public ServerReader(Socket c) : base(c) { }
    }
}
