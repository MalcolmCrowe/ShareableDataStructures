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
            var rdr = asy.rbuf;
            int p = -1;
            try
            {
                var fn = rdr.GetString();
                db = SDatabase.Open(path,fn);
                asy.Write(Types.Done);
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
                    asy.Write(Types.Exception);
                    asy.PutString(e.Message);
                    asy.Flush();
                }
                catch (Exception) { }
                goto _return;
            }
            // start a Strong protocol service
            for (; ;)
            {
                p = -1;
                try
                {
                    p = rdr.ReadByte();
                } catch(Exception)
                {
                    p = -1;
                }
                if (p < 0)
                    goto _return;
                try
                {
                    switch ((Types)p)
                    {
                        case Types.DescribedGet:
                        case Types.Get:
                            {
                                var tr = db.Transact();
                                var qy = rdr._Get(tr) as SQuery ??
                                    throw new Exception("Bad query");
                                RowSet rs = qy.RowSet(tr,qy,SDict<long,SFunction>.Empty,Context.Empty);
                                var sb = new StringBuilder("[");
                                var cm = "";
                                for (var b = rs?.First();b!=null;b=b.Next())
                                {
                                    sb.Append(cm); cm = ",";
                                    ((RowBookmark)b)._ob.Append(db,sb);
                                }
                                sb.Append(']');
                                asy.Write(Types.Done);
                                if ((Types)p == Types.DescribedGet)
                                {
                                    asy.PutInt(rs._qry.Display.Length??0);
                                    for (var b = rs._qry.Display.First(); b != null; b = b.Next())
                                         asy.PutString(b.Value.Item2);
                                }
                                asy.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Types.SCreateTable:
                            {
                                var tr = db.Transact();
                                var tn = rdr.GetString();// table name
                                if (tr.names.Contains(tn))
                                    throw new Exception("Duplicate table name " + tn);
                                var tb = new STable(tr, tn);
                                tr = (STransaction)tr.Install(tb,tr.curpos); 
                                var n = rdr.GetInt(); // #cols
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = rdr.GetString(); // column name
                                    var dt = (Types)rdr.ReadByte(); // dataType
                                    tr = (STransaction)tr.Install(new SColumn(tr,cn,dt,tb.uid),tr.curpos);
                                }
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SInsert:
                            {
                                var tr = db.Transact();
                                tr = SInsertStatement.Get(db,rdr).Obey(tr);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.Insert:
                            {
                                var tr = db.Transact();
                                var tb = (STable)tr.names.Lookup(rdr.GetString()); // table name
                                var n = rdr.GetInt(); // # named cols
                                var cs = SList<long>.Empty;
                                Exception? ex = null;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = rdr.GetString();
                                    if (tb.names.Lookup(cn) is SColumn sc)
                                        cs += (sc.uid, i);
                                    else
                                        ex = new Exception("Column " + cn + " not found");
                                }
                                var nc = rdr.GetInt(); // #cols
                                if ((n==0 && nc!=tb.cpos.Length) || (n!=0 && n!=nc))
                                    throw new Exception("Wrong number of columns");
                                var nr = rdr.GetInt(); // #records
                                for (var i = 0; i < nr; i++)
                                {
                                    var f = SDict<long, Serialisable>.Empty;
                                    if (n == 0)
                                        for (var b = tb.cpos.First(); b!= null; b = b.Next())
                                        {
                                            if (b.Value.Item2 is SColumn sc)
                                                f += (sc.uid, rdr._Get(tr)); // serialisable values
                                        }
                                    else
                                        for (var b = cs; b.Length != 0; b = b.next)
                                            f += (b.element, rdr._Get(tr)); // serialisable values
                                    tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f),tr.curpos);
                                }
                                if (ex != null)
                                    throw ex;
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                          case Types.SAlter:
                            {
                                var tr = db.Transact();
                                var tn = rdr.GetString(); // table name
                                var tb = (STable)tr.names.Lookup(tn) ??
                                    throw new Exception("Table " + tn + " not found");
                                var cn = rdr.GetString(); // column name or ""
                                var nm = rdr.GetString(); // new name
                                if (cn.Length == 0)
                                    tr = (STransaction)tr.Install(new SAlter(tr, nm, Types.STable, tb.uid, 0), tr.curpos);
                                else
                                    tr = (STransaction)tr.Install(new SAlter(tr, nm, Types.SColumn, tb.uid,
                                            (tb.names.Lookup(cn) is SColumn sc) ? sc.uid :
                                            throw new Exception("Column " + cn + " not found")), tr.curpos);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SDrop:
                            {
                                var tr = db.Transact();
                                var nm = rdr.GetString(); // object name
                                var pt = tr.names.Lookup(nm) ??
                                    throw new Exception("Object " + nm + " not found");
                                var cn = rdr.GetString();
                                tr = (STransaction)tr.Install(
                                    (cn.Length == 0) ?
                                        new SDrop(tr, pt.uid, -1) :
                                        new SDrop(tr,
                                            (((STable)pt).names.Lookup(cn) is SColumn sc) ? sc.uid :
                                            throw new Exception("Column " + cn + " not found"),
                                        pt.uid), tr.curpos);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SCreateIndex:
                            {
                                var tr = db.Transact();
                                var tn = rdr.GetString(); // table name
                                var tb = (STable)tr.names.Lookup(tn) ??
                                    throw new Exception("Table " + tn + " not found");
                                var xt = rdr.ReadByte();
                                var rn = rdr.GetString();
                                var ru = (rn.Length==0)?-1: (tr.names.Lookup(rn)?.uid ??
                                    throw new Exception("Table " + tn + " not found"));
                                var nc = rdr.GetInt();
                                var cs = SList<long>.Empty;
                                for (var i=0;i<nc;i++)
                                {
                                    var cn = rdr.GetString();
                                    var se = (tb.names.Lookup(cn) is SColumn sc)? sc.uid :
                                        throw new Exception("Column " + cn + " not found");
                                    cs += (se, i);
                                }
                                tr = (STransaction)tr.Install(new SIndex(tr, tb.uid, xt < 2, ru,cs),tr.curpos);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.Read:
                            {
                                var id = rdr.GetLong();
                                var sb = new StringBuilder();
                                db.Get(id).Append(db,sb);
                                asy.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Types.SUpdateSearch:
                            {
                                var tr = db.Transact();
                                tr = SUpdateSearch.Get(db, rdr).Obey(tr,Context.Empty);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SUpdate:
                            {
                                var tr = db.Transact();
                                var id = rdr.GetLong();
                                var rc = db.Get(id);
                                var tb = (STable)tr.objects[rc.table]; 
                                var n = rdr.GetInt(); // # cols updated
                                var f = SDict<string, Serialisable>.Empty;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = rdr.GetString();
                                    f += (cn, rdr._Get(db));
                                }
                                tr = (STransaction)tr.Install(new SUpdate(tr, rc, f),tr.curpos);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SDeleteSearch:
                            {
                                var tr = db.Transact();
                                tr = SDeleteSearch.Get(db, rdr).Obey(tr,Context.Empty);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SDelete:
                            {
                                var tr = db.Transact();
                                var id = rdr.GetLong();
                                var rc = db.Get(id) as SRecord ??
                                    throw new Exception("Record " + id + " not found");
                                tr = (STransaction)tr.Install(new SDelete(tr, rc.table,rc.uid),tr.curpos);
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Types.Done);
                                asy.Flush();
                                break;
                            }
                        case Types.SBegin:
                            db = new STransaction(db, false);
                            asy.WriteByte((byte)Types.Done);
                            asy.Flush();
                            break;
                        case Types.SRollback:
                            db = db.Rollback();
                            asy.WriteByte((byte)Types.Done);
                            asy.Flush();
                            break;
                        case Types.SCommit:
                            {
                                var tr = db as STransaction ??
                                    throw new Exception("No transaction to commit");
                                db = tr.Commit();
                                asy.WriteByte((byte)Types.Done);
                                asy.Flush();
                                break;
                            }
                        default:
                            throw new Exception("Protocol error");
                    }
                }
                catch (SocketException)
                {
                    return;
                }
                catch (Exception e)
                {
                    try
                    {
                        db = db.Rollback();
           //             db.result = null;
                        asy.StartException();
                        asy.Write(Types.Exception);
                        asy.PutString(e.Message);
                         asy.Flush();
                    }
                    catch (Exception) { }
                }
            }
        _return:;
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
                throw new Exception("Cannot open a port on " + host);
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
                bad: throw new Exception("Cannot access path " + StrongServer.path);
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
    "0.0"," (2 February 2019)", " github.com/MalcolmCrowe/ShareableDataStructures"
};
    }
    public class ServerStream :StreamBase
    {
        internal Socket client;
        internal int rx = 0;
        internal Reader rbuf;
        bool exception = false;
        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override long Length => 0;

        public override long Position { get => 0; set => throw new NotImplementedException(); }

        internal ServerStream(Socket c)
        {
            client = c;
            rbuf = new SocketReader(this);
            wbuf = new Buffer(this);
            wbuf.wpos = 2;
            rbuf.pos = 2;
            rbuf.buf.len = 0;
        }

        public override void Flush()
        {
            if (wbuf==null || wbuf.wpos == 2)
                return;
            // now always send bSize bytes (not wcount)
            if (exception) // version 2.0
                unchecked
                {
                    exception = false;
                    wbuf.buf[0] = (byte)((Buffer.Size - 1) >> 7);
                    wbuf.buf[1] = (byte)((Buffer.Size - 1) & 0x7f);
                    wbuf.wpos -= 4;
                    wbuf.buf[2] = (byte)(wbuf.wpos >> 7);
                    wbuf.buf[3] = (byte)(wbuf.wpos & 0x7f);
                }
            else
            {
                wbuf.wpos -= 2;
                wbuf.buf[0] = (byte)(wbuf.wpos >> 7);
                wbuf.buf[1] = (byte)(wbuf.wpos & 0x7f);
            }
            try
            {
                client.Send(wbuf.buf, Buffer.Size,SocketFlags.None);
                wbuf.wpos = 2;
            }
            catch (Exception)
            {
               Console.WriteLine("Socket Exception reported on Flush");
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Get a byte from the stream: if necessary refill the buffer from the network
        /// </summary>
        /// <returns>the byte</returns>
        public override bool GetBuf(Buffer b)
        {
            var rcount = 0;
            rx = 0;
            try
            {
                var rc = client.Receive(b.buf, Buffer.Size, 0);
                if (rc == 0)
                {
                    rcount = 0;
                    return false;
                }
                rcount = (b.buf[0] << 7) + b.buf[1];
                b.len = rcount+2;
                return rcount > 0;
            }
            catch (SocketException)
            {
                return false;
            }
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            int j;
            for (j = 0; j < count; j++)
            {
                int x = ReadByte();
                if (x < 0)
                    break;
                buffer[offset + j] = (byte)x;
            }
            return j;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        protected override void PutBuf(Buffer b)
        {
            Flush();
        }
        internal void StartException()
        {
            rbuf.pos = rbuf.buf.len;
            if (wbuf!=null)
                wbuf.wpos = 4;
            exception = true;
        }
        public void Write(Types p)
        {
            WriteByte((byte)p);
        }
    }

}
