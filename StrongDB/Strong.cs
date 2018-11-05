using System;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Security.Principal;
using System.Security.AccessControl;
using Shareable;

namespace StrongDB
{
    class StrongServer
    {
        /// <summary>
        /// The client socket
        /// </summary>
        Socket client;
        /// <summary>
        /// the Pyrrho protocol stream for this client
        /// </summary>
		internal AsyncStream asy;
        SDatabase db;
        static int _cid = 0;
        int cid = _cid++;
        static Random testlock = new Random();
        public DateTime lastop = DateTime.Now;
        public Thread myThread = null;
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
            asy = new AsyncStream(client);
            myThread = Thread.CurrentThread;
            int p = -1;
            try
            {
                var fn = asy.GetString();
         //       Console.WriteLine("Received " + fn);
                db = SDatabase.Open(path,fn);
         /*       if (fn.CompareTo("AsynTest")==0)
                    var rnd = new Random(0);
                for (var j = 0; j < 10; j++)
                    {

          //          Thread.Sleep(testlock.Next(1000, 5000));
                        lock (testlock)
                        {
          //                  asy.Flush();

                            var n = rnd.Next(10, 30);
                        Console.WriteLine("Receive group should be " + n);
                            for (var i = 0; i < n; i++)
                            {
                                var x = rnd.Next(1, 255);
                                var y = asy.ReadByte();
                                if (asy.rcount != n)
                                    Console.WriteLine("Buffer length " + asy.rcount + "!=" + n);
                                if (x != y)
                                    Console.WriteLine("Mismatch " + x + " vs " + y);
                            }
                            Console.WriteLine("Matched " + n);
                            var m = rnd.Next(10, 30);
                        Console.WriteLine("Send group will be " + m);
                            for (var i = 0; i < m; i++)
                            {
                                var x = rnd.Next(1, 255);
                                Console.Write(" " + x);
                                asy.WriteByte((byte)x);
                            }
                        asy.Flush();
                            Console.WriteLine(" Sent " + m);
                        }
                } */
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
                    asy.Write(Responses.Exception);
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
                    p = asy.ReadByte();
                } catch(Exception)
                {
                    p = -1;
                }
                if (p < 0)
                    goto _return;
                try
                {
                    switch ((Protocol)p)
                    {
                        case Protocol.Get:
                            {
                                var tb = STable.Get(db, asy);
                                var n = asy.GetInt();
                                var k = SCList<Variant>.Empty;
                                for (var i = 0; i < n; i++)
                                    k = k.InsertAt(new Variant(Variants.Single,asy._Get(db)), k.Length);
                                SIndex px = null;
                                for (var b = db.objects.First(); px == null && b != null; b = b.Next())
                                    if (b.Value.val is SIndex x && x.table == tb.uid)
                                        px = x;
                                var sb = new StringBuilder("[");
                                var cm = "";
                                for (var b=px.rows.PositionAt(k);b!=null; b=(MTreeBookmark)b.Next())
                                {
                                    sb.Append(cm); cm = ",";
                                    db.Get(b.value()).Append(sb);
                                }
                                sb.Append(']');
                                asy.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Protocol.Table:
                            {
                                var tr = db.Transact();
                                var tn = asy.GetString();// table name
                                if (tr.names.Contains(tn))
                                    throw new Exception("Duplicate table name " + tn);
                                var tb = new STable(tr, tn);
                                tr = new STransaction(tr,tb); 
                                var n = asy.GetInt(); // #cols
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = asy.GetString(); // column name
                                    var dt = (Types)asy.ReadByte(); // dataType
                                    tr = new STransaction(tr, new SColumn(tr,cn,dt,tb.uid));
                                }
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                        case Protocol.Insert:
                            {
                                var tr = db.Transact();
                                var tb = (STable)tr.names.Lookup(asy.GetString()); // table name
                                var n = asy.GetInt(); // # named cols
                                var cs = SList<string>.Empty;
                                for (var i = 0; i < n; i++)
                                    cs = cs.InsertAt(asy.GetString(),cs.Length);
                                var nc = (n==0)?tb.cpos.Length:cs.Length;
                                var nr = asy.GetInt(); // #records
                                for (var i = 0; i < nr; i++)
                                {
                                    var f = SDict<string, Serialisable>.Empty;
                                    if (n == 0)
                                        for (var b = tb.cpos; b.Length != 0; b = b.next)
                                            f = f.Add(b.element.name, asy._Get(tr)); // serialsable values
                                    else
                                        for (var b = cs; b.Length != 0; b = b.next)
                                            f = f.Add(b.element, asy._Get(tr)); // serialsable values
                                    tr = new STransaction(tr, new SRecord(tr, tb.uid, f));
                                }
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                          case Protocol.Alter:
                            {
                                var tr = db.Transact();
                                var tn = asy.GetString(); // table name
                                var tb = (STable)tr.names.Lookup(tn)??
                                    throw new Exception("Table "+tn+" not found"); 
                                var cn = asy.GetString(); // column name or ""
                                var nm = asy.GetString(); // new name
                                if (cn.Length==0)
                                    tr = new STransaction(tr,new SAlter(tr, nm, Types.STable, tb.uid, 0));
                                else
                                    tr = new STransaction(tr, new SAlter(tr, nm, Types.SColumn, tb.uid, 
                                        tb.Find(cn)?.uid??throw new Exception("Column "+cn+" not found")));
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                        case Protocol.Drop:
                            {
                                var tr = db.Transact();
                                var nm = asy.GetString(); // object name
                                var pt = tr.names.Lookup(nm) ??
                                    throw new Exception("Object " + nm + " not found");
                                var cn = asy.GetString();
                                if (cn.Length==0)
                                    tr = new STransaction(tr,new SDrop(tr,pt.uid,-1));
                                else
                                    tr = new STransaction(tr, new SDrop(tr,
                                        ((STable)pt).Find(cn)?.uid ?? throw new Exception("Column " + cn + " not found"),
                                        pt.uid));
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                        case Protocol.Index:
                            {
                                var tr = db.Transact();
                                var tn = asy.GetString(); // table name
                                var tb = (STable)tr.names.Lookup(tn) ??
                                    throw new Exception("Table " + tn + " not found");
                                var xt = asy.ReadByte();
                                var rn = asy.GetString();
                                var nc = asy.GetInt();
                                var cs = SList<long>.Empty;
                                for (var i=0;i<nc;i++)
                                {
                                    var cn = asy.GetString();
                                    cs = cs.InsertAt(tb.Find(cn)?.uid ??
                                        throw new Exception("Column " + cn + " not found"), cs.Length);
                                }
                                tr = new STransaction(tr, new SIndex(tr, tb.uid, xt < 2, cs));
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                        case Protocol.Read:
                            {
                                var id = asy.GetLong();
                                var sb = new StringBuilder();
                                db.Get(id).Append(sb);
                                asy.PutString(sb.ToString());
                                asy.Flush();
                                break;
                            }
                        case Protocol.Update:
                            {
                                var tr = db.Transact();
                                var id = asy.GetLong();
                                var rc = db.Get(id);
                                var tb = (STable)tr.Lookup(rc.table); 
                                var n = asy.GetInt(); // # cols updated
                                var f = SDict<string, Serialisable>.Empty;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = asy.GetString();
                                    f = f.Add(cn, asy._Get(db));
                                }
                                tr = new STransaction(tr, new SUpdate(tr, rc, f));
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                        case Protocol.Delete:
                            {
                                var tr = db.Transact();
                                var id = asy.GetLong();
                                var rc = db.Get(id) as SRecord ??
                                    throw new Exception("Record " + id + " not found");
                                tr = new STransaction(tr, new SDelete(tr, rc.table,rc.uid));
                                db = db.MaybeAutoCommit(tr);
                                asy.Write(Responses.Done);
                                asy.Flush();
                                break;
                            }
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        db = db.Rollback();
           //             db.result = null;
                        asy.StartException();
                        asy.Write(Responses.Exception);
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
        /// <summary>
        /// the default database folder
        /// </summary>
        internal static string host = "127.0.0.1";
        internal static int port = 50433;
        /// <summary>
        /// a TCP listener for the Pyrrho service
        /// </summary>
		static TcpListener tcp;
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
    "Strong DBMS (c) 2018 Malcolm Crowe and University of the West of Scotland",
    "0.0"," (15 November 2018)", " github.com/MalcolmCrowe/ShareableDataStructures"
};
    }
    public class AsyncStream :StreamBase
    {
        internal Socket client;
        internal int rx = 0;
        internal int rcount = 0;
        bool exception = false;

        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override long Length => 0;

        public override long Position { get => 0; set => throw new NotImplementedException(); }

        internal AsyncStream(Socket c)
        {
            client = c;
            rbuf = new Buffer(this);
            wbuf = new Buffer(this);
            wbuf.pos = 2;
            rbuf.pos = 2;
            rbuf.len = 0;
        }

        public override void Flush()
        {
            if (wbuf.pos == 2)
                return;
            if (wbuf.wait != null)
                wbuf.wait.WaitOne();
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            if (exception) // version 2.0
                unchecked
                {
                    exception = false;
                    wbuf.buf[0] = (byte)((Buffer.Size - 1) >> 7);
                    wbuf.buf[1] = (byte)((Buffer.Size - 1) & 0x7f);
                    wbuf.pos -= 4;
                    wbuf.buf[2] = (byte)(wbuf.pos >> 7);
                    wbuf.buf[3] = (byte)(wbuf.pos & 0x7f);
                    rcount = 0;
                }
            else
            {
                wbuf.pos -= 2;
                wbuf.buf[0] = (byte)(wbuf.pos >> 7);
                wbuf.buf[1] = (byte)(wbuf.pos & 0x7f);
            }
            try
            {
                IAsyncResult br = client.BeginSend(wbuf.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback1), wbuf);
                if (!br.IsCompleted)
                    br.AsyncWaitHandle.WaitOne();
                wbuf.pos = 2;
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
        protected override bool GetBuf(Buffer b)
        {
            b.pos = 2;
            rcount = 0;
            rx = 0;
            try
            {
                b.wait = new ManualResetEvent(false);
                client.BeginReceive(b.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback), b);
                b.wait.WaitOne();
                b.len = rcount + 2;
                return rcount > 0;
            }
            catch (SocketException)
            {
                return false;
            }
        }
        /// <summary>
        /// Callback on completion of a read request from the network
        /// </summary>
        /// <param name="ar">the async result</param>
        protected void Callback(IAsyncResult ar)
        {
            var buf = ar.AsyncState as Buffer;
            try
            {
                int rc = client.EndReceive(ar);
                if (rc == 0)
                {
                    rcount = 0;
                    buf.wait.Set();
                }
                if (rc + rx == Buffer.Size)
                {
                    rcount = ((buf.buf[0]) << 7) + buf.buf[1];
                    buf.wait.Set();
                }
                else
                {
                    rx += rc;
                    client.BeginReceive(buf.buf, rx, Buffer.Size - rx, 0, new AsyncCallback(Callback), buf);
                }
            }
            catch (SocketException)
            {
                rcount = 0;
                buf.wait.Set();
                Close();
            }
        }
        public override int ReadByte()
        {
            if (rbuf.pos >= rcount + 2)
                GetBuf(rbuf);
            return base.ReadByte();
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
        /// <summary>
        /// Callback on completion of a write request to the network
        /// </summary>
        /// <param name="ar">the async result</param>
        void Callback1(IAsyncResult ar)
        {
            try
            {
                Buffer buf = ar.AsyncState as Buffer;
                client.EndSend(ar);
                buf.wait.Set();
            }
            catch (Exception)
            {
                  Console.WriteLine("Socket Exception reported on Write");
            }
        }
        internal void StartException()
        {
            rcount = 0;
            wbuf.pos = 4;
            exception = true;
        }
        public void Write(Responses p)
        {
            WriteByte((byte)p);
        }
    }
}
