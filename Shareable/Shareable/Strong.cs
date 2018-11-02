using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Security.Principal;
using System.Security.AccessControl;

namespace Shareable
{
    public enum Protocol
    { 
        EoF = -1, Get = 1, Begin = 2, Commit = 3, Rollback = 4,
        Table = 5, Alter = 6, Drop = 7, Index = 8, Insert = 9,
        Update = 10, Delete = 11, View = 12
    }
    public enum Responses
    {
        Done = 0, Exception = 1
    }
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
        bool disposed = false;
        public DateTime lastop = DateTime.Now;
        public Thread myThread = null;
        private int nextCol = 0;
        private Serialisable nextCell = null;
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
            //     client.Blocking = false;
            // process the connection string
            asy = new AsyncStream(client);
            myThread = Thread.CurrentThread;
            int p = -1;
            try
            {
                var fn = asy.GetString();
                var db = SDatabase.Open(fn);
            } catch (Exception e)
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
                        case Protocol.Get: { break; }
                        case Protocol.Table:
                            {
                                var tr = db.Transact();
                                var tb = STable.Get(tr, asy);
                                tr = new STransaction(tr,tb); // table name
                                var n = asy.GetInt(); // #cols
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = asy.GetString(); // column name
                                    var dt = (Types)asy.ReadByte(); // dataType
                                    tr = new STransaction(tr, new SColumn(tr,cn,dt,tb.uid));
                                }
                                db = db.MaybeAutoCommit(tr);
                                break;
                            }
                        case Protocol.Insert:
                            {
                                var tr = db.Transact();
                                var tb = (STable)tr.names.Lookup(asy.GetString()); // table name
                                var n = asy.GetInt(); // # named cols
                                var cs = (n==0)?tb.cpos:SList<SColumn>.Empty;
                                for (var i = 0; i < n; i++)
                                {
                                    var cn = asy.GetString();
                                    for (var b = tb.cpos; b.Length != 0; b = b.next)
                                        if (b.element.name.CompareTo(cn) == 0)
                                        {
                                            cs = cs.InsertAt(b.element, cs.Length);
                                            break;
                                        }
                                }
                                var nc = cs.Length;
                                var nr = asy.GetInt(); // #records
                                for (var i=0;i<nr;i++)
                                {
                                    var f = SDict<string, Serialisable>.Empty;
                                    for (var b = cs; b.Length != 0; b = b.next)
                                        f = f.Add(b.element.name, asy._Get(tr)); // serialsable values
                                    tr = new STransaction(tr, new SRecord(tr, tb.uid, f));
                                }
                                db = db.MaybeAutoCommit(tr);
                                break;
                            }
                        case Protocol.Alter:
                            {
                                var tr = db.Transact();

                                db = db.MaybeAutoCommit(tr);
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
        internal static string path = "";
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
            if (path != "")
                Console.WriteLine("Database folder " + path);
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
                        path = args[k].Substring(3);
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
            if (path == "")
                return;
            if (path.Contains("/") && !path.EndsWith("/"))
                path += "/";
            else if (!path.EndsWith("\\"))
                path += "\\";
            var acl = Directory.GetAccessControl(path);
            if (acl == null)
                goto bad;
            var acr = acl.GetAccessRules(true, true, typeof(SecurityIdentifier));
            if (acr == null)
                goto bad;
            foreach (FileSystemAccessRule r in acr)
                if ((FileSystemRights.Write & r.FileSystemRights) == FileSystemRights.Write
                    && r.AccessControlType == AccessControlType.Allow)
                    return;
                bad: throw new Exception("Cannot access path " + path);
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

        internal AsyncStream(Socket client)
        {
            rbuf = new Buffer(this);
            wbuf = new Buffer(this);
            wbuf.pos = 2;
            rbuf.pos = 2;
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
        protected override void GetBuf(Buffer b)
        {
            b.pos = 2;
            rcount = 0;
            rx = 0;
            try
            {
                rbuf.wait = new ManualResetEvent(false);
                int x = rcount;
                client.BeginReceive(rbuf.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback), rbuf);
                rbuf.wait.WaitOne();
            }
            catch (SocketException)
            {
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
                    return;
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
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            wbuf.pos -= 2;
            wbuf.buf[0] = (byte)(wbuf.pos >> 7);
            wbuf.buf[1] = (byte)(wbuf.pos & 0x7f);
            try
            {
                client.BeginSend(wbuf.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback1), wbuf);
                if (wbuf.wait != null)
                    wbuf.wait.WaitOne();
            }
            catch (Exception)
            {
                Console.WriteLine("Socket Exception reported on Write");
            }
            wbuf.pos = 2;
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
