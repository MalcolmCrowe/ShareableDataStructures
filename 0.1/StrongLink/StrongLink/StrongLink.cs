using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using Shareable;
#nullable enable
namespace StrongLink
{
    public class ServerException : Exception
    {
        public ServerException(string message) : base(message) { }
    }
    public enum IndexType { Primary =0, Unique=1, Reference=2 };
    public class StrongConnect
    {
        internal ClientStream asy;
        public bool inTransaction = false;
        SDict<long, string> preps = SDict<long, string>.Empty;
        public SDict<int, string>? description = null; // see ExecuteQuery
        public StrongConnect(string host, int port, string fn)
        {
            Socket? socket = null;
            try
            {
                IPEndPoint ep;
                socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                if (char.IsDigit(host[0]))
                {
                    IPAddress ip = IPAddress.Parse(host);
                    ep = new IPEndPoint(ip, port);
                    socket.Connect(ep);
                }
                else
                {
#if MONO1
                    var he = Dns.GetHostByName(hostName);
#else
                    IPHostEntry he = Dns.GetHostEntry(host);
#endif
                    for (int j = 0; j < he.AddressList.Length; j++)
                        try
                        {
                            IPAddress ip = he.AddressList[j];
                            ep = new IPEndPoint(ip, port);
                            socket.Connect(ep);
                            if (socket.Connected)
                                break;
                        }
                        catch (Exception) { }
                }
            }
            catch (Exception)
            {
            }
            if (socket == null || !socket.Connected)
                throw new Exception("No connection to " + host + ":" + port);
            asy = new ClientStream(this, socket);
            asy.PutString(fn);
            asy.Flush();
            asy.rbuf.ReadByte();
            preps = SDict<long, string>.Empty;
        }
        public long Prepare(string n)
        {
            var u = -(preps.Length ?? 0) - 2;
            preps += (u, n);
            return u;
        }
        public void CreateTable(string n)
        {
            var un = Prepare(n);
            asy.SendUids(preps);
            asy.Write(Types.SCreateTable);
            asy.PutLong(un);
            asy.PutInt(0);
            asy.PutInt(0);
            var b = asy.Receive();
            preps = SDict<long,string>.Empty;
        }
        public void CreateColumn(string c,Types t,string tn,params (string,SFunction)[] constraints)
        {
            var uc = Prepare(c);
            var ut = Prepare(tn);
            asy.SendUids(preps);
            asy.Write(Types.SCreateColumn);
            new SColumn(uc, t, ut, new SDict<string, SFunction>(constraints)).PutColDef(asy);
            var b = asy.Receive();
            preps = SDict<long,string>.Empty;
        }
        public void CreateIndex(string tn,IndexType t,string? rt,params string[] key)
        {
            var ut = Prepare(tn);
            long u = -1;
            if (rt!=null)
                u=Prepare(rt);
            var keys = SList<long>.Empty;
            for (var i = key.Length-1;i>=0; i--)
                keys += Prepare(key[i]);
            asy.SendUids(preps);
            new SIndex(ut, t == IndexType.Primary,u, keys).Put(asy);
            var b = asy.Receive();
            preps = SDict<long,string>.Empty;
        }
        public void Insert(string tn,string[] cols,params Serialisable[][] rows)
        {
            var ut = Prepare(tn);
            var u = new long[cols.Length];
            for (var i = 0; i < cols.Length; i++)
                u[i] = Prepare(cols[i]);
            asy.SendUids(preps);
            asy.Write(Types.Insert);
            asy.PutLong(ut);
            asy.PutInt(cols.Length);
            // insert cols if supplied
            for (var i = 0; i < cols.Length;i++)
                asy.PutLong(u[i]);
            // now the rows
            asy.PutInt(rows[0].Length);
            asy.PutInt(rows.Length);
            for (var i = 0; i < rows.Length; i++)
                for (var j = 0; j < rows[i].Length; j++)
                    rows[i][j].Put(asy);
            var b = asy.Receive();
            preps = SDict<long,string>.Empty;
        }
        public DocArray ExecuteQuery(string sql)
        {
            var pair = Parser.Parse(sql);
            var qry = pair.Item1 as SQuery;
            if (qry == null)
                throw new Exception("Bad query " + sql);
            return Get(pair.Item2,qry);
        }
        static long _ctr = 0;
        public Types ExecuteNonQuery(string sql)
        {
            var s = Parser.Parse(sql);
            if (s.Item2 == null)
                return Types.Exception;
            asy.SendUids(s.Item2);
            s.Item1.Put(asy);
            var b = asy.Receive();
            if (b == Types.Exception)
                inTransaction = false;
            else
            {
                var su = sql.Trim().Substring(0, 5).ToUpper();
                switch (su)
                {
                    case "BEGIN": inTransaction = true; break;
                    case "ROLLB":
                    case "COMMI": inTransaction = false; break;
                }
            }
            return b;
        }
        public DocArray Get(SDict<long,string> d,Serialisable tn)
        {
            asy.SendUids(d);
            asy.Write(Types.DescribedGet);
            tn.Put(asy);
            asy.Flush();
            var b = asy.ReadByte();
            if (b == (byte)Types.Exception)
            {
                inTransaction = false;
                asy.GetException();
            }
            if (b == (byte)Types.Done)
            {
                description = SDict<int, string>.Empty;
                var n = asy.rbuf.GetInt();
                for (var i = 0; i < n; i++)
                    description += (i, asy.rbuf.GetString());
                return new DocArray(asy.rbuf.GetString());
            }
            throw new Exception("PE28");
        }
        public void BeginTransaction()
        {
            asy.Write(Types.SBegin);
            var b = asy.Receive();
            if (b == Types.Exception)
            {
                inTransaction = false;
                asy.GetException();
            }
            if (b == Types.Done)
                inTransaction = true;
        }
        public void Rollback()
        {
            asy.Write(Types.SRollback);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void Commit()
        {
            asy.Write(Types.SCommit);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void ExecuteNonQuery(SDict<long,string> d,Serialisable s)
        {
            asy.SendUids(d);
            s.Put(asy);
            var b = asy.Receive();
            if (b == Types.Exception)
                inTransaction = false;
            else
                switch (s.type)
                {
                    case Types.SBegin: inTransaction = true; break;
                    case Types.SRollback:
                    case Types.SCommit: inTransaction = false; break;
                }
        } 
        public void Close()
        {
            asy.Close();
        }
    }
    /// <summary>
    /// not shareable
    /// </summary>
    class ClientStream : StreamBase
    {
        internal Socket client;
        static long _cid = 0;
        long cid = ++_cid;
        public bool getting = false;
        internal int rx = 0;
        internal Reader rbuf;
        internal ClientStream(StrongConnect pc, Socket c)
        {
            client = c;
            wbuf = new Buffer(this);
            rbuf = new SocketReader(this);
            rbuf.pos = 2;
            rbuf.buf.len = 0;
            wbuf.wpos = 2;
        }
        public override bool GetBuf(Buffer b)
        {
            getting = true;
            int rcount;
            rx = 0;
            try
            {
                var rc = client.Receive(b.buf, Buffer.Size, 0);
                if (rc == 0)
                {
                    rcount = 0;
                    getting = false;
                    return false;
                }
                rcount = (b.buf[0] << 7) + b.buf[1];
                b.len = rcount + 2;
                if (rcount == Buffer.Size - 1)
                    GetException();
                getting = false;
                return rcount > 0;
            }
            catch (SocketException)
            {
                return false;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            int j;
            for (j = 0; j < count; j++)
            {
                int x = rbuf.ReadByte();
                if (x < 0)
                    break;
                buffer[offset + j] = (byte)x;
            }
            return j;
        }
        public Types Receive()
        {
            if (wbuf == null)
                return Types.Serialisable; // won't occur
            if (wbuf.wpos > 2)
                Flush();
            return (Types)rbuf.ReadByte();
        }
        public void SendUids(SDict<long,string> u)
        {
            Write(Types.SNames);
            PutInt(u.Length);
            for (var b = u.First(); b != null; b = b.Next())
            {
                PutLong(b.Value.Item1);
                PutString(b.Value.Item2);
            }
        }
        public void SendUids(params (string, long)[] u)
        {
            Write(Types.SNames);
            PutInt(u.Length);
            for (var i=0;i<u.Length;i++)
            {
                PutString(u[i].Item1);
                PutLong(u[i].Item2);
            }
        }
        protected override void PutBuf(Buffer b)
        {
            Flush();
            b.wpos = 2;
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            for (int j = 0; j < count; j++)
                WriteByte(buffer[offset + j]);
        }
        public void Write(Types p)
        {
            WriteByte((byte)p);
        }
        public override void Flush()
        {
            rbuf.pos = 2;
            rbuf.buf.len = 0;
            if (wbuf == null)
                return;
            // now always send bSize bytes (not wcount)
            wbuf.wpos -= 2;
            wbuf.buf[0] = (byte)(wbuf.wpos >> 7);
            wbuf.buf[1] = (byte)(wbuf.wpos & 0x7f);
            try
            {
                client.Send(wbuf.buf, Buffer.Size, 0);
                wbuf.wpos = 2;
            }
            catch (SocketException e)
            {
                Console.WriteLine("Flush reports exception " + e.Message);
                throw e;
            }
        }
        internal int GetException()
        {
            return GetException(Types.Exception);
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        internal int GetException(Types proto)
        {
            Buffer bf = rbuf.buf;
            if (proto == Types.Exception)
            {
                var rcount = (bf.buf[rbuf.pos++] << 7) + (bf.buf[rbuf.pos++] & 0x7f);
                bf.len = rcount + 4;
                proto = (Types)bf.buf[rbuf.pos++];
            }
            var em = rbuf.GetString();
    //        Console.WriteLine("Received exception: " + em);
            throw new ServerException(em);
        }
        public override bool CanRead
        {
            get { return true; }
        }
        public override bool CanWrite
        {
            get { return true; }
        }
        public override bool CanSeek
        {
            get { return false; }
        }
        public override long Length
        {
            get => 0;
        }
        public override long Position
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        public override void SetLength(long value)
        {
            throw new Exception("The method or operation is not implemented.");
        }
    }
}
