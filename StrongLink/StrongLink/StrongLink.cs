using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using Shareable;

namespace StrongLink
{
    public enum IndexType { Primary, Unique, Reference };
    public class StrongConnect
    {
        internal AsyncClient asy;
        public StrongConnect(string host,int port,string fn)
        {
            Socket socket = null;
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
            asy = new AsyncClient(this, socket);
            asy.PutString(fn);
        }
        public void CreateTable(string n,params SColumn[] cols)
        {
            asy.Write(Protocol.Table);
            asy.PutString(n);
            asy.PutInt(cols.Length);
            foreach(var c in cols)
            {
                asy.PutString(c.name);
                asy.WriteByte((byte)c.dataType);
            }
            var b = asy.Receive();
        }
        public void CreateIndex(string tn,IndexType t,string rt,params string[] key)
        {
            asy.Write(Protocol.Index);
            asy.PutString(tn);
            asy.WriteByte((byte)t);
            if (rt == null)
                asy.PutInt(0);
            else
                asy.PutString(rt);
            asy.PutInt(key.Length);
            foreach (var s in key)
                asy.PutString(s);
            var b = asy.Receive();
        }
        public void Insert(string tn,string[] cols,Serialisable[][] rows)
        {
            asy.Write(Protocol.Insert);
            asy.PutString(tn);
            asy.PutInt(cols.Length);
            foreach (var s in cols)
                asy.PutString(s);
            asy.PutInt(rows[0].Length);
            asy.PutInt(rows.Length);
            for (var i = 0; i < rows.Length; i++)
                for (var j = 0; j < rows[i].Length; j++)
                    rows[i][j].Put(asy);
            var b = asy.Receive();
        }
    }
    class AsyncClient : StreamBase
    {
        /// <summary>
        /// For asynchronous IO
        /// </summary>
        StrongConnect connect = null;
        internal Socket client;
        internal int rx = 0;
        internal int rcount = 0;
        internal AsyncClient(StrongConnect pc, Socket c)
        {
            client = c;
            wbuf = new Buffer(this);
            rbuf = new Buffer(this);
            connect = pc;
        }
        protected override void GetBuf(Buffer b)
        {
            b.pos = 2;
            rcount = 0;
            rx = 0;
            try
            {
                b.wait = new ManualResetEvent(false);
                client.BeginReceive(b.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback), this);
                b.wait.WaitOne();
                if (rcount == Buffer.Size - 1)
                    GetException();
            }
            catch (SocketException)
            {
            }
        }
        void Callback(IAsyncResult ar)
        {
            try
            {
                int rc = client.EndReceive(ar);
                if (rc == 0)
                {
                    rcount = 0;
                    rbuf.wait.Set();
                    return;
                }
                if (rc + rx == Buffer.Size)
                {
                    rcount = (((int)rbuf.buf[0]) << 7) + (int)rbuf.buf[1];
                    rbuf.wait.Set();
                }
                else
                {
                    rx += rc;
                    client.BeginReceive(rbuf.buf, rx, Buffer.Size - rx, 0, new AsyncCallback(Callback), this);
                }
            }
            catch (SocketException)
            {
                rcount = 0;
                rbuf.wait.Set();
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
        public Responses Receive()
        {
            return (Responses)ReadByte();
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
            catch (SocketException)
            {
            }
            wbuf.pos = 2;
        }
        void Callback1(IAsyncResult ar)
        {
            Buffer buf = ar.AsyncState as Buffer;
            buf.wait.Set();
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            for (int j = 0; j < count; j++)
                WriteByte(buffer[offset + j]);
        }
        public void Write(Protocol p)
        {
            WriteByte((byte)p);
        }
        public override void Flush()
        {
            rcount = 0;
            rbuf.pos = 2;
            if (wbuf.wait != null)
                wbuf.wait.WaitOne();
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            wbuf.pos -= 2;
            wbuf.buf[0] = (byte)(wbuf.pos >> 7);
            wbuf.buf[1] = (byte)(wbuf.pos & 0x7f);
            try
            {
                IAsyncResult br = client.BeginSend(wbuf.buf, 0, Buffer.Size, 0, new AsyncCallback(Callback1), wbuf);
                if (!br.IsCompleted)
                    br.AsyncWaitHandle.WaitOne();
                wbuf.pos = 2;
            }
            catch (SocketException)
            {
            }
        }
        internal int GetException()
        {
            return (int)GetException(Responses.Exception);
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        internal int GetException(Responses proto)
        {
            if (proto == Responses.Exception)
            {
                rcount = (((int)rbuf.buf[rbuf.pos++]) << 7) + (((int)rbuf.buf[rbuf.pos++]) & 0x7f);
                rcount += 2;
                proto = (Responses)rbuf.buf[rbuf.pos++];
            }
            throw new Exception(GetString());
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
            get { throw new Exception("The method or operation is not implemented."); }
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
