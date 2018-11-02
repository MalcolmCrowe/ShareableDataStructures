using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using Shareable;

namespace StrongLink
{
    class Program
    {
        static StrongConnect conn;
        static void Main(string[] args)
        {
            conn = new StrongConnect("127.0.0.1", 50433, "Tpcc");
        }
        public void BuildTpcc()
        {
            CreationScript();
            FillItems();
        }
        public void CreationScript()
        {
            conn.CreateTable("WAREHOUSE", 
                new SColumn(null, "W_ID",Types.SInteger),
                new SColumn(null, "W_NAME",Types.SString),
                new SColumn(null, "W_STREET_1",Types.SString),
                new SColumn(null, "W_STREET_2",Types.SString),
                new SColumn(null, "W_CITY",Types.SString),
                new SColumn(null, "W_STATE",Types.SString),
                new SColumn(null, "W_ZIP",Types.SString),
                new SColumn(null, "W_TAX",Types.SNumeric),
                new SColumn(null, "W_YTD",Types.SNumeric)
            );
            conn.CreateIndex("WAREHOUSE", IndexType.Primary, null, "W_ID");
            conn.CreateTable("DISTRICT",
                new SColumn(null, "D_ID", Types.SInteger),
                new SColumn(null, "D_W_ID", Types.SInteger),
                new SColumn(null, "D_NAME", Types.SString),
                new SColumn(null, "D_STREET_1", Types.SString),
                new SColumn(null, "D_STREET_2", Types.SString),
                new SColumn(null, "D_CITY", Types.SString),
                new SColumn(null, "D_STATE", Types.SString),
                new SColumn(null, "D_ZIP", Types.SString),
                new SColumn(null, "D_TAX", Types.SNumeric),
                new SColumn(null, "D_YTD", Types.SNumeric),
                new SColumn(null, "D_NEXT_O_ID", Types.SInteger)
                );
            conn.CreateIndex("DISTRICT", IndexType.Primary, null, "D_W_ID,", "D_ID");
            conn.CreateIndex("DISTRICT", IndexType.Reference, "WAREHOUSE", "D_W_ID");
            conn.CreateTable("CUSTOMER",
                new SColumn(null, "C_ID", Types.SInteger),
                new SColumn(null, "C_D_ID", Types.SInteger),
                new SColumn(null, "C_W_ID", Types.SInteger),
                new SColumn(null, "C_FIRST", Types.SString),
                new SColumn(null, "C_MIDDLE", Types.SString),
                new SColumn(null, "C_LAST", Types.SString),
                new SColumn(null, "C_STREET_1", Types.SString),
                new SColumn(null, "C_STREET_2", Types.SString),
                new SColumn(null, "C_CITY", Types.SString),
                new SColumn(null, "C_STATE", Types.SString),
                new SColumn(null, "C_ZIP", Types.SString),
                new SColumn(null, "C_PHONE", Types.SString),
                new SColumn(null, "C_SINCE", Types.SDate),
                new SColumn(null, "C_CREDIT", Types.SString),
                new SColumn(null, "C_CREDIT_LIM",Types.SNumeric),
                new SColumn(null, "C_DISCOUNT", Types.SNumeric),
                new SColumn(null, "C_BALANCE", Types.SNumeric),
                new SColumn(null, "C_YTD_PAYMENT", Types.SNumeric),
                new SColumn(null, "C_PAYMENT_CNT", Types.SNumeric),
                new SColumn(null, "C_DELIVERY_CNT", Types.SNumeric),
                new SColumn(null, "C_DATA", Types.SString)
                );
            conn.CreateIndex("CUSTOMER", IndexType.Primary, null, "C_W_ID", "C_D_ID", "C_ID");
            conn.CreateIndex("CUSTOMER", IndexType.Reference, "DISTRICT", "C_W_ID", "C_D_ID");
            conn.CreateTable("HISTORY",
                new SColumn(null, "H_C_ID", Types.SInteger),
                new SColumn(null, "H_C_D_ID", Types.SInteger),
                new SColumn(null, "H_C_W_ID", Types.SInteger),
                new SColumn(null, "H_D_ID", Types.SInteger),
                new SColumn(null, "H_W_ID", Types.SInteger),
                new SColumn(null, "H_DATE", Types.SDate),
                new SColumn(null, "H_AMOUNT", Types.SNumeric),
                new SColumn(null, "H_DATA", Types.SString)
                );
            conn.CreateIndex("HISTORY", IndexType.Reference, "CUSTOMER", "H_C_W_ID", "H_C_D_ID", "H_C_ID");
            conn.CreateIndex("HISTORY", IndexType.Reference, "DISTRICT", "H_W_ID", "H_D_ID");
            conn.CreateTable("ORDER",
                new SColumn(null, "O_ID", Types.SInteger),
                new SColumn(null, "O_D_ID", Types.SInteger),
                new SColumn(null, "O_W_ID", Types.SInteger),
                new SColumn(null, "O_C_ID", Types.SInteger),
                new SColumn(null, "O_ENTRY_D", Types.SDate),
                new SColumn(null, "O_CARRIER_ID", Types.SInteger),
                new SColumn(null, "O_OL_CNT", Types.SInteger),
                new SColumn(null, "O_ALL_LOCAL", Types.SNumeric)
                );
            conn.CreateIndex("ORDER", IndexType.Primary, null, "O_W_ID", "O_D_ID", "O_ID");
            conn.CreateIndex("ORDER", IndexType.Reference, "CUSTOMER", "O_W_ID", "O_D_ID", "O_C_ID");
            conn.CreateTable("NEW_ORDER",
                new SColumn(null, "NO_O_ID", Types.SInteger),
                new SColumn(null, "NO_D_ID", Types.SInteger),
                new SColumn(null, "NO_W_ID", Types.SInteger)
                );
            conn.CreateIndex("NEW_ORDER", IndexType.Primary, null, "NO_W_ID", "NO_D_ID", "NO_O_ID");
            conn.CreateIndex("NEW_ORDER", IndexType.Reference,"ORDER", "NO_W_ID", "NO_D_ID", "NO_O_ID");
            conn.CreateTable("ITEM",
                new SColumn(null, "I_ID", Types.SInteger),
                new SColumn(null, "I_IM_ID", Types.SInteger),
                new SColumn(null, "I_NAME", Types.SString),
                new SColumn(null, "I_PRICE", Types.SNumeric),
                new SColumn(null, "I_DATA", Types.SString)
                );
            conn.CreateIndex("ITEM", IndexType.Primary, null, "I_ID");
            conn.CreateTable("STOCK",
                new SColumn(null, "S_I_ID", Types.SInteger),
                new SColumn(null, "S_W_ID", Types.SInteger),
                new SColumn(null, "S_QUANTITY", Types.SNumeric),
                new SColumn(null, "S_DIST_01", Types.SString),
                new SColumn(null, "S_DIST_02", Types.SString),
                new SColumn(null, "S_DIST_03", Types.SString),
                new SColumn(null, "S_DIST_04", Types.SString),
                new SColumn(null, "S_DIST_05", Types.SString),
                new SColumn(null, "S_DIST_06", Types.SString),
                new SColumn(null, "S_DIST_07", Types.SString),
                new SColumn(null, "S_DIST_08", Types.SString),
                new SColumn(null, "S_DIST_09", Types.SString),
                new SColumn(null, "S_DIST_10", Types.SString),
                new SColumn(null, "S_YTD", Types.SNumeric),
                new SColumn(null, "S_ORDER_CNT", Types.SInteger),
                new SColumn(null, "S_REMOTE_CNT", Types.SInteger),
                new SColumn(null, "S_DATA", Types.SString)
                );
            conn.CreateIndex("STOCK", IndexType.Primary, null, "S_W_ID","S_I_ID");
            conn.CreateIndex("STOCK", IndexType.Reference, "ITEM", "S_I_ID");
            conn.CreateIndex("STOCK", IndexType.Reference, "WAREHOUSE", "S_W_ID");
            conn.CreateTable("ORDER_LINE",
                new SColumn(null, "OL_O_ID", Types.SInteger),
                new SColumn(null, "OL_D_ID", Types.SInteger),
                new SColumn(null, "OL_W_ID", Types.SInteger),
                new SColumn(null, "OL_NUMBER", Types.SInteger),
                new SColumn(null, "OL_I_ID", Types.SInteger),
                new SColumn(null, "OL_SUPPLY_W_ID", Types.SInteger),
                new SColumn(null, "OL_DELIVERY_D", Types.SDate),
                new SColumn(null, "OL_QUANTITY", Types.SNumeric),
                new SColumn(null, "OL_AMOUNT", Types.SNumeric),
                new SColumn(null, "OL_DIST_INFO", Types.SString)
                );
            conn.CreateIndex("ORDER_LINE", IndexType.Primary, null, "OL_W_ID", "OL_D_ID","OL_O_ID","OL_NUMBER");
            conn.CreateIndex("ORDER_LINE", IndexType.Reference, "ORDER", "OL_W_ID", "OL_D_ID", "OL_O_ID");
            conn.CreateIndex("ORDER_LINE", IndexType.Reference, "STOCK", "OL_SUPPLY_W_ID", "OL_I_ID");
            conn.CreateTable("DELIVERY",
                new SColumn(null, "DL_W_ID", Types.SInteger),
                new SColumn(null, "DL_ID", Types.SInteger),
                new SColumn(null, "DL_CARRIER_ID", Types.SInteger),
                new SColumn(null, "DL_DONE", Types.SInteger),
                new SColumn(null, "DL_SKIPPED", Types.SInteger)
                );
            conn.CreateIndex("DELIVERY", IndexType.Primary, null, "DL_W_ID", "DL_ID");

        }
    }
    public enum IndexType { Primary, Unique, Reference };
    class StrongConnect
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
