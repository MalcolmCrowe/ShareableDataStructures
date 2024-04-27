using System.Text;
using System.Net.Sockets;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Security;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level1
{
    /// <summary>
    /// On the network there are
    /// serious advantages in making all messages exactly the same size.
    /// </summary>
    internal class TCPStream : Stream
    {
        internal string? debug = null;
        /// <summary>
        /// Buffer size for communications.
        /// important: all buffers have exactly this size
        /// </summary>
        internal const int bSize = 2048;
        /// <summary>
        /// A Buffer: we use one read buffer and two write buffers
        /// </summary>
        internal class Buffer
        {
            // first 2 bytes indicate how many following bytes are good
            internal byte[] bytes = new byte[bSize];
            internal ManualResetEvent wait = new (true);
        }
        /// <summary>
        /// The array of write buffers
        /// </summary>
        internal Buffer[] wbufs = new Buffer[2];
        /// <summary>
        /// the single read buffer
        /// </summary>
        internal Buffer rbuf = new ();
        internal Buffer awakebuf = new();
        /// <summary>
        /// points to the current write buffer
        /// </summary>
        internal Buffer? wbuf = null;
        /// <summary>
        /// The client Socket
        /// </summary>
        internal Socket? client;
        /// <summary>
        /// Pyrrho cryptograhy
        /// </summary>
        public Crypt crypt;
        /// <summary>
        /// Number of characters read this operation
        /// </summary>
        internal int rx = 0;
        /// <summary>
        /// number of characters written this operation
        /// </summary>
        internal int wx = 0;
        /// <summary>
        /// the count of characters in this buffer
        /// </summary>
        internal int rcount = 0;
        /// <summary>
        /// The current position in the read buffer
        /// </summary>
        internal int rpos = 2;
        /// <summary>
        /// the current position in the write buffer
        /// </summary>
        internal int wcount = 2;
        internal string? tName; // name of the thread
        static int _uid = 0;
        internal int uid = ++_uid;
        internal int ncells = 0;
        /// <summary>
        /// Constructor: a new AsyncStream
        /// <paramref name="rs">Always non-null for an outgoing connection</paramref>
        /// </summary>
        internal TCPStream()
        {
            client = null;
            wbufs[0] = new Buffer();
            wbufs[1] = new Buffer();
            wbuf = wbufs[0];
            tName = Thread.CurrentThread.Name;
            crypt = new Crypt(this);
        }
        /// <summary>
        /// Get a byte from the stream: if necessary refill the buffer from the network
        /// </summary>
        /// <returns>the byte</returns>
        public override int ReadByte()
        {
            if (client == null)
                throw new PEException("PE0100");
            try
            {
                if (wcount != 2)
                    Flush();
                if (rpos < rcount + 2)
                    return rbuf.bytes[rpos++];
                rpos = 2;
                rcount = 0;
                rx = 0;

                rbuf.wait = new ManualResetEvent(false);
                int x = rcount;
                client.BeginReceive(rbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback), rbuf);
                rbuf.wait.WaitOne();
                if (rcount <= 0)
                    return -1 - x;
                if (rcount == bSize - 1)
                    return (int)ServerGetException();
            }
            catch (SocketException)
            {
                //          Console.WriteLine("Socket (" + uid + ") Exception reported on Read");
                return -1;
            }
            return rbuf.bytes[rpos++];
        }
        /// <summary>
        /// Callback on completion of a read request from the network
        /// </summary>
        /// <param name="ar">the async result</param>
        protected void Callback(IAsyncResult ar)
        {
            if (client == null || ar.AsyncState is not Buffer buf)
                throw new PEException("PE0011");
            try
            {
                int rc = client.EndReceive(ar);
                if (rc == 0)
                {
                    rcount = 0;
                    buf.wait.Set();
                    return;
                }
                if (rc + rx == bSize)
                {
                    rcount = ((buf.bytes[0]) << 7) + (int)buf.bytes[1];
                    buf.wait.Set();
                }
                else
                {
                    rx += rc;
                    client.BeginReceive(buf.bytes, rx, bSize - rx, 0, new AsyncCallback(Callback), buf);
                }
            }
            catch (SocketException)
            {
                rcount = 0;
                //         Console.WriteLine("Socket (" + uid + ") Exception reported on Read");
                buf.wait.Set();
                Close();
            }
        }
        /// <summary>
        /// Read a set of bytes from the network into an array
        /// </summary>
        /// <param name="buffer">the byte array to transfer into</param>
        /// <param name="offset">the position in the array</param>
        /// <param name="count">the number of bytes to transfer</param>
        /// <returns>the number of bytes received</returns>
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
        /// <summary>
        /// Write a byte to the network
        /// </summary>
        /// <param name="value">the byte to write</param>
        public override void WriteByte(byte value)
        {
            if (wbuf == null)
                throw new PEException("PE0012");
            if (debug != null)
                Console.WriteLine(debug + value.ToString("X"));
            if (wcount < bSize-1)
                wbuf.bytes[wcount++] = value;
            if (wcount >= bSize - 1)
            {
                // update ncells
                if (ncells != 1)
                {
                    int owc = wcount;
                    wcount = 3;
                    PutInt(ncells);
                    wcount = owc;
                }
                WriteBuf();
                ncells = 1;
                wbuf.bytes[wcount++] = value;
            }
        }
        public void Write(Protocol p)
        {
            WriteByte((byte)p);
        }
        public void Write(Responses p)
        {
            WriteByte((byte)p);
        }
        internal void SendAwake()
        {
            if (client == null)
                throw new PEException("PE0013");
            awakebuf.wait = new ManualResetEvent(false);
            awakebuf.bytes[0] = 0; awakebuf.bytes[1] = 1;
            awakebuf.bytes[2] = (byte)Responses.Continue;
            client.BeginSend(awakebuf.bytes, 0, bSize, 0, new AsyncCallback(Callback1), awakebuf);
            awakebuf.wait?.WaitOne();
        }
        /// <summary>
        /// flush a write buffer to the network
        /// </summary>
        private void WriteBuf()
        {
            if (wbuf == null || client==null)
                throw new PEException("PE0013");
            if (PyrrhoStart.DebugMode)
                Console.WriteLine("WriteBuf");
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            wcount -= 2;
            wbuf.bytes[0] = (byte)(wcount >> 7);
            wbuf.bytes[1] = (byte)(wcount & 0x7f);
            try
            {
                client.BeginSend(wbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback1), wbuf);
                wx = (wx + 1) & 1;
                wbuf = wbufs[wx];
                wbuf.wait?.WaitOne();
            }
            catch (Exception)
            {
                //          Console.WriteLine("Socket (" + uid + ") Exception reported on Write");
            }
            wcount = 2;
        }
        /// <summary>
        /// Callback on completion of a write request to the network
        /// </summary>
        /// <param name="ar">the async result</param>
        void Callback1(IAsyncResult ar)
        {
            if (ar.AsyncState is not Buffer buf || client == null)
                throw new PEException("PE0014");
            client.EndSend(ar);
            buf.wait.Set();
        }
        /// <summary>
        /// Write a set of bytes from an array
        /// </summary>
        /// <param name="buffer">the array of bytes</param>
        /// <param name="offset">the position in the array</param>
        /// <param name="count">the number of bytes to transfer</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            for (int j = 0; j < count; j++)
                WriteByte(buffer[offset + j]);
        }
        internal void DrainInput()
        {
            rpos = rcount + 2;
        }
        bool exception = false;
        /// <summary>
        /// Flush the buffers to the network
        /// </summary>
        public override void Flush()
        {
            if (wbuf == null || client==null)
                throw new PEException("PE0015");
            if (wcount == 2)
                return;
            int ox = (wx + 1) & 1;
            Buffer obuf = wbufs[ox];
            obuf.wait?.WaitOne();
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            if (exception) // version 2.0
                unchecked
                {
                    exception = false;
                    wbuf.bytes[0] = (bSize - 1) >> 7;
                    wbuf.bytes[1] = (bSize - 1) & 0x7f;
                    wcount -= 4;
                    wbuf.bytes[2] = (byte)(wcount >> 7);
                    wbuf.bytes[3] = (byte)(wcount & 0x7f);
                }
            else
            {
                wcount -= 2;
                wbuf.bytes[0] = (byte)(wcount >> 7);
                wbuf.bytes[1] = (byte)(wcount & 0x7f);
            }
            if (PyrrhoStart.DebugMode)
                Console.WriteLine("Flushing " + wcount);
            try
            {
                IAsyncResult br = client.BeginSend(wbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback1), wbuf);
                if (!br.IsCompleted)
                    br.AsyncWaitHandle.WaitOne();
                wcount = 2;
            }
            catch (Exception)
            {
                //        Console.WriteLine("Socket (" + uid + ") Exception reported on Flush");
            }
        }
        /// <summary>
        /// Version 2.0 extended exception handling.
        /// Discard the write buffer contents and send an exception block
        /// instead. Note that it is possible for exception obs to be
        /// more than one buffer: any additional buffers are normal.
        /// </summary>
        internal void StartException()
        {
            rcount = 0;
            wcount = 4;
            exception = true;
        }
        /// <summary>
        /// implement CanRead (not used in Pyrrho)
        /// </summary>
        public override bool CanRead
        {
            get { return true; }
        }
        /// <summary>
        /// implement CanWrite (not used in Pyrrho)
        /// </summary>
        public override bool CanWrite
        {
            get { return true; }
        }
        /// <summary>
        /// implement CanSeek (not used in Pyrrho)
        /// </summary>
        public override bool CanSeek
        {
            get { return false; }
        }
        /// <summary>
        /// Length is not implemented
        /// </summary>
        public override long Length
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }
        /// <summary>
        /// Position is not implemented
        /// </summary>
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
        /// <summary>
        /// Seek is not implemented
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="origin"></param>
        /// <returns></returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        /// <summary>
        /// SetLength is not implemented
        /// </summary>
        /// <param name="value">the new length</param>
        public override void SetLength(long value)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        /// <summary>
        /// Close the async stream
        /// </summary>
        public override void Close()
        {
            if (PyrrhoStart.DebugMode)
                Console.WriteLine("Closing AsyncStream " + uid);
            base.Close();
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <returns>a Unicode string from the stream</returns>
        internal string GetString()
        {
            int n = GetInt();
            byte[] b = new byte[n];
            Read(b, 0, n);
            return Encoding.UTF8.GetString(b, 0, b.Length);
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <returns>an Int32 from the stream</returns>
        public int GetInt()
        {
            byte[] bytes = new byte[4];
            Read(bytes, 0, 4);
            int n = 0;
            for (int j = 0; j < 4; j++)
                n = (n << 8) + bytes[j];
            return n;
        }
        public long GetLong()
        {
            byte[] bytes = new byte[8];
            Read(bytes, 0, 8);
            long n = 0;
            for (int j = 0; j < 8; j++)
                n = (n << 8) + bytes[j];
            return n;
        }        /// <summary>
                 /// Get an exception passed in the ClientAsync
                 /// </summary>
                 /// <returns>the Exception</returns>
        internal Exception GetException()
        {
            try
            {
                int n = GetInt();
                byte[] bytes = new byte[n];
                n = Read(bytes, 0, n);
                return new Exception(Encoding.UTF8.GetString(bytes, 0, bytes.Length));
            }
            catch (Exception ex) { return ex; }
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <param name="n">a Unicode string for the stream</param>
        internal void PutString(string n)
        {
            byte[] b = Encoding.UTF8.GetBytes(n);
            PutInt(b.Length);
            Write(b, 0, b.Length);
        }
        internal void PutRaw(string n)
        {
            byte[] b = Encoding.UTF8.GetBytes(n);
            Write(b, 0, b.Length);
        }
        /// <summary>
        /// send a numeric 
        /// </summary>
        /// <param name="n"></param>
        internal void PutNumeric(Numeric n)
        {
            PutString(n.ToString());
        }
        /// <summary>
        /// send a real
        /// </summary>
        /// <param name="n"></param>
        internal void PutReal(Numeric n)
        {
                PutString(n.DoubleFormat());
        }
        /// <summary>
        /// send a DateTime to the client
        /// </summary>
        /// <param name="q">the DateTime</param>
        internal void PutDateTime(DateTime d)
        {
            PutLong(d.Ticks);
        }
        /// <summary>
        /// Send a TimeSpan to the client
        /// </summary>
        /// <param name="t">the TimeSpan</param>
        internal void PutTimeSpan(TimeSpan t)
        {
            PutLong(t.Ticks);
        }
        /// <summary>
        /// Send an Interval to the client
        /// </summary>
        /// <param name="v">the interval</param>
        internal void PutInterval(Interval v)
        {
            WriteByte(v.yearmonth ? (byte)1 : (byte)0);
            if (v.yearmonth)
            {
                PutInt(v.years);
                PutInt(v.months);
            }
            else
                PutLong(v.ticks);
        }
        internal void PutWarnings(Context cx)
        {
            for (var b=cx?.warnings.First();b is not null;b=b.Next())
            {
                var e = (DBException)b.value();
                Write(Responses.Warning);
                PutString(e.signal);
                PutInt(e.objects.Length);
                for (int i = 0; i < e.objects.Length; i++)
                    PutString(e.objects[i]?.ToString() ?? "");
            }
        }
        /// <summary>
        /// send an Integer
        /// </summary>
        /// <param name="n"></param>
        internal void PutInteger(Integer n)
        {
            PutString(n.ToString());
            return;
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <param name="n">an Int32 for the stream</param>
        internal void PutInt(int n)
        {
            byte[] b = new byte[4];
            b[0] = (byte)(n >> 24);
            b[1] = (byte)(n >> 16);
            b[2] = (byte)(n >> 8);
            b[3] = (byte)n;
            Write(b, 0, 4);
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <param name="n">an Int64 for the stream</param>
        public void PutLong(long n)
        {
            byte[] b = new byte[8];
            b[0] = (byte)(n >> 56);
            b[1] = (byte)(n >> 48);
            b[2] = (byte)(n >> 40);
            b[3] = (byte)(n >> 32);
            b[4] = (byte)(n >> 24);
            b[5] = (byte)(n >> 16);
            b[6] = (byte)(n >> 8);
            b[7] = (byte)n;
            Write(b, 0, 8);
        }

        /// <summary>
        /// Send a SqlRow to the client.
        /// </summary>
        /// <param name="rt">the structured type</param>
        /// <param name="r">the row to send</param>
        internal void PutRow(Context _cx, TRow r)
        {
            var n = r.Length;
            PutInt(n);
            var j = 0;
            for (var b = r.columns.First(); b != null; b = b.Next(), j++)
                if (b.value() is long p && _cx._Dom(p) is Domain d)
                {
                    PutString(Domain.NameFor(_cx, p, b.key()));
                    var c = r[p];
                    if (c is TList ta && ta.Length >= 1)
                        c = ta[0];
                    c = d.Coerce(_cx, c);
                    PutString(d.name);
                    PutInt(d.Typecode()); // other flags are 0
                    PutCell(_cx, d, c);
                }
        }
        /// <summary>
        /// Send an Array value to the client
        /// </summary>
        /// <param name="a">the Array</param>
        internal void PutArray(Context _cx, TypedValue a)
        {
            PutString("ARRAY");
            if (a is TArray ta)
            {
                int n = ta.Length;
                var et = a.dataType.elType ?? throw new DBException("22G03");
                PutString(et.name??et.ToString());
                PutInt(et.Typecode());
                PutInt(n);
                for (var b = ta.array.First(); b != null; b = b.Next())
                    PutCell(_cx, et, b.value());
            } else if (a is TList tl)
            {
                int n = tl.Length;
                var et = tl.dataType.elType ?? throw new DBException("22G03");
                PutString(et.ToString());
                PutInt(et.Typecode());
                PutInt(n);
                for (var b = tl.list.First(); b != null; b = b.Next())
                    PutCell(_cx, et, b.value());
            }
        }
        /// <summary>
        /// send a Multiset to the client
        /// </summary>
        /// <param name="m">the Multiset</param>
        internal void PutMultiset(Context cx, TMultiset m)
        {
            PutString("MULTISET");
            var et = m.dataType.elType ?? m.dataType ?? Domain.Content;
            PutString(et.ToString());
            PutInt(et.Typecode());
            PutInt((int)m.Count);
            for (var e = m.First(); e != null; e = e.Next())
                PutCell(cx, et, e.Value());
        }
        /// <summary>
        /// send a Set to the client
        /// </summary>
        /// <param name="m">the Multiset</param>
        internal void PutSet(Context cx, TSet m)
        {
            PutString("SET");
            var et = m.dataType.elType ?? m.dataType ?? Domain.Content;
            PutString(et.ToString());
            PutInt(et.Typecode());
            PutInt((int)m.tree.Count);
            for (var e = m.First(); e != null; e = e.Next())
                PutCell(cx, et, e.Value());
        }
        /// <summary>
        /// Send a Table to the client
        /// </summary>
        /// <param name="r">the RowSet</param>
        internal void PutTable(Context cx, RowSet r)
        {
            PutString("TABLE");
            PutSchema(cx);
            int n = 0;
            for (var e = r.First(cx); e != null; e = e.Next(cx))
                n++;
            PutInt(n);
            for (var e = r.First(cx); e != null; e = e.Next(cx))
                for (var b = r.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx._Dom(p) is Domain dt)
                        PutCell(cx, dt, e[b.key()]);
        }
        /// <summary>
        /// Send an array of bytes to the client (e.g. a blob)
        /// </summary>
        /// <param name="b">the byte array</param>
        internal void PutBytes(byte[] b)
        {
            PutInt(b.Length);
            Write(b, 0, b.Length);
        }
        /// <summary>
        /// Send the tree of filename to the client
        /// </summary>
        internal void PutFileNames()
        {
            string[] files;
            files = Directory.GetFiles(Directory.GetCurrentDirectory());
            var ar = new List<string>();
            for (int j = 0; j < files.Length; j++)
            {
                string s = files[j];
                int m = s.LastIndexOf("\\");
                if (m >= 0)
                    s = s[(m + 1)..];
                m = s.LastIndexOf("/");
                if (m >= 0)
                    s = s[(m + 1)..];
                int n = s.Length - 4;
                if (s.IndexOf(".", 0, n) >= 0)
                    continue;
                ar.Add(s[..n]);
            }
            WriteByte((byte)Responses.Files);
            PutInt(ar.Count);
            for (int j = 0; j < ar.Count; j++)
                PutString(ar[j]);
        }

        /// <summary>
        /// Send a result schema to the client
        /// </summary>
        /// <param name="rowSet">the results</param>
        internal void PutSchema(Context cx)
        {
            if (cx.obs[cx.result] is not RowSet result)
            {
#if EMBEDDED
                WriteByte(11);
#else
                Write(Responses.Done);
#endif
                Flush();
                return;
            }
#if EMBEDDED
            WriteByte(13);
#else
            Write(Responses.Schema);
#endif
            var dt = result.rowType;
            int m = result.display;
            if (m == 0)
                m = dt.Length;
            PutInt((m>0)?m:1);
            if (m > dt.Length)
            {
                Console.WriteLine("Unreasonable rowType length " + dt.Length + " < " + m);
                throw new PEException("PE247");
            }
            PutString("Data");
            int[] flags = new int[m];
            result.Schema(cx, flags);
            var j = 0;
            if (m == 0)
            {
                PutString("POSITION");
                PutString("INTEGER");
                PutInt(1);
            }
            else
                for (var b = dt.First(); j < m && b != null; b = b.Next(), j++)
                    if (b.value() is long p)
                    {
                        if (result.representation[p] is not Domain dn)
                            throw new PEException("PE24602");
                        var i = b.key();
                        PutString(result.NameFor(cx, i));
                        if (dn.kind != Sqlx.TYPE && dn.kind != Sqlx.NODETYPE && dn.kind != Sqlx.EDGETYPE)
                            PutString(dn.kind.ToString());
                        else
                            PutString(dn.name);
                        PutInt(flags[j]);
                    }
            Flush();
        }
        /// <summary>
        /// Send a schemaKey and result schema to the client for a weakly typed language
        /// </summary>
        /// <param name="rowSet">the results</param>
        internal void PutSchema1(Context cx,RowSet result)
        {
            if (result == null)
            {
#if EMBEDDED
                WriteByte(11);
#else
                Write(Responses.Done);
#endif
                Flush();
                return;
            }
#if EMBEDDED
            WriteByte(72);
#else
            Write(Responses.Schema1);
#endif
            PutLong(result.lastChange);// compute the schemakey
            int m = result.display;
            PutInt(m);
            if (m == 0)
                Console.WriteLine("No columns?");
            if (m > result.Length)
                Console.WriteLine("Unreasonable rowType length " + result.Length + " < " + m);
            if (m > 0)
            {
                PutString("Data");
                int[] flags = new int[m];
                result.Schema(cx, flags);
                var j = 0;
                for (var b=result.representation.First();b is not null;b=b.Next(),j++)
                {
                    var n = cx.NameFor(b.key());
                    PutString(n);
                    var k = b.value().kind;
                    switch (k)
                    {
                        case Sqlx.DOCUMENT:
                        case Sqlx.LEVEL:
                            n = k.ToString();
                            break;
                    }
                    PutString(n);
                    PutInt(flags[j]);
                }
            }
            Flush();
        }
        internal void PutColumns(Context cx,Domain dt)
        {
            if (dt == null || dt.Length == 0)
            {
#if EMBEDDED
                WriteByte(11);
#else
                Write(Responses.NoData);
#endif
                Flush();
                return;
            }
#if EMBEDDED
            WriteByte(71);
#else
            Write(Responses.Columns);
#endif
            int m = dt.Length;
            PutInt(m);
            for (var j = 0; j < m; j++)
                if (cx.db.objects[dt[j]??-1L] is SqlCopy sc 
                    && cx.db.objects[sc.copyFrom] is TableColumn dn)
                {
                    var n = sc.NameFor(cx);
                    PutString(n);
                    PutString((sc.domain.kind == Sqlx.DOCUMENT) ? "DOCUMENT": n);
                    var flags = sc.domain.Typecode() + (dn.domain.notNull ? 0x100 : 0) +
                    ((dn.generated != GenerationRule.None) ? 0x200 : 0);
                    PutInt(flags);
                }
            Flush();
        }
        /// <summary>
        /// Send ReadCheck information if present
        /// </summary>
        /// <param name="rs"></param>
        static internal (string?,string?) Check(Context cx, Cursor rb)
        {
            if (!cx.versioned)
                return (null, null);
            var rec = rb.Rec();
            string? rc = null;
            var ro = cx.role;
            if (rec != null && rec != BList<TableRow>.Empty)
            {
                var sb = new StringBuilder();
                var cm = "";
                for (var b = rec.First(); b != null; b = b.Next())
                    for (var c = b.value().tabledefpos.First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is DBObject ob && ob.infos[ro.defpos] is ObInfo md &&
                                md.metadata.Contains(Sqlx.ENTITY))
                        {
                            var tr = b.value();
                            sb.Append(cm); cm = ",";
                            sb.Append('/');
                            sb.Append(tr.tabledefpos);
                            sb.Append('/');
                            sb.Append(tr.defpos);
                            sb.Append('/');
                            sb.Append(tr.ppos);
                        }
                rc = sb.ToString();
            }
            return (rb._Rvv(cx).ToString(),rc);
        }
        /// <summary>
        /// Send a data cell.
        /// Normal result of SELECT in client-server comms.
        /// Used in server-server comms to collect traversal conditions,
        /// and thus reduce the amount of data transferred
        /// </summary>
        /// <param name="nt"></param>
        /// <param name="tv"></param>
        internal void PutCell(Context _cx, Domain dt, TypedValue p, string? rv=null, string? rc=null)
        {
            p = dt.Coerce(_cx, p);
            if (rv is not null)
            {
                WriteByte(3);
                PutString(rv);
            } 
            if (rc is not null)
            {
                WriteByte(4);
                PutString(rc);
            }

            if (p == TNull.Value)
            {
                WriteByte(0);
                return;
            }
            if (dt.CompareTo(p.dataType)==0 || p.dataType is NodeType)
                WriteByte(1);
            else if (dt is UDType ut && ut.prefix is string pf)
            {
                WriteByte(5);
                PutString(pf);
                PutString(p.dataType.name);
                PutInt(p.dataType.Typecode());
            }
            else if (dt is UDType vt && vt.suffix is string sf)
            {
                WriteByte(6);
                PutString(sf);
                PutString(p.dataType.name);
                PutInt(p.dataType.Typecode());
            }
            else if (p is TTypeSpec)
            {
                WriteByte(2);
                PutString("CHAR");
                PutInt(Domain.Char.Typecode());
            }
            else
            {
                WriteByte(2);
                PutString(p.dataType.name);
                PutInt(p.dataType.Typecode());
            }
            PutData(_cx,p);
        }
        /// <summary>
        /// Send obs cell contents.
        /// Normal result of SELECT in client-server comms.
        /// </summary>
        /// <param name="tv"></param>
        internal void PutData(Context _cx, TypedValue tv)
        {
            switch (tv.dataType.kind)
            {
                case Sqlx.Null: break;
                case Sqlx.SENSITIVE:
                    PutData(_cx, ((TSensitive)tv).value);
                    break;
                case Sqlx.BOOLEAN:
                    {
                        PutInt((tv.ToBool() is bool b)?(b?1:0):-1);
                        break;
                    }
                case Sqlx.INTEGER:
                    {
                        if (tv is TInteger ti)
                            PutInteger(ti.ivalue);
                        else
                        {
                            var iv = (TInt)tv;
                            if (iv.ToInt() is int v)
                                PutInteger(new Integer((long)v));
                            else
                                PutInteger(new Integer(iv.value));
                        }
                    }
                    break;
                case Sqlx.NUMERIC:
                    {
                        Numeric v;
                        if (tv is TNumeric nv)
                            v = nv.value;
                        else if (tv.ToLong() is long lv)
                            v = new Numeric(lv);
                        else if (tv.ToDouble() is double dv)
                            v = new Numeric(dv);
                        else break;
                        PutNumeric(v);
                    }
                    break;
                case Sqlx.REAL:
                    {
                        Numeric v;
                        if (tv is TNumeric nv)
                            v = nv.value;
                        else if (tv.ToDouble() is double dv)
                            v = new Numeric(dv);
                        else break;
                        PutReal(v);
                    }
                    break;
                case Sqlx.NCHAR:
                case Sqlx.CLOB: 
                case Sqlx.NCLOB: 
                case Sqlx.LEVEL:
                case Sqlx.CHAR:
                    PutString(tv.ToString());
                    break;
                case Sqlx.PASSWORD: PutString("********"); break;
                case Sqlx.POSITION:
                    PutString(tv.ToString()); break;
                case Sqlx.DATE:
                    {
                        if (tv.ToLong() is long tl)
                            PutDateTime(new DateTime(tl));
                        else if (tv is TDateTime td) // backward compatibility
                            PutDateTime(td.value);
                        break;
                    }
                case Sqlx.TIME:
                    {
                        if (tv.ToLong() is long tt)
                            PutTimeSpan(new TimeSpan(tt));
                        else if (tv is TTimeSpan sp && sp.value is TimeSpan s)
                            PutTimeSpan(s);
                        else throw new PEException("PE42161");
                        break;
                    }
                case Sqlx.TIMESTAMP:
                    {
                        if (tv.ToLong() is long ts)
                            PutDateTime(new DateTime(ts));
                        else if (tv is TDateTime d)
                            PutDateTime(d.value);
                        else throw new PEException("PE42162");
                        break;
                    }
                case Sqlx.DOCUMENT:
                    {
                        if (tv is TDocument d)
                            PutBytes(d.ToBytes(null));
                        else throw new PEException("PE42163");
                        break;
                    }
                case Sqlx.DOCARRAY:
                    {
                        if (tv is TDocArray d)
                            PutBytes(d.ToBytes());
                        else throw new PEException("PE42164");
                        break;
                    }
                case Sqlx.OBJECT: PutString(tv.ToString()); break;
                case Sqlx.BLOB:
                    {
                        if (tv is TBlob d)
                            PutBytes(d.value);
                        else throw new PEException("PE42165");
                        break;
                    }
                case Sqlx.REF:
                case Sqlx.ROW: PutRow(_cx, (TRow)tv); break; // different!
                case Sqlx.ARRAY: PutArray(_cx, tv); break;
                case Sqlx.SET:
                    {
                        if (tv is TSet d)
                            PutSet(_cx, d);
                        else throw new PEException("PE42166");
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        if (tv is TMultiset d)
                            PutMultiset(_cx, d);
                        else throw new PEException("PE42166");
                        break;
                    }
                case Sqlx.INTERVAL:
                    {
                        if (tv is TInterval d)
                        PutInterval(d.value);
                        else throw new PEException("PE42168");
                        break;
                    }
                case Sqlx.NODETYPE:
                case Sqlx.EDGETYPE:
                    if (tv is TRow && tv.dataType is NodeType nt)
                        PutString(nt.Describe(_cx));
                    else
                        PutString(tv.ToString(_cx));
                    break;
                case Sqlx.TYPE: 
                    if (tv.dataType is UDType u && _cx.db.objects[u.defpos] is UDType ut)// may be different!
                    {
                        var tf = ut.rowType.First();
                        if (ut.prefix != null)
                        {
                            if (tf != null && tv is TRow tr && tr.values[tf.value()??-1L] is TypedValue nv)
                                tv = nv;
                            PutString(ut.prefix + tv.ToString());
                            break;
                        }
                        if (ut.suffix is not null)
                        {
                            if (tf != null && tv is TRow tr && tr.values[tf.value()??-1L] is TypedValue nv)
                                tv = nv;
                            PutString(tv.ToString()+ut.suffix);
                            break;
                        }
                        if (tv is TTypeSpec tt)
                        {
                            PutString(tt._dataType.name);
                            break;
                        }
                    }
                    goto case Sqlx.ROW;
                case Sqlx.TYPE_URI: PutString(tv.ToString()); break;
                default:
                    PutString(tv.ToString()); break;
            }
        }
        /// <summary>
        /// obs transfer API
        /// </summary>
        /// <returns>a tree of strings from the stream</returns>
        internal string[] GetStrings()
        {
            int n = GetInt();
            string[] obs = new string[n];
            for (int j = 0; j < n; j++)
                obs[j] = GetString();
            Flush();
            return obs;
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        Responses ServerGetException()
        {
            rcount = (rbuf.bytes[rpos++] << 7) + (rbuf.bytes[rpos++] & 0x7f);
            rcount += 2;
            var proto = (Responses)rbuf.bytes[rpos++];
            Exception? e;
            string sig;
            switch (proto)
            {
                case Responses.Exception: sig = GetString(); e = new DBException(sig, GetStrings()); break;
                case Responses.FatalError: sig = "2E206"; e = new DBException(sig, GetString()).Mix(); break;
                case Responses.TransactionConflict: sig = "40001"; e = new DBException(sig, GetString()).ISO(); break;
                default:
                    Console.WriteLine("Unexpected response " + (int)proto);
                    return proto;
            }
            throw e;
        }
    }
}
