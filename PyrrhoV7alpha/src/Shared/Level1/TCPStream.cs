using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
#if WINDOWS_PHONE
using Windows.Networking.Sockets;
#else
using System.Net;
using System.Net.Sockets;
#endif
using System.Threading;
using Pyrrho;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Security;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level1
{
    /// <summary>
    /// On the network there are
    /// serious advantages in making all messages exactly the same size.
    /// </summary>
    internal class TCPStream : Stream
    {
        internal string debug = null;
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
            internal ManualResetEvent wait = null;
        }
        /// <summary>
        /// The array of write buffers
        /// </summary>
        internal Buffer[] wbufs = new Buffer[2];
        /// <summary>
        /// the single read buffer
        /// </summary>
        internal Buffer rbuf = new Buffer();
        /// <summary>
        /// points to the current write buffer
        /// </summary>
        internal Buffer wbuf = null;
#if WINDOWS_PHONE
        internal StreamSocket client;
#endif
#if (!SILVERLIGHT) && (!WINDOWS_PHONE) 
        /// <summary>
        /// The client Socket
        /// </summary>
        internal Socket client;
#endif
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
        internal string tName; // name of the thread
        static int _uid = 0;
        internal int uid = ++_uid;
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
            if (wcount != 2)
                Flush();
            if (rpos < rcount + 2)
                return rbuf.bytes[rpos++];
            rpos = 2;
            rcount = 0;
            rx = 0;
            try
            {
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
                if (rc + rx == bSize)
                {
                    rcount = (((int)buf.bytes[0]) << 7) + (int)buf.bytes[1];
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
            if (debug != null)
                Console.WriteLine(debug + value.ToString("X"));
            if (wcount < bSize)
                wbuf.bytes[wcount++] = value;
            if (wcount >= bSize)
                WriteBuf();
        }
        public void Write(Protocol p)
        {
            WriteByte((byte)p);
        }
        public void Write(Responses p)
        {
            WriteByte((byte)p);
        }
        /// <summary>
        /// flush a write buffer to the network
        /// </summary>
        private void WriteBuf()
        {
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
                if (wbuf.wait != null)
                    wbuf.wait.WaitOne();
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
            try
            {
                Buffer buf = ar.AsyncState as Buffer;
                client.EndSend(ar);
                buf.wait.Set();
            }
            catch (Exception)
            {
                //          Console.WriteLine("Socket (" + uid + ") Exception reported on Write");
            }
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
            if (wcount == 2)
                return;
            int ox = (wx + 1) & 1;
            Buffer obuf = wbufs[ox];
            if (obuf.wait != null)
                obuf.wait.WaitOne();
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            if (exception) // version 2.0
                unchecked
                {
                    exception = false;
                    wbuf.bytes[0] = (byte)((bSize - 1) >> 7);
                    wbuf.bytes[1] = (byte)((bSize - 1) & 0x7f);
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
        /// instead. Note that it is possible for exception data to be
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
        /// data transfer API
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
        /// data transfer API
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
            Read(bytes, 0, 4);
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
        /// data transfer API
        /// </summary>
        /// <param name="n">a Unicode string for the stream</param>
        internal void PutString(string n)
        {
            byte[] b = Encoding.UTF8.GetBytes(n);
            PutInt(b.Length);
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
        internal void PutWarnings(Transaction tr)
        {
            for (var b=tr.warnings.First();b!=null;b=b.Next())
            {
                var e = (DBException)b.value();
                Write(Responses.Warning);
                PutString(e.signal);
                PutInt(e.objects.Length);
                for (int i = 0; i < e.objects.Length; i++)
                    PutString(e.objects[i].ToString());
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
        /// data transfer API
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
        /// data transfer API
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
            var n = (int)r.Length;
            PutInt(n);
            string[] names = new string[n];
            for (int j = 0; j < n; j++)
            {
                var c = r[j];
                string kn = "";
                if (r.info.Length > j)
                    kn = r.info.columns[j].name;
                PutString(kn);
                var dt = r.info.columns[j].domain;
                PutString(dt.ToString());
                PutInt(dt.Typecode()); // other flags are 0
                PutCell(_cx,dt, c);
            }
        }
        /// <summary>
        /// Send an Array value to the client
        /// </summary>
        /// <param name="a">the Array</param>
        internal void PutArray(Context _cx, TArray a)
        {
            PutString("ARRAY");
            int n = a.list.Count;
            var et = a.dataType.elType ?? ((a.Length > 0) ? a[0].dataType : Domain.Content);
            PutString(et.ToString());
            PutInt(et.domain.Typecode());
            PutInt(n);
            for (int j = 0; j < n; j++)
                PutCell(_cx,et.domain, a[j]);
        }
        /// <summary>
        /// send a Multiset to the client
        /// </summary>
        /// <param name="m">the Multiset</param>
        internal void PutMultiset(Context _cx, TMultiset m)
        {
            PutString("MULTISET");
            var e = m.First();
            var et = m.dataType.elType ?? m?.dataType ?? Domain.Content;
            PutString(et.ToString());
            PutInt(et.domain.Typecode());
            PutInt((int)m.Count);
            for (; e != null; e = e.Next())
                PutCell(_cx,et.domain, e.Value());
        }
        /// <summary>
        /// Send a Table to the client
        /// </summary>
        /// <param name="r">the RowSet</param>
        internal void PutTable(Context _cx, RowSet r)
        {
            PutString("TABLE");
            _cx.result = r.qry.defpos;
            PutSchema(_cx);
            int n = 0;
            for (var e = r.First(_cx); e != null; e = e.Next(_cx))
                n++;
            PutInt(n);
            for (var e = r.First(_cx); e != null; e = e.Next(_cx))
            {
                var dt = r.rowType;
                for (int i = 0; i < dt.Length; i++)
                    PutCell(_cx,dt.columns[i].domain, e.row[dt.columns[i].defpos]);
            }
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
        /// Send the list of filename to the client
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
                    s = s.Substring(m + 1);
                m = s.LastIndexOf("/");
                if (m >= 0)
                    s = s.Substring(m + 1);
                int n = s.Length - 4;
                if (s.IndexOf(".", 0, n) >= 0)
                    continue;
                ar.Add(s.Substring(0, n));
            }
            WriteByte((byte)Responses.Files);
            PutInt(ar.Count);
            for (int j = 0; j < ar.Count; j++)
                PutString((string)ar[j]);
        }

        /// <summary>
        /// Send a result schema to the client
        /// </summary>
        /// <param name="rowSet">the results</param>
        internal void PutSchema(Context cx)
        {
            var result = cx.data[cx.result];
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
            WriteByte(13);
#else
            Write(Responses.Schema);
#endif
            var dt = result.rowType;
            int m = result.qry.display;
            PutInt(m);
            if (m == 0)
            {
                Console.WriteLine("No columns?");
                throw new PEException("PE246");
            }
            if (m > dt.Length)
            {
                Console.WriteLine("Unreasonable rowType length " + dt.Length + " < " + m);
                throw new PEException("PE247");
            }
            if (m > 0)
            {
                PutString("Data");
                int[] flags = new int[m];
                result.Schema(dt, flags);
                for (int j = 0; j < m; j++)
                {
                    var dn = dt.columns[j];
                    var dc = (SqlValue)cx.obs[dn.defpos];
                    PutString(dc.alias??dc.name??("Col"+j));
                    PutString(DBObject.Uid(dn.domain.defpos));
                    PutInt(flags[j]);
                }
            }
            Flush();
        }
        /// <summary>
        /// Send a schemaKey and result schema to the client for a weakly typed language
        /// </summary>
        /// <param name="rowSet">the results</param>
        internal void PutSchema1(RowSet result)
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
            if (result.qry is From fm) // compute the schemakey
                PutLong(fm.lastChange);
            else
                PutLong(0);
            var dt = result.rowType;
            int m = result.qry.display;
            PutInt(m);
            if (m == 0)
                Console.WriteLine("No columns?");
            if (m > dt.Length)
                Console.WriteLine("Unreasonable rowType length " + dt.Length + " < " + m);
            if (m > 0)
            {
                PutString("Data");
                int[] flags = new int[m];
                result.Schema(dt, flags);
                for (int j = 0; j < m; j++)
                {
                    var dn = dt.columns[j];
                    PutString(dn.name);
                    PutString((dn.domain.kind == Sqlx.DOCUMENT) ? "DOCUMENT" : dn.name);
                    PutInt(flags[j]);
                }
            }
            Flush();
        }
        internal void PutColumns(ObInfo dt)
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
                if (dt.columns[j] is SqlCol sc && sc.tableCol is TableColumn dn)
                {
                    PutString(sc.name);
                    PutString((dn.domain.kind == Sqlx.DOCUMENT) ? "DOCUMENT" : sc.name);
                    var flags = dn.domain.Typecode() + (dn.notNull ? 0x100 : 0) +
                    ((dn.generated != GenerationRule.None) ? 0x200 : 0);
                    PutInt(flags);
                }
            Flush();
        }
        /// <summary>
        /// Send a key.
        /// Used in server-server comms to collect traversal conditions,
        /// and thus reduce the amount of data transferred
        /// </summary>
        /// <param name="link"></param>
        internal void PutKey(Context _cx, PRow key)
        {
            PutLong(key.Length);
            var vs = new TypedValue[key.Length];
            for (int i = key.Length - 1; i >= 0; i--)
                PutCell(_cx,key[i].dataType, key[i]);
        }
        /// <summary>
        /// Send a data cell.
        /// Normal result of SELECT in client-server comms.
        /// Used in server-server comms to collect traversal conditions,
        /// and thus reduce the amount of data transferred
        /// </summary>
        /// <param name="nt"></param>
        /// <param name="tv"></param>
        internal void PutCell(Context _cx, Domain dt, TypedValue p)
        {
            if (p == null || p.IsNull)
            {
                WriteByte(0);
                return;
            }
            if (dt.Equals(p.dataType))
                WriteByte(1);
            else
            {
                WriteByte(2);
                PutString(p.dataType.ToString());
                PutInt(p.dataType.Typecode());
            }
            PutData(_cx,p);
        }
        /// <summary>
        /// Send data cell contents.
        /// Normal result of SELECT in client-server comms.
        /// Used in server-server comms to collect traversal conditions,
        /// and thus reduce the amount of data transferred
        /// </summary>
        /// <param name="tv"></param>
        internal void PutData(Context _cx, TypedValue tv)
        {
            switch (tv.dataType.kind)
            {
                case Sqlx.Null: break;
                case Sqlx.SENSITIVE:
                    PutData(_cx,((TSensitive)tv).value);
                    break;
                case Sqlx.BOOLEAN:
                    PutInt(((bool)tv.Val()) ? 1 : 0);
                    break;
                case Sqlx.INTEGER:
                    {
                        var v = tv.Val();
                        if (v is int)
                            v = (long)(int)v;
                        if (v is long)
                            v = new Integer((long)v);
                        PutInteger((Integer)v);
                    }
                    break;
                case Sqlx.LEVEL:
                    PutString(tv.ToString());
                    break;
                case Sqlx.NUMERIC:
                    {
                        var v = tv.Val();
                        if (v is long)
                            v = new Numeric((long)v);
                        else if (tv.Val() is double)
                            v = new Numeric((double)v);
                        PutNumeric((Numeric)v);
                    }
                    break;
                case Sqlx.REAL:
                    {
                        var v = tv.Val();
                        if (!(v is Numeric))
                            v = new Numeric((double)v);
                        PutReal((Numeric)v);
                    }
                    break;
                case Sqlx.NCHAR:
                    goto case Sqlx.CHAR;
                case Sqlx.CLOB: goto case Sqlx.CHAR;
                case Sqlx.NCLOB: goto case Sqlx.CHAR;
                case Sqlx.CHAR:
                    PutString(tv.ToString());
                    break;
                case Sqlx.PASSWORD: PutString("********"); break;
                case Sqlx.DATE:
                    if (tv.Val() is long)
                        PutDateTime(new DateTime(tv.ToLong().Value));
                    else if (tv.Val() is DateTime) // backward compatibility
                        PutDateTime((DateTime)tv.Val());
                    else
                        PutDateTime(((Date)tv.Val()).date); break;
                case Sqlx.TIME:
                    if (tv.Val() is long)
                        PutTimeSpan(new TimeSpan(tv.ToLong().Value));
                    else
                        PutTimeSpan((TimeSpan)tv.Val()); break;
                case Sqlx.TIMESTAMP:
                    if (tv.Val() is long)
                        PutDateTime(new DateTime(tv.ToLong().Value));
                    else
                        PutDateTime((DateTime)tv.Val()); break;
                case Sqlx.DOCUMENT: PutBytes(((TDocument)tv.Val()).ToBytes(null)); break;
                case Sqlx.DOCARRAY: PutBytes(((TDocArray)tv.Val()).ToBytes()); break;
                case Sqlx.OBJECT: PutString(tv.ToString()); break;
                case Sqlx.BLOB: PutBytes((byte[])tv.Val()); break;
                case Sqlx.REF:
                case Sqlx.ROW: PutRow(_cx,tv as TRow); break; // different!
                case Sqlx.ARRAY: PutArray(_cx,(TArray)tv); break;
                case Sqlx.MULTISET: PutMultiset(_cx,(TMultiset)tv.Val()); break;
                case Sqlx.TABLE: PutTable(_cx,(RowSet)tv.Val()); break;
                case Sqlx.INTERVAL: PutInterval((Interval)tv.Val()); break;
                case Sqlx.TYPE: goto case Sqlx.ROW;
                case Sqlx.TYPE_URI: PutString(tv.ToString()); break;
                case Sqlx.XML: PutString(tv.ToString()); break;
                default:
                    PutString(tv.ToString()); break;
            }
        }
        /// <summary>
        /// data transfer API
        /// </summary>
        /// <returns>a list of strings from the stream</returns>
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
            Exception e = null;
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
