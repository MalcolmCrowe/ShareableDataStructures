/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.*;
/**
 *
 * @author Malcolm
 */
public class ClientStream extends StreamBase {
            /// <summary>
        /// For asynchronous IO
        /// </summary>
        StrongConnect connect = null;
        Socket client;
        int rx = 0;
        int rcount = 0;
        ClientStream(StrongConnect pc, Socket c) throws Exception
        {
            client = c;
            wbuf = new Buffer(this);
            rbuf = new Buffer(this);
            rbuf.pos = 2;
            rbuf.len = 0;
            wbuf.pos = 2;
            connect = pc;
        }
        @Override
        protected boolean GetBuf(Buffer b) throws Exception
        {
            b.pos = 2;
            rcount = 0;
            rx = 0;
            try
            {
                var rc = client.getInputStream().read(b.buf, 0, Buffer.Size);
                if (rc == 0)
                {
                    rcount = 0;
                    return false;
                }
                rcount = (((int)rbuf.buf[0]) << 7) + (int)rbuf.buf[1];
                b.len = rcount + 2;
                if (rcount == Buffer.Size - 1)
                    GetException();
                return rcount> 0;
            }
            catch (SocketException e)
            {
                return false;
            }
        }
        public int Read(byte[] buffer, int offset, int count) throws Exception
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
        public byte Receive() throws Exception
        {
            if (wbuf.pos > 2)
                Flush();
            return (byte)ReadByte();
        }
        protected void PutBuf(Buffer b)
        {
            Flush();
        }
        public void Write(byte[] buffer, int offset, int count) throws Exception
        {
            for (int j = 0; j < count; j++)
                WriteByte(buffer[offset + j]);
        }
        public void Write(byte p) throws Exception
        {
            WriteByte(p);
        }
        public void Flush()
        {
            rcount = 0;
            rbuf.pos = 2;
            rbuf.len = 0;
            // now always send bSize bytes (not wcount)
            wbuf.pos -= 2;
            wbuf.buf[0] = (byte)(wbuf.pos >> 7);
            wbuf.buf[1] = (byte)(wbuf.pos & 0x7f);
            try
            {
                var s = client.getOutputStream();
                s.write(wbuf.buf, 0, Buffer.Size);
                s.flush();
                wbuf.pos = 2;
            }
            catch (Exception e)
            {
            }
        }
        int GetException() throws Exception
        {
            return GetException(Responses.Exception);
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        int GetException(byte proto) throws Exception
        {
            if (proto == Responses.Exception)
            {
                rcount = (((int)rbuf.buf[rbuf.pos++]) << 7) + (((int)rbuf.buf[rbuf.pos++]) & 0x7f);
                rcount += 2;
                proto = rbuf.buf[rbuf.pos++];
            }
            throw new Exception(GetString());
        }

    @Override
    protected long getLength() {
        return 0;
    }
}
