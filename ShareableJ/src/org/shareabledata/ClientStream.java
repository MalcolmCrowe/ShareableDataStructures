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
        StrongConnect connect = null;
        Socket client;
        Reader rbuf;
        int rx = 0;
        ClientStream(StrongConnect pc, Socket c) throws Exception
        {
            client = c;
            wbuf = new Buffer(this);
            rbuf = new Reader(this);
            rbuf.pos = 2;
            rbuf.buf.len = 0;
            wbuf.wpos = 2;
            connect = pc;
        }
        @Override
        protected boolean GetBuf(Buffer b) throws Exception
        {
            var rcount = 0;
            rx = 0;
            try
            {
                var rc = client.getInputStream().read(b.buf, 0, Buffer.Size);
                if (rc == 0)
                {
                    rcount = 0;
                    return false;
                }
                rcount = ((b.buf[0] & 0xff) << 7) + (b.buf[1] &0xff);
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
                int x = rbuf.ReadByte();
                if (x < 0)
                    break;
                buffer[offset + j] = (byte)x;
            }
            return j;
        }
        public byte Receive() throws Exception
        {
            if (wbuf.wpos > 2)
                Flush();
            return (byte)rbuf.ReadByte();
        }
        protected void PutBuf(Buffer b)
        {
            Flush();
            b.wpos = 2;
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
            rbuf.pos = 2;
            rbuf.buf.len = 0;
            // now always send bSize bytes (not wcount)
            wbuf.wpos -= 2;
            wbuf.buf[0] = (byte)(wbuf.wpos >> 7);
            wbuf.buf[1] = (byte)(wbuf.wpos & 0x7f);
            try
            {
                var s = client.getOutputStream();
                s.write(wbuf.buf, 0, Buffer.Size);
                s.flush();
                wbuf.wpos = 2;
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
            Buffer bf = rbuf.buf;
            if (proto == Responses.Exception)
            {
                var rcount = (((int)bf.buf[rbuf.pos++]) << 7) + (((int)bf.buf[rbuf.pos++]) & 0x7f);
                bf.len = rcount + 2;
                proto = bf.buf[rbuf.pos++];
            }
            throw new Exception(rbuf.GetString());
        }

    @Override
    protected long getLength() {
        return 0;
    }
}
