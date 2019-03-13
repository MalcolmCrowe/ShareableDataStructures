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
public class ServerStream extends StreamBase {
        Socket client;
        int rx = 0;
        Reader rbuf;
        boolean exception = false;
        ServerStream(Socket c) throws Exception
        {
            super();
            client = c;
            rbuf = new SocketReader(this);
            wbuf = new Buffer(this);
            wbuf.wpos = 2;
            rbuf.pos = 2;
            rbuf.buf.len = 0;
        }

        public void Flush() 
        {
            if (wbuf.wpos == 2)
                return;
            // now always send bSize bytes (not wcount)
            if (exception) // version 2.0
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
            try {
            client.getOutputStream().write(wbuf.buf, 0, Buffer.Size);
            wbuf.wpos = 2;
            } catch(Exception e)
            {
            }
        }
        /// <summary>
        /// Get a byte from the stream: if necessary refill the buffer from the network
        /// </summary>
        /// <returns>the byte</returns>
        @Override
        protected boolean GetBuf(Buffer b)
        {
            b.wpos = 2;
            var rcount = 0;
            rx = 0;
            try{
                var rc = client.getInputStream().read(b.buf, 0, Buffer.Size);
                if (rc == 0)
                {
                    rcount = 0;
                    return false;
                }
                rcount = ((b.buf[0]&0xff) << 7) + (b.buf[1]& 0xff);
                b.len = rcount + 2;
                return rcount > 0;
            }
            catch (Exception e)
            {
                return false;
            }
        }
        @Override
        protected void PutBuf(Buffer b)
        {
            Flush();
        }
        void StartException()
        {
            wbuf.wpos = 4;
            exception = true;
        }

    @Override
    protected long getLength() {
        return 0;
    }
};
