/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.net.Socket;

/**
 * This class is not shareable
 * @author Malcolm
 */
public class ClientReader extends SocketReader
{
        boolean getting;
        public ClientReader(Socket c)  throws Exception
        { super(c); } 
        public boolean GetBuf(long p) // parameter is ignored
        {
            getting = true;
            int rcount;
            try
            {
                var rc = client.getInputStream().read(buf.buf, Buffer.Size, 0);
                if (rc == 0)
                {
                    rcount = 0;
                    getting = false;
                    return false;
                }
                rcount = (buf.buf[0] << 7) + buf.buf[1];
                buf.len = rcount + 2;
                if (rcount == Buffer.Size - 1)
                    GetException();
                getting = false;
                return rcount > 0;
            }
            catch (Exception e)
            {
                return false;
            }
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        int GetException() throws Exception
        {
            var rcount = (buf.buf[buf.pos++] << 7) + (buf.buf[buf.pos++] & 0x7f);
            buf.len = rcount + 4;
            var b = buf.buf[buf.pos++];
            if (b != (byte)Types.Exception)
                throw new Exception("PE30");
            var em = GetString();
    //        Console.WriteLine("Received exception: " + em);
            throw new ServerException(em);
        }
}
