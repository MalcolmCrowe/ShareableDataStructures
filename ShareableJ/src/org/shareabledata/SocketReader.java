/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.Socket;

/**
 *
 * @author Malcolm
 */
public class SocketReader extends ReaderBase {
    protected Socket client;
    public SocketReader(Socket c) throws Exception
    {
        client = c;
    }
    /// <summary>
    /// Get a byte from the stream: if necessary refill the buffer from the network
    /// </summary>
    /// <returns>the byte</returns>
    @Override
    public boolean GetBuf(long s) // s is ignored for ServerStream
    {
        int rcount;
        try
        {
            var rc = client.getInputStream().read(buf.buf);
            if (rc == 0)
            {
                rcount = 0;
                return false;
            }
            rcount = (buf.buf[0] << 7) + buf.buf[1];
            buf.len = rcount + 2;
            return rcount > 0;
        }
        catch (Exception e)
        {
            return false;
        }
    }
    @Override
    public int ReadByte() throws Exception
    {
        if (buf.pos >= buf.len)
        {
            if (!GetBuf(0))
                throw new Exception("EOF on input");
            buf.pos = 2;
        }
        return (buf.len == 0) ? -1 : buf.buf[buf.pos++];
    }
    @Override
    public STable GetTable() throws Exception
    {
        var un = GetLong();
        var nm = db.role.uids.get(un);
        if (db.role.globalNames.defines(nm))
        {
            var tb = (STable)db.objects.get(db.role.globalNames.get(nm));
            context = tb;
            return tb;
        }
        throw new Exception("No such table " + nm);
    }
}
