/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class SocketReader extends Reader {
    public SocketReader(StreamBase f) throws Exception
    {
        super(f);
        pos = 2;
    }
    @Override
    public int ReadByte() throws Exception
    {
        if (pos >= buf.len)
        {
            if (!buf.fs.GetBuf(buf))
                return -1;
            pos = 2;
        }
        return (buf.len == 0) ? -1 : buf.buf[pos++];
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
