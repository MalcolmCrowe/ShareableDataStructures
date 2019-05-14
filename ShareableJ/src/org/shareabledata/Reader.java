/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.util.ArrayList;
import java.util.List;
import java.io.*;
/**
 * This class is not shareable
 * @author Malcolm
 */
public class Reader extends ReaderBase {
    public RandomAccessFile file;
    public final long limit;
    public boolean GetBuf(long s) throws Exception
    {
        int m = (limit == 0 || limit >= s + Buffer.Size) ? Buffer.Size : (int)(limit - s);
        synchronized(file)
        {
            file.seek(s);
            buf.len = file.read(buf.buf, 0, m);
        }
        buf.start = s;
        return buf.len>0;
    }
    @Override
    public int ReadByte() throws Exception
    {
        if (Position() >= limit)
            return -1;
        if (buf.pos==buf.len)
        {
            if (!GetBuf(buf.start + buf.len))
                return -1;
            buf.pos = 0;
        }
        return buf.buf[buf.pos++];
    }
    Reader(SDatabase d) throws Exception
    {
        db = d;
        file = d.File();
        limit = file.length();
        GetBuf(d.curpos);
    }
    Reader(SDatabase d, long s) throws Exception
    {
        db = d;
        db = d;
        file = d.File();
        limit = d.curpos;
        GetBuf(s);
    }
}
