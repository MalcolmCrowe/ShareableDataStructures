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
public abstract class WriterBase extends IOBase {
    public void Write(int t) throws Exception
    {
        WriteByte((byte)t);
    }
    public void PutInt(int n) throws Exception
    {
        PutInteger(new Bigint(n));
    }
    public void PutInteger(Bigint b) throws Exception
    {
        var m = b.bytes.length;
        WriteByte((byte)m);
        for (int j = 0; j<m ; j++)
            WriteByte(b.bytes[j]);
    }
    public void PutLong(long n) throws Exception
    {
        PutInteger(new Bigint(n));
    }
    public void PutString(String s) {
        try {
            byte[] cs = s.getBytes("UTF-8");
            PutInt(cs.length);
            for (int i = 0; i < cs.length; i++)
                WriteByte(cs[i]);
        } catch(Exception e)
        {
            throw new Error("UTF-8 Coding error");
        }
    }
}
