/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 * This class is not shareable
 * 
 * @author Malcolm
 */
public abstract class StreamBase {
    protected Buffer wbuf;
    protected long wposition = 0;
    protected StreamBase() {}
    protected abstract long getLength();
    protected abstract boolean GetBuf(Buffer b);
    protected abstract void PutBuf(Buffer b);
    public void WriteByte(byte value)  {
        if (wbuf==null)
            throw new Error("Panic");
        wbuf.PutByte(value);
    }
        public void PutInt(int n)
        {
            PutInt(new Bigint(n));
        }
        public void PutInt(Bigint b) 
        {
            var m = b.bytes.length;
            WriteByte((byte)m);
            for (int j = 0; j<m ; j++)
                WriteByte(b.bytes[j]);
        }
        public void PutLong(long n) 
        {
            PutInt(new Bigint(n));
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
