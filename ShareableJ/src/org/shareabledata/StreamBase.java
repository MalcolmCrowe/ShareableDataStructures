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
    protected abstract boolean GetBuf(Buffer b) throws Exception;
    protected abstract void PutBuf(Buffer b)throws Exception;
    public void WriteByte(byte value) throws Exception {
        if (wbuf==null)
            System.out.println("Panic");
        wbuf.PutByte(value);
    }
    public void PutInt(int n) throws Exception {
        for (int j = 24; j >= 0; j -= 8) {
            WriteByte((byte) (n >> j));
        }
    }

    public void PutLong(long t)  throws Exception {
        for (int j = 56; j >= 0; j -= 8) {
            WriteByte((byte) (t >> j));
        }
    }

    public void PutString(String s)  throws Exception{
        byte[] cs = s.getBytes("UTF-8");
        PutInt(cs.length);
        for (int i = 0; i < cs.length; i++) {
            WriteByte(cs[i]);
        }
    }

}
