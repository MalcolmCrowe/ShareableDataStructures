/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.IOException;

/**
 *
 * @author Malcolm
 */
public class Buffer {

    public static final int Size = 1024;
    public byte[] buf;
    public long start;
    public int len;
    public int wpos;
    public boolean busy = false;
    StreamBase fs;

    public Buffer(StreamBase f) {
        buf = new byte[Size];
        wpos = 0;
        len = Size;
        start = f.getLength();
        fs = f;
    }

    Buffer(StreamBase f, long s)  {
        buf = new byte[Size];
        start = s;
        wpos = 0;
        f.GetBuf(this);
        fs = f;
    }

    int GetByte() {
        if (wpos >= len) {
            start += len;
            wpos = 0;
            if (!fs.GetBuf(this))
               return -1;
        }
        return buf[wpos++];
    }

    void PutByte(byte b)  {
        if (wpos >= len) {
            fs.PutBuf(this);
            start += len;
            wpos = 0;
        }
        buf[wpos++] = b;
    }
}
