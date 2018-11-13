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
    public int pos;
    public boolean busy = false;
    StreamBase fs;

    public Buffer(StreamBase f) throws IOException {
        buf = new byte[Size];
        pos = 0;
        len = Size;
        start = f.getLength();
        fs = f;
    }

    Buffer(AStream f, long s) throws Exception {
        buf = new byte[Size];
        start = s;
        pos = 0;
        f.GetBuf(this);
        fs = f;
    }

    int GetByte() throws Exception {
        if (pos >= len) {
            start += len;
            pos = 0;
            if (!fs.GetBuf(this))
               return -1;
        }
        return buf[pos++];
    }

    void PutByte(byte b) throws Exception {
        if (pos >= len) {
            fs.PutBuf(this);
            start += len;
            pos = 0;
        }
        buf[pos++] = b;
    }
}
