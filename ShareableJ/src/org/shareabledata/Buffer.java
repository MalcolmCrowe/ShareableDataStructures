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
    boolean eof;
    AStream fs;

    public Buffer(AStream f) throws IOException {
        buf = new byte[Size];
        pos = 0;
        len = Size;
        start = f.file.length();
        eof = false;
        fs = f;
    }

    Buffer(AStream f, long s) throws IOException {
        buf = new byte[Size];
        start = s;
        pos = 0;
        f.file.seek(start);
        len = f.file.read(buf, 0, Size);
        eof = len < Size;
        fs = f;
    }

    int GetByte() throws IOException {
        if (pos >= len) {
            if (eof) {
                return -1;
            }
            start += len;
            pos = 0;
            fs.file.seek(start);
            len = fs.file.read(buf, 0, Size);
            eof = len < Size;
        }
        return buf[pos++];
    }

    void PutByte(byte b) throws IOException {
        if (pos >= len) {
            fs.file.seek(fs.file.length());
            fs.file.write(buf, 0, len);
            start += len;
            pos = 0;
        }
        buf[pos++] = b;
    }
}
