/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is not shareable
 * 
 * @author Malcolm
 */
public abstract class StreamBase {
    protected Buffer rbuf, wbuf;
    protected long position = 0;
    protected StreamBase() {}
        protected abstract boolean GetBuf(Buffer b) throws Exception;
        protected abstract void PutBuf(Buffer b)throws Exception;
    public Serialisable _Get(SDatabase d) throws Exception {
        int tp = ReadByte();
        Serialisable s = null;
        switch (tp) {
            case Types.Serialisable:
                s = Serialisable.Get(this);
                break;
            case Types.STimestamp:
                s = STimestamp.Get(this);
                break;
            case Types.SInteger:
                s = SInteger.Get(this);
                break;
            case Types.SNumeric:
                s = SNumeric.Get(this);
                break;
            case Types.SString:
                s = SString.Get(this);
                break;
            case Types.SDate:
                s = SDate.Get(this);
                break;
            case Types.STimeSpan:
                s = STimeSpan.Get(this);
                break;
            case Types.SBoolean:
                s = SBoolean.Get(this);
                break;
            case Types.STable:
                s = STable.Get(this);
                break;
            case Types.SRow:
                s = SRow.Get(d, this);
                break;
            case Types.SColumn:
                s = SColumn.Get(this);
                break;
            case Types.SRecord:
                s = SRecord.Get(d, this);
                break;
            case Types.SUpdate:
                s = SUpdate.Get(d, this);
                break;
                                case Types.SDelete: s = SDelete.Get(this); break;
                case Types.SAlter: s = SAlter.Get(this); break;
                case Types.SDrop: s = SDrop.Get(this); break;
                case Types.SIndex: s = SIndex.Get(d, this); break;
        }
        return s;
    }
    public int ReadByte() throws IOException {
        return rbuf.GetByte();
    }
    public void WriteByte(byte value) throws IOException {
        wbuf.PutByte(value);
    }
    public void PutInt(int n) throws IOException {
        for (int j = 24; j >= 0; j -= 8) {
            WriteByte((byte) (n >> j));
        }
    }

    public void PutLong(long t)  throws IOException {
        for (int j = 56; j >= 0; j -= 8) {
            WriteByte((byte) (t >> j));
        }
    }

    public void PutString(String s)  throws IOException{
        byte[] cs = s.getBytes("UTF-8");
        PutInt(cs.length);
        for (int i = 0; i < cs.length; i++) {
            WriteByte(cs[i]);
        }
    }

    public int GetInt() throws IOException{
        int v = 0;
        for (int j = 0; j < 4; j++) {
            v = (v << 8) + ReadByte();
        }
        return v;
    }

    public long GetLong() throws IOException {
        long v = 0;
        for (int j = 0; j < 8; j++) {
            v = (v << 8) + ReadByte();
        }
        return v;
    }

    public String GetString() throws IOException {
        int n = GetInt();
        byte[] cs = new byte[n];
        for (int j = 0; j < n; j++) {
            cs[j] = (byte) ReadByte();
        }
        return new String(cs, 0, n, "UTF-8");
    }


}
