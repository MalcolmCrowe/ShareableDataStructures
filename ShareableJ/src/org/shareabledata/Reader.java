/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Malcolm
 */
public class Reader {
        public Buffer buf;
        public int pos = 0;
        Reader(StreamBase f) throws Exception
        {
            buf = new Buffer(f);
        }
        Reader(StreamBase f, long s) throws Exception
        {
            buf = new Buffer(f, s);
        }
        long getPosition(){ return buf.start + pos; }
        public int ReadByte() throws Exception
        {
            if (pos >= buf.len)
            {
                buf = new Buffer(buf.fs, buf.start + buf.len);
                pos = 0;
            }
            return (buf.len == 0) ? -1 : (buf.buf[pos++]&0xff);
        }
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
        
    public int GetInt() throws Exception{
        int v = 0;
        for (int j = 0; j < 4; j++) {
            v = (v << 8) + (ReadByte()&0xff);
        }
        return v;
    }

    public long GetLong() throws Exception {
        long v = 0;
        for (int j = 0; j < 8; j++) {
            v = (v << 8) + (ReadByte()&0xff);
        }
        return v;
    }

    public String GetString() throws Exception {
        int n = GetInt();
        byte[] cs = new byte[n];
        for (int j = 0; j < n; j++) {
            cs[j] = (byte) ReadByte();
        }
        return new String(cs, 0, n, "UTF-8");
    }
    /// <summary>
    /// Called from Commit(): file is already locked
    /// </summary>
    /// <param name="tr"></param>
    /// <param name="pos"></param>
    /// <returns></returns>

    public SDbObject[] GetAll(SDatabase d, long pos, long max) throws Exception {
        List<SDbObject> r = new ArrayList<SDbObject>();
        while (getPosition() < max) {
            r.add((SDbObject) _Get(d));
        }
        return (SDbObject[]) r.toArray(new SDbObject[0]);
    }

}
