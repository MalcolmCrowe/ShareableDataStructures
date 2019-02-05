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
        Reader(StreamBase f)
        {
            buf = new Buffer(f);
        }
        Reader(StreamBase f, long s) 
        {
            buf = new Buffer(f, s);
        }
        long getPosition(){ return buf.start + pos; }
        public int ReadByte()
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
            case Types.Serialisable: s = Serialisable.Get(this); break;
            case Types.SInteger:     s = SInteger.Get(this);     break;
            case Types.SNumeric:     s = SNumeric.Get(this);     break;
            case Types.SString:      s = SString.Get(this);      break;
            case Types.SDate:        s = SDate.Get(this);        break;
            case Types.STimeSpan:    s = STimeSpan.Get(this);    break;
            case Types.SBoolean:     s = SBoolean.Get(this);     break;
            case Types.STable:       s = STable.Get(d,this);     break;
            case Types.SRow:         s = SRow.Get(d, this);      break;
            case Types.SColumn:      s = SColumn.Get(this);      break;
            case Types.SRecord:      s = SRecord.Get(d, this);   break;
            case Types.SUpdate:      s = SUpdate.Get(d, this);   break;
            case Types.SDelete:      s = SDelete.Get(this);      break;
            case Types.SAlter:       s = SAlter.Get(this);       break;
            case Types.SDrop:        s = SDrop.Get(this);        break;
            case Types.SIndex:       s = SIndex.Get(d, this);    break;
            case Types.SExpression:  s = SExpression.Get(d,this);break;
            case Types.SFunction:    s = SFunction.Get(d,this);  break;
            case Types.SInPredicate: s = SInPredicate.Get(d,this);break;
            case Types.SValues:      s = SValues.Get(d,this);    break;
            case Types.SSelect:      s = SSelectStatement.Get(d,this); break;
            case Types.SOrder:       s = SOrder.Get(d,this);     break;
            case Types.SBigInt:      s = SInteger.Get(this);      break;
            case Types.SUpdateSearch: s= SUpdateSearch.Get(d,this);break;
            case Types.SDeleteSearch:s = SDeleteSearch.Get(d,this);break;
            case Types.SSearch:      s = SSearch.Get(d,this);     break;
            case Types.SAliasedTable:s = SAliasedTable.Get(d,this);break;
            case Types.SDropStatement:s= SDropStatement.Get(this);break;
            case Types.SAlterStatement:s=SAlterStatement.Get(this);break;
            case Types.SGroupQuery: s = SGroupQuery.Get(d, this); break;
            case Types.SJoin: s = SJoin.Get(d, this); break;
        }
        return s;
    }
        
    public Bigint GetInteger() {
        var n = ReadByte();
        var cs = new byte[n];
        for (int j = 0; j < n; j++)
            cs[j] = (byte)ReadByte();
        return new Bigint(cs);
    }
    
    public int GetInt() 
    {
        return GetInteger().toInt();
    }
    
    public long GetLong() 
    {
        return GetInteger().toLong();
    }

    public String GetString() {
        int n = GetInt();
        byte[] cs = new byte[n];
        for (int j = 0; j < n; j++) {
            cs[j] = (byte) ReadByte();
        }
        try {
            return new String(cs, 0, n, "UTF-8");
        } catch(Exception e)
        {
            return "!! Encoding error";
        }
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
