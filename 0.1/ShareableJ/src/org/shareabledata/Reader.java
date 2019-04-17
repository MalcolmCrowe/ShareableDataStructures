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
        public Context ctx = null;
        public SDbObject context = SRole.Public;
        public long lastAlias = SDbObject.maxAlias;
        public SDatabase db;
        Reader(SDatabase d)
        {
            db = d;
            buf = new Buffer(d.File());
        }
        Reader(StreamBase f)
        {
            db = SDatabase._system;
            buf = new Buffer(f);
        }
        Reader(SDatabase d, long s) throws Exception
        {
            db = d;
            buf = new Buffer(d.File(), s);
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
        public Serialisable _Get() throws Exception {
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
            case Types.STable:       s = STable.Get(this);     break;
            case Types.SRow:         s = SRow.Get(this);      break;
            case Types.SColumn:      s = SColumn.Get(this);      break;
            case Types.SRecord:      s = SRecord.Get(this);   break;
            case Types.SUpdate:      s = SUpdate.Get(this);   break;
            case Types.SDelete:      s = SDelete.Get(this);   break;
            case Types.SAlter:       s = SAlter.Get(this);       break;
            case Types.SDrop:        s = SDrop.Get(this);        break;
            case Types.SIndex:       s = SIndex.Get(this);    break;
            case Types.SExpression:  s = SExpression.Get(this);break;
            case Types.SFunction:    s = SFunction.Get(this);  break;
            case Types.SInPredicate: s = SInPredicate.Get(this);break;
            case Types.SValues:      s = SValues.Get(this);    break;
            case Types.SSelect:      s = SSelectStatement.Get(this); break;
            case Types.SOrder:       s = SOrder.Get(this);     break;
            case Types.SBigInt:      s = SInteger.Get(this);      break;
            case Types.SUpdateSearch: s= SUpdateSearch.Get(this);break;
            case Types.SDeleteSearch:s = SDeleteSearch.Get(this);break;
            case Types.SSearch:      s = SSearch.Get(this);     break;
            case Types.SAlias:      s = SAlias.Get(this);break;
            case Types.SGroupQuery: s = SGroupQuery.Get(this); break;
            case Types.STableExp: s = SJoin.Get(this); break;
            case Types.SName:       s = SDbObject.Get(this); break;
            case Types.SArg:        s = new SArg(this); break;
            case Types.SDropIndex: s = new SDropIndex(this); break;
        }
        return s;
    }
    public STable GetTable() throws Exception
    {
        var tb = new STable(getPosition() -1);
        var nm = GetString();
        db = db.Install(tb,nm,getPosition());
        return tb;
    }
        
    public Bigint GetInteger() throws Exception {
        var n = ReadByte();
        var cs = new byte[n];
        for (int j = 0; j < n; j++)
            cs[j] = (byte)ReadByte();
        return new Bigint(cs);
    }
    
    public int GetInt() throws Exception
    {
        return GetInteger().toInt();
    }
    
    public long GetLong() throws Exception
    {
        return GetInteger().toLong();
    }

    public String GetString() throws Exception {
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
            r.add((SDbObject) _Get());
        }
        return (SDbObject[]) r.toArray(new SDbObject[0]);
    }

}
