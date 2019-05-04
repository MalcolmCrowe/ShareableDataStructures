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
public class LogBookmark extends RowBookmark {
    public final SysRows _sr;
    public final long _log;
    public final long _next;
    LogBookmark(SysRows rs,long lg,SDbObject ob,long nx,int p)
            throws Exception
    {
        super(rs,_Cx(rs,rs._Row(new SString(ob.Uid()),
            new SString(new Types().types[ob.type]), //Type
            new SString(ob.toString()),
            new SString(rs._tr.role.uids.Contains(ob.uid)?rs._tr.Name(ob.uid):""),
            new SString(SDbObject._Uid(ob.getAffects()))),null),p);
        _sr = rs; _log = lg; _next = nx;
    }
    public static LogBookmark New(SysRows rs)
    {
        try {
            var rdr = new Reader(rs._tr,0);
            SDbObject s = (SDbObject)rdr._Get();
            return (s == null) ? null
                    : new LogBookmark(rs, 0, 
                            s, rdr.Position(), 0);
        } catch(Exception e)
        {
            return null;
        }
    }
    @Override
    public Bookmark<Serialisable> Next()
    {
        try{
        var rdr = new Reader(_sr._tr,_next);
        var ob = rdr._Get();
        SDbObject s = (SDbObject)ob;
        return (s==null)?null:new LogBookmark(_sr,_next,s,
          rdr.Position(),Position + 1);
        } catch(Exception e) { return null; }
    }
}
