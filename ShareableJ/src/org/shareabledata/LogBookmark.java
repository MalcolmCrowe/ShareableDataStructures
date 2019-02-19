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
            LogBookmark(SysRows rs,long lg,SRow ob,long nx,int p)
            {
                super(rs,ob,p);
                _sr = rs; _log = lg; _next = nx;
            }
            @Override
            public Bookmark<Serialisable> Next()
            {
                try{
                var rdr = new Reader(_sr.fs,_next);
                var ob = rdr._Get(_rs._tr);
                SDbObject s = (SDbObject)ob;
                return (s==null)?null:new LogBookmark(_sr,_next,
                        _sr._Row(new SString(SDbObject._Uid(s.uid)),
                    new SInteger((int)s.type), 
                    new SString(s.toString())),rdr.getPosition(),
                  Position + 1);
                } catch(Exception e) { return null; }
            }
}
