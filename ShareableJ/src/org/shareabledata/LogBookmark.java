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
public class LogBookmark extends SysBookmark {
                public final SDatabase _db;
            public final long _log;
            public final long _next;
            LogBookmark(SDatabase d,long lg,Serialisable ob,long nx,int p)
            {
                super(ob,p);
                _db = d; _log = lg; _next = nx;
            }
            static LogBookmark New(SDatabase db,long lg,int pos) 
            {
                try
                {
                var si = db._Get(lg);
                return (si==null)?null:
                    new LogBookmark(db, lg, si.item, si.next, pos);
                } catch(Exception e)
                {
                    return null;
                }
            }

            public SSlot<Long, Long> getValue()
            {
                return new SSlot<Long, Long>(_log,_log);
            }

            public Bookmark<SSlot<Long, Long>> Next()
            {
                return New(_db, _next, Position + 1);
            }
}
