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
public class SysRows extends SDict<Long,Long> {
        public final SDatabase db;
        public final SysTable tb;
        SysRows(SDatabase d,SysTable t)
        {
            super(null);
            db = d; tb = t;
        }
        public Bookmark<SSlot<Long, Long>> First()
        {
            switch (tb.name)
            {
                case "_Log": return LogBookmark.New(db,0,0);
            }
            return null;
        }
}
