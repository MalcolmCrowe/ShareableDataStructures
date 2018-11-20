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
public class SysBookmark extends Bookmark<SSlot<Long,Long>> {
            public final Serialisable _ob;
            protected SysBookmark(Serialisable ob,int p)
            {
                super(p);
                _ob = ob;
            }
            public SSlot<Long, Long> getValue() 
            {
                return null;
            }

            public Bookmark<SSlot<Long, Long>> Next()
            {
                return null;
            }
}
