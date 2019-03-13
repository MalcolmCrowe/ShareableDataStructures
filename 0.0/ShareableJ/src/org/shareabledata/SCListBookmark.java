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
    public class SCListBookmark<K extends Comparable> extends SListBookmark<K> 
    {
        public final SCList<K> _s;
        SCListBookmark(SCList<K> s, int p) { super(s,p); _s = s; }
        @Override
        public Bookmark<K> Next()
        {
            return (_s.Length <= 1) ? null : 
                    new SCListBookmark<>((SCList<K>)_s.next, Position + 1);
        }
    }
