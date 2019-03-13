/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author 66668214
 */
public class SListBookmark<T> extends Bookmark<T> {
        final SList<T> _s;
        SListBookmark(SList<T> s, int p) 
        { 
            super(p);
            _s = s;  
        }
        public Bookmark<T> Next()
        {
            return (_s.next==null) ? null 
                    : new SListBookmark<T>(_s.next, Position + 1);
        }
        public T getValue()
        {
            return _s.element;
        }
}
