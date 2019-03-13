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
public class SArrayBookmark<T> extends Bookmark<T> {
        final SArray<T> _a;
        SArrayBookmark(SArray<T> a, int p)
        { 
            super(p);
            _a = a; 
        }
        public Bookmark<T> Next()
        {
            return (Position+1 >= _a.elements.length) ? null 
                : new SArrayBookmark<T>(_a, Position+1);
        }
        public T getValue()
        {
            return _a.elements[Position];
        }
}
