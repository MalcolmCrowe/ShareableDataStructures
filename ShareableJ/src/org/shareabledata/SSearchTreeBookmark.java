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
public class SSearchTreeBookmark<T extends Comparable<T>> extends Bookmark<T> {
        final SSearchTree<T> _s;
        final SList<SSearchTree<T>> _stk;
        SSearchTreeBookmark(SSearchTree<T> s, boolean doLeft,
            SList<SSearchTree<T>> stk,
            int p) 
        {
            super(p);
            try {
            for (;doLeft && s.left != null; s = s.left)
                stk = (stk==null)?new SList<SSearchTree<T>>(s)
                        : stk.InsertAt(s, 0);                
            } catch(Exception e) 
            {
                System.out.println(e.getMessage());
            } // does not throw exception
            _s = s; _stk = stk; 
        }
        public T getValue()
        {
            return _s.node;
        }
        public Bookmark<T> Next() 
        {
            try{
            return (_s.right != null)?
                new SSearchTreeBookmark<T>(_s.right, true, _stk, Position + 1)
                : (_stk == null) ? null
                : new SSearchTreeBookmark<T>(_stk.First().getValue(), false, _stk.RemoveAt(0), Position + 1);
            } catch(Exception e) 
            {
                return null;
            }
        }    
}
