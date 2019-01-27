/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 * A shareable list of Comparable
 * @author Malcolm
 * @param <K>
 */
public class SCList<K extends Comparable> extends SList<K> 
        implements Comparable {
    public SCList(K el,SCList<K> nx) 
    {
        super(el,nx);
    }
    public SCList(K...els) throws Exception
    {
        super(els);
    }
    @Override
    public Bookmark<K> First()
    {
        return (Length == 0) ? null : new SCListBookmark<>(this,0);
    }
    @Override
    public int compareTo(Object o) {
       SCList<K> them = (SCList<K>)o;
       SCList<K> me = this;
       for (;me!=null && them!=null;me=(SCList<K>)me.next,them=(SCList<K>)them.next)
       {
           int c = me.element.compareTo(them.element);
           if (c!=0)
               return c;
       }
       return (me!=null)?1:(them!=null)?-1:0;
    }
    @Override
        public SCList<K> InsertAt(K x, int n) {
        if (n>Length)
            throw new Error("Cannot add beyond end of list");
        if (n == 0) 
            return new SCList<>(x, this);
        if (next==null)
            return new SCList<K>(element,new SCList(x,null));
        return new SCList<K>(element, ((SCList<K>)next).InsertAt(x, n - 1));
    }

    @Override
    public SCList<K> RemoveAt(int n) {
        if (n>=Length)
            throw new Error("Cannot remove beyond end of list");
        if (n == 0) 
            return (SCList<K>)next;
        return new SCList<K>(element, ((SCList<K>)next).RemoveAt(n - 1));
    }

    @Override
    public SCList<K> UpdateAt(K x, int n) throws Exception {
        if (n>=Length)
            throw new Exception("Cannot update beyond end of list");
        if (n == 0)
            return new SCList<K>(x, (SCList<K>)next);
        return new SCList<K>(element, ((SCList<K>)next).UpdateAt(x, n - 1));
    }
}
