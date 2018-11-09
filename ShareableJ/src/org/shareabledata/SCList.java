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
}
