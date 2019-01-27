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
public class Context implements ILookup<String,Serialisable>
{
    public final ILookup<String,Serialisable> here;
    public final Context next;
    public static Context Empty = new Context();
    Context()
    {
        here = null;
        next = null;
    }
    public Context(ILookup<String,Serialisable> h,Context n)
    {
        here = h;
        next = n;
    }
    @Override
    public boolean defines(String s) {
        return (here!=null) && (here.defines(s) || 
                (next!=null && next.defines(s)));
    }

    @Override
    public Serialisable get(String s) {
        if (here==null)
            return Serialisable.Null;
        var r = here.get(s);
        if (r!=null)
            return r;
        return next.get(s);
    }
    
}
