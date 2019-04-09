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
public class Context
{
    public final ILookup<Long,Serialisable> refs;
    public final Context next;
    public static Context Empty = new Context();
    private Context()
    {
        refs = null;
        next = null;
    }
    private Context(ILookup<Long,Serialisable> a, Context n)
    {
        refs = a;
        next = n;
    }
    public static Context New(ILookup<Long,Serialisable> a, Context n)
    {
        if (a==null)
            return n;
        return new Context(a,n);
    }
    public static Context Replace(ILookup<Long,Serialisable> a, Context n)
    {
        if (a==null)
            return n;
        return new Context(a,(n==null)?null:n.next);
    }
    public static Context Append(Context a,Context b)
    {
        if (a.refs==null)
            return b;
        if (b.refs==null)
            return a;
        if (a.next == null)
            return new Context(a.refs, b);
        return new Context(a.refs, Append(a.next, b));        
    }
    public SRow Row() throws Exception
    {
        if (refs instanceof SRow)
            return (SRow)refs;
        if (next==null)
            throw new Exception("PE05");
        return next.Row();
    }
    public SDict<Long,Serialisable> Ags() throws Exception
    {
        if (refs instanceof SDict)
        {
            var r = (SDict)refs;
            var f = r.First();
            if (f == null || ((Serialisable)f.getValue().val).type!=Types.SRow)
                return r;
        }
        if (next==null)
             throw new Exception("PE25");
        return next.Ags();
    }
    public boolean defines(Long u) {
        if (refs==null)
            return false;
        return (refs.defines(u)) || 
                (next!=null && next.defines(u));
    }
    public Serialisable get(Long u) {
        if (refs==null)
            return Serialisable.Null;
        if (refs.defines(u))
            return refs.get(u);
        if (next!=null)
            return next.get(u);
        return Serialisable.Null;
    }    
}
