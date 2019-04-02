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
    public final STransaction tr;
    public final Context next;
    public static Context Empty = new Context();
    private Context()
    {
        refs = null;
        next = null;
        tr = null;
    }
    private Context(ILookup<Long,Serialisable> a, Context n, STransaction t)
    {
        refs = a;
        next = n;
        tr = t;
    }
    public static Context New(ILookup<Long,Serialisable> a, Context n, STransaction t)
    {
        return (a==null)?((n==null)?Empty:n):new Context(a,n,t);
    }
    public static Context Append(Context a,Context b,STransaction tr)
    {
        if (a.refs==null)
            return b;
        if (b.refs==null)
            return a;
        if (a.next == null)
            return new Context(a.refs, b, tr);
        return new Context(a.refs, Append(a.next, b, tr), tr);        
    }
    public SRow Row() throws Exception
    {
        if (refs instanceof SRow)
            return (SRow)refs;
        if (next==null)
            throw new Exception("PE05");
        return next.Row();
    }
    public STransaction Transaction() throws Exception
    {
        if (tr!=null)
            return tr;
        if (next!=null)
            return next.Transaction();
        throw new Exception("PE26");
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
