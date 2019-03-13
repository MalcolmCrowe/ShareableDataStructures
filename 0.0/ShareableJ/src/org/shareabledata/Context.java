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
    public final ILookup<String,Serialisable> head;
    public final ILookup<Long,Serialisable> ags;
    public final Context next;
    public static Context Empty = new Context();
    Context()
    {
        head = null;
        ags = null;
        next = null;
    }
    public Context(ILookup<String,Serialisable> h,
            ILookup<Long,Serialisable> a, Context n)
    {
        head = h;
        ags = a;
        next = n;
    }
    public Context(RowBookmark b,Context c)
    {
        head = b;
        ags = b._ags;
        next = c;
    }
    public Context(ILookup<String,Serialisable> h,Context n)
    {
        head = h;
        ags = null;
        next = n;
    }
    public Context(Context n,ILookup<Long,Serialisable> a)
    {
        head = null;
        ags = a;
        next = n;
    }    
    public boolean defines(String s) {
        if (head==null && ags == null && next==null)
            return false;
        return (head!=null && head.defines(s)) || 
                (next!=null && next.defines(s));
    }
    public Serialisable get(String s) {
        if (head==null && ags==null && next==null)
            return Serialisable.Null;
        if (head!=null)
        {
            var r = head.get(s);
            if (r!=null)
                return r;
        }
        return next.get(s);
    }
    public boolean defines(long s) {
        if (head==null && ags == null && next==null)
            return false;
        return (ags!=null && ags.defines(s)) || 
                (next!=null && next.defines(s));
    }
    public Serialisable get(long s) {
        if (head==null && ags==null && next==null)
            return Serialisable.Null;
        if (ags!=null)
        {
            var r = ags.get(s);
            if (r!=null)
                return r;
        }
        return next.get(s);
    }    
}
