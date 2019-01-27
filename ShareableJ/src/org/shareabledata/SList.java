/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

@SuppressWarnings("unchecked")
/**
 * An empty list is null.
 * @author Malcolm Crowe shareabledata.org
 */
public class SList<T> extends Collection<T> 
{
    public final T element;
    public final SList<T> next;
    public SList(T e, SList<T> n) 
    {
        super(((n==null)?0:n.Length)+1);
        element = e;
        next = n;
    }
    public SList(T... args)
    {
        super(args.length);
        if (args.length == 0) { // empty list is null so this won't do
            throw new Error("Bad parameter");
        }
        SList<T> n = null;
        for (int i = args.length - 1; i > 0; i--) {
            n = new SList<>(args[i], n);
        }
        element = args[0];
        next = n;
    }

    public SList<T> InsertAt(T x, int n) {
        if (n>Length)
            throw new Error("Cannot add beyond end of list");
        if (n == 0) 
            return new SList<>(x, this);
        if (next==null)
            return new SList<T>(element,new SList(x,null));
        return new SList<T>(element, next.InsertAt(x, n - 1));
    }

    public SList<T> RemoveAt(int n) throws Exception {
        if (n>=Length)
            throw new Exception("Cannot remove beyond end of list");
        if (n == 0) 
            return next;
        return new SList<T>(element, next.RemoveAt(n - 1));
    }

    public SList<T> UpdateAt(T x, int n) throws Exception {
        if (n>=Length)
            throw new Exception("Cannot update beyond end of list");
        if (n == 0)
            return new SList<>(x, next);
        return new SList<>(element, next.UpdateAt(x, n - 1));
    }

    @Override
    public Bookmark<T> First()
    {
        return new SListBookmark<T>(this,0);
    }
}
