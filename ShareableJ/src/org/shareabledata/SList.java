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
public class SList<T> extends Shareable<T> 
{
    public final T element;
    public final SList<T> next;
    public SList(T e, SList<T> n) 
    {
        super(((n==null)?0:n.getLength())+1);
        element = e;
        next = n;
    }
    public SList(T... args) throws Exception 
    {
        super(args.length);
        if (args.length == 0) { // empty list is null so this won't do
            throw new Exception("Bad parameter");
        }
        SList<T> n = null;
        for (int i = args.length - 1; i > 0; i--) {
            n = new SList<>(args[i], n);
        }
        element = args[0];
        next = n;
    }
    public int getLength() {
        return (next != null) ? next.getLength() + 1 : 1;
    }

    public SList<T> InsertAt(T x, int n) {
        if (n == 0) {
            return new SList<>(x, this);
        }
        return new SList<T>(element, next.InsertAt(x, n - 1));
    }

    public SList<T> RemoveAt(int n) {
        if (n == 0) {
            return next;
        }
        return new SList<T>(element, next.RemoveAt(n - 1));
    }

    public SList<T> UpdateAt(T x, int n) {
        if (n == 0) {
            return new SList<>(x, next);
        }
        return new SList<>(element, next.UpdateAt(x, n - 1));
    }

    @Override
    public Bookmark<T> First()
    {
        return new SListBookmark<T>(this,0);
    }
}
