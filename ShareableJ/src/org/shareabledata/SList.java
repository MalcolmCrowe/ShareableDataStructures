/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

@SuppressWarnings("unchecked")
/**
 *
 * @author Malcolm Crowe shareabledata.org
 */
public class SList<T> {

    public final T element;
    public final SList<T> next;

    public SList(T e, SList<T> n) {
        element = e;
        next = n;
    }

    public SList(T... args) throws Exception {
        if (args.length == 0) {
            throw new Exception("Bad parameter");
        }
        SList<T> n = null;
        for (int i = args.length - 1; i > 0; i--) {
            n = new SList<T>(args[i], n);
        }
        element = args[0];
        next = n;
    }

    public int getLength() {
        return (next != null) ? next.getLength() + 1 : 1;
    }

    public SList<T> InsertAt(T x, int n) {
        if (n == 0) {
            return new SList<T>(x, this);
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
            return new SList<T>(x, next);
        }
        return new SList<T>(element, next.UpdateAt(x, n - 1));
    }

    public T[] ToArray() {
        T[] r;
        r = (T[]) new Object[getLength()];
        int i = 0;
        for (SList<T> x = this; x != null; x = x.next) {
            r[i++] = x.element;
        }
        return r;
    }
}
