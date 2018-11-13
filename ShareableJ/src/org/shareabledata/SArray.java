/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.*;
/**
 *
 * @author 66668214
 */
public class SArray<T> extends Shareable<T> {
        public final T[] elements;
        public SArray(T ... els)
        { 
            super(els.length);
            var e = new ArrayList<T>();
            for (int i = 0; i <els.length; i++)
                e.add(els[i]);
            elements = (T[])e.toArray();
        }
        public SArray<T> InsertAt(int n, T ... els)
        {
            T[] x = (T[])new Object[elements.length + els.length];
            for (int i = 0; i < n; i++)
                x[i] = elements[i];
            for (int i = 0; i < els.length; i++)
                x[i + n] = els[i];
            for (int i = n; i < elements.length; i++)
                x[i + els.length] = elements[i];
            return new SArray<T>(x);
        }
        public SArray<T> RemoveAt(int n)
        {
            T[] x = (T[]) new Object[elements.length - 1];
            for (int i = 0; i < n; i++)
                x[i] = elements[i];
            for (int i = n+1; i < elements.length; i++)
                x[i - 1] = elements[i];
            return new SArray<T>(x);
        }
        public SArray<T> UpdateAt(T x, int n)
        {
            T[] a = (T[]) new Object[elements.length - 1];
            for (int i = 0; i < n; i++)
               a[i] = elements[i];
            a[n] = x;
            for (int i = n+1; i < elements.length; i++)
                a[i] = elements[i];
            return new SArray<T>(a);
        }
        public Bookmark<T> First()
        {
            return (Length==0)? null 
                    : new SArrayBookmark<T>(this,0);
        }
}
