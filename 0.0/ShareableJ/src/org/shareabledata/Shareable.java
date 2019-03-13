/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.lang.reflect.Array;

/**
 *
 * @author 66668214
 */
public abstract class Shareable<T> {

    public static boolean HasLength = true;
    public final int Length;

    protected Shareable(int c) {
        Length = c;
    }

    public abstract Bookmark<T> First();

    public T[] ToArray(Class<T> c) {
        T[] r = null;
        r = (T[]) Array.newInstance(c, Length);
        for (Bookmark<T> b = First(); b != null; b = b.Next()) {
            r[b.Position] = b.getValue();
        }
        return r;
    }
}
