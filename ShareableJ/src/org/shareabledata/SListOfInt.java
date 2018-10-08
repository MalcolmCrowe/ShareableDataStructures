/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm Crowe shareabledata.org 2018
 */
public class SListOfInt {

    public final int element;
    public final SListOfInt next;

    public SListOfInt(int e, SListOfInt n) {
        element = e;
        next = n;
    }

    public SListOfInt InsertAt(int x, int n) {
        if (n == 0) {
            return new SListOfInt(x, this);
        }
        return new SListOfInt(element, next.InsertAt(x, n - 1));
    }

    public SListOfInt RemoveAt(int n) {
        if (n == 0) {
            return next;
        }
        return new SListOfInt(element, next.RemoveAt(n - 1));
    }

}
