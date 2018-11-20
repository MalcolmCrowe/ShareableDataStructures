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
public class SysItem {

    public final Serialisable item;
    public final long next;

    SysItem(Serialisable i, long n) {
        item = i;
        next = n;
    }
}
