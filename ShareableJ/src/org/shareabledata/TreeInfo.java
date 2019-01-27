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
public class TreeInfo<K extends Comparable> {
    public final K headName;
    public final TreeBehaviour onDuplicate, onNullKey;
    public final boolean asc;
    TreeBehaviour For(char c) {
        switch (c) {
            case 'I':
                return TreeBehaviour.Ignore;
            case 'A':
                return TreeBehaviour.Allow;
            default:
            case 'D':
                return TreeBehaviour.Disallow;
        }
    }
    public TreeInfo(K h, char d, char n, boolean a) {
        headName = h;
        onDuplicate = For(d);
        onNullKey = For(n);
        asc = a;
    }
}
