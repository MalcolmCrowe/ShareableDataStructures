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
public class SDict<K extends Comparable,V> extends Shareable<SSlot<K,V>> {
    public static final int Size = 8;
    public final SBucket<K,V> root;
    SDict(SBucket<K,V> r) 
    { 
        super((r==null)?0:r.total); 
        root = r; 
    }
    @Override
    public Bookmark<SSlot<K, V>> First() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
