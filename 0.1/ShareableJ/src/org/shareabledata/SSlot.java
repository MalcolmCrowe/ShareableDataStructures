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
public class SSlot<K,V> {
    public final K key;
    public final V val;
    public SSlot(K k,V v) { key = k; val = v; }
}
