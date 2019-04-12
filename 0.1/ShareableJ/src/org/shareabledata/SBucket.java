/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.ArrayList;
/**
 *
 * @author Malcolm
 */
public abstract class SBucket<K extends Comparable,V> {
        public final byte count;
        public final int total;
        protected SBucket(int c,int tot) { count = (byte)c; total = tot; }
        // API for SDict to call
        public abstract boolean Contains(K k);
        public abstract V Lookup(K k);
        abstract SBucket<K, V> Add(K k, V v);
        abstract SBucket<K, V> Update(K k, V v);
        abstract SBucket<K, V> Remove(K k);
        public abstract MatchPos PositionFor(K k);
        // API for internal housekeeping
        SBucket<K,V> Split() { return new SInner<K, V>(TopHalf(), total, LowHalf()); }
        abstract SBucket<K, V> TopHalf();
        abstract SSlot<K,SBucket<K,V>> LowHalf();
        abstract SSlot<K, Object> Slot(int i);
        abstract void Add(ArrayList ab);
        public abstract K Last();
        public SBucket<K, V> Gtr() { return null; }
        public int getEndPos() { return count - 1; }
}
