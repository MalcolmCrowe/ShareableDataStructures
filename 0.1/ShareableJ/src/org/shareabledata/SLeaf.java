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
public class SLeaf<K extends Comparable, V> extends SBucket<K, V> {

    public final SSlot<K, V>[] slots;

    public SLeaf(SSlot<K, V>... s) {
        super(s.length, s.length);
        slots = s;
    }

    public SLeaf(SSlot<K, V>[] s, int low, int high) {
        super(high + 1 - low, high + 1 - low);
        var a = new ArrayList<SSlot<K, V>>();
        for (int i = 0; i < count; i++) {
            a.add(s[i + low]);
        }
        slots = a.toArray(new SSlot[0]);
    }

    @Override
    public boolean Contains(K k) {
        MatchPos m = PositionFor(k);
        return m.match;
    }

    @Override
    public V Lookup(K k) {
        MatchPos m = PositionFor(k);
        return m.match ? slots[m.pos].val : null;
    }

    @Override
    SBucket<K, V> Add(K k, V v) {
        return new SLeaf<K, V>(Add(PositionFor(k).pos, new SSlot<K, V>(k, v)));
    }

    @Override
    SBucket<K, V> Update(K k, V v) {
        return new SLeaf<K, V>(Replace(PositionFor(k).pos, new SSlot<K, V>(k, v)));
    }

    @Override
    SBucket<K, V> Remove(K k) {
        return new SLeaf<K, V>(Remove(PositionFor(k).pos));
    }

    @Override
    public MatchPos PositionFor(K k) {
        // binary search
        int low = 0, high = count, mid;
        while (low < high) {
            mid = (low + high) >> 1;
            K midk = slots[mid].key;
            int c = k.compareTo(midk);
            if (c == 0) {
                return new MatchPos(mid, true);
            }
            if (c > 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return new MatchPos(high, false);
    }

    @Override
    SBucket<K, V> TopHalf() {
        return new SLeaf<K, V>(slots, SDict.SIZE >> 1, SDict.SIZE - 1);
    }

    @Override
    SSlot<K, SBucket<K, V>> LowHalf() {
        int m = SDict.SIZE >> 1;
        return new SSlot<K, SBucket<K, V>>(slots[m - 1].key, new SLeaf<K, V>(slots, 0, m - 1));
    }

    @Override
    SSlot<K, Object> Slot(int i) {
        return new SSlot<K, Object>(slots[i].key, slots[i].val);
    }

    @Override
    void Add(ArrayList ab) {
        for (int i = 0; i < count; i++) {
            ab.add(slots[i]);
        }
    }

    SSlot<K, V>[] Splice(int ix, SSlot<K, V> ns, SSlot<K, V> os) {
        var s = new ArrayList<SSlot<K,V>>();
        int j = 0;
        while (j < ix) {
            s.add(slots[j++]);
        }
        s.add(ns);
        s.add(os);
        j++;
        while (j < count) {
            s.add(slots[j++]);
        }
        return s.toArray(new SSlot[0]);
    }

    SSlot<K, V>[] Replace(int j, SSlot<K, V> d) {
        var s = new ArrayList<SSlot<K,V>>();
        int i = 0;
        while (i < count) {
            s.add((i==j)?d:slots[i]);
            i++;
        }
        return s.toArray(new SSlot[0]);
    }

    SSlot<K, V>[] Add(int ix, SSlot<K, V> s) {
        var t = new ArrayList<SSlot<K,V>>();
        int j = 0;
        while (j < ix) {
            t.add(slots[j++]);
        }
        t.add(s);
        while (j < count) {
            t.add(slots[j++]);
        }
        return t.toArray(new SSlot[0]);
    }

    SSlot<K, V>[] Remove(int ix) {
        var s = new ArrayList<SSlot<K,V>>();
        int j = 0;
        while (j < ix && j < count - 1)
            s.add(slots[j++]);
        while (j < count - 1)
            s.add(slots[j++ + 1]);
        return s.toArray(new SSlot[0]);
    }

}
