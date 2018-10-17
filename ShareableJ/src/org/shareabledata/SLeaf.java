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

    public SLeaf(SSlot<K, V>[] s) {
        super(s.length, s.length);
        slots = s;
    }

    public SLeaf(SSlot<K, V>[] s, int low, int high) {
        super(high + 1 - low, high + 1 - low);
        slots = (SSlot<K, V>[]) new Object[count];
        for (int i = 0; i < count; i++) {
            slots[i] = s[i + low];
        }
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
        return new SLeaf<K, V>(slots, SDict.Size >> 1, SDict.Size - 1);
    }

    @Override
    SSlot<K, SBucket<K, V>> LowHalf() {
        int m = SDict.Size >> 1;
        return new SSlot<K, SBucket<K, V>>(slots[m - 1].key, new SLeaf<K, V>(slots, 0, m - 1));
    }

    @Override
    SSlot<K, Object> Slot(int i) {
        return new SSlot<K, Object>(slots[i].key, slots[i].val);
    }

    @Override
    void Add(ArrayList ab) {
        for (int i=0;i<count;i++)
            ab.add(slots[i]);
    }
        SSlot<K,V>[] Splice(int ix,SSlot<K,V> ns,SSlot<K,V> os)
        {
            SSlot<K,V>[] s = (SSlot<K,V>[])new Object[count + 1];
            int j = 0, k = 0;
            while (j < ix)
                s[k++] = slots[j++];
            s[k++] = ns;
            s[k++] = os;
            j++;
            while (j < count)
                s[k++] = slots[j++];
            return s;
        }
        SSlot<K,V>[] Replace(int j,SSlot<K,V> d)
        {
            SSlot<K,V>[] s = (SSlot<K,V>[])new Object[count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }
        SSlot<K,V>[] Add(int ix, SSlot<K,V> s)
        {
            SSlot<K,V>[] t = (SSlot<K,V>[])new Object[count + 1];
            int j = 0, k = 0;
            while (j < ix)
                t[k++] = slots[j++];
            t[k++] = s;
            while (j < count)
                t[k++] = slots[j++];
            return t;
        }
        SSlot<K,V>[] Remove(int ix)
        {
            SSlot<K,V>[] s = (SSlot<K,V>[])new Object[count - 1];
            int j = 0;
            while (j < ix && j < count - 1)
            {
                s[j] = slots[j];
                j++;
            }
            while (j < count - 1)
            {
                s[j] = slots[j + 1];
                j++;
            }
            return s;
        }

}
