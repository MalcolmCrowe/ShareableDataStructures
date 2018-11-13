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
public class SDict<K extends Comparable, V> extends Shareable<SSlot<K, V>> {

    public static final int SIZE = 8;
    public final SBucket<K, V> root;

    public SDict(K k, V v) {
        this(new SLeaf<K, V>(new SSlot<K, V>(k, v)));
    }

    SDict(SBucket<K, V> r) {
        super((r == null) ? 0 : r.total);
        root = r;
    }

    public int Count() {
        return (root == null) ? 0 : root.total;
    }

    @Override
    public Bookmark<SSlot<K, V>> First() {
        return (root == null || root.total == 0) ? null
                : new SDictBookmark<K, V>(new SBookmark<K, V>(root, 0, null));
    }

    public SDict<K, V> Add(K k, V v) {
        return (root == null || root.total == 0) ? new SDict<>(k, v)
                : (root.Contains(k)) ? new SDict<>(root.Update(k, v))
                : (root.count == SIZE) ? new SDict<>(root.Split()).Add(k, v)
                        : new SDict<>(root.Add(k, v));
    }

    public SDict<K, V> Remove(K k) {
        return (root == null || root.Lookup(k) == null) ? this
                : (root.total == 1) ? new SDict<>(null)
                        : new SDict<>(root.Remove(k));
    }

    public boolean Contains(K k) {
        return (root == null) ? false : root.Contains(k);
    }

    public V Lookup(K k) {
        return (root == null) ? null : root.Lookup(k);
    }

    public SDict<K, V> Merge(SDict<K, V> ud) {
        SDict<K, V> r = null;
        var ob = First();
        var ub = ud.First();
        while (ob != null && ub != null) {
            var ok = ob.getValue().key;
            var uk = ub.getValue().key;
            var c = ok.compareTo(uk);
            if (c == 0) {
                var uv = ub.getValue().val;
                r = (r == null) ? new SDict<K, V>(uk, uv) : r.Add(uk, uv);
                ob = ob.Next();
                ub = ub.Next();
            } else if (c < 0) {
                var ov = ob.getValue().val;
                r = (r == null) ? new SDict(ok, ov) : r.Add(ok, ov);
                ob = ob.Next();
            } else {
                var uv = ub.getValue().val;
                r = (r == null) ? new SDict<K, V>(uk, uv) : r.Add(uk, uv);
                ub = ub.Next();
            }
        }
        for (; ob != null; ob = ob.Next()) {
            var ok = ob.getValue().key;
            var ov = ob.getValue().val;
            r = (r == null) ? new SDict(ok, ov) : r.Add(ok, ov);
        }
        for (; ub != null; ub = ub.Next()) {
            var uk = ub.getValue().key;
            var uv = ub.getValue().val;
            r = (r == null) ? new SDict<K, V>(uk, uv) : r.Add(uk, uv);
        }
        return r;
    }
}

