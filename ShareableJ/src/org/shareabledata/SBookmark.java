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
public class SBookmark<K extends Comparable,V> {
        public final SBucket<K, V> _bucket;
        public final int _bpos;
        public final SBookmark<K, V> _parent;
        public SBookmark(SBucket<K,V> b,int bp,SBookmark<K,V> n)
        {
            _bucket = b; _bpos = bp; _parent = n;
        }
        public K getKey() { return _bucket.Slot(_bpos).key; } 
        public V getValue() {
            return (V)_bucket.Slot(_bpos).val;
        }
        /// <summary>
        /// The position in the tree
        /// </summary>
        /// <returns>The zero-based position in a traversal</returns>
        public int position()
        {
            int r = (_parent==null)?0:_parent.position();
            for (int i = 0; i < _bpos; i++)
            {
                Object ob = _bucket.Slot(i).val;
                int k = 1;
                if (ob instanceof SBucket)
                    k = ((SBucket<K,V>)ob).total;
                r += k;
            }
            return r;
        }
        /// <summary>
        /// This implements both SDict.First() and SBookmark.Next()
        /// </summary>
        /// <param name="stk"></param>
        /// <param name="tree"></param>
        /// <returns></returns>
        public SBookmark<K,V> Next(SBookmark<K,V> stk,SDict<K,V> tree)
        {
            Object b;
            SSlot<K,Object> d;
             // guaranteed to be at a LEAF
            int stkPos = stk._bpos;
            if (++stkPos == stk._bucket.count) // this is the right test for a leaf
            {
                // at end of current bucket: pop till we aren't
                for (; ; )
                {
                    if (++stkPos <= stk._bucket.count)// this is the right test for a non-leaf; redundantly ok for first time (leaf)
                        break;
                    stk = stk._parent;
                    if (stk == null)
                        break;
                    stkPos = stk._bpos;
                }
                // we may run out of the BTree
                if (stk == null)
                    return null;
            }
            stk = new SBookmark<K, V>(stk._bucket, stkPos, stk._parent);
            if (stk._bpos == stk._bucket.count)
            { // will only happen for a non-leaf
                b = ((SInner<K,V>)(stk._bucket)).gtr;
       //         d = new SSlot<>(null, null); // or compiler complains
            }
            else // might be leaf or not
            {
                d = stk._bucket.Slot(stkPos);
                b = d.val;
            }
            while (b instanceof SBucket) // now ensure we are at a leaf
            {
                stk = new SBookmark<>((SBucket<K,V>)b, 0, stk);
                d = stk._bucket.Slot(0);
                b = d.val;
            }
            return stk;

        } 
        public SBookmark<K, V> Previous(SBookmark<K, V> stk, SDict<K, V> tree)
        {
            SBucket<K, V> b;
            SSlot<K, Object> d;
             // guaranteed to be at a LEAF
            var stkPos = stk._bpos-1;
            if (stkPos < 0)
            {
                while (stkPos < 0)
                {
                    // before start of current bucket: pop till we aren't
                    stk = stk._parent;
                    if (stk == null)
                        return null;
                    stkPos = stk._bpos - 1;
                }
            }
            stk = new SBookmark<K, V>(stk._bucket, stkPos, stk._parent);
            d = stk._bucket.Slot(stkPos);
            b = (SBucket<K,V>)d.val;
            while (b != null) // now ensure we are at a leaf
            {
                stk = new SBookmark<K, V>(b, b.getEndPos(), stk);
                b = ((SBucket<K,V>)d.val).Gtr();
            }
            return stk;
        }
    }

