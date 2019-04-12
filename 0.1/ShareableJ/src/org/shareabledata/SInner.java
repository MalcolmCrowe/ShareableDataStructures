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
public class SInner<K extends Comparable,V> extends SBucket<K,V> {
        public final SSlot<K, SBucket<K, V>>[] slots;
        public final SBucket<K, V> gtr;
        public SInner(SBucket<K,V> v, int t,SSlot<K,SBucket<K,V>>... s)
        { 
            super(s.length,t);
            slots = s; gtr = v;
        }
        public SInner(SBucket<K,V> v,int t, SSlot<K,SBucket<K,V>>[] s, int low, int high)
        {
            super(high+1-low,t);
            var a = new ArrayList<SSlot<K,SBucket<K,V>>>();
            for (int i = 0; i < count; i++)
                a.add(s[i + low]);
            slots = a.toArray(new SSlot[0]);
            gtr = v;
        }
        public SInner(SBucket<K,V> v,int t,SSlot<K,SBucket<K,V>>[] s1,int low1, int high1,
            SSlot<K, SBucket<K, V>>[] s2, int low2, int high2) 
        {
            super(high1+high2+2-low1-low2,t);
            var a = new ArrayList<SSlot<K,SBucket<K,V>>>();
            int j;
            for (j = low1; j <= high1; j++)
                a.add(s1[j]);
            for (j = low2; j <= high2; j++)
                a.add(s2[j]);
            slots = a.toArray(new SSlot[0]);
            gtr = v;
        }
        @Override
        public boolean Contains(K k)
        {
            MatchPos m = PositionFor(k);
            if (m.pos == count)
                return gtr.Contains(k);
            return slots[m.pos].val.Contains(k);
        }
        @Override
        public V Lookup(K k)
        {
            MatchPos m = PositionFor(k);
            if (m.pos == count)
                return gtr.Lookup(k);
            return slots[m.pos].val.Lookup(k);
        }
        @Override
        public MatchPos PositionFor(K k)
        {
            // binary search
            int low = 0, high = count, mid;
            while (low < high)
            {
                mid = (low + high) >> 1;
                K midk = slots[mid].key;
                int c = k.compareTo(midk);
                if (c == 0)
                    return new MatchPos(mid,true);
                if (c > 0)
                    low = mid + 1;
                else
                    high = mid;
            }
            return new MatchPos(high,false);
        }
        @Override
        void Add(ArrayList ab)
        {
            for (int i=0;i<count;i++)
                ab.add(slots[i]);
        }
        @Override
        SBucket<K, V> Update(K k, V v)
        {
            MatchPos m = PositionFor(k);
            if (m.pos == count)
                return new SInner<K, V>(gtr.Update(k, v), total, slots);
            else
            {
                SSlot<K,SBucket<K,V>> d = slots[m.pos];
                SBucket<K,V> b = d.val.Update(k, v);
                return new SInner<K, V>(gtr, total, Replace(m.pos, new SSlot<K, SBucket<K, V>>(d.key, b)));
            }
        }
        @Override
        SBucket<K, V> Add(K k, V v)
        {
            // by the time we get here we have made sure there is at least one empty Slot
            // in the current bucket
            MatchPos m = PositionFor(k); // (j<count && k<=slots[j]) || j==count
            SBucket<K, V> b;
            if (m.pos < count)
            {
                SSlot<K,SBucket<K,V>> d = slots[m.pos];
                b = d.val;
                if (b.count == SDict.SIZE)
                    return Split(m.pos).Add(k, v); // try again
                return new SInner<K, V>(gtr, total + 1, Replace(m.pos, new SSlot<K, SBucket<K, V>>(d.key, b.Add(k, v))));
            }
            else
            {
                if (gtr.count == SDict.SIZE)
                    return SplitGtr().Add(k, v); // try again
                return new SInner<K, V>(gtr.Add(k, v), total + 1, slots);
            }
        }
        SInner<K, V> SplitGtr()
        {
            return new SInner<K, V>(gtr.TopHalf(), total, slots, 0, count - 1, new SSlot[] { gtr.LowHalf() }, 0, 0);
        }
        SSlot<K,SBucket<K,V>> LowHalf()
        {
            int m = SDict.SIZE >> 1;
            int h = 0;
            for (int i = 0; i < m; i++)
                h += slots[i].val.total;
            return new SSlot<K, SBucket<K, V>>(slots[m - 1].key, new SInner<K, V>(slots[m - 1].val, h, slots, 0, m - 2));
        }

        SBucket<K, V> Remove(K k)
        {
            MatchPos nj = PositionFor(k);
            SBucket<K, V> nb;
            int m = SDict.SIZE >> 1;
            if (nj.pos < count)
            {
                SSlot<K,SBucket<K,V>> e = slots[nj.pos];
                nb = e.val;
                nb = nb.Remove(k);
                if (nb.count >= m)
                    return new SInner<K, V>(gtr, total - 1, 
                            Replace(nj.pos, new SSlot<K, SBucket<K, V>>(nb.Last(), nb)));
            }
            else
            {
                nb = gtr.Remove(k);
                if (nb.count >= m)
                    return new SInner<K, V>(nb, total - 1, slots);
            }
            // completely rebuild the current non-leaf node (too many cases to consider otherwise)
            // still two different cases depending on whether children are leaves
            int S = SDict.SIZE;
            SBucket<K, V> b, g = null;
            ArrayList ab = new ArrayList();
            int i, j;
            for (j = 0; j < count; j++)
            {
                b = (j == nj.pos) ? nb : slots[j].val;
                b.Add(ab);
                if (b instanceof SInner)
                    ab.add(new SSlot<K, SBucket<K, V>>(slots[j].key, ((SInner<K, V>)b).gtr));
            }
            b = (count == nj.pos) ? nb : gtr;
            b.Add(ab);
            if (b instanceof SInner)
                g = ((SInner<K, V>)b).gtr;
            var s = (SSlot<K,V>[])ab.toArray(new SSlot[ab.size()]);
            if (g == null) // we use Size entries from s for each new Bucket (all Leaves)
            {
                var a = new ArrayList<SSlot<K, V>>();
                for (j = 0; j < s.length; j++)
                    a.add(s[j]);
                var ss = a.toArray(new SSlot[0]);
                if (s.length <= S) // can happen at root: reduce height of tree
                    return new SLeaf<K, V>(ss);
                // suppose s.Length = Size*A+B
                int A = s.length / S;
                int B = s.length - A * S;
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                var ts = new ArrayList<SSlot<K, SBucket<K, V>>>(); // new list of children
                int sce = 0;
                SSlot<K, V> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                int D = (B == 0) ? (A - 1) : A;
                for (i = 0; i < D - C; i++)
                {
                    d = ss[sce + S - 1]; // last entry in new bucket
                    ts.add(new SSlot<K, SBucket<K, V>>(d.key, new SLeaf<K, V>(ss, sce, sce + S - 1)));
                    sce += S;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    m = S >> 1;
                    d = ss[sce + m - 1];
                    ts.add(new SSlot<K, SBucket<K, V>>(d.key, new SLeaf<K, V>(ss, sce, sce + m - 1)));
                    sce += m;
                }
                return new SInner<K, V>(new SLeaf<K, V>(ss, sce, s.length - 1), total - 1, ts.toArray(new SSlot[0]));
            }
            else // we use Size+1 entries from s for each new Bucket: g is an extra one
            {
                var a = new ArrayList<SSlot<K, SBucket<K, V>>>();
                for (j = 0; j < s.length; j++)
                    a.add((SSlot<K, SBucket<K, V>>)s[j]);
                var ss = a.toArray(new SSlot[0]);
                if (s.length <= S) // can happen at root: reduce height of tree
                    return new SInner<K, V>(g, total - 1, ss);
                int A = (s.length + 1) / (S + 1); // not forgetting g
                int B = s.length + 1 - A * (S + 1);
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                var ts = new ArrayList<SSlot<K, SBucket<K, V>>>(); // new list of children
                int sce = 0;
                SSlot<K, SBucket<K, V>> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                int D = (B == 0) ? (A - 1) : A;
                for (i = 0; i < D - C; i++)
                {
                    d = ss[sce + S]; // last entry in new bucket
                    int dt = 0;
                    for (int di = sce; di < sce + S; di++)
                        dt += ((SBucket)ss[di].val).total;
                    ts.add(new SSlot<K, SBucket<K, V>>(d.key, new SInner<K, V>(d.val, dt, ss, sce, sce + S - 1)));
                    sce += S + 1;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    d = ss[sce + m];
                    int dt = 0;
                    for (int di = sce; di < sce + m; di++)
                        dt += ((SBucket)ss[di].val).total;
                    ts.add(new SSlot<K, SBucket<K, V>>(d.key, new SInner<K, V>(d.val, dt, ss, sce, sce + m - 1)));
                    sce += m + 1;
                }
                int gt = 0;
                for (int di = sce; di < s.length; di++)
                    gt += ((SBucket)ss[di].val).total;
                return new SInner<K, V>(new SInner<K, V>(g, gt, ss, sce, s.length - 1), total - 1, ts.toArray(new SSlot[0]));
            }

        }
        protected SSlot<K, SBucket<K, V>>[] Replace(int j, SSlot<K, SBucket<K, V>> d)
        {
            SSlot<K, SBucket<K, V>>[] s = (SSlot<K, SBucket<K, V>>[])new SSlot [count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }

        SSlot<K,Object> Slot(int i)
        {
            return new SSlot<K,Object>(slots[i].key,slots[i].val);
        }

        SBucket<K, V> TopHalf()
        {
            int m = SDict.SIZE >> 1;
            int h = total;
            for (int i = 0; i < m; i++)
                h -= slots[i].val.total;
            return new SInner<K, V>(gtr, h, slots, m, SDict.SIZE - 1);
        }
        SBucket<K,V> Split(int j)
        {
            SSlot<K,SBucket<K,V>> d = slots[j];
            SBucket<K,V> b = d.val;
            return new SInner<K, V>(gtr, total, Splice(j, b.LowHalf(), new SSlot<K, SBucket<K, V>>(d.key, b.TopHalf())));
        }
        SSlot<K, SBucket<K, V>>[] Splice(int ix, SSlot<K, SBucket<K, V>> ns, SSlot<K, SBucket<K, V>> os) // insert ns at ppos ix, replace next by os
        {
            SSlot<K, SBucket<K, V>>[] s = (SSlot<K, SBucket<K, V>>[])new SSlot[count + 1];
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
        @Override
        public K Last()
        {
            return gtr.Last();
        }
        @Override
        public SBucket<K,V> Gtr()
        {
            return gtr;
        }
        @Override
        public int getEndPos() {return count;}
}
