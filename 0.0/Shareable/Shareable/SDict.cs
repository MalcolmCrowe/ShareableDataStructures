﻿using System;
using System.Collections;
using System.Text;
#nullable enable

namespace Shareable
{
    /// <summary>
    /// B-star Tree implementation.
    /// All trees contain ValueTuples (shareable key-value pairs) in key order.
    /// Each node in the tree is an SBucket containing at most Size ValueTuples.
    /// The algorithm ensures that all branches of the tree have the same length.
    /// All nodes except the root have at least Size/2 slots
    /// There are two kinds of SBucket: SLeaf and SInner. 
    /// Efforts towards strong typing of this data structure complicate the coding a bit.
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    public class SDict<K,V> : Collection<ValueTuple<K,V>>,ILookup<K,V> where K: IComparable
    {
        /// <summary>
        /// Size is a system configuration parameter: the maximum number of entries in a Bucket.
        /// It should be an even number, preferably a power of two.
        /// It is const, since experimentally there is little impact on performance 
        /// using values in the range 8 to 32.
        /// </summary>
        public const int Size = 8;
        public readonly SBucket<K, V>? root;
        protected SDict(SBucket<K,V>? r):base(r?.total??0) { root = r; }
        public SDict(K k, V v) : this(new ValueTuple<K, V>(k, v)) { }
        public SDict(params ValueTuple<K, V>[] pairs):base(pairs.Length)
        {
            SBucket<K, V>? r = null;
            foreach (ValueTuple<K,V> p in pairs)
                r = (r==null)?new SLeaf<K,V>(p): r+p;
            root = r;
        }
        public static SDict<K,V> operator+(SDict<K,V> d,ValueTuple<K,V> x)
        {
            return d.Add(x.Item1, x.Item2);
        }
        public static SDict<K, V> operator-(SDict<K, V> d, K k)
        {
            return d.Remove(k);
        }
        /// <summary>
        /// Avoid unnecessary constructor calls by using this constant empty tree
        /// </summary>
        public readonly static SDict<K, V> Empty = new SDict<K, V>(new ValueTuple<K,V>[0]);

        public V this[K s] => Lookup(s);

        /// <summary>
        /// Add a new entry or update an existing one in the tree
        /// </summary>
        /// <param name="k">the key</param>
        /// <param name="v">the value to add</param>
        /// <returns>the modified tree</returns>
        protected virtual SDict<K,V> Add(K k,V v)
        {
            return (root == null || root.total == 0)? new SDict<K, V>(k, v) :
                (root.Contains(k))? new SDict<K,V>(root.Update(k,v)) :
                (root.count == Size)? new SDict<K, V>(root.Split())+(k, v) :
                new SDict<K, V>(root+(k, v));
        }
        /// <summary>
        /// Remove an entry from the tree (Note: we won't have duplicate keys)
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        protected virtual SDict<K,V> Remove(K k)
        {
            return (root==null || root.Lookup(k)==null) ? this :
                (root.total == 1) ? Empty :
                new SDict<K, V>(root.Remove(k));
        }
        public bool Contains(K k)
        {
            return (root == null) ? false : root.Contains(k);
        }
        public V Lookup(K k)
        {
            if (root==null)
                throw new Exception("empty");
            return root.Lookup(k);
        }
        /// <summary>
        /// Start a traversal of the tree
        /// </summary>
        /// <returns>A bookmark for the first entry or null</returns>
        public override Bookmark<ValueTuple<K, V>>? First()
        {
            return (SBookmark<K, V>.Next(null, this) is SBookmark<K,V> b)?
                new SDictBookmark<K, V>(b):null;
        }
        public virtual Bookmark<ValueTuple<K,V>>? PositionAt(K k)
        {
            SBookmark<K, V>? bmk = null;
            var cb = root;
            while (cb != null)
            {
                var bpos = cb.PositionFor(k, out bool b);
                bmk = new SBookmark<K, V>(cb, bpos, bmk);
                if (bpos == cb.count)
                {
                    var inr = cb as SInner<K, V>;
                    if (inr == null)
                        return null;
                    cb = inr.gtr;
                }
                else
                    cb = (cb.Slot(bpos).Item2 ?? throw new Exception("??")) as SBucket<K,V>;
            }
            return (bmk==null)?null:new SDictBookmark<K,V>(bmk);
        }
        public SDict<K,V> Merge(SDict<K,V>ud)
        {
            var r = Empty;
            var ob = First();
            var ub = ud.First();
            while (ob != null && ub != null)
            {
                var c = ob.Value.Item1.CompareTo(ub.Value.Item1);
                if (c == 0)
                {
                    r += (ub.Value.Item1, ub.Value.Item2);
                    ob = ob.Next();
                    ub = ub.Next();
                } else if (c < 0)
                {
                    r += ob.Value;
                    ob = ob.Next();
                } else
                {
                    r += ub.Value;
                    ub = ub.Next();
                }
            }
            for (; ob != null; ob = ob.Next())
                r += ob.Value;
            for (; ub != null; ub = ub.Next())
                r += ub.Value;
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var cm = "";
            for (var b=First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append('('); sb.Append(b.Value.Item1);
                sb.Append('='); sb.Append(b.Value.Item2); sb.Append(')');
            }
            sb.Append(']');
            return sb.ToString();
        }

        public bool defines(K s)
        {
            return Contains(s);
        }
    }
    public class SDictBookmark<K, V> : Bookmark<ValueTuple<K, V>> where K : IComparable
    {
        public readonly SBookmark<K, V> _bmk;
        public override ValueTuple<K, V> Value => ((SLeaf<K,V>)_bmk._bucket).slots[_bmk._bpos]; // ok
        internal SDictBookmark(SBookmark<K,V> bmk) :base(bmk.position()) { _bmk = bmk; }
        public K key => Value.Item1;
        public V val => Value.Item2;
        public override Bookmark<ValueTuple<K, V>>? Next() // ok
        {
            var b = _bmk.Next();
            return (b==null)? null:(Bookmark<ValueTuple<K,V>>)new SDictBookmark<K,V>(b);
        }
    }
    /// <summary>
    /// C# version 7 is picky about generic matching so we need this (7.1 should be easier)
    /// </summary>
    interface IBucket
    {
        byte Count();
        int Total();
    }
    /// <summary>
    /// This is a base class for the two sorts of SBucket.
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V">the value type</typeparam>
    public abstract class SBucket<K,V> : IBucket where K:IComparable
    {
        public readonly byte count;
        public readonly int total;
        protected SBucket(int c,int tot) { count = (byte)c; total = tot; }
        // API for SDict to call
        public abstract bool Contains(K k);
        public abstract V Lookup(K k);
        internal abstract SBucket<K, V> Add(K k, V v);
        internal abstract SBucket<K, V> Update(K k, V v);
        internal abstract SBucket<K, V> Remove(K k);
        public abstract int PositionFor(K k, out bool match);
        // API for internal housekeeping
        internal SBucket<K,V> Split() { return new SInner<K, V>(TopHalf(), total, LowHalf()); }
        internal abstract SBucket<K, V> TopHalf();
        internal abstract ValueTuple<K, SBucket<K, V>> LowHalf();
        internal abstract ValueTuple<K, object?> Slot(int i);
        public static SBucket<K,V> operator+(SBucket<K,V>b,ValueTuple<K,V> x)
        {
            return b.Add(x.Item1, x.Item2);
        }
        internal virtual void Add(ArrayList ab)
        {
            throw new NotImplementedException();
        }
        // Implementation of the IBucket interface
        public byte Count() { return count; }
        public int Total() { return total; }
    }
    /// <summary>
    /// SBookmarks are used to traverse an SDict. 
    /// The current position is the ValueTuple in  _bucket[_bpos]. 
    /// They naturally form a stack, where _parent is closer to the root.
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    public class SBookmark<K,V> where K:IComparable
    {
        public readonly SBucket<K, V> _bucket;
        public readonly int _bpos;
        public readonly SBookmark<K, V>? _parent;
        public SBookmark(SBucket<K,V> b,int bp,SBookmark<K,V>? n)
        {
            _bucket = b; _bpos = bp; _parent = n;
        }
        public SBookmark<K,V>? Next() { return Next(this); }
        public K Key { get { return _bucket.Slot(_bpos).Item1; } }
        public V Value { get { return (_bucket.Slot(_bpos).Item2 is V v) ? v : default(V); } }
        /// <summary>
        /// The position in the tree
        /// </summary>
        /// <returns>The zero-based position in a traversal</returns>
        public int position()
        {
            var r = _parent?.position()??0;
            for (var i = 0; i < _bpos; i++)
                r += (_bucket.Slot(i).Item2 is IBucket b)?b.Total() : 1;
            return r;
        }
        /// <summary>
        /// This implements both SDict.First() and SBookmark.Next()
        /// </summary>
        /// <param name="stk"></param>
        /// <param name="tree"></param>
        /// <returns></returns>
        public static SBookmark<K,V>? Next(SBookmark<K,V>? stk,SDict<K,V>? tree=null)
        {
            SBucket<K, V>? b;
            ValueTuple<K,object?> d;
            if (stk == null) // following Create or Reset
            {
                // if Tree is empty return null
                if (tree == null || tree.root==null || tree.Length == 0)
                    return null;
                // The first entry is root.slots[0] or below
                stk = new SBookmark<K, V>(tree.root, 0, null);
                d = tree.root.Slot(0);
                b = d.Item2 as SBucket<K, V>;
            }
            else // guaranteed to be at a LEAF
            {
                var stkPos = stk._bpos;
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
                    b = ((SInner<K, V>)stk._bucket).gtr;
                    d = new ValueTuple<K,object?>(default(K), null); // or compiler complains
                }
                else // might be leaf or not
                {
                    d = stk._bucket.Slot(stkPos);
                    b = d.Item2 as SBucket<K, V>;
                }
            }
            while (b != null) // now ensure we are at a leaf
            {
                stk = new SBookmark<K, V>(b, 0, stk);
                d = stk._bucket.Slot(0);
                b = d.Item2 as SBucket<K, V>;
            }
            return stk;

        }
    }
    /// <summary>
    /// The slots in a leaf node contain actual values
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V">the value type</typeparam>
    public class SLeaf<K,V> :SBucket<K,V> where K: IComparable
    {
        public readonly ValueTuple<K,V>[] slots;
        public SLeaf(params ValueTuple<K,V>[] s) :base(s.Length,s.Length) { slots = s; }
        public SLeaf(ValueTuple<K,V>[] s,int low,int high) :base(high+1-low,high+1-low)
        {
            slots = new ValueTuple<K,V>[count];
            for (var i = 0; i < count; i++)
                slots[i] = s[i + low];
        }
        internal override ValueTuple<K,object?> Slot(int i)
        {   return slots[i];  }
        public override bool Contains(K k)
        {
            PositionFor(k, out bool b);
            return b;
        }
        public override V Lookup(K k)
        {
            var j = PositionFor(k, out bool b);
            return b ? slots[j].Item2 : default(V);
        }
        internal override SBucket<K, V> Update(K k,V v)
        {
            return new SLeaf<K, V>(Replace(PositionFor(k, out bool _), new ValueTuple<K, V>(k, v)));
        }
        internal override SBucket<K, V> Add(K k, V v)
        {
            return new SLeaf<K, V>(Add(PositionFor(k, out bool _), new ValueTuple<K, V>(k, v)));
        }
        internal override SBucket<K, V> Remove(K k)
        {
            return new SLeaf<K, V>(Remove(PositionFor(k, out bool _)));
        }
        public override int PositionFor(K k, out bool match)
        {
            if (k==null)
            {
                match = slots[0].Item1 == null;
                return 0;
            }
            // binary search
            int low = 0, high = count, mid;
            while (low < high)
            {
                mid = (low + high) >> 1;
                K midk = slots[mid].Item1;
                int c = k.CompareTo(midk);
                if (c == 0)
                {
                    match = true;
                    return mid;
                }
                if (c > 0)
                    low = mid + 1;
                else
                    high = mid;
            }
            match = false;
            return high;
        }
        internal override void Add(ArrayList ab)
        {
            for (var i = 0; i < count; i++)
                ab.Add(slots[i]);
        }
        internal override ValueTuple<K,SBucket<K,V>> LowHalf()
        {
            int m = SDict<K, V>.Size >> 1;
            return new ValueTuple<K,SBucket<K,V>>(slots[m - 1].Item1, new SLeaf<K, V>(slots, 0, m - 1));
        }
        internal override SBucket<K, V> TopHalf()
        {
            return new SLeaf<K, V>(slots, SDict<K, V>.Size >> 1, SDict<K, V>.Size - 1);
        }
        ValueTuple<K,V>[] Splice(int ix,ValueTuple<K,V> ns,ValueTuple<K,V> os)
        {
            var s = new ValueTuple<K,V>[count + 1];
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
        ValueTuple<K,V>[] Replace(int j,ValueTuple<K,V> d)
        {
            var s = new ValueTuple<K,V>[count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }
        ValueTuple<K,V>[] Add(int ix, ValueTuple<K,V> s)
        {
            var t = new ValueTuple<K,V>[count + 1];
            int j = 0, k = 0;
            while (j < ix)
                t[k++] = slots[j++];
            t[k++] = s;
            while (j < count)
                t[k++] = slots[j++];
            return t;
        }
        ValueTuple<K,V>[] Remove(int ix)
        {
            var s = new ValueTuple<K,V>[count - 1];
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
    /// <summary>
    /// The slots in an inner node have (last key in child subtree, child SBucket node).
    /// gtr is a child SBucket node for keys greater than any of the slot subtrees.
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V">the value type</typeparam>
    public class SInner<K, V> : SBucket<K, V> where K : IComparable
    {
        public readonly ValueTuple<K, SBucket<K, V>>[] slots;
        public readonly SBucket<K, V> gtr;
        public SInner(SBucket<K,V> v, int t,params ValueTuple<K,SBucket<K,V>>[] s) :base(s.Length,t)
        { slots = s; gtr = v; }
        public SInner(SBucket<K,V> v,int t, ValueTuple<K,SBucket<K,V>>[] s, int low, int high)
            :base(high+1-low,t)
        {
            slots = new ValueTuple<K, SBucket<K, V>>[count];
            for (var i = 0; i < count; i++)
                slots[i] = s[i + low];
            gtr = v;
        }
        public SInner(SBucket<K,V> v,int t,ValueTuple<K,SBucket<K,V>>[] s1,int low1, int high1,
            ValueTuple<K, SBucket<K, V>>[] s2, int low2, int high2) : base(high1+high2+2-low1-low2,t)
        {
            slots = new ValueTuple<K, SBucket<K, V>>[count];
            int j, k = 0;
            for (j = low1; j <= high1; j++)
                slots[k++] = s1[j];
            for (j = low2; j <= high2; j++)
                slots[k++] = s2[j];
            gtr = v;
        }
        public override bool Contains(K k)
        {
            int j = PositionFor(k, out bool m);
            if (j == count)
                return gtr.Contains(k);
            return slots[j].Item2.Contains(k);
        }
        public override V Lookup(K k)
        {
            int j = PositionFor(k, out bool m);
            if (j == count)
                return gtr.Lookup(k);
            return slots[j].Item2.Lookup(k);
        }
        public override int PositionFor(K k, out bool match)
        {
            if (k==null)
            {
                match = slots[0].Item1 == null;
                return 0;
            }
            // binary search
            int low = 0, high = count, mid;
            while (low < high)
            {
                mid = (low + high) >> 1;
                K midk = slots[mid].Item1;
                int c = k.CompareTo(midk);
                if (c == 0)
                {
                    match = true;
                    return mid;
                }
                if (c > 0)
                    low = mid + 1;
                else
                    high = mid;
            }
            match = false;
            return high;
        }
        internal override SBucket<K, V> Update(K k, V v)
        {
            int j = PositionFor(k, out bool m);
            if (j == count)
                return new SInner<K, V>(gtr.Update(k, v), total, slots);
            else
            {
                var d = slots[j];
                var b = d.Item2.Update(k, v);
                return new SInner<K, V>(gtr, total, Replace(j, new ValueTuple<K, SBucket<K, V>>(d.Item1, b)));
            }
        }
        internal override SBucket<K, V> Add(K k, V v)
        {
            // by the time we get here we have made sure there is at least one empty Slot
            // in the current bucket
            int j = PositionFor(k, out bool m); // (j<count && k<=slots[j]) || j==count
            SBucket<K, V> b;
            if (j < count)
            {
                var d = slots[j];
                b = d.Item2;
                if (b.count == SDict<K, V>.Size)
                    return Split(j)+(k, v); // try again
                return new SInner<K, V>(gtr, total + 1, Replace(j, new ValueTuple<K, SBucket<K, V>>(d.Item1, b.Add(k, v))));
            }
            else
            {
                if (gtr.count == SDict<K, V>.Size)
                    return SplitGtr()+(k, v); // try again
                return new SInner<K, V>(gtr.Add(k, v), total + 1, slots);
            }
        }
        protected virtual SInner<K, V> SplitGtr()
        {
            return new SInner<K, V>(gtr.TopHalf(), total, slots, 0, count - 1, new ValueTuple<K, SBucket<K, V>>[] { gtr.LowHalf() }, 0, 0);
        }
        internal override void Add(ArrayList ab)
        {
            for (var i = 0; i < count; i++)
                ab.Add(slots[i]);
        }
        internal override ValueTuple<K,SBucket<K,V>> LowHalf()
        {
            int m = SDict<K, V>.Size >> 1;
            int h = 0;
            for (int i = 0; i < m; i++)
                h += slots[i].Item2.total;
            return new ValueTuple<K, SBucket<K, V>>(slots[m - 1].Item1, new SInner<K, V>(slots[m - 1].Item2, h, slots, 0, m - 2));
        }

        internal override SBucket<K, V> Remove(K k)
        {
            int nj = PositionFor(k, out bool mm);
            SBucket<K, V> nb;
            int m = SDict<K, V>.Size >> 1;
            if (nj < count)
            {
                var e = slots[nj];
                nb = e.Item2;
                nb = nb.Remove(k);
                if (nb.count >= m)
                    return new SInner<K, V>(gtr, total - 1, Replace(nj, new ValueTuple<K, SBucket<K, V>>(e.Item1, nb)));
            }
            else
            {
                nb = gtr.Remove(k);
                if (nb.count >= m)
                    return new SInner<K, V>(nb, total - 1, slots);
            }
            // completely rebuild the current non-leaf node (too many cases to consider otherwise)
            // still two different cases depending on whether children are leaves
            int S = SDict<K, V>.Size;
            SBucket<K, V>? b, g = null;
            var ab = new ArrayList();
            int i, j;
            for (j = 0; j < count; j++)
            {
                b = (j == nj) ? nb : slots[j].Item2;
                b.Add(ab);
                if (b is SInner<K, V>)
                    ab.Add(new ValueTuple<K, SBucket<K, V>>(slots[j].Item1, ((SInner<K, V>)b).gtr));
            }
            b = (count == nj) ? nb : gtr;
            b.Add(ab);
            if (b is SInner<K, V>)
                g = ((SInner<K, V>)b).gtr;
            var s = ab.ToArray();
            if (g == null) // we use Size entries from s for each new Bucket (all Leaves)
            {
                var ss = new ValueTuple<K, V>[s.Length];
                for (j = 0; j < s.Length; j++)
                    ss[j] = (ValueTuple<K, V>)s[j];
                if (s.Length <= S) // can happen at root: reduce height of tree
                    return new SLeaf<K, V>(ss);
                // suppose s.Length = Size*A+B
                int A = s.Length / S;
                int B = s.Length - A * S;
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                ValueTuple<K, SBucket<K, V>>[] ts = new ValueTuple<K, SBucket<K, V>>[(B == 0) ? (A - 1) : A]; // new list of children
                int sce = 0, dst = 0;
                ValueTuple<K, V> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                for (i = 0; i < ts.Length - C; i++)
                {
                    d = ss[sce + S - 1]; // last entry in new bucket
                    ts[dst++] = new ValueTuple<K, SBucket<K, V>>(d.Item1, new SLeaf<K, V>(ss, sce, sce + S - 1));
                    sce += S;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    m = S >> 1;
                    d = ss[sce + m - 1];
                    ts[dst++] = new ValueTuple<K, SBucket<K, V>>(d.Item1, new SLeaf<K, V>(ss, sce, sce + m - 1));
                    sce += m;
                }
                return new SInner<K, V>(new SLeaf<K, V>(ss, sce, s.Length - 1), total - 1, ts);
            }
            else // we use Size+1 entries from s for each new Bucket: g is an extra one
            {
                var ss = new ValueTuple<K, SBucket<K, V>>[s.Length];
                for (j = 0; j < s.Length; j++)
                    ss[j] = (ValueTuple<K, SBucket<K, V>>)s[j];
                if (s.Length <= S) // can happen at root: reduce height of tree
                    return new SInner<K, V>(g, total - 1, ss);
                int A = (s.Length + 1) / (S + 1); // not forgetting g
                int B = s.Length + 1 - A * (S + 1);
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                ValueTuple<K, SBucket<K, V>>[] ts = new ValueTuple<K, SBucket<K, V>>[(B == 0) ? (A - 1) : A]; // new list of children
                int sce = 0, dst = 0;
                ValueTuple<K, SBucket<K, V>> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                for (i = 0; i < ts.Length - C; i++)
                {
                    d = ss[sce + S]; // last entry in new bucket
                    int dt = 0;
                    for (int di = sce; di < sce + S; di++)
                        dt += ss[di].Item2.total;
                    ts[dst++] = new ValueTuple<K, SBucket<K, V>>(d.Item1, new SInner<K, V>(d.Item2, dt, ss, sce, sce + S - 1));
                    sce += S + 1;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    d = ss[sce + m];
                    int dt = 0;
                    for (int di = sce; di < sce + m; di++)
                        dt += ss[di].Item2.total;
                    ts[dst++] = new ValueTuple<K, SBucket<K, V>>(d.Item1, new SInner<K, V>(d.Item2, dt, ss, sce, sce + m - 1));
                    sce += m + 1;
                }
                int gt = 0;
                for (int di = sce; di < s.Length; di++)
                    gt += ss[di].Item2.total;
                return new SInner<K, V>(new SInner<K, V>(g, gt, ss, sce, s.Length - 1), total - 1, ts);
            }

        }
        protected ValueTuple<K, SBucket<K, V>>[] Replace(int j, ValueTuple<K, SBucket<K, V>> d)
        {
            var s = new ValueTuple<K, SBucket<K, V>>[count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }

        internal override ValueTuple<K,object?> Slot(int i)
        {
            return slots[i];
        }

        internal override SBucket<K, V> TopHalf()
        {
            int m = SDict<K, V>.Size >> 1;
            int h = total;
            for (int i = 0; i < m; i++)
                h -= slots[i].Item2.total;
            return new SInner<K, V>(gtr, h, slots, m, SDict<K, V>.Size - 1);
        }
        SBucket<K,V> Split(int j)
        {
            var d = slots[j];
            var b = d.Item2;
            return new SInner<K, V>(gtr, total, Splice(j, b.LowHalf(), new ValueTuple<K, SBucket<K, V>>(d.Item1, b.TopHalf())));
        }
        ValueTuple<K, SBucket<K, V>>[] Splice(int ix, ValueTuple<K, SBucket<K, V>> ns, ValueTuple<K, SBucket<K, V>> os) // insert ns at ppos ix, replace next by os
        {
            var s = new ValueTuple<K, SBucket<K, V>>[count + 1];
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
    }
}
