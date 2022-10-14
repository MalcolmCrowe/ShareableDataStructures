using System;
using System.Collections;
using System.Collections.Generic;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Common
{
    /// <summary>
    /// A BTree is a BTREE which uses immutable classes
    /// to assist with transaction start and rollback.
    /// Hence no Mutators.
    /// Shareable if both K and V are shareable.
    /// This subclass is for IComparable struct type keys.
    /// </summary>
    public class BTree<K,V> : ATree<K,V>
        where K: IComparable
    {
        public readonly static BTree<K, V> Empty = new BTree<K, V>();
        /// <summary>
        /// Constructor
        /// </summary>
        public BTree() : base(null) { }
        /// <summary>
        /// Constructor: Build a new non-empty tree 
        /// </summary>
        /// <param name="k">The first key for the BTree</param>
        /// <param name="v">The first value for the BTree</param>
        public BTree(K k, V v) : base(new Leaf<K,V>(new KeyValuePair<K,V>(k,v))) {}
        /// <summary>
        /// Constructor: called when modifying the BTree (hence protected). 
        /// Any modification gives a new BTree(usually sharing most of its Buckets and Slots with the old one)
        /// </summary>
        /// <param name="b">The top level (root) Bucket</param>
        protected BTree(Bucket<K,V> b) : base(b) {}
        /// <summary>
        /// The Compare function checks for null values
        /// </summary>
        /// <param name="a">A key to compare</param>
        /// <param name="b">Another key to compare</param>
        /// <returns></returns>
        public override int Compare(K a, K b)
        {
            if (a == null)
            {
                if (b == null)
                    return 0;
                return -1;
            }
            if (b == null)
                return 1;
            return a.CompareTo(b);
        }
        /// <summary>
        /// Creator: Create a new BTree that adds a given key,value pair.
        /// An Add operation is either an Update or an Insert depending on 
        /// whether the old tree contains the key k.
        /// Protected because something useful should be done with the returned value:
        /// code like Add(foo,bar); would do nothing.
        /// </summary>
        /// <param name="k">The key for the new Slot</param>
        /// <param name="v">The value for the new Slot</param>
        /// <returns>The new BTree</returns>
        protected override ATree<K,V> Add(K k, V v)
        {
            if (Contains(k))
                return new BTree<K,V>(root.Update(this, k, v));
            return Insert(k, v);
        }
        public static BTree<K, V> operator +(BTree<K, V> tree, (K, V) v)
        {
            return (BTree<K,V>)tree.Add(v.Item1, v.Item2);
        }
        public static BTree<K, V> operator +(BTree<K, V> tree, ATree<K, V> a)
        {
            return (BTree<K, V>)tree.Add(a);
        }
        /// <summary>
        /// Add of trees, optionally non-destructive
        /// </summary>
        /// <param name="c"></param>
        /// <param name="x">bool Item2 true for non-destructive</param>
        /// <returns></returns>
        public static BTree<K,V> operator+(BTree<K,V> c,(BTree<K,V>,bool) x)
        {
            var (d, nd) = x;
            for (var b = d.First(); b != null; b = b.Next())
                if (!(c.Contains(b.key())&&nd))
                    c += (b.key(), b.value());
            return c;
        }
        public static BTree<K, V> operator -(BTree<K, V> tree, K k)
        {
            return (BTree<K, V>)tree?.Remove(k);
        }
        public static BTree<K,V> operator-(BTree<K,V> tree,BList<K> ks)
        {
            for (var b = ks.First(); b != null; b = b.Next())
                tree -= b.value();
            return tree;
        }
        /// <summary>
        /// Creator: Create a new BTree that has a new association in the BTree
        /// May have reorganised buckets or greater depth than this BTree
        /// </summary>
        /// <param name="k">The key for the new Slot (guaranteed not in this BTree)</param>
        /// <param name="v">The value for the new Slot</param>
        /// <returns>The new BTree</returns>
        protected override ATree<K,V> Insert(K k, V v) // this does not contain k
        {
            if (root==null || root.total==0)  // empty BTree
                return new BTree<K,V>(k, v);
            if (root.count == Size)
                return new BTree<K,V>(root.Split()).Add(k, v);
            return new BTree<K,V>(root.Add(this, k, v));
        }
        /// <summary>
        /// Creator: Create a new BTree that has a modified value
        /// </summary>
        /// <param name="k">The key (guaranteed to be in this BTree)</param>
        /// <param name="v">The new value for this key</param>
        /// <returns>The new BTree</returns>
        protected override ATree<K,V> Update(K k, V v) // this Contains k
        {
            if (!Contains(k))
                throw new Exception("PE01");
            return new BTree<K,V>(root.Update(this, k, v));
        }
        /// <summary>
        /// Creator: Create a new BTree if necessary that does not have a given key
        /// May have reoganised buckets or smaller depth than this BTree.
        /// </summary>
        /// <param name="k">The key to remove</param>
        /// <returns>This BTree or a new BTree (guaranteed not to have key k)</returns>
        protected override ATree<K,V> Remove(K k)
        {
            if (!Contains(k))
                return this;
            if (root.total == 1) // empty index
                return Empty;
            // note: we allow root to have 1 entry
            return new BTree<K,V>(root.Remove(this, k));
        }
        internal static BTree<K,bool> FromList(BList<K> ls)
        {
            var r = BTree<K,bool>.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
                r += (b.value(), true);
            return r;
        }
    }
        /// <summary>
        /// Buckets are used to hold up to Size key-value pairs.
        /// Inner buckets have values that are subtrees (Inner or Leaf)
        /// Various constructors help to ensure that Buckets are never modified.
        /// (No Mutators)
        /// </summary>
        public class Inner<K,V> : Bucket<K,V>
    {
        internal readonly KeyValuePair<K, Bucket<K, V>>[] slots;
        // INVARIANT: this subtree has >Size/2 entries
        // INVARIANT: for each j, slot[j].Value holds subtree with keys <=slots[j].Key but >slots[j-1].Key
        public readonly Bucket<K,V> gtr; // never null: holds subtree with keys >slots[count-1].Key
        internal override int EndPos => count;
        internal override Bucket<K, V> Gtr() { return gtr;  }
        /// <summary>
        /// Constructor: given array of Slots for the Bucket, and the gtr subtree
        /// </summary>
        /// <param name="v">subtree containing keys >slots[count-1].Key</param>
        /// <param name="t">number of values in the new bucket and all its children</param>
        /// <param name="s">the slots for this bucket are all subtrees
        /// PRE: for each j, slot[j].Value holds subtree with keys .le.slots[j].Key but .gt.slots[j-1].Key </param>
        public Inner(Bucket<K, V> v, long t, params KeyValuePair<K, Bucket<K, V>>[] s)
            : base(s.Length, t)
        {
            slots = s;
            gtr = v;
        }
        /// <summary>
        /// Constructor: given slice of an array of Slots for the Bucket, and the gtr subtree
        /// </summary>
        /// <param name="v">subtree containing keys >slots[count-1].Key</param>
        /// <param name="t">number of values in the new bucket and all its children</param>
        /// <param name="s">the slots for this bucket are a slice of this array</param>
        /// <param name="low">First entry in the array to use</param>
        /// <param name="high">Last entry in the array to use</param>
        /// PRE: for each j, slot[j].Value holds subtree with keys .le.slots[j].Key but .gt.slots[j-1].Key
        public Inner(Bucket<K, V> v, long t, KeyValuePair<K, Bucket<K,V>>[] s, int low, int high)
            : base(high+1-low,t)
        {
            slots = new KeyValuePair<K, Bucket<K,V>>[count];
            for (int j = 0; j < count; j++)
                slots[j] = s[j + low];
            gtr = v;
        }
        /// <summary>
        /// Constructor: given two slices of Slots for the Bucket, and the gtr subtree
        /// </summary>
        /// <param name="v">subtree containing keys >slots[count-1].Key</param>
        /// <param name="s1">Slots whose values are subtrees</param>
        /// <param name="low1">First entry in s1 to use</param>
        /// <param name="high1">Last entry in s1 to use</param>
        /// <param name="s2">Slots whose values are subtrees</param>
        /// <param name="low2">First entry in s2 to use</param>
        /// <param name="high2">Last entry in s2 to use</param>
        /// PRE: For s1 and s2: for each j, slot[j].Value holds subtree with keys .le.slots[j].Key but .gt.slots[j-1].Key
        public Inner(Bucket<K, V> v, long t, KeyValuePair<K, Bucket<K, V>>[] s1, int low1, int high1,
            KeyValuePair<K, Bucket<K, V>>[] s2, int low2, int high2)
            : base(high1+high2+2-low1-low2,t)
        {
            slots = new KeyValuePair<K, Bucket<K, V>>[count];
            int j, k = 0;
            for (j = low1; j <= high1; j++)
                slots[k++] = s1[j];
            for (j = low2; j <= high2; j++)
                slots[k++] = s2[j];
            gtr = v;
        }
        public override KeyValuePair<K, object> Slot(int i)
        {
            return new KeyValuePair<K, object>(slots[i].Key, slots[i].Value);
        }
        /// <summary>
        /// Accessor: Look to see if our subtree has a given key
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">The key to look for</param>
        /// <returns>true iff the key is in our subtree</returns>
        public override bool Contains(ATree<K,V> t, K k)
        {
            int j = PositionFor(t, k, out bool _);
            if (j == count)
                return gtr.Contains(t, k);
            return slots[j].Value.Contains(t, k);
        }
        /// <summary>
        /// Accessor: Return the value corresponding to a given key
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key to look for</param>
        /// <returns>the object found (or null of not there)</returns>
        public override V Lookup(ATree<K,V> t, K k)
        {
            int j = PositionFor(t, k, out bool _);
            if (j == count)
                return gtr.Lookup(t, k);
            return slots[j].Value.Lookup(t, k);
        }
        /// <summary>
        /// Accessor: find the position in this bucket at which k should be placed
        /// (maybe ==count if k is gtr than all entries)
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">a key</param>
        /// <returns>the position for the key in this bucket</returns>
        public override int PositionFor(ATree<K, V> t, K k, out bool match)
        {
            // binary search
            int low = 0, high = count, mid;
            while (low < high)
            {
                mid = (low + high) >> 1;
                K midk = slots[mid].Key;
                int c = t.Compare(k, midk);
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
        /// <summary>
        /// Create a new Bucket from this one, splitting a full bucket
        /// PRE: This Bucket has a space, but the jth Bucket is full (j.lt.count)
        /// </summary>
        /// <param name="j">The index of the full Bucket</param>
        /// <returns>The new Bucket</returns>
        protected virtual Bucket<K,V> Split(int j)// this has a space; but the jth Bucket is full (j<count)
        {
            var d = slots[j];
            var b = d.Value;
            return new Inner<K, V>(gtr, total, Splice(j, b.LowHalf(), new KeyValuePair<K, Bucket<K, V>>(d.Key, b.TopHalf())));
        }
        /// <summary>
        /// Creator: Create a new Bucket from this, with an updated value
        /// The updated value will be in a new leaf descendant of the new Bucket
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key value (guaranteed to be in our subtree)</param>
        /// <param name="v">The new value for this key</param>
        /// <returns>The new Bucket</returns>
        public override Bucket<K,V> Update(ATree<K,V> t, K k, V v)
        {
            int j = PositionFor(t, k, out bool _);
            if (j == count)
                return new Inner<K,V>(gtr.Update(t, k, v),total, slots);
            else
            {
                var d = slots[j];
                var b = d.Value.Update(t, k, v);
                return new Inner<K, V>(gtr, total, Replace(j, new KeyValuePair<K, Bucket<K, V>>(d.Key, b)));
            }
        }
        /// <summary>
        /// Creator: Create a new Bucket from this, with an added key,value pair
        /// The new Slot will be in a new leaf descendant of the new Bucket
        /// PRE: This Bucket has at least one space
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key value (guaranteed not to be in our subtree)</param>
        /// <param name="v">The new value for this key</param>
        /// <returns>The new Bucket</returns>
        public override Bucket<K,V> Add(ATree<K,V> t, K k, V v)
        {
            // by the time we get here we have made sure there is at least one empty Slot
            // in the current bucket
            int j = PositionFor(t, k, out bool _); // (j<count && k<=slots[j]) || j==count
            Bucket<K,V> b;
            if (j < count)
            {
                var d = slots[j];
                b = d.Value;
                if (b.count == ATree<K,V>.Size)
                    return Split(j).Add(t, k, v); // try again
                return new Inner<K, V>(gtr, total+1, Replace(j, new KeyValuePair<K, Bucket<K, V>>(d.Key, b.Add(t, k, v))));
            }
            else
            {
                if (gtr.count == ATree<K,V>.Size)
                    return SplitGtr().Add(t, k, v); // try again
                return new Inner<K,V>(gtr.Add(t, k, v), total+1, slots);
            }
        }
        /// <summary>
        /// Construct a slot for the lower half of this Bucket
        /// </summary>
        /// <returns>The new Slot</returns>
        public override KeyValuePair<K,Bucket<K,V>> LowHalf()
        {
            int m = ATree<K,object>.Size >> 1;
            long h = 0;
            for (int i = 0; i < m; i++)
                h += slots[i].Value.total;
            return new KeyValuePair<K,Bucket<K,V>>(slots[m - 1].Key, new Inner<K,V>(slots[m - 1].Value, h, slots, 0, m - 2));
        }
        /// <summary>
        /// Creator: Construct a Bucket for the upper half of this Bucket
        /// </summary>
        /// <returns>The new Bucket</returns>
        public override Bucket<K,V> TopHalf()
        {
            int m = ATree<K,V>.Size >> 1;
            long h = total;
            for (int i = 0; i < m; i++)
                h -= slots[i].Value.total;
            return new Inner<K,V>(gtr, h, slots, m, ATree<K,V>.Size - 1);
        }
        /// <summary>
        /// Creator: this has a space; but the gtr Bucket is full
        /// </summary>
        /// <returns>The new Bucket</returns>
        protected virtual Inner<K,V> SplitGtr()
        {
            return new Inner<K,V>(gtr.TopHalf(), total, slots, 0, count - 1, new KeyValuePair<K,Bucket<K,V>>[] { gtr.LowHalf() }, 0, 0);
        }
        /// <summary>
        /// Very internal: add a list of slots using a weak type
        /// </summary>
        /// <param name="ab">the slots to add</param>
        public override void Add(List<object> ab)
        {
            for (int i = 0; i < count; i++)
                ab.Add(slots[i]);
        }
        /// <summary>
        /// Creator: Create a new Bucket from this, which does not contain key k
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key to remove (guaranteed to be in this subtree)</param>
        /// <returns>The new Bucket</returns>
        public override Bucket<K, V> Remove(ATree<K, V> t, K k)
        {
            int nj = PositionFor(t, k, out bool _);
            Bucket<K,V> nb;
            int m = ATree<K,V>.Size >> 1;
            if (nj < count)
            {
                var e = slots[nj];
                nb = e.Value;
                nb = nb.Remove(t, k);
                if (nb.count >= m)
                    return new Inner<K,V>(gtr, total-1, Replace(nj, new KeyValuePair<K, Bucket<K, V>>(e.Key, nb)));
            }
            else
            {
                nb = gtr.Remove(t, k);
                if (nb.count >= m)
                    return new Inner<K,V>(nb, total-1, slots);
            }
            // completely rebuild the current non-leaf node (too many cases to consider otherwise)
            // still two different cases depending on whether children are leaves
            int S = ATree<K,V>.Size;
            var ab = new List<object>();
            Bucket<K,V> b, g = null;
            int i, j;
            for (j = 0; j < count; j++)
            {
                b = (j == nj) ? nb : slots[j].Value;
                b.Add(ab);
                if (b is Inner<K, V> bj)
                    ab.Add(new KeyValuePair<K, Bucket<K, V>>(slots[j].Key, bj.gtr));
            }
            b = (count == nj) ? nb : gtr;
            b.Add(ab);
            if (b is Inner<K,V> bi)
                g = bi.gtr;
            var s = ab.ToArray();
            if (g == null) // we use Size entries from s for each new Bucket (all Leaves)
            {
                var ss = new KeyValuePair<K,V>[s.Length];
                for (j = 0; j < s.Length; j++)
                    ss[j] = (KeyValuePair<K,V>)s[j]; 
                if (s.Length <= S) // can happen at root: reduce height of tree
                    return new Leaf<K, V>(ss);
                // suppose s.Length = Size*A+B
                int A = s.Length / S;
                int B = s.Length - A * S;
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                KeyValuePair<K, Bucket<K,V>>[] ts = new KeyValuePair<K, Bucket<K,V>>[(B == 0) ? (A - 1) : A]; // new list of children
                int sce = 0, dst = 0;
                KeyValuePair<K, V> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                for (i = 0; i < ts.Length - C; i++)
                {
                    d = ss[sce + S - 1]; // last entry in new bucket
                    ts[dst++] = new KeyValuePair<K,Bucket<K,V>>(d.Key, new Leaf<K,V>(ss, sce, sce + S - 1));
                    sce += S;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    m = S >> 1;
                    d = ss[sce + m - 1];
                    ts[dst++] = new KeyValuePair<K, Bucket<K, V>>(d.Key, new Leaf<K, V>(ss, sce, sce + m - 1));
                    sce += m;
                }
                return new Inner<K,V>(new Leaf<K,V>(ss, sce, s.Length - 1), total-1, ts);
            }
            else // we use Size+1 entries from s for each new Bucket: g is an extra one
            {
                var ss = new KeyValuePair<K, Bucket<K,V>>[s.Length];
                for (j = 0; j < s.Length; j++)
                    ss[j] = (KeyValuePair<K, Bucket<K,V>>)s[j]; 
                if (s.Length <= S) // can happen at root: reduce height of tree
                    return new Inner<K,V>(g, total-1, ss);
                int A = (s.Length + 1) / (S + 1); // not forgetting g
                int B = s.Length + 1 - A * (S + 1);
                // need t.Length = A-1 if B==0, else A (size gtr can take up to Size entries)
                KeyValuePair<K, Bucket<K, V>>[] ts = new KeyValuePair<K, Bucket<K, V>>[(B == 0) ? (A - 1) : A]; // new list of children
                int sce = 0, dst = 0;
                KeyValuePair<K, Bucket<K, V>> d;
                // if B==0 or B>=Size>>1 we want t.Length entries constructed here
                // if 1<=B<(Size>>1) we need to keep one in hand for later
                int C = (1 <= B && B < (S >> 1)) ? 1 : 0;
                for (i = 0; i < ts.Length - C; i++)
                {
                    d = ss[sce + S]; // last entry in new bucket
                    long dt = 0;
                    for (int di = sce; di < sce + S; di++)
                        dt += ss[di].Value.total;
                    ts[dst++] = new KeyValuePair<K, Bucket<K, V>>(d.Key, new Inner<K, V>(d.Value, dt, ss, sce, sce + S - 1));
                    sce += S + 1;
                }
                if (C == 1)
                {
                    // be careful for the last entry: the new gtr still needs at least Size>>1 entries
                    d = ss[sce + m];
                    long dt = 0;
                    for (int di = sce; di < sce+m; di++)
                        dt += ss[di].Value.total;
                    ts[dst++] = new KeyValuePair<K, Bucket<K, V>>(d.Key, new Inner<K, V>(d.Value, dt, ss, sce, sce + m - 1));
                    sce += m + 1;
                }
                long gt = 0;
                for (int di = sce; di < s.Length; di++)
                    gt += ss[di].Value.total;
                return new Inner<K,V>(new Inner<K,V>(g, gt, ss, sce, s.Length - 1), total-1, ts);
            }
        }
        // utility routines
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="ix">A position in slots array</param>
        /// <param name="ns">A new slot to be inserted at this position</param>
        /// <param name="os">A slot to replace the old ixth one, now at the next position</param>
        /// <returns>A new slot array</returns>
        protected KeyValuePair<K, Bucket<K, V>>[] Splice(int ix, KeyValuePair<K, Bucket<K, V>> ns, KeyValuePair<K, Bucket<K, V>> os) // insert ns at ppos ix, replace next by os
        {
            var s = new KeyValuePair<K, Bucket<K, V>>[count + 1];
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
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="j">A position in slots array</param>
        /// <param name="q">A new slot to update this position</param>
        /// <returns>a new slot array</returns>
        protected KeyValuePair<K, Bucket<K, V>>[] Replace(int j, KeyValuePair<K, Bucket<K, V>> d)
        {
            var s = new KeyValuePair<K, Bucket<K, V>>[count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }
    }
    /// <summary>
    /// Leaf Buckets contain the actual key,value pairs
    /// Immutable. No Mutators
    /// </summary>
    public class Leaf<K,V> : Bucket<K,V>
    {
        internal readonly KeyValuePair<K,V>[] slots;
        internal override int EndPos => count - 1;
        internal override Bucket<K, V> Gtr() { return null; }
        /// <summary>
        /// Constructor: a Leaf from a given set of Slots
        /// </summary>
        /// <param name="s">The given Slots</param>
        public Leaf(params KeyValuePair<K,V>[] s) :base(s.Length,s.Length)
        {
            slots = s;
        }
        /// <summary>
        /// Constructor: a Leaf from a slice of a given array of Slots
        /// </summary>
        /// <param name="s">The array of Slots</param>
        /// <param name="low">The first entry to use</param>
        /// <param name="high">The last entry to use</param>
        public Leaf(KeyValuePair<K, V>[] s, int low, int high) : base(high+1-low,high+1-low)
        {
            slots = new KeyValuePair<K,V>[count];
            for (int j = 0; j < count; j++)
                slots[j] = s[j + low];
        }
        /// <summary>
        /// Return a weakly typed slot
        /// </summary>
        /// <param name="i">the position to use</param>
        /// <returns>a slot with a weaker type</returns>
        public override KeyValuePair<K, object> Slot(int i)
        {
            return new KeyValuePair<K,object>(slots[i].Key,slots[i].Value);
        }
        /// <summary>
        /// Accessor: Find given key in this subtree
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key to find</param>
        /// <returns>true iff the key is found</returns>
        public override bool Contains(ATree<K,V> t, K k)
        {
            PositionFor(t, k, out bool b);
            return b;
        }
        /// <summary>
        /// Accessor: Find the value for a given key
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">The key to find</param>
        /// <returns>The corresponding value (or null)</returns>
        public override V Lookup(ATree<K,V> t, K k)
        {
            int j = PositionFor(t, k, out bool b);
            if (!b)
                return default;
            return slots[j].Value;
        }
        /// <summary>
        /// Creator: Create a new Leaf bucket with new value for given key
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">the key (guaranteed to be in the Leaf)</param>
        /// <param name="v">the new value</param>
        /// <returns>The new Bucket</returns>
        public override Bucket<K,V> Update(ATree<K,V> t, K k, V v)
        {
            var j = PositionFor(t, k, out bool b);
            if (!b)
                throw new Exception("PE06");
            return new Leaf<K,V>(Replace(j, new KeyValuePair<K,V>(slots[j].Key, v)));
        }
        /// <summary>
        /// Creator: Create a new Leaf bucket with a new key,value pair
        /// PRE: there is space in this bucket for the new Slot
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">the key value (guaranteed not in the Leaf)</param>
        /// <param name="v">the corresponding value</param>
        /// <returns>The new Leaf Bucket</returns>
        public override Bucket<K,V> Add(ATree<K,V> t, K k, V v)
        {
            // by the time we get here we have made sure there is at least one empty Slot
            // in the current bucket
            int j = PositionFor(t, k, out bool _); // (j<count && k<=slots[j]) || j==count
            return new Leaf<K,V>(Add(j, new KeyValuePair<K,V>(k, v)));
        }
        /// <summary>
        /// Make a Slot (to go in an Inner) using the low half of this Leaf
        /// </summary>
        /// <returns>A new Slot</returns>
        public override KeyValuePair<K,Bucket<K,V>> LowHalf()
        {
            int m = ATree<K,V>.Size >> 1;
            return new KeyValuePair<K, Bucket<K,V>>(slots[m - 1].Key, new Leaf<K, V>(slots, 0, m - 1));
        }
        /// <summary>
        /// Creator: Make a new Leaf Bucket using the top half of this Bucket
        /// </summary>
        /// <returns>A new Bucket</returns>
        public override Bucket<K,V> TopHalf()
        {
            int m = ATree<K,V>.Size >> 1;
            return new Leaf<K,V>(slots, m, ATree<K,V>.Size - 1);
        }
        /// <summary>
        /// Creator: Create a new Leaf bucket without a given key
        /// </summary>
        /// <param name="k">The key to avoid</param>
        /// <returns>A new Bucket</returns>
        public override Bucket<K,V> Remove(ATree<K,V> t, K k)
        {
            int j = PositionFor(t, k, out bool b);
            if (!b)
                throw new PEException("PE07");
            return new Leaf<K,V>(Remove(j));
        }
        /// <summary>
        /// Add a list of slots
        /// </summary>
        /// <param name="ab">weaker typed list to add</param>
        public override void Add(List<object> ab)
        {
            for (int i = 0; i < count; i++)
                ab.Add(slots[i]);
        }
        /// <summary>
        /// Accessor: find the position in this bucket at which k should be placed
        /// (maybe ==count if k is gtr than all entries)
        /// </summary>
        /// <param name="t">the tree</param>
        /// <param name="k">a key</param>
        /// <returns>the position for the key in this bucket</returns>
        public override int PositionFor(ATree<K, V> t, K k, out bool match)
        {
            // binary search
            int low = 0, high = count, mid;
            while (low < high)
            {
                mid = (low + high) >> 1;
                K midk = slots[mid].Key;
                int c = t.Compare(k, midk);
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
        // utility routines
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="ix">A position in slots array</param>
        /// <param name="ns">A new slot to be inserted at this position</param>
        /// <param name="os">A slot to replace the old ixth one, now at the next position</param>
        /// <returns>A new slot array</returns>
        protected KeyValuePair<K, V>[] Splice(int ix, KeyValuePair<K, V> ns, KeyValuePair<K, V> os) // insert ns at ppos ix, replace next by os
        {
            var s = new KeyValuePair<K, V>[count + 1];
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
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="j">A position in slots array</param>
        /// <param name="d">A new slot to update this position</param>
        /// <returns>a new slot array</returns>
        protected KeyValuePair<K, V>[] Replace(int j, KeyValuePair<K, V> d)
        {
            var s = new KeyValuePair<K,V>[count];
            int i = 0;
            while (i < count)
            {
                s[i] = slots[i];
                i++;
            }
            s[j] = d;
            return s;
        }
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="ix">A position in slots array</param>
        /// <param name="s">A slot to be inserted at this position</param>
        /// <returns>a new slot array</returns>
        protected KeyValuePair<K, V>[] Add(int ix, KeyValuePair<K, V> s)
        {
            var t = new KeyValuePair<K, V>[count + 1];
            int j = 0, k = 0;
            while (j < ix)
                t[k++] = slots[j++];
            t[k++] = s;
            while (j < count)
                t[k++] = slots[j++];
            return t;
        }
        /// <summary>
        /// Helper for a new array of Slots based on this
        /// </summary>
        /// <param name="ix">A position in the Slot array to be removed</param>
        /// <returns>a new slot array</returns>
        protected KeyValuePair<K, V>[] Remove(int ix)
        {
            var s = new KeyValuePair<K, V>[count - 1];
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
    /// Warning: the semantics of BList operations have changed.
    /// The + and - operators always add or remove and are O(N) 
    /// The following two alternatives are O(logN)
    /// To add an entry to the end of a list, use new BList(BList old,V v)
    /// If you want to replace an item, use new BList(BList old,int k,V v)
    /// </summary>
    /// <typeparam name="V"></typeparam>
    public class BList<V> : BTree<int, V>
    {
        public new static readonly BList<V> Empty = new BList<V>();
        public int Length => (int)Count;
        protected BList() : base() { }
        BList(BTree<int, V> t) : base(t.root) { }
        public BList(V v) : base(0, v) { }
        /// <summary>
        /// Use this constructor for b=b.REPLACE(k,v)
        /// We don't implement REPLACE itself because people forget the LHS
        /// </summary>
        /// <param name="b">The old list</param>
        /// <param name="k">An index</param>
        /// <param name="v"></param>
        public BList(BList<V> b, int k, V v)
            : base((((BTree<int, V>)b) + (k, v)).root)
        {
            if (k < 0 || k >= b.Length) // b is old version
                throw new NotSupportedException();
        }
        /// <summary>
        /// Use this constructor for b=b.ADDTOTAIL(v)
        /// We don't implement ADDTOTAIL itself because people forget the LHS
        /// </summary>
        /// <param name="b">The old list</param>
        /// <param name="v">A value to add at the end</param>
        public BList(BList<V> b, V v)
            : base((((BTree<int, V>)b) + (b.Length, v)).root) { }
        protected BList(Bucket<int, V> r) : base(r) { }
        protected override ATree<int, V> Add(int k, V v)
        {
            var r = BTree<int,V>.Empty;
            var done = false;
            var c = 0;
            for (var b= First();b!=null;b=b.Next())
            {
                if (b.key() == k)
                {
                    r += (c++, v);
                    done = true;
                }
                r += (c++, b.value());
            }
            if (!done)
            {
                while (c < k - 1)
                    r += (c++, default(V));
                r += (c++, v);
            }
            return new BList<V>(r);
        }
        protected override ATree<int, V> Remove(int k)
        {
            var r = Empty;
            for (var b = First(); b != null; b = b.Next())
                if (b.key() != k)
                    r = new BList<V>(r,b.value());
            return r;
        }
        public static BList<V> operator +(BList<V> b, (int, V) x)
        {
            var (k, v) = x;
            if (k == b.Length) // use ADDTOTAIL
                return new BList<V>(b, v); 
            // use slow version
            return (BList<V>)b.Add(k, v);
        }
        public static BList<V> operator +(BList<V> b, V v)
        {
            return (BList<V>)b.Add((int)b.Count, v);
        }
        public static BList<V> operator +(BList<V> b, BList<V> c)
        {
            for (var cb = c.First(); cb != null; cb = cb.Next())
                b += cb.value();
            return b;
        }
        public static BList<V> operator -(BList<V> b, int i)
        {
            return (BList<V>)b.Remove(i);
        }
        public V[] ToArray()
        {
            var r = new V[Length];
            var i = 0;
            for (var b = First(); b != null; b = b.Next(), i++)
                r[i] = b.value();
            return r;
        }
        public static BList<V> FromArray(params V[] vs)
        {
            var r = Empty;
            foreach (var v in vs)
                r += v;
            return r;
        }
        public virtual bool Has(V v)
        {
            for (var b = First(); b != null; b = b.Next())
                if ((object)b.value() == (object)v)
                    return true;
            return false;
        }
    }
}
