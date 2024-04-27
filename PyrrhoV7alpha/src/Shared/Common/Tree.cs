using System;
using System.Collections.Generic;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Common
{
    /// <summary>
    /// The base class for all Trees in Pyrrho
    /// All trees contain KeyValuePairs in key keys
    /// </summary>
 //   [System.Diagnostics.DebuggerDisplay("{ToString()}")]
	public abstract class ATree<K,V>(Bucket<K, V>? r) where K:IComparable
	{
        /// <summary>
        /// MemoryLimit is a server configuration parameter. The default value of zero (no limit)
        /// means that the server will use all of virtual memory if necessary for its obs structures
        /// It is more practical to set the limit to the size of physical memory to minimise thrashing.
        /// The parameter is here because almost all server obs structures are BTrees.
        /// </summary>
        public const long MemoryLimit = 0L;
        /// <summary>
        /// Size is a system configuration parameter: the maximum number of entries in a Bucket.
        /// It should be an even number, preferably a power of two.
        /// It is const, since experimentally there is little impact on DBMS performance 
        /// using values in the range 8 to 32.
        /// </summary>
        public const int Size = 8;
        /// <summary>
        /// Count is the number of entries in the BTree
        /// </summary>
        public virtual long Count
        {
            get {
                if (root == null)
                    return 0;
                return root.total;
            }
        }
        /// <summary>
        /// The BTree is a hierarchy of Buckets, which are Inner or Leaf buckets
        /// </summary>
        public readonly Bucket<K,V>? root = r;

        /// <summary>
        /// Indexer: Get the value corresponding to a given key
        /// </summary>
        /// <param name="k">The key to find</param>
        /// <returns>The corresponding value (or null)</returns>
        public V? this[K k]
        {
            get
            {
                if (root == null)
                    return default;
                return root.Lookup(this, k);
            }
        }
        /// <summary>
        /// Iteration through the tree contents is done using immutable ABookmarks.
        /// </summary>
        /// <returns>A bookmark positioned at the first element of the tree, or null if the tree is empty</returns>
        public ABookmark<K, V>? First()
        {
            return ABookmark<K, V>.Next(null, this);
        }
        /// <summary>
        /// Iteration through the tree contents is done using immutable ABookmarks.
        /// </summary>
        /// <returns>A bookmark positioned at the last element of the tree, or null if the tree is empty</returns>
        public ABookmark<K, V>? Last()
        {
            ABookmark<K, V>? bm = null;
            var cb = root;
            for (; ; )
            {
                if (cb == null)
                    return null;
                if (cb is Inner<K, V> inr) 
                { 
                    bm = new ABookmark<K, V>(cb, cb.count, bm);
                    cb = inr.gtr;
                }
                else 
                    return new ABookmark<K, V>(cb, cb.count-1, bm);
            }
        }
        /// <summary>
        /// Accessor: look to see if the tree contains a given key
        /// </summary>
        /// <param name="k">the given key</param>
        /// <returns>true if the tree has an association for this key</returns>
        /// <summary>
        public virtual bool Contains(K k)
        {
            if (root == null)
                return false;
            return root.Contains(this, k);
        }
		protected abstract ATree<K,V> Add(K k,V v);
        /// <summary>
        /// Add a given association to a given tree
        /// </summary>
        /// <param name="tree">ref the tree</param>
        /// <param name="k">a key</param>
        /// <param name="v">a new value</param>
		public static void Add(ref ATree<K,V> tree,K k,V v)
		{
			tree = tree.Add(k,v);
        }
        public static ATree<K,V> operator+(ATree<K,V> tree,(K,V) v)
        {
            return tree.Add(v.Item1, v.Item2);
        }
        public virtual ATree<K,V> Add(ATree<K,V>a)
        {
            var tree = this;
            for (var b = a?.First(); b != null; b = b.Next())
                tree = tree.Add(b.key(), b.value());
            return tree;
        }
        /// <summary>
        /// Add a given association to a given tree, checking the key is not null
        /// </summary>
        /// <param name="tree">ref the tree</param>
        /// <param name="k">a non-null key</param>
        /// <param name="v">a new value</param>
        public static void AddNN(ref ATree<K, V> tree, K k, V v)
        {
            if (v == null)
                throw new Exception("PE000");
            Add(ref tree, k, v);
        }
		protected abstract ATree<K,V> Insert(K k,V v);
		internal abstract ATree<K,V> Update(K k,V v);
        /// <summary>
        /// Update a given association in the tree
        /// </summary>
        /// <param name="tree">ref the tree</param>
        /// <param name="k">a key</param>
        /// <param name="v">the new value</param>
		public static void Update(ref ATree<K,V> tree,K k,V v)
		{
			tree = tree.Update(k,v);
		}
		internal abstract ATree<K,V> Remove(K k);
        /// <summary>
        /// Remove a given key
        /// </summary>
        /// <param name="tree">ref the tree</param>
        /// <param name="k">the key to remove</param>
 		public static void Remove(ref ATree<K,V> tree,K k)
		{
			tree = tree.Remove(k);
		}
        public static ATree<K,V> operator-(ATree<K,V> tree,K k)
        {
            return tree.Remove(k);
        }
        protected virtual ATree<K, V> Remove(K k, V v)
        { // overridden for PPTree and PMTree where k may lead to several objects
            return Remove(k);
        }
        /// <summary>
        /// Remove a given association
        /// </summary>
        /// <param name="tree">ref the tree</param>
        /// <param name="k">a key</param>
        /// <param name="v">the value to remove</param>
		public static void Remove(ref ATree<K, V> tree, K k, V v)
        {
            tree = tree.Remove(k, v);
        }
        public abstract int Compare(K a, K b);
        /// <summary>
        /// Find the position where a key would be inserted.
        /// Note: we do not check that key is actually in the tree.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public virtual ABookmark<K, V>? PositionAt(K key)
        {
            ABookmark<K, V>? bmk = null;
            var cb = root;
            while (cb != null)
            {
                var bpos = cb.PositionFor(this, key, out bool _);
                bmk = new ABookmark<K, V>(cb, bpos, bmk);
                if (bpos == cb.count)
                {
                    if (cb is Inner<K, V> inr)
                        cb = inr.gtr;
                    else 
                        return null;
                }
                else
                    cb = cb.Slot(bpos).Value as Bucket<K, V>;
            }
            return bmk;
        }
        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            var cm = '(';
            for (var b = First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ',';
                var k = b.key();
                if (k is long)
                    sb.Append(Uid((long)(object)k));
                else
                    sb.Append(k);
                var v = b.value();
                if ((object?)k != (object?)v)
                {
                    sb.Append('=');
                    sb.Append(b.value());
                }
            }
            if (cm==',')
                sb.Append(')');
            return sb.ToString();
        }
        static string Uid(long u)
        {
            if (u >= 0x7000000000000000)
                return "%" + (u - 0x7000000000000000);
            if (u >= 0x6000000000000000)
                return "`" + (u - 0x6000000000000000);
            if (u >= 0x5000000000000000)
                return "#" + (u - 0x5000000000000000);
            if (u >= 0x4000000000000000)
                return "!" + (u - 0x4000000000000000);
            if (u == -1)
                return "_";
            return "" + u;
        }
    }

    interface IBucket
    {
        byte Count();
        long Total();
    }
    /// <summary>
    /// BTree implementation internals
    /// A Bucket gives access to a subtree of the tree
    /// May be Inner or Leaf bucket
    /// Leaf slots contain actual associations of the tree
    /// Row for Inner slots are subtree Buckets
    /// Immutable
    /// </summary>
    /// <remarks>
    /// Constructor: a new bucket
    /// </remarks>
    /// <param name="c">the count for the new bucket</param>
    /// <param name="tot">the number of values in the new buckey and all its children</param>
    public abstract class Bucket<K,V>(int c, long tot) : IBucket where K:IComparable
    {
        /// <summary>
        /// The number of slots in use
        /// </summary>
        public readonly byte count = (byte)c;
        /// <summary>
        /// The nuimber of values in this bucket and all its children
        /// </summary>
        public readonly long total = tot;

        /// <summary>
        /// Accessor: Look to see if the subtree contains the given key
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">the key to look for</param>
        /// <returns>true iff the subtree contains the key</returns>
        public abstract bool Contains(ATree<K,V> t, K k);
        /// <summary>
        /// Accessor: Get the value for a given key
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">the key to look for</param>
        /// <returns>the associated value (or null)</returns>
        public abstract V? Lookup(ATree<K,V> t, K k);
        /// <summary>
        /// Creator: Make a new Bucket that updates a value
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">a key</param>
        /// <param name="v">a new value</param>
        /// <returns></returns>
        public abstract Bucket<K,V> Update(ATree<K,V> t, K k, V v);
        /// <summary>
        /// Creator: Make a new Bucket adding a new Slot
        /// PRE: there is space in the Bucket
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">a key</param>
        /// <param name="v">a value</param>
        /// <returns></returns>
        public abstract Bucket<K,V> Add(ATree<K,V> t, K k, V v);
        /// <summary>
        /// Creator: Split the root Bucket
        /// gives a bucket with 1 key and 2 children
        /// </summary>
        /// <returns>a new root bucket</returns>
        public Bucket<K,V> Split() 
        {
            return new Inner<K,V>(TopHalf(), total, LowHalf());
        }
        /// <summary>
        /// Helper for splitting buckets: Low half
        /// </summary>
        /// <returns>A Slot (key,Low half Inner)</returns>
        public abstract KeyValuePair<K,Bucket<K,V>> LowHalf();
        /// <summary>
        /// Helper for splitting buckets: high half
        /// </summary>
        /// <returns>A Bucket with the top half slots</returns>
        public abstract Bucket<K,V> TopHalf();
        /// <summary>
        /// Creator: Remove an entry from a Bucket
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">the key to remove</param>
        /// <returns></returns>
        public abstract Bucket<K,V>? Remove(ATree<K,V> t, K k);
        /// <summary>
        /// Accessor: find the position in this bucket at which k should be placed
        /// (maybe ==count if k is gtr than all entries)
        /// </summary>
        /// <param name="t">The tree</param>
        /// <param name="k">a key</param>
        /// <param name="match">whether the key is this bucket</param>
        /// <returns>the position for the key in this bucket</returns>
        public abstract int PositionFor(ATree<K, V> t, K k, out bool match);
        /// <summary>
        /// Return the slot at position i.
        /// Needed because of type mismatch between Leaf and Inner buckets
        /// </summary>
        /// <param name="i">The index of the required slot</param>
        /// <returns>The slot with a weaker type</returns>
        public abstract KeyValuePair<K, object?> Slot(int i);
        /// <summary>
        /// Add a tree of slots: needs a surprisingly weak type
        /// </summary>
        /// <param name="ab">the tree</param>
        public abstract void Add(List<object?> ab);
        /// <summary>
        /// IBucket interface requirement
        /// </summary>
        /// <returns>the number of slots in use</returns>
        public byte Count() { return count; }
        /// <summary>
        /// IBucket interface requirement
        /// </summary>
        /// <returns>the number of entries in this bucket and its children</returns>
        public long Total() { return total; }
        internal virtual int EndPos => count - 1;
        internal abstract Bucket<K, V>? Gtr();
    }
    /// <summary>
    /// Traversal of Trees is done using a stack of immutable ABookmarks.
    /// Given a tree t, we get the first ABookmark by t.First() (or null if the tree is empty)
    /// Given a bookmark b, we get the next bookmark by b.Next() (or null if there are no more entries)
    /// </summary>
    /// <typeparam name="K">The tree's key type</typeparam>
    /// <typeparam name="V">The tree's value type</typeparam>
    /// <remarks>
    /// Constructor: a bucket, a position in it, and the rest of the stack
    /// </remarks>
    /// <param name="b">The current bucket</param>
    /// <param name="bp">The current position</param>
    /// <param name="n">The rest of the stack</param>
    public sealed class ABookmark<K, V>(Bucket<K, V> b, int bp, ABookmark<K, V>? n) where K:IComparable // IMMUTABLE
    {
        /// <summary>
        /// The Bucket this stack entry refers to
        /// </summary>
        public readonly Bucket<K, V> _bucket = b;
        /// <summary>
        /// The position in the Bucket (always less than or equal to the bucket.count)
        /// </summary>
        public readonly int _bpos = bp;
        /// <summary>
        /// The parent stack entry
        /// </summary>
        public readonly ABookmark<K, V>? _parent = n;

        /// <summary>
        /// The key for the bookmarked entry
        /// </summary>
        /// <returns>The bookmarked key</returns>
        public K key() { return _bucket.Slot(_bpos).Key;  }
        /// <summary>
        /// The value for the bookmarked entry
        /// </summary>
        /// <returns>The bookmarked value</returns>
        public V value()
        {
            var v = _bucket.Slot(_bpos).Value;
            if (v is V v1)
                return v1;
            throw new Exception("Null in tree");
        }
        /// <summary>
        /// Tree positions are numbered from 0
        /// </summary>
        /// <returns>The tree position of the bookmarked entry in the tree</returns>
        public long position()
        {
            var r = 0L;
            if (_parent != null)
                r = _parent.position();
            for (int i = 0; i < _bpos; i++)
                r += (_bucket.Slot(i).Value is IBucket b) ? b.Total() : 1;
            return r;
        }
        /// <summary>
        /// Get a bookmark for the next entry in the tree
        /// </summary>
        /// <returns>The bookmark, or null if we are already the last entry</returns>
        public ABookmark<K, V>? Next()
        {
            return Next(this);
        }
        /// <summary>
        /// This is actually the implementation of First() for a tree and Next() for a bookmark
        /// </summary>
        /// <param name="stk">The current bookmark, or null to get the first entry</param>
        /// <param name="tree">(optional) The tree to get the first entry</param>
        /// <returns>The required bookmark, or null if none exists</returns>
        public static ABookmark<K, V>? Next(ABookmark<K,V>? stk,ATree<K,V>? tree = null)
        {
            Bucket<K, V>? b;
            KeyValuePair<K, object?> d;
            if (stk ==null) // following Create or Reset
            {
                // if Tree is empty return null
                if (tree==null || tree.root == null)
                    return null;
                // The first entry is root.slots[0] or below
                stk = new ABookmark<K, V>(tree.root, 0, null);
                d = tree.root.Slot(0);
                b = d.Value as Bucket<K, V>;
            }
            else // guaranteed to be at a LEAF
            {
                var stkPos = stk._bpos;
                if (++stkPos == stk._bucket.count) // this is the right test for a leaf
                {
                    // at end of current bucket: pop till we aren't
                    for(;;)
                    {
                        if (++stkPos <= stk._bucket.count)// this is the right test for a non-leaf; redundantly ok for first time (leaf)
                            break;
                        stk = stk._parent;
                        if (stk is null)
                            break;
                        stkPos = stk._bpos;
                    }
                    // we may run out of the BTree
                    if (stk == null)
                        return null;
                }
                stk = new ABookmark<K, V>(stk._bucket, stkPos, stk._parent);
                if (stk._bpos == stk._bucket.count)
                { // will only happen for a non-leaf
                    b = ((Inner<K, V>)stk._bucket).gtr;
                }
                else // might be leaf or not
                {
                    d = stk._bucket.Slot(stkPos);
                    b = d.Value as Bucket<K, V>;
                }
            }
            while (b != null) // now ensure we are at a leaf
            {
                stk = new ABookmark<K, V>(b, 0, stk);
                d = stk._bucket.Slot(0);
                b = d.Value as Bucket<K, V>;
            }
            return stk;
        }
        public ABookmark<K,V>? Previous()
        {
            return Previous(this);
        }
        public static ABookmark<K, V>? Previous(ABookmark<K, V>? stk, ATree<K, V>? tree = null)
        {
            Bucket<K, V>? b;
            KeyValuePair<K, object?> d;
            if (stk == null) // Last()
            {
                // if Tree is empty return null
                if (tree == null || tree.root == null || tree.Count == 0)
                    return null;
                // The last entry is root.slots[root.count-1] or below
                stk = new ABookmark<K, V>(tree.root, tree.root.EndPos, null);
                d = tree.root.Slot(tree.root.count - 1);
                b = tree.root.Gtr();
            }
            else // guaranteed to be at a LEAF
            {
                var stkPos = stk._bpos - 1;
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
                stk = new ABookmark<K, V>(stk._bucket, stkPos, stk._parent);
                d = stk._bucket.Slot(stkPos);
                b = d.Value as Bucket<K, V>;
            }
            while (d.Value is Bucket<K, V> && b is not null)
            {
                stk = new ABookmark<K, V>(b, b.EndPos, stk);
                d = b.Slot(b.count - 1);
                b = b.Gtr();
            }
            return stk;
        }
    }

}
