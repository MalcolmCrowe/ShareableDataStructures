using System;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;
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
    /// A strongly typed Tree where the key is an ITypedValue
    /// </summary>
    /// <typeparam name="K">The Key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    internal class CTree<K, V> : BTree<K, V>,IComparable
    where K : IComparable where V : IComparable
    {
        public new static CTree<K, V> Empty = new CTree<K, V>();
        protected CTree():base() {}
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="e">A starting entry for the tree</param>
        public CTree(KeyValuePair<K, V> e) : this(new Leaf<K, V>(e)) { }
        /// <summary>
        /// Constructor: Build a new non-empty tree 
        /// </summary>
        /// <param name="k">The first key for the BTree</param>
        /// <param name="v">The first value for the BTree</param>
        public CTree(K k, V v) : this(new KeyValuePair<K, V>(k, v)) { }
        /// <summary>
        /// Constructor: called when modifying the Tree (hence protected). 
        /// Any modification gives a new Tree(usually sharing most of its Buckets and Slots with the old one)
        /// </summary>
        /// <param name="cx">The context for user defined types etc</param>
        /// <param name="kt">The key type</param>
        /// <param name="b">The new top level (root) Bucket</param>
        protected CTree(Bucket<K, V> b)
            : base(b)
        { }
        /// <summary>
        /// Comparison of keys
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public override int Compare(K a, K b)
        {
            return (a == null) ? ((b == null) ? 0 : -1) : a.CompareTo(b);
        }
        /// <summary>
        /// Creator: Create a new Tree that adds a given key,value pair.
        /// An Add operation is either an UpdatePost or an Insert depending on 
        /// whether the old tree contains the key k.
        /// Protected because something useful should be done with the returned value:
        /// code like Add(foo,bar); would do nothing.
        /// </summary>
        /// <param name="k">The key for the new Slot</param>
        /// <param name="v">The value for the new Slot</param>
        /// <returns>The new CTree</returns>
        protected override ATree<K, V> Add(K k, V v)
        {
            if (Contains(k))
                return new CTree<K, V>(root.Update(this, k, v));
            return Insert(k, v);
        }
        /// <summary>
        /// Creator: Create a new Tree that has a new slot in the Tree
        /// May have reorganised buckets or greater depth than this Tree
        /// </summary>
        /// <param name="k">The key for the new Slot (guaranteed not in this Tree)</param>
        /// <param name="v">The value for the new Slot</param>
        /// <returns>The new CTree</returns>
        protected override ATree<K, V> Insert(K k, V v) // this does not contain k
        {
            if (root==null || root.total == 0)  // empty BTree
                return new CTree<K, V>(new KeyValuePair<K, V>(k, v));
            if (root.count == Size)
                return new CTree<K, V>(root.Split()).Add(k, v);
            return new CTree<K, V>(root.Add(this, k, v));
        }
        /// <summary>
        /// Creator: Create a new Tree that has a modified value
        /// </summary>
        /// <param name="k">The key (guaranteed to be in this Tree)</param>
        /// <param name="v">The new value for this key</param>
        /// <returns>The new CTree</returns>
        protected override ATree<K, V> Update(K k, V v) // this Contains k
        {
            if (!Contains(k))
                throw new PEException("PE01");
            return new CTree<K, V>(root.Update(this, k, v));
        }
        /// <summary>
        /// Creator: Create a new Tree if necessary that does not have a given key
        /// May have reoganised buckets or smaller depth than this BTree.
        /// </summary>
        /// <param name="k">The key to remove</param>
        /// <returns>This CTree or a new CTree (guaranteed not to have key k)</returns>
        protected override ATree<K, V> Remove(K k)
        {
            if (!Contains(k))
                return this;
            if (root.total == 1) // empty index
                return Empty;
            // note: we allow root to have 1 entry
            return new CTree<K, V>(root.Remove(this, k));
        }

        public int CompareTo(object obj)
        {
            var that = (CTree<K,V>)obj??Empty;
            var tb = that.First();
            var b = First();
            for (;b!=null && tb!=null;b=b.Next(),tb=tb.Next())
            {
                var c = b.key().CompareTo(tb.key());
                if (c != 0)
                    return c;
                c = b.value().CompareTo(tb.value());
                if (c != 0)
                    return c;
            }
            return (b != null)? 1 : (tb!=null)?-1: 0;
        }

        public static CTree<K, V> operator +(CTree<K, V> tree, (K, V) v)
        {
            return (CTree<K,V>)tree.Add(v.Item1, v.Item2);
        }
        public static CTree<K, V> operator -(CTree<K, V> tree, K k)
        {
            return (CTree<K,V>)tree.Remove(k);
        }
        public static CTree<K,V> operator-(CTree<K,V> tree,CTree<K,V>s)
        {
            for (var b = s.First(); b != null; b = b.Next())
                tree -= b.key();
            return tree;
        }
        /// <summary>
        /// Add of trees, optionally non-destructive
        /// </summary>
        /// <param name="c"></param>
        /// <param name="x">bool Item2 true for non-destructive</param>
        /// <returns></returns>
        public static CTree<K, V> operator +(CTree<K, V> c, (CTree<K, V>, bool) x)
        {
            var (d, nd) = x;
            c = c ?? Empty;
            for (var b = d?.First(); b != null; b = b.Next())
                if (!(c.Contains(b.key()) && nd))
                    c += (b.key(), b.value());
            return c;
        }
        public static CTree<K, V> operator +(CTree<K, V> tree, ATree<K, V> a)
        {
            return (CTree<K, V>)tree.Add(a);
        }
    }
    /// <summary>
    /// A generic strongly-typed Tree for values in the database.
    /// The tree's single-level key and value type are determined at creation.
    /// IMMUTABLE
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class SqlTree : CTree<TypedValue, TypedValue>
    {
        public readonly Sqlx kind;
        /// <summary>
        /// Collect all the information about the tree type.
        /// info.headType is the key type for the SqlTree
        /// </summary>
        public readonly TreeInfo info;
        /// <summary>
        /// private Constructor: for tree operations
        /// </summary>
        /// <param name="ti">The tree info</param>
        /// <param name="vT">The value type</param>
        /// <param name="b">The root bucket</param>
        SqlTree(TreeInfo ti,Sqlx k,Bucket<TypedValue, TypedValue> b)
            : base(b)
        {
            info = ti;
            kind = k;
        }
        /// <summary>
        /// Constructor: a tree at a given depth with a given initial entry.
        /// </summary>
        /// <param name="ti">The treeinfo for the SqlTree</param>
        /// <param name="vType">the nominal value type</param>
        /// <param name="k">a key</param>
        /// <param name="v">a value</param>
        public SqlTree(TreeInfo ti, Sqlx kT, TypedValue k, TypedValue v)
            : this(ti, kT, new Leaf<TypedValue, TypedValue>(new KeyValuePair<TypedValue, TypedValue>(k, v)))
        { }
        public override int Compare(TypedValue a, TypedValue b)
        {
            return info.headType?.Compare(a, b) ?? 0;
        }
        /// <summary>
        /// The normal way of adding a key,value pair to the tree
        /// </summary>
        /// <param name="t">The tree being added to</param>
        /// <param name="k">A key</param>
        /// <param name="v">A value</param>
        public static TreeBehaviour Add(ref SqlTree t, TypedValue k, TypedValue v)
        {
            if (k == null && t.info.onNullKey != TreeBehaviour.Allow)
                return t.info.onNullKey;
            if (t.Contains(k) && t.info.onDuplicate != TreeBehaviour.Allow)
                return t.info.onDuplicate;
   /*         if (k != null && !t.info.headType.CanTakeValueOf(k.dataType))
                throw new DBException("22005M", t.info.headType.kind, k.ToString()).ISO()
                    .AddType(t.info.headType).AddValue(k); */
            ATree<TypedValue, TypedValue> a = t;
            AddNN(ref a, k, v);
            t = (SqlTree)a;
            return TreeBehaviour.Allow;
        }
        /// <summary>
        /// Implementation of the Add operation
        /// </summary>
        /// <param name="k">A key</param>
        /// <param name="v">A valkue</param>
        /// <returns>The new tree</returns>
        protected override ATree<TypedValue, TypedValue> Add(TypedValue k, TypedValue v)
        {
            if (Contains(k))
                return new SqlTree(info, kind, root.Update(this, k, v));
            return Insert(k, v);
        }
        /// <summary>
        /// Implementation of the Insert operation
        /// </summary>
        /// <param name="k">A key</param>
        /// <param name="v">A value</param>
        /// <returns>The new tree</returns>
        protected override ATree<TypedValue, TypedValue> Insert(TypedValue k, TypedValue v)
        {
            if (root == null || root.total == 0)  // empty BTree
                return new SqlTree(info, kind, k, v);
            if (root.count == Size)
                return new SqlTree(info, kind, root.Split()).Add(k, v);
            return new SqlTree(info, kind, root.Add(this, k, v));
        }
        /// <summary>
        /// The normal Remove operation: remove a key
        /// </summary>
        /// <param name="t">The tree target</param>
        /// <param name="k">A key to remove</param>
        public static void Remove(ref SqlTree t, TypedValue k)
        {
            ATree<TypedValue, TypedValue> a = t;
            Remove(ref a, k);
            t = (SqlTree)a;
        }
        /// <summary>
        /// The normal Remove operation: remove a (key,value) pair
        /// </summary>
        /// <param name="t">The tree target</param>
        /// <param name="k">A key</param>
        /// <param name="old">A value to remove for this key</param>
        public static void Remove(ref SqlTree t, TypedValue k, TypedValue old)
        {
            ATree<TypedValue, TypedValue> a = t;
            Remove(ref a, k, old);
            t = (SqlTree)a;
        }
        /// <summary>
        /// Implementation of the Remove operation
        /// </summary>
        /// <param name="k">The key to remove</param>
        /// <returns>The new tree</returns>
        protected override ATree<TypedValue, TypedValue> Remove(TypedValue k)
        {
            if (!Contains(k))
                return this;
            if (root.total == 1) // empty index
                return null;
            // note: we allow root to have 1 entry
            return new SqlTree(info, kind, root.Remove(this, k));
        }
        /// <summary>
        /// The normal Update operation for a key
        /// </summary>
        /// <param name="t">The tree target</param>
        /// <param name="k">The key</param>
        /// <param name="v">The new value</param>
        public static void Update(ref SqlTree t, TypedValue k, TypedValue v)
        {
            ATree<TypedValue, TypedValue> a = t;
            Update(ref a, k, v);
            t = (SqlTree)a;
        }
        /// <summary>
        /// Implementation of the Update operation
        /// </summary>
        /// <param name="k">The key</param>
        /// <param name="v">The new value for the key</param>
        /// <returns>The new tree</returns>
        protected override ATree<TypedValue, TypedValue> Update(TypedValue k, TypedValue v)
        {
            if (!Contains(k))
                throw new PEException("PE01");
            return new SqlTree(info, kind, root.Update(this, k, v));
        }
        public override ABookmark<TypedValue, TypedValue> PositionAt(TypedValue key)
        {
            if (key == null || key.IsNull)
                return First();
            return base.PositionAt(key);
        }
        public static SqlTree operator +(SqlTree tree, (TypedValue,TypedValue) v)
        {
            return (SqlTree)tree.Add(v.Item1, v.Item2);
        }
        public static SqlTree operator -(SqlTree tree, TypedValue k)
        {
            return (SqlTree)tree.Remove(k);
        }
        internal SqlTree Fix(Context cx)
        {
            var r = new SqlTree(info.Fix(cx), kind, null);
            for (var b = First(); b != null; b = b.Next())
                r += (b.key().Fix(cx), b.value().Fix(cx));
            return r;
        }
        internal SqlTree Relocate(Context cx)
        {
            var r = new SqlTree(info.Relocate(cx), kind, null);
            for (var b = First(); b != null; b = b.Next())
                r += (b.key().Relocate(cx), b.value().Relocate(cx));
            return r;
        }
    }
    internal class CList<V> : BList<V>, IComparable where V : IComparable
    {
        public new static readonly CList<V> Empty = new CList<V>();
        protected CList() : base() { }
        public CList(V v) : base(v) { }
        public CList(CList<V> c, V v) : base(c, v) { }
        public CList(CList<V> c, int k, V v) : base(c, k, v) { }
        protected CList(Bucket<int, V> r) : base(r) { }
        protected override ATree<int, V> Add(int k, V v)
        {
            return new CList<V>(base.Add(k, v).root);
        }
        protected override ATree<int, V> Remove(int k)
        {
            return new CList<V>(base.Remove(k).root);
        }
        public static CList<V> operator +(CList<V> b, (int, V) x)
        {
            return (CList<V>)b.Add(x.Item1, x.Item2);
        }
        public static CList<V> operator +(CList<V> b, V k)
        {
            return (CList<V>)b.Add((int)b.Count, k);
        }
        public static CList<V> operator -(CList<V> b, int i)
        {
            return (CList<V>)b.Remove(i);
        }
        public static CList<V> operator-(CList<V> b,CList<V> c)
        {
            while (b.Count>0 && c.Count>0 && b[0].CompareTo(c[0])==0) // NB not !=Empty
            {
                b -= 0;
                c -= 0;
            }
            return b;
        }
        public static CList<V> operator +(CList<V> a,CList<V> b)
        {
            var r = a ?? Empty;
            for (var x = b?.First(); x != null; x = x.Next())
                r += x.value();
            return r;
        }
        public CList<V> Without(V v)
        {
            var r = Empty;
            for (var b = First(); b != null; b = b.Next())
                if (b.value().CompareTo(v) != 0)
                    r += b.value();
            return r;
        }
        public CList<V> Replace(V v,V w)
        {
            var r = Empty;
            for (var b = First(); b != null; b = b.Next())
                if (b.value().CompareTo(v) == 0)
                    r += w;
                else r += b.value();
            return r;
        }
        public int CompareTo(object obj)
        {
            var that = (CList<V>)obj;
            var b = First();
            var tb = that.First();
            for (;b!=null && tb!=null; b=b.Next(),tb=tb.Next())
            {
                var c = b.value().CompareTo(tb.value());
                if (c != 0)
                    return c;
            }
            return (b == null) ? ((tb == null) ? 0 : 1) : -1;
        }
        public override bool Has(V v)
        {
            for (var b = First(); b != null; b = b.Next())
                if (v.CompareTo(b.value()) == 0)
                    return true;
            return false;
        }
        public int IndexOf(V v)
        {
            for (var b = First(); b != null; b = b.Next())
                if (v.CompareTo(b.value()) == 0)
                    return b.key();
            return -1;
        }

        internal CTree<V, bool> ToTree() 
        {
            var r = CTree<V, bool>.Empty;
            for (var b = First(); b != null; b = b.Next())
                r += (b.value(), true);
            return r;
        }
    }
}
