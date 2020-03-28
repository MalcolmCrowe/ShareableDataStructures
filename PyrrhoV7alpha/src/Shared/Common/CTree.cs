using System;
using System.Collections.Generic;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Common
{
    /// <summary>
    /// A strongly typed Tree where the key is an ITypedValue
    /// </summary>
    /// <typeparam name="K">The Key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    internal class CTree<K, V> : BTree<K, V>
    where K : ITypedValue
    {
        internal readonly Sqlx kind;
        internal CTree(Sqlx k) :this(k,null) 
        {
            kind = k;
        }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="kt">The key type</param>
        /// <param name="e">A starting entry for the tree</param>
        public CTree(Sqlx k, KeyValuePair<K, V> e) : this(k, new Leaf<K, V>(e)) { }
        /// <summary>
        /// Constructor: called when modifying the Tree (hence protected). 
        /// Any modification gives a new Tree(usually sharing most of its Buckets and Slots with the old one)
        /// </summary>
        /// <param name="cx">The context for user defined types etc</param>
        /// <param name="kt">The key type</param>
        /// <param name="b">The new top level (root) Bucket</param>
        protected CTree(Sqlx k,Bucket<K, V> b)
            : base(b)
        {
            kind = k;
        }
        /// <summary>
        /// Comparison of keys
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public override int Compare(K a, K b)
        {
            return (a == null || a.IsNull) ? ((b == null || b.IsNull) ? 0 : -1) : a._CompareTo(b);
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
                return new CTree<K, V>(kind,root.Update(this, k, v));
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
                return new CTree<K, V>(kind,new KeyValuePair<K, V>(k, v));
            if (root.count == Size)
                return new CTree<K, V>(kind,root.Split()).Add(k, v);
            return new CTree<K, V>(kind,root.Add(this, k, v));
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
            return new CTree<K, V>(kind,root.Update(this, k, v));
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
                return new CTree<K, V>(kind);
            // note: we allow root to have 1 entry
            return new CTree<K, V>(kind,root.Remove(this, k));
        }
        public static CTree<K, V> operator +(CTree<K, V> tree, (K, V) v)
        {
            return (CTree<K,V>)tree.Add(v.Item1, v.Item2);
        }
        public static CTree<K, V> operator -(CTree<K, V> tree, K k)
        {
            return (CTree<K,V>)tree.Remove(k);
        }
    }
    /// <summary>
    /// A generic strongly-typed Tree for values in the database.
    /// The tree's single-level key and value type are determined at creation.
    /// IMMUTABLE
    /// </summary>
    internal class SqlTree : CTree<TypedValue, TypedValue>
    {
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
            : base(k,b)
        {
            info = ti;
        }
        /// <summary>
        /// Constructor: a tree at a given depth with a given initial entry.
        /// </summary>
        /// <param name="ti">The treeinfo for the SqlTree</param>
        /// <param name="vType">the nominal value type</param>
        /// <param name="k">a key</param>
        /// <param name="v">a value</param>
        public SqlTree(TreeInfo ti, Sqlx kT,TypedValue k, TypedValue v)
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
            if (k != null && !t.info.headType.CanTakeValueOf(k.dataType))
                throw new DBException("22005M", t.info.headType.kind, k.ToString()).ISO()
                    .AddType(t.info.headType).AddValue(k);
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
    }
    internal class CList<K> : BList<K>,IComparable where K : IComparable
    {
        public new static readonly CList<K> Empty = new CList<K>();
        protected CList() : base() { }
        public CList(K k) : base(k) { }
        protected CList(Bucket<int, K> r) : base(r) { }
        protected override ATree<int, K> Add(int k, K v)
        {
            return new CList<K>(base.Add(k, v).root);
        }
        protected override ATree<int, K> Remove(int k)
        {
            return new CList<K>(base.Remove(k).root);
        }
        public static CList<K> operator +(CList<K> b, (int, K) x)
        {
            return (CList<K>)b.Add(x.Item1, x.Item2);
        }
        public static CList<K> operator +(CList<K> b, K k)
        {
            return (CList<K>)b.Add((int)b.Count, k);
        }
        public static CList<K> operator -(CList<K> b, int i)
        {
            return (CList<K>)b.Remove(i);
        }
        public int CompareTo(object obj)
        {
            var that = (CList<K>)obj;
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
    }
}
