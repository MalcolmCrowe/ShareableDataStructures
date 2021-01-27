using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level3
{

    /// <summary>
    /// See BTree for details 
    /// MTree is used for total and partial orderings where the value is long (e.g. in Index structure)
    /// for partial ordering the final stage Slot Row all implement BTree (and are ATree(long,bool))
    /// Logically a MTree contains associations of form (key,pos)
    /// For partial orders this is implemented as a tree of (key,T) where T contains (pos,true).
    /// All the implementation is done with SqlTrees.
    /// A null key is entered if permitted by allowNulls;
    /// of course in a multi-level MTree with allowNulls, any element of the key might be null.
    /// We detect partial ordering through the TreeBehaviour on onDuplicate.
    /// Immutable: No Mutators
    /// </summary>
    internal class MTree
    {
        /// <summary>
        /// The MTree is implemented using a SqlTree for the keys at this level.
        /// So info.nominalKeyType describes the TypedValue[], info.vType is always Int.
        /// And impl.nominalKeyType describes one component of the TypedValue[], impl.vType is MTree, Partial or Int.
        /// </summary>
        internal readonly SqlTree impl; // of type Domain.MImpl
        internal readonly TreeInfo info; // if info is null, so is SqlTree, and partial is not null
        /// <summary>
        /// The number of entries in the MTree
        /// </summary>
        internal readonly long count;
        /// <summary>
        /// Count gets the total number of Multikeys. 
        /// impl.Count gets the number of keys at this level
        /// </summary>
        public long Count { get { return count; } }
        /// <summary>
        /// Constructor: a new empty MTree for given TreeInfo info
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="ti">The Tree type info</param>
        internal MTree(TreeInfo ti)
        {
            info = ti;
            count = 0;
        }
        /// <summary>
        /// Constructor: an MTree with one given Slot
        /// </summary>
        /// <param name="ti">The MTree info</param>
        /// <param name="k">The key</param>
        /// <param name="v">The value</param>
        public MTree(TreeInfo ti, PRow k, long v)
        {
            info = ti;
            if (info.tail == null)
            {
                if (info.onDuplicate == TreeBehaviour.Allow)
                {
                    var x = new CTree<long, bool>(v, true);
                    impl = new SqlTree(info, Sqlx.T, k._head, new TPartial(x));
                }
                else
                    impl = new SqlTree(info, Sqlx.INT, k._head, new TInt(v));
            }
            else
            {
                var x = new MTree(info.tail, k._tail, v);
                impl = new SqlTree(info, Sqlx.M, k._head, new TMTree(x));
            }
            count = 1;
        }
        /// <summary>
        /// Constructor: implementation of add, update etc
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="i">Updated implementation tree</param>
        /// <param name="c">The new count</param>
        private MTree(TreeInfo ti,SqlTree i, long c) 
        {
            info = ti;
            impl = i;
            count = c;
        }
        /// <summary>
        /// A key for this index has a null at position cur: return a suitable new value for this null
        /// </summary>
        /// <param name="key"></param>
        /// <param name="off"></param>
        /// <param name="cur"></param>
        /// <returns></returns>
        internal TypedValue NextKey(BList<TypedValue> key, int off, int cur)
        {
            if (off < cur)
            {
                var mt = Ensure(key, off);
                return mt.NextKey(key, off + 1, cur);
            }
            var v = impl?.Last().key() ?? new TInt(0);
            if (v.dataType.kind==Sqlx.INTEGER)
                return new TInt(v.ToInt()+1);
            return info.headType.defaultValue;
        }
        /// <summary>
        /// Accessor: Look for given multi-column key
        /// </summary>
        /// <param name="k">A multikey</param>
        /// <returns>true iff the MTree contains the multikey</returns>
        public bool Contains(PRow k)
        {
            if (k == null) // happens if a short key is supplied
                return Count!=0;
            if (impl==null || !impl.Contains(k[0]))
                return false;
            var tv = impl[k[0]];
            if (tv.dataType.kind == Sqlx.M)
            {
                MTree mt = tv.Val() as MTree;
                return mt.Contains(k._tail);
            }
            return true;
        }
        /// <summary>
        /// The Add operation: add an association to the tree
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree target</param>
        /// <param name="k">The key</param>
        /// <param name="v">The value</param>
        /// <returns>Whether the element was added, if not, why not</returns>
        public static TreeBehaviour Add(ref MTree t, PRow k, long v)
        {
            if (k == null)
            {
                if (t.info.onNullKey != TreeBehaviour.Allow)
                    return t.info.onNullKey;
                k = new PRow(0);
            }
            if (t.Contains(k) && t.info.onDuplicate != TreeBehaviour.Allow)
                return t.info.onDuplicate;
            if (t.impl == null)
            {
                t = new MTree(t.info, k, v);
                return TreeBehaviour.Allow;
            }
            TypedValue nv = null; 
            SqlTree st = t.impl; // care: t is immutable
            if (st.Contains(k[0]))
            {
                TypedValue tv = st[k[0]];
                switch (tv.dataType.kind)
                {
                    case Sqlx.M:
                        {
                            MTree mt = tv.Val() as MTree;
                            mt +=(k._tail, v);
                            nv = new TMTree(mt); // care: immutable
                            break;
                        }
                    case Sqlx.T:
                        {
                            var bt = tv.Val() as CTree<long, bool>;
                            bt +=(v, true);
                            nv = new TPartial(bt); // care: immutable
                            break;
                        }
                    default:
                        throw new PEException("PE116");
                }
                SqlTree.Update(ref st, k[0], nv);
            }
            else
            {
                switch (t.impl.kind)
                {
                    case Sqlx.M:
                        {
                            TreeInfo ti = t.info.tail;
                            MTree mt = new MTree(ti, k._tail, v);
                            nv = new TMTree(mt);
                            break;
                        }
                    case Sqlx.T:
                        {
                            var bt = new CTree<long, bool>(v, true);
                            nv = new TPartial(bt);
                            break;
                        }
                    default:
                        if (t.info.onDuplicate == TreeBehaviour.Allow)
                            goto case Sqlx.T;
                        nv = new TInt(v);
                        break;
                }
                SqlTree.Add(ref st, k[0], nv);
            }
            t = new MTree(t.info, st, t.count + 1);
            return TreeBehaviour.Allow;
        }
        public static MTree operator+(MTree mt,(PRow,long) v)
        {
            if (Add(ref mt, v.Item1, v.Item2) != TreeBehaviour.Allow)
                throw new DBException("23000","duplicate key ",v.Item1);
            return mt;
        }
        public static MTree operator-(MTree mt,PRow k)
        {
            Remove(ref mt, k);
            return mt;
        }
        public static MTree operator -(MTree mt, (PRow,long) x)
        {
            var (k, p) = x;
            Remove(ref mt, k, p);
            return mt;
        }
        /// <summary>
        /// Traversal of trees is done by Bookmarks
        /// </summary>
        /// <returns>The bookmark for the first entry in the tree, or null if there are no entries</returns>
        internal MTreeBookmark First()
        {
            return MTreeBookmark.New(this);
        }
        internal MTreeBookmark Last()
        {
            return MTreeBookmark.Last(this);
        }
        /// <summary>
        /// Return the tree defined by the off-th key columns, or an empty one
        /// </summary>
        /// <param name="k">An array of key column values</param>
        /// <param name="off">An index into k[]</param>
        MTree Ensure(BList<TypedValue> k, int off)
        {
            if (impl != null && impl.Contains(k[off]))
            {
                TypedValue tv = impl[k[off]];
                if (tv.dataType.kind != Sqlx.M)
                    return null;
                return tv.Val() as MTree;
            }
            TreeInfo ti = info.tail;
            return new MTree(ti);
        }

        /// <summary>
        /// The update operation: change a long value for an existing key
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree</param>
        /// <param name="k">The key</param>
        /// <param name="v">The new long value</param>
        public static void Update(ref MTree t, PRow k, long v)
        {
            if (!t.Contains(k))
                throw new PEException("PE113");
            SqlTree st = t.impl; // care: t is immutable
            var k0 = k[0];
            TypedValue nv,tv = t.impl[k0];
            switch (tv.dataType.kind)
            {
                case Sqlx.M:
                    {
                        MTree mt = tv.Val() as MTree;
                        Update(ref mt, k._tail, v);
                        nv = new TMTree(mt);
                        break;
                    }
                default:
                    nv = new TInt(v);
                    break;
            }
            SqlTree.Update(ref st, k0, nv);
            t = new MTree(t.info, st, t.count);
        }
        /// <summary>
        /// The normal Remove operation: remove a key
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree target</param>
        /// <param name="k">The key to remove</param>
        public static void Remove(ref MTree t, PRow k)
        {
            if (!t.Contains(k))
                return;
            SqlTree st = t.impl; // care: t is immutable
            var k0 = k[0];
             TypedValue tv = t.impl[k0];
            long nc = t.count;
            switch (tv.dataType.kind)
            {
                case Sqlx.M:
                    {
                        MTree mt = tv.Val() as MTree;
                        long c = mt.Count;
                        Remove(ref mt, k._tail);
                        if (mt == null)
                        {
                            nc -= c; ;
                            SqlTree.Remove(ref st, k0);
                        }
                        else
                        {
                            nc -= c - mt.Count;
                            TypedValue nv = new TMTree(mt);
                            SqlTree.Update(ref st, k0, nv);
                        }
                        break;
                    }
                case Sqlx.T:
                    {
                        var bt = tv.Val() as CTree<long, bool>;
                        nc -= bt.Count;
                        SqlTree.Remove(ref st, k0);
                        break;
                    }
                default:
                    nc--;
                    SqlTree.Remove(ref st, k0);
                    break;
            }
            t = (st==null)?null:new MTree(t.info, st, nc);
        }
        /// <summary>
        /// The partial-ordering Remove: remove a particular association
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree</param>
        /// <param name="k">The key</param>
        /// <param name="v">The value to remove</param>
        public static void Remove(ref MTree t, PRow k, long v)
        {
            if (!t.Contains(k))
                return;
            SqlTree st = t.impl; // care: t is immutable
            var k0 = k[0];
            long nc = t.count;
             TypedValue nv, tv = t.impl[k0];
            switch (tv.dataType.kind)
            {
                case Sqlx.M:
                    {
                        MTree mt = tv.Val() as MTree;
                        long c = mt.Count;
                        Remove(ref mt, k._tail, v);
                        nc -= c - mt.Count;
                        if (mt.Count == 0)
                            SqlTree.Remove(ref st, k0);
                        else
                        {
                            nv = new TMTree(mt);
                            SqlTree.Update(ref st, k0, nv);
                        }
                        break;
                    }
                case Sqlx.T:
                    {
                        var bt = tv.Val() as CTree<long, bool>;
                        if (!bt.Contains(v))
                            return;
                        nc--;
                        bt -=v;
                        if (bt.Count == 0)
                            SqlTree.Remove(ref st, k0);
                        else
                        {
                            nv = new TPartial(bt);
                            SqlTree.Update(ref st, k0, nv);
                        }
                        break;
                    }
                default:
                    nc--;
                    SqlTree.Remove(ref st, k0, tv);
                    break;
            }
            t = new MTree(t.info, st, nc);
        }
        /// <summary>
        /// Get an ABookmark at the start of partial lookup. 
        /// </summary>
        /// <param name="m">A list of keys, guaranteed in the right order!</param>
        /// <returns> T:ATree(long,bool), M:MTree or else TInt</returns>
        public MTreeBookmark PositionAt(PRow m)
        {
            return MTreeBookmark.New(this, m);
        }
        public long? Get(PRow k)
        {
                var tv = impl[k._head];
                if (tv==null)
                    return null;
                switch (tv.dataType.kind)
                {
                    case Sqlx.M:
                        {
                            var mt = tv.Val() as MTree;
                            return mt.Get(k._tail);
                        }
                    case Sqlx.T:
                        {
                            var pt = tv.Val() as TPartial;
                            return pt.value.First().key();
                        }
                }
                return tv.Val() as long?;
        }
        internal MTree Fix(Context cx)
        {
            var r = new MTree(info.Fix(cx));
            for (var b = First(); b != null; b = b.Next())
            {
                var iq = b.Value();
                if (iq!=null)
                    r += (b.key().Fix(cx), b.Value().Value);
            }
            return r;
        }
        internal MTree Replaced(Context cx,DBObject so,DBObject sv)
        {
            return new MTree(info.Replaced(cx, so, sv), impl, count);
        }
        internal MTree Relocate(Writer wr)
        {
            var r = new MTree(info.Relocate(wr));
            for (var b = First(); b != null; b = b.Next())
            {
                var iq = b.Value();
                if (iq != null)
                    r += (wr.Fix(b.key()), b.Value().Value);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(" " + Count);
            var cm = " (";
            for (var a=info;a!=null;a=a.tail)
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(a.head));
                sb.Append(" ");
                sb.Append(a.headType);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// MTree traversal is done by MTreeBookmarks.
    /// mb = mt.First() bookmarks the first entry in mt, or null if there are no entries.
    /// mb = mb.Next() bookmarks the next entry after mb, or null if there are no more entries.
    /// All Bookmarks in an MTree have the same length; and the last (deepest) component of the
    /// bookmark may have a partial depending on allowDuplicates.
    /// The first element of the group of ties is bookmarked by the bookmark with pmk set to null.
    /// So in handling joins we can get back to the first in a group of ties by truncating the bookmark
    /// to the right length. This depth is given by the length of the join condition.
    /// Thus we detect the presence of ties by (a) an over-long bookmark and (b) a non-null pmk.
    /// </summary>
    internal sealed class MTreeBookmark 
    {
        readonly ABookmark<TypedValue, TypedValue> _outer;
        internal readonly TreeInfo _info;
        internal readonly MTreeBookmark _inner;
        readonly ABookmark<long, bool> _pmk;
        internal readonly bool _changed;
        internal readonly long _pos;
        internal readonly PRow _filter;
        MTreeBookmark(ABookmark<TypedValue,TypedValue> outer,TreeInfo info, bool changed, MTreeBookmark inner,
            ABookmark<long,bool> pmk,long pos,PRow filter = null)
        { 
            _outer = outer;
            _info = info;
            _changed = changed;
            _inner = inner;
            _pmk = pmk;
            _pos = pos;
            _filter = filter;
        }
        /// <summary>
        /// Implementation of mt.First
        /// </summary>
        /// <param name="mt">The tree whose first bookmark is required</param>
        /// <returns>An MTreeBookmark or null</returns>
        internal static MTreeBookmark New(MTree mt)
        {
            for (var outer = mt.impl?.First(); outer != null; outer = outer.Next())
            {
                var ov = outer.value();
                switch (ov.dataType.kind)
                {
                    case Sqlx.M:
                        var inner = (ov.Val() as MTree)?.First();
                        if (inner != null)
                            return new MTreeBookmark(outer, mt.info, false, inner, null,0);
                        break;
                    case Sqlx.T:
                        var pmk = (ov.Val() as CTree<long, bool>)?.First();
                        if (pmk != null)
                            return new MTreeBookmark(outer, mt.info, false, null, pmk,0);
                        break;
                    default:
                        return new MTreeBookmark(outer, mt.info, false, null, null,0);
                }
            }
            return null;
        }
        internal static MTreeBookmark Last(MTree mt)
        {
            for (var outer = mt.impl?.Last(); outer != null; outer = outer.Previous())
            {
                var ov = outer.value();
                switch (ov.dataType.kind)
                {
                    case Sqlx.M:
                        var inner = (ov.Val() as MTree)?.Last();
                        if (inner != null)
                            return new MTreeBookmark(outer, mt.info, false, inner, null, 0);
                        break;
                    case Sqlx.T:
                        var pmk = (ov.Val() as CTree<long, bool>)?.Last();
                        if (pmk != null)
                            return new MTreeBookmark(outer, mt.info, false, null, pmk, 0);
                        break;
                    default:
                        return new MTreeBookmark(outer, mt.info, false, null, null, 0);
                }
            }
            return null;
        }
        /// <summary>
        /// Gets a bookmark starting from a given key
        /// </summary>
        /// <param name="mt">The MTree</param>
        /// <param name="key">A key</param>
        /// <returns>A bookmark, or null if the key is not found</returns>
        internal static MTreeBookmark New(MTree mt,PRow key)
        {
            if (key == null)
                return New(mt);
            var outer = mt.impl?.PositionAt(key._head);
            if (outer == null)
                return null;
            MTreeBookmark inner = null;
            ABookmark<long, bool> pmk = null;
            for (; ; )
            {
                if (inner!=null)
                {
                    inner = inner.Next();
                    if (inner != null)
                        goto done;
                }
                if (pmk!=null)
                {
                    pmk = pmk.Next();
                    if (pmk != null)
                        goto done;
                }
                var tv = outer.value();
                switch (tv.dataType.kind)
                {
                    case Sqlx.M:
                        inner = (tv.Val() as MTree).PositionAt(key._tail);
                        if (inner != null)
                            goto done;
                        outer = outer.Next();
                        if (outer == null)
                            return null;
                        continue;
                    case Sqlx.T:
                        pmk = (tv.Val() as CTree<long, bool>).First();
                        if (pmk != null && key._tail == null)
                            goto done;
                        continue;
                    default:
                        if (key._tail == null)
                            goto done;
                        return null;
                }
            }
            done:
            return new MTreeBookmark(outer, mt.info, true, inner, pmk, 0, key);
        }
        /// <summary>
        /// The key at this bookmark
        /// </summary>
        /// <returns>The key at this bookmark</returns>
        public PRow key()
        {
            if (_outer == null)
                return null;
            return new PRow(_outer.key(), _inner?.key());
        }
        /// <summary>
        /// The value at this bookmark
        /// </summary>
        /// <returns>The value at this bookmark</returns>
        public long? Value()
        {
            if (_inner != null)
                return _inner.Value();
            if (_pmk != null)
                return _pmk.key();
            if (_outer?.value() != null)
                return _outer.value().ToLong();
            return 0L;
        }
        /// <summary>
        /// The bookmark for the next entry in the MTree, or null if no such entry exists
        /// </summary>
        /// <returns>The next bookmark or null</returns>
        public MTreeBookmark Next()
        {
            var inner = _inner;
            var outer = _outer;
            var pmk = _pmk;
            var pos = _pos;
            var changed = false;
            for (;;)
            {
                if (inner != null)
                {
                    inner = inner.Next();
                    if (inner != null)
                        goto done;
                }
                if (pmk != null)
                {
                    pmk = pmk.Next();
                    if (pmk != null)
                        goto done;
                }
                var h = _filter?._head;
                if (h!= null && !h.IsNull)
                    return null;
                outer = ABookmark<TypedValue, TypedValue>.Next(outer);
                if (outer == null)
                    return null;
                changed = true;
                var oval = outer.value();
                switch (oval.dataType.kind)
                {
                    case Sqlx.M:
                        inner = ((MTree)oval.Val()).PositionAt(_filter?._tail);
                        if (inner != null)
                            goto done;
                        break;
                    case Sqlx.T:
                        pmk = ((CTree<long, bool>)oval.Val()).First();
                        if (pmk != null)
                            goto done;
                        break;
                    default:
                        goto done;
                }
            }
            done: 
            return new MTreeBookmark(outer, _info, changed, inner, pmk, pos+1,_filter);
        }
        public MTreeBookmark Previous()
        {
            var inner = _inner;
            var outer = _outer;
            var pmk = _pmk;
            var pos = _pos;
            var changed = false;
            for (; ; )
            {
                if (inner != null)
                {
                    inner = inner.Previous();
                    if (inner != null)
                        goto done;
                }
                if (pmk != null)
                {
                    pmk = pmk.Previous();
                    if (pmk != null)
                        goto done;
                }
                var h = _filter?._head;
                if (h != null && !h.IsNull)
                    return null;
                outer = ABookmark<TypedValue, TypedValue>.Previous(outer);
                if (outer == null)
                    return null;
                changed = true;
                var oval = outer.value();
                switch (oval.dataType.kind)
                {
                    case Sqlx.M:
                        inner = ((MTree)oval.Val()).PositionAt(_filter?._tail);
                        if (inner != null)
                            goto done;
                        break;
                    case Sqlx.T:
                        pmk = ((CTree<long, bool>)oval.Val()).Last();
                        if (pmk != null)
                            goto done;
                        break;
                    default:
                        goto done;
                }
            }
        done:
            return new MTreeBookmark(outer, _info, changed, inner, pmk, pos + 1, _filter);
        }
        /// <summary>
        /// The position of the bookmark in the tree
        /// </summary>
        /// <returns>The position in the tree (starting at 0)</returns>
        public long Position()
        {
            return _pos;
        }
        /// <summary>
        /// In join processing if there are ties in both first and second we
        /// often need to repeat groups of tied rows.
        /// </summary>
        /// <param name="depth"></param>
        /// <returns>an earlier bookmark or null</returns>
        internal MTreeBookmark ResetToTiesStart(int depth)
        {
            var m = (depth>1)?_inner?.ResetToTiesStart(depth - 1):null;
            var ov = (depth==1)?_outer.value().Val() as BTree<long,bool>:null;
            return new MTreeBookmark(_outer, _info, false,
                    m, ov?.First(), _inner?._pos??0);
        }
        /// <summary>
        /// Find out if there are more matches for a partial ordering
        /// </summary>
        /// <param name="depth">The depth in the key</param>
        /// <returns>whether there are more matches</returns>
        internal bool hasMore(int depth)
        {
            if (depth > 1)
                return _pmk?.Next()!=null || (_inner!=null && _inner.hasMore(depth - 1));
            var ov = _outer.value();
            switch(ov.dataType.kind)
            {
                case Sqlx.M:
                    {
                        var m = ov.Val() as MTree;
                        return _inner._pos < m.count-1;
                    }
                case Sqlx.T:
                    {
                        var t = ov.Val() as CTree<long, bool>;
                        return _pmk.position() < t.Count -1;
                    }
                default:
                    return false;
            }
        }
        /// <summary>
        /// Whether there has been a change at the given depth
        /// </summary>
        /// <param name="depth">the depth in the key</param>
        /// <returns>whether there has been a change</returns>
        internal bool changed(int depth)
        {
            if (_changed)
                return true;
            if (depth > 1)
                return _inner.changed(depth - 1);
            return false;
        }
        public override string ToString()
        {
            return key().ToString() + "," + Value();
        }
    }
    /// <summary>
    /// TreeBehaviour deals with duplicates and nulls behaviour in SQL arrays and multisets
    /// </summary>
    internal enum TreeBehaviour { Ignore, Allow, Disallow };
    /// <summary>
    /// TreeInfo is used for handling complex SQL result types (for MTree, RTree)
    /// </summary>
    internal class TreeInfo
    {
        internal readonly long head;
        internal readonly Domain headType;
        internal readonly TreeBehaviour onDuplicate, onNullKey; // onDuplicate effective only if tail is null
        internal readonly TreeInfo tail;
        TreeInfo(long h,Domain dm,TreeBehaviour d,TreeBehaviour n,TreeInfo t)
        {
            head = h; headType = dm; onDuplicate = d; onNullKey = n; tail = t;
        }
        /// <summary>
        /// Set up Tree information for a simple result set
        /// </summary>
        /// <param name="multi">The multi-column key type</param>
        /// <param name="d">Whether duplicates are allowed at this level</param>
        /// <param name="n">Whether nulls are allowed at this level</param>
        /// <param name="off">the offset of the current level in multi</param>
        internal TreeInfo(CList<SqlValue> cols,TreeBehaviour d, TreeBehaviour n, int off=0)
        {
            if (off < cols.Length)
            {
                var v = cols[off];
                (head, headType) = (v.defpos,v.domain);
            }
            onDuplicate = d;
            onNullKey = n;
            tail = (off+1 < cols.Length)?new TreeInfo(cols, d,n, off+1):null;
        }
        internal TreeInfo(CList<long> cols, BTree<long,DBObject> ds, TreeBehaviour d, TreeBehaviour n, int off = 0)
        {
            if (off < (int)cols.Count)
            {
                head = cols[off];
                headType = ds[head].domain;
            }
            onDuplicate = d;
            onNullKey = n;
            tail = (off + 1 < (int)cols.Count) ? new TreeInfo(cols, ds, d, n, off + 1) : null;
        }
        internal TreeInfo(CList<long> cols, Domain dt, TreeBehaviour d, TreeBehaviour n, int off = 0)
        {
            if (off < (int)cols.Count)
            {
                head = cols[off];
                headType = dt.representation[head];
            }
            onDuplicate = d;
            onNullKey = n;
            tail = (off + 1 < (int)cols.Count) ? new TreeInfo(cols, dt, d, n, off + 1) : null;
        }
        internal TreeInfo Fix(Context cx)
        {
            return new TreeInfo(cx.obuids[head]??head, (Domain)headType.Fix(cx), 
                onDuplicate, onNullKey, tail?.Fix(cx));
        }
        internal TreeInfo Replaced(Context cx)
        {
            return new TreeInfo(cx.done[head]?.defpos ?? head, (Domain)headType.Fix(cx),
                onDuplicate, onNullKey, tail?.Replaced(cx));
        }
        internal TreeInfo Relocate(Level2.Writer wr)
        {
            return new TreeInfo(wr.Fix(head), (Domain)headType._Relocate(wr),
                onDuplicate, onNullKey, tail?.Relocate(wr));
        }
        internal TreeInfo Replaced(Context cx,DBObject so,DBObject sv)
        {
            return new TreeInfo(cx.done[head]?.defpos ?? head, 
                (Domain)headType._Replace(cx, so, sv),
                onDuplicate, onNullKey, tail?.Replaced(cx, so, sv));
        }
    }

}

