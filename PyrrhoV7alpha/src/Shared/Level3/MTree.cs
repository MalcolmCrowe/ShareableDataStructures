using System.Text;
using Pyrrho.Common;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{

    /// <summary>
    /// See BTree for details 
    /// MTree is used for total and partial orderings where the value is long (e.g. in Index structure)
    /// for partial ordering the final stage Slot Row all implement BTree (and are ATree(long,bool))
    /// Logically a MTree contains associations of form (key,pos)
    /// Show partial orders this is implemented as a tree of (key,T) where T contains (pos,true).
    /// All the implementation is done with SqlTrees or STree (STree allows set-valued keys).
    /// A null key is entered if permitted by allowNulls;
    /// of course in a multi-level MTree with allowNulls, any element of the key might be null.
    /// We detect partial ordering through the TreeBehaviour on onDuplicate.
    /// Immutable: No Mutators
    ///     
    /// </summary>
    internal class MTree
    {
        /// <summary>
        /// The MTree is implemented using a SqlTree for the keys at this level.
        /// Thus impl.keyType is always info[keyLen-1] to implement an index at this level.
        /// </summary>
        internal readonly SqlTree? impl; 
        internal readonly Domain info; // same info for all levels!
        internal readonly TreeBehaviour nullsAndDuplicates; 
        internal readonly int off;
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
        /// <param name="ks">The key Domain info</param>
        /// <param name="ln">The portion of the key implemented by this MTree</param>
        internal MTree(Domain ks,TreeBehaviour nd,int ff)
        {
            count = 0;
            info = ks;
            off = ff;
            nullsAndDuplicates = nd;
        }
        /// <summary>
        /// Constructor: an MTree with one given Slot
        /// </summary>
        /// <param name="ti">The MTree info</param>
        /// <param name="fk">Whether the index is for a foreign key (allows duplicates)</param>
        /// <param name="k">The key (length<=ti.Length)</param>
        /// <param name="off">A position in the key (starts at 0)</param>
        /// <param name="v">The value (picks a row in something)</param>
        public MTree(Domain ti, TreeBehaviour fk, CList<TypedValue> k, int ff, long v)
        {
            info = ti;
            off = ff;
            nullsAndDuplicates = fk;
            if (!ti.rowType.Contains(ff) || ti.representation[ti.rowType[ff]??-1L] is not Domain sd)
                throw new PEException("PE6001");
            if (k[ff] is not TypedValue head)
                throw new PEException("PE6002");
            if (ff==ti.Length-1 && k!=CList<TypedValue>.Empty)
            {
                if (fk==TreeBehaviour.Allow)
                {
                    var x = new CTree<long, bool>(v, true);
                    impl = new SqlTree(sd, Qlx.T, head, new TPartial(x));
                }
                else
                    impl = new SqlTree(sd, Qlx.INT, head, new TInt(v));
            }
            else
            {
                var x = new MTree(ti, fk, k, ff+1, v);
                impl = new SqlTree(sd, Qlx.M, head, new TMTree(x));
            }
            count = 1;
        }
        /// <summary>
        /// Constructor: implementation of add, update etc
        /// </summary>
        /// <param name="i">Updated implementation tree</param>
        /// <param name="c">The new count</param>
        private MTree(MTree mt,SqlTree i, long c) 
        {
            info = mt.info;
            nullsAndDuplicates = mt.nullsAndDuplicates;
            off = mt.off;
            impl = i;
            count = c;
        }
        internal int Cardinality(CList<TypedValue>? filt=null,int off=0)
        {
            if (count == 0L)
                return 0;
            if (info == null)
                return (int)count;
            if (nullsAndDuplicates==TreeBehaviour.Disallow)
                return (int)count;
            if (filt==null)
            {
                filt = CList<TypedValue>.Empty;
                for (var b = info.rowType.First(); b != null; b = b.Next())
                    filt += TNull.Value;
            }
            if (filt[off] is TypedValue v && v is not TNull)
            {
                if (impl?[v] is TypedValue t)
                    return t.dataType.kind switch
                    {
                        Qlx.T => (int)((TPartial)t).value.Count,
                        Qlx.INT => (int)((TInt)t).value,
                        Qlx.M => ((TMTree)t).value.Cardinality(filt,off+1),
                        _ => 1,
                    };
                else
                    return 0;
            }
            var r = 0;
            for (var b = impl?.First(); b != null; b = b.Next())
            {
                var t = b.value();
                switch (t.dataType.kind)
                {
                    case Qlx.T:
                        r += (int)((TPartial)t).value.Count;
                        break;
                    case Qlx.INT:
                        r += (int)((TInt)t).value;
                        break;
                    case Qlx.M:
                        r += ((TMTree)t).value.Cardinality(filt,off+1);
                        break;
                    default:
                        r++;
                        break;
                }
            }
            return r;
        }
        /// <summary>
        /// A key for this index has a null at position cur: return a suitable new value for this null
        /// </summary>
        /// <param name="key"></param>
        /// <param name="ff">position in key</param>
        /// <param name="cur"></param>
        /// <returns></returns>
        internal TypedValue NextKey(Qlx kind, CList<TypedValue> key, int ff, int cur)
        {
            if (off < cur && Ensure(key,ff) is MTree mt) 
                return mt.NextKey(kind,key, ff + 1, cur);
            return impl?.AutoKey(kind) ?? ((kind == Qlx.CHAR) ? 
                new TChar("1") : new TInt(1));
        }
        /// <summary>
        /// Return the tree defined by the off-th key columns, or an empty one
        /// </summary>
        /// <param name="k">A tree of key column values</param>
        /// <param name="off">An index into k</param>
        MTree Ensure(CList<TypedValue> k, int off)
        {
            return (k[off] is TypedValue v && v is not TNull && impl?[v] is TMTree tm)?
                    tm.value : new MTree(info,nullsAndDuplicates,off);
        }
        /// <summary>
        /// Accessor: Look for given multi-column key
        /// </summary>
        /// <param name="k">A multikey</param>
        /// <returns>true iff the MTree contains the multikey</returns>
        public bool Contains(CList<TypedValue> k,int ff=0)
        {
            if (k[ff] is not TypedValue h) // happens if a short key is supplied
                return count!=0;
            if (h is TSet ts)
            {
                for (var b = ts._First(); b != null; b = b.Next())
                    if (b.Value() is TypedValue e && !Contains(k-ff+(ff,e), ff))
                        return false;
                return true;
            }
            if (impl == null || !impl.Contains(h))
                return false;
            var tv = impl[h];
            if (tv is not null && tv is TMTree tm && tm.value is MTree mt)
                return mt.Contains(k,ff+1);
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
        public static TreeBehaviour Add(ref MTree t, CList<TypedValue> k, int off, long v)
        {
            if (t.info == null)
                return TreeBehaviour.Disallow;
            var head = k[off] ?? TNull.Value;
            if (head is TNull)
                return (t.nullsAndDuplicates==TreeBehaviour.Allow 
                    || t.nullsAndDuplicates == TreeBehaviour.Ignore)?
                    TreeBehaviour.Allow:  TreeBehaviour.Disallow;
            if (t.impl == null)
            {
                t = new MTree(t.info, t.nullsAndDuplicates, k, off, v);
                return TreeBehaviour.Allow;
            }
            TypedValue nv;
            var st = t.impl; // care: t is immutable
            if (st[head] is TypedValue tv)
            {
                switch (tv.dataType.kind)
                {
                    case Qlx.M:
                        {
                            if (tv is not TMTree tm)
                                throw new PEException("PE4500");
                            var mt = tm.value;
                            mt += (k, off+1, v);
                            nv = new TMTree(mt); // care: immutable
                            break;
                        }
                    case Qlx.T:
                        {
                            if (tv is not TPartial tp)
                                throw new PEException("PE4501");
                            var bt = tp.value;
                            bt += (v, true);
                            nv = new TPartial(bt); // care: immutable
                            break;
                        }
                    default:
                        return TreeBehaviour.Allow; // throw new PEException("PE116");
                }
                SqlTree.Update(ref st, head, nv);
            }
            else
            {
                switch (t.impl.kind)
                {
                    case Qlx.M:
                        {
                            MTree mt = new (t.info, t.nullsAndDuplicates, k, off+1, v);
                            nv = new TMTree(mt);
                            break;
                        }
                    case Qlx.T:
                        {
                            var bt = new CTree<long, bool>(v, true);
                            nv = new TPartial(bt);
                            break;
                        }
                    default:
                        if (t.nullsAndDuplicates==TreeBehaviour.Allow)
                            goto case Qlx.T;
                        if (t.nullsAndDuplicates == TreeBehaviour.Ignore && t.Contains(k,off))
                                return TreeBehaviour.Allow;
                        nv = new TInt(v);
                        break;
                }
                SqlTree.Add(ref st, head, nv);
            }
            t = new MTree(t, st, t.count + 1);
            return TreeBehaviour.Allow;
        }
        public static MTree operator+(MTree mt,(CList<TypedValue>,int,long) x)
        {
            var (k, off, v) = x;
            var a = Add(ref mt, k, off, v);
            if (a != TreeBehaviour.Allow)
                throw new DBException("23000","duplicate key ",k);
            return mt;
        }
        public static MTree? operator-(MTree? mt,CList<TypedValue> k)
        {
            Remove(ref mt, k, 0);
            return mt;
        }
        public static MTree operator -(MTree mt, (CList<TypedValue>,int,long) x)
        {
            var (k, off, p) = x;
            Remove(ref mt, k, off, p);
            return mt;
        }
        /// <summary>
        /// Traversal of trees is done by Bookmarks
        /// </summary>
        /// <returns>The bookmark for the first entry in the tree, or null if there are no entries</returns>
        internal MTreeBookmark? First()
        {
            return MTreeBookmark.New(this);
        }
        internal MTreeBookmark? Last()
        {
            return MTreeBookmark.Last(this);
        }
        /// <summary>
        /// The update operation: change a long value for an existing key
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree</param>
        /// <param name="k">The key</param>
        /// <param name="v">The new long value</param>
        public static void Update(ref MTree t, CList<TypedValue> k, int off, long v)
        {
            if (!t.Contains(k,off) || t.impl==null || t.info==null)
                throw new PEException("PE113");
            SqlTree st = t.impl; // care: t is immutable
            if (k[off] is not TypedValue head)
                throw new PEException("PE6008");
            TypedValue nv, tv = t.impl[head] ?? throw new PEException("PE6009");
            switch (tv.dataType.kind)
            {
                case Qlx.M:
                    {
                        if (tv is not TMTree tm)
                            throw new PEException("PE4505");
                        var mt = tm.value;
                        Update(ref mt, k, off+1, v);
                        nv = new TMTree(mt);
                        break;
                    }
                default:
                    nv = new TInt(v);
                    break;
            }
            SqlTree.Update(ref st, head, nv);
            t = new MTree(t, st, t.count);
        }
        /// <summary>
        /// The normal Remove operation: remove a key
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree target</param>
        /// <param name="k">The key to remove</param>
        public static void Remove(ref MTree? t, CList<TypedValue> k, int off)
        {
            if (t==null || !t.Contains(k) || t.impl==null)// care: t is immutable
                return;
            var st = t.impl;
            if (k[off] is not TypedValue head)
                throw new PEException("PE6010");
            if (t.impl[head] is not TypedValue tv)
                return;
            long nc = t.count;
            switch (tv.dataType.kind)
            {
                case Qlx.M:
                    {
                        if (tv is not TMTree tm)
                            throw new PEException("PE4001");
                        long c = tm.value.Count;
                        MTree? mt = tm.value;
                        Remove(ref mt, k, off+1);
                        if (mt == null)
                        {
                            nc -= c; ;
                            st = (SqlTree)st.Remove(head);
                        }
                        else
                        {
                            nc -= c - mt.Count;
                            TypedValue nv = new TMTree(mt);
                            SqlTree.Update(ref st, head, nv);
                        }
                        break;
                    }
                case Qlx.T:
                    {
                        if (tv is not TPartial tp)
                            throw new PEException("PE4002");
                        st = t.impl;
                        nc -= tp.value.Count;
                        st = (SqlTree)st.Remove(head);
                        break;
                    }
                default:
                    nc--;
                    st = (SqlTree)st.Remove(head);
                    break;
            }
            if (t is not null && t.info is not null)
                t = (st==null)?null:new MTree(t, st, nc);
        }
        /// <summary>
        /// The partial-ordering Remove: remove a particular association
        /// </summary>
        /// <param name="cx">The context for user-defined types</param>
        /// <param name="t">The tree</param>
        /// <param name="k">The key</param>
        /// <param name="v">The value to remove</param>
        public static void Remove(ref MTree t, CList<TypedValue> k, int off, long v)
        {
            if (t==null || t.impl==null || k[off] is not TypedValue head 
                || t.impl[head] is not TypedValue tv || t.info==null)
                return;
            var st = t.impl; // care: t is immutable
            long nc = t.count;
            TypedValue nv;
            switch (tv.dataType.kind)
            {
                case Qlx.M:
                    {
                        if (tv is not TMTree tm)
                            throw new PEException("PE4005");
                        var mt = tm.value;
                        long c = mt.Count;
                        MTree? nt = mt;
                        Remove(ref nt, k, off+1, v);
                        if (nt == null || nt.Count == 0)
                        {
                            nc -= mt.Count;
                            st = (SqlTree)st.Remove(head);
                        }
                        else
                        {
                            nc -= c - mt.Count;
                            nv = new TMTree(mt);
                            SqlTree.Update(ref st, head, nv);
                        }
                        break;
                    }
                case Qlx.T:
                    {
                        if (tv is not TPartial tp)
                            throw new PEException("PE4006");
                        var bt = tp.value;
                        if (!bt.Contains(v))
                            return;
                        nc--;
                        bt -=v;
                        if (bt.Count == 0)
                            st = (SqlTree)st.Remove(head);
                        else
                        {
                            nv = new TPartial(bt);
                            SqlTree.Update(ref st, head, nv);
                        }
                        break;
                    }
                default:
                    nc--;
                    SqlTree.Remove(ref st,head);
                    break;
            }
            t = new MTree(t, st, nc);
        }
        /// <summary>
        /// Get a ABookmark at the start of partial lookup. 
        /// </summary>
        /// <param name="m">A tree of keys, guaranteed in the right order!</param>
        /// <returns> T:ATree(long,bool), M:MTree or else TInt</returns>
        public MTreeBookmark? PositionAt(CList<TypedValue> m,int ff)
        {
            return MTreeBookmark.New(this, m, ff);
        }
        public long? Get(CList<TypedValue> k,int ff)
        {
            if (impl == null || k[ff] is not TypedValue head)
                return null;
            var tv = impl[head];
            if (tv == null)
                return null;
            switch (tv.dataType.kind)
            {
                case Qlx.M:
                    {
                        if (tv is TMTree tm)
                            return tm.value.Get(k,ff+1);
                        return null;
                    }
                case Qlx.T:
                    {
                        if (tv is TPartial pt)
                            return pt.value.First()?.key();
                        return null;
                    }
            }
            return (tv as TInt)?.value;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(" " + Count);
            sb.Append('@'); sb.Append(off);
            sb.Append(info);
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
    /// 
    /// </summary>
    internal sealed class MTreeBookmark 
    {
        readonly MTree _mt;
        readonly ABookmark<TypedValue, TypedValue> _outer;
        internal readonly Domain _info;
        internal readonly MTreeBookmark? _inner;
        readonly ABookmark<long, bool>? _pmk;
        internal readonly bool _changed;
        internal readonly long _pos;
        internal readonly CList<TypedValue> _key;
        MTreeBookmark(MTree mt,ABookmark<TypedValue,TypedValue> outer,Domain info, bool changed, 
            MTreeBookmark? inner,
            ABookmark<long,bool>? pmk,long pos,CList<TypedValue>? key = null)
        {
            _mt = mt;
            _outer = outer;
            _info = info;
            _changed = changed;
            _inner = inner;
            _pmk = pmk;
            _pos = pos;
            _key = key??CList<TypedValue>.Empty;
        }
        /// <summary>
        /// Implementation of mt.First
        /// </summary>
        /// <param name="mt">The tree whose first bookmark is required</param>
        /// <returns>An MTreeBookmark or null</returns>
        internal static MTreeBookmark? New(MTree mt)
        {
            if (mt.info == null)
                throw new PEException("PE4020");
            for (var outer = mt.impl?.First(); outer != null; outer = outer.Next())
            {
                var ov = outer.value();
                switch (ov.dataType.kind)
                {
                    case Qlx.M:
                        var inner = (ov as TMTree)?.value.First();
                        if (inner != null)
                            return new MTreeBookmark(mt,outer, mt.info, false, inner, null,0);
                        break;
                    case Qlx.T:
                        var pmk = (ov as TPartial)?.value.First();
                        if (pmk != null)
                            return new MTreeBookmark(mt,outer, mt.info, false, null, pmk,0);
                        break;
                    default:
                        return new MTreeBookmark(mt,outer, mt.info, false, null, null,0);
                }
            }
            return null;
        }
        internal static MTreeBookmark? Last(MTree mt)
        {
            if (mt.info==null)
                throw new PEException("PE4019");
            for (var outer = mt.impl?.Last(); outer != null; outer = outer.Previous())
            {
                var ov = outer.value();
                switch (ov.dataType.kind)
                {
                    case Qlx.M:
                        var inner = (ov as TMTree)?.value.Last();
                        if (inner != null)
                            return new MTreeBookmark(mt, outer, mt.info, false, inner, null, 0);
                        break;
                    case Qlx.T:
                        var pmk = (ov as TPartial)?.value.Last();
                        if (pmk != null)
                            return new MTreeBookmark(mt, outer, mt.info, false, null, pmk, 0);
                        break;
                    default:
                        return new MTreeBookmark(mt, outer, mt.info, false, null, null, 0);
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
        internal static MTreeBookmark? New(MTree mt,CList<TypedValue> key,int off)
        {
            if (mt.info == null)
                throw new PEException("PE4050");
            if (off>=key.Length)
                return New(mt);
            if (key[off] is not TypedValue head)
                return null;
            var outer = mt.impl?.PositionAt(head);
            if (outer == null)
                return null;
            MTreeBookmark? inner = null;
            ABookmark<long, bool>? pmk = null;
             for (; ; )
            {
                if (inner is not null)
                {
                    inner = inner.Next();
                    if (inner != null)
                        goto done;
                }
                if (pmk is not null)
                {
                    pmk = pmk.Next();
                    if (pmk != null)
                        goto done;
                }
                if (outer.value() is TypedValue tv)
                switch (tv.dataType.kind)
                {
                    case Qlx.M:
                        inner = (tv as TMTree)?.value.PositionAt(key,off+1);
                        if (inner != null)
                            goto done;
                        outer = outer.Next();
                        if (outer == null)
                            return null;
                        continue;
                    case Qlx.T:
                        pmk = (tv as TPartial)?.value.First();
                        if (pmk != null)
                            goto done;
                        continue;
                    default:
                            goto done;
                }
                return null;
            }
            done:
            return new MTreeBookmark(mt, outer, mt.info, true, inner, pmk, 0, key);
        }
        /// <summary>
        /// The key at this bookmark
        /// </summary>
        /// <returns>The key at this bookmark</returns>
        public CList<TypedValue> key()
        {
            if (_outer == null)
                throw new PEException("PE6012");
            var r = new CList<TypedValue>(_outer.key());
            if (_inner?.key() is CList<TypedValue> ik)
                r += ik;
            return r;
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
        public MTreeBookmark? Next()
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
                outer = ABookmark<TypedValue, TypedValue>.Next(outer);
                if (outer == null)
                    return null;
                changed = true;
                var oval = outer.value();
                if (oval == null)
                    return null;
                switch (oval.dataType.kind)
                {
                    case Qlx.M:
                        inner = ((TMTree)oval).value?.PositionAt(_key,_mt.off+1);
                        if (inner != null)
                            goto done;
                        break;
                    case Qlx.T:
                        pmk = ((TPartial)oval).value?.First();
                        if (pmk != null)
                            goto done;
                        break;
                    default:
                        goto done;
                }
            }
            done: 
            return new MTreeBookmark(_mt, outer, _info, changed, inner, pmk, pos+1,_key);
        }
        public MTreeBookmark? Previous()
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
                var h = _key[_mt.off]??TNull.Value;
                if (h !=TNull.Value)
                    return null;
                outer = ABookmark<TypedValue, TypedValue>.Previous(outer);
                if (outer == null)
                    return null;
                changed = true;
                var oval = outer.value();
                switch (oval.dataType.kind)
                {
                    case Qlx.M:
                        inner = ((TMTree)oval).value?.PositionAt(_key,_mt.off+1);
                        if (inner != null)
                            goto done;
                        break;
                    case Qlx.T:
                        pmk = ((TPartial)oval).value?.Last();
                        if (pmk != null)
                            goto done;
                        break;
                    default:
                        goto done;
                }
            }
        done:
            return new MTreeBookmark(_mt, outer, _info, changed, inner, pmk, pos + 1, _key);
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
            var ov = (depth==1)?(_outer.value()as TPartial)?.value :null;
            return new MTreeBookmark(_mt, _outer, _info, false,
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
                return _pmk?.Next() is not null || (_inner is not null && _inner.hasMore(depth - 1));
            var ov = _outer.value();
            switch(ov.dataType.kind)
            {
                case Qlx.M:
                    {
                        if (ov is not TMTree tm || _inner == null)
                            throw new PEException("PE4040");
                        return _inner._pos < tm.value.count-1;
                    }
                case Qlx.T:
                    {
                        if (ov is not TPartial t || _pmk == null)
                            throw new PEException("PE4041");
                        return _pmk.position() < t.value.Count -1;
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
            if (depth > 1 && _inner is not null)
                return _inner.changed(depth - 1);
            return false;
        }
        public override string ToString()
        {
            return (key()?.ToString()??"_") + "," + Value();
        }
    }
    /// <summary>
    /// TreeBehaviour deals with duplicates and nulls behaviour in SQL arrays and multisets
    /// </summary>
    internal enum TreeBehaviour { Ignore, Allow, Disallow };

}

