using System;
using System.Configuration;
using System.Security.Cryptography;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// See BTree for details 
    /// RTree is used for total and partial orderings where the value is of type RRow
    /// where an RRow is a snaphot of the Context's Cursors (rationale below)
    /// Logically an RTree contains associations of form (TRow,RRow)
    /// RTree uses MTree for implementation.
    /// IMMUTABLE
    /// </summary>
    internal class RTree
    {
        internal readonly long defpos; // RowSet
        internal Domain keyType;
        internal readonly MTree mt;
        /// <summary>
        /// rows is a set of snapshots of cx.cursors, taken during Build.
        /// We do this so that we can implement updatable joins:
        /// it allows us to accommodate sorted operands of the join.
        /// In previous versions it was just the sorted BList of the rows,
        /// to get the current row, simply subscript by defpos. 
        /// </summary>
        internal readonly BList<BTree<long,Cursor>> rows; // RowSet
        /// <summary>
        /// Constructor: a new empty MTree for given TreeSpec
        /// </summary>
        internal RTree(long dp,Context cx,Domain dt,
            TreeBehaviour d=TreeBehaviour.Disallow,TreeBehaviour n=TreeBehaviour.Allow)
        {
            defpos = dp;
            keyType = dt;
            mt = new MTree(keyType,d,0);
            rows = BList<BTree<long, Cursor>>.Empty;
        }
        protected RTree(RTree t, MTree m,BList<BTree<long,Cursor>> rs)
        {
            defpos = t.defpos;
            keyType = t.keyType;
            mt = m;
            rows = rs;
        }
        public static TreeBehaviour Add(ref RTree t, TRow k, BTree<long,Cursor> v)
        {
            var m = t.mt;
            TreeBehaviour tb = MTree.Add(ref m, 
                k.ToKey(),0, t.rows.Count);
            if (tb == TreeBehaviour.Allow)
                t = new RTree(t, m, t.rows + v);
            return tb;
        }
        public static RTree operator+(RTree t,(TRow,BTree<long,Cursor>)x)
        {
            var (k, v) = x;
            Add(ref t, k, v);
            return t??throw new PEException("PE6013");
        }
        public RTreeBookmark? First(Context cx)
        {
            return RTreeBookmark.New(cx,this);
        }
        public RTreeBookmark? Last(Context cx)
        {
            return RTreeBookmark.New(this, cx);
        }
        public RTreeBookmark? PositionAt(Context cx,CList<TypedValue> key, int ff=0)
        {
            return RTreeBookmark.New(cx, this, key, ff);
        }
    }
    
    internal class RTreeBookmark : Cursor
    {
        internal readonly RTree _rt;
        internal readonly MTreeBookmark _mb;
        internal readonly BTree<long, Cursor> _cs; // for updatable joins
        internal readonly CList<TypedValue> _key;
        RTreeBookmark(Context cx,RTree rt,int pos,MTreeBookmark mb,
            BTree<long,Cursor> cs, Cursor rr)
            :base(cx,rt.defpos,rt.keyType,pos,rr._ds,rr)
        { 
            _rt = rt; _mb = mb; _cs = cs;
            _key = mb._key;
        }
        RTreeBookmark(Context cx, RTree rt, int pos, MTreeBookmark mb,
            BTree<long, Cursor> cs)
            : this(cx, rt, pos, mb, cs, cs[rt.defpos]??throw new PEException("PE48177"))
        { }
        RTreeBookmark(Context cx,RTree rt, int pos, MTreeBookmark mb) 
            :this(cx,rt,pos,mb,rt.rows[(int)(mb.Value()??-1L)]?? throw new PEException("PE48178"))
        { }
        public override MTreeBookmark Mb()
        {
            return _mb;
        }
        public override Cursor? ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (mb is not null && mb.Value().HasValue)
                return new RTreeBookmark(_cx,_rt, _pos+1, mb);
            return null;
        }
        internal static RTreeBookmark? New(Context cx,RTree rt)
        {
            for (var mb = rt.mt.First(); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx,rt, 0, mb);
            return null;
        }
        internal static RTreeBookmark? New(RTree rt,Context cx)
        {
            for (var mb = rt.mt.Last(); mb != null; mb = mb.Previous())
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx, rt, 0, mb);
            return null;
        }
        internal static RTreeBookmark? New(Context cx,RTree rt,CList<TypedValue> key, int off=0)
        {
            for (var mb = rt.mt.PositionAt(key,off); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx, rt, 0, mb);
            return null;
        }
        protected override Cursor? _Next(Context cx)
        {
            var mb = _mb;
            for (;;)
            {
                mb = mb.Next();
                if (mb == null)
                    return null;
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx,_rt, _pos+1,mb);
            }
        }
        protected override Cursor? _Previous(Context cx)
        {
            var mb = _mb;
            for (; ; )
            {
                mb = mb.Previous();
                if (mb == null)
                    return null;
                if (mb.Value().HasValue)
                {
                    //         var rvv = _rt.rows[(int)mb.Value().Value].rvv;
                    //         var d = (rvv != null) ? rvv.def : 0;
                    return new RTreeBookmark(cx, _rt, _pos + 1, mb);
                }
            }
        }
        internal override BList<TableRow> Rec()
        {
            var s = BTree<long, TableRow>.Empty;
            var r = BList<TableRow>.Empty;
            for (var b = _cs.First(); b != null; b = b.Next())
                for (var c = b.value()?.Rec()?.First(); c != null; c = c.Next())
                {
                    var d = c.value();
                    if (!s.Contains(d.ppos))
                        s += (d.ppos, d);
                }
            for (var b = s.First(); b != null; b = b.Next())
                r += b.value();
            return r;
        }
    }
}

