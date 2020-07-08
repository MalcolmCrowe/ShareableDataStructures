using System;
using Pyrrho.Common;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level4
{
    /// <summary>
    /// See BTree for details 
    /// RTree is used for total and partial orderings where the value is of type TRow: 
    /// Logically an RTree contains associations of form (TRow,TRow)
    /// The rows in the RTree are a List of TRow
    /// RTree uses MTree for implementation.
    /// IMMUTABLE
    /// </summary>
    internal class RTree
    {
        internal readonly long defpos;
        internal readonly Domain domain;
        internal readonly Context _cx;
        internal readonly RowType keyType;
        internal readonly MTree mt;
        internal readonly BList<TRow> rows;
        /// <summary>
        /// Constructor: a new empty MTree for given TreeSpec
        /// </summary>
        internal RTree(long dp,Context cx,RowType ks,Domain dt,
            TreeBehaviour d=TreeBehaviour.Disallow,TreeBehaviour n=TreeBehaviour.Allow)
        {
            defpos = dp;
            keyType = ks;
            domain = dt;
            _cx = cx;
            mt = new MTree(new TreeInfo(ks,dt,d,n));
            rows = BList<TRow>.Empty;
        }
        internal RTree(long dp,Context cx,RowType ks,Domain dt,
            BList<TRow> rs, TreeBehaviour d= TreeBehaviour.Disallow,
            TreeBehaviour n = TreeBehaviour.Allow)
        {
            defpos = dp;
            keyType = ks;
            domain = dt;
            _cx = cx;
            var m = new MTree(new TreeInfo(ks, dt, d, n));
            var i = 0;
            for (var b=rs.First();b!=null;b=b.Next(),i++)
            {
                var rw = b.value();
                MTree.Add(ref m, new PRow(new TRow(ks, dt, rw.values)), i);
            }
            mt = m;
            rows = rs;
        }
        protected RTree(long dp,Context cx,RowType k,Domain d,MTree m,BList<TRow> rs)
        {
            defpos = dp;
            keyType = k;
            _cx = cx;
            domain = d;
            mt = m;
            rows = rs;
        }
        public static TreeBehaviour Add(ref RTree t, TRow k, TRow v)
        {
            var m = t.mt;
            TreeBehaviour tb = MTree.Add(ref m, 
                (k.Length==0)?null:new PRow(k), t.rows.Count);
            if (tb == TreeBehaviour.Allow)
                t = new RTree(t.defpos,t._cx,t.keyType, t.domain, m, t.rows + v);
            return tb;
        }
        public static RTree operator+(RTree t,(TRow,TRow)x)
        {
            Add(ref t, x.Item1, x.Item2);
            return t;
        }
        public RTreeBookmark First(Context cx)
        {
            return RTreeBookmark.New(cx,this);
        }
        public Cursor PositionAt(Context cx, PRow key)
        {
            return RTreeBookmark.New(cx,this,key);
        }
        public bool Contains(PRow k)
        {
            return mt.Contains(k);
        }
        public TRow Get(Context cx,TRow key)
        {
            return rows[(int)mt.Get(cx.MakeKey(keyType)).Value];
        }
    }
    internal class RTreeBookmark : Cursor
    {
        internal readonly RTree _rt;
        internal readonly MTreeBookmark _mb;
        internal readonly TRow _key;
        RTreeBookmark(Context cx,RTree rt, int pos, MTreeBookmark mb) 
            :base(cx,rt.defpos,pos,mb._pos,rt.rows[(int)(mb.Value()??-1L)])
        {
            _rt = rt; _mb = mb; _key = new TRow(_rt.keyType,_rt.domain, mb.key());
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            throw new NotImplementedException();
        }
        public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (mb!=null && mb.Value().HasValue)
                return new RTreeBookmark(_cx,_rt, _pos + 1, mb);
            return null;
        }
        internal static RTreeBookmark New(Context cx,RTree rt)
        {
            for (var mb = rt.mt.First(); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                {
            //        var rvv = rt.rows[(int)mb.Value().Value].rvv;
                    return new RTreeBookmark(cx,rt, 0, mb);
                }
            return null;
        }
        internal static RTreeBookmark New(Context cx, RTree rt, PRow key)
        {
            for (var mb = rt.mt.PositionAt(key); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                {
                    //        var rvv = rt.rows[(int)mb.Value().Value].rvv;
                    return new RTreeBookmark(cx, rt, 0, mb);
                }
            return null;
        }
        public override Cursor Next(Context cx)
        {
            var mb = _mb;
            for (;;)
            {
                mb = mb.Next();
                if (mb == null)
                    return null;
                if (mb.Value().HasValue)
                {
           //         var rvv = _rt.rows[(int)mb.Value().Value].rvv;
           //         var d = (rvv != null) ? rvv.def : 0;
                    return new RTreeBookmark(cx,_rt, _pos+1,mb);
                }
            }
        }
        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
}

