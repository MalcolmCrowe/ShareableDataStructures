using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level4
{
    /// <summary>
    /// See BTree for details 
    /// RTree is used for total and partial orderings where the value is of type SqlRow: 
    /// Logically an RTree contains associations of form (key,SqlRow)
    /// The rows in the RTree are a List of SqlRow
    /// RTree uses MTree for implementation.
    /// RTrees are mutable, so RTree objects are transaction-local and not shareable.
    /// </summary>
    internal class RTree
    {
        internal RowSet _rs;
        internal MTree mt;
        internal TreeInfo info;
        internal List<TRow> rows = new List<TRow>();
        /// <summary>
        /// Constructor: a new empty MTree for given TreeSpec
        /// </summary>
        internal RTree(RowSet rs, TreeInfo ti)
        {
            _rs = rs;
            info = ti;
            mt = new MTree(new TreeInfo(ti, ti.onDuplicate, TreeBehaviour.Allow));
        }
        public static TreeBehaviour Add(ref RTree t, TRow k, TRow v)
        {
            TreeBehaviour tb = MTree.Add(ref t.mt, 
                (k.Length==0)?null:new PRow(k), t.rows.Count);
            if (tb == TreeBehaviour.Allow)
                t.rows.Add(v);
            return tb;
        }
        public static RTree operator+(RTree t,(TRow,TRow)x)
        {
            Add(ref t, x.Item1, x.Item2);
            return t;
        }
        public RTreeBookmark First(Context _cx)
        {
            return RTreeBookmark.New(_cx,this);
        }
        public RowBookmark PositionAt(Context _cx, PRow key)
        {
            for (var b = First(_cx); b != null; b = b.Next(_cx) as RTreeBookmark)
            {
                int j = 0;
                var k = b.key;
                var dt = k.info;
                for (var pk = key; pk != null; j++, pk = pk._tail)
                {
                    var c = ((SqlValue)dt.columns[j]).domain.Compare(k[j], pk._head);
                    if (c > 0)
                        return null;
                    if (c < 0)
                        goto skip;
                }
                return b;
                skip:;
            }
            return null;
        }
    }
    internal class RTreeBookmark : RowBookmark
    {
        internal readonly RTree _rt;
        internal readonly MTreeBookmark _mb;
        readonly TRow _row,_key;
        public override TRow row => _row;
        public override TRow key => _key;
        RTreeBookmark(Context _cx, RTree rt, int pos, MTreeBookmark mb) 
            : base(_cx,rt._rs,pos,mb.Position())
        {
            _rt = rt; _mb = mb; 
            _row = _rt.rows[(int)mb.Value()];
            _key = new TRow(_rs.keyType, mb.key());
        }
        public override MTreeBookmark Mb()
        {
            return _mb;
        }
        public override RowBookmark ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (mb!=null && mb.Value().HasValue)
                return new RTreeBookmark(_cx,_rt, _pos + 1, mb);
            return null;
        }
        internal static RTreeBookmark New(Context _cx, RTree rt)
        {
            for (var mb = rt.mt.First(); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                {
            //        var rvv = rt.rows[(int)mb.Value().Value].rvv;
                    return new RTreeBookmark(_cx,rt, 0, mb);
                }
            return null;
        }
        public override RowBookmark Next(Context _cx)
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
                    return new RTreeBookmark(_cx,_rt, _pos+1, mb);
                }
            }
        }

        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
}

