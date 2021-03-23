using System;
using System.Configuration;
using System.Security.Cryptography;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// RTree is used for total and partial orderings where the value is of type RRow: 
    /// Logically an RTree contains associations of form (TRow,RRow)
    /// RTree uses MTree for implementation.
    /// IMMUTABLE
    /// </summary>
    internal class RTree
    {
        internal readonly long defpos; // RowSet
        internal readonly Domain domain;
        internal CList<long> keyType => domain.rowType;
        internal readonly Context _cx;
        internal readonly MTree mt;
        internal readonly BList<Cursor> rows;
        /// <summary>
        /// Constructor: a new empty MTree for given TreeSpec
        /// </summary>
        internal RTree(long dp,Context cx,CList<long> ks,Domain dt,
            TreeBehaviour d=TreeBehaviour.Disallow,TreeBehaviour n=TreeBehaviour.Allow)
        {
            defpos = dp;
            domain = dt+(Domain.RowType,ks);
            _cx = cx;
            mt = new MTree(new TreeInfo(ks,dt,d,n));
            rows = BList<Cursor>.Empty;
        }
        internal RTree(long dp, Context cx, Domain dt, TreeBehaviour d = TreeBehaviour.Disallow,
             TreeBehaviour n = TreeBehaviour.Allow)
        {
            defpos = dp;
            domain = dt;
            _cx = cx;
            var m = new MTree(new TreeInfo(dt.rowType, dt, d, n));
            mt = m;
            rows = BList<Cursor>.Empty;
        }
        protected RTree(long dp,Context cx,CList<long> k,Domain d,MTree m,BList<Cursor> rs)
        {
            defpos = dp;
            _cx = cx;
            domain = d+(Domain.RowType,k);
            mt = m;
            rows = rs;
        }
        public static TreeBehaviour Add(ref RTree t, TRow k, Cursor v)
        {
            var m = t.mt;
            TreeBehaviour tb = MTree.Add(ref m, 
                (k.Length==0)?null:new PRow(k), t.rows.Count);
            if (tb == TreeBehaviour.Allow)
                t = new RTree(t.defpos,t._cx,t.keyType, t.domain, m, t.rows + v);
            return tb;
        }
        public RTreeBookmark First(Context cx)
        {
            return RTreeBookmark.New(cx,this);
        }
        public RTreeBookmark Last(Context cx)
        {
            return RTreeBookmark.New(this, cx);
        }
        internal RTree Fix(Context cx)
        {
            return new RTree(cx.rsuids[defpos]??defpos, _cx, 
                cx.Fix(keyType), (Domain)domain.Fix(cx),
                mt.Fix(cx),cx.Fix(rows));
        }
        internal RTree Relocate(Writer wr)
        {
            return new RTree(wr.Fix(defpos), _cx, (Domain)domain._Relocate(wr),
                mt.info.onDuplicate, mt.info.onNullKey);
        }
        internal RTree Replace(Context cx,DBObject so,DBObject sv)
        {
            return new RTree(defpos, cx, cx.Replaced(keyType), (Domain)domain._Replace(cx,so,sv),
                mt.Replaced(cx,so,sv), rows);
        }
    }
    internal class RTreeBookmark : Cursor
    {
        internal readonly RTree _rt;
        internal readonly MTreeBookmark _mb;
        internal readonly TRow _key;
        RTreeBookmark(Context cx,RTree rt,int pos,MTreeBookmark mb,Cursor rr)
            :base(cx,rt.defpos,rt.domain,pos,rr._defpos,rr._ppos,rr)
        { 
            _rt = rt; _mb = mb; 
            _key = new TRow(new Domain(Sqlx.ROW,cx,_rt.keyType), mb.key());
        }
        RTreeBookmark(Context cx,RTree rt, int pos, MTreeBookmark mb) 
            :this(cx,rt,pos,mb,rt.rows[(int)(mb.Value()??-1L)])
        { }
        public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (mb!=null && mb.Value().HasValue)
                return new RTreeBookmark(_cx,_rt, _pos+1, mb);
            return null;
        }
        internal static RTreeBookmark New(Context cx,RTree rt)
        {
            for (var mb = rt.mt.First(); mb != null; mb = mb.Next())
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx,rt, 0, mb);
            return null;
        }
        internal static RTreeBookmark New(RTree rt,Context cx)
        {
            for (var mb = rt.mt.Last(); mb != null; mb = mb.Previous())
                if (mb.Value().HasValue)
                    return new RTreeBookmark(cx, rt, 0, mb);
            return null;
        }
        protected override Cursor _Next(Context cx)
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
        protected override Cursor _Previous(Context cx)
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
            throw new NotImplementedException();
        }
        internal override Cursor _Fix(Context cx)
        {
            throw new NotImplementedException();
        }
    }
}

