using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level4
{
    /// shareable as of 26 April 2021
    internal class GroupingRowSet : RowSet
    {
        /// <summary>
        /// Constructor: called from QuerySpecification
        /// </summary>
        /// <param name="q">The query select list (a Domain defpos)</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="gr">The group specification</param>
        /// <param name="h">The having condition</param>
        public GroupingRowSet(Iix dp,Context cx, long q, RowSet rs, BTree<long,object> m)
            : base(dp.dp, cx, _Mem(dp,cx,q, rs, m))
        {
            cx.Add(this);
        }
        protected GroupingRowSet(long dp, Context cx, BTree<long, object> m) 
            : base(dp,cx, m) 
        { }
        protected GroupingRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// For views etc should propagate grouping to the source rowset rs as much as possible
        /// </summary>
        /// <param name="dp">The defpos of the future GroupingRowSet</param>
        /// <param name="cx">The context</param>
        /// <param name="d">The query select list (a Domain defpos)</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="m">Properties for the new GroupingRowSet</param>
        /// <returns>Updated list of properties for the GroupingRowSet</returns>
        static BTree<long, object> _Mem(Iix dp, Context cx,long d, RowSet rs, 
            BTree<long,object> m)
        {
            var gr = (long)m[Group];
            var groups = (GroupSpecification)cx.obs[gr];
            var gs = CList<long>.Empty;
            for (var b = groups.sets.First(); b != null; b = b.Next())
                gs = _Info(cx,(Grouping)cx.obs[b.value()],gs);
            m += (Groupings,gs);
            m += (_Domain, d);
            var fi = rs.finder;
            var ad = (Domain)cx.obs[d];
            for (var b = gs.First(); b != null; b = b.Next())
                for (var c = ((Grouping)cx.obs[b.value()]).members.First(); c != null; c = c.Next())
                    fi += (c.key(), new Finder(c.key(), dp.dp));
            for (var b = cx._Dom(ad).aggs.First(); b != null; b = b.Next())
                fi += (b.key(), new Finder(b.key(), dp.dp));
            m += (_Finder, fi); 
            m += (IIx, cx.Ix(rs.iix.lp,dp.dp));
            m += (_Source, rs.defpos);
            if (rs.Keys() is CList<long> ks)
                m += (Index.Keys, ks);
            m += (Index.Keys, _Key(cx, gr));
            m += (Table.LastData, rs.lastData);
            var h = (CTree<long,bool>)m[Having]??CTree<long,bool>.Empty;
            m += (_Depth, cx.Depth(h,groups,cx._Dom(d),rs));
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GroupingRowSet(defpos,m);
        }
        public static GroupingRowSet operator+(GroupingRowSet rs,(long,object)x)
        {
            return (GroupingRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new GroupingRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSet)base._Replace(cx, so, sv);
            r += (Groupings, cx.Replaced(groupings));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (GroupingRowSet)base._Fix(cx);
            var ng = cx.Fix(groupings);
            if (ng != groupings)
                r += (Groupings, ng);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (GroupingRowSet)base._Relocate(cx);
            r += (Groupings, cx.Fix(groupings));
            return r;
        }
        static CList<long> _Key(Context cx,long gr)
        {
            var ns = CList<long>.Empty;
            var ck = BTree<long, bool>.Empty;
            for (var b=((GroupSpecification)cx.obs[gr]).sets.First();b!=null;b=b.Next())
                for (var c=((Grouping)cx.obs[b.value()]).members.First();c!=null;c=c.Next())
                {
                    var s = c.key();
                    if (!ck.Contains(s))
                    {
                        var se = (SqlValue)cx.obs[s]
                            ?? throw new PEException("PE855");
                        ns += se.defpos;
                        ck += (s, true);
                    }
                }
            return ns;
        }
        protected override bool CanAssign()
        {
            return false;
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        /// <summary>
        /// Build the grouped tables in the result.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override RowSet Build(Context _cx)
        {
            var cx = new Context(_cx);
            var sce = (RowSet)cx.obs[source];
            cx.finder += sce.finder;
            var ts = BTree<long, BTree<PRow,BTree<long,Register>>>.Empty;
            // Traverse the source rowset building partial sums for aggregation expressions
            // for each combination of grouping expressions.
            // Both of these are SqlValues of course.
            for (var rb = sce.First(cx); rb != null;
                rb = rb.Next(cx))
                if (!rb.IsNull)
                {
                    for (var b = matches.First(); b != null; b = b.Next()) // this is now redundant
                    {
                        var sc = cx.obs[b.key()] as SqlValue;
                        if (sc==null || sc.CompareTo(b.value()) != 0)
                                goto next;
                    }
                    for (var b = where.First(); b != null; b = b.Next()) // so is this
                        if (cx.obs[b.key()].Eval(cx) != TBool.True)
                            goto next;
                    for (var gb = groupings.First(); gb != null; gb = gb.Next())
                    {
                        var g = (Grouping)_cx.obs[gb.value()];
                        var tg = ts[g.defpos] ?? BTree<PRow, BTree<long, Register>>.Empty;
                        var key = cx.MakeKey(g.keys);
                        var tk = tg[key] ?? BTree<long, Register>.Empty;
                        var dm = cx._Dom(this);
                        if (tk == BTree<long, Register>.Empty)
                            for (var b = dm.rowType.First(); b != null; b = b.Next())
                                tk = cx.obs[b.value()].StartCounter(cx, this, tk);
                        for (var b = dm.rowType.First(); b != null; b = b.Next())
                            tk = cx.obs[b.value()].AddIn(cx, rb, tk);
                        tg += (key, tk);
                        ts += (g.defpos, tg);
                    }
                next:;
                }
            var rows= BList<TRow>.Empty;
            cx.finder = finder + cx.finder;
            for (var gb = ts.First(); gb != null; gb = gb.Next())
            {
                var g = cx.obs[gb.key()] as Grouping;
                for (var b = gb.value().First(); b != null; b = b.Next())
                {
                    var vs = CTree<long,TypedValue>.Empty;
                    var k = b.key();
                    for (var c = g.keys.First(); c != null; c = c.Next(), k = k._tail)
                    {
                        var p = c.value();
                        vs += (p, k._head);
                        cx.values += (p, k._head);
                    }
                    cx.funcs = ts[g.defpos][b.key()];
                    // for the having calculation to work we must ensure that
                    // having uses the uids that are in aggs
                    for (var h = having.First(); h != null; h = h.Next())
                        if (cx.obs[h.key()].Eval(cx) != TBool.True)
                            goto skip;
                    var dm = cx._Dom(this);
                    for (var c = dm.rowType.First(); c != null; c = c.Next())
                        if (!vs.Contains(c.value()))
                        {
                            var sv = (SqlValue)cx.obs[c.value()];
                            vs+=(sv.defpos,sv.Eval(cx));
                        }
                    rows+= new TRow(dm, vs);
                skip:;
                }
            }
            return (RowSet)New(_cx,E+(_Rows,rows)+(_Built,true)+(Index.Tree,null));
        }
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            throw new DBException("42174");
        }
        /// <summary>
        /// Bookmark implementation
        /// </summary>
        /// <returns>the first row or null if there are none</returns>
        protected override Cursor _First(Context _cx)
        {
            return GroupingBookmark.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return GroupingBookmark.New(this, _cx);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" groupings (");
            var cm = "";
            for (var b=groupings.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (having!=CTree<long,bool>.Empty)
            {
                sb.Append(" having ("); cm = "";
                for (var b=having.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
        }
        /// <summary>
        /// An enumerator for a grouping rowset: behaviour is different during building
        ///     /// shareable as of 26 April 2021
        /// </summary>
        internal class GroupingBookmark : Cursor
        {
            public readonly GroupingRowSet _grs;
            public readonly ABookmark<int, TRow> _ebm;
            GroupingBookmark(Context _cx, GroupingRowSet grs,
                ABookmark<int,TRow> ebm, int pos)
                : base(_cx, grs, pos, E,ebm.value())
            {
                _grs = grs;
                _ebm = ebm;
                _cx.cursors += (grs.defpos, this);
            }
            GroupingBookmark(GroupingBookmark cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _grs = cu._grs;
                _ebm = cu._ebm;
                cx.cursors += (_grs.defpos, this);
            }
            GroupingBookmark(Context cx,GroupingBookmark cu): base(cx,cu)
            {
                _grs = (GroupingRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _ebm = _grs.rows.PositionAt(cu?._pos ?? 0);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new GroupingBookmark(this,cx,p,v);
            }
            internal static GroupingBookmark New(Context cx, GroupingRowSet grs)
            {
                var ox = cx.finder;
                cx.finder += grs.finder;
                var ebm = grs.rows?.First();
                var r = (ebm==null)?null:new GroupingBookmark(cx, grs, ebm, 0);
                cx.finder = ox;
                return r;
            }
            internal static GroupingBookmark New(GroupingRowSet grs, Context cx)
            {
                var ox = cx.finder;
                cx.finder += grs.finder;
                var ebm = grs.rows?.Last();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                cx.finder = ox;
                return r;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _grs.finder;
                var ebm = _ebm.Next();
                if (ebm==null)
                {
                    cx.finder = ox;
                    return null;
                }
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                cx.finder = ox;
                return r;
            }
            protected override Cursor _Previous(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _grs.finder;
                var ebm = _ebm.Previous();
                if (ebm == null)
                {
                    cx.finder = ox;
                    return null;
                }
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                cx.finder = ox;
                return r;
            }
            internal override Cursor _Fix(Context cx)
            {
                return new GroupingBookmark(cx, this);
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    // shareable as of 26 April 2021
    internal class EvalRowSet : RowSet
    {
        internal TRow row => (TRow)mem[TrivialRowSet.Singleton];
        /// <summary>
        /// Constructor: Build a rowSet that aggregates obs from a given source.
        /// For views we should propagate aggregations from q to the source rowset rs
        /// </summary>
        /// <param name="rs">The source obs</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Iix dp, Context cx, long q, RowSet rs,
            BTree<long, object> m)
            : base(dp.dp, cx, _Mem(dp.dp, cx, q, rs, m) + (_Source, rs.defpos)
                  + (IIx, new Iix(rs.iix, dp.dp))
                  + (RSTargets, rs.rsTargets)
                  + (Table.LastData, rs.lastData))
        {
            cx.Add(this);
        }
        protected EvalRowSet(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(long dp, Context cx, long q, RowSet rs,
                BTree<long, object> m)
        {
            var h = (CTree<long, bool>)m[Having] ?? CTree<long, bool>.Empty;
            var dm = cx._Dom(q);
            var fi = rs.finder;
            for (var b = dm.aggs.First(); b != null; b = b.Next())
                fi += (b.key(), new Finder(b.key(), dp));
            var r = new BTree<long, object>(Domain.Aggs, dm.aggs);
            if (h != CTree<long, bool>.Empty)
                r += (Having, h);
            r = r + (_Domain, dm.defpos) + (_Finder, fi);
            rs = rs.Apply(new BTree<long, object>(Domain.Aggs, dm.aggs), cx, rs);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EvalRowSet(defpos, m);
        }
        public static EvalRowSet operator +(EvalRowSet rs, (long, object) x)
        {
            return (EvalRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new EvalRowSet(dp, mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return new EvalBookmark(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return new EvalBookmark(_cx, this);
        }
        protected override bool CanAssign()
        {
            return false;
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var tg = BTree<long, Register>.Empty;
            var sce = (RowSet)cx.obs[source];
            cx.finder += sce.finder;
            var dm = cx._Dom(this);
            var cols = dm.rowType;
            if (cols.Length == 1 && cx.obs[cols[0]] is SqlFunction sf && sce.Built(cx) &&
                sf.kind == Sqlx.COUNT && sf.mod == Sqlx.TIMES && where == CTree<long, bool>.Empty)
                return (RowSet)New(cx, E + (_Built, true) + (TrivialRowSet.Singleton,
                    new TRow(dm, new CTree<long, TypedValue>(sf.defpos,
                        new TInt(sce.Build(cx)?.Cardinality(cx) ?? 0)))));
            cx.Add(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                tg = ((SqlValue)cx.obs[b.value()]).StartCounter(cx, this, tg);
            for (var ebm = sce.First(cx); ebm != null; ebm = ebm.Next(cx))
                if ((!ebm.IsNull) && Eval(having, cx))
                    for (var b = dm.rowType.First(); b != null; b = b.Next())
                        tg = ((SqlValue)cx.obs[b.value()]).AddIn(cx, ebm, tg);
            var vs = CTree<long, TypedValue>.Empty;
            cx.funcs = tg;
            for (int i = 0; i < cols.Length; i++)
            {
                var s = cols[i];
                vs += (s, cx.obs[s].Eval(cx));
            }
            return (RowSet)New(cx, E + (_Built, true) + (TrivialRowSet.Singleton, new TRow(dm, vs)));
        }
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation> Update(Context cx, RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation> Delete(Context cx, RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (EvalRowSet)base._Fix(cx);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RowSet)base._Replace(cx, so, sv);
            var rw = row.Replace(cx, so, sv);
            if (rw != row)
                r += (TrivialRowSet.Singleton, rw);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (mem.Contains(Having))
            {
                sb.Append(" having (");
                var cm = "";
                for (var b = having.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
        }
        // shareable as of 26 April 2021
        internal class EvalBookmark : Cursor
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers)
                : base(_cx, ers, 0, E, ers.row)
            {
                _ers = ers;
            }
            EvalBookmark(Context cx, EvalBookmark cu) : base(cx, cu)
            {
                _ers = (EvalRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
            }
            EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _ers = cu._ers;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new EvalBookmark(this, cx, p, v);
            }
            protected override Cursor _Next(Context _cx)
            {
                return null; // just one row in the rowset
            }
            protected override Cursor _Previous(Context cx)
            {
                return null; // just one row in the rowset
            }
            internal override Cursor _Fix(Context cx)
            {
                return new EvalBookmark(cx, this);
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
}
