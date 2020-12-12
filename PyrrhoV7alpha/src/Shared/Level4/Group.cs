using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Pyrrho.Level2;
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
    internal class GroupingRowSet : RowSet
    {
        internal const long
            Groupings = -406;   //BList<long>   Grouping
        /// <summary>
        /// The source rowset for the grouping operation. 
        /// See section 6.2.1 of SourceIntro.doc for explanations of terms
        /// </summary>
        internal long source => (long)(mem[From.Source]??-1L);
        internal BTree<long, bool> having =>
            (BTree<long, bool>)mem[TableExpression.Having]??BTree<long,bool>.Empty;
        internal BList<long> groupings =>
            (BList<long>)mem[Groupings] ?? BList<long>.Empty;
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        /// <summary>
        /// Constructor: called from QuerySpecification
        /// </summary>
        /// <param name="q">The query</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="gr">The group specification</param>
        /// <param name="h">The having condition</param>
        public GroupingRowSet(Context cx, Query q, RowSet rs, long gr, BTree<long, bool> h)
            : base(q.defpos, cx, q.domain, rs.finder, _Key(cx, q, gr), q.where,
                  q.ordSpec, q.matches, q.matching, null, _Mem(cx, rs, gr, h))
        { }
        static BTree<long,object> _Mem(Context cx,RowSet rs,long gr,BTree<long,bool>h)
        {
            var m = BTree<long, object>.Empty;
            m += (From.Source,rs.defpos);
            m += (TableExpression.Having,h);
            m += (TableExpression.Group, gr);
            var groups = (GroupSpecification)cx.obs[gr];
            var gs = BList<long>.Empty;
            for (var b = groups.sets.First(); b != null; b = b.Next())
                gs = _Info(cx,(Grouping)cx.obs[b.value()],gs);
            m += (Groupings,gs);
            return m;
        }
        protected GroupingRowSet(Context cx,GroupingRowSet rs, BTree<long,Finder> nd,
            BList<TRow> rws,bool bt) :base(cx,rs+(_Rows,rws),nd,bt)
        { }
        protected GroupingRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new GroupingRowSet(defpos,m);
        }
        internal override RowSet New(Context cx,BTree<long, Finder> nd, bool bt)
        {
            return new GroupingRowSet(cx,this,nd,rows,bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new GroupingRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
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
        internal override Basis Fix(Context cx)
        {
            var r = (GroupingRowSet)base.Fix(cx);
            var ns = cx.rsuids[source] ?? source;
            if (ns != source)
                r += (From.Source, ns);
            var ng = cx.Fix(groupings);
            if (ng != groupings)
                r += (Groupings, ng);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (GroupingRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            r += (Groupings, wr.Fix(groupings));
            return r;
        }
        static BList<long> _Info(Context cx,Grouping g,BList<long>gs)
        {
            gs += g.defpos;
            var cs = BList<SqlValue>.Empty;
            for (var b=g.members.First();b!=null;b=b.Next())
                cs += (SqlValue)cx.obs[b.key()];
            var dm = new Domain(Sqlx.ROW,cs);
            cx.Replace(g,g + (DBObject._Domain, dm));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        static CList<long> _Key(Context cx,Query q,long gr)
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
        /// <summary>
        /// Build the grouped tables in the result.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override RowSet Build(Context _cx)
        {
            var cx = new Context(_cx);
            cx.copy = matching;
            cx.from += Source(cx).finder;
            var ts = BTree<long, BTree<PRow,BTree<long,Register>>>.Empty;
            // Traverse the source rowset building partial sums for aggregation expressions
            // for each combination of grouping expressions.
            // Both of these are SqlValues of course.
            for (var rb = FirstB(cx) as GroupingBuilding; rb != null;
                rb = rb.Next(cx) as GroupingBuilding)
                if (!rb.IsNull)
                    for (var gb = groupings.First(); gb != null; gb = gb.Next())
                    {
                        var g = (Grouping)_cx.obs[gb.value()];
                        var tg = ts[g.defpos] ?? BTree<PRow, BTree<long, Register>>.Empty;
                        for (var b = rb._grs.having.First(); b != null; b = b.Next())
                            if (cx.obs[b.key()].Eval(cx) != TBool.True)
                                goto next;
                        var key = cx.MakeKey(keys);
                        var tk = tg[key] ?? BTree<long, Register>.Empty;
                        if (tk==BTree<long,Register>.Empty)
                            for (var b = rt.First(); b != null; b = b.Next())
                                tk = cx.obs[b.value()].StartCounter(cx, this, tk);
                        for (var b = rt.First(); b != null; b = b.Next())
                            tk = cx.obs[b.value()].AddIn(cx, rb, tk);
                        tg += (key, tk);
                        ts += (g.defpos, tg);
                        next:;
                    }
            var rows= BList<TRow>.Empty;
            for (var gb = ts.First(); gb != null; gb = gb.Next())
            {
                var g = cx.obs[gb.key()] as Grouping;
                for (var b = gb.value().First(); b != null; b = b.Next())
                {
                    var vs = BTree<long,TypedValue>.Empty;
                    var k = b.key();
                    for (var c = g.members.First(); c != null; c = c.Next(), k = k._tail)
                        vs+=(c.key(), k._head);
                    cx.funcs = ts[g.defpos][b.key()];
                    for (var c = rt.First(); c != null; c = c.Next())
                        if (!vs.Contains(c.value()))
                        {
                            var sv = (SqlValue)cx.obs[c.value()];
                            vs+=(sv.defpos,sv.Eval(cx));
                        }
                    rows+= new TRow(domain, vs);
                }
            }
            return new GroupingRowSet(cx,this,needed,rows,true).ComputeNeeds(cx);
        }
        /// <summary>
        /// Bookmark implementation
        /// </summary>
        /// <returns>the first row or null if there are none</returns>
        protected override Cursor _First(Context _cx)
        {
            return GroupingBookmark.New(_cx,this);
        }
        Cursor FirstB(Context _cx)
        {
            return GroupingBuilding.New(_cx, this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class GroupingBuilding : Cursor
        {
            public readonly GroupingRowSet _grs;
            public readonly Cursor _bbm;
            public readonly ABookmark<TRow, BTree<long, TypedValue>> _ebm;
            GroupingBuilding(Context _cx, GroupingRowSet grs, Cursor bbm,
                ABookmark<TRow, BTree<long, TypedValue>> ebm, int pos)
                : base(_cx, grs, pos, (bbm != null) ? bbm._defpos : 0,
                      new TRow(grs.domain, bbm.values))
            {
                _grs = grs;
                _bbm = bbm;
                _ebm = ebm;
                _cx.cursors += (grs.defpos, this);
            }
            internal static GroupingBuilding New(Context cx, GroupingRowSet grs)
            {
                Cursor bbm;
                var oc = cx.from;
                var sce = grs.Source(cx);
                cx.from += sce.finder;
                for (bbm = sce.First(cx); bbm != null; bbm = bbm.Next(cx))
                {
                    var rb = new GroupingBuilding(cx, grs, bbm, null, 0);
                    if (rb.Matches(cx))
                    {
                        cx.from = oc;
                        return rb;
                    }
                }
                cx.from = oc;
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new System.NotImplementedException();
            }
            protected override Cursor _Next(Context _cx)
            {
                var bbm = _bbm;
                for (bbm = bbm.Next(_cx); bbm != null; bbm = bbm.Next(_cx))
                {
                    var rb = new GroupingBuilding(_cx, _grs, bbm, _ebm, _pos + 1);
                    if (rb.Matches(_cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
        /// <summary>
        /// An enumerator for a grouping rowset: behaviour is different during building
        /// </summary>
        internal class GroupingBookmark : Cursor
        {
            public readonly GroupingRowSet _grs;
            public readonly ABookmark<int, TRow> _ebm;
            GroupingBookmark(Context _cx, GroupingRowSet grs,
                ABookmark<int,TRow> ebm, int pos)
                : base(_cx, grs, pos, -1L,ebm.value())
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
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new GroupingBookmark(this,cx,p,v);
            }
            internal static GroupingBookmark New(Context cx, GroupingRowSet grs)
            {
                var ox = cx.from;
                var sce = grs.Source(cx);
                cx.from += sce.finder;
                for (var ebm = grs.rows?.First(); ebm != null; ebm = ebm.Next())
                {
                    var r = new GroupingBookmark(cx, grs, ebm, 0);
                    if (r.Matches(cx) && Query.Eval(grs.where, cx))
                    {
                        cx.from = ox;
                        return r;
                    }
                }
                cx.from = ox;
                return null;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.from;
                cx.from += _grs.Source(cx).finder;
                var ebm = _ebm.Next();
                var dt =_grs.domain;
                for (; ebm != null; ebm = ebm.Next())
                {
                    var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                    for (var b = dt.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,r);
                    if (r.Matches(cx) && Query.Eval(_grs.where, cx))
                    {
                        cx.from = ox;
                        return r;
                    }
                }
                return null;
            }

            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
}
