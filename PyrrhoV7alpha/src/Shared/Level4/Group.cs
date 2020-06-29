using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
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
        /// <summary>
        /// The source rowset for the grouping operation. 
        /// See section 6.2.1 of SourceIntro.doc for explanations of terms
        /// </summary>
        internal readonly RowSet source;
        /// <summary>
        /// The request group specification
        /// </summary>
        internal readonly GroupSpecification groups;
        internal readonly BTree<long, bool> having;
        internal readonly BList<long> groupings;
        /// <summary>
        /// All the rows match the query rowType.
        /// </summary>
        internal readonly BList<TRow> rows = BList<TRow>.Empty;
        internal override RowSet Source => source;
        /// <summary>
        /// Constructor: called from QuerySpecification
        /// </summary>
        /// <param name="q">The query</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="gr">The group specification</param>
        /// <param name="h">The having condition</param>
        public GroupingRowSet(Context cx, Query q, RowSet rs, long gr, BTree<long,bool> h)
            : base(q.defpos,cx,q.domain,q.rowType,rs.finder,_Key(cx,q,gr),q.where,
                  q.ordSpec,q.matches,q.matching)
        {
            source = rs;
            having = h;
            groups = (GroupSpecification)cx.obs[gr];
            var gs = BList<long>.Empty;
            for (var b = groups.sets.First(); b != null; b = b.Next())
                gs = _Info(cx,(Grouping)cx.obs[b.value()],gs);
            groupings = gs;
            rows = (BList<TRow>)Build(cx);
        }
        protected GroupingRowSet(GroupingRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            rows = rs.rows;
            having = rs.having;
            groups = rs.groups;
        }
        internal override RowSet New(long a, long b)
        {
            return new GroupingRowSet(this, a, b);
        }
        static BList<long> _Info(Context cx,Grouping g,BList<long>gs)
        {
            gs += g.defpos;
            var cs = BList<SqlValue>.Empty;
            for (var b=g.members.First();b!=null;b=b.Next())
                cs += (SqlValue)cx.obs[b.key()];
            var oi = new ObInfo(g.defpos, cx, cs);
            cx.Replace(g,g + (DBObject._Domain, oi.domain));
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
        internal override RowSet New(long dp, Context cx)
        {
            throw new System.NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Grouping ");
            sb.Append(groups);
            var cm = "having ";
            for(var b=having.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value().ToString());
            }
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        /// <summary>
        /// Build the grouped tables in the result.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        protected override object Build(Context _cx)
        {
            var cx = new Context(_cx);
            cx.copy = matching;
            cx.from += source.finder;
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
                    rows+= new TRow(rt, dataType, vs);
                }
            }
            return rows;
        }
        /// <summary>
        /// Bookmark implementation
        /// </summary>
        /// <returns>the first row or null if there are none</returns>
        public override Cursor First(Context _cx)
        {
            return GroupingBookmark.New(_cx,this);
        }
        Cursor FirstB(Context _cx)
        {
            return GroupingBuilding.New(_cx, this);
        }
        internal class GroupingBuilding : Cursor
        {
            public readonly GroupingRowSet _grs;
            public readonly Cursor _bbm;
            public readonly ABookmark<TRow, BTree<long, TypedValue>> _ebm;
            GroupingBuilding(Context _cx, GroupingRowSet grs, Cursor bbm,
                ABookmark<TRow, BTree<long, TypedValue>> ebm, int pos)
                : base(_cx, grs.defpos, pos, (bbm != null) ? bbm._defpos : 0,
                      grs.rt, grs.dataType, new TRow(grs, bbm.values))
            {
                _grs = grs;
                _bbm = bbm;
                _ebm = ebm;
                _cx.cursors += (grs.defpos, this);
            }
            internal static GroupingBuilding New(Context _cx, GroupingRowSet grs)
            {
                Cursor bbm;
                var oc = _cx.from;
                _cx.from += grs.source.finder;
                for (bbm = grs.source.First(_cx); bbm != null; bbm = bbm.Next(_cx))
                {
                    var rb = new GroupingBuilding(_cx, grs, bbm, null, 0);
                    if (rb.Matches(_cx))
                    {
                        _cx.from = oc;
                        return rb;
                    }
                }
                _cx.from = oc;
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new System.NotImplementedException();
            }
            public override Cursor Next(Context _cx)
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
            internal static GroupingBookmark New(Context _cx, GroupingRowSet grs)
            {
                var ox = _cx.from;
                _cx.from += grs.source.finder;
                for (var ebm = grs.rows?.First(); ebm != null; ebm = ebm.Next())
                {
                    var r = new GroupingBookmark(_cx, grs, ebm, 0);
                    if (r.Matches(_cx) && Query.Eval(grs.where, _cx))
                    {
                        _cx.from = ox;
                        return r;
                    }
                }
                _cx.from = ox;
                return null;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            public override Cursor Next(Context cx)
            {
                var ox = cx.from;
                cx.from += _grs.source.finder;
                var ebm = _ebm.Next();
                var dt =_grs.dataType;
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
