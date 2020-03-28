using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    internal class GroupingRowSet : RowSet
    {
        /// <summary>
        /// The grouping row set has an array of Selectors used for grouping.
        /// Each grouping set therefore corresponds to a number gid (bitmap of participating Selectors)
        /// We have a BTree of Groupings (not an arraylist!)
        /// </summary>
        internal class GroupInfo
        {
            /// <summary>
            /// The enclosing grouping rowset
            /// </summary>
            internal readonly GroupingRowSet grs;
            /// <summary>
            /// The grouping of this group
            /// </summary>
            internal readonly Grouping group;
            internal readonly ObInfo nominalKeyInfo, nominalInfo;
            /// <summary>
            /// The information for a group
            /// </summary>
            /// <param name="q">the query</param>
            /// <param name="r">The parent grouping row set (may be null)</param>
            /// <param name="g">The grouping required</param>
            internal GroupInfo(GroupingRowSet r, Grouping g)
            {
                grs = r; group = g;
                var ss = ObInfo.Any; 
                for (var b=g.members.First();b!=null;b=b.Next())
                {
                    var sv = b.key();
                    ss += r.info[sv] ??
                        throw new PEException("PE856");
                }
                nominalKeyInfo = ss; //Domain.For(cx,ss);
                if (grs != null)
                {
                    nominalInfo = r.info;
       //             for (var i = 0; i < q.cols.Count; i++)
        //                q.cols[i].AddNeeds(this);
                }
            }
            /// <summary>
            /// Construct the group key for the current row
            /// </summary>
            /// <returns>A key for the row</returns>
            internal TRow MakeKey(Cursor rb)
            {
                var r = new List<TypedValue>();
                var rw = rb;
                for (int k = 0; k < nominalKeyInfo.Length; k++)
                {
                    var n = nominalKeyInfo[k].defpos;
                    r.Add(rw[n]);
                }
                return new TRow(nominalKeyInfo, r.ToArray());
            }
        }
        /// <summary>
        /// The source rowset for the grouping operation
        /// </summary>
        internal readonly RowSet source;
        /// <summary>
        /// The request group specification
        /// </summary>
        internal readonly GroupSpecification groups;
        internal readonly BTree<long, SqlValue> having;
        /// <summary>
        /// The group information: gid->groupinfo
        /// </summary>
        internal readonly BTree<long, GroupInfo> groupInfo = BTree<long,GroupInfo>.Empty;
        /// <summary>
        /// The grouped tables: one for each groupinfo
        /// </summary>
        internal readonly BTree<long,BList<TRow>> rows = BTree<long,BList<TRow>>.Empty;
        internal readonly ObInfo buildInfo = ObInfo.Any;
        internal override RowSet Source => source;
        /// <summary>
        /// Constructor: called from QuerySpecification
        /// </summary>
        /// <param name="q">The query</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="gr">The group specification</param>
        /// <param name="h">The having condition</param>
        public GroupingRowSet(Context _cx, Query q, RowSet rs, GroupSpecification gr, BTree<long,SqlValue> h)
            : base(q.defpos,_cx,q.rowType.info,rs.finder,_Key(_cx,q,gr),q.where,q.ordSpec,
                  q.matches,Context.Copy(q.matching))
        {
            source = rs;
            having = h;
            groups = gr;
            for (var g = gr.sets.First(); g != null; g = g.Next())
            {
                var gi = new GroupInfo(this, g.value());
                groupInfo += (g.value().defpos, gi);
                for (var b =gi.nominalKeyInfo.columns.First();b!=null;b=b.Next())
                {
                    var kc = b.value();
                    if (!buildInfo.map.Contains(kc.name))
                        buildInfo += kc;
                }
            }
            for (var b = buildInfo.columns.First(); b != null; b = b.Next())
                if (b.value().aggregates())
                    _cx.from += (b.value().defpos, rs.defpos);
            rows = (BTree<long,BList<TRow>>)Build(_cx);
        }
        protected GroupingRowSet(GroupingRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            rows = rs.rows;
            having = rs.having;
            groupInfo = rs.groupInfo;
            groups = rs.groups;
            buildInfo = rs.buildInfo;
        }
        internal override RowSet New(long a, long b)
        {
            return new GroupingRowSet(this, a, b);
        }
        static ObInfo _Key(Context cx,Query q,GroupSpecification gr)
        {
            var ss = q.rowType.info;
            var ns = BTree<long, SqlValue>.Empty;
            for (var b=gr.sets.First();b!=null;b=b.Next())
                for (var c=b.value().members.First();c!=null;c=c.Next())
                {
                    var s = c.key();
                    if (!ns.Contains(s))
                    {
                        var se = q.rowType[s]
                            ?? throw new PEException("PE855");
                        ss += se;
                        ns += (s, se);
                    }
                }
            return ss; 
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
        protected override object Build(Context _cx)
        {
            var g_rows = BTree<long, BTree<TRow, GroupRow>>.Empty;
            var cx = new Context(_cx);
            cx.copy = matching;
            cx.from = _finder;
            //            cx.profile = _cx.profile;
            for (var rb = FirstB(cx) as GroupingBuilding; rb != null;
                rb = rb.Next(cx) as GroupingBuilding)
                if (!rb.IsNull)
                    for (var gi = groupInfo.First(); gi != null; gi = gi.Next())
                    {
                        var g = gi.value();
                        for (var b = rb._grs.having.First(); b != null; b = b.Next())
                            if (b.value()?.Eval(cx) != TBool.True)
                                goto next;
                        var gI = g_rows[gi.key()] ?? BTree<TRow,GroupRow>.Empty;
                        var key = (TRow)g.nominalKeyInfo.Eval(cx);
                        GroupRow r = gI[key];
                        if (r == null)
                        {
                            r = new GroupRow(cx, g.group.defpos, info, g, rb,  g.nominalInfo, key);
                            gI += (key, r);
                            g_rows += (gi.key(), gI);
                        }
                        for (var gb = g.nominalInfo.columns.First(); gb != null; gb = gb.Next())
                            gb.value()?.AddIn(cx, rb, key);
                        next:;
                    }
            var rowsX = BTree<long, BList<TRow>>.Empty;
            for (var rg = g_rows.First(); rg != null; rg = rg.Next())
                for (var gb = rg.value().First(); gb != null; gb = gb.Next())
                {
                var key = rg.key();
                var vs = BTree<long, TypedValue>.Empty;
                    for (var b = info.columns.First(); b != null; b = b.Next())
                        ((SqlValue)_cx.obs[b.value().defpos]).AddReg(cx, defpos, gb.key());
                    var rw = rg.value();
                    for (var b = gb.key().values.First(); b != null; b = b.Next())
                        cx.values += (b.key(), b.value());
                    for (var b = info.columns.First(); b != null; b = b.Next())
                        if (b.value() is ObInfo sv)
                            vs += (sv.defpos, sv.Eval(cx));
                    for (var b = gb.key().values.First(); b != null; b = b.Next())
                        vs += (b.key(), b.value());
                    var gT = rowsX[key] ?? BList<TRow>.Empty;
                    var row = new TRow(dataType, vs);
                    gT += row;
                    rowsX += (key, gT);
                }
            return rowsX;
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
                      grs.buildInfo, new TRow(grs.buildInfo.domain, bbm.values))
            {
                _grs = grs;
                _bbm = bbm;
                _ebm = ebm;
                _cx.cursors += (grs.defpos, this);
            }
            internal static GroupingBuilding New(Context _cx, GroupingRowSet grs)
            {
                Cursor bbm;
                for (bbm = grs.source.First(_cx); bbm != null; bbm = bbm.Next(_cx))
                {
                    var rb = new GroupingBuilding(_cx, grs, bbm, null, 0);
                    if (rb.Matches(_cx))
                        return rb;
                }
                return null;
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
            public readonly ABookmark<long,GroupInfo> _gib;
            public readonly ABookmark<int, TRow> _ebm;
            GroupingBookmark(Context _cx, GroupingRowSet grs,
                ABookmark<long,GroupInfo> gib,ABookmark<int,TRow> ebm, int pos)
                : base(_cx, grs, pos, -1L,ebm.value())
            {
                _grs = grs;
                _gib = gib;
                _ebm = ebm;
                _cx.cursors += (grs.defpos, this);
            }
            internal static GroupingBookmark New(Context _cx, GroupingRowSet grs)
            {
                var ox = _cx.from;
                _cx.from = grs._finder;
                for (var gib = grs.groupInfo.First(); gib != null; gib = gib.Next())
                {
                    var gid = gib.key();
                    for (var ebm = grs.rows[gid]?.First();ebm!=null;ebm=ebm.Next())
                    {
                        var r = new GroupingBookmark(_cx, grs,gib, ebm, 0);
                        if (r.Matches(_cx) && Query.Eval(grs.where, _cx))
                        {
                            _cx.from = ox;
                            return r;
                        }
                    }
                }
                _cx.from = ox;
                return null;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from = _grs._finder;
                var gib = _gib;
                var ebm = _ebm.Next();
                for (; ; )
                {
                    for (; ebm != null; ebm = ebm.Next())
                    {
                        var r = new GroupingBookmark(_cx, _grs, gib, ebm, _pos + 1);
                        for (var b = _grs.info.columns.First(); b != null; b = b.Next())
                            ((SqlValue)_cx.obs[b.value().defpos]).OnRow(r);
                        if (r.Matches(_cx) && Query.Eval(_grs.where, _cx))
                        {
                            _cx.from = ox;
                            return r;
                        }
                    }
                    gib = gib.Next();
                    if (gib == null)
                    {
                        _cx.from = ox;
                        return null;
                    }
                    ebm = _grs.rows[gib.key()].First();
                }
            }

            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
}
