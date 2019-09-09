using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    internal class GroupingRowSet : RowSet
    {
        public override string keywd()
        {
            return " Grouping ";
        }
        /// <summary>
        /// The grouping row set has an array of Selectors used for grouping.
        /// Each grouping set therefore corresponds to a number gid (bitmap of participating Selectors)
        /// We have a BTree of Groupings (not an arraylist!)
        /// </summary>
        internal class GroupInfo
        {
            internal Query qry;
            /// <summary>
            /// The enclosing grouping rowset
            /// </summary>
            internal GroupingRowSet grs;
            /// <summary>
            /// The grouping of this group
            /// </summary>
            internal Grouping group;
            internal Domain nominalKeyType, nominalRowType;
            /// <summary>
            /// The information for a group
            /// </summary>
            /// <param name="q">the query</param>
            /// <param name="r">The parent grouping row set (may be null)</param>
            /// <param name="g">The grouping required</param>
            internal GroupInfo(Query q,GroupingRowSet r, Grouping g)
            {
                qry = q;  grs = r; group = g;
     //           if (q.Check(g) is Ident nm) // needs more thought for views with passed-in where
     //               throw new DBException("42170",nm.ToString());
                BList<Selector> ss = BList<Selector>.Empty; 
                for (var b=g.members.First();b!=null;b=b.Next())
                {
                    var sv = b.key();
                    ss += new Selector(sv.name,sv.defpos,sv.nominalDataType,b.value());
                }
                nominalKeyType = new Domain(ss);
                if (grs != null)
                {
                    nominalRowType = q.rowType;
       //             for (var i = 0; i < q.cols.Count; i++)
        //                q.cols[i].AddNeeds(this);
                }
            }
            /// <summary>
            /// If we already have a row for this key, add it in, otherwise start the row
            /// </summary>
            internal void AddIn(Context _cx,GroupingBookmark gb)
            {
                for (var b = gb._grs.having.First(); b != null; b = b.Next())
                    if (b.value()?.Eval(gb._rs._tr,_cx) != TBool.True)
                        return;
                var key = new TRow(nominalKeyType,gb.row.values);
                GroupRow r = grs.g_rows[key] 
                    ?? new GroupRow(_cx,group.defpos,this, gb,nominalRowType, key);
                for (int j = 0; j < nominalRowType.Length; j++)
                    r.columns[j]?.SetReg(_cx,key)?.AddIn(_cx,gb);
            }
            /// <summary>
            /// Construct the group key for the current row
            /// </summary>
            /// <returns>A key for the row</returns>
            internal TRow MakeKey(RowBookmark rb)
            {
                var r = new List<TypedValue>();
                var rw = rb.row;
                for (int k = 0; k < nominalKeyType.Length; k++)
                {
                    var n = nominalKeyType.columns[k].defpos;
                    r.Add(rw[n]);
                }
                return new TRow(nominalKeyType, r.ToArray());
            }
        }
        /// <summary>
        /// Helper class for Grouping operations.
        /// Constructed for a new GroupingRowSet, used in the middle of GroupingRowSet.Build()
        /// </summary>
        internal abstract class GroupRowEntry
        {
            internal GroupingRowSet grs;
            internal Ident id;
            internal int? ix;
            protected GroupRowEntry(GroupingRowSet rs,Ident n,int? x=null)
            { grs = rs; id = n; ix = x; }
            internal abstract TypedValue Eval(Context _cx, ABookmark<TRow, GroupRow> bm);
        }
        internal class GroupKeyEntry : GroupRowEntry
        {
            internal GroupKeyEntry(GroupingRowSet rs, Ident n) : base(rs, n) { }
            internal override TypedValue Eval(Context _cx, ABookmark<TRow, GroupRow> bm)
            {
                return bm.key()[id];
            }
        }
        internal class GroupValueEntry : GroupRowEntry
        {
            internal GroupValueEntry(GroupingRowSet rs, Ident n, int? x) : base(rs, n, x) { }
            internal override TypedValue Eval(Context _cx, ABookmark<TRow, GroupRow> bm)
            {
                return bm.value().columns[ix.Value].Eval(grs._tr,_cx);
            }
        }
        /// <summary>
        /// The source rowset for the grouping operation
        /// </summary>
        internal RowSet source;
        /// <summary>
        /// The having condition for the grouping operation
        /// </summary>
        internal BTree<long,SqlValue> having;
        /// <summary>
        /// The current group
        /// </summary>
        internal GroupInfo curg = null;
        /// <summary>
        /// The request group specification
        /// </summary>
        internal GroupSpecification groups;
        /// <summary>
        /// The collection of rows in the GroupedRowSet during construction
        /// </summary>
        internal CTree<TRow, GroupRow> g_rows = null;
        /// <summary>
        /// A helper for simplifying grouping operations
        /// </summary>
        internal BTree<long, GroupRowEntry> ges = BTree<long,GroupRowEntry>.Empty;
        /// <summary>
        /// If !building we have a simple indexed collection of values
        /// </summary>
        internal CTree<TRow, TRow> rows = null;
        /// <summary>
        /// The group information: gid->groupinfo
        /// </summary>
        internal BTree<long, GroupInfo> info = BTree<long, GroupInfo>.Empty;
        /// <summary>
        /// Constructor: called from QuerySpecification
        /// </summary>
        /// <param name="q">The query</param>
        /// <param name="rs">The source rowset</param>
        /// <param name="gr">The group specification</param>
        /// <param name="h">The having condition</param>
        public GroupingRowSet(Context _cx, QuerySpecification q, RowSet rs, GroupSpecification gr, BTree<long,SqlValue> h)
            : base(rs._tr,_cx,q,q.rowType,_Type(q,gr))
        {
            source = rs;
            having = h;
            groups = gr;
            for (var g = gr.sets.First();g!=null;g=g.Next())
                info+=(g.value().defpos,new GroupInfo(q, this, g.value()));
            building = true;
        }
        static Domain _Type(Query q,GroupSpecification gr)
        {
            var ss = BList<Selector>.Empty;
            var ns = BTree<long, Selector>.Empty;
            var i = 0;
            for (var b=gr.sets.First();b!=null;b=b.Next())
                for (var c=b.value().members.First();c!=null;c=c.Next())
                {
                    var s = c.key();
                    if (!ns.Contains(s.defpos))
                    {
                        var se = new Selector(s.name, s.defpos, s.nominalDataType, i++);
                        ss += se;
                        ns += (s.defpos, se);
                    }
                }
            return new Domain(ss);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Grouping ");
            sb.Append(qry);
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
        internal override RowSet Source()
        {
            return source;
        }
        internal override void AddCondition(Context _cx,BTree<long,SqlValue> cond, long done)
        {
            source.AddCondition(_cx,cond, done);
        }
        internal override void AddMatch(SqlValue v, TypedValue typedValue)
        {
            base.AddMatch(v, typedValue);
            source.AddMatch(v, typedValue);
        }
        internal GroupRowEntry Lookup(Selector stc)
        {
            return ges[stc.defpos];
        }
        protected override void Build(Context _cx)
        {
            g_rows = new CTree<TRow, GroupRow>(source.keyType);
            for (var rb = _First(_cx) as GroupingBookmark; rb != null; 
                rb = rb.Next(_cx) as GroupingBookmark)
                for(var gi=info.First();gi!=null;gi=gi.Next())
                    gi.value().AddIn(_cx, rb);
            building = false;
            rows = new CTree<TRow, TRow>(source.keyType);
            for (var rg = g_rows.First(); rg != null; rg = rg.Next())
            {
                var rw = rg.value();
                for (var i=0;i< rowType.Length;i++)
                    if (_cx.func[rw[i].defpos] is FunctionData fd)
                        fd.cur = fd.regs[rg.key()];
                var vs = BTree<long, TypedValue>.Empty;
                for (var i = rowType.Length - 1; i >= 0; i--)
                    if (rw[i] is SqlValue sv)
                        vs += (sv.defpos, sv.Eval(_tr, _cx));
                rows +=(rg.key(), new TRow(rowType, vs));
            }
            g_rows = null; 
        }
        /// <summary>
        /// Bookmark implementation
        /// </summary>
        /// <returns>the first row or null if there are none</returns>
        protected override RowBookmark _First(Context _cx)
        {
            return GroupingBookmark.New(_cx,this);
        }
        public override RowBookmark First(Context _cx)
        {
            Build(_cx);
            return _First(_cx);
        }
        /// <summary>
        /// An enumerator for a grouping rowset: behaviour is different during building
        /// </summary>
        internal class GroupingBookmark : RowBookmark
        {
            public readonly GroupingRowSet _grs;
            public readonly RowBookmark _bbm;
            public readonly ABookmark<TRow, TRow> _ebm;
            readonly TRow _row, _key;
            public override TRow row => _row;

            public override TRow key => _key;

            GroupingBookmark(Context _cx, GroupingRowSet grs, RowBookmark bbm,
                ABookmark<TRow, TRow> ebm, int pos)
                : base(_cx, grs, pos, (bbm != null) ? bbm._defpos : 0)
            {
                _grs = grs;
                _bbm = bbm;
                _ebm = ebm;
                if (ebm == null) // during building
                {
                    var vs = _cx.values;
                    _row = new TRow(grs.source.rowType, vs);
                    _key = new TRow(grs.source.keyType, vs);
                }
                else {
                    var vs = ebm.value().values;
                    _row = new TRow(grs.qry.rowType, vs);
                    _key = new TRow(grs.keyType, vs);
                    _cx.Add(grs.qry, this);
                }
            }
            internal static GroupingBookmark New(Context _cx,GroupingRowSet grs)
            {
                RowBookmark bbm = null;
                GroupingBookmark r = null;
                if (grs.building)
                {
                    for (bbm = grs.source.First(_cx);bbm!=null;bbm=bbm.Next(_cx))
                    {
                        var rb = new GroupingBookmark(_cx,grs, bbm, null, 0);
                        if (rb.Matches()) 
                            return rb;
                    }
                    return null;
                }
                else
                {
                    for (var ebm = grs.rows.First(); ebm != null; ebm = ebm.Next())
                    {
                        r = new GroupingBookmark(_cx,grs, bbm, ebm, 0);
                        if (r.Matches() && Query.Eval(grs.qry.where,grs._tr,_cx))
                            return r;
                    }
                    return null;
                }
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            public override RowBookmark Next(Context _cx)
            {
                var bbm = _bbm;
                if (_grs.building)
                {
                    for (bbm = bbm.Next(_cx); bbm != null; bbm = bbm.Next(_cx))
                    {
                        var rb = new GroupingBookmark(_cx,_grs, bbm, _ebm, _pos + 1);
                        if (rb.Matches()) 
                            return rb;
                    }
                }
                else
                {
                    for (var ebm = _ebm.Next(); ebm != null; ebm = ebm.Next())
                    {
                        var r = new GroupingBookmark(_cx,_grs, _bbm, ebm, _pos + 1);
                        for (var b = _rs.qry.cols.First(); b != null; b = b.Next())
                            b.value().OnRow(r);
                        if (r.Matches() && Query.Eval(_rs.qry.where, _rs._tr, _cx))
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
