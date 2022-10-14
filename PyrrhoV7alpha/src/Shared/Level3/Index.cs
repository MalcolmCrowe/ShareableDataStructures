using System;
using Pyrrho.Level2;
using Pyrrho.Level4; // for rename/drop
using Pyrrho.Common;
using System.Text;
using System.Configuration;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// This class corresponds to logical Index database objects.
    /// Indexes are database objects that are created by primary key and unique constraints.
    /// Indexes have unique names of form U(nnn), since they are not named in SQL.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class Index : DBObject 
    {
        static long _uniq = 0;
        internal const long
            Adapter = -157, // long Procedure 
            IndexConstraint = -158,// PIndex.ConstraintType
            Keys = -159, // CList<long> for Objects: TableColumn
                      // for RowSets: SqlValue
            References = -160, // BTree<long,BList<TypedValue>> computed by adapter
            RefIndex = -161, // long for Table: Index
                      // for RowSets: IndexRowSet/FilterRowSet/OrderedRowSet
            RefTable = -162, // long Table
            TableDefPos = -163, // long Table
            Tree = -164; // MTree
        /// <summary>
        /// Unique identifier nnn in U(nnn)
        /// </summary>
        public readonly long _nindex = _uniq++;
        /// <summary>
        /// The defining position for the table in the database.
        /// alas: if we cache table or valueType here we miss alterations, new TableColumns etc
        /// </summary>
        public long tabledefpos => (long)mem[TableDefPos];
        /// <summary>
        /// The flags describe the type of index
        /// </summary>
        public PIndex.ConstraintType flags => (PIndex.ConstraintType)(mem[IndexConstraint] ?? 0);
        /// <summary>
        /// The indexed rows: note the strong types inside here will need to be updated if column names change
        /// </summary>
        public MTree rows => (MTree)mem[Tree];
        public CList<long> keys => (CList<long>)mem[Keys]??CList<long>.Empty; //TableColumn
        /// <summary>
        /// for Foreign key, the referenced index
        /// </summary>
        public long refindexdefpos => (long)(mem[RefIndex] ?? -1L);
        /// <summary>
        /// for Foreign key, the referenced table
        /// </summary>
        public long reftabledefpos => (long)(mem[RefTable] ?? -1L);
        /// <summary>
        /// The adapter function
        /// </summary>
        public long adapter => (long)(mem[Adapter]??-1L);
        /// <summary>
        /// The references as computed by the adapter function if any
        /// </summary>
        public CTree<long, CList<TypedValue>> references =>
            (CTree<long, CList<TypedValue>>)mem[References];
        public Index(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// Constructor: a new Index 
        /// </summary>
        /// <param name="tb">The level 3 database</param>
        /// <param name="c">The level 2 index</param>
        public Index(PIndex c, Context cx)
            : base(c.ppos, c.defpos, cx.db.role.defpos, _IndexProps(c, cx)
                 + (TableDefPos, c.tabledefpos) + (IndexConstraint, c.flags)
                  + (ObInfo.Name, c.name))
        { }
        static BTree<long, object> _IndexProps(PIndex c, Context cx)
        {
            var r = BTree<long, object>.Empty;
            if (c.adapter != "")
            {
                r += (Adapter, c.adapter);
                r += (References, BTree<long, BList<TypedValue>>.Empty);
            }
            if (c.reference > 0 && cx.db.objects[c.reference] is Index rx)
            {
                var rp = rx.tabledefpos;
                var rt = (Table)cx.db.objects[rp];
                if (rx != null)
                {
                    r += (RefIndex, rx.defpos);
                    r += (RefTable, rx.tabledefpos);
                }
            }
            var cols = CList<long>.Empty;
            var tb = (Table)cx.obs[c.tabledefpos];
            for (var b = c.columns.First(); b != null; b = b.Next())
            {
                var pos = b.value();
                if (pos == 0)
                {
                    var pd = (PeriodDef)cx.db.objects[tb.systemPS];
                    pos = pd.startCol;
                }
                cols += pos;
            }
            TreeBehaviour isfk = (c.reference >= 0 || c.flags == PIndex.ConstraintType.NoType) ?
                TreeBehaviour.Allow : TreeBehaviour.Disallow;
            r += (Keys, cols);
            var rows = new MTree(new TreeInfo(cx, cols, isfk, isfk));
            r += (Tree, rows);
            return r;
        }
        public static Index operator +(Index x, (long, object) v)
        {
            return (Index)x.New(x.mem + v);
        }
        public static Index operator +(Index x,(PRow,long) v)
        {
            return x + (Tree, x.rows + v);
        }
        public static Index operator -(Index x, PRow k)
        {
            return x + (Tree, x.rows - k);
        }
        public static Index operator -(Index x, (PRow,long) y)
        {
            return x + (Tree, x.rows - y);
        }
        internal PRow MakeKey(CTree<long,TypedValue> vs)
        {
            PRow r = null;
            for (var b = keys.Last(); b != null; b = b.Previous())
                r = new PRow(vs[b.value()], r);
            return r;
        }
        /// <summary>
        /// Creator: an Index by modifying this from tableRows/versionedRows information
        /// </summary>
        /// <param name="db">The database</param>
        /// <returns>the new Index</returns>
        public Index Build(Context cx)
        {
            Table tb = (Table)cx.db.objects[tabledefpos];
            MTree rs = rows;
            bool rx = ((flags & PIndex.ConstraintType.ForeignKey) == PIndex.ConstraintType.ForeignKey);
            bool ux = ((flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != PIndex.ConstraintType.NoType);
            Index px = tb.FindPrimaryIndex(cx);
            if (px != null)
                for (var d = px.rows.First(); d != null; d = d.Next())
                {
                    long pp = d.Value().Value;
                    var r = tb.tableRows[pp];
                    var m = r.MakeKey(px);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(cx.db, m);
                        if (ux && rs.Contains(m))
                        {
                            var oi = tb.infos[cx.role.defpos];
                            throw new DBException("44002", "PRIMARY/UNIQUE", oi.name).Mix()
                                .Add(Sqlx.TABLE_NAME, new TChar(oi.name))
                                .Add(Sqlx.CONSTRAINT_NAME, new TChar("PRIMARY/UNIQUE"));
                        }
                        rs += (m, pp);
                    }
                }
            else
            {
                // there is no primary index, so we do it from the tableRows information
                for (var pp = tb.tableRows.PositionAt(0); pp != null; pp = pp.Next())
                {
                    var r = pp.value();
                    var m = r.MakeKey(this);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(cx.db, m);
                        if (ux && rs.Contains(m))
                        {
                            var oi = tb.infos[cx.role.defpos];
                            throw new DBException("44002", "PRIMARY/UNIQUE").Mix()
                                  .Add(Sqlx.TABLE_NAME, new TChar(oi.name))
                                  .Add(Sqlx.CONSTRAINT_NAME, new TChar("PRIMARY/UNIQUE"));
                        }
                        rs += (m, pp.key());
                    }
                }
            }
            return this + (Tree, rs);
        }
        /// <summary>
        /// Check referential integrity
        /// </summary>
        /// <param name="m">The key to check</param>
        void CheckRef(Database db, PRow m)
        {
            if (!(db is Transaction))
                return;
            Index rx = (Index)db.objects[refindexdefpos];
 /*           if (rx is VirtualIndex)
                return; */
            var tb = (Table)db.objects[reftabledefpos];
            if (!rx.rows.Contains(m))
            {
                var oi = tb.infos[db.role.defpos];
                throw new DBException("44002", "REFERENCES", oi.name).Mix()
                    .Add(Sqlx.TABLE_NAME, new TChar(oi.name))
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar("REFERENCES"));
            }
        }
        internal override void Cascade(Context cx, 
            Drop.DropAction a = Level2.Drop.DropAction.Restrict, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            if (reftabledefpos >= 0 && cx.db.objects[reftabledefpos] is Table ta)
                ta.FindPrimaryIndex(cx).Cascade(cx, a, u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is Table tb)
                    for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                        for (var c=xb.value().First();c!=null;c=c.Next())
                    {
                        var rx = (Index)cx.db.objects[c.key()];
                        if (rx == null || rx.refindexdefpos != defpos)
                            continue;
                        rx.Cascade(cx, a, u);
                    }
        }
        internal override Database Drop(Database db, Database nd, long p)
        {
            if (nd.objects[tabledefpos] is Table tb)
            {
                var xs = CTree<CList<long>, CTree<long,bool>>.Empty;
                var ks = CList<long>.Empty;
                for (var b = tb.indexes.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (c.key() != defpos)
                        {
                            var cs = b.key();
                            for (var d = cs.First(); d != null; d = d.Next())
                                ks += d.value();
                            var t = tb.indexes[ks] ?? CTree<long, bool>.Empty;
                            xs += (ks, t + (c.key(), true));
                        }
                tb += (Table.Indexes, xs);
                nd += (tb, p);
            }
            if (nd.objects[reftabledefpos] is Table rt)
            {
                var xs = CTree<long,CTree<CList<long>,CList<long>>>.Empty;
                for (var b = rt.rindexes.First(); b != null; b = b.Next())
                    if (b.key() == tabledefpos)
                    {
                        var nx = b.value() - keys;
                        if (nx.Count>0)
                            xs += (tabledefpos, nx);
                    } 
                    else
                        xs += (tabledefpos, b.value());
                rt += (Table.RefIndexes, xs);
                nd += (rt, p);
            }
            return base.Drop(db, nd, p);
        }
        /// <summary>
        /// A readable version of the Index
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Key:"); sb.Append(keys);
            sb.Append(" Kind="); sb.Append(flags);
            if (refindexdefpos != -1)
            {
                sb.Append(" RefIndex="); sb.Append(Uid(refindexdefpos));
                sb.Append(" RefTable="); sb.Append(Uid(reftabledefpos));
            }
            sb.Append(" Rows:"); sb.Append(rows);
            if (adapter!=-1)
            {
                sb.Append(" Adapter="); sb.Append(Uid(adapter));
                sb.Append(" References:"); sb.Append(references);
            }
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Index(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Index(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Index)base._Relocate(cx);
            r += (Adapter, cx.Fix(adapter));
            r += (Keys, cx.FixLl(keys));
            r += (References, cx.Fix(references));
            r += (RefIndex, cx.Fix(refindexdefpos));
            r += (RefTable, cx.Fix(reftabledefpos));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (Index)base._Fix(cx);
            var na = cx.Fix(adapter);
            if (na!=adapter)
            r += (Adapter, na);
            var nk = cx.FixLl(keys);
            if (nk!=keys)
            r += (Keys, nk);
            var nr = cx.Fix(references);
            if (nr!=references)
            r += (References, nr);
            var ni = cx.Fix(refindexdefpos);
            if (refindexdefpos!=ni)
                r += (RefIndex, ni);
            var nt = cx.Fix(reftabledefpos);
            if (reftabledefpos!=nt)
                r += (RefTable, nt);
            return r;
        }
    }
/*    /// <summary>
    /// A VirtualTable can have virtual indexs: they are for navigation properties
    /// and do not attempt to act as constraints on the remote table
    /// </summary>
    internal class VirtualIndex : Index
    {
        public VirtualIndex(VIndex pt,Context cx) :base(pt.defpos,_Mem(cx,pt))
        { }
        public VirtualIndex(long dp,BTree<long,object> m) : base(dp,m)
        { }
        static BTree<long,object> _Mem(Context cx,VIndex px)
        {
            var r = BTree<long, object>.Empty;
            r += (TableDefPos, px.tabledefpos);
            r += (IndexConstraint, px.flags);
            if (px.reference > 0 && cx.db.objects[px.reference] is Index rx)
            {
                r += (RefIndex, rx.defpos);
                r += (RefTable, rx.tabledefpos);
            }
            var cols = CList<long>.Empty;
            var tb = (Table)cx.obs[px.tabledefpos];
            cx.obs += tb.framing.obs;
            var dm = cx._Dom(tb);
            for (var b = px.seqs.First(); b != null; b = b.Next())
            {
                var seq = b.value();
                cols += dm.rowType[seq];
            }
            r += (Keys, cols);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new VirtualIndex(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new VirtualIndex(dp, mem);
        }
        internal override void Cascade(Context cx, Drop.DropAction a = Level2.Drop.DropAction.Restrict, BTree<long, TypedValue> u = null)
        {
        }
    } */
}
