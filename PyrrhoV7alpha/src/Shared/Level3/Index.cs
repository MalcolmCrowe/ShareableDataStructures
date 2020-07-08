using System;
using Pyrrho.Level2;
using Pyrrho.Level4; // for rename/drop
using Pyrrho.Common;
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

namespace Pyrrho.Level3
{
    /// <summary>
    /// This class corresponds to logical Index database objects.
    /// Indexes are database objects that are created by primary key and unique constraints.
    /// Indexes have unique names of form U(nnn), since they are not named in SQL.
    /// </summary>
    internal class Index : DBObject // can't implement RowSet unfortunately, see below
    {
        static long _uniq = 0;
        internal const long
            Adapter = -157, // long Procedure 
            IndexConstraint = -158,// PIndex.ConstraintType
            Keys = -159, // RowType
            References = -160, // BTree<long,BList<TypedValue>> computed by adapter
            RefIndex = -161, // long Index
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
        public RowType keys => 
            (RowType)mem[Keys]??RowType.Empty;
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
        public BTree<long, BList<TypedValue>> references =>
            (BTree<long, BList<TypedValue>>)mem[References];
        internal override Sqlx kind => Sqlx.NONE;
        public Index(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// Constructor: a new Index 
        /// </summary>
        /// <param name="tb">The level 3 database</param>
        /// <param name="c">The level 2 index</param>
        public Index(PIndex c, Context cx)
            : base(c.name, c.ppos, c.defpos, cx.db.role.defpos, _IndexProps(c, cx)
                 + (TableDefPos, c.tabledefpos) + (IndexConstraint, c.flags))
        { }
        static BTree<long, object> _IndexProps(PIndex c, Context cx)
        {
            var r = BTree<long, object>.Empty;
            if (c.adapter != "")
            {
                r += (Adapter, cx.db.GetProcedure(c.adapter, 1));
                r += (References, BTree<long, BList<TypedValue>>.Empty);
            }
            if (c.reference > 0)
            {
                var rx = (Index)cx.db.objects[c.reference];
                var rp = rx.tabledefpos;
                var rt = (Table)cx.db.objects[rp];
                if (rx!=null)
                {
                    r += (RefIndex, rx.defpos);
                    r += (RefTable, rx.tabledefpos);
                }
            }
            var cols = RowType.Empty;
            var ds = BTree<long, DBObject>.Empty;
            var tb = (Table)cx.obs[c.tabledefpos];
            for (var b=c.columns.First();b!=null;b=b.Next())
            {
                var pos = b.value();
                if (pos == 0)
                {
                    var pd = (PeriodDef)cx.db.objects[tb.systemPS];
                    pos = pd.startCol;
                }
                var ob = (DBObject)cx.db.objects[pos];
                ds += (ob.defpos, ob);
                cols += (pos,ob.domain);
            }
            TreeBehaviour isfk = (c.reference >= 0 || c.flags == PIndex.ConstraintType.NoType) ?
                TreeBehaviour.Allow : TreeBehaviour.Disallow;
            r += (Keys, cols);
            var rows = new MTree(new TreeInfo(cols, ds, isfk, isfk));
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
        internal PRow MakeKey(TableRow rw)
        {
            PRow r = null;
            for (var b = keys.Last(); b != null; b = b.Previous())
                r = new PRow(rw.vals[b.value().Item1], r);
            return r;
        }
        /// <summary>
        /// Creator: an Index by modifying this from tableRows/versionedRows information
        /// </summary>
        /// <param name="db">The database</param>
        /// <returns>the new Index</returns>
        public Index Build(Database db)
        {
            Table tb = (Table)db.objects[tabledefpos];
            MTree rs = rows;
            bool rx = ((flags & PIndex.ConstraintType.ForeignKey) == PIndex.ConstraintType.ForeignKey);
            bool ux = ((flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != PIndex.ConstraintType.NoType);
            Index px = tb.FindPrimaryIndex(db);
            if (px != null)
                for (var d = px.rows.First(); d != null; d = d.Next())
                {
                    long pp = d.Value().Value;
                    var r = tb.tableRows[pp];
                    var m = r.MakeKey(px);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(db, m);
                        if (ux && rs.Contains(m))
                        {
                            var oi = (ObInfo)db.role.infos[tb.defpos];
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
                    var r = pp.value() as TableRow;
                    var m = r.MakeKey(this);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(db, m);
                        if (ux && rs.Contains(m))
                        {
                            var oi = (ObInfo)db.role.infos[tb.defpos];
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
            var tb = (Table)db.objects[reftabledefpos];
            if (!rx.rows.Contains(m))
            {
                var oi = (ObInfo)db.role.infos[tb.defpos];
                throw new DBException("44002", "REFERENCES", oi.name).Mix()
                    .Add(Sqlx.TABLE_NAME, new TChar(oi.name))
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar("REFERENCES"));
            }
        }
        /// <summary>
        /// Verify a given new foreign key
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="xmess">An error message</param>
        /// <param name="okay">A result boolean</param>
        /// <param name="m">The key</param>
        /// <param name="r">The record</param>
        public void CheckForeign(Database d, ref string xmess, ref bool okay, PRow m, Record r)
        {
            if (!(d is Transaction))
                return;
            if (m == null)
            {
                xmess = "null value in foreign key " + m.ToString();
                okay = false;
                return;
            }

        }
        /// <summary>
        /// Verify a new Unique or Primary key
        /// </summary>
        /// <param name="tb">The database</param>
        /// <param name="xmess">An error message</param>
        /// <param name="okay">A boolean result</param>
        /// <param name="m">The key</param>
        /// <param name="r">The record</param>
        public void CheckUnique(Database da, ref string xmess, ref bool okay, PRow m, Record r)
        {
            if (m != null && da is Transaction && rows.Contains(m))
            {
                Table tb = (Table)da.objects[tabledefpos];
                var oi = (ObInfo)da.role.infos[tabledefpos];
                xmess = "duplicate key " + oi.name + " " + m.ToString();
                okay = false;
            }
        }
        bool MatchCols(long[] a, long[] b)
        {
            if (a.Length != b.Length)
                return false;
            for (int i = 0; i < a.Length; i++)
                if (a[i] != b[i])
                    return false;
            return true;
        }
        /// <summary>
        /// Find the location in the key for a given tableColumn
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        public int PosFor(TableColumn c)
        {
            int r;
            for (r = 0; r < (int)keys.Count; r++)
                if (keys[r].Item1 == c.defpos)
                    return r;
            return -1;
        }
        internal override void Cascade(Context cx, 
            Drop.DropAction a = Level2.Drop.DropAction.Restrict, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            if (reftabledefpos >= 0 && cx.db.objects[reftabledefpos] is Table ta)
                ta.FindPrimaryIndex(cx.db).Cascade(cx, a, u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is Table tb)
                    for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                    {
                        var rx = (Index)cx.db.objects[xb.value()];
                        if (rx == null || rx.refindexdefpos != defpos)
                            continue;
                        rx.Cascade(cx, a, u);
                    }
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var tb = (Table)nd.objects[tabledefpos];
            if (tb != null)
            {
                var xs = BTree<RowType, long>.Empty;
                var ks = BTree<long, bool>.Empty;
                for (var b = tb.indexes.First(); b != null; b = b.Next())
                    if (b.value() != defpos)
                    {
                        var cs = b.key();
                        for (var c = cs.First(); c != null; c = c.Next())
                            ks += (c.value().Item1, true);
                        xs += (cs, b.value());
                    }
                tb += (Table.Indexes, xs);
                tb += (Table.TableCols, ks);
                nd += (tb, p);
            }
            return base.Drop(d, nd, p);
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
            if (mem.Contains(Adapter))
            {
                sb.Append(" Adapter="); sb.Append(adapter);
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
        internal override Basis _Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
        internal override Basis _Relocate(Context cx)
        {
            return this;
        }
    }
}
