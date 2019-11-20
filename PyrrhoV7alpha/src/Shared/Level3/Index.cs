using System;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Level4; // for rename/drop
using Pyrrho.Common;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

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
            Adapter = -157, // Procedure 
            IndexConstraint = -158,// PIndex.ConstraintType
            Keys = -159, // BList<TableColumn>
            References = -160, // BTree<long,BList<TypedValue>> computed by adapter
            RefIndex = -161, // long
            RefTable = -162, // long
            TableDefPos = -163, // long
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
        public CList<TableColumn> keys => (CList<TableColumn>)mem[Keys]??CList<TableColumn>.Empty;
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
        public Procedure adapter => (Procedure)mem[Adapter];
        /// <summary>
        /// The references as computed by the adapter function if any
        /// </summary>
        public BTree<long, BList<TypedValue>> references =>
            (BTree<long, BList<TypedValue>>)mem[References];
        /// <summary>
        /// Constructor: for a new rows tree on an old Index
        /// </summary>
        /// <param name="x">The old index</param>
        /// <param name="r">The new rows tree</param>
        public Index(Index x, MTree r, BTree<long, BTree<long, bool>> p)
            : base(x.defpos, x.mem + (Tree, r))
        {
            _nindex = x._nindex;
        }
        public Index(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// Constructor: a new Index from the datafile
        /// </summary>
        /// <param name="tb">The level 3 database</param>
        /// <param name="c">The level 2 index</param>
        public Index(PIndex c, ref Database db)
            : base(c.name, c.ppos, c.defpos, db.role.defpos, _IndexProps(c, ref db)
                 + (TableDefPos, c.tabledefpos) + (IndexConstraint, c.flags))
        { }
        static BTree<long, object> _IndexProps(PIndex c, ref Database db)
        {
            var r = BTree<long, object>.Empty;
            if (c.adapter != "")
            {
                r += (Adapter, new Parser(db).ParseSqlValue(c.adapter));
                r += (References, BTree<long, BList<TypedValue>>.Empty);
            }
            if (c.reference > 0)
            {
                var rx = (Index)db.objects[c.reference];
                var rp = rx.tabledefpos;
                var rt = (Table)db.objects[rp];
                if (rx!=null)
                {
                    r += (RefIndex, rx.defpos);
                    r += (RefTable, rx.tabledefpos);
                }
            }
            var cols = CList<TableColumn>.Empty;
            var tb = (Table)db.objects[c.tabledefpos];
            for (int j = 0; j < c.columns.Count; j++)
            {
                var pos = Math.Abs(c.columns[j].defpos);
                if (pos == 0)
                {
                    var pd = (PeriodDef)db.objects[tb.systemPS];
                    pos = pd.startCol;
                }
                cols += (TableColumn)db.objects[pos];
            }
            TreeBehaviour isfk = (c.reference >= 0 || c.flags == PIndex.ConstraintType.NoType) ?
                TreeBehaviour.Allow : TreeBehaviour.Disallow;
            r += (Keys, cols);
            var rows = new MTree(new TreeInfo(cols, isfk, isfk));
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
        internal PRow MakeKey(BTree<long,TypedValue> fl)
        {
            PRow r = null;
            for (var b = keys.Last(); b != null; b = b.Previous())
                r = new PRow(fl[b.value().defpos], r);
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
                    var r = (TableRow)db.objects[pp];
                    var m = r.MakeKey(px);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(db, m);
                        if (ux && rs.Contains(m))
                        {
                            var oi = (ObInfo)db.role.obinfos[tb.defpos];
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
                            var oi = (ObInfo)db.role.obinfos[tb.defpos];
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
        internal PRow MakeAutoKey(BTree<long, object> fl)
        {
            var r = BList<TypedValue>.Empty;
            for (var b = keys.First(); b!=null; b=b.Next())
            {
                var kc = b.value();
                var v = (TypedValue)fl[kc.defpos];
                if (v != null)
                    r += v;
                else
                {
                    if (kc.domain.kind != Sqlx.INTEGER)
                        throw new DBException("22004");
                    v = rows.NextKey(r, 0, b.key());
                    if (v == TNull.Value)
                        v = new TInt(0);
                    r += v;
                }
            }
            return new PRow(r);
        }
        internal PRow MakeAutoKey(BTree<long, TypedValue> fl)
        {
            var r = BList<TypedValue>.Empty;
            for (var i = 0; i < (int)keys.Count; i++)
            {
                var v = fl[keys[i].defpos];
                if (v != null)
                    r += v;
                else
                {
                    if (keys[i].domain.kind != Sqlx.INTEGER)
                        throw new DBException("22004");
                    v = rows.NextKey(r, 0, i);
                    if (v == TNull.Value)
                        v = new TInt(0);
                    r += v;
                }
            }
            return new PRow(r);
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
                var oi = (ObInfo)db.role.obinfos[tb.defpos];
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
                var oi = (ObInfo)da.role.obinfos[tabledefpos];
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
                if (keys[r].defpos == c.defpos)
                    return r;
            return -1;
        }
        internal override (Database, Role) Cascade(Database d, Database nd, Role ro, 
            Drop.DropAction a = Drop.DropAction.Restrict, BTree<long, TypedValue> u = null)
        {
            if (a != 0)
                nd += (Database.Cascade,true);
            var td = (Table)nd.objects[tabledefpos];
            nd += (td + (Table.Indexes, td.indexes - keys), nd.loadpos);
            if (reftabledefpos >= 0 && nd.objects[reftabledefpos] is Table ta)
            {
                var px = ta.FindPrimaryIndex(nd);
                nd += (px + (Dependents, px.dependents - defpos), nd.loadpos);
            }
            for (var b = ro.dbobjects.First(); b != null; b = b.Next())
                if (d.objects[b.value()] is Table tb)
                    for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                    {
                        var rx = (Index)d.objects[xb.value()];
                        if (rx == null || rx.refindexdefpos != defpos)
                            continue;
                        if (a==Drop.DropAction.Restrict)
                            throw new DBException("23001", "Primary index "+Uid(defpos),Uid(tb.defpos),Uid(rx.defpos));
                        (nd, ro) = rx.Cascade(d, nd, ro, a, u);
                    }
            return base.Cascade(d, nd, ro, a, u);
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
            sb.Append(" KeyType:"); sb.Append(keys);
            if (mem.Contains(Adapter))
            {
                sb.Append(" Adapter="); sb.Append(adapter.defpos);
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
        internal override Basis Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
    internal class IndexCursor
    {
        internal readonly Index _index;
        internal readonly Table _table;
        internal readonly BTree<long, TypedValue> _match;
        internal readonly MTreeBookmark _bmk;
        internal readonly TableRow _rec;
        IndexCursor(Table tb,Index index, BTree<long, TypedValue> match, MTreeBookmark bmk, TableRow rec = null)
        {
            _index = index; _match = match; _bmk = bmk;  _rec = rec;
            _table = tb;
        }
        static TableRow MoveToMatch(Table _table, BTree<long, TypedValue> _match, ref MTreeBookmark bmk)
        {
            for (; bmk != null; bmk = bmk.Next())
            {
                var r = (TableRow)_table.tableRows[bmk.Value().Value];
                for (var m = _match.First(); m != null; m = m.Next())
                    if (m.value().CompareTo(r.fields[m.key()]) != 0)
                        goto next;
                return r;
                next:;
            }
            return null;
        }
        public IndexCursor Next()
        {
            var bmk = _bmk?.Next();
            var r = MoveToMatch(_table,_match,ref bmk);
            if (r == null)
                return null;
            return new IndexCursor(_table, _index, _match, bmk, r);
        }
        internal static IndexCursor New(Table tb,Index index,BTree<long,TypedValue> match)
        {
            var bmk = index.rows.First();
            var r = MoveToMatch(tb, match, ref bmk);
            if (r == null)
                return null;
            return new IndexCursor(tb, index, match, bmk, r);
        }
    }

}
