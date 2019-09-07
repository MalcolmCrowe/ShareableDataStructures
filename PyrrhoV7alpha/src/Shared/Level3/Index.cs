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
            Adapter = -161, // Procedure 
            IndexConstraint = -162,// PIndex.ConstraintType
            Key = -163, // BList<Selector>
            KeyType = -164, // Domain
            References = -165, // BTree<long,BList<TypedValue>> computed by adapter 
            RefIndex = -166, // long
            RefTable = -167, // long
            TableDefPos = -168, // long
            Tree = -169; // MTree
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
        public Domain keyType => (Domain)mem[KeyType];
        /// <summary>
        /// for Foreign key, the referenced index
        /// </summary>
        public long refindexdefpos => (long)(mem[RefIndex] ?? -1L);
        /// <summary>
        /// for Foreign key, the referenced table
        /// </summary>
        public long reftabledefpos => (long)(mem[RefTable] ?? -1L);
        /// <summary>
        /// The TableColumns for the key
        /// </summary>
        public BList<Selector> cols => (BList<Selector>)mem[Key];
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
        public Index(PIndex c, Database db)
            : base(c.name, c.ppos, c.defpos, db.role.defpos, _IndexProps(c,db)
                 + (TableDefPos, c.tabledefpos) + (IndexConstraint, c.flags))
        { }
        static BTree<long,object> _IndexProps(PIndex c,Database db)
        {
            var r = BTree<long, object>.Empty;
            if (c.adapter!="")
            {
                r += (Adapter, new Parser(db).ParseSqlValue(c.adapter, Domain.Int));
                r += (References, BTree<long, BList<TypedValue>>.Empty);
            }
            if (c.reference>0)
            {
                var ro = (DBObject)db.schemaRole.objects[c.reference];
                if (ro is Index rx)
                {
                    if (rx.Denied(db as Transaction, Grant.Privilege.References))
                        throw new DBException("42105", rx);
                    r += (RefIndex, rx);
                    r += (RefTable, rx.tabledefpos);
                }
            }
            var cols = BList<Selector>.Empty;
            bool primary = (c.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey;
            var tb = (Table)db.role.objects[c.tabledefpos];
            for (int j = 0; j < c.columns.Count; j++)
            {
                var pos = Math.Abs(c.columns[j]);
                if (pos == 0)
                {
                    var ix = ((c.flags & PIndex.ConstraintType.SystemTimeIndex) == PIndex.ConstraintType.SystemTimeIndex) ? Sqlx.SYSTEM_TIME : Sqlx.APPLICATION;
                    var pd = tb.periodDef;
                    pos = pd.startColDefpos;
                    /*              pd = new PeriodDef(pd)
                                  {
                                      indexdefpos = c.defpos
                                  };
                                  db.Change(pd, TAction.NoOp, null); */
                }
                var ob = (Selector)db.schemaRole.objects[pos];
                cols+=(j,ob);
     /*           if (ob is ColumnPath cp)
                {
                    cp.seq = j;
                    cp.AddRef(db, this);
                    /*     if (!tb.HasColumn(db,pos)) // can happen with ColumnPath selectors
                         {
                             tb = new Table(tb);
                             tb.columns+=(pos, true);
                             db.Change(tb, TAction.NoOp, null);
                         } 
                } */
            }
            r += (Key, cols);
            TreeBehaviour isfk = (c.reference >= 0 || c.flags == PIndex.ConstraintType.NoType) ? 
                TreeBehaviour.Allow : TreeBehaviour.Disallow;
            var kt = new Domain(cols);
            r += (KeyType, kt);
            var rows = new MTree(new TreeInfo(kt, isfk, isfk));
            r += (Tree, rows);
            return r;
        }
        public static Index operator+(Index x,(long,object)v)
        {
            return (Index)x.New(x.mem + v);
        }
        public static Index operator-(Index x,PRow k)
        {
            return x + (Tree, x.rows - k);
        }
        public static Index operator+(Index x,TableRow row)
        {
            return x+(Tree,x.rows+(TableRow.MakeKey(x, row.fields), row.defpos));
        }
        public static Index operator-(Index x,TableRow row)
        {
            return x + (Tree, x.rows - TableRow.MakeKey(x, row.fields));
        }
        /// <summary>
        /// Creator: an Index by modifying this from tableRows/versionedRows information
        /// </summary>
        /// <param name="db">The database</param>
        /// <returns>the new Index</returns>
        public Index Build(Database db)
        {
            Table tb = (Table)db.schemaRole.objects[tabledefpos];
            MTree rs = rows;
            bool rx = ((flags & PIndex.ConstraintType.ForeignKey) == PIndex.ConstraintType.ForeignKey);
            bool ux = ((flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != PIndex.ConstraintType.NoType);
            if ((flags & PIndex.ConstraintType.SystemTimeIndex) == PIndex.ConstraintType.SystemTimeIndex)
            {
                // we construct the temporal index by digging through the history
                if (tb.versionedRows != null)
                    for (var e = tb.versionedRows.First();e!= null;e=e.Next())
                    {
                        var r = tb.tableRows[e.key()];
                        if (e.value().start!=null && !e.value().start.IsNull)
                            rs += (new PRow(e.value().start), r.ppos);
                    }
            }
            else if ((flags & PIndex.ConstraintType.ApplicationTimeIndex) == PIndex.ConstraintType.ApplicationTimeIndex)
            {
                // we construct the temporal index by digging through the history
                if (tb.versionedRows != null)
                    for(var e = tb.versionedRows.First(); e!= null;e=e.Next())
                    {
                        var r = tb.tableRows[e.key()];
                        rs += (r.MakeKey(this), r.ppos);
                    }
            }
            else if (tb.tableRows == null)
            {
                Index px = tb.FindPrimaryIndex();
                if (px != null)
                    for (var d = px.rows.First();d!= null;d=d.Next())
                    {
                        long pp = d.Value().Value;
                        var r = (TableRow)db.schemaRole.objects[pp];
                        var m = r.MakeKey(px);
                        if (m != null)
                        {
                            if (rx)
                                CheckRef(db, m);
                            if (ux && rs.Contains(m))
                                throw new DBException("44002", "PRIMARY/UNIQUE", tb.name).Mix()
                                    .Add(Sqlx.TABLE_NAME, new TChar(tb.name))
                                    .Add(Sqlx.CONSTRAINT_NAME, new TChar("PRIMARY/UNIQUE"));
                            rs += (m, pp);
                        }
                    }
            }
            else
            {
                // there is no primary index, so we do it from the tableRows information
                for (var pp = tb.tableRows.First();pp!= null;pp=pp.Next())
                {
                    var pos = pp.key();
                    var r = (TableRow)db.schemaRole.objects[pos];
                    var m = r.MakeKey(this);
                    if (m != null)
                    {
                        if (rx)
                            CheckRef(db, m);
                        if (ux && rs.Contains(m))
                            throw new DBException("44002", "PRIMARY/UNIQUE").Mix()
                                  .Add(Sqlx.TABLE_NAME, new TChar(tb.name))
                                  .Add(Sqlx.CONSTRAINT_NAME, new TChar("PRIMARY/UNIQUE"));
                        rs += (m, pos);
                    }
                }
            }
            return this+(Tree,rs);
        }
        internal PRow MakeAutoKey(BTree<long, TypedValue> fl)
        {
            var r = BList<TypedValue>.Empty;
            for (var i = 0; i < (int)cols.Count; i++)
            {
                var v = fl[cols[i].defpos];
                if (v != null)
                    r += v;
                else
                {
                    if (cols[i].domain.kind != Sqlx.INTEGER)
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
        void CheckRef(Database db,PRow m)
        {
            if (!(db is Transaction))
                return;
            Index rx = (Index)db.schemaRole.objects[refindexdefpos];
            var tb = (Table)db.schemaRole.objects[reftabledefpos];
            if (!rx.rows.Contains(m))
                throw new DBException("44002", "REFERENCES", rx.name).Mix()
                    .Add(Sqlx.TABLE_NAME, new TChar(tb.name))
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar("REFERENCES"));
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
            int j;
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
            if (m!=null && da is Transaction && rows.Contains(m))
            {
                Table tb = (Table)da.schemaRole.objects[tabledefpos];
                xmess = "duplicate key " + tb.name + " " + m.ToString();
                okay = false;
            }
        }
        bool MatchCols(long[]a,long[]b)
        {
            if (a.Length != b.Length)
                return false;
            for (int i = 0; i < a.Length; i++)
                if (a[i] != b[i])
                    return false;
            return true;
        }
        /// <summary>
        /// Check depndencies for rename and delete operations
        /// </summary>
        /// <param name="t">The rename or drop transaction</param>
        /// <param name="act">RESTRICT, CASCADE or NAME</param>
        /// <returns></returns>
        public override Sqlx Dependent(Transaction tr,Context cx)
        {
            // we drop indexes if the table or key TableColumns are dropped
            if (tr.refObj.defpos == refindexdefpos)
                return Sqlx.RESTRICT;
            if (tr.refObj.defpos == tabledefpos)
                return Sqlx.DROP;
            for (int j = 0; j < cols.Count; j++)
                if (tr.refObj.defpos == cols[j].defpos)
                    return (cols.Count == 1) ? Sqlx.DROP : Sqlx.RESTRICT;
/*            var adp = adapter;
            if (adp==null)
                return Sqlx.NO;
            var bi = tr.context.blockid;
            var q = new Query(tr, bi, keyType);
            var oq = q.Push(tr);
            try
            {
                SqlValue.Setup(tr,q,adp, Domain.Bool);
                q.Conditions(tr, q);
                for (var a = tr.context.refs.First(); a != null; a = a.Next())
                    if (a.value() == tr.refObj.defpos)
                        return Sqlx.RESTRICT;
            }
            catch (DBException e) { throw e; }
            finally { q.Pop(tr,oq); } */
            return Sqlx.NO;
        }
        /// <summary>
        /// Find the location in the key for a given tableColumn
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        public int PosFor(TableColumn c)
        {
            int r;
            for (r = 0; r < (int)cols.Count; r++)
                if (cols[r].defpos == c.defpos)
                    return r;
            return -1;
        }

        /// <summary>
        /// A readable version of the Index
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Key:");sb.Append(cols);
            sb.Append(" Kind=");sb.Append(flags);
            if (refindexdefpos != -1) {
                sb.Append(" RefIndex="); sb.Append(Uid(refindexdefpos));
                sb.Append(" RefTable="); sb.Append(Uid(reftabledefpos));
            }
            sb.Append(" Rows:");sb.Append(rows);
            sb.Append(" KeyType:"); sb.Append(keyType);
            if (mem.Contains(Adapter)) { 
                sb.Append(" Adapter="); sb.Append(adapter.name);
                sb.Append(" References:"); sb.Append(references);
            }
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Index(defpos, m);
        }
    }

    internal class IndexCursor
    {
        internal readonly Database _db;
        internal readonly Index _index;
        internal readonly BTree<long, TypedValue> _match;
        internal readonly MTreeBookmark _bmk;
        internal readonly TableRow _rec;
        IndexCursor(Database db, Index index, BTree<long, TypedValue> match, MTreeBookmark bmk, TableRow rec = null)
        {
            _db = db; _index = index; _match = match; _bmk = bmk;  _rec = rec;
        }
        static TableRow MoveToMatch(Database _db, BTree<long, TypedValue> _match, ref MTreeBookmark bmk)
        {
            for (; bmk != null; bmk = bmk.Next())
            {
                var r = (TableRow)_db.schemaRole.objects[bmk.Value().Value];
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
            var r = MoveToMatch(_db,_match,ref bmk);
            if (r == null)
                return null;
            return new IndexCursor(_db, _index, _match, bmk, r);
        }
        internal static IndexCursor New(Database db,Index index,BTree<long,TypedValue> match)
        {
            var bmk = index.rows.First();
            var r = MoveToMatch(db, match, ref bmk);
            if (r == null)
                return null;
            return new IndexCursor(db, index, match, bmk, r);
        }
    }

}
