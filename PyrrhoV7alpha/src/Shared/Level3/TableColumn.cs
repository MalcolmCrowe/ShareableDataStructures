using System;
using System.Globalization;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
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
    /// A Database object representing a table column
    /// </summary>
    internal class TableColumn : DBObject,IComparable
    {
        internal const long
            Checks = -268,  // BTree<long,Check>
            Generated = -269, // GenerationRule (C)
            Table = -270, // long
            UpdateAssignments = -271, // BList<UpdateAssignment>
            UpdateString = -272; // string
        /// <summary>
        /// A set of column constraints
        /// </summary>
        public BTree<long, Check> constraints => 
            (BTree<long, Check>)mem[Checks] ?? BTree<long,Check>.Empty;
        public TypedValue defaultValue => (TypedValue)mem[Domain.Default];
        public GenerationRule generated =>
            (GenerationRule)(mem[Generated] ?? GenerationRule.None);
        public bool notNull => (bool)(mem[Domain.NotNull] ?? false);
        public long tabledefpos => (long)(mem[Table] ?? -1L);
        public BList<UpdateAssignment> update =>
            (BList<UpdateAssignment>)mem[UpdateAssignments] ?? BList<UpdateAssignment>.Empty;
        public string updateString => (string)mem[UpdateString];
        public readonly static TableColumn Doc = new TableColumn(-1, BTree<long, object>.Empty);
        /// <summary>
        /// Constructor: a new TableColumn 
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="c">The PColumn def</param>
        /// <param name="dt">the data type</param>
        public TableColumn(Table tb, PColumn c, Domain dt)
            : base(c.defpos, _TableColumn(c,dt)+(Table, tb.defpos)) {}
        protected TableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TableColumn operator+(TableColumn s,(long,object)x)
        {
            return new TableColumn(s.defpos, s.mem + x);
        }
        static BTree<long,object> _TableColumn(PColumn c,Domain dt)
        {
            var r = BTree<long, object>.Empty + (Definer, c.db.role.defpos) + (_Domain, dt);
            if (c.notNull)
                r += (Domain.NotNull, true);
            if (c.generated != GenerationRule.None)
                r += (Generated, c.generated);
            Grant.Privilege priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect;
            if (c.generated == GenerationRule.None)
                priv |= Grant.Privilege.Insert | Grant.Privilege.GrantInsert;
            if (c.upd != BList<UpdateAssignment>.Empty || c.generated == GenerationRule.None)
                priv |= Grant.Privilege.Update | Grant.Privilege.GrantUpdate;
            var oi = new ObInfo(c.ppos, c.name, dt, priv);
            if (dt.defaultString != "")
                r = r + (Domain.DefaultString, dt.defaultString)
                  + (Domain.Default, dt.defaultValue);
            if (c.dv != null)
                r += (Domain.Default, c.dv);
            if (c.ups!="")
                r = r + (UpdateString, c.ups) + (UpdateAssignments, c.upd);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableColumn(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableColumn(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (TableColumn)base.Relocate(wr);
            var tb = wr.Fix(tabledefpos);
            if (tb != tabledefpos)
                r += (Table, tb);
            var ge = generated.Relocate(wr);
            if (ge != generated)
                r += (Generated, ge);
            var cs = BTree<long, Check>.Empty;
            var ch = false;
            for (var b=constraints?.First();b!=null;b=b.Next())
            {
                var k = wr.Fix(b.key());
                var c = (Check)b.value().Relocate(wr);
                ch = ch || (k != b.key()) || (c != b.value());
                cs += (k, c);
            }
            if (ch)
                r += (Checks, cs);
            var ua = BList<UpdateAssignment>.Empty;
            ch = false;
            for (var b=update.First();b!=null;b=b.Next())
            {
                var u = (UpdateAssignment)b.value().Relocate(wr);
                ch = ch || (u != b.value());
                ua += u;
            }
            if (ch)
                r += (UpdateAssignments, ua);
            return r;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(Checks,constraints+(ck.defpos,ck)));
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var tc = (TableColumn) base.Replace(cx, so, sv);
            tc = (TableColumn)cx.obs[tc.defpos];
            var dm = (Domain)tc.domain.Replace(cx,so, sv);
            if (dm != domain)
                tc += (_Domain, dm);
            var ge = (SqlValue)tc.generated.exp?.Replace(cx, so, sv);
            if (ge!=tc.generated.exp)
                tc += (Generated,new GenerationRule(tc.generated.gen,tc.generated.gfs,ge));
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = tc.update.First(); b != null; b = b.Next())
                ua += b.value().Replace(cx, so, sv);
            if (ua != tc.update)
                tc += (UpdateAssignments, ua);
            return cx.Add(tc);
        }
        /// <summary>
        /// Accessor: Check a new column notnull condition
        /// Normally fail if null values found
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="reverse">If true fail if non-null values found</param>
        internal void ColumnCheck(Transaction tr, bool reverse)
        {
            var cx = new Context(tr);
            var tb = tr.objects[tabledefpos] as Table;
            if (tb == null)
                return;
            var rt = (ObInfo)tr.role.obinfos[tb.defpos];
            var fm = new From(tr.uid, tb,rt);
            for (var rb = fm.RowSets(tr,cx).First(cx); 
                rb != null; rb = rb.Next(cx))
            {
                var v = rb.row[defpos];
                var nullfound = v == null;
                if (nullfound ^ reverse)
                {
                    var ti = (ObInfo)tr.role.obinfos[defpos];
                    throw new DBException(reverse ? "44005" : "44004", ti.name, rt.name).ISO()
                        .Add(Sqlx.TABLE_NAME, new TChar(rt.name))
                        .Add(Sqlx.COLUMN_NAME, new TChar(ti.name));
                }
            }
        }
        /// <summary>
        /// Accessor: Check a new column check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        /// <param name="signal">signal is 44003 for column check, 44001 for domain check</param>
        internal void ColumnCheck(Transaction tr, Check c, string signal)
        {
            var tb = tr.objects[tabledefpos] as Table;
            if (tb == null)
                return;
            var cx = new Level4.Context(tr);
            var rt = (ObInfo)tr.role.obinfos[tb.defpos];
            Query nf = new From(tr.uid,tb,rt).AddCondition(cx,c.search.Disjoin());
            nf = c.search.Conditions(cx, nf, false, out _);
            if (nf.RowSets(tr, cx).First(cx) != null)
            {
                var ti = (ObInfo)tr.role.obinfos[defpos];
                throw new DBException(signal, c.name, this, tb).ISO()
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar(c.name.ToString()))
                    .Add(Sqlx.COLUMN_NAME, new TChar(ti.name))
                    .Add(Sqlx.TABLE_NAME, new TChar(rt.name));
            }
        }
        internal override (Database,Role) Cascade(Database d, Database nd,Role ro, 
            Drop.DropAction a = 0,BTree<long,TypedValue>u=null)
        {
            if (a != 0)
                nd += (Database.Cascade, true);
            var tb = (Table)d.objects[tabledefpos];
            for (var b = tb?.indexes.First(); b != null; b = b.Next())
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value().defpos == defpos && nd.objects[b.value()] is Index x)
                        (nd,ro) = x.Cascade(d,nd,ro,a,u);
            tb = (Table)nd.objects[tabledefpos];
            tb += (Dependents, tb.dependents - defpos);
            nd += (tb, nd.loadpos);
            for (var b=tb.tableRows.PositionAt(0);b!=null;b=b.Next())
            {
                var rw = (TableRow)b.value();
                tb += (b.key(), rw +(TableRow.Fields, rw.fields - defpos));
            }
            nd = nd + (tb, nd.loadpos) + (ro, nd.loadpos);
            for (var b = nd.roles.First(); b != null; b = b.Next())
                if (nd.objects[b.value()] is Role rr 
                    && rr.obinfos[tabledefpos] is ObInfo oi)
                {
                    for (var c = oi.columns.First(); c != null; c = c.Next())
                        if (c.value().defpos == defpos)
                        {
                            oi -= c.value();
                            break;
                        }
                    nd += (rr + oi, nd.loadpos);
                    ro = (Role)nd.objects[ro.defpos];
                }
            return base.Cascade(d,nd,ro,a,u);
        }
        /// <summary>
        /// a readable version of the table column
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Table)) { sb.Append("Table="); sb.Append(Uid(tabledefpos)); }
            if (mem.Contains(Checks) && constraints.Count>0)
            { sb.Append(" Checks:"); sb.Append(constraints); }
            if (mem.Contains(Generated) && generated != GenerationRule.None)
            { sb.Append(" Generated="); sb.Append(generated); }
            if (mem.Contains(Domain.NotNull) && notNull) sb.Append(" Not Null");
            if (mem.Contains(UpdateString))
            {
                sb.Append(" UpdateString="); sb.Append(updateString);
                sb.Append(" Update:"); sb.Append(update);
            }
            return sb.ToString();
        }

        public int CompareTo(object obj)
        {
            return (obj is TableColumn tc) ? defpos.CompareTo(tc.defpos) : 1;
        }
    }
    internal enum Generation { No, Expression, RowStart, RowEnd, Position };
    /// <summary>
    /// Helper for parsing a GenerationRule 
    /// </summary>
    internal class GenerationRule : Basis
    {
        internal const long
            _Generation = -273, // Generation
            GenExp = -274, // SqlValue
            GenString = -275; // string
        internal readonly static GenerationRule None = new GenerationRule(Generation.No);
        public Generation gen => (Generation)(mem[_Generation] ?? Generation.No); // or START or END for ROW START|END
        public SqlValue exp => (SqlValue)mem[GenExp];
        public string gfs => (string)mem[GenString];
        public GenerationRule(Generation g) : base(new BTree<long, object>(_Generation, g)) { }
        public GenerationRule(Generation g, string s, SqlValue e)
            : base(BTree<long, object>.Empty + (_Generation, g) + (GenExp, e) + (GenString, s)) { }
        protected GenerationRule(BTree<long, object> m) : base(m) { }
        public static GenerationRule operator +(GenerationRule gr, (long, object) x)
        {
            return (GenerationRule)gr.New(gr.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GenerationRule(m);
        }
        internal override Basis Relocate(Writer wr)
        {
            var e = (SqlValue)exp?.Relocate(wr);
            return (e == exp)?this: this + (GenExp, e);
        }
        internal TypedValue Eval(Transaction tr,Context cx)
        {
            switch (gen)
            { 
                case Generation.Expression: return exp.Eval(tr, cx);
            }// or START/END
            return null;
        }
        public override string ToString()
        {
            return (gen == Generation.Expression) ? gfs : gen.ToString();
        }
    }
    /// <summary>
    /// This is a type of Selector that corresponds to subColumn that is specified in a constraint
    /// and so must be realised in the physical infrastructure. 
    /// </summary>
    internal class ColumnPath : TableColumn
    {
        internal const long
            Prev = -321; // TableColumn
        /// <summary>
        /// The prefix Selector
        /// </summary>
        public SqlValue prev => (SqlValue)mem[Prev];
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="pp">the level 2 column path information</param>
        /// <param name="rs">the set of grantees</param>
        public ColumnPath(Database db, PColumnPath pp)
            : this(pp, (TableColumn)db.objects[pp.coldefpos])
        { }
        public ColumnPath(long dp, string n, TableColumn pr, Database db)
            : base(dp, new BTree<long, object>(Prev, pr)+(Name,n)) { }
        protected ColumnPath(PColumnPath pp, TableColumn pr)
            : base(pp.ppos, BTree<long, object>.Empty + (Prev, pr)
                  + (Classification, pr.classification)+(Name,pp.Name))
        { }
        protected ColumnPath(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnPath(defpos, m);
        }
        /// <summary>
        /// Poke a value into a given document according to this ColumnPath
        /// </summary>
        /// <param name="d">The document</param>
        /// <param name="ss">The list of path components</param>
        /// <param name="i">An index into this path</param>
        /// <param name="v">the new value</param>
        /// <returns>the updated Document</returns>
        TypedValue Set(TDocument d, string[] ss, int i, TypedValue v)
        {
            var s = ss[i];
            var nd = new TDocument();
            if (i < ss.Length - 1)
            {
                var tv = d[s];
                if (tv as TDocument != null)
                    v = Set(tv as TDocument, ss, i + 1, v);
            }
            return new TDocument(d, (s, v));
        }
    }

    /// <summary>
    /// This class (new in v7) computes the current state of the TableRow and stores it in the
    /// Table for the schemaRole.
    /// </summary>
    internal class TableRow : DBObject
    {
        internal const long
            Fields = -227, // BTree<long,TypedValue>
            Prev = -276, // long
            Time = -278; // long
        public long tabledefpos => (long)mem[TableColumn.Table];
        public long time => (long)mem[Time];
        public long prev => (long)(mem[Prev] ?? -1L);
        public BTree<long, TypedValue> fields => 
            (BTree<long,TypedValue>)mem[Fields]??BTree<long,TypedValue>.Empty;
        public string provenance =>(string)mem[Domain.Provenance];
        public TableRow(Record rc,Database db,BTree<long,TypedValue>x) 
            :base(rc.ppos,
                 (rc is Update up)?up._defpos:rc.defpos,
                 db.role.defpos,_Mem(x)
                 +(TableColumn.Table,rc.tabledefpos)+(Prev,_Prev(rc))
                 +(LastChange,rc.ppos)+(Domain.Provenance,rc.provenance)+(Time,rc.time)
                 +(Fields,x))
        { }
        protected TableRow(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static TableRow operator+(TableRow r,(long,object)x)
        {
            return new TableRow(r.defpos, r.mem + x);
        }
        public static TableRow operator +(TableRow r, (long, TypedValue) x)
        {
            return new TableRow(r.defpos, r.mem + x + (Fields,r.fields+x));
        }
        static ObInfo _Type(Record rc, Database db)
        {
            return (rc.subType >= 0) ? (ObInfo)db.objects[rc.subType]
                : ((ObInfo)db.role.obinfos[rc.tabledefpos]);
        }
        static long _Prev(Record rc)
        {
            return (rc is Update up) ? up.prev : rc.ppos;
        }
        static BTree<long,object> _Mem(BTree<long,TypedValue> fl)
        {
            var r = BTree<long, object>.Empty;
            for (var b = fl.First(); b != null; b = b.Next())
                r += (b.key(), b.value());
            return r;
        }
        public static PRow MakeKey(Index x,BTree<long,TypedValue> fl)
        {
            PRow r = null;
            for (var i = (int)x.keys.Count - 1; i >= 0; i--)
            {
                var v = (TypedValue)fl[x.keys[i].defpos];
                if (v==null)
                    return x.MakeAutoKey(fl);
                r = new PRow(v, r);
            }
            return r;
        }
        public static PRow MakeKey(Index x, BTree<long, object> fl)
        {
            PRow r = null;
            for (var i = (int)x.keys.Count - 1; i >= 0; i--)
            {
                var v = (TypedValue)fl[x.keys[i].defpos];
                if (v == null)
                    return x.MakeAutoKey(fl);
                r = new PRow(v, r);
            }
            return r;
        }
        public PRow MakeKey(Index x)
        {
            PRow r = null;
            for (var i = (int)x.keys.Count - 1; i >= 0; i--)
                r = new PRow(fields[x.keys[i].defpos], r);
            return r;
        }
        public PRow MakeKey(long[] cols)
        {
            PRow r = null;
            for (var i = (int)cols.Length - 1; i >= 0; i--)
                r = new PRow(fields[cols[i]], r);
            return r;
        }
        public PRow MakeKey(BList<TableColumn> cols)
        {
            PRow r = null;
            for (var b=cols.First();b!=null;b=b.Next())
                r = new PRow(fields[b.value().defpos], r);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRow(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableRow(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
        //Handle restrict/cascade for Delete
        internal override (Database, Role) Cascade(Database db, Database nd, Role ro, Drop.DropAction a = 0,
            BTree<long, TypedValue> u = null)
        {
            if (a != 0)
                nd += (Database.Cascade, true);
            var ta = (Table)db.objects[tabledefpos];
            var px = ta.FindPrimaryIndex(db);
            for (var b = ro.dbobjects.First(); b != null; b = b.Next())
                if (nd.objects[b.value()] is Table tb)
                    for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                    {
                        var rx = (Index)nd.objects[xb.value()];
                        if (rx == null || rx.reftabledefpos != tabledefpos)
                            continue;
                        var x = (Index)db.objects[rx.refindexdefpos];
                        var pk = MakeKey(x.keys);
                        if (!rx.rows.Contains(pk))
                            continue;
                        var ca = rx.flags;
                        if (u==null) 
                            ca &= (PIndex.ConstraintType.CascadeDelete
                                |PIndex.ConstraintType.SetDefaultDelete
                                |PIndex.ConstraintType.SetNullDelete);
                        else
                            ca &= (PIndex.ConstraintType.CascadeUpdate 
                                | PIndex.ConstraintType.SetDefaultUpdate
                                | PIndex.ConstraintType.SetNullUpdate);
                        PRow dk = null;
                        if (ca == PIndex.ConstraintType.SetDefaultDelete ||
                            ca == PIndex.ConstraintType.SetDefaultUpdate)
                            for (var kb = rx.keys.Last(); kb != null; kb = kb.Previous())
                                dk = new PRow(kb.value().defaultValue, dk);
                        if (db is Transaction && ca==0 && a==0)
                            throw new DBException("23000", "RESTRICT - foreign key in use", pk);
                        nd += (Database.Cascade, true);
                        var rt = (Table)nd.objects[rx.tabledefpos];
                        for (var d = rx.rows.PositionAt(pk); d != null && d.key()._CompareTo(pk) == 0; d = d.Next())
                            if (d.Value() != null)
                            {
                                var dp = d.Value().Value;
                                var rr = (TableRow)rt.tableRows[dp];  
                                if (ca == PIndex.ConstraintType.CascadeDelete)
                                {
                                    for (var rb = rt.indexes.First(); rb != null; rb = rb.Next())
                                    {
                                        var ix = (Index)nd.objects[rb.value()];
                                        var inf = ix.rows.info;
                                        var key = rr.MakeKey(ix);
                                        ix -= key;
                                        if (ix.rows == null)
                                            ix += (Index.Tree, new MTree(inf));
                                        nd += (ix, nd.loadpos);
                                    }
                                    rt -= dp;
                                }
                                else
                                {
                                    var rz = rr;
                                    var pb = px.keys.First();
                                    var ok = rr.MakeKey(rx);
                                    for (var fb = rx.keys.First(); pb!=null && fb != null; 
                                        pb=pb.Next(),fb = fb.Next())
                                    {
                                        var q = pb.value().defpos;
                                        TypedValue v = TNull.Value;
                                        if (u?.Contains(q)!=false)
                                        {
                                            switch (ca)
                                            {
                                                case PIndex.ConstraintType.CascadeUpdate:
                                                    v = u[q]; break;
                                                case PIndex.ConstraintType.SetDefaultDelete:
                                                case PIndex.ConstraintType.SetDefaultUpdate:
                                                    v = dk[fb.key()];
                                                    break;
                                                    // otherwise SetNull cases
                                            }
                                            rr += (fb.value().defpos, v);
                                        }
                                    }
                                    if (rr != rz)
                                    {
                                        var nk = rr.MakeKey(rx);
                                        rt += (nd, rr);
                                        rx -= (ok, rr.defpos);
                                        if (nk != null && nk._head!=TNull.Value)
                                            rx += (nk, rr.defpos);
                                    }
                                }
                                nd += (rx, nd.loadpos);
                            }
                        nd += (rt, nd.loadpos);
                    }
            return (nd, ro);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table=");sb.Append(Uid(tabledefpos));
            sb.Append(" Prev=");sb.Append(Uid(prev));
            sb.Append(" Time=");sb.Append(new DateTime(time));
            sb.Append(" Fields:");sb.Append(fields.ToString());
            return sb.ToString();
        }
    }
    internal class PeriodDef : TableColumn
    {
        internal const long
            StartCol = -387, // long
            EndCol = -388; // long
        internal long startCol => (long)mem[StartCol];
        internal long endCol => (long)mem[EndCol];
        public PeriodDef(long lp, long tb, long sc, long ec, Database db)
            : base(lp, BTree<long, object>.Empty + (Table, tb) + (StartCol, sc) 
                  + (EndCol, ec) 
                  + (_Domain,((TableColumn)db.objects[sc]).domain))
        { }
        protected PeriodDef(long dp, BTree<long, object> m)
            : base(dp, m) { }
        public static PeriodDef operator +(PeriodDef p, (long, object) x)
        {
            return new PeriodDef(p.defpos, p.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodDef(defpos, m); ;
        }
        internal override DBObject Relocate(long dp)
        {
            return new PeriodDef(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            return new PeriodDef(wr.Fix(defpos), wr.Fix(tabledefpos),
                wr.Fix(startCol), wr.Fix(endCol),wr.db);
        }
    }
}
