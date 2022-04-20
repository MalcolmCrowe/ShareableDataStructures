using System;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Configuration;
using System.Runtime.CompilerServices;
using System.Xml.Linq;
using System.Xml.Schema;
using System.Net;
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
    /// A Database object representing a table column
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TableColumn : DBObject
    {
        internal const long
            Checks = -268,  // CTree<long,bool> Check
            Generated = -269, // GenerationRule (C)
            _Table = -270, // long
            UpdateAssignments = -271, // CTree<UpdateAssignment,bool>
            UpdateString = -272; // string
        /// <summary>
        /// A set of column constraints
        /// </summary>
        public CTree<long, bool> constraints => 
            (CTree<long, bool>)mem[Checks] ?? CTree<long,bool>.Empty;
        public TypedValue defaultValue => (TypedValue)mem[Domain.Default]??TNull.Value;
        public GenerationRule generated =>
            (GenerationRule)(mem[Generated] ?? GenerationRule.None);
        public bool notNull => (bool)(mem[Domain.NotNull] ?? false);
        public long tabledefpos => (long)(mem[_Table] ?? -1L);
        public CTree<UpdateAssignment,bool> update =>
            (CTree<UpdateAssignment,bool>)mem[UpdateAssignments] 
            ?? CTree<UpdateAssignment,bool>.Empty;
        public string updateString => (string)mem[UpdateString];
        public readonly static TableColumn Doc = new TableColumn(-1, BTree<long, object>.Empty);
        /// <summary>
        /// Constructor: a new TableColumn 
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="c">The PColumn def</param>
        /// <param name="dt">the obs type</param>
        public TableColumn(Table tb, PColumn c, Domain dt,Role ro)
            : base(c.defpos, _TableColumn(c,dt,ro)+(_Table, tb.defpos) + (LastChange, c.ppos)) {}
        /// <summary>
        /// Ad hoc TableColumn for LogRows, LogRowCol
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="tb"></param>
        /// <param name="nm"></param>
        /// <param name="dt"></param>
        internal TableColumn(Context cx,Table tb,string nm,Domain dt)
            :base(cx.GetUid(),BTree<long,object>.Empty+(Name,nm)+(_Table,tb.defpos)
                 + (_Domain,dt.defpos))
        {
            cx.Add(this);
        }
        protected TableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TableColumn operator+(TableColumn s,(long,object)x)
        {
            return new TableColumn(s.defpos, s.mem + x);
        }
        static BTree<long,object> _TableColumn(PColumn c,Domain dt,Role ro)
        {
            var r = BTree<long, object>.Empty + (Definer,ro.defpos) 
                + (_Domain, dt.defpos) + (_Framing,c.framing) + (LastChange,c.ppos);
            if (c.notNull)
                r += (Domain.NotNull, true);
            if (c.generated != GenerationRule.None)
                r += (Generated, c.generated);
            if (dt.defaultString != "")
                r = r + (Domain.DefaultString, dt.defaultString)
                  + (Domain.Default, dt.defaultValue);
            if (dt.sensitive)
                r += (Sensitive, true);
            if (!c.dv.IsNull)
                r += (Domain.Default, c.dv);
            if (c.ups!="")
                r = r + (UpdateString, c.ups) + (UpdateAssignments, c.upd);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableColumn(defpos,m);
        }
        internal override DBObject Instance(Iix lp,Context cx, Domain q,BList<Ident>cs=null)
        {
            var r = base.Instance(lp, cx);
            cx.instances += (r.defpos, lp);
            for (var b = constraints.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Check ck)
                    ck.Instance(lp, cx, q);
            return r;
        }
        internal override (DBObject, Ident) _Lookup(Iix lp, Context cx, string nm, Ident n)
        {
            var ci = cx.Inf(defpos);
            SqlValue r = new SqlCopy(lp, cx, nm,-1L,cx.obs[defpos])
                + (_Domain,ci.domain);
            cx.Add(r);
            for (; n != null && ci.dataType.rowType != CList<long>.Empty; n=n.sub)
            {
                var ti = cx.Inf(ci.dataType.structure);
                if (ti.names.Contains(n.ident))
                {
                    var cp = ti.names[n.ident];
                    ci = cx.Inf(cp);
                    r = new SqlField(cx.GetIid(), n.ident, r.defpos, 
                        ci.dataType, cp);
                    cx.Add(r);
                }
                else break;
            }
            return (r, n);
        }
        internal override Domain Domains(Context cx, Grant.Privilege pr = Grant.Privilege.NoPrivilege)
        {
            return cx.Inf(defpos).dataType;
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableColumn(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (TableColumn)base._Relocate(cx);
            r += (_Table, cx.Fix(tabledefpos));
            r += (Generated, generated._Relocate(cx));
            r += (Checks, cx.Fix(constraints));
            r += (UpdateAssignments, cx.Fix(update));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (TableColumn)base._Fix(cx);
            var nd = cx.Fix(domain);
            if (nd!=domain)
                r += (_Domain, nd);
            var nt = cx.Fix(tabledefpos);
            if (nt != tabledefpos)
                r += (_Table, nt);
            var ng = generated.Fix(cx);
            if (ng != generated)
                r += (Generated, ng);
            var nc = cx.Fix(constraints);
            if (nc != constraints)
                r += (Checks, nc);
            var nu = cx.Fix(update);
            if (nu != update)
                r += (UpdateAssignments, nu);
            return r;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(Checks,constraints+(ck.defpos,true)));
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var tc = (TableColumn) base._Replace(cx, so, sv);
            var dm = cx.ObReplace(domain,so, sv);
            if (dm != domain)
                tc += (_Domain, dm);
            if (tc.generated.exp != -1L)
            {
                var go = tc.generated.exp;
                var ge = (SqlValue)cx._Replace(go, so, sv);
                if (ge != cx._Ob(go))
                    tc += (Generated, new GenerationRule(tc.generated.gen, 
                        tc.generated.gfs, ge, tc.defpos));
            }
            var ua = CTree<UpdateAssignment,bool>.Empty;
            for (var b = tc.update.First(); b != null; b = b.Next())
                ua += (b.key().Replace(cx, so, sv),true);
            if (ua != tc.update)
                tc += (UpdateAssignments, ua);
            if (tc!=this)
                tc = (TableColumn)New(cx, tc.mem);
            cx.done += (defpos, tc);
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
            var fm = new From(new Ident("", cx.Ix(tr.uid)),new Context(tr),tb);
            for (var rb = fm.First(cx); 
                rb != null; rb = rb.Next(cx))
            {
                var v = rb[defpos];
                var nullfound = v == null;
                if (nullfound ^ reverse)
                {
                    var ti = (ObInfo)tr.role.infos[tabledefpos];
                    var ci = (ObInfo)tr.role.infos[defpos];
                    throw new DBException(reverse ? "44005" : "44004", ti.name, ci.name).ISO()
                        .Add(Sqlx.TABLE_NAME, new TChar(ci.name))
                        .Add(Sqlx.COLUMN_NAME, new TChar(ti.name));
                }
            }
        }
        /// <summary>
        /// Accessor: Check a new column check constraint
        /// </summary>
        /// <param name="c">The new Check constraint</param>
        /// <param name="signal">signal is 44003 for column check, 44001 for domain check</param>
        internal void ColumnCheck(Transaction tr, Check c, string signal)
        {
            var tb = tr.objects[tabledefpos] as Table;
            if (tb == null)
                return;
            var cx = new Context(tr);
            var sch = (SqlValue)cx.obs[c.search];
            var nf = (From)new From(new Ident("", cx.Ix(tr.uid)),cx,tb)
                .New(cx, BTree<long, object>.Empty+(RowSet._Where, sch.Disjoin(cx)));
            if (nf.First(cx) != null)
            {
                var ti = cx.Inf(tabledefpos);
                var ci = cx.Inf(defpos);
                throw new DBException(signal, c.name, this, tb).ISO()
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar(c.name.ToString()))
                    .Add(Sqlx.COLUMN_NAME, new TChar(ci.name))
                    .Add(Sqlx.TABLE_NAME, new TChar(ti.name));
            }
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0,BTree<long,TypedValue>u=null)
        {
            base.Cascade(cx, a, u);
            var tb = (Table)cx.db.objects[tabledefpos];
            for (var b = tb?.indexes.First(); b != null; b = b.Next())
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() == defpos && cx.db.objects[b.value()] is Index x)
                        x.Cascade(cx,a,u);      
        }
        internal override Database Drop(Database d, Database nd,long p)
        {
            var tb = (Table)nd.objects[tabledefpos];
            if (tb != null)
            {
                for (var b = nd.roles.First(); b != null; b = b.Next())
                {
                    var ro = (Role)nd.objects[b.value()];
                    if (ro.infos[defpos] is ObInfo ci && ro.infos[tabledefpos] is ObInfo ti)
                    {
                        ti += (ObInfo._DataType, ti.dataType - ci.defpos);
                        ro += (ti,false);
                        nd += (ro, p);
                    }
                }
                tb += (Table.TableCols, tb.tblCols - defpos);
                tb += (Dependents, tb.dependents - defpos);
                nd += (tb, nd.loadpos);
                for (var b = tb.tableRows.First(); b != null; b = b.Next())
                {
                    var rw = b.value();
                    tb += (b.key(), rw - defpos);
                }
            }
            return base.Drop(d, nd,p);
        }
        internal override Database DropCheck(long ck, Database nd,long p)
        {
            return nd + (this + (Checks, constraints - ck),p);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.values += (defpos, v);
        }
        /// <summary>
        /// a readable version of the table column
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(Uid(domain));
            if (mem.Contains(_Table)) { sb.Append(" Table="); sb.Append(Uid(tabledefpos)); }
            if (mem.Contains(Checks) && constraints.Count>0)
            { sb.Append(" Checks:"); sb.Append(constraints); }
            if (mem.Contains(Generated) && generated != GenerationRule.None)
            { sb.Append(" Generated="); sb.Append(generated); }
            if (mem.Contains(Domain.NotNull) && notNull) sb.Append(" Not Null");
            if (defaultValue!=null && 
              ((!defaultValue.IsNull) || PyrrhoStart.VerboseMode))
            { sb.Append(" colDefault "); sb.Append(defaultValue); }
            if (mem.Contains(UpdateString))
            {
                sb.Append(" UpdateString="); sb.Append(updateString);
                sb.Append(" Update:"); sb.Append(update);
            }
            return sb.ToString();
        }
    }
    internal enum Generation { No, Expression, RowStart, RowEnd, Position };
    /// <summary>
    /// Helper for GenerationRule
    /// At end of parsing, tc.gen.framing matches pc.generated.framing. 
    /// In case the transaction continues uncommitted, both should be relocated in RdrClose
    /// to fix any heap uids
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class GenerationRule : Basis
    {
        internal const long
            _Generation = -273, // Generation
            GenExp = -274, // long
            GenString = -275; // string
        internal readonly static GenerationRule None = new GenerationRule(Generation.No);
        public Generation gen => (Generation)(mem[_Generation] ?? Generation.No); // or START or END for ROW START|END
        public long exp => (long)(mem[GenExp]??-1L);
        public string gfs => (string)mem[GenString];
        public long target => (long)(mem[From.Target] ?? -1L);
        public GenerationRule(Generation g) : base(new BTree<long, object>(_Generation, g)) { }
        public GenerationRule(Generation g, string s, SqlValue e, long t)
            : base(BTree<long, object>.Empty + (_Generation, g) + (GenExp, e.defpos) + (GenString, s)
                  +(From.Target,t)) { }
        protected GenerationRule(BTree<long, object> m) : base(m) { }
        public static GenerationRule operator +(GenerationRule gr, (long, object) x)
        {
            return (GenerationRule)gr?.New(gr.mem + x)??None;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GenerationRule(m);
        }
        internal override Basis _Relocate(Context cx)
        {
            if (exp < 0)
                return this;
            return this + (GenExp, cx.Fix(exp));
        }
        internal override Basis _Fix(Context cx)
        {
            var r = this;
            var ne = cx.Fix(exp);
            if (exp !=ne)
                r += (GenExp, ne);
            return r;
        }
        internal TypedValue Eval(Context cx)
        {
            switch (gen)
            { 
                case Generation.Expression:
                    var e = cx.obs[exp];
                    return e.Instance(cx.GetIid(),cx,cx._Dom(e)).Eval(cx);
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
    /// // shareable as of 26 April 2021
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
                  + (Classification, pr.classification))
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
    /// Table. 
    /// It is Role-independent, so it doesn't follow the representation of any domain 
    /// and therefore can't subclass TRow.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TableRow
    {
        internal readonly long defpos;
        internal readonly long time;
        internal readonly long tabledefpos;
        internal readonly long owner;
        internal readonly long user;
        internal readonly long ppos;
        internal readonly long prev;
        internal readonly string provenance;
        internal readonly long subType;
        internal readonly Level classification;
        internal readonly CTree<long, TypedValue> vals;
        public TableRow(Record rc, Database db)
        {
            defpos = rc.defpos;
            time = rc.time; user = db.user.defpos; provenance = rc.provenance;
            tabledefpos = rc.tabledefpos;
            subType = rc.subType;
            classification = rc.classification ?? Level.D;
            owner = db.user.defpos;
            ppos = rc.ppos;
            prev = rc.ppos;
            vals = rc.fields;
        }
        public TableRow(Update up, Database db, TableRow old, Level lv=null)
        {
            defpos = up.defpos;
            time = up.time; user = db.user.defpos; provenance = up.provenance;
            tabledefpos = up.tabledefpos;
            classification = lv ?? old.classification ?? Level.D;
            subType = up.subType;
            ppos = up.ppos;
            prev = up.prev;
            var v = old.vals;
            for (var b = up.fields.First(); b != null; b = b.Next())
                if (b.value().IsNull)
                    v -= b.key();
                else
                    v += (b.key(), b.value());
            vals = v;
        }
        protected TableRow(TableRow r,CTree<long,TypedValue> vs)
        {
            defpos = r.defpos;
            time = r.time; user = r.user; provenance = r.provenance;
            tabledefpos = r.tabledefpos;
            classification = r.classification;
            ppos = r.ppos;
            subType = r.subType;
            prev = r.prev;
            vals = vs;
        }
        internal TableRow(Table tb,Cursor c)
        {
            (defpos,prev) = c._ds[tb.defpos];
            subType = c.dataType.defpos;
            classification = tb.classification;
            tabledefpos = tb.defpos;
            vals = c.values;
        }
        protected TableRow(long vp,CTree<long,TypedValue> vs)
        {
            var dp = (long)vs[DBObject.Defpos].ToLong();
            var pp = vs[DBObject.LastChange]?.ToLong() ?? 01L;
            defpos = dp;
            time = DateTime.Now.Ticks;
            user = -1L;
            provenance = "";
            tabledefpos = vp;
            subType = -1L;
            classification = Level.D;
            ppos = dp;
            prev = pp;
            vals = vs;
        }
        public static TableRow operator+(TableRow r,(long,TypedValue)x)
        {
            return new TableRow(r, r.vals + x);
        }
        public static TableRow operator-(TableRow r,long p)
        {
            return new TableRow(r, r.vals -p);
        }
        internal void Cascade(TableActivation cx, CTree<long, TypedValue> u = null)
        {
            var db = cx.db;
            //       var fr = (TableRowSet)cx.next.obs[cx._fm.defpos];
            var tb = (Table)cx.obs[cx._fm.target];
            for (var ib = tb.indexes.First(); ib != null; ib = ib.Next())
            {
                var ik = ib.key();
                var rx = (Index)db.objects[ib.value()];
                if (db.objects[rx.refindexdefpos] is Index _rx)
                {
                    var pk = _rx.MakeKey(vals);
                    var ku = BTree<long, UpdateAssignment>.Empty;
                    if (u != null)
                    {
                        for (var xb = _rx.keys.First(); xb != null; xb = xb.Next())
                        {
                            var p = xb.value();
                            var q = rx.keys[xb.key()];
                            TypedValue v = TNull.Value;
                            switch (rx.flags & PIndex.Updates)
                            {
                                case PIndex.ConstraintType.CascadeUpdate:
                                    v = u[p]; break;
                                case PIndex.ConstraintType.SetDefaultUpdate:
                                    v = ((DBObject)db.objects[p]).Domains(cx).defaultValue; break;
                                default:
                                    continue;
                            }
                            ku += (q, new UpdateAssignment(q, v));
                        }
                        if (ku == BTree<long, UpdateAssignment>.Empty) // not updating a key
                            return;
                        cx.updates += ku;
                    }
                    var restrict = (cx._tty == PTrigger.TrigType.Delete && rx.flags.HasFlag(PIndex.ConstraintType.RestrictDelete))
                        || (cx._tty == PTrigger.TrigType.Update && rx.flags.HasFlag(PIndex.ConstraintType.RestrictUpdate)
                        && (cx._cx as TableActivation)?._tty != PTrigger.TrigType.Delete);
                    for (var d = rx.rows.PositionAt(pk); d != null && d.key().CompareTo(pk) == 0;
                        d = d.Next())
                    {
                        if (restrict)
                            throw new DBException("23000", "RESTRICT - foreign key in use", pk);
                        cx.next.cursors += cx.cursors;
                        if (cx._trs.At(cx, d.Value().Value, u) is Cursor cu)
                        {
                            cx.next.cursors += (cx._trs.data, cu);
                            cx.EachRow((int)d._pos);
                        }
                    }
                }
            }
        }
        public PRow MakeKey(Index x)
        {
            PRow r = null;
            for (var i = (int)x.keys.Count - 1; i >= 0; i--)
                r = new PRow(vals[x.keys[i]], r);
            return r;
        }
        public PRow MakeKey(long[] cols)
        {
            PRow r = null;
            for (var i = cols.Length - 1; i >= 0; i--)
                r = new PRow(vals[cols[i]], r);
            return r;
        }
        public PRow MakeKey(CList<long> cols)
        {
            PRow r = null;
            for (var b = cols.First(); b != null; b = b.Next())
                r = new PRow(vals[b.value()], r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table=");sb.Append(DBObject.Uid(tabledefpos));
            sb.Append(" Prev=");sb.Append(DBObject.Uid(prev));
            sb.Append(" Time=");sb.Append(new DateTime(time));
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class RemoteTableRow : TableRow 
    {
        internal readonly string url;
        internal readonly RestRowSet rrs;
        public RemoteTableRow(long dp,CTree<long,TypedValue> v,string u,RestRowSet r) 
            : base(r.target,v+(DBObject.Defpos,new TInt(dp)))
        {
            url = u;
            rrs = r;
        }
    }
    internal class PeriodDef : TableColumn
    {
        internal const long
            StartCol = -387, // long TableColumn
            EndCol = -388; // long TableColumn
        internal long startCol => (long)mem[StartCol];
        internal long endCol => (long)mem[EndCol];
        public PeriodDef(long lp, long tb, long sc, long ec, Database db)
            : base(lp, BTree<long, object>.Empty + (_Table, tb) + (StartCol, sc) 
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
        internal override Basis _Relocate(Context cx)
        {
            return new PeriodDef(cx.Fix(defpos), cx.Fix(tabledefpos),
                cx.Fix(startCol), cx.Fix(endCol),cx.db);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = new PeriodDef(cx.Fix(defpos), 
                cx.Fix(tabledefpos),
                cx.Fix(startCol), 
                cx.Fix(endCol), cx.db);
            return r;
        }
    }
}
