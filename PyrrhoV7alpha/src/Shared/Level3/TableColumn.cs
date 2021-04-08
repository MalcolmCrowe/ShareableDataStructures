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
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// </summary>
    internal class TableColumn : DBObject
    {
        internal const long
            Checks = -268,  // CTree<long,bool> Check
            Generated = -269, // GenerationRule (C)
            Table = -270, // long
            UpdateAssignments = -271, // CTree<UpdateAssignment,bool>
            UpdateString = -272; // string
        /// <summary>
        /// A set of column constraints
        /// </summary>
        public CTree<long, bool> constraints => 
            (CTree<long, bool>)mem[Checks] ?? CTree<long,bool>.Empty;
        public TypedValue defaultValue => (TypedValue)mem[Domain.Default]??TNull.Value.New(domain);
        public GenerationRule generated =>
            (GenerationRule)(mem[Generated] ?? GenerationRule.None);
        public bool notNull => (bool)(mem[Domain.NotNull] ?? false);
        public long tabledefpos => (long)(mem[Table] ?? -1L);
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
        /// <param name="dt">the data type</param>
        public TableColumn(Table tb, PColumn c, Domain dt,Role ro)
            : base(c.defpos, _TableColumn(c,dt,ro)+(Table, tb.defpos) + (LastChange, c.ppos)) {}
        /// <summary>
        /// Ad hoc TableColumn for LogRows, LogRowCol
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="tb"></param>
        /// <param name="nm"></param>
        /// <param name="dt"></param>
        internal TableColumn(Context cx,Table tb,string nm,Domain dt)
            :base(cx.GetUid(),BTree<long,object>.Empty+(Name,nm)+(Table,tb.defpos)
                 + (_Domain,dt))
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
                + (_Domain, dt) + (_Framing,c.framing) + (LastChange,c.ppos);
            if (c.notNull)
                r += (Domain.NotNull, true);
            if (c.generated != GenerationRule.None)
                r += (Generated, c.generated);
            if (dt.defaultString != "")
                r = r + (Domain.DefaultString, dt.defaultString)
                  + (Domain.Default, dt.defaultValue);
            if (dt.IsSensitive())
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
        internal override DBObject Relocate(long dp)
        {
            return new TableColumn(dp,mem);
        }
        internal override CList<long> _Cols(Context cx)
        {
            return cx.Inf(defpos).domain.rowType;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableColumn)base._Relocate(wr);
            r += (_Domain, domain._Relocate(wr));
            r += (Table, wr.Fixed(tabledefpos).defpos);
            r += (Generated, generated._Relocate(wr));
            r += (Checks, wr.Fix(constraints));
            r += (UpdateAssignments, wr.Fix(update));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableColumn)base.Fix(cx);
            var nd = (Domain)domain.Fix(cx);
            if (nd.CompareTo(domain)!=0)
                r += (_Domain, nd);
            var nt = cx.obuids[tabledefpos] ?? tabledefpos;
            if (nt != tabledefpos)
                r += (Table, nt);
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
            var dm = (Domain)tc.domain._Replace(cx,so, sv);
            if (dm != domain)
                tc += (_Domain, dm);
            if (tc.generated.exp != -1L)
            {
                var go = tc.generated.exp;
                var ge = (SqlValue)cx._Replace(go, so, sv);
                if (ge != cx._Ob(go))
                    tc += (Generated, new GenerationRule(tc.generated.gen, tc.generated.gfs, ge));
            }
            var ua = CTree<UpdateAssignment,bool>.Empty;
            for (var b = tc.update.First(); b != null; b = b.Next())
                ua += (b.key().Replace(cx, so, sv),true);
            if (ua != tc.update)
                tc += (UpdateAssignments, ua);
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
            var fm = new From(new Ident("",tr.uid), new Context(tr),tb);
            for (var rb = fm.RowSets(cx, CTree<long, RowSet.Finder>.Empty).First(cx); 
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
            cx.Install1(c.framing);
            cx.Install2(c.framing);
            var sch = (SqlValue)cx.obs[c.search];
            var nf = (Query)new From(new Ident("", tr.uid), cx, tb).AddCondition(cx, sch.Disjoin(cx));
            nf = sch.Conditions(cx, nf, false, out _);
            if (nf.RowSets(cx, CTree<long, RowSet.Finder>.Empty).First(cx) != null)
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
                        ti += (_Domain, ti.domain - ci.defpos);
                        ro += (ti,false);
                        nd += (ro, p);
                    }
                }
                tb += (_Domain, tb.domain - defpos);
                tb += (Level3.Table.TableCols, tb.tblCols - defpos);
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
            sb.Append(" "); sb.Append(domain);
            if (mem.Contains(Table)) { sb.Append(" Table="); sb.Append(Uid(tabledefpos)); }
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
        public Framing framing =>
            (Framing)mem[DBObject._Framing] ?? Framing.Empty;
        public GenerationRule(Generation g) : base(new BTree<long, object>(_Generation, g)) { }
        public GenerationRule(Generation g, string s, SqlValue e)
            : base(BTree<long, object>.Empty + (_Generation, g) + (GenExp, e.defpos) + (GenString, s)) { }
        protected GenerationRule(BTree<long, object> m) : base(m) { }
        public static GenerationRule operator +(GenerationRule gr, (long, object) x)
        {
            return (GenerationRule)gr?.New(gr.mem + x)??None;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GenerationRule(m);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (exp < 0)
                return this;
            return this + (GenExp, wr.Fixed(exp).defpos);
        }
        internal override Basis Fix(Context cx)
        {
            var r = this;
            var ne = cx.obuids[exp] ?? exp;
            if (exp !=ne)
                r += (GenExp, ne);
            return r;
        }
        internal TypedValue Eval(Context cx)
        {
            switch (gen)
            { 
                case Generation.Expression: cx.Frame(exp); return cx.obs[exp].Eval(cx);
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
    /// Table. 
    /// It is Role-independent, so it doesn't follow the representation of any domain 
    /// and therefore can't subclass TRow.
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
            defpos = c._defpos;
            prev = c._defpos;
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
        internal virtual TableRow _Insert(Context cx, long np,
            CTree<long,TypedValue> vs, long st, string prov, Level cl)
        {
            var tb = (Table)cx.obs[tabledefpos];
            Record r;
            if (cl != Level.D)
                r = new Record3(tb, vs, st, cl, np, cx);
            else if (prov != null)
                r = new Record1(tb, vs, prov, np, cx);
            else
                r = new Record(tb, vs, np, cx);
            cx.Add(r);
            return new TableRow(r, cx.db);
        }
        internal virtual TableRow _Update(Context cx, long np, 
            CTree<long,TypedValue> vs, SqlValue level)
        {
            var tb = (Table)cx.db.objects[tabledefpos];
            var u = (level == null) ?
                new Update(this, tb, vs, np, cx) :
                new Update1(this, tb, vs,
                    (Level)level.Eval(cx).Val(), np, cx);
            cx.Add(u);
            return new TableRow(u, cx.db);
        }
        internal virtual bool _Delete(Context cx)
        {
            var np = cx.db.nextPos;
            cx.Add(new Delete1(this, np, cx));
            return true;
        }
        //Handle restrict/cascade for Delete and Update
        internal Role Cascade(Database db, Context cx, Role ro, long p,
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            if (a != 0)
                cx.db += (Database.Cascade, true);
            var ta = (Table)db.objects[tabledefpos];
            var px = ta.FindPrimaryIndex(db);
            for (var b = ro.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is Table tb)
                    for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                    {
                        var rx = (Index)cx.db.objects[xb.value()];
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
                                dk = new PRow(cx.obs[kb.value()].Eval(cx), dk);
                        if (db is Transaction && ca==0 && a==0)
                            throw new DBException("23000", "RESTRICT - foreign key in use", pk);
                        cx.db += (Database.Cascade, true);
                        var rt = (Table)cx.db.objects[rx.tabledefpos];
                        for (var d = rx.rows.PositionAt(pk); d != null && d.key()._CompareTo(pk) == 0; d = d.Next())
                            if (d.Value() != null)
                            {
                                var dp = d.Value().Value;
                                var rr = rt.tableRows[dp];  
                                if (ca == PIndex.ConstraintType.CascadeDelete)
                                {
                                    for (var rb = rt.indexes.First(); rb != null; rb = rb.Next())
                                    {
                                        var ix = (Index)cx.db.objects[rb.value()];
                                        var inf = ix.rows.info;
                                        var key = rr.MakeKey(ix);
                                        ix -= key;
                                        if (ix.rows == null)
                                            ix += (Index.Tree, new MTree(inf));
                                        cx.db += (ix, cx.db.loadpos);
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
                                        var q = pb.value();
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
                                            var tc = fb.value();
                                            rr += (tc, v);
                                        }
                                    }
                                    if (rr != rz)
                                    {
                                        var nk = rr.MakeKey(rx);
                                        rt += rr;
                                        rx -= (ok, rr.defpos);
                                        if (nk != null && nk._head!=TNull.Value)
                                            rx += (nk, rr.defpos);
                                    }
                                }
                                cx.db += (rx, cx.db.loadpos);
                                cx.Add(rx);
                            }
                        cx.db += (rt, cx.db.loadpos);
                        cx.Add(rt);
                    }
            return ro;
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
        internal override TableRow _Insert(Context cx, long np, CTree<long, TypedValue> vs, long st, string prov, Level cl)
        {
            var vw = (RestView)cx.obs[rrs.restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var (url, targetName, sql) = rrs.GetUrl(cx, vi);
            var rq = rrs.GetRequest(cx, url);
            rq.Method = "POST";
            if (vi.metadata.Contains(Sqlx.URL))
            {
                rq.Accept = vw.mime ?? "application/json";
                var cm = "[{";
                vs += vals;
                for (var b = rrs.remoteCols.First(); b != null; b = b.Next())
                {
                    sql.Append(cm); cm = ",";
                    sql.Append('"'); sql.Append(b.key());
                    sql.Append("\":"); sql.Append(vs[b.value()[rrs.defpos]]);
                }
                sql.Append("}]");
            }
            else
            {
                rq.ContentType = "text/plain";
                sql.Append("insert into "); sql.Append(targetName);
                var cm = " values(";
                for (var b = rrs.remoteCols.First(); b != null; b = b.Next())
                    if (vs[b.value()[rrs.defpos]] is TypedValue tv)
                    {
                        sql.Append(cm); cm = ",";
                        if (tv.dataType.kind == Sqlx.CHAR)
                        {
                            sql.Append("'");
                            sql.Append(tv.ToString().Replace("'", "'''"));
                            sql.Append("'");
                        }
                        else
                            sql.Append(tv);
                    }
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            var bs = Encoding.UTF8.GetBytes(sql.ToString());
            rq.ContentLength = bs.Length;
            try
            {
                var rqs = rq.GetRequestStream();
                rqs.Write(bs, 0, bs.Length);
                rqs.Close();
            }
            catch (WebException)
            {
                throw new DBException("3D002", url);
            }
            var rp = RestRowSet.GetResponse(rq);
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("--> " + rp.StatusCode);
            if (rp == null || rp.StatusCode != HttpStatusCode.OK)
                throw new DBException("2E201");
            var ld = rp.GetResponseHeader("LastData");
            if (ld != null)
                cx.data += (rrs.defpos, rrs + (Table.LastData, long.Parse(ld)));
            var et = rp.GetResponseHeader("ETag");
            var es = et.Split(',');
            if (es.Length == 3)
                cx.affected = (cx.affected ?? Rvv.Empty) 
                    + (rrs.target, (long.Parse(es[1]), long.Parse(es[2])));
            return new RemoteTableRow(np, vs, url, rrs);
        }
        internal override TableRow _Update(Context cx, long np, CTree<long, TypedValue> vs, SqlValue level)
        {
            var vw = (RestView)cx.obs[rrs.restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var (url, targetName, sql) = rrs.GetUrl(cx, vi);
            var rq = rrs.GetRequest(cx, url);
            if (vi.metadata.Contains(Sqlx.URL))
            {
                rq.Accept = vw.mime ?? "application/json";
                rq.Method = "PUT";
                var cm = "[{";
                vs = cx.cursors[rrs.defpos].values;
                for (var b = rrs.assig.First();b!=null;b=b.Next())
                {
                    var ua = b.key();
                    vs += (ua.vbl, ((SqlValue)cx.obs[ua.val]).Eval(cx));
                }
                var rb = rrs.rt.First();
                for (var b = rrs.remoteCols.First(); b != null; b = b.Next(),rb=rb.Next())
                {
                    sql.Append(cm); cm = ",";
                    sql.Append('"'); sql.Append(b.key());
                    var tv = vs[rb.value()];
                    sql.Append("\":"); 
                    if (tv.dataType.kind == Sqlx.CHAR)
                    {
                        sql.Append("'");
                        sql.Append(tv.ToString().Replace("'", "'''"));
                        sql.Append("'");
                    }
                    else
                        sql.Append(tv);
                }
                sql.Append("}]");
            }
            else
            {
                rq.ContentType = "text/plain";
                rq.Method = "POST";
                sql.Append("update "); sql.Append(targetName);
                var cm = " set ";
                for (var b = rrs.assig.First(); b != null; b = b.Next())
                {
                    var ua = b.key();
                    sql.Append(cm); cm = ",";
                    sql.Append(((SqlValue)cx.obs[ua.vbl]).name); sql.Append("=");
                    var tv = ((SqlValue)cx.obs[ua.val]).Eval(cx);
                    if (tv.dataType.kind == Sqlx.CHAR)
                    {
                        sql.Append("'");
                        sql.Append(tv.ToString().Replace("'", "'''"));
                        sql.Append("'");
                    }
                    else
                        sql.Append(tv);
                }
                if (rrs.where.Count > 0 || rrs.matches.Count > 0)
                {
                    var sw = rrs.WhereString(cx);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
                        sql.Append(sw);
                    }
                }
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            var bs = Encoding.UTF8.GetBytes(sql.ToString());
            rq.ContentLength = bs.Length;
            try
            {
                var rqs = rq.GetRequestStream();
                rqs.Write(bs, 0, bs.Length);
                rqs.Close();
            }
            catch (WebException)
            {
                throw new DBException("3D002", url);
            }
            var rp = RestRowSet.GetResponse(rq);
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("--> " + rp.StatusCode);
            if (rp == null || rp.StatusCode != HttpStatusCode.OK)
                throw new DBException("2E201");
            var ld = rp.GetResponseHeader("LastData");
            if (ld != null && ld!="")
                cx.data += (rrs.defpos,rrs+ (Table.LastData, long.Parse(ld)));
            var et = rp.GetResponseHeader("ETag");
            var es = et.Split(',');
            if (es.Length == 3)
                cx.affected = (cx.affected ?? Rvv.Empty) 
                    + (rrs.target, (long.Parse(es[1]), long.Parse(es[2])));
            return new RemoteTableRow(np, vs, url, rrs);
        }
        internal override bool _Delete(Context cx)
        {
            var vw = (RestView)cx.obs[rrs.restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var (url, targetName, sql) = rrs.GetUrl(cx, vi);
            var rq = rrs.GetRequest(cx, url);
            if (vi.metadata.Contains(Sqlx.URL))
                rq.Method = "DELETE";
            else
            {
                rq.ContentType = "text/plain";
                rq.Method = "POST";
                sql.Append("delete from "); sql.Append(targetName);
                if (rrs.where.Count > 0 || rrs.matches.Count > 0)
                {
                    var sw = rrs.WhereString(cx);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
                        sql.Append(sw);
                    }
                }
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            var bs = Encoding.UTF8.GetBytes(sql.ToString());
            rq.ContentLength = bs.Length;
            try
            {
                var rqs = rq.GetRequestStream();
                rqs.Write(bs, 0, bs.Length);
                rqs.Close();
            }
            catch (WebException)
            {
                throw new DBException("3D002", url);
            }
            var rp = RestRowSet.GetResponse(rq);
            if (PyrrhoStart.HTTPFeedbackMode)
               Console.WriteLine("--> " + rp.StatusCode);
            if (rp == null || rp.StatusCode != HttpStatusCode.OK)
                throw new DBException("2E201");
            var ld = rp.GetResponseHeader("LastData");
            if (ld != null)
                cx.data += (rrs.defpos, rrs + (Table.LastData, long.Parse(ld)));
            var et = rp.GetResponseHeader("ETag");
            var es = et.Split(',');
            if (es.Length == 3)
                cx.affected = (cx.affected ?? Rvv.Empty) 
                    + (rrs.target, (long.Parse(es[1]), long.Parse(es[2])));
            return true;
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            return new PeriodDef(wr.Fix(defpos), wr.Fix(tabledefpos),
                wr.Fix(startCol), wr.Fix(endCol),wr.cx.db);
        }
        internal override Basis Fix(Context cx)
        {
            var r = new PeriodDef(cx.obuids[defpos]??defpos, 
                cx.obuids[tabledefpos]??tabledefpos,
                cx.obuids[startCol]??startCol, 
                cx.obuids[endCol]??endCol, cx.db);
            return r;
        }
    }
}
