using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
    /// When a Table is accessed
    /// any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class Table : DBObject
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // CTree<Domain,CTree<long,bool>> SqlValue,Index
            KeyCols = -320, // CTree<long,bool> TableColumn (over all indexes)
            LastData = -258, // long
            _NodeType = -86, // long NodeType
            RefIndexes = -250, // CTree<long,CTree<Domain,Domain>> referencing Table,referencing TableColumns,referenced TableColumns
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // CTree<long,bool> Check
            TableCols = -332, // CTree<long,Domain> TableColumn
            TableRows = -181, // BTree<long,TableRow>
            Triggers = -267; // CTree<PTrigger.TrigType,CTree<long,bool>> (T) 
        /// <summary>
        /// The rows of the table with the latest version for each
        /// </summary>
		public BTree<long, TableRow> tableRows => 
            (BTree<long,TableRow>)(mem[TableRows]??BTree<long,TableRow>.Empty);
        public CTree<Domain, CTree<long,bool>> indexes => 
            (CTree<Domain,CTree<long,bool>>)(mem[Indexes]??CTree<Domain,CTree<long,bool>>.Empty);
        public CTree<long, bool> keyCols =>
            (CTree<long, bool>)(mem[KeyCols] ?? CTree<long, bool>.Empty);
        internal CTree<long, Domain> tableCols =>
            (CTree<long, Domain>)(mem[TableCols] ?? CTree<long, Domain>.Empty);
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal string? iri => (string?)mem[Domain.Iri];
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal CTree<long,CTree<Domain,Domain>> rindexes =>
            (CTree<long, CTree<Domain, Domain>>)(mem[RefIndexes] 
            ?? CTree<long, CTree<Domain, Domain>>.Empty);
        internal CTree<long, bool> tableChecks => 
            (CTree<long, bool>)(mem[TableChecks] ?? CTree<long, bool>.Empty);
        internal CTree<PTrigger.TrigType, CTree<long,bool>> triggers =>
            (CTree<PTrigger.TrigType, CTree<long, bool>>)(mem[Triggers] 
            ?? CTree<PTrigger.TrigType, CTree<long, bool>>.Empty);
        internal long nodeType => (long)(mem[_NodeType] ?? -1L);
        internal virtual long lastData => (long)(mem[LastData] ?? 0L);
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt) :base(pt.ppos, BTree<long, object>.Empty
            +(Definer,pt.definer)+(Owner,pt.owner)+(Infos,pt.infos)
            +(LastChange, pt.ppos)
            + (_Domain,pt.dataType)
            +(Triggers, CTree<PTrigger.TrigType, CTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
        internal Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator +(Table tb, (Context,int,DBObject) x) // tc can be SqlValue for Type def
        {
            var (cx, i, tc) = x;
            var td = tb.domain;
            var cd = tc.domain;
            if (i < 0)
                i = td.Length;
            var rt = Add(td.rowType, i, tc.defpos);
            var rs = new CTree<long, Domain>(tc.defpos,cd);
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() is long p && td.representation[p] is Domain d)
                    rs += (p, d);
            td = (Domain)cx.Add((Domain)td.New(-1L,td.mem+(Domain.RowType,rt)+(Domain.Representation,rs)));
            var ts = tb.tableCols + (tc.defpos, cd);
            var m = tb.mem + (TableCols, ts) + (_Domain,td);
            m += (Dependents, tb.dependents + (tc.defpos, true));
            if (tc.sensitive)
                m += (Sensitive, true);
            return (Table)cx.Add((Table)tb.New(m));
        }
        internal static BList<long?> Add(BList<long?> a, int k, long v)
        {
            var r = BList<long?>.Empty;
            for (var b = a.First(); b != null; b = b.Next())
            {
                if (b.key() == k)
                    r += v;
                var p = b.value();
                if (p != v)
                    r += p;
            }
            if (k<0 || a.Length == k)
                r += v;
            return r;
        }
        public static Table operator-(Table tb,long p)
        {
            return (Table)tb.New(tb.mem + (TableRows,tb.tableRows-p));
        }
        /// <summary>
        /// Add a new or updated row, indexes already fixed.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="rw"></param>
        /// <returns></returns>
        public static Table operator +(Table t, TableRow rw)
        {
            var se = t.sensitive || rw.classification!=Level.D;
            return (Table)t.New(t.mem + (TableRows,t.tableRows+(rw.defpos,rw)) 
                + (Sensitive,se));
        }
        public static Table operator+(Table tb,(long,object)v)
        {
            return (Table)tb.New(tb.mem + v);
        }

        internal virtual ObInfo _ObInfo(long ppos, string name, Grant.Privilege priv)
        {
            return new ObInfo(name,priv);
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableChecks,tableChecks+(ck.defpos,true)));
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
            for (var b = tableCols.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is DBObject ob)
                    cx.Add(ob);
        }
        internal override DBObject AddTrigger(Trigger tg)
        {
            var tb = this;
            var ts = triggers[tg.tgType] ?? CTree<long, bool>.Empty;
            return tb + (Triggers, triggers+(tg.tgType, ts + (tg.defpos, true)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Table(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Table(dp, m);
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (Table)base.ShallowReplace(cx, was, now);
            var xs = ShallowReplace(cx, indexes, was, now);
            if (xs != indexes)
                r += (Indexes, xs);
            var ks = cx.ShallowReplace(keyCols, was, now);
            if (ks != keyCols)
                r += (KeyCols, ks);
            var rs = ShallowReplace(cx, rindexes,was,now);
            if (rs != rindexes)
                r += (RefIndexes, rs);
            var cs = cx.ShallowReplace(tableCols, was, now);
            if (cs!=tableCols)
                r += (TableCols, cs);
            var ts = ShallowReplace(cx, tableRows, was, now);
            if (ts != tableRows)
                r += (TableRows, ts);
            return r;
        }
        static CTree<Domain,CTree<long,bool>> ShallowReplace(Context cx,CTree<Domain,CTree<long,bool>> xs,long was,long now)
        {
            for (var b=xs.First();b!=null;b=b.Next())
            {
                var k = b.key().ShallowReplace1(cx,was,now);
                if (k != b.key())
                    xs -= b.key();
                var v = cx.ShallowReplace(b.value(),was,now);
                if (k != b.key() || v != b.value())
                    xs += (k, v);
            }
            return xs;
        }
        static CTree<long,CTree<Domain,Domain>> ShallowReplace(Context cx,CTree<long,CTree<Domain,Domain>> rs,long was,long now)
        {
            for (var b=rs.First();b!=null;b=b.Next())
            {
                var v = ShallowReplace(cx, b.value(),was,now);
                if (v != b.value())
                    rs += (b.key(), v);
            }
            return rs;
        }
        static CTree<Domain,Domain> ShallowReplace(Context cx,CTree<Domain,Domain> rx,long was,long now)
        {
            for (var b=rx.First();b!=null;b=b.Next())
            {
                var k = b.key().ShallowReplace1(cx,was,now);
                if (k != b.key())
                    rx -= b.key();
                var v = b.value().ShallowReplace1(cx,was,now);
                if (k != b.key() || v != b.value())
                    rx += (k, v);
            }
            return rx;
        }
        static BTree<long,TableRow> ShallowReplace(Context cx,BTree<long,TableRow> ts,long was, long now)
        {
            for (var b=ts.First();b!=null;b=b.Next())
            {
                var r = b.value().ShallowReplace(cx, was, now);
                if (r != b.value())
                    ts += (b.key(), r);
            }
            return ts;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var na = cx.Fix(applicationPS);
            if (na!=applicationPS)
                r += (ApplicationPS, na);
            var ni = cx.FixTDTlb(indexes);
            if (ni!=indexes)
                r += (Indexes, ni);
            var nr = cx.FixTlTDD(rindexes);
            if (nr != rindexes)
                r += (RefIndexes, ni);
            var tc = cx.FixTlD(tableCols);
            if (tc!=tableCols)
                r += (TableCols, tc);
            var ns = cx.Fix(systemPS);
            if (ns!=systemPS)
                r += (SystemPS, ns);
            var nk = cx.FixTlb(keyCols);
            if (nk != keyCols)
                r += (KeyCols, nk);
            var nc = cx.FixTlb(tableChecks);
            if (nc!=tableChecks)
                r += (TableChecks, nc);
            var nt = cx.FixTTElb(triggers);
            if (nt!=triggers)
                r += (Triggers, nt);
            var ut = cx.Fix(nodeType);
            if (ut != nodeType)
                r += (_NodeType, ut);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (Table)base._Replace(cx, so, sv);
            var dm = domain.Replace(cx, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
            return r;
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue>? u = null)
        {
            var ro = cx.role ?? throw new DBException("42105");
            base.Cascade(cx, a, u);
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix)
                        ix.Cascade(cx, a, u);
            for (var b = ro.dbobjects.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is Table tb)
                    for (var c = tb.indexes.First(); c != null; c = c.Next())
                        for (var d = c.value().First(); d != null; d = d.Next())
                            if (cx.db.objects[d.key()] is Index ix)
                                if (ix.reftabledefpos == defpos)
                                    tb.Cascade(cx, a, u);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro && infos[bp] is ObInfo oi
                    && oi.name is not null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += (ro, p);
                }
            return base.Drop(d, nd, p);
        }
        internal override Database DropCheck(long ck, Database nd, long p)
        {
            return nd + (this + (TableChecks, tableChecks - ck),p);
        }
        internal virtual Index? FindPrimaryIndex(Context cx)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix &&
                        ix.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                         return ix;
            return null;
        }
        internal Index[]? FindIndex(Database db, Domain key, 
            PIndex.ConstraintType fl=(PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            var r = BList<Index>.Empty;
            for (var b = indexes[key]?.First(); b != null; b = b.Next())
            if (db.objects[b.key()] is Index x && (x.flags&fl)!=0)
                    r += x;
            return (r==BList<Index>.Empty)?null:r.ToArray();
        }
        internal Index[]? FindIndex(Database db, BList<long?> cols,
    PIndex.ConstraintType fl = (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            var r = BList<Index>.Empty;
            for (var b = indexes?.First(); b != null; b = b.Next())
                if (Context.Match(b.key().rowType,cols))
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (db.objects[c.key()] is Index x && (x.flags & fl) != 0)
                            r += x;
            return (r == BList<Index>.Empty) ? null : r.ToArray();
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, 
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null)
        {
            cx.Add(this);
            cx.Add(framing);
            var m = BTree<long, object>.Empty + (_From, fm) + (_Ident,id);
            if (a != null)
                m += (_Alias, a);
            var rowSet = (RowSet)cx._Add(new TableRowSet(id.iix.dp, cx, defpos,m));
//#if MANDATORYACCESSCONTROL
            Audit(cx, rowSet);
//#endif
            return rowSet;
        }
        public override bool Denied(Context cx, Grant.Privilege priv)
        { 
            if (cx.db.user != null && enforcement.HasFlag(priv) &&
                !(cx.db.user.defpos == cx.db.owner
                    || cx.db.user.clearance.ClearanceAllows(classification)))
                return true;
            return base.Denied(cx, priv);
        }
        internal CTree<Domain, CTree<long,bool>> IIndexes(Context cx,BTree<long, long?> sim)
        {
            var xs = CTree<Domain, CTree<long, bool>>.Empty;
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var bs = BList<DBObject>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() is long p && cx._Ob(sim[p]??-1L) is DBObject tp)
                        bs += tp;
                var k = (Domain)cx.Add(new Domain(-1L,cx,Sqlx.ROW,bs,bs.Length));
                xs += (k, b.value());
            }
            return xs;
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" rows ");sb.Append(tableRows.Count);
            if (PyrrhoStart.VerboseMode && mem.Contains(Enforcement)) 
            { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (indexes.Count!=0) 
            { 
                sb.Append(" Indexes:(");
                var cm = "";
                for (var b=indexes.First();b is not null;b=b.Next())
                {
                    sb.Append(cm);cm = ";";
                    var cn = "(";
                    for (var c = b.key().First(); c != null; c = c.Next())
                        if (c.value() is long p)
                        {
                            sb.Append(cn); cn = ",";
                            sb.Append(Uid(p));
                        }
                    sb.Append(')'); cn = "";
                    for (var c=b.value().First();c is not null;c=c.Next())
                    {
                        sb.Append(cn); cn = ",";
                        sb.Append(Uid(c.key()));
                    }
                }
                sb.Append(')');
                sb.Append(" KeyCols: "); sb.Append(keyCols);
            }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
            if (nodeType>=0) { sb.Append(" NodeType "); sb.Append(Uid(nodeType)); }
            return sb.ToString();
        }
        internal static string ToCamel(string s)
        {
            var sb = new StringBuilder();
            sb.Append(char.ToLower(s[0]));
            sb.Append(s.AsSpan(1));
            return sb.ToString();
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition,
        /// and computes navigation properties
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, DBObject from,
            ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || domain.kind!=Sqlx.Null || from.domain.kind!=Sqlx.Null)
                throw new DBException("42105");
            var nm = NameFor(cx);
            var versioned = mi.metadata.Contains(Sqlx.ENTITY);
            var key = BuildKey(cx, out Domain keys);
            var fields = CTree<string, bool>.Empty;
            var sb = new StringBuilder("\r\nusing System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + nm + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Index x)
                        x.Note(cx, sb);
            for (var b = tableChecks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Note(cx, sb);
            sb.Append("/// </summary>\r\n");
            sb.Append("[Table("); sb.Append(defpos); sb.Append(')'); sb.Append(lastChange); sb.Append(")]\r\n");
            sb.Append("public class " + nm + (versioned ? " : Versioned" : "") + " {\r\n");
            for (var b = domain.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var tn = ((dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name) + "?"; // all fields nullable
                dt.FieldType(cx, sb);
                var ci = infos[cx.role.defpos];
                if (ci != null && ci.name != null)
                {
                    fields += (ci.name, true);
                    for (var d = ci.metadata.First(); d != null; d = d.Next())
                        switch (d.key())
                        {
                            case Sqlx.X:
                            case Sqlx.Y:
                                sb.Append(" [" + d.key().ToString() + "]\r\n");
                                break;
                        }
                    if (ci.description?.Length > 1)
                        sb.Append("  // " + ci.description + "\r\n");
                    if (cx._Ob(p) is TableColumn tc)
                    {
                        for (var c = tc.constraints.First(); c != null; c = c.Next())
                            if (cx._Ob(c.key()) is Check ck)
                                ck.Note(cx, sb);
                        if (tc.generated is GenerationRule gr)
                            gr.Note(sb);
                    }
                    for (var c = dt.constraints.First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is DBObject ck)
                            ck.Note(cx, sb);
                }
                else if (cx.obs[p]?.infos[ro.defpos] is ObInfo fi && fi.name != null)
                    fields += (fi.name, true);
                if ((keys.rowType.Last()?.value() ?? -1L) == p && dt.kind == Sqlx.INTEGER)
                    sb.Append("  [AutoKey]\r\n");
                var cn = cx.NameFor(p);
                sb.Append("  public " + tn + " " + cn + ";\r\n");
            }
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Index x &&
                            x.flags.HasFlag(PIndex.ConstraintType.ForeignKey) &&
                            cx.db.objects[x.refindexdefpos] is Index rx &&
                            cx._Ob(rx.tabledefpos) is Table tb && tb.infos[ro.defpos] is ObInfo rt &&
                            rt.name != null)
                    {
                        // many-one relationship
                        var sa = new StringBuilder();
                        var cm = "";
                        for (var d = b.key().First(); d != null; d = d.Next())
                            if (d.value() is long p)
                            {
                                sa.Append(cm); cm = ",";
                                sa.Append(cx.NameFor(p));
                            }
                        if (!rt.metadata.Contains(Sqlx.ENTITY))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + " " + rn
                            + "=> conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (rt.metadata.Contains(Sqlx.ENTITY))
                        for (var c = b.value().First(); c != null; c = c.Next())
                        {
                            var sa = new StringBuilder();
                            var cm = "(\"";
                            var rn = ToCamel(rt.name);
                            for (var i = 0; fields.Contains(rn); i++)
                                rn = ToCamel(rt.name) + i;
                            fields += (rn, true);
                            var x = tb.FindIndex(cx.db, c.key())?[0];
                            if (x != null)
                            // one-one relationship
                            {
                                cm = "";
                                for (var bb = c.value().First(); bb != null; bb = bb.Next())
                                if (bb.value() is long p && cx._Ob(p) is DBObject ob && 
                                        ob.infos[ro.defpos] is ObInfo vi && vi.name is not null){
                                    sa.Append(cm); cm = ",";
                                    sa.Append(vi.name);
                                }
                                sb.Append("  public " + rt.name + " " + rn
                                    + "s => conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                                continue;
                            }
                            // one-many relationship
                            var rb = c.value().First();
                            for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                                if (xb.value() is long xp && rb.value() is long rp)
                                {
                                    sa.Append(cm); cm = "),(\"";
                                    sa.Append(cx.NameFor(xp)); sa.Append("\",");
                                    sa.Append(cx.NameFor(rp));
                                }
                            sa.Append(')');
                            sb.Append("  public " + rt.name + "[] " + rn
                                + "s => conn.FindWith<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                        }
                    else //  e.g. this is Brand
                    {
                        // tb is auxiliary table e.g. BrandSupplier
                        for (var d = tb.indexes.First(); d != null; d = d.Next())
                            for (var e = d.value().First(); e != null; e = e.Next())
                                if (cx.db.objects[e.key()] is Index px && px.reftabledefpos != defpos
                                            && cx.db.objects[px.reftabledefpos] is Table ts// e.g. Supplier
                                            && ts.infos[ro.definer] is ObInfo ti &&
                                            ti.metadata.Contains(Sqlx.ENTITY) &&
                                            ts.FindPrimaryIndex(cx) is Index tx)
                                {
                                    var sk = new StringBuilder(); // e.g. Supplier primary key
                                    var cm = "\\\"";
                                    for (var c = tx.keys.First(); c != null; c = c.Next())
                                        if (c.value() is long p && cx._Ob(p) is DBObject ob 
                                            && ob.infos[ro.defpos] is ObInfo ci &&
                                                        ci.name != null)
                                        {
                                            sk.Append(cm); cm = "\\\",\\\"";
                                            sk.Append(ci.name);
                                        }
                                    sk.Append("\\\"");
                                    var sa = new StringBuilder(); // e.g. BrandSupplier.Brand = Brand
                                    cm = "\\\"";
                                    var rb = px.keys.First();
                                    for (var xb = keys?.First(); xb != null && rb != null;
                                        xb = xb.Next(), rb = rb.Next())
                                        if (xb.value() is long xp && rb.value() is long rp)
                                        {
                                            sa.Append(cm); cm = "\\\" and \\\"";
                                            sa.Append(cx.NameFor(xp)); sa.Append("\\\"=\\\"");
                                            sa.Append(cx.NameFor(rp));
                                        }
                                    sa.Append("\\\"");
                                    var rn = ToCamel(rt.name);
                                    for (var i = 0; fields.Contains(rn); i++)
                                        rn = ToCamel(rt.name) + i;
                                    fields += (rn, true);
                                    sb.Append("  public " + ti.name + "[] " + rn
                                        + "s => conn.FindIn<" + ti.name + ">(\"select "
                                        + sk.ToString() + " from \\\"" + rt.name + "\\\" where "
                                        + sa.ToString() + "\");\r\n");
                                }
                    }
                }
            sb.Append("}\r\n");
            return new TRow(from.domain, new TChar(domain.name), new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Context cx, DBObject from, 
            ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || domain.kind==Sqlx.Null || from.domain.kind ==Sqlx.Null
                || cx.db.user is not User ud)
                throw new DBException("42105");
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(domain.name); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + cx.db.name + ", Role " 
                + ro.name + "r\n */");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(cx,out Domain keys);
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(')');
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(ud.name); sb.Append("\r\n */");
            if (mi.description != "")
                sb.Append("/* " + mi.description + "*/\r\n");
            sb.Append("public class " + domain.name + ((versioned) ? " extends Versioned" : "") + " {\r\n");
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && tableCols[bp] is Domain dt)
                {
                    var p = b.key();
                    var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                    if (keys != null)
                    {
                        int j;
                        for (j = 0; j < keys.Length; j++)
                            if (keys[j] == p)
                                break;
                        if (j < keys.Length)
                            sb.Append("  @Key(" + j + ")\r\n");
                    }
                    dt.FieldJava(cx, sb);
                    sb.Append("  public " + tn + " " + cx.NameFor(p) + ";");
                    sb.Append("\r\n");
                }
            sb.Append("}\r\n");
            return new TRow(from.domain,new TChar(domain.name),new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Context cx, DBObject from, ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || domain.kind==Sqlx.Null || from.domain.kind == Sqlx.Null)
                throw new DBException("42105");
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(domain.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            var key = BuildKey(cx, out Domain keys);
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + domain.name + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            for(var b = domain.representation.First();b is not null;b=b.Next())
            {
                sb.Append("  self." + cx.NameFor(b.key()) + " = " + b.value().defaultValue);
                sb.Append("\r\n");
            }
            sb.Append("  self._schemakey = "); sb.Append(from.lastChange); sb.Append("\r\n");
            if (keys!=Domain.Null)
            {
                var comma = "";
                sb.Append("  self._key = ["); 
                for (var i=0;i<keys.Length;i++)
                {
                    sb.Append(comma); comma = ",";
                    sb.Append('\'');  sb.Append(keys[i]); sb.Append('\'');
                }
                sb.Append("]\r\n");
            }
            return new TRow(from.domain, new TChar(domain.name),new TChar(key),
                new TChar(sb.ToString()));
        }
        internal virtual string BuildKey(Context cx,out Domain keys)
        {
            keys = Domain.Row;
            for (var xk = indexes.First(); xk != null; xk = xk.Next())
                for (var c = xk.value().First();c is not null;c=c.Next())
            if (cx.db.objects[c.key()] is Index x && x.tabledefpos == defpos &&
                        (x.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey)
                    keys = x.keys;
            var comma = "";
            var sk = new StringBuilder();
            if (keys != Domain.Row)
                for (var i = 0; i < keys.Length; i++)
                    if ( cx.db.objects[keys[i]??-1L] is TableColumn cd)
                    {
                        sk.Append(comma); comma = ",";
                        sk.Append(cd.NameFor(cx));
                    }
            return sk.ToString();
        }
    }
    internal class VirtualTable : Table
    {
        internal const long
            _RestView = -372; // long RestView
        public long restView => (long)(mem[_RestView] ?? -1L);
        internal VirtualTable(PTable pt, Context cx) : base(pt)
        {
            cx.Add(this);
        }
        internal VirtualTable(Ident tn, Context cx, Domain dm)
            : this(new PTable(tn.ident, dm, tn.iix.dp, cx), cx)
        { }
        protected VirtualTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static VirtualTable operator +(VirtualTable v, (long, object) x)
        {
            var (dp, ob) = x;
            if (v.mem[dp] == ob)
                return v;
            return (VirtualTable)v.New(v.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new VirtualTable(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return (dp == defpos) ? this : new VirtualTable(dp, mem);
        }
        internal override ObInfo _ObInfo(long ppos, string name, Grant.Privilege priv)
        {
            var ti = base._ObInfo(ppos, name, priv);
            return ti;
        }
        internal override Index? FindPrimaryIndex(Context cx)
        {
            if (cx.db.objects[restView] is not RestView rv)
                return null;
            cx.Add(rv.framing);
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix &&
                            ix.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                        return ix;
            return null;
        }
        internal override string BuildKey(Context cx, out Domain keys)
        {
            keys = Domain.Row;
            if (cx.db.objects[restView] is not RestView rv)
                throw new PEException("PE5801");
            cx.Add(rv.framing);
            for (var xk = indexes.First(); xk != null; xk = xk.Next())
                for (var c = xk.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index x && x.tabledefpos == defpos &&
                        (x.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey)
                        keys = x.keys;
            var comma = "";
            var sk = new StringBuilder();
            if (keys != Domain.Row)
                for (var i = 0; i < keys.Length; i++)
                    if (cx.obs[keys[i]??-1L] is SqlValue se)
                    {
                        sk.Append(comma);
                        comma = ",";
                        sk.Append(se.NameFor(cx));
                    }
            return sk.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" RestView="); sb.Append(Uid(restView));
            return sb.ToString();
        }
    }
}
