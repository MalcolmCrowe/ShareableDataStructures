using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Reflection.Emit;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// The Domain base has columns, but not all Domains with Columns are Tables 
    /// (e.g. Keys, Orderings and Groupings are domains with columns but are not tables).
    /// Tables are not always associated with base tables either: the valueType of a View
    /// or SqlCall can be a Table.
    /// From v 7.04 we identify Table with Relation Type, hence Table is a kind of Domain
    /// that also has a collection of rows.
    /// Any TableRow is entered into the Tables of its datatype and its supertypes if any.
    /// (thus the Domain of any tableRow is a Table).
    /// Rows with IRI metadata always have a table supertype of their domain.
    /// UDTypes do not normally have rows directly, but can do so.
    /// Nodes are rows of NodeTypes, Edges are rows of EdgeTypes, and named tables of these can be created. 
    /// CREATE TABLE creates a named table.
    /// CREATE TABLE OF TYPE makes a table whose type is a new subtype (of table,nodetype,edgetype).
    /// When a Table is accessed any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// 
    /// </summary>
    internal class Table : Domain
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // CTree<Domain,CTree<long,bool>> QlValue,Index
            KeyCols = -320, // CTree<long,bool> TableColumn (over all indexes)
            LastData = -258, // long
            RefIndexes = -250, // CTree<long,CTree<Domain,Domain>> referencing Table,referencing TableColumns,referenced TableColumns
            SysRefIndexes = -111, // CTree<long,CTree<long,CTree<long,CTree<long,bool>>>> nodeTable,node, edgeColumn,edge 
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // CTree<long,bool> Check
            TableRows = -181, // BTree<long,TableRow>
            Triggers = -267; // CTree<PTrigger.TrigType,CTree<long,bool>> (T) 
        /// <summary>
        /// The rows of the table with the latest version for each.
        /// The values of tableRows include supertype and subtype fields, 
        /// because the record fields are simply shared with subtypes.
        /// </summary>
		public BTree<long, TableRow> tableRows => 
            (BTree<long,TableRow>)(mem[TableRows]??BTree<long,TableRow>.Empty);
        /// <summary>
        /// Indexes are not inherited: Records are entered in their table's indexes and those of its supertypes.
        /// </summary>
        public CTree<Domain, CTree<long,bool>> indexes => 
            (CTree<Domain,CTree<long,bool>>)(mem[Indexes]??CTree<Domain,CTree<long,bool>>.Empty);
        public CTree<long, bool> keyCols =>
            (CTree<long, bool>)(mem[KeyCols] ?? CTree<long, bool>.Empty);
        /// <summary>
        /// The domain information for a table is directly in the table properties.
        /// RowType and Representation exclude inherited columns.
        /// The corresponding properties in TableRowSet include inherited columns.
        /// </summary>
        public override Domain domain => throw new NotImplementedException();
        internal override bool Defined() => defpos > 0;
        internal virtual CTree<long, Domain> tableCols => representation;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal CTree<long,CTree<Domain,Domain>> rindexes =>
            (CTree<long, CTree<Domain, Domain>>)(mem[RefIndexes] 
            ?? CTree<long, CTree<Domain, Domain>>.Empty);
        internal CTree<long, CTree<long, CTree<long, bool>>> sindexes =>
            (CTree<long, CTree<long, CTree<long, bool>>>)(mem[SysRefIndexes]
            ?? CTree<long, CTree<long, CTree<long, bool>>>.Empty);
        internal CTree<long, bool> tableChecks => 
            (CTree<long, bool>)(mem[TableChecks] ?? CTree<long, bool>.Empty);
        internal CTree<PTrigger.TrigType, CTree<long,bool>> triggers =>
            (CTree<PTrigger.TrigType, CTree<long, bool>>)(mem[Triggers] 
            ?? CTree<PTrigger.TrigType, CTree<long, bool>>.Empty);
        internal virtual long lastData => (long)(mem[LastData] ?? 0L);
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt) :base(pt.ppos, pt.dataType.mem
            +(Definer,pt.definer)+(Owner,pt.owner)+(Infos,pt.infos)
            +(LastChange, pt.ppos)
            +(Triggers, CTree<PTrigger.TrigType, CTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
        protected Table(Qlx t) : base(--_uid,t, BTree<long,object>.Empty) { }
        internal Table(Context cx,CTree<long,Domain>rs, CList<long> rt, BTree<long,ObInfo> ii)
            : base(cx,rs,rt,ii) { }
        internal Table(long dp, Context cx, CTree<long, Domain> rs, CList<long> rt, int ds)
            : base(dp, cx, Qlx.TABLE, rs, rt, ds) { }
        internal Table(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// The Node Type and PathDomain things make this operation more complex,
        /// For multiple inheritance the subtype has a new uid for identity and is used here.
        /// but otherwise we do not worry about identity inheritance (this is fixed in Record.Install).
        /// Although this is not theoretically necessary, for presentational purposes 
        /// we adjust so that our identity column is first (seq=0), followed by leaving and arriving if any,
        /// other properties in declaration order.
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="x">(Context,suggested seq,TableColumn)</param>
        /// <returns>The updated Table including updated PathDomain (note TableColumn may be changed)</returns>
        /// <exception cref="DBException"></exception>
        /// <exception cref="PEException"></exception>
        public static Table operator +(Table tb, (Context, TableColumn) x)
        {
            var (cx, tc) = x;
            var oi = tb.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.TABLE);
            var ci = tc.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN);
            var ns = oi.names;
            tc += (TableColumn.Seq, tb.Length);
            if (ci.name != null)
                ns += (ci.name,(0L, tc.defpos));
            cx.Add(tc);
            oi += (ObInfo._Names, ns);
            if (!tb.representation.Contains(tc.defpos))
            {
                tb += (RowType, tb.rowType + tc.defpos);
                tb += (Representation, tb.representation + (tc.defpos, tc.domain));
                tb += (Infos, tb.infos + (cx.role.defpos, oi));
                tb += (ObInfo._Names, oi.names);
            }
            for (var b = tb.subtypes.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table st)
                {
                    var ss = st.super;
                    for (var c = ss.First(); c != null; c = c.Next())
                        if (c.key().defpos == tb.defpos)
                            ss -= c.key();
                    st += (cx, tc);
                    st += (Under, ss + (tb, true));
                    cx.db += st;
                }
            if (tb is EdgeType et && tc.tc is TConnector cc)
            {
                var cs = et.connects;
                for (var b=cs.First();b!=null;b=b.Next())
                    if (b.key() is TConnector oc && cx.uids.Contains(oc.cp))
                        cs -= oc;
                et += (EdgeType.Connects, cs + (cc,true));
                tb = et;
            }
            cx.Add(tb);
            cx.db += (tb.defpos, tb);
            tb += (Dependents, tb.dependents + (tc.defpos, true));
            if (tc.sensitive)
                tb += (Sensitive, true);
            return (Table)cx.Add(tb);
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
            var d = tb.depth;
            var m = tb.mem;
            var (dp, ob) = v;
            if (tb.mem[dp] != ob && ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > tb.depth)
                    m += (_Depth, d);
            }
            return (Table)tb.New(m + v);
        }
        /// <summary>
        /// PathDomain is always a Table even for NodeType, EdgeType etc
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual Table _PathDomain(Context cx)
        {
            return this;
        }
        internal override long ColFor(Context cx, string c)
        {
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is DBObject ob &&
                        ((p >= Transaction.TransPos && ob is QlValue sv
                            && (sv.alias ?? sv.name) == c)
                        || ob.NameFor(cx) == c))
                    return p;
            return -1L;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return New(defpos,mem+(TableChecks,tableChecks+(ck.defpos,true)));
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
            for (var b = tableCols.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is DBObject ob && !cx.obs.Contains(b.key()))
                    cx.Add(ob);
        }
        internal override DBObject Add(Context cx, PMetadata pm)
        {
            var ob = (Table)base.Add(cx, pm);
            if (pm.iri != "" && pm.iri!=" Null")
            {
                var nb = (Table)ob.Relocate(pm.ppos); // make a new subtype
                if (nb is EdgeType ne && nb.defpos != defpos)
                    ne.Fix(cx);
                nb += (ObInfo.Name, pm.iri);
                nb += (Under, new CTree<Domain,bool>(ob,true));
                nb += (Subtypes, ob.subtypes + (nb.defpos,true));
                ob = nb;
            }
            return cx.Add(ob);
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
            var ks = Context.ShallowReplace(keyCols, was, now);
            if (ks != keyCols)
                r += (KeyCols, ks);
            var rs = ShallowReplace(cx, rindexes,was,now);
            if (rs != rindexes)
                r += (RefIndexes, rs);
            var ss = ShallowReplace(cx, sindexes, was, now);
            if (ss != sindexes)
                r += (SysRefIndexes, ss);
            var ts = ShallowReplace(cx, tableRows, was, now);
            if (ts != tableRows)
            {
             //   for (var b = ts.First(); b != null; b = b.Next()) // verify that all tablerows have the right types for all vals
             //       b.value()?.Check(r,cx);
                r += (TableRows, ts);
            }
            return r;
        }
        static CTree<Domain,CTree<long,bool>> ShallowReplace(Context cx,CTree<Domain,CTree<long,bool>> xs,long was,long now)
        {
            for (var b=xs.First();b!=null;b=b.Next())
            {
                var k = (Domain)b.key().ShallowReplace(cx,was,now);
                if (k != b.key())
                    xs -= b.key();
                var v = Context.ShallowReplace(b.value(),was,now);
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
        static CTree<long, CTree<long, CTree<long,bool>>> ShallowReplace(Context cx, CTree<long, CTree<long, CTree<long,bool>>> rs, long was, long now)
        {
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var ch = false;
                if (b.value() is CTree<long, CTree<long, bool>> t)
                {
                    for (var c = t.First(); c != null; c = c.Next())
                        if (c.key() == was)
                        {
                            var tn = t[now] ?? CTree<long, bool>.Empty;
                            t -= was;
                            t += (now, tn + c.value());
                            ch = true;
                        }
                    if (ch)
                        rs += (b.key(), t);
                }
            }
            return rs;
        }
        static CTree<Domain,Domain> ShallowReplace(Context cx,CTree<Domain,Domain> rx,long was,long now)
        {
            for (var b=rx.First();b!=null;b=b.Next())
            {
                var k = (Domain)b.key().ShallowReplace(cx, was, now);
                if (k != b.key())
                    rx -= b.key();
                var v = (Domain)b.value().ShallowReplace(cx, was, now);
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
                    ts += (b.key(), r); // to be checked by caller
            }
            return ts;
        }
       internal override (CList<long>, CTree<long, Domain>, CList<long>, BTree<long, long?>, Names, BTree<long,Names>)
ColsFrom(Context cx, long dp,CList<long> rt, CTree<long, Domain> rs, CList<long> sr, 
           BTree<long, long?> tr, Names ns, BTree<long,Names> ds, long ap)
        {
            for (var b=super.First();b!=null;b=b.Next())
                if (b.key() is Table st)
                    (rt, rs, sr, tr, ns, ds) = st.ColsFrom(cx, dp, rt, rs, sr, tr, ns, ds, ap);
            var j = 0;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value()) is DBObject ob && !tr.Contains(ob.defpos))
                {
                    var nm = ob.NameFor(cx);
                    var nn = nm ?? ob.alias;
                    var rv = cx._Ob(cx.names[nn ?? ""].Item2) as SqlReview;
                    var m = rv?.mem??BTree<long, object>.Empty;
                    if (ob is TableColumn tc)
                    {
                        m += (_Domain, tc.domain);
                        var qv = new QlInstance(ap,rv?.defpos??cx.GetUid(), cx, nm??"", dp, tc,m);
                        rt += qv.defpos;
                        sr += ob.defpos;
                        rs += (qv.defpos, qv.domain);
                        tr += (tc.defpos, qv.defpos);
                        ds += (tc.defpos, tc.domain.names);
                        ob = qv;
                        if (rv is not null)
                        {
                            cx.undefined -= rv.defpos;
                            cx.Replace(rv, qv);
                            cx.NowTry();
                        }
                        else
                            cx.Add(qv);
                    }
                    else if (ob is QlInstance sv)
                    {
                        rt += ob.defpos;
                        sr += sv.sPos;
                        tr += (sv.sPos, sv.defpos);
                        rs += (sv.defpos, sv.domain);
                    }
                    else
                    {
                        rt += ob.defpos;
                        sr += ob.defpos;
                        rs += (ob.defpos, ob.domain);
                        tr += (ob.defpos, ob.defpos);
                        ns += (nn ?? ("Col" + j), (ap,ob.defpos));
                    }
                    j++;
                    if (nm != null && ob is QlValue)
                    {
                        ns += (nm, (ap,ob.defpos));
                        cx.Add(nm, ap, ob);
                    }
         //           else
         //               throw new DBException("42105");
                }
            return (rt, rs, sr, tr, ns, ds);
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
                r += (RefIndexes, nr);
            var ss = cx.FixTlTlTlb(sindexes);
            if (ss != sindexes)
                r += (SysRefIndexes, ss);
            var ns = cx.Fix(systemPS);
            if (ns!=systemPS)
                r += (SystemPS, ns);
            var nk = cx.FixTlb(keyCols);
            if (nk != keyCols)
                r += (KeyCols, nk);
            var nc = cx.FixTlb(tableChecks);
            if (nc != tableChecks)
                r += (TableChecks, nc);
            var nw = tableRows;
            for (var b = nw.First(); b != null; b = b.Next())
                if (cx.uids.Contains(b.key()))
                    nw -= b.key();
            if (nw != tableRows)
                r += (TableRows, nw);
            var nt = cx.FixTTElb(triggers);
            if (nt!=triggers)
                r += (Triggers, nt);
            return r;
        }
        protected override void _Cascade(Context cx, Drop.DropAction a, BTree<long, TypedValue> u)
        {
            for (var b = tableChecks.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Check ck)
                    ck.Cascade(cx, a, u);
            for (var b = rowType.First();b!=null;b=b.Next())
                if (cx.db.objects[b.value()] is TableColumn tc)
                        tc.Cascade(cx, a, u);
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix)
                        ix.Cascade(cx, a, u);
        }
        internal override Database Drop(Database d, Database nd)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro && infos[bp] is ObInfo oi
                    && oi.name is not null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += ro;
                }
            return base.Drop(d, nd);
        }
        internal override Database DropCheck(long ck, Database nd)
        {
            return nd + (this + (TableChecks, tableChecks - ck));
        }
        internal virtual Table Base(Context cx)
        {
            return this;
        }
        internal Table Top()
        {
            var t = this;
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key() is Table a)
                {
                    var c = a.Top();
                    if (c.defpos < t.defpos && c.defpos > 0)
                        t = c;
                }
            return t;
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
        internal CTree<long,bool> AllForeignKeys(Context cx)
        {
            var fk = CTree<long, bool>.Empty;
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix &&
                        ix.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                        fk += (ix.defpos, true);
            return fk;
        }
        internal Index[]? FindIndex(Database db, Domain key, 
            PIndex.ConstraintType fl=(PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            var r = BList<Index>.Empty;
            for (var b = indexes[key]?.First(); b != null; b = b.Next())
            if (db.objects[b.key()] is Index x && (x.flags&fl)!=0)
                    r += x;
            return (r==BList<Index>.Empty)?null:r.ListToArray();
        }
        internal Index[]? FindIndex(Database db, CList<long> cols,
    PIndex.ConstraintType fl = (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            var r = BList<Index>.Empty;
            for (var b = indexes?.First(); b != null; b = b.Next())
                if (Context.Match(b.key().rowType,cols))
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (db.objects[c.key()] is Index x && (x.flags & fl) != 0)
                            r += x;
            return (r == BList<Index>.Empty) ? null : r.ListToArray();
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, long ap,
            Grant.Privilege pr=Grant.Privilege.Select, string? a=null, TableRowSet? ur = null)
        {
            cx.Add(this);
            cx.Add(framing);
            var m = mem + (_From, fm) + (_Ident,id);
            if (a != null)
                m += (_Alias, a);
            var rowSet = (RowSet)cx._Add(new TableRowSet(id.uid, cx, defpos, ap, m));
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
        internal virtual Table? Delete(Context cx, Delete del)
        {
            if (cx._Ob(defpos) is not Table a) return null;
            for (var b=a.super.First();b!=null;b=b.Next())
                (b.key() as Table)?.Delete(cx, del);
            if (a.tableRows[del.delpos] is TableRow delRow)
            {
                for (var b = indexes.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx.db.objects[c.key()] is Index ix &&
                            ix.rows is MTree mt && ix.rows.info is Domain inf &&
                            delRow.MakeKey(ix) is CList<TypedValue> key)
                        {
                            ix -= (key, delRow.defpos);
                            if (ix.rows == null)
                                ix += (Index.Tree, new MTree(inf, mt.nullsAndDuplicates, 0));
                            cx.Install(ix);
                        }
                if (cx.db is Transaction tr && del.delpos > Transaction.TransPos)
                    for (var b = tr.physicals.First(); b != null; b = b.Next())
                        if (b.value().ppos == del.delpos)
                        {
                            tr += (Transaction.Physicals, tr.physicals - b.key());
                            break;
                        }
            }
            var tb = a;
            tb -= del.delpos;
            tb += (LastData, del.ppos);
            cx.Install(tb);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (del.ppos, del.type));
            return tb;
        }
        internal Table? SubDel(Context cx, Delete del)
        {
            if (tableRows[del.delpos] is not TableRow delRow)
                return this;
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix &&
                        ix.rows is MTree mt && ix.rows.info is Domain inf &&
                        delRow.MakeKey(ix) is CList<TypedValue> key)
                    {
                        ix -= (key, delRow.defpos);
                        if (ix.rows == null)
                            ix += (Index.Tree, new MTree(inf, mt.nullsAndDuplicates, 0));
                        cx.Install(ix);
                    }
            var tb = this;
            tb -= del.delpos;
            tb += (LastData, del.ppos);
            cx.Install(tb);
            for (var b = subtypes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table t)
                    t.SubDel(cx, del);
            return tb;
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
        internal override TRow RoleClassValue(Context cx, RowSet from,
            ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi)
                throw new DBException("42105").Add(Qlx.ROLE);
            var nm = NameFor(cx);
            var versioned = mi.metadata.Contains(Qlx.ENTITY);
            var key = BuildKey(cx, out Domain keys);
            var fields = CTree<string, bool>.Empty;
            var sb = new StringBuilder("\r\nusing Pyrrho;\r\nusing Pyrrho.Common;\r\n");
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
            sb.Append("[Table("); sb.Append(defpos); sb.Append(','); sb.Append(lastChange); sb.Append(")]\r\n");
            sb.Append("public class " + nm + (versioned ? " : Versioned" : "") + " {\r\n");
            Note(cx, sb);
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo fi && fi.name != null)
                {
                    fields += (fi.name, true);
                    var dt = b.value();
                    var tn = ((dt is Table) ? dt.name : dt.SystemType.Name) + "?"; // all fields nullable
                    tc.Note(cx, sb);
                    if ((keys.rowType.Last()?.value() ?? -1L) == tc.defpos && dt.kind == Qlx.INTEGER)
                        sb.Append("  [AutoKey]\r\n");
                    sb.Append("  public " + tn + " " + tc.NameFor(cx) + ";\r\n");
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
                        if (!rt.metadata.Contains(Qlx.ENTITY))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        var fn = cx.NameFor(rx.keys[0]??-1L)??"";
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + "? " + rn
                            + "=> conn?.FindOne<" + rt.name + ">((\""+fn.ToString()+"\"," + sa.ToString() + "));\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (rt.metadata.Contains(Qlx.ENTITY))
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
                                if (bb.value() is long p && c.value().representation[p] is DBObject ob && 
                                        ob.infos[ro.defpos] is ObInfo vi && vi.name is not null){
                                    sa.Append(cm); cm = ",";
                                    sa.Append(vi.name);
                                }
                                sb.Append("  public " + rt.name + "? " + rn
                                    + "s => conn?.FindOne<" + rt.name + ">((\"" +sa.ToString()+"\","+ sa.ToString() + "));\r\n");
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
                            sb.Append("  public " + rt.name + "[]? " + rn
                                + "s => conn?.FindWith<" + rt.name + ">("+ sa.ToString() + ");\r\n");
                        }
                    else //  e.g. this is Brand
                    {
                        // tb is auxiliary table e.g. BrandSupplier
                        for (var d = tb.indexes.First(); d != null; d = d.Next())
                            for (var e = d.value().First(); e != null; e = e.Next())
                                if (cx.db.objects[e.key()] is Index px && px.reftabledefpos != defpos
                                            && cx.db.objects[px.reftabledefpos] is Table ts// e.g. Supplier
                                            && ts.infos[ro.definer] is ObInfo ti &&
                                            ti.metadata.Contains(Qlx.ENTITY) &&
                                            ts.FindPrimaryIndex(cx) is Index tx)
                                {
                                    var sk = new StringBuilder(); // e.g. Supplier primary key
                                    var cm = "\\\"";
                                    for (var c = tx.keys.First(); c != null; c = c.Next())
                                        if (c.value() is long p && representation[p] is DBObject ob 
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
                                    sb.Append("  public " + ti.name + "[]? " + rn
                                        + "s => conn?.FindIn<" + ti.name + ">(\"select "
                                        + sk.ToString() + " from \\\"" + rt.name + "\\\" where "
                                        + sa.ToString() + "\");\r\n");
                                }
                    }
                }
            sb.Append("}\r\n");
            return new TRow(from, new TChar(name), new TChar(key),
                new TChar(sb.ToString()));
        }
        internal override void Note(Context cx, StringBuilder sb, string pre = "/// ")
        {
            if (infos[cx.role.defpos] is ObInfo ci && ci.name != null)
            {
                if (ci.description?.Length > 1)
                    sb.Append(pre + ci.description + "\r\n");
                for (var d = ci.metadata.First(); d != null; d = d.Next())
                    if (pre == "/// ")
                        switch (d.key())
                        {
                            case Qlx.X:
                            case Qlx.Y:
                                sb.Append(" [" + d.key().ToString() + "]\r\n");
                                break;
                        }
            }
        }
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Context cx, RowSet from, 
            ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || kind==Qlx.Null || from.kind ==Qlx.Null
                || cx.db.user is not User ud)
                throw new DBException("42105").Add(Qlx.ROLE);
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(NameFor(cx)); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + cx.db.name + ", Role " 
                + ro.name + "\r\n */\r\n");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(cx,out Domain keys);
            sb.Append("\r\n@Schema("); sb.Append(lastChange); sb.Append(')');
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(ud.name); sb.Append("\r\n */\r\n");
            if (mi.description != "")
                sb.Append("/* " + mi.description + "*/\r\n");
            var su = new StringBuilder();
            var cm = "";
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key().name != "")
                {
                    su.Append(cm); cm = ","; su.Append(b.key().name);
                }
            if (this is UDType && super.Count!=0L)
                sb.Append("public class " + mi.name + " extends " + su.ToString() + " {\r\n");
            else
                sb.Append("public class " + mi.name + (versioned ? " extends Versioned" : "") + " {\r\n");
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && tableCols[bp] is Domain dt)
                {
                    var tn = (dt.kind == Qlx.TYPE) ? dt.name : Java(dt.kind);
                    if (keys != null)
                    {
                        int j;
                        for (j = 0; j < keys.Length; j++)
                            if (keys[j] == bp)
                                break;
                        if (j < keys.Length)
                            sb.Append("  @Key(" + j + ")\r\n");
                    }
                    dt.FieldJava(cx, sb);
                    sb.Append("  public " + tn + " " + cx.NameFor(bp) + ";");
                    sb.Append("\r\n");
                }
            sb.Append("}\r\n");
            return new TRow(from,new TChar(name),new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || kind==Qlx.Null || from.kind == Qlx.Null)
                throw new DBException("42105").Add(Qlx.ROLE);
            var versioned = true;
            var sb = new StringBuilder();
            var nm = NameFor(cx);
            sb.Append("# "); sb.Append(nm); 
            sb.Append(" from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            var key = BuildKey(cx, out Domain keys);
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            var su = new StringBuilder();
            var cm = "";
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key().name != "")
                {
                    su.Append(cm); cm = ","; su.Append(b.key().name);
                }
            if (this is UDType && super.Count!=0L)
                sb.Append("public class " + nm + "(" + su.ToString() + "):\r\n");
            else
                sb.Append("class " + nm + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            for(var b = representation.First();b is not null;b=b.Next())
            {
                sb.Append("  self." + cx.NameFor(b.key()) + " = " + b.value().defaultValue);
                sb.Append("\r\n");
            }
            sb.Append("  self._schemakey = "); sb.Append(from.lastChange); sb.Append("\r\n");
            if (keys!=Null)
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
            return new TRow(from, new TChar(name),new TChar(key),
                new TChar(sb.ToString()));
        }
        internal virtual string BuildKey(Context cx,out Domain keys)
        {
            keys = Row;
            for (var xk = indexes.First(); xk != null; xk = xk.Next())
                for (var c = xk.value().First();c is not null;c=c.Next())
            if (cx.db.objects[c.key()] is Index x && x.tabledefpos == defpos &&
                        (x.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey)
                    keys = x.keys;
            var comma = "";
            var sk = new StringBuilder();
            if (keys != Row)
                for (var i = 0; i < keys.Length; i++)
                    if ( cx.db.objects[keys[i]??-1L] is TableColumn cd)
                    {
                        sk.Append(comma); comma = ",";
                        sk.Append(cd.NameFor(cx));
                    }
            return sk.ToString();
        }
        internal override TRow RoleSQLValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is null
                || kind == Qlx.Null || from.kind == Qlx.Null)
                throw new DBException("42105").Add(Qlx.ROLE);
            var sb = new StringBuilder();
            sb.Append("--"); sb.Append(name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n-- from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            var key = BuildKey(cx, out Domain _);
            sb.Append("create view ");sb.Append(name); sb.Append(" of (");
            var cm = "";
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string cs && representation[p] is Domain cd) {
                    sb.Append(cm); cm = ",";
                    sb.Append(cs); sb.Append(' '); sb.Append(cd.name);
                }
            sb.Append(")\r\n as get 'https://yourhost:8180/");
            sb.Append(cx.db.name);sb.Append('/');sb.Append(ro.name);sb.Append('/');sb.Append(name);sb.Append("'\r\n");
            return new TRow(from, new TChar(name), new TChar(key),
                new TChar(sb.ToString()));
        }

        internal virtual void Update(Context cx, TableRow prev, CTree<long, TypedValue> fields)
        {  }
    }
} 
