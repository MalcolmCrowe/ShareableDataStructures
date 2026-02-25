using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// The Domain base has columns and includes inherited columns, but not all Domains with Columns are Tables 
    /// (e.g. Keys, Orderings and Groupings are domains with columns but are not tables).
    /// Tables are not always associated with base tables either: the valueType of a View
    /// or SqlCall can be a Table.
    /// From v 7.04 we identify Table with Relation Type, hence Table is a kind of Domain
    /// that also has a collection of rows.
    /// Any TableRow is entered into the Tables of its datatype and its supertypes if any.
    /// (thus the Domain of any tableRow is a Table).
    /// Rows with IRI metadata always have a table supertype of their domain.
    /// When a Table is accessed any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// From v7.1 the rowType is a CTree(int,long), not a CList(long): for base tables 
    /// this facilitates ensuring that reference columns come after non-reference columns (left-to-right
    /// semantics in evaluation of queries).
    /// The following are used in integrity constraints:
    ///   Domain.Colrefs: for a domain, associates referenced types to referencing columns
    ///   Table.Indexes: Associates collections of key columns with indexes
    ///   Table.RefIndexes: tracks the use of rows by a referencing index (SQL) deprecated
    ///   Table.SysRefIndexes: tracks the use of rows by referencing rows
    /// When relationships between rows are defined structurally (as in the graph insert statement)
    /// there is less need to specify value-based foreign keys, but if defined they need to be updated
    /// automatically by the implementation when a reference is changed.
    /// </summary>
    internal class Table : Domain
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege
            Indexes = -264, // CTree<Domain,CTree<long,bool>> QlValue,Index
            LastData = -258, // long
            MultiplicityIndexes = -467, // CTree<long,long> Column,Index
            RefCols = -271, // CTree<long,bool> TableColumn referencing this table 
            SysRefIndexes = -111, // CTree<long,CTree<long,CTree<long,CTree<long,bool>>>>
                            // referencedTable.sindexes[referencedrec][referencingcol][referencingrec]
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // CTree<long,bool> CheckFields
            TableRows = -181, // BTree<long,TableRow>
            Triggers = -267; // CTree<PTrigger.TrigType,CTree<long,bool>> 
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
        internal override bool Defined() => defpos > 0;
        internal virtual CTree<long, Domain> tableCols => representation;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal CTree<long, bool> refCols => (CTree<long, bool>)(mem[RefCols] ?? CTree<long, bool>.Empty);
        internal CTree<long, CTree<long, CTree<long, bool>>> sindexes =>
            (CTree<long, CTree<long, CTree<long, bool>>>)(mem[SysRefIndexes]
            ?? CTree<long, CTree<long, CTree<long, bool>>>.Empty);

        // For N-ary edges and advanced multiplicity generally (Fritz Laux)
        // An n-ary association is a relation of degree n+x (x= number of properties)
        // with a compound primary key of n-attributes.
        // The multiplicity of a column defines the minimum (participation)
        // and maximum  (cardinality) number of values (nodes)
        // that can occur if you hold the other n-1 columns fixed. 
        // Multiplicity is a column metadata (MINVALUE,MAXVALUE) that leads to
        // provision of a MultiplicityIndex on the Table.
        // This type of constraint brings a run-time cost O(NM) on insert/delete
        // where N is the size of the table and M is the number of mindexes on it.
        internal CTree<long, long> mindexes =>
            (CTree<long, long>)(mem[MultiplicityIndexes] ?? CTree<long, long>.Empty);
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
        internal Table(Qlx t) : base(--_uid,t, BTree<long,object>.Empty) { }
        internal Table(Context cx,CTree<long,Domain>rs, CTree<int,long> rt, int ds, BTree<long,ObInfo> ii)
            : base(cx,rs,rt,ds,ii) { }
        internal Table(long dp, Context cx, CTree<long, Domain> rs, CTree<int,long> rt, int ds)
            : base(dp, cx, Qlx.TABLE, rs, rt, ds) { }
        public Table(long dp, CTree<Domain, bool> u)
            : base(dp, u)
        { }
        internal Table(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        /// <summary>
        /// The Node Type and PathDomain things make this operation more complex,
        /// For multiple inheritance the subtype has a new uid for identity and is used here.
        /// but otherwise we do not worry about identity inheritance (this is fixed in Record.Install).
        /// Although this is not theoretically necessary, for presentational purposes 
        /// we adjust so that our identity column is first (seq=0), followed by leaving and arriving if any,
        /// other properties in declaration order.
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="x">(Context,TableColumn)</param>
        /// <returns>The updated Table including updated PathDomain (note TableColumn may be changed)</returns>
        /// <exception cref="DBException"></exception>
        /// <exception cref="PEException"></exception>
        public static Table operator +(Table tb, (Context, TableColumn) x)
        {
            var (cx, tc) = x;
            if (tb.representation.Contains(tc.defpos))
            // Column exists but may have new Connector metadata
            {
                cx.db += tc;
                cx._Add(tc);
                return tb;
            }
            // Column is new
            var ci = tc.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN);
            var ns = tb.names;
            //      tc += (TableColumn.Seq, tb.Length);
            if (ci.name != null)
                ns += (ci.name, (0L, tc.defpos));
            cx.Add(tc);
            if (!tb.representation.Contains(tc.defpos))
            {
                tb += (Representation, tb.representation + (tc.defpos, tc.domain));
                tb += (ObInfo._Names, tb.names);
            }
            // fix rowType: hidden columns come last (tc might be hidden)
            var nrt = CTree<int, long>.Empty;
            var hdn = CTree<int, long>.Empty;
            int h=0,j = 0;
            for (var b = tb.rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value()) is TableColumn c)
                    if (c.hide) hdn += (h++, b.value());
                    else nrt += (j++, b.value());
            if (tc.hide) hdn += (h++, tc.defpos);
            else nrt += (j++, tc.defpos);
            tb += (Display, j);
            for (var b = hdn.First(); b != null; b = b.Next())
                nrt += (j++, b.value());
            tb += (RowType, nrt);
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
            cx.Add(tb);
            cx.db += (tb.defpos, tb);
            tb += (Dependents, tb.dependents + (tc.defpos, true));
            if (tc.sensitive)
                tb += (Sensitive, true);
     /*       for (var b = tc.cs.First(); b != null; b = b.Next())
                if (b.value() is TConnector nc && nc.rd is Table rt)
                    cx.Add(rt + (RefCols, rt.refCols + (tc.defpos, true))); */
            var nr = CTree<int, long>.Empty;
            var hd = CTree<int, long>.Empty;
            var ds = 0;
            for (var b = tb.rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value()) is TableColumn c && c.hide)
                    hd += ((int)hd.Count, b.value());
                else
                    nr += (ds++, b.value());
            for (var b = hd.First(); b != null; b = b.Next())
                nr += (ds + b.key(), b.value());
            tb = tb + (RowType, nr) + (Display, ds);
            cx._Add(tb);
            cx.db += tb;
            return (Table)cx.Add(tb);
        }
/*        /// <summary>
        /// Metadata for a reference column has been changed: update the referencing Table
        /// </summary>
        /// <param name="dm"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        /// <exception cref="PEException"></exception>
        public static Table operator +(Table dm, (Context, TableColumn, ABookmark<Qlx, TypedValue>) x)
        {
            var (cx, rc, b) = x;
            var rs = rc.cs;
            for (var c = rc.cs.First(); c != null; c = c.Next())
                if (c.value() is TConnector cc
                    && cx._Ob(rc.tabledefpos) is Table tb && tb.defpos > 0
                    && cc.cm is TMetadata mc)
                {
                    var md = mc + (b.key(), b.value());
                    var ms = md.ToString();
                    rs += (cc.rd,new TConnector(cc.q, cc.cn, cc.rd, cc.cp, cc.fk, ms, md));
                    rc += (Infos, rc.infos + (cx.role.defpos,
                            (rc.infos[cx.role.defpos] ?? throw new DBException("42105"))
                            + (ObInfo._Metadata, md)));
                    rc += (TableColumn.Connectors, rs);
                }
            cx.db += rc;
            cx.Add(rc);
            dm = (Table)(cx.db.objects[dm.defpos] ?? dm);
            dm += (cx,rc);
            cx.Add(dm);
            cx.db += dm;
            return dm;
        } */
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
        internal override long ColFor(Context cx, string c, Qlx kt=Qlx.NO)
        {
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is DBObject ob &&
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
        internal override (Context,DBObject) Add(Context cx, TMetadata md)
        {
            (cx,var ob) = base.Add(cx, md);
            for (var b = md.First(); b != null; b = b.Next())
                switch (b.key())
                {
                    case Qlx.SECURITY:
                        {
                            ob = (Table)(cx.Add(new Classify(ob.defpos, 
                                ((TLevel)b.value()).val, cx.db.nextPos, cx.db))
                                ?? throw new DBException("42105"));
                            break;
                        }
                    case Qlx.SCOPE:
                        {
                            if (ob is Table t)
                            ob = (Table)(cx.Add(new Enforcement(t, 
                                (Grant.Privilege)(b.value().ToInt() ?? 0), cx.db.nextPos, cx.db))
                                ?? throw new DBException("42105"));
                            break;
                        }
                    case Qlx.IRI:
                        if (b.value() is TChar iri && ob is Table ut)
                        {
                            var nm = iri.ToString();
                            var nb = (Table)(cx.Add(new PTable(nm, new Table(cx.db.nextPos, mem),
                                cx.db.nextPos, cx)) ?? throw new DBException("42105"));
                            nb += (ObInfo.Name, iri.ToString());
                            nb += (Under, new CTree<Domain, bool>(ut, true));
              //              nb += (Subtypes, ut.subtypes + (nb.defpos, true));
                            ob = nb;
                        }
                        break;
                    case Qlx.MULTIPLICITY:
                        if (b.value() is TSet ts)
                            for (var c = ts.First(); c != null; c = c.Next())
                                if (cx._Ob(c.Value().ToLong() ?? -1L) is TableColumn co
                                    && !mindexes.Contains(co.defpos))
                                    ob = AddMultiplicityIndex(cx, co);
                        break;
                    case Qlx.NODETYPE:
                        {
                            if (ob is EdgeType)
                                break;
                            var nb = (Table)ob._AddMetadata(cx.role.defpos, b.key(), b.value());
                            ob = nb.AddNodeOrEdgeType(cx);
                            break;
                        }
                    case Qlx.EDGETYPE:
                        {
                            var nb = (Table)ob._AddMetadata(cx.role.defpos, b.key(), b.value());
                            ob = nb.AddNodeOrEdgeType(cx);
                            break;
                        }
                }
            cx.db += ob;
            ob = (Table)cx.Add(ob);
            return (cx,ob);
        }
        internal Table AddMultiplicityIndex(Context cx, TableColumn co)
        {
            // construct a foreignkey-type index for all columns with co as the last
            var ks = CTree<int,long>.Empty;
            for (var d = rowType.First(); d != null; d = d.Next())
                if (d.value() != co.defpos)
                    ks += (d.key(),d.value());
            ks += ((int)ks.Count,co.defpos);
            var ob = this;
            var px = new PIndex("M" + co.name, this, new Domain(cx, representation, ks, display,
                BTree<long, ObInfo>.Empty), PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeDelete
                | PIndex.ConstraintType.CascadeUpdate, -1L, cx.db.nextPos, cx.db);
            cx.Add(px); // returns index
            ob = (Table)(cx.db.objects[ob.defpos] ?? throw new DBException("42105"));
            if (cx._Ob(px.defpos) is Index mx)
            {
                ob += (MultiplicityIndexes, ob.mindexes + (co.defpos, mx.defpos));
                mx.Build(cx);
            }
            cx.db += ob;
            return ob;
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
            var ss = ShallowReplace(cx, sindexes, was, now);
            if (ss != sindexes)
                r += (SysRefIndexes, ss);
            var ts = ShallowReplace(cx, tableRows, was, now);
            if (ts != tableRows)
            {
             //   for (var b = ts.First(); b != null; b = b.Next()) // verify that all tablerows have the right types for all vals
             //       b.value()?.CheckFields(r,cx);
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
                if (k!=b.key() || v != b.value())
                    xs += (k, v);
            }
            return xs;
        }
        static CTree<long,bool> ShallowReplace(Context cx,CTree<long,bool> rs,long was,long now)
        {
            var r = CTree<long, bool>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = k == was ? b.key() : k;
                if (nk != k)
                    ch = true;
                r += (nk, true);
            }
            return ch?r:rs;
        }
        static CTree<Domain,bool> ShallowReplace(Context cx,CTree<Domain,bool> rs,long was, long now)
        {
            var r = CTree<Domain,bool>.Empty;
            var ch = false;
            for (var b=rs.First();b!=null;b=b.Next())
            {
                var k = (Domain)b.key().ShallowReplace(cx, was, now);
                r += (k, true);
                if (k!=b.key())
                        ch = true;
            }
            return ch ? r : rs;
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
       internal override (CTree<int,long>, CTree<long, Domain>, CTree<int,long>, CTree<long, long>, Names, BTree<long,Names>)
ColsFrom(Context cx, long dp,CTree<int,long> rt, CTree<long, Domain> rs, CTree<int,long> sr, 
           CTree<long, long> tr, Names ns, BTree<long,Names> ds, long ap)
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
                        rt += ((int)rt.Count, qv.defpos);
                        sr += ((int)sr.Count, ob.defpos);
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
                        rt += ((int)rt.Count, ob.defpos);
                        sr += ((int)sr.Count, sv.sPos);
                        tr += (sv.sPos, sv.defpos);
                        rs += (sv.defpos, sv.domain);
                    }
                    else
                    {
                        rt += ((int)rt.Count, ob.defpos);
                        sr += ((int)rt.Count, ob.defpos);
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
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var na = cx.Fix(applicationPS);
            if (na != applicationPS)
                r += (ApplicationPS, na);
            var ni = cx.FixTDTlb(indexes);
            if (ni != indexes)
                r += (Indexes, ni);
     //       var ss = cx.FixTlTlTlb(sindexes); much too expensive to fix this way
     //       if (ss != sindexes)
     //           r += (SysRefIndexes, ss);
            var ns = cx.Fix(systemPS);
            if (ns != systemPS)
                r += (SystemPS, ns);
            var nk = cx.FixTlb(refCols);
            if (nk != refCols)
                r += (RefCols, nk);
            var nc = cx.FixTlb(tableChecks);
            if (nc != tableChecks)
                r += (TableChecks, nc);
      //      var nw = tableRows;       much too expensive to fix this way
      //      for (var b = nw.First(); b != null; b = b.Next())
      //          if (cx.uids.Contains(b.key()))
      //              nw -= b.key();
      //      if (nw != tableRows)
      //          r += (TableRows, nw);
            var nt = cx.FixTTElb(triggers);
            if (nt != triggers)
                r += (Triggers, nt);
            return r;
        }
        protected override void _Cascade(Context cx, Drop.DropAction a, BTree<long, TypedValue> u)
        {
            for (var b = tableChecks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Cascade(cx, a, u);
            for (var b = rowType.First();b!=null;b=b.Next())
                if (cx._Ob(b.value()) is TableColumn tc)
                        tc.Cascade(cx, a, u);
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Index ix)
                        ix.Cascade(cx, a, u);
        }
        internal override Database Drop(Database d, Database nd)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro && infos[bp] is ObInfo oi
                    && oi.name is not null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    if (ro.nodeTypes[oi.name] == defpos)
                        ro += (Role.NodeTypes, ro.nodeTypes - oi.name);
                    if (ro.edgeTypes[oi.name] == defpos)
                        ro += (Role.EdgeTypes, ro.edgeTypes - oi.name);
                    if (ro.graphs[oi.name] == defpos)
                        ro += (Role.Graphs, ro.graphs - oi.name);
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
        internal virtual Table Specific(Context cx,TableRow tr)
        { 
            return this; 
        }
        internal virtual Index? FindPrimaryIndex(Context cx)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Index ix)
                         return ix;
            return null;
        }
        internal Index[]? FindIndex(Database db, Domain key)
        {
            var r = BList<Index>.Empty;
            for (var b = indexes?.First(); b != null; b = b.Next())
            {
                var ca = key.First();
                var c = b.key().First();
                for (; ca != null && c != null; ca = ca.Next(), c = c.Next())
                    if (((DBObject?)db.objects[ca.value()])?.domain.kind !=
                        ((DBObject?)db.objects[c.value()])?.domain.kind)
                        goto skip;
                if (ca==null && c==null)
                for (var d= b.value().First(); d != null; d = d.Next())
                    if (db.objects[d.key()] is Index x)
                        r += x;
                    skip:;
            }
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
                        if (cx._Ob(c.key()) is Index ix &&
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
                    if (cx._Ob(c.key()) is Index ix &&
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
                if (cx._Ob(b.key()) is Table t)
                    t.SubDel(cx, del);
            return tb;
        }
        public virtual string Describe(Context cx)
        {
            var sb = new StringBuilder();
            var ty = (colRefs.Count>1L) ? "EDGE " : "NODE ";
            sb.Append(ty); sb.Append(name);
            var cm = " {";
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain d)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(cx.NameFor(p));
                    sb.Append(' ');
                    sb.Append(d.ToString());
                }
            if (cm == ",")
                sb.Append('}');
            return sb.ToString();
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" rows ");sb.Append(tableRows.Count);
            if (refCols!=CTree<long,bool>.Empty)
            {
                sb.Append(" refCols "); sb.Append(refCols.ToString());
            }
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
            }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
            if (PyrrhoStart.VerboseMode && mem.Contains(Enforcement)) 
            { sb.Append(" Enforcement="); sb.Append(enforcement); }
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
            if (cx._Ob(c.key()) is Index x && x.tabledefpos == defpos)
                    keys = x.keys;
            var comma = "";
            var sk = new StringBuilder();
            if (keys != Row)
                for (var i = 0; i < keys.Length; i++)
                    if ( cx._Ob(keys[i] ?? -1L) is TableColumn cd)
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
        static Random ran = new(0);
        /// <summary>
        /// My current idea is that given a  single node n, Pyrrho should be able to compute a list of nearby nodes 
        /// with x,y coordinates relative to n, in the following format
        /// (nodetype, id, x, y, lv, ar)
        /// Line 0 of this list should be the given node n, 
        /// lv and ar are the line numbers of the leaving and arriving nodes for edges.
        /// Nearby means that -500<x<500 and -500<y<500.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="start"></param>
        /// <returns></returns>
        internal static (BList<NodeInfo>, CTree<Table, int>) NodeTable(Context cx, TNode start)
        {
            var types = new CTree<Table, int>((Table)start.dataType, 0);
            var ntable = new BList<NodeInfo>(new NodeInfo(0,
                new TChar(start.defpos.ToString()), 0F, 0F, -1L, -1L, start.Summary(cx)));
            var nodes = new CTree<TNode, int>(start, 0);  // nodes only, no edges
            var edges = CTree<TEdge, int>.Empty;
            var todo = new BList<TNode>(start); // nodes only, no edges
            ran = new Random(0);
            while (todo.Count > 0)
            {
                var tn = todo[0]; todo -= 0;
            }
            return (ntable, types);
        }
        internal virtual Table AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var r = this;
            var oi = infos[cx.role.defpos] ?? new ObInfo(name);
            oi += (Method.TypeDef, this);
            r += (Infos, infos + (cx.role.defpos, oi));
            cx.db += r;
            var md = oi.metadata;
            if (md.Contains(Qlx.NODETYPE))
                ro += (Role.NodeTypes, ro.nodeTypes + (NameFor(cx), defpos));
            if (md.Contains(Qlx.EDGETYPE))
                ro += (Role.EdgeTypes, ro.edgeTypes + (NameFor(cx), defpos));
            if (md.Contains(Qlx.GRAPH))
                ro += (Role.Graphs, ro.graphs + (NameFor(cx), defpos));
            cx.db += ro;
            cx.db += (Database.Role, cx.db.objects[cx.role.defpos] ?? throw new DBException("42105"));
            return r;
        }
        // We have been updated. Ensure that all our subtypes are updated
        internal void Refresh(Context cx)
        {
            for (var b = subtypes?.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table bd)
                {
                    bd = bd + (Under, bd.super + (this, true));// + (IdIx, idIx) + (IdCol, idCol);
                    cx.db += bd;
                    bd.Refresh(cx);
                }
        }
        internal TypedValue PreConnect(Context cx, Qlx ab, Domain ct, string cn)
        {
            var q = ab;
            switch (ab)
            {
                case Qlx.ARROWBASE: q = Qlx.FROM; break;
                case Qlx.RARROW: q = Qlx.TO; break;
                case Qlx.ARROWBASETILDE:
                case Qlx.TILDE:
                case Qlx.RBRACKTILDE: q = Qlx.WITH; break;
                case Qlx.TO:
                case Qlx.FROM:
                case Qlx.WITH: break;
                default: return TNull.Value;
            }
            TypedValue r = TNull.Value;
            for (var b = colRefs.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is TableColumn cc && cx._Ob(b.key()) is Domain dm)
                        if ((ct.defpos < 0 || ct.EqualOrStrongSubtypeOf(dm))
                            && (model[q] == cc.defpos||cc.NameFor(cx)==cn))
                            return new TConnector(q, cc.NameFor(cx), dm, cc.defpos);
            if (r == TNull.Value && cx.parsingGQL != Context.ParsingGQL.Match &&
                BuildNodeTypeConnector(cx, new TConnector(q, cn, ct), this).Item2 is TConnector nc
                && nc.rd.defpos > 0)
                return nc;
            return r;
        }
        internal TypedValue PostConnect(Context cx, Qlx ba, Domain ct, string cn)
        {
            var q = ba;
            switch (ba)
            {
                case Qlx.ARROW: q = Qlx.TO; break;
                case Qlx.RARROWBASE: q = Qlx.FROM; break;
                case Qlx.ARROWBASETILDE:
                case Qlx.TILDE:
                case Qlx.RBRACKTILDE: q = Qlx.WITH; break;
                case Qlx.TO:
                case Qlx.FROM:
                case Qlx.WITH: break;
                default: return TNull.Value;
            }
            TypedValue r = TNull.Value;
            for (var b = colRefs.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is TableColumn cc && cx._Ob(b.key()) is Domain dm)
                            if ((ct.defpos<0||ct.EqualOrStrongSubtypeOf(dm))
                                && (model[q] ==cc.defpos || cc.NameFor(cx)==cn))
                                return new TConnector(q,cc.NameFor(cx),dm,cc.defpos);
            if (r == TNull.Value && cx.parsingGQL != Context.ParsingGQL.Match &&
                BuildNodeTypeConnector(cx, new TConnector(q, cn, ct), this).Item2 is TConnector nc
                && nc.rd.defpos > 0)
                return nc;
            return r;
        }
        (Table, TConnector) BuildNodeTypeConnector(Context cx, TConnector tc, Table nt)
        {
            var ut = cx.FindTable(name);
            if (ut is null)
                return (nt, tc);
            var cs = CTree<(Qlx, Domain), CTree<TypedValue, bool>>.Empty;
            var ns = CTree<string, TConnector>.Empty;
            var nst = cx.db.nextStmt;
            var k = 1;
            for (var b=ut.colRefs[model[tc.q]]?.First(); b != null; b = b.Next()) // ??
                    if (cx.db.objects[b.key()] is Table ct)
                    return (ct, tc); //??
            var dn = tc.rd ?? throw new PEException("PE90152"); // dn might be a Union of NodeTypes
            var tt = cs[(tc.q, dn)];
            var cn = (tc.cn == "") ? (tc.q.ToString() + k) : tc.cn;
            if (ns[cn] is TConnector x && x.cp > 0L)
                return (ut, x);
            if (cx._Ob(ut.names[tc.cn.ToUpper()].Item2) is TableColumn ec)
                return (ut,new TConnector(tc.q, tc.cn, tc.rd, ec.defpos, tc.fk, tc.cs, tc.cm));
            var tn = new TConnector(tc.q, tc.cn, tc.rd, cx.db.nextPos, tc.fk, tc.cs, tc.cm);
            var md = (tc.cm ?? TMetadata.Empty) + (Qlx.CONNECTING, tn) + (Qlx.OPTIONAL, TBool.False);
            TableColumn nc;
            if (ut.infos[cx.role.defpos] is ObInfo ui && cx._Ob(ui.names[cn.ToUpper()].Item2) is TableColumn nc1)
                nc = nc1;
            else {
                var pc = new PColumn3(ut, cn, Ref, "", md, nst, cx.db.nextPos, cx, false)
                { reftype = tc.rd.defpos};
                pc.FromTCon(tn);
                ut = (Table)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                nc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105").Add(Qlx.COLUMN));
            }
            var di = new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(nc), 1);
            ut = (Table)(cx.db.objects[ut.defpos] ?? throw new DBException("42105").Add(Qlx.REF));
            var um = ut.infos[ut.definer]?.metadata[Qlx.REFERENCES] as TSet ?? new TSet(Connector);
            ut = (Table)cx.Add(ut, TMetadata.Empty + (Qlx.REFERENCES, um + tn)).Item2;
            ut = ut.AddNodeOrEdgeType(cx);
            return ((Table)cx.Add(ut), tn);
        }
        internal (Table,TypedValue) Connect(Context cx, GqlNode? b, GqlNode? a, TypedValue cc,
     bool allowChange = false, long dp = -1L)
        {
            if (cc is not TConnector ec || ec.cp > 0L)
                return (this,TNull.Value);
            var found = false;
            var os = infos[definer]?.metadata[Qlx.REFERENCES] as TSet;
            for (var c = os?.First(); c != null; c = c.Next())
                if (c.Value() is TConnector tc)
                {
                    long bt = (b?.domain ?? TypeSpec).defpos;
                    long at = (a?.domain ?? TypeSpec).defpos;
                    var cn = (ec.cn == "") ? ec.cn : tc.cn;
                    TypedValue qv = tc.q switch
                    {
                        Qlx.TO => ec.q switch
                        {
                            Qlx.ARROW => new TConnector(tc.q, tc.cn, tc.rd),
                            Qlx.RARROW => new TConnector(tc.q, tc.cn, tc.rd),
                            _ => TNull.Value
                        },
                        Qlx.FROM => ec.q switch
                        {
                            Qlx.ARROWBASE => new TConnector(tc.q, tc.cn, tc.rd),
                            Qlx.RARROWBASE => new TConnector(tc.q, tc.cn, tc.rd),
                            _ => TNull.Value
                        },
                        Qlx.WITH => ec.q switch
                        {
                            Qlx.ARROWBASETILDE => new TConnector(tc.q, tc.cn, tc.rd),
                            Qlx.RBRACKTILDE => new TConnector(tc.q, tc.cn, tc.rd),
                            Qlx.TILDE => new TConnector(tc.q, tc.cn, tc.rd) ??
                                        new TConnector(tc.q, tc.cn, tc.rd),
                            _ => TNull.Value
                        },
                        _ => TNull.Value
                    };
                    if (qv != TNull.Value)
                        found = true;
                }
            var r = this;
            if (!found)
            {
                long nn = -1L;
                Qlx q = Qlx.Null;
                long bt = (b?.domain ?? TypeSpec).defpos;
                long at = (a?.domain ?? TypeSpec).defpos;
                if (b != null) switch (ec.q)
                    {
                        case Qlx.ARROWBASE: q = Qlx.FROM; nn = bt; break;
                        case Qlx.ARROW: q = Qlx.TO; nn = at; break;
                        case Qlx.RARROW: q = Qlx.TO; nn = bt; break;
                        case Qlx.RARROWBASE: q = Qlx.FROM; nn = at; break;
                        case Qlx.ARROWBASETILDE: q = Qlx.WITH; nn = bt; break;
                        case Qlx.RBRACKTILDE: q = Qlx.WITH; nn = at; break;
                    }
                var cn = (ec.cn == "") ? q.ToString() : ec.cn;
                var nc = new TConnector(q, cn, cx.db.objects[nn] as Domain??Null);
                var ns = os;
                if (nn > 0 && defpos > 0)
                {
                    (r, nc) = BuildNodeTypeConnector(cx, nc);
                    ns = (os ?? new TSet(Connector)) + nc;
                }
                else
                {
                    ns = (os ?? new TSet(Connector)) + nc;
                    cx.MetaPend(dp + 1L, dp + 1L, cn,
                        TMetadata.Empty + (Qlx.REFERENCES, ns));
                }
                if (ns!=os && ns.Cardinality() >= 1)
                    r = (Table)cx.Add(r,TMetadata.Empty+ (Qlx.REFERENCES, ns)).Item2;
            }
            return (r,(TypedValue?)os??TNull.Value);
        }
        internal (Table, CTree<string, QlValue>) Connect(Context cx, TNode? b, TNode a, GqlEdge ed, TypedValue cc,
                CTree<string, QlValue> ls, bool allowChange = false)
        {
            if (cc is not TConnector nc)
                return (this, ls);
            var found = false;
            for (var c = (infos[definer]?.metadata[Qlx.REFERENCES] as TSet)?.First(); c != null; c = c.Next())
                if (c.Value() is TConnector tc)
                {
                    TypedValue qv = tc.q switch
                    {
                        Qlx.TO => nc.q switch
                        {
                            Qlx.ARROW or Qlx.ARROWR => Connect(cx, a, nc, tc, ed), // ]-> ->
                            Qlx.RARROW or Qlx.ARROWL => Connect(cx, b, nc, tc, ed), // <-[ <-
                            _ => TNull.Value
                        },
                        Qlx.FROM => nc.q switch
                        {
                            Qlx.ARROWBASE or Qlx.ARROWR => Connect(cx, b, nc, tc, ed), // -[ ->
                            Qlx.RARROWBASE or Qlx.ARROWL => Connect(cx, a, nc, tc, ed), // ]- <-
                            _ => TNull.Value
                        },
                        Qlx.WITH => nc.q switch
                        {
                            Qlx.ARROWLTILDE or Qlx.RARROWTILDE or Qlx.ARROWBASETILDE // <~ <~[ ~[
                                => Connect(cx, b, nc, tc, ed),
                            Qlx.RBRACKTILDE or Qlx.ARROWTILDE or Qlx.ARROWRTILDE // ]~ ]~> ~>
                                => Connect(cx, a, nc, tc, ed),
                            Qlx.TILDE => Connect(cx, a, nc, tc, ed) ?? Connect(cx, b, nc, tc, ed), // ~
                            _ => TNull.Value
                        },
                        _ => TNull.Value
                    };
                    if (qv != TNull.Value)
                    {
                        var n = cx.NameFor(tc.cp) ?? tc.cn;
                        if (tc.rd.kind == Qlx.REF)
                            ls += (n, new SqlLiteral(cx.GetUid(), qv));
                        else
                        {
                            var ov = ls[tc.cn]?._Eval(cx) ?? TNull.Value;
                            ls += (n, new SqlLiteral(cx.GetUid(), tc.rd.Coerce(cx, qv + ov)));
                        }
                        found = true;
                        break;
                    }
                }
            var r = this;
            if (!found)
            {
                TNode? nn = null;
                Qlx q = Qlx.Null;
                long bt = (b == null) ? -1L : b.dataType.defpos;
                long at = a.dataType.defpos;
                if (b != null) switch (nc.q)
                    {
                        case Qlx.ARROWBASE: q = Qlx.FROM; nn = b; break;
                        case Qlx.ARROW: q = Qlx.TO; nn = a; break;
                        case Qlx.RARROW: q = Qlx.FROM; nn = a; break;
                        case Qlx.RARROWBASE: q = Qlx.TO; nn = b; break;
                        case Qlx.ARROWBASETILDE: q = Qlx.WITH; nn = b; break;
                        case Qlx.RBRACKTILDE: q = Qlx.WITH; nn = a; break;
                    }
                if (nn != null)
                {
                    (r, var rc) = BuildNodeTypeConnector(cx,
                        new TConnector(q, nc.cn,nn.dataType));
                    ls += (cx.NameFor(rc.cp) ?? rc.cn,
                        (SqlLiteral)cx.Add(new SqlLiteral(cx.GetUid(), 
                            new TRef(nn.defpos, nc.rd ??Null))));
                }
            }
            return (r, ls);
        }
        internal static TypedValue Connect(Context cx, TNode? n, TConnector nc, TConnector ec, GqlEdge ed,
            CTree<long, long>? rn = null)
        {
            if (n == null || (nc.cn != "" && ec.cn.ToUpper() != nc.cn.ToUpper()))
                return TNull.Value;
       /*     if (nc.cd is SqlTypeExpr gl && gl.Eval(cx).dataType is Domain ld
                && ld.kind == Qlx.AMPERSAND)
                for (var b = gl._NodeTypes(cx).First(); b != null; b = b.Next())
                    if (Connect(cx, n, (Table)b.key(), nc, ec, ed, rn) is TypedValue v && v != TNull.Value)
                        return v;*/
            var nt = n.dataType as Table?? throw new DBException("42105");
            return Connect(cx, n, nt, nc, ec, ed, rn)
                        ?? ((cx.values[rn?[n.defpos] ?? -1L] is TNode m) ?
                        Connect(cx, m, m.dataType as Table, nc, ec, ed, rn) ?? TNull.Value
                        : TNull.Value);
        }
        static TypedValue? Connect(Context cx, TNode n, Table? nt, TConnector nc, TConnector ec, GqlEdge ed,
            CTree<long, long>? rn = null)
        {
            if (nt is null)
                return null;
            if (ec.rd != nt && ec.rd is Domain en)
            {
                if (en is Table ut && !nt.EqualOrStrongSubtypeOf(en))
                    return null;
            }
            var m = (rn?[n.defpos] is long mp && mp > 0) ? mp : n.defpos;
            if (/*ec.cd.kind == Qlx.REF && */ cx._Ob(n.tableRow.tabledefpos) is Domain rd)
                return new TRef(m,rd);
            if (ec.rd.kind == Qlx.SET && ec.rd is Domain de && cx._Ob(n.tableRow.tabledefpos) is Domain sd)
                return /*(de.kind == Qlx.REF) ? */new TRef(m, sd) /*: n*/;
            if (ec.rd is Domain d && n.dataType.EqualOrStrongSubtypeOf(d))
                return n;
            throw new DBException("22G0V");
        }
        internal (Table, TConnector) BuildNodeTypeConnector(Context cx, TConnector tc)
        {
            if (infos[definer]?.metadata[Qlx.REFERENCES] is TSet ts && ts.Contains(tc))
                return (this, tc);
            var d = tc.rd ?? throw new PEException("PE90151");
            if (d is Table nt)
                return BuildNodeTypeConnector(cx, tc, nt);
            else
                for (var c = d.alts.First(); c != null; c = c.Next())
                    if (c.key() is Table ct)
                        return BuildNodeTypeConnector(cx, tc, ct);
            throw new PEException("PE40721");
        }
        /// <summary>
        /// We have a new node type cs and have been given columns ls
        /// New columns specified are added or inserted.
        /// We will construct Physicals for new columns required
        /// </summary>
        /// <param name="x">The GqlNode or GqlEdge if any to apply this to</param>
        /// <param name="ll">The properties from an inline document, or default values</param>
        /// <param name="cs">A list of TConnectors for EdgeType</param>
        /// <returns>The new node type: we promise a new PNodeType for this</returns>
        /// <exception cref="DBException"></exception>
        internal virtual Table Build(Context cx, GqlNode? x, long _ap, string nm, CTree<string, QlValue> dc,
            Qlx q = Qlx.NO, Table? nt = null, CList<TypedValue>? cs = null)
        {
            var ut = this;
            var md = infos[definer]?.metadata??TMetadata.Empty;
            var nst = cx.db.nextStmt;
            var e = x as GqlEdge;
            if (defpos < 0)
                return this;
            var ls = x?.docValue ?? CTree<string, QlValue>.Empty;
            ls += dc;
            if (name is not string tn || tn == "")
                throw new DBException("42000", "Node name");
            var st = (name != "") ? ut.super : CTree<Domain, bool>.Empty;
            // The new Type may not yet have a Physical record, so fix that
            if (cx.parse == ExecuteStatus.Obey)
                if (HaveNodeOrEdgeType(cx) is NodeType nd)
                    ut = nd;
                else
                {
                    PNodeType? pt = null;
                    if (nt is not null)
                        st += (nt, true);
                    if (ut is EdgeType te)
                    {
                        if (!cx.role.edgeTypes.Contains(tn))
                        {
                            var pe = new PEdgeType(tn, te, st, -1L, cx.db.nextPos, cx);// backwards compatibility
                            pt = pe;
                        }
                    }
                    else if (ut is NodeType)
                        pt = new PNodeType(tn, (NodeType)ut, st, -1L, cx.db.nextPos, cx);
                    if (pt != null)
                        ut = (NodeType)(cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.INSERT_STATEMENT));
                }
            var rt = ut.rowType;
            var rs = ut.representation;
            var ui = ut?.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.TYPE);
            var uds = ut.names ?? Names.Empty;
            var sn = CTree<string,long>.Empty; // properties we are adding
            for (var b = ls.First(); b != null; b = b.Next())
            {
                var n = b.key();
                if (ut.AllCols(cx).Contains(n))
                    continue;
                var d = cx._Dom(b.value().defpos) ?? Content;
                var pc = new PColumn3(ut, n, d, d.defaultString, 
                    d.infos[d.definer]?.metadata??TMetadata.Empty, nst, cx.db.nextPos, cx);
                ut = (Table)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                rt += ((int)rt.Count, pc.ppos);
                rs += (pc.ppos, d);
                var cn = new Ident(n, pc.ppos);
                uds += (cn.ident, (cn.lp, pc.ppos));
                cx.Add(ut.name, _ap, this);
                sn += (n, pc.ppos);
            }
            if (ut is not Table et)
                return ut;
            cx.db += ut;
            for (var b = cs?.First(); b != null; b = b.Next())
                if (b.value() is TConnector tc)
                    ut = et.BuildNodeTypeConnector(cx, tc).Item1;
            cx.db += ut;
            // update defs for inherited properties
            for (var b = ut.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is TableColumn uc
                    && uc.infos[uc.definer] is ObInfo ci
                        && ci.name is string sc //&& p != ut.idCol
                        && !rs.Contains(p))
                {
                    rt += ((int)rt.Count, p);
                    rs += (p, ut.representation[p] ?? Domain.Char);
                    uds += (sc, (_ap, p));
                }
            cx.Add(tn, _ap, this);
            ut += (RowType, rt);
            ut += (Representation, rs);
            for (var b = ut.super.First(); b != null; b = b.Next())
                if (b.key().infos[cx.role.defpos] is ObInfo si)
                    uds += si.names;
            var ri = BTree<long, int?>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    ri += (p, b.key());
            for (var b = sn.First(); b != null; b = b.Next())
                if (b.value() is long qq && ri[qq] is int i)
                    uds += (b.key(), (_ap, qq));
            var oi = new ObInfo(ut.name, Grant.AllPrivileges)
                + (ObInfo._Names, uds) + (Method.TypeDef, ut);
            ut = (Table)cx.Add(ut,md).Item2;
            var ro = cx.role + (Role.DBObjects, cx.role.dbobjects + (ut.name, ut.defpos));
            if (md.Contains(Qlx.NODETYPE))
                ro += (Role.NodeTypes, ro.nodeTypes + (ut.NameFor(cx), ut.defpos));
            if (md.Contains(Qlx.EDGETYPE))
                ro += (Role.EdgeTypes, ro.edgeTypes + (ut.NameFor(cx), ut.defpos));
            if (md.Contains(Qlx.GRAPH))
                ro += (Role.Graphs, ro.graphs + (ut.NameFor(cx), ut.defpos));
            cx.db = cx.db + ut + ro;
            return ut;
        }
        internal virtual Domain? HaveNodeOrEdgeType(Context cx)
        {
            if (name != "")
                if (cx.role.nodeTypes[name] is long p && p < Transaction.Analysing)
                    return this;
            var pn = CTree<string, bool>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                    pn += (n, true);
            return (cx.role.unlabelledNodeTypesInfo[pn] is long q && q < Transaction.Analysing) ?
                this : null;
        }
        internal virtual Table Check(Context cx, GqlNode e, long ap, bool allowExtras = true)
        {
            var r = this;
            if (cx._Ob(defpos) is not Table nt || nt.infos[definer] is not ObInfo ni)
                throw new DBException("PE42133", name);
     //       for (var b = super.First(); b != null; b = b.Next())
     //           if (b.key() is Table s && cx.names[s.NameFor(cx)].Item2 > (((SqlInsert?)cx.exec)?.forNode??e.defpos))
     //               return s.Check(cx, e, ap, allowExtras);
            for (var b = e.docValue.First(); b != null; b = b.Next())
                if (!(ni.names.Contains(b.key()) || e.domain.names.Contains(b.key())) && allowExtras)
                {
                    var nc = new PColumn3(r, b.key(), b.value().domain,
                        ni.metadata.ToString(), ni.metadata, cx.db.nextStmt, cx.db.nextPos, cx, true);
                    r = (Table?)cx.Add(nc)
                        ?? throw new DBException("42105");
                    ni = r.infos[definer] ?? throw new PEException("PE03131");
                    ni += (ObInfo._Names, ni.names + (b.key(), (ap, nc.defpos)));
                    r += (Infos, r.infos + (cx.role.defpos, ni));
                    r += (ObInfo._Names, ni.names);
                    cx.Add(r);
                    cx.db += r;
                }
            return r;
        }
        /// <summary>
        /// AllowExtra is supposed to compute whether it is possible to add properties to a node or edge. 
        /// We want to prevent this if the target has an & label (when we would not know which nodetype to alter), 
        /// or if the target has committed rows.
        /// It is fine if there are no extra properties to be added.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns>The nodetype for receiving extra properties</returns>
        internal Table? AllowExtra(Context cx, BTree<long, object>? m = null)
        {
            m ??= BTree<long, object>.Empty;
            var dc = (CTree<string, QlValue>?)m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty;
            var oi = OnInsert(cx, defpos, m);
            if (oi.Count != 1)
                return null;
            var nt = cx.db.objects[oi.First()?.key()?.defpos ?? -1L] as Table;
            if (dc.Count > 0 && nt?.tableRows.First()?.key() is long p && p < Transaction.TransPos)
                return null;
            return nt;
        }
        internal TableRow? Get(Context cx, TypedValue? id)
        {
            if (id is null)
                return null;
            var px = FindPrimaryIndex(cx);
            return tableRows[px?.rows?.impl?[id]?.ToLong() ?? id?.ToLong() ?? -1L];
        }
        internal TableRow? GetS(Context cx, TypedValue? id)
        {
            if (id is null)
                return null;
            if (Get(cx, id) is TableRow rt)
                return new TableRow(rt.defpos, rt.ppos, defpos, rt.vals);
            for (var t = super.First(); t != null; t = t.Next())
                if (cx._Ob(t.key().defpos) is Table nt && nt.Get(cx, id) is TableRow tr)
                    return tr;
            return null;
        }
        internal override CTree<Domain, bool> _NodeTypes(Context cx)
        {
            return new CTree<Domain, bool>(this, true);
        }
        internal override CTree<Domain, bool> OnInsert(Context cx, long _ap, BTree<long, object>? m = null, CTree<TypedValue, bool>? cs = null)
        {
            var nt = this;
            var dn = cx.FindTable(name);
            if (name!="" && dn is null)
            {
                var pn = new PType(name, nt, cx.db.nextPos, cx);
                nt = (Table)(cx.Add(pn) ?? throw new DBException("42105"));
            }
            if (nt.defpos < 0)
                return CTree<Domain, bool>.Empty;
            var dc = (CTree<string, QlValue>?)m?[GqlNode.DocValue];
            for (var b = dc?.First(); b != null; b = b.Next())
                if (b.value() is QlValue sv && !nt.names.Contains(b.key()))
                {
                    var pc = new PColumn3(nt, b.key(), sv.domain, "", TMetadata.Empty,
                        cx.db.nextStmt, cx.db.nextPos, cx, true);
                    nt = (Table)(cx.Add(pc) ?? throw new DBException("42105"));
                }
            nt = (Table)(cx._Ob(nt.defpos) ?? nt);
            return new CTree<Domain, bool>(nt, true);
        }
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            var th = (Table)(cx.db.objects[defpos] ?? throw new PEException("PE50001"));
            ds ??= BTree<long, TableRow>.Empty;
            if (defpos < 0)
            {
                for (var b = cx.db.role.nodeTypes.First(); b != null; b = b.Next())
                    if (b.value() is long p1 && cx._Ob(p1) is Domain td)
                        if (td is Table nt1 && nt1.kind == kind)
                            ds = nt1.For(cx, ms, xn, ds);
                        else if (td.kind == Qlx.UNION)
                            for (var c = td.alts.First(); c != null; c = c.Next())
                                if (cx._Ob(c.key().defpos) is Table ef)
                                    ds = ef.For(cx, ms, xn, ds);
                for (var b = cx.db.unlabelledNodeTypes.First(); b != null; b = b.Next())
                    if (b.value() is long p2 && p2 >= 0 && cx._Ob(p2) is Table nt2)
                        ds = nt2.For(cx, ms, xn, ds);
                return ds;
            }
            if (!ms.flags.HasFlag(MatchStatement.Flags.Schema))
            {
                var cl = xn.EvalProps(cx, th);
                if (th.FindPrimaryIndex(cx) is Level3.Index px
                    && px.MakeKey(cl) is CList<TypedValue> pk)
                    return (tableRows[px.rows?.Get(pk, 0) ?? -1L] is TableRow tr0) ?
                        ds + (tr0.defpos, tr0) : ds;
                for (var c = indexes.First(); c != null; c = c.Next())
                    for (var d = c.value().First(); d != null; d = d.Next())
                        if (cx._Ob(d.key()) is Level3.Index x
                            && x.MakeKey(cl) is CList<TypedValue> xk)
                            return (th.tableRows[x.rows?.Get(xk, 0) ?? -1L] is TableRow tr) ?
                                ds + (tr.defpos, tr) : ds;
                // let DbNode check any given properties match
                var lm = ms.truncating.Contains(defpos) ? ms.truncating[defpos].Item1 : int.MaxValue;
                var la = ms.truncating.Contains(TableType.defpos) ? ms.truncating[TableType.defpos].Item1 : int.MaxValue;
                for (var b = th.tableRows.First(); b != null && lm-- > 0 && la-- > 0; b = b.Next())
                    if (b.value() is TableRow tr)
                        ds += (tr.defpos, tr);
            }
            else  // rowType flag
                ds += (defpos, th.Schema(cx));
            return ds;
        }

        /// <summary>
        /// Construct a fake TableRow for a nodetype rowType
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal TableRow Schema(Context cx)
        {
            var vals = CTree<long, TypedValue>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value()) is TableColumn tc)
                    vals += (tc.defpos, new TTypeSpec(tc.name,tc.domain));
            return new TableRow(defpos, -1L, defpos, vals);
        }

        static void AddType(ref CTree<Table, int> ts, Table t)
        {
            if (!ts.Contains(t))
                ts += (t, (int)ts.Count);
        }
        static int? Have(CTree<TNode, int> no, TableRow tr)
        {
            for (var b = no.First(); b != null; b = b.Next())
                if (tr.Equals(b.key().tableRow))
                    return b.value();
            return null;
        }
        static bool Have(CTree<TEdge, int> ed, TableRow tr)
        {
            for (var b = ed.First(); b != null; b = b.Next())
                if (tr.Equals(b.key().tableRow))
                    return true;
            return false;
        }
        /// <summary>
        /// Return coordinates for an adjacent edge icon
        /// </summary>
        /// <param name="nt"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        /// <exception cref="PEException"></exception>
        static (double, double) GetSpace(BList<NodeInfo> nt, NodeInfo n)
        {
            var np = 6;
            var df = Math.PI / 3;
            // search for a space on circle of radious size d centered at n
            for (var d = 80.0; d < 500.0; d += 40.0, np *= 2, df /= 2)
            {
                var ang = ran.Next(1, 7) * df;
                for (var j = 0; j < np; j++, ang += df)
                {
                    var (x, y) = (n.x + d * 0.75 * Math.Cos(ang), n.y + d * 0.75 * Math.Sin(ang));
                    for (var b = nt.First(); b != null; b = b.Next())
                        if (b.value() is NodeInfo ni
                            && (dist(ni.x, ni.y, x, y) < 40.0 // the edge icon
                            || dist(ni.x, ni.y, 2 * x - n.x, 2 * y - n.y) < 40.0)) // the new node icon
                            goto skip;
                    return (x, y);
                skip:;
                }
            }
            throw new PEException("PE31800");
        }
        static NodeInfo? HasSpace(BList<NodeInfo> nt, NodeInfo e, double d = 40.0, NodeInfo? f = null)
        {
            for (var b = nt.First(); b != null; b = b.Next())
                if (b.value() is NodeInfo ni && ni != f
                    && dist(ni.x, ni.y, e.x, e.y) < d)
                    return ni;
            return null;
        }
        static NodeInfo TryAdjust(BList<NodeInfo> nt, NodeInfo e, NodeInfo a, NodeInfo b)
        {
            var d = dist(a.x, a.y, b.x, b.y);
            var dx = b.x - a.x;
            var dy = b.y - a.y;
            var cx = 20 * dx / d;
            var cy = 20 * dy / d;
            var e1 = new NodeInfo(e.type, e.id, e.x + cx, e.y + cy, e.lv, e.ar, e.props);
            if (HasSpace(nt, e1, 10.0, e) == null)
                return e1;
            e1 = new NodeInfo(e.type, e.id, e.x - cx, e.y - cy, e.lv, e.ar, e.props);
            if (HasSpace(nt, e1) == null)
                return e1;
            return e;
        }
        static double dist(double x, double y, double p, double q)
        {
            return Math.Sqrt((x - p) * (x - p) + (y - q) * (y - q));
        }

        internal virtual void Update(Context cx, TableRow prev, CTree<long, TypedValue> fields)
        {  }

        internal class NodeInfo
        {
            internal readonly int type;
            internal readonly TypedValue id;
            internal float x, y;
            internal readonly long lv, ar;
            internal string[] props;
            internal NodeInfo(int t, TypedValue i, double u, double v, long l, long a, string[] ps)
            {
                type = t; id = i; x = (float)u; y = (float)v; lv = l; ar = a;
                props = ps;
            }
            public override string ToString()
            {
                var sb = new StringBuilder();
                sb.Append(type);
                sb.Append(',');
                if (id is TChar)
                { sb.Append('\''); sb.Append(id); sb.Append('\''); }
                else
                    sb.Append(id);
                sb.Append(',');
                sb.Append(x);
                sb.Append(',');
                sb.Append(y);
                sb.Append(',');
                sb.Append(lv);
                sb.Append(',');
                sb.Append(ar);
                sb.Append(",\"<p>");
                var cm = "";
                for (var i = 0; i < props.Length; i++)
                {
                    sb.Append(cm); cm = "<br/>"; sb.Append(props[i]);
                }
                sb.Append("</p>\"");
                return sb.ToString();
            }

        }

    }
} 
