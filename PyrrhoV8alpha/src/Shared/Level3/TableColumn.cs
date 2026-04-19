using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Text;

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
    /// A Database object representing a table column.
    /// FOREIGN KEY keymap constrain a reference column with its own uid, which is an anonymous connector;
    /// its domain includes a keymap with foreign key uids.
    /// Other REFERENCE keymap have their own uid in the ordinary way, and are added to the set of connectors.
    /// Table metadata keeps track of connectors.
    /// HasRefColumns computes the set of connectors.
    /// </summary>
    internal class TableColumn : DBObject
    {
        internal const long
            Checks = -268,  // CTree<long,bool> CheckFields
            ColumnDefault = -121, // long QlValue
            _Generation = -269, // GenerationRule (C)
            Hide = -272, // bool (omit from display e.g. a foreign key reference column)
            KeyMap = -70, // CTree<int,long> TableColumn (may be empty)
            RefTable = -162, // long Domain (Table or RowSet) (may be -1L)
            Seq = -344, // int column position in this table
            _Table = -270; // long
        public CTree<long, bool> checks => (CTree<long, bool>)(mem[Checks] ?? CTree<long, bool>.Empty);
        public long colDefault => (long)(mem[ColumnDefault] ?? -1L);
        public Generation generation =>
            (Generation)(mem[_Generation] ?? Generation.No);
        public int seq => (int)(mem[Seq] ?? -1);
        public long tabledefpos => (long)(mem[_Table] ?? -1L);
        public bool optional => (bool)(mem[Domain.Optional] ?? false);
        public readonly static TableColumn Doc = new (-1L,BTree<long, object>.Empty);
        public bool hide => (bool)(mem[Hide] ?? false);
        public CTree<int,long> keyMap => (CTree<int,long>)(mem[KeyMap] ?? CTree<int,long>.Empty);
        public PIndex.ConstraintType refAction => 
            mem[QuerySearch.Action] is PIndex.ConstraintType ct?ct:
            mem[QuerySearch.Action] is long q ? (PIndex.ConstraintType)(int)q :
            mem[QuerySearch.Action] is int p?(PIndex.ConstraintType)p:PIndex.ConstraintType.NoType;
        /// <summary>
        /// Constructor: a new TableColumn 
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="c">The PColumn def</param>
        /// <param name="dt">the object type</param>
        public TableColumn(Table tb, PColumn c, Domain dt,Context cx)
            : base(c.defpos, _TableColumn(c,dt,cx)+(_Table, tb.defpos) + (LastChange, c.ppos)
                     + (Owner, cx.user?.defpos ?? -501L)  
                     + (KeyMap,(tb.mem[c.refindex] as CTree<int,long>)??c.keymap))
        {
            var lp = cx.names[c.name].Item1;
            cx.names += (c.name, (lp, c.defpos));
        }
        internal TableColumn(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static TableColumn operator +(TableColumn et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (TableColumn)et.New(m + x);
        }
        public static TableColumn operator-(TableColumn c,long k)
        {
            return (TableColumn)c.New(c.mem - k);
        }
        public static TableColumn operator +(TableColumn e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (TableColumn)e.New(m + (p, o));
        }
        static BTree<long,object> _TableColumn(PColumn c,Domain dt,Context cx)
        {
            var r = BTree<long, object>.Empty + (Definer, c.definer)
                + (Owner, c.owner) + (Infos, c.infos) + (Seq, c.seq)
                + (_Domain, dt) + (LastChange, c.ppos) + (Domain.Optional, c.optional);
            r = r + (ObInfo._Names, dt.names) + (ObInfo.Defs, dt.defs);
            if (dt is not UDType ut && !c.optional)
                r += (Domain.Optional, false);
            if (c.generation != Generation.No)
                r += (_Generation, c.generation);
            if (dt.sensitive)
                r += (Sensitive, true);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableColumn(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new TableColumn(dp, m);
        }
        internal override DBObject Instance(long lp,Context cx, RowSet? ur=null)
        {
            var r = base.Instance(lp, cx);
            for (var b = checks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Instance(lp, cx);
            return r;
        }
        internal override TypedValue _Default()
        {
            return domain.defaultValue;
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, Ident ic, Ident? n, DBObject? p)
        {
            if (cx._Ob(defpos) is not DBObject ob)
                throw new DBException("42105").Add(Qlx.COLUMN);
            QlValue r = new QlInstance(new Ident(ic.ident,lp), cx, p?.defpos??-1L, ob) + (_Domain, domain);
            cx.Add(r);
            return (r, n);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var nt = cx.Fix(tabledefpos);
            if (nt != tabledefpos)
                r = cx.Add(r, _Table, nt);
            var nc = cx.FixTlb(checks);
            if (nc != checks)
                r += (Checks, nc);
            return r;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(Checks,checks+(ck.defpos,true)));
        }
        internal override (Context,DBObject) Add(Context cx, TMetadata md)
        {
            var ckp = 0L;
            (cx,var tc) = base.Add(cx,md);
            var table = cx._Ob(tabledefpos) as Table ?? throw new PEException("PE30331");
            var tm = TMetadata.Empty; // this will become the new bits of metadata to add to the TableColumn 
            var oi = tc.infos[cx.role.defpos] ?? new ObInfo(name);
            var om = oi.metadata??TMetadata.Empty;
            for (var b = md.First(); b != null; b = b.Next())
                switch (b.key())
                {
                    case Qlx.ACTION:
                        {
                            if (domain.kind != Qlx.REF)
                                continue; 
                            om += (b.key(), b.value());
                            var a = (PIndex.ConstraintType)(b.value().ToInt() ?? 0);
                            tc += (QuerySearch.Action, PIndex.ReferentialAction(refAction, a));
                            break;
                        }
                    case Qlx.REFERENCES: throw new PEException("PE61712");
                    case Qlx.DEFAULT:
                        {
                            if (md[Qlx.GENERATED] is TInt it && it.ToInt()==(int)Generation.Expression)
                                break;
                            if (md[Qlx.VALUE] is TInt td && td.ToLong() is long dp)
                                tc = tc + (ColumnDefault, dp);
                            om += (b.key(), b.value());
                            break;
                        }
                    case Qlx.GENERATED:
                        {
                            var g = (Generation)(b.value()?.ToInt() ?? 0L);
                            tc = tc + (_Generation, g);
                            break;
                        }
                    case Qlx.SECURITY:
                        table = (Table)(cx.Add(new Classify(tc.defpos, 
                            ((TLevel)b.value()).val, cx.db.nextPos,cx.db))
                            ?? throw new DBException("42105"));
                        break;
                    case Qlx.CHECK:
                        {
                            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new PEException("PE60631"));
                            var nst = md[Qlx.SCOPE]?.ToLong() ?? -1L;
                            var ckn = md[Qlx.CONSTRAINT]?.ToString() ?? "";
                            var sce = md[Qlx.SOURCE]?.ToString() ?? "";
                            if (cx.obs[md[Qlx.CHECK]?.ToLong() ?? -1L] is QlValue se)
                            {
                                var pc = new PCheck2(table, tc, ckn, se, sce, nst, cx.db.nextPos, cx);
                                cx.Add(pc);
                                if (ckn != "")
                                    table += (ObInfo.ConstraintNames, table.constraintNames + (ckn, ckp));
                                tc += (Checks, checks + (pc.ppos, true));
                            }
                            break;
                        }
                    case Qlx.OPTIONAL:
                        {
                            tc += (Domain.Optional, b.value().ToBool() ?? false);
                            break;
                        }
 /*                   case Qlx.CONNECTING:
                        {
                            var cc = b.value() as TConnector ?? throw new PEException("PE60641");
                            tc += (Connectors, new CTree<Domain,TConnector>(cc.rd,cc));
                            RefMd(cx, tc, b);
                            break;
                        } */
                    case Qlx.MINVALUE:
                    case Qlx.MAXVALUE:
                        {
                            tc = (TableColumn)tc.Add(cx,TMetadata.Empty + (b.key(), b.value())).Item2;
                            om += (Qlx.MULTIPLICITY, new TSet(Domain.Ref) 
                                + new TRef(defpos, tc.domain.super.First()?.key()??Domain.Null));
                            break;
                        }
                }
            if (om != oi.metadata)
            {
                oi += (ObInfo._Metadata, om);
                tc += (ObInfo._Metadata, om);
            }
            if (oi != tc.infos[cx.role.defpos])
                tc += (Infos, tc.infos + (cx.role.defpos, oi));
            cx.Add(tc);
            cx.db += tc;
            //      cx.Install(table.Add(cx, s, tm) + (Infos, table.infos + (cx.role.defpos, ti))); 
            if (tm!=TMetadata.Empty)
                cx.MetaPend(table.defpos, defpos, tm.ToString(), tm);
            cx.Install(table);
            cx.db += table;
            return (cx,tc);
        }

        /// <summary>
        /// Accessor: CheckFields a new column notnull condition
        /// Normally fail if null values found
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="reverse">If true fail if non-null values found</param>
        internal void ColumnCheck(Transaction tr, bool reverse)
        {
            var cx = new Context(tr);
            if (tr.objects[tabledefpos] is not Table tb || cx.role == null)
                return;
            var fm = tb.RowSets(new Ident("", tr.uid),new Context(tr),tb,tr.uid,0L);
            for (var rb = fm.First(cx);
                rb != null; rb = rb.Next(cx))
            {
                var v = rb[defpos];
                var nullfound = v == null;
                if (nullfound ^ reverse)
                    throw new DBException(reverse ? "44005" : "44004", tb.name??"?", name??"?").ISO()
                        .Add(Qlx.TABLE_NAME, new TChar(tb.name ?? "?"))
                        .Add(Qlx.COLUMN_NAME, new TChar(name ?? "?"));
            }
        }
        internal override DBObject Apply(Context cx, Domain tb)
        {
            cx.Add(framing);
            var f = framing.obs;
            if (cx.obs[colDefault] is QlValue ex)
                f = ex._Apply(cx, tb, f);
            for (var b = checks.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue se)
                    f = se._Apply(cx, tb, f);
            return cx.Add(this + (_Framing, framing+(Framing.Obs,f)));
        }
        /// <summary>
        /// Accessor: CheckFields a new column check constraint
        /// </summary>
        /// <param name="c">The new CheckFields constraint</param>
        /// <param name="signal">signal is 44003 for column check, 44001 for domain check</param>
        internal void ColumnCheck(Transaction tr, Check c, string signal)
        {
            var cx = new Context(tr);
            if (tr.objects[tabledefpos] is Table tb && cx.obs[c.search] is QlValue sch &&
                tb.RowSets(new Ident("", tr.uid), cx, tb, tr.uid,0L)
                .Apply(cx, BTree<long, object>.Empty + (RowSet._Where, sch.Disjoin(cx))) is RowSet nf &&
                nf.First(cx) != null && cx.role != null &&
                cx._Ob(tabledefpos) is DBObject t && t.infos[cx.role.defpos] is ObInfo ti && 
                infos[cx.role.defpos] is ObInfo ci)
                throw new DBException(signal, c.name ?? "", this, tb).ISO()
                    .Add(Qlx.CONSTRAINT_NAME, new TChar(c.name ?? ""))
                    .Add(Qlx.COLUMN_NAME, new TChar(ci.name ?? "?"))
                    .Add(Qlx.TABLE_NAME, new TChar(ti.name ?? "?"));
        }
        protected override void _Cascade(Context cx,Drop.DropAction a, BTree<long, TypedValue>u)
        {
            base._Cascade(cx, a, u);
            for (var b = checks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Cascade(cx, a, u);
        }
        internal override Database Drop(Database d, Database nd)
        {
            if (nd.objects[tabledefpos] is Table tb)
            {
                tb += (Dependents, tb.dependents - defpos);
                var rws = tb.tableRows;
                for (var b = rws.First(); b != null; b = b.Next())
                {
                    var rw = b.value();
                    rws += (b.key(), rw - defpos);
                }
                tb += (Table.TableRows, rws);
                var r = CTree<int,long>.Empty;
                for (var b = tb.rowType.First(); b != null; b = b.Next())
                    if (b.value() != defpos)
                        r += ((int)r.Count, b.value());
                tb += (Domain.RowType, r);
                tb += (Domain.Representation, tb.representation - defpos);
                if (tb.colRefs.Count>0L && tb.HasRefCols(nd).Count==0)
                    nd += (tb - Table.ColRefs);
                nd += tb;
            }
            return base.Drop(d, nd);
        }
        internal override Database DropCheck(long ck, Database nd)
        {
            return nd + (this + (Checks, checks - ck));
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.values += (defpos, v);
        }
 /*       internal override void Note(Context cx, StringBuilder sb, string pre = "  ")
        {
            sb.Append(pre);
            for (var b=cs.First();b!=null;b=b.Next())
                if (b.value() is TConnector cc)
                    switch (cc.q)
                    {
                        case Qlx.ID:
                        case Qlx.FROM:
                        case Qlx.WITH:
                        case Qlx.TO:
                            sb.Append(' '); sb.Append(cc.q); break;
                    }
            if (pre == "  ")
            {
                domain.FieldType(cx, sb);
                for (var c = checks.First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Check ck)
                        ck.Note(cx, sb, pre);
            }
            else
                sb.Append("\r\n");
        }*/
        /// <summary>
        /// a readable version of the table column
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(domain);
            if (optional) sb.Append(" optional");
            sb.Append(" Table="); sb.Append(Uid(tabledefpos)); 
            if (checks.Count>0)
            { sb.Append(" Checks:"); sb.Append(checks); }
            if (generation != Generation.No)
            { sb.Append(" Generated="); sb.Append(generation); }
            if (mem.Contains(ColumnDefault) && colDefault>0)
            { sb.Append(" default "); sb.Append(Uid(colDefault)); }
            if (keyMap != CTree<int,long>.Empty)
            { sb.Append(" keyMap ");
                var cm = '(';
                for (var b=keyMap.First();b!=null;b=b.Next())
                {
                    sb.Append(cm);cm = ',';
                    sb.Append(b.value());
                }
                sb.Append(')'); }
            if (refAction!=PIndex.ConstraintType.NoType)
            {
                sb.Append(' ');sb.Append(refAction);
            }
            return sb.ToString();
        }

        internal void JsonSchema(Context cx, StringBuilder sb)
        {
            sb.Append(NameFor(cx)); sb.Append(':');// sb.Append("{Name:'"); sb.Append(NameFor(cx)); sb.Append('\'');
            domain.FieldJson(cx, sb);
            sb.Append('}');
        }
    }
    /// <summary>
    /// Generation.Default supports use of general Value expressions in TableColumn default metadata
    /// </summary>
    internal enum Generation { No, Expression, Default, RowStart, RowEnd };
     /// <summary>
    /// This is a type of Selector that corresponds to subColumn that is specified in a constraint
    /// and so must be realised in the physical infrastructure. 
    /// 
    /// </summary>
    internal class ColumnPath : TableColumn
    {
        internal const long
            Prev = -321; // TableColumn
        /// <summary>
        /// The prefix Selector
        /// </summary>
        public QlValue? prev => (QlValue?)mem[Prev];
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="pp">the level 2 column path information</param>
        public ColumnPath(long dp, string n, TableColumn pr, Database db)
            : base(dp, new BTree<long, object>(Prev, pr)
                  + (Infos, new BTree<long, ObInfo>(db.role.defpos, new ObInfo(n))))
        { }
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
        /// <param name="ss">The tree of path components</param>
        /// <param name="i">An index into this path</param>
        /// <param name="v">the new value</param>
        /// <returns>the updated Document</returns>
        TDocument Set(TDocument d, string[] ss, int i, TypedValue v)
        {
            var s = ss[i];
            if (i < ss.Length - 1 && d[s] is TDocument tv)
                v = Set(tv, ss, i + 1, v);
            return new TDocument(d, (s, v));
        }
    }

    /// <summary>
    /// This class (new in v7) computes the current state of the TableRow and stores it in the
    /// Table. 
    /// It is Role-independent, so it doesn't follow the representation of any domain 
    /// and therefore can't subclass TRow.
    /// 
    /// </summary>
    internal class TableRow : IEquatable<TableRow>
    {
        internal readonly long defpos;
        internal readonly long time;
        internal readonly long tabledefpos; //Domain: Table or JoinedType 
        internal readonly long user;
        internal readonly long ppos;
        internal readonly long prev;
        internal readonly long subType;
        internal readonly Level classification;
        internal readonly CTree<long, TypedValue> vals;
        internal static TableRow Any = new (-1L); // creates a dummy TableRow for TPath
        public TableRow(Record rc, Context cx)
        {
            rc.CheckFields(cx);
            defpos = rc.defpos;
            time = rc.time;
            user = (cx.user ?? User.None).defpos;
            tabledefpos = rc.tabledefpos;
            subType = rc.subType;
            classification = rc.classification ?? Level.D;
            ppos = rc.ppos;
            prev = rc.ppos;
            vals = rc.fields;
        }
        public TableRow(Update up, Context cx, TableRow? old, Level? lv=null)
        {
            up.CheckFields(cx);
            defpos = up.defpos;
            time = up.time;
            user = (cx.user ?? User.None).defpos;
            tabledefpos = up.tabledefpos;
            classification = lv ?? old?.classification ?? Level.D;
            subType = up.subType;
            ppos = up.ppos;
            prev = up.prev;
            var v = old?.vals??CTree<long,TypedValue>.Empty;
            for (var b = up.fields.First(); b != null; b = b.Next())
                if (b.value() == TNull.Value)
                    v -= b.key();
                else
                    v += (b.key(), b.value());
            vals = v;
        }
        protected TableRow(TableRow r,CTree<long,TypedValue> vs)
        {
            defpos = r.defpos;
            time = r.time; user = r.user; 
            tabledefpos = r.tabledefpos;
            classification = r.classification;
            ppos = r.ppos;
            subType = r.subType;
            prev = r.prev;
            vals = vs;
        }
        internal TableRow(TableRow r, long tp, CTree<long, TypedValue> vs)
        {
            defpos = r.defpos;
            time = r.time; user = r.user;
            tabledefpos = tp;
            classification = r.classification;
            ppos = r.ppos;
            subType = r.subType;
            prev = r.prev;
            vals = vs;
        }
        internal TableRow(long dp,Table tb,Cursor c)
        {
            defpos = dp;
            prev = dp;
            subType = c.dataType.defpos;
            classification = tb.classification;
            tabledefpos = tb.defpos;
            vals = c.values;
        }
        internal TableRow(long vp,CTree<long,TypedValue>? vs=null)
        {
            vs ??= CTree<long, TypedValue>.Empty;
            var dp = (vs[DBObject.Defpos] is TRef v)?v.ToLong()??-1L:-1L;
            var pp = vs[Table.LastData]?.ToLong() ?? 01L;
            defpos = dp;
            time = DateTime.Now.Ticks;
            user = -1L;
            tabledefpos = vp;
            subType = -1L;
            classification = Level.D;
            ppos = dp;
            prev = pp;
            vals = vs;
        }
        internal TableRow(long dp,long pp,long tp,CTree<long,TypedValue> vs)
        {
            defpos = dp;
            time = DateTime.Now.Ticks;
            user = -1L;
            tabledefpos = tp;
            subType = -1L;
            classification = Level.D;
            ppos = dp;
            prev = pp;
            vals = vs;
        }
        internal TableRow(TableRow x,long un,long was,long now)
        {
            defpos = x.defpos;
            time = x.time;
            tabledefpos = x.tabledefpos;
            user = x.user;
            ppos = x.ppos;
            prev = x.prev;
            subType = un;
            classification = x.classification;
            vals = x.vals;
            if (x.vals[was] is TypedValue tv)
                vals = vals - was + (now, tv);
        }
        public static TableRow operator+(TableRow r,(long,TypedValue)x)
        {
            return new TableRow(r, r.vals + x);
        }
        public static TableRow operator-(TableRow r,long p)
        {
            return new TableRow(r, r.vals -p);
        }
        internal TableRow Check(Domain dm,Context cx)
        {
            var vs = vals;
            for (var b = dm.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var dv = dm.representation[p] ?? Domain.Ref;
                if (vs[p] is not TypedValue v)
                    throw new DBException("22G0Y", cx.NameFor(p)??"");
                if (v != TNull.Value && !v.dataType.EqualOrStrongSubtypeOf(dv))
                    throw new PEException("PE10704");
            }
            return this;
        }
        internal Context Cascade(Context _cx, TableActivation cx, CTree<long,TypedValue>?u = null)
        {
            var db = cx.db;
            if (db != null && cx.obs[tabledefpos] is Table tb && cx._Ob(cx.refcol) is TableColumn tc)
                for (var b = tb.sindexes[defpos]?[cx.refcol]?.First(); b != null; b = b.Next())
                {
                    var q = b.key();
                    var ku = BTree<long, UpdateAssignment>.Empty;
                    if (cx._tty==PTrigger.TrigType.Update && tc.keyMap!=CTree<int,long>.Empty
                        && tc.domain.kind==Qlx.REF && tc.domain.elType is Domain rd
                        && cx._Ob(rd.defpos) is Table rt && rt.FindPrimaryIndex(cx) is Index px)
                    {
                        var pc = px.keys.First();
                        for (var c = tc.keyMap.First(); c != null && pc!=null; c = c.Next(), pc=pc.Next())
                        {
                            TypedValue v = u?[pc.value()]??TNull.Value;
                            switch (cx.flags & PIndex.Updates)
                            {
                                case PIndex.ConstraintType.CascadeUpdate:
                                    v = u?[c.value()] ?? TNull.Value; break;
                                case PIndex.ConstraintType.SetDefaultUpdate:
                                    v = cx._Dom(pc.value())?.defaultValue ?? TNull.Value; break;
                            }
                            ku += (c.value(), new UpdateAssignment(c.value(), v));
                        }
                    }
                    cx.updates += ku;
                    var restrict = (cx._tty == PTrigger.TrigType.Delete && cx.flags.HasFlag(PIndex.ConstraintType.RestrictDelete))
                        || (cx._tty == PTrigger.TrigType.Update && cx.flags.HasFlag(PIndex.ConstraintType.RestrictUpdate)
                        && (cx._cx as TableActivation)?._tty != PTrigger.TrigType.Delete);
                    if (cx.next != null)
                    {
                        if (restrict)
                            throw new DBException("23000", "RESTRICT - reference in use", q);
                        cx.next.cursors += cx.cursors;
                        if (cx._trs.At(cx, q, u) is Cursor cu)
                        {
                            cx.next.cursors += (cx._trs.data, cu);
                            _cx = cx.EachRow(_cx, cu._pos);
                            cx.SlideDown(); // get next to accept our changes
                            _cx.db = (cx.next??cx).db; // get _cx to accept our changes
                        }
                    }
                }
            return _cx;
        }
        public CList<TypedValue> MakeKey(Index x)
        {
            var r = CList<TypedValue>.Empty;
            for (var b=x.keys.First();b is not null;b=b.Next())
                if (b.value() is long p)
                    r += vals[p] ?? TNull.Value;
            return r;
        }
        public CList<TypedValue> MakeKey(Index x, Context cx)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = x.keys.First(); b is not null; b = b.Next())
                if (b.value() is long p)
                {
                    p = (cx.uids[p] is long q && q!=0L)?q: p;
                    r += vals[p] ?? TNull.Value;
                }
            return r;
        }
        public CList<TypedValue> MakeKey(CTree<int,long> cols)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = cols.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    r += vals[p]??TNull.Value;
            return r;
        }
        internal TableRow ShallowReplace(Context cx,long was, long now)
        {
            var vs = cx.ShallowReplace(vals, was, now);
            return (vs != vals) ? new TableRow(this,vs) : this; // CheckFields is done by caller: can't be done just now
        }
        internal TypedValue SpecificType(Context cx)
        {
            var sb = new StringBuilder();
            if (subType >= 0 && cx.NameFor(subType) is string ns)
                sb.Append(ns);//.Trim(':'));
            else if (cx._Ob(tabledefpos) is Table tb)
            {
                for (var b = tb.subtypes.First(); b != null; b = b.Next())
                {
                    if (cx._Ob(b.key()) is Table s)
                    {
                        for (var c = s.constraints.First(); c != null; c = c.Next())
                        {
                            if (cx._Ob(c.key()) is SqlValueExpr se)
                            {
                                var ac = new Context(cx);
                                ac.values += vals;
                                if (se.Eval(ac) != TBool.True)
                                    goto nextb;
                            }
                            if (cx._Ob(c.key()) is MatchStatement ms)
                            {
                                var ac = new Context(cx);
                                ac.result = (Domain)ac.Add(new TrivialRowSet(0L, ac.GetUid(), ac, new TRow(s, vals)));
                                if (ms._Obey(ac).result is not RowSet rs || rs.rows.Count == 0)
                                    goto nextb;
                            }
                        }
                        return new TChar(s.NameFor(cx));
                    }
                nextb:;
                }
                return new TChar(tb.NameFor(cx));
            }
            return TNull.Value;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table=");
            sb.Append(DBObject.Uid(tabledefpos));
            sb.Append(" Prev=");sb.Append(DBObject.Uid(prev));
            sb.Append(" Time=");sb.Append(new DateTime(time));
            return sb.ToString();
        }
        public bool Equals(TableRow? other)
        {
            if (other==null) return false;
            if (tabledefpos != other.tabledefpos) return false;
            if (defpos > 0 && defpos != other.defpos) return false;
            for (var b = vals.First(); b != null; b = b.Next())
                if (other.vals[b.key()]?.CompareTo(b.value()) != 0) return false;
            return true;
        }
        internal TableRow Fix(Context cx)
        {
            return this;
        }
    }
    
    internal class RemoteTableRow(long dp, CTree<long, TypedValue> v, string u, RestRowSet r) 
        : TableRow(r.target,v) //? we used to add a TRef field here with key DBObject.Defpos
    {
        internal readonly string url = u;
        internal readonly RestRowSet rrs = r;
    }
    internal class PeriodDef : TableColumn
    {
        internal const long
            StartCol = -387, // long TableColumn
            EndCol = -388; // long TableColumn
        internal long startCol => (long)(mem[StartCol]??-1L);
        internal long endCol => (long)(mem[EndCol]??-1L);
        public PeriodDef(long lp, long tb, long sc, long ec, Database db)
            : base(lp, BTree<long, object>.Empty + (_Table, tb) + (StartCol, sc)
                  + (EndCol, ec)
                  + (_Domain, ((TableColumn)(db.objects[sc] ?? throw new DBException("4200"))).domain))

        { }
        protected PeriodDef(long dp, BTree<long, object> m)
            : base(dp, m) { }
        public static PeriodDef operator +(PeriodDef p, (long, object) x)
        {
            var (dp, ob) = x;
            if (p.mem[dp] == ob)
                return p;
            return new PeriodDef(p.defpos, p.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodDef(defpos, m); ;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new PeriodDef(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            r += (StartCol, cx.Fix(startCol));
            r += (EndCol, cx.Fix(endCol));
            return r;
        }
    }
}
