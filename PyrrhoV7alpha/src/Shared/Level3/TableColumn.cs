using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level5;
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
    /// A Database object representing a table column
    /// 
    /// </summary>
    internal class TableColumn : DBObject
    {
        internal const long
            Checks = -268,  // CTree<long,bool> Check
            Generated = -269, // GenerationRule (C)
            Connector = -391, // TConnector
            Seq = -344, // int column position in this table
            _Table = -270, // long
            UpdateAssignments = -271, // CTree<UpdateAssignment,bool>
            UpdateString = -272; // string
        public CTree<long, bool> checks => (CTree<long, bool>)(mem[Checks] ?? CTree<long, bool>.Empty);
        public GenerationRule generated =>
            (GenerationRule)(mem[Generated] ?? GenerationRule.None);
        public int seq => (int)(mem[Seq] ?? -1);
        public long tabledefpos => (long)(mem[_Table] ?? -1L);
        public bool notNull => (bool)(mem[Domain.NotNull] ?? false);
         public CTree<UpdateAssignment,bool> update =>
            (CTree<UpdateAssignment,bool>?)mem[UpdateAssignments] 
            ?? CTree<UpdateAssignment,bool>.Empty;
        public string? updateString => (string?)mem[UpdateString];
        public string? defaultString => (string?)mem[Domain.DefaultString];
        public readonly static TableColumn Doc = new (-1L,BTree<long, object>.Empty);
        /// <summary>
        /// These properties are used for special columns in the Typed Graph Model
        /// </summary>
        public TypedValue tc => (TypedValue)(mem[Connector] ?? TNull.Value);
        public long toType => (long)(mem[Index.RefTable] ?? -1L);
        public long index => (long)(mem[Index.RefIndex] ?? -1L);
        /// <summary>
        /// Constructor: a new TableColumn 
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="c">The PColumn def</param>
        /// <param name="dt">the obs type</param>
        public TableColumn(Table tb, PColumn c, Domain dt,Context cx)
            : base(c.defpos, _TableColumn(c,dt,cx)+(_Table, tb.defpos) + (LastChange, c.ppos)
                     + (Owner, cx.user?.defpos ?? -501L)) 
        {  }
        protected TableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
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
                + (_Domain, dt) + (LastChange, c.ppos);
            if (dt.infos[cx.role.defpos] is ObInfo oi)
                r = r + (ObInfo._Names, oi.names) + (ObInfo.Defs, oi.defs);
            if (c.notNull || dt.notNull)
                r += (Domain.NotNull, true);
            if (c.generated != GenerationRule.None)
                r += (Generated, c.generated);
            if (c.dfs != "")
                r += (Domain.DefaultString, c.dfs);
            if (dt.sensitive)
                r += (Sensitive, true);
            if (c.dv != TNull.Value)
                r += (Domain.Default, c.dv);
            if (c.ups!="")
                r = r + (UpdateString, c.ups) + (UpdateAssignments, c.upd);
            if (c.connector is TConnector cc && cc.q!=Qlx.Null)
                r = r + (Connector,cc);
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
                if (cx.db.objects[b.key()] is Check ck)
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
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nt = cx.Fix(tabledefpos);
            if (nt != tabledefpos)
                r = cx.Add(r, _Table, nt);
            var ng = generated.Fix(cx);
            if (ng != generated)
                r = cx.Add(r, Generated, ng);
            var nc = cx.FixTlb(checks);
            if (nc != checks)
                r += (Checks, nc);
            var nu = cx.FixTub(update);
            if (nu != update)
                r += (UpdateAssignments, nu);
            var tt = tc.Fix(cx);
            if (tt != tc)
                r += (Connector, tt);
            return r;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(Checks,checks+(ck.defpos,true)));
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableColumn)base._Replace(cx, so, sv);
            if (generated.exp != -1L)
            {
                var go = generated.exp;
                var ge = (QlValue)cx._Replace(go, so, sv);
                if (ge != cx._Ob(go))
                    r += (Generated, new GenerationRule(generated.gen,
                        generated.gfs, ge, defpos, cx.db.nextStmt));
            }
            var ua = CTree<UpdateAssignment,bool>.Empty;
            for (var b = update.First(); b != null; b = b.Next())
                ua += (b.key().Replace(cx, so, sv),true);
            if (ua != update)
                r +=(cx, UpdateAssignments, ua);
            return r;
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
            if (tr.objects[tabledefpos] is not Table tb || cx.role == null)
                return;
            var fm = tb.RowSets(new Ident("", tr.uid),new Context(tr),tb,tr.uid,0L);
            for (var rb = fm.First(cx);
                rb != null; rb = rb.Next(cx))
            {
                var v = rb[defpos];
                var nullfound = v == null;
                if (nullfound ^ reverse && tb.infos[cx.role.defpos] is ObInfo ti &&
                    infos[cx.role.defpos] is ObInfo ci)
                    throw new DBException(reverse ? "44005" : "44004", ti.name??"?", ci.name??"?").ISO()
                        .Add(Qlx.TABLE_NAME, new TChar(ci.name ?? "?"))
                        .Add(Qlx.COLUMN_NAME, new TChar(ti.name ?? "?"));
            }
        }
        internal override DBObject Apply(Context cx, Domain tb)
        {
            cx.Add(framing);
            var f = ObTree.Empty;
            if (cx.obs[generated.exp] is QlValue ex)
                f = ex._Apply(cx, tb, f);
            for (var b = checks.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue se)
                    f = se._Apply(cx, tb, f);
            return cx.Add(this + (_Framing, framing+(Framing.Obs,f)));
        }
        /// <summary>
        /// Accessor: Check a new column check constraint
        /// </summary>
        /// <param name="c">The new Check constraint</param>
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
                if (cx.db.objects[b.key()] is Check ck)
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
                var r = CList<long>.Empty;
                for (var b = tb.rowType.First(); b != null; b = b.Next())
                    if (b.value() != defpos)
                        r += b.value();
                tb += (Domain.RowType, r);
                tb += (Domain.Representation, tb.representation - defpos);
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
        internal override void Note(Context cx, StringBuilder sb, string pre = "  ")
        {
            sb.Append(pre);
            if (tc is TConnector cc)
            switch (cc.q)
            {
                case Qlx.ID:
                case Qlx.FROM:
                case Qlx.WITH:
                case Qlx.TO:
                    sb.Append(cc.q); break;
            }
            if (pre == "  ")
            {
                domain.FieldType(cx, sb);
                for (var c = checks.First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Check ck)
                        ck.Note(cx, sb, pre);
                if (generated is GenerationRule gr)
                    gr.Note(sb);
            }
            else
                sb.Append("\r\n");
        }
        /// <summary>
        /// a readable version of the table column
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(domain);
            if (mem.Contains(_Table)) { sb.Append(" Table="); sb.Append(Uid(tabledefpos)); }
            if (mem.Contains(Checks) && checks.Count>0)
            { sb.Append(" Checks:"); sb.Append(checks); }
            if (mem.Contains(Generated) && generated.gen != Generation.No)
            { sb.Append(" Generated="); sb.Append(generated); }
            if (mem.Contains(Domain.NotNull) && domain.notNull) sb.Append(" Not Null");
            if (domain.defaultValue is not null && 
              ((domain.defaultValue != TNull.Value) || PyrrhoStart.VerboseMode))
            { sb.Append(" colDefault "); sb.Append(domain.defaultValue); }
            if (mem.Contains(UpdateString))
            {
                sb.Append(" UpdateString="); sb.Append(updateString);
                sb.Append(" Update:"); sb.Append(update);
            }
            if (tc is TConnector cc && cc.q!=Qlx.Null)
                sb.Append(" "+cc.q);
            return sb.ToString();
        }
    }
    internal enum Generation { No, Expression, RowStart, RowEnd, Position };
    /// <summary>
    /// Helper for GenerationRule
    /// 
    /// </summary>
    internal class GenerationRule : Basis
    {
        internal const long
            _Generation = -273, // Generation
            GenExp = -274, // long QlValue
            GenString = -275; // string
        internal readonly static GenerationRule None = new (Generation.No);
        public Generation gen => (Generation)(mem[_Generation] ?? Generation.No); // or START or END for ROW START|END
        public long exp => (long)(mem[GenExp]??-1L);
        public string gfs => (string)(mem[GenString]??"");
        public long target => (long)(mem[RowSet.Target] ?? -1L);
        public long nextStmt => (long)(mem[Database.NextStmt] ?? -1L);  
        public GenerationRule(Generation g) : base(new BTree<long, object>(_Generation, g)) { }
        public GenerationRule(Generation g, string s, QlValue e, long t, long nst)
            : base(BTree<long, object>.Empty + (_Generation, g) + (GenExp, e.defpos) + (GenString, s)
                  +(RowSet.Target,t) + (Database.NextStmt,nst)) { }
        protected GenerationRule(BTree<long, object> m) : base(m) { }
        public static GenerationRule operator +(GenerationRule gr, (long, object) x)
        {
            var (dp, ob) = x;
            if (gr.mem[dp] == ob)
                return gr;
            return (GenerationRule)gr.New(gr.mem + x)??None;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GenerationRule(m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx, m);
            var ne = cx.Fix(exp);
            if (exp !=ne)
                r += (GenExp, ne);
            var tg = cx.Fix(target);
            if (tg != target)
                r += (RowSet.Target, tg);
            return r;
        }
        internal void Note(StringBuilder sb)
        {
            if (gen == Generation.No)
                return;
            sb.Append("// GenerationRule "); sb.Append(gen); 
            sb.Append(' ');  sb.Append(gfs); sb.Append("\r\n");
        }
        public override string ToString()
        {
            return (gen == Generation.Expression) ? gfs : gen.ToString();
        }
    }
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
        internal readonly long tabledefpos; // may be a label set type
        internal readonly long user;
        internal readonly long ppos;
        internal readonly long prev;
        internal readonly long subType;
        internal readonly Level classification;
        internal readonly CTree<long, TypedValue> vals;
        internal static TableRow Any = new (-1L); // creates a dummy TableRow for TPath
        public TableRow(Record rc, Context cx)
        {
            rc.Check(cx);
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
            up.Check(cx);
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
            var dp = (vs[DBObject.Defpos] is TInt v)?v.ToLong()??-1L:-1L;
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
                var dv = dm.representation[p] ?? Domain.Position;
                if (vs[p] is not TypedValue v)
                    throw new DBException("22G0Y", cx.NameFor(p)??"");
                if (v != TNull.Value && !v.dataType.EqualOrStrongSubtypeOf(dv))
                    throw new PEException("PE10704");
            }
            return this;
        }
        internal Context Cascade(Context _cx, TableActivation cx, CTree<long, TypedValue>? u = null)
        {
            var db = cx.db;
            //       var fr = (TableRowSet)cx.next.obs[cx._fm.defpos];
            if (db != null && cx.obs[cx._fm.target] is Table tb)
                for (var ib = tb.indexes.First(); ib != null; ib = ib.Next())
                    for (var c = ib.value().First(); c != null; c = c.Next())
                        if (db.objects[c.key()] is Index rx && db.objects[rx.refindexdefpos] is Index _rx
                            && _rx.MakeKey(vals) is CList<TypedValue> pk)
                        {
                            var ku = BTree<long, UpdateAssignment>.Empty;
                            if (u != null)
                            {
                                for (var xb = _rx.keys.First(); xb != null; xb = xb.Next())
                                    if (xb.value() is long p && rx.keys[xb.key()] is long q)
                                    {
                                        TypedValue v;
                                        switch (rx.flags & PIndex.Updates)
                                        {
                                            case PIndex.ConstraintType.CascadeUpdate:
                                                v = u[p] ?? TNull.Value; break;
                                            case PIndex.ConstraintType.SetDefaultUpdate:
                                                v = cx._Dom(p)?.defaultValue ?? TNull.Value; break;
                                            default:
                                                continue;
                                        }
                                        ku += (q, new UpdateAssignment(q, v));
                                    }
                                if (ku == BTree<long, UpdateAssignment>.Empty) // not updating a key
                                    return cx;
                                cx.updates += ku;
                            }
                            var restrict = (cx._tty == PTrigger.TrigType.Delete && rx.flags.HasFlag(PIndex.ConstraintType.RestrictDelete))
                                || (cx._tty == PTrigger.TrigType.Update && rx.flags.HasFlag(PIndex.ConstraintType.RestrictUpdate)
                                && (cx._cx as TableActivation)?._tty != PTrigger.TrigType.Delete);
                            if (cx.next != null)
                                for (var d = rx.rows?.PositionAt(pk,0); 
                                    d != null && d.key() is CList<TypedValue> k && k.CompareTo(pk) == 0;
                                    d = d.Next())
                                {
                                    if (restrict && pk is not null)
                                        throw new DBException("23000", "RESTRICT - foreign key in use", pk);
                                    cx.next.cursors += cx.cursors;
                                    if (d.Value() is long vv && cx._trs.At(cx, vv, u) is Cursor cu)
                                    {
                                        cx.next.cursors += (cx._trs.data, cu);
                                        _cx = cx.EachRow(_cx,(int)d._pos);
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
                    p = cx.uids[p] ?? p;
                    r += vals[p] ?? TNull.Value;
                }
            return r;
        }
        public CList<TypedValue> MakeKey(CList<long> cols)
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
            return (vs != vals) ? new TableRow(this,vs) : this; // Check is done by caller: can't be done just now
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
            var vs = CTree<long,TypedValue>.Empty;
            for (var b = vals.First(); b != null; b = b.Next())
            {
                var nk = cx.Fix(b.key());
                if (cx.db.objects[b.key()] is TableColumn tc && tc.domain.kind == Qlx.POSITION)
                {
                    var v = b.value().ToLong() ?? -1L;
                    var nv = cx.uids[v] ?? v;
                    vs += (nk, new TInt(nv));
                }
                else
                    vs += (nk, b.value().Fix(cx));
            }
            var d = cx.db.objects[cx.Fix(tabledefpos)] as Domain ?? throw new PEException("PE50403");
            return new TableRow(this, ((Domain)d.Fix(cx)).defpos, vs);
        }
    }
    
    internal class RemoteTableRow(long dp, CTree<long, TypedValue> v, string u, RestRowSet r) : TableRow(r.target,v+(DBObject.Defpos,new TInt(dp))) 
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
