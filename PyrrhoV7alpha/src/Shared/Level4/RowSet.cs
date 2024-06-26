using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
using System.Text;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// A RowSet is the result of a stage of query processing: left to right semantics
    /// means that its construction should be delayed until the end of all dependent
    /// clauses for its syntax (i.e. all wheres, group by, orderings etc).
    /// The _Domain by this stage will have uids for all columns in the result (the display)
    /// or available for use in filtering/ordering clauses.
    /// 
    /// The where-condition in the query syntax is not quite the same the _Where property.
    /// The syntax allows:
    ///     a: columns from the _Domain. These can be operands in the _Where property.
    ///     b: columns from completed scopes to the left at this level, or in enclosing scopes
    ///         possibly accessed using a forward reference chain. These can be
    ///         operands in the _Where property.
    ///     c: aggregation operands. these are not in the domain of the aggregating selectrowset, 
    ///         but will be in the domain of one of the sources. These cannot be operands in the
    ///         _Where property, so expressions containing these must be passed to sources. 
    /// 
    /// The _Where and some other properties are determined after rowset creation:
    /// candidate properties to be added are supplied to the Apply method.
    /// The New method replaces all of the properties of the rowset other than defpos and type.
    /// 
    /// RowSets must not contain any physical uids in their column uids
    /// or within their domain's representation. See InstanceRowSet for how to avoid.
    /// 
    /// An aggregation E has a unique "home" rowset A that computes it: such that no source of A has E.
    /// A is always a SelectRowSet or RestRowSet, and E.from must be A.
    /// 
    /// IMMUTABLE
    ///     /
    /// </summary>
    internal abstract class RowSet : Domain
    {
        internal readonly static BTree<long, object> E = BTree<long, object>.Empty;
        [Flags]
        internal enum Assertions { None=0,
                SimpleCols=1, // columns are all SqlCopy or SqlLiteral
                MatchesTarget=2, // RowType is equal to sce.Rowtype
                AssignTarget=4, // RowType is statically assignment compatible to domain
                ProvidesTarget=8,// RowType's uids are in sce.RowType in some order
                SpecificRows=16 } // RowSet's rows correspond to specific records in the database
        internal const long
            AggDomain = -465, // Domain temporary during SplitAggs
            Ambient = -175, // CTree<long,bool> QlValue for RestView support
            Asserts = -212, // Assertions
            Assig = -174, // CTree<UpdateAssignment,bool> 
            _Built = -402, // bool
            _CountStar = -281, // long 
            _Data = -368, // long RowSet
            Distinct = -239, // bool
            Filter = -180, // CTree<long,TypedValue> matches to be imposed by this query
            Group = -199, // long GroupSpecification
            Groupings = -406,   //BList<long?>   Grouping
            GroupCols = -386, // Domain
            GroupIds = -408, // CTree<long,Domain> temporary during SplitAggs
            Having = -200, // CTree<long,bool> QlValue
            ISMap = -213, // BTree<long,long?> QlValue,TableColumn
            _Matches = -182, // CTree<long,TypedValue> matches guaranteed elsewhere
            Matching = -183, // CTree<long,CTree<long,bool>> QlValue QlValue (symmetric)
            OrdSpec = -184, // Domain
            Periods = -185, // BTree<long,PeriodSpec>
            UsingOperands = -411, // BTree<long,long?> QlValue
            Referenced = -378, // CTree<long,bool> QlValue (referenced columns)
            RestRowSetSources = -331, // CTree<long,bool>    RestRowSet or RestRowSetUsing
            _Rows = -407, // CList<TRow> 
            RowOrder = -404, // Domain
            RSTargets = -197, // BTree<long,long?> Table TableRowSet 
            SIMap = -214, // BTree<long,long?> TableColumn,QlValue
            _Scalar = -206, // bool
            _Source = -151, // long RowSet
            Static = -152, // RowSet (defpos for STATIC)
            Stem = -211, // CTree<long,bool> RowSet 
            Target = -153, // long (a table or view for simple IDU ops)
            _Where = -190, // CTree<long,bool> Boolean conditions to be imposed by this query
            Windows = -201; // CTree<long,bool> WindowSpecification
        internal Assertions asserts => (Assertions)(mem[Asserts] ?? Assertions.None);
        internal new string? name => (string?)mem[ObInfo.Name];
        internal BTree<string, (int,long?)> names => // -> QlValue
    (BTree<string, (int,long?)>?)mem[ObInfo.Names] ?? BTree<string, (int,long?)>.Empty;
        /// <summary>
        /// indexes are added where the targets are selected from tables, restviews, and INNER or CROSS joins.
        /// indexes are not added for unions or outer joins
        /// </summary>
        internal CTree<Domain, CTree<long, bool>> indexes => // as defined for tables and restview targets
            (CTree<Domain, CTree<long, bool>>?)mem[Table.Indexes]??CTree<Domain, CTree<long, bool>>.Empty;
        public override Domain domain => this;
        /// <summary>
        /// keys are added where the current set of columns includes all the keys from a target index or ordering.
        /// At most one keys entry is currently allowed: ordering if available, then target index
        /// </summary>
        internal Domain keys => (Domain?)mem[Level3.Index.Keys] ?? Row; 
        internal Domain ordSpec => (Domain?)mem[OrdSpec] ?? Row;
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>?)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal Domain rowOrder => (Domain?)mem[RowOrder] ?? Row;
        internal CTree<long, bool> where =>
            (CTree<long, bool>?)mem[_Where] ?? CTree<long, bool>.Empty;
        internal long data => (long)(mem[_Data]??-1L);
        internal CTree<long, TypedValue> filter =>
            (CTree<long, TypedValue>?)mem[Filter] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, TypedValue> matches =>
            (CTree<long, TypedValue>?)mem[_Matches] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, CTree<long, bool>> matching =>
            (CTree<long, CTree<long, bool>>?)mem[Matching]
            ?? CTree<long, CTree<long, bool>>.Empty;
        internal long groupSpec => (long)(mem[Group] ?? -1L);
        internal CTree<long, bool> ambient =>
            (CTree<long, bool>)(mem[Ambient] ?? CTree<long, bool>.Empty);
        internal long target => (long)(mem[Target] // for table-focussed RowSets
            ?? rsTargets.First()?.key() ?? -1L); // for safety
        internal BTree<long, long?> rsTargets =>
            (BTree<long, long?>?)mem[RSTargets] ?? BTree<long, long?>.Empty;
        internal int selectDepth => (int)(mem[QlValue.SelectDepth] ?? -1);
        internal long source => (long)(mem[_Source] ??  -1L);
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>?)mem[Assig]
            ?? CTree<UpdateAssignment, bool>.Empty;
        internal BTree<long, long?> iSMap =>
            (BTree<long, long?>?)mem[ISMap] ?? BTree<long, long?>.Empty;
        internal BTree<long, long?> sIMap =>
            (BTree<long, long?>?)mem[SIMap] ?? BTree<long, long?>.Empty;
        internal CList<TRow> rows =>
            (CList<TRow>?)mem[_Rows] ?? CList<TRow>.Empty;
        internal long lastData => (long)(mem[Table.LastData] ?? 0L);
        internal BList<long?> remoteCols =>
            (BList<long?>?)mem[RestRowSet.RemoteCols] ?? BList<long?>.Empty;
        /// <summary>
        /// The group specification
        /// </summary>
        internal long group => (long)(mem[Group] ?? -1L);
        internal BList<long?> groupings =>
            (BList<long?>?)mem[Groupings] ?? BList<long?>.Empty;
        internal Domain groupCols => (Domain)(mem[GroupCols]??Null);
        /// <summary>
        /// The having clause
        /// </summary>
        internal CTree<long, bool> having =>
            (CTree<long, bool>?)mem[Having] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal CTree<long, bool> window =>
            (CTree<long, bool>?)mem[Windows] ?? CTree<long, bool>.Empty;
        internal MTree? tree => (MTree?)mem[Level3.Index.Tree];
        internal bool scalar => (bool)(mem[_Scalar] ?? false);
        /// <summary>
        /// Constructor: a new or modified RowSet
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="cx">The processing environment</param>
        /// <param name="m">The properties tree</param>
        protected RowSet(long dp, Context cx, BTree<long, object> m)
            : base(dp, _Mem(dp,cx,m))
        {
            cx.Add(this);
        }
        /// <summary>
        /// Constructor: relocate a rowset with no property changes
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="m"></param>
        protected RowSet(long dp, BTree<long, object> m) : base(dp, m)
        {  }
        // Compute assertions. Also watch for Matching and Ambient info from sources
        protected static BTree<long, object> _Mem(long dp,Context cx, BTree<long, object> m)
        {
            var a = Assertions.SimpleCols | 
                (((Assertions)(m[Asserts] ?? Assertions.None)) & Assertions.SpecificRows);
            if (cx.obs[(long)(m[_Source] ?? m[_Data] ?? -1L)] is RowSet sce)
            {
                sce.AddFrom(cx, dp);
                var sb = sce.rowType.First();
                var rt = (BList<long?>)(m[RowType] ?? BList<long?>.Empty);
                for (var b = rt.First(); b != null && sb is not null; b = b.Next(), sb = sb?.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v &&
                        sb.value() is long sp && cx.obs[sp] is QlValue sv) {
                    if (!(v is SqlCopy || v is SqlLiteral))
                        a &= ~(Assertions.MatchesTarget | Assertions.ProvidesTarget
                            | Assertions.SimpleCols);
                    if (sb == null ||
                        !v.domain.CanTakeValueOf(sv.domain))
                        a &= ~Assertions.AssignTarget;
                    if (sb == null || v.defpos != sv.defpos)
                        a &= ~Assertions.MatchesTarget;
                    if (sce != null && !sce.representation.Contains(v.defpos))
                        a &= ~Assertions.ProvidesTarget;
                }
            }
            m += (Asserts, a);
            return m;
        }
        internal virtual Assertions Requires => Assertions.None;
        public static RowSet operator +(RowSet et, (long, object) x)
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
            return (RowSet)et.New(m + x);
        }
        public static RowSet operator +(RowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (RowSet)rs.New(m + (p, o));
        }
        internal override long ColFor(Context cx, string c)
        {
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob &&
                        ((p >= Transaction.TransPos && ob is QlValue sv
                            && (sv.alias ?? sv.name) == c)
                        || ob.NameFor(cx) == c))
                    return p;
            return -1L;
        }
        protected BTree<long,object> _Depths(Context cx,BTree<long,object> m,int d,long p,object o)
        {
            switch (p)
            {
                case Assig: m += (_Depth, cx._DepthTUb((CTree<UpdateAssignment, bool>)o, d)); break;
                case TransitionRowSet.Defaults:
                case Filter:
                case _Matches:
                case Periods: m += (_Depth, cx._DepthTVX((CTree<long, TypedValue>)o, d)); break;
                case TransitionRowSet.ForeignKeys:
                case Having:
                case SelectRowSet.RdCols:
                case Referenced:
                case RestRowSetSources:
                case Stem:
                case _Where:
                case Windows: m += (_Depth, cx._DepthTVX((CTree<long, bool>)o, d)); break;
                case InstanceRowSet.SRowType:
                case SqlRowSet.SqlRows:
                case Groupings: m += (_Depth, cx._DepthBV((BList<long?>)o, d)); break;
                case GroupIds: m += (_Depth, cx._DepthTVD((CTree<long, Domain>)o, d)); break;
                case ISMap:
                case RSTargets:
                case SIMap:
                case TransitionRowSet.TargetTrans:
                case TransitionRowSet.TransTarget:
                case UsingOperands: m += (_Depth, cx._DepthTVV((BTree<long, long?>)o, d)); break;
                case Matching: m += (_Depth, cx._DepthTVTVb((CTree<long, CTree<long, bool>>)o, d)); break;
                case ExplicitRowSet.ExplRows: m += (_Depth, Context._DepthLPlT(this, (BList<(long,TRow)>)o, d)); break;
                case _Rows: m += (_Depth, Context._DepthLT((CList<TRow>)o, d)); break;
                default:
                    {
                        if (o is long q && cx.obs[q] is DBObject ob)
                        {
                            d = Math.Max(ob.depth + 1, d);
                            if (d > depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }
            return m;
        }
        internal virtual BList<long?> SourceProps => BList<long?>.FromArray(_Source,
            JoinRowSet.JFirst, JoinRowSet.JSecond,
            MergeRowSet._Left, MergeRowSet._Right,
            RestRowSetUsing.RestTemplate,RestRowSetUsing.UsingTableRowSet);
        static readonly BList<long?> pre = BList<long?>.FromArray(Having,Matching, _Where, _Matches);
        static readonly BTree<long, bool> prt = new BTree<long, bool>(Having, true) 
            + (_Where, true) + (Matching, true) + (_Matches, true);
        internal virtual CTree<long,Domain> _RestSources(Context cx)
        {
            var r = CTree<long,Domain>.Empty;
            for (var b = SourceProps.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[(long)(mem[p]??-1L)] is RowSet rs)
                    r += rs._RestSources(cx);
            return r;
        }
        internal virtual CTree<long, bool> _ProcSources(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = SourceProps.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[(long)(mem[p] ?? -1L)] is RowSet rs)
                    r += rs._ProcSources(cx);
            return r;
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n, DBObject? r)
        {
            if (cx._Dom(defpos) is not Domain dm)
                throw new DBException("42105").Add(Qlx.DOMAIN);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && dm.representation[p] is DBObject co && n is not null)
                {
                    if (co.NameFor(cx) == nm && n is not null)
                    {
                        var ob = new QlValue(n, BList<Ident>.Empty, cx, dm.representation[co.defpos] ?? Content,
                            new BTree<long, object>(_From, defpos));
                        cx.Add(ob);
                        return (ob, n.sub);
                    }
                    if (co is UDType ut && ut.infos[cx.role.defpos] is ObInfo ui && n is not null
                        && cx.db.objects[ui.names[n.ident].Item2 ?? -1L] is DBObject so)
                    {
                        var ob = new SqlField(lp, nm + "." + n.ToString(), -1, p, 
                            ut.representation[so.defpos]??Content, so.defpos);
                        cx.DefineForward(n.ident);
                        cx.undefined -= lp;
                        cx.Add(ob);
                        return (ob, n.sub);
                    }
                }
            return base._Lookup(lp, cx, nm, n, r);
        }
        internal virtual RowSet Sort(Context cx,Domain os,bool dct)
        {
            if (os.CompareTo(rowOrder)==0) // skip if current rowOrder already finer
                return this;
            return new OrderedRowSet(cx, this, os, dct);
        }
        /// <summary>
        /// A change to some properties requiring further actions in general.
        /// For most properties, we can readily check that it can be
        /// applied to this rowset. 
        /// 
        /// Passing expressions down to source rowsets:
        /// A non-aggregating expression R (in the select tree or a where condition) can be passed down to 
        /// a source S if all its operands are known to S (it can then be added to R's finder)
        /// An aggregating expression E in R can be passed down to a SelectRowSet or RestRowSet source S if 
        /// all its operands are all known to S (for rowsets T between R and S in the pipeline see P1)
        /// 
        /// When a set of aggregations E are passed down from R to S 
        /// P1. The domain of S (and any T) must be transformed
        /// P2. Any group ids of E known to S must become grouped in S, and all other ids of E in S must also be grouped.
        /// 
        /// </summary>
        /// <param name="mm">The properties to be applied</param>
        /// <param name="cx">The context</param>
        /// <param name="m">PRIVATE: Show accumulating properties of the result</param>
        /// <returns>updated rowset</returns>
        internal virtual RowSet Apply(BTree<long, object> mm,Context cx,BTree<long,object>? m = null)
        {
            m ??= mem;
            for (var b=mm.First();b is not null;b=b.Next())
            {
                var k = b.key();
                if (m[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return (RowSet)New(m);
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            var od = cx.done;
            cx.done = ObTree.Empty;
            var recompute = false;
            for (var mb = pre.First(); mb != null; mb = mb.Next())
            if (mb.value() is long p && mm[p] is object v)
                    switch (p)
                    {
                        case Matching:
                            {
                                var mg = (CTree<long, CTree<long, bool>>)v;
                                var rm = (CTree<long, CTree<long, bool>>?)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
                                for (var b = mg.First(); b != null; b = b.Next())
                                {
                                    var x = b.key();
                                    var rx = rm[x] ?? CTree<long, bool>.Empty;
                                    var kx = Knows(cx, x);
                                    if (b.value() is CTree<long, bool> mx)
                                        for (var c = mx.First(); c != null; c = c.Next())
                                        {
                                            var y = c.key();
                                            if (!(kx || Knows(cx, y)))
                                                mx -= y;
                                            else
                                                rx += (y, true);
                                        }
                                    if (rx.Count > 0)
                                        rm += (x, rx);
                                    else
                                    {
                                        rm -= x;
                                        mg -= x;
                                    }
                                }
                                m += (Matching, mg);
                                mm -= Matching; // but not further down
                                break;
                            }
                        case _Where:
                            {
                                var w = (CTree<long, bool>)v;
                                var ma = (CTree<long, TypedValue>?)mm[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var oa = ma;
                                var mw = (CTree<long, bool>?)m[_Where] ?? CTree<long, bool>.Empty;
                                var mh = (CTree<long, bool>?)m[Having] ?? CTree<long, bool>.Empty;
                                for (var b = w.First(); b != null; b = b.Next())
                                    if (cx.obs[b.key()] is QlValue sv)
                                    {
                                        var k = b.key();
                                        var matched = false;
                                        if (sv is SqlValueExpr se && se.op == Qlx.EQL)
                                            if (cx.obs[se.left] is QlValue le &&
                                                cx.obs[se.right] is QlValue ri)
                                            {
                                                var im = (CTree<long, TypedValue>?)m[_Matches] ?? CTree<long, TypedValue>.Empty;
                                                if (le.isConstant(cx) && !im.Contains(ri.defpos))
                                                {
                                                    matched = true;
                                                    ma += (ri.defpos, le.Eval(cx));
                                                }
                                                if (ri.isConstant(cx) && !im.Contains(le.defpos))
                                                {
                                                    matched = true;
                                                    ma += (le.defpos, ri.Eval(cx));
                                                }
                                            }
                                        if (sv.KnownBy(cx, this, true)==true) // allow ambient values
                                        {
                                            if (source>=0)
                                                sv.AddFrom(cx, source);
                                            if (sv.IsAggregation(cx,CTree<long,bool>.Empty) != CTree<long, bool>.Empty)
                                                mh += (k, true);
                                            else if (!matched)
                                                mw += (k, true);
                                        }
                                        else
                                        {
                                            mw -= k;
                                            cx.forReview += (k, (cx.forReview[k] ?? CTree<long, bool>.Empty) + (defpos, true));
                                        }
                                    }
                                var ow = (CTree<long, bool>?)m[_Where] ?? CTree<long, bool>.Empty;
                                if (ma != oa)
                                    mm += (_Matches, ma);
                                if (mw.CompareTo(ow) != 0)
                                    m += (_Where, ow + mw);
                                var oh = (CTree<long, bool>?)m[Having] ?? CTree<long, bool>.Empty;
                                if (mh.CompareTo(oh) != 0)
                                    m += (Having, mh + oh);
                                break;
                            }
                        case _Matches:
                            {
                                var ma = (CTree<long, TypedValue>?)v ?? CTree<long, TypedValue>.Empty;
                                var ms = (CTree<long, TypedValue>?)m[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var im = (CTree<long, CTree<long, bool>>?)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
                                var ng = im + ((CTree<long, CTree<long, bool>>?)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty);
                                for (var b = ma.First(); b != null; b = b.Next())
                                    for (var c = ng[b.key()]?.First(); c != null; c = c.Next())
                                        ma += (c.key(), b.value());
                                for (var b = ma.First(); b != null; b = b.Next())
                                {
                                    var sp = b.key();
                                    if (!Knows(cx, sp))
                                    {
                                        ma -= sp;
                                        continue;
                                    }
                                    if (ms[sp] is TypedValue v0)
                                    {
                                        if (v0.CompareTo(b.value()) == 0)
                                            continue;
                                        cx.done = od;
                                        return new EmptyRowSet(defpos, cx, new Domain(-1L,m));
                                    }
                                    ms += (sp, b.value());
                                    var me = CTree<long, CTree<long, bool>>.Empty;
                                    var mg = (CTree<long, CTree<long, bool>>?)m[Matching] ?? me;
                                    var nm = (CTree<long, CTree<long, bool>>?)mm[Matching] ?? me;
                                    nm = CombineMatching(mg, nm);
                                    var rs = (CTree<long, Domain>)(m[Representation] ?? CTree<long, Domain>.Empty);
                                    for (var c = nm[sp]?.First(); c != null; c = c.Next())
                                    {
                                        var ck = c.key();
                                        if (rs.Contains(ck) && !ma.Contains(ck))
                                            ms += (ck, b.value());
                                    }
                                }
                                if (ms.Count == 0)
                                    m -= _Matches;
                                else
                                    m += (_Matches, ms);
                                break;
                            }
                    }
            for (var mb = mm.First(); mb != null; mb = mb.Next())
                if (mb.value() is object v)
                {
                    var p = mb.key();
                    switch (p)
                    {
                        case _Source:
                            {
                                recompute = true;
                                m += (_Source, v);
                                mm -= _Source;
                                if (v is long pf && cx.obs[pf] is DBObject fo)
                                    fo.AddFrom(cx, defpos);
                                break;
                            }
                        case Assig:
                            {
                                var sg = (CTree<UpdateAssignment, bool>)v;
                                for (var b = sg.First(); b != null; b = b.Next())
                                {
                                    var ua = b.key();
                                    if ((!Knows(cx, ua.vbl, true))
                                        || !((QlValue?)cx.obs[ua.val])?.KnownBy(cx, this, true) == true)
                                        sg -= b.key();
                                }
                                if (sg == CTree<UpdateAssignment, bool>.Empty)
                                    mm -= Assig;
                                else
                                    m += (Assig, sg);
                                break;
                            }
                        case UsingOperands:
                            {
                                var nr = (BTree<long, long?>)v;
                                var rt = (BList<long?>)(m[RowType]??BList<long?>.Empty);
                                var rs = (CTree<long,Domain>)(m[Representation]??CTree<long,Domain>.Empty);
                                // We need to ensure that the source rowset supplies all of the restOperands
                                // we now know about.
                                var ch = false;
                                for (var b = nr.First(); b != null; b = b.Next())
                                    if (cx.obs[b.key()] is QlValue s &&
                                        !rs.Contains(s.defpos))
                                    {
                                        rt += s.defpos;
                                        rs += (s.defpos, s.domain);
                                        ch = true;
                                    }
                                if (ch)
                                    m = m + (RowType, rt) + (Representation, rs);
                                // but don't go further down (??)
                                mm -= UsingOperands;
                                break;
                            }
                        default:
                            if (!prt.Contains(p))
                                m += (p, v);
                            break;
                    }
                }
            // Anything in mm that can't be passed to sources must have raised an exception by now
            if (mm != BTree<long, object>.Empty && mm?.First()?.key() < 0)
                // Recursively apply any remaining changes to sources
                // On return mm may contain pipeline changes
                for (var b = SourceProps.First(); b != null; b = b.Next())
                    if (b.value() is long p && mem[p] is long sp && sp > 0) // we have this kind of source rowset
                    {
                        var ms = mm;
                        if (p == RestView.UsingTable)
                            ms -= Target;
                        if (cx.obs[sp] is RowSet sc)
                        {
                            if (ms[_Where] is CTree<long, bool> wh)
                            {
                                for (var c = wh.First(); c != null; c = c.Next())
                                    if (cx.obs[c.key()] is QlValue wv && !wv.KnownBy(cx, sc))
                                        wh -= c.key();
                                ms += (_Where, wh);
                            }
                            sc = sc.Apply(ms, cx);
                            if (sc.defpos != sp)
                                m += (p, sc.defpos);
                        }
                    }
            if (recompute)
                m = _Mem(defpos, cx, m);
            var r = this;
            if (m != mem)
            {
                r = (RowSet)New(m);
                cx.Add(r);
            }
            cx.done = od;
            return r;
        }
        /// <summary>
        /// Implemented for SelectRowSet and RestRowSet
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual RowSet ApplySR(BTree<long, object> mm, Context cx)
        {
            return this;
        }
        /// <summary>
        /// If aggs got pushed down further there may be intermediate rowsets T
        /// If we construct a new source rowset S', rowsets T between R and S' in the pipeline must get new Domains
        /// These are built bottom-up.
        /// Selectlist expressions in T should be the subexpressions of R's select tree that it knows.
        /// Matching subexpressions in S (this) are flagged as ambient.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="sa">An intermediate rowset (T in the notes above)</param>
        internal void ApplyT(Context cx, RowSet sa)
        {
            var am = ambient;
            for (var b = Sources(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is RowSet s)
                {
                    if (s is SelectRowSet || s is RestRowSet 
                        || (this is RestRowSetUsing ru && ru.usingTableRowSet == s.defpos)
                        || !s.AggSources(cx).Contains(sa.defpos))
                        continue;
                    if (s is RestRowSetUsing rsu && cx.obs[rsu.template] is RestRowSet rrs
                        && cx.obs[rsu.usingTableRowSet] is TableRowSet uts)
                    {
                        var rt = BList<long?>.Empty;
                        var rp = CTree<long, Domain>.Empty;
                        for (var c = groupCols.First(); c != null; c = c.Next())
                            if (c.value() is long p && uts.Knows(cx, p))
                            {
                                rt += p;
                                rp += (p, representation[p] ?? Content);
                            }
                        for (var c = rrs.rowType.First(); c != null && c.key() < rrs.display;
                            c = c.Next())
                            if (c.value() is long p && !rp.Contains(p))
                            {
                                rt += p;
                                rp += (p, rrs.representation[p] ?? Content);
                            } 
                        for (var c=rrs.aggs.First();c!=null;c=c.Next())
                        {
                            var p = c.key();
                            if (!rp.Contains(p))
                            {
                                rt += p;
                                rp += (p, cx.obs[p]?.domain ?? Content); 
                            }
                        }
                        rsu = rsu + (RowType, rt) +(Representation,rp) + (Display,rt.Length)
                            + (cx,GroupCols, cx.GroupCols(groupCols.rowType, rsu)) + (Aggs,rrs.aggs);
                        cx.Add(rsu);
                        continue;
                    }
                    var rc = BList<long?>.Empty;
                    var rs = CTree<long, Domain>.Empty;
                    var kb = s.KnownBase(cx);
                    for (var c = representation.First(); c != null; c = c.Next())
                    { 
                        if (cx.obs[c.key()] is QlValue sc)
                        {
                            for (var d = sc.KnownFragments(cx, kb).First();
                                  d != null; d = d.Next())
                                if (cx.obs[d.key()] is QlValue v)
                                {
                                    var p = d.key();
                                    if (!rs.Contains(d.key()))
                                        rc += p;
                                    rs += (p, v.domain);
                                }
                        }
                    }
                    s = (RowSet)cx.Add(s + (RowType,rc)+(Representation,rs));
                    s.ApplyT(cx, sa);
                    am += s.ambient;
                    for (var c = s.groupCols.representation.First(); c != null; c = c.Next())
                            am += (c.key(),true);
                }
            if (am!=ambient)       
                cx.Add(this + (Ambient, am));
        }
        /// <summary>
        /// KnownBase computes a set of QlValue uids known by this rowSet. The set is
        /// defined by the keys of the CTree result. We ignore the Domain values of these trees.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns>A set of SqlValues used to compute this RowSet</returns>
        internal CTree<long,Domain> KnownBase(Context cx)
        {
            var kb = CTree<long, Domain>.Empty; 
            // we know the sources
            for (var b = Sources(cx).First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is RowSet s) { 
                kb += s.representation;
                kb += s.KnownBase(cx);
            }
            return kb;
        }
        internal virtual CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            if (cx.obs[source] is RowSet sc)
                for (var b = ag.First(); b != null; b = b.Next())
                    for (var c = ((QlValue?)cx.obs[b.key()])?.Operands(cx).First();
                        c != null; c = c.Next())
                        if (sc.Knows(cx, c.key()))
                            ma += (b.key(), true);
            return ma;
        }
        internal static CTree<long,CTree<long,bool>> 
            CombineMatching(CTree<long, CTree<long, bool>>p, CTree<long, CTree<long, bool>>q)
        {
            for (var b=q.First();b is not null;b=b.Next())
            {
                var k = b.key();
                var m = b.value();
                if (p[k] is CTree<long,bool> t)
                    p += (k, t + m);
                else
                    p += (k, m);
            }
            return p;
        }
        internal virtual bool CanSkip(Context cx)
        {
            return false;
        }
        protected virtual bool CanAssign()
        {
            return true; // but see SelectRowSet, EvalRowSet, GroupRowSet, OrderedRowSet
        }
        internal TRow _Row(Context cx,Cursor sce)
        {
            var a = asserts;
            if (a.HasFlag(Assertions.MatchesTarget))
                return new TRow(this, this, sce);//??
            if (a.HasFlag(Assertions.ProvidesTarget))
                return new TRow(this, sce.values);
            if (CanAssign() && a.HasFlag(Assertions.AssignTarget))
                return new TRow(sce, this);
            var vs = CTree<long, TypedValue>.Empty;
            var oc = cx.values;
            cx.values = sce.values;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = (ambient.Contains(p)?cx.values[p]:cx.obs[p]?.Eval(cx)) ?? TNull.Value;
                    if (v == TNull.Value && sce[p] is TypedValue tv
                        && tv != TNull.Value)
                        v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
                    vs += (p, v);
                }
            cx.values = oc;
            return new TRow(this, vs);
        }
        internal TypedValue RevIndex(Context cx,long t,Domain d)
        {
            if (cx.cursors[defpos] is Cursor cu && cx.db.objects[t] is Table tb
                 && tb.FindPrimaryIndex(cx) is Level3.Index px)
            {
                var r = new TSet(new Domain(-1L,Qlx.SET,px.keys));
                for (var b = tb.indexes[d]?.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Level3.Index x
                        && x.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                    {
                        var vs = CTree<long, TypedValue>.Empty;
                        for (var c = x.keys.First(); c != null; c = c.Next())
                            if (c.value() is long p)
                                vs += (p, cu[c.key()]);
                        if (x.MakeKey(vs) is CList<TypedValue> k)
                            for (var c = x.rows?.PositionAt(k, 0); c != null; c = c.Next())
                                if (c.key().CompareTo(k)==0 && c.Value() is long rp
                                    && tb.tableRows[rp] is TableRow tr
                                    && px.MakeKey(tr.vals) is CList<TypedValue> vv)
                                    r = r.Add(new TRow(px.keys,vv));
                    }
                return r;
            }
            return TNull.Value;
        }
        internal virtual string DbName(Context cx)
        {
            return cx.db.name??"_system";
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm,
            Grant.Privilege pr = Grant.Privilege.Select, string? a=null)
        {
            return (fm>=0)?this+(_From,fm):this;
        }
        internal RowSet AddUpdateAssignment(Context cx, UpdateAssignment ua)
        {
            var s = (long)(mem[_Source] ?? -1L);
            if (cx.obs[s] is RowSet rs && rs.Knows(cx,ua.vbl)
                && cx.obs[ua.val] is QlValue v && v.KnownBy(cx, rs))
                rs.AddUpdateAssignment(cx, ua);
            var r = this + (cx,Assig, assig + (ua, true));
            cx.Add(r);
            return r;
        }
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, Domain rt)
        {
            if (rsTargets == BTree<long, long?>.Empty)
                throw new DBException("42174");
            var r = base.Insert(cx, ts, rt);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Table tb)
                    r += tb.Insert(cx, ts, rt);
            return r;
        }
        internal override BTree<long, TargetActivation>Update(Context cx, RowSet fm)
        {
            if (rsTargets == BTree<long, long?>.Empty)
                throw new DBException("42174");
            var r = base.Update(cx, fm);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Table tb)
                    r += tb.Update(cx, fm);
            return r;
        }
        internal override BTree<long, TargetActivation> Delete(Context cx, RowSet fm)
        {
            if (rsTargets == BTree<long, long?>.Empty)
                throw new DBException("42174");
            var r = base.Delete(cx, fm);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Table tb)
                    r += tb.Delete(cx, fm);
            return r;
        }
        internal virtual bool Built(Context cx)
        {
            return true;
        }
        internal virtual RowSet Build(Context cx)
        {
            if (Built(cx))
                return this;
            var r = (RowSet)New(mem + (_Built, true));
            cx._Add(r);
            return r;
        }
        internal override CTree<long,bool>_Rdc(Context cx)
        {
            var rc = CTree<long,bool>.Empty;
            if (cx.db.autoCommit == true)
            {
                var d = display;
                if (d == 0)
                    d = int.MaxValue;
                for (var b = rowType.First(); b != null && d-- > 0; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue v)
                        rc += v._Rdc(cx);
            }
            return rc;
        }
        internal virtual int Cardinality(Context cx)
        {
            var r = 0;
            if (where == CTree<long, bool>.Empty)
            {
                for (var b = Sources(cx).First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is RowSet rs)
                        r += rs.Cardinality(cx);
                return r;
            }
            for (var b = First(cx); b != null; b = b.Next(cx))
                r++;
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RowSet)base.Fix(cx);
            if (r.defpos != defpos)
                for (var b = Sources(cx).First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue sv)
                        sv.AddFrom(cx, r.defpos);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var gs = cx.Fix(groupSpec);
            if (gs != groupSpec)
                r += (Group, gs);
            var ng = cx.FixLl(groupings);
            if (ng != groupings)
                r += (Groupings, ng);
            var nk = keys.Fix(cx);
            if (nk != keys)
                r += (Level3.Index.Keys, nk);
            var xs = cx.FixTDTlb(indexes);
            if (xs != indexes)
                r += (Table.Indexes, xs);
            var fl = cx.FixTlV(filter);
            if (filter != fl)
                r += (Filter, fl);
            var no = rowOrder.Fix(cx);
            if (no != rowOrder)
                r += (RowOrder, no);
            var nw = cx.FixTlb(where);
            if (nw != where)
                r += (_Where, nw);
            var nm = cx.FixTlV(matches);
            if (nm != matches)
                r += (_Matches, nm);
            var mg = cx.FixTlTlb(matching);
            if (mg != matching)
                r += (Matching, mg);
            var ts = cx.FixTll(rsTargets);
            if (ts!=rsTargets)
                r += (RSTargets, ts);
            var tg = cx.Fix(target);
            if (tg != target)
                r += (Target, tg);
            var ag = cx.FixTub(assig);
            if (ag != assig)
                r += (Assig, ag);
            var sim = cx.FixTll(sIMap);
            if (sim != sIMap)
                r += (SIMap, sim);
            var ism = cx.FixTll(iSMap);
            if (ism != iSMap)
                r += (ISMap, ism);
            var s = (long)(mem[_Source] ?? -1L);
            var ns = cx.Fix(s);
            if (ns != s)
                r += (_Source, ns);
            var na = cx.FixTsPil(names);
            if (na != names)
                r += (ObInfo.Names, na);
            r += (Asserts, asserts);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (RowSet)base.Replace(cx, was, now);
            if (r.defpos != defpos)
                for (var b = Sources(cx).First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is DBObject ob)
                        ob.AddFrom(cx, r.defpos);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSet)base._Replace(cx, so, sv);
    //        if (defpos >= 0 && cx.obs[defpos] is RowSet od && od.dbg > dbg)
    //            r = od;
            var fl = cx.ReplaceTlT(r.filter, so, sv);
            if (fl != r.filter)
                r += (cx, Filter, fl);
            var xs = cx.ReplacedTDTlb(r.indexes);
            if (xs != r.indexes)
                r += (Table.Indexes, xs);
            var ks = r.keys.Replaced(cx);
            if (ks != r.keys)
                r += (Level3.Index.Keys, ks);
            var si = cx.ReplacedTll(r.sIMap);
            if (si != r.sIMap)
                r += (SIMap, si);
            var mi = cx.ReplacedTll(r.iSMap);
            if (mi != r.iSMap)
                r += (ISMap, mi);
            var ns = cx.ReplacedTsPil(r.names);
            if (ns != r.names)
                r += (ObInfo.Names, ns);
            var os = r.ordSpec.Replace(cx, so, sv);
            if (os != r.ordSpec)
                r += (OrdSpec, os);
            var ro = r.rowOrder.Replaced(cx);
            if (ro != r.rowOrder)
                r += (RowOrder, ro);
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (QlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
            }
            if (w != r.where)
                r += (_Where, w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (QlValue)cx._Replace(b.key(), so, sv);
                if (bk.defpos != b.key())
                    ms += (bk.defpos, b.value());
            }
            if (ms != r.matches)
                r += (_Matches, ms);
            var mg = r.matching;
            if (so.defpos != sv.defpos)
                for (var b = mg.First(); b != null; b = b.Next())
                {
                    var a = b.key();
                    if (a == so.defpos)
                        a = sv.defpos;
                    var ma = b.value();
                    for (var c = ma.First(); c != null; c = c.Next())
                        if (c.key() == so.defpos)
                            ma = ma - so.defpos + (sv.defpos, true);
                    mg = mg - b.key() + (a, ma);
                }
            if (mg != r.matching)
                r += (Matching, mg);
            var ch = false;
            var ts = BTree<long, long?>.Empty;
            for (var b = r.rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long v && v != defpos)
                {
                    var p = cx.ObReplace(v, so, sv);
                    var k = cx.done[b.key()]?.defpos ?? b.key();
                    if (p != b.value() || k != b.key())
                        ch = true;
                    ts += (k, p);
                }
            if (ch)
                r += (RSTargets, ts);
            var n = cx.ObReplace(r.source, so, sv);
            if (r.source != n)
                r = Apply(E + (_Source, n), cx);
            if (cx.done[r.groupSpec] is GroupSpecification gs)
            {
                var ng = gs.defpos;
                if (ng != r.groupSpec)
                    r += (Group, ng);
                var og = r.groupings;
                r += (Groupings, cx.ReplacedLl(og));
                if (!Context.Match(r.groupings, og))
                    r += (GroupCols, cx.GroupCols(r.groupings, r));
            }
            if (r.target > 0 && cx._Replace(r.target, so, sv) is RowSet tg && tg.defpos != r.target)
                r += (Target, tg.defpos);
            var ua = CTree<UpdateAssignment, bool>.Empty;
            for (var b = r.assig?.First(); b != null; b = b.Next())
            {
                var na = b.key().Replace(cx, so, sv);
                ua += (na, true);
            }
            if (ua != r.assig)
                r += (Assig, ua);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (RowSet)base.ShallowReplace(cx, was, now);
            var si = sIMap;
            var pp = -1L;
            for (var b=si.First();b!=null;b=b.Next())
                if (b.key() is long p && p==was)
                {
                    pp = p;
                    si -= was;
                    si += (now, b.value());
                }
            if (pp>0)
            {
                r += (SIMap, si);
                r += (ISMap, iSMap + (pp, now));
            }
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            if (rowType[i] is long p)
            {
                if (cx.obs[p] is QlValue sv && (sv.alias ?? sv.name) is string sn && sn != "")
                    return sn;
                if (cx.obs[p] is Domain sd && sd.name is string n && n != "")
                    return n;
                if (cx.obs[p] is SqlCall sc && cx.NameFor(sc.procdefpos) is string pn)
                    return pn;
                if (this is InstanceRowSet ir && ir.sRowType[i] is long ip
                    && cx.db.objects[ip] is DBObject tc && tc.infos[cx.role.defpos] is ObInfo ci
                    && ci.name is string im && im != "")
                    return im;
                if (cx.obs[p] is SystemTableColumn ss && ss.name is string ns && ns != "")
                    return ns;
            }
            return "Col" + i;
        }
        internal virtual CTree<long,bool> Sources(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            var p = source;
            if (p >= 0)
                r += (p,true);
            return r;
        }
        internal virtual BTree<long,RowSet> AggSources(Context cx)
        {
            var r = BTree<long, RowSet>.Empty;
            if (cx.obs[source] is RowSet sc)
            {
                if (sc is RestRowSet || sc is SelectRowSet)
                    r += (sc.defpos, sc);
                else if (sc != null)
                    r += sc.AggSources(cx);
            }
            return r;
        }
        internal virtual CTree<long,Cursor> SourceCursors(Context cx)
        {
            var p = source;
            var r = (cx.obs[p] is RowSet rs)?rs.SourceCursors(cx) : CTree<long, Cursor>.Empty;
            if (cx.cursors[p] is Cursor cu)
                r += (p, cu);
            return r;
        }
        internal override DBObject QParams(Context cx)
        {
            var r = base.QParams(cx).mem;
            var m = cx.QParams(matches);
            if (m != matches)
                r += (_Matches, m);
            return (RowSet)New(r);
        }
        /// <summary>
        /// Can this calculate the value of reference rp?
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="rp"></param>
        /// <param name="ambient">allow ambient values (never allowed for Apply)</param>
        /// <returns></returns>
        internal virtual bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[rp] is SqlCopy sc && Knows(cx, sc.copyFrom, ambient))
                return true;
            if (rp == defpos)
                return true;
            for (var b = rowType.First(); b != null // && b.key()<ds
                    ; b = b.Next())
                if (b.value() == rp)
                    return true;
            if (ambient && cx.obs[rp] is QlValue sv 
                && (sv.from < defpos || sv.defpos < defpos) && sv.selectDepth<selectDepth)
                return true;
            return false;
        }
        internal BList<long?> _Info(Context cx, Grouping g, BList<long?> gs)
        {
            gs += g.defpos;
            var cs = BList<DBObject>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue s)
                    cs += s;
            var dm = new Domain(Qlx.ROW, cx, cs);
            cx.Add(dm);
            cx.Add((Grouping)g.New(g.mem + dm.mem));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
       //     int m = domain.rowType.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keys.First(); j < flags.Length && ib != null;
                ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = rowType.First(); b != null;
                    b = b.Next())
                    if (b.key() == ib.key())
                    {
                        adds[b.key()] = (j + 1) << 4;
                        found = true;
                        break;
                    }
                if (!found)
                    addFlags = false;
            }
            var d = display;
            if (d == 0)
                d = int.MaxValue;
            for (var b = rowType.First(); b != null && b.key()<d; b = b.Next())
                if (b.value() is long cp && representation[cp] is Domain dc)
                {
                    var i = b.key();
                    flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                    if (cx._Ob(cp) is DBObject tc)
                        flags[i] += (tc.domain.notNull ? 0x100 : 0) +
                            (((tc as TableColumn)?.generated != GenerationRule.None) ? 0x200 : 0);
                }
        }
        /// <summary>
        /// Bookmarks are used to traverse rowsets
        /// </summary>
        /// <returns>a bookmark for the first row if any</returns>
        protected abstract Cursor? _First(Context cx);
        protected abstract Cursor? _Last(Context cx);
        public virtual Cursor? First(Context cx)
        {
            var rs = Build(cx);
            rs = (RowSet)(cx.obs[rs.defpos] ?? throw new PEException("0089"));
            return rs._First(cx);
        }
        public virtual Cursor? Last(Context cx)
        {
            var rs = Build(cx);
            rs = (RowSet)(cx.obs[rs.defpos] ?? throw new PEException("0088"));
            return rs._Last(cx);
        }
        /// <summary>
        /// Find a bookmark for the given key (base implementation rarely used)
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual Cursor? PositionAt(Context cx, CList<TypedValue> key)
        {
            for (var bm = _First(cx); bm != null; bm = bm.Next(cx))
            {
                var kb = key.First();
                for (var b = keys.First(); kb is not null &&  b != null; b = b.Next(),kb=kb.Next())
                    if (kb.value()!=TNull.Value && b.value() is long p &&  cx.obs[p] is QlValue s && 
                        s.Eval(cx).CompareTo(kb.value()) != 0)
                        goto next;
                return bm;
            next:;
            }
            return null;
        }
        public void ShowPlan(Context cx)
        {
            var show = new BList<(int,long)>((0,defpos));
            while (show != BList<(int,long)>.Empty)
            {
                var (i,p) = show[0];
                show -= 0;
                if (cx.obs[p] is RowSet s)
                {
                    Console.WriteLine(s.ToString(i));
                    for (var b = s.Sources(cx).First(); b != null; b = b.Next())
                    {
                        p = b.key();
                        if (p >= 0)
                            show += (i + 1, p);
                    }
                }
            }
        }
        internal string ToString(int i)
        {
            var sb = new StringBuilder();
            for (var j = 0; j < i; j++)
                sb.Append(' ');
            sb.Append(Uid(defpos));
            sb.Append(' '); sb.Append(GetType().Name);
            sb.Append(' '); Show(sb);
            Show(sb);
            return sb.ToString();
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            string cm;
            if (scalar)
                sb.Append(" scalar ");
            if (indexes.Count > 0)
            {
                sb.Append(" Indexes=[");
                cm = "";
                for (var b = indexes.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    var cn = "(";
                    for (var d = b.key().First(); d != null; d = d.Next())
                        if (d.value() is long p)
                        {
                            sb.Append(cn); cn = ",";
                            sb.Append(Uid(p));
                        }
                    sb.Append(")=");
                    cn = "[";
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                            if (c.key() is long p)
                            {
                                sb.Append(cn); cn = ",";
                                sb.Append(Uid(p));
                            }
                        sb.Append(']');
                    }
                }
                sb.Append(']');
            }
            if (keys!=Row)
            {
                cm = " key (";
                for (var b = keys.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
            if (ordSpec != Row)
            {
                cm = " ordSpec (";
                for (var b = ordSpec.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p){
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
                sb.Append(')');
            }
            if (rowOrder != Row)
            {
                cm = " order (";
                for (var b = rowOrder.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
            if (where != CTree<long, bool>.Empty)
            {
                cm = " where (";
                for (var b = where.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
            if (having!=CTree<long,bool>.Empty)
            {
                sb.Append(" having (");
                cm = "";
                for (var b = having.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
            if (matches != CTree<long, TypedValue>.Empty)
            {
                cm = " matches (";
                for (var b = matches.First(); b != null; b = b.Next())
                    if (b.value() is TypedValue v)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(b.key())); sb.Append('=');
                        sb.Append(v);
                    }
                sb.Append(')');
            }
            if (matching != CTree<long, CTree<long, bool>>.Empty)
            {
                cm = " matching (";
                for (var b = matching.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = "),";
                    sb.Append(Uid(b.key())); sb.Append("=(");
                    var cc = "";
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                        sb.Append(cc); cc = ",";
                        sb.Append(Uid(c.key()));
                    }
                }
                sb.Append("))");
            }
            if (groupSpec >= 0)
            {
                sb.Append(" groupSpec: "); sb.Append(Uid(groupSpec));
            }
            if (groupings!=BList<long?>.Empty)
            { 
                sb.Append(" groupings (");
                cm = "";
                for (var b = groupings.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
                if (having != CTree<long, bool>.Empty)
                {
                    sb.Append(" having ("); cm = "";
                    for (var b = having.First(); b != null; b = b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(b.key()));
                    }
                    sb.Append(')');
                }
            }
            if (groupCols != null && groupCols.rowType != BList<long?>.Empty)
            {
                sb.Append(" GroupCols(");
                cm = "";
                for (var b = groupCols.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
            if (rsTargets != BTree<long, long?>.Empty)
            {
                sb.Append(" targets: ");
                cm = "";
                for (var b = rsTargets.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(b.key())); sb.Append('=');
                        sb.Append(Uid(p));
                    }
            }
            if (from!=-1L)
            {  sb.Append(" From: "); sb.Append(Uid(from)); }
            if (mem.Contains(_Source))
            {  sb.Append(" Source: "); sb.Append(Uid(source)); }
            if (data >0)
            { sb.Append(" Data: "); sb.Append(Uid(data)); }
            if (assig.Count > 0) { sb.Append(" Assigs:"); sb.Append(assig); }
            var ro = (BTree<long, long?>?)mem[UsingOperands]??BTree<long,long?>.Empty;
            if (ro != null && ro.Count>0)
            {
                sb.Append(" UsingOperands "); cm = "(";
                for(var b=ro.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm == ",")
                    sb.Append(')');
            }
            if (ambient!=CTree<long,bool>.Empty)
            {
                sb.Append(" Ambient("); cm = "";
                for (var b=ambient.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" Asserts ("); sb.Append(asserts);
                sb.Append(')');
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            Show(sb);
            return sb.ToString();
        }
    }
    /// <summary>
    /// 1. Throughout the code RowSets are navigated using Cursors instead of
    /// Enumerators of any sort. The great advantage of bookmarks generally is
    /// that they are immutable. (E.g. the ABookmark classes always refer to a
    /// snapshot of the tree at the time of constructing the bookmark.)
    /// 2. The Rowset field is readonly but NOT immutable as it contains the Context cx.
    /// cx.values is updated to provide shortcuts to current values.
    /// 3. The TypedValues obtained by Value(..) are mostly immutable also, but
    /// updates to row columns (e.g. during trigger operations) are managed by 
    /// allowing the Cursor to override the usual results of 
    /// evaluating a row column: this is needed in (at least) the following 
    /// circumstances (a) UPDATE (b) INOUT and OUT parameters that are passed
    /// column references (c) triggers.
    /// 4. In view of the anxieties of 3. these overrides are explicit
    /// in the Cursor itself.
    ///  / (but e.g. LogSystemBookmark  and its subclasses are not)
    /// </summary>
    internal abstract class Cursor : TRow
    {
        internal static BTree<long,(long,long)> E = BTree<long,(long,long)>.Empty;
        public readonly long _rowsetpos;
        public readonly Domain _dom; // snapshot of RowSet
        public readonly int _pos;
        public readonly BTree<long, (long,long)> _ds;   // target->(defpos,ppos)
        public readonly int display;
        protected Cursor(Context cx, RowSet rs, int pos, BTree<long,(long,long)> ds, 
            TRow rw) : base(rs, rw.values)
        {
            _rowsetpos = rs.defpos;
            _dom = rs;
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
            cx.values += values;
        }
        protected Cursor(Context cx, RowSet rs, int pos, BTree<long,(long,long)> ds, 
            TypedValue[] vs) 
            : base(rs, vs)
        {
            _rowsetpos = rs.defpos;
            _dom = rs;
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
            cx.values += values;
        }
        protected Cursor(Context cx, Cursor cu)
            : base((Domain)cu.dataType.Fix(cx), cx.FixTlV(cu.values))
        {
            _rowsetpos = cx.Fix(cu._rowsetpos);
            _dom = cu._dom;
            _pos = cu._pos;
            _ds = cx.FixBlll(cu._ds);
            display = cu.display;
            cx.cursors += (cu._rowsetpos, this);
            cx.values += values;
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx, long rd, Domain dm, int pos, BTree<long,(long,long)> ds,
            TRow rw) : base(dm, rw.values)
        {
            _rowsetpos = rd;
            _dom = dm;
            _pos = pos;
            _ds = ds;
            cx.cursors += (rd, this);
            cx.values += values;
        }
        protected Cursor(Cursor cu, Context cx, long p, TypedValue v)
            : base(cu.dataType, cu.values + (p, v))
        {
            _rowsetpos = cu._rowsetpos;
            _dom = cu._dom;
            _pos = cu._pos;
            _ds = cu._ds;
            display = cu.display;
            cx.cursors += (cu._rowsetpos, this);
            cx.values += values;
        }
        public static Cursor operator +(Cursor cu, (Context, long, TypedValue) x)
        {
            var (cx, p, tv) = x;
            return cu.New(cx, p, tv);
        }
        protected virtual Cursor New(Context cx, long p, TypedValue v)
        {
            throw new DBException("42174");
        }
        public virtual Cursor? Next(Context cx)
        {
            for (var b = _dom.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    cx.values -= p;
            return _Next(cx);
        }
        public virtual Cursor? Previous(Context cx)
        {
            for (var b = _dom.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    cx.values -= p;
            return _Previous(cx);
        }
        protected abstract Cursor? _Next(Context cx);
        protected abstract Cursor? _Previous(Context cx);
        internal ETag _Rvv(Context cx)
        {
            var rs = (RowSet?)cx.obs[_rowsetpos]??throw new PEException("PE1505");
        //    var dn = rs.DbName(cx);
            var rvv = Rvv.Empty;
            if (rs is TableRowSet ts && cx._Ob(ts.target) is Table tb)
            {
                if ((ts.where == CTree<long, bool>.Empty && ts.matches == CTree<long, TypedValue>.Empty)
                 || ts.aggs != CTree<long, bool>.Empty)
                    rvv += (ts.target, (-1L, tb.lastData));
                else if (cx.cursors[ts.defpos] is Cursor cu)
                    rvv += (ts.target, cu);
            }
            else
                for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is RowSet tg)
                    {
                        if (((tg.where == CTree<long, bool>.Empty && tg.matches == CTree<long, TypedValue>.Empty)
                        || tg.aggs != CTree<long, bool>.Empty)
                        && cx.obs[p] is TableRowSet t && cx._Ob(t.target) is Table tt)
                            rvv += (tg.target, (-1L, tt.lastData));
                        else if (cx.cursors[tg.defpos] is Cursor cu)
                            rvv += (tg.target, cu);
                    }
            return new ETag(cx.db,rvv);
        }
        internal bool Matches(Context cx)
        {
            if (cx.obs[_rowsetpos] is RowSet rs)
            {
                for (var b = rs.matches.First(); b != null; b = b.Next())
                    if (values[b.key()] is TypedValue v && v.CompareTo(b.value()) != 0)
                        return false;
                for (var b = rs.where.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue sw && sw.Eval(cx) != TBool.True)
                        return false;
            }
            return true;
        }
        /// <summary>
        /// These methods are for join processing
        /// </summary>
        /// <returns></returns>
        public virtual MTreeBookmark? Mb()
        {
            return null; // throw new PEException("PE543");
        }
        public virtual Cursor? ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (Mb()?.Value() == mb.Value())
                return this;
            return null; 
        }
        internal abstract BList<TableRow>? Rec();
        internal TNode? node(Context cx)
        {
            if (cx.obs[_rowsetpos] is NodeType nt && Rec()?[0] is TableRow tr)
                return new TNode(cx,tr);
            return null;
        }
        internal string NameFor(Context cx, int i)
        {
            var p = _dom.rowType[i];
            return cx.obs[p??-1L]?.name??"";
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
    internal class GroupingBookmark : Cursor
    {
        public readonly RowSet _grs;
        public readonly ABookmark<int, TRow> _ebm;
        GroupingBookmark(Context _cx, RowSet grs,
            ABookmark<int, TRow> ebm, int pos)
            : base(_cx, grs, pos, E, ebm.value())
        {
            _grs = grs;
            _ebm = ebm;
            _cx.cursors += (grs.defpos, this);
        }
        GroupingBookmark(GroupingBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        {
            _grs = cu._grs;
            _ebm = cu._ebm;
            cx.cursors += (_grs.defpos, this);
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new GroupingBookmark(this, cx, p, v);
        }
        internal static GroupingBookmark? New(Context cx, RowSet grs)
        {
            var ebm = grs.rows?.First();
            var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
            return r;
        }
        internal static GroupingBookmark? New(RowSet grs, Context cx)
        {
            var ebm = grs.rows?.Last();
            var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
            return r;
        }
        /// <summary>
        /// Move to the next grouped row
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context cx)
        {
            var ebm = _ebm.Next();
            if (ebm == null)
            {
                cx.funcs -= _grs.defpos;
                return null;
            }
            var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
            for (var b = _grs.representation.First(); b != null; b = b.Next())
                ((QlValue?)cx.obs[b.key()])?.OnRow(cx, r);
            return r;
        }
        protected override Cursor? _Previous(Context cx)
        {
            var ebm = _ebm.Previous();
            if (ebm == null)
            {
                cx.funcs -= _grs.defpos;
                return null;
            }
            var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
            for (var b = _grs.representation.First(); b != null; b = b.Next())
                ((QlValue?)cx.obs[b.key()])?.OnRow(cx, r);
            return r;
        }
        internal override BList<TableRow> Rec()
        {
            return BList<TableRow>.Empty;
        }
    }
    internal class EvalBookmark : Cursor
    {
        readonly RowSet _ers;
        internal EvalBookmark(Context _cx, RowSet ers)
            : base(_cx, ers, 0, E, ers.rows[0] ?? throw new PEException("PE48147"))
        {
            _ers = ers;
        }
        internal static EvalBookmark? New(Context cx, RowSet ers)
        {
            if (ers.rows[0] == null)
                return null;
            return new EvalBookmark(cx, ers);
        }
        EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        {
            _ers = cu._ers;
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new EvalBookmark(this, cx, p, v);
        }
        protected override Cursor? _Next(Context _cx)
        {
            _cx.funcs -= _ers.defpos;
            return null; // just one row in the rowset
        }
        protected override Cursor? _Previous(Context cx)
        {
            cx.funcs -= _ers.defpos;
            return null; // just one row in the rowset
        }
        internal override BList<TableRow> Rec()
        {
            return BList<TableRow>.Empty;
        }
    }
    /// <summary>
    /// A rowset of distinct TRows
    /// </summary>
    internal class BindingRowSet : RowSet
    {
        internal MTree? mt =>(MTree?)mem[Level3.Index.Tree];
        internal BindingRowSet(Context cx,long dp, Domain dm) 
            : this(dp, new BTree<long, object>(RowType, dm.rowType)
                  +(Representation,dm.representation)
                  +(Level3.Index.Tree,new MTree(dm, TreeBehaviour.Ignore, 0)))
        {
            cx.Add(this);
        }
        protected BindingRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BindingRowSet operator+(BindingRowSet sr,(long,object)x)
        {
            return new BindingRowSet(sr.defpos, sr.mem + x);
        }
        public static BindingRowSet operator+(BindingRowSet sr,(Context,TRow) x)
        {
            var (cx,r) = x;
            var k = CList<TypedValue>.Empty;
            for (var b = sr.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    k += r[p];
            if (sr.mt is null || sr.mt.Contains(k))
                return sr;
            sr = sr + (_Rows, sr.rows + r) + (Level3.Index.Tree,sr.mt+(k, 0, sr.rows.Count));
            cx.Add(sr);
            return sr;
        }
        /// <summary>
        /// Ensure the BindingRowSet has bindings for nb
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="nb"></param>
        /// <returns></returns>
        internal static BindingRowSet Get(Context cx,TRow rw)
        {
            if (cx.obs[cx.result] is BindingRowSet bs)
            {
                var rt = bs.rowType;
                var rs = bs.representation;
                for (var b = rw.dataType.representation.First(); b != null; b = b.Next())
                {
                    if (rs[b.key()] is Domain bd)
                    {
                        if (bd.defpos != b.value().defpos)
                            throw new DBException("42104", b.key());
                    } else
                    {
                        rt += b.key();
                        rs += (b.key(), b.value());
                    }
                }
                if (rt.Count==bs.Length)
                    return bs;
                var nd = new Domain(cx.GetUid(), cx, Qlx.TABLE, rs, rt);
                var rws = CList<TRow>.Empty;
                for (var b = bs.rows.First(); b != null; b = b.Next())
                    if (b.value() is TRow r) {
                        var nr = new TRow(nd, r.values + rw.values);
                        rws += nr;
                    }
                return (BindingRowSet)cx.Add(new BindingRowSet(cx, nd.defpos, nd) + (_Rows, rws));
            }
            return (BindingRowSet)cx.Add(new BindingRowSet(cx, cx.GetUid(), rw.dataType) + (_Rows, new CList<TRow>(rw)));
        }
        protected override Cursor? _First(Context _cx)
        {
            return BindingCursor.New(_cx, this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return BindingCursor.New(this,_cx);
        }
        internal Cursor? PositionAt(Context cx,int k)
        {
            return (rows.PositionAt(k) is ABookmark<int, TRow> b) ? new BindingCursor(cx, this, 0, b) : null;
        }
        internal override int Cardinality(Context cx)
        {
            return 1;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(rows);
            return sb.ToString();
        }
        public class BindingCursor: Cursor
        {
            readonly BindingRowSet _srs;
            readonly ABookmark<int, TRow> _bmk;
            internal BindingCursor(Context cx, BindingRowSet rs, int pos, ABookmark<int, TRow> bmk)
                : base(cx, rs, pos, E, rs.rows[bmk.key()]??new TRow(rs,CTree<long,TypedValue>.Empty))
            {
                _srs = rs;
                _bmk = bmk;
            }
            BindingCursor(BindingCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _srs = cu._srs;
                _bmk = cu._bmk;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new BindingCursor(this, cx, p, v);
            }
            internal static BindingCursor? New(Context _cx, BindingRowSet rs)
            {
                for (var b = rs.rows.First(); b is not null; b = b.Next())
                {
                    var rb = new BindingCursor(_cx, rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal static BindingCursor? New(BindingRowSet rs, Context _cx)
            {
                for (var b = rs.rows.Last(); b != null; b = b.Previous())
                {
                    var rb = new BindingCursor(_cx, rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var b = _bmk.Next(); b is not null; b = b.Next())
                {
                    var rb = new BindingCursor(_cx, _srs, _pos + 1, b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var b = _bmk.Previous(); b != null; b = b.Previous())
                {
                    var rb = new BindingCursor(_cx, _srs, _pos + 1, b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow>? Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    /// <summary>
    /// A rowset of one TRow
    /// </summary>
    internal class TrivialRowSet: RowSet
    {
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)(mem[Singleton]??TRow.Empty);
        internal TrivialRowSet(Context cx,Domain dm) 
            : this(cx.GetUid(),cx,new TRow(dm)) { }
        internal TrivialRowSet(long dp, Context cx, TRow r, string? a=null)
            : base(dp, cx, _Mem(dp,cx,r.dataType,a)+(Singleton,r)
                  +(Asserts,Assertions.ProvidesTarget))
        {
            cx.Add(this);
        }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(long dp, Context cx, Domain dm, string? a)
        {
            var ns = BTree<string, (int, long?)>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string n)
                    ns += (n, (b.key(), p));
            var r = dm.mem + (ObInfo.Names, ns);
            if (a != null)
                r = r + (_Alias, a) + (_Ident, new Ident(a, new Iix(dp)));
            return r;
        }
        public static TrivialRowSet operator +(TrivialRowSet et, (long, object) x)
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
            return (TrivialRowSet)et.New(m + x);
        }
        public static TrivialRowSet operator +(TrivialRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            if (p==Singleton)
                m += (_Depth,Math.Max(((TRow)o).dataType.depth+1,d));
            else
                m = rs._Depths(cx, m, d, p, o);
            return (TrivialRowSet)rs.New(m + (p, o));
        }
        protected override Cursor? _First(Context _cx)
        {
            return new TrivialCursor(_cx,this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return new TrivialCursor(_cx, this);
        }
        internal override int Cardinality(Context cx)
        {
            return 1;
        }
        public override Cursor? PositionAt(Context _cx,CList<TypedValue> key)
        {
            var kb = key.First();
            for (var b=row.dataType.rowType.First(); b != null && kb is not null; b=b.Next(),kb=kb.Next())
                if (kb.value()!=TNull.Value)
                    return null;
            return new TrivialCursor(_cx, this);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new TrivialRowSet(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nr = row.Fix(cx);
            if (row!=nr)
                r += (Singleton, nr);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TrivialRowSet)base._Replace(cx, so, sv);
            var rw = (TRow)row.Replace(cx, so, sv);
            if (rw!=row)
                r += (Singleton, rw);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TrivialRowSet(defpos,m);
        }
        public override string ToString()
        {
            var sb= new StringBuilder(base.ToString());
            sb.Append(row);
            return sb.ToString();
        }
        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t) 
                :base(_cx,t,0,E,_Val(_cx,t.domain))
            {
                trs = t;
            }
            TrivialCursor(TrivialCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                trs = cu.trs;
            }
            static TRow _Val(Context cx,Domain dm)
            {
                var vs = CTree<long, TypedValue>.Empty;
                for (var b = dm.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue sv)
                        vs += (p, sv.Eval(cx));
                cx.values += vs;
                return new TRow(dm, vs);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TrivialCursor(this, cx, p, v);
            }
            protected override Cursor? _Next(Context _cx)
            {
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    /// <summary>
    /// A rowset consisting of selected Columns from another rowset (e.g. SELECT A,B FROM C).
    /// 
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        internal override Assertions Requires => Assertions.SimpleCols;
        /// <summary>
        /// If all uids of dm are uids of r.domain (maybe in different order)
        /// </summary>
        internal SelectedRowSet(Context cx, Domain dm, RowSet r)
                :base(cx.GetUid(),_Mem(cx,dm,r)+(_Source,r.defpos)
                     +(RSTargets,new BTree<long,long?>(r.target,r.defpos))
                     +(Asserts,r.asserts) + (ISMap,r.iSMap) + (SIMap,r.sIMap))
        {
            cx.Add(this);
            r.AddFrom(cx, defpos);
        } 
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, Domain dm, RowSet r)
        {
            var m = r.mem + (_Domain,dm);
            var ns = BTree<string, (int,long?)>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string n)
                    ns += (n, (b.key(),p));
            return m + (ObInfo.Names, ns);
        }
        public static SelectedRowSet operator +(SelectedRowSet et, (long, object) x)
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
            return (SelectedRowSet)et.New(m + x);
        }
        public static SelectedRowSet operator +(SelectedRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (SelectedRowSet)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectedRowSet(defpos, m);
        }
        internal override bool CanSkip(Context cx)
        {
            if (cx.obs[source] is not RowSet sc)
                return false;
            return Context.Match(rowType,sc.rowType) && target >= Transaction.Analysing;
        }
        internal override RowSet Apply(BTree<long,object> mm,Context cx,BTree<long,object>?m=null)
        {
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            var ags = assig;
            if (mm[Assig] is CTree<UpdateAssignment,bool> sg)
            for (var b=sg.First();b is not null;b=b.Next())
            {
                var ua = b.key();
                if (!Knows(cx,ua.vbl))
                    sg -= b.key(); ;
                if ((!assig.Contains(ua)) && Knows(cx,ua.vbl))
                    ags += (ua, true);
            }
            mm += (Assig, ags);
            return base.Apply(mm,cx);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SelectedRowSet(dp,m);
        }
        internal override bool Built(Context cx)
        {
            return ((RowSet?)cx.obs[source])?.Built(cx)??false;
        }
        internal override RowSet Build(Context cx)
        {
            if (!Built(cx))
                (((RowSet?)cx.obs[source]) ?? throw new PEException("PE1510")).Build(cx);
            return this;
        }
        protected override Cursor? _First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return SelectedCursor.New(this, _cx);
        }
        
        internal class SelectedCursor : Cursor
        {
            internal readonly SelectedRowSet _srs;
            internal readonly Cursor? _bmk; // for rIn
            internal SelectedCursor(Context cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(cx,srs, pos, bmk._ds, bmk) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectedCursor(SelectedCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,p,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            internal static SelectedCursor? New(Context cx,SelectedRowSet srs)
            {
                for (var bmk = ((RowSet?)cx.obs[srs.source])?.First(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal static SelectedCursor? New(SelectedRowSet srs, Context cx)
            {
                for (var bmk = ((RowSet?)cx.obs[srs.source])?.Last(cx); 
                    bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectedCursor(this, cx, p, v);
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var bmk = _bmk?.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                for (var bmk = _bmk?.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, _srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _bmk?.Rec()??BList<TableRow>.Empty;
            }
        }
    }
    
    internal class SelectRowSet : RowSet
    {
        internal const long
            Building = -401, // bool
            RdCols = -124, // CTree<long,bool> TableColumn (needed cols for select tree and where)
            ValueSelect = -385; // long QlValue
        internal bool building => mem.Contains(Building);
        internal CTree<long, bool> rdCols =>
            (CTree<long, bool>)(mem[RdCols] ?? CTree<long, bool>.Empty);
        internal long valueSelect => (long)(mem[ValueSelect] ?? -1L);
        internal TRow? row => rows[0];
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec (select tree defines dm)
        /// Query environment can supply values for the select tree but source columns
        /// should bind more closely.
        /// </summary>
        internal SelectRowSet(Iix lp, Context cx, Domain dm, RowSet r, BTree<long, object>? m = null)
            : base(lp.dp, _Mem(cx, dm, r, m)) //  not p, cx, _Mem..
        {
            cx.Add(this);
        }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Context cx,Domain dm,RowSet r,BTree<long,object>?m)
        {
            m ??= BTree<long,object>.Empty;
            var gr = (long)(m[Group]??-1L);
            var groups = (GroupSpecification?)cx.obs[gr];
            var gs = BList<long?>.Empty;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                gs = r._Info(cx, g, gs);
            m += (Groupings, gs);
            var os = (Domain)(m[OrdSpec] ?? Row);
            var di = (bool)(m[Distinct] ?? false);
            if (os.CompareTo(r.rowOrder) != 0 || di != r.distinct)
                r = (RowSet)cx.Add(new OrderedRowSet(cx, r, os, di));
            var rc = CTree<long, bool>.Empty;
            var d = Math.Max(dm.depth, r.depth) + 1;
            var wh = (CTree<long, bool>?)m[_Where];
            // For SelectRowSet, we compute the RdCols: all needs of selectors and wheres
            if (m[ISMap] is BTree<long, long?> im)
            {
                for (var b = dm.rowType.First(); b != null && b.key() < d; b = b.Next())
                    if (b.value() is long p && dm.representation[p] is DBObject ob)
                        for (var c = ob.Needs(cx).First(); c != null; c = c.Next())
                            if (im[c.key()] is long k)
                                rc += (k, true);
                for (var b = wh?.First(); b != null && b.key() < d; b = b.Next())
                    if (cx._Ob(b.key()) is DBObject ok)
                        for (var c = ok.Needs(cx).First(); c != null; c = c.Next())
                            if (im[c.key()] is long k)
                                rc += (k, true);
            }
            m += (RdCols, rc);
            var ma = (CTree<long, CTree<long, bool>>?)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            if (ma.Count > 0 || r.matching.Count > 0)
                m += (Matching, CombineMatching(ma, r.matching));
            for (var b = r.rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is TableRowSet tt)
                {
                    // see what indexes we can use
                    if (tt.indexes != CTree<Domain, CTree<long,bool>>.Empty)
                    {
                        var xs = CTree<Domain,CTree<long,bool>>.Empty;
                        for (var x = tt.indexes.First();x is not null;x=x.Next())
                        {
                            var k = x.key();
                            for (var c = k.First(); c != null; c = c.Next())
                                if (((BTree<long,long?>?)m[ISMap])?.Contains(c.value()??-1L)==true)
                                    goto next;
                            var t = tt.indexes[k] ?? CTree<long, bool>.Empty;
                            if (k.Length!=0)
                                xs += (k, t+x.value());
                            next:;
                        }
                        m += (Table.Indexes, xs);
                    }
                }
            if (r.keys != Row)
                m += (Level3.Index.Keys, r.keys);
            var a = r.asserts;
            var ns = BTree<string, (int, long?)>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)    
                {
                    if (v is not SqlCopy && v is not SqlLiteral)
                        a &= ~(Assertions.MatchesTarget | Assertions.ProvidesTarget
                        | Assertions.SimpleCols);
                    ns += (v.NameFor(cx), (b.key(), v.defpos));
                }
            return cx.DoDepth(m + dm.mem + (_Source,r.defpos) + (RSTargets,r.rsTargets) + (Asserts,a)
                + (QlValue.SelectDepth, cx.sD) + (ObInfo.Names,ns));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SelectRowSet(dp,m);
        }
        public static SelectRowSet operator +(SelectRowSet et, (long, object) x)
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
            return (SelectRowSet)et.New(m + x);
        }
        public static SelectRowSet operator +(SelectRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (SelectRowSet)rs.New(m + (p, o));
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null)
        {
            return this;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectRowSet)base._Replace(cx, so, sv);
            var no = so.alias ?? ((so as QlValue)?.name) ?? so.infos[cx.role.defpos]?.name;
            var nv = sv.alias ?? ((sv as QlValue)?.name) ?? sv.infos[cx.role.defpos]?.name;
            if (nv is not null && no!=nv && no is not null && names is BTree<string,(int,long?)> ns 
                && names.Contains(no))
            {
                ns = ns + (nv, names[no]) - no;
                r+=(cx, ObInfo.Names, ns);
            }
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SelectRowSet)base.Fix(cx);
            if (r.id != null)
                r += (_Ident, r.id.Fix(cx));
            return r;
        }
        internal override bool Built(Context cx)
        {
            if (building)
                throw new PEException("0017");
            if (aggs != CTree<long, bool>.Empty && !mem.Contains(MatchStatement.MatchFlags)) 
                return false; 
            return (bool)(mem[_Built]??false);
        }
        protected override bool CanAssign()
        {
            return false;
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[valueSelect] is QlValue svs && cx.obs[svs.from] is RowSet es)
                for (var b = es.Sources(cx).First(); b != null; b = b.Next())
                    if (cx._Dom(b.key())?.representation.Contains(rp)==true)
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long, object>? m = null)
        {
            var rt = this;
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            var am = BTree<long, object>.Empty;
            var gr = (long)(mm[Group] ?? -1L);
            var groups = (GroupSpecification?)cx.obs[gr];
            var gs = groupings;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                    gs = _Info(cx, g, gs);
            if (gs != groupings)
            {
                am += (Groupings, gs);
                am += (GroupCols, cx.GroupCols(gs, this));
                am += (Group, gr);
            }
            if (mm[Aggs] is CTree<long, bool> t)
                am += (Aggs, aggs + t);
            if (mm[Having] is CTree<long, bool> h && h != CTree<long, bool>.Empty)
                am += (Having, having + h);
            var pm = mm - Aggs - Group - Groupings - GroupCols - Having;
            var r = (SelectRowSet)base.Apply(pm, cx);
            if (am != BTree<long, object>.Empty)
                r = (SelectRowSet)r.ApplySR(am + (AggDomain, rt), cx); // NB  AggDomain has previous row type
            return r;
        }
        /// <summary>
        /// Version of Apply for applyng Aggs and Groups to SelectRowSet and RestRowSet.
        /// 
        /// We get here if there are any aggregations at this level (and if there are groupings there must be aggs).
        /// Let R be this, and have a source S. During this method, new properties that were proposed for R
        /// may be applied to a source S instead, and so on, for example, aggregation or grouping.
        /// This may result in some subexpressions becoming meaningless in R, because its value has been calculated
        /// earlier in the pipeline (and is now available in cx.values). We want to replace these subexpressions
        /// with the special QlValue Ambient so that cx.values gives the value instead of the subexpression.
        /// Selectlist expressions in T should be the subexpressions of R's select tree that it knows.
        /// </summary>
        /// <param name="mm">Aggs,Group,Having properties for S</param>
        /// <param name="cx">The context</param>
        /// <returns>The new state of rowset S.</returns>
        internal override RowSet ApplySR(BTree<long, object> mm, Context cx)
        {
            for (var b = mm.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return this;
            var ag = (CTree<long, bool>?)mm[Aggs] ?? throw new DBException("42105").Add(Qlx.FUNCTION);
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            if (mm[GroupCols] is Domain gc && gc.Length>0 && mm[Groupings] is BList<long?> lg
                && mm[Group] is long gs)
            {
                m += (GroupCols, gc);
                m += (Groupings, lg);
                m += (Group, gs);
                m += (Aggs, ag);
            }
            else
                // if we get here we have a non-grouped aggregation to add
                m += (Aggs, ag);
            var r = (SelectRowSet)New(m);
            cx.Add(r);
            if (mm[Having] is CTree<long, bool> h)
            {
                var hh = CTree<long, bool>.Empty;
                for (var b = h.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue v)
                    {
                        if (v.KnownBy(cx, r) == true)
                            hh += (b.key(), true);
                    }
                if (hh != CTree<long, bool>.Empty)
                    m += (Having, hh);
                r = (SelectRowSet)New(m);
                cx.Add(r);
            }
            var am = ambient;
            // see if the aggs and goupings can be pushed down further 
            for (var b = r.SplitAggs(mm, cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is RowSet a && (a is RestRowSet || a is RestRowSetUsing)) // a is where they can move to
                {
                    var na = a.ApplySR(b.value() + (AggDomain, r), cx);
                    if (na != a)
                    {
                        r = cx.obs[defpos] as SelectRowSet ?? throw new PEException("PE207034");
                        r.ApplyT(cx, na);
                    }
                    if (a is RestRowSet)
                        for (var c = na.groupCols.representation.First(); c != null; c = c.Next())
                            am += (c.key(), true);
                }
            if (am != ambient)
                cx.Add(r + (Ambient, am));
            cx.done = od;
            return cx.obs[defpos] as RowSet ?? throw new DBException("42105").Add(Qlx.TABLE); // will have changed
        }
        /// <summary>
        /// In the general case, an aggregating expression might contain aggregations from different sources,
        /// and if any of these are RestRowSets (or even SelectRowSets), we would like to push them down,
        /// to distribute the work.
        /// 
        /// A simple example to bear in mind is
        /// select sum(a)+count(f),b,g from v,w group by b,g
        /// If one of v,w is remote, this is well worth the trouble to aggregate it separately.
        /// as network traffic and computer load are reduced.
        /// The KnownFragments methods help to split up the select items in R. We try to preserve R's column ordering
        /// in the resulting S's as far as possible (since mostly no splitting of expressions occurs)..
        /// 
        /// At the level of the given rowset R, we have some aggregation expressions, 
        /// containing a possibly larger nunber of aggregates and a number of groups.
        /// For each source restrowset or selectrowset S (e.g. va or vb) we want to traverse the given select tree, 
        /// identifying aggregation functions and group ids that are known to S. 
        /// For any such we construct a subquery S' containing just these.
        /// With our uid machinery there is no need to form new aliases, and R is not changed in the process
        /// (we can continue to write sum(a)+count(f) in R).
        /// </summary>
        /// <param name="mm">R's aggregation properties (this is a SelectRowSet R)</param>
        /// <param name="cx">The context</param>
        /// <returns>A set of aggregation properties for AggSources S</returns>
        internal BTree<long, BTree<long,object>> SplitAggs(BTree<long,object> mm, Context cx)
        {
            var r = BTree<long, BTree<long,object>>.Empty;
            var rdct = aggs.Count == 0 && distinct;
            var ss = AggSources(cx);
            var ua = BTree<long, long?>.Empty; // all uids -> unique sid
            var ub = CTree<long, bool>.Empty; // uids with unique sids
            if (ss == BTree<long, RowSet>.Empty || !mm.Contains(Domain.Aggs))
                return r;
            for (var b = ss.First(); b != null; b = b.Next())
            {
                var k = b.key();
                for (var c = b.value()?.rowType.First(); c != null; c = c.Next())
                    if (c.value() is long u && !ub.Contains(u))
                    {
                        if (ua.Contains(u))
                        {
                            ub += (u, true);
                            ua -= u;
                        }
                        else
                            ua += (u, k);
                    }
            }
            for (var b = ((CTree<long, bool>?)mm[Aggs])?.First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is QlValue e){
                var a = b.key(); // An aggregating SqlFunction
                var sa = -1L;
                if (e is SqlFunction sf && sf.op == Qlx.COUNT && sf.mod == Qlx.TIMES)
                {
                    //if (Sources(cx).Count > 1)
                    //    goto no;
                    for (var c = ss.First(); c != null; c = c.Next())
                    {
                        var ex = r[c.key()] ?? BTree<long, object>.Empty;
                        var eg = (CTree<long, bool>?)ex[Aggs] ?? CTree<long, bool>.Empty;
                        r += (c.key(), ex + (Aggs, eg + (a, true)));
                    }
                    continue;
                }
                for (var c = e.Operands(cx).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    if (ub.Contains(u) || !ua.Contains(u) ) goto no;
                    if (sa < 0)
                        sa = ua[u]??-1L;
                    else if (sa != ua[u]) goto no;
                }
                if (sa < 0) goto no;
                var ax = r[sa] ?? BTree<long, object>.Empty;
                var ag = (CTree<long, bool>?)ax[Aggs] ?? CTree<long, bool>.Empty;
                r += (sa, ax + (Aggs, ag + (a, true)));
            }
            for (var b = ((Domain?)mm[GroupCols])?.representation?.First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is QlValue g){// might be an expression
                var sa = -1L;
                for (var c = g.Operands(cx).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    if (ub.Contains(u) || !ua.Contains(u)) goto no;
                    if (sa < 0)
                        sa = ua[u]??-1L;
                    else if (sa != ua[u]) goto no;
                }
                if (sa < 0) goto no;
                var ux = r[sa] ?? BTree<long, object>.Empty;
                var gc = (CTree<long, Domain>?)ux[GroupIds] ?? CTree<long, Domain>.Empty; // temporary 
                r += (sa, ux + (GroupIds, gc + (g.defpos, b.value())));
            }
            // groupCols is a Domain
            for (var b = ss.First(); b != null; b = b.Next())
            {
                var s = b.value();
                if (!r.Contains(s.defpos))
                    continue;
                var sp = r[s.defpos];
                if (sp == null)
                    continue;
                var nc = s.groupCols?.rowType ?? BList<long?>.Empty;
                var ns = s.groupCols?.representation ?? CTree<long, Domain>.Empty;
                for (var c = ((CTree<long, Domain>?)sp[GroupIds])?.First(); c != null; c = c.Next())
                {
                    var k = c.key();
                    if ((cx.obs[k] as QlValue)?.KnownBy(cx,s)!=true)//(!s.Knows(cx, k))
                        continue;
                    nc += k;
                    ns += (k, c.value());
                }
                if (nc != BList<long?>.Empty)
                {
                    var nd = new Domain(cx.GetUid(), cx, Qlx.ROW, ns, nc);
                    r += (s.defpos, sp +nd.mem);
                    cx.Add(nd);
                    sp -= GroupIds;
                    sp += (GroupCols, nd);
                    r += (s.defpos, sp + (Group, group) + (Groupings, groupings));
                }
                if (rdct)
                    r += (s.defpos,sp+(Distinct, true));
            }
            return r;
        no: // if nothing can be done, return empty
            return BTree<long, BTree<long,object>>.Empty;
        }
        internal override bool CanSkip(Context cx)
        {
            if (cx.obs[source] is RowSet sc)
                return Context.Match(rowType,sc.rowType) && where.CompareTo(sc.where) == 0;
            return false;
        }
        internal override RowSet Build(Context cx)
        {
            if (Built(cx))
                return this;
            if (building)
                throw new PEException("0077");
            var ags = aggs;
            cx.groupCols += (defpos, groupCols);
            if (ags != CTree<long, bool>.Empty && cx.obs[source] is RowSet sce)
            {
                SqlFunction? countStar = null;
                if (display == 1 && cx.obs[rowType[0] ?? -1L] is SqlFunction sf
                    && _ProcSources(cx).Count == 0
                    && sf.op == Qlx.COUNT && sf.mod == Qlx.TIMES)
                    countStar = sf;
                if (countStar != null && sce.Built(cx) && _RestSources(cx).Count == 0 && groupCols.Length == 0)
                {
                    var v = sce.Build(cx)?.Cardinality(cx) ?? 0;
                    Register rg = new(cx, TRow.Empty, countStar) { count = v };
                    var cp = rowType[0] ?? -1L;
                    cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty
                        + (TRow.Empty, new BTree<long, Register>(cp, rg)));
                    var sg = new TRow(this, new CTree<long, TypedValue>(cp, new TInt(v)));
                    cx.values += (defpos, sg);
                    return (RowSet)cx.Add(this + (_Built, true) + (_Rows, new CList<TRow>(sg)));
                }
                var r = (SelectRowSet)cx.Add(this + (Building, true));
                cx.obs += (defpos, r); // doesn't change depth
                cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty);
                sce = sce.Build(cx);
                var nrest = sce is not RestRowSet && sce is not RestRowSetUsing;
                if (nrest)
                    for (var rb = sce.First(cx); rb != null; rb = rb.Next(cx))
                        if (r.groupings.Count == 0)
                            for (var b0 = ags.First(); b0 != null; b0 = b0.Next())
                            {
                                if (cx.obs[b0.key()] is SqlFunction sf0)
                                    sf0.AddIn(TRow.Empty, cx);
                            }
                        else for (var g = r.groupings.First(); g != null; g = g.Next())
                                if (g.value() is long p && cx.obs[p] is Grouping gg)
                                {
                                    var vals = CTree<long, TypedValue>.Empty;
                                    for (var gb = gg.keys.First(); gb != null; gb = gb.Next())
                                        if (gb.value() is long gp && cx.obs[gp] is QlValue v)
                                            vals += (gp, v.Eval(cx));
                                    var key = new TRow(r.groupCols, vals);
                                    for (var b1 = ags.First(); b1 != null; b1 = b1.Next())
                                        if (cx.obs[b1.key()] is SqlFunction sf1)
                                            sf1.AddIn(key, cx);
                                }
                var rws = CList<TRow>.Empty;
                var fd = cx.funcs[defpos];
                for (var b = fd?.First(); b != null; b = b.Next())
                    if (b.value() != BTree<long, Register>.Empty)
                    {
                        // Remember the aggregating SqlValues are probably not just aggregation SqlFunctions
                        // Seed the keys in cx.values
                        var vs = b.key().values;
                        cx.values += vs;
                        for (var d = r.matching.First(); d != null; d = d.Next())
                            if (cx.values[d.key()] is TypedValue v)
                                for (var c = d.value().First(); c != null; c = c.Next())
                                    if (!vs.Contains(c.key()))
                                        vs += (c.key(), v);
                        // and the aggregate function accumulated values
                        for (var c = aggs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is QlValue v)
                            {
                                if (v is SqlFunction fr && fr.op == Qlx.RESTRICT && nrest)
                                    cx.values += (fr.val, fr.Eval(cx));
                                else
                                    cx.values += (v.defpos, v.Eval(cx));
                            }
                        // compute the aggregation expressions from these seeds
                        for (var c = rowType.First(); c != null; c = c.Next())
                            if (c.value() is long p && cx.obs[p] is QlValue sv
                                && sv.IsAggregation(cx, aggs) != CTree<long, bool>.Empty)
                                vs += (sv.defpos, sv.Eval(cx));
                        // add in any exposed RESTRICT values
                        for (var c = aggs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlFunction fr && fr.op == Qlx.RESTRICT && nrest)
                                vs += (fr.val, fr.Eval(cx));
                        // for the having calculation to work we must ensure that
                        // having uses the uids that are in aggs
                        for (var h = r.having.First(); h != null; h = h.Next())
                            if (cx.obs[h.key()]?.Eval(cx) != TBool.True)
                                goto skip;
                        rws += new TRow(this, vs);
                    skip:;
                    }
                //            cx.cursors = oc;
                if (rws == CList<TRow>.Empty)
                    rws += new TRow(this, CTree<long, TypedValue>.Empty);
                r = (SelectRowSet)r.New(r.mem - Building);
                cx.obs += (defpos, r); // doesnt change depth
                return (RowSet)cx.Add((RowSet)r.New(r.mem + (_Rows, rws) + (_Built, true) - Level3.Index.Tree
                    + (Groupings, r.groupings)));
            }
            return (RowSet)cx.Add(this + (_Built, true));
        }
        internal override RowSet Sort(Context cx, Domain os, bool dct)
        {
            if (rows.Count>0)
            {
                var rws = BList<(long, TRow)>.Empty;
                for (var b = rows.First(); b != null; b = b.Next())
                    rws += ((b.key(),b.value()));
                var ers = new ExplicitRowSet(cx.GetUid(), cx, this-_Rows, rws);
                return ers.Sort(cx, os, dct);
            } 
            return base.Sort(cx, os, dct);
        }
        public override Cursor? First(Context _cx)
        {
            var r = (SelectRowSet)Build(_cx);
            _cx.rdC += rdCols;
            if (aggs != CTree<long, bool>.Empty)
            {
                if (groupings == BList<long?>.Empty)
                    return EvalBookmark.New(_cx, r);
                return GroupingBookmark.New(_cx, r);
            }
            return SelectCursor.New(_cx, r);
        }
        protected override Cursor? _First(Context cx)
        {
            return SelectCursor.New(cx,this);
        }
        public override Cursor? Last(Context _cx)
        {
            _cx.rdC += rdCols;
            return base.First(_cx);
        }
        protected override Cursor? _Last(Context cx)
        {
            return SelectCursor.New(this, cx);
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, Domain rt)
        {
            return cx.obs[source]?.Insert(cx, ts, rt) ?? throw new DBException("42105").Add(Qlx.INSERT);
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm)
        {
            return cx.obs[source]?.Delete(cx,fm) ?? throw new DBException("42105").Add(Qlx.DELETE);
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm)
        {
            return cx.obs[source]?.Update(cx,fm) ?? throw new DBException("42105").Add(Qlx.UPDATE);
        }
        
        internal class SelectCursor : Cursor
        {
            readonly SelectRowSet _srs;
            readonly Cursor? _bmk; // for rIn, not used directly for Eval
            SelectCursor(Context _cx, SelectRowSet srs, Cursor bmk, int pos)
                : base(_cx, srs, pos, bmk._ds, srs._Row(_cx, bmk))
            {
                _bmk = bmk;
                _srs = srs;
                for (var b = srs.matching.First(); b != null; b = b.Next())
                    if (_cx.values[b.key()] is TypedValue v)
                        for (var c = b.value().First(); c != null; c = c.Next())
                            _cx.values += (c.key(), v);
            }
            SelectCursor(SelectCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectCursor(this, cx,p, v);
            }
            internal static SelectCursor? New(Context cx, SelectRowSet srs)
            {
                var sce = (RowSet?)cx.obs[srs.source];
                for (var bmk = sce?.First(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                   var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                cx.funcs -= srs.defpos;
                return null;
            }
            internal static SelectCursor? New(SelectRowSet srs, Context cx)
            {
                for (var bmk = ((RowSet?)cx.obs[srs.source])?.Last(cx);
                      bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                cx.funcs -= srs.defpos;
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var bmk = _bmk?.Next(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        (cx.obs[b.key()] as QlValue)?.OnRow(cx,rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                cx.funcs -= _srs.defpos;
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                for (
                    var bmk = _bmk?.Previous(cx); 
                    bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        ((QlValue?)cx.obs[b.key()])?.OnRow(cx, rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                cx.funcs -= _srs.defpos;
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _bmk?.Rec()??BList<TableRow>.Empty;
            }
        }
    }
     /// <summary>
    /// This abstract class helps with RowSets with an underlying database object.
    /// Each instance will have transaction-local defpos and rowType, but for
    /// triggers, insert, update, delete etc the column uids need to map to the shared rowType
    /// and vice versa.
    /// </summary>
    internal abstract class InstanceRowSet : RowSet
    {
        internal const long
            SRowType = -215; // BList<long?>   TableColumn
        internal CTree<long, bool> referenced =>
            (CTree<long, bool>?)mem[Referenced] ?? CTree<long, bool>.Empty;
        internal BList<long?> sRowType =>
            (BList<long?>?)mem[SRowType] ?? BList<long?>.Empty;
        internal override Assertions Requires => Assertions.SimpleCols;
        protected InstanceRowSet(long dp, Context cx, BTree<long, object> m)
            : base(dp, _Mem1(cx,m)) 
        { }
        protected InstanceRowSet(long dp, BTree<long, object> m)
            : base(dp, m) 
        { }
        static BTree<long,object> _Mem1(Context cx,BTree<long,object> m)
        {
            m ??= BTree<long, object>.Empty;
            var rt = (BList<long?>)(m[RowType]??BList<long?>.Empty);
            var rs = (CTree<long, Domain>)(m[Representation]??CTree<long, Domain>.Empty);
            var ism = BTree<long, long?>.Empty; // QlValue, TableColumn
            var sim = BTree<long, long?>.Empty; // TableColumn,QlValue
            var sr = (BList<long?>?)m[SRowType]??BList<long?>.Empty;
            var ns = BTree<string, (int,long?)>.Empty;
            var nr = CTree<long, Domain>.Empty;
            var fb = rt.First();
            for (var b = sr.First(); b != null && fb != null; b = b.Next(), fb = fb.Next())
                if (b.value() is long sp && cx._Ob(sp) is DBObject ob && fb.value() is long ip)
                {
                    ism += (ip, sp);
                    sim += (sp, ip);
                    if (cx.obs[ip] is QlValue sv)
                    {
                        ns += (ob.alias ?? sv.name ?? "", (b.key(), ip));
                        nr += (ip, sv.domain);
                    }
                    else
                        nr += (ip, rs[ip] ?? Domain.Content);
                }
            if (nr == CTree<long, Domain>.Empty)
                nr = rs;
       //     if (cx.obs[(long)(m[Target]??-1L)] is Table tb)
       //         m += (Table.Indexes,tb.IIndexes(cx,sim));
            return m + (ISMap, ism) + (SIMap, sim) + (ObInfo.Names,ns) + (Representation,nr);
        }
        public static InstanceRowSet operator +(InstanceRowSet et, (long, object) x)
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
            return (InstanceRowSet)et.New(m + x);
        }
        public static InstanceRowSet operator +(InstanceRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (InstanceRowSet)rs.New(m + (p, o));
        }
        internal override CTree<long, Cursor> SourceCursors(Context cx)
        {
            return CTree<long,Cursor>.Empty;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (InstanceRowSet)base._Replace(cx, so, sv);
            var srt = cx.ReplacedLl(r.sRowType);
            if (srt != r.sRowType)
                r += (cx, SRowType, srt);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var srt = cx.FixLl(sRowType);
            if (srt != sRowType)
                r += (SRowType, srt);
            return r;
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (target >= 0) { sb.Append(" Target="); sb.Append(Uid(target)); }
            sb.Append(" SRow:(");
            var cm = "";
            for (var b = sRowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(p));
                }
            sb.Append(')');
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" ISMap:("); cm = "";
                for (var b = iSMap.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                        sb.Append('='); sb.Append(Uid(p));
                    }
                sb.Append(')');
                sb.Append(" SIMap:("); cm = "";
                for (var b = sIMap.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                        sb.Append('='); sb.Append(Uid(p));
                    }
                sb.Append(')');
                sb.Append(" Referenced: ("); cm = "";
                for (var b = referenced.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
        }
    }
    /// <summary>
    /// A TableRowSet is constructed for each TableReference accessible from the current role.
    /// Feb 2023: enhanced to allow subtypes.
    /// The domain information for a table is directly in the table properties.
    /// RowType and Representation include inherited columns.
    /// The corresponding properties in Table exclude inherited columns.
    /// </summary>
    internal class TableRowSet : InstanceRowSet
    {
        internal const long
            _Index = -410; // long Index
        public long index => (long)(mem[_Index] ?? -1L);
        /// <summary>
        /// Constructor: a rowset defined by a base table
        /// </summary>
        internal TableRowSet(long lp,Context cx, long t, BTree<long,object>? m = null)
            : base(lp, cx, _Mem(lp,cx,t,m))
        {
            cx.Add(this);
        }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(long dp, Context cx, long t, BTree<long,object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (cx.db.objects[t] is not Table tb) 
                throw new DBException("42105").Add(Qlx.TABLE);
            cx._Add(tb);
            cx.Add(tb.framing);
            if (tb.NameFor(cx) is not string n)
                throw new DBException("42105").Add(Qlx.TABLE_NAME);
            /*
             * TableColumns are placed in the base table(s) for the type and supertype(s) that specify them 
             * (the partial records in each base table all have the same defining position, 
             * corresponding to the position of the record in the transaction log). 
             * Selecting from the subtype gives each of its rows, 
             * including columns inherited from the supertype. 
             * The order and visibility of columns is determined by the current role.
             */
            var tn = new Ident(n, cx.Ix(dp));
            var rt = BList<long?>.Empty;        // our total row type
            var rs = CTree<long, Domain>.Empty; // our total row type representation
            var sr = BList<long?>.Empty;        // our total SRow type
            var tr = BTree<long, long?>.Empty;  // mapping to physical columns
            var ns = BTree<string, (int,long?)>.Empty; // column names
            (rt,rs,sr,tr,ns) = tb.ColsFrom(cx,dp,rt,rs,sr,tr,ns);
            var xs = CTree<Domain, CTree<long, bool>>.Empty;
            var pk = Row;
            for (var b = tb.indexes.First(); b != null; b = b.Next())
            {
                var bs = BList<DBObject>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() is long p && cx._Ob(tr[p]??-1L) is QlValue tp)
                        bs += tp;
                var nk = (Domain)cx.Add(new Domain(-1L,cx,Qlx.ROW,bs,bs.Length));
                if (pk == Row)
                {
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is Level3.Index x && x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                            pk = nk;
                    if (pk.Length!=0)
                        pk = (Domain)cx.Add(pk);
                }
                if (nk.Length!=0)
                    xs += (nk, b.value());
            }
            if (pk != null)
                m += (Level3.Index.Keys,pk);
            cx.AddDefs(tn, rt, (string?)m[_Alias]);
            if (rt.Length==0) // add POSITION
            {
                var ps = new SqlFunction(cx.GetUid(), cx, Qlx.POSITION, null, null, null, Qlx.NO);
                ps += (_From, dp);
                cx.Add(ps);
                rt += ps.defpos;
                rs += (ps.defpos, Int);
            }
            var r = (m ?? BTree<long, object>.Empty)
                + (RowType,rt) + (Representation,rs)+(Display,rt.Length)
                + (SRowType, sr) + (Table.Indexes, xs)
                + (Table.LastData, tb.lastData) + (QlValue.SelectDepth,cx.sD)
                + (Target,t) + (ObInfo.Names,ns) + (ObInfo.Name,tn.ident)
                + (_Ident,tn) +(RSTargets,new BTree<long,long?>(t,dp));
            r = cx.DoDepth(r);
            r += (Asserts, (Assertions)(_Mem(dp,cx, r)[Asserts]??Assertions.None));
            return r;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new TableRowSet(dp,m);
        }
        internal override BList<long?> SourceProps => BList<long?>.Empty;
        internal override RowSet Apply(BTree<long,object> mm, Context cx, BTree<long,object>? m = null)
        {
            if (mm == BTree<long, object>.Empty)
                return this;
            if (cx.undefined!=CTree<long,int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            m ??= mem;
            if (mm[_Where] is CTree<long, bool> w)
            {
                for (var b = w.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var imm = (CTree<long, TypedValue>?)mm[_Matches] ?? CTree<long, TypedValue>.Empty;
                    if (cx.obs[k] is SqlValueExpr se && se.op == Qlx.EQL &&
                        cx.obs[se.left] is QlValue le && cx.obs[se.right] is QlValue ri)
                    {
                        if (le.isConstant(cx) && !imm.Contains(ri.defpos))
                            mm += (_Matches, imm + (ri.defpos, le.Eval(cx)));
                        if (ri.isConstant(cx) && !imm.Contains(le.defpos))
                            mm += (_Matches, imm + (le.defpos, ri.Eval(cx)));
                    }
                }
                mm += (_Where, w);
            }
            if (mm[_Matches] is CTree<long,TypedValue> ma && rowOrder==Domain.Null)
            {
                var trs = this;
                var (index, _, _) = trs.BestForMatch(cx, ma);
                index ??= trs.BestForOrdSpec(cx, (Domain)(m[OrdSpec]??Domain.Row));
                if (index != null && index.rows != null)
                {
                    mm += (Level3.Index.Tree, index.rows);
                    mm += (_Index, index.defpos);
                    for (var b = trs.indexes.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        if (b.value().Contains(index.defpos))
                            mm += (Level3.Index.Keys, k);
                    }
                }
            }
            return base.Apply(mm, cx, m);
        }
        internal (Level3.Index?, int, CList<TypedValue>) BestForMatch(Context cx, BTree<long, TypedValue> filter)
        {
            int matches = 0;
            var match = CList<TypedValue>.Empty;
            Level3.Index? index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
                for (var c = p.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index x &&
                               x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey)
                           && x.tabledefpos == target)
                    {
                        int sc = 0;
                        int nm = 0;
                        var pr = CList<TypedValue>.Empty;
                        var havematch = false;
                        int sb = 1;
                        for (var b = x.keys.rowType.Last(); b != null; b = b.Previous())
                        {
                            var j = b.key();
                            var d = b.value();
                            for (var fd = filter.First(); fd != null; fd = fd.Next())
                                if (cx.obs[fd.key()] is SqlCopy co && co.copyFrom==d)
                                {
                                    sc += 9 - j;
                                    nm++;
                                    pr += fd.value();
                                    havematch = true;
                                    goto nextj;
                                }
                            pr += TNull.Value;
                        nextj:;
                        }
                        if (!havematch)
                            pr = CList<TypedValue>.Empty;
                        sc += sb;
                        if (sc > bs)
                        {
                            index = x;
                            matches = nm;
                            match = pr;
                            bs = sc;
                        }
                    }
            return (index, matches, match);
        }
        internal Level3.Index? BestForOrdSpec(Context cx, Domain ordSpec)
        {
            Level3.Index? index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
                for (var c = p.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index x &&
                              x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey)
                          && x.tabledefpos == target &&
                          cx._Dom(x.defpos) is Domain dm)
                    {
                        int sc = 0;
                        int n = 0;
                        int sb = 1;
                        var j = dm.rowType.Length - 1;
                        for (var b = dm.rowType.Last(); b != null; b = b.Previous(), j--)
                            if (n < ordSpec.Length)
                            {
                                var ok = ordSpec[n];
                                if (ok != -1L)
                                {
                                    n++;
                                    sb *= 10;
                                }
                            }
                        sc += sb;
                        if (sc > bs)
                        {
                            index = x;
                            bs = sc;
                        }
                    }
            return index;
        }
        public static TableRowSet operator +(TableRowSet et, (long, object) x)
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
            return (TableRowSet)et.New(m + x);
        }
        public static TableRowSet operator +(TableRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (TableRowSet)rs.New(m + (p, o));
        }
        internal override int Cardinality(Context cx)
        {
            return(where==CTree<long,bool>.Empty && cx.obs[target] is Table tb)? 
                (int)tb.tableRows.Count
                :base.Cardinality(cx);
        }
        internal override CTree<long, bool> _ProcSources(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[from] is SelectRowSet ss && cx.obs[ss.valueSelect] is QlValue svs && cx.obs[svs.from] is RowSet es)
                for (var b = es.Sources(cx).First(); b != null; b = b.Next())
                    if ((cx.obs[b.key()] as Domain)?.representation.Contains(rp)==true)
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableRowSet)base._Replace(cx, so, sv);
            r +=(cx, RSTargets, cx.ReplacedTll(rsTargets));
            return r;
        }
        internal CTree<Domain, CTree<long, bool>> IIndexes(Context cx)
        {
            var xs = CTree<Domain, CTree<long, bool>>.Empty;
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var bs = BList<DBObject>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() is long p && cx._Ob(cx.Fix(p)) is Domain tp)
                        bs += tp;
                var k = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Qlx.ROW,bs,bs.Length));
                xs += (k, b.value());
            }
            return xs;
        }
        public override Cursor? First(Context _cx)
        {
            var key = CList<TypedValue>.Empty;
            for (var b = keys.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    TypedValue v = TNull.Value;
                    if (matches[p] is TypedValue t0 && t0 != TNull.Value)
                        v = t0;
                    if (v == TNull.Value)
                        for (var c = matching[p]?.First(); v == TNull.Value && c != null; c = c.Next())
                            for (var d = _cx.cursors.First(); v == TNull.Value && d != null; d = d.Next())
                                if (d.value()[c.key()] is TypedValue tv && tv != TNull.Value)
                                    v = tv;
                    if (v == TNull.Value)
                        return TableCursor.New(_cx, this, null);
                    key += v;
                }
            return TableCursor.New(_cx, this, key);
        }
        protected override Cursor? _First(Context cx)
        {
            throw new NotImplementedException();
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException();
        }
        public override Cursor? Last(Context _cx)
        {
            var key = CList<TypedValue>.Empty;
            for (var b = keys.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    TypedValue v = TNull.Value;
                    if (matches[p] is TypedValue t0 && t0 != TNull.Value)
                        v = t0;
                    for (var d = _cx.cursors.First(); v == TNull.Value && d != null; d = d.Next())
                        if (d.value()[p] is TypedValue tv && tv != TNull.Value)
                            v = tv;
                    for (var c = matching[p]?.First(); v == TNull.Value && c != null; c = c.Next())
                        for (var d = _cx.cursors.First(); v == TNull.Value && d != null; d = d.Next())
                            if (d.value()[c.key()] is TypedValue tv && tv != TNull.Value)
                                v = tv;
                    if (v == TNull.Value)
                        return TableCursor.New(_cx, this, null);
                    key += v;
                }
            return TableCursor.New(this, _cx,key);
        }
        /// <summary>
        /// Prepare an Insert on a single table including trigger operation.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ts">The source rowset</param>
        /// <param name="iC">A tree of columns matching some of ts.rowType</param>
        /// <returns>The activations for the changes made</returns>
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, Domain iC)
        {
            return new BTree<long, TargetActivation>(target,
                new TableActivation(cx, this, ts, PTrigger.TrigType.Insert, iC));
        }
        /// <summary>
        /// Execute an Update operation on the Table, including triggers
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="fm">The source rowset</param>
        /// <param name="iter">whether the operation affects multiple rows</param>
        /// <returns>The activations for the changes made</returns>
        internal override BTree<long, TargetActivation> Update(Context cx, RowSet fm)
        {
            return new BTree<long, TargetActivation>(target,
                new TableActivation(cx, this, fm, PTrigger.TrigType.Update));
        }
        /// <summary>
        /// Prepare a Delete on a Table, including triggers
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="fm">The source rowset</param>
        /// <param name="iter">whether the operation affects multiple rows</param>
        /// <returns>the activation for the changes made</returns>
        internal override BTree<long, TargetActivation> Delete(Context cx, RowSet fm)
        {
            return new BTree<long, TargetActivation>(target,
                new TableActivation(cx, this, fm, PTrigger.TrigType.Delete));
        }
        internal override RowSet Sort(Context cx, Domain os, bool dct)
        {
            cx.Add(os);
            if (indexes.Contains(os))
            {
                var ot = (RowSet)cx.Add(this + (Level3.Index.Keys, os) + (RowOrder, os));
                if (!dct)
                    return ot;
                if (FindIndex(cx.db,os) is not null)
                    return ot;
                return (RowSet)cx.Add(new DistinctRowSet(cx, ot));
            }
            return base.Sort(cx, os, dct);
        }

        private Level3.Index? FindIndex(Database db, Domain os,
            PIndex.ConstraintType fl = (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            for (var b = indexes[os]?.First(); b != null; b = b.Next())
                if (db.objects[b.key()] is Level3.Index x && (x.flags&fl)!=0)
                    return x;
            return null;
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Target:"); sb.Append(Uid(target));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (id != null)
            { sb.Append(' '); sb.Append(id); }
            if (alias is not null)
            { sb.Append(" Alias: "); sb.Append(alias); }
            return sb.ToString();
        }
        
        internal class TableCursor : Cursor
        {
            internal readonly Table _table;
            internal readonly TableRowSet _trs;
            internal readonly ABookmark<long, TableRow>? _bmk;
            internal readonly TableRow? _rec;
            internal readonly MTreeBookmark? _mb;
            internal readonly CList<TypedValue> _key;
            protected TableCursor(Context cx, TableRowSet trs, Table tb, int pos, 
                TableRow rec,
                ABookmark<long, TableRow>? bmk, MTreeBookmark? mb = null,CList<TypedValue>? key=null) 
                : base(cx,trs, pos, 
                      new BTree<long,(long,long)>(tb.defpos,(rec.defpos,rec.ppos)),
                      _Row(cx,trs,rec))
            {
                _bmk = bmk; _table = tb; _trs = trs; _mb = mb; 
                _key = key ?? CList<TypedValue>.Empty; 
                _rec = rec;
                cx.cursors += (trs.target, this);
            }
            TableCursor(TableCursor cu,Context cx, long p,TypedValue v) :base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _table = cu._table; _trs = cu._trs; _key = cu._key; _mb = cu._mb;
                _rec = cu._rec;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableCursor(this, cx, p, v);
            }
            static TRow _Row(Context cx,TableRowSet trs, TableRow rw)
            {
         //       var vs = CTree<long, TypedValue>.Empty;
                var ws = CTree<long, TypedValue>.Empty;
                for (var b = trs.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p) 
                        if (trs.iSMap[p] is long sp && rw.vals[sp] is TypedValue v)
                            ws += (p, v);
                        else if (cx.obs[p] is SqlFunction f && f.op == Qlx.POSITION)
                            ws += (p, new TPosition(rw.defpos));
                return new TRow(trs, ws);
            }
            internal static TableCursor? New(Context cx,TableRowSet trs,long defpos)
            {
                var table = cx.db.objects[trs.target] as Table;
                var rec = table?.tableRows[defpos];
                if (table==null || rec == null)
                    return null;
                return new TableCursor(cx, trs, table, 0, rec, null);
            }
            internal static TableCursor? New(Context _cx, TableRowSet trs, CList<TypedValue>? key = null)
            {
                key ??= CList<TypedValue>.Empty;
                if (_cx.role is not Role ro)
                    throw new PEException("PE50320");
                if (_cx.db.objects[trs.target] is not Table table || table.infos[ro.defpos] is null)
                    throw new DBException("42105").Add(Qlx.TABLE);
                if (trs.keys!=Row)
                {
                    var t = (trs.index >= 0 && _cx.db.objects[trs.index] is Level3.Index ix)? ix.rows : null;
                    if (t == null && trs.indexes.Contains(trs.keys))
                        for (var b = trs.indexes[trs.keys]?.First(); b != null; b = b.Next())
                            if (_cx.db.objects[b.key()] is Level3.Index iy)
                            {
                                t = iy.rows;
                                break;
                            }
                    if (t is not null)
                        for (var bmk = t.PositionAt(key, 0); bmk != null; bmk = bmk.Next())
                        {
                            var iq = bmk.Value();
                            if (iq == null || table.tableRows[iq.Value] is not TableRow rec)
                                continue;
                            //#if MANDATORYACCESSCONTROL
                            if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                                && (_cx.db.user == null || (_cx.db.user.defpos != table.definer
                                && _cx.db.user.defpos != _cx.db.owner
                                && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))))
                                continue;
                            //#endif
                            var rb = new TableCursor(_cx, trs, table, 0, rec, null, bmk, key);
                            if (rb.Matches(_cx))
                            {
                                table._ReadConstraint(_cx, rb);
                                var tt = _cx.rdS[table.defpos] ?? CTree<long, bool>.Empty;
                                _cx.rdS += (table.defpos, tt + (rec.defpos, true));
                                return rb;
                            }
                        }
                }
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
//#if MANDATORYACCESSCONTROL
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        (_cx.db.user == null || (_cx.db.user.defpos != table.definer
                         && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))))
                        continue;
//#endif
                    var rb = new TableCursor(_cx, trs, table, 0, rec, b);
                    if (rb.Matches(_cx))
                    {
                        table._ReadConstraint(_cx, rb);
                        var tt = _cx.rdS[table.defpos] ?? CTree<long, bool>.Empty;
                        _cx.rdS += (table.defpos, tt + (rec.defpos, true));
                        return rb;
                    }
                }
                return null;
            }
            internal static TableCursor? New(TableRowSet trs, Context _cx, CList<TypedValue>? key = null)
            {
                key ??= CList<TypedValue>.Empty;
                if (_cx.db.objects[trs.target] is not Table table)
                    throw new PEException("PE48175");
                if (trs.keys != Domain.Row && trs.tree != null)
                {
                    var t = trs.tree;
                    for (var bmk = t.PositionAt(key, 0); bmk != null; bmk = bmk.Previous())
                        if (bmk.Value() is long q && table.tableRows[q] is TableRow rec)
                        {
//#if MANDATORYACCESSCONTROL
                        if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                            && (_cx.db.user==null || (_cx.db.user.defpos != table.definer
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))))
                            continue;
//#endif
                            var rb = new TableCursor(_cx, trs, table, 0, rec, null, bmk, key);
                            if (rb.Matches(_cx))
                            {
                                table._ReadConstraint(_cx, rb);
                                return rb;
                            }
                        }
                }
                for (var b = table.tableRows.Last(); b != null; b = b.Previous())
                {
                    var rec = b.value();
//#if MANDATORYACCESSCONTROL
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
//#endif
                    var rb = new TableCursor(_cx, trs, table, 0, rec, b);
                    if (rb.Matches(_cx))
                    {
                        table._ReadConstraint(_cx, rb);
                        return rb;
                    }
                }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                var bmk = _bmk;
                var mb = _mb;
                var table = _table;
                if (mb != null)
                    for (; ; )
                    {
                        if (mb != null)
                            mb = mb.Next();
                        if (mb == null)
                            return null;
                        if (mb.Value() == null)
                            continue;
                        var rec = _table.tableRows[mb.Value()??-1L];
                        if (rec == null)
                            continue;
//#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) 
                            && (_cx.db.user==null || (_cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))))
                            continue;
//#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, null, mb,
                            rec.MakeKey(_trs.keys.rowType));
                        if (rb.Matches(_cx))
                        {
                            _table._ReadConstraint(_cx, rb);
                            return rb;
                        }
                    }
                else
                    for (; ; )
                    {
                        if (bmk != null)
                            bmk = bmk.Next();
                        if (bmk == null)
                            return null;
                        var rec = bmk.value();
//#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) 
                            && (_cx.db.user==null || (_cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))))
                            continue;
//#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, bmk);
                        if (rb.Matches(_cx))
                        {
                            _table._ReadConstraint(_cx, rb);
                            return rb;
                        }
                    }
            }
            protected override Cursor? _Previous(Context _cx)
            {
                var bmk = _bmk;
                var mb = _mb;
                var table = _table;
                if (mb != null)
                    for (; ; )
                    {
                        if (mb != null)
                            mb = mb.Previous();
                        if (mb == null)
                            return null;
                        if (mb.Value() == null)
                            continue;
                        var rec = _table.tableRows[mb.Value()??-1L];
                        if (rec == null)
                            continue;
//#if MANDATORYACCESSCOLNTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) 
                            && (_cx.db.user == null || (_cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner 
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))))
                            continue;
//#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, null, mb,
                            rec.MakeKey(_trs.keys.rowType));
                        if (rb.Matches(_cx))
                        {
                            _table._ReadConstraint(_cx, rb);
                            return rb;
                        }
                    }
                else
                    for (; ; )
                    {
                        if (bmk != null)
                            bmk = bmk.Previous();
                        if (bmk == null)
                            return null;
                        var rec = bmk.value();
//#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) 
                            && (_cx.db.user == null || (_cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner 
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))))
                            continue;
//#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, bmk);
                        if (rb.Matches(_cx))
                        {
                            _table._ReadConstraint(_cx, rb);
                            return rb;
                        }
                    }
            }
            internal override BList<TableRow> Rec()
            { 
                return (_rec is not null)?new BList<TableRow>(_rec):BList<TableRow>.Empty;
            }
            public override MTreeBookmark? Mb()
            {
                return _mb;
            }
        }
    }
    /// <summary>
    /// A rowset for distinct values
    /// 
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        internal override Assertions Requires => Assertions.MatchesTarget;
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context cx,RowSet r) 
            : base(cx.GetUid(),cx,_Mem(cx,r)+(_Source,r.defpos)
                  +(Table.LastData,r.lastData)+(_Where,r.where)
                  +(_Matches,r.matches)
                  +(ObInfo.Names, r.names))
        {
            cx.Add(this);
        }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, RowSet r)
        {
            var bs = BList<DBObject>.Empty;
            for (var b = r.rowType.First(); b != null && b.key() < r.display; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject tp)
                    bs += tp;
            var dm = new Domain(-1L,cx,Qlx.ROW,bs,bs.Length);
            return dm.mem + (Level3.Index.Keys, dm);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new DistinctRowSet(defpos,m);
        }
        public static DistinctRowSet operator +(DistinctRowSet et, (long, object) x)
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
            return (DistinctRowSet)et.New(m + x);
        }
        public static DistinctRowSet operator +(DistinctRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (DistinctRowSet)rs.New(m + (p, o));
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new DistinctRowSet(dp,m);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var sce = (RowSet?)cx.obs[source]??throw new PEException("PE1801");
            var mt = new MTree(keys,TreeBehaviour.Ignore,0);
            for (var a = sce.First(cx); a != null; a = a.Next(cx))
                mt += (a.ToKey(), 0, 0);
            return (RowSet)cx.Add(this+(_Built,true)+(Level3.Index.Tree,mt)+(Level3.Index.Keys,keys));
        }

        protected override Cursor? _First(Context cx)
        {
            return DistinctCursor.New(cx,this);
        }
        protected override Cursor? _Last(Context cx)
        {
            return DistinctCursor.New(this, cx);
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, Domain rt)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation>Delete(Context cx,RowSet fm)
        {
            throw new DBException("42174");
        }
        
        internal class DistinctCursor : Cursor
        {
            readonly DistinctRowSet _drs;
            readonly MTreeBookmark? _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,E,new TRow(drs, bmk.key()))
            {
                _bmk = bmk;
                _drs = drs;
            }
            internal static DistinctCursor? New(Context cx,DistinctRowSet drs)
            {
                if (drs.tree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.tree.First(); bmk != null; bmk = bmk.Next()) 
                { 
                    var rb = new DistinctCursor(cx,drs,0, bmk);
                    if (rb.Matches(cx) && Eval(drs.where, cx))
                        return rb;
                }
                return null;
            }
            internal static DistinctCursor? New(DistinctRowSet drs,Context cx)
            {
                if (drs.tree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.tree.Last(); bmk != null; bmk = bmk.Previous())
                {
                    var rb = new DistinctCursor(cx, drs, 0, bmk);
                    if (rb.Matches(cx) && Eval(drs.where, cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var bmk = _bmk?.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctCursor(cx, _drs,_pos + 1, bmk);
                    if (rb.Matches(cx) && Eval(_drs.where, cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                for (var bmk = _bmk?.Previous(); bmk != null; bmk = bmk.Previous())
                {
                    var rb = new DistinctCursor(cx, _drs, _pos + 1, bmk);
                    if (rb.Matches(cx) && Eval(_drs.where, cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    internal class OrderedRowSet : RowSet
    {
        internal const long
            _RTree = -412; // RTree
        internal RTree? rtree => (RTree?)mem[_RTree];
        internal Domain order => (Domain)(mem[Level3.Index.Keys] ?? Domain.Row);
        internal override Assertions Requires => Assertions.MatchesTarget;
        internal OrderedRowSet(Context cx, RowSet r, Domain os, bool dct)
            : this(cx.GetUid(), cx, r, os, dct)
        { }
        internal OrderedRowSet(long dp, Context cx, RowSet r, Domain os, bool dct)
            : base(dp, cx, cx.DoDepth(r.mem -Aggs + (_Source, r.defpos)
                 + (RSTargets, r.rsTargets) + (RowOrder, os) + (Level3.Index.Keys, os)
                 + (Table.LastData, r.lastData) + (ObInfo.Name, r.name??"")
                 + (ISMap, r.iSMap) + (SIMap,r.sIMap)
                 + (ObInfo.Names,r.names) + (Distinct, dct)))
        {
            cx.Add(this);
        }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static OrderedRowSet operator +(OrderedRowSet et, (long, object) x)
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
            return (OrderedRowSet)et.New(m + x);
        }
        public static OrderedRowSet operator +(OrderedRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (OrderedRowSet)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        internal override RowSet Apply(BTree<long,object> mm, Context cx, BTree<long,object>?m=null)
        {
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            var ms = (CTree<long,TypedValue>?)mm[_Matches]??CTree<long, TypedValue>.Empty;
            for (var b = keys.First(); b != null; b = b.Next())
                if (b.value() is long p && ms.Contains(p) && cx.obs[source] is RowSet sc)
                    return sc.Apply(mm, cx); // remove this from the pipeline
            return base.Apply(mm, cx);
        }
        internal override bool CanSkip(Context cx)
        {
            var sc = (RowSet?)cx.obs[source];
            var match = true; // if true, all of the ordering columns are constant
            for (var b = rowOrder.First(); match && b != null; b = b.Next())
                if (b.value() is long p && !matches.Contains(p))
                    match = false;
            return match || (sc is InstanceRowSet && rowOrder.CompareTo(sc.rowOrder) == 0);
        }
        internal override RowSet Sort( Context cx, Domain os, bool dct)
        {
            if (os.CompareTo(rowOrder)==0) // skip if same
                return this;
            return (RowSet)cx.Add((RowSet)New(mem + (OrdSpec, os)) + (RowOrder, os));
        }
        internal override DBObject New(long dp, BTree<long,object>m)
        {
            return new OrderedRowSet(dp, m);
        }
        protected override bool CanAssign()
        {
            return false;
        }
        internal override bool Built(Context cx)
        {
            return  mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var sce = (RowSet?)cx.obs[source] ?? throw new DBException("42105").Add(Qlx.TABLE);
            var tree = new RTree(sce.defpos, cx, keys,
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = sce.First(cx); e != null; e = e.Next(cx))
            {
                var vs = CTree<long, TypedValue>.Empty;
                cx.cursors += (sce.defpos, e);
                for (var b = rowOrder.First(); b != null; b = b.Next())
                if (cx.obs[b.value()??-1L] is QlValue s)
                    vs += (s.defpos, s.Eval(cx));
                var rw = new TRow(keys, vs);
                tree +=(rw, SourceCursors(cx));
            }
            return (RowSet)cx.Add((this + (_Built, true) + (Level3.Index.Tree, tree.mt) + (_RTree, tree)));
        }
        protected override Cursor? _First(Context cx)
        {
            if (rtree == null || rtree.rows.Count == 0L)
                return null;
            return OrderedCursor.New(cx, this, RTreeBookmark.New(cx, rtree));
        }
        protected override Cursor? _Last(Context cx)
        {
            if (rtree == null || rtree.rows.Count == 0L)
                return null;
            return OrderedCursor.New(cx, this, RTreeBookmark.New(rtree, cx));
        }
        
        internal class OrderedCursor : Cursor
        {
            internal readonly OrderedRowSet _ors;
            internal RTreeBookmark? _rb;
            internal OrderedCursor(Context cx,OrderedRowSet ors,RTreeBookmark rb,BTree<long,Cursor> cu)
                :base(cx,ors,rb._pos,rb._ds,rb)
            {
                _ors = ors; _rb = rb;
                cx.cursors += cu;
            }
            public override MTreeBookmark? Mb()
            {
                return _rb?.Mb();
            }
            internal static OrderedCursor? New(Context cx,OrderedRowSet ors,
                RTreeBookmark? rb)
            {
                if (rb == null || ors.rtree?.rows[rb._pos] is not BTree<long,Cursor> cu)
                    return null;
                return new OrderedCursor(cx, ors, rb, cu);
            }
            protected override Cursor? _Next(Context _cx)
            {
                return New(_cx, _ors, (RTreeBookmark?)_rb?.Next(_cx));
            }
            protected override Cursor? _Previous(Context _cx)
            {
                return New(_cx, _ors, (RTreeBookmark?)_rb?.Previous(_cx));
            }
            internal override BList<TableRow>? Rec()
            {
                return _rb?.Rec();
            }
        }
    }
    
    internal class EmptyRowSet : RowSet
    {
        internal EmptyRowSet(long dp, Context cx,Domain dm,BList<long?>?us=null,
            CTree<long,Domain>?re=null) 
            : base(dp, _Mem(cx,dm,us,re)) 
        {
            cx.Add(this);
        }
        protected EmptyRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,Domain dm,BList<long?>? us,
            CTree<long,Domain>? re)
        {
            re ??= CTree<long, Domain>.Empty;
            if (us != null && !Context.Match(us,dm.rowType))
                dm = (Domain)dm.Relocate(cx.GetUid()) + (RowType, us)
                    +(Representation,dm.representation+re);
            return dm.mem;
        }
        public static EmptyRowSet operator +(EmptyRowSet et, (long, object) x)
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
            return (EmptyRowSet)et.New(m + x);
        }
        public static EmptyRowSet operator +(EmptyRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (EmptyRowSet)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EmptyRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new EmptyRowSet(dp,m);
        }
        protected override Cursor? _First(Context _cx)
        {
            return null;
        }
        protected override Cursor? _Last(Context _cx)
        {
            return null;
        }
        internal override int Cardinality(Context cx)
        {
            return 0;
        }
    }
    /// <summary>
    /// A RowSet for UNNEST of an array
    /// </summary>
    internal class ArrayRowSet : RowSet
    {
        public ArrayRowSet(long dp, Context cx, QlValue sv) :
            base(dp, new BTree<long,object>(SqlLiteral._Val, sv.Eval(cx)))
        {
            cx.Add(this);
        }
        protected ArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ArrayRowSet operator +(ArrayRowSet et, (long, object) x)
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
            return (ArrayRowSet)et.New(m + x);
        }
        public static ArrayRowSet operator +(ArrayRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (ArrayRowSet)rs.New(m + (p, o));
        }
        protected override Cursor? _First(Context cx)
        {
            var a = (TList?)mem[SqlLiteral._Val];
            if (a == null) return null;
            return ArrayCursor.New(cx, this, a, 0);
        }
        protected override Cursor? _Last(Context cx)
        {
            var a = (TList?)mem[SqlLiteral._Val];
            if (a==null) return null;
            return ArrayCursor.New(cx,this,a,a.Length-1);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ArrayRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new ArrayRowSet(dp,m);
        }
         internal class ArrayCursor : Cursor
        {
            internal readonly ArrayRowSet _ars;
            internal readonly TList _ar;
            internal readonly int _ix;
            ArrayCursor(Context cx,ArrayRowSet ars,TList ar, int ix)
                : base(cx,ars,ix,E,(TRow)ar[ix])
            {
                _ars = ars; _ix = ix;  _ar = ar;
            }
            internal static ArrayCursor? New(Context cx, ArrayRowSet ars, TList ar, int ix)
            {
                if (ix<0 || ar.Length <= ix)
                    return null;
                return new ArrayCursor(cx, ars, ar, ix);
            }
            protected override Cursor? _Next(Context cx)
            {
                return New(cx,_ars,_ar,_ix+1);
            }

            protected override Cursor? _Previous(Context cx)
            {
                return New(cx, _ars, _ar, _ix - 1);
            }

            internal override BList<TableRow> Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    /// <summary>
    /// A RowSet for UNNEST of a Set
    /// </summary>
    internal class SetRowSet : RowSet
    {
        public SetRowSet(long dp, Context cx, QlValue sv) :
            base(dp, new BTree<long, object>(SqlLiteral._Val, sv.Eval(cx)))
        {
            cx.Add(this);
        }
        protected SetRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SetRowSet operator +(SetRowSet et, (long, object) x)
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
            return (SetRowSet)et.New(m + x);
        }
        public static SetRowSet operator +(SetRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (SetRowSet)rs.New(m + (p, o));
        }

        protected override Cursor? _First(Context cx)
        {
            var a = (TSet?)mem[SqlLiteral._Val];
            return (a == null) ? null : SetCursor.New(cx, this, a, a?.First());
        }
        protected override Cursor? _Last(Context cx)
        {
            var a = (TSet?)mem[SqlLiteral._Val];
            return (a == null) ? null : SetCursor.New(cx, this, a, a?.Last());
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SetRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SetRowSet(dp, m);
        }
        internal class SetCursor : Cursor
        {
            internal readonly SetRowSet _mrs;
            internal readonly TSet _ms;
            internal readonly TSet.SetBookmark _mb;
            SetCursor(Context cx, SetRowSet mrs, TSet ms, TSet.SetBookmark mb)
                : base(cx, mrs, (int)mb.Position(), E, (TRow)mb.Value())
            {
                _mrs = mrs; _ms = ms; _mb = mb;
            }
            internal static SetCursor? New(Context cx, SetRowSet mrs, TSet ms,
                TSet.SetBookmark? mb)
            {
                if (mb == null)
                    return null;
                return new SetCursor(cx, mrs, ms, mb);
            }
            protected override Cursor? _Next(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Next());
            }

            protected override Cursor? _Previous(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Previous());
            }

            internal override BList<TableRow> Rec()
            {
                throw new NotImplementedException();
            }

        }
    }
    /// <summary>
    /// A RowSet for UNNEST of a Multiset
    /// </summary>
    internal class MultisetRowSet : RowSet
    {
        public MultisetRowSet(long dp, Context cx, QlValue sv) :
            base(dp, new BTree<long,object>(SqlLiteral._Val, sv.Eval(cx)))
        {
            cx.Add(this);
        }
        protected MultisetRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static MultisetRowSet operator +(MultisetRowSet et, (long, object) x)
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
            return (MultisetRowSet)et.New(m + x);
        }
        public static MultisetRowSet operator +(MultisetRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (MultisetRowSet)rs.New(m + (p, o));
        }
        protected override Cursor? _First(Context cx)
        {
            var a = (TMultiset?)mem[SqlLiteral._Val];
            return (a==null)?null:MultisetCursor.New(cx, this, a, a?.First());
        }
        protected override Cursor? _Last(Context cx)
        {
            var a = (TMultiset?)mem[SqlLiteral._Val];
            return (a==null)?null:MultisetCursor.New(cx, this, a, a?.Last());
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MultisetRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new MultisetRowSet(dp,m);
        }
        internal class MultisetCursor : Cursor
        {
            internal readonly MultisetRowSet _mrs;
            internal readonly TMultiset _ms;
            internal readonly TMultiset.MultisetBookmark _mb;
            MultisetCursor(Context cx,MultisetRowSet mrs,TMultiset ms,TMultiset.MultisetBookmark mb)
                : base(cx,mrs,(int)mb.Position(),E,(TRow)mb.Value())
            {
                _mrs = mrs; _ms = ms; _mb = mb;
            }
            internal static MultisetCursor? New(Context cx,MultisetRowSet mrs,TMultiset ms,
                TMultiset.MultisetBookmark? mb)
            {
                if (mb == null)
                    return null;
                return new MultisetCursor(cx, mrs, ms, mb);
            }
            protected override Cursor? _Next(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Next());
            }

            protected override Cursor? _Previous(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Previous());
            }

            internal override BList<TableRow> Rec()
            {
                throw new NotImplementedException();
            }

        }
    }
    /// <summary>
    /// A rowset for SqlRows
    /// 
    /// </summary>
    internal class SqlRowSet : RowSet
    {
        internal const long
            SqlRows = -413; // BList<long?>  SqlRow
        internal BList<long?> sqlRows =>
            (BList<long?>?)mem[SqlRows]??BList<long?>.Empty;
        internal SqlRowSet(long dp, Context cx, Domain xp, BList<long?> rs)
            : base(dp, cx, _Mem(cx, xp) + (SqlRows, rs))
        {
            cx.Add(this);
        }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, Domain dm)
        {
            var ns = BTree<string, (int,long?)>.Empty;
            var i = 0;
            for (var b = dm.Needs(cx).First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                    ns += (n, (i++, b.key()));
            return dm.mem + (ObInfo.Names, ns) + (Asserts, Assertions.AssignTarget);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowSet(defpos,m);
        }
        public static SqlRowSet operator +(SqlRowSet et, (long, object) x)
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
            return (SqlRowSet)et.New(m + x);
        }
        public static SqlRowSet operator +(SqlRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (SqlRowSet)rs.New(m + (p, o));
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlRowSet(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRowSet)base._Replace(cx, so, sv);
            r +=(cx, SqlRows, cx.ReplacedLl(sqlRows));
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var nw = cx.FixLl(sqlRows);
            if (nw != sqlRows)
                r = cx.Add(r, SqlRows, nw);
            return r;
        }
        protected override Cursor? _First(Context _cx)
        {
            return SqlCursor.New(_cx,this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return SqlCursor.New(this, _cx);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" SqlRows [");
            var cm = "";
            for (var b = sqlRows.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(p));
                }
            sb.Append(']');
        }
        
        internal class SqlCursor : Cursor
        {
            readonly SqlRowSet _srs;
            readonly ABookmark<int, long?> _bmk;
            SqlCursor(Context cx,SqlRowSet rs,int pos,ABookmark<int,long?> bmk)
                : base(cx,rs,pos,E,_Row(cx,rs,bmk.key()))
            {
                _srs = rs;
                _bmk = bmk;
            }
            SqlCursor(SqlCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _srs = cu._srs;
                _bmk = cu._bmk;
            }
            static TRow _Row(Context cx, SqlRowSet rs, int p)
            {
                var vs = CTree<long, TypedValue>.Empty;
                var rv = (SqlRow?)cx.obs[rs.sqlRows[p] ?? -1L];
                var n = rv?.domain.Length;
                if (n < rs.Length)
                    throw new DBException("22109");
                int j = 0;
                for (var b = rs.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long tp && rs.representation[tp] is Domain td && rv is not null
                            && rv.domain.rowType[j++] is long dp && cx.obs[dp] is QlValue dd)
                        vs += (tp, dd.Eval(cx));
                return new TRow(rs,vs);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor? New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.sqlRows.First();b is not null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal static SqlCursor? New(SqlRowSet rs, Context _cx)
            {
                for (var b = rs.sqlRows.Last(); b != null; b = b.Previous())
                {
                    var rb = new SqlCursor(_cx, rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var b=_bmk.Next();b is not null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,_srs, _pos+1,b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var b = _bmk.Previous(); b != null; b = b.Previous())
                {
                    var rb = new SqlCursor(_cx, _srs, _pos + 1, b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow>? Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    /// <summary>
    /// a row set for TRows.
    /// Each TRow has the same domain as the ExplicitRowSet.
    /// 
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        internal const long
            ExplRows = -414; // BList<(long,TRow>)> QlValue
        internal BList<(long, TRow)> explRows =>
            (BList<(long, TRow)>?)mem[ExplRows] ?? BList<(long, TRow)>.Empty;
        internal long index => (long)(mem[TableRowSet._Index] ?? -1L);
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,Domain dt,BList<(long,TRow)>r)
            : base(dp, _Mem(cx,dt,r))
        {
            cx.Add(this);
        }
        protected ExplicitRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,Domain dt,BList<(long,TRow)>r)
        {
            var m = dt.mem + (ExplRows, r);
            /* special case: if dt has GDefs, ignore field columns
            if (m[MatchStatement.GDefs] is CTree<long, TGParam> gs)
                for (var b = gs.First(); b != null; b = b.Next())
                    if (b.value() is TGParam g && g.type.HasFlag(TGParam.Type.Field))
                        dt -= g.uid; */
            if (r.Length == 0)
            {
                var t = new MTree(dt, TreeBehaviour.Ignore, 0);
                var x = new Level3.Index(cx.GetUid(), 
                    new BTree<long, object>(Level3.Index.IndexConstraint, PIndex.ConstraintType.NoType)
                        + (Level3.Index.Keys, dt) + (Level3.Index.Tree,t));
                cx.Add(x);
                m += (TableRowSet._Index, x.defpos);
            }
            return m;
        }
        public static ExplicitRowSet operator +(ExplicitRowSet et, (long, object) x)
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
            return (ExplicitRowSet)et.New(m + x);
        }
        public static ExplicitRowSet operator +(ExplicitRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (ExplicitRowSet)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExplicitRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ExplicitRowSet(dp, m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ExplicitRowSet)base._Replace(cx, so, sv);
            var s = BList<(long,TRow)>.Empty;
            for (var b=explRows.First();b is not null;b=b.Next())
            {
                var (p, q) = b.value();
                s += (p, (TRow)q.Replace(cx,so,sv));
            }
            r +=(cx, ExplRows, s);
            return r;
        }
        protected override Cursor? _First(Context _cx)
        {
            return ExplicitCursor.New(_cx,this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return ExplicitCursor.New(this, _cx);
        }
        internal override int Cardinality(Context cx)
        {
            return (where == CTree<long, bool>.Empty) ?
                (int)explRows.Count : base.Cardinality(cx);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (rows.Count<10)
            {
                var cm = "";
                sb.Append('[');
                for (var b=explRows.First();b is not null;b=b.Next())
                {
                    var (p, rw) = b.value();
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                    sb.Append(": ");sb.Append(rw);
                }
                sb.Append(']');
            }
        }
        
        internal class ExplicitCursor : Cursor
        {
            protected readonly ExplicitRowSet _ers;
            protected readonly ABookmark<int,(long,TRow)> _prb;
            protected ExplicitCursor(Context cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(cx,ers,pos,E, prb.value().Item2)
            {
                _ers = ers;
                _prb = prb;
                cx.values += values;
            }
            protected ExplicitCursor(ExplicitCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _ers = cu._ers;
                _prb = cu._prb;
                cx.values += (p, v);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ExplicitCursor(this, cx, p, v);
            }
            internal static ExplicitCursor? New(Context _cx,ExplicitRowSet ers)
            {
                for (var b=ers.explRows.First();b is not null;b=b.Next())
                {
                    var rb = new ExplicitCursor(_cx,ers, b, 0);
                    if (rb.Matches(_cx) && Eval(ers.where, _cx))
                        return rb;
                }
                return null;
            }
            internal static ExplicitCursor? New(ExplicitRowSet ers, Context _cx)
            {
                for (var b = ers.explRows.Last(); b != null; b = b.Previous())
                {
                    var rb = new ExplicitCursor(_cx, ers, b, 0);
                    if (rb.Matches(_cx) && Eval(ers.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var prb = _prb.Next(); prb is not null; prb=prb.Next())
                {
                    var rb = new ExplicitCursor(_cx,_ers, prb, _pos+1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var prb = _prb.Previous(); prb != null; prb = prb.Previous())
                {
                    var rb = new ExplicitCursor(_cx, _ers, prb, _pos + 1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    internal class TrueRowSet : ExplicitRowSet
    {
        internal const long
            TrueResult = -437; // TrueRowSet
        TrueRowSet(Context cx) : base(cx.GetUid(), BTree<long, object>.Empty
            + (_Domain, new Domain(cx.GetUid(), cx, Qlx.TABLE,
                new BList<DBObject>(new SqlLiteral(--_uid, TBool.True)))))
        {
            Context._system.obs += (TrueResult, this);
        }
        internal static TrueRowSet OK(Context cx) => 
            (TrueRowSet)(cx.obs[TrueResult] ?? new TrueRowSet(cx));
    }
    internal class ProcRowSet : RowSet
    {
        internal long call => (long)(mem[CallStatement.Call]??-1L);
        internal ProcRowSet(SqlCall ca, Context cx) 
            :base(cx.GetUid(),cx,
                 _Mem(cx,cx.db.objects[ca.procdefpos] as Procedure??throw new DBException("42000","ProcRowSet"))
                 +(CallStatement.Call,ca.defpos))
        {
            cx.Add(this);
        }
        internal ProcRowSet(Context cx,Procedure pr)
            : base(cx.GetUid(), cx,  _Mem(cx, pr))
        {
            cx.Add(this);
        }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, Procedure pr)
        {
            var dp = cx.GetPrevUid();
            if (pr.domain.Length == 0 || pr.NameFor(cx) is not string n)
                throw new DBException("42000","Void?");
            var tn = new Ident(n, cx.Ix(dp));
            var rt = BList<long?>.Empty;        // our total row type
            var rs = CTree<long, Domain>.Empty; // our total row type representation
            var sr = BList<long?>.Empty;        // our total SRow type
            var tr = BTree<long, long?>.Empty;  // mapping to physical columns (ignored)
            var ns = BTree<string, (int, long?)>.Empty; // column names
            (rt, rs, sr, _, ns) = pr.domain.ColsFrom(cx, dp, rt, rs, sr, tr, ns);
            var r = BTree<long, object>.Empty
                + (RowType, rt) + (Representation, rs) + (Display, rt.Length)
                + (InstanceRowSet.SRowType, sr)
                + (QlValue.SelectDepth, cx.sD)
                + (Target, pr.defpos) + (ObInfo.Names, ns) + (ObInfo.Name, tn.ident)
                + (_Ident, tn) + (RSTargets, new BTree<long, long?>(pr.defpos, dp));
            r = cx.DoDepth(r);
            r += (Asserts, (Assertions)(_Mem(dp, cx, r)[Asserts] ?? Assertions.None));
            return r;
        }
        public static ProcRowSet operator +(ProcRowSet et, (long, object) x)
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
            return (ProcRowSet)et.New(m + x);
        }
        public static ProcRowSet operator +(ProcRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (ProcRowSet)rs.New(m + (p, o));
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var fc = (SqlProcedureCall)(cx._Ob(call) ?? throw new DBException("42105").Add(Qlx.PROCEDURE));
            cx.values += (defpos,fc.Eval(cx));
            return (RowSet)cx.Add(this+(_Built,true));
        }
        protected override Cursor? _First(Context cx)
        {
            var v = cx.values[defpos];
            return (v==null || v==TNull.Value)?null:ProcRowSetCursor.New(cx, this, (TList)v);
        }
        protected override Cursor? _Last(Context cx)
        {
            var v = cx.values[defpos];
            return (v==null || v == TNull.Value) ? null : ProcRowSetCursor.New(this, cx, (TList)v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ProcRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ProcRowSet(dp, mem);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ProcRowSet)base._Replace(cx, so, sv);
            r +=(cx, CallStatement.Call, cx.done[call]?.defpos??call);
            return r;
        }
        internal override CTree<long, bool> _ProcSources(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Call: "); sb.Append(Uid(call));
        }
        
        internal class ProcRowSetCursor : Cursor
        {
            readonly ProcRowSet _prs;
            readonly ABookmark<int, TypedValue> _bmk;
            ProcRowSetCursor(Context cx, ProcRowSet prs, int pos,
                ABookmark<int, TypedValue> bmk, TRow rw)
                : base(cx, prs, pos, E, new TRow(rw, prs))
            { 
                _prs = prs; _bmk = bmk;
                cx.values += values;
            }
            internal static ProcRowSetCursor? New(Context cx,ProcRowSet prs,TList ta)
            {
                for (var b = ta.list.First();b is not null;b=b.Next())
                {
                    var r = new ProcRowSetCursor(cx, prs, 0, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            internal static ProcRowSetCursor? New(ProcRowSet prs, Context cx, TList ta)
            {
                for (var b = ta.list.Last(); b != null; b = b.Previous())
                {
                    var r = new ProcRowSetCursor(cx, prs, 0, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ProcRowSetCursor(cx, _prs, _pos, _bmk,
                    this + (p, v));
            }

            protected override Cursor? _Next(Context cx)
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var r = new ProcRowSetCursor(cx, _prs, _pos+1, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                for (var b = _bmk.Previous(); b != null; b = b.Previous())
                {
                    var r = new ProcRowSetCursor(cx, _prs, _pos + 1, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    /// <summary>
    /// Deal with CRUD operations on Tables, including triggers and cascades.
    /// 
    /// For triggers, the main complication is that triggers execute with 
    /// their definer's role (many different roles for the same table!). 
    /// Each trigger therefore has its own context and its own (Target)Cursor 
    /// for the TRS. Its target row type is used for the OLD/NEW ROW/TABLE.
    ///
    /// The rowtype for the TRS itself is the rowType of the source rowset,
    /// which for INSERT comes from the values to be inserted, and for others
    /// from the rowset's version of the destination table.
    /// Accordingly, TableActivations use TableColumn uids for
    /// values, while QlValue uids are used in other sorts of Activation.
    /// 
    /// The defpos of the TransitionRowSet gives 
    /// - the TargetCursor in the TableActivation ta (for the table owner)
    ///     (ta.next is the enclosing query processing context)
    /// - a TransitionCursor in the TriggerActivation tga (for the trigger definer)
    ///     (but tga.next is ta)
    /// - the TransitionCursor in other contexts (for the parser role), whose
    /// _tgc and _tgc.rec fields show the current table column values
    /// 
    /// All of these activations and cursors share the immutable _trs, and
    /// for a tablecolumn c and corresponding sqlvalue s, 
    /// s=_trs.targetTrans[c] and c=_trs.transTarget[s].
    /// 
    /// On creation of Table/TriggerActivations cursor values are placed for uids.
    /// On exit from Target/Table/TriggerActivations, values are gathered back into
    /// the TransitionCursors and TargetCursors.
    /// 
    /// 
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal const long
            _Adapters = -429, // Adapters
            Defaults = -415, // CTree<long,TypedValue>  QlValue
            ForeignKeys = -392, // CTree<long,bool>  Index
            IxDefPos = -420, // long    Index
            TargetTrans = -418, // BTree<long,long?>   TableColumn,QlValue
            TransTarget = -419; // BTree<long,long?>   QlValue,TableColumn
        internal CTree<long, TypedValue> defaults =>
            (CTree<long, TypedValue>?)mem[Defaults] ?? CTree<long, TypedValue>.Empty;
        internal BTree<long, long?> targetTrans =>
            (BTree<long, long?>?)mem[TargetTrans]??BTree<long,long?>.Empty;
        internal BTree<long, long?> transTarget =>
            (BTree<long, long?>?)mem[TransTarget]??BTree<long,long?>.Empty;
        internal CTree<long, bool> foreignKeys =>
            (CTree<long, bool>?)mem[ForeignKeys] ?? CTree<long, bool>.Empty;
        internal TransitionRowSet(TargetActivation cx, TableRowSet ts, RowSet data)
            : base(cx.GetUid(), cx,_Mem(cx, ts, data)+(_Where,ts.where)+(_Data,data.defpos)
                  +(Matching,ts.matching))
        {
            cx.Add(this);
        }
        static BTree<long, object> _Mem(TargetActivation cx,
            TableRowSet ts, RowSet data)
        {
            var m = BTree<long, object>.Empty;
            var tr = cx.db;
            var ta = ts.target;
            var t = tr.objects[ta] as Table;
            if (t == null || t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105").Add(Qlx.INSERT, new TChar(t?.infos[cx.role.defpos]?.name ?? "??"));
            if (cx._Ob(ta) is not Domain da)
                throw new DBException("42105").Add(Qlx.TABLE);
            m += ts.mem;
            m += (_Data, data.defpos);
            m += (Target, ta);
            m += (ISMap, ts.iSMap);
            m += (RSTargets, new BTree<long, long?>(ts.target, ts.defpos));
            m += (IxDefPos, t.FindPrimaryIndex(cx)?.defpos ?? -1L);
            // check now about conflict with generated columns
            var dfs = BTree<long, TypedValue>.Empty;
            if (cx._tty != PTrigger.TrigType.Delete)
                for (var b = da.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && tr.objects[p] is TableColumn tc // i.e. not remote
                     && tc.domain.defaultValue is TypedValue tv  && tv != TNull.Value)
                    {
                        dfs += (tc.defpos, tv);
                        cx.values += (tc.defpos, tv);
                    }
            var fk = t.AllForeignKeys(cx);
            for (var b = t.super.First(); b != null; b = b.Next())
                if (b.key() is Table tt)
                    fk += tt.AllForeignKeys(cx);
            m += (TargetTrans, ts.sIMap);
            m += (TransTarget, ts.iSMap);
            m += (ForeignKeys, fk);
            m += (Defaults, dfs);
            return m;
        }
        protected TransitionRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionRowSet(defpos, m);
        }
        public static TransitionRowSet operator +(TransitionRowSet et, (long, object) x)
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
            return (TransitionRowSet)et.New(m + x);
        }
        public static TransitionRowSet operator +(TransitionRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (TransitionRowSet)rs.New(m + (p, o));
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new TransitionRowSet(dp, m);
        }
        protected override Cursor? _First(Context _cx)
        {
            return TransitionCursor.New((TableActivation)_cx, this);
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException(); // never
        }
        /// <summary>
        /// Set up a transitioncursor on TableRow p
        /// </summary>
        /// <param name="cx">A TableActivation</param>
        /// <param name="dp">TableRow defpos in this table</param>
        /// <param name="u">Pending updates to this row</param>
        /// <returns></returns>
        internal Cursor? At(TableActivation cx,long dp,CTree<long,TypedValue>? u)
        {
            if (u is not null)
                cx.pending += (dp, u);
            for (var b = (TransitionCursor?)First(cx); b != null;
                b = (TransitionCursor?)b.Next(cx))
                if (b._ds[cx._trs.target].Item1 == dp)
                    return b;
            return null;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TransitionRowSet)base._Replace(cx, so, sv);
            var ds = cx.ReplaceTlT(defaults, so, sv);
            if (ds != defaults)
                r +=(cx, Defaults, ds);
            var fk = cx.ReplacedTlb(foreignKeys);
            if (fk != foreignKeys)
                r +=(cx, ForeignKeys, fk);
            var gt = cx.ReplacedTll(targetTrans);
            if (gt != targetTrans)
                r += (cx,TargetTrans, gt);
            var tg = cx.ReplacedTll(transTarget);
            if (tg != transTarget)
                r += (cx,TransTarget, tg);
            return r;
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Target: "); sb.Append(Uid(target));
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" TransTarget: (");
                var cm = "";
                for (var b = transTarget.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                        sb.Append('='); sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
        }
        
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TargetCursor? _tgc;
            internal TransitionCursor(TableActivation ta, TransitionRowSet trs, Cursor fbm, int pos,
                Domain iC)
                : base(ta.next ?? throw new PEException("PE49205"), trs, pos, fbm._ds,
                      new TRow((Domain)trs, iC, fbm))
            {
                _trs = trs;
                _fbm = fbm;
                var cx = ta.next;
                for (var b = trs.iSMap.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.values[b.key()] is TypedValue v)
                        cx.values += (p, v);
                _tgc = TargetCursor.New(ta, this, true); // retrieve it from ta
            }
            TransitionCursor(TransitionCursor cu, TableActivation ta, long p, TypedValue v)
                : base(cu, ta.next??throw new PEException("PE49207"),
                      cu._trs.targetTrans[p]??-1L,v)
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _tgc = TargetCursor.New(ta, this,false); // retrieve it from ta
                ta.values += (p, v);
                ta.next.values += (ta._trs.target, this);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new TransitionCursor(this, (TableActivation)cx, p, v);
            }
            public static TransitionCursor operator +(TransitionCursor cu,
                (TargetActivation, long, TypedValue) x)
            {
                var (cx, p, tv) = x;
                return new TransitionCursor(cu, (TableActivation)cx, p, tv);
            }
            internal static TransitionCursor? New(TableActivation ta, TransitionRowSet trs)
            {
                var sce = (RowSet?)ta.obs[trs.data];  // annoying we can't cache this
                for (var fbm = sce?.First(ta); fbm != null;
                    fbm = fbm.Next(ta))
                    if (fbm.Matches(ta) && Eval(trs.where, ta))
                        return new TransitionCursor(ta, trs, fbm, 0, ta.insertCols);
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                if (cx.obs[_trs.where.First()?.key() ?? -1L] is SqlFunction sv && 
                    sv.op == Qlx.CURRENT)
                    return null;
                var ta = (TableActivation)cx;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = new TransitionCursor(ta, _trs, fbm, _pos+1,ta.insertCols);
                    for (var b = _trs.where.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()]?.Eval(cx) != TBool.True)
                            goto skip;
                    return ret;
                skip:;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                return null; // never
            }
            internal override BList<TableRow> Rec()
            {
                return _fbm?.Rec()??BList<TableRow>.Empty;
            }

        }
        
        internal class TargetCursor : Cursor
        {
            internal readonly Domain _td;
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor? _fb;
            internal readonly TableRow? _rec;
            TargetCursor(TableActivation ta, Cursor trc, Cursor? fb, Domain td, CTree<long, TypedValue> vals)
                : base(ta, ta._trs.defpos, td, trc._pos,
                      (fb is not null) ? (fb._ds + trc._ds) : trc._ds,
                      new TRow(td, vals))
            {
                _trs = ta._trs; _td = td; _fb = fb;
                var t = ta._tgt as Table ?? throw new DBException("42105").Add(Qlx.TABLE);
                var tt = ta._trs.target;
                var p = trc._ds.Contains(tt) ? trc._ds[tt].Item1 : -1L;
                _rec = (fb is not null) ? t.tableRows[p] : new TableRow(ta.db.nextPos, t, this);
                ta.values += (values, false);
                for (var b = ((Table)_td).tableChecks.First(); b != null; b = b.Next())
                    if (b.key() is long cp && ta.db.objects[cp] is Check ck)
                    {
                        ta.obs += ck.framing.obs;
                        if (ta._Ob(ck.search) is QlValue se && se.Eval(ta) != TBool.True)
                            throw new DBException("22211", ck.name ?? "!!");
                    }
                ta.values += (ta._trs.target, this);
            }
            public static TargetCursor operator+(TargetCursor tgc,(Context,long,TypedValue)x)
            {
                var (cx, p, tv) = x;
                var tc = (TableActivation)cx;
                if (tgc.dataType.representation.Contains(p))
                    return new TargetCursor(tc, tgc, tgc._fb, tgc._td, tgc.values + (p, tv));
                else 
                    return tgc;
            }
            internal static TargetCursor? New(TableActivation cx, TransitionCursor? trc,
                bool check)
            {
                if (trc == null || cx.next == null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var trs = trc._trs;
                if (cx._tgt is not Table t)
                    return null;
                var fb = (cx._tty != PTrigger.TrigType.Insert) ?
                    cx.next.cursors[trs.rsTargets[cx._tgt.defpos] ?? -1L] : null;
                for (var b = trs.First(); b != null; b = b.Next())
                    if (b.value() is long p && trs.transTarget[p] is long tp)
                    {
                        if (trc[p] is TypedValue v)
                        {
                            cx.CheckMetadata(tp, v);
                            vs += (tp, v);
                        }
                        else if (fb?[tp] is TypedValue fv) // for matching cases
                            vs += (tp, fv);
                    }
                    vs = CheckPrimaryKey(cx, trc, vs);
                cx.values += vs;
                for (var b = t.tableCols.First(); b != null; b = b.Next()) 
                    if (cx.obs[b.key()] is TableColumn tc)
                    {
                        if (check)
                            switch (tc.generated.gen)
                            {
                                case Generation.Expression:
                                    if (vs[tc.defpos] is TypedValue vt && vt!=TNull.Value)
                                        throw new DBException("22G0X", tc.NameFor(cx));
                                    cx.values += tc.Frame(vs);
                                    cx.Add(tc.framing);
                                    var v = cx.obs[tc.generated.exp]?.Eval(cx)??TNull.Value;
                                    trc += (cx, tc.defpos, v);
                                    vs += (tc.defpos, v);
                                    break;
                            }
                        var cv = vs[tc.defpos];
                        if (tc.defaultString is string dfs && cv == TNull.Value 
                            && tc.domain.Parse(0,dfs,cx) is TypedValue dv)
                        {
                            cx.values += (tc.defpos, dv);
                            vs += (tc.defpos, dv);
                        }
                        cv = vs[tc.defpos];
                        if (tc.notNull && cv == TNull.Value)
                            throw new DBException("22004", tc.NameFor(cx));
                        for (var cb = tc.checks?.First(); cb != null; cb = cb.Next())
                            if (cx.db.objects[cb.key()] is Check ck)
                            {
                                cx.Add(ck.framing);
                                cx.values += ck.Frame(vs);
                                if (cx.obs[ck.search] is QlValue se && se.Eval(cx) != TBool.True)
                                    throw new DBException("44003", ck.NameFor(cx), tc.NameFor(cx), t.NameFor(cx));
                            }
                    }
                var r = new TargetCursor(cx, trc, fb, t, vs);
                if (r._rec != null)
                    for (var b = trc._trs.foreignKeys.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is Level3.Index ix
                            && cx.db.objects[ix.refindexdefpos] is Level3.Index rx
                            // && rx.rows != null
                            && cx.db.objects[ix.reftabledefpos] is Table tb
                            && tb.Top().FindPrimaryIndex(cx) is Level3.Index px && px.rows!=null
                               && rx.MakeKey(r._rec.vals) is CList<TypedValue> k &&
                               k[0] is not TNull)
                        {
                            if (cx.db.objects[ix.adapter] is Procedure ad)
                            {
                                cx = (TableActivation)ad.Exec(cx, ix.keys.rowType);
                                k = ((TRow)cx.val).ToKey();
                            }
                            else if (!px.rows.Contains(k))
                            {
                                for (var bp = cx.pending.First(); bp != null; bp = bp.Next())
                                    if (bp.value() is CTree<long, TypedValue> vk && rx.MakeKey(vk) is CList<TypedValue> pk
                                        && pk.CompareTo(k) == 0)
                                        goto skip;
                                throw new DBException("23000", "missing foreign key "
                                    + trs.name + k.ToString());
                            skip:;
                            }
                            if (rx.infos[rx.definer]?.metadata?[Qlx.MAXVALUE]?.ToInt() is int mx && mx!=-1 &&  rx.rows?.Cardinality(k) > mx)
                                throw new DBException("21000");
                        }
                return r;
            }
            static (BList<long?>,CTree<long,Domain>) 
                ColsFrom(Context cx,Table tb,BList<long?> rt,CTree<long,Domain> rs)
            {
                cx.Add(tb.framing);
                for(var b= tb.super.First();b!=null;b=b.Next())
                    if (b.key() is Table st)
                        (rt, rs) = ColsFrom(cx, st, rt, rs);
                for (var b = tb.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.db.objects[p] is TableColumn tc)
                    {
                        rt += tc.defpos;
                        rs += (tc.defpos, tc.domain);
                    }
                return (rt, rs);
            }
            /// <summary>
            /// Implement the autokey feature: if a key column is an integer or char type,
            /// the engine will pick a suitable unused value. 
            /// The works cleverly for multi-column indexes. 
            /// The transition rowset adds to a private copy of the index as there may
            /// be several rows to add, and the null column(s!) might not be the first key column.
            /// But first remember we should us the super*type of ta if any
            /// </summary>
            /// <param name="fl"></param>
            /// <param name="ix"></param>
            /// <returns></returns>
            static CTree<long,TypedValue> CheckPrimaryKey(Context cx, TransitionCursor trc,
                CTree<long,TypedValue>vs)
            {
                if (cx is TableActivation ta)
                {
                    Table tb = ta.table;
                    if (tb.Top().FindPrimaryIndex(ta) is Level3.Index ix)
                    {
                        var k = CList<TypedValue>.Empty;
                        for (var b = ix.keys.First(); b != null; b = b.Next())
                            if (b.value() is long p && (cx.obs[p] ?? cx.db.objects[p]) is TableColumn tc
                                && vs[tc.defpos] is TypedValue v)
                            {
                                if (v == TNull.Value)
                                {
                                    if (ix.rows is null)
                                        v = tc.domain.kind == Qlx.CHAR ? new TChar("1") : tc.domain.defaultValue;
                                    else
                                    {
                                        v = ix.rows.NextKey(tc.domain.kind, k, 0, b.key());
                                        vs += (tc.defpos, v);
                                        cx.values += (trc._trs.targetTrans[tc.defpos] ?? -1L, v);
                                    }
                                }
                                k += v;
                            }
                    }
                }
                return vs;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                while (cx.next is not null && cx is not TableActivation)
                    cx = cx.next;
                TableActivation ta = cx as TableActivation??throw new PEException("PE1932");
                var trc = (TransitionCursor?)cx.next?.cursors[_trs.defpos]??throw new PEException("PE1933");
                var tp = _trs.targetTrans[p]??-1L;
                if (tp >=0)
                    trc += (ta, tp, v);
                cx.next.cursors += (_trs.defpos, trc);
                return ta.cursors[ta.table.defpos]??throw new PEException("PE1934");
            }
            protected override Cursor? _Next(Context cx)
            {
                throw new NotImplementedException();
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            internal override BList<TableRow> Rec()
            { 
                return (_rec==null)?BList<TableRow>.Empty : new BList<TableRow>(_rec);
            }
        }
        
        internal class TriggerCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly TargetCursor _tgc;
            internal TriggerCursor(TriggerActivation ta, Cursor tgc,CTree<long,TypedValue>? vals=null)
                : base(ta, ta._trs?.defpos??-1L, ta._trig.domain, tgc._pos, tgc._ds,
                      new TRow(ta._trig.domain, 
                          ta.trigTarget, vals??tgc.values))
            {
                _trs = ta._trs ?? throw new PEException("PE1935");
                _tgc = (TargetCursor)tgc;
                ta.values += (_trs.target, this);
                ta.values += values;
            }
            public static TriggerCursor operator +(TriggerCursor trc, (Context, long, TypedValue) x)
            {
                var (cx, p, v) = x;
                return (TriggerCursor)trc.New(cx, p, v);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                while (cx.next is not null && cx is not TriggerActivation)
                    cx = cx.next;
                var ta = (TriggerActivation?)cx??throw new PEException("PE1936");
                var tp = ta.trigTarget?[p]??throw new PEException("PE1937");
                if (ta.next == null)
                    throw new PEException("PE1938");
;                return new TriggerCursor(ta, _tgc+(ta.next,tp,v));
            }
            protected override Cursor? _Next(Context cx)
            {
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }

    /// <summary>
    /// Used for oldTable/newTable in trigger execution
    /// 
    /// </summary>
    internal class TransitionTableRowSet : RowSet
    {
        internal const long
            Data = -432,        // BTree<long,TableRow> RecPos
            _Map = -433,         // BTree<long,long?>   TableColumn
            RMap = -434,        // BTree<long,long?>   TableColumn
            Trs = -431;         // TransitionRowSet
        internal TransitionRowSet _trs => (TransitionRowSet?)mem[Trs]??throw new PEException("PE1941");
        internal BTree<long,TableRow> obs => 
            (BTree<long,TableRow>?)mem[Data]??BTree<long,TableRow>.Empty;
        internal BTree<long, long?> map =>
            (BTree<long, long?>?)mem[_Map]??BTree<long,long?>.Empty;
        internal BTree<long,long?> rmap =>
            (BTree<long, long?>?)mem[RMap]??BTree<long,long?>.Empty;
        internal bool old => (bool)(mem[TransitionTable.Old] ?? true);
        /// <summary>
        /// Old table: compute the rows that will be in the update/delete
        /// New table: get the rows from the context
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp, Context cx, TransitionRowSet trs, 
            TransitionTable tt,bool old)
            : base(dp, cx, _Mem(cx, trs, tt, old)
                  + (RSTargets, trs.rsTargets) + (Target,trs.target))
        {
            cx.Add(this);
        }
        protected TransitionTableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,TransitionRowSet trs,TransitionTable tt,bool old)
        {
            var dat = BTree<long, TableRow>.Empty;
            var dm = cx._Dom(tt.trig);
            if ((!old) && cx.newTables.Contains(trs.defpos)
                && cx.newTables[trs.defpos] is BTree<long, TableRow> tda)
                dat = tda;
            else
                for (var b = trs.First(cx); b != null; b = b.Next(cx))
                    for (var c = b.Rec()?.First(); c is not null; c = c.Next())
                        if (cx.db.objects[c.value().tabledefpos] is Table tb)
                        {
                            if (tb.nodeTypes.Count == 0)
                                dat += (tb.defpos, c.value());
                            else
                                for (var d = tb.nodeTypes.First(); d != null; d = d.Next())
                                    dat += (d.key().defpos, c.value());
                        }
            var ma = BTree<long, long?>.Empty;
            var rm = BTree<long, long?>.Empty;
            var rb = trs.rowType.First();
            for (var b=dm.rowType.First();b is not null&&rb is not null;b=b.Next(),rb=rb.Next())
            if (b.value() is long p && rb.value() is long q &&  trs.transTarget[q] is long f){
                ma += (f, p);
                rm += (p, f);
            }
            return dm.mem + (TransitionTable.Old,old)
                + (Trs,trs) + (Data,dat) + (_Map,ma)
                +(RMap,rm);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTableRowSet(defpos,m);
        }
        public static TransitionTableRowSet operator +(TransitionTableRowSet et, (long, object) x)
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
            return (TransitionTableRowSet)et.New(m + x);
        }
        public static TransitionTableRowSet operator +(TransitionTableRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            switch(p)
            {
                case Data: m += (_Depth, cx._DepthTVX((BTree<long, TableRow>)o, d)); break;
                case _Map:
                case RMap: m += (_Depth, cx._DepthTVV((BTree<long, long?>)o, d)); break;
                default: m = rs._Depths(cx, m, d, p, o); break;
            }
            return (TransitionTableRowSet)rs.New(m + (p, o));
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new TransitionTableRowSet(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TransitionTableRowSet)base._Replace(cx, so, sv);
            var ts = _trs.Replace(cx, so, sv);
            if (ts != _trs)
                r += (cx,Trs, ts);
            var mp = cx.ReplacedTll(map);
            if (mp != map)
                r +=(cx, _Map, mp);
            var rm = cx.ReplacedTll(rmap);
            if (rm != rmap)
                r +=(cx, RMap, rm);
            return r;
        }
        protected override Cursor? _First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException(); // never
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(old ? " OLD" : " NEW");
        }
        
        internal class TransitionTableCursor : Cursor
        {
            internal readonly TableRowSet.TableCursor _tc;
            internal readonly TransitionTableRowSet _tt;
            TransitionTableCursor(Context cx,TransitionTableRowSet tt,
                TableRowSet.TableCursor tc,ABookmark<long,TableRow> bmk,int pos)
                :base(cx,tt,pos,
                     new BTree<long,(long,long)>(bmk.key(),(bmk.value().defpos,bmk.value().ppos)),
                     new TRow(tt,tt.rmap,bmk.value().vals))
            {
                _tc = tc; _tt = tt;
            }
            TransitionTableCursor(TransitionTableCursor cu, Context cx, long p, TypedValue v)
                : base(cu, cx, p, v) 
            { _tc = cu._tc; _tt = cu._tt; }
            internal static TransitionTableCursor? New(Context cx,TransitionTableRowSet tt)
            {
                if (cx.obs[tt.rsTargets.First()?.value() ?? -1L] is not RowSet rs)
                        return null;
                var tc = (TableRowSet.TableCursor?)rs.First(cx);
                if (tc == null || tc._bmk==null)
                    return null;
                return new TransitionTableCursor(cx,tt,tc,tc._bmk,tc._pos);
            }
            protected override Cursor? _Next(Context _cx)
            {
                var tc = (TableRowSet.TableCursor?)_tc.Next(_cx);
                return (tc != null && tc._bmk is not null) ? 
                    new TransitionTableCursor(_cx, _tt, tc, tc._bmk,_pos + 1):null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException(); // never
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                if (_tt.old)
                    throw new DBException("23103","OLD TABLE");
                return new TransitionTableCursor(this,cx,p,v);
            }

            internal override BList<TableRow>? Rec()
            {
                return (_tc._bmk?.value() is TableRow rc)?new BList<TableRow>(rc):null;
            }
        }
    }
    
    internal class RowSetSection : RowSet
    {
        internal const long
            Offset = -438, // int
            Size = -439; // int
        internal int offset => (int)(mem[Offset]??0);
        internal int size => (int)(mem[Size] ?? 0);
        internal override Assertions Requires => Assertions.MatchesTarget;
        internal RowSetSection(Context cx,RowSet s, int o, int c)
            : base(cx.GetUid(),_Mem(cx,s)+(Offset,o)+(Size,c)
                  +(RSTargets,s.rsTargets)
                  +(Table.LastData,s.lastData))
        {
            cx.Add(this);
        }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,RowSet r)
        {
            var m = r.mem + (ISMap, r.iSMap) + (SIMap, r.sIMap);
            if (r.names != BTree<string, (int,long?)>.Empty)
                m += (ObInfo.Names, r.names);
            m += (_Source, r.defpos);
            cx._Add(r + (_From, cx.GetPrevUid()));
            return m;
        }
        public static RowSetSection operator +(RowSetSection et, (long, object) x)
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
            return (RowSetSection)et.New(m + x);
        }
        public static RowSetSection operator +(RowSetSection rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (RowSetSection)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RowSetSection(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new RowSetSection(dp,m);
        }
        protected override Cursor? _First(Context cx)
        {
            var b = ((RowSet?)cx.obs[source])?.First(cx);
            for (int i = 0; b is not null && i < offset; i++)
                b = b.Next(cx);
            return (b==null)?null:RowSetSectionCursor.New(cx,this,b);
        }
        protected override Cursor? _Last(Context cx)
        {
            var b = ((RowSet?)cx.obs[source])?.Last(cx);
            for (int i = 0; b != null && i < offset; i++)
                b = b.Previous(cx);
            return (b==null)?null:RowSetSectionCursor.New(cx, this, b);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (size != 0)
            { sb.Append(" Size "); sb.Append(size); }
            if (offset!=0)
            { sb.Append(" Offset "); sb.Append(offset); }
        }
        
        internal class RowSetSectionCursor : Cursor
        {
            readonly RowSetSection _rss;
            readonly Cursor _rb;
            RowSetSectionCursor(Context cx, RowSetSection rss, Cursor rb)
                : base(cx, rss, rb._pos, rb._ds, rb) 
            { 
                _rss = rss;
                _rb = rb; 
            }
            internal static RowSetSectionCursor? New(Context cx,RowSetSection rss,Cursor rb)
            {
                if (rb == null)
                    return null;
                return new RowSetSectionCursor(cx, rss, rb);
            }
            protected override Cursor? _Next(Context cx)
            {
                if (_pos+1 >= _rss.size)
                    return null;
                if (_rb.Next(cx) is Cursor rb)
                    return new RowSetSectionCursor(cx, _rss, rb);
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                if (_pos + 1 >= _rss.size)
                    return null;
                if (_rb.Previous(cx) is Cursor rb)
                    return new RowSetSectionCursor(cx, _rss, rb);
                return null;
            }
            internal override BList<TableRow>? Rec()
            {
                return _rb?.Rec();
            }
        }
    }
    
    internal class DocArrayRowSet : RowSet
    {
        internal const long
            Docs = -440; // BList<QlValue>
        internal BList<QlValue> vals => (BList<QlValue>?)mem[Docs]??BList<QlValue>.Empty;
        internal DocArrayRowSet(Context cx, SqlRowArray d)
            : base(cx.GetUid(), _Mem(cx, d))
        {
            cx.Add(this);
        }
        static BTree<long, object> _Mem(Context cx, SqlRowArray d)
        {
            var vs = BList<QlValue>.Empty;
            if (d != null)
                for (int i = 0; i < d.rows.Count; i++)
                    if (cx.obs[d.rows[i]??-1L] is QlValue v)
                        vs += v;
            return Domain.TableType.mem + (Docs, vs);
        }
        protected DocArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static DocArrayRowSet operator +(DocArrayRowSet et, (long, object) x)
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
            return (DocArrayRowSet)et.New(m + x);
        }
        public static DocArrayRowSet operator +(DocArrayRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            if (p == Docs)
                m += (_Depth, Context._DepthBO((BList<QlValue>)o, d));
            else
                m = rs._Depths(cx, m, d, p, o);
            return (DocArrayRowSet)rs.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new DocArrayRowSet(defpos,m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new DocArrayRowSet(dp, m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (DocArrayRowSet)base._Replace(cx, so, sv);
            var ds = BList<QlValue>.Empty;
            for (var b = vals.First(); b != null; b = b.Next())
                ds += (QlValue?)cx.done[b.value().defpos] ?? b.value();
            r +=(cx, Docs, ds);
            return r;
        }
        protected override Cursor? _First(Context _cx)
        {
            return DocArrayBookmark.New(_cx,this);
        }
        protected override Cursor? _Last(Context cx)
        {
            return DocArrayBookmark.New(this, cx);
        }
        
        internal class DocArrayBookmark : Cursor
        {
            readonly DocArrayRowSet _drs;
            readonly ABookmark<int, QlValue> _bmk;

            DocArrayBookmark(Context _cx,DocArrayRowSet drs, ABookmark<int, QlValue> bmk) 
                :base(_cx,drs,bmk.key(),E,_Row(_cx,bmk.value())) 
            {
                _drs = drs;
                _bmk = bmk;
            }
            static TRow _Row(Context cx,QlValue sv)
            {
                return new TRow(sv.domain,new CTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
            }
            internal static DocArrayBookmark? New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            internal static DocArrayBookmark? New(DocArrayRowSet drs, Context cx)
            {
                var bmk = drs.vals.Last();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(cx, drs, bmk);
            }
            protected override Cursor? _Next(Context _cx)
            {
                var bmk = ABookmark<int, QlValue>.Next(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,_drs, bmk);
            }
            protected override Cursor? _Previous(Context _cx)
            {
                var bmk = ABookmark<int, QlValue>.Previous(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx, _drs, bmk);
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    
    /// <summary>
    /// WindowRowSet is built repeatedly during traversal of the source rowset.
    /// </summary>
    internal class WindowRowSet : RowSet
    {
        internal const long
            Multi = -441, // TMultiset
            Window = -442; // SqlFunction
        internal RTree rtree => (RTree?)mem[OrderedRowSet._RTree]??throw new PEException("PE1941");
        internal TMultiset values => (TMultiset?)mem[Multi] ??throw new PEException("PE1942");
        internal SqlFunction wf=> (SqlFunction?)mem[Window] ??throw new PEException("PE1943");
        protected WindowRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowRowSet(defpos, m);
        }
        protected override Cursor? _First(Context _cx)
        {
            return WindowCursor.New(_cx,this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return WindowCursor.New(this, _cx);
        }
        public static WindowRowSet operator +(WindowRowSet et, (long, object) x)
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
            return (WindowRowSet)et.New(m + x);
        }
        public static WindowRowSet operator +(WindowRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            switch(p)
            {
                case Multi: m += (_Depth, Math.Max(((TMultiset)o).dataType.depth + 1, d)); break;
                case Window: m += (_Depth, Math.Max(((SqlFunction)o).depth+1, d)); break;
                default: m = rs._Depths(cx, m, d, p, o); break;
            }
            return (WindowRowSet)rs.New(m + (p, o));
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return (dp == defpos) ? this : new WindowRowSet(dp,mem);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var w = (WindowSpecification)(cx.obs[wf.window] ?? throw new PEException("PE651"));
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var od = (w.order!=Row)?w.order:w.partition;
            if (od == null)
                return this;
            var tree = new RTree(source,cx,od,TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset((Domain)cx.Add(new Domain(cx.GetUid(), Qlx.MULTISET, wf.domain)));
            for (var rw = ((RowSet?)cx.obs[source])?.First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(od, rw.values), cx.cursors);
                values.Add(v);
            }
            return (RowSet)cx.Add(this+(Multi,values)+(_Built,true)+(Level3.Index.Tree,tree.mt));
        }
        
        internal class WindowCursor : Cursor
        {
            WindowCursor(Context cx,long rp,Domain dm,int pos, TRow rw) 
                : base(cx,rp,dm,pos,E,rw)
            { }
            internal static Cursor? New(Context cx, WindowRowSet ws)
            {
                ws = (WindowRowSet)ws.Build(cx);
                var bmk = ws.rtree.First(cx);
                if (bmk == null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var cb = ws.rtree.mt.info.First();
                for (var bb = bmk._key.First(); bb != null && cb != null;
                    bb = bb.Next(), cb = cb.Next())
                    if (cb.value() is long p)
                        vs += (p, bb.value());
                return new WindowCursor(cx, ws.defpos, ws, 0, new TRow(ws, vs));
            }
            internal static Cursor? New(WindowRowSet ws, Context cx)
            {
                ws = (WindowRowSet)ws.Build(cx);
                var bmk = ws.rtree.Last(cx);
                if (bmk == null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var cb = ws.rtree.mt.info.First();
                for (var bb = bmk._key.First(); bb != null && cb != null;
                    bb = bb.Next(), cb = cb.Next())
                    if (cb.value() is long p)
                        vs += (p, bb.value());
                return new WindowCursor(cx, ws.defpos, ws, 0, new TRow(ws, vs));
            }
            protected override Cursor _Next(Context cx)
            {
                throw new NotImplementedException();
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    /// <summary>
    /// The RestRowSet is basically a client instance for access to a RestView.
    /// The RestView syntax deliberately is in terms of a single view, but
    /// the actual query sent by the RestRowSet can be a general one, provided
    /// it is built from the remote columns it defines and their domains, together with 
    /// a set of UsingOperands supplied as literals for each roundtrip. 
    /// </summary>
    internal class RestRowSet : InstanceRowSet
    {
        internal const long
            DefaultUrl = -370,  // string 
            _RestView = -459,    // long RestView
            RemoteCols = -373, // BList<long?> QlValue
            RestValue = -457,   // TList
            SqlAgent = -256; // string

        internal TList aVal => mem[RestValue] as TList??throw new DBException("42105").Add(Qlx.VIEW);
        internal long restView => (long)(mem[_RestView] ?? -1L);
        internal string? defaultUrl => (string?)mem[DefaultUrl];
        internal string sqlAgent => (string?)mem[SqlAgent] ?? "Pyrrho";
        internal CTree<long, string> namesMap =>
            (CTree<long, string>?)mem[RestView.NamesMap] ?? CTree<long, string>.Empty;
        public RestRowSet(Iix lp, Context cx, RestView vw, Domain q)
            : base(lp.dp, _Mem(cx,lp,vw,q) +(_RestView,vw.defpos) +(Infos,vw.infos)
                  +(Asserts,Assertions.SpecificRows|Assertions.AssignTarget) + (_Depth,vw.depth+1))
        {
            cx.Add(this);
            cx.restRowSets += (lp.dp, true);
            cx.versioned = true;
        }
        protected RestRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, Iix lp, RestView vw, Domain q)
        {
            var vs = BList<DBObject>.Empty; // the resolved select tree in query rowType order
            var vd = BList<long?>.Empty;
            var qn = q?.Needs(cx, -1L, CTree<long, bool>.Empty);
            cx.defs += (vw.NameFor(cx), lp, Ident.Idents.Empty);
            int d;
            var nm = CTree<long, string>.Empty;
            var mn = BTree<string, (int,long?)>.Empty;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var ur = (Table?)cx.db.objects[vw.usingTable];
            var un = CTree<string, bool>.Empty;
            for (var b = ur?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string s && s != "")
                    un += (s, true);
            for (var b = vw.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string ns && !un.Contains(ns))
                {
                    vd += p;
                    nm += (p, ns);
                    mn += (ns, (b.key(),p));
                }
            for (var b = vw.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is QlValue sc && !vs.Has(sc)
                        && (q == null || qn?.Contains(p) == true))
                    vs += sc;
            d = vs.Length;
            for (var b = vw.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue sc && !vs.Has(sc))
                    vs += sc;
            var fd = (vs == BList<DBObject>.Empty) ? vw : new Domain(cx.GetUid(), cx, Qlx.TABLE, vs, d);
            var r = fd.mem + (ObInfo.Name, vw.NameFor(cx))
                   + (RestView.NamesMap, nm) + (ObInfo.Names,mn)
                   + (RestView.UsingTable, vw.usingTable)
                   + (Matching, mg) + (RemoteCols, vd) + (ObInfo.Names,mn)
                   + (RSTargets, new BTree<long, long?>(vw.defpos, lp.dp));
            if (vw.usingTable < 0 && cx.role != null &&
                vw.infos[cx.role.defpos] is ObInfo vi)
                r += (DefaultUrl, vi.metadata[Qlx.URL]?.ToString() ??
                    vi.metadata[Qlx.DESC]?.ToString() ?? "");
            return r;
        }
        public static RestRowSet operator +(RestRowSet et, (long, object) x)
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
            return (RestRowSet)et.New(m + x);
        }
        public static RestRowSet operator +(RestRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (RestRowSet)rs.New(m + (p, o));
        }
        public static RestRowSet operator-(RestRowSet rs,long p)
        {
            return (RestRowSet)rs.New(rs.mem - p);
        }
        internal override BList<long?> SourceProps => BList<long?>.Empty;
        protected override Cursor? _First(Context _cx)
        {
            return RestCursor.New(_cx, this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return RestCursor.New(this, _cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (m==mem)?this:new RestRowSet(defpos,m);
        }
        internal override string DbName(Context cx)
        {
            var _vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105").Add(Qlx.VIEW);
            if (cx._Ob(_vw.defpos)?.infos[cx.role.defpos] is ObInfo vi)
                return GetUrl(cx,vi).Item1??"";
            return "";
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new RestRowSet(dp, m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RestRowSet)base._Replace(cx, so, sv);
            r += (_RestView, cx.ObReplace(restView, so, sv));
            var ch = false;
            var rc = BList<long?>.Empty;
            var nm = CTree<long, string>.Empty;
            for (var b = r.remoteCols.First(); b != null; b = b.Next())
                if (b.value() is long op)
                {
                    var p = cx.ObReplace(op, so, sv);
                    rc += p;
                    if (r.namesMap[op] is string nn)
                        nm += (p, nn);
                    ch = ch || p != op;
                }
            if (ch)
            {
                r +=(cx, RemoteCols, rc);
                r +=(cx, RestView.NamesMap, nm);
            }
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var rc = cx.FixLl(remoteCols);
            if (rc != remoteCols)
                r = cx.Add(r, RemoteCols, rc);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r = cx.Add(r, RestView.NamesMap, nm);
            var rv = cx.Fix(restView);
            if (rv != restView)
                r = cx.Add(r, _RestView, rv);
            return r;
        }
        public HttpRequestMessage GetRequest(Context cx, string url, ObInfo? vi)
        {
            var rq = new HttpRequestMessage
            {
                RequestUri = new Uri(url)
            };
            rq.Headers.Add("UserAgent","Pyrrho " + PyrrhoStart.Version[1]);
            var cu = cx.user ?? throw new DBException("42105").Add(Qlx.USER);
            var cr = cu.name + ":";
            var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
            rq.Headers.Add("Authorization", "Basic " + d);
            if (cx.db is not null && vi?.metadata.Contains(Qlx.ETAG) == true)
            {
                rq.Headers.Add("If-Unmodified-Since",
                    ""+ new THttpDate(((Transaction)cx.db).startTime,
                            vi.metadata.Contains(Qlx.MILLI)));
                var vwdesc = cx.url ?? defaultUrl;
                if (vwdesc is string ur && (cx.db as Transaction)?.etags[ur] is string et)
                    rq.Headers.Add("If-Match" , et);
            }
            return rq;
        }
        internal (string?, string?) GetUrl(Context cx, ObInfo vi,
            string? url = null)
        {
            url = cx.url ?? url ?? defaultUrl;
            var sql = new StringBuilder();
            string targetName ;
            string[] ss;
            var mu = vi?.metadata.Contains(Qlx.URL) == true;
            if (mu)
            {
                if (url == "" || url == null)
                    url = vi?.metadata[Qlx.URL]?.ToString();
                sql.Append(url);
                for (var b = matches.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue kn)
                    {
                        sql.Append('/'); sql.Append(kn.name);
                        sql.Append('='); sql.Append(b.value());
                    }
                url = sql.ToString();
                ss = url.Split('/');
                if (ss.Length <= 5)
                    throw new DBException("2E305", url);
                targetName = ss[5];
            }
            else
            {
                if (url == "" || url == null)
                    url = vi?.metadata[Qlx.DESC]?.ToString() ?? cx.url;
                if (url == null)
                    return (null, null);
                ss = url.Split('/');
                if (ss.Length <= 5)
                    throw new DBException("2E305", url);
                targetName = ss[5];
                var ub = new StringBuilder(ss[0]);
                for (var i = 1; i < ss.Length && i < 5; i++)
                {
                    ub.Append('/');
                    ub.Append(ss[i]);
                }
                url = ub.ToString();
            }
            return (url, targetName);
        }
        internal override CTree<long, Domain> _RestSources(Context cx)
        {
            return new CTree<long,Domain>(defpos,this);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        /// <summary>
        /// Add grouping information and deal with needed usingrowsetcols
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <param name="im"></param>
        /// <returns></returns>
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long, object>? m = null)
        {
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            // what might we need?
            var xs = ((CTree<long, bool>?)mm[_Where] ?? CTree<long, bool>.Empty)
                + ((CTree<long, bool>?)mm[Having] ?? CTree<long, bool>.Empty);
            // deal with group cols
            var gr = (long)(mm[Group] ?? -1L);
            var groups = (GroupSpecification?)cx.obs[gr];
            var gs = groupings;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                    gs = _Info(cx, g, gs);
            if (gs != groupings)
            {
                mm += (Groupings, gs);
                var gc = cx.GroupCols(gs, this);
                mm += (GroupCols, gc);
                for (var c = gc.rowType.First(); c != null; c = c.Next())
                    if (c.value() is long p)
                        xs += (p, true);
            }
            // Collect all operands that we don't have yet
            var ou = CTree<long, bool>.Empty;
            for (var b = xs.First(); b is not null; b = b.Next())
                if (cx.obs[b.key()] is QlValue e)
                    for (var c = e.Operands(cx).First(); c != null; c = c.Next())
                        if (!representation.Contains(c.key()))
                            ou += (c.key(), true);
            // Add them to the Domain if necessary
            var rc = rowType;
            var rs = representation;
            var nm = namesMap;
            for (var b = ou.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue v)
                {
                    var p = b.key();
                    rc += p;
                    rs += (p, v.domain);
                    nm += (p, v.alias ?? v.NameFor(cx));
                }
            var im = mem;
            var ag = (CTree<long, bool>?)mm[Aggs] ?? CTree<long, bool>.Empty;
            if (ag != CTree<long, bool>.Empty)
            {
                var nc = BList<long?>.Empty; // start again with the aggregating rowType, follow any ordering given
                var ns = CTree<long, Domain>.Empty;
                var gb = ((Domain?)m?[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
                var kb = KnownBase(cx);
                var es = BList<QlValue>.Empty;
                for (var b = rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue v)
                    {
                        // See if we can economise on top-level aggregations
                        for (var c = es.First(); c != null; c = c.Next())
                            if (v._MatchExpr(cx, c.value(), this))
                                cx.Replace(c.value(), new SqlCopy(cx.GetUid(), cx, v.name ?? "", defpos, v));
                        if (v is SqlFunction ct && ct.op == Qlx.COUNT && ct.mod == Qlx.TIMES)
                        {
                            nc += p;
                            ns += (p, Int);
                        }
                        else if (v.IsAggregation(cx, CTree<long, bool>.Empty) != CTree<long, bool>.Empty)
                            for (var c = ((QlValue?)cx.obs[p])?.KnownFragments(cx, kb).First();
                                c != null; c = c.Next())
                                if (ns[c.key()] is Domain cd)
                                {
                                    var k = c.key();
                                    nc += k;
                                    ns += (k, cd);
                                }
                                else if (gb.Contains(p) && Knows(cx, p))
                                {
                                    nc += p;
                                    ns += (p, v.domain);
                                }
                        if (v is SqlValueExpr || v is SqlFunction)
                            es += v;
                    }
                im = im + (RowType, nc) + (Representation, ns) + (Aggs, ag);
                if (nm != namesMap)
                    im += (RestView.NamesMap, nm);
            }
            if (nm != namesMap)
                im += (RestView.NamesMap, nm);
            return base.Apply(mm, cx, im);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override RowSet ApplySR(BTree<long,object> mm, Context cx)
        {
            for (var b = mm.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return this;
            var ag = (CTree<long, bool>?)mm[Aggs] ?? throw new DBException("42105").Add(Qlx.FUNCTION);
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            var r = this;
            if (mm[GroupCols] is Domain gd)
            {
                // we restrict to grouping columns we know
                var ng = cx.GroupCols(gd.rowType, r);
                m += (GroupCols, ng);
                if (mm[Groupings] is BList<long?> l1)
                    m += (Groupings, KnownCols(cx,l1));
                if (mm[Group] is long g && cx.obs[g] is GroupSpecification gs)
                    m += (Group, gs.Known(cx, this).defpos);
                if (ng != groupCols)
                    r = (RestRowSet)cx.Add((RestRowSet)Apply(m, cx));
            }
            m += (Aggs, ag);
            var dm = mm[AggDomain] as Domain??Null;
            var nc = BList<long?>.Empty; // start again with the aggregating rowType, follow any ordering given
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain?)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    if (v is SqlFunction ct && ct.op == Qlx.COUNT && ct.mod == Qlx.TIMES)
                    {
                        nc += p;
                        ns += (p, Int);
                    }
                    else if (v.KnownBy(cx, r))
                    {
                        if (!ns.Contains(p))
                            nc += p;
                        ns += (p, v.domain);
                    }
                }
            for (var b = gb.First(); b != null; b = b.Next())
                if (b.key() is long p && cx.obs[p] is QlValue v && v.KnownBy(cx,r))
                {
                    if (!ns.Contains(p))
                        nc += p;
                    ns += (p, v.domain);
                }
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Dom(p) is Domain dd)
                {
                    if (!ns.Contains(p))
                        nc += p;
                    ns += (p, dd);
                }
            var d = remoteCols.Length;
            m = m + (RowType, nc) + (Representation,ns) + (Display,d) + (Aggs, ag);
            r = (RestRowSet)r.New(m);
            cx.Add(r);
            cx.done = od;
            return (RowSet?)cx.obs[defpos]??this; // will have changed
        }
        internal override CTree<long, bool> _ProcSources(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        BList<long?> KnownCols(Context cx,BList<long?> ls)
        {
            var r = BList<long?>.Empty;
            for (var b = (ls.First()); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v
                    && v.KnownBy(cx,this))
                    r += p;
            return r;
        }
        internal override CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            for (var b = ag.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue v)
                    for (var c = v.Operands(cx).First(); c != null; c = c.Next())
                        if (namesMap.Contains(c.key()))
                            ma += (b.key(), true);
            return ma;
        }
 /*       protected static BList<long?> _Info(Context cx, Grouping g, BList<long?> gs)
        {
            gs += g.defpos;
            var cs = BList<DBObject>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue v)
                    cs += v;
            var dm = new Domain(Qlx.ROW, cx, cs);
            cx.Replace(g, g + (RowType,dm.rowType)+(Representation,dm.representation));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        } */
        internal override RowSet Build(Context cx)
        {
            return RoundTrip(cx);
        }
        RowSet RoundTrip(Context cx)
        {
            if (cx.obs[restView] is not RestView vw || vw.infos[cx.role.defpos] is not ObInfo vi)
                throw new PEException("PE1411");
            var (url,targetName) = GetUrl(cx,vi,defaultUrl);
            var sql = new StringBuilder();
            if (url == null)
                throw new DBException("42105").Add(Qlx.URL);
            var rq = GetRequest(cx,url, vi);
            rq.Headers.Add("Accept", vw?.mime ?? "application/json");
            if (vi?.metadata.Contains(Qlx.URL) == true)
                rq.Method = HttpMethod.Get;
            else
            {
                rq.Method = HttpMethod.Post;
                sql.Append("select ");
                if (distinct)
                    sql.Append("distinct ");
                var co = "";
                cx.groupCols += (defpos, groupCols);
                var hasAggs = aggs!=CTree<long,bool>.Empty;
                for (var b = rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue s)
                    {
                        if (hasAggs && (s is SqlCopy || s is SqlReview) 
                            && !s.Grouped(cx, this))
                            continue;
                        var sn = s.ToString(sqlAgent, Remotes.Selects, remoteCols,
                           namesMap, cx);
                        if (sn != "")
                        {
                            sql.Append(co); co = ",";
                            sql.Append(sn);
                        }
                    }
                sql.Append(" from "); sql.Append(targetName);
                if (where.Count > 0 || matches.Count > 0)
                {
                    var sw = WhereString(cx, namesMap);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
                        sql.Append(sw);
                    }
                }
                if (groupCols.rowType!=BList<long?>.Empty)
                {
                    sql.Append(" group by "); co = "";
                    for (var b = groupCols.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && cx.obs[p] is QlValue sv)
                        {
                            sql.Append(co); co = ",";
                            sql.Append(sv.alias ?? sv.name);
                        }
                }
                if (having.Count > 0)
                {
                    var sw = HavingString(cx);
                    if (sw.Length > 0)
                    {
                        sql.Append(" having ");
                        sql.Append(sw);
                    }
                } 
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            if (rq.Method==HttpMethod.Post)
            {
                var sc = new StringContent(sql.ToString(), Encoding.UTF8);
                rq.Content = sc;
                cx.url = url;
            }
            var wr = PyrrhoStart.htc.Send(rq);
            try
            {
                if (wr == null)
                    throw new DBException("2E201", url);
                var et = GetH(wr.Headers,"ETag");
                if (et != null && et.StartsWith("W/"))
                    et = et[2..];
                if (et != null)
                    et = et.Trim('"');
                var ds = GetH(wr.Headers,"Description");
                var cl = GetH(wr.Headers, "Classification");
                var ld = GetH(wr.Headers, "LastData");
                var lv = (cl != "") ? Level.Parse(cl,cx) : Level.D;
                var mime = GetH(wr.Content.Headers, "Content-Type");
                if (mime=="")
                    mime = "text/plain";
                var ss = wr.Content.ReadAsStream();
                var sr = new StreamReader(ss).ReadToEnd();
        //        if (PyrrhoStart.HTTPFeedbackMode)
        //           Console.WriteLine(sr);
                TypedValue? a = null;
                var or = cx.result;
                cx.result = target; // sneaky
                if (sr != null)
                    a = Parse(0, sr, mime, cx);
                cx.result = or;
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (a is TList aa)
                        Console.WriteLine("--> " + aa.list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (a?.ToString() ?? "null"));
                }
                var len = (a is TList ta) ? ta.Length : 1;
                var r = this + (_Built, true) 
                    + (Level3.Index.Tree,new MTree(Domain.Null,TreeBehaviour.Disallow,len));
                if (a != null)
                    r += (RestValue, a);
                cx._Add(r);
                if (et != null && et != "" && rq.Method == HttpMethod.Post) // Pyrrho manual 3.8.1
                {
                    var vwdesc = cx.url ?? defaultUrl;
                    var tr = (Transaction)cx.db;
                    if (vwdesc != null)
                        cx.db = tr + (Transaction._ETags, tr.etags + (vwdesc, et));
                    if (PyrrhoStart.DebugMode || PyrrhoStart.HTTPFeedbackMode)
                        Console.WriteLine("Response ETag: " + et);
                }
                if (ds != null)
                    r += (ObInfo.Description, ds);
                if (lv != Level.D)
                    r += (Classification, lv);
                if (ld != null && ld != "")
                    r += (Table.LastData, long.Parse(ld));
                return r;
            }
            catch(DBException e)
            {
                throw e;
            }
            catch
            {
                throw new DBException("40082");
            }
        }
        static string GetH(System.Net.Http.Headers.HttpHeaders hm,string hn)
        {
            hm.TryGetValues(hn, out IEnumerable<string>? vals);
            return vals?.First()??"";
        }
        BTree<long,bool> Grouped(Context cx, Grouping gs, StringBuilder sql, ref string cm, 
            BTree<long,bool> ids)
        {
            for (var b = gs.members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is Grouping gi && Knows(cx, gi.defpos) && !ids.Contains(gi.defpos))
                {
                    ids += (gi.defpos, true);
                    sql.Append(cm); cm = ",";
                    sql.Append(gi.alias);
                }
            for (var b=gs.groups.First();b is not null;b=b.Next())
                ids = Grouped(cx, b.value(), sql, ref cm, ids);
            return ids;
        }
        public string WhereString<V>(Context cx,CTree<long,V>cs) where V: IComparable
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue sv && sv.KnownBy(cx, cs))
                {
                    try
                    {
                        var sw = sv.ToString(sqlAgent, Remotes.Operands, remoteCols, namesMap, cx);
                        if (sw.Length > 1)
                        {
                            sb.Append(cm); cm = " and ";
                            sb.Append(sw);
                        }
                    }
                    catch (DBException) { }
                }
            var ms = BTree<string, string>.Empty;
            for (var b = matches.First(); b != null; b = b.Next())
            {
                if (!cs.Contains(b.key()))
                    continue;
                try
                {
                    var nm = cx.obs[b.key()]?.ToString(sqlAgent, Remotes.Operands, 
                        remoteCols, namesMap, cx)??"??";
                    var tv = b.value();
                    if (tv.dataType.kind == Qlx.CHAR)
                        ms += (nm, "'" + tv.ToString() + "'");
                    else
                        ms += (nm, tv.ToString());
                }
                catch (DBException) { return ""; }
            }
            for (var b = ms.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.key());
                sb.Append('=');
                sb.Append(b.value());
            }
            return sb.ToString();
        }
        public string HavingString(Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = having.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue sv)
                    try
                    {
                        var sw = sv.ToString(sqlAgent, Remotes.Operands, remoteCols, namesMap, cx);
                        if (sw.Length > 1)
                        {
                            sb.Append(cm); cm = " and ";
                            sb.Append(sw);
                        }
                    }
                    catch (DBException) { }
            return sb.ToString();
        }
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, Domain rt)
        {
            ts +=(cx,Target, rsTargets.First()?.key() ?? throw new DBException("42105").Add(Qlx.TABLE));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105").Add(Qlx.VIEW);
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.ROLE);
            var ta = vi.metadata.Contains(Qlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, this, ts, PTrigger.TrigType.Insert)
                : new RESTActivation(cx, this, ts, PTrigger.TrigType.Insert);
            return new BTree<long, TargetActivation>(ts.target, ta);
        }
        internal override BTree<long, TargetActivation> Update(Context cx,RowSet fm)
        {
            fm += (cx,Target, rsTargets.First()?.key() ?? throw new DBException("42105").Add(Qlx.TABLE));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105").Add(Qlx.VIEW);
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.ROLE);
            var ta = vi.metadata.Contains(Qlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, this, fm, PTrigger.TrigType.Update)
                : new RESTActivation(cx, this, fm, PTrigger.TrigType.Update);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override BTree<long, TargetActivation> Delete(Context cx,RowSet fm)
        {
            fm += (cx,Target, rsTargets.First()?.key()??throw new DBException("42105").Add(Qlx.TABLE));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105").Add(Qlx.VIEW); 
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.ROLE);
            var ta = vi.metadata.Contains(Qlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, this, fm, PTrigger.TrigType.Delete)
                : new RESTActivation(cx, this, fm, PTrigger.TrigType.Delete);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(' ');
            if (defaultUrl != "")
                sb.Append(defaultUrl);
            sb.Append(" RestView "); sb.Append(Uid(restView));
            if (remoteCols != BList<long?>.Empty)
            {
                sb.Append(" RemoteCols:");
                var cm = "(";
                for (var b = remoteCols.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
            if (namesMap != CTree<long, string>.Empty)
            {
                sb.Append(" RemoteNames:");
                var cm = "(";
                for (var b = namesMap.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append('=');
                    sb.Append(b.value());
                }
                sb.Append(')');
            }
            if (mem.Contains(Having))
            {
                sb.Append(" having (");
                var cm = "";
                for (var b = having.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
        }
        
        internal class RestCursor : Cursor
        {
            readonly RestRowSet _rrs;
            readonly int _ix;
            RestCursor(Context cx,RestRowSet rrs,int pos,int ix)
                :this(cx,rrs,pos, ix, _Value(cx,rrs,ix))
            { }
            RestCursor(Context cx,RestRowSet rrs,int pos,int ix,TRow tr)
                :base(cx,rrs.defpos,rrs, pos,E,tr)
            {
                _rrs = rrs; _ix = ix;
                var tb = rrs.rowType.First();
                for (var b = rrs.sRowType.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                    if (b.value() is long p && tb.value() is long tp)
                    cx.values += (p, tr[tp]);
            }
            RestCursor(Context cx,RestCursor rb,long p,TypedValue v)
                :base(cx,rb._rrs,rb._pos,E,rb+(p,v))
            {
                _rrs = rb._rrs; _ix = rb._ix;
            }
            static TRow _Value(Context cx, RestRowSet rrs, int pos)
            {
                cx.values += ((TRow)rrs.aVal[pos]).values;
                return new TRow(rrs,cx.values);
            }
            internal static RestCursor? New(Context cx,RestRowSet rrs)
            {
                cx.obs += (rrs.defpos, rrs); // doesn't change depth
                return (rrs.aVal.Length!=0)?new RestCursor(cx, rrs, 0, 0):null;
            }
            internal static RestCursor? New(RestRowSet rrs, Context cx)
            {
                cx.obs += (rrs.defpos, rrs); // doesn't change depth
                var i = rrs.aVal.Length - 1;
                return (i >= 0) ? new RestCursor(cx, rrs, 0, i) : null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new RestCursor(cx, this, p, v);
            }
            protected override Cursor? _Next(Context cx)
            {
                var i = _ix + 1;
                return (i < _rrs.aVal.Length) ? new RestCursor(cx, _rrs, _pos + 1, i) : null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                var i = _ix - 1; 
                return (i >=0)?new RestCursor(cx, _rrs, _pos + 1, i): null;
            }
            internal override BList<TableRow> Rec()
            {
                var r = BList<TableRow>.Empty;
                for (var i = 0; i < _rrs.aVal.Length; i++)
                    r += new RemoteTableRow(_rrs.restView, ((TRow)_rrs.aVal[i]).values,
                        _rrs.defaultUrl??"",_rrs);
                return r;
            }
        }
    }
    internal class RestRowSetUsing : InstanceRowSet
    {
        internal const long
            RestTemplate = -443, // long RestRowSet
            UrlCol = -446, // long QlValue
            UsingCols = -259,// BList<long?> QlValue
            UsingTableRowSet = -460; // long TableRowSet
        internal long urlCol => (long)(mem[UrlCol] ?? -1L);
        internal long template => (long)(mem[RestTemplate]??-1L);
        internal BList<long?> usingCols =>
            (BList<long?>?)mem[UsingCols]??BList<long?>.Empty;
        internal long usingTableRowSet => (long)(mem[UsingTableRowSet] ?? -1L);
        public RestRowSetUsing(Iix lp,Context cx,RestView vw, RestRowSet rs,
            TableRowSet uf)  :base(lp.dp,_Mem(cx,rs,uf) 
                 + (RestTemplate, rs.defpos) + (ObInfo.Name, vw.NameFor(cx))
                 + (RSTargets,new BTree<long, long?>(vw.defpos, lp.dp))
                 + (UsingTableRowSet, uf.defpos)  
                 + (ISMap, uf.iSMap) + (SIMap,uf.sIMap)
                 + (SRowType, uf.sRowType))
        {
            cx.Add(this);
        }
        protected RestRowSetUsing(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,RestRowSet rs, TableRowSet uf)
        {
            var r = BTree<long, object>.Empty;
            rs += (cx,UsingTableRowSet, uf.defpos);
            cx.Add(rs);
            var ab = uf.rowType.Last();
            var ul = ab?.value()??throw new DBException("42105").Add(Qlx.USING);
            var uc = uf.rowType - ab.key();
            return r + (UsingCols,uc)+(UrlCol,ul) + (RowType,rs.rowType) 
                + (Representation,rs.representation) + (_Depth,rs.depth+1)
                + (ObInfo.Names,rs.names+uf.names);
        }
        public static RestRowSetUsing operator +(RestRowSetUsing et, (long, object) x)
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
            return (RestRowSetUsing)et.New(m + x);
        }
        public static RestRowSetUsing operator +(RestRowSetUsing rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (RestRowSetUsing)rs.New(m + (p, o));
        }
        internal override CTree<long,bool> Sources(Context cx)
        {
            return base.Sources(cx) + (usingTableRowSet,true) + (template,true);
        }
        internal override BTree<long, RowSet> AggSources(Context cx)
        {
            var t = template;
            return new BTree<long, RowSet>(t, (RowSet?)cx.obs[t]??throw new DBException("42105").Add(Qlx.TABLE)); // not usingTableRowSet
        }
        internal override CTree<long, Domain> _RestSources(Context cx)
        {
            return ((RowSet?)cx.obs[template])?._RestSources(cx)??CTree<long,Domain>.Empty;
        }
        internal override bool Knows(Context cx, long rp, bool ambient = false)
        {
            if (cx.obs[usingTableRowSet] is TableRowSet ur &&
                ur.representation.Contains(rp) == true) // really
                return false;
            return base.Knows(cx, rp, ambient);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        /// <summary>
        /// Following Build, cx.values[defpos] is an array of RowSetUsingCursor.
        /// If there are aggregations going on, they are gathered together by a parent SelectRowSet
        /// and we don't need to worry about them. If the RestRowSet has sent back grouped rows,
        /// don't worry because the register mechanism is taking account of how the functions
        /// accumulate.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override RowSet Build(Context cx)
        {
            if (Built(cx))
                return this;
            cx.values += (defpos, new TList(this));
            var pos = 0;
            var a = (TList?)cx.values[defpos] ?? new TList(this);
            if (cx.obs[template] is RestRowSet rr && 
                cx.obs[usingTableRowSet] is TableRowSet ru)
            for (var uc = (TableRowSet.TableCursor?)ru.First(cx); uc != null;
                uc = (TableRowSet.TableCursor?)uc.Next(cx))
            {
                cx.values += uc.values;
                cx.cursors += (ru.from, uc);
                cx.url = uc[urlCol].ToString();
                for (var rc = (RestRowSet.RestCursor?)rr.First(cx); rc != null;
                    rc = (RestRowSet.RestCursor?)rc.Next(cx))
                    a += new RestUsingCursor(cx, this, pos++, uc, rc);
                cx.obs += (rr.defpos, rr - RestRowSet.RestValue - _Built); // doesn't change depth
                cx.values -= uc.values;
            }
            cx.values += (defpos, a);
            cx.url = null;
            return base.Build(cx);
        }
        internal override RowSet Apply(BTree<long,object>mm,Context cx,BTree<long,object>? im=null)
        {
            if (mm == BTree<long, object>.Empty)
                return this;
 /*           if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            } */
            var ru = (RestRowSetUsing)base.Apply(mm,cx);
            // Watch out for situation where an aggregation has successfully moved to the Template
            // as the having condition will no longer work at the RestRowSetUsing level
            var re = (RestRowSet?)cx.obs[ru.template]??throw new DBException("42105").Add(Qlx.VIEW);
            var uh = ru.having;
            for (var b = uh.First(); b != null; b = b.Next())
                if (re.having.Contains(b.key()))
                    uh -= b.key();
            if (uh != ru.having)
                ru += (cx,Having, uh);
            return ru;
        }
        protected override Cursor? _First(Context _cx)
        {
            return RestUsingCursor.New(_cx, this);
        }
        protected override Cursor? _Last(Context _cx)
        {
            return RestUsingCursor.New(this, _cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestRowSetUsing(defpos, m);
        }

        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new RestRowSetUsing(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RestRowSetUsing)base._Replace(cx, so, sv);
            r += (cx,UsingTableRowSet, cx.ObReplace(usingTableRowSet, so, sv));
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nu = cx.Fix(usingTableRowSet);
            if (nu != usingTableRowSet)
                r += (UsingTableRowSet, nu);
            return r;
        }
        /// <summary>
        /// It makes no sense to use the RestView to alter the contents of the using table.
        /// But the permission to insert remote rows may have been granted
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="ts"></param>
        /// <param name="rt"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, Domain rt)
        {
            return ((RowSet?)cx.obs[template])?.Insert(cx, ts, rt) ?? throw new DBException("42105").Add(Qlx.INSERT);
        }
        /// <summary>
        /// It makes no sense to use the RestView to alter the contents of the using table.
        /// Lots of use cases suggest update of remote data will be useful.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="fm"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Update(Context cx, RowSet fm)
        {
            return ((RowSet?)cx.obs[template])?.Update(cx, fm) ?? throw new DBException("42105").Add(Qlx.UPDATE);
        }
        /// <summary>
        /// It makes no sense to use the RestView to alter the contents of the using table.
        /// But the permission to delete remote rows may have been granted.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="fm"></param>
        /// <param name="iter"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Delete(Context cx, RowSet fm)
        {
            return ((RowSet?)cx.obs[template])?.Delete(cx, fm) ?? throw new DBException("42105").Add(Qlx.DELETE);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Template: ");sb.Append(Uid(template));
            sb.Append(" UsingTableRowSet:"); sb.Append(Uid(usingTableRowSet));
            sb.Append(" UrlCol:"); sb.Append(Uid(urlCol));
            if (mem.Contains(Having))
            {
                sb.Append(" having (");
                var cm = "";
                for (var b = having.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
        }
        internal class RestUsingCursor : Cursor
        {
            internal readonly RestRowSetUsing _ru;
            internal readonly TableRowSet.TableCursor _tc;
            internal RestUsingCursor(Context cx,RestRowSetUsing ru,int pos,
                TableRowSet.TableCursor tc,RestRowSet.RestCursor rc)
                :base(cx,ru,pos,tc._ds,_Value(ru,tc,rc))
            {
                _ru = ru; _tc = tc; 
            }
            static TRow _Value(RestRowSetUsing ru,
                TableRowSet.TableCursor tc, RestRowSet.RestCursor rc)
            {
                return new TRow(ru, tc.values + rc.values - ru.defpos);
            }
            static RestUsingCursor? Get(Context cx, RestRowSetUsing ru, int pos)
            {
                var ls = ((TList?)cx.values[ru.defpos])?.list;
                if (ls==null || pos < 0 || pos >= ls.Length)
                    return null;
                var cu = (RestUsingCursor?)ls[pos];
                if (cu != null)
                {
                    cx.cursors += (cu._tc._rowsetpos, cu._tc);
                    cx.cursors += (ru.defpos, cu);
                    cx.values += cu.values;
                    for (var b = ru.aggs.First(); b != null; b = b.Next())
                        cx.values -= b.key();
                    for (var b = ru.sRowType.First(); b != null; b = b.Next())
                        if (b.value() is long p)
                            cx.values += (p, cu[b.key()]);
                }
                return cu;
            }
            internal static Cursor? New(Context cx, RestRowSetUsing ru)
            {
                return Get(cx,ru,0);
            }
            internal static Cursor? New(RestRowSetUsing ru, Context cx)
            {
                var ls = ((TList?)cx.values[ru.defpos])?.list;
                if (ls == null || ls.Length==0)
                    return null;
                var n = ls.Length - 1;
                return Get(cx,ru,n);
            }
            protected override Cursor? _Next(Context cx)
            {
                return Get(cx, _ru, _pos + 1);
            }
            protected override Cursor? _Previous(Context cx)
            {
                return Get(cx, _ru, _pos - 1);
            }
            internal override BList<TableRow> Rec()
            {
                return (_tc._rec==null)?BList<TableRow>.Empty:new BList<TableRow>(_tc._rec);
            }

        }
    } 
    // An ad-hoc RowSet for a row history: the work is mostly done by
    // LogRowsCursor
    
    internal class LogRowsRowSet : RowSet
    {
        internal const long
            TargetTable = -369; // long Table
        public long targetTable => (long)(mem[TargetTable] ?? -1L);
        public LogRowsRowSet(long dp, Context cx, long td)
            : base(dp, _Mem(cx, td))
        {
            cx.db = cx.db.BuildLog();
            cx.Add(this);
        }
        protected LogRowsRowSet(long dp,BTree<long,object> m) :base(dp,m)
        { }
        static BTree<long,object> _Mem(Context cx, long td)
        {
            var tb = cx.db.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix();
            cx.Add(tb);
            var rt = BList<DBObject>.Empty;
            rt += new SqlFormal(cx, "Pos", Position);
            rt += new SqlFormal(cx, "Action", Char);
            rt += new SqlFormal(cx, "DefPos", Position);
            rt += new SqlFormal(cx, "Transaction", Position);
            rt += new SqlFormal(cx, "Timestamp", Timestamp);
            var dm = new Domain(Qlx.TABLE, cx, rt);
            return dm.mem +(TargetTable, tb.defpos)
                +(Table.LastData,tb.lastData);
        }
        public static LogRowsRowSet operator +(LogRowsRowSet et, (long, object) x)
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
            return (LogRowsRowSet)et.New(m + x);
        }
        public static LogRowsRowSet operator +(LogRowsRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (LogRowsRowSet)rs.New(m + (p, o));
        }
        protected override Cursor? _First(Context _cx)
        {
            return LogRowsCursor.New(_cx, this,0,targetTable);
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException(); // someday maybe
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new LogRowsRowSet(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowsRowSet)base._Replace(cx, so, sv);
            r +=(cx, TargetTable, cx.ObReplace(targetTable, so, sv));
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LogRowsRowSet(defpos, m);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" for "); sb.Append(Uid(targetTable));
        }
        
        internal class LogRowsCursor : Cursor
        {
            readonly LogRowsRowSet _lrs;
            readonly TRow _row;
            LogRowsCursor(Context cx,LogRowsRowSet rs,int pos,TRow rw)
                :base(cx,rs,pos,E,rw)
            {
                _lrs = rs; _row = rw;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowsCursor(cx, _lrs, _pos, _row + (p, v));
            }
            internal static LogRowsCursor? New(Context cx,LogRowsRowSet lrs,int pos,long p)
            {
                if (cx.db==null)
                    return null;
                Database db = cx.db;
                Physical? ph;
                for (var b = cx.db.log.PositionAt(p); b != null; b = b.Next())
                {
                    var vs = CTree<long, TypedValue>.Empty;
                    p = b.key();
                    switch (b.value())
                    {
                        case Physical.Type.Record:
                         case Physical.Type.Record2:
                        case Physical.Type.Record3:
                            {
                                (ph, _) = db._NextPhysical(b.key());
                                var rc = ph as Record;
                                if (rc?.tabledefpos == lrs.targetTable)
                                    for (var c = lrs.rowType.First(); c != null; c = c.Next())
                                        if (c.value() is long cp)
                                            switch (c.key())
                                            {
                                                case 0: vs += (cp, new TPosition(p)); break;
                                                case 1: vs += (cp, new TChar("Insert")); break;
                                                case 2: vs += (cp, new TPosition(rc.defpos)); break;
                                                case 3: vs += (cp, new TPosition(rc.trans)); break;
                                                case 4:
                                                    vs += (cp, new TDateTime(new DateTime(rc.time)));
                                                    break;
                                            }
                                return new LogRowsCursor(cx, lrs, pos,
                                    new TRow(lrs, vs));
                            }
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                (ph, _) = db._NextPhysical(b.key());
                                var rc = ph as Record;
                                if (rc?.tabledefpos==lrs.targetTable)
                                for (var c = lrs.rowType.First(); c != null; c = c.Next())
                                    if (c.value() is long cp)
                                        vs += c.key() switch
                                        {
                                            0 => (cp, new TPosition(p)),
                                            1 => (cp, new TChar("Update")),
                                            2 => (cp, new TPosition(rc.defpos)),
                                            3 => (cp, new TPosition(rc.trans)),
                                            4 => (cp, new TDateTime(new DateTime(rc.time))),
                                            _ => (cp, TNull.Value)
                                        };
                                return new LogRowsCursor(cx, lrs, pos,
                                    new TRow(lrs, vs));
                            }
                        case Physical.Type.Delete:
                        case Physical.Type.Delete1:
                            {
                                (ph, _) = db._NextPhysical(b.key());
                                if (ph is Delete rc && rc.tabledefpos == lrs.targetTable)
                                    for (var c = lrs.rowType.First(); c != null; c = c.Next())
                                        if (c.value() is long cp)
                                            vs += c.key() switch
                                            {
                                                0 => (cp, new TPosition(p)),
                                                1 => (cp, new TChar("Delete")),
                                                2 => (cp, new TPosition(rc.ppos)),
                                                3 => (cp, new TPosition(rc.trans)),
                                                4 => (cp, new TDateTime(new DateTime(rc.time))),
                                                _ => (cp, TNull.Value),
                                            };
                                return new LogRowsCursor(cx, lrs, pos, new TRow(lrs, vs));
                            }
                        case Physical.Type.PTransaction:
                            {
                                db._NextPhysical(b.key());
                                break;
                            }
                    }
                }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,-1L);
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException(); // someday maybe
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
    /// <summary>
    /// An Ad-hoc RowSet for a row,column history: the work is mostly done by
    /// LogRowColCursor
    /// 
    /// </summary>
    internal class LogRowColRowSet : RowSet
    {
        internal const long
            LogCol = -337, // long TableColumn
            LogRow = -339; // long TableRow
        public long targetTable => (long)(mem[LogRowsRowSet.TargetTable] ?? -1L);
        public long logCol => (long)(mem[LogCol] ?? -1L);
        public long logRow => (long)(mem[LogRow] ?? -1L);
        public LogRowColRowSet(long dp, Context cx, long r, long c)
        : base(dp, _Mem(cx, c) + (LogRow, r) + (LogCol, c))
        {
            cx.Add(this);
        }
        protected LogRowColRowSet(long dp,BTree<long,object>m) :base(dp,m)
        { }
        static BTree<long,object> _Mem(Context cx, long cd)
        {
            var db = cx.db ?? throw new DBException("42105").Add(Qlx.DATABASE);
            var tc = db.objects[cd] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            cx.Add(tc);
            var tb = db.objects[tc.tabledefpos] as Table 
                ?? throw new PEException("PE1502");
            cx.Add(tb);
            var rt = BList<DBObject>.Empty;
            rt += new SqlFormal(cx, "Pos", Char);
            rt += new SqlFormal(cx, "Value", Char);
            rt += new SqlFormal(cx, "StartTransaction", Char);
            rt += new SqlFormal(cx, "StartTimestamp", Timestamp);
            rt += new SqlFormal(cx, "EndTransaction", Char);
            rt += new SqlFormal(cx, "EndTimestamp", Timestamp);
            return new Domain(Qlx.TABLE,cx,rt).mem
                + (LogRowsRowSet.TargetTable, tb.defpos)
                + (Table.LastData,tb.lastData);
        }
        public static LogRowColRowSet operator +(LogRowColRowSet et, (long, object) x)
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
            return (LogRowColRowSet)et.New(m + x);
        }
        public static LogRowColRowSet operator +(LogRowColRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            m = rs._Depths(cx, m, d, p, o);
            return (LogRowColRowSet)rs.New(m + (p, o));
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" for "); sb.Append(targetTable);
            sb.Append('(');  sb.Append(Uid(logRow)); 
            sb.Append(',');  sb.Append(Uid(logCol)); 
            sb.Append(')'); 
        }
        internal override Basis New(BTree<long,object> m)
        {
            return new LogRowColRowSet(defpos, m);
        }
        protected override Cursor? _First(Context cx)
        {
            return LogRowColCursor.New(cx,this,0,(null,logRow));
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException(); // someday maybe
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new LogRowColRowSet(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowColRowSet)base._Replace(cx, so, sv);
            r += (LogRowsRowSet.TargetTable, cx.ObReplace(targetTable, so, sv));
            r += (cx, LogCol, cx.ObReplace(logCol, so, sv));
            return r;
        }
        
        internal class LogRowColCursor : Cursor
        {
            readonly LogRowColRowSet _lrs;
            readonly (Physical?, long) _next;
            LogRowColCursor(Context cx, LogRowColRowSet lrs, int pos, 
                (Physical?, long) next, TRow rw) 
                : base(cx, lrs, pos, E, rw) 
            {
                _lrs = lrs; _next = next;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowColCursor(cx, _lrs, _pos, _next, 
                    this + (p, v));
            }
            internal static LogRowColCursor? New(Context cx,LogRowColRowSet lrs,int pos,
                (Physical?, long) nx, PTransaction? trans = null)
            {
                var vs = CTree<long, TypedValue>.Empty;
                if (cx.db==null)
                    throw new PEException("PE48180");
                var tc = lrs.logCol;
                var rp = nx.Item2;
                if (rp < 0 || cx.db==null)
                    return null;
                if (nx.Item1==null)
                    nx = cx.db._NextPhysical(rp);
                for (; nx.Item1 != null;)
                    if (nx.Item1 is Record rc && lrs.logRow == rc.defpos && !rc.fields.Contains(tc))
                    {                    // may be an Update 
                        var b = lrs.rowType.First();
                        if (b != null && b.value() is long p1)
                        {
                            vs += (p1, new TPosition(rc.ppos));
                            b = b.Next();
                        }
                        if (b != null && b.value() is long p2)
                        {
                            vs += (p2, rc.fields[tc] ?? TNull.Value);
                            b = b.Next();
                        }
                        if (b != null && b.value() is long p3)
                        {
                            vs += (p3, new TPosition(rc.trans));
                            b = b.Next();
                        }
                        if (b != null && b.value() is long p4)
                        {
                            vs += (p4, new TDateTime(new DateTime(rc.time)));
                            b = b.Next();
                        }
                        var done = false;
                        for (nx = cx.db._NextPhysical(nx.Item2);
                            nx.Item1 != null && nx.Item2 >= 0;
                            nx = cx.db._NextPhysical(nx.Item2))
                        {
                            var (ph, _) = nx;
                            switch (ph.type)
                            {
                                case Physical.Type.Delete:
                                case Physical.Type.Delete1:
                                    done = ((Delete)ph).delpos == rp;
                                    break;
                                case Physical.Type.Update:
                                case Physical.Type.Update1:
                                    {
                                        var up = (Update)ph;
                                        done = up.defpos == rp && up.fields.Contains(tc);
                                        break;
                                    }
                                case Physical.Type.PTransaction:
                                    trans = (PTransaction)ph;
                                    break;
                            }
                            if (done)
                                break;
                        }
                        if (done && trans != null && b != null && b.value() is long p)
                        {
                            vs += (p, new TPosition(trans.ppos));
                            b = b.Next();
                            if (b != null && b.value() is long pp)
                                vs += (pp, new TDateTime(new DateTime(trans.pttime)));
                        }
                        var rb = new LogRowColCursor(cx, lrs, pos, nx, new TRow(lrs, vs));
                        if (rb.Matches(cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,_next);
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException(); // someday maybe
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
}