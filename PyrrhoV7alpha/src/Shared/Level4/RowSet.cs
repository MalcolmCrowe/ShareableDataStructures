using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Net;
using System.Runtime.Intrinsics.Arm;
using System.Security.Cryptography;
using System.Text;
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
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal abstract class RowSet : DBObject
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
            Asserts = -212, // Assertions
            Assig = -174, // CTree<UpdateAssignment,bool> 
            _Built = -402, // bool
            _CountStar = -281, // long 
            _Data = -368, // long RowSet
            Distinct = -239, // bool
            Filter = -180, // CTree<long,TypedValue> matches to be imposed by this query
            Group = -199, // long GroupSpecification
            Groupings = -406,   //BList<long?>   Grouping
            GroupCols = -461, // Domain
            GroupIds = -408, // CTree<long,Domain> temporary during SplitAggs
            Having = -200, // CTree<long,bool> SqlValue
            ISMap = -213, // BTree<long,long?> SqlValue,TableColumn
            _Matches = -182, // CTree<long,TypedValue> matches guaranteed elsewhere
            Matching = -183, // CTree<long,CTree<long,bool>> SqlValue SqlValue (symmetric)
            OrdSpec = -184, // Domain
            Periods = -185, // BTree<long,PeriodSpec>
            UsingOperands = -411, // BTree<long,long?> SqlValue
            Referenced = -378, // CTree<long,bool> SqlValue (referenced columns)
            _Repl = -186, // CTree<string,string> Sql output for remote views
            RestRowSetSources = -331, // CTree<long,bool>    RestRowSet or RestRowSetUsing
            _Rows = -407, // CList<TRow> 
            RowOrder = -404, // Domain
            RSTargets = -197, // BTree<long,long?> Table TableRowSet 
            SIMap = -214, // BTree<long,long?> TableColumn,SqlValue
            Scalar = -95, // bool
            _Source = -151, // RowSet
            Static = -152, // RowSet (defpos for STATIC)
            Stem = -211, // CTree<long,bool> RowSet 
            Target = -153, // long (a table or view for simple IDU ops)
            _Where = -190, // CTree<long,bool> Boolean conditions to be imposed by this query
            Windows = -201; // CTree<long,bool> WindowSpecification
        internal Assertions asserts => (Assertions)(mem[Asserts] ?? Assertions.None);
        internal new string? name => (string?)mem[ObInfo.Name];
        internal BTree<string, long?> names => // -> SqlValue
    (BTree<string, long?>?)mem[ObInfo.Names] ?? BTree<string, long?>.Empty;
        /// <summary>
        /// indexes are added where the targets are selected from tables, restviews, and INNER or CROSS joins.
        /// indexes are not added for unions or outer joins
        /// </summary>
        internal CTree<Domain, CTree<long, bool>> indexes => // as defined for tables and restview targets
            (CTree<Domain, CTree<long, bool>>?)mem[Table.Indexes]??CTree<Domain, CTree<long, bool>>.Empty;
        /// <summary>
        /// keys are added where the current set of columns includes all the keys from a target index or ordering.
        /// At most one keys entry is currently allowed: ordering if available, then target index
        /// </summary>
        internal Domain keys => (Domain?)mem[Level3.Index.Keys] ?? Domain.Row; 
        internal Domain ordSpec => (Domain?)mem[OrdSpec] ?? Domain.Row;
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>?)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal Domain rowOrder => (Domain?)mem[RowOrder] ?? Domain.Row;
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
        internal long target => (long)(mem[Target] // for table-focussed RowSets
            ?? rsTargets.First()?.key() ?? -1L); // for safety
        internal BTree<long, long?> rsTargets =>
            (BTree<long, long?>?)mem[RSTargets] ?? BTree<long, long?>.Empty;
        internal int selectDepth => (int)(mem[SqlValue.SelectDepth] ?? -1);
        internal long source => (long)(mem[_Source] ??  -1L);
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>?)mem[Assig]
            ?? CTree<UpdateAssignment, bool>.Empty;
        internal BTree<long, long?> iSMap =>
            (BTree<long, long?>?)mem[ISMap] ?? BTree<long, long?>.Empty;
        internal BTree<long, long?> sIMap =>
            (BTree<long, long?>?)mem[SIMap] ?? BTree<long, long?>.Empty;
        internal BList<TRow> rows =>
            (BList<TRow>?)mem[_Rows] ?? BList<TRow>.Empty;
        internal long lastData => (long)(mem[Table.LastData] ?? 0L);
        internal BList<long?> remoteCols =>
            (BList<long?>?)mem[RestRowSet.RemoteCols] ?? BList<long?>.Empty;
        /// <summary>
        /// The group specification
        /// </summary>
        internal long group => (long)(mem[Group] ?? -1L);
        internal BList<long?> groupings =>
            (BList<long?>?)mem[Groupings] ?? BList<long?>.Empty;
        internal Domain groupCols => (Domain)(mem[GroupCols]??Domain.Null);
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
        internal bool scalar => (bool)(mem[Scalar] ?? false);
        /// <summary>
        /// Constructor: a new or modified RowSet
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="cx">The processing environment</param>
        /// <param name="m">The properties list</param>
        protected RowSet(long dp, Context cx, BTree<long, object> m)
            : base(dp, _Mem(cx,m))
        {
            cx.Add(this);
        }
        /// <summary>
        /// special version for RowSet constructors that compute asserts
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="dp"></param>
        /// <param name="m"></param>
        protected RowSet(Context cx,long dp,BTree<long,object>m) :base(dp,m)
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
        // Compute assertions. Also watch for Matching info from sources
        protected static BTree<long, object> _Mem(Context cx, BTree<long, object> m)
        {
            var dp = (long)(m[_Domain] ?? -1L);
            var dm = (Domain)(cx.obs[dp] ?? cx.db.objects[dp] ?? Domain.Null);
            var de = (int)(m[_Depth]??1);
            var md = de;
            if (dm == null)
            {
                dp = (long)(m[Target] ?? -1L);
                var ob = cx._Ob(dp);
                dm = cx._Dom(ob);
                if (dm!=null)
                    de = Math.Max(de, dm.depth + 1);
            }
            var a = Assertions.SimpleCols | 
                (((Assertions)(m[Asserts] ?? Assertions.None)) & Assertions.SpecificRows);
            if (cx.obs[(long)(m[_Source] ?? m[_Data] ?? -1L)] is RowSet sce && cx._Dom(sce) is Domain sd)
            {
                de = Math.Max(de, sce.depth + 1);
                var sb = sd?.rowType.First();
                for (var b = dm?.rowType.First(); b != null && sb!=null; b = b.Next(), sb = sb?.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && cx._Dom(v) is Domain vd &&
                        sb.value() is long sp && cx.obs[sp] is SqlValue sv && cx._Dom(sv) is Domain ds) {
                    if (!(v is SqlCopy || v is SqlLiteral))
                        a &= ~(Assertions.MatchesTarget | Assertions.ProvidesTarget
                            | Assertions.SimpleCols);
                    if (sb == null ||
                        !vd.CanTakeValueOf(ds))
                        a &= ~Assertions.AssignTarget;
                    if (sb == null || v.defpos != sv.defpos)
                        a &= ~Assertions.MatchesTarget;
                    if (sce != null && sd!=null && !sd.representation.Contains(v.defpos))
                        a &= ~Assertions.ProvidesTarget;
                }
            }
            m += (Asserts, a);
            //   var ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            //   if (sce!=null && (ma.Count > 0 || sce.matching.Count > 0))
            //       m += (Matching, sce.CombineMatching(ma, sce.matching));
            if (de != md)
                m += (_Depth, de);
            return m;
        }
        internal virtual Assertions Requires => Assertions.None;
        public static RowSet operator +(RowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (RowSet)rs.New(rs.mem + x);
        }
        internal virtual BList<long?> SourceProps => BList<long?>.FromArray(_Source,
            JoinRowSet.JFirst, JoinRowSet.JSecond,
            MergeRowSet._Left, MergeRowSet._Right,
            RestRowSetUsing.RestTemplate,RestView.UsingTableRowSet);
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
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n)
        {
            if (cx._Dom(defpos) is not Domain dm)
                throw new DBException("42105");
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is DBObject co && co.NameFor(cx) == nm && n!=null)
                {
                    var ob = new SqlValue(n, dm.representation[co.defpos],
                        new BTree<long, object>(_From, defpos));
                    cx.Add(ob);
                    return (ob, n.sub);
                }
            return base._Lookup(lp, cx, nm, n);
        }
        internal virtual RowSet Sort(Context cx,Domain os,bool dct)
        {
            if (os.CompareTo(rowOrder)==0) // skip if current rowOrder already finer
                return this;
            return new OrderedRowSet(cx, this, os, dct);
        }
        /// <summary>
        ///  Limit the list of proposed changes to what r knows
        /// </summary>
        /// <param name="mm">The list of changes</param>
        /// <param name="p">The property to consider</param>
        /// <param name="cx">The context</param>
        /// <param name="r">The rowset involved</param>
        /// <returns>A reverse set of requests</returns>
        internal BTree<long,object>
            CanApply(BTree<long,object> mm,long p,Context cx, RowSet r)
        {
            var v = mm[p];
            if (v is CTree<long, bool> tv && tv.Count > 0)
            {
                var a = CTree<long, bool>.Empty;
                for (var b = tv.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValue sv)
                    {
                        if (sv.KnownBy(cx, r))
                            a += (sv.defpos, true);
                        else if (this is RestRowSet rr && cx.obs[rr.usingTableRowSet] is RowSet ut
                            && sv.KnownBy(cx, ut))
                            a += (sv.defpos, true);
                        else // allow lateral
                        {
                            if (r.id != null && sv.id!=null)
                                for (var c = sv.Needs(cx, r.defpos).First(); c != null; c = c.Next())
                                    if (sv.id.iix.lp < 0 || sv.id.iix.lp > r.id.iix.lp)
                                        goto no;
                            a += (sv.defpos, true);
                        no:
                            cx.forReview += (sv.defpos, (cx.forReview[sv.defpos] ?? CTree<long, bool>.Empty) + (defpos, true));
                        }
                    }
                mm = (a == tv) ? mm : (a.Count == 0) ? (mm - p) : (mm + (p, a));
            }
            if (v is CTree<long, TypedValue> vv && vv.Count > 0)
            {
                var a = CTree<long, TypedValue>.Empty;
                for (var b = vv.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue sv && sv.KnownBy(cx, r))
                        a += (sv.defpos, b.value());
                mm = (a == vv) ? mm : (a.Count == 0) ? (mm - p) : (mm + (p, a));
            }
            else if (v is BList<long?> lv && lv.Count > 0)
            {
                var a = BList<long?>.Empty;
                for (var b = lv.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue sv && sv.KnownBy(cx, r))
                        a += sv.defpos;
                mm = (a == lv) ? mm : (a.Count == 0) ? (mm - p) : (mm + (p, a));
            }
            return mm;
        }
        /// <summary>
        /// A change to some properties requiring further actions in general.
        /// For most properties, we can readily check that it can be
        /// applied to this rowset. 
        /// 
        /// Passing expressions down to source rowsets:
        /// A non-aggregating expression R (in the select list or a where condition) can be passed down to 
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
        /// <param name="m">PRIVATE: For accumulating properties of the result</param>
        /// <returns>updated rowset</returns>
        internal virtual RowSet Apply(BTree<long, object> mm,Context cx,BTree<long,object>? m = null)
        {
            m ??= mem;
            for (var b=mm.First();b!=null;b=b.Next())
            {
                var k = b.key();
                if (m[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return (RowSet)New(cx,m);
            var od = cx.done;
            cx.done = ObTree.Empty;
            if (cx._Dom((long)(m[_Domain]??-1L)) is not Domain dm)
                throw new PEException("PE29400");
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
                                var mw = (CTree<long, bool>?)mm[_Where] ?? CTree<long, bool>.Empty;
                                var mh = (CTree<long, bool>?)mm[Having] ?? CTree<long, bool>.Empty;
                                for (var b = w.First(); b != null; b = b.Next())
                                    if (cx.obs[b.key()] is SqlValue sv)
                                    {
                                        var k = b.key();
                                        var matched = false;
                                        if (sv is SqlValueExpr se && se.kind == Sqlx.EQL)
                                            if (cx.obs[se.left] is SqlValue le &&
                                                cx.obs[se.right] is SqlValue ri)
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
                                        if (sv.KnownBy(cx, this, true)) // allow ambient values
                                        {
                                            if (sv.IsAggregation(cx) != CTree<long, bool>.Empty)
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
                                if (ma != oa)
                                    mm += (_Matches, ma);
                                var ow = (CTree<long, bool>?)m[_Where] ?? CTree<long, bool>.Empty;
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
                                        return new EmptyRowSet(defpos, cx, dm.defpos);
                                    }
                                    ms += (sp, b.value());
                                    var me = CTree<long, CTree<long, bool>>.Empty;
                                    var mg = (CTree<long, CTree<long, bool>>?)m[Matching] ?? me;
                                    var nm = (CTree<long, CTree<long, bool>>?)mm[Matching] ?? me;
                                    nm = CombineMatching(mg, nm);
                                    for (var c = nm[sp]?.First(); c != null; c = c.Next())
                                    {
                                        var ck = c.key();
                                        if (dm.representation.Contains(ck) && !ma.Contains(ck))
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
                        case _Domain:
                            {
                                recompute = true;
                                m += (p, v);
                                break;
                            }
                        case _Source:
                            {
                                recompute = true;
                                m += (_Source, v);
                                mm -= _Source;
                                break;
                            }
                        case Assig:
                            {
                                var sg = (CTree<UpdateAssignment, bool>)v;
                                for (var b = sg.First(); b != null; b = b.Next())
                                {
                                    var ua = b.key();
                                    if ((!Knows(cx, ua.vbl, true))
                                        || !((SqlValue?)cx.obs[ua.val])?.KnownBy(cx, this, true) == true)
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
                                var rd = (Domain?)cx.obs[(long)(m[_Domain] ?? -1L)] ?? Domain.Content;
                                var rt = rd.rowType;
                                var rs = rd.representation;
                                // We need to ensure that the source rowset supplies all of the restOperands
                                // we now know about.
                                var ch = false;
                                for (var b = nr.First(); b != null; b = b.Next())
                                    if (cx.obs[b.key()] is SqlValue s &&
                                        cx._Dom(s) is Domain sd && !rd.representation.Contains(s.defpos))
                                    {
                                        rt += s.defpos;
                                        rs += (s.defpos, sd);
                                        ch = true;
                                    }
                                rd = (Domain)cx.Add(rd + (Domain.RowType, rt) + (Domain.Representation, rs));
                                if (ch)
                                {
                                    cx.Add(rd);
                                    m += (_Domain, rd.defpos);
                                }
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
                    if (b.value() is long p && m[p] is long sp && sp > 0) // we have this kind of source rowset
                    {
                        var ms = mm;
                        if (p == RestView.UsingTableRowSet)
                            ms = ms - Target - _Domain;
                        if (cx.obs[sp] is RowSet sc)
                        {
                            sc = sc.Apply(ms, cx);
                            if (sc.defpos != sp)
                                m += (p, sc.defpos);
                        }
                    }
            if (recompute)
                m = _Mem(cx, m);
            var r = this;
            if (m != mem)
            {
                r = (RowSet)New(cx,m);
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
        /// If we construct a new source rowset S', rowsets T between R and S' in the pipeline must get new Domains.
        /// These are built bottom-up.
        /// Selectlist expressions in T should be the subexpressions of R's select list that it knows.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="nd"></param>
        internal void ApplyT(Context cx, RowSet sa, CTree<long, Domain> es)
        {
            for (var b = Sources(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is RowSet s)
                {
                    if (s is SelectRowSet || s is RestRowSet
                        || (this is RestRowSetUsing ru && ru.usingTableRowSet == s.defpos)
                        || !s.AggSources(cx).Contains(sa.defpos))
                        continue;
                    var rc = BList<long?>.Empty;
                    var rs = CTree<long, Domain>.Empty;
                    var kb = s.KnownBase(cx);
                    for (var c = es.First(); c != null; c = c.Next())
                        if (cx.obs[c.key()] is SqlValue sc)
                            for (var d = sc.KnownFragments(cx, kb).First();
                                  d != null; d = d.Next())
                                if (cx.obs[d.key()] is SqlValue v && rs.Contains(d.key()))
                                {
                                    var p = d.key();
                                    rc += p;
                                    rs += (p, cx._Dom(v)??Domain.Null);
                                }
                    if (s is RestRowSetUsing)
                        for (var c = cx._Dom(sa)?.aggs.First(); c != null; c = c.Next())
                            if (!rs.Contains(c.key()) && cx.obs[c.key()] is SqlValue v)
                            {
                                rc += v.defpos;
                                rs += (v.defpos, cx._Dom(v)??Domain.Null);
                            }
                    var sd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rc);
                    cx.Add(sd);
                    cx.Add(s + (_Domain, sd.defpos));
                    s.ApplyT(cx, sa, es);
                }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        internal CTree<long,Domain> KnownBase(Context cx)
        {
            var kb = CTree<long, Domain>.Empty; 
            // we know the sources
            for (var b = Sources(cx).First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is RowSet s && cx._Dom(s) is Domain sd) { 
                kb += sd.representation;
                kb += s.KnownBase(cx);
            }
            return kb;
        }
        internal virtual CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            if (cx.obs[source] is RowSet sc)
                for (var b = ag.First(); b != null; b = b.Next())
                    for (var c = ((SqlValue?)cx.obs[b.key()])?.Operands(cx).First();
                        c != null; c = c.Next())
                        if (sc.Knows(cx, c.key()))
                            ma += (b.key(), true);
            return ma;
        }
        internal static CTree<long,CTree<long,bool>> 
            CombineMatching(CTree<long, CTree<long, bool>>p, CTree<long, CTree<long, bool>>q)
        {
            for (var b=q.First();b!=null;b=b.Next())
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
            var dm = cx._Dom(this)??Domain.Content;
            if (a.HasFlag(Assertions.MatchesTarget))
                return new TRow(this, dm, sce);
            if (a.HasFlag(Assertions.ProvidesTarget))
                return new TRow(dm, sce.values);
            if (CanAssign() && a.HasFlag(Assertions.AssignTarget))
                return new TRow(sce, dm);
            var vs = CTree<long, TypedValue>.Empty;
            var oc = cx.values;
            cx.values = sce.values;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var s = cx.obs[p];
                    var v = s?.Eval(cx) ?? TNull.Value;
                    if (v == TNull.Value && sce[p] is TypedValue tv
                        && tv != TNull.Value)
                        v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
                    vs += (p, v);
                }
            cx.values = oc;
            return new TRow(dm, vs);
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
        internal SqlValue MaybeAdd(Context cx, SqlValue su)
        {
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
              if (b.value() is long p && cx.obs[p] is SqlValue sv && sv._MatchExpr(cx, su, this))
                    return sv;
            su += (_Alias, alias ?? name ?? cx.Alias());
            Add(cx, su);
            return su;
        }
        internal RowSet AddUpdateAssignment(Context cx, UpdateAssignment ua)
        {
            var s = (long)(mem[_Source] ?? -1L);
            if (cx.obs[s] is RowSet rs && rs.Knows(cx,ua.vbl)
                && cx.obs[ua.val] is SqlValue v && v.KnownBy(cx, rs))
                rs.AddUpdateAssignment(cx, ua);
            var r = this + (Assig, assig + (ua, true));
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
            if (cx.db.autoCommit == true && cx._Dom(this) is Domain dm)
            {
                var d = dm.display;
                if (d == 0)
                    d = int.MaxValue;
                for (var b = dm.rowType.First(); b != null && d-- > 0; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
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
            var mg = cx.FixTTllb(matching);
            if (mg != matching)
                r += (Matching, mg);
            var ts = cx.FixV(rsTargets);
            if (ts!=rsTargets)
                r += (RSTargets, ts);
            var tg = cx.Fix(target);
            if (tg != target)
                r += (Target, tg);
            var ag = cx.FixTub(assig);
            if (ag != assig)
                r += (Assig, ag);
            var s = (long)(mem[_Source] ?? -1L);
            var ns = cx.Fix(s);
            if (ns != s)
                r += (_Source, ns);
            var na = cx.FixTsl(names);
            if (na != names)
                r += (ObInfo.Names, na);
            r += (Asserts, asserts);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var rd = cx._Dom((long?)r[_Domain]??-1L)??throw new PEException("PE1501");
            var rs = (Domain)rd.Replace(cx, so, sv);
            var de = depth;
            if (cx.obs[source] is RowSet sc)
                de = _Max(de, sc.depth + 1);
            if (rs.defpos != domain)
                r += (_Domain, rs.defpos);
            var fl = cx.ReplaceTlT(filter, so, sv);
            if (fl != filter)
                r += (Filter, fl);
            var xs = cx.ReplacedTDTlb(indexes);
            if (xs != indexes)
                r += (Table.Indexes, xs);
            var ks = keys.Replaced(cx);
            if (ks != keys)
                r += (Level3.Index.Keys, ks);
            r += (SIMap, cx.ReplacedTll(sIMap));
            r += (ISMap, cx.ReplacedTll(iSMap));
            if (r[ObInfo.Names] is BTree<string, long?> ns)
                r += (ObInfo.Names, cx.ReplacedTsl(ns));
            var os = ordSpec.Replace(cx,so,sv);
            if (os != ordSpec)
                r += (OrdSpec, os);
            var ro = rowOrder.Replaced(cx);
            if (ro != rowOrder)
                r += (RowOrder, ro);
            var w = where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
                de = Math.Max(de, v.depth);
            }
            if (w != where)
                r += (_Where, w);
            var ms = matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx._Replace(b.key(), so, sv);
                if (bk.defpos != b.key())
                    ms += (bk.defpos, b.value());
                de = Math.Max(de, bk.depth);
            }
            if (ms != matches)
                r += (_Matches, ms);
            var mg = matching;
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
            if (mg != matching)
                r += (Matching, mg);
            var ch = false;
            var ts = BTree<long, long?>.Empty;
            for (var b = rsTargets.First(); b != null; b = b.Next())
                if (b.value() is long v && v != defpos)
                {
                    var p = cx.ObReplace(v, so, sv);
                    var k = cx.done[b.key()]?.defpos ?? b.key();
                    if (p != b.value() || k != b.key())
                        ch = true;
                    ts += (k, p);
                    if (cx.obs[p] is RowSet tr)
                        de = Math.Max(de, tr.depth + 1);
                }
            if (ch)
                r += (RSTargets, ts);
            r += (Domain.Representation, rs.representation);
            if (m[_Source] is long sp)
            {
                var n = cx.ObReplace(sp, so, sv);
                if (sp != n)
                    r += Apply(E+(_Source, n),cx,m).mem;
                if (cx.obs[n] is RowSet tr)
                de = Math.Max(de, tr.depth);
            }
            if (cx.done[groupSpec] is GroupSpecification gs)
            {
                var ng = gs.defpos;
                if (ng != groupSpec)
                    r += (Group, ng);
                var og = groupings;
                r += (Groupings, cx.ReplacedLl(og));
                if (!Context.Match(groupings,og))
                    r += (GroupCols, cx.GroupCols(groupings));
            }
            if (target>0 && cx._Replace(target, so, sv) is RowSet tg && tg.defpos != target)
                r += (Target, tg);
            var ua = CTree<UpdateAssignment, bool>.Empty;
            for (var b = assig?.First(); b != null; b = b.Next())
            {
                var na = b.key().Replace(cx, so, sv);
                ua += (na, true);
                if (cx.obs[na.vbl] is SqlValue vb)
                    de = Math.Max(de, vb.depth + 1);
                if (cx.obs[na.val] is SqlValue va)
                    de = Math.Max(de,va.depth + 1);
            }
            if (ua != assig)
                r += (Assig, ua);
            r += (_Depth, de);
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            if (cx._Dom(this) is Domain dm && dm.rowType[i] is long p)
            {
                if (cx.role != null && cx._Ob(p)?.infos[cx.role.defpos] is ObInfo ci && ci.name is string nm)
                    return nm;
                if (cx.obs[p] is SqlValue sv && (sv.alias ?? sv.name) is string n)
                    return n;
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
            if (rp == defpos)
                return true;
            var dm = cx._Dom(this)??throw new PEException("PE1503");
            for (var b = dm.rowType.First(); b != null // && b.key()<ds
                    ; b = b.Next())
                if (b.value() == rp)
                    return true;
            if (ambient && cx.obs[rp] is SqlValue sv 
                && (sv.from < defpos || sv.defpos < defpos) && sv.selectDepth<selectDepth)
                return true;
            return false;
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            var dm = cx._Dom(this)??throw new PEException("PE1504");
       //     int m = dm.rowType.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keys.First(); j < flags.Length && ib != null;
                ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = dm.rowType.First(); b != null;
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
            var d = dm.display;
            if (d == 0)
                d = int.MaxValue;
            for (var b = dm.rowType.First(); b != null && b.key()<d; b = b.Next())
                if (b.value() is long cp && dm.representation[cp] is Domain dc)
                {
                    var i = b.key();
                    flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                    if (cx.db.objects[cp] is TableColumn tc)
                        flags[i] += (tc.notNull ? 0x100 : 0) +
                            ((tc.generated != GenerationRule.None) ? 0x200 : 0);
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
            rs = (RowSet)(cx.obs[rs.defpos]??throw new PEException("0089"));
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
                for (var b = keys.First(); kb!=null &&  b != null; b = b.Next(),kb=kb.Next())
                    if (kb.value()!=TNull.Value && b.value() is long p &&  cx.obs[p] is SqlValue s && 
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
                    Console.WriteLine(s.ToString(cx, i));
                    for (var b = s.Sources(cx).First(); b != null; b = b.Next())
                    {
                        p = b.key();
                        if (p >= 0)
                            show += (i + 1, p);
                    }
                }
            }
        }
        internal string ToString(Context cx,int i)
        {
            var sb = new StringBuilder();
            for (var j = 0; j < i; j++)
                sb.Append(' ');
            sb.Append(Uid(defpos));
            sb.Append(' ');sb.Append(GetType().Name);
            if (cx.obs[domain] is Domain sd)
            {
                sb.Append(' '); sd.Show(sb);
            }
            Show(sb);
            return sb.ToString();
        }
        internal virtual void Show(StringBuilder sb)
        {
            sb.Append(' '); sb.Append(Uid(defpos));
            if (domain>=0)
            {
                sb.Append(':'); sb.Append(Uid(domain));
            }
            string cm;
            if (indexes.Count > 0)
            {
                sb.Append(" Indexes=[");
                cm = "";
                for (var b = indexes.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                        sb.Append(cm); cm = ",";
                        var cn = "(";
                        for (var d = b.key().First(); d != null; d = d.Next())
                            if (d.value() is long p)
                            {
                                sb.Append(cn); cn = ",";
                                sb.Append(Uid(p));
                            }
                        sb.Append(')'); sb.Append(Uid(c.key()));
                    }
                sb.Append(']');
            }
            if (keys!=Domain.Row)
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
            if (ordSpec != Domain.Row)
            {
                cm = " ordSpec (";
                for (var b = ordSpec.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p){
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
                sb.Append(')');
            }
            if (rowOrder != Domain.Row)
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
                for(var b=ro.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm == ",")
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
    /// 2. The Rowset field is readonly but NOT immutable as it contains the Context _cx.
    /// _cx.values is updated to provide shortcuts to current values.
    /// 3. The TypedValues obtained by Value(..) are mostly immutable also, but
    /// updates to row columns (e.g. during trigger operations) are managed by 
    /// allowing the Cursor to override the usual results of 
    /// evaluating a row column: this is needed in (at least) the following 
    /// circumstances (a) UPDATE (b) INOUT and OUT parameters that are passed
    /// column references (c) triggers.
    /// 4. In view of the anxieties of 3. these overrides are explicit
    /// in the Cursor itself.
    ///  /// shareable as of 26 April 2021 (but e.g. LogSystemBookmark  and its subclasses are not)
    /// </summary>
    internal abstract class Cursor : TRow
    {
        internal static BTree<long,(long,long)> E = BTree<long,(long,long)>.Empty;
        public readonly long _rowsetpos;
        public readonly Domain _dom;
        public readonly int _pos;
        public readonly BTree<long, (long,long)> _ds;   // target->(defpos,ppos)
        public readonly int display;
        protected Cursor(Context cx, RowSet rs, int pos, BTree<long,(long,long)> ds, 
            TRow rw) : base(cx._Dom(rs)??throw new PEException("PE1500"), rw.values)
        {
            _rowsetpos = rs.defpos;
            _dom = cx._Dom(rs)??Domain.Content;
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
            cx.values += values;
        }
        protected Cursor(Context cx, RowSet rs, int pos, BTree<long,(long,long)> ds, 
            TypedValue[] vs) 
            : base(cx._Dom(rs)??throw new DBException("42105"), vs)
        {
            _rowsetpos = rs.defpos;
            _dom = cx._Dom(rs) ?? throw new DBException("42105");
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
            return _Next(cx);
        }
        public virtual Cursor? Previous(Context cx)
        {
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
                 || (cx._Dom(ts) ?? Domain.Content).aggs != CTree<long, bool>.Empty)
                    rvv += (ts.target, (-1L, tb.lastData));
                else if (cx.cursors[ts.defpos] is Cursor cu)
                    rvv += (ts.target, cu);
            }
            else
                for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is RowSet tg)
                    {
                        if (((tg.where == CTree<long, bool>.Empty && tg.matches == CTree<long, TypedValue>.Empty)
                        || (cx._Dom(tg) ?? Domain.Content).aggs != CTree<long, bool>.Empty)
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
                    if (cx.obs[b.key()] is SqlValue s)
                    {
                        var v = s.Eval(cx);
                        if (v == null || v.CompareTo(b.value()) != 0)
                        return false;

                    } 
                for (var b = rs.where.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValue sw && sw.Eval(cx) != TBool.True)
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
            if (this[0] is TChar v && cx.db.nodeIds[v.ToString()] is TNode n)
                return n;
            return null;
        }
        internal string NameFor(Context cx, int i)
        {
    //        var rs = (RowSet)cx.obs[_rowsetpos];
            var p = _dom.rowType[i];
            return ((SqlValue?)cx.obs[p??-1L])?.name??"";
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class TrivialRowSet: RowSet
    {
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)(mem[Singleton]??TRow.Empty);
        internal TrivialRowSet(Context cx) 
            : this(cx.GetUid(),cx,TRow.Empty) { }
        internal TrivialRowSet(long dp, Context cx, TRow r, string? a=null)
            : base(dp, cx, _Mem(dp,cx,r.dataType,a)+(Singleton,r))
        {
            cx.Add(this);
        }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp, Context cx,Domain dm,string? a)
        {
            var ns = BTree<string, long?>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            if (b.value() is long p)
                ns += (cx.NameFor(p), p);
            var r = BTree<long, object>.Empty + (_Domain, dm.defpos) 
                + (ObInfo.Names, ns) + (_Depth,dm.depth+1);
            if (a != null)
                r = r + (_Alias, a) + (_Ident, new Ident(a, new Iix(dp)));
            return r;
        }
        public static TrivialRowSet operator +(TrivialRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (TrivialRowSet)rs.New(rs.mem + x);
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
            for (var b=row.dataType.rowType.First(); b != null && kb!=null; b=b.Next(),kb=kb.Next())
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var rw = (TRow)row.Replace(cx, so, sv);
            if (rw!=row)
                r += (Singleton, rw);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TrivialRowSet(defpos,m);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" ("); sb.Append(row.ToString());
        }
        // shareable as of 26 April 2021
        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t) 
                :base(_cx,t,0,E,t.row)
            {
                trs = t;
            }
            TrivialCursor(TrivialCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                trs = cu.trs;
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        internal override Assertions Requires => Assertions.SimpleCols;
        /// <summary>
        /// If all uids of dm are uids of r.domain (maybe in different order)
        /// </summary>
        internal SelectedRowSet(Context cx, long dm, RowSet r)
                :base(cx.GetUid(),_Mem(cx,dm,r)+(_Domain,dm)+(_Source,r.defpos)
                     +(RSTargets,new BTree<long,long?>(r.target,r.defpos))
                     +(Asserts,r.asserts) + (ISMap,r.iSMap) + (SIMap,r.sIMap))
        {
            cx.Add(this);
        } 
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, long d, RowSet r)
        {
            if (cx._Ob(d) is not Domain dm)
                throw new DBException("42105");
            var m = r.mem;
            var ns = BTree<string, long?>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    ns += (cx.NameFor(p), p);
            m += (_Depth, Context.Depth(dm, r));
            return m + (ObInfo.Names, ns);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectedRowSet(defpos, m);
        }
        internal override bool CanSkip(Context cx)
        {
            if (cx.obs[source] is not RowSet sc || cx._Dom(this) is not Domain dm
                || cx._Dom(sc) is not Domain sd)
                return false;
            return Context.Match(dm.rowType,sd.rowType) && target >= Transaction.Analysing;
        }
        internal override RowSet Apply(BTree<long,object> mm,Context cx,BTree<long,object>?m=null)
        {
            var ags = assig;
            if (mm[Assig] is CTree<UpdateAssignment,bool> sg)
            for (var b=sg.First();b!=null;b=b.Next())
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
        public static SelectedRowSet operator +(SelectedRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (SelectedRowSet)rs.New(rs.mem + x);
        }
        internal override bool Built(Context cx)
        {
            return ((RowSet?)cx.obs[source])?.Built(cx)??false;
        }
        internal override RowSet Build(Context cx)
        {
            if (!Built(cx))
                (((RowSet?)cx.obs[source])??throw new PEException("PE1510")).Build(cx);
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
        // shareable as of 26 April 2021
        internal class SelectedCursor : Cursor
        {
            internal readonly SelectedRowSet _srs;
            internal readonly Cursor? _bmk; // for rIn
            internal SelectedCursor(Context cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(cx,srs, pos, bmk._ds, srs._Row(cx,bmk)) 
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
    // shareable as of 26 April 2021
    internal class SelectRowSet : RowSet
    {
        internal const long
            Building = -401, // bool
            RdCols = -124, // CTree<long,bool> TableColumn (needed cols for select list and where)
            ValueSelect = -385; // long SqlValue
        internal bool building => mem.Contains(Building);
        internal CTree<long, bool> rdCols =>
            (CTree<long, bool>)(mem[RdCols] ?? CTree<long, bool>.Empty);
        internal long valueSelect => (long)(mem[ValueSelect] ?? -1L);
        internal TRow? row => rows[0];
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec (select list defines dm)
        /// Query environment can supply values for the select list but source columns
        /// should bind more closely.
        /// </summary>
        internal SelectRowSet(Iix lp, Context cx, Domain dm, RowSet r,BTree<long,object>? m=null)
            : base(cx, lp.dp, _Mem(cx,dm,r,m)) //  not dp, cx, _Mem..
        { }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Context cx,Domain dm,RowSet r,BTree<long,object>?m)
        {
            m ??= BTree<long, object>.Empty;
            var gr = (long)(m[Group]??-1L);
            var groups = (GroupSpecification?)cx.obs[gr];
            var gs = BList<long?>.Empty;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                gs = _Info(cx, g, gs);
            m += (Groupings, gs);
            var os = (Domain)(m[OrdSpec] ?? Domain.Row);
            var di = (bool)(m[Distinct] ?? false);
            if (os.CompareTo(r.rowOrder) != 0 || di != r.distinct)
                r = (RowSet)cx.Add(new OrderedRowSet(cx, r, os, di));
            var rc = CTree<long, bool>.Empty;
            var d = dm.display;
            if (d == 0)
                d = dm.rowType.Length;
            var wh = (CTree<long, bool>?)m[_Where];
            // For SelectRowSet, we compute the RdCols: all needs of selectors and wheres
            if (m[ISMap] is BTree<long, long?> im)
            {
                for (var b = dm.rowType.First(); b != null && b.key() < d; b = b.Next())
                    if (b.value() is long p && cx._Ob(p) is DBObject ob)
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
                        for (var x = tt.indexes.First();x!=null;x=x.Next())
                        {
                            var k = x.key();
                            for (var c = k.First(); c != null; c = c.Next())
                                if (((BTree<long,long?>?)m[ISMap])?.Contains(c.value()??-1L)==true)
                                    goto next;
                            var t = tt.indexes[k] ?? CTree<long, bool>.Empty;
                            xs += (k, t+x.value());
                            next:;
                        }
                        m += (Table.Indexes, xs);
                    }
                }
            if (r.keys != Domain.Row)
                m += (Level3.Index.Keys, r.keys);
            return m +(_Domain,dm.defpos)+(_Source,r.defpos) + (RSTargets,r.rsTargets)
                +(_Depth,Context.Depth(dm,r)) + (SqlValue.SelectDepth, cx.sD)
                + (Asserts,r.asserts);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SelectRowSet(dp,m);
        }
        public static SelectRowSet operator +(SelectRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (SelectRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null)
        {
            return this;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var no = so.alias ?? ((so as SqlValue)?.name) ?? so.infos[cx.role.defpos]?.name;
            var nv = sv.alias ?? ((sv as SqlValue)?.name) ?? sv.infos[cx.role.defpos]?.name;
            if (nv!=null && no!=nv && no!=null && names is BTree<string,long?> ns 
                && names.Contains(no))
            {
                ns = ns + (nv, names[no]) - no;
                r += (ObInfo.Names, ns);
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
        protected static BList<long?> _Info(Context cx, Grouping g, BList<long?> gs)
        {
            gs += g.defpos;
            var cs = BList<DBObject>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s)
                    cs += s;
            var dm = new Domain(Sqlx.ROW, cx, cs);
            cx.Replace(g, g + (_Domain, dm.defpos));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        internal override bool Built(Context cx)
        {
            if (building)
                throw new PEException("0017");
            var dm = cx._Dom(this)??throw new PEException("PE1500");
            if (dm.aggs != CTree<long, bool>.Empty)
                return false;
            return (bool)(mem[_Built]??false);
        }
        protected override bool CanAssign()
        {
            return false;
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[valueSelect] is SqlValue svs && cx.obs[svs.from] is RowSet es)
                for (var b = es.Sources(cx).First(); b != null; b = b.Next())
                    if (cx._Dom(cx.obs[b.key()])?.representation.Contains(rp)==true)
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long,object>? m = null)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE48145");
            var gr = (long)(mm[Group] ?? -1L);
            var groups = (GroupSpecification?)cx.obs[gr];
            var gs = groupings;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                gs = _Info(cx, g, gs);
            var am = BTree<long, object>.Empty;
            if (gs != groupings)
            {
                am += (Groupings, gs);
                am += (GroupCols, cx.GroupCols(gs));
                am += (Group, gr);
            }
            if (mm[Domain.Aggs] is CTree<long,bool> t)
                am += (Domain.Aggs, dm.aggs + t);
            if (mm[Having] is CTree<long,bool>h && h!=CTree<long,bool>.Empty)
                am += (Having, having + h);
            var pm = mm - Domain.Aggs - Group - Groupings - GroupCols - Having;
            var r = (SelectRowSet)base.Apply(pm, cx);
            if (am != BTree<long, object>.Empty)
                r = (SelectRowSet)r.ApplySR(am + (AggDomain, r.domain), cx);
            return r;
        }
        /// <summary>
        /// Version of Apply for applyng Aggs and Groups to SelectRowSet and RestRowSet.
        /// 
        /// We get here if there are any aggregations at this level (and if there are groupings there must be aggs).
        /// If we construct a new source rowset S', rowsets T between R and S' in the pipeline must get new Domains.
        /// These are built bottom-up.
        /// Selectlist expressions in T should be the subexpressions of R's select list that it knows.
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
            var ag = (CTree<long, bool>?)mm[Domain.Aggs]??throw new DBException("42105");
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            if (mm[GroupCols] is Domain gc && mm[Groupings] is BList<long?> lg
                && mm[Group] is long gs)
            {
                m += (GroupCols, gc);
                m += (Groupings, lg);
                m += (Group, gs);
                m += (Domain.Aggs, ag);
            }
            else
                // if we get here we have a non-grouped aggregation to add
                m += (Domain.Aggs, ag);
            var nc = BList<long?>.Empty; // start again with the aggregating rowType, follow any ordering given
            var dm = (Domain?)cx.obs[(long)(mm[AggDomain]??-1L)];
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain?)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            var dn = cx._Dom(cx.obs[source]);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    if (v.IsAggregation(cx) != CTree<long, bool>.Empty)
                        for (var c = v.KnownFragments(cx, kb).First();
                            c != null; c = c.Next())
                        {
                            var k = c.key();
                            if (ns.Contains(k))
                                continue;
                            if (cx._Dom(k) is not Domain dc)
                                throw new PEException("PE48146");
                            nc += k;
                            ns += (k, dc);
                        }
                    else if (gb.Contains(p))
                    {
                        nc += p;
                        if (cx._Dom(v) is not Domain dv)
                            throw new PEException("PE48146");
                        ns += (p, dv);
                    }
                }
            var d = nc.Length;
            for (var b = dn?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && !ns.Contains(p) && cx._Dom(p) is Domain dv)
                {
                    nc += p;
                    ns += (p, dv);
                }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc, d) + (Domain.Aggs, ag);
            m = m + (_Domain, nd.defpos) + (Domain.Aggs, ag);
            cx.Add(nd);
            var r = (SelectRowSet)New(m);
            cx.Add(r);
            if (mm[Having] is CTree<long, bool> h)
            {
                var hh = CTree<long, bool>.Empty;
                for (var b = h.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v && v.KnownBy(cx, r))
                        hh += (b.key(), true);
                if (hh != CTree<long, bool>.Empty)
                    m += (Having, hh);
                r = (SelectRowSet)New(m);
                cx.Add(r);
            }
            // see if the aggs can be pushed down further XXX No: this algorithm causes errors
            /*          for (var b = r.SplitAggs(mm, cx).First(); b != null; b = b.Next())
                      if (cx.obs[b.key()] is RowSet a){
                          var na = a.ApplySR(b.value() + (AggDomain, nd.defpos), cx);
                          if (na != a)
                              r.ApplyT(cx, na, nd.representation);
                      } */
            cx.done = od;
            return (RowSet?)cx.obs[defpos] ?? throw new DBException("42105"); // will have changed
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
        /// For each source restrowset or selectrowset S (e.g. va or vb) we want to traverse the given select list, 
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
            var dm = cx._Dom(this);
            var rdct = dm?.aggs.Count == 0 && distinct;
            var ss = AggSources(cx);
            var ua = BTree<long, long?>.Empty; // all uids -> unique sid
            var ub = CTree<long, bool>.Empty; // uids with unique sids
            if (ss == BTree<long, RowSet>.Empty || !mm.Contains(Domain.Aggs))
                return r;
            for (var b = ss.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var d = cx._Dom(b.value());
                for (var c = d?.rowType.First(); c != null; c = c.Next())
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
            for (var b = ((CTree<long, bool>?)mm[Domain.Aggs])?.First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is SqlValue e){
                var a = b.key(); // An aggregating SqlFunction
                var sa = -1L;
                if (e is SqlFunction sf && sf.kind == Sqlx.COUNT && sf.mod == Sqlx.TIMES)
                {
                    //if (Sources(cx).Count > 1)
                    //    goto no;
                    for (var c = ss.First(); c != null; c = c.Next())
                    {
                        var ex = r[c.key()] ?? BTree<long, object>.Empty;
                        var eg = (CTree<long, bool>?)ex[Domain.Aggs] ?? CTree<long, bool>.Empty;
                        r += (c.key(), ex + (Domain.Aggs, eg + (a, true)));
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
                var ag = (CTree<long, bool>?)ax[Domain.Aggs] ?? CTree<long, bool>.Empty;
                r += (sa, ax + (Domain.Aggs, ag + (a, true)));
            }
            for (var b = ((Domain?)mm[GroupCols])?.representation?.First(); b != null; b = b.Next())
            if (cx.obs[b.key()] is SqlValue g){// might be an expression
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
                    nc += k;
                    ns += (k, c.value());
                }
                if (nc != BList<long?>.Empty)
                {
                    var nd = new Domain(cx.GetUid(), cx, Sqlx.ROW, ns, nc);
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
            if (cx.obs[source] is RowSet sc && cx._Dom(sc) is Domain sd && cx._Dom(this) is Domain dm)
                return Context.Match(dm.rowType,sd.rowType) && where.CompareTo(sc.where) == 0;
            return false;
        }
        internal override RowSet Build(Context cx)
        {
            if (Built(cx))
                return this;
            if (building)
                throw new PEException("0077");
            var dm = cx._Dom(this)??throw new DBException("42105");
            var ags = dm.aggs;
            cx.groupCols += (domain, groupCols);
            if (ags != CTree<long, bool>.Empty && cx.obs[source] is RowSet sce) {
                SqlFunction? countStar = null;
                if (dm.display == 1 && cx.obs[dm.rowType[0]??-1L] is SqlFunction sf
                    && _ProcSources(cx).Count == 0
                    && sf.kind == Sqlx.COUNT && sf.mod == Sqlx.TIMES && where == CTree<long, bool>.Empty)
                    countStar = sf;
                if (countStar != null && sce.Built(cx) && _RestSources(cx).Count == 0)
                {
                    var v = sce.Build(cx)?.Cardinality(cx) ?? 0;
                    Register rg = new(cx, TRow.Empty, countStar) { count = v };
                    var cp = dm.rowType[0]??-1L;
                    cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty
                        + (TRow.Empty, new BTree<long,Register>(cp, rg)));
                    cx.regs -= cp;
                    var sg = new TRow(dm, new CTree<long, TypedValue>(cp, new TInt(v)));
                    cx.values += (defpos, sg);
                    return (RowSet)New(cx, E + (_Built, true) + (_Rows, new BList<TRow>(sg)));
                }
                var r = this + (Building, true);
                cx.obs += (defpos, r); // doesn't change depth
                cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty);
                for (var rb = sce.First(cx); rb != null; rb = rb.Next(cx))
                {
                    var ad = false;
                    for (var b = _RestSources(cx).First(); (!ad) && b != null; b = b.Next())
                        ad = b.value().aggs != CTree<long, bool>.Empty;
                    if (ad)
                        goto next; // AddIns have been done from Json return (at least if ss.Count==1?!)
                    if (r.groupings.Count == 0)
                        for (var b =ags.First(); b != null; b = b.Next())
                            ((SqlFunction?)cx.obs[b.key()])?.AddIn(TRow.Empty, cx);
                    else for (var g = r.groupings.First(); g != null; g = g.Next())
                            if (g.value() is long p && cx.obs[p] is Grouping gg)
                            {
                                var vals = CTree<long, TypedValue>.Empty;
                                for (var gb = gg.keys.First(); gb != null; gb = gb.Next())
                                    if (gb.value() is long gp && cx.obs[gp] is SqlValue v)
                                        vals += (gp, v.Eval(cx));
                                var key = new TRow(r.groupCols, vals);
                                for (var b = ags.First(); b != null; b = b.Next())
                                    ((SqlFunction?)cx.obs[b.key()])?.AddIn(key, cx);
                            }
                        next:;
                }
                var rws = BList<TRow>.Empty;
                var fd = cx.funcs[defpos];
                for (var b = fd?.First(); b != null; b = b.Next())
                    if (b.value() != BTree<long, Register>.Empty)
                    {
                        // Remember the aggregating SqlValues are probably not just aggregation SqlFunctions
                        // Seed the keys in cx.values
                        var vs = b.key().values;
                        cx.regs += b.value();
                        cx.values += vs;
                        for (var d = r.matching.First(); d != null; d = d.Next())
                            if (cx.values[d.key()] is TypedValue v)
                                for (var c = d.value().First(); c != null; c = c.Next())
                                    if (!vs.Contains(c.key()))
                                        vs += (c.key(), v);
                        // and the aggregate function accumulated values
                        for (var c = dm.aggs.First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlValue v)
                                cx.values += (v.defpos, v.Eval(cx));
                        // compute the aggregation expressions from these seeds
                        for (var c = dm.rowType.First(); c != null; c = c.Next())
                            if (c.value() is long p && cx.obs[p] is SqlValue sv 
                                && sv.IsAggregation(cx) != CTree<long, bool>.Empty)
                                vs += (sv.defpos, sv.Eval(cx));
                        // for the having calculation to work we must ensure that
                        // having uses the uids that are in aggs
                        for (var h = r.having.First(); h != null; h = h.Next())
                            if (cx.obs[h.key()]?.Eval(cx) != TBool.True)
                                goto skip;
                        rws += new TRow(dm, vs);
                    skip:;
                    }
    //            cx.cursors = oc;
                if (rws == BList<TRow>.Empty)
                    rws += new TRow(dm, CTree<long, TypedValue>.Empty);
                r = (SelectRowSet)r.New(r.mem - Building);
                cx.obs += (defpos, r); // doesnt change depth
                return (RowSet)r.New(cx, E + (_Rows, rws) + (_Built, true) - Level3.Index.Tree
                    + (Groupings, r.groupings));
            }
            return (RowSet)New(cx, E + (_Built, true));
        }
        public override Cursor? First(Context _cx)
        {
            var r = (SelectRowSet)Build(_cx);
            var dm = _cx._Dom(r)??throw new PEException("PE1500");
            _cx.rdC += rdCols;
            if (dm.aggs != CTree<long, bool>.Empty)
            {
                if (groupings==BList<long?>.Empty)
                    return EvalBookmark.New(_cx, r);
                return GroupingBookmark.New(_cx, r);
            }
            return SelectCursor.New(_cx, r);
        }
        protected override Cursor? _First(Context cx)
        {
            throw new NotImplementedException();
        }
        public override Cursor? Last(Context _cx)
        {
            var r = (SelectRowSet)Build(_cx);
            var dm = _cx._Dom(r) ?? throw new PEException("PE1500");
            if (dm.aggs != CTree<long, bool>.Empty)
            {
                if (groupings == BList<long?>.Empty)
                    return EvalBookmark.New(_cx, r);
                return GroupingBookmark.New(r, _cx);
            }
            return SelectCursor.New(r, _cx);
        }
        protected override Cursor? _Last(Context cx)
        {
            throw new NotImplementedException();
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, Domain rt)
        {
            return cx.obs[source]?.Insert(cx, ts, rt) ?? throw new DBException("42105");
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm)
        {
            return cx.obs[source]?.Delete(cx,fm) ?? throw new DBException("42105");
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm)
        {
            return cx.obs[source]?.Update(cx,fm) ?? throw new DBException("42105");
        }
        // shareable as of 26 April 2021
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
                        ((SqlValue?)cx.obs[b.key()])?.OnRow(cx,rb);
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
                        ((SqlValue?)cx.obs[b.key()])?.OnRow(cx, rb);
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
        internal class GroupingBookmark : Cursor
        {
            public readonly SelectRowSet _grs;
            public readonly ABookmark<int, TRow> _ebm;
            GroupingBookmark(Context _cx, SelectRowSet grs,
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
            internal static GroupingBookmark? New(Context cx, SelectRowSet grs)
            {
                var ebm = grs.rows?.First();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                return r;
            }
            internal static GroupingBookmark? New(SelectRowSet grs, Context cx)
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
                    return null;
                var dt = cx._Dom(_grs)??throw new PEException("PE1500");
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue?)cx.obs[b.key()])?.OnRow(cx, r);
                return r;
            }
            protected override Cursor? _Previous(Context cx)
            {
                var ebm = _ebm.Previous();
                if (ebm == null)
                    return null;
                var dt = cx._Dom(_grs) ?? throw new PEException("PE1500");
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue?)cx.obs[b.key()])?.OnRow(cx, r);
                return r;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
        internal class EvalBookmark : Cursor
        {
            readonly SelectRowSet _ers;
            internal EvalBookmark(Context _cx, SelectRowSet ers)
                : base(_cx, ers, 0, E, ers.row??throw new PEException("PE48147"))
            {
                _ers = ers;
            }
            internal static EvalBookmark? New(Context cx,SelectRowSet ers)
            {
                if (ers.row == null)
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
                return null; // just one row in the rowset
            }
            protected override Cursor? _Previous(Context cx)
            {
                return null; // just one row in the rowset
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
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
            : base(cx, dp, _Mem1(cx,m)) 
        { }
        protected InstanceRowSet(long dp, BTree<long, object> m)
            : base(dp, m) 
        { }
        static BTree<long,object> _Mem1(Context cx,BTree<long,object> m)
        {
            m ??= BTree<long, object>.Empty;
            var ism = BTree<long, long?>.Empty; // SqlValue, TableColumn
            var sim = BTree<long, long?>.Empty; // TableColumn,SqlValue
            if (cx._Dom((long)(m[_Domain] ?? -1L)) is not Domain dm)
                throw new DBException("42105");
            m += (_Depth, Context.Depth(dm));
            var sr = (BList<long?>?)m[SRowType]??BList<long?>.Empty;
            var ns = BTree<string, long?>.Empty;
            var fb = dm.rowType.First();
            for (var b = sr.First(); b != null && fb != null; b = b.Next(), fb = fb.Next())
                if (b.value() is long sp && cx._Ob(sp) is DBObject ob && fb.value() is long ip)
                {
                    ism += (ip, sp);
                    sim += (sp, ip);
                    ns += (ob.alias ?? ob.NameFor(cx), ip);
                }
            if (cx.obs[(long)(m[Target]??-1L)] is Table tb)
                m += (Table.Indexes,tb.IIndexes(cx,sim));
            return m + (ISMap, ism) + (SIMap, sim) + (ObInfo.Names,ns);
        }
        public static InstanceRowSet operator +(InstanceRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (InstanceRowSet)rs.New(rs.mem + x);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var srt = cx.ReplacedLl(sRowType);
            if (srt!=sRowType)
                r += (SRowType, srt);
            var sim = cx.ReplacedTll(sIMap);
            if (sim != sIMap)
                r += (SIMap, sim);
            var ism = cx.ReplacedTll(iSMap);
            if (ism != iSMap)
                r += (ISMap, ism);
            var xs = cx.ReplacedTDTlb(indexes);
            if (xs != indexes)
                r += (Table.Indexes, xs);
            var ks = cx.done[keys.defpos];
            if (ks != keys && ks!=null)
                r += (Level3.Index.Keys, ks);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var srt = cx.FixLl(sRowType);
            if (srt != sRowType)
                r += (SRowType, srt);
            var sim = cx.FixTll(sIMap);
            if (sim != sIMap)
                r += (SIMap, sim);
            var ism = cx.FixTll(iSMap);
            if (ism != iSMap)
                r += (ISMap, ism);
            var xs = cx.FixTDTlb(indexes);
            if (xs!=indexes)
                r += (Table.Indexes, xs);
            var ks = (Domain?)keys?.Fix(cx);
            if (ks != keys && ks is not null)
                r += (Level3.Index.Keys, ks);
            return r;
        }
        internal override CTree<long, Cursor> SourceCursors(Context cx)
        {
            return CTree<long,Cursor>.Empty;
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
    /// The domain includes columns from the supertype, and a record for the table is
    /// installed in this table and its supertypes.
    /// // shareable as of 26 April 2021
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
        { }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(long dp, Context cx, long t, BTree<long,object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (cx._Ob(t) is not Table tb) 
                throw new DBException("42105");
            cx.Add(tb.framing);
            if (cx._Dom(tb) is not Domain dt || tb.NameFor(cx) is not string n)
                throw new DBException("42105");
            var tn = new Ident(n, cx.Ix(dp));
            var rt = BList<long?>.Empty;        // our total row type
            var rs = CTree<long, Domain>.Empty; // our total row type representation
            var sr = BList<long?>.Empty;        // our total SRow type
            var tr = BTree<long, long?>.Empty;  // mapping to physical columns
            var ns = BTree<string, long?>.Empty; // column names
            (rt,rs,sr,tr,ns) = ColsFrom(cx,dp,tb,rt,rs,sr,tr,ns);
            var xs = CTree<Domain, CTree<long, bool>>.Empty;
            var pk = Domain.Row;
            for (var b = tb.indexes.First(); b != null; b = b.Next())
            {
                var bs = BList<DBObject>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() is long p && cx._Ob(tr[p]??-1L) is DBObject tp)
                        bs += tp;
                var nk = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length));
                if (pk == Domain.Row)
                {
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is Level3.Index x && x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                            pk = nk;
                    pk = (Domain)cx.Add(pk);
                }
                xs += (nk, b.value());
            }
            if (pk != null)
                m += (Level3.Index.Keys,pk);
            var rr = BList<long?>.Empty;
            if (tb.defpos < 0) // system tables have columns in the wrong order!
            {
                for (var b = rt.Last(); b != null; b = b.Previous())
                    rr += b.value();
                rt = rr;
            }
            var dm = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rt, rt.Length);
            dm = (Domain)cx.Add(dm);
            cx.AddDefs(tn, dm, (string?)m[_Alias]);
            var r = (m ?? BTree<long, object>.Empty) + (_Domain, dm.defpos) + (Target, t)
                + (SRowType, sr) + (Table.Indexes, xs)
                + (Table.LastData, tb.lastData) + (SqlValue.SelectDepth,cx.sD)
                + (Target,t) + (ObInfo.Names,ns) + (ObInfo.Name,tn.ident)
                + (_Ident,tn) 
                + (_Depth, 2)+(RSTargets,new BTree<long,long?>(t,dp));
            r += (Asserts, (Assertions)(_Mem(cx, r)[Asserts]??Assertions.None) |Assertions.SpecificRows);
            return r;
        }
        static (BList<long?>,CTree<long,Domain>,BList<long?>,BTree<long,long?>,BTree<string,long?>)
            ColsFrom(Context cx,long dp,Table tb, 
                BList<long?>rt, CTree<long, Domain>rs, BList<long?> sr,BTree<long, long?>tr, BTree<string, long?>ns)
        {
            if (cx._Ob(tb.nodeType) is NodeType nt && nt.super is NodeType su && cx._Ob(su.structure) is Table st)
            {
                cx.Add(st.framing);
                if (cx._Dom(st) is Domain sd)
                    (rt, rs, sr, tr, ns) = ColsFrom(cx, dp, st, rt, rs, sr, tr, ns);
            }
            for (var b = tb.tableCols.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is TableColumn tc)
                {
                    var nc = new SqlCopy(cx.GetUid(), cx, tc.NameFor(cx), dp, tc,
                        BTree<long, object>.Empty + (SqlValue.SelectDepth, cx.sD));
                    var cd = b.value();
                    if (cd != Domain.Content)
                        cd = (Domain)cd.Instance(dp, cx, null);
                    nc += (_Domain, cd.defpos);
                    cx.Add(nc);
                    // this really should be a bit cleverer
                    var i = (nc.name != "ID" && nc.name != "LEAVING" && nc.name != "ARRIVING") ? -1 : tc.seq;
                    rt = Table.Add(rt,i,nc.defpos);
                    sr = Table.Add(sr, i, tc.defpos);
                    rs += (nc.defpos, cd);
                    tr += (tc.defpos, nc.defpos);
                    ns += (nc.alias ?? nc.name ?? "", nc.defpos);
                }
            return (rt, rs, sr, tr, ns);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new TableRowSet(dp,m);
        }
        internal override RowSet Apply(BTree<long,object> mm, Context cx, BTree<long,object>? m = null)
        {
            if (mm == BTree<long, object>.Empty)
                return this;
            m ??= mem;
            if (mm[_Where] is CTree<long, bool> w)
            {
                for (var b = w.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var imm = (CTree<long, TypedValue>?)mm[_Matches]??CTree<long,TypedValue>.Empty;
                    if (cx.obs[k] is SqlValueExpr se && se.kind == Sqlx.EQL &&
                    cx.obs[se.left] is SqlValue le && cx.obs[se.right] is SqlValue ri) { 
                        if (le.isConstant(cx) && !imm.Contains(ri.defpos))
                            mm += (_Matches, imm + (ri.defpos, le.Eval(cx)));
                        if (ri.isConstant(cx) && !imm.Contains(le.defpos))
                            mm += (_Matches,imm + (le.defpos, ri.Eval(cx)));
                    }
                }
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
        public static TableRowSet operator+(TableRowSet rs,(long,object)x)
        {
            var (k, ob) = x;
            if (k == RSTargets || rs.mem[k]==ob) // TableRowSet is shared: don't update rsTargets
                return rs;
            return (TableRowSet)rs.New(rs.mem + x);
        }
        internal override int Cardinality(Context cx)
        {
            return(where==CTree<long,bool>.Empty && cx.obs[target] is Table tb)? 
                (int)tb.tableRows.Count
                :base.Cardinality(cx);
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[from] is SelectRowSet ss && cx.obs[ss.valueSelect] is SqlValue svs && cx.obs[svs.from] is RowSet es)
                for (var b = es.Sources(cx).First(); b != null; b = b.Next())
                    if (cx._Dom(cx.obs[b.key()])?.representation.Contains(rp)==true)
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            r += (RSTargets, cx.ReplacedTll(rsTargets));
            return r;
        }
        internal CTree<Domain, CTree<long, bool>> IIndexes(Context cx)
        {
            var xs = CTree<Domain, CTree<long, bool>>.Empty;
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var bs = BList<DBObject>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (c.value() is long p && cx._Ob(cx.Fix(p)) is DBObject tp)
                        bs += tp;
                var k = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length));
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
        /// <param name="iC">A list of columns matching some of ts.rowType</param>
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
                if (FindIndex(cx.db,os)!=null)
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
            if (alias!=null)
            { sb.Append(" Alias: "); sb.Append(alias); }
            return sb.ToString();
        }
        // shareable as of 26 April 2021
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
            static TRow _Row(Context cx, TableRowSet trs, TableRow rw)
            {
         //       var vs = CTree<long, TypedValue>.Empty;
                var ws = CTree<long, TypedValue>.Empty;
                var dm = cx._Dom(trs)??Domain.Content;
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && trs.iSMap[p] is long sp && rw.vals[sp] is TypedValue v)
                        ws += (p, v);
                return new TRow(dm, ws);
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
                    throw new PEException("PE50310");
                if (_cx.db.objects[trs.target] is not Table table || table.infos[ro.defpos] is null)
                    throw new DBException("42105");
                if (trs.keys!=Domain.Row)
                {
                    var t = (trs.index >= 0 && _cx.db.objects[trs.index] is Level3.Index ix)? ix.rows : null;
                    if (t == null && trs.indexes.Contains(trs.keys))
                        for (var b = trs.indexes[trs.keys]?.First(); b != null; b = b.Next())
                            if (_cx.db.objects[b.key()] is Level3.Index iy)
                            {
                                t = iy.rows;
                                break;
                            }
                    if (t!=null)
                    for (var bmk = t.PositionAt(key,0);bmk != null;bmk=bmk.Next())
                    {
                        var iq = bmk.Value();
                        if (iq == null || table.tableRows[iq.Value] is not TableRow rec)
                            continue;
//#if MANDATORYACCESSCONTROL
                        if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                            && _cx.db.user.defpos != table.definer
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                            continue;
//#endif
                        var rb = new TableCursor(_cx, trs, table, 0, rec, null, bmk, key);
                            if (rb.Matches(_cx))
                            {
                                table._ReadConstraint(_cx, rb);
                                var tt = _cx.rdS[table.defpos] ?? CTree<long, bool>.Empty;
                                _cx.rdS += (table.defpos, tt+(rec.defpos,true));
                                return rb;
                            }
                    }
                }
                for (var b = table.tableRows.First(); b != null; b = b.Next())
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
                            && _cx.db.user.defpos != table.definer
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
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
                            && _cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
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
                            && _cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
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
                            && _cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
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
                            && _cx.db.user.defpos != table.definer 
                            && _cx.db.user.defpos != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
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
                return (_rec!=null)?new BList<TableRow>(_rec):BList<TableRow>.Empty;
            }
            public override MTreeBookmark? Mb()
            {
                return _mb;
            }
        }
    }
    /// <summary>
    /// A rowset for distinct values
    /// // shareable as of 26 April 2021
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
                  +(_Matches,r.matches) + (_Domain,r.domain)
                  +(ObInfo.Names, r.names)+(_Depth,r.depth+1))
        {
            cx.Add(this);
        }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, RowSet r)
        {
            var dm = cx._Dom(r) ?? Domain.Content;
            var bs = BList<DBObject>.Empty;
            for (var b = dm.rowType.First(); b != null && b.key() < dm.display; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is DBObject tp)
                    bs += tp;
            var ks = cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length));
            return new BTree<long, object>(Level3.Index.Keys, ks);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new DistinctRowSet(defpos,m);
        }
        public static DistinctRowSet operator+(DistinctRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (DistinctRowSet)rs.New(rs.mem + x);
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
            return (RowSet)New(cx,E+(_Built,true)+(Level3.Index.Tree,mt)+(Level3.Index.Keys,keys));
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
        // shareable as of 26 April 2021
        internal class DistinctCursor : Cursor
        {
            readonly DistinctRowSet _drs;
            readonly MTreeBookmark? _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,E,new TRow(cx._Dom(drs)??throw new PEException("PE49001"), bmk.key()))
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
    // shareable as of 26 April 2021
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
            : base(dp, cx, BTree<long, object>.Empty + (_Domain, r.domain) + (_Source, r.defpos)
                 + (RSTargets, r.rsTargets) + (RowOrder, os) + (Level3.Index.Keys, os)
                 + (Table.LastData, r.lastData) + (ObInfo.Name, r.name??"")
                 + (ISMap, r.iSMap) + (SIMap,r.sIMap)
                 + (ObInfo.Names,r.names) + (Distinct, dct) + (_Depth, r.depth + 1))
        {
            cx.Add(this);
        }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        public static OrderedRowSet operator +(OrderedRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (OrderedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Apply(BTree<long,object> mm, Context cx, BTree<long,object>?m=null)
        {
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
            var sce = (RowSet?)cx.obs[source] ?? throw new DBException("42105");
            var tree = new RTree(sce.defpos, cx, keys,
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = sce.First(cx); e != null; e = e.Next(cx))
            {
                var vs = CTree<long, TypedValue>.Empty;
                cx.cursors += (sce.defpos, e);
                for (var b = rowOrder.First(); b != null; b = b.Next())
                if (cx.obs[b.value()??-1L] is SqlValue s)
                    vs += (s.defpos, s.Eval(cx));
                var rw = new TRow(keys, vs);
                tree +=(rw, SourceCursors(cx));
            }
            return (RowSet)New(cx, E + (_Built, true) + (Level3.Index.Tree, tree.mt) + (_RTree, tree));
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
        // shareable as of 26 April 2021
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
    // shareable as of 26 April 2021
    internal class EmptyRowSet : RowSet
    {
        internal EmptyRowSet(long dp, Context cx,long dm,BList<long?>?us=null,
            CTree<long,Domain>?re=null) 
            : base(dp, _Mem(cx,dm,us,re)) 
        {
            cx.Add(this);
        }
        protected EmptyRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,long dm,BList<long?>? us,
            CTree<long,Domain>? re)
        {
            var dt = cx._Dom(dm) ?? throw new DBException("42105");
            re ??= CTree<long, Domain>.Empty;
            if (us != null && !Context.Match(us,dt.rowType))
                dt = (Domain)dt.Relocate(cx.GetUid()) + (Domain.RowType, us)
                    +(Domain.Representation,dt.representation+re);
            return new BTree<long,object>(_Domain, dt.defpos);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EmptyRowSet(defpos,m);
        }
        public static EmptyRowSet operator+(EmptyRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (EmptyRowSet)rs.New(rs.mem + x);
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
        public ArrayRowSet(long dp, Context cx, SqlValue sv) :
            base(dp, new BTree<long,object>(SqlLiteral._Val, sv.Eval(cx)))
        { }
        protected ArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        protected override Cursor? _First(Context cx)
        {
            var a = (TArray?)mem[SqlLiteral._Val];
            if (a == null) return null;
            return ArrayCursor.New(cx, this, a, 0);
        }
        protected override Cursor? _Last(Context cx)
        {
            var a = (TArray?)mem[SqlLiteral._Val];
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
            internal readonly TArray _ar;
            internal readonly int _ix;
            ArrayCursor(Context cx,ArrayRowSet ars,TArray ar, int ix)
                : base(cx,ars,ix,E,(TRow)ar[ix])
            {
                _ars = ars; _ix = ix;  _ar = ar;
            }
            internal static ArrayCursor? New(Context cx, ArrayRowSet ars, TArray ar, int ix)
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
    /// A RowSet for UNNEST of a Multiset
    /// </summary>
    internal class MultisetRowSet : RowSet
    {
        public MultisetRowSet(long dp, Context cx, SqlValue sv) :
            base(dp, new BTree<long,object>(SqlLiteral._Val, sv.Eval(cx)))
        { }
        protected MultisetRowSet(long dp, BTree<long, object> m) : base(dp, m) { }

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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlRowSet : RowSet
    {
        internal const long
            SqlRows = -413; // BList<long?>  SqlRow
        internal BList<long?> sqlRows =>
            (BList<long?>?)mem[SqlRows]??BList<long?>.Empty;
        internal SqlRowSet(long dp, Context cx, Domain xp, BList<long?> rs)
            : base(dp, cx, _Mem(cx, xp, rs) + (SqlRows, rs)
                  + (_Depth, cx.Depth(rs, new BList<DBObject?>(xp))))
        {
            cx.Add(this);
        }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, Domain dm, BList<long?> rs)
        {
            var ns = BTree<string, long?>.Empty;
            for (var b = dm.Needs(cx).First(); b != null; b = b.Next())
            {
                var p = b.key();
                ns += (cx.NameFor(p), p);
            }
            return BTree<long, object>.Empty + (_Domain, dm.defpos)
                + (ObInfo.Names, ns) + (Asserts, Assertions.AssignTarget);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowSet(defpos,m);
        }
        public static SqlRowSet operator +(SqlRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (SqlRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlRowSet(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            r += (SqlRows, cx.ReplacedLl(sqlRows));
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nw = cx.FixLl(sqlRows);
            if (nw!=sqlRows)
            r += (SqlRows, nw);
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
        // shareable as of 26 April 2021
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
            static TRow _Row(Context cx,SqlRowSet rs,int p)
            {
                var dt = cx._Dom(rs) ?? throw new DBException("42105");
                var dm = new Domain(cx.GetUid(), cx, Sqlx.ROW, dt.representation, dt.rowType, dt.display);
                var vs = CTree<long, TypedValue>.Empty;
                var rv = (SqlRow?)cx.obs[rs.sqlRows[p]??-1L];
                var rd = cx._Dom(rv);
                var n = rd?.rowType.Length;
                if (n < dm.rowType.Length)
                    throw new DBException("22109");
                int j = 0;
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && cx._Dom(cx.obs[dt.rowType[j]??-1L]) is Domain d && rd!=null)
                    vs += (bp, d.Coerce(cx,cx.obs[rd.rowType[j++]??-1L]?.Eval(cx)??TNull.Value));
                return new TRow(dm,vs);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor? New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.sqlRows.First();b!=null;b=b.Next())
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
                for (var b=_bmk.Next();b!=null;b=b.Next())
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        internal const long
            ExplRows = -414; // BList<(long,TRow>)> SqlValue
        internal BList<(long, TRow)> explRows =>
            (BList<(long, TRow)>?)mem[ExplRows] ?? BList<(long, TRow)>.Empty;
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,Domain dt,BList<(long,TRow)>r)
            : base(dp, BTree<long, object>.Empty+(ExplRows,r)+(_Domain,dt.defpos))
        {
            cx.Add(this);
        }
        protected ExplicitRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExplicitRowSet(defpos,m);
        }
        public static ExplicitRowSet operator+(ExplicitRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (ExplicitRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ExplicitRowSet(dp, m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var s = BList<(long,TRow)>.Empty;
            for (var b=explRows.First();b!=null;b=b.Next())
            {
                var (p, q) = b.value();
                s += (p, (TRow)q.Replace(cx,so,sv));
            }
            r += (ExplRows, s);
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
                for (var b=explRows.First();b!=null;b=b.Next())
                {
                    var (p, rw) = b.value();
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                    sb.Append(": ");sb.Append(rw);
                }
                sb.Append(']');
            }
        }
        // shareable as of 26 April 2021
        internal class ExplicitCursor : Cursor
        {
            readonly ExplicitRowSet _ers;
            readonly ABookmark<int,(long,TRow)> _prb;
            ExplicitCursor(Context _cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(_cx,ers,pos,E, prb.value().Item2.ToArray())
            {
                _ers = ers;
                _prb = prb;
            }
            ExplicitCursor(ExplicitCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _ers = cu._ers;
                _prb = cu._prb;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ExplicitCursor(this, cx, p, v);
            }
            internal static ExplicitCursor? New(Context _cx,ExplicitRowSet ers)
            {
                for (var b=ers.explRows.First();b!=null;b=b.Next())
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
                for (var prb = _prb.Next(); prb!=null; prb=prb.Next())
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
    // shareable as of 26 April 2021
    internal class ProcRowSet : RowSet
    {
        internal SqlCall call => (SqlCall)(mem[SqlCall.Call]??throw new DBException("42000"));
        internal ProcRowSet(SqlCall ca, Context cx) 
            :base(cx.GetUid(),cx,_Mem(cx,ca) 
            +(SqlCall.Call,ca)+(_Depth,ca.depth+1))
        {
            cx.Add(this);
        }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlCall ca)
        {
            var m = BTree<long, object>.Empty + (_Domain, ca.domain);
            if (cx._Ob(ca.call) is CallStatement cs && 
                cx._Ob(cs.procdefpos) is Procedure pr && 
                pr.infos[cx.role.defpos] is ObInfo pi &&
                pi.names != BTree<string, long?>.Empty)
                m += (ObInfo.Names, pi.names);
            return m;
        }
        public static ProcRowSet operator+(ProcRowSet p,(long,object)x)
        {
            var (dp, ob) = x;
            if (p.mem[dp] == ob)
                return p;
            return (ProcRowSet)p.New(p.mem + x);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            cx.values += (defpos,call.Eval(cx));
            return (RowSet)New(cx,E+(_Built,true));
        }
        protected override Cursor? _First(Context cx)
        {
            var v = cx.values[defpos];
            return (v==null || v==TNull.Value)?null:ProcRowSetCursor.New(cx, this, (TArray)v);
        }
        protected override Cursor? _Last(Context cx)
        {
            var v = cx.values[defpos];
            return (v==null || v == TNull.Value) ? null : ProcRowSetCursor.New(this, cx, (TArray)v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ProcRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ProcRowSet(dp, mem);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            r += (SqlCall.Call, cx.done[call.defpos]??call);
            return r;
        }
        internal override CTree<long, bool> _ProcSources(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Call: "); sb.Append(call);
        }
        // shareable as of 26 April 2021
        internal class ProcRowSetCursor : Cursor
        {
            readonly ProcRowSet _prs;
            readonly ABookmark<int, TypedValue> _bmk;
            ProcRowSetCursor(Context cx, ProcRowSet prs, int pos,
                ABookmark<int, TypedValue> bmk, TRow rw)
                : base(cx, prs, pos, E, new TRow(rw, cx._Dom(prs) ?? throw new PEException("PE49207")))
            { 
                _prs = prs; _bmk = bmk;
                cx.values += values;
            }
            internal static ProcRowSetCursor? New(Context cx,ProcRowSet prs,TArray ta)
            {
                for (var b = ta.list.First();b!=null;b=b.Next())
                {
                    var r = new ProcRowSetCursor(cx, prs, 0, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            internal static ProcRowSetCursor? New(ProcRowSet prs, Context cx, TArray ta)
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
    /// values, while SqlValue uids are used in other sorts of Activation.
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
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal const long
            _Adapters = -429, // Adapters
            Defaults = -415, // BTree<long,TypedValue>  SqlValue
            ForeignKeys = -392, // CTree<long,bool>  Index
            IxDefPos = -420, // long    Index
            TargetTrans = -418, // BTree<long,long?>   TableColumn,SqlValue
            TransTarget = -419; // BTree<long,long?>   SqlValue,TableColumn
        internal CTree<long, TypedValue> defaults =>
            (CTree<long, TypedValue>?)mem[Defaults] ?? CTree<long, TypedValue>.Empty;
        internal BTree<long, long?> targetTrans =>
            (BTree<long, long?>?)mem[TargetTrans]??BTree<long,long?>.Empty;
        internal BTree<long, long?> transTarget =>
            (BTree<long, long?>?)mem[TransTarget]??BTree<long,long?>.Empty;
        internal CTree<long, bool> foreignKeys =>
            (CTree<long, bool>?)mem[ForeignKeys] ?? CTree<long, bool>.Empty;
        internal BList<long?>? insertCols => (BList<long?>?)mem[Table.TableCols];
        internal TransitionRowSet(TargetActivation cx, TableRowSet ts, RowSet data)
            : base(cx.GetUid(), cx,_Mem(cx, ts, data)
                  +(_Where,ts.where)+(_Data,data.defpos)+(_Depth,Context.Depth(ts,data))
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
                throw new DBException("42105", t?.infos[cx.role.defpos]?.name ?? "??");
            if (cx._Dom(ta) is not Domain da)
                throw new DBException("42105");
            m += (_Domain, ts.domain);
            m += (_Data, data.defpos);
            m += (Target, ta);
            m += (ISMap, ts.iSMap);
            m += (RSTargets, new BTree<long, long?>(ts.target, ts.defpos));
            m += (IxDefPos, t?.FindPrimaryIndex(cx)?.defpos ?? -1L);
            // check now about conflict with generated columns
            var dfs = BTree<long, TypedValue>.Empty;
            if (cx._tty != PTrigger.TrigType.Delete)
                for (var b = da.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && tr.objects[p] is TableColumn tc // i.e. not remote
                     && cx._Dom(tc) is Domain dc && (tc.defaultValue ?? dc.defaultValue) is TypedValue tv
                            && tv != TNull.Value)
                    {
                        dfs += (tc.defpos, tv);
                        cx.values += (tc.defpos, tv);
                    }
            var fk = CTree<long, bool>.Empty;
            for (var tt = t; tt != null;)
            {
                for (var b = tt.indexes.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx.db.objects[c.key()] is Level3.Index ix &&
                            ix.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                            fk += (ix.defpos, true);
                tt = (cx._Ob(tt.nodeType) is NodeType nt && nt.super is NodeType su 
                    && cx._Ob(su.structure) is Table st) ? st : null;
            }
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
        public static TransitionRowSet operator+ (TransitionRowSet trs,(long,object)x)
        {
            var (dp, ob) = x;
            if (trs.mem[dp] == ob)
                return trs;
            return (TransitionRowSet)trs.New(trs.mem + x);
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
        /// Set up a transitioncursor on TableRow dp
        /// </summary>
        /// <param name="cx">A TableActivation</param>
        /// <param name="dp">TableRow defpos in this table</param>
        /// <param name="u">Pending updates to this row</param>
        /// <returns></returns>
        internal Cursor? At(TableActivation cx,long dp,CTree<long,TypedValue>? u)
        {
            if (u!=null)
                cx.pending += (dp, u);
            for (var b = (TransitionCursor?)First(cx); b != null;
                b = (TransitionCursor?)b.Next(cx))
                if (b._ds[cx._trs.target].Item1 == dp)
                    return b;
            return null;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ds = cx.ReplaceTlT(defaults, so, sv);
            if (ds != defaults)
                r += (Defaults, ds);
            var fk = cx.ReplacedTlb(foreignKeys);
            if (fk != foreignKeys)
                r += (ForeignKeys, fk);
            var gt = cx.ReplacedTll(targetTrans);
            if (gt != targetTrans)
                r += (TargetTrans, gt);
            var tg = cx.ReplacedTll(transTarget);
            if (tg != transTarget)
                r += (TransTarget, tg);
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
        // shareable as of 26 April 2021
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TargetCursor? _tgc;
            internal TransitionCursor(TableActivation ta, TransitionRowSet trs, Cursor fbm, int pos,
                Domain iC)
                : base(ta.next ?? throw new PEException("PE49205"), trs, pos, fbm._ds,
                      new TRow(ta._Dom(trs) ?? throw new PEException("PE49206"), iC, fbm))
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
                if (cx.obs[_trs.where.First()?.key() ?? -1L] is SqlValue sv && 
                    cx._Dom(sv)?.kind == Sqlx.CURRENT)
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
        // shareable as of 26 April 2021
        internal class TargetCursor : Cursor
        {
            internal readonly Domain _td;
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor? _fb;
            internal readonly TableRow? _rec;
            TargetCursor(TableActivation ta, Cursor trc, Cursor? fb, Domain td, CTree<long,TypedValue> vals)
                : base(ta, ta._trs.defpos, td, trc._pos, 
                      (fb!=null)?(fb._ds+trc._ds):trc._ds, 
                      new TRow(td, vals))
            { 
                _trs = ta._trs; _td = td; _fb = fb;
                var t = (Table?)ta.db.objects[ta._tgt] ?? throw new DBException("42105");
                var tt = ta._trs.target;
                var p = trc._ds.Contains(tt) ? trc._ds[tt].Item1 : -1L;
                _rec = (fb!=null)?t.tableRows[p]: new TableRow(t, this);
                ta.values += (values, false);
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
                if (trc == null || cx.next==null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var trs = trc._trs;
                if (cx.db.objects[cx._tgt] is not Table t)
                    return null;
    /*            cx.Add(t.framing);
                if (cx._Dom(t) is not Domain dm)
                    return null; */
                var rt = BList<long?>.Empty;
                var rs = CTree<long, Domain>.Empty;
                (rt, rs) = ColsFrom(cx,t,rt,rs);
                if (rt==BList<long?>.Empty)
                    return null;
                var dm = new Domain(t.domain, cx, Sqlx.TABLE, rs, rt, rt.Length);
                var fb = (cx._tty!=PTrigger.TrigType.Insert)?
                    cx.next.cursors[trs.rsTargets[cx._tgt]??-1L]:null;
                rt = trc._trs.insertCols??dm.rowType;
                for (var b = rt.First(); b != null; b = b.Next())
                    if (b.value() is long p &&
                        trs.targetTrans[p] is long tp)
                    {
                        if (trc[tp] is TypedValue v)
                            vs += (p, v);
                        else if (fb?[tp] is TypedValue fv) // for matching cases
                            vs += (p, fv);
                    }
                if (t.nodeType>=0)
                    vs = CheckNodeId(cx,t,vs);
                else
                    vs = CheckPrimaryKey(cx, trc, vs);
                cx.values += vs;
                for (var b = rs.First(); b != null; b = b.Next()) // cs was t.tableCols
                    if (cx.obs[b.key()] is TableColumn tc)
                    {
                        if (check)
                            switch (tc.generated.gen)
                            {
                                case Generation.Expression:
                                    if (vs[tc.defpos] is TypedValue vt && vt!=TNull.Value)
                                        throw new DBException("0U000", tc.NameFor(cx));
                                    cx.values += tc.Frame(vs);
                                    cx.Add(tc.framing);
                                    var v = cx.obs[tc.generated.exp]?.Eval(cx)??TNull.Value;
                                    trc += (cx, tc.defpos, v);
                                    vs += (tc.defpos, v);
                                    break;
                            }
                        var cv = vs[tc.defpos];
                        if (tc.defaultValue != TNull.Value && cv == TNull.Value)
                            vs += (tc.defpos, tc.defaultValue);
                        cv = vs[tc.defpos];
                        if (tc.notNull && cv == TNull.Value)
                            throw new DBException("22206", tc.NameFor(cx));
                        for (var cb = tc.constraints?.First(); cb != null; cb = cb.Next())
                            if (cx.db.objects[cb.key()] is Check ck)
                            {
                                cx.Add(ck.framing);
                                cx.values += ck.Frame(vs);
                                if (cx.obs[ck.search] is SqlValue se && se.Eval(cx) != TBool.True)
                                    throw new DBException("22212", tc.NameFor(cx));
                            }
                    }
                var r = new TargetCursor(cx, trc, fb, dm, vs);
                if (r._rec != null)
                    for (var b = trc._trs.foreignKeys.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is Level3.Index ix &&
                               cx.db.objects[ix.refindexdefpos] is Level3.Index rx
                               && rx.rows != null && ix.MakeKey(r._rec.vals) is CList<TypedValue> k &&
                               k[0] is not TNull)
                        {
                            if (cx.db.objects[ix.adapter] is Procedure ad)
                            {
                                cx = (TableActivation)ad.Exec(cx, ix.keys.rowType);
                                k = ((TRow)cx.val).ToKey();
                            }
                            if (!rx.rows.Contains(k))
                            {
                                for (var bp = cx.pending.First(); bp != null; bp = bp.Next())
                                    if (bp.value() is CTree<long, TypedValue> vk && rx.MakeKey(vk) is CList<TypedValue> pk
                                        && pk.CompareTo(k) == 0)
                                        goto skip;
                                throw new DBException("23000", "missing foreign key "
                                    + cx.NameFor(ix.tabledefpos) + k.ToString());
                            skip:;
                            }
                            if (rx.infos[rx.definer]?.metadata?[Sqlx.MAX]?.ToInt() is int mx && mx!=-1 &&  rx.rows?.Cardinality(k) > mx)
                                throw new DBException("21000");
                        }
                return r;
            }
            static (BList<long?>,CTree<long,Domain>) 
                ColsFrom(Context cx,Table tb,BList<long?> rt,CTree<long,Domain> rs)
            {
                cx.Add(tb.framing);
                if (cx._Ob(tb.nodeType) is NodeType nt && nt.super is NodeType su && cx._Ob(su.structure) is Table st)
                    (rt, rs) = ColsFrom(cx, st, rt, rs);
                for (var b = tb.tableCols.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is TableColumn tc)
                    {
                        rt += tc.defpos;
                        var cd = cx._Dom(tc)??Domain.Null;
                        rs += (tc.defpos, cd);
                    }
                return (rt, rs);
            }
            /// <summary>
            /// Implement the autokey feature: if a key column is an integer or char type,
            /// the engine will pick a suitable unused value. 
            /// The works cleverly for multi-column indexes. 
            /// The transition rowset adds to a private copy of the index as there may
            /// be several rows to add, and the null column(s!) might not be the first key column.
            /// </summary>
            /// <param name="fl"></param>
            /// <param name="ix"></param>
            /// <returns></returns>
            static CTree<long,TypedValue> CheckPrimaryKey(Context cx, TransitionCursor trc,
                CTree<long,TypedValue>vs)
            {
                if (cx is TableActivation ta && ta?.index != null)
                {
                    var k = CList<TypedValue>.Empty;
                    for (var b = ta.index.keys.First(); b != null; b = b.Next())
                        if (b.value() is long p && (cx.obs[p] ?? cx.db.objects[p]) is TableColumn tc
                            && vs[tc.defpos] is TypedValue v)
                        {
                            if (v == TNull.Value && cx._Dom(tc) is Domain dc && ta.index.rows != null)
                            {
                                v = ta.index.rows.NextKey(dc.kind, k, 0, b.key());
                                var tr = (Transaction)cx.db;
                                for (var c = tr.physicals.PositionAt(tr.step); c != null; c = c.Next())
                                    if (c.value() is Record r && r.fields.Contains(tc.defpos)
                                        && r.fields[tc.defpos]?.CompareTo(v) == 0)
                                        v = Inc(v);
                                vs += (tc.defpos, v);
                                cx.values += (trc._trs.targetTrans[tc.defpos]??-1L, v);
                                if (ta.index.MakeKey(vs) is CList<TypedValue> pk)
                                    ta.index += (Level3.Index.Tree, ta.index.rows + (pk,0, -1L));
                            }
                            k += v;
                        }
                }
                return vs;
            }
            static TypedValue Inc(TypedValue v)
            {
                if (v is TChar tc)
                    return new TChar((int.Parse(tc.value) + 1).ToString());
                return new TInt((v.ToInt()??0) + 1);
            }
            /// <summary>
            /// If the nodeId seems to be null, make a unique nodeId, by temporarily giving it
            /// the string version of the new Record ppos. This will be replaced during Commit
            /// to the (unique) ppos of the new Record.
            /// </summary>
            /// <param name="cx"></param>
            /// <param name="t"></param>
            /// <param name="vs"></param>
            /// <returns></returns>
            /// <exception cref="DBException"></exception>
            static CTree<long,TypedValue> CheckNodeId(Context cx,Table t,CTree<long,TypedValue> vs)
            {
                var x = t.FindPrimaryIndex(cx)??throw new DBException("42105");
                var p = x.keys[0]??-1L;
                if (vs[p] is not TypedValue v || v == TNull.Value)
                    vs += (p, new TChar(cx.db.nextPos.ToString()));
                return vs;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                while (cx.next!=null && cx is not TableActivation)
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
        // shareable as of 26 April 2021
        internal class TriggerCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly TargetCursor _tgc;
            internal TriggerCursor(TriggerActivation ta, Cursor tgc,CTree<long,TypedValue>? vals=null)
                : base(ta, ta._trs?.defpos??-1L, ta._Dom(ta._trig)??Domain.Content, tgc._pos, tgc._ds,
                      new TRow(ta._Dom(ta._trig)??throw new PEException("PE49208"), 
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
                while (cx.next!=null && cx is not TriggerActivation)
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
    /// // shareable as of 26 April 2021
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
            Domain dm,bool old)
            : base(dp, cx, _Mem(cx, trs, dm, old)
                  + (RSTargets, trs.rsTargets) + (Target,trs.target)
                  +(_Depth,Context.Depth(trs,dm)))
        {
            cx.Add(this);
        }
        protected TransitionTableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,TransitionRowSet trs,Domain dm,bool old)
        {
            var dat = BTree<long, TableRow>.Empty;
            if ((!old) && cx.newTables.Contains(trs.defpos)
                && cx.newTables[trs.defpos] is BTree<long,TableRow> tda)
                dat = tda;
            else
                for (var b = trs.First(cx); b != null; b = b.Next(cx))
                    for (var c=b.Rec()?.First();c!=null;c=c.Next())
                        dat += (c.value().tabledefpos, c.value());
            var ma = BTree<long, long?>.Empty;
            var rm = BTree<long, long?>.Empty;
            var rb = cx._Dom(trs)?.rowType.First();
            for (var b=dm.rowType.First();b!=null&&rb!=null;b=b.Next(),rb=rb.Next())
            if (b.value() is long p && rb.value() is long q &&  trs.transTarget[q] is long f){
                ma += (f, p);
                rm += (p, f);
            }
            return BTree<long, object>.Empty + (TransitionTable.Old,old)
                + (Trs,trs) + (Data,dat) + (_Map,ma)
                +(RMap,rm) + (_Domain, dm.defpos);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTableRowSet(defpos,m);
        }
        public static TransitionTableRowSet operator+(TransitionTableRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (TransitionTableRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new TransitionTableRowSet(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ts = _trs.Replace(cx, so, sv);
            if (ts != _trs)
                r += (Trs, ts);
            var mp = cx.ReplacedTll(map);
            if (mp != map)
                r += (_Map, mp);
            var rm = cx.ReplacedTll(rmap);
            if (rm != rmap)
                r += (RMap, rm);
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
        // shareable as of 26 April 2021
        internal class TransitionTableCursor : Cursor
        {
            internal readonly TableRowSet.TableCursor _tc;
            internal readonly TransitionTableRowSet _tt;
            TransitionTableCursor(Context cx,TransitionTableRowSet tt,
                TableRowSet.TableCursor tc,ABookmark<long,TableRow> bmk,int pos)
                :base(cx,tt,pos,
                     new BTree<long,(long,long)>(bmk.key(),(bmk.value().defpos,bmk.value().ppos)),
                     new TRow(cx._Dom(tt)??throw new PEException("PE23101"), 
                         tt.rmap,bmk.value().vals))
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
                return (tc != null && tc._bmk!=null) ? 
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
    // shareable as of 26 April 2021
    internal class RowSetSection : RowSet
    {
        internal const long
            Offset = -438, // int
            Size = -439; // int
        internal int offset => (int)(mem[Offset]??0);
        internal int size => (int)(mem[Size] ?? 0);
        internal override Assertions Requires => Assertions.MatchesTarget;
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(_cx.GetUid(),_Mem(s)+(Offset,o)+(Size,c)+(_Source,s.defpos)
                  +(RSTargets,s.rsTargets) + (_Domain,s.domain)
                  +(Table.LastData,s.lastData))
        {
            _cx.Add(this);
        }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(RowSet r)
        {
            var m = BTree<long, object>.Empty + (ISMap, r.iSMap) + (SIMap, r.sIMap);
            if (r.names != BTree<string, long?>.Empty)
                m += (ObInfo.Names, r.names);
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RowSetSection(defpos,m);
        }
        public static RowSetSection operator+(RowSetSection rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (RowSetSection)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new RowSetSection(dp,m);
        }
        protected override Cursor? _First(Context cx)
        {
            var b = ((RowSet?)cx.obs[source])?.First(cx);
            for (int i = 0; b!=null && i < offset; i++)
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
        // shareable as of 26 April 2021
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
    // shareable as of 26 April 2021
    internal class DocArrayRowSet : RowSet
    {
        internal const long
            Docs = -440; // BList<SqlValue>
        internal BList<SqlValue> vals => (BList<SqlValue>?)mem[Docs]??BList<SqlValue>.Empty;
        internal DocArrayRowSet(Context cx, SqlRowArray d)
            : base(cx.GetUid(), _Mem(cx, d)
                  +(_Domain,Domain.TableType.defpos))
        {
            cx.Add(this);
        }
        static BTree<long, object> _Mem(Context cx, SqlRowArray d)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for (int i = 0; i < d.rows.Count; i++)
                    if (cx.obs[d.rows[i]??-1L] is SqlValue v)
                        vs += v;
            return new BTree<long, object>(Docs, vs);
        }
        protected DocArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DocArrayRowSet(defpos,m);
        }
        public static DocArrayRowSet operator+(DocArrayRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (DocArrayRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new DocArrayRowSet(dp, m);
        }
        protected override BTree<long,object> _Replace(Context cx, DBObject so, DBObject sv,BTree<long,object> m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ds = BList<SqlValue>.Empty;
            for (var b = vals.First(); b != null; b = b.Next())
                ds += (SqlValue?)cx.done[b.value().defpos] ?? b.value();
            r += (Docs, ds);
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
        // shareable as of 26 April 2021
        internal class DocArrayBookmark : Cursor
        {
            readonly DocArrayRowSet _drs;
            readonly ABookmark<int, SqlValue> _bmk;

            DocArrayBookmark(Context _cx,DocArrayRowSet drs, ABookmark<int, SqlValue> bmk) 
                :base(_cx,drs,bmk.key(),E,_Row(_cx,bmk.value())) 
            {
                _drs = drs;
                _bmk = bmk;
            }
            static TRow _Row(Context cx,SqlValue sv)
            {
                if (cx._Dom(sv) is Domain dm)
                    return new TRow(dm,new CTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
                throw new PEException("PE1106");
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
                var bmk = ABookmark<int, SqlValue>.Next(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,_drs, bmk);
            }
            protected override Cursor? _Previous(Context _cx)
            {
                var bmk = ABookmark<int, SqlValue>.Previous(_bmk, _drs.vals);
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
    // shareable as of 26 April 2021
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
        public static WindowRowSet operator+(WindowRowSet rs,(long,object)x)
        {
            return (WindowRowSet)rs.New(rs.mem+x);
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
            var dw = cx._Dom(wf) ?? throw new PEException("PE652");
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var od = cx._Dom(w.order)??cx._Dom(w.partition);
            if (od == null)
                return this;
            var tree = new RTree(source,cx,od,TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset((Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.MULTISET, dw)));
            for (var rw = ((RowSet?)cx.obs[source])?.First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(od, rw.values), cx.cursors);
                values.Add(v);
            }
            return (RowSet)New(cx,E+(Multi,values)+(_Built,true)+(Level3.Index.Tree,tree.mt));
        }
        // shareable as of 26 April 2021
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
                var dm = cx._Dom(ws) ?? throw new PEException("PE23200");
                var vs = CTree<long, TypedValue>.Empty;
                var cb = ws.rtree.mt.info.First();
                for (var bb = bmk._key.First(); bb != null && cb != null;
                    bb = bb.Next(), cb = cb.Next())
                    if (cb.value() is long p)
                        vs += (p, bb.value());
                return new WindowCursor(cx, ws.defpos, dm, 0, new TRow(dm, vs));
            }
            internal static Cursor? New(WindowRowSet ws, Context cx)
            {
                ws = (WindowRowSet)ws.Build(cx);
                var bmk = ws.rtree.Last(cx);
                if (bmk == null)
                    return null;
                var dm = cx._Dom(ws) ?? throw new PEException("PE23201");
                var vs = CTree<long, TypedValue>.Empty;
                var cb = ws.rtree.mt.info.First();
                for (var bb = bmk._key.First(); bb != null && cb != null;
                    bb = bb.Next(), cb = cb.Next())
                    if (cb.value() is long p)
                        vs += (p, bb.value());
                return new WindowCursor(cx, ws.defpos, dm, 0, new TRow(dm, vs));
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
            RemoteCols = -373, // BList<long?> SqlValue
            RestValue = -457,   // TArray
            SqlAgent = -256; // string
        internal TArray aVal => (TArray?)mem[RestValue]??throw new PEException("PE1951");
        internal long restView => (long)(mem[_RestView] ?? -1L);
        internal string? defaultUrl => (string?)mem[DefaultUrl];
        internal string sqlAgent => (string?)mem[SqlAgent] ?? "Pyrrho";
        internal CTree<long, string> namesMap =>
            (CTree<long, string>?)mem[RestView.NamesMap] ?? CTree<long, string>.Empty;
        internal long usingTableRowSet => (long)(mem[RestView.UsingTableRowSet] ?? -1L);
        public RestRowSet(Iix lp, Context cx, RestView vw, Domain q)
            : base(lp.dp, _Mem(cx,lp,vw,q) +(_RestView,vw.defpos) +(Infos,vw.infos)
                  +(Asserts,Assertions.SpecificRows|Assertions.AssignTarget))
        {
            cx.Add(this);
            cx.restRowSets += (lp.dp, true);
            cx.versioned = true;
        }
        protected RestRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, Iix lp, RestView vw, Domain q)
        {
            var vs = BList<DBObject>.Empty; // the resolved select list in query rowType order
            var vd = BList<long?>.Empty;
            var dt = cx._Dom(vw) ?? throw new DBException("42105");
            var qn = q?.Needs(cx, -1L, CTree<long, bool>.Empty);
            cx.defs += (vw.NameFor(cx), lp, Ident.Idents.Empty);
            int d;
            var nm = CTree<long, string>.Empty;
            var mn = BTree<string, long?>.Empty;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var ma = BTree<string, SqlValue>.Empty; // the set of referenceable columns in dt
            for (var b = vw.framing.obs.First(); b != null; b = b.Next())
            {
                var c = b.value();
                if (c is SqlValue sc && sc.name != null)
                {
                    ma += (sc.name, sc);
                    mn += (sc.name, c.defpos);
                }
            }
            var ur = (RowSet?)cx.obs[vw.usingTableRowSet];
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) is string ns 
                    && ur?.names.Contains(ns) != true &&
                        vw.framing.obs[mn[ns]??-1L] is not SqlCopy)
                {
                    vd += p;
                    nm += (p, ns);
                }
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue sc && !vs.Has(sc)
                        && (q == null || qn?.Contains(p) == true))
                    vs += sc;
            d = vs.Length;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sc && !vs.Has(sc))
                    vs += sc;
            var fd = (vs == BList<DBObject>.Empty) ? dt : new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs, d);
            var r = BTree<long, object>.Empty + (ObInfo.Name, vw.NameFor(cx))
                   + (_Domain, fd.defpos) + (RestView.NamesMap, nm)
                   + (RestView.UsingTableRowSet, vw.usingTableRowSet)
                   + (Matching, mg) + (RemoteCols, vd) + (ObInfo.Names,mn)
                   + (RSTargets, new BTree<long, long?>(vw.viewPpos, lp.dp))
                   + (_Depth, Context.Depth(fd, vw));
            if (vw.usingTableRowSet < 0 && cx.role != null &&
                cx._Ob(vw.viewPpos) is DBObject ov && ov.infos[cx.role.defpos] is ObInfo vi)
                r += (DefaultUrl, vi.metadata[Sqlx.URL]?.ToString() ??
                    vi.metadata[Sqlx.DESC]?.ToString() ?? "");
            return r;
        }
        public static RestRowSet operator+(RestRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (RestRowSet)rs.New(rs.mem + x);
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
            return new RestRowSet(defpos,m);
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[usingTableRowSet] is TableRowSet ur &&
                cx._Dom(ur)?.representation.Contains(rp)==true) // really
                    return false;
            return base.Knows(cx, rp, ambient);
        }
        internal override string DbName(Context cx)
        {
            var _vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105");
            if (cx._Ob(_vw.viewPpos)?.infos[cx.role.defpos] is ObInfo vi)
                return GetUrl(cx,vi).Item1??"";
            return "";
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new RestRowSet(dp, m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            r += (_RestView, cx.ObReplace(restView, so, sv));
            var ch = false;
            var rc = BList<long?>.Empty;
            var nm = CTree<long, string>.Empty;
            for (var b = remoteCols.First(); b != null; b = b.Next())
                if (b.value() is long op)
                {
                    var p = cx.ObReplace(op, so, sv);
                    rc += p;
                    if (namesMap[op] is string nn)
                        nm += (p, nn);
                    ch = ch || p != op;
                }
            if (ch)
                r = r + (RemoteCols, rc) + (RestView.NamesMap,nm);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var rc = cx.FixLl(remoteCols);
            if (rc != remoteCols)
                r += (RemoteCols, rc);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (RestView.NamesMap, nm);
            var rv = cx.Fix(restView);
            if (rv != restView)
                r += (_RestView, rv);
            return r;
        }
        public HttpRequestMessage GetRequest(Context cx, string url, ObInfo? vi)
        {
            var rv = (RestView?)cx.obs[restView] ?? throw new PEException("PE1410");
            var rq = new HttpRequestMessage
            {
                RequestUri = new Uri(url)
            };
            rq.Headers.Add("UserAgent","Pyrrho " + PyrrhoStart.Version[1]);
            var cu = cx.user ?? throw new DBException("42105");
            var cr = (rv.clientName??cu.name) + ":" + rv.clientPassword;
            var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
            rq.Headers.Add("Authorization", "Basic " + d);
            if (cx.db!=null && vi?.metadata.Contains(Sqlx.ETAG) == true)
            {
                rq.Headers.Add("If-Unmodified-Since",
                    ""+ new THttpDate(((Transaction)cx.db).startTime,
                            vi.metadata.Contains(Sqlx.MILLI)));
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
            var mu = vi?.metadata.Contains(Sqlx.URL) == true;
            if (mu)
            {
                if (url == "" || url==null)
                    url = vi?.metadata[Sqlx.URL]?.ToString();
                sql.Append(url);
                for (var b = matches.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue kn){
                    sql.Append('/'); sql.Append(kn.name);
                    sql.Append('='); sql.Append(b.value());
                }
                url = sql.ToString();
                ss = url.Split('/');
                targetName = ss[5];
            }
            else
            {
                if (url == "" || url==null)
                    url = vi?.metadata[Sqlx.DESC]?.ToString() ?? cx.url;
                if (url == null)
                    return (null, null);
                ss = url.Split('/');
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
            return new CTree<long,Domain>(defpos,cx._Dom(this) ?? throw new DBException("42105"));
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
        internal override RowSet Apply(BTree<long,object> mm, Context cx, BTree<long,object>? m=null)
        {
            // what might we need?
            var xs = ((CTree<long,bool>?)mm[_Where]??CTree<long,bool>.Empty)
                +  ((CTree<long, bool>?)mm[Having] ?? CTree<long, bool>.Empty); 
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
                var gc = cx.GroupCols(gs);
                mm += (GroupCols, gc);
                for (var c = gc.rowType.First(); c != null; c = c.Next())
                    if (c.value() is long p)
                        xs += (p, true);
            }
            var dt = cx._Dom(this) ?? throw new DBException("42105");
            // Collect all operands that we don't have yet
            var ou = CTree<long, bool>.Empty;
            for (var b = xs.First();b!=null;b=b.Next())
            if (cx.obs[b.key()] is SqlValue e)
                for (var c = e.Operands(cx).First(); c != null; c = c.Next())
                    if (!dt.representation.Contains(c.key()))
                        ou += (c.key(), true);
            // Add them to the Domain if necessary
            var rc = dt.rowType;
            var rs = dt.representation;
            var nm = namesMap;
            for (var b = ou.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v && cx._Dom(v) is Domain vd)
                {
                    var p = b.key();
                    rc += p;
                    rs += (p, vd);
                    nm += (p, v.alias ?? v.NameFor(cx));
                }
            var im = mem;
            var ag = (CTree<long, bool>?)mm[Domain.Aggs] ?? CTree<long, bool>.Empty;
            if (ag != CTree<long, bool>.Empty)
            {
                var nc = BList<long?>.Empty; // start again with the aggregating rowType, follow any ordering given
                var ns = CTree<long, Domain>.Empty;
                var gb = ((Domain?)m?[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
                var kb = KnownBase(cx);
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                    {
                        if (v is SqlFunction ct && ct.kind == Sqlx.COUNT && ct.mod == Sqlx.TIMES)
                        {
                            nc += p;
                            ns += (p, Domain.Int);
                        }
                        else if (v.IsAggregation(cx) != CTree<long, bool>.Empty &&
                            cx._Dom(v) is Domain dv)
                            for (var c = ((SqlValue?)cx.obs[p])?.KnownFragments(cx, kb).First();
                                c != null; c = c.Next())
                                if (ns[c.key()] is Domain cd)
                                {
                                    var k = c.key();
                                    nc += k;
                                    ns += (k, cd);
                                }
                                else if (gb.Contains(p))
                                {
                                    nc += p;
                                    ns += (p, dv);
                                }
                    }
                var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc) + (Domain.Aggs, ag);
                im = im + (_Domain, nd.defpos) + (Domain.Aggs, ag);
                if (nm != namesMap)
                    im += (RestView.NamesMap, nm);
                cx.Add(nd);
            }
            return base.Apply(mm,cx,im); 
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override RowSet ApplySR(BTree<long,object> mm, Context cx)
        {
            var dn = cx._Dom(this)??throw new DBException("42105");
            for (var b = mm.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return this;
            var ag = (CTree<long, bool>?)mm[Domain.Aggs] ?? throw new DBException("42105");
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            var r = this;
            if (mm[GroupCols] is Domain gd &&
                mm[Groupings] is BList<long?> l1 && mm[Group] is GroupSpecification gs)
            {
                m += (GroupCols, gd);
                m += (Groupings, l1);
                m += (Group, gs);
                if (gd != groupCols)
                    r = (RestRowSet)Apply(m, cx);
            }
            else
                m += (Domain.Aggs, ag);
            var dm = (Domain?)cx.obs[(long)(mm[AggDomain]??-1L)]??Domain.Null;
            var nc = BList<long?>.Empty; // start again with the aggregating rowType, follow any ordering given
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain?)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    if (v is SqlFunction ct && ct.kind == Sqlx.COUNT && ct.mod == Sqlx.TIMES)
                    {
                        nc += p;
                        ns += (p, Domain.Int);
                    }
                    if (v.IsAggregation(cx) != CTree<long, bool>.Empty &&
                        cx._Dom(v) is Domain dv)
                        for (var c = v.KnownFragments(cx, kb).First();
                            c != null; c = c.Next())
                            if (cx._Dom(c.key()) is Domain fd)
                            {
                                var k = c.key();
                                nc += k;
                                ns += (k, fd);
                            }
                            else if (gb.Contains(p))
                            {
                                nc += p;
                                ns += (p, dv);
                            }
                }
            var d = nc.Length;
            for (var b = dn.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Dom(p) is Domain dd)
                {
                    nc += p;
                    ns += (p, dd);
                }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc,d) + (Domain.Aggs, ag);
            m = m + (_Domain, nd.defpos) + (Domain.Aggs, ag);
            cx.Add(nd);
            r = (RestRowSet)r.New(m);
            cx.Add(r);
            cx.done = od;
            return (RowSet?)cx.obs[defpos]??this; // will have changed
        }

        internal override CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            for (var b = ag.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    for (var c = v.Operands(cx).First(); c != null; c = c.Next())
                        if (namesMap.Contains(c.key()))
                            ma += (b.key(), true);
            return ma;
        }
        protected static BList<long?> _Info(Context cx, Grouping g, BList<long?> gs)
        {
            gs += g.defpos;
            var cs = BList<DBObject>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    cs += v;
            var dm = new Domain(Sqlx.ROW, cx, cs);
            cx.Replace(g, g + (_Domain, dm.defpos));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        internal override RowSet Build(Context cx)
        {
            return RoundTrip(cx);
        }
        RowSet RoundTrip(Context cx)
        {
            if (cx._Dom(this) is not Domain dd)
                throw new DBException("42105");
            if (usingTableRowSet >= 0 && !cx.cursors.Contains(usingTableRowSet))
                throw new PEException("PE389");
            if (cx.obs[restView] is not RestView vw || 
                cx._Ob(vw.viewPpos) is not DBObject ov || ov.infos[cx.role.defpos] is not ObInfo vi)
                throw new PEException("PE1411");
            var (url,targetName) = GetUrl(cx,vi,defaultUrl);
            var sql = new StringBuilder();
            if (url == null)
                throw new DBException("42105");
            var rq = GetRequest(cx,url, vi);
            rq.Headers.Add("Accept", vw?.mime ?? "application/json");
            if (vi?.metadata.Contains(Sqlx.URL) == true)
                rq.Method = HttpMethod.Get;
            else
            {
                rq.Method = HttpMethod.Post;
                sql.Append("select ");
                if (distinct)
                    sql.Append("distinct ");
                var co = "";
                cx.groupCols += (domain, groupCols);
                var hasAggs = dd.aggs!=CTree<long,bool>.Empty;
                for (var b = dd.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue s)
                    {
                        if (hasAggs && (s is SqlCopy || s.GetType().Name=="SqlValue") 
                            && !s.AggedOrGrouped(cx, this))
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
                        if (b.value() is long p && cx.obs[p] is SqlValue sv)
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
                if (cx._Dom(this) is Domain rd && sr != null)
                    a = rd.Parse(0, sr, mime, cx);
                cx.result = or;
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (a is TArray aa)
                        Console.WriteLine("--> " + aa.list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (a?.ToString() ?? "null"));
                }
                var len = (a is TArray ta) ? ta.Length : 1;
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
            for (var b=gs.groups.First();b!=null;b=b.Next())
                ids = Grouped(cx, b.value(), sql, ref cm, ids);
            return ids;
        }
        public string WhereString<V>(Context cx,CTree<long,V>cs) where V: IComparable
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue sv && sv.KnownBy(cx, cs))
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
                    if (tv.dataType.kind == Sqlx.CHAR)
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
                if (cx.obs[b.key()] is SqlValue sv)
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
            ts +=(Target, rsTargets.First()?.key() ?? throw new DBException("42105"));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105");
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105");
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, ts, PTrigger.TrigType.Insert)
                : new RESTActivation(cx, ts, PTrigger.TrigType.Insert);
            return new BTree<long, TargetActivation>(ts.target, ta);
        }
        internal override BTree<long, TargetActivation> Update(Context cx,RowSet fm)
        {
            fm += (Target, rsTargets.First()?.key() ?? throw new DBException("42105"));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105");
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105");
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, fm, PTrigger.TrigType.Update)
                : new RESTActivation(cx, fm, PTrigger.TrigType.Update);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override BTree<long, TargetActivation> Delete(Context cx,RowSet fm)
        {
            fm += (Target, rsTargets.First()?.key()??throw new DBException("42105"));
            var vw = (RestView?)cx.obs[restView] ?? throw new DBException("42105"); 
            var vi = vw.infos[cx.role.defpos] ?? throw new DBException("42105");
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, fm, PTrigger.TrigType.Delete)
                : new RESTActivation(cx, fm, PTrigger.TrigType.Delete);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(' ');
            if (defaultUrl != "")
                sb.Append(defaultUrl);
            sb.Append(" RestView"); sb.Append(Uid(restView));
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
            if (usingTableRowSet >= 0)
            {
                sb.Append(" UsingTableRowSet "); sb.Append(Uid(usingTableRowSet));
            }
        }
        // shareable as of 26 April 2021
        internal class RestCursor : Cursor
        {
            readonly RestRowSet _rrs;
            readonly int _ix;
            RestCursor(Context cx,RestRowSet rrs,int pos,int ix)
                :this(cx,rrs,pos, ix, _Value(cx,rrs,ix))
            { }
            RestCursor(Context cx,RestRowSet rrs,int pos,int ix,TRow tr)
                :base(cx,rrs.defpos,cx._Dom(rrs)??throw new PEException("PE43346"), pos,E,tr)
            {
                _rrs = rrs; _ix = ix;
                var tb = cx._Dom(rrs)?.rowType.First();
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
                if (cx._Dom(rrs) is not Domain dm)
                    throw new PEException("PE43335");
                cx.values += ((TRow)rrs.aVal[pos]).values;
                return new TRow(dm,cx.values);
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
            UrlCol = -446, // long SqlValue
            UsingCols = -259; // BList<long?> SqlValue
        internal long usingTableRowSet => (long)(mem[RestView.UsingTableRowSet] ?? -1L);
        internal long urlCol => (long)(mem[UrlCol] ?? -1L);
        internal long template => (long)(mem[RestTemplate]??-1L);
        internal BList<long?> usingCols =>
            (BList<long?>?)mem[UsingCols]??BList<long?>.Empty;
        public RestRowSetUsing(Iix lp,Context cx,RestView vw, long rp,
            TableRowSet uf)  :base(lp.dp,_Mem(cx,rp,uf) 
                 + (RestTemplate, rp) + (ObInfo.Name, vw.NameFor(cx))
                 + (RSTargets,new BTree<long, long?>(vw.viewPpos, lp.dp))
                 + (RestView.UsingTableRowSet, uf.defpos)  
                 + (ISMap, uf.iSMap) + (SIMap,uf.sIMap)
                 + (SRowType, uf.sRowType))
        {
            cx.Add(this);
        }
        protected RestRowSetUsing(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,long rp, TableRowSet uf)
        {
            var r = BTree<long, object>.Empty;
            var rr = (RowSet?)cx.obs[rp];
            var rd = cx._Dom(rr);
            var ud = cx._Dom(uf);
            if (rr == null || rd == null || ud == null)
                throw new DBException("42105");
            // identify the urlCol and usingCols
            var ab = ud.rowType.Last();
            var ul = ab?.value();
            if (ab == null || ul == null)
                throw new DBException("42105");
            var uc = ud.rowType - ab.key();
            return r + (UsingCols,uc)+(UrlCol,ul) + (_Domain,rd.defpos)
                  +(_Depth, Context.Depth(rd, uf, rr));
        }
        public static RestRowSetUsing operator +(RestRowSetUsing rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (RestRowSetUsing)rs.New(rs.mem + x);
        }
        internal override CTree<long,bool> Sources(Context cx)
        {
            return base.Sources(cx) + (usingTableRowSet,true) + (template,true);
        }
        internal override BTree<long, RowSet> AggSources(Context cx)
        {
            var t = template;
            return new BTree<long, RowSet>(t, (RowSet?)cx.obs[t]??throw new DBException("42105")); // not usingTableRowSet
        }
        internal override CTree<long, Domain> _RestSources(Context cx)
        {
            return ((RowSet?)cx.obs[template])?._RestSources(cx)??CTree<long,Domain>.Empty;
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
            var dt = cx._Dom(this) ?? throw new DBException("42105");
            cx.values += (defpos, new TArray(dt));
            var pos = 0;
            var a = (TArray?)cx.values[defpos] ?? new TArray(dt);
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
            var ru = (RestRowSetUsing)base.Apply(mm,cx);
            // Watch out for situation where an aggregation has successfully moved to the Template
            // as the having condition will no longer work at the RestRowSetUsing level
            var re = (RestRowSet?)cx.obs[ru.template]??throw new DBException("42105");
            var uh = ru.having;
            for (var b = uh.First(); b != null; b = b.Next())
                if (re.having.Contains(b.key()))
                    uh -= b.key();
            if (uh != ru.having)
                ru += (Having, uh);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            r += (RestView.UsingTableRowSet, cx.ObReplace(usingTableRowSet, so, sv));
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nu = cx.Fix(usingTableRowSet);
            if (nu != usingTableRowSet)
                r += (RestView.UsingTableRowSet, nu);
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
            return ((RowSet?)cx.obs[template])?.Insert(cx, ts, rt) ?? throw new DBException("42105");
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
            return ((RowSet?)cx.obs[template])?.Update(cx, fm) ?? throw new DBException("42105");
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
            return ((RowSet?)cx.obs[template])?.Delete(cx, fm) ?? throw new DBException("42105");
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" ViewDomain: ");sb.Append(Uid(domain));
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
                :base(cx,ru,pos,tc._ds,_Value(cx,ru,tc,rc))
            {
                _ru = ru; _tc = tc; 
            }
            static TRow _Value(Context cx,RestRowSetUsing ru,
                TableRowSet.TableCursor tc, RestRowSet.RestCursor rc)
            {
                if (cx._Dom(ru) is not Domain dm)
                    throw new PEException("PE43334");
                return new TRow(dm, tc.values + rc.values - ru.defpos);
            }
            static RestUsingCursor? Get(Context cx, RestRowSetUsing ru, int pos)
            {
                var ls = ((TArray?)cx.values[ru.defpos])?.list;
                if (ls==null || pos < 0 || pos >= ls.Length)
                    return null;
                var cu = (RestUsingCursor?)ls[pos];
                if (cu != null && cx._Dom(ru) is Domain dm)
                {
                    cx.cursors += (cu._tc._rowsetpos, cu._tc);
                    cx.cursors += (ru.defpos, cu);
                    cx.values += cu.values;
                    for (var b = dm.aggs.First(); b != null; b = b.Next())
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
                var ls = ((TArray?)cx.values[ru.defpos])?.list;
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
    // shareable as of 26 April 2021
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
            var r = new BTree<long, object>(TargetTable, tb.defpos);
            var rt = BList<DBObject>.Empty;
            rt += new SqlFormal(cx, "Pos", Domain.Position);
            rt += new SqlFormal(cx, "Action", Domain.Char);
            rt += new SqlFormal(cx, "DefPos", Domain.Position);
            rt += new SqlFormal(cx, "Transaction", Domain.Position);
            rt += new SqlFormal(cx, "Timestamp", Domain.Timestamp);
            return r + (_Domain, new Domain(Sqlx.TABLE,cx,rt).defpos)
                +(Table.LastData,tb.lastData);
        }
        public static LogRowsRowSet operator+(LogRowsRowSet r,(long,object)x)
        {
            var (dp, ob) = x;
            if (r.mem[dp] == ob)
                return r;
            return (LogRowsRowSet)r.New(r.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            r += (TargetTable, cx.ObReplace(targetTable, so, sv));
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
        // shareable as of 26 April 2021
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
                var dm = cx._Dom(lrs);
                if (dm == null)
                    return null;
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
                                if (rc?.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = dm.rowType.First(); c != null; c = c.Next())
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
                                    new TRow(dm, vs));
                            }
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                (ph, _) = db._NextPhysical(b.key());
                                var rc = ph as Record;
                                if (rc?.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = dm.rowType.First(); c != null; c = c.Next())
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
                                    new TRow(dm, vs));
                            }
                        case Physical.Type.Delete:
                        case Physical.Type.Delete1:
                            {
                                (ph, _) = db._NextPhysical(b.key());
                                if (ph is not Delete rc || rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = dm.rowType.First(); c != null; c = c.Next())
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
                                return new LogRowsCursor(cx, lrs, pos, new TRow(dm, vs));
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
    /// // shareable as of 26 April 2021
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
            var db = cx.db ?? throw new DBException("42105");
            var tc = db.objects[cd] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            cx.Add(tc);
            var tb = db.objects[tc.tabledefpos] as Table 
                ?? throw new PEException("PE1502");
            cx.Add(tb);
            var rt = BList<DBObject>.Empty;
            rt += new SqlFormal(cx, "Pos", Domain.Char);
            rt += new SqlFormal(cx, "Value", Domain.Char);
            rt += new SqlFormal(cx, "StartTransaction", Domain.Char);
            rt += new SqlFormal(cx, "StartTimestamp", Domain.Timestamp);
            rt += new SqlFormal(cx, "EndTransaction", Domain.Char);
            rt += new SqlFormal(cx, "EndTimestamp", Domain.Timestamp);
            return BTree<long, object>.Empty 
                + (_Domain,new Domain(Sqlx.TABLE,cx,rt).defpos)
                + (LogRowsRowSet.TargetTable, tb.defpos)
                + (Table.LastData,tb.lastData);
        }
        public static LogRowColRowSet operator+(LogRowColRowSet r,(long,object)x)
        {
            var (dp, ob) = x;
            if (r.mem[dp] == ob)
                return r;
            return (LogRowColRowSet)r.New(r.mem + x);
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
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            r += (LogRowsRowSet.TargetTable, cx.ObReplace(targetTable, so, sv));
            r += (LogCol, cx.ObReplace(logCol, so, sv));
            return r;
        }
        // shareable as of 26 April 2021
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
                if (cx._Dom(lrs) is not Domain dm || cx.db==null)
                    throw new PEException("PE48180");
                var tc = lrs.logCol;
                var rp = nx.Item2;
                if (rp < 0 || cx.db==null || dm==null)
                    return null;
                if (nx.Item1==null)
                    nx = cx.db._NextPhysical(rp);
                for (; nx.Item1 != null;)
                    if (nx.Item1 is Record rc && lrs.logRow == rc.defpos && !rc.fields.Contains(tc))
                    {                    // may be an Update 
                        var b = dm.rowType.First();
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
                        var rb = new LogRowColCursor(cx, lrs, pos, nx, new TRow(dm, vs));
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