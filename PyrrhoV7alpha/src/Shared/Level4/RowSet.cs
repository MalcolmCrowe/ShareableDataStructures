using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System;
using System.IO;
using System.Net;
using System.Reflection;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
            Groupings = -406,   //CList<long>   Grouping
            GroupCols = -461, // Domain
            GroupIds = -408, // CTree<long,Domain> temporary during SplitAggs
            Having = -200, // CTree<long,bool> SqlValue
            ISMap = -213, // CTree<long,long> SqlValue,TableColumn
            _Matches = -182, // CTree<long,TypedValue> matches guaranteed elsewhere
            Matching = -183, // CTree<long,CTree<long,bool>> SqlValue SqlValue (symmetric)
            _Needed = -401, // CTree<long,bool>  SqlValue
            OrdSpec = -184, // CList<long>
            Periods = -185, // BTree<long,PeriodSpec>
            UsingOperands = -411, // CTree<long,long> SqlValue
            Referenced = -378, // CTree<long,bool> SqlValue (referenced columns)
            _Repl = -186, // CTree<string,string> Sql output for remote views
            RestRowSetSources = -331, // CTree<long,bool>    RestRowSet or RestRowSetUsing
            _Rows = -407, // CList<TRow> 
            RowOrder = -404, //CList<long> SqlValue 
            RSTargets = -197, // CTree<long,long> Table TableRowSet 
            SIMap = -214, // CTree<long,long> TableColumn,SqlValue
            Scalar = -95, // bool
            _Source = -151, // RowSet
            Static = -152, // RowSet (defpos for STATIC)
            Stem = -211, // CTree<long,bool> RowSet 
            Target = -153, // long (a table or view for simple IDU ops)
            _Where = -190, // CTree<long,bool> Boolean conditions to be imposed by this query
            Windows = -201; // CTree<long,bool> WindowSpecification
        internal Assertions asserts => (Assertions)(mem[Asserts] ?? Assertions.None);
        internal string name => (string)mem[ObInfo.Name];
        internal CTree<string, long> names => // -> SqlValue
    (CTree<string, long>)mem[ObInfo.Names] ?? CTree<string, long>.Empty;
        /// <summary>
        /// indexes are added where the targets are selected from tables, restviews, and INNER or CROSS joins.
        /// indexes are not added for unions or outer joins
        /// </summary>
        internal CTree<CList<long>, CTree<long, bool>> indexes => // as defined for tables and restview targets
            (CTree<CList<long>, CTree<long, bool>>)mem[Table.Indexes]??CTree<CList<long>, CTree<long, bool>>.Empty;
        /// <summary>
        /// keys are added where the current set of columns includes all the keys from a target index or ordering.
        /// At most one keys entry is currently allowed: ordering if available, then target index
        /// </summary>
        internal CList<long> keys => (CList<long>)mem[Index.Keys] ?? CList<long>.Empty; 
        internal CList<long> ordSpec => (CList<long>)mem[OrdSpec] ?? CList<long>.Empty;
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal CList<long> rowOrder => (CList<long>)mem[RowOrder] ?? CList<long>.Empty;
        internal CTree<long, bool> where =>
            (CTree<long, bool>)mem[_Where] ?? CTree<long, bool>.Empty;
        internal long data => (long)(mem[_Data]??-1L);
        internal CTree<long, TypedValue> filter =>
            (CTree<long, TypedValue>)mem[Filter] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, TypedValue> matches =>
            (CTree<long, TypedValue>)mem[_Matches] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, CTree<long, bool>> matching =>
            (CTree<long, CTree<long, bool>>)mem[Matching]
            ?? CTree<long, CTree<long, bool>>.Empty;
        internal CTree<long,bool> needed =>
            (CTree<long,bool>)mem[_Needed]; // must be initially null
        internal long groupSpec => (long)(mem[Group] ?? -1L);
        internal long target => (long)(mem[Target] // for table-focussed RowSets
            ?? rsTargets.First()?.key() ?? -1L); // for safety
        internal CTree<long, long> rsTargets =>
            (CTree<long, long>)mem[RSTargets] ?? CTree<long, long>.Empty;
        internal int selectDepth => (int)(mem[SqlValue.SelectDepth] ?? -1);
        internal long source => (long)(mem[_Source] ??  -1L);
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>)mem[Assig]
            ?? CTree<UpdateAssignment, bool>.Empty;
        internal CTree<long, long> iSMap =>
            (CTree<long, long>)mem[ISMap] ?? CTree<long, long>.Empty;
        internal CTree<long, long> sIMap =>
            (CTree<long, long>)mem[SIMap] ?? CTree<long, long>.Empty;
        internal BList<TRow> rows =>
            (BList<TRow>)mem[_Rows] ?? BList<TRow>.Empty;
        internal long lastData => (long)(mem[Table.LastData] ?? 0L);
        internal CList<long> remoteCols =>
            (CList<long>)mem[RestRowSet.RemoteCols] ?? CList<long>.Empty;
        /// <summary>
        /// The group specification
        /// </summary>
        internal long group => (long)(mem[Group] ?? -1L);
        internal CList<long> groupings =>
            (CList<long>)mem[Groupings] ?? CList<long>.Empty;
        internal Domain groupCols => (Domain)mem[GroupCols];
        /// <summary>
        /// The having clause
        /// </summary>
        internal CTree<long, bool> having =>
            (CTree<long, bool>)mem[Having] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal CTree<long, bool> window =>
            (CTree<long, bool>)mem[Windows] ?? CTree<long, bool>.Empty;
        internal MTree tree => (MTree)mem[Index.Tree];
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
        { }
        // Compute assertions. Also watch for Matching info from sources
        protected static BTree<long, object> _Mem(Context cx, BTree<long, object> m)
        {
            var dp = (long)(m[_Domain] ?? -1L);
            var dm = (Domain)(cx.obs[dp] ?? cx.db.objects[dp]);
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
            var sce = (RowSet)cx.obs[(long)(m[_Source]??m[_Data]??-1L)];
            if (sce!=null)
                de = Math.Max(de, sce.depth + 1);
            var sd = cx._Dom(sce);
            var sb = sd?.rowType.First();
            for (var b = dm.rowType.First(); b != null; b = b.Next(), sb=sb?.Next())
            {
                var p = b.value();
                var v = cx.obs[b.value()];
                if (!(v is SqlCopy || v is SqlLiteral))
                    a &= ~(Assertions.MatchesTarget|Assertions.ProvidesTarget
                        |Assertions.SimpleCols); 
                if (sb == null ||
                    !cx._Dom(cx.obs[p]).CanTakeValueOf(cx._Dom(cx.obs[sb.value()])))
                    a &= ~Assertions.AssignTarget;
                if (sb == null || p != sb.value())
                    a &= ~Assertions.MatchesTarget;
                if (sce!=null && !sd.representation.Contains(p))
                    a &= ~Assertions.ProvidesTarget;
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
            return (RowSet)rs.New(rs.mem + x);
        }
        internal virtual BList<long> SourceProps => BList<long>.FromArray(_Source,
            JoinRowSet.JFirst, JoinRowSet.JSecond,
            MergeRowSet._Left, MergeRowSet._Right,
            RestRowSetUsing.RestTemplate,RestView.UsingTableRowSet);
        static BList<long> pre = BList<long>.FromArray(Having,Matching, _Where, _Matches);
        static BTree<long, bool> prt = new BTree<long, bool>(Having, true) 
            + (_Where, true) + (Matching, true) + (_Matches, true);
        internal virtual CTree<long,Domain> _RestSources(Context cx)
        {
            var r = CTree<long,Domain>.Empty;
            for (var b = SourceProps.First(); b != null; b = b.Next())
                if (cx.obs[(long)(mem[b.value()]??-1L)] is RowSet rs)
                    r += rs._RestSources(cx);
            return r;
        }
        internal virtual CTree<long, bool> _ProcSources(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = SourceProps.First(); b != null; b = b.Next())
                if (cx.obs[(long)(mem[b.value()] ?? -1L)] is RowSet rs)
                    r += rs._ProcSources(cx);
            return r;
        }
        internal override (DBObject, Ident) _Lookup(long lp, Context cx, string nm, Ident n)
        {
            var dm = cx._Dom(defpos);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var co = cx._Ob(p);
                var ci = (ObInfo)co.mem[cx.db._role];
                if (ci.name == nm)
                {
                    var ob = new SqlValue(n, dm.representation[p],
                        new BTree<long, object>(_From, defpos));
                    cx.Add(ob);
                    return (ob, n.sub);
                }
            }
            return base._Lookup(lp, cx, nm, n);
        }
        internal virtual RowSet Sort(Context cx,CList<long> os,bool dct)
        {
            if (SameOrder(cx, os, rowOrder)) // skip if current rowOrder already finer
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
                {
                    var k = b.key();
                    var sv = (SqlValue)cx.obs[k];
                    if (sv.KnownBy(cx, r))
                        a += (k, true);
                    else if (this is RestRowSet rr && cx.obs[rr.usingTableRowSet] is RowSet ut
                        && sv.KnownBy(cx,ut))
                        a += (k, true);
                    else // allow lateral
                    {
                        if (r.id!=null)
                        for (var c = sv.Needs(cx,r.defpos).First(); c != null; c = c.Next())
                            if (sv.id.iix.lp<0 || sv.id.iix.lp>r.id.iix.lp)
                                goto no;
                        a += (k, true);
                    no:
                        cx.forReview += (k, (cx.forReview[k] ?? CTree<long, bool>.Empty) + (defpos, true));
                    }
                }
                mm = (a == tv) ? mm : (a.Count == 0) ? (mm - p) : (mm + (p, a));
            }
            if (v is CTree<long, TypedValue> vv && vv.Count > 0)
            {
                var a = CTree<long, TypedValue>.Empty;
                for (var b = vv.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var sv = (SqlValue)cx.obs[k];
                    if (sv.KnownBy(cx, r))
                        a += (k, b.value());
                }
                mm = (a == vv) ? mm : (a.Count == 0) ? (mm - p) : (mm + (p, a));
            }
            else if (v is CList<long> lv && lv.Count > 0)
            {
                var a = CList<long>.Empty;
                for (var b = lv.First(); b != null; b = b.Next())
                {
                    var k = b.value();
                    var sv = (SqlValue)cx.obs[k];
                    if (sv.KnownBy(cx, r))
                        a += k;
                }
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
        internal virtual RowSet Apply(BTree<long, object> mm,Context cx,BTree<long,object> m = null)
        {
            m = m ?? mem;
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
            var dm = cx._Dom((long)(m[_Domain]??-1L));
            var recompute = false;
            for (var mb = pre.First(); mb != null; mb = mb.Next())
            {
                var p = mb.value();
                if (mm.Contains(p))
                {
                    var v = mm[p];
                    switch (p)
                    {
                        case Matching:
                            {
                                var mg = (CTree<long, CTree<long, bool>>)v;
                                var rm = (CTree<long,CTree<long,bool>>)m[Matching]??CTree<long,CTree<long,bool>>.Empty;
                                for (var b = mg.First(); b != null; b = b.Next())
                                {
                                    var x = b.key();
                                    var rx = rm[x] ?? CTree<long, bool>.Empty;
                                    var kx = Knows(cx, x);
                                    var mx = b.value();
                                    for (var c = mx.First(); c != null; c = c.Next())
                                    {
                                        var y = c.key();
                                        if (!(kx||Knows(cx, y)))
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
                                var ma = (CTree<long, TypedValue>)mm[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var oa = ma;
                                var mw = (CTree<long,bool>)mm[_Where] ?? CTree<long, bool>.Empty;
                                var mh = (CTree<long, bool>)mm[Having] ?? CTree<long, bool>.Empty;
                                for (var b = w.First(); b != null; b = b.Next())
                                {
                                    var k = b.key();
                                    var sv = (SqlValue)cx.obs[k];
                                    var matched = false;
                                    if (sv is SqlValueExpr se && se.kind == Sqlx.EQL)
                                    {
                                        var le = (SqlValue)cx.obs[se.left];
                                        var ri = (SqlValue)cx.obs[se.right];
                                        var im = (CTree<long, TypedValue>)m[_Matches]??CTree<long,TypedValue>.Empty;
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
                                var ow = (CTree<long, bool>)m[_Where]??CTree<long,bool>.Empty;
                                if (mw.CompareTo(ow)!=0)
                                    m += (_Where, ow+mw);
                                var oh = (CTree<long, bool>)m[Having] ?? CTree<long, bool>.Empty;
                                if (mh.CompareTo(oh)!=0)
                                    m += (Having, mh+oh);
                                break;
                            }
                        case _Matches:
                            {
                                var ma = (CTree<long, TypedValue>)v ?? CTree<long, TypedValue>.Empty;
                                var ms = (CTree<long, TypedValue>)m[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var im = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
                                var ng = im + (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
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
                                    if (ms.Contains(sp))
                                    {
                                        if (ms[sp].CompareTo(b.value()) == 0)
                                            continue;
                                        cx.done = od;
                                        return new EmptyRowSet(defpos, cx, dm.defpos);
                                    }
                                    ms += (sp, b.value());
                                    var me = CTree<long, CTree<long, bool>>.Empty;
                                    var mg = (CTree<long, CTree<long, bool>>)m[Matching] ?? me;
                                    var nm = (CTree<long, CTree<long, bool>>)mm[Matching] ?? me;
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
                }
            }
            for (var mb = mm.First(); mb != null; mb = mb.Next())
            {
                var p = mb.key();
                var v = mb.value();
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
                            for (var b = sg?.First(); b != null; b = b.Next())
                            {
                                var ua = b.key();
                                if ((!Knows(cx,ua.vbl,true))
                                    || !((SqlValue)cx.obs[ua.val]).KnownBy(cx, this,true))
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
                            var nr = (CTree<long, long>)v;
                            var rd = (Domain)cx.obs[(long)m[_Domain]];
                            // We need to ensure that the source rowset supplies all of the restOperands
                            // we now know about.
                            var ch = false;
                            for (var b = nr.First(); b != null; b = b.Next())
                            {
                                var bp = b.key();
                                if (!rd.representation.Contains(bp))
                                {
                                    rd += (cx, (SqlValue)cx.obs[bp]);
                                    ch = true;
                                }
                            }
                            if (ch)
                            {
                                cx.Add(rd);
                                m = m + (_Domain, rd.defpos);
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
            if (mm != BTree<long, object>.Empty && mm.First().key() < 0)
                // Recursively apply any remaining changes to sources
                // On return mm may contain pipeline changes
                for (var b = SourceProps.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (m.Contains(p)) // we have this kind of source rowset
                    {
                        var sp = (long)m[p];
                        if (sp < 0)
                            continue;
                        var ms = mm;
                        if (p == RestView.UsingTableRowSet)
                            ms = ms - Target - _Domain;
                        var sc = (RowSet)cx.obs[sp];
                        sc = sc.Apply(ms, cx);
                        if (sc.defpos!=sp) 
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
            {
                var s = (RowSet)cx.obs[b.key()];
                if (s is SelectRowSet || s is RestRowSet
                    || (this is RestRowSetUsing ru && ru.usingTableRowSet == s.defpos)
                    || !s.AggSources(cx).Contains(sa.defpos))
                    continue;
                var rc = CList<long>.Empty;
                var rs = CTree<long, Domain>.Empty;
                var kb = s.KnownBase(cx);
                for (var c = es.First(); c != null; c = c.Next())
                    for (var d = ((SqlValue)cx.obs[c.key()]).KnownFragments(cx, kb).First();
                          d != null; d = d.Next())
                    {
                        var p = d.key();
                        var v = (SqlValue)cx.obs[p];
                        if (rs.Contains(p))
                            continue;
                        rc += p;
                        rs += (p, cx._Dom(v));
                    }
                if (s is RestRowSetUsing)
                    for (var c = cx._Dom(sa).aggs.First(); c != null; c = c.Next())
                        if (!rs.Contains(c.key()))
                        {
                            var v = (SqlValue)cx.obs[c.key()];
                            rc += v.defpos;
                            rs += (v.defpos, cx._Dom(v));
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
            {
                var s = (RowSet)cx.obs[b.key()];
                kb += cx._Dom(s).representation;
                kb += s.KnownBase(cx);
            }
            return kb;
        }
        internal virtual CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            var sc = (RowSet)cx.obs[source];
            for (var b = ag.First(); b != null; b = b.Next())
                for (var c = ((SqlValue)cx.obs[b.key()]).Operands(cx).First();
                    c != null; c = c.Next())
                    if (sc.Knows(cx, c.key()))
                        ma += (b.key(), true);
            return ma;
        }
        internal CTree<long,CTree<long,bool>> CombineMatching(CTree<long, CTree<long, bool>>p, CTree<long, CTree<long, bool>>q)
        {
            for (var b=q.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var m = b.value();
                if (p.Contains(k))
                    p += (k, p[k] + m);
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
            var dm = cx._Dom(this);
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
            {
                var s = cx.obs[b.value()];
                var v = s?.Eval(cx) ?? TNull.Value;
                if (v.IsNull && sce[b.value()] is TypedValue tv
                    && !tv.IsNull)
                    v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
                vs += (b.value(), v);
            }
            cx.values = oc;
            return new TRow(dm, vs);
        }
        internal virtual string DbName(Context cx)
        {
            return cx.db.name;
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm,
            Grant.Privilege pr = Grant.Privilege.Select, string a=null)
        {
            return (fm>=0)?this+(_From,fm):this;
        }
        internal SqlValue MaybeAdd(Context cx, SqlValue su)
        {
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                if (sv._MatchExpr(cx, su, this))
                    return sv;
            }
            su += (_Alias, alias ?? name ?? cx.Alias());
            Add(cx, su);
            return su;
        }
        /// <summary>
        /// Return true if b is an equal or finer ordering than a
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        internal bool SameOrder(Context cx, CList<long> a, CList<long> b)
        {
            var ab = a?.First();
            var bb = b?.First();
            if (a.CompareTo(b) == 0)
                return true;
            for (; ab != null && bb != null; ab = ab.Next(), bb = bb.Next())
                if (((SqlValue)cx.obs[ab.value()])._MatchExpr(cx, (SqlValue)cx.obs[bb.value()], this))
                    return false;
            return ab==null; //  if b is not null, b is finer ordering
        }
        internal RowSet AddUpdateAssignment(Context cx, UpdateAssignment ua)
        {
            var s = (long)(mem[_Source] ?? -1L);
            if (cx.obs[s] is RowSet rs && rs.Knows(cx,ua.vbl)
                && (ua.val == -1L || ((SqlValue)cx.obs[ua.val]).KnownBy(cx, rs)))
                rs.AddUpdateAssignment(cx, ua);
            var r = this + (Assig, assig + (ua, true));
            cx.Add(r);
            return r;
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, CList<long> rt)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Insert(cx, ts, rt);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Insert(cx, ts, rt);
            return r;
        }
        internal override BTree<long, TargetActivation>Update(Context cx, RowSet fm)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Update(cx, fm);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Update(cx, fm);
            return r;
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Delete(cx, fm);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Delete(cx, fm);
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
        internal virtual RowSet ComputeNeeds(Context cx, Domain xp = null)
        {
            if (needed != null)// || source <0)
                return this;
            var ln = CTree<long, bool>.Empty;
            ln = cx.Needs(ln, this, xp);
            ln = cx.Needs(ln, this, keys);
            ln = cx.Needs(ln, this, rowOrder);
            ln = AllWheres(cx, ln);
            ln = AllMatches(cx, ln);
            for(var b=ln.First();b!=null;b=b.Next())
            {
                var s = cx.obs[b.key()];
                if (Sources(cx).Contains(s.from))
                    ln -= b.key();
            }    
            var r = this + (_Needed,ln);
            return (RowSet)cx.Add(r);
        }
        internal override CTree<long,bool>_Rdc(Context cx)
        {
            var rc = CTree<long,bool>.Empty;
            if (cx.db.autoCommit)
                return rc;
            var dm = cx._Dom(this);
            var d = dm.display;
            if (d == 0)
                d = int.MaxValue;
            for (var b = dm.rowType.First(); b != null && d-- > 0; b = b.Next())
                rc += cx.obs[b.value()]._Rdc(cx);
            return rc;
        }
        internal virtual int Cardinality(Context cx)
        {
            var r = 0;
            if (where==CTree<long,bool>.Empty)
            {
                for (var b=Sources(cx).First();b!=null;b=b.Next())
                    r += ((RowSet)cx.obs[b.key()]).Cardinality(cx);
                return r;
            }
            for (var b = First(cx); b != null; b = b.Next(cx))
                r++;
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RowSet)base._Fix(cx);
            var gs = cx.Fix(r.groupSpec);
            if (gs != r.groupSpec)
                r += (Group, gs);
            var ng = cx.FixLl(r.groupings);
            if (ng != r.groupings)
                r += (Groupings, ng);
            var nk = cx.FixLl(r.keys);
            if (nk != r.keys)
                r += (Index.Keys, nk);
            var xs = cx.FixTLTllb(r.indexes);
            if (xs != r.indexes)
                r += (Table.Indexes, xs);
            var fl = cx.FixTlV(r.filter);
            if (r.filter != fl)
                r += (Filter, fl);
            var no = cx.FixLl(r.rowOrder);
            if (no != r.rowOrder)
                r += (RowOrder, no);
            var nw = cx.FixTlb(r.where);
            if (nw != r.where)
                r += (_Where, nw);
            var nm = cx.FixTlV(r.matches);
            if (nm != r.matches)
                r += (_Matches, nm);
            var mg = cx.FixTTllb(r.matching);
            if (mg != r.matching)
                r += (Matching, mg);
            var ts = cx.FixV(r.rsTargets);
            if (ts!=r.rsTargets)
                r += (RSTargets, ts);
            var tg = cx.Fix(r.target);
            if (tg != r.target)
                r += (Target, tg);
            var ag = cx.FixTub(r.assig);
            if (ag != r.assig)
                r += (Assig, ag);
            var s = (long)(mem[_Source] ?? -1L);
            var ns = cx.Fix(s);
            if (ns != s)
                r += (_Source, ns);
            var na = cx.FixTsl(r.names);
            if (na != r.names)
                r += (ObInfo.Names, na);
            if (tree?.info != null)
            {
                var inf = tree.info.Fix(cx);
                var ks = CList<long>.Empty;
                for (var t = inf; t != null; t = t.tail)
                    ks += t.head;
                r = r + (Index.Tree, new MTree(inf)) + (Index.Keys,ks);
            }
            r += (Domain.Representation, cx._Dom(r).representation);
            r += (Asserts, asserts);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            //        if (wr.rss.Contains(defpos)) If you uncomment: representation will sometimes be incorrect
            //            return wr.rss[defpos];
            var r = (RowSet)base._Relocate(cx);
            r += (Index.Keys, cx.FixLl(keys));
            r += (RowOrder, cx.FixLl(rowOrder));
            r += (Filter, cx.FixTlV(filter));
            r += (_Where, cx.FixTlb(where));
            r += (_Matches, cx.FixTlV(matches));
            r += (Group, cx.Fix(groupSpec));
            r += (Groupings, cx.FixLl(groupings));
            r += (RSTargets, cx.FixV(rsTargets));
            r += (Assig, cx.FixTub(assig));
            if (tree != null)
            {
                var inf = tree.info.Relocate(cx);
                var ks = CList<long>.Empty;
                for (var t = inf; t != null; t = t.tail)
                    ks += t.head;
                r = r + (Index.Tree, new MTree(inf)) + (Index.Keys,ks);
            }
            if (mem.Contains(_Source))
                r = (RowSet)r.New(cx,E+(_Source, cx.Fix((long)mem[_Source])));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos)) // includes the case was==this
                return cx.done[defpos];
            var r = (RowSet)base._Replace(cx, so, sv);
            var de = r.depth;
            var rs = cx._Dom(r);
            rs = (Domain)rs._Replace(cx, so, sv);
            de = _Max(de, rs.depth + 1);
            if (cx.obs[r.source] is RowSet sc)
                de = _Max(de, sc.depth + 1);
            if (rs.defpos != r.domain)
                r += (_Domain, rs.defpos);
            var fl = cx.ReplaceTlT(r.filter, so, sv);
            if (fl != r.filter)
                r += (Filter, fl);
            var xs = cx.ReplacedTLllb(r.indexes);
            if (xs != r.indexes)
                r += (Table.Indexes, xs);
            var ks = cx.ReplacedLl(r.keys);
            if (ks != r.keys)
                r += (Index.Keys, ks);
            r += (SIMap, cx.ReplacedTll(r.sIMap));
            r += (ISMap, cx.ReplacedTll(r.iSMap));
            if (r.mem[ObInfo.Names] is CTree<string, long> ns)
                r += (ObInfo.Names, cx.ReplacedTsl(ns));
            var os = r.ordSpec;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var ow = (SqlValue)cx._Replace(b.value(), so, sv);
                if (b.value() != ow.defpos)
                    os += (b.key(), ow.defpos);
                de = Math.Max(de, ow.depth + 1);
            }
            if (os != r.ordSpec)
                r += (OrdSpec, os);
            var ro = cx.ReplacedLl(r.rowOrder);
            if (ro != r.rowOrder)
                r += (RowOrder, ro);
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
                de = Math.Max(de, v.depth);
            }
            if (w != r.where)
                r += (_Where, w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx._Replace(b.key(), so, sv);
                if (bk.defpos != b.key())
                    ms += (bk.defpos, b.value());
                de = Math.Max(de, bk.depth);
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
            if (r.tree != null)
            {
                var tre = r.tree.Replaced(cx, so, sv);
                r = r + (Index.Tree, tre) + (Index.Keys, tre.Keys());
            }
            var ch = false;
            var ts = CTree<long, long>.Empty;
            for (var b = r.rsTargets.First(); b != null; b = b.Next())
            {
                var v = b.value();
                if (v == defpos)
                    continue;
                var p = cx.ObReplace(v, so, sv);
                var k = cx.done[b.key()]?.defpos ?? b.key();
                if (p != b.value() || k != b.key())
                    ch = true;
                ts += (k, p);
                de = Math.Max(de, cx.obs[p].depth + 1);
            }
            if (ch)
                r += (RSTargets, ts);
            r += (Domain.Representation, cx._Dom(r).representation);
            if (mem.Contains(_Source))
            {
                var s = (long)mem[_Source];
                var n = cx.ObReplace(s, so, sv);
                if (s != n)
                    r = (RowSet)r.New(cx, E + (_Source, n));
                de = Math.Max(de, cx.obs[n].depth);
            }
            if (cx.done.Contains(r.groupSpec))
            {
                var ng = cx.done[r.groupSpec].defpos;
                if (ng != r.groupSpec)
                    r += (Group, ng);
                var og = r.groupings;
                r += (Groupings, cx.ReplacedLl(og));
                if (r.groupings.CompareTo(og) != 0)
                    r += (GroupCols, cx.GroupCols(r.groupings));
            }
            ch = (r != this);
            if (cx._Replace(r.target, so, sv) is RowSet tg && tg.defpos != r.target)
            {
                ch = true;
                r += (Target, tg);
            }
            var ua = CTree<UpdateAssignment, bool>.Empty;
            for (var b = r.assig?.First(); b != null; b = b.Next())
            {
                var na = b.key().Replace(cx, so, sv);
                ua += (na, true);
                de = Math.Max(de, Math.Max(cx.obs[na.vbl].depth, cx.obs[na.val].depth) + 1);
            }
            if (ua != r.assig)
                r += (Assig, ua);
            r += (_Depth, de);
            if (ch)
                cx.Add(r);
            cx.done += (defpos, r);
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            var dm = cx._Dom(this);
            var p = dm.rowType[i];
            if (cx._Ob(p).infos[cx.role.defpos] is ObInfo ci)
                return ci.name;
            if (cx.obs[p] is SqlValue sv && (sv.alias ?? sv.name) is string n)
                return n;
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
            var sc = (RowSet)cx.obs[source];
            if (sc is RestRowSet || sc is SelectRowSet)
                r += (sc.defpos, sc);
            else if (sc!=null)
                r += sc.AggSources(cx);
            return r;
        }
        internal virtual CTree<long,Cursor> SourceCursors(Context cx)
        {
            var p = source;
            var s = (RowSet)cx.obs[p];
            return s.SourceCursors(cx) + (p,cx.cursors[p]);
        }
        internal override DBObject QParams(Context cx)
        {
            var r = base.QParams(cx);
            var m = cx.QParams(matches);
            if (m != matches)
                r += (_Matches, m);
            return r;
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
            var dm = cx._Dom(this);
       //     var ds = dm.display;
       //     if (ds == 0)
       //         ds = dm.Length;
            for (var b = dm.rowType.First(); b != null // && b.key()<ds
                    ; b = b.Next())
                if (b.value() == rp)
                    return true;
            if (ambient && cx.obs[rp] is SqlValue sv 
                && (sv.from < defpos || sv.defpos < defpos) && sv.selectDepth<selectDepth)
                return true;
            return false;
        }
        internal virtual CList<long> Keys()
        {
            return null;
        }
        internal virtual CTree<long, bool> AllWheres(Context cx, CTree<long, bool> nd)
        {
            nd = cx.Needs(nd, this, where);
            if (cx.obs[source] is RowSet sce)
            {
                var ns = sce.AllWheres(cx, nd);
                for (var b = ns.First(); b != null; b = b.Next())
                    nd += (b.key(), b.value());
            }
            return nd;
        }
        internal virtual CTree<long, bool> AllMatches(Context cx, CTree<long, bool>nd)
        {
            nd = cx.Needs(nd, this, matches);
            if (cx.obs[source] is RowSet sce)
            {
                var ns = sce.AllMatches(cx, nd);
                for (var b = ns.First(); b != null; b = b.Next())
                        nd += (b.key(), b.value());
            }
            return nd;
        }
        /// <summary>
        /// Test if the given source RowSet matches the requested ordering
        /// </summary>
        /// <returns>whether the orderings match</returns>
        protected bool SameOrder(Context cx, RowSet q, RowSet sce)
        {
            if (rowOrder == null || rowOrder.Length == 0)
                return true;
            return rowOrder.CompareTo(sce?.rowOrder) == 0;
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            var dm = cx._Dom(this);
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
            for (int i = 0; i < flags.Length; i++)
            {
                var cp = dm.rowType[i];
                var dc = dm.representation[cp];
                flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                if (cx.db.objects[cp] is TableColumn tc)
                    flags[i] += ((tc.notNull) ? 0x100 : 0) +
                        ((tc.generated != GenerationRule.None) ? 0x200 : 0);
            }
        }
        /// <summary>
        /// Bookmarks are used to traverse rowsets
        /// </summary>
        /// <returns>a bookmark for the first row if any</returns>
        protected abstract Cursor _First(Context cx);
        protected abstract Cursor _Last(Context cx);
        public virtual Cursor First(Context cx)
        {
            var rs = Build(cx);
            return rs._First(cx);
        }
        public virtual Cursor Last(Context cx)
        {
            var rs = Build(cx);
            if (!rs.Built(cx))
            {
                var sv = (SqlValue)cx.obs[rs.needed.First().key()];
                throw new DBException("42112", sv.name);
            }
            return rs._Last(cx);
        }
        /// <summary>
        /// Find a bookmark for the given key (base implementation rarely used)
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual Cursor PositionAt(Context _cx, PRow key)
        {
            for (var bm = _First(_cx); bm != null; bm = bm.Next(_cx))
            {
                var k = key;
                for (var b = keys.First(); b != null; b = b.Next())
                    if (_cx.obs[b.value()].Eval(_cx).CompareTo(k._head) != 0)
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
                var s = (RowSet)cx.obs[p];
                Console.WriteLine(s.ToString(cx,i));
                for (var b = s.Sources(cx).First(); b != null; b = b.Next())
                {
                    p = b.key();
                    if (p>=0)
                        show += (i + 1, p);
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
            if (domain>=0)
            {
                sb.Append(' '); ((Domain)cx.obs[domain]).Show(sb);
            }
            Show(sb);
            return sb.ToString();
        }
        internal virtual void Show(StringBuilder sb)
        {
            sb.Append(' '); sb.Append(Uid(defpos));
            if (domain>=0)
            {
                sb.Append(":"); sb.Append(Uid(domain));
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
                        {
                            sb.Append(cn); cn = ",";
                            sb.Append(Uid(d.value()));
                        }
                        sb.Append(")"); sb.Append(Uid(c.key()));
                    }
                sb.Append("]");
            }
            if (keys.Count != 0)
            {
                cm = " key (";
                for (var b = keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (ordSpec != CList<long>.Empty)
            {
                cm = " ordSpec (";
                for (var b = ordSpec.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (rowOrder != CList<long>.Empty)
            {
                cm = " order (";
                for (var b = rowOrder.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (where != CTree<long, bool>.Empty)
            {
                cm = " where (";
                for (var b = where.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
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
                sb.Append(")");
            }
            if (matches != CTree<long, TypedValue>.Empty)
            {
                cm = " matches (";
                for (var b = matches.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append("=");
                    sb.Append(b.value());
                }
                sb.Append(")");
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
            if (groupings!=CList<long>.Empty)
            { 
                sb.Append(" groupings (");
                cm = "";
                for (var b = groupings.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
                if (having != CTree<long, bool>.Empty)
                {
                    sb.Append(" having ("); cm = "";
                    for (var b = having.First(); b != null; b = b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(b.key()));
                    }
                    sb.Append(")");
                }
            }
            if (groupCols!=null)
            {
                sb.Append(" GroupCols(");
                cm = "";
                for (var b=groupCols.rowType.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (rsTargets != CTree<long, long>.Empty)
            {
                sb.Append(" targets: ");
                cm = "";
                for (var b = rsTargets.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append("=");
                    sb.Append(Uid(b.value()));
                }
            }
            if (from!=-1L)
            {  sb.Append(" From: "); sb.Append(Uid(from)); }
            if (mem.Contains(_Source))
            {  sb.Append(" Source: "); sb.Append(Uid(source)); }
            if (data >0)
            { sb.Append(" Data: "); sb.Append(Uid(data)); }
            if (assig.Count > 0) { sb.Append(" Assigs:"); sb.Append(assig); }
            var ro = (CTree<long, long>)mem[UsingOperands];
            if (ro != null && ro.Count>0)
            {
                sb.Append(" UsingOperands "); cm = "(";
                for(var b=ro.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm == ",")
                    sb.Append(")");
            }
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" Asserts ("); sb.Append(asserts);
                sb.Append(")");
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
            TRow rw) : base(cx._Dom(rs), rw.values)
        {
            _rowsetpos = rs.defpos;
            _dom = cx._Dom(rs);
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
            cx.values += values;
            if (cx.result!=rs.defpos)
                cx.funcs -= rs.defpos;
        }
        protected Cursor(Context cx, RowSet rs, int pos, BTree<long,(long,long)> ds, 
            TypedValue[] vs, BTree<long, object> m = null) : base(cx, cx._Dom(rs), vs)
        {
            _rowsetpos = rs.defpos;
            _dom = cx._Dom(rs);
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
            cx.values += values;
            cx.funcs -= rs.defpos;
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
            cx.funcs -= cu._rowsetpos;
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx, long rd, Domain dm, int pos, BTree<long,(long,long)> ds,
            TRow rw, BTree<long, object> m = null) : base(dm, rw.values)
        {
            _rowsetpos = rd;
            _dom = dm;
            _pos = pos;
            _ds = ds;
            cx.cursors += (rd, this);
            cx.values += values;
            cx.funcs -= rd;
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
            cx.funcs -= cu._rowsetpos;
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
        internal abstract Cursor _Fix(Context cx);
        public virtual Cursor Next(Context cx)
        {
            return _Next(cx);
        }
        public virtual Cursor Previous(Context cx)
        {
            return _Previous(cx);
        }
        protected abstract Cursor _Next(Context cx);
        protected abstract Cursor _Previous(Context cx);
        internal ETag _Rvv(Context cx)
        {
            var rs = (RowSet)cx.obs[_rowsetpos];
        //    var dn = rs.DbName(cx);
            var rvv = Rvv.Empty;
            if (rs is TableRowSet ts)
            {
                if ((ts.where == CTree<long, bool>.Empty && ts.matches == CTree<long, TypedValue>.Empty)
                 || cx._Dom(ts).aggs != CTree<long, bool>.Empty)
                    rvv += (ts.target, (-1L, ((Table)cx._Ob(ts.target)).lastData));
                else
                    rvv += (ts.target, cx.cursors[ts.defpos]);
            }
            else
                for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                {
                    var tg = (RowSet)cx.obs[b.value()];
                    if (((tg.where == CTree<long, bool>.Empty && tg.matches == CTree<long, TypedValue>.Empty)
                        || cx._Dom(tg).aggs != CTree<long, bool>.Empty)
                        && cx.obs[b.value()] is TableRowSet t) 
                        rvv += (tg.target, (-1L, ((Table)cx._Ob(t.target)).lastData));
                    else
                        rvv += (tg.target, cx.cursors[b.value()]);
                }
            return new ETag(cx.db,rvv);
        }
        internal bool Matches(Context cx)
        {
            var rs = (RowSet)cx.obs[_rowsetpos];
            if (IsNull)
                return false;
            for (var b = rs.matches.First(); b != null; b = b.Next())
            {
                var v = cx.obs[b.key()].Eval(cx);
                if (v == null || v.CompareTo(b.value()) != 0)
                    return false;
            }
            for (var b = rs.where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        /// <summary>
        /// These methods are for join processing
        /// </summary>
        /// <returns></returns>
        public virtual MTreeBookmark Mb()
        {
            return null; // throw new PEException("PE543");
        }
        public virtual Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            if (Mb().Value() == mb.Value())
                return this;
            return null; 
        }
        internal abstract BList<TableRow> Rec();
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal override object Val()
        {
            return this;
        }
        internal string NameFor(Context cx, int i)
        {
    //        var rs = (RowSet)cx.obs[_rowsetpos];
            var p = _dom.rowType[i];
            return (cx.obs[p] is SqlValue sv)?sv.name:"";
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
/*    /// <summary>
    /// From is named after the SQL reserved word (explicit in most syntax)
    /// which is followed by a table or view reference, called the source.
    /// INSERT INTO also has a From, as does a routine call.
    /// If the FROM T syntax is missing, From._static is used as the RowSet.
    /// We don't call the From constructor until T has been constructed:
    /// T is a rowSet or an instanceable object (table or view).
    /// Often T is a simple target, but there can be many "rsTargets" if T is a
    /// derived table or view.
    /// The FROM keyword follows an SqlValue list that defines the display list
    /// of the From RowSet: the expressions in the list may contain * ellipses,
    /// unknown identifiers, etc: which will be resolved as far as possible by
    /// the From constructor, and any * ellipses expanded.
    /// At this stage this list has been packaged as a Domain Q, whose columns
    /// reference the SqlValues that were specified in the select statement.
    /// The domain R of the From will be an updated version of this, using the
    /// Context's Replace process, resolving (at least the initial part of)
    /// unknown identifiers, and expanding ellipses using the details from T.
    /// R will also contain, as non-display columns (following thedisplay columns)
    /// all columns of T, in case these are referred to in later syntax, 
    /// where T and its components are not directly referenceable. 
    /// If some of these non-display columns are never actually referenced, 
    /// they will be removed during rowset review.
    /// shareable as of 26 April 2021
    /// </summary>
    internal class From : RowSet
    {
        internal override Assertions Requires => Assertions.SimpleCols;
        /// <summary>
        /// Contructor 
        /// This constructor is used in a CRUD operation for a TableReference, i.e.
        /// a Table, View or Rowset (subquery or emptyrowset).
        /// If it is a select operation, then q will be a partially known
        /// set of display columns as specified in the SelectList. If the select list 
        /// contains *, then the set of display columns will be extended to all of the
        /// referenceable columns in the table expression. By the end of parsing
        /// the TableExpression the select list (and the resulting domain of
        /// the SelectRowSet that will be constructed) will be fully known. At this
        /// stage some of the table references may have been constructed. Remember that
        /// the columns in table references will supply the operands for the 
        /// expressions in the select list (not all will be simple).
        /// If it is an Insert operation there may be a column list specified in the cr parameter.
        /// Where conditions etc may folow the TableExpression, so non-referenced columns
        /// in the TableExpression will be added to the select list in case they are 
        /// referenced in where conditions, join conditions or ordering specifications.
        /// The TableReference ic may identify a DBObject ob (Table or View) by name, in which case an
        /// instance of the object will be constructed with the given uid ic.iix.dp.
        /// 
        /// The defining position of instance we will construct may not actually be the same as
        /// the ic.iix.dp we have been given, as our table name or alias may already have an
        /// instance uid (a foward reference noted in cx.defs). It cannot have more than one!
        /// 
        /// If q is null, there will be just one tablereference maybe with named columns, otherwise all columns.
        /// </summary>
        /// <param name="ic">Lexical identifier: ic.ident the name if any,
        ///  ic.iix.lp will be the SELECT uid if different from ic.iix.dp, 
        ///  ic.iix.sd the select depth, ic.iix.dp the table reference uid</param>
        /// <param name="cx">The context</param>
        /// <param name="ob">The object to be instanced</param>
        /// <param name="q">The value list if any</param>
        /// <param name="pr">The access required if not Select</param>
        public From(Ident ic, Context cx, DBObject ob, 
            Grant.Privilege pr = Grant.Privilege.Select,string a=null)
            : this(ic, cx, _Mem(ic, cx, ob, pr, a)) { }
        From(Ident ic, Context cx, BTree<long, object> m)
            : base(cx, ic.iix.dp, m)
        {
            cx.UpdateDefs(ic, this, (string)m[_Alias]);
        }
        public From(long dp, Context cx, SqlCall pc, CList<long> cr = null)
            : base(dp,cx, _Mem(dp, cx, pc, cr))
        { }
        public From(long dp, Context cx, RowSet rs, string a)
            : base(dp, cx, _Mem(cx, rs, dp, a))
        {
            cx.Add(this);
        }
        protected From(long defpos, BTree<long, object> m) : base(defpos, m)
        { }
        public static From operator +(From f, (long, object) x)
        {
            return (From)f.New(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new From(defpos, m);
        }
        /// <summary>
        /// Workshop for the CRUD From constructor
        /// If q is non-null, it will be a TABLE domain with a set of Value columns (the display)
        /// and if there are no stars will know how many there are, and maybe their names or aliases.
        /// We probably won't know their Domains (but they may be literal values, simple names,
        /// expressions or function calls with a a constrained Union Domain result).
        /// Consider instead the set of non-star Ident operands N in these expressions. 
        /// We process them recursively, using Resolve.
        /// At this stage there are four sorts of Ident here:
        ///  (1) Identifiers that have occurred to the left of the current SELECT: may also be
        ///  only partially known but they will eventually be defined by the RowSet they reference.
        ///  (2) Identifiers that reference previous tables in the tableexpression.
        ///  (3) Identifiers i that will be satisfied from the current ob. There are two cases:
        ///  i has form f.i where f is a forward reference (the name or alias of this or a future tablereference)
        ///  i is a simple name which may unambiguously match a column name in this tablereference
        ///  (4) identifiers that will be satisfied from a later table in the tableexpression.
        ///  And all of these identifiers are in cx.defs already (ambiguous references already excluded).
        /// For each instancerowset T in the target list, we traverse q and identify references to T.
        /// We will replace unknown SqlValues by SqlCopy as we go.
        /// Remember that at the end of the From clause we still have where conditions etc which may
        /// create more references to T.
        /// </summary>
        /// <param name="ic">Lexical identifier: ic.ident the name if any,
        ///  ic.iix.lp will be the SELECT uid if different from ic.iix.dp, 
        ///  ic.iix.sd the select depth, ic.iix.dp the table reference uid</param>
        /// <param name="cx">The context</param>
        /// <param name="ob">The DBObject named: this will be instanced</param>
        /// <param name="q">The select list</param>
        /// <param name="pr">The privilege required for CRUD</param>
        /// <param name="cr">The named column list for SqlInsert</param>
        /// <param name="a">The alias for this tablereference if any</param>
        /// <returns>The properties for this instance rowset</returns>
        /// <exception cref="DBException"></exception>
        internal static BTree<long, object> _Mem(Ident ic, Context cx, DBObject ob, Grant.Privilege pr, string a)
        {
            var ts = ob.RowSets(ic, cx, cx._Dom(ob), ic.iix.dp, pr,a); // get the target rowset
            var m = ts.mem;
            var rt = ts.rsTargets;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var tn = ic.ident; // the object name
            cx.iim += (ic.iix.dp, ic.iix);
            var dm = (Domain)cx.obs[(long)m[_Domain]];
            var fa = (pr == Grant.Privilege.Select) ? Assertions.None : Assertions.AssignTarget;
            fa |= ts.asserts & Assertions.SpecificRows;
            m = m + (ObInfo.Name, tn)
                   + (Target, ts.target) + (_Domain, dm.defpos)
                   + (_Source, ts.defpos) + (Matching, mg)
                   + (RSTargets, rt) + (Asserts, fa) 
                   + (Domain.Representation, dm.representation)
                   + (_Depth, cx.Depth(dm, ts, ob));
            if (a != null)
                m += (_Alias, a);
            if (ts.keys != CList<long>.Empty)
                m += (Index.Keys, ts.keys);
            return m;
        }
        static BTree<long, object> _Mem(long dp, Context cx, SqlCall ca, CList<long> cr = null)
        {
            var pc = (CallStatement)cx.obs[ca.call];
            var proc = (Procedure)((Procedure)cx.db.objects[pc.procdefpos]).Instance(dp,cx);
            var prs = new ProcRowSet(dp, ca, cx);
            cx.Add(prs);
            var ma = CTree<string,long>.Empty;
            for (var b=cx._Dom(proc).rowType.First();b!=null;b=b.Next())
                ma += (((SqlValue)cx.obs[b.value()]).name,b.value());
            for (var b = cx._Dom(prs).rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue sv &&
                    sv.domain == Domain.Content.defpos && ma.Contains(sv.name))
                    cx.obs += (sv.defpos, new SqlCopy(sv.defpos, cx, sv.name, ca.defpos, ma[sv.name]));
            var pi = proc.infos[cx.role.defpos];
            var m = BTree<long, object>.Empty;
            if (pi.names != CTree<string, long>.Empty)
                m += (ObInfo.Names, pi.names);
            return m + (Target, pc.procdefpos) + (_Depth, 1 + ca.depth)
                + (_Domain, proc.domain) + (ObInfo.Name, pi.name)
                + (_Source, prs.defpos)
                + (_Depth, cx.Depth(ca,proc,prs));
        }
        static BTree<long, object> _Mem(Context cx,RowSet rs, long dp, string a)
        {
            var r =  BTree<long, object>.Empty
                + (Target, rs.defpos) + (_Depth, 1 + rs.depth)
                + (_Domain, rs.domain) + (ObInfo.Name, a)
                + (ISMap, rs.iSMap) + (SIMap,rs.sIMap) + (ObInfo.Names,rs.names);
            var ma = (CTree<long, CTree<long, bool>>)r[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            if (rs != null && (ma.Count > 0 || rs.matching.Count > 0))
                r += (Matching, rs.CombineMatching(ma, rs.matching));
            return r;
        }
        internal override bool Knows(Context cx, long p)
        {
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
                if (b.value() == p)
                    return true;
            return base.Knows(cx,p);
        }
        public override Cursor First(Context cx)
        {
            return FromCursor.New(cx, this);
        }
        public override Cursor Last(Context cx)
        {
            if (source == Static)
                return new TrivialRowSet(defpos, cx, (TRow)cx._Dom(this).Eval(cx)).Last(cx);
            return FromCursor.New(this, cx);
        }
        protected override Cursor _First(Context cx)
        {
            throw new NotImplementedException();
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException();
        }
        internal override bool Built(Context cx)
        {
            return ((RowSet)cx.obs[source]).Built(cx);
        }
        internal override RowSet Build(Context cx)
        {
            ((RowSet)cx.obs[source]).Build(cx);
            return this;
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[defpos];
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (From)base._Replace(cx, was, now);
            var ch = (r != this);
            if (cx._Replace(target, was, now) is RowSet so && so.defpos != r.target)
            {
                ch = true;
                r += (Target, so);
            }
            var ua = CTree<UpdateAssignment, bool>.Empty;
            for (var b = assig?.First(); b != null; b = b.Next())
                ua += (b.key().Replace(cx, was, now), true);
            if (ua != assig)
                r += (Assig, ua);
            if (ch)
                cx.Add(r);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new From(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (From)base._Relocate(cx);
            r += (Assig, cx.Fix(assig));
            var tg = cx.Fix(target);
            if (tg != target)
                r += (Target, tg);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (From)base._Fix(cx);
            var na = cx.Fix(assig);
            if (assig != na)
                r += (Assig, na);
            var nt = cx.Fix(target);
            if (nt != target)
                r += (Target, nt);
            return r;
        }
        /// <summary>
        /// Accessor: Check a new table check constraint
        /// </summary>
        /// <param name="c">The new Check constraint</param>
        internal void TableCheck(Context _cx, PCheck c)
        {
            var cx = new Context(_cx.db);
            var trs = new TableRowSet(cx.GetUid(), cx, target);
            if (trs.First(cx) != null)
                throw new DBException("44000", c.check).ISO();
        }
        internal override RowSet Sort(Context cx, CList<long> os, bool dct)
        {
            if (SameOrder(cx, os, rowOrder) && ((!dct) || dct==distinct))
                return this;
            var sce = (RowSet)cx.obs[source];
            var ns = sce.Sort(cx, os, dct);
            if (ns.SameOrder(cx, os, ns.rowOrder) && ((!dct) || dct == ns.distinct))
            {
                var r = this + (RowOrder, os) + (Distinct, dct);
                return (RowSet)cx.Add(r);
            }
            return base.Sort(cx, os, dct);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (alias!=null && alias!="") { sb.Append(" Alias "); sb.Append(alias); }
        }
        // shareable as of 26 April 2021
        internal class FromCursor : Cursor
        {
            internal readonly RowSet _trs;
            internal readonly Cursor _bmk;
            FromCursor(Context cx, RowSet trs, Cursor bmk, int pos)
                : base(cx, trs, pos, bmk._ds, trs._Row(cx, bmk))
            {
                _trs = trs;
                _bmk = bmk;
            }
            FromCursor(FromCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _trs = cu._trs;
                _bmk = cu._bmk;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new FromCursor(this, cx, p, v);
            }
            public static FromCursor New(Context cx, RowSet trs)
            {
                var ox = cx.finder;
                var sce = (RowSet)cx.obs[trs.source];
                if (sce!=null)
                    cx.finder = sce.defpos;
                for (var bmk = sce?.First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new FromCursor(cx, trs, bmk, 0);
                    if (rb.Matches(cx) && Eval(trs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            public static FromCursor New(RowSet trs, Context cx)
            {
                var ox = cx.finder;
                var sce = (RowSet)cx.obs[trs.source];
                cx.finder = sce.defpos;
                for (var bmk = sce.Last(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new FromCursor(cx, trs, bmk, 0);
                    if (rb.Matches(cx) && Eval(trs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder = _trs.source;
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new FromCursor(cx, _trs, bmk, _pos + 1);
                    if (rb.Matches(cx) && Eval(_trs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                var ox = cx.finder;
                cx.finder = _trs.source;
                for (var bmk = _bmk.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new FromCursor(cx, _trs, bmk, _pos + 1);
                    if (rb.Matches(cx) && Eval(_trs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _bmk.Rec();
            }

            internal override Cursor _Fix(Context cx)
            {
                return this;
            }
            public override MTreeBookmark Mb()
            {
                return _bmk.Mb();
            }
        }
    } */
    // shareable as of 26 April 2021
    internal class TrivialRowSet: RowSet
    {
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)mem[Singleton];
        internal TrivialRowSet(Context cx,Domain dm) 
            : this(cx.GetUid(),cx,new TRow(dm,CTree<long,TypedValue>.Empty))
        {  }
        internal TrivialRowSet(long dp, Context cx, TRow r,
            Grant.Privilege pr=Grant.Privilege.Select,string a=null)
            : base(dp, _Mem(dp,cx,r.dataType)+(Singleton,r)+(_Alias,a)
                  +(_Ident,new Ident(a,new Iix(dp))))
        {
            cx.Add(this);
        }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,Domain dm)
        {
            var ns = CTree<string, long>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                ns += (cx.NameFor(p), p);
            }
            return BTree<long, object>.Empty + (_Domain, dm.defpos) 
                + (ObInfo.Names, ns) + (_Depth,dm.depth+1);
        }
        public static TrivialRowSet operator +(TrivialRowSet rs, (long, object) x)
        {
            return (TrivialRowSet)rs.New(rs.mem + x);
        }
        protected override Cursor _First(Context _cx)
        {
            return new TrivialCursor(_cx,this,-1L);
        }
        protected override Cursor _Last(Context _cx)
        {
            return new TrivialCursor(_cx, this, -1L);
        }
        internal override int Cardinality(Context cx)
        {
            return 1;
        }
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            for (int i = 0; key != null; i++, key = key._tail)
                if (row[i].CompareTo(key._head) != 0)
                    return null;
            return new TrivialCursor(_cx, this, -1L);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TrivialRowSet(dp,mem);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (TrivialRowSet)base._Fix(cx);
            var nr = row.Fix(cx);
            if (row!=nr)
                r += (Singleton, nr);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RowSet)base._Replace(cx, so, sv);
            var rw = (TRow)row.Replace(cx, so, sv);
            if (rw!=row)
                r += (Singleton, rw);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TrivialRowSet(defpos,m);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" "); sb.Append(row.ToString());
        }
        // shareable as of 26 April 2021
        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t,long d) 
                :base(_cx,t,0,E,t.row)
            {
                trs = t;
            }
            internal TrivialCursor(Context cx,Domain dm)
                :base(cx,null,0,E,new TRow(dm,(PRow)null))
            { }
            TrivialCursor(TrivialCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                trs = cu.trs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TrivialCursor(this, cx, p, v);
            }
            protected override Cursor _Next(Context _cx)
            {
                return null;
            }
            protected override Cursor _Previous(Context _cx)
            {
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
                :base(cx, cx.GetUid(),_Mem(cx,dm,r)+(_Domain,dm)+(_Source,r.defpos)
                     +(RSTargets,new CTree<long,long>(r.target,r.defpos))
                     +(_Depth,cx.Depth(cx._Dom(dm),r))
                     +(Asserts,r.asserts) + (ISMap,r.iSMap) + (SIMap,r.sIMap))
        {
            cx.Add(this);
        } 
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Context cx,long d,RowSet r)
        {
            var dm = (Domain)cx._Ob(d);
            var m = r.mem;
            var ns = CTree<string, long>.Empty;
            for (var b=dm.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                ns += (cx.NameFor(p), p);
            }
            return m + (ObInfo.Names,ns);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectedRowSet(defpos, m);
        }
        internal override bool CanSkip(Context cx)
        {
            var sc = (RowSet)cx.obs[source];
            if (sc == null)
                return false;
            var dm = cx._Dom(this);
            var sd = cx._Dom(sc);
            return dm.rowType.CompareTo(sd.rowType) == 0 && target >= Transaction.Analysing;
        }
        internal override RowSet Apply(BTree<long,object> mm,Context cx,BTree<long,object>m=null)
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
        internal override DBObject Relocate(long dp)
        {
            return new SelectedRowSet(dp,mem);
        }
        public static SelectedRowSet operator +(SelectedRowSet rs, (long, object) x)
        {
            return (SelectedRowSet)rs.New(rs.mem + x);
        }
        internal override bool Built(Context cx)
        {
            return ((RowSet)cx.obs[source]).Built(cx);
        }
        internal override RowSet Build(Context cx)
        {
            if (!Built(cx))
                ((RowSet)cx.obs[source]).Build(cx);
            return this;
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return SelectedCursor.New(this, _cx);
        }
        // shareable as of 26 April 2021
        internal class SelectedCursor : Cursor
        {
            internal readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(cx,srs, pos, bmk._ds, srs._Row(cx,bmk)) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectedCursor(SelectedCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,AllowRvv(cu._srs,p),v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            static long AllowRvv(RowSet rs,long p)
            {
                return p; //??
            }
            SelectedCursor(Context cx,SelectedCursor cu) :base(cx,cu)
            {
                _srs = (SelectedRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = (Cursor)cu._bmk?.Fix(cx);
            }
            internal static SelectedCursor New(Context cx,SelectedRowSet srs)
            {
                var sce = (RowSet)cx.obs[srs.source];
                for (var bmk = sce.First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal static SelectedCursor New(SelectedRowSet srs, Context cx)
            {
                var sce = (RowSet)cx.obs[srs.source];
                for (var bmk = sce.Last(cx); bmk != null; bmk = bmk.Previous(cx))
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
            protected override Cursor _Next(Context cx)
            {
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                for (var bmk = _bmk.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, _srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _bmk.Rec();
            }
            internal override Cursor _Fix(Context cx)
            {
                return new SelectedCursor(cx, this);
            }
        }
    }
    // shareable as of 26 April 2021
    internal class SelectRowSet : RowSet
    {
        internal const long
            RdCols = -124, // CTree<long,bool> TableColumn (needed cols for select list and where)
            ValueSelect = -385; // long SqlValue
        internal CTree<long, bool> rdCols =>
            (CTree<long, bool>)mem[RdCols] ?? CTree<long, bool>.Empty;
        internal long valueSelect => (long)(mem[ValueSelect] ?? -1L);
        internal TRow row => rows[0];
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec (select list defines dm)
        /// Query environment can supply values for the select list but source columns
        /// should bind more closely.
        /// </summary>
        internal SelectRowSet(Iix lp, Context cx, Domain dm, RowSet r,BTree<long,object> m=null)
            : base(cx, lp.dp, _Mem(cx,lp,dm,r,m)) //  not dp, cx, _Mem..
        { }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Context cx,Iix dp,Domain dm,RowSet r,BTree<long,object>m)
        {
            m = m ?? BTree<long, object>.Empty;
            var gr = (long)(m[Group]??-1L);
            var groups = (GroupSpecification)cx.obs[gr];
            var gs = CList<long>.Empty;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                gs = _Info(cx, (Grouping)cx.obs[b.value()], gs);
            m += (Groupings, gs);
            var os = (CList<long>)m[OrdSpec] ?? CList<long>.Empty;
            var di = (bool)(m[Distinct] ?? false);
            if (os.CompareTo(r.rowOrder) != 0 || di != r.distinct)
                r = (RowSet)cx.Add(new OrderedRowSet(cx, r, os, di));
            var rc = CTree<long, bool>.Empty;
            var d = dm.display;
            if (d == 0)
                d = dm.rowType.Length;
            var wh = (CTree<long, bool>)m[_Where];
            // For SelectRowSet, we compute the RdCols: all needs of selectors and wheres
            if (m[ISMap] is CTree<long, long> im)
            {
                for (var b = dm.rowType.First(); b != null && b.key() < d; b = b.Next())
                    for (var c = cx._Ob(b.value()).Needs(cx).First(); c != null; c = c.Next())
                        rc += (im[c.key()], true);
                for (var b = wh?.First(); b != null && b.key() < d; b = b.Next())
                    for (var c = cx._Ob(b.key()).Needs(cx).First(); c != null; c = c.Next())
                        rc += (im[c.key()], true);
            }
            m += (RdCols, rc);
            var ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            if (ma.Count > 0 || r.matching.Count > 0)
                m += (Matching, r.CombineMatching(ma, r.matching));
            for (var b = r.rsTargets.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is TableRowSet tt)
                {
                    // see what indexes we can use
                    if (tt.indexes != CTree<CList<long>, CTree<long,bool>>.Empty)
                    {
                        var xs = CTree<CList<long>,CTree<long,bool>>.Empty;
                        for (var x = tt.indexes.First();x!=null;x=x.Next())
                        {
                            var k = x.key();
                            for (var c = k.First(); c != null; c = c.Next())
                                if (((CTree<long,long>)m[ISMap]).Contains(c.value()))
                                    goto next;
                            var t = tt.indexes[k] ?? CTree<long, bool>.Empty;
                            xs += (k, t+x.value());
                            next:;
                        }
                        m += (Table.Indexes, xs);
                    }
                }
            if (r.keys != CList<long>.Empty)
                m += (Index.Keys, r.keys);
            return m +(_Domain,dm.defpos)+(_Source,r.defpos) + (RSTargets,r.rsTargets)
                +(_Depth,cx.Depth(dm,r)) + (SqlValue.SelectDepth, cx.sD)
                + (Asserts,r.asserts);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectRowSet(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectRowSet(dp,mem);
        }
        public static SelectRowSet operator +(SelectRowSet rs, (long, object) x)
        {
            return (SelectRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string a=null)
        {
            return this;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectRowSet)base._Replace(cx, so, sv);
            var no = so.alias ?? ((so as SqlValue)?.name) ?? so.infos[cx.role.defpos]?.name;
            var nv = sv.alias ?? ((sv as SqlValue)?.name) ?? sv.infos[cx.role.defpos]?.name;
            if (no!=nv)
            {
                var ns = r.names;
                ns = ns + (nv, ns[no]) - no;
                r += (ObInfo.Names, ns);
                cx.Add(r);
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
        protected static CList<long> _Info(Context cx, Grouping g, CList<long> gs)
        {
            gs += g.defpos;
            var cs = BList<SqlValue>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                cs += (SqlValue)cx.obs[b.key()];
            var dm = new Domain(Sqlx.ROW, cx, cs);
            cx.Replace(g, g + (_Domain, dm.defpos));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        internal override bool Built(Context cx)
        {
            var dm = cx._Dom(this);
            if (dm.aggs != CTree<long, bool>.Empty && !cx.funcs.Contains(defpos))
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
                    if (cx._Dom(cx.obs[b.key()]).representation.Contains(rp))
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long,object> m = null)
        {
            var gr = (long)(mm[Group] ?? -1L);
            var groups = (GroupSpecification)cx.obs[gr];
            var gs = groupings;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                gs = _Info(cx, (Grouping)cx.obs[b.value()], gs);
            var am = BTree<long, object>.Empty;
            if (gs != groupings)
            {
                am += (Groupings, gs);
                am += (GroupCols, cx.GroupCols(gs));
                am += (Group, gr);
            }
            if (mm.Contains(Domain.Aggs))
                am += (Domain.Aggs, cx._Dom(this).aggs + (CTree<long, bool>)mm[Domain.Aggs]);
            if (mm.Contains(Having))
                am += (Having, having + (CTree<long, bool>)mm[Having]);
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
            var ag = (CTree<long, bool>)mm[Domain.Aggs];
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            if (mm.Contains(GroupCols))
            {
                m += (GroupCols, mm[GroupCols]);
                m += (Groupings, mm[Groupings]);
                m += (Group, mm[Group]);
                m += (Domain.Aggs, ag);
            }
            else
                // if we get here we have a non-grouped aggregation to add
                m += (Domain.Aggs, ag);
            var nc = CList<long>.Empty; // start again with the aggregating rowType, follow any ordering given
            var dm = (Domain)cx.obs[(long)mm[AggDomain]];
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            var dn = cx._Dom(cx.obs[source]);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var v = (SqlValue)cx.obs[p];
                if (v.IsAggregation(cx) != CTree<long, bool>.Empty)
                    for (var c = ((SqlValue)cx.obs[p]).KnownFragments(cx, kb).First();
                        c != null; c = c.Next())
                    {
                        var k = c.key();
                        if (ns.Contains(k))
                            continue;
                        nc += k;
                        ns += (k, cx._Dom(cx.obs[k]));
                    }
                else if (gb.Contains(p))
                {
                    nc += p;
                    ns += (p, cx._Dom(v));
                }
            }
            var d = nc.Length;
            for (var b=dn.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (ns.Contains(p))
                    continue;
                nc += p;
                ns += (p, cx._Dom(cx.obs[p]));
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
                {
                    var k = b.key();
                    if (((SqlValue)cx.obs[k]).KnownBy(cx, r))
                        hh += (k, true);
                }
                if (hh != CTree<long, bool>.Empty)
                    m += (Having, hh);
                r = (SelectRowSet)New(m);
                cx.Add(r);
            }
            // see if the aggs can be pushed down further
            for (var b = r.SplitAggs(mm, cx).First(); b != null; b = b.Next())
            {
                var a = (RowSet)cx.obs[b.key()];
                var na = a.ApplySR(b.value() + (AggDomain, nd.defpos), cx);
                if (na != a)
                    r.ApplyT(cx, na, nd.representation);
            }
            cx.done = od;
            return (RowSet)cx.obs[defpos]; // will have changed
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
        internal BTree<long, BTree<long, object>> SplitAggs(BTree<long, object> mm, Context cx)
        {
            var r = BTree<long, BTree<long, object>>.Empty;
            var dm = cx._Dom(this);
            var rdct = dm.aggs.Count == 0 && distinct;
            var ss = AggSources(cx);
            var ua = CTree<long, long>.Empty; // all uids -> unique sid
            var ub = CTree<long, bool>.Empty; // uids with unique sids
            if (ss == BTree<long, RowSet>.Empty || !mm.Contains(Domain.Aggs))
                return r;
            for (var b = ss.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var d = cx._Dom(b.value());
                for (var c = d.rowType.First(); c != null; c = c.Next())
                {
                    var u = c.value();
                    if (ub.Contains(u))
                        continue;
                    if (ua.Contains(u))
                    {
                        ub += (u, true);
                        ua -= u;
                    }
                    else
                        ua += (u, k);
                }
            }
            for (var b = ((CTree<long, bool>)mm[Domain.Aggs]).First(); b != null; b = b.Next())
            {
                var a = b.key(); // An aggregating SqlFunction
                var sa = -1L;
                var e = (SqlValue)cx.obs[a];
                if (e is SqlFunction sf && sf.kind == Sqlx.COUNT && sf.mod == Sqlx.TIMES)
                {
                    //if (Sources(cx).Count > 1)
                    //    goto no;
                    for (var c = ss.First(); c != null; c = c.Next())
                    {
                        var ex = r[c.key()] ?? BTree<long, object>.Empty;
                        var eg = (CTree<long, bool>)ex[Domain.Aggs] ?? CTree<long, bool>.Empty;
                        r += (c.key(), ex + (Domain.Aggs, eg + (a, true)));
                    }
                    continue;
                }
                for (var c = e.Operands(cx).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    if (ub.Contains(u) || !ua.Contains(u) ) goto no;
                    if (sa < 0)
                        sa = ua[u];
                    else if (sa != ua[u]) goto no;
                }
                if (sa < 0) goto no;
                var ax = r[sa] ?? BTree<long, object>.Empty;
                var ag = (CTree<long, bool>)ax[Domain.Aggs] ?? CTree<long, bool>.Empty;
                r += (sa, ax + (Domain.Aggs, ag + (a, true)));
            }
            for (var b = ((Domain)mm[GroupCols])?.representation?.First(); b != null; b = b.Next())
            {
                var g = (SqlValue)cx.obs[b.key()]; // might be an expression
                var sa = -1L;
                for (var c = g.Operands(cx).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    if (ub.Contains(u) || !ua.Contains(u)) goto no;
                    if (sa < 0)
                        sa = ua[u];
                    else if (sa != ua[u]) goto no;
                }
                if (sa < 0) goto no;
                var ux = r[sa] ?? BTree<long, object>.Empty;
                var gc = (CTree<long, Domain>)ux[GroupIds] ?? CTree<long, Domain>.Empty; // temporary 
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
                var nc = s.groupCols?.rowType ?? CList<long>.Empty;
                var ns = s.groupCols?.representation ?? CTree<long, Domain>.Empty;
                for (var c = ((CTree<long, Domain>)sp[GroupIds])?.First(); c != null; c = c.Next())
                {
                    var k = c.key();
                    nc += k;
                    ns += (k, c.value());
                }
                if (nc != CList<long>.Empty)
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
            return BTree<long, BTree<long, object>>.Empty;
        }
        internal override bool CanSkip(Context cx)
        {
            var sc = (RowSet)cx.obs[source];
            var sd = cx._Dom(sc);
            var dm = cx._Dom(this);
            return dm.rowType.CompareTo(sd.rowType) == 0 && where.CompareTo(sc.where) == 0;
        }
        internal override RowSet Build(Context cx)
        {
            if (Built(cx))
                return this;
            var dm = cx._Dom(this);
            cx.groupCols += (domain, groupCols);
            if (dm.aggs != CTree<long, bool>.Empty)
            {
                var sce = (RowSet)cx.obs[source];
                SqlFunction countStar= null;
                if (dm.display == 1 && cx.obs[dm.rowType[0]] is SqlFunction sf
                    && _ProcSources(cx).Count == 0
                    && sf.kind == Sqlx.COUNT && sf.mod == Sqlx.TIMES && where == CTree<long, bool>.Empty)
                    countStar = sf;
                if (countStar!=null && sce.Built(cx) && _RestSources(cx).Count == 0)
                {
                    var v = sce.Build(cx)?.Cardinality(cx) ?? 0;
                    var rg = new Register(cx, TRow.Empty, countStar);
                    rg.count = v;
                    var cp = dm.rowType[0];
                    cx.funcs += (defpos,cx.funcs[defpos]??BTree<TRow,BTree<long,Register>>.Empty+
                        (TRow.Empty, (cx.funcs[defpos]?[TRow.Empty] ?? BTree<long, Register>.Empty)
                        + (cp, rg)));
                    var sg = new TRow(dm, new CTree<long, TypedValue>(cp, new TInt(v)));
                    cx.values += (defpos, sg);
                    return (RowSet)New(cx, E + (_Built, true) + (_Rows, new BList<TRow>(sg)));
                }
                cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty);
                for (var rb = sce.First(cx); rb != null; rb = rb.Next(cx))
                    if (!rb.IsNull)
                    {
                        var ad = false;
                        for (var b = _RestSources(cx).First(); (!ad) && b != null; b = b.Next())
                            ad = b.value().aggs != CTree<long, bool>.Empty;
                        if(ad)
                            goto next; // AddIns have been done from Json return (at least if ss.Count==1?!)
                        if (groupings.Count == 0)
                            for (var b = dm.aggs.First(); b != null; b = b.Next())
                                ((SqlFunction)cx.obs[b.key()]).AddIn(TRow.Empty, cx);
                        else for (var g = groupings.First(); g != null; g = g.Next())
                            {
                                var gg = (Grouping)cx.obs[g.value()];
                                var vals = CTree<long, TypedValue>.Empty;
                                for (var gb = gg.keys.First(); gb != null; gb = gb.Next())
                                {
                                    var p = gb.value();
                                    var kv = cx.obs[p].Eval(cx);
                                    if (kv == null || kv.IsNull)
                                        throw new DBException("22004", ((SqlValue)cx.obs[p]).name);
                                    vals += (p, kv);
                                }
                                var key = new TRow(groupCols, vals);
                                for (var b = dm.aggs.First(); b != null; b = b.Next())
                                    ((SqlFunction)cx.obs[b.key()]).AddIn(key, cx);
                            }
                        next:;
                    }
                var rws = BList<TRow>.Empty;
                var oc = cx.cursors;
                cx.cursors = BTree<long, Cursor>.Empty;
                for (var b = cx.funcs[defpos]?.First(); b != null; b = b.Next())
                {
                    cx.values = CTree<long, TypedValue>.Empty;
                    // Remember the aggregating SqlValues are probably not just aggregation SqlFunctions
                    // Seed the keys in cx.values
                    var vs = b.key().values;
                    cx.values += vs;
                    for (var d = matching.First(); d != null; d = d.Next())
                        if (cx.values[d.key()] is TypedValue v)
                            for (var c = d.value().First(); c != null; c = c.Next())
                                vs += (c.key(), v);
                    // and the aggregate function accumulated values
                    for (var c = dm.aggs.First(); c != null; c = c.Next())
                    {
                        var v = (SqlValue)cx.obs[c.key()];
                        cx.values += (v.defpos, v.Eval(cx));
                    }
                    // compute the aggregation expressions from these seeds
                    var ovs = vs;
                    for (var c = dm.rowType.First();c!=null;c=c.Next())
                    {
                        var sv = (SqlValue)cx.obs[c.value()];
                        if (sv.IsAggregation(cx)!=CTree<long,bool>.Empty)
                            vs += (sv.defpos, sv.Eval(cx));
                    }
                    if (vs == ovs) // nothing for this rowset
                        continue;
                    // for the having calculation to work we must ensure that
                    // having uses the uids that are in aggs
                    for (var h = having.First(); h != null; h = h.Next())
                        if (cx.obs[h.key()].Eval(cx) != TBool.True)
                            goto skip;
                    rws += new TRow(dm, vs);
                skip:;
                }
                cx.cursors = oc;
                if (rws == BList<TRow>.Empty)
                    rws += new TRow(dm, CTree<long, TypedValue>.Empty);
                return (RowSet)New(cx, E + (_Rows, rws) + (_Built, true) + (Index.Tree, null)
                    + (Index.Keys, groupings));
            }
            return (RowSet)New(cx, E + (_Built, true));
        }
        protected override Cursor _First(Context _cx)
        {
            var dm = _cx._Dom(this);
            _cx.rdC += rdCols;
            if (dm.aggs != CTree<long, bool>.Empty)
            {
                if (groupings==CList<long>.Empty)
                    return EvalBookmark.New(_cx, this);
                return GroupingBookmark.New(_cx, this);
            }
            return SelectCursor.New(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            var dm = _cx._Dom(this);
            if (dm.aggs != CTree<long, bool>.Empty)
            {
                if (groupings == CList<long>.Empty)
                    return EvalBookmark.New(_cx, this);
                return GroupingBookmark.New(this, _cx);
            }
            return SelectCursor.New(this, _cx);
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, CList<long> rt)
        {
            return cx.obs[source].Insert(cx, ts, rt);
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm)
        {
            return cx.obs[source].Delete(cx,fm);
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm)
        {
            return cx.obs[source].Update(cx,fm);
        }
        // shareable as of 26 April 2021
        internal class SelectCursor : Cursor
        {
            readonly SelectRowSet _srs;
            readonly Cursor _bmk; // for rIn, not used directly for Eval
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
            SelectCursor(Context cx,SelectCursor cu):base(cx,cu)
            {
                _srs = (SelectRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = (Cursor)cu._bmk?.Fix(cx);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectCursor(this, cx,p, v);
            }
            internal static SelectCursor New(Context cx, SelectRowSet srs)
            {
                var sce = (RowSet)cx.obs[srs.source];
                for (var bmk = sce.First(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                   var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal static SelectCursor New(SelectRowSet srs, Context cx)
            {
                for (var bmk = ((RowSet)cx.obs[srs.source]).Last(cx);
                      bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                for (var bmk = _bmk.Next(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                for (
                    var bmk = _bmk.Previous(cx); 
                    bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx, rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _bmk.Rec();
            }
            internal override Cursor _Fix(Context cx)
            {
                return new SelectCursor(cx,this);
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
            GroupingBookmark(Context cx, GroupingBookmark cu) : base(cx, cu)
            {
                _grs = (SelectRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _ebm = _grs.rows.PositionAt(cu?._pos ?? 0);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new GroupingBookmark(this, cx, p, v);
            }
            internal static GroupingBookmark New(Context cx, SelectRowSet grs)
            {
                var ebm = grs.rows?.First();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                return r;
            }
            internal static GroupingBookmark New(SelectRowSet grs, Context cx)
            {
                var ebm = grs.rows?.Last();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                return r;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            protected override Cursor _Next(Context cx)
            {
                var ebm = _ebm.Next();
                if (ebm == null)
                    return null;
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                return r;
            }
            protected override Cursor _Previous(Context cx)
            {
                var ebm = _ebm.Previous();
                if (ebm == null)
                    return null;
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                return r;
            }
            internal override Cursor _Fix(Context cx)
            {
                return new GroupingBookmark(cx, this);
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
                : base(_cx, ers, 0, E, ers.row)
            {
                _ers = ers;
            }
            internal static EvalBookmark New(Context cx,SelectRowSet ers)
            {
                if (ers.row == null)
                    return null;
                return new EvalBookmark(cx, ers);
            }
            EvalBookmark(Context cx, EvalBookmark cu) : base(cx, cu)
            {
                _ers = (SelectRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
            }
            EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _ers = cu._ers;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new EvalBookmark(this, cx, p, v);
            }
            protected override Cursor _Next(Context _cx)
            {
                return null; // just one row in the rowset
            }
            protected override Cursor _Previous(Context cx)
            {
                return null; // just one row in the rowset
            }
            internal override Cursor _Fix(Context cx)
            {
                return new EvalBookmark(cx, this);
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
            SRowType = -215; // CList<long>   TableColumn
        internal CTree<long, bool> referenced =>
            (CTree<long, bool>)mem[Referenced] ?? CTree<long, bool>.Empty;
        internal CList<long> sRowType =>
            (CList<long>)mem[SRowType] ?? CList<long>.Empty;
        internal override Assertions Requires => Assertions.SimpleCols;
        protected InstanceRowSet(long dp, Context cx, BTree<long, object> m)
            : base(cx, dp, _Mem1(cx,m)) 
        { }
        protected InstanceRowSet(long dp, BTree<long, object> m)
            : base(dp, m) 
        { }
        static BTree<long,object> _Mem1(Context cx,BTree<long,object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            var ism = CTree<long, long>.Empty; // SqlValue, TableColumn
            var sim = CTree<long, long>.Empty; // TableColumn,SqlValue
            var dm = cx._Dom((long)(m[_Domain]??-1L));
            if (dm.defpos >= 0)
                m += (_Depth, cx.Depth(dm));
            var sr = (CList<long>)m[SRowType];
            var ns = CTree<string, long>.Empty;
            var fb = dm.rowType.First();
            for (var b = sr.First(); b != null && fb != null;
                b = b.Next(), fb = fb.Next())
            {
                var sp = b.value();
                var ip = fb.value();
                ism += (ip, sp);
                sim += (sp, ip);
                var ob = cx._Ob(sp);
                ns += (ob.alias ?? ob.NameFor(cx), ip);
            }
            if (cx.obs[(long)(m[Target]??-1L)] is Table tb)
                m += (Table.Indexes,tb.IIndexes(sim));
            return m + (ISMap, ism) + (SIMap, sim) + (ObInfo.Names,ns);
        }
        public static InstanceRowSet operator +(InstanceRowSet rs, (long, object) x)
        {
            return (InstanceRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InstanceRowSet)base._Replace(cx, so, sv);
            var srt = cx.ReplacedLl(r.sRowType);
            if (srt!=sRowType)
                r += (SRowType, srt);
            var sim = cx.ReplacedTll(r.sIMap);
            if (sim != r.sIMap)
                r += (SIMap, sim);
            var ism = cx.ReplacedTll(r.iSMap);
            if (ism != r.iSMap)
                r += (ISMap, ism);
            var xs = cx.ReplacedTLllb(r.indexes);
            if (xs != r.indexes)
                r += (Table.Indexes, xs);
            var ks = cx.ReplacedLl(r.keys);
            if (ks != r.keys)
                r += (Index.Keys, ks);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (InstanceRowSet)base._Fix(cx);
            var srt = cx.FixLl(r.sRowType);
            if (srt != r.sRowType)
                r += (SRowType, srt);
            var sim = cx.FixTll(r.sIMap);
            if (sim != r.sIMap)
                r += (SIMap, sim);
            var ism = cx.FixTll(r.iSMap);
            if (ism != r.iSMap)
                r += (ISMap, ism);
            var xs = cx.FixTLTllb(r.indexes);
            if (xs!=r.indexes)
                r += (Table.Indexes, xs);
            var ks = cx.FixLl(r.keys);
            if (ks != r.keys)
                r += (Index.Keys, ks);
            return cx._Add(r);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (InstanceRowSet)base._Relocate(cx);
            r += (SRowType, cx.FixLl(sRowType));
            r += (SIMap, cx.FixTll(sIMap));
            r += (ISMap, cx.FixTll(iSMap));
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
            for (var b=sRowType.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" ISMap:("); cm = "";
                for (var b = iSMap.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                    sb.Append("="); sb.Append(Uid(b.value()));
                }
                sb.Append(")");
                sb.Append(" SIMap:("); cm = "";
                for (var b = sIMap.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                    sb.Append("="); sb.Append(Uid(b.value()));
                }
                sb.Append(")");
                sb.Append(" Referenced: ("); cm = "";
                for (var b = referenced.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
        }
    }
    /// <summary>
    /// A TableRowSet is constructed for each TableReference
    /// accessible from the current role.
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
        internal TableRowSet(long lp,Context cx, long t, BTree<long,object> m = null)
            : base(lp, cx, _Mem(lp,cx,t,m))
        { }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(long dp, Context cx, long t, BTree<long,object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            var tb = (Table)cx._Ob(t);
            var n = tb.NameFor(cx) ?? throw new DBException("42105");
            var tn = new Ident(n, cx.Ix(dp));
            var rt = CList<long>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var tr = CTree<long, long>.Empty;
            var ns = CTree<string, long>.Empty;
            for (var b = tb.tableCols.First(); b != null; b = b.Next())
            {
                var tc = ((TableColumn)cx.db.objects[b.key()]);
                var nc = new SqlCopy(cx.GetUid(), cx, tc.NameFor(cx), dp, tc,
                    BTree<long,object>.Empty+(SqlValue.SelectDepth,cx.sD));
                var cd = b.value();
                if (cd != Domain.Content)
                    cd = (Domain)cd.Instance(dp, cx, null);
                nc += (_Domain, cd.defpos);
                cx.Add(nc);
                rt += nc.defpos;
                rs += (nc.defpos, cd);
                tr += (tc.defpos, nc.defpos);
                ns += (nc.alias??nc.name, nc.defpos);
            }
            var xs = CTree<CList<long>, CTree<long, bool>>.Empty;
            CList<long> pk = null;
            for (var b = tb.indexes.First(); b != null; b = b.Next())
            {
                var nk = CList<long>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    nk += tr[c.value()];
                if (pk == null)
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is Index x && x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                            pk = nk;
                xs += (nk, b.value());
            }
            if (pk != null)
                m += (Index.Keys,pk);
            var rr = CList<long>.Empty;
            if (tb.defpos < 0) // system tables have columns in the wrong order!
            {
                for (var b = rt.Last(); b != null; b = b.Previous())
                    rr += b.value();
                rt = rr;
            }
            var dm = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rt, rt.Length);
            cx.Add(dm);
            cx.AddDefs(tn, dm, (string)m[_Alias]);
            var r = (m ?? BTree<long, object>.Empty) + (_Domain, dm.defpos) + (Target, t)
                + (SRowType, cx._Dom(tb).rowType) + (Table.Indexes, xs)
                + (Table.LastData, tb.lastData) + (SqlValue.SelectDepth,cx.sD)
                + (Target,t) + (ObInfo.Names,ns) + (ObInfo.Name,tn.ident)
                + (_Ident,tn)
                + (_Depth, 2)+(RSTargets,new CTree<long,long>(t,dp));
            r += (Asserts, (Assertions)_Mem(cx, r)[Asserts] |Assertions.SpecificRows);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        internal override CTree<long, bool> AllWheres(Context cx, CTree<long, bool> nd)
        {
            nd = cx.Needs(nd, this, where);
            if (cx.obs[source] is RowSet sce)
            {
                var ns = sce.AllWheres(cx, nd);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, true);
                }
            }
            return nd;
        }
        internal override CTree<long, bool> AllMatches(Context cx, CTree<long, bool> nd)
        {
            nd = cx.Needs(nd, this, matches);
            if (cx.obs[source] is RowSet sce)
            {
                var ns = sce.AllMatches(cx, nd);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, true);
                }
            }
            return nd;
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long,object> m = null)
        {
            if (mm == BTree<long, object>.Empty)
                return this;
            m = m ?? mem;
            if (mm[_Where] is CTree<long, bool> w)
            {
                for (var b = w.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var imm = (CTree<long, TypedValue>)mm[_Matches]??CTree<long,TypedValue>.Empty;
                    if (cx.obs[k] is SqlValueExpr se && se.kind == Sqlx.EQL)
                    {
                        var le = (SqlValue)cx.obs[se.left];
                        var ri = (SqlValue)cx.obs[se.right];
                        if (le.isConstant(cx) && !imm.Contains(ri.defpos))
                            mm += (_Matches, imm + (ri.defpos, le.Eval(cx)));
                        if (ri.isConstant(cx) && !imm.Contains(le.defpos))
                            mm += (_Matches,imm + (le.defpos, ri.Eval(cx)));
                    }
                }
            }
            if (mm[_Matches] is CTree<long,TypedValue> ma && rowOrder==CList<long>.Empty)
            {
                var trs = this;
                var (index, nmt, match) = trs.BestForMatch(cx, ma);
                if (index == null)
                    index = trs.BestForOrdSpec(cx, (CList<long>)m[OrdSpec]??CList<long>.Empty);
                if (index != null && index.rows != null)
                {
                    mm += (Index.Tree, index.rows);
                    mm += (_Index, index.defpos);
                    for (var b = trs.indexes.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        if (b.value().Contains(index.defpos))
                            mm += (Index.Keys, k);
                    }
                }
            }
            return base.Apply(mm, cx, m);
        }
        internal (Index, int, PRow) BestForMatch(Context cx, BTree<long, TypedValue> filter)
        {
            int matches = 0;
            PRow match = null;
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
                for (var c=p.value().First();c!=null;c=c.Next())
            {
                var x = (Index)cx.db.objects[c.key()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != target)
                    continue;
                int sc = 0;
                int nm = 0;
                PRow pr = null;
                var havematch = false;
                int sb = 1;
                for (var b = x.keys.Last(); b != null; b = b.Previous())
                {
                    var j = b.key();
                    var d = b.value();
                    for (var fd = filter.First(); fd != null; fd = fd.Next())
                    {
                        if (cx.obs[fd.key()] is SqlCopy co
                            && co.copyFrom == d)
                        {
                            sc += 9 - j;
                            nm++;
                            pr = new PRow(fd.value(), pr);
                            havematch = true;
                            goto nextj;
                        }
                    }
                    pr = new PRow(TNull.Value, pr);
                nextj:;
                }
                if (!havematch)
                    pr = null;
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
        internal Index BestForOrdSpec(Context cx, CList<long> ordSpec)
        {
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
                for (var c = p.value().First(); c != null; c = c.Next())
                {
                    var x = (Index)cx.db.objects[c.key()];
                    if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                        || x.tabledefpos != defpos)
                        continue;
                    var dm = cx._Dom(x.defpos);
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
            var (k, _) = x;
            if (k == RSTargets) // TableRowSet is shared: don't update rsTargets
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
                    if (cx._Dom(cx.obs[b.key()]).representation.Contains(rp))
                        return true;
            return base.Knows(cx, rp, ambient);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableRowSet)base._Replace(cx, so, sv);
            r += (RSTargets, cx.ReplacedTll(rsTargets));
            cx.Add(r);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (TableRowSet)base._Relocate(cx);
            r += (RSTargets, cx.FixTll(rsTargets));
            r += (Table.Indexes, ((Table)cx.db.objects[target]).IIndexes(r.iSMap));
            cx.Add(r);
            return r;
        }
        public override Cursor First(Context _cx)
        {
            PRow key = null;
            for (var b = keys.First(); b != null; b = b.Next())
            {
                TypedValue v = null;
                if (matches[b.value()] is TypedValue t0 && t0 != TNull.Value)
                    v = t0;
                if (v == null)
                    for (var c = matching[b.value()]?.First(); v == null && c != null; c = c.Next())
                        for (var d = _cx.cursors.First(); v == null && d != null; d = d.Next())
                            if (d.value()[c.key()] is TypedValue tv && !tv.IsNull)
                                v = tv;
                if (v == null)
                    return TableCursor.New(_cx, this, null);
                key = new PRow(v, key);
            }
            return TableCursor.New(_cx,this,PRow.Reverse(key));
        }
        protected override Cursor _First(Context cx)
        {
            throw new NotImplementedException();
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException();
        }
        public override Cursor Last(Context _cx)
        {
            PRow key = null;
            for (var b = keys.First(); b != null; b = b.Next())
            {
                TypedValue v = null;
                if (matches[b.value()] is TypedValue t0 && t0 != TNull.Value)
                    v = t0;
                for (var d = _cx.cursors.First(); v == null && d != null; d = d.Next())
                    if (d.value()[b.value()] is TypedValue tv && !tv.IsNull)
                        v = tv;
                for (var c = matching[b.value()]?.First(); v == null && c != null; c = c.Next())
                    for (var d = _cx.cursors.First(); v == null && d != null; d = d.Next())
                        if (d.value()[c.key()] is TypedValue tv && !tv.IsNull)
                            v = tv;
                if (v == null)
                    return TableCursor.New(_cx, this, null);
                key = new PRow(v, key);
            }
            return TableCursor.New(this, _cx,PRow.Reverse(key));
        }
        /// <summary>
        /// Prepare an Insert on a single table including trigger operation.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ts">The source rowset</param>
        /// <param name="iC">A list of columns matching some of ts.rowType</param>
        /// <returns>The activations for the changes made</returns>
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, CList<long> iC)
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
        internal override RowSet Sort(Context cx, CList<long> os, bool dct)
        {
            if (indexes.Contains(os))
            {
                var ot = (RowSet)cx.Add(this + (Index.Keys, os) + (RowOrder, os));
                if (!dct)
                    return ot;
                if (FindIndex(cx.db,os)!=null)
                    return ot;
                return (RowSet)cx.Add(new DistinctRowSet(cx, ot));
            }
            return base.Sort(cx, os, dct);
        }

        private Index FindIndex(Database db, CList<long> os,
            PIndex.ConstraintType fl = (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
        {
            for (var b = indexes[os].First(); b != null; b = b.Next())
                if (db.objects[b.key()] is Index x && (x.flags&fl)!=0)
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
            internal readonly ABookmark<long, TableRow> _bmk;
            internal readonly TableRow _rec;
            internal readonly MTreeBookmark _mb;
            internal readonly PRow _key;
            protected TableCursor(Context cx, TableRowSet trs, Table tb, int pos, 
                TableRow rec,
                ABookmark<long, TableRow> bmk, MTreeBookmark mb = null,PRow key=null) 
                : base(cx,trs, pos, 
                      new BTree<long,(long,long)>(tb.defpos,(rec.defpos,rec.ppos)),
                      _Row(cx,trs,rec))
            {
                _bmk = bmk; _table = tb; _trs = trs; _mb = mb; _key = key; _rec = rec;
                cx.cursors += (trs.target, this);
            }
            TableCursor(TableCursor cu,Context cx, long p,TypedValue v) :base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _table = cu._table; _trs = cu._trs; _key = cu._key; _mb = cu._mb;
                _rec = cu._rec;
            }
            TableCursor(Context cx,TableCursor cu) :base(cx,cu)
            {
                var t = cu._table.defpos;
                _table = (Table)((DBObject)cx.db.objects[cx.Fix(t)]).Fix(cx);
                _trs = (TableRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = _table.tableRows.PositionAt(cx.Fix(cu._ds[t].Item1));
                _mb = cu._mb;
                _key = cu._key;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableCursor(this, cx, p, v);
            }
            static TRow _Row(Context cx, TableRowSet trs, TableRow rw)
            {
         //       var vs = CTree<long, TypedValue>.Empty;
                var ws = CTree<long, TypedValue>.Empty;
                var dm = cx._Dom(trs);
                for (var b = dm.rowType.First();b!=null;b=b.Next())
                {
                    var p = b.value();
                    var v = rw.vals[trs.iSMap[p]];
                    ws += (p, v);
                }
                return new TRow(dm, ws);
            }
            internal static TableCursor New(Context cx,TableRowSet trs,long defpos)
            {
                var table = cx.db.objects[trs.target] as Table;
                var rec = table.tableRows[defpos];
                return new TableCursor(cx, trs, table, 0, rec, null, null, null);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs, PRow key = null)
            {
                var table = _cx.db.objects[trs.target] as Table;
                if (trs.keys!=CList<long>.Empty)
                {
                    var t = (trs.index >= 0) ? ((Index)_cx.db.objects[trs.index]).rows : null;
                    if (t == null && trs.indexes.Contains(trs.keys))
                        for (var b = trs.indexes[trs.keys]?.First(); b != null; b = b.Next())
                            if (_cx.db.objects[b.key()] is Index ix)
                            {
                                t = ix.rows;
                                break;
                            }
                    if (t!=null)
                    for (var bmk = t.PositionAt(key);bmk != null;bmk=bmk.Next())
                    {
                        var iq = bmk.Value();
                        if (iq == null)
                            continue;
                         var rec = table.tableRows[iq.Value];
#if MANDATORYACCESSCONTROL
                        if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                            && _cx.db._user != table.definer
                            && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                            continue;
#endif
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
#if MANDATORYACCESSCONTROL
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
#endif
                    var rb = new TableCursor(_cx, trs, table, 0, rec, b);
                    if (rb.Matches(_cx))
                    {
                        table._ReadConstraint(_cx, rb);
                        return rb;
                    }
                }
                return null;
            }
            internal static TableCursor New(TableRowSet trs, Context _cx, PRow key = null)
            {
                var table = _cx.db.objects[trs.target] as Table;
                if (trs.keys != CList<long>.Empty && trs.tree != null)
                {
                    var t = trs.tree;
                    for (var bmk = t.PositionAt(key); bmk != null; bmk = bmk.Previous())
                    {
                        var iq = bmk.Value();
                        if (iq == null)
                            continue;
                        var rec = table.tableRows[iq.Value];
#if MANDATORYACCESSCONTROL
                        if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                            && _cx.db._user != table.definer
                            && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                            continue;
#endif
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
#if MANDATORYACCESSCONTROL
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
#endif
                    var rb = new TableCursor(_cx, trs, table, 0, rec, b);
                    if (rb.Matches(_cx))
                    {
                        table._ReadConstraint(_cx, rb);
                        return rb;
                    }
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
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
                        var rec = _table.tableRows[mb.Value().Value];
#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                            _cx.db._user != table.definer && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                            continue;
#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, null, mb,
                            rec.MakeKey(_trs.keys));
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
#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                            _cx.db._user != table.definer && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                            continue;
#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, bmk);
                        if (rb.Matches(_cx))
                        {
                            _table._ReadConstraint(_cx, rb);
                            return rb;
                        }
                    }
            }
            protected override Cursor _Previous(Context _cx)
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
                        var rec = _table.tableRows[mb.Value().Value];
#if MANDATORYACCESSCOLNTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                            _cx.db._user != table.definer && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                            continue;
#endif
                        var rb = new TableCursor(_cx, _trs, _table, _pos + 1, rec, null, mb,
                            rec.MakeKey(_trs.keys));
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
#if MANDATORYACCESSCONTROL
                        if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                            _cx.db._user != table.definer && _cx.db._user != _cx.db.owner
                            && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                            continue;
#endif
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
                return new BList<TableRow>(_rec);
            }
            internal override Cursor _Fix(Context cx)
            {
                return new TableCursor(cx, this);
            }
            public override MTreeBookmark Mb()
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
                  +(_Matches,r.matches)
                  +(ObInfo.Names, r.names)+(_Depth,r.depth+1))
        {
            cx.Add(this);
        }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,RowSet r)
        {
            var dp = cx.GetPrevUid();
            var dm = cx._Dom(r);
            var ks = CList<long>.Empty;
            for (var b =dm.rowType.First(); b != null && b.key()<dm.display; b = b.Next())
            {
                var p = b.value();
                ks += p;
            }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, dm.representation, ks, ks.Length);
            cx.Add(nd);
            return BTree<long, object>.Empty + (_Domain, nd.defpos)
                + (Index.Keys,ks);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new DistinctRowSet(defpos,m);
        }
        public static DistinctRowSet operator+(DistinctRowSet rs,(long,object)x)
        {
            return (DistinctRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new DistinctRowSet(dp,mem);
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var sce = (RowSet)cx.obs[source];
            var ks = keys;
            var mt = new MTree(new TreeInfo(cx, ks, TreeBehaviour.Ignore, TreeBehaviour.Ignore));
            var rs = BTree<int, TRow>.Empty;
            for (var a = sce.First(cx); a != null; a = a.Next(cx))
            {
                var vs = BList<TypedValue>.Empty;
                for (var ti = mt.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref mt, new PRow(vs), 0);
                rs += ((int)rs.Count, a);
            }
            return (RowSet)New(cx,E+(_Built,true)+(Index.Tree,mt)+(Index.Keys,keys));
        }

        protected override Cursor _First(Context cx)
        {
            return DistinctCursor.New(cx,this);
        }
        protected override Cursor _Last(Context cx)
        {
            return DistinctCursor.New(this, cx);
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, CList<long> rt)
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
            readonly MTreeBookmark _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,E,new TRow(cx._Dom(drs), bmk.key()))
            {
                _bmk = bmk;
                _drs = drs;
            }
            DistinctCursor(Context cx,DistinctCursor cu)
                :base(cx,cu)
            {
                _drs = (DistinctRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = _drs.tree.PositionAt(cu._bmk.key()?.Fix(cx));
            }
            internal static DistinctCursor New(Context cx,DistinctRowSet drs)
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
            internal static DistinctCursor New(DistinctRowSet drs,Context cx)
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
            internal override Cursor _Fix(Context cx)
            {
                return new DistinctCursor(cx, this);
            }
            protected override Cursor _Next(Context cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctCursor(cx, _drs,_pos + 1, bmk);
                    if (rb.Matches(cx) && Eval(_drs.where, cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                for (var bmk = _bmk.Previous(); bmk != null; bmk = bmk.Previous())
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
        internal RTree rtree => (RTree)mem[_RTree];
        internal CList<long> order => (CList<long>)mem[Index.Keys] ?? CList<long>.Empty;
        internal override Assertions Requires => Assertions.MatchesTarget;
        internal OrderedRowSet(Context cx, RowSet r, CList<long> os, bool dct)
            : this(cx.GetUid(), cx, r, os, dct)
        { }
        internal OrderedRowSet(long dp, Context cx, RowSet r, CList<long> os, bool dct)
            : base(dp, cx, _Mem(dp, cx, r) + (_Source, r.defpos)
                 + (RSTargets, r.rsTargets) + (RowOrder, os) + (Index.Keys, os)
                 + (Table.LastData, r.lastData) + (ObInfo.Name, r.name)
                 + (ISMap, r.iSMap) + (SIMap,r.sIMap)
                 + (ObInfo.Names,r.names) + (Distinct, dct) + (_Depth, r.depth + 1))
        {
            cx.Add(this);
        }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(long dp, Context cx, RowSet r)
        {
            return BTree<long, object>.Empty + (_Domain, r.domain);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        public static OrderedRowSet operator +(OrderedRowSet rs, (long, object) x)
        {
            return (OrderedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long,object>m=null)
        {
            var ms = (CTree<long,TypedValue>)mm[_Matches]??CTree<long, TypedValue>.Empty;
            for (var b = keys.First(); b != null; b = b.Next())
                if (ms.Contains(b.value()))
                {
                    var sc = (RowSet)cx.obs[source];
                    return sc.Apply(mm, cx); // remove this from the pipeline
                }
            return base.Apply(mm, cx);
        }
        internal override bool CanSkip(Context cx)
        {
            var sc = (RowSet)cx.obs[source];
            var match = true; // if true, all of the ordering columns are constant
            for (var b = rowOrder.First(); match && b != null; b = b.Next())
                if (!matches.Contains(b.value()))
                    match = false;
            return match || (sc is InstanceRowSet && rowOrder.CompareTo(sc.rowOrder) == 0);
        }
        internal override RowSet Sort( Context cx, CList<long> os, bool dct)
        {
            if (SameOrder(cx, os, rowOrder)) // skip if current rowOrder already finer
                return this;
            return (RowSet)cx.Add((RowSet)New(mem + (OrdSpec, os)) + (RowOrder, os));
        }
        internal override DBObject Relocate(long dp)
        {
            return new OrderedRowSet(dp, mem);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (OrderedRowSet)base._Fix(cx);
            var nt = tree?.Fix(cx);
            if (nt!=tree)
                r += (_RTree, nt);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (OrderedRowSet)base._Relocate(cx);
            if (tree != null)
                r += (_RTree, tree.Relocate(cx));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (OrderedRowSet)base._Replace(cx, so, sv);
            if (rtree != null)
                r += (_RTree, rtree.Replace(cx, so, sv));
            cx.done += (defpos, r);
            return r;
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
            var sce = (RowSet)cx.obs[source];
            var dm = new Domain(Sqlx.ROW, cx, keys);
            var tree = new RTree(sce.defpos, cx, dm,
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = sce.First(cx); e != null; e = e.Next(cx))
            {
                var vs = CTree<long, TypedValue>.Empty;
                cx.cursors += (sce.defpos, e);
                for (var b = rowOrder.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()];
                    vs += (s.defpos, s.Eval(cx));
                }
                var rw = new TRow(dm, vs);
                RTree.Add(ref tree, rw, SourceCursors(cx));
            }
            return (RowSet)New(cx, E + (_Built, true) + (Index.Tree, tree.mt) + (_RTree, tree));
        }
        protected override Cursor _First(Context cx)
        {
            if (rtree == null || rtree.rows.Count == 0L)
                return null;
            return OrderedCursor.New(cx, this, RTreeBookmark.New(cx, rtree));
        }
        protected override Cursor _Last(Context cx)
        {
            if (rtree == null || rtree.rows.Count == 0L)
                return null;
            return OrderedCursor.New(cx, this, RTreeBookmark.New(rtree, cx));
        }
        // shareable as of 26 April 2021
        internal class OrderedCursor : Cursor
        {
            internal readonly OrderedRowSet _ors;
            internal RTreeBookmark _rb;
            internal OrderedCursor(Context cx,OrderedRowSet ors,RTreeBookmark rb)
                :base(cx,ors,rb._pos,rb._ds,rb)
            {
                _ors = ors; _rb = rb;
                cx.cursors += _ors.rtree.rows[rb._pos];
            }
            OrderedCursor(Context cx,OrderedCursor cu):base(cx,cu)
            {
                _ors = (OrderedRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _rb = (RTreeBookmark)_rb?.Fix(cx);
            }
            public override MTreeBookmark Mb()
            {
                return _rb?.Mb();
            }
            internal static OrderedCursor New(Context cx,OrderedRowSet ors,
                RTreeBookmark rb)
            {
                if (rb == null)
                    return null;
                return new OrderedCursor(cx, ors, rb);
            }
            protected override Cursor _Next(Context _cx)
            {
                return New(_cx, _ors, (RTreeBookmark)_rb.Next(_cx));
            }
            protected override Cursor _Previous(Context _cx)
            {
                return New(_cx, _ors, (RTreeBookmark)_rb.Previous(_cx));
            }
            internal override BList<TableRow> Rec()
            {
                return _rb.Rec();
            }
            internal override Cursor _Fix(Context cx)
            {
                return new OrderedCursor(cx, this);
            }
        }
    }
    // shareable as of 26 April 2021
    internal class EmptyRowSet : RowSet
    {
        internal EmptyRowSet(long dp, Context cx,long dm,CList<long>us=null,
            CTree<long,Domain>re=null) 
            : base(dp, _Mem(cx,dm,us,re)) 
        {
            cx.Add(this);
        }
        protected EmptyRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,long dm,CList<long> us,
            CTree<long,Domain> re)
        {
            var dt = cx._Dom(dm);
            if (us != null && us.CompareTo(dt.rowType) != 0)
                dt = (Domain)dt.Relocate(cx.GetUid()) + (Domain.RowType, us)
                    +(Domain.Representation,dt.representation+re);
            return new BTree<long, object>(_Domain, dt.defpos);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EmptyRowSet(defpos,m);
        }
        public static EmptyRowSet operator+(EmptyRowSet rs,(long,object)x)
        {
            return (EmptyRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new EmptyRowSet(dp,mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return null;
        }
        protected override Cursor _Last(Context _cx)
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
            base(dp, new BTree<long, object>(SqlLiteral._Val, sv.Eval(cx)))
        { }
        protected ArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        protected override Cursor _First(Context cx)
        {
            var a = (TArray)mem[SqlLiteral._Val];
            return ArrayCursor.New(cx, this, a, 0);
        }
        protected override Cursor _Last(Context cx)
        {
            var a = (TArray)mem[SqlLiteral._Val];
            return ArrayCursor.New(cx,this,a,a.Length-1);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ArrayRowSet(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ArrayRowSet(dp, mem);
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
            internal static ArrayCursor New(Context cx, ArrayRowSet ars, TArray ar, int ix)
            {
                if (ix<0 || ar.Length <= ix)
                    return null;
                return new ArrayCursor(cx, ars, ar, ix);
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx,_ars,_ar,_ix+1);
            }

            protected override Cursor _Previous(Context cx)
            {
                return New(cx, _ars, _ar, _ix - 1);
            }

            internal override BList<TableRow> Rec()
            {
                throw new NotImplementedException();
            }

            internal override Cursor _Fix(Context cx)
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
            base(dp, new BTree<long, object>(SqlLiteral._Val, sv.Eval(cx)))
        { }
        protected MultisetRowSet(long dp, BTree<long, object> m) : base(dp, m) { }

        protected override Cursor _First(Context cx)
        {
            var a = (TMultiset)mem[SqlLiteral._Val];
            return MultisetCursor.New(cx, this, a, a.First());
        }
        protected override Cursor _Last(Context cx)
        {
            var a = (TMultiset)mem[SqlLiteral._Val];
            return MultisetCursor.New(cx, this, a, a.Last());
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MultisetRowSet(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new MultisetRowSet(dp,mem);
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
            internal static MultisetCursor New(Context cx,MultisetRowSet mrs,TMultiset ms,
                TMultiset.MultisetBookmark mb)
            {
                if (mb == null)
                    return null;
                return new MultisetCursor(cx, mrs, ms, mb);
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Next());
            }

            protected override Cursor _Previous(Context cx)
            {
                return New(cx, _mrs, _ms, _mb.Previous());
            }

            internal override BList<TableRow> Rec()
            {
                throw new NotImplementedException();
            }

            internal override Cursor _Fix(Context cx)
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
            SqlRows = -413; // CList<long>  SqlRow
        internal CList<long> sqlRows =>
            (CList<long>)mem[SqlRows]??CList<long>.Empty;
        internal SqlRowSet(long dp,Context cx,Domain xp, CList<long> rs) 
            : base(dp, cx, _Mem(dp,cx,xp,rs)+(SqlRows,rs)
                  +(_Needed,CTree<long,bool>.Empty)
                  +(_Depth,cx.Depth(rs,xp)))
        {
            cx.Add(this);
        }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx, Domain dm, CList<long> rs)
        {
            var ns = CTree<string, long>.Empty;
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
            return (SqlRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRowSet)base._Replace(cx, so, sv);
            r += (SqlRows, cx.ReplacedLl(sqlRows));
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlRowSet)base._Fix(cx);
            var nw = cx.FixLl(sqlRows);
            if (nw!=sqlRows)
            r += (SqlRows, nw);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlRowSet)base._Relocate(cx);
            r += (SqlRows, cx.FixLl(sqlRows));
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return SqlCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return SqlCursor.New(this, _cx);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" SqlRows [");
            var cm = "";
            for (var b = sqlRows.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
            }
            sb.Append("]");
        }
        // shareable as of 26 April 2021
        internal class SqlCursor : Cursor
        {
            readonly SqlRowSet _srs;
            readonly ABookmark<int, long> _bmk;
            SqlCursor(Context cx,SqlRowSet rs,int pos,ABookmark<int,long> bmk)
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
                var dt = cx._Dom(rs);
                var dm = new Domain(cx.GetUid(), cx, Sqlx.ROW, dt.representation, dt.rowType, dt.display);
                var vs = CTree<long, TypedValue>.Empty;
                var rv = (SqlRow)cx.obs[rs.sqlRows[p]];
                var rd = cx._Dom(rv);
                var sd = cx._Dom(rs);
                var n = rd.rowType.Length;
                if (n < dm.rowType.Length)
                    throw new DBException("22109");
                int j = 0;
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                {
                    var d = cx._Dom(cx.obs[sd.rowType[j]]);
                    vs += (b.value(), d.Coerce(cx,(cx.obs[rd.rowType[j++]]?.Eval(cx)??TNull.Value)));
                }
                return new TRow(dm,vs);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.sqlRows.First();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal static SqlCursor New(SqlRowSet rs, Context _cx)
            {
                for (var b = rs.sqlRows.Last(); b != null; b = b.Previous())
                {
                    var rb = new SqlCursor(_cx, rs, 0, b);
                    if (rb.Matches(_cx) && Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                for (var b=_bmk.Next();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,_srs, _pos+1,b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context _cx)
            {
                for (var b = _bmk.Previous(); b != null; b = b.Previous())
                {
                    var rb = new SqlCursor(_cx, _srs, _pos + 1, b);
                    if (rb.Matches(_cx) && Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
    (BList<(long, TRow)>)mem[ExplRows];
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,Domain dt,BList<(long,TRow)>r)
            : base(dp, BTree<long,object>.Empty+(ExplRows,r)+(_Domain,dt.defpos))
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
            return (ExplicitRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ExplicitRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (ExplicitRowSet)base._Replace(cx, so, sv);
            var s = BList<(long,TRow)>.Empty;
            for (var b=explRows.First();b!=null;b=b.Next())
            {
                var (p, q) = b.value();
                s += (p, (TRow)q.Replace(cx,so,sv));
            }
            r += (ExplRows, s);
            cx.done += (defpos, r);
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return ExplicitCursor.New(_cx,this,0);
        }
        protected override Cursor _Last(Context _cx)
        {
            return ExplicitCursor.New(_cx, this, (int)explRows.Count-1);
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
                sb.Append("[");
                for (var b=explRows.First();b!=null;b=b.Next())
                {
                    var (p, rw) = b.value();
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(p));
                    sb.Append(": ");sb.Append(rw);
                }
                sb.Append("]");
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
            ExplicitCursor(Context cx,ExplicitCursor cu):base(cx,cu)
            {
                _ers = (ExplicitRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _prb = _ers.explRows.PositionAt(cu._pos);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ExplicitCursor(this, cx, p, v);
            }
            internal static ExplicitCursor New(Context _cx,ExplicitRowSet ers,int i)
            {
                for (var b=ers.explRows.First();b!=null;b=b.Next())
                {
                    var rb = new ExplicitCursor(_cx,ers, b, 0);
                    if (rb.Matches(_cx) && Eval(ers.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                for (var prb = _prb.Next(); prb!=null; prb=prb.Next())
                {
                    var rb = new ExplicitCursor(_cx,_ers, prb, _pos+1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context _cx)
            {
                for (var prb = _prb.Previous(); prb != null; prb = prb.Previous())
                {
                    var rb = new ExplicitCursor(_cx, _ers, prb, _pos + 1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override Cursor _Fix(Context cx)
            {
                return new ExplicitCursor(cx, this);
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
        internal SqlCall call => (SqlCall)mem[SqlCall.Call];
        internal ProcRowSet(long dp, SqlCall ca, Context cx) 
            :base(cx.GetUid(),cx,_Mem(cx.GetPrevUid(),dp,cx,ca) +(_Needed,CTree<long,bool>.Empty)
            +(SqlCall.Call,ca)+(_Depth,ca.depth+1))
        {
            cx.Add(this);
        }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,long fp,Context cx,SqlCall ca)
        {
            var m = BTree<long, object>.Empty + (_Domain, ca.domain);
            var cs = (CallStatement)cx._Ob(ca.call);
            var pi = cx._Ob(cs.procdefpos).infos[cx.role.defpos];
            if (pi.names != CTree<string, long>.Empty)
                m += (ObInfo.Names, pi.names);
            return m;
        }
        public static ProcRowSet operator+(ProcRowSet p,(long,object)x)
        {
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
        protected override Cursor _First(Context cx)
        {
            var v = cx.values[defpos];
            return (v==TNull.Value)?null:ProcRowSetCursor.New(cx, this, (TArray)v);
        }
        protected override Cursor _Last(Context cx)
        {
            var v = cx.values[defpos];
            return (v == TNull.Value) ? null : ProcRowSetCursor.New(this, cx, (TArray)v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ProcRowSet(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ProcRowSet(dp, mem+(SqlCall.Call,call.Relocate(dp)));
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (ProcRowSet)base._Replace(cx, so, sv);
            r += (SqlCall.Call, cx.done[call.defpos]??call);
            cx.done += (defpos, r);
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
            ProcRowSetCursor(Context cx,ProcRowSet prs,int pos,
                ABookmark<int,TypedValue>bmk, TRow rw) 
                : base(cx,prs,bmk.key(),E,new TRow(rw,cx._Dom(prs))) 
            { 
                _prs = prs; _bmk = bmk;
                cx.values += values;
            }
            internal static ProcRowSetCursor New(Context cx,ProcRowSet prs,TArray ta)
            {
                for (var b = ta.list.First();b!=null;b=b.Next())
                {
                    var r = new ProcRowSetCursor(cx, prs, 0, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            internal static ProcRowSetCursor New(ProcRowSet prs, Context cx, TArray ta)
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

            protected override Cursor _Next(Context cx)
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var r = new ProcRowSetCursor(cx, _prs, _pos+1, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
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
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
            TargetTrans = -418, // CTree<long,long>   TableColumn,SqlValue
            TransTarget = -419; // CTree<long,long>   SqlValue,TableColumn
        internal CTree<long, TypedValue> defaults =>
            (CTree<long, TypedValue>)mem[Defaults] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, long> targetTrans =>
            (CTree<long, long>)mem[TargetTrans];
        internal CTree<long, long> transTarget =>
            (CTree<long, long>)mem[TransTarget];
        internal CTree<long, bool> foreignKeys =>
            (CTree<long, bool>)mem[ForeignKeys] ?? CTree<long, bool>.Empty;
        internal CList<long> insertCols => (CList<long>)mem[Table.TableCols];
        internal long indexdefpos => (long)(mem[IxDefPos] ?? -1L);
        internal Adapters _eqs => (Adapters)mem[_Adapters];
        internal TransitionRowSet(TargetActivation cx, TableRowSet ts, RowSet data)
            : base(cx.GetUid(), cx,_Mem(cx.GetPrevUid(), cx, ts, data)
                  +(_Where,ts.where)+(_Data,data.defpos)+(_Depth,cx.Depth(ts,data))
                  +(Matching,ts.matching))
        {
            cx.Add(this);
        }
        static BTree<long, object> _Mem(long defpos, TargetActivation cx, 
            TableRowSet ts,RowSet data)
        {
            var m = BTree<long, object>.Empty;
            var tr = cx.db;
            var ta = ts.target;
            m += (_Domain, ts.domain);
            m += (_Data, data.defpos);
            m += (Target, ta);
            m += (RSTargets, new CTree<long,long>(ts.target,ts.defpos));
            var t = tr.objects[ta] as Table;
            m += (IxDefPos, t?.FindPrimaryIndex(cx)?.defpos ?? -1L);
            // check now about conflict with generated columns
            if (t!=null && t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", t.infos[cx.role.defpos].name);
            var dfs = BTree<long, TypedValue>.Empty;
            if (cx._tty != PTrigger.TrigType.Delete)
            {
                for (var b = cx._Dom(ta).rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (tr.objects[p] is TableColumn tc) // i.e. not remote
                    {
                        var tv = tc.defaultValue ?? 
                            cx._Dom(tc).defaultValue;
                        if (!tv.IsNull)
                        {
                            dfs += (tc.defpos, tv);
                            cx.values += (tc.defpos, tv);
                        }
                    }
                }
            }
            var fk = CTree<long, bool>.Empty;
            for (var b = t?.indexes.First();b!=null;b=b.Next())
                for (var c=b.value().First();c!=null;c=c.Next())
            {
                var p = c.key();
                var ix = (Index)cx.db.objects[p];
                if (ix.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                    fk += (p, true);
            }
            m += (TargetTrans, ts.sIMap);
            m += (TransTarget, ts.iSMap);
            m += (ForeignKeys, fk);
            m += (Defaults, dfs);
            m += (_Needed, CTree<long, bool>.Empty);
            return m;
        }
        protected TransitionRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionRowSet(defpos, m);
        }
        public static TransitionRowSet operator+ (TransitionRowSet trs,(long,object)x)
        {
            return (TransitionRowSet)trs.New(trs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionRowSet(dp, mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionCursor.New((TableActivation)_cx, this);
        }
        protected override Cursor _Last(Context cx)
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
        internal Cursor At(TableActivation cx,long dp,CTree<long,TypedValue>u)
        {
            cx.pending += (dp, u);
            for (var b = (TransitionCursor)First(cx); b != null;
                b = (TransitionCursor)b.Next(cx))
                if (b._ds[cx._trs.target].Item1 == dp)
                    return b;
            return null;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RowSet)base._Replace(cx, so, sv);
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
            cx.done += (defpos, r);
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
                for (var b=transTarget.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                    sb.Append("="); sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
        }
        // shareable as of 26 April 2021
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TargetCursor _tgc;
            internal TransitionCursor(TableActivation ta, TransitionRowSet trs, Cursor fbm, int pos,
                CList<long> iC) : base(ta.next, trs, pos, fbm._ds, new TRow(ta._Dom(trs), iC, fbm))
            {
                _trs = trs;
                _fbm = fbm;
                var cx = ta.next;
                for (var b = trs.iSMap.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    cx.values += (k, cx.values[b.value()]);
                }
                _tgc = TargetCursor.New(ta, this, true); // retrieve it from ta
            }
            TransitionCursor(TransitionCursor cu, TableActivation ta, long p, TypedValue v)
                : base(cu, ta.next,cu._trs.targetTrans[p],v)
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _tgc = TargetCursor.New(ta, this,false); // retrieve it from ta
                ta.values += (p, v);
                ta.next.values += (ta._trs.target, this);
            }
            internal TransitionCursor(TableActivation ta, TransitionRowSet trs,
                CTree<long,TypedValue> vs,long dpos, int pos)
                :base(ta.next,trs,pos,E,_Row(ta,trs,vs))
            {
                _trs = trs;
                _fbm = null;
                _tgc = TargetCursor.New(ta,this,true);
                var cx = ta.next;
                cx.values += (values, false);
                cx.values += (ta._trs.target, this);
            }
            static TRow _Row(TableActivation ta,TransitionRowSet trs,CTree<long,TypedValue> vs)
            {
                var nv = CTree<long, TypedValue>.Empty;
                var dm = (Domain)ta.obs[ta.obs[trs.data].domain];
                var db = dm.rowType.First();
                var dt = ta._Dom(trs);
                for (var b = dt.rowType.First(); b != null && db != null; b = b.Next(), db = db.Next())
                    nv += (b.value(), vs[db.value()]);
                return new TRow(dt, nv);
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
            internal static TransitionCursor New(TableActivation ta, TransitionRowSet trs)
            {
                var sce = (RowSet)ta.obs[trs.data];  // annoying we can't cache this
                for (var fbm = sce?.First(ta); fbm != null;
                    fbm = fbm.Next(ta))
                    if (fbm.Matches(ta) && Eval(trs.where, ta))
                        return new TransitionCursor(ta, trs, fbm, 0, ta.insertCols);
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                if (cx.obs[_trs.where.First()?.key() ?? -1L] is SqlValue sv && 
                    cx._Dom(sv).kind == Sqlx.CURRENT)
                    return null;
                var ta = (TableActivation)cx;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = new TransitionCursor(ta, _trs, fbm, _pos+1,ta.insertCols);
                    for (var b = _trs.where.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()].Eval(cx) != TBool.True)
                            goto skip;
                    return ret;
                skip:;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                return null; // never
            }
            internal override Cursor _Fix(Context cx)
            {
                return this;
            }
            internal override BList<TableRow> Rec()
            {
                return _fbm.Rec();
            }

        }
        // shareable as of 26 April 2021
        internal class TargetCursor : Cursor
        {
            internal readonly Domain _td;
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fb;
            internal readonly TableRow _rec;
            TargetCursor(TableActivation ta, Cursor trc, Cursor fb, Domain td, CTree<long,TypedValue> vals)
                : base(ta, ta._trs.defpos, td, trc._pos, 
                      (fb!=null)?(fb._ds+trc._ds):trc._ds, 
                      new TRow(td, vals))
            { 
                _trs = ta._trs; _td = td; _fb = fb;
                var t = (Table)ta.db.objects[ta._tgt];
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
            internal static TargetCursor New(TableActivation cx, TransitionCursor trc,
                bool check)
            {
                if (trc == null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var trs = trc._trs;
                var t = (Table)cx.db.objects[cx._tgt];
                var dm = cx._Dom(t);
                var fb = (cx._tty!=PTrigger.TrigType.Insert)?cx.next.cursors[trs.rsTargets[cx._tgt]]:null;
                var rt = trc._trs.insertCols??dm.rowType;
                for (var b = rt.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    var tp = trs.targetTrans[p];
                    if (trc[tp] is TypedValue v)
                        vs += (p, v);
                    else if (fb?[tp] is TypedValue fv) // for matching cases
                        vs += (p, fv);
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
                                    if (vs[tc.defpos] is TypedValue vt && !vt.IsNull)
                                        throw new DBException("0U000", tc.infos[cx.role.defpos].name);
                                    cx.values += tc.Frame(vs);
                                    cx.obs += tc.framing.obs;
                                    var e = cx.obs[tc.generated.exp];
                                    var v = e.Eval(cx);
                                    trc += (cx, tc.defpos, v);
                                    vs += (tc.defpos, v);
                                    break;
                            }
                        var cv = vs[tc.defpos];
                        if ((!tc.defaultValue.IsNull) && (cv == null || cv.IsNull))
                            vs += (tc.defpos, tc.defaultValue);
                        cv = vs[tc.defpos];
                        if (tc.notNull && (cv == null || cv.IsNull))
                            throw new DBException("22206", tc.infos[cx.role.defpos].name);
                        for (var cb = tc.constraints?.First(); cb != null; cb = cb.Next())
                        {
                            var cp = cb.key();
                            var ck = (Check)cx.db.objects[cp]; // not in framing!
                            cx.obs += ck.framing.obs;
                            cx.values += ck.Frame(vs);
                            var se = (SqlValue)cx.obs[ck.search];
                            if (se.Eval(cx) != TBool.True)
                                throw new DBException("22212", tc.infos[cx.role.defpos].name);
                        }
                    }
                var r = new TargetCursor(cx, trc, fb, dm, vs);
                for (var b = trc._trs.foreignKeys.First(); b != null; b = b.Next())
                {
                    var ix = (Index)cx.db.objects[b.key()];
                    var rx = (Index)cx.db.objects[ix.refindexdefpos];
         /*           if (rx is VirtualIndex)
                        continue; */
                    var k = ix.MakeKey(r._rec.vals);
                    if (k._head == null)
                        continue;
                    var ap = ix.adapter;
                    if (ap>=0)
                    {
                        var ad = (Procedure)cx.db.objects[ap];
                        cx = (TableActivation)ad.Exec(cx, ix.keys);
                        k = new PRow((TRow)cx.val);
                    }
                    if (!rx.rows.Contains(k))
                    {
                        for (var bp = cx.pending.First(); bp != null; bp = bp.Next())
                            if (bp.value() is CTree<long, TypedValue> vk) {
                                if (rx.MakeKey(vk).CompareTo(k) == 0)
                                       goto skip;
                            }
                        throw new DBException("23000", "missing foreign key "
                            + cx._Ob(ix.tabledefpos).infos[cx.role.defpos].name + k.ToString());
                    skip:;
                    }
                }
                return r;
            }
            /// <summary>
            /// Implement the autokey feature: if a key column is an integer type,
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
                var ta = cx as TableActivation;
                if (ta.index!=null)
                {
                    var k = CList<TypedValue>.Empty;
                    for (var b = ta.index.keys.First(); b != null; b = b.Next())
                    {
                        var tc = (TableColumn)(cx.obs[b.value()]??cx.db.objects[b.value()]);
                        var v = vs[tc.defpos];
                        if (v == null || v.IsNull)
                        {
                            if (cx._Dom(tc).kind != Sqlx.INTEGER)
                                throw new DBException("22004");
                            v = ta.index.rows.NextKey(k, 0, b.key());
                            if (v.IsNull)
                                v = new TInt(0);
                            vs += (tc.defpos, v);
                            cx.values += (trc._trs.targetTrans[tc.defpos], v);
                            var pk = ta.index.MakeKey(vs);
                            ta.index += (Index.Tree, ta.index.rows + (pk, -1L));
                        }
                        k += v;
                    }
                }
                return vs;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                TableActivation ta;
                while ((ta=cx as TableActivation)==null)
                    cx = cx.next;
                var trc = (TransitionCursor)cx.next.cursors[_trs.defpos];
                var tp = _trs.targetTrans[p];
                if (tp != 0)
                    trc += (ta, tp, v);
                cx.next.cursors += (_trs.defpos, trc);
                return ta.cursors[ta.table.defpos];
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
                return new BList<TableRow>(_rec);
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        // shareable as of 26 April 2021
        internal class TriggerCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly TargetCursor _tgc;
            internal TriggerCursor(TriggerActivation ta, Cursor tgc,CTree<long,TypedValue> vals=null)
                : base(ta, ta._trs.defpos, ta._Dom(ta._trig), tgc._pos, tgc._ds,
                      new TRow(ta._Dom(ta._trig), ta.trigTarget, vals??tgc.values))
            {
                _trs = ta._trs;
                _tgc = (TargetCursor)tgc;
                ta.values += (ta._trs.target, this);
                ta.values += values;
            }
            public static TriggerCursor operator +(TriggerCursor trc, (Context, long, TypedValue) x)
            {
                var (cx, p, v) = x;
                return (TriggerCursor)trc.New(cx, p, v);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                while (!(cx is TriggerActivation))
                    cx = cx.next;
                var ta = (TriggerActivation)cx;
                var tp = ta.trigTarget[p];
                return new TriggerCursor(ta, _tgc+(ta.next,tp,v));
            }
            protected override Cursor _Next(Context cx)
            {
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
            internal override Cursor _Fix(Context cx)
            {
                return this;
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
            _Map = -433,         // CTree<long,long>   TableColumn
            RMap = -434,        // CTree<long,long>   TableColumn
            Trs = -431;         // TransitionRowSet
        internal TransitionRowSet _trs => (TransitionRowSet)mem[Trs];
        internal BTree<long,TableRow> obs => 
            (BTree<long,TableRow>)mem[Data]??BTree<long,TableRow>.Empty;
        internal CTree<long, long> map =>
            (CTree<long, long>)mem[_Map];
        internal CTree<long,long> rmap =>
            (CTree<long, long>)mem[RMap];
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
            : base(dp, cx, _Mem(cx, dp, trs, dm, old)
                  + (RSTargets, trs.rsTargets) + (Target,trs.target)
                  +(_Depth,cx.Depth(trs,dm)))
        {
            cx.Add(this);
        }
        protected TransitionTableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,long dp,TransitionRowSet trs,Domain dm,bool old)
        {
            var dat = BTree<long, TableRow>.Empty;
            if ((!old) && cx.newTables.Contains(trs.defpos))
                dat = cx.newTables[trs.defpos];
            else
                for (var b = trs.First(cx); b != null; b = b.Next(cx))
                    for (var c=b.Rec().First();c!=null;c=c.Next())
                        dat += (c.value().tabledefpos, c.value());
            var ma = CTree<long, long>.Empty;
            var rm = CTree<long, long>.Empty;
            var rb = cx._Dom(trs).rowType.First();
            for (var b=dm.rowType.First();b!=null&&rb!=null;b=b.Next(),rb=rb.Next())
            {
                var p = b.value();
                var q = rb.value();
                var f = trs.transTarget[q];
                ma += (f, p);
                rm += (p, f);
            }
            return BTree<long,object>.Empty + (TransitionTable.Old,old)
                + (Trs,trs) + (Data,dat) + (_Map,ma)
                +(RMap,rm) + (_Domain, dm.defpos);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTableRowSet(defpos,m);
        }
        public static TransitionTableRowSet operator+(TransitionTableRowSet rs,(long,object)x)
        {
            return (TransitionTableRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionTableRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RowSet)base._Replace(cx, so, sv);
            var ts = _trs.Replace(cx, so, sv);
            if (ts != _trs)
                r += (Trs, ts);
            var mp = cx.ReplacedTll(map);
            if (mp != map)
                r += (_Map, mp);
            var rm = cx.ReplacedTll(rmap);
            if (rm != rmap)
                r += (RMap, rm);
            cx.done += (defpos, r);
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context cx)
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
                TableRowSet.TableCursor tc,int pos)
                :base(cx,tt,pos,
                     new BTree<long,(long,long)>(tc._bmk.key(),(tc._bmk.value().defpos,tc._bmk.value().ppos)),
                     new TRow(cx._Dom(tt), tt.rmap,tc._bmk.value().vals))
            {
                _tc = tc; _tt = tt;
            }
            TransitionTableCursor(TransitionTableCursor cu, Context cx, long p, TypedValue v)
                : base(cu, cx, p, v) { }
            internal static TransitionTableCursor New(Context cx,TransitionTableRowSet tt)
            {
                var rs = (RowSet)cx.obs[tt.rsTargets.First().value()];
                var tc = (TableRowSet.TableCursor)rs.First(cx);
                return new TransitionTableCursor(cx,tt,tc,tc._pos);
            }
            protected override Cursor _Next(Context _cx)
            {
                var tc = (TableRowSet.TableCursor)_tc.Next(_cx);
                return (tc != null) ? new TransitionTableCursor(_cx, _tt, tc, _pos + 1):null;
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException(); // never
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                if (_tt.old)
                    throw new DBException("23103","OLD TABLE");
                return new TransitionTableCursor(this,cx,p,v);
            }

            internal override BList<TableRow> Rec()
            {
                return new BList<TableRow>(_tc._bmk.value());
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
            }
        }
    }
    /*
    /// <summary>
    /// RoutineCallRowSet is a table-valued routine call
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class RoutineCallRowSet : InstanceRowSet
    {
        internal const long
            Actuals = -435, // CList<long>  SqlValue
            Proc = -436,    // Procedure
            Result = -437;  // RowSet
        internal Procedure proc => (Procedure)mem[Proc];
        internal CList<long> actuals => (CList<long>)mem[Actuals];
        internal RowSet result => (RowSet)mem[Result];
        protected RoutineCallRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RoutineCallRowSet(defpos,m);
        }
        public static RoutineCallRowSet operator+(RoutineCallRowSet rs,(long,object) x)
        {
            return (RoutineCallRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RoutineCallRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RoutineCallRowSet)base._Replace(cx, so, sv);
            r += (Actuals, cx.Replaced(actuals));
            r += (Proc, proc.Replace(cx,so,sv));
            r += (Result, cx.obs[result.defpos].Replace(cx,so,sv)); 
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var _cx = new Context(cx);
            cx = proc.Exec(_cx, actuals);
            return (RowSet)New(cx,E+(Result,cx.obs[cx.result])+(_Built,true));
        }
        protected override Cursor _First(Context cx)
        {
            return result.First(cx);
        }
        protected override Cursor _Last(Context cx)
        {
            return result.Last(cx);
        }
    } */
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
            : base(_cx.GetUid(),_Mem(_cx,s)+(Offset,o)+(Size,c)+(_Source,s.defpos)
                  +(RSTargets,s.rsTargets) + (_Domain,s.domain)
                  +(Table.LastData,s.lastData))
        {
            _cx.obs += (defpos, this);
        }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,RowSet r)
        {
            var m = BTree<long, object>.Empty + (ISMap, r.iSMap) + (SIMap, r.sIMap);
            if (r.names != CTree<string, long>.Empty)
                m += (ObInfo.Names, r.names);
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RowSetSection(defpos,m);
        }
        public static RowSetSection operator+(RowSetSection rs,(long,object)x)
        {
            return (RowSetSection)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RowSetSection(dp,mem);
        }
        protected override Cursor _First(Context cx)
        {
            var b = ((RowSet)cx.obs[source]).First(cx);
            for (int i = 0; b!=null && i < offset; i++)
                b = b.Next(cx);
            return RowSetSectionCursor.New(cx,this,b);
        }
        protected override Cursor _Last(Context cx)
        {
            var b = ((RowSet)cx.obs[source]).Last(cx);
            for (int i = 0; b != null && i < offset; i++)
                b = b.Previous(cx);
            return RowSetSectionCursor.New(cx, this, b);
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
            internal static RowSetSectionCursor New(Context cx,RowSetSection rss,Cursor rb)
            {
                if (rb == null)
                    return null;
                return new RowSetSectionCursor(cx, rss, rb);
            }
            protected override Cursor _Next(Context cx)
            {
                if (_pos+1 >= _rss.size)
                    return null;
                if (_rb.Next(cx) is Cursor rb)
                    return new RowSetSectionCursor(cx, _rss, rb);
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                if (_pos + 1 >= _rss.size)
                    return null;
                if (_rb.Previous(cx) is Cursor rb)
                    return new RowSetSectionCursor(cx, _rss, rb);
                return null;
            }
            internal override BList<TableRow> Rec()
            {
                return _rb.Rec();
            }
            internal override Cursor _Fix(Context cx)
            {
                return this;
            }
        }
    }
    // shareable as of 26 April 2021
    internal class DocArrayRowSet : RowSet
    {
        internal const long
            Docs = -440; // BList<SqlValue>
        internal BList<SqlValue> vals => (BList<SqlValue>)mem[Docs];
        internal DocArrayRowSet(Context cx, SqlRowArray d)
            : base(cx.GetUid(), _Mem(cx, d)
                  +(_Domain,Domain.TableType.defpos))
        {
            cx.Add(this);
        }
        static BTree<long,object> _Mem(Context cx,SqlRowArray d)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += (SqlValue)cx.obs[d.rows[i]];
            return new BTree<long,object>(Docs,vs);
        }
        protected DocArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DocArrayRowSet(defpos,m);
        }
        public static DocArrayRowSet operator+(DocArrayRowSet rs,(long,object)x)
        {
            return (DocArrayRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new DocArrayRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (DocArrayRowSet)base._Replace(cx, so, sv);
            var ds = BList<SqlValue>.Empty;
            for (var b = vals.First(); b != null; b = b.Next())
                ds += (SqlValue)cx.done[b.value().defpos] ?? b.value();
            r += (Docs, ds);
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return DocArrayBookmark.New(_cx,this);
        }
        protected override Cursor _Last(Context cx)
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
            DocArrayBookmark(Context cx, DocArrayBookmark cu): base(cx, cu) 
            {
                _drs = (DocArrayRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = _drs.vals.PositionAt(cu._pos);
            }
            static TRow _Row(Context cx,SqlValue sv)
            {
                return new TRow(sv,new CTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
            }
            internal static DocArrayBookmark New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            internal static DocArrayBookmark New(DocArrayRowSet drs, Context cx)
            {
                var bmk = drs.vals.Last();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(cx, drs, bmk);
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = ABookmark<int, SqlValue>.Next(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,_drs, bmk);
            }
            protected override Cursor _Previous(Context _cx)
            {
                var bmk = ABookmark<int, SqlValue>.Previous(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx, _drs, bmk);
            }
            internal override Cursor _Fix(Context cx)
            {
                return new DocArrayBookmark(cx,this);
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
        internal RTree rtree => (RTree)mem[OrderedRowSet._RTree];
        internal TMultiset values => (TMultiset)mem[Multi];
        internal SqlFunction wf=> (SqlFunction)mem[Window];
        internal WindowRowSet(Context cx,RowSet sc, SqlFunction f)
            :base(cx.GetUid(),cx,_Mem(cx.GetPrevUid(),cx,sc)+(_Source,sc.defpos)
                 +(Window,f)+(Table.LastData,sc.lastData)
                 +(ISMap,sc.iSMap)+(SIMap,sc.sIMap)+(ObInfo.Names,sc.names)
                 +(_Depth,cx.Depth(sc,f)))
        {
            cx.Add(this);
        }
        protected WindowRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,RowSet sc)
        {
            return BTree<long, object>.Empty 
                +(_Domain,sc.domain);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowRowSet(defpos, m);
        }
        protected override Cursor _First(Context _cx)
        {
            return WindowCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return WindowCursor.New(this, _cx);
        }
        public static WindowRowSet operator+(WindowRowSet rs,(long,object)x)
        {
            return (WindowRowSet)rs.New(rs.mem+x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WindowRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WindowRowSet)base._Replace(cx, so, sv);
            r += (Window, cx.done[wf.defpos] ?? wf);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            return ((WindowRowSet)base._Fix(cx))+(Multi,values.Fix(cx))
                +(OrderedRowSet._RTree,tree.Fix(cx))+(Window,wf.Fix(cx));
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (WindowRowSet)base._Relocate(cx);
            r += (Window, wf.Relocate(cx));
            return r;
        }
        internal override bool Built(Context cx)
        {
            return mem.Contains(_Built);
        }
        internal override RowSet Build(Context cx)
        {
            var w = (WindowSpecification)cx.obs[wf.window];
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var od = cx._Dom(w.order)??cx._Dom(w.partition);
            if (od == null)
                return this;
            var tree = new RTree(source,cx,od,TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset((Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.MULTISET, cx._Dom(wf))));
            for (var rw = ((RowSet)cx.obs[source]).First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(od, rw.values), cx.cursors);
                values.Add(v);
            }
            return (RowSet)New(cx,E+(Multi,values)+(_Built,true)+(Index.Tree,tree.mt));
        }
        // shareable as of 26 April 2021
        internal class WindowCursor : Cursor
        {
            WindowCursor(Context cx,long rp,Domain dm,int pos,TRow rw) 
                : base(cx,rp,dm,pos,E,rw)
            { }
            internal static Cursor New(Context cx,WindowRowSet ws)
            {
                ws = (WindowRowSet)ws.Build(cx);
                var bmk = ws?.rtree.First(cx);
                return (bmk == null) ? null 
                    : new WindowCursor(cx,ws.defpos,cx._Dom(ws), 0,bmk._key);
            }
            internal static Cursor New(WindowRowSet ws, Context cx)
            {
                ws = (WindowRowSet)ws.Build(cx);
                var bmk = ws?.rtree.Last(cx);
                return (bmk == null) ? null
                    : new WindowCursor(cx, ws.defpos, cx._Dom(ws), 0, bmk._key);
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
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
            RemoteCols = -373, // CList<long> SqlValue
            RestValue = -457,   // TArray
            SqlAgent = -256; // string
        internal TArray aVal => (TArray)mem[RestValue];
        internal long restView => (long)(mem[_RestView] ?? -1L);
        internal string defaultUrl => (string)mem[DefaultUrl];
        internal string sqlAgent => (string)mem[SqlAgent] ?? "Pyrrho";
        internal CTree<long, string> namesMap =>
            (CTree<long, string>)mem[RestView.NamesMap] ?? CTree<long, string>.Empty;
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
            var vs = BList<SqlValue>.Empty; // the resolved select list in query rowType order
            var vd = CList<long>.Empty;
            var dt = cx._Dom(vw);
            var qn = q?.Needs(cx, -1L, CTree<long, bool>.Empty);
            cx.defs += (vw.infos[cx.role.defpos].name, lp, Ident.Idents.Empty);
            int d;
            var nm = CTree<long, string>.Empty;
            var mn = CTree<string, long>.Empty;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var ma = BTree<string, SqlValue>.Empty; // the set of referenceable columns in dt
            for (var b = vw.framing.obs.First(); b != null; b = b.Next())
            {
                var c = b.value();
                if (c is SqlValue sc)
                {
                    ma += (sc.name, sc);
                    mn += (sc.name, c.defpos);
                }
            }
            var ur = (RowSet)cx.obs[vw.usingTableRowSet];
            for (var b = dt.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var ns = cx._Ob(p).NameFor(cx);
                if (ur?.names.Contains(ns)==true)
                    continue; 
                var sf = (SqlValue)vw.framing.obs[mn[ns]];
                if (!(sf is SqlCopy))
                {
                    vd += p;
                    nm += (p, ns);
                }
            }
            for (var b = dt.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var sc = (SqlValue)cx.obs[p];
                if (vs.Has(sc))
                    continue;
                if (q != null && !qn.Contains(p))
                    continue;
                vs += sc;
            }
            d = vs.Length; // apparent repetition of the dt.rowType loop here is simply to get the display
            for (var b = dt.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var sc = (SqlValue)cx.obs[p];
                if (vs.Has(sc))
                    continue;
                vs += sc;
            }
            var fd = (vs == BList<SqlValue>.Empty) ? dt : new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs, d);
            var r = BTree<long, object>.Empty + (ObInfo.Name, vw.infos[cx.role.defpos].name)
                   + (_Domain, fd.defpos) + (RestView.NamesMap, nm)
                   + (RestView.UsingTableRowSet, vw.usingTableRowSet)
                   + (Matching, mg) + (RemoteCols, vd)
                   + (RSTargets, new CTree<long, long>(vw.viewPpos, lp.dp))
                   + (_Depth, cx.Depth(fd, vw));
            if (vw.usingTableRowSet < 0)
            {
                var vi = cx._Ob(vw.viewPpos).infos[cx.role.defpos];
                r += (DefaultUrl, vi.metadata[Sqlx.URL]?.ToString() ??
                    vi.metadata[Sqlx.DESC]?.ToString() ?? "");
            }
            return r;
        }
        public static RestRowSet operator+(RestRowSet rs,(long,object)x)
        {
            return (RestRowSet)rs.New(rs.mem + x);
        }
        public static RestRowSet operator-(RestRowSet rs,long p)
        {
            return (RestRowSet)rs.New(rs.mem - p);
        }
        internal override BList<long> SourceProps => BList<long>.Empty;
        protected override Cursor _First(Context _cx)
        {
            return RestCursor.New(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return RestCursor.New(this, _cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestRowSet(defpos,m);
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            if (cx.obs[usingTableRowSet] is TableRowSet ur)
            {
                var ud = cx._Dom(ur);
                if (ud.representation.Contains(rp)) // really
                    return false;
            }
            return base.Knows(cx, rp, ambient);
        }
        internal override string DbName(Context cx)
        {
            var _vw = (RestView)cx.obs[restView];
            var vi = cx._Ob(_vw.viewPpos).infos[cx.role.defpos];
            return GetUrl(cx,vi).Item1;
        }
        internal override DBObject Relocate(long dp)
        {
            return new RestRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RestRowSet)base._Replace(cx, so, sv);
            r += (_RestView, cx.ObReplace(restView, so, sv));
            var ch = false;
            var rc = CList<long>.Empty;
            var nm = CTree<long, string>.Empty;
            for (var b = remoteCols.First();b!=null;b=b.Next())
            {
                var op = b.value();
                var p = cx.ObReplace(op, so, sv);
                rc += p;
                if (namesMap.Contains(op))
                    nm += (p,namesMap[op]);
                ch = ch || p != op;
            }
            if (ch)
                r = r + (RemoteCols, rc) + (RestView.NamesMap,nm);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RestRowSet)base._Fix(cx);
            var rc = cx.FixLl(remoteCols);
            if (rc != remoteCols)
                r += (RemoteCols, rc);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (RestView.NamesMap, nm);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RestRowSet)base._Relocate(cx);
            var rc = cx.FixLl(remoteCols);
            if (rc != remoteCols)
                r += (RemoteCols, rc);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (RestView.NamesMap, nm);
            return r;
        }
        public HttpWebRequest GetRequest(Context cx, string url, ObInfo vi)
        {
            var rv = (RestView)cx.obs[restView];
            var vwdesc = cx.url ?? defaultUrl;
            var rq = WebRequest.Create(url) as HttpWebRequest;
            rq.UserAgent = "Pyrrho " + PyrrhoStart.Version[1];
            var cr = (rv.clientName??cx.user.name) + ":" + rv.clientPassword;
            var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
            rq.Headers.Add("Authorization: Basic " + d);
            if (vi?.metadata.Contains(Sqlx.ETAG) == true)
            {
                rq.Headers.Add("If-Unmodified-Since: "
                    + new THttpDate(((Transaction)cx.db).startTime,
                            vi.metadata.Contains(Sqlx.MILLI)));
                if ((cx.db as Transaction)?.etags[vwdesc] is string et)
                    rq.Headers.Add("If-Match: " + et);
            }
            return rq;
        }
        public static HttpWebResponse GetResponse(WebRequest rq)
        {
            HttpWebResponse wr;
            try
            {
                wr = rq.GetResponse() as HttpWebResponse;
            }
            catch (WebException e)
            {
                wr = e.Response as HttpWebResponse;
                if (wr == null)
                    throw new DBException("3D003");
                if (wr.StatusCode == HttpStatusCode.Unauthorized)
                    throw new DBException("42105");
                if (wr.StatusCode == HttpStatusCode.NotFound)
                    throw new DBException("3D000");
                if (wr.StatusCode == HttpStatusCode.Forbidden)
                {
                    var ws = wr.GetResponseStream();
                    if (ws!=null)
                    {
                        var s = new StreamReader(ws).ReadToEnd();
                        if (PyrrhoStart.HTTPFeedbackMode)
                            Console.WriteLine(s);
                        ws.Close();
                        throw new DBException(s.Substring(0, 5));
                    }
                    throw new DBException("42105");
                }
                if ((int)wr.StatusCode == 412)
                    throw new DBException("40082");
            }
            catch
            {
                throw new DBException("40082");
            }
            return wr;
        }
        internal (string, string) GetUrl(Context cx, ObInfo vi,
            string url = null)
        {
            url = cx.url ?? url ?? defaultUrl;
            var sql = new StringBuilder();
            string targetName ;
            string[] ss;
            var mu = vi?.metadata.Contains(Sqlx.URL) == true;
            if (mu)
            {
                if (url == "" || url==null)
                    url = vi.metadata[Sqlx.URL].ToString();
                sql.Append(url);
                for (var b = matches.First(); b != null; b = b.Next())
                {
                    var kn = ((SqlValue)cx.obs[b.key()]).name;
                    sql.Append("/"); sql.Append(kn);
                    sql.Append("="); sql.Append(b.value());
                }
                url = sql.ToString();
                ss = url.Split('/');
                targetName = ss[5];
            }
            else
            {
                url = vi.metadata[Sqlx.DESC]?.ToString() ?? cx.url ?? url ??defaultUrl;
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
            return new CTree<long,Domain>(defpos,cx._Dom(this));
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
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long,object> m=null)
        {
            // what might we need?
            var xs = ((CTree<long,bool>)mm[_Where]??CTree<long,bool>.Empty)
                +  ((CTree<long, bool>)mm[Having] ?? CTree<long, bool>.Empty); 
            // deal with group cols
            var gr = (long)(mm[Group] ?? -1L);
            var groups = (GroupSpecification)cx.obs[gr];
            var gs = groupings;
            for (var b = groups?.sets.First(); b != null; b = b.Next())
                gs = _Info(cx, (Grouping)cx.obs[b.value()], gs);
            if (gs != groupings)
            {
                mm += (Groupings, gs);
                var gc = cx.GroupCols(gs);
                mm += (GroupCols, gc);
                for (var c = gc.rowType.First(); c != null; c = c.Next())
                    xs += (c.value(), true);
            }
            var dt = cx._Dom(this);
            // Collect all operands that we don't have yet
            var ou = CTree<long, bool>.Empty;
            for (var b = xs.First();b!=null;b=b.Next())
            {
                var e = (SqlValue)cx.obs[b.key()];
                for (var c = e.Operands(cx).First(); c != null; c = c.Next())
                    if (!dt.representation.Contains(c.key()))
                        ou += (c.key(), true);
            }
            // Add them to the Domain if necessary
            var rc = dt.rowType;
            var rs = dt.representation;
            var nm = namesMap;
            for (var b = ou.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var v = cx.obs[p];
                rc += p;
                rs += (p, cx._Dom(v));
                nm += (p, v.alias?? v.NameFor(cx));
            }
            var im = mem;
            var ag = (CTree<long, bool>)mm[Domain.Aggs] ?? CTree<long, bool>.Empty;
            if (ag != CTree<long, bool>.Empty)
            {
                var dm = (Domain)cx.obs[(long)(mm[AggDomain] ?? -1L)];
                var nc = CList<long>.Empty; // start again with the aggregating rowType, follow any ordering given
                var ns = CTree<long, Domain>.Empty;
                var gb = ((Domain)m?[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
                var kb = KnownBase(cx);
                var ut = (TableRowSet)cx.obs[usingTableRowSet];
                Domain ud = (ut == null) ? null : cx._Dom(ut);
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    var v = (SqlValue)cx.obs[p];
                    if (v is SqlFunction ct && ct.kind == Sqlx.COUNT && ct.mod == Sqlx.TIMES)
                    {
                        nc += p;
                        ns += (p, Domain.Int);
                    }
                    else if (v.IsAggregation(cx) != CTree<long, bool>.Empty)
                        for (var c = ((SqlValue)cx.obs[p]).KnownFragments(cx, kb).First();
                            c != null; c = c.Next())
                        {
                            var k = c.key();
                            if (ns.Contains(k))
                                continue;
                            nc += k;
                            ns += (k, cx._Dom(cx.obs[k]));
                        }
                    else if (gb.Contains(p))
                    {
                        nc += p;
                        ns += (p, cx._Dom(v));
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
            var ag = (CTree<long, bool>)mm[Domain.Aggs];
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = mem;
            var r = this;
            if (mm[GroupCols] is Domain gd)
            {
                m += (GroupCols, gd);
                m += (Groupings, mm[Groupings]);
                m += (Group, mm[Group]);
                if (gd != groupCols)
                    r = (RestRowSet)Apply(m, cx);
            }
            else
                m += (Domain.Aggs, ag);
            var dm = (Domain)cx.obs[(long)mm[AggDomain]];
            var nc = CList<long>.Empty; // start again with the aggregating rowType, follow any ordering given
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            var dn = cx._Dom(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var v = (SqlValue)cx.obs[p]; 
                if (v is SqlFunction ct && ct.kind == Sqlx.COUNT && ct.mod == Sqlx.TIMES)
                {
                    nc += p;
                    ns += (p, Domain.Int);
                }
                if (v.IsAggregation(cx) != CTree<long, bool>.Empty)
                    for (var c = ((SqlValue)cx.obs[p]).KnownFragments(cx, kb).First();
                        c != null; c = c.Next())
                    {
                        var k = c.key();
                        if (ns.Contains(k))
                            continue;
                        nc += k;
                        ns += (k, cx._Dom(cx.obs[k]));
                    }
                else if (gb.Contains(p))
                {
                    nc += p;
                    ns += (p, cx._Dom(v));
                }
            }
            var d = nc.Length;
            for (var b = dn.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (ns.Contains(p))
                    continue;
                nc += p;
                ns += (p, cx._Dom(cx.obs[p]));
            }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc,d) + (Domain.Aggs, ag);
            m = m + (_Domain, nd.defpos) + (Domain.Aggs, ag);
            cx.Add(nd);
            r = (RestRowSet)r.New(m);
            cx.Add(r);
            cx.done = od;
            return (RowSet)cx.obs[defpos]; // will have changed
        }

        internal override CTree<long, bool> AggsKnown(CTree<long, bool> ag, Context cx)
        {
            var ma = CTree<long, bool>.Empty;
            for (var b=ag.First();b!=null;b=b.Next())
                for (var c = ((SqlValue)cx.obs[b.key()]).Operands(cx).First();
                                        c != null; c = c.Next())
                    if (namesMap.Contains(c.key()))
                        ma += (b.key(), true);
            return ma;
        }
        protected static CList<long> _Info(Context cx, Grouping g, CList<long> gs)
        {
            gs += g.defpos;
            var cs = BList<SqlValue>.Empty;
            for (var b = g.members.First(); b != null; b = b.Next())
                cs += (SqlValue)cx.obs[b.key()];
            var dm = new Domain(Sqlx.ROW, cx, cs);
            cx.Replace(g, g + (_Domain, dm.defpos));
            for (var b = g.groups.First(); b != null; b = b.Next())
                gs = _Info(cx, b.value(), gs);
            return gs;
        }
        internal override RowSet Build(Context cx)
        {
            if (usingTableRowSet >= 0 && !cx.cursors.Contains(usingTableRowSet))
                throw new PEException("PE389");
            var vw = (RestView)cx.obs[restView];
            var vi = cx._Ob(vw.viewPpos).infos[cx.role.defpos];
            var (url,targetName) = GetUrl(cx,vi,defaultUrl);
            var sql = new StringBuilder();
            var rq = GetRequest(cx,url, vi);
            rq.Accept = vw?.mime ?? "application/json";
            if (vi?.metadata.Contains(Sqlx.URL) == true)
                rq.Method = "GET";
            else
            {
                rq.Method = "POST";
                sql.Append("select ");
                if (distinct)
                    sql.Append("distinct ");
                var co = "";
                var dd = cx._Dom(this);
                cx.groupCols += (domain, groupCols);
                var hasAggs = dd.aggs!=CTree<long,bool>.Empty;
                for (var b = dd.rowType.First(); b != null; b = b.Next())
                    if (cx.obs[b.value()] is SqlValue s)
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
                if (groupCols!=null)
                {
                    sql.Append(" group by "); co = "";
                    for (var b=groupCols.rowType.First(); b != null; b = b.Next())
                    {
                        sql.Append(co); co = ",";
                        var sv = (SqlValue)cx.obs[b.value()];
                        sql.Append(sv.alias??sv.name);
                    }
                }
                if (having.Count > 0)
                {
                    var sw = HavingString(cx, namesMap);
                    if (sw.Length > 0)
                    {
                        sql.Append(" having ");
                        sql.Append(sw);
                    }
                } 
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            if (rq.Method=="POST")
            {
                var bs = Encoding.UTF8.GetBytes(sql.ToString());
                rq.ContentType = "text/plain";
                rq.ContentLength = bs.Length;
                cx.url = url;
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
            }
            HttpWebResponse wr;
            try
            {
                wr = GetResponse(rq);
                if (wr == null)
                    throw new DBException("2E201", url);
                var et = wr.GetResponseHeader("ETag");
                if (et != null && et.StartsWith("W/"))
                    et = et.Substring(2);
                if (et != null)
                    et = et.Trim('"');
                var ds = wr.GetResponseHeader("Description");
                var cl = wr.GetResponseHeader("Classification");
                var ld = wr.GetResponseHeader("LastData");
                var lv = (cl != "") ? Level.Parse(cl,cx) : Level.D;
                var mime = wr.GetResponseHeader("Content-Type") ?? "text/plain";
                var s = wr.GetResponseStream();
                var sr = new StreamReader(s).ReadToEnd();
        //        if (PyrrhoStart.HTTPFeedbackMode)
        //           Console.WriteLine(sr);
                TypedValue a = null;
                var or = cx.result;
                cx.result = target; // sneaky
                var rd = cx._Dom(this);
                if (s != null)
                    a = rd.Parse(0, sr, mime, cx);
                cx.result = or;
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (a is TArray aa)
                        Console.WriteLine("--> " + aa.list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (a?.ToString() ?? "null"));
                }
                s.Close();
                var len = (a is TArray ta) ? ta.Length : 1;
                var r = this + (RestValue, a) + (_Built, true) 
                    + (Index.Tree,new MTree(len));
                cx._Add(r);
                if (et != null && et != "" && rq.Method == "POST") // Pyrrho manual 3.8.1
                {
                    var vwdesc = cx.url ?? defaultUrl;
                    var tr = (Transaction)cx.db;
                    cx.db = tr + (Transaction._ETags,tr.etags+(vwdesc,et));
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
        BTree<long,bool> Grouped(Context cx, Grouping gs, StringBuilder sql, ref string cm, 
            BTree<long,bool> ids)
        {
            for (var b = gs.members.First(); b!=null; b=b.Next())
            {
                var gi = (Grouping)cx.obs[b.key()];
                if (Knows(cx,gi.defpos) && !ids.Contains(gi.defpos))
                {
                    ids+=(gi.defpos,true);
                    sql.Append(cm); cm = ",";
                    sql.Append(gi.alias);
                }
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
            {
                var sv = (SqlValue)cx.obs[b.key()];
                if (sv.KnownBy(cx, cs))
                {
                    try
                    {
                        var sw = sv.ToString(sqlAgent,Remotes.Operands, remoteCols,namesMap, cx);
                        if (sw.Length > 1)
                        {
                            sb.Append(cm); cm = " and ";
                            sb.Append(sw);
                        }
                    }
                    catch (DBException) { }
                }
            }
            var ms = BTree<string, string>.Empty;
            for (var b = matches.First(); b != null; b = b.Next())
            {
                if (!cs.Contains(b.key()))
                    continue;
                try
                {
                    var nm = cx.obs[b.key()].ToString(sqlAgent, Remotes.Operands, 
                        remoteCols, namesMap, cx);
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
                sb.Append("=");
                sb.Append(b.value());
            }
            return sb.ToString();
        }
        public string HavingString<V>(Context cx, CTree<long,V> cs) where V:IComparable
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = having.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.key()];
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
            return sb.ToString();
        }
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, CList<long> rt)
        {
            ts +=(Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = vw.infos[cx.role.defpos];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, ts, PTrigger.TrigType.Insert)
                : new RESTActivation(cx, ts, PTrigger.TrigType.Insert);
            return new BTree<long, TargetActivation>(ts.target, ta);
        }
        internal override BTree<long, TargetActivation> Update(Context cx,RowSet fm)
        {
            fm += (Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = vw.infos[cx.role.defpos];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, fm, PTrigger.TrigType.Update)
                : new RESTActivation(cx, fm, PTrigger.TrigType.Update);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override BTree<long, TargetActivation> Delete(Context cx,RowSet fm)
        {
            fm += (Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = vw.infos[cx.role.defpos];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, fm, PTrigger.TrigType.Delete)
                : new RESTActivation(cx, fm, PTrigger.TrigType.Delete);
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" ");
            if (defaultUrl != "")
                sb.Append(defaultUrl);
            if (remoteCols != CList<long>.Empty)
            {
                sb.Append(" RemoteCols:");
                var cm = "(";
                for (var b = remoteCols.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (namesMap != CTree<long, string>.Empty)
            {
                sb.Append(" RemoteNames:");
                var cm = "(";
                for (var b = namesMap.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append("=");
                    sb.Append(b.value());
                }
                sb.Append(")");
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
                sb.Append(")");
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
                :base(cx,rrs.defpos,cx._Dom(rrs), pos,E,tr)
            {
                _rrs = rrs; _ix = ix;
                var tb = cx._Dom(rrs).rowType.First();
                for (var b = rrs.sRowType.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                    cx.values += (b.value(), tr[tb.value()]);
            }
            RestCursor(Context cx,RestCursor rb,long p,TypedValue v)
                :base(cx,rb._rrs,rb._pos,E,rb+(p,v))
            {
                _rrs = rb._rrs; _ix = rb._ix;
            }
            static TRow _Value(Context cx, RestRowSet rrs, int pos)
            {
                cx.values += ((TRow)rrs.aVal[pos]).values;
                return new TRow(cx._Dom(rrs),cx.values);
            }
            internal static RestCursor New(Context cx,RestRowSet rrs)
            {
                cx.obs += (rrs.defpos, rrs);
                return (rrs.aVal.Length!=0)?new RestCursor(cx, rrs, 0, 0):null;
            }
            internal static RestCursor New(RestRowSet rrs, Context cx)
            {
                cx.obs += (rrs.defpos, rrs);
                var i = rrs.aVal.Length - 1;
                return (i >= 0) ? new RestCursor(cx, rrs, 0, i) : null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new RestCursor(cx, this, p, v);
            }
            protected override Cursor _Next(Context cx)
            {
                var i = _ix + 1;
                return (i < _rrs.aVal.Length) ? new RestCursor(cx, _rrs, _pos + 1, i) : null;
            }
            protected override Cursor _Previous(Context cx)
            {
                var i = _ix - 1; 
                return (i >=0)?new RestCursor(cx, _rrs, _pos + 1, i): null;
            }
            internal override BList<TableRow> Rec()
            {
                var r = BList<TableRow>.Empty;
                for (var i = 0; i < _rrs.aVal.Length; i++)
                    r += new RemoteTableRow(_rrs.restView, ((TRow)_rrs.aVal[i]).values,
                        _rrs.defaultUrl,_rrs);
                return r;
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class RestRowSetUsing : InstanceRowSet
    {
        internal const long
            RestTemplate = -443, // long RestRowSet
            UrlCol = -446, // long SqlValue
            UsingCols = -259; // CList<long> SqlValue
        internal long usingTableRowSet => (long)(mem[RestView.UsingTableRowSet] ?? -1L);
        internal long urlCol => (long)(mem[UrlCol] ?? -1L);
        internal long template => (long)mem[RestTemplate];
        internal CList<long> usingCols =>
            (CList<long>)mem[UsingCols]??CList<long>.Empty;
        public RestRowSetUsing(Iix lp,Context cx,RestView vw, long rp,
            TableRowSet uf, Domain q)  :base(lp.dp,_Mem(lp.dp,cx,rp,uf,q) 
                 + (RestTemplate, rp) + (ObInfo.Name, vw.infos[cx.role.defpos].name)
                 + (RSTargets,new CTree<long, long>(vw.viewPpos, lp.dp))
                 + (RestView.UsingTableRowSet, uf.defpos)  
                 + (ISMap, uf.iSMap) + (SIMap,uf.sIMap)
                 + (SRowType, uf.sRowType))
        {
            cx.Add(this);
        }
        protected RestRowSetUsing(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(long dp,Context cx,long rp, TableRowSet uf,Domain q)
        {
            var r = BTree<long, object>.Empty;
            var rr = (RowSet)cx.obs[rp];
            var rd = cx._Dom(rr);
            var ud = cx._Dom(uf);
            // identify the urlCol and usingCols
            var ab = ud.rowType.Last();
            var ul = ab.value();
            var uc = ud.rowType - ab.key();
            return r + (UsingCols,uc)+(UrlCol,ul) + (_Domain,rd.defpos)
                  +(_Depth, cx.Depth(rd, uf, rr));
        }
        public static RestRowSetUsing operator +(RestRowSetUsing rs, (long, object) x)
        {
            return (RestRowSetUsing)rs.New(rs.mem + x);
        }
        internal override CTree<long,bool> Sources(Context cx)
        {
            return base.Sources(cx) + (usingTableRowSet,true) + (template,true);
        }
        internal override BTree<long, RowSet> AggSources(Context cx)
        {
            var t = template;
            return new BTree<long, RowSet>(t, (RowSet)cx.obs[t]); // not usingTableRowSet
        }
        internal override CTree<long, Domain> _RestSources(Context cx)
        {
            return ((RowSet)cx.obs[template])._RestSources(cx);
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
            var dt = cx._Dom(this);
            cx.values += (defpos, new TArray(dt));
            var rr = (RestRowSet)cx.obs[template];
            var ru = (TableRowSet)cx.obs[usingTableRowSet];
            var pos = 0;
            var a = (TArray)cx.values[defpos] ?? new TArray(dt);
            for (var uc = (TableRowSet.TableCursor)ru.First(cx); uc != null;
                uc = (TableRowSet.TableCursor)uc.Next(cx))
            {
                cx.values += uc.values;
                cx.cursors += (ru.from, uc);
                cx.url = uc[urlCol].ToString();
                for (var rc = (RestRowSet.RestCursor)rr.First(cx); rc != null;
                    rc = (RestRowSet.RestCursor)rc.Next(cx))
                    a += new RestUsingCursor(cx, this, pos++, uc, rc);
                cx.obs += (rr.defpos, rr - RestRowSet.RestValue - _Built);
                cx.values -= uc.values;
            }
            cx.values += (defpos, a);
            cx.url = null;
            return base.Build(cx);
        }
        internal override RowSet Apply(BTree<long,object>mm,Context cx,BTree<long,object> im=null)
        {
            if (mm == BTree<long, object>.Empty)
                return this;
            var ru = (RestRowSetUsing)base.Apply(mm,cx);
            // Watch out for situation where an aggregation has successfully moved to the Template
            // as the having condition will no longer work at the RestRowSetUsing level
            var re = (RestRowSet)cx.obs[ru.template];
            var uh = ru.having;
            for (var b = uh.First(); b != null; b = b.Next())
                if (re.having.Contains(b.key()))
                    uh -= b.key();
            if (uh != ru.having)
                ru += (Having, uh);
            return ru;
        }
        protected override Cursor _First(Context _cx)
        {
            return RestUsingCursor.New(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return RestUsingCursor.New(this, _cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestRowSetUsing(defpos, m);
        }

        internal override DBObject Relocate(long dp)
        {
            return new RestRowSetUsing(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RestRowSetUsing)base._Replace(cx,so,sv);
            r += (RestView.UsingTableRowSet, cx.ObReplace(usingTableRowSet, so, sv));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RestRowSetUsing)base._Fix(cx);
            var nu = cx.Fix(usingTableRowSet);
            if (nu != usingTableRowSet)
                r += (RestView.UsingTableRowSet, nu);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RestRowSetUsing)base._Relocate(cx);
            r += (RestView.UsingTableRowSet, cx.Fix(usingTableRowSet));
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
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, CList<long> rt)
        {
            return ((RowSet)cx.obs[template]).Insert(cx, ts, rt);
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
            return ((RowSet)cx.obs[template]).Update(cx, fm);
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
            return ((RowSet)cx.obs[template]).Delete(cx, fm);
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
                sb.Append(")");
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
                return new TRow(cx._Dom(ru), tc.values + rc.values - ru.defpos);
            }
            static RestUsingCursor Get(Context cx, RestRowSetUsing ru, int pos)
            {
                var ls = ((TArray)cx.values[ru.defpos]).list;
                if (pos < 0 || pos >= ls.Length)
                    return null;
                var cu = (RestUsingCursor)ls[pos];
                cx.cursors += (cu._tc._rowsetpos, cu._tc);
                cx.cursors += (ru.defpos, cu);
                cx.values += cu.values;
                var dm = cx._Dom(ru);
                for (var b = dm.aggs.First(); b != null; b = b.Next())
                    cx.values -= b.key();
                for (var b = ru.sRowType.First(); b != null; b = b.Next())
                    cx.values += (b.value(), cu[b.key()]);
                return cu;
            }
            internal static Cursor New(Context cx, RestRowSetUsing ru)
            {
                return Get(cx,ru,0);
            }
            internal static Cursor New(RestRowSetUsing ru, Context cx)
            {
                var ls = ((TArray)cx.values[ru.defpos]).list;
                var n = ls.Length - 1;
                return Get(cx,ru,n);
            }
            protected override Cursor _Next(Context cx)
            {
                return Get(cx, _ru, _pos + 1);
            }
            protected override Cursor _Previous(Context cx)
            {
                return Get(cx, _ru, _pos - 1);
            }
            internal override BList<TableRow> Rec()
            {
                return new BList<TableRow>(_tc._rec);
            }

            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
            : base(dp, _Mem(cx, dp, td))
        {
            cx.db = cx.db.BuildLog();
            cx.Add(this);
        }
        protected LogRowsRowSet(long dp,BTree<long,object> m) :base(dp,m)
        { }
        static BTree<long, object> _Mem(Context cx, long dp, long td)
        {
            var tb = cx.db.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix();
            cx.Add(tb);
            var r = new BTree<long, object>(TargetTable, tb.defpos);
            var rt = BList<SqlValue>.Empty;
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
            return (LogRowsRowSet)r.New(r.mem + x);
        }
        protected override Cursor _First(Context _cx)
        {
            return LogRowsCursor.New(_cx, this,0,targetTable);
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException(); // someday maybe
        }
        internal override DBObject Relocate(long dp)
        {
            return new LogRowsRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowsRowSet) base._Replace(cx, so, sv);
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
            internal static LogRowsCursor New(Context cx,LogRowsRowSet lrs,int pos,long p)
            {
                Physical ph;
                PTransaction pt = null;
                for (var b = cx.db.log.PositionAt(p); b != null; b = b.Next())
                {
                    var vs = CTree<long, TypedValue>.Empty;
                    p = b.key();
                    switch (b.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record1:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Record;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = cx._Dom(lrs).rowType.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
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
                                }
                                return new LogRowsCursor(cx, lrs, pos, 
                                    new TRow(cx._Dom(lrs), vs));
                            }
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Record;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = cx._Dom(lrs).rowType.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
                                    switch (c.key())
                                    {
                                        case 0: vs += (cp, new TPosition(p)); break;
                                        case 1: vs += (cp, new TChar("Update")); break;
                                        case 2: vs += (cp, new TPosition(rc.defpos)); break;
                                        case 3: vs += (cp, new TPosition(rc.trans)); break;
                                        case 4:
                                            vs += (cp, new TDateTime(new DateTime(rc.time)));
                                            break;
                                    }
                                }
                                return new LogRowsCursor(cx, lrs, pos, 
                                    new TRow(cx._Dom(lrs), vs));
                            }
                        case Physical.Type.Delete:
                        case Physical.Type.Delete1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Delete;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = cx._Dom(lrs).rowType.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
                                    switch (c.key())
                                    {
                                        case 0: vs += (cp, new TPosition(p)); break;
                                        case 1: vs += (cp, new TChar("Delete")); break;
                                        case 2: vs += (cp, new TPosition(rc.ppos)); break;
                                        case 3: vs += (cp, new TPosition(rc.trans)); break;
                                        case 4:
                                            vs += (cp, new TDateTime(new DateTime(rc.time)));
                                            break;
                                        default: vs += (cp, TNull.Value); break;
                                    }
                                }
                                return new LogRowsCursor(cx, lrs, pos, new TRow(cx._Dom(lrs), vs));
                            }
                        case Physical.Type.PTransaction:
                        case Physical.Type.PTransaction2:
                            {
                                (ph,_) = cx.db._NextPhysical(b.key(), pt);
                                pt = (PTransaction)ph;
                                break;
                            }
                    }
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,-1L);
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException(); // someday maybe
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
        : base(dp, _Mem(cx, dp, c) + (LogRow, r) + (LogCol, c))
        {
            cx.Add(this);
        }
        protected LogRowColRowSet(long dp,BTree<long,object>m) :base(dp,m)
        { }
        static BTree<long, object> _Mem(Context cx, long dp, long cd)
        {
            var tc = cx.db.objects[cd] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            cx.Add(tc);
            var tb = cx.db.objects[tc.tabledefpos] as Table;
            cx.Add(tb);
            var rt = BList<SqlValue>.Empty;
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
            return (LogRowColRowSet)r.New(r.mem + x);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" for "); sb.Append(targetTable);
            sb.Append("(");  sb.Append(Uid(logRow)); 
            sb.Append(",");  sb.Append(Uid(logCol)); 
            sb.Append(")"); 
        }
        internal override Basis New(BTree<long,object> m)
        {
            return new LogRowColRowSet(defpos, m);
        }
        protected override Cursor _First(Context cx)
        {
            return LogRowColCursor.New(cx,this,0,(null,logRow));
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException(); // someday maybe
        }
        internal override DBObject Relocate(long dp)
        {
            return new LogRowColRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowColRowSet) base._Replace(cx, so, sv);
            r += (LogRowsRowSet.TargetTable, cx.ObReplace(targetTable, so, sv));
            r += (LogCol, cx.ObReplace(logCol, so, sv));
            return r;
        }
        // shareable as of 26 April 2021
        internal class LogRowColCursor : Cursor
        {
            readonly LogRowColRowSet _lrs;
            readonly (Physical,long) _next;
            LogRowColCursor(Context cx, LogRowColRowSet lrs, int pos, 
                (Physical,long) next, TRow rw) 
                : base(cx, lrs, pos, E, rw) 
            {
                _lrs = lrs; _next = next;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowColCursor(cx, _lrs, _pos, _next, 
                    this + (p, v));
            }
            internal static LogRowColCursor New(Context cx,LogRowColRowSet lrs,int pos,
                (Physical,long) nx,PTransaction trans=null)
            {
                var vs = CTree<long, TypedValue>.Empty;
           //     var tb = lrs.targetTable;
                var tc = lrs.logCol;
                var rp = nx.Item2;
                if (trans == null)
                    for (var b = cx.db.log.PositionAt(nx.Item2); trans == null && b != null;
                        b = b.Previous())
                        if (b.value() is Physical.Type.PTransaction ||
                            b.value() is Physical.Type.PTransaction2)
                            trans = (PTransaction)cx.db._NextPhysical(b.key()).Item1;
                if (rp < 0)
                    return null;
                if (nx.Item1==null)
                    nx = cx.db._NextPhysical(rp,trans);
                for (; nx.Item1 != null;)
                {
                    var rc = nx.Item1 as Record; // may be an Update 
                    if (rc==null || lrs.logRow != rc.defpos || !rc.fields.Contains(tc))
                    {
                        nx = cx.db._NextPhysical(nx.Item2, trans);
                        if (nx.Item1 is PTransaction nt)
                            trans = nt;
                        continue;
                    }
                    var b = cx._Dom(lrs).rowType.First(); vs += (b.value(), new TPosition(rc.ppos));
                    b = b.Next(); vs += (b.value(), rc.fields[tc] ?? TNull.Value);
                    b = b.Next(); vs += (b.value(), new TPosition(rc.trans));
                    b = b.Next(); vs += (b.value(), new TDateTime(new DateTime(rc.time)));
                    var done = false;
                    for (nx = cx.db._NextPhysical(nx.Item2, trans);
                        nx.Item1 != null && nx.Item2 >= 0;
                        nx = cx.db._NextPhysical(nx.Item2, trans))
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
                            case Physical.Type.PTransaction2:
                                trans = (PTransaction)ph;
                                break;
                        }
                        if (done)
                            break;
                    }
                    if (done)
                    {
                        b = b.Next(); vs += (b.value(), new TPosition(trans.ppos));
                        b = b.Next(); vs += (b.value(), new TDateTime(new DateTime(trans.pttime)));
                    }
                    var rb = new LogRowColCursor(cx, lrs, pos, nx, new TRow(cx._Dom(lrs), vs));
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,_next);
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException(); // someday maybe
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
        }
    }
}