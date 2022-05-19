using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System;
using System.IO;
using System.Net;
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
    /// The Domain by this stage will have uids for all columns in the target
    /// whether mentioned explicitly or not: at the top level, the display will be 
    /// the columns that will be sent to the client.
    /// The finder assists with looking up the values of row entries specified by
    /// the Domain: it says which RowSet computed (or can compute) the value for a uid.
    /// Finder must be updated in Review, Replace and Relocate.
    /// 
    /// RowSets must not contain any physical uids in their column uids
    /// or within their domain's representation. See InstanceRowSet for how to avoid.
    /// 
    /// An aggregation E has a unique "home" rowset A that computes it: such that no source of A has E.
    /// A is always a SelectRowSet or RestRowSet, and E.from must be A.
    /// 
    /// Validity:
    /// V1. All operands of non-aggregating expressions and all column ids must be in the finder.
    /// V2. For an aggregating expression E in R's rowType, E must be in the finder.
    /// V3. If a rowType has an aggregation, then all columns must be aggregations or be grouped.
    /// V4. If a rowset R has a grouping, then the group columns must be in the select list (and finder)
    /// V5. If R has a grouping, the select list must include at least one aggregation E 
    /// (R is then not necessarily the home rowset of E, so R need not be SelectRowSrt or RestRowSet)
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
                ProvidesTarget=8 } // RowType's uids are in sce.RowType in some order
        internal const long
            AggDomain = -465, // Domain temporary during SplitAggs
            Asserts = -212, // Assertions
            Assig = -174, // CTree<UpdateAssignment,bool> 
            _Built = -402, // bool
            _CountStar = -281, // long 
            _Data = -368, // long RowSet
            Distinct = -239, // bool
            Filter = -180, // CTree<long,TypedValue> matches to be imposed by this query
            _Finder = -403, // CTree<long,Finder> SqlValue
            Group = -199, // long GroupSpecification
            Groupings = -406,   //CList<long>   Grouping
            GroupCols = -461, // Domain
            GroupIds = -462, // CTree<long,Domain> temporary during SplitAggs
            Having = -200, // CTree<long,bool> SqlValue
            _Matches = -182, // CTree<long,TypedValue> matches guaranteed elsewhere
            Matching = -183, // CTree<long,CTree<long,bool>> SqlValue SqlValue (symmetric)
            _Needed = -401, // CTree<long,Finder>  SqlValue
            OrdSpec = -184, // CList<long>
            Periods = -185, // BTree<long,PeriodSpec>
            UsingOperands = -411, // CTree<long,long> SqlValue
            _Repl = -186, // CTree<string,string> Sql output for remote views
            RestRowSetSources = -331, // CTree<long,bool>    RestRowSet or RestRowSetUsing
            _Rows = -407, // CList<TRow> 
            RowOrder = -404, //CList<long> SqlValue 
            RSTargets = -197, // CTree<long,long> Table/View RowSet 
            SimpleTableQuery = -247, //bool
            Scalar = -95, // bool
            _Source = -151, // RowSet
            Static = -152, // RowSet (defpos for STATIC)
            Stem = -211, // CTree<long,bool> RowSet 
            _Where = -190, // CTree<long,bool> Boolean conditions to be imposed by this query
            Windows = -201; // CTree<long,bool> WindowSpecification
        internal Assertions asserts => (Assertions)(mem[Asserts] ?? Assertions.None);
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
        internal CTree<long,Finder> needed =>
            (CTree<long,Finder>)mem[_Needed]; // must be initially null
        internal long groupSpec => (long)(mem[Group] ?? -1L);
        internal long target => (long)(mem[From.Target] // for table-focussed RowSets
            ?? rsTargets.First()?.key() ?? -1L); // for safety
        internal CTree<long, long> rsTargets =>
            (CTree<long, long>)mem[RSTargets] ?? CTree<long, long>.Empty;
        internal long source => (long)(mem[_Source] ??  -1L);
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>)mem[Assig]
            ?? CTree<UpdateAssignment, bool>.Empty;
        internal BList<TRow> rows =>
            (BList<TRow>)mem[_Rows] ?? BList<TRow>.Empty;
        internal long lastData => (long)(mem[Table.LastData] ?? 0L);
        internal CList<long> remoteCols =>
            (CList<long>)mem[RestView.RemoteCols] ?? CList<long>.Empty;
        /// <summary>
        /// Whether we have a simple table query
        /// </summary>
        internal bool simpletablequery => (bool)(mem[SimpleTableQuery] ?? false);
        /// <summary>
        /// List of rowsets whose cursors are constant during traversal of this rowset
        /// </summary>
        internal CTree<long, bool> stem =>
            (CTree<long, bool>)mem[Stem] ?? CTree<long, bool>.Empty;
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
        internal readonly struct Finder : IComparable
        {
            public readonly long col;
            public readonly long rowSet;
            public Finder(long c, long r)
            {
                col = c; rowSet = r;
            }
            internal Finder Fix(Context cx)
            {
                var c = cx.Fix(col);
                var r = cx.Fix(rowSet);
                return (c != col || r != rowSet) ? new Finder(c, r) : this;
            }
            public override string ToString()
            {
                return Uid(rowSet) + "[" + Uid(col) + "]";
            }

            public int CompareTo(object obj)
            {
                var that = (Finder)obj;
                var c = rowSet.CompareTo(that.rowSet);
                if (c != 0)
                    return c;
                return col.CompareTo(that.col);
            }
        }
        /// <summary>
        /// The finder maps the list of SqlValues whose values are in cursors,
        /// to a Finder.
        /// In a Context there may be several rowsets being calculated independently
        /// (e.g. a join of subqueries), and it can combine sources into a from association
        /// saying which rowset (and so which cursor) contains the current values.
        /// We could simply search along cx.obs to pick the rowset we want.
        /// Before we compute a Cursor, we place the source finder in the  Context, 
        /// so that the evaluation can use values retrieved from the source cursors.
        /// </summary>
        internal CTree<long, Finder> finder =>
            (CTree<long, Finder>)mem[_Finder] ?? CTree<long, Finder>.Empty;
        /// <summary>
        /// Constructor: a new or modified RowSet
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="cx">The ptocessing environment</param>
        /// <param name="m">The properties list</param>
        protected RowSet(long dp, Context cx, BTree<long, object> m)
            : base(dp, _Mem(cx,m))
        {
            cx.Add(this);
        }
        /// <summary>
        /// special version for From constructors that compute asserts
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
        protected static BTree<long,object> _Mem(Context cx,BTree<long,object>m)
        {
            var dp = (long)(m[_Domain] ?? -1L);
            var dm = (Domain)(cx.obs[dp]??cx.db.objects[dp]);
            var a = Assertions.SimpleCols;
            var sce = (RowSet)cx.obs[(long)(m[_Source]??m[_Data]??-1L)];
            var sd = cx._Dom(sce);
            var sb = sd?.rowType.First();
            if (sce != null)
                a |= Assertions.AssignTarget | Assertions.ProvidesTarget |
                    Assertions.MatchesTarget;
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
            var ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            if (sce!=null && (ma.Count > 0 || sce.matching.Count > 0))
                m += (Matching, sce.CombineMatching(ma, sce.matching));
            return m;
        }
        internal virtual Assertions Requires => Assertions.None;
        public static RowSet operator +(RowSet rs, (long, object) x)
        {
            return (RowSet)rs.New(rs.mem + x);
        }
        internal virtual BList<long> SourceProps => BList<long>.FromArray(_Source,
            JoinRowSet.JFirst,JoinRowSet.JSecond,
            MergeRowSet._Left,MergeRowSet._Right,
            RestRowSetUsing.RestTemplate,RestView.UsingTableRowSet);
        static BList<long> pre = BList<long>.FromArray(Having,Matching, _Where, _Matches);
        static BList<long> can = BList<long>.FromArray(Having,OrdSpec, _Where, _Matches);
        static BTree<long, bool> prt = new BTree<long, bool>(Having, true) 
            + (_Where, true) + (Matching, true) + (_Matches, true);
        static BTree<long, bool> pres = BTree<long, bool>.FromList(pre);
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
        internal override DBObject New(Context cx,BTree<long,object>mm)
        {
            return Apply(mm, cx, this);
        }
        internal RowSet MaybeApply(BTree<long, object> mm, Context cx, RowSet im)
        {
            mm -= _Built; // never passed to sources
            mm -= JoinRowSet.OnCond;
            mm -= JoinRowSet.JoinCond;
            mm -= JoinRowSet.JoinKind;
            mm -= Matching;
            // Apply is sometimes called with a full set of mem changes. The following should never be
            // passed to sources
            mm -= _Domain;
            mm -= _Depth;
            mm -= Name;
            mm -= _Finder;
            mm -= Asserts;
            if (mm == BTree<long, object>.Empty)
                return im;
            var m = mm;
            for (var b = can.First(); b != null; b = b.Next())
                m = CanApply(m, b.value(), cx, im);
            return Apply(m, cx, im);
        }
        internal override (DBObject, Ident) _Lookup(long lp, Context cx, string nm, Ident n)
        {
            var oi = cx.Inf(defpos);
            if (oi.names.Contains(n.ident))
            {
                var p = oi.names[n.ident];
                var ob = new SqlValue(n.iix.dp, n.ident, oi.dataType.representation[p],
                    new BTree<long, object>(_From,defpos));
                cx.Add(ob);
                return (ob, n.sub);
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
                        for (var c = sv.Needs(cx,r.defpos).First(); c != null; c = c.Next())
                            if (!(cx.iim[cx.instances[c.key()]] is Iix ix 
                                && ix.sd < r.Iim(cx).sd && ix.CompareTo(r.Iim(cx)) <= 0))
                                goto no;
                        a += (k, true);
                    no:;
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
        /// IMPORTANT: Initially, im is this. 
        /// In any override it is (this + properties already applied). 
        /// Do not access any properties of this: use im instead.
        /// 
        /// Passing expressions down to source rowsets:
        /// A non-aggregating expression R (in the select list or a where condition) can be passed down to 
        /// a source S if all its operands are known to S (it can then be added to R's finder)
        /// An aggregating expression E in R can be passed down to a SelectRowSet or RestRowSet source S if 
        /// all its operands are all known to S (for rowsets T between R and S in the pipeline see P1)
        /// 
        /// When a set of aggregations E are passed down from R to S (satisfying V1 and V2):
        /// P1. The donain of S (and any T) must be transformed according to validity test V3 and V4.
        /// P2. Any group ids of E known to S must become grouped in S, and all other ids of E in S must also be grouped.
        /// P3. If a where expression E is passed down to its home rowset, it becomes a having expression 
        /// (and vice versa if it is moved down further).
        /// 
        /// </summary>
        /// <param name="mm">The properties to be applied</param>
        /// <param name="cx">The context</param>
        /// <param name="im">The current state of the rowset to modify</param>
        /// <returns>updated rowset</returns>
        internal virtual RowSet Apply(BTree<long, object> mm,Context cx,RowSet im)
        {
            for (var b=mm.First();b!=null;b=b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return im;
            var od = cx.done;
            cx.done = ObTree.Empty;
            var dm = cx._Dom(im);
            var recompute = false;
            var m = im.mem;
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
                                var rm = im.matching;
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
                                m += (Matching, CombineMatching(im.matching,mg));
                                mm -= Matching; // but not further down
                                break;
                            }
                        case _Where:
                            {
                                var w = (CTree<long, bool>)v;
                                var ma = (CTree<long, TypedValue>)mm[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var oa = ma;
                                var mw = (CTree<long,bool>)m[_Where] ?? CTree<long, bool>.Empty;
                                var ow = mw;
                                var mh = (CTree<long, bool>)m[_Where] ?? CTree<long, bool>.Empty;
                                var oh = mh;
                                for (var b = w.First(); b != null; b = b.Next())
                                {
                                    var k = b.key();
                                    var sv = (SqlValue)cx.obs[k];
                                    var matched = false;
                                    if (sv is SqlValueExpr se && se.kind == Sqlx.EQL)
                                    {
                                        var le = (SqlValue)cx.obs[se.left];
                                        var ri = (SqlValue)cx.obs[se.right];
                                        if (le.isConstant(cx) && !im.matches.Contains(ri.defpos))
                                        {
                                            matched = true;
                                            ma += (ri.defpos, le.Eval(cx));
                                        }
                                        if (ri.isConstant(cx) && !im.matches.Contains(le.defpos))
                                        {
                                            matched = true;
                                            ma += (le.defpos, ri.Eval(cx));
                                        }
                                    }
                                    if (sv.KnownBy(cx, this))
                                    {
                                        if (sv.IsAggregation(cx) != CTree<long, bool>.Empty) 
                                            mh += (k, true);
                                        else if (!matched)
                                            mw += (k, true);
                                    } 
                                }
                                if (ma != oa)
                                    mm += (_Matches, ma);
                                if (mw != ow)
                                    m += (_Where, mw);
                                if (mh != oh)
                                    m += (Having, mh);
                                break;
                            }
                        case _Matches:
                            {
                                var ma = (CTree<long, TypedValue>)v ?? CTree<long, TypedValue>.Empty;
                                var ms = (CTree<long, TypedValue>)m[_Matches] ?? CTree<long, TypedValue>.Empty;
                                var ng = im.matching + (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
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
                            if (!(im is From))
                                mm -= p;
                            else if (im.where != CTree<long, bool>.Empty)
                            {
                                var w = im.where;
                                var d = (Domain)cx.obs[(long)v];
                                for (var b = w.First(); b != null; b = b.Next())
                                {
                                    var s = b.key();
                                    var sv = (SqlValue)cx.obs[s];
                                    if (!sv.KnownBy(cx, d.representation))
                                        w -= s;
                                }
                                if (w != im.where)
                                    m += (_Where,w);
                            }
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
                                if (!(finder.Contains(ua.vbl) && im.finder[ua.vbl] is Finder fi)
                                    || !((SqlValue)cx.obs[ua.val]).KnownBy(cx, im))
                                    sg -= b.key();
                                for (var tb = ((CTree<long, long>)m[RSTargets]).First(); tb != null; tb = tb.Next())
                                {
                                    var ts = (RowSet)cx.obs[tb.value()];
                                    var td = cx._Dom(ts);
                                    if (ts.assig.Contains(ua))
                                        sg -= ua;
                                    if (td.representation.Contains(ua.vbl))
                                    {
                                        sg -= ua;
                                        cx.Add(ts + (Assig, ts.assig + (ua, true)));
                                        if (im.defpos == ts.defpos)
                                        {
                                            im = (RowSet)cx.obs[im.defpos];
                                            m += (Assig, im.assig);
                                        }
                                    }
                                }
                            }
                            if (sg == CTree<UpdateAssignment, bool>.Empty)
                                mm -= Assig;
                            else
                                mm += (Assig, sg);
                            break;
                        }
                    case _Finder:
                        {
                            var fi = (CTree<long, Finder>)v;
                            var of = (CTree<long, Finder>)m[_Finder] ?? CTree<long, Finder>.Empty;
                            m += (p, of + fi);
                            if (!(im is From))
                                mm -= _Finder;
                            break;
                        }
                    case UsingOperands:
                        {
                            var nr = (CTree<long, long>)v;
                            var rf = (CTree<long, Finder>)m[_Finder];
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
                                    rf += (bp, new Finder(bp, b.value()));
                                    ch = true;
                                }
                            }
                            if (ch)
                            {
                                cx.Add(rd);
                                m = m + (_Domain, rd.defpos) + (_Finder, rf);
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
            if (mm != BTree<long, object>.Empty && mm.First().key() < 0)
                // Recursively apply any remaining changes to sources
                // On return mm may contain pipeline changes
                for (var b = SourceProps.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (m.Contains(p))
                    {
                        var sp = (long)m[p];
                        var sc = (RowSet)cx.obs[sp];
                        sc = sc.MaybeApply(mm, cx, sc);
                        if (sc.defpos!=sp) 
                            m += (p, sc.defpos);
                    }
                }
             if (recompute)
                m = _Mem(cx, m);
            if (m != im.mem)
            {
                im = (RowSet)im.New(m);
                cx.Add(im);
            }
            cx.done = od;
            return im;
        }
        /// <summary>
        /// Implemented for SelectRowSet and RestRowSet
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <param name="im"></param>
        /// <returns></returns>
        internal virtual RowSet ApplySR(BTree<long,object> mm,Context cx,RowSet im)
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
        internal void ApplyT(Context cx,RowSet sa, CTree<long,Domain> es)
        {
            for (var b = Sources(cx).First(); b != null; b = b.Next())
            {
                var s = (RowSet)cx.obs[b.key()];
                if (s is SelectRowSet || s is RestRowSet 
                    || (this is RestRowSetUsing ru && ru.usingTableRowSet==s.defpos)
                    || !s.AggSources(cx).Contains(sa.defpos))
                    continue;
                var rc = CList<long>.Empty;
                var rs = CTree<long, Domain>.Empty;
                var fi = CTree<long, Finder>.Empty;
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
                        fi += (p, new Finder(p, s.defpos));
                    }
                if (s is RestRowSetUsing)
                    for (var c = cx._Dom(sa).aggs.First(); c != null; c = c.Next())
                        if (!rs.Contains(c.key()))
                        {
                            var v = (SqlValue)cx.obs[c.key()];
                            rc += v.defpos;
                            rs += (v.defpos, cx._Dom(v));
                            fi += (v.defpos, new Finder(v.defpos, s.defpos));
                        }
                var sd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rc);
                cx.Add(sd);
                cx.Add(s + (_Domain, sd.defpos) + (_Finder, fi));
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
        // For matching columns, we trace back through source, maybe add more matching uids
        internal CTree<long, CTree<long, bool>> AddMatching(Context cx, CTree<long, CTree<long, bool>> ma)
        {
            for (var b=ma.First();b!=null;b=b.Next())
            {
                var c = b.key();
                for (var r=this;r!=null;r=(RowSet)cx.obs[r.source])
                {
                    var f = r.finder[c];
                    if (f.col!=c && f.col!=0L)
                    {
                        ma = Pair(c, f.col, ma);
                        ma = Pair(f.col, c, ma);
                        break;
                    }
                }
            }
            return ma;
        }

        private CTree<long, CTree<long, bool>> Pair(long c, long col, CTree<long, CTree<long, bool>> ma)
        {
            var m = ma[c]??CTree<long,bool>.Empty;
            return m.Contains(col) ? ma : ma + (c, m + (col, true));
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
            var ox = cx.finder;
            cx.finder += finder;
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var s = cx.obs[b.value()];
                var v = s?.Eval(cx) ?? TNull.Value;
                if (v.IsNull && sce[b.value()] is TypedValue tv
                    && !tv.IsNull)
                    v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
                vs += (b.value(), v);
            }
            cx.finder = ox;
            return new TRow(dm, vs);
        }
        internal virtual string DbName(Context cx)
        {
            return cx.db.name;
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm, Domain fd)
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
        internal virtual RowSet Relocate1(Context cx)
        {
            var rs = this;
            var ts = rs.rsTargets;
            for (var c = ts.First(); c != null; c = c.Next())
            {
                var or = c.value();
                var nr = cx.Fix(or);
                if (or != nr)
                    ts += (c.key(), nr);
            }
            var fi = rs.finder;
            for (var c = fi.First(); c != null; c = c.Next())
            {
                var f = c.value();
                var or = f.rowSet;
                var nr = cx.Fix(or);
                if (or != nr)
                    fi += (c.key(), new Finder(f.col, nr));
            }
            return rs + (_Finder, fi) + (RSTargets, ts);
        }
        internal RowSet AddUpdateAssignment(Context cx, UpdateAssignment ua)
        {
            var s = (long)(mem[_Source] ?? -1L);
            if (cx.obs[s] is RowSet rs && rs.finder.Contains(ua.vbl)
                && (ua.val == -1L || ((SqlValue)cx.obs[ua.val]).KnownBy(cx, rs)))
                rs.AddUpdateAssignment(cx, ua);
            var r = this + (Assig, assig + (ua, true));
            cx.Add(r);
            return r;
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Insert(cx, ts, iter, rt);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Insert(cx, ts, false, rt);
            return r;
        }
        internal override BTree<long, TargetActivation>Update(Context cx, RowSet fm, bool iter)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Update(cx, fm, iter);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Update(cx, fm, false);
            return r;
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm, bool iter)
        {
            if (rsTargets == CTree<long, long>.Empty)
                throw new DBException("42174");
            var r = base.Delete(cx, fm, iter);
            for (var b = rsTargets.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Delete(cx, fm, false);
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
            return (r.defpos == defpos)? r:r.MaybeBuild(cx);
        }
        internal virtual RowSet ComputeNeeds(Context cx, Domain xp = null)
        {
            if (needed != null)// || source <0)
                return this;
            var rd = _Rdc(cx);
            var ln = (CTree<long, Finder>.Empty,rd);
            ln = cx.Needs(ln, this, xp);
            ln = cx.Needs(ln, this, keys);
            ln = cx.Needs(ln, this, rowOrder);
            ln = AllWheres(cx, ln);
            ln = AllMatches(cx, ln);
            var (nd, rc) = ln;
            var r = this + (_Needed,nd)+(InstanceRowSet.RdCols, rc)+
                (_Finder,finder+nd);
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
        /// <summary>
        /// Deal with RowSet prequisites (a kind of implementation of LATERAL).
        /// We can't calculate these earlier since the source rowsets haven't been constructed.
        /// Cursors will contain corresponding values of prerequisites.
        /// Once First()/Next() has been called, we need to see if they
        /// have been evaluated/changed. Either we can't or we must build this RowSet.
        /// </summary>
        /// <returns></returns>
        internal RowSet MaybeBuild(Context cx)
        {
            RowSet r = this;
            do
                r = r.ComputeNeeds(cx);
            while (r.needed == null);
            // We cannot Build if Needs not met by now.
            var ox = cx.finder;
            var bd = true;
            for (var b = r.needed?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                cx.finder += (p, b.value()); // enable build
                if (cx.obs[p].Eval(cx) == null)
                    bd = false;
            }
            cx.finder = ox;
            if (bd) // all prerequisites are ok
                r = r.Build(cx);  // we can Build (will set built to true)
            return r;
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
        protected RowSet Fixup(Context cx, RowSet now)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = finder.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var f = b.value();
                if (f.rowSet == defpos)
                    fi += (p, new Finder(f.col, now.defpos));
                else
                    fi += (p, f);
            }
            now += (_Finder, fi);
            cx.Add(now);
            for (var b = cx.obs.First(); b != null; b = b.Next())
                if (b.value() == this)
                    cx.obs += (b.key(), now);
            return now;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RowSet)base._Fix(cx);
            var gs = cx.Fix(groupSpec);
            if (gs != groupSpec)
                r += (Group, gs);
            var ng = cx.Fix(groupings);
            if (ng != groupings)
                r += (Groupings, ng);
            var nf = cx.Fix(finder);
            if (nf.CompareTo(finder) != 0)
                r += (_Finder, nf);
            var nk = cx.Fix(keys);
            if (nk != keys)
                r += (Index.Keys, nk);
            var fl = cx.Fix(filter);
            if (filter != fl)
                r += (Filter, fl);
            var no = cx.Fix(rowOrder);
            if (no != rowOrder)
                r += (RowOrder, no);
            var nw = cx.Fix(where);
            if (nw != where)
                r += (_Where, nw);
            var nm = cx.Fix(matches);
            if (nm != matches)
                r += (_Matches, nm);
            var mg = cx.Fix(matching);
            if (mg != matching)
                r += (Matching, mg);
            var ts = cx.FixV(rsTargets);
            if (ts!=rsTargets)
                r += (RSTargets, ts);
            var tg = cx.Fix(target);
            if (tg != target)
                r += (From.Target, tg);
            var ag = cx.Fix(assig);
            if (ag != assig)
                r += (Assig, ag);
            var s = (long)(mem[_Source] ?? -1L);
            var ns = cx.Fix(s);
            if (ns != s)
                r += (_Source, ns);
            if (tree?.info != null)
            {
                var inf = tree.info.Fix(cx);
                var ks = CList<long>.Empty;
                for (var t = inf; t != null; t = t.tail)
                    ks += t.head;
                r = r + (Index.Tree, new MTree(inf)) + (Index.Keys,ks);
            }
            r += (Asserts, asserts);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            //        if (wr.rss.Contains(defpos)) If you uncomment: representation will sometimes be incorrect
            //            return wr.rss[defpos];
            var r = (RowSet)base._Relocate(cx);
            r += (_Finder, cx.Fix(finder));
            r += (Index.Keys, cx.Fix(keys));
            r += (RowOrder, cx.Fix(rowOrder));
            r += (Filter, cx.Fix(filter));
            r += (_Where, cx.Fix(where));
            r += (_Matches, cx.Fix(matches));
            r += (Group, cx.Fix(groupSpec));
            r += (Groupings, cx.Fix(groupings));
            r += (RSTargets, cx.FixV(rsTargets));
            r += (Assig, cx.Fix(assig));
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
            var de = 0;
            var rs = cx._Dom(this);
            if (rs.representation.Contains(so.defpos))
                de = _Max(de, sv.depth);
            var r = (RowSet)base._Replace(cx, so, sv);
            rs = (Domain)rs._Replace(cx, so, sv);
            if (rs.defpos != r.domain)
                r += (_Domain, rs.defpos);
            var fi = cx.Replaced(r.finder);
            if (fi!=r.finder)
                r += (_Finder, fi);
            var fl = cx.Replace(r.filter,so,sv);
            if (fl!=r.filter)
                r += (Filter, fl);
            var ks = cx.Replaced(keys);
            if (ks!=keys)
                r += (Index.Keys, ks);
            var os = r.ordSpec;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var ow = (SqlValue)cx._Replace(b.value(), so, sv);
                if (b.value() != ow.defpos)
                    os += (b.key(), ow.defpos);
                de = Math.Max(de, ow.depth);
            }
            if (os != r.ordSpec)
                r += (OrdSpec, os);
            var ro = cx.Replaced(rowOrder);
            if (ro!=r.rowOrder)
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
                            ma = ma - so.defpos + (sv.defpos,true);
                    mg = mg - b.key() + (a, ma);
                }
            if (mg != r.matching)
                r += (Matching, mg);
            if (tree != null)
            {
                var tre = tree.Replaced(cx, so, sv);
                r = r + (Index.Tree,tre) + (Index.Keys,tre.Keys());
            }
            var ch = false;
            var ts = CTree<long, long>.Empty;
            for (var b = rsTargets.First(); b != null; b = b.Next())
            {
                var v = b.value();
                if (v == defpos)
                    continue;
                var p = cx.ObReplace(v, so, sv);
                var k = cx.done[b.key()]?.defpos ?? b.key();
                if (p != b.value() || k != b.key())
                    ch = true;
                ts += (k, p);
            }
            if (ch)
                r += (RSTargets, ts);
            if (mem.Contains(_Source))
            {
                var s = (long)mem[_Source];
                var ns = cx.ObReplace(s, so, sv);
                if (s!=ns)
                    r = (RowSet)r.New(cx,E+(_Source, ns));
            }
            if (cx.done.Contains(groupSpec))
            {
                var ng = cx.done[groupSpec].defpos;
                if (ng!=groupSpec)
                    r += (Group, ng);
                r += (Groupings, cx.Replaced(groupings));
                if (r.groupings.CompareTo(groupings)!=0)
                    r += (GroupCols,cx.GroupCols(r.groupings));
            }
            r += (_Depth, de + 1);
            cx.Add(r);
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            var dm = cx._Dom(this);
            var p = dm.rowType[i];
            if (cx.role.infos[p] is ObInfo ci)
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
        internal virtual bool Knows(Context cx, long rp)
        {
            var dm = cx._Dom(this);
            return finder.Contains(rp) || rp == defpos
        //        || dm.representation.Contains(rp)
        //        || (cx.obs[source] is RowSet rs && rs.Knows(cx, rp))
                || (cx.obs[rp] is SqlValue sv && sv.isConstant(cx));
        }
        internal virtual CList<long> Keys()
        {
            return null;
        }
        internal virtual (CTree<long, Finder>,CTree<long,bool>) AllWheres(Context cx, 
            (CTree<long, Finder>,CTree<long,bool>) ln)
        {
            var (nd,rc) = cx.Needs(ln, this, where);
            if (cx.obs[source] is RowSet sce)
            {
                var (ns,ss) = sce.AllWheres(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                    nd += (b.key(), b.value());
                rc += ss;
            }
            return (nd,rc);
        }
        internal virtual (CTree<long, Finder>,CTree<long,bool>) AllMatches(Context cx, 
            (CTree<long, Finder>,CTree<long,bool>) ln)
        {
            var (nd,rc) = cx.Needs(ln, this, matches);
            if (cx.obs[source] is RowSet sce)
            {
                var (ns, ss) = sce.AllMatches(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                        nd += (b.key(), b.value());
                rc += ss;
            }
            return (nd,rc);
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
            int m = dm.rowType.Length;
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
            var rs = MaybeBuild(cx);
            if (!rs.Built(cx))
            {
                var sv = (SqlValue)cx.obs[rs.needed.First().key()];
                throw new DBException("42112", sv.name);
            }
            return rs._First(cx);
        }
        public virtual Cursor Last(Context cx)
        {
            var rs = MaybeBuild(cx);
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
            var cm = "";
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
            if (PyrrhoStart.VerboseMode && finder != CTree<long, Finder>.Empty)
            {
                cm = "(";
                sb.Append(" Finder: ");
                for (var b = finder.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = "],";
                    sb.Append(Uid(b.key()));
                    var f = b.value();
                    sb.Append("="); sb.Append(Uid(f.rowSet));
                    sb.Append('['); sb.Append(Uid(f.col));
                }
                sb.Append("])");
            }
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
            TRow rw, BTree<long, object> m = null) : base(cx._Dom(rs), rw.values)
        {
            _rowsetpos = rs.defpos;
            _dom = cx._Dom(rs);
            _pos = pos;
            _ds = ds;
            display = _dom.display;
            cx.cursors += (rs.defpos, this);
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
            cx.funcs -= rs.defpos;
        }
        protected Cursor(Context cx, Cursor cu)
            : base((Domain)cu.dataType.Fix(cx), cx.Fix(cu.values))
        {
            _rowsetpos = cx.Fix(cu._rowsetpos);
            _dom = cu._dom;
            _pos = cu._pos;
            _ds = cx.Fix(cu._ds);
            display = cu.display;
            cx.cursors += (cu._rowsetpos, this);
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
            var dn = rs.DbName(cx);
            var rvv = Rvv.Empty;
            if (rs is TableRowSet ts)
            {
                if ((ts.where == CTree<long, bool>.Empty && ts.matches == CTree<long, TypedValue>.Empty)
                 || cx._Dom(ts).aggs != CTree<long, bool>.Empty)
                    rvv += (ts.target, (-1L, ((Table)cx.obs[ts.target]).lastData));
                else
                    rvv += (ts.target, cx.cursors[ts.defpos]);
            } else
            for (var b = rs.rsTargets.First(); b != null; b = b.Next())
            {
                var tg = (RowSet)cx.obs[b.value()];
                if ((tg.where == CTree<long, bool>.Empty && tg.matches == CTree<long, TypedValue>.Empty)
                    || cx._Dom(tg).aggs != CTree<long, bool>.Empty)
                    rvv += (tg.target, (-1L, ((Table)cx.obs[b.key()]).lastData));
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
                if (cx.obs[b.key()].Eval(cx) == TBool.False)
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
            var rs = (RowSet)cx.obs[_rowsetpos];
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
    /// <summary>
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
        internal const long
            Target = -153; // long (a table or view for simple IDU ops)
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
        /// <param name="a">An alias if any</param>
        /// <param name="cr">NonNull for SqlInsert, an explicit column list if not empty</param>
        public From(Ident ic, Context cx, DBObject ob, Domain q = null,
            Grant.Privilege pr = Grant.Privilege.Select, BList<Ident> cr = null,
            string a = null)
            : this(ic, cx, _Mem(ic, cx, ob, q, pr, cr, a)) { }
        From(Ident ic, Context cx, BTree<long, object> m)
            : base(cx, ic.iix.dp, m)
        {
            var ids = cx.defs[(alias ?? name, cx.sD)].Item2;
            var dm = cx._Dom(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                // update defs with the newly defined entries and their aliases
                var c = (SqlValue)cx.obs[b.value()];
                if (c.name != "")
                {
                    var cix = new Iix(ic.iix.lp,cx,c.defpos);
                    ids += (c.name, cix, Ident.Idents.Empty);
                    if ((!cx.defs.Contains(c.name)) ||
                        cx.defs[c.name][cix.sd].Item1 is Iix ix && ix.dp >= 0 &&
                        (ix.dp < Transaction.TransPos || ix.sd < ic.iix.sd))
                    {
                        cx.defs += (c.name, cix, Ident.Idents.Empty);
                        cx.iim += (cix.dp, cix);
                    }
          /*          else if (cx.defs[c.name].Contains(c.iix.sd)) cf test8:10
                    {
                        var x = cx.defs[c.name][c.iix.sd].Item1;
                        if (x.dp>=0 && x.dp!=c.defpos)
                            cx.defs += (c.name, new Iix(-1L, cx, -1L), Ident.Idents.Empty);
                    } */
                    if (c.alias != null)
                    {
                        ids += (c.alias, cix, Ident.Idents.Empty);
                        cx.defs += (c.alias, cix, Ident.Idents.Empty);
                    }
                }
            }
            cx.defs += (alias??name, ic.iix, ids);
            cx.iim += (ic.iix.dp, ic.iix);
        }
        public From(long dp, Context cx, SqlCall pc, Domain q, CList<long> cr = null)
            : base(dp,cx, _Mem(dp, cx, pc, q, cr))
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
        /// If q is non-null, it will be a TABLE domain with a set of columns (the display)
        /// and if there are no stars will know how many there are, and maybe their names or aliases.
        /// We probably won't know their Domains (but they may be literal values, simple names,
        /// expressions or function calls with a a constrained Union Domain result).
        /// Consider instead the set of non-star Ident operands N in these expressions. 
        /// At this stage there are three sorts of Ident here:
        ///  Identifiers that have occurred to the left of the current SELECT: may also be
        ///  only partially known but they will eventually be defined by the RowSet they are from.
        ///  All other Idents i in N will name ix.cx.lp as their From. There are three cases:
        ///  i is now the uid of an SqlCopy targeting a previous tablereference in the tableexpression.
        ///  i has form f.i where f is a forward reference (the name or alias of this or a future tablereference)
        ///  i is a simple name which may unambiguously match a column name in this tablereference
        ///  And all of these are in cx.defs already (ambiguous references already excluded).
        ///  
        /// For this tablereference ic, we will be constructing an Instance of ob whose rowType will
        /// contain uids in the above collection N, including all the f.u idents where f names ic or its alias a,
        /// and any unabiguous references to the names of ob; and heap uids for any unreferenced columns
        /// in case they are referenced later: these will be in the display if q contains a star.
        /// Where we have constructed this RowSet, all of these references will be to SqlCopy 
        /// targeting this tablereference and (for a base table) with a copyFrom field identifying the base column.
        /// 
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
        internal static BTree<long, object> _Mem(Ident ic,Context cx, DBObject ob, Domain q,
           Grant.Privilege pr, BList<Ident> cr,string a)
        {
            var vs = BList<SqlValue>.Empty; // the resolved select list for this instance in query rowType order
            var qn = CTree<long, bool>.Empty; // the set N of identifiers in q
            var dt = ob.Domains(cx, pr); // the domain of the referenced object ob
            var ma = BTree<string, DBObject>.Empty; // the set of referenceable columns in dt
            for (var b = dt.rowType.First(); b != null && b.key() < dt.display; b = b.Next())
            {
                var p = b.value();
                var tc = (DBObject)cx.db.objects[p] ?? cx.obs[p];
                var s = tc.ToString();
                var ci = (ObInfo)cx.role.infos[tc.defpos];
                var nm = ci?.name ?? ((SqlValue)tc).name;
                ma += (nm, tc);
                if (ci?.alias != null)
                    ma += (ci.alias, tc);
                if (tc is SqlCopy sc && sc.alias != null)
                    ma += (sc.alias, tc);
            }
            qn = q?.Needs(cx,-1L,qn);
            var tn = ic.ident; // the object name
            var n = a ?? tn;
            var _ix = ic.iix; // our eventual uid
            var fu = BTree<string, long>.Empty;
            // we begin by examining the f.u entries in cx.defs. If f matches n we will add to fu
            if (cx.defs.Contains(tn)) // care: our name may have occurred earlier (for a different instance)
            {
                var (ix,ids) = cx.defs[(tn, ic.iix.sd)];
                if (cx.obs[ix.dp] is ForwardReference)
                {
                    _ix = ix;
                    for (var b = ids.First(); b != null; b = b.Next())
                        if (b.value() != BTree<int, (Iix, Ident.Idents)>.Empty)
                            fu += (b.key(), b.value().Last().value().Item1.dp);
                }
            }
            cx.defs += (tn, _ix, Ident.Idents.Empty);
            cx.iim += (ic.iix.dp, ic.iix);
            if (a != null && cx.defs.Contains(a))
            {
                var (ix, ids) = cx.defs[(a, ic.iix.sd)];
                if (cx.obs[ix.dp] is ForwardReference)
                {
                    if (_ix.dp != ic.iix.dp)
                        throw new DBException("42104", a);
                    _ix = ix;
                    for (var b = ids.First(); b != null; b = b.Next())
                        if (b.value() != BTree<int, (Iix, Ident.Idents)>.Empty)
                            fu += (b.key(), b.value().Last().value().Item1.dp);
                }
            }
            if (a != null)
                cx.defs += (a, _ix, Ident.Idents.Empty);
            int d = dt.Length; // di is the Domain of the referenced object ob
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var tr = CTree<long,long>.Empty; // the mapping to physical positions
            var mp = CTree<long, bool>.Empty; // the set of referenced columns
            if (cr == null || cr==BList<Ident>.Empty)
            {
                // we want to add everything from dt that matches q.Needs with lexical uids
                if (q!= null)
                {
                    for (var b = qn.First(); b != null; b = b.Next())
                    {
                        var p = b.key();
                        if (cx.obs[p] is SqlValue uv 
                                && uv.Iim(cx).sd>=cx.sD
                                && (!dt.rowType.Has(p)) 
                                &&!(uv is SqlCopy))// && dt.rowType.Has(uc.copyFrom))))
                        {
                            if (uv is SqlStar)
                            {
                                vs += uv;
                                mp += (uv.defpos, true);
                                continue;
                            }
                            var tc = ma[uv.name];
                            if (tc == null || (tc is TableColumn && cx.obs[uv.from] is VirtualTable)) //??
                                continue;
                            SqlValue nv = null;
                            if (tc is SqlValue sc)
                                nv = (SqlValue)sc.Relocate(tc.defpos);
                            if (nv == null && (fu.Contains(uv.name) && fu[uv.name] == p
                                || cx.defs[(uv.name, _ix.sd)].Item1.dp >= 0))
                            {
                                if (mp.Contains(tc.defpos))
                                {
                                    cx.Replace(uv, (SqlValue)cx.obs[tr[tc.defpos]]);
                                    continue;
                                }
                                nv = new SqlCopy(uv.defpos, cx, uv.name, _ix.dp, tc.defpos);
                            }
                            if (nv == null)
                                continue;
                            nv += (_From, _ix.dp);
                            if (uv.alias != null)
                                nv += (_Alias, uv.alias);
                            cx.Replace(uv, nv); // update the context (but not q)
                            if (nv is SqlCopy su && cx.obs[su.copyFrom] is SqlCopy)
                            {
                                var mgu = mg[su.copyFrom] ?? CTree<long, bool>.Empty;
                                var mgp = mg[p] ?? CTree<long, bool>.Empty;
                                mg = mg + (uv.defpos, mgu + (p, true))
                                    + (p, mgp + (su.copyFrom, true));
                            }
                            vs += nv;
                            tr += (tc.defpos, nv.defpos);
                            mp += (tc.defpos, true);
                        }
                    }
                }
            }
            else
                for (var b = cr.First(); b != null; b = b.Next())
                {
                    var c = b.value();
                    var tc = cx.obs[cx.defs[c].dp]
                        ?? throw new DBException("42112", c.ident);
                    while (tc is SqlCopy sc)
                        tc = cx.obs[sc.copyFrom]??(DBObject)cx.db.objects[sc.copyFrom];
                    var sv = (tc is TableColumn) ?
                        new SqlCopy(c.iix.dp, cx, c.ident, ic.iix.dp, tc.defpos) :
                        (SqlValue)tc;
                    cx.Add(sv);
                    tr += (tc.defpos, sv.defpos);
                    vs += sv;
                    mp += (tc.defpos, true);
                }
            if (vs.Length != 0)
                d = vs.Length;
            if (ob is Table)
                for (var b = dt.rowType.First(); b != null && b.key() < dt.display; b = b.Next())
                {
                    var p = b.value();
                    var co = cx.obs[p] ?? (DBObject)cx.db.objects[p];
                    SqlCopy sc = null;
                    while (co is SqlCopy)
                    {
                        sc = co as SqlCopy;
                        co = cx.obs[sc.copyFrom] ?? (DBObject)cx.db.objects[sc.copyFrom];
                    }
                    p = co.defpos;
                    if (tr.Contains(p))
                    {
                        var aa = true;
                        for (var c = vs.First(); aa && c != null; c = c.Next())
                            aa = tr[p] != c.value().defpos;
                        if (aa)
                            vs += (SqlValue)cx.obs[tr[p]];
                        continue;
                    }
                    var ci = cx.Inf(p);
                    if (sc == null && cx.defs.Contains(ci.name))
                    {
                        var ix = cx.defs[(ci.name,cx.sD)].Item1;
                        var so = cx.obs[ix.dp];
                        if (so?.GetType().Name == "SqlValue" && ix.sd>=cx.sD)
                        {
                            sc = new SqlCopy(ix.dp, cx, ci.name, ic.iix.dp, co,
                                new BTree<long, object>(_Alias, so.alias));
                            cx.Add(sc);
                        }
                    }
                    if (sc == null || !(ob is RowSet))
                    {
                        sc = new SqlCopy(cx.GetUid(), cx, ci?.name ?? co.alias ?? co.name, ic.iix.dp, co);
                        cx.Add(sc);
                    }
                    vs += sc;
                    tr += (p, sc.defpos);
                }
            else
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    var sc = (SqlValue)cx.obs[p];
                    if (vs.Has(sc))
                        continue;
                    vs += sc;
                    tr += (p, sc.defpos);
                }
            var rt = CList<long>.Empty;
            if (ob.defpos<0) // system tables have columns in the wrong order!
                for (var b = tr.Last(); b != null; b = b.Previous())
                    rt += b.value();
            else
                for (var b = tr.First(); b != null; b = b.Next())
                    rt += b.value();
            var fd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs) + (Domain.Display, d);
            cx._Add(fd);
            var sd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs)+(Domain.RowType,rt);
            cx._Add(sd);
            var ts = ob.RowSets(ic, cx, sd, ic.iix.dp, fd);
            if (ts is SelectRowSet)
                return ts.mem + (Name, ic.ident) + (_Domain,fd.defpos);
            var fi = ts.finder;
            for (var b = sd.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (!fi.Contains(p))
                    fi += (p, new Finder(p, ic.iix.dp));
            }
            if (cr!=null) // SqlInsert: columns are now assignment compatible
            {
                var qb = fd.rowType.First();
                for (var b = sd.rowType.First(); b != null && qb != null; b = b.Next(), qb = qb.Next())
                    fi += (qb.value(), new Finder(b.value(), ic.iix.dp));
            }
            var op = ob.defpos;
            if (ob is RestRowSet rrs)
                op = ((RestView)cx.obs[rrs.restView]).viewPpos;
            var fa = (pr==Grant.Privilege.Select)? Assertions.None: 
                (cr == null) ? Assertions.AssignTarget : Assertions.ProvidesTarget;
            var r = BTree<long, object>.Empty + (Name, tn)
                   + (Target, ts.target) + (_Domain,fd.defpos)
                   + (_Finder, fi) 
                   + (_Source, ts.defpos) + (Matching, mg)
                   + (RSTargets, new CTree<long, long>(op, ts.defpos))
                   + (Asserts, fa)
                   + (_Depth,cx.Depth(fd,ts,ob));
            if (ts is TableRowSet tt)
            {
                if (tt.indexes != CTree<CList<long>, long>.Empty)
                    r += (Table.Indexes, tt.indexes);
                if (tt.skeys.Count>0)
                {
                    var ks = CList<long>.Empty;
                    for (var b = tt.skeys.First(); b != null; b = b.Next())
                        ks += tt.sIMap[b.value()];
                    r += (Index.Keys, ks);
                }
            }
            if (ts.keys != CList<long>.Empty)
                r += (Index.Keys, ts.keys);
            if (a!=null)
                r += (_Alias,a);
            return r;
        }
        static BTree<long, object> _Mem(long dp, Context cx, SqlCall ca, Domain q, CList<long> cr = null)
        {
            var pc = (CallStatement)cx.obs[ca.call];
            var proc = (Procedure)((Procedure)cx.db.objects[pc.procdefpos]).Instance(dp,cx);
            var prs = new ProcRowSet(dp, ca, cx);
            cx.Add(prs);
            var ma = CTree<string,long>.Empty;
            for (var b=cx._Dom(proc).rowType.First();b!=null;b=b.Next())
                ma += (((SqlValue)cx.obs[b.value()]).name,b.value());
            for (var b = q.rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue sv &&
                    sv.domain == Domain.Content.defpos && ma.Contains(sv.name))
                    cx.obs += (sv.defpos, new SqlCopy(sv.defpos, cx, sv.name, ca.defpos, ma[sv.name]));
            return BTree<long, object>.Empty
                + (Target, pc.procdefpos) + (_Depth, 1 + ca.depth)
                + (_Domain, proc.domain) + (Name, proc.name)
                + (_Source, prs.defpos)
                + (_Depth, cx.Depth(ca,proc,prs));
        }
        static BTree<long, object> _Mem(Context cx,RowSet rs, long dp, string a)
        {
            var r =  BTree<long, object>.Empty
                + (Target, rs.defpos) + (_Depth, 1 + rs.depth)
                + (_Domain, rs.domain) + (Name, a);
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
            if (r != this)
                r = (From)New(cx, r.mem);
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
            var trs = new TableRowSet(cx.GetUid(), cx, target, domain);
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
                var r = this + (_Source, ns.defpos) + (RowOrder, os) + (Distinct, dct)
                    + (_Finder, ns.finder);
                return (RowSet)cx.Add(r);
            }
            return base.Sort(cx, os, dct);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            if (mem.Contains(_Alias)) { sb.Append(" Alias "); sb.Append(alias); }
            if (mem.Contains(Target)) { sb.Append(" Target="); sb.Append(Uid(target)); }
            if (mem.Contains(Table.Indexes)) 
            {   
                var indexes = (CTree<CList<long>, long>)mem[Table.Indexes];
                if (indexes.Count > 0)
                {
                    sb.Append(" Indexes=[");
                    var cm = "";
                    for (var b = indexes.First(); b != null; b = b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        var cn = "(";
                        for (var c = b.key().First(); c != null; c = c.Next())
                        {
                            sb.Append(cn); cn = ",";
                            sb.Append(Uid(c.value()));
                        }
                        sb.Append(")"); sb.Append(Uid(b.value()));
                    }
                }
                sb.Append("]");
            }
            if (mem.Contains(Index.Keys) && keys.Count>0)
            {
                sb.Append(" Keys=(");
                var cm = "";
                for (var b=keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
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
                    cx.finder += sce.finder;
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
                cx.finder += sce.finder;
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
                cx.finder += ((RowSet)cx.obs[_trs.source]).finder;
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
                cx.finder += ((RowSet)cx.obs[_trs.source]).finder;
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
    }
    // shareable as of 26 April 2021
    internal class TrivialRowSet: RowSet
    {
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)mem[Singleton];
        internal TrivialRowSet(Context cx) 
            : this(cx.GetUid(),cx,TRow.Empty) { }
        internal TrivialRowSet(long dp, Context cx, TRow r, long rc=-1L, CTree<long,Finder>fi=null)
            : base(dp, cx, _Mem(dp,cx,r.dataType)+(Singleton,r))
        {
            cx.Add(this);
        }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,Domain dm)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                fi += (b.key(), new Finder(b.key(), dp));
            return BTree<long, object>.Empty + (_Domain, dm.defpos) + (_Finder, fi)
                + (_Depth,dm.depth+1);
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
                :base(cx.GetUid(),cx,r.mem+(_Domain,dm)+(_Source,r.defpos)
                     +(RSTargets,new CTree<long,long>(r.target,r.defpos))
                     +(_Depth,cx.Depth(cx._Dom(dm),r)))
        {
            cx.Add(this);
        } 
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
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
        internal override RowSet Apply(BTree<long,object> mm,Context cx,RowSet im)
        {
            var ags = im.assig;
            if (mm[Assig] is CTree<UpdateAssignment,bool> sg)
            for (var b=sg.First();b!=null;b=b.Next())
            {
                var ua = b.key();
                if (!im.finder.Contains(ua.vbl))
                    sg -= b.key(); ;
                if ((!im.assig.Contains(ua)) && finder[ua.vbl].rowSet == defpos)
                    ags += (ua, true);
            }
            mm += (Assig, ags);
            return base.Apply(mm,cx,im);
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
                return (p == Rvv.RVV) ? p : rs.finder[p].col;
            }
            SelectedCursor(Context cx,SelectedCursor cu) :base(cx,cu)
            {
                _srs = (SelectedRowSet)cx.obs[cx.Fix(cu._rowsetpos)].Fix(cx);
                _bmk = (Cursor)cu._bmk?.Fix(cx);
            }
            internal static SelectedCursor New(Context cx,SelectedRowSet srs)
            {
                var ox = cx.finder;
                var sce = (RowSet)cx.obs[srs.source];
                cx.finder += sce.finder;
                for (var bmk = sce.First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal static SelectedCursor New(SelectedRowSet srs, Context cx)
            {
                var ox = cx.finder;
                var sce = (RowSet)cx.obs[srs.source];
                cx.finder += sce.finder;
                for (var bmk = sce.Last(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectedCursor(this, cx, p, v);
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
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
                cx.finder += _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, _srs, bmk, _pos + 1);
                    if (rb.Matches(cx))
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
                return new SelectedCursor(cx, this);
            }
        }
    }
    // shareable as of 26 April 2021
    internal class SelectRowSet : RowSet
    {
        internal TRow row => rows[0];
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec (select list defines dm)
        /// Query environment can supply values for the select list but source columns
        /// should bind more closely.
        /// </summary>
        internal SelectRowSet(Iix lp, Context cx, Domain dm, RowSet r,BTree<long,object> m=null)
            : base(lp.dp, _Mem(cx,lp,dm,r,m)) // don't pass cx to base: not dp, cx, _Mem..
        {
            cx.Add(this);
        }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
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
            var fi = r.finder;
            if (m[_Finder] is CTree<long, Finder> mf)
                fi += mf;
            for (var b = dm.Needs(cx,r.defpos).First(); b != null; b = b.Next())
                if (!fi.Contains(b.key()))
                {
                    var ku = b.key();
                    if (cx.obs[ku].name is string su && cx.defs.Contains(su))
                    {
                        var kf = cx.defs[su][cx.sD].Item1.dp;
                        fi += (ku, new Finder(kf, r.defpos));
                    }
                }
            var ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            if (ma.Count > 0 || r.matching.Count > 0)
                m += (Matching, r.CombineMatching(ma, r.matching));
            for (var b = r.rsTargets.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is TableRowSet tt)
                {
                    // see what indexes we can use
                    if (tt.indexes != CTree<CList<long>, long>.Empty)
                    {
                        var xs = CTree<CList<long>,long>.Empty;
                        for (var x = tt.indexes.First();x!=null;x=x.Next())
                        {
                            var k = x.key();
                            for (var c = k.First(); c != null; c = c.Next())
                                if (!fi.Contains(c.value()))
                                    goto next;
                            xs += (k, x.value());
                            next:;
                        }
                        m += (Table.Indexes, xs);
                    }
                    if (tt.skeys.Count > 0)
                    {
                        var ks = CList<long>.Empty;
                        for (var c = tt.skeys.First(); c != null; c = c.Next())
                            if (fi.Contains(c.value()))
                                ks += tt.sIMap[c.value()];
                        m += (Index.Keys, ks);
                    }
                }
            if (r.keys != CList<long>.Empty)
                m += (Index.Keys, r.keys);
            return m + (_Finder, fi)+(_Domain,dm.defpos)+(_Source,r.defpos) + (RSTargets,r.rsTargets)
                +(_Depth,cx.Depth(dm,r));
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
        internal override RowSet RowSets(Ident id,Context cx, Domain q, long fm, Domain fd)
        {
            return this;
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
        internal override RowSet Apply(BTree<long, object> mm, Context cx, RowSet im)
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
                am += (Domain.Aggs, cx._Dom(im).aggs + (CTree<long, bool>)mm[Domain.Aggs]);
            if (mm.Contains(Having))
                am += (Having, im.having + (CTree<long, bool>)mm[Having]);
            var pm = mm - Domain.Aggs - Group - Groupings - GroupCols - Having;
            var r = (SelectRowSet)base.Apply(pm, cx, im);
            if (am!=BTree<long,object>.Empty)
                r = (SelectRowSet)ApplySR(am + (AggDomain, im.domain), cx, r);
            if (cx._Dom(r).aggs.Count>0)
            {
                var vw = true;
                for (var b = cx._Dom(r).aggs.First(); b != null; b = b.Next())
                    if ((b.key() >= Transaction.TransPos && b.key() < Transaction.Executables)
                        || b.key() >= Transaction.HeapStart)
                        vw = false;
                if (!vw)
                {
                    // check for agged or grouped
                    var os = CTree<long, bool>.Empty;
                    for (var b = cx._Dom(r).rowType.First(); b != null; b = b.Next())
                        os += ((SqlValue)cx.obs[b.value()]).Operands(cx);
                    for (var b = r.having.First(); b != null; b = b.Next())
                        os += ((SqlValue)cx.obs[b.key()]).Operands(cx);
                    for (var b = os.First(); b != null; b = b.Next())
                    {
                        var v = (SqlValue)cx.obs[b.key()];
                        if (!v.AggedOrGrouped(cx, r))
                            throw new DBException("42170", v.alias ?? v.name);
                    }
                }
            }
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
        /// <param name="im">The current state of rowset S</param>
        /// <returns>The new state of rowset S.</returns>
        internal override RowSet ApplySR(BTree<long, object> mm, Context cx, RowSet im)
        {
            for (var b = mm.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return im;
            var ag = (CTree<long, bool>)mm[Domain.Aggs];
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = im.mem;
            if (mm.Contains(GroupCols))
            {
                m += (GroupCols, mm[GroupCols]);
                m += (Groupings, mm[Groupings]);
                m += (Group, mm[Group]);
                m += (Domain.Aggs, ag);
            }
            else  // if we get here we have a non-grouped aggregation to add
                m += (Domain.Aggs, ag);
            var nc = CList<long>.Empty; // start again with the aggregating rowType, follow any ordering given
            var dm = (Domain)cx.obs[(long)mm[AggDomain]];
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            var fi = CTree<long, Finder>.Empty;
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
                        fi += (k, new Finder(k, defpos));
                    }
                else if (gb.Contains(p))
                {
                    nc += p;
                    ns += (p, cx._Dom(v));
                    fi += (p, new Finder(p, defpos));
                }
            }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc) + (Domain.Aggs, ag);
            m = m + (_Domain, nd.defpos) + (_Finder, fi) + (Domain.Aggs, ag);
            cx.Add(nd);
            im = (RowSet)im.New(m);
            cx.Add(im);
            if (mm[Having] is CTree<long, bool> h)
            {
                var hh = CTree<long, bool>.Empty;
                for (var b = h.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    if (((SqlValue)cx.obs[k]).KnownBy(cx, im))
                        hh += (k, true);
                }
                if (hh != CTree<long, bool>.Empty)
                    m += (Having, hh);
                im = (RowSet)im.New(m);
                cx.Add(im);
            }
            // see if the aggs can be pushed down further
            for (var b = SplitAggs(mm, cx).First(); b != null; b = b.Next())
            {
                var a = (RowSet)cx.obs[b.key()];
                var na = a.ApplySR(b.value() + (AggDomain,nd.defpos), cx, a);
                if (na!=a)
                    im.ApplyT(cx, na, nd.representation);
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
   //         var cx = new Context(_cx);
            cx.groupCols += (dm, groupCols);
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
                    var rg = new Register(cx,TRow.Empty,countStar);
                    rg.count = v;
                    var cp = dm.rowType[0];
                    cx.funcs += (defpos,cx.funcs[defpos]??BTree<TRow,BTree<long,Register>>.Empty+
                        (TRow.Empty, (cx.funcs[defpos]?[TRow.Empty] ?? BTree<long, Register>.Empty)
                        + (cp, rg)));
                    var sg = new TRow(dm, new CTree<long, TypedValue>(cp, new TInt(v)));
                    cx.values += (defpos, sg);
                    return (RowSet)New(cx, E + (_Built, true) + (_Rows, new BList<TRow>(sg)));
                }
                cx.finder += sce.finder;
                cx.funcs += (defpos, BTree<TRow, BTree<long, Register>>.Empty);
                for (var rb = sce.First(cx); rb != null;
                    rb = rb.Next(cx))
                    if (!rb.IsNull)
                    {
                        cx.finder += sce.finder;
                        for (var b = matches.First(); b != null; b = b.Next()) // this is now redundant
                        {
                            var sc = cx.obs[b.key()] as SqlValue;
                            if (sc == null || sc.CompareTo(b.value()) != 0)
                                goto next;
                        }
                        for (var b = where.First(); b != null; b = b.Next()) // so is this
                            if (cx.obs[b.key()].Eval(cx) != TBool.True)
                                goto next;
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
                                        throw new DBException("22004", cx.obs[p].name);
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
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            return cx.obs[source].Insert(cx, ts, iter,rt);
        }
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm, bool iter)
        {
            return cx.obs[source].Delete(cx,fm, iter);
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm, bool iter)
        {
            return cx.obs[source].Update(cx,fm, iter);
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
                var ox = cx.finder;
                cx.finder = srs.finder;
                for (var bmk = ((RowSet)cx.obs[srs.source]).First(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                   var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal static SelectCursor New(SelectRowSet srs, Context cx)
            {
                var ox = cx.finder;
                cx.finder = srs.finder;
                for (var bmk = ((RowSet)cx.obs[srs.source]).Last(cx);
                      bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder = _srs.finder;
                for (var bmk = _bmk.Next(cx); 
                    bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,rb);
                    if (rb.Matches(cx))
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
                cx.finder = _srs.finder;
                for (
                    var bmk = _bmk.Previous(cx); 
                    bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = rb._dom.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx, rb);
                    if (rb.Matches(cx))
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
                var ox = cx.finder;
                cx.finder += grs.finder;
                var ebm = grs.rows?.First();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                cx.finder = ox;
                return r;
            }
            internal static GroupingBookmark New(SelectRowSet grs, Context cx)
            {
                var ox = cx.finder;
                cx.finder += grs.finder;
                var ebm = grs.rows?.Last();
                var r = (ebm == null) ? null : new GroupingBookmark(cx, grs, ebm, 0);
                cx.finder = ox;
                return r;
            }
            /// <summary>
            /// Move to the next grouped row
            /// </summary>
            /// <returns>whether there is a next row</returns>
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _grs.finder;
                var ebm = _ebm.Next();
                if (ebm == null)
                {
                    cx.finder = ox;
                    return null;
                }
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                cx.finder = ox;
                return r;
            }
            protected override Cursor _Previous(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _grs.finder;
                var ebm = _ebm.Previous();
                if (ebm == null)
                {
                    cx.finder = ox;
                    return null;
                }
                var dt = cx._Dom(_grs);
                var r = new GroupingBookmark(cx, _grs, ebm, _pos + 1);
                for (var b = dt.representation.First(); b != null; b = b.Next())
                    ((SqlValue)cx.obs[b.key()]).OnRow(cx, r);
                cx.finder = ox;
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
            ISMap = -213, // CTree<long,long> SqlValue,TableColumn
            SIMap = -214, // CTree<long,long> TableColumn,SqlValue
            RdCols = -462,  // CTree<long,bool> TableColumn (read columns)
            SRowType = -215; // CList<long>   TableColumn
        internal CTree<long, bool> rdCols =>
            (CTree<long, bool>)mem[RdCols] ?? CTree<long, bool>.Empty;
        internal CTree<long, long> iSMap =>
            (CTree<long, long>)mem[ISMap] ?? CTree<long, long>.Empty;
        internal CTree<long, long> sIMap =>
            (CTree<long, long>)mem[SIMap] ?? CTree<long, long>.Empty;
        internal CList<long> sRowType =>
            (CList<long>)mem[SRowType] ?? CList<long>.Empty;
        internal override Assertions Requires => Assertions.SimpleCols;
        protected InstanceRowSet(long dp, Context cx, BTree<long, object> m)
            : base(dp, cx, _Mem1(cx,m)) 
        { }
        protected InstanceRowSet(long dp, BTree<long, object> m)
            : base(dp, m) 
        { }
        static BTree<long,object> _Mem1(Context cx,BTree<long,object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            var ism = CTree<long, long>.Empty;
            var sim = CTree<long, long>.Empty;
            var dm = cx._Dom((long)(m[_Domain]??-1L));
            var sr = (CList<long>)m[SRowType];
            var fb = dm.rowType.First();
            for (var b = sr.First(); b != null && fb != null;
                b = b.Next(), fb = fb.Next())
            {
                var ip = b.value();
                var sp = fb.value();
                ism += (ip, sp);
                sim += (sp, ip);
            }
            if (cx.obs[(long)(m[From.Target]??-1L)] is Table tb)
                m += (Table.Indexes,tb.IIndexes(ism));
            m -= RSTargets;
            return m + (ISMap, ism) + (SIMap, sim);
        }
        public static InstanceRowSet operator +(InstanceRowSet rs, (long, object) x)
        {
            return (InstanceRowSet)rs.New(rs.mem + x);
        }
         internal override RowSet Apply(BTree<long, object> mm, Context cx,RowSet im)
        {
            if (mm == BTree<long, object>.Empty)
                return im;
            im = base.Apply(mm, cx,im); 
            var m = im.mem;
            if (mm[RdCols] is CTree<long,bool>rc)
            {
                var mr = (CTree<long, bool>)m[RdCols]??CTree<long,bool>.Empty;
                var dm = cx._Dom(im);
                var sIM = ((InstanceRowSet)im).sIMap;
                for (var b = rc.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    if (sIM.Contains(k) && dm.representation.Contains(sIM[k]))
                    {
                        rc -= k;
                        mr += (k, true);
                    }
                }
                mm += (RdCols, rc);
                m += (RdCols, mr);
                return (RowSet)im.New(m);
            }
            return im;
        }
        internal override RowSet Relocate1(Context cx)
        {
            var r = base.Relocate1(cx);
            var srt = CList<long>.Empty;
            for (var b=sRowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var np = cx.Fix(p);
                srt += np;
            }
            if (srt.CompareTo(sRowType) != 0)
                r += (SRowType, srt);
            var sim = CTree<long, long>.Empty;
            var ism = CTree<long, long>.Empty;
            for (var b = sIMap.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var f = b.value();
                var nk = cx.Fix(k);
                var nf = cx.Fix(f);
                sim += (nk, nf);
                ism += (nf, nk);
            }
            if (sim.CompareTo(sIMap) != 0)
                r = r + (SIMap, sim) + (ISMap, ism);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InstanceRowSet)base._Replace(cx, so, sv);
            r += (SRowType, cx.Replaced(sRowType));
            r += (SIMap, cx.Replaced(sIMap));
            r += (ISMap, cx.Replaced(iSMap));
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (InstanceRowSet)base._Fix(cx);
            var srt = cx.Fix(sRowType);
            if (srt != sRowType)
                r += (SRowType, srt);
            var sim = cx.Fix(sIMap);
            if (sim != sIMap)
                r += (SIMap, sim);
            var ism = cx.Fix(iSMap);
            if (ism != iSMap)
                r += (ISMap, ism);
            var rc = cx.Fix(rdCols);
            if (rc != rdCols)
                r += (RdCols, rc);
            return cx._Add(r);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (InstanceRowSet)base._Relocate(cx);
            r += (SRowType, cx.Fix(sRowType));
            r += (SIMap, cx.Fix(sIMap));
            r += (ISMap, cx.Fix(iSMap));
            return r;
        }
        internal override CTree<long, Cursor> SourceCursors(Context cx)
        {
            return CTree<long,Cursor>.Empty;
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" SRow:(");
            var cm = "";
            for (var b=sRowType.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" RdCols:("); cm = "";
                for (var b=rdCols.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                }
                sb.Append(")");
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
            _Index = -410, // long Index
            SKeys = -453; // CList<long> TableColumn
        public long index => (long)(mem[_Index] ?? -1L);
        public CTree<CList<long>, long> indexes =>
            (CTree<CList<long>, long>)mem[Table.Indexes] ?? CTree<CList<long>, long>.Empty;
        public CList<long> skeys => (CList<long>)mem[SKeys] ?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a rowset defined by a base table
        /// </summary>
        internal TableRowSet(long lp,Context cx, long t, long dm)
            : base(cx.GetUid(), cx, _Mem(cx.GetPrevUid(),lp,cx,t, dm) +(From.Target,t))
        { }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(long dp,long lp, Context cx, long t, long f)
        {
            var tb = (Table)(cx.obs[t] ?? cx.db.objects[t]);
            cx.Add(tb);
            var r = new BTree<long, object>(_Domain, f) + (From.Target, t)
                + (SRowType, tb.Domains(cx).rowType);
            var fi = CTree<long, Finder>.Empty;
            var dm = (Domain)(cx.obs[f]??cx.db.objects[f]);
            for (var b = dm.rowType.First();b!=null;b=b.Next())
                fi += (b.value(), new Finder(b.value(), dp));
            r = r + (_Finder, fi) + (Table.LastData, tb.lastData)
                + (_Depth,2);
            r += (Asserts, _Mem(cx, r));
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        /// <summary>
        /// TableRowSet does not have predefined dataType.display
        /// (because rowType is in declaration order, not select-list order)
        /// So the rdc is predefined instead, see Table.RowSets().
        /// It is still adjusted in ComputeNeeds for wheres etc
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <returns>The starting InstanceRowSet.RdCols for the tablerowset</returns>
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = rdCols;
            return (r==CTree<long,bool>.Empty)?r:base._Rdc(cx);
        }
        internal override (CTree<long, Finder>, CTree<long, bool>) AllWheres(Context cx,
(CTree<long, Finder>, CTree<long, bool>) ln)
        {
            var (nd, rc) = cx.Needs(ln, this, where);
            if (cx.obs[source] is RowSet sce)
            {
                var (ns, ss) = sce.AllWheres(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, b.value());
                    rc += (iSMap[u], true);
                }
                rc += ss;
            }
            return (nd, rc);
        }
        internal override (CTree<long, Finder>, CTree<long, bool>) AllMatches(Context cx,
            (CTree<long, Finder>, CTree<long, bool>) ln)
        {
            var (nd, rc) = cx.Needs(ln, this, matches);
            if (cx.obs[source] is RowSet sce)
            {
                var (ns, ss) = sce.AllMatches(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, b.value());
                    rc += (iSMap[u], true);
                }
                rc += ss;
            }
            return (nd, rc);
        }

        internal override RowSet Apply(BTree<long, object> mm, Context cx, RowSet im)
        {
            if (mm == BTree<long, object>.Empty)
                return im;
            var m = im.mem;
            if (mm[_Where] is CTree<long, bool> w)
            {
                for (var b = w.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    m += (_Where, im.where + (k, true));
                    var imm = im.matches;
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
            if (mm[_Matches] is CTree<long,TypedValue> ma)
            {
                var trs = (TableRowSet)im;
                var (index, nmt, match) = trs.BestForMatch(cx, ma);
                if (index == null)
                    index = trs.BestForOrdSpec(cx, im.ordSpec);
                if (index != null && index.rows != null)
                {
                    m += (Index.Tree, index.rows);
                    m += (_Index, index.defpos);
                    m += (SKeys, index.keys);
                    m += (_Matches, ma);
                    for (var b = trs.indexes.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        if (b.value() == index.defpos)
                            m += (Index.Keys, k);
                    }
                }
            }
            if (m != im.mem)
                im = (RowSet)cx.Add((RowSet)im.New(m));
            return base.Apply(mm, cx, im);
        }
        internal (Index, int, PRow) BestForMatch(Context cx, BTree<long, TypedValue> filter)
        {
            int matches = 0;
            PRow match = null;
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
            {
                var x = (Index)cx.db.objects[p.value()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != target)
                    continue;
                var dt = (ObInfo)cx.db.role.infos[x.defpos];
                int sc = 0;
                int nm = 0;
                PRow pr = null;
                var havematch = false;
                int sb = 1;
                var j = dt.dataType.Length - 1;
                for (var b = dt.dataType.rowType.Last(); b != null; b = b.Previous(), j--)
                {
                    var c = b.value();
                    for (var fd = filter.First(); fd != null; fd = fd.Next())
                    {
                        if (cx.obs[fd.key()] is SqlCopy co
                            && co.copyFrom == c)
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
            {
                var x = (Index)cx.db.objects[p.value()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != defpos)
                    continue;
                var dt = (ObInfo)cx.db.role.infos[x.defpos];
                int sc = 0;
                int n = 0;
                int sb = 1;
                var j = dt.dataType.Length - 1;
                for (var b = dt.dataType.rowType.Last(); b != null; b = b.Previous(), j--)
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
            if (rs.defpos < Transaction.TransPos)
                throw new PEException("PE402");
            return (TableRowSet)rs.New(rs.mem + x);
        }
        internal override int Cardinality(Context cx)
        {
            return(where==CTree<long,bool>.Empty && cx.obs[target] is Table tb)? 
                (int)tb.tableRows.Count
                :base.Cardinality(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableRowSet(dp,mem);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (TableRowSet)base._Fix(cx);
            r += (Table.Indexes, ((Table)cx.db.objects[r.target]).IIndexes(r.iSMap));
            cx.Add(r);
            return r;
        }
        internal override RowSet Relocate1(Context cx)
        {
            var r = (TableRowSet)base.Relocate1(cx);
            r += (Table.Indexes, ((Table)cx.db.objects[target]).IIndexes(r.iSMap));
            cx.Add(r);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableRowSet)base._Replace(cx, so, sv);
            r += (RSTargets, cx.Replaced(rsTargets));
            r += (Table.Indexes, ((Table)cx.db.objects[target]).IIndexes(r.iSMap));
            cx.Add(r);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (TableRowSet)base._Relocate(cx);
            r += (RSTargets, cx.Fix(rsTargets));
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
                {
                    for (var d = _cx.cursors.First(); v == null && d != null; d = d.Next())
                        if (d.value()[b.value()] is TypedValue tv && !tv.IsNull)
                            v = tv;
                    for (var c = matching[b.value()]?.First(); v == null && c != null; c = c.Next())
                        for (var d = _cx.cursors.First(); v == null && d != null; d = d.Next())
                            if (d.value()[c.key()] is TypedValue tv && !tv.IsNull)
                                v = tv;
                }
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
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, bool iter,
            CList<long> iC)
        {
            var ic = sRowType;
            if (iC != null)
            {
                ic = CList<long>.Empty;
                for (var b = iC.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (iSMap.Contains(p))
                        ic += p;
                }
            }
            return new BTree<long, TargetActivation>(target,
                new TableActivation(cx, this, ts, PTrigger.TrigType.Insert, ic));
        }
        /// <summary>
        /// Execute an Update operation on the Table, including triggers
        /// </summary>
        /// <param name="f">The Update statement</param>
        /// <param name="ur">The update row identifiers may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override BTree<long, TargetActivation>Update(Context cx, RowSet fm, bool iter)
        {
            return new BTree<long, TargetActivation>(target,
                new TableActivation(cx, this, fm, PTrigger.TrigType.Update));
        }
        /// <summary>
        /// Prepare a Delete on a Table, including triggers
        /// </summary>
        /// <param name="f">The Delete operation</param>
        /// <param name="ds">A set of delete strings may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override BTree<long, TargetActivation>Delete(Context cx, RowSet fm, bool iter)
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
                var ix = (Index)cx.obs[indexes[os]];
                if (ix.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                    ix.flags.HasFlag(PIndex.ConstraintType.Unique))
                    return ot;
                return (RowSet)cx.Add(new DistinctRowSet(cx, ot));
            }
            return base.Sort(cx, os, dct);
        }
        internal override void Show(StringBuilder sb)
        {
            base.Show(sb);
            sb.Append(" Target:"); sb.Append(Uid(target));
            if (indexes.Count>0)
            {
                var cm = "(";
                sb.Append(" Indexes: ["); 
                for (var b=indexes.First();b!=null;b=b.Next())
                {
                    var k = b.key();
                    sb.Append(cm); cm = ",(";
                    var cc = "";
                    for (var c=k.First();c!=null;c=c.Next())
                    {
                        sb.Append(cc); cc = ",";
                        sb.Append(Uid(c.value()));
                    }
                    sb.Append(")="); sb.Append(Uid(b.value()));
                }
                sb.Append("]");
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
                var vs = CTree<long, TypedValue>.Empty;
                var ws = CTree<long, TypedValue>.Empty;
                var dm = cx._Dom(trs);
                for (var b = dm.rowType.First();b!=null;b=b.Next())
                {
                    var p = b.value();
                    var v = rw.vals[trs.sIMap[p]];
                    ws += (p, v);
                }
                return new TRow(dm, ws);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs, PRow key = null)
            {
                var table = _cx.db.objects[trs.target] as Table;
                var ox = _cx.finder;
                _cx.finder = trs.finder;
                if (trs.keys!=CList<long>.Empty)
                {
                    var t = (trs.index >= 0) ? ((Index)_cx.db.objects[trs.index]).rows : null;
                    if (t==null && trs.indexes.Contains(trs.keys) 
                            && _cx.db.objects[trs.indexes[trs.keys]] is Index ix)
                        t = ix.rows;
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
                            _cx.finder = ox;
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
                        _cx.finder = ox;
                        return rb;
                    }
                }
                _cx.finder = ox;
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
                return new BList<TableRow>(_bmk.value());
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
            : base(cx.GetUid(),cx,_Mem(cx.GetPrevUid(),cx,r)+(_Source,r.defpos)
                  +(Table.LastData,r.lastData)+(_Where,r.where)
                  +(_Matches,r.matches) + (Index.Keys,r.keys)
                  +(_Depth,r.depth+1))
        {
            cx.Add(this);
        }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,RowSet r)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = cx._Dom(r).rowType.First(); b != null; b = b.Next())
                fi += (b.value(), new Finder(b.value(), dp));
            return BTree<long, object>.Empty + (_Domain, r.domain) + (_Finder, fi);
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
            var dm = cx._Dom(sce);
            var ks = (sce.keys.Count > 0) ? sce.keys : dm.rowType;
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
            return (RowSet)New(cx,E+(_Built,true)+(Index.Tree,mt)+(Index.Keys,sce.keys));
        }

        protected override Cursor _First(Context cx)
        {
            return DistinctCursor.New(cx,this);
        }
        protected override Cursor _Last(Context cx)
        {
            return DistinctCursor.New(this, cx);
        }
        internal override BTree<long, TargetActivation>Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation>Update(Context cx,RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, TargetActivation>Delete(Context cx,RowSet fm, bool iter)
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
                var ox = cx.finder;
                cx.finder += ((RowSet)cx.obs[drs.source]).finder;
                if (drs.tree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.tree.First(); bmk != null; bmk = bmk.Next()) 
                { 
                    var rb = new DistinctCursor(cx,drs,0, bmk);
                    if (rb.Matches(cx) && Eval(drs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal static DistinctCursor New(DistinctRowSet drs,Context cx)
            {
                var ox = cx.finder;
                cx.finder += ((RowSet)cx.obs[drs.source]).finder;
                if (drs.tree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.tree.Last(); bmk != null; bmk = bmk.Previous())
                {
                    var rb = new DistinctCursor(cx, drs, 0, bmk);
                    if (rb.Matches(cx) && Eval(drs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal override Cursor _Fix(Context cx)
            {
                return new DistinctCursor(cx, this);
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += ((RowSet)cx.obs[_drs.source]).finder;
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctCursor(cx, _drs,_pos + 1, bmk);
                    if (rb.Matches(cx) && Eval(_drs.where, cx))
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
                cx.finder += ((RowSet)cx.obs[_drs.source]).finder;
                for (var bmk = _bmk.Previous(); bmk != null; bmk = bmk.Previous())
                {
                    var rb = new DistinctCursor(cx, _drs, _pos + 1, bmk);
                    if (rb.Matches(cx) && Eval(_drs.where, cx))
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
                throw new NotImplementedException();
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
                 + (Table.LastData, r.lastData) + (Name, r.name)
                 + (Distinct, dct) + (_Depth, r.depth + 1))
        {
            cx.Add(this);
        }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(long dp, Context cx, RowSet r)
        {
            var fi = CTree<long, Finder>.Empty;
            var dm = cx._Dom(r);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                fi += (b.value(), new Finder(b.value(), dp));
            return BTree<long, object>.Empty + (_Domain, r.domain) + (_Finder, fi);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        public static OrderedRowSet operator +(OrderedRowSet rs, (long, object) x)
        {
            return (OrderedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, RowSet im)
        {
            var ms = (CTree<long,TypedValue>)mm[_Matches]??CTree<long, TypedValue>.Empty;
            for (var b = im.keys.First(); b != null; b = b.Next())
                if (ms.Contains(b.value()))
                {
                    var sc = (RowSet)cx.obs[im.source];
                    var nf = CTree<long, Finder>.Empty;
                    for (var c = finder.First(); c != null; c = c.Next())
                    {
                        var f = c.value();
                        var ns = (f.rowSet == defpos) ? source : f.rowSet;
                        nf += (b.key(), new Finder(f.col, ns));
                    }
                    return sc.Apply(mm + (_Finder, nf), cx, sc);
                }
            return base.Apply(mm, cx, im);
        }
        internal override bool CanSkip(Context cx)
        {
            var sc = (RowSet)cx.obs[source];
            while (sc is From fm)
                sc = (RowSet)cx.obs[fm.source];
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
            var ors = this;
            var fi = CTree<long, Finder>.Empty;
            for (var b = finder.First(); b != null; b = b.Next())
            {
                var f = b.value();
                var ns = (f.rowSet == source) ? ors.defpos : f.rowSet;
                fi += (b.key(), new Finder(f.col, ns));
            }
            return (RowSet)cx.Add((RowSet)New(mem + (OrdSpec, os)) + (_Finder, fi) + (RowOrder, os));
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
            var oc = cx.finder;
            cx.finder += sce.finder;
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
            cx.finder = oc;
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
                  +(_Needed,CTree<long,Finder>.Empty)
                  +(_Depth,cx.Depth(rs,xp)))
        {
            cx.Add(this);
        }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx, Domain dm, CList<long> rs)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = dm.Needs(cx).First(); b != null; b = b.Next())
                fi += (b.key(), new Finder(b.key(), dp));
            return BTree<long, object>.Empty + (_Domain, dm.defpos) + (_Finder, fi)
                + (Asserts, Assertions.AssignTarget);
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
            r += (SqlRows, cx.Replaced(sqlRows));
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlRowSet)base._Fix(cx);
            var nw = cx.Fix(sqlRows);
            if (nw!=sqlRows)
            r += (SqlRows, nw);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlRowSet)base._Relocate(cx);
            r += (SqlRows, cx.Fix(sqlRows));
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
                var dm = new Domain(cx.GetUid(), Sqlx.ROW, cx._Dom(rs).mem);
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
                throw new NotImplementedException();
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
            : base(dp, _Fin(dp,dt)+(ExplRows,r)+(_Domain,dt.defpos))
        {
            cx.Add(this);
        }
        protected ExplicitRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Fin(long dp,Domain dt)
        {
            var f = CTree<long, Finder>.Empty;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                f += (b.value(), new Finder(b.value(), dp));
            return new BTree<long, object>(_Finder, f);
        }
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
                var ox = _cx.finder;
                _cx.finder += ers.finder;
                for (var b=ers.explRows.First();b!=null;b=b.Next())
                {
                    var rb = new ExplicitCursor(_cx,ers, b, 0);
                    if (rb.Matches(_cx) && Eval(ers.where, _cx))
                    {
                        _cx.finder = ox;
                        return rb;
                    }
                }
                _cx.finder = ox;
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var ox = _cx.finder;
                _cx.finder += _ers.finder;
                for (var prb = _prb.Next(); prb!=null; prb=prb.Next())
                {
                    var rb = new ExplicitCursor(_cx,_ers, prb, _pos+1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                    {
                        _cx.finder = ox;
                        return rb;
                    }
                }
                _cx.finder = ox;
                return null;
            }
            protected override Cursor _Previous(Context _cx)
            {
                var ox = _cx.finder;
                _cx.finder += _ers.finder;
                for (var prb = _prb.Previous(); prb != null; prb = prb.Previous())
                {
                    var rb = new ExplicitCursor(_cx, _ers, prb, _pos + 1);
                    if (rb.Matches(_cx) && Eval(_ers.where, _cx))
                    {
                        _cx.finder = ox;
                        return rb;
                    }
                }
                _cx.finder = ox;
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
            :base(cx.GetUid(),cx,_Mem(cx.GetPrevUid(),dp,cx,ca) +(_Needed,CTree<long,Finder>.Empty)
            +(SqlCall.Call,ca)+(_Depth,ca.depth+1))
        {
            cx.Add(this);
        }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,long fp,Context cx,SqlCall ca)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = ((Domain)cx.obs[ca.domain]).Needs(cx).First(); b != null; b = b.Next())
                fi += (b.key(), new Finder(b.key(), dp));
            return BTree<long, object>.Empty + (_Domain, ca.domain) + (_Finder, fi);
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
        internal Domain dataType => (Domain)mem[ObInfo._DataType];
        internal CTree<long, bool> foreignKeys =>
            (CTree<long, bool>)mem[ForeignKeys] ?? CTree<long, bool>.Empty;
        internal CList<long> insertCols => (CList<long>)mem[Table.TableCols];
        internal long indexdefpos => (long)(mem[IxDefPos] ?? -1L);
        internal Adapters _eqs => (Adapters)mem[_Adapters];
        internal TransitionRowSet(TargetActivation cx, TableRowSet ts, RowSet data, CList<long> iC)
            : base(cx.GetUid(), cx,_Mem(cx.GetPrevUid(), cx, ts, data, iC)
                  +(_Where,ts.where)+(_Data,data.defpos)+(_Depth,cx.Depth(ts,data))
                  +(Matching,ts.matching))
        {
            cx.Add(this);
        }
        static BTree<long, object> _Mem(long defpos, TargetActivation cx, 
            TableRowSet ts,RowSet data,CList<long> iC)
        {
            var m = BTree<long, object>.Empty;
            var tr = cx.db;
            var ta = ts.target;
            m += (_Domain, ts.domain);
            m += (_Data, data.defpos);
            m += (ObInfo._DataType, (Domain)cx.obs[ts.domain]);
            m += (From.Target, ta);
            m += (RSTargets, new CTree<long,long>(ts.target,ts.defpos));
            var t = tr.objects[ta] as Table;
            m += (IxDefPos, t?.FindPrimaryIndex(tr)?.defpos ?? -1L);
            var ti = (ObInfo)tr.schema.infos[ta];
            // check now about conflict with generated columns
            if (t!=null && t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", ti.name);
            var dfs = BTree<long, TypedValue>.Empty;
            if (cx._tty != PTrigger.TrigType.Delete)
            {
                for (var b = ti.dataType.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    cx.finder += (p, new Finder(p, defpos));
                    if (tr.objects[p] is TableColumn tc) // i.e. not remote
                    {
                        var tv = tc.defaultValue ?? 
                            tc.Domains(cx).defaultValue;
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
            {
                var p = b.value();
                var ix = (Index)cx.db.objects[p];
                if (ix.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                    fk += (p, true);
            }
            m += (TargetTrans, ts.iSMap);
            m += (TransTarget, ts.sIMap);
            m += (ForeignKeys, fk);
            if (iC!=null)
                m += (Table.TableCols, iC);
            m += (Defaults, dfs);
            m += (_Needed, CTree<long, Finder>.Empty);
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
            var ds = cx.Replace(defaults, so, sv);
            if (ds != defaults)
                r += (Defaults, ds);
            var fk = cx.Replaced(foreignKeys);
            if (fk != foreignKeys)
                r += (ForeignKeys, fk);
            var gt = cx.Replaced(targetTrans);
            if (gt != targetTrans)
                r += (TargetTrans, gt);
            var tg = cx.Replaced(transTarget);
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
            internal TransitionCursor(TableActivation ta, TransitionRowSet trs, Cursor fbm, int pos)
                : base(ta.next, trs, pos, fbm._ds,
                      new TRow(trs, trs.dataType, fbm))
            {
                _trs = trs;
                _fbm = fbm;
                var cx = ta.next;
                for (var b = trs.finder.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var f = b.value();
                    if (cx.cursors.Contains(f.rowSet))
                        cx.values += (k, cx.cursors[f.rowSet][f.col]);
                }
                cx.values += (values, false);
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
                var ox = ta.finder;
                var sce = (RowSet)ta.obs[trs.data];  // annoying we can't cache this
                ta.finder += sce?.finder;
                for (var fbm = sce?.First(ta); fbm != null;
                    fbm = fbm.Next(ta))
                    if (fbm.Matches(ta) && Eval(trs.where, ta))
                    {
                        var r = new TransitionCursor(ta, trs, fbm, 0);
                        ta.finder = ox;
                        return r;
                    }
                ta.finder = ox;
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += ((RowSet)cx.obs[_fbm._rowsetpos]).finder;
                if (cx.obs[_trs.where.First()?.key() ?? -1L] is SqlValue sv && 
                    cx._Dom(sv).kind == Sqlx.CURRENT)
                    return null;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = new TransitionCursor((TableActivation)cx, _trs, fbm, _pos+1);
                    for (var b = _trs.where.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()].Eval(cx) != TBool.True)
                            goto skip;
                    cx.finder = ox;
                    return ret;
                skip:;
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException(); // never
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
            }
            internal override BList<TableRow> Rec()
            {
                return _fbm.Rec();
            }

        }
        // shareable as of 26 April 2021
        internal class TargetCursor : Cursor
        {
            internal readonly ObInfo _ti;
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fb;
            internal readonly TableRow _rec;
            TargetCursor(TableActivation ta, Cursor trc, Cursor fb, ObInfo ti, CTree<long,TypedValue> vals)
                : base(ta, ta._trs.defpos, ti.dataType, trc._pos, 
                      (fb!=null)?(fb._ds+trc._ds):trc._ds, 
                      new TRow(ti.dataType, vals))
            { 
                _trs = ta._trs; _ti = ti; _fb = fb;
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
                    return new TargetCursor(tc, tgc, tgc._fb, tgc._ti, tgc.values + (p, tv));
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
                var ro = (Role)cx.db.objects[t.definer];
                var ti = (ObInfo)ro.infos[t.defpos];
                var fb = (cx._tty!=PTrigger.TrigType.Insert)?cx.next.cursors[trs.rsTargets[cx._tgt]]:null;
                var rt = trc._trs.insertCols??ti.dataType.rowType;
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
                var oc = cx.finder;
                for (var b = t.tblCols.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is TableColumn tc)
                    {
                        if (check)
                            switch (tc.generated.gen)
                            {
                                case Generation.Expression:
                                    if (vs[tc.defpos] is TypedValue vt && !vt.IsNull)
                                        throw new DBException("0U000", cx.Inf(tc.defpos).name);
                                    cx.finder = oc;
                                    cx.values += tc.Frame(vs);
                                    var e = cx.obs[tc.generated.exp];
                                    var v = e.Eval(cx);
                                    trc += (cx, tc.defpos, v);
                                    vs += (tc.defpos, v);
                                    cx.finder = trs.finder;
                                    break;
                            }
                        var cv = vs[tc.defpos];
                        if ((!tc.defaultValue.IsNull) && (cv == null || cv.IsNull))
                            vs += (tc.defpos, tc.defaultValue);
                        cv = vs[tc.defpos];
                        if (tc.notNull && (cv == null || cv.IsNull))
                            throw new DBException("22206", cx.Inf(tc.defpos).name);
                        for (var cb = tc.constraints?.First(); cb != null; cb = cb.Next())
                        {
                            var cp = cb.key();
                            var ck = (Check)cx.db.objects[cp]; // not in framing!
                            cx.values += ck.Frame(vs);
                            var se = (SqlValue)cx.obs[ck.search];
                            if (se.Eval(cx) != TBool.True)
                                throw new DBException("22212", cx.Inf(tc.defpos).name);
                        }
                    }
                var r = new TargetCursor(cx, trc, fb,ti, vs);
                for (var b = trc._trs.foreignKeys.First(); b != null; b = b.Next())
                {
                    var ix = (Index)cx.db.objects[b.key()];
                    var rx = (Index)cx.db.objects[ix.refindexdefpos]; 
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
                            + cx.Inf(ix.tabledefpos).name + k.ToString());
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
                            if (tc.Domains(cx).kind != Sqlx.INTEGER)
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
                throw new NotImplementedException();
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException();
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
                  + (RSTargets, trs.rsTargets) + (From.Target,trs.target)
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
            var fi = CTree<long, Finder>.Empty;
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
                fi += (p, new Finder(q,trs.defpos));
            }
            return BTree<long,object>.Empty + (TransitionTable.Old,old)
                + (Trs,trs) + (Data,dat) + (_Map,ma)
                +(RMap,rm) + (_Finder,fi) + (_Domain, dm.defpos);
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
            var mp = cx.Replaced(map);
            if (mp != map)
                r += (_Map, mp);
            var rm = cx.Replaced(rmap);
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
            : base(_cx.GetUid(),s.mem+(Offset,o)+(Size,c)+(_Source,s.defpos)
                  +(RSTargets,s.rsTargets)
                  +(Table.LastData,s.lastData))
        {
            _cx.obs += (defpos, this);
        }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
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
                throw new NotImplementedException();
            }
            internal override Cursor _Fix(Context cx)
            {
                throw new NotImplementedException();
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
                throw new NotImplementedException();
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
                 +(_Depth,cx.Depth(sc,f)))
        {
            cx.Add(this);
        }
        protected WindowRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,RowSet sc)
        {
            var fi = CTree<long, Finder>.Empty;
            var sd = cx._Dom(sc);
            for (var b = sd.rowType.First(); b != null; b = b.Next())
                fi += (b.value(), new Finder(b.value(), dp));
            return BTree<long, object>.Empty + (_Finder, fi)
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
            var w = wf.window;
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var wd = cx._Dom(w);
            var kd = new Domain(cx.GetUid(),cx,Sqlx.ROW, wd.representation, w.order);
            var tree = new RTree(source,cx,kd,
                TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset((Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.MULTISET, cx._Dom(wf))));
            for (var rw = ((RowSet)cx.obs[source]).First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(kd, rw.values), cx.cursors);
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
                ws = (WindowRowSet)ws.MaybeBuild(cx);
                var bmk = ws?.rtree.First(cx);
                return (bmk == null) ? null 
                    : new WindowCursor(cx,ws.defpos,cx._Dom(ws), 0,bmk._key);
            }
            internal static Cursor New(WindowRowSet ws, Context cx)
            {
                ws = (WindowRowSet)ws.MaybeBuild(cx);
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
            ETag = -416, // string
            _RestView = -459,    // long RestView
            RestValue = -457,   // TArray
            SqlAgent = -458; // string
        internal TArray aVal => (TArray)mem[RestValue];
        internal long restView => (long)(mem[_RestView] ?? -1L);
        internal string defaultUrl => (string)mem[DefaultUrl];
        internal string etag => (string)mem[ETag] ?? "";
        internal string sqlAgent => (string)mem[SqlAgent] ?? "Pyrrho";
        internal CTree<long, string> namesMap =>
            (CTree<long, string>)mem[RestView.NamesMap] ?? CTree<long, string>.Empty;
        internal long usingTableRowSet => (long)(mem[RestView.UsingTableRowSet] ?? -1L);
        public RestRowSet(Iix lp, Context cx, RestView vw, Domain q)
            : base(lp.dp, _Mem(cx,lp,vw,q) +(_RestView,vw.defpos)
                  +(Asserts,Assertions.AssignTarget))
        {
            cx.Add(this);
            cx.restRowSets += (lp.dp, true);
        }
        protected RestRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, Iix lp, RestView vw, Domain q)
        {
            var vs = BList<SqlValue>.Empty; // the resolved select list in query rowType order
            var vd = CList<long>.Empty;
            var dt = cx._Dom(vw);
            var qn = q?.Needs(cx, -1L, CTree<long,bool>.Empty);
            cx.defs += (vw.name, lp, Ident.Idents.Empty);
            cx.iim += (lp.dp, lp);
            var d = dt.Length;
            var nm = CTree<long, string>.Empty;
            var mn = CTree<string, long>.Empty;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var ma = BTree<string, SqlValue>.Empty; // the set of referenceable columns in dt
            var fi = CTree<long, Finder>.Empty;
            for (var b = vw.framing.obs.First(); b != null; b = b.Next())
            {
                var c = b.value();
                if (c is SqlValue sv)
                {
                    ma += (c.name, sv);
                    mn += (c.name, c.defpos);
                }
            }
            for (var b = dt.rowType.First(); b != null && b.key() < dt.display; b = b.Next())
            {
                var p = b.value();
                var s = (SqlValue)cx.obs[p];
                var sf = (SqlValue)vw.framing.obs[mn[s.name]];
                if (!(sf is SqlCopy))
                {
                    fi += (p, new Finder(p, lp.dp));
                    vd += p;
                    nm += (p, s.name);
                }
            }
            var ur = (RowSet)cx.obs[vw.usingTableRowSet];
            Domain ud = (ur==null)?null:cx._Dom(ur);
            for (var b = dt.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var sc = (SqlValue)cx.obs[p];
                if (vs.Has(sc))
                    continue;
                if (q!=null &&  !qn.Contains(p))
                    continue;
                vs += sc;
                if (ud?.representation.Contains(p) == true)
                    fi += (p, new Finder(p, ur.defpos));
                else
                    fi += (p, new Finder(p, lp.dp));
            }
            d = vs.Length;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var sc = (SqlValue)cx.obs[p];
                if (vs.Has(sc))
                    continue;
                vs += sc;
                if (ud?.representation.Contains(p) == true)
                    fi += (p, new Finder(p, ur.defpos));
                else
                    fi += (p, new Finder(p, lp.dp));
            }
            var fd = (vs==BList<SqlValue>.Empty)?dt:new Domain(cx.GetUid(), cx, Sqlx.TABLE, vs,d);
            var r = BTree<long, object>.Empty + (Name, vw.name)
                   + (_Domain, fd.defpos) + (RestView.NamesMap,nm)
                   + (_Finder, fi) 
                   + (RestView.UsingTableRowSet,vw.usingTableRowSet)
                   + (Matching, mg) + (RestView.RemoteCols, vd)
                   + (RSTargets, new CTree<long, long>(vw.viewPpos, lp.dp))
                   + (_Depth, cx.Depth(fd, vw));
            if (vw.usingTableRowSet < 0)
            {
                var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
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
        internal override Domain Domains(Context cx, Grant.Privilege pr = Grant.Privilege.NoPrivilege)
        {
            var vw = (RestView)cx.obs[restView];
            return cx._Dom(vw);
        }
        internal override bool Knows(Context cx, long rp)
        {
            if (cx.obs[usingTableRowSet] is TableRowSet ur)
            {
                var ud = cx._Dom(ur);
                if (ud.representation.Contains(rp)) // really
                    return false;
            }
            return base.Knows(cx, rp);
        }
        internal override string DbName(Context cx)
        {
            var _vw = (RestView)cx.obs[restView];
            var vi = (ObInfo)cx.db.role.infos[_vw.viewPpos];
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
                r = r + (RestView.RemoteCols, rc) + (RestView.NamesMap,nm);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RestRowSet)base._Fix(cx);
            var rc = cx.Fix(remoteCols);
            if (rc != remoteCols)
                r += (RestView.RemoteCols, rc);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (RestView.NamesMap, nm);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RestRowSet)base._Relocate(cx);
            var rc = cx.Fix(remoteCols);
            if (rc != remoteCols)
                r += (RestView.RemoteCols, rc);
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
        internal (string, string, StringBuilder) GetUrl(Context cx, ObInfo vi,
            string url=null)
        {
            url = cx.url ?? url?? defaultUrl;
            var sql = new StringBuilder();
            string targetName = "";
            if (vi?.metadata.Contains(Sqlx.URL)==true)
            {
                sql.Append(url);
                for (var b = matches.First(); b != null; b = b.Next())
                {
                    var kn = ((SqlValue)cx.obs[b.key()]).name;
                    sql.Append("/"); sql.Append(kn);
                    sql.Append("="); sql.Append(b.value());
                }
                url = sql.ToString();
                sql.Clear();
            }
            else
            {
                var ss = url.Split('/');
                if (ss.Length > 5)
                    targetName = ss[5];
                var ub = new StringBuilder(ss[0]);
                for (var i = 1; i < ss.Length && i < 5; i++)
                {
                    ub.Append('/');
                    ub.Append(ss[i]);
                }
                url = ub.ToString();
            }
            return (url,targetName,sql);
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
        internal override RowSet Apply(BTree<long, object> mm, Context cx, RowSet im)
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
            var fi = finder;
            var rc = dt.rowType;
            var rs = dt.representation;
            var nm = namesMap;
            for (var b = ou.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var v = cx.obs[p];
                rc += p;
                rs += (p, cx._Dom(v));
                fi += (p, new Finder(p, defpos));
                nm += (p, v.alias??v.name);
            }
            if (fi!=finder || nm!=namesMap)
            {
                var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rc);
                cx.Add(nd);
                im = (RowSet)im.New(cx,im.mem+(_Domain, nd.defpos)+(_Finder,fi)
                    +(RestView.NamesMap,nm));
            }
            return base.Apply(mm, cx, im);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mm"></param>
        /// <param name="cx"></param>
        /// <param name="im"></param>
        /// <returns></returns>
        internal override RowSet ApplySR(BTree<long, object> mm, Context cx, RowSet im)
        {
            for (var b = mm.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (mem[k] == mm[k])
                    mm -= k;
            }
            if (mm == BTree<long, object>.Empty)
                return im;
            var ag = (CTree<long, bool>)mm[Domain.Aggs];
            var od = cx.done;
            cx.done = ObTree.Empty;
            var m = im.mem;
            if (mm[GroupCols] is Domain gd)
            {
                m += (GroupCols, gd);
                m += (Groupings, mm[Groupings]);
                m += (Group, mm[Group]);
                if (gd != im.groupCols)
                    im = im.Apply(m, cx, im);
            }
            else
                m += (Domain.Aggs, ag);
            var dm = (Domain)cx.obs[(long)mm[AggDomain]];
            var nc = CList<long>.Empty; // start again with the aggregating rowType, follow any ordering given
            var ns = CTree<long, Domain>.Empty;
            var gb = ((Domain)m[GroupCols])?.representation ?? CTree<long, Domain>.Empty;
            var kb = KnownBase(cx);
            var fi = CTree<long, Finder>.Empty;
            var ut = (TableRowSet)cx.obs[usingTableRowSet];
            Domain ud = (ut == null) ? null : cx._Dom(ut);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var v = (SqlValue)cx.obs[p];
                if (v is SqlFunction ct && ct.kind==Sqlx.COUNT && ct.mod==Sqlx.TIMES)
                {
                    nc += p;
                    ns += (p, Domain.Int);
                    fi += (p, new Finder(p, defpos));
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
                        fi += (k, new Finder(k, defpos));
                    }
                else if (gb.Contains(p))
                {
                    nc += p;
                    ns += (p, cx._Dom(v));
                    if (ud?.representation.Contains(p) == true)
                        fi += (p, new Finder(p, ut.defpos));
                    else
                        fi += (p, new Finder(p, defpos));
                }
            }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, ns, nc) + (Domain.Aggs, ag);
            m = m + (_Domain, nd.defpos) + (_Finder, fi) + (Domain.Aggs, ag);
            cx.Add(nd);
            im = (RowSet)im.New(m);
            cx.Add(im);
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
            var vi = (ObInfo)((vw!=null)? cx.db.role.infos[vw.viewPpos]:null);
            var (url,targetName,sql) = GetUrl(cx,vi,defaultUrl);
            var rq = GetRequest(cx,url, vi);
            rq.Accept = vw?.mime ?? "application/json";
            cx.finder += finder;
            if (vi?.metadata.Contains(Sqlx.URL) == true)
            {
                rq.Method = "GET";
            }
            else
            {
                rq.Method = "POST";
                sql.Append("select ");
                if (distinct)
                    sql.Append("distinct ");
                var co = "";
                var dd = cx._Dom(this);
                cx.groupCols += (dd, groupCols);
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
            HttpWebResponse wr = null;
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
                var lv = (cl != "") ? Level.Parse(cl) : Level.D;
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
                    if (a is TArray)
                        Console.WriteLine("--> " + ((TArray)a).list.Count + " rows");
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
                    r += (ETag, et);
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
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, bool iter,
            CList<long> rt)
        {
            ts +=(From.Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, ts, PTrigger.TrigType.Insert)
                : new RESTActivation(cx, ts, PTrigger.TrigType.Insert);
            //      if (iter)
            //         throw new NotImplementedException();
            return new BTree<long, TargetActivation>(ts.target, ta);
        }
        internal override BTree<long, TargetActivation> Update(Context cx,RowSet fm, bool iter)
        {
            fm += (From.Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, fm, PTrigger.TrigType.Update)
                : new RESTActivation(cx, fm, PTrigger.TrigType.Update);
            //     if (iter)
            //         throw new NotImplementedException();
            return new BTree<long, TargetActivation>(fm.target, ta);
        }
        internal override BTree<long, TargetActivation> Delete(Context cx,RowSet fm, bool iter)
        {
            fm += (From.Target, rsTargets.First().key());
            var vw = (RestView)cx.obs[restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
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
                var ox = cx.finder;
                cx.finder += rrs.finder;
                cx.obs += (rrs.defpos, rrs);
                if (rrs.aVal.Length!=0)

           //     for (var i = 0; i < rrs.aVal.Length; i++)
                {
                    var rb = new RestCursor(cx, rrs, 0, 0);
           //         if (rb.Matches(cx))
           //         {
                        cx.finder = ox;
                        return rb;
           //         }
                }
                cx.finder = ox;
                return null;
            }
            internal static RestCursor New(RestRowSet rrs, Context cx)
            {
                var ox = cx.finder;
                cx.finder += rrs.finder;
                cx.obs += (rrs.defpos, rrs);
          //      for (
                    var i = rrs.aVal.Length-1; 
          //          i>=0; i--)
          if (i>=0)
                {
                    var rb = new RestCursor(cx, rrs, 0, i);
            //        if (rb.Matches(cx))
            //        {
                        cx.finder = ox;
                        return rb;
            //        }
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new RestCursor(cx, this, p, v);
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _rrs.finder;
            //    for (
                    var i = _ix+1; 
                if (i < _rrs.aVal.Length)
            //        ; i++)
                {
                    var rb = new RestCursor(cx, _rrs, _pos+1, i);
            //        if (rb.Matches(cx))
            //        {
                        cx.finder = ox;
                        return rb;
            //        }
                }
                cx.finder = ox;
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                var ox = cx.finder;
                cx.finder += _rrs.finder;
                //    for (
                var i = _ix - 1; 
                if (i >=0)
            //        ; i--)
                {
                    var rb = new RestCursor(cx, _rrs, _pos + 1, i);
                    //        if (rb.Matches(cx))
                    //        {
                    cx.finder = ox;
                        return rb;
                    // }
                }
                cx.finder = ox;
                return null;
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
                 + (RestTemplate, rp) + (Name,vw.name)
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
            var fi = CTree<long, Finder>.Empty;
            for (var b = rd.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                fi += (p, new Finder(p,dp));
            }
            // identify the urlCol and usingCols
            var ab = ud.rowType.Last();
            var ul = ab.value();
            var uc = ud.rowType - ab.key();
            return r + (UsingCols,uc)+(UrlCol,ul) + (_Domain,rd.defpos)
                  +(_Finder,fi)+ (_Depth, cx.Depth(rd, uf, rr));
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
            var oc = cx.finder;
            cx.finder = ru.finder + ((RowSet)cx.obs[template]).finder;
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
                cx.finder = ru.finder + ((RowSet)cx.obs[template]).finder;
                cx.values -= uc.values;
            }
            cx.values += (defpos, a);
            cx.finder = oc;
            cx.url = null;
            return base.Build(cx);
        }
        internal override RowSet Apply(BTree<long,object>mm,Context cx,RowSet im)
        {
            if (mm == BTree<long, object>.Empty)
                return im;
            var ru = (RestRowSetUsing)base.Apply(mm,cx,im);
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
        /// <param name="iter"></param>
        /// <param name="rt"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Insert(Context cx, RowSet ts, bool iter, CList<long> rt)
        {
            return ((RowSet)cx.obs[template]).Insert(cx, ts, iter, rt);
        }
        /// <summary>
        /// It makes no sense to use the RestView to alter the contents of the using table.
        /// Lots of use cases suggest update of remote data will be useful.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="fm"></param>
        /// <param name="iter"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Update(Context cx, RowSet fm, bool iter)
        {
            return ((RowSet)cx.obs[template]).Update(cx, fm, iter);
        }
        /// <summary>
        /// It makes no sense to use the RestView to alter the contents of the using table.
        /// But the permission to delete remote rows may have been granted.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="fm"></param>
        /// <param name="iter"></param>
        /// <returns></returns>
        internal override BTree<long, TargetActivation> Delete(Context cx, RowSet fm, bool iter)
        {
            return ((RowSet)cx.obs[template]).Delete(cx, fm, iter);
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
                throw new NotImplementedException();
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
            var fi = CTree<long, Finder>.Empty;
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Pos", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Action", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "DefPos", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Transaction", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Timestamp", Domain.Timestamp);
            return r + (_Domain, new Domain(Sqlx.TABLE,cx,rt).defpos) + (_Finder, fi)
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
            var fi = CTree<long, Finder>.Empty;
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Pos", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Value", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "StartTransaction", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "StartTimestamp", Domain.Timestamp);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "EndTransaction", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "EndTimestamp", Domain.Timestamp);
            return BTree<long, object>.Empty 
                + (_Domain,new Domain(Sqlx.TABLE,cx,rt).defpos)
                + (LogRowsRowSet.TargetTable, tb.defpos) + (_Finder,fi)
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
                var tb = lrs.targetTable;
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