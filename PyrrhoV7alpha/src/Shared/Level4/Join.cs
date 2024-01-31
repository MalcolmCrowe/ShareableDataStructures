using Pyrrho.Common;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// A row set for a Join operation.
    /// OnCond is nearly the same as matches and
    /// JoinCond is nearly the same as where, 
    /// except for nullable behaviour for LEFT/RIGHT/FULL
    ///     /
    /// </summary>
	internal class JoinRowSet : RowSet
    {
        internal const long
            JFirst = -447, // long RowSet
            JSecond = -448, // long RowSet
            JoinCond = -203, // CTree<long,bool>
            JoinKind = -204, // Sqlx
            JoinUsing = -208, // BTree<long,long?> SqlValue SqlValue (right->left)
            Natural = -207, // Sqlx
            OnCond = -205; // BTree<long,long?> SqlValue SqlValue for simple equality
        /// <summary>
        /// NATURAL or USING or NO (the default)
        /// </summary>
        public Sqlx naturaljoin => (Sqlx)(mem[Natural] ?? Sqlx.NO);
        /// <summary>
        /// The tree of common TableColumns for natural join
        /// </summary>
        internal BTree<long, long?> joinUsing =>
            (BTree<long, long?>?)mem[JoinUsing] ?? BTree<long, long?>.Empty;
        /// <summary>
        /// the kind of Join
        /// </summary>
        public Sqlx joinKind => (Sqlx)(mem[JoinKind] ?? Sqlx.CROSS);
        /// <summary>
        /// During analysis, we collect requirements for the join conditions.
        /// </summary>
        internal CTree<long, bool> joinCond =>
            (CTree<long, bool>?)mem[JoinCond] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// Simple On-conditions: left=right
        /// </summary>
        internal BTree<long, long?> onCond =>
            (BTree<long, long?>?)mem[OnCond] ?? BTree<long, long?>.Empty;
        /// <summary>
        /// The two row sets being joined
        /// </summary>
		internal long first => (long)(mem[JFirst]??-1L);
        internal long second => (long)(mem[JSecond]??-1L);
        /// <summary>
        /// Constructor: build the rowset for the Join.
        /// Important: we have already identified the references and aliases in the select tree.
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(long dp,Context cx, RowSet lr, Sqlx k,RowSet rr,
            BTree<long,object>? m = null) :
            base(dp, cx, _Mem(cx,m,k,lr,(RowSet)cx.Add(rr+(_From,dp))))
        {
            cx.Add(this);
        }
        protected JoinRowSet(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx, BTree<long, object>? m,
            Sqlx k,RowSet lr, RowSet rr)
        {
            m ??= BTree<long, object>.Empty;
            m += (JoinKind, (k==Sqlx.COMMA)?Sqlx.CROSS:k);
            m += (JFirst, lr.defpos);
            m += (JSecond, rr.defpos);
            m += (ISMap, lr.iSMap + rr.iSMap);
            m += (SIMap, lr.sIMap + rr.sIMap);
            var oc = (BTree<long, long?>)(m[OnCond] ?? BTree<long, long?>.Empty);
            // Step 1: examine the names and find names in common
            var cm = CTree<string, (long, long)>.Empty; // common names
            var ns = CTree<long,string>.Empty; // all names
            var lc = CTree<long,bool>.Empty; // left columns to rename (true) or drop (false)
            var rc = CTree<long,bool>.Empty; // right columns to rename (true) or drop (false)
            for (var b = lr.names.First(); b != null; b = b.Next())
                if (b.value().Item2 is long lk)
                {
                    var n = b.key();
                    ns += (lk, n);
                    if (rr.names[b.key()].Item2 is long rk)
                    {
                        cm += (n, (lk, rk));
                        lc += (lk, true);
                        rc += (rk, true);
                    }
                }
            for (var b = rr.names.First(); b != null; b = b.Next())
                if (b.value().Item2 is long p)
                ns += (p, b.key());
            // Step 2: consider NATURAL and USING
            if (cm!=CTree<string,(long,long)>.Empty)
            {
                var nt = (Sqlx)(m[Natural] ?? Sqlx.NO);
                if (nt==Sqlx.NATURAL)
                {
                    for (var b = cm.First(); b != null; b = b.Next())
                    {
                        oc += b.value();
                        var (lk, rk) = b.value();
                        lc += (lk,true);
                        rc += (rk,false);
                    }
                    cm = CTree<string, (long, long)>.Empty;
                }
                if (nt==Sqlx.USING)
                {
                    var ju = (BTree<long, long?>)(m[JoinUsing]??BTree<long,long?>.Empty);
                    for (var b = ju.First(); b != null; b = b.Next())
                        if (b.value() is long lk)
                        {
                            var rk = b.key();
                            oc += (lk, rk);
                            lc += (lk, true);
                            rc += (rk, false);
                            cm -= ns[lk] ?? "";
                        }
                }
            }
            // Step 3: rename the columns remaining in lc, rc and construct the join's domain
            var ls = BList<string>.Empty;
            var rs = BList<string>.Empty;
            var lm = BTree<string, SqlValue>.Empty;
            var nn = BTree<string, (int,long?)>.Empty;
            var cs = BList<long?>.Empty;
            var fs = BList<long?>.Empty;
            var re = CTree<long, Domain>.Empty;
            for (var b = lr.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                {
                    var n = sv.name ?? ("Col" + cs.Length);
                    lm += (n, sv);
                    re += (sv.defpos, sv.domain);
                    if (((!lc.Contains(sv.defpos)) || lc[sv.defpos]) && !cm.Contains(n))
                    {
                        ls += n;
                        nn += (n, (b.key(),sv.defpos));
                        cs += sv.defpos;
                    }
                    else if (cm.Contains(n))
                    {
                        var ln = (lr.alias ?? lr.name) + "." + n;
                        var nl = sv + (_Alias, ln);
                        cx.Replace(sv, nl);
                        ls += ln;
                        nn += (ln, (b.key(), sv.defpos));
                        cs += sv.defpos;
                    }
                    else
                        fs += sv.defpos;
                }
            for (var b = rr.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue rv)
                {
                    var n = rv.name ?? ("Col" + cs.Length);
                    lm += (n, rv);
                    re += (rv.defpos, rv.domain);
                    if (((!rc.Contains(rv.defpos)) || rc[rv.defpos]) && !cm.Contains(n))
                    {
                        rs += n;
                        nn += (n, (b.key(), rv.defpos));
                        cs += rv.defpos;
                    }
                    else if (cm.Contains(n))
                    {
                        var rn = (rr.alias ?? rr.name) + "." + n;
                        var nr = rv + (_Alias, rn);
                        cx.Replace(rv, nr);
                        rs += rn;
                        nn += (rn, (b.key(), rv.defpos));
                        cs += rv.defpos;
                    }
                    else
                        fs += rv.defpos;
                }
            var ds = cs.Length;
            // add the dropped columns after the display in case they are referenced in where conditions
            for (var b = fs.First(); b != null; b = b.Next())
                cs += b.value();
            m += (ObInfo.Names, nn);
            m += (RowType, cs);
            m += (Representation, re);
            m += (Display, ds);
            // Step 4: construct orderings based on the onCondition (as modified above).
            // note that there may be further optimisations once the TableExpression is complete
            if (oc == BTree<long, long?>.Empty)
            {
                if (m[JoinKind] is Sqlx.INNER)
                    m += (JoinKind, Sqlx.CROSS); // will be reviewed later
            }
            else
            {
                var ma = (CTree<long, CTree<long, bool>>)(m[Matching] ?? CTree<long, CTree<long, bool>>.Empty);
                var lo = Row; // left ordering
                var ro = Row; // right
                for (var b = oc.First(); b != null; b = b.Next())
                    if (b.value() is long ri && re[ri] is Domain nr && re[b.key()] is Domain nl)
                    {
                        var le = b.key();
                        lo = (Domain)lo.New(lo.mem + (RowType, lo.rowType + le)
                            + (Representation, lo.representation + (le, nl)));
                        ro = (Domain)ro.New(ro.mem + (RowType, ro.rowType + ri)
                            + (Representation, ro.representation + (ri, nr)));
                        var ml = ma[le] ?? CTree<long, bool>.Empty;
                        var mr = ma[ri] ?? CTree<long, bool>.Empty;
                        ma = ma + (le, ml + (ri, true))
                            + (ri, mr + (le, true));
                        var mm = Math.Min(le, ri);
                        if (cx.NameFor(le) is string li && cx.NameFor(ri) == li &&
                            cx.defs[li] is BTree<int, (Iix, Ident.Idents)> td)
                        {
                            var (ix, ids) = td[cx.sD];
                            Iix nx = new (ix.lp, ix.sd, mm);
                            cx.defs = new Ident.Idents(cx.defs + (li, td + (cx.sD, (nx, ids ?? Ident.Idents.Empty))));
                        }
                    }
                lr = lr.Sort(cx, lo, false);
                rr = rr.Sort(cx, ro, false);
                m += (JFirst, lr.defpos);
                m += (JSecond, rr.defpos);
                m += (Matching, ma);
                m += (OnCond, oc);
            }
            m += (RSTargets, lr.rsTargets + rr.rsTargets);
            return cx.DoDepth(m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinRowSet(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new JoinRowSet(dp, m);
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            return rp == first || rp == second || base.Knows(cx, rp, ambient);
        }
        internal int Compare(Context cx)
        {
            int c;
            var ls = (RowSet?)cx.obs[first];
            for (var b = joinCond.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValueExpr se && cx.obs[se.left] is SqlValue lv &&
                        cx.obs[se.right] is SqlValue rv)
                {
                    c = lv.Eval(cx).CompareTo(rv.Eval(cx));
                    if (c != 0)
                        return (ls?.representation.Contains(lv.defpos) == true) ? c : -c;
                }
            for (var b = onCond.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue lv && b.value() is long bv 
                    && cx.obs[bv] is SqlValue rv)
                {
                    c = lv.Eval(cx).CompareTo(rv.Eval(cx));
                    if (c != 0)
                        return (ls?.representation.Contains(lv.defpos) == true) ? c : -c;
                }
            return 0;
        }
        public static JoinRowSet operator +(JoinRowSet et, (long, object) x)
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
            return (JoinRowSet)et.New(m + x);
        }
        public static JoinRowSet operator +(JoinRowSet rs, (Context, long, object) x)
        {
            var d = rs.depth;
            var m = rs.mem;
            var (cx, p, o) = x;
            if (rs.mem[p] == o)
                return rs;
            switch(p)
            {
                case JoinCond: m += (_Depth, cx._DepthTVX((CTree<long, bool>)o, d)); break;
                case JoinUsing:
                case OnCond:m += (_Depth, cx._DepthTVV((BTree<long, long?>)o, d)); break;
                default: m = rs._Depths(cx, m, d, p, o);break;
            }
            return (JoinRowSet)rs.New(m + (p, o));
        }
        internal override int Cardinality(Context cx)
        {
            if (where == CTree<long, bool>.Empty && kind == Sqlx.CROSS && cx.obs[first] is RowSet fi
                && cx.obs[second] is RowSet se)
                return fi.Cardinality(cx) * se.Cardinality(cx);
            var r = 0;
            for (var b = First(cx); b != null; b = b.Next(cx))
                r++;
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.FixTlb(joinCond);
            if (nc != joinCond)
                r += (JoinCond, nc);
            var nf = cx.Fix(first);
            if (nf != first)
                r += (JFirst, nf);
            var ns = cx.Fix(second);
            if (ns != second)
                r += (JSecond, ns);
            var oc = cx.FixTll(onCond);
            if (oc != onCond)
                r += (OnCond, oc);
            return r;
        }
        internal override CTree<long,bool> Sources(Context cx)
        {
            return new CTree<long,bool>(first,true) + (second,true);
        }
        internal override BTree<long, RowSet> AggSources(Context cx)
        {
            var f = first;
            var s = second;
            var rf = (RowSet?)cx.obs[f]??throw new PEException("PE1500");
            var rs = (RowSet?)cx.obs[s] ?? throw new PEException("PE1500");
            return new BTree<long, RowSet>(f, rf) + (s, rs);
        }
        internal override CTree<long, Cursor> SourceCursors(Context cx)
        {
            var pf = first;
            var ps = second;
            var rf = (RowSet?)cx.obs[pf] ?? throw new PEException("PE1500");
            var rs = (RowSet?)cx.obs[ps] ?? throw new PEException("PE1500");
            var r =  rf.SourceCursors(cx) +rs.SourceCursors(cx);
            if (cx.cursors[pf] is Cursor cf)
                r += (pf, cf);
            if (cx.cursors[ps] is Cursor cr)
                r += (ps, cr);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (JoinRowSet)base._Replace(cx, so, sv);
            var jc = joinCond;
            for (var b = jc.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    jc += (b.key(), true);
            }
            if (jc != joinCond)
                r +=(cx, JoinCond, jc);
            r += (JFirst, cx.ObReplace(first, so, sv));
            r += (JSecond, cx.ObReplace(second, so, sv));
            if (so.defpos != sv.defpos)
            {
                var oc = BTree<long,long?>.Empty;
                var ch = false;
                for (var b = onCond.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    var nk = (k == so.defpos) ? sv.defpos : k;
                    var v = b.value();
                    var nv = (v==so.defpos) ? sv.defpos : v;
                    ch = ch || (nk != k) || (nv != v);
                    oc += (nk, nv);
                }
                if (ch)
                    r+=(cx, OnCond, oc);
            }
            return r;
        }
        internal override RowSet Apply(BTree<long, object> mm,Context cx,BTree<long,object>? m=null)
        {
            if (cx.undefined != CTree<long, int>.Empty)
            {
                cx.Later(defpos, mm);
                return this;
            }
            var fi = (RowSet?)cx.obs[first]??throw new PEException("PE1407");
            var se = (RowSet?)cx.obs[second]??throw new PEException("PE1407");
            var mg = matching;
         //   mm += (Matching, mg);
            m ??= mem;
            var lo = Row;
            var ro = Row;
            var oj = (CTree<long, bool>)(m[JoinCond] ?? CTree<long, bool>.Empty);
            if (mm[_Where] is CTree<long, bool> wh)
            {
                for (var b = wh.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValueExpr sv && cx.obs[sv.left] is SqlValue le && cx.obs[sv.right] is SqlValue ri)
                    {
                        var lc = le.isConstant(cx);
                        var lv = lc ? le.Eval(cx) : null;
                        var rc = ri.isConstant(cx);
                        var rv = rc ? ri.Eval(cx) : null;
                        var lf = fi.Knows(cx, le.defpos);
                        var ls = se.Knows(cx, le.defpos);
                        var rf = fi.Knows(cx, ri.defpos);
                        var rs = se.Knows(cx, ri.defpos);
                        wh -= sv.defpos;
                        if (sv.op == Sqlx.EQL)
                        {
                            if (lc && lv != null)
                            {
                                if (rc)
                                {
                                    if (lv?.CompareTo(rv) == 0)
                                        wh -= b.key();
                                    else
                                        return new EmptyRowSet(defpos, cx, this);
                                }
                                else
                                {
                                    if (rf)
                                    {
                                        fi = fi.Apply(mm + (_Matches, fi.matches + (ri.defpos, lv)), cx);
                                        for (var c = matching[ri.defpos]?.First(); c != null; c = c.Next())
                                            if (se.Knows(cx, c.key()))
                                                se = se.Apply(mm + (_Matches, se.matches + (c.key(), lv)), cx);
                                    }
                                    else if (rs)
                                    {
                                        se = se.Apply(mm + (_Matches, se.matches + (ri.defpos, lv)), cx);
                                        for (var c = matching[ri.defpos]?.First(); c != null; c = c.Next())
                                            if (fi.Knows(cx, c.key()))
                                                fi = fi.Apply(mm + (_Matches, fi.matches + (c.key(), lv)), cx);
                                    }
                                }
                            }
                            else if (rc && rv != null)
                            {
                                if (lf)
                                {
                                    fi = fi.Apply(mm + (_Matches, fi.matches + (le.defpos, rv)), cx);
                                    for (var c = matching[le.defpos]?.First(); c != null; c = c.Next())
                                        if (se.Knows(cx, c.key()))
                                            se = se.Apply(mm + (_Matches, se.matches + (c.key(), rv)), cx);
                                }
                                else if (ls)
                                {
                                    se = se.Apply(mm + (_Matches, se.matches + (le.defpos, rv)), cx);
                                    for (var c = matching[le.defpos]?.First(); c != null; c = c.Next())
                                        if (fi.Knows(cx, c.key()))
                                            fi = fi.Apply(mm + (_Matches, fi.matches + (c.key(), rv)), cx);
                                }
                            }
                            else
                            {
                                if (!((lf && rs) || (ls && rf)))
                                    continue;
                                var ml = mg[le.defpos] ?? CTree<long, bool>.Empty;
                                var mr = mg[ri.defpos] ?? CTree<long, bool>.Empty;
                                mg = mg + (le.defpos, ml + (ri.defpos, true))
                                    + (ri.defpos, mr + (le.defpos, true));
                                mm += (Matching, mg);
                                if (lf && rs && cx.obs[sv.left] is SqlValue vl && cx.obs[sv.right] is SqlValue vr)
                                {
                                    mm += (OnCond, onCond + (sv.left, sv.right));
                                    lo = (Domain)lo.New(lo.mem + (RowType, lo.rowType + vl.defpos)
                                        + (Representation, lo.representation + (vl.defpos, vl.domain)));
                                    ro = (Domain)ro.New(ro.mem + (RowType, ro.rowType + vr.defpos)
                                        + (Representation, ro.representation + (vr.defpos, vr.domain)));
                                }
                                else if (ls && rf)
                                {
                                    var ns = new SqlValueExpr(cx.GetUid(), cx, Sqlx.EQL, ri, le, Sqlx.NO);
                                    if (cx.obs[ns.left] is SqlValue nl && cx.obs[ns.right] is SqlValue nr)
                                    {
                                        mm += (OnCond, onCond + (ns.left, ns.right));
                                        lo = (Domain)lo.New(lo.mem + (RowType, lo.rowType + nl.defpos)
                                            + (Representation, lo.representation + (nl.defpos, nl.domain)));
                                        ro = (Domain)ro.New(ro.mem + (RowType, ro.rowType + nr.defpos)
                                            + (Representation, ro.representation + (nr.defpos, nr.domain)));
                                    }
                                }
                                mm += (JoinCond, oj + (sv.defpos, true));
                            }
                        }
                        else
                            mm += (JoinCond, oj + (b.key(), true));
                    }
                m += (_Where, wh);
            }
            if (mm[JoinCond] is CTree<long, bool> jc)
                m += (JoinCond, jc);
            if (mm[OnCond] is BTree<long, long?> v)
            {
                if (joinKind == Sqlx.CROSS)
                    m += (JoinKind, Sqlx.INNER);
                m += (OnCond, v);
                fi = fi.Sort(cx, (Domain)lo.New(lo.mem + (RowType, lo.rowType + fi.ordSpec.rowType)
                    + (Representation, lo.representation + fi.ordSpec.representation)), fi.distinct);
                se = se.Sort(cx, (Domain)ro.New(ro.mem + (RowType, ro.rowType + se.ordSpec.rowType)
                    + (Representation, ro.representation + se.ordSpec.representation)), se.distinct);
            }
            m += (JFirst, fi.defpos);
            m += (JSecond, se.defpos);
            if (mm[Assig] is CTree<UpdateAssignment, bool>sg)
            {
                var sf = fi.assig;
                var ss = se.assig;
                var gs = assig;
                for (var b = sg.First(); b != null; b = b.Next())
                {
                    var ua = b.key();
                    if (!Knows(cx,ua.vbl))
                        sg -= b.key();
                    if (fi.representation.Contains(ua.vbl))
                    {
                        gs -= ua;
                        sf += (ua, true);
                    }
                    if (se.representation.Contains(ua.vbl))
                    {
                        gs -= ua;
                        ss += (ua, true);
                    }
                }
                if (sf != fi.assig)
                    fi.Apply(new BTree<long,object>(Assig, sf),cx);
                if (ss != se.assig)
                    se.Apply(new BTree<long,object>(Assig, ss),cx);
                if (gs != assig)
                    m += (Assig, gs);
            }
            return base.Apply(mm, cx, m);
        }
        /// <summary>
        /// Set up a bookmark for the rows of this join
        /// </summary>
        /// <param name="matches">matching information</param>
        /// <returns>the enumerator</returns>
        protected override Cursor? _First(Context _cx)
        {
            JoinBookmark? r = joinKind switch
            {
                Sqlx.CROSS => CrossJoinBookmark.New(_cx, this),
                Sqlx.INNER => InnerJoinBookmark.New(_cx, this),
                Sqlx.LEFT => LeftJoinBookmark.New(_cx, this),
                Sqlx.RIGHT => RightJoinBookmark.New(_cx, this),
                Sqlx.FULL => FullJoinBookmark.New(_cx, this),
                _ => throw new PEException("PE57"),
            };
            var b = r?.MoveToMatch(_cx);
            return b;
        }
        protected override Cursor? _Last(Context cx)
        {
            JoinBookmark? r = joinKind switch
            {
                Sqlx.CROSS => CrossJoinBookmark.New(this, cx),
                Sqlx.INNER => InnerJoinBookmark.New(this, cx),
                Sqlx.LEFT => LeftJoinBookmark.New(this, cx),
                Sqlx.RIGHT => RightJoinBookmark.New(this, cx),
                Sqlx.FULL => FullJoinBookmark.New(this, cx),
                _ => throw new PEException("PE57"),
            };
            var b = r?.MoveToMatch(cx);
            return b;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(joinKind);
            if (joinCond!=CTree<long,bool>.Empty)
            { 
                sb.Append(" JoinCond: ");
                var cm = "(";
                for (var b=joinCond.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
            sb.Append(" First: ");sb.Append(Uid(first));
            sb.Append(" Second: "); sb.Append(Uid(second));
            if (onCond!=BTree<long,long?>.Empty)
            {
                sb.Append(" on");
                var cm = " ";
                for (var b = onCond.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                        sb.Append('='); sb.Append(Uid(p));
                    }
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A base class for join bookmarks. A join bookmark is composite: it contains bookmarks for left and right.
    /// If there are no ties, all is simple.
    /// But if there are ties on both first and second we need to ensure that
    /// all tying values of right must be used with each tying value of left.
    /// We discover there is a tie when the MTreeBookmark is over-long or has pmk nonnull.
    /// With joins we always have MTree for both left and right (from Indexes or from Ordering)
    ///     /
    /// </summary>
	internal abstract class JoinBookmark : Cursor
	{
        /// <summary>
        /// The associated join row set
        /// </summary>
		internal readonly JoinRowSet _jrs;
        protected readonly Cursor? _left, _right;
        internal readonly bool _useLeft, _useRight;
        internal readonly BTree<long, Cursor> _ts;
        protected JoinBookmark(Context cx, JoinRowSet jrs, Cursor? left, bool ul, Cursor? right,
            bool ur, int pos) : base(cx, jrs, pos, (left?._ds??E)+(right?._ds??E), 
                _Vals(jrs,cx.obs[jrs.first] as Domain??throw new PEException("PE1403"), left, ul, right, ur))
        {
            _jrs = jrs;
            _left = left;
            _useLeft = ul;
            _right = right;
            _useRight = ur;
            _ts = cx.cursors;
        }
        protected JoinBookmark(JoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v)
        {
            _jrs = cu._jrs;
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
            _ts = cx.cursors;
        }
        protected JoinBookmark(Context cx,JoinBookmark cu) 
            :base(cx,(RowSet?)cx.obs[cu._jrs.defpos]??throw new PEException("PE1408"),
                 cu._pos,cu._ds,cu) 
        {
            _jrs = cu._jrs;
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
            _ts = cx.cursors;
        }
        static TRow _Vals(Domain dom, Domain dl, Cursor? left, bool ul, Cursor? right, bool ur)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = dom.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    if (dl.representation.Contains(p) == true)
                        vs += (p, ul ? left?[p] ?? TNull.Value : TNull.Value);
                    else
                        vs += (p, ur ? right?[p] ?? TNull.Value : TNull.Value);
                }
            return new TRow(dom, vs);
        }
        public override Cursor? Next(Context _cx)
        {
            return ((JoinBookmark?)_Next(_cx))?.MoveToMatch(_cx);
        }
        public override Cursor? Previous(Context cx)
        {
            return ((JoinBookmark?)_Previous(cx))?.PrevToMatch(cx);
        }
        internal Cursor? MoveToMatch(Context _cx)
        {
            JoinBookmark? r = this;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark?)r.Next(_cx);
            return r;
        }
        internal Cursor? PrevToMatch(Context _cx)
        {
            JoinBookmark? r = this;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark?)r.Previous(_cx);
            return r;
        }
        internal override BList<TableRow> Rec()
        {
            var r = BList<TableRow>.Empty;
            if (_useLeft && _left?.Rec() is BList<TableRow> tl)
                r += tl;
            if (_useRight && _right?.Rec() is BList<TableRow> tr)
                r += tr;
            return r;
        }
    }
    /// <summary>
    /// An enumerator for an inner join rowset
    /// Key for left and right is given by the JoinCondition
    ///     /
    /// </summary>
    internal class InnerJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a new Inner Join enumerator
        /// </summary>
        /// <param name="j">The part row set</param>
        InnerJoinBookmark(Context _cx,JoinRowSet j, Cursor? left, Cursor? right,int pos=0) 
            : base(_cx,j,left,true,right,true,pos)
        {
            // warning: now check the joinCondition using AdvanceToMatch
        }
        InnerJoinBookmark(InnerJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new InnerJoinBookmark(this, cx, p, v);
        }
        internal static InnerJoinBookmark? New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet?)cx.obs[j.first])?.First(cx);
            var right = ((RowSet?)cx.obs[j.second])?.First(cx);
            if (left == null || right == null)
                return null;
            for (;;)
            {
                var bm = new InnerJoinBookmark(cx,j, left, right);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                {
                    if ((left = left.Next(cx)) == null)
                        return null;
                }
                else if ((right = right.Next(cx)) == null)
                    return null;
            }
        }
        internal static InnerJoinBookmark? New(JoinRowSet j, Context cx)
        {
            var left = ((RowSet?)cx.obs[j.first])?.Last(cx);
            var right = ((RowSet?)cx.obs[j.second])?.Last(cx);
            if (left == null || right == null)
                return null;
            for (; ; )
            {
                var bm = new InnerJoinBookmark(cx, j, left, right);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                {
                    if ((left = left.Previous(cx)) == null)
                        return null;
                }
                else if ((right = right.Previous(cx)) == null)
                    return null;
            }
        }
        /// <summary>
        /// Move to the next row in the inner join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right?.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
            }
            left = left?.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right?.ResetToTiesStart(_cx,mb);
            else
                right = right?.Next(_cx);
            if (right == null)
                return null;
            for (; ; )
            {
                var ret = new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
                int c = _jrs.Compare( _cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                {
                    if ((left = left.Next(_cx)) == null)
                        return null;
                }
                else
                    if ((right = right.Next(_cx)) == null)
                        return null;
            }
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right?.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new InnerJoinBookmark(_cx, _jrs, left, right, _pos + 1);
            }
            left = left?.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right?.ResetToTiesStart(_cx, mb);
            else
                right = right?.Previous(_cx);
            if (right == null)
                return null;
            for (; ; )
            {
                var ret = new InnerJoinBookmark(_cx, _jrs, left, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                {
                    if ((left = left.Previous(_cx)) == null)
                        return null;
                }
                else
                    if ((right = right.Previous(_cx)) == null)
                    return null;
            }
        }
    }
    /// <summary>
    /// Enumerator for a left join
    ///     /
    /// </summary>
    internal class LeftJoinBookmark : JoinBookmark
    {
        readonly Cursor? hideRight = null;
        /// <summary>
        /// Constructor: a left join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        LeftJoinBookmark(Context _cx,JoinRowSet j, Cursor? left, Cursor? right,bool ur,int pos) 
            : base(_cx,j,left,true,right,ur,pos)
        {
            // care: ensure you AdvanceToMatch
            hideRight = right;
        }
        LeftJoinBookmark(LeftJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new LeftJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new leftjoin bookmark
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first entry or null if there is none</returns>
        internal static LeftJoinBookmark? New(Context cx,JoinRowSet j)
        {
            if (cx.obs[j.first] is not RowSet lr || cx.obs[j.second] is not RowSet sr)
                throw new PEException("PE47192");
            var left = lr.First(cx);
            var right = sr.First(cx);
            if (left == null)
                return null;
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(cx,j, left, null, false, 0);
                var bm = new LeftJoinBookmark(cx,j, left, right,true,0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    return new LeftJoinBookmark(cx,j, left, right, false, 0);
                else
                    right = right.Next(cx);
            }
        }
        internal static LeftJoinBookmark? New(JoinRowSet j, Context cx)
        {
            if (cx.obs[j.first] is not RowSet lr || cx.obs[j.second] is not RowSet sr)
                throw new PEException("PE47193");
            var left = lr.Last(cx);
            var right = sr.Last(cx);
            if (left == null)
                return null;
            for (; ; )
            {
                if (right == null)
                    return new LeftJoinBookmark(cx, j, left, null, false, 0);
                var bm = new LeftJoinBookmark(cx, j, left, right, true, 0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                    return new LeftJoinBookmark(cx, j, left, right, false, 0);
                else
                    right = right.Previous(cx);
            }
        }
        /// <summary>
        /// Move to the next row in a left join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
            }
            left = left?.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right?.Next(_cx);
            }
            for (;;)
            {
                if (left == null)
                    return null;
                if (right == null)
                    return new LeftJoinBookmark(_cx,_jrs, left, null, false, _pos + 1);
                var ret = new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    return new LeftJoinBookmark(_cx,_jrs, left, right, false, _pos + 1);
                else
                    right = right.Next(_cx);
            }
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new LeftJoinBookmark(_cx, _jrs, left, right, true, _pos + 1);
            }
            left = left?.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right?.Previous(_cx);
            }
            for (; ; )
            {
                if (left == null)
                    return null;
                if (right == null)
                    return new LeftJoinBookmark(_cx, _jrs, left, null, false, _pos + 1);
                var ret = new LeftJoinBookmark(_cx, _jrs, left, right, true, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                    return new LeftJoinBookmark(_cx, _jrs, left, right, false, _pos + 1);
                else
                    right = right.Previous(_cx);
            }
        }
    }
    internal class RightJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a right join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        RightJoinBookmark(Context _cx,JoinRowSet j, Cursor? left, bool ul, Cursor? right,int pos) 
            : base(_cx,j,left,ul,right,true,pos)
        {
            // care: ensure you AdvanceToMatch
        }
        RightJoinBookmark(RightJoinBookmark cu,Context cx,long p,TypedValue v):base(cu, cx, p, v) 
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new RightJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// a bookmark for the right join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>the bookmark for the first row or null if none</returns>
        internal static RightJoinBookmark? New(Context cx,JoinRowSet j)
        {
            if (cx.obs[j.first] is not RowSet lr || cx.obs[j.second] is not RowSet sr)
                throw new PEException("PE47193");
            var left = lr.First(cx);
            var right = sr.First(cx);
            if (right == null)
                return null;
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(cx,j, null, false, right, 0);
                var bm = new RightJoinBookmark(cx,j, left, true, right,0);
                int c = j.Compare( cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    left = left.Next(cx);
                else
                    return new RightJoinBookmark(cx,j, left, false, right, 0);
            }
        }
        internal static RightJoinBookmark? New(JoinRowSet j,Context cx)
        {
            if (cx.obs[j.first] is not RowSet lr || cx.obs[j.second] is not RowSet sr)
                throw new PEException("PE47194");
            var left = lr.Last(cx);
            var right = sr.Last(cx);
            if (right == null)
                return null;
            for (; ; )
            {
                if (left == null)
                    return new RightJoinBookmark(cx, j, null, false, right, 0);
                var bm = new RightJoinBookmark(cx, j, left, true, right, 0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                    left = left.Previous(cx);
                else
                    return new RightJoinBookmark(cx, j, left, false, right, 0);
            }
        }
        /// <summary>
        /// Move to the next row in a right join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
            }
            right = right?.Next(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left?.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left?.Next(_cx);
            }
            for (;;)
            {
                if (right == null)
                    return null;
                if (left == null)
                    return new RightJoinBookmark(_cx,_jrs, null, false, right, _pos + 1);
                var ret = new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    left = left.Next(_cx);
                else
                    return new RightJoinBookmark(_cx,_jrs, left, false, right, _pos + 1);
            }
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new RightJoinBookmark(_cx, _jrs, left, true, right, _pos + 1);
            }
            right = right?.Previous(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left?.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left?.Previous(_cx);
            }
            for (; ; )
            {
                if (right == null)
                    return null;
                if (left == null)
                    return new RightJoinBookmark(_cx, _jrs, null, false, right, _pos + 1);
                var ret = new RightJoinBookmark(_cx, _jrs, left, true, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                    left = left.Previous(_cx);
                else
                    return new RightJoinBookmark(_cx, _jrs, left, false, right, _pos + 1);
            }
        }
    }
    /// <summary>
    /// A full join bookmark for a join row set
    ///     /
    /// </summary>
    internal class FullJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a full join bookmark for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        FullJoinBookmark(Context _cx,JoinRowSet j, Cursor? left, bool ul, Cursor? right, 
            bool ur, int pos)
            : base(_cx,j, left, ul, right, ur, pos)
        { }
        FullJoinBookmark(FullJoinBookmark cu,Context cx, long p, TypedValue v):base(cu,cx,p,v)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new FullJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new bookmark for a full join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first row or null if none</returns>
        internal static FullJoinBookmark? New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet?)cx.obs[j.first])?.First(cx);
            var right = ((RowSet?)cx.obs[j.second])?.First(cx);
            if (left == null && right == null)
                return null;
            var bm = new FullJoinBookmark(cx,j, left, true, right, true, 0);
            int c = j.Compare(cx);
            if (c == 0)
                return bm;
            if (c < 0)
                return new FullJoinBookmark(cx,j, left, true, right, false, 0);
            return new FullJoinBookmark(cx,j, left, false, right, true, 0);
        }
        internal static FullJoinBookmark? New(JoinRowSet j, Context cx)
        {
            if (cx.obs[j.first] is not RowSet lr || cx.obs[j.second] is not RowSet sr)
                throw new PEException("PE47195");
            var left = lr.Last(cx);
            var right = sr.Last(cx);
            if (left == null && right == null)
                return null;
            var bm = new FullJoinBookmark(cx, j, left, true, right, true, 0);
            int c = j.Compare(cx);
            if (c == 0)
                return bm;
            if (c > 0)
                return new FullJoinBookmark(cx, j, left, true, right, false, 0);
            return new FullJoinBookmark(cx, j, left, false, right, true, 0);
        }
        /// <summary>
        /// Move to the next row in a full join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right?.Mb() is MTreeBookmark mr 
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new FullJoinBookmark(_cx,_jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left?.Next(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml 
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right?.ResetToTiesStart(_cx,mb);
                    else
                        right = right?.Next(_cx);
                }
                else
                    right = right?.Next(_cx);
            }
            if (left == null && right == null)
                return null;
            if (left == null)
                return new FullJoinBookmark(_cx,_jrs, null, false, right, true, _pos + 1);
            if (right == null)
                return new FullJoinBookmark(_cx,_jrs, left, true, right, false, _pos + 1);
            new FullJoinBookmark(_cx,_jrs, left, true, right, true, _pos + 1);
            int c = _jrs.Compare(_cx);
            return new FullJoinBookmark(_cx,_jrs, left, c <= 0, right, c >= 0, _pos + 1);
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right?.Mb() is MTreeBookmark mr
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new FullJoinBookmark(_cx, _jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left?.Previous(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right?.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right?.ResetToTiesStart(_cx, mb);
                    else
                        right = right?.Previous(_cx);
                }
                else
                    right = right?.Previous(_cx);
            }
            if (left == null && right == null)
                return null;
            if (left == null)
                return new FullJoinBookmark(_cx, _jrs, null, false, right, true, _pos + 1);
            if (right == null)
                return new FullJoinBookmark(_cx, _jrs, left, true, right, false, _pos + 1);
            new FullJoinBookmark(_cx, _jrs, left, true, right, true, _pos + 1);
            int c = _jrs.Compare(_cx);
            return new FullJoinBookmark(_cx, _jrs, left, c >= 0, right, c <= 0, _pos + 1);
        }
    }
    /// <summary>
    /// A cross join bookmark for a join row set
    ///     /
    /// </summary>
    internal class CrossJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a cross join bookmark for a join row set
        /// </summary>
        /// <param name="j">a join row set</param>
        CrossJoinBookmark(Context _cx,JoinRowSet j, Cursor? left = null, Cursor? right = null,
            int pos=0) : base(_cx,j,left,true,right,true,pos)
        { }
        CrossJoinBookmark(CrossJoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v) { }
        public static CrossJoinBookmark? New(Context cx,JoinRowSet j)
        {
            if (cx.obs[j.first] is not RowSet fi || cx.obs[j.second] is not RowSet se)
                throw new PEException("PE48168");
            var f = fi.First(cx);
            if (f == null) // don't test s (possible lateral dependency)
                return null;
            var s = se.First(cx);
            for (;; )
            {
                if (s != null)
                {
                    var rb = new CrossJoinBookmark(cx, j, f, s);
                    if (DBObject.Eval(j.joinCond, cx))
                        return rb;
                    s = s.Next(cx);
                }
                if (s == null)
                {
                    f = f.Next(cx);
                    if (f == null)
                        return null;
                    s = se.First(cx);
                }
            }
        }
        public static CrossJoinBookmark? New(JoinRowSet j,Context cx)
        {
            if (cx.obs[j.first] is not RowSet fi || cx.obs[j.second] is not RowSet se)
                throw new PEException("PE48169");
            var f = fi.Last(cx);
            var s = se.Last(cx);
            if (f == null || s == null)
                return null;
            for (; ; )
            {
                if (s != null)
                {
                    var rb = new CrossJoinBookmark(cx, j, f, s);
                    if (DBObject.Eval(j.joinCond, cx))
                        return rb;
                    s = s.Previous(cx);
                }
                if (s == null)
                {
                    f = f.Previous(cx);
                    if (f == null)
                        return null;
                    s = se.Last(cx);
                }
            }
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new CrossJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// Move to the next row in the cross join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor? _Next(Context cx)
        {
            if (cx.obs[_jrs.second] is not RowSet se)
                throw new PEException("PE48170");
            var left = _left;
            var right = _right;
            right = right?.Next(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right?.Next(cx);
                }
                if (right == null)
                {
                    left = left?.Next(cx);
                    if (left == null)
                        return null;
                    right = se.First(cx);
                }
            }
        }
        protected override Cursor? _Previous(Context cx)
        {
            if (cx.obs[_jrs.second] is not RowSet se)
                throw new PEException("PE48171");
            var left = _left;
            var right = _right;
            right = right?.Previous(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right?.Previous(cx);
                }
                if (right == null)
                {
                    left = left?.Previous(cx);
                    if (left == null)
                        return null;
                    right = se.Last(cx);
                }
            }
        }
    }
}

