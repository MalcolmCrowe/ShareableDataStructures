using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System;
using System.Configuration;
using System.Runtime.ExceptionServices;
using System.Security.Authentication.ExtendedProtection;
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
    /// A row set for a Join operation.
    /// OnCond is nearly the same as matches and
    /// JoinCond is nearly the same as where, 
    /// except for nullable behaviour for LEFT/RIGHT/FULL
    ///     /// shareable as of 26 April 2021
    /// </summary>
	internal class JoinRowSet : RowSet
    {
        internal const long
            JFirst = -447, // long RowSet
            JSecond = -448, // long RowSet
            JoinCond = -203, // CTree<long,bool>
            JoinKind = -204, // Sqlx
            JoinUsing = -208, // CTree<long,long> SqlValue SqlValue (right->left)
            Natural = -207, // Sqlx
            OnCond = -344; // CTree<long,long> SqlValue SqlValue for simple equality
        /// <summary>
        /// NATURAL or USING or NO (the default)
        /// </summary>
        public Sqlx naturaljoin => (Sqlx)(mem[Natural] ?? Sqlx.NO);
        /// <summary>
        /// The list of common TableColumns for natural join
        /// </summary>
        internal CTree<long, long> joinUsing =>
            (CTree<long, long>)mem[JoinUsing] ?? CTree<long, long>.Empty;
        /// <summary>
        /// the kind of Join
        /// </summary>
        public Sqlx kind => (Sqlx)(mem[JoinKind] ?? Sqlx.CROSS);
        /// <summary>
        /// During analysis, we collect requirements for the join conditions.
        /// </summary>
        internal CTree<long, bool> joinCond =>
            (CTree<long, bool>)mem[JoinCond] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// Simple On-conditions: left=right
        /// </summary>
        internal CTree<long, long> onCond =>
            (CTree<long, long>)mem[OnCond] ?? CTree<long, long>.Empty;
        /// <summary>
        /// The two row sets being joined
        /// </summary>
		internal long first => (long)mem[JFirst];
        internal long second => (long)mem[JSecond];
        internal Sqlx joinKind => (Sqlx)(mem[JoinKind]??Sqlx.NO);
        /// <summary>
        /// Constructor: build the rowset for the Join
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(long dp,Context _cx, Domain q, RowSet lr, Sqlx k,RowSet rr,
            BTree<long,object> m = null) :
            base(dp, _cx, _Mem(_cx,dp,q,m,k,lr,rr))
        {
            _cx.Add(this);
        }
        static BTree<long, object> _Mem(Context cx, long dp,Domain q, BTree<long, object> m,
            Sqlx k,RowSet lr, RowSet rr)
        {
            m = m ?? BTree<long, object>.Empty;
            m += (JoinKind, k);
            m += (JFirst, lr.defpos);
            m += (JSecond, rr.defpos);
            m += (IIx, new Iix(lr.iix, dp));
            if (!m.Contains(Natural))
                m += (Natural, Sqlx.NO);
            var nv = BList<SqlValue>.Empty;
            var dm = (q.kind==Sqlx.TIMES)?q
                :(Domain)(Domain.TableType.Relocate(cx.GetUid()));
            var dl = cx._Dom(lr);
            var d = dl.display;
            var ns = CTree<string, long>.Empty;
            for (var b = dl.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                ns += (c.name, p);
                if (b.key() < d)
                    dm += (cx,c);
                else
                    nv += c;
            }
            var dr = cx._Dom(rr);
            d = dr.display;
            for (var b = dr.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                var cn = c.name;
                if (((SqlValue)cx.obs[ns[cn]] is SqlValue cc) &&
                   !((Sqlx)m[Natural] == Sqlx.USING
                    && ((CTree<long, long>)m[JoinUsing]).Contains(c.defpos)))
                {
                    if (((Sqlx)m[Natural] == Sqlx.NATURAL))
                        nv += c;
                    else
                    {
                        c += (Name, rr.name + "." + cn);
                        cx.Add(c);
                        cc += (Name, lr.name + "." + cn);
                        cx.Add(cc);
                        if (b.key() < d)
                            dm += (cx,c);
                        else
                            nv += c;
                    }
                }
                else if (b.key() < d)
                    dm += (cx,c);
                else
                    nv += c;
            }
            dm += (Domain.Display, dm.Length);
            for (var b = nv.First(); b != null; b = b.Next())
                dm += (cx,b.value());
            cx.Add(dm);
            var lo = CList<long>.Empty; // left ordering
            var ro = CList<long>.Empty; // right
            for (var b=((CTree<long,long>)m[OnCond])?.First();b!=null;b=b.Next())
            {
                lo += b.key();
                ro += b.value();
            }
            var ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
            var jc = (CTree<long, bool>)m[JoinCond] ?? CTree<long, bool>.Empty;
            if ((Sqlx)m[Natural] == Sqlx.USING)
            {
                int mm = 0; // common.Count
                var ju = (CTree<long, long>)m[JoinUsing] ?? CTree<long, long>.Empty;
                var od = dm;
                for (var c = dr.rowType.First(); c != null; c = c.Next())
                {
                    var rc = c.value();
                    var rv = (SqlValue)cx.obs[rc];
                    if (ju.Contains(rc))
                    {
                        var lc = ju[rc];
                        lo += lc;
                        ro += rc;
                        dm -= rv.defpos;
                        var ml = ma[lc] ?? CTree<long, bool>.Empty;
                        var mr = ma[rc] ?? CTree<long, bool>.Empty;
                        ma = ma + (lc, ml + (rc, true))
                            + (rc, mr + (lc, true));
                        mm++;
                        break;
                    }
                }
                if (od != dm)
                    cx.Add(od);
                if (mm == 0)
                    m += (JoinKind, Sqlx.CROSS);
            }
            else if ((Sqlx)m[Natural] == Sqlx.NATURAL)
            {
                var lt = dl.rowType;
                var rt = dr.rowType;
                var od = dm;
                var oc = CTree<long, long>.Empty;
                for (var b = lt.First(); b != null; b = b.Next())
                {
                    var ll = b.value();
                    var lv = (SqlValue)cx.obs[ll];
                    for (var c = rt.First(); c != null; c = c.Next())
                    {
                        var rc = c.value();
                        var rv = (SqlValue)cx.obs[rc];
                        if ((lv.alias??lv.name).CompareTo(rv.alias??rv.name) == 0)
                        {
                            var cp = new SqlValueExpr(cx.GetIid(), cx, Sqlx.EQL, lv, rv, Sqlx.NULL)
                                +(_From,dp);
                            cx.Add(cp);
                            m += (JoinCond, cp.Disjoin(cx));
                            lo += lv.defpos;
                            ro += rv.defpos;
                            dm -= rv.defpos;
                            dm += (cx,rv); // add it at the end
                            oc += (lv.defpos, rv.defpos);
                            var je = cx.Add(new SqlValueExpr(cx.GetIid(), cx, Sqlx.EQL, lv, rv, Sqlx.NO));
                            jc += (je.defpos,true);
                            break;
                        }
                    }
                }
                if (od != dm)
                    cx.Add(dm);
                if (oc.Count == 0L)
                    m += (JoinKind, Sqlx.CROSS);
                else
                {
                    m += (JoinCond, jc);
                    m += (OnCond, oc);
                }
            }
            else
            {
                var lm = BTree<string, SqlValue>.Empty;
                for (var b = dl.rowType.First(); b != null; b = b.Next())
                {
                    var sv = (SqlValue)cx.obs[b.value()];
                    lm += (sv.name, sv);
                }
                for (var b = dr.rowType.First(); b != null; b = b.Next())
                {
                    var rv = (SqlValue)cx.obs[b.value()];
                    var n = rv.name;
                    if (lm[n] is SqlValue lv)
                    {
                        var ln = (lr.alias ?? lr.name) + "." + n;
                        var nl = lv + (_Alias, ln);
                        cx.Replace(lv, nl);
                        var rn = (rr.alias ?? rr.name) + "." + n;
                        var nr = rv + (_Alias, rn);
                        cx.Replace(rv, nr);
                    }
                }
            }
            m += (_Domain, dm.defpos);
            // ensure each joinCondition has the form leftExpr compare rightExpr
            // reversing if necessary
            // if not, move it to where
            var wh = (CTree<long, bool>)m[_Where] ?? CTree<long, bool>.Empty;
            var oj = jc;
            for (var b = jc.First(); b != null; b = b.Next())
            {
                ma = (CTree<long, CTree<long, bool>>)m[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
                if (cx.obs[b.key()] is SqlValueExpr se)
                {
                    var lv = cx.obs[se.left] as SqlValue;
                    var rv = (SqlValue)cx.obs[se.right];
                    if (se.kind == Sqlx.EQL)
                    {
                        var ml = ma[lv.defpos] ?? CTree<long, bool>.Empty;
                        var mr = ma[rv.defpos] ?? CTree<long, bool>.Empty;
                        ma = ma + (lv.defpos, ml + (rv.defpos, true))
                            + (rv.defpos, mr + (lv.defpos, true));
                    }
                    if (lv.isConstant(cx) || rv.isConstant(cx))
                        continue;
                    if (lv.IsFrom(cx, lr, true) && rv.IsFrom(cx, rr, true))
                        continue;
                    if (lv.IsFrom(cx, rr, true) && rv.IsFrom(cx, lr, true))
                    {
                        cx.Replace(se, new SqlValueExpr(se.iix, cx, se.kind, rv, lv, se.mod));
                        continue;
                    }
                }
                wh += (b.key(), b.value());
                jc -= b.key();
            }
            if (jc != oj)
                m = m + (_Where, wh) + (JoinCond, jc);
            var ld = lr.lastData;
            var rd = rr.lastData;
            if (ld != 0 && rd != 0)
                m += (Table.LastData, Math.Max(ld, rd));
            var fi = CTree<long, Finder>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                fi += (b.value(), new Finder(b.value(), dp));
            m = m + (_Domain, dm.defpos) + (_Finder,fi);
            if (lo != CList<long>.Empty)
            {
                lr = lr.Sort(cx, lo, false);
                m += (JFirst, lr.defpos);
            }
            if (ro!= CList<long>.Empty)
            {
                rr = rr.Sort(cx, ro, false);
                m += (JSecond, rr.defpos);
            }
            // check if we should add more matching uids:
            if (ma != CTree<long, CTree<long, bool>>.Empty)
            {
                ma = lr.AddMatching(cx, ma);
                ma = rr.AddMatching(cx, ma);
                m += (Matching, ma);
            }
            m += (RSTargets, lr.rsTargets + rr.rsTargets);
            m += (Asserts, Assertions.MatchesTarget);
            m += (_Depth, cx.Depth(wh+jc,lr, rr, dm));
            return m;
        }
        protected JoinRowSet(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinRowSet(defpos, m);
        }
        internal override bool Knows(Context cx, long rp)
        {
            return rp == first || rp == second || base.Knows(cx, rp);
        }
        internal int Compare(Context cx)
        {
            var oc = cx.finder;
            cx.finder += finder;
            for (var b = joinCond.First(); b != null; b = b.Next())
            {
                var se = cx.obs[b.key()] as SqlValueExpr;
                var c = cx.obs[se.left].Eval(cx)?.CompareTo(cx.obs[se.right].Eval(cx)) ?? -1;
                if (c != 0)
                {
                    cx.finder = oc;
                    return c;
                }
            }
            cx.finder = oc;
            return 0;
        }
        public static JoinRowSet operator +(JoinRowSet rs, (long, object) x)
        {
            return (JoinRowSet)rs.New(rs.mem + x);
        }
        internal override int Cardinality(Context cx)
        {
            if (where == CTree<long, bool>.Empty && kind == Sqlx.CROSS)
                return ((RowSet)cx.obs[first]).Cardinality(cx)
                    * ((RowSet)cx.obs[second]).Cardinality(cx);
            var r = 0;
            for (var b = First(cx); b != null; b = b.Next(cx))
                r++;
            return r;
        }
        /// <summary>
        /// Now is the right time to optimise join conditions. 
        /// At this stage all comparisons have form left op right.
        /// Ideally we can find an index that makes at least some of the join trivial.
        /// Then we impose orderings for left and right that respect any remaining comparison join conditions,
        /// overriding ordering requests from top down analysis.
        /// </summary>
        /// <param name="ord">Requested top-down order</param>
        internal override DBObject Orders(Context cx, CList<long> ord)
        {
            var r = (JoinRowSet)base.Orders(cx, ord); // relocated if shared
            var k = kind;
            var jc = joinCond;
            var lf = (RowSet)cx.obs[first];
            var rg = (RowSet)cx.obs[second];
            for (var b = jc.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValueExpr se) // we already know these have the right form
                {
                    var lo = lf.ordSpec;
                    var ro = rg.ordSpec;
                    var lv = cx.obs[se.left] as SqlValue
                        ?? throw new PEException("PE196");
                    var rv = (SqlValue)cx.obs[se.right];
                    if (!cx.HasItem(lo, lv.defpos))
                        lf = (RowSet)lf.Orders(cx, lo + lv.defpos);
                    if (!cx.HasItem(ro, rv.defpos))
                        rg = (RowSet)rg.Orders(cx, ro + rv.defpos);
                }
            if (joinCond.Count == 0)
                for (var b = ord?.First(); b != null; b = b.Next()) // test all of these 
                {
                    var p = b.value();
                    var lo = lf.ordSpec;
                    var ro = rg.ordSpec;
                    if (cx._Dom(lf).rowType.Has(p)// && !(left.rowSet is IndexRowSet))
                        && !cx.HasItem(lo, p))
                        lf = (RowSet)lf.Orders(cx, lo + p);
                    if (cx._Dom(rg).rowType.Has(p)// && !(right.rowSet is IndexRowSet))
                        && !cx.HasItem(ro, p))
                        rg = (RowSet)rg.Orders(cx, ro + p);
                }
            cx.Add(lf);
            cx.Add(rg);
            return (RowSet)New(cx, r.mem + (JoinKind, k) + (JoinCond, jc));
        }
        internal override DBObject Relocate(long dp)
        {
            return new JoinRowSet(dp, mem);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (JoinRowSet)base._Fix(cx);
            var nc = cx.Fix(joinCond);
            if (nc != joinCond)
                r += (JoinCond, nc);
            var nf = cx.Fix(first);
            if (nf != first)
                r += (JFirst, nf);
            var ns = cx.Fix(second);
            if (ns != second)
                r += (JSecond, ns);
            var oc = cx.Fix(onCond);
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
            return new BTree<long, RowSet>(f, (RowSet)cx.obs[f]) + (s, (RowSet)cx.obs[s]);
        }
        internal override CTree<long, Cursor> SourceCursors(Context cx)
        {
            var pf = first;
            var rf = (RowSet)cx.obs[pf];
            var ps = second;
            var rs = (RowSet)cx.obs[ps];
            return rf.SourceCursors(cx) + (pf, cx.cursors[pf])
                +rs.SourceCursors(cx) + (ps,cx.cursors[ps]);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (JoinRowSet)base._Relocate(cx);
            r += (JoinCond, cx.Fix(joinCond));
            r += (JFirst, cx.Fix(first));
            r += (JSecond, cx.Fix(second));
            r += (Matching, cx.Fix(matching));
            if (r.mem.Contains(OnCond))
                r += (OnCond, cx.Fix(onCond));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (JoinRowSet)base._Replace(cx, so, sv);
            var jc = r.joinCond;
            var oc = r.onCond;
            for (var b = jc.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    jc += (b.key(), true);
            }
            if (jc != r.joinCond)
                r += (JoinCond, jc);
            r += (JFirst, cx.ObReplace(first, so, sv));
            r += (JSecond, cx.ObReplace(second, so, sv));
            if (so.defpos != sv.defpos)
                for (var b = onCond.First(); b != null; b = b.Next())
                {
                    if (b.key() == so.defpos)
                    {
                        oc -= so.defpos;
                        oc += (sv.defpos, b.value());
                    }
                    else if (b.value() == so.defpos)
                        oc += (b.key(), sv.defpos);
                }
            if (oc != r.onCond)
                r += (OnCond, oc);
            cx.Add(r);
            return r;
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx,RowSet im)
        {
            var jm = (JoinRowSet)im;
            var fi = (RowSet)cx.obs[jm.first];
            var se = (RowSet)cx.obs[jm.second];
            var mg = jm.matching;
            mm += (Matching, mg);
            var m = im.mem;
            var lo = CList<long>.Empty;
            var ro = CList<long>.Empty;
            var oj = (CTree<long, bool>)m[JoinCond] ?? CTree<long, bool>.Empty;
            if (mm[_Where] is CTree<long, bool> wh)
            {
                for (var b = wh.First(); b != null; b = b.Next())
                {
                    if (cx.obs[b.key()] is SqlValueExpr sv && sv.kind == Sqlx.EQL)
                    {
                        var k = b.key();
                        var le = (SqlValue)cx.obs[sv.left];
                        var ri = (SqlValue)cx.obs[sv.right];
                        var lc = le.isConstant(cx);
                        var lv = lc ? le.Eval(cx) : null;
                        var rc = ri.isConstant(cx);
                        var rv = rc ? ri.Eval(cx) : null;
                        var lf = fi.Knows(cx, le.defpos);
                        var ls = se.Knows(cx, le.defpos);
                        var rf = fi.Knows(cx, ri.defpos);
                        var rs = se.Knows(cx, ri.defpos);
                        if (lc)
                        {
                            if (rc)
                            {
                                if (lv.CompareTo(rv) == 0)
                                    wh -= b.key();
                                else
                                    im = new EmptyRowSet(defpos, cx, domain);
                            }
                            else
                            {
                                if (rf)
                                {
                                    fi = (RowSet)fi.New(cx, mm + (_Matches, fi.matches + (ri.defpos, lv)));
                                    for (var c = matching[ri.defpos]?.First(); c != null; c = c.Next())
                                        if (se.finder.Contains(c.key()))
                                            se = (RowSet)se.New(cx, mm + (_Matches, se.matches + (c.key(), lv)));
                                }
                                else if (rs)
                                {
                                    se = (RowSet)se.New(cx, mm + (_Matches, se.matches + (ri.defpos, lv)));
                                    for (var c = matching[ri.defpos]?.First(); c != null; c = c.Next())
                                        if (fi.finder.Contains(c.key()))
                                            fi = (RowSet)fi.New(cx, mm + (_Matches, fi.matches + (c.key(), lv)));
                                }
                            }
                        }
                        else if (rc)
                        {
                            if (lf)
                            {
                                fi = (RowSet)fi.New(cx, mm + (_Matches, fi.matches + (le.defpos, rv)));
                                for (var c = matching[le.defpos]?.First(); c != null; c = c.Next())
                                    if (se.Knows(cx, c.key()))
                                        se = (RowSet)se.New(cx, mm + (_Matches, se.matches + (c.key(), rv)));
                            }
                            else if (ls)
                            {
                                se = (RowSet)se.New(cx, mm + (_Matches, se.matches + (le.defpos, rv)));
                                for (var c = matching[le.defpos]?.First(); c != null; c = c.Next())
                                    if (fi.Knows(cx, c.key()))
                                        fi = (RowSet)se.New(cx, mm + (_Matches, fi.matches + (c.key(), rv)));
                            }
                        }
                        else
                        {
                            var ml = mg[le.defpos] ?? CTree<long, bool>.Empty;
                            var mr = mg[ri.defpos] ?? CTree<long, bool>.Empty;
                            mg = mg + (le.defpos, ml + (ri.defpos, true))
                                + (ri.defpos, mr + (le.defpos, true));
                            mm += (Matching, mg);
                            if (lf && rs)
                            {
                                mm += (OnCond, onCond + (sv.left, sv.right));
                                lo += sv.left;
                                ro += sv.right;
                            }
                            else if (ls && rf)
                            {
                                var ns = new SqlValueExpr(cx.GetIid(), cx, Sqlx.EQL, ri, le, Sqlx.NO);
                                mm += (OnCond, onCond + (ns.left, ns.right));
                                lo += ns.left;
                                ro += ns.right;
                            }
                            mm += (JoinCond, oj+(sv.defpos,true));
                        }
                    } else
                        mm += (JoinCond, oj+(b.key(),true));
                }
            }
            if (mm[JoinCond] is CTree<long, bool> jc)
                m += (JoinCond, jc);
            if (mm[OnCond] is CTree<long, long> v)
            {
                if (joinKind == Sqlx.CROSS)
                    m += (JoinKind, Sqlx.INNER);
                m += (OnCond, v);
                fi = fi.Sort(cx, lo+fi.ordSpec, fi.distinct);
                se = se.Sort(cx, ro+se.ordSpec, se.distinct);
            }
            m += (JFirst, fi.defpos);
            m += (JSecond, se.defpos);
            if (mm[Assig] is CTree<UpdateAssignment, bool>sg)
            {
                var fd = cx._Dom(fi);
                var sd = cx._Dom(se);
                var sf = fi.assig;
                var ss = se.assig;
                var gs = im.assig;
                for (var b = sg.First(); b != null; b = b.Next())
                {
                    var ua = b.key();
                    if (!(finder.Contains(ua.vbl) && finder.Contains(ua.vbl)))
                        sg -= b.key();
                }
                for (var b = assig.First(); b != null; b = b.Next())
                {
                    var ua = b.key();
                    if (fd.representation.Contains(ua.vbl))
                    {
                        gs -= ua;
                        sf += (ua, true);
                    }
                    if (sd.representation.Contains(ua.vbl))
                    {
                        gs -= ua;
                        ss += (ua, true);
                    }
                }
                if (sf != fi.assig)
                    cx.Add(fi + (Assig, sf));
                if (ss != se.assig)
                    cx.Add(se + (Assig, ss));
                if (gs != assig)
                    mm += (Assig, gs);
            }
            return (RowSet)cx.Add((RowSet)im.New(m));
        }
        internal override (CTree<long, Finder>,CTree<long,bool>) AllWheres(Context cx, 
            (CTree<long, Finder>,CTree<long,bool>) ln)
        {
            var(nd,rc) = cx.Needs(ln, this, where);
            if (cx.obs[first] is RowSet lf)
            {
                var (ns, ss) = lf.AllWheres(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                    nd += (b.key(), b.value());
                rc += ss;
            }
            if (cx.obs[second] is RowSet rs)
            {
                var (ns, ss) = rs.AllWheres(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                    nd += (b.key(), b.value());
                rc += ss;
            }
            return (nd, rc);
        }
        internal override (CTree<long, Finder>,CTree<long,bool>) AllMatches(Context cx, 
            (CTree<long, Finder>,CTree<long,bool>) ln)
        {
            var(nd,rc) = cx.Needs(ln, this, matches);
            if (cx.obs[first] is RowSet lf)
            {
                var (ns, ss) = lf.AllMatches(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, b.value());
                    rc += (u, true);
                }
                rc += ss;
            }
            if (cx.obs[second] is RowSet rs)
            {
                var (ns, ss) = rs.AllMatches(cx, ln);
                for (var b = ns.First(); b != null; b = b.Next())
                {
                    var u = b.key();
                    nd += (u, b.value());
                    rc += (u,true);
                }
                rc += ss;
            }
            return (nd,rc);
        }
        /// <summary>
        /// Set up a bookmark for the rows of this join
        /// </summary>
        /// <param name="matches">matching information</param>
        /// <returns>the enumerator</returns>
        protected override Cursor _First(Context _cx)
        {
            JoinBookmark r;
            switch (joinKind)
            {
                case Sqlx.CROSS: r= CrossJoinBookmark.New(_cx,this); break;
                case Sqlx.INNER: r= InnerJoinBookmark.New(_cx,this); break;
                case Sqlx.LEFT: r = LeftJoinBookmark.New(_cx, this); break;
                case Sqlx.RIGHT: r = RightJoinBookmark.New(_cx, this); break;
                case Sqlx.FULL: r = FullJoinBookmark.New(_cx,this); break;
                default:
                    throw new PEException("PE57");
            }
            var b = r?.MoveToMatch(_cx);
            return b;
        }
        protected override Cursor _Last(Context cx)
        {
            JoinBookmark r;
            switch (joinKind)
            {
                case Sqlx.CROSS: r = CrossJoinBookmark.New(this, cx); break;
                case Sqlx.INNER: r = InnerJoinBookmark.New(this, cx); break;
                case Sqlx.LEFT: r = LeftJoinBookmark.New(this, cx); break;
                case Sqlx.RIGHT: r = RightJoinBookmark.New(this, cx); break;
                case Sqlx.FULL: r = FullJoinBookmark.New(this, cx); break;
                default:
                    throw new PEException("PE57");
            }
            var b = r?.MoveToMatch(cx);
            return b;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ");sb.Append(kind);
            if (joinCond!=CTree<long,bool>.Empty)
            { 
                sb.Append(" JoinCond: ");
                var cm = "(";
                for (var b=joinCond.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
            sb.Append(" First: ");sb.Append(Uid(first));
            sb.Append(" Second: "); sb.Append(Uid(second));
            if (onCond!=CTree<long,long>.Empty)
            {
                sb.Append(" on");
                var cm = " ";
                for (var b = onCond.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";  sb.Append(Uid(b.key()));
                    sb.Append("="); sb.Append(Uid(b.value()));
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
    ///     /// shareable as of 26 April 2021
    /// </summary>
	internal abstract class JoinBookmark : Cursor
	{
        /// <summary>
        /// The associated join row set
        /// </summary>
		internal readonly JoinRowSet _jrs;
        protected readonly Cursor _left, _right;
        internal readonly bool _useLeft, _useRight;
        internal readonly BTree<long, Cursor> _ts;
        protected JoinBookmark(Context cx, JoinRowSet jrs, Cursor left, bool ul, Cursor right,
            bool ur, int pos) : base(cx, jrs, pos, (left?._ds??E)+(right?._ds??E), 
                _Vals(cx._Dom(jrs), left, ul, right, ur))
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
            :base(cx,(RowSet)cx.obs[cu._jrs.defpos],cu._pos,cu._ds,cu) 
        {
            _jrs = cu._jrs;
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
            _ts = cx.cursors;
        }
        static TRow _Vals(Domain dom, Cursor left, bool ul, Cursor right, bool ur)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = dom.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (ul && left.values.Contains(p))
                    vs += (p, left[p]);
                else if (ur && right.values.Contains(p))
                    vs += (p, right[p]);
            }
            return new TRow(dom, vs);
        }
        public override Cursor Next(Context _cx)
        {
            return ((JoinBookmark)_Next(_cx))?.MoveToMatch(_cx);
        }
        public override Cursor Previous(Context cx)
        {
            return ((JoinBookmark)_Previous(cx))?.PrevToMatch(cx);
        }
        internal Cursor MoveToMatch(Context _cx)
        {
            JoinBookmark r = this;
            var ox = _cx.finder;
            _cx.finder = _jrs.finder;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark)r.Next(_cx);
            _cx.finder = ox;
            return r;
        }
        internal Cursor PrevToMatch(Context _cx)
        {
            JoinBookmark r = this;
            var ox = _cx.finder;
            _cx.finder = _jrs.finder;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark)r.Previous(_cx);
            _cx.finder = ox;
            return r;
        }
        internal override BList<TableRow> Rec()
        {
            var r = BList<TableRow>.Empty;
            if (_useLeft)
                r += _left.Rec();
            if (_useRight)
                r += _right.Rec();
            return r;
        }
    }
    /// <summary>
    /// An enumerator for an inner join rowset
    /// Key for left and right is given by the JoinCondition
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class InnerJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a new Inner Join enumerator
        /// </summary>
        /// <param name="j">The part row set</param>
        InnerJoinBookmark(Context _cx,JoinRowSet j, Cursor left, Cursor right,int pos=0) 
            : base(_cx,j,left,true,right,true,pos)
        {
            // warning: now check the joinCondition using AdvanceToMatch
        }
        InnerJoinBookmark(InnerJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        InnerJoinBookmark(Context cx, InnerJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new InnerJoinBookmark(this, cx, p, v);
        }
        internal static InnerJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet)cx.obs[j.first]).First(cx);
            var right = ((RowSet)cx.obs[j.second]).First(cx);
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
        internal static InnerJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = ((RowSet)cx.obs[j.first]).Last(cx);
            var right = ((RowSet)cx.obs[j.second]).Last(cx);
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
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
            }
            left = left.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right.ResetToTiesStart(_cx,mb);
            else
                right = right.Next(_cx);
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
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new InnerJoinBookmark(_cx, _jrs, left, right, _pos + 1);
            }
            left = left.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right.ResetToTiesStart(_cx, mb);
            else
                right = right.Previous(_cx);
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
        internal override Cursor _Fix(Context cx)
        {
            return new InnerJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// Enumerator for a left join
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class LeftJoinBookmark : JoinBookmark
    {
        readonly Cursor hideRight = null;
        /// <summary>
        /// Constructor: a left join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        LeftJoinBookmark(Context _cx,JoinRowSet j, Cursor left, Cursor right,bool ur,int pos) 
            : base(_cx,j,left,true,right,ur,pos)
        {
            // care: ensure you AdvanceToMatch
            hideRight = right;
        }
        LeftJoinBookmark(LeftJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        LeftJoinBookmark(Context cx,LeftJoinBookmark cu):base(cx,cu)
        {
            hideRight = (Cursor)cu.hideRight?.Fix(cx);
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new LeftJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new leftjoin bookmark
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first entry or null if there is none</returns>
        internal static LeftJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet)cx.obs[j.first]).First(cx);
            var right = ((RowSet)cx.obs[j.second]).First(cx);
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
        internal static LeftJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = ((RowSet)cx.obs[j.first]).Last(cx);
            var right = ((RowSet)cx.obs[j.second]).Last(cx);
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
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
            }
            left = left.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right.Next(_cx);
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
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new LeftJoinBookmark(_cx, _jrs, left, right, true, _pos + 1);
            }
            left = left.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right.Previous(_cx);
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
        internal override Cursor _Fix(Context cx)
        {
            return new LeftJoinBookmark(cx,this);
        }
    }
    /// shareable as of 26 April 2021
    internal class RightJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a right join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        RightJoinBookmark(Context _cx,JoinRowSet j, Cursor left, bool ul, Cursor right,int pos) 
            : base(_cx,j,left,ul,right,true,pos)
        {
            // care: ensure you AdvanceToMatch
        }
        RightJoinBookmark(RightJoinBookmark cu,Context cx,long p,TypedValue v):base(cu, cx, p, v) 
        { }
        RightJoinBookmark(Context cx, RightJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new RightJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// a bookmark for the right join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>the bookmark for the first row or null if none</returns>
        internal static RightJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet)cx.obs[j.first]).First(cx);
            var right = ((RowSet)cx.obs[j.second]).First(cx);
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
        internal static RightJoinBookmark New(JoinRowSet j,Context cx)
        {
            var left = ((RowSet)cx.obs[j.first]).Last(cx);
            var right = ((RowSet)cx.obs[j.second]).Last(cx);
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
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
            }
            right = right.Next(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left.Next(_cx);
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
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new RightJoinBookmark(_cx, _jrs, left, true, right, _pos + 1);
            }
            right = right.Previous(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left.Previous(_cx);
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
        internal override Cursor _Fix(Context cx)
        {
            return new RightJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// A full join bookmark for a join row set
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class FullJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a full join bookmark for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        FullJoinBookmark(Context _cx,JoinRowSet j, Cursor left, bool ul, Cursor right, 
            bool ur, int pos)
            : base(_cx,j, left, ul, right, ur, pos)
        { }
        FullJoinBookmark(FullJoinBookmark cu,Context cx, long p, TypedValue v):base(cu,cx,p,v)
        { }
        FullJoinBookmark(Context cx, FullJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new FullJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new bookmark for a full join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first row or null if none</returns>
        internal static FullJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = ((RowSet)cx.obs[j.first]).First(cx);
            var right = ((RowSet)cx.obs[j.second]).First(cx);
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
        internal static FullJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = ((RowSet)cx.obs[j.first]).Last(cx);
            var right = ((RowSet)cx.obs[j.second]).Last(cx);
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
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right.Mb() is MTreeBookmark mr 
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new FullJoinBookmark(_cx,_jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left.Next(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml 
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right.ResetToTiesStart(_cx,mb);
                    else
                        right = right.Next(_cx);
                }
                else
                    right = right.Next(_cx);
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
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right.Mb() is MTreeBookmark mr
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new FullJoinBookmark(_cx, _jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left.Previous(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right.ResetToTiesStart(_cx, mb);
                    else
                        right = right.Previous(_cx);
                }
                else
                    right = right.Previous(_cx);
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
        internal override Cursor _Fix(Context cx)
        {
            return new FullJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// A cross join bookmark for a join row set
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class CrossJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a cross join bookmark for a join row set
        /// </summary>
        /// <param name="j">a join row set</param>
        CrossJoinBookmark(Context _cx,JoinRowSet j, Cursor left = null, Cursor right = null,
            int pos=0) : base(_cx,j,left,true,right,true,pos)
        { }
        CrossJoinBookmark(CrossJoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v) { }
        CrossJoinBookmark(Context cx,CrossJoinBookmark cu):base(cx,cu)
        { }
        public static CrossJoinBookmark New(Context cx,JoinRowSet j)
        {
            var f = ((RowSet)cx.obs[j.first]).First(cx);
            var s = ((RowSet)cx.obs[j.second]).First(cx);
            if (f == null) // don't test s (possible lateral dependency)
                return null;
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
                    s = ((RowSet)cx.obs[j.second]).First(cx);
                }
            }
        }
        public static CrossJoinBookmark New(JoinRowSet j,Context cx)
        {
            var f = ((RowSet)cx.obs[j.first]).Last(cx);
            var s = ((RowSet)cx.obs[j.second]).Last(cx);
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
                    s = ((RowSet)cx.obs[j.second]).Last(cx);
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
        protected override Cursor _Next(Context cx)
        {
            var left = _left;
            var right = _right;
            right = right.Next(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right.Next(cx);
                }
                if (right == null)
                {
                    left = left.Next(cx);
                    if (left == null)
                        return null;
                    right = ((RowSet)cx.obs[_jrs.second]).First(cx);
                }
            }
        }
        protected override Cursor _Previous(Context cx)
        {
            var left = _left;
            var right = _right;
            right = right.Previous(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right.Previous(cx);
                }
                if (right == null)
                {
                    left = left.Previous(cx);
                    if (left == null)
                        return null;
                    right = ((RowSet)cx.obs[_jrs.second]).Last(cx);
                }
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new CrossJoinBookmark(cx, this);
        }
    }
}

