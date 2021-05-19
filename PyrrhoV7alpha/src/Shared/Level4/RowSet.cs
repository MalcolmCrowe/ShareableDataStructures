using System;
using Pyrrho.Common;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Diagnostics.Eventing.Reader;
using System.Data;
using System.Runtime.Remoting.Channels;
using System.Security.Policy;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Net;
using System.IO;
using System.Collections.Generic;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// A RowSet is the result of a stage of query processing. 
    /// We take responsibility for cx.values in the finder list.
    /// 
    /// RowSet is a subclass of TypedValue so inherits domain and a column ordering.
    /// RowSet Domains have fully specified representation and defpos matches the
    /// rowSet.
    /// TableRowSet, IndexRowSet and TransitionRowSet access physical columns
    /// and ignore the column ordering.
    /// Other RowSets must not contain any physical uids in their column uids
    /// or within their domain's representation.
    /// 
    /// RowSet defpos (uids) are carefully managed:
    /// SystemRowSet uids match the relevant System Table (thus a negative uid)
    /// TableRowSet uids match the table defining position
    /// IndexRowSet uids match the index defpos
    /// TransitionRowSet is somewhere on the heap (accessible transition and target contexts)
    /// EmptyRowSet uid is -1
    /// The following rowsets always have physical or lexical uids and
    /// rowtype and domain information comes from the syntax:
    /// TrivialRowSet, SelectedRowSet, JoinRowSet, MergeRowSet, 
    /// SelectRowSet, EvalRowSet, GroupRowSet, TableExpRowSet, ExplicitRowSet.
    /// All other rowSets get their defining position by cx.nextId++
    /// 
    /// IMMUTABLE
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal abstract class RowSet : DBObject
    {
        internal const long
            Built = -402, // bool
            _CountStar = -281, // long
            _Finder = -403, // CTree<long,Finder> SqlValue
            _Needed = -401, // CTree<long,Finder>  SqlValue
            RowOrder = -404, //CList<long> SqlValue 
            _Rows = -407, // CList<TRow> 
            RSTargets = -197, // CTree<long,long> Table/RowSet RowSet 
            _Source = -151; // RowSet
        internal CTree<long, bool> aggs =>
            (CTree<long, bool>)mem[Query.Aggregates] ?? CTree<long, bool>.Empty;
        internal CList<long> rt => domain.rowType;
        internal CList<long> keys => (CList<long>)mem[Index.Keys] ?? CList<long>.Empty;
        internal CList<long> rowOrder => (CList<long>)mem[RowOrder] ?? CList<long>.Empty;
        internal CTree<long, bool> where =>
            (CTree<long, bool>)mem[Query.Where] ?? CTree<long, bool>.Empty;
        internal CTree<long, TypedValue> matches =>
            (CTree<long, TypedValue>)mem[Query._Matches] ?? CTree<long, TypedValue>.Empty;
        internal CTree<long, CTree<long, bool>> matching =>
            (CTree<long, CTree<long, bool>>)mem[Query.Matching] 
            ?? CTree<long, CTree<long, bool>>.Empty;
        internal CTree<long, Finder> needed =>
            (CTree<long, Finder>)mem[_Needed]; // must be initially null
        internal bool built => (bool)(mem[Built]??false);
        internal long groupSpec => (long)(mem[TableExpression.Group]??-1L);
        internal long target => (long)(mem[From.Target] // for table-focussed RowSets
            ??rsTargets.First()?.key()??-1L); // for safety
        internal CTree<long,long> rsTargets => 
            (CTree<long,long>)mem[RSTargets]??CTree<long,long>.Empty;
        internal long source => (long)(mem[_Source] ?? -1L);
        internal CTree<UpdateAssignment, bool> assig =>
            (CTree<UpdateAssignment, bool>)mem[Query.Assig] 
            ?? CTree<UpdateAssignment, bool>.Empty;
        internal BList<TRow> rows =>
            (BList<TRow>)mem[_Rows] ?? BList<TRow>.Empty;
        internal long lastData => (long)(mem[Table.LastData] ?? 0L);
        internal long countStar => (long)(mem[_CountStar] ?? -1L);
        internal readonly struct Finder :IComparable
        {
            public readonly long col;
            public readonly long rowSet;
            public Finder(long c, long r) 
            {
                col = c; rowSet = r; 
            }
            internal Finder Relocate(Context cx)
            {
                var c = cx.obuids[col]??col;
                var r = cx.rsuids[rowSet]??rowSet;
                return (c != col || r != rowSet) ? new Finder(c, r) : this;
            }
            internal Finder Relocate(Writer wr)
            {
                var c = wr.Fix(col);
                var r = wr.rss.Contains(rowSet) ? wr.rss[rowSet].defpos
                    : (rowSet==wr.curs) ? wr.Fix(wr.curs)
                    : ((RowSet)wr.cx.data[rowSet]._Relocate(wr)).defpos;
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
        /// We could simply search along cx.data to pick the rowset we want.
        /// Before we compute a Cursor, we place the source finder in the  Context, 
        /// so that the evaluation can use values retrieved from the source cursors.
        /// </summary>
        internal CTree<long, Finder> finder =>
            (CTree<long, Finder>)mem[_Finder] ?? CTree<long, Finder>.Empty;
        internal int display => domain.display;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="rt">The way for a cursor to calculate a row</param>
        /// <param name="kt">The way for a cursor to calculate a key (by default same as rowType)</param>
        /// <param name="or">The ordering of rows in this rowset</param>
        /// <param name="wh">A set of boolean conditions on row values</param>
        /// <param name="ma">A filter for row column values</param>
        protected RowSet(long dp, Context cx, Domain dt,
            CTree<long, Finder> fin = null, CList<long> kt = null,
            CTree<long, bool> wh = null, CList<long> or = null,
            CTree<long, TypedValue> ma = null,
            BTree<long,object> m=null) 
            : base(dp,_Fin(dp,cx,fin,dt.rowType,m)
                +(_Domain,dt)+(Index.Keys,kt??dt.rowType)
                +(RowOrder,or??CList<long>.Empty)
                +(Query.Where, wh??CTree<long,bool>.Empty)
                +(Query._Matches,ma??CTree<long, TypedValue>.Empty))
        {
            cx.data += (dp, this);
        }
        protected RowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        protected RowSet(Context cx,RowSet rs,CTree<long,Finder>nd, bool bt) 
            :this(rs.defpos,rs.mem+(_Needed,nd)+(Built,bt))
        {
            cx.data += (rs.defpos, this);
        }
        protected static BTree<long,object> _Fin(long dp,Context cx,CTree<long,Finder> fin,CList<long> rt,
            BTree<long,object>m)
        {
            fin = fin ?? CTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlValue sv && sv.alias is string a
                    && cx.defs.Contains(a))
                    fin += (cx.defs[a].Item1, new Finder(p, dp));
                fin += (p, new Finder(p, dp));
            }
            return (m??BTree<long,object>.Empty) +(_Finder,fin);
        }
        protected static CTree<long,TypedValue> _Matches(Context cx,CTree<long,bool> wh)
        {
            var r = CTree<long, TypedValue>.Empty;
            for (var b=wh.First();b!=null;b=b.Next())
                r = cx.obs[b.key()].AddMatch(cx,r);
            return r;
        }
        internal abstract RowSet New(Context cx, CTree<long, Finder> nd, bool bt);
        internal virtual bool TableColsOk => false;
        public static RowSet operator+(RowSet rs,(long,object)x)
        {
            return (RowSet)rs.New(rs.mem+x);
        }
        internal virtual string DbName(Context cx)
        {
            return cx.db.name;
        }
        internal SqlValue MaybeAdd(Context cx, SqlValue su)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                if (sv._MatchExpr(cx, su))
                    return sv;
            }
            su += (_Alias, alias ?? name ?? ("C_" + (defpos & 0xfff)));
            Add(cx, su);
            return su;
        }
        internal virtual RowSet Relocate1(Context cx)
        {
            var rs = this;
            var ts = rs.rsTargets;
            for (var c = ts.First(); c != null; c = c.Next())
            {
                var or = c.value();
                var nr = cx.rsuids[or] ?? or;
                if (or!=nr)
                    ts += (c.key(), nr);
            }
            var fi = rs.finder;
            for (var c = fi.First(); c != null; c = c.Next())
            {
                var f = c.value();
                var or = f.rowSet;
                var nr = cx.rsuids[or] ?? or;
                if (or!=nr)
                    fi += (c.key(), new Finder(f.col, nr));
            }
            return rs + (_Finder, fi) + (RSTargets, ts);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            t = Scan(t, keys, VIC.RK|VIC.OV);
            t = Scan(t, rowOrder, VIC.RK|VIC.OV);
            t = Scan(t, where, VIC.RK|VIC.OV);
            t = Scan(t, matches, VIC.RK|VIC.OV);
            t = Scan(t, finder);
            t = Scan(t, needed);
            t = Scan(t, assig, VIC.RK|VIC.OV);
            t = Scan(t, aggs, VIC.RK|VIC.OV);
            t = Scan(t, rsTargets, VIC.RK|VIC.OV, VIC.RK|VIC.RV);
            t = Scan(t, target, VIC.RK|VIC.OV);
            t = Scan(t, from, VIC.RK|VIC.OV|VIC.RV);
            t = Scan(t, (long)(mem[_Source] ?? -1L), VIC.RK|VIC.RV);
            return base.Scan(t);
        }
        internal override bool Uses(Context cx, long t)
        {
            for(var b=rt.First();b!=null;b=b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                if (sv.Uses(cx, t))
                    return true;
            }
            for (var b=where.First();b!=null;b=b.Next())
            {
                var sv = (SqlValue)cx.obs[b.key()];
                if (sv.Uses(cx, t))
                    return true;
            }
            for (var b = matches.First(); b != null; b = b.Next())
                if (b.key() == t)
                    return true;
            if (cx.obs[groupSpec] is GroupSpecification gs &&
                gs.sets.Has(t))
                return true;
            return false;
        }
        internal override DBObject Instance(Context cx)
        {
            var np = cx.rsuids[defpos] ?? defpos;
            if (np != defpos)
                return (DBObject)cx.data[np].Fix(cx);
            return this;
        }
        internal static CTree<long,long> RSTg(BList<long> ls)
        {
            var r = CTree<long, long>.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
                r += (b.value(), b.value());
            return r;
        }
        protected CTree<long,CTree<long,bool>> Fix (CTree<long, CTree<long, bool>> ma,Context cx)
        {
            var r = CTree<long,CTree<long, bool>>.Empty;
            for (var b = ma.First(); b != null; b = b.Next())
            {
                var k = b.key();
                r += (cx.obuids[k] ?? k, cx.Fix(b.value()));
            }
            return r;
        }
        protected FDJoinPart Fix(FDJoinPart fd,BTree<long,long?>obuids)
        {
            var r = CTree<long, long>.Empty;
            for (var b=fd.conds.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var v = b.value();
                r += (obuids[k] ?? k, obuids[v] ?? v);
            }
            return fd + (FDJoinPart.FDConds, r);
        }
        internal RowSet AddUpdateAssignment(Context cx,UpdateAssignment ua)
        {
            var s = (long)(mem[_Source] ?? -1L);
            if (cx.data[s] is RowSet rs && rs.finder.Contains(ua.vbl) 
                && ((SqlValue)cx.obs[ua.val]).KnownBy(cx,rs))
                    rs.AddUpdateAssignment(cx, ua);
            var r = this + (Query.Assig, assig + (ua, true));
            cx.data += (r.defpos, r);
            return r;
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            if (rsTargets==CTree<long,long>.Empty)
                throw new DBException("42174");
            for (var b = rsTargets.First(); b != null; b = b.Next())
                cx = cx.data[b.value()]?.Insert(cx, fm, false, prov, cl);
            return cx;
        }
        internal virtual RowSet Build(Context cx)
        {
            if (needed == null)
                throw new PEException("PE422");
            return New(cx,needed,true);
        }
        internal RowSet ComputeNeeds(Context cx)
        {
            if (needed != null)
                return this;
            var nd = CTree<long, Finder>.Empty;
            nd = cx.Needs(nd, this, rt);
            nd = cx.Needs(nd, this, keys);
            nd = cx.Needs(nd, this, rowOrder);
            nd = AllWheres(cx, nd);
            if (!(this is IndexRowSet || this is TableRowSet))
                nd = AllMatches(cx, nd);
            var r = New(cx, nd, false);
            if (cx.aggregators!=CTree<long,bool>.Empty)
                for (var b = finder.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValue sv && sv.LocallyConstant(cx, r))
                        cx._Add(sv + (SqlValue.LocalConstant, 
                            sv.localConstant + (defpos, true)));
            cx.data += (defpos, r);
            return r; 
        }
        /// <summary>
        /// Deal with RowSet prequisites (a kind of implementation of LATERAL).
        /// We can't calculate these earlier since the source rowsets haven't been constructed.
        /// Cursors will contain corresponding values of prerequisites.
        /// Once First()/Next() has been called, we need to see if they
        /// have been evaluated/changed. Either we can't or we must build this RowSet.
        /// </summary>
        /// <param name="was"></param>
        /// <returns></returns>
        internal RowSet MaybeBuild(Context cx,BTree<long,TypedValue>was=null)
        {
            var r = this;
            // We cannot Build if Needs not met by now.
            if (was == null || this is RestRowSet)
            {
                var ox = cx.finder;
                r = ComputeNeeds(cx); // install nd immediately in case Build looks for it
                var bd = true;
                for (var b = r.needed.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    cx.finder += (p,b.value()); // enable build
                    if (cx.obs[p].Eval(cx) == null)
                        bd = false;
                }
                cx.finder = ox;
                if (bd) // all prerequisites are ok
                    r = r.Build(cx);  // we can Build (will set built to true)
                return r;
            }
            for (var b = r.needed.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var fi = b.value();
                var v = cx.cursors[fi.rowSet]?[fi.col];
                if (v == null)
                    throw new PEException("PE188");
                var ov = was?[p];
                if (ov == null || ov.CompareTo(v) != 0)
                {
                    r = r.Build(cx); // we must Build again
                //    cx.cursors += (r.defpos,r._First(cx));
                    return r;
                }
            }
            // Otherwise we don't need to
            return r;
        }
        protected void Fixup(Context cx,RowSet now)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = finder.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var f = b.value();
                if (f.rowSet == defpos)
                    fi += (p, new Finder(f.col, now.defpos));
                else
                    fi += (p,f);
            }
            now += (_Finder, fi);
            cx.data += (now.defpos, now);
            for (var b = cx.data.First(); b != null; b = b.Next())
                if (b.value() == this)
                    cx.data += (b.key(), now);  
        }
        internal override Basis Fix(Context cx)
        {
            var r = this;
            var np = cx.rsuids[defpos] ?? defpos;
            if (np!=defpos)
                r = (RowSet)Relocate(np);
            var nd = (Domain)domain.Fix(cx);
            if (nd.CompareTo(domain)!=0)
                r += (_Domain, nd);
            var gs = (GroupSpecification)cx.obs[cx.obuids[groupSpec] ?? groupSpec]?.Fix(cx);
            if (gs != null && gs.defpos != groupSpec)
                r += (TableExpression.Group, gs.defpos);
            var nf = cx.Fix(finder);
            if (nf.CompareTo(finder)!=0)
                r += (_Finder, nf);
            var nk = cx.Fix(keys);
            if (nk != keys)
                r += (Index.Keys, nk);
            var no = cx.Fix(rowOrder);
            if (no != rowOrder)
                r += (RowOrder, no);
            var nw = cx.Fix(where);
            if (nw != where)
                r += (Query.Where, nw);
            var nm = cx.Fix(matches);
            if (nm != matches)
                r += (Query._Matches, nm);
            var ts = CTree<long,long>.Empty;
            var ch = false;
            for (var b=rsTargets.First();b!=null;b=b.Next())
            {
                var n = cx.rsuids[b.value()]??b.value();
                var k = cx.obuids[b.key()]??b.key();
                if (n != b.value() || k!=b.key())
                    ch = true;
                ts += (k,n);
            }
            if (ch)
                r += (RSTargets, ts);
            var ag = cx.Fix(assig);
            if (ag != assig)
                r += (Query.Assig, ag);
            var s = (long)(mem[_Source] ?? -1L);
            var ns = cx.rsuids[s] ?? s;
            if (ns != s)
                r += (_Source, ns);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
    //        if (wr.rss.Contains(defpos)) If you uncomment: representation will sometimes be incorrect
    //            return wr.rss[defpos];
            var r = (RowSet)Relocate(wr.Fix(defpos));
            var oc = wr.curs;
            wr.curs = defpos;
            var dm = (Domain)domain._Relocate(wr);
            r += (_Domain, dm);
            r += (_Finder, wr.Fix(finder));
            r += (Index.Keys, wr.Fix(keys));
            r += (RowOrder, wr.Fix(rowOrder));
            r += (Query.Where, wr.Fix(where));
            r += (Query._Matches, wr.Fix(matches));
            r += (TableExpression.Group, wr.Fix(groupSpec));
            r += (RSTargets, wr.Fix(rsTargets));
            r += (Query.Assig, wr.Fix(assig));
            if (mem.Contains(_Source))
                r += (_Source, wr.Fix((long)mem[_Source]));
            wr.rss += (defpos, r);
            wr.curs = oc;
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSet)base._Replace(cx,so,sv);
            var dm = (Domain)domain._Replace(cx,so,sv);
            r += (_Domain, dm);
            r += (_Finder, cx.Replaced(finder));
            r += (Index.Keys, cx.Replaced(keys));
            r += (RowOrder, cx.Replaced(rowOrder));
            r += (Query.Where, cx.Replaced(where));
            r += (Query._Matches, cx.Replaced(matches));
            var ch = false;
            var ts = CTree<long,long>.Empty;
            for (var b=rsTargets.First();b!=null; b=b.Next())
            {
                var p = cx.RsReplace(b.value(), so, sv);
                var k = cx.done[b.key()]?.defpos??b.key();
                if (p != b.value() || k!=b.key())
                    ch = true;
                ts += (k,p);
            }
            if (ch)
                r += (RSTargets, ts);
            if (mem.Contains(_Source))
                r += (_Source, cx.RsReplace((long)mem[_Source],so,sv));
            if (cx.done.Contains(groupSpec))
                r += (TableExpression.Group, cx.done[groupSpec].defpos);
            cx.data += (defpos, r);
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            var p = rt[i];
            var sv = cx.obs[p];
            var n = sv?.alias ?? sv?.name;
            return cx.Inf(p)?.name ?? n??("Col"+i);
        }
        internal virtual RowSet Source(Context cx)
        {
            return cx.data[(long)(mem[_Source]??-1L)];
        }
        internal virtual BList<long> Sources(Context cx)
        {
            var r = BList<long>.Empty;
            var p = (long)(mem[_Source] ?? -1L);
            if (p >= 0)
                r += p;
            return r;
        }
        internal override DBObject QParams(Context cx)
        {
            var r = base.QParams(cx);
            var m = cx.QParams(matches);
            if (m != matches)
                r += (Query._Matches, m);
            return r;
        }
        /// <summary>
        /// See if the source can replace this (removal of a pipeline step or condition).
        /// For step removal to be possible, the rowTypes must match exactly
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual RowSet Review(Context cx,BTree<long,bool> skip)
        {
            return skip.Contains(defpos)?new EmptyRowSet(cx.nextHeap++,cx,domain)
                :this;
        }
        internal virtual CTree<long,bool> Review(Context cx,CTree<long,bool> ag)
        {
            for (var b=rt.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (cx.obs[p].aggregates(cx))
                    ag += (p, true);
            }
            for (var b=ag.First();b!=null;b=b.Next())
            {
                var p = b.key();
                if (!((SqlValue)cx.obs[p]).KnownBy(cx,this))
                    ag -= p;
            }
            return ag;
        }
        internal virtual CTree<long,TypedValue> Review(Context cx,CTree<long,TypedValue> ma)
        {
            var ms = matches;
            for (var b = ma.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (!domain.representation.Contains(k))
                    ma -= k;
                else if (!ms.Contains(k))
                    ms += (k, b.value());
            }
            if (ms != matches)
                cx.data += (defpos, this + (Query._Matches, ms));
            return ma;
        }
        internal virtual CTree<UpdateAssignment,bool> Review(Context cx,
            CTree<UpdateAssignment,bool> sg)
        {
            for (var b = sg.First(); b != null; b = b.Next())
            {
                var ua = b.key();
                if (!(finder.Contains(ua.vbl) && finder[ua.vbl] is Finder fi 
                    && fi.rowSet==defpos))
                    sg -= b.key();
            }
            return sg;
        }
        internal virtual bool Knows(Context cx,long rp)
        {
            return finder.Contains(rp) || rp == defpos 
                || (Source(cx) is RowSet rs && rs.Knows(cx,rp));
        }
        internal override DBObject AddCondition(Context cx, long prop, long cond)
        {
            var cs = (CTree<long, bool>)mem[prop] ?? CTree<long, bool>.Empty;
            if (cs.Contains(cond))
                return this;
            var sv = (SqlValue)cx.obs[cond];
            if (!sv.KnownBy(cx, this))
                return this;
            if (mem.Contains(_Source) && cx.data[(long)mem[_Source]] is RowSet sc)
            {
                if (sc.defpos>Transaction.TransPos && sv.KnownBy(cx, sc))
                {
                    var nr = (RowSet)sc.AddCondition(cx, prop, cond);
                    if (nr != sc)
                    {
                        cx.data += (nr.defpos, nr);
                        return this + (_Source, nr.defpos);
                    }
                    return this;
                }
                return this + (prop, cs + (cond, true));
            }
            return new SelectRowSet(cx, this, CTree<long,bool>.Empty+(cond, true));
        }
        internal override DBObject AddMatch(Context cx,SqlValue sv,TypedValue v)
        {
            if (matches.Contains(sv.defpos) || !finder.Contains(sv.defpos))
                return this;
            return this + (Query._Matches, matches + (sv.defpos, v));
        }
        internal virtual CTree<long,Finder> AllWheres(Context cx,CTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,where);
            if (Source(cx) is RowSet sce) 
                nd = cx.Needs(nd,this,sce.AllWheres(cx,nd));
            return nd;
        }
        internal virtual CTree<long,Finder> AllMatches(Context cx,CTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,matches);
            if (Source(cx) is RowSet sce)
                nd = cx.Needs(nd,this,sce.AllMatches(cx,nd));
            return nd;
        }
        /// <summary>
        /// Test if the given source RowSet matches the requested ordering
        /// </summary>
        /// <returns>whether the orderings match</returns>
        protected bool SameOrder(Context cx,Query q,RowSet sce)
        {
            if (rowOrder == null || rowOrder.Length == 0)
                return true;
            return rowOrder.CompareTo(sce?.rowOrder)==0;
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            int m = rt.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keys.First(); j<flags.Length && ib != null; 
                ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = rt.First(); b != null; 
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
                var cp = rt[i];
                var dc = domain.representation[cp];
                if (dc.kind == Sqlx.SENSITIVE)
                    dc = dc.elType;
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
            if (!rs.built)
            {
                var sv = (SqlValue)cx.obs[rs.needed.First().key()];
                throw new DBException("42112", sv.name);
            }
            return rs._First(cx);
        }
        public virtual Cursor Last(Context cx)
        {
            var rs = MaybeBuild(cx);
            if (!rs.built)
            {
                var sv = (SqlValue)cx.obs[rs.needed.First().key()];
                throw new DBException("42112", sv.name);
            }
            return rs._Last(cx);
        }
        /// <summary>
        /// Find a bookmark for the given key
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual Cursor PositionAt(Context _cx,PRow key)
        {
            for (var bm = _First(_cx); bm!=null; bm=bm.Next(_cx))
            {
                var k = key;
                for (var b=keys.First();b!=null;b=b.Next())
                    if (_cx.obs[b.value()].Eval(_cx).CompareTo(k._head) != 0)
                        goto next;
                return bm;
                next:;
            }
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');sb.Append(Uid(defpos));
            var cm = "(";
            var d = display-1;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = (b.key()==d)?"|":",";
                sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (needed != null && needed != CTree<long, Finder>.Empty)
            {
                sb.Append(" NEEDED: "); cm = "";
                for (var b = needed.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
            }
            if (keys!=rt && keys.Count!=0)
            {
                cm = " key (";
                for (var b = keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (rowOrder !=CList<long>.Empty)
            {
                cm = " order (";
                for (var b=rowOrder.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (where != CTree<long,bool>.Empty)
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
            if (matching != CTree<long,CTree<long,bool>>.Empty)
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
            if (groupSpec>=0)
            { sb.Append(" groupSpec: ");sb.Append(Uid(groupSpec)); }
            if (rsTargets != CTree<long,long>.Empty)
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
            if (mem.Contains(_Source))
            {
                sb.Append(" Source: "); sb.Append(Uid(source));
            }
            if (PyrrhoStart.VerboseMode)
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
        public readonly long _rowsetpos;
        public readonly int _pos;
        public readonly long _defpos;
        public readonly long _ppos;
        public readonly int display;
        public readonly CTree<long, TypedValue> _needed;
        protected Cursor(Context _cx, RowSet rs, int pos, long defpos, long ppos,
            TRow rw,BTree<long,object>m=null) :base(rs.domain,rw.values)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            _ppos = ppos;
            display = rs.display;
            var nd = CTree<long, TypedValue>.Empty;
            for (var b=rs.needed?.First();b!=null;b=b.Next())
            {
                var fi = b.value();
                nd += (b.key(), _cx.cursors[fi.rowSet][fi.col]);
            }
            _needed = nd;
            _cx.cursors += (rs.defpos, this);
            PyrrhoServer.Debug(1, GetType().Name);
        }
        protected Cursor(Context _cx, RowSet rs, int pos, long defpos, long ppos,
            TypedValue[] vs, BTree<long,object>m = null) : base(rs, vs)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            _ppos = ppos;
            display = rs.display;
            var nd = CTree<long, TypedValue>.Empty;
            for (var b = rs.needed.First(); b != null; b = b.Next())
            {
                var fi = b.value();
                nd += (b.key(), _cx.cursors[fi.rowSet][fi.col]);
            }
            _needed = nd;
            _cx.cursors += (rs.defpos, this);
        }
        protected Cursor(Context cx,Cursor cu)
            :base((Domain)cu.dataType.Fix(cx),cx.Fix(cu.values))
        {
            _rowsetpos = cx.RsUnheap(cu._rowsetpos);
            _pos = cu._pos;
            _defpos = cx.ObUnheap(cu._defpos);
            _ppos = cx.ObUnheap(cu._ppos);
            display = cu.display;
            _needed = cx.Fix(cu._needed);
            cx.cursors += (cu._rowsetpos, this);
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx,long rd,Domain dm,int pos,long defpos,long ppos,
            TRow rw,BTree<long,object>m=null) :base(dm,rw.values)
        {
            _rowsetpos = rd;
            _pos = pos;
            _defpos = defpos;
            _ppos = ppos;
            cx.cursors += (rd, this);
        }
        protected Cursor(Cursor cu,Context cx,long p,TypedValue v) 
            :base (cu.dataType,cu.values+(p,v))
        {
            _rowsetpos = cu._rowsetpos;
            _pos = cu._pos;
            _defpos = cu._defpos;
            _ppos = cu._ppos;
            display = cu.display;
            cx.cursors += (cu._rowsetpos, this);
        }
        public static Cursor operator+(Cursor cu,(Context,long,TypedValue)x)
        {
            var (cx, p, tv) = x;
            return cu.New(cx,p,tv);
        }
        protected virtual Cursor New(Context cx,long p,TypedValue v)
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
        internal void _Rvv(Context cx)
        {
            var rs = cx.data[_rowsetpos];
            var dn = rs.DbName(cx);
            var etag = cx.etags.cons[dn].rvv;
            for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                if ((rs.where == CTree<long, bool>.Empty && rs.matches == CTree<long, TypedValue>.Empty)
                    || rs.aggregates(cx))
                    etag += (b.key(), ( -1L, ((Table)cx.obs[b.key()]).lastData));
                else
                    etag += (b.key(), cx.cursors[b.value()]);
            cx.etags.cons[dn].rvv += etag;
        }
        internal bool Matches(Context cx)
        {
            var rs = cx.data[_rowsetpos];
            if (IsNull)
                return false;
            for (var b = rs.matches.First(); b != null; b = b.Next())
                if (rs.finder.Contains(b.key()))
                {
                    var fi = rs.finder[b.key()];
                    if (cx.cursors[fi.rowSet][fi.col].CompareTo(b.value()) != 0)
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
            return null; // throw new PEException("PE544");
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
        internal string NameFor(Context cx,int i)
        {
            var rs = cx.data[_rowsetpos];
            var p = rs.rt[i];
            var ob = (SqlValue)cx.obs[p];
            return ob.name;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class TrivialRowSet: RowSet
    {
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)mem[Singleton];
        internal TrivialRowSet(long dp, Context cx, TRow r, long rc=-1L, CTree<long,Finder> fi=null)
            : base(dp, cx, r.dataType,fi,
                  null,null,null,null,new BTree<long,object>(Singleton,r))
        { }
        protected TrivialRowSet(Context cx, RowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt) { }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override RowSet New(Context cx,CTree<long,Finder> nd,bool bt)
        {
            return new TrivialRowSet(cx,this,nd,bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TrivialRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override bool TableColsOk => true;
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
        internal override Basis Fix(Context cx)
        {
            var r = (TrivialRowSet)base.Fix(cx);
            var nr = row.Fix(cx);
            if (row!=nr)
                r += (Singleton, nr);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TrivialRowSet(defpos,m);
        }
        // shareable as of 26 April 2021
        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t,long d) 
                :base(_cx,t,0,0,d,t.row)
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
    /// NB we assume that the rowtype contains only simple SqlCopy expressions
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        internal const long
            RdCols = -462,  // BTree<long,bool>
            SQMap = -408; // CTree<long,Finder> SqlValue
        internal CTree<long, Finder> sQMap =>
            (CTree<long, Finder>)mem[SQMap];
        internal BTree<long, bool> rdCols =>
            (BTree<long, bool>)mem[RdCols] ?? BTree<long, bool>.Empty;
        /// <summary>
        /// This constructor builds a rowset for the given Query
        /// directly using its defpos, rowType, where and match info.
        /// q.ordSpec though is only a requested ordering, not the actual order
        /// </summary>
        internal SelectedRowSet(Context cx,Query q,RowSet r,CTree<long,Finder>fi)
            :base(q.defpos,cx,q.domain,fi,null,q.where,null,q.matches, // NB: NOT q.ordSpec
                 _Fin(cx,q,q.mem+(_Source,r.defpos)
                     +(RSTargets,new CTree<long,long>(r.target,q.defpos))
                     +(Table.LastData,r.lastData)+(Query.Assig,q.assig)))
        { }
        internal SelectedRowSet(Context cx, Domain dm, RowSet r)
            :base(cx.nextHeap++,cx,dm,null,null,null,null,null,
                 _Fin(dm,r)+(_Source,r.defpos))
        { }
        SelectedRowSet(Context cx, SelectedRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Fin(Domain d,RowSet r)
        {
            var fi = CTree<long, Finder>.Empty;
            var rb = r.rt.First();
            for (var b = d.rowType.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                fi += (b.value(), new Finder(rb.value(), r.defpos));
            return new BTree<long,object>(SQMap,fi);
        }
        static BTree<long, object> _Fin(Context cx, Query q, BTree<long,object>m)
        {
            var fi = CTree<long, Finder>.Empty;
            for (var b = q.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlCopy sc)
                    p = sc.copyFrom;
                fi += (b.value(), new Finder(p, q.defpos));
            }
            return m + (SQMap, fi);
        } 
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectedRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new SelectedRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet Review(Context cx,BTree<long,bool> skip)
        {
            if (skip.Contains(defpos))
                return base.Review(cx, skip);
            var sc = cx.data[source];
            if (rt.CompareTo(sc.rt) == 0 && target>=Transaction.Analysing)
            {
                cx.data += (defpos, sc);
                return sc;
            }
            if (cx.db.autoCommit)
                return this;
            // Construct rdCols for use in cx.rdC (see SelectedCursor)
            var rc = BTree<long, bool>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var p = sQMap[b.value()].col;
                if (b.key() < domain.display)
                    rc += (p, true);
                else
                    for (var c = where.First(); c != null; c = c.Next())
                        if (((SqlValue)cx.obs[c.key()]).Uses(cx, p))
                        {
                            rc += (p, true);
                            break;
                        }
            }
            for (var b = matches.First(); b != null; b = b.Next())
                rc += (sQMap[b.key()].col, true);
            return this + (RdCols,rc);
        }
        internal override CTree<UpdateAssignment, bool> Review(Context cx, CTree<UpdateAssignment, bool> sg)
        {
            var ags = assig;
            for (var b=sg.First();b!=null;b=b.Next())
            {
                var ua = b.key();
                if (!finder.Contains(ua.vbl))
                    sg -= b.key(); ;
                if ((!assig.Contains(ua)) && finder[ua.vbl].rowSet == defpos)
                    ags += (ua, true);
            }
            if (ags != assig)
                cx.data += (defpos, this + (Query.Assig, ags));
            return sg;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectedRowSet(dp,mem);
        }
        internal override RowSet Relocate1(Context cx)
        {
            var r = base.Relocate1(cx);
            var sq = sQMap;
            for (var b=sq.First();b!=null;b=b.Next())
            {
                var f = b.value();
                var or = f.rowSet;
                var nr = cx.rsuids[or] ?? or;
                if (nr!=or)
                    sq += (b.key(), new Finder(f.col,nr));
            }
            return r + (SQMap, sq);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectedRowSet)base._Replace(cx, so, sv);
            r += (SQMap, cx.Replaced(sQMap));
            return r;
        }
        internal override RowSet New(Context cx,CTree<long,Finder> nd,bool bt)
        {
            return new SelectedRowSet(cx,this,nd,bt);
        }
        public static SelectedRowSet operator +(SelectedRowSet rs, (long, object) x)
        {
            return (SelectedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Build(Context cx)
        {
            for (var i=0; i<rt.Length;i++)
                cx.obs[rt[i]]?.Build(cx,this);
            return built?this:New(cx,needed,true);
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return SelectedCursor.New(this, _cx);
        }
        public override Cursor PositionAt(Context cx, PRow key)
        {
            if (cx.data[source] is IndexRowSet irs)
                return new SelectedCursor(cx, this, IndexRowSet.IndexCursor.New(cx, irs, key),0);
            return base.PositionAt(cx, key);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SelectedRowSet)base.Fix(cx);
            var nm = cx.Fix(sQMap);
            if (nm != sQMap)
                r += (SQMap, nm);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SelectedRowSet)base._Relocate(wr);
            r += (SQMap, wr.Fix(sQMap));
            wr.cx.data += (r.defpos, r);
            return r;
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            var ta = (TargetActivation)((DBObject)cx.db.objects[target]).Insert(cx, fm, false, prov, cl);
            if (!iter)
                return ta;
            var data = cx.data[fm.source];
            var trs = (TransitionRowSet)ta._trs;
            for (var b = data.First(ta); b != null; b = b.Next(ta))
            {
                new TransitionRowSet.TransitionCursor(ta, trs, b.values, b._defpos, b._pos);
                ta.EachRow();
            }
            return ta.Finish();
        }
        internal override Context Update(Context cx,RowSet fm,bool iter)
        {
            var ta = (TargetActivation)((Table)cx.db.objects[target]).Update(cx,fm,false);
            if (!iter)
                return ta;
            for (var b = ta._trs.First(ta); b != null; b = b.Next(ta))
                ta.EachRow();
            return ta.Finish();
        }
        internal override Context Delete(Context cx,RowSet fm,bool iter)
        {
            var ta = (TargetActivation)((Table)cx.db.objects[target]).Delete(cx, fm,false);
            if (!iter)
                return ta;
            for (var b = ta._trs.First(ta); b != null; b = b.Next(ta))
                ta.EachRow();
            return ta.Finish();
        }
        internal override BTree<long,VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, sQMap);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
    //        if (PyrrhoStart.VerboseMode)
            {
                sb.Append(" SQMap: "); sb.Append(sQMap);
            }
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class SelectedCursor : Cursor
        {
            internal readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(cx,srs, pos, bmk._defpos, bmk._ppos, 
                      new TRow(srs.domain,srs.sQMap,bmk.values)) 
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
                _srs = (SelectedRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _bmk = cu._bmk?._Fix(cx);
            }
            internal static SelectedCursor New(Context cx,SelectedRowSet srs)
            {
                var ox = cx.finder;
                var sce = srs.Source(cx);
                cx.finder += sce.finder;
                for (var bmk = sce.First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        if (!cx.db.autoCommit)
                            cx.obs[srs.target]._ReadConstraint(cx, rb);
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal static SelectedCursor New(SelectedRowSet srs, Context cx)
            {
                var ox = cx.finder;
                var sce = srs.Source(cx);
                cx.finder += sce.finder;
                for (var bmk = sce.Last(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectedCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        if (!cx.db.autoCommit)
                            cx.obs[srs.target]._ReadConstraint(cx, rb);
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
            protected override Cursor _Next(Context _cx)
            {
                var ox = _cx.finder;
                _cx.finder += _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedCursor(_cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(_cx))
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
                _cx.finder += _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Previous(_cx); bmk != null; bmk = bmk.Previous(_cx))
                {
                    var rb = new SelectedCursor(_cx, _srs, bmk, _pos + 1);
                    if (rb.Matches(_cx))
                    {
                        _cx.finder = ox;
                        return rb;
                    }
                }
                _cx.finder = ox;
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
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Note we cannot assume that columns are simple SqlCopy.
        /// For views we should pass grouping and aggregation to the source...
        /// </summary>
        internal SelectRowSet(Context cx, QuerySpecification q, RowSet r)
            : base(q.defpos, cx, q.domain, r.finder, null, q.where, q.ordSpec, 
                  q.matches, new BTree<long,object>(_Source,r.defpos)
                  +(RSTargets,r.rsTargets)
                  +(Table.LastData,r.lastData))
        { }
        internal SelectRowSet(Context cx,RowSet r,CTree<long,bool>conds)
            :base(cx.GetUid(),cx,r.domain,r.finder,r.keys,conds,null,r.matches,
                 r.mem+(_Source,r.defpos))
        { }
        SelectRowSet(Context cx, SelectRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override RowSet New(Context cx, CTree<long, Finder> nd,bool bt)
        {
            return new SelectRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new SelectRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Review(Context cx,BTree<long,bool> skip)
        {
            if (skip.Contains(defpos))
                return base.Review(cx, skip);
            var sc = cx.data[source];
            if (rt.CompareTo(sc.rt)==0 && where.CompareTo(sc.where)==0)
            {
                cx.data += (defpos, sc);
                return sc;
            }
            return this;
        }
        internal override RowSet Build(Context cx)
        {
            for (var i=0;i<rt.Length;i++)
                cx.obs[rt[i]].Build(cx, this);
            return built?this:New(cx,needed,true);
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectCursor.New(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return SelectCursor.New(this, _cx);
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            return cx.data[source].Insert(cx, fm, iter, prov, cl);
        }
        internal override Context Delete(Context cx, RowSet fm, bool iter)
        {
            return cx.data[source].Delete(cx,fm, iter);
        }
        internal override Context Update(Context cx,RowSet fm, bool iter)
        {
            return cx.data[source].Update(cx,fm, iter);
        }
        // shareable as of 26 April 2021
        internal class SelectCursor : Cursor
        {
            readonly SelectRowSet _srs;
            readonly Cursor _bmk; // for rIn, not used directly for Eval
            SelectCursor(Context _cx, SelectRowSet srs, Cursor bmk, int pos)
                : base(_cx, srs, pos, bmk._defpos, bmk._ppos, _Row(_cx, bmk, srs))
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
                _srs = (SelectRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _bmk = cu._bmk?._Fix(cx);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectCursor(this, cx,p, v);
            }
            static TRow _Row(Context cx, Cursor bmk, SelectRowSet srs)
            {
                var ox = cx.finder;
                cx.finder += cx.data[srs.source].finder;
                var vs = CTree<long,TypedValue>.Empty;
                for (var b = srs.rt.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()]; 
                    var v = s?.Eval(cx)??TNull.Value;
                    if (v.IsNull && bmk[b.value()] is TypedValue tv
                        && !tv.IsNull)
                        v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
                    vs += (b.value(),v);
                }
                cx.finder = ox;
                return new TRow(srs.domain, vs);
            }
            internal static SelectCursor New(Context cx, SelectRowSet srs)
            {
                for (var bmk = cx.data[srs.source].First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal static SelectCursor New(SelectRowSet srs, Context cx)
            {
                for (var bmk = cx.data[srs.source].Last(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = _srs.domain.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                for (var bmk = _bmk.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = _srs.domain.representation.First(); b != null; b = b.Next())
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
    }
    // shareable as of 26 April 2021
    internal class EvalRowSet : RowSet
    {
        /// <summary>
        /// The having search condition for the aggregation
        /// </summary>
		internal CTree<long, bool> having =>
            (CTree<long, bool>)mem[TableExpression.Having] ?? CTree<long, bool>.Empty;
        internal TRow row => (TRow)mem[TrivialRowSet.Singleton];
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        /// <summary>
        /// Constructor: Build a rowSet that aggregates data from a given source.
        /// For views we should propagate aggregations from q to the source rowset rs
        /// </summary>
        /// <param name="rs">The source data</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Context cx, QuerySpecification q, RowSet rs)
            : base(q.defpos, cx, q.domain, rs.finder, null, null,
                  q.ordSpec, q.matches,
                  BTree<long, object>.Empty + (_Source, rs.defpos)
                  + (TableExpression.Having, q.where)
                  + (RowSet.RSTargets,rs.rsTargets)
                  + (Table.LastData, rs.lastData))
        {
            cx.aggregators += (q.defpos, true);
        }
        protected EvalRowSet(Context cx, EvalRowSet rs, CTree<long, Finder> nd, TRow rw)
            : base(cx, rs + (TrivialRowSet.Singleton, rw), nd, true)
        {
            cx.values += (defpos, rw);
        }
        EvalRowSet(Context cx, EvalRowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
        { }
        protected EvalRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        {  }
        internal override Basis New(BTree<long, object> m)
        {
            return new EvalRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new EvalRowSet(cx.GetUid(), m);
            cx.aggregators += (rs.defpos, true);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new EvalRowSet(cx, this, nd, bt);
        }
        public static EvalRowSet operator +(EvalRowSet rs, (long, object) x)
        {
            return (EvalRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new EvalRowSet(dp, mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return new EvalBookmark(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return new EvalBookmark(_cx, this);
        }
        internal override RowSet Build(Context _cx)
        {
            var tg = BTree<long, Register>.Empty;
            var cx = new Context(_cx);
            var sce = cx.data[source];
            cx.finder += sce.finder;
            var oi = rt;
            var k = new TRow(domain, CTree<long, TypedValue>.Empty);
            cx.data += (defpos, this);
            for (var b = rt.First(); b != null; b = b.Next())
                tg = ((SqlValue)_cx.obs[b.value()]).StartCounter(cx, this, tg);
            var ebm = sce.First(cx);
            if (ebm != null)
            {
                for (; ebm != null; ebm = ebm.Next(cx))
                    if ((!ebm.IsNull) && Query.Eval(having, cx))
                        for (var b = rt.First(); b != null; b = b.Next())
                            tg = ((SqlValue)_cx.obs[b.value()]).AddIn(cx, ebm, tg);
            }
            var cols = oi;
            var vs = CTree<long, TypedValue>.Empty;
            cx.funcs = tg;
            for (int i = 0; i < cols.Length; i++)
            {
                var s = cols[i];
                vs += (s, _cx.obs[s].Eval(cx));
            }
            return new EvalRowSet(_cx, this, needed, new TRow(domain, vs));
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            throw new DBException("42174");
        }
        internal override Context Update(Context cx,RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override Context Delete(Context cx,RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t,having, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (EvalRowSet)base.Fix(cx);
            cx.aggregators += (r.defpos, true);
            return r;
        }
        // shareable as of 26 April 2021
        internal class EvalBookmark : Cursor
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers) 
                : base(_cx, ers, 0, 0, 0, ers.row)
            {
                _ers = ers;
            }
            EvalBookmark(Context cx, EvalBookmark cu) : base(cx, cu) 
            {
                _ers = (EvalRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
            }
            EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                _ers = cu._ers;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new EvalBookmark(this,cx,p,v);
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
    /// A TableRowSet consists of all of the *accessible* TableColumns from a Table
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TableRowSet : RowSet
    {
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key.
        /// Independent of role, user, command.
        /// Context must have a suitable tr field
        /// </summary>
        internal TableRowSet(Context cx, long t,CTree<long,Finder>fi,CTree<long,bool> wh)
            : base(t, cx, cx.Inf(t).domain,fi,null,null,null,_Matches(cx,wh),
                  new BTree<long,object>(RSTargets,new CTree<long,long>(t,t))
                  +(From.Target,t)
                  +(Table.LastData,((Table)(cx.obs[t]??cx.db.objects[t])).lastData))
        { }
        TableRowSet(Context cx, TableRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new TableRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TableRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        public static TableRowSet operator+(TableRowSet rs,(long,object)x)
        {
            return (TableRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        internal override DBObject Relocate(long dp)
        {
            return new TableRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableRowSet)base._Replace(cx, so, sv);
            r += (RSTargets, cx.Replaced(rsTargets));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableRowSet)base.Fix(cx);
            var tg = rsTargets.First();
            var nk = cx.rsuids[tg.key()] ?? tg.key();
            var nt = cx.obuids[tg.value()] ?? tg.value();
            var nv = new CTree<long, long>(nk, nt);
            if (nv.CompareTo(rsTargets)!=0)
                r += (RSTargets, nv);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableRowSet)base._Relocate(wr);
            r += (RSTargets, wr.Fix(rsTargets));
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return TableCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return TableCursor.New(this, _cx);
        }
        // shareable as of 26 April 2021
        internal class TableCursor : Cursor
        {
            internal readonly Table _table;
            internal readonly TableRowSet _trs;
            internal readonly ABookmark<long, TableRow> _bmk;
            protected TableCursor(Context _cx, TableRowSet trs, Table tb, int pos, 
                ABookmark<long, TableRow> bmk) 
                : base(_cx,trs.defpos, trs.domain, pos, bmk.key(), bmk.value().ppos, 
                      _Row(trs,bmk.value()))
            {
                _bmk = bmk; _table = tb; _trs = trs;
            }
            TableCursor(TableCursor cu,Context cx, long p,TypedValue v) :base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _table = cu._table; _trs = cu._trs;
            }
            TableCursor(Context cx,TableCursor cu) :base(cx,cu)
            {
                _table = (Table)((DBObject)cx.db.objects[cx.ObUnheap(cu._table.defpos)]).Fix(cx);
                _trs = (TableRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _bmk = _table.tableRows.PositionAt(cx.ObUnheap(cu._defpos));
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableCursor(this, cx, p, v);
            }
            static TRow _Row(TableRowSet trs,TableRow rw)
            {
                var vs = CTree<long, TypedValue>.Empty;
                for (var b=trs.domain.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    vs += (p, rw.vals[p]);
                }
                return new TRow(trs.domain, vs);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs)
            {
                var table = _cx.db.objects[trs.target] as Table;
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, trs, table, 0, b);
                }
                return null;
            }
            internal static TableCursor New(TableRowSet trs, Context _cx)
            {
                var table = _cx.db.objects[trs.target] as Table;
                for (var b = table.tableRows.Last(); b != null; b = b.Previous())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, trs, table, 0, b);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var table = _table;
                for (;;) 
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    var rec = bmk.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        _cx.db._user!=table.definer && _cx.db._user!=_cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx,_trs,_table, _pos + 1, bmk);
                }
            }
            protected override Cursor _Previous(Context _cx)
            {
                var bmk = _bmk;
                var table = _table;
                for (; ; )
                {
                    if (bmk != null)
                        bmk = bmk.Previous();
                    if (bmk == null)
                        return null;
                    var rec = bmk.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db._user != table.definer && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, _trs, _table, _pos + 1, bmk);
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
        }
    }
    /// <summary>
    /// VirtualRowSet dummy - will be replaced by Instance()
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class VirtualRowSet : RowSet
    {
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key.
        /// Independent of role, user, command.
        /// f.target is a View vw but f.domain is not v.domain 
        /// So this.domain does not match the View 
        /// (VirtualRowSet is like SelectedRowSet in this respect)
        /// </summary>
        internal VirtualRowSet(Context cx, From f, CTree<long, Finder> fi)
            : base(f.defpos, cx, f.domain, fi, null, f.where, f.ordSpec, f.filter,
                  new BTree<long, object>(RSTargets, new CTree<long, long>(f.target, f.defpos))
                + (_Needed, CTree<long, Finder>.Empty)
                + (Query.Assig, f.assig))
        { }
        protected VirtualRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        protected override Cursor _First(Context _cx)
        {
            throw new NotImplementedException();
        }
        protected override Cursor _Last(Context _cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return this;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new VirtualRowSet(defpos,m);
        }

        internal override DBObject Relocate(long dp)
        {
            return new VirtualRowSet(dp,mem);
        }
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table)
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class IndexRowSet : RowSet
    {
        internal const long
            _Index = -410; // long Index
        /// <summary>
        /// The Index to use
        /// </summary>
        internal long index => (long)(mem[_Index]??-1L);
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Context cx, Table tb, Index x,
            CTree<long,TypedValue> fl,long? dp=null, BTree<long,object>m=null) 
            : base(dp??x.defpos,cx,cx.Inf(tb.defpos).domain,null,x.keys,null,null,
                  fl,(m??BTree<long,object>.Empty) + (From.Target,tb.defpos)
                  +(_Index,x.defpos)+(Table.LastData,tb.lastData))
        { }
        protected IndexRowSet(Context cx, IndexRowSet irs, CTree<long, Finder> nd, bool bt)
            : base(cx, irs, nd, bt)
        { }
        protected IndexRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new IndexRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new IndexRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new IndexRowSet(cx, this, nd, bt);
        }
        public static IndexRowSet operator+(IndexRowSet rs,(long,object)x)
        {
            return (IndexRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        internal override DBObject Relocate(long dp)
        {
            return new IndexRowSet(dp,mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (IndexRowSet)base.Fix(cx);
            var ni = cx.obuids[index] ?? index;
            if (ni != index)
                r += (_Index, ni);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (IndexRowSet)base._Replace(cx, so, sv);
            r += (_Index, cx.ObReplace(index, so, sv));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (IndexRowSet)base._Relocate(wr);
            var ch = r != this;
            r += (_Index, wr.Fix(index));
            ch = ch || r.index != index;
            if (ch)
                wr.cx.obs += (r.defpos, r);
            return r;
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            return ((Table)cx.db.objects[target]).Insert(cx, fm, iter, prov, cl);
        }
        protected override Cursor _First(Context _cx)
        {
            return IndexCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context cx)
        {
            return IndexCursor.New(this, cx);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            return IndexCursor.New(_cx,this, key);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Keys: ");
            var cm = "(";
            for (var b=keys.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class IndexCursor : Cursor
        {
            internal readonly IndexRowSet _irs;
            internal readonly MTreeBookmark _bmk;
            internal readonly TableRow _rec;
            internal readonly PRow _key;
            protected IndexCursor(Context _cx, IndexRowSet irs, int pos, MTreeBookmark bmk, TableRow trw,
                PRow key=null)
                : base(_cx, irs, pos, trw.defpos, trw.ppos, new TRow(irs.domain, trw.vals))
            {
                _bmk = bmk; _irs = irs; _rec = trw; _key = key;
            }
            protected IndexCursor(IndexCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _irs = cu._irs; _rec = cu._rec; _key = cu._key;
            }
            IndexCursor(Context cx,IndexCursor cu) :base(cx,cu)
            {
                _irs = (IndexRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _key = cu._key?.Fix(cx);
                var nx = (Index)((DBObject)cx.db.objects[cx.ObUnheap(_irs.index)]).Fix(cx);
                _bmk = nx.rows.PositionAt(_key);
                var iq = _bmk?.Value();
                _rec = (iq == null) ? null 
                    : ((Table)((DBObject)cx.db.objects[cx.ObUnheap(_irs.target)]).Fix(cx))
                    .tableRows[iq.Value];
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new IndexCursor(this, cx, p, v);
            }
            public override MTreeBookmark Mb()
            {
                return _bmk;
            }
            internal override BList<TableRow> Rec()
            {
                return new BList<TableRow>(_rec);
            }
            public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                var tb = (Table)_cx.db.objects[_irs.target];
                return new IndexCursor(_cx,_irs, _pos + 1, mb, tb.tableRows[mb.Value().Value]);
            }
            internal static IndexCursor New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = (Table)_cx.db.objects[_irs.target];
                var index = (Index)_cx.db.objects[_irs.index];
                for (var bmk = index.rows.PositionAt(key); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new IndexCursor(_cx,_irs, 0, bmk, rec, key);
                }
                return null;
            }
            internal static IndexCursor New(IndexRowSet irs, Context _cx, PRow key = null)
            {
                var _irs = irs;
                var table = (Table)_cx.db.objects[_irs.target];
                var index = (Index)_cx.db.objects[_irs.index];
                for (var bmk = index.rows.PositionAt(key); bmk != null;
                    bmk = bmk.Previous())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new IndexCursor(_cx, _irs, 0, bmk, rec, key);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = (Table)_cx.db.objects[_irs.target];
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != _table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new IndexCursor(_cx, _irs, _pos + 1, bmk, rec);
                }
            }
            protected override Cursor _Previous(Context _cx)
            {
                var bmk = _bmk;
                var _table = (Table)_cx.db.objects[_irs.target];
                for (; ; )
                {
                    bmk = bmk.Previous();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != _table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new IndexCursor(_cx, _irs, _pos + 1, bmk, rec);
                }
            }
            internal override Cursor _Fix(Context cx)
            {
                return new IndexCursor(cx, this);
            }
        }
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table) with a constant filter
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class FilterRowSet : IndexRowSet
    {
        internal const long
            IxFilter = -411; // PRow
        internal PRow filter => (PRow)mem[IxFilter];
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal FilterRowSet(Context cx, Table tb, Index x, PRow filt)
            : this(cx, tb, x, cx.GetUid(), filt)
        { }
        FilterRowSet(Context cx, Table tb, Index x, long dp, PRow filt)
            : base(cx, tb, x, null, dp, BTree<long, object>.Empty
                  + (IxFilter, filt) + (RSTargets, new CTree<long, long>(tb.defpos, dp))
                  + (From.Target, tb.defpos) + (Table.LastData, tb.lastData))
        { }
        FilterRowSet(Context cx, FilterRowSet irs, CTree<long, Finder> nd, bool bt)
            : base(cx, irs, nd, bt)
        { }
        protected FilterRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new FilterRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new FilterRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new FilterRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        public static FilterRowSet operator +(FilterRowSet rs, (long, object) x)
        {
            return (FilterRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        internal override DBObject Relocate(long dp)
        {
            return new FilterRowSet(dp, mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (FilterRowSet)base.Fix(cx);
            var f = r.filter.Fix(cx);
            if (f != filter)
                r += (IxFilter, f);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (FilterRowSet)base._Relocate(wr);
            var f = wr.Fix(r.filter);
            if (f != filter)
                r += (IxFilter, f);
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return FilterCursor.New(_cx, this);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Cursor PositionAt(Context _cx, PRow key)
        {
            return FilterCursor.New(_cx, this, key);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Filter ("); sb.Append(filter); sb.Append(")");
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class FilterCursor : IndexCursor
        {
            internal readonly FilterRowSet _frs;
            FilterCursor(Context _cx, FilterRowSet frs, int pos, MTreeBookmark bmk, 
                TableRow trw, PRow key = null)
                : base(_cx, frs, pos, bmk, trw, frs.filter)
            {
                _frs = frs;
            }
            FilterCursor(FilterCursor cu, Context cx, long p, TypedValue v) 
                : base(cu, cx, p, v)
            {
               _frs = cu._frs; 
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new FilterCursor(this, cx, p, v);
            }
            public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                var tb = (Table)_cx.db.objects[_frs.target];
                return new FilterCursor(_cx, _frs, _pos + 1, mb, 
                    tb.tableRows[mb.Value().Value]);
            }
            internal static FilterCursor New(Context _cx, FilterRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = (Table)_cx.db.objects[_irs.target];
                var index = (Index)_cx.db.objects[_irs.index];
                for (var bmk = index.rows.PositionAt(key ?? irs.filter); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new FilterCursor(_cx, _irs, 0, bmk, rec, key ?? irs.filter);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = (Table)_cx.db.objects[_frs.target];
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new FilterCursor(_cx, _frs, _pos + 1, bmk, rec);
                }
            }
        }
    }
    /// <summary>
    /// A rowset for distinct values
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        internal MTree mtree => (MTree)mem[Index.Tree];
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r) 
            : base(_cx.GetUid(),_cx,r.domain,r.finder,r.keys,
                  r.where,null,null,
                  new BTree<long,object>(_Source,r.defpos)
                  +(Table.LastData,r.lastData))
        { }
        protected DistinctRowSet(Context cx,DistinctRowSet rs,CTree<long,Finder> nd,MTree mt) 
            :base(cx,rs+(Index.Tree,mt),nd,true)
        { }
        DistinctRowSet(Context cx, DistinctRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DistinctRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long,Finder> nd, bool bt)
        {
            return new DistinctRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new DistinctRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static DistinctRowSet operator+(DistinctRowSet rs,(long,object)x)
        {
            return (DistinctRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override DBObject Relocate(long dp)
        {
            return new DistinctRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (DistinctRowSet)base._Replace(cx, so, sv);
            r += (Index.Tree, mtree.Replaced(cx,so,sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (DistinctRowSet)base.Fix(cx);
            if (mtree!=null)
               r += (Index.Tree, new MTree(mtree.info.Fix(cx)));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (DistinctRowSet)base._Relocate(wr);
              if (mtree != null)
                r += (Index.Tree, new MTree(mtree.info.Relocate(wr)));
            return r;
        }
        internal override RowSet Build(Context cx)
        {
            var ds = BTree<long,DBObject>.Empty;
            var sce = cx.data[source];
            for (var b = sce.keys.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                ds += (sv.defpos,sv);
            }
            var mt = new MTree(new TreeInfo(sce.keys, ds, TreeBehaviour.Ignore, TreeBehaviour.Ignore));
            var rs = BTree<int, TRow>.Empty;
            for (var a = sce.First(cx); a != null; a = a.Next(cx))
            {
                var vs = BList<TypedValue>.Empty;
                for (var ti = mt.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref mt, new PRow(vs), 0);
                rs += ((int)rs.Count, a);
            }
            return new DistinctRowSet(cx,this,needed,mt);
        }

        protected override Cursor _First(Context cx)
        {
            return DistinctCursor.New(cx,this);
        }
        protected override Cursor _Last(Context cx)
        {
            return DistinctCursor.New(this, cx);
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            throw new DBException("42174");
        }
        internal override Context Update(Context cx,RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        internal override Context Delete(Context cx,RowSet fm, bool iter)
        {
            throw new DBException("42174");
        }
        // shareable as of 26 April 2021
        internal class DistinctCursor : Cursor
        {
            readonly DistinctRowSet _drs;
            readonly MTreeBookmark _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,0,0,new TRow(drs.domain,bmk.key()))
            {
                _bmk = bmk;
                _drs = drs;
            }
            DistinctCursor(Context cx,DistinctCursor cu)
                :base(cx,cu)
            {
                _drs = (DistinctRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _bmk = _drs.mtree.PositionAt(cu._bmk.key()?.Fix(cx));
            }
            internal static DistinctCursor New(Context cx,DistinctRowSet drs)
            {
                var ox = cx.finder;
                cx.finder += cx.data[drs.source].finder;
                if (drs.mtree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.mtree.First(); bmk != null; bmk = bmk.Next()) 
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
                cx.finder += cx.data[drs.source].finder;
                if (drs.mtree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
                for (var bmk = drs.mtree.Last(); bmk != null; bmk = bmk.Previous())
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
                cx.finder += cx.data[_drs.source].finder;
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
                cx.finder += cx.data[_drs.source].finder;
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
        internal RTree tree => (RTree)mem[_RTree];
        internal bool distinct => (bool)(mem[QuerySpecification.Distinct]??false);
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        public OrderedRowSet(Context _cx,RowSet r,CList<long> os,bool dct)
            :base(_cx.GetUid(), _cx, r.domain,r.finder,os,r.where,
                 os,r.matches,new BTree<long,object>(_Source,r.defpos)
                 +(RSTargets,r.rsTargets)
                 +(Table.LastData, r.lastData)
                 +(QuerySpecification.Distinct,dct))
        { }
        protected OrderedRowSet(Context cx,OrderedRowSet rs,CTree<long,Finder> nd,RTree tr) 
            :base(cx,rs+(_RTree,tr),nd,true)
        { }
        OrderedRowSet(Context cx, OrderedRowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
        { }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new OrderedRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new OrderedRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        public static OrderedRowSet operator+(OrderedRowSet rs,(long,object)x)
        {
            return (OrderedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Review(Context cx,BTree<long,bool>skip)
        {
            if (skip.Contains(defpos))
                return base.Review(cx, skip);
            var sc = cx.data[source];
            var match = true; // if true, all of the ordering columns are constant
            for (var b = rowOrder.First(); match && b != null; b = b.Next())
                if (!matches.Contains(b.value()))
                    match = false;
            if (match || (sc is SelectedRowSet && rowOrder.CompareTo(sc.rowOrder) == 0)
                    || (sc is IndexRowSet irs && rowOrder.CompareTo(irs.keys) == 0))
            {
                cx.data += (defpos, sc);
                return sc;
            }
            return this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new OrderedRowSet(dp, mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (OrderedRowSet)base.Fix(cx);
            var nt = tree?.Fix(cx);
            if (nt!=tree)
                r += (_RTree, nt);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (OrderedRowSet)base._Relocate(wr);
            if (tree != null)
                r += (_RTree, tree.Relocate(wr));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (OrderedRowSet)base._Replace(cx, so, sv);
            if (tree != null)
                r += (_RTree, tree.Replace(cx, so, sv));
            return r;
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, rowOrder, VIC.RK | VIC.OV);
            return base.Scan(t);
        }
        internal override RowSet Build(Context cx)
        {
            var _cx = new Context(cx);
            var sce = cx.data[source];
            _cx.finder += sce.finder;
            var dm = new Domain(Sqlx.ROW,cx, keys);
            var tree = new RTree(sce.defpos,keys, dm, 
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = sce.First(_cx); e != null; e = e.Next(_cx))
            {
                var vs = CTree<long,TypedValue>.Empty;
                for (var b = rowOrder.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()];
                    vs += (s.defpos,s.Eval(_cx));
                }
                var rw = new TRow(dm, vs);
                RTree.Add(ref tree, rw, _cx.cursors);
            }
            return new OrderedRowSet(cx,this,needed,tree);
        }
        protected override Cursor _First(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(_cx, tree));
        }
        protected override Cursor _Last(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(tree, _cx));
        }
        // shareable as of 26 April 2021
        internal class OrderedCursor : Cursor
        {
            readonly OrderedRowSet _ors;
            readonly RTreeBookmark _rb;
            internal OrderedCursor(Context cx,OrderedRowSet ors,RTreeBookmark rb)
                :base(cx,ors,rb._pos,rb._defpos,rb._ppos,rb)
            {
                cx.cursors += rb._cs; // for updatabale joins
                _ors = ors; _rb = rb;
            }
            OrderedCursor(Context cx,OrderedCursor cu):base(cx,cu)
            {
                _ors = (OrderedRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _rb = (RTreeBookmark)_rb?._Fix(cx);
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
        internal EmptyRowSet(long dp, Context cx,Domain dm) 
            : base(dp, new BTree<long,object>(_Domain,dm)) 
        {
            cx.data += (dp, this);
        }
        EmptyRowSet(Context cx, RowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) { }
        protected EmptyRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new EmptyRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new EmptyRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new EmptyRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
            : base(dp, cx, xp,null, null,null,null,null,
                  new BTree<long,object>(SqlRows,rs))
        { }
        SqlRowSet(Context cx, SqlRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new SqlRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.TransPos)
                return (RowSet)New(m);
            var rs = new SqlRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
            var r = (SqlRowSet)base._Replace(cx, so, sv);
            r += (SqlRows, cx.Replaced(sqlRows));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlRowSet)base.Fix(cx);
            var nw = cx.Fix(sqlRows);
            if (nw!=sqlRows)
            r += (SqlRows, nw);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlRowSet)base._Relocate(wr);
            r += (SqlRows, wr.Fix(sqlRows));
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
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?>t)
        {
            t = Scan(t, sqlRows, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        // shareable as of 26 April 2021
        internal class SqlCursor : Cursor
        {
            readonly SqlRowSet _srs;
            readonly ABookmark<int, long> _bmk;
            SqlCursor(Context cx,SqlRowSet rs,int pos,ABookmark<int,long> bmk)
                : base(cx,rs,pos,cx.GetUid(),0,_Row(cx,rs,bmk.key()))
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
                var rowType = new Domain(Sqlx.ROW, rs.domain.mem);
                return (TRow)rowType.Coerce(cx,cx.obs[rs.sqlRows[p]].Eval(cx));
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
    // shareable as of 26 April 2021
    internal class TableExpRowSet : RowSet
    {
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal TableExpRowSet(long dp, Context cx, long tgt,Domain dm, CList<long> ks, 
            RowSet sc,CTree<long, bool> wh, CTree<long, TypedValue> ma,CTree<long,Finder> fi)
            : base(dp, cx, dm, _Fin(fi,sc), ks, 
                  wh, sc.rowOrder, ma,
                  new BTree<long,object>(_Source,sc.defpos)
                  +(RSTargets,sc.rsTargets)
                  +(Table.LastData,sc.lastData)) 
        { }
        TableExpRowSet(Context cx, TableExpRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
        protected TableExpRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableExpRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TableExpRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        static CTree<long,Finder> _Fin(CTree<long,Finder> fi,RowSet sc)
        {
            for (var b = sc.finder.First(); b != null; b = b.Next())
                if (!fi.Contains(b.key()))
                    fi += (b.key(), b.value());
            return fi;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd,bool bt)
        {
            return new TableExpRowSet(cx, this, nd, bt);
        }
        public static TableExpRowSet operator+(TableExpRowSet rs,(long,object)x)
        {
            return (TableExpRowSet)rs.New(rs.mem + x);
        }
        internal override bool Knows(Context cx, long rp)
        {
            if (rp == countStar)
                return true;
            return base.Knows(cx, rp);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableExpRowSet(dp,mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return TableExpCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return TableExpCursor.New(this, _cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_CountStar))
            {
                sb.Append(" CountStar="); sb.Append(Uid(countStar));
            }
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class TableExpCursor : Cursor
        {
            internal readonly TableExpRowSet _trs;
            internal readonly Cursor _bmk;
            TableExpCursor(Context cx,TableExpRowSet trs,Cursor bmk,int pos) 
                :base(cx,trs,pos,bmk._defpos,bmk._ppos,_Row(trs,bmk))
            {
                _trs = trs;
                _bmk = bmk;
            }
            TableExpCursor(TableExpCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _trs = cu._trs;
                _bmk = cu._bmk;
            }
            TableExpCursor(Context cx,TableExpCursor cu) :base(cx,cu)
            {
                _trs = (TableExpRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
                _bmk = cu._bmk?._Fix(cx);
            }
            static TRow _Row(TableExpRowSet t,Cursor bmk)
            {
                var v = CTree<long, TypedValue>.Empty;
                var s = t.countStar;
                for (var b = t.domain.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (p!=s)
                        v += (p, bmk[p] ?? TNull.Value);
                }
                return new TRow(t.domain, v);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableExpCursor(this, cx, p, v);
            }
            public static TableExpCursor New(Context cx,TableExpRowSet trs)
            {
                var ox = cx.finder;
                var sce = cx.data[trs.source];
                cx.finder += sce.finder;
                for (var bmk=sce.First(cx);bmk!=null;bmk=bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, trs, bmk, 0);
                    if (rb.Matches(cx) && Eval(trs.where, cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            public static TableExpCursor New(TableExpRowSet trs, Context cx)
            {
                var ox = cx.finder;
                var sce = cx.data[trs.source];
                cx.finder += sce.finder;
                for (var bmk = sce.Last(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new TableExpCursor(cx, trs, bmk, 0);
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
                cx.finder += cx.data[_trs.source].finder;
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, _trs, bmk, _pos+1);
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
                cx.finder += cx.data[_trs.source].finder;
                for (var bmk = _bmk.Previous(cx); bmk != null; bmk = bmk.Previous(cx))
                {
                    var rb = new TableExpCursor(cx, _trs, bmk, _pos + 1);
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
                return new TableExpCursor(cx, this);
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
    (BList<(long, TRow)>)mem[ExplicitRowSet.ExplRows];
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Domain dt,BList<(long,TRow)>r)
            : base(dp, _Fin(dp,dt)+(ExplRows,r)+(_Domain,dt))
        { }
        ExplicitRowSet(Context cx, ExplicitRowSet rs, CTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
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
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new ExplicitRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd,bool bt)
        {
            return new ExplicitRowSet(cx, this, nd, bt);
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
            var r = (ExplicitRowSet)base._Replace(cx, so, sv);
            var s = BList<(long,TRow)>.Empty;
            for (var b=explRows.First();b!=null;b=b.Next())
            {
                var (p, q) = b.value();
                s += (p, (TRow)q.Replaced(cx));
            }
            r += (ExplRows, s);
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
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?>t)
        {
            t = Scan(t, explRows, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var r = base.ToString();
            if (rows.Count<10)
            {
                var sb = new StringBuilder(r);
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
                r = sb.ToString();
            }
            return r;
        }
        // shareable as of 26 April 2021
        internal class ExplicitCursor : Cursor
        {
            readonly ExplicitRowSet _ers;
            readonly ABookmark<int,(long,TRow)> _prb;
            ExplicitCursor(Context _cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(_cx,ers,pos,prb.value().Item1,
                     prb.value().Item2[LastChange]?.ToLong()??0L,
                     prb.value().Item2.ToArray())
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
                _ers = (ExplicitRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
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
        internal ProcRowSet(long dp, SqlCall ca, Context cx) :base(dp,cx,ca.domain,null,null,
            null,null,null,BTree<long,object>.Empty+(_Needed,CTree<long,Finder>.Empty)
            +(Built,true)+(SqlCall.Call,ca))
        { }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        ProcRowSet(Context cx,ProcRowSet prs,CTree<long,Finder>nd,bool bt) :base(cx,prs,nd,bt)
        { }
        public static ProcRowSet operator+(ProcRowSet p,(long,object)x)
        {
            return (ProcRowSet)p.New(p.mem + x);
        }
        internal override RowSet Build(Context cx)
        {
            cx.values += (defpos,call.Eval(cx));
            return this;
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
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new ProcRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new ProcRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
            var r = (ProcRowSet)base._Replace(cx, so, sv);
            r += (SqlCall.Call, cx.done[call.defpos]??call);
            return r;
        }
        internal override BTree<long,VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, call.call, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Call: "); sb.Append(call);
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class ProcRowSetCursor : Cursor
        {
            readonly ProcRowSet _prs;
            readonly ABookmark<int, TypedValue> _bmk;
            ProcRowSetCursor(Context cx,ProcRowSet prs,int pos,
                ABookmark<int,TypedValue>bmk, TRow rw) 
                : base(cx,prs,bmk.key(),-1,0,new TRow(rw,prs)) 
            { 
                _prs = prs; _bmk = bmk;
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
    /// Deal with execution of Triggers. The main complication here is that 
    /// triggers execute with their definer's role 
    /// (Many roles can be involved in defining table, columns, etc).
    /// Each trigger therefore has its own context and its own cursors for the TRS.
    /// This target row type is used for the OLD/NEW ROW/TABLE.
    /// Another nuisance is that the TransitionRowSet must manage autokeys, 
    /// as it may be preparing many new rows in the transaction context.
    /// The rowtype for the TRS itself is the rowType of the source rowset,
    /// which for INSERT comes from the values to be inserted, and for others
    /// from the rowset's version of the destination table.
    /// The target activation has the current execution role, which in
    /// general may be different from any of the trigger activation roles.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal const long
            _Adapters = -429, // Adapters
            _Data = -368, // RowSet
            Defaults = -415, // BTree<long,TypedValue>  SqlValue
            ForeignKeys = -392, // BTree<long,bool>  Index
            IxDefPos = -420, // long    Index
            TargetTrans = -418, // CTree<long,Finder>   TableColumn
            TransTarget = -419, // CTree<long,Finder>   SqlValue
            TrsFrom = -416; // From (SqlInsert, QuerySearch or UpdateSearch)
        internal BTree<long, TypedValue> defaults =>
            (BTree<long, TypedValue>)mem[Defaults] ?? BTree<long, TypedValue>.Empty;
        internal RowSet data => (RowSet)mem[_Data];
        internal CTree<long, Finder> targetTrans =>
            (CTree<long, Finder>)mem[TargetTrans];
        internal CTree<long, Finder> transTarget =>
            (CTree<long, Finder>)mem[TransTarget];
        internal BTree<long, bool> foreignKeys =>
            (BTree<long, bool>)mem[ForeignKeys] ?? BTree<long, bool>.Empty;
        internal long indexdefpos => (long)(mem[IxDefPos] ?? -1L);
        internal Adapters _eqs => (Adapters)mem[_Adapters];
        internal TransitionRowSet(TargetActivation cx, RowSet fm)
            : base(cx.GetUid(), cx, fm.domain,
                  fm?.finder,
                  null, fm?.where, null, null, 
                  _Mem(cx.GetPrevUid(), cx, fm))
        {
            cx.data += (defpos, this);
        }
        static BTree<long, object> _Mem(long defpos, TargetActivation cx, RowSet fm)
        {
            var m = new BTree<long, object>(_Data,(cx._tty==PTrigger.TrigType.Insert)?fm.Source(cx):fm);
            var tr = cx.db;
            var tgTn = CTree<long, Finder>.Empty;
            var tnTg = CTree<long, Finder>.Empty;
            var ta = fm.rsTargets.First().key();
            m += (From.Target, ta);
            m += (RSTargets, fm.rsTargets);
            var t = tr.objects[ta] as Table;
            m += (IxDefPos, t?.FindPrimaryIndex(tr)?.defpos ?? -1L);
            var ti = (ObInfo)tr.schema.infos[ta];
            // check now about conflict with generated columns
            if (t!=null && t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", ti.name);
            var rt = fm.rt;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = p;
                while (cx.obs[c] is SqlCopy sc)
                    c = sc.copyFrom;
                tgTn += (c, new Finder(p, defpos));
                tnTg += (p, new Finder(c, ta));
            }
            m += (TargetTrans, tgTn);
            m += (TransTarget, tnTg);
            var dfs = BTree<long, TypedValue>.Empty;
            if (cx._tty != PTrigger.TrigType.Delete)
            {
                for (var b = ti.domain.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    cx.finder += (p, new Finder(p, defpos));
                    if (tr.objects[p] is TableColumn tc) // i.e. not remote
                    {
                        var tv = tc.defaultValue ?? tc.domain.defaultValue;
                        if (!tv.IsNull)
                        {
                            dfs += (tc.defpos, tv);
                            cx.values += (tc.defpos, tv);
                        }
                        for (var c = tc.constraints.First(); c != null; c = c.Next())
                        {
                            cx.Frame(c.key());
                            for (var d = tc.domain.constraints.First(); d != null; d = d.Next())
                                cx.Frame(d.key());
                        }
                        cx.Frame(tc.generated.exp, tc.defpos);
                    }
                }
            }
            var fk = BTree<long, bool>.Empty;
            for (var b = t.indexes.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var ix = (Index)cx.db.objects[p];
                if (ix.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                    fk += (p, true);
            }
            m += (ForeignKeys, fk);
            m += (Defaults, dfs);
            return m;
        }
        protected TransitionRowSet(Context cx, TransitionRowSet rs, CTree<long, Finder> nd,
            bool bt) : base(cx, rs, nd, bt)
        { }
        protected TransitionRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new TransitionRowSet(cx, this, nd, bt);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TransitionRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override bool TableColsOk => true;
        internal override DBObject Relocate(long dp)
        {
            return new TransitionRowSet(dp, mem);
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionCursor.New((TargetActivation)_cx, this);
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException(); // never
        }
        internal TransitionCursor At(Context cx,long dp)
        {
            for (var b = First(cx); b != null; b = b.Next(cx))
                if (b._defpos == dp)
                    return (TransitionCursor)b;
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" From: "); sb.Append(Uid(target));
            if (data != null)
            { sb.Append(" Data: "); sb.Append(Uid(data.defpos)); }
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TargetCursor _tgc;
            TransitionCursor(TargetActivation ta, TransitionRowSet trs, Cursor fbm, int pos)
                : base(ta.next, trs, pos, fbm._defpos, fbm._ppos, new TRow(fbm,trs))
            {
                _trs = trs;
                _fbm = fbm;
                _tgc = TargetCursor.New(ta, this,true);
                var cx = ta.next;
                cx.values += (values, false);
                cx.values += (_defpos, this);
            }
            TransitionCursor(TransitionCursor cu, TargetActivation cx, long p, TypedValue v)
                : base(cu, cx,
                     cu._trs.targetTrans.Contains(p) ?
                        cu._trs.targetTrans[p].col
                        : p,
                     v)
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _tgc = TargetCursor.New(cx, this,false);
                cx.values += (p, v);
                cx.values += (_defpos, this);
            }
            internal TransitionCursor(TargetActivation ta, TransitionRowSet trs,
                CTree<long,TypedValue> vs,long dpos, int pos)
                :base(ta.next,trs,pos,-1L,-1L,_Row(trs,vs))
            {
                _trs = trs;
                _fbm = null;
                _tgc = TargetCursor.New(ta,this,true);
                var cx = ta.next;
                cx.values += (values, false);
                cx.values += (_defpos, this);
            }
            static TRow _Row(TransitionRowSet trs,CTree<long,TypedValue> vs)
            {
                var nv = CTree<long, TypedValue>.Empty;
                var db = trs.data.rt.First();
                for (var b = trs.rt.First(); b != null && db != null; b = b.Next(), db = db.Next())
                    nv += (b.value(), vs[db.value()]);
                return new TRow(trs.domain, nv);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new TransitionCursor(this, (TargetActivation)cx, p, v);
            }
            public static TransitionCursor operator +(TransitionCursor cu,
                (TargetActivation, long, TypedValue) x)
            {
                var (cx, p, tv) = x;
                return new TransitionCursor(cu, cx, p, tv);
            }
            internal static TransitionCursor New(TargetActivation _cx, TransitionRowSet trs)
            {
                var ox = _cx.finder;
                var sce = trs.data; //_cx.data[trs.from.defpos];
                _cx.finder += sce?.finder;
                for (var fbm = sce?.First(_cx); fbm != null;
                    fbm = fbm.Next(_cx))
                    if (fbm.Matches(_cx) && Eval(trs.where, _cx))
                    {
                        var r = new TransitionCursor(_cx, trs, fbm, 0);
                        _cx.finder = ox;
                        return r;
                    }
                _cx.finder = ox;
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.finder;
                cx.finder += cx.data[_fbm._rowsetpos].finder;
                if (cx.obs[_trs.where.First()?.key() ?? -1L] is SqlValue sv && sv.domain.kind == Sqlx.CURRENT)
                    return null;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = new TransitionCursor((TargetActivation)cx, _trs, fbm, _pos+1);
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
            internal readonly TableRow _rec;
            TargetCursor(TargetActivation cx, Cursor trc, ObInfo ti, CTree<long,TypedValue> vals)
                : base(cx, cx._trs.defpos, ti.domain, trc._pos, trc._defpos, trc._ppos, 
                      new TRow(ti.domain,vals)) 
            { 
                _trs = (TransitionRowSet)cx._trs; _ti = ti;
                var t = (Table)cx.db.objects[cx._trs.target];
                var p = trc._defpos;
                for (var b = cx.cursors.First(); p == 0L && b != null; b = b.Next())
                {
                    var rs = cx.data[b.key()];
                    if (rs.target == t.defpos)
                        p = b.value()._defpos;
                }
                _rec = t.tableRows[p]?? new TableRow(t, this);
                cx.values += (values, false);
                cx.values += (_defpos, this);
            }
            public static TargetCursor operator+(TargetCursor tgc,(Context,long,TypedValue)x)
            {
                var (cx, p, tv) = x;
                var tc = (TargetActivation)cx;
                if (tgc.dataType.representation.Contains(p))
                    return new TargetCursor(tc, tgc, tgc._ti, tgc.values + (p, tv));
                else 
                    return tgc;
            }
            internal static TargetCursor New(TargetActivation cx, TransitionCursor trc,
                bool check)
            {
                if (trc == null)
                    return null;
                var vs = CTree<long, TypedValue>.Empty;
                var trs = trc._trs;
                var t = (Table)cx.db.objects[trc._trs.target];
                var ro = (Role)cx.db.objects[t.definer];
                var ti = (ObInfo)ro.infos[t.defpos];
                var rt = ti.domain.rowType;
                for (var b = rt.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    var tp = trc._trs.targetTrans[p].col;
                    if (trc[tp] is TypedValue v)
                        vs += (p, v);
                }
                vs = CheckPrimaryKey(cx, trc, vs);
                cx.values += vs;
                var oc = cx.finder;
                for (var b = rt.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.value()] is TableColumn tc)
                    {
                        if (check)
                            switch (tc.generated.gen)
                            {
                                case Generation.Expression:
                                    if (vs[tc.defpos] is TypedValue vt && !vt.IsNull)
                                        throw new DBException("0U000", cx.Inf(tc.defpos).name);
                                    cx.finder = oc;
                                    cx.Frame(tc.generated.exp, tc.defpos);
                                    cx.values += vs;
                                    var v = cx.obs[tc.generated.exp].Eval(cx);
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
                            var ck = (Check)cx.db.objects[cp];
                            cx.obs += (ck.defpos, ck);
                            cx.Frame(ck.search, ck.defpos);
                            cx.finder += trs.targetTrans;
                            var se = (SqlValue)cx.obs[ck.search];
                            if (se.Eval(cx) != TBool.True)
                                throw new DBException("22212", cx.Inf(tc.defpos).name);
                        }
                    }
                var r = new TargetCursor(cx, trc, ti, vs);
                for (var b = trc._trs.foreignKeys.First(); b != null; b = b.Next())
                {
                    var ix = (Index)cx.db.objects[b.key()];
                    var rx = (Index)cx.db.objects[ix.refindexdefpos];
                    var k = ix.MakeKey(r._rec.vals);
                    var ap = ix.adapter;
                    if (ap>=0)
                    {
                        var ad = (Procedure)cx.db.objects[ap];
                        cx = (TargetActivation)ad.Exec(cx, ix.keys);
                        k = new PRow((TRow)cx.val);
                    }
                    if (!rx.rows.Contains(k))
                        throw new DBException("23000","missing foreign key "
                            + cx.Inf(ix.tabledefpos).name+k.ToString());
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
                if (ta?.index is Index ix)
                {
                    var k = BList<TypedValue>.Empty;
                    for (var b = ix.keys.First(); b != null; b = b.Next())
                    {
                        var tc = (TableColumn)cx.obs[b.value()];
                        var v = vs[tc.defpos];
                        if (v == null || v.IsNull)
                        {
                            if (tc.domain.kind != Sqlx.INTEGER)
                                throw new DBException("22004");
                            v = ix.rows.NextKey(k, 0, b.key());
                            if (v.IsNull)
                                v = new TInt(0);
                            vs += (tc.defpos, v);
                            var pk = ix.MakeKey(vs);
                            ta.index += (Index.Tree, ix.rows + (pk, -1L));
                        }
                        k += v;
                    }
                }
                return vs;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                while (!(cx is TargetActivation))
                    cx = cx.next;
                var trc = (TransitionCursor)cx.next.cursors[_trs.defpos];
                trc += ((TargetActivation)cx,_trs.targetTrans[p].col, v);
                cx.next.cursors += (_trs.defpos, trc);
                return trc._tgc;
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
            internal void Validate(Database db,THttpDate st,Rvv rv)
            {
                if (st == null && rv == null)
                    return;
                var t = (Table)db.objects[_trs.target];
                var tr = t.tableRows[_defpos]; 
                if (st!=null)
                {
                    var delta = st.milli ? 10000 : 10000000;
                    if (tr.time > st.value.Value.Ticks + delta)
                        throw new DBException("40029", _defpos);
                }
                var rp = rv?[_trs.target]?[_defpos];
                if (rp != null && tr.ppos > rp.Value)
                    throw new DBException("40029", _defpos);
            }
        }
        // shareable as of 26 April 2021
        internal class TriggerCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly TargetCursor _tgc;
            internal TriggerCursor(TriggerActivation cx, Cursor tgc,CTree<long,TypedValue> vals=null)
                : base(cx, cx._trs.defpos, cx._trig.domain, tgc._pos, tgc._defpos, tgc._ppos,
                      new TRow(cx._trig.domain, cx.trigTarget, vals??tgc.values))
            {
                _trs = cx._trs;
                _tgc = (TargetCursor)tgc;
                cx.values += (tgc._defpos, this);
                cx.values += values;
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
            Map = -433,         // CTree<long,Finder>   TableColumn
            RMap = -434,        // CTree<long,Finder>   TableColumn
            Trs = -431;         // TransitionRowSet
        internal TransitionRowSet _trs => (TransitionRowSet)mem[Trs];
        internal BTree<long,TableRow> data => 
            (BTree<long,TableRow>)mem[Data]??BTree<long,TableRow>.Empty;
        internal CTree<long, Finder> map =>
            (CTree<long, Finder>)mem[Map];
        internal CTree<long,Finder> rmap =>
            (CTree<long, Finder>)mem[RMap];
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
            : base(dp, cx, dm, null, null,null,null,null,_Mem(cx, dp,trs, dm, old)
                  + (RSTargets, trs.rsTargets) + (From.Target,trs.target))
        { }
        internal TransitionTableRowSet(Context cx,TransitionTableRowSet rs,
            CTree<long,Finder>nd,bool bt) :base(cx,rs,nd,bt)
        { }
        protected TransitionTableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,long dp,TransitionRowSet trs,Domain dm,bool old)
        {
            var dat = BTree<long, TableRow>.Empty;
            if ((!old) && cx.newTables.Contains(trs.defpos))
                dat = cx.newTables[trs.defpos];
            else
                for (var b = trs.First(cx); b != null; b = b.Next(cx))
                    for (var c=b.Rec().First();c!=null;c=c.Next())
                        dat += (b._defpos, c.value());
            var fi = CTree<long, Finder>.Empty;
            var ma = CTree<long, Finder>.Empty;
            var rm = CTree<long, Finder>.Empty;
            var rb = trs.rt.First();
            for (var b=dm.rowType.First();b!=null&&rb!=null;b=b.Next(),rb=rb.Next())
            {
                var p = b.value();
                var q = rb.value();
                var f = trs.transTarget[q];
                ma += (f.col, new Finder(p,dp));
                rm += (p, f);
                fi += (p, new Finder(trs.finder[q].col,trs.defpos));
            }
            return BTree<long,object>.Empty + (TransitionTable.Old,old)
                + (Trs,trs) + (Data,dat) + (Map,ma)
                +(RMap,rm) + (_Finder,fi);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTableRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new TransitionTableRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TransitionTableRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        public static TransitionTableRowSet operator+(TransitionTableRowSet rs,(long,object)x)
        {
            return (TransitionTableRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionTableRowSet(dp,mem);
        }
        internal override RowSet Source(Context cx)
        {
            return _trs.Source(cx);
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException(); // never
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(old ? " OLD" : " NEW");
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class TransitionTableCursor : Cursor
        {
            internal readonly ABookmark<long, TableRow> _bmk;
            internal readonly TransitionTableRowSet _tt;
            TransitionTableCursor(Context cx,TransitionTableRowSet tt,
                ABookmark<long,TableRow> bmk,int pos)
                :base(cx,tt,pos,bmk.key(),bmk.value().ppos,
                     new TRow(tt.domain,tt.rmap,bmk.value().vals))
            {
                _bmk = bmk; _tt = tt;
            }
            TransitionTableCursor(TransitionTableCursor cu, Context cx, long p, TypedValue v)
                : base(cu, cx, p, v) { }
            internal static TransitionTableCursor New(Context cx,TransitionTableRowSet tt)
            {
                var bmk = tt.data.First();
                return (bmk!=null)? new TransitionTableCursor(cx, tt, bmk, 0): null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk.Next();
                return (bmk != null) ? new TransitionTableCursor(_cx, _tt, bmk, _pos + 1):null;
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
                return new BList<TableRow>(_bmk.value());
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
    internal class RoutineCallRowSet : RowSet
    {
        internal const long
            Actuals = -435, // CList<long>  SqlValue
            Proc = -436,    // Procedure
            Result = -437;  // RowSet
        internal Procedure proc => (Procedure)mem[Proc];
        internal CList<long> actuals => (CList<long>)mem[Actuals];
        internal RowSet result => (RowSet)mem[Result];
        internal RoutineCallRowSet(Context cx,From f,Procedure p, BList<long> r) 
            :base(cx.GetUid(),cx,f.domain,null,null,f.where,null,null,
                 BTree<long,object>.Empty+(Proc,p)+(Actuals,r))
        { }
        protected RoutineCallRowSet(Context cx,RoutineCallRowSet rs,
            CTree<long,Finder> nd,RowSet r) 
            :base(cx,rs+(Result,r),nd,true)
        { }
        protected RoutineCallRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RoutineCallRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, CTree<long,Finder> nd,bool bt)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new RoutineCallRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
            var r = (RoutineCallRowSet)base._Replace(cx, so, sv);
            r += (Actuals, cx.Replaced(actuals));
            r += (Proc, cx.done[proc.defpos] ?? proc);
            r += (Result, cx.data[result.defpos]); // hmm
            return r;
        }
        internal override RowSet Build(Context cx)
        {
            var _cx = new Context(cx);
            cx = proc.Exec(_cx, actuals);
            return new RoutineCallRowSet(cx, this, needed, cx.data[cx.result]);
        }
        protected override Cursor _First(Context cx)
        {
            return result.First(cx);
        }
        protected override Cursor _Last(Context cx)
        {
            return result.Last(cx);
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, actuals, VIC.RK|VIC.OV);
            t = Scan(t, proc.defpos, VIC.RK|VIC.OV);
            t = Scan(t, result.domain.rowType, VIC.RK|VIC.OV);
            return base.Scan(t);
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
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(_cx.GetUid(),s.mem+(Offset,o)+(Size,c)+(_Source,s.defpos)
                  +(RSTargets,s.rsTargets)
                  +(Table.LastData,s.lastData))
        {
            _cx.data += (defpos, this);
        }
        protected RowSetSection(Context cx, RowSetSection rs, CTree<long,Finder> nd,bool bt)
            :base(cx,rs,nd,bt)
        { }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RowSetSection(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new RowSetSection(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long,Finder>nd,bool bt)
        {
            return new RowSetSection(cx, this, nd, bt);
        }
        public static RowSetSection operator+(RowSetSection rs,(long,object)x)
        {
            return (RowSetSection)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RowSetSection(dp,mem);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        protected override Cursor _First(Context cx)
        {
            var b = cx.data[source].First(cx);
            for (int i = 0; b!=null && i < offset; i++)
                b = b.Next(cx);
            return RowSetSectionCursor.New(cx,this,b);
        }
        protected override Cursor _Last(Context cx)
        {
            var b = cx.data[source].Last(cx);
            for (int i = 0; b != null && i < offset; i++)
                b = b.Previous(cx);
            return RowSetSectionCursor.New(cx, this, b);
        }
        // shareable as of 26 April 2021
        internal class RowSetSectionCursor : Cursor
        {
            readonly RowSetSection _rss;
            readonly Cursor _rb;
            RowSetSectionCursor(Context cx, RowSetSection rss, Cursor rb)
                : base(cx, rss, rb._pos, rb._defpos, rb._ppos, rb) 
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
            : base(cx.GetUid(), _Mem(cx, d)+(_Domain,Domain.TableType))
        { }
        static BTree<long,object> _Mem(Context cx,SqlRowArray d)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += (SqlValue)cx.obs[d.rows[i]];
            return new BTree<long,object>(Docs,vs);
        }
        DocArrayRowSet(Context cx,DocArrayRowSet ds, CTree<long,Finder> nd,bool bt) 
            :base(cx,ds,nd,bt)
        { }
        protected DocArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DocArrayRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new DocArrayRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long,Finder> nd, bool bt)
        {
            return new DocArrayRowSet(cx,this, nd, bt);
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
                :base(_cx,drs,bmk.key(),0,0,_Row(_cx,bmk.value())) 
            {
                _drs = drs;
                _bmk = bmk;
            }
            DocArrayBookmark(Context cx, DocArrayBookmark cu): base(cx, cu) 
            {
                _drs = (DocArrayRowSet)cx.data[cx.RsUnheap(cu._rowsetpos)].Fix(cx);
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
        internal RTree tree => (RTree)mem[OrderedRowSet._RTree];
        internal TMultiset values => (TMultiset)mem[Multi];
        internal SqlFunction wf=> (SqlFunction)mem[Window];
        internal WindowRowSet(Context cx,RowSet sc, SqlFunction f)
            :base(cx.GetUid(),cx,sc.domain,null,null,sc.where,
                 null,null,BTree<long,object>.Empty+(_Source,sc.defpos)
                 +(Window,f)+(Table.LastData,sc.lastData))
        { }
        protected WindowRowSet(Context cx,WindowRowSet rs,CTree<long,Finder> nd,
            RTree tr,TMultiset vs) :base(cx,rs,nd,true)
        { }
        protected WindowRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new WindowRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
        internal override Basis Fix(Context cx)
        {
            return ((WindowRowSet)base.Fix(cx))+(Multi,values.Fix(cx))
                +(OrderedRowSet._RTree,tree.Fix(cx))+(Window,wf.Fix(cx));
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (WindowRowSet)base._Relocate(wr);
            r += (Window, wf._Relocate(wr));
            return r;
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, wf.defpos, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        internal override RowSet Build(Context cx)
        {
            var w = (WindowSpecification)cx.obs[wf.window];
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var kd = new Domain(Sqlx.ROW, w.domain.representation, w.order);
            var tree = new RTree(source,w.order, w.domain, 
                TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset(new Domain(Sqlx.MULTISET,wf.domain));
            for (var rw = cx.data[source].First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(kd, rw.values), cx.cursors);
                values.Add(v);
            }
            return new WindowRowSet(cx, this, CTree<long,Finder>.Empty, tree, values);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            throw new NotImplementedException();
        }
        // shareable as of 26 April 2021
        internal class WindowCursor : Cursor
        {
            WindowCursor(Context cx,long rp,Domain dm,int pos,long defpos,TRow rw) 
                : base(cx,rp,dm,pos,defpos,0L,rw)
            { }
            internal static Cursor New(Context cx,WindowRowSet ws)
            {
                ws = (WindowRowSet)ws.MaybeBuild(cx);
                var bmk = ws?.tree.First(cx);
                return (bmk == null) ? null 
                    : new WindowCursor(cx,ws.defpos,ws.domain,0,bmk._defpos,bmk._key);
            }
            internal static Cursor New(WindowRowSet ws, Context cx)
            {
                ws = (WindowRowSet)ws.MaybeBuild(cx);
                var bmk = ws?.tree.Last(cx);
                return (bmk == null) ? null
                    : new WindowCursor(cx, ws.defpos, ws.domain, 0, bmk._defpos, bmk._key);
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
    /// This section nneeds much more work, along with deep testing of the HTTP service
    /// and query rewriting for RESTViews.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class RestRowSet : RowSet
    {
        internal const long
            DefaultUrl = -370,  // string 
            ETag = -416, // string
            RemoteAggregates = -384, // bool
            RemoteCols = -373, // CList<long> SqlValue
            RemoteGroups = -374, // CList<long> SqlValue
            RestView = -459,    // long 
            RestValue = -457,   // TArray
            SqlAgent = 458, // string
            UrlCol = -446, // long SqlValue
       //     UsingCols = -259, // CTree<string,long> SqlValue
            UsingRowSet = -260; // long RowSet
        internal TArray aVal => (TArray)mem[RestValue];
        internal long restView => (long)(mem[RestView] ?? -1L);
        internal CList<long> remoteCols => 
            (CList<long>)mem[RemoteCols]??CList<long>.Empty;
        internal string defaultUrl => (string)mem[DefaultUrl] ?? "";
        internal string etag => (string)mem[ETag] ?? "";
        internal string sqlAgent => (string)mem[SqlAgent] ?? "Pyrrho";
        internal long usingRowSet => (long)(mem[UsingRowSet] ?? -1L);
        internal long urlCol => (long)(mem[UrlCol] ?? -1L);
        internal bool remoteAggregates => (bool)(mem[RemoteAggregates] ?? false);
        internal CList<long> remoteGroups =>
            (CList<long>)mem[RemoteGroups]??CList<long>.Empty;
        public RestRowSet(Context cx, From f, RestView vw)
            : base(f.defpos, _Mem(cx,f,vw) +(RestView,vw.defpos)+(RemoteCols,vw._Cols(cx))
                  +(RSTargets,new CTree<long,long>(vw.viewTable,f.defpos))
                  +(UsingRowSet,vw.usingTable)
                  +(Query._Matches,f.matches)+(Query.Where,f.where)+(From.Target,f.target)
                  +(Query.OrdSpec,f.ordSpec)+(_Domain,f.domain)
                  +(Index.Keys,f.domain.rowType))
        {
            SetupETags(cx);
        }
        protected RestRowSet(Context cx, RestRowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt) 
        { }
        protected RestRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Context cx,From fm,RestView vw)
        {
            var r = BTree<long, object>.Empty;
            var fi = CTree<long, Finder>.Empty;
            var dp = fm.defpos;
            var rt = fm.domain.rowType;
            var uc = vw.usingTable;
            if (uc < 0)
                r = _Fin(dp, cx, fi, rt, BTree<long, object>.Empty);
            else
            {
                var ut = (Table)cx.db.objects[uc];
                var rp = ut.domain.representation;
                var ur = ut.domain.rowType.Last().value();
                fi += (ur, new Finder(ur, uc));
                for (var b=rt.First();b!=null;b=b.Next())
                {
                    var p = b.value();
                    if (cx.obs[p] is SqlValue sv && sv.alias is string a
                            && cx.defs.Contains(a))
                        fi += (cx.defs[a].Item1, new Finder(p, dp));
                    if (cx.obs[p] is SqlCopy sc && rp.Contains(sc.copyFrom))
                        fi += (p, new Finder(p, uc));
                    else
                        fi += (p, new Finder(p, vw.defpos));
                }
                r = r+(_Finder, fi)+(UrlCol,ur);
            }
            var oi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            if (oi.description != "")
                r += (DefaultUrl, oi.description);
            if (oi.metadata[Sqlx.SQLAGENT] is string sg)
                r += (SqlAgent, sg);
            return r;
        }
        public static RestRowSet operator+(RestRowSet rs,(long,object)x)
        {
            return (RestRowSet)rs.New(rs.mem + x);
        }
        protected override Cursor _First(Context _cx)
        {
            return RestCursor.New(_cx, this);
        }
        protected override Cursor _Last(Context _cx)
        {
            return RestCursor.New(this, _cx);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new RestRowSet(cx, this, nd, bt);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestRowSet(defpos,m);
        }
        internal void SetupETags(Context cx)
        {
            var vi = (ObInfo)cx.db.role.infos[restView];
            var rv = (RestView)cx.obs[restView];
            string user = rv.clientName ?? cx.user.name, password = rv.clientPassword;
            var vwdesc = (string)vi.metadata[Sqlx.URL] ?? vi.description;
            var ss = vwdesc.Split('/');
            if (ss.Length > 3)
            {
                var st = ss[2].Split('@');
                if (st.Length > 1)
                {
                    var su = st[0].Split(':');
                    user = su[0];
                    if (su.Length > 1)
                        password = su[1];
                }
            }
            if (cx.etags?.cons.Contains(vwdesc) != true)
            {
                if (cx.etags == null)
                    cx.etags = new ETags();
                var hps = new HttpParams(vwdesc);
                cx.etags.cons += (vwdesc, hps);
                if (user == null)
                    hps.defaultCredentials = true;
                else
                {
                    var cr = user + ":" + password;
                    var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
                    hps.authorization = d;
                }
            }
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
            var r = (RestRowSet)base._Replace(cx, so, sv);
            r += (RestView, cx.ObReplace(restView, so, sv));
            var ch = false;
            var rg = CList<long>.Empty;
            for (var b= remoteGroups.First();b!=null;b=b.Next())
            {
                var p = cx.ObReplace(b.value(), so, sv);
                rg += p;
                ch = ch || p != b.value();
            }
            if (ch)
                r += (RemoteGroups, rg);
            if (usingRowSet >= 0)
                r += (UsingRowSet, cx.RsReplace(UsingRowSet, so, sv));
            ch = false;
            var rc = CList<long>.Empty;
            for (var b = remoteCols.First();b!=null;b=b.Next())
            {
                var p = cx.ObReplace(b.value(), so, sv);
                rc += p;
                ch = ch || p != b.value();
            }
            if (ch)
                r += (RemoteCols, rc);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RestRowSet)base.Fix(cx);
            var nv = cx.obuids[restView]??restView;
            if (nv!=restView)
            r += (RestView, nv);
            var ng = cx.Fix(remoteGroups);
            if (ng!=remoteGroups)
                r += (RemoteGroups, ng);
            var rc = cx.Fix(remoteCols);
            if (rc != remoteCols)
                r += (RemoteCols, rc);
            var nu = cx.rsuids[usingRowSet]??usingRowSet;
            if (nu!=usingRowSet)
                r += (UsingRowSet, nu);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (RestRowSet)base._Relocate(wr);
            r += (RestView, wr.Fix(restView));
            var rg = wr.Fix(remoteGroups);
            if (rg != remoteGroups)
                r += (RemoteGroups, rg);
            var rc = wr.Fix(remoteCols);
            if (rc != remoteCols)
                r += (RemoteCols, rc);
            if (usingRowSet >= 0)
            {
                r += (UsingRowSet, wr.Fix(usingRowSet));
            }
            return r;
        }
        public HttpWebRequest GetRequest(Context cx, string url, ObInfo vi)
        {
            SetupETags(cx);
            var vwdesc = (string)vi.metadata[Sqlx.URL] ?? vi.description;
            var rq = WebRequest.Create(url) as HttpWebRequest;
            rq.UserAgent = "Pyrrho " + PyrrhoStart.Version[1];
            var hps = cx.etags.cons[vwdesc];
            if (hps.defaultCredentials)
                rq.UseDefaultCredentials = true;
            else
                rq.Headers.Add("Authorization: Basic " + hps.authorization);
            if (vi.metadata.Contains(Sqlx.ETAG))
            {
                rq.Headers.Add("If-Unmodified-Since: "
                    + new THttpDate(((Transaction)cx.db).startTime,
                            vi.metadata.Contains(Sqlx.MILLI)));
                var s = cx.etags.cons[vwdesc].rvv?.ToString() ?? "";
                if (s != "")
                    rq.Headers.Add("If-Match: " + s);
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
                if (wr.StatusCode == HttpStatusCode.Forbidden)
                {
                    var ws = wr.GetResponseStream();
                    if (ws!=null)
                    {
                        var s = new StreamReader(ws).ReadToEnd();
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
        internal (string,string,StringBuilder) GetUrl(Context cx, ObInfo vi)
        {
            var url = (string)(cx.cursors[usingRowSet]?[urlCol].Val()
                    ?? vi.metadata[Sqlx.URL]
                    ?? vi.description);
            var sql = new StringBuilder();
            string targetName = "";
            if (vi.metadata.Contains(Sqlx.URL))
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
        internal override RowSet Build(Context cx)
        {
            if (cx.data[usingRowSet] is RowSet us &&
                !cx.cursors[us.defpos].Matches(cx))
                return new EmptyRowSet(-1, cx, domain);
            var vw = (RestView)cx.obs[restView];
            var vi = (ObInfo)cx.db.role.infos[vw.viewPpos];
            var (url,targetName,sql) = GetUrl(cx,vi);
            var rq = GetRequest(cx,url, vi);
            rq.Accept = vw.mime ?? "application/json";
            if (vi.metadata.Contains(Sqlx.URL))
                rq.Method = "GET";
            else
            {
                rq.Method = "POST";
                sql.Append("select ");
                var co = "";
                for (var b = remoteCols.First(); b != null; b = b.Next())
                {
                    sql.Append(co); co = ",";
                    sql.Append(cx.obs[b.value()].name);
                }
                sql.Append(" from "); sql.Append(targetName);
                var cs = cx.obs[_Source] as CursorSpecification;
                var cm = " group by ";
                if ((remoteGroups != null && remoteGroups.Count > 0)
                    || aggregates(cx))
                {
                    var ids = BTree<long, bool>.Empty;
                    for (var b = remoteGroups.First(); b != null; b = b.Next())
                    {
                        var n = b.value();
                        if (!ids.Contains(n))
                        {
                            ids += (n, true);
                            sql.Append(cm); cm = ",";
                            sql.Append(cx.obs[n].name);
                        }
                    }
                    for (var b = needed.First(); b != null; b = b.Next())
                        if (!ids.Contains(b.key()))
                        {
                            ids += (b.key(), true);
                            sql.Append(cm); cm = ",";
                            sql.Append(cx.obs[b.key()].name);
                        }
                    if (cs?.where.Count > 0 || cs?.matches.Count > 0)
                    {
                        var sw = WhereString(cx);
                        if (sw.Length > 0)
                        {
                            sql.Append((ids.Count > 0) ? " having " : " where ");
                            sql.Append(sw);
                        }
                    }
                }
                else
                if (where.Count > 0 || matches.Count > 0)
                {
                    var sw = WhereString(cx);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
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
                TypedValue a = null;
                var or = cx.result;
                cx.result = target; // sneaky
                if (s != null)
                    a = domain.Parse(0, new StreamReader(s).ReadToEnd(), mime, cx);
                cx.result = or;
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (a is TArray)
                        Console.WriteLine("--> " + ((TArray)a).list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (a?.ToString() ?? "null"));
                }
                s.Close();
                var r = this + (RestValue, a) + (Built, true);
                if (et != null && et != "" && rq.Method == "POST") // Pyrrho manual 3.8.1
                {
                    r += (ETag, et);
                    var vwdesc = (string)vi.metadata[Sqlx.URL] ?? vi.description;
                    cx.etags.cons[vwdesc].rvv += Rvv.Parse(et);
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
        internal override BList<long> Sources(Context cx)
        {
            var r = base.Sources(cx);
            if (usingRowSet >= 0)
                r += usingRowSet;
            return r;
        }
        public string WhereString(Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = where.First(); b != null; b = b.Next())
                if (Knows(cx, b.key()))
                {
                    try
                    {
                        var sw = cx.obs[b.key()].ToString(sqlAgent, remoteCols, cx);
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
            if (domain.representation.Contains(b.key())){
                try
                {
                    var nm = cx.obs[b.key()].ToString(sqlAgent,remoteCols, cx);
                    var tv = b.value();
                    if (tv.dataType.kind == Sqlx.CHAR)
                        ms += (nm, "'" + tv.ToString() + "'");
                    else
                        ms += (nm, tv.ToString());
                }
                catch (DBException) { }
            }
            for (var b=ms.First();b!=null;b=b.Next())
            { 
                sb.Append(cm); cm = " and ";
                sb.Append(b.key());
                sb.Append("=");
                sb.Append(b.value());
            }
            return sb.ToString();
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, remoteCols, VIC.RV | VIC.OV);
            t = Scan(t, remoteGroups, VIC.RK|VIC.OV);
            t = Scan(t, restView, VIC.RK | VIC.OV);
            t = Scan(t, urlCol, VIC.RK | VIC.OV);
       //     t = Scan(t, usingCols, VIC.RK | VIC.OV);
            t = Scan(t, usingRowSet, VIC.RK | VIC.OV);
            return base.Scan(t);
        }
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            var vi = (ObInfo)cx.db.role.infos[restView];
            var ta = vi.metadata.Contains(Sqlx.URL)?
                (TargetActivation)new HTTPActivation(cx, this, fm, PTrigger.TrigType.Insert, prov, cl)
                :new RESTActivation(cx, this, fm, PTrigger.TrigType.Insert, prov, cl);
            if (!iter)
                return ta;
            var trs = (TransitionRowSet)ta._trs;
            for (var b = trs.data.First(ta); b != null; b = b.Next(ta))
            {
                var cu = new TransitionRowSet.TransitionCursor(ta, trs, b.values, b._defpos, b._pos);
                cx.cursors += (defpos, cu._tgc);
                ta.EachRow();
            }
            return ta.Finish();
        }
        internal override Context Update(Context cx,RowSet fm, bool iter)
        {
            var vi = (ObInfo)cx.db.role.infos[restView];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, this, fm, PTrigger.TrigType.Update)
                : new RESTActivation(cx, this, fm, PTrigger.TrigType.Update);
            if (!iter)
                return ta;
            var trs = (TransitionRowSet)ta._trs;
            for (var b = trs.data.First(ta); b != null; b = b.Next(ta))
            {
                var cu = new TransitionRowSet.TransitionCursor(ta, trs, b.values, b._defpos, b._pos);
                cx.cursors += (defpos, cu._tgc);
                ta.EachRow();
            }
            return ta.Finish();
        }
        internal override Context Delete(Context cx,RowSet fm, bool iter)
        {
            var vi = (ObInfo)cx.db.role.infos[restView];
            var ta = vi.metadata.Contains(Sqlx.URL) ?
                (TargetActivation)new HTTPActivation(cx, this, fm, PTrigger.TrigType.Delete)
                : new RESTActivation(cx, this, fm, PTrigger.TrigType.Delete);
            return ta.Finish();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ");
            if (defaultUrl != "")
                sb.Append(defaultUrl);
            if (remoteAggregates) sb.Append(" RemoteAggregates");
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
            if (remoteGroups != null)
            { sb.Append(" RemoteGroups:"); sb.Append(remoteGroups); }
            if (usingRowSet >= 0)
            {
                sb.Append(" UsingRowSet:");
                sb.Append(Uid(usingRowSet));
                sb.Append(" UrlCol:");
                sb.Append(Uid(urlCol));
            }
            return sb.ToString();
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
                :base(cx,rrs.defpos,rrs.domain,pos,tr[Defpos]?.ToLong()??0L,
                     tr[LastChange]?.ToLong()??0L,tr)
            {
                cx.cursors += (rrs.defpos, this);
                _rrs = rrs; _ix = ix;
            }
            RestCursor(Context cx,RestCursor rb,long p,TypedValue v)
                :base(cx,rb._rrs,rb._pos,rb._ix,rb._ppos,rb+(p,v))
            {
                cx.cursors += (rb._rrs.defpos, this);
                _rrs = rb._rrs; _ix = rb._ix;
            }
            static TRow _Value(Context cx, RestRowSet rrs, int pos)
            {
                return (TRow)rrs.aVal[pos];
            }
            internal static RestCursor New(Context cx,RestRowSet rrs)
            {
                var ox = cx.finder;
                cx.finder += rrs.finder;
                for (var i = 0; i < rrs.aVal.Length; i++)
                {
                    var rb = new RestCursor(cx, rrs, 0, i);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
                }
                cx.finder = ox;
                return null;
            }
            internal static RestCursor New(RestRowSet rrs, Context cx)
            {
                var ox = cx.finder;
                cx.finder += rrs.finder;
                for (var i = rrs.aVal.Length-1; i>=0; i--)
                {
                    var rb = new RestCursor(cx, rrs, 0, i);
                    if (rb.Matches(cx))
                    {
                        cx.finder = ox;
                        return rb;
                    }
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
                for (var i = _ix+1; i < _rrs.aVal.Length; i++)
                {
                    var rb = new RestCursor(cx, _rrs, _pos+1, i);
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
                for (var i = _ix - 1; i >0; i--)
                {
                    var rb = new RestCursor(cx, _rrs, _pos + 1, i);
                    if (rb.Matches(cx))
                        return rb;
                }
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

    // An ad-hoc RowSet for a row history: the work is mostly done by
    // LogRowsCursor
    // shareable as of 26 April 2021
    internal class LogRowsRowSet : RowSet
    {
        internal const long
            TargetTable = -369; // long Table
        public long targetTable => (long)(mem[TargetTable] ?? -1L);
        public LogRowsRowSet(long dp, Context cx, long td)
            : base(dp, _Mem(cx, dp, td)+(Built,true))
        {
            cx.db = cx.db.BuildLog();
        }
        protected LogRowsRowSet(long dp,BTree<long,object> m) :base(dp,m)
        { }
        protected LogRowsRowSet(Context cx, RowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
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
            return r + (_Domain, new Domain(Sqlx.TABLE,rt)) + (_Finder, fi)
                +(Table.LastData,tb.lastData);
        }
        public static LogRowsRowSet operator+(LogRowsRowSet r,(long,object)x)
        {
            return (LogRowsRowSet)r.New(r.mem + x);
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new LogRowsRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new LogRowsRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
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
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, targetTable, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(Uid(targetTable));
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class LogRowsCursor : Cursor
        {
            readonly LogRowsRowSet _lrs;
            readonly TRow _row;
            LogRowsCursor(Context cx,LogRowsRowSet rs,int pos,long defpos,long ppos,TRow rw)
                :base(cx,rs,pos,defpos,ppos,rw)
            {
                _lrs = rs; _row = rw;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowsCursor(cx, _lrs, _pos, _defpos, _ppos, _row + (p, v));
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
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
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
                                return new LogRowsCursor(cx, lrs, pos, p, ph.ppos,
                                    new TRow(lrs.domain, vs));
                            }
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Record;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
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
                                return new LogRowsCursor(cx, lrs, pos, p, ph.ppos,
                                    new TRow(lrs.domain, vs));
                            }
                        case Physical.Type.Delete:
                        case Physical.Type.Delete1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Delete;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
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
                                return new LogRowsCursor(cx, lrs, pos, p, 
                                    ph.ppos, new TRow(lrs.domain, vs));
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
                return New(cx,_lrs,_pos+1,_defpos+1);
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
        : base(dp, _Mem(cx, dp, c) + (LogRow, r) + (LogCol, c)+(Built,true))
        { }
        protected LogRowColRowSet(long dp,BTree<long,object>m) :base(dp,m)
        { }
        protected LogRowColRowSet(Context cx, RowSet rs, CTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
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
            return BTree<long, object>.Empty + (_Domain,new Domain(Sqlx.TABLE,rt))+
                (LogRowsRowSet.TargetTable, tb.defpos) + (_Finder,fi)
                +(Table.LastData,tb.lastData);
        }
        public static LogRowColRowSet operator+(LogRowColRowSet r,(long,object)x)
        {
            return (LogRowColRowSet)r.New(r.mem + x);
        }
        internal override BTree<long, VIC?> Scan(BTree<long, VIC?> t)
        {
            t = Scan(t, logRow, VIC.RK|VIC.OV);
            t = Scan(t, logCol, VIC.RK|VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(targetTable);
            sb.Append("(");  sb.Append(Uid(logRow)); 
            sb.Append(",");  sb.Append(Uid(logCol)); 
            sb.Append(")"); 
            return sb.ToString();
        }
        internal override Basis New(BTree<long,object> m)
        {
            return new LogRowColRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new LogRowColRowSet(cx.GetUid(), m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd, bool bt)
        {
            return new LogRowColRowSet(cx, this, nd, bt);
        }
        protected override Cursor _First(Context _cx)
        {
            return LogRowColCursor.New(_cx,this,0,(null,logRow));
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
            LogRowColCursor(Context cx, LogRowColRowSet lrs, int pos, long defpos, long ppos,
                (Physical,long) next, TRow rw) 
                : base(cx, lrs, pos, defpos, ppos, rw) 
            {
                _lrs = lrs; _next = next;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowColCursor(cx, _lrs, _pos, _defpos, _defpos, _next, 
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
                    var b = lrs.rt.First(); vs += (b.value(), new TPosition(rc.ppos));
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
                    var rb = new LogRowColCursor(cx, lrs, pos, rc.ppos, rc.ppos,
                                        nx, new TRow(lrs.domain, vs));
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