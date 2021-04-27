using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
using System.Data.Common;
using System.Runtime.InteropServices;
using System.Configuration;
using System.Threading;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// Query Processing
    /// Queries include joins, groupings, window functions and subqueries. 
    /// Some queries have Selections containg SqlCols, predicates etc (which are all SqlValues),
    /// others (Froms) have a Selection implied by the target's ObInfo (which might be Any).
    /// Following analysis every Query has a mapping of SqlValues to columns in its result RowSet. 
    /// SqlValueExpr is a syntax-derived structure for evaluating the value of an SQL expression. 
    /// A RowSet’s outer context is its query.
    /// Analysis used to perform the following passes over this - but in the current implementation
    /// this analysis takes place during the left-to-right parsing of the Query. 
    /// (1) Sources: The From list is analysed during Context (by From constructor) (allows us to interpret *)
    /// (2) Selects: Next step is to process the SelectLists to define Selects (and update SqlValues) (create Evaluation steps if required)
    /// (3) Conditions: Then examine SearchConditions and move them down the tree if possible
    /// (4) Orders: Then examine Orders and move them down the tree if possible (not below SearchConditions though) create Evaluation and ordering steps if required)
    /// (5) RowSets: Compute the rowset for the given query
    /// shareable
    /// </summary>
    internal class Query : DBObject
    {
        internal const long
            Aggregates = -191, // CTree<long,bool> SqlValue
            Assig = -174, // CTree<UpdateAssignment,bool> 
            FetchFirst = -179, // int
            Filter = -180, // CTree<long,TypedValue> matches to be imposed by this query
            _Matches = -182, // CTree<long,TypedValue> matches guaranteed elsewhere
            Matching = -183, // CTree<long,CTree<long,bool>> SqlValue SqlValue (symmetric)
            OrdSpec = -184, // CList<long>
            Periods = -185, // BTree<long,PeriodSpec>
            _Repl = -186, // CTree<string,string> Sql output for remote views
            Where = -190; // CTree<long,bool> Boolean conditions to be imposed by this query
        public CTree<long,bool> aggs => 
            (CTree<long,bool>)mem[Aggregates] ?? CTree<long,bool>.Empty;
        internal CTree<long, TypedValue> matches =>
             (CTree<long, TypedValue>)mem[_Matches] ?? CTree<long, TypedValue>.Empty; // guaranteed constants
        internal CTree<long, CTree<long, bool>> matching =>
            (CTree<long, CTree<long, bool>>)mem[Matching] ?? CTree<long, CTree<long, bool>>.Empty;
        internal CTree<string, string> replace =>
            (CTree<string,string>)mem[_Repl]??CTree<string, string>.Empty; // for RestViews
        internal int display => domain.display;
        internal CList<long> ordSpec => (CList<long>)mem[OrdSpec]??CList<long>.Empty;
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal CList<long> rowType => domain.rowType;
        internal CTree<long,bool> where => 
            (CTree<long,bool>)mem[Where]?? CTree<long,bool>.Empty;
        internal CTree<long, TypedValue> filter =>
            (CTree<long, TypedValue>)mem[Filter] ?? CTree<long, TypedValue>.Empty;
        /// <summary>
        /// For Updatable Views and Joins we need some extra machinery
        /// </summary>
        internal CTree<UpdateAssignment, bool> assig => 
            (CTree<UpdateAssignment,bool>)mem[Assig]??CTree<UpdateAssignment,bool>.Empty;
        /// <summary>
        /// The Fetch First Clause (-1 if not specified)
        /// </summary>
        internal int fetchFirst => (int)(mem[FetchFirst]??-1);
        protected Query(long dp, BTree<long, object> m) 
            : base(dp, m) 
        { }
        protected Query(long u) : base(u, BTree<long,object>.Empty
            + (_Domain, Domain.TableType))
        { }
        public static Query operator+(Query q,(long,object)x)
        {
            return (Query)q.New(q.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Query(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx,BTree<long,object>m)
        {
            if (defpos >= Transaction.Analysing || cx.parse==ExecuteStatus.Parse)
                return (m==mem)?this:(Query)New(m);
            return (Query)cx.Add(new Query(cx.GetUid(), m));
        }
        internal override CList<long> _Cols(Context cx)
        {
            return rowType;
        }
        internal virtual CTree<long, bool> _RestViews(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal override DBObject Relocate(long dp)
        {
            return new Query(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Query)base._Relocate(wr);
            r += (_Domain, domain._Relocate(wr));
            r += (Filter, wr.Fix(filter));
            r += (_Matches,wr.Fix(matches));
            r += (OrdSpec, wr.Fix(ordSpec));
            r += (Where, wr.Fix(where));
            r += (Assig, wr.Fix(assig));
            return r;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, domain.rowType, VIC.OK|VIC.OV);
            t = Scan(t, filter, VIC.OK | VIC.OV);
            t = Scan(t, matches, VIC.OK | VIC.OV);
            t = Scan(t, ordSpec, VIC.OK | VIC.OV);
            t = Scan(t, where, VIC.OK | VIC.OV);
            t = Scan(t, assig, VIC.OK| VIC.OV);
            if (from>0L)
                t = Scan(t, from, VIC.OK | VIC.OV);
            return t;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Query)base.Fix(cx);
            var nd = domain.Fix(cx);
            if (nd!=domain)
            r += (_Domain, nd);
            var nf = cx.Fix(filter);
            if (filter!=nf)
                r += (Filter, nf);
            var nm = cx.Fix(matches);
            if (matches!=nm)
                r += (_Matches, nm);
            var no = cx.Fix(ordSpec);
            if (ordSpec!=no)
                r += (OrdSpec, no);
            var nw = cx.Fix(where);
            if (where!=nw)
                r += (Where, nw);
            var ne = cx.Fix(assig);
            if (assig!=ne)
                r += (Assig, ne);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            for (var b = where?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Calls(defpos, cx))
                    return true;
            return Calls(defpos, cx);
        }
        internal override DBObject _Replace(Context cx,DBObject was,DBObject now)
        {
            if (cx.done.Contains(defpos)) // includes the case was==this
                return cx.done[defpos];
            var de = 0;
            var r = this;
            if (domain.representation.Contains(was.defpos))
                de = _Max(de, now.depth);
            var dm = domain._Replace(cx, was, now);
            if (dm != r.domain)
                r += (_Domain, dm); 
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), was, now);
                if (v.defpos != b.key())
                    w += (b.key(), true);
                de = Math.Max(de, v.depth);
            }
            if (w!=r.where)
                r += (Where,w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx._Replace(b.key(), was, now);
                if (bk.defpos != b.key())
                    ms += (bk.defpos, b.value());
                de = Math.Max(de, bk.depth);
            }
            if (ms!=r.matches)
                r += (_Matches, ms);
            var os = r.ordSpec;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var ow = (SqlValue)cx._Replace(b.value(),was,now);
                if (b.value()!=ow.defpos)
                    os += (b.key(), ow.defpos);
                de = Math.Max(de, ow.depth);
            }
            if (os!=r.ordSpec)
                r += (OrdSpec, os);
            var ag = r.assig;
            for (var b = ag.First(); b != null; b = b.Next())
            {
                var a = b.key();
                var aa = (SqlValue)cx._Replace(a.val,was,now);
                var ab = (SqlValue)cx._Replace(a.vbl,was,now);
                if (aa.defpos != a.val || ab.defpos != a.vbl)
                    ag += (new UpdateAssignment(ab.defpos,aa.defpos), true);
                de = Math.Max(de, Math.Max(aa.depth, ab.depth));
            }
            if (ag != r.assig)
                r += (Assig, ag);
            if (de >= r.depth)
                r += (Depth, de + 1);
            r = (Query)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal virtual Query Refresh(Context cx)
        {
            return (Query)cx.obs[defpos];
        }
        internal virtual bool HasColumn(Context cx,SqlValue sv)
        {
            return false;
        }
        /// <summary>
        /// Add a new column to the query, and update the row type
        /// (Needed for alter)
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal override DBObject Add(Context cx, SqlValue v)
        {
            if (v == null)
                return this;
            var r = (Query)base.Add(cx, v);
            var a = v.aggregates(cx);
            if (a)
                r += (Aggregates, r.aggs+(v.defpos,true));
            return r;
        }
        internal override DBObject Conditions(Context cx)
        {
            var svs = where;
            var r = this;
            for (var b = svs.First(); b != null; b = b.Next())
            {
                r = ((SqlValue)cx.obs[b.key()]).Conditions(cx, r, true, out bool mv);
                if (mv)
                    svs -=b.key();
            }
            return r.AddCondition(cx,svs);
        }
        /// <summary>
        /// </summary>
        /// <param name="svs">A list of where conditions</param>
        /// <param name="tr"></param>
        /// <param name="q">A source query</param>
        /// <returns></returns>
        internal override DBObject MoveConditions(Context cx, Query q)
        {
            var wh = where;
            var fi = filter;
            var qw = q?.where;
            var qf = q?.filter;
            if (q != null)
                for (var b = where.First(); b != null; b = b.Next())
                {
                    var k = b.key();
                    if (!q.where.Contains(k))
                        qw += (k, true);
                    wh -= k;
                }
            for (var b = fi.First(); b != null; b = b.Next())
                qf += (b.key(),b.value());
            var oq = q;
            if (qw!=q?.where)
                q=(Query)q.AddCondition(cx,Where, qw);
            if (qf != q?.filter)
                q += (Filter, qf);
            if (q != oq)
                cx.Add(q);
            var r = this;
            if (wh != where)
                r=(Query)r.AddCondition(cx,Where, wh);
            if (fi != filter)
                r += (Filter, fi);
            if (r != this)
                r = (Query)cx.Add(r);
            return (Query)New(cx,r.mem);
        }
        internal override DBObject Orders(Context cx, CList<long> ord)
        {
            return (ordSpec.CompareTo(ord)==0)?
                this: (Query)New(cx, mem + (OrdSpec, ord));
        }
        /// <summary>
        /// Bottom up: Add q.matches to this.
        /// Since it is bottom-up we don't need to worry about sharing.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        internal override DBObject AddMatches(Context cx,Query q)
        {
            var m = matches;
            for (var b = q.matches.First(); b != null; b = b.Next())
                m += (b.key(), b.value());
            return (m==matches)?this:(Query)cx.Replace(this,this+ (_Matches, m));
        }
        /// <summary>
        /// The given Sqlvalue is guaranteed to be a constant at this level and context.
        /// We don't propagate to other levels.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="sv"></param>
        /// <param name="tv"></param>
        /// <returns></returns>
        internal override DBObject AddMatch(Context cx,SqlValue sv, TypedValue tv)
        {
            return (Query)cx.Replace(this,New(cx,mem + (_Matches, matches + (sv.defpos, tv))));
        }
        internal override DBObject AddCondition(Context cx, CTree<long,bool> conds)
        {
            cx.Replace(this, AddCondition(cx,Where, conds));
            return Refresh(cx);
        }
        internal override DBObject AddCondition(Context cx, long prop, SqlValue cond, bool onlyKnown)
        {
            if (where.Contains(cond.defpos))
                return this;
            if (onlyKnown && !cond.KnownBy(cx, this))
                return this;
            var filt = filter;
            var q = this + (Dependents, dependents + (cond.defpos, true));
            if (cond is SqlValueExpr se && se.kind == Sqlx.EQL)
            {
                var lv = cx.obs[se.left] as SqlValue;
                var rg = (SqlValue)cx.obs[se.right];
                if (lv.target >= 0 && rg.isConstant(cx))
                    filt += (lv.target, rg.Eval(cx));
                else if (rg.target >= 0 && lv.isConstant(cx))
                    filt += (rg.target, lv.Eval(cx));
            }
            if (filt != filter)
                q += (Filter, filt);
            if (prop == Where)
                q += (Where, q.where + (cond.defpos, true));
            else if (q is TableExpression te)
                q = te + (TableExpression.Having, te.having + (cond.defpos, true));
            if (cond.depth >= q.depth)
                q += (Depth, cond.depth + 1);
            cx.Replace(this, q);
            return (Query)New(cx, q.mem);
        }
        internal bool KnowsOneOf(Context cx,BTree<long,bool> t)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.key()]).KnownBy(cx, this))
                    return true;
            return false;
        }
        internal virtual bool Knows(Context cx,long p)
        {
            return false;
        }
        internal override BTree<long,Register> StartCounter(Context _cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                tg = _cx.obs[b.value()].StartCounter(_cx, rs, tg);
            return tg;
        }
        internal new virtual BTree<long, Register> AddIn(Context _cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                tg = _cx.obs[b.value()].AddIn(_cx, rb, tg);
            return tg;
        }
        public static void Eqs(Context cx, CTree<long, bool> svs, ref Adapters eqs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                ((SqlValue)cx.obs[b.key()]).Eqs(cx, ref eqs);
        }
        /// <summary>
        /// Ensure that the distinct request is propagated to the query
        /// </summary>
        internal virtual Query SetDistinct(Context cx)
        {
            return this;
        }
        /// <summary>
        /// delegate AccessibleCols
        /// </summary>
        /// <returns>the selectors</returns>
        internal virtual Domain AccessibleCols()
        {
            return Domain.Null;
        }
        internal virtual bool NameMatches(Ident n)
        {
            return name==n.ident;
        }
        /// <summary>
        /// The number of columns in the query (not the same as the result length)
        /// </summary>
        internal int Size(Context cx) { return rowType.Length; }
        /// <summary>
        /// Get the SqlValue at the given column position
        /// </summary>
        /// <param name="i">the position</param>
        /// <returns>the value expression</returns>
        internal SqlValue ValAt(Context cx,int i)
        {
            var cols = rowType;
            if (i >= 0 && i < cols.Length)
                return (SqlValue)cx.obs[cols[i]];
            throw new PEException("PE335");
        }
        internal virtual bool Uses(Context cx,long t)
        {
            return false;
        }
        internal static bool Uses(Context cx,CTree<long,bool> x,long t)
        {
            for (var b = x.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.key()]).Uses(cx,t))
                    return true;
            return false;
        }
        internal override bool aggregates(Context cx)
        {
            if (aggs!=CTree<long,bool>.Empty)
                return true;
            for (var b = where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].aggregates(cx))
                    return true;
            return false;
        }
        internal RowSet Ordering(Context cx, RowSet r,bool distinct)
        {
            var os = ordSpec;
            if (os != null && os.Length > 0 && r.rowOrder.CompareTo(os)!=0)
                return new OrderedRowSet(cx,r, os, distinct);
            if (distinct)
                return new DistinctRowSet(cx,r);
            return r;
        }
        internal virtual RowSet RowSets(Context cx,CTree<long,RowSet.Finder> fi)
        {
            cx.results += (defpos, defpos);
            return cx.data[defpos];
        }
        internal void CondString(StringBuilder sb, CTree<long, bool> cond, string cm)
        {
            for (var b = cond?.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(Uid(b.key()));
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var fl = domain.kind==Sqlx.CONTENT;
            for (var b = domain.representation.First(); (!fl) && b != null; b = b.Next())
                fl = b.value().kind == Sqlx.CONTENT;
            if (fl)
                sb.Append(" CONTENT ");
            sb.Append(" RowType:(");
            var cm = "";
            for (var b=rowType?.First();b!=null;b=b.Next())
            {
                sb.Append(cm); 
                cm = (b.key()+1==display)?"|":","; 
                sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (assig.Count>0) { sb.Append(" Assigs:"); sb.Append(assig); }
            if (mem.Contains(FetchFirst)) 
            { sb.Append(" FetchFirst="); sb.Append(fetchFirst); }
            if (filter.Count>0) { sb.Append(" Filter:"); sb.Append(filter); }
            if (matches.Count>0) { sb.Append(" Matches:"); sb.Append(matches); }
            if (ordSpec!=CList<long>.Empty) 
            { 
                sb.Append(" OrdSpec (");
                cm = "";
                for (var b = ordSpec.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append("=");
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (mem.Contains(_Repl)) { sb.Append(" Replace:"); sb.Append(replace); }
            if (mem.Contains(Where)) { sb.Append(" Where:"); sb.Append(where); }
            return sb.ToString();
        }

        internal SqlValue MaybeAdd(Context cx, SqlValue su)
        {
            for (var b=domain.rowType.First();b!=null;b=b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                if (sv._MatchExpr(cx, this, su))
                    return sv;
            }
            su += (_Alias, alias ?? name ?? ("C_" + (defpos&0xfff)));
            Add(cx,su);
            return su;
        }
    }
    /// <summary>
    /// Implement a CursorSpecification 
    /// shareable as of 26 April 2021
    /// </summary>
    internal class CursorSpecification : Query
    {
        internal const long
            _Source = -195, // string
            Union = -196; // long
        /// <summary>
        /// The source string
        /// </summary>
        public string _source=> (string)mem[_Source];
        /// <summary>
        /// The QueryExpression part of the CursorSpecification
        /// </summary>
        public long union => (long)(mem[Union]??-1L);
        /// <summary>
        /// Constructor: a CursorSpecification from the Parser
        /// </summary>
        /// <param name="t">The transaction</param>
        /// <param name="dt">the expected data type</param>
        protected CursorSpecification(long u,BTree<long,object>m)
            : base(u, m) 
        { }
        internal CursorSpecification(long u) : base(u) { }
        public static CursorSpecification operator +(CursorSpecification q, (long, object) x)
        {
            return (CursorSpecification)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CursorSpecification(defpos, m);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse==ExecuteStatus.Parse)
                return (m==mem)?this:(Query)New(m);
            return cx.Add(new CursorSpecification(cx.GetUid(), m));
        }
        internal override CTree<long, bool> _RestViews(Context cx)
        {
            var un = (Query)cx.obs[union];
            return un._RestViews(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CursorSpecification(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (CursorSpecification)base._Relocate(wr);
            r += (Union, wr.Fixed(union)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CursorSpecification)base.Fix(cx);
            var nu = cx.obuids[union] ?? union;
            if (nu!=union)
            r += (Union,nu);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CursorSpecification)base._Replace(cx, so, sv);
            if (cx._Replace(r.union, so, sv) is Query un && un.defpos != r.union)
                r += (Union,un.defpos);
            r = (CursorSpecification)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            var u = ((Query)cx.obs[union]);
            if (u == null)
                return this;
            u = u.Refresh(cx);
            return (Query)New(cx,(u.defpos==union)?mem:(mem+(Union,u.defpos)+(_From,u.from)));
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[union]).Uses(cx,t) || base.Uses(cx,t);
        }
        internal override DBObject ReviewJoins(Context cx)
        {
            var r = (CursorSpecification)base.ReviewJoins(cx);
            var changed = r != this;
            var u = ((Query)cx.obs[union]);
            if (u == null)
                return r;
            u = (Query)u.ReviewJoins(cx);
            changed = changed ||(u.defpos != union);
            return changed?(Query)New(cx,r.mem + (Union, u.defpos)):this;
        }
        /// <summary>
        /// Analysis stage Conditions: do Conditions on the union.
        /// </summary>
        internal override DBObject Conditions(Context cx)
        {
            var u = (Query)cx.obs[union];
            var r = (CursorSpecification)MoveConditions(cx, u);
            u = (QueryExpression)u.Refresh(cx).Conditions(cx);
            return (Query)New(cx,(u.defpos==union)?r.mem:(r.mem+(Union,u.defpos)));
        }
        internal override DBObject Orders(Context cx, CList<long> ord)
        {
            var r = (CursorSpecification)base.Orders(cx, ord);
            var ch = r != this;
            var u = ((Query)cx.obs[union]).Orders(cx, ord);
            ch = ch || u.defpos != union;
            return ch?(Query)New(cx,r.mem+(Union,u.defpos)):this;
        }
        internal override bool Knows(Context cx,long c)
        {
            return base.Knows(cx,c) 
                || ((Query)cx.obs[union]).Knows(cx,c);
        }
        internal override bool aggregates(Context cx)
        {
            return ((Query)cx.obs[union]).aggregates(cx) || base.aggregates(cx);
        }
        internal override RowSet RowSets(Context cx, CTree<long, RowSet.Finder> fi)
        {
            var u = (Query)cx.obs[union];
            if (u == null)
                return null;
            var r = u.RowSets(cx, fi);
            cx.result = r.defpos;
            return cx.data[r.defpos];
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = ((Query)cx.obs[union]).StartCounter(cx,rs, tg);
            return base.StartCounter(cx,rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rs, BTree<long, Register> tg)
        {
            tg = ((Query)cx.obs[union]).AddIn(cx,rs,tg);
            return base.AddIn(cx,rs,tg);
        }
        /// <summary>
        /// Ensure that the distinct request is propagated to the query
        /// </summary>
        internal override Query SetDistinct(Context cx)
        {
            var u = ((Query)cx.obs[union]).SetDistinct(cx);
            return (u.defpos == union) ? this : (Query)New(cx, mem + (Union, u.defpos));
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, union, VIC.OK | VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_Source))
            { sb.Append(" Source={"); sb.Append(_source); sb.Append('}'); }
            sb.Append(" Union: "); sb.Append(Uid(union)); 
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement a TableExpression as a subclass of Query
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TableExpression : Query
    {
        internal const long
            Nuid = -198, // long From (or RowSet)
            Group = -199, // long GroupSpecification
            Having = -200, // CTree<long,bool> SqlValue
            Windows = -201; // CTree<long,bool> WindowSpecification
        /// <summary>
        /// The from clause of the tableexpression
        /// </summary>
        internal long nuid => (long)(mem[Nuid]??-1L);
        /// <summary>
        /// The group specification
        /// </summary>
        internal long group => (long)(mem[Group]??-1L);
        /// <summary>
        /// The having clause
        /// </summary>
        internal CTree<long,bool> having =>
            (CTree<long,bool>)mem[Having]??CTree<long,bool>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal CTree<long,bool> window =>
            (CTree<long,bool>)mem[Windows]??CTree<long,bool>.Empty;
        /// <summary>
        /// Constructor: a tableexpression from the parser
        /// </summary>
        /// <param name="t">the transaction</param>
        internal TableExpression(long u, BTree<long, object> m) : base(u, m) 
        {  }
        public static TableExpression operator +(TableExpression q, (long, object) x)
        {
            return (TableExpression)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableExpression(defpos,m);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse == ExecuteStatus.Parse)
                return (m == mem) ? this : (Query)New(m);
            return (Query)cx.Add(new TableExpression(cx.GetUid(), m));
        }
        internal override CTree<long, bool> _RestViews(Context cx)
        {
            var fm = (Query)cx.obs[nuid];
            return fm._RestViews(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableExpression(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableExpression)base._Relocate(wr);
            r += (Having, wr.Fix(having));
            r += (Windows, wr.Fix(window));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableExpression)base.Fix(cx);
            var nh = cx.Fix(having);
            if (nh != having)
                r += (Having, nh);
            var nw = cx.Fix(window);
            if (nw != window)
                r += (Windows, nw);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TableExpression)base._Replace(cx, was, now);
            r = (TableExpression)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            ((Query)cx.obs[nuid]).Refresh(cx);
            return base.Refresh(cx);
        }
        internal override DBObject ReviewJoins(Context cx)
        {
            var r = (TableExpression)base.ReviewJoins(cx);
            var ch = r != this;
            var t = ((Query)cx.obs[nuid]).ReviewJoins(cx);
            ch = ch || t.defpos != nuid;
            return ch ? (Query)New(cx,r.mem + (Nuid, t.defpos)) : this;
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[nuid]).Uses(cx,t);
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[nuid].aggregates(cx) || base.aggregates(cx);
        }
        internal override RowSet RowSets(Context cx, CTree<long, RowSet.Finder> fi)
        {
            if (nuid == From._static.defpos)
                return new TrivialRowSet(defpos, cx,new TRow(domain),-1L,fi);
            var fr = (Query)cx.obs[nuid];
            var r = fr.RowSets(cx,fi);
            if (r == null)
                return null;
            if (cx.obs[group] is GroupSpecification grp)
                for (var b=where.First();b!=null;b=b.Next())
                    ((SqlValue)cx.obs[b.key()]).Grouped(cx, grp);
            var ma = CTree<long, long>.Empty;
            var cb = r.rt.First();
            for (var b=rowType.First();b!=null&&cb!=null&&b.key()<domain.display;b=b.Next(),cb=cb.Next())
            {
                var p = b.value();
                var c = cb.value();
                fi += (p, new RowSet.Finder(c, r.defpos));
                ma += (c, p);
            }
            var kt = CList<long>.Empty;
            for (var b = r.keys.First(); b != null && b.key()<domain.display; b = b.Next())
                if (ma.Contains(b.value()))
                    kt += ma[b.value()];
            var rs = new TableExpRowSet(defpos, cx, nuid, domain, kt, r, where, matches+filter, fi);
            cx.results += (defpos, rs.defpos);
            return cx.data[rs.defpos];
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = cx.obs[nuid].StartCounter(cx,rs, tg);
            return base.StartCounter(cx,rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rs, BTree<long, Register> tg)
        {
            tg = cx.obs[nuid].AddIn(cx,rs,tg);
            return base.AddIn(cx,rs,tg);
        }
        internal override bool Knows(Context cx,long c)
        {
            return ((Query)cx.obs[nuid]).Knows(cx,c);
        }
        internal override DBObject AddMatches(Context cx, Query q)
        {
            var m = mem;
            for (var b = having.First(); b != null; b = b.Next())
                q = (TableExpression)((SqlValue)cx.obs[b.key()]).AddMatches(cx,q);
            cx.Add(q);
            var t = ((Query)cx.obs[nuid]).AddMatches(cx, q);
            return (t.defpos!=nuid)?(Query)New(cx,m+(Nuid,t)):this;
        }
        internal override DBObject Conditions(Context cx)
        {
            var fm = (Query)cx.obs[nuid];
            var r = (TableExpression)((Query)MoveConditions(cx, fm)).Refresh(cx);
            r.AddHavings(cx, fm);
            fm = fm.Refresh(cx);
            return (Query)New(cx,r.mem);
        }
        internal void AddHavings(Context cx, Query q)
        {
            var ha = having;
            var qw = q.where;
            for (var b = ha.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = ((SqlValue)cx.obs[k]).Import(cx,q);
                if (v is SqlValue w && !q.where.Contains(k))
                        qw += (k, true);
            }
            if (qw != q.where)
                cx.Replace(q, q.AddCondition(cx,Where, qw));
        }
        internal override DBObject Orders(Context cx,CList<long> ord)
        {
            var r = (TableExpression)base.Orders(cx, ord);
            var ch = r != this;
            var t = ((From)cx.obs[nuid]).Orders(cx, ord);
            ch = ch || r.defpos != nuid;
            return ch?New(cx,r.mem+(Nuid,t.defpos)):this;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, nuid, VIC.OK|VIC.OV|VIC.RV);
            t = Scan(t, group, VIC.OK | VIC.OV);
            t = Scan(t, having, VIC.OK | VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: "); sb.Append(Uid(nuid));
            if (group != -1L) { sb.Append(" Group:"); sb.Append(Uid(group)); }
            if (having.Count!=0) { sb.Append(" Having:"); sb.Append(having); }
            if (window.Count!=0) { sb.Append(" Window:"); sb.Append(window); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Join is implemented as a subclass of Query
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class JoinPart : Query
    {
        internal const long
            _FDInfo = -202, // FDJoinPart
            JoinCond = -203, // CTree<long,bool>
            JoinKind = -204, // Sqlx
            JoinUsing = -208, // CTree<long,long> SqlValue SqlValue (right->left)
            LeftOrder = -205, // CList<long>
            LeftOperand = -206, // long Query
            Natural = -207, // Sqlx
            RightOrder = -209, // CList<long>
            RightOperand = -210; // long Query
        /// <summary>
        /// NATURAL or USING or NO (the default)
        /// </summary>
        public Sqlx naturaljoin => (Sqlx)(mem[Natural]??Sqlx.NO);
        /// <summary>
        /// The list of common TableColumns for natural join
        /// </summary>
        internal CTree<long,long> joinUsing => 
            (CTree<long,long>)mem[JoinUsing]??CTree<long,long>.Empty;
        /// <summary>
        /// the kind of Join
        /// </summary>
        public Sqlx kind => (Sqlx)(mem[JoinKind]??Sqlx.CROSS);
        /// <summary>
        /// The join condition is implemented by ordering, using any available indexes.
        /// Rows in the join will use left/rightInfo.Keys() for ordering and theta-operation.
        /// If the join condition requires an ordering that conflicts with an explicit
        /// ordering, form a lateral join instead.
        /// </summary>
        internal CList<long> leftOrder => (CList<long>)mem[LeftOrder]??CList<long>.Empty; // initialised once domain is known
        internal CList<long> rightOrder => (CList<long>)mem[RightOrder]??CList<long>.Empty;
        /// <summary>
        /// During analysis, we collect requirements for the join conditions.
        /// </summary>
        internal CTree<long, bool> joinCond => 
            (CTree<long,bool>)mem[JoinCond]??CTree<long, bool>.Empty;
        /// <summary>
        /// The left element of the join
        /// </summary>
        public long left => (long)(mem[LeftOperand]??-1L);
        /// <summary>
        /// The right element of the join
        /// </summary>
        public long right => (long)(mem[RightOperand]??-1L);
        /// <summary>
        /// A FD-join depends on a functional relationship between left and right
        /// </summary>
        internal FDJoinPart FDInfo => (FDJoinPart)mem[_FDInfo];
        /// <summary>
        /// Constructor: a join part being built by the parser
        /// </summary>
        /// <param name="t"></param>
        protected JoinPart(long u, BTree<long,object> m)  : base(u,m) 
        { }
        internal JoinPart(long u) : base(u) 
        { }
        public static JoinPart operator+ (JoinPart j,(long,object)x)
        {
            return new JoinPart(j.defpos, j.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinPart(defpos,m);
        }
        internal override DBObject New(Context cx,BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse==ExecuteStatus.Parse)
                return (m==mem)?this:(Query)New(m);
            return (Query)cx.Add(new JoinPart(cx.GetUid(), m));
        }
        internal override CTree<long, bool> _RestViews(Context cx)
        {
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            return lf._RestViews(cx)+rg._RestViews(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new JoinPart(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (JoinPart)base._Relocate(wr);
            r += (JoinCond, wr.Fix(joinCond));
            r += (LeftOrder,wr.Fix(leftOrder));
            r += (RightOrder, wr.Fix(rightOrder));
            r += (JoinUsing, wr.Fix(joinUsing));
            r += (Matching, wr.Fix(matching));
            r += (LeftOperand, wr.Fixed(left).defpos);
            r += (RightOperand, wr.Fixed(right).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (JoinPart)base.Fix(cx);
            var nc = cx.Fix(joinCond);
            if (nc != joinCond)
                r += (JoinCond, nc);
            var nl = cx.Fix(leftOrder);
            if (nl != leftOrder)
                r += (LeftOrder, nl);
            var nr = cx.Fix(rightOrder);
            if (nr != rightOrder)
                r += (RightOrder, nr);
            var nn = cx.Fix(joinUsing);
            if (nn != joinUsing)
                r += (JoinUsing, nn);
            var nf = cx.obuids[left] ?? left;
            if (nf != left)
                r += (LeftOperand, nf); // can't use operator+=(p,Query) here
            var ns = cx.obuids[right] ?? right;
            if (ns != right)
                r += (RightOperand, ns);
            var ma = cx.Fix(matching);
            if (ma != matching)
                r += (Matching, ma);
            return r;
        }
        internal override DBObject ReviewJoins(Context cx)
        {
            var r = (JoinPart)((kind==Sqlx.CROSS)?Conditions(cx):base.ReviewJoins(cx));
            var ch = r != this;
            var lf = ((Query)cx.obs[left]).ReviewJoins(cx);
            ch = ch || (lf.defpos != left);
            var rg = ((Query)cx.obs[right]).ReviewJoins(cx);
            ch = ch || (rg.defpos != right);
            if (ch)
            {
                r += (LeftOperand, lf.defpos);
                r += (RightOperand, rg.defpos);
                return (JoinPart)New(cx,r.mem);
            }
            return this;
        }
        internal override bool Knows(Context cx,long c)
        {
            return ((Query)cx.obs[left]).Knows(cx,c) 
                || ((Query)cx.obs[right]).Knows(cx,c);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[left]).Uses(cx,t) 
                || ((Query)cx.obs[right]).Uses(cx,t) || Uses(cx,joinCond,t);
        }
        internal override bool aggregates(Context cx)
        {
            return ((Query)cx.obs[left]).aggregates(cx)
                || ((Query)cx.obs[right]).aggregates(cx)
                ||base.aggregates(cx);
        }
        internal override DBObject AddMatches(Context cx,Query q)
        {
            if (q.matches.Count == 0)
                return this;
            var lf = ((Query)cx.obs[left]).AddMatches(cx,q);
            var rg = ((Query)cx.obs[right]).AddMatches(cx,q);
            var r = (JoinPart)base.AddMatches(cx,q);
            for (var b = joinCond.First(); b != null; b = b.Next())
                r = (JoinPart)((SqlValue)cx.obs[b.key()]).AddMatches(cx,q);
            r += (LeftOperand, lf);
            r += (RightOperand, rg);
            return (Query)cx.Replace(this, New(cx, r.mem));
        }
        internal override Query Refresh(Context cx)
        {
            var lf = ((Query)cx.obs[left]).Refresh(cx);
            var rg = ((Query)cx.obs[right]).Refresh(cx);
            var r = base.Refresh(cx);
            var d = Math.Max(depth, (Math.Max(lf.depth, rg.depth) + 1));
            return r+(Depth,d);
        }
        /// <summary>
        /// Analysis stage Selects: call for left and right.
        /// </summary>
        internal Query Selects(Context cx, Query qs)
        {
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            var nv = BTree<long,Domain>.Empty;
            var dm = Domain.TableType;
            var d = lf.display;
            for (var b= lf.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                if (b.key() < d)
                    dm += (p, c.domain);
                else
                    nv += (p, c.domain);
            }
            d = rg.display;
            for (var b = rg.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                if (b.key() < d)
                    dm += (p, c.domain);
                else
                    nv += (p, c.domain);
            }
            d = dm.display;
            for (var b = nv.First(); b != null; b = b.Next())
                dm += (b.key(), b.value());
            dm += (Domain.Display, d);
            var r = this + (_Domain,dm);
            var lo = CList<long>.Empty; // left ordering
            var ro = CList<long>.Empty; // right
            var ma = r.matching;
            if (naturaljoin==Sqlx.USING)
            {
                int m = 0; // common.Count
                var ju = joinUsing;
                var oq = qs;
                for (var c = rg.rowType.First(); c != null; c = c.Next())
                {
                    var rr = c.value();
                    var rv = (SqlValue)cx.obs[rr];
                    if (ju.Contains(rr))
                    {
                        var ll = ju[rr];
                        lo += ll;
                        ro += rr;
                        r = (JoinPart)r.Hide(cx, rv);
                        qs = (Query)qs.Remove(cx, rv);
                        var ml = matching[ll] ?? CTree<long, bool>.Empty;
                        var mr = matching[rr] ?? CTree<long, bool>.Empty;
                        ma = ma + (ll, ml + (rr, true))
                            + (rr, mr + (ll, true));
                        m++;
                        break;
                    }
                }
                if (oq != qs)
                    cx.Add(qs);
                if (m == 0)
                    r += (JoinKind, Sqlx.CROSS);
                else
                {
                    cx.Add(cx.obs[left] + (OrdSpec, lo));
                    cx.Add(cx.obs[right] + (OrdSpec, ro));
                }
            } 
            else if (naturaljoin != Sqlx.NO)
            {
                int m = 0; // common.Count
                var lt = lf.rowType;
                var rt = rg.rowType;
                var oq = qs;
                for (var b=lt.First();b!=null;b=b.Next())
                {
                    var ll = b.value();
                    var lv = (SqlValue)cx.obs[ll];
                    for (var c = rt.First();c!=null;c=c.Next())
                    {
                        var rr = c.value();
                        var rv = (SqlValue)cx.obs[rr];
                        if (lv.name.CompareTo(rv.name) == 0)
                        {
                            var cp = new SqlValueExpr(cx.GetUid(), cx, Sqlx.EQL, lv, rv, Sqlx.NULL);
                            cx.Add(cp);
                            r += (JoinCond, cp.Disjoin(cx));
                            lo += lv.defpos;
                            ro += rv.defpos;
                            r = (JoinPart)r.Hide(cx,rv);
                            qs = (QuerySpecification)qs.Remove(cx,rv);
                            m++;
                            break;
                        }
                    }
                }
                if (oq != qs)
                    cx.Add(qs);
                if (m == 0)
                    r += (JoinKind, Sqlx.CROSS);
                else
                {
                    cx.Add(cx.obs[left] + (OrdSpec, lo));
                    cx.Add(cx.obs[right] + (OrdSpec, ro));
                }
            }
            else
            {
                var lm = BTree<string, SqlValue>.Empty;
                for (var b = lf.rowType.First(); b != null; b = b.Next())
                {
                    var sv = (SqlValue)cx.obs[b.value()];
                    lm += (sv.name, sv);
                }
                for (var b = rg.rowType.First(); b != null; b = b.Next())
                {
                    var rv = (SqlValue)cx.obs[b.value()];
                    var n = rv.name;
                    if (lm[n] is SqlValue lv)
                    {
                        var ln = (lf.alias ?? lf.name) + "." + n;
                        var nl = lv + (_Alias, ln);
                        cx.Replace(lv, nl);
                        var rn = (rg.alias ?? rg.name) + "." + n;
                        var nr = rv + (_Alias, rn);
                        cx.Replace(rv, nr);
                    }
                }
            }
            // first ensure each joinCondition has the form leftExpr compare rightExpr
            // reversing if necessary
            // if not, move it to where
            var wh = where;
            var jc = r.joinCond;
            for (var b = r.joinCond.First(); b != null; b = b.Next())
            {
                var fs = (Query)cx.obs[left];
                var sc = (Query)cx.obs[right];
                if (cx.obs[b.key()] is SqlValueExpr se)
                {
                    var lv = cx.obs[se.left] as SqlValue;
                    var rv = (SqlValue)cx.obs[se.right];
                    if (se.kind == Sqlx.EQL)
                    {
                        var ml = matching[lv.defpos] ?? CTree<long, bool>.Empty;
                        var mr = matching[rv.defpos] ?? CTree<long, bool>.Empty;
                        ma = ma + (lv.defpos, ml+(rv.defpos,true)) 
                            + (rv.defpos, mr+(lv.defpos,true));
                    }
                    if (lv.isConstant(cx) || rv.isConstant(cx))
                        continue;
                    if (lv.IsFrom(cx, fs, true) && rv.IsFrom(cx, sc, true))
                        continue;
                    if (lv.IsFrom(cx, sc, true) && rv.IsFrom(cx, fs, true))
                    {
                        cx.Replace(se, new SqlValueExpr(se.defpos, cx, se.kind, rv, lv, se.mod));
                        continue;
                    }
                }
                wh += (b.key(), b.value());
                jc -= b.key();
            }
            if (ma != CTree<long, CTree<long,bool>>.Empty)
                r += (Matching, ma);
            if (jc!=r.joinCond)
                r = (JoinPart)r.AddCondition(cx, wh) + (JoinCond,jc);
            cx.Add(r);
            return r;
        }
        /// <summary>
        /// Analysis stage Conditions: call for left and right
        /// Turn the join condition into an ordering request, and set it up
        /// </summary>
        internal override DBObject Conditions(Context cx)
        {
            var r = (JoinPart)base.Conditions(cx);
            var k = kind;
            if (k==Sqlx.CROSS)
                k = Sqlx.INNER;
            var w = where;
            var jc = joinCond;
            for (var b = where.First(); b != null; b = b.Next())
            {
                var qq = (JoinPart)((SqlValue)cx.obs[b.key()]).JoinCondition(cx, r, ref jc, ref w);
                if (qq != r)
                {
                    w -= b.key();
                    r = qq;
                }
            }
            cx.Add(((Query)cx.obs[r.left]).Conditions(cx));
            cx.Add(((Query)cx.obs[r.right]).Conditions(cx));
            r = (JoinPart)r.New(r.mem + (JoinCond, jc) + (JoinKind, k));
            r =  (JoinPart)r.AddCondition(cx,Where,w).Orders(cx,r.ordSpec);
            if (r.FDInfo==null)
                r+= (JoinKind,kind);
            return (Query)New(cx,r.mem);
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
            var n = 0;
            var r = (JoinPart)base.Orders(cx,ord); // relocated if shared
            var k = kind;
            var jc = joinCond;
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            // First try to find a perfect foreign key relationship, either way round
            if (GetRefIndex(cx, lf, rg, true) is FDJoinPart fa)
            {
                r += (_FDInfo, fa);
                n = (int)fa.conds.Count;
            }
            if (n < joinCond.Count && GetRefIndex(cx,rg, lf, false) is FDJoinPart fb) 
            {
                r += (_FDInfo,fb);
                n = (int)fb.conds.Count;
            }
            if (n > 0) // we will use this information instead of the left and right From rowsets
            {
                for (var b = r.FDInfo.conds.First(); b != null; b = b.Next())
                    jc -= b.value();
                k = Sqlx.NO;
            }
            else
            {
                // Now look to see if there is a suitable index for left or right
                if (GetIndex(cx,lf, true) is FDJoinPart fc)
                {
                    r += (_FDInfo,fc);
                    n = (int)fc.conds.Count;
                }
                if (n < joinCond.Count && GetIndex(cx,rg, false) is FDJoinPart fd)
                {
                    r +=(_FDInfo,fd);
                    n = (int)fd.conds.Count;
                }
                if (n > 0) //we will use the selected index instead of its From rowset: and order the other side for its rowset to be used
                {
                    for (var b = r.FDInfo.conds.First(); b != null; b = b.Next())
                        jc -=b.value();
                    k = Sqlx.NO;
                }
            }
            // Everything remaining in joinCond is not in FDInfo.conds
            for (var b = jc.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValueExpr se) // we already know these have the right form
                {
                    var lo = lf.ordSpec;
                    var ro = rg.ordSpec;
                    var lv = cx.obs[se.left] as SqlValue
                        ?? throw new PEException("PE196");
                    var rv = (SqlValue)cx.obs[se.right];
                    if (!cx.HasItem(lo,lv.defpos))
                        lf = (Query)lf.Orders(cx,lo+lv.defpos);
                    if (!cx.HasItem(ro,rv.defpos))
                        rg = (Query)rg.Orders(cx,ro+rv.defpos);
                }
            if (joinCond.Count == 0)
                for(var b=ord?.First(); b!=null; b=b.Next()) // test all of these 
                {
                    var oi = (SqlValue)cx.obs[b.value()];
                    var lo = lf.ordSpec;
                    var ro = rg.ordSpec;
                    if (lf.HasColumn(cx,oi)// && !(left.rowSet is IndexRowSet))
                        && !cx.HasItem(lo,oi.defpos))
                        lf = (Query)lf.Orders(cx,lo+oi.defpos);
                    if (rg.HasColumn(cx,oi)// && !(right.rowSet is IndexRowSet))
                        && !cx.HasItem(ro,oi.defpos))
                        rg = (Query)rg.Orders(cx,ro + oi.defpos);
                }
            cx.Add(lf);
            cx.Add(rg);
            return (Query)New(cx,r.mem + (JoinKind, k) + (JoinCond, jc));
        }
        /// <summary>
        /// See if there is a ForeignKey Index whose foreign key is taken from the one side of joinCond,
        /// and the referenced primary key is given by the corresponding terms on the other side.
        /// We will return null if the Queries are not Table Froms.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        FDJoinPart GetRefIndex(Context cx,Query a, Query b,bool left)
        {
            FDJoinPart best = null;
            if (a is From fa &&  b is From fb && cx.db.objects[fa.target] is Table ta 
                && cx.db.objects[fb.target] is Table tb)
            {
                for (var bx = ta.indexes.First(); bx != null; bx = bx.Next())
                {
                    var x = (Index)cx.db.objects[bx.value()];
                    cx._Add(x);
                    if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                    && x.tabledefpos == ta.defpos && x.reftabledefpos == tb.defpos)
                    {
                        var cs = CTree<long,long>.Empty;
                        var rx = (Index)cx.db.objects[x.refindexdefpos];
                        cx._Add(x);
                        var br = rx.keys.First();
                        for (var bc=x.keys.First();bc!=null&&br!=null;bc=bc.Next(),br=br.Next())
                        {
                            var found = false;
                            for (var bj = joinCond.First(); bj != null; bj = bj.Next())
                                if (cx.obs[bj.key()] is SqlValueExpr se
                                    && cx.obs[left ? se.left : se.right] is SqlCopy sc 
                                    && sc.copyFrom == bc.value()
                                    && cx.obs[left ? se.right : se.left] is SqlCopy sd 
                                    && sd.copyFrom == br.value())
                                {
                                    cs +=(bc.key(), se.defpos);
                                    found = true;
                                    break;
                                }
                            if (!found)
                                goto next;
                        }
                        var fi = new FDJoinPart(ta, x, tb, rx, cs, left);
                        if (best == null || best.conds.Count < fi.conds.Count)
                            best = fi;
                        next:;
                    }
                }
            }
            return best;
        }
        FDJoinPart GetIndex(Context cx,Query a,bool left)
        {
            var tr = cx.db;
            FDJoinPart best = null;
            if (a is From fa && tr.objects[fa.target] is Table ta)
            {
                for (var bx = ta.indexes.First(); bx != null; bx = bx.Next())
                {
                    var x = (Index)tr.objects[bx.value()];
                    cx._Add(x);
                    if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                        x.flags.HasFlag(PIndex.ConstraintType.Unique))
                    && x.tabledefpos == ta.defpos)
                    {
                        var cs = CTree<long,long>.Empty;
                        var jc = joinCond;
                        for (var bc=x.keys.First();bc!=null;bc=bc.Next())
                        {
                            var found = false;
                            for (var bj = jc.First(); bj != null; bj = bj.Next())
                                if (cx.obs[bj.key()] is SqlValueExpr se
                                    && cx.obs[left ? se.left : se.right] is SqlValue sc
                                    && sc.defpos == bc.value())
                                {
                                    cs +=(bc.key(), se.defpos);
                                    found = true;
                                    break;
                                }
                            if (!found)
                                goto next;
                        }
                        var fi = new FDJoinPart(ta, x, null, null, cs, !left);
                        if (best == null || best.conds.Count < fi.conds.Count)
                            best = fi;
                        next:;
                    }
                }
            }
            return best;
        }
        /// <summary>
        /// Check if we have a given column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>whether we have it</returns>
        internal override bool HasColumn(Context cx,SqlValue s)
        {
            if (((Query)cx.obs[left]).HasColumn(cx,s) 
                || ((Query)cx.obs[right]).HasColumn(cx,s))
                return true;
            return base.HasColumn(cx,s);
        }
        /// <summary>
        /// Analysis stage RowSets: build the join rowset
        /// </summary>
        internal override RowSet RowSets(Context cx, CTree<long, RowSet.Finder> fi)
        {
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            var ma = matching;
            for (var b = (matches+filter).First(); b != null; b = b.Next())
            {
                var p = b.key();
                var v = b.value();
                var sv = (SqlValue)cx.obs[p];
                if (lf.HasColumn(cx, sv))
                {
                    lf = (Query)lf.AddMatch(cx, sv, v);
                    for (var c=ma[p]?.First();c!=null;c=c.Next())
                        rg = (Query)rg.AddMatch(cx, (SqlValue)cx.obs[c.key()], v);
                }
                else if (rg.HasColumn(cx,sv))
                {
                    rg = (Query)rg.AddMatch(cx, sv, b.value());
                    for (var c = ma[p]?.First(); c != null; c = c.Next())
                        lf = (Query)lf.AddMatch(cx, (SqlValue)cx.obs[c.key()], v);
                }
            }
            if (FDInfo is FDJoinPart fd)
            {
                var tb = (Table)cx.obs[fd.table];
                var ix = (Index)cx.obs[fd.index];
                var rt = (Table)cx.obs[fd.rtable];
                var rx = (Index)cx.obs[fd.rindex];
                var ds = new IndexRowSet(cx, tb, ix, cx.Filter(tb,where));
                var rs = new IndexRowSet(cx, rt, rx,null);
                if (fd.reverse)
                    return new JoinRowSet(cx, this, Selected(cx,lf,ds,fi), 
                        Selected(cx,rg,rs,fi));
                else
                    return new JoinRowSet(cx, this, Selected(cx,lf,rs,fi), 
                        Selected(cx,rg,ds,fi));
            }
            var lr = lf.RowSets(cx,fi);
            var lo = lf.ordSpec;
            if (lo?.Length > 0)
                lr = new OrderedRowSet(cx, lr, lo, false);
            var rr = rg.RowSets(cx,lr.finder);
            var ro = rg.ordSpec;
            if (ro?.Length>0)
                rr = new OrderedRowSet(cx, rr, ro, false);
            var res = new JoinRowSet(cx, this, lr, rr);
            cx.results += (defpos, res.defpos);
            return res;
        }
        static RowSet Selected(Context cx,Query q,RowSet r, CTree<long, RowSet.Finder> fi)
        {
            if (q.rowType == r.rt)
                return r;
            return new SelectedRowSet(cx, q, r, fi);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, left, VIC.OK | VIC.OV);
            t = Scan(t, right, VIC.OK | VIC.OV);
            t = Scan(t, leftOrder, VIC.OK | VIC.OV);
            t = Scan(t, rightOrder, VIC.OK | VIC.OV);
            t = Scan(t, joinCond, VIC.OK | VIC.OV);
            t = Scan(t, joinUsing, VIC.OK | VIC.OV, VIC.OK | VIC.OV);
            t = Scan(t, matching, VIC.OK | VIC.OV, VIC.OK | VIC.OV);
            return t;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(Uid(left));
            if (naturaljoin != Sqlx.NO && naturaljoin != Sqlx.USING)
            {
                sb.Append(" ");
                sb.Append(naturaljoin);
            }
            if (kind != Sqlx.NO)
            {
                sb.Append(" ");
                sb.Append(kind);
            }
            sb.Append(" join");
            sb.Append(Uid(right));
            if (naturaljoin == Sqlx.USING)
            {
                var comma = " ";
                for(var ic = joinUsing.First();ic!=null;ic=ic.Next())
                {
                    sb.Append(comma);sb.Append(Uid(ic.key()));
                    sb.Append("=");sb.Append(Uid(ic.value()));
                    comma = ",";
                }
            }
            CondString(sb, joinCond, " on ");
            sb.Append(" matching");
            for(var b=matching.First();b!=null;b=b.Next())
                for (var c=b.value().First();c!=null;c=c.Next())
                {
                    sb.Append(" ");sb.Append(Uid(b.key()));
                    sb.Append("=");sb.Append(Uid(c.key()));
                }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Information about functional dependency for join evaluation
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class FDJoinPart : Basis
    {
        internal const long
            FDConds = -211, // CTree<long,long> SqlValue,SqlValue
            FDIndex = -212, // long Index
            FDRefIndex = -213, // long Index
            FDRefTable = -214, // long Table
            FDTable = -215, // long Table
            Reverse = -216; // bool
        /// <summary>
        /// The primary key Index giving the functional dependency
        /// </summary>
        public long index => (long)(mem[FDIndex]??-1L);
        public long table => (long)(mem[FDTable]??-1L);
        /// <summary>
        /// The foreign key index if any
        /// </summary>
        public long rindex => (long)(mem[FDRefIndex]??-1L);
        public long rtable => (long)(mem[FDRefTable]??-1L);
        /// <summary>
        /// The joinCond entries moved to this FDJoinPart: the indexing is hierarchical: 0, then 1 etc.
        /// </summary>
        public CTree<long, long> conds => 
            (CTree<long,long>)mem[FDConds]??CTree<long, long>.Empty;
        /// <summary>
        /// True if right holds the primary key
        /// </summary>
        public bool reverse => (bool)(mem[Reverse]??false);
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ix">an index</param>
        /// <param name="s">the source expressions</param>
        /// <param name="d">the destination expressions</param>
        public FDJoinPart(Table tb, Index ix, Table rt, Index rx, CTree<long,long> c, bool r)
            :base(BTree<long,object>.Empty
                 +(FDIndex,ix.defpos)+(FDTable,tb.defpos)
                 +(FDRefIndex,rx.defpos)+(FDRefTable,rt.defpos)
                 +(FDConds,c)+(Reverse,r))
        { }
        protected FDJoinPart(BTree<long, object> m) : base(m) { }
        public static FDJoinPart operator+(FDJoinPart f,(long,object)x)
        {
            return new FDJoinPart(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FDJoinPart(m);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = this;
            r += (FDConds, wr.Fix(conds));
            r += (FDIndex, wr.Fix(index));
            r += (FDTable, wr.Fix(table));
            r += (FDRefIndex, wr.Fix(rindex));
            r += (FDRefTable, wr.Fix(rtable));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = this;
            var nc = cx.Fix(conds);
            if (nc != conds)
                r += (FDConds, nc);
            var ni = cx.obuids[index]??index;
            if (ni != index)
                r += (FDIndex, ni);
            var nt = cx.obuids[table]??table;
            if (nt != table)
                r += (FDTable, nt);
            var nr = cx.obuids[rindex]??rindex;
            if (nr != rindex)
                r += (FDRefIndex, nr);
            var nu = cx.obuids[rtable]??rtable;
            if (nu != rtable)
                r += (FDRefTable, nu);
            return r;
        }
    }
}
