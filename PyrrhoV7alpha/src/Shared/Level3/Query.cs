using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
using System.Data.Common;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
    /// </summary>
    internal class Query : DBObject
    {
        internal const long
            _Aggregates = -191, // bool
            Assig = -174, // BTree<UpdateAssignment,bool> 
            FetchFirst = -179, // int
            Filter = -180, // BTree<long,TypedValue>
            _Matches = -182, // BTree<long,TypedValue>
            Matching = -183, // BTree<long,BTree<long,bool>>
            OrdSpec = -184, // CList<long>
            Periods = -185, // BTree<long,PeriodSpec>
            _Repl = -186, // BTree<string,string>
            Where = -190; // BTree<long,bool>
        public bool _aggregates => (bool)(mem[_Aggregates] ?? false);
        public string name => (string)mem[Name] ?? "";
        internal BTree<long, TypedValue> matches =>
             (BTree<long, TypedValue>)mem[_Matches] ?? BTree<long, TypedValue>.Empty; // guaranteed constants
        internal BTree<long, BTree<long, bool>> matching =>
            (BTree<long, BTree<long, bool>>)mem[Matching] ?? BTree<long, BTree<long, bool>>.Empty;
        internal BTree<string, string> replace =>
            (BTree<string,string>)mem[_Repl]??BTree<string, string>.Empty; // for RestViews
        internal int display => domain.display;
  //      internal BTree<SqlValue, SqlValue> import =>
  //          (BTree<SqlValue,SqlValue>)mem[_Import]??BTree<SqlValue, SqlValue>.Empty; // cache Imported SqlValues
        internal CList<long> ordSpec => (CList<long>)mem[OrdSpec]??CList<long>.Empty;
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal CList<long> rowType => domain.rowType;
        /// <summary>
        /// where clause, may be updated during Conditions() analysis.
        /// This is a disjunction of filters.
        /// the value for a filter, initially null, is updated to the implementing rowSet 
        /// </summary>
        internal BTree<long,bool> where => 
            (BTree<long,bool>)mem[Where]?? BTree<long,bool>.Empty;
        internal BTree<long, TypedValue> filter =>
            (BTree<long, TypedValue>)mem[Filter] ?? BTree<long, TypedValue>.Empty;
        /// <summary>
        /// For Updatable Views and Joins we need some extra machinery
        /// </summary>
        internal BTree<UpdateAssignment, bool> assig => 
            (BTree<UpdateAssignment,bool>)mem[Assig]??BTree<UpdateAssignment,bool>.Empty;
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
        internal override CList<long> _Cols(Context cx)
        {
            return rowType;
        }
        internal override DBObject Relocate(long dp)
        {
            return new Query(dp, mem);
        }
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            domain.Scan(cx);
            cx.Scan(assig);
            cx.Scan(filter);
            cx.Scan(matching);
            cx.Scan(matches);
            cx.Scan(where);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Query)base._Relocate(wr);
            r += (_Domain, domain._Relocate(wr));
            r += (Filter, wr.Fix(filter));
            r += (_Matches,wr.Fix(matches));
            r += (Matching, wr.Fix(matching));
            r += (OrdSpec, wr.Fix(ordSpec));
            r += (Where, wr.Fix(where));
            r += (Assig, wr.Fix(assig));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Query)base.Fix(cx);
            r += (_Domain, domain.Fix(cx));
            if (filter!=null)
                r += (Filter, cx.Fix(filter));
            if (matches.Count>0)
                r += (_Matches, cx.Fix(matches));
            if (matching.Count>0)
                r += (Matching, cx.Fix(matching));
            if (ordSpec.Count>0)
                r += (OrdSpec, cx.Fix(ordSpec));
            if (where.Count>0)
                r += (Where, cx.Fix(where));
            if (assig.Count>0)
                r += (Assig, cx.Fix(assig)); 
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
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            var r = this;
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var x = b.value();
                if (x == was.defpos)
                {
                    ch = true;
                    de = Math.Max(de, now.depth);
                    vs += (SqlValue)now;
                }
                else
                {
                    if (cx.done.Contains(x))
                        ch = true;
                    vs += (SqlValue)(cx.done[x] ?? cx.obs[x]);
                }
            }
            if (ch)
                r += (_Domain, new Domain(r.domain.kind,vs));
            var dm = domain._Replace(cx, was, now);
            if (dm != domain)
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
            var mg = r.matching;
            for (var b = mg.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx._Replace(b.key(), was, now);
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var ck = (SqlValue)cx._Replace(c.key(), was, now);
                    if (ck.defpos != c.key())
                        bv += (ck.defpos, true);
                    de = Math.Max(de, ck.depth);
                }
                if (bk.defpos != b.key() || bv != b.value())
                    mg += (bk.defpos, bv);
                de = Math.Max(de, bk.depth);
            }
            if (mg!=r.matching)
                r += (Matching, mg);
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
            cx.done += (defpos, r);
            return r;
        }
        internal virtual Query Refresh(Context cx)
        {
            return (Query)cx.obs[defpos];
        }
        internal override DBObject TableRef(Context cx, From f)
        {
            if (cx.done.Contains(defpos)) 
                return cx.done[defpos];
            var cs = rowType;
            var r = this;
            for (var b = cs.First(); b != null; b = b.Next())
            {
                var x = (SqlValue)cx.obs[b.value()];
                var bv = x?.TableRef(cx, f);
                if (bv != x && bv!=null)
                    cs += (b.key(), bv.defpos);
            }
            if (cs != rowType)
                r += (_Domain, new Domain(Sqlx.ROW,cx,cs));
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx.obs[b.key()].TableRef(cx, f);
                if (v.defpos != b.key())
                    w = w - b.key() + (v.defpos, true);
            }
            if (w != r.where)
                r += (Where,w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx.obs[b.key()].TableRef(cx, f);
                if (bk.defpos != b.key())
                    ms = ms - b.key() + (bk.defpos, b.value());
            }
            if (ms != r.matches)
                r += (_Matches, ms);
            var mg = matching;
            for (var b = mg.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)cx.obs[b.key()].TableRef(cx, f);
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var ck = (SqlValue)cx.obs[c.key()].TableRef(cx, f);
                    if (ck.defpos != c.key())
                        bv = bv - c.key() + (ck.defpos, true);
                }
                if (bk.defpos!=b.key())
                    mg -= b.key();
                if (bv != b.value())
                    mg += (bk.defpos, bv);
            }
            if (mg != matching)
                r += (Matching, mg);
            var os = ordSpec;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var it = b.value();
                var ow = (SqlValue)cx.obs[it].TableRef(cx, f);
                if (it != ow.defpos)
                    os += (b.key(), ow.defpos);
            }
            if (os != ordSpec)
                r += (OrdSpec, os);
            var ag = r.assig;
            for (var b = ag.First(); b != null; b = b.Next())
            {
                var aa = (SqlValue)cx.obs[b.key().val].TableRef(cx, f);
                var ab = (SqlValue)cx.obs[b.key().vbl].TableRef(cx, f);
                if (aa.defpos != b.key().val || ab.defpos != b.key().vbl)
                    ag += (new UpdateAssignment(ab.defpos, aa.defpos), b.value());
            }
            if (ag != r.assig)
                r += (Assig, ag);
            cx.done += (defpos, r);
            return (r == this) ? this : (Query)cx.Add(r);
        }
        internal virtual Query AddRestViews(Context cx,CursorSpecification q)
        {
            return this;
        }
        internal virtual bool HasColumn(Context cx,SqlValue sv)
        {
            return false;
        }
        /// <summary>
        /// Add a new column to the query, and update the row type
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal virtual Query Add(Context cx,SqlValue v)
        {
            if (v == null)
                return this;
            var deps = dependents + (v.defpos,true);
            var dpt = _Max(depth, 1 + v.depth);
            var r = this +(Dependents,deps)+(Depth,dpt)
                +(_Domain,domain+(v.defpos,v.domain));
            var a = v.aggregates(cx);
            if (a)
                r += (_Aggregates, a);
            return r;
        }
        internal Query Add(Context cx,int i,SqlValue v)
        {
            if (v == null)
                return this;
            var deps = dependents + (v.defpos, true);
            var dpt = _Max(depth, 1 + v.depth);
            var r = this + (Dependents, deps) + (Depth, dpt)
                + (_Domain, domain + (i,v.defpos));
            var a = v.aggregates(cx);
            if (a)
                r += (_Aggregates, a);
            return r;
        }
        internal Query Remove(Context cx,SqlValue v)
        {
            if (v == null)
                return this;
            var rt = CList<long>.Empty;
            var rp = BTree<long, Domain>.Empty; 
            var ch = false;
            var rb = domain.representation.First();
            for (var b = rowType?.First(); b != null && rb!=null; b = b.Next(),rb=rb.Next())
                if (b.value() == v.defpos)
                    ch = true;
                else
                {
                    rp += (rb.key(),rb.value());
                    rt += b.value();
                }
            return ch ?
                this + (_Domain, new Domain(Sqlx.ROW, cx, rt)) + (Dependents, dependents - v.defpos)
                : this;
        }
        static bool _Aggs(Context cx, BList<long>ss)
        {
            var r = false;
            for (var b = ss.First(); b != null; b = b.Next())
                if (cx.obs[b.value()].aggregates(cx))
                    r = true;
            return r;
        }
        internal virtual Query Conditions(Context cx)
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
        internal Query MoveConditions(Context cx, Query q)
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
                q=q.AddCondition(cx,Where, qw);
            if (qf != q?.filter)
                q += (Filter, qf);
            if (q != oq)
                cx.Add(q);
            var r = this;
            if (wh != where)
                r=r.AddCondition(cx,Where, wh);
            if (fi != filter)
                r += (Filter, fi);
            if (r != this)
                r = (Query)cx.Add(r);
            return r;
        }
        internal BTree<long,bool> Needs(Context cx,BTree<long,bool> s)
        {
            for (var b = rowType?.First(); b != null; b = b.Next())
                s = ((SqlValue)cx.obs[b.value()]).Needs(cx, s);
            return s;
        }
        internal virtual Query Orders(Context cx, CList<long> ord)
        {
            return (cx.SameRowType(this,ordSpec,ord)!=false)?
                this: this + (OrdSpec, ord);
        }
        public static bool Eval(BTree<long, bool> svs, Context cx)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        internal virtual Query AddMatches(Context cx,Query q)
        {
            var m = matches;
            for (var b = q.matches.First(); b != null; b = b.Next())
                m += (b.key(), b.value());
            return (Query)cx.Replace(this,this + (_Matches, m));
        }
        internal Query AddMatch(Context cx,SqlValue sv, TypedValue tv)
        {
            return (Query)cx.Replace(this,this + (_Matches, matches + (sv.defpos, tv)));
        }
        /// <summary>
        /// Add a condition and/or update to the QueryWhere. 
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">a set of update assignments</param>
        /// <param name="data">some insert data</param>
        /// <param name="rqC">SqlValues requested from possibly remote contributors</param>
        /// <param name="needed">SqlValues that remote contributors will need to be told</param>
        /// <returns>an updated querywhere, now containing typedvalues for the condition</returns>
        internal Query AddCondition(Context cx,SqlValue cond)
        {
            return where.Contains(cond.defpos) ? this : 
                (Query)cx.Add(this + (Where, where + (cond.defpos, true)));
        }
        internal virtual Query AddCondition(Context cx, BTree<long, bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return this;
        }
        internal Query AddCondition(Context cx, BTree<long,bool> conds)
        {
            cx.Replace(this, AddCondition(cx,Where, conds));
            return Refresh(cx);
        }
        internal Query AddCondition(Context cx,long prop,BTree<long,bool> conds)
        {
            var q = this;
            for (var b = conds.First(); b != null; b = b.Next())
                q = q.AddCondition(cx, prop, (SqlValue)cx.obs[b.key()]);
            return q;
        }
        internal Query AddCondition(Context cx,long prop, SqlValue cond)
        {
            if (where.Contains(cond.defpos))
                return this;
            var filt = filter;
            var q = this + (Dependents,dependents+(cond.defpos,true));
            if (cond is SqlValueExpr se && se.kind == Sqlx.EQL)
            {
                var lv = cx.obs[se.left] as SqlValue;
                var rg = (SqlValue)cx.obs[se.right];
                if (lv.target>=0 && rg is SqlLiteral ll)
                    filt += (lv.target, ll.Eval(cx));
                else if (rg.target>=0 && lv is SqlLiteral lr)
                    filt += (rg.target, lr.Eval(cx));
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
            return (Query)cx.Add(q);
        }
        internal long QuerySpec(Context cx)
        {
            var r = -1L;
            for (var b = cx.obs.PositionAt(Transaction.Analysing); b != null; b = b.Next())
            {
                if (b.value() is Query q)
                {
                    if (q.defpos >= defpos)
                        return r;
                    if (q is QuerySpecification || q is From)
                        r = q.defpos;
                }
            }
            return r;
        }
        /// <summary>
        /// Distribute a set of update assignments to table expressions
        /// </summary>
        /// <param name="assigns">the list of assignments</param>
        internal virtual Query DistributeAssigns(BTree<UpdateAssignment, bool> assigns)
        {
            return this;
        }
        internal virtual bool Knows(Context cx,SqlValue c)
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
        public static void Eqs(Context cx, BTree<long, bool> svs, ref Adapters eqs)
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
        /// propagate the Insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs"the rowsets affected></param>
        internal virtual Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// The number of columns in the query (not the same as the result length)
        /// </summary>
        internal int Size(Context cx) { return rowType.Length; }
        internal Query AddPairs(Query q)
        {
            var r = this;
            for (var b = q.matching.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    r = AddMatchedPair(b.key(), c.key());
            return r;
        }
        /// <summary>
        /// Simply add a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        Query _AddMatchedPair(long a, long b)
        {
            if (matching[a]?.Contains(b)!=true)
            {
                var c = matching[a] ?? BTree<long, bool>.Empty;
                c +=(b, true);
                var d = matching + (a, c);
                return this + (Matching, d);
            }
            return this;
        }
        /// <summary>
        /// Ensure Match relation is transitive after adding a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        internal Query AddMatchedPair(long a, long b)
        {
            var q = _AddMatchedPair(a, b);
            if (q == this)
                return this;
            for (; ; )
            {
                var cur = q;
                for (var c = matching.First(); c != null; c = c.Next())
                    for (var d = matching[c.key()].First(); d != null; d = d.Next())
                        q = q._AddMatchedPair(c.key(), d.key())._AddMatchedPair(d.key(), c.key());
                if (q == cur)
                    return q;
            }
            // not reached
        }
        /// <summary>
        /// We do not explore transitivity! Put extra pairs in for multiple matches.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        internal bool MatchedPair(SqlValue a, SqlValue b)
        {
            return matching[a.defpos]?[b.defpos] == true;
        }
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
        internal static bool Uses(Context cx,BTree<long,bool> x,long t)
        {
            for (var b = x.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.key()]).Uses(cx,t))
                    return true;
            return false;
        }
        internal override bool aggregates(Context cx)
        {
            if (_aggregates)
                return true;
            for (var b = where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].aggregates(cx))
                    return true;
            return false;
        }
        internal Query AddCols(Context cx,Query from)
        {
            var r = this;
            var sl = CList<long>.Empty;
            for (var b = from.rowType.First(); b != null; b = b.Next())
            {
                var c = b.value();
                sl += c;
            }
            return r+(_Domain,new Domain(Sqlx.ROW,cx,sl));
        }
        internal RowSet Ordering(Context cx, RowSet r,bool distinct)
        {
            var os = ordSpec;
            if (os != null && os.Length > 0 && !cx.SameRowType(this,r.rowOrder,os))
                return new OrderedRowSet(cx,r, os, distinct);
            if (distinct)
                return new DistinctRowSet(cx,r);
            return r;
        }
        internal virtual RowSet RowSets(Context cx,BTree<long,RowSet.Finder> fi)
        {
            cx.results += (defpos, defpos);
            return cx.data[defpos];
        }
        internal void CondString(StringBuilder sb, BTree<long, bool> cond, string cm)
        {
            for (var b = cond?.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(Uid(b.key()));
            }
        }
        public string WhereString(BTree<long, bool> svs, BTree<long, TypedValue> mts, 
            TRow pre)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = svs?.First(); b != null; b = b.Next())
            {
                var sw = Uid(b.key());
                if (sw.Length > 1)
                {
                    sb.Append(cm); cm = " and ";
                    sb.Append(sw);
                }
            }
            for (var b = mts?.First(); b != null; b = b.Next())
            {
                var nm = Uid(b.key());
                sb.Append(cm); cm = " and ";
                sb.Append(nm);
                sb.Append("=");
                var tv = b.value();
                if (tv.dataType.kind == Sqlx.CHAR)
                    sb.Append("'" + tv.ToString() + "'");
                else
                    sb.Append(tv.ToString());
            }
            return sb.ToString();
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
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
            }
            sb.Append(")");
            if (assig.Count>0) { sb.Append(" Assigs:"); sb.Append(assig); }
            if (mem.Contains(FetchFirst)) { sb.Append(" FetchFirst="); sb.Append(fetchFirst); }
            if (filter.Count>0) { sb.Append(" Filter:"); sb.Append(filter); }
 //           if (mem.Contains(_Import)) { sb.Append(" Import:"); sb.Append(import); }
            if (matches.Count>0) { sb.Append(" Matches:"); sb.Append(matches); }
            if (matching.Count>0) { sb.Append(" Matching:"); sb.Append(matching); }
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
    }

    // An ad-hoc SystemTable for a row history: the work is mostly done by
    // LogTableSelectBookmark
    internal class LogRowTable :Query
    {
        internal const long
            LogRows = -368, // SystemTable
            TargetTable = -369; // long Table
        public SystemTable logRows => (SystemTable)mem[LogRows]; 
        public long targetTable => (long)(mem[TargetTable]??-1L);
        public LogRowTable(Transaction tr, Context cx, long td, string ta) 
            :base(tr.uid,_Mem(tr,td))
        { }
        static BTree<long,object> _Mem(Transaction tr,long td)
        {
            var r = new BTree<long,object>(TargetTable,tr.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix());
            var tt = new SystemTable("" + td);
            tt += new SystemTableColumn(tt, "Pos", Domain.Int, 1);
            tt += new SystemTableColumn(tt, "Action", Domain.Char, 0);
            tt += new SystemTableColumn(tt, "DefPos", Domain.Int, 0);
            tt += new SystemTableColumn(tt, "Transaction", Domain.Int, 0);
            tt += new SystemTableColumn(tt, "Timestamp", Domain.Timestamp, 0);
            return r + (LogRows, tt) + (_Domain, tt.domain);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(targetTable);
            sb.Append(logRows);
            return sb.ToString();
        }
    }
    /// <summary>
    /// An Ad-hoc SystemTable for a row,column history: the work is mostly done by
    /// LogRowColSelectBookmark
    /// </summary>
    internal class LogRowColTable : Query
    {
        public readonly Table st,tb;
        public readonly long rd, cd;
        public LogRowColTable(Transaction tr, Context cx, long r, long c, string ta)
        : base(tr.uid, BTree<long,object>.Empty)
        {
            var tc = tr.objects[c] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            rd = r;
            cd = c;
            tb = tr.objects[tc.tabledefpos] as Table;
            var tt = new SystemTable("" + rd + ":" + cd);
            tt+=new SystemTableColumn(tt, "Pos", Domain.Int,1);
            tt+=new SystemTableColumn(tt, "Value", Domain.Char,0);
            tt+=new SystemTableColumn(tt, "StartTransaction", Domain.Int,0);
            tt+=new SystemTableColumn(tt, "StartTimestamp", Domain.Timestamp,0);
            tt+=new SystemTableColumn(tt, "EndTransaction", Domain.Int,0);
            tt+=new SystemTableColumn(tt, "EndTimestamp", Domain.Timestamp,0);
            st = tt;
        }
    }
    /// <summary>
    /// Implement a CursorSpecification 
    /// </summary>
    internal class CursorSpecification : Query
    {
        internal const long
            RVQSpecs = -192, // BList<long> QuerySpecification
            RestGroups = -193, // BTree<string,int>
            RestViews = -194, // BTree<long,bool>
            _Source = -195, // string
            Union = -196, // long
            UsingFrom = -197; // long
        /// <summary>
        /// The source string
        /// </summary>
        public string _source=> (string)mem[_Source];
        /// <summary>
        /// The QueryExpression part of the CursorSpecification
        /// </summary>
        public long union => (long)(mem[Union]??-1L);
        /// <summary>
        /// For RESTView implementation
        /// </summary>
        public long usingFrom => (long)(mem[UsingFrom]??-1L);
        /// <summary>
        /// Going up: For a RESTView source, the enclosing QuerySpecifications
        /// </summary>
        internal BList<long> rVqSpecs =>
            (BList<long>)mem[RVQSpecs] ?? BList<long>.Empty;
        /// <summary>
        /// looking down: RestViews contained in this Query and its subqueries
        /// </summary>
        internal BTree<long, bool> restViews =>
            (BTree<long, bool>)mem[RestViews]?? BTree<long, bool>.Empty;
        internal BTree<string, int> restGroups =>
            (BTree<string, int>)mem[RestGroups] ?? BTree<string, int>.Empty;
        /// <summary>
        /// Constructor: a CursorSpecification from the Parser
        /// </summary>
        /// <param name="t">The transaction</param>
        /// <param name="dt">the expected data type</param>
        protected CursorSpecification(long u,BTree<long,object>m)
            : base(u, m) 
        { }
        internal CursorSpecification(long u) : base(u) { }
        internal CursorSpecification(CursorSpecification cs,Query q)
            : this(cs.defpos, cs.mem + (Union,q.defpos)
                  + (_Domain,q.domain)
                  + (Dependents, new BTree<long, bool>(q.defpos, true))
                  + (Depth, 1 + q.depth))
        { }
        public static CursorSpecification operator +(CursorSpecification q, (long, object) x)
        {
            return (CursorSpecification)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CursorSpecification(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new CursorSpecification(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.Scan(rVqSpecs);
            cx.Scan(restViews);
            cx.ObScanned(union);
            cx.ObScanned(usingFrom);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (CursorSpecification)base._Relocate(wr);
            r += (RVQSpecs, wr.Fix(rVqSpecs));
            r += (RestViews, wr.Fix(restViews));
            r += (Union, wr.Fixed(union)?.defpos??-1L);
            r += (UsingFrom, wr.Fixed(usingFrom)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (CursorSpecification)base.Fix(cx);
            r += (RVQSpecs, cx.Fix(rVqSpecs));
            r += (RestViews, cx.Fix(restViews));
            r += (Union,cx.obuids[union]);
            if (usingFrom>=0)
                r += (UsingFrom, cx.obuids[usingFrom]); 
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CursorSpecification)base._Replace(cx, so, sv);
            if (cx._Replace(r.union, so, sv) is Query un && un.defpos != r.union)
                r += (Union,un.defpos);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            ((Query)cx.obs[union]).Refresh(cx);
            return base.Refresh(cx);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[union]).Uses(cx,t) || base.Uses(cx,t);
        }
        /// <summary>
        /// Analysis stage Conditions: do Conditions on the union.
        /// </summary>
        internal override Query Conditions(Context cx)
        {
            var u = (Query)cx.obs[union];
            var r = (CursorSpecification)MoveConditions(cx, u);
            u = (QueryExpression)u.Refresh(cx).Conditions(cx);
            return r._Union(u.Refresh(cx));
        }
        internal override Query Orders(Context cx, CList<long> ord)
        {
            var r = (CursorSpecification)base.Orders(cx, ord);
            return r._Union(((Query)cx.obs[union]).Orders(cx, ord));
        }
        internal override Query AddRestViews(Context cx,CursorSpecification q)
        {
            return _Union((Query)cx.obs[union]).AddRestViews(cx,q);
        }
        /// <summary>
        /// Add a condition and/or update to the QueryWhere. 
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">a set of update assignments</param>
        /// <param name="data">some insert data</param>
        /// <param name="rqC">SqlValues requested from possibly remote contributors</param>
        /// <param name="needed">SqlValues that remote contributors will need to be told</param>
        /// <returns>an updated querywhere, now containing typedvalues for the condition</returns>
        internal override Query AddCondition(Context cx,BTree<long,bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var cs = new CursorSpecification(defpos,base.AddCondition(cx,cond, assigns, data).mem);
            return cs._Union(((Query)cx.obs[cs.union]).AddCondition(cx,cond, assigns, data));
        }
        internal override bool Knows(Context cx,SqlValue c)
        {
            return base.Knows(cx,c) 
                || (((Query)cx.obs[union])?.Knows(cx,c) 
                ?? ((Query)cx.obs[usingFrom])?.Knows(cx,c) ?? false);
        }
        internal override bool aggregates(Context cx)
        {
            return ((Query)cx.obs[union]).aggregates(cx) || base.aggregates(cx);
        }
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            var r = ((Query)cx.obs[union]).RowSets(cx, fi);
            r = Ordering(cx,r,false);
            cx.result = r;
            cx.results += (defpos, r.defpos);
            return r.ComputeNeeds(cx);
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
            return _Union((Query)cx.obs[union]).SetDistinct(cx);
        }
        internal CursorSpecification _Union(Query u)
        {
            return new CursorSpecification(this, u) + (_From, u.from);
        }
        /// <summary>
        /// propagate the Insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs"the rowsets affected></param>
        internal override Context Insert(Context cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, 
            Level cl)
        {
            return ((Query)cx.obs[union]).Insert(cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return ((Query)cx.obs[union]).Update(cx,ur, eqs,rs);
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return ((Query)cx.obs[union]).Delete(cx,dr,eqs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_Source))
            { sb.Append(" Source={"); sb.Append(_source); sb.Append('}'); }
            sb.Append(" Union: "); sb.Append(Uid(union)); 
            if (usingFrom!=-1L)
            { sb.Append(" Using: "); sb.Append(Uid(usingFrom)); }
            if (rVqSpecs.Count>0) { sb.Append(" RVQSpecs:"); sb.Append(rVqSpecs); }
            if (restViews.Count>0)
            { sb.Append(" RestViews:{"); sb.Append(restViews); sb.Append('}'); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement a TableExpression as a subclass of Query
    /// </summary>
    internal class TableExpression : Query
    {
        internal const long
            Target = -198, // long From
            Group = -199, // long GroupSpecification
            Having = -200, // BTree<long,bool> SqlValue
            Windows = -201; // BTree<long,bool> WindowSpecification
        /// <summary>
        /// The from clause of the tableexpression
        /// </summary>
        internal long target => (long)(mem[Target]??-1L);
        /// <summary>
        /// The group specification
        /// </summary>
        internal long group => (long)(mem[Group]??-1L);
        /// <summary>
        /// The having clause
        /// </summary>
        internal BTree<long,bool> having =>
            (BTree<long,bool>)mem[Having]??BTree<long,bool>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal BTree<long,bool> window =>
            (BTree<long,bool>)mem[Windows]??BTree<long,bool>.Empty;
        /// <summary>
        /// Constructor: a tableexpression from the parser
        /// </summary>
        /// <param name="t">the transaction</param>
        internal TableExpression(long u, BTree<long, object> m) : base(u, m) 
        { }
        public static TableExpression operator +(TableExpression q, (long, object) x)
        {
            return (TableExpression)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableExpression(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableExpression(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.ObScanned(target);
            cx.Scan(having);
            cx.Scan(window);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableExpression)base._Relocate(wr);
            r += (Target, wr.Fixed(target).defpos);
            r += (Having, wr.Fix(having));
            r += (Windows, wr.Fix(window));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableExpression)base.Fix(cx);
            r += (Target, cx.obuids[target]);
            r += (Having, cx.Fix(having));
            r += (Windows, cx.Fix(window));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TableExpression)base._Replace(cx, was, now);
            var fm = cx.Replace(r.target, was, now);
            if (fm != r.target)
                r += (Target, fm);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            ((Query)cx.obs[target]).Refresh(cx);
            return base.Refresh(cx);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[target]).Uses(cx,t);
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[target].aggregates(cx) || base.aggregates(cx);
        }
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            if (target == From._static.defpos)
                return new TrivialRowSet(defpos, cx,new TRow(domain),-1L,fi);
            var fr = (Query)cx.obs[target];
            var r = fr.RowSets(cx,fi);
            if (r == null)
                return null;
            if (where.Count > 0)
            {
                var grp = (GroupSpecification)cx.obs[group];
                if (grp != null)
                    for (var gs = grp.sets.First();gs!=null;gs=gs.Next())
                        ((GroupSpecification)cx.obs[gs.value()]).Grouped(cx,where);
            }
            var ma = BTree<long, long>.Empty;
            var cb = r.rt.First();
            for (var b=rowType.First();b!=null&&cb!=null;b=b.Next(),cb=cb.Next())
            {
                var p = b.value();
                var c = cb.value();
                fi += (p, new RowSet.Finder(c, r.defpos));
                ma += (c, p);
            }
            var kt = CList<long>.Empty;
            for (var b = r.keys.First(); b != null; b = b.Next())
                kt += ma[b.value()];
            var rs = new TableExpRowSet(defpos, cx, rowType, kt, r, where, matches, fi);
            cx.results += (defpos, rs.defpos);
            return rs.ComputeNeeds(cx);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = cx.obs[target].StartCounter(cx,rs, tg);
            return base.StartCounter(cx,rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rs, BTree<long, Register> tg)
        {
            tg = cx.obs[target].AddIn(cx,rs,tg);
            return base.AddIn(cx,rs,tg);
        }
        internal override bool Knows(Context cx,SqlValue c)
        {
            return ((Query)cx.obs[target]).Knows(cx,c);
        }
        /// <summary>
        /// propagate an Insert operation
        /// </summary>
        /// <param name="prov">provenance</param>
        /// <param name="data">insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Insert(Context cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return ((Query)cx.obs[target]).Insert(cx,prov, data, eqs, rs,cl);
        }
        /// <summary>
        /// propagate Delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return ((Query)cx.obs[target]).Delete(cx,dr,eqs);
        }
        /// <summary>
        /// propagate Update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return ((Query)cx.obs[target]).Update(cx,ur,eqs,rs);
        }
        internal override Query AddMatches(Context cx, Query q)
        {
            var m = mem;
            for (var b = having.First(); b != null; b = b.Next())
                q = ((SqlValue)cx.obs[b.key()]).AddMatches(cx,q);
            q = ((Query)cx.obs[target]).AddMatches(cx, q);
            cx.Replace(this,q);
            return Refresh(cx);
        }
        internal override Query Conditions(Context cx)
        {
            var fm = (Query)cx.obs[target];
            var r = (TableExpression)MoveConditions(cx, fm).Refresh(cx);
            r.AddHavings(cx, fm);
            fm = fm.Refresh(cx);
            r = (TableExpression)r.AddPairs(fm);
            return (Query)cx.Add(r);
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
        internal override Query Orders(Context cx,CList<long> ord)
        {
            return ((TableExpression)base.Orders(cx,ord))+(Target,((From)cx.obs[target]).Orders(cx,ord).defpos);
        }
        internal override Query AddRestViews(Context cx,CursorSpecification q)
        {
            return this+(Target,((Query)cx.obs[target]).AddRestViews(cx,q).defpos);
        }
        /// <summary>
        /// Add cond and/or update data to this query
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        internal override Query AddCondition(Context cx,BTree<long,bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return (TableExpression)(base.AddCondition(cx,cond, assigns, data)+
            (Target,((From)cx.obs[target]).AddCondition(cx,cond, assigns, data).defpos));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: "); sb.Append(Uid(target));
            if (group != -1L) { sb.Append(" Group:"); sb.Append(Uid(group)); }
            if (having.Count!=0) { sb.Append(" Having:"); sb.Append(having); }
            if (window.Count!=0) { sb.Append(" Window:"); sb.Append(window); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Join is implemented as a subclass of Query
    /// </summary>
    internal class JoinPart : Query
    {
        internal const long
            _FDInfo = -202, // FDJoinPart
            JoinCond = -203, // BTree<long,bool>
            JoinKind = -204, // Sqlx
            LeftOrder = -205, // CList<long>
            LeftOperand = -206, // long Query
            Natural = -207, // Sqlx
            NamedCols = -208, // BList<long> SqlValue
            RightOrder = -209, // CList<long>
            RightOperand = -210; // long Query
        /// <summary>
        /// NATURAL or USING or NO (the default)
        /// </summary>
        public Sqlx naturaljoin => (Sqlx)(mem[Natural]??Sqlx.NO);
        /// <summary>
        /// The list of common TableColumns for natural join
        /// </summary>
        internal BList<long> namedCols => (BList<long>)mem[NamedCols];
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
        internal BTree<long, bool> joinCond => 
            (BTree<long,bool>)mem[JoinCond]??BTree<long, bool>.Empty;
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
        protected JoinPart(long u, BTree<long,object> m)  : base(u,m) { }
        internal JoinPart(long u) : base(u) { }
        public static JoinPart operator+ (JoinPart j,(long,object)x)
        {
            return new JoinPart(j.defpos, j.mem + x);
        }
        public static JoinPart operator -(JoinPart j, (Context,SqlValue) x)
        {
            var (cx, v) = x;
            return (JoinPart)j.Remove(cx,v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinPart(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new JoinPart(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.Scan(joinCond);
            cx.Scan(leftOrder);
            cx.Scan(rightOrder);
            cx.Scan(namedCols);
            cx.ObScanned(left);
            cx.ObScanned(right);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (JoinPart)base._Relocate(wr);
            r += (JoinCond, wr.Fix(joinCond));
            r += (LeftOrder,wr.Fix(leftOrder));
            r += (RightOrder, wr.Fix(rightOrder));
            r += (NamedCols, wr.Fix(namedCols));
            r += (LeftOperand, wr.Fixed(left).defpos);
            r += (RightOperand, wr.Fixed(right).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (JoinPart)base.Fix(cx);
            r += (JoinCond, cx.Fix(joinCond));
            r += (LeftOrder, cx.Fix(leftOrder));
            r += (RightOrder, cx.Fix(rightOrder));
            r += (NamedCols, cx.Fix(namedCols));
            r += (LeftOperand, cx.obuids[left]);
            r += (RightOperand, cx.obuids[right]);
            return r;
        }
        internal override bool Knows(Context cx,SqlValue c)
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
        internal override Query AddMatches(Context cx,Query q)
        {
            var lf = ((Query)cx.obs[left]).AddMatches(cx,q).defpos;
            var rg = ((Query)cx.obs[right]).AddMatches(cx,q).defpos;
            var r = base.AddMatches(cx,q);
            for (var b = joinCond.First(); b != null; b = b.Next())
                r = ((SqlValue)cx.obs[b.key()]).AddMatches(cx,q);
            return (Query)cx.Replace(this,new JoinPart(r.defpos,r.mem
                +(LeftOperand,lf)+(RightOperand,rg)));
        }
        internal override Query Refresh(Context cx)
        {
            ((Query)cx.obs[left]).Refresh(cx);
            ((Query)cx.obs[right]).Refresh(cx);
            return base.Refresh(cx);
        }
        /// <summary>
        /// Analysis stage Selects: call for left and right.
        /// </summary>
        internal Query Selects(Context cx, QuerySpecification qs)
        {
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            var dm = lf.domain;
            for (var b = rg.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                dm += (p, c.domain);
            }
            var r = this + (_Domain,dm);
            var lo = CList<long>.Empty; // left ordering
            var ro = CList<long>.Empty; // right
            if (naturaljoin != Sqlx.NO)
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
                            var cp = new SqlValueExpr(cx.nextHeap++, cx, Sqlx.EQL, lv, rv, Sqlx.NULL);
                            cx.Add(cp);
                            r += (JoinCond, cp.Disjoin(cx));
                            lo += lv.defpos;
                            ro += rv.defpos;
                            r = (JoinPart)r.Remove(cx,rv);
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
                    r += (LeftOperand, 
                        cx.Add(cx.obs[left] + (OrdSpec, lo)).defpos);
                    r += (RightOperand, 
                        cx.Add(cx.obs[right] + (OrdSpec, ro)).defpos);
                }
            }
            else
            {
                var am = BTree<string, bool>.Empty;
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
                    if (lv.isConstant(cx) || rv.isConstant(cx))
                        continue;
                    if (lv.IsFrom(cx, fs, true) && rv.IsFrom(cx, sc, true))
                        continue;
                    if (lv.IsFrom(cx, sc, true) && rv.IsFrom(cx, fs, true))
                    {
                        cx.Replace(se,new SqlValueExpr(se.defpos,cx,se.kind,rv, lv, se.mod));
                        continue;
                    }
                }
                wh += (b.key(), b.value());
                jc -= b.key();
            }
            if (jc!=r.joinCond)
                r = (JoinPart)r.AddCondition(cx, wh) + (JoinCond,jc);
            return (JoinPart)cx.Add(r);
        }
        /// <summary>
        /// Analysis stage Conditions: call for left and right
        /// Turn the join condition into an ordering request, and set it up
        /// </summary>
        internal override Query Conditions(Context cx)
        {
            var r = (JoinPart)base.Conditions(cx);
            var k = kind;
            if (k==Sqlx.CROSS)
                k = Sqlx.INNER;
            for (var b = joinCond.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValueExpr se && se.kind==Sqlx.EQL)
                    r = (JoinPart)r.AddMatchedPair(se.left, se.right);
            r = r +(LeftOperand,((Query)cx.obs[r.left]).AddPairs(r).defpos)
                +(RightOperand,((Query)cx.obs[r.right]).AddPairs(r).defpos);
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
            if (jc.Count == 0)
                k = Sqlx.CROSS;
            var lf = ((Query)cx.obs[r.left]).Conditions(cx).defpos;
            var rg = ((Query)cx.obs[r.right]).Conditions(cx).defpos;
            r = (JoinPart)r.New(r.mem + (LeftOperand, lf) + (RightOperand, rg)
                + (JoinCond, jc) + (JoinKind, k));
            return r.AddCondition(cx,Where,w);
        }
        /// <summary>
        /// Now is the right time to optimise join conditions. 
        /// At this stage all comparisons have form left op right.
        /// Ideally we can find an index that makes at least some of the join trivial.
        /// Then we impose orderings for left and right that respect any remaining comparison join conditions,
        /// overriding ordering requests from top down analysis.
        /// </summary>
        /// <param name="ord">Requested top-down order</param>
        internal override Query Orders(Context cx, CList<long> ord)
        {
            var n = 0;
            var r = this;
            var k = kind;
            var jc = joinCond;
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            // First try to find a perfect foreign key relationship, either way round
            if (GetRefIndex(cx, lf, rg, true) is FDJoinPart fa)
            {
                r += (_FDInfo,fa);
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
                    jc -= b.value().defpos;
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
                        jc -=b.value().defpos;
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
                        lf = lf.Orders(cx,lo+lv.defpos);
                    if (!cx.HasItem(ro,rv.defpos))
                        rg = rg.Orders(cx,ro+rv.defpos);
                }
            if (joinCond.Count == 0)
                for(var b=ord?.First(); b!=null; b=b.Next()) // test all of these 
                {
                    var oi = (SqlValue)cx.obs[b.value()];
                    var lo = lf.ordSpec;
                    var ro = rg.ordSpec;
                    if (lf.HasColumn(cx,oi)// && !(left.rowSet is IndexRowSet))
                        && !cx.HasItem(lo,oi.defpos))
                        lf = lf.Orders(cx,lo+oi.defpos);
                    if (rg.HasColumn(cx,oi)// && !(right.rowSet is IndexRowSet))
                        && !cx.HasItem(ro,oi.defpos))
                        rg = rg.Orders(cx,ro + oi.defpos);
                }
            cx.Add(lf);
            cx.Add(rg);
            return (Query)cx.Add(r + (LeftOperand, lf.defpos) + (RightOperand, rg.defpos) 
                + (JoinKind, k) + (JoinCond, jc));
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
                    if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                    && x.tabledefpos == ta.defpos && x.reftabledefpos == tb.defpos)
                    {
                        var cs = BTree<long,SqlValue>.Empty;
                        var rx = (Index)cx.db.objects[x.refindexdefpos];
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
                                    cs +=(bc.key(), se);
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
                    if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                        x.flags.HasFlag(PIndex.ConstraintType.Unique))
                    && x.tabledefpos == ta.defpos)
                    {
                        var cs = BTree<long,SqlValue>.Empty;
                        var jc = joinCond;
                        for (var bc=x.keys.First();bc!=null;bc=bc.Next())
                        {
                            var found = false;
                            for (var bj = jc.First(); bj != null; bj = bj.Next())
                                if (cx.obs[bj.key()] is SqlValueExpr se
                                    && cx.obs[left ? se.left : se.right] is SqlValue sc
                                    && sc.defpos == bc.value())
                                {
                                    cs +=(bc.key(), se);
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
        /// propagate delete operation
        /// </summary>
        /// <param name="dr">a list of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            cx = ((Query)cx.obs[left]).Delete(cx, dr, eqs);
            return ((Query)cx.obs[right]).Delete(cx,dr,eqs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Insert(Context cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            Eqs(cx,joinCond,ref eqs); // add in equality columns
            cx = ((Query)cx.obs[left]).Insert(cx, prov, data, eqs, rs, cl); // careful: data has extra columns!
            return ((Query)cx.obs[right]).Insert(cx,prov, data, eqs, rs,cl);
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowset</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            cx = ((Query)cx.obs[left]).Update(cx, ur, eqs,rs);
            return ((Query)cx.obs[right]).Update(cx,ur,eqs,rs);
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
        internal override Query AddRestViews(Context cx,CursorSpecification q)
        {
            return this+(LeftOperand,((Query)cx.obs[left]).AddRestViews(cx,q).defpos)
                +(RightOperand,((Query)cx.obs[right]).AddRestViews(cx,q).defpos);
        }
        /// <summary>
        /// Distribute any new where condition to left and right
        /// </summary>
        /// <param name="cond">the condition to add</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">the insert data</param>
        internal override Query AddCondition(Context cx,BTree<long,bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var r = (JoinPart)base.AddCondition(cx,cond, assigns, data);
            var lf = (Query)cx.obs[r.left];
            var rg = (Query)cx.obs[r.right];
            var rc = r.joinCond;
            for (var b=rc.First();b!=null;b=b.Next())
            {
                var c = (SqlValue)cx.obs[b.key()];
                if (c.IsFrom(cx,lf))
                {
                    lf.AddCondition(cx, c);
                    rc -= b.key();
                } 
                else if(c.IsFrom(cx,rg))
                {
                    rg.AddCondition(cx, c);
                    rc -= b.key();
                }
            }
            return (JoinPart)(r + (JoinCond,rc)).Refresh(cx);
        }
        /// <summary>
        /// Analysis stage RowSets: build the join rowset
        /// </summary>
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            var lf = (Query)cx.obs[left];
            var rg = (Query)cx.obs[right];
            for (var b = matches.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.key()];
                if (lf.HasColumn(cx,sv))
                    lf.AddMatch(cx, sv, b.value());
                else
                    rg.AddMatch(cx, sv, b.value());
            }
            if (FDInfo is FDJoinPart fd)
            {
                var ds = new IndexRowSet(cx, fd.table, fd.index,fi);
                var rs = new IndexRowSet(cx, fd.rtable, fd.rindex,fi);
                if (fd.reverse)
                    return new JoinRowSet(cx, this, new SelectedRowSet(cx,lf,ds,fi), 
                        new SelectedRowSet(cx,rg,rs,fi));
                else
                    return new JoinRowSet(cx, this, new SelectedRowSet(cx,lf,rs,fi), 
                        new SelectedRowSet(cx,rg,ds,fi));
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
            return res.ComputeNeeds(cx);
        }
        internal int Compare(Context cx)
        {
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var se = cx.obs[b.key()] as SqlValueExpr;
                var c = cx.obs[se.left].Eval(cx)?.CompareTo(cx.obs[se.right].Eval(cx))??-1;
                if (c != 0)
                    return c;
            }
            return 0;
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
                for(var ic = namedCols.First();ic!=null;ic=ic.Next())
                {
                    sb.Append(comma);
                    sb.Append(ic.value());
                    comma = ",";
                }
            }
            CondString(sb, joinCond, " on ");
            return sb.ToString();
        }
    }
    /// <summary>
    /// Information about functional dependency for join evaluation
    /// </summary>
    internal class FDJoinPart : Basis
    {
        internal const long
            FDConds = -211, // BTree<long,SqlValue> SqlValue ??
            FDIndex = -212, // Index
            FDRefIndex = -213, // Index
            FDRefTable = -214, // Table
            FDTable = -215, // Table
            Reverse = -216; // bool
        /// <summary>
        /// The primary key Index giving the functional dependency
        /// </summary>
        public Index index => (Index)mem[FDIndex];
        public Table table => (Table)mem[FDTable];
        /// <summary>
        /// The foreign key index if any
        /// </summary>
        public Index rindex => (Index)mem[FDRefIndex];
        public Table rtable => (Table)mem[FDRefTable];
        /// <summary>
        /// The joinCond entries moved to this FDJoinPart: the indexing is hierarchical: 0, then 1 etc.
        /// </summary>
        public BTree<long, SqlValue> conds => 
            (BTree<long,SqlValue>)mem[FDConds]??BTree<long, SqlValue>.Empty;
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
        public FDJoinPart(Table tb, Index ix, Table rt, Index rx, BTree<long,SqlValue> c, bool r)
            :base(BTree<long,object>.Empty
                 +(FDIndex,ix)+(FDTable,tb)
                 +(FDRefIndex,rx)+(FDRefTable,rt)
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
        internal override void Scan(Context cx)
        {
            cx.Scan(conds);
            index.Scan(cx);
            table.Scan(cx);
            rindex?.Scan(cx);
            rtable?.Scan(cx);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = this;
            r += (FDConds, wr.Fix(conds));
            r += (FDIndex, index._Relocate(wr));
            r += (FDTable, table._Relocate(wr));
            r += (FDRefIndex, rindex?._Relocate(wr));
            r += (FDRefTable, rtable?._Relocate(wr));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = this;
            r += (FDConds, cx.Fix(conds));
            r += (FDIndex, index.Fix(cx));
            r += (FDTable, table.Fix(cx));
            r += (FDRefIndex, rindex?.Fix(cx));
            r += (FDRefTable, rtable?.Fix(cx));
            return r;
        }
    }
}
