using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level4;
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
    /// QuerySpecification = SELECT [ALL|DISTINCT] SelectList TableExpression .
    /// </summary>
    internal class QuerySpecification : Query
    {
        internal const long
            Distinct = -239, // bool
            RVJoinType = -240, // Domain
            Scope = -241,  // BTree<long,Domain> referenceable DBObjects (in cx.defs)
            TableExp = -242; // TableExpression
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        /// <summary>
        /// the TableExpression
        /// </summary>
        internal TableExpression tableExp => (TableExpression)mem[TableExp];
        /// <summary>
        /// For RESTView work
        /// </summary>
        internal Domain rvJoinType => (Domain)mem[RVJoinType];
        internal BTree<long, Domain> scope =>
            (BTree<long, Domain>)mem[Scope] ?? BTree<long, Domain>.Empty;
        /// <summary>
        /// Constructor: a Query Specification is being built by the parser
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ct">the expected type</param>
        protected QuerySpecification(long u,BTree<long, object> m)
            : base(u, m)
        { }
        internal QuerySpecification(long dp,Context cx,Domain xp) : this(dp, _Mem(cx,xp))
        { }
        static BTree<long,object> _Mem(Context cx,Domain xp)
        {
            var m = BTree<long, object>.Empty;
            if (cx.db.types[xp] is long p)
                m += (_From, p);
            var sc = BTree<long, Domain>.Empty;
            if (xp!=Domain.Content)
            {
                for (var b = xp.representation.First(); b != null; b = b.Next())
                    sc += (b.key(), b.value());
                m += (Scope, sc);
            }
            m += (_Domain, Domain.TableType+(Domain.Display,1));
            return m;
        }
        public static QuerySpecification operator +(QuerySpecification q, (long, object) x)
        {
            return (QuerySpecification)q.New(q.mem + x);
        }
        public static QuerySpecification operator +(QuerySpecification q, (Context, SqlValue) x)
        {
            return (QuerySpecification)q.Add(x.Item1,x.Item2);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySpecification(defpos,m);
        }
        internal override Query Add(Context cx, SqlValue v)
        {
            var r = base.Add(cx, v);
            r += (Scope, scope + (v.defpos, v.domain));
            return r;
        }
        internal bool HasStar(Context cx,Query f=null)
        {
            for (var b=rowType.First();b!=null;b=b.Next())
            {
                if (cx.obs[b.value()] is SqlStar st 
                    && (f==null || (st.prefix < 0
                    || (cx.obs[st.prefix] is SqlValue sv
                        && cx.defs[sv.name].Item1 == f.defpos))))
                    return true;
            }
            return false;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuerySpecification(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.ObScanned(tableExp.defpos);
        }
        internal override Basis _Relocate(Level2.Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (QuerySpecification)base._Relocate(wr);
            var te = tableExp.Relocate(wr);
            if (te != tableExp)
                r += (TableExp, te);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (QuerySpecification)base.Fix(cx);
            var te = tableExp.Fix(cx);
            if (te != tableExp)
                r += (TableExp, te);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            var r =(QuerySpecification)base.Refresh(cx);
            var te = r.tableExp?.Refresh(cx);
            return (te == r.tableExp) ? r : (QuerySpecification)cx.Add(r + (TableExp, te));
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuerySpecification)base._Replace(cx, was, now);
            var te = r.tableExp?._Replace(cx, was, now);
            if (te != r.tableExp)
                r += (TableExp, te);
            cx.done += (defpos, r);
            return r;
        }
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            var r = tableExp?.RowSets(cx,fi);
            if (r==null)
                r = new TrivialRowSet(defpos,cx,new TRow(domain),-1L,fi);
            if (aggregates(cx))
            {
                if (tableExp?.group != -1L)
                    r = new GroupingRowSet(cx,this, r, tableExp.group, tableExp.having);
                else
                    r = new EvalRowSet(cx, this, r); // r.where becomes evalrowset.having
            }
            else
                r = new SelectRowSet(cx, this, r);
            var cols = rowType;
            for (int i = 0; i < Size(cx); i++)
                if (cx.obs[cols[i]] is SqlFunction f && f.window != -1L)
                    f.RowSets(cx,this);
            if (distinct)
                r = new DistinctRowSet(cx,r);
            cx.results += (defpos, r.defpos);
            return r.ComputeNeeds(cx);
        }
        /// <summary>
        /// Analysis stage Conditions().
        /// Move aggregations down if possible.
        /// Check having in the tableexpression
        /// Remember that conditions in the TE are ineffective (TE has no rowsets)
        /// </summary>
        internal override Query Conditions(Context cx)
        {
            var r = this;
            var cols = rowType;
            for (int i = 0; i < Size(cx); i++)
                r = (QuerySpecification)((SqlValue)cx.obs[cols[i]]).Conditions(cx,r,false,out _);
            //      q.CheckKnown(where,tr);
            r = (QuerySpecification)r.MoveConditions(cx, tableExp);
            r += (TableExp,tableExp.Conditions(cx));
            r += (Depth, _Max(r.depth, 1 + r.tableExp.depth));
            return AddPairs(r.tableExp);
        }
        internal override Query Orders(Context cx,CList<long> ord)
        {
            var d = 0;
            for (var b = ord.First(); b != null; b = b.Next())
                d = _Max(d, cx.obs[b.value()].depth);
            return (QuerySpecification)base.Orders(cx,ord)
                +(TableExp,tableExp.Orders(cx,ord) + (Depth, _Max(depth, 1 + d)));
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            return tableExp.Update(cx,ur,eqs,rs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">some data to insert</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            for (var b=rowType.First();b!=null;b=b.Next())
                ((SqlValue)_cx.obs[b.value()]).Eqs(_cx,ref eqs);
            return tableExp.Insert(_cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return tableExp.Delete(cx,dr,eqs);
        }
        internal override Query AddRestViews(Context cx,CursorSpecification q)
        {
            return this+(TableExp,tableExp.AddRestViews(cx,q));
        }
        /// <summary>
        /// Add a cond and/or data to this
        /// </summary>
        /// <param name="cond">the condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override Query AddCondition(Context cx,BTree<long,bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return (new QuerySpecification(defpos,base.AddCondition(cx,cond,assigns, data).mem+
            (TableExp,tableExp.AddCondition(cx,cond,assigns, data))));
        }
        /// <summary>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (distinct)
                sb.Append(" distinct");
            sb.Append(" TableExp ");
            sb.Append((tableExp == null) ? "?" : Uid(tableExp.defpos));
            return sb.ToString();
        }
    }
    /// <summary>
    /// QueryExpression = QueryTerm 
    /// | QueryExpression ( UNION | EXCEPT ) [ ALL | DISTINCT ] QueryTerm .
    /// QueryTerm = QueryPrimary | QueryTerm INTERSECT [ ALL | DISTINCT ] QueryPrimary .
    /// This class handles UNION, INTERSECT and EXCEPT.
    /// </summary>
    internal class QueryExpression : Query
    {
        internal static readonly QueryExpression Get =
                new QueryExpression(-1, BTree<long, object>.Empty
                    +(_Domain,Domain.Null));
        internal const long
            _Distinct = -243, // bool
            _Left = -244,// long Query
            Op = -245, // Sqlx
            _Right = -246, // long QueryExpression
            SimpleTableQuery = -247; //bool
        /// <summary>
        /// The left Query operand
        /// </summary>
        internal long left => (long)(mem[_Left]??-1L);
        /// <summary>
        /// The operator (UNION etc) NO if no operator
        /// </summary>
        internal Sqlx op => (Sqlx)(mem[Op]??Sqlx.NO);
        /// <summary>
        /// The right Query operand
        /// </summary>
        internal long right => (long)(mem[_Right]??-1L);
        /// <summary>
        /// whether distinct has been specified
        /// </summary>
        internal bool distinct => (bool)(mem[_Distinct]??false);
        /// <summary>
        /// Whether we have a simple table query
        /// </summary>
        internal bool simpletablequery => (bool)(mem[SimpleTableQuery]??false);
        /// <summary>
        /// Constructor: a QueryExpression from the parser
        /// </summary>
        /// <param name="t">the context</param>
        /// <param name="a">the left Query</param>
        internal QueryExpression(long u,bool stq, BTree<long, object> m=null)
            : base(u, (m??BTree<long,object>.Empty) + (SimpleTableQuery,stq)
                  +(_Domain,Domain.TableType)) 
        { }
        internal QueryExpression(long u, Context cx, Query a, Sqlx o, QueryExpression b)
            : base(u,BTree<long,object>.Empty+(_Left,a.defpos)+(Op,o)+(_Right,b.defpos)
                  +(_Domain,a.domain)
                  +(Dependents,new BTree<long,bool>(a.defpos,true)+(b.defpos,true))
                  +(Depth,1+_Max(a.depth,b.depth)))
        { }
        internal QueryExpression(long u) : base(u) { }
        internal QueryExpression(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QueryExpression operator+(QueryExpression q,(long,object)x)
        {
            return new QueryExpression(q.defpos,q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QueryExpression(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new QueryExpression(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.ObScanned(left);
            cx.ObScanned(right);
        }
        internal override Basis _Relocate(Level2.Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (QueryExpression)base._Relocate(wr);
            r += (_Left, wr.Fixed(left).defpos);
            r += (_Right, wr.Fixed(right)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (QueryExpression)base.Fix(cx);
            r += (_Left, cx.obuids[left]);
            if (right>=0)
            r += (_Right, cx.obuids[right]);
            return r;
        }
        internal override bool aggregates(Context cx)
        {
            return ((Query)cx.obs[left]).aggregates(cx)||
                (((Query)cx.obs[right])?.aggregates(cx)??false)||base.aggregates(cx);
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QueryExpression)base._Replace(cx, was, now);
            var lf = cx.Replace(r.left, was, now);
            if (lf != r.left)
                r += (_Left, lf);
            var rg = cx.Replace(r.right, was, now);
            if (rg != r.right)
                r += (_Right, rg);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            var r = (QueryExpression)base.Refresh(cx);
            var rr = r;
            if (cx.obs[r.left] is Query lo)
            {
                var lf = lo.Refresh(cx);
                if (lf.defpos != r.left)
                    r += (_Left, lf.defpos);
            }
            if (cx.obs[r.right] is Query ro)
            {
                var rg = ro.Refresh(cx);
                if (rg.defpos != r.right)
                    r += (_Right, rg.defpos);
            }
            return (r == rr) ? rr : (QueryExpression)cx.Add(r);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = cx.obs[left].StartCounter(cx,rs,tg);
            if (right!=-1L) tg = cx.obs[right].StartCounter(cx,rs,tg);
            return base.StartCounter(cx,rs,tg);
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rs, BTree<long, Register> tg)
        {
            tg = cx.obs[left].AddIn(cx,rs,tg);
            if (right!=-1L) tg = cx.obs[right].AddIn(cx,rs,tg);
            return base.AddIn(cx,rs,tg);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,
            BTree<string, bool> dr, Adapters eqs)
        {
            cx = ((Query)cx.obs[left]).Delete(cx, dr, eqs);
            return ((Query)cx.obs[right])?.Delete(cx, dr, eqs)??cx;
        }
        /// <summary>
        ///  propagate an insert operation
        /// </summary>
        /// <param name="prov">provenance</param>
        /// <param name="data">a set of insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Insert(Context cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            cx = ((Query)cx.obs[left]).Insert(cx, prov, data, eqs, rs, cl);
            return ((Query)cx.obs[right])?.Insert(cx,prov, data, eqs, rs,cl)??cx;
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Update(Context cx, 
            BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            cx = ((Query)cx.obs[left]).Update(cx, ur, eqs, rs);
            return ((Query)cx.obs[right])?.Update(cx, ur, eqs, rs)??cx;
        }
        internal override Query AddMatches(Context cx,Query q)
        {
            var r = this;
            if (right == -1L)
                r += (_Left,cx.Add(((Query)cx.obs[left]).AddMatches(cx,q)).defpos);
            return (QueryExpression)cx.Add(r);
        }
        /// <summary>
        /// Analysis stage Conditions().
        /// Check left and right
        /// </summary>
        internal override Query Conditions(Context cx)
        {
            var m = mem;
            var r = (QueryExpression)base.Conditions(cx);
            if (right != -1L)
            {
                m+=(_Left,cx.Add(((Query)cx.obs[left]).AddCondition(cx, where, null, null)).defpos);
                m+=(_Right,cx.Add(((Query)cx.obs[right]).AddCondition(cx, where, null, null)).defpos);
                r = new QueryExpression(defpos, m);
            }
            else
                r = (QueryExpression)MoveConditions(cx, (Query)cx.obs[left]);
            var qe = r;
            ((Query)cx.obs[qe.left]).Conditions(cx);
            if (qe.right != -1L)
                ((Query)cx.obs[qe.right]).Conditions(cx);
            return (QueryExpression)Refresh(cx);
        }
        /// <summary>
        /// Add cond and/or data for modification operations
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">update assignments</param>
        /// <param name="data">insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override Query AddCondition(Context cx,BTree<long,bool> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var q = new QueryExpression(defpos,base.AddCondition(cx, cond, assigns, data).mem);
            q = new QueryExpression(defpos,
                q.mem+(_Left,cx.Add(((Query)cx.obs[left]).AddCondition(cx,cond, assigns, data)).defpos));
            if (q.right!=-1L)
                q = new QueryExpression(defpos,q.mem+(_Right,
                    cx.Add(((QueryExpression)cx.obs[right]).AddCondition(cx,cond, assigns, data)).defpos));
            return q;
        }
        /// <summary>
        /// look to see if we have this column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>wht=ether we have it</returns>
        internal override bool HasColumn(Context cx,SqlValue sv)
        {
            if (!((Query)cx.obs[left]).HasColumn(cx,sv))
                return true;
            if (right != -1L && ((Query)cx.obs[right]).HasColumn(cx,sv))
                return true;
            return base.HasColumn(cx,sv);
        }
        /// <summary>
        /// Analysis stage Orders().
        /// Check left and right.
        /// </summary>
        /// <param name="ord">the default orderitems</param>
        internal override Query Orders(Context cx,CList<long> ord)
        {
            if (ordSpec != null)
                ord = ordSpec;
            var m = base.Orders(cx, ord).mem;
            m += (_Left, cx.Add(((Query)cx.obs[left]).Orders(cx, ord)).defpos);
            if (right != -1L)
                m += (_Right, cx.Add(((Query)cx.obs[right]).Orders(cx, ord)).defpos);
            return new QueryExpression(defpos, m);
        }
        internal override Query AddRestViews(Context cx,CursorSpecification q)
        {
            var m = mem;
            m += (_Left,cx.Add(((Query)cx.obs[left]).AddRestViews(cx,q)).defpos);
            if (right!=-1L)
                m += (_Right,cx.Add(((Query)cx.obs[right]).AddRestViews(cx,q)).defpos);
            return new QueryExpression(defpos, m);
        }
        /// <summary>
        /// propagate distinct
        /// </summary>
        internal override Query SetDistinct(Context cx)
        {
            var m = mem;
            m += (_Left,cx.Add(((Query)cx.obs[left]).SetDistinct(cx)).defpos);
            if (right!=-1L)
                m +=(_Right,cx.Add(((Query)cx.obs[right]).SetDistinct(cx)).defpos);
            return new QueryExpression(defpos, m);
        }
        /// <summary>
        /// Analysis stage RowSets(). Implement UNION, INTERSECT and EXCEPT.
        /// </summary>
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            var lr = ((Query)cx.obs[left]).RowSets(cx,fi);
            if (right != -1L)
            {
                var rr = ((Query)cx.obs[right]).RowSets(cx,lr.finder);
                lr = new MergeRowSet(cx, this, lr, rr, distinct, op);
            }
            var r = Ordering(cx, lr, false);
            if (fetchFirst != -1L)
                r = new RowSetSection(cx, r, 0, fetchFirst);
            cx.results += (defpos, r.defpos);
            return r.ComputeNeeds(cx);
        }
         public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Left: "); sb.Append(Uid(left));
            if (op != Sqlx.NO)
            {
                sb.Append(' '); sb.Append(op);
            }
            sb.Append(" ");
            if (distinct)
                sb.Append("distinct ");
            if (right != -1L)
            {
                sb.Append("Right: ");
                sb.Append(Uid(right)); 
            }
            if (ordSpec != CList<long>.Empty)
            {
                sb.Append(" order by (");
                var cm = "";
                for (var b = ordSpec.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append("=");
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            return sb.ToString();
        }
    }
}
