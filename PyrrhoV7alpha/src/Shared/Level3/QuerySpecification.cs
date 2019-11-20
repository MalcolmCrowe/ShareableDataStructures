using System;
using System.Collections.Generic;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
    /// <summary>
    /// QuerySpecification = SELECT [ALL|DISTINCT] SelectList TableExpression .
    /// </summary>
    internal class QuerySpecification : Query
    {
        internal const long
            Distinct = -239, // bool
            RVJoinType = -241, // Domain
            TableExp = -242; // TableExpression
        internal static readonly QuerySpecification Default =
            new QuerySpecification(Transaction.TransPos,
                BTree<long, object>.Empty
                + (RowType, ObInfo.Any + SqlStar.Default) +
                (Display, 1));
        internal bool distinct => (bool)(mem[Distinct] ?? false);
        /// <summary>
        /// the TableExpression
        /// </summary>
        internal TableExpression tableExp => (TableExpression)mem[TableExp];
        /// <summary>
        /// For RESTView work
        /// </summary>
        internal Domain rvJoinType => (Domain)mem[RVJoinType];
        /// <summary>
        /// Constructor: a Query Specification is being built by the parser
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ct">the expected type</param>
        internal QuerySpecification(long u,BTree<long, object> m)
            : base(u, m)
        { }
        public static QuerySpecification operator +(QuerySpecification q, (long, object) x)
        {
            return (QuerySpecification)q.New(q.mem + x);
        }
        public static QuerySpecification operator +(QuerySpecification q, SqlValue x)
        {
            return (QuerySpecification)q.New((((Query)q) + x).mem);
        }
        public static QuerySpecification operator -(QuerySpecification q, long col)
        {
            return (QuerySpecification)q.Remove(col);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySpecification(defpos,m);
        }
        internal override Query SelQuery()
        {
            return this;
        }
        internal override QuerySpecification QuerySpec()
        {
            return this;
        }
        internal override Query Refresh(Context cx)
        {
            var r =(QuerySpecification)base.Refresh(cx);
            var te = r.tableExp?.Refresh(cx);
            return (te == r.tableExp) ? r : (QuerySpecification)cx.Add(r + (TableExp, te));
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            var r = tableExp?.RowSets(tr,cx)??new TrivialRowSet(tr,cx,this,null);
            if (aggregates())
            {
                if (tableExp?.group != null)
                    r = new GroupingRowSet(cx,this, r, tableExp.group, tableExp.having);
                else
                    r = new EvalRowSet(tr, cx, this, r, tableExp.having);
            }
            else
                r = new SelectedRowSet(tr, cx, this, r);
            var cols = rowType.columns;
            for (int i = 0; i < Size(cx); i++)
                if (cols[i] is SqlFunction f && f.window != null)
                    f.RowSets(tr, this);
            if (distinct)
                r = new DistinctRowSet(cx,r);
            return r;
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
            var cols = rowType.columns;
            for (int i = 0; i < Size(cx); i++)
                r = (QuerySpecification)((SqlValue)cols[i]).Conditions(cx,r,false,out _);
            //      q.CheckKnown(where,tr);
            r = (QuerySpecification)r.MoveConditions(cx, tableExp);
            r += (TableExp,tableExp.Conditions(cx));
            r += (Depth, _Max(r.depth, 1 + r.tableExp.depth));
            return AddPairs(r.tableExp);
        }
        internal override Query Orders(Transaction tr,Context cx,OrderSpec ord)
        {
            var d = 0;
            for (var b = ord.items.First(); b != null; b = b.Next())
                d = _Max(d, b.value().depth);
            return (QuerySpecification)base.Orders(tr,cx,ord)
                +(TableExp,tableExp.Orders(tr,cx,ord) + (Depth, _Max(depth, 1 + d)));
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Update(Transaction tr,Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            return tableExp.Update(tr,cx,ur,eqs,rs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">some data to insert</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Insert(Transaction tr,Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            var cols = rowType.columns;
            for (var i=0;i<cols.Count;i++)
                cols[i].Eqs(data._tr,_cx,ref eqs);
            return tableExp.Insert(tr,_cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr,Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return tableExp.Delete(tr,cx,dr,eqs);
        }
        internal override Query AddRestViews(CursorSpecification q)
        {
            return this+(TableExp,tableExp.AddRestViews(q));
        }
        /// <summary>
        /// Add a cond and/or data to this
        /// </summary>
        /// <param name="cond">the condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override Query AddCondition(Context cx,BTree<long,SqlValue> cond,
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
            sb.Append(" from ");
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
                new QueryExpression(-1, BTree<long, object>.Empty);
        internal const long
            _Distinct = -243, // bool
            _Left = -244,// Query
            Op = -245, // Sqlx
            _Right = -246, // QueryExpression
            SimpleTableQuery = -247; //bool
        /// <summary>
        /// The left Query operand
        /// </summary>
        internal Query left => (Query)mem[_Left];
        /// <summary>
        /// The operator (UNION etc) NO if no operator
        /// </summary>
        internal Sqlx op => (Sqlx)(mem[Op]??Sqlx.NO);
        /// <summary>
        /// The right Query operand
        /// </summary>
        internal QueryExpression right => (QueryExpression)mem[_Right];
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
            : base(u, (m??BTree<long,object>.Empty) + (SimpleTableQuery,stq)) 
        { }
        internal QueryExpression(long u, Context cx, ObInfo rt, bool stq, BTree<long, object> m=null)
            : base(u, cx, rt, (m ?? BTree<long, object>.Empty) + (SimpleTableQuery, stq))
        { }
        internal QueryExpression(long u, Context cx, Query a, Sqlx o, QueryExpression b)
            : base(u,BTree<long,object>.Empty+(_Left,a)+(Op,o)+(_Right,b)
                  +(Display,a.display)+(RowType,a.rowType.Relocate(u))
                  +(Dependents,new BTree<long,bool>(a.defpos,true)+(b.defpos,true))
                  +(Depth,1+_Max(a.depth,b.depth)))
        { }
        internal QueryExpression(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QueryExpression operator+(QueryExpression q,(long,object)x)
        {
            return new QueryExpression(q.defpos,q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QueryExpression(defpos,m);
        }
        internal override bool aggregates()
        {
            return left.aggregates()||(right?.aggregates()??false)||base.aggregates();
        }
        internal override Query Refresh(Context cx)
        {
            var r = (QueryExpression)base.Refresh(cx);
            var rr = r;
            var lf = r.left.Refresh(cx);
            if (lf != r.left)
                r += (_Left, lf);
            var rg = r.right.Refresh(cx);
            if (rg != r.right)
                r += (_Right, rg);
            return (r == rr) ? rr : (QueryExpression)cx.Add(r);
        }
        internal override void Build(Context _cx, RowSet rs)
        {
            left.Build(_cx,rs);
            right?.Build(_cx,rs);
            base.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            left.StartCounter(_cx,rs);
            right?.StartCounter(_cx,rs);
            base.StartCounter(_cx,rs);
        }
        internal override void AddIn(Context _cx, RowBookmark rs)
        {
            left.AddIn(_cx,rs);
            right?.AddIn(_cx,rs);
            base.AddIn(_cx,rs);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr,Context cx,
            BTree<string, bool> dr, Adapters eqs)
        {
            tr = left.Delete(tr, cx, dr, eqs);
            if (right!=null)
                tr = right.Delete(tr, cx, dr, eqs);
            return tr;
        }
        /// <summary>
        ///  propagate an insert operation
        /// </summary>
        /// <param name="prov">provenance</param>
        /// <param name="data">a set of insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Insert(Transaction tr,Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            tr = left.Insert(tr, _cx, prov, data, eqs, rs, cl);
            if (right!=null)
                tr = right.Insert(tr,_cx,prov, data, eqs, rs,cl);
            return tr;
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Update(Transaction tr,Context cx, 
            BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            tr = left.Update(tr, cx, ur, eqs, rs);
            if (right!=null)
                tr = right.Update(tr, cx, ur, eqs, rs);
            return tr;
        }
        internal override Query AddMatches(Context cx, Query q)
        {
            var r = this;
            if (right == null)
                r += (_Left,left.AddMatches(cx, q));
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
            if (right != null)
            {
                m+=(_Left,left.AddCondition(cx, where, null, null));
                m+=(_Right,right.AddCondition(cx, where, null, null));
                r = new QueryExpression(defpos, m);
            }
            else
                r = (QueryExpression)MoveConditions(cx, left);
            var qe = r;
            qe.left.Conditions(cx);
            if (qe.right != null)
                qe.right.Conditions(cx);
            return (QueryExpression)Refresh(cx);
        }
        /// <summary>
        /// Add cond and/or data for modification operations
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">update assignments</param>
        /// <param name="data">insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override Query AddCondition(Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var q = new QueryExpression(defpos,base.AddCondition(cx, cond, assigns, data).mem);
            q = new QueryExpression(defpos,q.mem+(_Left,left.AddCondition(cx,cond, assigns, data)));
            if (q.right!=null)
                q = new QueryExpression(defpos,q.mem+(_Right,right.AddCondition(cx,cond, assigns, data)));
            return q;
        }
        /// <summary>
        /// look to see if we have this column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>wht=ether we have it</returns>
        internal override bool HasColumn(SqlValue sv)
        {
            if (!left.HasColumn(sv))
                return true;
            if (right != null && right.HasColumn(sv))
                return true;
            return base.HasColumn(sv);
        }
        /// <summary>
        /// Analysis stage Orders().
        /// Check left and right.
        /// </summary>
        /// <param name="ord">the orderitems</param>
        internal override Query Orders(Transaction tr,Context cx,OrderSpec ord)
        {
            if (ordSpec != null)
                ord = ordSpec;
            var m = base.Orders(tr,cx, ord).mem;
            m += (_Left, left.Orders(tr,cx, ord));
            if (right != null)
                m += (_Right, right.Orders(tr,cx, ord));
            return new QueryExpression(defpos, m);
        }
        internal override Query AddRestViews(CursorSpecification q)
        {
            var m = mem;
            m += (_Left,left.AddRestViews(q));
            if (right!=null)
                m += (_Right,right.AddRestViews(q));
            return new QueryExpression(defpos, m);
        }
        /// <summary>
        /// propagate distinct
        /// </summary>
        internal override Query SetDistinct()
        {
            var m = mem;
            m += (_Left,left.SetDistinct());
            if (right!=null)
                m +=(_Right,right.SetDistinct());
            return new QueryExpression(defpos, m);
        }
        /// <summary>
        /// Analysis stage RowSets(). Implement UNION, INTERSECT and EXCEPT.
        /// </summary>
        internal override RowSet RowSets(Transaction tr,Context cx)
        {
            var lr = left.RowSets(tr,cx);
            if (right != null)
            {
                var rr = right.RowSets(tr, cx);
                lr = new MergeRowSet(cx, this, lr, rr, distinct, op);
            }
            return Ordering(cx, lr, false);
        }
         public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Left: "); sb.Append(Uid(left.defpos));
            sb.Append(") ");
            sb.Append(op);
            sb.Append(" ");
            if (distinct)
                sb.Append("distinct ");
            if (right != null)
            {
                sb.Append("Right: ");
                sb.Append(Uid(right.defpos)); sb.Append(") ");
            }
            if (ordSpec != null && ordSpec.items.Count!=0)
            {
                sb.Append(" order by (");
                sb.Append(ordSpec.items);
                sb.Append(')');
            }
            return sb.ToString();
        }
    }
}
