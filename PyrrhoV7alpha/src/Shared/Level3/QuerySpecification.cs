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
    internal class QuerySpecification : SelectQuery
    {
        internal const long
            Distinct = -255, // bool
            Star = -256, // bool
            RVJoinType = -257, // Domain
            TableExp = -258; // TableExpression
        internal bool selectStar => (bool)(mem[Star]??false);
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
        protected QuerySpecification(QuerySpecification q, SqlValue s) : base(q, s) { }
        public static QuerySpecification operator +(QuerySpecification q, (long, object) x)
        {
            return (QuerySpecification)q.New(q.mem + x);
        }
        public static QuerySpecification operator+(QuerySpecification q,SqlValue s)
        {
            return new QuerySpecification(q, s);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySpecification(defpos,m);
        }
        internal override SelectQuery SelQuery()
        {
            return this;
        }
        internal override QuerySpecification QuerySpec()
        {
            return this;
        }
        internal override Query Refresh(Context cx)
        {
            var q = (QuerySpecification)base.Refresh(cx);
            var te = cx.obs[tableExp.defpos];
            if (te != tableExp)
                q = (QuerySpecification)cx.Add(q + (TableExp, te));
            return q;
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            var r = tableExp.RowSets(tr,cx);
            if (aggregates())
            {
                if (tableExp.group != null)
                    r = new GroupingRowSet(cx,this, r, tableExp.group, tableExp.having);
                else
                    r = new EvalRowSet(tr, cx, r.qry, r, tableExp.having);
            }
            else
                r = new SelectedRowSet(tr, cx, this, r);
            for (int i = 0; i < Size; i++)
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
        internal override Query Conditions(Transaction tr,Context cx,Query q)
        {
            var r = this;
            for (int i = 0; i < Size; i++)
                r = (QuerySpecification)cols[i].Conditions(tr,cx, q, false,out _);
            //      q.CheckKnown(where,tr);
            r = (QuerySpecification)q.MoveConditions(cx, tableExp);
            r += (TableExp,tableExp.Conditions(tr,cx, q));
            if (r.tableExp.having.Count > 0)
            {
                var h = BTree<long, SqlValue>.Empty;
                for (var b=r.tableExp.having.First();b!=null;b=b.Next())
                    h += (b.key(),SqlValue.Setup(tr, cx, q, b.value(), Domain.Bool));
                q = q.Conditions(tr,cx,q);
                r = (QuerySpecification)cx.obs[defpos];
            }
            return AddPairs(r.tableExp);
        }
        internal override Query Orders(Transaction tr,Context cx,OrderSpec ord)
        {
            return (QuerySpecification)base.Orders(tr,cx,ord)
                +(TableExp,tableExp.Orders(tr,cx,ord));
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
            Level cl,bool autokey = false)
        {
            for (var i=0;i<cols.Count;i++)
                cols[i].Eqs(data._tr,_cx,ref eqs);
            return tableExp.Insert(tr,_cx,prov, data, eqs, rs, cl,autokey);
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
        internal override Query AddCondition(Transaction tr,Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return (new QuerySpecification(defpos,base.AddCondition(tr,cx,cond,assigns, data).mem+
            (TableExp,tableExp.AddCondition(tr,cx,cond,assigns, data))));
        }
        internal override bool Knows(Selector c)
        {
            if (base.Knows(c))
                return true;
            if (!aggregates())
                return tableExp.Knows(c);
            return tableExp.group?.Has(c) ?? false;
        }
        /// <summary>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var comma = " ";
            if (distinct)
                sb.Append(" distinct");
            if (selectStar)
                sb.Append(" * ");
            else
                for (int j = 0; j < display; j++)
                {
                    sb.Append(comma);
                    sb.Append(cols[j]);
                    comma = ",";
                }
            sb.Append(" from ");
            sb.Append(tableExp);
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
        internal const long
            _Distinct = -259, // bool
            _Left = -260,// Query
            Op = -261, // Sqlx
            Order = -262, // OrderSpec
            _Right = -263, // QueryExpression
            SimpleTableQuery = -264; //bool
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
        /// The order by clause
        /// </summary>
        internal OrderSpec order => (OrderSpec)mem[Order];
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
        internal QueryExpression(long u, Domain rt, bool stq, BTree<long, object> m=null)
            : base(u, rt, (m ?? BTree<long, object>.Empty) + (SimpleTableQuery, stq))
        { }
        internal QueryExpression(long u, Query a, Sqlx o, QueryExpression b)
            : base(u,BTree<long,object>.Empty+(_Left,a)+(Op,o)+(_Right,b)
                  +(Cols,a.cols)+(Display,a.display)+(SqlValue.NominalType, a.rowType)
                  +(Dependents,BList<long>.Empty+a.defpos+b.defpos)
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
            var q =(QueryExpression)base.Refresh(cx);
            var lf = cx.obs[left.defpos];
            if (lf != left)
                q = (QueryExpression)cx.Add(q + (_Left, lf));
            var rg = cx.obs[right.defpos];
            if (rg != right)
                q = (QueryExpression)cx.Add(q + (_Right, rg));
            return q;
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
            Level cl,bool autokey=false)
        {
            tr = left.Insert(tr, _cx, prov, data, eqs, rs, cl, autokey);
            if (right!=null)
                tr = right.Insert(tr,_cx,prov, data, eqs, rs,cl,autokey);
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
        internal override Query Conditions(Transaction tr,Context cx,Query q)
        {
            var m = mem;
            var r = (QueryExpression)base.Conditions(tr,cx,q);
            if (right != null)
            {
                m+=(_Left,left.AddCondition(tr,cx, where, null, null));
                m+=(_Right,right.AddCondition(tr,cx, where, null, null));
                r = new QueryExpression(defpos, m);
            }
            else
                r = (QueryExpression)MoveConditions(cx, left);
            var qe = r;
            qe = (QueryExpression)qe.left.Conditions(tr,cx,q);
            q = (Query)cx.obs[q.defpos];
            if (qe.right != null)
            {
                r = (QueryExpression)qe.right.Conditions(tr,cx, q);
                q = (Query)cx.obs[q.defpos];
            }
            return q;
        }
        /// <summary>
        /// Add cond and/or data for modification operations
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">update assignments</param>
        /// <param name="data">insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override Query AddCondition(Transaction tr,Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var q = new QueryExpression(defpos,base.AddCondition(tr,cx, cond, assigns, data).mem);
            q = new QueryExpression(defpos,q.mem+(_Left,left.AddCondition(tr,cx,cond, assigns, data)));
            if (q.right!=null)
                q = new QueryExpression(defpos,q.mem+(_Right,right.AddCondition(tr,cx,cond, assigns, data)));
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
        internal override bool Knows(Selector c)
        {
            return left.Knows(c) || (right?.Knows(c) ?? false);
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
            if (right == null)
                return lr;
            var rr = right.RowSets(tr,cx);
            return new MergeRowSet(cx,this, lr, rr, distinct, op);
        }
         public override string ToString()
        {
            var sb = new StringBuilder("QueryExpression (Left: ");
            sb.Append(left);
            sb.Append(") ");
            sb.Append(op);
            sb.Append(" ");
            if (distinct)
                sb.Append("distinct ");
            if (right != null)
            {
                sb.Append("Right: ");
                sb.Append(right); sb.Append(") ");
            }
            if (order != null && order.items.Count!=0)
            {
                sb.Append(" order by (");
                sb.Append(order.items);
                sb.Append(')');
            }
            return sb.ToString();
        }
    }
}
