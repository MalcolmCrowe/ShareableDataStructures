using System;
using System.Collections.Generic;
using System.Text;
using PyrrhoBase;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// QuerySpecification = SELECT [ALL|DISTINCT] SelectList TableExpression .
    /// </summary>
    internal class QuerySpecification : SelectQuery
    {
        public override string Tag => "QS";
        internal bool selectStar = false;
        /// <summary>
        /// whether distinct was specified
        /// </summary>
        internal bool distinct = false;
        /// <summary>
        /// the TableExpression
        /// </summary>
        internal TableExpression tableExp = null;
        /// <summary>
        /// For RESTView work
        /// </summary>
        internal RowType rvJoinType = null;
        /// <summary>
        /// Constructor: a Query Specification is being built by the parser
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ct">the expected type</param>
        internal QuerySpecification(Transaction t, string i, Domain ct)
            : base(t, i, ct)
        {
        }
        protected QuerySpecification(QuerySpecification q,ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs) :base(q,ref cs,ref vs)
        {
            distinct = q.distinct;
            _aggregates = q._aggregates;
            tableExp = (TableExpression)q.tableExp.Copy(ref cs,ref vs);
            CopyContexts(q, cs, vs);
        }
        internal override Query Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new QuerySpecification(this,ref cs,ref vs);
        }
        internal override QuerySpecification QuerySpec(Transaction tr)
        {
            return tr.Ctx(blockid) as QuerySpecification;
        }
        internal override SelectQuery SelQuery()
        {
            return this;
        }
        /// <summary>
        /// Analysis stage Sources(). 
        /// For Select *, copy the select list from the source tableexpression.
        /// Examine the select list for window functions
        /// </summary>
        /// <param name="cx">a parent context</param>
        internal override void Sources(Context cx)
        {
            tableExp.Sources(cx);
            simpleQuery = tableExp.simpleQuery;
            base.Sources(cx);
        }
        /// <summary>
        /// Analysis stage Selects().
        /// Setup operands in the select list.
        /// </summary>
        /// <param name="spec">where the select list comes from</param>
        internal override void Selects(Transaction tr, Query spec)
        {
            tableExp.enc = this; // ??
            tableExp.Selects(tr, spec);
            if (selectStar && cols.Count == 0) // make a new display from the sources
            {
                for (int i = 0; i < tableExp.Size; i++)
                    Add(tr, tableExp.ValAt(i).Export(tr, this), tableExp.names[i]);
                if (display == 0)
                    display = tableExp.display;
            }
            else
            {
                var sc = new List<TColumn>();
                for (int i = 0; i < display; i++)
                {
                    SqlValue.Setup(tr, spec, cols[i], Domain.Content);
                    var n = names[i];
                    if (n.Defpos() <= 0)
                        n.Set(tr,cols[i].name);
                    sc.Add(new TColumn(n, ValAt(i).nominalDataType.New(tr)));
                }
                for (int i = display; i < Size; i++)
                {
                    SqlValue.Setup(tr,spec,cols[i], Domain.Content);
                    var n = names[i];
                    n.Set(tr,cols[i].name);
                    if (needs.Contains(cols[i].name))
                        sc.Add(new TColumn(n, ValAt(i).nominalDataType.New(tr)));
                }
                var ndt = new TableType(sc.ToArray());
                nominalDataType = nominalDataType.LimitBy(ndt);
            }
            for (var b = tableExp.where.First(); b != null; b = b.Next())
                if (!where.Contains(b.key()))
                    ATree<long, SqlValue>.Add(ref where, b.key(), b.value());
            base.Selects(tr, spec);//,ref dfs);
        }
        // redo some of the above if we need to add a column later
        internal void Selects1(Transaction tr)
        {
            var sc = new List<TColumn>();
            for (int i = 0; i < display; i++)
            {
                var n = names[i];
                if (i < display)
                    sc.Add(new TColumn(n, ValAt(i).nominalDataType.New(tr)));
            }
            for (int i = display; i < Size; i++)
            {
                var n = names[i];
                n.Set(tr,cols[i].name);
                if (needs.Contains(cols[i].name))
                    sc.Add(new TColumn(n, ValAt(i).nominalDataType.New(tr)));
            }
            var ndt = new TableType(sc.ToArray());
      //      if (display == 1)
      //          nominalDataType = new Domain(Sqlx.UNION,
      //             new Domain[2] { ndt, ndt[0] });
      //      else
                nominalDataType = ndt;
        }
        /// <summary>
        /// For Views and subqueries we will build an ExportedRowSet.
        /// This recursion is called for f.source in the RowSets phase
        /// (and so precedes RESTView.Conditions)
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal override void AddMatches(Transaction tr, Query q)
        {
            tableExp.AddMatches(tr, q);
            if (q.Size != Size) // panic
            {
                Console.WriteLine("Add Matches Panic");
                return;
            }
            for (var i = 0; i < Size; i++)
                q.AddMatchedPair(cols[i], q.cols[i]);
            base.AddMatches(tr, q);
        }
        /// <summary>
        /// Analysis stage Conditions().
        /// Move aggregations down if possible.
        /// Check having in the tableexpression
        /// Remember that conditions in the TE are ineffective (TE has no rowsets)
        /// </summary>
        internal override void Conditions(Transaction tr,Query q)
        {
            for (int i = 0; i < Size; i++)
                cols[i].Conditions(tr, q, false);
            //      q.CheckKnown(where,tr);
            MoveConditions(ref where, tr, tableExp);
            tableExp.Conditions(tr, q);
            if (tableExp.having.Count > 0)
            {
                SqlValue.Setup(tr, q, tableExp.having, Domain.Bool);
                q.Conditions(ref tableExp.having, tr);
            }
            AddPairs(tableExp);
        }
        internal override void Orders(Transaction tr,OrderSpec ord)
        {
            base.Orders(tr,ord);
            tableExp.Orders(tr,ord);
        }
        /// <summary>
        /// delegate the accessibleCols
        /// </summary>
        /// <returns>the selectors</returns>
        internal override Selector[] AccessibleCols()
        {
            return tableExp.AccessibleCols();
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Update(Transaction tr,ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return tableExp.Update(tr,ur,eqs, rs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">some data to insert</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Insert(string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey = false)
        {
            for (var i=0;i<cols.Count;i++)
                cols[i].Eqs(data.tr,ref eqs);
            return tableExp.Insert(prov, data, eqs, rs, cl,autokey);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override int Delete(Transaction tr,ATree<string, bool> dr, Adapters eqs)
        {
            return tableExp.Delete(tr,dr,eqs);
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            tableExp.AddRestViews(q);
        }
        internal void UpdateQuerySpec(Transaction tr,SqlValue so,SqlValue sn,ref ATree<long,SqlValue> map)
        {
            UpdateCols(tr, so, sn, ref map);
            tableExp.from.UpdateCols(tr, so, sn, ref map);
            _aggregates = false;
            for (var i = 0; i < cols.Count; i++)
                _aggregates = _aggregates || cols[i].aggregates();
        }
        /// <summary>
        /// Add a cond and/or data to this
        /// </summary>
        /// <param name="cond">the condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override void AddCondition(Transaction tr,ATree<long,SqlValue> cond, UpdateAssignment[] assigns, RowSet data)
        {
            base.AddCondition(tr,cond,assigns, data);
            tableExp.AddCondition(tr,cond,assigns, data);
        }
        /// <summary>
        /// Analysis stage RowSets().
        /// Implement grouping and aggregation. Compute any required window functions.
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            tableExp.RowSets(tr);
            if (aggregates())
            {
                if (tableExp.group != null)
                    rowSet = new GroupingRowSet(this, tableExp.rowSet, tableExp.group, tableExp.having);
                else
                    rowSet = new EvalRowSet(this, tableExp.rowSet.qry, tableExp.rowSet, tableExp.having);
            }
            else
                rowSet = new SelectedRowSet(tr, tableExp, this);
            for (int i = 0; i < Size; i++)
                if (cols[i] is SqlFunction f && f.window != null)
                    f.RowSets(tr,this);
            base.RowSets(tr);
            if (distinct)
                rowSet = new DistinctRowSet(this,rowSet);
        }
        internal override bool Knows(SqlTypeColumn c)
        {
            if (base.Knows(c))
                return true;
            if (!aggregates())
                return tableExp.Knows(c);
            return tableExp.group?.Has(c.name) ?? false;
        }
        /// <summary>
        public override string ToString()
        {
            var sb = new StringBuilder("select");
            var comma = " ";
            if (distinct)
                sb.Append(" distinct");
            if (selectStar)
                sb.Append(" * ");
            else
                for (int j = 0; j < display; j++)
                {
                    sb.Append(comma);
                    sb.Append(names[j]);
                    comma = ",";
                }
            sb.Append(" from ");
            sb.Append(tableExp.ToString());
            sb.Append(base.ToString());
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
        public override string Tag => "QE";
        /// <summary>
        /// The left Query operand
        /// </summary>
        internal Query left;
        /// <summary>
        /// The operator (UNION etc) NO if no operator
        /// </summary>
        internal Sqlx op;
        /// <summary>
        /// The right Query operand
        /// </summary>
        internal QueryExpression right;
        /// <summary>
        /// whether distinct has been specified
        /// </summary>
        internal bool distinct = false;
        /// <summary>
        /// The order by clause
        /// </summary>
        internal OrderSpec order = null;
        /// <summary>
        /// Whether we have a simple table query
        /// </summary>
        internal bool simpletablequery = false;
        /// <summary>
        /// Used for View processing: lexical positions of ends of columns
        /// </summary>
        internal ATree<int, Ident> viewAliases = BTree<int, Ident>.Empty;
        /// <summary>
        /// Constructor: a QueryExpression from the parser
        /// </summary>
        /// <param name="t">the context</param>
        /// <param name="a">the left Query</param>
        internal QueryExpression(Transaction t, string i, Domain k, bool stq)
            : base(t,i,k)
        {
            right = null; op = Sqlx.NO; simpletablequery = stq;
        }
        /// <summary>
        /// Constructor: a QueryExpression from the parser
        /// </summary>
        /// <param name="t">the context</param>
        /// <param name="a">the left Query</param>
        /// <param name="sv">the operator (NO if none)</param>
        /// <param name="b">the right Query</param>
        /// <param name="m">ALL or DISTINCT</param>
        internal QueryExpression(Transaction t, string i, Query a, Sqlx o, QueryExpression b, Sqlx m)
            : base(t,i, a.nominalDataType)
        {
            left = a; op = o; right = b;
            CopyCols(left);
            distinct = (m != Sqlx.ALL);
            if (distinct)
            {
                left.SetDistinct();
                right.SetDistinct();
            }
        }
        protected QueryExpression(QueryExpression q, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(q,ref cs,ref vs)
        {
            left = q.left.Copy(ref cs,ref vs);
            op = q.op;
            right = (QueryExpression)q.right?.Copy(ref cs,ref vs);
            distinct = q.distinct;
            order = q.order;
            simpletablequery = q.simpletablequery;
            ATree<string, Context>.Add(ref cs, left.blockid, left);
            if (right != null)
                ATree<string, Context>.Add(ref cs, right.blockid, right);
            CopyContexts(q, cs, vs);
        }
        internal override Query Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new QueryExpression(this,ref cs,ref vs);
        }
        /// <summary>
        /// Analysis stage Sources(). check left and right
        /// </summary>
        /// <param name="cx"></param>
        internal override void Sources(Context cx)
        {
            left.Sources(cx);
            if (right != null)
                right.Sources(cx);
            else
            {
                simpleQuery = left.simpleQuery;
                for (int i = 0; i < left.Size; i++)
                    Add(cx, left.ValAt(i), left.names[i]);
                if (fetchFirst >= 0)
                    left.fetchFirst = fetchFirst;
            }
            base.Sources(cx);
        }
        /// <summary>
        /// Analysis stage Selects(). 
        /// Row types should be assignment-compatible (TBD).
        /// </summary>
        internal override void Selects(Transaction tr,Query spec)
        {
            bool changed = false;
            left.Selects(tr, spec);
            cols = left.cols;
            names = left.names;
            nominalDataType = left.nominalDataType;
            display = left.display;
            if (right != null)
                right.Selects(tr, spec);
            if (order != null)
            {
                ordSpec = order;
                foreach (var oi in order.items)
                {
                    SqlValue.Setup(tr,this,oi.what, Domain.Content);
                    var n = oi.what.name;
                    if (!left.nominalDataType.names[n.ToString()].HasValue)
                    {
                        changed = left.MaybeAdd(tr,n,oi.what);
                        ATree<Ident,Need>.Add(ref needs, oi.what.name, Need.selected);
                    }
                }
            }
            if (changed && left is QuerySpecification qs)
                qs.Selects1(tr);
            base.Selects(tr, spec);
        }
        internal override bool aggregates()
        {
            return left.aggregates()||(right?.aggregates()??false)||base.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            left.Build(rs);
            right?.Build(rs);
            base.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            left.StartCounter(tr, rs);
            right?.StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void AddIn(Transaction tr, RowSet rs)
        {
            left.AddIn(tr, rs);
            right?.AddIn(tr, rs);
            base.AddIn(tr, rs);
        }
        /// <summary>
        /// propagate a delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override int Delete(Transaction tr,ATree<string, bool> dr, Adapters eqs)
        {
            return left.Delete(tr, dr, eqs) + (right?.Delete(tr, dr, eqs) ?? 0);
        }
        /// <summary>
        ///  propagate an insert operation
        /// </summary>
        /// <param name="prov">provenance</param>
        /// <param name="data">a set of insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Insert(string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey=false)
        {
            return left.Insert(prov, data, eqs, rs,cl,autokey) + 
                (right?.Insert(prov, data, eqs, rs,cl,autokey) ??0);
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Update(Transaction tr,ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return left.Update(tr, ur, eqs, rs) + (right?.Update(tr, ur, eqs, rs) ?? 0);
        }
        internal override void AddMatches(Transaction tr, Query q)
        {
            if (right==null)
                left.AddMatches(tr, q);
            base.AddMatches(tr, q);
        }
        /// <summary>
        /// Analysis stage Conditions().
        /// Check left and right
        /// </summary>
        internal override void Conditions(Transaction tr,Query q)
        {
            if (right != null)
            {
                left.AddCondition(tr, where, null, null);
                right.AddCondition(tr, where, null, null);
            }
            else
                MoveConditions(ref where,tr, left);
            left.Conditions(tr,q);
            right?.Conditions(tr,q);
            base.Conditions(tr,q);
        }
        /// <summary>
        /// Add cond and/or data for modification operations
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">update assignments</param>
        /// <param name="data">insert data</param>
        /// <returns>an updated querywhere</returns>
        internal override void AddCondition(Transaction tr,ATree<long,SqlValue> cond, UpdateAssignment[] assigns, RowSet data)
        {
            base.AddCondition(tr,cond, assigns, data);
            left.AddCondition(tr,cond, assigns, data);
            right?.AddCondition(tr,cond, assigns, data);
        }
        /// <summary>
        /// look to see if we have this column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>wht=ether we have it</returns>
        internal override bool HasColumn(Ident s)
        {
            if (!left.HasColumn(s))
                return true;
            if (right != null && right.HasColumn(s))
                return true;
            return base.HasColumn(s);
        }
        internal override bool Knows(SqlTypeColumn c)
        {
            return left.Knows(c) || (right?.Knows(c) ?? false);
        }
        /// <summary>
        /// delegate accessible columns
        /// </summary>
        /// <returns>the selectors</returns>
        internal override Selector[] AccessibleCols()
        {
            return left.AccessibleCols();
        }
        /// <summary>
        /// Analysis stage Orders().
        /// Check left and right.
        /// </summary>
        /// <param name="ord">the orderitems</param>
        internal override void Orders(Transaction tr,OrderSpec ord)
        {
            if (ordSpec != null)
                ord = ordSpec;
            base.Orders(tr,ord);
            left.Orders(tr,ord);
            right?.Orders(tr,ord);
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            left.AddRestViews(q);
            right?.AddRestViews(q);
        }
        /// <summary>
        /// propagate distinct
        /// </summary>
        internal override void SetDistinct()
        {
            left.SetDistinct();
            right?.SetDistinct();
        }
        /// <summary>
        /// Analysis stage RowSets(). Implement UNION, INTERSECT and EXCEPT.
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            left.RowSets(tr);
            if (right == null)
            {
                rowSet = left.rowSet;
                Ordering(false);
                return;
            }
            right.RowSets(tr);
            nominalDataType = left.nominalDataType;
            rowSet = new MergeRowSet(left, right, distinct, op);
        }
         public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(left.ToString());
            sb.Append(" ");
            sb.Append(op.ToString());
            sb.Append(" ");
            if (distinct)
                sb.Append("distinct ");
            if (right!=null)
                sb.Append(right.ToString());
            if (order != null && order.items.Count!=0)
            {
                sb.Append(" order by (");
                var c = "";
                foreach (var oi in order.items)
                {
                    sb.Append(c);
                    sb.Append(oi.ToString());
                    c = ",";
                }
            }
            return sb.ToString();
        }
        /// <summary>
        /// close the query
        /// </summary>
        internal override void Close(Transaction tr)
        {
            left.Close(tr);
            right?.Close(tr);
        }
    }
}
