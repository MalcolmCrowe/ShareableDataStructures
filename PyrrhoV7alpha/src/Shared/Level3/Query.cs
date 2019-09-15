using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
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
    /// Query Processing
    /// Queries include joins, groupings, window functions and subqueries. 
    /// Some queries have select lists and predicates etc (which are all SqlValues). 
    /// A Query's outer context is the enclosing query if any.
    /// Following analysis every Query has a mapping of SqlValues to columns in its result RowSet. 
    /// SqlValueExpr is a syntax-derived structure for evaluating the value of an SQL expression. 
    /// A RowSet’s outer context is its query.
    /// Analysis performs the following passes over this
    /// (1) Sources: The From list is analysed during Context (by From constructor) (allows us to interpret *)
    /// (2) Selects: Next step is to process the SelectLists to define Selects (and update SqlValues) (create Evaluation steps if required)
    /// (3) Conditions: Then examine SearchConditions and move them down the tree if possible
    /// (4) Orders: Then examine Orders and move them down the tree if possible (not below SearchConditions though) create Evaluation and ordering steps if required)
    /// (5) RowSets: Compute the rowset for the given query
    /// </summary>
    internal class Query : DBObject
    {
        internal const long
            Assig = -183, // BTree<UpdateAssignment,bool>
            Cols = -184, // BList<SqlValue>
            Data = -185, // RowSet
            Display = -186, // int
            Enc = -187, // Query
            FetchFirst = -188, // int
            Filter = -283, // BTree<long,TypedValue>
            _Import = -189, // BTree<SqlValue,SqlValue>
            Matches = -190, // BTree<SqlValue,TypedValue>
            Matching = -191, // BTree<SqlValue,ATree<SqlValue,bool>>
            OrdSpec = -192, // OrderSpec
            Periods = -193, // BTree<long,PeriodSpec>
            _Replace = -194, // BTree<string,string>
            SCols = -195, // BTree<SqlValue,int?>
            SimpleQuery = -197, // From
            Where = -200; // BTree<long,SqlValue>
        /// <summary>
        /// Supplied for QuerySpecification; constructed for FROM items during Sources().
        /// What happens for other forms of Query is more debatable: so we provide a careful API.
        /// void Add(SqlValue v, Ident n, Ident a=null) will add a new column to the associated result RowSet.
        /// int Size will give cols.Count=selects.Count (display.LEQ.Size).
        /// defs can have extra entries to help with Selector aliasing.
        /// SqlValue ValAt(int i) returns cols[i] or null if out of range
        /// SqlValue ValFor(string a) returns v such that v.name==a or null if not found
        /// int ColFor(SqlValue v) returns i such that cols[i]==v or -1 if not found
        /// int ColFor(string a) returns i such that cols[i].name==a or -1 if not found
        /// </summary>
        internal BList<SqlValue> cols => (BList<SqlValue>)mem[Cols]?? BList<SqlValue>.Empty;
        internal BTree<SqlValue,int?> scols => 
            (BTree<SqlValue,int?>)mem[SCols]??BTree<SqlValue,int?>.Empty;
        internal Table simpleQuery => (Table)mem[SimpleQuery]; // will be non-null for simple queries and insertable views
        internal BTree<SqlValue, TypedValue> matches =>
             (BTree<SqlValue, TypedValue>)mem[Matches] ?? BTree<SqlValue, TypedValue>.Empty; // guaranteed constants
        internal BTree<SqlValue, BTree<SqlValue, bool>> matching =>
            (BTree<SqlValue, BTree<SqlValue, bool>>)mem[Matching] ?? BTree<SqlValue, BTree<SqlValue, bool>>.Empty;
        internal BTree<string, string> replace =>
            (BTree<string,string>)mem[_Replace]??BTree<string, string>.Empty; // for RestViews
        internal int display => (int)(mem[Display]??0);
        internal Query enc => (Query)mem[Enc]; // not used after preAnalyse except for RestViews
        internal BTree<SqlValue, SqlValue> import =>
            (BTree<SqlValue,SqlValue>)mem[_Import]??BTree<SqlValue, SqlValue>.Empty; // cache Imported SqlValues
        /// <summary>
        /// ordering to use, initialised during Selects(), constructed during Orders() 
        /// </summary>
        internal OrderSpec ordSpec => (OrderSpec)mem[OrdSpec];
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        /// <summary>
        /// where clause, may be updated during Conditions() analysis.
        /// This is a disjunction of filters.
        /// the value for a filter, initially null, is updated to the implementing rowSet 
        /// </summary>
        internal BTree<long,SqlValue> where => 
            (BTree<long,SqlValue>)mem[Where]?? BTree<long,SqlValue>.Empty;
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
        internal RowSet data => (RowSet)mem[Data];
        /// <summary>
        /// results: constructed during RowSets() analysis
        /// </summary>
        internal Domain rowType => (Domain)mem[SqlValue.NominalType];
        /// <summary>
        /// Constructor: a query object
        /// </summary>
        /// <param name="cx">the query context</param>
        /// <param name="k">the expected data type</param>
        internal Query(long u,Domain dt,BTree<long,object>m=null)
            : base(u,(m??BTree<long,object>.Empty)+(SqlValue.NominalType, dt))
        { }
        internal Query(long u, Domain dt, string n,BTree<long, object> m = null)
            : base(u, (m ?? BTree<long, object>.Empty) + (SqlValue.NominalType, dt)+(Name,n))
        { }
        internal Query(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Query operator+(Query q,(long,object)x)
        {
            return (Query)q.New(q.mem + x);
        }
        public static Query operator+(Query q,(SqlValue,string) x)
        {
            return q.Add(x.Item1,x.Item2);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Query(defpos,m);
        }
        internal virtual SqlValue ValFor(SqlValue sv)
        {
            var iq = scols?[sv];
            if (iq == null)
                return null;
            return cols[iq.Value];
        }
        internal virtual void Resolve(Context cx)
        { }
        internal virtual Query Selects(Context cx)
        {
            return this;
        }
        internal override DBObject Replace(Context cx,DBObject was,DBObject now)
        {
            if (cx.done.Contains(defpos)) // includes the case was==this
                return cx.done[defpos];
            var cs = cols;
            var r = this;
            for (var b = cs.First(); b != null; b = b.Next())
            {
                var x = b.value();
                var bv = x.Replace(cx, was, now).WithA(x.alias);
                if (bv!=x)
                    cs += (b.key(), bv);
            }
            if (cs!=cols)
            {
                r += (Cols, cs);
                r += (SqlValue.NominalType, new Domain(cs));
            }
            if (cols==BList<SqlValue>.Empty)
            {
                var ts = rowType.columns;
                for (var b = ts.First(); b != null; b = b.Next())
                {
                    var x = b.value();
                    var bv = (Selector)x.Replace(cx, was, now).WithA(x.alias);
                    if (bv!=x)
                        ts += (b.key(), bv);
                }
                if (ts != rowType.columns)
                    r += (SqlValue.NominalType, new Domain(ts));
            }
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue) b.value().Replace(cx, was, now);
                if (v != b.value())
                    w += (b.key(), v);
            }
            if (w!=r.where)
                r = r.AddCondition(Where,w);
            var ss = r.scols;
            for (var b = r.scols.First();b!=null;b=b.Next())
            {
                var x = b.key();
                var bk = x.Replace(cx, was, now).WithA(x.alias);
                if (bk != b.key())
                    ss += (bk, b.value());
            }
            if (ss!=r.scols)
                r += (SCols, ss);
            var ms = r.matches;
            for (var b=ms.First();b!=null;b=b.Next())
            {
                var bk = (SqlValue)b.key().Replace(cx, was, now);
                if (bk != b.key())
                    ms += (bk, b.value());
            }
            if (ms!=r.matches)
                r += (Matches, ms);
            var mg = r.matching;
            for (var b = mg.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)b.key().Replace(cx,was,now);
                var bv = b.value();
                for (var c = bv.First();c!=null;c=c.Next())
                {
                    var ck = (SqlValue)c.key().Replace(cx,was,now);
                    if (ck != c.key())
                        bv += (ck, c.value());
                }
                if (bk!=b.key() || bv!=b.value())
                    mg += (bk, bv);
            }
            if (mg!=r.matching)
                r += (Matching, mg);
            var os = r.ordSpec;
            for (var b = os?.items.First(); b != null; b = b.Next())
            {
                var oi = b.value();
                var ow = (SqlValue)oi.Replace(cx,was,now);
                if (oi!=ow)
                    os += (b.key(), ow);
            }
            if (os!=r.ordSpec)
                r += (OrdSpec, os);
            var im = r.import;
            for (var b=im.First();b!=null;b=b.Next())
            {
                var ik = (SqlValue)b.key().Replace(cx,was,now);
                var iv = (SqlValue)b.value().Replace(cx,was,now);
                if (ik != b.key() || iv != b.value())
                    im += (ik, iv);
            }
            if (im != r.import)
                r += (_Import, im);
            var ag = r.assig;
            for (var b = ag.First(); b != null; b = b.Next())
            {
                var aa = (SqlValue)b.key().val.Replace(cx,was,now);
                var ab = (SqlValue)b.key().vbl.Replace(cx,was,now);
                if (aa != b.key().val || ab != b.key().vbl)
                    ag += (new UpdateAssignment(ab,aa), b.value());
            }
            if (ag != r.assig)
                r += (Assig, ag);
            cx.done += (defpos, r);
            return (r==this)?this:(Query)cx.Add(r);
        }
        internal virtual Query Refresh(Context cx)
        {
            return (Query)cx.obs[defpos];
        }
        internal virtual CursorSpecification CursorSpec()
        {
            return enc?.CursorSpec();
        }
        internal virtual Query AddRestViews(CursorSpecification q)
        {
            return this;
        }
        internal virtual bool HasColumn(SqlValue sv)
        {
            return false;
        }
        /// <summary>
        /// Add a new column to the query, and update the row type
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal virtual Query Add(SqlValue v,string n)
        {
            if (v == null)
                return this;
            var m = (int)cols.Count;
            var deps = dependents + (v.defpos,true);
            var dpt = _Max(depth, 1 + v.depth);
            return this + (Cols, cols + (m, v)) + (SCols,scols+(v,m))
                + (SqlValue.NominalType, rowType.columns 
                + (m,new Selector(n, v.defpos, v.nominalDataType, m)))
                +(Dependents,deps)+(Depth,dpt);
        }

        /// <summary>
        /// Item1 of the return value of this method is always this.
        /// AddMatches modifies the parameter q so we get this from Item2.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        internal virtual Query AddMatches(Context cx, Query q)
        {
            return this;
        }
        internal void AddMatch(Context cx, SqlValue sv, TypedValue tv)
        {
            cx.row.values += (sv.defpos, tv);
        }
        internal void RemoveMatch(Context cx, SqlValue sv)
        {
            cx.row.values -= sv.defpos;
        }
        internal virtual Query Conditions(Transaction tr,Context cx,Query q)
        {
            var svs = where;
            var r = this;
            for (var b = svs.First(); b != null; b = b.Next())
            {
                r = b.value().Conditions(tr,cx, r, true, out bool mv);
                if (mv)
                    svs -=b.key();
            }
            return r.AddCondition(cx,svs);
        }
        /// <summary>
        /// </summary>
        /// <param name="svs">A list of where conditions</param>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        internal Query MoveConditions(Context cx, Query q)
        {
            var wh = where;
            var fi = filter;
            var qw = q.where;
            var qf = q.filter;
            var s = cx.depths.ToString();
            for (var b = where.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                if (v is SqlValue w) 
                {
                    if (!q.where.Contains(k))
                        qw+=(k, w);
                    wh -= k;
                }
            }
            for (var b = fi.First(); b != null; b = b.Next())
                qf += (b.key(),b.value());
            var oq = q;
            if (qw!=q.where)
                q=q.AddCondition(Where, qw);
            if (qf != q.filter)
                q += (Filter, qf);
            if (q != oq)
                cx.Add(q);
            var r = this;
            if (wh != where)
                r=r.AddCondition(Where, wh);
            if (fi != filter)
                r += (Filter, fi);
            if (r != this)
                r = (Query)cx.Add(r);
            return r;
        }
        internal virtual Query Orders(Transaction tr,Context cx, OrderSpec ord)
        {
            var ndt = rowType;
            for (var i = 0; i < ord.items.Count; i++)
            {
                var w = ord.items[i];
                if (ndt.names[w.name] == null)
                    return this;
            }
            return this+(OrdSpec,ord);
        }
        public static bool Eval(BTree<long, SqlValue> svs, Transaction tr, Context cx)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value().Eval(tr, cx) != TBool.True)
                    return false;
            return true;
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
        internal virtual Query AddCondition(Transaction tr,Context cx, BTree<long, SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return this;
        }
        internal Query AddCondition(Context cx, SqlValue cond)
        {
            cx.Replace(this,AddCondition(Where, cond));
            return (Query)cx.done[defpos];
        }
        internal Query AddCondition(Context cx, BTree<long,SqlValue> conds)
        {
            cx.Replace(this, AddCondition(Where, conds));
            return (Query)cx.done[defpos];
        }
        internal Query AddCondition(long prop,BTree<long,SqlValue> conds)
        {
            var q = this;
            for (var b = conds.First(); b != null; b = b.Next())
                q = q.AddCondition(prop, b.value());
            return q;
        }
        internal Query AddCondition(long prop, SqlValue cond)
        {
            if (where.Contains(cond.defpos))
                return this;
            var filt = filter;
            var q = this;
            if (cond is SqlValueExpr se && se.kind == Sqlx.EQL)
            {
                if (se.left is TableColumn cl && se.right is SqlLiteral ll)
                    filt += (cl.defpos, ll.val);
                else if (se.right is TableColumn cr && se.left is SqlLiteral lr)
                    filt += (cr.defpos, lr.val);
            }
            if (filt != filter)
                q += (Filter, filt);
            if (prop == Where)
                q += (Where, q.where + (cond.defpos, cond));
            else if (q is TableExpression te)
                q = te + (TableExpression.Having, te.having + (cond.defpos, cond));
            return q;
        }
        /// <summary>
        /// We are a view or subquery source. 
        /// The supplied where-condition is to be transformed to use our SqlTypeColumns.
        /// </summary>
        /// <param name="tr">The connection</param>
        /// <param name="cond">A where condition</param>
        internal Query ImportCondition(Context cx, SqlValue cond)
        {
            return AddCondition(cx, cond.Import(this));
        }
        /// <summary>
        /// Distribute a set of update assignments to table expressions
        /// </summary>
        /// <param name="assigns">the list of assignments</param>
        internal virtual Query DistributeAssigns(BTree<UpdateAssignment, bool> assigns)
        {
            return this;
        }
        internal virtual bool Knows(Selector c)
        {
            return false;
        }
        internal virtual void Build(Context _cx, RowSet rs)
        {
        }
        internal virtual void StartCounter(Context _cx, RowSet rs)
        {
        }
        internal virtual void AddIn(Context _cx, RowBookmark rb)
        {
        }
        internal virtual void SetReg(Context _cx,TRow k)
        {
            for (var b = where.First(); b != null; b = b.Next())
                b.value().SetReg(_cx,k);
        }
        public static void Eqs(Transaction tr,Context cx, BTree<long, SqlValue> svs, ref Adapters eqs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                b.value().Eqs(tr,cx, ref eqs);
        }
        /// <summary>
        /// Ensure that the distinct request is propagated to the query
        /// </summary>
        internal virtual Query SetDistinct()
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
        internal virtual Transaction Insert(Transaction tr, Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return tr;
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Transaction Update(Transaction tr, Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return tr;
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Transaction Delete(Transaction tr, Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return tr;
        }
        /// <summary>
        /// The number of columns in the query (not the same as the result length)
        /// </summary>
        internal int Size { get { return (int)cols.Count; } }
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
        Query _AddMatchedPair(SqlValue a, SqlValue b)
        {
            if (!matching[a].Contains(b))
            {
                var c = matching[a] ?? BTree<SqlValue, bool>.Empty;
                c +=(b, true);
                return (Query)New(mem + (Matching, matching + (a, c)));
            }
            return this;
        }
        /// <summary>
        /// Ensure Match relation is transitive after adding a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        internal Query AddMatchedPair(SqlValue a, SqlValue b)
        {
            var cx = _AddMatchedPair(a, b);
            if (cx == this)
                return this;
            for (; ; )
            {
                var cur = cx;
                for (var c = matching.First(); c != null; c = c.Next())
                    for (var d = matching[c.key()].First(); d != null; d = d.Next())
                        cx = _AddMatchedPair(c.key(), d.key())._AddMatchedPair(d.key(), c.key());
                if (cx == cur)
                    return cx;
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
            return matching[a]?[b] == true;
        }

        /// <summary>
        /// Get the SqlValue at the given column position
        /// </summary>
        /// <param name="i">the position</param>
        /// <returns>the value expression</returns>
        internal SqlValue ValAt(int i)
        {
            if (i >= 0 && i < cols.Count)
                return cols[i];
            throw new PEException("PE335");
        }
        internal virtual bool aggregates()
        {
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().aggregates())
                    return true;
            return false;
        }
        internal Query AddCols(Query from)
        {
            var r = this;
            var sl = BList<SqlValue>.Empty;
            var sc = BTree<SqlValue, int?>.Empty;
            for (var b = from.rowType.columns.First(); b != null; b = b.Next())
            {
                var c = b.value();
                sl += c;
                sc += (c, c.seq);
            }
            r += (Cols, sl);
            r += (SCols, sc);
            r += (Display, (int)sl.Count);
            r += (SqlValue.NominalType, from.rowType);
            return r;
        }
        [Flags]
        internal enum Need { noNeed = 0, selected = 1, joined = 2, condition = 4, grouped = 8 }
        internal virtual SelectQuery SelQuery()
        {
            return enc?.SelQuery();
        }
        internal virtual QuerySpecification QuerySpec()
        {
            return enc?.QuerySpec();
        }
        internal RowSet Ordering(Context _cx, RowSet r,bool distinct)
        {
            if (ordSpec != null && ordSpec.items.Count > 0 && !ordSpec.SameAs(this,r.rowOrder))
                return new OrderedRowSet(_cx,this, r, ordSpec, distinct);
            if (distinct)
                return new DistinctRowSet(_cx,r);
            return r;
        }
        internal virtual RowSet RowSets(Transaction tr,Context cx)
        {
            return null;
        }
        internal void CondString(StringBuilder sb, BTree<long, SqlValue> cond, string cm)
        {
            for (var b = cond?.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value());
            }
        }
        public string WhereString(BTree<long, SqlValue> svs, BTree<SqlValue, TypedValue> mts, 
            TRow pre)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = svs?.First(); b != null; b = b.Next())
            {
                var sw = b.value().ToString();
                if (sw.Length > 1)
                {
                    sb.Append(cm); cm = " and ";
                    sb.Append(sw);
                }
            }
            for (var b = mts?.First(); b != null; b = b.Next())
            {
                var nm = b.key().name;
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
            if (mem.Contains(Assig)) { sb.Append(" Assigs:"); sb.Append(assig); }
            if (mem.Contains(Cols)) { sb.Append(" Cols:"); sb.Append(cols); }
            if (mem.Contains(Display)) { sb.Append(" Display="); sb.Append(display); }
    //        if (mem.Contains(Enc) && enc!=null) { sb.Append(" Enc="); sb.Append(enc.blockid); }
            if (mem.Contains(FetchFirst)) { sb.Append(" FetchFirst="); sb.Append(fetchFirst); }
            if (mem.Contains(Filter)) { sb.Append(" Filter:"); sb.Append(filter); }
            if (mem.Contains(_Import)) { sb.Append(" Import:"); sb.Append(import); }
            if (mem.Contains(Matches)) { sb.Append(" Matches:"); sb.Append(matches); }
    //        if (mem.Contains(_Needs)) { sb.Append(" Needs:"); sb.Append(needs); }
            if (mem.Contains(OrdSpec)) { sb.Append(" OrdSpec:"); sb.Append(ordSpec); }
            if (mem.Contains(_Replace)) { sb.Append(" Replace:"); sb.Append(replace); }
            if (mem.Contains(SqlValue.NominalType)) { sb.Append(" RowType:");
                var cm = "(";
                for (var b=rowType?.columns.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(b.value().name);
                }
                sb.Append(")");
            }
            if (mem.Contains(SCols)) { sb.Append(" SCols:"); sb.Append(scols); }
     //       if (mem.Contains(SimpleQuery)) { sb.Append(" SimpleQuery="); sb.Append(simpleQuery.blockid); }
            if (mem.Contains(Where)) { sb.Append(" Where:"); sb.Append(where); }
            return sb.ToString();
        }
    }

    /// <summary>
    /// Most query classes require name resolution.
    /// </summary>
    internal class SelectQuery : Query
    {
        internal const long
            Unknown = -201, // MethodCall
            _Aggregates = -202, // bool
            Defs1 = -203; // BTree<long,SqlValue>
        internal SqlMethodCall unknown => (SqlMethodCall)mem[Unknown];
        /// <summary>
        /// whether the list of cols contains aggregates
        /// </summary>
        internal bool _aggregates => (bool)(mem[_Aggregates]??false);
        public ATree<long,SqlValue> defs1 => (ATree<long,SqlValue>)mem[Defs1]??BTree<long,SqlValue>.Empty; // for Views
        internal SelectQuery(long u,BTree<long,object>m) 
            : base(u, m) { }
        internal SelectQuery(long u, Domain dt, BTree<long, object> m = null)
    : base(u, (m ?? BTree<long, object>.Empty) + (SqlValue.NominalType, dt))
        { }
        internal SelectQuery(long u, Domain dt, string n, BTree<long, object> m = null)
    : base(u, (m ?? BTree<long, object>.Empty) + (SqlValue.NominalType, dt) + (Name, n))
        { }
        protected SelectQuery(SelectQuery q, SqlValue s) : base(q.defpos, q.mem + (Cols, q.cols + s)
            + (SqlValue.NominalType, q.rowType + 
            new Selector(s.alias??s.name, s.defpos, s.nominalDataType, q.rowType.Length))
            + (Display,q.display+1)+(Dependents,q.dependents+(s.defpos,true))
            +(Depth,_Max(q.depth,1+s.depth)))
        { }
        public static SelectQuery operator +(SelectQuery q, (long, object) x)
        {
            return (SelectQuery)q.New(q.mem + x);
        }
        public static SelectQuery operator+(SelectQuery q,SqlValue v)
        {
            return new SelectQuery(q, v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectQuery(defpos, m);
        }
        internal override SelectQuery SelQuery()
        {
            return this;
        }
        internal override DBObject Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SelectQuery)base.Replace(cx,was,now);
            var ds = r.defs1;
            for (var b = defs1.First(); b != null; b = b.Next())
            {
                var x = b.value();
                var bv = x.Replace(cx,was,now).WithA(x.alias);
                if (bv!=b.value())
                    ds += (b.key(), bv);
            }
            if (ds != r.defs1)
                r += (Defs1, ds);
            if (r != this)
                cx.Add(r);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool aggregates()
        {
            return _aggregates || base.aggregates();
        }
        internal override void Build(Context _cx, RowSet rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                    b.value().Build(_cx,rs);
            base.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                    b.value().StartCounter(_cx,rs);
            base.StartCounter(_cx,rs);
        }
        internal override void AddIn(Context _cx, RowBookmark rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                    b.value().AddIn(_cx,rs);
            base.AddIn(_cx,rs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Unknown)) { sb.Append(" Unknown:"); sb.Append(unknown); }
            if (mem.Contains(_Aggregates)) { sb.Append(" Aggregates="); sb.Append(_aggregates); }
            if (mem.Contains(Defs1)) { sb.Append(" defs1:"); sb.Append(defs1); }
            return sb.ToString();
        }
    }
    // An ad-hoc SystemTable for a row history: the work is mostly done by
    // LogTableSelectBookmark
    internal class LogRowTable :SelectQuery
    {
        public readonly Table st,tb;
        public LogRowTable(Transaction tr, long td, string ta) 
            :base(tr.uid,BTree<long,object>.Empty)
        {
            tb = tr.role.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix();
            var tt = new SystemTable("" + td);
            new SystemTableColumn(tt, "Pos", 0, Domain.Int);
            new SystemTableColumn(tt, "Action", 1, Domain.Char);
            new SystemTableColumn(tt, "DefPos", 2, Domain.Int);
            new SystemTableColumn(tt, "Transaction", 3, Domain.Int);
            new SystemTableColumn(tt, "Timestamp", 4, Domain.Timestamp);
            st = tt;
        }
    }
    /// <summary>
    /// An Ad-hoc SystemTable for a row,column history: the work is mostly done by
    /// LogRowColSelectBookmark
    /// </summary>
    internal class LogRowColTable : SelectQuery
    {
        public readonly Table st,tb;
        public readonly long rd, cd;
        public LogRowColTable(Transaction tr, long r, long c, string ta)
        : base(tr.uid, BTree<long, object>.Empty)
        {
            var tc = tr.role.objects[cd] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            rd = r;
            cd = c;
            tb = tr.role.objects[tc.tabledefpos] as Table;
            var tt = new SystemTable("" + rd + ":" + cd);
            new SystemTableColumn(tt, "Pos", 0, Domain.Int);
            new SystemTableColumn(tt, "Value", 1, Domain.Char);
            new SystemTableColumn(tt, "StartTransaction", 2, Domain.Int);
            new SystemTableColumn(tt, "StartTimestamp", 3, Domain.Timestamp);
            new SystemTableColumn(tt, "EndTransaction", 4, Domain.Int);
            new SystemTableColumn(tt, "EndTimestamp", 5, Domain.Timestamp);
            st = tt;
        }
    }
    /// <summary>
    /// Implement a CursorSpecification 
    /// </summary>
    internal class CursorSpecification : Query
    {
        internal const long
            _Source = -204, // string
            Union = -205, // QueryExpression
            UsingFrom = -206, // Query
            RVQSpecs = -207, // BList<QuerySpecification>
            RestGroups = -208, // BTree<string,int>
            RestViews = -209; // BTree<long,RestView>
        /// <summary>
        /// The source string
        /// </summary>
        public string _source=> (string)mem[_Source];
        /// <summary>
        /// The QueryExpression part of the CursorSpecification
        /// </summary>
        public QueryExpression union => (QueryExpression)mem[Union];
        /// <summary>
        /// For RESTView implementation
        /// </summary>
        public From usingFrom => (From)mem[UsingFrom];
        /// <summary>
        /// Going up: For a RESTView source, the enclosing QuerySpecifications
        /// </summary>
        internal BList<QuerySpecification> rVqSpecs =>
            (BList<QuerySpecification>)mem[RVQSpecs] ?? BList<QuerySpecification>.Empty;
        /// <summary>
        /// looking down: RestViews contained in this Query and its subqueries
        /// </summary>
        internal BTree<long, RestView> restViews =>
            (BTree<long, RestView>)mem[RestViews]?? BTree<long, RestView>.Empty;
        internal BTree<string, int> restGroups =>
            (BTree<string, int>)mem[RestGroups] ?? BTree<string, int>.Empty;
        /// <summary>
        /// Constructor: a CursorSpecification from the Parser
        /// </summary>
        /// <param name="t">The transaction</param>
        /// <param name="dt">the expected data type</param>
        internal CursorSpecification(long u,BTree<long,object>m)
            : base(u, m+(Dependents,new BTree<long,bool>(((Query)m[Union])?.defpos??-1,true))
                  +(Depth,1+((Query)m[Union])?.depth??0)) { }
        public static CursorSpecification operator +(CursorSpecification q, (long, object) x)
        {
            return (CursorSpecification)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new CursorSpecification(defpos, m);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CursorSpecification)base.Replace(cx, so, sv);
            var un = union?.Replace(cx, so, sv);
            if (un != union)
                r += (Union, un);
            cx.done += (defpos, r);
            return r;
        }
        internal override CursorSpecification CursorSpec()
        {
            return this;
        }
        /// <summary>
        /// Analysis stage Conditions: do Conditions on the union.
        /// </summary>
        internal override Query Conditions(Transaction tr,Context cx,Query q)
        {
            var u = union;
            var r = MoveConditions(cx, u);
            u = (QueryExpression)((Query)cx.obs[u.defpos]).Conditions(tr,cx, r);
            return new CursorSpecification(defpos, r.mem + (Union, cx.obs[u.defpos]));
        }
        internal override Query Orders(Transaction tr,Context cx, OrderSpec ord)
        {
            return base.Orders(tr,cx,ord)+(Union, union.Orders(tr,cx, ord));
        }
        internal override Query AddRestViews(CursorSpecification q)
        {
            return (Query)New(mem + (Union, union.AddRestViews(q)));
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
        internal override Query AddCondition(Transaction tr,Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var cs = new CursorSpecification(defpos,base.AddCondition(tr,cx,cond, assigns, data).mem);
            return cs+(Union,cs.union.AddCondition(tr,cx,cond, assigns, data));
        }
        internal override bool Knows(Selector c)
        {
            return base.Knows(c) || (union?.Knows(c) ?? usingFrom?.Knows(c) ?? false);
        }
        internal override bool aggregates()
        {
            return union.aggregates() || base.aggregates();
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            var r = union.RowSets(tr, cx);
            r = Ordering(cx,r,false);
            return r;
        }
        internal override void Build(Context _cx, RowSet rs)
        {
            union.Build(_cx,rs);
            base.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            union.StartCounter(_cx,rs);
            base.StartCounter(_cx,rs);
        }
        internal override void AddIn(Context _cx, RowBookmark rs)
        {
            union.AddIn(_cx,rs);
            base.AddIn(_cx,rs);
        }
        /// <summary>
        /// Ensure that the distinct request is propagated to the query
        /// </summary>
        internal override Query SetDistinct()
        {
            return this+(Union,union.SetDistinct());
        }
        /// <summary>
        /// propagate the Insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs"the rowsets affected></param>
        internal override Transaction Insert(Transaction tr,Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, 
            Level cl)
        {
            return union.Insert(tr,_cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Update(Transaction tr,Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return union.Update(tr,cx,ur, eqs,rs);
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr,Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return union.Delete(tr,cx,dr,eqs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_Source))
            { sb.Append(" Source={"); sb.Append(_source); sb.Append('}'); }
            sb.Append(" Union: "); sb.Append(Uid(union.defpos)); 
            if (mem.Contains(UsingFrom))
            { sb.Append(" Using: "); sb.Append(Uid(usingFrom.defpos)); }
            if (mem.Contains(RVQSpecs)){ sb.Append(" RVQSpecs:"); sb.Append(rVqSpecs); }
            if (mem.Contains(RestViews))
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
            From = -210, // Query
            Group = -211, // GroupSpecification
            Having = -212, // BTree<long,SqlValue>
            Windows = -213; // BTree<long,WindowSpecification>
        /// <summary>
        /// The from clause of the tableexpression
        /// </summary>
        internal Query from => (Query)mem[From];
        /// <summary>
        /// The group specification
        /// </summary>
        internal GroupSpecification group => (GroupSpecification)mem[Group];
        /// <summary>
        /// The having clause
        /// </summary>
        internal BTree<long,SqlValue> having =>
            (BTree<long,SqlValue>)mem[Having]??BTree<long,SqlValue>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal BTree<long,WindowSpecification> window =>
            (BTree<long,WindowSpecification>)mem[Windows];
        /// <summary>
        /// Constructor: a tableexpression from the parser
        /// </summary>
        /// <param name="t">the transaction</param>
        internal TableExpression(long u, BTree<long,object> m) 
            : base(u,m+(Dependents,new BTree<long,bool>(((Query)m[From])?.defpos??-1,true))
                  +(Depth,((Query)m[From])?.depth??0))
        { }
        public static TableExpression operator +(TableExpression q, (long, object) x)
        {
            return (TableExpression)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableExpression(defpos,m);
        }
        internal override Query Refresh(Context cx)
        {
            var q = (TableExpression)base.Refresh(cx);
            var fm = (Query)cx.obs[from.defpos];
            if (fm != q.from)
                q = (TableExpression)cx.Add(q+(From, fm));
            return q;
        }
        internal override bool aggregates()
        {
            return from.aggregates() || base.aggregates();
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            var r = from.RowSets(tr,cx);
            if (where.Count > 0)
            {
                var gp = false;
                if (group != null)
                    for (var gs = group.sets.First();gs!=null;gs=gs.Next())
                        gs.value().Grouped(where, ref gp);
            }
            return r;
        }
        internal override void Build(Context _cx, RowSet rs)
        {
            from.Build(_cx,rs);
            base.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            from.StartCounter(_cx,rs);
            base.StartCounter(_cx,rs);
        }
        internal override void AddIn(Context _cx, RowBookmark rs)
        {
            from.AddIn(_cx,rs);
            base.AddIn(_cx,rs);
        }
        internal override bool Knows(Selector c)
        {
            return from.Knows(c);
        }
        /// <summary>
        /// propagate an Insert operation
        /// </summary>
        /// <param name="prov">provenance</param>
        /// <param name="data">insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Insert(Transaction tr,Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return from.Insert(tr,_cx,prov, data, eqs, rs,cl);
        }
        /// <summary>
        /// propagate Delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr,Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return from.Delete(tr,cx,dr,eqs);
        }
        /// <summary>
        /// propagate Update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Update(Transaction tr,Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return from.Update(tr,cx,ur,eqs,rs);
        }
        internal override Query AddMatches(Context cx, Query q)
        {
            var m = mem;
            for (var b = having.First(); b != null; b = b.Next())
                q = b.value().AddMatches(q);
            q = from.AddMatches(cx, q);
            cx.Replace(this,q);
            return (Query)cx.done[defpos];
        }
        internal override Query Conditions(Transaction tr,Context cx, Query q)
        {
            var r = (TableExpression)MoveConditions(cx, from);
            var fm = (Query)cx.obs[from.defpos];
            r = r.MoveHavings(tr, cx, fm);
            fm = (Query)cx.obs[fm.defpos];
            r = (TableExpression)r.AddPairs(fm);
/*            var h = r.having;
            var w = r.where;
            if (r.group != null && r.group.sets.Count > 0)
            {
                for (var b = w.First(); b != null; b = b.Next())
                {
                    if (!h.Contains(b.key()))
                        h+=(b.key(), b.value());
                    w -=b.key();
                }
                r = (TableExpression)cx.Add(r + (Where, w));
                // but avoid having-conditions that depend on aggregations
                var qs = (QuerySpecification)cx.obs[QuerySpec().defpos];
                w = qs.where;
                for (var b=h.First();b!=null;b=b.Next())
                    if (b.value().Import(this)==null)
                    {
                        if (!qs.where.Contains(b.key()))
                            w +=(b.key(), b.value());
                        h -=b.key();
                    }
                cx.Add(r + (Having, h));
                cx.Add(qs + (Where, w));
            } */
            return Refresh(cx);
        }
        internal TableExpression MoveHavings(Transaction tr,Context cx, Query q)
        {
            var ha = having;
            var qw = q.where;
            for (var b = ha.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value().Import(q);
                if (v is SqlValue w) // allow substitution with matching expression
                {
                    if (!q.where.Contains(k))
                        qw += (k, w);
                    ha -= k;
                }
            }
            if (ha != having)
            {
                cx.Replace(q, q.AddCondition(Where, qw));
                cx.Replace(this, this + (Having, ha));
                return (TableExpression)cx.done[defpos];
            }
            return this;
        }
        internal override Query Orders(Transaction tr,Context cx,OrderSpec ord)
        {
            return ((TableExpression)base.Orders(tr,cx,ord))+(From,from.Orders(tr,cx,ord));
        }
        internal override Query AddRestViews(CursorSpecification q)
        {
            return this+(From,from.AddRestViews(q));
        }
        /// <summary>
        /// Add cond and/or update data to this query
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        internal override Query AddCondition(Transaction tr,Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return (TableExpression)(base.AddCondition(tr,cx,cond, assigns, data)+
            (From,from.AddCondition(tr,cx,cond, assigns, data)));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" From: "); sb.Append(Uid(from.defpos));
            if (mem.Contains(Group)) sb.Append(group);
            if (mem.Contains(Having)) { sb.Append(" Having:"); sb.Append(having); }
            if (mem.Contains(Windows)) { sb.Append(" Window:"); sb.Append(window); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Join is implemented as a subclass of Query
    /// </summary>
    internal class JoinPart : SelectQuery
    {
        internal const long
            _FDInfo = -214, // FDJoinPart
            JoinCond = -215, // BTree<long,SqlValue>
            JoinKind = -216, // Sqlx
            LeftInfo = -217, // OrderSpec
            LeftOperand = -218, // Query
            Natural = -219, // Sqlx
            NamedCols = -220, // BList<long>
            RightInfo = -221, // OrderSpec
            RightOperand = -222; //Query
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
        internal Sqlx kind => (Sqlx)(mem[JoinKind]??Sqlx.CROSS);
        /// <summary>
        /// The join condition is implemented by ordering, using any available indexes.
        /// Rows in the join will use left/rightInfo.Keys() for ordering and theta-operation.
        /// </summary>
        internal OrderSpec leftInfo => (OrderSpec)mem[LeftInfo]; // initialised once nominalDataType is known
        internal OrderSpec rightInfo => (OrderSpec)mem[RightInfo];
        /// <summary>
        /// During analysis, we collect requirements for the join conditions.
        /// </summary>
        internal BTree<long, SqlValue> joinCond => 
            (BTree<long,SqlValue>)mem[JoinCond]??BTree<long, SqlValue>.Empty;
        /// <summary>
        /// The left element of the join
        /// </summary>
        public Query left => (Query)mem[LeftOperand];
        /// <summary>
        /// The right element of the join
        /// </summary>
        public Query right => (Query)mem[RightOperand];
        /// <summary>
        /// A FD-join depends on a functional relationship between left and right
        /// </summary>
        internal FDJoinPart FDInfo => (FDJoinPart)mem[_FDInfo];
        /// <summary>
        /// Constructor: a join part being built by the parser
        /// </summary>
        /// <param name="t"></param>
        internal JoinPart(long u, BTree<long,object> m) 
            : base(u,m) { }
        public static JoinPart operator+ (JoinPart j,(long,object)x)
        {
            return new JoinPart(j.defpos, j.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinPart(defpos,m);
        }
        internal override bool Knows(Selector c)
        {
            return left.Knows(c) || right.Knows(c);
        }
        internal override bool aggregates()
        {
            return left.aggregates()||right.aggregates()||base.aggregates();
        }
        internal override Query AddMatches(Context cx, Query q)
        {
            var lf = left.AddMatches(cx,q);
            var rg = right.AddMatches(cx, q);
            var r = base.AddMatches(cx, q);
            for (var b = joinCond.First(); b != null; b = b.Next())
                r = b.value().AddMatches(q);
            return new JoinPart(r.defpos,r.mem
                +(LeftOperand,lf)+(RightOperand,rg));
        }
        internal override Query Refresh(Context cx)
        {
            var q = (JoinPart)base.Refresh(cx);
            var lf = cx.obs[left.defpos];
            if (lf != left)
                q = (JoinPart)cx.Add(q+(LeftOperand, lf));
            var rg = cx.obs[right.defpos];
            if (rg != right)
                q = (JoinPart)cx.Add(q+(RightOperand, rg));
            return q;
        }
        /// <summary>
        /// Analysis stage Selects: call for left and right.
        /// </summary>
        internal override Query Selects(Context cx)
        {
            var r = this;
            int n = left.display;
            var vs = BList<SqlValue>.Empty;
            var ss = BList<Selector>.Empty;
            var lo = BList<SqlValue>.Empty; // left ordering
            var ro = BList<SqlValue>.Empty; // right
            var ds = 0;
            var de = depth;
            var sb = dependents;
            if (naturaljoin != Sqlx.NO)
            {
                int m = 0; // common.Count
                int rn = right.display;
                // which columns are common?
                bool[] lc = new bool[n];
                bool[] rc = new bool[rn];
                for (int i = 0; i < rn; i++)
                    rc[i] = false;
                for (int i = 0; i < n; i++)
                {
                    var ll = left.cols[i];
                    for (int j = 0; j < rn; j++)
                    {
                        var rr = right.cols[j];
                        if (ll.name.CompareTo(rr.name) == 0)
                        {
                            lc[i] = true;
                            rc[j] = true;
                            var cp = new SqlValueExpr(rr.defpos-1, Sqlx.EQL, ll, rr, Sqlx.NULL);
        //                    cp.left.Needed(tr, Need.joined);
        //                    cp.right.Needed(tr, Need.joined);
                            r += (JoinCond,cp.Disjoin());
                            lo += ll;
                            ro += rr;
                            m++;
                            break;
                        }
                    }
                    if (!lc[i])
                    {
                        vs += ll;
                        ss += new Selector(ll,ds++);
                        de = _Max(de, 1 + ll.depth);
                        sb += (ll.defpos, true);
                    }
                }
                for (int i = 0; i < n; i++)
                    if (lc[i])
                    {
                        var ll = left.cols[i];
                        vs += ll;
                        ss += new Selector(ll, ds++);
                        de = _Max(de, 1 + ll.depth);
                        sb += (ll.defpos, true);
                    }
                for (int i = 0; i < rn; i++)
                    if (!rc[i])
                    {
                        var rr = right.cols[i];
                        vs += rr;
                        ss += new Selector(rr, ds++);
                        de = _Max(de, 1 + rr.depth);
                        sb += (rr.defpos, true);
                    }
                if (m == 0)
                    r += (JoinKind, Sqlx.CROSS);
                else
                {
                    r += (LeftOperand, left + (OrdSpec, new OrderSpec(lo)));
                    r += (RightOperand, right + (OrdSpec, new OrderSpec(ro)));
                }
            }
            else
            {
                for (int j = 0; j < left.display; j++)
                {
                    var cl = left.cols[j];
                    for (var i = 0; i < right.cols.Count; i++)
                        if (right.rowType.names.Contains(cl.name))
                            cl = new SqlValueExpr(cl.defpos,Sqlx.DOT,
                                new SqlValue(left.defpos,left.name),cl,Sqlx.NO,
                                new BTree<long,object>(Name,left.name+"."+cl.name));
                    vs += cl;
                    ss += new Selector(cl, ds++);
                    de = _Max(de, 1 + cl.depth);
                    sb += (cl.defpos, true);
                }
                for (int j = 0; j < right.display; j++)
                {
                    var cr = right.cols[j];
                    for (var i = 0; i < left.cols.Count; i++)
                        if (left.rowType.names.Contains(cr.name))
                            cr = new SqlValueExpr(cr.defpos, Sqlx.DOT,
                                new SqlValue(right.defpos, right.name), cr, Sqlx.NO,
                                new BTree<long, object>(Name, right.name + "." + cr.name));
                vs += cr;
                    ss += new Selector(cr, ds++);
                    de = _Max(de, 1 + cr.depth);
                    sb += (cr.defpos, true);
                }
            }
            /*      // first ensure each joinCondition has the form leftExpr compare rightExpr
                  // if not, move it to where
                  for (var b = joinCond.First(); b != null; b = b.Next())
                  {
                      if (b.value() is SqlValueExpr se)
                      {
                          if (se.left.isConstant || se.right.isConstant)
                              continue;
                          if (se.left.IsFrom(tr, left, true) && se.right.IsFrom(tr, right, true))
                          {
                              se.left.Needed(tr, Need.joined);
                              se.right.Needed(tr, Need.joined);
                              continue;
                          }
                          if (se.left.IsFrom(tr, right, true) && se.right.IsFrom(tr, left, true))
                          {
                              var ns = new SqlValueExpr(tr, se.kind, se.right, se.left, se.mod);
                              ATree<long, SqlValue>.Remove(ref joinCond, se.sqid);
                              ATree<long, SqlValue>.Add(ref joinCond, ns.sqid, ns);
                              se.left.Needed(tr, Need.joined);
                              se.right.Needed(tr, Need.joined);
                              continue;
                          }
                      }
                      ATree<long, SqlValue>.Add(ref where, b.key(), b.value());
                  } */
            r += (Display, ds);
            r += (SqlValue.NominalType,new Domain(ss));
            r += (Cols, vs);
            r += (Dependents, sb);
            r += (Depth, de);
            return (JoinPart)cx.Add(r);
        }

        /// <summary>
        /// Analysis stage Conditions: call for left and right
        /// Turn the join condition into an ordering request, and set it up
        /// </summary>
        internal override Query Conditions(Transaction tr,Context cx,Query q)
        {
            var r = (JoinPart)base.Conditions(tr,cx,q);
            q = (Query)cx.obs[q.defpos];
            var k = kind;
            if (k==Sqlx.CROSS)
                k = Sqlx.INNER;
            for (var b = joinCond.First(); b != null; b = b.Next())
                if (b.value() is SqlValueExpr se)
                    r = (JoinPart)r.AddMatchedPair(se.left, se.right);
            r = r +(LeftOperand,r.left.AddPairs(r))+(RightOperand,r.right.AddPairs(r));
            var w = where;
            var jc = joinCond;
            for (var b = where.First(); b != null; b = b.Next())
            {
                var qq = (JoinPart)b.value().JoinCondition(cx, r, ref jc, ref w);
                if (qq != r)
                {
                    w -= b.key();
                    r = qq;
                }
            }
            if (jc.Count == 0)
                k = Sqlx.CROSS;
            var lf = r.left.Conditions(tr,cx,q);
            q = (Query)cx.obs[q.defpos];
            var rg = r.right.Conditions(tr,cx,q);
            r = (JoinPart)r.New(r.mem + (LeftOperand, lf) + (RightOperand, rg)
                + (JoinCond, jc) + (JoinKind, k));
            return r.AddCondition(Where,w);
        }
        /// <summary>
        /// Now is the right time to optimise join conditions. 
        /// At this stage all comparisons have form left op right.
        /// Ideally we can find an index that makes at least some of the join trivial.
        /// Then we impose orderings for left and right that respect any remaining comparison join conditions,
        /// overriding ordering requests from top down analysis.
        /// </summary>
        /// <param name="ord">Requested top-down order</param>
        internal override Query Orders(Transaction tr,Context cx, OrderSpec ord)
        {
            var n = 0;
            var r = this;
            var k = kind;
            var jc = joinCond;
            // First try to find a perfect foreign key relationship, either way round
            if (GetRefIndex(tr,left, right, true) is FDJoinPart fa)
            {
                r += (_FDInfo,fa);
                n = (int)fa.conds.Count;
            }
            if (n < joinCond.Count && GetRefIndex(tr,right, left, false) is FDJoinPart fb) 
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
                if (GetIndex(tr,left, true) is FDJoinPart fc)
                {
                    r += (_FDInfo,fc);
                    n = (int)fc.conds.Count;
                }
                if (n < joinCond.Count && GetIndex(tr,right, false) is FDJoinPart fd)
                {
                    r +=(_FDInfo,fd);
                    n = (int)fd.conds.Count;
                }
                if (n > 0) //we will use the selected index instead of its From rowset: and order the other side for its rowset to be used
                {
                    for (var b = FDInfo.conds.First(); b != null; b = b.Next())
                        jc -=b.value().defpos;
                    k = Sqlx.NO;
                }
            }
            var lf = r.left;
            var rg = r.right;
            // Everything remaining in joinCond is not in FDInfo.conds
            for (var b = jc.First(); b != null; b = b.Next())
                if (b.value() is SqlValueExpr se) // we already know these have the right form
                {
                    lf = lf.Orders(tr,cx,
                        lf.ordSpec+(OrderSpec.Items,
                        lf.ordSpec.items+((int)lf.ordSpec.items.Count,
                        se.left)));
                    rg = rg.Orders(tr,cx,
                        rg.ordSpec+(OrderSpec.Items,
                        rg.ordSpec.items+((int)rg.ordSpec.items.Count,
                        se.right)));
                }
            if (joinCond.Count == 0)
                for(var b=ord.items.First(); b!=null; b=b.Next()) // test all of these 
                {
                    var oi = b.value();
                    if (r.left.HasColumn(oi)// && !(left.rowSet is IndexRowSet))
                        && !lf.ordSpec.HasItem(oi))
                        lf = lf.Orders(tr,cx, 
                            lf.ordSpec+(OrderSpec.Items,lf.ordSpec.items+((int)lf.ordSpec.items.Count, oi)));
                    if (r.right.HasColumn(oi)// && !(right.rowSet is IndexRowSet))
                        && !rg.ordSpec.HasItem(oi))
                        rg = rg.Orders(tr,cx,
                            rg.ordSpec + (OrderSpec.Items, rg.ordSpec.items + oi));
                }
            return r + (LeftOperand, lf) + (RightOperand, rg) + (JoinKind, k) + (JoinCond, jc);
        }
        /// <summary>
        /// See if there is a ForeignKey Index whose foreign key is taken from the one side of joinCond,
        /// and the referenced primary key is given by the corresponding terms on the other side.
        /// We will return null if the Queries are not Table Froms.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        FDJoinPart GetRefIndex(Transaction tr,Query a, Query b,bool left)
        {
            FDJoinPart best = null;
            if (a is From fa &&  b is From fb && fa.target is Table ta && fb.target is Table tb)
            {
                for (var bx = ta.indexes.First(); bx != null; bx = bx.Next())
                {
                    var x = bx.value();
                    if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                    && x.tabledefpos == ta.defpos && x.reftabledefpos == tb.defpos)
                    {
                        var cs = BList<SqlValue>.Empty;
                        var rx = tb.indexes[x.refindexdefpos];
                        var br = rx.cols.First();
                        for (var bc=x.cols.First();bc!=null&&br!=null;bc=bc.Next(),br=br.Next())
                        {
                            var found = false;
                            for (var bj = joinCond.First(); bj != null; bj = bj.Next())
                                if (bj.value() is SqlValueExpr se
                                    && (left ? se.left : se.right) is Selector sc && sc.defpos == bc.value().defpos
                                    && (left ? se.right : se.left) is Selector sd && sd.defpos == br.value().defpos)
                                {
                                    cs +=(bc.key(), se);
                                    found = true;
                                    break;
                                }
                            if (!found)
                                goto next;
                        }
                        var fi = new FDJoinPart(x, rx, cs, left);
                        if (best == null || best.conds.Count < fi.conds.Count)
                            best = fi;
                        next:;
                    }
                }
            }
            return best;
        }
        FDJoinPart GetIndex(Transaction tr,Query a,bool left)
        {
            FDJoinPart best = null;
            if (a is From fa && fa.target is Table ta)
            {
                for (var bx = ta.indexes.First(); bx != null; bx = bx.Next())
                {
                    var x = bx.value();
                    if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                        x.flags.HasFlag(PIndex.ConstraintType.Unique))
                    && x.tabledefpos == ta.defpos)
                    {
                        var cs = BList<SqlValue>.Empty;
                        var jc = joinCond;
                        for (var bc=x.cols.First();bc!=null;bc=bc.Next())
                        {
                            var found = false;
                            for (var bj = jc.First(); bj != null; bj = bj.Next())
                                if (bj.value() is SqlValueExpr se
                                    && (left ? se.left : se.right) is Selector sc
                                    && sc.defpos == bc.value().defpos)
                                {
                                    cs +=(bc.key(), se);
                                    found = true;
                                    break;
                                }
                            if (!found)
                                goto next;
                        }
                        var fi = new FDJoinPart(x, null, cs, !left);
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
        internal override Transaction Delete(Transaction tr,Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            tr = left.Delete(tr, cx, dr, eqs);
            return right.Delete(tr,cx,dr,eqs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Transaction Insert(Transaction tr,Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            Eqs(data._tr,_cx,joinCond,ref eqs); // add in equality columns
            tr = left.Insert(tr,_cx, prov, data, eqs, rs, cl); // careful: data has extra columns!
            return right.Insert(tr,_cx,prov, data, eqs, rs,cl);
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowset</param>
        internal override Transaction Update(Transaction tr, Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            tr = left.Update(tr, cx, ur, eqs,rs);
            return right.Update(tr,cx,ur,eqs,rs);
        }
        /// <summary>
        /// Check if we have a given column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>whether we have it</returns>
        internal override bool HasColumn(SqlValue s)
        {
            if (left.HasColumn(s) || right.HasColumn(s))
                return true;
            return base.HasColumn(s);
        }
        internal override Query AddRestViews(CursorSpecification q)
        {
            return this+(LeftOperand,left.AddRestViews(q))
                +(RightOperand,right.AddRestViews(q));
        }
        /// <summary>
        /// Distribute any new where condition to left and right
        /// </summary>
        /// <param name="cond">the condition to add</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">the insert data</param>
        internal override Query AddCondition(Transaction tr,Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var leftrepl = BTree<SqlValue, SqlValue>.Empty;
            var rightrepl = BTree<SqlValue, SqlValue>.Empty;
            for (var b = joinCond.First(); b != null; b = b.Next())
                b.value().BuildRepl(left, ref leftrepl, ref rightrepl);
            var r = (JoinPart)base.AddCondition(tr,cx,cond, assigns, data);
            for (var b = cond.First(); b != null; b = b.Next())
            {
                cx.Replace(left, b.value().DistributeConditions(tr, cx, left, leftrepl));
                cx.Replace(right,b.value().DistributeConditions(tr, cx, right, rightrepl));
            }
            cx.Replace(left, left.DistributeAssigns(assig)); 
            cx.Replace(right,right?.DistributeAssigns(assig));
            return (JoinPart)cx.obs[defpos];
        }
        /// <summary>
        /// Analysis stage RowSets: build the join rowset
        /// </summary>
        internal override RowSet RowSets(Transaction tr,Context cx)
        {
            for (var b = matches.First(); b != null; b = b.Next())
            {
                if (left.HasColumn(b.key()))
                    left.AddMatch(cx, b.key(), b.value());
                else
                    right.AddMatch(cx, b.key(), b.value());
            }
            var lr = left.RowSets(tr,cx);
            if (left.ordSpec != null)
            {
                var kl = new Domain(left.ordSpec.items);
                lr = new SortedRowSet(cx, left, lr, kl,
                    new TreeInfo(kl, TreeBehaviour.Allow, TreeBehaviour.Allow));
            }
            var rr = right.RowSets(tr,cx);
            if (right.ordSpec != null)
            {
                var kr = new Domain(right.ordSpec.items);
                rr = new SortedRowSet(cx, right, rr, kr,
                    new TreeInfo(kr, TreeBehaviour.Allow, TreeBehaviour.Allow));
            }
            return new JoinRowSet(cx,this,lr,rr);
        }
        internal int Compare(Transaction tr,Context cx)
        {
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var se = b.value() as SqlValueExpr;
                var c = se.left.Eval(tr, cx)?.CompareTo(se.right.Eval(tr,cx))??-1;
                if (c != 0)
                    return c;
            }
            return 0;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(Uid(left.defpos));
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
            sb.Append(Uid(right.defpos));
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
            FDConds = -223, // BTree<long,SqlValue>
            FDIndexDefPos = -224, // long
            FDRefIndexDefPos = -225, // long
            FDRefTableDefPos = -226, // long
            FDTableDefPos = -227, // long
            Reverse = -228; // bool
        /// <summary>
        /// The primary key Index giving the functional dependency
        /// </summary>
        public long indexDefPos => (long)(mem[FDIndexDefPos]??-1);
        public long tableDefPos => (long)(mem[FDTableDefPos] ?? -1);
        /// <summary>
        /// The foreign key index if any
        /// </summary>
        public long rindexDefPos => (long)(mem[FDRefIndexDefPos]??-1);
        public long rtableDefPos => (long)(mem[FDRefTableDefPos] ?? -1);
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
        public FDJoinPart(Index ix, Index rx, BList<SqlValue> c, bool r)
            :base(BTree<long,object>.Empty
                 +(FDIndexDefPos,ix.defpos)+(FDTableDefPos,ix.tabledefpos)
                 +(FDRefIndexDefPos,rx.defpos)+(FDRefTableDefPos,rx.tabledefpos)
                 +(FDConds,c)+(Reverse,r))
        { }
        protected FDJoinPart(BTree<long, object> m) : base(m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new FDJoinPart(m);
        }
    }
}
