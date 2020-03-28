using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
            Display = -177, // int
            FetchFirst = -179, // int
            Filter = -180, // BTree<long,TypedValue>
            Matches = -182, // BTree<SqlValue,TypedValue>
            Matching = -183, // BTree<SqlValue,BTree<SqlValue,bool>>
            OrdSpec = -184, // OrderSpec
            Periods = -185, // BTree<long,PeriodSpec>
            _Repl = -186, // BTree<string,string>
            RowType = -187,  // Selection (Any or defpos must match)
            SimpleQuery = -189, // From
            Where = -190; // BTree<long,SqlValue>
        public bool _aggregates => (bool)(mem[_Aggregates] ?? false);
        public string name => (string)mem[Name] ?? "";
        internal Table simpleQuery => (Table)mem[SimpleQuery]; // will be non-null for simple queries and insertable views
        internal BTree<SqlValue, TypedValue> matches =>
             (BTree<SqlValue, TypedValue>)mem[Matches] ?? BTree<SqlValue, TypedValue>.Empty; // guaranteed constants
        internal BTree<SqlValue, BTree<SqlValue, bool>> matching =>
            (BTree<SqlValue, BTree<SqlValue, bool>>)mem[Matching] ?? BTree<SqlValue, BTree<SqlValue, bool>>.Empty;
        internal BTree<string, string> replace =>
            (BTree<string,string>)mem[_Repl]??BTree<string, string>.Empty; // for RestViews
        internal int display => (int)(mem[Display]??0);
  //      internal BTree<SqlValue, SqlValue> import =>
  //          (BTree<SqlValue,SqlValue>)mem[_Import]??BTree<SqlValue, SqlValue>.Empty; // cache Imported SqlValues
        internal OrderSpec ordSpec => (OrderSpec)mem[OrdSpec];
        internal BTree<long, PeriodSpec> periods =>
            (BTree<long, PeriodSpec>)mem[Periods] ?? BTree<long, PeriodSpec>.Empty;
        internal Selection rowType => (Selection)mem[RowType];
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
        internal Query(long dp, BTree<long, object> m) : 
            base(dp, m) { }
        public static Query operator+(Query q,(long,object)x)
        {
   //         var (p, rt) = x;
   //         if (p == RowType)
  //              Validate((Selection)rt);
            return (Query)q.New(q.mem + x);
        }
        public static Query operator+(Query q,SqlValue x)
        {
            return q.Add(x);
        }
        public static Query operator+(Query q,(Query,Selection) x)
        {
            var (r, s) = x;
            var ns = new Selection(q.defpos,q.name);
            for (var b = s.info.columns.First(); b != null; b = b.Next())
                ns += new SqlRowSetCol(b.value(),r.defpos);
            return q + (RowType, ns);
        }
        public static Query operator +(Query q, (int,SqlValue) x)
        {
            return q.Add(x.Item1,x.Item2);
        }
        public static Query operator+(Query q, (SqlValue,TypedValue)x)
        {
            return (Query)q.New(q.mem+(Matches,q.matches+x));
        }
        public static Query operator -(Query q, SqlValue x)
        {
            return q.Remove(x);
        }
        public static Query operator -(Query q, int i)
        {
            return q.Remove(i);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Query(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Query(dp, mem);
        }
        internal (QuerySpecification,int) DoStar(QuerySpecification q,int i)
        {
            var qt = q.rowType;
            for (var b = rowType.First(); b != null; b = b.Next(), i++)
                qt += (i, b.value());
            return (q + (RowType, qt) + (Display, i),i);
        }

        internal override Basis Relocate(Writer wr)
        {
            var r = (Query)base.Relocate(wr);
            var ua = BTree<UpdateAssignment, bool>.Empty;
            var ch = false;
            for (var b=assig?.First();b!=null;b=b.Next())
            {
                var u = (UpdateAssignment)b.key().Relocate(wr);
                ch = ch || (u != b.key());
                ua += (u, b.value());
            }
            if (ch)
                r += (Assig, ua);
            var cs = rowType.Relocate(wr);
            if (cs != rowType)
                r += (RowType, cs);
            var fi = BTree<long, TypedValue>.Empty;
            ch = false;
            for (var b=filter?.First();b!=null;b=b.Next())
            {
                var k = wr.Fix(b.key());
                ch = ch || (k != b.key());
                fi += (k, b.value());
            }
            if (ch)
                r += (Filter, fi);
            var im = BTree<SqlValue, SqlValue>.Empty;
            ch = false;
      /*      for (var b=import?.First();b!=null;b=b.Next())
            {
                var ik = (SqlValue)b.key().Relocate(wr);
                var iv = (SqlValue)b.value().Relocate(wr);
                ch = ch || (ik != b.key() || iv != b.value());
                im += (ik, iv);
            }
            if (ch)
                r += (_Import, im); */
            var ma = BTree<SqlValue, TypedValue>.Empty;
            ch = false;
            for (var b = matches?.First(); b != null; b = b.Next())
            {
                var mk = (SqlValue)b.key().Relocate(wr);
                ch = ch || (mk != b.key());
                ma += (mk, b.value());
            }
            if (ch)
                r += (Matches,ma);
            var mg = BTree<SqlValue, BTree<SqlValue, bool>>.Empty;
            ch = false;
            for (var b=matching?.First();b!=null;b=b.Next())
            {
                var k = (SqlValue)b.key().Relocate(wr);
                ch = ch || k != b.key();
                var mm = BTree<SqlValue, bool>.Empty;
                for (var mb=b.value()?.First();mb!=null;mb=mb.Next())
                {
                    var mk = (SqlValue)mb.key().Relocate(wr);
                    ch = ch || mk != mb.key();
                    mm += (mk, mb.value());
                }
                mg += (k, mm);
            }
            if (ch)
                r += (Matching, mg);
            var ord = (OrderSpec)ordSpec?.Relocate(wr);
            if (ord != ordSpec)
                r += (OrdSpec, ord);
            var wh = BTree<long, SqlValue>.Empty;
            ch = false;
            for (var b=where.First();b!=null;b=b.Next())
            {
                var k = wr.Fix(b.key());
                var v = (SqlValue)b.value().Relocate(wr);
                ch = ch || (b.key() != k || b.value() != v);
                wh += (k, v);
            }
            if (ch)
                r += (Where, wh);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (Query)base.Frame(cx);
            var ua = BTree<UpdateAssignment, bool>.Empty;
            var ch = false;
            for (var b = assig?.First(); b != null; b = b.Next())
            {
                var u = b.key().Frame(cx);
                ch = ch || (u != b.key());
                ua += (u, b.value());
            }
            if (ch)
                r += (Assig, ua);
 /*           var cs = new Selection(defpos,name);
            ch = false;
            var ds = cx.defs[name].Item2;
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var c = (SqlValue)b.value().Frame(cx);
                if (ds?[c.name].Item1 is SqlValue sv)
                {
                    r = r.AddMatchedPair(sv, c);
                    cx.AddRowSetsPair(sv, c);
                }
                ch = ch || c != b.value();
                cs += c;
            } */
            var ma = BTree<SqlValue, TypedValue>.Empty;
            ch = false;
            for (var b = matches?.First(); b != null; b = b.Next())
            {
                var mk = (SqlValue)b.key().Frame(cx);
                ch = ch || (mk != b.key());
                ma += (mk, b.value());
            }
            if (ch)
                r += (Matches, ma);
            var mg = BTree<SqlValue, BTree<SqlValue, bool>>.Empty;
            ch = false;
            for (var b = matching?.First(); b != null; b = b.Next())
            {
                var k = (SqlValue)b.key().Frame(cx);
                ch = ch || k != b.key();
                var mm = BTree<SqlValue, bool>.Empty;
                for (var mb = b.value()?.First(); mb != null; mb = mb.Next())
                {
                    var mk = (SqlValue)mb.key().Frame(cx);
                    ch = ch || mk != mb.key();
                    mm += (mk, mb.value());
                }
                mg += (k, mm);
            }
            if (ch)
                r += (Matching, mg);
            var wh = BTree<long, SqlValue>.Empty;
            ch = false;
            for (var b = where.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = (SqlValue)b.value().Frame(cx);
                ch = ch || (b.key() != k || b.value() != v);
                wh += (k, v);
            }
            if (ch)
                r += (Where, wh);
            return cx.Add(r,true);
        }
        internal override bool Calls(long defpos, Database db)
        {
            for (var b = where?.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return Calls(rowType.cols,defpos, db);
        }
        internal virtual SqlValue ValFor(Context cx,string s)
        {
            var rt = rowType;
            var iq = rt.map[s];
            if (iq!=null)
                 return rt[iq.Value] as SqlValue;
            return null;
        }
        internal override DBObject _Replace(Context cx,DBObject was,DBObject now)
        {
            if (cx.done.Contains(defpos)) // includes the case was==this
                return cx.done[defpos];
            if (now is SqlTableCol sc)
                cx.undef -= now.defpos;
            var de = 0;
            var cs = new Selection(defpos, name);
            var ch = false;
            var r = this;
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var x = b.value();
                var bv = (SqlValue)x._Replace(cx, was, now);
                if (bv != x)
                    ch = true;
                de = Math.Max(de, bv.depth);
                cs += bv;
            }
            if (ch)
                r += (RowType, cs);
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value()._Replace(cx, was, now);
                if (v != b.value())
                    w += (b.key(), v);
                de = Math.Max(de, v.depth);
            }
            if (w!=r.where)
                r = r.AddCondition(cx,Where,w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)b.key()._Replace(cx, was, now);
                if (bk != b.key())
                    ms += (bk, b.value());
                de = Math.Max(de, bk.depth);
            }
            if (ms!=r.matches)
                r += (Matches, ms);
            var mg = r.matching;
            for (var b = mg.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)b.key()._Replace(cx, was, now);
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var ck = (SqlValue)c.key()._Replace(cx, was, now);
                    if (ck != c.key())
                        bv += (ck, c.value());
                    de = Math.Max(de, ck.depth);
                }
                if (bk != b.key() || bv != b.value())
                    mg += (bk, bv);
                de = Math.Max(de, bk.depth);
            }
            if (mg!=r.matching)
                r += (Matching, mg);
            var os = r.ordSpec;
            for (var b = os?.items.First(); b != null; b = b.Next())
            {
                var it = b.value();
                var ow = (SqlValue)it._Replace(cx,was,now);
                if (it!=ow)
                    os += (b.key(), ow);
                de = Math.Max(de, ow.depth);
            }
            if (os!=r.ordSpec)
                r += (OrdSpec, os);
            var ag = r.assig;
            for (var b = ag.First(); b != null; b = b.Next())
            {
                var aa = (SqlValue)b.key().val._Replace(cx,was,now);
                var ab = (SqlValue)b.key().vbl._Replace(cx,was,now);
                if (aa != b.key().val || ab != b.key().vbl)
                    ag += (new UpdateAssignment(ab,aa), b.value());
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
                var x = b.value();
                var bv = (SqlValue)x?.TableRef(cx, f);
                if (bv != x && bv!=null)
                    cs += (b.key(), bv);
            }
            if (cs != rowType)
                r += (RowType, cs);
            var w = r.where;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().TableRef(cx, f);
                if (v != b.value())
                    w += (b.key(), v);
            }
            if (w != r.where)
                r = r.AddCondition(cx,Where, w);
            var ms = r.matches;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)b.key().TableRef(cx, f);
                if (bk != b.key())
                    ms += (bk, b.value());
            }
            if (ms != r.matches)
                r += (Matches, ms);
            var mg = r.matching;
            for (var b = mg.First(); b != null; b = b.Next())
            {
                var bk = (SqlValue)b.key().TableRef(cx, f);
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var ck = (SqlValue)c.key().TableRef(cx, f);
                    if (ck != c.key())
                        bv += (ck, c.value());
                }
                if (bk != b.key() || bv != b.value())
                    mg += (bk, bv);
            }
            if (mg != r.matching)
                r += (Matching, mg);
            var os = r.ordSpec;
            for (var b = os?.items.First(); b != null; b = b.Next())
            {
                var it = b.value();
                var ow = (SqlValue)it.TableRef(cx, f);
                if (it != ow)
                    os += (b.key(), ow);
            }
            if (os != r.ordSpec)
                r += (OrdSpec, os);
    /*        var im = r.import;
            for (var b = im.First(); b != null; b = b.Next())
            {
                var ik = (SqlValue)b.key().TableRef(cx, f);
                var iv = (SqlValue)b.value().TableRef(cx, f);
                if (ik != b.key() || iv != b.value())
                    im += (ik, iv);
            }
            if (im != r.import)
                r += (_Import, im); */
            var ag = r.assig;
            for (var b = ag.First(); b != null; b = b.Next())
            {
                var aa = (SqlValue)b.key().val.TableRef(cx, f);
                var ab = (SqlValue)b.key().vbl.TableRef(cx, f);
                if (aa != b.key().val || ab != b.key().vbl)
                    ag += (new UpdateAssignment(ab, aa), b.value());
            }
            if (ag != r.assig)
                r += (Assig, ag);
            cx.done += (defpos, r);
            return (r == this) ? this : (Query)cx.Add(r);
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
        internal Query Add(SqlValue v)
        {
            if (v == null)
                return this;
            var rt = rowType;
            if (rt == null || rt == Selection.Star)
                rt = new Selection(defpos, name);
            var deps = dependents + (v.defpos,true);
            var dpt = _Max(depth, 1 + v.depth);
            var r = this +(Dependents,deps)+(Depth,dpt)
                +(RowType,rt + v);
            var a = v.aggregates();
            if (a)
                r += (_Aggregates, a);
            return r;
        }
        internal Query Add(int i,SqlValue v)
        {
            if (v == null)
                return this;
            var deps = dependents + (v.defpos, true);
            var dpt = _Max(depth, 1 + v.depth);
            var r = this + (Dependents, deps) + (Depth, dpt)
                + (RowType, rowType + (i,v));
            var a = v.aggregates();
            if (a)
                r += (_Aggregates, a);
            return r;
        }
        /// <summary>
        /// Remove a column from the query, and update the row type
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal virtual Query Remove(SqlValue v)
        {
            var r = this;
            if (v == null)
                return this;
            var i = rowType?.map[v.name];
            if (i == null)
                return this;
            if (i.Value<display)
                r += (Display, display - 1);
            var rt = rowType - v;
            return r + (RowType,rt) + (Dependents, _Deps(rt.cols)) 
                + (Depth,  _Max(rt.cols))
                + (_Aggregates, _Aggs(rt.cols));
        }
        internal virtual Query Remove(int i)
        {
            var r = this;
            if (i < display)
                r += (Display, display - 1);
            var rt = rowType - i;
            return r + (RowType, rt) + (Dependents, _Deps(rt.cols))
                + (Depth, _Max(rt.cols))
                + (_Aggregates, _Aggs(rt.cols));
        }
        internal virtual Query Remove(long col)
        {
            var r = this;
            SqlValue v = null;
            var i = 0;
            for (var b = rowType.First(); v == null && b != null; b = b.Next(), i++)
            {
                var sc = b.value();
                if (sc.defpos == col)
                    v = sc;
            }
            if (v == null)
                return this;
            if (i < display)
                r += (Display, display - 1);
            var rt = rowType - v;
            return r + (RowType, rt) + (Dependents, _Deps(rt.cols))
                + (Depth, _Max(rt.cols))
                + (_Aggregates, _Aggs(rt.cols));
        }
        static bool _Aggs(BList<SqlValue>ss)
        {
            var r = false;
            for (var b = ss.First(); b != null; b = b.Next())
                if (b.value().aggregates())
                    r = true;
            return r;
        }
        internal virtual Query Conditions(Context cx)
        {
            var svs = where;
            var r = this;
            for (var b = svs.First(); b != null; b = b.Next())
            {
                r = b.value().Conditions(cx, r, true, out bool mv);
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
                    var v = b.value();
                    if (v is SqlValue w)
                    {
                        if (!q.where.Contains(k))
                            qw += (k, w);
                        wh -= k;
                    }
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
        internal virtual Query Orders(Context cx, OrderSpec ord)
        {
            return (ordSpec == null || !ordSpec.SameAs(this,ord))?
                this + (OrdSpec, ord) : this;
        }
        public static bool Eval(BTree<long, SqlValue> svs, Context cx)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value().Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        internal virtual Query AddMatches(Context cx,Query q)
        {
            var m = matches;
            for (var b = q.matches.First(); b != null; b = b.Next())
                m += (b.key(), b.value());
            return (Query)cx.Replace(this,new Query(defpos, mem + (Matches, m)));
        }
        internal Query AddMatch(Context cx,SqlValue sv, TypedValue tv)
        {
            return (Query)cx.Replace(this,new Query(defpos, mem + (Matches, matches + (sv, tv))));
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
        internal virtual Query AddCondition(Context cx, BTree<long, SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return this;
        }
        internal Query AddCondition(Context cx, SqlValue cond)
        {
            cx.Replace(this,AddCondition(cx,Where, cond));
            return Refresh(cx);
        }
        internal Query AddCondition(Context cx, BTree<long,SqlValue> conds)
        {
            cx.Replace(this, AddCondition(cx,Where, conds));
            return Refresh(cx);
        }
        internal Query AddCondition(Context cx,long prop,BTree<long,SqlValue> conds)
        {
            var q = this;
            for (var b = conds.First(); b != null; b = b.Next())
                q = q.AddCondition(cx, prop, b.value());
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
                if (se.left.target>=0 && se.right is SqlLiteral ll)
                    filt += (se.left.target, ll.val);
                else if (se.right.target>=0 && se.left is SqlLiteral lr)
                    filt += (se.right.target, lr.val);
            }
            if (filt != filter)
                q += (Filter, filt);
            if (prop == Where)
                q += (Where, q.where + (cond.defpos, cond));
            else if (q is TableExpression te)
                q = te + (TableExpression.Having, te.having + (cond.defpos, cond));
            if (cond.depth >= q.depth)
                q += (Depth, cond.depth + 1);
            cx.Replace(this, q);
            return (Query)cx.Add(q);
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
        internal virtual bool Knows(SqlValue c)
        {
            return false;
        }
        internal override void Build(Context _cx, RowSet rs)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                b.value().Build(_cx, rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                b.value().StartCounter(_cx, rs);
        }
        internal new virtual void AddIn(Context _cx, Cursor rb, TRow key)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                b.value().AddIn(_cx, rb, key);
        }
        internal virtual void AddReg(Context cx, long rs, TRow key)
        {
            for (var b = where.First(); b != null; b = b.Next())
                b.value().AddReg(cx,rs,key);
        }
        public static void Eqs(Context cx, BTree<long, SqlValue> svs, ref Adapters eqs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                b.value().Eqs(cx,ref eqs);
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
        Query _AddMatchedPair(SqlValue a, SqlValue b)
        {
            if (matching[a]?.Contains(b)!=true)
            {
                var c = matching[a] ?? BTree<SqlValue, bool>.Empty;
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
        internal Query AddMatchedPair(SqlValue a, SqlValue b)
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
            return matching[a]?[b] == true;
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
                return cols[i];
            throw new PEException("PE335");
        }
        internal virtual bool Uses(long t)
        {
            return false;
        }
        internal static bool Uses(BTree<long,SqlValue> x,long t)
        {
            for (var b = x.First(); b != null; b = b.Next())
                if (b.value().Uses(t))
                    return true;
            return false;
        }
        internal override bool aggregates()
        {
            if (_aggregates)
                return true;
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().aggregates())
                    return true;
            return false;
        }
        internal Query AddCols(Context cx,Query from)
        {
            var r = this;
            var sl = new Selection(defpos, name);
            for (var b = from.rowType.First(); b != null; b = b.Next())
            {
                var c = b.value();
                sl += c;
            }
            return r+(Display, (int)sl.Length)+(RowType,sl);
        }
        internal RowSet Ordering(Context _cx, RowSet r,bool distinct)
        {
            if (ordSpec != null && ordSpec.items.Length > 0 && !ordSpec.SameAs(this,r.rowOrder))
                return new OrderedRowSet(_cx,r, ordSpec, distinct);
            if (distinct)
                return new DistinctRowSet(_cx,r);
            return r;
        }
        internal virtual RowSet RowSets(Context cx)
        {
            return cx.data[defpos];
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
        /// <summary>
        /// Selection contains an ObInfo and a Domain.
        /// Check them for consistency
        /// </summary>
        /// <param name="s"></param>
        static void Validate(Selection s)
        { 
            var oi= s.info;
            var dm = oi.domain;
            if (dm.kind == Sqlx.CONTENT)
                return;
            if (s.Length != oi.Length || oi.Length != dm.Length)
                throw new PEException("PE751");
            if (s.defpos != oi.defpos || oi.defpos != dm.defpos)
                throw new PEException("PE752");
            if (s.name != oi.name)
                throw new PEException("PE753");
            var cb = oi.columns.First();
            var db = dm.representation.First();
            for (var b= s.First();b!=null && cb!=null && db!=null;b=b.Next(),cb=cb.Next(),
                db=db.Next())
            {
                var v = b.value();
                var c = cb.value();
                var (p, d) = db.value();
                if (v.defpos != c.defpos || c.defpos != p)
                    throw new PEException("PE754");
                if ((c.domain.kind!=Sqlx.UNION && v.domain != c.domain) || c.domain != d)
                    throw new PEException("PE755");
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" RowType:");sb.Append(rowType);
            if (mem.Contains(SqlStar.Prefix)) 
            {   sb.Append(" Star="); 
                if (mem[SqlStar.Prefix] is Query pq)
                    sb.Append(Uid(pq.defpos)); 
            }
            if (mem.Contains(Assig)) { sb.Append(" Assigs:"); sb.Append(assig); }
            if (mem.Contains(Display)) { sb.Append(" Display="); sb.Append(display); }
            if (mem.Contains(FetchFirst)) { sb.Append(" FetchFirst="); sb.Append(fetchFirst); }
            if (mem.Contains(Filter)) { sb.Append(" Filter:"); sb.Append(filter); }
 //           if (mem.Contains(_Import)) { sb.Append(" Import:"); sb.Append(import); }
            if (mem.Contains(Matches)) { sb.Append(" Matches:"); sb.Append(matches); }
            if (mem.Contains(Matching)) { sb.Append(" Matching:"); sb.Append(matching); }
            if (mem.Contains(OrdSpec)) { sb.Append(" OrdSpec:"); sb.Append(ordSpec); }
            if (mem.Contains(_Repl)) { sb.Append(" Replace:"); sb.Append(replace); }
            if (mem.Contains(Where)) { sb.Append(" Where:"); sb.Append(where); }
            return sb.ToString();
        }
    }

    // An ad-hoc SystemTable for a row history: the work is mostly done by
    // LogTableSelectBookmark
    internal class LogRowTable :Query
    {
        public readonly SystemTable logRows; 
        public readonly Table targetTable;
        public LogRowTable(Transaction tr, Context cx, long td, string ta) 
            :base(tr.uid,BTree<long,object>.Empty)
        {
            targetTable = tr.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix();
            var tt = new SystemTable("" + td);
            new SystemTableColumn(tt, "Pos", Domain.Int,1);
            new SystemTableColumn(tt, "Action", Domain.Char,0);
            new SystemTableColumn(tt, "DefPos", Domain.Int,0);
            new SystemTableColumn(tt, "Transaction", Domain.Int,0);
            new SystemTableColumn(tt, "Timestamp", Domain.Timestamp,0);
            logRows = tt;
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
        : base(tr.uid, BTree<long, object>.Empty)
        {
            var tc = tr.objects[c] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            rd = r;
            cd = c;
            tb = tr.objects[tc.tabledefpos] as Table;
            var tt = new SystemTable("" + rd + ":" + cd);
            new SystemTableColumn(tt, "Pos", Domain.Int,1);
            new SystemTableColumn(tt, "Value", Domain.Char,0);
            new SystemTableColumn(tt, "StartTransaction", Domain.Int,0);
            new SystemTableColumn(tt, "StartTimestamp", Domain.Timestamp,0);
            new SystemTableColumn(tt, "EndTransaction", Domain.Int,0);
            new SystemTableColumn(tt, "EndTimestamp", Domain.Timestamp,0);
            st = tt;
        }
    }
    /// <summary>
    /// Implement a CursorSpecification 
    /// </summary>
    internal class CursorSpecification : Query
    {
        internal const long
            RVQSpecs = -192, // BList<QuerySpecification>
            RestGroups = -193, // BTree<string,int>
            RestViews = -194, // BTree<long,RestView>
            _Source = -195, // string
            Union = -196, // QueryExpression
            UsingFrom = -197; // Query
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
            : base(u, m+(Dependents,new BTree<long,bool>(((Query)m[Union])?.defpos??-1L,true))
                  +(Depth,1+((Query)m[Union])?.depth??0)) 
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (CursorSpecification)base.Relocate(wr);
            var qs = BList<QuerySpecification>.Empty;
            var ch = false;
            for (var b=rVqSpecs.First();b!=null;b=b.Next())
            {
                var n = (QuerySpecification)b.value().Relocate(wr);
                qs += n;
                if (n != b.value())
                    ch = true;
            }
            if (ch)
                r += (RVQSpecs, qs);
            var vs = BTree<long, RestView>.Empty;
            ch = false;
            for (var b=restViews.First();b!=null;b=b.Next())
            {
                var v = (RestView)b.value().Relocate(wr);
                vs += (b.key(), v);
                if (v != b.value())
                    ch = true;
            }
            if (ch)
                r += (RestViews, vs);
            var un = union.Relocate(wr);
            if (un != union)
                r += (Union, un);
            var uf = usingFrom?.Relocate(wr);
            if (uf != usingFrom)
                r += (UsingFrom, uf);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (CursorSpecification)base._Replace(cx, so, sv);
            var un = union?._Replace(cx, so, sv);
            if (un != union)
                r += (Union, un);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (CursorSpecification)base.Frame(cx);
            var u = union.Frame(cx);
            if (u != union)
                r += (Union, u);
            return cx.Add(r,true);
        }
        internal override Query Refresh(Context cx)
        {
            var r = (CursorSpecification)base.Refresh(cx);
            var un = r.union?.Refresh(cx);
            return (un == r.union) ? r : (CursorSpecification)cx.Add(r + (Union, un));
        }
        internal override bool Uses(long t)
        {
            return union.Uses(t) || base.Uses(t);
        }
        /// <summary>
        /// Analysis stage Conditions: do Conditions on the union.
        /// </summary>
        internal override Query Conditions(Context cx)
        {
            var u = union;
            var r = MoveConditions(cx, u);
            u = (QueryExpression)Refresh(cx).Conditions(cx);
            return new CursorSpecification(defpos, r.mem + (Union, u.Refresh(cx)));
        }
        internal override Query Orders(Context cx, OrderSpec ord)
        {
            return base.Orders(cx,ord)+(Union, union.Orders(cx, ord));
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
        internal override Query AddCondition(Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var cs = new CursorSpecification(defpos,base.AddCondition(cx,cond, assigns, data).mem);
            return cs+(Union,cs.union.AddCondition(cx,cond, assigns, data));
        }
        internal override bool Knows(SqlValue c)
        {
            return base.Knows(c) || (union?.Knows(c) ?? usingFrom?.Knows(c) ?? false);
        }
        internal override bool aggregates()
        {
            return union.aggregates() || base.aggregates();
        }
        internal override RowSet RowSets(Context cx)
        {
            var r = union.RowSets(cx);
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
        internal override void AddIn(Context _cx, Cursor rs, TRow key)
        {
            union.AddIn(_cx,rs,key);
            base.AddIn(_cx,rs,key);
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
        internal override Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, 
            Level cl)
        {
            return union.Insert(_cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return union.Update(cx,ur, eqs,rs);
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return union.Delete(cx,dr,eqs);
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
            Needed = -178, // Selection
            From = -198, // Query
            Group = -199, // GroupSpecification
            Having = -200, // BTree<long,SqlValue>
            Windows = -201; // BTree<long,WindowSpecification>
        /// <summary>
        /// The from clause of the tableexpression
        /// </summary>
        internal Query from => (Query)mem[From];
        internal Selection needed => (Selection)mem[Needed];
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
        internal TableExpression(long u, Selection ne, BTree<long,object> m) 
            : base(u,m+(Needed,ne))
        { }
        protected TableExpression(long u, BTree<long, object> m) : base(u, m) 
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
        internal override Basis Relocate(Writer wr)
        {
            var r=(TableExpression)base.Relocate(wr);
            var f = from.Relocate(wr);
            if (f != from)
                r += (From, f);
            var hv = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=having?.First();b!=null; b=b.Next())
            {
                var h = (SqlValue)b.value().Relocate(wr);
                hv += (b.key(), h);
                if (h != b.value())
                    ch = true;
            }
            if (ch)
                r += (Having, hv);
            var ws = BTree<long, WindowSpecification>.Empty;
            ch = false;
            for (var b=window?.First();b!=null;b=b.Next())
            {
                var w = (WindowSpecification)b.value().Relocate(wr);
                ws += (b.key(), w);
                if (w != b.value())
                    ch = true;
            }
            if (ch)
                r += (Windows, ws);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TableExpression)base._Replace(cx, was, now);
            var fm = from?._Replace(cx, was, now);
            if (fm != from)
                r += (From, fm);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Refresh(Context cx)
        {
            var r = (TableExpression)base.Refresh(cx);
            var fm = r.from?.Refresh(cx);
            return (fm == r.from) ? r : (TableExpression)cx.Add(r + (From, fm));
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (TableExpression)base.Frame(cx);
            var f = from.Frame(cx);
            if (f != from)
                r += (From, f);
            var hv = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=having.First();b!=null;b=b.Next())
            {
                var h = (SqlValue)b.value().Frame(cx);
                if (h != b.value())
                    ch = true;
                hv += (b.key(), h);
            }
            if (ch)
                r += (Having, hv);
            var ws = BTree<long, WindowSpecification>.Empty;
            ch = false;
            for (var b = window?.First(); b != null; b = b.Next())
            {
                var w = (WindowSpecification)b.value().Frame(cx);
                if (w != b.value())
                    ch = true;
                ws += (b.key(), w);
            }
            if (ch)
                r += (Windows, ws);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return from.Uses(t);
        }
        internal override bool aggregates()
        {
            return from.aggregates() || base.aggregates();
        }
        internal override RowSet RowSets(Context cx)
        {
            var r = from.RowSets(cx);
            if (r == null)
                return null;
            if (where.Count > 0)
            {
                var gp = false;
                if (group != null)
                    for (var gs = group.sets.First();gs!=null;gs=gs.Next())
                        gs.value().Grouped(where, ref gp);
            }
            return new TableExpRowSet(defpos, cx, needed, r, where, matches);
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
        internal override void AddIn(Context _cx, Cursor rs, TRow key)
        {
            from.AddIn(_cx,rs,key);
            base.AddIn(_cx,rs,key);
        }
        internal override bool Knows(SqlValue c)
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
        internal override Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return from.Insert(_cx,prov, data, eqs, rs,cl);
        }
        /// <summary>
        /// propagate Delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,BTree<string, bool> dr, Adapters eqs)
        {
            return from.Delete(cx,dr,eqs);
        }
        /// <summary>
        /// propagate Update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Update(Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet> rs)
        {
            return from.Update(cx,ur,eqs,rs);
        }
        internal override Query AddMatches(Context cx, Query q)
        {
            var m = mem;
            for (var b = having.First(); b != null; b = b.Next())
                q = b.value().AddMatches(q);
            q = from.AddMatches(cx, q);
            cx.Replace(this,q);
            return Refresh(cx);
        }
        internal override Query Conditions(Context cx)
        {
            var r = (TableExpression)MoveConditions(cx, from).Refresh(cx);
            r.AddHavings(cx, r.from);
            var fm = r.from.Refresh(cx);
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
                var v = b.value().Import(q);
                if (v is SqlValue w && !q.where.Contains(k))
                        qw += (k, w);
            }
            if (qw != q.where)
                cx.Replace(q, q.AddCondition(cx,Where, qw));
        }
        internal override Query Orders(Context cx,OrderSpec ord)
        {
            return ((TableExpression)base.Orders(cx,ord))+(From,from.Orders(cx,ord));
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
        internal override Query AddCondition(Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            return (TableExpression)(base.AddCondition(cx,cond, assigns, data)+
            (From,from.AddCondition(cx,cond, assigns, data)));
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
    internal class JoinPart : Query
    {
        internal const long
            _FDInfo = -202, // FDJoinPart
            JoinCond = -203, // BTree<long,SqlValue>
            JoinKind = -204, // Sqlx
            LeftInfo = -205, // OrderSpec
            LeftOperand = -206, // Query
            Natural = -207, // Sqlx
            NamedCols = -208, // BList<long>
            RightInfo = -209, // OrderSpec
            RightOperand = -210; //Query
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
        public override Sqlx kind => (Sqlx)(mem[JoinKind]??Sqlx.CROSS);
        /// <summary>
        /// The join condition is implemented by ordering, using any available indexes.
        /// Rows in the join will use left/rightInfo.Keys() for ordering and theta-operation.
        /// </summary>
        internal OrderSpec leftInfo => (OrderSpec)mem[LeftInfo]; // initialised once domain is known
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
            : base(u,(m??BTree<long,object>.Empty)+(_Domain,Domain.TableType)) { }
        public static JoinPart operator+ (JoinPart j,(long,object)x)
        {
            return new JoinPart(j.defpos, j.mem + x);
        }
        public static JoinPart operator -(JoinPart j, SqlValue v)
        {
            return (JoinPart)j.Remove(v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinPart(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new JoinPart(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (JoinPart)base.Relocate(wr);
            var co = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var c = (SqlValue)b.value().Relocate(wr);
                co += (b.key(), c);
                if (c != b.value())
                    ch = true;
            }
            if (ch)
                r += (JoinCond, co);
            var os = leftInfo?.Relocate(wr);
            if (os != leftInfo)
                r += (LeftInfo, os);
            os = rightInfo?.Relocate(wr);
            if (os != rightInfo)
                r += (RightInfo, os);
            var cs = BList<long>.Empty;
            ch = false;
            for (var b=namedCols.First();b!=null;b=b.Next())
            {
                var n = wr.Fix(b.value());
                cs += n;
                if (n != b.value())
                    ch = true;
            }
            if (ch)
                r += (NamedCols, cs);
            var q = (Query)left.Relocate(wr);
            if (q != left)
                r += (LeftOperand, q);
            q = (Query)right.Relocate(wr);
            if (q != right)
                r += (RightOperand, q);
            return r;
        }
        internal override bool Knows(SqlValue c)
        {
            return left.Knows(c) || right.Knows(c);
        }
        internal override bool Uses(long t)
        {
            return left.Uses(t) || right.Uses(t) || Uses(joinCond,t);
        }
        internal override bool aggregates()
        {
            return left.aggregates()||right.aggregates()||base.aggregates();
        }
        internal override Query AddMatches(Context cx,Query q)
        {
            var lf = left.AddMatches(cx,q);
            var rg = right.AddMatches(cx,q);
            var r = base.AddMatches(cx,q);
            for (var b = joinCond.First(); b != null; b = b.Next())
                r = b.value().AddMatches(q);
            return (Query)cx.Replace(this,new JoinPart(r.defpos,r.mem
                +(LeftOperand,lf)+(RightOperand,rg)));
        }
        internal override Query Refresh(Context cx)
        {
            var r = (JoinPart)base.Refresh(cx);
            var rr = r;
            var lf = r.left.Refresh(cx);
            if (lf != r.left)
                r += (LeftOperand, lf);
            var rg = r.right.Refresh(cx);
            if (rg != r.right)
                r += (RightOperand, rg);
            return (r == rr) ? rr : (JoinPart)cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (JoinPart)base.Frame(cx);
            var f = left.Frame(cx);
            if (f != left)
                r += (LeftOperand, f);
            var s = right.Frame(cx);
            if (s != right)
                r += (RightOperand, s);
            var jc = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var v = b.value();
                var nv = (SqlValue)v.Frame(cx);
                if (v != nv)
                    ch = true;
                jc += (b.key(), nv);
            }
            if (ch)
                r += (JoinCond, jc);
            return cx.Add(r,true);
        }
        /// <summary>
        /// Analysis stage Selects: call for left and right.
        /// </summary>
        internal Query Selects(Context cx, QuerySpecification qs)
        {
            var r = this;
            int n = left.display;
            var lo = new Selection(left.defpos,left.name); // left ordering
            var ro = new Selection(right.defpos,right.name); // right
            var de = qs.depth;
            var sb = dependents;
            var cs = new Selection(defpos, name); 
            if (naturaljoin != Sqlx.NO)
            {
                int m = 0; // common.Count
                int rn = right.display;
                // which columns are common?
                bool[] lc = new bool[n];
                bool[] rc = new bool[rn];
                for (int i = 0; i < rn; i++)
                    rc[i] = false;
                var lt = left.rowType;
                var rt = right.rowType;
                var oq = qs;
                for (int i = 0; i < n; i++)
                {
                    var ll = lt[i];
                    for (int j = 0; j < rn; j++)
                    {
                        var rr = rt[j];
                        if (ll.name.CompareTo(rr.name) == 0)
                        {
                            lc[i] = true;
                            rc[j] = true;
                            var cp = new SqlValueExpr(rr.defpos - 1, Sqlx.EQL, ll, rr, Sqlx.NULL);
                            r += (JoinCond, cp.Disjoin());
                            lo += ll;
                            ro += rr;
                            r -= rr;
                            qs -= rr.defpos;
                            m++;
                            break;
                        }
                    }
                    if (lc[i])
                    {
                        de = _Max(de, 1 + ll.depth);
                        sb += (ll.defpos, true);
                        cs += ll;
                    }
                }
                if (oq != qs)
                    cx.Add(qs);
                for (int i = 0; i < n; i++)
                    if (!lc[i])
                    {
                        var ll = left.rowType[i];
                        de = _Max(de, 1 + ll.depth);
                        sb += (ll.defpos, true);
                        cs += ll;
                    }
                for (int i = 0; i < rn; i++)
                    if (!rc[i])
                    {
                        var rr = right.rowType[i];
                        de = _Max(de, 1 + rr.depth);
                        sb += (rr.defpos, true);
                        cs += rr;
                    }
                if (m == 0)
                    r += (JoinKind, Sqlx.CROSS);
                else
                {
                    r += (LeftOperand, cx.Add(left + (OrdSpec, new OrderSpec(lo))));
                    r += (RightOperand, cx.Add(right + (OrdSpec, new OrderSpec(ro))));
                }
            }
            else
            {
                for (int j = 0; j < left.rowType.Length; j++)
                {
                    var cl = left.rowType[j];
                    for (var i = 0; i < right.rowType.Length; i++)
                        if (right.rowType.map.Contains(cl.name) && !(cl is SqlValueExpr))
                        {
                            var al = (cl.defpos < Transaction.Analysing) ? cx.nextHeap++ : cl.defpos;
                            var nn = (left.alias ?? left.name) + "." + cl.name;
                            var nc = cl + (_Alias, nn)+(SqlValue.Info,cl.info+(Name,nn));
                            cx.Replace(cl, nc);
                            cl = nc;
                            break;
                        }
                    de = _Max(de, 1 + cl.depth);
                    sb += (cl.defpos, true);
                    cs += cl;
                }
                for (int j = 0; j < right.rowType.Length; j++)
                {
                    var cr = right.rowType[j];
                    for (var i = 0; i < left.rowType.Length; i++)
                        if (left.rowType.map.Contains(cr.name) && !(cr is SqlValueExpr))
                        {
                            var ar = (cr.defpos < Transaction.Analysing) ? cx.nextHeap++ : cr.defpos;
                            var nn = (right.alias ?? right.name) + "." + cr.name;
                            var nc = cr + (_Alias, nn) + (SqlValue.Info, cr.info + (Name, nn));
                            cx.Replace(cr, nc);
                            cr = nc;
                            break;
                        }
                    de = _Max(de, 1 + cr.depth);
                    sb += (cr.defpos, true);
                    cs += cr;
                }
            }
            // first ensure each joinCondition has the form leftExpr compare rightExpr
            // if not, move it to where
            var jcond = r.joinCond;
            var wh = where;
            for (var b = r.joinCond.First(); b != null; b = b.Next())
            {
                if (b.value() is SqlValueExpr se)
                {
                    de = _Max(de, 1 + se.depth);
                    if (se.left.isConstant || se.right.isConstant)
                        continue;
                    if (se.left.IsFrom(left, true) && se.right.IsFrom(right, true))
                    {
               //         se.left.Needed(tr, Need.joined);
               //         se.right.Needed(tr, Need.joined);
                        continue;
                    }
                    if (se.left.IsFrom(right, true) && se.right.IsFrom(left, true))
                    {
                        jcond += (se.defpos,
                            new SqlValueExpr(se.defpos,se.kind, se.right, se.left, se.mod));
               //         se.left.Needed(tr, Need.joined);
               //         se.right.Needed(tr, Need.joined);
                        continue;
                    }
                }
                wh += (b.key(), b.value());
            }
            r = r + (Depth, de) + (JoinCond, jcond) + (Where,wh) + (RowType,cs)
                +(LeftOperand,cx.obs[left.defpos])+(RightOperand,cx.obs[right.defpos]);
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
                if (b.value() is SqlValueExpr se && se.kind==Sqlx.EQL)
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
            var lf = r.left.Conditions(cx);
            var rg = r.right.Conditions(cx);
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
        internal override Query Orders(Context cx, OrderSpec ord)
        {
            var n = 0;
            var r = this;
            var k = kind;
            var jc = joinCond;
            // First try to find a perfect foreign key relationship, either way round
            if (GetRefIndex(cx, left, right, true) is FDJoinPart fa)
            {
                r += (_FDInfo,fa);
                n = (int)fa.conds.Count;
            }
            if (n < joinCond.Count && GetRefIndex(cx,right, left, false) is FDJoinPart fb) 
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
                if (GetIndex(cx.tr,left, true) is FDJoinPart fc)
                {
                    r += (_FDInfo,fc);
                    n = (int)fc.conds.Count;
                }
                if (n < joinCond.Count && GetIndex(cx.tr,right, false) is FDJoinPart fd)
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
            var lf = r.left;
            var rg = r.right;
            // Everything remaining in joinCond is not in FDInfo.conds
            for (var b = jc.First(); b != null; b = b.Next())
                if (b.value() is SqlValueExpr se) // we already know these have the right form
                {
                    var lo = lf.ordSpec ?? new OrderSpec(lf.defpos, lf.name);
                    var ro = rg.ordSpec ?? new OrderSpec(rg.defpos, rg.name);
                    if (!lo.HasItem(se.left))
                        lf = lf.Orders(cx,lo+(OrderSpec.Items,lo.items+se.left));
                    if (!ro.HasItem(se.right))
                        rg = rg.Orders(cx,ro+(OrderSpec.Items,ro.items+se.right));
                }
            if (joinCond.Count == 0)
                for(var b=ord?.items.First(); b!=null; b=b.Next()) // test all of these 
                {
                    var oi = b.value();
                    var lo = lf.ordSpec ?? new OrderSpec(lf.defpos, lf.name);
                    var ro = rg.ordSpec ?? new OrderSpec(rg.defpos, rg.name);
                    if (r.left.HasColumn(oi)// && !(left.rowSet is IndexRowSet))
                        && !lo.HasItem(oi))
                        lf = lf.Orders(cx,lo+(OrderSpec.Items,lo.items+oi));
                    if (r.right.HasColumn(oi)// && !(right.rowSet is IndexRowSet))
                        && !ro.HasItem(oi))
                        rg = rg.Orders(cx,ro + (OrderSpec.Items, ro.items+oi));
                }
            cx.Add(lf);
            cx.Add(rg);
            return (Query)cx.Add(r + (LeftOperand, lf) + (RightOperand, rg) + (JoinKind, k) + (JoinCond, jc));
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
            if (a is From fa &&  b is From fb && cx.tr.objects[fa.target] is Table ta 
                && cx.tr.objects[fb.target] is Table tb)
            {
                for (var bx = ta.indexes.First(); bx != null; bx = bx.Next())
                {
                    var x = (Index)cx.tr.objects[bx.value()];
                    if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                    && x.tabledefpos == ta.defpos && x.reftabledefpos == tb.defpos)
                    {
                        var cs = BTree<long,SqlValue>.Empty;
                        var rx = (Index)cx.tr.objects[x.refindexdefpos];
                        var br = rx.keys.First();
                        for (var bc=x.keys.First();bc!=null&&br!=null;bc=bc.Next(),br=br.Next())
                        {
                            var found = false;
                            for (var bj = joinCond.First(); bj != null; bj = bj.Next())
                                if (bj.value() is SqlValueExpr se
                                    && (left ? se.left : se.right) is SqlValue sc && sc.Defpos() == bc.value().defpos
                                    && (left ? se.right : se.left) is SqlValue sd && sd.Defpos() == br.value().defpos)
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
        FDJoinPart GetIndex(Transaction tr,Query a,bool left)
        {
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
                                if (bj.value() is SqlValueExpr se
                                    && (left ? se.left : se.right) is SqlValue sc
                                    && sc.defpos == bc.value().defpos)
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
            cx = left.Delete(cx, dr, eqs);
            return right.Delete(cx,dr,eqs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            Eqs(_cx,joinCond,ref eqs); // add in equality columns
            _cx = left.Insert(_cx, prov, data, eqs, rs, cl); // careful: data has extra columns!
            return right.Insert(_cx,prov, data, eqs, rs,cl);
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
            cx = left.Update(cx, ur, eqs,rs);
            return right.Update(cx,ur,eqs,rs);
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
        internal override Query AddCondition(Context cx,BTree<long,SqlValue> cond,
            BList<UpdateAssignment> assigns, RowSet data)
        {
            var leftrepl = BTree<SqlValue, SqlValue>.Empty;
            var rightrepl = BTree<SqlValue, SqlValue>.Empty;
            for (var b = joinCond.First(); b != null; b = b.Next())
                b.value().BuildRepl(left, ref leftrepl, ref rightrepl);
            var r = (JoinPart)base.AddCondition(cx,cond, assigns, data);
            for (var b = cond.First(); b != null; b = b.Next())
            {
                cx.Replace(left, b.value().DistributeConditions(cx, left, leftrepl));
                cx.Replace(right,b.value().DistributeConditions(cx, right, rightrepl));
            }
            cx.Replace(left, left.DistributeAssigns(assig)); 
            cx.Replace(right,right?.DistributeAssigns(assig));
            return (JoinPart)Refresh(cx);
        }
        /// <summary>
        /// Analysis stage RowSets: build the join rowset
        /// </summary>
        internal override RowSet RowSets(Context cx)
        {
            for (var b = matches.First(); b != null; b = b.Next())
            {
                if (left.HasColumn(b.key()))
                    left.AddMatch(cx, b.key(), b.value());
                else
                    right.AddMatch(cx, b.key(), b.value());
            }
            if (FDInfo is FDJoinPart fd)
            {
                var ds = new IndexRowSet(cx, fd.table, fd.index);
                var rs = new IndexRowSet(cx, fd.rtable, fd.rindex);
                if (fd.reverse)
                    return new JoinRowSet(cx, this, new SelectedRowSet(cx,left,ds), 
                        new SelectedRowSet(cx,right,rs));
                else
                    return new JoinRowSet(cx, this, new SelectedRowSet(cx,left,rs), 
                        new SelectedRowSet(cx,right,ds));
            }
            var lr = left.RowSets(cx);
            if (left.ordSpec?.items.Length >0)
            {
                var kl = left.ordSpec.items;
                lr = new SortedRowSet(cx, lr, kl.info,
                    new TreeInfo(kl.info, TreeBehaviour.Allow, TreeBehaviour.Allow));
            }
            var rr = right.RowSets(cx);
            if (right.ordSpec?.items.Length>0)
            {
                var kr = right.ordSpec.items;
                rr = new SortedRowSet(cx, rr, kr.info,
                    new TreeInfo(kr.info, TreeBehaviour.Allow, TreeBehaviour.Allow));
            }
            return new JoinRowSet(cx,this,lr,rr);
        }
        internal int Compare(Context cx)
        {
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var se = b.value() as SqlValueExpr;
                var c = se.left.Eval(cx)?.CompareTo(se.right.Eval(cx))??-1;
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
            FDConds = -211, // BTree<long,SqlValue>
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
    }
    internal class Selection
    {
        internal long defpos => info.defpos;
        internal readonly BList<SqlValue> cols;
        internal BTree<string, int?> map => info.map;
        internal ObInfo info;
        internal string name => info.name;
        public static Selection Empty = new Selection();
        public static Selection Star = new Selection();
        public int Length => cols.Length;
        Selection() 
        { 
            cols = BList<SqlValue>.Empty;
            info = ObInfo.Any;
        }
        internal Selection(long dp,string nm)
        {
            cols = BList<SqlValue>.Empty;
            info = new ObInfo(dp,nm);
        }
        internal Selection(SystemTable st)
        {
            var cs = BList<SqlValue>.Empty;
            var mp = BTree<string, int?>.Empty;
            var nf = new ObInfo(st.defpos, st.name, st.domain, BList<ObInfo>.Empty);
            var n = 0;
            for (var b=st.tableCols.First();b!=null;b=b.Next(),n++)
            {
                var tc = (SystemTableColumn)b.value();
                cs += new SqlTableCol(tc.defpos, tc.name, defpos, tc);
                mp += (tc.name, n);
                nf += new ObInfo(tc.defpos, tc.name, tc.domain, Grant.AllPrivileges);
            }
            info = nf;
            cols = cs;
        }
        Selection(BList<SqlValue> cs, BTree<string, int?> m, Domain dt, string n, 
            long dp)
        {
            cols = cs;
            var infs = BList<ObInfo>.Empty;
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var c = b.value();
                var nm = (c.alias==null || c.alias == "") ? c.name : c.alias;
                infs += new ObInfo(c.defpos,nm,c.domain);
            }
            info = new ObInfo(dp, n, dt, infs);
        }
        Selection(BList<SqlValue> cs, ObInfo inf)
        {
            cols = cs;
            info = inf;
        }
        public static Selection operator+(Selection s,(int,SqlValue) x)
        {
            if (s.defpos == -1L)
                throw new PEException("PE198");
            var (i, v) = x;
            var cols = s.cols + (i, v);
            return new Selection(cols, s.info + (i,v.info));
        }
        public static Selection operator+(Selection s,SqlValue v)
        {
            return s + (s.Length, v);
        }
        public static Selection operator+(Selection s,(string,int)x)
        {
            return new Selection(s.cols, s.map + (x.Item1, x.Item2),
                s.info.domain, s.name, s.defpos);
        }
        public static Selection operator-(Selection s,SqlValue v)
        {
            return s - s.map[v.name].Value;
        }
        public static Selection operator-(Selection s,int i)
        {
            var ma = BTree<string, int?>.Empty;
            for (var b = s.map.First(); b != null; b = b.Next())
            {
                var k = b.value().Value;
                if (k == i)
                    continue;
                ma += (b.key(), ((k > i) ? (k - 1) : k));
            }
            return new Selection(s.cols - i, ma, s.info.domain, s.name, s.defpos);
        }
        internal SqlValue this[string n] => this[map[n] ?? -1];
        internal SqlValue this[long p]
        {
            get
            {
                for (var b = cols.First(); b!=null; b=b.Next())
                {
                    var s = b.value();
                    if (s.defpos == p)
                        return s;
                }
                return null;
            }
        }
        internal bool Contains(long pos)
        {
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var s = b.value();
                if (s.defpos == pos)
                    return true;
            }
            return false;
        }
        internal SqlValue this[int i] =>cols[i];
        internal ABookmark<int,SqlValue> First() => cols.First();
        public TRow Parse(Scanner lx,bool union=false)
        {
            return (TRow)info.Parse(lx, union);
        }
        public Selection Needs(long dp)
        {
            var r = new Selection(dp,"");
            for (var b = First(); b != null; b = b.Next())
                r = b.value().Needs(r);
            return r;
        }
        internal Selection Relocate(Writer wr)
        {
            var r = new Selection(wr.Fix(defpos), name);
            for (var b=cols.First();b!=null;b=b.Next())
                r += (SqlValue)b.value().Relocate(wr);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append('('); sb.Append(cols); sb.Append(')');
            return sb.ToString();
        }
    }
}
