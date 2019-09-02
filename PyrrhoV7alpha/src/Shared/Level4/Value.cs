using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using PyrrhoBase;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Xml;
#if !SILVERLIGHT && !WINDOWS_PHONE
using System.Xml.XPath;
using System.Net;
#endif
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
    /// The SqlValue class corresponds to the occurrence of identifiers and expressions in
    /// SELECT statements etc: they are evaluated in a RowSet 
    /// So Eval is a way of getting the current TypedValue for the identifier etc for the current
    /// rowset positions.
    /// SqlValues are constructed for every data reference in the SQL source of a Query or Activation. 
    /// Many of these are SqlNames constructed for an identifier in a query: 
    /// during query analysis all of these must be resolved to a corresponding data reference 
    /// (so that many SqlNames are resolved to the same thing). 
    /// An SqlValue’s home context is the Query, Activation, or SqlValue whose source defines it.
    /// Others are SqlLiterals for any constant data in the SQL source and 
    /// SqlValues accessed by base tables referenced in the From part of a query. 
    /// Obviously some SqlValues will be rows or arrays: 
    /// The elements of row types are SqlColumns listed among the references of the row,
    /// there is an option to place the column TypedValues in its variables
    /// SqlNames are resolved for a given context. 
    /// This mechanism distinguishes between cases where the same identifier in SQL 
    /// can refer to different data in different places in the source 
    /// (e.g. names used within stored procedures, view definitions, triggers etc). 
    /// </summary>
    internal class SqlValue : IComparable
    {
        internal static long _id = 0;
        internal readonly long sqid = ++_id;
        internal readonly Transaction tr;
        protected readonly Ident _name;
        internal Ident alias = null;
        internal Sqlx kind = Sqlx.NO;
        internal Domain nominalDataType = Domain.Content;
        internal ETag etag = null;
        internal ATree<SqlValue,int> needed = BTree<SqlValue,int>.Empty; // this expression needs this data: Done in constructors
        /// <summary>
        /// a multiset for checking DISTINCT
        /// </summary>
        protected TMultiset dset = null;
        internal bool isSetup = false;
        /// <summary>
        /// constructor: a new sqlValue
        /// </summary>
        /// <param name="cx">the context<param>
        /// <param name="ty">the kind of SqlValue</param>
        internal SqlValue(Transaction t, Domain dt,Ident n = null)
        {
            tr = t;
            nominalDataType = dt;
            _name = n ?? new Ident("C_"+sqid,0);
        }
        protected SqlValue(Transaction t, Ident n) : this(cx, Domain.Value, n) {}
        protected SqlValue(SqlValue v,ref ATree<long,SqlValue> vs)
        {
            tr = v.tr;
            ATree<long, SqlValue>.Add(ref vs, v.sqid, this);
            _name = v._name;
            alias = v.alias;
            kind = v.kind;
            nominalDataType = v.nominalDataType;
            etag = v.etag;
            needed = v.needed;
        }
        internal virtual Context Ctx()
        {
            return null;
        }
        internal void Needs(params SqlValue[] vs)
        {
            if (vs == null)
                return;
            foreach (var v in vs)
                if (v != null && !(v is SqlLiteral))
                    ATree<SqlValue, int>.Add(ref needed, v, (int)needed.Count);
        }
        internal void Needs(ATree<long, SqlValue> w)
        {
            for (var b = w.First(); b != null; b = b.Next())
                ATree<SqlValue, int>.Add(ref needed, b.value(), (int)needed.Count);
        }
        internal void Needs(Transaction t,Query q)
        {
            for (var b=q?.needs.First();b!=null;b=b.Next())
                if (q.Lookup(cx,b.key()) is SqlValue sv && !needed.Contains(sv))
                    ATree<SqlValue, int>.Add(ref needed, q.Lookup(cx,b.key()),(int)needed.Count);
        }
        internal virtual void _AddNeeds(List<TColumn> cs)
        {
            for (var b = needed.First(); b != null; b = b.Next())
                b.key().AddNeeds(cs);
        }
        internal void AddNeeds(List<TColumn> cs)
        {
            if (alias == null)
                _AddNeeds(cs);
        }
        internal virtual void AddNeeds(GroupingRowSet.GroupInfo gi)
        {
            for (var b = needed.First(); b != null; b = b.Next())
                b.key().AddNeeds(gi);
        }
        internal virtual void AddNeeds(Transaction tr,Query q)
        {
            for (var b = needed.First(); b != null; b = b.Next())
                b.key().AddNeeds(tr,q);
        }
        internal virtual SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlValue(this, ref vs);
        }
        internal SqlValue Copy(ref ATree<long,SqlValue>vs)
        {
            if (this is SqlFunction sf && sf.query?.defs[alias] is SqlTypeColumn stc)
                return stc;
            return vs[sqid] ?? _Copy(ref vs);
        }
        /// <summary>
        /// The Import transformer allows the the import of an SqlValue expression into a subquery
        /// or view. This means that identifiers/column names will refer to their meanings inside
        /// the subquery.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q">The inner query</param>
        /// <returns></returns>
        internal virtual SqlValue Import(Transaction tr, Query q)
        {
            return this;
        }
        /// <summary>
        /// The Export transformer allows things like select * from a view. In simple cases this merely
        /// copies column names from the view. 
        /// But if a selector in the view is COUNT(*) this will not
        /// make sense outside the view. Similar problems arise with table-valued functions.
        /// In such a case Export will replace it with a simple column, with the blockid 
        /// for the GroupRowSet, EvalRowSet, or SqlCall.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q">The outer query</param>
        /// <returns></returns>
        internal virtual SqlValue Export(Transaction tr, Query q)
        {
            return this;
        }
        internal static ATree<long,SqlValue> Import(ATree<long,SqlValue> svs,Transaction tr,Query q)
        {
            var r = BTree<long,SqlValue>.Empty;
            for (var b = svs.First(); b != null; b = b.Next())
                if (b.value().Import(tr, q) is SqlValue v)
                    ATree<long,SqlValue>.Add(ref r, v.sqid, v);
            return r;
        }
        internal virtual void Disjunction(List<SqlValue> cond)
        {
            cond.Add(this);
        }
        internal virtual Ident name
        {
            get {
                return _name;
            }
    /*        set
            {
                _name = value;
                Resolve(tr);
            } */
        }
        internal virtual void Resolve(Transaction tr,Query f)
        {
            var cx = tr.context;
            // resolve all unresolved SqlNames matching this
            for (var q = cx.cur as Query; q != null; q = q.enc)
                if (q is SelectQuery qs)
                {
                    SqlName nx = null;
                    for (var s = qs.unresolved; s != null; s = nx) // s=s.next
                    {
                        nx = s.next; // s.next may get clobbered in s.Resolved
                        if (cx.Lookup(tr,s) == this || (nominalDataType.kind == Sqlx.DOCUMENT && s._name.ident == _name.ident))
                        {
                            var tgt = this;
                            s._name.Set(this, tr);
                            if (s._name.sub != null && _name.ToString() != s._name.ToString())
                            {
                                var dt = nominalDataType.Path(tr,s._name.sub);
                                if (dt != null)
                                    tgt = new SqlTypeColumn(tr,dt, s._name, true, false,f);
                            }
                            s.Resolved(tr,f,tgt);
                            qs.Add(tgt);
                            var i = s.ColFor(qs);
                            if (i >= 0)
                                qs.SetAt(i, tgt, s._name);
                        }
                    }
                }
        }

        internal virtual void AddMatches(Transaction tr, Query f)
        {
        }

        /// <summary>
        /// Get the position of a given expression, allowing for adapter functions
        /// </summary>
        /// <param name="sv">the expression</param>
        /// <returns>the position in the query</returns>
        internal virtual int ColFor(Query q)
        {
            var n = (q.alias != null && name.ident == q.alias.ident && name.sub != null) ? name.sub : name;
            for (int i = 0; i < q.cols.Count; i++)
                if (q.cols[i] is SqlValue sv && ((n != null) ? n.CompareTo(sv.name) : ToString().CompareTo(sv.ToString())) == 0)
                    return i;
            return -1;
        }
        void Replace(ref ATree<string,SqlValue> t,SqlValue ov,SqlValue nv)
        {
            for(var b=t.First();b!= null;b=b.Next())
                if (b.value()== ov)
                    ATree<string, SqlValue>.Update(ref t, b.key(), nv);
        }
        /// <summary>
        /// Return true if the cache contains any out-of-date values:
        /// build a new cache as we go. 
        /// </summary>
        /// <param name="cacheWhere"></param>
        /// <param name="newcache"></param>
        /// <returns></returns>
        internal virtual bool Check(Transaction tr, RowSet rs,ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            if (isConstant)
                return false;
            var v = Eval(tr,rs).NotNull();
            if (v != null)
                ATree<SqlValue, TypedValue>.Add(ref newcache, this, v);
            return !tr.Match(v, cacheWhere?[this]);
        }
        /// <summary>
        /// The mnethod is to be called where ctx is known to be the correct context for evaluation.
        /// Many SqlValues (Variable,Literal,Cursor) ignore the RowSet parameter.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="rs"></param>
        /// <returns></returns>
        internal virtual TypedValue Eval(Transaction tr,RowSet rs)
        {
            return null;
        }
        internal virtual TypedValue Eval(RowBookmark bmk)
        {
            return Eval(bmk._rs.tr,bmk._rs); // obviously we aim to do better than this
        }
        internal virtual TypedValue Eval(GroupingRowSet grs,ABookmark<TRow,GroupRow> bm)
        {
            return Eval(grs.tr, grs); // obviously we aim to do better than this
        }
        internal object Val(Context tr,RowSet rs=null)
        {
            return Eval(tr, rs)?.NotNull()?.Val(tr);
        }
        internal virtual ATree<long,SqlValue> Disjoin()
        {
            return new BTree<long,SqlValue>(sqid, this);
        }

        internal virtual SqlValue Find(Transaction tr, Ident n)
        {
            if (n == null)
                return this;
            var iq = nominalDataType.names.Get(n,out Ident sub);
            if (iq.HasValue)
            {
                var fn = nominalDataType.names[iq.Value];
                n.Set(tr,fn);
                return new SqlField(tr, nominalDataType[iq.Value], this, fn).Find(tr,sub);
            }
            return null;
        }

        internal void Expected(ref Domain t, Domain exp)
        {
            var nt = t.Constrain(exp) ?? throw new DBException("42161", exp, t).Mix();
            t = nt;
        }
        internal void Constrain(Domain dt)
        {
            nominalDataType = nominalDataType.Constrain(dt);
        }
        internal virtual void RowSet(Transaction tr,From f)
        {
            if (f.Eval() is TRow r)
                f.data = new TrivialRowSet(tr,f, r);
        }
        internal virtual Selector GetColumn(Transaction tr,From q,Table t)
        {
            if (t.dbix != name.Dbix)
                Console.WriteLine("dbix mismatch?");
            return tr.Db(t.dbix).GetObject(name.Defpos()) as Selector;
        }
        internal virtual void Build(RowSet rs)
        {
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them
        /// </summary>
        internal virtual void StartCounter(Transaction tr,RowSet rs)
        {
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them. 
        /// Carefully watch out for common subexpressions, and only AddIn once!
        /// </summary>
        internal virtual void _AddIn(Transaction tr,RowSet rs,ref ATree<long,bool?> aggsDone) { }
        internal void AddIn(Transaction tr,RowSet rs)
        {
            ATree<long, bool?> aggsDone = BTree<long, bool?>.Empty;
            _AddIn(tr, rs, ref aggsDone);
        }
        /// <summary>
        /// Used for Window Function evaluation.
        /// Called by GroupingBookmark (when not building) and SelectedRowBookmark
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(RowBookmark bmk)
        { }
        /// <summary>
        /// If the value is in a grouped construct then every ident reference needs to be aggregated or grouped
        /// </summary>
        /// <param name="group">the group by clause</param>
        internal virtual bool Check(Transaction tr, GroupSpecification group)
        {
            if (group == null)
                return true;
            if (group.Has(name) || group.Has(alias))
                return false;
            for (var b = needed.First(); b != null; b = b.Next())
                if (b.key().Check(tr,group))
                    return true;
            return false;
        }
        internal virtual void Needed(Transaction tr,Query.Need n)
        {
            for (var b = needed.First(); b != null; b = b.Next())
                b.key().Needed(tr,n);
        }
        /// <summary>
        /// analysis stage Sources() and Selects(): setup SqlValue operands
        /// </summary>
        /// <param name="d">The required data type or null</param>
        internal virtual void _Setup(Transaction tr,Query q,Domain d)
        {
        }
        internal static void Setup(Transaction tr,Query q,SqlValue v, Domain d)
        {
            if (v == null)
                return;
            if (v is SqlName sn && sn.resolvedTo is SqlValue sv)
                v = sv;
            d = d.LimitBy(v.nominalDataType);
            v._Setup(tr, q, d);
            v.isSetup = true;
        }
        internal static void Setup(Transaction tr, Query q, ATree<long,SqlValue>t, Domain d)
        {
            for (var b = t.First(); b != null; b = b.Next())
                Setup(tr, q, b.value(), d);
        }
        /// <summary>
        /// analysis stage Conditions(). See if q can fully evaluate this.
        /// If so, evaluation of an enclosing QuerySpec column can be moved down to q.
        /// </summary>
        internal virtual bool Conditions(Transaction tr,Query q,bool disj)
        {
            return false;
        }
        internal virtual SqlValue Simplify(Transaction tr,Query source)
        {
            return this;
        } 
        /// <summary>
        /// test whether the given SqlValue is structurally equivalent to this (always has the same value in this context)
        /// </summary>
        /// <param name="cx">The context - with a list of known equivalences</param>
        /// <param name="v">The expression to test against</param>
        /// <returns>Whether the expressions match</returns>
        internal virtual bool _MatchExpr(Transaction t,SqlValue v)
        {
            return false;
        }
        internal bool MatchExpr(Transaction t,SqlValue v)
        {
            return cx.MatchedPair(this, v) || _MatchExpr(cx, v);
        }
        /// <summary>
        /// analysis stage conditions(): test to see if this predicate can be distributed.
        /// </summary>
        /// <param name="tr">The current transaction (maybe not q.transaction!)</param>
        /// <param name="q">the query to test</param>
        /// <param name="ut">(for RestView) a usingTableType</param>
        /// <returns>true if the whole of thsi is provided by q and/or ut</returns>
        internal virtual bool IsFrom(Transaction tr,Query q, bool ordered, Domain ut=null)
        {
            return false;
        }
        /// <summary>
        /// As above but check against accessiblecolumns instead of selected columns
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        internal virtual bool IsKnown(Transaction tr,Query q)
        {
            for (var b = needed.First(); b != null; b = b.Next())
                if (!b.key().IsKnown(tr, q))
                    return false;
            return true;
        }
        internal virtual SqlValue For(Transaction tr,Query q)
        {
            if (IsKnown(tr, q))
                return this;
            for (var b = q.matchExps[this]?.First(); b != null; b = b.Next())
                if (b.key().IsKnown(tr, q))
                    return b.key();
            return null;
        }
        /// <summary>
        /// We want to call this at top level of UpdateQuerySpec and UpdateOrders, but not in
        /// recursive calls inside SqlValueExpr and SqlFunction.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="cx"></param>
        /// <param name="so"></param>
        /// <param name="sv"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        internal virtual SqlValue _Replace(Transaction tr,Transaction t,SqlValue so,SqlValue sv,ref ATree<long,SqlValue> map)
        {
            if (map[sqid] is SqlValue sm)
                return sm;
            if (MatchExpr(cx, so))
            {
                sv.alias = alias;
                return sv;
            }
            return this;
        }
        /// <summary>
        /// We want to call this within SqlValueExpression and other SqlValue subclasses, to fix
        /// subexpression references. SQL does not allow subexpressions to have aliases.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="cx"></param>
        /// <param name="so"></param>
        /// <param name="sv"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        internal SqlValue Replace(Transaction tr,Transaction t,SqlValue so,SqlValue sv,ref ATree<long,SqlValue> map)
        {
            var r = (cx.defs[alias] ?? this)._Replace(tr, cx, so, sv, ref map);
            if (r != this)
                ATree<long, SqlValue>.Add(ref map, sqid, r);
            return r;
        }
        internal virtual int? _PosIn(Transaction tr, Query q, bool ordered,out Ident sub)
        {
            sub = null;
            return null;
        }
        internal int? PosIn(Transaction tr,Query q,bool ordered,out Ident sub)
        {
            var r = _PosIn(tr, q, ordered, out sub);
            if (!(q is JoinPart))
            for (var b = q.matchExps[this]?.First();r==null && b!=null;b=b.Next())
                r = b.key()._PosIn(tr, q, ordered, out sub);
            return r;
        }
        internal virtual bool isConstant
        {
            get { return false; }
        }
        internal virtual bool aggregates()
        {
            return false;
        }
        /// <summary>
        /// Analysis stage Conditions: update join conditions
        /// </summary>
        /// <param name="tr">The connection</param>
        /// <param name="j">The joinpart</param>
        /// <param name="joinCond">Check this only contains simple left op right comparisons</param>
        /// <param name="where">See if where contains any suitable joincoditions and move them if so</param>
        internal virtual bool JoinCondition(Transaction tr,JoinPart j, ref ATree<long,SqlValue> joinCond,ref ATree<long,SqlValue> where)
        {
            var r = false;
            if (For(tr, j.left) is SqlValue v)
            {
                ATree<long, SqlValue>.Add(ref j.left.where, sqid, v);
                r = true;
            }
            if (For(tr, j.right) is SqlValue w)
            {
                ATree<long, SqlValue>.Add(ref j.right.where, sqid, w);
                r = true;
            }
            return r;
        }
        /// <summary>
        /// Analysis stage Conditions: Distribute conditions to joins, froms
        /// </summary>
        /// <param name="q"> Query</param>
        /// <param name="repl">Updated list of equality conditions for possible replacements</param>
        /// <param name="needed">Updated list of fields mentioned in conditions</param>
        internal virtual void DistributeConditions(Transaction tr,Query q, Ident.Tree<SqlValue> repl, RowSet data)
        {
        }
        internal virtual SqlValue PartsIn(Domain dt)
        {
            return this;
        }
        internal virtual void BuildRepl(Transaction tr,Query lf,ref Ident.Tree<SqlValue> lr,ref Ident.Tree<SqlValue> rr)
        {
        }
        /// <summary>
        /// Analysis stage Conditions: Collect equality comparisons in AND wheres
        /// </summary>
        /// <param name="al">List of Equality comparisonpredicates</param>
        /// <returns>Whether all top-level conditions are ANDs</returns>
        internal virtual bool WhereEquals(List<SqlValueExpr> al)
        {
            return true;
        }
        internal bool Matches(Transaction tr,RowSet rs)
        {
            return (Eval(tr,rs) is TypedValue tv) ? tv == TBool.True : true;
        }
        internal virtual bool HasAnd(SqlValue s)
        {
            return s == this;
        }
        internal virtual Target LVal(Transaction tr)
        {
            if (name.sub != null)
            {
 /*               if (cx is Query q && q.staticLink.contexts[name.ident] is Activation a)
                    return new Target(a, name.sub, nominalDataType);
                else */
                    throw new DBException("42112", name);
            }
            return new Target(tr, name, nominalDataType);
        }
        internal virtual SqlValue Invert()
        {
            return new SqlValueExpr(tr, Sqlx.NOT, this, null, Sqlx.NO);
        }
        internal virtual SqlValue Operand()
        {
            return null;
        }
        internal static bool OpCompare(Sqlx op, int c)
        {
            switch (op)
            {
                case Sqlx.EQL: return c == 0;
                case Sqlx.NEQ: return c != 0;
                case Sqlx.GTR: return c > 0;
                case Sqlx.LSS: return c < 0;
                case Sqlx.GEQ: return c >= 0;
                case Sqlx.LEQ: return c <= 0;
            }
            throw new PEException("PE61");
        }
        /// <summary>
        /// Get the position of a given expression, allowing for adapter functions
        /// </summary>
        /// <param name="sv">the expression</param>
        /// <returns>the position in the query</returns>
        internal virtual int? ColFor(RowSet rs)
        {
            return rs.qry.nominalDataType.names[name];
        }
        internal virtual Domain FindType(Domain dt)
        {
            var vt = nominalDataType;
            if (vt == null || vt.kind==Sqlx.CONTENT)
                return dt;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22005U", dt.kind, vt.kind);
            if ((isConstant && vt.kind == Sqlx.INTEGER) || vt.kind==Sqlx.Null)
                return dt; // keep union options open
            return vt;
        }
        internal static Domain RowType(Transaction t, SqlValue v)
        {
            var dt = v.nominalDataType;
            if (dt.columns != null)
                return dt;
            return new TableType(cx, new SqlValue[] {v});
        }
        internal Ident NameForRowType()
        {
            return (alias ?? name)?.ForTableType();
        }
        public virtual int CompareTo(object obj)
        {
            return (obj is SqlValue that)?sqid.CompareTo(that.sqid):1;
        }
        public override string ToString()
        {
            return (alias??name).ToString();
        }
        /// <summary>
        /// Produce a readable view of the SqlValue (for RESTViews)
        /// </summary>
        /// <param name="sb">A string builder</param>
        /// <param name="c">The connection</param>
        /// <param name="uf">The using table</param>
        /// <param name="ur">The current using record</param>
        /// <param name="eflag">Whether to use extended names</param>
        /// <param name="cms">whether to generate separators (eg we are recursively generating a comma-separated list )</param>
        /// <param name="cm">the current separator</param>
        public virtual void ToString1(StringBuilder sb,Transaction c,From uf,Record ur,string eflag,bool cms,ref string cm)
        {
            if (cms)
                sb.Append(cm); 
            name.ToString1(sb, c, eflag);
            cm = Sep(cm);
        }
        internal string Sep(string s)
        {
            switch (s)
            {
                case " having ":
                case " where ": return " and ";
                case "":
                case "[":
                case "{":
                case "(":
                case " ":
                case " [":
                case " {":
                case " (":
                    return ",";
                default: return s;
            }
        }
        public virtual string ToString1(Transaction t,Record ur)
        {
            var s = ToString();
            return (cx.context.cur as Query)?.replace[s] ?? s;
        }
        /// <summary>
        /// Compute relevant equality pairings.
        /// Currently this is only for EQL joinConditions
        /// </summary>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual void Eqs(Transaction tr,ref Adapters eqs)
        {
        }
        internal virtual int? FillHere(Ident n)
        {
            return null;
        }
        internal virtual int Ands()
        {
            return 1;
        }
        /// <summary>
        /// RestView.Selects: Analyse subexpressions qs selectlist and add them to gfreqs
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="gf"></param>
        /// <param name="gfreqs"></param>
        /// <param name="i"></param>
        internal virtual void _AddReqs(Transaction tr,From gf,Domain ut, ref ATree<SqlValue,int> gfreqs,int i)
        {
            if (IsFrom(tr,gf,false,ut))
                ATree<SqlValue, int>.Add(ref gfreqs, this, i);
        }
        internal void AddReqs(Transaction tr, From gf, Domain ut, ref ATree<SqlValue, int> gfreqs, int i)
        {
            for (var b = gfreqs.First(); b != null; b = b.Next())
                if (MatchExpr(gf, b.key()))
                    return;
            _AddReqs(tr, gf, ut, ref gfreqs, i);
        }
        /// <summary>
        /// See SqlValue::ColsForRestView.
        /// This stage is RESTView.Selects.
        /// Default behaviour here works for SqlLiterals and for columns and simple expressions that
        /// can simply be added to the remote query. Note that where-conditions will
        /// be further modified in RestView:Conditions.
        /// </summary>
        /// <param name="gf">the GlobalFrom query to transform</param>
        /// <param name="gs">the top-level group specification if any</param>
        /// <param name="gfc">the proposed columns for the global from</param>
        /// <param name="rem">the proposed columns for the remote query</param>
        /// <param name="reg">the proposed groups for the remote query</param>
        /// <returns>the column to use in the global QuerySpecification</returns>
        internal virtual SqlValue _ColsForRestView(Transaction tr, From gf, GroupSpecification gs,  
            ref ATree<SqlValue,Ident> gfc, ref ATree<SqlValue,Ident> rem, ref ATree<Ident,bool?> reg,
            ref ATree<long,SqlValue> map)
        {
            var an = Alias();
            gfc = new BTree<SqlValue,Ident>(this,an);
            rem = gfc;
            return this;
        }
        /// <summary>
        /// Modify requested columns for RESTView/using feature.See notes on SqlHttpUsing class.
        /// During Context, the globalFrom gf has been set up so that gf.source is a remote CursorSpecification
        /// probably with a usingFrom, and upward and downward referenes to other RESTView QuerySpecifications.
        /// The recursion within a particular RESTView definition is handled by _ColsForRestView.
        /// A nested RESTView definition may recursively call ColsForRestView (see RestView::Selects).
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="gf"></param>
        /// <param name="gs"></param>
        /// <returns>the column to use in the global QuerySpecification</returns>
        internal SqlValue ColsForRestView(Transaction tr, From gf,GroupSpecification gs, ref ATree<long,SqlValue> map)
        {
            if (map[sqid] is SqlValue sr)
                return sr;
            var reg = BTree<Ident, bool?>.Empty;
            var gfc = BTree<SqlValue, Ident>.Empty;
            var rem = BTree<SqlValue, Ident>.Empty;
            SqlValue sv = this;
            var an = alias ?? name;
            if (gs?.Has(an)==true)
            {
                var gfc0 = BTree<SqlValue, Ident>.Empty;
                var reg0 = BTree<Ident, bool?>.Empty;
                sv = _ColsForRestView(tr, gf, gs, ref gfc0, ref rem, ref reg0, ref map);
                ATree<SqlValue, Ident>.Add(ref gfc, new SqlTypeColumn(tr,gf,nominalDataType,an,false,false), an);
                ATree<Ident, bool?>.Add(ref reg, an, true);
            }                
            else
            {
                sv = _ColsForRestView(tr, gf, gs, ref gfc, ref rem, ref reg, ref map);
                an = sv.alias ?? sv.name;
            }
            for (var b = gfc.First(); b != null; b = b.Next())
            {
                var su = b.key();
                gf.MaybeAdd(tr, ref su);
            }
            if ((!isConstant) || gf.QuerySpec(tr)?.tableExp.group?.Has(an) == true)
                for (var b=rem.First();b!=null;b=b.Next())
                    gf.source.Add(tr, b.key(), b.value());
            var cs = gf.source as CursorSpecification;
            for (var b = reg.First(); b != null; b = b.Next())
                if (cs.restGroups.Get(b.key(), out Ident sb)==null)
                    cs.restGroups.Add(b.key());
            return sv;
        }
        internal Ident Alias()
        {
            return alias ?? name ?? new Ident("C_" + sqid, 0);
        }
        internal bool IsNeeded(Transaction tr,Query q)
        {
            var qs = q.QuerySpec(tr);
            // check this is being grouped
            bool gp = false;
            bool nogroups = true;
            if (qs?.tableExp?.group is GroupSpecification g)
                foreach (var gs in g.sets)
                {
                    gs.Grouped(this, ref gp);
                    nogroups = false;
                }
            return gp || nogroups;
        }

        internal virtual bool Grouped(GroupSpecification gs)
        {
            return gs?.Has(alias ?? name)==true;
        }
        /// <summary>
        /// If this value contains an aggregator, set the regiser for it.
        /// If not, return null and the caller will make a Literal.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal virtual SqlValue SetReg(TRow key)
        {
            return null;
        }
    }
    /// <summary>
    /// A TYPE value for use in CAST
    /// </summary>
    internal class SqlTypeExpr : SqlValue
    {
        internal Domain type;
        /// <summary>
        /// constructor: a new Type expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTypeExpr(Domain ty, Transaction t)
            : base(cx,Domain.TypeSpec)
        {
            type = ty;
        }
        /// <summary>
        /// Lookup the type name in the context
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            return new TTypeSpec(type);
        }
        public override string ToString()
        {
            return "TYPE(..)";
        }
    }
    /// <summary>
    /// A Subtype value for use in TREAT
    /// </summary>
    internal class SqlTreatExpr : SqlValue
    {
        SqlValue val;
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(SqlValue v,Domain ty, Transaction t)
            : base(cx,(ty.kind==Sqlx.ONLY && ty.iri!=null)?v.nominalDataType.Copy(ty.iri):ty)
        {
            val = v;
            Needs(val);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,val,nominalDataType);
        }
        internal override bool Check(Transaction tr, RowSet rs,ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            return val.Check(tr, rs,cacheWhere, ref newcache);
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (val.Eval(tr,rs)?.NotNull() is TypedValue tv)
            {
                if (!nominalDataType.HasValue(tr, tv))
                    throw new DBException("2200G", nominalDataType.ToString(), val.ToString()).ISO();
                return tv;
            }
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            if (val.Eval(bmk)?.NotNull() is TypedValue tv)
            {
                if (!nominalDataType.HasValue(tr, tv))
                    throw new DBException("2200G", nominalDataType.ToString(), val.ToString()).ISO();
                return tv;
            }
            return null;
        }
        public override string ToString()
        {
            return "TREAT(..)";
        }
    }
    internal class SqlField : SqlValue
    {
        SqlValue left;
        Ident sub;
        internal SqlField(Transaction tr,Domain dt, SqlValue lf,Ident s) : base(tr,dt,new Ident(lf.name,s))
        {
            left = lf;
            sub = s;
        }
        internal override TypedValue Eval(Context tr, RowSet rs)
        {
            return left.Eval(tr,rs)[sub];
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return left.Eval(bmk)[sub];
        }
    }
    /// <summary>
    /// A SqlValue expression structure.
    /// Various additional operators have been added for JavaScript: e.g.
    /// modifiers BINARY for AND, OR, NOT; EXCEPT for (binary) XOR
    /// ASC and DESC for ++ and -- , with modifier BEFORE
    /// QMARK and COLON for ? :
    /// UPPER and LOWER for shifts (GTR is a modifier for the unsigned right shift)
    /// </summary>
    internal class SqlValueExpr : SqlValue
    {
        /// <summary>
        /// the left SqlValue operand
        /// </summary>
        public SqlValue left = null;
        /// <summary>
        /// the right SqlValue operand
        /// </summary>
        public SqlValue right = null;
        /// <summary>
        /// the modifier (e.g. DISTINCT)
        /// </summary>
        public Sqlx mod;
        /// <summary>
        /// used for JavaScript assignment operators
        /// </summary>
        public Target assign = null;
        /// <summary>
        /// constructor for an SqlValueExpr
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">an operator</param>
        /// <param name="lf">the left operand</param>
        /// <param name="rg">the right operand</param>
        /// <param name="m">a modifier (e.g. DISTINCT)</param>
        public SqlValueExpr(Transaction t, Sqlx op, SqlValue lf, SqlValue rg, Sqlx m)
            : base(cx, _Type(cx, op, m, lf, rg))
        {
            left = lf; right = rg; mod = m; kind = op;
            etag = ETag.Add(lf?.etag, rg?.etag);
            Needs(left, right);
        }
        protected SqlValueExpr(SqlValueExpr v, ref ATree<long, SqlValue> vs) : base(v, ref vs)
        {
            left = v.left?.Copy(ref vs);
            right = v.right?.Copy(ref vs);
            mod = v.mod;
            assign = v.assign;
        }
        void _Type(SqlValue sv)
        {
            if (sv != null && sv.nominalDataType != null)
                nominalDataType = sv.nominalDataType;
        }
        internal override ATree<long, SqlValue> Disjoin()
        {
            if (kind == Sqlx.AND)
            {
                // parsing guarantees right associativity
                var r = right.Disjoin();
                ATree<long, SqlValue>.Add(ref r, left.sqid, left);
                return r;
            }
            return base.Disjoin();
        }
        /// <summary>
        /// analysis stage Selects(): setup the operands
        /// </summary>
        /// <param name="q">The required data type</param>
        /// <param name="s">a default value</param>
        internal override void _Setup(Transaction tr, Query q, Domain d)
        {
            assign?.Setup(tr);
            switch (kind)
            {
                case Sqlx.AND: CheckType(d, Domain.Bool);
                    Setup(tr, q, left, d);
                    Setup(tr, q, right, d);
                    break;
                case Sqlx.ASC: CheckType(d, Domain.UnionNumeric);
                    Setup(tr, q, left, d); break;// JavaScript;
                case Sqlx.ASSIGNMENT:
                    Setup(tr, q, left, Domain.Content);
                    Setup(tr, q, right, left.nominalDataType);
                    _Type(right);
                    break;
                case Sqlx.BOOLEAN: CheckType(d, Domain.Bool);
                    Setup(tr, q, left, Domain.Bool); break;
                case Sqlx.CALL: CheckType(Domain.JavaScript, Domain.ArgList);
                    Setup(tr, q, left, Domain.JavaScript);
                    Setup(tr, q, right, Domain.ArgList);
                    break;
                case Sqlx.COLLATE: CheckType(d, Domain.Char);
                    Setup(tr, q, left, Domain.Char);
                    Setup(tr, q, right, Domain.Char);
                    break;
                case Sqlx.COLON: // JavaScript
                    Setup(tr, q, left, Domain.Content);
                    Setup(tr, q, right, left.nominalDataType);
                    _Type(left);
                    break;
                case Sqlx.COMMA: // JavaScript
                    Setup(tr, q, left, Domain.Char);
                    Setup(tr, q, right, Domain.Content);
                    break;
                case Sqlx.CONCATENATE: goto case Sqlx.COLLATE;
                case Sqlx.CONTAINS:
                    CheckType(d, Domain.Bool);
                    Setup(tr, q, left, Domain.Period);
                    Setup(tr, q, right, Domain.Content); // Can be UnionDate or left.nominalDataType
                    break;
                case Sqlx.DESC: goto case Sqlx.ASC;
                case Sqlx.DIVIDE: goto case Sqlx.TIMES;
                case Sqlx.DOT:
                    Setup(tr, q, left, Domain.Content);
                    if (right is SqlName n) // probably already is a SqlTypeColumn
                    {
                        var t = n.LVal(tr);
                        if (t == null)
                        {
                            if (left.nominalDataType.kind == Sqlx.DOCUMENT)
                                right = new SqlTypeColumn(tr, Domain.Content, n.name, true, false,q);
                            else
                                throw new DBException("42112", n.name.ident).Mix();
                        }
                        else
                        {
                            n.name.Set(tr,t.name);
                            Setup(t, d[n.name]);
                            nominalDataType = t.dataType;
                        }
                    }
                    break;
                case Sqlx.EQL: CheckType(d, Domain.Bool);
                    Setup(tr, q, left, Domain.Content);
                    Setup(tr, q, right, left.nominalDataType);
                    break;
                case Sqlx.EQUALS: goto case Sqlx.OVERLAPS;
                case Sqlx.EXCEPT: goto case Sqlx.UNION;
                case Sqlx.GEQ: goto case Sqlx.EQL;
                case Sqlx.GTR: goto case Sqlx.EQL;
                case Sqlx.INTERSECT: goto case Sqlx.UNION;
                case Sqlx.LBRACK:
                    Setup(tr, q, left, Domain.Array);
                    CheckType(d, left.nominalDataType.elType);
                    nominalDataType = left.nominalDataType.elType;
                    Setup(tr, q, right, Domain.Int);
                    break;
                case Sqlx.LEQ: goto case Sqlx.EQL;
                case Sqlx.LOWER: goto case Sqlx.UPPER; //JavaScript >>
                case Sqlx.LSS: goto case Sqlx.EQL;
                case Sqlx.MINUS:
                    if (left == null)
                    {
                        Setup(tr, q, right, d);
                        nominalDataType = right.nominalDataType;
                        break;
                    }
                    goto case Sqlx.PLUS;
                case Sqlx.NEQ: goto case Sqlx.EQL;
                case Sqlx.NO:
                    Setup(tr, q, left, d);
                    _Type(left);
                    break;
                case Sqlx.NOT: CheckType(d, Domain.Bool);
                    Setup(tr, q, left, d); break;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.OVERLAPS: CheckType(d, Domain.Bool);
                    Setup(tr, q, left, Domain.UnionDate);
                    Setup(tr, q, right, left.nominalDataType);
                    break;
                case Sqlx.PERIOD:
                    if (d == Domain.Content || d==Domain.Value)
                        d = Domain.UnionDate;
                    Setup(tr, q, left, d);
                    Setup(tr, q, right, left.nominalDataType);
                    break;
                case Sqlx.PLUS:
                    {
                        Domain nt = FindType(Domain.UnionDateNumeric);
                        if (nt.kind == Sqlx.INTERVAL)
                        {
                            Setup(tr, q, left, left.nominalDataType);
                            Setup(tr, q, right, right.nominalDataType);
                        }
                        else if (nt.kind == Sqlx.DATE || nt.kind == Sqlx.TIME || nt.kind == Sqlx.TIMESTAMP)
                        {
                            Setup(tr, q, left, left.nominalDataType);
                            Setup(tr, q, right, Domain.Interval);
                        }
                        else
                        {
                            CheckType(d, nt);
                            Setup(tr, q, left, nt);
                            Setup(tr, q, right, nt);
                            nominalDataType = nt;
                        }
                    }
                    break;
                case Sqlx.PRECEDES: goto case Sqlx.OVERLAPS;
                case Sqlx.QMARK: // JavaScript
                    Setup(tr, q, left, Domain.Bool);
                    Setup(tr, q, right, Domain.Content);
                    _Type(right);
                    break;
                case Sqlx.RBRACK:
                    Setup(tr, q, left, new Domain(Sqlx.ARRAY, d));
                    Setup(tr, q, right, Domain.Int);
                    break;
                case Sqlx.SET:
                    _Type(left);
                    break;
                case Sqlx.SUCCEEDS: goto case Sqlx.OVERLAPS;
                case Sqlx.TIMES:
                    nominalDataType = Domain.UnionNumeric;
                    CheckType(d, Domain.UnionNumeric);
                    Setup(tr, q, left, Domain.UnionNumeric);
                    Setup(tr, q, right, Domain.UnionNumeric);
                    break;
                case Sqlx.UNION:
                    Setup(tr, q, left, Domain.Collection);
                    Setup(tr, q, right, left.nominalDataType);
                    _Type(left);
                    break;
                case Sqlx.UPPER: CheckType(Domain.Int, Domain.Int); // JvaScript shift <<
                    Setup(tr, q, left, Domain.Int);
                    Setup(tr, q, right, Domain.Int);
                    break;
                case Sqlx.XMLATTRIBUTES: goto case Sqlx.COLLATE;
                case Sqlx.XMLCONCAT: goto case Sqlx.COLLATE;
                default:
                    throw new DBException("22005V", d.ToString(), (q.nominalDataType??Domain.Content).ToString()).ISO()
                        .AddType(d);
            }
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlValueExpr(this, ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            if (left.Import(tr, q) is SqlValue a)
            {
                var b = right?.Import(tr, q);
                if (left == a && right == b)
                    return this;
                if (right == null || b != null)
                    return new SqlValueExpr(tr, kind, a, b, mod) { alias = alias };
            }
            return null;
        }
        internal override SqlValue Export(Transaction tr, Query q)
        {
            if (alias != null)
                return new SqlTypeColumn(tr, nominalDataType, alias, false, false,q);
            if (left.Export(tr, q) is SqlValue a)
            {
                var b = right?.Export(tr, q);
                if (left == a && right == b)
                    return this;
                if (right == null || b != null)
                    return new SqlValueExpr(tr, kind, a, b, mod) { alias = alias };
            }
            return null;
        }
        void Setup(Target t, Domain d)
        {
            if (d != null && d != Domain.Content && !d.CanTakeValueOf(t.dataType))
                throw new DBException("22005W", d, t.dataType).ISO()
                    .AddType(d).AddValue(t.dataType);
            if (d.kind != Sqlx.Null && d.kind != Sqlx.CONTENT &&
                d.kind != Sqlx.UNION && d != Domain.ArgList && t.dataType.EqualOrStrongSubtypeOf(d))
                t.dataType = d;
        }
        /// <summary>
        /// Examine a binary expression and work out the resulting type.
        /// The main complication here is handling things like x+1
        /// (e.g. confusion between NUMERIC and INTEGER)
        /// </summary>
        /// <param name="dt">Target union type</param>
        /// <returns>Actual type</returns>
        internal override Domain FindType(Domain dt)
        {
            if (left == null)
                return right.FindType(dt);
            Domain tl = left.FindType(dt);
            if (kind == Sqlx.DOT && right is SqlName n)
                return (tl.kind == Sqlx.UNION || tl.columns == null) ? right.FindType(dt) : tl[n.name];
            Domain tr = (right == null) ? dt : right.FindType(dt);
            switch (tl.kind)
            {
                case Sqlx.PERIOD: return Domain.Period;
                case Sqlx.CHAR: return tl;
                case Sqlx.NCHAR: return tl;
                case Sqlx.DATE: if (kind == Sqlx.MINUS)
                        return Domain.Interval;
                    return tl;
                case Sqlx.INTERVAL: return tl;
                case Sqlx.TIMESTAMP:
                    if (kind == Sqlx.MINUS)
                        return Domain.Interval;
                    return tl;
                case Sqlx.INTEGER: return tr;
                case Sqlx.NUMERIC: if (tr.kind == Sqlx.REAL) return tr;
                    return tl;
                case Sqlx.REAL: return tl;
                case Sqlx.LBRACK:
                    return tl.elType;
                case Sqlx.UNION:
                    return tl;
            }
            return tr;
        }
        internal override bool HasAnd(SqlValue s)
        {
            if (s == this)
                return true;
            if (kind != Sqlx.AND)
                return false;
            return (left != null && left.HasAnd(s)) || (right != null && right.HasAnd(s));
        }
        internal override int Ands()
        {
            if (kind == Sqlx.AND)
                return left.Ands() + right.Ands();
            return base.Ands();
        }
        static void CheckType(Domain left, Domain right)
        {
            if (left != null && !left.CanTakeValueOf(right))
                throw new DBException("22005X", left.ToString(), right.ToString()).ISO();
        }
        internal override bool isConstant
        {
            get
            {
                return left.isConstant && (right == null || right.isConstant);
            }
        }
        internal override bool aggregates()
        {
            return (left?.aggregates() == true) || (right?.aggregates() == true);
        }
        internal override void _AddReqs(Transaction tr, From gf, Domain ut, ref ATree<SqlValue, int> gfreqs, int i)
        {
            left.AddReqs(tr, gf, ut, ref gfreqs, i);
            right?.AddReqs(tr, gf, ut, ref gfreqs, i);

        }
        const int ea = 1, eg = 2, la = 4, lr = 8, lg = 16, ra = 32, rr = 64, rg = 128;
        internal override SqlValue _ColsForRestView(Transaction tr, From gf, GroupSpecification gs, ref ATree<SqlValue,Ident> gfc,ref ATree<SqlValue,Ident>rem, ref ATree<Ident,bool?> reg, ref ATree<long,SqlValue> map)
        {
            var rgl = BTree<Ident, bool?>.Empty;
            var gfl = BTree<SqlValue, Ident>.Empty;
            var rel = BTree<SqlValue, Ident>.Empty;
            var rgr = BTree<Ident, bool?>.Empty;
            var gfr = BTree<SqlValue, Ident>.Empty;
            var rer = BTree<SqlValue, Ident>.Empty;
            // we distinguish many cases here using the above constants: exp/left/right:agg/grouped/remote
            int cse = 0, csa = 0;
            SqlValue el = left, er = right;
            if (gf.QuerySpec(tr).aggregates())
                cse += ea;
            if (gs?.Has(alias ?? name)==true)
                cse += eg;
            if (left.aggregates())
                cse += la;
            if (right?.aggregates() == true)
                cse += ra;
            var cs = gf.source as CursorSpecification;
            if (left.IsKnown(tr, gf) && (!left.isConstant))
            {
                cse += lr;
                el = left._ColsForRestView(tr, gf, gs, ref gfl, ref rel, ref rgl,ref map);
            }
            if (right?.IsKnown(tr, gf)==true && (!right.isConstant))
            {
                cse += rr;
                er = right._ColsForRestView(tr, gf, gs, ref gfr, ref rer, ref rgr, ref map);
            }
            if (left.Grouped(gs))
                cse += lg;
            if (right?.Grouped(gs)==true)
                cse += rg;
            // I know we could save on the declaration of csa here
            // But this case numbering follows documentation
            switch (cse)
            {
                case ea + lr + rr: 
                case lr + rr: csa = 1; break;
                case ea + lr:
                case lr: csa = 2; break;
                case ea + rr:
                case rr: csa = 3; break;
                case ea + la + lr + ra + rr: csa = 4; break;
                case ea + eg + lr + rr: csa = 5; break;
                case ea + eg + lr: csa = 6; break;
                case ea + eg + rr: csa = 7; break;
                case ea + eg + la + lr + ra + rr: csa = 8; break;
                case ea + la + lr + rr + rg: csa = 9; break;
                case ea + lr + lg + ra + rr: csa = 10; break;
                case ea + la + lr + rg: csa = 11; break;
                case ea + ra + rr + lg: csa = 12; break;
                default:
                    {   // if none of the above apply, we can't rewrite this expression
                        // so simply ensure we can compute it
                        for (var b = needed.First(); b != null; b = b.Next())
                        {
                            var sv = b.key();
                            var id = sv.alias ?? sv.name ?? new Ident("C_"+sv.sqid,0);
                            ATree<SqlValue, Ident>.Add(ref gfc, sv, id);
                            ATree<SqlValue, Ident>.Add(ref rem, sv, id);
                            if (aggregates())
                                ATree<Ident, bool?>.Add(ref reg, id, true);
                        }
                        return base._ColsForRestView(tr, gf, gs, ref gfc, ref rem, ref reg, ref map);
                    }
            }
            gfc = BTree<SqlValue, Ident>.Empty;
            rem = BTree<SqlValue, Ident>.Empty;
            reg = BTree<Ident, bool?>.Empty;
            SqlValueExpr se = this;
            SqlValue st = null;
            var nn = Alias();
            var nl = el?.Alias();
            var nr = er?.Alias();
            switch (csa)
            {
                case 1: // lr rr : QS->Cexp as exp ; CS->Left’ op right’ as Cexp
                    // rel and rer will have just one entry each
                    st = new SqlTypeColumn(tr, nominalDataType, nn, false, false, gf)
                    { alias = nn };
                    ATree<SqlValue, Ident>.Add(ref rem, 
                        new SqlValueExpr(tr, kind, rel.First().key(), rer.First().key(), mod)
                        { alias = nn }, nn);
                    ATree<SqlValue, Ident>.Add(ref gfc, st, nn);
                    ATree<long, SqlValue>.Add(ref map, sqid, st);
                    return st;
                case 2: // lr: QS->Cleft op right as exp; CS->Left’ as Cleft 
                    // rel will have just one entry, rer will have 0 entries
                    se = new SqlValueExpr(tr, kind,
                        new SqlTypeColumn(tr, left.nominalDataType, nl, false, false, gf), right, mod)
                    { alias = alias };
                    ATree<SqlValue,Ident>.Add(ref rem, rel.First().key(), nl);
                    ATree<SqlValue,Ident>.Add(ref gfc, gfl.First().key(), nl);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    return se;
                case 3:// rr: QS->Left op Cright as exp; CS->Right’ as CRight
                    // rer will have just one entry, rel will have 0 entries
                    se = new SqlValueExpr(tr, kind, left,
                        new SqlTypeColumn(tr, right.nominalDataType, er.alias ?? er.name, false, false, gf), mod)
                    { alias = alias };
                    ATree<SqlValue, Ident>.Add(ref rem, rer.First().key(), nr);
                    ATree<SqlValue, Ident>.Add(ref gfc, gfr.First().key(), nr);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    return se;
                case 4: // ea lr rr: QS->SCleft op SCright; CS->Left’ as Cleft,right’ as Cright
                    // gfl, gfr, rgl and rgr may have sevral entries: we need all of them
                    se = new SqlValueExpr(tr, kind, el, er, mod)
                    { alias = nn };
                    CopyFrom(ref gfc, gfl); CopyFrom(ref gfc, gfr);
                    CopyFrom(ref rem, rel); CopyFrom(ref rem, rer);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    return se;
                case 5: // ea eg lr rr: QS->Cexp as exp  group by exp; CS->Left’ op right’ as Cexp group by Cexp
                    // rel and rer will have just one entry each
                    ATree<Ident, bool?>.Add(ref reg, nn, true);
                    goto case 1;
                case 6: // ea eg lr: QS->Cleft op right as exp group by exp; CS-> Left’ as Cleft group by Cleft
                    CopyFrom(ref reg, rel);
                    goto case 2;
                case 7: // ea eg rr: QS->Left op Cright as exp group by exp; CS->Right’ as Cright group by Cright
                    CopyFrom(ref reg, rer);
                    goto case 3;
                case 8: // ea eg la lr ra rr: QS->SCleft op SCright as exp group by exp; CS->Left’ as Cleft,right’ as Cright group by Cleft,Cright
                    GroupOperands(ref reg, rel);
                    GroupOperands(ref reg, rer);
                    goto case 4;
                case 9: // ea la lr rr rg: QS->SCleft op Cright as exp group by right; CS->Left’ as Cleft,right’ as Cright group by Cright
                    se = new SqlValueExpr(tr, kind, el,er, mod)
                    { alias = alias };
                    CopyFrom(ref gfc, gfl); CopyFrom(ref rem, rel);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    return se;
                case 10: // ea lr lg rg: QS->Left op SCright as exp group by left; CS->Left’ as Cleft,right’ as Cright group by Cleft
                    se = new SqlValueExpr(tr, kind,el,er, mod)
                    { alias = alias };
                    CopyFrom(ref gfc, gfr); CopyFrom(ref rem, rer);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    return se;
                case 11: // ea la lr rg: QS->SCleft op right as exp group by right; CS->Left’ as Cleft
                    se = new SqlValueExpr(tr, kind, el, right, mod) { alias = alias };
                    CopyFrom(ref gfc, gfl); CopyFrom(ref rem, rel);
                    ATree<long, SqlValue>.Add(ref map, sqid, se);
                    break;
                case 12: // ea lg ra: QS->Left op SCright as exp group by left; CS->Right’ as Cright
                    se = new SqlValueExpr(tr, kind, left, er, mod) { alias = alias };
                    CopyFrom(ref gfc, gfr); CopyFrom(ref rem, rer);
                    break;
            }
            se = new SqlValueExpr(tr, kind, el, er, mod) { alias = nn };
                if (gs.Has(nn))// what we want if grouped
                    st = new SqlTypeColumn(tr, se.nominalDataType, nn, false, false, gf); 
            if (gs.Has(nn))
            {
                ATree<SqlValue, Ident>.Add(ref rem, se, se.alias);
                ATree<SqlValue, Ident>.Add(ref gfc, se, alias);
            }
            else
            {
                if (!el.isConstant)
                    ATree<SqlValue, Ident>.Add(ref gfc, el, nl);
                if (!er.isConstant)
                    ATree<SqlValue, Ident>.Add(ref gfc, er, nr);
            }
            ATree<long, SqlValue>.Add(ref map, sqid, se);
            return se;
        }
        void CopyFrom(ref ATree<SqlValue, Ident> dst, ATree<SqlValue, Ident> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
                ATree<SqlValue, Ident>.Add(ref dst, b.key(), b.value());
        }
        void CopyFrom(ref ATree<Ident,bool?> dst, ATree<SqlValue, Ident> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
            {
                var sv = b.key();
                ATree<Ident, bool?>.Add(ref dst, sv.alias??sv.name, true);
            }
        }
        void GroupOperands(ref ATree<Ident,bool?> dst,ATree<SqlValue,Ident> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
                if (b.key().Operand() is SqlValue sv)
                    ATree<Ident, bool?>.Add(ref dst, sv.alias??sv.name, true);
        }
        internal override void Build(RowSet rs)
        {
            left?.Build(rs);
            right?.Build(rs);
        }
        internal override void StartCounter(Transaction tr,RowSet rs)
        {
            left?.StartCounter(tr, rs);
            right?.StartCounter(tr,rs);
        }
        internal override void _AddIn(Transaction tr,RowSet rs,ref ATree<long,bool?> aggsDone)
        {
            left?._AddIn(tr,rs,ref aggsDone);
            right?._AddIn(tr,rs,ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            left?.SetReg(key);
            right?.SetReg(key);
            return this;
        }
        internal override void OnRow(RowBookmark bmk)
        {
            left?.OnRow(bmk);
            right?.OnRow(bmk);
        }
        /// <summary>
        /// helper for Query optimisation machinery. 
        /// This is never called with kind==AND
        /// </summary>
        /// <param name="q">The query</param>
        internal override bool IsFrom(Transaction tr,Query q,bool ordered,Domain ut=null)
        {
            var r = left.IsFrom(tr,q,ordered,ut);
            if (right == null || !r)
                return r;
            return right.IsFrom(tr, q,ordered,ut);
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            var r = left.IsKnown(tr, q);
            if (right == null || !r)
                return r;
            return right.IsKnown(tr, q);
        }
        internal override SqlValue For(Transaction tr, Query q)
        {
            if (left.For(tr, q) is SqlValue lf)
            {
                if (right == null)
                    return (lf.sqid == left.sqid) ? this : new SqlValueExpr(tr, kind, lf, null, mod);
                if (right.For(tr, q) is SqlValue rg)
                    return (lf.sqid == left.sqid && rg.sqid == right.sqid) ? this : new SqlValueExpr(tr, kind, lf, rg, mod);
            }
            return null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long,SqlValue> map)
        {
            var b = base._Replace(tr, cx, so, sv, ref map);
            if (b != this)
                return b;
            var nl = left.Replace(tr, cx, so, sv,ref map);
            var nr = right?.Replace(tr, cx, so, sv,ref map);
            if (nl != left || nr != right)
            {
                var r = new SqlValueExpr(tr, kind, nl, nr, mod) { alias = alias };
                ATree<long, SqlValue>.Add(ref map, sqid, r);
                return r;
            }
            return this;
        }
        /// <summary>
        /// Analysis stage Conditions: set up join conditions.
        /// Code for altering ordSpec has been moved to Analysis stage Orders.
        /// </summary>
        /// <param name="j">a join part</param>
        /// <returns></returns>
        internal override bool JoinCondition(Transaction tr,JoinPart j, ref ATree<long,SqlValue> joinCond, ref ATree<long,SqlValue> where)// update j.joinCondition, j.thetaCondition and j.ordSpec
        {
            switch (kind)
            {
                case Sqlx.AND:
                    left.JoinCondition(tr,j, ref joinCond, ref where);
                    right.JoinCondition(tr,j, ref joinCond, ref where);
                    break;
                case Sqlx.LSS:
                case Sqlx.LEQ:
                case Sqlx.GTR:
                case Sqlx.GEQ:
                case Sqlx.NEQ:
                case Sqlx.EQL:
                    {
                        if (left.isConstant)
                        {
                            if (kind != Sqlx.EQL)
                                break;
                            if (right.IsFrom(tr, j.left, false) && right.name.Defpos() > 0)
                            { 
                                j.left.AddMatch(right.name, left.Eval(tr, null));
                                return true;
                            }
                            else if (right.IsFrom(tr, j.right, false) && right.name.Defpos() > 0)
                            {
                                j.right.AddMatch(right.name, left.Eval(tr, null));
                                return true;
                            }
                            break;
                        }
                        if (right.isConstant)
                        {
                            if (kind != Sqlx.EQL)
                                break;
                            if (left.IsFrom(tr, j.left, false) && left.name.Defpos() > 0)
                            {
                                j.left.AddMatch(left.name, right.Eval(tr, null));
                                return true;
                            }
                            else if (left.IsFrom(tr, j.right, false) && left.name.Defpos() > 0)
                            {
                                j.right.AddMatch(left.name, right.Eval(tr, null));
                                return true;
                            }
                            break;
                        }
                        if (left is Activation.Variable || right is Activation.Variable)
                            break;
                        var ll = left.IsFrom(tr,j.left,true);
                        var rr = right.IsFrom(tr,j.right, true);
                        if (ll && rr)
                        {
                            ATree<long, SqlValue>.Add(ref joinCond, sqid, this);
                            return true;
                        }
                        var rl = right.IsFrom(tr,j.left, true);
                        var lr = left.IsFrom(tr,j.right, true);
                        if (rl && lr)
                        {
                            var nv = new SqlValueExpr(tr, Sqlx.EQL, right, left, mod);
                            ATree<long, SqlValue>.Add(ref joinCond, nv.sqid, nv);
                            return true;
                        }
                        break;
                    }
            }
            return base.JoinCondition(tr, j, ref joinCond, ref where);
        }
        /// <summary>
        /// Analysis stage Conditions: distribute conditions to joins and froms.
        /// OR expressions cannot be distributed
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="repl">updated list of potential replacements (because of equality)</param>
        internal override void DistributeConditions(Transaction tr,Query q, Ident.Tree<SqlValue> repl, RowSet data)
        {
            base.DistributeConditions(tr,q, repl, data);
            switch (kind)
            {
                case Sqlx.NO: left.DistributeConditions(tr,q,repl, data); return;
                case Sqlx.AND: left.DistributeConditions(tr,q,repl, data);
                    right.DistributeConditions(tr,q,repl,data); return;
                case Sqlx.EQL:
                    if (IsFrom(tr,q,false)) 
                        q.AddCondition(tr,new BTree<long,SqlValue>(sqid,this), null, null);
                    else if (repl[left.name] is SqlValue lr && lr.IsFrom(tr,q, false))
                    {
                        if (data != null)
                            data = new ReplaceRowSet(data, left.name, right, new TableType(left.name, right, q.nominalDataType));
                        var ns = new SqlValueExpr(tr, kind, repl[left.name], right, Sqlx.NO);
                        q.AddCondition(tr, new BTree<long, SqlValue>(ns.sqid,ns), null, data);
                    }
                    else if (repl[right.name] is SqlValue rl && rl.IsFrom(tr,q, false))
                    {
                        if (data != null)
                            data = new ReplaceRowSet(data, right.name, left, new TableType(right.name, left, q.nominalDataType));
                        var ns = new SqlValueExpr(tr, kind, repl[right.name], left, Sqlx.NO);
                        q.AddCondition(tr, new BTree<long, SqlValue>(ns.sqid,ns), null, data);
                    } 
                    return;
                case Sqlx.GTR: goto case Sqlx.EQL;
                case Sqlx.LSS: goto case Sqlx.EQL;
                case Sqlx.NEQ: goto case Sqlx.EQL;
                case Sqlx.LEQ: goto case Sqlx.EQL;
                case Sqlx.GEQ: goto case Sqlx.EQL;
            }
        }
        internal override SqlValue PartsIn(Domain dt)
        {
            var lf = left.PartsIn(dt);
            var rg = right.PartsIn(dt);
            if (lf == null)
                return (kind==Sqlx.AND)?rg:null;
            if (rg == null)
                return (kind == Sqlx.AND)?lf:null;
            return new SqlValueExpr(tr, kind, lf, rg, mod);
        }
        internal override void BuildRepl(Transaction tr,Query lf, ref Ident.Tree<SqlValue> lr, ref Ident.Tree<SqlValue> rr)
        {
            switch (kind)
            {
                case Sqlx.NO: left.BuildRepl(tr,lf,ref lr,ref rr); return;
                case Sqlx.AND:
                    left.BuildRepl(tr,lf, ref lr, ref rr);
                    right.BuildRepl(tr,lf,  ref lr, ref rr); return;
                case Sqlx.EQL:
                    if (left.IsFrom(tr,lf, false))
                    {
                        Ident.Tree<SqlValue>.Add(ref rr, left.name, right);
                        Ident.Tree<SqlValue>.Add(ref lr, right.name, left);
                    }
                    else
                    {
                        Ident.Tree<SqlValue>.Add(ref lr, left.name, right);
                        Ident.Tree<SqlValue>.Add(ref rr, right.name, left);
                    }
                    return;
            }
        }
        /// <summary>
        /// Analysis stage Conditions: look for equality conditions in WHERE
        /// </summary>
        /// <param name="al">Equality ComparisonPredicates found</param>
        /// <returns>whether all top level conditions are AND</returns>
        internal override bool WhereEquals(List<SqlValueExpr> al)
        {
            switch (kind)
            {
                case Sqlx.EQL: al.Add(this); return true;
                case Sqlx.AND:
                    return left.WhereEquals(al) && (right?.WhereEquals(al)??true);
                case Sqlx.OR: return false;
                default: return true;
            }
        }
        /// <summary>
        /// convenience method for JavaScript assignment operators
        /// </summary>
        /// <param name="p"></param>
        /// <param name="v"></param>
        void Set(Transaction tr,TypedValue v)
        {
            if (assign != null)
                assign.Assign(tr,v);
        }
        /// <summary>
        /// Evaluate the expression (binary operators).
        /// May return null if operands not yet ready
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (rs?.qry is Query q && alias != null && !rs.building)
            {
                var iq = q.nominalDataType.names.Get(alias,out Ident s);
                if (iq.HasValue)
                {
                    if (q.row is RowBookmark rb && rb.valueInProgress != _name.iix &&
                      rb.Get(alias)?[s] is TypedValue tv)
                        return tv;
                }
            }
            TypedValue v = null;
            try
            {
                switch (kind)
                {
                    case Sqlx.AND:
                        {
                            var a = left.Eval(tr,rs)?.NotNull();
                            var b = right.Eval(tr,rs)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (mod == Sqlx.BINARY) // JavaScript
                                v = nominalDataType.Copy(tr, new TInt(a.ToLong() & b.ToLong()));
                            else
                                v = (a.IsNull||b.IsNull)?
                                    nominalDataType.New(tr):TBool.For(tr, nominalDataType, ((TBool)a).value.Value && ((TBool)b).value.Value);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.ASC: // JavaScript ++
                        {
                            v = left.Eval(tr,rs)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return nominalDataType.New(tr);
                            var w = v.dataType.Eval(tr, v, Sqlx.ADD, new TInt(1L));
                            Set(tr,w);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.ASSIGNMENT:
                        {
                            var a = tr.context.Lookup(tr,left.name)?.LVal(tr);
                            var b = right.Eval(tr,rs)?.NotNull();
                            if (b == null)
                                return null;
                            if (a == null)
                                return b;
                            return a.Assign(tr,b);
                        }
#if MONGO
                    case Sqlx.CALL: // JavScript function evaluation
                        {
                            var a = new Activation(ctx.dynLink, null);
                            var lx = new JavaScript.Lexer(left.Eval1(cx).ToString().ToCharArray(), 0);
                            var ex = new JavaScript.Parser(ctx.staticLink, lx).ParseElement();
                            if (ex is JavaScript.Function)
                            {
                                var f = (JavaScript.Function)ex;
                                var p = right as SqlArgList;
                                var na = p?.args.Length ?? 0;
                                var np = f.parms.Length;
                                if (p != null)
                                {
                                    if (na != np)
                                        throw new DBException((na > np) ? "22110" : "22109");
                                    for (var i = 0; i < np; i++)
                                    {
                                        var s = f.parms[i];
                                        cx.LVal(s).Assign(p.args[i].Eval(cx));
                                    }
                                }
                                else // maybe a built-in function
                                {
                                    int k = 0;
                                    foreach (var la in p.args)
                                    {
                                        var s = new Ident("p" + (k++));
                                        cx.LVal(s).Assign(la.Eval(cx));
                                    }
                                }
                                ex = f.body;
                            }
                            ex.Obey(cx.transaction);
                            if (a.ret == null && a is StructuredActivation)
                                return ((StructuredActivation)a).ths.Eval1(a);
                            return a.ret;
                        }
#endif
                    case Sqlx.COLLATE:
                        {
                            var a = left.Eval(tr,rs)?.NotNull();
                            object o = a?.Val(tr);
                            if (o == null)
                                return null;
                            Domain ct = left.nominalDataType;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = right.Eval(tr,rs)?.NotNull();
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return left.Eval(tr,rs)?.NotNull();
                                Domain nt = new Domain(ct.kind, ct.prec, ct.charSet, new CultureInfo(cname));
                                return new TChar(nt, (string)o);
                            }
                            throw new DBException("2H000", "Collate on non-string?").ISO();
                        }
                    case Sqlx.COMMA: // JavaScript
                        {
                            if (left.Eval(tr,rs)?.NotNull() == null)// for side effects
                                return null;
                            return right.Eval(tr,rs);
                        }
                    case Sqlx.CONCATENATE:
                        {
                            if (left.nominalDataType.kind == Sqlx.ARRAY && right.nominalDataType.kind == Sqlx.ARRAY)
                                return left.nominalDataType.Concatenate(tr, (TArray)left.Eval(tr,rs), (TArray)right.Eval(tr,rs));
                            var lf = left.Eval(tr,rs)?.NotNull();
                            var or = right.Eval(tr,rs)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            var stl = lf.ToString();
                            var str = or.ToString();
                            return new TChar(or.dataType, (lf.IsNull && or.IsNull)?null:stl + str);
                        }
                    case Sqlx.CONTAINS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            if (ta == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            if (a==null)
                                return nominalDataType.New(tr);
                            if (right.nominalDataType.kind == Sqlx.PERIOD)
                            {
                                var tb = right.Eval(tr,rs)?.NotNull();
                                if (tb == null)
                                    return null;
                                var b = tb.Val(tr) as Period;
                                if (b == null)
                                    return TBool.Null;
                                return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.start) <= 0 && a.end.CompareTo(tr, b.end) >= 0);
                            }
                            var c = right.Eval(tr,rs)?.NotNull();
                            if (c == null)
                                return null;
                            if (c == TNull.Value)
                                return TBool.Null;
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, c) <= 0 && a.end.CompareTo(tr, c) >= 0);
                        }
                    case Sqlx.DESC: // JavaScript --
                        {
                            v = left.Eval(tr,rs)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return nominalDataType.New(tr);
                            var w = v.dataType.Eval(tr, v, Sqlx.MINUS, new TInt(1L));
                            Set(tr,w);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = nominalDataType.Eval(tr, left.Eval(tr,rs)?.NotNull(), kind, right.Eval(tr,rs)?.NotNull());
                        if (v != null)
                            Set(tr,v);
                        return v;
                    case Sqlx.DOT:
                        v = left.Eval(tr,rs);
                        if (v != null)
                            v = v[right.name];
                        return v;
                    case Sqlx.EQL:
                        {
                            if (assign!=null) // JavaScript
                            {
                                v = right.Eval(tr,rs)?.NotNull();
                                if (v!=null)
                                    Set(tr,v);
                                return v;
                            }
                            var rv = right.Eval(tr,rs)?.NotNull();
                            if (rv==null)
                                return null;
#if MONGO
                            if (rv.dataType.kind == Sqlx.REGULAR_EXPRESSION)
                                return TBool.For(RegEx.PCREParse(rv.ToString()).Like(left.Eval(cx).ToString(), null));
#endif
                            return TBool.For(tr,nominalDataType,rv!=null && rv.CompareTo(tr, left.Eval(tr,rs)?.NotNull()) == 0);
                        }
                    case Sqlx.EQUALS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return TBool.Null;
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.start) == 0 && b.end.CompareTo(tr, a.end) == 0);
                        }
                    case Sqlx.EXCEPT:
                        {
                            var ta = left.Eval(tr,rs) as TMultiset;
                            var tb = right.Eval(tr,rs) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr, TMultiset.Except(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.GEQ:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) >= 0);
                        }
                    case Sqlx.GTR:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) > 0);
                        }
                    case Sqlx.INTERSECT:
                        {
                            var ta = left.Eval(tr,rs) as TMultiset;
                            var tb = right.Eval(tr,rs) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr, TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = left.Eval(tr,rs)?.NotNull();
                            var ar = right.Eval(tr,rs)?.NotNull();
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return nominalDataType.New(tr);
                            return nominalDataType.Copy(tr,((TArray)al)[sr.Value]);
                        }
                    case Sqlx.LEQ:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) <= 0);
                        }
                    case Sqlx.LOWER: // JavScript >> and >>>
                        {
                            long a;
                            var ol = left.Eval(tr,rs)?.NotNull();
                            var or = right.Eval(tr,rs)?.NotNull();
                            if (ol==null || or == null)
                                return null;
                            if (or.IsNull)
                                return nominalDataType.New(tr);
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (mod == Sqlx.GTR)
                                unchecked
                                {
                                    a = (long)(((ulong)ol.Val(tr)) >> s);
                                }
                            else
                            {
                                if (ol.IsNull)
                                    return nominalDataType.New(tr);
                                a = ol.ToLong().Value >> s;
                            }
                            v = new TInt(a);
                            Set(tr,v);
                            return v;
                        }
                    case Sqlx.LSS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) < 0);
                        }
                    case Sqlx.MINUS:
                        {
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (tb == null)
                                return null;
                            if (left == null)
                            {
                                TypedValue oz = right.nominalDataType.Copy(tr, new TInt(0));
                                return right.nominalDataType.Eval(tr, oz, Sqlx.MINUS, tb);
                            }
                            var ta = left.Eval(tr,rs)?.NotNull();
                            if (ta == null)
                                return null;
                            v = left.nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.NEQ:
                        {
                            var rv = right.Eval(tr,rs)?.NotNull();
#if MONGO
                            if (rv.dataType.kind == Sqlx.REGULAR_EXPRESSION)
                                return TBool.For(!RegEx.PCREParse(rv.ToString()).Like(left.Eval(cx).ToString(), null));
#endif
                            return TBool.For(tr, nominalDataType, left.nominalDataType.Compare(tr, left.Eval(tr,rs)?.NotNull(), rv) != 0);
                        }
                    case Sqlx.NO: return left.Eval(tr,rs);
                    case Sqlx.NOT:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            if (ta == null)
                                return null;
                            if (mod == Sqlx.BINARY)
                                return new TInt(~ta.ToLong());
                            var bv = ta as TBool;
                            if (bv.IsNull)
                                throw tr.Exception("22004").ISO();
                            return TBool.For(tr, nominalDataType, !bv.value.Value);
                        }
                    case Sqlx.OR:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            switch (mod)
                            {
                                case Sqlx.BINARY: v = new TInt(ta.ToLong() | tb.ToLong()); break;
                                case Sqlx.EXCEPT: v = new TInt(ta.ToLong() ^ tb.ToLong()); break;
                                default:
                                    {
                                        if (ta.IsNull || tb.IsNull)
                                            return nominalDataType.New(tr);
                                        var a = ta as TBool;
                                        var b = tb as TBool;
                                        v = TBool.For(tr, nominalDataType, a.value.Value || b.value.Value);
                                    }
                                    break;
                            }
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.OVERLAPS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return nominalDataType.New(tr);
                            return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) >= 0 && b.end.CompareTo(tr, a.start) >= 0);
                        }
                    case Sqlx.PERIOD:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TPeriod(Domain.Period, new Period(ta, tb));
                        }
                    case Sqlx.PLUS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = left.nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.PRECEDES:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a==null || b==null)
                                return nominalDataType.New(tr);
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) == 0);
                            return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) <= 0);
                        }
                    case Sqlx.QMARK:
                        {
                            var a = right as SqlValueExpr;
                            var lf = left.Eval(tr,rs)?.NotNull();
                            if (lf == null)
                                return null;
                            if (lf.IsNull)
                                return nominalDataType.New(tr);
                            var b = ((bool)lf.Val(tr)) ? a.left : a.right;
                            return a.left.nominalDataType.Coerce(tr, b.Eval(tr,rs));
                        }
                    case Sqlx.RBRACK:
                        {
                            var a = left.Eval(tr,rs)?.NotNull();
                            var b = right.Eval(tr,rs)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull ||b.IsNull)
                                return nominalDataType.New(tr);
                            return ((TArray)a)[b.ToInt().Value];
                        }
                    case Sqlx.SUCCEEDS:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a==null || b==null)
                                return nominalDataType.New(tr);
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.end) == 0);
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.end) >= 0);
                        }
                    case Sqlx.TIMES:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr,
                                TMultiset.Union((TMultiset)ta, (TMultiset)tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.UPPER: // JavaScript <<
                        {
                            var lf = left.Eval(tr,rs)?.NotNull();
                            var or = right.Eval(tr,rs)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            long a;
                            if (or.IsNull)
                                return nominalDataType.New(tr);
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (lf.IsNull)
                                return nominalDataType.New(tr);
                            a = lf.ToLong().Value >> s;
                            v = new TInt(a);
                            Set(tr, v);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.nominalDataType, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = left.Eval(tr,rs)?.NotNull();
                            var tb = right.Eval(tr,rs)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TChar(left.nominalDataType, ta.ToString() + " " + tb.ToString());
                        }
                }
                return null;
            }
            catch (DBException ex)
            {
                throw ex;
            }
            catch (DivideByZeroException)
            {
                throw new DBException("22012").ISO();
            }
            catch (Exception)
            {
                throw new DBException("22000").ISO();
            }
        }
        internal override TypedValue Eval(GroupingRowSet grs, ABookmark<TRow, GroupRow> bm)
        {
            Transaction tr = grs.tr;
            TypedValue v = null;
            try
            {
                switch (kind)
                {
                    case Sqlx.AND:
                        {
                            var a = left.Eval(grs, bm)?.NotNull();
                            var b = right.Eval(grs, bm)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (mod == Sqlx.BINARY) // JavaScript
                                v = nominalDataType.Copy(tr, new TInt(a.ToLong() & b.ToLong()));
                            else
                                v = (a.IsNull || b.IsNull) ?
                                    nominalDataType.New(tr) : TBool.For(tr, nominalDataType, ((TBool)a).value.Value && ((TBool)b).value.Value);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.ASC: // JavaScript ++
                        {
                            v = left.Eval(grs, bm)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return nominalDataType.New(tr);
                            var w = v.dataType.Eval(tr, v, Sqlx.ADD, new TInt(1L));
                            Set(tr, w);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.ASSIGNMENT:
                        {
                            var a = tr.context.Lookup(tr, left.name)?.LVal(tr);
                            var b = right.Eval(grs, bm)?.NotNull();
                            if (b == null)
                                return null;
                            if (a == null)
                                return b;
                            return a.Assign(tr, b);
                        }
#if MONGO
                    case Sqlx.CALL: // JavScript function evaluation
                        {
                            var a = new Activation(ctx.dynLink, null);
                            var lx = new JavaScript.Lexer(left.Eval1(cx).ToString().ToCharArray(), 0);
                            var ex = new JavaScript.Parser(ctx.staticLink, lx).ParseElement();
                            if (ex is JavaScript.Function)
                            {
                                var f = (JavaScript.Function)ex;
                                var p = right as SqlArgList;
                                var na = p?.args.Length ?? 0;
                                var np = f.parms.Length;
                                if (p != null)
                                {
                                    if (na != np)
                                        throw new DBException((na > np) ? "22110" : "22109");
                                    for (var i = 0; i < np; i++)
                                    {
                                        var s = f.parms[i];
                                        cx.LVal(s).Assign(p.args[i].Eval(cx));
                                    }
                                }
                                else // maybe a built-in function
                                {
                                    int k = 0;
                                    foreach (var la in p.args)
                                    {
                                        var s = new Ident("p" + (k++));
                                        cx.LVal(s).Assign(la.Eval(cx));
                                    }
                                }
                                ex = f.body;
                            }
                            ex.Obey(cx.transaction);
                            if (a.ret == null && a is StructuredActivation)
                                return ((StructuredActivation)a).ths.Eval1(a);
                            return a.ret;
                        }
#endif
                    case Sqlx.COLLATE:
                        {
                            var a = left.Eval(grs, bm)?.NotNull();
                            object o = a?.Val(tr);
                            if (o == null)
                                return null;
                            Domain ct = left.nominalDataType;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = right.Eval(grs, bm)?.NotNull();
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return left.Eval(grs, bm)?.NotNull();
                                Domain nt = new Domain(ct.kind, ct.prec, ct.charSet, new CultureInfo(cname));
                                return new TChar(nt, (string)o);
                            }
                            throw new DBException("2H000", "Collate on non-string?").ISO();
                        }
                    case Sqlx.COMMA: // JavaScript
                        {
                            if (left.Eval(grs, bm)?.NotNull() == null)// for side effects
                                return null;
                            return right.Eval(grs, bm);
                        }
                    case Sqlx.CONCATENATE:
                        {
                            if (left.nominalDataType.kind == Sqlx.ARRAY && right.nominalDataType.kind == Sqlx.ARRAY)
                                return left.nominalDataType.Concatenate(tr, (TArray)left.Eval(grs, bm), (TArray)right.Eval(grs, bm));
                            var lf = left.Eval(grs, bm)?.NotNull();
                            var or = right.Eval(grs, bm)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            var stl = lf.ToString();
                            var str = or.ToString();
                            return new TChar(or.dataType, (lf.IsNull && or.IsNull) ? null : stl + str);
                        }
                    case Sqlx.CONTAINS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            if (ta == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            if (a == null)
                                return nominalDataType.New(tr);
                            if (right.nominalDataType.kind == Sqlx.PERIOD)
                            {
                                var tb = right.Eval(grs, bm)?.NotNull();
                                if (tb == null)
                                    return null;
                                var b = tb.Val(tr) as Period;
                                if (b == null)
                                    return TBool.Null;
                                return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.start) <= 0 && a.end.CompareTo(tr, b.end) >= 0);
                            }
                            var c = right.Eval(grs, bm)?.NotNull();
                            if (c == null)
                                return null;
                            if (c == TNull.Value)
                                return TBool.Null;
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, c) <= 0 && a.end.CompareTo(tr, c) >= 0);
                        }
                    case Sqlx.DESC: // JavaScript --
                        {
                            v = left.Eval(grs, bm)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return nominalDataType.New(tr);
                            var w = v.dataType.Eval(tr, v, Sqlx.MINUS, new TInt(1L));
                            Set(tr, w);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = nominalDataType.Eval(tr, left.Eval(grs, bm)?.NotNull(), kind, right.Eval(grs, bm)?.NotNull());
                        if (v != null)
                            Set(tr, v);
                        return v;
                    case Sqlx.DOT:
                        v = left.Eval(grs, bm);
                        if (v != null)
                            v = v[right.name];
                        return v;
                    case Sqlx.EQL:
                        {
                            if (assign != null) // JavaScript
                            {
                                v = right.Eval(grs, bm)?.NotNull();
                                if (v != null)
                                    Set(tr, v);
                                return v;
                            }
                            var rv = right.Eval(grs, bm)?.NotNull();
                            if (rv == null)
                                return null;
#if MONGO
                            if (rv.dataType.kind == Sqlx.REGULAR_EXPRESSION)
                                return TBool.For(RegEx.PCREParse(rv.ToString()).Like(left.Eval(cx).ToString(), null));
#endif
                            return TBool.For(tr, nominalDataType, rv != null && rv.CompareTo(tr, left.Eval(grs, bm)?.NotNull()) == 0);
                        }
                    case Sqlx.EQUALS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return TBool.Null;
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.start) == 0 && b.end.CompareTo(tr, a.end) == 0);
                        }
                    case Sqlx.EXCEPT:
                        {
                            var ta = left.Eval(grs, bm) as TMultiset;
                            var tb = right.Eval(grs, bm) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr, TMultiset.Except(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.GEQ:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) >= 0);
                        }
                    case Sqlx.GTR:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) > 0);
                        }
                    case Sqlx.INTERSECT:
                        {
                            var ta = left.Eval(grs, bm) as TMultiset;
                            var tb = right.Eval(grs, bm) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr, TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = left.Eval(grs, bm)?.NotNull();
                            var ar = right.Eval(grs, bm)?.NotNull();
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return nominalDataType.New(tr);
                            return nominalDataType.Copy(tr, ((TArray)al)[sr.Value]);
                        }
                    case Sqlx.LEQ:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) <= 0);
                        }
                    case Sqlx.LOWER: // JavScript >> and >>>
                        {
                            long a;
                            var ol = left.Eval(grs, bm)?.NotNull();
                            var or = right.Eval(grs, bm)?.NotNull();
                            if (ol == null || or == null)
                                return null;
                            if (or.IsNull)
                                return nominalDataType.New(tr);
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (mod == Sqlx.GTR)
                                unchecked
                                {
                                    a = (long)(((ulong)ol.Val(tr)) >> s);
                                }
                            else
                            {
                                if (ol.IsNull)
                                    return nominalDataType.New(tr);
                                a = ol.ToLong().Value >> s;
                            }
                            v = new TInt(a);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.LSS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(tr, nominalDataType, ta.CompareTo(tr, tb) < 0);
                        }
                    case Sqlx.MINUS:
                        {
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (tb == null)
                                return null;
                            if (left == null)
                            {
                                TypedValue oz = right.nominalDataType.Copy(tr, new TInt(0));
                                return right.nominalDataType.Eval(tr, oz, Sqlx.MINUS, tb);
                            }
                            var ta = left.Eval(grs, bm)?.NotNull();
                            if (ta == null)
                                return null;
                            v = left.nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.NEQ:
                        {
                            var rv = right.Eval(grs, bm)?.NotNull();
#if MONGO
                            if (rv.dataType.kind == Sqlx.REGULAR_EXPRESSION)
                                return TBool.For(!RegEx.PCREParse(rv.ToString()).Like(left.Eval(cx).ToString(), null));
#endif
                            return TBool.For(tr, nominalDataType, left.nominalDataType.Compare(tr, left.Eval(grs, bm)?.NotNull(), rv) != 0);
                        }
                    case Sqlx.NO: return left.Eval(grs, bm);
                    case Sqlx.NOT:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            if (ta == null)
                                return null;
                            if (mod == Sqlx.BINARY)
                                return new TInt(~ta.ToLong());
                            var bv = ta as TBool;
                            if (bv.IsNull)
                                throw tr.Exception("22004").ISO();
                            return TBool.For(tr, nominalDataType, !bv.value.Value);
                        }
                    case Sqlx.OR:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            switch (mod)
                            {
                                case Sqlx.BINARY: v = new TInt(ta.ToLong() | tb.ToLong()); break;
                                case Sqlx.EXCEPT: v = new TInt(ta.ToLong() ^ tb.ToLong()); break;
                                default:
                                    {
                                        if (ta.IsNull || tb.IsNull)
                                            return nominalDataType.New(tr);
                                        var a = ta as TBool;
                                        var b = tb as TBool;
                                        v = TBool.For(tr, nominalDataType, a.value.Value || b.value.Value);
                                    }
                                    break;
                            }
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.OVERLAPS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return nominalDataType.New(tr);
                            return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) >= 0 && b.end.CompareTo(tr, a.start) >= 0);
                        }
                    case Sqlx.PERIOD:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TPeriod(Domain.Period, new Period(ta, tb));
                        }
                    case Sqlx.PLUS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = left.nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.PRECEDES:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return nominalDataType.New(tr);
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) == 0);
                            return TBool.For(tr, nominalDataType, a.end.CompareTo(tr, b.start) <= 0);
                        }
                    case Sqlx.QMARK:
                        {
                            var a = right as SqlValueExpr;
                            var lf = left.Eval(grs, bm)?.NotNull();
                            if (lf == null)
                                return null;
                            if (lf.IsNull)
                                return nominalDataType.New(tr);
                            var b = ((bool)lf.Val(tr)) ? a.left : a.right;
                            return a.left.nominalDataType.Coerce(tr, b.Eval(grs, bm));
                        }
                    case Sqlx.RBRACK:
                        {
                            var a = left.Eval(grs, bm)?.NotNull();
                            var b = right.Eval(grs, bm)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull || b.IsNull)
                                return nominalDataType.New(tr);
                            return ((TArray)a)[b.ToInt().Value];
                        }
                    case Sqlx.SUCCEEDS:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val(tr) as Period;
                            var b = tb.Val(tr) as Period;
                            if (a == null || b == null)
                                return nominalDataType.New(tr);
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.end) == 0);
                            return TBool.For(tr, nominalDataType, a.start.CompareTo(tr, b.end) >= 0);
                        }
                    case Sqlx.TIMES:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = nominalDataType.Eval(tr, ta, kind, tb);
                            Set(tr, v);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs, bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(tr,
                                TMultiset.Union((TMultiset)ta, (TMultiset)tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.UPPER: // JavaScript <<
                        {
                            var lf = left.Eval(grs, bm)?.NotNull();
                            var or = right.Eval(grs, bm)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            long a;
                            if (or.IsNull)
                                return nominalDataType.New(tr);
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (lf.IsNull)
                                return nominalDataType.New(tr);
                            a = lf.ToLong().Value >> s;
                            v = new TInt(a);
                            Set(tr, v);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.nominalDataType, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = left.Eval(grs, bm)?.NotNull();
                            var tb = right.Eval(grs,bm)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TChar(left.nominalDataType, ta.ToString() + " " + tb.ToString());
                        }
                }
                return null;
            }
            catch (DBException ex)
            {
                throw ex;
            }
            catch (DivideByZeroException)
            {
                throw new DBException("22012").ISO();
            }
            catch (Exception)
            {
                throw new DBException("22000").ISO();
            }
        }
        internal override Target LVal(Transaction tr)
        {
            switch (kind)
            {
                case Sqlx.DOT:
                    if (right is SqlName)
                        return new Target(left.LVal(tr), ((SqlName)right).name, nominalDataType);
                    break;
                case Sqlx.QMARK:
                    return (left.Eval(tr, (tr.context as Query)?.rowSet) == TBool.True) ? left.LVal(tr) : right.LVal(tr);
            }
            return base.LVal(tr);
        }
        static Domain _Type(Transaction t,Sqlx kind,Sqlx mod,SqlValue left,SqlValue right)
        {
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    return Domain.Bool;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: return right.nominalDataType;
                case Sqlx.COLLATE: return Domain.Char;
                case Sqlx.COLON: return left.nominalDataType; // JavaScript
                case Sqlx.CONCATENATE: return Domain.Char;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        var dl = left.nominalDataType.kind;
                        var dr = right.nominalDataType.kind;
                        if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            return left.nominalDataType;
                        return left.FindType(Domain.UnionNumeric);
                    }
                case Sqlx.DOT: return right.nominalDataType;
                case Sqlx.EQL: return Domain.Bool;
                case Sqlx.EXCEPT: return left.nominalDataType;
                case Sqlx.GTR: return Domain.Bool;
                case Sqlx.INTERSECT: return left.nominalDataType;
                case Sqlx.LOWER: return Domain.Int; // JavaScript >> and >>>
                case Sqlx.LSS: return Domain.Bool;
                case Sqlx.MINUS:
                    if (left != null)
                    {
                        var dl = left.nominalDataType.kind;
                        var dr = right.nominalDataType.kind;
                        if (dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME)
                        {
                            if (dr == dl)
                                return Domain.Interval;
                            if (dr == Sqlx.INTERVAL)
                                return left.nominalDataType;
                        }
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            return right.nominalDataType;
                        return left.FindType(Domain.UnionDateNumeric);
                    }
                    return right.FindType(Domain.UnionDateNumeric);
                case Sqlx.NEQ: return Domain.Bool;
                case Sqlx.LEQ: return Domain.Bool;
                case Sqlx.GEQ: return Domain.Bool;
                case Sqlx.NO: return left.nominalDataType;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        var dl = left.nominalDataType.kind;
                        var dr = right.nominalDataType.kind;
                        if ((dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME) && dr == Sqlx.INTERVAL)
                            return left.nominalDataType;
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            return right.nominalDataType;
                        return left.FindType(Domain.UnionDateNumeric);
                    }
                case Sqlx.QMARK:
                    { // JavaScript
                        var r = right as SqlValueExpr;
                        return r.nominalDataType;
                    }
                case Sqlx.RBRACK: return new Domain(Sqlx.ARRAY, left.nominalDataType);
                case Sqlx.SET: return left.nominalDataType; // JavaScript
                case Sqlx.TIMES:
                    {
                        var dl = left.nominalDataType.kind;
                        var dr = right.nominalDataType.kind;
                        if (dl == Sqlx.NUMERIC || dr == Sqlx.NUMERIC)
                            return Domain.Numeric;
                        if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            return left.nominalDataType;
                        if (dr == Sqlx.INTERVAL && (dl == Sqlx.INTEGER || dl == Sqlx.NUMERIC))
                            return right.nominalDataType;
                        return left.FindType(Domain.UnionNumeric);
                    }
                case Sqlx.UNION: return left.nominalDataType;
                case Sqlx.UPPER: return Domain.Int; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: return Domain.Char;
                case Sqlx.XMLCONCAT: return Domain.Char;
            }
            return Domain.Content;
        }
        internal override SqlValue Invert()
        {
            switch (kind)
            {
                case Sqlx.AND: return new SqlValueExpr(tr, Sqlx.OR, left.Invert(), right.Invert(), Sqlx.NULL);
                case Sqlx.OR: return new SqlValueExpr(tr, Sqlx.AND, left.Invert(), right.Invert(), Sqlx.NULL);
                case Sqlx.NOT: return left;
                case Sqlx.EQL: return new SqlValueExpr(tr,Sqlx.NEQ,left,right,Sqlx.NULL);
                case Sqlx.GTR: return new SqlValueExpr(tr, Sqlx.LEQ, left, right, Sqlx.NULL);
                case Sqlx.LSS: return new SqlValueExpr(tr, Sqlx.GEQ, left, right, Sqlx.NULL);
                case Sqlx.NEQ: return new SqlValueExpr(tr, Sqlx.EQL, left, right, Sqlx.NULL);
                case Sqlx.GEQ: return new SqlValueExpr(tr, Sqlx.LSS, left, right, Sqlx.NULL);
                case Sqlx.LEQ: return new SqlValueExpr(tr, Sqlx.GTR, left, right, Sqlx.NULL);
            } 
            return base.Invert();
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the SqlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Transaction t,SqlValue v)
        {
            if (v is SqlName w)
                v = w.resolvedTo;
            var e = v as SqlValueExpr;
            if (e == null || (nominalDataType != null && nominalDataType != v.nominalDataType))
                return false;
            if (left != null)
            {
                if (!left.MatchExpr(cx,e.left))
                    return false;
            }
            else
                if (e.left != null)
                    return false;
            if (right != null)
            {
                if (!right.MatchExpr(cx,e.right))
                    return false;
            }
            else
                if (e.right != null)
                    return false;
            return true;
        }
        internal override void AddMatches(Transaction tr, Query f)
        {
            if (kind == Sqlx.EQL && IsKnown(tr, f))
                f.AddMatchedPair(left, right);
        }
        /// <summary>
        /// analysis stage Conditions()
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q,bool disj)
        {
            var needed = BTree<SqlValue, int>.Empty;
            switch (kind)
            {
                case Sqlx.AND:
                case Sqlx.OR:
                case Sqlx.LSS:
                case Sqlx.LEQ:
                case Sqlx.GTR:
                case Sqlx.GEQ:
                case Sqlx.NEQ:
                    {
                        left.Conditions(tr, q, false);
                        right.Conditions(tr, q, false);
                        break;
                    }
                case Sqlx.EQL:
                    {
                        if (!disj)
                            goto case Sqlx.OR;
                        if (right.isConstant && left.IsFrom(tr, q, false) && left.name.Defpos() > 0)
                        {
                            q.AddMatch(left.name, right.Eval(tr, null));
                            return true;
                        }
                        else if (left.isConstant && right.IsFrom(tr, q, false) && right.name.Defpos() > 0)
                        {
                            q.AddMatch(right.name, left.Eval(tr, null));
                            return true;
                        }
                        goto case Sqlx.AND;
                    }
                case Sqlx.NO:
                case Sqlx.NOT:
                    {
                        left.Conditions(tr, q, false);
                        break;
                    }
            }
            if (q != null && nominalDataType == Domain.Bool)
                DistributeConditions(tr, q, Ident.Tree<SqlValue>.Empty, q.rowSet);
            return false;
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            var a = left?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var b = right?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            return a || b;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("(");
            sb.Append(left.ToString());
            switch (kind)
            {
                case Sqlx.ASSIGNMENT: sb.Append(":="); break;
                case Sqlx.COLON: sb.Append(":"); break;
                case Sqlx.EQL: sb.Append("="); break;
                case Sqlx.COMMA: sb.Append(","); break;
                case Sqlx.CONCATENATE: sb.Append("||"); break;
                case Sqlx.DIVIDE: sb.Append("/"); break;
                case Sqlx.DOT: sb.Append("."); break;
                case Sqlx.DOUBLECOLON: sb.Append("::"); break;
                case Sqlx.GEQ: sb.Append(">="); break;
                case Sqlx.GTR: sb.Append(">"); break;
                case Sqlx.LBRACK: sb.Append("["); break;
                case Sqlx.LEQ: sb.Append("<="); break;
                case Sqlx.LPAREN: sb.Append("("); break;
                case Sqlx.LSS: sb.Append("<"); break;
                case Sqlx.MINUS: sb.Append("-"); break;
                case Sqlx.NEQ: sb.Append("<>"); break;
                case Sqlx.PLUS: sb.Append("+"); break;
                case Sqlx.TIMES: sb.Append("*"); break;
                case Sqlx.AND: sb.Append(" and "); break;
            }
            if (right != null)
                sb.Append(right.ToString());
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (kind == Sqlx.LPAREN)
                sb.Append(")");
            sb.Append(")");
            if (alias!=null)
            {
                sb.Append(" as ");
                sb.Append(alias);
            }
            return sb.ToString();
        }
        /// <summary>
        /// If an expression contains aggregations, continue recursion but only emit code for the aggregations
        /// </summary>
        /// <param name="sb"></param>
        /// <param name="c"></param>
        /// <param name="uf"></param>
        /// <param name="ur"></param>
        /// <param name="eflag"></param>
        public override void ToString1(StringBuilder sb, Transaction c, From uf, Record ur, string eflag,bool cms,ref string cp)
        {
            if (aggregates() && eflag!=null)
            {
                left?.ToString1(sb, c, uf, ur, eflag, cms,ref cp);
                right?.ToString1(sb, c, uf, ur, eflag, cms,ref cp);
                return;
            }
            if (cms)
                sb.Append(cp);
            cp = ",";
            if (kind==Sqlx.CONCATENATE)
            {
                sb.Append("concat(");
                left.ToString1(sb, c, uf, ur, eflag, false, ref cp);
                sb.Append(',');
                right.ToString1(sb, c, uf, ur, eflag, false, ref cp);
                sb.Append(')');
                return;
            }
            sb.Append('(');
            left.ToString1(sb,c,uf,ur,eflag, false,ref cp);
            switch (kind)
            {
                case Sqlx.ASSIGNMENT: sb.Append(":="); break;
                case Sqlx.COLON: sb.Append(":"); break;
                case Sqlx.EQL: sb.Append("="); break;
                case Sqlx.COMMA: sb.Append(","); break;
                case Sqlx.CONCATENATE: sb.Append("||"); break;
                case Sqlx.DIVIDE: sb.Append("/"); break;
                case Sqlx.DOT: sb.Append("."); break;
                case Sqlx.DOUBLECOLON: sb.Append("::"); break;
                case Sqlx.GEQ: sb.Append(">="); break;
                case Sqlx.GTR: sb.Append(">"); break;
                case Sqlx.LBRACK: sb.Append("["); break;
                case Sqlx.LEQ: sb.Append("<="); break;
                case Sqlx.LPAREN: sb.Append("("); break;
                case Sqlx.LSS: sb.Append("<"); break;
                case Sqlx.MINUS: sb.Append("-"); break;
                case Sqlx.NEQ: sb.Append("<>"); break;
                case Sqlx.PLUS: sb.Append("+"); break;
                case Sqlx.TIMES: sb.Append("*"); break;
                case Sqlx.AND: sb.Append(" and "); break;
            }
            if (right != null)
                right.ToString1(sb,c,uf,ur,eflag, false,ref cp);
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (kind == Sqlx.LPAREN)
                sb.Append(")");
            sb.Append(")");
            if (eflag!=null && alias!=null)
            {
                sb.Append(" as ");sb.Append(alias);
            }
            if (cms)
                cp = Sep(cp);
        }
        public override string ToString1(Transaction t,Record ur)
        {
            if ((cx.context.cur as Query)?.replace[ToString()] is string s)
                return s;
            var sb = new StringBuilder(left.ToString1(cx,ur));
            switch(kind)
            {
                case Sqlx.EQL: sb.Append("="); break;
                case Sqlx.GTR: sb.Append(">"); break;
                case Sqlx.LSS: sb.Append("<"); break;
                case Sqlx.GEQ: sb.Append(">="); break;
                case Sqlx.LEQ: sb.Append("<="); break;
                case Sqlx.NEQ: sb.Append("<>"); break;
                case Sqlx.AND: sb.Append(" and "); break;
                case Sqlx.PLUS: sb.Append("+"); break;
                case Sqlx.MINUS: sb.Append("-"); break;
                default: sb.Append(' '); sb.Append(kind.ToString());sb.Append(' '); break;
            }
            if (right != null)
                sb.Append(right.ToString1(cx,ur));
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (kind == Sqlx.LPAREN)
                sb.Append(")");
            /*           if (right != null)
                       {
                           var tv = right.Eval(cx)?.NotNull();
                           if (tv == null)
                               sb.Append("<null>");
                           else if (tv.dataType.Typecode() == 3)
                           {
                               sb.Append('\'');
                               sb.Append(tv.ToString());
                               sb.Append('\'');
                           }
                           else
                               sb.Append(tv.ToString());
                       } */
            return sb.ToString();
        }
    }
    /// <summary>
    /// A SqlValue that is the null literal
    /// </summary>
    internal class SqlNull : SqlValue
    {
        /// <summary>
        /// constructor for null
        /// </summary>
        /// <param name="cx">the context</param>
        internal SqlNull(Transaction t)
            : base(cx,Domain.Null)
        { }
        /// <summary>
        /// the value of null
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            return TNull.Value;
        }
        internal override bool _MatchExpr(Transaction t,SqlValue v)
        {
            if (v is SqlName w)
                v = w.resolvedTo;
            return v is SqlNull;
        }
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            return true;
        }
        public override string ToString()
        {
            return "NULL";
        }
    }
    /// <summary>
    /// The SqlLiteral subclass
    /// </summary>
    internal class SqlLiteral : SqlValue
    {
        internal TypedValue val;
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(TypedValue v) : base(v.dataType)
        {
            val = v;
        }
        public SqlLiteral(Transaction t, Domain dt)
            : base(cx, dt)
        {
            val = null;
        }
        public SqlLiteral(SqlLiteral v) :base(v.tr,v.nominalDataType)
        {
            val = v.val;
        }
        protected SqlLiteral(SqlLiteral v, ref ATree<long, SqlValue> vs): base(v,ref vs)
        {
            val = v.val;
        }
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            return true;
        }
        /// <summary>
        /// test for structural equivalence
        /// </summary>
        /// <param name="v">an SqlValue</param>
        /// <returns>whether they are structurally equivalent</returns>
        internal override bool _MatchExpr(Transaction t,SqlValue v)
        {
            if (v is SqlName w)
                v = w.resolvedTo;
            var c = v as SqlLiteral;
            if (c == null || (nominalDataType != null && nominalDataType != v.nominalDataType))
                return false;
            return val == c.val;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlLiteral(this,ref vs);
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
   //         if (name != null && ((!(cx is Query)) ||  (cx as Query)?.row!=null) && ctx[name] is TypedValue tv)
   //             return tv;
            return val ?? nominalDataType.New(tr);
        }
        internal override TypedValue Eval(GroupingRowSet grs, ABookmark<TRow, GroupRow> bm)
        {
            return val;
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            if (d.kind!=Sqlx.Null && (val==null || !val.dataType.EqualOrStrongSubtypeOf(d)))
                val = d.Coerce(tr,val);
        }
        public override int CompareTo(object obj)
        {
            var that = obj as SqlLiteral;
            if (that == null)
                return 1;
            return val?.CompareTo(tr, that.val) ?? throw new PEException("PE000");
        }
        /// <summary>
        /// A literal is supplied by any query
        /// </summary>
        /// <param name="q">the query</param>
        /// <returns>true</returns>
        internal override bool IsFrom(Transaction tr,Query q,bool ordered,Domain ut=null)
        {
            return true;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            return true;
        }
        internal override SqlValue For(Transaction tr, Query q)
        {
            return this;
        }
        internal override bool isConstant
        {
            get
            {
                return val == null || val.IsConstant;
            }
        }
        internal override Domain FindType(Domain dt)
        {
            var vt = val.dataType;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22005Y", dt.kind, vt.kind).ISO();
            if (vt.kind==Sqlx.INTEGER)
                return dt; // keep union options open
            return vt;
        }
        internal override void _AddNeeds(List<TColumn> cs)
        {
        }
        internal override void _AddReqs(Transaction tr, From gf, Domain ut, ref ATree<SqlValue, int> gfreqs, int i)
        {
        }
        public override string ToString()
        {
            return val.ToString();
        }
        public override void ToString1(StringBuilder sb, Transaction c,From uf,Record r,string eflag,bool cms,ref string cm)
        {
            if (cms)
                sb.Append(cm);
            sb.Append(val.ToString(c));
            if (alias!=null)
            {
                sb.Append(" as "); sb.Append(alias);
            }
            if (cms)
                cm = Sep(cm);
        }
        public override string ToString1(Transaction t,Record ur)
        {
            if (val.dataType.Typecode() == 3)
                return "'" + val.ToString() + "'";
            return base.ToString1(cx,ur);
        }
    }
    /// <summary>
    /// A DateTime Literal
    /// </summary>
    internal class SqlDateTimeLiteral : SqlLiteral
    {
        /// <summary>
        /// construct a datetime literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">the data type</param>
        /// <param name="n">the string version of the date/time</param>
        public SqlDateTimeLiteral(Transaction tr, Domain op, string n)
            : base(tr, op.Parse(tr,n))
        {}
    }
    internal class SqlTableRowStart : SqlValue
    {
        internal From from;
        internal Record rec;
        public SqlTableRowStart(Context tr, From f, Record r) : base(tr, f.nominalDataType)
        {
            from = f;
            rec = r;
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var table = from.target as Table;
            return table.versionedRows[rec.ppos].start ?? from.nominalDataType.New(tr);
        }
    }
    /// <summary>
    /// An element in a structured SqlValue.
    ///  The elements of row types are SqlTypeColumns listed among the references of the row. 
    ///  There is an option to place the column TypedValues in its variables; 
    ///  if they are not there, then the whole will evaluate to a Row: this can iterate. 
    ///  Such substructure TypedValues are built whenever columns are individually updated 
    ///  or a method is called.
    /// </summary>
    internal class SqlTypeColumn : SqlValue
    {
        internal string blockid;
        internal SqlValue was = null; // tunnel for ColsForRestView
        /// <summary>
        /// A column from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="dt">the column type</param>
        public SqlTypeColumn(Transaction tr, Domain dt, Ident cn, bool add, bool resolve, Query q)
            : base(tr, dt, cn)
        {
            blockid = q?.blockid ?? tr.context.cur?.blockid ?? tr.context.blockid;
            if (resolve)
                Resolve(tr,q);
        }
        protected SqlTypeColumn(Transaction tr,Domain dt,Ident cn,string bkid) :base(tr,dt,cn)
        {
            blockid = bkid;
        }
        public SqlTypeColumn(Transaction tr,Query f,Domain dt,Ident cn, bool add, bool resolve)
            :base(tr,dt,cn)
        {
            blockid = f.blockid;
            if (add)
                f.Add(tr,this, cn);
            if (resolve)
                Resolve(tr,f);
        }
        protected SqlTypeColumn(SqlTypeColumn v, ref ATree<long, SqlValue> vs) : base(v,ref vs)
        {
            blockid = v.blockid;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlTypeColumn(this,ref vs);
        }
        internal override void _Setup(Transaction tr, Query q, Domain d)
        {
            if (blockid == q.blockid && (nominalDataType == Domain.Content || nominalDataType==Domain.Value))
                nominalDataType = tr.dataType(q.nominalDataType.names[(alias ?? name).ident]);
            base._Setup(tr, q, d);
        }
        internal override bool Check(Transaction tr, GroupSpecification group)
        {
            if (group == null)
                return true;
            // if all keys are grouped return false
            if (tr.Db(name.dbix) is Transaction db && db.objects[name.Defpos()] is Selector s
    && db.objects[s.tabledefpos] is Table tb && db.FindPrimaryIndex(tb) is Index ix)
            {
                for (var i=0; i < ix.cols.Length;i++) 
                    if (group.Has(db.objects[ix.cols[i]].NameInSession(db)))
                        return base.Check(tr, group); // if any keys not grouped, this must be grouped
            }
            return false;
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            SqlValue v = null;
            int i = 0;
            if (q == null)
                return this;
            if (tr.Ctx(blockid)?.contexts.Contains(q.blockid) == true)
                for (; i < q.names.Length; i++)
                    if (name.Match(q, q.names[i]))
                    {
                        v = q.cols[i];
                        break;
                    }
            if (v == null)
                return this;
            if (!(v is SqlTypeColumn))
                return new SqlTypeColumn(tr, nominalDataType, v.alias ?? v.name, blockid);
            if (name.Suffix(tr, v.alias ?? v.name) is Ident s)
                return new SqlTypeColumn(tr, nominalDataType, new Ident(v.alias ?? v.name, s), blockid);
            return v;
        }
        internal override void Needed(Transaction tr,Query.Need n)
        {
            if (tr.Ctx(blockid) is Query sq)
                sq.Needs(name, n);
        }
        public override void ToString1(StringBuilder sb, Transaction c, From uf, Record ur, string eflag, bool cms, ref string cm)
        {
            if (was!=null && eflag!=null)
            {
                was.ToString1(sb, c, uf, ur, eflag, cms, ref cm);
                return;
            }
            if (cms)
                sb.Append(cm);
            var iq = uf?.names.Get(name, out Ident s);
            if (iq.HasValue && ur.Field(uf.names[iq.Value].segpos) is TypedValue tv)
                sb.Append(tv.ToString(c));
            else
                name.ToString1(sb, c, eflag);
            if (eflag!=null && eflag!=blockid)
            {
                sb.Append('#'); sb.Append(blockid);
            }
            cm = Sep(cm);
        }
  
        public override string ToString1(Transaction t, Record ur = null)
        {
            if ((cx.context.cur as Query)?.replace[ToString()] is string s)
                return s;
            var f = ur?.Field(name.Defpos());
            if (f == null)
                return base.ToString1(cx, ur);
            var fs = f.ToString();
            var fn = name.ToString();
            if (fs == fn)
                return fn;
            return fs + " as " + fn;
        }
        internal override bool IsFrom(Transaction tr, Query q, bool ordered,Domain ut=null)
        {
            return q.PosFor(q.names,name).HasValue || PosIn(tr, q, ordered, out Ident sub).HasValue
                || ut?.names[name]!=null;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            return q.Knows(this);
        }
        internal override int? _PosIn(Transaction tr, Query q, bool ordered, out Ident sub)
        {
            sub = null;
            int? r = null;
            if (q.contexts[blockid] is Query qc) // test for accessibility
            {
                // need to be careful here as Request logic keeps reinitialising f.names
                r = q.names.Get(name,out sub) ?? q.names.Get(qc,name,out sub) ?? 
                    (q as From)?.accessibleDataType.names.Get(name,out sub);
                if (r == null)
                    return null;
                if (!q.needs.Contains(name))
                    ATree<Ident, Query.Need>.Add(ref q.needs, name, Query.Need.selected);
                return r;
            }
            return null;
        }
        internal override bool _MatchExpr(Transaction t,SqlValue v)
        {
            if (v is SqlName w)
                v = w.resolvedTo;
            if (v is SqlTypeColumn stc)
                return blockid==stc.blockid && name.Match(cx, v.name);
            return false;
        }
        internal override SqlValue PartsIn(Domain dt)
        {
            return (dt.names[name].HasValue) ? this : null;
        }
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            return q.names[name] != null;
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs) 
        {
            if (rs?.qry is Query q && q.blockid==blockid)
            {
                var i = PosIn(tr, q, false, out Ident sub);
                if (i.HasValue && q.row?.Value()?[i.Value]?[sub] is TypedValue tv)
                    return tv;
            } 
            if (tr.Ctx(blockid) is Query qt)
            {
                var n = alias??name;
                if (qt.NameMatches(qt, n))
                    n = n.sub;
                if (qt?.row?.Get(n) is TypedValue tw)
                    return tw;
                if (qt.defs[n] is SqlValue sv && sv != this && sv.Eval(tr, rs) is TypedValue tv)
                    return tv;
            }
            if (rs.Eval(this) is TypedValue tx)
                return tx;
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return bmk.Get(name);
        }
        internal override TypedValue Eval(GroupingRowSet grs, ABookmark<TRow, GroupRow> bm)
        {
            return grs.Lookup(this)?.Eval(bm) ?? base.Eval(grs,bm);
        }
        internal override void AddNeeds(GroupingRowSet.GroupInfo gi)
        {
            AddNeeds(gi, name);
            AddNeeds(gi, alias);
        }
        internal override void AddNeeds(Transaction tr,Query q)
        {
            var found = false;
            var qa = q.AccessibleCols();
            for (var i = 0; i < qa.Length; i++)
                if (name.segpos == qa[i].defpos)
                    found = true;
            if (!found)
                return;
            for (var i = 0; i < q.cols.Count; i++)
                if (q.cols[i].MatchExpr(tr.context, this))
                    return;
            ATree<long, SqlValue>.Add(ref q.cols, q.cols.Count, this);
        }
        void AddNeeds(GroupingRowSet.GroupInfo gi,Ident n)
        {
            var iq = gi.nominalKeyType.names[n];
            if (iq != null)
                ATree<Ident, GroupingRowSet.GroupRowEntry>.Add(ref gi.grs.ges, name, new GroupingRowSet.GroupKeyEntry(gi.grs, n));
            else
            {
                iq = gi.nominalRowType.names[n];
                if (iq != null)
                    ATree<Ident, GroupingRowSet.GroupRowEntry>.Add(ref gi.grs.ges, name, new GroupingRowSet.GroupValueEntry(gi.grs, n, iq.Value));
            }
        }
        internal override void _AddNeeds(List<TColumn> cs)
        {
            cs.Add(new TColumn(alias??name, nominalDataType));
        }
        internal override int? FillHere(Ident n)
        {
            if (name.CompareTo(n) != 0)
                return null;
            n.dbix = name.dbix;
            n.segpos = name.Defpos();
            n.type = Ident.IDType.Column;
            return 0;
        }
    }
    internal class SqlTransitionColumn : SqlTypeColumn
    {
        internal SqlTransitionColumn(Transaction t, Domain dt, Ident n, string blkid) : base(cx, dt, n, blkid) { }
        protected SqlTransitionColumn(SqlTransitionColumn c,ref ATree<long, SqlValue> vs) : base(c,ref vs) { }
        internal override TypedValue Eval(Transaction tr, RowSet rs)
        {
            var ta = tr.Ctx(blockid) as TriggerActivation;
            var q = tr.context.cur;
            if (name.HeadsMatch(q, ta._trig.oldTable) || name.HeadsMatch(q, ta._trig.newTable))
                for (var b = ta.vars.First(); b != null; b = b.Next())
                    if (name.HeadsMatch(ta, b.key()))
                        return ta._trs.qry.row?.Get(name.sub);
            var rw = BTree<long, TypedValue>.Empty;
            if (name.HeadsMatch(q, ta._trig.oldRow))
                rw = ta._trs.from.oldRow;
            else if (name.HeadsMatch(q, ta._trig.newRow))
                rw = ta._trs.from.newRow;
            return rw[name.sub.segpos];
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlTransitionColumn(this,ref vs);
        }
    }
    /// <summary>
    /// A Row value
    /// </summary>
    internal class SqlRow : SqlValue
    {
        internal SqlValue[] columns;
#if MONGO
        public SqlRow(Transaction t,TRow r) :base(cx,r.dataType)
        {
            columns = new SqlValue[nominalDataType.Length];
            etag = r.etag;
            for (int i = 0; i < r.dataType.Length;i++ )
                this[i] = r[i].Build(ctx);
            Initialise();
        }
#endif
         /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(Transaction t, Domain t, params SqlValue[] r)
            : base(cx, t)
        {
            columns = new SqlValue[nominalDataType.Length];
            var rr = r;
            if (r.Length == 1 && r[0].nominalDataType.Equals(t))
                rr = (r[0] as SqlRow).columns;
            for (int i = 0; i < t.Length; i++)
                if (i < rr.Length)
                {
                    columns[i] = rr[i];
                    etag = ETag.Add(rr[i].etag, etag);
                }
            Needs(r);
        }
        public SqlRow(Transaction t,SqlValue[] r)
            : base(cx,new TableType(cx,r))
        {
            columns = r;
        }
        protected SqlRow(SqlRow v, ref ATree<long, SqlValue> vs) :base(v,ref vs)
        {
            columns = new SqlValue[v.columns.Length];
            for (int i = 0; i < v.columns.Length; i++)
                columns[i] = v.columns[i]?.Copy(ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var v = new SqlValue[columns.Length];
            for (var i = 0; i < columns.Length; i++)
                v[i] = columns[i].Import(tr, q);
            return new SqlRow(tr, v);
        }
        internal SqlRow Initialise(Transaction tr)
        {
            var dm = tr.GetDomain(nominalDataType,out Database d);
            var bkid = tr.context.blockid;
            RoleObject ro = null;
            if (dm!=null)
                ro = d._Role.defs[dm.super];
            for (int i = 0; i < nominalDataType.Length; i++)
            {
                var id = nominalDataType.names[i];
                var cf = columns[i];
                if (cf == null)
                {
                    cf = new SqlTypeColumn(tr, nominalDataType[i], id, true, false,tr.context.cur as Query);
                    columns[i] = cf;
                }
                if (tr.context.cur is Query q)
                    Ident.Tree<SqlValue>.Add(ref q.defs, id, cf);
            }
            return this;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlRow(this,ref vs);
        }
        internal SqlValue this[int i] { get { return columns[i]; } set { columns[i] = value; } }
        internal int Length { get { return nominalDataType.Length;  } }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            if (d == null || d.columns==null || (d.kind != Sqlx.ROW && d.kind != Sqlx.TABLE))
                return;
            if (d.columns.Length != Length)
                throw new DBException("22005Z", nominalDataType, d);
            for (int i = 0; i < nominalDataType.Length;i++ )
                this[i]._Setup(tr,q,d.columns[i]);
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType[i].Coerce(tr, this[i].Eval(tr,rs)?.NotNull()) is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(tr, nominalDataType, r);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType[i].Coerce(tr, this[i].Eval(bmk)?.NotNull()) is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(tr, nominalDataType, r);
        }
        internal override bool IsFrom(Transaction tr,Query q,bool ordered,Domain ut=null)
        {
            var r = true;
            for (var i = 0; i < columns.Length; i++)
            {
                var s = columns[i].IsFrom(tr,q,ordered,ut);
                r = r && s;
            }
            return r;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            var r = true;
            for (var i = 0; i < columns.Length; i++)
            {
                var s = columns[i].IsKnown(tr, q);
                r = r && s;
            }
            return r;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nc = new SqlValue[columns.Length];
            var changed = false;
            for(var i=0;i<columns.Length;i++)
            {
                nc[i] = columns[i].Replace(tr, cx, so, sv, ref map);
                if (nc[i] != columns[i])
                    changed = true;
            }
            return changed ? new SqlRow(tr, nc) : this;
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < columns.Length; i++)
                if (columns[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(RowSet rs)
        {
            for (var i = 0; i < columns.Length; i++)
                columns[i].Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            for (var i = 0; i < columns.Length; i++)
                columns[i].StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            for (var i = 0; i < columns.Length; i++)
                columns[i]._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            var nulls = true;
            for (var i = 0; i < columns.Length; i++)
                if (columns[i].SetReg(key)!=null)
                    nulls = false;
            return nulls?null:this;
        }
        public override string ToString()
        {
            return "ROW(..)";
        }
    }
    internal class SqlRowArray : SqlValue
    {
        internal readonly SqlRow[] rows;
        internal SqlRowArray(Transaction t, Domain dt, SqlRow[] rs) : base(cx, dt)
        {
            rows = rs;
            Needs(rows);
        }
        protected SqlRowArray(SqlRowArray s, ref ATree<long, SqlValue> vs) :base(s,ref vs)
        {
            rows = new SqlRow[s.rows.Length];
            for (var i = 0; i < s.rows.Length; i++)
                rows[i] = (SqlRow)s.rows[i].Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlRowArray(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var r = new SqlRow[rows.Length];
            for (var i = 0; i < rows.Length; i++)
                r[i] = (SqlRow)rows[i].Import(tr,q);
            return new SqlRowArray(tr, nominalDataType, r);
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var r = new TArray(nominalDataType, rows.Length);
            for (var j = 0; j < rows.Length; j++)
                r[j] = rows[j].Eval(tr,rs);
            return r;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var r = new TArray(nominalDataType, rows.Length);
            for (var j = 0; j < rows.Length; j++)
                r[j] = rows[j].Eval(bmk);
            return r;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nr = new SqlRow[rows.Length];
            var changed = false;
            for (var i=0;i<rows.Length;i++)
            {
                nr[i] = rows[i].Replace(tr, cx, so, sv, ref map) as SqlRow;
                if (nr[i] != rows[i])
                    changed = true;
            }
            return changed ? new SqlRowArray(tr, nominalDataType, nr) : this;
        }
        internal override void RowSet(Transaction tr,From f)
        {
            for (var i = 0; i < rows.Length; i++)
                for (var j = 0; j < rows[i].Length; j++)
                    rows[i].columns[j]._Setup(tr, f, nominalDataType[j]); 
            f.data = new SqlRowSet(tr,f,nominalDataType as RowType, rows);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < rows.Length; i++)
                if (rows[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(RowSet rs)
        {
            for (var i = 0; i < rows.Length; i++)
                rows[i].Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            for (var i = 0; i < rows.Length; i++)
                rows[i].StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            for (var i = 0; i < rows.Length; i++)
                rows[i]._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            var nulls = true;
            for (var i = 0; i < rows.Length; i++)
                if (rows[i].SetReg(key) != null)
                    nulls = false;
            return nulls ? null : this;
        }
    }
    /// <summary>
    /// This class is only used during aggregation computations
    /// </summary>
    internal class GroupRow : SqlRow
    {
        Idents info;
        internal GroupRow(Transaction tr,GroupingRowSet.GroupInfo gi, Domain gt, TRow key)
            : base(tr, gt)
        {
            info = gi.group.names;
            var dt = gi.grs.qry.nominalDataType;
            var vs = BTree<long, SqlValue>.Empty;
            for (int j = 0; j < dt.Length; j++) // not grs.rowType!
            {
                var n = dt.names[j];
                var sc = gi.grs.qry.ValFor(n) ?? gi.grs.qry.Find(tr,n);
                if (sc is SqlName sn)
                    sc = sn.resolvedTo;
                if (sc != null)
                    columns[j] = (key[n] is TypedValue tv) ? new SqlLiteral(tr, tv)
                        : sc.SetReg(key) ?? new SqlLiteral(tr, sc.Eval(tr, gi.grs) ?? TNull.Value);
            }
            for (var b = gi.grs.having.First(); b != null; b = b.Next())
                b.value().AddIn(tr,gi.grs);
            ATree<TRow, GroupRow>.Add(ref gi.grs.g_rows, key, this);
        }
        internal GroupRow(Transaction tr,Query q,TRow key) :base(tr,q.nominalDataType)
        {
            var vs = BTree<long, SqlValue>.Empty;
            var dt = q.nominalDataType;
            for (int j = 0; j < dt.Length; j++) // not grs.rowType!
            {
                var n = dt.names[j];
                columns[j] = (key[n] is TypedValue tv) ? new SqlLiteral(tr, tv)
                    : q.ValFor(n).Copy(ref vs);
            }
        }
        protected GroupRow(GroupRow g, ref ATree<long, SqlValue> vs) :base(g,ref vs)
        {
            info = g.info;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new GroupRow(this,ref vs);
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType[i].Coerce(tr, this[i].Eval(tr,rs)?.NotNull()) is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(nominalDataType, r, info);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType[i].Coerce(tr, this[i].Eval(bmk)?.NotNull()) is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(nominalDataType, r, info);
        }
    }
    // a document value (field keys are constant strings, values are expressions)
    internal class SqlDocument : SqlValue
    {
        protected ATree<int,SqlValue> content = BTree<int,SqlValue>.Empty;
        protected Ident.Tree<int?> colNames = Ident.Tree<int?>.Empty;
        internal SqlValue cur = null, res = null;
        public SqlDocument(Transaction t,Domain dt) :base(cx,dt) 
        {
        }
        public SqlDocument(Transaction t,TDocument doc) :base(cx,Domain.Document)
        {
            for(var f = doc.content.First();f!= null;f=f.Next())
            {
                Ident.Tree<int?>.Add(ref colNames, f.value().name, (int)content.Count);
                ATree<int, SqlValue>.Add(ref content, (int)content.Count, 
                    f.value().typedValue.Build(cx)
                    );
            }
        }
        public SqlDocument(Transaction t,char[] inp, ref int pos) :base(cx,Domain.Document)
        {

        }
        protected SqlDocument(SqlDocument d, ref ATree<long, SqlValue> vs) :base(d,ref vs)
        {
            for (var b = d.content.First(); b != null; b = b.Next())
                ATree<int, SqlValue>.Add(ref content, b.key(), b.value().Copy(ref vs));
            colNames = d.colNames;
            cur = d.cur?.Copy(ref vs);
            res = d.res?.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlDocument(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var r = new SqlDocument(tr, nominalDataType);
            for (var b = content.First(); b != null; b = b.Next())
                ATree<int, SqlValue>.Add(ref r.content, b.key(), b.value().Import(tr,q));
            r.colNames = colNames;
            r.cur = cur?.Import(tr,q);
            r.res = res?.Import(tr,q);
            return r;
        }
        internal KeyValuePair<Ident,SqlValue>[] doc
        {
            get
            {
                var r = new KeyValuePair<Ident, SqlValue>[content.Count];
                for (var c = colNames.First(); c != null; c = c.Next())
                    if (c.value().HasValue)
                        r[c.value().Value] = new KeyValuePair<Ident, SqlValue>(c.key(), content[c.value().Value]);
                return r;
            }
        }
        internal long Length {  get { return content.Count; } }
        internal void Add(Ident s, SqlValue v)
        {
            var iq = colNames[s];
            if (iq.HasValue)
                ATree<int, SqlValue>.Add(ref content, iq.Value, v);
            else
            {
                var i = (int)colNames.Count;
                ATree<int, SqlValue>.Add(ref content, i, v);
                Ident.Tree<int?>.Add(ref colNames, s, i);
            }
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            for (int i = 0; i < content.Count; i++)
            {
                var w = content[i];
                Setup(tr,q,w,w.nominalDataType);
            }
        }
        public SqlValue this[Ident n]
        {
            get
            {
                var iq = colNames[n];
                if (iq.HasValue)
                    return content[iq.Value];
                return null;
            }
            set { Add(n, value); }
        }
        internal long Count { get { return content.Count; } }
        internal override void StartCounter(Transaction tr,RowSet rs)
        {
            res = this[new Ident("initial",0)];
            if (res != null)
                res.Eval(tr,rs);
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var d = new TDocument(tr);
            foreach (var x in doc)
            {
                if (x.Value.Eval(tr,rs)?.NotNull() is TypedValue tv)
                {
                    if (tv.dataType.kind == Sqlx.CHAR && tv.ToString()[0] == '$')
                    {
                        var val = tv.ToString().Substring(1).Trim();
                        var csi = tr.caseSensitiveIds;
                        tr.caseSensitiveIds = true;
                        try
                        {
                            var v = new Parser(tr).ParseSqlValue(val, Domain.Content);
                            var needed = BTree<SqlValue, Ident>.Empty;
                            Setup(tr, rs.qry, v, Domain.Content);
                            tv = v.Eval(tr,rs)?.NotNull();
                            if (tv == null)
                                return null;
                        }
                        catch (Exception e) { throw e; }
                        finally
                        {
                            tr.caseSensitiveIds = csi;
                        }
                    }
                    d.Add(x.Key, tv);
                }
                else
                    return null;
            }
            return d;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var d = new TDocument(tr);
            foreach (var x in doc)
            {
                if (x.Value.Eval(bmk)?.NotNull() is TypedValue tv)
                {
                    if (tv.dataType.kind == Sqlx.CHAR && tv.ToString()[0] == '$')
                    {
                        var val = tv.ToString().Substring(1).Trim();
                        var csi = tr.caseSensitiveIds;
                        tr.caseSensitiveIds = true;
                        try
                        {
                            var v = new Parser(tr).ParseSqlValue(val, Domain.Content);
                            var needed = BTree<SqlValue, Ident>.Empty;
                            Setup(tr, rs.qry, v, Domain.Content);
                            tv = v.Eval(bmk)?.NotNull();
                            if (tv == null)
                                return null;
                        }
                        catch (Exception e) { throw e; }
                        finally
                        {
                            tr.caseSensitiveIds = csi;
                        }
                    }
                    d.Add(x.Key, tv);
                }
                else
                    return null;
            }
            return d;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var nm = new Ident[colNames.Count];
            for (var i = 0; i < nm.Length; i++)
                nm[i] = new Ident(""+i,0);
            for (var n = colNames.First();n!= null;n=n.Next())
                if (n.value().HasValue)
                    nm[n.value().Value] = n.key();
            var cm = "";
            for (var e = content.First();e!= null;e=e.Next())
            {
                sb.Append(cm);
                sb.Append(nm[e.key()]);
                sb.Append(": ");
                if (e.value() == null)
                    sb.Append("null");
                else
                    sb.Append(e.value().ToString());
                cm = ", ";
            }
            sb.Append("}");
            return sb.ToString();
        }
    }
    internal class SqlDocArray : SqlDocument
    {
        public SqlDocArray(Transaction t) : base(cx, Domain.DocArray) { }
        public SqlDocArray(Transaction t, TDocArray d) : base(cx, Domain.DocArray) 
        {
            for (var f = d.content.First();f!= null;f=f.Next())
                Add(new Ident(""+f.key(),0), f.value().Build(cx));
        }
        protected SqlDocArray(SqlDocArray a,ref ATree<long, SqlValue> vs) : base(a,ref vs) { }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlDocArray(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var r = new SqlDocArray(tr);
            for (var b = content.First(); b != null; b = b.Next())
                ATree<int, SqlValue>.Add(ref r.content, b.key(), b.value()?.Import(tr, q));
            r.colNames = colNames;
            r.cur = cur?.Import(tr, q);
            r.res = res?.Import(tr, q);
            return r;
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var r = new TDocArray(tr);
            foreach (var c in doc)
                if (c.Value.Eval(tr,rs)?.NotNull() is TypedValue v)
                    r[int.Parse(c.Key.ident)] = v;
                else
                    return null;
            return r;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var r = new TDocArray(tr);
            foreach (var c in doc)
                if (c.Value.Eval(bmk)?.NotNull() is TypedValue v)
                    r[int.Parse(c.Key.ident)] = v;
                else
                    return null;
            return r;
        }
        internal SqlValue this[int i]
        {
            get { return this[new Ident("" + i,0)]; }
            set { this[new Ident("" + i,0)] = value; }
        }
        internal void Add(SqlValue v)
        {
            this[new Ident("" + Count,0)] = v;
        }
        internal override void RowSet(Transaction tr,From f)
        {
            if (Count > 0)
            {
                f.data = new DocArrayRowSet(tr,f, this);
                var d0 = this[0] as SqlDocument;
                foreach (var a in d0.doc)
                    f.Add(tr,a.Value, a.Key);
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var cm = "";
            for (var e = content.First();e!= null;e=e.Next())
            {
                sb.Append(cm);
                if (e.value() == null)
                    sb.Append("null");
                else
                    sb.Append(e.value().ToString());
                cm = ", ";
            }
            sb.Append("]");
            return sb.ToString();

        }
    }
    internal class SqlXmlValue : SqlValue
    {
        public XmlName element;
        public List<KeyValuePair<XmlName, SqlValue>> attrs = new List<KeyValuePair<XmlName, SqlValue>>();
        public List<SqlXmlValue> children = new List<SqlXmlValue>();
        public SqlValue content = null; // will become a string literal on evaluation
        public SqlXmlValue(XmlName e,Transaction t) : base(cx, Domain.XML)
        {
            element = e;
        }
        protected SqlXmlValue(SqlXmlValue x, ref ATree<long, SqlValue> vs) : base(x,ref vs)
        {
            element = x.element;
            foreach (var p in x.attrs)
                attrs.Add(new KeyValuePair<XmlName, SqlValue>(p.Key, p.Value.Copy(ref vs)));
            foreach (var c in x.children)
                children.Add(c.Copy(ref vs) as SqlXmlValue);
            content = x.content?.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlXmlValue(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var r = new SqlXmlValue(element, tr);
            foreach (var p in attrs)
                r.attrs.Add(new KeyValuePair<XmlName, SqlValue>(p.Key, p.Value.Import(tr,q)));
            foreach (var c in children)
                r.children.Add(c.Import(tr,q) as SqlXmlValue);
            r.content = content?.Import(tr,q);
            return r;
        }
        public void Add(XmlName n,SqlValue a)
        {
            attrs.Add(new KeyValuePair<XmlName, SqlValue>(n, a));
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var r = new TXml(element.ToString());
            foreach (var a in attrs)
                if (a.Value.Eval(tr,rs)?.NotNull() is TypedValue ta)
                    r = new TXml(r, a.Key.ToString(), ta);
                else
                    return null;
            foreach (var c in children)
                if (c.Eval(tr,rs) is TypedValue tc)
                    r.children.Add((TXml)tc);
                else
                    return null;
            if (content?.Eval(tr,rs)?.NotNull() is TypedValue tv)
                return new TXml(r, (tv as TChar)?.value);
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var r = new TXml(element.ToString());
            foreach (var a in attrs)
                if (a.Value.Eval(bmk)?.NotNull() is TypedValue ta)
                    r = new TXml(r, a.Key.ToString(), ta);
                else
                    return null;
            foreach (var c in children)
                if (c.Eval(bmk) is TypedValue tc)
                    r.children.Add((TXml)tc);
                else
                    return null;
            if (content?.Eval(bmk)?.NotNull() is TypedValue tv)
                return new TXml(r, (tv as TChar)?.value);
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("<");
            sb.Append(element.ToString());
            foreach (var a in attrs)
            {
                sb.Append(" ");
                sb.Append(a.Key.ToString());
                sb.Append("=");
                sb.Append(a.Value.ToString());
            }
            if (content != null || children.Count!=0)
            {
                sb.Append(">");
                if (content != null)
                    sb.Append(content);
                else
                    foreach (var c in children)
                        sb.Append(c.ToString());
                sb.Append("</");
                sb.Append(element.ToString());
            } 
            else sb.Append("/");
            sb.Append(">");
            return sb.ToString();
        }
        internal class XmlName
        {
            public string prefix = "";
            public string keyname;
            public XmlName(string k) {
                keyname = k;
            }
            public override string ToString()
            {
                if (prefix == "")
                    return keyname;
                return prefix + ":" + keyname;
            }
        }
    }
    internal class SqlSelectArray : SqlValue
    {
        public QueryExpression aqe;
        public SqlSelectArray(Context tr,QueryExpression qe,Domain dt)
            : base(tr, dt)
        {
            aqe = qe;
        }
        protected SqlSelectArray(SqlSelectArray s, ref ATree<long, SqlValue> vs) :base(s,ref vs)
        {
            var c = s.aqe.contexts;
            aqe = (QueryExpression)aqe.Copy(ref c,ref vs);
            aqe.contexts = c;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlSelectArray(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new SqlSelectArray(tr, aqe, q.nominalDataType);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            aqe.PreAnalyse(tr);
            var sc = new List<SqlValue>();
            for (int i = 0; i < aqe.display; i++)
                sc.Add(aqe.cols[i]);
            var at = new TableType(tr,sc.ToArray());
            if (at.Length == 1)
                nominalDataType = new Domain(Sqlx.ARRAY, at[0]);
            else
                nominalDataType = new Domain(Sqlx.ARRAY, at);
            if (d.kind == Sqlx.ARRAY)
            {
                if (d.elType != null && d.elType.Length != 1)
                    nominalDataType = nominalDataType.Constrain(d);
            }
        }
        internal override bool IsFrom(Transaction tr,Query q, bool ordered,Domain ut=null)
        {
            // called for side effects on needed
            for (var i = 0; i < aqe.cols.Count; i++)
                 aqe.cols[i].IsFrom(tr,q, ordered,ut);
            Query.PartsFrom(aqe.where,tr,q, ordered,ut);
            return false;
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var va = new TArray(nominalDataType);
            aqe.Analyse(tr);
            var ars = aqe.rowSet;
            int j = 0;
            var nm = aqe.names[0];
            for (var rb=ars.First();rb!= null;rb=rb.Next())
            {
                var rw = rb.Value();
                if (nominalDataType.elType.Length == 0)
                    va[j++] = rw[nm];
                else
                {
                    var vs = new TypedValue[aqe.display];
                    for (var i = 0; i < aqe.display; i++)
                        vs[i] = rw[i];
                    va[j++] = new TRow(tr, nominalDataType.elType, vs);
                }
            }
            return va;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var cs = BTree<string, Context>.Empty;
            return new SqlSelectArray(tr, aqe.Copy(ref cs, ref map) as QueryExpression, nominalDataType);
        }
        internal override bool aggregates()
        {
            return aqe.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            aqe.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            aqe.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            aqe.AddIn(tr, rs);
        }
        internal override SqlValue SetReg(TRow key)
        {
            aqe.SetReg(key);
            return this;
        }
        public override string ToString()
        {
            return "ARRAY[..]";
        }
    }
    /// <summary>
    /// an array value
    /// </summary>
    internal class SqlValueArray : SqlValue
    {
        /// <summary>
        /// the array
        /// </summary>
        public SqlValue[] array;
        // alternatively, the source
        public SqlValueSelect svs = null;
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(Transaction t, Domain t,SqlValue[] v)
            : base(cx, new Domain(Sqlx.ARRAY, t))
        {
            array = v;
            Needs(v);
        }
        public SqlValueArray(Transaction t, Domain t, SqlValue sv)
             : base(cx, t)
        {
            svs = sv as SqlValueSelect;
        }
        protected SqlValueArray(SqlValueArray v, ref ATree<long, SqlValue> vs) :base(v,ref vs)
        {
            if (svs != null)
            {
                svs = v.svs.Copy(ref vs) as SqlValueSelect;
                return;
            }
            array = new SqlValue[v.array.Length];
            for (int i = 0; i < array.Length; i++)
                array[i] = v.array[i]?.Copy(ref vs);
        }
        /// <summary>
        /// analysis stage Selects(): Setup the array elements
        /// </summary>
        /// <param name="q">The required data type or null</param>
        /// <param name="s">the defaullt select</param>
        /// <returns>the possibly new SqlValue</returns>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            var t = nominalDataType.elType ?? Domain.Value;
            if (svs != null)
            {
                svs._Setup(tr, q, d);
                return;
            }
            for (int j = 0; j < array.Length; j++)
            {
                Setup(tr,q,array[j], t);
                t = t.LimitBy(array[j].nominalDataType);
            }
            if (t!=nominalDataType.elType)
                nominalDataType = new Domain(Sqlx.ARRAY,t);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlValueArray(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var a = new SqlValue[array.Length];
            for (int i = 0; i < array.Length; i++)
                a[i] = array[i]?.Import(tr,q);
            return new SqlValueArray(tr, nominalDataType, a);
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs.Eval(tr, rs) as TRowSet;
                for (var b = ers.rowSet.First(); b != null; b = b.Next())
                    ar.Add(b.row[0]);
                return new TArray(nominalDataType, ar);
            }
            var a = new TArray(nominalDataType,array.Length);
            for (int j = 0; j < array.Length; j++)
                a[j] = array[j]?.Eval(tr,rs)?.NotNull() ?? nominalDataType.New(tr);
            return a;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs.Eval(bmk) as TRowSet;
                for (var b = ers.rowSet.First(); b != null; b = b.Next())
                    ar.Add(b.row[0]);
                return new TArray(nominalDataType, ar);
            }
            var a = new TArray(nominalDataType, array.Length);
            for (int j = 0; j < array.Length; j++)
                a[j] = array[j]?.Eval(bmk)?.NotNull() ?? nominalDataType.New(tr);
            return a;
        }
        /// <summary>
        /// get an enumerator for the array elements
        /// </summary>
        /// <returns>an enumerator</returns>
        public IEnumerator<SqlValue> GetEnumerator()
        {
            return (IEnumerator<SqlValue>)array.GetEnumerator();
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var na = new SqlValue[array.Length];
            var changed = false;
            for (var i=0;i<array.Length;i++)
            {
                na[i] = array[i].Replace(tr, cx, so, sv, ref map);
                if (na[i] != array[i])
                    changed = true;
            }
            return (changed) ? new SqlValueArray(tr, nominalDataType, na) : this;
        }
        internal override bool aggregates()
        {
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    if (array[i].aggregates())
                        return true;
            return svs?.aggregates() ?? false;
        }
        internal override void Build(RowSet rs)
        {
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    array[i].Build(rs);
            svs?.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    array[i].StartCounter(tr, rs);
            svs?.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    array[i]._AddIn(tr, rs,ref aggsDone);
            svs?.AddIn(tr, rs);
            base._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            var nulls = true;
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    if (array[i].SetReg(key) != null)
                        nulls = false;
            if (svs?.SetReg(key)!=null)
                nulls = false;
            return nulls?null:this;
        }
        public override string ToString()
        {
            return "VALUES..";
        }
    }
    /// <summary>
    /// A subquery
    /// </summary>
    internal class SqlValueSelect : SqlValue
    {
        /// <summary>
        /// the subquery
        /// </summary>
        public Query expr = null;
        public Domain targetType = null;
        public string source;
        /// <summary>
        /// constructor: a subquery
        /// <param name="q">the context</param>
        /// <param name="t">the rowset type</param>
        /// <param name="s">the query source</param>
        /// </summary>
        public SqlValueSelect(Context tr, Query q, Domain t, string s)
            : base(tr,(t.kind!=Sqlx.Null)?t:q.nominalDataType) 
        {
            targetType = t;
            expr = q;
            source = s;
            Needs(tr,expr);
        }
        protected SqlValueSelect(SqlValueSelect s, ref ATree<long, SqlValue> vs) :base(s,ref vs)
        {
            var c = s.expr.contexts;
            expr = s.expr.Copy(ref c,ref vs);
            expr.contexts = c;
            targetType = s.targetType;
            source = s.source;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlValueSelect(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new SqlValueSelect(tr, expr, targetType, source);
        }
        static void _Types(Domain r, Domain t, List<Domain> ts)
        {
            switch (t.kind)
            {
                case Sqlx.ARRAY: ts.Add(new Domain(Sqlx.ARRAY, r)); break;
                case Sqlx.MULTISET: ts.Add(new Domain(Sqlx.MULTISET, r)); break;
                case Sqlx.TABLE: ts.Add(new Domain(Sqlx.TABLE, r)); break;
                default: ts.Add(r); break;
            }
        }
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            expr.Conditions(tr, q);
            return false;
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var oq = expr.Push(tr);
            try
            {
                expr.Analyse(tr);
                var ers = expr.rowSet;
                expr.acts = tr.stack;
                if (targetType?.kind == Sqlx.TABLE)
                    return new TRowSet(ers??new EmptyRowSet(tr,expr));
                var rb = ers.First();
                if (rb == null)
                    return nominalDataType.New(tr);
                TypedValue tv = rb._rs.qry.cols[0].Eval(tr,expr.rowSet)?.NotNull();
                if (targetType != null)
                    tv = targetType.Coerce(tr, tv);
                return tv;
            }
            catch (Exception e) { throw e; }
            finally { expr.Pop(tr,oq); }
        }
        internal override void RowSet(Transaction tr,From f)
        {
            expr.Analyse(tr);
            f.data = new ExportedRowSet(tr,f,expr.rowSet,f.nominalDataType);
        }
        /// <summary>
        /// analysis stage Selects(): setup the results of the subquery
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            isSetup = true;
            targetType = targetType ?? d;
            for (var n = expr.defs.First(); n != null; n = n.Next())
                if (!expr.defs.Contains(n.key()))
                    Ident.Tree<SqlValue>.Add(ref expr.defs, n.key(), n.value());
            for (int i = 0; i < expr.cols.Count; i++)
            {
                var c = expr.cols[i];
                if (c.name.Dbix < 0)
                    continue;
                if (tr.Db(c.name.dbix)._Role?.defs[c.name.Defpos()]?.name is Ident ni
                 && !expr.defs.Contains(ni))
                    Ident.Tree<SqlValue>.Add(ref expr.defs, ni, expr.cols[i]);
            }
            expr.PreAnalyse(tr);
            var oq = tr.context.cur;
            try
            {
                tr.context.cur = expr;
                expr.Conditions(tr,q);
            }
            catch (Exception e) { throw e; }
            finally
            {
                tr.context.cur = oq;
            }
            //             expr.Orders(expr.ordSpec);
            expr.analysed = tr.cid;
            if ((targetType.kind != Sqlx.ROW || targetType.kind != Sqlx.TABLE) && expr.nominalDataType.Length == 1)
                nominalDataType = expr.nominalDataType[0];
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var cs = BTree<string, Context>.Empty;
            return new SqlValueSelect(tr,expr.Copy(ref cs,ref map),nominalDataType,source);
        }
        internal override bool aggregates()
        {
            return expr.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            expr.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            expr.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            expr.AddIn(tr, rs);
        }
        internal override SqlValue SetReg(TRow key)
        {
            expr.SetReg(key);
            return this;
        }
        public override string ToString()
        {
            return "Select..";
        }
    }
    /// <summary>
    /// A Column Function SqlValue class
    /// </summary>
    internal class ColumnFunction : SqlValue
    {
        /// <summary>
        /// the set of column references
        /// </summary>
        internal Ident.Tree<bool> cols = Ident.Tree<bool>.Empty;
        /// <summary>
        /// constructor: a new ColumnFunction
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="t">the datatype</param>
        /// <param name="c">the set of TableColumns</param>
        public ColumnFunction(Transaction t, Sqlx t, Ident[] c)
            : base(cx,Domain.Bool)
        {
            kind = t; 
            foreach (var ic in c)
                Ident.Tree<bool>.Add(ref cols,ic,true);
        }
        protected ColumnFunction(ColumnFunction c, ref ATree<long, SqlValue> vs) :base(c,ref vs)
        {
            cols = c.cols;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new ColumnFunction(this,ref vs);
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if ((tr.context.cur as Query)?.row.Grouping() is Idents gp)
            {
                for (var b = cols.First(); b != null; b = b.Next())
                    if (!gp[b.key()].HasValue)
                        return new TInt(0);
                return new TInt(1);
            }
            return base.Eval(tr,rs);
        }
        public override string ToString()
        {
            return "GROUPING(..)";
        }
    }
    /// <summary>
    /// During Sources analysis, references to table columns and subquery selects are fixed.
    /// During Selects analysis, references to enclosing queries and local variables are fixed.
    /// </summary>
    internal class SqlName : SqlValue
    {
        internal SqlName next = null;
        internal Context ctx; 
        internal int sdepth = -1; // -1 is undefined. sdepth is location of the referenced Column or Variable
        internal List<Ident> refs = new List<Ident>();
        internal SqlValue resolvedTo = null;
        internal SqlName(Transaction t, Ident nm)
            : base(cx, nm)
        {
            ctx = cx.context;
            Unresolved();
        }
        protected SqlName(Transaction t,SqlName v, ref ATree<long, SqlValue> vs) : base(v,ref vs)
        {
            ctx = cx.context;
            resolvedTo = v.resolvedTo; //.Copy(ref vs);
            if (resolvedTo==null)
                Unresolved();
        }
        void Unresolved()
        {
            var qs = ctx.SelQuery();
            if (qs == null)
                throw new DBException("42112", name.ToString());
            next = qs.unresolved;
            qs.unresolved = this;
        }
        internal override void Resolve(Transaction t,Query q)
        {
            // do nothing
        }
        internal override Context Ctx()
        {
            return ctx;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlName(tr,this, ref vs);
        }
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            return resolvedTo?.Conditions(tr, q, disj) ?? false;
        }
        internal override bool IsFrom(Transaction tr, Query q, bool ordered,Domain ut=null)
        {
            return resolvedTo?.IsFrom(tr, q, ordered,ut) ?? base.IsFrom(tr,q,ordered,ut);
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            if (resolvedTo is SqlTypeColumn stc)
                return stc.IsKnown(tr, tr.Ctx(stc.blockid) as Query);
            return false;
        }
        internal override SqlValue _ColsForRestView(Transaction tr, From gf, GroupSpecification gs, ref ATree<SqlValue, Ident> gfc, ref ATree<SqlValue, Ident> rem, ref ATree<Ident, bool?> reg,ref ATree<long,SqlValue> map)
        {
            return map[resolvedTo.sqid]??
                resolvedTo._ColsForRestView(tr, gf, gs, ref gfc, ref rem, ref reg, ref map);
        }
        internal override SqlValue Import(Transaction tr,Query q)
        {
            return q?.Lookup(tr,name) ?? this;
        }
        internal override SqlValue Operand()
        {
            return resolvedTo.Operand();
        }
        internal override SqlValue Export(Transaction tr, Query q)
        {
            return resolvedTo.Export(tr, q);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            var sv = tr.context.cur.MustFind(tr,name); // from the top of the stack
            nominalDataType = sv.nominalDataType;
        }
        internal override Ident name
        {
            get
            {
                return _name;
            }
        }
        internal SqlValue Resolved(Transaction t,Query q,SqlValue target)
        {
            q = q ?? ctx as Query;
            if (q!=null)
                Ident.Tree<SqlValue>.Add(ref q.defs, _name, target);
            nominalDataType = target.nominalDataType;
            resolvedTo = target;
            target.Needed(cx,Query.Need.selected);
            foreach (var n in refs)
                n.Set(target, cx);
            for (var qe = ctx.cur as Query; qe != null; qe = qe.enc)
                if (qe is SelectQuery qs)
                {
                    SqlName prev = null, nx = null;
                    for (var n = qs?.unresolved; n != null; prev = n, n = nx)
                    {
                        nx = n.next;
                        if (n == this)
                        {
                  /*          var iq = qs.names[name];
                            if (name.ident != target.name.ident && iq.HasValue)
                                qs.names.Replace(iq.Value, target.alias??target.name); */
                            name.Set(cx,target.name);
                            if (alias != null)
                            {
                                qs.AddMatchedPair(alias, name);
                                qs.AddMatchedPair(name, alias); // why do I need both?
                            }
                            if (prev == null)
                                qs.unresolved = next;
                            else
                                prev.next = next;
                            next = null;
                            break;
                        }
                    }
                }
            return this;
        }
        internal override bool _MatchExpr(Transaction t, SqlValue v)
        {
            return resolvedTo?._MatchExpr(cx, v) ?? false;
        }
        internal override TypedValue Eval(Context tr, RowSet rs)
        {
            return resolvedTo?.Eval(tr,rs)?? 
               (ctx as Query)?.row?.Get(name) ??
                throw new DBException("42112",name.ident);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return resolvedTo?.Eval(bmk) ??
               (ctx as Query)?.row?.Get(name) ??
                base.Eval(bmk);
        }
        internal override TypedValue Eval(GroupingRowSet grs, ABookmark<TRow, GroupRow> bm)
        {
            return resolvedTo?.Eval(grs, bm);
        }
        internal override void AddNeeds(GroupingRowSet.GroupInfo gi)
        {
            resolvedTo?.AddNeeds(gi);
        }
        internal override void _AddNeeds(List<TColumn> cs)
        {
            resolvedTo?._AddNeeds(cs);
        }
        internal override void AddNeeds(Transaction tr,Query q)
        {
            resolvedTo?.AddNeeds(tr,q);
        }
        internal override Selector GetColumn(Transaction tr,From q, Table t)
        {
            return (resolvedTo??ctx.MustFind(tr,name)).GetColumn(tr,q, t);
        }
        internal override bool Check(Transaction tr,GroupSpecification group)
        {
            return resolvedTo.Check(tr,group);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            resolvedTo = map[resolvedTo?.sqid ?? 0] ?? resolvedTo?.Replace(tr, cx, so, sv, ref map);
            return this;
        }
 /*       public override void ToString1(StringBuilder sb, Transaction c, From uf, Record ur, bool eflag, bool cms, ref string cm)
        {
            resolvedTo?.ToString1(sb, c, uf, ur, eflag, cms, ref cm);
        } */
        public override string ToString()
        {
            return name.ToString();
        }
    }
    internal class SqlCursor : Activation.Variable
    {
        internal CursorSpecification spec;
        internal SqlCursor(Transaction tr, CursorSpecification cs, Ident n) : base(tr, cs.nominalDataType, n)
        {
            spec = cs;
            Needs(tr,spec);
        }
        protected SqlCursor(SqlCursor v, ref ATree<long, SqlValue> vs) : base(v,ref vs)
        {
            spec = v.spec;
        }
        internal override TypedValue Eval(Context tr,RowSet rs) 
        {
            for (var b = spec.contexts1.First(); b != null; b = b.Next())
                ATree<string, Context>.Add(ref tr.context.contexts, b.key(), b.value());
            return new TContext(tr.Ctx(spec.blockid) as Query);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlCursor(this,ref vs);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var cs = BTree<string, Context>.Empty;
            return new SqlCursor(tr, spec.Copy(ref cs, ref map) as CursorSpecification, name);
        }
        public override string ToString()
        {
            return "Cursor "+name.ToString();
        }
    }
    internal class SqlCall : SqlValue
    {
        public CallStatement call;
        protected SqlCall(Transaction t, CallStatement c, Ident n=null) : base(cx,c.returnType,n)
        {
            call = c;
            Needs(c.parms);
        }
        protected SqlCall(SqlCall c, ref ATree<long, SqlValue> vs) :base(c,ref vs)
        {
            var cs = BTree<string, Context>.Empty;
            call = (CallStatement)c.call.Copy(ref cs,ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlCall(this,ref vs);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nc = call.Replace(tr, cx, so, sv, ref map);
            return (nc == call)? this: new SqlCall(tr, nc);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(RowSet rs)
        {
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].IsKnown(rs.tr, rs.qry))
                    call.parms[i].Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].IsKnown(tr, rs.qry))
                    call.parms[i].StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].IsKnown(tr, rs.qry))
                    call.parms[i]._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            var nulls = true;
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].SetReg(key) != null)
                    nulls = false;
            return nulls ? null : this;
        }
    }
    /// <summary>
    /// An SqlValue that is a procedure/function call or static method
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        /// <summary>
        /// construct a procedure call SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="c">the call statement</param>
        public SqlProcedureCall(Transaction t, CallStatement c)
            : base(cx, c, c.pname)
        {
            Resolve(tr,null);
            Needs(c.parms);
        }
        protected SqlProcedureCall(SqlProcedureCall p, ref ATree<long, SqlValue> vs) :base(p,ref vs)
        {
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlProcedureCall(this,ref vs);
        }
        /// <summary>
        /// analysis stage Selects(): set up the operands
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            if (call.proc == null)
            {
                var pn = call.pname.Suffix(call.parms.Length); // might already be okay if a defining position
                call.proc = tr.GetProcedure(pn, out Database db);
                call.database = db?.dbix??0;
                if (call.proc != null)
                    call.pname.Set(call.database,call.proc.defpos, Ident.IDType.Procedure);
                else
                {
                    call.returnType = d;
                    UDType ut = tr.GetDomain(call.var.nominalDataType, out Database dd) as UDType;
                    if (ut == null)
                        throw new DBException("42108", ((SqlName)call.var).name).Mix();
                    call.proc = ut.GetMethod(dd, pn);
                }
            }
            if (call.proc != null)
            {
                //              if (call.proc.Returns(call.database) == null)
                //              new Parser(transaction).ReparseFormals(call.database,call.proc);
                nominalDataType = call.proc.Returns(tr.Db(call.database));
            }
        }
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            try
            {
                return (call.proc.Exec(tr, call.database, call.pname, call.parms) is TypedValue tv)?
                    ((tv != TNull.Value)?tv:nominalDataType.New(tr)):null;
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return nominalDataType.New(tr);
            }
        }
        internal override void Eqs(Transaction tr,ref Adapters eqs)
        {
            if (tr.Db(call.database).objects[call.proc.invdefpos] is Procedure inv)
                eqs = eqs.Add(call.proc.defpos, call.parms[0].name.Defpos(), call.proc.defpos, inv.defpos);
            base.Eqs(tr,ref eqs);
        }
        internal override int ColFor(Query q)
        {
            if (call.parms.Length == 1)
                return call.parms[0].ColFor(q);
            return base.ColFor(q);
        }
        internal override bool IsFrom(Transaction tr,Query q,bool ordered,Domain ut=null)
        {
            var r = true;
            if (ordered && !call.proc.monotonic)
                r = false;
            for (var i = 0; i < call.parms.Length; i++)
            {
                var s = call.parms[i].IsFrom(tr,q,ordered,ut);
                r = r && s;
            }
            return r;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            var r = true;
            for (var i = 0; i < call.parms.Length; i++)
            {
                var s = call.parms[i].IsKnown(tr, q);
                r = r && s;
            }
            return r;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nc = call.Replace(tr, cx, so, sv, ref map);
            return (nc == call)?  this: new SqlProcedureCall(tr, nc);
        }
        public override string ToString()
        {
            return call.pname + "(..)";
        }
    }
    /// <summary>
    /// A SqlValue that is evaluated by calling a method
    /// </summary>
    internal class SqlMethodCall : SqlCall // instance methods
    {
        public Transaction trans;
        public string blockid;
        public SqlMethodCall next = null; // for list of unknown Methods (See SelectQuery)
        /// <summary>
        /// construct a new MethodCall SqlValue.
        /// At construction time the proc and target will be unknown
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="c">the call statement</param>
        public SqlMethodCall(Transaction tr, CallStatement c)
            : base(tr,c)
        {
            trans = tr;
            blockid = tr.context.blockid;
            Needs(c.parms);
        }
        protected SqlMethodCall(SqlMethodCall mc, ref ATree<long, SqlValue> vs) :base(mc,ref vs)
        {
            trans = mc.trans;
            blockid = mc.blockid;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlMethodCall(this,ref vs);
        }
        /// <summary>
        /// analysis stage Selects(): setup the method call so it can be evaluated
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
   //         if (call.var != null && call.var is SqlName)
   //             call.var = (call.var.name.scope as Query)?.defs[call.var.name];
            if (call.proc == null)
            {
                var v = call.var;
                if (v != null)
                {
                    var ut = tr.GetDomain(v.nominalDataType, out Database db) as UDType;
                    call.database = db?.dbix??0;
                    if (ut == null)
                        throw new DBException("42108", v.nominalDataType.name).Mix();
                    call.proc = ut.GetMethod(db, call.pname);
                    name.Set(call.database, call.proc.defpos,Ident.IDType.Method);
                }
                if (call.proc != null)
                    call.pname.Set(call.database, call.proc.defpos, Ident.IDType.Procedure);
            }
            if (call.proc != null)
            {
                var rk = call.proc.Returns(tr.Db(call.database));
                if (rk.Constrain(d) == null)
                    throw new DBException("42161", d, rk).Mix();
                call.returnType = rk;
                nominalDataType = rk;
            }
        }
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (call.var == null)
                throw new PEException("PE241");
            var needed = BTree<SqlValue, Ident>.Empty;
            return (((Method)call.proc).Exec(tr, call.database, call.returnType, call.var.nominalDataType as RowType, call.var, call.pname, call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : nominalDataType.New(tr)):null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nc = call.Replace(tr, cx, so, sv, ref map);
            return (nc == call)? this:new SqlMethodCall(tr, nc);
        }
        public override string ToString()
        {
            return call.pname.ToString()+"(..)";
        }
    }
    /// <summary>
    /// An SqlValue that is a constructor expression
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        /// <summary>
        /// the type
        /// </summary>
        public UDType ut;
        SqlRow sce = null;
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(Transaction t, UDType u, CallStatement c)
            : base(cx, c)
        {
            ut = u;
            Needs(c.parms);
        }
        /// <summary>
        /// Analysis stage Selects(): setup the constructor call so that we can call it
        /// </summary>
        /// <param name="q">the context</param>
        /// <param name="q">The required data type or null</param>
        /// <param name="s">a default select to use</param>
        /// <returns></returns>
        internal override void _Setup(Transaction t, Query q, Domain d)
        {
            sce = new SqlRow(cx, d);
            sce.Initialise(cx);
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Transaction tr, RowSet rs)
        {
            var dt = tr.Db(call.database).GetDataType(ut.defpos);
            var needed = BTree<SqlValue, Ident>.Empty;
            return (((Method)call.proc).Exec(tr, call.database, dt, dt, null, call.pname, call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : nominalDataType.New(tr)) : null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var nc = call.Replace(tr, cx, so, sv, ref map);
            return (nc == call)? this: new SqlConstructor(tr, ut, nc);
        }
        public override string ToString()
        {
            return call.pname+"(..)";
        }
    }
    /// <summary>
    /// An SqlValue corresponding to a default constructor call
    /// </summary>
    internal class SqlDefaultConstructor : SqlValue
    {
        /// <summary>
        /// the type
        /// </summary>
        public UDType ut;
        public int database;
        public SqlRow sce;
        /// <summary>
        /// the actual parameters
        /// </summary>
        public SqlValue[] ins = new SqlValue[0];
        /// <summary>
        /// the number of parameters
        /// </summary>
        public int Length { get { return ins.Length; }}
        /// <summary>
        /// construct a SqlValue default co9nstructor for a type
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="lk">the actual parameters</param>
        public SqlDefaultConstructor(Transaction t, int db, UDType u, SqlValue[] lk)
            : base(cx,cx.Db(db).GetDataType(u.defpos))
        {
            database = db;
            ut = u;
            ins = lk;
            Needs(lk);
        }
        /// <summary>
        /// analysis stage Selects(): setup the operands
        /// </summary>
        /// <param name="q">The required data type or null</param>
        /// <param name="s">a default select</param>
        /// <returns>a possibly modified new SqlValue</returns>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            var dt = tr.Db(database).GetDataType(ut.defpos);
            sce = new SqlRow(tr,dt).Initialise(tr);
            for (int j = 0; j < Length; j++)
                Setup(tr,q,ins[j],dt.columns[j]);
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var dt = tr.Db(database).GetDataType(ut.defpos);
            try
            {
                var obs = new TypedValue[Length];
                for (int i = 0; i < Length; i++)
                    obs[i] = ins[i].Eval(tr,rs);
                return new TRow(tr, dt, obs);
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return dt.New(tr);
            }
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var dt = tr.Db(database).GetDataType(ut.defpos);
            try
            {
                var obs = new TypedValue[Length];
                for (int i = 0; i < Length; i++)
                    obs[i] = ins[i].Eval(bmk);
                return new TRow(tr, dt, obs);
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return dt.New(tr);
            }
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var ni = new SqlValue[ins.Length];
            var changed = false;
            for (var i=0;i<ins.Length;i++)
            {
                ni[i] = ins[i].Replace(tr, cx, so, sv, ref map);
                if (ni[i] != ins[i])
                    changed = true;
            }
            return changed ? new SqlDefaultConstructor(tr, database, ut, ni) : this;
        }
        public override string ToString()
        {
            return nominalDataType.name+"(..)";
        }
     }
    /// <summary>
    /// A built-in SQL function
    /// </summary>
    internal class SqlFunction : SqlValue
    {
        /// <summary>
        /// the query
        /// </summary>
        internal Query query = null;
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod;
        /// <summary>
        /// the value parameter for the function
        /// </summary>
        public SqlValue val = null;
        /// <summary>
        /// operands for the function
        /// </summary>
        public SqlValue op1 = null, op2 = null;
        /// <summary>
        /// a Filter for the function
        /// </summary>
        public SqlValue filter = null;
        /// <summary>
        /// a name for the window for a window function
        /// </summary>
        public Ident windowName = null;
        /// <summary>
        /// the window for a window function
        /// </summary>
        public WindowSpecification window = null;
        /// <summary>
        /// Check for monotonic
        /// </summary>
        public bool monotonic = false;
        /// <summary>
        /// For Window Function evaluation
        /// </summary>
        bool valueInProgress = false;
        protected class Register
        {
            /// <summary>
            /// The current partition: type is window.partitionType
            /// </summary>
            internal TRow profile = null;
            /// <summary>
            /// The RowSet for helping with evaluation if window!=null.
            /// Belongs to this partition, computed at RowSets stage of analysis 
            /// for our enclosing parent QuerySpecification (source).
            /// </summary>
            internal OrderedRowSet wrs = null;
            /// <summary>
            /// The bookmark for the current row
            /// </summary>
            internal RowBookmark wrb = null;
            /// the result of COUNT
            /// </summary>
            internal long count = 0L;
            /// <summary>
            /// the results of MAX, MIN, FIRST, LAST, ARRAY
            /// </summary>
            internal TypedValue acc;
            /// <summary>
            /// the results of XMLAGG
            /// </summary>
            internal StringBuilder sb = null;
            /// <summary>
            ///  the sort of sum/max/min we have
            /// </summary>
            internal Domain sumType = Domain.Content;
            /// <summary>
            ///  the sum of long
            /// </summary>
            internal long sumLong;
            /// <summary>
            /// the sum of INTEGER
            /// </summary>
            internal Integer sumInteger = null;
            /// <summary>
            /// the sum of double
            /// </summary>
            internal double sum1, acc1;
            /// <summary>
            /// the sum of Decimal
            /// </summary>
            internal Common.Numeric sumDecimal;
#if OLAP
        /// <summary>
        /// some other accumulators
        /// </summary>
        protected double sum2, acc2, acc3;
#endif
            /// <summary>
            /// the boolean result so far
            /// </summary>
            internal bool bval;
            /// <summary>
            /// a multiset for accumulating things
            /// </summary>
            internal TMultiset mset = null;
            internal void Clear()
            {
                count = 0; acc = TNull.Value; sb = null;
                sumType = Domain.Content; sumLong = 0;
                sumInteger = null; sum1 = 0.0; acc1 = 0.0;
                sumDecimal = Numeric.Zero;
            }
        }
        protected Register cur = new Register();
        ATree<TRow, Register> regs = null;
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(Transaction t, Sqlx f, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m, Domain dt=null) : 
            base(cx,_Type(f,vl,o1,dt),new Ident(f.ToString(),0)) 
        {
            query = cx.context.cur;
            kind = f;
            val = vl;
            op1 = o1;
            op2 = o2;
            mod = m;
            Needs(val, op1, op2);
        }
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(Transaction t, Sqlx f, Domain dt, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m) :
            base(cx, dt)
        {
            query = cx.context.cur;
            kind = f;
            val = vl;
            op1 = o1;
            op2 = o2;
            mod = m;
            Needs(val, op1, op2);
        }
        protected SqlFunction(SqlFunction v, ref ATree<long, SqlValue> vs) :base(v,ref vs)
        {
            query = v.query;
            kind = v.kind;
            val = v.val?.Copy(ref vs);
            op1 = v.op1?.Copy(ref vs);
            op2 = v.op2?.Copy(ref vs);
            mod = v.mod;
            filter = v.filter?.Copy(ref vs);
            windowName = v.windowName;
            window = v.window;
            regs = v.regs;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new SqlFunction(this,ref vs);
        }
        internal override void Needed(Transaction tr,Query.Need n)
        {
            switch (kind)
            {
                default:
                    base.Needed(tr,n);
                    return;
                case Sqlx.ANY:
                case Sqlx.AVG:
                case Sqlx.COLLECT:
                case Sqlx.COUNT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECT:
                case Sqlx.MAX:
                case Sqlx.MIN:
                case Sqlx.SOME:
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
#if OLAP
                case Sqlx.VAR_POP:
                case VAR_SAMP:
#endif
                case Sqlx.SUM:
                case Sqlx.XMLAGG:
                    return;
            }
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            if (aggregates0())
                return this;
            return new SqlFunction(tr, kind, val?.Import(tr, q), op1?.Import(tr, q), op2?.Import(tr, q), mod, nominalDataType)
            { windowName = windowName, window = window };
        }
        internal override SqlValue Export(Transaction tr, Query q)
        {
            return new SqlTypeColumn(tr, nominalDataType, alias ?? new Ident("C_" + sqid, 0), false, false, q);
        }
        internal override void AddNeeds(GroupingRowSet.GroupInfo gi)
        {
            val?.AddNeeds(gi);
        }
        internal override void AddNeeds(Transaction tr,Query q)
        {
            if (!aggregates0())
                val?.AddNeeds(tr,q); 
        }
        internal override SqlValue Operand()
        {
            if (aggregates0())
                return val;
            return val?.Operand();
        }
        internal override SqlValue For(Transaction tr, Query q)
        {
            if (window != null)
                return null;
            return base.For(tr, q);
        }
        /// <summary>
        /// analysis stage Selects(): set up operands
        /// </summary>
        /// <param name="d">The required data type or null</param>
        /// <param name="s">a default select</param>
        /// <returns></returns>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            if (query == null)
                query = tr.context.cur as Query;
            switch (kind)
            {
                case Sqlx.ABS:
                    Setup(tr,q,val,FindType(Domain.UnionNumeric)); break;
                case Sqlx.ANY:
                    Setup(tr, q, val, Domain.Bool); break;
                case Sqlx.AVG:
                    Setup(tr, q, val, FindType(Domain.UnionNumeric)); break;
                case Sqlx.ARRAY:
                    Setup(tr, q, val, Domain.Collection); break; // Mongo $push
                case Sqlx.CARDINALITY:
                    Setup(tr, q, val, Domain.Int); break;
                case Sqlx.CASE:
                    Setup(tr, q, val, d);
                    Setup(tr, q, op1,val.nominalDataType);
                    Setup(tr, q, op2,val.nominalDataType); break;
                case Sqlx.CAST:
                    Setup(tr, q, val,val.nominalDataType);
                    monotonic = ((SqlTypeExpr)op1).type.kind != Sqlx.CHAR && val.nominalDataType.kind != Sqlx.CHAR;
                    break;
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    Setup(tr, q, val,FindType(Domain.UnionNumeric)); break;
                case Sqlx.CHAR_LENGTH:
                    Setup(tr, q, val,Domain.Char); break;
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK: break;
                case Sqlx.COLLECT:
                    Setup(tr, q, val,Domain.Collection); break;
#if OLAP
                case Sqlx.CORR: needs = needs.Add(Setup(val,Domain.Real)); break;
#endif
                case Sqlx.COUNT: Setup(tr, q, val,Domain.Int);
                    break;
#if OLAP
                case Sqlx.COVAR_POP: goto case Sqlx.CORR;
                case Sqlx.COVAR_SAMP: goto case Sqlx.CORR;
                case Sqlx.CUME_DIST: goto case Sqlx.CORR;
#endif
                case Sqlx.CURRENT_DATE: monotonic = true;  break;
                case Sqlx.CURRENT_TIME: monotonic = true; break;
                case Sqlx.CURRENT_TIMESTAMP: monotonic = true; break;
#if OLAP
                case Sqlx.DENSE_RANK: needs = needs.Add(Setup(val,FindType(Domain.UnionNumeric))); break;
#endif
                case Sqlx.ELEMENT:
                    Setup(tr, q, val,Domain.Collection); break;
                case Sqlx.EXP:
                    Setup(tr, q, val, Domain.Real); monotonic = true;  break;
                case Sqlx.EVERY: goto case Sqlx.ANY;
                case Sqlx.EXTRACT:
                    Setup(tr,q,val,Domain.UnionDate); monotonic = (mod == Sqlx.YEAR); break;
                case Sqlx.FLOOR: goto case Sqlx.CEIL;
                case Sqlx.FIRST:
                    Setup(tr, q, val, Domain.Content); break; // Mongo
                case Sqlx.FUSION: goto case Sqlx.COLLECT;
                case Sqlx.INTERSECTION: goto case Sqlx.COLLECT;
                case Sqlx.LAST: goto case Sqlx.FIRST;
                case Sqlx.SECURITY:
                    if (tr.db._User.defpos != tr.db.owner)
                        throw new DBException("42105", this);
                    Setup(tr, q, val, Domain.Level); break;
                case Sqlx.LN: goto case Sqlx.EXP;
                case Sqlx.LOCALTIME: goto case Sqlx.CURRENT_TIME;
                case Sqlx.LOCALTIMESTAMP: goto case Sqlx.CURRENT_TIMESTAMP;
                case Sqlx.LOWER:
                    Setup(tr, q, val,Domain.Char); break;
                case Sqlx.MAX:
                    Setup(tr, q, val,Domain.Content); break;
                case Sqlx.MIN: goto case Sqlx.MAX;
                case Sqlx.MOD:
                    Setup(tr, q, op1,FindType(Domain.UnionNumeric));
                    Setup(tr, q, op2,op1.nominalDataType); break;
                case Sqlx.NEXT: break;
                case Sqlx.NORMALIZE:
                    Setup(tr, q, val,Domain.Char); break;
                case Sqlx.NULLIF: Setup(tr, q, op1,Domain.Content);
                    Setup(tr, q, op2,op1.nominalDataType); break;
#if SIMILAR
                case Sqlx.OCCURRENCES_REGEX: goto case Sqlx.POSITION_REGEX;
#endif
                case Sqlx.OCTET_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.OVERLAY: goto case Sqlx.NORMALIZE;
#if OLAP
                case Sqlx.PERCENT_RANK: goto case Sqlx.CORR;
                case Sqlx.PERCENTILE_CONT: goto case Sqlx.CORR;
                case Sqlx.PERCENTILE_DISC: goto case Sqlx.CORR;
#endif
                case Sqlx.POSITION:
                    Setup(tr, q, op1, Domain.Char);
                    if (op1!=null)
                        Setup(tr, q, op2, op1.nominalDataType); break;
#if SIMILAR
                case Sqlx.POSITION_REGEX: Setup(val,Domain.Char);
                    Setup(op1,Domain.Regex);
                    Setup(op2,Domain.Collection);
                    break;
#endif
                case Sqlx.POWER: goto case Sqlx.EXP;
                case Sqlx.PROVENANCE: break;
                case Sqlx.RANK: 
                    Setup(tr,q,val,FindType(Domain.UnionNumeric)); break;
#if OLAP
                case Sqlx.REGR_AVGX: goto case Sqlx.CORR;
                case Sqlx.REGR_AVGY: goto case Sqlx.CORR;
                case Sqlx.REGR_COUNT: 
                    Setup(tr,q,val,Domain.Real); break;
                case Sqlx.REGR_R2: goto case Sqlx.CORR;
                case Sqlx.REGR_SLOPE: goto case Sqlx.CORR;
                case Sqlx.REGR_SXX: goto case Sqlx.CORR;
                case Sqlx.REGR_SXY: goto case Sqlx.CORR;
                case Sqlx.REGR_SYY: goto case Sqlx.CORR;
#endif
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.SET: goto case Sqlx.COLLECT;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.STDDEV_POP:  Setup(tr,q,val, Domain.Real); break;
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUBSTRING:
                    Setup(tr, q, val,Domain.Char);
                    Setup(tr, q, op1,Domain.Int);
                    Setup(tr, q, op2,Domain.Int);
                    break;
#if SIMILAR
                case Sqlx.SUBSTRING_REGEX: Setup(val,Domain.Char);
                    Setup(op1,Domain.Regex);
                    Setup(op2,Domain.Collection);
                    break;
#endif
                case Sqlx.SUM:
                    Setup(tr, q, val,FindType(Domain.UnionNumeric)); break;
                case Sqlx.TRANSLATE:
                    Setup(tr, q, val,Domain.Char); break;
#if SIMILAR
                case Sqlx.TRANSLATE_REGEX: goto case Sqlx.SUBSTRING_REGEX;
#endif
                case Sqlx.TRIM:
                    Setup(tr, q, val,Domain.Char);
                    Setup(tr, q, op1,val.nominalDataType); break;
                case Sqlx.TYPE_URI: Setup(tr, q, op1,Domain.Content);
                    break;
                case Sqlx.UPPER: goto case Sqlx.LOWER;
#if OLAP
                case Sqlx.VAR_POP: goto case Sqlx.CORR;
                case Sqlx.VAR_SAMP: goto case Sqlx.CORR;
#endif
                case Sqlx.WHEN:
                    Setup(tr, q, val,d);
                    Setup(tr, q, op1,Domain.Bool);
                    Setup(tr, q, op2,val.nominalDataType); break;
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLAGG: goto case Sqlx.LOWER;
                case Sqlx.XMLCOMMENT: goto case Sqlx.LOWER;
                case Sqlx.XMLPI: goto case Sqlx.LOWER;
                //      case Sqlx.XMLROOT: goto case Sqlx.LOWER;
                case Sqlx.XMLQUERY: Setup(tr, q, op1,Domain.Char);
                    Setup(tr, q, op2,op1.nominalDataType); break;
            }
            var tx = query as TableExpression;
            var qs = query as QuerySpecification;
            if (qs != null)
                tx = qs.tableExp;
            if (tx!=null && windowName!=null)
            {
                window = tx.window[windowName];
                if (window == null)
                    throw new DBException("42161", windowName).Mix();
            }
            else if (tx!=null && window != null)
            {
                windowName = tr.db.Genuid(0);
                if (tx.window == null)
                    tx.window = Ident.Tree<WindowSpecification>.Empty;
                Ident.Tree<WindowSpecification>.AddNN(ref tx.window, windowName, window);
            }
        }
        /// <summary>
        /// conditions processing
        /// </summary>
        /// <param name="q">the context</param>
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            Setup(tr, q, this, Domain.Bool);
            return false;
        }
        /// <summary>
        /// Prepare Window Function evaluation
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal void RowSets(Transaction tr, Query q)
        {
            // we first compute the needs of this window function
            // The key for registers will consist of partition/grouping columns
            // Each register has a rowset ordered by the order columns if any
            // for the moment we just use the whole source row
            // We build all of the WRS's at this stage for saving in f
            window.ordType = window.order.KeyType(q.nominalDataType, window.partition);
            window.order.keyType = null;
            window.partitionType = window.order.KeyType(q.nominalDataType, 0, window.partition);
            window.order.keyType = null;
        }
        internal override void Build(RowSet rs)
        {
            if (window == null || regs!=null)
                return;
            window.building = true;
            regs = new CTree<TRow, Register>(rs.tr, window.partitionType);
            var ob = rs.qry.row;
            for (var b = rs.First();b!=null;b=b.Next())
            {
                PRow ks = null;
                for (var i = window.partition - 1;i>=0; i--)
                    ks = new PRow(window.order[i].what.Eval(b),ks);
                var pkey = new TRow(window.partitionType, ks);
                cur = regs[pkey];
                ks = null;
                for (var i = window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order[i].what.Eval(b), ks);
                var okey = new TRow(window.ordType, ks);
                var worder = new OrderSpec();
                for (var i = window.partition; i < window.order.items.Count; i++)
                    worder.items.Add(window.order.items[i]);
                worder.keyType = window.ordType;
                if (cur ==null)
                {
                    cur = new Register();
                    ATree<TRow, Register>.Add(ref regs, pkey, cur);
                    cur.wrs = new OrderedRowSet(rs.qry, rs, worder, false);
                    cur.wrs.tree = new RTree(rs, new TreeInfo(window.ordType, TreeBehaviour.Allow, TreeBehaviour.Allow));
                }
                var dt = rs.qry.nominalDataType;
                var vs = new TypedValue[dt.Length];
                for (var i = 0; i < dt.Length; i++)
                    vs[i] = rs.qry.cols[i].Eval(b);
                RTree.Add(ref cur.wrs.tree, okey,new TRow(dt,vs));
            }
            rs.qry.row = ob;
            window.building = false;
        }
        /// <summary>
        /// See if two current values match as expressions
        /// </summary>
        /// <param name="v">one SqlValue</param>
        /// <param name="w">another SqlValue</param>
        /// <returns>whether they match</returns>
        static bool MatchExp(Transaction t,SqlValue v, SqlValue w)
        {
            return v?.MatchExpr(cx,w) ?? w == null;
        }
        /// <summary>
        /// Check SqlValues for structural matching
        /// </summary>
        /// <param name="v">another sqlValue</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Transaction t,SqlValue v)
        {
            if (v is SqlName w)
                v = w.resolvedTo;
            return (v is SqlFunction f && (nominalDataType == null || nominalDataType == v.nominalDataType)) &&
             MatchExp(cx,val, f.val) && MatchExp(cx,op1, f.op1) && MatchExp(cx,op2, f.op2);
        }
        internal override bool IsFrom(Transaction tr,Query q,bool ordered,Domain ut=null)
        {
            var r = true;
            if (kind == Sqlx.COUNT && mod == Sqlx.TIMES)
            {
                ATree<Ident, Query.Need>.Add(ref q.needs, name, Query.Need.selected);
                return true;
            }
            if (ordered && !monotonic)
                r = false;
            if (val != null)
            {
                var s = val.IsFrom(tr, q, ordered,ut);
                r = r && s;
            }
            if (op1 != null)
            {
                var s = op1.IsFrom(tr,q,ordered,ut);
                r = r && s;
            }
            if (op2 != null)
            {
                var s = op2.IsFrom(tr,q,ordered,ut);
                r = r && s;
            }
            return r;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
  //          if (aggregates0() && !query.IsKnown(tr,q))
  //              return false;
            var r = true;
            if (kind == Sqlx.COUNT && mod == Sqlx.TIMES)
            {
                ATree<Ident, Query.Need>.Add(ref q.needs, name, Query.Need.selected);
                return true;
            }
            if (val != null)
            {
                var s = val.IsKnown(tr, q);
                r = r && s;
            }
            if (op1 != null)
            {
                var s = op1.IsKnown(tr, q);
                r = r && s;
            }
            if (op2 != null)
            {
                var s = op2.IsKnown(tr, q);
                r = r && s;
            }
            return r;
        }
        internal override bool Grouped(GroupSpecification gs)
        {
            return base.Grouped(gs) || ((!aggregates0()) && val.Grouped(gs));
        }
        internal static Domain _Type(Sqlx kind,SqlValue val, SqlValue op1, Domain dt)
        {
            switch (kind)
            {
                case Sqlx.ABS: return Domain.UnionNumeric;
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AT: return Domain.Content;
                case Sqlx.AVG: return Domain.UnionNumeric;
                case Sqlx.ARRAY: return Domain.Collection; // Mongo $push
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CASE: return val.nominalDataType;
                case Sqlx.CAST: return ((SqlTypeExpr)op1).type;
                case Sqlx.CEIL: return Domain.UnionNumeric;
                case Sqlx.CEILING: return Domain.UnionNumeric;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: return Domain.Char;
                case Sqlx.COLLECT: return Domain.Collection;
#if OLAP
                case Sqlx.CORR: return Domain.Real;
#endif
                case Sqlx.COUNT: return Domain.Int;
#if OLAP
                case Sqlx.COVAR_POP: return Domain.Real;
                case Sqlx.COVAR_SAMP: return Domain.Real;
                case Sqlx.CUME_DIST: return Domain.Real;
#endif
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
#if OLAP
                case Sqlx.DENSE_RANK: return Domain.Int;
#endif
                case Sqlx.ELEMENT: return val.nominalDataType.elType;
                case Sqlx.FIRST: return Domain.Content;
                case Sqlx.EXP: return Domain.Real;
                case Sqlx.EVERY: return Domain.Bool;
                case Sqlx.EXTRACT: return Domain.Int;
                case Sqlx.FLOOR: return Domain.UnionNumeric;
                case Sqlx.FUSION: return Domain.Collection;
                case Sqlx.INTERSECTION: return Domain.Collection;
                case Sqlx.LAST: return Domain.Content;
                case Sqlx.SECURITY: return Domain.Level;
                case Sqlx.LN: return Domain.Real;
                case Sqlx.LOCALTIME: return Domain.Timespan;
                case Sqlx.LOCALTIMESTAMP: return Domain.Timestamp;
                case Sqlx.LOWER: return Domain.Char;
                case Sqlx.MAX: return Domain.Content;
                case Sqlx.MIN: return Domain.Content;
                case Sqlx.MOD: return Domain.UnionNumeric;
                case Sqlx.NEXT: return Domain.UnionDate;
                case Sqlx.NORMALIZE: return Domain.Char;
                case Sqlx.NULLIF: return op1.nominalDataType;
#if SIMILAR
                case Sqlx.OCCURRENCES_REGEX: return Domain.Int;
#endif
                case Sqlx.OCTET_LENGTH: return Domain.Int;
                case Sqlx.OVERLAY: return Domain.Char;
                case Sqlx.PARTITION: return Domain.Char;
#if OLAP
                case Sqlx.PERCENT_RANK: return Domain.Real;
                case Sqlx.PERCENTILE_CONT: return Domain.Real;
                case Sqlx.PERCENTILE_DISC: return Domain.Real;
#endif
                case Sqlx.POSITION: return Domain.Int;
#if SIMILAR
                case Sqlx.POSITION_REGEX: return Domain.Int;
#endif
                case Sqlx.PROVENANCE: return Domain.Char;
                case Sqlx.POWER: return Domain.Real;
                case Sqlx.RANK: return Domain.Int;
#if OLAP
                case Sqlx.REGR_AVGX: return Domain.Real;
                case Sqlx.REGR_AVGY: return Domain.Real;
                case Sqlx.REGR_COUNT: return Domain.Int;
                case Sqlx.REGR_R2: return Domain.Real;
                case Sqlx.REGR_SLOPE: return Domain.Real;
                case Sqlx.REGR_SXX: return Domain.Real;
                case Sqlx.REGR_SXY: return Domain.Real;
                case Sqlx.REGR_SYY: return Domain.Real;
#endif
                case Sqlx.ROW_NUMBER: return Domain.Int;
                case Sqlx.SET: return Domain.Collection;
#if SIMILAR
                case Sqlx.SIMILAR: return Domain.Bool;
#endif
                case Sqlx.STDDEV_POP: return Domain.Real;
                case Sqlx.STDDEV_SAMP: return Domain.Real;
                case Sqlx.SUBSTRING: return Domain.Char;
#if SIMILAR
                case Sqlx.SUBSTRING_REGEX: return Domain.Char;
#endif
                case Sqlx.SUM: return Domain.UnionNumeric;
                case Sqlx.TRANSLATE: return Domain.Char;
#if SIMILAR
                case Sqlx.TRANSLATE_REGEX: return Domain.Char;
#endif
                case Sqlx.TYPE_URI: return Domain.Char;
                case Sqlx.TRIM: return Domain.Char;
                case Sqlx.UPPER: return Domain.Char;
#if OLAP
                case Sqlx.VAR_POP: return Domain.Real;
                case Sqlx.VAR_SAMP: return Domain.Real;
#endif
                case Sqlx.VERSIONING: return Domain.Int;
                case Sqlx.WHEN: return val.nominalDataType;
                case Sqlx.XMLCAST: return op1.nominalDataType;
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                //      case Sqlx.XMLROOT: goto case Sqlx.LOWER;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return dt;
        }
        internal override TypedValue Eval(Transaction tr, RowSet rs)
        {
            var rb = rs.qry.row;
            RowBookmark firstTie = null;
            if (rb==null || window?.building == true)
                return null;
            if (query is Query q0 && tr.Ctx(q0.blockid) is Query q && ((q.rowSet is GroupingRowSet g && !g.building && g.g_rows == null) || q.rowSet is ExportedRowSet))
                    return q.row?.Get(alias ?? name);
            if (window != null)
            {
                if (valueInProgress)
                    return null;
                valueInProgress = true;
                PRow ks = null;
                for (var i = window.partition - 1; i >= 0; i--)
                    ks = new PRow(window.order[i].what.Eval(rb), ks);
                cur = regs[new TRow(window.partitionType,ks)];
                cur.Clear();
                cur.wrs.building = false; // ? why should it be different?
                ks = null;
                for (var i = window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order[i].what.Eval(rb), ks);
                var dt = rs.qry.nominalDataType;
                for (var b = firstTie = cur.wrs.PositionAt(ks); b != null; b = b.Next())
                {
                    for (var i=0;i<dt.Length;i++)
                    {
                        var n = dt.names[i];
                        if (rb.Get(n) is TypedValue tv && dt[i].Compare(tr, tv, b.Get(n)) != 0)
                            goto skip;
                    }
                    cur.wrb = b;
                    break;
                    skip:;
                }
                for (var b = cur.wrs.First(); b != null; b = b.Next())
                    if (InWindow(b))
                        switch (window.exclude)
                        {
                            case Sqlx.NO:
                                AddIn(b);
                                break;
                            case Sqlx.CURRENT:
                                if (b._pos != cur.wrb._pos)
                                    AddIn(b);
                                break;
                            case Sqlx.TIES:
                                if (!Ties(cur.wrb, b))
                                    AddIn(b);
                                break;
                        }
                valueInProgress = false;
            }
            TypedValue v = null;
            switch (kind)
            {
                case Sqlx.ABS:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            {
                                var w = v.ToLong();
                                return new TInt((w < 0L) ? -w : w);
                            }
                        case Sqlx.REAL:
                            {
                                var w = v.ToDouble();
                                if (w == null)
                                    return TNull.Value;
                                return new TReal((w.Value < 0.0) ? -w.Value : w.Value);
                            }
                        case Sqlx.NUMERIC:
                            {
                                Common.Numeric w = (Numeric)v.Val(tr);
                                return new TNumeric((w < Numeric.Zero) ? -w : w);
                            }
                        case Sqlx.UNION:
                            {
                                var cs = val.nominalDataType.columns;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.INTEGER)
                                        goto case Sqlx.INTEGER;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.NUMERIC)
                                        goto case Sqlx.NUMERIC;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.REAL)
                                        goto case Sqlx.REAL;
                                break;
                            }
                    }
                    break;
                case Sqlx.ANY: return TBool.For(cur.bval);
                case Sqlx.ARRAY: // Mongo $push
                    {
                        if (window == null || cur.mset == null || cur.mset.Count == 0)
                            return cur.acc;
                        cur.acc = new TArray(new Domain(Sqlx.ARRAY, cur.mset.tree?.First()?.key().dataType));
                        var ar = cur.acc as TArray;
                        for (var d = cur.mset.tree.First();d!= null;d=d.Next())
                            ar.list.Add(d.key());
                        return cur.acc;
                    }
                case Sqlx.AVG:
                    {
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.NUMERIC: return new TReal(cur.sumDecimal / new Common.Numeric(cur.count));
                            case Sqlx.REAL: return new TReal(cur.sum1 / cur.count);
                            case Sqlx.INTEGER:
                                if (cur.sumInteger != null)
                                    return new TReal(new Common.Numeric(cur.sumInteger, 0) / new Common.Numeric(cur.count));
                                return new TReal(new Common.Numeric(cur.sumLong) / new Common.Numeric(cur.count));
                        }
                        return nominalDataType.New(tr);
                    }
                case Sqlx.CARDINALITY:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CASE:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        SqlFunction f = this;
                        for (; ; )
                        {
                            SqlFunction fg = f.op2 as SqlFunction;
                            if (fg == null)
                                return f.op2?.Eval(tr,rs)??null;
                            if (f.op1.nominalDataType.Compare(tr, f.op1.Eval(tr,rs), v) == 0)
                                return f.val.Eval(tr,rs);
                            f = fg;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        return nominalDataType.Coerce(tr, v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Ceiling(v.ToDouble().Value));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Ceiling((Common.Numeric)v.Val(tr)));
                    }
                    break;
                case Sqlx.CHAR_LENGTH:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return nominalDataType.New(tr);
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK: return new TRvv(rs);
                case Sqlx.COLLECT: return nominalDataType.Coerce(tr, (TypedValue)cur.mset ??TNull.Value);
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
#if OLAP
                case Sqlx.CORR:goto case STDDEV_POP;
#endif
                case Sqlx.COUNT: return new TInt(cur.count);
#if OLAP
                case Sqlx.COVAR_POP:
                    {
                        if (count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Covariance Population");
                        double m1 = sum1 / count;
                        double m2 = sum2 / count;
                        return new TReal((acc3 - count * sum1 * m2 - count * m1 * sum2 + count * m1 * m2) / count);
                    }
                case Sqlx.COVAR_SAMP:
                    {
                        if (count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Covariance Sample");
                        double m1 = sum1 / count;
                        double m2 = sum2 / count;
                        return new TReal((acc3 - count * sum1 * m2 - count * m1 * sum2 + count * m1 * m2) / (count - 1));
                    }
                case Sqlx.CUME_DIST:
                    {
                        if (val == null)
                            return new TReal(((double)(1 + win.firstTie._pos - win.windowStart)) / win.windowCount);
                        for (win.firstTie = win.start; win.firstTie != null && win.order[win.partition].what.nominalDataType.Compare(cx, tb.key()[win.orderWindow],
                                val.Eval(cx)) < 0; win.firstTie = win.firstTie.Next() as RTreeBookmark)
                            ;
                        return new TReal(((double)win.firstTie?._pos) / win.windowCount);
                    }
#endif
                case Sqlx.CURRENT:
                    {
                        if (val.Eval(tr,rs) is TContext tc && tc.ctx is Query cs && rs.qry is Query qv)
                            return TBool.For(qv.row?._recpos == cs.row?._recpos);
                        break;
                    }
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE: return new TChar(tr.db._Role.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.CURRENT_USER: return new TChar(tr.db._User.name);
#if OLAP
                case Sqlx.DENSE_RANK:
                    {
                        if (val == null)
                            return new TInt(1 + win.cur._pos - win.windowStart);
                        for (win.firstTie = win.start; win.firstTie != null && win.order[win.partition].what.nominalDataType.Compare(cx,
                                win.firstTie.key()[win.orderWindow], val.Eval(cx)) < 0; win.firstTie = win.firstTie.Next() as RTreeBookmark)
                            ;
                        return new TInt(win.firstTie?._pos);
                    }
#endif
                case Sqlx.ELEMENT:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        if (m.Count != 1)
                            throw new DBException("21000").Mix();
                        return m.First().Value();
                    }
                case Sqlx.EXP:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    if (v == TNull.Value)
                        return nominalDataType.New(tr);
                    return new TReal(Math.Exp(v.ToDouble().Value));
                case Sqlx.EVERY:
                    {
                        object o = cur.mset.tree[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Sqlx.EXTRACT:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        switch (v.dataType.kind)
                        {
                            case Sqlx.DATE:
                                {
                                    DateTime dt = (DateTime)v.Val(tr);
                                    switch (mod)
                                    {
                                        case Sqlx.YEAR: return new TInt((long)dt.Year);
                                        case Sqlx.MONTH: return new TInt((long)dt.Month);
                                        case Sqlx.DAY: return new TInt((long)dt.Day);
                                        case Sqlx.HOUR: return new TInt((long)dt.Hour);
                                        case Sqlx.MINUTE: return new TInt((long)dt.Minute);
                                        case Sqlx.SECOND: return new TInt((long)dt.Second);
                                    }
                                    break;
                                }
                            case Sqlx.INTERVAL:
                                {
                                    Interval it = (Interval)v.Val(tr);
                                    switch (mod)
                                    {
                                        case Sqlx.YEAR: return new TInt(it.years);
                                        case Sqlx.MONTH: return new TInt(it.months);
                                        case Sqlx.DAY: return new TInt(it.ticks / TimeSpan.TicksPerDay);
                                        case Sqlx.HOUR: return new TInt(it.ticks / TimeSpan.TicksPerHour);
                                        case Sqlx.MINUTE: return new TInt(it.ticks / TimeSpan.TicksPerMinute);
                                        case Sqlx.SECOND: return new TInt(it.ticks / TimeSpan.TicksPerSecond);
                                    }
                                    break;
                                }
                        }
                        throw new DBException("42000", mod).ISO().Add(Sqlx.ROUTINE_NAME, new TChar("Extract"));
                    }
                case Sqlx.FIRST:  nominalDataType = cur.acc.dataType;  return cur.mset.tree.First().key();
                case Sqlx.FLOOR:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val(tr) == null)
                        return v;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble().Value));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val(tr)));
                    }
                    break;
                case Sqlx.FUSION: return nominalDataType.Coerce(tr, cur.mset);
                case Sqlx.INTERSECTION:return nominalDataType.Coerce(tr, cur.mset);
                case Sqlx.LAST: nominalDataType = cur.acc.dataType; return cur.mset.tree.Last().key();
                case Sqlx.LN:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val(tr) == null)
                        return v;
                    return new TReal(Math.Log(v.ToDouble().Value));
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return nominalDataType.New(tr);
                    }
                case Sqlx.MAX: return cur.acc;
                case Sqlx.MIN: return cur.acc;
                case Sqlx.MOD:
                    if (op1 != null)
                        v = op1.Eval(tr,rs);
                    if (v.Val(tr) == null)
                        return nominalDataType.New(tr);
                    switch (op1.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return new TInt(v.ToLong() % op2.Eval(tr,rs).ToLong());
                        case Sqlx.NUMERIC:
                            return new TNumeric(((Numeric)v.Val(tr)) % (Numeric)op2.Eval(tr,rs).Val(tr));
                    }
                    break;
                case Sqlx.NORMALIZE:
                    if (val != null)
                        v = val.Eval(tr,rs);
                    return v; //TBD
                case Sqlx.NULLIF:
                    {
                        TypedValue a = op1.Eval(tr,rs)?.NotNull();
                        if (a == null)
                            return null;
                        if (a.IsNull)
                            return nominalDataType.New(tr);
                        TypedValue b = op2.Eval(tr,rs)?.NotNull();
                        if (b == null)
                            return null;
                        if (b.IsNull || op1.nominalDataType.Compare(tr, a, b) != 0)
                            return a;
                        return nominalDataType.New(tr);
                    }
#if SIMILAR
                case Sqlx.OCCURRENCES_REGEX:
                    return new TInt(
                        RegEx.XPathParse(op1 as RegularExpression).Occurrences(val.Eval(cx).ToString(), op2 as RegularExpressionParameters));
#endif
                case Sqlx.OCTET_LENGTH:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.Val(tr) is byte[] bytes)
                            return new TInt(bytes.Length);
                        return nominalDataType.New(tr);
                    }
                case Sqlx.OVERLAY:
                    v = val?.Eval(tr,rs)?.NotNull();
                    return v; //TBD
                case Sqlx.PARTITION:
                        return TNull.Value;
                case Sqlx.POSITION:
                    {
                        if (op1 != null && op2 != null)
                        {
                            string t = op1.Eval(tr,rs)?.ToString();
                            string s = op2.Eval(tr,rs)?.ToString();
                            if (t != null && s != null)
                                return new TInt(s.IndexOf(t));
                            return nominalDataType.New(tr);
                        }
                        return TNull.Value;
                    }
#if SIMILAR
                case Sqlx.POSITION_REGEX:
                    return new TInt(RegEx.XPathParse(op1 as RegularExpression).Position(val.Eval(cx).ToString(), op2 as RegularExpressionParameters));
#endif
                case Sqlx.POWER:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        var w = op1?.Eval(tr,rs)?.NotNull();
                        if (w == null)
                            return null;
                        if (v.IsNull)
                            return nominalDataType.New(tr);
                        return new TReal(Math.Pow(v.ToDouble().Value, w.ToDouble().Value));
                    }
                case Sqlx.PROVENANCE:
                    return TNull.Value;
                case Sqlx.RANK:
                    return new TInt(firstTie._pos + 1);
#if OLAP
                case Sqlx.REGR_AVGX:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal(sum1 / count);
                    }
                case Sqlx.REGR_AVGY:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal(sum2 / count);
                    }
                case Sqlx.REGR_COUNT: return new TInt(count);
                case Sqlx.REGR_INTERCEPT:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((sum2 - sum1 * (count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1)) / count);
                    }
                case Sqlx.REGR_R2:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((count * acc3 - sum1 * sum2) * (count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1) / (count * acc2 - sum2 * sum2));
                    }
                case Sqlx.REGR_SLOPE:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1));
                    }
                case Sqlx.REGR_SXX: return new TReal(acc1);
                case Sqlx.REGR_SXY: return new TReal(acc3);
                case Sqlx.REGR_SYY: return new TReal(acc2);
#endif
                case Sqlx.ROW_NUMBER: return new TInt(cur.wrb._pos+1);
                case Sqlx.SECURITY: // classification pseudocolumn
                    return TLevel.New(rs.qry.row?.Rec()?.classification ?? Level.D);
                case Sqlx.SET:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set();
                    }
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.STDDEV_POP:
                    {
                        if (cur.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        double m = cur.sum1 / cur.count;
                        return new TReal(Math.Sqrt((cur.acc1 - 2 * cur.count * m + cur.count * m * m) / cur.count));
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (cur.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        double m = cur.sum1 / cur.count;
                        return new TReal(Math.Sqrt((cur.acc1 - 2 * cur.count * m + cur.count * m * m) / (cur.count - 1)));
                    }
                case Sqlx.SUBSTRING:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        string sv = v.ToString();
                        var w = op1?.Eval(tr,rs)??null;
                        if (sv == null || w == null)
                            return nominalDataType.New(tr);
                        var x = op2?.Eval(tr,rs)??null;
#if SIMILAR
                        if (mod == Sqlx.REGULAR_EXPRESSION)
                            return new TChar(RegEx.SQLParse(op1.Eval(cx).Val() as RegularExpression).Substring(val.Eval(cx).ToString(), null));
#endif
                        if (x == null)
                            return new TChar((w == null || w.IsNull) ? null : sv.Substring(w.ToInt().Value));
                        return new TChar(sv.Substring(w.ToInt().Value, x.ToInt().Value));
                    }
#if SIMILAR
                case Sqlx.SUBSTRING_REGEX:
                    return new TChar(RegEx.XPathParse(op1.Eval(cx).Val() as RegularExpression).Substring(val.Eval(cx).ToString(), op2.Eval(cx).Val() as RegularExpressionParameters));
#endif
                case Sqlx.SUM:
                    {
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.Null: return TNull.Value;
                            case Sqlx.NULL: return TNull.Value;
                            case Sqlx.REAL: return new TReal(cur.sum1);
                            case Sqlx.INTEGER:
                                if (cur.sumInteger != null)
                                    return new TInteger(cur.sumInteger);
                                else
                                    return new TInt(cur.sumLong);
                            case Sqlx.NUMERIC: return new TNumeric(cur.sumDecimal);
                        }
                        throw new PEException("PE68");
                    }
                case Sqlx.TRANSLATE:
                     v = val?.Eval(tr,rs)?.NotNull();
                    return v; // TBD
#if SIMILAR
                case Sqlx.TRANSLATE_REGEX:
                    return new TChar(RegEx.XPathParse(op1.Eval(cx).Val() as RegularExpression).Translate(val.Eval(cx).ToString(), op2.Eval(cx).Val() as RegularExpressionParameters));
#endif
                case Sqlx.TRIM:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.IsNull)
                            return nominalDataType.New(tr);
                        string sv = v.ToString();
                        object c = null;
                        if (op1 != null)
                        {
                            string s = op1.Eval(tr,rs).ToString();
                            if (s != null && s.Length > 0)
                                c = s[0];
                        }
                        if (c != null)
                            switch (mod)
                            {
                                case Sqlx.LEADING: return new TChar(sv.TrimStart((char)c));
                                case Sqlx.TRAILING: return new TChar(sv.TrimEnd((char)c));
                                case Sqlx.BOTH: return new TChar(sv.Trim((char)c));
                                default: return new TChar(sv.Trim((char)c));
                            }
                        else
                            switch (mod)
                            {
                                case Sqlx.LEADING: return new TChar(sv.TrimStart());
                                case Sqlx.TRAILING: return new TChar(sv.TrimEnd());
                                case Sqlx.BOTH: return new TChar(sv.Trim());
                            }
                        return new TChar(sv.Trim());
                    }
                case Sqlx.TYPE_URI: goto case Sqlx.PROVENANCE;
                case Sqlx.UPPER:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        if (!v.IsNull)
                            return new TChar(v.ToString().ToUpper());
                        return nominalDataType.New(tr);
                    }
#if OLAP
                case Sqlx.VAR_POP:
                    {
                        if (count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Variance Pop");
                        double m = sum1 / count;
                        return new TReal((acc1 - 2 * count * m + count * m * m) / count);
                    }
                case Sqlx.VAR_SAMP:
                    {
                        if (count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Variance Samp");
                        double m = sum1 / count;
                        return new TReal((acc1 - 2 * count * m + count * m * m) / (count - 1));
                    }
#endif
                case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var rv = (tr.context.cur as From)?.row._Rvv();
                        if (rv != null)
                            return new TInt(rv.off);
                        return TNull.Value;
                    }
                case Sqlx.WHEN: // searched case
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        TypedValue a = op1.Eval(tr,rs);
                        if (a == TBool.True)
                            return v;
                        return op2.Eval(tr,rs);
                    }
                case Sqlx.XMLAGG: return new TChar(cur.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        v = val?.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            return null;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        object a = op2?.Eval(tr,rs)?.NotNull();
                        object x = val?.Eval(tr,rs)?.NotNull();
                        if (a == null || x == null)
                            return null;
                        string n = XmlConvert.EncodeName(op1.Eval(tr,rs).ToString());
                        string r = "<" + n  + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                //	case Sqlx.XMLFOREST: break; not required
                //	case Sqlx.XMLPARSE: break; not required in this version
                case Sqlx.XMLPI:
                    v = val?.Eval(tr,rs)?.NotNull();
                    if (v == null)
                        return null;
                    return new TChar("<?" + v + " " + op1.Eval(tr,rs) + "?>");
                /*       case Sqlx.XMLROOT:
                           {
                               if (val != null)
                                   v = val.Value;
                               string doc = v.ToString();
                               int i = doc.IndexOf("?>");
                               if (i < 0)
                               {
                                   i = 0;
                                   doc = "?>" + doc;
                               }
                               string nd = "<?xml ";
                               if (op1 != null)
                                   nd += "version='" + op1.Value + "' ";
                               nd += "encoding='UTF-8' ";
                               if (op2 != null)
                                   nd += "standalone='" + ((bool)op1.Value ? "yes'" : "no'");
                               return nd + doc.Substring(i);
                           } */
#if !SILVERLIGHT && !WINDOWS_PHONE
                case Sqlx.XMLQUERY:
                    {
                        string doc = op1.Eval(tr,rs).ToString();
                        string pathexp = op2.Eval(tr,rs).ToString();
                        StringReader srdr = new StringReader(doc);
                        XPathDocument xpd = new XPathDocument(srdr);
                        XPathNavigator xn = xpd.CreateNavigator();
                        return new TChar((string)XmlFromXPI(xn.Evaluate(pathexp)));
                    }
#endif
                case Sqlx.MONTH:
                case Sqlx.DAY:
                case Sqlx.HOUR:
                case Sqlx.MINUTE:
                case Sqlx.SECOND:
                case Sqlx.YEAR:
                    {
                        v = val?.Eval(tr, rs)?.NotNull();
                        if (v == null)
                            return null;
                        return new TInt(Extract(tr, kind, v));
                    }
            }
            throw new DBException("42154", kind).Mix();
        }
        /// <summary>
        /// helper for the current value
        /// </summary>
        /// <param name="win">the current window</param>
        /// <returns>the value</returns>
        internal override TypedValue Eval(GroupingRowSet grs, ABookmark<TRow,GroupRow> bm)
        {
            Transaction tr = grs.tr;
            TypedValue v = null;
            SetReg(bm.key());
            switch (kind)
            {
                case Sqlx.ABS:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            {
                                var w = v.ToLong();
                                return new TInt((w < 0L) ? -w : w);
                            }
                        case Sqlx.REAL:
                            {
                                var w = v.ToDouble();
                                if (w == null)
                                    return TNull.Value;
                                return new TReal((w.Value < 0.0) ? -w.Value : w.Value);
                            }
                        case Sqlx.NUMERIC:
                            {
                                Common.Numeric w = (Numeric)v.Val(tr);
                                return new TNumeric((w < Numeric.Zero) ? -w : w);
                            }
                        case Sqlx.UNION:
                            {
                                var cs = val.nominalDataType.columns;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.INTEGER)
                                        goto case Sqlx.INTEGER;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.NUMERIC)
                                        goto case Sqlx.NUMERIC;
                                for (int i = 0; i < cs.Length; i++)
                                    if (cs[i].kind == Sqlx.REAL)
                                        goto case Sqlx.REAL;
                                break;
                            }
                    }
                    break;
                case Sqlx.AVG:
                    {
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.NUMERIC: return new TReal(cur.sumDecimal / new Common.Numeric(cur.count));
                            case Sqlx.REAL: return new TReal(cur.sum1 / cur.count);
                            case Sqlx.INTEGER:
                                if (cur.sumInteger != null)
                                    return new TReal(new Common.Numeric(cur.sumInteger, 0) / new Common.Numeric(cur.count));
                                return new TReal(new Common.Numeric(cur.sumLong) / new Common.Numeric(cur.count));
                        }
                        return nominalDataType.New(tr);
                    }
                case Sqlx.CARDINALITY:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CASE:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        SqlFunction f = this;
                        for (; ; )
                        {
                            SqlFunction g = f.op2 as SqlFunction;
                            if (g == null)
                                return f.op2?.Eval(grs, bm) ?? null;
                            if (f.op1.nominalDataType.Compare(tr, f.op1.Eval(grs, bm), v) == 0)
                                return f.val.Eval(grs, bm);
                            f = g;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        return ((SqlTypeExpr)op1).type.Coerce(tr, v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Ceiling(v.ToDouble().Value));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Ceiling((Common.Numeric)v.Val(tr)));
                    }
                    break;
                case Sqlx.CHAR_LENGTH:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return nominalDataType.New(tr);
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
#if OLAP
                case Sqlx.CORR:goto case STDDEV_POP;
#endif
                case Sqlx.COUNT: return new TInt(cur.count);
#if OLAP
                case Sqlx.COVAR_POP:
                    {
                        if (count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Covariance Population");
                        double m1 = sum1 / count;
                        double m2 = sum2 / count;
                        return new TReal((acc3 - count * sum1 * m2 - count * m1 * sum2 + count * m1 * m2) / count);
                    }
                case Sqlx.COVAR_SAMP:
                    {
                        if (count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Covariance Sample");
                        double m1 = sum1 / count;
                        double m2 = sum2 / count;
                        return new TReal((acc3 - count * sum1 * m2 - count * m1 * sum2 + count * m1 * m2) / (count - 1));
                    }
                case Sqlx.CUME_DIST:
                    {
                        if (val == null)
                            return new TReal(((double)(1 + win.firstTie._pos - win.windowStart)) / win.windowCount);
                        for (win.firstTie = win.start; win.firstTie != null && win.order[win.partition].what.nominalDataType.Compare(cx, tb.key()[win.orderWindow],
                                val.Eval(cx)) < 0; win.firstTie = win.firstTie.Next() as RTreeBookmark)
                            ;
                        return new TReal(((double)win.firstTie?._pos) / win.windowCount);
                    }
#endif
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE: return new TChar(tr.db._Role.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.CURRENT_USER: return new TChar(tr.db._User.name);
#if OLAP
                case Sqlx.DENSE_RANK:
                    {
                        if (val == null)
                            return new TInt(1 + win.cur._pos - win.windowStart);
                        for (win.firstTie = win.start; win.firstTie != null && win.order[win.partition].what.nominalDataType.Compare(cx,
                                win.firstTie.key()[win.orderWindow], val.Eval(cx)) < 0; win.firstTie = win.firstTie.Next() as RTreeBookmark)
                            ;
                        return new TInt(win.firstTie?._pos);
                    }
#endif
                case Sqlx.ELEMENT:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        if (m.Count != 1)
                            throw new DBException("21000").Mix();
                        return m.First().Value();
                    }
                case Sqlx.EXP:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    if (v == TNull.Value)
                        return nominalDataType.New(tr);
                    return new TReal(Math.Exp(v.ToDouble().Value));
                case Sqlx.EXTRACT:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        return new TInt(Extract(grs.tr, mod, v));
                    }
                case Sqlx.FLOOR:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val(tr) == null)
                        return v;
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble().Value));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val(tr)));
                    }
                    break;
                case Sqlx.LN:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val(tr) == null)
                        return v;
                    return new TReal(Math.Log(v.ToDouble().Value));
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return nominalDataType.New(tr);
                    }
                case Sqlx.MAX:
                    return cur.acc;
                case Sqlx.MIN:
                    return cur.acc;
                case Sqlx.MOD:
                    if (op1 != null)
                        v = op1.Eval(grs, bm);
                    if (v.Val(tr) == null)
                        return nominalDataType.New(tr);
                    switch (op1.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return new TInt(v.ToLong() % op2.Eval(grs, bm).ToLong());
                        case Sqlx.NUMERIC:
                            return new TNumeric(((Numeric)v.Val(tr)) % (Numeric)op2.Eval(grs, bm).Val(tr));
                    }
                    break;
                case Sqlx.NORMALIZE:
                    if (val != null)
                        v = val.Eval(grs, bm);
                    return v; //TBD
                case Sqlx.NULLIF:
                    {
                        TypedValue a = op1.Eval(grs, bm)?.NotNull();
                        if (a == null)
                            return null;
                        if (a.IsNull)
                            return nominalDataType.New(tr);
                        TypedValue b = op2.Eval(grs, bm)?.NotNull();
                        if (b == null)
                            return null;
                        if (b.IsNull || op1.nominalDataType.Compare(tr, a, b) != 0)
                            return a;
                        return nominalDataType.New(tr);
                    }
#if SIMILAR
                case Sqlx.OCCURRENCES_REGEX:
                    return new TInt(
                        RegEx.XPathParse(op1 as RegularExpression).Occurrences(val.Eval(cx).ToString(), op2 as RegularExpressionParameters));
#endif
                case Sqlx.OCTET_LENGTH:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.Val(tr) is byte[] bytes)
                            return new TInt(bytes.Length);
                        return nominalDataType.New(tr);
                    }
                case Sqlx.OVERLAY:
                    v = val?.Eval(grs, bm)?.NotNull();
                    return v; //TBD
                case Sqlx.PARTITION:
                    return TNull.Value;
#if OLAP
                case Sqlx.PERCENT_RANK:
                    {
                        if (win.windowCount <= 1)
                            throw new DBException("22003").ISO().Add(Sqlx.ROUTINE_NAME, "Percent Rank");
                        if (val == null)
                            return new TReal(((double)win.firstTie._pos - win.windowStart) / (win.windowCount - 1));
                        // Hypothetical set function. 
                        for (win.firstTie = win.start; win.firstTie != null && win.order[win.partition].what.nominalDataType.Compare(cx,
                                win.firstTie.key()[win.orderWindow], val.Eval(cx)) < 0; win.firstTie = win.firstTie.Next() as RTreeBookmark)
                            ;
                        return new TReal(((double)(win.firstTie?._pos - 1)) / (win.windowCount - 1));
                    }
                case Sqlx.PERCENTILE_CONT:
                    {
                        v = op1.Eval(cx);
                        if (v.IsNull)
                            throw new DBException("22003").ISO().Add(Sqlx.ROUTINE_NAME, "Percentile Count");
                        double d = v.ToDouble();
                        if (d < 0.0 || d > 1.0)
                            throw new DBException("22003").ISO().Add(Sqlx.ROUTINE_NAME, "Percentile Count");
                        d = d * (win.windowCount - 1);
                        long rowlit0 = (long)Math.Floor(d);
                        double factor = d - rowlit0;
                        var we = win.values.First();
                        long ctr = 0L;
                        for (; ctr < rowlit0 && we != null;we = we.Next())
                            ctr += we.value().ToLong().Value;
                        if (we == null)
                            return new TReal(0.0);
                        double t0y = we.key().ToDouble();
                        double t1y = t0y;
                        for (;we!=null;we= we.Next())
                            t1y = we.key().ToDouble();
                        return new TReal(t0y + factor * (t1y - t0y));
                    }
                case Sqlx.PERCENTILE_DISC:
                    {
                        v = op1.Eval(cx);
                        if (v.IsNull)
                            throw new DBException("22003").ISO().Add(Sqlx.ROUTINE_NAME, "Perecntile Disc");
                        double d = v.ToDouble();
                        if (d < 0.0 || d > 1.0)
                            throw new DBException("22003").ISO().Add(Sqlx.ROUTINE_NAME, "Perecntile Disc");
                        d = d * win.windowCount;
                        long rowlit0 = (long)Math.Floor(d);
                        var we = win.values.First();
                        long ctr = 0L;
                        for (;we!=null && ctr < rowlit0;we=we.Next())
                            ctr += we.value().ToLong().Value;
                        return we?.key();
                    }
#endif
                case Sqlx.POSITION:
                    {
                        if (op1 != null && op2 != null)
                        {
                            string t = op1.Eval(grs, bm)?.ToString();
                            string s = op2.Eval(grs, bm)?.ToString();
                            if (t != null && s != null)
                                return new TInt(s.IndexOf(t));
                            return nominalDataType.New(tr);
                        }
                        return TNull.Value;
                    }
#if SIMILAR
                case Sqlx.POSITION_REGEX:
                    return new TInt(RegEx.XPathParse(op1 as RegularExpression).Position(val.Eval(cx).ToString(), op2 as RegularExpressionParameters));
#endif
                case Sqlx.POWER:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        var w = op1?.Eval(grs, bm)?.NotNull();
                        if (w == null)
                            return null;
                        if (v.IsNull)
                            return nominalDataType.New(tr);
                        return new TReal(Math.Pow(v.ToDouble().Value, w.ToDouble().Value));
                    }
                case Sqlx.PROVENANCE:
                    {
                        return TNull.Value;
                    }
                case Sqlx.RANK:
                    {
                        if (val == null)
                            return new TInt(cur.wrb._pos+1);
                        // Hypothetical set function. 
                        PRow ks = null;
                        for (var i = window.order.items.Count - 1; i > window.partition; i--)
                        {
                            var j=bm.value().nominalDataType.ColFor(window.order.items[i].what.name);
                            ks = new PRow(bm.value()[j].Eval(grs,bm), ks);
                        }
                        var b = (RTreeBookmark)cur.wrs.PositionAt(ks);
                        return new TInt(b._pos+1);
                    }
#if OLAP
                case Sqlx.REGR_AVGX:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal(sum1 / count);
                    }
                case Sqlx.REGR_AVGY:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal(sum2 / count);
                    }
                case Sqlx.REGR_COUNT: return new TInt(count);
                case Sqlx.REGR_INTERCEPT:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((sum2 - sum1 * (count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1)) / count);
                    }
                case Sqlx.REGR_R2:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((count * acc3 - sum1 * sum2) * (count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1) / (count * acc2 - sum2 * sum2));
                    }
                case Sqlx.REGR_SLOPE:
                    {
                        if (count == 0)
                            return TNull.Value;
                        return new TReal((count * acc3 - sum1 * sum2) / (count * acc1 - sum1 * sum1));
                    }
                case Sqlx.REGR_SXX: return new TReal(acc1);
                case Sqlx.REGR_SXY: return new TReal(acc3);
                case Sqlx.REGR_SYY: return new TReal(acc2);
#endif
                case Sqlx.SET:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set();
                    }
                case Sqlx.STDDEV_POP:
                    {
                        if (cur.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        double m = cur.sum1 / cur.count;
                        return new TReal(Math.Sqrt((cur.acc1 - 2 * cur.count * m + cur.count * m * m) / cur.count));
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (cur.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        double m = cur.sum1 / cur.count;
                        return new TReal(Math.Sqrt((cur.acc1 - 2 * cur.count * m + cur.count * m * m) / (cur.count - 1)));
                    }
                case Sqlx.SUBSTRING:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        string sv = v.ToString();
                        var w = op1?.Eval(grs, bm) ?? null;
                        if (sv == null || w == null)
                            return nominalDataType.New(tr);
                        var x = op2?.Eval(grs, bm) ?? null;
#if SIMILAR
                        if (mod == Sqlx.REGULAR_EXPRESSION)
                            return new TChar(RegEx.SQLParse(op1.Eval(cx).Val() as RegularExpression).Substring(val.Eval(cx).ToString(), null));
#endif
                        if (x == null)
                            return new TChar((w == null || w.IsNull) ? null : sv.Substring(w.ToInt().Value));
                        return new TChar(sv.Substring(w.ToInt().Value, x.ToInt().Value));
                    }
#if SIMILAR
                case Sqlx.SUBSTRING_REGEX:
                    return new TChar(RegEx.XPathParse(op1.Eval(cx).Val() as RegularExpression).Substring(val.Eval(cx).ToString(), op2.Eval(cx).Val() as RegularExpressionParameters));
#endif
                case Sqlx.SUM:
                    {
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.Null: return TNull.Value;
                            case Sqlx.NULL: return TNull.Value;
                            case Sqlx.REAL: return new TReal(cur.sum1);
                            case Sqlx.INTEGER:
                                if (cur.sumInteger != null)
                                    return new TInteger(cur.sumInteger);
                                else
                                    return new TInt(cur.sumLong);
                            case Sqlx.NUMERIC: return new TNumeric(cur.sumDecimal);
                        }
                        throw new PEException("PE68");
                    }
                case Sqlx.TRANSLATE:
                    v = val?.Eval(grs, bm)?.NotNull();
                    return v; // TBD
#if SIMILAR
                case Sqlx.TRANSLATE_REGEX:
                    return new TChar(RegEx.XPathParse(op1.Eval(cx).Val() as RegularExpression).Translate(val.Eval(cx).ToString(), op2.Eval(cx).Val() as RegularExpressionParameters));
#endif
                case Sqlx.TRIM:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.IsNull)
                            return nominalDataType.New(tr);
                        string sv = v.ToString();
                        object c = null;
                        if (op1 != null)
                        {
                            string s = op1.Eval(grs, bm).ToString();
                            if (s != null && s.Length > 0)
                                c = s[0];
                        }
                        if (c != null)
                            switch (mod)
                            {
                                case Sqlx.LEADING: return new TChar(sv.TrimStart((char)c));
                                case Sqlx.TRAILING: return new TChar(sv.TrimEnd((char)c));
                                case Sqlx.BOTH: return new TChar(sv.Trim((char)c));
                                default: return new TChar(sv.Trim((char)c));
                            }
                        else
                            switch (mod)
                            {
                                case Sqlx.LEADING: return new TChar(sv.TrimStart());
                                case Sqlx.TRAILING: return new TChar(sv.TrimEnd());
                                case Sqlx.BOTH: return new TChar(sv.Trim());
                            }
                        return new TChar(sv.Trim());
                    }
                case Sqlx.TYPE_URI: goto case Sqlx.PROVENANCE;
                case Sqlx.UPPER:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        if (!v.IsNull)
                            return new TChar(v.ToString().ToUpper());
                        return nominalDataType.New(tr);
                    }
#if OLAP
                case Sqlx.VAR_POP:
                    {
                        if (count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Variance Pop");
                        double m = sum1 / count;
                        return new TReal((acc1 - 2 * count * m + count * m * m) / count);
                    }
                case Sqlx.VAR_SAMP:
                    {
                        if (count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, "Variance Samp");
                        double m = sum1 / count;
                        return new TReal((acc1 - 2 * count * m + count * m * m) / (count - 1));
                    }
#endif
                case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var rv = (tr.context.cur as From)?.row._Rvv();
                        if (rv != null)
                            return new TInt(rv.off);
                        return TNull.Value;
                    }
                case Sqlx.WHEN: // searched case
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        TypedValue a = op1.Eval(grs, bm);
                        if (a == TBool.True)
                            return v;
                        return op2.Eval(grs, bm);
                    }
                case Sqlx.XMLAGG:
                    return new TChar(cur.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        object a = op2?.Eval(grs, bm)?.NotNull();
                        object x = val?.Eval(grs, bm)?.NotNull();
                        if (a == null || x == null)
                            return null;
                        string n = XmlConvert.EncodeName(op1.Eval(grs, bm).ToString());
                        string r = "<" + n + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                //	case Sqlx.XMLFOREST: break; not required
                //	case Sqlx.XMLPARSE: break; not required in this version
                case Sqlx.XMLPI:
                    v = val?.Eval(grs, bm)?.NotNull();
                    if (v == null)
                        return null;
                    return new TChar("<?" + v + " " + op1.Eval(grs, bm) + "?>");
                /*       case Sqlx.XMLROOT:
                           {
                               if (val != null)
                                   v = val.Value;
                               string doc = v.ToString();
                               int i = doc.IndexOf("?>");
                               if (i < 0)
                               {
                                   i = 0;
                                   doc = "?>" + doc;
                               }
                               string nd = "<?xml ";
                               if (op1 != null)
                                   nd += "version='" + op1.Value + "' ";
                               nd += "encoding='UTF-8' ";
                               if (op2 != null)
                                   nd += "standalone='" + ((bool)op1.Value ? "yes'" : "no'");
                               return nd + doc.Substring(i);
                           } */
#if !SILVERLIGHT && !WINDOWS_PHONE
                case Sqlx.XMLQUERY:
                    {
                        string doc = op1.Eval(grs, bm).ToString();
                        string pathexp = op2.Eval(grs, bm).ToString();
                        StringReader srdr = new StringReader(doc);
                        XPathDocument xpd = new XPathDocument(srdr);
                        XPathNavigator xn = xpd.CreateNavigator();
                        return new TChar((string)XmlFromXPI(xn.Evaluate(pathexp)));
                    }
#endif
                case Sqlx.MONTH:
                case Sqlx.DAY:
                case Sqlx.HOUR:
                case Sqlx.MINUTE:
                case Sqlx.SECOND:
                case Sqlx.YEAR:
                    {
                        v = val?.Eval(grs, bm)?.NotNull();
                        if (v == null)
                            return null;
                        return new TInt(Extract(tr, kind, v));
                    }
            }
            throw new DBException("42154", kind).Mix();
        }
        long Extract(Transaction tr,Sqlx mod,TypedValue v)
        {
            switch (v.dataType.kind)
            {
                case Sqlx.DATE:
                    {
                        DateTime dt = (DateTime)v.Val(tr);
                        switch (mod)
                        {
                            case Sqlx.YEAR: return dt.Year;
                            case Sqlx.MONTH: return dt.Month;
                            case Sqlx.DAY: return dt.Day;
                            case Sqlx.HOUR: return dt.Hour;
                            case Sqlx.MINUTE: return dt.Minute;
                            case Sqlx.SECOND: return dt.Second;
                        }
                        break;
                    }
                case Sqlx.INTERVAL:
                    {
                        Interval it = (Interval)v.Val(tr);
                        switch (mod)
                        {
                            case Sqlx.YEAR: return it.years;
                            case Sqlx.MONTH: return it.months;
                            case Sqlx.DAY: return it.ticks / TimeSpan.TicksPerDay;
                            case Sqlx.HOUR: return it.ticks / TimeSpan.TicksPerHour;
                            case Sqlx.MINUTE: return it.ticks / TimeSpan.TicksPerMinute;
                            case Sqlx.SECOND: return it.ticks / TimeSpan.TicksPerSecond;
                        }
                        break;
                    }
            }
            throw new DBException("42000", mod).ISO().Add(Sqlx.ROUTINE_NAME, new TChar("Extract"));
        }

#if !SILVERLIGHT && !WINDOWS_PHONE
        /// <summary>
        /// helper for XML processing instruction
        /// </summary>
        /// <param name="sv">the object</param>
        /// <returns>the result xml string</returns>
        object XmlFromXPI(object o)
        {
            if (o is XPathNodeIterator pi)
            {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; pi.MoveNext(); j++)
                    sb.Append(XmlFromXPI(pi.Current));
                return sb.ToString();
            }
            return (o as XPathNavigator)?.OuterXml ?? o.ToString();
        }
#endif
        /// <summary>
        /// Xml encoding
        /// </summary>
        /// <param name="a">an object to encode</param>
        /// <returns>an encoded string</returns>
        string XmlEnc(object a)
        {
            return a.ToString().Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\r", "&#x0d;");
        }
        internal override SqlValue SetReg(TRow key)
        {
            if ((window!=null || aggregates0()))
            {
                cur = regs?[key];
                if (cur == null)
                {
                    cur = new Register() { profile = key };
                    if (regs == null)
                        regs = new CTree<TRow, Register>(tr, key.dataType);
                    ATree<TRow, Register>.Add(ref regs, key, cur);
                }
            } else
            {
                val.SetReg(key);
                op1.SetReg(key);
                op2.SetReg(key);
            }
            return this;
        }
        /// <summary>
        /// for aggregates and window functions we need to implement StartCounter
        /// </summary>
        internal override void StartCounter(Transaction tr,RowSet rs)
        {
            cur.acc1 = 0.0;
            dset = null;
            switch (kind)
            {
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.AVG:
                    cur.count = 0L;
                    cur.sumType = Domain.Content;
                    break;
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                    cur.mset = new TMultiset(tr, val.nominalDataType);
                    break;
                case Sqlx.XMLAGG:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    cur.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    cur.bval = false;
                    break;
                case Sqlx.ARRAY:
                    cur.acc = new TArray(new Domain(Sqlx.ARRAY, Domain.Content));
                    break;
                case Sqlx.COUNT:
                    cur.count = 0L;
                    break;
                case Sqlx.FIRST:
                    cur.acc = null; // NOT TNull.Value !
                    break;
                case Sqlx.LAST:
                    cur.acc = TNull.Value;
                    break;
                case Sqlx.MAX:
                case Sqlx.MIN:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    cur.sumType = Domain.Content;
                    cur.acc = null;
                    break;
#if OLAP
                case Sqlx.REGR_AVGX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_AVGY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_COUNT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_INTERCEPT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_R2: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SLOPE: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SYY: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.STDDEV_POP:
                    cur.acc1 = 0.0;
                    cur.sum1 = 0.0;
                    cur.count = 0L;
                    break; 
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUM:
                    cur.sumType = Domain.Content;
                    cur.sumInteger = null;
                    break;
#if OLAP
                case Sqlx.VAR_POP:goto case Sqlx.STDDEV_POP;
                case Sqlx.VAR_SAMP: goto case Sqlx.STDDEV_POP;
#endif
                default:
                    val?.StartCounter(tr, rs);
                    break;
            }
        }
        /// <summary>
        /// for aggregates we need to implement AddIn
        /// </summary>
        /// <param name="ws">the window specification</param>
        internal void StartCounter(Transaction tr,RowSet rs, WindowSpecification ws)
        {
            switch (kind)
            {


                default: StartCounter(tr,rs); break;
            }
        }
        internal override void _AddIn(Transaction tr,RowSet rs,ref ATree<long,bool?>aggsDone)
        {
            if (aggsDone[sqid] == true)
                return;
            ATree<long, bool?>.Add(ref aggsDone, sqid, true);
            if (filter != null && !filter.Matches(tr,rs))
                return;
            if (mod == Sqlx.DISTINCT)
            {
                var v = val.Eval(tr,rs)?.NotNull();
                if (v != null)
                {
                    if (dset == null)
                        dset = new TMultiset(tr,v.dataType);
                    else if (dset.Contains(v))
                        return;
                    dset.Add(v);
                    etag = ETag.Add(v.etag, etag);
                }
            }
            switch (kind)
            {
                case Sqlx.AVG: // is not used with Remote
                    {
                        var v = val.Eval(tr, rs);
                        if (v == null)
                        {
                            (tr as Transaction)?.Warning("01003");
                            break;
                        }
                        etag = ETag.Add(v.etag, etag);
                    }
                    cur.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ANY:
                    {
                        if (window != null)
                            goto case Sqlx.COLLECT;
                        var v = val.Eval(tr, rs)?.NotNull();
                        if (v != null)
                        {
                            if (v.Val(tr) is bool)
                                cur.bval = cur.bval || (bool)v.Val(tr);
                            else
                                (tr as Transaction)?.Warning("01003");
                            etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.ARRAY: // Mongo $push
                    if (val != null)
                    {
                        if (cur.acc == null)
                            cur.acc = new TArray(new Domain(Sqlx.ARRAY, val.nominalDataType));
                        var ar = cur.acc as TArray;
                        var v = val.Eval(tr, rs)?.NotNull();
                        if (v != null)
                        {
                            ar.list.Add(v);
                            etag = ETag.Add(v.etag, etag);
                        }
                        else
                            (tr as Transaction)?.Warning("01003");
                    }
                    break;
                case Sqlx.COLLECT:
                    {
                        if (val != null)
                        {
                            if (cur.mset == null && val.Eval(tr, rs) != null)
                                cur.mset = new TMultiset(tr, val.nominalDataType);
                            var v = val.Eval(tr, rs)?.NotNull();
                            if (v != null)
                            {
                                cur.mset.Add(v);
                                etag = ETag.Add(v.etag, etag);
                            }
                            else
                                (tr as Transaction)?.Warning("01003");
                        }
                        break;
                    }
#if OLAP
                case Sqlx.CORR: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.COUNT:
                    {
                        if (mod == Sqlx.TIMES)
                        {
                            cur.count++;
                            break;
                        }
                        var v = val.Eval(tr, rs)?.NotNull();
                        if (v != null)
                        {
                            cur.count++;
                            etag = ETag.Add(v.etag, etag);
                        }
                    }
                    break;
#if OLAP
                case Sqlx.COVAR_POP:
                    {
                        var o1 = op1.Eval(cx);
                        var o2 = op2.Eval(cx);
                        if (o1.IsNull)
                        {
                            cx.Warning("01003");
                            break;
                        }
                        if (o2.IsNull)
                            break;
                        count++;
                        etag = ETag.Add(o1.etag, ETag.Add(o2.etag,etag));
                        var d1 = o1.ToDouble();
                        var d2 = o2.ToDouble();
                        sum1 += d1;
                        sum2 += d2;
                        acc1 += d1 * d1;
                        acc2 += d2 * d2;
                        acc3 += d1 * d2;
                        break;
                    }
                case Sqlx.COVAR_SAMP: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.EVERY:
                    {
                        var v = val.Eval(tr, rs)?.NotNull();
                        if (v is TBool vb)
                        {
                            cur.bval = cur.bval && vb.value.Value;
                            etag = ETag.Add(v.etag, etag);
                        }
                        else
                            tr.Warning("01003");
                        break;
                    }
                case Sqlx.FIRST:
                    if (val != null && cur.acc == null)
                    {
                        cur.acc = val.Eval(tr,rs)?.NotNull();
                        if (cur.acc != null)
                        {
                            nominalDataType = cur.acc.dataType;
                            etag = ETag.Add(cur.acc.etag, etag);
                        }
                    }
                    break;
                case Sqlx.FUSION:
                    {
                        if (cur.mset == null || cur.mset.IsNull)
                        {
                            var vv = val.Eval(tr,rs)?.NotNull();
                            if (vv == null || vv.IsNull)
                                cur.mset = new TMultiset(tr, val.nominalDataType.elType); // check??
                            else
                                (tr as Transaction)?.Warning("01003");
                        }
                        else
                        {
                            var v = val.Eval(tr,rs)?.NotNull();
                            cur.mset = TMultiset.Union(cur.mset, v as TMultiset, true);
                            etag = ETag.Add(v?.etag, etag);
                        }
                        break;
                    }
                case Sqlx.INTERSECTION:
                    {
                        var v = val.Eval(tr,rs)?.NotNull();
                        if (v == null)
                            (tr as Transaction)?.Warning("01003");
                        else
                        {
                            var mv = v as TMultiset;
                            if (cur.mset == null || cur.mset.IsNull)
                                cur.mset = mv;
                            else
                                cur.mset = TMultiset.Intersect(cur.mset, mv, true);
                            etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.LAST:
                    if (val != null)
                    {
                        cur.acc = val.Eval(tr,rs)?.NotNull();
                        if (cur.acc != null)
                        {
                            nominalDataType = cur.acc.dataType;
                            etag = ETag.Add(val.etag, etag);
                        }
                    }
                    break;
                case Sqlx.MAX:
                    {
                        TypedValue v = val.Eval(tr,rs)?.NotNull();
                        if (v != null && (cur.acc == null || cur.acc.CompareTo(tr, v) < 0))
                        {
                            cur.acc = v;
                            etag = ETag.Add(v.etag, etag);
                        }
                        else
                            (tr as Transaction)?.Warning("01003");
                        break;
                    }
                case Sqlx.MIN:
                    {
                        TypedValue v = val.Eval(tr,rs)?.NotNull();
                        if (v != null && (cur.acc == null || cur.acc.CompareTo(tr, v) > 0))
                        {
                            cur.acc = v;
                            etag = ETag.Add(v.etag, etag);
                        }
                        else
                            (tr as Transaction)?.Warning("01003");
                        break;
                    }
#if OLAP
                case Sqlx.REGR_AVGX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_AVGY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_COUNT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_INTERCEPT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_R2: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SLOPE: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SYY: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.STDDEV_POP: // not used for Remote
                    {
                        var o = val.Eval(tr,rs);
                        var v = o.ToDouble().Value;
                        cur.sum1 -= v;
                        cur.acc1 -= v * v;
                        cur.count--;
                        break;
                    }
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SUM:
                    {
                        var v = val.Eval(tr, rs)?.NotNull();
                        if (v==null)
                        {
                            tr.Warning("01003");
                            return;
                        }
                        etag = ETag.Add(v.etag, etag);
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInt)
                                {
                                    cur.sumType = Domain.Int;
                                    cur.sumLong = v.ToLong().Value;
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    cur.sumType = Domain.Int;
                                    cur.sumInteger = (Integer)v.Val(tr);
                                    return;
                                }
                                if (v is TReal)
                                {
                                    cur.sumType = Domain.Real;
                                    cur.sum1 = ((TReal)v).dvalue;
                                    return;
                                }
                                if (v is TNumeric)
                                {
                                    cur.sumType = Domain.Numeric;
                                    cur.sumDecimal = ((TNumeric)v).value;
                                    return;
                                }
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInt)
                                {
                                    long a = v.ToLong().Value;
                                    if (cur.sumInteger == null)
                                    {
                                        if ((a > 0) ? (cur.sumLong <= long.MaxValue - a) : (cur.sumLong >= long.MinValue - a))
                                            cur.sumLong += a;
                                        else
                                            cur.sumInteger = new Integer(cur.sumLong) + new Integer(a);
                                    }
                                    else
                                        cur.sumInteger = cur.sumInteger + new Integer(a);
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (cur.sumInteger == null)
                                        cur.sumInteger = new Integer(cur.sumLong) + a;
                                    else
                                        cur.sumInteger = cur.sumInteger + a;
                                    return;
                                }
                                break;
                            case Sqlx.REAL:
                                if (v is TReal)
                                {
                                    cur.sum1 += ((TReal)v).dvalue;
                                    return;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                if (v is TNumeric)
                                {
                                    cur.sumDecimal = cur.sumDecimal + ((TNumeric)v).value;
                                    return;
                                }
                                break;
                        }
                        throw new DBException("22108").Mix();
                    }
#if OLAP
                case Sqlx.VAR_POP: goto case Sqlx.STDDEV_POP;
                case Sqlx.VAR_SAMP: goto case Sqlx.VAR_POP;
#endif
                case Sqlx.XMLAGG:
                    {
                        cur.sb.Append(' ');
                        var o = val.Eval(tr,rs)?.NotNull();
                        if (o != null)
                        {
                            cur.sb.Append(o.ToString());
                            etag = ETag.Add(o.etag, etag);
                        }
                        else
                            tr.Warning("01003");
                        break;
                    }
                default:
                    val?.AddIn(tr, rs);
                    break;
            }
        }
 
        /// <summary>
        /// Window Function AddIn
        /// </summary>
        /// <param name="bmk">an enumerator for the window</param>
        internal void AddIn(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var n = val?.name; // null for row_number()
            switch (kind)
            {
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                case Sqlx.MAX:
                case Sqlx.MIN:
                case Sqlx.SOME:
                case Sqlx.XMLAGG:
                case Sqlx.ANY:
                    {
                        TypedValue b = Eval(bmk);
                        cur.mset.Add(b);
                        break;
                    }
                case Sqlx.AVG: // is not used with Remote
                    {
                        var v = val.Eval(bmk);
                        if (v == null)
                        {
                            (tr as Transaction)?.Warning("01003");
                            break;
                        }
                    }
                    cur.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ARRAY: // Mongo $push
                    if (val != null)
                    {
                        if (cur.acc == null)
                            cur.acc = new TArray(new Domain(Sqlx.ARRAY, val.nominalDataType));
                        var ar = cur.acc as TArray;
                        var v = val.Eval(bmk)?.NotNull();
                        if (v != null)
                            ar.list.Add(v);
                        else
                            (tr as Transaction)?.Warning("01003");
                    }
                    break;
#if OLAP
                case Sqlx.CORR: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.COUNT:
                    {
                        if (mod == Sqlx.TIMES)
                        {
                            cur.count++;
                            break;
                        }
                        var v = val.Eval(bmk)?.NotNull();
                        if (v != null)
                            cur.count++;
                    }
                    break;
#if OLAP
                case Sqlx.COVAR_POP:
                    {
                        var o1 = op1.Eval(bmk);
                        var o2 = op2.Eval(bmk);
                        if (o1.IsNull)
                        {
                            cx.Warning("01003");
                            break;
                        }
                        if (o2.IsNull)
                            break;
                        count++;
                        var d1 = o1.ToDouble();
                        var d2 = o2.ToDouble();
                        sum1 += d1;
                        sum2 += d2;
                        acc1 += d1 * d1;
                        acc2 += d2 * d2;
                        acc3 += d1 * d2;
                        break;
                    }
                case Sqlx.COVAR_SAMP: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.FIRST:
                    if (val != null && cur.acc == null)
                    {
                        cur.acc = val.Eval(bmk)?.NotNull();
                        if (cur.acc != null)
                            nominalDataType = cur.acc.dataType;
                    }
                    break;
                case Sqlx.LAST:
                    if (val != null)
                    {
                        cur.acc = val.Eval(bmk)?.NotNull();
                        if (cur.acc != null)
                            nominalDataType = cur.acc.dataType;
                    }
                    break;
#if OLAP
                case Sqlx.REGR_AVGX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_AVGY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_COUNT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_INTERCEPT: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_R2: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SLOPE: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXX: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SXY: goto case Sqlx.COVAR_POP;
                case Sqlx.REGR_SYY: goto case Sqlx.COVAR_POP;
#endif
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.STDDEV_POP: // not used for Remote
                    {
                        var o = val.Eval(bmk);
                        var v = o.ToDouble().Value;
                        cur.sum1 -= v;
                        cur.acc1 -= v * v;
                        cur.count--;
                        break;
                    }
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUM:
                    {
                        var v = val.Eval(bmk)?.NotNull();
                        if (v == null)
                        {
                            tr.Warning("01003");
                            return;
                        }
                        etag = ETag.Add(v.etag, etag);
                        switch (cur.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInt)
                                {
                                    cur.sumType = Domain.Int;
                                    cur.sumLong = v.ToLong().Value;
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    cur.sumType = Domain.Int;
                                    cur.sumInteger = (Integer)v.Val(tr);
                                    return;
                                }
                                if (v is TReal)
                                {
                                    cur.sumType = Domain.Real;
                                    cur.sum1 = ((TReal)v).dvalue;
                                    return;
                                }
                                if (v is TNumeric)
                                {
                                    cur.sumType = Domain.Numeric;
                                    cur.sumDecimal = ((TNumeric)v).value;
                                    return;
                                }
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInt)
                                {
                                    long a = v.ToLong().Value;
                                    if (cur.sumInteger == null)
                                    {
                                        if ((a > 0) ? (cur.sumLong <= long.MaxValue - a) : (cur.sumLong >= long.MinValue - a))
                                            cur.sumLong += a;
                                        else
                                            cur.sumInteger = new Integer(cur.sumLong) + new Integer(a);
                                    }
                                    else
                                        cur.sumInteger = cur.sumInteger + new Integer(a);
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (cur.sumInteger == null)
                                        cur.sumInteger = new Integer(cur.sumLong) + a;
                                    else
                                        cur.sumInteger = cur.sumInteger + a;
                                    return;
                                }
                                break;
                            case Sqlx.REAL:
                                if (v is TReal)
                                {
                                    cur.sum1 += ((TReal)v).dvalue;
                                    return;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                if (v is TNumeric)
                                {
                                    cur.sumDecimal = cur.sumDecimal + ((TNumeric)v).value;
                                    return;
                                }
                                break;
                        }
                        throw new DBException("22108").Mix();

                    }
#if OLAP
                case Sqlx.VAR_POP: goto case Sqlx.STDDEV_POP;
                case Sqlx.VAR_SAMP: goto case Sqlx.VAR_POP;
#endif
                default:
                    val?.AddIn(tr,rs);
                    break;
            }
        }
        /// <summary>
        /// Window Funmctions: bmk is a bookmark in cur.wrs
        /// </summary>
        /// <param name="bmk"></param>
        /// <returns></returns>
        bool InWindow(RowBookmark bmk)
        {
            if (bmk == null)
                return false;
            var tr = bmk._rs.tr;
            if (window.units == Sqlx.RANGE && !(TestStartRange(tr, bmk) && TestEndRange(tr, bmk)))
                return false;
            if (window.units == Sqlx.ROWS && !(TestStartRows(tr, bmk) && TestEndRows(tr, bmk)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Transaction tr, RowBookmark bmk)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            long limit;
            if (window.high.current)
                limit = cur.wrb._pos;
            else if (window.high.preceding)
                limit = cur.wrb._pos - (window.high.distance?.ToLong()??0);
            else
                limit = cur.wrb._pos + (window.high.distance?.ToLong()??0);
            return bmk._pos <= limit; 
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Transaction tr, RowBookmark bmk)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            long limit;
            if (window.low.current)
                limit =cur.wrb._pos;
            else if (window.low.preceding)
                limit = cur.wrb._pos - (window.low.distance?.ToLong() ?? 0);
            else
                limit = cur.wrb._pos + (window.low.distance?.ToLong() ?? 0);
            return bmk._pos >= limit;
        }

        /// <summary>
        /// Test the window against the end of the given range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRange(Transaction tr,RowBookmark bmk)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            var n = val.name;
            var kt = val.nominalDataType;
            var wrv = cur.wrb.Get(n);
            TypedValue limit;
            if (window.high.current)
                limit = wrv;
            else if (window.high.preceding)
                limit = kt.Eval(tr, wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, window.high.distance);
            else
                limit = kt.Eval(tr, wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, window.high.distance);
            return kt.Compare(tr, bmk.Get(n), limit) <= 0; 
        }
        /// <summary>
        /// Test a window against the start of a range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRange(Transaction tr,RowBookmark bmk)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            var n = val.name;
            var kt = val.nominalDataType;
            var tv = cur.wrb.Get(n);
            TypedValue limit;
            if (window.low.current)
                limit = tv;
            else if (window.low.preceding)
                limit = kt.Eval(tr, tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS, window.low.distance);
            else
                limit = kt.Eval(tr, tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS, window.low.distance);
            return kt.Compare(tr, bmk.Get(n), limit) >= 0; // OrderedKey comparison
        }
        /// <summary>
        /// Alas, we can't use Value() here, as the value is still in progress.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns>whether b ties a</returns>
        bool Ties(RowBookmark a,RowBookmark b)
        {
            var tr = a._rs.tr;
            for (var i = window.partition; i < window.order.items.Count; i++)
            {
                var oi = window.order.items[i];
                var n = oi.what.name;
                var av = a.Get(n);
                var bv = b.Get(n);
                if (av==bv)
                    continue;
                if (av == null)
                    return false;
                if (av.CompareTo(tr, bv) != 0)
                    return false;
            }
            return true;
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            var a = val?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var b = op1?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var c = op2?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            return a || b || c;
        }
        internal override bool Check(Transaction tr,GroupSpecification group)
        {
            if (aggregates0())
                return false;
            return base.Check(tr,group);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var b = base._Replace(tr, cx, so, sv, ref map);
            if (b != this)
                return b;
            var an = alias ?? name;
            var nv = val?.Replace(tr, cx, so, sv, ref map);
            var o1 = op1?.Replace(tr, cx, so, sv, ref map);
            var o2 = op2?.Replace(tr, cx, so, sv, ref map);
            if (nv != val || o1 != op1 || o2 != op2)
            {
                var r = new SqlFunction(tr, kind, nominalDataType, nv, o1, o2, mod);
                ATree<long, SqlValue>.Add(ref map, sqid, r);
                return r;
            }
            return this;
        }
        public override string ToString()
        {
            switch (kind)
            {
                case Sqlx.PARTITION:
                case Sqlx.VERSIONING:
                case Sqlx.CHECK: return kind.ToString();
                case Sqlx.POSITION: if (op1 == null) goto case Sqlx.PARTITION; 
                    break;
            }
            var sb = new StringBuilder();
            sb.Append(kind);
            sb.Append('(');
            if (val != null)
                sb.Append(val);
            if (op1!=null)
            {
                sb.Append(':');sb.Append(op1);
            }
            if (op2 != null)
            {
                sb.Append(':'); sb.Append(op2);
            }
            if (mod == Sqlx.TIMES)
                sb.Append('*');
            else if (mod != Sqlx.NO)
            {
                sb.Append(' '); sb.Append(mod);
            }
            sb.Append(')');
            if (alias!=null)
            {
                sb.Append(" as ");  sb.Append(alias);
            }
            return sb.ToString();
        }
        public override void ToString1(StringBuilder sb, Transaction t, From uf,Record ur, string eflag, bool cms, ref string cp)
        {
            if (cms)
                sb.Append(cp);
            cp = Sep(cp);
            switch (kind)
            {
                case Sqlx.COUNT:
                    if (mod == Sqlx.TIMES)
                    {
                        sb.Append("COUNT(*)");
                        break;
                    }
                    goto casedefault;
                default:
                    casedefault:
                    sb.Append(kind);
                    sb.Append('(');
                    var cm = "";
                    val?.ToString1(sb,cx, uf, ur,eflag, true,ref cm);
                    op1?.ToString1(sb, cx, uf, ur, eflag, true,ref cm);
                    op2?.ToString1(sb, cx, uf, ur, eflag, true,ref cm);
                    sb.Append(')');
                    break;
            }
            if (alias!=null && eflag!=null)
            {
                sb.Append(" as ");
                alias.ToString1(sb, cx, eflag);
            }
        }
        internal override void _AddReqs(Transaction tr, From gf, Domain ut, ref ATree<SqlValue, int> gfreqs, int i)
        {
            if (aggregates0() && val?.IsFrom(tr, gf, false, ut)!=false)
                ATree<SqlValue, int>.Add(ref gfreqs, this, i);
            else
                for (var b = needed.First(); b != null; b = b.Next())
                {
                    if (b.key() is SqlValue sc && sc.IsFrom(tr, gf, false))
                    {
                        if (sc is SqlFunction && sc.alias == null)
                            sc.alias = new Ident("C_" + sc.sqid, 0);
                        ATree<SqlValue, int>.Add(ref gfreqs, sc, i);
                    }
                }
        }
        /// <summary>
        /// See notes on SqlHttpUsing class: we are generating columns for the contrib query.
        /// </summary>
        /// <param name="gf">The query: From with a RestView target</param>
        /// <returns></returns>
        internal override SqlValue _ColsForRestView(Transaction tr, From gf, GroupSpecification gs, ref ATree<SqlValue, Ident> gfc, ref ATree<SqlValue, Ident> rem, ref ATree<Ident, bool?> reg,ref ATree<long,SqlValue>map)
        {
            var ac = new Ident("C_" + sqid, 0);
            var qs = gf.QuerySpec(tr);
            var cs = gf.source as CursorSpecification;
            var an = alias ?? ac;
            if (qs.names[an]==null)
                qs.names.Add(an);
            switch (kind)
            {
                case Sqlx.AVG:
                    {
                        Resolve(tr,qs);
                        var n0 = ac;
                        var n1 = new Ident("D_" + sqid, 0);
                        var c0 = new SqlFunction(tr, Sqlx.SUM, val, null, null, Sqlx.NO) { alias = n0 };
                        var c1 = new SqlFunction(tr, Sqlx.COUNT, val, null, null, Sqlx.NO) { alias = n1 };
                        ATree<SqlValue, Ident>.Add(ref rem, c0, n0);
                        ATree<SqlValue, Ident>.Add(ref rem, c1, n1);
                        var s0 = new SqlTypeColumn(tr, nominalDataType, n0, false, false, gf);
                        var s1 = new SqlTypeColumn(tr, Domain.Int, n1, false, false, gf);
                        var ct = new SqlValueExpr(tr, Sqlx.DIVIDE,
                                new SqlValueExpr(tr, Sqlx.TIMES,
                                    new SqlFunction(tr, Sqlx.SUM, s0, null, null, Sqlx.NO),
                                    new SqlLiteral(tr, new TReal(Domain.Real, 1.0)), Sqlx.NO),
                                new SqlFunction(tr, Sqlx.SUM, s1, null, null, Sqlx.NO), Sqlx.NO)
                        { alias = an };
                        ATree<SqlValue, Ident>.Add(ref gfc, s0, n0);
                        ATree<SqlValue, Ident>.Add(ref gfc, s1, n1);
                        ATree<long, SqlValue>.Add(ref map, sqid, ct);
                        return ct;
                    }
                case Sqlx.EXTRACT:
                    {
                        Resolve(tr,qs);
                        var ct = new SqlFunction(tr, mod, val, null, null, Sqlx.NO, Domain.Int)
                        { alias = an };
                        SqlValue st = ct;
                        ATree<SqlValue, Ident>.Add(ref rem, ct, an);
                        st = new SqlTypeColumn(tr, Domain.Int, an, false, false, gf);
                        ATree<SqlValue, Ident>.Add(ref gfc, st, an);
                        ATree<long, SqlValue>.Add(ref map, sqid, st);
                        return st;
                    }
                case Sqlx.STDDEV_POP:
                    {
                        Resolve(tr,qs);
                        var n0 = ac;
                        var n1 = new Ident("D_" + sqid, 0);
                        var n2 = new Ident("E_" + sqid, 0);
                        var c0 = new SqlFunction(tr, Sqlx.SUM, val, null, null, Sqlx.NO) { alias = n0 };
                        var c1 = new SqlFunction(tr, Sqlx.COUNT, val, null, null, Sqlx.NO) { alias = n1 };
                        var c2 = new SqlFunction(tr, Sqlx.SUM,
                            new SqlValueExpr(tr, Sqlx.TIMES, val, val, Sqlx.NO), null, null, Sqlx.NO)
                        { alias = n2 };
                        ATree<SqlValue, Ident>.Add(ref rem, c0, n0);
                        ATree<SqlValue, Ident>.Add(ref rem, c1, n1);
                        ATree<SqlValue, Ident>.Add(ref rem, c2, n2);
                        // c0 is SUM(x), c1 is COUNT, c2 is SUM(X*X)
                        // SQRT((c2-2*c0*xbar+xbar*xbar)/c1)
                        var s0 = new SqlTypeColumn(tr, nominalDataType, n0, false, false, gf);
                        var s1 = new SqlTypeColumn(tr, nominalDataType, n1, false, false, gf);
                        var s2 = new SqlTypeColumn(tr, nominalDataType, n2, false, false, gf);
                        var xbar = new SqlValueExpr(tr, Sqlx.DIVIDE, s0, s1, Sqlx.NO);
                        var cu = new SqlFunction(tr, Sqlx.SQRT,
                            new SqlValueExpr(tr, Sqlx.DIVIDE,
                                new SqlValueExpr(tr, Sqlx.PLUS,
                                    new SqlValueExpr(tr, Sqlx.MINUS, s2,
                                        new SqlValueExpr(tr,Sqlx.TIMES,xbar,
                                            new SqlValueExpr(tr, Sqlx.TIMES, s0, 
                                                new SqlLiteral(tr, new TReal(Domain.Real, 2.0)), 
                                                Sqlx.NO),
                                            Sqlx.NO),
                                        Sqlx.NO),
                                    new SqlValueExpr(tr, Sqlx.TIMES, xbar, xbar, Sqlx.NO),
                                    Sqlx.NO),
                                s1, Sqlx.NO), 
                            null,null,Sqlx.NO);
                        ATree<SqlValue,Ident>.Add(ref gfc, s0, n0);
                        ATree<SqlValue, Ident>.Add(ref gfc, s1, n1);
                        ATree<SqlValue, Ident>.Add(ref gfc, s2, n2);
                        ATree<long, SqlValue>.Add(ref map, sqid, cu);
                        return cu;
                    }
                default:
                    {
                        Resolve(tr,qs);
                        if (aggregates0())
                        {
                            var nk = kind;
                            var vt = val?.nominalDataType;
                            var vn = ac;
                            if (kind == Sqlx.COUNT)
                            {
                                nk = Sqlx.SUM;
                                vt = Domain.Int;
                            }
                            var st = Copy(ref map);
                            st.alias = ac;
                            ATree<SqlValue, Ident>.Add(ref rem, st, ac);
                            var va = new SqlTypeColumn(tr, vt, vn, false, false, gf);
                            var sf = new SqlFunction(tr, nk, nominalDataType,
                                va, op1, op2, mod)
                            { alias = an };
                            ATree<SqlValue, Ident>.Add(ref gfc, va, vn);
                            ATree<long, SqlValue>.Add(ref map, sqid, sf);
                            return sf;
                        }
                        if (aggregates())
                        {
                            var sr = new SqlFunction(tr, kind, nominalDataType,
                                val._ColsForRestView(tr, gf, gs, ref gfc, ref rem, ref reg, ref map),
                                op1, op2, mod)
                            { alias = an };
                            ATree<long, SqlValue>.Add(ref map, sqid, sr);
                            return sr;
                        }
                        alias = an;
                        ATree<SqlValue, Ident>.Add(ref gfc, this, an);
                        ATree<SqlValue, Ident>.Add(ref rem, this, an);
                        var sn = new SqlTypeColumn(tr,gf,nominalDataType,an,false,false);
                        ATree<long, SqlValue>.Add(ref map, sqid, sn);
                        return sn;
                    }
            }
        }

        internal bool aggregates0()
        {
            if (window != null)
                return false;
            switch (kind)
            {
                case Sqlx.ANY:
                case Sqlx.ARRAY:
                case Sqlx.AVG:
                case Sqlx.COLLECT:
#if OLAP
                case Sqlx.CORR:
#endif
                case Sqlx.COUNT:
#if OLAP
                case Sqlx.COVAR_POP:
                case Sqlx.COVAR_SAMP:
#endif
                case Sqlx.EVERY:
                case Sqlx.FIRST:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                case Sqlx.LAST:
                case Sqlx.MAX:
                case Sqlx.MIN:
#if OLAP
                case Sqlx.REGR_AVGX: 
                case Sqlx.REGR_AVGY: 
                case Sqlx.REGR_COUNT: 
                case Sqlx.REGR_INTERCEPT: 
                case Sqlx.REGR_R2: 
                case Sqlx.REGR_SLOPE: 
                case Sqlx.REGR_SXX: 
                case Sqlx.REGR_SXY: 
                case Sqlx.REGR_SYY:
#endif
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
                case Sqlx.SOME:
                case Sqlx.SUM:
#if OLAP
                case Sqlx.VAR_POP:
                case Sqlx.VAR_SAMP:
#endif
                case Sqlx.XMLAGG:
                    return true;
            }
            return false;
        }
        internal override bool aggregates()
        {
            return aggregates0() || (val?.aggregates()==true) || (op1?.aggregates()==true) || (op2?.aggregates()==true);
        }
    }
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(Transaction t, SqlValue op1, SqlValue op2)
            : base(cx, Sqlx.COALESCE, Domain.Content,null, op1, op2, Sqlx.NO) { }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,op1, d);
            nominalDataType = op1.nominalDataType;
            Setup(tr,q,op2, nominalDataType);
        }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            return (op1.Eval(tr,rs) is TypedValue lf) ? ((lf == TNull.Value) ? op2.Eval(tr,rs) : lf) : null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return (op1.Eval(bmk) is TypedValue lf) ? ((lf == TNull.Value) ? op2.Eval(bmk) : lf) : null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var n1 = op1.Replace(tr, cx, so, sv, ref map);
            var n2 = op2.Replace(tr, cx, so, sv, ref map);
            return (n1 == op1 && n2 == op2)?this : new SqlCoalesce(tr, n1, n2);
        }
        public override string ToString()
        {
            return "COALESCE(..)";
        }
    }
    internal class SqlTypeUri : SqlFunction
    {
        internal SqlTypeUri(Transaction t, SqlValue op1)
            : base(cx, Sqlx.TYPE_URI, Domain.Char, null, op1, null, Sqlx.NO) { }
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            TypedValue v = null;
            if (op1 != null)
                v = op1.Eval(tr,rs);
            if (v==null || v.IsNull)
                return nominalDataType.New(tr);
            var st = v.dataType;
            if (st.iri != null)
                return v;
 /*           var d = LocalTrans(op1);
            if (d == null)
                return nominalDataType.New(tr);
            long td = st.DomainDefPos(d,-1);
            if (td > 0)
            {
                var dt = d.GetDataType(td);
                if (dt != null)
                    return new TChar( dt.iri);
            } */
            return nominalDataType.New(tr);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            TypedValue v = null;
            if (op1 != null)
                v = op1.Eval(bmk);
            if (v == null || v.IsNull)
                return nominalDataType.New(tr);
            var st = v.dataType;
            if (st.iri != null)
                return v;
            return nominalDataType.New(tr);
        }
        public override string ToString()
        {
            return "TYPE_URI(..)";
        }
    }
    /// <summary>
    /// Quantified Predicate subclass of SqlValue
    /// </summary>
    internal class QuantifiedPredicate : SqlValue
    {
        public SqlValue what = null;
        /// <summary>
        /// The comparison operator: LSS etc
        /// </summary>
        public Sqlx op;
        /// <summary>
        /// whether ALL has been specified
        /// </summary>
        public bool all = false;
        /// <summary>
        /// The query specification to test against
        /// </summary>
        public QuerySpecification select = null;
        /// <summary>
        /// A new Quantified Predicate built by the parser (or by Copy, Invert here)
        /// </summary>
        /// <param name="w">The test expression</param>
        /// <param name="sv">the comparison operator, or AT</param>
        /// <param name="a">whether ALL has been specified</param>
        /// <param name="s">the query specification to test against</param>
        internal QuantifiedPredicate(Transaction tr,SqlValue w, Sqlx o, bool a, QuerySpecification s)
            : base(tr,Domain.Bool)
        {
            what = w; op = o; all = a; select = s;
            Needs(w);
        }
        protected QuantifiedPredicate(QuantifiedPredicate q, ref ATree<long, SqlValue> vs) :base(q,ref vs)
        {
            what = q.what.Copy(ref vs);
            op = q.op;
            all = q.all;
            var c = q.select.contexts;
            select = (QuerySpecification)q.select.Copy(ref c,ref vs);
            select.contexts = c;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new QuantifiedPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new QuantifiedPredicate(tr,what.Import(tr, q), op, all, select);
        }
        /// <summary>
        /// Invert this search condition e.g. NOT (a LSS SOME b) is (a GEQ ALL b)
        /// </summary>
        /// <param name="j">the part part</param>
        /// <returns>the new search condition</returns>
        internal override SqlValue Invert()
        {
            switch (op)
            {
                case Sqlx.EQL: return new QuantifiedPredicate(tr, what, Sqlx.NEQ, !all, select);
                case Sqlx.NEQ: return new QuantifiedPredicate(tr, what, Sqlx.EQL, !all, select);
                case Sqlx.LEQ: return new QuantifiedPredicate(tr, what, Sqlx.GTR, !all, select);
                case Sqlx.LSS: return new QuantifiedPredicate(tr, what, Sqlx.GEQ, !all, select);
                case Sqlx.GEQ: return new QuantifiedPredicate(tr, what, Sqlx.LSS, !all, select);
                case Sqlx.GTR: return new QuantifiedPredicate(tr, what, Sqlx.LEQ, !all, select);
                default: throw new PEException("PE65");
            }
        }
        /// <summary>
        /// Analysis stage Selects: setup the operands
        /// </summary>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,what,d);
        }
        /// <summary>
        /// Analysis stage Conditions: process conditions
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q,bool disj)
        {
            select.Analyse(tr);
            what.Conditions(tr, q, false);
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            for (var rb = select.rowSet.First(); rb != null; rb = rb.Next())
            {
                var col = rb.Value()[0];
                if (what.Eval(tr,rs) is TypedValue w)
                {
                    if (OpCompare(op, col.dataType.Compare(tr, w, col)) && !all)
                        return TBool.True;
                    else if (all)
                        return TBool.False;
                }
                else
                    return null;
            }
            return TBool.For(all);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            for (var rb = select.rowSet.First(); rb != null; rb = rb.Next())
            {
                var col = rb.Value()[0];
                if (what.Eval(bmk) is TypedValue w)
                {
                    if (OpCompare(op, col.dataType.Compare(tr, w, col)) && !all)
                        return TBool.True;
                    else if (all)
                        return TBool.False;
                }
                else
                    return null;
            }
            return TBool.For(all);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var wh = what.Replace(tr, cx, so, sv, ref map);
            var cs = BTree<string, Context>.Empty;
            return (wh == what) ? this: new QuantifiedPredicate(tr, wh, op, all, select.Copy(ref cs, ref map) as QuerySpecification);
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            return what?.Check(tr, rs, cacheWhere, ref newcache)?? false;
        }
        internal override bool aggregates()
        {
            return what.aggregates()||select.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            if (what.IsKnown(rs.tr, rs.qry))
                what.Build(rs);
            select.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            if (what.IsKnown(tr, rs.qry))
                what.StartCounter(tr, rs);
            select.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            if (what.IsKnown(tr, rs.qry))
                what._AddIn(tr, rs, ref aggsDone);
            select.AddIn(tr, rs);
        }
        internal override SqlValue SetReg(TRow key)
        {
            select.SetReg(key);
            return (what.SetReg(key)!=null)?this:null;
        }
        public override string ToString()
        {
            return op.ToString();
        } 
    }
    /// <summary>
    /// BetweenPredicate subclass of SqlValue
    /// </summary>
    internal class BetweenPredicate : SqlValue
    {
        public SqlValue what = null;
        /// <summary>
        /// BETWEEN or NOT BETWEEN
        /// </summary>
        public bool between = false;
        /// <summary>
        /// The low end of the range of values specified
        /// </summary>
        public SqlValue low = null;
        /// <summary>
        /// The high end of the range of values specified
        /// </summary>
        public SqlValue high = null;
        /// <summary>
        /// A new BetweenPredicate from the parser
        /// </summary>
        /// <param name="w">the test expression</param>
        /// <param name="b">between or not between</param>
        /// <param name="a">The low end of the range</param>
        /// <param name="sv">the high end of the range</param>
        internal BetweenPredicate(Transaction tr,SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(tr,Domain.Bool)
        {
            what = w; between = b; low = a; high = h;
            Needs(w, a, h);
        }
        protected BetweenPredicate(BetweenPredicate b, ref ATree<long, SqlValue> vs) :base(b,ref vs)
        {
            what = b.what.Copy(ref vs); between = b.between;  low = b.low?.Copy(ref vs); high = b.high?.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new BetweenPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new BetweenPredicate(tr,what.Import(tr, q), between, low.Import(tr, q), high.Import(tr, q));
        }
        /// <summary>
        /// Invert the between predicate (for part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new BetweenPredicate(tr,what, !between, low, high);
        }
        internal override bool aggregates()
        {
            return what.aggregates()||low.aggregates()||high.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            what.Build(rs);
            low.Build(rs);
            high.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            what.StartCounter(tr,rs);
            low.StartCounter(tr,rs);
            high.StartCounter(tr,rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            what._AddIn(tr, rs, ref aggsDone);
            low._AddIn(tr, rs, ref aggsDone);
            high._AddIn(tr, rs, ref aggsDone);
        }
        internal override void OnRow(RowBookmark bmk)
        {
            what.OnRow(bmk);
            low.OnRow(bmk);
            high.OnRow(bmk);
        }
        internal override SqlValue SetReg(TRow key)
        {
            var a = what.SetReg(key) != null;
            var b = low.SetReg(key) != null;
            var c = high.SetReg(key) != null;
            return (a||b||c)? this : null; // avoid shortcut evaluation
        }
        /// <summary>
        /// Analysis stage Selects: setup operands
        /// </summary>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Domain dl = low.nominalDataType;
            if (dl == Domain.Int)
                dl = what.nominalDataType;
            Setup(tr, q, low, dl);
            Setup(tr, q, high, dl);
            Setup(tr,q,what, dl);
        }
        /// <summary>
        /// Analysis stage Conditions: support distribution of conditions to froms etc
        /// </summary>
        internal override bool Conditions(Transaction tr,Query q,bool disj)
        {
            what.Conditions(tr, q, false);
            low?.Conditions(tr, q, false);
            high?.Conditions(tr, q, false);
            return false;
        }
         /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (what.Eval(tr,rs) is TypedValue w)
            {
                var t = what.nominalDataType;
                if (low.Eval(tr,rs) is TypedValue lw)
                {
                    if (t.Compare(tr, w, t.Coerce(tr, lw)) < 0)
                        return TBool.False;
                    if (high.Eval(tr,rs) is TypedValue hg)
                        return TBool.For(t.Compare(tr, w, t.Coerce(tr, hg)) <= 0);
                }
            }
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            if (what.Eval(bmk) is TypedValue w)
            {
                var t = what.nominalDataType;
                if (low.Eval(bmk) is TypedValue lw)
                {
                    if (t.Compare(tr, w, t.Coerce(tr, lw)) < 0)
                        return TBool.False;
                    if (high.Eval(bmk) is TypedValue hg)
                        return TBool.For(t.Compare(tr, w, t.Coerce(tr, hg)) <= 0);
                }
            }
            return null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var wh = what.Replace(tr, cx, so, sv, ref map);
            var lw = low?.Replace(tr, cx, so, sv, ref map);
            var hg = high?.Replace(tr, cx, so, sv, ref map);
            return (wh == what && lw == low && hg == high)? this: new BetweenPredicate(tr, wh, between, low, high);
        }
        public override string ToString()
        {
            return "BETWEEN ..";
        }
    }
#if SIMILAR
    /// <summary>
    /// Similar subclass of SqlValue, also used for LIKE_REGEX
    /// </summary>
    internal class SimilarPredicate : SqlValue
    {
        /// <summary>
        /// The left operand
        /// </summary>
        public SqlValue left = null;
        /// <summary>
        /// The regular expression pattern
        /// </summary>
        public SqlValue right = null;
        /// <summary>
        /// similar or not similar
        /// </summary>
        public bool like = false;
        /// <summary>
        /// The flags for LIKE_REGEX
        /// </summary>
        public SqlValue flag = null;
        /// <summary>
        /// The escape character for SIMILAR
        /// </summary>
        public SqlValue escape = null;
        /// <summary>
        /// A like predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="k">like or not like</param>
        /// <param name="b">the right operand</param>
        /// <param name="e">the escape character</param>
        internal SimilarPredicate(SqlValue a, bool k,SqlValue b, SqlValue f, SqlValue e)
            : base(a,Domain.Bool)
        {
            left = a; like = k; right = b; flag = f;  escape = e;
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new SimilarPredicate(left, !like, right, flag, escape);
        }
        /// <summary>
        /// Analysis stage Selects: setup the operands
        /// </summary>
        internal override void _Setup(Domain d)
        {
            Setup(left,Domain.Char);
            Setup(right,left.nominalDataType);
            Setup(escape,left.nominalDataType);
        }
        /// <summary>
        /// Evaluate the SimilarPredicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction t)
        {
            var re = new RegularExpression(this, right, flag, escape);
            var rp = new RegularExpressionParameters(this);
            var r = RegEx.SQLParse(re).Like(left.Eval(cx).ToString(), rp);
            if (!like)
                r = !r;
            return TBool.For(r);
        }
        /// <summary>
        /// If groupby is specified we need to check TableColumns are aggregated or grouped
        /// </summary>
        /// <param name="group">the group by</param>
        internal override void Check(Transaction t,List<SqlValue> group)
        {
            if (left != null)
                left.Check(cx,group);
            if (right != null)
                right.Check(cx,group);
            if (escape != null)
                escape.Check(cx,group);
        }
        public override string ToString()
        {
            return "SIMILAR..";
        }
    }
#endif
    /// <summary>
    /// LikePredicate subclass of SqlValue
    /// </summary>
    internal class LikePredicate : SqlValue
    {
        /// <summary>
        /// The left operand
        /// </summary>
        public SqlValue left = null;
        /// <summary>
        /// The right operand
        /// </summary>
        public SqlValue right = null;
        /// <summary>
        /// like or not like
        /// </summary>
        public bool like = false;
        /// <summary>
        /// The escape character
        /// </summary>
        public SqlValue escape = null;
        /// <summary>
        /// A like predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="k">like or not like</param>
        /// <param name="b">the right operand</param>
        /// <param name="e">the escape character</param>
        internal LikePredicate(Transaction tr,SqlValue a, bool k, SqlValue b, SqlValue e)
            : base(tr, Domain.Bool)
        {
            left = a; like = k; right = b; escape = e;
            Needs(a, b, e);
        }
        protected LikePredicate(LikePredicate p, ref ATree<long, SqlValue> vs) :base(p,ref vs)
        {
            left = p.left.Copy(ref vs);
            right = p.right.Copy(ref vs);
            like = p.like;
            escape = p.escape?.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new LikePredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new LikePredicate(tr,left.Import(tr, q), like, right.Import(tr, q), escape.Import(tr, q));
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new LikePredicate(tr,left, !like, right, escape);
        }
        /// <summary>
        /// Analysis stage Selects: setup the operands
        /// </summary>
        internal override void _Setup(Transaction tr, Query q,Domain d)
        {
            Setup(tr,q,left,Domain.Char);
            Setup(tr,q,right,left.nominalDataType);
            Setup(tr,q,escape,left.nominalDataType);
        }
        /// <summary>
        /// Helper for computing LIKE
        /// </summary>
        /// <param name="a">the left operand string</param>
        /// <param name="b">the right operand string</param>
        /// <param name="e">the escape character</param>
        /// <returns>the boolean result</returns>
        bool Like(string a, string b, char e)
        {
            if (a == null || b == null)
                return false;
            if (a.Length == 0)
                return (b.Length == 0 || (b.Length == 1 && b[0] == '%'));
            if (b.Length == 0)
                return false;
            int j=0;
            if (b[0] == e && ++j == b.Length)
                throw new DBException("22025").Mix();
            if (j == 0 && b[0] == '_')
                return Like(a.Substring(1), b.Substring(j+1), e); 
            if (j == 0 && b[0] == '%')
             {
                int m = b.IndexOf('%', 1);
                if (m < 0)
                    m = b.Length;
                for (j = 0; j <= a.Length - m + 1; j++)
                    if (Like(a.Substring(j), b.Substring(1), e))
                        return true;
                return false;
            }
            return a[0] == b[j] && Like(a.Substring(1), b.Substring(j + 1), e);
        }
        /// <summary>
        /// Evaluate the LikePredicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            bool r = false;
            if (left.Eval(tr,rs)?.NotNull() is TypedValue lf && right.Eval(tr,rs)?.NotNull() is TypedValue rg)
            {
                if (lf.IsNull && rg.IsNull)
                    r = true;
                else if ((!lf.IsNull) & !rg.IsNull)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (escape != null)
                        e = escape.Val(tr,rs).ToString();
                    if (e.Length != 1)
                        throw new DBException("22019").ISO(); // invalid escape character
                    r = Like(a, b, e[0]);
                }
                if (!like)
                    r = !r;
                return r ? TBool.True : TBool.False;
            }
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            bool r = false;
            if (left.Eval(bmk)?.NotNull() is TypedValue lf && right.Eval(bmk)?.NotNull() is TypedValue rg)
            {
                if (lf.IsNull && rg.IsNull)
                    r = true;
                else if ((!lf.IsNull) & !rg.IsNull)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (escape != null)
                        e = escape.Val(bmk._rs.tr).ToString();
                    if (e.Length != 1)
                        throw new DBException("22019").ISO(); // invalid escape character
                    r = Like(a, b, e[0]);
                }
                if (!like)
                    r = !r;
                return r ? TBool.True : TBool.False;
            }
            return null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var lf = left.Replace(tr, cx, so, sv, ref map);
            var rg = right.Replace(tr, cx, so, sv, ref map);
            var es = escape?.Replace(tr, cx, so, sv, ref map);
            return (lf == left && rg == right && es == escape)? this: new LikePredicate(tr, lf, like, rg, es);
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            var a = left?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var b = right?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var c = escape?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            return a || b || c;
        }
        internal override bool aggregates()
        {
            return left.aggregates() || right.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            left.Build(rs);
            right.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            left.StartCounter(tr, rs);
            right.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            left._AddIn(tr, rs, ref aggsDone);
            right._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            return (left.SetReg(key)!=null||right.SetReg(key)!=null)?this:null;
        }
        public override string ToString()
        {
            return "LIKE..";
        }
    }
    /// <summary>
    /// The InPredicate subclass of SqlValue
    /// </summary>
    internal class InPredicate : SqlValue
    {
        public SqlValue what = null;
        /// <summary>
        /// In or not in
        /// </summary>
        public bool found = false;
        /// <summary>
        /// A query should be specified (unless a list of values is supplied instead)
        /// </summary>
        public Query where = null; // or
        /// <summary>
        /// A list of values to check (unless a query is supplied instead)
        /// </summary>
        public SqlValue[] vals = null;
        public InPredicate(Transaction t, SqlValue w, SqlValue[] vs = null) : base(cx, Domain.Bool)
        {
            what = w;
            vals = vs;
            Needs(w); Needs(vs);
        }
        protected InPredicate(InPredicate p, ref ATree<long, SqlValue> vs) : base(p,ref vs)
        {
            what = p.what.Copy(ref vs);
            found = p.found;
            if (p.where != null)
            {
                var c = p.where.contexts;
                where = p.where.Copy(ref c,ref vs);
                where.contexts = c;
            }
            else
            {
                vals = new SqlValue[p.vals.Length];
                for (var i = 0; i < vals.Length; i++)
                    vals[i] = p.vals[i].Copy(ref vs);
            }
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new InPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            var r = new InPredicate(tr,what.Import(tr, q)) { found = found };
            if (where == null)
            {
                r.vals = new SqlValue[vals.Length];
                for (var i = 0; i < vals.Length; i++)
                    r.vals[i] = vals[i].Import(tr,q);
            }
            return r;
        }
        /// <summary>
        /// Analysis stage Selects: setup the operands
        /// </summary>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,what, Domain.Row);
            var dt = what.nominalDataType;
            if (where != null)
            {
                where.Analyse(tr);
                if (!(what.nominalDataType.Length == 0 && what.nominalDataType.CanTakeValueOf(where.nominalDataType.columns[0])) &&
                    !what.nominalDataType.CanTakeValueOf(where.nominalDataType))
                    throw new DBException("22005$", what.nominalDataType, where.nominalDataType.columns[0]).ISO();
            }
            else
                for (int i = 0; i < vals.Length; i++)
                    Setup(tr,q,vals[i], vals[i].nominalDataType);
        }
        /// <summary>
        /// Analysis stage Conditions: check to see what conditions can be distributed
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            where?.Analyse(tr);
            what?.Conditions(tr, q,false);
            if (vals != null)
                foreach (var v in vals)
                    v.Conditions(tr, q, false);
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (what.Eval(tr,rs) is TypedValue w)
            {
                if (vals != null)
                {
                    foreach (var v in vals)
                        if (v.nominalDataType.Compare(tr, w, v.Eval(tr,rs)) == 0)
                            return TBool.For(found);
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = where.rowSet.First(); rb != null; rb = rb.Next())
                    {
                        if (w.dataType.Length == 0)
                        {
                            var col = rb.Value()[0];
                            var dt = col.dataType;
                            if (dt.Compare(tr, w, col) == 0)
                                return TBool.For(found);
                        }
                        else if (w.dataType.Compare(tr, w, rb.Value()) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
            }
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            if (what.Eval(bmk) is TypedValue w)
            {
                if (vals != null)
                {
                    foreach (var v in vals)
                        if (v.nominalDataType.Compare(tr, w, v.Eval(bmk)) == 0)
                            return TBool.For(found);
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = where.rowSet.First(); rb != null; rb = rb.Next())
                    {
                        if (w.dataType.Length == 0)
                        {
                            var col = rb.Value()[0];
                            var dt = col.dataType;
                            if (dt.Compare(tr, w, col) == 0)
                                return TBool.For(found);
                        }
                        else if (w.dataType.Compare(tr, w, rb.Value()) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
            }
            return null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var wh = what.Replace(tr, cx, so, sv, ref map);
            var cs = BTree<string, Context>.Empty;
            return (wh==what)? this: new InPredicate(tr, wh, vals) { where = where?.Copy(ref cs, ref map) };
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            var r = what.Check(tr, rs, cacheWhere, ref newcache);
            var b = Query.Check(where.where, tr, rs, cacheWhere, ref newcache);
            r = r || b;
            if (vals != null)
                foreach (var v in vals)
                {
                    b = v?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
                    r = r || b;
                }
            return r;
        }
        internal override bool aggregates()
        {
            if (vals != null)
                foreach (var v in vals)
                    if (v.aggregates())
                        return true;
            return what.aggregates()||base.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            if (vals != null)
                foreach (var v in vals)
                    v.Build(rs);
            what.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            if (vals != null)
                foreach (var v in vals)
                    v.StartCounter(tr, rs);
            what.StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            if (vals != null)
                foreach (var v in vals)
                    v._AddIn(tr, rs, ref aggsDone);
            what._AddIn(tr, rs, ref aggsDone);
            base._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            bool nulls = true;
            if (vals != null)
                foreach (var v in vals)
                    if (v.SetReg(key) != null)
                        nulls = false;
            return (what.SetReg(key) == null && nulls) ? null : this;
        }
        public override string ToString()
        {
            return "IN..";
        }
    }
    /// <summary>
    /// MemberPredicate is a subclass of SqlValue
    /// </summary>
    internal class MemberPredicate : SqlValue
    {
        /// <summary>
        /// the test expression
        /// </summary>
        public SqlValue lhs;
        /// <summary>
        /// found or not found
        /// </summary>
        public bool found = false;
        /// <summary>
        /// the right operand
        /// </summary>
        public SqlValue rhs;
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal MemberPredicate(Transaction tr,SqlValue a, bool f, SqlValue b)
            : base(tr, Domain.Bool)
        {
            lhs = a; found = f; rhs = b;
            Needs(lhs, rhs);
        }
        protected MemberPredicate(MemberPredicate m, ref ATree<long, SqlValue> vs) :base(m,ref vs)
        {
            lhs = m.lhs.Copy(ref vs);
            found = m.found;
            rhs = m.rhs.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new MemberPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new MemberPredicate(tr,lhs.Import(tr, q), found, rhs.Import(tr, q));
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new MemberPredicate(tr,lhs, !found, rhs);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,lhs,Domain.Value);
            Setup(tr,q,rhs,lhs.nominalDataType);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q,bool disj)
        {
            lhs.Conditions(tr, q, false);
            rhs.Conditions(tr, q, false);
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            if (lhs.Eval(tr,rs) is TypedValue a && rhs.Eval(tr,rs) is TypedValue b)
            {
                if (b.IsNull)
                    return nominalDataType.New(tr);
                if (a.IsNull)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                throw tr.Exception("42113", b.GetType().Name).Mix();
            }
            return null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            if (lhs.Eval(bmk) is TypedValue a && rhs.Eval(bmk) is TypedValue b)
            {
                if (b.IsNull)
                    return nominalDataType.New(tr);
                if (a.IsNull)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                throw tr.Exception("42113", b.GetType().Name).Mix();
            }
            return null;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var lh = lhs.Replace(tr, cx, so, sv, ref map);
            var rh = rhs.Replace(tr, cx, so, sv, ref map);
            return (lh == lhs || rh == rhs)? this:new MemberPredicate(tr, lh, found, rh);
        }
        internal override bool aggregates()
        {
            return lhs.aggregates()||rhs.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            lhs.Build(rs);
            rhs.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            lhs.StartCounter(tr, rs);
            rhs.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            lhs._AddIn(tr, rs, ref aggsDone);
            rhs._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            return (lhs.SetReg(key)!=null||rhs.SetReg(key)!=null)?this:null;
        }
        public override string ToString()
        {
            return "MEMBER OF..";
        }
    }
    /// <summary>
    /// TypePredicate is a subclass of SqlValue
    /// </summary>
    internal class TypePredicate : SqlValue
    {
        /// <summary>
        /// the test expression
        /// </summary>
        public SqlValue lhs;
        /// <summary>
        /// OF or NOT OF
        /// </summary>
        public bool found;
        /// <summary>
        /// the right operand
        /// </summary>
        public Domain[] rhs;
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal TypePredicate(SqlValue a, bool f, Domain[] r)
            : base(a.tr, Domain.Bool)
        {
            lhs = a; found = f; rhs = r; 
        }
        protected TypePredicate(TypePredicate t, ref ATree<long, SqlValue> vs) :base(t,ref vs)
        {
            lhs = t.lhs.Copy(ref vs);
            found = t.found;
            rhs = t.rhs;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new TypePredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new TypePredicate(lhs.Import(tr, q), found, rhs);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new TypePredicate(lhs, !found, rhs);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,lhs,Domain.Value);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            lhs.Conditions(tr, q, false);
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            var a = lhs.Eval(tr,rs);
            if (a == null)
                return null;
            if (a.IsNull)
                return TBool.False;
            bool b = false;
            var at = a.dataType;
            foreach (var tt in rhs)
                b = at.EqualOrStrongSubtypeOf(tt); // implemented as Equals for ONLY
            return TBool.For(tr, nominalDataType, b == found);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var a = lhs.Eval(bmk);
            if (a == null)
                return null;
            if (a.IsNull)
                return TBool.False;
            bool b = false;
            var at = a.dataType;
            foreach (var tt in rhs)
                b = at.EqualOrStrongSubtypeOf(tt); // implemented as Equals for ONLY
            return TBool.For(tr, nominalDataType, b == found);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        public Sqlx op;
        public SqlValue left,right;
        public PeriodPredicate(Transaction t,SqlValue op1, Sqlx o, SqlValue op2) :base(cx,Domain.Bool)
        {
            left = op1;
            op = o;
            right = op2;
            Needs(left, right);
        }
        protected PeriodPredicate(PeriodPredicate p, ref ATree<long, SqlValue> vs) :base(p,ref vs)
        {
            op = p.op;
            left = p.left.Copy(ref vs);
            right = p.right.Copy(ref vs);
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new PeriodPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new PeriodPredicate(tr,left?.Import(tr,q),op,right?.Import(tr,q));
        }
        internal override void _Setup(Transaction tr,Query q, Domain d)
        {
            Setup(tr,q,left,Domain.Content);
            Setup(tr,q,right,left.nominalDataType);
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            var a = left?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            var b = right?.Check(tr, rs, cacheWhere, ref newcache) ?? false;
            return a || b;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var lf = left.Replace(tr, cx, so, sv, ref map);
            var rg = right.Replace(tr, cx, so, sv, ref map);
            return (lf == left && rg == right) ? this: new PeriodPredicate(tr, lf, op, rg);
        }
        internal override bool aggregates()
        {
            return (left?.aggregates()??false)||(right?.aggregates()??false);
        }
        internal override void Build(RowSet rs)
        {
            left?.Build(rs);
            right?.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            left?.StartCounter(tr, rs);
            right?.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            left?._AddIn(tr, rs, ref aggsDone);
            right?._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            return (left?.SetReg(key)!=null||right?.SetReg(key)!=null)?this:null;
        }
        public override string ToString()
        {
            return op.ToString();
        }
    }
    /// <summary>
    /// A base class for QueryPredicates such as ANY
    /// </summary>
    internal abstract class QueryPredicate : SqlValue
    {
        /// <summary>
        /// the base query
        /// </summary>
        public Query expr = null;
        public QueryPredicate(Transaction t) : base(cx, Domain.Bool)
        {
            Needs(cx,expr);
        }
        protected QueryPredicate(QueryPredicate q, ref ATree<long, SqlValue> vs) :base(q,ref vs)
        {
            var c = q.expr.contexts;
            expr = q.expr.Copy(ref c,ref vs);
            expr.contexts = c;
        }
        /// <summary>
        /// analysis stage Conditions: analyse the expr (up to building its rowset)
        /// </summary>
        internal override bool Conditions(Transaction tr, Query q, bool disj)
        {
            expr.Analyse(tr);
            return false;
        }
        internal override bool IsKnown(Transaction tr, Query q)
        {
            for (var b = expr.needs.First(); b != null; b = b.Next())
                if (!q.names[b.key()].HasValue)
                    return false;
            return base.IsKnown(tr,q);
        }
        /// <summary>
        /// if groupby is specified we need to check TableColumns are aggregated or grouped
        /// </summary>
        /// <param name="group"></param>
        internal override bool Check(Transaction tr, GroupSpecification group)
        {
            for (var i = 0; i < expr.cols.Count; i++)
                if (expr.cols[i].Check(tr,group))
                    return true;
            return base.Check(tr,group);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            expr.Analyse(tr);
        }
        internal override bool aggregates()
        {
            return expr.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            expr.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            expr.StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            expr.AddIn(tr, rs);
            base._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            expr.SetReg(key);
            return base.SetReg(key);
        }
    }
    /// <summary>
    /// the EXISTS predicate
    /// </summary>
    internal class ExistsPredicate : QueryPredicate
    {
        public ExistsPredicate(Transaction t) : base(cx) { }
        protected ExistsPredicate(ExistsPredicate e, ref ATree<long, SqlValue> vs) : base(e,ref vs) { }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new ExistsPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new ExistsPredicate(tr);
        }
        /// <summary>
        /// The predicate is true if the rowSet has at least one element
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet rs)
        {
            return TBool.For(expr.rowSet.First()!=null);
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var cs = BTree<string, Context>.Empty;
            return new ExistsPredicate(tr) { expr = expr.Copy(ref cs, ref map) };
        }
        public override string ToString()
        {
            return "EXISTS..";
        }
    }
    /// <summary>
    /// the unique predicate
    /// </summary>
    internal class UniquePredicate : QueryPredicate
    {
        public UniquePredicate(Transaction t) : base(cx) {}
        protected UniquePredicate(UniquePredicate u, ref ATree<long, SqlValue> vs) : base(u,ref vs) { }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new UniquePredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new UniquePredicate(tr);
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Transaction tr,RowSet r)
        {
            RowSet rs = expr.rowSet;
            RTree a = new RTree(rs,new TreeInfo(rs.rowType, TreeBehaviour.Disallow, TreeBehaviour.Disallow));
            ATree<string, Context>.Add(ref tr.context.contexts, expr.alias?.ident ?? expr.blockid, expr);
            for (var rb=rs.First();rb!= null;rb=rb.Next())
                if (RTree.Add(ref a, rb.Value(), rb.Value()) == TreeBehaviour.Disallow)
                    return TBool.False;
            return TBool.True;
        }
        internal override SqlValue _Replace(Transaction tr, Transaction t, SqlValue so, SqlValue sv, ref ATree<long, SqlValue> map)
        {
            var cs = BTree<string, Context>.Empty;
            return new UniquePredicate(tr) { expr = expr.Copy(ref cs, ref map) };
        }
        public override string ToString()
        {
            return "UNIQUE..";
        }
    }
    /// <summary>
    /// the null predicate: test to see if a value is null in this row
    /// </summary>
    internal class NullPredicate : SqlValue
    {
        /// <summary>
        /// the value to test
        /// </summary>
        public SqlValue val = null;
        /// <summary>
        /// IS NULL or IS NOT NULL
        /// </summary>
        public bool isnull = true;
        /// <summary>
        /// Constructor: null predicate
        /// </summary>
        /// <param name="v">the value to test</param>
        /// <param name="b">false for NOT NULL</param>
        internal NullPredicate(SqlValue v, bool b)
            : base(v.tr,Domain.Bool)
        {
            val = v; isnull = b;
            Needs(v);
        }
        protected NullPredicate(NullPredicate n, ref ATree<long, SqlValue> vs) :base(n,ref vs)
        {
            val = n.val.Copy(ref vs);
            isnull = n.isnull;
        }
        internal override SqlValue _Copy(ref ATree<long, SqlValue> vs)
        {
            return new NullPredicate(this,ref vs);
        }
        internal override SqlValue Import(Transaction tr, Query q)
        {
            return new NullPredicate(val.Import(tr, q), isnull);
        }
        /// <summary>
        /// analysis stage Selects(): setup the operand
        /// </summary>
        /// <param name="q"></param>
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,val,Domain.Content);
        }
        /// <summary>
        /// analysis stage conditions(): test to see if this predicate can be distributed
        /// </summary>
        /// <param name="q">the query to test</param>
        /// <returns>whether the given query is the source of this value</returns>
        internal override bool IsFrom(Transaction tr,Query q, bool ordered,Domain ut=null)
        {
            return !ordered;
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr, RowSet rs)
        {
            return (val.Eval(tr, rs) is TypedValue tv)? TBool.For(tv.IsNull == isnull) : null;
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return (val.Eval(bmk) is TypedValue tv) ? TBool.For(tv.IsNull == isnull) : null;
        }
        internal override bool Check(Transaction tr, RowSet rs, ATree<SqlValue, TypedValue> cacheWhere, ref ATree<SqlValue, TypedValue> newcache)
        {
            return val.Check(tr, rs, cacheWhere, ref newcache);
        }
        internal override bool aggregates()
        {
            return val.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            val.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            val.StartCounter(tr, rs);
        }
        internal override void _AddIn(Transaction tr, RowSet rs, ref ATree<long, bool?> aggsDone)
        {
            val._AddIn(tr, rs, ref aggsDone);
        }
        internal override SqlValue SetReg(TRow key)
        {
            return (val.SetReg(key)!=null)?this:null;
        }
        public override string ToString()
        {
            return isnull?"IS NULL":"IS NOT NULL";
        }
    }
    internal abstract class SqlHttpBase : SqlValue
    {
        public ATree<long,SqlValue> where = BTree<long,SqlValue>.Empty;
        public ATree<int,ATree<long, TypedValue>> matches;
        protected RowSet rows = null;
        protected SqlHttpBase(Transaction t, Query q) : base(cx, q.nominalDataType)
        {
            matches = q.matches;
        }
        internal virtual void AddCondition(SqlValue wh)
        {
            if (wh!=null)
                ATree<long, SqlValue>.Add(ref where, wh.sqid, wh);
        }
        internal void AddCondition(ATree<long,SqlValue> svs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                AddCondition(b.value());
        }
        internal abstract TypedValue Eval(Transaction tr, RowSet rs, bool asArray);
        internal abstract TypedValue Eval(RowBookmark bmk, bool asArray);
        internal override TypedValue Eval(Context tr, RowSet rs)
        {
            return Eval(tr, rs,false);
        }
        internal override TypedValue Eval(RowBookmark bmk)
        {
            return Eval(bmk, false);
        }
        internal virtual void Delete(Transaction tr,RestView rv, From f,ATree<string,bool>dr,Adapters eqs)
        {
        }
        internal virtual void Update(Transaction tr,RestView rv, From f, ATree<string, bool> ds, Adapters eqs, List<RowSet> rs)
        {
        }
        internal virtual void Insert(RestView rv, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
        {
        }
    }
    internal class SqlHttp : SqlHttpBase
    {
        string blockid; // the From
        public SqlValue expr; // for the url
        public string mime;
        public Record pre;
        public Domain targetType;
        public Domain keyType = null;
        public string remoteCols;
        internal SqlHttp(Transaction t, Query f, SqlValue v, string m, Domain tgt, ATree<long,SqlValue> w, string rCols,Record ur=null,ATree<int,ATree<long,TypedValue>> mts=null)
            : base(cx,f)
        {
            blockid = f.blockid;
            expr = v; mime = m; where = w; pre = ur; targetType = tgt; remoteCols = rCols;
            for (var d=mts?.First();d!=null;d=d.Next())
                for (var b=d.value().First();b!=null;b=b.Next())
                {
                    var td = matches[d.key()] ?? BTree<long, TypedValue>.Empty;
                    ATree<long, TypedValue>.Add(ref td, b.key(), b.value());
                    ATree<int, ATree<long, TypedValue>>.Add(ref matches, d.key(), td);
                }
            if (f is From gf)
                gf.source.ImportMatches(cx.Db(gf.target.dbix), gf,gf.accessibleDataType);
            Needs(v); Needs(w);
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            Setup(tr,q,expr,Domain.Char);
            targetType = d;
            nominalDataType = d;
        }
        /// <summary>
        /// A lot of the fiddly rowType calculation is repeated from RestView.RowSets()
        /// - beware of these mutual dependencies
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Transaction t,RowSet rs,bool asArray)
        {
            var tr = cx as Transaction;
            var r = (tr!=null && expr?.Eval(tr, rs) is TypedValue ev)?
                    OnEval(tr,ev):null;
            if ((!asArray) && r is TArray ta)
                return new TRowSet(tr, rs.qry, ta);
            if (r != null)
                return r;
            var globalFrom = tr.Ctx(blockid) as From;
            var rtype = globalFrom.source.nominalDataType;
            if (rtype.defaultValue != "")
                return new Parser(tr).ParseSqlValue(rtype.defaultValue, targetType).Eval(tr, rs);
            return nominalDataType.New(tr);
        }
        internal override TypedValue Eval(RowBookmark bmk,bool asArray)
        {
            var rs = bmk._rs;
            var tr = rs.tr as Transaction;
            var r = (tr != null && expr?.Eval(bmk) is TypedValue ev) ?
                    OnEval(tr, ev) : null;
            if ((!asArray) && r is TArray ta)
                r = new TRowSet(tr, rs.qry, ta);
            if (r != null)
                return r;
            var globalFrom = tr.Ctx(blockid) as From;
            var rtype = globalFrom.source.nominalDataType;
            if (rtype.defaultValue != "")
                return new Parser(tr).ParseSqlValue(rtype.defaultValue, targetType).Eval(bmk);
            return nominalDataType.New(tr);
        }
        TypedValue OnEval(Transaction tr, TypedValue ev)
        {
            var db = tr.db;
            string url = ev.ToString();
            var rx = url.LastIndexOf("/");
            var globalFrom = tr.Ctx(blockid) as From;
            var rtype = globalFrom.source.nominalDataType;
            var vw = globalFrom.target as View;
            string targetName = "";
            if (globalFrom != null)
            {
                targetName = url.Substring(rx + 1);
                url = url.Substring(0, rx);
            }
#if !SILVERLIGHT && !WINDOWS_PHONE
            if (url != null)
            {
                var rq = GetRequest(db, url);
                rq.Method = "POST";
                rq.Accept = mime;
                var sql = new StringBuilder("select ");
                sql.Append(remoteCols);
                sql.Append(" from "); sql.Append(targetName);
                var qs = tr.context.QuerySpec(tr);
                var cm = " group by ";
                if ((vw.remoteGroups != null && vw.remoteGroups.sets.Count > 0) || globalFrom.aggregates())
                {
                    var ids = new List<Ident>();
                    var cs = globalFrom.source as CursorSpecification;
                    for (var i = 0; i < cs.restGroups.Length; i++)
                    {
                        var n = cs.restGroups[i];
                        if (!Contains(ids, n.ToString()))
                        {
                            ids.Add(n);
                            sql.Append(cm); cm = ",";
                            sql.Append(n);
                        }
                    }
                    if (vw.remoteGroups != null)
                        foreach (var gs in vw.remoteGroups.sets)
                            Grouped(tr, gs, sql, ref cm, ids, globalFrom);
                    for (var b = globalFrom.needs.First(); b != null; b = b.Next())
                        if (b.value().HasFlag(Query.Need.joined)
                            && globalFrom.accessibleDataType.names[b.key()].HasValue
                            && !Contains(ids, b.key().ToString()))
                        {
                            ids.Add(b.key());
                            sql.Append(cm); cm = ",";
                            sql.Append(b.key());
                        }
                    var keycols = new List<SqlValue>();
                    foreach (var id in ids)
                        keycols.Add(globalFrom.Lookup0(tr, id));
                    keyType = new RowType(tr, keycols.ToArray());
                    if (globalFrom.source.where.Count > 0 || globalFrom.source.matches.Count > 0)
                    {
                        var sw = globalFrom.WhereString(globalFrom.source.where, globalFrom.source.matches, tr, pre);
                        if (sw.Length > 0)
                        {
                            sql.Append((ids.Count > 0) ? " having " : " where ");
                            sql.Append(sw);
                        }
                    }
                }
                else
                if (globalFrom.source.where.Count > 0 || globalFrom.source.matches.Count > 0)
                {
                    var sw = globalFrom.WhereString(globalFrom.source.where, globalFrom.source.matches, tr, pre);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
                        sql.Append(sw);
                    }
                }
#if !EMBEDDED
                if (PyrrhoStart.HTTPFeedbackMode)
                    Console.WriteLine(url + " " + sql.ToString());
#endif
                if (globalFrom != null)
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
                var wr = GetResponse(rq);
                if (wr == null)
                    throw new DBException("2E201", url);
#if !EMBEDDED
                var et = wr.GetResponseHeader("ETag");
                if (et != null)
                {
                    tr.etags.Add(et);
                    if (PyrrhoStart.DebugMode)
                        Console.WriteLine("Response ETag: " + et);
                }
#endif
                var s = wr.GetResponseStream();
                TypedValue r = null;
                if (s != null)
                    r = rtype.Parse(tr, new StreamReader(s).ReadToEnd());
#if !EMBEDDED
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (r is TArray)
                        Console.WriteLine("--> " + ((TArray)r).list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (r?.ToString() ?? "null"));
                }
#endif
                s.Close();
                return r;
            }
#endif
            return null;
        }
        void Grouped(Transaction tr,Grouping gs,StringBuilder sql,ref string cm,List<Ident> ids,Query gf)
        {
            for (var i = 0; i < gs.names.Length; i++)
            {
                var g = gs.names[i];
                if (gf.PosFor(gf.names,g).HasValue && !Contains(ids,g.ToString()))
                {
                    ids.Add(g);
                    sql.Append(cm); cm = ",";
                    if (Char.IsDigit(g.ident[0]))
                        sql.Append(tr.Db(g.dbix)._Role.defs[long.Parse(g.ident)].name.ToString());
                    else
                        sql.Append(g.ToString());
                }
            }
            foreach (var gi in gs.groups)
                Grouped(tr,gi, sql, ref cm,ids, gf);
        }
        bool Contains(List<Ident> ids,string n)
        {
            foreach (var i in ids)
                if (i.ToString() == n)
                    return true;
            return false;
        }
#if !SILVERLIGHT && !WINDOWS_PHONE
        public static HttpWebResponse GetResponse(WebRequest rq)
        {
            HttpWebResponse wr = null;
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
                    throw new DBException("42105");
            }
            catch (Exception e)
            {
                throw new DBException(e.Message);
            }
            return wr;
        }
#endif
        public static HttpWebRequest GetRequest(Database db,string url)
        {
            string user = null, password = null;
            var ss = url.Split('/');
            if (ss.Length>3)
            {
                var st = ss[2].Split('@');
                if (st.Length>1)
                {
                    var su = st[0].Split(':');
                    user = su[0];
                    if (su.Length > 1)
                        password = su[1];
                }
            }
            var rq = WebRequest.Create(url) as HttpWebRequest;
#if EMBEDDED
            rq.UserAgent = "Pyrrho";
#else
            rq.UserAgent = "Pyrrho "+PyrrhoStart.Version[1];
#endif
            if (user == null)
                rq.UseDefaultCredentials = true;
            else
            {
                var cr = user + ":" + password;
                var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
                rq.Headers.Add("Authorization: Basic " + d);
            }
            return rq;
        }
        internal override void RowSet(Transaction tr,From f)
        {
            var ta = Eval(tr,f.rowSet) as TArray;
            var rs = new ExplicitRowSet(tr,f);
            foreach (TRow x in ta?.list)
                if (x != null)
                    rs.Add(x);
            f.data = rs;
        }
        /// <summary>
        /// Execute a Delete operation (for an updatable REST view)
        /// </summary>
        /// <param name="f">The From</param>
        /// <param name="dr">The delete information</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override void Delete(Transaction tr,RestView rv, From f, ATree<string, bool> dr, Adapters eqs)
        {
            var url = expr.Eval(tr,f.rowSet).ToString();
            if (f.source.where.Count >0 || f.source.matches.Count>0)
            {
                var wc = f.WhereString(f.source.where, f.source.matches, tr, pre);
                if (wc == null)
                    throw new DBException("42152", ToString()).Mix();
                url += "/" + wc;
            }
            var wr = GetRequest(tr[rv], url);
#if !EMBEDDED
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("DELETE " +url);
#endif
            wr.Method = "DELETE";
            wr.Accept = mime;
            var ws = GetResponse(wr);
            var et = ws.GetResponseHeader("ETag");
            if (et != null)
                tr.etags.Add(et);
            if (ws.StatusCode != HttpStatusCode.OK)
                throw new DBException("2E203").Mix();
        }
        /// <summary>
        /// Execute an Update operation (for an updatable REST view)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="ds">The list of updates</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the rowsets affected</param>
        internal override void Update(Transaction tr, RestView rv, From f, ATree<string, bool> ds, Adapters eqs, List<RowSet> rs)
        {
            var db = tr[rv];
            var url = expr.Eval(tr, f.rowSet).ToString();
            if (f.source.where.Count > 0 || f.source.matches.Count>0)
            {
                var wc = f.WhereString(f.source.where, f.source.matches, tr, pre);
                if (wc == null)
                    throw new DBException("42152", ToString()).Mix();
                url += "/" + wc;
            }
            var wr = GetRequest(db, url);
            wr.Method = "PUT";
            wr.Accept = mime;
            var dc = new TDocument(tr);
            foreach (var b in f.assigns)
                dc.Add(b.vbl.name, b.val.Eval(tr, f.rowSet));
            var d = Encoding.UTF8.GetBytes(dc.ToString());
            wr.ContentLength = d.Length;
#if !EMBEDDED
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("PUT " + url+" "+dc.ToString());
#endif
            var ps = wr.GetRequestStream();
            ps.Write(d, 0, d.Length);
            ps.Close();
            var ws = GetResponse(wr);
            var et = ws.GetResponseHeader("ETag");
            if (et != null)
                tr.etags.Add(et);
            ws.Close();
        }
        /// <summary>
        /// Execute an Insert (for an updatable REST view)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the data to be inserted</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the rowsets affected</param>
        internal override void Insert(RestView rv, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
        {
            if (data.tr is Transaction tr)
            {
                var db = data.tr.Db(rv.dbix);
                var url = expr.Eval(data.tr,f.rowSet).ToString();
                var ers = new ExplicitRowSet(data.tr, f);
                var wr = GetRequest(db, url);
                wr.Method = "POST";
                wr.Accept = mime;
                var dc = new TDocArray(data);
                var d = Encoding.UTF8.GetBytes(dc.ToString());
#if !EMBEDDED
                if (PyrrhoStart.HTTPFeedbackMode)
                    Console.WriteLine("POST " + url + " "+dc.ToString());
#endif
                wr.ContentLength = d.Length;
                var ps = wr.GetRequestStream();
                ps.Write(d, 0, d.Length);
                ps.Close();
                var ws = GetResponse(wr);
                var et = ws.GetResponseHeader("ETag");
                if (et != null)
                    tr.etags.Add(et);
            }
        }
        public override void ToString1(StringBuilder sb, Transaction c, From uf,Record ur, string eflag,bool cms, ref string cp)
        {
            sb.Append(" RESTView ");
            sb.Append(expr.Val(c) ?? "");
            sb.Append(" ");
            sb.Append(remoteCols);
            sb.Append(" ");
            var cm = " where ";
            for (var b = where.First(); b != null; b = b.Next())
            {
                sb.Append(cm); 
                b.value().ToString1(sb, c, uf,ur, eflag, false,ref cm);
                cm = Sep(cm);
            }
        }
    }
    /// <summary>
    /// To implement RESTViews properly we need to hack the nominalDataType of the FROM globalView.
    /// After stage Selects, globalFrom.nominalDataType is as declared in the view definition.
    /// So globalRowSet always has the same rowType as globalfrom,
    /// and the same grouping operation takes place on each remote contributor
    /// </summary>
    internal class SqlHttpUsing : SqlHttpBase
    {
        string blockid; 
        Table usingTable;
        Index usingIndex;
        Domain usingTableType;
        TableColumn[] usingTableColumns,usingIndexKeys;
        // the globalRowSetType is our nominalDataType
        ATree<Ident, long> usC = BTree<Ident, long>.Empty;
        /// <summary>
        /// Get our bearings in the RestView (repeating some query analysis)
        /// </summary>
        /// <param name="f"></param>
        /// <param name="ut"></param>
        internal SqlHttpUsing(Transaction tr,From f,Table ut) : base(tr,f)
        {
            blockid = f.blockid;
            var globalFrom = tr.Ctx(blockid) as From;
            usingTable = ut;
            var db = tr.Db(ut.dbix);
            usingIndex = db.FindPrimaryIndex(ut);
            usingTableType = db.Tracker(ut.defpos).type;
            if (usingIndex == null)
                throw new DBException("42164", ut.NameInSession(db));
            usingTableColumns = new TableColumn[ut.cols.Count];
            usingIndexKeys = new TableColumn[usingIndex.cols.Length];
            for (var b = ut.cols.First(); b != null; b = b.Next())
            {
                var tc = db.GetObject(b.key()) as TableColumn;
                usingTableColumns[tc.seq] = tc;
            }
            for (var i = 0; i<usingIndexKeys.Length; i++)
            {
                var tc = db.GetObject(usingIndex.cols[i]) as TableColumn;
                usingIndexKeys[i] = tc;
            }
            for (var i=0;i<f.cols.Count;i++)
            {
                var c = f.cols[i];
                var iq = usingTableType.names.Get(c.name,out Ident sb);
                if (iq.HasValue)
                    ATree<Ident, long>.Add(ref usC, c.name, usingTableColumns[iq.Value].defpos);
            }
        }
        internal override void _Setup(Transaction tr,Query q,Domain d)
        {
            nominalDataType = d; // this value is overwritten later
        }
        internal override void AddCondition(SqlValue wh)
        {
            var globalFrom = tr.Ctx(blockid) as From;
            base.AddCondition(wh?.PartsIn(globalFrom.source.nominalDataType));
        }
        internal override TypedValue Eval(Transaction tr, RowSet rs, bool asArray)
        {
            var globalFrom = tr.Ctx(blockid) as From;
            var vw = globalFrom.target as View;
            var qs = globalFrom.QuerySpec(tr); // can be a From if we are in a join
            var cs = globalFrom.source as CursorSpecification;
            var db = tr.Db(globalFrom.target.dbix);
            var uf = cs.usingFrom;
            globalFrom.ImportMatches(db, rs);
            uf?.ImportMatches(db, globalFrom,uf?.nominalDataType); // TypedValue matches, not matching expressions
            cs.MoveConditions(ref cs.where, tr, uf);
            var urs = new IndexRowSet(tr, uf, usingIndex, null) { matches = uf.matches };
            var ers = new ExplicitRowSet(tr, cs);
            cs.rowSet = ers;
            for (var b = urs.First(); b != null; b = b.Next())
            {
                var ur = b.Rec();
                var url = ur.Field(usingTableColumns[usingTableColumns.Length - 1].defpos);
                var sv = new SqlHttp(tr, globalFrom, new SqlLiteral(tr, url), "application/json", globalFrom.source.nominalDataType,
                    globalFrom.where, cs.ToString1(tr, uf, ur), ur, globalFrom.matches);
                if (sv.Eval(tr, rs, true) is TArray rv)
                    for (var i = 0; i < rv.Length; i++)
                        ers.Add(rv[i] as TRow);
                else
                    return null;
            }
            return new TRowSet(ers);
        }
        internal override TypedValue Eval(RowBookmark bmk, bool asArray)
        {
            var rs = bmk._rs;
            var tr = rs.tr;
            var globalFrom = tr.Ctx(blockid) as From;
            var vw = globalFrom.target as View;
            var qs = globalFrom.QuerySpec(tr); // can be a From if we are in a join
            var cs = globalFrom.source as CursorSpecification;
            var db = tr.Db(globalFrom.target.dbix);
            var uf = cs.usingFrom;
            globalFrom.ImportMatches(db, rs);
            uf?.ImportMatches(db, globalFrom, uf?.nominalDataType); // TypedValue matches, not matching expressions
            cs.MoveConditions(ref cs.where, tr, uf);
            var urs = new IndexRowSet(tr, uf, usingIndex, null) { matches = uf.matches };
            var ers = new ExplicitRowSet(tr, cs);
            cs.rowSet = ers;
            for (var b = urs.First(); b != null; b = b.Next())
            {
                var ur = b.Rec();
                var url = ur.Field(usingTableColumns[usingTableColumns.Length - 1].defpos);
                var sv = new SqlHttp(tr, globalFrom, new SqlLiteral(tr, url), "application/json", globalFrom.source.nominalDataType,
                    globalFrom.where, cs.ToString1(tr, uf, ur), ur, globalFrom.matches);
                if (sv.Eval(bmk, true) is TArray rv)
                    for (var i = 0; i < rv.Length; i++)
                        ers.Add(rv[i] as TRow);
                else
                    return null;
            }
            return new TRowSet(ers);
        }
        internal override void Delete(Transaction tr,RestView rv, From f, ATree<string, bool> dr, Adapters eqs)
        {
            var globalFrom = tr.Ctx(blockid) as From;
            for (var b = usingIndex.rows.First(tr);b!=null;b=b.Next(tr))
            {
                var qv = b.Value();
                if (!qv.HasValue)
                    continue;
                var db = tr[rv];
                var ur = db.GetD(qv.Value) as Record;
                var urs = new TrivialRowSet(tr, (globalFrom.source as CursorSpecification).usingFrom, ur);
                if (!(globalFrom.CheckMatch(tr,ur)&&Query.Eval(globalFrom.where,tr,urs)))
                    continue;
                var url = ur.Field(usingTableColumns[usingTableColumns.Length - 1].defpos);
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.nominalDataType, Query.PartsIn(where,f.source.nominalDataType),"",ur);
                s.Delete(tr,rv, f, dr, eqs);
            }
        }
        internal override void Update(Transaction tr,RestView rv, From f, ATree<string, bool> ds, Adapters eqs, List<RowSet> rs)
        {
            var globalFrom = tr.Ctx(blockid) as From;
            for (var b = usingIndex.rows.First(tr); b != null; b = b.Next(tr))
            {
                var qv = b.Value();
                if (!qv.HasValue)
                    continue;
                var db = tr[rv];
                var ur = db.GetD(qv.Value) as Record;
                var uf = (globalFrom.source as CursorSpecification).usingFrom;
                var urs = new TrivialRowSet(tr, uf, ur);
                if (!(globalFrom.CheckMatch(tr, ur) && Query.Eval(globalFrom.where, tr, urs)))
                    continue;
                var url = ur.Field(usingTableColumns[usingTableColumns.Length - 1].defpos);
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.nominalDataType, Query.PartsIn(f.source.where, f.source.nominalDataType), "", ur);
                s.Update(tr, rv, f, ds, eqs, rs);
            }
        }
        internal override void Insert(RestView rv, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
        {
            if (data.tr is Transaction tr)
            {
                var ers = new ExplicitRowSet(data.tr, f);
                for (var b = usingIndex.rows.First(data.tr); b != null; b = b.Next(data.tr))
                {
                    var qv = b.Value();
                    if (!qv.HasValue)
                        continue;
                    var db = data.tr.Db(rv.dbix);
                    var ur = db.GetD(qv.Value) as Record;
                    var url = ur.Field(usingTableColumns[usingTableColumns.Length - 1].defpos);
                    var rda = new TDocArray(data.tr);
                    for (var a = data.First(); a != null; a = a.Next())
                    {
                        var rw = a.Value();
                        var dc = new TDocument(data.tr,f, rw);
                        for (var i = 0; i < usingIndex.cols.Length - 1; i++)
                        {
                            var ft = usingIndexKeys[i].DataType(db);
                            var cn = usingIndexKeys[i].NameInSession(db);
                            if (ft.Compare(data.tr, ur.Field(usingIndexKeys[i].defpos), dc[cn]) != 0)
                                goto skip;
                            dc = dc.Remove(cn);
                        }
                        for (var i = 0; i < usingTableColumns.Length - 1; i++)
                        {
                            var ft = usingTableColumns[i].DataType(db);
                            var cn = usingTableColumns[i].NameInSession(db);
                            dc = dc.Remove(cn);
                        }
                        rda.Add(dc);
                        skip:;
                    }
                    if (rda.content.Count == 0)
                        continue;
                    var wr = SqlHttp.GetRequest(db, url.ToString());
                    wr.Method = "POST";
                    wr.Accept = "application/json";
                    var d = Encoding.UTF8.GetBytes(rda.ToString());
#if !EMBEDDED
                    if (PyrrhoStart.HTTPFeedbackMode)
                        Console.WriteLine("POST " + url + " "+rda.ToString());
#endif
                    wr.ContentLength = d.Length;
                    var ps = wr.GetRequestStream();
                    ps.Write(d, 0, d.Length);
                    ps.Close();
                    var ws = SqlHttp.GetResponse(wr);
                    var et = ws.GetResponseHeader("ETag");
                    if (et != null)
                        tr.etags.Add(et);
                }
                rs.Add(ers);
            }
        }
        public override void ToString1(StringBuilder sb,Transaction tr, From uf,Record ur,string eflag, bool cms,ref string cp)
        {
            sb.Append(" RESTView Using ");
            var db = tr.Db(usingTable.dbix);
            sb.Append(usingTable.NameInSession(db));
            var q = tr.context.cur as CursorSpecification;
            sb.Append(" ");
            sb.Append(q._source);
            var cm = " where ";
            for (var b = q.where.First(); b != null; b = b.Next())
            {
                sb.Append(cm); 
                b.value().ToString1(sb,tr,uf,ur,eflag,false, ref cm);
                cm = Sep(cm);
            }
        }
    }
#if MONGO || OLAP || SIMILAR
    internal class RegularExpression : SqlValue
    {
        SqlValue regex;
        public SqlValue options; //ilmsux
        public SqlValue escape;
        internal RegularExpression(Transaction t, SqlValue r,SqlValue o,SqlValue e) :base(cx,Domain.Char)
        {
            regex = r;
            options = o;
            escape = e?? new SqlLiteral(cx,TRegEx.bs);
        }
        internal override TypedValue Eval(Transaction t)
        {
            return new TRegEx(regex.Eval(cx),options.Eval(cx),escape.Eval(cx));
        }
        internal override void _Setup(Domain d)
        {
            Setup(regex,Domain.Char);
            Setup(options,Domain.Char);
            Setup(escape,Domain.Char);
        }
        public override string ToString()
        {
            return regex.ToString();
        }
   }
    internal class RegularExpressionParameters : SqlValue
    {
        public Sqlx mode = Sqlx.AFTER; // START or AFTER
        public SqlValue startPos = null;
        public Sqlx units = Sqlx.CHARACTERS; // CHARACTERS or OCTETS
        public SqlValue occurrence = null;
        public SqlValue group = null;
        public SqlValue with = null;
        public bool all = false;
        internal RegularExpressionParameters(Transaction t) : base(cx,Domain.Null) {}
        internal override void _Setup(Domain d)
        {
            Setup(startPos,Domain.Int);
            Setup(occurrence,Domain.Int);
            Setup(group,Domain.Int);
            Setup(with,Domain.Char);
        }
     }
#endif
}
