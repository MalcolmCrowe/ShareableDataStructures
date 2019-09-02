using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Text;
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
    internal class Query : Domain
    {
        /// <summary>
        /// Supplied for QuerySpecification; constructed for FROM items during Sources().
        /// What happens for other forms of Query is more debatable: so we provide a careful API.
        /// void Add(SqlValue v, Ident n, Ident a=null) will add a new column to the associated result RowSet.
        /// int Size will give cols.Count=selects.Count (display.LEQ.Size).
        /// defs can have extra entries to help with SqlName aliasing.
        /// SqlValue ValAt(int i) returns cols[i] or null if out of range
        /// SqlValue ValFor(string a) returns v such that v.name==a or null if not found
        /// int ColFor(SqlValue v) returns i such that cols[i]==v or -1 if not found
        /// int ColFor(string a) returns i such that cols[i].name==a or -1 if not found
        /// </summary>
        internal ATree<long,SqlValue> cols = BTree<long,SqlValue>.Empty;
        internal ATree<long,int?> scols = null; // will be set at end of Sources()
        internal Idents names = new Idents(); // with dots for aliases etc where necessary (unlike Domain)
        long preanalysed = 0; // Check for transaction match
        /// <summary>
        /// whether the analysis has been done
        /// </summary>
        public long analysed = 0;
        internal From simpleQuery = null; // will be non-null for simple queries and insertable views
        internal ATree<int,ATree<long, TypedValue>> matches = BTree<int,ATree<long, TypedValue>>.Empty; // guaranteed constants
        internal RowBookmark row = null; // the current row
        internal ATree<string, string> replace = BTree<string, string>.Empty; // for RestViews
        internal int display;
        internal Query enc = null; // not used after preAnalyse except for RestViews
        internal ATree<string, Activation> acts = BTree<string, Activation>.Empty; // only used after preanalyse
        internal ATree<string, Context> contexts1 = null; // saved on Pop for restoring on later Push
        internal ATree<Ident,Need> needs = BTree<Ident,Need>.Empty; // Idents needed for SqlValues in this query: includes exp aliases for groups, excludes non-grouped aggregated cols
        internal ATree<long, SqlValue> import = BTree<long, SqlValue>.Empty; // cache Imported SqlValues
        /// <summary>
        /// ordering to use, initialised during Selects(), constructed during Orders() 
        /// </summary>
        internal OrderSpec ordSpec = new OrderSpec();
        /// <summary>
        /// where clause, may be updated during Conditions() analysis.
        /// This is a disjunction of filters.
        /// the value for a filter, initially null, is updated to the implementing rowSet 
        /// </summary>
        internal ATree<long,SqlValue> where = BTree<long,SqlValue>.Empty;
        /// <summary>
        /// For Updatable Views and Joins we need some extra machinery
        /// </summary>
        internal RowSet insertData = null;
        internal ATree<UpdateAssignment, bool> assig = null;
        /// <summary>
        /// The Fetch First Clause (-1 if not specified)
        /// </summary>
        internal int fetchFirst = -1;
        /// <summary>
        /// results: constructed during RowSets() analysis
        /// </summary>
        internal RowSet rowSet = null;
        /// <summary>
        /// Constructor: a query object
        /// </summary>
        /// <param name="cx">the query context</param>
        /// <param name="k">the expected data type</param>
        internal Query(Transaction tr, string i, Domain k, Ident n = null)
            : base(i, Ident.IDType.Block, n, k)
        {
            ATree<string, Context>.Add(ref tr.contexts, i, this);
            ATree<string, Context>.Add(ref contexts, i, this);
            if (n!=null)
            {
                ATree<string, Context>.Add(ref cx.context.contexts, n.ident, this);
                ATree<string, Context>.Add(ref contexts, n.ident, this);
            }
            if (cx.context.cur is Query q)
            {
                staticLink = q.staticLink;
                enc = q;
            }
            else
            {
                staticLink = cx.context;
                enc = cx.context.cur;
            }
            cx.context.cur = this;
        }
        /// <summary>
        /// Copy of Queries can take place after PreAnalyse and before Analyse.
        /// Needed because query optimisation can e.g. add conditions to where, matches, replace, ordSpec etc
        /// </summary>
        /// <param name="q">The query we are copying</param>
        protected Query(Query q, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) : base(q)
        {
            blockid = q.blockid;
            ATree<string, Context>.Add(ref cs, blockid, this);
            for (var i=0;i<q.cols.Count;i++)
            {
                var c = q.cols[i];
                ATree<long,SqlValue>.Add(ref cols,cols.Count,vs[c.sqid]??c.Copy(ref vs));
                names.Add(q.NameFor(c));
            }
            preanalysed = q.preanalysed;
            enc = cs[q.enc?.blockid] as Query;
            matches = q.matches;
            replace = q.replace;
            display = q.display;
            displayType = q.displayType;
            ordSpec = q.ordSpec;
            for (var b = q.where.First(); b != null; b = b.Next())
                ATree<long, SqlValue>.Add(ref where, b.key(), b.value());
            needs = q.needs;
            fetchFirst = q.fetchFirst;
        }
        internal virtual Query Copy(ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new Query(this,ref cs,ref vs);
        }
        /// <summary>
        /// This must be called at the end of each copy sonstructor in subclasses
        /// </summary>
        /// <param name="q"></param>
        /// <param name="cs"></param>
        protected void CopyContexts(Query q,ATree<string,Context>cs,ATree<long,SqlValue> vs)
        {
            for (var b = q.contexts.First(); b != null; b = b.Next())
                ATree<string, Context>.Add(ref contexts, b.key(), cs[b.key()]);
            for (var b = q.import.First(); b != null; b = b.Next())
                if (vs[b.value().sqid] is SqlValue sv)
                    ATree<long, SqlValue>.Add(ref import, b.key(), sv);
        }
        internal override Context StaticCtx()
        {
            return staticLink;
        }
        internal Activation Acts(string id)
        {
            return acts[id] ?? enc?.Acts(id);
        }
        internal override SelectQuery SelQuery()
        {
            return (enc is SelectQuery qs) ? qs : enc?.SelQuery();
        }
        internal override QuerySpecification QuerySpec(Transaction tr)
        {
            return (((enc is QuerySpecification qs) ? qs : enc?.QuerySpec(tr)) 
                is QuerySpecification q)? tr.Ctx(q.blockid) as QuerySpecification : null;
        }
        internal virtual CursorSpecification CursorSpec()
        {
            return (enc is CursorSpecification cs) ? cs : enc?.CursorSpec();
        }
        internal override Context staticLink
        {
            get { return base.staticLink ?? enc?.staticLink; }
            set
            {
                if (enc == null)
                    base.staticLink = value;
            }
        }
        internal virtual void AddRestViews(CursorSpecification q)
        {
        }
        /// <summary>
        /// Add a given name and value to the query
        /// </summary>
        /// <param name="v">the value</param>
        /// <param name="n">the name</param>
        /// <param name="alias">the alias if any</param>
        internal virtual void Add(Transaction tr,SqlValue v, Ident n, Ident alias = null)
        {
            if (v == null || n==null)
                return;
            if (v.alias==null)
                v.alias = alias;
            var an = alias ?? n;
            if (names[an].HasValue) // The query is being reparsed
                return;
            names.Add(an);
            ATree<long,SqlValue>.Add(ref cols,cols.Count,v);
            if (n != null)
            {
                AddDef(n, v);
                if (v.name!=null && n.Defpos(Ident.IDType.Column) <= 0 && v.name.Defpos() > 0)
                    n.Set(v.name.Dbix, v.name.Defpos(), Ident.IDType.Column); // won't change n.type
                if (v is SqlName sn)
                    sn.refs.Add(n);
            }
            if (alias != null)
            {
                AddDef(alias, v);
                Ident.Tree<SqlValue>.Add(ref defs, alias, v);
                if (v is SqlName ao)
                    ao.refs.Add(alias);
            }
        }
        internal bool MaybeAdd(Transaction tr,Ident n, SqlValue v)
        {
            if (staticLink.defs[n] is SqlValue sv && sv.name.Defpos() != 0)
                return false;
            AddDef(n, v);
            if (v is SqlName ao)
                ao.refs.Add(n);
            return true;
        }
        internal bool MaybeAdd(Transaction tr, ref SqlValue sv)
        {
            for (var i = 0; i < cols.Count; i++)
            {
                var c = cols[i];
                if (c.MatchExpr(this, sv))
                {
                    sv = c;
                    return false;
                }
            }
            Add(tr,sv,sv.Alias());
            return true;
        }
        /// <summary>
        /// The number of columns in the query (not the same as the result length)
        /// </summary>
        internal int Size { get { return (int)cols.Count; } }

        public virtual string Tag => "QY";

        /// <summary>
        /// For RESTViews we want to be aware of Idents and SqlValues that are guaranteed to match.
        /// Matches can come from View and Subqueries and from usingTable columns.
        /// This recursion is called for source in From.Selects, so that the matching info is ready in time for
        /// the Conditions phase.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal virtual void AddMatches(Transaction tr, Query q)
        {
            for (var b = where.First(); b != null; b = b.Next())
                b.value().AddMatches(tr, q);
        }
        internal void AddMatch(Ident nm,TypedValue tv)
        {
            AddMatch(nm.dbix, nm.Defpos(),tv);
        }
        internal void AddMatch(int d,long p, TypedValue tv)
        {
            var td = matches[d] ?? BTree<long, TypedValue>.Empty;
            ATree<long, TypedValue>.Add(ref td, p, tv);
            ATree<int, ATree<long, TypedValue>>.Add(ref matches, d, td);
        }
        internal void RemoveMatch(int d,long p)
        {
            if (matches[d] is ATree<long, TypedValue> td)
            {
                ATree<long, TypedValue>.Remove(ref td, p);
                if (td.Count > 0)
                    ATree<int, ATree<long, TypedValue>>.Add(ref matches, d, td);
                else
                    ATree<int, ATree<long, TypedValue>>.Remove(ref matches, d);
            }
        }
        /// <summary>
        /// We want to disambiguate a column reference
        /// </summary>
        /// <param name="n"></param>
        internal virtual Ident FindAliasFor(Ident n)
        {
            return (names[n] != null)? new Ident(blockid,0) :null; // overrides will do better than this
        }
        /// <summary>
        ///  Make a new groupspecification containing groupings in gs that we know,
        ///  and add a grouping for n if necessary.
        ///  If possible avoid creating a new gs; return null if no groupings work for us
        /// </summary>
        /// <param name="gs"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal GroupSpecification For(Transaction tr,List<QuerySpecification> qss, Idents n)
        {
            GroupSpecification gr = null;
            foreach (var qs in qss)
            {
                var gs = qs.tableExp.group;
                if (gs == null && n?.Length == 0)
                    continue;
                if ((gs != null && Known(tr,gs,qs)) && (n?.Length == 0 || gs?.Has(n) == true))
                {
                    //        qs.tableExp.group = null;
                    gr = gs;
                    continue;
                }
                if (gs != null)
                {
                    //       var rm = new List<Grouping>();
                    foreach (var g in gs.sets)
                        if (Known(tr,g,qs))
                        {
                            //                rm.Add(g);
                            if (gr == null)
                                gr = new GroupSpecification();
                            gr.Add(g);
                        }
                    //        foreach (var r in rm)
                    //           gs.sets.Remove(r);
                    //       if (gs.sets.Count == 0)
                    //            qs.tableExp.group = null;
                }
            }
            if (gr == null)
                gr = new GroupSpecification();
            if (!gr.Has(n))
                gr.Add(n);
            return gr;
        }
        bool Known(Transaction tr,GroupSpecification gs,QuerySpecification qs)
        {
            foreach (var g in gs.sets)
                if (!Known(tr,g,qs))
                    return false;
            return true;
        }
        bool Known(Transaction tr,Grouping g,QuerySpecification qs)
        {
            foreach (var gg in g.groups)
                if (!Known(tr,gg,qs))
                    return false;
            for (var i = 0; i < g.names.Length; i++)
            {
                var gn = g.names[i];
                var iq = qs.PosFor(qs.names, gn);
                if (iq != null && qs.cols[iq.Value].IsFrom(tr, this, false))
                    continue;
                if (PosFor(names, gn) == null)
                    return false;
            }
            return true;
        }
        /// <summary>
        /// Copy the query structure from an existing model
        /// </summary>
        /// <param name="q">the model query</param>
        internal void CopyCols(Query q)
        {
            cols = q.cols;
            defs = Ident.Tree<SqlValue>.Empty;
            contexts1 = staticLink.contexts;
            names = q.names;
            display = q.display;
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
        /// <summary>
        /// Set the value expression for a given column
        /// </summary>
        /// <param name="i">the position</param>
        /// <param name="v">the value</param>
        /// <param name="n">the column name</param>
        internal void SetAt(int i, SqlValue v, Ident n)
        {
            if (rowSet!=null)
                Console.WriteLine("WARNING: bad SetAt call for " + ToString());
            //     names[i] = n; can't do this: loses alias
            if (i >= cols.Count)
                return;
            ATree<long,SqlValue>.Add(ref cols,i, v);
            AddDef(n, v);
            if (v is SqlName sn)
                sn.refs.Add(n);
        }
        /// <summary>
        /// Get the expression for a given column name
        /// </summary>
        /// <param name="a">the name or alias</param>
        /// <returns>the expression</returns>
        internal SqlValue ValFor(Ident a) //{ return selects[a]; }
        {
            var iq = names[a];
            return (iq.HasValue)? cols[iq.Value]: null;
        }
        internal SqlValue ValFor(string s)
        {
            var iq = names[s];
            return (iq.HasValue) ? cols[iq.Value] : null;
        }
        /// <summary>
        /// Get the position of a given column name
        /// </summary>
        /// <param name="a">the name or alias</param>
        /// <returns>the position</returns>
        internal virtual int ColFor(Ident a)
        {
            return names[a] ?? -1;
        }
        internal override Context Ctx(Ident id)
        {
            return (nominalDataType.names[id].HasValue || defs.Contains(id)) ? this : enc?.Ctx(id);
        }
        internal virtual bool aggregates()
        {
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().aggregates())
                    return true;
            return false;
        }
        internal virtual void Build(RowSet rs)
        {
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().IsKnown(rs.tr, rs.qry))
                    b.value().Build(rs);
        }
        internal virtual void StartCounter(Transaction tr,RowSet rs)
        {
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().IsKnown(tr,rs.qry))
                    b.value().StartCounter(tr,rs);
        }
        internal virtual void AddIn(Transaction tr,RowSet rs)
        {
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().IsKnown(tr, rs.qry))
                    b.value().AddIn(tr, rs);
        }
        internal virtual void SetReg(TRow key)
        {
            for (var b = where.First(); b != null; b = b.Next())
                    b.value().SetReg(key);
        }
        /// <summary>
        /// Get the name for a given expression
        /// </summary>
        /// <param name="v">the name or alias</param>
        /// <returns>the expression</returns>
        internal Ident NameFor(SqlValue v)
        {
            var i = v.ColFor(this);
            if (i < 0)
                return null;
            var c = cols[i];
            return c.alias??c.name;
        }
        internal SqlValue Import(Transaction tr,SqlValue sv)
        {
            if (import[sv.sqid] is SqlValue a)
                return a;
            var v = sv.Import(tr, this);
            ATree<long, SqlValue>.Add(ref import, sv.sqid, v);
            return v;
        }
        internal void Scols()
        {
            scols = BTree<long, int?>.Empty;
            for (var i = 0; i < cols.Count; i++)
                if (cols[i].name!=null)
                    ATree<long, int?>.Add(ref scols, cols[i].name.Defpos(), i);
        }
        internal override TRow Eval()
        {
  //          if (row != null) These two lines fail for RestViews
  //              return row.Value();
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                r[i] = cols[i].Eval(rowSet.tr,rowSet);
            return new TRow(nominalDataType, r);
        }
        /// <summary>
        /// See if the given (possibly dotted) ident is in this query
        /// </summary>
        /// <param name="name">the name or alias</param>
        /// <returns>the SqlValue or null if not found</returns>
        internal virtual SqlValue Lookup0(Context cx,Ident name)
        {
            if (NameMatches(this,name))
            {
                if (name.sub == null)
                    return null; // happens if a column name matches an enclosing query name
                return Lookup0(cx,name.sub);
            }
            if (staticLink.contexts[name.ident] is Context c && name.sub is Ident ns)
            {
                c.NameMatches(this,name);
                return c.Lookup(cx,ns);
            }
            if (ValFor(name.ident) is SqlValue sv)
            {
                if (sv is SqlName sn && sn.resolvedTo is SqlTypeColumn stc)
                    sv = stc;
                name.Set(sv, cx);
                if ((!(sv is SqlTypeColumn)) && sv.name.ident==name.ident && nominalDataType[name] is Domain dt)
                    return new SqlTypeColumn(cx, dt, name, false, false,this);
                if (name.sub == null)
                    return sv;
                if (sv.nominalDataType.Path(cx,name.sub) is Domain t)
                    return new SqlTypeColumn(cx, t, name, false, false,this);
            }
            return staticLink.defs[name];
        }
        internal override SqlValue _Lookup(Context cx,Ident name)
        {
            return Lookup0(cx, name) ?? staticLink?._Lookup(cx,name);
        }
        internal override Target LVal(Transaction tr,Ident name)
        {
            if (staticLink.defs[name] is SqlValue v)
                return new Target(tr, name, v.nominalDataType);
            return base.LVal(tr,name);
        }
        /// <summary>
#if MONGO
        /// <summary>
        /// Query profiling for MongoDB
        /// </summary>
        public QueryProfile profile = null;
#endif
        /// <summary>
        /// Check the name and type resolution for this query
        /// </summary>
        internal void PreAnalyse(Transaction tr)
        {
            if (preanalysed == tr.cid)
                return;
            var b = Push(tr);
            var d = staticLink.defs;
            try
            {
                preanalysed = tr.cid;
                // each of the following must be allowed to traverse all the children (subqueries are handled within ValueSetup)
                Sources(tr); // analyse sources for reference ambiguity
                Scols();
                Selects(tr, this); // analyse Select List 
            }
            catch (Exception e) { throw e; }
            finally { 
                staticLink.defs = d;
                Pop(tr,b);
            }
        }
        /// <summary>
        /// Control of the stages of anlysis of the query.
        /// Should only be called from Transaction.Execute(something): not from any Query or subclass method (except From for view)
        /// </summary>
        internal void Analyse(Transaction tr)
        {
            if (analysed!=tr.cid)
            {
                PreAnalyse(tr);
                var b = Push(tr);
                try
                {
                    Conditions(tr,this); // analyse match conditions 
                    Orders(tr,ordSpec); // analyse order requirements and set up ordSpec
                    analysed = tr.cid;
                }
                catch (Exception e) { throw e; }
                finally { Pop(tr,b); }
            }
            RowSets(tr); // choose indexes on sources, finally delivering the order requested by ordSpec
        }
        /// <summary>
        /// REST implementation: reanalyse this query
        /// </summary>
        /// <param name="c">the parent context</param>
        internal void ReAnalyse(Transaction tr)
        {
            preanalysed = 0;
            analysed = 0;
            Analyse(tr);
        }
        /// <summary>
        /// Analysis stage Sources().
        /// The From list is analysed during Context (by From constructor) (allows us to interpret *)
        /// At this stage we begin a catalogue of SqlValues available in the query
        /// </summary>
        /// <param name="cx">the context</param>
        internal virtual void Sources(Context cx)
        {
        }
        /// <summary>
        /// Analysis stage Selects().
        /// From, TableExpression and JoinPart get their columns from database objects or Variables.
        /// CursorSpecification, QueryExpression, QuerySpecification from the Selectlist.
        /// </summary>
        /// <param name="spec">the query containing the select list</param>
        internal virtual void Selects(Transaction tr,Query spec)
        {
            SqlValue.Setup(tr,this,where, Domain.Bool);
    //        if (nominalDataType.Length == 0)
                nominalDataType = new RowType(this);
        }
        /// <summary>
        /// Fix nominalDataTypes following RESTView query rewriting
        /// </summary>
        /// <param name="tr"></param>
        internal virtual void ReSelects(Transaction tr)
        {

        }
        /// <summary>
        /// Analysis stage Conditions().
        /// Then examine SearchConditions and move them down the tree if possible
        /// </summary>
        internal virtual void Conditions(Transaction tr,Query q)
        {
            Conditions(ref where,tr);
        }
        internal void Conditions(ref ATree<long,SqlValue> svs,Transaction tr)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                if (b.value().Conditions(tr, this, true))
                    ATree<long,SqlValue>.Remove(ref svs,b.key());
        }
        internal void MoveConditions(ref ATree<long,SqlValue> svs,Transaction tr, Query q)
        {
            for (var b = svs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value().Import(tr,q);
                if (v.For(tr, q) is SqlValue w) // allow substitution with matching expression
                {
                    if (!q.where.Contains(k))
                        ATree<long, SqlValue>.Add(ref q.where, k, w);
                    ATree<long, SqlValue>.Remove(ref svs, k);
                }
            }
        }
        /// <summary>
        /// Check to see if this query has the given Ident
        /// </summary>
        /// <param name="s">the ident to search for</param>
        /// <returns>whether we have this ident</returns>
        internal virtual bool HasColumn(Ident s)
        {
            return ValFor(s) != null;//selects.Contains(s);
        }
        /// <summary>
        /// Analysis stage Orders().
        /// Move ordering operations down the tree if possible
        /// </summary>
        /// <param name="ord">a requested ordering</param>
        internal virtual void Orders(Transaction tr,OrderSpec ord)
        {
            for (var i=0;i<ord.items.Count;i++)
                if (nominalDataType.names[ord[i].what.alias??ord[i].what.name]==null)
                    return;
            ordSpec = ord;
        } 
        internal void UpdateOrders(Transaction tr,SqlValue so,SqlValue sn,ref ATree<long,SqlValue> map)
        {
            for (var i = 0; i < ordSpec.items.Count; i++)
                ordSpec.items[i].what = ordSpec.items[i].what._Replace(tr, this, so, sn,ref map);
        }
        internal void UpdateCols(Transaction tr,SqlValue so,SqlValue sn, ref ATree<long, SqlValue> map)
        {
            for (var i = 0; i < cols.Count; i++)
            {
                ATree<long,SqlValue>.Add(ref cols,i,cols[i]._Replace(tr, this, so, sn, ref map));
                names[i] = cols[i].NameForRowType();
            }
            UpdateOrders(tr, so, sn, ref map);
        }
        /// <summary>
        /// Compute the rowset for the given query
        /// </summary>
        internal virtual void RowSets(Transaction tr)
        {
        }
        /// <summary>
        /// Execute an Insert operation
        /// </summary>
        /// <param name="prov">the provenance if defined</param>
        /// <param name="data">the data supplied</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">a set of affected rowsets</param>
        internal virtual int Insert(string prov,RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey = false)
        {
            return 0;
        }
        /// <summary>
        /// Execute an Update operation
        /// </summary>
        /// <param name="ur">a set of version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">a set of affected rowsets</param>
        internal virtual int Update(Transaction tr,ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return 0;
        }
        /// <summary>
        /// Execute a delete operation
        /// </summary>
        /// <param name="dr">a set of version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual int Delete(Transaction tr,ATree<string,bool> dr, Adapters eqs)
        {
            return 0;
        }
        /// <summary>
        /// Accessor: the accesible columns for this query
        /// </summary>
        /// <returns>a list of Selectors</returns>
        internal virtual Selector[] AccessibleCols()
        {
            return null;
        }
        internal Query Push(Transaction tr)
        {
            var cx = tr.context;
            Query q = cx.cur;
            if (q == this)
                return this;
            staticLink = cx;
            lookup = staticLink.lookup;
            if (staticLink.cur == null)
                staticLink.contexts = contexts1 ?? BTree<string, Context>.Empty;
            staticLink.cur = this;
            return q;
        }
        internal void Pop(Transaction tr, Query old)
        {
            if (old == this)
                return;
            contexts1 = staticLink.contexts;
            staticLink.cur = old;
        }
        public static bool Eval(ATree<long,SqlValue> svs,Transaction tr,RowSet rs=null)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value().Eval(tr, rs) != TBool.True)
                    return false;
            return true;
        }
  /*      public void CheckKnown(ATree<long,SqlValue> svs,Transaction tr)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                if (!b.value().IsKnown(tr, this))
                    throw new DBException("42112", b.value().ToString());
        } */
        public static bool Check(ATree<long,SqlValue> svs,Transaction tr,RowSet rs,ATree<SqlValue,TypedValue> cachedWhere,ref ATree<SqlValue,TypedValue> newcache)
        {
            var r = false;
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value().Check(tr, rs, cachedWhere, ref newcache))
                    r = true;
            return r;
        }
        internal void CondString(StringBuilder sb, ATree<long, SqlValue> cond, string cm)
        {
            for (var b = cond?.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value());
            }
        }
        internal void ToString1(StringBuilder sb,Transaction tr)
        {
            var cm = " (";
            for (var i = 0; i < cols.Count; i++)
            {
                var s = cols[i];
                if (s is SqlTypeColumn sc)
                {
                    sb.Append(cm); cm = ",";
                    names[i].ToString1(sb, tr, blockid);
                }
                else
                    s.ToString1(sb, tr, null, null, blockid, true, ref cm);
            }
            sb.Append(")");
        }
        public string ToString1(Transaction tr, From uf, Record ur)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var i = 0; i < Size; i++)
            {
                var c = cols[i];
                sb.Append(cm);
                var se = new StringBuilder();
                c.Import(tr, uf).ToString1(se, tr, uf, ur, blockid, false, ref cm);
                sb.Append(se);
                if ((c.alias ?? c.name) is Ident an && an.ToString() != se.ToString())
                {
                    sb.Append(" as "); sb.Append(an);
                }
            }
            return sb.ToString();
        }
        public static ATree<long,SqlValue> PartsFrom(ATree<long, SqlValue> svs, Transaction tr, Query q, bool ordered,Domain ut=null)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value() is SqlValue sv && sv.IsFrom(tr, q, ordered,ut))
                    ATree<long, SqlValue>.Add(ref r, sv.sqid, sv);
            return r;
        }
        public static ATree<long, SqlValue> PartsIn(ATree<long, SqlValue> svs, Domain dt)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = svs?.First(); b != null; b = b.Next())
                if (b.value().PartsIn(dt) is SqlValue s)
                    ATree<long, SqlValue>.Add(ref r, s.sqid, s);
            return r;
        }
        internal void ImportMatches(Database db, Query q, Domain adt)
        {
            for (var b = q.matches[db.dbix]?.First(); b != null; b = b.Next())
            {
                var n = db.objects[b.key()].NameInSession(db);
                for (var i = 0; i < adt.names.Length; i++)
                    if (n.Match(q, adt.names[i]))
                    {
                        AddMatch(adt.names[i], b.value());
                        q.RemoveMatch(db.dbix, b.key());
                        break;
                    }
            }
        }
        internal void ImportMatches(Database db, RowSet rs)
        {
            for (var b = rs.matches[db.dbix]?.First(); b != null; b = b.Next())
            {
                var n = db.objects[b.key()].NameInSession(db);
                var iq = nominalDataType.names[n];
                if (!iq.HasValue)
                    continue;
                AddMatch(nominalDataType.names[iq.Value], b.value());
            }
        }
        public static void Eqs(Transaction tr,ATree<long,SqlValue> svs,ref Adapters eqs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                b.value().Eqs(tr,ref eqs);
        }
        /// <summary>
        /// Take appropriate action if distinct has been specified
        /// </summary>
        internal virtual void SetDistinct()
        {
        }
        /// <summary>
        /// Distribute conditions, assignments and data to the query
        /// </summary>
        /// <param name="cond">The condition to add</param>
        /// <param name="needed">Field names needed</param>
        /// <param name="rqC">Display columns needed</param>
        /// <returns>the updated QueryWhere</returns>
        internal virtual void AddCondition(Transaction tr, ATree<long, SqlValue> cond, UpdateAssignment[] assigns, RowSet data)
        {
            for (var b = PartsFrom(cond, tr, this, false).First(); b != null; b = b.Next())
                AddCondition(tr, b.value());
        }
        internal void AddCondition(Transaction tr,SqlValue cond)
        {
            if (where.Contains(cond.sqid) || !cond.IsKnown(tr, this))
                return;
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().MatchExpr(this,cond))
                    return;
            ATree<long, SqlValue>.Add(ref where, cond.sqid, cond);
        }
        /// <summary>
        /// We are a view or subquery source. 
        /// The supplied where-condition is to be transformed to use our SqlTypeColumns.
        /// </summary>
        /// <param name="tr">The connection</param>
        /// <param name="cond">A where condition</param>
        internal void ImportCondition(Transaction tr, SqlValue cond)
        { 
            AddCondition(tr, cond.Import(tr,this));
        }
        /// <summary>
        /// Distribute a set of update assignments to table expressions
        /// </summary>
        /// <param name="assigns">the list of assignments</param>
        internal virtual void DistributeAssigns(ATree<UpdateAssignment,bool> assigns)
        { }
        /// <summary>
        /// Perform ordering
        /// </summary>
        /// <param name="distinct">whether distinct has been specified</param>
        internal void Ordering(bool distinct)
        {
            if (ordSpec != null &&  ordSpec.items.Count > 0 && !ordSpec.SameAs(this,rowSet.rowOrder))
                rowSet = new OrderedRowSet(this,rowSet,ordSpec,distinct);
            else if (distinct)
                rowSet = new DistinctRowSet(this,rowSet);
        }
        /// <summary>
        /// Close the query (discard the bookmarks)
        /// </summary>
        internal override void Close(Transaction tr)
        {
            var r = row;
            row = null;
            r?.Close(tr);
            base.Close(tr);
        }
        internal virtual bool Knows(SqlTypeColumn c)
        {
            return PosFor(names,c.name).HasValue;
        }
        internal void Needs(Ident id,Need need)
        {
            var n = needs.Contains(id)? needs[id] : Need.noNeed;
            ATree<Ident, Need>.Add(ref needs, id, n | need);
        }
        internal int? PosFor(Idents ids,Ident n) // allow matching equivalence
        {
            if (ids == null)
                return null;
            var r = ids[n];
            for (var b = matching[n]?.First(); r == null && b != null; b = b.Next())
                r = ids[b.key()];
            return r;
        }
        [Flags]
        internal enum Need { noNeed = 0, selected = 1, joined = 2, condition = 4, grouped = 8 }
    }

    /// <summary>
    /// Most query classes require name resolution.
    /// </summary>
    internal class SelectQuery : Query
    {
        public override string Tag => "SQ";
        internal SqlName unresolved = null;
        internal SqlMethodCall unknown = null;
        /// <summary>
        /// whether the list of cols contains aggregates
        /// </summary>
        internal bool _aggregates = false;
        public Ident.Tree<SqlValue> defs1 = null; // for Views
        internal SelectQuery(Transaction tr, string i, Domain dt, Ident n = null) : base(cx,i, dt, n) { }
        internal override SelectQuery SelQuery()
        {
            return this;
        }
        /// <summary>
        /// For Copy(): note resolution is all done beforehand, and we never call SelectQuery.Copy
        /// </summary>
        /// <param name="s"></param>
        protected SelectQuery(SelectQuery s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(s,ref cs,ref vs)
        {
        }
        internal void CheckResolved(Context cnx)
        {
            var qs = enc?.SelQuery(); // enclosing SelectQuery
            SqlName sn = null;
            for (var un = unresolved; un != null; un = sn)
            {
                sn = un.next;
                var n = un.name;
                // mark it as unresolved in the enclosing scope
                if (qs == null)
                    throw new DBException("42112", n).Mix(); // if no enclosing scope report the first one
                un.next = qs.unresolved;
                qs.unresolved = un;
            }
            SqlMethodCall sm = null;
            for (var um = unknown; um != null; um = sm)
            {
                sm = um.next;
                var n = um.name;
                var v = um.call.var;
                if (v != null)
                {
                    var ut = cnx.GetDomain(v.nominalDataType, out Database db) as UDType;
                    um.call.database = db?.dbix ?? 0;
                    if (ut == null)
                        throw new DBException("42108", v.nominalDataType.name).Mix();
                    um.call.proc = ut.GetMethod(db, um.call.pname);
                    if (um.call.proc != null)
                        um.call.pname.Set(um.call.database, um.call.proc.defpos, Ident.IDType.Method);
                    else
                    {
                        um.next = qs.unknown;
                        qs.unknown = um;
                    }
                }
            }
            if (defs1 != null) // zap defs from Views
                staticLink.defs = defs1;
            unresolved = null;
        }
        internal override void Add(Transaction tr, SqlValue v, Ident n, Ident alias = null)
        {
            _aggregates = _aggregates || v.aggregates();
            base.Add(tr, v, n, alias);
        }
        internal override bool aggregates()
        {
            return _aggregates || base.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                if (b.value().IsKnown(rs.tr, rs.qry))
                    b.value().Build(rs);
            base.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                if (b.value().IsKnown(tr, rs.qry))
                    b.value().StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void AddIn(Transaction tr, RowSet rs)
        {
            for (var b = cols.First(); b != null; b = b.Next())
                if (b.value().IsKnown(tr, rs.qry))
                    b.value().AddIn(tr, rs);
            base.AddIn(tr, rs);
        }
    }
    /// <summary>
    /// Implement a CursorSpecification 
    /// </summary>
    internal class CursorSpecification : Query
    {
        public override string Tag => "CS";
        /// <summary>
        /// The source string
        /// </summary>
        public string _source;
        /// <summary>
        /// The QueryExpression part of the CursorSpecification
        /// </summary>
        public QueryExpression union = null;
        /// <summary>
        /// For RESTView implementation
        /// </summary>
        public From usingFrom = null;
        /// <summary>
        /// Going up: For a RESTView source, the enclosing QuerySpecifications
        /// </summary>
        internal List<QuerySpecification> rVqSpecs = new List<QuerySpecification>();
        /// <summary>
        /// looking down: RestViews contained in this Query and its subqueries
        /// </summary>
        internal ATree<long, RestView> restViews = BTree<long, RestView>.Empty;
        internal Idents restGroups = new Idents();
        /// <summary>
        /// Constructor: a CursorSpecification from the Parser
        /// </summary>
        /// <param name="t">The transaction</param>
        /// <param name="dt">the expected data type</param>
        internal CursorSpecification(Transaction t, string i, Domain dt)
            : base(t, i, dt)
        {
        }
        protected CursorSpecification(CursorSpecification c,ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs) 
            :base(c,ref cs,ref vs)
        {
            _source = c._source;
            if (c.union != null) // RestViews have no QE union part
                union = (QueryExpression)c.union.Copy(ref cs,ref vs);
            if (c.usingFrom != null)
                usingFrom = (From)c.usingFrom.Copy(ref cs, ref vs);
            foreach (var rq in c.rVqSpecs)
                if (cs[rq.blockid] is QuerySpecification qs)
                    rVqSpecs.Add(qs);
            restViews = c.restViews;
            CopyContexts(c, cs, vs);
        }
        internal override Query Copy(ref ATree<string,Context>cs, ref ATree<long, SqlValue> vs)
        {
            return new CursorSpecification(this, ref cs, ref vs) { contexts = cs };
        }
        internal override CursorSpecification CursorSpec()
        {
            return this;
        }
        /// <summary>
        /// Analysis stage Sources: Do Sources() on the union
        /// If ordering is to be done on the result of a window function, add the expression to the select list.
        /// </summary>
        /// <param name="cx">the parent context</param>
        internal override void Sources(Context cx)
        {
            if (union.simpletablequery && fetchFirst>=0)
                union.fetchFirst = fetchFirst;
            union.Sources(cx);
            simpleQuery = union.simpleQuery; // will mostly be null
/*#if !EMBEDDED
            remoteBase = union.remoteBase;
#endif */
            base.Sources(cx);
        }
        /// <summary>
        /// Analysis stage Selects: Do Selects() on the union
        /// </summary>
        /// <param name="spec">the context giving the select list</param>
        internal override void Selects(Transaction tr, Query spec)
        {
            union.Selects(tr, spec);
            display = union.display;
            cols = union.cols;
            base.Selects(tr, spec);
            nominalDataType = new TableType(nominalDataType);
            names = nominalDataType.names;
        }
        internal override void AddMatches(Transaction tr, Query q)
        {
            union?.AddMatches(tr, q);
            base.AddMatches(tr, q);
        }
        /// <summary>
        /// Analysis stage Conditions: do Conditions on the union.
        /// </summary>
        internal override void Conditions(Transaction tr,Query q)
        {
            MoveConditions(ref where,tr, union);
            union.Conditions(tr,this);
            base.Conditions(tr,this);
        }
        internal override void Orders(Transaction tr, OrderSpec ord)
        {
            base.Orders(tr, ord);
            union.Orders(tr, ord);
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            union?.AddRestViews(q);
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
        internal override void AddCondition(Transaction tr,ATree<long,SqlValue> cond,UpdateAssignment[] assigns, RowSet data)
        {
            base.AddCondition(tr,cond, assigns, data);
            union.AddCondition(tr,cond, assigns, data);
        }
        internal override bool Knows(SqlTypeColumn c)
        {
            return base.Knows(c) || (union?.Knows(c) ?? usingFrom?.Knows(c) ?? false);
        }
        internal override bool aggregates()
        {
            return union.aggregates() || base.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            union.Build(rs);
            base.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            union.StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void AddIn(Transaction tr, RowSet rs)
        {
            union.AddIn(tr, rs);
            base.AddIn(tr, rs);
        }
        /// <summary>
        /// Ensure that the distinct request is propagated to the query
        /// </summary>
        internal override void SetDistinct()
        {
            union.SetDistinct();
        }
        /// <summary>
        /// delegate AccessibleCols
        /// </summary>
        /// <returns>the selectors</returns>
        internal override Selector[] AccessibleCols()
        {
            return union.AccessibleCols();
        }
        /// <summary>
        /// propagate the Insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs"the rowsets affected></param>
        internal override int Insert(string prov, RowSet data, Adapters eqs, List<RowSet> rs, 
            Level cl,bool autokey=false)
        {
            return union.Insert(prov, data, eqs, rs, cl, autokey);
        }
        /// <summary>
        /// propagate the Update operation 
        /// </summary>
        /// <param name="ur">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the rowsets affected</param>
        internal override int Update(Transaction tr,ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return union.Update(tr,ur, eqs, rs);
        }
        /// <summary>
        /// propagate the delete operation
        /// </summary>
        /// <param name="dr">the version ids</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override int Delete(Transaction tr,ATree<string, bool> dr, Adapters eqs)
        {
            return union.Delete(tr,dr,eqs);
        }
        /// <summary>
        /// Analysis stage RowSets: After building the rowsets for the union, reorder if necessary and then remove the extra TableColumns added in Selects()
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            union.RowSets(tr);
            rowSet = union.rowSet;
            base.RowSets(tr);
    //        Selects(this); // safety
            Ordering(false);
        }
        public override string ToString()
        {
            return _source;
        }
    }
    /// <summary>
    /// Implement a TableExpression as a subclass of Query
    /// </summary>
    internal class TableExpression : Query
    {
        public override string Tag => "TE";
        /// <summary>
        /// The from clause of the tableexpression
        /// </summary>
        internal SelectQuery from = null;
        /// <summary>
        /// The group specification
        /// </summary>
        internal GroupSpecification group = null;
        /// <summary>
        /// The having clause
        /// </summary>
        internal ATree<long,SqlValue> having = BTree<long,SqlValue>.Empty;
        /// <summary>
        /// A set of window names defined
        /// </summary>
        internal Ident.Tree<WindowSpecification> window = null;
        /// <summary>
        /// Constructor: a tableexpression from the parser
        /// </summary>
        /// <param name="t">the transaction</param>
        internal TableExpression(Transaction t, string i, Domain dt) : base(t,i,dt)
        {
            if (t.context.cur is QuerySpecification qs)
                qs.tableExp = this;
        }
        protected TableExpression(TableExpression t,ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs) 
            :base(t,ref cs,ref vs)
        {
            from = (SelectQuery)t.from.Copy(ref cs,ref vs);
            group = t.group;
            for (var b = t.having.First(); b != null; b = b.Next())
            {
                var ns = b.value().Copy(ref vs);
                ATree<long, SqlValue>.Add(ref having, ns.sqid, ns);
            }
            window = t.window;
            CopyContexts(t, cs, vs);
        }
        internal override Query Copy(ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new TableExpression(this,ref cs,ref vs);
        }
        /// <summary>
        /// Analysis stage Sources: From builds a select list: propagate it to the TableExpression
        /// </summary>
        /// <param name="cx">a parent context</param>
        internal override void Sources(Context cx)
        {
            from.Sources(cx);
            for (var d = from.defs.First();d!= null;d=d.Next())
            {
                var v = d.value();
                var n = d.key();
                MaybeAdd(cx,n, v);
                SelQuery()?.MaybeAdd(cx,n, v); 
                if (from.alias?.ident is string s && !s.Contains(":"))
                    n = new Ident(s, 0, Ident.IDType.Block, n);
                Ident.Tree<SqlValue>.Add(ref defs, n, v);
                if (v is SqlName sn)
                    sn.refs.Add(n);
                SelQuery()?.MaybeAdd(cx,n, v); 
            }
            base.Sources(cx); 
        }
        /// <summary>
        /// Analysis stage Selects: Do Selects on From, and setup any expressions in the GroupSpecifications
        /// </summary>
        internal override void Selects(Transaction tr,Query spec)
        {
            from.Selects(tr, spec);
            for (int i = 0; i < from.Size; i++)
                Add(tr,from.ValAt(i), from.names[i]);
            if (enc.aggregates() && ((!(from is From)) || !(((From)from).target is RestView)))
                for (var i=0;i<enc.cols.Count;i++)
                    if (enc.cols[i].Check(tr,group))
                        throw new DBException("42170", enc.cols[i].alias??enc.cols[i].name);
            simpleQuery = from.simpleQuery;
            display = Size;
            Scols();
            nominalDataType = from.nominalDataType;
            base.Selects(tr, spec);
        }
        internal override bool aggregates()
        {
            return from.aggregates() || base.aggregates();
        }
        internal override void Build(RowSet rs)
        {
            from.Build(rs);
            base.Build(rs);
        }
        internal override void StartCounter(Transaction tr, RowSet rs)
        {
            from.StartCounter(tr, rs);
            base.StartCounter(tr, rs);
        }
        internal override void AddIn(Transaction tr, RowSet rs)
        {
            from.AddIn(tr, rs);
            base.AddIn(tr, rs);
        }
        internal override void ReSelects(Transaction tr)
        {
            from.ReSelects(tr);
        }
        /// <summary>
        /// delegate accessibleColumns
        /// </summary>
        /// <returns>the selectors</returns>
        internal override Selector[] AccessibleCols()
        {
            return from.AccessibleCols();
        }
        internal override bool Knows(SqlTypeColumn c)
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
        internal override int Insert(string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey = false)
        {
            return from.Insert(prov, data, eqs, rs,cl,autokey);
        }
        /// <summary>
        /// propagate Delete operation
        /// </summary>
        /// <param name="dr">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override int Delete(Transaction tr,ATree<string, bool> dr, Adapters eqs)
        {
            return from.Delete(tr,dr,eqs);
        }
        /// <summary>
        /// propagate Update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Update(Transaction tr,ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return from.Update(tr,ur,eqs,rs);
        }
        internal override void AddMatches(Transaction tr, Query q)
        {
            for (var b = having.First(); b != null; b = b.Next())
                b.value().AddMatches(tr, q);
            from.AddMatches(tr, q);
            base.AddMatches(tr, q);
        }
        internal override void Conditions(Transaction tr, Query q)
        {
            MoveConditions(ref where, tr, from);
            MoveConditions(ref having, tr, from);
            AddPairs(from);
            from.Conditions(tr, q);
            if (group != null && group.sets.Count > 0)
            {
                for (var b = where.First(); b != null; b = b.Next())
                {
                    if (!having.Contains(b.key()))
                        ATree<long, SqlValue>.Add(ref having, b.key(), b.value());
                    ATree<long, SqlValue>.Remove(ref where, b.key());
                }
                // but avoid having-conditions that depend on aggregations
                var qs = QuerySpec(tr);
                for (var b=having.First();b!=null;b=b.Next())
                    if (b.value().Import(tr,this).For(tr,this)==null)
                    {
                        if (!qs.where.Contains(b.key()))
                            ATree<long, SqlValue>.Add(ref qs.where, b.key(), b.value());
                        ATree<long, SqlValue>.Remove(ref having, b.key());
                    }
            }
        }
        internal override void Orders(Transaction tr,OrderSpec ord)
        {
            base.Orders(tr,ord);
            from.Orders(tr,ord);
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            from.AddRestViews(q);
        }
        /// <summary>
        /// Add cond and/or update data to this query
        /// </summary>
        /// <param name="cond">a condition</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">some insert data</param>
        internal override void AddCondition(Transaction tr,ATree<long,SqlValue> cond, UpdateAssignment[] assigns, RowSet data)
        {
            base.AddCondition(tr,cond, assigns, data);
            from.AddCondition(tr,cond, assigns, data);
        }
        /// <summary>
        /// whether we have a column with the given Ident
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>whether we have it</returns>
        internal override bool HasColumn(Ident s)
        {
            if (from.HasColumn(s))
                return true;
            return base.HasColumn(s);
        }
        /// <summary>
        /// Analsyis stage RowSets: build the From rowsets; add the wheres; and perform grouping if required
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            if (rowSet == null || rowSet.tr!=tr)
            {
                from.RowSets(tr);
                rowSet = from.rowSet;
            }
            if (where.Count>0)
            {
                var gp = false;
                if (group != null)
                    foreach (var gs in group.sets)
                        gs.Grouped(where, ref gp);
            }
            base.RowSets(tr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(from.ToString());
            if (group != null)
            {
                sb.Append(" group by");
                if (group.distinct)
                    sb.Append(" distinct");
                var comma = " ";
                foreach(var ws in group.sets)
                {
                    sb.Append(comma);
                    sb.Append(ws.ToString());
                    comma = ",";
                }
                if (having != null)
                {
                    sb.Append(" having ");
                    sb.Append(having.ToString()); //remoteInfo));
                }
            }
            return sb.ToString();
        }
        /// <summary>
        /// close this query
        /// </summary>
        internal override void Close(Transaction tr)
        {
            from.Close(tr);
            base.Close(tr);
        }
    }
    /// <summary>
    /// A Join is implemented as a subclass of Query
    /// </summary>
    internal class JoinPart : SelectQuery
    {
        public override string Tag => "JP";
        /// <summary>
        /// NATURAL or USING or TEMPORAL or NO (the default)
        /// </summary>
        public Sqlx naturaljoin = Sqlx.NO;
        /// <summary>
        /// The list of common TableColumns for natural join
        /// </summary>
        internal Ident[] namedCols = null;
        /// <summary>
        /// the kind of Join
        /// </summary>
        internal Sqlx kind;
        /// <summary>
        /// The join condition is implemented by ordering, using any available indexes.
        /// Rows in the join will use left/rightInfo.Keys() for ordering and theta-operation.
        /// </summary>
        internal OrderSpec leftInfo = null; // initialised once nominalDataType is known
        internal OrderSpec rightInfo = null;
        /// <summary>
        /// During analysis, we collect requirements for the join conditions.
        /// </summary>
        internal ATree<long, SqlValue> joinCond = BTree<long, SqlValue>.Empty;
        /// <summary>
        /// The left element of the join
        /// </summary>
        public Query left = null;
        /// <summary>
        /// The right element of the join
        /// </summary>
        public Query right = null;
        /// <summary>
        /// A FD-join depends on a functional relationship between left and right
        /// </summary>
        internal FDJoinPart FDInfo = null;
        /// <summary>
        /// Constructor: a join part being built by the parser
        /// </summary>
        /// <param name="t"></param>
        internal JoinPart(Transaction t, string i) : base(t, i,RowType.Table) { }
        protected JoinPart(JoinPart j,ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs) :base(j,ref cs,ref vs)
        {
            naturaljoin = j.naturaljoin;
            namedCols = j.namedCols;
            kind = j.kind;
            leftInfo = j.leftInfo;
            rightInfo = j.rightInfo;
            left = j.left.Copy(ref cs,ref vs);
            right = j.right.Copy(ref cs,ref vs);
            for (var b = j.joinCond.First(); b != null; b = b.Next())
            {
                var ns = b.value().Copy(ref vs);
                ATree<long, SqlValue>.Add(ref joinCond, ns.sqid, ns);
            }
            if (j.FDInfo!=null)
                FDInfo = new FDJoinPart(j.FDInfo,vs);
            CopyContexts(j, cs, vs);
        }
        internal override Query Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new JoinPart(this,ref cs,ref vs);
        }
        internal override bool Knows(SqlTypeColumn c)
        {
            return left.Knows(c) || right.Knows(c);
        }
        internal override bool aggregates()
        {
            return left.aggregates()||right.aggregates()||base.aggregates();
        }
        /// <summary>
        /// Analysis stage Sources: collect the TableColumns.
        /// For natural sort out the common TableColumns
        /// </summary>
        /// <param name="cx">The context: for recording our alias as a table identifier</param>
        internal override void Sources(Context cx)
        {
            for (var b = right.contexts.First(); b != null; b = b.Next())
                if (!contexts.Contains(b.key()))
                    ATree<string, Context>.Add(ref contexts, b.key(), b.value());
            left.Sources(cx);
            right.Sources(cx);
            base.Sources(cx);
        }
        /// <summary>
        /// Analysis stage Selects: call for left and right.
        /// </summary>
        internal override void Selects(Transaction tr,Query spec) 
        {
            left.Selects(tr, spec);
            right.Selects(tr, spec);
            int n = left.display;
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
                    var ll = left.names[i];
                    for (int j = 0; j < rn; j++)
                    {
                        var rr = right.names[j];
                        if (ll.CompareTo(rr) == 0)
                        {
                            lc[i] = true;
                            rc[j] = true;
                            var cp = new SqlValueExpr(tr, Sqlx.EQL, left.ValAt(i), right.ValAt(j), Sqlx.NULL);
                            cp.left.Needed(tr, Need.joined);
                            cp.right.Needed(tr, Need.joined);
                            ATree<long, SqlValue>.Add(ref joinCond, cp.sqid, cp);
                            m++;
                            break;
                        }
                    }
                    if (!lc[i])
                        Add(tr,left.ValAt(i), ll);
                }
                for (int i = 0; i < n; i++)
                    if (lc[i])
                        Add(tr,left.ValAt(i), left.names[i]);

                for (int i = 0; i < rn; i++)
                    if (!rc[i])
                        Add(tr,right.ValAt(i), right.names[i]);
                if (m == 0)
                    kind = Sqlx.CROSS;
            }
            else
            {
                for (int j = 0; j < left.display; j++)
                {
                    var nm = left.names[j];
                    for (var i=0;i<right.cols.Count;i++)
                        if (right.cols[i].name.CompareTo(nm)==0)
                            nm = new Ident(left.FindAliasFor(nm), nm);
                    Add(tr,left.ValAt(j), nm);
                }
                for (int j = 0; j < right.display; j++)
                {
                    var nm = right.names[j];
                    for (var i=0;i<left.cols.Count;i++)
                        if (left.cols[i].name.CompareTo(nm)==0)
                            nm = new Ident(right.FindAliasFor(nm), nm);
                    Add(tr,right.ValAt(j), nm);
                }
            }
            // first ensure each joinCondition has the form leftExpr compare rightExpr
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
            }
            display = Size;
            base.Selects(tr, spec);
        }
        internal override void ReSelects(Transaction tr)
        {
            cols = BTree<long, SqlValue>.Empty;
            names = new Idents();
            for (int j = 0; j < left.display; j++)
            {
                var nm = left.names[j];
                if (right.ValFor(nm) != null)
                    nm = new Ident(left.alias.ident, 0, Ident.IDType.Block, nm);
                Add(tr, left.ValAt(j), nm);
            }
            for (int j = 0; j < right.display; j++)
            {
                var nm = right.names[j];
                if (left.ValFor(nm) != null)
                    nm = new Ident(right.alias.ident, 0, Ident.IDType.Block, nm);
                Add(tr, right.ValAt(j), nm);
            }
            nominalDataType = new TableType(this);
        }
        internal override void AddMatches(Transaction tr, Query q)
        {
            left.AddMatches(tr, q);
            right.AddMatches(tr, q);
            for (var b = joinCond.First(); b != null; b = b.Next())
                b.value().AddMatches(tr, q);
            base.AddMatches(tr, q);
        }
        internal override Ident FindAliasFor(Ident n)
        {
            return left.FindAliasFor(n) ?? right.FindAliasFor(n);
        }
        /// <summary>
        /// Analysis stage Conditions: call for left and right
        /// Turn the join condition into an ordering request, and set it up
        /// </summary>
        internal override void Conditions(Transaction t,Query q)
        {
            base.Conditions(t,q);
            if (kind==Sqlx.CROSS)
                kind = Sqlx.INNER;
            for (var b = joinCond.First(); b != null; b = b.Next())
                if (b.value() is SqlValueExpr se)
                    AddMatchedPair(se.left, se.right);
            left.AddPairs(this);
            right.AddPairs(this);
            for (var b = where.First(); b != null; b = b.Next())
                if (b.value().JoinCondition(t, this, ref joinCond, ref where))
                    ATree<long, SqlValue>.Remove(ref where,b.key());
            if (joinCond.Count == 0)
                kind = Sqlx.CROSS;
            left.Conditions(t,q);
            right.Conditions(t,q);
        }
        /// <summary>
        /// Now is the right time to optimise join conditions. 
        /// At this stage all comparisons have form left op right.
        /// Ideally we can find an index that makes at least some of the join trivial.
        /// Then we impose orderings for left and right that respect any remaining comparison join conditions,
        /// overriding ordering requests from top down analysis.
        /// </summary>
        /// <param name="ord">Requested top-down order</param>
        internal override void Orders(Transaction tr, OrderSpec ord)
        {
            var n = 0;
            // First try to find a perfect foreign key relationship, either way round
            if (GetRefIndex(tr, left, right, true) is FDJoinPart fa)
            {
                FDInfo = fa;
                n = (int)fa.conds.Count;
            }
            if (n < joinCond.Count && GetRefIndex(tr, right, left, false) is FDJoinPart fb && n < fb.index.cols.Length)
            {
                FDInfo = fb;
                n = (int)fb.conds.Count;
            }
            if (n > 0) // we will use this information instead of the left and right From rowsets
            {
                for (var b = FDInfo.conds.First(); b != null; b = b.Next())
                    ATree<long, SqlValue>.Remove(ref joinCond, b.value().sqid);
                if (FDInfo.reverse)
                {
                    left.rowSet = new IndexRowSet(tr, left as From, FDInfo.index, null);
                    right.rowSet = new IndexRowSet(tr, right as From, FDInfo.rindex, null);
                }
                else
                {
                    left.rowSet = new IndexRowSet(tr, left as From, FDInfo.rindex, null);
                    right.rowSet = new IndexRowSet(tr, right as From, FDInfo.index, null);
                }
                kind = Sqlx.NO;
            }
            else
            {
                // Now look to see if there is a suitable index for left or right
                if (GetIndex(tr, left, true) is FDJoinPart fc)
                {
                    FDInfo = fc;
                    n = (int)fc.conds.Count;
                }
                if (n < joinCond.Count && GetIndex(tr, right, false) is FDJoinPart fd && n < fd.index.cols.Length)
                {
                    FDInfo = fd;
                    n = (int)fd.conds.Count;
                }
                if (n > 0) //we will use the selected index instead of its From rowset: and order the other side for its rowset to be used
                {
                    if (FDInfo.reverse)
                        right.rowSet = new IndexRowSet(tr, right as From, FDInfo.index, null);
                    else
                        left.rowSet = new IndexRowSet(tr, left as From, FDInfo.index, null);
                    for (var b = FDInfo.conds.First(); b != null; b = b.Next())
                        ATree<long, SqlValue>.Remove(ref joinCond, b.value().sqid);
                    kind = Sqlx.NO;
                }
            }
            // Everything remaining in joinCond is not in FDInfo.conds
            for (var b = joinCond.First(); b != null; b = b.Next())
                if (b.value() is SqlValueExpr se) // we already know these have the right form
                {
                    var c = Push(tr);
                    left.ordSpec.itemsAdd(this,new OrderItem(tr, se.left.name));
                    right.ordSpec.itemsAdd(this,new OrderItem(tr, se.right.name));
                    Pop(tr, c);
                }
            if (joinCond.Count == 0)
                foreach (var i in ord.items) // we need to test all of these
                {
                    if (i.what.IsFrom(tr, left, true) && !(left.rowSet is IndexRowSet))
                        left.ordSpec.itemsAdd(this,i);
                    if (i.what.IsFrom(tr, right, true) && !(right.rowSet is IndexRowSet))
                        right.ordSpec.itemsAdd(this,i);
                }
            if (!(left.rowSet is IndexRowSet))
                left.Orders(tr, left.ordSpec);
            if (!(right.rowSet is IndexRowSet))
                right.Orders(tr, right.ordSpec);
        }
        /// <summary>
        /// See if there is a ForeignKey Index whose foreign key is taken from the one side of joinCond,
        /// and the referenced primary key is given by the corresponding terms on the other side.
        /// We will return null if the Queries are not Table Froms.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        FDJoinPart GetRefIndex(Transaction tr, Query a, Query b,bool left)
        {
            FDJoinPart best = null;
            if ((a as From)?.target is Table ta && (b as From)?.target is Table tb)
            {
                var db = tr.Db(ta.dbix);
                for (var bx = db.indexes.First(); bx != null; bx = bx.Next())
                    if (db.objects[bx.key()] is Index x)
                        if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                        && x.tabledefpos == ta.defpos && x.reftabledefpos == tb.defpos)
                        {
                            var cs = BTree<int,SqlValue>.Empty;
                            var rx = db.objects[x.refindexdefpos] as Index;
                            for (var i = 0; i < x.cols.Length; i++)
                            {
                                var found = false;
                                for (var bj = joinCond.First(); bj != null; bj = bj.Next())
                                    if (bj.value() is SqlValueExpr se
                                        && (left ? se.left : se.right) is SqlTypeColumn sc && sc.name.Defpos() == x.cols[i]
                                        && (left ? se.right : se.left) is SqlTypeColumn sd && sd.name.Defpos() == rx.cols[i])
                                    {
                                        ATree<int, SqlValue>.Add(ref cs, i, se);
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
            return best;
        }
        FDJoinPart GetIndex(Transaction tr,Query a,bool left)
        {
            FDJoinPart best = null;
            if ((a as From)?.target is Table ta)
            {
                var db = tr.Db(ta.dbix);
                for (var bx = db.indexes.First(); bx != null; bx = bx.Next())
                    if (db.objects[bx.key()] is Index x)
                        if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                            x.flags.HasFlag(PIndex.ConstraintType.Unique))
                        && x.tabledefpos == ta.defpos)
                        {
                            var cs = BTree<int,SqlValue>.Empty;
                            for (var i = 0; i < x.cols.Length; i++)
                            {
                                var found = false;
                                for (var bj = joinCond.First(); bj != null; bj = bj.Next())
                                    if (bj.value() is SqlValueExpr se
                                        && (left ? se.left : se.right) is SqlTypeColumn sc && sc.name.Defpos() == x.cols[i])
                                    {
                                        ATree<int, SqlValue>.Add(ref cs, i, se);
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
            return best;
        }
        /// <summary>
        /// propagate delete operation
        /// </summary>
        /// <param name="dr">a list of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override int Delete(Transaction tr,ATree<string, bool> dr, Adapters eqs)
        {
            return left.Delete(tr,dr,eqs) + right.Delete(tr,dr,eqs);
        }
        /// <summary>
        /// propagate an insert operation
        /// </summary>
        /// <param name="prov">the provenance</param>
        /// <param name="data">the insert data</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Insert(string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey=false)
        {
            Eqs(data.tr,joinCond,ref eqs); // add in equality columns
            return left.Insert(prov, data, eqs, rs,cl,autokey)+ // careful: data has extra columns!
            right.Insert(prov, data, eqs, rs,cl,autokey);
        }
        /// <summary>
        /// propagate an update operation
        /// </summary>
        /// <param name="ur">a set of versions</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">affected rowsets</param>
        internal override int Update(Transaction tr, ATree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return left.Update(tr,ur,eqs,rs) + right.Update(tr,ur,eqs,rs);
        }
        /// <summary>
        /// delegate accessibleCols
        /// </summary>
        /// <returns></returns>
        internal override Selector[] AccessibleCols()
        {
            var la = left.AccessibleCols();
            var ra = right.AccessibleCols();
            var r = new Selector[la.Length + ra.Length];
            for (int i = 0; i < la.Length; i++)
                r[i] = la[i];
            for (int i = 0; i < ra.Length; i++)
                r[i + la.Length] = ra[i];
            return r;
        }
        /// <summary>
        /// Check if we have a given column
        /// </summary>
        /// <param name="s">the name</param>
        /// <returns>whether we have it</returns>
        internal override bool HasColumn(Ident s)
        {
            if (left.HasColumn(s) || right.HasColumn(s))
                return true;
            return base.HasColumn(s);
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            left.AddRestViews(q);
            right.AddRestViews(q);
        }
        /// <summary>
        /// Distribute any new where condition to left and right
        /// </summary>
        /// <param name="cond">the condition to add</param>
        /// <param name="assigns">some update assignments</param>
        /// <param name="data">the insert data</param>
        internal override void AddCondition(Transaction tr,ATree<long,SqlValue> cond, UpdateAssignment[] assigns, RowSet data)
        {
            Ident.Tree<SqlValue> leftrepl = Ident.Tree<SqlValue>.Empty;
            Ident.Tree<SqlValue> rightrepl = Ident.Tree<SqlValue>.Empty;
            for (var b=joinCond.First();b!=null;b=b.Next())
                b.value().BuildRepl(tr,left, ref leftrepl, ref rightrepl);
            base.AddCondition(tr,cond, assigns, data);
            for(var b=cond.First();b!=null;b=b.Next())
            {
                b.value().DistributeConditions(tr,left, leftrepl, data);
                b.value().DistributeConditions(tr,right, rightrepl, data);
            }
            left.DistributeAssigns(assig);
            right?.DistributeAssigns(assig); 
        }
        /// <summary>
        /// Analysis stage RowSets: build the join rowset
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            var lfcols = BTree<int,ATree<long, bool>>.Empty;
            for (var d = matches.First(); d != null; d = d.Next())
                for (var b = d.value().First(); b != null; b = b.Next())
                {
                    var i = left.nominalDataType.names.ForPos(d.key(), b.key());
                    if (i >= 0)
                        left.AddMatch(d.key(), b.key(), b.value());
                    else
                        right.AddMatch(d.key(), b.key(), b.value());
                }
            left.RowSets(tr);
            right.RowSets(tr);
            rowSet = new JoinRowSet(tr, this);
            Ordering(false);
        }
        internal int Compare(Transaction tr)
        {
            for (var b=joinCond.First();b!=null;b=b.Next())
            {
                var se = b.value() as SqlValueExpr;
                var c = se.left.Eval(tr, rowSet)?.CompareTo(tr,se.right.Eval(tr,rowSet))??-1;
                if (c != 0)
                    return c;
            }
            return 0;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(left.ToString());
            if (naturaljoin != Sqlx.NO && naturaljoin != Sqlx.USING)
            {
                sb.Append(" ");
                sb.Append(naturaljoin.ToString());
            }
            if (kind != Sqlx.NO)
            {
                sb.Append(" ");
                sb.Append(kind.ToString());
            }
            sb.Append(" join");
            sb.Append(right.ToString());
            if (naturaljoin == Sqlx.USING)
            {
                var comma = " ";
                foreach(var ic in namedCols)
                {
                    sb.Append(comma);
                    sb.Append(ic.ToString());
                    comma = ",";
                }
            }
            CondString(sb, joinCond, " on ");
            return sb.ToString();
        }
        /// <summary>
        /// close the query
        /// </summary>
        internal override void Close(Transaction tr)
        {
            left.Close(tr);
            right.Close(tr);
            base.Close(tr);
        }
    }
    /// <summary>
    /// Information about functional dependency for join evaluation
    /// </summary>
    internal class FDJoinPart
    {
        /// <summary>
        /// The primary key Index giving the functional dependency
        /// </summary>
        public Index index = null;
        /// <summary>
        /// The foreign key index if any
        /// </summary>
        public Index rindex = null;
        /// <summary>
        /// The joinCond entries moved to this FDJoinPart: the indexing is hierarchical: 0, then 1 etc.
        /// </summary>
        public ATree<int, SqlValue> conds = BTree<int, SqlValue>.Empty;
        /// <summary>
        /// True if right holds the primary key
        /// </summary>
        public bool reverse = false;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ix">an index</param>
        /// <param name="s">the source expressions</param>
        /// <param name="d">the destination expressions</param>
        public FDJoinPart(Index ix, Index rx, ATree<int,SqlValue> c, bool r)
        {
            conds = c;
            index = ix;
            rindex = rx;
            reverse = r;
        }
        internal FDJoinPart(FDJoinPart f,ATree<long,SqlValue>vs)
        {
            index = f.index;
            rindex = f.rindex;
            reverse = f.reverse;
            for (var b = f.conds.First(); b != null; b = b.Next())
            {
                var ns = b.value().Copy(ref vs);
                ATree<int, SqlValue>.Add(ref conds,b.key(),vs[b.value().sqid]);
            }
        }
    }
}
