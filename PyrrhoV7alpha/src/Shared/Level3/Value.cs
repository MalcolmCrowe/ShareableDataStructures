using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
    /// The SqlValue class corresponds to the occurrence of identifiers and expressions in
    /// SELECT statements etc: they are evaluated in a RowSet or Activation Context  
    /// So Eval is a way of getting the current TypedValue for the identifier etc for the current
    /// rowset positions.
    /// SqlValues are constructed for every obs reference in the SQL source of a RowSet or Activation. 
    /// Many of these are SqlNames constructed for an identifier in a query: 
    /// during query analysis all of these must be resolved to a corresponding obs reference 
    /// (so that many SqlNames are resolved to the same thing). 
    /// An SqlValue’s home context is the RowSet, Activation, or SqlValue whose source defines it.
    /// Others are SqlLiterals for any constant obs in the SQL source and 
    /// SqlValues accessed by base tables referenced in the From part of a query. 
    /// Obviously some SqlValues will be rows or arrays: 
    /// The elements of row types are SqlColumns listed among the references of the row,
    /// there is an option to place the column TypedValues in its variables
    /// SqlNames are resolved for a given context. 
    /// This mechanism distinguishes between cases where the same identifier in SQL 
    /// can refer to different obs in different places in the source 
    /// (e.g. names used within stored procedures, view definitions, triggers etc). 
    /// 
    /// SqlValues are DBObjects from version 7. However, the base class can be transformed to
    /// other SqlValue sublasses on resolution, retaining the same defpos
    /// and possibly increasing (but never reducing) the depth of the SqlValue.
    /// They participate in rowset rewriting where expressions are interesting if they
    /// are locally constant (i.e. entirely needed) or entirely from views.
    /// shareable as of 26 April 2021
    /// </summary>
    internal class SqlValue : DBObject,IComparable
    {
        internal const long
            Left = -308, // long SqlValue
            Right = -309, // long SqlValue
            SelectDepth = -462, // int 
            Sub = -310; // long SqlValue
        internal long left => (long)(mem[Left]??-1L); 
        internal long right => (long)(mem[Right]??-1L);
        internal long sub => (long)(mem[Sub]??-1L);
        internal int selectDepth => (int)(mem[SelectDepth] ?? -1);
        public new string? name => (string?)mem[ObInfo.Name];
        internal virtual long target => defpos;
        public SqlValue(Ident nm,Domain? dt=null,BTree<long,object>?m=null)
            :base(nm.iix.dp,(m??BTree<long, object>.Empty)
                +(_Domain,dt?.defpos??Domain.Content.defpos)
                +(ObInfo.Name,nm.ident) + (_Ident,nm) + (SelectDepth,nm.iix.sd))
        { }
        protected SqlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        protected SqlValue(Context cx,string nm,Domain dt,long cf=-1L)
            :base(cx.GetUid(),_Mem(cf)+(ObInfo.Name,nm)+(_Domain,dt.defpos))
        {
            cx.Add(this);
        }
        static BTree<long,object> _Mem(long cf)
        {
            var r = BTree<long, object>.Empty;
            if (cf >= 0)
                r += (SqlCopy.CopyFrom, cf);
            return r;
        }
        public static SqlValue operator+(SqlValue s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlValue)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValue(defpos, m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is DBObject lf) r += lf._Rdc(cx);
            if (cx.obs[right] is DBObject rg) r += rg._Rdc(cx); 
            if (cx.obs[sub] is DBObject sb) r += sb._Rdc(cx);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[left]?.Calls(defpos, cx)==true || cx.obs[right]?.Calls(defpos,cx)==true;
        }
        internal virtual bool KnownBy(Context cx,RowSet q,bool ambient = false)
        {
            return q.Knows(cx, defpos, ambient);
        }
        internal virtual bool KnownBy<V>(Context cx,CTree<long,V> cs,bool ambient = false) 
            where V : IComparable
        {
            return cs.Contains(defpos);
        }
        internal virtual CTree<long,Domain> KnownFragments(Context cx,CTree<long,Domain> kb,
            bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (left >= 0 && cx.obs[left] is SqlValue le)
            { 
                r += le.KnownFragments(cx, kb,ambient);
                y = y && r.Contains(left);
            }
            if (right >= 0 && cx.obs[right] is SqlValue ri)
            {
                r += ri.KnownFragments(cx, kb,ambient);
                y = y && r.Contains(right);
            }
            if (sub >= 0 && cx.obs[sub] is SqlValue su)
            {
                r += su.KnownFragments(cx, kb,ambient);
                y = y && r.Contains(sub);
            }
            if (y && cx._Dom(this) is Domain td)
                return new CTree<long, Domain>(defpos, td);
            return r;
        }
        internal string? Alias(Context cx)
        {
            return alias ?? name ?? cx.Alias();
        }
        internal virtual Domain? _Dom(Context cx)
        {
            return cx._Dom(this);
        }

        internal override CTree<long, bool> Needs(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override CTree<long, bool> Needs(Context cx,long r)
        {
            var rs = (RowSet?)cx.obs[r];
            if (rs is RestRowSet rrs && rrs.remoteCols.Has(defpos))
                return CTree<long, bool>.Empty;
            if (rs?.Knows(cx,defpos,true) == true) // allow ambient access
                return CTree<long, bool>.Empty;
            return new CTree<long, bool>(defpos,true);
        }
        /// <summary>
        /// We call this.Define() if cx.defs[n,cx.sD] leads to this
        /// and this is at level sD and undefined (i.e. not a subclass)
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <param name="p">The instanced uid</param>
        /// <param name="f">The from rowSet</param>
        /// <param name="tc">The shared target object</param>
        /// <returns>f updated</returns>
        internal void Define(Context cx,long p,RowSet f,DBObject tc)
        {
            if (GetType().Name != "SqlValue" || name==null ||
                cx._Ob(p) is not SqlValue old)
                return;
            var m = new BTree<long, object>(_Domain, tc.domain);
            if (alias != null)
                m += (_Alias, alias);
            var nv = (tc is SqlValue sv)? sv
                : new SqlCopy(defpos, cx, name, f.defpos, tc, m);
            cx.done = ObTree.Empty;
            cx.Replace(this, nv);
            cx.done = ObTree.Empty;
            cx.Replace(old, nv);
        }
        internal virtual SqlValue Reify(Context cx)
        {
            var dm = cx._Dom(this);
            for (var b = dm?.rowType?.First(); b != null; b = b.Next())
            if (cx.role!=null && b.value() is long p && cx._Ob(p)?.infos[cx.role.defpos] is ObInfo ci 
                    && ci.name == name && name!=null)
                    return new SqlCopy(defpos, cx, name, defpos, p);
            return this;
        }
        internal virtual SqlValue AddFrom(Context cx,long q)
        {
            if (from > 0)
                return this;
            return (SqlValue)cx.Add(this + (_From, q));
        }
        internal static string For(Sqlx op)
        {
            return op switch
            {
                Sqlx.ASSIGNMENT => ":=",
                Sqlx.COLON => ":",
                Sqlx.EQL => "=",
                Sqlx.COMMA => ",",
                Sqlx.CONCATENATE => "||",
                Sqlx.DIVIDE => "/",
                Sqlx.DOT => ".",
                Sqlx.DOUBLECOLON => "::",
                Sqlx.GEQ => ">=",
                Sqlx.GTR => ">",
                Sqlx.LBRACK => "[",
                Sqlx.LEQ => "<=",
                Sqlx.LPAREN => "(",
                Sqlx.LSS => "<",
                Sqlx.MINUS => "-",
                Sqlx.NEQ => "<>",
                Sqlx.PLUS => "+",
                Sqlx.TIMES => "*",
                Sqlx.AND => " and ",
                _ => op.ToString(),
            };
        }
        internal virtual bool Grouped(Context cx,GroupSpecification gs)
        {
            return gs.Has(cx, defpos);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override string NameFor(Context cx)
        {
            return alias ?? name ?? "";
        }
        internal virtual bool AggedOrGrouped(Context cx,RowSet r)
        {
            var dm = cx._Dom(r)??Domain.Content;
            for (var b = dm.aggs.First(); b != null; b = b.Next())
            {
                var v = (SqlFunction?)cx.obs[b.key()]??throw new PEException("PE1700");
                if (v.kind == Sqlx.COUNT && v.mod == Sqlx.TIMES)
                    return true;
                if (v.Operands(cx).Contains(defpos))
                    return true;
            }
            if (r.groupCols is Domain gc)
            {
                if (gc.representation.Contains(defpos))
                    return true;
                for (var b = r.matching[defpos]?.First(); b != null; b = b.Next())
                    if (gc.representation.Contains(b.key()))
                        return true;
            }
            return false; 
        }
        /// <summary>
        /// We are building a rowset, and resolving entries in the select list.
        /// We return a BList only to help with SqlStar: otherwise the list always has just one entry.
        /// </summary>
        /// <param name="cx">the Context</param>
        /// <param name="f">the new From's defining position</param>
        /// <param name="m">the From properties</param>
        /// <returns>Top level selectors, and updated From properties</returns>
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            if ((GetType().Name != "SqlValue" || cx.obs[from] is VirtualTable) && domain != Domain.Content.defpos)
                return (new BList<DBObject>(this), m);
            var r = this;
            var ns = (BTree<string, long?>?)m[ObInfo.Names] ?? BTree<string, long?>.Empty;
  /*          if (infos.Contains(cx.role.defpos))
            {
                var ic = new Ident(new Ident(infos[cx.role.defpos]?.name, cx.Ix(f)), id);
                if (cx.obs[cx.defs[ic].dp] is DBObject tg)
                {
                    var cp = (tg is SqlCopy sc) ? sc.copyFrom : tg.defpos;
                    var nc = new SqlCopy(defpos, cx, name, f, cp);
                    if (alias != "")
                        nc += (_Alias, alias);
                    if (nc.defpos < Transaction.Executables && nc.defpos < tg.defpos)
                        m = cx.Replace(tg, nc, m);
                    else
                        m = cx.Replace(this, nc, m);
                    r = nc;
                }
            }*/
            if (r == this)
            {
                if (name!=null && ns.Contains(name) && cx.obs[ns[name]??-1L] is SqlValue sv 
                    && sv.GetType().Name!="SqlValue" && sv.domain != Domain.Content.defpos)
                {
                    var nv = (SqlValue)sv.Relocate(defpos);
                    if (alias is string a)
                        nv += (_Alias, a);
                    cx.undefined -= defpos;
                    m = cx.Replace(sv, nv, m);
                    cx.Add(nv);
                    r = nv;
                }
            }
 /*           if (r == this)
            {
                var dm = (Domain)cx.obs[(long)(m[_Domain]??-1L)]??cx._Dom(f);
                if (dm!=null && cx.defs.Contains(name))
                {
                    var ob = cx.obs[cx.defs[(name, cx.sD)].Item1.dp];
                    if (cx._Dom(ob)?.kind != Sqlx.CONTENT && ob?.defpos != defpos
                        && ob is SqlValue sb && ob.GetType().Name!="SqlValue")
                    {
                        var nc = new SqlValue(new Ident(name,new Iix(lexical,cx,defpos),null), cx._Dom(sb), 
                            mem + (_From,ob.from));
                        if (alias != "")
                            nc += (_Alias, alias);
                        r = nc;
                        cx.Add(r);
                        cx.undefined -= defpos;
                    }
                }
            } */
            if (r != this)
            {
                if (m[Table.Indexes] is CTree<Domain,CTree<long,bool>> ixs)
                {
                    var xs=CTree<Domain,CTree<long,bool>>.Empty;
                    for (var b = ixs.First(); b != null; b = b.Next())
                        xs += (b.key().Replaced(cx), cx.ReplacedTlb(b.value()));
                    m += (Table.Indexes, xs);
                }
                if (m[Index.Keys] is Domain d)
                    m += (Index.Keys, d.Replaced(cx));
            }
            return (new BList<DBObject>(r), m);
        }
        /// <summary>
        /// Eval is used to deliver the TypedValue for the current Cursor if any
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values[defpos] is TypedValue v)
                return v;
            if (cx.cursors[from] is Cursor cu && cu.values[defpos] is TypedValue w)
                return w;
            // check for lateral case
            if (cx.obs[from] is RowSet r)
                for (var b = r.Sources(cx).First(); b != null; b = b.Next())
                    if (cx.cursors[b.key()] is TypedValue sv)
                        return sv;
            return _Default();
        }
        internal override void Set(Context cx, TypedValue v)
        {
            base.Set(cx, v);
            if (cx.obs[from] is RowSet rs
                && cx.cursors[rs.defpos] is Cursor cu)
            {
                cu += (cx, defpos, v);
                cx.cursors += (rs.defpos, cu);
            }
        }
        internal virtual CTree<long,bool> Disjoin(Context cx)
        {
            return new CTree<long,bool>(defpos, true);
        }
        /// <summary>
        /// Used for Window Function evaluation.
        /// Called by GroupingBookmark (when not building) and SelectedCursor
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(Context cx,Cursor bmk)
        { }
        /// <summary>
        /// test whether the given SqlValue is structurally equivalent to this (always has the same value in this context)
        /// </summary>
        /// <param name="v">The expression to test against</param>
        /// <returns>Whether the expressions match</returns>
        internal virtual bool _MatchExpr(Context cx,SqlValue v,RowSet r)
        {
            return v!=null && (defpos==v.defpos ||
                (r.matching[defpos] is CTree<long,bool> t && t.Contains(v.defpos)));
        }
        /// <summary>
        /// analysis stage conditions(): test to see if this predicate can be distributed.
        /// </summary>
        /// <param name="q">the query to test</param>
        /// <param name="ut">(for RestView) a usingTableType</param>
        /// <returns>true if the whole of thsi is provided by q and/or ut</returns>
        internal virtual bool IsFrom(Context cx,RowSet q, bool ordered=false, Domain? ut=null)
        {
            return false;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var dm = cx.ObReplace(domain, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
            var ag = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = IsAggregation(cx).First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = p;
                if (p == so.defpos)
                {
                    np = sv.defpos;
                    ch = ch || p != np;
                }
                ag += (np,true);
            }
            if (ch)
                r += (Domain.Aggs, ag);
            return r;
        }
        /// <summary>
        /// During From construction we want the From to supply the columns needed by a query.
        /// We will look these up in the souurce table ObInfo. For now we create a derived
        /// Selection structure that contains only simple SqlValues or SqlTableCols.
        /// The SqlValues will have usable uids. The SqlTableCol uids will need to be replaced,
        /// but we don't do that just now.
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal virtual CTree<long,bool> Needs(Context cx, long r, CTree<long,bool> qn)
        {
            return qn.Contains(defpos)?qn:(qn + (defpos,true));
        }
        internal virtual bool isConstant(Context cx)
        {
            return false;
        }
        internal bool? Matches(Context cx)
        {
           return (Eval(cx) is TBool tb)?tb.value:null;
        }
        internal virtual bool HasAnd(Context cx,SqlValue s)
        {
            return s == this;
        }
        internal virtual CTree<long,bool> IsAggregation(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        internal virtual SqlValue Invert(Context cx)
        {
            return new SqlValueExpr(cx.GetUid(), cx, Sqlx.NOT, this, null, Sqlx.NO);
        }
        internal static bool OpCompare(Sqlx op, int c)
        {
            return op switch
            {
                Sqlx.EQL => c == 0,
                Sqlx.NEQ => c != 0,
                Sqlx.GTR => c > 0,
                Sqlx.LSS => c < 0,
                Sqlx.GEQ => c >= 0,
                Sqlx.LEQ => c <= 0,
                _ => throw new PEException("PE61"),
            };
        }
        internal virtual RowSet RowSetFor(long dp,Context cx,BList<long?> us,
            CTree<long,Domain> re)
        {
            if (cx.val is TRow r)
                return new TrivialRowSet(dp,cx, r);
            return new EmptyRowSet(dp,cx,domain,us,re);
        }
        internal virtual Domain FindType(Context cx,Domain dt)
        {
            var dm = cx._Dom(this);
            if (dm == null || dm.kind==Sqlx.CONTENT)
                return dt;
            if (dt.kind!=Sqlx.TABLE && dt.kind!=Sqlx.ROW)
                dm = dm.Scalar(cx);
            if (!dt.CanTakeValueOf(dm))
                return dt;
         //       throw new DBException("22005", dt.kind, dm.kind);
            if ((isConstant(cx) && dm.kind == Sqlx.INTEGER) || dm.kind==Sqlx.Null)
                return dt; // keep union options open
            return dm;
        }
        /// <summary>
        /// Transform this, replacing aggregation subexpressions by aggregation columns of dm
        /// The expression must be functionally dependent on dm
        /// </summary>
        /// <param name="c"></param>
        /// <param name="dm"></param>
        /// <returns></returns>
        internal virtual SqlValue Having(Context c, Domain dm)
        {
            throw new DBException("42112", ToString());
        }
        internal virtual bool Match(Context c,SqlValue v)
        {
            return v!=null && defpos == v.defpos;
        }
        public virtual int CompareTo(object? obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        internal virtual SqlValue Constrain(Context cx,Domain dt)
        {
            var dm = cx._Dom(this)??Domain.Content;
            var nd = dm.Constrain(cx, cx.GetUid(), dt) ??
                throw new DBException("22000", Sqlx.MINUS);
            if (dm != nd)
            {
                cx.Add(nd);
                return (SqlValue)cx.Add(this + (_Domain, nd.defpos));
            }
            return this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (id == null && name!="") { sb.Append(' '); sb.Append(name); }
            if (mem.Contains(_From)) { sb.Append(" From:"); sb.Append(Uid(from)); }
            if (alias != null) { sb.Append(" Alias="); sb.Append(alias); }
            if (left!=-1L) { sb.Append(" Left:"); sb.Append(Uid(left)); }
            if (right!=-1L) { sb.Append(" Right:"); sb.Append(Uid(right)); }
            if (sub!=-1L) { sb.Append(" Sub:"); sb.Append(Uid(sub)); }
            return sb.ToString();
        }
        internal override string ToString(string sg, Remotes rf, 
            BList<long?> cs, CTree<long, string> ns, Context cx)
        {
            switch (rf)
            {
                case Remotes.Selects:
                    if (ns[defpos] is string ss)
                        return ss;
                    var an = alias ?? name ?? "";
                    if (!cs.Has(defpos))
                    {
                        var v = Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return "";
                        var s = v.ToString(cs, cx);
                        if (an != "")
                            s += " as " + an;
                        return s;
                    }
                    break;
                case Remotes.Operands:
                    if (ns[defpos] is string so)
                        return so;
                    if (!cs.Has(defpos))
                        return Eval(cx)?.ToString(cs, cx)??"";
                    break;
            }
            return name??"";
        }
        /// <summary>
        /// Compute relevant equality pairings.
        /// Currently this is only for EQL joinConditions
        /// </summary>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual void Eqs(Context cx,ref Adapters eqs)
        {
        }
        internal virtual int Ands(Context cx)
        {
            return 1;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlValue(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nf = cx.Fix(from);
            if (from!=nf)
                r += (_From, nf);
            var nl = cx.Fix(left);
            if (left!=nl)
                r += (Left, nl);
            var nr = cx.Fix(right);
            if (right!=nr)
                r += (Right, nr);
            var nd = cx.Fix(domain);
            if (nd!=domain)
            r += (_Domain,nd);
            var ns = cx.Fix(sub);
            if (sub!=ns)
                r += (Sub, ns);
            return r;
        }
        internal SqlValue Sources(Context cx)
        {
            var r = (SqlValue)Fix(cx);
            if (r == this)
                return this;
            var np = cx.GetUid();
            r = (SqlValue)r.Relocate(np);
            cx.Add(r);
            cx.uids += (defpos, np);
            return r;
        }
    }
    // shareable as of 26 April 2021
    internal class SqlCopy : SqlValue
    {
        internal const long
            CopyFrom = -284; // long
        public long copyFrom => (long)(mem[CopyFrom]??-1L);
        public SqlCopy(long dp, Context cx, string nm, long fp, long cp,
            BTree<long, object>? m = null)
            : this(dp, cx, nm,fp, cx.obs[cp]??(DBObject?)cx.db.objects[cp], m)
        {
            cx.undefined -= dp;
        }
        public SqlCopy(long dp, Context cx, string nm, long fp, DBObject? cf,
           BTree<long, object>? m = null)
            : base(dp, _Mem(fp, cf, m) + (ObInfo.Name,nm)  + (_From,fp))
        {
            cx.undefined -= dp;
            if (dp == cf?.defpos) // someone has forgotten the from clause
                throw new DBException("42112", nm);
        }
        static BTree<long,object> _Mem(long fp,DBObject? cf, BTree<long,object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (fp>=0)
                m += (_From, fp);
            if (cf != null)
                m = m + (CopyFrom, cf.defpos) + (_Domain, cf.domain);
            return m;
        }
        protected SqlCopy(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static SqlCopy operator +(SqlCopy s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlCopy)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCopy(defpos, m);
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n)
        {
            if (n == null)
                return (this, null);
            if (cx.obs[from] is RowSet f && cx._Dom(f) is Domain dm)
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) == n.ident && cx.obs[p] is TableColumn c){
                    var ob = new SqlField(n.iix.dp, n.ident, defpos, c.domain, c.defpos);
                    cx.Add(ob);
                    return (ob, n.sub);
                }
            return base._Lookup(lp, cx,nm, n);
        }
        internal override bool IsFrom(Context cx, RowSet q, bool ordered = false, Domain? ut = null)
        {
            return cx._Dom(q)?.representation.Contains(defpos)==true;
        }
        internal override bool KnownBy(Context cx, RowSet r, bool ambient = false)
        {
            return r.Knows(cx, defpos, ambient);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient=false)
        {
            if (kb[defpos] is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            if (kb[copyFrom] is Domain dc)
                return new CTree<long, Domain>(defpos, dc);
            return CTree<long,Domain>.Empty;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v == null)
                return false;
            if (defpos == v.defpos)
                return true;
            if (v is SqlCopy sc && copyFrom == sc.copyFrom)
                return true;
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return (copyFrom<Transaction.Executables)?
                new CTree<long,bool>(copyFrom,true): CTree<long,bool>.Empty;
        }
        internal override CTree<long,bool> Operands(Context cx)
        {
            var r = new CTree<long, bool>(defpos, true);
            if (cx.obs[copyFrom] is SqlValue s)
                r += s.Operands(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlCopy(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m) 
        {
            var r = base._Fix(cx,m);
            // This exception is a hack: if copyFrom is in this range,
            // it must be a virtual column (eg for RestView).
            // Making a special VirtualColumn class would not make the code any more readable
            if (copyFrom < Transaction.Executables || copyFrom >= Transaction.HeapStart)
            {
                var nc = cx.Fix(copyFrom);
                if (nc != copyFrom)
                    r += (CopyFrom, nc);
            }
            return r;
        }
        internal override TypedValue Eval(Context cx)
        {
            if (cx is CalledActivation ca)
            {
                if (ca.locals.Contains(copyFrom))
                    return cx.values[copyFrom]??TNull.Value;
            }
            else if (cx.obs[copyFrom] is SqlElement)
                return cx.values[copyFrom]??TNull.Value;
            if (cx.obs[copyFrom] is TableColumn tc && tc.framing.obs != ObTree.Empty)
                cx.Add(tc.framing);
            if (from < 0 && cx.values[copyFrom] is TypedValue tv && tv != TNull.Value)
                return tv;
            return base.Eval(cx);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.obs[copyFrom]?.Set(cx,v);
            base.Set(cx, v);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            if (((RowSet?)cx.obs[rs])?.Knows(cx,cf) == true)
                return CTree<long, bool>.Empty;
            return (cf<-1L)?CTree<long,bool>.Empty
                : base.Needs(cx, rs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" copy from "); sb.Append(Uid(copyFrom));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A TYPE value for use in CAST
    ///     // shareable as of 26 April 2021
    /// </summary>
    internal class SqlTypeExpr : SqlValue
    {
        /// <summary>
        /// constructor: a new Type expression
        /// </summary>
        /// <param name="ty">the type</param>
        internal SqlTypeExpr(long dp,Domain ty)
            : base(dp,BTree<long, object>.Empty + (_Domain, ty.defpos))
        {}
        protected SqlTypeExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeExpr operator +(SqlTypeExpr s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlTypeExpr)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeExpr(defpos, m);
        }
        /// <summary>
        /// Lookup the type name in the context
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5000");
            return new TTypeSpec(dm);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlTypeExpr(dp, m);
        }
    }
    /// <summary>
    /// A Subtype value for use in TREAT
    /// shareable as of 26 April 2021
    /// </summary>
    internal class SqlTreatExpr : SqlValue
    {
        internal const long
            TreatExpr = -313; // long SqlValue
        long val => (long)(mem[TreatExpr]??-1L);
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(long dp,SqlValue v,Domain ty)
            : base(dp,BTree<long, object>.Empty + (_Domain,ty.defpos) +(TreatExpr,v.defpos)
                  +(_Depth,v.depth+1))
        { }
        protected SqlTreatExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTreatExpr operator +(SqlTreatExpr s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlTreatExpr)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTreatExpr(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlTreatExpr(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.Fix(val);
            if (ne!=val)
            r += (TreatExpr, ne);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlTreatExpr)base.AddFrom(cx, q);
            var a = ((SqlValue?)cx.obs[val])?.AddFrom(cx, q);
            if (a!=null && a.defpos != val)
                r += (TreatExpr, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var v = cx.ObReplace(val, so, sv);
            if (v != val)
                r += (TreatExpr, v);
            // watch out for changes required to the target Domain 
            // (this algorithm really only works for a simple TreatExpr)
            if ((so.defpos == val || sv.defpos==val) && cx._Dom(v) is Domain dm
                && cx._Dom(so) is Domain od && cx._Dom(sv) is Domain vd &&
                dm.defpos >= Transaction.HeapStart)
                    cx.Replace(dm,(Domain)vd.New(vd.mem + Diff(od, dm)));
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            SqlValue r = this;
            if (cx.obs[val] is SqlValue ol)
            {
                BList<DBObject> ls;
                (ls, m) = ol.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv  && nv.defpos != val)
                {
                    r = new SqlTreatExpr(defpos, nv, cx._Dom(this)??throw new PEException("PE8400"));
                    m = cx.Replace(this, r, m);
                }
            }
            if (cx._Dom(this) is Domain dm && dm.kind==Sqlx.CONTENT && dm.defpos>=0 &&
                cx._Dom(val) is Domain dv && dv.kind!=Sqlx.CONTENT)
            {
                var nd = (Domain)dm.New(cx,dv.mem + (dm.mem - Domain.Kind));
                cx.Add(nd);
                if (nd.defpos != domain)
                {
                    r += (_Domain, nd.defpos);
                    cx.Add(r);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        static BTree<long,object> Diff(Domain a,Domain b)
        {
            var r = a.mem;
            for (var bm=b.mem.First();bm!=null;bm=bm.Next())
            {
                var k = bm.key();
                var v = bm.value();
                switch(k)
                {
                    case _Depth:
                    case ObInfo.Name:
                    case Domain.Kind:
                        r -= k;
                        continue;
                }    
                if ((!r.Contains(k)) || a.mem[k]!=v)
                    r += (k, v);
            }
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            return ((SqlValue?)cx.obs[val])?.IsAggregation(cx)??CTree<long,bool>.Empty;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, q, ambient)??false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient)??false;
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tv = cx.obs[val]?.Eval(cx) ?? TNull.Value;
            if (cx._Dom(this)?.HasValue(cx, tv) != true)
                throw new DBException("2200G", cx._Dom(this)?.ToString() ?? "??",
                    cx._Dom(cx.obs[val])?.ToString() ?? "??").ISO();
            return tv;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return ((SqlValue?)cx.obs[val])?.Needs(cx,r,qn)??qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            return ((SqlValue?)cx.obs[val])?.Needs(cx, rs)??CTree<long,bool>.Empty;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Val= "); sb.Append(Uid(val));
            return sb.ToString();
        }
    }
    internal class SqlCaseSimple : SqlValue
    {
        internal const long 
            Cases = -466,       // BList<(long,long)> SqlValue SqlValue 
            CaseElse = -228;    // long SqlValue
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public BList<(long, long)> cases =>
            (BList<(long,long)>?)mem[Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[CaseElse] ?? -1L);
        internal SqlCaseSimple(long dp, Domain dm, SqlValue vl, BList<(long, long)> cs, long el)
            : base(dp, BTree<long, object>.Empty + (_Domain, dm.defpos)
                  + (SqlFunction._Val, vl.defpos) + (Cases, cs) + (CaseElse, el))
        { }
        protected SqlCaseSimple(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCaseSimple operator+(SqlCaseSimple s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlCaseSimple)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCaseSimple(defpos,m);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is SqlValue sw)
                    r += sw.Needs(cx);
                if (cx.obs[x] is SqlValue sx)
                    r += sx.Needs(cx);
            }
            if (cx.obs[caseElse] is SqlValue se)
                r += se.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx,long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is SqlValue sw)
                    r += sw.Needs(cx,rs);
                if (cx.obs[x] is SqlValue sx)
                    r += sx.Needs(cx,rs);
            }
            if (cx.obs[caseElse] is SqlValue se)
                r += se.Needs(cx,rs);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn + Needs(cx);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return Needs(cx);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s && !s.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this)??throw new PEException("PE1910");
            var oc = cx.values;
            var v = cx.obs[val]?.Eval(cx)??TNull.Value;
            for (var b = cases.First();b!=null;b=b.Next())
            {
                var (w, r) = b.value();
                if (dm.Compare(v, cx.obs[w]?.Eval(cx)??TNull.Value) == 0)
                {
                    var x = cx.obs[r]?.Eval(cx)??TNull.Value;
                    cx.values = oc;
                    return x;
                }
            }
            var e = cx.obs[caseElse]?.Eval(cx)??TNull.Value;
            cx.values = oc;
            return e;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject>? ls;
            var r = this;
            var ch = false;
            var d = depth;
            SqlValue v = (SqlValue?)cx.obs[val] ?? SqlNull.Value,
                ce = (SqlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long,long)>.Empty;
            if (v!=SqlNull.Value)
            {
                (ls, m) = v.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=v.defpos)
                {
                    ch = true; v = nv;
                    d = Math.Max(d, v.depth + 1);
                }
            }
            if (ce!=SqlNull.Value)
            {
                (ls, m) = ce.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
                    d = Math.Max(d, ce.depth + 1);
                }
            }
            for (var b=cases.First();b!=null;b=b.Next())
            {
                var (s, c) = b.value();
                SqlValue sv = (SqlValue?)cx.obs[s] ?? SqlNull.Value,
                    sc = (SqlValue?)cx.obs[c] ?? SqlNull.Value;
                if (sv!=SqlNull.Value)
                {
                    (ls, m) = sv.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; sv = nv;
                        d = Math.Max(d,sv.depth + 1);
                    }
                }
                if (sc!=SqlNull.Value)
                {
                    (ls, m) = sc.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; sc = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                }
                css += (sc.defpos, sv.defpos);
            }
            if (ch)
            {
                r = new SqlCaseSimple(defpos, cx._Dom(this) ?? throw new DBException("42105"), v, css, ce.defpos);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlCaseSimple(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nv = cx.Fix(val);
            if (nv != val)
                r += (SqlFunction._Val, nv);
            var nc = cx.FixLll(cases);
            if (nc != cases)
                r += (Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r += (CaseElse, ne);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var vl = cx.ObReplace(val, so, sv);
            if (vl != val)
                r += (SqlFunction._Val, vl);
            var ch = false;
            var nc = BList<(long, long)>.Empty;
            for (var b=cases.First();b!=null;b=b.Next())
            {
                var (w, x) = b.value();
                var nw = cx.ObReplace(w, so, sv);
                var nx= cx.ObReplace(x, so, sv);
                nc += (nw, nx);
                ch = ch || (nw != w) || (nx != x);
            }
            if (ch)
                r += (Cases, nc);
            var ne = cx.ObReplace(caseElse, so, sv);
            if (ne != caseElse)
                r += (CaseElse, ne);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(Uid(val));
            var cm = "{";
            for (var b=cases.First();b!=null;b=b.Next())
            {
                var (w,r) = b.value();
                sb.Append(cm);cm = ",";
                sb.Append(Uid(w));sb.Append(':');
                sb.Append(Uid(r));
            }
            sb.Append('}');sb.Append(Uid(caseElse));
            return sb.ToString();
        }
    }
    internal class SqlCaseSearch : SqlValue
    {
        public BList<(long, long)> cases =>
            (BList<(long, long)>?)mem[SqlCaseSimple.Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[SqlCaseSimple.CaseElse] ?? -1L);
        internal SqlCaseSearch(long dp, Domain dm, BList<(long, long)> cs, long el)
            : base(dp, BTree<long, object>.Empty + (_Domain, dm.defpos)
                  + (SqlCaseSimple.Cases, cs) + (SqlCaseSimple.CaseElse, el))
        { }
        protected SqlCaseSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCaseSearch operator +(SqlCaseSearch s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlCaseSearch)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCaseSearch(defpos, m);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is SqlValue sw)
                    r += sw.Needs(cx);
                if (cx.obs[x] is SqlValue sx)
                    r += sx.Needs(cx);
            }
            if (cx.obs[caseElse] is SqlValue se)
                r += se.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is SqlValue sw)
                    r += sw.Needs(cx,rs);
                if (cx.obs[x] is SqlValue sx)
                    r += sx.Needs(cx,rs);
            }
            if (cx.obs[caseElse] is SqlValue se)
                r += se.Needs(cx,rs);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn+ Needs(cx);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return Needs(cx);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s && !s.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override TypedValue Eval(Context cx)
        {
            var oc = cx.values;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, r) = b.value();
                if (cx.obs[w]?.Eval(cx) == TBool.True)
                {
                    var v = cx.obs[r]?.Eval(cx)??TNull.Value;
                    cx.values = oc;
                    return v;
                }
            }
            var e = cx.obs[caseElse]?.Eval(cx)??TNull.Value;
            cx.values = oc;
            return e;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlCaseSearch(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.FixLll(cases);
            if (nc != cases)
                r += (SqlCaseSimple.Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r += (SqlCaseSimple.CaseElse, ne);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ch = false;
            var nc = BList<(long, long)>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                var nw = cx.ObReplace(w, so, sv);
                var nx = cx.ObReplace(x, so, sv);
                nc += (nw, nx);
                ch = ch || (nw != w) || (nx != x);
            }
            if (ch)
                r += (SqlCaseSimple.Cases, nc);
            var ne = cx.ObReplace(caseElse, so, sv);
            if (ne != caseElse)
                r += (SqlCaseSimple.CaseElse, ne);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject>? ls;
            var r = this;
            var ch = false;
            var d = depth;
            SqlValue ce = (SqlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long, long)>.Empty;
            if (ce != SqlNull.Value)
            {
                (ls, m) = ce.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (s, c) = b.value();
                SqlValue sv = (SqlValue?)cx.obs[s] ?? SqlNull.Value,
                    sc = (SqlValue?)cx.obs[c] ?? SqlNull.Value;
                if (sv != SqlNull.Value)
                {
                    (ls, m) = sv.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != sv.defpos)
                    {
                        ch = true; sv = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                }
                if (sc != SqlNull.Value)
                {
                    (ls, m) = sc.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != sc.defpos)
                    {
                        ch = true; sc = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                }
                css += (sc.defpos, sv.defpos);
            }
            if (ch)
            {
                r = new SqlCaseSearch(defpos, cx._Dom(this) ?? throw new DBException("42105"), css, ce.defpos);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "{";
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, r) = b.value();
                sb.Append(cm); cm = ",";
                sb.Append(Uid(w)); sb.Append(':');
                sb.Append(Uid(r));
            }
            sb.Append('}'); sb.Append(Uid(caseElse));
            return sb.ToString();
        }
    }
    internal class SqlField : SqlValue
    {
        internal const long
            Field = -315, // long TableColumn
            Parent = -384; // long SqlValue
        public long field => (long)(mem[Field] ?? -1L);
        public long parent => (long)(mem[Parent] ?? -1L);
        internal SqlField(long dp, string nm, long pa, long dt, long fc)
            : base(dp, BTree<long, object>.Empty + (ObInfo.Name, nm)
                  + (Parent, pa) + (Field, fc) + (_Domain,dt))
        { }
        protected SqlField(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlField operator +(SqlField s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlField)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlField(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlField(dp, m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlField sf)
                return field == sf.field && parent == sf.parent;
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.obs[parent]?.Eval(cx)?[field]??TNull.Value;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var ch = false;
            var d = depth;
            SqlValue sf = (SqlValue?)cx.obs[field]??SqlNull.Value, 
                sp = (SqlValue?)cx.obs[parent]??SqlNull.Value;
            if (sf!=SqlNull.Value)
            {
                (ls, m) =sf.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != sf.defpos)
                {
                    ch = true; sf = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (sp!=SqlNull.Value)
            {
                (ls, m) = sp.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=sp.defpos)
                {
                    ch = true; sp = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch && name!=null)
            {
                r = new SqlField(defpos, name, sp.defpos, domain, sf.defpos);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r),m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Parent="); sb.Append(Uid(parent));
            sb.Append(" Field="); sb.Append(Uid(field));
            return sb.ToString();
        }
    }
    /// shareable as of 26 April 2021
    internal class SqlElement : SqlValue
    {
        internal SqlElement(Ident nm,Ident pn,Domain dt) 
            : base(nm,dt,BTree<long, object>.Empty+(_From,pn.iix.dp))
        { }
        protected SqlElement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlElement operator +(SqlElement s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlElement)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlElement(defpos,m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            return (cx.obs[from] is SqlValue)?
                new CTree<long,bool>(from,true): base.Needs(cx,r, qn);
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.values[defpos]??TNull.Value;
        }
        internal override string ToString(string sg,Remotes rf,BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder(cx.obs[from]?.ToString(sg,rf,cs,ns,cx)??"");
            sb.Append('['); sb.Append(name); sb.Append(']'); 
            return sb.ToString();
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlElement(dp,m);
        }
    }
    /// <summary>
    /// A SqlValue expression structure.
    /// Various additional operators have been added for JavaScript: e.g.
    /// modifiers BINARY for AND, OR, NOT; EXCEPT for (binary) XOR
    /// ASC and DESC for ++ and -- , with modifier BEFORE
    /// QMARK and COLON for ? :
    /// UPPER and LOWER for shifts (GTR is a modifier for the unsigned right shift)
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlValueExpr : SqlValue
    {
        internal const long
            Modifier = -316; // Sqlx
        public Sqlx kind => (Sqlx)(mem[Domain.Kind]??Sqlx.Null);
        /// <summary>
        /// the modifier (e.g. DISTINCT)
        /// </summary>
        public Sqlx mod => (Sqlx)(mem[Modifier]??Sqlx.NO);
        /// <summary>
        /// constructor for an SqlValueExpr
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">an operator</param>
        /// <param name="lf">the left operand</param>
        /// <param name="rg">the right operand</param>
        /// <param name="m">a modifier (e.g. DISTINCT)</param>
        public SqlValueExpr(long dp, Context cx, Sqlx op, SqlValue? lf, SqlValue? rg, 
            Sqlx m, BTree<long, object>? mm = null)
            : base(dp, _Type(cx, op, m, lf, rg, mm)
                  + (Modifier, m) + (Domain.Kind, op) 
                  +(Dependents,new CTree<long,bool>(lf?.defpos??-1L,true)+(rg?.defpos??-1L,true))
                  +(_Depth,1+_Max((lf?.depth??0),(rg?.depth??0))))
        { }
        protected SqlValueExpr(long dp, BTree<long, object> m) : base(dp, m) 
        {  }
        public static SqlValueExpr operator +(SqlValueExpr s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlValueExpr(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlValueExpr(dp,m);
        }
        internal override (BList<DBObject>,BTree<long,object>) Resolve(Context cx, long f, BTree<long,object> m)
        {
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            SqlValue lf = (SqlValue?)cx.obs[left]??SqlNull.Value, 
                rg = (SqlValue?)cx.obs[right]??SqlNull.Value, 
                su = (SqlValue?)cx.obs[sub]??SqlNull.Value;
            var d = depth;
            if (lf!=SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != lf.defpos)
                {
                    ch = true; lf = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (rg !=SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (su != SqlNull.Value)
            {
                (ls, m) = su.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != su.defpos)
                {
                    ch = true; su = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new SqlValueExpr(defpos, cx, kind, lf, rg, mod, mem);
                if (su != SqlNull.Value)
                    r += (Sub, su);
                if (d != depth)
                    r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object> m)
        {
            var r = base._Replace(cx, so, sv, m);
            var lf = cx.ObReplace(left, so, sv);
            if (lf != left)
                r += (Left, lf);
            var rg = cx.ObReplace(right, so, sv);
            if (rg != right)
                r += (Right, rg);
            var dr = cx._Dom(this)??Domain.Content;
            if ((dr.kind==Sqlx.UNION || dr.kind==Sqlx.CONTENT) && so.domain != sv.domain &&
                cx.obs[lf] is SqlValue lv && cx.obs[rg] is SqlValue rv)
            {
                if (cx._Dom((long)(_Type(cx, kind, mod, lv, rv)[_Domain] ?? -1L)) is not Domain nd)
                    throw new PEException("PE29001");
                cx.Add(nd);
                r += (_Domain, nd.defpos);
            }
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueExpr)base.AddFrom(cx, q);
            if (cx.obs[r.left] is SqlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.left)
                    r += (Left, a.defpos);
            }
            if (cx.obs[r.right] is SqlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (Right, a.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            var ch = false;
            var nl = ((SqlValue?)c.obs[left])?.Having(c, dm);
            ch = ch || (nl != null && nl.defpos != left);
            var nr = ((SqlValue?)c.obs[right])?.Having(c, dm);
            ch = ch || (nr != null && nr.defpos != right);
            var nu = ((SqlValue?)c.obs[sub])?.Having(c, dm);
            ch = ch || (nu != null && nu.defpos != sub);
            var r = this;
            if (ch)
            {
                r = new SqlValueExpr(c.GetUid(), c, kind, nl, nr, mod);
                if (nu != null)
                    r += (Sub, nu);
                return (SqlValue)c.Add(r);
            }
            return r;
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is SqlValueExpr ve)
            {
                if (kind != ve.kind || mod != ve.mod)
                    return false;
                if (cx.obs[left] is SqlValue lf && cx.obs[ve.left] is SqlValue le &&
                    !le.Match(cx, lf))
                    return false;
                if (cx.obs[right] is SqlValue rg && cx.obs[ve.right] is SqlValue re &&
                    !re.Match(cx, rg))
                    return false; 
                if (cx.obs[sub] is SqlValue su && cx.obs[ve.sub] is SqlValue se &&
                    !se.Match(cx, su))
                    return false;
                return true;
            }
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, q, ambient) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, q, ambient) != false);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            if (cs.Contains(defpos))
                return true;
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, cs, ambient) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, cs, ambient) != false);
        }
        internal override CTree<long, bool> Disjoin(Context cx)
        { // parsing guarantees right associativity
            return (kind == Sqlx.AND && cx.obs[right] is SqlValue rg) ?
                rg.Disjoin(cx) + (left, true)
                : base.Disjoin(cx);
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[left] is SqlValue lf && !lf.Grouped(cx, gs))
                return false;
            if (cx.obs[right] is SqlValue rg && !rg.Grouped(cx, gs))
                return false;
            return true;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[left] is SqlValue sl)
                r += sl.Operands(cx);
            if (cx.obs[right] is SqlValue sr)
                r += sr.Operands(cx);
            return r;
        }
        /// <summary>
        /// Examine a binary expression and work out the resulting type.
        /// The main complication here is handling things like x+1
        /// (e.g. confusion between NUMERIC and INTEGER)
        /// </summary>
        /// <param name="dt">Target union type</param>
        /// <returns>Actual type</returns>
        internal override Domain FindType(Context cx,Domain dt)
        {
            var rg = (SqlValue?)cx.obs[right]??SqlNull.Value;
            if (kind == Sqlx.DOT)
                return rg.FindType(cx, dt); 
            Domain tr = (rg==SqlNull.Value)? dt : rg.FindType(cx, dt);
            if (cx.obs[left] is SqlValue lf)
            {
                Domain tl = lf.FindType(cx, dt);
                switch (tl.kind)
                {
                    case Sqlx.PERIOD: return Domain.Period;
                    case Sqlx.CHAR: return tl;
                    case Sqlx.NCHAR: return tl;
                    case Sqlx.DATE:
                        if (kind == Sqlx.MINUS)
                            return Domain.Interval;
                        return tl;
                    case Sqlx.INTERVAL: return tl;
                    case Sqlx.TIMESTAMP:
                        if (kind == Sqlx.MINUS)
                            return Domain.Interval;
                        return tl;
                    case Sqlx.INTEGER: return tr;
                    case Sqlx.NUMERIC:
                        if (tr.kind == Sqlx.REAL) return tr;
                        return tl;
                    case Sqlx.REAL: return tl;
                    case Sqlx.LBRACK:
                        return cx._Dom(tl.elType)??Domain.Content;
                    case Sqlx.UNION:
                        return tl;
                }
            } 
            return tr;
        }
        internal override bool HasAnd(Context cx,SqlValue s)
        {
            if (s == this)
                return true;
            if (kind != Sqlx.AND)
                return false;
            return (cx.obs[left] as SqlValue)?.HasAnd(cx,s)==true 
            || (cx.obs[right] as SqlValue)?.HasAnd(cx,s) == true;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is SqlValue lf)
                r += lf.IsAggregation(cx);
            if (cx.obs[right] is SqlValue rg)
                r += rg.IsAggregation(cx);
            if (cx.obs[sub] is SqlValue su)
                r += su.IsAggregation(cx);
            return r;
        }
        internal override int Ands(Context cx)
        {
            if (kind == Sqlx.AND)
                return ((cx.obs[left] as SqlValue)?.Ands(cx)??0) 
                    + ((cx.obs[right] as SqlValue)?.Ands(cx)??0);
            return base.Ands(cx);
        }
        internal override bool isConstant(Context cx)
        {
            return (cx.obs[left] as SqlValue)?.isConstant(cx)!=false 
                && (cx.obs[right] as SqlValue)?.isConstant(cx)!=false;
        }
        internal override BTree<long,SystemFilter> SysFilter(Context cx, BTree<long,SystemFilter> sf)
        {
            switch(kind)
            {
                case Sqlx.AND:
                    {
                        if (cx.obs[left] is SqlValue lf && cx.obs[right] is SqlValue rg)
                            return lf.SysFilter(cx, rg.SysFilter(cx, sf));
                        break;
                    }
                case Sqlx.EQL:
                case Sqlx.GTR:
                case Sqlx.LSS:
                case Sqlx.LEQ:
                case Sqlx.GEQ:
                    {
                        if (cx.obs[left] is SqlValue lf && cx.obs[right] is SqlValue rg)
                        {
                            if (lf.isConstant(cx) && rg is SqlCopy sc)
                                return SystemFilter.Add(sf, sc.copyFrom, Neg(kind), lf.Eval(cx));
                            if (rg.isConstant(cx) && lf is SqlCopy sl)
                                return SystemFilter.Add(sf, sl.copyFrom, kind, rg.Eval(cx));
                        }
                        break;
                    }
                default:
                    return sf;
            }
            return base.SysFilter(cx, sf);
        }
        static Sqlx Neg(Sqlx o)
        {
            return o switch
            {
                Sqlx.GTR => Sqlx.LSS,
                Sqlx.GEQ => Sqlx.LEQ,
                Sqlx.LEQ => Sqlx.GEQ,
                Sqlx.LSS => Sqlx.GTR,
                _ => o,
            };
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.StartCounter(cx,rs, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.StartCounter(cx,rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.AddIn(cx,rb, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.AddIn(cx,rb, tg);
            return tg;
        }
        internal override void OnRow(Context cx,Cursor bmk)
        {
            (cx.obs[left] as SqlValue)?.OnRow(cx,bmk);
            (cx.obs[right] as SqlValue)?.OnRow(cx,bmk);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            if (kind != Sqlx.DOT)
                throw new DBException("42174");
            var lf = cx.obs[left];
            var rw = (TRow?)lf?.Eval(cx)??TRow.Empty;
            lf?.Set(cx, rw += (right, v));
        }
        /// <summary>
        /// Evaluate the expression (mostly binary operators).
        /// The difficulty here is the avoidance of side-effects and the possibility of default values
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this) ?? Domain.Content;
            TypedValue v = dm.defaultValue;
            switch (kind)
            {
                case Sqlx.AND:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (a == TBool.False && mod!=Sqlx.BINARY)
                            return a;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (mod == Sqlx.BINARY && a is TInt aa && b is TInt ab) // JavaScript
                            return new TInt(aa.value & ab.value);
                        else if (a is TBool ba && b is TBool bb)
                            return TBool.For(ba.value && bb.value);
                        break;
                    }
                case Sqlx.ASC: // JavaScript ++
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        var x = dm.Eval(defpos, cx, v, Sqlx.ADD, new TInt(1L));
                        return (mod == Sqlx.BEFORE) ? x : v;
                    }
                case Sqlx.ASSIGNMENT:
                    {
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return b;
                    }
                case Sqlx.COLLATE:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        Domain ct = a.dataType;
                        if (ct.kind == Sqlx.CHAR)
                        {
                            TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                            if (b == TNull.Value)
                                return a;
                            string cname = b.ToString();
                            if (ct.culture.Name == cname)
                                return a;
                            Domain nt = (Domain)cx.Add(new Domain(defpos, ct.kind, BTree<long, object>.Empty
                                + (Domain.Precision, ct.prec) + (Domain.Charset, ct.charSet)
                                + (Domain.Culture, new CultureInfo(cname))));
                            return new TChar(nt, a.ToString());
                        }
                        throw new DBException("2H000", "Collate on non-string?").ISO();
                    }
                case Sqlx.COMMA: // JavaScript
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return a;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        return b;
                    }
                case Sqlx.CONCATENATE:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TArray aa && b is TArray bb)
                            return b.dataType.Concatenate(aa, bb);
                        return new TChar(b.dataType, a.ToString() + b.ToString());
                    }
                case Sqlx.CONTAINS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod aa && b is TPeriod bb)
                            return TBool.For(aa.value.start.CompareTo(bb.value.start) <= 0
                                  && aa.value.end.CompareTo(bb.value.end) >= 0);
                        return v;
                    }
                case Sqlx.DESC: // JavaScript --
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        var w = a.dataType.Eval(defpos, cx, v, Sqlx.MINUS, new TInt(1L));
                        return (mod == Sqlx.BEFORE) ? w : v;
                    }
                case Sqlx.DIVIDE:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return dm.Eval(defpos, cx, a, kind, b);
                    }
                case Sqlx.DOT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TRow ra)
                        {
                            if (cx.obs[right] is SqlField sf && sf.field is long fp)
                                return ra.values[fp] ?? v;
                            if (cx.obs[right] is SqlCopy sc && sc.defpos is long dp)
                                return ra.values[dp] ??
                                    ((sc.copyFrom is long cp) ? (ra.values[cp] ?? v) : v);
                        }
                        return v;
                    }
                case Sqlx.EQL:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(b.CompareTo(a) == 0);
                    }
                case Sqlx.EQUALS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                            return TBool.For(pa.value.start.CompareTo(pb.value.start) == 0
                                && pb.value.end.CompareTo(pa.value.end) == 0);
                        break;
                    }
                case Sqlx.EXCEPT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb)
                            return a.dataType.Coerce(cx, TMultiset.Except(ma, mb, mod == Sqlx.ALL));
                        break;
                    }
                case Sqlx.GEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) >= 0);
                    }
                case Sqlx.GTR:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) > 0);
                    }
                case Sqlx.INTERSECT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb && TMultiset.Intersect(ma, mb, mod == Sqlx.ALL) is TMultiset mc)
                            return a.dataType.Coerce(cx, mc);
                        break;
                    }
                case Sqlx.LBRACK:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TArray aa && b is TInt bb)
                            return aa[bb.value]??v;
                        break;
                    }
                case Sqlx.LEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) <= 0);
                    }
                case Sqlx.LOWER: // JavScript >> and >>>
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TInt ia && b is TInt ib)
                        {
                            var s = (byte)(ib.value & 0x1f);
                            if (s == 0)
                                return a;
                            if (mod == Sqlx.GTR)
                                return new TInt((long)(((ulong)ia.value) >> s));
                            else
                                return new TInt((long)(((ulong)ia.value) << s));
                        }
                        break;
                    }
                case Sqlx.LSS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) < 0);
                    }
                case Sqlx.MINUS:
                    {
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (cx.obs[left] == null)
                        {
                            var w = dm.Eval(defpos, cx, new TInt(0), Sqlx.MINUS, b);
                            return w;
                        }   
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        v = dm.Eval(defpos, cx, a, kind, b);
                        return v;
                    }
                case Sqlx.NEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) != 0);
                    }
                case Sqlx.NO:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        return a;
                    }
                case Sqlx.NOT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (mod == Sqlx.BINARY && a is TInt ia)
                            return new TInt(~ia.value);
                        if (a is TBool b)
                            return TBool.For(!b.value);
                        break;
                    }
                case Sqlx.OR:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (a == TBool.True && mod != Sqlx.BINARY)
                            return a;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (mod == Sqlx.BINARY && a is TInt aa && b is TInt ab) // JavaScript
                            return new TInt(aa.value | ab.value);
                        else if (a is TBool ba && b is TBool bb)
                            return TBool.For(ba.value || bb.value);
                        break;
                    }
                case Sqlx.OVERLAPS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                            return TBool.For(pa.value.end.CompareTo(pb.value.start) >= 0
                                && pb.value.end.CompareTo(pa.value.start) >= 0);
                        break;
                    }
                case Sqlx.PERIOD:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return new TPeriod(Domain.Period, new Period(a, b));
                    }
                case Sqlx.PLUS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return dm.Eval(defpos, cx, a, kind, b);
                    }
                case Sqlx.PRECEDES:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                        {
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(pa.value.end.CompareTo(pb.value.start) == 0);
                            return TBool.For(pa.value.end.CompareTo(pb.value.start) <= 0);
                        }
                        break;
                    }
                case Sqlx.QMARK: // v7 API for Prepare
                    {
                        return cx.values[defpos] ?? v;
                    }
                case Sqlx.RBRACK:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TArray aa && b is TInt bb)
                            return aa[bb.value] ?? v;
                        break;
                    }
                case Sqlx.SUCCEEDS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                        {
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(pa.value.start.CompareTo(pb.value.end) == 0);
                            return TBool.For(pa.value.start.CompareTo(pb.value.end) >= 0);
                        }
                        break;
                    }
                case Sqlx.TIMES:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return dm.Eval(defpos, cx, a, kind, b);
                    }
                case Sqlx.UNION:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb && TMultiset.Union(ma, mb, mod == Sqlx.ALL) is TMultiset mc)
                            return a.dataType.Coerce(cx,mc);
                        break;
                    }
                case Sqlx.UPPER: // JavaScript <<
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TInt aa && b is TInt bb)
                        {
                            var s = (byte)(bb.value & 0x1f);
                            return new TInt(aa.value << s);
                        }
                        break;
                    }
                //       case Sqlx.XMLATTRIBUTES:
                //         return new TypedValue(left.domain, BuildXml(left) + " " + BuildXml(right));
                case Sqlx.XMLCONCAT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TXml aa && b is TXml bb)
                            return new TXml(aa.ToString() + " " + bb.ToString());
                        break;
                    }
            }
            throw new DBException("22000", kind).ISO();
        }
        static BTree<long, object> _Type(Context cx, Sqlx kind, Sqlx mod,
            SqlValue? left, SqlValue? right, BTree<long, object>? mm = null)
        {
            mm ??= BTree<long, object>.Empty;
            var ag = CTree<long, bool>.Empty;
            var dl = Domain.Content;
            var dr = Domain.Content;
            if (left != null)
            {
                mm += (Left, left.defpos);
                dl = cx._Dom(left) ?? Domain.Content;
                ag += left.IsAggregation(cx);
            }
            if (right != null)
            {
                mm += (Right, right.defpos);
                dr = cx._Dom(right) ?? Domain.Content;
                ag += right.IsAggregation(cx);
            }
            var kl = dl.kind;
            var kr = dr.kind;
            var dm = Domain.Content;
            var nm = (string?)mm?[ObInfo.Name] ?? "";
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: dm = cx._Dom(right) ?? Domain.Content;
                    nm = left?.name ?? ""; break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON: dm = cx._Dom(left) ?? Domain.Content;
                    nm = left?.name ?? ""; break;// JavaScript
                case Sqlx.CONCATENATE: dm = Domain.Char; break;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.INTEGER
                            || dr.kind == Sqlx.NUMERIC))
                            dm = dl;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.DOT: dm = cx._Dom(right);
                    if (left != null && left.name != "" && right != null && right.name != "")
                        nm = left.name + "." + right.name;
                    break;
                case Sqlx.EQL: dm = Domain.Bool; break;
                case Sqlx.EXCEPT: dm = cx._Dom(left); break;
                case Sqlx.GTR: dm = Domain.Bool; break;
                case Sqlx.INTERSECT: dm = cx._Dom(left); break;
                case Sqlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Sqlx.LSS: dm = Domain.Bool; break;
                case Sqlx.MINUS:
                    if (left != null)
                    {
                        if (kl == Sqlx.DATE || kl == Sqlx.TIMESTAMP || kl == Sqlx.TIME)
                        {
                            if (dr == dl)
                                dm = Domain.Interval;
                            else if (kr == Sqlx.INTERVAL)
                                dm = cx._Dom(left);
                        }
                        else if (kl == Sqlx.INTERVAL && (kr == Sqlx.DATE || kl == Sqlx.TIMESTAMP || kl == Sqlx.TIME))
                            dm = cx._Dom(right);
                        else if (kl == Sqlx.REAL || kl == Sqlx.NUMERIC)
                            dm = cx._Dom(left);
                        else if (kr == Sqlx.REAL || kr == Sqlx.NUMERIC)
                            dm = cx._Dom(right);
                        else
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                    if (right != null)
                        dm = right.FindType(cx, Domain.UnionDateNumeric);
                    break;
                case Sqlx.NEQ: dm = Domain.Bool; break;
                case Sqlx.LEQ: dm = Domain.Bool; break;
                case Sqlx.GEQ: dm = Domain.Bool; break;
                case Sqlx.NO: dm = cx._Dom(left); break;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        if ((kl == Sqlx.DATE || kl == Sqlx.TIMESTAMP || kl == Sqlx.TIME) && kr == Sqlx.INTERVAL)
                            dm = cx._Dom(left);
                        else if (kl == Sqlx.INTERVAL && (kr == Sqlx.DATE || kl == Sqlx.TIMESTAMP || kl == Sqlx.TIME))
                            dm = cx._Dom(right);
                        else if (kl == Sqlx.REAL || kl == Sqlx.NUMERIC)
                            dm = cx._Dom(left);
                        else if (kr == Sqlx.REAL || kr == Sqlx.NUMERIC)
                            dm = cx._Dom(right);
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                    dm = Domain.Content; break;
                case Sqlx.RBRACK:
                    {
                        if (left == null || cx._Dom(left) is not Domain d0)
                            throw new PEException("PE5001");
                        dm = (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, d0)); break;
                    }
                case Sqlx.SET: dm = cx._Dom(left); nm = left?.name ?? ""; break; // JavaScript
                case Sqlx.TIMES:
                    {
                        if (kl == Sqlx.NUMERIC || kr == Sqlx.NUMERIC)
                            dm = Domain.Numeric;
                        else if (kl == Sqlx.INTERVAL && (kr == Sqlx.INTEGER || kr == Sqlx.NUMERIC))
                            dm = cx._Dom(left);
                        else if (kr == Sqlx.INTERVAL && (kl == Sqlx.INTEGER || kl == Sqlx.NUMERIC))
                            dm = cx._Dom(right);
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.UNION: dm = cx._Dom(left); nm = left?.name ?? ""; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            dm ??= Domain.Content;
  /*          if (kl == Sqlx.UNION && kr != Sqlx.UNION && NumericOp(kind) && dr != null &&
                left != null && left.Constrain(cx, dr) is SqlValue nl && left.defpos != nl.defpos)
                cx.Replace(left, nl);
            if (kr == Sqlx.UNION && kl != Sqlx.UNION && NumericOp(kind) && dl != null &&
                right != null && right.Constrain(cx, dl) is SqlValue nr && right.defpos != nr.defpos)
                cx.Replace(right, nr);*/
            mm ??= BTree<long, object>.Empty; //?
            if (ag != CTree<long, bool>.Empty && dm != Domain.Content)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                mm += (Domain.Aggs, ag);
            }
            return mm + (_Domain, dm.defpos) + (ObInfo.Name, nm);
        }
        internal override SqlValue Constrain(Context cx, Domain dt)
        {
            // se are here because the domain is UNION and we want to make it dt.
            var le = cx._Ob(left);
            var rg = cx._Ob(right);
            if (le != null && rg != null && cx._Dom(le) is Domain dl && cx._Dom(rg) is Domain dr
                && dl.kind != Sqlx.UNION && dr.kind != Sqlx.UNION
                && dt.CanTakeValueOf(dl) && dt.CanTakeValueOf(dr))
                return (SqlValue)cx.Add(this + (_Domain, dt.defpos));
            if (le == null && rg != null && cx._Dom(rg) is Domain er  && er.kind != Sqlx.UNION
                && dt.CanTakeValueOf(er))
                return (SqlValue)cx.Add(this + (_Domain, dt.defpos));
            if (le != null && rg == null && cx._Dom(le) is Domain el && el.kind != Sqlx.UNION 
                && dt.CanTakeValueOf(el))
                return (SqlValue)cx.Add(this + (_Domain, dt.defpos));
            return base.Constrain(cx, dt);
        }
        internal override SqlValue Invert(Context cx)
        {
            var lv = (SqlValue?)cx.obs[left]??SqlNull.Value;
            var rv = (SqlValue?)cx.obs[right]??SqlNull.Value;
            return kind switch
            {
                Sqlx.AND => new SqlValueExpr(defpos, cx, Sqlx.OR, lv.Invert(cx),
                                        rv.Invert(cx), Sqlx.NULL),
                Sqlx.OR => new SqlValueExpr(defpos, cx, Sqlx.AND, lv.Invert(cx),
                                        rv.Invert(cx), Sqlx.NULL),
                Sqlx.NOT => lv,
                Sqlx.EQL => new SqlValueExpr(defpos, cx, Sqlx.NEQ, lv, rv, Sqlx.NULL),
                Sqlx.GTR => new SqlValueExpr(defpos, cx, Sqlx.LEQ, lv, rv, Sqlx.NULL),
                Sqlx.LSS => new SqlValueExpr(defpos, cx, Sqlx.GEQ, lv, rv, Sqlx.NULL),
                Sqlx.NEQ => new SqlValueExpr(defpos, cx, Sqlx.EQL, lv, rv, Sqlx.NULL),
                Sqlx.GEQ => new SqlValueExpr(defpos, cx, Sqlx.LSS, lv, rv, Sqlx.NULL),
                Sqlx.LEQ => new SqlValueExpr(defpos, cx, Sqlx.GTR, lv, rv, Sqlx.NULL),
                _ => base.Invert(cx),
            };
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the SqlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Context cx,SqlValue v,RowSet r)
        {
            if (base._MatchExpr(cx,v,r)) return true;
            var dm = cx._Dom(domain);
            if (v is not SqlValueExpr e || (dm != null && dm.CompareTo(cx._Dom(v))==0))
                return false;
            if (cx.obs[left] is SqlValue lv && !lv._MatchExpr(cx, lv,r))
                return false;
            if (cx.obs[e.left] != null)
                return false;
            if (cx.obs[right] is SqlValue rv && !rv._MatchExpr(cx, rv,r))
                return false;
            if (cx.obs[e.right] != null)
                return false;
            return true;
        }
        internal override CTree<long, TypedValue> Add(Context cx, CTree<long, TypedValue> ma,
            Table? tb = null)
        {
            if (kind == Sqlx.EQL)
            {
                if (cx.obs[left] is SqlCopy sc && cx.obs[right] is SqlLiteral sr
                    && (tb==null || (cx.db.objects[sc.copyFrom] is TableColumn tc
                    && tc.tabledefpos==tb.defpos)))
                    return ma += (sc.copyFrom, sr.val);
                if (cx.obs[right] is SqlCopy sd && cx.obs[left] is SqlLiteral sl
                    && (tb == null || (cx.db.objects[sd.copyFrom] is TableColumn td
                    && td.tabledefpos == tb.defpos)))
                    return ma += (sd.copyFrom, sl.val);
            }
            return base.Add(cx, ma);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long rs, CTree<long, bool> qn)
        {
            var r = qn;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx,rs,r) ?? r;
            if (kind!=Sqlx.DOT)
                r = ((SqlValue?)cx.obs[right])?.Needs(cx,rs,r) ?? r;
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[left] is SqlValue lf) r += lf.Needs(cx);
            if (cx.obs[right] is SqlValue rg) r += rg.Needs(cx);
            if (cx.obs[sub] is SqlValue su) r += su.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx, rs);
            if (kind!=Sqlx.DOT && cx.obs[right] is SqlValue sw)
                r += sw.Needs(cx, rs);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(Uid(defpos)); sb.Append('(');
            if (left!=-1L)
                sb.Append(Uid(left));
            sb.Append(For(kind));
            if (right != -1L)
                sb.Append(Uid(right));
            if (kind == Sqlx.LBRACK)
                sb.Append(']');
            if (kind == Sqlx.LPAREN)
                sb.Append(')');
            sb.Append(')');
            if (alias != null)
            {
                sb.Append(" as ");
                sb.Append(alias);
            }
            return sb.ToString();
        }
        internal override string ToString(string sg, Remotes rf, BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder();
            var lp = false;
            if (left >= 0 && right >= 0 && kind != Sqlx.LBRACK && kind != Sqlx.LPAREN)
            {
                sb.Append('(');
                lp = true;
            }
            if (left >= 0)
            {
                var lf = cx.obs[left]?.ToString(sg, Remotes.Operands, cs, ns, cx)??"";
                sb.Append(lf);
            }
            sb.Append(For(kind));
            if (right >= 0)
            {
                var rg = cx.obs[right]?.ToString(sg, Remotes.Operands, cs, ns, cx)??"";
                sb.Append(rg);
            }
            if (kind == Sqlx.LBRACK)
                sb.Append(']');
            if (lp || kind == Sqlx.LPAREN)
                sb.Append(')');
            switch (rf)
            {
                case Remotes.Selects:
                    var nm = alias ?? name ??"";
                    if (nm != "")
                    {
                        sb.Append(" as ");
                        sb.Append(nm);
                    }
                    break;
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A SqlValue that is the null literal
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlNull : SqlValue
    {
        internal static SqlNull Value = new();
        /// <summary>
        /// constructor for a null expression
        /// </summary>
        SqlNull()
            : base(--_uid,BTree<long,object>.Empty)
        { }
        /// <summary>
        /// the value of null
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return TNull.Value;
        }
        internal override bool _MatchExpr(Context cx, SqlValue v,RowSet r)
        {
            return v is SqlNull;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        { 
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return v is SqlNull;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long r, CTree<long, bool> qn)
        {
            return qn;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return this;
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return this;
        }
        public override string ToString()
        {
            return "NULL";
        }
        internal override string ToString(string sg, Remotes rf, BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            return "null";
        }
    }
    // shareable as of 26 April 2021
    internal class SqlSecurity : SqlValue
    {
        internal SqlSecurity(long dp) : base(new Ident("SECURITY",new Iix(dp)), Domain._Level) { }
        protected SqlSecurity(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSecurity(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlSecurity(dp,m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return true;
        }
    }
    /// <summary>
    /// Added for LogRowsRowSet and similar: Values are computed in Cursor constructor
    /// and do not depend on other SqlValues
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlFormal : SqlValue 
    {
        public SqlFormal(Context cx, string nm, Domain dm, long cf=-1L)
            : base(cx, nm, dm, cf) { }
        protected SqlFormal(long dp,BTree<long,object>m):base(dp,m){ }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFormal(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlFormal(dp,m);
        }
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            return CTree<long,bool>.Empty;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return true;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cf = mem[SqlCopy.CopyFrom];
            if (cf != null)
            { sb.Append(" from: "); sb.Append((long)cf); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The SqlLiteral subclass
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlLiteral : SqlValue
    {
        internal const long
            _Val = -317;// TypedValue
        internal TypedValue val=>(TypedValue)(mem[_Val]??TNull.Value);
        internal override long target => -1;
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(long dp, TypedValue v, Domain? td=null) 
            : base(dp,BTree<long, object>.Empty+(_Domain, v.dataType.defpos)+(_Val, v))
        {
            if (td != null  && v.dataType!=null && !td.CanTakeValueOf(v.dataType))
                throw new DBException("22000", v);
            if (dp == -1L)
                throw new PEException("PE999");
        }
        public SqlLiteral(long dp, string n, TypedValue v, Domain? td=null)
            : base(dp, BTree<long, object>.Empty 
                 + (_Domain, td?.defpos??v.dataType.defpos) + (_Val, v) + (ObInfo.Name,n))
        {  }
        public SqlLiteral(long dp, BTree<long, object> m) : base(dp, m) 
        {
            if (dp == -1L)
                throw new PEException("PE999");
        }
        public static SqlLiteral operator+(SqlLiteral s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlLiteral(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlLiteral(defpos,m);
        }
        public SqlLiteral(long dp, Domain dt) : base(dp, BTree<long, object>.Empty
            + (_Domain, dt.defpos) + (_Val, dt.defaultValue))
        { }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlLiteral(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nv = val.Fix(cx);
            if (nv != val)
                r += (_Val, nv);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return true;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return CTree<long,bool>.Empty;
        }

        internal override bool Match(Context c, SqlValue v)
        {
            if (val != null && v is SqlLiteral l 
                && val.dataType.kind == l.val.dataType.kind
                && val.CompareTo(l.val) == 0)
                return true;
            return false;
        }
        internal override DBObject QParams(Context cx)
        {
            var r = base.QParams(cx);
            if (val is TQParam tq && cx.values[tq.qid.dp] is TypedValue tv)
            {
                r = New(r.defpos,r.mem+(_Val, tv));
                cx.Add(r);
            }
            return r;
        }
        /// <summary>
        /// test for structural equivalence
        /// </summary>
        /// <param name="v">an SqlValue</param>
        /// <returns>whether they are structurally equivalent</returns>
        internal override bool _MatchExpr(Context cx,SqlValue v,RowSet r)
        {
            if (cx._Dom(this)?.CompareTo(cx._Dom(v))!=0)
                return false;
            return v is SqlLiteral c &&  val == c.val;
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue Eval(Context cx)
        {
            if (val is TQParam tq && cx.values[tq.qid.dp] is TypedValue tv && tv != TNull.Value)
                return tv;
            return val ?? cx._Dom(this)?.defaultValue ?? TNull.Value;
        }
        public override int CompareTo(object? obj)
        {
            return (obj is SqlLiteral that)?
                val?.CompareTo(that.val) ?? throw new PEException("PE000") 
                : 1;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            return new CTree<long, Domain>(defpos, val.dataType);
        }
        /// <summary>
        /// A literal is supplied by any query
        /// </summary>
        /// <param name="q">the query</param>
        /// <returns>true</returns>
        internal override bool IsFrom(Context cx,RowSet q,bool ordered,Domain? ut=null)
        {
            return true;
        }
        internal override bool isConstant(Context cx)
        {
            return true; // !(val is TQParam);
        }
        internal override Domain FindType(Context cx, Domain dt)
        {
            var vt = val.dataType;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22005", dt.kind, vt.kind).ISO();
            if (vt.kind==Sqlx.INTEGER)
                return dt; // keep union options open
            return vt;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            return CTree<long,bool>.Empty;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        internal override SqlValue Constrain(Context cx, Domain dt)
        {
            var dm = cx._Dom(this) ?? Domain.Content;
            if (dt.CanTakeValueOf(dm))
                return (SqlValue)cx.Add(this + (_Domain, dt));
            return base.Constrain(cx, dt);
        }
        internal override string ToString(string sg, Remotes rf, BList<long?> cs, CTree<long, string> ns, Context cx)
        {
            if (val.dataType.kind == Sqlx.CHAR)
            {
                var sb = new StringBuilder();
                sb.Append('\'');
                sb.Append(val.ToString().Replace("'", "''"));
                sb.Append('\'');
                return sb.ToString();
            }
            return val.ToString();   
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(val);
            if (alias != null)
            {
                sb.Append(" as ");
                sb.Append(alias);
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A DateTime Literal
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlDateTimeLiteral : SqlLiteral
    {
        /// <summary>
        /// construct a datetime literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">the obs type</param>
        /// <param name="n">the string version of the date/time</param>
        public SqlDateTimeLiteral(long dp, Context cx, Domain op, string n)
            : base(dp, op.Parse(dp,n,cx))
        {}
        protected SqlDateTimeLiteral(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDateTimeLiteral operator+(SqlDateTimeLiteral s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlDateTimeLiteral(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDateTimeLiteral(defpos,m);
        }
    }
    /// <summary>
    /// A Row value
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlRow : SqlValue
    {
        public SqlRow(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(long dp, Context cx, BList<DBObject> vs, BTree<long, object>? m = null)
            : base(dp, _Mem(cx,vs,m) + _Deps(vs))
        { }
        internal SqlRow(long dp, Domain dm, BList<DBObject> vs) : base(dp, _Mem(dm, vs)) { }
        internal SqlRow(Context cx, Table tb, long dp)
            : base(cx.GetUid(), _Mem(dp,cx,tb)+(Dependents,new CTree<long,bool>(tb.defpos,true)))
        { }
        public SqlRow(long dp, Context cx, Domain xp, BList<long?> vs, BTree<long, object>? m = null)
            : base(dp, _Inf(cx, m, xp, vs) + _Deps(cx,vs))
        { }
        static BTree<long, object> _Mem(Context cx, BList<DBObject> vs, BTree<long, object>? m)
        {
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, vs));
            m = (m ?? BTree<long, object>.Empty) + (_Domain, dm.defpos) + (Domain.Aggs, dm.aggs);
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var ob = b.value();
                m += (ob.defpos, ob);
            }
            return m;
        }
        static BTree<long,object> _Mem(Domain dm,BList<DBObject> vs)
        {
            var r = new BTree<long, object>(_Domain, dm.defpos);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && vs[b.key()] is SqlValue v)
                    r += (p, v);
            return r;
        }
        /// <summary>
        /// Annoyingly, a tablerow reference needs to be instanced
        /// </summary>
        /// <param name="dp">the defpos of the Record in the table</param>
        /// <param name="cx"></param>
        /// <param name="nt">the node type</param>
        /// <returns></returns>
        /// <exception cref="PEException"></exception>
        static BTree<long, object> _Mem(long dp, Context cx, Table tb)
        {
            var r = BTree<long, object>.Empty;
            var tr = tb.tableRows[dp] ?? throw new PEException("PE917456");
            var td = cx._Dom(tb) ?? throw new PEException("PE917457");
            var sl = BList<DBObject>.Empty;
            for (var b = td.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = (SqlValue)cx.Add(new SqlLiteral(cx.GetUid(), tr.vals[p] ?? TNull.Value));
                    sl += v;
                    r += (p, v);
                }
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, sl));
            r += (_Domain, dm.defpos);
            r += (_Depth, 2);
            return r;
        }
        protected static BTree<long, object> _Inf(Context cx, BTree<long, object>? m,
    Domain xp, BList<long?> vs)
        {
            var cb = xp.First();
            var bs = BList<DBObject>.Empty;
            var r = m ?? BTree<long, object>.Empty;
            for (var b = vs.First(); b != null; b = b.Next(), cb = cb?.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue ob)
                {
                    bs += ob;
                    r += (p, ob);
                }
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length));
            return r + (_Domain, dm.defpos);
        }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return (SqlRow)s.New(s.mem + m);
        }
        public static SqlRow operator +(SqlRow s, (Context,SqlValue) x)
        {
            var (cx, sv) = x;
            if (cx._Dom(s) is not Domain dm)
                throw new PEException("PE5002");
            return (SqlRow)s.New(s.mem + (_Domain,
                dm.New(cx,dm.mem+(Domain.RowType,dm.rowType+sv.defpos)
                +(Domain.Representation,dm.representation+(sv.defpos,cx._Dom(sv)??Domain.Content))).defpos));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlRow(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var cs = BList<long?>.Empty;
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = (SqlValue)cx._Replace(p, so, sv);
                    cs += v.defpos;
                    vs += v;
                    if (v.defpos != b.value())
                        ch = true;
                }
            if (ch)
            {
                var dm = cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW, vs));
                r = r + (_Domain, dm.defpos) + _Deps(vs);
            }
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            var dm = cx._Dom(this);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.Grouped(cx,gs))
                        return false;
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            var dt = c._Dom(this);
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = dt?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& c.obs[p] is SqlValue v)
                {
                    var nv = v.Having(c, dm);
                    vs += nv;
                    ch = ch || nv != v;
                }
            return ch ? (SqlValue)c.Add(new SqlRow(c.GetUid(), c, vs)) : this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlRow r)
            {
                var rb = c._Dom(r)?.rowType.First();
                for (var b = c._Dom(this)?.rowType.First(); b != null && rb != null;
                    b = b.Next(), rb = rb.Next())
                    if (b.value() is long p && c.obs[p] is SqlValue s
                        && rb.value() is long rp && c.obs[rp] is SqlValue t
                        && !s.Match(c, t))
                        return false;
                return true;
            }
            return false;
        }
        internal override (BList<DBObject>, BTree<long,object>) Resolve(Context cx, long f, BTree<long,object> m)
        {
            var dm = cx._Dom(this);
            var cs = BList<DBObject>.Empty;
            var om = m;
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue c)
                {
                    BList<DBObject> ls;
                    (ls, m) = c.Resolve(cx, f, m);
                    cs += ls;
                }
            if (m != om)
            {
                var sv = new SqlRow(defpos, cx, cs);
                m = cx.Replace(this, sv, m);
            }
            var r = (SqlValue?)cx.obs[defpos] ?? throw new PEException("PE1800");
            return (new BList<DBObject>(r), m);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var ch = false;
            for (var b = cx._Dom(r)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue a)
                {
                    a = a.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    r += (cx, a);
                }
            return ch?(SqlValue)cx.Add(r):this;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b=cx._Dom(this)?.rowType.First();b!=null;b=b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.KnownBy(cx,q, ambient))
                        return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue s && !s.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d0)
                return new CTree<long, Domain>(defpos, d0);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5004");
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                {
                    r += s.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(p);
                }
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            var dm = cx._Dom(this);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                    r += sv.IsAggregation(cx);
            return r;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var vs = CTree<long, TypedValue>.Empty;
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5100");
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long s)
                    vs += (s, cx.obs[s]?.Eval(cx) ?? TNull.Value);
            return new TRow(dm, vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            var dm = cx._Dom(this);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                    tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            var dm = cx._Dom(this);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                tg = s.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue s)
                    qn = s.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                    r += s.Needs(cx, rs);
            return r;
        }
        internal override string ToString(string sg, Remotes rf, BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            sb.Append(" (");
            for (var b = cx._Dom(this)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(s.ToString(sg, rf, cs, ns, cx));
                }
            sb.Append(')');
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "";
            sb.Append(" (");
            for (var b = mem.PositionAt(0L); b != null; b = b.Next())
                if (b.value() is SqlValue s)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append('='); sb.Append(s);
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// Prepare an SqlValue with reified columns for use in trigger
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlOldRow : SqlRow
    {
        internal SqlOldRow(Ident ic, Context cx, RowSet fm)
            : base(ic.iix.dp, _Mem(ic,cx,fm))
        { }
        protected SqlOldRow(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic,Context cx,RowSet fm)
        {
            var r = BTree<long, object>.Empty + (_Domain, fm.domain)
                   + (ObInfo.Name, ic.ident) + (_From, fm.defpos);
            var ids = Ident.Idents.Empty;
            var dm = cx._Dom(fm);
            for (var b= dm?.rowType.First();b!=null;b=b.Next())
            if (b.value() is long p && cx._Ob(p) is SqlValue cv){
                if (cv.name == null)
                    throw new PEException("PE5030");
                var f = new SqlField(cx.GetUid(), cv.name, ic.iix.dp, cv.domain, fm.iSMap[cv.defpos]??-1L);
                var cix = new Iix(f.defpos, cx, f.defpos);
                cx.Add(f);
                ids += (f.name??"", cix, Ident.Idents.Empty);
            }
            cx.defs += (ic.ident, ic.iix, ids);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlOldRow(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlOldRow(dp,m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        /// <summary>
        /// We should not try to recalaculate the contents: use the activation's values
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values[defpos] is TypedValue v)
                return v;
            return base.Eval(cx);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            TriggerActivation? ta = null;
            if (cx.values[defpos] is TransitionRowSet.TargetCursor tgc)
            for (var c = cx; ta == null && c != null; c = c.next)
                if (c is TriggerActivation t && t._trs?.defpos == tgc._rowsetpos)
                    ta = t;
            if (ta!=null)
                ta.values += (Trigger.OldRow, v);
            base.Set(cx, v);
        }
    }
    // shareable as of 26 April 2021
    internal class SqlNewRow : SqlRow
    {
        internal SqlNewRow(Ident ic, Context cx, RowSet fm)
            : base(ic.iix.dp, _Mem(ic, cx, fm))
        { }
        protected SqlNewRow(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic, Context cx, RowSet fm)
        {
            var r = BTree<long, object>.Empty;
            var tg = cx._Ob(fm.target);
            r = r + (_Domain, fm.domain)
                   + (ObInfo.Name, ic.ident) + (_From, fm.defpos);
            var ids = Ident.Idents.Empty;
            var dm = cx._Dom(tg);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
            if (b.value() is long p && cx._Ob(p) is TableColumn co){
                var f = new SqlField(cx.GetUid(), co.NameFor(cx), ic.iix.dp, co.domain, co.defpos);
                var cix = new Iix(f.defpos, cx, f.defpos);
                cx.Add(f);
                ids += (f.name??"", cix, Ident.Idents.Empty);
            }
            cx.defs += (ic.ident, ic.iix, ids);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlNewRow(dp, m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlNewRow(defpos, m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        /// <summary>
        /// We should not try to recalaculate the contents: use the activation's values
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values[defpos] is TypedValue v)
                return v;
            return base.Eval(cx);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            TriggerActivation? ta = null;
            if (cx.values[defpos] is TransitionRowSet.TargetCursor tgc)
                for (var c = cx; ta == null && c != null; c = c.next)
                    if (c is TriggerActivation t && t._trs?.defpos == tgc._rowsetpos)
                        ta = t;
            if (ta != null)
                ta.values += (Trigger.NewRow, v);
            base.Set(cx, v);
        }
    }
    // shareable as of 26 April 2021
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = -319; // BList<long?> SqlValue
        internal BList<long?> rows =>
            (BList<long?>?)mem[Rows]?? BList<long?>.Empty;
        public SqlRowArray(long dp,Context cx,Domain ap,BList<long?> rs) 
            : base(dp, BTree<long, object>.Empty+(_Domain, ap.defpos)+(Rows, rs)
                  +(_Depth,cx.Depth(rs,new BList<DBObject?>(ap))))
        { }
        internal SqlRowArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlRowArray operator+(SqlRowArray s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlRowArray(s.defpos, s.mem + x);
        }
        public static SqlRowArray operator+(SqlRowArray s,SqlRow x)
        {
            return new SqlRowArray(s.defpos, s.mem + (Rows, s.rows + x.defpos));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowArray(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlRowArray(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nr = cx.FixLl(rows);
            if (nr != rows)
                r += (Rows, nr);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var rws = BList<long?>.Empty;
            var ch = false;
            for (var b = rows?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    ch = ch || v != b.value();
                    rws += v;
                }
            if (ch)
                r += (Rows, rws);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.Grouped(cx, gs))
                    return false;
            return true;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRowArray)base.AddFrom(cx, q);
            var rws = BList<long?>.Empty;
            var ch = false;
            var dm = cx._Dom(this)??Domain.Content;
            var ag = dm.aggs;
            for (var b = r.rows?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlRow o && o.AddFrom(cx, q) is SqlRow a)
                {
                    if (a.defpos != b.value())
                        ch = true;
                    rws += a.defpos;
                    ag += a.IsAggregation(cx);
                }
            if (ch)
                r += (Rows, rws);
            if (ag!=dm.aggs)
            {
                dm = (Domain)dm.New(cx,dm.mem+(Domain.Aggs, ag));
                r += (Domain.Aggs, ag);
            }
            r += (_Domain, dm.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5101");
            if (kb[defpos] is Domain d0)
                return new CTree<long, Domain>(defpos, d0);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(p);
                }
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override TypedValue Eval(Context cx)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5102");
            var vs = BList<TypedValue>.Empty;
            for (var b=rows.First(); b!=null; b=b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                vs += v.Eval(cx);
            return new TArray(dm, vs);
        }
        internal override RowSet RowSetFor(long dp, Context cx, BList<long?> us,
            CTree<long, Domain> re)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE5103");
            var rs = BList<(long, TRow)>.Empty;
            var xp = (Domain)dm.New(cx, dm.mem + (Domain.Kind, Sqlx.TABLE));
            var isConst = true;
            if (us != null && !Context.Match(us,xp.rowType))
                xp = (Domain)dm.New(cx, xp.mem + (Domain.RowType, us)
                    + (Domain.Representation, xp.representation + re));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v &&
                    v.Eval(cx) is TypedValue x && x.ToArray() is TypedValue[] y)
                    rs += (v.defpos, new TRow(xp, y));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.obs[p] ?? throw new DBException("42000");
                    isConst = (v as SqlValue)?.isConstant(cx) == true;
                    var x = v.Eval(cx);
                    var y = x.ToArray() ?? throw new DBException("42000");
                    rs += (v.defpos, new TRow(xp, y));
                }
            if (isConst)
                return new ExplicitRowSet(dp, cx, xp, rs);
            return new SqlRowSet(dp, cx, xp, rows);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    qn = v.Needs(cx,r,qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var dm = cx._Dom(this) ?? Domain.Content;
            BList<DBObject> ls;
            var ch = false;
            var d = depth;
            var rs = BList<long?>.Empty;
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                    rs += v.defpos;
                }
            if (ch)
            {
                r = new SqlRowArray(defpos, cx, dm, rs);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlXmlValue : SqlValue
    {
        internal const long
            Attrs = -323, // BList<(XmlName,long)> SqlValue
            Children = -324, // BList<long?> SqlXmlValue
            Content = -325, // long SqlXmlValue
            Element = -326; // XmlName
        public XmlName element => (XmlName?)mem[Element]??new XmlName("","");
        public BList<(XmlName, long)> attrs =>
            (BList<(XmlName, long)>?)mem[Attrs] ?? BList<(XmlName, long)>.Empty;
        public BList<long?> children =>
            (BList<long?>?)mem[Children]?? BList<long?>.Empty;
        public long content => (long)(mem[Content]??-1L); // will become a string literal on evaluation
        public SqlXmlValue(long dp, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, (m ?? BTree<long, object>.Empty) + (_Domain, Domain.XML.defpos) 
                  + (Element,n)+(Content,c.defpos)) { }
        protected SqlXmlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlXmlValue operator+(SqlXmlValue s,(long,object)m)
        {
            return new SqlXmlValue(s.defpos, s.mem + m);
        }
        public static SqlXmlValue operator +(SqlXmlValue s, SqlXmlValue child)
        {
            return new SqlXmlValue(s.defpos, 
                s.mem + (Children,s.children+child.defpos));
        }
        public static SqlXmlValue operator +(SqlXmlValue s, (XmlName,SqlValue) attr)
        {
            var (n, a) = attr;
            return new SqlXmlValue(s.defpos,
                s.mem + (Attrs, s.attrs + (n,a.defpos)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlXmlValue(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlXmlValue(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var na = cx.FixLXl(attrs);
            if (na!=attrs)
            r += (Attrs, na);
            var nc = cx.FixLl(children);
            if (nc!=children)
            r += (Children, nc);
            var nv = cx.Fix(content);
            if (content!=nv)
                r += (Content, nv);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var at = attrs;
            for (var b=at.First();b!=null;b=b.Next())
            {
                var (n, ao) = b.value();
                var v = cx.ObReplace(ao,so,sv);
                if (v != ao)
                    at = new BList<(XmlName,long)>(at,b.key(), (n, v));
            }
            if (at!=null && at != attrs)
                r += (Attrs, at);
            var co = cx.ObReplace(content,so,sv);
            if (co != content)
                r += (Content, co);
            var ch = children;
            for (var b = ch.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        ch = new BList<long?>(ch, b.key(), v);
                }
            if (ch!=null && ch != children)
                r += (Children, ch);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlXmlValue)base.AddFrom(cx, q);
            var aa = r.attrs;
            for (var b=r.attrs.First();b!=null;b=b.Next())
            {
                var (n, ao) = b.value();
                if (cx.obs[ao] is SqlValue o)
                {
                    var a = o.AddFrom(cx, q);
                    if (a.defpos != ao)
                        aa = new BList<(XmlName, long)>(aa, b.key(), (n, a.defpos));
                }
            }
            if (aa != r.attrs)
                r += (Attrs, aa);
            var ch = r.children;
            for (var b = r.children.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlXmlValue o)
                {
                    var a = o.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch += (b.key(), a.defpos);
                }
            if (ch != r.children)
                r += (Children, ch);
            if (cx.obs[r.content] is SqlValue oc)
            {
                var c = oc.AddFrom(cx, q);
                if (c.defpos != r.content)
                    r += (Content, c.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override TypedValue Eval(Context cx)
        {
            var r = new TXml(element.ToString());
            for (var b = attrs?.First(); b != null; b = b.Next())
            {
                var (n, a) = b.value();
                if (cx.obs[a]?.Eval(cx) is TypedValue ta)
                    r += (n.ToString(), ta);
            }
            for(var b=children?.First();b!=null;b=b.Next())
                if (b.value() is long p && cx.obs[p]?.Eval(cx) is TypedValue tc)
                    r +=(TXml)tc;
            if (cx.obs[content]?.Eval(cx) is TypedValue tv)
                r += tv.ToString();
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[content] is SqlValue x)
                qn = x.Needs(cx, r, qn);
            for (var b = attrs.First(); b != null; b = b.Next())
            {
                var (_, a) = b.value();
                if (cx.obs[a] is SqlValue y)
                    qn = y.Needs(cx, r, qn);
            }
            for (var b = children.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue z)
                    qn = z.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var ch = false;
            var d = depth;
            var a = BTree<int, (XmlName, long)>.Empty;
            var c = BList<long?>.Empty;
            for (var b = attrs.First(); b != null; b = b.Next())
            {
                var (n, p) = b.value();
                var v = (SqlValue?)cx.obs[p] ?? SqlNull.Value;
                if (v != SqlNull.Value)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                }
                a += (b.key(), (n, v.defpos));
            }
            for (var b = children.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                    c += v.defpos;
                }
            var co = (SqlValue?)cx.obs[content] ?? SqlNull.Value;
            if (co != SqlNull.Value)
            {
                (ls, m) = co.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=co.defpos)
                {
                    ch = true; co = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new SqlXmlValue(defpos, element, co, m);
                if (a != BTree<int, (XmlName, long)>.Empty)
                    r += (Attrs, a);
                if (c != BList<long?>.Empty)
                    r += (Children, c);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("<");
            sb.Append(element.ToString());
            for(var b=attrs.First();b!=null;b=b.Next())
            {
                sb.Append(' ');
                sb.Append(b.value().Item1);
                sb.Append('=');
                sb.Append(Uid(b.value().Item2));
            }
            if (content != -1L || children.Count != 0)
            {
                sb.Append('>');
                if (content != -1L)
                    sb.Append(Uid(content));
                else
                    for (var b = children.First(); b != null; b = b.Next())
                        if (b.value() is long p)
                            sb.Append(" "+Uid(p));
                sb.Append("</");
                sb.Append(element.ToString());
            }
            else sb.Append('/');
            sb.Append('>');
            return sb.ToString();
        }
        // shareable as of 26 April 2021
        internal class XmlName
        {
            public readonly string prefix;
            public readonly string keyname;
            public XmlName(string k,string p="") {
                keyname = k;
                prefix = p;
            }
            public override string ToString()
            {
                if (prefix == "")
                    return keyname;
                return prefix + ":" + keyname;
            }
        }
    }
    // shareable as of 26 April 2021
    internal class SqlSelectArray : SqlValue
    {
        internal const long
            ArrayValuedQE = -327; // long RowSet
        public long aqe => (long)(mem[ArrayValuedQE]??-1L);
        public SqlSelectArray(long dp, Context cx, RowSet qe, BTree<long, object>? m = null)
            : base(dp, (m ?? BTree<long, object>.Empty + (Domain.Aggs,cx._Dom(qe)?.aggs??CTree<long,bool>.Empty)
                  + (_Domain, qe.domain) + (ArrayValuedQE, qe.defpos))) { }
        protected SqlSelectArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator+(SqlSelectArray s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlSelectArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSelectArray(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlSelectArray(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nq = cx.Fix(aqe);
            if (nq!=aqe)
            r += (ArrayValuedQE, nq);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var ae = cx.ObReplace(aqe,so,sv);
            if (ae != aqe)
                r += (ArrayValuedQE, ae);
             return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return false;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this)??Domain.Content;
            var q = (RowSet?)cx.obs[aqe] ?? throw new PEException("PE1701");
            var va = BList<TypedValue>.Empty;
            var et = cx._Dom(dm.elType);
            var nm = q.name;
            for (var rb=q.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb;
                if (et==null && nm!=null && rw[nm] is TypedValue v)
                    va += v;
                else
                {
                    var qd = cx._Dom(q)??Domain.Content;
                    var vs = new TypedValue[qd.display];
                    for (var i = 0; i < qd.display; i++)
                        vs[i] = rw[i];
                    va += new TRow(qd, vs);
                }
            }
            return new TArray(dm,va);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[aqe]?.StartCounter(cx,rs,tg)??tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[aqe]?.AddIn(cx,rb,tg)??tg;
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from where etc
        /// From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        public override string ToString()
        {
            return "ARRAY[..]";
        }
    }
    /// <summary>
    /// an array value
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlValueArray : SqlValue
    {
        internal const long
            Array = -328, // BList<long?> SqlValue
            Svs = -329; // long SqlValueSelect
        /// <summary>
        /// the array
        /// </summary>
        public BList<long?> array =>(BList<long?>?)mem[Array]??BList<long?>.Empty;
        // alternatively, the source
        public long svs => (long)(mem[Svs] ?? -1L);
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(long dp,Domain xp,BList<long?> v)
            : base(dp,BTree<long, object>.Empty+(_Domain, xp.defpos)+(Array,v))
        { }
        protected SqlValueArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueArray operator+(SqlValueArray s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlValueArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueArray(defpos,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                r += v._Rdc(cx);
            if (cx.obs[svs] is SqlValue s) 
                r += s._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlValueArray(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            r += (Array, cx.FixLl(array));
            if (svs>=0)
                r += (Svs, cx.Fix(svs));
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object> m)
        {
            var r = base._Replace(cx, so, sv, m);
            var ar = BList<long?>.Empty;
            for (var b = ar.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        ar += (b.key(), v);
                }
            if (ar != array)
                r += (Array, ar);
            var ss = cx.ObReplace(svs, so, sv);
            if (ss != svs)
                r += (Svs, ss);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0 || cx._Dom(this) is not Domain dm)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = BList<long?>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v) {
                    var a = v.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    ar += a.defpos;
                    ag += a.IsAggregation(cx);
                }
            if (ch)
                r += (Array, ar);
            if (cx.obs[svs] is SqlValue s)
            {
                s = s.AddFrom(cx, q);
                if (s.defpos != svs)
                    r += (Svs, s.defpos);
                ag += s.IsAggregation(cx);
            }
            if (ag!=dm.aggs)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                r += (Domain.Aggs, ag);
            }
            r += (_Domain, dm.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return ((SqlValue?)cx.obs[svs])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return ((SqlValue?)cx.obs[svs])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(p);
                }
            if (y && cx._Dom(this) is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this) ?? throw new PEException("PE5070");
            if (svs != -1L)
            {
                var ar = CList<TypedValue>.Empty;
                if (cx.obs[svs]?.Eval(cx) is TArray ers)
                    for (var b = ers.list?.First(); b != null; b = b.Next())
                        if (b.value()[0] is TypedValue v)
                            ar += v;
                return new TArray(dm, ar);
            }
            var vs = BList<TypedValue>.Empty;
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    vs += cx.obs[p]?.Eval(cx) ?? dm.defaultValue;
            return new TArray(dm, vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            if (cx.obs[svs] is SqlValue s)
                tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.AddIn(cx, rb, tg);
            if (cx.obs[svs] is SqlValue s)
                tg = s.AddIn(cx, rb, tg);
            return base.AddIn(cx,rb, tg);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    qn = v.Needs(cx, r, qn);
            if (cx.obs[svs] is SqlValue s)
                qn = s.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject> ls;
            var r = this;
            var ch = false;
            var d = depth;
            var dm = cx._Dom(this) ?? Domain.Content;
            var vs = BList<long?>.Empty;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v) {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                    vs += v.defpos;
                }
            var sva = (SqlValue?)cx.obs[svs] ?? SqlNull.Value;
            if (sva!=SqlNull.Value)
            {
                (ls, m) = sva.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=sva.defpos)
                {
                    ch = true; sva = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new SqlValueArray(defpos, dm, vs);
                if (sva != SqlNull.Value)
                    r += (Svs, sva.defpos);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            return "VALUES..";
        }
    }
    /// <summary>
    /// an multiset value
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlValueMultiset : SqlValue
    {
        internal const long
            MultiSqlValues = -302; // CTree<long,bool> SqlValue
        /// <summary>
        /// the array
        /// </summary>
        public CTree<long,bool> multi => (CTree<long,bool>)(mem[MultiSqlValues] ?? CTree<long,bool>.Empty);
        /// <summary>
        /// construct an SqlValueMultiset value
        /// </summary>
        /// <param name="a">the array</param>
        public SqlValueMultiset(long dp, Domain xp, CTree<long,bool> v)
            : base(dp, BTree<long, object>.Empty + (_Domain, xp.defpos) + (MultiSqlValues, v))
        { }
        protected SqlValueMultiset(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueMultiset operator +(SqlValueMultiset s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlValueMultiset(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueMultiset(defpos, m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    r += v._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlValueMultiset(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            r += (MultiSqlValues, cx.FixTlb(multi));
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var mu = CTree<long, bool>.Empty;
            for (var b = multi.First(); b != null; b = b.Next())
            {
                var v = cx.ObReplace(b.key(), so, sv);
                if (v != b.key())
                    mu += (v, true);
            }
            if (mu != multi)
                r += (MultiSqlValues, mu);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0 || cx._Dom(this) is not Domain dm)
                return this;
            var r = (SqlValueMultiset)base.AddFrom(cx, q);
            var mu = CTree<long,bool>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                {
                    var a = v.AddFrom(cx, q);
                    if (a.defpos != b.key())
                        ch = true;
                    mu += (a.defpos,true);
                    ag += a.IsAggregation(cx);
                }
            if (ch)
                r += (MultiSqlValues, mu);
            if (ag != dm.aggs)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                r += (Domain.Aggs, ag);
            }
            r += (_Domain, dm.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = multi?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = multi?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = multi?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                {
                    r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(b.key());
                }
            if (y && cx._Dom(this) is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        /// <summary>
        /// evaluate the multiset
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this) ?? throw new PEException("PE5070");
            var vs = BTree<TypedValue,long?>.Empty;
            var n = 0;
            for (var b = multi?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()]?.Eval(cx) is TEdge te)
                {
                    vs += (te, 1L);
                    n++;
                }
            return new TMultiset(dm, vs, n);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    tg = v.AddIn(cx, rb, tg);
            return base.AddIn(cx, rb, tg);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject> ls;
            var r = this;
            var d = depth;
            var vs = BList<long?>.Empty;
            for (var b = multi.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                    vs += v.defpos;
                }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            return "VALUES..";
        }
    }

    /// <summary>
    /// A subquery
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlValueSelect : SqlValue
    {
        internal const long
            Expr = -330; // long RowSet
        /// <summary>
        /// the subquery
        /// </summary>
        public long expr =>(long)(mem[Expr]??-1L);
        internal bool scalar => (bool)(mem[RowSet.Scalar] ?? false);
        public SqlValueSelect(long dp,Context cx,RowSet r,Domain xp)
            : base(dp, BTree<long, object>.Empty + (Domain.Aggs,(cx._Dom(r)??Domain.Null).aggs)
                  + (Expr, r.defpos) + (_Domain, r.domain) + (RowSet.Scalar,xp.kind!=Sqlx.TABLE)
                  + (Dependents, new CTree<long, bool>(r.defpos, true))
                  + (_Depth, r.depth + 1))
        {
            r += (SelectRowSet.ValueSelect, dp);
            cx.Add(r);
        }
        protected SqlValueSelect(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueSelect operator+(SqlValueSelect s,(long,object)x)
        {
            return new SqlValueSelect(s.defpos, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueSelect(defpos,m);
        }
        internal override RowSet RowSetFor(long dp, Context cx, BList<long?> us,
            CTree<long,Domain> re)
        {
            if (cx.obs[expr] is not RowSet r || cx._Dom(r) is not Domain dm)
                throw new PEException("PE5200");
            if (us == null || Context.Match(us,dm.rowType))
                return r;
            var xp = (Domain)dm.New(cx,dm.mem + (Domain.RowType, us)
                +(Domain.Representation,dm.representation+re));
            return new SelectedRowSet(cx, xp.defpos, r);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[expr]?._Rdc(cx)??CTree<long,bool>.Empty;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlValueSelect(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.Fix(expr);
            if (ne!=expr)
            r += (Expr,ne);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var ex = (RowSet)cx._Replace(expr,so,sv);
            var d = Math.Max(depth, ex.depth + 1);
            if (ex.defpos != expr)
                r = r + (Expr, ex.defpos) + (Dependents, dependents + (ex.defpos, true)) + (_Depth, d);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            if (cx._Dom(this) is not Domain dm || cx.obs[expr] is not RowSet ers)
                throw new PEException("PE6200");
            if (scalar)
            {
                var r = ers.First(cx)?[0] ?? dm.defaultValue;
        //        cx.funcs -= ers.defpos;
                return r;
            }
            var rs = BList<TypedValue>.Empty;
            for (var b = ers.First(cx); b != null; b = b.Next(cx))
                rs += b;
            return new TArray(dm, rs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[expr]?.StartCounter(cx,rs,tg)??tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[expr]?.AddIn(cx,rb,tg)??tg;
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[expr] is not RowSet e || cx._Dom(e) is not Domain ed)
                throw new PEException("PE6201");
            var nd = CTree<long, bool>.Empty;
            for (var b = ed.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    nd += v.Needs(cx);
            for (var b = e.where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    nd += v.Needs(cx);
            for (var b = e.Sources(cx).First(); b != null; b = b.Next())
                if (cx._Dom(b.key()) is Domain dm)
                    for (var c = dm.rowType.First(); c != null && c.key() < dm.display; c = c.Next())
                        nd -= c.key();
            return qn + nd;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            return cx.obs[expr]?.Needs(cx, rs)??CTree<long,bool>.Empty;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
   //         sb.Append(" TargetType: ");sb.Append(targetType);
            sb.Append(" (");sb.Append(Uid(expr)); sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Column Function SqlValue class
    /// shareable as of 26 April 2021
    /// </summary>
    internal class ColumnFunction : SqlValue
    {
        internal const long
            Bits = -333; // BList<Ident>
        /// <summary>
        /// the set of column references
        /// </summary>
        internal BList<Ident> bits => (BList<Ident>?)mem[Bits]??BList<Ident>.Empty;
        /// <summary>
        /// constructor: a new ColumnFunction
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="t">the datatype</param>
        /// <param name="c">the set of TableColumns</param>
        public ColumnFunction(long dp, BList<Ident> c)
            : base(dp, BTree<long, object>.Empty+(_Domain, Domain.Bool.defpos)+ (Bits, c)) { }
        protected ColumnFunction(long dp, BTree<long, object> m) :base(dp, m) { }
        public static ColumnFunction operator+(ColumnFunction s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new ColumnFunction(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnFunction(defpos,mem);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            return false;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ColumnFunction(dp,m);
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return true;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("grouping");
            var cm = '(';
            for (var b = bits.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.value());
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlCursor : SqlValue
    {
        internal const long
            Spec = -334; // long RowSet
        internal long spec=>(long)(mem[Spec]??-1L);
        internal SqlCursor(long dp, RowSet cs, string n) 
            : base(dp, BTree<long, object>.Empty+
                  (_Domain, cs.domain)+(ObInfo.Name, n)+(Spec,cs.defpos)
                  +(Dependents,new CTree<long,bool>(cs.defpos,true))
                  +(_Depth,1+cs.depth))
        { }
        protected SqlCursor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCursor operator+(SqlCursor s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlCursor(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCursor(defpos,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[spec]?._Rdc(cx)??CTree<long,bool>.Empty;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlCursor(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(spec);
            if (ns != spec)
                r += (Spec, ns);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var sp = cx.ObReplace(spec,so,sv);
            if (sp != spec)
                r += (Spec, sp);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return false;
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed 
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(name);
            sb.Append(" cursor for ");
            sb.Append(Uid(spec));
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlCall : SqlValue
    {
        internal const long
            Call = -335; // long CallStatement
        public long call =>(long)(mem[Call]??-1L);
        public SqlCall(long dp, CallStatement c, BTree<long, object>?m = null)
            : base(dp, m ?? BTree<long, object>.Empty
                  + (_Domain, c.domain) + (Domain.Aggs, c.aggs)
                  + (Call, c.defpos)+(Dependents,new CTree<long,bool>(c.defpos,true))
                  +(_Depth,1+c.depth)+(ObInfo.Name,c.name))
        {  }
        protected SqlCall(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCall operator+(SqlCall c,(long,object)x)
        {
            var (dp, ob) = x;
            if (c.mem[dp] == ob)
                return c;
            return (SqlCall)c.New(c.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCall(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlCall(dp,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[call]?._Rdc(cx)??CTree<long,bool>.Empty;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nc = cx.Fix(call);
            if (nc != call)
                r += (Call, nc);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlCall)base.AddFrom(cx, q);
            if (cx.obs[call] is CallStatement c)
            {
                if (cx.obs[c.var] is SqlValue a)
                {
                    a = a.AddFrom(cx, q);
                    if (a.defpos != c.var)
                        c += (CallStatement.Var, a.defpos);
                }
                var vs = BList<long?>.Empty;
                for (var b = c.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        vs += v.AddFrom(cx, q).defpos;
                c += (CallStatement.Parms, vs);
                r += (Call, c.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var ca = cx.ObReplace(call,so,sv);
            if (ca != call)
                r += (Call, ca);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            if (cx.obs[call] is CallStatement ca)
            for (var b=ca.parms.First();b!=null;b=b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx,q, ambient))
                        return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            if (cx.obs[call] is CallStatement ca)
                for (var b = ca.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                        return false;
            return true;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[call] is CallStatement ca)
                for (var b = ca.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        tg = v.AddIn(cx, rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long r, CTree<long, bool> qn)
        {
            if (cx.obs[call] is CallStatement ca)
                for (var b = ca.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        qn = v.Needs(cx,r,qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[call] is CallStatement ca)
                for (var b = ca.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        r += v.Needs(cx, rs);
            return r;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm,
            Grant.Privilege pr = Grant.Privilege.Select, string? a = null)
        {
            var ro = cx.role ?? throw new DBException("42105");
            if (cx.obs[call] is not CallStatement pc || cx.db.objects[pc.procdefpos] is not Procedure proc)
                throw new PEException("PE6840");
            proc = (Procedure)proc.Instance(id.iix.dp, cx);
            var prs = new ProcRowSet(this, cx);
            cx.Add(prs);
            if (cx._Dom(proc) is not Domain pd || cx._Dom(prs) is not Domain rd)
                throw new PEException("PE6841");
            var ma = BTree<string, long?>.Empty;
            for (var b = pd.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    ma += (v.NameFor(cx), b.value());
            for (var b = rd.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv && sv.name is string nm 
                    && ma[nm] is long mp &&
                    sv.domain == Domain.Content.defpos && ma.Contains(sv.NameFor(cx)))
                    cx.Add(new SqlCopy(sv.defpos, cx, nm, defpos, mp));
            var m = BTree<long, object>.Empty;
            if (proc.infos[ro.defpos] is ObInfo pi && pi.names != BTree<string, long?>.Empty
                && pi.name!=null)
                m = m + (ObInfo.Names, pi.names)+ (ObInfo.Name, pi.name);
            m = m + (RowSet.Target, pc.procdefpos) + (_Depth, 1 + depth)
                + (_Domain, proc.domain) 
                + (_Depth, Depth(cx,new BList<DBObject?>(proc)+prs));
            prs = (ProcRowSet)prs.Apply(m, cx);
            return prs;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(name);  
            sb.Append(' '); sb.Append(Uid(call));
            return sb.ToString();
        }
    }
    /// <summary>
    /// An SqlValue that is a procedure/function call or static method
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(long dp, CallStatement c) : base(dp, c) { }
        protected SqlProcedureCall(long dp,BTree<long,object>m):base(dp,m) { }
        public static SqlProcedureCall operator+(SqlProcedureCall s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlProcedureCall(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlProcedureCall(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlProcedureCall(dp,m);
        }
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            if (cx.obs[call] is not CallStatement c || cx._Dom(this) is not Domain dm)
                throw new PEException("PE47166");
            try
            {
                var pp = c.procdefpos;
                var proc = cx.obs[pp] as Procedure;
                if (proc==null && tr.objects[pp] is Procedure tp)
                {
                    proc = (Procedure)tp.Instance(defpos,cx);
                    cx.Add(proc);
                }
                if (proc == null)
                    throw new PEException("PE47167");
                proc.Exec(cx, c.parms);
                var r = cx.val??dm.defaultValue;
                cx.values += (defpos, r);
                return r;
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return dm.defaultValue;
            }
        }
        internal override void Eqs(Context cx,ref Adapters eqs)
        {
            if (cx.obs[call] is not CallStatement c || cx.obs[c.procdefpos] is not Procedure proc)
                throw new PEException("PE47168");
            if (cx.db.objects[proc.inverse] is Procedure inv && c.parms[0] is long cp)
                eqs = eqs.Add(proc.defpos, cp, proc.defpos, inv.defpos);
            base.Eqs(cx,ref eqs);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[call] is CallStatement c)
                for (var b = c.parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        r += v.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// A SqlValue that is evaluated by calling a method
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlMethodCall : SqlCall // instance methods
    {
        /// <summary>
        /// construct a new MethodCall SqlValue.
        /// At construction time the proc and target will be unknown.
        /// Domain of a MethodCall is the result domain
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="c">the call statement</param>
        public SqlMethodCall(long dp, CallStatement c) : base(dp,c)
        { }
        protected SqlMethodCall(long dp,BTree<long, object> m) : base(dp, m) { }
        public static SqlMethodCall operator+(SqlMethodCall s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlMethodCall(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlMethodCall(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlMethodCall(dp,m);
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[call] is CallStatement c)
                qn += (c.var, true);
            return base.Needs(cx, r, qn);
        }
        internal override (BList<DBObject>, BTree<long,object>) Resolve(Context cx, long f, BTree<long,object> m)
        {
            if (domain>=0)
                return (new BList<DBObject>(this),m);
            BList<DBObject> ls;
            if (cx.obs[call] is CallStatement c)
            {
                (ls, m) = base.Resolve(cx, f, m);
                if (ls[0] is SqlMethodCall mc && cx.obs[c.var] is SqlValue ov)
                {
                    (ls, m) = ov.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv)
                        ov = nv;
                    if (cx.role!=null && infos[cx.role.defpos] is ObInfo oi)
                    { // we need the names
                        var p = oi.methodInfos[c.name]?[cx.Signature(c.parms)] ?? -1L;
                        var nc = c + (CallStatement.Var, ov.defpos) + (CallStatement.ProcDefPos, p);
                        cx.Add(nc);
                        if (cx.db.objects[p] is DBObject ob && ob.Instance(defpos, cx) is Procedure pr)
                        { 
                            mc = mc + (Call, nc.defpos) + (_Domain, pr.domain);
                            return (new BList<DBObject>((SqlValue)cx.Add(mc)), m);
                        }
                    }
                }
            }
            return (new BList<DBObject>(this), mem);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[call] is not CallStatement c || cx.obs[c.var] is not SqlValue v
                || cx._Dom(v) is not Domain dv || cx.role==null)
                throw new PEException("PE241");
            var vv = v.Eval(cx);
            for (var b = dv.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && vv[bp] is TypedValue tv)
                    cx.values += (bp, tv);
            var p = c.procdefpos;
            if (p<0 && dv.infos[cx.role.defpos] is ObInfo oi)
                p = oi.methodInfos[c.name]?[cx.Signature(c.parms)]??-1L;
            if (cx.db.objects[p] is not Method me)
                throw new DBException("42108", c.name);
            var proc = (Method)me.Instance(c.defpos,cx);
            return proc.Exec(cx, c.var, c.parms).val;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[call] is not CallStatement c || cx.obs[c.var] is not SqlValue v)
                throw new PEException("PE47196");
            var r = v.Needs(cx, rs);
            for (var b = c.parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue x)
                    r += x.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// An SqlValue that is a constructor expression
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(long dp, Context cx, Domain ut, CallStatement c)
            : base(dp, (CallStatement)cx.Add(c+(_Domain,ut.defpos)))
        { }
        protected SqlConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlConstructor operator+(SqlConstructor s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlConstructor(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlConstructor(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlConstructor(dp,m);
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            if (cx.obs[call] is not CallStatement c ||
                tr.objects[c.procdefpos] is not Method proc)
                throw new PEException("PE5802");
            var ac = new CalledActivation(cx, proc);
            return proc.Exec(ac, -1L, c.parms).val;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
    }
    /// <summary>
    /// An SqlValue corresponding to a default constructor call
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlDefaultConstructor : SqlValue
    {
        internal const long
            Sce = -336; // long SqlRow
        /// <summary>
        /// the type
        /// </summary>
        public long sce=>(long)(mem[Sce]??-1L);
        /// <summary>
        /// construct a SqlValue default constructor for a type
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="lk">the actual parameters</param>
        public SqlDefaultConstructor(long dp, Context cx, Domain u, BList<long?> ins)
            : base(dp, _Mem(cx.GetUid(),cx,(UDType)u,ins))
        { }
        protected SqlDefaultConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlDefaultConstructor)s.New(s.mem + x);
        }
        static BTree<long,object> _Mem(long ap,Context cx,UDType u,BList<long?> ps)
        {
            var rb = u.representation.First();
            for (var b = ps.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
             if (b.value() is long p && cx.obs[p] is SqlValue v){
                var dt = rb.value();
                cx.Add(dt);
                cx.Add(v + (_Domain, dt.defpos));
            }
            return BTree<long, object>.Empty
                  + (Sce, cx._Add(new SqlRow(ap, cx, u, ps)).defpos)
                  + (_Domain, u.defpos) + _Deps(cx,ps);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDefaultConstructor(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlDefaultConstructor(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            if (cx.obs[sce] is SqlRow os)
            {
                var sc = os.Replace(cx, so, sv);
                if (sc.defpos != sce)
                    r += (Sce, sc.defpos);
            }
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(sce);
            if (ns != sce)
                r += (Sce, ns);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlDefaultConstructor)base.AddFrom(cx, q);
            if (cx.obs[r.sce] is SqlRow sc)
            {
                var a = sc.AddFrom(cx, q);
                cx.Add(a);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override CTree<long,bool> Needs(Context cx, long rs)
        {
            return cx.obs[sce]?.Needs(cx, rs)??CTree<long,bool>.Empty;
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE48100");
            try
            { 
                var vs = CTree<long,TypedValue>.Empty;
                if (cx.obs[sce] is SqlRow sc && cx._Dom(sc) is Domain sd)
                {
                    var db = dm.rowType.First();
                    for (var b = sd.rowType.First(); b != null && db!=null; b = b.Next(), db=db.Next())
                        if (b.value() is long p && cx.obs[p] is SqlValue v
                            && db.value() is long dp)
                            vs += (dp, v.Eval(cx));
                }
                cx.values += vs;
                var r = new TRow(dm, vs);
                cx.values += (defpos,r);
                return r;
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return dm.defaultValue;
            }
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var d = depth;
            var sc = (SqlValue?)cx.obs[sce] ?? SqlNull.Value;
            if (sc != SqlNull.Value)
            {
                (ls, m) = sc.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != sc.defpos)
                {
                    r = this + (Sce, nv.defpos) + (_Depth,Math.Max(d,nv.depth));
                    m = cx.Replace(this, r, m);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[sce] is SqlValue nv)
                qn += nv.Needs(cx);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Sce:");sb.Append(Uid(sce));
            return sb.ToString();
        }
     }
    /// <summary>
    /// A built-in SQL function
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlFunction : SqlValue
    {
        internal const long
            Filter = -338, // CTree<long,bool> SqlValue
            Mod = -340, // Sqlx
            Monotonic = -341, // bool
            Op1 = -342, // long SqlValue
            Op2 = -343, // long SqlValue
            _Val = -345,//long SqlValue
            Window = -346, // long WindowSpecification
            WindowId = -347; // long
        /// <summary>
        /// the query
        /// </summary>
        public Sqlx kind => (Sqlx)(mem[Domain.Kind]??Sqlx.NO);
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod => (Sqlx)(mem[Mod]??Sqlx.NO);
        /// <summary>
        /// the value parameter for the function
        /// </summary>
        public long val => (long)(mem[_Val]??-1L);
        /// <summary>
        /// operands for the function
        /// </summary>
        public long op1 => (long)(mem[Op1]??-1L);
        public long op2 => (long)(mem[Op2]??-1L);
        /// <summary>
        /// a Filter for the function
        /// </summary>
        public CTree<long,bool> filter => 
            (CTree<long,bool>?)mem[Filter]??CTree<long,bool>.Empty;
        /// <summary>
        /// a name for the window for a window function
        /// </summary>
        public long windowId => (long)(mem[WindowId]??-1L);
        /// <summary>
        /// the window for a window function
        /// </summary>
        public long window => (long)(mem[Window]??-1L);
        /// <summary>
        /// Check for monotonic
        /// </summary>
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(long dp, Context cx, Sqlx f, SqlValue? vl, SqlValue? o1, SqlValue? o2, Sqlx m,
            BTree<long, object>? mm = null) 
            : base(dp,_Mem(cx,vl,o1,o2,mm)+(_Domain,_Type(cx, f, vl, o1).defpos)
                +(ObInfo.Name,f.ToString())+(Domain.Kind,f)+(Mod,m))
        { }
        protected SqlFunction(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(Context cx, SqlValue? vl, SqlValue? o1, SqlValue? o2, BTree<long, object>? m)
        {
            var r = m ?? BTree<long, object>.Empty;
            if (vl != null)
                r += (_Val, vl.defpos);
            if (o1 != null)
                r += (Op1, o1.defpos);
            if (o2 != null)
                r += (Op2, o2.defpos);
            var w = cx._Ob((long)(m?[Window] ?? -1L)) as WindowSpecification;
            r += _Deps(1, vl, o1, o2, w);
            return r;
        }
        public static SqlFunction operator+(SqlFunction s,(Context,long,object)x)
        {
            var (cx, a, v) = x;
            var m = s.mem + (a, v);
            if (a == Op1 && cx.obs[s.val] is SqlValue sv && cx.obs[(long)v] is SqlValue u)
                m += (_Domain, _Type(cx,s.kind, sv, u).defpos);
            if (a == _Val && cx.obs[s.op1] is SqlValue sw && cx.obs[(long)v] is SqlValue w)
                m += (_Domain, _Type(cx,s.kind,w, sw).defpos);
            return new SqlFunction(s.defpos, m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFunction(defpos,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {

            var r = CTree<long, bool>.Empty;
            if (window>=0) // Window functions do not aggregate rows!
                return r; 
            if (aggregates(kind))
                r += (defpos, true);
            if (cx.obs[val] is SqlValue vl)
                r += vl.IsAggregation(cx);
            if (cx.obs[op1] is SqlValue o1)
                r += o1.IsAggregation(cx);
            if (cx.obs[op2] is SqlValue o2)
                r += o2.IsAggregation(cx);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            if (aggregates(kind))
            {
                for (var b = dm.aggs.First(); b != null; b = b.Next())
                    if (c.obs[b.key()] is SqlFunction sf && Match(c, sf))
                        return sf;
                return base.Having(c, dm);
            }
            SqlValue? nv=null, n1=null, n2=null;
            bool ch = false;
            if (c.obs[val] is SqlValue vl)
            { nv = vl.Having(c, dm); ch = nv != vl; }
            if (c.obs[op1] is SqlValue o1)
            { n1 = o1.Having(c, dm); ch = n1 != o1; }
            if (c.obs[op2] is SqlValue o2)
            { n2 = o2.Having(c, dm); ch = n2 != o2; }
            return ch ? (SqlValue)c.Add(new SqlFunction(c.GetUid(), c, kind, nv, n1, n2, mod)):this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlFunction f)
            {
                if (filter != CTree<long, bool>.Empty)
                {  
                    var fb=f.filter.First();
                    for (var b = filter.First(); b != null; b = b.Next(), fb = fb.Next())
                        if (fb==null || (c.obs[b.key()] is SqlValue le && c.obs[fb.key()] is SqlValue fe
                            && !le.Match(c, fe)))
                            return false;
                    if (fb != null)
                        return false;
                }
                if (kind != f.kind || mod != f.mod || windowId !=f.windowId)
                    return false;
                if (c.obs[op1] is SqlValue o1 && c.obs[f.op1] is SqlValue f1 && !o1.Match(c, f1))
                    return false;
                if (c.obs[op2] is SqlValue o2 && c.obs[f.op2] is SqlValue f2 && !o2.Match(c, f2))
                    return false; 
                if (c.obs[val] is SqlValue vl && c.obs[f.val] is SqlValue fv && !vl.Match(c, fv))
                    return false;
                if (window>=0 || f.window>=0)
                    return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[val] is SqlValue v) r += v._Rdc(cx);
            if (cx.obs[op1] is SqlValue o1) r += o1._Rdc(cx);
            if (cx.obs[op2] is SqlValue o2) r += o2._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlFunction(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nf = cx.FixTlb(filter);
            if (filter !=nf)
                r += (Filter, nf);
            var n1 = cx.Fix(op1);
            if (op1 !=n1)
                r += (Op1, n1);
            var n2 = cx.Fix(op2);
            if (op2 !=n2)
                r += (Op2, n2);
            var w = cx.FixTlb(filter);
            if (w != filter)
                r += (Filter, w);
            var nv = cx.Fix(val);
            if (val !=nv)
                r += (_Val, nv);
            var ni = cx.Fix(windowId);
            if (windowId !=ni)
                r += (WindowId, ni);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlFunction)base.AddFrom(cx, q);
            if (cx.obs[r.val] is SqlValue ov)
            {
                var a = ov.AddFrom(cx, q);
                if (a.defpos != r.val)
                    r += (cx, _Val, a.defpos);
            }
            if (cx.obs[r.op1] is SqlValue o1)
            {
                var a = o1.AddFrom(cx, q);
                if (a.defpos != r.op1)
                    r += (cx, Op1, a.defpos);
            }
            if (cx.obs[r.op2] is SqlValue o2)
            {
                var a = o2.AddFrom(cx, q);
                if (a.defpos != r.op2)
                    r += (cx, Op2, a.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override (BList<DBObject>,BTree<long,object>) Resolve(Context cx, long f, BTree<long,object> m)
        {
            SqlValue vl = (SqlValue?)cx.obs[val] ?? SqlNull.Value, 
                o1 = (SqlValue?)cx.obs[op1] ?? SqlNull.Value, 
                o2 = (SqlValue?)cx.obs[op2] ?? SqlNull.Value, 
                r = this;
            var w = (WindowSpecification?)cx.obs[window];
            var ch = false;
            var d = depth;
            BList<DBObject> ls;
            if (vl!=SqlNull.Value)
            {
                (ls, m) = vl.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != vl.defpos)
                {
                    ch = true; vl = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (o1 != SqlNull.Value)
            {
                (ls, m) = o1.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != o1.defpos)
                {
                    ch = true; o1 = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (o2 != SqlNull.Value)
            {
                (ls, m) = o2.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != o2.defpos)
                {
                    ch = true; o2 = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (cx.obs[window] is WindowSpecification ow)
            {
                (ls, m) = ow.Resolve(cx, f, m); 
                if (ls[0] is WindowSpecification nw && nw.defpos!=ow.defpos)
                {
                    ch = true; w = nw;
                    d = Math.Max(d, nw.depth + 1);
                }
            }
            var ki = cx._Dom(this)?.kind??Sqlx.CONTENT;
            if (ch || ki == Sqlx.CONTENT || ki == Sqlx.UNION)
            {
                var mm = mem;
                if (w != null)
                    mm += (Window, w.defpos);
                r = new SqlFunction(defpos, cx, kind, vl, o1, o2, mod, mm);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r),m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var w = filter;
            var ds = BList<long?>.Empty;
            var os = BList<DBObject?>.Empty;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
                os += v;
            }
            if (w != filter)
                r += (Filter, w);
            var o1 = cx.ObReplace(op1, so, sv);
            if (o1 != op1)
            {
                r += (Op1, o1);
                ds += o1;
            }
            var o2 = cx.ObReplace(op2, so, sv);
            if (o2 != op2)
            {
                r += (Op2, o2);
                ds += o2;
            }
            var vl = cx.ObReplace(val, so, sv);
            if (vl != val)
            {
                r += (_Val, vl);
                ds += vl;
            }
            var dt = cx._Dom(this)??throw new PEException("PE48101");
            if (dt.kind==Sqlx.UNION || dt.kind==Sqlx.CONTENT)
            {
                var dm = _Type(cx, kind, cx._Ob(val) as SqlValue, cx._Ob(op1) as SqlValue);
                if (dm != null && dm != dt)
                {
                    r += (_Domain, dm.defpos);
                    os += dm;
                }
            }
            var fw = cx.ObReplace(window,so,sv);
            if (fw != window)
            {
                r += (Window, fw);
                ds += fw;
            }
            r += (_Depth, Math.Max(depth, cx.Depth(ds, os)));
            return r;
        }
        internal override bool Grouped(Context cx,GroupSpecification gs)
        {
            if (cx.obs[val] is SqlValue v && !v.Grouped(cx, gs)) return false;
            if (cx.obs[op1] is SqlValue o1 && !o1.Grouped(cx, gs)) return false;
            if (cx.obs[op2] is SqlValue o2 && !o2.Grouped(cx, gs)) return false;
            return true;
        }
        internal override bool AggedOrGrouped(Context cx, RowSet r)
        {
            return base.AggedOrGrouped(cx, r);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is SqlValue sv)
                r += sv.Operands(cx);
            if (cx.obs[op1] is SqlValue s1)
                r += s1.Operands(cx);
            if (cx.obs[op2] is SqlValue s2)
                r += s2.Operands(cx);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            if (aggregates(kind) && cx._Dom(q)?.aggs.Contains(defpos)==true)
                return true;
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, q, ambient) != false &&
            ((SqlValue?)cx.obs[op1])?.KnownBy(cx, q, ambient) != false &&
            ((SqlValue?)cx.obs[op2])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            if (cs.Contains(defpos))
                return true;
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient)!=false &&
            ((SqlValue?)cx.obs[op1])?.KnownBy(cx, cs, ambient)!=false &&
            ((SqlValue?)cx.obs[op2])?.KnownBy(cx, cs, ambient) !=false;  
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            var r = CTree<long,Domain>.Empty;
            var y = true;
            if (cx.obs[val] is SqlValue v)
            {
                r += v.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(val);
            }
            if (cx.obs[op1] is SqlValue o1)
            {
                r += o1.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(op1);
            }
            if (cx.obs[op2] is SqlValue o2)
            {
                r += o2.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(op2);
            }
            if (y && cx._Dom(this) is Domain dt)
                return new CTree<long, Domain>(defpos, dt);
            return r;
        }
        internal override Domain _Dom(Context cx)
        {
            return _Type(cx,kind,(SqlValue?)cx.obs[val],(SqlValue?)cx.obs[op1]);
        }
        internal static Domain _Type(Context cx,Sqlx kind,SqlValue? val, SqlValue? op1)
        {
            switch (kind)
            {
                case Sqlx.ABS:
                case Sqlx.CEIL:
                case Sqlx.CEILING: 
                case Sqlx.MOD:
                case Sqlx.SUM:
                case Sqlx.FLOOR:
                    {
                        var d = cx._Dom(val) ?? Domain.UnionNumeric;
                        if (d.kind == Sqlx.CONTENT || d.kind == Sqlx.Null)
                            d = Domain.UnionNumeric;
                        return d;
                    }
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AVG: return Domain.Numeric;
                case Sqlx.ARRAY: return Domain.Collection; 
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CAST: return cx._Dom(op1)??Domain.Content;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: return Domain.Rvv;
                case Sqlx.COLLECT: return Domain.Collection;
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_ROLE: return Domain.Char;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.DESCRIBE: return Domain.Char;
                case Sqlx.ELEMENT: return cx._Dom(val)?.elType??Domain.Content;
                case Sqlx.FIRST: return Domain.Content;
                case Sqlx.EXP: return Domain.Real;
                case Sqlx.EVERY: return Domain.Bool;
                case Sqlx.EXTRACT: return Domain.Int;
                case Sqlx.FUSION: return Domain.Collection;
                case Sqlx.INTERSECTION: return Domain.Collection;
                case Sqlx.LAST: return Domain.Content;
                case Sqlx.SECURITY: return Domain._Level;
                case Sqlx.LN: return Domain.Real;
                case Sqlx.LOCALTIME: return Domain.Timespan;
                case Sqlx.LOCALTIMESTAMP: return Domain.Timestamp;
                case Sqlx.LOWER: return Domain.Char;
                case Sqlx.MAX: return cx._Dom(val) ?? Domain.Content;
                case Sqlx.MIN: return cx._Dom(val) ?? Domain.Content;
                case Sqlx.NEXT: return cx._Dom(val) ?? Domain.UnionDate;
                case Sqlx.NORMALIZE: return Domain.Char;
                case Sqlx.NULLIF: return cx._Dom(op1)??Domain.Content;
                case Sqlx.OCTET_LENGTH: return Domain.Int;
                case Sqlx.OVERLAY: return Domain.Char;
                case Sqlx.PARTITION: return Domain.Char;
                case Sqlx.POSITION: return Domain.Int;
                case Sqlx.POWER: return Domain.Real;
                case Sqlx.RANK: return Domain.Int;
                case Sqlx.ROW_NUMBER: return Domain.Int;
                case Sqlx.SET: return Domain.Collection;
                case Sqlx.SPECIFICTYPE: return Domain.Char;
                case Sqlx.SQRT: return Domain.Real;
                case Sqlx.STDDEV_POP: return Domain.Real;
                case Sqlx.STDDEV_SAMP: return Domain.Real;
                case Sqlx.SUBSTRING: return Domain.Char;
                case Sqlx.TRANSLATE: return Domain.Char;
                case Sqlx.TYPE_URI: return Domain.Char;
                case Sqlx.TRIM: return Domain.Char;
                case Sqlx.UPPER: return Domain.Char;
                case Sqlx.USER: return Domain.Char;
                case Sqlx.VERSIONING: cx.versioned = true;  
                    return (op1==null)?Domain.Int:Domain.Rvv;
                case Sqlx.WHEN: return cx._Dom(val)??Domain.Content;
                case Sqlx.XMLCAST: return cx._Dom(op1)??Domain.Content;
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return Domain.Null;
        }
        internal override TypedValue Eval(Context cx)
        {
            var fc = cx.regs[defpos];
            var ws = cx.obs[window] as WindowSpecification;
            if (ws!=null)
            {
                var kv = CTree<long, TypedValue>.Empty;
                var ks = CList<TypedValue>.Empty;
                var ps = CList<TypedValue>.Empty;
                var pd = cx._Dom(ws.partition) ?? Domain.Row;
                var od = cx._Dom(ws.order)??cx._Dom(ws.partition)??Domain.Row;
                for (var b = od.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue sv)
                    {
                        var tv = sv.Eval(cx);
                        ks += tv;
                        if (b.key() < pd.Length)
                        {
                            kv += (p, tv);
                            ps += tv;
                        }
                    }
                var key = new TRow(od, kv);
                var oc = cx.values;
                fc = StartCounter(cx, key);
                if (ks!=null)
                    fc.wtree?.PositionAt(cx, ks);
                for (RTreeBookmark? b = fc.wtree?.First(cx); b != null; b = (RTreeBookmark?)b.Next(cx))
                {
                    cx.values = b.values;
                    if (InWindow(cx, b, fc))
                        switch (ws.exclude)
                        {
                            case Sqlx.NO:
                            case Sqlx.Null:
                                AddIn(b, cx);
                                break;
                            case Sqlx.CURRENT:
                                if (b._pos != fc.wrb?._pos)
                                    AddIn(b, cx);
                                break;
                            case Sqlx.TIES:
                                if (fc.wrb?.CompareTo(b) != 0)
                                    AddIn(b, cx);
                                break;
                        }

                }
                cx.values = oc;
                switch (kind)
                {
                    case Sqlx.RANK:
                        {
                            if (ks == null)
                                break;
                            var ob = fc.wtree?.PositionAt(cx, ks);
                            var ra = 0;
                            if (fc.wtree is RTree rfc)
                                for (Cursor? pb = rfc.PositionAt(cx, ps); pb != null; pb = pb.Next(cx))
                                {
                                    ra++;
                                    if (ob != null && ob._mb.Value() == ((RTreeBookmark)pb)._mb.Value())
                                    {
                                        fc.row = ra;
                                        break;
                                    }
                                }
                            break;
                        }
                    case Sqlx.ROW_NUMBER:
                        {
                            if (ks == null)
                                break;
                            var ob = fc.wtree?.PositionAt(cx, ps);
                            var rn = 1;
                            for (var pb = ob; pb != null; pb = (RTreeBookmark?)pb.Next(cx), rn++)
                                if (pb._mb!=null && pb._mb.key() is CList<TypedValue> k 
                                    && k.CompareTo(ks)==0)
                                {
                                    fc.row = rn;
                                    break;
                                }
                            break;
                        }
                }
            }
            else if (aggregates(kind))
            {
                if (cx._Ob(from) is not DBObject og)
                    throw new PEException("PE29005");
                var gc = cx.groupCols[og.domain];
                var key = (gc==null || gc == Domain.Null) ? TRow.Empty : new TRow(gc, cx.values);
                fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            }
            var dataType = cx._Dom(this)??throw new PEException("PE1900");
            TypedValue dv = dataType.defaultValue??TNull.Value;
            TypedValue v = TNull.Value;
            switch (kind)
            {
                case Sqlx.ABS:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1902");
                        var vd = cx._Dom(vl) ?? throw new PEException("PE1901");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vd.kind)
                        {
                            case Sqlx.INTEGER:
                                {
                                    if (v is TInt w)
                                        return new TInt((w.value < 0L) ? -w.value : w.value);
                                    break;
                                }
                            case Sqlx.REAL:
                                {
                                    var w = v.ToDouble();
                                    if (w is double.NaN)
                                        break;
                                    return new TReal((w < 0.0) ? -w : w);
                                }
                            case Sqlx.NUMERIC:
                                {
                                    if (v is not TNumeric w)
                                        break;
                                    return new TNumeric((w.value < Numeric.Zero) ? w.value.Negate() : w.value);
                                }
                            case Sqlx.UNION:
                                {
                                    var cs = vd.unionOf;
                                    if (cs.Contains(Domain.Int))
                                        goto case Sqlx.INTEGER;
                                    if (cs.Contains(Domain.Numeric))
                                        goto case Sqlx.NUMERIC;
                                    if (cs.Contains(Domain.Real))
                                        goto case Sqlx.REAL;
                                    break;
                                }
                        }
                        break;
                    }
                case Sqlx.ANY:
                    if (fc == null)
                        break;
                    return TBool.For(fc.bval);
                case Sqlx.ARRAY: // Mongo $push
                    {
                        if (fc == null)
                            break;
                        if (ws == null || fc.mset == null || fc.mset.Count == 0)
                            return fc.acc ?? dv;
                        if (fc.mset.tree?.First()?.key().dataType is not Domain de)
                            throw new PEException("PE48183");
                        var ar = new TArray((Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, de)));
                        for (var d = fc.mset.tree.First(); d != null; d = d.Next())
                            ar += d.key();
                        fc.acc = ar;
                        return fc.acc;
                    }
                case Sqlx.AVG:
                    {
                        if (fc == null)
                            break;
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.NUMERIC:
                                if (fc.sumDecimal is null)
                                    throw new PEException("PE48184");
                                return new TReal(fc.sumDecimal / new Numeric(fc.count));
                            case Sqlx.REAL: return new TReal(fc.sum1 / fc.count);
                            case Sqlx.INTEGER:
                                if (fc.sumInteger is not null)
                                    return new TReal(new Numeric(fc.sumInteger, 0) / new Numeric(fc.count));
                                return new TReal(new Numeric(fc.sumLong) / new Numeric(fc.count));
                        }
                        return dv;
                    }
                case Sqlx.CARDINALITY:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1960");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CAST:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1961");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (cx._Dom(cx.obs[op1]) is Domain cd)
                            return cd.Coerce(cx, v);
                        break;
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1962");
                        var vd = cx._Dom(vl) ?? throw new PEException("PE19063");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vd.kind)
                        {
                            case Sqlx.INTEGER:
                                return v;
                            case Sqlx.DOUBLE:
                                return new TReal(Math.Ceiling(v.ToDouble()));
                            case Sqlx.NUMERIC:
                                return new TNumeric(Numeric.Ceiling(((TNumeric)v).value));
                        }
                        break;
                    }
                case Sqlx.CHAR_LENGTH:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1964");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK:
                    {
                        var rv = Rvv.Empty;
                        if (cx.obs[from] is not RowSet rs)
                            break;
                        for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                            if (b.value() is long p && cx.cursors[p] is Cursor c)
                                rv += (b.key(), c._ds[b.key()]);
                        return new TRvv(rv);
                    }
                case Sqlx.COLLECT:
                    if (fc == null || fc.mset == null)
                        break;
                    return dataType.Coerce(cx, fc.mset);
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
                case Sqlx.COUNT:
                    if (fc == null)
                        break;
                    return new TInt(fc.count);
                case Sqlx.CURRENT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1965");
                        if (vl?.Eval(cx) is Cursor tc && cx.values[tc._rowsetpos] is Cursor tq)
                            return TBool.For(tc._pos == tq._pos);
                        break;
                    }
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE:
                    if (cx.db == null || cx.db.role is not Role ro || ro.name == null)
                        break;
                    return new TChar(ro.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.DESCRIBE:
                    {
                        var nd = cx.cursors[from]?.node(cx);
                        return (TypedValue?)nd ?? TNull.Value;
                    }
                case Sqlx.ELEMENT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1966");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v is not TMultiset m)
                            throw new DBException("42113", v).Mix();
                        if (m.tree == null || m.Count != 1)
                            throw new DBException("21000").Mix();
                        return m.tree.First()?.key() ?? TNull.Value;
                    }
                case Sqlx.EXP:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1967");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Exp(v.ToDouble()));
                    }
                case Sqlx.EVERY:
                    {
                        if (fc == null)
                            break;
                        object? o = fc.mset?.tree?[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Sqlx.EXTRACT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1968");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (v.dataType.kind)
                        {
                            case Sqlx.DATE:
                                {
                                    DateTime dt = ((TDateTime)v).value;
                                    switch (mod)
                                    {
                                        case Sqlx.YEAR: return new TInt(dt.Year);
                                        case Sqlx.MONTH: return new TInt(dt.Month);
                                        case Sqlx.DAY: return new TInt(dt.Day);
                                        case Sqlx.HOUR: return new TInt(dt.Hour);
                                        case Sqlx.MINUTE: return new TInt(dt.Minute);
                                        case Sqlx.SECOND: return new TInt(dt.Second);
                                    }
                                    break;
                                }
                            case Sqlx.INTERVAL:
                                {
                                    Interval it = ((TInterval)v).value;
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
                case Sqlx.FIRST:
                    return fc?.mset?.tree?.First()?.key() ?? throw new DBException("42135");
                case Sqlx.FLOOR:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1969");
                        var vd = cx._Dom(vl) ?? throw new PEException("PE1970");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vd.kind)
                        {
                            case Sqlx.INTEGER:
                                return v;
                            case Sqlx.DOUBLE:
                                return new TReal(Math.Floor(v.ToDouble()));
                            case Sqlx.NUMERIC:
                                return new TNumeric(Numeric.Floor(((TNumeric)v).value));
                        }
                        break;
                    }
                case Sqlx.FUSION: if (fc == null || fc.mset == null) break;
                    return dataType.Coerce(cx, fc.mset);
                case Sqlx.INTERSECTION: if (fc == null || fc.mset == null) break;
                    return dataType.Coerce(cx, fc.mset);
                case Sqlx.LAST: return fc?.mset?.tree?.Last()?.key() ?? throw new DBException("42135");
                case Sqlx.LAST_DATA:
                    {
                        if (cx.obs[from] is not RowSet rs)
                            break;
                        return new TInt(rs.lastData);
                    }
                case Sqlx.LN:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1971");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Log(v.ToDouble()));
                    }
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1972");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return dv;
                    }
                case Sqlx.MAX: if (fc == null) break;
                    return fc.acc ?? dv;
                case Sqlx.MIN: if (fc == null) break;
                    return fc.acc ?? dv;
                case Sqlx.MOD:
                    {
                        if (cx.obs[op1] is not SqlValue o1 || cx._Dom(o1) is not Domain od) break;
                        if (o1 != null)
                            v = o1.Eval(cx);
                        if (v == TNull.Value)
                            return dv;
                        switch (od.kind)
                        {
                            case Sqlx.INTEGER:
                                if (v is not TInt iv || cx.obs[op2]?.Eval(cx) is not TInt mv) break;
                                return new TInt(iv.value % mv.value);
                            case Sqlx.NUMERIC:
                                if (v is not TNumeric nv || cx.obs[op2]?.Eval(cx) is not TNumeric vm)
                                    break;
                                return new TNumeric(nv.value % vm.value);
                        }
                        break;
                    }
                case Sqlx.NORMALIZE:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1974");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        return v; //TBD
                    }
                case Sqlx.NULLIF:
                    {
                        TypedValue a = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return dv;
                        TypedValue b = cx.obs[op2]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return dv;
                        if (a.dataType.Compare(a, b) != 0)
                            return a;
                        return dv;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1975");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TInt(((TBlob)v).value.Length);
                    }
                case Sqlx.OVERLAY:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1976");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        return v; //TBD
                    }
                case Sqlx.PARTITION:
                    return TNull.Value;
                case Sqlx.POSITION:
                    {
                        if (op1 != -1L && op2 != -1L)
                        {
                            string t = cx.obs[op1]?.Eval(cx)?.ToString() ?? "";
                            string s = cx.obs[op2]?.Eval(cx)?.ToString() ?? "";
                            return new TInt(s.IndexOf(t));
                        }
                        break;
                    }
                case Sqlx.POWER:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1977");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        var w = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (w == TNull.Value)
                            return dv;
                        return new TReal(Math.Pow(v.ToDouble(), w.ToDouble()));
                    }
                case Sqlx.RANK:
                case Sqlx.ROW_NUMBER:
                    if (fc == null)
                        break;
                    return new TInt(fc.row);
                case Sqlx.SECURITY:
                    return cx.cursors[from]?[Classification] ?? TLevel.D;
                case Sqlx.SET:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1978");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v is not TMultiset)
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set();
                    }
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SPECIFICTYPE:
                    {
                        var rs = cx._Ob(from) as RowSet;
                        var tr = cx.cursors[rs?.from ?? -1L]?.Rec()?[0];
                        var p = (tr?.subType >= 0) ? tr.subType : tr?.tabledefpos;
                        return new TChar((p is long) ? cx.NameFor(p.Value).Trim(':') : "");
                    }
                case Sqlx.SQRT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1979");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Sqrt(v.ToDouble()));
                    }
                case Sqlx.STDDEV_POP:
                    {
                        if (fc==null || fc.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        double m = fc.sum1 / fc.count;
                        return new TReal(Math.Sqrt((fc.acc1 - 2 * fc.count * m + fc.count * m * m)
                            / fc.count));
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (fc==null || fc.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        double m = fc.sum1 / fc.count;
                        return new TReal(Math.Sqrt((fc.acc1 - 2 * fc.count * m + fc.count * m * m)
                            / (fc.count - 1)));
                    }
                case Sqlx.SUBSTRING:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1980");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string sv = v.ToString();
                        var w = cx.obs[op1]?.Eval(cx)??TNull.Value;
                        if (w is not TInt i1)
                            return dv;
                        var x = cx.obs[op2]?.Eval(cx)??TNull.Value;
                        var n1 = (int)i1.value;
                        if (n1<0 || n1 >= sv.Length)
                            throw new DBException("22000");
                        if (x is not TInt i2)
                            return new TChar(sv[n1..]);
                        var n2 = (int)i2.value;
                        if (n2 < 0 || n2 + n1 -1 >= sv.Length)
                            throw new DBException("22000");
                        return new TChar(sv.Substring(n1, n2));
                    }
                case Sqlx.SUM:
                    {
                        if (fc == null)
                            break;
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.Null: return TNull.Value;
                            case Sqlx.NULL: return TNull.Value;
                            case Sqlx.REAL: return new TReal(fc.sum1);
                            case Sqlx.INTEGER:
                                if (fc.sumInteger is not null)
                                    return new TInteger(fc.sumInteger);
                                else
                                    return new TInt(fc.sumLong);
                            case Sqlx.NUMERIC: 
                                if (fc.sumDecimal is null)
                                    break;
                                return new TNumeric(fc.sumDecimal);
                        }
                        return TNull.Value;
                    }
                case Sqlx.TRANSLATE:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1981");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        return v; // TBD
                    }
                case Sqlx.TRIM:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1982");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string sv = v.ToString();
                        char c = '\0';
                        if (op1 != -1L)
                        {
                            string s = cx.obs[op1]?.Eval(cx)?.ToString()??"";
                            if (s != null && s.Length > 0)
                                c = s[0];
                        }
                        if (c != 0)
                            return mod switch
                            {
                                Sqlx.LEADING => new TChar(sv.TrimStart((char)c)),
                                Sqlx.TRAILING => new TChar(sv.TrimEnd((char)c)),
                                Sqlx.BOTH => new TChar(sv.Trim((char)c)),
                                _ => new TChar(sv.Trim((char)c)),
                            };
                        else
                            return mod switch
                            {
                                Sqlx.LEADING => new TChar(sv.TrimStart()),
                                Sqlx.TRAILING => new TChar(sv.TrimEnd()),
                                Sqlx.BOTH => new TChar(sv.Trim()),
                                _ => new TChar(sv.Trim()),
                            };
                    }
                case Sqlx.UPPER:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1983");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TChar(v.ToString().ToUpper());
                    }
                case Sqlx.USER: return new TChar(cx.db.user?.name ?? "");
                case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1984");
                        var vcx = new Context(cx);
                        if (vl !=null)
                        {
                            vcx.result = vl.defpos;
                            return new TRvv("");
                        }
                        vcx.result = from;
                        var p = -1L;
                        for (var b=cx.cursors[from]?.Rec()?.First();b!=null;b=b.Next())
                        {
                            var t = b.value();
                            if (t.ppos > p)
                                p = t.ppos;
                        }
                        if (p!=-1L)
                            return new TInt(p);
                        return TNull.Value;
                    } 
                case Sqlx.WHEN: // searched case
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1985");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        TypedValue a = cx.obs[op1]?.Eval(cx)??TNull.Value;
                        if (a == TBool.True)
                            return v;
                        return cx.obs[op2]?.Eval(cx)??TNull.Value;
                    }
                case Sqlx.XMLAGG:
                    if (fc == null || fc.sb == null)
                        break;
                    return new TChar(fc.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1986");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        var a = cx.obs[op2]?.Eval(cx);
                        var x = cx.obs[val]?.Eval(cx);
                        if (a == null || x == null)
                            return dv;
                        string n = XmlConvert.EncodeName(cx.obs[op1]?.Eval(cx)?.ToString()??"");
                        string r = "<" + n  + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                 case Sqlx.XMLPI:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1987");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TChar("<?" + v + " " + cx.obs[op1]?.Eval(cx) ?? "" + "?>");
                    }
         /*       case Sqlx.XMLQUERY:
                    {
                        string doc = op1.Eval(tr,rs).ToString();
                        string pathexp = op2.Eval(tr,rs).ToString();
                        StringReader srdr = new StringReader(doc);
                        XPathDocument xpd = new XPathDocument(srdr);
                        XPathNavigator xn = xpd.CreateNavigator();
                        return new TChar((string)XmlFromXPI(xn.Evaluate(pathexp)));
                    } */
                case Sqlx.MONTH:
                case Sqlx.DAY:
                case Sqlx.HOUR:
                case Sqlx.MINUTE:
                case Sqlx.SECOND:
                case Sqlx.YEAR:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1988");
                        v = vl?.Eval(cx)??TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TInt(Extract(kind, v));
                    }
            }
            throw new DBException("42154", kind).Mix();
        }
        /// <summary>
        /// Xml encoding
        /// </summary>
        /// <param name="a">an object to encode</param>
        /// <returns>an encoded string</returns>
        static string? XmlEnc(object a)
        {
            return a.ToString()?.Replace("&", "&amp;").Replace("<", "&lt;")
                .Replace(">", "&gt;").Replace("\r", "&#x0d;");
        }
        static long Extract(Sqlx mod,TypedValue v)
        {
            switch (v.dataType.kind)
            {
                case Sqlx.DATE:
                    {
                        DateTime dt = ((TDateTime)v).value;
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
                        Interval it = ((TInterval)v).value;
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
        /*        /// <summary>
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
                /// <summary>
                /// Xml encoding
                /// </summary>
                /// <param name="a">an object to encode</param>
                /// <returns>an encoded string</returns>
                string XmlEnc(object a)
                {
                    return a.ToString().Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\r", "&#x0d;");
                } */
        /// <summary>
        /// for aggregates and window functions we need to implement StartCounter
        /// </summary>
        internal Register StartCounter(Context cx,TRow key)
        {
            var oc = cx.values;
            var fc = new Register(cx, key, this) { acc1 = 0.0, mset = null };
            switch (kind)
            {
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.AVG:
                    fc.count = 0L;
                    fc.sumType = Domain.Content;
                    break;
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                    {
                        if (cx.obs[val] is not SqlValue vl || cx._Dom(vl) is not Domain dm)
                            throw new PEException("PE48185");
                        fc.mset = new TMultiset(dm);
                        break;
                    }
                case Sqlx.XMLAGG:
                    if (window>=0)
                        goto case Sqlx.COLLECT;
                    fc.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window>=0)
                        goto case Sqlx.COLLECT;
                    fc.bval = false;
                    break;
                case Sqlx.ARRAY:
                    fc.acc = new TArray((Domain)cx.Add(
                        new Domain(cx.GetUid(),Sqlx.ARRAY, Domain.Content)));
                    break;
                case Sqlx.COUNT:
                    fc.count = 0L;
                    break;
                case Sqlx.FIRST:
                    fc.acc = null; // NOT TNull.Value !
                    break;
                case Sqlx.LAST:
                    fc.acc = TNull.Value;
                    break;
                case Sqlx.MAX:
                case Sqlx.MIN:
                    if (window>=0L)
                        goto case Sqlx.COLLECT;
                    fc.sumType = Domain.Content;
                    fc.acc = null;
                    break;
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
                    fc.acc1 = 0.0;
                    fc.sum1 = 0.0;
                    fc.count = 0L;
                    break; 
                case Sqlx.SUM:
                    fc.sumType = Domain.Content;
                    fc.sumLong = 0L;
                    fc.sumInteger = null;
                    break;
            }
            cx.values = oc;
            return fc;
        }
        internal void AddIn(TRow key,Context cx)
        {
            // not all window functions use val
            var fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx,key);
            if (mod == Sqlx.DISTINCT)
            {
                if (cx.obs[val] is not SqlValue vl || cx._Dom(vl) is not Domain dm)
                    throw new PEException("PE48187");
                var v = vl.Eval(cx)??TNull.Value;
                if (v != null)
                {
                    if (fc.mset == null)
                        fc.mset = new TMultiset((Domain)
                            cx.Add(new Domain(cx.GetUid(),Sqlx.MULTISET,dm)));
                    else if (fc.mset.Contains(v))
                        return;
                    fc.mset.Add(v);
                }
            }
            switch (kind)
            {
                case Sqlx.AVG:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48188");
                        var v = vl.Eval(cx);
                        if (v == null)
                            break;
                    }
                    fc.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ANY:
                    {
                        if (window>=0)
                            goto case Sqlx.COLLECT;
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48189");
                        var v = vl.Eval(cx);
                        if (v is TBool tb)
                                fc.bval = fc.bval || tb.value;
                        break;
                    }
                case Sqlx.COLLECT:
                    {
                        if (cx.obs[val] is not SqlValue vl || cx._Dom(vl) is not Domain dm)
                            throw new PEException("PE48190");
                        fc.mset ??= new TMultiset(dm);
                        var v = vl.Eval(cx);
                        if (v != TNull.Value)
                            fc.mset.Add(v);
                        break;
                    }
                case Sqlx.COUNT:
                    {
                        if (mod == Sqlx.TIMES)
                        {
                            fc.count++;
                            break;
                        }
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48191");
                        var v = vl.Eval(cx);
                        if (v != TNull.Value)
                            fc.count++;
                    }
                    break;
                case Sqlx.EVERY:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48192");
                        var v = vl.Eval(cx);
                        if (v is TBool vb)
                            fc.bval = fc.bval && vb.value;
                        break;
                    }
                case Sqlx.FIRST:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48193");
                        if (vl != null && fc.acc == null)
                            fc.acc = vl.Eval(cx);
                        break;
                    }
                case Sqlx.FUSION:
                    {
                        if (cx.obs[val] is not SqlValue vl || cx._Dom(val) is not Domain dm)
                            throw new PEException("PE48194");
                        if (fc.mset == null)
                        {
 
                            var vv = vl.Eval(cx);
                            if (vv != TNull.Value && dm.elType is Domain de)
                                fc.mset = new TMultiset(de); 
                        }
                        else
                        {
                            var v = vl.Eval(cx);
                            fc.mset = TMultiset.Union(fc.mset, v as TMultiset, true);
                        }
                        break;
                    }
                case Sqlx.INTERSECTION:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48195");
                        var v = vl.Eval(cx);
                        var mv = v as TMultiset;
                        if (fc.mset == null)
                            fc.mset = mv;
                        else
                            fc.mset = TMultiset.Intersect(fc.mset, mv, true);
                        break;
                    }
                case Sqlx.LAST:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48196");
                        fc.acc = vl.Eval(cx);
                        break;
                    }
                case Sqlx.MAX:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48197");
                        TypedValue v = vl.Eval(cx);
                        if (v != TNull.Value && (fc.acc == null || fc.acc.CompareTo(v) < 0))
                            fc.acc = v;
                        break;
                    }
                case Sqlx.MIN:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48198");
                        TypedValue v = vl.Eval(cx);
                        if (v != TNull.Value && (fc.acc == null || fc.acc.CompareTo(v) > 0))
                            fc.acc = v;
                        break;
                    }
                case Sqlx.STDDEV_POP: 
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48199");
                        var o = vl.Eval(cx);
                        var v = o.ToDouble();
                        fc.sum1 -= v;
                        fc.acc1 -= v * v;
                        fc.count--;
                        break;
                    }
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SUM:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48200");
                        var v = vl.Eval(cx);
                        if (v == null || v==TNull.Value)
                            return;
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInteger iv)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumInteger = iv.ivalue;
                                }
                                else if (v is TInt vc)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumLong = vc.value;
                                }
                                else if (v is TReal rv)
                                {
                                    fc.sumType = Domain.Real;
                                    fc.sum1 = rv.dvalue;
                                }
                                else if (v is TNumeric nv)
                                {
                                    fc.sumType = Domain.Numeric;
                                    fc.sumDecimal = nv.value;
                                }
                                else
                                    throw new DBException("22000", kind);
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInteger vn)
                                {
                                    Integer a = vn.ivalue;
                                    if (fc.sumInteger is null)
                                        fc.sumInteger = new Integer(fc.sumLong) + a;
                                    else
                                        fc.sumInteger += a;
                                }
                                else if (v is TInt vi)
                                {
                                    var a = vi.value;
                                    if (fc.sumInteger is null)
                                    {
                                        if ((a > 0) ? (fc.sumLong <= long.MaxValue - a) : (fc.sumLong >= long.MinValue - a))
                                            fc.sumLong += a;
                                        else
                                            fc.sumInteger = new Integer(fc.sumLong) + new Integer(a);
                                    }
                                    else
                                        fc.sumInteger += new Integer(a);
                                }
                                break;
                            case Sqlx.REAL:
                                { 
                                    if (v is TReal rv)
                                    fc.sum1 += rv.dvalue;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                {
                                    if (v is TNumeric nv)
                                    {
                                        if (fc.sumDecimal is not Numeric sn)
                                            sn = Numeric.Zero;
                                        fc.sumDecimal = sn + nv.value;
                                    }
                                }
                                break;
                        }
                        break;
                    }
                case Sqlx.XMLAGG:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48201");
                        fc.sb?.Append(' ');
                        var o = vl.Eval(cx);
                        if (o != TNull.Value)
                            fc.sb?.Append(o.ToString());
                        break;
                    }
            }
            if (window<0)
            {
                var t1 = cx.funcs[from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                t2 += (defpos, fc);
                t1 += (key, t2);
                cx.funcs += (from, t1);
            }
        }
        /// <summary>
        /// Window Functions: bmk is a bookmark in cur.wrs
        /// </summary>
        /// <param name="bmk"></param>
        /// <returns></returns>
        bool InWindow(Context cx, RTreeBookmark bmk, Register fc)
        {
            if (bmk == null || cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.units == Sqlx.RANGE && !(TestStartRange(cx,bmk,fc) && TestEndRange(cx,bmk,fc)))
                return false;
            if (ws.units == Sqlx.ROWS && !(TestStartRows(cx,bmk,fc) && TestEndRows(cx,bmk,fc)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Context cx, RTreeBookmark bmk,Register fc)
        {
            if (cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.high == null || ws.high.unbounded)
                return true;
            long limit;
            if (fc.wrb == null)
                return false;
            if (ws.high.current)
                limit = fc.wrb._pos;
            else if (ws.high.preceding)
                limit = fc.wrb._pos - (ws.high.distance?.ToLong()??0);
            else
                limit = fc.wrb._pos + (ws.high.distance?.ToLong()??0);
            return bmk._pos <= limit; 
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Context cx, RTreeBookmark bmk,Register fc)
        {
            if (cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.low == null || ws.low.unbounded)
                return true;
            long limit;
            if (fc.wrb == null)
                return false;
            if (ws.low.current)
                limit =fc.wrb._pos;
            else if (ws.low.preceding)
                limit = fc.wrb._pos - (ws.low.distance?.ToLong() ?? 0);
            else
                limit = fc.wrb._pos + (ws.low.distance?.ToLong() ?? 0);
            return bmk._pos >= limit;
        }

        /// <summary>
        /// Test the window against the end of the given range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRange(Context cx, RTreeBookmark bmk, Register fc)
        {
            if (cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.high == null || ws.high.unbounded)
                return true;
            var n = val;
            if (cx._Dom(val) is not Domain kt || fc.wrb == null)
                return false;
            var wrv = fc.wrb[n];
            TypedValue limit;
            if (cx.db is not Transaction)
                return false;
            if (ws.high.current)
                limit = wrv;
            else if (ws.high.preceding)
                limit = kt.Eval(defpos,cx,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, 
                    ws.high.distance);
            else
                limit = kt.Eval(defpos,cx,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, 
                    ws.high.distance);
            return kt.Compare(bmk[n], limit) <= 0; 
        }
        /// <summary>
        /// Test a window against the start of a range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRange(Context cx, RTreeBookmark bmk, Register fc)
        {
            if (cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.low == null || ws.low.unbounded)
                return true;
            var n = val;
            var kt = cx._Dom(val) ?? Domain.Content;
            if (fc.wrb == null)
                return false;
            var tv = fc.wrb?[n] ?? TNull.Value;
            TypedValue limit;
            if (cx.db is not Transaction)
                return false;
            if (ws.low.current)
                limit = tv;
            else if (ws.low.preceding)
                limit = kt.Eval(defpos, cx, tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS,
                    ws.low.distance);
            else
                limit = kt.Eval(defpos, cx, tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS,
                    ws.low.distance);
            return kt.Compare(bmk[n], limit) >= 0; // OrderedKey comparison
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (aggregates(kind) && r!=-1L && from != r)
                qn += (defpos, true);
            else
            {
                qn = ((SqlValue?)cx.obs[val])?.Needs(cx, r, qn) ?? qn;
                qn = ((SqlValue?)cx.obs[op1])?.Needs(cx, r, qn) ?? qn;
                qn = ((SqlValue?)cx.obs[op2])?.Needs(cx, r, qn) ?? qn;
            }
            qn = ((WindowSpecification?)cx.obs[window])?.Needs(cx, r, qn) ?? qn;
            return qn;
        }
   
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (aggregates(kind) && from != rs)
                return r;
            if (cx.obs[val] is SqlValue v)
                r += v.Needs(cx, rs);
            if (cx.obs[op1] is SqlValue o1)
                r += o1.Needs(cx, rs);
            if (cx.obs[op2] is SqlValue o2)
                r += o2.Needs(cx, rs);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is SqlValue v)
                r += v.Needs(cx);
            if (cx.obs[op1] is SqlValue o1)
                r += o1.Needs(cx);
            if (cx.obs[op2] is SqlValue o2)
                r += o2.Needs(cx);
            return r;
        }
        // tailor REST call to remote DBMS
        internal override string ToString(string sg, Remotes rf, BList<long?> cs,
            CTree<long, string> ns, Context cx)
        {
            if (kind==Sqlx.COUNT && mod==Sqlx.TIMES)
                return "COUNT(*)";
            switch (sg)
            {
                case "Pyrrho":
                    {
                        var sb = new StringBuilder();
                        string vl,o1,o2;
                        if (!aggregates(kind)) // ||((RowSet)cx.obs[from]).Built(cx))
                        {
                            vl = (cx.obs[val] is SqlValue sv)?sv.ToString(sg, rf, cs, ns, cx):"";
                            o1 = (cx.obs[op1] is SqlValue s1)?s1.ToString(sg, rf, cs, ns, cx):"";
                            o2 = (cx.obs[op2] is SqlValue s2)?s2.ToString(sg, rf, cs, ns, cx):"";
                        } else
                        {
                            vl = ((cx.obs[val] is SqlCopy vc)? ns[vc.copyFrom] : ns[val]) ?? "";
                            o1 = ((cx.obs[op1] is SqlCopy c1)? ns[c1.copyFrom] : ns[op1]) ?? "";
                            o2 = ((cx.obs[op2] is SqlCopy c2)? ns[c2.copyFrom] : ns[op2])  ?? "";
                        }
                        switch (kind)
                        {
                            case Sqlx.COUNT:
                                if (mod != Sqlx.TIMES)
                                    goto case Sqlx.ABS;
                                sb.Append("COUNT(*)");
                                break;
                            case Sqlx.ABS: 
                            case Sqlx.ANY: 
                            case Sqlx.AVG:
                            case Sqlx.CARDINALITY:
                            case Sqlx.CEIL: 
                            case Sqlx.CEILING: 
                            case Sqlx.CHAR_LENGTH: 
                            case Sqlx.CHARACTER_LENGTH:
                            case Sqlx.EVERY:
                            case Sqlx.EXP: 
                            case Sqlx.FLOOR: 
                            case Sqlx.LN:
                            case Sqlx.LOWER:
                            case Sqlx.MAX:
                            case Sqlx.MIN:
                            case Sqlx.NORMALIZE:
                            case Sqlx.NULLIF:
                            case Sqlx.OCTET_LENGTH:
                            case Sqlx.SET:
                            case Sqlx.SUM:
                            case Sqlx.TRANSLATE:
                            case Sqlx.UPPER:
                            case Sqlx.XMLAGG:
                                sb.Append(kind);sb.Append('(');
                                if (mod == Sqlx.DISTINCT)
                                    sb.Append("DISTINCT ");
                                sb.Append(vl); sb.Append(')'); break;
                            case Sqlx.ARRAY: 
                            case Sqlx.CURRENT: 
                            case Sqlx.ELEMENT:
                            case Sqlx.FIRST:
                            case Sqlx.LAST: 
                            case Sqlx.SECURITY: 
                            case Sqlx.NEXT:
                            case Sqlx.OVERLAY:
                            case Sqlx.PARTITION:
                            case Sqlx.RANK:
                            case Sqlx.ROW_NUMBER:
                            case Sqlx.STDDEV_POP:
                            case Sqlx.STDDEV_SAMP:
                            case Sqlx.TYPE_URI:
                            case Sqlx.TRIM:
                            case Sqlx.WHEN:
                            case Sqlx.XMLCOMMENT:
                            case Sqlx.XMLPI:
                            case Sqlx.XMLQUERY:
                                throw new DBException("42000", ToString());
                            case Sqlx.CAST:
                            case Sqlx.XMLCAST:
                                if (cx._Dom(this) is not Domain dm)
                                    throw new DBException("42000");
                                sb.Append(kind); sb.Append('(');
                                sb.Append(vl); sb.Append(" as ");
                                sb.Append(dm.name); sb.Append(')');
                                break;
                            case Sqlx.COLLECT:
                            case Sqlx.FUSION: 
                            case Sqlx.INTERSECTION: 
                                sb.Append(kind); sb.Append('(');
                                if (mod!=Sqlx.NO)
                                {
                                    sb.Append(mod); sb.Append(' ');
                                }
                                sb.Append(vl); sb.Append(')');
                                break;
                            case Sqlx.CHECK:
                            case Sqlx.CURRENT_DATE:
                            case Sqlx.CURRENT_TIME: 
                            case Sqlx.CURRENT_TIMESTAMP: 
                            case Sqlx.USER:
                            case Sqlx.LOCALTIME:
                            case Sqlx.LOCALTIMESTAMP:
                            case Sqlx.VERSIONING:
                                sb.Append(kind); break;
                            case Sqlx.EXTRACT:
                                sb.Append(kind); sb.Append('(');
                                sb.Append(mod); sb.Append(" from ");
                                sb.Append(vl); sb.Append(')');
                                break;
                            case Sqlx.MOD:
                            case Sqlx.POWER:
                                sb.Append(kind); sb.Append('(');
                                sb.Append(o1); sb.Append(',');
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Sqlx.POSITION:
                                sb.Append(kind); sb.Append('(');
                                sb.Append(o1); sb.Append(" in ");
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Sqlx.SUBSTRING:
                                sb.Append(kind); sb.Append('(');
                                sb.Append(vl); sb.Append(" from ");
                                sb.Append(o1);
                                if (o2 != null)
                                { sb.Append(" for "); sb.Append(o2); }
                                sb.Append(')');
                                break;
                        }
                        var an = alias??"";
                        if (an!="")
                        { sb.Append(" as "); sb.Append(an); }    
                        return sb.ToString();
                    }
            }
            return base.ToString(sg, rf, cs, ns,cx);
        }
        public override string ToString()
        {
            switch (kind)
            {
                case Sqlx.PARTITION:
                case Sqlx.VERSIONING:
                case Sqlx.CHECK: return kind.ToString();
                case Sqlx.POSITION: if (op1 !=-1L) goto case Sqlx.PARTITION; 
                    break;
            }
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');
            sb.Append(kind);
            sb.Append('(');
            if (val != -1L)
                sb.Append(Uid(val));
            if (op1!=-1L)
            {
                sb.Append(':');sb.Append(Uid(op1));
            }
            if (op2 != -1L)
            {
                sb.Append(':'); sb.Append(Uid(op2));
            }
            if (mod != Sqlx.NO && mod != Sqlx.TIMES)
            {
                sb.Append(' '); sb.Append(mod);
            }
            sb.Append(')');
            if (alias!=null)
            {
                sb.Append(" as ");  sb.Append(alias);
            }
            if (filter!=CTree<long, bool>.Empty)
            {
                sb.Append(" filter=(");
                var cm = "";
                for (var b=filter.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
            }
            if (window >= 0L)
            { sb.Append(" Window "); sb.Append(Uid(window)); }
            return sb.ToString();
        }
        internal static bool aggregates(Sqlx kind)
        {
            return kind switch
            {
                Sqlx.ANY or Sqlx.ARRAY or Sqlx.AVG or Sqlx.COLLECT or Sqlx.COUNT or Sqlx.EVERY or Sqlx.FIRST or Sqlx.FUSION or Sqlx.INTERSECTION or Sqlx.LAST or Sqlx.MAX or Sqlx.MIN or Sqlx.STDDEV_POP or Sqlx.STDDEV_SAMP or Sqlx.SOME or Sqlx.SUM or Sqlx.XMLAGG => true,
                _ => false,
            };
        }
    }
    /// <summary>
    /// The Parser converts this n-adic function to a binary one
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long dp, Context cx, SqlValue op1, SqlValue op2)
            : base(dp, cx, Sqlx.COALESCE, null, op1, op2, Sqlx.NO) { }
        protected SqlCoalesce(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator+(SqlCoalesce s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlCoalesce(s.defpos, s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[op1] is SqlValue o1 && o1.Eval(cx) is TypedValue v1 && v1!=TNull.Value) ? 
                v1 : (cx.obs[op2] is SqlValue o2)? o2.Eval(cx) : TNull.Value;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            if (c.obs[op1] is not SqlValue le || c.obs[op2] is not SqlValue rg)
                throw new PEException("PE48188");
            var nl = le.Having(c, dm);
            var nr = rg.Having(c, dm);
            return (le == nl && rg == nr) ? this :
                (SqlValue)c.Add(new SqlCoalesce(c.GetUid(), c, nl, nr));
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is not SqlCoalesce sc)
                return false;
            if (c.obs[op1] is not SqlValue le || c.obs[op2] is not SqlValue rg ||
                c.obs[sc.op1] is not SqlValue vl || c.obs[sc.op2] is not SqlValue vr)
                throw new PEException("PE48189");
            return le.Match(c, vl) && rg.Match(c, vr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("coalesce (");
            sb.Append(op1);
            sb.Append(',');
            sb.Append(op2);
            sb.Append(')');
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlTypeUri : SqlFunction
    {
        internal SqlTypeUri(long dp, Context cx, SqlValue op1)
            : base(dp, cx, Sqlx.TYPE_URI, null, op1, null, Sqlx.NO) { }
        protected SqlTypeUri(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeUri operator+(SqlTypeUri s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SqlTypeUri(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeUri(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlTypeUri(dp,m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE49200");
            TypedValue v = TNull.Value;
            if (cx.obs[op1] is DBObject ob)
                v = ob.Eval(cx);
            if (v == TNull.Value)
                return dm.defaultValue;
            var st = v.dataType;
            if (st.iri != null)
                return v;
            return dm.defaultValue;
        }
        public override string ToString()
        {
            return "TYPE_URI(..)";
        }
    }
    /// <summary>
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class SqlStar : SqlValue
    {
        public readonly long prefix = -1L; // SqlValue
        internal SqlStar(long dp, long pf) : base(new Ident("*",new Iix(dp)), Domain.Content)
        {
            prefix = pf;
        }
        protected SqlStar(long dp, long pf, BTree<long, object> m) : base(dp, m)
        {
            prefix = pf;
        }
        protected SqlStar(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlStar(defpos, prefix, m);
        }
        public static SqlStar operator +(SqlStar s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return (SqlStar)s.New(s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return new TInt(-1L);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlStar(dp, m);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var vs = BList<DBObject>.Empty;
            var fm = cx._Ob((long)(m[RowSet._Source] ?? -1L));
            if ((cx.obs[prefix] ?? fm) is not RowSet rq || cx._Dom(rq) is not Domain rd)
                throw new PEException("PE49201");
            var dr = rd.display;
            if (dr == 0)
                dr = rd.rowType.Length;
            for (var c = rd.rowType.First(); c != null && c.key() < dr; c = c.Next())
                if (c.value() is long p && cx.obs[p] is SqlValue v)
                    vs += (SqlValue)cx.Add(v);
            return (vs, m);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            return CTree<long, bool>.Empty;
        }
    }
    /// <summary>
    /// Quantified Predicate subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class QuantifiedPredicate : SqlValue
    {
        internal const long // these constants are used in other classes too
            All = -348, // bool
            Between = -349, // bool
            Found = -350, // bool
            High = -351, // long SqlValue
            Low = -352, // long SqlValue
            Op = -353, // Sqlx
            _Select = -354, //long RowSet
            Vals = -355, //BList<long?> SqlValue
            What = -356, //long SqlValue
            Where = -357; // long SqlValue
        public long what => (long)(mem[What]??-1L);
        /// <summary>
        /// The comparison operator: LSS etc
        /// </summary>
        public Sqlx op => (Sqlx)(mem[Op]??Sqlx.NO);
        /// <summary>
        /// whether ALL has been specified
        /// </summary>
        public bool all => (bool)(mem[All]??false);
        /// <summary>
        /// The query specification to test against
        /// </summary>
        public long select => (long)(mem[_Select]??-1L);
        /// <summary>
        /// A new Quantified Predicate built by the parser (or by Copy, Invert here)
        /// </summary>
        /// <param name="w">The test expression</param>
        /// <param name="sv">the comparison operator, or AT</param>
        /// <param name="a">whether ALL has been specified</param>
        /// <param name="s">the rowset to test against</param>
        internal QuantifiedPredicate(long dp,Context cx,SqlValue w, Sqlx o, bool a, 
            RowSet s)
            : base(dp,_Mem(cx,w,s) + (What,w.defpos)+(Op,o)+(All,a)+(_Select,s.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(s.defpos,true))
                  +(_Depth,1+_Max(w.depth,s.depth))) {}
        protected QuantifiedPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,RowSet s)
        {
            if (cx._Dom(s) is not Domain ds)
                throw new PEException("PE23100");
            var m = BTree<long, object>.Empty;
            var ag = w.IsAggregation(cx) + ds.aggs;
            var dm = Domain.Bool;
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static QuantifiedPredicate operator+(QuantifiedPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new QuantifiedPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuantifiedPredicate(defpos, m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is DBObject w) r += w._Rdc(cx);
            if (cx.obs[select] is DBObject s) r += s._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new QuantifiedPredicate(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nw = cx.Fix(what);
            if (nw != what)
                r += (What, nw);
            var ns = cx.Fix(select);
            if (ns != select)
                r += (_Select, ns);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var wh = cx.ObReplace(what,so,sv);
            if (wh != what)
                r += (What, wh);
            var se = cx.ObReplace(select, so, sv);
            if (se != select)
                r += (_Select, se);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue?)cx.obs[what])?.Grouped(cx, gs) != false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (QuantifiedPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue?)cx.obs[r.what])?.AddFrom(cx, q)??SqlNull.Value;
            if (a.defpos != r.what)
                r += (What, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert this search condition e.g. NOT (a LSS SOME b) is (a GEQ ALL b)
        /// </summary>
        /// <param name="j">the part part</param>
        /// <returns>the new search condition</returns>
        internal override SqlValue Invert(Context cx)
        {
            var w = (SqlValue?)cx.obs[what]??SqlNull.Value;
            var s = (RowSet?)cx.obs[select]??throw new PEException("PE1904");
            return op switch
            {
                Sqlx.EQL => new QuantifiedPredicate(defpos, cx, w, Sqlx.NEQ, !all, s),
                Sqlx.NEQ => new QuantifiedPredicate(defpos, cx, w, Sqlx.EQL, !all, s),
                Sqlx.LEQ => new QuantifiedPredicate(defpos, cx, w, Sqlx.GTR, !all, s),
                Sqlx.LSS => new QuantifiedPredicate(defpos, cx, w, Sqlx.GEQ, !all, s),
                Sqlx.GEQ => new QuantifiedPredicate(defpos, cx, w, Sqlx.LSS, !all, s),
                Sqlx.GTR => new QuantifiedPredicate(defpos, cx, w, Sqlx.LEQ, !all, s),
                _ => throw new PEException("PE65"),
            };
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is SqlValue w)
                r += w.Needs(cx, rs);
            if (cx.obs[select] is SqlValue s)
                r += s.Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[what] is not SqlValue wv || cx.obs[select] is not RowSet s)
                throw new PEException("PE43305");
            for (var rb = s.First(cx); rb != null; rb = rb.Next(cx))
            {
                var col = rb[0];
                if (wv.Eval(cx) is TypedValue w)
                {
                    if (OpCompare(op, col.dataType.Compare(w, col)) && !all)
                        return TBool.True;
                    else if (all)
                        return TBool.False;
                }
                else
                    return TNull.Value;
            }
            return TBool.For(all);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[what] is SqlValue w)
                tg = w.StartCounter(cx, rs, tg);
            if (cx.obs[select] is RowSet s)
                tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[what] is SqlValue w)
                tg = w.AddIn(cx, rb, tg);
            if (cx.obs[select] is RowSet s)
                tg = s.AddIn(cx, rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference. If the select needs something
        /// From will add it to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[what] is SqlValue w)
                qn = w.Needs(cx, r, qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[what])?.KnownBy(cx, cs)==true;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[what])?.KnownBy(cx, q)==true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE43324");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[what] is SqlValue w)
            {
                r += w.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(what);
            }
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var d = depth;
            var rs = (RowSet?)cx.obs[select] ?? throw new PEException("PE1900");
            if (cx.obs[what] is SqlValue w)
            {
                (ls, m) = w.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=w.defpos)
                {
                    r = new QuantifiedPredicate(defpos, cx, nv, op, all, rs);
                    r += (_Depth, Math.Max(d, nv.depth + 1));
                    m = cx.Replace(this, r, m);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (op != Sqlx.NO)
            {
                sb.Append(' ');sb.Append(op); sb.Append(' ');
            }
            if (all)
                sb.Append(" all ");
            sb.Append(what);
            sb.Append(" filter (");
            sb.Append(select);
            sb.Append(')');
            return sb.ToString();
        } 
    }
    /// <summary>
    /// BetweenPredicate subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class BetweenPredicate : SqlValue
    {
        public long what =>(long)(mem[QuantifiedPredicate.What]??-1L);
        /// <summary>
        /// BETWEEN or NOT BETWEEN
        /// </summary>
        public bool between => (bool)(mem[QuantifiedPredicate.Between]??false);
        /// <summary>
        /// The low end of the range of values specified
        /// </summary>
        public long low => (long)(mem[QuantifiedPredicate.Low]??-1L);
        /// <summary>
        /// The high end of the range of values specified
        /// </summary>
        public long high => (long)(mem[QuantifiedPredicate.High]??-1L);
        /// <summary>
        /// A new BetweenPredicate from the parser
        /// </summary>
        /// <param name="w">the test expression</param>
        /// <param name="b">between or not between</param>
        /// <param name="a">The low end of the range</param>
        /// <param name="sv">the high end of the range</param>
        internal BetweenPredicate(long dp,Context cx,SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(dp,_Mem(cx,w,a,h)
                  +(QuantifiedPredicate.What,w.defpos)+(QuantifiedPredicate.Between,b)
                  +(QuantifiedPredicate.Low,a.defpos)+(QuantifiedPredicate.High,h.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(a.defpos,true)+(h.defpos,true))
                  +(_Depth,1+_Max(w.depth,a.depth,h.depth)))
        { }
        protected BetweenPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(Context cx,SqlValue w,SqlValue a,SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = w.IsAggregation(cx);
            if (a != null)
                ag += a.IsAggregation(cx);
            if (b != null)
                ag += b.IsAggregation(cx);
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static BetweenPredicate operator+(BetweenPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new BetweenPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BetweenPredicate(defpos,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is SqlValue wh)
                r += wh.IsAggregation(cx);
            if (cx.obs[low] is SqlValue lw)
                r += lw.IsAggregation(cx); 
            if (cx.obs[high] is SqlValue hi)
                r += hi.IsAggregation(cx);
            return r;
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
                cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43336");
            var nw = wh.Having(cx,dm);
            var nl = lo.Having(cx,dm);;
            var nh = hi.Having(cx,dm);
            return (wh == nw && lo == nl && hi == nh) ? this :
                (SqlValue)cx.Add(new BetweenPredicate(cx.GetUid(), cx, nw, between, nl, nh));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is BetweenPredicate that)
            {
                if (between != that.between)
                    return false;
                if (cx.obs[what] is SqlValue w)
                    if (cx.obs[that.what] is SqlValue tw && !w.Match(cx,tw))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[low] is SqlValue lw)
                    if (cx.obs[that.low] is SqlValue tl && !lw.Match(cx, tl))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[high] is SqlValue hg)
                    if (cx.obs[that.high] is SqlValue th && !hg.Match(cx,th))
                        return false;
                    else if (that.what >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new BetweenPredicate(dp,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (cx.obs[what] is SqlValue w) r += w._Rdc(cx);
            if (cx.obs[low] is SqlValue lo) r += lo._Rdc(cx);
            if (cx.obs[high] is SqlValue hi) r += hi._Rdc(cx);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue?)cx.obs[what])?.Grouped(cx, gs) != false &&
            ((SqlValue?)cx.obs[low])?.Grouped(cx, gs) != false &&
            ((SqlValue?)cx.obs[high])?.Grouped(cx, gs) != false;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nw = cx.Fix(what);
            if (what!=nw)
                r += (QuantifiedPredicate.What, cx.Fix(what));
            var nl = cx.Fix(low);
            if (low!=nl)
                r += (QuantifiedPredicate.Low, nl);
            var nh = cx.Fix(high);
            if (high !=nh)
                r += (QuantifiedPredicate.High, nh);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r += (QuantifiedPredicate.What, wh);
            var lw = cx.ObReplace(low, so, sv);
            if (lw != low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = cx.ObReplace(high, so, sv);
            if (hg != high)
                r += (QuantifiedPredicate.High, hg);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return (((SqlValue?)cx.obs[low])?.KnownBy(cx, q, ambient)??true)
                && (((SqlValue?)cx.obs[high])?.KnownBy(cx, q, ambient)??true)
                && (((SqlValue?)cx.obs[what])?.KnownBy(cx, q, ambient)??true);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return (((SqlValue?)cx.obs[low])?.KnownBy(cx, cs, ambient) ?? true)
                && (((SqlValue?)cx.obs[high])?.KnownBy(cx, cs, ambient) ?? true)
                && (((SqlValue?)cx.obs[what])?.KnownBy(cx, cs, ambient)??true);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE43335");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[low] is SqlValue lo)
            {
                r += lo.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(low);
            }
            if (cx.obs[high] is SqlValue hi)
            {
                r += hi.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(high);
            } 
            if (cx.obs[what] is SqlValue w)
            {
                r += w.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(what);
            } 
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from < 0)
                return this;
            var r = (BetweenPredicate)base.AddFrom(cx, q);
            if (cx.obs[r.what] is SqlValue wo)
            {
                var a = wo.AddFrom(cx, q);
                if (a.defpos != r.what)
                    r += (QuantifiedPredicate.What, a.defpos);
            }
            if (cx.obs[r.low] is SqlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.low)
                    r += (QuantifiedPredicate.Low, a.defpos);
            }
            if (cx.obs[r.high] is SqlValue ho)
            {
                var a = ho.AddFrom(cx, q);
                if (a.defpos != r.high)
                    r += (QuantifiedPredicate.High, a.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the between predicate (for part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
                    cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43337");
            return new BetweenPredicate(defpos, cx, wh, !between,lo, hi);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
        cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43338");
            tg = wh.StartCounter(cx,rs,tg);
            tg = lo.StartCounter(cx,rs,tg);
            tg = hi.StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43339");
            tg = wh.AddIn(cx,rb, tg);
            tg = lo.AddIn(cx,rb, tg);
            tg = hi.AddIn(cx,rb, tg);
            return tg;
        }
        internal override void OnRow(Context cx,Cursor bmk)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43340");
            wh.OnRow(cx,bmk);
            lo.OnRow(cx,bmk);
            hi.OnRow(cx,bmk);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = cx.obs[what]?.Needs(cx, rs)??CTree<long,bool>.Empty;
            r += cx.obs[low]?.Needs(cx, rs) ?? CTree<long, bool>.Empty;
            r += cx.obs[high]?.Needs(cx, rs) ?? CTree<long, bool>.Empty;
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43341");
            return wh.LocallyConstant(cx, rs)
                && lo.LocallyConstant(cx,rs)
                && hi.LocallyConstant(cx, rs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
                    cx.obs[high] is not SqlValue hi || cx._Dom(wh) is not Domain t)
                throw new PEException("PE43342");
            if (wh.Eval(cx) is TypedValue w && w!=TNull.Value)
            {
                if (lo.Eval(cx) is TypedValue lw && lw!=TNull.Value)
                {
                    if (t.Compare(w, t.Coerce(cx,lw)) < 0)
                        return TBool.False;
                    if (hi.Eval(cx) is TypedValue hg && hg!=TNull.Value)
                        return TBool.For(t.Compare(w, t.Coerce(cx,hg)) <= 0);
                }
            }
            return TNull.Value;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[what] is SqlValue w)
                qn = w.Needs(cx, r, qn);
            if (cx.obs[low] is SqlValue lo)
                qn = lo.Needs(cx, r, qn);
            if (cx.obs[high] is SqlValue hi)
                qn = hi.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = false;
            var d = depth;
            SqlValue w = (SqlValue?)cx.obs[what] ?? SqlNull.Value,
                l = (SqlValue?)cx.obs[low] ?? SqlNull.Value,
                h = (SqlValue?)cx.obs[high] ?? SqlNull.Value;
            BList<DBObject> ls;
            if (w != SqlNull.Value)
            {
                (ls, m) = w.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=w.defpos)
                {
                    ch = true; w = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (l != SqlNull.Value)
            {
                (ls, m) = l.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != l.defpos)
                {
                    ch = true; l = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (h != SqlNull.Value)
            {
                (ls, m) = h.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != h.defpos)
                {
                    ch = true; h = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }if (ch)
            {
                r = new BetweenPredicate(defpos, cx, w, between, l, h);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(what);
            sb.Append(" between ");
            sb.Append(low);
            sb.Append(" and ");
            sb.Append(high);
            return sb.ToString();
        }
    }

    /// <summary>
    /// LikePredicate subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class LikePredicate : SqlValue
    {
        internal const long
            Escape = -358, // long SqlValue
            _Like = -359; // bool
        /// <summary>
        /// like or not like
        /// </summary>
        public bool like => (bool)(mem[_Like]??false);
        /// <summary>
        /// The escape character
        /// </summary>
        public long escape => (long)(mem[Escape]??-1L);
        /// <summary>
        /// A like predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="k">like or not like</param>
        /// <param name="b">the right operand</param>
        /// <param name="e">the escape character</param>
        internal LikePredicate(long dp,Context cx,SqlValue a, bool k, SqlValue b, SqlValue? e)
            : base(dp, _Mem(cx,a,b,e) + (Left,a.defpos)+(_Like,k)+(Right,b.defpos)
                  +(_Domain,Domain.Bool.defpos)
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true))
                  +(_Depth,1+_Max(a.depth,b.depth,e?.depth??0)))
        { }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b,SqlValue? e)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = a.IsAggregation(cx) + b.IsAggregation(cx);
            if (e != null)
            {
                m = m + (Escape, e.defpos) + (e.defpos, true);
                ag += e.IsAggregation(cx);
            }
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static LikePredicate operator+(LikePredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new LikePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LikePredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new LikePredicate(dp,m);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var le = (SqlValue)(cx.obs[left] ?? throw new PEException("PE43320"));
            var nl = le.Having(cx, dm);
            var rg = (SqlValue)(cx.obs[right] ?? throw new PEException("PE43321"));
            var nr = rg.Having(cx, dm);
            var es = (SqlValue?)cx.obs[escape];
            var ne = es?.Having(cx, dm);
            if (le == nl && rg == nr && es == ne)
                return this;
            return (SqlValue)cx.Add(new LikePredicate(cx.GetUid(), cx, nl, like, nr, ne));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is LikePredicate that)
            {
                if (like != that.like)
                    return false;
                if (cx.obs[left] is SqlValue le && cx.obs[that.left] is SqlValue tl)
                    if (!le.Match(cx, tl))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is SqlValue rg && cx.obs[that.right] is SqlValue tr)
                    if (!rg.Match(cx, tr))
                        return false;
                    else if (that.right >= 0)
                        return false;
                if (cx.obs[escape] is SqlValue es && cx.obs[that.escape] is SqlValue te)
                    if (!es.Match(cx, te))
                        return false;
                    else if (that.escape >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = base._Rdc(cx);
            if (cx.obs[escape] is SqlValue es) r += es._Rdc(cx);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.Fix(escape);
            if (escape!=ne)
                r += (Escape, ne);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var wh = cx.ObReplace(left, so, sv);
            if (wh != left)
                r += (Left, wh);
            var rg = cx.ObReplace(right, so, sv);
            if (rg != right)
                r += (Right, rg);
            var esc = cx.ObReplace(escape, so, sv);
            if (esc != escape)
                r += (Escape, esc);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue?)cx.obs[escape])?.Grouped(cx, gs) != false &&
            ((SqlValue?)cx.obs[left])?.Grouped(cx, gs) != false &&
            ((SqlValue?)cx.obs[right])?.Grouped(cx, gs)!=false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from>0)
                return this;
            var r = (LikePredicate)base.AddFrom(cx, q);
            if (cx.obs[r.escape] is SqlValue e)
            {
                var a = e.AddFrom(cx, q);
                if (a.defpos != r.escape)
                    r += (Escape, a.defpos);
            }
            if (cx.obs[r.left] is SqlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.left)
                    r += (Left, a.defpos);
            }
            if (cx.obs[r.right] is SqlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (Right, a.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[left])?.KnownBy(cx, q, ambient) != false
                && ((SqlValue?)cx.obs[right])?.KnownBy(cx, q, ambient) !=false
                && ((SqlValue?)cx.obs[escape])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[left])?.KnownBy(cx, cs, ambient) != false
                && ((SqlValue?)cx.obs[right])?.KnownBy(cx, cs, ambient) != false
                && ((SqlValue?)cx.obs[escape])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE43303");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[left] is SqlValue le)
            {
                r += le.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(left);
            }
            if (cx.obs[right] is SqlValue ri)
            {
                r += ri.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(right);
            }
            if (cx.obs[escape] is SqlValue es)
            {
                r += es.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(escape);
            } 
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            if (cx.obs[left] is not SqlValue a || cx.obs[right] is not SqlValue b)
                throw new PEException("PE49301");
            return new LikePredicate(defpos,cx,a, !like,b, (SqlValue?)cx.obs[escape]);
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
                return Like(a[1..], b[(j + 1)..], e); 
            if (j == 0 && b[0] == '%')
             {
                int m = b.IndexOf('%', 1);
                if (m < 0)
                    m = b.Length;
                for (j = 0; j <= a.Length - m + 1; j++)
                    if (Like(a[j..], b[1..], e))
                        return true;
                return false;
            }
            return a[0] == b[j] && Like(a[1..], b[(j + 1)..], e);
        }
        /// <summary>
        /// Evaluate the LikePredicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            bool r = false;
            if (cx.obs[left] is SqlValue le && le.Eval(cx) is TypedValue lf && 
                cx.obs[right] is SqlValue re && re.Eval(cx) is TypedValue rg)
            {
                if (lf == TNull.Value && rg == TNull.Value)
                    r = true;
                else if (lf != TNull.Value & rg != TNull.Value)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (cx.obs[escape] is SqlValue oe)
                        e = oe.Eval(cx).ToString();
                    if (e.Length != 1)
                        throw new DBException("22020").ISO(); // invalid escape character
                    r = Like(a, b, e[0]);
                }
                if (!like)
                    r = !r;
                return r ? TBool.True : TBool.False;
            }
            return TNull.Value;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, 
            BTree<long, Register> tg)
        {
            tg = cx.obs[left]?.StartCounter(cx, rs, tg)??tg;
            tg = cx.obs[right]?.StartCounter(cx, rs, tg)??tg;
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = cx.obs[left]?.AddIn(cx,rb, tg)??tg;
            tg = cx.obs[right]?.AddIn(cx,rb, tg)??tg;
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue?)cx.obs[left])?.Needs(cx,r,qn)??qn;
            qn = ((SqlValue?)cx.obs[right])?.Needs(cx,r,qn)??qn;
            qn = ((SqlValue?)cx.obs[escape])?.Needs(cx,r,qn) ?? qn;
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = cx.obs[left]?.Needs(cx, rs)??CTree<long,bool>.Empty;
            r += cx.obs[right]?.Needs(cx, rs)??CTree<long,bool>.Empty;
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49500");
            return le.LocallyConstant(cx, rs) && rg.LocallyConstant(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            SqlValue lf = (SqlValue?)cx.obs[left]??SqlNull.Value, 
                rg = (SqlValue?)cx.obs[right]??SqlNull.Value, 
                es = (SqlValue?)cx.obs[escape]??SqlNull.Value;
            var ch = false;
            var d = depth;
            BList<DBObject> ls;
            if (lf != SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (rg != SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (es != SqlNull.Value)
            {
                (ls, m) = es.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != es.defpos)
                {
                    ch = true; es = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new LikePredicate(defpos, cx, lf, like, rg, es);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' '); sb.Append(Uid(defpos));
            sb.Append(": "); sb.Append(Uid(left));
            if (!like)
                sb.Append(" not");
            sb.Append(" like ");
            sb.Append(Uid(right));
            if (escape!=-1L)
            {
                sb.Append(" escape "); sb.Append(Uid(escape));
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The InPredicate subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class InPredicate : SqlValue
    {
        public long what => (long)(mem[QuantifiedPredicate.What]??-1L);
        /// <summary>
        /// In or not in
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// A rowSet should be specified (unless a list of values is supplied instead)
        /// </summary>
        public long select => (long)(mem[QuantifiedPredicate._Select]??-1L); // or
        /// <summary>
        /// A list of values to check (unless a query is supplied instead)
        /// </summary>
        public BList<long?> vals => (BList<long?>)(mem[QuantifiedPredicate.Vals]??BList<long?>.Empty);
        public InPredicate(long dp,Context cx, SqlValue w, BList<SqlValue>? vs = null) 
            : base(dp, _Mem(cx,w,vs))
        {}
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,BList<SqlValue>? vs)
        {
            var m = BTree<long, object>.Empty;
            var cs = BList<long?>.Empty;
            var dm = Domain.Bool;
            var ag = w.IsAggregation(cx);
            m += (QuantifiedPredicate.What, w.defpos);
            if (vs != null)
            {
                for (var b = vs.First(); b != null; b = b.Next())
                {
                    var s = b.value();
                    cs += s.defpos;
                    ag += s.IsAggregation(cx);
                }
                m += (QuantifiedPredicate.Vals, cs);
                m += _Deps(vs + w);
            }
            else
                m += _Deps(1, w);
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE43350");
            return w.IsAggregation(cx);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE43351");
            var nw = w.Having(cx, dm);
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    var nv = v.Having(cx, dm);
                    vs += nv;
                    ch = ch || nv != v;
                }
            return (w == nw && !ch) ? this :
                (SqlValue)cx.Add(new InPredicate(cx.GetUid(), cx, nw, vs));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is InPredicate that)
            {
                if (found != that.found)
                    return false;
                if (cx.obs[what] is not SqlValue w)
                    throw new PEException("PE43353");
                if (cx.obs[that.what] is SqlValue tw && !w.Match(cx, tw))
                    return false;
                else if (that.what >= 0)
                    return false;
                if (vals.Count != that.vals.Count)
                    return false;
                var tb = that.vals.First();
                for (var b = vals.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue e
                        && tb.value() is long tp
                        && cx.obs[tp] is SqlValue te && !e.Match(cx, te))
                        return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is SqlValue w) r += w._Rdc(cx);
            if (cx.obs[select] is RowSet s) r += s._Rdc(cx);
            return r;
        }
        public static InPredicate operator+(InPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new InPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new InPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return (dp == defpos) ? this : new InPredicate(dp,mem);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nw = cx.Fix(what);
            if (what!=nw)
                r += (QuantifiedPredicate.What, nw);
            var ns = cx.Fix(select);
            if (select!=ns)
                r += (QuantifiedPredicate._Select, ns);
            var nv = cx.FixLl(vals);
            if (vals!=nv)
            r += (QuantifiedPredicate.Vals, nv);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r += (QuantifiedPredicate.What, wh);
            var wr = cx.ObReplace(select, so, sv);
            if (wr != select)
                r += (QuantifiedPredicate._Select, wr);
            var vs = vals;
            for (var b = vs.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        vs += (b.key(), v);
                }
            if (vs != vals)
                r += (QuantifiedPredicate.Vals, vs);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue?)cx.obs[what])?.Grouped(cx, gs) != false &&
            gs.Grouped(cx, vals) != false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (InPredicate)base.AddFrom(cx, q);
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE43360");
            var a = w.AddFrom(cx, q);
            if (a.defpos != r.what)
                r += (QuantifiedPredicate.What, a);
            var vs = r.vals;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue u)
                {
                    var v = u.AddFrom(cx, q);
                    if (v.defpos != b.value())
                        ch = true;
                    vs += (b.key(), v.defpos);
                }
            if (ch)
                r += (QuantifiedPredicate.Vals, vs);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return cx.obs[what] is SqlValue w && w.KnownBy(cx, q, ambient);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return cx.obs[what] is SqlValue w && w.KnownBy(cx, cs, ambient);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE43362");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    if (cx.obs[p] is SqlValue v)
                        r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(p);
                }
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is SqlValue w)
                r = w.Needs(cx, rs);
            if (cx.obs[select] is RowSet s)
                r += s.Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[select] is RowSet)
                return false;
            if (cx.obs[what] is SqlValue w && !w.LocallyConstant(cx, rs))
                return false;
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is SqlValue sv &&
                    !sv.LocallyConstant(cx, rs))
                    return false;
            return true;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE49503");
            var tv = w.Eval(cx);
            if (cx.obs[select] is RowSet s)
                for (var rb = s.First(cx); rb != null; rb = rb.Next(cx))
                    if (rb[0].CompareTo(tv) == 0)
                        return TBool.For(found);
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is SqlValue sv &&
                    sv.Eval(cx).CompareTo(tv) == 0)
                    return TBool.For(found);
            return TBool.For(!found);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var v = vals?.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is SqlValue sv)
                    tg = sv.StartCounter(cx, rs, tg);
            if (cx.obs[what] is SqlValue w)
                tg = w.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is SqlValue sv)
                    tg = sv.AddIn(cx, rb, tg);
            if (cx.obs[what] is SqlValue w)
                tg = w.AddIn(cx, rb, tg);
            return tg;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[what] is SqlValue w)
                qn = w.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            var d = depth;
            SqlValue w = (SqlValue?)cx.obs[what] ?? SqlNull.Value,
                s = (SqlValue?)cx.obs[select]??SqlNull.Value;
            if (w != SqlNull.Value)
            {
                (ls, m) = w.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=w.defpos)
                {
                    ch = true; w = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (s!=SqlNull.Value)
            {
                (ls, m) = s.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != s.defpos)
                {
                    ch = true; s = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            for (var b = vals?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                        d = Math.Max(d, nv.depth + 1);
                    }
                    vs += v;
                }
            if (ch)
            {
                r = new InPredicate(defpos, cx, w, vs);
                if (s!=SqlNull.Value)
                    r += (QuantifiedPredicate._Select, s.defpos);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Uid(what));
            if (!found)
                sb.Append(" not");
            sb.Append(" in (");
            if (vals != BList<long?>.Empty)
            {
                var cm = "";
                for (var b = vals.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
            }
            else
                sb.Append(Uid(select));
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// MemberPredicate is a subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class MemberPredicate : SqlValue
    {
        internal const long
            Found = -360, // bool
            Lhs = -361, // long SqlValue
            Rhs = -362; // long SqlValue
        /// <summary>
        /// the test expression
        /// </summary>
        public long lhs => (long)(mem[Lhs]??-1L);
        /// <summary>
        /// found or not found
        /// </summary>
        public bool found => (bool)(mem[Found]??false);
        /// <summary>
        /// the right operand
        /// </summary>
        public long rhs => (long)(mem[Rhs]??-1L);
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal MemberPredicate(long dp,Context cx,SqlValue a, bool f, SqlValue b)
            : base(dp, _Mem(cx,a,b)
                  +(Lhs,a)+(Found,f)+(Rhs,b)+(_Depth,1+_Max(a.depth,b.depth))
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true)))
        { }
        protected MemberPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = a.IsAggregation(cx) + b.IsAggregation(cx);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static MemberPredicate operator+(MemberPredicate s,(long,object)x)
        {
            return new MemberPredicate(s.defpos, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MemberPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new MemberPredicate(dp,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49504");
            return lh.IsAggregation(cx) + rh.IsAggregation(cx);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49505");
            var nl = lh.Having(cx, dm);
            var nr = rh.Having(cx, dm);
            return (lh == nl && rh == nr) ? this :
                (SqlValue)cx.Add(new MemberPredicate(cx.GetUid(), cx, nl, found, nr));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49506");
            if (v is MemberPredicate that)
            {
                if (cx.obs[that.lhs] is not SqlValue tl || cx.obs[that.rhs] is not SqlValue tr)
                    throw new PEException("PE49507");
                if (found != that.found || !lh.Match(cx, tl) || !rh.Match(cx, tr))
                    return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = base._Rdc(cx);
            if (cx.obs[lhs] is SqlValue lh) r += lh._Rdc(cx);
            if (cx.obs[rhs] is SqlValue rh) r += rh._Rdc(cx);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nl = cx.Fix(lhs);
            if (nl != lhs)
                r += (Lhs, nl);
            var nr = cx.Fix(rhs);
            if (nr != rhs)
                r += (Rhs, rhs);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        { 
            var r = base._Replace(cx, so, sv,m);
            var lf = cx.ObReplace(lhs,so,sv);
            if (lf != left)
                r += (Lhs,lf);
            var rg = cx.ObReplace(rhs,so,sv);
            if (rg != rhs)
                r += (Rhs,rg);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49508");
            return lh.Grouped(cx, gs) && rh.Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49509");
            var r = (MemberPredicate)base.AddFrom(cx, q);
            var a = lh.AddFrom(cx, q);
            if (a.defpos != r.lhs)
                r += (Lhs, a.defpos);
            a = rh.AddFrom(cx, q);
            if (a.defpos != r.rhs)
                r += (Rhs, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49510");
            return new MemberPredicate(defpos,cx,lh, !found, rh);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49511");
            return lh.Needs(cx, rs) + rh.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49512");
            return lh.LocallyConstant(cx,rs) && rh.LocallyConstant(cx,rs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[lhs]?.Eval(cx) is TypedValue a && cx.obs[rhs]?.Eval(cx) is TypedValue b)
            {
                if (b == TNull.Value)
                    return (cx._Dom(this)??throw new PEException("PE49210")).defaultValue;
                if (a == TNull.Value)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                if (cx.db!=null)
                    throw cx.db.Exception("42113", b.GetType().Name).Mix();
            }
            return TNull.Value;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[lhs] is SqlValue lh)
                tg = lh.StartCounter(cx, rs, tg);
            if (cx.obs[rhs] is SqlValue rh)
                tg = rh.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[lhs] is SqlValue lh)
                tg = lh.AddIn(cx,rb, tg);
            if (cx.obs[rhs] is SqlValue rh)
                tg = rh.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49513");
            qn = lh.Needs(cx,r,qn);
            qn = rh.Needs(cx,r,qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49514");
            return lh.KnownBy(cx, cs, ambient) 
                && rh.KnownBy(cx, cs, ambient);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49515");
            return lh.KnownBy(cx, q, ambient) 
                && rh.KnownBy(cx, q, ambient);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh || cx._Dom(this) is not Domain dm)
                throw new PEException("PE49516");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            r += lh.KnownFragments(cx, kb, ambient);
            y = y && r.Contains(lhs);
            r += rh.KnownFragments(cx, kb, ambient);
            y = y && r.Contains(rhs);
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            var d = depth;
            SqlValue lh = (SqlValue?)cx.obs[lhs] ?? SqlNull.Value,
                rh = (SqlValue?)cx.obs[rhs] ?? SqlNull.Value;
            if (lh != SqlNull.Value)
            {
                (ls, m) = lh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != lh.defpos)
                {
                    ch = true; lh = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (rh != SqlNull.Value)
            {
                (ls, m) = rh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rh.defpos)
                {
                    ch = true; rh = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new MemberPredicate(defpos, cx, lh, found, rh);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            } 
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(lhs);
            if (!found)
                sb.Append(" not");
            sb.Append(" member of ");
            sb.Append(rhs);
            return sb.ToString();
        }
    }
    /// <summary>
    /// TypePredicate is a subclass of SqlValue
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class TypePredicate : SqlValue
    {
        /// <summary>
        /// the test expression
        /// </summary>
        public long lhs => (long)(mem[MemberPredicate.Lhs]??-1L);
        /// <summary>
        /// OF or NOT OF
        /// </summary>
        public bool found => (bool)(mem[MemberPredicate.Found]??false);
        /// <summary>
        /// the right operand: a list of Domain
        /// </summary>
        public BList<Domain> rhs => 
            (BList<Domain>)(mem[MemberPredicate.Rhs] ?? BList<Domain>.Empty); // naughty: MemberPreciate Rhs is SqlValue
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal TypePredicate(long dp,SqlValue a, bool f, BList<Domain> r)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool.defpos)
                  +(MemberPredicate.Lhs,a.defpos)+(MemberPredicate.Found,f)
                  +(MemberPredicate.Rhs,r))
        {  }
        protected TypePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TypePredicate operator+(TypePredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new TypePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TypePredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new TypePredicate(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nl = cx.Fix(lhs);
            if (nl!=lhs)
            r += (MemberPredicate.Lhs, nl);
            var nr = cx.FixBD(rhs);
            if (nr!=rhs)
            r += (MemberPredicate.Rhs, nr);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var lh = cx.ObReplace(lhs, so, sv);
            if (lh != lhs)
                r += (MemberPredicate.Lhs, lh);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (cx.obs[lhs] is not SqlValue v)
                throw new PEException("PE23205");
            if (from > 0)
                return this;
            var r = (TypePredicate)base.AddFrom(cx, q);
            var a = v.AddFrom(cx, q);
            if (a.defpos != r.lhs)
                r += (MemberPredicate.Lhs, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            if (cx.obs[lhs] is not SqlValue v)
                throw new PEException("PE23204");
            return new TypePredicate(defpos,v, !found, rhs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[lhs] is not SqlValue v)
                throw new PEException("PE23203");
            var a = v.Eval(cx);
            if (a == TNull.Value)
                return TNull.Value;
            bool b = false;
            var at = a.dataType;
            for (var t =rhs.First();t!=null;t=t.Next())
                b = at.EqualOrStrongSubtypeOf(t.value()); // implemented as Equals for ONLY
            return TBool.For(b == found);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            if (cx.obs[lhs] is not SqlValue lh)
                throw new PEException("PE49520");
            return lh.Needs(cx,r,qn);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[lhs] is not SqlValue lh)
                throw new PEException("PE49521");
            return lh.Needs(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var d = depth;
            if (cx.obs[lhs] is SqlValue lh)
            {
                (ls, m) = lh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != lh.defpos)
                {
                    r = new TypePredicate(defpos, lh, found, rhs);
                    r += (_Depth, Math.Max(d, lh.depth + 1));
                    m = cx.Replace(this, r, m);
                }
            }
            return (new BList<DBObject>(r), m);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        internal Sqlx kind => (Sqlx)(mem[Domain.Kind]??Sqlx.NO);
        public PeriodPredicate(long dp,Context cx,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,_Mem(cx,op1,op2)+(Domain.Kind,o)
                 +(Dependents,new CTree<long,bool>(op1.defpos,true)+(op2.defpos,true))
                 +(_Depth,1+_Max(op1.depth,op2.depth)))
        { }
        protected PeriodPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = CTree<long,bool>.Empty;
            if (a != null)
            {
                m += (Left, a.defpos);
                ag += a.IsAggregation(cx);
            }
            if (b != null)
            {
                m += (Right, b.defpos);
                ag += b.IsAggregation(cx);
            }
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static PeriodPredicate operator+(PeriodPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new PeriodPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new PeriodPredicate(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv, m);
            var a = cx.ObReplace(left, so, sv);
            if (a != left)
                r += (Left, a);
            var b = cx.ObReplace(right, so, sv);
            if (b != right)
                r += (Right, b);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49530");
            return le.Grouped(cx, gs) && rg.Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49531");
            var r = (PeriodPredicate)base.AddFrom(cx, q);
            var a = le.AddFrom(cx, q);
            if (a.defpos != r.left)
                r += (Left, a.defpos);
            a = rg.AddFrom(cx, q);
            if (a.defpos != r.right)
                r += (Right, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49532");
            tg = le.StartCounter(cx, rs, tg);
            tg = rg.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49533");
            tg = le.AddIn(cx,rb, tg);
            tg = rg.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49534");
            qn = le.Needs(cx, r, qn);
            qn = rg.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49535");
            return le.Needs(cx, rs) + rg.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue rg)
                throw new PEException("PE49536");
            return le.LocallyConstant(cx, rs) && rg.LocallyConstant(cx, rs);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            if (cx.obs[left] is not SqlValue le || cx.obs[right] is not SqlValue ri)
                throw new PEException("PE42333");
            var nl = le.Having(cx, dm);
            var nr = ri.Having(cx, dm);
            return (le == nl && ri == nr) ? this :
                (SqlValue)cx.Add(new PeriodPredicate(cx.GetUid(), cx, nl, kind, nr));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is PeriodPredicate that)
            {
                if (kind != that.kind)
                    return false;
                if (cx.obs[left] is SqlValue le)
                    if (cx.obs[that.left] is SqlValue tl && !le.Match(cx, tl))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is SqlValue rg)
                    if (cx.obs[that.right] is SqlValue tr && !rg.Match(cx, tr))
                        return false;
                    else if (that.right >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = false;
            var d = depth;
            SqlValue lf = (SqlValue?)cx.obs[left] ?? SqlNull.Value,
                rg = (SqlValue?)cx.obs[right] ?? SqlNull.Value;
            BList<DBObject> ls;
            if (lf != SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (rg != SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                    d = Math.Max(d, nv.depth + 1);
                }
            }
            if (ch)
            {
                r = new PeriodPredicate(defpos, cx, lf, kind, rg);
                r += (_Depth, d);
                m = cx.Replace(this, r, m);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(left);
            sb.Append(' '); sb.Append(kind); sb.Append(' ');
            sb.Append(right);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A base class for RowSetPredicates such as ANY
    /// // shareable as of 26 April 2021
    /// </summary>
    internal abstract class RowSetPredicate : SqlValue
    {
        internal const long
            RSExpr = -363; // long RowSet
        public long expr => (long)(mem[RSExpr]??-1);
        /// <summary>
        /// the base query
        /// </summary>
        public RowSetPredicate(long dp,Context cx,RowSet e,BTree<long,object>?m=null) 
            : base(dp,_Mem(cx,e,m)+(RSExpr,e.defpos)
                  +(Dependents,new CTree<long,bool>(e.defpos,true))+(_Depth,1+e.depth))
        {  }
        protected RowSetPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(Context cx,RowSet e,BTree<long,object>?m)
        {
            m ??= BTree<long, object>.Empty;
            var dm = Domain.Bool;
            if (cx._Dom(e) is not Domain dr)
                throw new PEException("PE23202");
            var ag = dr.aggs;
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)dm.New(cx,dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static RowSetPredicate operator+(RowSetPredicate q,(long,object)x)
        {
            var (dp, ob) = x;
            if (q.mem[dp] == ob)
                return q;
            return (RowSetPredicate)q.New(q.mem + x);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            if (cx.obs[expr] is not SqlValue e)
                throw new PEException("PE49540");
            return e._Rdc(cx);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nq = cx.Fix(expr);
            if (nq != expr)
                r += (RSExpr, nq);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx,DBObject so,DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx,so,sv,m);
            var e = cx.ObReplace(expr,so,sv);
            if (e != expr)
                r += (RSExpr, e);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return false;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[expr] is not SqlValue e)
                throw new PEException("PE49541");
            return e.StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[expr] is not SqlValue e)
                throw new PEException("PE49542");
            return e.AddIn(cx,rb,tg);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.q. where conditions
        /// From will add them to cx.needed.
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[expr] is not SqlValue e)
                throw new PEException("PE49543");
            return e.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
    }
    /// <summary>
    /// the EXISTS predicate
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class ExistsPredicate : RowSetPredicate
    {
        public ExistsPredicate(long dp,Context cx,RowSet e) : base(dp,cx,e,BTree<long, object>.Empty
            +(Dependents,new CTree<long,bool>(e.defpos,true))+(_Depth,1+e.depth)) { }
        protected ExistsPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ExistsPredicate operator+(ExistsPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new ExistsPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExistsPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new ExistsPredicate(dp,m);
        }
        /// <summary>
        /// The predicate is true if the rowSet has at least one element
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[expr] is not RowSet e)
                throw new PEException("PE49546");
            return TBool.For(e.First(cx)!=null);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            if (cx.obs[expr] is not SqlValue e)
                throw new PEException("PE49547");
            return e._Rdc(cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("exists (");
            sb.Append(expr);
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// the unique predicate
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class UniquePredicate : RowSetPredicate
    {
        public UniquePredicate(long dp,Context cx,RowSet e) : base(dp,cx,e) {}
        protected UniquePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UniquePredicate operator +(UniquePredicate s, (long, object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new UniquePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UniquePredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new UniquePredicate(dp,m);
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var ex = (RowSet)cx._Replace(expr, so, sv);
            if (ex.defpos != expr)
                r += (RSExpr, ex.defpos);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var rs = ((RowSet?)cx.obs[expr]);
            if (rs == null)
                return TBool.False;
            if (cx._Dom(rs) is not Domain dr)
                throw new PEException("PE47182");
            RTree a = new (rs.defpos,cx,dr, TreeBehaviour.Disallow, TreeBehaviour.Disallow);
            for (var rb=rs.First(cx);rb!= null;rb=rb.Next(cx))
                if (RTree.Add(ref a, rb, cx.cursors) == TreeBehaviour.Disallow)
                    return TBool.False;
            return TBool.True;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        public override string ToString()
        {
            return "UNIQUE..";
        }
    }
    /// <summary>
    /// the null predicate: test to see if a value is null in this row
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class NullPredicate : SqlValue
    {
        internal const long
            NIsNull = -364, //bool
            NVal = -365; //long SqlValue
        /// <summary>
        /// the value to test
        /// </summary>
        public long val => (long)(mem[NVal]??-1L);
        /// <summary>
        /// IS NULL or IS NOT NULL
        /// </summary>
        public bool isnull => (bool)(mem[NIsNull]??true);
        /// <summary>
        /// Constructor: null predicate
        /// </summary>
        /// <param name="v">the value to test</param>
        /// <param name="b">false for NOT NULL</param>
        internal NullPredicate(long dp,SqlValue v, bool b)
            : base(dp,new BTree<long,object>(_Domain,Domain.Bool.defpos)
                  +(NVal,v.defpos)+(NIsNull,b)+(Dependents,new CTree<long,bool>(v.defpos,true))
                  +(_Depth,1+v.depth))
        { }
        protected NullPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static NullPredicate operator+(NullPredicate s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new NullPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new NullPredicate(defpos,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49550");
            return v.IsAggregation(cx);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new NullPredicate(dp,m);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49550");
            var r = (NullPredicate)base.AddFrom(cx, q);
            var a = v.AddFrom(cx, q);
            if (a.defpos != val)
                r += (NVal, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nv = cx.Fix(val);
            if (nv != val)
                r += (NVal, nv);
            return r;
        }
        protected override BTree<long, object> _Replace(Context cx, DBObject so, DBObject sv, BTree<long, object>m)
        {
            var r = base._Replace(cx, so, sv,m);
            var vl = cx.ObReplace(val,so,sv);
            if (vl != val)
                r += (NVal, vl);
            return r;
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49551");
            var nv = v.Having(cx, dm);
            return (v==nv) ? this :
                (SqlValue)cx.Add(new NullPredicate(cx.GetUid(), nv, isnull));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is NullPredicate that)
            {
                if (isnull != that.isnull)
                    return false;
                if (cx.obs[val] is SqlValue w && cx.obs[that.val] is SqlValue x)
                    if (!w.Match(cx, x))
                        return false;
                    else if (that.val >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue?)cx.obs[val])?.Grouped(cx, gs)??false;
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[val]?.Eval(cx) is TypedValue tv)? TBool.For(tv==TNull.Value) : TNull.Value;
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[val]?.StartCounter(cx, rs, tg)??throw new DBException("22000");
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[val]?.AddIn(cx,rb, tg)??throw new DBException("22000");
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient)??false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, q, ambient)??false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            if (cx._Dom(this) is not Domain dm)
                throw new PEException("PE49559");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (val >= 0 && cx.obs[val] is SqlValue v)
            {
                r += v.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(val);
            }
            if (y)
                return new CTree<long, Domain>(defpos, dm);
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49560");
            return v.Needs(cx,r,qn);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49562");
            return v.Needs(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var d = depth;
            BList<DBObject> ls;
            if (cx.obs[val] is SqlValue s)
            {
                (ls, m) = s.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=s.defpos)
                {
                    r = new NullPredicate(defpos, nv, isnull);
                    r += (_Depth, Math.Max(d, nv.depth + 1));
                    m = cx.Replace(this, r, m);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            return isnull?"is null":"is not null";
        }
    }
}
