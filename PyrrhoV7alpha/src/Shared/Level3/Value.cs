using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Xml;
using Pyrrho.Level5;
using Pyrrho.Level1;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
        internal bool scalar => (bool)(mem[RowSet._Scalar]??(domain.kind!=Sqlx.TABLE && domain.kind!=Sqlx.CONTENT
            && domain.kind!=Sqlx.VALUE));
        public SqlValue(Ident nm,Ident ic,BList<Ident> ch, Context cx,Domain dt,BTree<long,object>?m=null)
            :base(nm.iix.dp, cx.DoDepth((m??BTree<long,object>.Empty) +(ObInfo.Name,nm.ident) 
                 + (_Ident,ic) + (SelectDepth,nm.iix.sd) + (_Domain,dt) + (Chain,ch)))
        {
            for (var b = ch.First(); b != null; b = b.Next())
                if (cx.obs[b.value()?.iix.dp ?? -1L] is ForwardReference fr)
                    cx.obs += (fr.defpos, fr + (ForwardReference.Subs, fr.subs + (defpos, true)));
            cx.Add(this);
        }
        public SqlValue(Ident nm, BList<Ident> ch, Context cx, Domain dt, BTree<long, object>? m = null)
    : base(nm.iix.dp, cx.DoDepth((m ?? BTree<long, object>.Empty) + (ObInfo.Name, nm.ident)
         + (_Ident, nm) + (SelectDepth, nm.iix.sd) + (_Domain, dt) + (Chain, ch)))
        {
            for (var b = ch.First(); b != null; b = b.Next())
                if (cx.obs[b.value()?.iix.dp ?? -1L] is ForwardReference fr)
                    cx.obs += (fr.defpos, fr + (ForwardReference.Subs, fr.subs + (defpos, true)));
            cx.Add(this);
        }
        protected SqlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        protected SqlValue(Context cx,string nm,Domain dt,long cf=-1L)
            :base(cx.GetUid(),cx.DoDepth(_Mem(cf)+(ObInfo.Name,nm)+(_Domain,dt)))
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
        public static SqlValue operator +(SqlValue et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValue)et.New(m + x);
        }
        public static SqlValue operator +(SqlValue et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValue)et.New(m + (dp, ob));
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
        internal virtual void Validate(Context cx)
        {  }
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
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal string? Alias(Context cx)
        {
            return alias ?? name ?? cx.Alias();
        }
        internal static int _Depths(BList<DBObject> os)
        {
            var d = 1;
            for (var b=os.First();b is not null;b=b.Next())
                d = Math.Max(b.value().depth + 1, d);
            return d;
        }
        internal static int _Depths(BList<SqlValue> os)
        {
            var d = 1;
            for (var b = os.First(); b != null; b = b.Next())
                d = Math.Max(b.value().depth + 1, d);
            return d;
        }
        internal static int _Depths(params DBObject?[] ob)
        {
            var d = 1;
            foreach (var o in ob)
                if (o is not null)
                    d = Math.Max(o.depth + 1,d);
            return d;
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
        internal virtual void Define(Context cx,long p,RowSet f,DBObject tc)
        {   }
        internal virtual SqlValue Reify(Context cx)
        {
            for (var b = domain.rowType?.First(); b != null; b = b.Next())
            if (cx.role is not null && b.value() is long p 
                    && domain.representation[p]?.infos[cx.role.defpos] is ObInfo ci 
                    && ci.name == name && name is not null)
                    return new SqlCopy(defpos, cx, name, defpos, p);
            return this;
        }
        internal static string Show(Sqlx op)
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
                Sqlx.NO => "",
                Sqlx.PLUS => "+",
                Sqlx.TIMES => "*",
                Sqlx.AND => " and ",
                _ => op.ToString(), // e.g. LBRACK
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
        internal virtual bool Grouped(Context cx, RowSet r)
        {
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
        /// We are building a rowset, and resolving entries in the select tree.
        /// We return a BList only to help with SqlStar: otherwise the tree always has just one entry.
        /// </summary>
        /// <param name="cx">the Context</param>
        /// <param name="f">the new From's defining position</param>
        /// <param name="m">the From properties</param>
        /// <returns>Top level selectors, and updated From properties</returns>
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            if (domain.kind != Sqlx.CONTENT)
                return (new BList<DBObject>(this), m);
            var ns = (BTree<string, (int,long?)>)(m[ObInfo.Names] ?? BTree<string, (int,long?)>.Empty);
            if (name != null && ns.Contains(name) && cx.obs[ns[name].Item2 ?? -1L] is DBObject ob
             && (ob is SqlValue || ob is SystemTableColumn) && ob.domain.kind != Sqlx.CONTENT)
            {
                var nv = ob.Relocate(defpos);
                if (alias is string a)
                    nv += (_Alias, a);
                cx.undefined -= defpos;
                cx.Replace(ob, nv);
                cx.Add(nv);
                cx.NowTry();
            }
            var r = cx.obs[defpos] ?? throw new PEException("PE20602");
            if (r.dbg != dbg)
            {
                if (m[Table.Indexes] is CTree<Domain, CTree<long, bool>> ixs)
                {
                    var xs = CTree<Domain, CTree<long, bool>>.Empty;
                    for (var b = ixs.First(); b != null; b = b.Next())
                        xs += (b.key().Replaced(cx), cx.ReplacedTlb(b.value()));
                    m += (Table.Indexes, xs);
                }
                if (m[Index.Keys] is Domain d && d.Length!=0)
                    m += (Index.Keys, d.Replaced(cx));
            }
            return (new BList<DBObject>(r), m);
        }
        /// <summary>
        /// Eval is used to deliver the TypedValue for the current Cursor if any
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue _Eval(Context cx)
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
            return v is not null && (defpos==v.defpos ||
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValue)base._Replace(cx, so, sv);
            var ag = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = IsAggregation(cx,CTree<long,bool>.Empty).First(); b != null; b = b.Next())
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
        /// We will look these up in the souurce table ObInfo. So now we create a derived
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
        internal virtual CTree<long,bool> IsAggregation(Context cx,CTree<long,bool> ags)
        {
            for (var b = ags.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlFunction fr && fr.op == Sqlx.RESTRICT
                    && fr.val == defpos)
                    return new CTree<long, bool>(defpos, true);
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
            if (domain.kind==Sqlx.CONTENT)
                return dt;
            var dm = domain;
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
            return v is not null && defpos == v.defpos;
        }
        public virtual int CompareTo(object? obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        internal virtual SqlValue Constrain(Context cx,Domain dt)
        {
            var dm = domain;
            if (scalar && dt.kind == Sqlx.TABLE)
                throw new DBException("22000", Sqlx.MINUS);
            var nd = (dm.Constrain(cx, cx.GetUid(), dt) ??
                throw new DBException("22000", Sqlx.MINUS));
            if (dm != nd)
            {
                cx.Add(nd);
                return (SqlValue)cx.Add(this+(_Domain,nd));
            }
            return this;
        }
        internal SqlValue ConstrainScalar(Context cx)
        {
            var t = this + (RowSet._Scalar, true);
            cx.Add(t);
            return t;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(domain);
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
    internal class SqlReview : SqlValue
    {
        internal Ident? idChain => (Ident?)mem[_Ident];
        public SqlReview(Ident nm, Ident ic, BList<Ident> ch, Context cx, Domain dt, BTree<long, object>? m = null) 
            : base(nm, ic, ch, cx, dt, m)
        {
            cx.undefined += (nm.iix.dp, cx.sD);
        }
        protected SqlReview(long dp,BTree<long,object>m) :base(dp,m)
        {  }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlReview(defpos,m);
        }
        internal override DBObject New(long defpos, BTree<long, object> m)
        {
            return new SqlReview(defpos, m);
        }
        public static SqlReview operator+(SqlReview s,(long,object)x)
        {
            return (SqlReview)s.New(s.mem + x);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            if (idChain is not null && chain is not null)
            {
                var len = idChain.Length;
                var (pa, sub) = cx.Lookup(defpos, idChain, len);
                // pa is the object that was found, or null
                if (pa is SqlValue sv && sv.domain.infos[cx.role.defpos] is ObInfo si && sub is not null
                    && si.names[sub.ident].Item2 is long sp && cx.db.objects[sp] is TableColumn tc1)
                {
                    var co = new SqlCopy(sub.iix.dp, cx, sub.ident, sv.defpos, tc1);
                    var nc = new SqlValueExpr(idChain.iix.dp, cx, Sqlx.DOT, sv, co, Sqlx.NO);
                    cx.Add(co);
                    return (new BList<DBObject>(cx.Add(nc)),m);
                }
                if (pa is SqlValue sv1 && sv1.domain is NodeType && sub is not null)
                {
                    var co = new SqlField(sub.iix.dp, sub.ident, -1, sv1.defpos, Domain.Content, sv1.defpos);
                    return (new BList<DBObject>(cx.Add(co)), m);
                }
            }
            var ns = (BTree<string, (int, long?)>)(m[ObInfo.Names] ?? BTree<string, (int, long?)>.Empty);
            if (name != null && ns.Contains(name) && cx.obs[ns[name].Item2 ?? -1L] is DBObject ob
             && (ob is SqlValue || ob is SystemTableColumn) && ob.domain.kind != Sqlx.CONTENT)
            {
                var nv = ob.Relocate(defpos);
                if (alias is string a)
                    nv += (_Alias, a);
                cx.undefined -= defpos;
                cx.Replace(ob, nv);
                cx.Add(nv);
                cx.NowTry();
            }
            var r = cx.obs[defpos] ?? throw new PEException("PE20602");
            if (r.dbg != dbg)
            {
                if (m[Table.Indexes] is CTree<Domain, CTree<long, bool>> ixs)
                {
                    var xs = CTree<Domain, CTree<long, bool>>.Empty;
                    for (var b = ixs.First(); b != null; b = b.Next())
                        xs += (b.key().Replaced(cx), cx.ReplacedTlb(b.value()));
                    m += (Table.Indexes, xs);
                }
                if (m[Index.Keys] is Domain d && d.Length != 0)
                    m += (Index.Keys, d.Replaced(cx));
            }
            return (new BList<DBObject>(r), m);
        }
        internal override void Define(Context cx, long p, RowSet f, DBObject tc)
        {
            if (name == null ||
                cx._Ob(p) is not SqlValue old)
                return;
            var m = BTree<long, object>.Empty;
            if (alias != null)
                m += (_Alias, alias);
            var nv = (tc is SqlValue sv) ? sv
                : new SqlCopy(defpos, cx, name, f.defpos, tc, m);
            cx.done = ObTree.Empty;
            cx.Replace(this, nv);
            cx.done = ObTree.Empty;
            cx.Replace(old, nv);
        }
        internal override bool Verify(Context cx)
        {
            return true;
        }
    }
    internal class SqlCopy : SqlValue
    {
        internal const long
            CopyFrom = -284; // long
        public long copyFrom => (long)(mem[CopyFrom]??-1L);
        public SqlCopy(long dp, Context cx, string nm, long fp, long cp,
            BTree<long, object>? m = null)
            : this(dp, cx, nm,fp,(DBObject?)(cx.obs[cp]??cx.db.objects[cp]), m)
        {
            cx.undefined -= dp;
            cx.NowTry();
        }
        public SqlCopy(long dp, Context cx, string nm, long fp, DBObject? cf,
           BTree<long, object>? m = null)
            : base(dp, _Mem(fp, cf, m) + (ObInfo.Name,nm)  + (_From,fp))
        {
            cx.Add(this);
            cx.undefined -= dp;
            if (dp == cf?.defpos) // someone has forgotten the from clause
                throw new DBException("42112", nm);
            cx.NowTry();
        }
        static BTree<long,object> _Mem(long fp,DBObject? cf, BTree<long,object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (fp>=0)
                m += (_From, fp);
            if (cf != null)
                m = m + (CopyFrom, cf.defpos) + (_Domain,cf.domain) + (_Depth,cf?.depth??1);
            return m;
        }
        protected SqlCopy(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static SqlCopy operator +(SqlCopy et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCopy)et.New(m + x);
        }
        public static SqlCopy operator +(SqlCopy et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCopy)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCopy(defpos, m);
        }
        internal override bool IsFrom(Context cx, RowSet q, bool ordered = false, Domain? ut = null)
        {
            return q.representation.Contains(defpos)==true;
        }
        internal override void Validate(Context cx)
        {
            if (cx.obs[from] is SqlValue sv)
                sv.Validate(cx);
            else
                cx.obs += (defpos, (SqlCopy)New(mem - _From));
        }
        internal override bool KnownBy(Context cx, RowSet r, bool ambient = false)
        {
            if (r is SystemRowSet && r.representation.Contains(copyFrom))
                return true;
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
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n, DBObject? r)
        {
            if (n?.ident is string s && domain.infos[cx.role.defpos] is ObInfo oi
                && domain is UDType
                && oi.names[s].Item2 is long p && oi.names[s].Item1 is int sq)
            {
                var f = new SqlField(lp, s, sq, defpos, domain.representation[p] ?? Domain.Content, p);
                return (cx.Add(f), n.sub); 
            }
            return base._Lookup(lp, cx, nm, n, r);
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
        internal override TypedValue _Eval(Context cx)
        {
            var dv = domain.defaultValue;
            if (defpos >= Transaction.Executables && defpos < Transaction.HeapStart)
                for (var c = cx; c != null; c = c.next)
                {
                    if (from==-1L || (c is CalledActivation ca && ca.locals.Contains(copyFrom)))
                        return cx.values[copyFrom] ?? dv;
                    if (c is TriggerActivation ta && ta.trigTarget?[defpos] is long cp
                            && ta._trs?.targetTrans[cp] is long fp
                            && cx.values[fp] is TypedValue v)
                        return v; 
                }
            if (cx.obs[copyFrom] is SqlElement)
                return cx.values[copyFrom] ?? dv;
            if (cx.obs[from] is SqlCopy sc && sc._Eval(cx) is TypedValue tv)
            { 
                if (tv is TRow rw)
                    return rw[copyFrom] ?? dv;
                if (tv is TInt ti && sc.domain is NodeType nt && nt.tableRows[ti.value] is TableRow tr)
                    return tr.vals[copyFrom] ?? dv;
            }
            return cx.values[defpos] ?? cx.values[copyFrom] ?? dv;
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
    /// A TYPE value for use in CAST and in graphs
    ///     
    /// </summary>
    internal class SqlTypeExpr : SqlValue
    {
        /// <summary>
        /// constructor: a new Type expression
        /// </summary>
        /// <param name="ty">the type</param>
        internal SqlTypeExpr(long dp,Domain ty) : base(dp,new BTree<long,object>(_Domain,ty))
        {}
        protected SqlTypeExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeExpr operator +(SqlTypeExpr et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTypeExpr)et.New(m + x);
        }
        public static SqlTypeExpr operator +(SqlTypeExpr et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTypeExpr)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeExpr(defpos, m);
        }
        internal override TypedValue _Eval(Context cx)
        {
            return new TTypeSpec(domain);
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
    /// </summary>
    internal class SqlTreatExpr : SqlValue
    {
        internal const long
            _Diff = -468, // BTree<long,object>
            TreatExpr = -313; // long SqlValue
        BTree<long, object> diff => (BTree<long, object>)(mem[_Diff] ?? BTree<long, object>.Empty);
        long val => (long)(mem[TreatExpr]??-1L);
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(long dp,SqlValue v,Domain ty)
            : base(dp,new BTree<long,object>(TreatExpr,v.defpos)
                  +(_Domain,ty)+(_Depth,Math.Max(v.depth,ty.depth)+1))
        { }
        protected SqlTreatExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTreatExpr operator +(SqlTreatExpr et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTreatExpr)et.New(m + x);
        }
        public static SqlTreatExpr operator +(SqlTreatExpr et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTreatExpr)et.New(m + (dp, ob));
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
                r = cx.Add(r, TreatExpr, ne);
            return r;
        }
        internal override DBObject AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlTreatExpr)base.AddFrom(cx, q);
            var a = ((SqlValue?)cx.obs[val])?.AddFrom(cx, q);
            if (a is not null && a.defpos != val)
                r += (cx, TreatExpr, a.defpos);
            return cx.Add(r);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlTreatExpr)base._Replace(cx, so, sv);
            var v = cx.ObReplace(val, so, sv);
            if (v != val)
                r +=(cx,TreatExpr, v);
            if ((so.defpos == val || sv.defpos == val) && cx._Dom(v) is Domain nd)
                r = (SqlTreatExpr)r.New(mem + (_Domain,nd.New(nd.mem + diff)));
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
                    r = new SqlTreatExpr(defpos, nv, domain);
                    cx.Replace(this, r);
                }
            }
            if (domain.kind==Sqlx.CONTENT && defpos>=0 &&
                cx._Dom(val) is Domain dv && dv.kind!=Sqlx.CONTENT)
            {
                var nd = (Domain)domain.New(dv.mem + (domain.mem - Domain.Kind));
                cx.Add(nd);
                if (nd != domain)
                {
                    r += (_Domain,nd);
                    cx.Add(r);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            return ((SqlValue?)cx.obs[val])?.IsAggregation(cx,ags)??CTree<long,bool>.Empty;
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
        internal override TypedValue _Eval(Context cx)
        {
            var tv = cx.obs[val]?.Eval(cx) ?? TNull.Value;
            if (!domain.HasValue(cx, tv))
                throw new DBException("2200G", ToString() ?? "??",
                    cx.obs[val]?.ToString() ?? "??").ISO();
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
            Cases = -475,       // BList<(long,long)> SqlValue SqlValue 
            CaseElse = -228;    // long SqlValue
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public BList<(long, long)> cases =>
            (BList<(long,long)>?)mem[Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[CaseElse] ?? -1L);
        internal SqlCaseSimple(long dp, Context cx, Domain dm, SqlValue vl, BList<(long, long)> cs, long el)
            : base(dp, _Mem(cx, dm, vl, cs, el))
        {
            cx.Add(this);
        }
        protected SqlCaseSimple(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, Domain dm, SqlValue vl, BList<(long, long)> cs, long el)
        {
            var r = cx.DoDepth(new BTree<long,object>(_Domain,dm) + (SqlFunction._Val, vl.defpos) 
                + (Cases, cs) + (CaseElse, el));
            var ds = new CTree<long, bool>(vl.defpos,true);
            var d = vl.depth + 1;
            if (dm.defpos >= 0)
            {
                ds += (dm.defpos, true);
                d = Math.Max(dm.depth, d);
            }
            if (cx.obs[el] is SqlValue e)
            {
                ds += (el, true);
                d = Math.Max(e.depth + 1,d);
            }
            d = cx._DepthBPVV(cs, d);
            return r + (_Depth,d);
        }
        public static SqlCaseSimple operator +(SqlCaseSimple et, (Context ,long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            switch (dp)
            {
                case Cases:
                    {
                        var cs = (BList<(long, long)>)ob;
                        m += (_Depth,cx._DepthBPVV(cs, et.depth));
                        break;
                    }
                default:
                    {
                        if (et.mem[dp] == ob)
                            return et;
                        if (ob is DBObject bb && dp != _Depth)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }
            return (SqlCaseSimple)et.New(m + (dp,ob));
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
        internal override TypedValue _Eval(Context cx)
        {
            var oc = cx.values;
            var v = cx.obs[val]?.Eval(cx)??TNull.Value;
            for (var b = cases.First();b is not null;b=b.Next())
            {
                var (w, r) = b.value();
                if (domain.Compare(v, cx.obs[w]?.Eval(cx)??TNull.Value) == 0)
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
            SqlValue v = (SqlValue?)cx.obs[val] ?? SqlNull.Value,
                ce = (SqlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long,long)>.Empty;
            if (v!=SqlNull.Value)
            {
                (ls, m) = v.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=v.defpos)
                {
                    ch = true; v = nv;
                }
            }
            if (ce!=SqlNull.Value)
            {
                (ls, m) = ce.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
                }
            }
            for (var b=cases.First();b is not null;b=b.Next())
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
                    }
                }
                if (sc!=SqlNull.Value)
                {
                    (ls, m) = sc.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; sc = nv;
                    }
                }
                css += (sc.defpos, sv.defpos);
            }
            if (ch)
            {
                r = new SqlCaseSimple(defpos, cx, domain, v, css, ce.defpos);
                cx.Replace(this, r);
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
                r = cx.Add(r, SqlFunction._Val, nv);
            var nc = cx.FixLll(cases);
            if (nc != cases)
                r = cx.Add(r, Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r = cx.Add(r, CaseElse, ne);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCaseSimple)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(val, so, sv);
            if (vl != val)
                r +=(cx, SqlFunction._Val, vl);
            var ch = false;
            var nc = BList<(long, long)>.Empty;
            for (var b=cases.First();b is not null;b=b.Next())
            {
                var (w, x) = b.value();
                var nw = cx.ObReplace(w, so, sv);
                var nx= cx.ObReplace(x, so, sv);
                nc += (nw, nx);
                ch = ch || (nw != w) || (nx != x);
            }
            if (ch)
                r +=(cx, Cases, nc);
            var ne = cx.ObReplace(caseElse, so, sv);
            if (ne != caseElse)
                r +=(cx, CaseElse, ne);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(Uid(val));
            var cm = "{";
            for (var b=cases.First();b is not null;b=b.Next())
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
        internal SqlCaseSearch(long dp, Context cx, Domain dm, BList<(long, long)> cs, long el)
            : base(dp, cx.DoDepth(new BTree<long, object>(_Domain, dm)
                  + (SqlCaseSimple.Cases, cs) + (SqlCaseSimple.CaseElse, el)))
        {
            cx.Add(this);
        }
        protected SqlCaseSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCaseSearch operator +(SqlCaseSearch et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCaseSearch)et.New(m + x);
        }
        public static SqlCaseSearch operator +(SqlCaseSearch et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            switch (dp)
            {
                case SqlCaseSimple.Cases:
                    {
                        var cs = (BList<(long, long)>)ob;
                        m += (_Depth, cx._DepthBPVV(cs, et.depth));
                        break;
                    }
                default:
                    {
                        if (et.mem[dp] == ob)
                            return et;
                        if (ob is DBObject bb && dp != _Depth)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }
            return (SqlCaseSearch)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
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
                r = cx.Add(r, SqlCaseSimple.Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r = cx.Add(r, SqlCaseSimple.CaseElse, ne);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCaseSearch)base._Replace(cx, so, sv);
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
                r +=(cx,SqlCaseSimple.Cases, nc);
            var ne = cx.ObReplace(caseElse, so, sv);
            if (ne != caseElse)
                r +=(cx, SqlCaseSimple.CaseElse, ne);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject>? ls;
            var r = this;
            var ch = false;
            SqlValue ce = (SqlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long, long)>.Empty;
            if (ce != SqlNull.Value)
            {
                (ls, m) = ce.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
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
                    }
                }
                if (sc != SqlNull.Value)
                {
                    (ls, m) = sc.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != sc.defpos)
                    {
                        ch = true; sc = nv;
                    }
                }
                css += (sc.defpos, sv.defpos);
            }
            if (ch)
            {
                r = new SqlCaseSearch(defpos, cx, domain, css, ce.defpos);
                cx.Replace(this, r);
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
        public long seq => (int)(mem[TableColumn.Seq] ?? -1);
        internal override long target => (long)(mem[RowSet.Target]??defpos);
        internal SqlField(long dp, string nm, int sq, long pa, Domain dt, long tg)
            : base(dp, BTree<long,object>.Empty + (ObInfo.Name, nm) + (TableColumn.Seq,sq)
                  + (_From, pa)  + (_Domain,dt) + (RowSet.Target,tg))
        { }
        protected SqlField(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlField operator +(SqlField et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlField)et.New(m + x);
        }
        public static SqlField operator +(SqlField et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlField)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
        {
            var tv = cx.values[from];
            if (tv is TRow tr) return tr.values[target]??TNull.Value;
            if (tv is TNode tn && tn.dataType.infos[cx.role.defpos] is ObInfo ni
                && ni.names[name??"?"].Item2 is long dp)
                return tn.tableRow.vals[dp] ?? TNull.Value;
            return TNull.Value;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target="); sb.Append(Uid(target));
            return sb.ToString();
        }
    }
    internal class SqlElement : SqlValue
    {
        internal SqlElement(Ident nm,BList<Ident> ch,Context cx,Ident pn,Domain dt) 
            : base(nm,ch,cx,dt,BTree<long, object>.Empty+(_From,pn.iix.dp)+(_Depth,dt.depth+1))
        {
            cx.Add(this);
        }
        protected SqlElement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlElement operator +(SqlElement et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlElement)et.New(m + x);
        }
        public static SqlElement operator +(SqlElement et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlElement)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
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
    /// 
    /// </summary>
    internal class SqlValueExpr : SqlValue
    {
        internal const long
            Modifier = -316,// Sqlx
            Op = -300;  // Sqlx
        /// <summary>
        /// the modifier (e.g. DISTINCT)
        /// </summary>
        public Sqlx mod => (Sqlx)(mem[Modifier] ?? Sqlx.NO);
        internal Sqlx op => (Sqlx)(mem[Op] ?? Sqlx.NO);
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
            : base(dp, _Mem(cx, op, m, lf, rg, mm))
        {
            cx.Add(this);
            lf?.ConstrainScalar(cx);
            rg?.ConstrainScalar(cx);
        }
        protected SqlValueExpr(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Context cx, Sqlx op, Sqlx mod,
    SqlValue? left, SqlValue? right, BTree<long, object>? mm = null)
        {
            mm ??= BTree<long, object>.Empty;
            mm += (Op, op);
            mm += (Modifier, mod);
            var ag = CTree<long, bool>.Empty;
            var d = 1;
            if (left != null)
            {
                mm += (Left, left.defpos);
                ag += left.IsAggregation(cx,CTree<long,bool>.Empty);
                d = Math.Max(left.depth + 1, d);
            }
            if (right != null)
            {
                mm += (Right, right.defpos);
                ag += right.IsAggregation(cx,CTree<long,bool>.Empty);
                d = Math.Max(right.depth + 1, d);
            }
            var dl = left?.domain??Domain.Content;
            var dr = right?.domain??Domain.Content;
            var dm = Domain.Content;
            var nm = (string?)mm?[ObInfo.Name] ?? "";
            switch (op)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT:
                    dm = dr;
                    nm = left?.name ?? ""; break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON:   dm = Domain.Bool; break;  // SPECIFICTYPE
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
                case Sqlx.DOT:
                    dm = dr;
                    if (left?.name != null && left.name != "" && right?.name != null && right.name != "")
                        nm = left.name + "." + right.name;
                    break;
                case Sqlx.ELEMENTID: dm = Domain.Int; break;
                case Sqlx.EQL: dm = Domain.Bool; break;
                case Sqlx.EXCEPT: dm = dl; break;
                case Sqlx.GTR: dm = Domain.Bool; break;
                case Sqlx.ID: dm = Domain.Int; break;
                case Sqlx.INTERSECT: dm = dl; break;
                case Sqlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Sqlx.LSS: dm = Domain.Bool; break;
                case Sqlx.MINUS:
                    if (left != null)
                    {
                        if (dl.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME)
                        {
                            if (dl.kind == dr.kind)
                                dm = Domain.Interval;
                            else if (dr.kind == Sqlx.INTERVAL)
                                dm = dl;
                        }
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME))
                            dm = dr;
                        else if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = left.domain;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
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
                case Sqlx.NO: dm = left?.domain ?? Domain.Content; break;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        if ((dl.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME) && dr.kind == Sqlx.INTERVAL)
                            dm = dl;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME))
                            dm = dr;
                        else if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                    dm = Domain.Content; break;
                case Sqlx.RBRACK:
                    {
                        if (left == null)
                            throw new PEException("PE5001");
                        dm = (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, left.domain)); break;
                    }
                case Sqlx.SET: dm = left?.domain ?? Domain.Content; nm = left?.name ?? ""; break; // JavaScript
                case Sqlx.TIMES:
                    {
                        if (dl.kind == Sqlx.NUMERIC || dr.kind == Sqlx.NUMERIC)
                            dm = Domain._Numeric;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.INTEGER || dr.kind == Sqlx.NUMERIC))
                            dm = dl;
                        else if (dr.kind == Sqlx.INTERVAL && (dl.kind == Sqlx.INTEGER || dl.kind == Sqlx.NUMERIC))
                            dm = dr;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.TYPE: dm = Domain.TypeSpec; break;
                case Sqlx.UNION: dm = dl; nm = left?.name ?? ""; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            dm ??= Domain.Content;
            /*          if (dl == Sqlx.UNION && dr != Sqlx.UNION && NumericOp(op) && dr != null &&
                          left != null && left.Constrain(cx, dr) is SqlValue nl && left.defpos != nl.defpos)
                          cx.Replace(left, nl);
                      if (dr == Sqlx.UNION && dl != Sqlx.UNION && NumericOp(op) && dl != null &&
                          right != null && right.Constrain(cx, dl) is SqlValue nr && right.defpos != nr.defpos)
                          cx.Replace(right, nr);*/
            mm ??= BTree<long, object>.Empty;
            mm += (_From, left?.from ?? right?.from ?? -1L);
            if (ag != CTree<long, bool>.Empty && dm != Domain.Content)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                mm += (Domain.Aggs, ag);
            }
            return cx.DoDepth(mm + (_Domain,dm) + (ObInfo.Name, nm));
        }

        public static SqlValueExpr operator +(SqlValueExpr et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueExpr)et.New(m + x);
        }
        public static SqlValueExpr operator +(SqlValueExpr et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueExpr)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlValueExpr(dp, m);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = domain == Domain.Content || domain.kind==Sqlx.UNION;
            BList<DBObject> ls;
            SqlValue lf = (SqlValue?)cx.obs[left] ?? SqlNull.Value,
                rg = (SqlValue?)cx.obs[right] ?? SqlNull.Value,
                su = (SqlValue?)cx.obs[sub] ?? SqlNull.Value;
            if (lf != SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                }
            }
            if (su != SqlNull.Value)
            {
                (ls, m) = su.Resolve(cx, f, m);
                if (ls?[0] is SqlValue nv && nv.defpos != su.defpos)
                {
                    ch = true; su = nv;
                }
            }
            if (ch)
            {
                r = new SqlValueExpr(defpos, cx, op, lf, rg, mod, mem);
                if (su != SqlNull.Value)
                    r += (cx, Sub, su);
                r += (_From, f);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueExpr)base._Replace(cx, so, sv);
            var ch = false;
            var lf = cx.ObReplace(left, so, sv);
            if (lf != left)
            {
                ch = true;
                r += (cx, Left, lf);
            }
            var rg = cx.ObReplace(right, so, sv);
            if (rg != right)
            {
                ch = true;
                r += (cx, Right, rg);
            }
            if (ch && (domain.kind == Sqlx.UNION || domain.kind == Sqlx.CONTENT)
                && cx.obs[lf] is SqlValue lv && cx.obs[rg] is SqlValue rv
                && so.domain != sv.domain)
            {
                if (_Mem(cx, domain.kind, mod, lv, rv)[_Domain] is not Domain nd)
                    throw new PEException("PE29001");
                cx.Add(nd);
                r += (_Domain, nd);
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
                    r += (cx, Left, a.defpos);
            }
            if (cx.obs[r.right] is SqlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (cx, Right, a.defpos);
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
                r = new SqlValueExpr(c.GetUid(), c, op, nl, nr, mod);
                if (nu != null)
                    r += (c, Sub, nu);
                return (SqlValue)c.Add(r);
            }
            return r;
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (!base.Match(cx, v)) return false;
            if (v is SqlValueExpr ve)
            {
                if (op != ve.op || mod != ve.mod)
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
        internal override void Validate(Context cx)
        {
            ((SqlValue?)cx.obs[left])?.Validate(cx);
            ((SqlValue?)cx.obs[right])?.Validate(cx);
            ((SqlValue?)cx.obs[sub])?.Validate(cx);
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
            return (op == Sqlx.AND && cx.obs[right] is SqlValue rg) ?
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
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is SqlValue sl)
                r += sl.Operands(cx);
            if (cx.obs[right] is SqlValue sr)
                r += sr.Operands(cx);
            return r;
        }
        internal override CTree<long, bool> ExposedOperands(Context cx,CTree<long,bool> ag,Domain? gc)
        {
            var r = CTree<long, bool>.Empty;
            if (gc?.representation.Contains(defpos)==true)
                return r;
            if (cx.obs[left] is SqlValue sl)
                r += sl.ExposedOperands(cx,ag,gc);
            if (cx.obs[right] is SqlValue sr)
                r += sr.ExposedOperands(cx,ag,gc);
            return r;
        }
        /// <summary>
        /// Examine a binary expression and work out the resulting type.
        /// The main complication here is handling things like x+1
        /// (e.g. confusion between NUMERIC and INTEGER)
        /// </summary>
        /// <param name="dt">Target union type</param>
        /// <returns>Actual type</returns>
        internal override Domain FindType(Context cx, Domain dt)
        {
            var le = cx.obs[left];
            var rg = cx.obs[right];
            var dl = le?.domain ?? Domain.Content;
            var dr = rg?.domain ?? Domain.Content;
            var dm = Domain.Content;
            switch (op)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT:
                    dm = dr;
                    break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON:
                    dm = dl;
                    break;// JavaScript
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
                        else if (le is SqlValue sl)
                            dm = sl.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.DOT:
                    dm = dr;
                    break;
                case Sqlx.ELEMENTID: dm = Domain.Int; break; // tableRow.defpos
                case Sqlx.EQL: dm = Domain.Bool; break;
                case Sqlx.EXCEPT: dm = dl; break;
                case Sqlx.GTR: dm = Domain.Bool; break;
                case Sqlx.ID: dm = Domain.Int; break;
                case Sqlx.INTERSECT: dm = dl; break;
                case Sqlx.LABELS: dm = Domain.SetType; break;
                case Sqlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Sqlx.LSS: dm = Domain.Bool; break;
                case Sqlx.MINUS:
                    if (le is not null)
                    {
                        if (dl.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME)
                        {
                            if (dl.kind == dr.kind)
                                dm = Domain.Interval;
                            else if (dr.kind == Sqlx.INTERVAL)
                                dm = dl;
                        }
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME))
                            dm = dr;
                        else if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
                        else if (le is SqlValue sm)
                            dm = sm.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                    if (rg is SqlValue sr)
                        dm = sr.FindType(cx, Domain.UnionDateNumeric);
                    break;
                case Sqlx.NEQ: dm = Domain.Bool; break;
                case Sqlx.LEQ: dm = Domain.Bool; break;
                case Sqlx.GEQ: dm = Domain.Bool; break;
                case Sqlx.NO: dm = dl; break;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        if ((dl.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME) && dr.kind == Sqlx.INTERVAL)
                            dm = dl;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.DATE || dl.kind == Sqlx.TIMESTAMP || dl.kind == Sqlx.TIME))
                            dm = dr;
                        else if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
                        else if (le is SqlValue ll)
                            dm = ll.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                    dm = Domain.Content; break;
                case Sqlx.RBRACK:
                    {
                        if (le is not SqlValue lf)
                            throw new PEException("PE5001");
                        var tl = lf.FindType(cx, dt);
                        dm = (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, tl.elType??Domain.Content)); break;
                    }
                case Sqlx.SET: dm = dl ?? Domain.Content; break; // JavaScript
                case Sqlx.SHORTESTPATH: dm = Domain.Array; break; // Neo4j
                case Sqlx.TIMES:
                    {
                        if (dl.kind == Sqlx.NUMERIC || dr.kind == Sqlx.NUMERIC)
                            dm = Domain._Numeric;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.INTEGER || dr.kind == Sqlx.NUMERIC))
                            dm = dl;
                        else if (dr.kind == Sqlx.INTERVAL && (dl.kind == Sqlx.INTEGER || dl.kind == Sqlx.NUMERIC))
                            dm = dr;
                        else if (le is SqlValue tl)
                            dm = tl.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.TYPE: dm = Domain.Char; break;
                case Sqlx.UNION: dm = dl; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            dm ??= Domain.Content;
            cx.Add(new SqlValueExpr(defpos, mem + (_Domain, dm)));
            return dm;
        }
        internal override bool HasAnd(Context cx, SqlValue s)
        {
            if (s == this)
                return true;
            if (op != Sqlx.AND)
                return false;
            return (cx.obs[left] as SqlValue)?.HasAnd(cx, s) == true
            || (cx.obs[right] as SqlValue)?.HasAnd(cx, s) == true;
        }
        internal override CTree<long, bool> IsAggregation(Context cx,CTree<long,bool> ags)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is SqlValue lf)
                r += lf.IsAggregation(cx,ags);
            if (cx.obs[right] is SqlValue rg)
                r += rg.IsAggregation(cx,ags);
            if (cx.obs[sub] is SqlValue su)
                r += su.IsAggregation(cx,ags);
            return r;
        }
        internal override int Ands(Context cx)
        {
            if (op == Sqlx.AND)
                return ((cx.obs[left] as SqlValue)?.Ands(cx) ?? 0)
                    + ((cx.obs[right] as SqlValue)?.Ands(cx) ?? 0);
            return base.Ands(cx);
        }
        internal override bool isConstant(Context cx)
        {
            return (cx.obs[left] as SqlValue)?.isConstant(cx) != false
                && (cx.obs[right] as SqlValue)?.isConstant(cx) != false;
        }
        internal override BTree<long, SystemFilter> SysFilter(Context cx, BTree<long, SystemFilter> sf)
        {
            switch (op)
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
                                return SystemFilter.Add(sf, sc.copyFrom, Neg(op), lf.Eval(cx));
                            if (rg.isConstant(cx) && lf is SqlCopy sl)
                                return SystemFilter.Add(sf, sl.copyFrom, op, rg.Eval(cx));
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
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.StartCounter(cx, rs, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.AddIn(cx, rb, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.AddIn(cx, rb, tg);
            return tg;
        }
        internal override void OnRow(Context cx, Cursor bmk)
        {
            (cx.obs[left] as SqlValue)?.OnRow(cx, bmk);
            (cx.obs[right] as SqlValue)?.OnRow(cx, bmk);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            if (op != Sqlx.DOT)
                throw new DBException("42174");
            var lf = cx.obs[left];
            var rw = (TRow?)lf?.Eval(cx) ?? TRow.Empty;
            lf?.Set(cx, rw += (right, v));
        }
        /// <summary>
        /// Evaluate the expression (mostly binary operators).
        /// The difficulty here is the avoidance of side-effects and the possibility of default values
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            TypedValue v = domain.defaultValue;
            switch (op)
            {
                case Sqlx.AND:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (a == TBool.False && mod != Sqlx.BINARY)
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
                        var x = domain.Eval(defpos, cx, v, Sqlx.ADD, new TInt(1L));
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
                case Sqlx.COLON: // SPECIFICTYPE
                    {
                        var a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        var b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b is TTypeSpec tt && a.dataType.EqualOrStrongSubtypeOf(tt._dataType))
                            return TBool.True;
                        return TBool.False;
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
                        if (a is TList aa && b is TList bb)
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
                        return domain.Eval(defpos, cx, a, op, b);
                    }
                case Sqlx.DOT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (a is TRow ra)
                        {
                            if (cx.obs[right] is SqlField sf)
                                return ra.values[sf.seq] ?? v;
                            if (cx.obs[right] is SqlCopy sc && sc.defpos is long dp)
                                return ra.values[dp] ??
                                    ((sc.copyFrom is long cp) ? (ra.values[cp] ?? v) : v);
                        }
                        if (a is TNode tn)
                        {
                            if (cx.obs[right] is SqlCopy sn)
                                return tn.tableRow.vals[sn.copyFrom] ?? TNull.Value;
                            if (cx.obs[right] is SqlField sf && tn.dataType.infos[cx.role.defpos] is ObInfo ni
                                && ni.names[sf.name ?? "?"].Item2 is long dp)
                                return tn.tableRow.vals[dp] ?? TNull.Value;
                        }
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
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
                        if (a is TList aa && b is TInt bb)
                            return aa[bb.value] ?? v;
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
                            var w = domain.Eval(defpos, cx, new TInt(0), Sqlx.MINUS, b);
                            return w;
                        }
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        v = domain.Eval(defpos, cx, a, op, b);
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
                        return domain.Eval(defpos, cx, a, op, b);
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
                        if (a is TList aa && b is TInt bb)
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
                        return domain.Eval(defpos, cx, a, op, b);
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
                            return a.dataType.Coerce(cx, mc);
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
            throw new DBException("22000", op).ISO();
        }
        internal override SqlValue Constrain(Context cx, Domain dt)
        {
            if (domain.kind==Sqlx.UNION && dt.kind!=Sqlx.UNION)
            {
                var le = cx._Dom(left);
                var rg = cx._Dom(right);
                if (le is not null && rg is not null
                    && le.kind != Sqlx.UNION && rg.kind != Sqlx.UNION
                    && dt.CanTakeValueOf(le) && dt.CanTakeValueOf(rg))
                    return (SqlValue)cx.Add(this+(_Domain,dt));
                if (le is null && rg is not null && rg.kind != Sqlx.UNION
                    && dt.CanTakeValueOf(rg))
                    return (SqlValue)cx.Add(this+(_Domain,dt));
                if (le is not null && rg is null && le.kind != Sqlx.UNION
                    && dt.CanTakeValueOf(le))
                    return (SqlValue)cx.Add(this+(_Domain,dt));
            }
            return base.Constrain(cx, dt);
        }
        internal override SqlValue Invert(Context cx)
        {
            var lv = (SqlValue?)cx.obs[left] ?? SqlNull.Value;
            var rv = (SqlValue?)cx.obs[right] ?? SqlNull.Value;
            return op switch
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
        internal override bool _MatchExpr(Context cx, SqlValue v, RowSet r)
        {
            if (base._MatchExpr(cx, v, r)) return true;
            if (v is not SqlValueExpr e || CompareTo(v) != 0)
                return false;
            if (cx.obs[left] is SqlValue lv && !lv._MatchExpr(cx, lv, r))
                return false;
            if (cx.obs[e.left] != null)
                return false;
            if (cx.obs[right] is SqlValue rv && !rv._MatchExpr(cx, rv, r))
                return false;
            if (cx.obs[e.right] != null)
                return false;
            return true;
        }
        internal override CTree<long, TypedValue> Add(Context cx, CTree<long, TypedValue> ma,
            Table? tb = null)
        {
            if (op == Sqlx.EQL)
            {
                if (cx.obs[left] is SqlCopy sc && cx.obs[right] is SqlLiteral sr
                    && (tb == null || (cx.db.objects[sc.copyFrom] is TableColumn tc
                    && tc.tabledefpos == tb.defpos)))
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
        internal override CTree<long, bool> Needs(Context cx, long rs, CTree<long, bool> qn)
        {
            var r = qn;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx, rs, r) ?? r;
            if (op != Sqlx.DOT)
                r = ((SqlValue?)cx.obs[right])?.Needs(cx, rs, r) ?? r;
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is SqlValue lf) r += lf.Needs(cx);
            if (cx.obs[right] is SqlValue rg) r += rg.Needs(cx);
            if (cx.obs[sub] is SqlValue su) r += su.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx, rs);
            if (op != Sqlx.DOT && cx.obs[right] is SqlValue sw)
                r += sw.Needs(cx, rs);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(Uid(defpos)); sb.Append('(');
            if (left != -1L)
                sb.Append(Uid(left));
            sb.Append(Show(op));
            if (right != -1L)
                sb.Append(Uid(right));
            if (op == Sqlx.LBRACK)
                sb.Append(']');
            if (op == Sqlx.LPAREN)
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
            if (left >= 0 && right >= 0 && op != Sqlx.LBRACK && op != Sqlx.LPAREN)
            {
                sb.Append('(');
                lp = true;
            } 
            if (left >= 0)
            {
                var lf = cx.obs[left]?.ToString(sg, Remotes.Operands, cs, ns, cx) ?? "";
                sb.Append(lf);
            }
            sb.Append(Show(op));
            if (right >= 0)
            {
                var rg = cx.obs[right]?.ToString(sg, Remotes.Operands, cs, ns, cx) ?? "";
                sb.Append(rg);
            }
            if (op == Sqlx.LBRACK)
                sb.Append(']');
            if (lp || op == Sqlx.LPAREN)
                sb.Append(')'); 
            switch (rf)
            {
                case Remotes.Selects:
                    var nm = alias ?? name ?? "";
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
    /// 
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
        internal override TypedValue _Eval(Context cx)
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            return this;
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
    
    internal class SqlSecurity : SqlValue
    {
        internal SqlSecurity(long dp,Context cx) 
            : base(new Ident("SECURITY",new Iix(dp)), BList<Ident>.Empty, cx, Domain._Level)
        {
            cx.Add(this);
        }
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
    /// 
    /// </summary>
    internal class SqlFormal : SqlValue 
    {
        public SqlFormal(Context cx, string nm, Domain dm, long cf=-1L)
            : base(cx, nm, dm, cf)
        {
            cx.Add(this);
        }
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
    /// 
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
            : base(dp, BTree<long, object>.Empty + (_Domain, v.dataType) + (_Val, v))
        {
            if (td != null  && v.dataType is not null && !td.CanTakeValueOf(v.dataType))
                throw new DBException("22000", v);
            if (dp == -1L)
                throw new PEException("PE999");
        }
        public SqlLiteral(long dp, string n, TypedValue v, Domain? td=null)
            : base(dp, _Mem(n,v,td))
        {  }
        protected SqlLiteral(long dp, BTree<long, object> m) : base(dp, m) 
        {  }
        static BTree<long,object> _Mem(string n,TypedValue v,Domain? td)
        {
            var r = new BTree<long, object>(_Val, v);
            r += (_Domain, td ?? v.dataType);
            r += (ObInfo.Name, n);
            return r;
        }
        public static SqlLiteral operator +(SqlLiteral et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlLiteral)et.New(m + x);
        }
        public static SqlLiteral operator +(SqlLiteral et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlLiteral)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlLiteral(defpos,m);
        }
        public SqlLiteral(long dp, Domain dt) 
            : base(dp, new BTree<long,object>(_Domain,dt) + (_Val, dt.defaultValue))
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
            if (CompareTo(v)!=0)
                return false;
            return v is SqlLiteral c &&  val == c.val;
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue _Eval(Context cx)
        {
            if (val is TQParam tq && cx.values[tq.qid.dp] is TypedValue tv && tv != TNull.Value)
                return tv;
            return val ?? domain.defaultValue ?? TNull.Value;
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
            if (dt.CanTakeValueOf(domain))
                return (SqlValue)cx.Add(this+(_Domain,dt));
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
    /// 
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
        {
            cx.Add(this);
        }
        protected SqlDateTimeLiteral(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDateTimeLiteral operator +(SqlDateTimeLiteral et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlDateTimeLiteral)et.New(m + x);
        }
        public static SqlDateTimeLiteral operator +(SqlDateTimeLiteral et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlDateTimeLiteral)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDateTimeLiteral(defpos,m);
        }
    }
    /// <summary>
    /// A Row value
    /// 
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
            : base(dp, _Mem(cx,vs,m))
        {
            cx.Add(this);
        }
        public SqlRow(long dp, Context cx, Domain xp, BList<long?> vs, BTree<long, object>? m = null)
            : base(dp, _Inf(cx, m, xp, vs) + (_Depth,cx._DepthBV(vs,xp.depth+1)))
        {
            cx.Add(this);
        } 
        internal SqlRow(long dp,Context cx,Domain dm) :base(dp,_Mem(cx,dm))
        { }
        static BTree<long, object> _Mem(Context cx, Domain dm)
        {
            var m = new BTree<long, object>(_Domain,dm) + (_Depth, dm.depth) + (Domain.Aggs, dm.aggs);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                    m += (p, sv);
            return m;
        }
        static BTree<long, object> _Mem(Context cx, BList<DBObject> vs, BTree<long, object>? m)
        {
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, vs));
            m ??= new BTree<long,object>(_Domain,dm) + (Domain.Aggs, dm.aggs);
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var ob = b.value();
                m += (ob.defpos, ob);
            }
            return m;
        }
        protected static BTree<long, object> _Inf(Context cx, BTree<long, object>? m,
    Domain xp, BList<long?> vs)
        {
            var cb = xp.First();
            var bs = BList<DBObject>.Empty;
            var r = m ?? BTree<long, object>.Empty;
            for (var b = vs.First(); b != null; b = b.Next(), cb = cb?.Next())
                if (b.value() is long p)
                {
                    var ob = cx.obs[p] as SqlValue ?? SqlNull.Value;
                    bs += ob;
                    r += (p, ob);
                }
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW,bs,bs.Length));
            return r + (_Domain,dm);
        }

        public static SqlRow operator +(SqlRow et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlRow)et.New(m + x);
        }
        public static SqlRow operator +(SqlRow et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlRow)et.New(m + (dp, ob));
        }
        public static SqlRow operator +(SqlRow s, (Context,SqlValue) x)
        {
            var (cx, sv) = x;
            var dm = s.domain;
            return (SqlRow)s.New(s.mem + (_Domain,
                dm.New(dm.mem + (Domain.RowType, dm.rowType + sv.defpos)
                + (Domain.Representation, dm.representation + (sv.defpos, sv.domain)))));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlRow(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRow)base._Replace(cx, so, sv);
            var cs = BList<long?>.Empty;
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
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
                var dm = cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, vs));
                r += (_Domain, dm);
                r += (_Depth, _Depths(vs));
            }
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.Grouped(cx,gs))
                        return false;
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& c.obs[p] is SqlValue v)
                {
                    var nv = v.Having(c, dm);
                    vs += nv.domain;
                    ch = ch || nv != v;
                }
            return ch ? (SqlValue)c.Add(new SqlRow(c.GetUid(), c, vs)) : this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlRow r)
            {
                var rb = r.domain.rowType.First();
                for (var b = domain.rowType.First(); b != null && rb != null;
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
            var cs = BList<DBObject>.Empty;
            var om = m;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue c)
                {
                    BList<DBObject> ls;
                    (ls, m) = c.Resolve(cx, f, m);
                    cs += ls;
                }
            if (m != om)
            {
                var sv = new SqlRow(defpos, cx, cs);
                cx.Replace(this, sv);
            }
            var r = (SqlValue?)cx.obs[defpos] ?? throw new PEException("PE1800");
            return (new BList<DBObject>(r), m);
        }
        internal override DBObject AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var ch = false;
            for (var b = r.domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue a)
                {
                    a = (SqlValue)a.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    r += (cx, a);
                }
            return ch?(SqlValue)cx.Add(r):this;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
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
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                {
                    r += s.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(p);
                }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            var r = CTree<long,bool>.Empty;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                    r += sv.IsAggregation(cx,ags);
            return r;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long s)
                    vs += (s, cx.obs[s]?.Eval(cx) ?? TNull.Value);
            return new TRow(domain, vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                    tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
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
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue s)
                    qn = s.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
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
            for (var b = domain.rowType.First(); b != null; b = b.Next())
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
    /// 
    /// </summary>
    internal class SqlOldRow : SqlRow
    {
        internal SqlOldRow(Ident ic, Context cx, RowSet fm)
            : base(ic.iix.dp, _Mem(ic,cx,fm))
        {
            cx.Add(this);
        }
        protected SqlOldRow(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic,Context cx,RowSet fm)
        {
            var r = fm.mem  + (ObInfo.Name, ic.ident) + (_From, fm.defpos) + (_Depth,fm.depth+1);
            var ids = Ident.Idents.Empty;
            for (var b= fm.rowType.First();b is not null;b=b.Next())
            if (b.value() is long p && cx.obs[p] is SqlValue cv){
                if (cv.name == null)
                    throw new PEException("PE5030");
                var f = new SqlField(cx.GetUid(), cv.name, b.key(), ic.iix.dp, cv.domain,
                    (cv as SqlCopy)?.copyFrom??-1L);
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
        internal override TypedValue _Eval(Context cx)
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
            if (ta is not null)
                ta.values += (Trigger.OldRow, v);
            base.Set(cx, v);
        }
    }
    
    internal class SqlNewRow : SqlRow
    {
        internal SqlNewRow(Ident ic, Context cx, RowSet fm)
            : base(ic.iix.dp, _Mem(ic, cx, fm))
        {
            cx.Add(this);
        }
        protected SqlNewRow(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic, Context cx, RowSet fm)
        {
            var tg = cx._Dom(fm.target);
            var r = fm.mem + (_Depth,fm.depth+1)
                   + (ObInfo.Name, ic.ident) + (_From, fm.defpos);
            var ids = Ident.Idents.Empty;
            for (var b = tg?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is TableColumn co)
                {
                    var f = new SqlField(cx.GetUid(), co.NameFor(cx), -1, ic.iix.dp, 
                        co.domain,co.defpos);
                    var cix = new Iix(f.defpos, cx, f.defpos);
                    cx.Add(f);
                    ids += (f.name ?? "", cix, Ident.Idents.Empty);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.values[defpos] is TypedValue v)
                return v;
            return base._Eval(cx);
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
    
    internal class SqlRowArray : SqlValue
    {
        internal const long
            _Rows = -319; // BList<long?> SqlValue
        internal BList<long?> rows =>
            (BList<long?>?)mem[_Rows]?? BList<long?>.Empty;
        public SqlRowArray(long dp,Context cx,Domain ap,BList<long?> rs) 
            : base(dp, BTree<long,object>.Empty+(_Domain,ap)+(_Rows, rs)+(_Depth,cx._DepthBV(rs,ap.depth+1)))
        {
            cx.Add(this);
        }
        internal SqlRowArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlRowArray operator +(SqlRowArray et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlRowArray)et.New(m + x);
        }
        public static SqlRowArray operator +(SqlRowArray et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch(dp)
            {
                case _Rows:
                    {
                        m += (_Depth,cx._DepthBV((BList<long?>)ob,et.depth));
                        break;
                    }
                case Domain.Aggs:
                    {
                        m += (_Depth, cx._DepthTVX((CTree<long, bool>) ob, et.depth));
                        break;
                    }
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                    }
                    break;
            }

            return (SqlRowArray)et.New(m + (dp, ob));
        }
        public static SqlRowArray operator+(SqlRowArray s,SqlRow x)
        {
            return new SqlRowArray(s.defpos, s.mem + (_Rows, s.rows + x.defpos));
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
                r += (_Rows, nr);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRowArray)base._Replace(cx, so, sv);
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
                r +=(cx, _Rows, rws);
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
            var ag = domain.aggs;
            for (var b = r.rows?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlRow o && o.AddFrom(cx, q) is SqlRow a)
                {
                    if (a.defpos != b.value())
                        ch = true;
                    rws += a.defpos;
                    ag += a.IsAggregation(cx,ag);
                }
            if (ch)
                r += (cx, _Rows, rws);
            var dm = domain;
            if (ag != domain.aggs)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                r += (cx, Domain.Aggs, ag);
            }
            r += (_Domain, dm);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s)
                {
                    var qb = s.KnownBy(cx, q, ambient);
                    if (qb != true)
                        return qb;
                }
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
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override TypedValue _Eval(Context cx)
        {
            var vs = BList<TypedValue>.Empty;
            for (var b=rows.First(); b is not null; b=b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                vs += v.Eval(cx);
            return new TList(domain, vs);
        }
        internal override RowSet RowSetFor(long dp, Context cx, BList<long?> us,
            CTree<long, Domain> re)
        {
            var dm = domain;
            var rs = BList<(long, TRow)>.Empty;
            var xp = (Domain)dm.New(dm.mem + (Domain.Kind, Sqlx.TABLE));
            var isConst = true;
            if (us != null && !Context.Match(us,xp.rowType))
                xp = (Domain)dm.New(xp.mem + (Domain.RowType, us)
                    + (Domain.Representation, xp.representation + re));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v &&
                    v.Eval(cx) is TypedValue x && x.ToArray() is TypedValue[] y)
                    rs += (v.defpos, new TRow(xp, y));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.obs[p] ?? throw new DBException("42000",""+dp);
                    isConst = (v as SqlValue)?.isConstant(cx) == true;
                    var x = v.Eval(cx);
                    var y = x.ToArray() ?? throw new DBException("42000",""+dp);
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
            BList<DBObject> ls;
            var ch = false;
            var rs = BList<long?>.Empty;
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                    }
                    rs += v.defpos;
                }
            if (ch)
            {
                r = new SqlRowArray(defpos, cx, r.domain, rs);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
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
    
    internal class SqlXmlValue : SqlValue
    {
        internal const long
            Attrs = -323, // BList<(XmlName,long)> SqlValue
            Children = -324, // BList<long?> SqlXmlValue
            _Content = -325, // long SqlXmlValue
            _Element = -326; // XmlName
        public XmlName element => (XmlName?)mem[_Element]??new XmlName("","");
        public BList<(XmlName, long)> attrs =>
            (BList<(XmlName, long)>?)mem[Attrs] ?? BList<(XmlName, long)>.Empty;
        public BList<long?> children =>
            (BList<long?>?)mem[Children]?? BList<long?>.Empty;
        public long content => (long)(mem[_Content]??-1L); // will become a string literal on evaluation
        public SqlXmlValue(long dp, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, (m ?? BTree<long, object>.Empty) + (_Domain,Domain.XML)
                  + (_Element,n)+(_Content,c.defpos)+(_Depth,c.depth+1)) { }
        protected SqlXmlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlXmlValue operator +(SqlXmlValue et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlXmlValue)et.New(m + x);
        }
        public static SqlXmlValue operator +(SqlXmlValue e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SqlXmlValue)e.New(m + (p, o));
        }
        public static SqlXmlValue operator +(SqlXmlValue s, SqlXmlValue child)
        {
            var ds = s.dependents?? CTree<long, bool>.Empty;
            return new SqlXmlValue(s.defpos, 
                s.mem + (Children,s.children+child.defpos)
                +(_Depth,Math.Max(s.depth,child.depth+1)));
        }
        public static SqlXmlValue operator +(SqlXmlValue s, (XmlName,SqlValue) attr)
        {
            var (n, a) = attr;
            var ds = s.dependents ?? CTree<long, bool>.Empty;
            return new SqlXmlValue(s.defpos,
                s.mem + (Attrs, s.attrs + (n,a.defpos))
                + (_Depth, Math.Max(s.depth, a.depth+1)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlXmlValue(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlXmlValue(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var na = cx.FixLXl(attrs);
            if (na != attrs)
                r = cx.Add(r, Attrs, na);
            var nc = cx.FixLl(children);
            if (nc != children)
                r = cx.Add(r, Children, nc);
            var nv = cx.Fix(content);
            if (content != nv)
                r = cx.Add(r, _Content, nv);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlXmlValue)base._Replace(cx, so, sv);
            var at = attrs;
            for (var b=at.First();b is not null;b=b.Next())
            {
                var (n, ao) = b.value();
                var v = cx.ObReplace(ao,so,sv);
                if (v != ao)
                    at = new BList<(XmlName,long)>(at,b.key(), (n, v));
            }
            if (at is not null && at != attrs)
                r +=(Attrs, at);
            var co = cx.ObReplace(content,so,sv);
            if (co != content)
                r +=(cx, _Content, co);
            var ch = children;
            for (var b = ch.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        ch = new BList<long?>(ch, b.key(), v);
                }
            if (ch is not null && ch != children)
                r +=(Children, ch);
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
            for (var b=r.attrs.First();b is not null;b=b.Next())
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
                    r += (_Content, c.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override TypedValue _Eval(Context cx)
        {
            var r = new TXml(element.ToString());
            for (var b = attrs?.First(); b != null; b = b.Next())
            {
                var (n, a) = b.value();
                if (cx.obs[a]?.Eval(cx) is TypedValue ta)
                    r += (n.ToString(), ta);
            }
            for(var b=children?.First();b is not null;b=b.Next())
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
                }
            }
            if (ch)
            {
                r = new SqlXmlValue(defpos, element, co, m);
                if (a != BList<(XmlName, long)>.Empty)
                    r += (Attrs, a);
                if (c != BList<long?>.Empty)
                    r += (Children, c);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("<");
            sb.Append(element.ToString());
            for(var b=attrs.First();b is not null;b=b.Next())
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
    
    internal class SqlSelectArray : SqlValue
    {
        internal const long
            ArrayValuedQE = -444; // long RowSet
        public long aqe => (long)(mem[ArrayValuedQE]??-1L);
        public SqlSelectArray(long dp, RowSet qe, BTree<long, object>? m = null)
            : base(dp, (m ?? BTree<long, object>.Empty) +(_Domain,qe) + (Domain.Aggs,qe.aggs)
                   + (ArrayValuedQE, qe.defpos) + (_Depth,qe.depth+1)) { }
        protected SqlSelectArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator +(SqlSelectArray et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlSelectArray)et.New(m + x);
        }
        public static SqlSelectArray operator +(SqlSelectArray et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch(dp)
            {
                case ArrayValuedQE:
                    {
                        if (ob is long p && cx.obs[p] is RowSet s)
                            m += (_Depth, Math.Max(s.depth + 1, d));
                        break;
                    }
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }

            return (SqlSelectArray)et.New(m + (dp, ob));
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlSelectArray)base._Replace(cx, so, sv);
            var ae = cx.ObReplace(aqe,so,sv);
            if (ae != aqe)
                r +=(cx, ArrayValuedQE, ae);
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
        internal override TypedValue _Eval(Context cx)
        {
            var q = (RowSet?)cx.obs[aqe] ?? throw new PEException("PE1701");
            var va = BList<TypedValue>.Empty;
            var et = domain.elType;
            var nm = q.name;
            for (var rb=q.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb;
                if (et==null && nm is not null && rw[nm] is TypedValue v)
                    va += v;
                else
                {
                    var vs = new TypedValue[q.display];
                    for (var i = 0; i < q.display; i++)
                        vs[i] = rw[i];
                    va += new TRow(q, vs);
                }
            }
            return new TList(domain,va);
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
    /// 
    /// </summary>
    internal class SqlValueArray : SqlValue
    {
        internal const long
            _Array = -328, // BList<long?> SqlValue
            Svs = -329; // long SqlValueSelect
        /// <summary>
        /// the array
        /// </summary>
        public BList<long?> array =>(BList<long?>?)mem[_Array]??BList<long?>.Empty;
        // alternatively, the source
        public long svs => (long)(mem[Svs] ?? -1L);
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(long dp,Context cx,Domain xp,BList<long?> v)
            : base(dp,xp.mem+(_Array,v)+(_Domain,xp)+(_Depth,cx._DepthBV(v,xp.depth+1)))
        {
            cx.Add(this);
        }
        protected SqlValueArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueArray operator +(SqlValueArray et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueArray)et.New(m + x);
        }
        public static SqlValueArray operator +(SqlValueArray et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch (dp)
            {
                case _Array:
                    m += (_Depth, cx._DepthBV((BList<long?>)ob,et.depth));
                    break;
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long,bool>)ob, et.depth));
                    break;
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                    }
                    break;
            }
            return (SqlValueArray)et.New(m + (dp, ob));
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
            r += (_Array, cx.FixLl(array));
            if (svs>=0)
                r = cx.Add(r, Svs, cx.Fix(svs));
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueArray)base._Replace(cx, so, sv);
            var ar = BList<long?>.Empty;
            for (var b = ar.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        ar += (b.key(), v);
                }
            if (ar != array)
                r +=(cx, _Array, ar);
            var ss = cx.ObReplace(svs, so, sv);
            if (ss != svs)
                r +=(cx, Svs, ss);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = BList<long?>.Empty;
            var ag = domain.aggs;
            var ch = false;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v) {
                    var a = (SqlValue)v.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    ar += a.defpos;
                    ag += a.IsAggregation(cx,ag);
                }
            if (ch)
                r += (cx,_Array, ar);
            if (cx.obs[svs] is SqlValue s)
            {
                s = (SqlValue)s.AddFrom(cx, q);
                if (s.defpos != svs)
                    r += (cx,Svs, s.defpos);
                ag += s.IsAggregation(cx,ag);
            }
            var dm = domain;
            if (ag != dm.aggs)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                r += (cx, Domain.Aggs, ag);
            }
            r += (_Domain, dm);
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
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (svs != -1L)
            {
                var ar = CList<TypedValue>.Empty;
                if (cx.obs[svs]?.Eval(cx) is TList ers)
                    for (var b = ers.list?.First(); b != null; b = b.Next())
                        if (b.value()[0] is TypedValue v)
                            ar += v;
                return new TList(domain, ar);
            }
            var vs = BList<TypedValue>.Empty;
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    vs += cx.obs[p]?.Eval(cx) ?? domain.defaultValue;
            return new TList(domain, vs);
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
            var vs = BList<long?>.Empty;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v) {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
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
                }
            }
            if (ch)
            {
                r = new SqlValueArray(defpos, cx, domain, vs);
                if (sva != SqlNull.Value)
                    r += (cx,Svs, sva.defpos);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("ARRAY[");
            sb.Append(domain.ToString()); sb.Append(']');
            if (array.Count > 0)
            {
                var cm = "(";
                for (var b = array.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
                sb.Append(')');
            }
            if (svs >= 0)
                sb.Append(Uid(svs));
            return sb.ToString();
        }
    }
    /// <summary>
    /// a multiset value
    /// 
    /// </summary>
    internal class SqlValueMultiset : SqlValue
    {
        internal const long
            MultiSqlValues = -302; // BList<long?> SqlValue
        /// <summary>
        /// the array
        /// </summary>
        public BList<long?> multi => (BList<long?>)(mem[MultiSqlValues] ?? BList<long?>.Empty);
        /// <summary>
        /// construct an SqlValueMultiset value
        /// </summary>
        /// <param name="a">the array</param>
        public SqlValueMultiset(long dp, Context cx, Domain xp, BList<long?> v)
            : base(dp, xp.mem + (MultiSqlValues, v)
                  +(_Depth,cx._DepthBV(v,xp.depth+1)))
        {
            cx.Add(this);
        }
        protected SqlValueMultiset(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueMultiset operator +(SqlValueMultiset et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueMultiset)et.New(m + x);
        }
        public static SqlValueMultiset operator +(SqlValueMultiset et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch(dp)
            {
                case MultiSqlValues:
                    m += (_Depth, cx._DepthBV((BList<long?>)ob, et.depth));
                    break;
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long, bool>)ob, et.depth));
                    break;
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                    }
                    break;
            }

            return (SqlValueMultiset)et.New(m + (dp, ob));
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
            r = cx.Add(r, MultiSqlValues, cx.FixLl(multi));
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueMultiset)base._Replace(cx, so, sv);
            var mu = BList<long?>.Empty;
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p){
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.key())
                        mu += v;
                }
            if (mu != multi)
                r +=(cx, MultiSqlValues, mu);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var dm = domain;
            var r = (SqlValueMultiset)base.AddFrom(cx, q);
            var mu = CTree<long,bool>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    var a = (SqlValue)v.AddFrom(cx, q);
                    if (a.defpos != b.key())
                        ch = true;
                    mu += (a.defpos,true);
                    ag += a.IsAggregation(cx,ag);
                }
            if (ch)
                r += (cx, MultiSqlValues, mu);
            if (ag != dm.aggs)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                r += (cx, Domain.Aggs, ag);
            }
            r += (_Domain, dm);
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
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = multi?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
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
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(b.key());
                }
            if (y)
                return new CTree<long, Domain>(defpos,domain);
            return r;
        }
        /// <summary>
        /// evaluate the multiset
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            var vs = BTree<TypedValue,long?>.Empty;
            var n = 0;
            for (var b = multi?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p]?.Eval(cx) is TypedValue te && te!=TNull.Value)
                {
                    vs += (te, 1L);
                    n++;
                }
            return new TMultiset(domain, vs, n);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
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
                if (b.value() is long p && cx.obs[p] is SqlValue v)
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
                if (b.value() is long p && cx.obs[p] is SqlValue v)
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
            var sb = new StringBuilder("MULTISET[");
            sb.Append(Uid(defpos)); sb.Append("](");
            var cm = "";
            for (var b=multi.First();b is not null;b=b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// a set value
    /// 
    /// </summary>
    internal class SqlValueSet : SqlValue
    {
        internal const long
            Elements = -261; // BList<long?> SqlValue
        /// <summary>
        /// the array
        /// </summary>
        public BList<long?> els => (BList<long?>)(mem[Elements] ?? BList<long?>.Empty);
        /// <summary>
        /// construct an SqlValueSet value
        /// </summary>
        /// <param name="v">the elements</param>
        public SqlValueSet(long dp, Context cx, Domain xp, BList<long?> v)
            : base(dp, xp.mem + (Elements, v)
                  +(_Depth,cx._DepthBV(v,xp.depth+1)))
        {
            cx.Add(this);
        }
        protected SqlValueSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueSet operator +(SqlValueSet et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueSet)et.New(m + x);
        }
        public static SqlValueSet operator +(SqlValueSet et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch(dp)
            {
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long, bool>)ob, et.depth));
                    break;
                case Elements:
                    m += (_Depth, cx._DepthBV((BList<long?>)ob, et.depth));
                    break;
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }

            return (SqlValueSet)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueSet(defpos, m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = els.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    r += v._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlValueSet(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            r += (Elements, cx.FixLl(els));
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueSet)base._Replace(cx, so, sv);
            var es = BList<long?>.Empty;
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        es += v;
                }
            if (es != els)
                r +=(cx, Elements, es);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var dm = domain;
            var r = (SqlValueMultiset)base.AddFrom(cx, q);
            var mu = BList<long?>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    var a = (SqlValue)v.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    mu += a.defpos;
                    ag += a.IsAggregation(cx,ag);
                }
            if (ch)
                r += (cx,Elements, mu);
            if (ag != dm.aggs)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                r += (cx, Domain.Aggs, ag);
            }
            r += (_Domain, dm);
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
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    r += v.KnownFragments(cx, kb, ambient);
                    y = y && r.Contains(b.key());
                }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        /// <summary>
        /// evaluate the set
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            var vs = CTree<TypedValue, bool>.Empty;
            var n = 0;
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p]?.Eval(cx) is TypedValue te && te != TNull.Value)
                {
                    vs += (te, true);
                    n++;
                }
            return new TSet(domain, vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
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
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject> ls;
            var r = this;
            var d = depth;
            var vs = BList<long?>.Empty;
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
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
            var sb = new StringBuilder("[");
            sb.Append(Uid(defpos)); sb.Append("](");
            var cm = "";
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A subquery
    /// 
    /// </summary>
    internal class SqlValueSelect : SqlValue
    {
        internal const long
            Expr = -330; // long RowSet
        /// <summary>
        /// the subquery
        /// </summary>
        public long expr =>(long)(mem[Expr]??-1L);
        public SqlValueSelect(long dp,Context cx,RowSet r,Domain xp)
            : base(dp, new BTree<long,object>(_Domain,r) + (Domain.Aggs,r.aggs)
                  + (Expr, r.defpos) + (RowSet._Scalar,xp.kind!=Sqlx.TABLE)
                  + (_Depth,Math.Max(r.depth+1,xp.depth+1)))
        {
            r += (cx,SelectRowSet.ValueSelect, dp);
            cx.Add(r);
        }
        protected SqlValueSelect(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueSelect operator +(SqlValueSelect et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlValueSelect)et.New(m + x);
        }

        public static SqlValueSelect operator +(SqlValueSelect e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (SqlValueSelect)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueSelect(defpos,m);
        }
        internal override RowSet RowSetFor(long dp, Context cx, BList<long?> us,
            CTree<long,Domain> re)
        {
            if (cx.obs[expr] is not RowSet r)
                throw new PEException("PE5200");
            if (us == null || Context.Match(us,r.rowType))
                return r;
            var xp = r +(Domain.RowType, us)
                +(Domain.Representation,r.representation+re);
            return new SelectedRowSet(cx, xp, r);
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
                r = cx.Add(r, Expr, ne);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueSelect)base._Replace(cx, so, sv);
            var ex = (RowSet)cx._Replace(expr,so,sv);
            if (ex.defpos != expr)
                r +=(cx, Expr, ex.defpos);
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
        internal override SqlValue Constrain(Context cx, Domain dt)
        {
            var r = base.Constrain(cx, dt);
            if (dt.kind != Sqlx.TABLE && !scalar)
                r = (SqlValueSelect)cx.Add(this + (RowSet._Scalar, true));
            return r;
        }
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[expr] is not RowSet ers)
                throw new PEException("PE6200");
            if (scalar)
            {
                var r = ers.First(cx)?[0] ?? domain.defaultValue;
        //        cx.funcs -= ers.defpos;
                return r;
            }
            var rs = BList<TypedValue>.Empty;
            for (var b = ers.First(cx); b != null; b = b.Next(cx))
                rs += b;
            return new TList(domain, rs);
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
            if (cx.obs[expr] is not RowSet e)
                throw new PEException("PE6201");
            var nd = CTree<long, bool>.Empty;
            for (var b = e.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    nd += v.Needs(cx);
            for (var b = e.where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                    nd += v.Needs(cx);
            for (var b = e.Sources(cx).First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Domain dm)
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
            if (scalar)
                sb.Append(" scalar");
   //         sb.Append(" TargetType: ");sb.Append(targetType);
            sb.Append(" (");sb.Append(Uid(expr)); sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Column Function SqlValue class
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
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool) + (Bits, c)) { }
        protected ColumnFunction(long dp, BTree<long, object> m) :base(dp, m) { }
        public static ColumnFunction operator +(ColumnFunction et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ColumnFunction)et.New(m + x);
        }
        public static ColumnFunction operator +(ColumnFunction et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ColumnFunction)et.New(m + (dp, ob));
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
    
    internal class SqlCursor : SqlValue
    {
        internal const long
            Spec = -334; // long RowSet
        internal long spec=>(long)(mem[Spec]??-1L);
        internal SqlCursor(long dp, RowSet cs, string n) 
            : base(dp, cs.mem+(ObInfo.Name, n)+(Spec,cs.defpos) +(_Depth,cs.depth+1))
        { }
        protected SqlCursor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCursor operator +(SqlCursor et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCursor)et.New(m + x);
        }
        public static SqlCursor operator +(SqlCursor et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCursor)et.New(m + (dp, ob));
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
                r = cx.Add(r, Spec, ns);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCursor)base._Replace(cx, so, sv);
            var sp = cx.ObReplace(spec,so,sv);
            if (sp != spec)
                r +=(cx, Spec, sp);
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
    
    internal class SqlCall : SqlValue
    {
        internal const long
            Parms = -133, // BList<long?> SqlValue
            ProcDefPos = -134, // long Procedure
            Result = -437, // Domain (e.g. Null, RowSet, SqlValue)
            Var = -135; // long SqlValue
        /// <summary>
        /// The target object (for a method)
        /// </summary>
        public long var => (long)(mem[Var] ?? -1L);
        /// <summary>
        /// The proc/method to call
        /// </summary>
		public long procdefpos => (long)(mem[ProcDefPos] ?? -1L);
        /// <summary>
        /// The tree of actual parameters
        /// </summary>
		public BList<long?> parms => (BList<long?>)(mem[Parms] ?? BList<long?>.Empty);
        public Domain result => (Domain)(mem[Result] ?? Domain.Null);
        public SqlCall(long lp, Context cx, Procedure pr, BList<long?> acts, long tg=-1L)
        : base(lp, _Mem(lp,cx,pr) + (Parms, acts) + (ProcDefPos,pr.defpos) 
              + (Var,tg)+(_Domain,pr.domain))
        {
            cx.Add(this);
        }
        internal SqlCall(long dp, string pn, BList<long?> acts, long tg)
: base(dp, BTree<long,object>.Empty + (Parms, acts) + (ObInfo.Name, pn) + (Var, tg))
        { }
        protected SqlCall(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,Procedure proc)
        {
            var m = BTree<long, object>.Empty;
            var ro = cx.role ?? throw new DBException("42105");
            if (proc.infos[ro.defpos] is not ObInfo pi || pi.name is null)
                throw new DBException("42105");
            if (proc.domain.rowType.Count > 0)
            {
                var prs = new ProcRowSet(cx, proc) + (ObInfo.Name, pi.name)
                    + (CallStatement.Call, dp);
                cx.Add(prs);
                m += (Result, prs);
                m += (Infos, proc.infos);
            }
            return m;
        }
        public static SqlCall operator +(SqlCall et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCall)et.New(m + x);
        }
        public static SqlCall operator+(SqlCall s,(Context,long,object)x)
        {
            var (cx, p, o) = x;
            var m = s.mem;
            switch (p)
            {
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long,bool>)o, s.depth));
                    break;
                case Parms:
                    m += (_Depth, cx._DepthBV((BList<long?>)o, s.depth));
                    break;
                case Result:
                    m += (_Depth,Math.Max(((Domain)o).depth,s.depth));
                    break;
                case ProcDefPos:
                case Var:
                    m += (_Depth, Math.Max(s.depth,(cx.obs[(long)o]?.depth??0) + 1));
                    break;
            }
            return (SqlCall)s.New(m + (p,o));
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
        internal override Basis New(BTree<long, object> m)
        {
            return (SqlCall)New(defpos, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var np = cx.Fix(procdefpos);
            if (np != procdefpos)
                r += (ProcDefPos, np);
            var ns = cx.FixLl(parms);
            if (parms != ns)
                r += (Parms, ns);
            var rs = result.Fix(cx);
            if (result != rs)
                r += (Result, rs);
            var va = cx.Fix(var);
            if (var != va)
                r += (Var, va);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCall)base._Replace(cx, so, sv);
            var nv = cx.ObReplace(var, so, sv);
            if (nv != var)
                r +=(cx,Var, nv);
            var np = cx.ReplacedLl(parms);
            if (np != parms)
                r +=(cx,Parms, np);
            var rs = result.Replace(cx, so, sv);
            if (rs != result)
                r+=(cx,Result, rs);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    tg = v.AddIn(cx, rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long,bool>.Empty;
            for (var b = parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue v)
                        r += v.Needs(cx, rs);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlCall)base.AddFrom(cx, q);
            if (cx.obs[var] is SqlValue a)
            {
                a = (SqlValue)a.AddFrom(cx, q);
                if (a.defpos != var)
                    r += (cx, Var, a.defpos);
            }
            var vs = BList<long?>.Empty;
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    vs += v.AddFrom(cx, q).defpos;
            r += (cx, Parms, vs);
            return (SqlCall)cx.Add(r);
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm,
            Grant.Privilege pr = Grant.Privilege.Select, string? a = null)
        {
            var ro = cx.role ?? throw new DBException("42105");
            if (cx.db.objects[procdefpos] is not Procedure proc
                || proc.infos[proc.definer] is not ObInfo pi || pi.name is null)
                throw new PEException("PE6840");
            var prs = new ProcRowSet(this, cx) + (RowSet.Target, procdefpos) + (ObInfo.Name, pi.name);
            cx.Add(prs);
            return prs;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (var != -1L && (cx.obs[var]?.Calls(defpos, cx) ?? false))
                return true;
            return procdefpos == defpos || Calls(parms, defpos, cx);
        }
        internal static bool Calls(BList<long?> ss, long defpos, Context cx)
        {
            for (var b = ss?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p]?.Calls(defpos, cx) == true)
                    return true;
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (var != -1L)
            {
                sb.Append(" Var="); sb.Append(Uid(var));
            }
            sb.Append(' '); sb.Append(name);
            sb.Append(' '); sb.Append(Uid(procdefpos));
            sb.Append(" (");
            var cm = "";
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// An SqlValue that is a procedure/function call or static method
    /// 
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(long dp, Context cx, Procedure pr, 
            BList<long?> acts) : base(dp, cx, pr,acts,-1L) 
        {
            cx.Add(this);
        }
        protected SqlProcedureCall(long dp,BTree<long,object>m):base(dp,m) { }
        public static SqlProcedureCall operator +(SqlProcedureCall et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlProcedureCall)et.New(m + x);
        }
        public static SqlProcedureCall operator +(SqlProcedureCall s, (Context, long, object) x)
        {
            var d = s.depth;
            var m = s.mem;
            var (cx, p, o) = x;
            if (s.mem[p] == o)
                return s;
            switch (p)
            {
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long, bool>)o, s.depth));
                    break;
                case Parms:
                    m += (_Depth, cx._DepthBV((BList<long?>)o, s.depth));
                    break;
                case Result:
                    m += (_Depth, Math.Max(((Domain)o).depth, s.depth));
                    break;
                case ProcDefPos:
                case Var:
                    m += (_Depth, Math.Max(s.depth, (cx.obs[(long)o]?.depth ?? 0) + 1));
                    break;
                default:
                    if (o is long q && cx.obs[q] is DBObject bb)
                    {
                        d = Math.Max(bb.depth + 1, d);
                        if (d > s.depth)
                            m += (_Depth, d);
                    }
                    break;
            }
            return (SqlProcedureCall)s.New(m + (p, o));
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
        internal override TypedValue _Eval(Context cx)
        {
            var tr = cx.db;
            var pp = procdefpos;
            var proc = cx.obs[pp] as Procedure;
            if (proc == null && tr.objects[pp] is Procedure tp)
            {
                proc = (Procedure)tp.Instance(defpos, cx);
                cx.Add(proc);
            }
            if (proc == null)
                throw new PEException("PE47167");
            proc.Exec(cx, parms);
            var r = cx.val ?? domain.defaultValue;
            cx.values += (defpos, r);
            return r;
        }
        internal override void Eqs(Context cx,ref Adapters eqs)
        {
            if (cx.obs[procdefpos] is not Procedure proc)
                throw new PEException("PE47168");
            if (cx.db.objects[proc.inverse] is Procedure inv && parms[0] is long cp)
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
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    r += v.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// A SqlValue that is evaluated by calling a method
    /// 
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
        public SqlMethodCall(long dp, Context cx, Procedure pr,
            BList<long?> acts, SqlValue tg) : base(dp,cx, pr, acts, tg.defpos)
        {
            cx.Add(this);
        }
        public SqlMethodCall(long dp, Context cx, string pn,BList<long?> acts, SqlValue tg) 
            : base(dp, pn, acts, tg.defpos)
        {
            cx.Add(this);
        }
        protected SqlMethodCall(long dp,BTree<long, object> m) : base(dp, m) { }
        public static SqlMethodCall operator +(SqlMethodCall et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlMethodCall)et.New(m + x);
        }
        public static SqlMethodCall operator +(SqlMethodCall s, (Context, long, object) x)
        {
            var d = s.depth;
            var m = s.mem;
            var (cx, p, o) = x;
            if (s.mem[p] == o)
                return s;
            switch (p)
            {
                case Domain.Aggs:
                    m += (_Depth, cx._DepthTVX((CTree<long, bool>)o, s.depth));
                    break;
                case Parms:
                    m += (_Depth, cx._DepthBV((BList<long?>)o, s.depth));
                    break;
                case Result:
                    m += (_Depth, Math.Max(((Domain)o).depth, s.depth));
                    break;
                case ProcDefPos:
                case Var:
                    m += (_Depth, Math.Max(s.depth, (cx.obs[(long)o]?.depth ?? 0) + 1));
                    break;
                default:
                    if (o is long q && cx.obs[q] is DBObject bb)
                    {
                        d = Math.Max(bb.depth + 1, d);
                        if (d > s.depth)
                            m += (_Depth, d);
                    }
                    break;
            }
            return (SqlMethodCall)s.New(m + (p, o));
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
            qn += (var, true);
            return base.Needs(cx, r, qn);
        }
        internal override (BList<DBObject>, BTree<long,object>) Resolve(Context cx, long f, BTree<long,object> m)
        {
            if (domain.kind!=Sqlx.Null)
                return (new BList<DBObject>(this),m);
            BList<DBObject> ls;
                (ls, m) = base.Resolve(cx, f, m);
                if (ls[0] is SqlMethodCall mc && cx.obs[mc.var] is SqlValue ov && mc.name is not null)
                {
                    (ls, m) = ov.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv)
                        ov = nv;
                    if (cx.role is not null && infos[cx.role.defpos] is ObInfo oi 
                        && cx._Ob(procdefpos) is Procedure pr)
                    { // we need the names
                        var p = oi.methodInfos[mc.name]?[cx.Signature(pr)] ?? -1L;
                        mc = mc + (cx, Var, ov.defpos) + (cx, ProcDefPos, p);
                        return (new BList<DBObject>(cx.Add(mc)),m);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[var] is not SqlValue v  || cx.role==null)
                throw new PEException("PE241");
            var vv = v.Eval(cx);
            for (var b = v.domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && vv[bp] is TypedValue tv)
                    cx.values += (bp, tv);
            var p = procdefpos;
            if (cx.db.objects[p] is not Method me)
                throw new DBException("42108", Uid(defpos));
            var proc = (Method)me.Instance(defpos,cx);
            return proc.Exec(cx, var, parms).val;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[var] is not SqlValue v)
                throw new PEException("PE47196");
            var r = v.Needs(cx, rs);
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue x)
                    r += x.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// An SqlValue that is a constructor expression
    /// 
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(long dp, Context cx, Procedure pr, BList<long?> args)
            : base(dp, cx,pr, args)
        {
            cx.Add(this);
        }
        protected SqlConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlConstructor operator +(SqlConstructor et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlConstructor)et.New(m + x);
        }
        public static SqlConstructor operator +(SqlConstructor et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlConstructor)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
        {
            var tr = cx.db;
            if (tr.objects[procdefpos] is not Method proc)
                throw new PEException("PE5802");
            var ac = new CalledActivation(cx, proc);
            return proc.Exec(ac, -1L, parms).val;
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
    /// 
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
        {
            cx.Add(this);
        }
        protected SqlDefaultConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(long ap, Context cx, UDType u, BList<long?> ps)
        {
            var rb = u.representation.First();
            for (var b = ps.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    var dt = rb.value();
                    cx.Add(v+(_Domain,dt));
                }
            return BTree<long, object>.Empty + (_Domain, u)
                  + (Sce, cx._Add(new SqlRow(ap, cx, u, ps)).defpos)
                  + (_Depth,cx._DepthBV(ps,u.depth+1));
        }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlDefaultConstructor)et.New(m + x);
        }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlDefaultConstructor)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDefaultConstructor(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SqlDefaultConstructor(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlDefaultConstructor)base._Replace(cx, so, sv);
            if (cx.obs[sce] is SqlRow os)
            {
                var sc = os.Replace(cx, so, sv);
                if (sc.defpos != sce)
                    r +=(cx, Sce, sc.defpos);
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
        internal override TypedValue _Eval(Context cx)
        {
            try
            { 
                var vs = CTree<long,TypedValue>.Empty;
                if (cx.obs[sce] is SqlRow sc)
                {
                    var db = domain.rowType.First();
                    for (var b = sc.domain.rowType.First(); b != null && db is not null; b = b.Next(), db=db.Next())
                        if (b.value() is long p && cx.obs[p] is SqlValue v
                            && db.value() is long dp)
                            vs += (dp, v.Eval(cx));
                }
                cx.values += vs;
                var r = new TRow(domain, vs);
                cx.values += (defpos,r);
                return r;
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return domain.defaultValue;
            }
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var sc = (SqlValue?)cx.obs[sce] ?? SqlNull.Value;
            if (sc != SqlNull.Value)
            {
                (ls, m) = sc.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != sc.defpos)
                {
                    r = this + (cx, Sce, nv.defpos);
                    cx.Replace(this, r);
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
    /// 
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
        public Sqlx op => (Sqlx)(mem[SqlValueExpr.Op] ?? Sqlx.NO);
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod => (Sqlx)(mem[Mod] ?? Sqlx.NO);
        /// <summary>
        /// the value parameter for the function
        /// </summary>
        public long val => (long)(mem[_Val] ?? -1L);
        /// <summary>
        /// operands for the function
        /// </summary>
        public long op1 => (long)(mem[Op1] ?? -1L);
        public long op2 => (long)(mem[Op2] ?? -1L);
        /// <summary>
        /// a Filter for the function
        /// </summary>
        public CTree<long, bool> filter =>
            (CTree<long, bool>?)mem[Filter] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// a name for the window for a window function
        /// </summary>
        public long windowId => (long)(mem[WindowId] ?? -1L);
        /// <summary>
        /// the window for a window function
        /// </summary>
        public long window => (long)(mem[Window] ?? -1L);
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
            : base(dp, _Mem(cx, vl, o1, o2, (mm ?? BTree<long, object>.Empty) + (_Domain, _Type(cx, f, vl, o1))
                + (ObInfo.Name, f.ToString()) + (SqlValueExpr.Op, f) + (Mod, m)))
        { 
            cx.Add(this);
            vl?.ConstrainScalar(cx);
        }
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
            r += (_Depth, _Depths(vl, o1, o2, w));
            return r;
        }
        public static SqlFunction operator +(SqlFunction et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlFunction)et.New(m + x);
        }
        public static SqlFunction operator +(SqlFunction et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch (dp)
            {
                case Op1:
                    {
                        if (cx.obs[et.op1] is SqlValue sw && ob is long p && cx.obs[p] is SqlValue w)
                            m += _Type(cx, et.op, w, sw).mem;
                        goto default;
                    }
                case _Val:
                    {
                        if (cx.obs[et.val] is SqlValue sv && ob is long p && cx.obs[p] is SqlValue u)
                            m += _Type(cx, et.op, sv, u).mem;
                        goto default;
                    }
                case Filter:
                    {
                        m += (_Depth, cx._DepthTVX((CTree<long, bool>)ob, d));
                        break;
                    }
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                    }
                    break;
            }
            return (SqlFunction)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFunction(defpos, m);
        }
        internal override void Validate(Context cx)
        {
            ((SqlValue?)cx.obs[val])?.Validate(cx); 
            ((SqlValue?)cx.obs[op1])?.Validate(cx); 
            ((SqlValue?)cx.obs[op2])?.Validate(cx);
        }
        internal override CTree<long, bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {

            var r = CTree<long, bool>.Empty;
            if (window >= 0) // Window functions do not aggregate rows!
                return r;
            if (aggregates(op))
                r += (defpos, true);
            if (cx.obs[val] is SqlValue vl)
                r += vl.IsAggregation(cx,ags);
            if (cx.obs[op1] is SqlValue o1)
                r += o1.IsAggregation(cx,ags);
            if (cx.obs[op2] is SqlValue o2)
                r += o2.IsAggregation(cx,ags);
            return r;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            if (aggregates(op))
            {
                for (var b = dm.aggs.First(); b != null; b = b.Next())
                    if (c.obs[b.key()] is SqlFunction sf && Match(c, sf))
                        return sf;
                return base.Having(c, dm);
            }
            SqlValue? nv = null, n1 = null, n2 = null;
            bool ch = false;
            if (c.obs[val] is SqlValue vl)
            { nv = vl.Having(c, dm); ch = nv != vl; }
            if (c.obs[op1] is SqlValue o1)
            { n1 = o1.Having(c, dm); ch = n1 != o1; }
            if (c.obs[op2] is SqlValue o2)
            { n2 = o2.Having(c, dm); ch = n2 != o2; }
            return ch ? (SqlValue)c.Add(new SqlFunction(c.GetUid(), c, op, nv, n1, n2, mod)) : this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlFunction f)
            {
                if (filter != CTree<long, bool>.Empty)
                {
                    var fb = f.filter.First();
                    for (var b = filter.First(); b != null; b = b.Next(), fb = fb.Next())
                        if (fb == null || (c.obs[b.key()] is SqlValue le && c.obs[fb.key()] is SqlValue fe
                            && !le.Match(c, fe)))
                            return false;
                    if (fb != null)
                        return false;
                }
                if (op != f.op || mod != f.mod || windowId != f.windowId)
                    return false;
                if (c.obs[op1] is SqlValue o1 && c.obs[f.op1] is SqlValue f1 && !o1.Match(c, f1))
                    return false;
                if (c.obs[op2] is SqlValue o2 && c.obs[f.op2] is SqlValue f2 && !o2.Match(c, f2))
                    return false;
                if (c.obs[val] is SqlValue vl && c.obs[f.val] is SqlValue fv && !vl.Match(c, fv))
                    return false;
                if (window >= 0 || f.window >= 0)
                    return false;
                return true;
            }
            return false;
        }
        internal override bool _MatchExpr(Context cx, SqlValue v, RowSet r)
        {
            if (v is not SqlFunction f)
                return false;
            return (op == f.op && val == f.val && op1 == f.op1 && op2 == f.op2);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is SqlValue v) r += v._Rdc(cx);
            if (cx.obs[op1] is SqlValue o1) r += o1._Rdc(cx);
            if (cx.obs[op2] is SqlValue o2) r += o2._Rdc(cx);
            return r;
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlFunction(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var nf = cx.FixTlb(filter);
            if (filter != nf)
                r += (Filter, nf);
            var n1 = cx.Fix(op1);
            if (op1 != n1)
                r += (Op1, n1);
            var n2 = cx.Fix(op2);
            if (op2 != n2)
                r += (Op2, n2);
            var w = cx.FixTlb(filter);
            if (w != filter)
                r += (Filter, w);
            var nv = cx.Fix(val);
            if (val != nv)
                r += (_Val, nv);
            var ni = cx.Fix(windowId);
            if (windowId != ni)
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            if (cx.obs[val] is SqlValue vl)
            {
                (ls, m) = vl.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != vl.defpos)
                {
                    ch = true;
                    r += (_Val, nv);
                }
            }
            if (cx.obs[op1] is SqlValue o1)
            {
                (ls, m) = o1.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != o1.defpos)
                {
                    ch = true;
                    r += (Op1, nv.defpos);
                }
            }
            if (cx.obs[op2] is SqlValue o2)
            {
                (ls, m) = o2.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != o2.defpos)
                {
                    ch = true;
                    r += (Op2, nv.defpos);
                }
            }
            if (cx.obs[window] is WindowSpecification ow)
            {
                (ls, m) = ow.Resolve(cx, f, m);
                if (ls[0] is WindowSpecification nw && nw.defpos != ow.defpos)
                {
                    ch = true;
                    r += (Window, nw.defpos);
                }
            }
            if (ch)
                cx.Replace(this, r);
            return (new BList<DBObject>(r), m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlFunction)base._Replace(cx, so, sv);
            var w = filter;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
            }
            if (w != filter)
                r += (cx, Filter, w);
            var o1 = cx.ObReplace(op1, so, sv);
            if (o1 != op1)
                r += (cx, Op1, o1);
            var o2 = cx.ObReplace(op2, so, sv);
            if (o2 != op2)
                r += (cx, Op2, o2);
            var vl = cx.ObReplace(val, so, sv);
            if (vl != val)
                r += (cx, _Val, vl);
            if (domain.kind == Sqlx.UNION || domain.kind == Sqlx.CONTENT)
            {
                var dm = _Type(cx, op, cx._Ob(val) as SqlValue, cx._Ob(op1) as SqlValue);
                if (dm != null)
                    r = (SqlFunction)cx.Add(r+(_Domain,dm));
            }
            var fw = cx.ObReplace(window, so, sv);
            if (fw != window)
                r += (cx, Window, fw);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[val] is SqlValue v && !v.Grouped(cx, gs)) return false;
            if (cx.obs[op1] is SqlValue o1 && !o1.Grouped(cx, gs)) return false;
            if (cx.obs[op2] is SqlValue o2 && !o2.Grouped(cx, gs)) return false;
            return true;
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
        internal override CTree<long, bool> ExposedOperands(Context cx,CTree<long,bool> ag,Domain? gc)
        {
            if (aggregates(op) || gc?.representation.Contains(defpos)==true)
                return CTree<long, bool>.Empty;
            var os = Operands(cx) -ag;
            if (gc is not null)
                for (var b = os.First(); b != null; b = b.Next())
                    if (gc.representation.Contains(b.key()))
                        os -= b.key();
            return os;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            if (aggregates(op) && q.aggs.Contains(defpos) == true)
                return true;
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, q, ambient) != false &&
            ((SqlValue?)cx.obs[op1])?.KnownBy(cx, q, ambient) != false &&
            ((SqlValue?)cx.obs[op2])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            if (cs.Contains(defpos))
                return true;
            return ((SqlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient) != false &&
            ((SqlValue?)cx.obs[op1])?.KnownBy(cx, cs, ambient) != false &&
            ((SqlValue?)cx.obs[op2])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            var r = CTree<long, Domain>.Empty;
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
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal static Domain _Type(Context cx, Sqlx op, SqlValue? val, SqlValue? op1)
        {
            switch (op)
            {
                case Sqlx.ABS:
                case Sqlx.CEIL:
                case Sqlx.CEILING:
                case Sqlx.MOD:
                case Sqlx.SUM:
                case Sqlx.FLOOR:
                    {
                        var d = val?.domain ?? Domain.UnionNumeric;
                        if (d.kind == Sqlx.CONTENT || d.kind == Sqlx.Null)
                            d = Domain.UnionNumeric;
                        return d;
                    }
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AVG: return Domain._Numeric;
                case Sqlx.ARRAY: return Domain.Collection;
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CAST: return op1?.domain ?? Domain.Content;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: return Domain._Rvv;
                case Sqlx.COLLECT: return Domain.Collection;
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; 
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_ROLE: return Domain.Char;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.DESCRIBE: return Domain.Char;
                case Sqlx.ELEMENT: return val?.domain.elType ?? Domain.Content;
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
                case Sqlx.MAX: return val?.domain ?? Domain.Content;
                case Sqlx.MIN: return val?.domain ?? Domain.Content;
                case Sqlx.NEXT: return val?.domain ?? Domain.UnionDate;
                case Sqlx.NORMALIZE: return Domain.Char;
                case Sqlx.NULLIF: return op1?.domain ?? Domain.Content;
                case Sqlx.OCTET_LENGTH: return Domain.Int;
                case Sqlx.OVERLAY: return Domain.Char;
                case Sqlx.PARTITION: return Domain.Char;
                case Sqlx.POSITION: return Domain.Int;
                case Sqlx.POWER: return Domain.Real;
                case Sqlx.RANK: return Domain.Int;
                case Sqlx.RESTRICT: return val?.domain ?? Domain.Content;
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
                case Sqlx.VERSIONING:
                    cx.versioned = true;
                    return (op1 == null) ? Domain.Int : Domain._Rvv;
                case Sqlx.WHEN: return val?.domain ?? Domain.Content;
                case Sqlx.XMLCAST: return op1?.domain ?? Domain.Content;
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return Domain.Null;
        }
        internal override TypedValue _Eval(Context cx)
        {
            var fc = StartCounter(cx, TRow.Empty); // replaced below
            var ws = cx.obs[window] as WindowSpecification;
            if (ws is not null)
            {
                var kv = CTree<long, TypedValue>.Empty;
                var ks = CList<TypedValue>.Empty;
                var ps = CList<TypedValue>.Empty;
                var pd = ws.partition;
                var od = (ws.order != Domain.Row) ? ws.order : ws.partition;
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
                if (ks is not null)
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
                switch (op)
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
                                if (pb._mb is not null && pb._mb.key() is CList<TypedValue> k
                                    && k.CompareTo(ks) == 0)
                                {
                                    fc.row = rn;
                                    break;
                                }
                            break;
                        }
                }
            }
            else if (aggregates(op))
            {
                var og = (cx.obs[from]?? cx.obs[cx.result]) as RowSet?? throw new PEException("PE29005");
                var gc = og.groupCols;
                var key = (gc == null || gc == Domain.Null) ? TRow.Empty : new TRow(gc, cx.values);
                fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            }
            TypedValue dv = domain.defaultValue ?? TNull.Value;
            TypedValue v = TNull.Value;
            switch (op)
            {
                case Sqlx.ABS:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1902");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vl.domain.kind)
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
                                    var cs = vl.domain.unionOf;
                                    if (cs.Contains(Domain.Int))
                                        goto case Sqlx.INTEGER;
                                    if (cs.Contains(Domain._Numeric))
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
                        var ar = new TList((Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, de)));
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
                        if (cx.obs[op1] is SqlValue ce && ce.domain is Domain cd)
                            return cd.Coerce(cx, v);
                        break;
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1962");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vl.domain.kind)
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
                    return domain.Coerce(cx, fc.mset);
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
                case Sqlx.ELEMENTID:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        return new TInt(n.tableRow.defpos);
                    }
                case Sqlx.EXP:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1967");
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        var vd = vl ?? throw new PEException("PE1970");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vd.domain.kind)
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
                case Sqlx.FUSION:
                    if (fc == null || fc.mset == null) break;
                    return domain.Coerce(cx, fc.mset);
                case Sqlx.ID:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        return (a is TNode n) ? n.id : a ?? TNull.Value;
                    }
                case Sqlx.INTERSECTION:
                    if (fc == null || fc.mset == null) break;
                    return domain.Coerce(cx, fc.mset);
                case Sqlx.LABELS:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        var s = new TSet(Domain.Char);
                        for (var b = n.dataType; b is not null; b = b.super)
                            s += new TChar(b.name);
                        return s;
                    }
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
                case Sqlx.MAX:
                    if (fc == null) break;
                    return fc.acc ?? dv;
                case Sqlx.MIN:
                    if (fc == null) break;
                    return fc.acc ?? dv;
                case Sqlx.MOD:
                    {
                        if (cx.obs[op1] is not SqlValue o1) break;
                        v = o1.Eval(cx);
                        if (v == TNull.Value)
                            return dv;
                        switch (o1.domain.kind)
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
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        var w = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (w == TNull.Value)
                            return dv;
                        return new TReal(Math.Pow(v.ToDouble(), w.ToDouble()));
                    }
                case Sqlx.RANK: goto case Sqlx.ROW_NUMBER;
                case Sqlx.RESTRICT:
                    if (fc == null)
                        break;
                    if (fc.acc is null && cx.conn._tcp is not null)
                        throw new DBException("42170");
                    return fc.acc ?? TNull.Value;
                case Sqlx.ROW_NUMBER:
                    if (fc == null)
                        break;
                    return new TInt(fc.row);
                case Sqlx.SECURITY:
                    return cx.cursors[from]?[Classification] ?? TLevel.D;
                case Sqlx.SET:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1978");
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Sqrt(v.ToDouble()));
                    }
                case Sqlx.STDDEV_POP:
                    {
                        if (fc == null || fc.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        return new TReal(Math.Sqrt(fc.acc1 * fc.count - fc.sum1 * fc.sum1)
                            / fc.count);
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (fc == null || fc.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        return new TReal(Math.Sqrt((fc.acc1 * fc.count - fc.sum1 * fc.sum1)
                            / (fc.count*(fc.count-1))));
                    }
                case Sqlx.SUBSTRING:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1980");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string sv = v.ToString();
                        var w = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (w is not TInt i1)
                            return dv;
                        var x = cx.obs[op2]?.Eval(cx) ?? TNull.Value;
                        var n1 = (int)i1.value;
                        if (n1 < 0 || n1 >= sv.Length)
                            throw new DBException("22000");
                        if (x is not TInt i2)
                            return new TChar(sv[n1..]);
                        var n2 = (int)i2.value;
                        if (n2 < 0 || n2 + n1 - 1 >= sv.Length)
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
                        v = vl.Eval(cx) ?? TNull.Value;
                        return v; // TBD
                    }
                case Sqlx.TRIM:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1982");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string sv = v.ToString();
                        char c = '\0';
                        if (op1 != -1L)
                        {
                            string s = cx.obs[op1]?.Eval(cx)?.ToString() ?? "";
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
                case Sqlx.TYPE:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        return new TTypeSpec(n.dataType);
                    }
                case Sqlx.UPPER:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1983");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TChar(v.ToString().ToUpper());
                    }
                case Sqlx.USER: return new TChar(cx.db.user?.name ?? "");
                case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1984");
                        var vcx = new Context(cx);
                        if (vl is not null)
                        {
                            vcx.result = vl.defpos;
                            return new TRvv("");
                        }
                        vcx.result = from;
                        var p = -1L;
                        for (var b = cx.cursors[from]?.Rec()?.First(); b is not null; b = b.Next())
                        {
                            var t = b.value();
                            if (t.ppos > p)
                                p = t.ppos;
                        }
                        if (p != -1L)
                            return new TInt(p);
                        return TNull.Value;
                    }
                case Sqlx.WHEN: // searched case
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1985");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        TypedValue a = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (a == TBool.True)
                            return v;
                        return cx.obs[op2]?.Eval(cx) ?? TNull.Value;
                    }
                case Sqlx.XMLAGG:
                    if (fc == null || fc.sb == null)
                        break;
                    return new TChar(fc.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1986");
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        string n = XmlConvert.EncodeName(cx.obs[op1]?.Eval(cx)?.ToString() ?? "");
                        string r = "<" + n + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                case Sqlx.XMLPI:
                    {
                        var vl = (SqlValue?)cx.obs[val] ?? throw new PEException("PE1987");
                        v = vl.Eval(cx) ?? TNull.Value;
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
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TInt(Extract(op, v));
                    }
            }
            throw new DBException("42154", op).Mix();
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
        static long Extract(Sqlx mod, TypedValue v)
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
        internal Register StartCounter(Context cx, TRow key)
        {
            var oc = cx.values;
            var fc = new Register(cx, key, this) { acc1 = 0.0, mset = null };
            switch (op)
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
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48185");
                        fc.mset = new TMultiset(vl.domain);
                        break;
                    }
                case Sqlx.XMLAGG:
                    if (window >= 0)
                        goto case Sqlx.COLLECT;
                    fc.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window >= 0)
                        goto case Sqlx.COLLECT;
                    fc.bval = false;
                    break;
                case Sqlx.ARRAY:
                    fc.acc = new TList((Domain)cx.Add(
                        new Domain(cx.GetUid(), Sqlx.ARRAY, Domain.Content)));
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
                case Sqlx.RESTRICT:
                    if (window >= 0L)
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
        internal void AddIn(TRow key, Context cx)
        {
            // not all window functions use val
            var fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            var ct = fc.count;
            if (mod == Sqlx.DISTINCT)
            {
                if (cx.obs[val] is not SqlValue vl)
                    throw new PEException("PE48187");
                var v = vl.Eval(cx) ?? TNull.Value;
                if (fc.mset == null)
                    fc.mset = new TMultiset((Domain)
                        cx.Add(new Domain(cx.GetUid(), Sqlx.MULTISET, vl.domain)));
                else if (fc.mset.Contains(v))
                    return;
                fc.mset = fc.mset.Add(v);
            }
            switch (op)
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
                        if (window >= 0)
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
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48190");
                        fc.mset ??= new TMultiset(vl.domain);
                        var v = vl.Eval(cx);
                        if (v != TNull.Value)
                            fc.mset = fc.mset.Add(v);
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
                case Sqlx.RESTRICT:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48198");
                        TypedValue v = vl.Eval(cx);
                        if (v != TNull.Value && (fc.acc == null || fc.acc.CompareTo(v) == 0))
                            fc.acc = v;
                        else 
                        if (cx.conn._tcp is not null) throw new DBException("42170", vl.name??"");
                        break;
                    }
                case Sqlx.STDDEV_POP:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48199");
                        var o = vl.Eval(cx);
                        var v = o.ToDouble();
                        fc.sum1 += v;
                        fc.acc1 += v * v;
                        fc.count++;
                        break;
                    }
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SUM:
                    {
                        if (cx.obs[val] is not SqlValue vl)
                            throw new PEException("PE48200");
                        var v = vl.Eval(cx);
                        if (v == null || v == TNull.Value)
                        {
                            fc.count = ct;
                            return;
                        }
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
                                    fc.sumType = Domain._Numeric;
                                    fc.sumDecimal = nv.value;
                                }
                                else
                                    throw new DBException("22000", domain.kind);
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
            if (window < 0)
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
            if (ws.units == Sqlx.RANGE && !(TestStartRange(cx, bmk, fc) && TestEndRange(cx, bmk, fc)))
                return false;
            if (ws.units == Sqlx.ROWS && !(TestStartRows(cx, bmk, fc) && TestEndRows(cx, bmk, fc)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Context cx, RTreeBookmark bmk, Register fc)
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
                limit = fc.wrb._pos - (ws.high.distance?.ToLong() ?? 0);
            else
                limit = fc.wrb._pos + (ws.high.distance?.ToLong() ?? 0);
            return bmk._pos <= limit;
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Context cx, RTreeBookmark bmk, Register fc)
        {
            if (cx.obs[window] is not WindowSpecification ws)
                return false;
            if (ws.low == null || ws.low.unbounded)
                return true;
            long limit;
            if (fc.wrb == null)
                return false;
            if (ws.low.current)
                limit = fc.wrb._pos;
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
                limit = kt.Eval(defpos, cx, wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS,
                    ws.high.distance);
            else
                limit = kt.Eval(defpos, cx, wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS,
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
            if (aggregates(op) && r != -1L && from != r)
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
            if (aggregates(op) && from != rs)
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
            if (op == Sqlx.COUNT && mod == Sqlx.TIMES)
                return "COUNT(*)";
            switch (sg)
            {
                case "Pyrrho":
                    {
                        var sb = new StringBuilder();
                        string vl, o1, o2;
                        if (!aggregates(op)) // ||((RowSet)cx.obs[from]).Built(cx))
                        {
                            vl = (cx.obs[val] is SqlValue sv) ? sv.ToString(sg, rf, cs, ns, cx) : "";
                            o1 = (cx.obs[op1] is SqlValue s1) ? s1.ToString(sg, rf, cs, ns, cx) : "";
                            o2 = (cx.obs[op2] is SqlValue s2) ? s2.ToString(sg, rf, cs, ns, cx) : "";
                        }
                        else
                        {
                            vl = ((cx.obs[val] is SqlCopy vc) ? ns[vc.copyFrom] : ns[val]) ?? "";
                            o1 = ((cx.obs[op1] is SqlCopy c1) ? ns[c1.copyFrom] : ns[op1]) ?? "";
                            o2 = ((cx.obs[op2] is SqlCopy c2) ? ns[c2.copyFrom] : ns[op2]) ?? "";
                        }
                        switch (op)
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
                                sb.Append(op); sb.Append('(');
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
                                sb.Append(op); sb.Append('(');
                                sb.Append(vl); sb.Append(" as ");
                                sb.Append(name); sb.Append(')');
                                break;
                            case Sqlx.COLLECT:
                            case Sqlx.FUSION:
                            case Sqlx.INTERSECTION:
                                sb.Append(op); sb.Append('(');
                                if (mod != Sqlx.NO)
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
                                sb.Append(op); break;
                            case Sqlx.EXTRACT:
                                sb.Append(op); sb.Append('(');
                                sb.Append(mod); sb.Append(" from ");
                                sb.Append(vl); sb.Append(')');
                                break;
                            case Sqlx.MOD:
                            case Sqlx.POWER:
                                sb.Append(op); sb.Append('(');
                                sb.Append(o1); sb.Append(',');
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Sqlx.POSITION:
                                sb.Append(op); sb.Append('(');
                                sb.Append(o1); sb.Append(" in ");
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Sqlx.SUBSTRING:
                                sb.Append(op); sb.Append('(');
                                sb.Append(vl); sb.Append(" from ");
                                sb.Append(o1);
                                if (o2 != null)
                                { sb.Append(" for "); sb.Append(o2); }
                                sb.Append(')');
                                break;
                        }
                        var an = alias ?? "";
                        if (an != "")
                        { sb.Append(" as "); sb.Append(an); }
                        return sb.ToString();
                    }
            }
            return base.ToString(sg, rf, cs, ns, cx);
        }
        public override string ToString()
        {
            switch (op)
            {
                case Sqlx.PARTITION:
                case Sqlx.VERSIONING:
                case Sqlx.CHECK: return op.ToString();
                case Sqlx.POSITION:
                    if (op1 != -1L) goto case Sqlx.PARTITION;
                    break;
            }
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');
            sb.Append(op);
            sb.Append('(');
            if (val != -1L)
                sb.Append(Uid(val));
            if (op1 != -1L)
            {
                sb.Append(':'); sb.Append(Uid(op1));
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
            if (alias is not null)
            {
                sb.Append(" as "); sb.Append(alias);
            }
            if (filter != CTree<long, bool>.Empty)
            {
                sb.Append(" filter=(");
                var cm = "";
                for (var b = filter.First(); b is not null; b = b.Next())
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
        internal static bool aggregates(Sqlx op)
        {
            return op switch
            {
                Sqlx.ANY or Sqlx.ARRAY or Sqlx.AVG or Sqlx.COLLECT or Sqlx.COUNT 
                or Sqlx.EVERY or Sqlx.FIRST or Sqlx.FUSION or Sqlx.INTERSECTION 
                or Sqlx.LAST or Sqlx.MAX or Sqlx.MIN or Sqlx.RESTRICT or Sqlx.STDDEV_POP 
                or Sqlx.STDDEV_SAMP or Sqlx.SOME or Sqlx.SUM or Sqlx.XMLAGG => true,
                _ => false,
            };
        }
    }

    /// <summary>
    /// The Parser converts this n-ary function to a binary one
    /// 
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long dp, Context cx, SqlValue op1, SqlValue op2)
            : base(dp, cx, Sqlx.COALESCE, null, op1, op2, Sqlx.NO)
        {
            cx.Add(this);
        }
        protected SqlCoalesce(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator +(SqlCoalesce et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCoalesce)et.New(m + x);
        }
        public static SqlCoalesce operator +(SqlCoalesce et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlCoalesce)et.New(m + (dp, ob));
        }
        internal override TypedValue _Eval(Context cx)
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
    
    internal class SqlTypeUri : SqlFunction
    {
        internal SqlTypeUri(long dp, Context cx, SqlValue op1)
            : base(dp, cx, Sqlx.TYPE_URI, null, op1, null, Sqlx.NO)
        {
            cx.Add(this);
        }
        protected SqlTypeUri(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeUri operator +(SqlTypeUri et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTypeUri)et.New(m + x);
        }
        public static SqlTypeUri operator +(SqlTypeUri et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlTypeUri)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
        {
            TypedValue v = TNull.Value;
            if (cx.obs[op1] is DBObject ob)
                v = ob.Eval(cx);
            if (v == TNull.Value)
                return domain.defaultValue;
            var st = v.dataType;
            if (st.name != null)
                return new TChar(st.name);
            return domain.defaultValue;
        }
        public override string ToString()
        {
            return "TYPE_URI(..)";
        }
    }
    /// <summary>
    ///     /
    /// </summary>
    internal class SqlStar : SqlValue
    {
        public readonly long prefix = -1L; // SqlValue
        internal SqlStar(long dp, Context cx, long pf) 
            : base(new Ident("*",new Iix(dp)),BList<Ident>.Empty, cx, Domain.Content)
        {
            prefix = pf;
            cx.Add(this);
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
        public static SqlStar operator +(SqlStar et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlStar)et.New(m + x);
        }
        public static SqlStar operator +(SqlStar et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (SqlStar)et.New(m + (dp, ob));
        }
        internal override TypedValue _Eval(Context cx)
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
            if ((cx.obs[prefix] ?? fm) is not RowSet rq)
                throw new PEException("PE49201");
            var dr = rq.display;
            if (dr == 0)
                dr = rq.rowType.Length;
            for (var c = rq.rowType.First(); c != null && c.key() < dr; c = c.Next())
                if (c.value() is long p && cx.obs[p] is DBObject ob)
                {
                    if (cx.obs[p] is DBObject nv)
                        ob = nv;
                    vs += cx.Add(ob);
                }
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
        internal override bool Verify(Context cx)
        {
            return true;
        }
    }
    /// <summary>
    /// Quantified Predicate subclass of SqlValue
    /// 
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
            : base(dp,_Mem(cx,w,s) + (What,w.defpos)+(Op,o)+(All,a)+(_Select,s.defpos)) 
        {
            cx.Add(this);
        }
        protected QuantifiedPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,RowSet s)
        {
            var m = BTree<long, object>.Empty;
            var ag = w.IsAggregation(cx,s.aggs) +s.aggs;
            var dm = Domain.Bool;
            if (ag!=CTree<long,bool>.Empty)
                m += (Domain.Aggs, ag);
            return m;
        }
        public static QuantifiedPredicate operator +(QuantifiedPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (QuantifiedPredicate)et.New(m + x);
        }

        public static QuantifiedPredicate operator +(QuantifiedPredicate e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (QuantifiedPredicate)e.New(m + (p, o));
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
                r = cx.Add(r, What, nw);
            var ns = cx.Fix(select);
            if (ns != select)
                r = cx.Add(r, _Select, ns);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (QuantifiedPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what,so,sv);
            if (wh != what)
                r +=(cx, What, wh);
            var se = cx.ObReplace(select, so, sv);
            if (se != select)
                r +=(cx, _Select, se);
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
                r += (cx, What, a.defpos);
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
        internal override TypedValue _Eval(Context cx)
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
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[what] is SqlValue w)
            {
                r += w.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(what);
            }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            BList<DBObject> ls;
            var rs = (RowSet?)cx.obs[select] ?? throw new PEException("PE1900");
            if (cx.obs[what] is SqlValue w)
            {
                (ls, m) = w.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=w.defpos)
                {
                    r = new QuantifiedPredicate(defpos, cx, nv, op, all, rs);
                    cx.Replace(this, r);
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
    /// 
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
        internal BetweenPredicate(long dp, Context cx, SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(dp, _Mem(cx, w, a, h)
                  + (QuantifiedPredicate.What, w.defpos) + (QuantifiedPredicate.Between, b)
                  + (QuantifiedPredicate.Low, a.defpos) + (QuantifiedPredicate.High, h.defpos))
        {
            cx.Add(this);
        }
        protected BetweenPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(Context cx,SqlValue w,SqlValue a,SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = w.IsAggregation(cx,CTree<long,bool>.Empty);
            if (a != null)
                ag += a.IsAggregation(cx, CTree<long, bool>.Empty);
            if (b != null)
                ag += b.IsAggregation(cx, CTree<long, bool>.Empty);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm);
            m += (_Depth,_Depths(w, a, b));
            return m;
        }
        public static BetweenPredicate operator +(BetweenPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (BetweenPredicate)et.New(m + x);
        }
        public static BetweenPredicate operator +(BetweenPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (BetweenPredicate)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BetweenPredicate(defpos,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool> ags)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is SqlValue wh)
                r += wh.IsAggregation(cx,ags);
            if (cx.obs[low] is SqlValue lw)
                r += lw.IsAggregation(cx,ags); 
            if (cx.obs[high] is SqlValue hi)
                r += hi.IsAggregation(cx,ags);
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (BetweenPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r +=(cx, QuantifiedPredicate.What, wh);
            var lw = cx.ObReplace(low, so, sv);
            if (lw != low)
                r +=(cx, QuantifiedPredicate.Low, lw);
            var hg = cx.ObReplace(high, so, sv);
            if (hg != high)
                r +=(cx, QuantifiedPredicate.High, hg);
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
                return new CTree<long, Domain>(defpos, domain);
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
                    r += (cx,QuantifiedPredicate.What, a.defpos);
            }
            if (cx.obs[r.low] is SqlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.low)
                    r += (cx,QuantifiedPredicate.Low, a.defpos);
            }
            if (cx.obs[r.high] is SqlValue ho)
            {
                var a = ho.AddFrom(cx, q);
                if (a.defpos != r.high)
                    r += (cx,QuantifiedPredicate.High, a.defpos);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[what] is not SqlValue wh || cx.obs[low] is not SqlValue lo ||
                    cx.obs[high] is not SqlValue hi)
                throw new PEException("PE43342");
            if (wh.Eval(cx) is TypedValue w && w!=TNull.Value)
            {
                if (lo.Eval(cx) is TypedValue lw && lw!=TNull.Value)
                {
                    if (wh.domain.Compare(w, wh.domain.Coerce(cx,lw)) < 0)
                        return TBool.False;
                    if (hi.Eval(cx) is TypedValue hg && hg!=TNull.Value)
                        return TBool.For(wh.domain.Compare(w, wh.domain.Coerce(cx,hg)) <= 0);
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
                }
            }
            if (l != SqlNull.Value)
            {
                (ls, m) = l.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != l.defpos)
                {
                    ch = true; l = nv;
                }
            }
            if (h != SqlNull.Value)
            {
                (ls, m) = h.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != h.defpos)
                {
                    ch = true; h = nv;
                }
            }if (ch)
            {
                r = new BetweenPredicate(defpos, cx, w, between, l, h);
                cx.Replace(this, r);
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
    /// 
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
                  +(_Domain,Domain.Bool)+(_Depth,_Depths(a,b,e)))
        {
            cx.Add(this);
        }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b,SqlValue? e)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = a.IsAggregation(cx, CTree<long, bool>.Empty) + b.IsAggregation(cx, CTree<long, bool>.Empty);
            if (e != null)
            {
                m = m + (Escape, e.defpos) + (e.defpos, true);
                ag += e.IsAggregation(cx, ag);
            }
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm);
            return m;
        }
        public static LikePredicate operator +(LikePredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (LikePredicate)et.New(m + x);
        }
        public static LikePredicate operator +(LikePredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (LikePredicate)et.New(m + (dp, ob));
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LikePredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(left, so, sv);
            if (wh != left)
                r +=(cx, Left, wh);
            var rg = cx.ObReplace(right, so, sv);
            if (rg != right)
                r +=(cx, Right, rg);
            var esc = cx.ObReplace(escape, so, sv);
            if (esc != escape)
                r +=(cx, Escape, esc);
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
                    r += (cx, Escape, a.defpos);
            }
            if (cx.obs[r.left] is SqlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.left)
                    r += (cx, Left, a.defpos);
            }
            if (cx.obs[r.right] is SqlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (cx, Right, a.defpos);
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
                return new CTree<long, Domain>(defpos, domain);
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
        internal override TypedValue _Eval(Context cx)
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
            BList<DBObject> ls;
            if (lf != SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                }
            }
            if (es != SqlNull.Value)
            {
                (ls, m) = es.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != es.defpos)
                {
                    ch = true; es = nv;
                }
            }
            if (ch)
            {
                r = new LikePredicate(defpos, cx, lf, like, rg, es);
                cx.Replace(this, r);
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
    /// 
    /// </summary>
    internal class InPredicate : SqlValue
    {
        public long what => (long)(mem[QuantifiedPredicate.What]??-1L);
        /// <summary>
        /// In, or not in, a tree of values, a rowset or a collection-valued scalar expression
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// The rowset option 
        /// </summary>
        public long select => (long)(mem[QuantifiedPredicate._Select]??-1L); // or
        /// <summary>
        /// The tree of values option
        /// </summary>
        public BList<long?> vals => (BList<long?>)(mem[QuantifiedPredicate.Vals]??BList<long?>.Empty);
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public InPredicate(long dp,Context cx, SqlValue w, BList<SqlValue>? vs = null) 
            : base(dp, _Mem(cx,w,vs))
        {
            cx.Add(this);
        }
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,BList<SqlValue>? vs)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var cs = BList<long?>.Empty;
            var ag = w.IsAggregation(cx,CTree<long,bool>.Empty);
            m += (QuantifiedPredicate.What, w.defpos);
            if (ag!=CTree<long,bool>.Empty)
                m += (Domain.Aggs, ag);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            if (vs != null)
            {
                for (var b = vs.First(); b != null; b = b.Next())
                {
                    var s = b.value();
                    cs += s.defpos;
                    ag += s.IsAggregation(cx,ag);
                }
                m += (QuantifiedPredicate.Vals, cs);
                m += (_Depth,_Depths(vs));
            }
            m += (_Domain, dm);
            return cx.DoDepth(m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE43350");
            return w.IsAggregation(cx,ags);
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
                if (cx.obs[that.val] is SqlValue vl && !vl.Eval(cx).Contains(w.Eval(cx)))
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
        public static InPredicate operator +(InPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (InPredicate)et.New(m + x);
        }
        public static InPredicate operator +(InPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            switch (dp)
            {
                case QuantifiedPredicate.Vals:
                    m += (_Depth,cx._DepthBV((BList<long?>)ob, et.depth));
                    break;
                default:
                    {
                        if (ob is long p && cx.obs[p] is DBObject bb)
                        {
                            d = Math.Max(bb.depth + 1, d);
                            if (d > et.depth)
                                m += (_Depth, d);
                        }
                        break;
                    }
            }

            return (InPredicate)et.New(m + (dp, ob));
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
                r = cx.Add(r, QuantifiedPredicate.What, nw);
            var ns = cx.Fix(select);
            if (select!=ns)
                r = cx.Add(r, QuantifiedPredicate._Select, ns);
            var nv = cx.FixLl(vals);
            if (vals!=nv)
                r = cx.Add(r, QuantifiedPredicate.Vals, nv);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (InPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r +=(cx, QuantifiedPredicate.What, wh);
            var wr = cx.ObReplace(select, so, sv);
            if (wr != select)
                r +=(cx, QuantifiedPredicate._Select, wr);
            var vs = vals;
            for (var b = vs.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.ObReplace(p, so, sv);
                    if (v != b.value())
                        vs += (b.key(), v);
                }
            if (vs != vals)
                r +=(cx, QuantifiedPredicate.Vals, vs);
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
                r += (cx,QuantifiedPredicate.What, a.defpos);
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
                r += (cx,QuantifiedPredicate.Vals, vs);
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
                return new CTree<long, Domain>(defpos, domain);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[what] is not SqlValue w)
                throw new PEException("PE49503");
            var tv = w.Eval(cx);
            if (cx.obs[val] is SqlValue se && se.Eval(cx).Contains(tv))
                return TBool.For(found);
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
            SqlValue w = (SqlValue?)cx.obs[what] ?? SqlNull.Value,
                s = (SqlValue?)cx.obs[select]??SqlNull.Value;
            if (w != SqlNull.Value)
            {
                (ls, m) = w.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=w.defpos)
                {
                    ch = true; w = nv;
                }
            }
            if (s!=SqlNull.Value)
            {
                (ls, m) = s.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != s.defpos)
                {
                    ch = true; s = nv;
                }
            }
            for (var b = vals?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; v = nv;
                    }
                    vs += v;
                }
            if (ch)
            {
                r = new InPredicate(defpos, cx, w, vs);
                if (s!=SqlNull.Value)
                    r += (cx,QuantifiedPredicate._Select, s.defpos);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Uid(what));
            if (!found)
                sb.Append(" not");
            sb.Append(" in ");
            if (val >= 0)
                sb.Append(Uid(val));
            else if (vals != BList<long?>.Empty)
            {
                var cm = "(";
                for (var b = vals.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(Uid(p));
                    }
            }
            else
            {
                sb.Append('('); sb.Append(Uid(select));
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// MemberPredicate is a subclass of SqlValue
    /// 
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
            : base(dp, _Mem(cx,a,b)+(Lhs,a)+(Found,f)+(Rhs,b)+(_Depth,_Depths(a,b)))
        {
            cx.Add(this);
        }
        protected MemberPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = a.IsAggregation(cx, CTree<long, bool>.Empty) + b.IsAggregation(cx, CTree<long, bool>.Empty);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm);
            return m;
        }
        public static MemberPredicate operator +(MemberPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (MemberPredicate)et.New(m + x);
        }
        public static MemberPredicate operator +(MemberPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (MemberPredicate)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MemberPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new MemberPredicate(dp,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49504");
            return lh.IsAggregation(cx,ags) + rh.IsAggregation(cx,ags);
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
                r = cx.Add(r, Rhs, rhs);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (MemberPredicate)base._Replace(cx, so, sv);
            var lf = cx.ObReplace(lhs,so,sv);
            if (lf != left)
                r +=(cx, Lhs, lf);
            var rg = cx.ObReplace(rhs,so,sv);
            if (rg != rhs)
                r +=(cx, Rhs, rg);
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
                r += (cx,Lhs, a.defpos);
            a = rh.AddFrom(cx, q);
            if (a.defpos != r.rhs)
                r += (cx,Rhs, a.defpos);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[lhs]?.Eval(cx) is TypedValue a && cx.obs[rhs]?.Eval(cx) is TypedValue b)
            {
                if (b == TNull.Value)
                    return domain.defaultValue;
                if (a == TNull.Value)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                if (cx.db is not null)
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
            if (cx.obs[lhs] is not SqlValue lh || cx.obs[rhs] is not SqlValue rh)
                throw new PEException("PE49516");
            var r = CTree<long, Domain>.Empty;
            var y = true;
            r += lh.KnownFragments(cx, kb, ambient);
            y = y && r.Contains(lhs);
            r += rh.KnownFragments(cx, kb, ambient);
            y = y && r.Contains(rhs);
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            SqlValue lh = (SqlValue?)cx.obs[lhs] ?? SqlNull.Value,
                rh = (SqlValue?)cx.obs[rhs] ?? SqlNull.Value;
            if (lh != SqlNull.Value)
            {
                (ls, m) = lh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != lh.defpos)
                {
                    ch = true; lh = nv;
                }
            }
            if (rh != SqlNull.Value)
            {
                (ls, m) = rh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rh.defpos)
                {
                    ch = true; rh = nv;
                }
            }
            if (ch)
            {
                r = new MemberPredicate(defpos, cx, lh, found, rh);
                cx.Replace(this, r);
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
    /// 
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
        /// the right operand: a tree of Domain
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
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool) 
                  + (_Depth,_Dep(a,r))+(MemberPredicate.Lhs,a.defpos)+(MemberPredicate.Found,f)
                  +(MemberPredicate.Rhs,r))
        {  }
        protected TypePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static int _Dep(SqlValue a,BList<Domain> r)
        {
            var d = a.depth+1;
            for (var b = r.First(); b != null; b = b.Next())
                d = Math.Max(b.value().depth + 1, d);
            return d;
        }
        public static TypePredicate operator +(TypePredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (TypePredicate)et.New(m + x);
        }
        public static TypePredicate operator +(TypePredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (TypePredicate)et.New(m + (dp, ob));
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
                r = cx.Add(r, MemberPredicate.Lhs, nl);
            var nr = cx.FixBD(rhs);
            if (nr!=rhs)
                r = cx.Add(r, MemberPredicate.Rhs, nr);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TypePredicate)base._Replace(cx, so, sv);
            var lh = cx.ObReplace(lhs, so, sv);
            if (lh != lhs)
                r +=(cx, MemberPredicate.Lhs, lh);
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
                r += (cx, MemberPredicate.Lhs, a.defpos);
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[lhs] is not SqlValue v)
                throw new PEException("PE23203");
            var a = v.Eval(cx);
            if (a == TNull.Value)
                return TNull.Value;
            bool b = false;
            var at = a.dataType;
            for (var t =rhs.First();t is not null;t=t.Next())
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
            if (cx.obs[lhs] is SqlValue lh)
            {
                (ls, m) = lh.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != lh.defpos)
                {
                    r = new TypePredicate(defpos, lh, found, rhs);
                    cx.Replace(this, r);
                }
            }
            return (new BList<DBObject>(r), m);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// 
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        internal Sqlx op => (Sqlx)(mem[SqlValueExpr.Op] ?? Sqlx.NO);
        public PeriodPredicate(long dp,Context cx,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,_Mem(cx,op1,op2)+(SqlValueExpr.Op,o)+(_Depth,_Depths(op1,op2)))
        {
            cx.Add(this);
        }
        protected PeriodPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = CTree<long, bool>.Empty;
            if (a != null)
            {
                m += (Left, a.defpos);
                ag += a.IsAggregation(cx, CTree<long, bool>.Empty);
            }
            if (b != null)
            {
                m += (Right, b.defpos);
                ag += b.IsAggregation(cx, CTree<long, bool>.Empty);
            }
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm);
            return m;
        }
        public static PeriodPredicate operator +(PeriodPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (PeriodPredicate)et.New(m + x);
        }
        public static PeriodPredicate operator +(PeriodPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (PeriodPredicate)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodPredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new PeriodPredicate(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (PeriodPredicate)base._Replace(cx, so, sv);
            var a = cx.ObReplace(left, so, sv);
            if (a != left)
                r +=(cx, Left, a);
            var b = cx.ObReplace(right, so, sv);
            if (b != right)
                r +=(cx, Right, b);
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
                r += (cx,Left, a.defpos);
            a = rg.AddFrom(cx, q);
            if (a.defpos != r.right)
                r += (cx,Right, a.defpos);
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
                (SqlValue)cx.Add(new PeriodPredicate(cx.GetUid(), cx, nl, op, nr));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is PeriodPredicate that)
            {
                if (op != that.op)
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
            SqlValue lf = (SqlValue?)cx.obs[left] ?? SqlNull.Value,
                rg = (SqlValue?)cx.obs[right] ?? SqlNull.Value;
            BList<DBObject> ls;
            if (lf != SqlNull.Value)
            {
                (ls, m) = lf.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value)
            {
                (ls, m) = rg.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                }
            }
            if (ch)
            {
                r = new PeriodPredicate(defpos, cx, lf, op, rg);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(left);
            sb.Append(' '); sb.Append(op); sb.Append(' ');
            sb.Append(right);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A base class for RowSetPredicates such as ANY
    /// 
    /// </summary>
    internal abstract class RowSetPredicate : SqlValue
    {
        internal const long
            RSExpr = -363; // long RowSet
        public long expr => (long)(mem[RSExpr]??-1);
        /// <summary>
        /// the base query
        /// </summary>
        public RowSetPredicate(long dp,Context cx,RowSet e) 
            : base(dp,_Mem(e)+(RSExpr,e.defpos))
        { }
        protected RowSetPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(RowSet e)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = e.aggs;
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)dm.New(dm.mem + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m = m + (_Domain, dm) + (_Depth, e.depth + 1);
            return m;
        }
        public static RowSetPredicate operator +(RowSetPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (RowSetPredicate)et.New(m + x);
        }
        public static RowSetPredicate operator +(RowSetPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (RowSetPredicate)et.New(m + (dp, ob));
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
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSetPredicate)base._Replace(cx, so, sv);
            var e = cx.ObReplace(expr,so,sv);
            if (e != expr)
                r += (cx,RSExpr, e);
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
    /// 
    /// </summary>
    internal class ExistsPredicate : RowSetPredicate
    {
        public ExistsPredicate(long dp,Context cx,RowSet e) : base(dp,cx,e)
        {
            cx.Add(this);
        }
        protected ExistsPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ExistsPredicate operator +(ExistsPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ExistsPredicate)et.New(m + x);
        }
        public static ExistsPredicate operator +(ExistsPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (ExistsPredicate)et.New(m + (dp, ob));
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
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[expr] is not RowSet e)
                throw new PEException("PE49546");
            return TBool.For(e.First(cx) is not null);
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
    /// 
    /// </summary>
    internal class UniquePredicate : RowSetPredicate
    {
        public UniquePredicate(long dp,Context cx,RowSet e) : base(dp,cx,e)
        {
            cx.Add(this);
        }
        protected UniquePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UniquePredicate operator +(UniquePredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (UniquePredicate)et.New(m + x);
        }
        public static UniquePredicate operator +(UniquePredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (UniquePredicate)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UniquePredicate(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new UniquePredicate(dp,m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (UniquePredicate)base._Replace(cx, so, sv);
            var ex = (RowSet)cx._Replace(expr, so, sv);
            if (ex.defpos != expr)
                r +=(cx, RSExpr, ex.defpos);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            var rs = ((RowSet?)cx.obs[expr]);
            if (rs == null)
                return TBool.False;
            RTree a = new (rs.defpos,cx,rs, TreeBehaviour.Disallow, TreeBehaviour.Disallow);
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
    /// 
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
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool)
                  + (NVal,v.defpos)+(NIsNull,b) +(_Depth,v.depth+1))
        { }
        protected NullPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static NullPredicate operator +(NullPredicate et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (NullPredicate)et.New(m + x);
        }
        public static NullPredicate operator +(NullPredicate et, (Context, long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (cx, dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is long p && cx.obs[p] is DBObject bb)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (NullPredicate)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new NullPredicate(defpos,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            if (cx.obs[val] is not SqlValue v)
                throw new PEException("PE49550");
            return v.IsAggregation(cx,ags);
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
                r += (cx, NVal, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nv = cx.Fix(val);
            if (nv != val)
                r = cx.Add(r, NVal, nv);
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (NullPredicate)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(val,so,sv);
            if (vl != val)
                r +=(cx, NVal, vl);
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
        internal override TypedValue _Eval(Context cx)
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
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (val >= 0 && cx.obs[val] is SqlValue v)
            {
                r += v.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(val);
            }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
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
            BList<DBObject> ls;
            if (cx.obs[val] is SqlValue s)
            {
                (ls, m) = s.Resolve(cx, f, m);
                if (ls[0] is SqlValue nv && nv.defpos!=s.defpos)
                {
                    r = new NullPredicate(defpos, nv, isnull);
                    cx.Replace(this, r);
                }
            }
            return (new BList<DBObject>(r), m);
        }
        public override string ToString()
        {
            return isnull?"is null":" is not null";
        }
    }
    /// <summary>
    /// SqlNode will evaluate to a TNode (and SqlEdge to a TEdge) once the enclosing
    /// CreateStatement or MatchStatement has been Obeyed.
    /// In general, any of the contained SqlValues in an SqlNode may evaluate to via a TGParam 
    /// that should have been bound by MatchStatement.Obey.
    /// However, TGParams are not found in CreateStatement graphs.
    /// CreateStatement.Obey will traverse its GraphExpression so that the context maps SqlNodes to TNodes.
    /// MatchStatement.Obey will traverse its GraphExpression binding as it goes, so that the dependent executable
    /// is executed only for fully-bound SqlNodes.
    /// </summary>
    internal class SqlNode : SqlValue
    {
        internal const long
            DocValue = -477,    // BTree<long,long?> SqlValue -> SqlValue
            IdValue = -480,     // long             SqlValue of Int
            LabelValue = -476,  // BList<long?>     SqlValue of TypeSpec (most-specific is last) -> TTypeSpec
            State = -245;       // CTree<long,TGParam> tgs in this SqlNode  (always empty for CreateStatement)
        public BTree<long, long?>? docValue => (BTree<long, long?>?)mem[DocValue];
        public long idValue => (long)(mem[IdValue] ?? -1L);
        public BList<long?> label => 
            (BList<long?>)(mem[LabelValue]??BList<long?>.Empty);
        public CTree<long, bool> search => // can occur in Match GraphExp
            (CTree<long, bool>)(mem[RowSet._Where] ?? CTree<long, bool>.Empty);
        public CTree<long, TGParam> state =>
            (CTree<long, TGParam>)(mem[State] ?? CTree<long, TGParam>.Empty);
        public Sqlx tok => (Sqlx)(mem[SqlValueExpr.Op] ?? Sqlx.Null);
        public SqlNode(Ident nm, BList<Ident> ch,Context cx, long i, BList<long?> l, BTree<long, long?>? d, 
            CTree<long,TGParam> tgs,NodeType? dm=null,BTree<long,object>? m = null) 
            : base(nm, ch, cx, dm??((m is null)?Domain.NodeType:Domain.EdgeType), _Mem(i,l,d,tgs,m))
        {  }
        protected SqlNode(long dp, BTree<long, object> m) : base(dp, m)
        {  }
        static BTree<long,object> _Mem(long i, BList<long?> l,BTree<long,long?>? d,CTree<long,TGParam> tgs,
            BTree<long,object>?m)
        {
            m ??= BTree<long, object>.Empty;
            if (i > 0)
                m += (IdValue, i);
            if (l!=BList<long?>.Empty)
                m += (LabelValue, l);
            if (d is not null)
                m += (DocValue, d);
            m += (State, tgs);
            return m;
        }
        public static SqlNode operator+(SqlNode n,(long,object)x)
        {
            return (SqlNode)n.New(n.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlNode(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlNode(dp, m);
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.values[defpos] ?? cx.binding[idValue] ?? TNull.Value;
        }
        internal NodeType? MostSpecificType(Context cx)
        {
            return cx.GType(label.Last()?.value());
        }
        internal virtual int MinLength(Context cx)
        {
            return 0;
        }
        internal virtual SqlNode Add(Context cx, SqlNode? an,CTree<long,TGParam> tgs)
        {
            return (SqlNode)cx.Add(this + (State,tgs + state));
        }
        protected virtual (NodeType,CTree<Sqlx,TypedValue>) 
            _NodeType(Context cx, CTree<string,SqlValue> ls, NodeType dt)
        {
            var nd = this;
            // a label with at least one char SqlValue must be here
            // evaluate them all as TTypeSpec or TChar
            var tl = nd.label;
            var ty = CTree<int, TypedValue>.Empty;
            NodeType? nt = null; // the node type of this node when we find it or construct it
            TableColumn? iC = null; // the special columns for this node
                                    // The label part gives us our bearings in the database. From it we will find
                                    // how to interpret identifiers and name properties (including structural properties)
                                    // - or maybe the database is wholly or relatively empty in which case we only
                                    // have the standard names and syntax.
            var md = CTree<Sqlx, TypedValue>.Empty; // some of what we will find on this search
                                                    // Begin to think about the names of special properties for the node we are building
                                                    // We may be using the default names, or we may inherit them from existing types
            if (tl == BList<long?>.Empty)
                return (dt, md);
            var sd = "ID";
            // the first part of the label is the least specific type (it may exist already).
            // it may be that all node/edge types for all parts of the label exist already
            // certainly the predecessor of an existing node must exist.
            // if the last one is undefined we will build it using the given property tree
            // (if it is defined we may add properties to it)
            // if types earlier in the label are unbdefined we will create them here
            // This loop traverses the given type label: watch for the position of the last component
            ABookmark<int, long?>? bn = null;
            for (var b = tl.First(); b != null; b = bn)  // tl is the iterative type label
                if (b.value() is long tp && cx.obs[tp] is SqlValue gl && gl.Eval(cx) is TypedValue gv)
                {
                    bn = b.Next();
                    ty += (b.key(), gv);
                    if (gv is not TChar gc)
                        gc = new TChar(gl.name ?? "?");
                    NodeType? gt = (gv is TTypeSpec tt)?tt._dataType as NodeType:
                        cx.db.objects[cx.role.dbobjects[gc.value] ?? -1L] as NodeType;
                    if (gt is not null && iC is null)
                    {
                        iC = cx.db.objects[gt.idCol] as TableColumn ?? throw new DBException("42105");
                        sd = iC.infos[cx.role.defpos]?.name ?? throw new DBException("42105");
                        dt = gt;
                    }
                    if (gt is null)
                    {
                        gt = new NodeType(cx.GetUid(), gc.value, (UDType)gl.domain, nt, cx);
                        md += (Sqlx.NODE, new TChar(sd));
                        if (bn != null) // Immediately build a type if not the last
                        {
                            if (cx.db.objects[cx.role.defpos] is Role rr
                                && cx.db.objects[rr.dbobjects[sd] ?? -1L] is NodeType ht)
                                gt = ht;
                            else
                            (gt, _) = gt.Build(cx, dt, ls, md);
                            ls = CTree<string, SqlValue>.Empty;
                        }
                    }
                    ty += (b.key(), new TTypeSpec(gt));
                    nt = gt;
                    nd += (_Domain, nt);
                    cx.obs += (defpos, nd);
                }
            if (nt is null)
                throw new DBException("42000","_NodeType");
            return (nt,md);
        }
        internal virtual SqlNode Create(Context cx, NodeType dt)
        {
            var nd = this;
            var ls = CTree<string, SqlValue>.Empty;
            for (var b = docValue?.First(); b != null; b = b.Next())
                if (cx.obs[b.value() ?? -1L] is SqlValue sv)
                {
                    if (cx.obs[b.key()] is not SqlValue sk) throw new DBException("42000","Create");
                    var k = (sk.name != null && sk.name != "COLON" && sk is SqlValue) ? sk.name
                        : sk.Eval(cx).ToString();
                    ls += (k, sv);
                }
            var (nt, md) = _NodeType(cx, ls, dt);
            if (nt.defpos < 0 && md == CTree<Sqlx, TypedValue>.Empty && ls == CTree<string, SqlValue>.Empty)
                throw new DBException("42161", "Specification",name??"Unbound");
            // we now have our NodeType nt and the names sd, il and ia of special columns
            // if nt is not built in the database yet we have the metadata we need to build it.
            // At this point, non-special properties matching those from dt
            // or new ones, may be provided in a Document. We prepare a tree
            for (var lb = dt.pathDomain.First(); lb != null; lb = lb.Next())
                if (lb.value() is long pl && cx.obs[pl] is TableColumn tc
                        && tc.infos[cx.role.defpos] is ObInfo ti && ti.name is string cn)
                    ls += (cn, new SqlLiteral(pl, cn, tc.domain.defaultValue, tc.domain));
            for (var b = nd.docValue?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue sf && cx.obs[b.value() ?? -1L] is SqlValue sv)
                {
                    var fn = (sf.Eval(cx) as TChar)?.value ?? sf.name ?? "?";
                    ls += (fn, sv);
                }
            // We are now ready to check or build the node type nt
            if (nt.defpos >= 0 && nt.defpos < Transaction.Analysing)
                nt = nt.Check(cx, ls);
            else if (cx.db.objects[cx.role.defpos] is Role rr
                                && cx.db.objects[rr.dbobjects[nt.name ?? "_"] ?? -1L] is NodeType ot)
                nt = ot;
            else
                (nt, ls) = nt.Build(cx, dt, ls, md); // dt is supertype
            nd += (_Domain, nt);
            nd = (SqlNode)cx.Add(nd);
            ls = nd._AddEnds(cx, ls);
            var sd = ((md[Sqlx.NODE] ?? md[Sqlx.EDGE]) as TChar)?.value ?? "ID";
            TNode? tn = null;
            if (nt.FindPrimaryIndex(cx) is Index px && ls[sd]?.Eval(cx) is TInt kk &&
                px.rows?.impl?[kk] is TInt t5 && nt.tableRows[t5.value] is TableRow tr)
            {
                tn = new TNode(cx, nt, tr);
                nd += (SqlLiteral._Val, tn);
                cx.values += (nd.defpos, tn);
            }
            else if (nt.defpos>0)
            {
                var vp = cx.GetUid();
                var ts = new TableRowSet(cx.GetUid(), cx, nt.defpos);
                var ll = BList<DBObject>.Empty;
                var iC = BList<long?>.Empty;
                var tb = ts.First();
                for (var bb = ts.rowType.First(); bb != null && tb != null; bb = bb.Next(), tb = tb.Next())
                    if (bb.value() is long bq && cx.NameFor(bq) is string n9
                        && ls[n9] is SqlValue sv && sv is not SqlNull)
                    {
                        ll += sv;
                        iC += tb.value();
                    }
                // ll generally has fewer columns than nt
                // carefully construct what would happen with ordinary SQL INSERT VALUES
                // we want dm to be constructed as having a subset of fm's columns using fm's iSMap
                var dr = BList<long?>.Empty;
                var ds = CTree<long, Domain>.Empty;
                for (var b = ll.First(); b != null; b = b.Next())
                    if (b.value() is SqlValue sv && sv.defpos > 0)
                    {
                        dr += sv.defpos;
                        ds += (sv.defpos, sv.domain);
                    }
                var fm = (TableRowSet)ts.New(cx.GetUid(), ts.mem + (Domain.RowType, dr) + (Domain.Display, dr.Length));
                var dm = new Domain(-1L, cx, Sqlx.ROW, ds, dr);
                cx.Add(fm);
                var rn = new SqlRow(cx.GetUid(), cx, dm, dm.rowType) + (cx, Table._NodeType, nt.defpos);
                cx.Add(rn);
                SqlValue n = rn;
                n = new SqlRowArray(vp, cx, dm, new BList<long?>(n.defpos));
                var sce = n.RowSetFor(vp, cx, fm.rowType, fm.representation)
                    + (cx, RowSet.RSTargets, fm.rsTargets)
                    + (RowSet.Asserts, RowSet.Assertions.AssignTarget);
                var s = new SqlInsert(cx.GetUid(), fm, sce.defpos, ts + (Domain.RowType, iC));
                cx.Add(s);
                var np = cx.db.nextPos;
                // NB: The TargetCursor/trigger machinery will place values in cx.values in the !0.. range
                // From the point of view of graph operations these are spurious, and should not be accessed
                // The only exception is to retrieve the value of tn
                s.Obey(cx);
                if (nd.name != null)
                    cx.defs += (nd.name, new Iix(nd.defpos), Ident.Idents.Empty);
                tn = cx.values[np] as TNode;
            }
            if (tn is not null)
            {
                nd += (SqlLiteral._Val, tn);
                cx.values += (nd.defpos, tn);
            }
            cx.Add(nd);
            return nd;
        }
        protected virtual CTree<string,SqlValue> _AddEnds(Context cx,CTree<string,SqlValue> ls)
        {
            if (domain is NodeType nt && cx.obs[idValue] is SqlNode il
                    && cx.NameFor(nt.idCol) is string iC && !ls.Contains(iC))
            {
                if (il.Eval(cx) is TNode tn)
                    ls += (iC, (SqlValue)cx.Add(new SqlLiteral(cx.GetUid(), tn.id)));
                else
                    ls += (iC, SqlNull.Value);
            }
            return ls;
        } 
        internal bool CheckProps1(Context cx,TNode n)
        {
            if (cx.binding[idValue] is TNode tn && tn.tableRow.defpos!=n.tableRow.defpos)
                return false;
            return CheckProps(cx, n);
        }
        /// <summary>
        /// This method is called during Match, so this.domain is not helpful.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal bool CheckProps(Context cx, TNode n)
        {
            if (this is not SqlEdge && n is TEdge)
                return false;
            if (cx.binding[idValue] is TNode ni && ni.id != n.id)
                return false;
            if (n.dataType.infos[cx.role.defpos] is ObInfo oi)
                for (var b = docValue?.First(); b != null; b = b.Next())
                    if (cx.GName(b.key()) is string k)
                    {
                        if (!oi.names.Contains(k))
                            return false;
                        if (cx.GConstrain(b.value()) is TypedValue xv)
                        {
                            if (xv is TGParam tg)
                            {
                                if (state.Contains(tg.uid) || cx.binding[tg.uid] is not TypedValue vv)
                                    continue;
                                xv = vv;
                            }
                            switch (k)
                            {
                                case "ID":
                                case "LEAVING":
                                case "ARRIVING":  // no need
                                    break;
                                case "SPECIFICTYPE":
                                    if (!n.dataType.Match(xv.ToString()))
                                        return false;
                                    break;
                                default:
                                    if (oi.names[k].Item2 is long d && xv.CompareTo(n.tableRow.vals[d]) != 0)
                                        return false;
                                    break;
                            }
                        }
                    }
            for (var b = search.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue se)
                {
                    var ov = cx.values[defpos];
                    cx.values += (defpos, n);
                    cx.values += n.tableRow.vals;
                    var r = se.Eval(cx);
                    if (ov is null)
                        cx.values -= defpos;
                    else
                        cx.values += (defpos,ov);
                    if (r != TBool.True)
                        return false;
                }
            return true;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (idValue > 0)
            { sb.Append(" Id="); sb.Append(Uid(idValue)); }
            var cm = " [";
            for (var b = label.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(p));
                }
            if (cm==",") sb.Append(']');
            cm = " {";
            for (var b = docValue?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append('='); sb.Append(Uid(p));
                }
            if (cm == ",") sb.Append('}');
            if (search!=CTree<long,bool>.Empty)
            {   sb.Append(" where ["); cm = ""; 
                for (var b=search.First();b!=null;b=b.Next())
                {
                    sb.Append(cm);cm = ",";
                    sb.Append(DBObject.Uid(b.key()));
                }
                sb.Append(']');
            }
            cm = " ";
            for (var b = state.First(); b != null; b = b.Next())
                if (b.key() < 0 && b.value() is TGParam ts)
                {
                    sb.Append(cm); cm = ",";
                    var k = (Sqlx)(int)(-b.key());
                    sb.Append(k);
                    if (ts.value != null)
                    {
                        sb.Append(' '); sb.Append(ts.value);
                    }
                }
            for (var b = state.PositionAt(0); b != null; b = b.Next())
                if (b.value() is TGParam tg)
                {
                    sb.Append(cm); cm = ","; sb.Append(Uid(b.key()));
                    if (tg.value != null)
                    {
                        sb.Append(' '); sb.Append(tg.value);
                    }
                }
            return sb.ToString();
        }
    }
    internal class SqlEdge : SqlNode
    {
        internal const long
            ArrivingValue = -479,  // long     SqlValue
            LeavingValue = -478;   // long SqlValue
        public long arrivingValue => (long)(mem[ArrivingValue]??-1L);
        public long leavingValue => (long)(mem[LeavingValue]??-1L);
        public Sqlx direction => (Sqlx)(mem[SqlValueExpr.Op] ?? Sqlx.NO); // ARROWBASE or ARROW
        public SqlEdge(Ident nm, BList<Ident> ch, Context cx, Sqlx t, long i, long l, long a, BList<long?> la, 
            BTree<long, long?>? d, CTree<long, TGParam> tgs, NodeType? dm=null)
            : base(nm, ch, cx, i, la,d, tgs,dm??Domain.EdgeType, BTree<long,object>.Empty
                  +(LeavingValue,l)+(ArrivingValue,a)+(SqlValueExpr.Op,t))
        { }
        protected SqlEdge(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static SqlEdge operator+(SqlEdge e,(long,object)x)
        {
            return (SqlEdge)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (SqlEdge)New(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlEdge(dp, m);
        }
        internal override int MinLength(Context cx)
        {
            return 1;
        }
        protected override (NodeType,CTree<Sqlx,TypedValue>) 
          _NodeType(Context cx, CTree<string,SqlValue> ls, NodeType dt)
        {
            var nd = this;
            //  nd for an edge will have a specific leavingnode and a specific arrivingnode 
            // the special columns for this node
            var lS = cx.obs[nd.leavingValue] as SqlNode;
            var lI = lS?.idValue ?? -1L;
            var lg = cx.binding[lI]??cx.nodes[lI];
            var lT = lg?.dataType??cx.obs[lI]?.domain??lS?.domain;
            var lN = lT?.name;
            var aS = cx.obs[nd.arrivingValue] as SqlNode;
            var aI = aS?.idValue ?? -1L;
            var ag = cx.binding[aI]??cx.nodes[aI];
            var aT = ag?.dataType??cx.obs[aI]?.domain??aS?.domain;
            var aN = aT?.name;
            // a label with at least one char SqlValue must be here
            // evaluate them all as TTypeSpec or TChar
            var tl = nd.label;
            var ty = CTree<int, TypedValue>.Empty;
            EdgeType? nt = null; // the node type of this node when we find it or construct it
            var md = CTree<Sqlx, TypedValue>.Empty; // some of what we will find on this search
                                                    // Begin to think about the names of special properties for the node we are building
                                                    // We may be using the default names, or we may inherit them from existing types
            var sd = "ID";
            var il = "LEAVING"; 
            var ia = "ARRIVING";
            // the first part of the label is the least specific type (it may exist already).
            // it may be that all node/edge types for all parts of the label exist already
            // certainly the predecessor of an existing node must exist.
            // if the last one is undefined we will build it using the given property tree
            // (if it is defined we may add properties to it)
            // if types earlier in the label are unbdefined we will create them here
            // This loop traverses the given type label: watch for the position of the last component
            bool? retrying;
            do
            {
                retrying = false;
                ABookmark<int, long?>? bn = null;
                NodeType? gt = null;
                for (var b = tl.First(); b != null; b = bn)  // tl is the iterative type label
                    if (b.value() is long tp && cx.obs[tp] is SqlValue gl && gl.Eval(cx) is TypedValue gv)
                        try
                        {
                            bn = b.Next();
                            ty += (b.key(), gv);
                            if (gv is not TChar gc)
                                gc = new TChar(gl.name ?? "?");
                            // Take account of any prepared edge-renaming rules
                            if (lN != null && aN != null && cx.conn.edgeTypes[gc.value]?[lN]?[aN] is string nN)
                                gc = new TChar(nN);
                            gt = (gv is TTypeSpec tt)?tt._dataType as EdgeType:
                                cx.db.objects[cx.role.dbobjects[gc.value] ?? -1L] as EdgeType;
                            gt = cx.db.objects[gt?.defpos??-1L] as EdgeType ?? gt;
                            if (gt is not null)
                            {
                                if (lN is not null && lN!="" && lT is not null)
                                    
                                    CheckType(cx, lN, lT.defpos, gt, gt.leaveIx, true);
                                if (aN is not null && aN!="" && aT is not null)
                                    CheckType(cx, aN, aT.defpos, gt, gt.arriveIx, false);  
                            }
                            if (gt is null)
                            {
                                gt = new EdgeType(cx.GetUid(), gc.value, (UDType)gl.domain, nt, cx);
                                md = md + (Sqlx.EDGE, new TChar(sd))
                                    + (Sqlx.LPAREN, new TChar(il))
                                    + (Sqlx.RPAREN, new TChar(ia));
                                if (lN is not null)
                                    md += (Sqlx.RARROW, new TChar(lN));
                                if (aN is not null)
                                    md += (Sqlx.ARROW, new TChar(aN));
                                if (bn != null) // Immediately build a type if not the last
                                {
                                    if (cx.db.objects[cx.role.defpos] is Role rr
                                        && cx.db.objects[rr.dbobjects[sd] ?? -1L] is NodeType ht)
                                        gt = ht;
                                    else
                                    (gt, _) = gt.Build(cx, dt, ls, md);
                                    ls = CTree<string, SqlValue>.Empty;
                                }
                            }
                            ty += (b.key(), new TTypeSpec(gt));
                            nt = (EdgeType)gt;
                        }
                        catch (DBException ex)
                        {
                            if (ex.signal == "22G0K" 
                                && gt is not null && lN is not null && aN is not null
                                && cx.conn._tcp is TCPStream tcp && cx.conn.props.Contains("AllowAsk")) // we can maybe do something
                            {
                                tcp.Write(Responses.AskClient);
                                tcp.PutString("Suggest type for "+gt.name+"("+lN+","+aN+")");
                                var p = tcp.ReadByte();
                                if ((Protocol)p == Protocol.ClientAnswer)
                                    Console.WriteLine(tcp.GetString());
                                retrying = true;
                            }
                        }
            } while (retrying == true);
            if (nt is null)
                throw new DBException("42000","_EdgeType");
            return (nt,md);
        }
        void CheckType(Context cx, string? n, long? t, NodeType gt, long lx, bool lv)
        {
            if (cx.db.objects[lx] is not Index nx
                || cx.db.objects[nx.refindexdefpos] is not Index rx
                || cx.db.objects[rx.tabledefpos] is not NodeType ut)
                throw new PEException("PE408010");
            if (t == null)
                throw new DBException("42133", n ?? "??");
            if (t == ut.defpos)
                return;
            if (((cx.values[t ?? -1L] as TNode)?.dataType ?? cx.db.objects[t ?? -1L]) is not NodeType at
                || at.FindPrimaryIndex(cx) is not Index ax)
                throw new DBException("42105");
            var ts = BTree<long,NodeType>.Empty; // a list of supertypes of ut
            NodeType? ct = null; // a common ancestor
            for (var st = ut; st != null; st = cx.db.objects[st.super?.defpos ?? -1L] as NodeType)
                ts += (st.defpos, st);
            for (var st = at; st != null && ct is null;
                st = cx.db.objects[st.super?.defpos ?? -1L] as NodeType)
                ct = ts[st.defpos];
            if (ct is null)
                throw new DBException("22G0K");
            Domain? di = null;
            for (var b = ct.rindexes[gt.defpos]?.First(); b != null; b = b.Next())
                if (b.key()[0] == (lv ? gt.leaveCol : gt.arriveCol))
                    di = b.key();
            if (di is null || ct.FindPrimaryIndex(cx) is not Level3.Index px)
                throw new DBException("42105");
            cx.Add(new PIndex(lv?"LEAVING":"ARRIVING", gt, di, 
                    PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate| PIndex.ConstraintType.NoBuild,
                    ax.defpos, cx.db.nextPos));
            cx.Add(new AlterEdgeType(lv, ct.defpos, gt.defpos, cx.db.nextPos));
        }
        internal override SqlNode Add(Context cx, SqlNode? an, CTree<long, TGParam> tgs)
        {
            if (an is null)
                throw new DBException("22G0L");
            tgs += state;
            var r = this;
            if (an.state[an.defpos] is TGParam lg)
                if (direction == Sqlx.ARROWBASE)
                {
                    tgs += (-(int)Sqlx.ARROW, lg);
                    r += (ArrivingValue, an.defpos);
                }
                else
                {
                    tgs += (-(int)Sqlx.RARROWBASE, lg);
                    r += (LeavingValue, an.defpos);
                }
            r += (State, tgs);
            return (SqlEdge)cx.Add(r);
        }
        protected override CTree<string, SqlValue> _AddEnds(Context cx, CTree<string, SqlValue> ls)
        {
            ls = base._AddEnds(cx, ls);
            if (domain is not EdgeType et)
                return ls;
            if (cx.db.objects[et.leaveCol] is TableColumn lc
                && cx.obs[leavingValue] is SqlNode sl
                && sl.Eval(cx) is TNode ln)
            {
                var lv = ln.id;
                var li = (lc.domain.kind == Sqlx.SET) ?
                    new SqlLiteral(cx.GetUid(), new TSet(lc.domain, CTree<TypedValue, bool>.Empty + (lv, true))) :
                    new SqlLiteral(cx.GetUid(), lv);
                ls += (cx.NameFor(et.leaveCol), (SqlValue)cx.Add(li));
            }
            if (cx.db.objects[et.arriveCol] is TableColumn ac
                && cx.obs[arrivingValue] is SqlNode sa
                && sa.Eval(cx) is TNode an)
            {
                var av = an.id;
                var ai = (ac.domain.kind == Sqlx.SET) ?
                    new SqlLiteral(cx.GetUid(), new TSet(ac.domain, CTree<TypedValue, bool>.Empty + (av, true))) :
                    new SqlLiteral(cx.GetUid(), av);
                ls += (cx.NameFor(et.arriveCol), (SqlValue)cx.Add(ai));
            }
            return ls;
        }
        internal override SqlNode Create(Context cx, NodeType dt)
        {
            return base.Create(cx, dt);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (leavingValue >= 0)
            {
                sb.Append(" leaving "); sb.Append(Uid(leavingValue));
            }
            if (arrivingValue >= 0)
            {
                sb.Append(" arriving "); sb.Append(Uid(arrivingValue));
            }
            if (tok!=Sqlx.NULL)
            {
                sb.Append(' ');sb.Append(tok);
            }
            return sb.ToString();
        }
    }
    class SqlPath : SqlEdge
    {
        internal const long
            MatchQuantifier = -484, // (int,int)
            Pattern = -485; // BList<long?>
        internal BList<long?> pattern => (BList<long?>)(mem[Pattern]??BList<long?>.Empty);
        internal (int, int) quantifier => ((int, int))(mem[MatchQuantifier]??(1, 1));
        public SqlPath(Context cx, BList<long?> p, (int, int) lh, long i, long a)
            : base(cx.GetUid(),new BTree<long, object>(Pattern,p)+(MatchQuantifier, lh)
                  +(LeavingValue,i)+(ArrivingValue,a))
        { }
        protected SqlPath(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static SqlPath operator +(SqlPath e, (long, object) x)
        {
            return (SqlPath)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (SqlPath)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlPath(dp, m);
        }
        internal override int MinLength(Context cx)
        {
            var m = 0;
            for (var b = pattern.First(); b != null; b = b.Next())
                if (cx.obs[b.value()??-1L] is SqlNode p)
                    m += p.MinLength(cx);
            return quantifier.Item1 * m;
        }
        internal override SqlNode Add(Context cx, SqlNode? an, CTree<long, TGParam> tgs)
        {
            var r = this;
            tgs += state;
            SqlEdge? last = null;
            for (var b = pattern.Last(); b != null && last is null; b = b.Next())
                last = cx.obs[b.value() ?? -1L] as SqlEdge;
            if (an?.state[an.defpos] is TGParam lg)
                if (direction == Sqlx.ARROWBASE)
                {
                    tgs += (-(int)Sqlx.ARROW, lg);
                    if (last is not null)
                        cx.Add(last+(ArrivingValue, an.defpos));
                }
                else
                {
                    tgs += (-(int)Sqlx.RARROWBASE, lg);
                    if (last is not null)
                        cx.Add(last+(LeavingValue, an.defpos));
                }
            r += (State, tgs);
            return (SqlNode)cx.Add(r);
        }
        public override string ToString()
        {
            var sb= new StringBuilder(base.ToString());
            var cm = "";
            sb.Append('[');
            for (var b=pattern.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()??-1L));
            }
            sb.Append(']');
            var (l, h) = quantifier;
            sb.Append('{');sb.Append(l);sb.Append(',');sb.Append(h); sb.Append('}');
            return sb.ToString();
        }
    }
    class SqlMatch : SqlValue
    {
        internal const long
            MatchAlts = -486; // BList<long?> SqlMatchAlt
        internal BList<long?> matchAlts => (BList<long?>)(mem[MatchAlts] ?? BList<long?>.Empty);
        public SqlMatch(Context cx, BList<long?>ms) 
            : this(cx.GetUid(),new BTree<long,object>(MatchAlts,ms))
        {
        }
        protected SqlMatch(long dp, BTree<long, object> m) : base(dp, m)
        {
        }
        public static SqlMatch operator +(SqlMatch e, (long, object) x)
        {
            return (SqlMatch)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (SqlMatch)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlMatch(dp, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " [";
            for (var b = matchAlts.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value() ?? -1L));
            }
            if (cm == ",") sb.Append(']');
            return sb.ToString();
        }
    }
    class SqlMatchAlt : SqlValue
    {
        internal const long
            MatchExps = -487, // BList<long?> SqlNode
            MatchMode = -483, // Sqlx
            PathId = -488;  // long
        internal Sqlx mode => (Sqlx)(mem[MatchMode] ?? Sqlx.NONE);
        internal long pathId => (long)(mem[PathId] ?? -1L);
        internal BList<long?> matchExps => (BList<long?>)(mem[MatchExps] ?? BList<long?>.Empty);
        public SqlMatchAlt(Context cx, Sqlx m, BList<long?> p, long pp)
            : base(cx.GetUid(), new BTree<long, object>(MatchMode, m) + (MatchExps, p) + (PathId,pp))
        {
            var min = 0; // minimum path length
            var hasPath = false;
            for (var b = p.First(); b != null; b = b.Next())
            {
                if (cx.obs[b.value() ?? -1L] is SqlNode sn)
                {
                    min += sn.MinLength(cx);
                    if (sn is SqlPath sp)
                    {
                        hasPath = true;
                        if (m == Sqlx.ALL && sp.quantifier.Item2 < 0)
                            throw new DBException("22G0M");
                    }
                }
            }
            if (hasPath && min <= 0)
                throw new DBException("22G0N");
        }
        protected SqlMatchAlt(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static SqlMatchAlt operator +(SqlMatchAlt e, (long, object) x)
        {
            return (SqlMatchAlt)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (SqlMatchAlt)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlMatchAlt(dp, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mode != Sqlx.NONE)
            {
                sb.Append(' '); sb.Append(mode);
            }
            if (pathId>=0)
            {
                sb.Append(' ');sb.Append(DBObject.Uid(pathId));
            }
            var cm = " [";
            for (var b=matchExps.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()??-1L));
            }
            if (cm == ",") sb.Append(']');
            return sb.ToString();
        }
    }
}
