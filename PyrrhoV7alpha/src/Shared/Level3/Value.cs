using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// The QlValue class corresponds to the occurrence of identifiers and expressions in
    /// SELECT statements etc: they are evaluated in a RowSet or Activation Context  
    /// So Eval is a way of getting the current TypedValue for the identifier etc for the current
    /// rowset positions.
    /// SqlValues are constructed for every obs reference in the SQL source of a RowSet or Activation. 
    /// Many of these are SqlNames constructed for an identifier in a query: 
    /// during query analysis all of these must be resolved to a corresponding obs reference 
    /// (so that many SqlNames are resolved to the same thing). 
    /// An QlValue’s home context is the RowSet, Activation, or QlValue whose source defines it.
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
    internal class QlValue : DBObject,IComparable
    {
        internal const long
            Left = -308, // long QlValue 
            Right = -309, // long QlValue
            Sub = -310; // long QlValue
        internal long left => (long)(mem[Left]??-1L);
        internal long right => (long)(mem[Right]??-1L);
        internal long sub => (long)(mem[Sub]??-1L);
      //  internal long selectDepth => (long)(mem[SelectDepth] ?? -1L);
        public new string? name => (string?)mem[ObInfo.Name];
        internal virtual long target => defpos;
        internal bool scalar => (bool)(mem[RowSet._Scalar]??(domain.kind!=Qlx.TABLE && domain.kind!=Qlx.CONTENT
            && domain.kind!=Qlx.VALUE));
        public QlValue(Ident nm,Ident ic,BList<Ident> ch, Context cx,Domain dt,BTree<long,object>?m=null)
            :base(nm.uid, cx.DoDepth((m??BTree<long,object>.Empty) +(ObInfo.Name,nm.ident) 
                 + (_Ident,ic) + (_Domain,dt) + (Chain,ch) + (Scope,nm.lp)))
        {
            var lm = nm.ident;
            long? pp = null;
            long? ps = null;
            for (var b = ch.First(); b != null; b = b.Next())
            {
                pp = ps;
                ps = b.value()?.uid;
                if (cx.obs[ps ?? -1L] is ForwardReference fr)
                    cx.Add(fr + (ForwardReference.Subs, fr.subs + (defpos, true)));
                lm = b.value().ident;
            }
            if (pp is long p)
            {
                var ss = cx.defs[p] ?? Names.Empty;
                cx.defs += (p, ss + (lm, (nm.lp,nm.uid)));
            }
            cx.Add(this);
            cx.Add(lm, nm.lp, this);
        }
        public QlValue(Ident nm, BList<Ident> ch, Context cx, Domain dt, BTree<long, object>? m = null)
    : base(nm.uid, cx.DoDepth((m ?? BTree<long, object>.Empty) + (ObInfo.Name, nm.ident)
         + (_Ident, nm) + (_Domain, dt) + (Chain, ch) + (Scope,nm.lp)))
        {
            var lm = nm.ident;
            long? pp = null;
            long? ps = null;
            for (var b = ch.First(); b != null; b = b.Next())
            {
                pp = ps;
                ps = b.value()?.uid;
                if (cx.obs[ps??-1L] is ForwardReference fr)
                    cx.Add(fr + (ForwardReference.Subs, fr.subs + (defpos, true)));
                lm = b.value().ident;
            }
            if (pp is long p)
            {
                var ss = cx.defs[p] ?? Names.Empty;
                cx.defs += (p, ss + (lm, (nm.lp,nm.uid)));
            }
            cx.Add(this);
            cx.Add(lm,nm.lp,this);
        }
        internal QlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        protected QlValue(Context cx,string nm,Domain dt,long cf=-1L)
            :base(cx.GetUid(),cx.DoDepth(_Mem(cf)+(ObInfo.Name,nm)+(_Domain,dt)))
        {
            cx.Add(this);
        }
        protected QlValue(Ident id,Context cx,BTree<long,object>m)
            :base(id.uid,m)
        {
            cx.Add(this);
            cx.Add(id.ident, id.lp, this);
        }
        static BTree<long,object> _Mem(long cf)
        {
            var r = BTree<long, object>.Empty;
            if (cf >= 0)
                r += (QlInstance.SPos, cf);
            return r;
        }
        internal override ObTree _Apply(Context cx, DBObject ob, ObTree f)
        {
            if (f[defpos] is QlValue x && x.dbg == dbg)
                return f;
            f += (defpos, this);
            for (var b = Operands(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue q)
                    f = q._Apply(cx, ob, f);
            return f;
        }
        public static QlValue operator +(QlValue et, (long, object) x)
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
            return (QlValue)et.New(m + x);
        }
        public static QlValue operator +(QlValue et, (Context, long, object) x)
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
            return (QlValue)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QlValue(defpos, m);
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
            if (q.mem[Domain.Nodes] is CTree<long,bool> xs && xs.Contains(defpos))
                return true;
            return q.Knows(cx, defpos, ambient);
        }
        internal virtual bool KnownBy<V>(Context cx,CTree<long,V> cs,bool ambient = false) 
            where V : IComparable
        {
            return cs.Contains(defpos);
        }
        /// <summary>
        /// The valueType of this function is important only for its keys.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="kb"></param>
        /// <param name="ambient"></param>
        /// <returns></returns>
        internal virtual CTree<long,Domain> KnownFragments(Context cx,CTree<long,Domain> kb,
            bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (left >= 0 && cx.obs[left] is QlValue le)
            { 
                r += le.KnownFragments(cx, kb,ambient);
                y = y && r.Contains(left);
            }
            if (right >= 0 && cx.obs[right] is QlValue ri)
            {
                r += ri.KnownFragments(cx, kb,ambient);
                y = y && r.Contains(right);
            }
            if (sub >= 0 && cx.obs[sub] is QlValue su)
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
        internal static int _Depths(BList<QlValue> os)
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
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            if (Eval(cx).dataType is NodeType nt) // or EdgeType
                return nt.For(cx, ms, xn, ds);
           return domain.For(cx, ms, xn, ds);
        }
        internal static string Show(Qlx op)
        {
            return op switch
            {
                Qlx.COLON => ":",
                Qlx.EQL => "=",
                Qlx.COMMA => ",",
                Qlx.CONCATENATE => "||",
                Qlx.DIVIDE => "/",
                Qlx.DOT => ".",
                Qlx.DOUBLECOLON => "::",
                Qlx.GEQ => ">=",
                Qlx.GTR => ">",
                Qlx.LBRACK => "[",
                Qlx.LEQ => "<=",
                Qlx.LPAREN => "(",
                Qlx.LSS => "<",
                Qlx.MINUS => "-",
                Qlx.NEQ => "<>",
                Qlx.NO => "",
                Qlx.PLUS => "+",
                Qlx.TIMES => "*",
                Qlx.AND => " and ",
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
        /// Some QlValues such as SqlReview, ForwardReference, SqlStar need to be resolved
        /// during parsing. This method is used to do this as soon as possible,
        /// when a potential receiving object rs is being constructed by the parser.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="sr">The source rowset for rs</param>
        /// <param name="m">Proposed details for rs, may be updated</param>
        /// <param name="ap">Proposed defpos of rs</param>
        /// <returns>a list of candidate objects to replace this, and an update to m</returns>
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (domain.kind != Qlx.CONTENT || defpos<ap)
                return (new BList<DBObject>(this), m);
            var ns = (Names)(m[ObInfo._Names] ?? Names.Empty);
            if (name != null && ns.Contains(name) && cx.obs[ns[name].Item2] is DBObject ob
             && (ob is QlValue || ob is SystemTableColumn) && ob.domain.kind != Qlx.CONTENT)
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
                    if (cx.cursors[b.key()] is TRow sv 
                        && sv[defpos] is TypedValue lv && lv != TNull.Value)
                            return lv;
            if (name != null && cx.names[name].Item2 is long p)
                return cx.values[p]??TNull.Value;
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
        /// test whether the given QlValue is structurally equivalent to this (always has the same value in this context)
        /// </summary>
        /// <param name="v">The expression to test against</param>
        /// <returns>Whether the expressions match</returns>
        internal virtual bool _MatchExpr(Context cx,QlValue v,RowSet r)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (QlValue)base._Replace(cx, so, sv);
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
        internal virtual bool HasAnd(Context cx,QlValue s)
        {
            return s == this;
        }
        internal virtual CTree<long,bool> IsAggregation(Context cx,CTree<long,bool> ags)
        {
            for (var b = ags.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlFunction fr && fr.op == Qlx.RESTRICT
                    && fr.val == defpos && SqlFunction.aggregatable(cx.obs[fr.val]?.domain.kind??Qlx.NO))
                    return new CTree<long, bool>(defpos, true);
            return CTree<long,bool>.Empty;
        }
        internal virtual QlValue Invert(Context cx)
        {
            return new SqlValueExpr(cx.GetUid(), cx, Qlx.NOT, this, null, Qlx.NO);
        }
        internal static bool OpCompare(Qlx op, int c)
        {
            return op switch
            {
                Qlx.EQL => c == 0,
                Qlx.NEQ => c != 0,
                Qlx.GTR => c > 0,
                Qlx.LSS => c < 0,
                Qlx.GEQ => c >= 0,
                Qlx.LEQ => c <= 0,
                _ => throw new PEException("PE61"),
            };
        }
        internal virtual RowSet RowSetFor(long ap,long dp,Context cx,CList<long> us,
            CTree<long,Domain> re)
        {
            if (cx.val is TRow r)
                return new TrivialRowSet(ap,dp,cx, r);
            return new EmptyRowSet(dp,cx,domain,us,re);
        }
        internal virtual Domain FindType(Context cx,Domain dt)
        {
            if (domain.kind==Qlx.CONTENT)
                return dt;
            var dm = domain;
            if (dt.kind!=Qlx.TABLE && dt.kind!=Qlx.ROW)
                dm = dm.Scalar(cx);
            if (!dt.CanTakeValueOf(dm))
                return dt;
         //       throw new DBException("22G03", dt.kind, dm.kind);
            if ((isConstant(cx) && dm.kind == Qlx.INTEGER) || dm.kind==Qlx.Null)
                return dt; // keep result options open
            return dm;
        }
        /// <summary>
        /// Transform this, replacing aggregation subexpressions by aggregation columns of dm
        /// The expression must be functionally dependent on dm
        /// </summary>
        /// <param name="c"></param>
        /// <param name="dm"></param>
        /// <returns></returns>
        internal virtual QlValue Having(Context c, Domain dm, long ap)
        {
            throw new DBException("42112", ToString());
        }
        internal virtual bool Match(Context c,QlValue v)
        {
            return v is not null && defpos == v.defpos;
        }
        public virtual int CompareTo(object? obj)
        {
            return (obj is QlValue that)?defpos.CompareTo(that.defpos):1;
        }
        internal virtual QlValue Constrain(Context cx,Domain dt)
        {
            var dm = domain;
            if (scalar && dt.kind == Qlx.TABLE)
                throw new DBException("22000", Qlx.MINUS);
            var nd = (dm.Constrain(cx, cx.GetUid(), dt) ??
                throw new DBException("22000", Qlx.MINUS));
            if (dm != nd)
            {
                cx.Add(nd);
                return (QlValue)cx.Add(this+(_Domain,nd));
            }
            return this;
        }
        internal QlValue ConstrainScalar(Context cx)
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
            CList<long> cs, CTree<long, string> ns, Context cx)
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
            return new QlValue(dp, m);
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
        internal QlValue Sources(Context cx)
        {
            var r = (QlValue)Fix(cx);
            if (r == this)
                return this;
            var np = cx.GetUid();
            r = (QlValue)r.Relocate(np);
            cx.Add(r);
            cx.uids += (defpos, np);
            return r;
        }
        internal override bool Verify(Context cx)
        {
            if (defpos < Transaction.Analysing || defpos >= Transaction.HeapStart)
                return true;
            if (GetType().Name != "QlValue" && from < 0)
                return false;
            return true;
        }
    }
    internal class SqlReview : QlValue
    {
        public SqlReview(Ident nm, Ident ic, BList<Ident> ch, Context cx, Domain dt, BTree<long, object>? m = null) 
            : base(nm, ic, ch, cx, dt, m)
        {
            cx.undefined += (nm.uid, cx.sD);
        }
        protected SqlReview(long dp,BTree<long,object>m) :base(dp,m)
        {  }
        internal override TypedValue _Eval(Context cx)
        {
            // as a last resort try to resolve the name
            TypedValue v = TNull.Value;
            var ns = names;
            for (var b=chain?.First();b!=null;b=b.Next())
                if (b.value() is Ident id && ns[id.ident].Item2 is long cp)
                {
                    if (v == TNull.Value && cx.values[cp] is TypedValue w)
                    {
                        v = w;
                        ns = w.dataType.names;
                    }
                    else if (v is TNode tn)
                        v = tn.tableRow.vals[cp] ?? TNull.Value;
                    else if (v is TRow tr)
                        v = tr.values[cp] ?? TNull.Value;
                }
            if (v != TNull.Value)
                return v;
            return base._Eval(cx);
        }
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
        /// <summary>
        /// The parser is constructing a new RowSet rs whose proposed properties are in m
        /// and whose defining position is ap, and our defpos is > ap.
        /// If our defpos>ap, there might be a column in m that could replace us: columns
        /// in sr have already been checked.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="sr">The source rowSet</param>
        /// <param name="m">The properties of rs so far</param>
        /// <param name="ap">The proposed defpos of rs</param>
        /// <returns>A list containing a single replacement, and the properties of rs</returns>
        /// <exception cref="PEException"></exception>
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50701");
            var ns = (Names)(m[ObInfo._Names] ?? Names.Empty);
            if (name != null && ns.Contains(name) && cx.obs[ns[name].Item2] is DBObject ob 
             && (ob is QlValue || ob is SystemTableColumn) && ob.domain.kind != Qlx.CONTENT)
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
                cx._Ob(p) is not QlValue old)
                return;
            var m = BTree<long, object>.Empty;
            if (alias != null)
                m += (_Alias, alias);
            var nv = (tc is QlValue sv) ? sv
                : new QlInstance(new Ident(name,defpos), cx, f.defpos, tc, m);
            cx.done = ObTree.Empty;
            cx.Replace(this, nv);
            cx.done = ObTree.Empty;
            cx.Replace(old, nv);
        }
        internal override ObTree _Apply(Context cx, DBObject ob, ObTree f)
        {
            if (ob.infos[cx.role.defpos] is ObInfo oi && name != null
                && oi.names[name].Item2 is long cp && cx.obs[cp] is TableColumn tc)
                return f + (defpos,new QlInstance(oi.names[name].Item1,defpos, cx, name, from, tc));
            return f + (defpos, this);
        }
        internal override bool Verify(Context cx)
        {
            return true;
        }
    }
    internal class QlInstance : QlValue
    {
        internal const long
            SPos = -284; // long
        public long sPos => (long)(mem[SPos]??-1L);
        public QlInstance(long lp, long dp, Context cx, string nm, long fp, long cp, BTree<long,object>?m=null)
            :this(lp, dp,cx,nm,fp,(DBObject?)(cx.obs[cp] ?? cx.db.objects[cp]),m)
        { }
        public QlInstance(long lp, long dp, Context cx, string nm, long fp, DBObject? cf, BTree<long, object>? m = null)
            : base(dp, _Mem(cx, dp, fp, cf, nm, m))
        {
            if (dp == cf?.defpos) // someone has forgotten the from clause
                throw new DBException("42112", nm);
            cx.Add(nm,lp, this);
            if (cx.undefined.Contains(dp))
            {
                cx.undefined -= dp;
                cx.NowTry();
            }
        }
        public QlInstance(Ident id, Context cx, long fp, long cp,
            BTree<long, object>? m = null)
            : this(id,cx,fp,(DBObject?)(cx.obs[cp]??cx.db.objects[cp]), m)
        {
            if (cx.obs[fp] is RowSet r)
                cx.Add(r.Apply(new BTree<long, object>(RowSet.ISMap, r.iSMap + (id.uid, cp))
                    + (RowSet.SIMap, r.sIMap + (cp, id.uid)), cx));
            if (cx.undefined.Contains(id.uid))
            {
                cx.undefined -= id.uid;
                cx.NowTry();
            }
        }
        public QlInstance(Ident id, Context cx, long fp, DBObject? cf,
           BTree<long, object>? m = null)
            : base(id, cx, _Mem(cx, id.uid, fp, cf, id.ident, m))
        {
            if (id.uid == cf?.defpos) // someone has forgotten the from clause
                throw new DBException("42112", id.ident);
            if (cx.undefined.Contains(id.uid))
            {
                cx.undefined -= id.uid;
                cx.NowTry();
            }
        }
        static BTree<long, object> _Mem(Context cx, long dp, long fp, DBObject? cf, string nm, BTree<long, object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (nm is not null)
                m += (ObInfo.Name, nm);
            if (fp >= 0)
                m += (_From, fp);
            if (cf != null)
            {
                if (cx.obs[fp] is RowSet r)
                    cx.Add(r.Apply(new BTree<long, object>(RowSet.ISMap, r.iSMap + (dp, cf.defpos))
                        + (RowSet.SIMap, r.sIMap + (cf.defpos, dp)), cx));
                m = m + (SPos, cf.defpos) + (_Domain, cf.domain) + (_Depth, cf?.depth ?? 1);
            }
            return m;
        }
        protected QlInstance(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static QlInstance operator +(QlInstance et, (long, object) x)
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
            return (QlInstance)et.New(m + x);
        }
        public static QlInstance operator +(QlInstance et, (Context, long, object) x)
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
            return (QlInstance)et.New(m + (dp, ob));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QlInstance(defpos, m);
        }
        internal override bool IsFrom(Context cx, RowSet q, bool ordered = false, Domain? ut = null)
        {
            return q.representation.Contains(defpos)==true;
        }
        internal override bool KnownBy(Context cx, RowSet r, bool ambient = false)
        {
            if (r is SystemRowSet && r.representation.Contains(sPos))
                return true;
            return r.Knows(cx, defpos, ambient);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient=false)
        {
            if (kb[defpos] is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            if (kb[sPos] is Domain dc)
                return new CTree<long, Domain>(defpos, dc);
            return CTree<long,Domain>.Empty;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return this;
        }
        internal override bool Match(Context c, QlValue v)
        {
            if (v == null)
                return false;
            if (defpos == v.defpos)
                return true;
            if (v is QlInstance sc && sPos == sc.sPos)
                return true;
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return (sPos<Transaction.Executables)?
                new CTree<long,bool>(sPos,true): CTree<long,bool>.Empty;
        }
        internal override CTree<long,bool> Operands(Context cx)
        {
            var r = new CTree<long, bool>(defpos, true);
            if (cx.obs[sPos] is QlValue s)
                r += s.Operands(cx);
            return r;
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, Ident ic, Ident? n, DBObject? r)
        {
            if (n?.ident is string s && domain.infos[cx.role.defpos] is ObInfo oi
                && domain is UDType  && oi.names[s].Item2 is long p)
            {
                var f = new SqlField(lp, s, cx.sD, defpos, domain.representation[p] ?? Domain.Content, p);
                return (cx.Add(f), n.sub); 
            }
            return base._Lookup(lp, cx, ic, n, r);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new QlInstance(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m) 
        {
            var r = base._Fix(cx,m);
            if (sPos < Transaction.Executables || sPos >= Transaction.HeapStart)
            {
                var nc = cx.Fix(sPos);
                if (nc != sPos)
                    r += (SPos, nc);
            }
            return r;
        }
        internal override TypedValue _Eval(Context cx)
        {
            var dv = domain.defaultValue;
            if (defpos >= Transaction.Executables && defpos < Transaction.HeapStart)
                for (var c = cx; c != null; c = c.next)
                {
                    if (from==-1L || (c is CalledActivation ca && ca.bindings.Contains(sPos)))
                        return cx.values[sPos] ?? dv;
                    if (c is TriggerActivation ta && ta.trigTarget?[defpos] is long cp
                            && ta._trs?.targetTrans[cp] is long fp
                            && cx.values[fp] is TypedValue v)
                        return v; 
                }
            if (cx.obs[sPos] is SqlElement)
                return cx.values[sPos] ?? dv;
            if (cx.obs[from] is QlInstance sc && sc._Eval(cx) is TypedValue tv)
            { 
                if (tv is TRow rw)
                    return rw[sPos] ?? dv;
                if (tv is TInt ti && cx.obs[sc.sPos] is TableColumn tc
                    && cx.db.objects[tc.toType] is NodeType nt && nt.tableRows[ti.ToInt()??-1L] is TableRow tr)
                    return tr.vals[sPos] ?? dv;
            }
            return cx.values[defpos] ?? cx.values[sPos] ?? dv;
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.obs[sPos]?.Set(cx,v);
            base.Set(cx, v);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var cf = sPos;
            while (cx.obs[cf] is QlInstance sc)
                cf = sc.sPos;
            if (((RowSet?)cx.obs[rs])?.Knows(cx,cf) == true)
                return CTree<long, bool>.Empty;
            return (cf<-1L)?CTree<long,bool>.Empty
                : base.Needs(cx, rs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" copy from "); sb.Append(Uid(sPos));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A TYPE value for use in CAST and in graphs
    ///     
    /// </summary>
    internal class SqlTypeExpr : QlValue
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    internal class SqlTreatExpr : QlValue
    {
        internal const long
            _Diff = -468, // BTree<long,object>
            TreatExpr = -313; // long QlValue
        BTree<long, object> diff => (BTree<long, object>)(mem[_Diff] ?? BTree<long, object>.Empty);
        internal long val => (long)(mem[TreatExpr]??-1L);
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(long dp,QlValue v,Domain ty)
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
            var a = ((QlValue?)cx.obs[val])?.AddFrom(cx, q);
            if (a is not null && a.defpos != val)
                r += (cx, TreatExpr, a.defpos);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlTreatExpr)base._Replace(cx, so, sv);
            var v = cx.ObReplace(val, so, sv);
            if (v != val)
                r +=(cx,TreatExpr, v);
            if ((so.defpos == val || sv.defpos == val) && cx._Dom(v) is Domain nd)
                r = (SqlTreatExpr)r.New(mem + (_Domain,nd.New(nd.mem + diff)));
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50702");
            QlValue r = this;
            if (cx.obs[val] is QlValue ol && ol.defpos>ap)
            {
                BList<DBObject> ls;
                (ls, m) = ol.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv  && nv.defpos != val)
                {
                    r = new SqlTreatExpr(defpos, nv, domain);
                    cx.Replace(this, r);
                }
            }
            if (domain.kind==Qlx.CONTENT && defpos>=0 &&
                cx._Dom(val) is Domain dv && dv.kind!=Qlx.CONTENT)
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
            return ((QlValue?)cx.obs[val])?.IsAggregation(cx,ags)??CTree<long,bool>.Empty;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, q, ambient)??false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient)??false;
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.values[val] is TypedValue v)
                return domain.Coerce(cx,v);
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
            return ((QlValue?)cx.obs[val])?.Needs(cx,r,qn)??qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            return ((QlValue?)cx.obs[val])?.Needs(cx, rs)??CTree<long,bool>.Empty;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Val= "); sb.Append(Uid(val));
            return sb.ToString();
        }
    }
    internal class SqlCaseSimple : QlValue
    {
        internal const long 
            Cases = -475,       // BList<(long,long)> QlValue QlValue 
            CaseElse = -228;    // long QlValue
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public BList<(long, long)> cases =>
            (BList<(long,long)>?)mem[Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[CaseElse] ?? -1L);
        internal SqlCaseSimple(long dp, Context cx, Domain dm, QlValue vl, BList<(long, long)> cs, long el,
            long fr = -1L)
            : base(dp, _Mem(cx, dm, vl, cs, el, fr))
        {
            cx.Add(this);
        }
        protected SqlCaseSimple(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, Domain dm, QlValue vl, BList<(long, long)> cs, long el, long fr)
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
            if (cx.obs[el] is QlValue e)
            {
                ds += (el, true);
                d = Math.Max(e.depth + 1,d);
            }
            d = cx._DepthBPVV(cs, d);
            return r + (_Depth,d) + (_From,fr);
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
        public static SqlCaseSimple operator +(SqlCaseSimple et, (long, object) x)
        {
            return (SqlCaseSimple)et.New(et.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCaseSimple(defpos,m);
        }
        internal override CTree<long, bool> IsAggregation(Context cx, CTree<long, bool> ags)
        {
            
            for (var b=cases.First();b!=null;b=b.Next())
            {
                var (cp, vp) = b.value();
                if (cx.obs[cp] is QlValue c)
                    ags = c.IsAggregation(cx, ags);
                if (cx.obs[vp] is QlValue v)
                    ags = v.IsAggregation(cx, ags);
            }
            if (cx.obs[caseElse] is QlValue e)
                ags = e.IsAggregation(cx, ags);
            return ags;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is QlValue sw)
                    r += sw.Needs(cx);
                if (cx.obs[x] is QlValue sx)
                    r += sx.Needs(cx);
            }
            if (cx.obs[caseElse] is QlValue se)
                r += se.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx,long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is QlValue sw)
                    r += sw.Needs(cx,rs);
                if (cx.obs[x] is QlValue sx)
                    r += sx.Needs(cx,rs);
            }
            if (cx.obs[caseElse] is QlValue se)
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
                if (cx.obs[b.key()] is QlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue s && !s.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override TypedValue _Eval(Context cx)
        {
            var oc = cx.values;
            var v = cx.obs[val]?.Eval(cx)??TNull.Value;
            for (var b = cases.First();b is not null;b=b.Next())
            {
                var (r, w) = b.value();
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
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[val] is QlValue v) tg = v.AddIn(cx, rb, tg);
            for (var b = cases.First(); b is not null; b = b.Next())
            {
                var (w, r) = b.value();
                if (cx.obs[w] is QlValue wv) tg = wv.AddIn(cx, rb, tg);
                if (cx.obs[r] is QlValue rv) tg = rv.AddIn(cx, rb, tg);
            }
            if (cx.obs[caseElse] is QlValue c) tg = c.AddIn(cx, rb, tg);
            return tg;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[val] is QlValue v) tg = v.StartCounter(cx, rs, tg);
            for (var b = cases.First(); b is not null; b = b.Next())
            {
                var (w, r) = b.value();
                if (cx.obs[w] is QlValue wv) tg = wv.StartCounter(cx, rs, tg);
                if (cx.obs[r] is QlValue rv) tg = rv.StartCounter(cx, rs, tg);
            }
            if (cx.obs[caseElse] is QlValue c) tg = c.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50703");
            BList<DBObject>? ls;
            var r = this;
            var a = alias;
            var ch = false;
            QlValue v = (QlValue?)cx.obs[val] ?? SqlNull.Value,
                ce = (QlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long,long)>.Empty;
            if (v!=SqlNull.Value && v.defpos>ap)
            {
                (ls, m) = v.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=v.defpos)
                {
                    ch = true; v = nv;
                }
            }
            if (ce!=SqlNull.Value && ce.defpos>ap)
            {
                (ls, m) = ce.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
                }
            }
            for (var b=cases.First();b is not null;b=b.Next())
            {
                var (s, c) = b.value();
                QlValue sv = (QlValue?)cx.obs[s] ?? SqlNull.Value,
                    sc = (QlValue?)cx.obs[c] ?? SqlNull.Value;
                if (sv!=SqlNull.Value && s>ap)
                {
                    (ls, m) = sv.Resolve(cx, sr, m, ap);
                    if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; sv = nv;
                    }
                }
                if (sc!=SqlNull.Value && c>ap)
                {
                    (ls, m) = sc.Resolve(cx, sr, m, ap);
                    if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                    {
                        ch = true; sc = nv;
                    }
                }
                css += (sc.defpos, sv.defpos);
            }
            if (ch)
            {
                var nr = new SqlCaseSimple(defpos, cx, domain, v, css, ce.defpos, from);
                if (a != null)
                    nr += (_Alias, a);
                cx.Replace(this, nr);
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
    internal class SqlCaseSearch : QlValue
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
        internal override CTree<long, bool> IsAggregation(Context cx, CTree<long, bool> ags)
        {
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (cp, vp) = b.value();
                if (cx.obs[cp] is QlValue c)
                    ags = c.IsAggregation(cx, ags);
                if (cx.obs[vp] is QlValue v)
                    ags = v.IsAggregation(cx, ags);
            }
            if (cx.obs[caseElse] is QlValue e)
                ags = e.IsAggregation(cx, ags);
            return ags;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is QlValue sw)
                    r += sw.Needs(cx);
                if (cx.obs[x] is QlValue sx)
                    r += sx.Needs(cx);
            }
            if (cx.obs[caseElse] is QlValue se)
                r += se.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                if (cx.obs[w] is QlValue sw)
                    r += sw.Needs(cx,rs);
                if (cx.obs[x] is QlValue sx)
                    r += sx.Needs(cx,rs);
            }
            if (cx.obs[caseElse] is QlValue se)
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
                if (cx.obs[b.key()] is QlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue s && !s.KnownBy(cx, cs, ambient))
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50704");
            BList<DBObject>? ls;
            var r = this;
            var ch = false;
            QlValue ce = (QlValue?)cx.obs[caseElse] ?? SqlNull.Value;
            var css = BList<(long, long)>.Empty;
            if (ce != SqlNull.Value && ce.defpos>ap)
            {
                (ls, m) = ce.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != ce.defpos)
                {
                    ch = true; ce = nv;
                }
            }
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (s, c) = b.value();
                QlValue sv = (QlValue?)cx.obs[s] ?? SqlNull.Value,
                    sc = (QlValue?)cx.obs[c] ?? SqlNull.Value;
                if (sv != SqlNull.Value && sv.defpos>ap)
                {
                    (ls, m) = sv.Resolve(cx, sr, m, ap);
                    if (ls[0] is QlValue nv && nv.defpos != sv.defpos)
                    {
                        ch = true; sv = nv;
                    }
                }
                if (sc != SqlNull.Value && sc.defpos>ap)
                {
                    (ls, m) = sc.Resolve(cx, sr, m, ap);
                    if (ls[0] is QlValue nv && nv.defpos != sc.defpos)
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
    internal class SqlField : QlValue
    {
        public long seq => (int)(mem[TableColumn.Seq] ?? -1);
        internal override long target => (long)(mem[RowSet.Target]??defpos);
        internal SqlField(long dp, string nm, int sq, long pa, Domain dt, long tg)
            : base(dp, BTree<long,object>.Empty + (ObInfo.Name, nm) + (TableColumn.Seq,sq)
                  + (_From, pa)  + (_Domain,dt) + (RowSet.Target,tg))
        { }
        internal SqlField(Ident id, Context cx, int sq, long pa, Domain dt, long tg)
            : base(id, cx, BTree<long, object>.Empty + (ObInfo.Name, id.ident) 
                + (TableColumn.Seq, sq) + (_From, pa) + (_Domain, dt) + (RowSet.Target, tg))
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return (cx.obs[target] as QlValue)?.KnownBy(cx, q, ambient) == true
                || base.KnownBy(cx, q, ambient);
        }
        internal override TypedValue _Eval(Context cx)
        {
            if ((cx.values[target]??cx.binding[target]) is TNode tt &&
                tt.dataType.infos[cx.role.defpos]?.names[name ?? ""].Item2 is long p
                && tt.tableRow.vals[p] is TypedValue nv)
                return nv;
            var tv = cx.values[from];
            if (tv is TRow tr) return tr.values[target]??tr.values[defpos]??TNull.Value;
            if (tv is TNode tn && tn.dataType.infos[cx.role.defpos] is ObInfo ni
                && ni.names[name??"?"].Item2 is long dp)
                return tn.tableRow.vals[dp] ?? TNull.Value;
            return TNull.Value;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" " + seq);
            sb.Append(" Target="); sb.Append(Uid(target));
            return sb.ToString();
        }
    }
    internal class SqlElement : QlValue
    {
        internal SqlElement(Ident nm,BList<Ident> ch,Context cx,Ident pn,Domain dt) 
            : base(nm,ch,cx,dt,BTree<long, object>.Empty+(_From,pn.uid)+(_Depth,dt.depth+1))
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            return (cx.obs[from] is QlValue)?
                new CTree<long,bool>(from,true): base.Needs(cx,r, qn);
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.values[defpos]??TNull.Value;
        }
        internal override string ToString(string sg,Remotes rf,CList<long> cs,
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
    /// A QlValue expression structure.
    /// Various additional operators have been added for JavaScript: e.g.
    /// modifiers BINARY for AND, OR, NOT; EXCEPT for (binary) XOR
    /// ASC and DESC for ++ and -- , with modifier BEFORE
    /// QMARK and COLON for ? :
    /// UPPER and LOWER for shifts (GTR is a modifier for the unsigned right shift)
    /// 
    /// </summary>
    internal class SqlValueExpr : QlValue
    {
        internal const long
            Modifier = -316,// Qlx
            Op = -300;  // Qlx
        /// <summary>
        /// the modifier (e.g. DISTINCT)
        /// </summary>
        public Qlx mod => (Qlx)(mem[Modifier] ?? Qlx.NO);
        internal Qlx op => (Qlx)(mem[Op] ?? Qlx.NO);
        /// <summary>
        /// constructor for an SqlValueExpr
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">an operator</param>
        /// <param name="lf">the left operand</param>
        /// <param name="rg">the right operand</param>
        /// <param name="m">a modifier (e.g. DISTINCT)</param>
        public SqlValueExpr(long dp, Context cx, Qlx op, QlValue? lf, QlValue? rg,
            Qlx m, BTree<long, object>? mm = null)
            : base(dp, _Mem(cx, op, m, lf, rg, mm))
        {
            cx.Add(this);
            lf?.ConstrainScalar(cx);
            rg?.ConstrainScalar(cx);
        }
        internal SqlValueExpr(Ident id, Context cx, Qlx op, QlValue? lf, QlValue? rg,
            Qlx m, BTree<long, object>? mm = null) :
            base(id, cx, _Mem(cx, op, m, lf, rg, mm))
        {
            lf?.ConstrainScalar(cx);
            rg?.ConstrainScalar(cx);
        }
        protected SqlValueExpr(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Context cx, Qlx op, Qlx mod,
    QlValue? left, QlValue? right, BTree<long, object>? mm = null)
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
                case Qlx.AND:
                    if (mod == Qlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Qlx.ASC: goto case Qlx.PLUS; // JavaScript
                case Qlx.COLLATE: dm = Domain.Char; break;
                case Qlx.COLON:   dm = Domain.Bool; break;  // SPECIFICTYPE
                case Qlx.CONCATENATE: dm = Domain.Char; break;
                case Qlx.DESC: goto case Qlx.PLUS; // JavaScript
                case Qlx.DIVIDE:
                    {
                        if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.INTEGER
                            || dr.kind == Qlx.NUMERIC))
                            dm = dl;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Qlx.DOT:
                    dm = dr;
                    if (left?.name != null && left.name != "" && right?.name != null && right.name != "")
                        nm = left.name + "." + right.name;
                    break;
                case Qlx.ELEMENTID: dm = Domain.Int; break;
                case Qlx.EQL: dm = Domain.Bool; break;
                case Qlx.EXCEPT: dm = dl; break;
                case Qlx.GTR: dm = Domain.Bool; break;
                case Qlx.ID: dm = Domain.Int; break;
                case Qlx.INTERSECT: dm = dl; break;
                case Qlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Qlx.LSS: dm = Domain.Bool; break;
                case Qlx.MINUS:
                    if (left != null)
                    {
                        if (dl.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME)
                        {
                            if (dl.kind == dr.kind)
                                dm = Domain.Interval;
                            else if (dr.kind == Qlx.INTERVAL)
                                dm = dl;
                        }
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME))
                            dm = dr;
                        else if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = left.domain;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                    if (right != null)
                        dm = right.FindType(cx, Domain.UnionDateNumeric);
                    break;
                case Qlx.NEQ: dm = Domain.Bool; break;
                case Qlx.LBRACK: dm = dl.elType ?? throw new DBException("2200G", Qlx.ARRAY, dl, Qlx.LBRACK); break;
                case Qlx.LEQ: dm = Domain.Bool; break;
                case Qlx.GEQ: dm = Domain.Bool; break;
                case Qlx.NO: dm = left?.domain ?? Domain.Content; break;
                case Qlx.NOT: goto case Qlx.AND;
                case Qlx.OR: goto case Qlx.AND;
                case Qlx.PATH: dm = new Domain(-1L, Qlx.ARRAY, dr); break;
                case Qlx.PLUS:
                    {
                        if ((dl.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME) && dr.kind == Qlx.INTERVAL)
                            dm = dl;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME))
                            dm = dr;
                        else if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Qlx.QMARK:
                    dm = Domain.Content; break;
                case Qlx.RBRACK:
                    {
                        if (left == null)
                            throw new PEException("PE5001");
                        dm = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.ARRAY, left.domain)); break;
                    }
                case Qlx.SET: dm = left?.domain ?? Domain.Content; nm = left?.name ?? ""; break; // JavaScript
                case Qlx.TIMES:
                    {
                        if (dl.kind == Qlx.NUMERIC || dr.kind == Qlx.NUMERIC)
                            dm = Domain._Numeric;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.INTEGER || dr.kind == Qlx.NUMERIC))
                            dm = dl;
                        else if (dr.kind == Qlx.INTERVAL && (dl.kind == Qlx.INTEGER || dl.kind == Qlx.NUMERIC))
                            dm = dr;
                        else if (left != null)
                            dm = left.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Qlx.TYPE: dm = Domain.TypeSpec; break;
                case Qlx.UNION: dm = dl; nm = left?.name ?? ""; break;
                case Qlx.UPPER: dm = Domain.Int; break; // JavaScript <<
            }
            dm ??= Domain.Content;
            /*          if (dl == Qlx.UNION && dr != Qlx.UNION && NumericOp(op) && dr != null &&
                          left != null && left.Constrain(cx, dr) is QlValue nl && left.defpos != nl.defpos)
                          cx.Replace(left, nl);
                      if (dr == Qlx.UNION && dl != Qlx.UNION && NumericOp(op) && dl != null &&
                          right != null && right.Constrain(cx, dl) is QlValue nr && right.defpos != nr.defpos)
                          cx.Replace(right, nr);*/
            mm ??= BTree<long, object>.Empty;
            var fm = left?.from ?? right?.from ?? -1L;
            var ap = 0L;
            if (fm >= 0)
            {
                mm += (_From, fm);
                ap = cx.obs[fm]?.scope ?? 0L;
            }
            if (ag != CTree<long, bool>.Empty && dm != Domain.Content)
            {
                var nt = CTree<long, bool>.Empty;
                for (var b = ag.First(); b != null; b = b.Next())
                    if ((cx.obs[b.key()]?.scope ?? 0L) > ap)
                        nt += (b.key(), true);
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50705");
            var r = this;
            var ch = domain == Domain.Content || domain.kind==Qlx.UNION;
            BList<DBObject> ls;
            QlValue lf = (QlValue?)cx.obs[left] ?? SqlNull.Value,
                rg = (QlValue?)cx.obs[right] ?? SqlNull.Value,
                su = (QlValue?)cx.obs[sub] ?? SqlNull.Value;
            if (lf != SqlNull.Value && lf.defpos>ap)
            {
                (ls, m) = lf.Resolve(cx, sr, m, ap);
                if (ls?[0] is QlValue nv && nv.defpos != lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value && rg.defpos>ap)
            {
                (ls, m) = rg.Resolve(cx, sr, m, ap);
                if (ls?[0] is QlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                }
            }
            if (su != SqlNull.Value && su.defpos>ap)
            {
                (ls, m) = su.Resolve(cx, sr, m, ap);
                if (ls?[0] is QlValue nv && nv.defpos != su.defpos)
                {
                    ch = true; su = nv;
                }
            }
            if (ch)
            {
                r = new SqlValueExpr(defpos, cx, op, lf, rg, mod, mem);
                if (su != SqlNull.Value)
                    r += (cx, Sub, su);
                r += (_From, sr.defpos);
                cx.Replace(this, r);
            }
            return (new BList<DBObject>(r), m);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            if (ch && (domain.kind == Qlx.UNION || domain.kind == Qlx.CONTENT)
                && cx.obs[lf] is QlValue lv && cx.obs[rg] is QlValue rv
                && so.domain != sv.domain)
            {
                if (_Mem(cx, domain.kind, mod, lv, rv)[_Domain] is not Domain nd)
                    throw new PEException("PE29001");
                cx.Add(nd);
                r += (_Domain, nd);
            }
            return r;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueExpr)base.AddFrom(cx, q);
            if (cx.obs[r.left] is QlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.left)
                    r += (cx, Left, a.defpos);
            }
            if (cx.obs[r.right] is QlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (cx, Right, a.defpos);
            }
            return (QlValue)cx.Add(r);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            var ch = false;
            var nl = ((QlValue?)c.obs[left])?.Having(c, dm, ap);
            ch = ch || (nl != null && nl.defpos != left);
            var nr = ((QlValue?)c.obs[right])?.Having(c, dm, ap);
            ch = ch || (nr != null && nr.defpos != right);
            var nu = ((QlValue?)c.obs[sub])?.Having(c, dm, ap);
            ch = ch || (nu != null && nu.defpos != sub);
            var r = this;
            if (ch)
            {
                r = new SqlValueExpr(c.GetUid(), c, op, nl, nr, mod);
                if (nu != null)
                    r += (c, Sub, nu);
                return (QlValue)c.Add(r);
            }
            return r;
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (!base.Match(cx, v)) return false;
            if (v is SqlValueExpr ve)
            {
                if (op != ve.op || mod != ve.mod)
                    return false;
                if (cx.obs[left] is QlValue lf && cx.obs[ve.left] is QlValue le &&
                    !le.Match(cx, lf))
                    return false;
                if (cx.obs[right] is QlValue rg && cx.obs[ve.right] is QlValue re &&
                    !re.Match(cx, rg))
                    return false;
                if (cx.obs[sub] is QlValue su && cx.obs[ve.sub] is QlValue se &&
                    !se.Match(cx, su))
                    return false;
                return true;
            }
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((cx.obs[left] as QlValue)?.KnownBy(cx, q, ambient) != false)
                && (op==Qlx.DOT || ((cx.obs[right] as QlValue)?.KnownBy(cx, q, ambient) != false));
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            if (cs.Contains(defpos))
                return true;
            return ((cx.obs[left] as QlValue)?.KnownBy(cx, cs, ambient) != false)
                && ((cx.obs[right] as QlValue)?.KnownBy(cx, cs, ambient) != false);
        }
        internal override CTree<long, bool> Disjoin(Context cx)
        { // parsing guarantees right associativity
            return (op == Qlx.AND && cx.obs[right] is QlValue rg) ?
                rg.Disjoin(cx) + (left, true)
                : base.Disjoin(cx);
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[left] is QlValue lf && !lf.Grouped(cx, gs))
                return false;
            if (cx.obs[right] is QlValue rg && !rg.Grouped(cx, gs))
                return false;
            return true;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is QlValue sl)
                r += sl.Operands(cx);
            if (cx.obs[right] is QlValue sr)
                r += sr.Operands(cx);
            return r;
        }
        internal override CTree<long, bool> ExposedOperands(Context cx,CTree<long,bool> ag,Domain? gc)
        {
            var r = CTree<long, bool>.Empty;
            if (gc?.representation.Contains(defpos)==true)
                return r;
            if (cx.obs[left] is QlValue sl)
                r += sl.ExposedOperands(cx,ag,gc);
            if (cx.obs[right] is QlValue sr && op!=Qlx.DOT)
                r += sr.ExposedOperands(cx,ag,gc);
            return r;
        }
        /// <summary>
        /// Examine a binary expression and work out the resulting type.
        /// The main complication here is handling things like x+1
        /// (e.g. confusion between NUMERIC and INTEGER)
        /// </summary>
        /// <param name="dt">Target result type</param>
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
                case Qlx.AND:
                    if (mod == Qlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Qlx.ASC: goto case Qlx.PLUS; // JavaScript
                case Qlx.COLLATE: dm = Domain.Char; break;
                case Qlx.COLON:
                    dm = dl;
                    break;// JavaScript
                case Qlx.CONCATENATE: dm = Domain.Char; break;
                case Qlx.DESC: goto case Qlx.PLUS; // JavaScript
                case Qlx.DIVIDE:
                    {
                        if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.INTEGER
                            || dr.kind == Qlx.NUMERIC))
                            dm = dl;
                        else if (le is QlValue sl)
                            dm = sl.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Qlx.DOT:
                    dm = dr;
                    break;
                case Qlx.ELEMENTID: dm = Domain.Int; break; // tableRow.defpos
                case Qlx.EQL: dm = Domain.Bool; break;
                case Qlx.EXCEPT: dm = dl; break;
                case Qlx.GTR: dm = Domain.Bool; break;
                case Qlx.ID: dm = Domain.Int; break;
                case Qlx.INTERSECT: dm = dl; break;
                case Qlx.LABELS: dm = Domain.SetType; break;
                case Qlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Qlx.LSS: dm = Domain.Bool; break;
                case Qlx.MINUS:
                    if (le is not null)
                    {
                        if (dl.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME)
                        {
                            if (dl.kind == dr.kind)
                                dm = Domain.Interval;
                            else if (dr.kind == Qlx.INTERVAL)
                                dm = dl;
                        }
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME))
                            dm = dr;
                        else if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else if (le is QlValue sm)
                            dm = sm.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                    if (rg is QlValue sr)
                        dm = sr.FindType(cx, Domain.UnionDateNumeric);
                    break;
                case Qlx.NEQ: dm = Domain.Bool; break;
                case Qlx.LEQ: dm = Domain.Bool; break;
                case Qlx.GEQ: dm = Domain.Bool; break;
                case Qlx.NO: dm = dl; break;
                case Qlx.NOT: goto case Qlx.AND;
                case Qlx.OR: goto case Qlx.AND;
                case Qlx.PLUS:
                    {
                        if ((dl.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME) && dr.kind == Qlx.INTERVAL)
                            dm = dl;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.DATE || dl.kind == Qlx.TIMESTAMP || dl.kind == Qlx.TIME))
                            dm = dr;
                        else if (dl.kind == Qlx.REAL || dl.kind == Qlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Qlx.REAL || dr.kind == Qlx.NUMERIC)
                            dm = dr;
                        else if (le is QlValue ll)
                            dm = ll.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Qlx.QMARK:
                    dm = Domain.Content; break;
                case Qlx.RBRACK:
                    {
                        if (le is not QlValue lf)
                            throw new PEException("PE5001");
                        var tl = lf.FindType(cx, dt);
                        dm = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.ARRAY, tl.elType??Domain.Content)); break;
                    }
                case Qlx.SET: dm = dl ?? Domain.Content; break; // JavaScript
                case Qlx.TIMES:
                    {
                        if (dl.kind == Qlx.NUMERIC || dr.kind == Qlx.NUMERIC)
                            dm = Domain._Numeric;
                        else if (dl.kind == Qlx.INTERVAL && (dr.kind == Qlx.INTEGER || dr.kind == Qlx.NUMERIC))
                            dm = dl;
                        else if (dr.kind == Qlx.INTERVAL && (dl.kind == Qlx.INTEGER || dl.kind == Qlx.NUMERIC))
                            dm = dr;
                        else if (le is QlValue tl)
                            dm = tl.FindType(cx, Domain.UnionNumeric);
                        break;
                    }
                case Qlx.TYPE: dm = Domain.Char; break;
                case Qlx.UNION: dm = dl; break;
                case Qlx.UPPER: dm = Domain.Int; break; // JavaScript <<
            }
            dm ??= Domain.Content;
            cx.Add(new SqlValueExpr(defpos, mem + (_Domain, dm)));
            return dm;
        }
        internal override bool HasAnd(Context cx, QlValue s)
        {
            if (s == this)
                return true;
            if (op != Qlx.AND)
                return false;
            return (cx.obs[left] as QlValue)?.HasAnd(cx, s) == true
            || (cx.obs[right] as QlValue)?.HasAnd(cx, s) == true;
        }
        internal override CTree<long, bool> IsAggregation(Context cx,CTree<long,bool> ags)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is QlValue lf)
                r += lf.IsAggregation(cx,ags);
            if (cx.obs[right] is QlValue rg)
                r += rg.IsAggregation(cx,ags);
            if (cx.obs[sub] is QlValue su)
                r += su.IsAggregation(cx,ags);
            return r;
        }
        internal override int Ands(Context cx)
        {
            if (op == Qlx.AND)
                return ((cx.obs[left] as QlValue)?.Ands(cx) ?? 0)
                    + ((cx.obs[right] as QlValue)?.Ands(cx) ?? 0);
            return base.Ands(cx);
        }
        internal override bool isConstant(Context cx)
        {
            return op!=Qlx.QMARK && (cx.obs[left] as QlValue)?.isConstant(cx) != false
                && (cx.obs[right] as QlValue)?.isConstant(cx) != false;
        }
        internal override BTree<long, SystemFilter> SysFilter(Context cx, BTree<long, SystemFilter> sf)
        {
            switch (op)
            {
                case Qlx.AND:
                    {
                        if (cx.obs[left] is QlValue lf && cx.obs[right] is QlValue rg)
                            return lf.SysFilter(cx, rg.SysFilter(cx, sf));
                        break;
                    }
                case Qlx.EQL:
                case Qlx.GTR:
                case Qlx.LSS:
                case Qlx.LEQ:
                case Qlx.GEQ:
                    {
                        if (cx.obs[left] is QlValue lf && cx.obs[right] is QlValue rg)
                        {
                            if (lf.isConstant(cx) && rg is QlInstance sc)
                                return SystemFilter.Add(sf, sc.sPos, Neg(op), lf.Eval(cx));
                            if (rg.isConstant(cx) && lf is QlInstance sl)
                                return SystemFilter.Add(sf, sl.sPos, op, rg.Eval(cx));
                        }
                        break;
                    }
                default:
                    return sf;
            }
            return base.SysFilter(cx, sf);
        }
        static Qlx Neg(Qlx o)
        {
            return o switch
            {
                Qlx.GTR => Qlx.LSS,
                Qlx.GEQ => Qlx.LEQ,
                Qlx.LEQ => Qlx.GEQ,
                Qlx.LSS => Qlx.GTR,
                _ => o,
            };
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is QlValue lf) tg = lf.StartCounter(cx, rs, tg);
            if (cx.obs[right] is QlValue rg) tg = rg.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is QlValue lf) tg = lf.AddIn(cx, rb, tg);
            if (cx.obs[right] is QlValue rg) tg = rg.AddIn(cx, rb, tg);
            return tg;
        }
        internal override void OnRow(Context cx, Cursor bmk)
        {
            (cx.obs[left] as QlValue)?.OnRow(cx, bmk);
            (cx.obs[right] as QlValue)?.OnRow(cx, bmk);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            if (op != Qlx.DOT)
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
                case Qlx.AND:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (mod != Qlx.BINARY)
                        {
                            if (a==TBool.True && b==TBool.True)
                                return TBool.True;
                            if (a == TBool.False || b == TBool.False)
                                return TBool.False;
                            return TBool.Unknown;
                        }
                        if (mod == Qlx.BINARY && a is TInt aa && b is TInt ab) // JavaScript
                            return new TInt(aa.value & ab.value);
                        else if (a is TBool ba && b is TBool bb)
                            return TBool.For((ba.value is bool xa && bb.value is bool xb)?(xa && xb):null);
                        break;
                    }
                case Qlx.ASC: // JavaScript ++
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        var x = domain.Eval(defpos, cx, v, Qlx.ADD, new TInt(1L));
                        return (mod == Qlx.BEFORE) ? x : v;
                    }
                case Qlx.COLLATE:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        Domain ct = a.dataType;
                        if (ct.kind == Qlx.CHAR)
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
                case Qlx.COLON: // SPECIFICTYPE
                    {
                        var a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        var b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b is TTypeSpec tt && a.dataType.EqualOrStrongSubtypeOf(tt._dataType))
                            return TBool.True;
                        return TBool.False;
                    }
                case Qlx.COMMA: // JavaScript
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return a;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        return b;
                    }
                case Qlx.CONCATENATE:
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
                case Qlx.CONTAINS:
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
                case Qlx.DESC: // JavaScript --
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        var w = a.dataType.Eval(defpos, cx, v, Qlx.MINUS, new TInt(1L));
                        return (mod == Qlx.BEFORE) ? w : v;
                    }
                case Qlx.DIVIDE:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return domain.Eval(defpos, cx, a, op, b);
                    }
                case Qlx.DOT:
                    {
                        var ol = cx.obs[left] as QlValue;
                 //       if (ol is GqlNode && cx.values[right] is TypedValue dv && dv != TNull.Value)
                 //           return dv;
                        TypedValue a = ol?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        if (a is TRow ra)
                        {
                            if (cx.obs[right] is SqlField sf)
                                return ra.values[sf.seq] ?? v;
                            if (cx.obs[right] is QlInstance sc && sc.defpos is long dp)
                                return ra.values[dp] ??
                                    ((sc.sPos is long cp) ? (ra.values[cp] ?? v) : v);
                        }
                        if (a is TNode tn)
                        {
                            if (cx.obs[right] is QlInstance sn)
                                return tn.tableRow.vals[sn.sPos] ?? TNull.Value;
                            if (cx.obs[right] is SqlField sf && tn.dataType.infos[cx.role.defpos] is ObInfo ni
                                && ni.names[sf.name ?? "?"].Item2 is long dp)
                                return tn.tableRow.vals[dp] ?? TNull.Value;
                        }
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return v;
                    }
                case Qlx.EQL:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(b.CompareTo(a) == 0);
                    }
                case Qlx.EQUALS:
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
                case Qlx.EXCEPT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb)
                            return a.dataType.Coerce(cx, TMultiset.Except(ma, mb, mod == Qlx.ALL));
                        break;
                    }
                case Qlx.GEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) >= 0);
                    }
                case Qlx.GTR:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) > 0);
                    }
                case Qlx.INTERSECT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb && TMultiset.Intersect(ma, mb, mod == Qlx.ALL) is TMultiset mc)
                            return a.dataType.Coerce(cx, mc);
                        break;
                    }
                case Qlx.LBRACK:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TArray aa && b is TInt bb)
                            return aa[(int)bb.value] ?? v;
                        if (a is TList al && b is TInt bl)
                            return al[bl.value] ?? v;
                        break;
                    }
                case Qlx.LEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) <= 0);
                    }
                case Qlx.LOWER: // JavScript >> and >>>
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
                            if (mod == Qlx.GTR)
                                return new TInt((long)(((ulong)ia.value) >> s));
                            else
                                return new TInt((long)(((ulong)ia.value) << s));
                        }
                        break;
                    }
                case Qlx.LSS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) < 0);
                    }
                case Qlx.MINUS:
                    {
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (cx.obs[left] == null)
                        {
                            var w = domain.Eval(defpos, cx, new TInt(0), Qlx.MINUS, b);
                            return w;
                        }
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        v = domain.Eval(defpos, cx, a, op, b);
                        return v;
                    }
                case Qlx.NEQ:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return TBool.For(a.CompareTo(b) != 0);
                    }
                case Qlx.NO:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        return a;
                    }
                case Qlx.NOT:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return TBool.Unknown;
                        if (mod == Qlx.BINARY && a is TInt ia)
                            return new TInt(~ia.value);
                        if (a is TBool b)
                            return TBool.For(!b.value);
                        break;
                    }
                case Qlx.OR:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (mod != Qlx.BINARY)
                        {
                            if (a==TBool.False && b==TBool.False)
                                return TBool.False;
                            if (a == TBool.True || b==TBool.True)
                                return TBool.True;
                            return TBool.Unknown;
                        }
                        if (mod == Qlx.BINARY && a is TInt aa && b is TInt ab) // JavaScript
                            return new TInt(aa.value | ab.value);
                        else if (a is TBool ba && b is TBool bb)
                            return (ba.value is bool xa) ? 
                                (bb.value is bool xb) ? TBool.For(xa || xb) : ba 
                                : TBool.Unknown;
                        break;
                    }
                case Qlx.OVERLAPS:
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
                case Qlx.PATH:
                    {
                        if (cx.obs[right] is QlValue pi)
                            return cx.path?[pi.defpos] ?? TNull.Value;
                        break;
                    }
                case Qlx.PERIOD:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return new TPeriod(Domain.Period, new Period(a, b));
                    }
                case Qlx.PLUS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return domain.Eval(defpos, cx, a, op, b);
                    }
                case Qlx.PRECEDES:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                        {
                            if (mod == Qlx.IMMEDIATELY)
                                return TBool.For(pa.value.end.CompareTo(pb.value.start) == 0);
                            return TBool.For(pa.value.end.CompareTo(pb.value.start) <= 0);
                        }
                        break;
                    }
                case Qlx.QMARK: // v7 API for Prepare
                    {
                        return cx.values[defpos] ?? v;
                    }
                case Qlx.RBRACK:
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
                case Qlx.SUCCEEDS:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TPeriod pa && b is TPeriod pb)
                        {
                            if (mod == Qlx.IMMEDIATELY)
                                return TBool.For(pa.value.start.CompareTo(pb.value.end) == 0);
                            return TBool.For(pa.value.start.CompareTo(pb.value.end) >= 0);
                        }
                        break;
                    }
                case Qlx.TIMES:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        return domain.Eval(defpos, cx, a, op, b);
                    }
                case Qlx.UNION:
                    {
                        TypedValue a = cx.obs[left]?.Eval(cx) ?? TNull.Value;
                        if (a == TNull.Value)
                            return v;
                        TypedValue b = cx.obs[right]?.Eval(cx) ?? TNull.Value;
                        if (b == TNull.Value)
                            return v;
                        if (a is TMultiset ma && b is TMultiset mb && TMultiset.Union(ma, mb, mod == Qlx.ALL) is TMultiset mc)
                            return a.dataType.Coerce(cx, mc);
                        break;
                    }
                case Qlx.UPPER: // JavaScript <<
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
            }
            throw new DBException("22000", op).ISO();
        }
        internal override QlValue Constrain(Context cx, Domain dt)
        {
            if (domain.kind==Qlx.UNION && dt.kind!=Qlx.UNION)
            {
                var le = cx._Dom(left);
                var rg = cx._Dom(right);
                if (le is not null && rg is not null
                    && le.kind != Qlx.UNION && rg.kind != Qlx.UNION
                    && dt.CanTakeValueOf(le) && dt.CanTakeValueOf(rg))
                    return (QlValue)cx.Add(this+(_Domain,dt));
                if (le is null && rg is not null && rg.kind != Qlx.UNION
                    && dt.CanTakeValueOf(rg))
                    return (QlValue)cx.Add(this+(_Domain,dt));
                if (le is not null && rg is null && le.kind != Qlx.UNION
                    && dt.CanTakeValueOf(le))
                    return (QlValue)cx.Add(this+(_Domain,dt));
            }
            return base.Constrain(cx, dt);
        }
        internal override QlValue Invert(Context cx)
        {
            var lv = (QlValue?)cx.obs[left] ?? SqlNull.Value;
            var rv = (QlValue?)cx.obs[right] ?? SqlNull.Value;
            return op switch
            {
                Qlx.AND => new SqlValueExpr(defpos, cx, Qlx.OR, lv.Invert(cx),
                                        rv.Invert(cx), Qlx.NULL),
                Qlx.OR => new SqlValueExpr(defpos, cx, Qlx.AND, lv.Invert(cx),
                                        rv.Invert(cx), Qlx.NULL),
                Qlx.NOT => lv,
                Qlx.EQL => new SqlValueExpr(defpos, cx, Qlx.NEQ, lv, rv, Qlx.NULL),
                Qlx.GTR => new SqlValueExpr(defpos, cx, Qlx.LEQ, lv, rv, Qlx.NULL),
                Qlx.LSS => new SqlValueExpr(defpos, cx, Qlx.GEQ, lv, rv, Qlx.NULL),
                Qlx.NEQ => new SqlValueExpr(defpos, cx, Qlx.EQL, lv, rv, Qlx.NULL),
                Qlx.GEQ => new SqlValueExpr(defpos, cx, Qlx.LSS, lv, rv, Qlx.NULL),
                Qlx.LEQ => new SqlValueExpr(defpos, cx, Qlx.GTR, lv, rv, Qlx.NULL),
                _ => base.Invert(cx),
            };
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the QlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Context cx, QlValue v, RowSet r)
        {
            if (base._MatchExpr(cx, v, r)) return true;
            if (v is not SqlValueExpr e)
                return false;
            if (cx.obs[left] is QlValue lv && !lv._MatchExpr(cx, lv, r))
                return false;
            if (cx.obs[right] is QlValue rv && !rv._MatchExpr(cx, rv, r))
                return false;
            return true;
        }
        internal override CTree<long, TypedValue> Add(Context cx, CTree<long, TypedValue> ma,
            Table? tb = null)
        {
            if (op == Qlx.EQL)
            {
                if (cx.obs[left] is QlInstance sc && cx.obs[right] is SqlLiteral sr
                    && (tb == null || (cx.db.objects[sc.sPos] is TableColumn tc
                    && tc.tabledefpos == tb.defpos)))
                    return ma += (sc.sPos, sr.val);
                if (cx.obs[right] is QlInstance sd && cx.obs[left] is SqlLiteral sl
                    && (tb == null || (cx.db.objects[sd.sPos] is TableColumn td
                    && td.tabledefpos == tb.defpos)))
                    return ma += (sd.sPos, sl.val);
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
            if (cx.obs[left] is QlValue sv)
                r = sv.Needs(cx, rs, r) ?? r;
            if (op != Qlx.DOT)
                r = ((QlValue?)cx.obs[right])?.Needs(cx, rs, r) ?? r;
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is QlValue lf) r += lf.Needs(cx);
            if (cx.obs[right] is QlValue rg) r += rg.Needs(cx);
            if (cx.obs[sub] is QlValue su) r += su.Needs(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[left] is QlValue sv)
                r = sv.Needs(cx, rs);
            if (op != Qlx.DOT && cx.obs[right] is QlValue sw)
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
            if (op == Qlx.LBRACK)
                sb.Append(']');
            if (op == Qlx.LPAREN)
                sb.Append(')');
            sb.Append(')');
            if (alias != null)
            {
                sb.Append(" as ");
                sb.Append(alias);
            }
            return sb.ToString();
        }
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder();
            var lp = false;
            if (left >= 0 && right >= 0 && op != Qlx.LBRACK && op != Qlx.LPAREN)
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
            if (op == Qlx.LBRACK)
                sb.Append(']');
            if (lp || op == Qlx.LPAREN)
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
    /// A QlValue that is the null literal
    /// 
    /// </summary>
    internal class SqlNull : QlValue
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
        internal override bool _MatchExpr(Context cx, QlValue v,RowSet r)
        {
            return v is SqlNull;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        { 
            return true;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return this;
        }
        internal override bool Match(Context c, QlValue v)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            return "null";
        }
    }
    
    internal class SqlSecurity : QlValue
    {
        internal SqlSecurity(long dp,Context cx) 
            : base(new Ident("SECURITY",dp), BList<Ident>.Empty, cx, Domain._Level)
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    internal class SqlFormal : QlValue 
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
            var cf = mem[QlInstance.SPos];
            if (cf != null)
            { sb.Append(" from: "); sb.Append((long)cf); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The SqlLiteral subclass
    /// 
    /// </summary>
    internal class SqlLiteral : QlValue
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return this;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return true;
        }
        internal override bool Match(Context c, QlValue v)
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
            if (val is TQParam tq && cx.values[tq.qid] is TypedValue tv)
            {
                r = New(r.defpos,r.mem+(_Val, tv));
                cx.Add(r);
            }
            return r;
        }
        /// <summary>
        /// test for structural equivalence
        /// </summary>
        /// <param name="v">an QlValue</param>
        /// <returns>whether they are structurally equivalent</returns>
        internal override bool _MatchExpr(Context cx,QlValue v,RowSet r)
        {
            if (v is not SqlLiteral)
                return false;
            return CompareTo(v) == 0;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return new CTree<long, bool>(defpos, true);
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue _Eval(Context cx)
        {
            if (val is TQParam tq && cx.values[tq.qid] is TypedValue tv && tv != TNull.Value)
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
            return val is not TQParam;
        }
        internal override Domain FindType(Context cx, Domain dt)
        {
            var vt = val.dataType;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22G03", dt.kind, vt.kind).ISO();
            if (vt.kind==Qlx.INTEGER)
                return dt; // keep result options open
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
        internal override QlValue Constrain(Context cx, Domain dt)
        {
            if (dt.CanTakeValueOf(domain))
                return (QlValue)cx.Add(this+(_Domain,dt));
            return base.Constrain(cx, dt);
        }
        internal override string ToString(string sg, Remotes rf, CList<long> cs, CTree<long, string> ns, Context cx)
        {
            if (val.dataType.kind == Qlx.CHAR)
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
    internal class SqlRow : QlValue
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
        public SqlRow(long dp, Context cx, Domain xp, CList<long> vs, BTree<long, object>? m = null)
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
                if (b.value() is long p && cx.obs[p] is QlValue sv)
                    m += (p, sv);
            return m;
        }
        static BTree<long, object> _Mem(Context cx, BList<DBObject> vs, BTree<long, object>? m)
        {
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Qlx.ROW, vs));
            m ??= new BTree<long,object>(_Domain,dm) + (Domain.Aggs, dm.aggs);
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var ob = b.value();
                m += (ob.defpos, ob);
            }
            return m;
        }
        protected static BTree<long, object> _Inf(Context cx, BTree<long, object>? m,
    Domain xp, CList<long> vs)
        {
            var cb = xp.First();
            var bs = BList<DBObject>.Empty;
            var r = m ?? BTree<long, object>.Empty;
            for (var b = vs.First(); b != null; b = b.Next(), cb = cb?.Next())
                if (b.value() is long p)
                {
                    var ob = cx.obs[p] as QlValue ?? SqlNull.Value;
                    bs += ob;
                    r += (p, ob);
                }
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(),cx,Qlx.ROW,bs,bs.Length));
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
        public static SqlRow operator +(SqlRow s, (Context,QlValue) x)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRow)base._Replace(cx, so, sv);
            var cs = CList<long>.Empty;
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var v = (QlValue)cx._Replace(p, so, sv);
                    cs += v.defpos;
                    vs += v;
                    if (v.defpos != b.value())
                        ch = true;
                }
            if (ch)
            {
                var dm = cx.Add(new Domain(cx.GetUid(), cx, Qlx.ROW, vs));
                r += (_Domain, dm);
                r += (_Depth, _Depths(vs));
            }
            return r;
        }
        internal override bool isConstant(Context cx)
        {
            for (var b = mem.PositionAt(0); b != null; b = b.Next())
                if (b.value() is QlValue v && !v.isConstant(cx))
                    return false;
            return true;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s && !s.Grouped(cx,gs))
                        return false;
            return true;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            var vs = BList<DBObject>.Empty;
            var ch = false;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& c.obs[p] is QlValue v)
                {
                    var nv = v.Having(c, dm, ap);
                    vs += nv.domain;
                    ch = ch || nv != v;
                }
            return ch ? (QlValue)c.Add(new SqlRow(c.GetUid(), c, vs)) : this;
        }
        internal override bool Match(Context c, QlValue v)
        {
            if (v is SqlRow r)
            {
                var rb = r.domain.rowType.First();
                for (var b = domain.rowType.First(); b != null && rb != null;
                    b = b.Next(), rb = rb.Next())
                    if (b.value() is long p && c.obs[p] is QlValue s
                        && rb.value() is long rp && c.obs[rp] is QlValue t
                        && !s.Match(c, t))
                        return false;
                return true;
            }
            return false;
        }
        internal override (BList<DBObject>, BTree<long,object>) Resolve(Context cx, RowSet sr, 
            BTree<long,object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50706");
            var cs = BList<DBObject>.Empty;
            var om = m;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue c)
                {
                    if (p > ap)
                    {
                        (var ls, m) = c.Resolve(cx, sr, m, ap);
                        cs += ls;
                    }
                    else
                        cs += c;
                }
            if (m != om)
            {
                var sv = new SqlRow(defpos, cx, cs) + (_From,sr.defpos);
                cx.Replace(this, sv);
            }
            var r = (QlValue?)cx.obs[defpos] ?? throw new PEException("PE1800");
            return (new BList<DBObject>(r), m);
        }
        internal override DBObject AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var ch = false;
            for (var b = r.domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue a)
                {
                    a = (QlValue)a.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    r += (cx, a);
                }
            return ch?(QlValue)cx.Add(r):this;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s && !s.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is QlValue s && !s.KnownBy(cx, cs, ambient))
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
                if (b.value() is long p && cx.obs[p] is QlValue s)
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
                if (b.value() is long p && cx.obs[p] is QlValue sv)
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
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
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
                if (b.value() is long p&& cx.obs[p] is QlValue s)
                    qn = s.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    r += s.Needs(cx, rs);
            return r;
        }
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            sb.Append(" (");
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
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
                if (b.value() is QlValue s)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append('='); sb.Append(s);
                }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// Prepare an QlValue with reified columns for use in trigger
    /// 
    /// </summary>
    internal class SqlOldRow : SqlRow
    {
        internal SqlOldRow(Ident ic, Context cx, RowSet fm)
            : base(ic.uid, _Mem(ic,cx,fm))
        {
            cx.Add(this);
            cx.Add(ic.ident, ic.lp, this);
        }
        protected SqlOldRow(long dp, BTree<long, object> m) : base(dp, m) 
        {  }
        static BTree<long, object> _Mem(Ident ic, Context cx, RowSet fm)
        {
            var tg = cx._Dom(fm.target);
            var r = fm.mem + (ObInfo.Name, ic.ident) + (_From, fm.defpos) + (_Depth, fm.depth + 1);
            var ids = Names.Empty;
            var on = cx.names;
            for (var b = tg?.rowType.First(); b is not null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is TableColumn tc)
                {
                    var tn = tc.NameFor(cx);
                    if (tn == "")
                        throw new PEException("PE5030");
                    var f = new SqlField(cx.GetUid(), tn, b.key(), ic.uid, tc.domain, p);
                    var cix = f.defpos;
                    cx.Add(f);
                    ids += (tn, (ic.lp, cix));
                }
            cx.defs += (ic.uid, ids);
            var oi = (tg?.infos[cx.role.defpos] ?? new ObInfo(ic.ident, Grant.AllPrivileges))
                    + (ObInfo._Names, ids);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            cx.names = on;
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
            if (ta is not null)
                ta.values += (Trigger.OldRow, v);
            base.Set(cx, v);
        }
    }
    
    internal class SqlNewRow : SqlRow
    {
        internal SqlNewRow(Ident ic, Context cx, RowSet fm)
            : base(ic.uid, _Mem(ic, cx, fm))
        {
            cx.Add(this);
            cx.Add(ic.ident, ic.lp, this);
        }
        protected SqlNewRow(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic, Context cx, RowSet fm)
        {
            var tg = cx._Dom(fm.target);
            var r = fm.mem + (_Depth,fm.depth+1)
                   + (ObInfo.Name, ic.ident) + (_From, fm.defpos);
            var ids = Names.Empty;
            var on = cx.names;
            for (var b = tg?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is TableColumn co)
                {
                    var f = new SqlField(cx.GetUid(), co.NameFor(cx), -1, ic.uid, 
                        co.domain,co.defpos);
                    var cix = f.defpos;
                    cx.Add(f);
                    ids += (f.name ?? "", (ic.lp, cix));
                }
            cx.defs += (ic.uid, ids);
            var oi = (tg?.infos[cx.role.defpos]??new ObInfo(ic.ident,Grant.AllPrivileges))
                    +(ObInfo._Names, ids);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            cx.names = on;
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    
    internal class SqlRowArray : QlValue
    {
        internal const long
            _Rows = -319; // CList<long> QlValue
        internal CList<long> rows =>
            (CList<long>?)mem[_Rows]?? CList<long>.Empty;
        public SqlRowArray(long dp,Context cx,Domain ap,CList<long> rs) 
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
                        m += (_Depth,cx._DepthBV((CList<long>)ob,et.depth));
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRowArray)base._Replace(cx, so, sv);
            var rws = CList<long>.Empty;
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
        internal override bool isConstant(Context cx)
        {
            var r = true;
            for (var b = rows.First(); b != null && r; b = b.Next())
                if (cx.obs[b.value()] is QlValue v)
                    r = v.isConstant(cx);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s && !s.Grouped(cx, gs))
                    return false;
            return true;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRowArray)base.AddFrom(cx, q);
            var rws = CList<long>.Empty;
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
            return (QlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
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
                if (b.value() is long p && cx.obs[p] is QlValue s && !s.KnownBy(cx, cs, ambient))
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                vs += v.Eval(cx);
            return new TList(domain, vs);
        }
        internal override RowSet RowSetFor(long ap, long dp, Context cx, CList<long> us,
            CTree<long, Domain> re)
        {
            var dm = domain;
            var rs = BList<(long, TRow)>.Empty;
            var xp = (Domain)dm.New(dm.mem + (Domain.Kind, Qlx.TABLE));
            var isConst = true;
            if (us != null && !Context.Match(us,xp.rowType))
                xp = (Domain)dm.New(xp.mem + (Domain.RowType, us)
                    + (Domain.Representation, xp.representation + re));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
                if (b.value() is long p)
                {
                    var v = cx.obs[p] ?? throw new DBException("42000",""+dp);
                    isConst = (v as QlValue)?.isConstant(cx) == true;
                    var x = v.Eval(cx);
                    var y = x.ToArray() ?? throw new DBException("42000",""+dp);
                    rs += (v.defpos, new TRow(xp, y));
                }
            if (isConst)
                return new ExplicitRowSet(ap, dp, cx, xp, rs);
            return new SqlRowSet(dp, cx, xp, rows);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    qn = v.Needs(cx,r,qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50707");
            var r = this;
            BList<DBObject> ls;
            var ch = false;
            var rs = CList<long>.Empty;
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    if (p > ap)
                    {
                        (ls, m) = v.Resolve(cx, sr, m, ap);
                        if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                        {
                            ch = true; v = nv;
                        }
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
   
    internal class SqlSelectArray : QlValue
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    internal class SqlValueArray : QlValue
    {
        internal const long
            _Array = -328, // CList<long> QlValue
            Svs = -329; // long QlValueQuery
        /// <summary>
        /// the array
        /// </summary>
        public CList<long> array =>(CList<long>?)mem[_Array]??CList<long>.Empty;
        // alternatively, the source
        public long svs => (long)(mem[Svs] ?? -1L);
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(long dp,Context cx,Domain xp,CList<long> v)
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
                    m += (_Depth, cx._DepthBV((CList<long>)ob,et.depth));
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                r += v._Rdc(cx);
            if (cx.obs[svs] is QlValue s) 
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueArray)base._Replace(cx, so, sv);
            var ar = CList<long>.Empty;
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
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = CList<long>.Empty;
            var ag = domain.aggs;
            var ch = false;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v) {
                    var a = (QlValue)v.AddFrom(cx, q);
                    if (a.defpos != b.value())
                        ch = true;
                    ar += a.defpos;
                    ag += a.IsAggregation(cx,ag);
                }
            if (ch)
                r += (cx,_Array, ar);
            if (cx.obs[svs] is QlValue s)
            {
                s = (QlValue)s.AddFrom(cx, q);
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
            return (QlValue)cx.Add(r);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return ((QlValue?)cx.obs[svs])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return ((QlValue?)cx.obs[svs])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            if (cx.obs[svs] is QlValue s)
                tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    tg = v.AddIn(cx, rb, tg);
            if (cx.obs[svs] is QlValue s)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    qn = v.Needs(cx, r, qn);
            if (cx.obs[svs] is QlValue s)
                qn = s.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50708");
            BList<DBObject> ls;
            var r = this;
            var ch = false;
            var vs = CList<long>.Empty;
            for (var b = array.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v) {
                    if (p > ap)
                    {
                        (ls, m) = v.Resolve(cx, sr, m, ap);
                        if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                        {
                            ch = true; v = nv;
                        }
                    }
                    vs += v.defpos;
                }
            var sva = (QlValue?)cx.obs[svs] ?? SqlNull.Value;
            if (sva!=SqlNull.Value && svs>ap)
            {
                (ls, m) = sva.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=sva.defpos)
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
    internal class SqlValueMultiset : QlValue
    {
        internal const long
            MultiSqlValues = -302; // CList<long> QlValue
        /// <summary>
        /// the array
        /// </summary>
        public CList<long> multi => (CList<long>)(mem[MultiSqlValues] ?? CList<long>.Empty);
        /// <summary>
        /// construct an SqlValueMultiset value
        /// </summary>
        /// <param name="a">the array</param>
        public SqlValueMultiset(long dp, Context cx, Domain xp, CList<long> v)
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
                    m += (_Depth, cx._DepthBV((CList<long>)ob, et.depth));
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
                if (cx.obs[b.key()] is QlValue v)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueMultiset)base._Replace(cx, so, sv);
            var mu = CList<long>.Empty;
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
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var dm = domain;
            var r = (SqlValueMultiset)base.AddFrom(cx, q);
            var mu = CTree<long,bool>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    var a = (QlValue)v.AddFrom(cx, q);
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
            return (QlValue)cx.Add(r);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = multi?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = multi?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, cs, ambient))
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50709");
            BList<DBObject> ls;
            var r = this;
            var d = depth;
            var vs = CList<long>.Empty;
            for (var b = multi.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    if (p > ap)
                    {
                        (ls, m) = v.Resolve(cx, sr, m, ap);
                        if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                        {
                            v = nv;
                            d = Math.Max(d, nv.depth + 1);
                        }
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
    internal class SqlValueSet : QlValue
    {
        internal const long
            Elements = -261; // CList<long> QlValue
        /// <summary>
        /// the array
        /// </summary>
        public CList<long> els => (CList<long>)(mem[Elements] ?? CList<long>.Empty);
        /// <summary>
        /// construct an SqlValueSet value
        /// </summary>
        /// <param name="v">the elements</param>
        public SqlValueSet(long dp, Context cx, Domain xp, CList<long> v)
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
                    m += (_Depth, cx._DepthBV((CList<long>)ob, et.depth));
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
                if (cx.obs[b.key()] is QlValue v)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlValueSet)base._Replace(cx, so, sv);
            var es = CList<long>.Empty;
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
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var dm = domain;
            var r = (SqlValueMultiset)base.AddFrom(cx, q);
            var mu = CList<long>.Empty;
            var ag = dm.aggs;
            var ch = false;
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    var a = (QlValue)v.AddFrom(cx, q);
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
            return (QlValue)cx.Add(r);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = els?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, cs, ambient))
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    tg = v.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50710");
            BList<DBObject> ls;
            var r = this;
            var d = depth;
            var vs = CList<long>.Empty;
            for (var b = els.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    if (p > ap)
                    {
                        (ls, m) = v.Resolve(cx, sr, m, ap);
                        if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                        {
                            v = nv;
                            d = Math.Max(d, nv.depth + 1);
                        }
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
    internal class QlMatchValue : QlValue
    {
        internal const long
            _NextStmt = -171; // long Executable
        public long match => (long)(mem[_NextStmt] ?? -1L);
        internal QlMatchValue(Context cx,Executable ms,Domain  dm)
            : base(cx.GetUid(), ms.mem + (_NextStmt, ms.defpos)
                  +(_Domain,((MatchStatement)ms).domain))
        { }
        protected QlMatchValue(long dp, BTree<long, object> m)
            : base(dp, m) { }
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[match] is not MatchStatement ms) return TNull.Value;
            cx = ms._Obey(cx); // NB the result is available in cx.result in all cases (e.g. CONTENT)
            var rs = cx.result as RowSet;
            var vl = cx.val;
            switch(domain.kind)
            {
                case Qlx.DOCARRAY: return new TDocArray(cx,rs ?? TrueRowSet.OK(cx));
                case Qlx.NO:
                case Qlx.LIST:
                    {
                        var rl = BList<TypedValue>.Empty;
                        for (var b = rs?.First(cx); b != null; b = b.Next(cx))
                            rl += b;
                        return new TList(domain, rl);
                    }
                case Qlx.ROW: return (TypedValue?)rs?.First(cx) ?? TNull.Value;
                default: return (domain.Length>0)?rs?.First(cx)?[0] ?? TNull.Value:TNull.Value;
            }
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var nm = cx.Fix(match);
            if (nm != match)
                m += (_NextStmt, nm);
            return base._Fix(cx, m);
        }
        internal override DBObject Relocate(long dp, Context cx)
        {
            return new QlMatchValue(dp, _Fix(cx,mem));
        }
        public override string ToString()
        {
            return base.ToString() + " Match "+Uid(match);
        }
    }
    /// <summary>
    /// A subquery
    /// 
    /// </summary>
    internal class QlValueQuery : QlValue
    {
        /// <summary>
        /// the subquery
        /// </summary>
        public CList<long> gqlStms =>(CList<long>)(mem[AccessingStatement.GqlStms]??CList<long>.Empty);
        public QlValueQuery(long dp,Context cx,Domain r,Domain xp,CList<long> ss)
            : base(dp, _Mem(cx,ss) + (_Domain,r) + (Domain.Aggs,r.aggs)
                  + (AccessingStatement.GqlStms, ss) + (RowSet._Scalar,xp.kind!=Qlx.TABLE))
        {
            r += (cx,SelectRowSet.ValueSelect, dp);
            cx.Add(r);
        }
        static BTree<long, object> _Mem(Context cx, CList<long> ss)
        {
            var d = 1;
            for (var b = ss.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is Executable e)
                    d = Math.Max(d, e.depth + 1);
            return new BTree<long, object>(_Depth, d);
        }
        protected QlValueQuery(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QlValueQuery operator +(QlValueQuery et, (long, object) x)
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
            return (QlValueQuery)et.New(m + x);
        }
        public static QlValueQuery operator +(QlValueQuery e, (Context, long, object) x)
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
            return (QlValueQuery)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QlValueQuery(defpos,m);
        }
        internal override RowSet RowSetFor(long ap, long dp, Context cx, CList<long> us,
            CTree<long,Domain> re)
        {
            var ef = gqlStms.First();
            var r = (cx.obs[ef?.value() ?? -1] as Executable)?._Obey(cx, ef?.Next())?.result as RowSet
                ?? throw new DBException("22G12");
            if (us == null) // || Context.Match(us,r.rowType))
                return r;
            var f = true;
            var rb = r.representation.First();
            for (var b = re.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                if (b.value()?.CanTakeValueOf(rb.value()) == false)
                    f = false;
            if (f)
                return r;
            var xp = r +(Domain.RowType, us)
                +(Domain.Representation,r.representation+re);
            return new SelectedRowSet(cx, ap, xp, r);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var ef = gqlStms.First();
            var r = (cx.obs[ef?.value() ?? -1] as Executable)?._Obey(cx, ef?.Next());
            return r?.result?._Rdc(cx)??CTree<long,bool>.Empty;
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new QlValueQuery(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ne = cx.FixLl(gqlStms);
            if (ne != gqlStms)
                r = cx.Add(r, AccessingStatement.GqlStms, ne);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (QlValueQuery)base._Replace(cx, so, sv);
            var ne = cx.ReplacedLl(gqlStms);
            if (ne != gqlStms)
                r +=(cx, AccessingStatement.GqlStms, ne);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override QlValue Constrain(Context cx, Domain dt)
        {
            var r = base.Constrain(cx, dt);
            if (dt.kind != Qlx.TABLE && !scalar)
                r = (QlValueQuery)cx.Add(this + (RowSet._Scalar, true));
            return r;
        }
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.values[defpos] is TypedValue tv)
                return tv;
            var ef = gqlStms.First();
            var re = (cx.obs[ef?.value() ?? -1] as Executable)?._Obey(cx, ef?.Next())?.result as RowSet
                ?? throw new DBException("22G12");
            if (scalar)
            {
                var r = re.First(cx)?[0] ?? domain.defaultValue;
                //        cx.funcs -= ers.defpos;
                return r;
            }
            var rs = BList<TypedValue>.Empty;
            for (var b = re.First(cx); b != null; b = b.Next(cx))
                rs += b;
            var rl = new TList(domain, rs);
     //       cx.values += (defpos, rl);
            return rl;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    tg = s.AddIn(cx, rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long p, CTree<long, bool> qn)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = gqlStms.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is QlValue s)
                    r += s?.Needs(cx, p, qn) ?? CTree<long, bool>.Empty;
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = gqlStms.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]?.Needs(cx, rs)??CTree<long,bool>.Empty;
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (scalar)
                sb.Append(" scalar ");
            sb.Append(' '); sb.Append(gqlStms);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Column Function QlValue class
    /// </summary>
    internal class ColumnFunction : QlValue
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return this;
        }
        internal override bool Match(Context cx, QlValue v)
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
    
    internal class SqlCursor : QlValue
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCursor)base._Replace(cx, so, sv);
            var sp = cx.ObReplace(spec,so,sv);
            if (sp != spec)
                r +=(cx, Spec, sp);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    
    internal abstract class SqlCall : QlValue
    {
        internal const long
            Parms = -133, // CList<long> QlValue
            ProcDefPos = -134, // long Procedure
            Var = -135; // long QlValue
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
		public CList<long> parms => (CList<long>)(mem[Parms] ?? CList<long>.Empty);
        public SqlCall(long lp, Context cx, Procedure pr, CList<long> acts, long tg=-1L)
        : base(lp, _Mem(lp,cx,pr) + (Parms, acts) + (ProcDefPos,pr.defpos) 
              + (Var,tg)+(_Domain,pr.domain))
        {
            cx.Add(this);
        }
        internal SqlCall(long dp, string pn, CList<long> acts, long tg)
: base(dp, BTree<long,object>.Empty + (Parms, acts) + (ObInfo.Name, pn) + (Var, tg))
        { }
        protected SqlCall(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,Procedure proc)
        {
            var m = BTree<long, object>.Empty;
            var ro = cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
            if (proc.infos[ro.defpos] is not ObInfo pi || pi.name is null)
                throw new DBException("42105").Add(Qlx.EXECUTE);
            if (proc.domain.rowType.Count > 0)
            {
                var prs = new ProcRowSet(cx, 0L, proc) + (ObInfo.Name, pi.name)
                    + (CallStatement.Call, dp);
                cx.Add(prs);
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
                    m += (_Depth, cx._DepthBV((CList<long>)o, s.depth));
                    break;
                case ProcDefPos:
                case Var:
                    m += (_Depth, Math.Max(s.depth,(cx.obs[(long)o]?.depth??0) + 1));
                    break;
            }
            return (SqlCall)s.New(m + (p,o));
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
        internal abstract SqlCall? Resolve(Context cx);
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var np = cx.Fix(procdefpos);
            if (np != procdefpos)
                r += (ProcDefPos, np);
            var ns = cx.FixLl(parms);
            if (parms != ns)
                r += (Parms, ns);
            var va = cx.Fix(var);
            if (var != va)
                r += (Var, va);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlCall)base._Replace(cx, so, sv);
            var nv = cx.ObReplace(var, so, sv);
            if (nv != var)
                r +=(cx,Var, nv);
            var np = cx.ReplacedLl(parms);
            if (np != parms)
                r +=(cx,Parms, np);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return true;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long,bool>.Empty;
            for (var b = parms.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue v)
                        r += v.Needs(cx, rs);
            return r;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlCall)base.AddFrom(cx, q);
            if (cx.obs[var] is QlValue a)
            {
                a = (QlValue)a.AddFrom(cx, q);
                if (a.defpos != var)
                    r += (cx, Var, a.defpos);
            }
            var vs = CList<long>.Empty;
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    vs += v.AddFrom(cx, q).defpos;
            r += (cx, Parms, vs);
            return (SqlCall)cx.Add(r);
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, long ap,
            Grant.Privilege pr = Grant.Privilege.Select, string? a = null, TableRowSet? ur = null)
        {
            var ro = cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
            if (cx.db.objects[procdefpos] is not Procedure proc
                || proc.infos[proc.definer] is not ObInfo pi || pi.name is null)
                throw new PEException("PE6840");
            var prs = new ProcRowSet(this, ap, cx) + (RowSet.Target, procdefpos) + (ObInfo.Name, pi.name);
            cx.Add(prs);
            return prs;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (var != -1L && (cx.obs[var]?.Calls(defpos, cx) ?? false))
                return true;
            return procdefpos == defpos || Calls(parms, defpos, cx);
        }
        internal static bool Calls(CList<long> ss, long defpos, Context cx)
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
    /// An QlValue that is a procedure/function call or static method
    /// 
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(long dp, Context cx, Procedure pr, 
            CList<long> acts) : base(dp, cx, pr,acts,-1L) 
        {
            cx.Add(this);
        }
        public SqlProcedureCall(long dp, Context cx, string pn, CList<long> acts, QlValue tg)
    : base(dp, pn, acts, tg.defpos)
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
                    m += (_Depth, cx._DepthBV((CList<long>)o, s.depth));
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
            var oc = cx.values;
            var ac = proc.Exec(cx, parms);
            var r = ac.val ?? domain.defaultValue;
            cx.values = oc;
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override SqlCall? Resolve(Context cx)
        {
            if (procdefpos < 0 && cx.db.Signature(cx, domain.rowType) is CList<Domain> ms
                        && cx._Ob(cx.role.procedures[name ?? ""]?[ms] ?? -1L) is Procedure pr)
                return (SqlCall)cx.Add(new SqlProcedureCall(defpos, cx, pr, parms));
            return null;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                    r += v.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// A QlValue that is evaluated by calling a method
    /// 
    /// </summary>
    internal class SqlMethodCall : SqlCall // instance methods
    {
        /// <summary>
        /// construct a new MethodCall QlValue.
        /// At construction time the proc and target will be unknown.
        /// Domain of a MethodCall is the valueType domain
        /// Target will be null for a constructor call
        /// </summary>
        public SqlMethodCall(long dp, Context cx, Procedure pr,
            CList<long> acts, QlValue? tg) : base(dp,cx, pr, acts, tg?.defpos??-1L)
        {
            cx.Add(this);
        }
        public SqlMethodCall(long dp, Context cx, string pn,CList<long> acts, QlValue tg) 
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
                    m += (_Depth, cx._DepthBV((CList<long>)o, s.depth));
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
        internal override SqlCall? Resolve(Context cx)
        {
            if (cx.obs[var] is QlValue tg && tg.domain is UDType ut && procdefpos<0
                        && cx.db.Signature(cx, domain.rowType) is CList<Domain> ms
                        && cx.db.objects[ut.infos[cx.role.defpos]?.methodInfos[name ?? ""]?[ms] ?? -1L] is Method pr)
                return (SqlCall)cx.Add(new SqlMethodCall(defpos, cx, pr, parms, tg));
            return null;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn += (var, true);
            return base.Needs(cx, r, qn);
        }
        /// <summary>
        /// This override resolves overloading of method names
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="sr"></param>
        /// <param name="m"></param>
        /// <param name="ap"></param>
        /// <returns></returns>
        /// <exception cref="PEException"></exception>
        internal override (BList<DBObject>, BTree<long,object>) Resolve(Context cx, RowSet sr, 
            BTree<long,object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50711");
            if (domain.kind!=Qlx.Null)
                return (new BList<DBObject>(this),m);
            BList<DBObject> ls;
            (ls, m) = base.Resolve(cx, sr, m, ap);
            if (ls[0] is SqlMethodCall mc && cx.obs[mc.var] is QlValue ov && mc.name is not null)
            {
                (ls, m) = ov.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv)
                    ov = nv;
                if (cx.role is not null && ov.domain.infos[cx.role.defpos] is ObInfo oi && name is string nm
                    && cx.db.objects[oi.methodInfos[nm]?[cx.db.Signature(cx,parms)] ?? -1L] is Procedure pr)
                { 
                    mc = mc + (cx, Var, ov.defpos) + (cx, ProcDefPos, pr.defpos) + (_Domain,pr.domain);
                    cx.undefined -= mc.defpos;
                    return (new BList<DBObject>(cx.Add(mc)), m);
                }
            }
            return (new BList<DBObject>(this), mem);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        /// <summary>
        /// Evaluate the method call and return the value
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[var] is not QlValue v  || cx.role==null)
                throw new PEException("PE241");
            var vv = v.Eval(cx);
            for (var b = v.domain.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && vv[bp] is TypedValue tv)
                    cx.values += (bp, tv);
            var p = procdefpos;
            if (cx.db.objects[p] is not Method me)
                throw new DBException("42108", Uid(defpos));
            var oc = cx.values;
            var act = new CalledActivation(cx,me);
            var proc = (Method)me.Instance(defpos,act);
            var r = proc.Exec(cx, var, parms).val;
            cx.values = oc;
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[var] is not QlValue v)
                throw new PEException("PE47196");
            var r = v.Needs(cx, rs);
            for (var b = parms.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue x)
                    r += x.Needs(cx, rs);
            return r;
        }
    }
    /// <summary>
    /// An QlValue that is a constructor expression
    /// 
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        public SqlConstructor(long dp, Context cx, Procedure pr, CList<long> args)
            : base(dp, cx, pr, args)
        {
            cx.Add(this);
        }
        internal SqlConstructor(long dp, Context cx, Domain ut, CList<long> args)
            :base(dp, BTree<long, object>.Empty + (Parms, args) + (ObInfo.Name, ut.name) + (_Domain, ut))
        { }
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
            return new SqlConstructor(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SqlConstructor(dp, m);
        }
        internal override SqlCall? Resolve(Context cx)
        {
            if (domain is UDType ut && procdefpos < 0
                        && cx.db.Signature(cx, domain.rowType) is CList<Domain> ms
                        && cx.db.objects[ut.infos[cx.role.defpos]?.methodInfos[name ?? ""]?[ms] ?? -1L] is Method pr)
                return (SqlCall)cx.Add(new SqlConstructor(defpos, cx, pr, parms));
            return null;
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
    }
    /// <summary>
    /// An QlValue corresponding to a default constructor call
    /// 
    /// </summary>
    internal class SqlDefaultConstructor : QlValue
    {
        internal const long
            Sce = -336; // long SqlRow
        /// <summary>
        /// the type
        /// </summary>
        public long sce=>(long)(mem[Sce]??-1L);
        /// <summary>
        /// construct a QlValue default constructor for a type
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="lk">the actual parameters</param>
        public SqlDefaultConstructor(long dp, Context cx, Domain u, CList<long> ins)
            : base(dp, _Mem(cx.GetUid(),cx,(UDType)u,ins))
        {
            cx.Add(this);
        }
        protected SqlDefaultConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(long ap, Context cx, UDType u, CList<long> ps)
        {
            var rb = u.representation.First();
            for (var b = ps.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlDefaultConstructor)base.AddFrom(cx, q);
            if (cx.obs[r.sce] is SqlRow sc)
            {
                var a = sc.AddFrom(cx, q);
                cx.Add(a);
            }
            return (QlValue)cx.Add(r);
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
                        if (b.value() is long p && cx.obs[p] is QlValue v
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50712");
            var r = this;
            BList<DBObject> ls;
            var sc = (QlValue?)cx.obs[sce] ?? SqlNull.Value;
            if (sc != SqlNull.Value && sc.defpos>ap)
            {
                (ls, m) = sc.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != sc.defpos)
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
            if (cx.obs[sce] is QlValue nv)
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
    internal class SqlFunction : QlValue
    {
        internal const long
            Filter = -338, // CTree<long,bool> QlValue
            Mod = -340, // Qlx
            Monotonic = -341, // bool
            Op1 = -342, // long QlValue
            Op2 = -343, // long QlValue
            _Val = -345,//long QlValue
            Window = -346, // long WindowSpecification
            WindowId = -347; // long
        public Qlx op => (Qlx)(mem[SqlValueExpr.Op] ?? Qlx.NO);
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Qlx mod => (Qlx)(mem[Mod] ?? Qlx.NO);
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
        /// Constructor: a function QlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(long ap, long dp, Context cx, Qlx f, QlValue? vl, QlValue? o1, QlValue? o2, Qlx m,
            BTree<long, object>? mm = null)
            : base(dp, _Mem(cx, vl, o1, o2, (mm ?? BTree<long, object>.Empty) + (_Domain, _Type(cx, f, vl, o1))
                + (ObInfo.Name, f.ToString()) + (SqlValueExpr.Op, f) + (Mod, m) + (Scope,ap)))
        { 
            cx.Add(this);
            vl?.ConstrainScalar(cx);
        }
        protected SqlFunction(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Context cx, QlValue? vl, QlValue? o1, QlValue? o2, BTree<long, object>? m)
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
                        if (cx.obs[et.op1] is QlValue sw && ob is long p && cx.obs[p] is QlValue w)
                            m += _Type(cx, et.op, w, sw).mem;
                        goto default;
                    }
                case _Val:
                    {
                        if (cx.obs[et.val] is QlValue sv && ob is long p && cx.obs[p] is QlValue u)
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
        internal override CTree<long, bool> IsAggregation(Context cx,CTree<long,bool>ags)
        {
            var r = CTree<long, bool>.Empty;
            if (window >= 0) // Window functions do not aggregate rows!
                return r;
            var vl = cx.obs[val] as QlValue;
            var va = aggregatable(vl?.domain.kind??Qlx.NO);
            if (vl is not null && aggregates(op) && va)
                r += (defpos, true);
            if (vl is not null)
                r += vl.IsAggregation(cx,ags);
            if (cx.obs[op1] is QlValue o1)
                r += o1.IsAggregation(cx,ags);
            if (cx.obs[op2] is QlValue o2)
                r += o2.IsAggregation(cx,ags);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            if (aggregates(op))
            {
                for (var b = dm.aggs.First(); b != null; b = b.Next())
                    if (c.obs[b.key()] is SqlFunction sf && Match(c, sf))
                        return sf;
                return base.Having(c, dm, ap);
            }
            QlValue? nv = null, n1 = null, n2 = null;
            bool ch = false;
            if (c.obs[val] is QlValue vl)
            { nv = vl.Having(c, dm, ap); ch = nv != vl; }
            if (c.obs[op1] is QlValue o1)
            { n1 = o1.Having(c, dm, ap); ch = n1 != o1; }
            if (c.obs[op2] is QlValue o2)
            { n2 = o2.Having(c, dm, ap); ch = n2 != o2; }
            return ch ? (QlValue)c.Add(new SqlFunction(ap,c.GetUid(), c, op, nv, n1, n2, mod)) : this;
        }
        internal override bool Match(Context c, QlValue v)
        {
            if (v is SqlFunction f)
            {
                if (filter != CTree<long, bool>.Empty)
                {
                    var fb = f.filter.First();
                    for (var b = filter.First(); b != null; b = b.Next(), fb = fb.Next())
                        if (fb == null || (c.obs[b.key()] is QlValue le && c.obs[fb.key()] is QlValue fe
                            && !le.Match(c, fe)))
                            return false;
                    if (fb != null)
                        return false;
                }
                if (op != f.op || mod != f.mod || windowId != f.windowId)
                    return false;
                if (c.obs[op1] is QlValue o1 && c.obs[f.op1] is QlValue f1 && !o1.Match(c, f1))
                    return false;
                if (c.obs[op2] is QlValue o2 && c.obs[f.op2] is QlValue f2 && !o2.Match(c, f2))
                    return false;
                if (c.obs[val] is QlValue vl && c.obs[f.val] is QlValue fv && !vl.Match(c, fv))
                    return false;
                if (window >= 0 || f.window >= 0)
                    return false;
                return true;
            }
            return false;
        }
        internal override bool _MatchExpr(Context cx, QlValue v, RowSet r)
        {
            if (v is not SqlFunction f || op != f.op || op1 != f.op1 || op2 != f.op2)
                return false;
            if (cx._Ob(val) is not QlValue vv || cx._Ob(f.val) is not QlValue fv)
                return false;
            return vv._MatchExpr(cx, fv, r);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is QlValue v) r += v._Rdc(cx);
            if (cx.obs[op1] is QlValue o1) r += o1._Rdc(cx);
            if (cx.obs[op2] is QlValue o2) r += o2._Rdc(cx);
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
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlFunction)base.AddFrom(cx, q);
            if (cx.obs[r.val] is QlValue ov)
            {
                var a = ov.AddFrom(cx, q);
                if (a.defpos != r.val)
                    r += (cx, _Val, a.defpos);
            }
            if (cx.obs[r.op1] is QlValue o1)
            {
                var a = o1.AddFrom(cx, q);
                if (a.defpos != r.op1)
                    r += (cx, Op1, a.defpos);
            }
            if (cx.obs[r.op2] is QlValue o2)
            {
                var a = o2.AddFrom(cx, q);
                if (a.defpos != r.op2)
                    r += (cx, Op2, a.defpos);
            }
            return (QlValue)cx.Add(r);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            if (cx.obs[val] is QlValue vl && val>ap)
            {
                (ls, m) = vl.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != vl.defpos)
                {
                    ch = true;
                    r += (_Val, nv);
                }
            }
            if (cx.obs[op1] is QlValue o1 && op1>ap)
            {
                (ls, m) = o1.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != o1.defpos)
                {
                    ch = true;
                    r += (Op1, nv.defpos);
                }
            }
            if (cx.obs[op2] is QlValue o2 && op2>ap)
            {
                (ls, m) = o2.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != o2.defpos)
                {
                    ch = true;
                    r += (Op2, nv.defpos);
                }
            }
            if (cx.obs[window] is WindowSpecification ow && window>ap)
            {
                (ls, m) = ow.Resolve(cx, sr, m, ap);
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlFunction)base._Replace(cx, so, sv);
            var w = filter;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (QlValue)cx._Replace(b.key(), so, sv);
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
            if (domain.kind == Qlx.UNION || domain.kind == Qlx.CONTENT)
            {
                var dm = _Type(cx, op, cx._Ob(val) as QlValue, cx._Ob(op1) as QlValue);
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
            if (cx.obs[val] is QlValue v && !v.Grouped(cx, gs)) return false;
            if (cx.obs[op1] is QlValue o1 && !o1.Grouped(cx, gs)) return false;
            if (cx.obs[op2] is QlValue o2 && !o2.Grouped(cx, gs)) return false;
            return true;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is QlValue sv)
                r += sv.Operands(cx);
            if (cx.obs[op1] is QlValue s1)
                r += s1.Operands(cx);
            if (cx.obs[op2] is QlValue s2)
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
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, q, ambient) != false &&
            ((QlValue?)cx.obs[op1])?.KnownBy(cx, q, ambient) != false &&
            ((QlValue?)cx.obs[op2])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs, bool ambient = false)
        {
            if (cs.Contains(defpos))
                return true;
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient) != false &&
            ((QlValue?)cx.obs[op1])?.KnownBy(cx, cs, ambient) != false &&
            ((QlValue?)cx.obs[op2])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain dm)
                return new CTree<long, Domain>(defpos, dm);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[val] is QlValue v)
            {
                r += v.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(val);
            }
            if (cx.obs[op1] is QlValue o1)
            {
                r += o1.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(op1);
            }
            if (cx.obs[op2] is QlValue o2)
            {
                r += o2.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(op2);
            }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal static Domain _Type(Context cx, Qlx op, QlValue? val, QlValue? op1)
        {
            switch (op)
            {
                case Qlx.ABS:
                case Qlx.CEIL:
                case Qlx.CEILING:
                case Qlx.MOD:
                case Qlx.FLOOR:
                    {
                        var d = val?.domain ?? Domain.UnionNumeric;
                        if (d.kind == Qlx.CONTENT || d.kind == Qlx.Null)
                            d = Domain.UnionNumeric;
                        return d;
                    }
                case Qlx.FIRST: 
                case Qlx.NEXT: 
                case Qlx.LAST: 
                case Qlx.MAX: 
                case Qlx.MIN: 
                case Qlx.SUM:
                    {
                        var d = val?.domain ?? Domain.UnionDateNumeric;
                        if (d.kind == Qlx.ARRAY || d.kind == Qlx.LIST || d.kind == Qlx.SET || d.kind == Qlx.MULTISET)
                            d = d.elType??Domain.Content;
                        if (d.kind == Qlx.CONTENT || d.kind == Qlx.Null)
                            d = Domain.UnionNumeric;
                        return d;
                    }
                case Qlx.ANY: return Domain.Bool;
                case Qlx.AVG: return Domain._Numeric;
                case Qlx.ARRAY: return Domain.Collection;
                case Qlx.CARDINALITY: return Domain.Int;
                case Qlx.CAST: return op1?.domain ?? Domain.Content;
                case Qlx.CHAR_LENGTH: return Domain.Int;
                case Qlx.CHARACTER_LENGTH: return Domain.Int;
                case Qlx.CHECK: return Domain._Rvv;
                case Qlx.COLLECT: return Domain.Multiset;
                case Qlx.COUNT: return Domain.Int;
                case Qlx.CURRENT: return Domain.Bool; 
                case Qlx.CURRENT_DATE: return Domain.Date;
                case Qlx.CURRENT_ROLE: return Domain.Char;
                case Qlx.CURRENT_TIME: return Domain.Timespan;
                case Qlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Qlx.DESCRIBE: return Domain.Char;
                case Qlx.ELEMENT: return val?.domain.elType ?? Domain.Content;
                case Qlx.EXP: return Domain.Real;
                case Qlx.EVERY: return Domain.Bool;
                case Qlx.EXTRACT: return Domain.Int;
                case Qlx.FUSION: return Domain.Collection;
                case Qlx.INTERSECTION: return Domain.Collection;
                case Qlx.SECURITY: return Domain._Level;
                case Qlx.LN: return Domain.Real;
                case Qlx.LOCALTIME: return Domain.Timespan;
                case Qlx.LOCALTIMESTAMP: return Domain.Timestamp;
                case Qlx.LOWER: return Domain.Char;
                case Qlx.NORMALIZE: return Domain.Char;
                case Qlx.NULLIF: return op1?.domain ?? Domain.Content;
                case Qlx.OCTET_LENGTH: return Domain.Int;
                case Qlx.OVERLAY: return Domain.Char;
                case Qlx.PARTITION: return Domain.Char;
                case Qlx.POSITION: return Domain.Int;
                case Qlx.POWER: return Domain.Real;
                case Qlx.RANK: return Domain.Int;
                case Qlx.RESTRICT: return val?.domain ?? Domain.Content;
                case Qlx.ROW_NUMBER: return Domain.Int;
                case Qlx.SET: return Domain.Collection;
                case Qlx.SPECIFICTYPE: return Domain.Char;
                case Qlx.SQRT: return Domain.Real;
                case Qlx.STDDEV_POP: return Domain.Real;
                case Qlx.STDDEV_SAMP: return Domain.Real;
                case Qlx.SUBSTRING: return Domain.Char;
                case Qlx.TRANSLATE: return Domain.Char;
                case Qlx.TYPE_URI: return Domain.Char;
                case Qlx.TRIM: return Domain.Char;
                case Qlx.UNNEST: return Domain.TableType;
                case Qlx.UPPER: return Domain.Char;
                case Qlx.USER: return Domain.Char;
                case Qlx.VERSIONING:
                    cx.versioned = true;
                    return (op1 == null) ? Domain.Int : Domain._Rvv;
                case Qlx.WHEN: return val?.domain ?? Domain.Content;
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
                    if (b.value() is long p && cx.obs[p] is QlValue sv)
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
                            case Qlx.NO:
                            case Qlx.Null:
                                AddIn(b, cx);
                                break;
                            case Qlx.CURRENT:
                                if (b._pos != fc.wrb?._pos)
                                    AddIn(b, cx);
                                break;
                            case Qlx.TIES:
                                if (fc.wrb?.CompareTo(b) != 0)
                                    AddIn(b, cx);
                                break;
                        }

                }
                cx.values = oc;
                switch (op)
                {
                    case Qlx.RANK:
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
                    case Qlx.ROW_NUMBER:
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
                var og = cx.obs[from] as RowSet ?? cx.result as RowSet?? throw new PEException("PE29005");
                var gc = og.groupCols;
                var key = (gc == null || gc == Domain.Null) ? TRow.Empty : new TRow(gc, cx.values);
                fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            }
            TypedValue dv = domain.defaultValue ?? TNull.Value;
            TypedValue v;
            switch (op)
            {
                case Qlx.ABS:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1902");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vl.domain.kind)
                        {
                            case Qlx.INTEGER:
                                {
                                    if (v is TInt w)
                                        return new TInt((w.value < 0L) ? -w.value : w.value);
                                    break;
                                }
                            case Qlx.REAL:
                                {
                                    var w = v.ToDouble();
                                    if (w is double.NaN)
                                        break;
                                    return new TReal((w < 0.0) ? -w : w);
                                }
                            case Qlx.NUMERIC:
                                {
                                    if (v is not TNumeric w)
                                        break;
                                    return new TNumeric((w.value < Numeric.Zero) ? w.value.Negate() : w.value);
                                }
                            case Qlx.UNION:
                                {
                                    var cs = vl.domain.unionOf;
                                    if (cs.Contains(Domain.Int))
                                        goto case Qlx.INTEGER;
                                    if (cs.Contains(Domain._Numeric))
                                        goto case Qlx.NUMERIC;
                                    if (cs.Contains(Domain.Real))
                                        goto case Qlx.REAL;
                                    break;
                                }
                        }
                        break;
                    }
                case Qlx.ANY:
                    if (fc == null)
                        break;
                    return TBool.For(fc.bval);
                case Qlx.ARRAY: // Mongo $push
                    {
                        if (fc == null)
                            break;
                        if (ws == null || fc.mset == null || fc.mset.Count == 0)
                            return fc.acc ?? dv;
                        if (fc.mset.tree?.First()?.key().dataType is not Domain de)
                            throw new PEException("PE48183");
                        var ar = new TList((Domain)cx.Add(new Domain(cx.GetUid(), Qlx.ARRAY, de)));
                        for (var d = fc.mset.tree.First(); d != null; d = d.Next())
                            ar += d.key();
                        fc.acc = ar;
                        return fc.acc;
                    }
                case Qlx.AVG:
                    {
                        if (cx.obs[val] is QlValue vl
                            && (vl.domain.kind == Qlx.ARRAY || vl.domain.kind == Qlx.SET || vl.domain.kind == Qlx.MULTISET)
                            && vl._Eval(cx) is TypedValue tv)
                        {
                            var sum = (vl.domain.elType ?? Domain.Int).defaultValue;
                            var count = 0;
                            for (var b = vl._Eval(cx).First(); b != null; b = b.Next())
                            {
                                count++;
                                sum += b.Value();
                            }
                            return sum /= count;
                        }
                        switch (fc.sumType.kind)
                        {
                            case Qlx.NUMERIC:
                                if (fc.sumDecimal is null)
                                    throw new PEException("PE48184");
                                return new TReal(fc.sumDecimal / new Numeric(fc.count));
                            case Qlx.REAL: return new TReal(fc.sum1 / fc.count);
                            case Qlx.INTEGER:
                                if (fc.sumInteger is not null)
                                    return new TReal(new Numeric(fc.sumInteger, 0) / new Numeric(fc.count));
                                return new TReal(new Numeric(fc.sumLong) / new Numeric(fc.count));
                        }
                        return dv;
                    }
                case Qlx.CARDINALITY:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1960");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return vl.domain.kind switch
                        {
                            Qlx.MULTISET => new TInt(((TMultiset)v).Count),
                            Qlx.SET => new TInt(((TSet)v).tree.Count),
                            Qlx.PATH or Qlx.ARRAY => new TInt(((TArray)v).Length),
                            _ => throw new DBException("42113", v).Mix(),
                        };
                    }
                case Qlx.CAST:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1961");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (cx.obs[op1] is QlValue ce && ce.domain is Domain cd)
                            return cd.Coerce(cx, v);
                        break;
                    }
                case Qlx.CEIL: goto case Qlx.CEILING;
                case Qlx.CEILING:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1962");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vl.domain.kind)
                        {
                            case Qlx.INTEGER:
                                return v;
                            case Qlx.DOUBLE:
                                return new TReal(Math.Ceiling(v.ToDouble()));
                            case Qlx.NUMERIC:
                                return new TNumeric(Numeric.Ceiling(((TNumeric)v).value));
                        }
                        break;
                    }
                case Qlx.CHAR_LENGTH:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1964");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Qlx.CHARACTER_LENGTH: goto case Qlx.CHAR_LENGTH;
                case Qlx.CHECK:
                    {
                        var rv = Rvv.Empty;
                        if (cx.obs[from] is not RowSet rs)
                            break;
                        for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                            if (b.value() is long p && cx.cursors[p] is Cursor c)
                                rv += (b.key(), c._ds[b.key()]);
                        return new TRvv(rv);
                    }
                case Qlx.COLLECT:
                    if (fc == null || fc.mset == null)
                        break;
                    return domain.Coerce(cx, fc.mset);
                //		case Qlx.CONVERT: transcoding all seems to be implementation-defined TBD
                case Qlx.COUNT:
                    {
                        if (cx.obs[val] is QlValue vl
                        && (vl.domain.kind == Qlx.ARRAY || vl.domain.kind == Qlx.SET || vl.domain.kind == Qlx.MULTISET)
                        && vl._Eval(cx) is TypedValue tv)
                        {
                            if (tv is TNull) return new TInt(0);
                            if (tv is TArray va) return new TInt(va.Length);
                            if (tv is TMultiset vm) return new TInt(vm.Count);
                            if (tv is TSet vs) return new TInt(vs.Cardinality());
                            if (tv is TList vx) return new TInt(vx.Length);
                            throw new PEException("PE40605");
                        }
                    }
                        return new TInt(fc.count);
                case Qlx.CURRENT:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1965");
                        if (vl?.Eval(cx) is Cursor tc && cx.values[tc._rowsetpos] is Cursor tq)
                            return TBool.For(tc._pos == tq._pos);
                        break;
                    }
                case Qlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Qlx.CURRENT_ROLE:
                    if (cx.db == null || cx.db.role is not Role ro || ro.name == null)
                        break;
                    return new TChar(ro.name);
                case Qlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Qlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Qlx.DESCRIBE:
                    {
                        var nd = (cx.cursors[from] as Cursor)?.node(cx);
                        return (TypedValue?)nd ?? TNull.Value;
                    }
                case Qlx.ELEMENT:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1966");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v is not TMultiset m)
                            throw new DBException("42113", v).Mix();
                        if (m.tree == null || m.Count != 1)
                            throw new DBException("21000").Mix();
                        return m.tree.First()?.key() ?? TNull.Value;
                    }
                case Qlx.ELEMENTID:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        return new TInt(n.tableRow.defpos);
                    }
                case Qlx.EXP:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1967");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Exp(v.ToDouble()));
                    }
                case Qlx.EVERY:
                    {
                        if (fc == null)
                            break;
                        object? o = fc.mset?.tree?[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Qlx.EXTRACT:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1968");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (v.dataType.kind)
                        {
                            case Qlx.DATE:
                                {
                                    DateTime dt = ((TDateTime)v).value;
                                    switch (mod)
                                    {
                                        case Qlx.YEAR: return new TInt(dt.Year);
                                        case Qlx.MONTH: return new TInt(dt.Month);
                                        case Qlx.DAY: return new TInt(dt.Day);
                                        case Qlx.HOUR: return new TInt(dt.Hour);
                                        case Qlx.MINUTE: return new TInt(dt.Minute);
                                        case Qlx.SECOND: return new TInt(dt.Second);
                                    }
                                    break;
                                }
                            case Qlx.INTERVAL:
                                {
                                    Interval it = ((TInterval)v).value;
                                    switch (mod)
                                    {
                                        case Qlx.YEAR: return new TInt(it.years);
                                        case Qlx.MONTH: return new TInt(it.months);
                                        case Qlx.DAY: return new TInt(it.ticks / TimeSpan.TicksPerDay);
                                        case Qlx.HOUR: return new TInt(it.ticks / TimeSpan.TicksPerHour);
                                        case Qlx.MINUTE: return new TInt(it.ticks / TimeSpan.TicksPerMinute);
                                        case Qlx.SECOND: return new TInt(it.ticks / TimeSpan.TicksPerSecond);
                                    }
                                    break;
                                }
                        }
                        throw new DBException("42000", mod).ISO().Add(Qlx.ROUTINE_NAME, new TChar("Extract"));
                    }
                case Qlx.FIRST:
                    return fc?.mset?.tree?.First()?.key() ?? throw new DBException("42135");
                case Qlx.FLOOR:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1969");
                        var vd = vl ?? throw new PEException("PE1970");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        switch (vd.domain.kind)
                        {
                            case Qlx.INTEGER:
                                return v;
                            case Qlx.DOUBLE:
                                return new TReal(Math.Floor(v.ToDouble()));
                            case Qlx.NUMERIC:
                                return new TNumeric(Numeric.Floor(((TNumeric)v).value));
                        }
                        break;
                    }
                case Qlx.FUSION:
                    if (fc == null || fc.mset == null) break;
                    return domain.Coerce(cx, fc.mset);
                case Qlx.ID:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        return (a is TNode n) ? n.id : a ?? TNull.Value;
                    }
                case Qlx.INTERSECTION:
                    if (fc == null || fc.mset == null) break;
                    return domain.Coerce(cx, fc.mset);
                case Qlx.LABELS:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        var s = new TSet(Domain.Char);
                        for (var b = ((NodeType)n.dataType).label.OnInsert(cx,0L).First(); b!=null;b=b.Next())
                            s += new TChar(b.key().NameFor(cx));
                        return s;
                    }
                case Qlx.LAST: return fc?.mset?.tree?.Last()?.key() ?? throw new DBException("42135");
                case Qlx.LAST_DATA:
                    {
                        if (cx.obs[from] is not RowSet rs)
                            break;
                        return new TInt(rs.lastData);
                    }
                case Qlx.LN:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1971");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Log(v.ToDouble()));
                    }
                case Qlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Qlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Qlx.LOWER:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1972");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return dv;
                    }
                case Qlx.MAX:
                    if (fc == null) break;
                    return fc.acc ?? dv;
                case Qlx.MIN:
                    if (fc == null) break;
                    return fc.acc ?? dv;
                case Qlx.MOD:
                    {
                        if (cx.obs[op1] is not QlValue o1) break;
                        v = o1.Eval(cx);
                        if (v == TNull.Value)
                            return dv;
                        switch (o1.domain.kind)
                        {
                            case Qlx.INTEGER:
                                if (v is not TInt iv || cx.obs[op2]?.Eval(cx) is not TInt mv) break;
                                return new TInt(iv.value % mv.value);
                            case Qlx.NUMERIC:
                                if (v is not TNumeric nv || cx.obs[op2]?.Eval(cx) is not TNumeric vm)
                                    break;
                                return new TNumeric(nv.value % vm.value);
                        }
                        break;
                    }
                case Qlx.NORMALIZE:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1974");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        return v; //TBD
                    }
                case Qlx.NULLIF:
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
                case Qlx.OCTET_LENGTH:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1975");
                        v = vl?.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TInt(((TBlob)v).value.Length);
                    }
                case Qlx.OVERLAY:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1976");
                        v = vl.Eval(cx) ?? TNull.Value;
                        return v; //TBD
                    }
                case Qlx.PARTITION:
                    return TNull.Value;
                case Qlx.POSITION:
                    {
                        if (cx.values[val] is TNode n)
                            return new TInt(n.tableRow.defpos);
                        if (op1 != -1L && op2 != -1L)
                        {
                            string t = cx.obs[op1]?.Eval(cx)?.ToString() ?? "";
                            string s = cx.obs[op2]?.Eval(cx)?.ToString() ?? "";
                            return new TInt(s.IndexOf(t));
                        }
                        var rv = Rvv.Empty;
                        if (cx.obs[from] is not RowSet rs)
                            break;
                        for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                            if (b.value() is long p && cx.cursors[p] is Cursor c)
                                rv += (b.key(), c._ds[b.key()]);
                        return new TInt(rv.version);
                    }
                case Qlx.POWER:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1977");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        var w = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (w == TNull.Value)
                            return dv;
                        return new TReal(Math.Pow(v.ToDouble(), w.ToDouble()));
                    }
                case Qlx.RANK: goto case Qlx.ROW_NUMBER;
                case Qlx.RESTRICT:
                    if (fc == null)
                        break;
                    if (fc.acc is null && cx.conn._tcp is not null)
                        throw new DBException("42170");
                    return fc.acc ?? TNull.Value;
                case Qlx.ROW_NUMBER:
                    if (fc == null)
                        break;
                    return new TInt(fc.row);
                case Qlx.SECURITY:
                    return cx.cursors[from]?[Classification] ?? TLevel.D;
                case Qlx.SET:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1978");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        if (v is not TMultiset)
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set();
                    }
                case Qlx.SOME: goto case Qlx.ANY;
                case Qlx.SPECIFICTYPE:
                    {
                        var rs = cx._Ob(from) as RowSet;
                        var tr = (cx.cursors[rs?.from ?? -1L] as Cursor)?.Rec()?[0];
                        var sb = new StringBuilder();
                        var cm = "";
                        if (tr?.subType >= 0 && cx.NameFor(tr.subType) is string ns)
                            sb.Append(ns);//.Trim(':'));
                        else if (tr is not null && cx.db.objects[tr.tabledefpos] is Table tb)
                        {
                            if (tb.nodeTypes.Count==0 && tb.NameFor(cx) is string nm)
                            {
                                sb.Append(cm); sb.Append(nm);//.Trim(':'));
                            } else
                            for (var b = tb.nodeTypes.First(); b != null; b = b.Next())
                                if (b.key().NameFor(cx) is string nk)
                                {
                                    sb.Append(cm); cm = "&";
                                        sb.Append(nk); //.Trim(':'));
                                }
                        }
                        return new TChar(sb.ToString());
                    }
                case Qlx.SQRT:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1979");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TReal(Math.Sqrt(v.ToDouble()));
                    }
                case Qlx.STDDEV_POP:
                    {
                        if (fc == null || fc.count == 0)
                            throw new DBException("22004").ISO().Add(Qlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        return new TReal(Math.Sqrt(fc.acc1 * fc.count - fc.sum1 * fc.sum1)
                            / fc.count);
                    }
                case Qlx.STDDEV_SAMP:
                    {
                        if (fc == null || fc.count <= 1)
                            throw new DBException("22004").ISO().Add(Qlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        return new TReal(Math.Sqrt((fc.acc1 * fc.count - fc.sum1 * fc.sum1)
                            / (fc.count*(fc.count-1))));
                    }
                case Qlx.SUBSTRING:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1980");
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
                            throw new DBException("22011");
                        if (x is not TInt i2)
                            return new TChar(sv[n1..]);
                        var n2 = (int)i2.value;
                        if (n2 < 0 || n2 + n1 - 1 >= sv.Length)
                            throw new DBException("22011");
                        return new TChar(sv.Substring(n1, n2));
                    }
                case Qlx.SUM:
                    {
                        if (cx.obs[val] is QlValue vl
                            && (vl.domain.kind == Qlx.ARRAY || vl.domain.kind == Qlx.SET || vl.domain.kind == Qlx.MULTISET)
                            && vl._Eval(cx) is TypedValue tv)
                        {
                            var sum = (vl.domain.elType ?? Domain.Int).defaultValue;
                            for (var b = vl.Eval(cx).First(); b != null; b = b.Next())
                                sum += b.Value();
                            return sum;
                        }
                        switch (fc.sumType.kind)
                        {
                            case Qlx.Null: return TNull.Value;
                            case Qlx.NULL: return TNull.Value;
                            case Qlx.REAL: return new TReal(fc.sum1);
                            case Qlx.INTEGER:
                                if (fc.sumInteger is not null)
                                    return new TInteger(fc.sumInteger);
                                else
                                    return new TInt(fc.sumLong);
                            case Qlx.NUMERIC:
                                if (fc.sumDecimal is null)
                                    break;
                                return new TNumeric(fc.sumDecimal);
                        }
                        return TNull.Value;
                    }
                case Qlx.TRANSLATE:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1981");
                        v = vl.Eval(cx) ?? TNull.Value;
                        return v; // TBD
                    }
                case Qlx.TRIM:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1982");
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
                                Qlx.LEADING => new TChar(sv.TrimStart((char)c)),
                                Qlx.TRAILING => new TChar(sv.TrimEnd((char)c)),
                                Qlx.BOTH => new TChar(sv.Trim((char)c)),
                                _ => new TChar(sv.Trim((char)c)),
                            };
                        else
                            return mod switch
                            {
                                Qlx.LEADING => new TChar(sv.TrimStart()),
                                Qlx.TRAILING => new TChar(sv.TrimEnd()),
                                Qlx.BOTH => new TChar(sv.Trim()),
                                _ => new TChar(sv.Trim()),
                            };
                    }
                case Qlx.TYPE:
                    {
                        TypedValue? a = cx.obs[val]?.Eval(cx);
                        if (a is not TNode n)
                            return TNull.Value;
                        return new TTypeSpec(n.dataType);
                    }
                case Qlx.UNNEST:
                    {
                        var ta = cx.obs[val]?.Eval(cx) as TArray;
                        var r = new TSet(ta?.dataType.elType ?? Domain.Content);
                        for (var i = 0; i < (ta?.Length??0); i++)
                            if (ta?[i] is TypedValue x)
                                r.Add(x);
                        return r;
                    }
                case Qlx.UPPER:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1983");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TChar(v.ToString().ToUpper());
                    }
                case Qlx.USER: return new TChar(cx.db.user?.name ?? "");
                case Qlx.VERSIONING: // row version pseudocolumn
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1984");
                        var vcx = new Context(cx);
                        if (vl is not null)
                        {
                            vcx.result = cx.obs[vl.defpos] as RowSet;
                            return new TRvv("");
                        }
                        vcx.result = cx.obs[from] as RowSet;
                        var p = -1L;
                        for (var b = (cx.cursors[from] as Cursor)?.Rec()?.First(); 
                            b is not null; b = b.Next())
                        {
                            var t = b.value();
                            if (t.ppos > p)
                                p = t.ppos;
                        }
                        if (p != -1L)
                            return new TInt(p);
                        return TNull.Value;
                    }
                case Qlx.WHEN: // searched case
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1985");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        TypedValue a = cx.obs[op1]?.Eval(cx) ?? TNull.Value;
                        if (a == TBool.True)
                            return v;
                        return cx.obs[op2]?.Eval(cx) ?? TNull.Value;
                    }
                case Qlx.MONTH:
                case Qlx.DAY:
                case Qlx.HOUR:
                case Qlx.MINUTE:
                case Qlx.SECOND:
                case Qlx.YEAR:
                    {
                        var vl = (QlValue?)cx.obs[val] ?? throw new PEException("PE1988");
                        v = vl.Eval(cx) ?? TNull.Value;
                        if (v == TNull.Value)
                            return dv;
                        return new TInt(Extract(op, v));
                    }
            }
            throw new DBException("42154", op).Mix();
        }
        static long Extract(Qlx mod, TypedValue v)
        {
            switch (v.dataType.kind)
            {
                case Qlx.DATE:
                    {
                        DateTime dt = ((TDateTime)v).value;
                        switch (mod)
                        {
                            case Qlx.YEAR: return dt.Year;
                            case Qlx.MONTH: return dt.Month;
                            case Qlx.DAY: return dt.Day;
                            case Qlx.HOUR: return dt.Hour;
                            case Qlx.MINUTE: return dt.Minute;
                            case Qlx.SECOND: return dt.Second;
                        }
                        break;
                    }
                case Qlx.INTERVAL:
                    {
                        Interval it = ((TInterval)v).value;
                        switch (mod)
                        {
                            case Qlx.YEAR: return it.years;
                            case Qlx.MONTH: return it.months;
                            case Qlx.DAY: return it.ticks / TimeSpan.TicksPerDay;
                            case Qlx.HOUR: return it.ticks / TimeSpan.TicksPerHour;
                            case Qlx.MINUTE: return it.ticks / TimeSpan.TicksPerMinute;
                            case Qlx.SECOND: return it.ticks / TimeSpan.TicksPerSecond;
                        }
                        break;
                    }
            }
            throw new DBException("42000", mod).ISO().Add(Qlx.ROUTINE_NAME, new TChar("Extract"));
        }
        /// <summary>
        /// for aggregates and window functions we need to implement StartCounter
        /// </summary>
        internal Register StartCounter(Context cx, TRow key)
        {
            var oc = cx.values;
            var fc = new Register(cx, key, this) { acc1 = 0.0, mset = null };
            switch (op)
            {
                case Qlx.ROW_NUMBER: break;
                case Qlx.AVG:
                    fc.count = 0L;
                    fc.sumType = Domain.Content;
                    break;
                case Qlx.COLLECT:
                case Qlx.EVERY:
                case Qlx.FUSION:
                case Qlx.INTERSECTION:
                    {
                        if (cx.obs[val] is not QlValue vl)
                            throw new PEException("PE48185");
                        fc.mset = new TMultiset(vl.domain);
                        break;
                    }
                case Qlx.SOME:
                case Qlx.ANY:
                    if (window >= 0)
                        goto case Qlx.COLLECT;
                    fc.bval = false;
                    break;
                case Qlx.ARRAY:
                    fc.acc = new TList((Domain)cx.Add(
                        new Domain(cx.GetUid(), Qlx.ARRAY, Domain.Content)));
                    break;
                case Qlx.COUNT:
                    fc.count = 0L;
                    break;
                case Qlx.FIRST:
                    fc.acc = null; // NOT TNull.Value !
                    break;
                case Qlx.LAST:
                    fc.acc = TNull.Value;
                    break;
                case Qlx.MAX:
                case Qlx.MIN:
                case Qlx.RESTRICT:
                    if (window >= 0L)
                        goto case Qlx.COLLECT;
                    fc.sumType = Domain.Content;
                    fc.acc = null;
                    break;
                case Qlx.STDDEV_POP:
                case Qlx.STDDEV_SAMP:
                    fc.acc1 = 0.0;
                    fc.sum1 = 0.0;
                    fc.count = 0L;
                    break;
                case Qlx.SUM:
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
            var fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            var vl = cx.obs[val] as QlValue;// not all window functions use val
            var v = vl?.Eval(cx)??TNull.Value;
            if (v.Cardinality() > 1)
            {
                for (var b = v.First(); b != null; b = b.Next())
                    if (b.Value() != TNull.Value)
                        fc.AddIn(cx,this, vl, b.Value());
            }
            else
                fc.AddIn(cx, this, vl, v);
            if (window < 0)
            {
                var t1 = cx.funcs[from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                if (fc is not null)
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
            if (ws.units == Qlx.RANGE && !(TestStartRange(cx, bmk, fc) && TestEndRange(cx, bmk, fc)))
                return false;
            if (ws.units == Qlx.ROWS && !(TestStartRows(cx, bmk, fc) && TestEndRows(cx, bmk, fc)))
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
                limit = kt.Eval(defpos, cx, wrv, (kt.AscDesc == Qlx.ASC) ? Qlx.MINUS : Qlx.PLUS,
                    ws.high.distance);
            else
                limit = kt.Eval(defpos, cx, wrv, (kt.AscDesc == Qlx.ASC) ? Qlx.PLUS : Qlx.MINUS,
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
                limit = kt.Eval(defpos, cx, tv, (kt.AscDesc != Qlx.DESC) ? Qlx.PLUS : Qlx.MINUS,
                    ws.low.distance);
            else
                limit = kt.Eval(defpos, cx, tv, (kt.AscDesc != Qlx.DESC) ? Qlx.MINUS : Qlx.PLUS,
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
                qn = ((QlValue?)cx.obs[val])?.Needs(cx, r, qn) ?? qn;
                qn = ((QlValue?)cx.obs[op1])?.Needs(cx, r, qn) ?? qn;
                qn = ((QlValue?)cx.obs[op2])?.Needs(cx, r, qn) ?? qn;
            }
            qn = ((WindowSpecification?)cx.obs[window])?.Needs(cx, r, qn) ?? qn;
            return qn;
        }

        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (aggregates(op) && from != rs)
                return r;
            if (cx.obs[val] is QlValue v)
                r += v.Needs(cx, rs);
            if (cx.obs[op1] is QlValue o1)
                r += o1.Needs(cx, rs);
            if (cx.obs[op2] is QlValue o2)
                r += o2.Needs(cx, rs);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[val] is QlValue v)
                r += v.Needs(cx);
            if (cx.obs[op1] is QlValue o1)
                r += o1.Needs(cx);
            if (cx.obs[op2] is QlValue o2)
                r += o2.Needs(cx);
            return r;
        }
        // tailor REST call to remote DBMS
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            if (op == Qlx.COUNT && mod == Qlx.TIMES)
                return "COUNT(*)";
            switch (sg)
            {
                case "Pyrrho":
                    {
                        var sb = new StringBuilder();
                        string vl, o1, o2;
                        if (!aggregates(op)) // ||((RowSet)cx.obs[from]).Built(cx))
                        {
                            vl = (cx.obs[val] is QlValue sv) ? sv.ToString(sg, rf, cs, ns, cx) : "";
                            o1 = (cx.obs[op1] is QlValue s1) ? s1.ToString(sg, rf, cs, ns, cx) : "";
                            o2 = (cx.obs[op2] is QlValue s2) ? s2.ToString(sg, rf, cs, ns, cx) : "";
                        }
                        else
                        {
                            vl = ((cx.obs[val] is QlInstance vc) ? ns[vc.sPos] : ns[val]) ?? "";
                            o1 = ((cx.obs[op1] is QlInstance c1) ? ns[c1.sPos] : ns[op1]) ?? "";
                            o2 = ((cx.obs[op2] is QlInstance c2) ? ns[c2.sPos] : ns[op2]) ?? "";
                        }
                        switch (op)
                        {
                            case Qlx.COUNT:
                                if (mod != Qlx.TIMES)
                                    goto case Qlx.ABS;
                                sb.Append("COUNT(*)");
                                break;
                            case Qlx.ABS:
                            case Qlx.ANY:
                            case Qlx.AVG:
                            case Qlx.CARDINALITY:
                            case Qlx.CEIL:
                            case Qlx.CEILING:
                            case Qlx.CHAR_LENGTH:
                            case Qlx.CHARACTER_LENGTH:
                            case Qlx.EVERY:
                            case Qlx.EXP:
                            case Qlx.FLOOR:
                            case Qlx.LN:
                            case Qlx.LOWER:
                            case Qlx.MAX:
                            case Qlx.MIN:
                            case Qlx.NORMALIZE:
                            case Qlx.NULLIF:
                            case Qlx.OCTET_LENGTH:
                            case Qlx.SET:
                            case Qlx.SUM:
                            case Qlx.TRANSLATE:
                            case Qlx.UPPER:
                                sb.Append(op); sb.Append('(');
                                if (mod == Qlx.DISTINCT)
                                    sb.Append("DISTINCT ");
                                sb.Append(vl); sb.Append(')'); break;
                            case Qlx.ARRAY:
                            case Qlx.CURRENT:
                            case Qlx.ELEMENT:
                            case Qlx.FIRST:
                            case Qlx.LAST:
                            case Qlx.SECURITY:
                            case Qlx.NEXT:
                            case Qlx.OVERLAY:
                            case Qlx.PARTITION:
                            case Qlx.RANK:
                            case Qlx.ROW_NUMBER:
                            case Qlx.STDDEV_POP:
                            case Qlx.STDDEV_SAMP:
                            case Qlx.TYPE_URI:
                            case Qlx.TRIM:
                            case Qlx.WHEN:
                                throw new DBException("42000", ToString());
                            case Qlx.CAST:
                                sb.Append(op); sb.Append('(');
                                sb.Append(vl); sb.Append(" as ");
                                sb.Append(name); sb.Append(')');
                                break;
                            case Qlx.COLLECT:
                            case Qlx.FUSION:
                            case Qlx.INTERSECTION:
                                sb.Append(op); sb.Append('(');
                                if (mod != Qlx.NO)
                                {
                                    sb.Append(mod); sb.Append(' ');
                                }
                                sb.Append(vl); sb.Append(')');
                                break;
                            case Qlx.CHECK:
                            case Qlx.CURRENT_DATE:
                            case Qlx.CURRENT_TIME:
                            case Qlx.CURRENT_TIMESTAMP:
                            case Qlx.USER:
                            case Qlx.LOCALTIME:
                            case Qlx.LOCALTIMESTAMP:
                            case Qlx.VERSIONING:
                                sb.Append(op); break;
                            case Qlx.EXTRACT:
                                sb.Append(op); sb.Append('(');
                                sb.Append(mod); sb.Append(" from ");
                                sb.Append(vl); sb.Append(')');
                                break;
                            case Qlx.MOD:
                            case Qlx.POWER:
                                sb.Append(op); sb.Append('(');
                                sb.Append(o1); sb.Append(',');
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Qlx.POSITION:
                                sb.Append(op); sb.Append('(');
                                sb.Append(o1); sb.Append(" in ");
                                sb.Append(o2); sb.Append(')');
                                break;
                            case Qlx.SUBSTRING:
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
                case Qlx.PARTITION:
                case Qlx.VERSIONING:
                case Qlx.CHECK: return op.ToString();
                case Qlx.POSITION:
                    if (op1 != -1L) goto case Qlx.PARTITION;
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
            if (mod != Qlx.NO && mod != Qlx.TIMES)
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
        internal static bool aggregates(Qlx op)
        {
            return op switch
            {
                Qlx.ANY or Qlx.ARRAY or Qlx.AVG or Qlx.COLLECT or Qlx.COUNT 
                or Qlx.EVERY or Qlx.FIRST or Qlx.FUSION or Qlx.INTERSECTION 
                or Qlx.LAST or Qlx.MAX or Qlx.MIN or Qlx.RESTRICT or Qlx.STDDEV_POP 
                or Qlx.STDDEV_SAMP or Qlx.SOME or Qlx.SUM => true,
                _ => false,
            };
        }
        internal static bool aggregatable(Qlx op)
        {
            return op switch
            {
                Qlx.ANY or Qlx.ARRAY or Qlx.COLLECT or Qlx.LIST or Qlx.CONTENT
                or Qlx.DOCARRAY or Qlx.DOCUMENT
                or Qlx.SET or Qlx.MULTISET or Qlx.ROW or Qlx.TABLE => false,
                _ => true,
            };
        }
    }

    /// <summary>
    /// The Parser converts this n-ary function to a binary one
    /// 
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long ap, long dp, Context cx, QlValue op1, QlValue op2)
            : base(ap, dp, cx, Qlx.COALESCE, null, op1, op2, Qlx.NO)
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
            return (cx.obs[op1] is QlValue o1 && o1.Eval(cx) is TypedValue v1 && v1!=TNull.Value) ? 
                v1 : (cx.obs[op2] is QlValue o2)? o2.Eval(cx) : TNull.Value;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            if (c.obs[op1] is not QlValue le || c.obs[op2] is not QlValue rg)
                throw new PEException("PE48188");
            var nl = le.Having(c, dm, ap);
            var nr = rg.Having(c, dm, ap);
            return (le == nl && rg == nr) ? this :
                (QlValue)c.Add(new SqlCoalesce(ap, c.GetUid(), c, nl, nr));
        }
        internal override bool Match(Context c, QlValue v)
        {
            if (v is not SqlCoalesce sc)
                return false;
            if (c.obs[op1] is not QlValue le || c.obs[op2] is not QlValue rg ||
                c.obs[sc.op1] is not QlValue vl || c.obs[sc.op2] is not QlValue vr)
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
        internal SqlTypeUri(long ap, long dp, Context cx, QlValue op1)
            : base(ap, dp, cx, Qlx.TYPE_URI, null, op1, null, Qlx.NO)
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    internal class SqlStar : QlValue
    {
        public readonly long prefix = -1L; // QlValue
        internal SqlStar(long dp, Context cx, long pf) 
            : base(new Ident("*",dp),BList<Ident>.Empty, cx, Domain.Content)
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50713");
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    /// Quantified Predicate subclass of QlValue
    /// 
    /// </summary>
    internal class QuantifiedPredicate : QlValue
    {
        internal const long // these constants are used in other classes too
            All = -348, // bool
            Between = -349, // bool
            Found = -350, // bool
            High = -351, // long QlValue
            Low = -352, // long QlValue
            Op = -353, // Qlx
            _Select = -354, //long RowSet
            Vals = -355; //CList<long> QlValue
        public long what => (long)(mem[WhileStatement.What]??-1L);
        /// <summary>
        /// The comparison operator: LSS etc
        /// </summary>
        public Qlx op => (Qlx)(mem[Op]??Qlx.NO);
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
        internal QuantifiedPredicate(long dp,Context cx,QlValue w, Qlx o, bool a, 
            RowSet s)
            : base(dp,_Mem(cx,w,s) + (WhileStatement.What, w.defpos)+(Op,o)+(All,a)+(_Select,s.defpos)) 
        {
            cx.Add(this);
        }
        protected QuantifiedPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,QlValue w,RowSet s)
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
                r = cx.Add(r, WhileStatement.What, nw);
            var ns = cx.Fix(select);
            if (ns != select)
                r = cx.Add(r, _Select, ns);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (QuantifiedPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what,so,sv);
            if (wh != what)
                r +=(cx, WhileStatement.What, wh);
            var se = cx.ObReplace(select, so, sv);
            if (se != select)
                r +=(cx, _Select, se);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((QlValue?)cx.obs[what])?.Grouped(cx, gs) != false;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (QuantifiedPredicate)base.AddFrom(cx, q);
            var a = ((QlValue?)cx.obs[r.what])?.AddFrom(cx, q)??SqlNull.Value;
            if (a.defpos != r.what)
                r += (cx, WhileStatement.What, a.defpos);
            return (QlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert this search condition e.g. NOT (a LSS SOME b) is (a GEQ ALL b)
        /// </summary>
        /// <param name="j">the part part</param>
        /// <returns>the new search condition</returns>
        internal override QlValue Invert(Context cx)
        {
            var w = (QlValue?)cx.obs[what]??SqlNull.Value;
            var s = (RowSet?)cx.obs[select]??throw new PEException("PE1904");
            return op switch
            {
                Qlx.EQL => new QuantifiedPredicate(defpos, cx, w, Qlx.NEQ, !all, s),
                Qlx.NEQ => new QuantifiedPredicate(defpos, cx, w, Qlx.EQL, !all, s),
                Qlx.LEQ => new QuantifiedPredicate(defpos, cx, w, Qlx.GTR, !all, s),
                Qlx.LSS => new QuantifiedPredicate(defpos, cx, w, Qlx.GEQ, !all, s),
                Qlx.GEQ => new QuantifiedPredicate(defpos, cx, w, Qlx.LSS, !all, s),
                Qlx.GTR => new QuantifiedPredicate(defpos, cx, w, Qlx.LEQ, !all, s),
                _ => throw new PEException("PE65"),
            };
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is QlValue w)
                r += w.Needs(cx, rs);
            if (cx.obs[select] is QlValue s)
                r += s.Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[what] is not QlValue wv || cx.obs[select] is not RowSet s)
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
            if (cx.obs[what] is QlValue w)
                tg = w.StartCounter(cx, rs, tg);
            if (cx.obs[select] is RowSet s)
                tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[what] is QlValue w)
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
            if (cx.obs[what] is QlValue w)
                qn = w.Needs(cx, r, qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return ((QlValue?)cx.obs[what])?.KnownBy(cx, cs)==true;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((QlValue?)cx.obs[what])?.KnownBy(cx, q)==true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[what] is QlValue w)
            {
                r += w.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(what);
            }
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50714");
            var r = this;
            BList<DBObject> ls;
            var rs = (RowSet?)cx.obs[select] ?? throw new PEException("PE1900");
            if (cx.obs[what] is QlValue w && what>ap)
            {
                (ls, m) = w.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=w.defpos)
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
            if (op != Qlx.NO)
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
    /// BetweenPredicate subclass of QlValue
    /// 
    /// </summary>
    internal class BetweenPredicate : QlValue
    {
        public long what =>(long)(mem[WhileStatement.What]??-1L);
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
        internal BetweenPredicate(long dp, Context cx, QlValue w, bool b, QlValue a, QlValue h)
            : base(dp, _Mem(cx, w, a, h)
                  + (WhileStatement.What, w.defpos) + (QuantifiedPredicate.Between, b)
                  + (QuantifiedPredicate.Low, a.defpos) + (QuantifiedPredicate.High, h.defpos))
        {
            cx.Add(this);
        }
        protected BetweenPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(Context cx,QlValue w,QlValue a,QlValue b)
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
            if (cx.obs[what] is QlValue wh)
                r += wh.IsAggregation(cx,ags);
            if (cx.obs[low] is QlValue lw)
                r += lw.IsAggregation(cx,ags); 
            if (cx.obs[high] is QlValue hi)
                r += hi.IsAggregation(cx,ags);
            return r;
        }
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
                cx.obs[high] is not QlValue hi)
                throw new PEException("PE43336");
            var nw = wh.Having(cx,dm,ap);
            var nl = lo.Having(cx,dm,ap);
            var nh = hi.Having(cx,dm,ap);
            return (wh == nw && lo == nl && hi == nh) ? this :
                (QlValue)cx.Add(new BetweenPredicate(cx.GetUid(), cx, nw, between, nl, nh));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (v is BetweenPredicate that)
            {
                if (between != that.between)
                    return false;
                if (cx.obs[what] is QlValue w)
                    if (cx.obs[that.what] is QlValue tw && !w.Match(cx,tw))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[low] is QlValue lw)
                    if (cx.obs[that.low] is QlValue tl && !lw.Match(cx, tl))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[high] is QlValue hg)
                    if (cx.obs[that.high] is QlValue th && !hg.Match(cx,th))
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
            if (cx.obs[what] is QlValue w) r += w._Rdc(cx);
            if (cx.obs[low] is QlValue lo) r += lo._Rdc(cx);
            if (cx.obs[high] is QlValue hi) r += hi._Rdc(cx);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((QlValue?)cx.obs[what])?.Grouped(cx, gs) != false &&
            ((QlValue?)cx.obs[low])?.Grouped(cx, gs) != false &&
            ((QlValue?)cx.obs[high])?.Grouped(cx, gs) != false;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nw = cx.Fix(what);
            if (what!=nw)
                r += (WhileStatement.What, cx.Fix(what));
            var nl = cx.Fix(low);
            if (low!=nl)
                r += (QuantifiedPredicate.Low, nl);
            var nh = cx.Fix(high);
            if (high !=nh)
                r += (QuantifiedPredicate.High, nh);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (BetweenPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r +=(cx, WhileStatement.What, wh);
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
            return (((QlValue?)cx.obs[low])?.KnownBy(cx, q, ambient)??true)
                && (((QlValue?)cx.obs[high])?.KnownBy(cx, q, ambient)??true)
                && (((QlValue?)cx.obs[what])?.KnownBy(cx, q, ambient)??true);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return (((QlValue?)cx.obs[low])?.KnownBy(cx, cs, ambient) ?? true)
                && (((QlValue?)cx.obs[high])?.KnownBy(cx, cs, ambient) ?? true)
                && (((QlValue?)cx.obs[what])?.KnownBy(cx, cs, ambient)??true);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[low] is QlValue lo)
            {
                r += lo.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(low);
            }
            if (cx.obs[high] is QlValue hi)
            {
                r += hi.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(high);
            } 
            if (cx.obs[what] is QlValue w)
            {
                r += w.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(what);
            } 
            if (y)
                return new CTree<long, Domain>(defpos, domain);
            return r;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from < 0)
                return this;
            var r = (BetweenPredicate)base.AddFrom(cx, q);
            if (cx.obs[r.what] is QlValue wo)
            {
                var a = wo.AddFrom(cx, q);
                if (a.defpos != r.what)
                    r += (cx, WhileStatement.What, a.defpos);
            }
            if (cx.obs[r.low] is QlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.low)
                    r += (cx,QuantifiedPredicate.Low, a.defpos);
            }
            if (cx.obs[r.high] is QlValue ho)
            {
                var a = ho.AddFrom(cx, q);
                if (a.defpos != r.high)
                    r += (cx,QuantifiedPredicate.High, a.defpos);
            }
            return (QlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the between predicate (for part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override QlValue Invert(Context cx)
        {
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
                    cx.obs[high] is not QlValue hi)
                throw new PEException("PE43337");
            return new BetweenPredicate(defpos, cx, wh, !between,lo, hi);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
        cx.obs[high] is not QlValue hi)
                throw new PEException("PE43338");
            tg = wh.StartCounter(cx,rs,tg);
            tg = lo.StartCounter(cx,rs,tg);
            tg = hi.StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
cx.obs[high] is not QlValue hi)
                throw new PEException("PE43339");
            tg = wh.AddIn(cx,rb, tg);
            tg = lo.AddIn(cx,rb, tg);
            tg = hi.AddIn(cx,rb, tg);
            return tg;
        }
        internal override void OnRow(Context cx,Cursor bmk)
        {
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
cx.obs[high] is not QlValue hi)
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
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
cx.obs[high] is not QlValue hi)
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
            if (cx.obs[what] is not QlValue wh || cx.obs[low] is not QlValue lo ||
                    cx.obs[high] is not QlValue hi)
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
            if (cx.obs[what] is QlValue w)
                qn = w.Needs(cx, r, qn);
            if (cx.obs[low] is QlValue lo)
                qn = lo.Needs(cx, r, qn);
            if (cx.obs[high] is QlValue hi)
                qn = hi.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50715");
            var r = this;
            var ch = false;
            QlValue w = (QlValue?)cx.obs[what] ?? SqlNull.Value,
                l = (QlValue?)cx.obs[low] ?? SqlNull.Value,
                h = (QlValue?)cx.obs[high] ?? SqlNull.Value;
            BList<DBObject> ls;
            if (w != SqlNull.Value && what>ap)
            {
                (ls, m) = w.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=w.defpos)
                {
                    ch = true; w = nv;
                }
            }
            if (l != SqlNull.Value && low>ap)
            {
                (ls, m) = l.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != l.defpos)
                {
                    ch = true; l = nv;
                }
            }
            if (h != SqlNull.Value && high>ap)
            {
                (ls, m) = h.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != h.defpos)
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
    /// LikePredicate subclass of QlValue
    /// 
    /// </summary>
    internal class LikePredicate : QlValue
    {
        internal const long
            Escape = -358, // long QlValue
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
        internal LikePredicate(long dp,Context cx,QlValue a, bool k, QlValue b, QlValue? e)
            : base(dp, _Mem(cx,a,b,e) + (Left,a.defpos)+(_Like,k)+(Right,b.defpos)
                  +(_Domain,Domain.Bool)+(_Depth,_Depths(a,b,e)))
        {
            cx.Add(this);
        }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, QlValue a, QlValue b,QlValue? e)
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
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            var le = (QlValue)(cx.obs[left] ?? throw new PEException("PE43320"));
            var nl = le.Having(cx, dm, ap);
            var rg = (QlValue)(cx.obs[right] ?? throw new PEException("PE43321"));
            var nr = rg.Having(cx, dm, ap);
            var es = (QlValue?)cx.obs[escape];
            var ne = es?.Having(cx, dm, ap);
            if (le == nl && rg == nr && es == ne)
                return this;
            return (QlValue)cx.Add(new LikePredicate(cx.GetUid(), cx, nl, like, nr, ne));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (v is LikePredicate that)
            {
                if (like != that.like)
                    return false;
                if (cx.obs[left] is QlValue le && cx.obs[that.left] is QlValue tl)
                    if (!le.Match(cx, tl))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is QlValue rg && cx.obs[that.right] is QlValue tr)
                    if (!rg.Match(cx, tr))
                        return false;
                    else if (that.right >= 0)
                        return false;
                if (cx.obs[escape] is QlValue es && cx.obs[that.escape] is QlValue te)
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
            if (cx.obs[escape] is QlValue es) r += es._Rdc(cx);
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            return ((QlValue?)cx.obs[escape])?.Grouped(cx, gs) != false &&
            ((QlValue?)cx.obs[left])?.Grouped(cx, gs) != false &&
            ((QlValue?)cx.obs[right])?.Grouped(cx, gs)!=false;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from>0)
                return this;
            var r = (LikePredicate)base.AddFrom(cx, q);
            if (cx.obs[r.escape] is QlValue e)
            {
                var a = e.AddFrom(cx, q);
                if (a.defpos != r.escape)
                    r += (cx, Escape, a.defpos);
            }
            if (cx.obs[r.left] is QlValue lo)
            {
                var a = lo.AddFrom(cx, q);
                if (a.defpos != r.left)
                    r += (cx, Left, a.defpos);
            }
            if (cx.obs[r.right] is QlValue ro)
            {
                var a = ro.AddFrom(cx, q);
                if (a.defpos != r.right)
                    r += (cx, Right, a.defpos);
            }
            return (QlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((QlValue?)cx.obs[left])?.KnownBy(cx, q, ambient) != false
                && ((QlValue?)cx.obs[right])?.KnownBy(cx, q, ambient) !=false
                && ((QlValue?)cx.obs[escape])?.KnownBy(cx, q, ambient) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            return ((QlValue?)cx.obs[left])?.KnownBy(cx, cs, ambient) != false
                && ((QlValue?)cx.obs[right])?.KnownBy(cx, cs, ambient) != false
                && ((QlValue?)cx.obs[escape])?.KnownBy(cx, cs, ambient) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain kd)
                return new CTree<long, Domain>(defpos, kd);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (cx.obs[left] is QlValue le)
            {
                r += le.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(left);
            }
            if (cx.obs[right] is QlValue ri)
            {
                r += ri.KnownFragments(cx, kb, ambient);
                y = y && r.Contains(right);
            }
            if (cx.obs[escape] is QlValue es)
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
        internal override QlValue Invert(Context cx)
        {
            if (cx.obs[left] is not QlValue a || cx.obs[right] is not QlValue b)
                throw new PEException("PE49301");
            return new LikePredicate(defpos,cx,a, !like,b, (QlValue?)cx.obs[escape]);
        }

        /// <summary>
        /// Helper for computing LIKE
        /// </summary>
        /// <param name="a">the left operand string</param>
        /// <param name="b">the right operand string</param>
        /// <param name="e">the escape character</param>
        /// <returns>the boolean valueType</returns>
        static bool Like(string a, string b, char e)
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
            if (cx.obs[left] is QlValue le && le.Eval(cx) is TypedValue lf && 
                cx.obs[right] is QlValue re && re.Eval(cx) is TypedValue rg)
            {
                if (lf == TNull.Value && rg == TNull.Value)
                    r = true;
                else if (lf != TNull.Value & rg != TNull.Value)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (cx.obs[escape] is QlValue oe)
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
            qn = ((QlValue?)cx.obs[left])?.Needs(cx,r,qn)??qn;
            qn = ((QlValue?)cx.obs[right])?.Needs(cx,r,qn)??qn;
            qn = ((QlValue?)cx.obs[escape])?.Needs(cx,r,qn) ?? qn;
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
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49500");
            return le.LocallyConstant(cx, rs) && rg.LocallyConstant(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50716");
            var r = this;
            QlValue lf = (QlValue?)cx.obs[left]??SqlNull.Value, 
                rg = (QlValue?)cx.obs[right]??SqlNull.Value, 
                es = (QlValue?)cx.obs[escape]??SqlNull.Value;
            var ch = false;
            BList<DBObject> ls;
            if (lf != SqlNull.Value && left>ap)
            {
                (ls, m) = lf.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value && right>ap)
            {
                (ls, m) = rg.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != rg.defpos)
                {
                    ch = true; rg = nv;
                }
            }
            if (es != SqlNull.Value && escape>ap)
            {
                (ls, m) = es.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != es.defpos)
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
    /// The InPredicate subclass of QlValue
    /// 
    /// </summary>
    internal class InPredicate : QlValue
    {
        public long what => (long)(mem[WhileStatement.What]??-1L);
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
        public CList<long> vals => (CList<long>)(mem[QuantifiedPredicate.Vals]??CList<long>.Empty);
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public InPredicate(long dp,Context cx, QlValue w, BList<QlValue>? vs = null) 
            : base(dp, _Mem(cx,w,vs))
        {
            cx.Add(this);
        }
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,QlValue w,BList<QlValue>? vs)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var cs = CList<long>.Empty;
            var ag = w.IsAggregation(cx,CTree<long,bool>.Empty);
            m += (WhileStatement.What, w.defpos);
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
            if (cx.obs[what] is not QlValue w)
                throw new PEException("PE43350");
            return w.IsAggregation(cx,ags);
        }
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            if (cx.obs[what] is not QlValue w)
                throw new PEException("PE43351");
            var nw = w.Having(cx, dm, ap);
            var vs = BList<QlValue>.Empty;
            var ch = false;
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    var nv = v.Having(cx, dm, ap);
                    vs += nv;
                    ch = ch || nv != v;
                }
            return (w == nw && !ch) ? this :
                (QlValue)cx.Add(new InPredicate(cx.GetUid(), cx, nw, vs));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (v is InPredicate that)
            {
                if (found != that.found)
                    return false;
                if (cx.obs[what] is not QlValue w)
                    throw new PEException("PE43353");
                if (cx.obs[that.what] is QlValue tw && !w.Match(cx, tw))
                    return false;
                else if (that.what >= 0)
                    return false;
                if (cx.obs[that.val] is QlValue vl && !vl.Eval(cx).Contains(w.Eval(cx)))
                    return false;
                if (vals.Count != that.vals.Count)
                    return false;
                var tb = that.vals.First();
                for (var b = vals.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                    if (b.value() is long p && cx.obs[p] is QlValue e
                        && tb.value() is long tp
                        && cx.obs[tp] is QlValue te && !e.Match(cx, te))
                        return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (cx.obs[what] is QlValue w) r += w._Rdc(cx);
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
                    m += (_Depth,cx._DepthBV((CList<long>)ob, et.depth));
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
                r = cx.Add(r, WhileStatement.What, nw);
            var ns = cx.Fix(select);
            if (select!=ns)
                r = cx.Add(r, QuantifiedPredicate._Select, ns);
            var nv = cx.FixLl(vals);
            if (vals!=nv)
                r = cx.Add(r, QuantifiedPredicate.Vals, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (InPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(what, so, sv);
            if (wh != what)
                r +=(cx, WhileStatement.What, wh);
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
            return ((QlValue?)cx.obs[what])?.Grouped(cx, gs) != false &&
            gs.Grouped(cx, vals) != false;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (InPredicate)base.AddFrom(cx, q);
            if (cx.obs[what] is not QlValue w)
                throw new PEException("PE43360");
            var a = w.AddFrom(cx, q);
            if (a.defpos != r.what)
                r += (cx, WhileStatement.What, a.defpos);
            var vs = r.vals;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue u)
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
                if (b.value() is long p && cx.obs[p] is QlValue v && !v.KnownBy(cx, q, ambient))
                    return false;
            return cx.obs[what] is QlValue w && w.KnownBy(cx, q, ambient);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is QlValue v && !v.KnownBy(cx, cs, ambient))
                    return false;
            return cx.obs[what] is QlValue w && w.KnownBy(cx, cs, ambient);
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
                    if (cx.obs[p] is QlValue v)
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
            if (cx.obs[what] is QlValue w)
                r = w.Needs(cx, rs);
            if (cx.obs[select] is RowSet s)
                r += s.Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[select] is RowSet)
                return false;
            if (cx.obs[what] is QlValue w && !w.LocallyConstant(cx, rs))
                return false;
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is QlValue sv &&
                    !sv.LocallyConstant(cx, rs))
                    return false;
            return true;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[what] is not QlValue w)
                throw new PEException("PE49503");
            var tv = w.Eval(cx);
            if (cx.obs[val] is QlValue se && se.Eval(cx).Contains(tv))
                return TBool.For(found);
            if (cx.obs[select] is RowSet s)
                for (var rb = s.First(cx); rb != null; rb = rb.Next(cx))
                    if (rb[0].CompareTo(tv) == 0)
                        return TBool.For(found);
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is QlValue sv &&
                    sv.Eval(cx).CompareTo(tv) == 0)
                    return TBool.For(found);
            return TBool.For(!found);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var v = vals?.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is QlValue sv)
                    tg = sv.StartCounter(cx, rs, tg);
            if (cx.obs[what] is QlValue w)
                tg = w.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value() is long p && cx.obs[p] is QlValue sv)
                    tg = sv.AddIn(cx, rb, tg);
            if (cx.obs[what] is QlValue w)
                tg = w.AddIn(cx, rb, tg);
            return tg;
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            if (cx.obs[what] is QlValue w)
                qn = w.Needs(cx, r, qn);
            return qn;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50717");
            var r = this;
            BList<DBObject> ls;
            var vs = BList<QlValue>.Empty;
            var ch = false;
            QlValue w = (QlValue?)cx.obs[what] ?? SqlNull.Value,
                s = (QlValue?)cx.obs[select]??SqlNull.Value;
            if (w != SqlNull.Value && what>ap)
            {
                (ls, m) = w.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=w.defpos)
                {
                    ch = true; w = nv;
                }
            }
            if (s!=SqlNull.Value && select>ap)
            {
                (ls, m) = s.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != s.defpos)
                {
                    ch = true; s = nv;
                }
            }
            for (var b = vals?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v)
                {
                    if (v.defpos > ap)
                    {
                        (ls, m) = v.Resolve(cx, sr, m, ap);
                        if (ls[0] is QlValue nv && nv.defpos != v.defpos)
                        {
                            ch = true; v = nv;
                        }
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
            else if (vals != CList<long>.Empty)
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
    /// MemberPredicate is a subclass of QlValue
    /// 
    /// </summary>
    internal class MemberPredicate : QlValue
    {
        internal const long
            Lhs = -361; // long QlValue
        /// <summary>
        /// the test expression
        /// </summary>
        public long lhs => (long)(mem[Lhs]??-1L);
        /// <summary>
        /// found or not found
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// the right operand
        /// </summary>
        public long rhs => (long)(mem[MultipleAssignment.Rhs]??-1L);
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal MemberPredicate(long dp,Context cx,QlValue a, bool f, QlValue b)
            : base(dp, _Mem(cx,a,b)+(Lhs,a)+(QuantifiedPredicate.Found, f)
                  +(MultipleAssignment.Rhs,b)+(_Depth,_Depths(a,b)))
        {
            cx.Add(this);
        }
        protected MemberPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, QlValue a, QlValue b)
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
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49504");
            return lh.IsAggregation(cx,ags) + rh.IsAggregation(cx,ags);
        }
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49505");
            var nl = lh.Having(cx, dm, ap);
            var nr = rh.Having(cx, dm, ap);
            return (lh == nl && rh == nr) ? this :
                (QlValue)cx.Add(new MemberPredicate(cx.GetUid(), cx, nl, found, nr));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49506");
            if (v is MemberPredicate that)
            {
                if (cx.obs[that.lhs] is not QlValue tl || cx.obs[that.rhs] is not QlValue tr)
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
            if (cx.obs[lhs] is QlValue lh) r += lh._Rdc(cx);
            if (cx.obs[rhs] is QlValue rh) r += rh._Rdc(cx);
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
                r = cx.Add(r, MultipleAssignment.Rhs, rhs);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (MemberPredicate)base._Replace(cx, so, sv);
            var lf = cx.ObReplace(lhs,so,sv);
            if (lf != left)
                r +=(cx, Lhs, lf);
            var rg = cx.ObReplace(rhs,so,sv);
            if (rg != rhs)
                r +=(cx, MultipleAssignment.Rhs, rg);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49508");
            return lh.Grouped(cx, gs) && rh.Grouped(cx, gs);
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49509");
            var r = (MemberPredicate)base.AddFrom(cx, q);
            var a = lh.AddFrom(cx, q);
            if (a.defpos != r.lhs)
                r += (cx,Lhs, a.defpos);
            a = rh.AddFrom(cx, q);
            if (a.defpos != r.rhs)
                r += (cx, MultipleAssignment.Rhs, a.defpos);
            return (QlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override QlValue Invert(Context cx)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49510");
            return new MemberPredicate(defpos,cx,lh, !found, rh);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49511");
            return lh.Needs(cx, rs) + rh.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
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
            if (cx.obs[lhs] is QlValue lh)
                tg = lh.StartCounter(cx, rs, tg);
            if (cx.obs[rhs] is QlValue rh)
                tg = rh.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[lhs] is QlValue lh)
                tg = lh.AddIn(cx,rb, tg);
            if (cx.obs[rhs] is QlValue rh)
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
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49513");
            qn = lh.Needs(cx,r,qn);
            qn = rh.Needs(cx,r,qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs, bool ambient = false)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49514");
            return lh.KnownBy(cx, cs, ambient) 
                && rh.KnownBy(cx, cs, ambient);
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
                throw new PEException("PE49515");
            return lh.KnownBy(cx, q, ambient)
                && rh.KnownBy(cx, q, ambient);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            if (cx.obs[lhs] is not QlValue lh || cx.obs[rhs] is not QlValue rh)
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
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50718");
            var r = this;
            var ch = false;
            BList<DBObject> ls;
            QlValue lh = (QlValue?)cx.obs[lhs] ?? SqlNull.Value,
                rh = (QlValue?)cx.obs[rhs] ?? SqlNull.Value;
            if (lh != SqlNull.Value && lhs>ap)
            {
                (ls, m) = lh.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != lh.defpos)
                {
                    ch = true; lh = nv;
                }
            }
            if (rh != SqlNull.Value && rhs>ap)
            {
                (ls, m) = rh.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != rh.defpos)
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
    /// TypePredicate is a subclass of QlValue
    /// 
    /// </summary>
    internal class TypePredicate : QlValue
    {
        /// <summary>
        /// the test expression
        /// </summary>
        public long lhs => (long)(mem[MemberPredicate.Lhs]??-1L);
        /// <summary>
        /// OF or NOT OF
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// the right operand: a tree of Domain
        /// </summary>
        public BList<Domain> rhs => 
            (BList<Domain>)(mem[MultipleAssignment.Rhs] ?? BList<Domain>.Empty); // naughty: MemberPreciate Rhs is QlValue
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal TypePredicate(long dp,QlValue a, bool f, BList<Domain> r)
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool) 
                  + (_Depth,_Dep(a,r))+(MemberPredicate.Lhs,a.defpos)+(QuantifiedPredicate.Found,f)
                  +(MultipleAssignment.Rhs,r))
        {  }
        protected TypePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static int _Dep(QlValue a,BList<Domain> r)
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
                r = cx.Add(r, MultipleAssignment.Rhs, nr);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TypePredicate)base._Replace(cx, so, sv);
            var lh = cx.ObReplace(lhs, so, sv);
            if (lh != lhs)
                r +=(cx, MemberPredicate.Lhs, lh);
            return r;
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (cx.obs[lhs] is not QlValue v)
                throw new PEException("PE23205");
            if (from > 0)
                return this;
            var r = (TypePredicate)base.AddFrom(cx, q);
            var a = v.AddFrom(cx, q);
            if (a.defpos != r.lhs)
                r += (cx, MemberPredicate.Lhs, a.defpos);
            return (QlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override QlValue Invert(Context cx)
        {
            if (cx.obs[lhs] is not QlValue v)
                throw new PEException("PE23204");
            return new TypePredicate(defpos,v, !found, rhs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            if (cx.obs[lhs] is not QlValue v)
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
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
            if (cx.obs[lhs] is not QlValue lh)
                throw new PEException("PE49520");
            return lh.Needs(cx,r,qn);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[lhs] is not QlValue lh)
                throw new PEException("PE49521");
            return lh.Needs(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50719");
            var r = this;
            BList<DBObject> ls;
            if (cx.obs[lhs] is QlValue lh && lhs>ap)
            {
                (ls, m) = lh.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != lh.defpos)
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
    internal class PeriodPredicate : QlValue
    {
        internal Qlx op => (Qlx)(mem[SqlValueExpr.Op] ?? Qlx.NO);
        public PeriodPredicate(long dp,Context cx,QlValue op1, Qlx o, QlValue op2) 
            :base(dp,_Mem(cx,op1,op2)+(SqlValueExpr.Op,o)+(_Depth,_Depths(op1,op2)))
        {
            cx.Add(this);
        }
        protected PeriodPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, QlValue a, QlValue b)
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49530");
            return le.Grouped(cx, gs) && rg.Grouped(cx, gs);
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49531");
            var r = (PeriodPredicate)base.AddFrom(cx, q);
            var a = le.AddFrom(cx, q);
            if (a.defpos != r.left)
                r += (cx,Left, a.defpos);
            a = rg.AddFrom(cx, q);
            if (a.defpos != r.right)
                r += (cx,Right, a.defpos);
            return (QlValue)cx.Add(r);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49532");
            tg = le.StartCounter(cx, rs, tg);
            tg = rg.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
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
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49534");
            qn = le.Needs(cx, r, qn);
            qn = rg.Needs(cx, r, qn);
            return qn;
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49535");
            return le.Needs(cx, rs) + rg.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue rg)
                throw new PEException("PE49536");
            return le.LocallyConstant(cx, rs) && rg.LocallyConstant(cx, rs);
        }
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            if (cx.obs[left] is not QlValue le || cx.obs[right] is not QlValue ri)
                throw new PEException("PE42333");
            var nl = le.Having(cx, dm, ap);
            var nr = ri.Having(cx, dm, ap);
            return (le == nl && ri == nr) ? this :
                (QlValue)cx.Add(new PeriodPredicate(cx.GetUid(), cx, nl, op, nr));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (v is PeriodPredicate that)
            {
                if (op != that.op)
                    return false;
                if (cx.obs[left] is QlValue le)
                    if (cx.obs[that.left] is QlValue tl && !le.Match(cx, tl))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is QlValue rg)
                    if (cx.obs[that.right] is QlValue tr && !rg.Match(cx, tr))
                        return false;
                    else if (that.right >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50720");
            var r = this;
            var ch = false;
            QlValue lf = (QlValue?)cx.obs[left] ?? SqlNull.Value,
                rg = (QlValue?)cx.obs[right] ?? SqlNull.Value;
            BList<DBObject> ls;
            if (lf != SqlNull.Value && left>ap)
            {
                (ls, m) = lf.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=lf.defpos)
                {
                    ch = true; lf = nv;
                }
            }
            if (rg != SqlNull.Value && right>ap)
            {
                (ls, m) = rg.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos != rg.defpos)
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
    internal abstract class RowSetPredicate : QlValue
    {
        internal const long
            RSExpr = -363; // long RowSet
        public long expr => (long)(mem[RSExpr] ?? -1);
        /// <summary>
        /// the base query
        /// </summary>
        public RowSetPredicate(long dp,Context cx,RowSet e) 
            : base(dp, _Mem(e) + (RSExpr, e.defpos))
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
            if (cx.obs[expr] is not QlValue e)
                throw new PEException("PE49540");
            return e._Rdc(cx);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var nq = cx.Fix(expr);
            if (nq != expr)
                r += (RSExpr, nq);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSetPredicate)base._Replace(cx, so, sv);
            var e = cx.ObReplace(expr, so, sv);
            if (e != expr)
                r += (cx, RSExpr, e);
            return r;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
        {
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return false;
        }
        internal RowSet Exec(Context cx)
        {
            var r = cx.obs[expr] as RowSet ?? throw new PEException("PE70822");
            cx.result = r;
            return r;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = (cx.result??domain).rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    tg = s.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
        {
            for (var b = (cx.result??domain).rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue s)
                    tg = s.AddIn(cx, rb, tg);
            return tg;
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
            if (cx.obs[expr] is not QlValue e)
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
            var rs = cx.obs[expr] as RowSet ?? throw new PEException("PE70823");
            cx.Add(rs + (RowSet._Built, false));
            return TBool.For(rs?.First(cx) is not null);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("exists ");
            sb.Append(Uid(expr));
            return sb.ToString();
        }
    }
    /// <summary>
    /// the unique predicate
    /// 
    /// </summary>
    internal class UniquePredicate : RowSetPredicate
    {
        public UniquePredicate(long dp,Context cx, RowSet e) : base(dp, cx, e)
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
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue _Eval(Context cx)
        {
            var r = Exec(cx);
            RTree a = new (-1L,cx,r, TreeBehaviour.Disallow, TreeBehaviour.Disallow);
            for (var rb=r.First(cx);rb!= null;rb=rb.Next(cx))
                if (RTree.Add(ref a, rb, cx.cursors) == TreeBehaviour.Disallow)
                    return TBool.False;
            return TBool.True;
        }
        internal override QlValue Having(Context c, Domain dm, long ap)
        {
            return base.Having(c, dm, ap); // throws error
        }
        internal override bool Match(Context c, QlValue v)
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
    internal class NullPredicate : QlValue
    {
        internal const long
            NIsNull = -364, //bool
            NVal = -365; //long QlValue
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
        internal NullPredicate(long dp,QlValue v, bool b)
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
            if (cx.obs[val] is not QlValue v)
                throw new PEException("PE49550");
            return v.IsAggregation(cx,ags);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new NullPredicate(dp,m);
        }
        internal override QlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            if (cx.obs[val] is not QlValue v)
                throw new PEException("PE49550");
            var r = (NullPredicate)base.AddFrom(cx, q);
            var a = v.AddFrom(cx, q);
            if (a.defpos != val)
                r += (cx, NVal, a.defpos);
            return (QlValue)cx.Add(r);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nv = cx.Fix(val);
            if (nv != val)
                r = cx.Add(r, NVal, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (NullPredicate)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(val,so,sv);
            if (vl != val)
                r +=(cx, NVal, vl);
            return r;
        }
        internal override QlValue Having(Context cx, Domain dm, long ap)
        {
            if (cx.obs[val] is not QlValue v)
                throw new PEException("PE49551");
            var nv = v.Having(cx, dm, ap);
            return (v==nv) ? this :
                (QlValue)cx.Add(new NullPredicate(cx.GetUid(), nv, isnull));
        }
        internal override bool Match(Context cx, QlValue v)
        {
            if (v is NullPredicate that)
            {
                if (isnull != that.isnull)
                    return false;
                if (cx.obs[val] is QlValue w && cx.obs[that.val] is QlValue x)
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
            return ((QlValue?)cx.obs[val])?.Grouped(cx, gs)??false;
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
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, cs, ambient)??false;
        }
        internal override bool KnownBy(Context cx, RowSet q, bool ambient = false)
        {
            return ((QlValue?)cx.obs[val])?.KnownBy(cx, q, ambient)??false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb, bool ambient = false)
        {
            if (kb[defpos] is Domain d)
                return new CTree<long, Domain>(defpos, d);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (val >= 0 && cx.obs[val] is QlValue v)
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
            if (cx.obs[val] is not QlValue v)
                throw new PEException("PE49560");
            return v.Needs(cx,r,qn);
        }
        internal override CTree<long, bool> Needs(Context cx, long rs)
        {
            if (cx.obs[val] is not QlValue v)
                throw new PEException("PE49562");
            return v.Needs(cx, rs);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, RowSet sr, 
            BTree<long, object> m, long ap)
        {
            if (defpos < ap)
                throw new PEException("PE50721");
            var r = this;
            BList<DBObject> ls;
            if (cx.obs[val] is QlValue s && val>ap)
            {
                (ls, m) = s.Resolve(cx, sr, m, ap);
                if (ls[0] is QlValue nv && nv.defpos!=s.defpos)
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
    /// GqlNode will evaluate to a TNode (and GqlEdge to a TEdge) once the enclosing
    /// GraphInsertStatement or MatchStatement has been Obeyed.
    /// In general, any of the contained SqlValues in an GqlNode may evaluate to via a TGParam 
    /// that should have been bound by MatchStatement._Obey.
    /// However, TGParams are not found in GraphInsertStatement graphs.
    /// GraphInsertStatement._Obey will traverse its GraphExpression so that the context maps SqlNodes to TNodes.
    /// MatchStatement._Obey will traverse its GraphExpression binding as it goes, so that the dependent executable
    /// is executed only for fully-bound SqlNodes.
    /// For an insert node label set, tok (mem[SVE.Op]) here can be Qlx.COLON or Qlx.AMPERSAND
    /// and the order of entries in the CTree is naturally in declaration order
    /// If Qlx.COLON, labels is a set of type labels where all are existing types except maybe the last
    /// If Qlx.AMPERSAND then all labels refer to existing types and the combination may be new.
    /// Thus at most one new graph type is required in _NodeType.
    /// </summary>
    internal class GqlNode : QlValue
    {
        internal const long
            DocValue = -477,    // CTree<string,QlValue> 
            IdValue = -480,     // long             QlValue of Int
            _Label = -360,      // GqlLabel (a subclass of Domain, deals with label sets etc)
            PrevTok = -232,     // Qlx
            State = -245;       // CTree<long,TGParam> tgs in this GqlNode  (always empty for GraphInsertStatement)
        public CTree<string, QlValue> docValue => (CTree<string,QlValue>)(mem[DocValue]??CTree<string,QlValue>.Empty);
        public long idValue => (long)(mem[IdValue] ?? -1L);
        public Domain label => (Domain)(mem[_Label] ?? GqlLabel.Empty);
        internal Qlx tok => (Qlx)(mem[PrevTok] ?? Qlx.Null);
        public CTree<long, bool> search => // can occur in Match GraphExp
            (CTree<long, bool>)(mem[RowSet._Where] ?? CTree<long, bool>.Empty);
        public CTree<long, TGParam> state =>
            (CTree<long, TGParam>)(mem[State] ?? CTree<long, TGParam>.Empty);
        public BTree<long,Names> defs => 
            (BTree<long,Names>)(mem[ObInfo.Defs] ?? BTree<long,Names>.Empty);
        public GqlNode(Ident nm, BList<Ident> ch, Context cx, long i, CTree<string, QlValue> d,
            CTree<long, TGParam> tgs, Domain? dm = null, BTree<long, object>? m = null)
            : base(nm, nm, ch, cx, _Type(dm,cx,d,m), _Mem(nm, i, d, tgs, dm, cx, m))
        {
            if (dm is null && tgs[-(long)Qlx.TYPE] is TGParam tg && cx.names[tg.value].Item2 is long t)
                for (var b = cx.obs[t]?.infos[cx.role.defpos]?.names.First();b!=null;b=b.Next())
                    if (b.value().Item2 is long p && p<Transaction.Analysing && cx.bindings.Contains(p))
                            cx.Add(tg.value, nm.lp, this);
            cx.names += (nm.ident, (nm.lp,defpos));
        }
        protected GqlNode(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Ident nm, long i, CTree<string, QlValue> d, CTree<long, TGParam> tgs,
            Domain? dm, Context cx, BTree<long, object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (i > 0)
            {
                if ((cx.parse.HasFlag(ExecuteStatus.Compile) || (i >= Transaction.Analysing && i < Transaction.Executables))
                    && cx.names[nm.ident].Item2 is long p)
                    i = p;
                m += (IdValue, i);
            }
            if (d is not null)
                m += (DocValue, d);
            var ng = CTree<long, TGParam>.Empty;
            for (var b = tgs.First(); b != null; b = b.Next())
                if (cx.parse.HasFlag(ExecuteStatus.Compile) || (b.key() >= Transaction.Analysing && b.key() < Transaction.Executables))
                {
                    if (b.value() is TGParam tg && cx.names[tg.value].Item2 is long p)
                    {
                        tg = new TGParam(p,tg.value,tg.dataType,tg.type,tg.from);
                        ng += (p, tg);
                    }
                //    else
                        ng += (b.key(), b.value());
                }
            m += (State, ng);
            m += (ObInfo.Defs, cx.defs);
            m += (ObInfo._Names, cx.names);
            if (!m.Contains(_Label) && dm is not null && dm.defpos>0) // otherwise leave it unlabelled
                m += (_Label, dm); // an explicit NodeType
            return m;
        }
        protected static NodeType _Type(Domain? dm,Context cx,CTree<string,QlValue> d,BTree<long,object>? m,
            long l= -1L,long a= -1L)
        {
            if (dm is NodeType nt && dm.defpos >= 0)
            {
                if (m?[_Label] is NodeType nl && nl.defpos>0 && nl.EqualOrStrongSubtypeOf(nt))
                    return nl;
                return nt;
            }
            if (m is null)
                return Domain.NodeType;
            if (m[_Label] is GqlLabel lb && cx.obs[cx.dnames[lb.name].Item2] is NodeType n)
                return n;
            if (l >= 0)
                m += (GqlEdge.LeavingValue, l);
            if (a >= 0)
                m += (GqlEdge.ArrivingValue, a);
            return (m[_Label] as Domain)?.ForExtra(cx,m+(DocValue,d))?? Domain.NodeType;
        }
        internal override string NameFor(Context cx)
        {
            if (name == "COLON" && label.defpos>=0)
                return label.NameFor(cx);
            return base.NameFor(cx);
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, Ident ic, Ident? n, DBObject? r)
        {
            if (n is not null && domain.infos[cx.role.defpos] is ObInfo si && ic.sub is not null
                &&  cx.db.objects[si.names[n.ident].Item2] is TableColumn tc1)
            {
                var co = new QlInstance(n, cx, defpos, tc1);
                var nc = new SqlValueExpr(ic.uid, cx, Qlx.DOT, this, co, Qlx.NO);
                cx.Add(co);
                return (cx.Add(nc),n.sub);
            }
            return base._Lookup(lp, cx, ic, n, r);
        }
        public static GqlNode operator +(GqlNode n, (long, object) x)
        {
            return (GqlNode)n.New(n.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GqlNode(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GqlNode(dp, m);
        }
        internal override bool isConstant(Context cx)
        {
            for (var b = docValue.First(); b != null; b = b.Next())
                if (b.value() is QlValue v && !v.isConstant(cx))
                    return false;
            return true;
        }
        internal override bool Verify(Context cx)
        {
            return true;
        }
        internal override Domain FindType(Context cx, Domain dt)
        {
            if (domain.defpos >= 0)
                return domain;
            if (dt.defpos >= 0)
                return dt;
            var du = CTree<Domain, bool>.Empty;
            var nd = dt;
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()??-1L] is NodeType bt && bt.infos[cx.role.defpos] is ObInfo ti)
                {
                    if (label.name != null && ti.name != label.name)
                        continue;
                    for (var c = docValue.First(); c != null; c = c.Next())
                        if (!ti.names.Contains(c.key()))
                            goto noMatch;
                    du += (bt,true);
                    nd = bt;
                noMatch:;
                }
            return (du.Count<=1)?nd:new Domain(-1L,Qlx.UNION,du);
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.values[defpos] ?? cx.binding[idValue] ?? TNull.Value;
        }
        internal virtual int MinLength(Context cx)
        {
            return 0;
        }
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            if (label!=GqlLabel.Empty)
                return label.For(cx, ms, xn, ds);
            return base.For(cx, ms, xn, ds);
        }
        internal virtual GqlNode Add(Context cx, GqlNode? an, CTree<long, TGParam> tgs, long ap)
        {
            return (GqlNode)cx.Add(this + (State, tgs + state));
        }
        internal virtual Domain _NodeType(Context cx, NodeType dt, long ap, bool allowExtras = true)
        {
            var nd = this;
            if (dt.name==label.name)
                return dt;
            var tl = nd.label.OnInsert(cx,0L,mem);
            var ll = docValue;
            NodeType? nt = null; // the node type of this node when we find it or construct it
            var md = CTree<Qlx, TypedValue>.Empty; // some of what we will find on this search
                                                   // Begin to think about the names of special properties for the node we are building
                                                   // We may be using the default names, or we may inherit them from existing types
                                                   //string? sd = null; // ID if present
            if (tl.Count <= 1)
            { // unlabelled nodes have types determined by their kind and property set
                var ps = CTree<string, bool>.Empty;
                for (var b = ll.First(); b != null; b = b.Next())
                        ps += (b.key(), true);
                if (cx.role.unlabelledNodeTypesInfo[ps] is long p && cx.db.objects[p] is NodeType nu)
                    return nu;
                var un = (cx.db.objects[cx.UnlabelledNodeSuper(ps)] is Domain st) ?
                    new CTree<Domain, bool>(st, true) : CTree<Domain, bool>.Empty;
                var ph = new PNodeType(nd.label.name,Domain.NodeType,un,-1L,cx.db.nextPos,cx);
                nt = (NodeType)(cx.Add(ph)??throw new DBException("42105"));
                for (var b = ll.First(); b != null; b = b.Next())
                {
                    var pc = new PColumn3(nt, b.key(), -1, b.value().domain, PColumn.GraphFlags.None,
                    -1L, -1L, cx.db.nextPos, cx);
                        cx.Add(pc);
                }
                if (cx.db.objects[nt.defpos] is NodeType nn)
                    nt = nn;
            }
            else if (nd.label.kind == Qlx.AMPERSAND)
            {
                var sb = new StringBuilder();
                var cm = "";
                for (var b = tl.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = "&"; sb.Append(b.key().name);
                }
                // JoinedNodeType is not committed to the database: it is a syntactic device leading to Record4 creation
                NodeType jt = new JoinedNodeType(ap,cx.GetUid(), sb.ToString(), Domain.NodeType, 
                    new BTree<long,object>(Domain.NodeTypes,tl), cx);
                return jt;
            }
            else
            {
                var fi =  tl.First()?.key() as NodeType ?? throw new PEException("PE70301");
                if (cx.db.objects[cx.role.nodeTypes[fi.name] ?? -1L] is NodeType fd)
                    fi = fd;
                for (var b = ll.First(); b != null; b = b.Next())
                    if (fi.infos[cx.role.defpos] is ObInfo oi && !oi.names.Contains(b.key()))
                    {
                        for (var c = ((Transaction)cx.db).physicals.First(); c != null; c = c.Next())
                            if (c.value() is PColumn3 p3 && p3.name == b.key())
                                goto skip;
                        var pc = new PColumn3(fi, b.key(), -1, b.value().domain,PColumn.GraphFlags.None,
                            -1L, -1L, cx.db.nextPos, cx);
                        cx.Add(pc);
                    skip:;
                    }
                if (cx.db.objects[fi.defpos] is NodeType nn)
                    nt = nn;
            }
            // Pyrrho extends GQL by allowing :a:b in INSERT (as in Neo4j) but accepts that some vendors have
            // this meaning :a is under :b and some that :b is under :a. 
            // Pyrrho infers the relationship from the order of declaration: if one of them has 
            // already been declared then the other will be a subtype. If neither, then the second will be
            // the subtype. If both have been declared already it is an error.
            var dc = CTree<long, bool>.Empty;
            // if existing components are related, the top and bottom types found 
            // We know we have :a:b if nd.name is not AMPERSAND and tl.Count==2
            // gc,gd etc will be the new node type and this data will be used to define the new type gt
            var te = Domain.Null;
            var be = Domain.Null;
            //QlValue? sg = null;
            for (var b = tl.First(); b != null; b = b.Next())  // tl is the iterative type label
            {
                if ((cx.db.objects[cx.role.dbobjects[b.key().name ?? ""] ?? -1L] as Domain?? b.key()) is NodeType ne)
                {
                    if (te == Domain.Null || te.defpos < ne.defpos)
                        te = ne;
                    if (be == Domain.Null || ne.defpos < be.defpos)
                        be = ne;
                    if (te is NodeType tt && nd.label.kind == Qlx.COLON)
                        nt = tt;
                }
            }
            if (nt is null)
                throw new DBException("42000", "_NodeType")
                    .Add(Qlx.INSERT_STATEMENT,new TChar(name??"??"));
            return nt;
        }
        internal GqlNode Create(Context cx, NodeType dt, long ap, bool allowExtras = true)
        {
            var nd = this;
            // If there are no labels, we create new node types rather than adding altering existing
            allowExtras = allowExtras && label.ForExtra(cx,mem) is not null;
            NodeType nt;
            var od = cx.names;
            var ods = cx.defs;
            if (allowExtras)
            {
                nt = (NodeType)_NodeType(cx, dt, ap, allowExtras);
                if (nt != nd.domain)
                {
                    nd += (_Domain, nt);
                    cx.Add(nd);
                }
                if (nt.defpos < 0 && docValue == CTree<string, QlValue>.Empty)
                    throw new DBException("42161", "Specification", name ?? "Unbound");
                // We are now ready to check or build the node type nt
                cx.defs = defs;
                cx.names = dt.infos[cx.role.defpos]?.names??Names.Empty;
                if (nt.defpos >= 0 && nt.defpos < Transaction.Analysing)
                    nt = nt.Check(cx, nd, 0L, allowExtras);
                else if (cx.db.objects[cx.role.defpos] is Role rr
                                    && cx.db.objects[rr.dbobjects[nt.name ?? "_"] ?? -1L] is NodeType ot)
                    nt = ot;
                else
                    nt = nt.Build(cx, this, ap,
                        new BTree<long, object>(Domain.NodeTypes, label.OnInsert(cx, ap, mem)));
                if (nt is JoinedNodeType) // JoinedNodeType will have done everything
                    return this;
            }
            else
                nt = dt;
            nd += (_Domain, nt);
            nd = (GqlNode)cx.Add(nd);
            var ls = docValue;
            ls = nd._AddEnds(cx, ls);
            TNode? tn = null;
            if (nt.defpos > 0 && !cx.parse.HasFlag(ExecuteStatus.GraphType))
            {
                var vp = cx.GetUid();
                var ts = new TableRowSet(cx.GetUid(), cx, nt.defpos, ap);
                var lo = BList<DBObject>.Empty;
                var iC = CList<long>.Empty;
                var tb = ts.First();
                for (var bb = ts.rowType.First(); bb != null && tb != null; bb = bb.Next(), tb = tb.Next())
                    if (bb.value() is long bq && cx.NameFor(bq) is string n9
                        && ls[n9] is QlValue sv && sv is not SqlNull)
                    {
                        lo += sv;
                        iC += tb.value();
                    }
                // ll generally has fewer columns than nt
                // carefully construct what would happen with ordinary SQL INSERT VALUES
                // we want dm to be constructed as having a subset of fm's columns using fm's iSMap
                var dr = CList<long>.Empty;
                var ds = CTree<long, Domain>.Empty;
                for (var b = lo.First(); b != null; b = b.Next())
                    if (b.value() is QlValue sv && sv.defpos > 0)
                    {
                        dr += sv.defpos;
                        ds += (sv.defpos, sv.domain);
                    }
                var fm = (TableRowSet)ts.New(cx.GetUid(), ts.mem + (Domain.RowType, dr) 
                    + (Domain.Representation, ds) + (Domain.Display, dr.Length));
                var dm = (dr.Length==0)?Domain.Row:new Domain(-1L, cx, Qlx.ROW, ds, dr);
                cx.Add(fm);
                var rn = new SqlRow(cx.GetUid(), cx, dm, dm.rowType);
                cx.Add(rn);
                QlValue n = rn;
                n = new SqlRowArray(vp, cx, dm, new CList<long>(n.defpos));
                var sce = n.RowSetFor(ap,vp, cx, fm.rowType, fm.representation)
                    + (cx, RowSet.RSTargets, fm.rsTargets)
                    + (RowSet.Asserts, RowSet.Assertions.AssignTarget);
                var s = new SqlInsert(cx.GetUid(), fm, sce.defpos, ts + (Domain.RowType, iC));
                cx.Add(s);
                var np = cx.db.nextPos;
                // NB: The TargetCursor/trigger machinery will place values in cx.values in the !0.. range
                // From the point of view of graph operations these are spurious, and should not be accessed
                // The only exception is to retrieve the value of tn
                s._Obey(cx); // ??
                if (nd.name != null)
                    cx.Add(nd.name, ap, nd);
                tn = cx.values[np] as TNode ?? throw new DBException("42105").Add(Qlx.INSERT_STATEMENT);
            }
            if (tn is not null)
            {
                nd += (SqlLiteral._Val, tn);
                if (nt.idCol<0)
                    nd += (IdValue, tn.tableRow.defpos);
                cx.values += (nd.defpos, tn);
            }
            cx.Add(nd);
            cx.defs = defs;
            cx.names = nt.infos[cx.role.defpos]?.names??Names.Empty;
            return nd;
        }
        protected virtual CTree<string,QlValue> _AddEnds(Context cx, CTree<string,QlValue> ls)
        {
            if (domain is NodeType nt && cx.obs[idValue] is GqlNode il
                    && cx.NameFor(nt.idCol) is string iC && !ls.Contains(iC))
            {
                if (il.Eval(cx) is TNode tn)
                    ls += (iC, (QlValue)cx.Add(new SqlLiteral(cx.GetUid(), tn.id)));
                else
                    ls += (iC, SqlNull.Value);
            }
            return ls;
        }
        /// <summary>
        /// This method is called during Match, so this.domain is not helpful.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal bool CheckProps(Context cx, MatchStatement ms, TNode n)
        {
            //       if (this is not GqlEdge && n is TEdge)
            //           return false;
            if (cx.binding[idValue] is TNode ni && ni.id.CompareTo(n.id) != 0)
                return false;
            cx.values += n.tableRow.vals;
            var ns = n._Names(cx);
            if (domain.defpos > 0L && n.dataType.defpos > 0L)
            {
                if (!n.dataType.EqualOrStrongSubtypeOf(domain))
                    return false;
            } else
            {
                cx.ParsingMatch = true; // yuk prevent creation of nodetype binding variable
                var ts = label.OnInsert(cx, defpos);
                cx.ParsingMatch = false;
                for (var b=ts.First();b!=null;b=b.Next())
                    if (b.key().defpos==Domain.NodeType.defpos || b.key().defpos==Domain.EdgeType.defpos 
                        ||(cx.obs[ms.bindings] as RowSet)?.domain.representation.Contains(b.key().defpos)!=false)
                            ts -= b.key();
                if (n.dataType is NodeType nt && !nt.Match(cx, ts))
                    return false;
            }
            for (var b = docValue?.First(); b != null; b = b.Next())
                if (b.key() is string k)
                {
                    if (!ns.Contains(k))
                        return false;
                    if (ns[k].Item2 is long e && !n.tableRow.vals.Contains(e))
                        return false;
                    if (b.value().Eval(cx) is TypedValue xv && xv is not TArray)
                    {
                        if (xv is TGParam tg)
                        {
                            if (state.Contains(tg.uid) || cx.binding[tg.uid] is not TypedValue vv)
                                continue;
                            xv = vv;
                        }
                        switch (k)
                        {
                            //          case "ID":
                            //          case "LEAVING":
                            //          case "ARRIVING":  // no need
                            //              break;
                            case "SPECIFICTYPE":
                                if (!n.dataType.Match(xv.ToString()))
                                    return false;
                                break;
                            default:
                                if (ns[k].Item2 is long d && xv.CompareTo(n.tableRow.vals[d]) != 0)
                                    return false;
                                break;
                        }
                    }
                }
            var ob = cx.names;
            if (search != CTree<long, bool>.Empty)
                for (var b = n.dataType.infos[cx.role.defpos]?.names.First(); b != null; b = b.Next())
                    if (b.value().Item2 is long p)
                        cx.names += (b.key(), (b.value().Item1, p));
            for (var b = search.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue se)
                {
                    var r = se.Eval(cx);
                    if (r != TBool.True)
                        return false;
                }
            cx.names = ob;
            return true;
        }
        internal CTree<long, TypedValue> EvalProps(Context cx, NodeType nt)
        {
            var r = CTree<long, TypedValue>.Empty;
            if (nt.infos[cx.role.defpos] is not ObInfo ni)
                return r;
            for (var b = docValue?.First(); b != null; b = b.Next())
                if (cx._Ob(ni.names[b.key()].Item2) is DBObject ob && b.value().Eval(cx) is TypedValue v)
                    r += (ob.defpos, v);
            return r;
        }
        internal virtual NodeType? InsertSchema(Context cx)
        {
            var ns = infos[cx.role.defpos]?.names;
            var tb = domain as NodeType;
            for (var b=docValue.First();b!=null;b=b.Next())
                if (b.value() is QlValue qv && ns?.Contains(b.key()) == false 
                    && tb is not null && tb.defpos>0)
                {
                    var pc = new PColumn(Physical.Type.PColumn, tb, b.key(), 
                        tb.Length, qv.domain, cx.db.nextPos, cx);
                    tb = (NodeType?)cx.Add(pc);
                }
            return tb;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (tok != Qlx.Null) { sb.Append(' '); sb.Append(tok); }
            if (idValue > 0)
            { sb.Append(" Id="); sb.Append(Uid(idValue)); }
            if (label is not NodeType)
            { sb.Append(':'); sb.Append(label.ToString()); }
            var cm = " {";
            for (var b = docValue?.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append('='); sb.Append(b.value());
                }
            if (cm == ",") sb.Append('}');
            if (search != CTree<long, bool>.Empty)
            {
                sb.Append(" where ["); cm = "";
                for (var b = search.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(']');
            }
            cm = " ";
            for (var b = state.First(); b != null; b = b.Next())
                if (b.key() < 0 && b.value() is TGParam ts)
                {
                    sb.Append(cm); cm = ",";
                    var k = (Qlx)(int)(-b.key());
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
    internal class GqlReference : GqlNode
    {
        internal const long 
            RefersTo = -452; // long GqlNode
        internal long refersTo => (long)(mem[RefersTo] ?? -1L);
        internal GqlReference(Context cx,long ap, long dp, GqlNode n, Qlx pr=Qlx.NO)
            : this(dp, n.mem + (RefersTo, n.defpos)+ (PrevTok,pr) )
        {
            cx.names += (n.name ?? throw new PEException("PE40431"), (ap,n.defpos));
        }
        internal GqlReference(long dp, NodeType nt)
            : this(dp, nt.mem + (RefersTo, nt.defpos) + (_Domain, nt)) { }
        protected GqlReference(long dp, BTree<long, object> m) : base(dp, m) { }
        public static GqlReference operator+ (GqlReference r,(long,object)x)
        {
            return new GqlReference(r.defpos, r.mem + x);
        }
        internal override TypedValue _Eval(Context cx)
        {
            return cx.obs[refersTo]?._Eval(cx) ?? TNull.Value;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (GqlReference)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(refersTo, so, sv);
            if (vl != refersTo)
                r += (RefersTo, vl);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            if (refersTo < Transaction.Executables || refersTo >= Transaction.HeapStart)
            {
                var nc = cx.Fix(refersTo);
                if (nc != refersTo)
                    r += (RefersTo, nc);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" refers to "); sb.Append(Uid(refersTo));
            return sb.ToString();
        }

    }
    /// <summary>
    /// For an insert edge label set, tok (mem[SVE.Op]) here can be Qlx.COLON or Qlx.AMPERSAND
    /// and the order of entries in the CTree is naturally in declaration order
    /// If Qlx.COLON, labels is a set of type labels where all are existing types except maybe the last
    /// If Qlx.AMPERSAND then all labels refer to existing types and the combination may be new.
    /// A further graph type selection by leavingValue and arrivingValue will occur in GqlEdge._NodeType
    /// and at most one new edge type will be required.
    /// </summary>
    internal class GqlEdge : GqlNode
    {
        internal const long
            ArrivingValue = -479,  // long QlValue
            LeavingValue = -478;   // long QlValue
        public long arrivingValue => (long)(mem[ArrivingValue]??-1L);
        public long leavingValue => (long)(mem[LeavingValue]??-1L);
        public GqlEdge(Ident nm, BList<Ident> ch, Context cx, Qlx t, long i, long l, long a,
            CTree<string, QlValue> d, CTree<long, TGParam> tgs, Domain? dm = null, BTree<long, object>? m = null)
            : base(nm, ch, cx, i, d, tgs, _Type(dm, cx, d, m, l, a), _Mem(cx, d, tgs, dm, m, nm, i, l, a, t))
        {
            if (dm is null && tgs[-(long)Qlx.TYPE] is TGParam tg
               && cx.names[tg.value].Item2 is long p && p < Transaction.Analysing && cx.bindings.Contains(p))
                cx.names += (tg.value, (nm.lp,p));
            if (dm is EdgeType et)
            {
                if (cx.obs[l] is GqlNode ln && et.leavingType != ln.domain.defpos
                    && cx.db.objects[et.leavingType] is NodeType lt
                    && lt.EqualOrStrongSubtypeOf(ln.domain) == true)
                {
                    cx.Replace(ln, ln + (_Domain, lt));
                    cx.names += (ln.NameFor(cx), (nm.lp,ln.defpos));
                    cx.defs += (ln.defpos, lt.names);
                }
                if (cx.obs[a] is GqlNode an && et.arrivingType != an.domain.defpos
                    && cx.db.objects[et.arrivingType] is NodeType at
                    && at.EqualOrStrongSubtypeOf(an.domain) == true)
                {
                    cx.Replace(an, an + (_Domain, at));
                    cx.names += (an.NameFor(cx), (nm.lp,an.defpos));
                    cx.defs += (an.defpos, at.names);
                }
            }
        }
        protected GqlEdge(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,CTree<string,QlValue> d,
             CTree<long, TGParam> tgs, Domain? dm , BTree<long,object>?m, Ident nm, long i,long l,long a, Qlx t)
        {
            m ??= BTree<long, object>.Empty;
            if (i > 0)
            {
                if ((cx.parse.HasFlag(ExecuteStatus.Compile)
                        ||(i >= Transaction.Analysing && i < Transaction.Executables))
                    && cx.names[nm.ident].Item2 is long p && p>0)
                    i = p;
                m += (IdValue, i);
            }
            m += (DocValue, d);
            var ng = CTree<long, TGParam>.Empty;
            for (var b = tgs.First(); b != null; b = b.Next())
                if (cx.parse.HasFlag(ExecuteStatus.Compile) 
                    || (b.key() >= Transaction.Analysing && b.key() < Transaction.Executables))
                {
                    if (b.value() is TGParam tg && cx.names[tg.value].Item2 is long p)
                        ng += (p, tg);
                    else
                        ng += (b.key(), b.value());
                }
            m += (State, ng);
            m += (ObInfo.Defs, cx.defs);
            if (!m.Contains(_Label) && dm is not null && dm.defpos > 0) // otherwise leave it unlabelled
                m += (_Label, dm); // an explicit NodeType
            if (l > 0)
                m += (LeavingValue, l);
            if (a > 0)
                m += (ArrivingValue, a);
            m += (PrevTok, t);
            return m;
        }
        public static GqlEdge operator+(GqlEdge e,(long,object)x)
        {
            return (GqlEdge)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (GqlEdge)New(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GqlEdge(dp, m);
        }
        internal override int MinLength(Context cx)
        {
            return 1;
        }
        internal override Domain _NodeType(Context cx, NodeType dt, long ap, bool allowExtras = true)
        {
            var nd = this;
            //  nd for an edge will have a specific leavingnode and a specific arrivingnode 
            // the special columns for this node
            // TableColumn? iC = null;
            var lS = cx.obs[nd.leavingValue] as GqlNode;
            var lI = lS?.idValue ?? -1L;
            var lg = cx.binding[lI] ?? cx.Node(lS?.domain, lI);
            var lT = lS?.domain;
            if (lT is null || lT.defpos < 0L)
                lT = lg?.dataType ?? cx.obs[lI]?.domain ?? lS?.domain;
            //var lN = lT?.name;
            var aS = cx.obs[nd.arrivingValue] as GqlNode;
            var aI = aS?.idValue ?? -1L;
            var ag = cx.binding[aI] ?? cx.Node(aS?.domain, aI);
            var aT = aS?.domain;
            if (aT is null || aT.defpos < 0)
                aT = ag?.dataType ?? cx.obs[aI]?.domain ?? aS?.domain;
            //var aN = aT?.name;
            // a label with at least one char QlValue must be here
            // evaluate them all as TTypeSpec or TChar
            if (cx.db.objects[cx.role.edgeTypes[label.name ?? ""] ?? -1L] is Domain ed)
            {
                if (lT is null || aT is null)
                    throw new DBException("22G0W", label.name ?? "");
                if (ed is EdgeType ef && CanConnect(cx,ef.leavingType,lT.defpos) && CanConnect(cx,ef.arrivingType,aT.defpos))
                    return ed;
                if (ed.kind != Qlx.UNION) throw new DBException("42002",label.name??"");
                for (var c = ed.unionOf.First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key().defpos] is EdgeType ee
                        && ee.leavingType == lT.defpos && ee.arrivingType == aT.defpos)
                        return ee;
            }
            var tl = nd.label.OnInsert(cx, 0L, nd.mem);
            EdgeType? nt = null; // the node type of this node when we find it or construct it
            var md = TMetadata.Empty; // some of what we will find on this search
                                      // Begin to think about the names of special properties for the node we are building
                                      //string? sd = null;
                                      //var il = "LEAVING";
                                      //var ia = "ARRIVING";
                                      // it may be that all node/edge types for all parts of the label exist already
                                      // certainly the predecessor of an existing node must exist.
                                      // if the last one is undefined we will build it using the given property tree
                                      // (if it is defined we may add properties to it)
                                      // if types earlier in the label are undefined we will create them here
            var dc = CTree<long, bool>.Empty;
            // if existing components are related, the top and bottom types found 
            EdgeType? te = null;
            EdgeType? be = null;
            for (var b = tl.First(); b != null; b = b.Next())  // tl is the iterative type label
            {
                if (b.key() is EdgeType ne)
                {
                    if (te is null || te.defpos < ne.defpos)
                        te = ne;
                    if (be is null || ne.defpos < be.defpos)
                        be = ne;
                }
            }
            if (te is not null)
                nt = te;
            if (nd.label.defpos<0 && lT is not null && aT is not null)
            { // unlabelled edges have types determined by their kind and property set
                var ps = CTree<string, bool>.Empty;
                var pl = BList<DBObject>.Empty;
                for (var b = docValue.First(); b != null; b = b.Next())
                    if (b.value() is QlValue s)
                    {
                        ps += (b.key(), true);
                        pl += s;
                    }
                if (nd.label.name != "" && cx.role.edgeTypes[nd.label.name] is long q)
                {
                    if (cx.db.objects[q] is EdgeType nv && nv.leavingType == lT.defpos && nv.arrivingType == aT.defpos)
                        nt = nv;
                    else if (cx.db.objects[q] is Domain de && de.kind == Qlx.UNION)
                    {
                        for (var c = de.unionOf.First(); nt is null && c != null; c = c.Next())
                            if (cx.db.objects[c.key().defpos] is EdgeType ee
                                && ee.leavingType == lT.defpos && ee.arrivingType == aT.defpos)
                                nt = ee;
                    }
                    else throw new PEException("PE20904");
                }
                else if (nd.label.name == "" && cx.role.unlabelledEdgeTypesInfo[ps] is long p
                    && cx.db.objects[p] is EdgeType un)
                    nt = un;
                if (nd.label is Domain nu && nu.kind == Qlx.UNION && lT is not null && aT is not null)
                {
                    nt = null;
                    for (var nb = nu.unionOf.First(); nt is null && nb != null; nb = nb.Next())
                        if (cx.db.objects[nb.key().defpos] is EdgeType xe
                            && xe.leavingType == lT.defpos && xe.arrivingType == aT.defpos)
                            nt = xe;
                }
                else if (lT is not null && aT is not null)
                {
                    var un = (cx.db.objects[cx.UnlabelledEdgeSuper(lT.defpos, aT.defpos, ps)] is Domain st) ?
                              new CTree<Domain, bool>(st, true) : CTree<Domain, bool>.Empty;
                    var dn = new Domain(Qlx.TYPE, cx, pl);
                    if (nt is null && lT is not null && aT is not null)
                    {
                        nt = new EdgeType(ap, cx.GetUid(), nd.label.name, new UDType(-1L, dn.mem),
                            new BTree<long, object>(Domain.Under, un), cx);
                        be = nt;
                        te = nt;
                        nt += (EdgeType.LeavingType, lT.defpos);
                        nt += (EdgeType.ArrivingType, aT.defpos);
                        cx.Add(nt);
                        var t = cx.Add(new PEdgeType(nd.label.name, nt, un, -1L,
                            lT.defpos, aT.defpos, cx.db.nextPos, cx));
                        for (var b = docValue.First(); b != null; b = b.Next())
                            if (t is EdgeType ut && ut.infos[cx.role.defpos] is ObInfo oi && !oi.names.Contains(b.key()))
                            {
                                var pc = new PColumn3(ut, b.key(), -1, b.value().domain, PColumn.GraphFlags.None,
                                    -1L, -1L, cx.db.nextPos, cx);
                                cx.Add(pc);
                            }
                        nt = (EdgeType?)cx.obs[nt.defpos];
                    }
                }
            }
            if (be is not EdgeType && lT is not null && aT is not null)
            {
                if (nd.label.kind == Qlx.UNION)
                {
                    for (var b = state.First(); b != null; b = b.Next())
                        if (b.value() is TGParam g && cx.role.edgeTypes[g.value] is long gp &&
                            cx._Ob(gp) is Domain du && du.kind == Qlx.UNION)
                        {
                            for (var c = du.unionOf.First(); c != null; c = c.Next())
                                if (c.key() is EdgeType de && de.leavingType == lT.defpos
                                    && de.arrivingType == aT.defpos)
                                {
                                    nt = de;
                                    break;
                                }
                            break;
                        }
                }
                else
                {
                    var ep = new PEdgeType(nd.label.name, Domain.EdgeType, tl, cx.db.nextStmt, lT.defpos, aT.defpos, cx.db.nextPos, cx);
                    be = (EdgeType)(cx.Add(ep) ?? throw new DBException("42105"));
                    nt = be;
                }
            }
            if (be is not null)
            {
                var bt = be.Build(cx, this, 0L, new BTree<long, object>(Domain.NodeTypes, tl), md);
                if (bt is not null)
                {
                    if (bt.defpos == nt?.defpos)
                        nt = (EdgeType)bt;
                    be = (EdgeType)cx.Add(bt);
                }
            }
            if (be is not null && nt?.leaveCol < 0)
            {
                nt = (EdgeType)be.Inherit(nt);
                nt = (EdgeType)cx.Add(nt);
            }
            if (nt is null)
                throw new DBException("42000", "_EdgeType").Add(Qlx.INSERT_STATEMENT, new TChar(name ?? "??"));
            return nt;
        }

        private bool CanConnect(Context cx, long end, long defpos)
        {
            if (end == defpos)
                return true;
            if (cx.db.objects[end] is NodeType et && cx.db.objects[defpos] is NodeType dt
                && dt.EqualOrStrongSubtypeOf(et))
                return true;
            if (cx._Ob(defpos) is JoinedNodeType jt)
                for (var b=jt.nodeTypes.First();b!=null;b=b.Next())
                    if (b.key().defpos==end)
                        return true;
            return false;
        }

        internal override GqlNode Add(Context cx, GqlNode? an, CTree<long, TGParam> tgs, long ap)
        {
            if (an is null)
                throw new DBException("22G0L");
            tgs += state;
            var r = this;
            var oan = an;
            if (an.state[an.defpos] is TGParam lg)
                if (tok == Qlx.ARROWBASE)
                {
                    tgs += (-(long)Qlx.ARROW, lg);
                    r += (ArrivingValue, an.defpos);
                    if (an.domain.defpos < 0 && domain is EdgeType et
                        && cx.db.objects[et.arrivingType] is NodeType at)
                    {
                        cx.Replace(an, an + (_Domain, at));
                        cx.names += (an.NameFor(cx), (ap,an.defpos));
                        cx.defs += (an.defpos, at.names);
                    }
                }
                else
                {
                    tgs += (-(long)Qlx.RARROWBASE, lg);
                    r += (LeavingValue, an.defpos);
                    if (an.domain.defpos < 0 && domain is EdgeType et
                        && cx.db.objects[et.leavingType] is NodeType lt)
                    {
                        cx.Replace(an, an + (_Domain, lt));
                        cx.names += (an.NameFor(cx), (ap,an.defpos));
                        cx.defs += (an.defpos, lt.names);

                    }
                }
            r += (State, tgs);
            return (GqlEdge)cx.Add(r);
        }
        protected override CTree<string, QlValue> _AddEnds(Context cx, CTree<string, QlValue> ls)
        {
            ls = base._AddEnds(cx, ls);
            if (cx.db.objects[domain.defpos] is not EdgeType et)
                return ls;
            if (cx.db.objects[et.leaveCol] is TableColumn lc
                && cx.obs[leavingValue] is GqlNode sl
                && sl.Eval(cx) is TNode ln)
            {
                var lv = new TInt(ln.tableRow.defpos);
                var li = (lc.domain.kind == Qlx.SET) ?
                    new SqlLiteral(cx.GetUid(), new TSet(lc.domain, CTree<TypedValue, bool>.Empty + (lv, true))) :
                    new SqlLiteral(cx.GetUid(), lv);
                if (cx.NameFor(et.leaveCol) is string lN)
                    ls += (lN, (QlValue)cx.Add(li));
            }
            if (cx.db.objects[et.arriveCol] is TableColumn ac
                && cx.obs[arrivingValue] is GqlNode sa
                && sa.Eval(cx) is TNode an)
            {
                var av = new TInt(an.tableRow.defpos);
                var ai = (ac.domain.kind == Qlx.SET) ?
                    new SqlLiteral(cx.GetUid(), new TSet(ac.domain, CTree<TypedValue, bool>.Empty + (av, true))) :
                    new SqlLiteral(cx.GetUid(), av);
                if (cx.NameFor(et.arriveCol) is string aN)
                    ls += (aN, (QlValue)cx.Add(ai));
            }
            return ls;
        }
        internal override NodeType? InsertSchema(Context cx)
        {
            var r = base.InsertSchema(cx) as EdgeType;
            if (label is GqlLabel lb && lb.kind == Qlx.DOUBLEARROW && lb.AllowExtra(cx, mem) is not null)
            {
                var lf = cx.db.objects[lb.left] as EdgeType;
                var nl = cx.NameFor(lb.left) ?? throw new DBException("42105");
                var rg = cx.db.objects[lb.right] as EdgeType;
                var nr = cx.NameFor(lb.right) ?? throw new DBException("42105");
                if (rg is not null && rg.defpos > 0)
                {
                    if (lf is not null && lf.defpos > 0)
                    {
                        // both defined: ensure we have => on properties
                        var li = lf.infos[cx.role.defpos] ?? throw new DBException("42105");
                        var ln = li.names;
                        var lt = lf.rowType;
                        var ls = lf.representation;
                        var cl = false;
                        var ri = rg.infos[cx.role.defpos] ?? throw new DBException("42105");
                        var rn = ri.names;
                        var ns = ln + rn; // care: this will lose data when keys match
                        for (var b = ns.First(); b != null; b = b.Next())
                        {
                            var k = b.key();
                            var lc = cx.db.objects[ln[k].Item2] as TableColumn;
                            var rc = cx.db.objects[rn[k].Item2] as TableColumn;
                            if (lc is not null && rc is not null)
                                cl = cl || cx.MergeColumn(lc.defpos, rc.defpos);
                            else if (rc is not null)
                            {
                                lt += rc.defpos;
                                ls += (rc.defpos, rc.domain);
                                ln += (b.key(), (b.value().Item1,rc.defpos));
                                cl = true;
                            }
                        }
                        var xl = cl ? ((EdgeType)cx.Add(lf + (Domain.RowType, lt) + (Domain.Representation, ls)
                                    + (Infos, lf.infos+(cx.role.defpos,li + (ObInfo._Names, ln))))) : lf;
                        var pe = new EditType(nl, xl, lf, new CTree<Domain, bool>(rg, true), cx.db.nextPos, cx);
                        r = (EdgeType)(cx.Add(pe) ?? throw new DBException("42105"));
                    }
                    else // rg defined, create lf
                    {
                        var li = new ObInfo(nl, Grant.AllPrivileges);
                        var ln = Names.Empty;
                        var lt = CList<long>.Empty;
                        var ls = CTree<long, Domain>.Empty;
                        var ri = rg.infos[cx.role.defpos] ?? throw new DBException("42105");
                        for (var b = ri.names.First(); b != null; b = b.Next())
                        {
                            var k = b.key();
                            var rc = cx.db.objects[b.value().Item2] as TableColumn ?? throw new DBException("42105");
                            lt += rc.defpos;
                            ls += (rc.defpos, rc.domain);
                            ln += (b.key(),(b.value().Item1,rc.defpos));
                        }
                        lf = new EdgeType(0L,cx.GetUid(), nl, rg, BTree<long, object>.Empty, cx);
                        var pe = new PEdgeType(nl, lf, new CTree<Domain, bool>(rg, true), -1L,
                            rg.leavingType, rg.arrivingType, cx.db.nextPos, cx);
                        r = (EdgeType)(cx.Add(pe) ?? throw new DBException("42105"));
                    }
                    for (var b = rg.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value().First(); c != null; c = c.Next())
                            for (var d = lf.indexes[b.key()]?.First(); d != null; d = d.Next())
                                if (cx.db.objects[d.key()] is Index lx
                                && cx.db.objects[c.key()] is Index rx)
                                    for (var e = lx.rows?.impl?.First(); e != null; e = e.Next())
                                        if (e.value().ToLong() is long v)
                                            rx += (new CList<TypedValue>(e.key()), v);    
                }
                else
                {
                    // lf defined, create a new supertype rg with just the special columns.
                    // This means reparenting them.
                    var ri = new ObInfo(nr, Grant.AllPrivileges);
                    var rn = Names.Empty;
                    var rt = CList<long>.Empty;
                    var rs = CTree<long, Domain>.Empty;
                    var li = lf?.infos[cx.role.defpos] ?? throw new DBException("42105");
                    var ln = li.names;
                    var np = cx.db.nextPos;
                    for (var b = li.names.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        var lc = cx.db.objects[b.value().Item2] as TableColumn ?? throw new DBException("42105");
                        if (lc.flags == PColumn.GraphFlags.None)
                            continue;
                        cx.db += lc + (TableColumn._Table, np);
                        rt += lc.defpos;
                        rs += (lc.defpos, lc.domain);
                        rn += (b.key(),(b.value().Item1,lc.defpos));
                    }
                    rg = new EdgeType(0L,np, nr, lf, BTree<long, object>.Empty, cx);
                    var pe = new PEdgeType(nr, rg, CTree<Domain, bool>.Empty, -1L,
                        lf.leavingType, rg.arrivingType, np, cx);
                    r = (EdgeType)(cx.Add(pe) ?? throw new DBException("42105"));
                    r = r + (Domain.RowType, rt) + (Domain.Representation, rs)
                        + (Infos, new BTree<long, ObInfo>(cx.role.defpos, ri + (ObInfo._Names, rn)));
                    cx.db += r;
                    for (var b = lf.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (!rg.indexes.Contains(b.key()) && cx.db.objects[c.key()] is Index x)
                            {
                                var px = new PIndex("", rg, b.key(), x.flags, x.refindexdefpos, cx.db.nextPos);
                                cx.Add(px);
                                if (cx.db.objects[px.ppos] is Index nx)
                                    nx.Build(cx);
                                cx.db += x + (Index.TableDefPos, np);
                            }
                    var pu = new EditType(nl, lf, lf, new CTree<Domain, bool>(r, true), cx.db.nextPos, cx);
                    cx.Add(pu);
                    for (var b = rg.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (!lf.indexes.Contains(b.key()) && cx.db.objects[c.key()] is Index x)
                            {
                                var px = new PIndex("", lf, b.key(), x.flags, x.refindexdefpos, cx.db.nextPos);
                                cx.Add(px);
                                if (cx.db.objects[px.ppos] is Index nx)
                                    nx.Build(cx);
                            }
                }
            }
            return r;
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
            if (tok!=Qlx.NULL)
            {
                sb.Append(' ');sb.Append(tok);
            }
            return sb.ToString();
        }
    }
    class GqlPath : GqlEdge
    {
        internal const long
            MatchQuantifier = -484, // (int,int)
            Pattern = -485, // CList<long>
            StartState = -238; // CTree<long,TGParam>
        internal CList<long> pattern => (CList<long>)(mem[Pattern]??CList<long>.Empty);
        internal (int, int) quantifier => ((int, int))(mem[MatchQuantifier]??(1, 1));
        internal Qlx inclusionMode => (Qlx)(mem[GqlMatchAlt.InclusionMode] ?? Qlx.ANY); // SHORTEST/LONGEST
        public GqlPath(long lp,Context cx, CList<long> p, (int, int) lh, long i, long a)
            : base(lp,_Mem(cx,p)+(Pattern,p)+(MatchQuantifier, lh)
                  +(LeavingValue,i)+(ArrivingValue,a)+(GqlMatchAlt.InclusionMode,cx.inclusionMode))
        { }
        protected GqlPath(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Context cx, CList<long> p)
        {
            var r = BTree<long, object>.Empty;
            var b = p.First();
            if (b?.value() is long ap && cx.obs[ap] is GqlNode a)
                r += (StartState, a.state);
            if (b?.Next()?.value() is long bp && cx.obs[bp] is GqlNode e)
                r += (State, e.state);
            return r;
        }
        public static GqlPath operator +(GqlPath e, (long, object) x)
        {
            return (GqlPath)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (GqlPath)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GqlPath(dp, m);
        }
        internal override int MinLength(Context cx)
        {
            var m = 0;
            for (var b = pattern.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is GqlNode p)
                    m += p.MinLength(cx);
            return quantifier.Item1 * m;
        }
        internal override GqlNode Add(Context cx, GqlNode? an, CTree<long, TGParam> tgs, long ap)
        {
            var r = this;
            tgs += state;
            GqlEdge? last = null;
            for (var b = pattern.Last(); b != null && last is null; b = b.Next())
                last = cx.obs[b.value()] as GqlEdge;
            if (an?.state[an.defpos] is TGParam lg)
                if (tok == Qlx.ARROWBASE)
                {
                    tgs += (-(long)Qlx.ARROW, lg);
                    if (last is not null)
                        cx.Add(last+(ArrivingValue, an.defpos));
                }
                else
                {
                    tgs += (-(long)Qlx.RARROWBASE, lg);
                    if (last is not null)
                        cx.Add(last+(LeavingValue, an.defpos));
                }
            r += (State, tgs);
            return (GqlNode)cx.Add(r);
        }
        public override string ToString()
        {
            var sb= new StringBuilder(base.ToString());
            var cm = "";
            sb.Append('[');
            for (var b=pattern.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(Uid(b.value()));
            }
            sb.Append(']');
            var (l, h) = quantifier;
            sb.Append('{');sb.Append(l);sb.Append(',');sb.Append(h); sb.Append('}');
            return sb.ToString();
        }
    }
    class GqlMatch : QlValue
    {
        internal const long
            MatchAlts = -486; // CList<long> GqlMatchAlt
        internal CList<long> matchAlts => (CList<long>)(mem[MatchAlts] ?? CList<long>.Empty);
        public GqlMatch(Context cx, CList<long>ms) 
            : this(cx.GetUid(),new BTree<long,object>(MatchAlts,ms))
        {
        }
        protected GqlMatch(long dp, BTree<long, object> m) : base(dp, m)
        {
        }
        public static GqlMatch operator +(GqlMatch e, (long, object) x)
        {
            return (GqlMatch)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (GqlMatch)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GqlMatch(dp, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " [";
            for (var b = matchAlts.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            if (cm == ",") sb.Append(']');
            return sb.ToString();
        }
    }
    class GqlMatchAlt : QlValue
    {
        internal const long
            MatchExps = -487, // CList<long> GqlNode
            InclusionMode = -497, // Qlx
            MatchMode = -483, // Qlx
            PathId = -488;  // long
        internal Qlx mode => (Qlx)(mem[MatchMode] ?? Qlx.NONE);
        internal Qlx inclusion => (Qlx)(mem[InclusionMode] ?? Qlx.NONE);
        internal long pathId => (long)(mem[PathId] ?? -1L);
        internal CList<long> matchExps => (CList<long>)(mem[MatchExps] ?? CList<long>.Empty);
        public GqlMatchAlt(long dp,Context cx, Qlx m, Qlx sh, CList<long> p, long pp)
            : base(dp, new BTree<long, object>(MatchMode, m) + (MatchExps, p) + (PathId,pp)
                  + (InclusionMode,sh))
        {
            var min = 0; // minimum path length
            var hasPath = false;
            for (var b = p.First(); b != null; b = b.Next())
            {
                if (cx.obs[b.value()] is GqlNode sn)
                {
                    min += sn.MinLength(cx);
                    if (sn is GqlPath sp)
                    {
                        hasPath = true;
                        if (m == Qlx.ALL && sp.quantifier.Item2 < 0)
                            throw new DBException("22G0M");
                    }
                }
            }
            if (hasPath && min <= 0)
                throw new DBException("22G0N");
        }
        protected GqlMatchAlt(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static GqlMatchAlt operator +(GqlMatchAlt e, (long, object) x)
        {
            return (GqlMatchAlt)e.New(e.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return (GqlMatchAlt)New(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GqlMatchAlt(dp, m);
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (GqlMatchAlt)base._Replace(cx, was, now);
            var ch = false;
            var ls = CList<long>.Empty;
            for (var b = matchExps.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is GqlNode sa)
                {
                    var a = sa.Replace(cx, was, now);
                    if (a != sa)
                        ch = true;
                    ls += a.defpos;
                }
            return ch ? cx.Add(r + (MatchExps, ls)) : r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mode != Qlx.NONE)
            {
                sb.Append(' '); sb.Append(mode);
                if (mem[InclusionMode] is Qlx sh)
                {
                    sb.Append(' '); sb.Append(sh);
                }
            }
            if (pathId>=0)
            {
                sb.Append(' ');sb.Append(Uid(pathId));
            }
            var cm = " [";
            for (var b=matchExps.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            if (cm == ",") sb.Append(']');
            return sb.ToString();
        }
    }
}
