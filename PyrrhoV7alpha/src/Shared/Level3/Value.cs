using System;
using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
            Await = -464, // CTree<long,bool> RowSet
            Left = -308, // long SqlValue
            _Meta = -307, // Metadata
            Right = -309, // long SqlValue
            Sub = -310; // long SqlValue
        internal long left => (long)(mem[Left]??-1L); 
        internal long right => (long)(mem[Right]??-1L);
        internal long sub => (long)(mem[Sub]??-1L);
        internal virtual long target => defpos;
        internal CTree<long, bool> await =>
            (CTree<long, bool>)mem[Await] ?? CTree<long, bool>.Empty;
        public SqlValue(Ident ic) : this(ic.iix, ic.ident) { }
        public SqlValue(Iix iix,string nm="",Domain dt=null,BTree<long,object>m=null)
            :base(iix.dp,(m??BTree<long, object>.Empty)+(IIx,iix)
                +(_Domain,dt?.defpos??Domain.Content.defpos)
                +(Name,nm))
        { }
        protected SqlValue(Context cx,string nm,Domain dt,long cf=-1L)
            :base(cx.GetUid(),_Mem(cf)+(Name,nm)+(_Domain,dt.defpos))
        {
            cx.Add(this);
        }
        protected SqlValue(Iix dp, BTree<long, object> m) : base(dp.dp, m+(IIx,dp))
        { }
        static BTree<long,object> _Mem(long cf)
        {
            var r = BTree<long, object>.Empty;
            if (cf >= 0)
                r += (SqlCopy.CopyFrom, cf);
            return r;
        }
        public static SqlValue operator+(SqlValue s,(long,object)x)
        {
            return (SqlValue)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            var ix = ((Iix)m[IIx]) ?? new Iix(defpos); 
            return new SqlValue(ix,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (left >= 0) r += cx.obs[left]._Rdc(cx);
            if (right >= 0) r += cx.obs[right]._Rdc(cx);
            if (sub >= 0) r += cx.obs[sub]._Rdc(cx);
            return r;
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[left]?.Calls(defpos, cx)==true || cx.obs[right]?.Calls(defpos,cx)==true;
        }
        internal virtual bool KnownBy(Context cx,RowSet q)
        {
            return q.Knows(cx, defpos);
        }
        internal virtual bool KnownBy<V>(Context cx,CTree<long,V> cs) where V : IComparable
        {
            return cs.Contains(defpos);
        }
        internal virtual CTree<long,Domain> KnownFragments(Context cx,CTree<long,Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (left >= 0)
            { 
                r += ((SqlValue)cx.obs[left]).KnownFragments(cx, kb);
                y = y && r.Contains(left);
            }
            if (right >= 0)
            {
                r += ((SqlValue)cx.obs[right]).KnownFragments(cx, kb);
                y = y && r.Contains(right);
            }
            if (sub >= 0)
            {
                r += ((SqlValue)cx.obs[sub]).KnownFragments(cx, kb);
                y = y && r.Contains(sub);
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        internal string Alias(Context cx)
        {
            return alias ?? name ?? cx.Alias();
        }
        internal virtual bool ConstantIn(Context cx,RowSet rs)
        {
            return rs.stem.Contains(rs.finder[defpos].rowSet);
        }
        internal virtual Domain _Dom(Context cx)
        {
            return cx._Dom(this);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx,long r)
        {
            var aw = CTree<long, CTree<long, bool>>.Empty;
            for (var b = IsAggregation(cx).First();b!=null;b=b.Next())
            {
                var k = b.key();
                var f = (SqlFunction)cx.obs[k];
                var a = aw[k] ?? CTree<long, bool>.Empty;
                aw += (k, a + (f.from, true));
            }
            cx.awaits += aw;
            var rs = (RowSet)cx.obs[r];
            if (rs is RestRowSet rrs && rrs.remoteCols.Has(defpos))
                return CTree<long, RowSet.Finder>.Empty;
            if (rs?.finder.Contains(defpos) == true)
                return CTree<long, RowSet.Finder>.Empty;
            return new CTree<long, RowSet.Finder>(defpos,new RowSet.Finder(defpos,from));
        }
        internal override bool LocallyConstant(Context cx,RowSet rs)
        {
            return isConstant(cx) || (rs.needed?.Contains(defpos)??false);
        }
        internal virtual SqlValue Reify(Context cx)
        {
            var dm = cx._Dom(this);
            for (var b = dm.rowType?.First(); b != null; b = b.Next())
            {
                var ci = cx.Inf(b.key());
                if (ci.name == name)
                    return new SqlCopy(iix, cx, name, defpos, ci.defpos);
            }
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
            switch (op)
            {
                case Sqlx.ASSIGNMENT: return ":=";
                case Sqlx.COLON: return ":";
                case Sqlx.EQL: return "=";
                case Sqlx.COMMA: return ",";
                case Sqlx.CONCATENATE: return "||";
                case Sqlx.DIVIDE: return "/";
                case Sqlx.DOT: return ".";
                case Sqlx.DOUBLECOLON: return "::";
                case Sqlx.GEQ: return ">=";
                case Sqlx.GTR: return ">";
                case Sqlx.LBRACK: return "[";
                case Sqlx.LEQ: return "<=";
                case Sqlx.LPAREN: return "(";
                case Sqlx.LSS: return "<";
                case Sqlx.MINUS: return "-";
                case Sqlx.NEQ: return "<>";
                case Sqlx.PLUS: return "+";
                case Sqlx.TIMES: return "*";
                case Sqlx.AND: return " and ";
                default: return op.ToString();
            }
        }
        internal virtual bool Grouped(Context cx,GroupSpecification gs)
        {
            return gs.Has(cx, defpos);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal virtual bool AggedOrGrouped(Context cx,RowSet r)
        {
            var dm = cx._Dom(r);
            for (var b = dm.aggs.First(); b != null; b = b.Next())
            {
                var v = (SqlFunction)cx.obs[b.key()];
                if (v.kind == Sqlx.COUNT && v.mod == Sqlx.TIMES)
                    return true;
                if (v.Operands(cx).Contains(defpos))
                    return true;
            }
            if (r.groupCols is Domain gc)
                if (gc.representation.Contains(defpos))
                    return true;
            return false; 
        }
        internal virtual bool WellDefinedOperands(Context cx)
        {
            if (cx.obs[left] is SqlValue lf && !lf.WellDefinedOperands(cx))
                return false;
            if (cx.obs[right] is SqlValue rg && !rg.WellDefinedOperands(cx))
                return false;
            if (cx.obs[sub] is SqlValue sb && !sb.WellDefinedOperands(cx))
                return false;
            return true;
        }
        internal virtual bool WellDefined()
        {
            return domain != Domain.Content.defpos || from >= 0;
        }
        internal virtual (SqlValue,RowSet) Resolve(Context cx,RowSet q)
        {
            if (domain != Domain.Content.defpos)
                return (this,q);
            if (q is From fm)
            {
                var id = new Ident(name, iix);
                var ic = new Ident(new Ident(q.name, cx.Ix(q.defpos)), id);
                if (cx.obs[cx.defs[ic].dp] is DBObject tg)
                {
                    var cp = (tg is SqlCopy sc) ? sc.copyFrom : tg.defpos;
                    var nc = new SqlCopy(iix, cx, name, q.defpos, cp);
                    if (nc.defpos<Transaction.Executables && nc.defpos < tg.defpos)
                        cx.Replace(tg, nc);
                    else
                        cx.Replace(this, nc);
                    q = (RowSet)cx.obs[q.defpos];
                    return (nc,q);
                }
            }
            if (cx.defs.Contains(name))
            {
                var ob = cx.obs[cx.defs[(name,cx.sD)].Item1.dp];
                if (cx._Dom(q).rowType== CList<long>.Empty)
                    return (this,q);
                if (cx._Dom(ob).kind != Sqlx.CONTENT && ob.defpos != defpos 
                    && ob is SqlValue sb)
                {
                    var nc = (SqlValue)cx.Replace(this,
                        new SqlValue(iix,name,cx._Dom(sb),mem));
                    return (nc,q);
                }
            }
            var rt = cx._Dom(q).rowType;
            var i = PosFor(cx,name);
            if (name != "" && i >= 0)
            {
                var sv = (SqlValue)cx.obs[rt[i]];
                var ns = sv;
                if (sv is SqlCopy sc && alias != null && alias != sc.name)
                    ns = ns +(_Domain,sc.domain);
                else if ((!(sv is SqlCopy)) && sv.domain != Domain.Content.defpos)
                    ns = new SqlCopy(iix, cx, name, q.defpos, sv);
                var nc = (SqlValue)cx.Replace(this, ns);
                q += (i, nc);
                return (nc, q);
            }
            return (this,q);
        }
        internal int PosFor(Context cx, string nm)
        {
            var i = 0;
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next(), i++)
            {
                var p = b.key();
                var ci = cx.Inf(p);
                if (ci.name == nm)
                    return i;
            }
            return -1;
        }
        /// <summary>
        /// Eval is used to deliver the TypedValue for the current Cursor if any
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx)
        {
            // open cursors
            if (cx.finder[defpos] is RowSet.Finder f &&
                        Eval(cx, f) is TypedValue v)
                return v;
            // a local variable etc
            if (cx.values.Contains(defpos))
                return cx.values[defpos];
            // check for lateral case
            if (cx.obs[from] is RowSet r)
                for (var b = r.Sources(cx).First(); b != null; b = b.Next())
                {
                    var sr = (RowSet)cx.obs[b.key()];
                    if (sr.finder[defpos] is RowSet.Finder sf &&
                        Eval(cx, sf) is TypedValue sv)
                        return sv;
                }
            return TNull.Value;
        }
        protected TypedValue Eval(Context cx,RowSet.Finder f)
        {
            if (cx.cursors == BTree<long, Cursor>.Empty)
                return cx.values[defpos];
            return (cx.obs[f.rowSet] is RowSet r)?
                ((cx.cursors[f.rowSet] is Cursor c)?
                    c[f.col] : TNull.Value)
            : null;
        }
        internal override void Set(Context cx, TypedValue v)
        {
            base.Set(cx, v);
            if (cx.obs[from] is RowSet rs 
                && cx.cursors[rs.defpos] is Cursor cu)
                cu += (cx, defpos, v);
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
        internal virtual bool IsFrom(Context cx,RowSet q, bool ordered=false, Domain ut=null)
        {
            return false;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            if (defpos == so.defpos)
                return sv;
            var r = this;
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
            if (r!=this)
                r = (SqlValue)New(cx, r.mem);
            cx.done += (defpos, r);
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
        protected (CTree<long, bool>, CTree<long, string>) GroupOperands(Context cx, CTree<long, bool> dst,
    CTree<long, string> ns, CList<long> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
                for (var c= cx.obs[b.value()].Operands(cx).First();
                    c!=null;c=c.Next())
                {
                    var sv = (SqlValue)cx.obs[c.key()];
                    dst += (sv.defpos, true);
                    ns += (sv.defpos, sv.alias ?? sv.name);
                }
            return (dst, ns);
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
            return new SqlValueExpr(cx.GetIid(), cx, Sqlx.NOT, this, null, Sqlx.NO);
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
        internal virtual RowSet RowSetFor(Iix dp,Context cx,CList<long> us,
            CTree<long,Domain> re)
        {
            if (cx.val is TRow r && !r.IsNull)
                return new TrivialRowSet(dp,cx, r, -1L,
                    ((RowSet)cx.obs[from])?.finder?? CTree<long,RowSet.Finder>.Empty);
            return new EmptyRowSet(dp.dp,cx,domain,us,re);
        }
        internal virtual Domain FindType(Context cx,Domain dt)
        {
            var dm = cx._Dom(this);
            if (dm == null || dm.kind==Sqlx.CONTENT)
                return dt;
            if (!dt.CanTakeValueOf(dm))
                throw new DBException("22005", dt.kind, dm.kind);
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
        public virtual int CompareTo(object obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        internal SqlValue Constrain(Context cx,Domain dt)
        {
            var dm = cx._Dom(this);
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
            if (mem.Contains(_From)) { sb.Append(" From:"); sb.Append(Uid(from)); }
            if (mem.Contains(_Alias)) { sb.Append(" Alias="); sb.Append(alias); }
            if (left!=-1L) { sb.Append(" Left:"); sb.Append(Uid(left)); }
            if (right!=-1L) { sb.Append(" Right:"); sb.Append(Uid(right)); }
            if (sub!=-1L) { sb.Append(" Sub:"); sb.Append(Uid(sub)); }
            if (await!=CTree<long,bool>.Empty)
            {
                sb.Append(" Await (");
                var cm = "";
                for (var b=await.First();b!=null;b=b.Next())
                {
                    sb.Append(cm);cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
            return sb.ToString();
        }
        internal override string ToString(string sg, Remotes rf, 
            CList<long> cs, CTree<long, string> ns, Context cx)
        {
            switch (rf)
            {
                case Remotes.Selects:
                    if (ns.Contains(defpos))
                        return ns[defpos];
                    var an = alias ?? name ?? "";
                    if (!cs.Has(defpos))
                    {
                        var v = Eval(cx);
                        if (v == TNull.Value)
                            return "";
                        var s = v.ToString(cs, cx);
                        if (an != "")
                            s += " as " + an;
                        return s;
                    }
                    break;
                case Remotes.Operands:
                    if (ns.Contains(defpos))
                        return ns[defpos];
                    if (!cs.Has(defpos))
                        return Eval(cx).ToString(cs, cx);
                    break;
            }
            return name;
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlValue(new Iix(iix,dp), mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlValue)base._Relocate(cx);
            r += (_From, cx.Fix(from));
            r += (Left, cx.Fix(left));
            r += (Right, cx.Fix(right));
            r += (Sub, cx.Fix(sub));
            // don't worry about TableRow
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlValue)base._Fix(cx);
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
            var na = cx.Fix(await);
            if (await != na)
                r += (Await, na);
            return cx.Add(r);
        }
        internal SqlValue Sources(Context cx)
        {
            var r = (SqlValue)_Fix(cx);
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
        public long copyFrom => (long)mem[CopyFrom];
        public SqlCopy(Iix dp, Context cx, string nm, long fp, long cp,
            BTree<long, object> m = null)
            : this(dp, cx, nm,fp, cx.obs[cp]??(DBObject)cx.db.objects[cp], m)
        { }
        public SqlCopy(Iix dp, Context cx, string nm, long fp, DBObject cf,
           BTree<long, object> m = null)
            : base(dp, _Mem(dp, cx, fp, m) + (CopyFrom, cf.defpos) +(Name,nm) 
                  + (_Domain,cf.domain))
        { }
        static BTree<long,object> _Mem(Iix dp, Context cx, long fp,BTree<long,object>m)
        {
            m = m ?? BTree<long, object>.Empty;
            if (fp>=0)
                m += (_From, fp);
            return m;
        }
        protected SqlCopy(Iix dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static SqlCopy operator +(SqlCopy s, (long, object) x)
        {
            return (SqlCopy)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            var ix = (Iix)m[IIx] ?? iix;
            return new SqlCopy(ix, m);
        }
        internal override (DBObject, Ident) _Lookup(Iix lp, Context cx, string nm, Ident n)
        {
            if (n == null)
                return (this, null);
            if (cx.Inf(copyFrom) is ObInfo oi && oi.names.Contains(n.ident))
            {
                var c = (TableColumn)cx.obs[oi.names[n.ident]];
                var f = (RowSet)cx.obs[from];
                var ts = (TableRowSet)cx.obs[f.rsTargets[c.tabledefpos]];
                var p = ts.sIMap[copyFrom];
                var ob = new SqlField(n.iix, n.ident, defpos, oi.dataType, p);
                cx.Add(ob);
                return (ob, n.sub);
            }
            return base._Lookup(lp, cx,nm, n);
        }
        internal override bool IsFrom(Context cx, RowSet q, bool ordered = false, Domain ut = null)
        {
            return cx._Dom(q).representation.Contains(defpos);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            if (r.Knows(cx, defpos))
                return true;
            if (r.Knows(cx, copyFrom))
                return true;
            if (cx._Dom(r).representation.Contains(copyFrom)
                && cx.obs[copyFrom] is SqlValue)
                return true;
            return r.finder.Contains(defpos);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            if (kb.Contains(copyFrom))
                return new CTree<long, Domain>(defpos, kb[defpos]);
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
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            return ((SqlValue)cx.obs[copyFrom]).ConstantIn(cx,rs)||base.ConstantIn(cx, rs);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCopy(new Iix(iix,dp), mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlCopy)base._Relocate(cx);
            r += (CopyFrom, cx.Fix(copyFrom));
            return r;
        }
       internal override Basis _Fix(Context cx) 
        {
            var r = (SqlCopy)base._Fix(cx);
            // This exception is a hack: if copyFrom is in this range,
            // it must be a virtual column (eg for RestView).
            // Making a special VirtualColumn class would not make the code any more readable
            if (copyFrom < Transaction.Executables || copyFrom >= Transaction.HeapStart)
            {
                var nc = cx.Fix(copyFrom);
                if (nc != copyFrom)
                    r += (CopyFrom, nc);
            }
            return cx.Add(r);
        }
        internal override TypedValue Eval(Context cx)
        {
            if (cx is CalledActivation ca)
            {
                if (ca.locals.Contains(copyFrom))
                    return cx.values[copyFrom];
            }
            else if (cx.obs[copyFrom] is SqlElement se)
                return cx.values[copyFrom];
            return base.Eval(cx);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.obs[copyFrom].Set(cx,v);
            base.Set(cx, v);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            if (((RowSet)cx.obs[rs])?.finder.Contains(cf) == true)
                return CTree<long, RowSet.Finder>.Empty;
            return (cf<-1L)?CTree<long,RowSet.Finder>.Empty
                : base.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            return (cx.obs[cf]?.LocallyConstant(cx,rs)??false) || base.LocallyConstant(cx, rs);
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
        internal SqlTypeExpr(Iix dp,Context cx,Domain ty)
            : base(dp,BTree<long, object>.Empty + (_Domain, ty.defpos))
        {}
        protected SqlTypeExpr(Iix iix, BTree<long, object> m) : base(iix, m) { }
        public static SqlTypeExpr operator +(SqlTypeExpr s, (long, object) x)
        {
            return (SqlTypeExpr)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeExpr(iix, m);
        }
        /// <summary>
        /// Lookup the type name in the context
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return new TTypeSpec(cx._Dom(this));
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
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlTypeExpr(new Iix(iix,dp),mem);
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
        internal SqlTreatExpr(Iix dp,SqlValue v,Domain ty, Context cx)
            : base(dp,_Mem(dp,cx,ty,v) +(TreatExpr,v.defpos)
                  +(_Depth,v.depth+1))
        { }
        protected SqlTreatExpr(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Iix dp,Context cx,Domain ty,SqlValue v)
        {
            var dv = cx._Dom(v);
            var dm = (ty.kind == Sqlx.ONLY && ty.iri != null) ?
                  cx.Add(ty + (Domain.Iri, ty.iri)) : ty;
            return new BTree<long, object>(_Domain, ty.defpos);
        }
        public static SqlTreatExpr operator +(SqlTreatExpr s, (long, object) x)
        {
            return (SqlTreatExpr)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTreatExpr(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTreatExpr(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlTreatExpr)base._Relocate(cx);
            r += (TreatExpr, cx.Fix(val));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlTreatExpr)base._Fix(cx);
            var ne = cx.Fix(val);
            if (ne!=val)
            r += (TreatExpr, ne);
            return cx.Add(r);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlTreatExpr)base.AddFrom(cx, q);
            var o = (SqlValue)cx.obs[val];
            var a = o.AddFrom(cx, q);
            if (a.defpos != val)
                r += (TreatExpr, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlTreatExpr)base._Replace(cx,so,sv);
            var v = cx.ObReplace(r.val,so,sv);
            if (v != r.val)
                r += (TreatExpr, v);
            if (r!=this)
                r = (SqlTreatExpr)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            return ((SqlValue)cx.obs[val]).IsAggregation(cx);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, q);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, cs);
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[val].Eval(cx) is TypedValue tv)
            {
                if (!cx._Dom(this).HasValue(cx,tv))
                    throw new DBException("2200G", cx._Ob(domain).ToString(), cx._Ob(val).ToString()).ISO();
                return tv;
            }
            return null;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx,r,qn);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
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
            Cases = -216,       // BList<(long,long)> SqlValue SqlValue 
            CaseElse = -228;    // long SqlValue
        public long val => (long)(mem[SqlFunction._Val] ?? -1L);
        public BList<(long, long)> cases =>
            (BList<(long,long)>)mem[Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[CaseElse] ?? -1L);
        internal SqlCaseSimple(Iix dp, Domain dm, SqlValue vl, BList<(long, long)> cs, long el)
            : base(dp, BTree<long, object>.Empty + (_Domain, dm.defpos)
                  + (SqlFunction._Val, vl.defpos) + (Cases, cs) + (CaseElse, el))
        { }
        protected SqlCaseSimple(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCaseSimple operator+(SqlCaseSimple s,(long,object)x)
        {
            return (SqlCaseSimple)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCaseSimple(iix,m);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                r += ((SqlValue)cx.obs[w]).Needs(cx);
                r += ((SqlValue)cx.obs[x]).Needs(cx);
            }
            r += ((SqlValue)cx.obs[caseElse]).Needs(cx);
            return r;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx,long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                r += ((SqlValue)cx.obs[w]).Needs(cx,rs);
                r += ((SqlValue)cx.obs[x]).Needs(cx,rs);
            }
            r += ((SqlValue)cx.obs[caseElse]).Needs(cx,rs);
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
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.key()]).KnownBy(cx, q))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.key()]).KnownBy(cx, cs))
                    return false;
            return true;
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this);
            var v = cx.obs[val].Eval(cx);
            for (var b = cases.First();b!=null;b=b.Next())
            {
                var (w, r) = b.value();
                if (dm.Compare(v, cx.obs[w].Eval(cx)) == 0)
                    return cx.obs[r].Eval(cx);
            }
            return cx.obs[caseElse].Eval(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCaseSimple(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlCaseSimple)base._Relocate(cx);
            r += (SqlFunction._Val, cx.Fix(val));
            r += (Cases, cx.Fix(cases));
            r += (CaseElse, cx.Fix(caseElse));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlCaseSimple)base._Fix(cx);
            var nv = cx.Fix(val);
            if (nv != val)
                r += (SqlFunction._Val, nv);
            var nc = cx.Fix(cases);
            if (nc != cases)
                r += (Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r += (CaseElse, ne);
            return cx.Add(r);
        }
        internal override (SqlValue, RowSet) Resolve(Context cx, RowSet fm)
        {
            SqlValue vl = null, r = this;
            if (cx.obs[val] is SqlValue ol)
                (vl, fm) = ol.Resolve(cx, fm);
            if (vl.defpos != val)
                r = (SqlValue)cx.Replace(this, new SqlCaseSimple(iix, cx._Dom(this), vl, cases, caseElse));
            return (r,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCaseSimple)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(val, so, sv);
            if (vl != r.val)
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
            cx.done += (defpos, r);
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
                sb.Append(Uid(w));sb.Append(":");
                sb.Append(Uid(r));
            }
            sb.Append("}");sb.Append(Uid(caseElse));
            return sb.ToString();
        }
    }
    internal class SqlCaseSearch : SqlValue
    {
        public BList<(long, long)> cases =>
            (BList<(long, long)>)mem[SqlCaseSimple.Cases] ?? BList<(long, long)>.Empty;
        public long caseElse => (long)(mem[SqlCaseSimple.CaseElse] ?? -1L);
        internal SqlCaseSearch(Iix dp, Domain dm, BList<(long, long)> cs, long el)
            : base(dp, BTree<long, object>.Empty + (_Domain, dm.defpos)
                  + (SqlCaseSimple.Cases, cs) + (SqlCaseSimple.CaseElse, el))
        { }
        protected SqlCaseSearch(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCaseSearch operator +(SqlCaseSearch s, (long, object) x)
        {
            return (SqlCaseSearch)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCaseSearch(iix, m);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b=cases.First();b!=null;b=b.Next())
            {
                var (w, x) = b.value();
                r += ((SqlValue)cx.obs[w]).Needs(cx);
                r += ((SqlValue)cx.obs[x]).Needs(cx);
            }
            r += ((SqlValue)cx.obs[caseElse]).Needs(cx);
            return r;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, x) = b.value();
                r += ((SqlValue)cx.obs[w]).Needs(cx, rs);
                r += ((SqlValue)cx.obs[x]).Needs(cx, rs);
            }
            r += ((SqlValue)cx.obs[caseElse]).Needs(cx, rs);
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
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.key()]).KnownBy(cx, q))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs)
        {
            for (var b = Needs(cx).First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.key()]).KnownBy(cx, cs))
                    return false;
            return true;
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this);
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, r) = b.value();
                if (cx.obs[w].Eval(cx)==TBool.True)
                    return cx.obs[r].Eval(cx);
            }
            return cx.obs[caseElse].Eval(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCaseSearch(new Iix(iix, dp), mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlCaseSearch)base._Relocate(cx);
            r += (SqlCaseSimple.Cases, cx.Fix(cases));
            r += (SqlCaseSimple.CaseElse, cx.Fix(caseElse));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlCaseSimple)base._Fix(cx);
            var nc = cx.Fix(cases);
            if (nc != cases)
                r += (SqlCaseSimple.Cases, nc);
            var ne = cx.Fix(caseElse);
            if (ne != caseElse)
                r += (SqlCaseSimple.CaseElse, ne);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
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
                r += (SqlCaseSimple.Cases, nc);
            var ne = cx.ObReplace(caseElse, so, sv);
            if (ne != caseElse)
                r += (SqlCaseSimple.CaseElse, ne);
            cx.done += (defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "{";
            for (var b = cases.First(); b != null; b = b.Next())
            {
                var (w, r) = b.value();
                sb.Append(cm); cm = ",";
                sb.Append(Uid(w)); sb.Append(":");
                sb.Append(Uid(r));
            }
            sb.Append("}"); sb.Append(Uid(caseElse));
            return sb.ToString();
        }
    }
    internal class SqlField : SqlValue
    {
        internal const long
            Field = -318, // long SqlValue
            Parent = -344; // long SqlValue
        public long field => (long)(mem[Field] ?? -1L);
        public long parent => (long)(mem[Parent] ?? -1L);
        internal SqlField(Iix dp, string nm, long s, Domain dm, long c)
            : base(dp, BTree<long, object>.Empty + (Name, nm) 
                  + (Parent, s) + (ObInfo._DataType, dm)+(Field,c)
                  + (_Domain,dm.defpos))
        { }
        protected SqlField(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlField operator +(SqlField s, (long, object) x)
        {
            return (SqlField)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlField(iix, m);
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
            return cx.obs[parent].Eval(cx)[field]??TNull.Value;
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
        internal SqlElement(Ident nm,Context cx,Ident pn,Domain dt) 
            : base(nm.iix,nm.ident,dt,BTree<long,object>.Empty+(_From,pn.iix.dp))
        { }
        protected SqlElement(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlElement operator +(SqlElement s, (long, object) x)
        {
            return (SqlElement)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlElement(iix,m);
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
        internal override string ToString(string sg,Remotes rf,CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder(cx.obs[from].ToString(sg,rf,cs,ns,cx));
            sb.Append("["); sb.Append(name); sb.Append("]"); 
            return sb.ToString();
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlElement(new Iix(iix,dp),mem);
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
        public Sqlx kind => (Sqlx)mem[Domain.Kind];
        /// <summary>
        /// the modifier (e.g. DISTINCT)
        /// </summary>
        public Sqlx mod => (Sqlx)mem[Modifier];
        /// <summary>
        /// constructor for an SqlValueExpr
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">an operator</param>
        /// <param name="lf">the left operand</param>
        /// <param name="rg">the right operand</param>
        /// <param name="m">a modifier (e.g. DISTINCT)</param>
        public SqlValueExpr(Iix dp, Context cx, Sqlx op, SqlValue lf, SqlValue rg, 
            Sqlx m, BTree<long, object> mm = null)
            : base(dp, _Type(dp, cx, op, m, lf, rg, mm)
                  + (Modifier, m) + (Domain.Kind, op) 
                  +(Dependents,new CTree<long,bool>(lf?.defpos??-1L,true)+(rg?.defpos??-1L,true))
                  +(_Depth,1+_Max((lf?.depth??0),(rg?.depth??0))))
        { }
        protected SqlValueExpr(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueExpr operator +(SqlValueExpr s, (long, object) x)
        {
            return new SqlValueExpr(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(iix, m);
        }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            var lf = (SqlValue)cx.obs[left];
            var rg = (SqlValue)cx.obs[right];
            return lf?.ConstantIn(cx, rs)!=false||rg?.ConstantIn(cx,rs)!=false;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueExpr(new Iix(iix,dp),mem);
        }
        internal override (SqlValue,RowSet) Resolve(Context cx, RowSet fm)
        {
            SqlValue lf=null, r = this;
            if (cx.obs[left] is SqlValue ol)
                (lf,fm) = ol.Resolve(cx, fm);
            var rt = cx._Dom(fm)?.rowType;
            var rg = (SqlValue)cx.obs[right];
            (rg,fm) = rg?.Resolve(cx, fm)??(rg,fm);
            if (lf?.defpos != left || rg?.defpos != right)
                r = (SqlValue)cx.Replace(this,
                    new SqlValueExpr(iix, cx, kind, lf, rg, mod, mem));
            return (r,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueExpr)base._Replace(cx, so, sv);
            var lf = cx.ObReplace(r.left, so, sv);
            if (lf != r.left)
                r += (Left, lf);
            var rg = cx.ObReplace(r.right, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            var dr = cx._Dom(r);
            if ((dr.kind==Sqlx.UNION || dr.kind==Sqlx.CONTENT) && so.domain != sv.domain)
            {
                dr = cx._Dom((long)(_Type(iix, cx, kind, mod, (SqlValue)cx.obs[lf],
                    (SqlValue)cx.obs[rg])[_Domain] ?? -1L));
                cx.Add(dr);
                r += (_Domain, dr.defpos);
            }
            r = (SqlValueExpr)New(cx, r.mem);
            cx.done += (defpos, r);
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
            var le = (SqlValue)c.obs[left];
            var nl = le?.Having(c, dm);
            var rg = (SqlValue)c.obs[right];
            var nr = rg?.Having(c, dm);
            var su = (SqlValue)c.obs[sub];
            var nu = su?.Having(c, dm);
            return (le == nl && rg == nr && su == nu) ? this
                : (SqlValue)c.Add(new SqlValueExpr(c.GetIid(), c, kind, nl, nr, mod) + (Sub,nu));
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlValueExpr ve)
            {
                if (kind != ve.kind || mod != ve.mod)
                    return false;
                var le = (SqlValue)c.obs[left];
                var nl = (SqlValue)c.obs[ve.left];
                var rg = (SqlValue)c.obs[right];
                var nr = (SqlValue)c.obs[ve.right];
                var su = (SqlValue)c.obs[sub];
                var nu = (SqlValue)c.obs[v.sub];
                return le?.Match(c, nl) != false && rg?.Match(c, nr) != false
                    && su?.Match(c, nu) != false;
            }
            return false;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, q) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, q) != false);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs)
        {
            if (cs.Contains(defpos))
                return true;
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, cs) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, cs) != false);
        }
        internal override CTree<long, bool> Disjoin(Context cx)
        { // parsing guarantees right associativity
            return (kind == Sqlx.AND)? 
                ((SqlValue)cx.obs[right]).Disjoin(cx)+(left, true)
                :base.Disjoin(cx);
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return (((SqlValue)cx.obs[left])?.Grouped(cx, gs)!=false) && 
            (((SqlValue)cx.obs[right])?.Grouped(cx, gs) !=false);
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
            var lf = cx.obs[left] as SqlValue;
            var rg = (SqlValue)cx.obs[right];
            if (lf == null || kind==Sqlx.DOT)
                return rg.FindType(cx,dt);
            Domain tl = lf.FindType(cx,dt);
            Domain tr = (rg == null) ? dt : rg.FindType(cx,dt);
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
                    return cx._Dom(tl.elType);
                case Sqlx.UNION:
                    return tl;
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
                    return cx.obs[left].SysFilter(cx, cx.obs[right].SysFilter(cx, sf));
                case Sqlx.EQL:
                case Sqlx.GTR:
                case Sqlx.LSS:
                case Sqlx.LEQ:
                case Sqlx.GEQ:
                    {
                        var lf = (SqlValue)cx.obs[left];
                        var rg = (SqlValue)cx.obs[right];
                        if (lf.isConstant(cx) && rg is SqlCopy sc)
                            return SystemFilter.Add(sf,sc.copyFrom, Neg(kind), lf.Eval(cx));
                        if (rg.isConstant(cx) && lf is SqlCopy sl)
                            return SystemFilter.Add(sf,sl.copyFrom, kind, rg.Eval(cx));
                        break;
                    }
                default:
                    return sf;
            }
            return base.SysFilter(cx, sf);
        }
        Sqlx Neg(Sqlx o)
        {
            switch (o)
            {
                case Sqlx.GTR: return Sqlx.LSS;
                case Sqlx.GEQ: return Sqlx.LEQ;
                case Sqlx.LEQ: return Sqlx.GEQ;
                case Sqlx.LSS: return Sqlx.GTR;
            }
            return o;
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
            var rw = (TRow)lf.Eval(cx);
            lf.Set(cx, rw += (right, v));
        }
        /// <summary>
        /// Evaluate the expression (binary operators).
        /// May return null if operands not yet ready
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            TypedValue v = null;
            var lf = cx.obs[left] as SqlValue;
            var rg = cx.obs[right] as SqlValue;
            var dm = cx._Dom(this);
            try
            {
                switch (kind)
                {
                    case Sqlx.AND:
                        {
                            var a = lf.Eval(cx);
                            var b = rg.Eval(cx);
                            if (a == null || b == null)
                                return null;
                            if (mod == Sqlx.BINARY) // JavaScript
                                v = new TInt(a.ToLong() & b.ToLong());
                            else
                                v = (a.IsNull || b.IsNull) ?
                                    dm.defaultValue :
                                    TBool.For(((TBool)a).value.Value && ((TBool)b).value.Value);
                            return v;
                        }
                    case Sqlx.ASC: // JavaScript ++
                        {
                            v = lf.Eval(cx);
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return dm.defaultValue;
                            var w = v.dataType.Eval(defpos,cx,v, Sqlx.ADD, new TInt(1L));
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.ASSIGNMENT:
                        {
                            var a = lf;
                            var b = rg.Eval(cx);
                            if (b == null)
                                return null;
                            if (a == null)
                                return b;
                            return v;
                        }
                    case Sqlx.COLLATE:
                        {
                            var a = lf.Eval(cx);
                            object o = a?.Val();
                            if (o == null)
                                return null;
                            Domain ct = cx._Dom(lf);
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = rg.Eval(cx);
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return lf.Eval(cx);
                                Domain nt = (Domain)cx.Add(new Domain(defpos,ct.kind, BTree<long, object>.Empty
                                    + (Domain.Precision, ct.prec) + (Domain.Charset, ct.charSet)
                                    + (Domain.Culture, new CultureInfo(cname))));
                                return new TChar(nt, (string)o);
                            }
                            throw new DBException("2H000", "Collate on non-string?").ISO();
                        }
                    case Sqlx.COMMA: // JavaScript
                        {
                            if (lf.Eval(cx) == null)// for side effects
                                return null;
                            return rg.Eval(cx);
                        }
                    case Sqlx.CONCATENATE:
                        {
                            var ld = cx._Dom(lf);
                            var rd = cx._Dom(rg);
                            if (ld.kind == Sqlx.ARRAY
                                && rd.kind == Sqlx.ARRAY)
                                return ld.Concatenate((TArray)lf.Eval(cx),
                                    (TArray)rg.Eval(cx));
                            var lv = lf.Eval(cx);
                            var or = rg.Eval(cx);
                            if (lf == null || or == null)
                                return null;
                            var stl = lv.ToString();
                            var str = or.ToString();
                            return new TChar(or.dataType, (lv.IsNull && or.IsNull) ? null 
                                : stl + str);
                        }
                    case Sqlx.CONTAINS:
                        {
                            var ta = lf.Eval(cx);
                            if (ta == null)
                                return null;
                            var a = ta.Val() as Period;
                            if (a == null)
                                return dm.defaultValue;
                            var rd = cx._Dom(rg);
                            if (rd.kind == Sqlx.PERIOD)
                            {
                                var tb = rg.Eval(cx);
                                if (tb == null)
                                    return null;
                                var b = tb.Val() as Period;
                                if (b == null)
                                    return TBool.Null;
                                return TBool.For(a.start.CompareTo(b.start) <= 0
                                    && a.end.CompareTo(b.end) >= 0);
                            }
                            var c = rg.Eval(cx);
                            if (c == null)
                                return null;
                            if (c.IsNull)
                                return TBool.Null;
                            return TBool.For(a.start.CompareTo(c) <= 0 && a.end.CompareTo(c) >= 0);
                        }
                    case Sqlx.DESC: // JavaScript --
                        {
                            v = lf.Eval(cx);
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return dm.defaultValue;
                            var w = v.dataType.Eval(defpos,cx,v, Sqlx.MINUS, new TInt(1L));
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = dm.Eval(defpos,cx,lf.Eval(cx), kind,
                            rg.Eval(cx));
                        return v;
                    case Sqlx.DOT:
                        v = cx.obs[left].Eval(cx); // might not be an SqlValue
                        if (rg is SqlField sf)
                            return v[sf.field];
                        if (rg is SqlCopy sc)
                            return v[sc.defpos]??v[sc.copyFrom];
                        if (v != null)
                            v = ((TRow)v)[rg.Eval(cx)?.ToLong()??-1L];
                        return v;
                    case Sqlx.EQL:
                        {
                            var rv = rg.Eval(cx);
                            if (rv == null)
                                return null;
                            return TBool.For(rv != null
                                && rv.CompareTo(lf.Eval(cx)) == 0);
                        }
                    case Sqlx.EQUALS:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return TBool.Null;
                            return TBool.For(a.start.CompareTo(b.start) == 0
                                && b.end.CompareTo(a.end) == 0);
                        }
                    case Sqlx.EXCEPT:
                        {
                            var ta = lf.Eval(cx) as TMultiset;
                            var tb = rg.Eval(cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return dm.Coerce(cx,TMultiset.Except(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.GEQ:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) >= 0);
                        }
                    case Sqlx.GTR:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) > 0);
                        }
                    case Sqlx.INTERSECT:
                        {
                            var ta = lf.Eval(cx) as TMultiset;
                            var tb = rg.Eval(cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return dm.Coerce(cx,TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = lf.Eval(cx);
                            var ar = rg.Eval(cx);
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return dm.defaultValue;
                            return ((TArray)al)[sr.Value];
                        }
                    case Sqlx.LEQ:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) <= 0);
                        }
                    case Sqlx.LOWER: // JavScript >> and >>>
                        {
                            long a;
                            var ol = lf.Eval(cx);
                            var or = rg.Eval(cx);
                            if (ol == null || or == null)
                                return null;
                            if (or.IsNull)
                                return dm.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (mod == Sqlx.GTR)
                                unchecked
                                {
                                    a = (long)(((ulong)ol.Val()) >> s);
                                }
                            else
                            {
                                if (ol.IsNull)
                                    return dm.defaultValue;
                                a = ol.ToLong().Value >> s;
                            }
                            v = new TInt(a);
                            return v;
                        }
                    case Sqlx.LSS:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) < 0);
                        }
                    case Sqlx.MINUS:
                        {
                            var tb = rg.Eval(cx);
                            if (tb == null)
                                return null;
                            if (lf == null)
                            {
                                v = dm.Eval(defpos,cx,new TInt(0), Sqlx.MINUS, tb);
                                return v;
                            }
                            var ta = lf.Eval(cx);
                            if (ta == null)
                                return null;
                            v = dm.Eval(defpos,cx,ta, kind, tb);
                            return v;
                        }
                    case Sqlx.NEQ:
                        {
                            var rv = rg.Eval(cx);
                            return TBool.For(lf.Eval(cx).CompareTo(rv) != 0);
                        }
                    case Sqlx.NO: return lf.Eval(cx);
                    case Sqlx.NOT:
                        {
                            var ta = lf.Eval(cx);
                            if (ta == null)
                                return null;
                            if (mod == Sqlx.BINARY)
                                return new TInt(~ta.ToLong());
                            var bv = ta as TBool;
                            if (bv.IsNull)
                                throw new DBException("22004").ISO();
                            return TBool.For(!bv.value.Value);
                        }
                    case Sqlx.OR:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            switch (mod)
                            {
                                case Sqlx.BINARY: v = new TInt(ta.ToLong() | tb.ToLong()); break;
                                case Sqlx.EXCEPT: v = new TInt(ta.ToLong() ^ tb.ToLong()); break;
                                default:
                                    {
                                        if (ta.IsNull || tb.IsNull)
                                            return dm.defaultValue;
                                        var a = ta as TBool;
                                        var b = tb as TBool;
                                        v = TBool.For(a.value.Value || b.value.Value);
                                    }
                                    break;
                            }
                            return v;
                        }
                    case Sqlx.OVERLAPS:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return dm.defaultValue;
                            return TBool.For(a.end.CompareTo(b.start) >= 0
                                && b.end.CompareTo(a.start) >= 0);
                        }
                    case Sqlx.PERIOD:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            return new TPeriod(Domain.Period, new Period(ta, tb));
                        }
                    case Sqlx.PLUS:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            return dm.Eval(defpos,cx,ta, kind, tb);
                        }
                    case Sqlx.PRECEDES:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return dm.defaultValue;
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(a.end.CompareTo(b.start) == 0);
                            return TBool.For(a.end.CompareTo(b.start) <= 0);
                        }
                    case Sqlx.QMARK: // v7 API for Prepare
                        {
                            return cx.values[defpos];
                        }
                    case Sqlx.RBRACK:
                        {
                            var a = lf.Eval(cx);
                            var b = rg.Eval(cx);
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull || b.IsNull)
                                return dm.defaultValue;
                            return ((TArray)a)[b.ToInt().Value];
                        }
                    case Sqlx.SUCCEEDS:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return dm.defaultValue;
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(a.start.CompareTo(b.end) == 0);
                            return TBool.For(a.start.CompareTo(b.end) >= 0);
                        }
                    case Sqlx.TIMES:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            v = dm.Eval(defpos,cx,ta, kind, tb);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var ld = cx._Dom(lf);
                            return ld.Coerce(cx,
                                TMultiset.Union((TMultiset)ta, (TMultiset)tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.UPPER: // JavaScript <<
                        {
                            var lv = lf.Eval(cx);
                            var or = rg.Eval(cx);
                            if (lf == null || or == null)
                                return null;
                            long a;
                            if (or.IsNull)
                                return dm.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (lv.IsNull)
                                return dm.defaultValue;
                            a = lv.ToLong().Value >> s;
                            v = new TInt(a);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.domain, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = lf.Eval(cx);
                            var tb = rg.Eval(cx);
                            if (ta == null || tb == null)
                                return null;
                            var ld = cx._Dom(lf);
                            return new TChar(ld, ta.ToString() 
                                + " " + tb.ToString());
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
                throw new DBException("22000",kind).ISO();
            }
        }
        static BTree<long,object> _Type(Iix dp, Context cx, Sqlx kind, Sqlx mod, 
            SqlValue left, SqlValue right, BTree<long,object>mm = null)
        {
            mm = mm ?? BTree<long, object>.Empty;
            var ag = CTree<long,bool>.Empty;
            var aw = (CTree<long, bool>)mm[Await] ?? CTree<long, bool>.Empty;
            if (left != null)
            {
                mm += (Left, left.defpos);
                ag += left.IsAggregation(cx);
                aw += left.await;
            }
            if (right != null)
            {
                mm += (Right, right.defpos);
                ag += right.IsAggregation(cx);
                aw += right.await;
            }
            var cs = CList<long>.Empty;
            var dm = Domain.Content;
            var nm = (string)mm?[Name]??""; 
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: dm = cx._Dom(right); 
                    cs = cx._Dom(left).rowType;  
                    nm = left.name; break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON: dm = cx._Dom(left); 
                    nm = left.name; 
                    cs = cx._Dom(right).rowType;  break;// JavaScript
                case Sqlx.CONCATENATE: dm = Domain.Char; break;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        var dl = cx._Dom(left);
                        var dr = cx._Dom(right);
                        if (dl.kind == Sqlx.REAL || dl.kind == Sqlx.NUMERIC)
                            dm = dl;
                        else if (dr.kind == Sqlx.REAL || dr.kind == Sqlx.NUMERIC)
                            dm = dr;
                        else if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.INTEGER
                            || dr.kind == Sqlx.NUMERIC))
                            dm = dl;
                        else
                            dm = left.FindType(cx, Domain.UnionNumeric); break;
                    }
                case Sqlx.DOT: dm = cx._Dom(right); 
                    if (left!=null && left.name!="" && right.name!="")
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
                        var dl = cx._Dom(left).kind;
                        var dr = cx._Dom(right).kind;
                        if (dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME)
                        {
                            if (dr == dl)
                                dm = Domain.Interval;
                            else if (dr == Sqlx.INTERVAL)
                                dm = cx._Dom(left);
                        }
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            dm = cx._Dom(right);
                        else if (dl == Sqlx.REAL || dl == Sqlx.NUMERIC)
                            dm = cx._Dom(left);
                        else if (dr == Sqlx.REAL || dr == Sqlx.NUMERIC)
                            dm = cx._Dom(right);
                        else
                            dm = left.FindType(cx,Domain.UnionDateNumeric);
                        break;
                    }
                    dm = right.FindType(cx,Domain.UnionDateNumeric); break;
                case Sqlx.NEQ: dm = Domain.Bool; break;
                case Sqlx.LEQ: dm = Domain.Bool; break;
                case Sqlx.GEQ: dm = Domain.Bool; break;
                case Sqlx.NO: dm = cx._Dom(left); break;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        var dl = cx._Dom(left)?.kind;
                        var dr = cx._Dom(right)?.kind;
                        if ((dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME) && dr == Sqlx.INTERVAL)
                            dm = cx._Dom(left);
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            dm = cx._Dom(right);
                        else if (dl == Sqlx.REAL || dl == Sqlx.NUMERIC)
                            dm = cx._Dom(left);
                        else if (dr == Sqlx.REAL || dr == Sqlx.NUMERIC)
                            dm = cx._Dom(right);
                        else 
                            dm = left.FindType(cx, Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                        dm = Domain.Content; break;
                case Sqlx.RBRACK:
                        dm= (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.ARRAY, cx._Dom(left))); break;
                case Sqlx.SET: dm = cx._Dom(left); cs = cx._Dom(left).rowType;  nm = left.name; break; // JavaScript
                case Sqlx.TIMES:
                    {
                        var dl = cx._Dom(left).kind;
                        var dr = cx._Dom(right).kind;
                        if (dl == Sqlx.NUMERIC || dr == Sqlx.NUMERIC)
                            dm = Domain.Numeric;
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            dm = cx._Dom(left);
                        else if (dr == Sqlx.INTERVAL && (dl == Sqlx.INTEGER || dl == Sqlx.NUMERIC))
                            dm = cx._Dom(right);
                        else
                            dm = left.FindType(cx,Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.UNION: dm = cx._Dom(left); nm = left.name; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            if (ag !=CTree<long,bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                mm += (Domain.Aggs, ag);
            }
            if (aw != CTree<long, bool>.Empty)
                mm += (Await, aw);
            return mm + (_Domain, dm.defpos) + (Name, nm);
        }
        internal override SqlValue Invert(Context cx)
        {
            var lv = (SqlValue)cx.obs[left];
            var rv = (SqlValue)cx.obs[right];
            switch (kind)
            {
                case Sqlx.AND:
                    return new SqlValueExpr(iix, cx, Sqlx.OR, lv.Invert(cx),
                        rv.Invert(cx), Sqlx.NULL);
                case Sqlx.OR:
                    return new SqlValueExpr(iix, cx, Sqlx.AND, lv.Invert(cx),
                        rv.Invert(cx), Sqlx.NULL);
                case Sqlx.NOT: return lv;
                case Sqlx.EQL: return new SqlValueExpr(iix, cx, Sqlx.NEQ, lv, rv, Sqlx.NULL);
                case Sqlx.GTR: return new SqlValueExpr(iix, cx, Sqlx.LEQ, lv, rv, Sqlx.NULL);
                case Sqlx.LSS: return new SqlValueExpr(iix, cx, Sqlx.GEQ, lv, rv, Sqlx.NULL);
                case Sqlx.NEQ: return new SqlValueExpr(iix, cx, Sqlx.EQL, lv, rv, Sqlx.NULL);
                case Sqlx.GEQ: return new SqlValueExpr(iix, cx, Sqlx.LSS, lv, rv, Sqlx.NULL);
                case Sqlx.LEQ: return new SqlValueExpr(iix, cx, Sqlx.GTR, lv, rv, Sqlx.NULL);
            }
            return base.Invert(cx);
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the SqlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Context cx,SqlValue v,RowSet r)
        {
            if (base._MatchExpr(cx,v,r)) return true;
            var e = v as SqlValueExpr;
            var lv = cx.obs[left] as SqlValue;
            var dm = cx._Dom(domain);
            if (e == null || (dm != null && dm.CompareTo(cx._Dom(v))==0))
                return false;
            if (lv != null && !lv._MatchExpr(cx, cx.obs[e.left] as SqlValue,r))
                return false;
            if (cx.obs[e.left] != null)
                return false;
            if (cx.obs[right] is SqlValue rv && !rv._MatchExpr(cx, cx.obs[e.right] as SqlValue,r))
                return false;
            if (cx.obs[e.right] != null)
                return false;
            return true;
        }
        internal override CTree<long, TypedValue> Add(Context cx, CTree<long, TypedValue> ma,
            Table tb = null)
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
                r = ((SqlValue)cx.obs[right])?.Needs(cx,rs,r) ?? r;
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long,RowSet.Finder>.Empty;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx, rs);
            if (kind!=Sqlx.DOT && cx.obs[right] is SqlValue sw)
                r += sw.Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return (cx.obs[left]?.LocallyConstant(cx, rs) ?? true)
                && (cx.obs[right]?.LocallyConstant(cx, rs) ?? true);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(Uid(defpos)); sb.Append("(");
            if (left!=-1L)
                sb.Append(Uid(left));
            sb.Append(For(kind));
            if (right != -1L)
                sb.Append(Uid(right));
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (kind == Sqlx.LPAREN)
                sb.Append(")");
            sb.Append(")");
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
            if (left >= 0 && right >= 0 && kind != Sqlx.LBRACK && kind != Sqlx.LPAREN)
            {
                sb.Append("(");
                lp = true;
            }
            if (left >= 0)
            {
                var lf = cx.obs[left].ToString(sg, Remotes.Operands, cs, ns, cx);
                if (lf.Length < 1)
                    return "";
                sb.Append(lf);
            }
            sb.Append(For(kind));
            if (right >= 0)
            {
                var rg = cx.obs[right].ToString(sg, Remotes.Operands, cs, ns, cx);
                if (rg.Length < 1)
                    return "";
                sb.Append(rg);
            }
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (lp || kind == Sqlx.LPAREN)
                sb.Append(")");
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
        /// <summary>
        /// constructor for a null expression
        /// </summary>
        internal SqlNull(Iix dp)
            : base(dp,new BTree<long,object>(_Domain, Domain.Null.defpos))
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
        internal override Basis _Relocate(Context cx)
        {
            return this;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
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
    // shareable as of 26 April 2021
    internal class SqlSecurity : SqlValue
    {
        internal SqlSecurity(Iix dp) : base(dp, "SECURITY", Domain._Level) { }
        protected SqlSecurity(Iix dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSecurity(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlSecurity(new Iix(iix,dp),mem);
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
        protected SqlFormal(Iix dp,BTree<long,object>m):base(dp,m){ }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFormal(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlFormal(new Iix(iix,dp),mem);
        }
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            return CTree<long,bool>.Empty;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
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
        internal TypedValue val=>(TypedValue)mem[_Val];
        internal readonly static SqlLiteral Null = 
            new SqlLiteral(new Iix(-1L,Context._system,--_uid),
                Context._system,TNull.Value,StandardDataType.types[Sqlx.Null]);
        internal override long target => -1;
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(Iix dp, Context cx, TypedValue v, Domain td=null) 
            : base(dp,BTree<long,object>.Empty+(_Domain, (td??v.dataType).defpos)+(_Val, v))
        {
            if (td != null  && v.dataType!=null && !td.CanTakeValueOf(v.dataType))
                throw new DBException("22000", v);
            if (dp.dp == -1L)
                throw new PEException("PE999");
        }
        public SqlLiteral(Iix iix, BTree<long, object> m) : base(iix, m) 
        {
            if (iix.dp == -1L)
                throw new PEException("PE999");
        }
        public static SqlLiteral operator+(SqlLiteral s,(long,object)x)
        {
            return new SqlLiteral(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlLiteral(iix,m);
        }
        public SqlLiteral(Iix dp, Domain dt) : base(dp, BTree<long, object>.Empty
            + (_Domain, dt.defpos) + (_Val, dt.defaultValue))
        { }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            return true;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlLiteral(new Iix(iix,dp),mem);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlLiteral)base._Fix(cx);
            var nv = val.Fix(cx);
            if (nv != val)
                r += (_Val, nv);
            return cx.Add(r);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlLiteral)base._Relocate(cx);
            r += (_Val, val.Relocate(cx));
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
            if (val is TQParam tq)
            {
                r += (_Val, cx.values[tq.qid.dp]);
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
            var c = v as SqlLiteral;
            if (c == null || cx._Dom(this).CompareTo(cx._Dom(v))!=0)
                return false;
            return val == c.val;
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue Eval(Context cx)
        {
            if (val is TQParam tq && cx.values[tq.qid.dp] is TypedValue tv && !tv.IsNull)
                return tv;
            return val ?? cx._Dom(this).defaultValue;
        }
        public override int CompareTo(object obj)
        {
            var that = obj as SqlLiteral;
            if (that == null)
                return 1;
            return val?.CompareTo(that.val) ?? throw new PEException("PE000");
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            return new CTree<long, Domain>(defpos, val.dataType);
        }
        /// <summary>
        /// A literal is supplied by any query
        /// </summary>
        /// <param name="q">the query</param>
        /// <returns>true</returns>
        internal override bool IsFrom(Context cx,RowSet q,bool ordered,Domain ut=null)
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return CTree<long,RowSet.Finder>.Empty;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return true;
        }
        internal override string ToString(string sg, Remotes rf, CList<long> cs, CTree<long, string> ns, Context cx)
        {
            return val.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(val.ToString());
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
        public SqlDateTimeLiteral(Iix dp, Context cx, Domain op, string n)
            : base(dp, cx, op.Parse(dp.lp,n))
        {}
        protected SqlDateTimeLiteral(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDateTimeLiteral operator+(SqlDateTimeLiteral s,(long,object)x)
        {
            return new SqlDateTimeLiteral(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDateTimeLiteral(iix,m);
        }
    }
    /// <summary>
    /// A Row value
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlRow : SqlValue
    {
        public SqlRow(Iix dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(Iix iix, Context cx, BList<SqlValue> vs, BTree<long, object> m = null)
            : base(iix, _Mem(cx,vs,m) + (Dependents, _Deps(vs)) + (_Depth, cx.Depth(vs)))
        { }
        public SqlRow(Iix dp, Context cx, Domain xp, CList<long> vs, BTree<long, object> m = null)
            : base(dp, _Inf(dp.dp, cx, m, xp, vs) + (Dependents, _Deps(vs))
                  + (_Depth, cx.Depth(vs)))
        { }
        static BTree<long, object> _Mem(Context cx,BList<SqlValue>vs,BTree<long,object> m)
        {
            var dm = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, vs));
            m = (m ?? BTree<long, object>.Empty) + (_Domain, dm.defpos) + (Domain.Aggs, dm.aggs);
            return m;
        }
        protected static BTree<long, object> _Inf(long dp, Context cx, BTree<long, object> m,
    Domain xp, CList<long> vs)
        {
            var dm = Domain.Row;
            var cb = xp.First();
            for (var b = vs.First(); b != null; b = b.Next(),cb=cb?.Next())
            {
                var ob = (SqlValue)cx.obs[b.value()];
                var cd = xp.representation[cb?.value()??-1L];
                dm += (cx,ob??new SqlNull(new Iix(b.value())));
            }
            dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()));
            return (m ?? BTree<long, object>.Empty) + (_Domain, dm.defpos);
        }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return (SqlRow)s.New(s.mem + m);
        }
        public static SqlRow operator +(SqlRow s, (Context,SqlValue) x)
        {
            var (cx, sv) = x;
            return (SqlRow)s.New(s.mem + (_Domain,(cx._Dom(s)+(cx,sv)).defpos));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(iix, m);
        }
        internal override bool WellDefinedOperands(Context cx)
        {
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue s && !s.WellDefinedOperands(cx))
                    return false;
            return base.WellDefinedOperands(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRow(new Iix(iix,dp),mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var cs = CList<long>.Empty;
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=cx._Dom(this).rowType.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)cx._Replace(b.value(),so,sv);
                cs += v.defpos;
                vs += v;
                if (v.defpos != b.value())
                    ch = true;
            }
            if (ch)
            {
                var dm = cx.Add(new Domain(cx.GetUid(),cx,Sqlx.ROW, vs));
                r = r+ (_Domain, dm.defpos) + (Dependents, _Deps(vs))
                  + (_Depth, cx.Depth(vs)); // don't use "new SqlRow" here as it won't work for SqlNewRow
            }
            if (r!=this)
                r = (SqlRow)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            var dm = cx._Dom(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).Grouped(cx,gs))
                        return false;
            return true;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            var dt = c._Dom(this);
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=dt.rowType.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)c.obs[b.value()];
                var nv = v.Having(c, dm);
                vs += nv;
                ch = ch || nv != v;
            }
            return ch ? (SqlValue)c.Add(new SqlRow(c.GetIid(), c, vs)) : this;
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlRow r)
            {
                var rb = c._Dom(r).rowType.First();
                for (var b=c._Dom(this).rowType.First();b!=null && rb!=null;
                    b=b.Next(),rb=rb.Next())
                    if (!((SqlValue)c.obs[b.value()]).Match(c,(SqlValue)c.obs[rb.value()]))
                        return false;
                return true;
            }
            return false;
        }
        internal override (SqlValue, RowSet) Resolve(Context cx, RowSet fm)
        {
            var dm = cx._Dom(this);
            if (dm.kind != Sqlx.CONTENT)
                return (this, fm);
            var cs = BList<SqlValue>.Empty;
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                var c = (SqlValue)cx.obs[b.value()];
                SqlValue v;
                (v, fm) = c.Resolve(cx, fm);
                cs += v;
            }
            var sv = new SqlRow(iix, cx, cs);
            var r = (SqlRow)cx.Replace(this, sv);
            return (r,fm);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var cs = CList<long>.Empty;
            var ch = false;
            for (var b = cx._Dom(r).rowType.First(); b != null; b = b.Next())
            {
                var a = ((SqlValue)cx.obs[b.value()]).AddFrom(cx, q);
                if (a.defpos != b.value())
                    ch = true;
                r += (cx,a);
            }
            return ch?(SqlValue)cx.Add(r):this;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b=cx._Dom(this).rowType.First();b!=null;b=b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx,q))
                        return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, cs))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b=cx._Dom(this).rowType.First();b!=null;b=b.Next())
            {
                r += ((SqlValue)cx.obs[b.value()]).KnownFragments(cx, kb);
                y = y && r.Contains(b.value());
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            var dm = cx._Dom(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue sv)
                    r += sv.IsAggregation(cx);
            return r;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values[defpos] is TRow r && !r.IsNull)
                return r;
            var vs = CTree<long,TypedValue>.Empty;
            var dm = cx._Dom(this);
            for (var b=dm.rowType.First();b!=null;b=b.Next())
            {
                var s = b.value();
                vs += (s, cx.obs[s]?.Eval(cx)??TNull.Value);
            }
            return new TRow(dm, vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            var dm = cx._Dom(this);
            for (var b=dm.rowType.First(); b!=null;b=b.Next())
                tg = cx.obs[b.value()].StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            var dm = cx._Dom(this);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
                tg = cx.obs[b.value()].AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,r,qn);
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
                r += cx.obs[b.value()].Needs(cx, rs);
            return r;
        }
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
            CTree<long, string> ns, Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            sb.Append(" (");
            for (var b = cx._Dom(this).rowType.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(cx.obs[b.value()].ToString(sg,rf,cs,ns,cx));
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// Prepare an SqlValue with reified columns for use in trigger
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlOldRow : SqlRow
    {
        internal SqlOldRow(Ident ic, Context cx, PTrigger tg, From fm)
            : base(ic.iix, _Mem(ic,cx,fm))
        { }
        protected SqlOldRow(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident ic,Context cx,From fm)
        {
            var r = BTree<long,object>.Empty;
            var ti = cx.Inf(fm.target);
            r = r + (_Domain, fm.domain)
                   + (Name, ic.ident) + (_From, fm.defpos);
            var ids = Ident.Idents.Empty;
            for (var b=ti.dataType.rowType.First();b!=null;b=b.Next())
            {
                var cp = b.value();
                var ci = cx.Inf(cp);
                var f = new SqlField(cx.GetIid(), ci.name, ic.iix.dp, 
                    ci.dataType, cp);
                cx.Add(f);
                ids += (f.name, f.iix, Ident.Idents.Empty);
            }
            cx.defs += (ic.ident, ic.iix, ids);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlOldRow(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlOldRow(new Iix(iix,dp),mem);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override void Set(Context cx, TypedValue v)
        {
            var tgc = ((TransitionRowSet.TargetCursor)cx.values[defpos]);
            TriggerActivation ta = null;
            for (var c = cx; ta == null && c != null; c = c.next)
                if (c is TriggerActivation t && t._trs.defpos == tgc._rowsetpos)
                    ta = t;
            ta.values += (Trigger.OldRow, v);
            base.Set(cx, v);
        }
    }
    // shareable as of 26 April 2021
    internal class SqlNewRow : SqlRow
    {
        internal SqlNewRow(Ident ic, Context cx, PTrigger tg, From fm)
            : base(ic.iix, _Mem(ic, cx, fm))
        { }
        protected SqlNewRow(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Ident ic, Context cx, From fm)
        {
            var r = BTree<long, object>.Empty;
            var ti = cx.Inf(fm.target);
            r = r + (_Domain, fm.domain)
                   + (Name, ic.ident) + (_From, fm.defpos);
            var ids = Ident.Idents.Empty;
            for (var b = ti.dataType.rowType.First(); b != null; b = b.Next())
            {
                var cp = b.value();
                var ci = cx.Inf(cp);
                var f = new SqlField(cx.GetIid(), ci.name, ic.iix.dp,
                    ci.dataType, cp);
                cx.Add(f);
                ids += (f.name, f.iix, Ident.Idents.Empty);
            }
            cx.defs += (ic.ident, ic.iix, ids);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlNewRow(new Iix(iix,dp), mem);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlNewRow(iix, m);
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return base.Having(c, dm); // throws error
        }
        internal override bool Match(Context c, SqlValue v)
        {
            return false;
        }
        internal override void Set(Context cx, TypedValue v)
        {
            var tgc = ((TransitionRowSet.TargetCursor)cx.values[defpos]);
            TriggerActivation ta = null;
            for (var c = cx; ta == null && c != null; c = c.next)
                if (c is TriggerActivation t && t._trs.defpos == tgc._rowsetpos)
                    ta = t;
            ta.values += (Trigger.NewRow, v);
            base.Set(cx, v);
        }
    }
    // shareable as of 26 April 2021
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = -319; // CList<long> SqlValue
        internal CList<long> rows =>
            (CList<long>)mem[Rows]?? CList<long>.Empty;
        public SqlRowArray(Iix dp,Context cx,Domain ap,CList<long> rs) 
            : base(dp, BTree<long,object>.Empty+(_Domain, ap.defpos)+(Rows, rs)
                  +(_Depth,cx.Depth(rs,ap)))
        { }
        internal SqlRowArray(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlRowArray operator+(SqlRowArray s,(long,object)x)
        {
            return new SqlRowArray(s.iix, s.mem + x);
        }
        public static SqlRowArray operator+(SqlRowArray s,SqlRow x)
        {
            return new SqlRowArray(s.iix, s.mem + (Rows, s.rows + x.defpos));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowArray(iix, m);
        }
        internal override bool WellDefinedOperands(Context cx)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue s && !s.WellDefinedOperands(cx))
                    return false;
            return base.WellDefinedOperands(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRowArray(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (Rows, cx.Fix(rows));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlRowArray)base._Fix(cx);
            var nr = cx.Fix(rows);
            if (nr != rows)
                r += (Rows, nr);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRowArray)base._Replace(cx,so,sv);
            var rws = CList<long>.Empty;
            var ch = false;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var v = cx.ObReplace(b.value(),so,sv);
                ch = ch || v != b.value();
                rws += v;
            }
            if (ch)
                r += (Rows, rws);
            if (r!=this)
                r = (SqlRowArray)New(cx, r.mem);
            cx.done += (defpos, r);
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
                if (!((SqlValue)cx.obs[b.value()]).Grouped(cx, gs))
                    return false;
            return true;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlRowArray)base.AddFrom(cx, q);
            var rws = CList<long>.Empty;
            var ch = false;
            var dm = cx._Dom(this);
            var ag = dm.aggs;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var o = (SqlRow)cx.obs[b.value()];
                var a = (SqlRow)o.AddFrom(cx, q);
                if (a.defpos != b.value())
                    ch = true;
                rws += a.defpos;
                ag += a.IsAggregation(cx);
            }
            if (ch)
                r += (Rows, rws);
            if (ag!=dm.aggs)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid())+(Domain.Aggs, ag));
                r += (Domain.Aggs, ag);
            }
            r += (_Domain, dm.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, cs))
                    return false;
            return true;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b=rows.First();b!=null;b=b.Next())
            {
                r += ((SqlValue)cx.obs[b.value()]).KnownFragments(cx, kb);
                y = y && r.Contains(b.value());
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        internal override TypedValue Eval(Context cx)
        {
            var vs = BList<TypedValue>.Empty;
            for (var b=rows.First(); b!=null; b=b.Next())
                vs += cx.obs[b.value()].Eval(cx);
            return new TArray(cx._Dom(this), vs);
        }
        internal override RowSet RowSetFor(Iix dp,Context cx,CList<long>us,
            CTree<long,Domain> re)
        {
            var rs = BList<(long,TRow)>.Empty;
            var xp = new Domain(cx,cx._Dom(this) + (Domain.Kind, Sqlx.TABLE));
            var isConst = true;
            if (us != null && us.CompareTo(xp.rowType) != 0)
                xp = new Domain(cx,xp + (Domain.RowType, us)
                    + (Domain.Representation,xp.representation+re));
            for (var b = rows.First(); b != null && isConst; b = b.Next())
            {
                var v = cx.obs[b.value()];
                isConst = (v as SqlValue)?.isConstant(cx) == true;
                var x = v.Eval(cx);
                var y = x.ToArray();
                rs += (v.defpos,new TRow(cx,xp,y));
            }
            if (isConst)
                return new ExplicitRowSet(dp.dp, cx, xp, rs);
            return new SqlRowSet(dp.dp, cx, xp, rows);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            for (var b=rows.First(); b!=null;b=b.Next())
                tg = cx.obs[b.value()].StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                tg = cx.obs[b.value()].AddIn(cx,rb, tg);
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
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,r,qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b=rows.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlXmlValue : SqlValue
    {
        internal const long
            Attrs = -323, // BTree<int,(XmlName,long)> SqlValue
            Children = -324, // CList<long> SqlXmlValue
            Content = -325, // long SqlXmlValue
            Element = -326; // XmlName
        public XmlName element => (XmlName)mem[Element];
        public BList<(XmlName, long)> attrs =>
            (BList<(XmlName, long)>)mem[Attrs] ?? BList<(XmlName, long)>.Empty;
        public CList<long> children =>
            (CList<long>)mem[Children]?? CList<long>.Empty;
        public long content => (long)(mem[Content]??-1L); // will become a string literal on evaluation
        public SqlXmlValue(Iix dp, Context cx, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, (m ?? BTree<long, object>.Empty) + (_Domain, Domain.XML.defpos) 
                  + (Element,n)+(Content,c.defpos)) { }
        protected SqlXmlValue(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlXmlValue operator+(SqlXmlValue s,(long,object)m)
        {
            return new SqlXmlValue(s.iix, s.mem + m);
        }
        public static SqlXmlValue operator +(SqlXmlValue s, SqlXmlValue child)
        {
            return new SqlXmlValue(s.iix, 
                s.mem + (Children,s.children+child.defpos));
        }
        public static SqlXmlValue operator +(SqlXmlValue s, (XmlName,SqlValue) attr)
        {
            var (n, a) = attr;
            return new SqlXmlValue(s.iix,
                s.mem + (Attrs, s.attrs + (n,a.defpos)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlXmlValue(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlXmlValue(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlXmlValue)base._Relocate(cx);
            r += (Attrs, cx.Fix(attrs));
            r += (Children, cx.Fix(children));
            r += (Content, cx.Fix(content));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlXmlValue)base._Fix(cx);
            var na = cx.Fix(attrs);
            if (na!=attrs)
            r += (Attrs, na);
            var nc = cx.Fix(children);
            if (nc!=children)
            r += (Children, nc);
            var nv = cx.Fix(content);
            if (content!=nv)
                r += (Content, nv);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlXmlValue)base._Replace(cx, so, sv);
            var at = r.attrs;
            for (var b=at?.First();b!=null;b=b.Next())
            {
                var (n, ao) = b.value();
                var v = cx.ObReplace(ao,so,sv);
                if (v != ao)
                    at = new BList<(XmlName,long)>(at,b.key(), (n, v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = cx.ObReplace(r.content,so,sv);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for(var b=ch?.First();b!=null;b=b.Next())
            {
                var v = cx.ObReplace(b.value(),so,sv);
                if (v != b.value())
                    ch = new CList<long>(ch, b.key(), v);
            }
            if (ch != r.children)
                r += (Children, ch);
            if (r!=this)
                r = (SqlXmlValue)New(cx, r.mem);
            cx.done += (defpos, r);
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
                var o = (SqlValue)cx.obs[ao];
                var a = o.AddFrom(cx, q);
                if (a.defpos != ao)
                    aa = new BList<(XmlName,long)>(aa, b.key(), (n, a.defpos));
            }
            if (aa != r.attrs)
                r += (Attrs, aa);
            var ch = r.children;
            for (var b=r.children.First();b!=null;b=b.Next())
            {
                var o = (SqlXmlValue)cx.obs[b.value()];
                var a = o.AddFrom(cx, q);
                if (a.defpos != b.value())
                    ch += (b.key(), a.defpos);
            }
            if (ch != r.children)
                r += (Children, ch);
            var oc = (SqlValue)cx.obs[r.content];
            var c = oc.AddFrom(cx,q);
            if (c.defpos != r.content)
                r += (Content, c.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override TypedValue Eval(Context cx)
        {
            var r = new TXml(element.ToString());
            for (var b = attrs?.First(); b != null; b = b.Next())
            {
                var (n, a) = b.value();
                if (cx.obs[a].Eval(cx) is TypedValue ta)
                    r += (n.ToString(), ta);
            }
            for(var b=children?.First();b!=null;b=b.Next())
                if (cx.obs[b.value()].Eval(cx) is TypedValue tc)
                    r +=(TXml)tc;
            if (cx.obs[content]?.Eval(cx) is TypedValue tv)
                r += (tv as TChar)?.value;
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[content]).Needs(cx,r,qn);
            for (var b = attrs.First(); b != null; b = b.Next())
            {
                var (n, a) = b.value();
                qn = ((SqlValue)cx.obs[a]).Needs(cx,r, qn);
            }
            for (var b = children.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,r,qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("<");
            sb.Append(element.ToString());
            for(var b=attrs.First();b!=null;b=b.Next())
            {
                sb.Append(" ");
                sb.Append(b.value().Item1);
                sb.Append("=");
                sb.Append(Uid(b.value().Item2));
            }
            if (content != -1L || children.Count!=0)
            {
                sb.Append(">");
                if (content != -1L)
                    sb.Append(Uid(content));
                else
                    for (var b=children.First(); b!=null;b=b.Next())
                        sb.Append(Uid(b.value()));
                sb.Append("</");
                sb.Append(element.ToString());
            } 
            else sb.Append("/");
            sb.Append(">");
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
        public SqlSelectArray(Iix dp, Context cx, RowSet qe, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty + (Domain.Aggs,cx._Dom(qe).aggs)
                  + (_Domain, qe.domain) + (ArrayValuedQE, qe.defpos))) { }
        protected SqlSelectArray(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator+(SqlSelectArray s,(long,object)x)
        {
            return new SqlSelectArray(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSelectArray(iix, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlSelectArray(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlSelectArray)base._Relocate(cx);
            r += (ArrayValuedQE, cx.Fix(aqe));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlSelectArray)base._Fix(cx);
            var nq = cx.Fix(aqe);
            if (nq!=aqe)
            r += (ArrayValuedQE, nq);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlSelectArray)base._Replace(cx,so,sv);
            var ae = cx.ObReplace(r.aqe,so,sv);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            if (r!=this)
                r = (SqlSelectArray)New(cx, r.mem);
            cx.done += (defpos, r);
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
            var dm = cx._Dom(this);
            var va = BList<TypedValue>.Empty;
            var q = (RowSet)cx.obs[aqe];
            var et = cx._Dom(dm.elType);
            var nm = q.name;
            for (var rb=q.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb;
                if (et==null)
                    va += rw[nm];
                else
                {
                    var qd = cx._Dom(q);
                    var vs = new TypedValue[qd.display];
                    for (var i = 0; i < qd.display; i++)
                        vs[i] = rw[i];
                    va += new TRow(cx, qd, vs);
                }
            }
            return new TArray(dm,va);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[aqe].StartCounter(cx,rs,tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[aqe].AddIn(cx,rb,tg);
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
            Array = -328, // CList<long> SqlValue
            Svs = -329; // long SqlValueSelect
        /// <summary>
        /// the array
        /// </summary>
        public CList<long> array =>(CList<long>)mem[Array]??CList<long>.Empty;
        // alternatively, the source
        public long svs => (long)(mem[Svs] ?? -1L);
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(Iix dp,Context cx,Domain xp,CList<long> v)
            : base(dp,BTree<long,object>.Empty+(_Domain, xp.defpos)+(Array,v))
        { }
        protected SqlValueArray(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueArray operator+(SqlValueArray s,(long,object)x)
        {
            return new SqlValueArray(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueArray(iix,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = array.First(); b != null; b = b.Next())
                r += cx.obs[b.value()]._Rdc(cx);
            if (svs >= 0) r += cx.obs[svs]._Rdc(cx);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueArray(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlValueArray)base._Relocate(cx);
            r += (Array, cx.Fix(array));
            r += (Svs, cx.Fix(svs));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlValueArray)base._Fix(cx);
            r += (Array, cx.Fix(array));
            if (svs>=0)
                r += (Svs, cx.Fix(svs));
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueArray)base._Replace(cx,so,sv);
            var ar = r.array;
            for (var b=ar?.First();b!=null;b=b.Next())
            {
                var v = cx.ObReplace(b.value(),so,sv);
                if (v != b.value())
                    ar += (b.key(), v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = cx.ObReplace(r.svs, so, sv);
            if (ss != r.svs)
                r += (Svs, ss);
            if (r!=this)
                r = (SqlValueArray)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = CList<long>.Empty;
            var dm = cx._Dom(this);
            var ag = dm.aggs;
            var ch = false;
            for (var b=array.First();b!=null;b=b.Next())
            {
                var a = ((SqlValue)cx.obs[b.value()]).AddFrom(cx, q);
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
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
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
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return ((SqlValue)cx.obs[svs])?.KnownBy(cx, q) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, cs))
                    return false;
            return ((SqlValue)cx.obs[svs])?.KnownBy(cx, cs) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b=array?.First(); b != null; b = b.Next())
            {
                r += ((SqlValue)cx.obs[b.value()]).KnownFragments(cx, kb);
                y = y && r.Contains(b.value());
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var dm = cx._Dom(this);
            if (svs != -1L)
            {
                var ar = CList<TypedValue>.Empty;
                var ers = cx.obs[svs]?.Eval(cx) as TArray;
                for (var b = ers.list?.First(); b != null; b = b.Next())
                    ar+=b.value()[0];
                return new TArray(dm, ar);
            }
            var vs = BList<TypedValue>.Empty;
            for (var b=array?.First();b!=null;b=b.Next())
                vs += cx.obs[b.value()]?.Eval(cx) ?? dm.defaultValue;
            return new TArray(dm,vs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            for (var b = array.First(); b != null; b = b.Next())
                cx.obs[b.value()].StartCounter(cx, rs, tg);
            if (svs!=-1L) tg = cx.obs[svs].StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = array.First(); b != null; b = b.Next())
                cx.obs[b.value()].AddIn(cx,rb,tg);
            if (svs!=-1L) tg = cx.obs[svs].AddIn(cx,rb,tg);
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
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,r,qn);
            qn = ((SqlValue)cx.obs[svs])?.Needs(cx,r,qn);
            return qn;
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
        public SqlValueSelect(Iix dp,Context cx,RowSet r,Domain xp)
            : base(dp, BTree<long,object>.Empty + (Domain.Aggs,cx._Dom(r).aggs)
                  + (Expr, r.defpos) + (_Domain, r.domain) + (RowSet.Scalar,xp.kind!=Sqlx.TABLE)
                  + (Dependents, new CTree<long, bool>(r.defpos, true))
                  + (_Depth, r.depth + 1))
        { }
        protected SqlValueSelect(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueSelect operator+(SqlValueSelect s,(long,object)x)
        {
            return new SqlValueSelect(s.iix, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueSelect(iix,m);
        }
        internal override RowSet RowSetFor(Iix dp, Context cx, CList<long> us,
            CTree<long,Domain> re)
        {
            var r = (RowSet)cx.obs[expr];
            var dm = cx._Dom(this);
            if (us == null || us.CompareTo(dm.rowType) == 0)
                return r;
            var xp = new Domain(cx,dm + (Domain.RowType, us)
                +(Domain.Representation,dm.representation+re));
            return new SelectedRowSet(cx, xp.defpos, r);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[expr]._Rdc(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueSelect(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlValueSelect)base._Relocate(cx);
            var e = cx.Fix(expr);
            if (e != expr)
                r += (Expr, e);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlValueSelect)base._Fix(cx);
            var ne = cx.Fix(expr);
            if (ne!=expr)
            r += (Expr,ne);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base._Replace(cx,so,sv);
            var ex = (RowSet)cx._Replace(r.expr,so,sv);
            if (ex.defpos != r.expr)
                r = r._Expr(cx,ex);
            if (r!=this)
                r = (SqlValueSelect)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal SqlValueSelect _Expr(Context cx,RowSet e)
        {
            var d = Math.Max(depth, e.depth + 1);
            return this + (Expr, e.defpos) + (Dependents, dependents + (e.defpos,true)) + (_Depth, d);
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
            var dm = cx._Dom(this);
            var ers = ((RowSet)cx.obs[expr]);
            if (scalar)
                return ers.First(cx)?[0] ?? dm.defaultValue;
            var rs = BList<TypedValue>.Empty;
            for (var b = ers.First(cx); b != null; b = b.Next(cx))
                rs += b;
            return new TArray(dm, rs);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[expr].StartCounter(cx,rs,tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[expr].AddIn(cx,rb,tg);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            var e = (RowSet)cx.obs[expr];
            var ed = cx._Dom(e);
            var nd = CTree<long,bool>.Empty;
            for (var b = ed.rowType.First(); b != null; b = b.Next())
                nd += ((SqlValue)cx.obs[b.value()]).Needs(cx);
            for (var b = e.where.First(); b != null; b = b.Next())
                nd += ((SqlValue)cx.obs[b.key()]).Needs(cx);
            for (var b = e.Sources(cx).First(); b != null; b = b.Next())
                for (var c = ((RowSet)cx.obs[b.key()]).finder.First(); c != null; c = c.Next())
                    nd -= c.key();
            return qn + nd;
        }
        internal override bool WellDefinedOperands(Context cx)
        {
            var ed = cx._Dom(cx.obs[expr]);
            for (var b = ed.rowType.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).WellDefinedOperands(cx))
                    return false;
            return base.WellDefinedOperands(cx);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return cx.obs[expr].Needs(cx, rs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
   //         sb.Append(" TargetType: ");sb.Append(targetType);
            sb.Append(" (");sb.Append(Uid(expr)); sb.Append(")");
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
            Bits = -333; // CList<long>
        /// <summary>
        /// the set of column references
        /// </summary>
        internal CList<long> bits => (CList<long>)mem[Bits];
        /// <summary>
        /// constructor: a new ColumnFunction
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="t">the datatype</param>
        /// <param name="c">the set of TableColumns</param>
        public ColumnFunction(Iix dp, Context cx, BList<Ident> c)
            : base(dp, BTree<long, object>.Empty+(_Domain, Domain.Bool.defpos)+ (Bits, c)) { }
        protected ColumnFunction(Iix dp, BTree<long, object> m) :base(dp, m) { }
        public static ColumnFunction operator+(ColumnFunction s,(long,object)x)
        {
            return new ColumnFunction(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnFunction(iix,mem);
        }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            return false;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            return this;
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            return false;
        }
        internal override DBObject Relocate(long dp)
        {
            return new ColumnFunction(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (Bits, cx.Fix(bits));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (ColumnFunction)base._Fix(cx);
            var nb = cx.Fix(bits);
            if (nb!=bits)
                r += (Bits, nb);
            return cx.Add(r);
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
        internal SqlCursor(Iix dp, RowSet cs, string n) 
            : base(dp, BTree<long,object>.Empty+
                  (_Domain, cs.domain)+(Name, n)+(Spec,cs.defpos)
                  +(Dependents,new CTree<long,bool>(cs.defpos,true))
                  +(_Depth,1+cs.depth))
        { }
        protected SqlCursor(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCursor operator+(SqlCursor s,(long,object)x)
        {
            return new SqlCursor(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCursor(iix,m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[spec]._Rdc(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCursor(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlCursor)base._Relocate(cx);
            r += (Spec, cx.Fix(spec));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlCursor)base._Fix(cx);
            var ns = cx.Fix(spec);
            if (ns != spec)
                r += (Spec, ns);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCursor)base._Replace(cx,so,sv);
            var sp = cx.ObReplace(r.spec,so,sv);
            if (sp != r.spec)
                r += (Spec, sp);
            if (r!=this)
                r = (SqlCursor)New(cx, r.mem);
            cx.done += (defpos, r);
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
            sb.Append(spec);
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class SqlCall : SqlValue
    {
        internal const long
            Call = -335; // long CallStatement
        public long call =>(long)(mem[Call]??-1L);
        public SqlCall(Iix dp, Context cx, CallStatement c, BTree<long, object> m = null)
            : base(dp, m ?? BTree<long, object>.Empty
                  + (_Domain, c.domain) + (Domain.Aggs, c.aggs)
                  + (Call, c.defpos)+(Dependents,new CTree<long,bool>(c.defpos,true))
                  +(_Depth,1+c.depth)+(Name,c.name))
        {  }
        protected SqlCall(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCall operator+(SqlCall c,(long,object)x)
        {
            return (SqlCall)c.New(c.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCall(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCall(new Iix(iix,dp),mem);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            return cx.obs[call]._Rdc(cx);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlCall)base._Relocate(cx);
            r += (Call, cx.Fix(call));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlCall)base._Fix(cx);
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
            var c = (CallStatement)cx.obs[r.call];
            if (c != null)
            {
                if (cx.obs[c.var] is SqlValue a)
                {
                    a = a.AddFrom(cx, q);
                    if (a.defpos != c.var)
                        c += (CallStatement.Var, a.defpos);
                }
                var vs = CList<long>.Empty;
                for (var b = c.parms.First(); b != null; b = b.Next())
                    vs += ((SqlValue)cx.obs[b.value()]).AddFrom(cx, q).defpos;
                c += (CallStatement.Parms, vs);
                r += (Call, c.defpos);
            }
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCall)base._Replace(cx,so,sv);
            var ca = cx.ObReplace(r.call,so,sv);
            if (ca != r.call)
                r += (Call, ca);
            if (r!=this)
                r = (SqlCall)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            var ca = (CallStatement)cx.obs[call];
            for (var b=ca.parms.First();b!=null;b=b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx,q))
                        return false;
            return true;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            var ca = (CallStatement)cx.obs[call];
            for (var b = ca.parms.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, cs))
                    return false;
            return true;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c.parms.First(); b != null; b = b.Next())
                tg = cx.obs[b.value()].AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long r, CTree<long, bool> qn)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c?.parms.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,r,qn);
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            var c = (CallStatement)cx.obs[call];
            for (var b = c?.parms.First(); b != null; b = b.Next())
                r += ((SqlValue)cx.obs[b.value()]).Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c.parms.First(); b != null; b = b.Next())
                if (!cx.obs[b.value()].LocallyConstant(cx, rs))
                    return false;
            return true;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(name);  
            sb.Append(" "); sb.Append(Uid(call));
            return sb.ToString();
        }
    }
    /// <summary>
    /// An SqlValue that is a procedure/function call or static method
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(Iix dp, Context cx, CallStatement c) : base(dp, cx, c) { }
        protected SqlProcedureCall(Iix dp,BTree<long,object>m):base(dp,m) { }
        public static SqlProcedureCall operator+(SqlProcedureCall s,(long,object)x)
        {
            return new SqlProcedureCall(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlProcedureCall(iix, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlProcedureCall(new Iix(iix,dp),mem);
        }
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            var c = (CallStatement)cx.obs[call];
            var dm =cx._Dom(this);
            try
            {
                var pp = c.procdefpos;
                var proc = (Procedure)cx.obs[pp];
                if (proc == null)
                {
                    proc = (Procedure)((Procedure)tr.objects[pp]).Instance(iix,cx);
                    cx.Add(proc);
                }
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
            var c = (CallStatement)cx.obs[call];
            var proc = (Procedure)cx.obs[c.procdefpos];
            if (cx.db.objects[proc.inverse] is Procedure inv)
                eqs = eqs.Add(proc.defpos, c.parms[0], proc.defpos, inv.defpos);
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            var c = (CallStatement)cx.obs[call];
            for (var b = c.parms.First(); b != null; b = b.Next())
                r = ((SqlValue)cx.obs[b.value()]).Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c.parms.First(); b != null; b = b.Next())
                if (!cx.obs[b.value()].LocallyConstant(cx, rs))
                    return false;
            return true;
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
        public SqlMethodCall(Iix dp, Context cx, CallStatement c) : base(dp,cx,c)
        { }
        protected SqlMethodCall(Iix dp,BTree<long, object> m) : base(dp, m) { }
        public static SqlMethodCall operator+(SqlMethodCall s,(long,object)x)
        {
            return new SqlMethodCall(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlMethodCall(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlMethodCall(new Iix(iix,dp),mem);
        }
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            var c = (CallStatement)cx.obs[call];
            qn += (c.var, true);
            return base.Needs(cx, r, qn);
        }
        internal override (SqlValue, RowSet) Resolve(Context cx, RowSet fm)
        {
            SqlValue v;
            var c = ((CallStatement)cx.obs[call]);
            (v, fm) = base.Resolve(cx, fm);
            var mc = (SqlMethodCall)v;
            var ov = (SqlValue)cx.obs[c.var];
            (v, fm) = ov.Resolve(cx, fm);
            var oi = (ObInfo)cx.role.infos[v.domain]; // we need the names
            var p = oi.methodInfos[c.name]?[(int)c.parms.Count] ?? -1L;
            var nc = c + (CallStatement.Var, v.defpos) + (CallStatement.ProcDefPos, p);
            cx.Add(nc);
            var pr = ((DBObject)cx.db.objects[p]).Instance(iix, cx) as Procedure;
            mc = mc + (Call, nc.defpos) + (_Domain, pr.domain);
            return ((SqlValue)cx.Add(mc), fm);
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
            var c = (CallStatement)cx.obs[call];
            if (c.var == -1L)
                throw new PEException("PE241");
            var v = (SqlValue)cx.obs[c.var];
            var vv = v.Eval(cx);
            for (var b =cx._Dom(v).rowType.First();b!=null;b=b.Next())
            {
                var f = b.value();
                cx.values += (f, vv[f]);
            }
            var p = c.procdefpos;
            if (p<0)
            {
                var oi = (ObInfo)cx.db.role.infos[v.domain];
                p = oi.methodInfos[c.name]?[c.parms.Length]??-1L;
                if (p < 0)
                    throw new DBException("42108", c.name);
            }
            var proc = (Method)((Method)cx.db.objects[p]).Instance(c.iix,cx);
            return proc.Exec(cx, c.var, c.parms).val;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var c = (CallStatement)cx.obs[call];
            var r = cx.obs[c.var].Needs(cx, rs);
            for (var b = c.parms.First(); b != null; b = b.Next())
                r += ((SqlValue)cx.obs[b.value()]).Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            var c = (CallStatement)cx.obs[call];
            if (!cx.obs[c.var].LocallyConstant(cx, rs))
                return false;
            for (var b = c.parms.First(); b != null; b = b.Next())
                if (!cx.obs[b.value()].LocallyConstant(cx, rs))
                    return false;
            return true;
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
        public SqlConstructor(Iix dp, Context cx, CallStatement c)
            : base(dp, cx, c)
        { }
        protected SqlConstructor(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlConstructor operator+(SqlConstructor s,(long,object)x)
        {
            return new SqlConstructor(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlConstructor(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlConstructor(new Iix(iix,dp),mem);
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            var c = (CallStatement)cx.obs[call];
            var proc = (Method)tr.objects[c.procdefpos];
            var ac = new CalledActivation(cx, proc, proc.domain);
            return proc.Exec(ac,-1L,c.parms).val;
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
        public SqlDefaultConstructor(Iix dp, Context cx, Domain u, CList<long> ins)
            : base(dp, _Mem(cx.GetIid(),cx,(UDType)u,ins))
        { }
        protected SqlDefaultConstructor(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor s, (long, object) x)
        {
            return (SqlDefaultConstructor)s.New(s.mem + x);
        }
        static BTree<long,object> _Mem(Iix ap,Context cx,UDType u,CList<long> ps)
        {
            var rb = u.representation.First();
            for (var b = ps.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
            {
                var dt = rb.value();
                cx.Add(dt);
                cx.Add((SqlValue)cx.obs[b.value()] + (_Domain, dt.defpos));
            }
            return BTree<long, object>.Empty
                  + (Sce, cx._Add(new SqlRow(ap, cx, u, ps)).defpos)
                  + (_Domain, u.defpos) + (Dependents, _Deps(ps)) + (_Depth, cx.Depth(ps));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDefaultConstructor(iix, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlDefaultConstructor(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlDefaultConstructor)base._Relocate(cx);
            var sc = cx.Fix(r.sce);
            if (sc != r.sce)
                r += (Sce, sc);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDefaultConstructor)base._Replace(cx,so,sv);
            var os = (SqlRow)cx.obs[r.sce];
            var sc = os._Replace(cx,so,sv);
            if (sc.defpos != r.sce)
                r += (Sce, sc.defpos);
            if (r!=this)
                r = (SqlDefaultConstructor)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlDefaultConstructor)base._Fix(cx);
            var ns = cx.Fix(sce);
            if (ns != sce)
                r += (Sce, ns);
            return cx.Add(r);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (SqlDefaultConstructor)base.AddFrom(cx, q);
            var sc = (SqlRow)cx.obs[r.sce];
            var a = sc.AddFrom(cx, q);
            cx.Add(a);
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return cx.obs[sce].Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[sce].LocallyConstant(cx, rs);
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            try
            { 
                var vs = CTree<long,TypedValue>.Empty;
                var i = 0;
                var dm = cx._Dom(this);
                for (var b = dm.representation.First(); b != null; b = b.Next(), i++)
                {
                    var p = cx._Dom((SqlValue)cx.obs[sce]).rowType[i];
                    vs += (b.key(), ((SqlValue)cx.obs[p]).Eval(cx));
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
                return cx._Dom(this).defaultValue;
            }
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            return qn+cx.obs[sce].Needs(cx);
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
        public Sqlx kind => (Sqlx)mem[Domain.Kind];
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod => (Sqlx)mem[Mod];
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
            (CTree<long,bool>)mem[Filter]??CTree<long,bool>.Empty;
        /// <summary>
        /// a name for the window for a window function
        /// </summary>
        public long windowId => (long)(mem[WindowId]??-1L);
        /// <summary>
        /// the window for a window function
        /// </summary>
        public WindowSpecification window => (WindowSpecification)mem[Window];
        /// <summary>
        /// Check for monotonic
        /// </summary>
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(Iix dp, Context cx, Sqlx f, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m,
            BTree<long, object> mm = null) 
            : base(dp,_Mem(f,vl,o1,o2,mm)+(_Domain,_Type(cx, f, vl, o1).defpos)
                +(Name,f.ToString())+(Domain.Kind,f)+(Mod,m)+(Dependents,_Deps(vl,o1,o2)) 
                +(_Depth,cx.Depth(vl,o1,o2)))
        { }
        protected SqlFunction(Iix dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(Sqlx f,SqlValue vl,SqlValue o1,SqlValue o2,BTree<long,object>m)
        {
            var r = m??BTree<long, object>.Empty;
            if (vl != null)
                r += (_Val, vl.defpos);
            if (o1 != null)
                r += (Op1, o1.defpos);
            if (o2 != null)
                r += (Op2, o2.defpos);
            if (m?.Contains(_From)==true && aggregates(f))
                r += (Await, ((CTree<long,bool>)m[Await]??CTree<long,bool>.Empty)
                    +((long)(m[_From]??-1L),true));
            return r;
        }
        public static SqlFunction operator+(SqlFunction s,(Context,long,object)x)
        {
            var (cx, a, v) = x;
            var m = s.mem + (a, v);
            if (a == Op1)
                m += (_Domain, _Type(cx,s.kind, (SqlValue)cx.obs[s.val], (SqlValue)cx.obs[(long)v]).defpos);
            if (a == _Val)
                m += (_Domain, _Type(cx,s.kind, (SqlValue)cx.obs[(long)v], (SqlValue)cx.obs[s.op1]).defpos);
            if (a == _From && aggregates(s.kind))
                m += (Await, v);
            return new SqlFunction(s.iix, m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFunction(iix,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            var r = CTree<long, bool>.Empty;
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
        internal override bool WellDefinedOperands(Context cx)
        {
            if (cx.obs[val] is SqlValue vl && !vl.WellDefinedOperands(cx))
                return false;
            if (cx.obs[op1] is SqlValue o1 && !o1.WellDefinedOperands(cx))
                return false; 
            if (cx.obs[op2] is SqlValue o2 && !o2.WellDefinedOperands(cx))
                return false;
            return base.WellDefinedOperands(cx);
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
            var vl = (SqlValue)c.obs[val];
            var nv = vl.Having(c, dm);
            var o1 = (SqlValue)c.obs[op1];
            var n1 = o1.Having(c, dm);
            var o2 = (SqlValue)c.obs[op2];
            var n2 = o2.Having(c, dm);
            return (vl == nv && o1 == n1 && o2 == n2) ? this
                : (SqlValue)c.Add(new SqlFunction(c.GetIid(), c, kind, nv, n1, n2, mod));
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlFunction f)
            {
                if (filter != CTree<long, bool>.Empty)
                {  
                    var fb=f.filter.First();
                    for (var b = filter.First(); b != null; b = b.Next(), fb = fb.Next())
                        if (fb==null || (SqlValue)c.obs[b.key()] is SqlValue le
                            && !le.Match(c, (SqlValue)c.obs[fb.key()]))
                            return false;
                    if (fb != null)
                        return false;
                }
                if (kind != f.kind || mod != f.mod || windowId !=f.windowId)
                    return false;
                if (c.obs[op1] is SqlValue o1 && !o1.Match(c, (SqlValue)c.obs[f.op1]))
                    return false;
                if (c.obs[op2] is SqlValue o2 && !o2.Match(c, (SqlValue)c.obs[f.op2]))
                    return false; 
                if (c.obs[val] is SqlValue vl && !vl.Match(c, (SqlValue)c.obs[f.val]))
                    return false;
                if (window!=null || f.window!=null)
                    return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (val >= 0) r += cx.obs[val]._Rdc(cx);
            if (op1 >= 0) r += cx.obs[op1]._Rdc(cx);
            if (op2 >= 0) r += cx.obs[op2]._Rdc(cx);
            return r;
        }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            var vl = (SqlValue)cx.obs[val];
            var o1 = (SqlValue)cx.obs[op1];
            var o2 = (SqlValue)cx.obs[op2];
            return vl?.ConstantIn(cx, rs)!=false|| o1?.ConstantIn(cx, rs) != false
                || o2?.ConstantIn(cx, rs) != false;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlFunction(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (Filter, cx.Fix(filter));
            r += (Op1, cx.Fix(op1));
            r += (Op2, cx.Fix(op2));
            r += (_Val, cx.Fix(val));
            r += (WindowId, cx.Fix(windowId));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (SqlFunction)base._Fix(cx);
            var nf = cx.Fix(filter);
            if (filter !=nf)
                r += (cx, Filter, nf);
            var n1 = cx.Fix(op1);
            if (op1 !=n1)
                r += (cx, Op1, n1);
            var n2 = cx.Fix(op2);
            if (op2 !=n2)
                r += (cx, Op2, n2);
            var w = cx.Fix(filter);
            if (w != filter)
                r += (cx, Filter, w);
            var nv = cx.Fix(val);
            if (val !=nv)
                r += (cx, _Val, nv);
            var ni = cx.Fix(windowId);
            if (windowId !=ni)
                r += (cx, WindowId, ni);
            return cx.Add(r);
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
            if (aggregates(kind))
                r += (cx, Await, await + (q, true));
            return (SqlValue)cx.Add(r);
        }
        static CTree<long,bool> _Deps(SqlValue vl,SqlValue o1,SqlValue o2)
        {
            var r = CTree<long, bool>.Empty;
            if (vl != null)
                r += (vl.defpos, true);
            if (o1 != null)
                r += (o1.defpos, true);
            if (o2 != null)
                r += (o2.defpos, true);
            return r;
        }
        internal override (SqlValue,RowSet) Resolve(Context cx, RowSet fm)
        {
            SqlValue vl = null, o1 = null, o2 = null;
            (vl,fm) = ((SqlValue)cx.obs[val])?.Resolve(cx, fm)??(vl,fm);
            (o1,fm) = ((SqlValue)cx.obs[op1])?.Resolve(cx, fm)??(o1,fm);
            (o2,fm) = ((SqlValue)cx.obs[op2])?.Resolve(cx, fm)??(o2,fm);
            var ki = cx._Dom(this).kind;
            if ((vl?.defpos??-1L) != val || (o1?.defpos??-1L) != op1 || 
                (o2?.defpos??-1L) != op2 || ki==Sqlx.CONTENT || ki==Sqlx.UNION)
                return ((SqlValue)cx.Replace(this,
                    new SqlFunction(iix, cx, kind, vl, o1, o2, mod, mem)),fm);
            return (this,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlFunction)base._Replace(cx,so,sv);
            var w = r.filter;
            var de = r.depth;
            for (var b = w.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)cx._Replace(b.key(), so, sv);
                if (v.defpos != b.key())
                    w += (b.key(), true);
                de = Math.Max(de, v.depth);
            }
            if (w != r.filter)
            {
                r += (cx, Filter, w);
                if (de != r.depth)
                    r += (cx, _Depth, de);
            }
            var o1 = cx.ObReplace(r.op1, so, sv);
            if (o1 != r.op1)
                    r += (cx, Op1, o1);
            var o2 = cx.ObReplace(r.op2, so, sv);
            if (o2 != r.op2)
                r += (cx, Op2, o2);
            var vl = cx.ObReplace(r.val, so, sv);
            if (vl != r.val)
                r += (cx, _Val, vl);
            var dt = cx._Dom(this);
            if (dt.kind==Sqlx.UNION || dt.kind==Sqlx.CONTENT)
            {
                var dm = _Type(cx, kind, cx._Ob(val) as SqlValue, cx._Ob(op1) as SqlValue);
                if (dm!=null && dm!= dt)
                    r += (cx,_Domain, dm.defpos);
            }
            if (r!=this)
                r = (SqlFunction)New(cx, r.mem);
            cx.done += (defpos, r);
            return cx.Add(r);
        }
        /// <summary>
        /// Prepare Window Function evaluation
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal SqlFunction RowSets(Context cx,RowSet q)
        {
            // we first compute the needs of this window function
            // The key for registers will consist of partition/grouping columns
            // Each register has a rowset ordered by the order columns if any
            // for the moment we just use the whole source row
            // We build all of the WRS's at this stage for saving in f
            return this+(cx,Window,window +(WindowSpecification.PartitionType, Domain.Row));
        }
        internal override bool Grouped(Context cx,GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[val])?.Grouped(cx, gs)!=false &&
            ((SqlValue)cx.obs[op1])?.Grouped(cx, gs)!=false &&
            ((SqlValue)cx.obs[op2])?.Grouped(cx, gs)!=false;
        }
        internal override bool AggedOrGrouped(Context cx, RowSet r)
        {
            return base.AggedOrGrouped(cx, r);
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if ((SqlValue)cx.obs[val] is SqlValue sv)
                r += sv.Operands(cx);
            if ((SqlValue)cx.obs[op1] is SqlValue s1)
                r += s1.Operands(cx);
            if ((SqlValue)cx.obs[op2] is SqlValue s2)
                r += s2.Operands(cx);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            if (aggregates(kind) && cx._Dom(q).aggs.Contains(defpos))
                return true;
            return ((SqlValue)cx.obs[val])?.KnownBy(cx, q) != false &&
            ((SqlValue)cx.obs[op1])?.KnownBy(cx, q) != false &&
            ((SqlValue)cx.obs[op2])?.KnownBy(cx, q) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            if (cs.Contains(defpos))
                return true;
            return ((SqlValue)cx.obs[val])?.KnownBy(cx, cs)!=false &&
            ((SqlValue)cx.obs[op1])?.KnownBy(cx, cs)!=false &&
            ((SqlValue)cx.obs[op2])?.KnownBy(cx, cs) !=false;  
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (val >= 0)
            {
                r += ((SqlValue)cx.obs[val]).KnownFragments(cx, kb);
                y = y && r.Contains(val);
            }
            if (op1 >= 0)
            {
                r += ((SqlValue)cx.obs[op1]).KnownFragments(cx, kb);
                y = y && r.Contains(op1);
            }
            if (op2 >= 0)
            {
                r += ((SqlValue)cx.obs[op2]).KnownFragments(cx, kb);
                y = y && r.Contains(op2);
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        internal override Domain _Dom(Context cx)
        {
            return _Type(cx,kind,(SqlValue)cx.obs[val],(SqlValue)cx.obs[op1]);
        }
        internal static Domain _Type(Context cx,Sqlx kind,SqlValue val, SqlValue op1)
        {
            switch (kind)
            {
                case Sqlx.ABS: return cx._Dom(val)??Domain.UnionNumeric;
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AVG: return Domain.Numeric;
                case Sqlx.ARRAY: return Domain.Collection; 
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CAST: return cx._Dom(op1);
                case Sqlx.CEIL: return cx._Dom(val)?? Domain.UnionNumeric;
                case Sqlx.CEILING: return cx._Dom(val)?? Domain.UnionNumeric;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: return Domain.Rvv;
                case Sqlx.COLLECT: return Domain.Collection;
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.CURRENT_USER: return Domain.Char;
                case Sqlx.ELEMENT: return cx._Dom(val)?.elType;
                case Sqlx.FIRST: return Domain.Content;
                case Sqlx.EXP: return Domain.Real;
                case Sqlx.EVERY: return Domain.Bool;
                case Sqlx.EXTRACT: return Domain.Int;
                case Sqlx.FLOOR: return cx._Dom(val) ?? Domain.UnionNumeric;
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
                case Sqlx.MOD: return cx._Dom(val) ?? Domain.UnionNumeric;
                case Sqlx.NEXT: return cx._Dom(val) ?? Domain.UnionDate;
                case Sqlx.NORMALIZE: return Domain.Char;
                case Sqlx.NULLIF: return cx._Dom(op1);
                case Sqlx.OCTET_LENGTH: return Domain.Int;
                case Sqlx.OVERLAY: return Domain.Char;
                case Sqlx.PARTITION: return Domain.Char;
                case Sqlx.POSITION: return Domain.Int;
                case Sqlx.PROVENANCE: return Domain.Char;
                case Sqlx.POWER: return Domain.Real;
                case Sqlx.RANK: return Domain.Int;
                case Sqlx.ROW_NUMBER: return Domain.Int;
                case Sqlx.SET: return Domain.Collection;
                case Sqlx.SQRT: return Domain.Real;
                case Sqlx.STDDEV_POP: return Domain.Real;
                case Sqlx.STDDEV_SAMP: return Domain.Real;
                case Sqlx.SUBSTRING: return Domain.Char;
                case Sqlx.SUM: return cx._Dom(val) ?? Domain.UnionNumeric;
                case Sqlx.TRANSLATE: return Domain.Char;
                case Sqlx.TYPE_URI: return Domain.Char;
                case Sqlx.TRIM: return Domain.Char;
                case Sqlx.UPPER: return Domain.Char;
                case Sqlx.VERSIONING: return (val==null)?Domain.Int:Domain.Rvv;
                case Sqlx.WHEN: return cx._Dom(val);
                case Sqlx.XMLCAST: return cx._Dom(op1);
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return Domain.Null;
        }
        internal override TypedValue Eval(Context cx)
        {
            RTreeBookmark firstTie = null;
            if (cx.values.Contains(defpos))
                return cx.values[defpos];
            Register fc = null;
            if (window != null)
            {
                PRow ks = null;
                var aw = (RowSet)cx.obs[await.First().key()];
                var gc = cx.groupCols[cx._Dom(aw)];
                var key = (gc == null) ? TRow.Empty : new TRow(gc, cx.values);
                fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
                for (var b = window.order.Last(); b != null; b = b.Previous())
                    ks = new PRow(cx.obs[b.value()].Eval(cx), ks);
                fc.wrb = firstTie = fc.wtree.PositionAt(cx, ks); 
                for (var b = fc.wtree.First(cx);
                    b != null; b = b.Next(cx) as RTreeBookmark)
                {
                    if (InWindow(cx,b,fc))
                        switch (window.exclude)
                        {
                            case Sqlx.NO:
                                AddIn(b,cx);
                                break;
                            case Sqlx.CURRENT:
                                if (b._pos != fc.wrb._pos)
                                    AddIn(b,cx);
                                break;
                            case Sqlx.TIES:
                                if (fc.wrb.CompareTo(b)!=0)
                                    AddIn(b,cx);
                                break;
                        }
                }
            }
            else if (aggregates(kind))
            {
                var aw = (RowSet)cx.obs[await.First().key()];
                var gc = cx.groupCols[cx._Dom(aw)];
                var key = (gc == null) ? TRow.Empty : new TRow(gc, cx.values);
                fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx, key);
            }
            TypedValue v = null;
            var dataType = cx._Dom(this);
            var vl = (SqlValue)cx.obs[val];
            var vd = cx._Dom(vl);
            switch (kind)
            {
                case Sqlx.ABS:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    switch (vd.kind)
                    {
                        case Sqlx.INTEGER:
                            {
                                var w = v.ToLong();
                                return new TInt((w < 0L) ? -w : w);
                            }
                        case Sqlx.REAL:
                            {
                                var w = v.ToDouble();
                                return new TReal((w < 0.0) ? -w : w);
                            }
                        case Sqlx.NUMERIC:
                            {
                                Common.Numeric w = (Numeric)v.Val();
                                return new TNumeric((w < Numeric.Zero) ? -w : w);
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
                case Sqlx.ANY: return TBool.For(fc.bval);
                case Sqlx.ARRAY: // Mongo $push
                    {
                        if (window == null || fc.mset == null || fc.mset.Count == 0)
                            return fc.acc;
                        fc.acc = new TArray(
                            (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.ARRAY, 
                                fc.mset.tree?.First()?.key().dataType)));
                        var ar = fc.acc as TArray;
                        for (var d = fc.mset.tree.First();d!= null;d=d.Next())
                            ar+=d.key();
                        return fc.acc;
                    }
                case Sqlx.AVG:
                    { 
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.NUMERIC: return new TReal(fc.sumDecimal / new Common.Numeric(fc.count));
                            case Sqlx.REAL: return new TReal(fc.sum1 / fc.count);
                            case Sqlx.INTEGER:
                                if (fc.sumInteger != null)
                                    return new TReal(new Common.Numeric(fc.sumInteger, 0) / new Common.Numeric(fc.count));
                                return new TReal(new Common.Numeric(fc.sumLong) / new Common.Numeric(fc.count));
                        }
                        return cx._Dom(this).defaultValue;
                    }
                case Sqlx.CARDINALITY:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CAST:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        return cx._Dom(cx.obs[op1]).Coerce(cx,v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    switch (cx._Dom(vl).kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Ceiling(v.ToDouble()));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Ceiling((Common.Numeric)v.Val()));
                    }
                    break;
                case Sqlx.CHAR_LENGTH:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return dataType.defaultValue;
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK: 
                    {
                        var rv = Rvv.Empty;
                        for (var b = ((RowSet)cx.obs[from]).rsTargets.First(); b != null; b = b.Next())
                            if (cx.cursors[b.value()] is Cursor c)
                                rv += (b.key(), c._ds[b.key()]);
                        return new TRvv(rv);
                    }
                case Sqlx.COLLECT: return dataType.Coerce(cx,(TypedValue)fc.mset ??TNull.Value);
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
                case Sqlx.COUNT: return new TInt(fc.count);
                case Sqlx.CURRENT:
                    {
                        if (vl.Eval(cx) is Cursor tc && cx.values[tc._rowsetpos] is Cursor tq)
                            return TBool.For(tc._pos == tq._pos);
                        break;
                    }
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE: return new TChar(cx.db.role.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.CURRENT_USER: return new TChar(cx.db.user?.name??"");
                case Sqlx.ELEMENT:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        if (m.Count != 1)
                            throw new DBException("21000").Mix();
                        return m.tree.First().key();
                    }
                case Sqlx.EXP:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    if (v.IsNull)
                        return dataType.defaultValue;
                    return new TReal(Math.Exp(v.ToDouble()));
                case Sqlx.EVERY:
                    {
                        object o = fc.mset.tree[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Sqlx.EXTRACT:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        switch (v.dataType.kind)
                        {
                            case Sqlx.DATE:
                                {
                                    DateTime dt = (DateTime)v.Val();
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
                                    Interval it = (Interval)v.Val();
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
                case Sqlx.FIRST:  return fc.mset.tree.First().key();
                case Sqlx.FLOOR:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    if (v.Val() == null)
                        return v;
                    switch (cx._Dom(vl).kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble()));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val()));
                    }
                    break;
                case Sqlx.FUSION: return dataType.Coerce(cx,fc.mset);
                case Sqlx.INTERSECTION:return dataType.Coerce(cx,fc.mset);
                case Sqlx.LAST: return fc.mset.tree.Last().key();
                case Sqlx.LAST_DATA:
                    return new TInt(((RowSet)cx.obs[from])?.lastData??0L);
                case Sqlx.LN:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    if (v.Val() == null)
                        return v;
                    return new TReal(Math.Log(v.ToDouble()));
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return dataType.defaultValue;
                    }
                case Sqlx.MAX: return fc.acc;
                case Sqlx.MIN: return fc.acc;
                case Sqlx.MOD:
                    {
                        var o1 = (SqlValue)cx.obs[op1];
                        if (o1 != null)
                            v = o1.Eval(cx);
                        if (v.Val() == null)
                            return dataType.defaultValue;
                        switch (cx._Dom(o1).kind)
                        {
                            case Sqlx.INTEGER:
                                return new TInt(v.ToLong() % cx.obs[op2].Eval(cx).ToLong());
                            case Sqlx.NUMERIC:
                                return new TNumeric(((Numeric)v.Val())
                                    % (Numeric)cx.obs[op2].Eval(cx).Val());
                        }
                        break;
                    }
                case Sqlx.NORMALIZE:
                    if (val != -1L)
                        v = cx.obs[val].Eval(cx);
                    return v; //TBD
                case Sqlx.NULLIF:
                    {
                        TypedValue a = cx.obs[op1].Eval(cx);
                        if (a == null)
                            return null;
                        if (a.IsNull)
                            return dataType.defaultValue;
                        TypedValue b = cx.obs[op2].Eval(cx);
                        if (b == null)
                            return null;
                        if (b.IsNull || a.dataType.Compare(a, b) != 0)
                            return a;
                        return dataType.defaultValue;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (v.Val() is byte[] bytes)
                            return new TInt(bytes.Length);
                        return dataType.defaultValue;
                    }
                case Sqlx.OVERLAY:
                    v = vl?.Eval(cx);
                    return v; //TBD
                case Sqlx.PARTITION:
                        return TNull.Value;
                case Sqlx.POSITION:
                    {
                        if (op1 != -1L && op2 != -1L)
                        {
                            string t = cx.obs[op1].Eval(cx)?.ToString();
                            string s = cx.obs[op2].Eval(cx)?.ToString();
                            if (t != null && s != null)
                                return new TInt(s.IndexOf(t));
                            return dataType.defaultValue;
                        }
                        return TNull.Value;
                    }
                case Sqlx.POWER:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        var w = cx.obs[op1]?.Eval(cx);
                        if (w == null)
                            return null;
                        if (v.IsNull)
                            return dataType.defaultValue;
                        return new TReal(Math.Pow(v.ToDouble(), w.ToDouble()));
                    }
                case Sqlx.PROVENANCE:
                    return cx.cursors[from]?[Domain.Provenance]??TNull.Value;
                case Sqlx.RANK:
                    return new TInt(firstTie._pos + 1);
                case Sqlx.ROW_NUMBER: return new TInt(fc.wrb._pos+1);
                case Sqlx.SECURITY:
                    return cx.cursors[from]?[Classification]?? TLevel.D;
                case Sqlx.SET:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set();
                    }
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SQRT:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        return new TReal(Math.Sqrt(v.ToDouble()));
                    }
                case Sqlx.STDDEV_POP:
                    {
                        if (fc.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        double m = fc.sum1 / fc.count;
                        return new TReal(Math.Sqrt((fc.acc1 - 2 * fc.count * m + fc.count * m * m)
                            / fc.count));
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (fc.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        double m = fc.sum1 / fc.count;
                        return new TReal(Math.Sqrt((fc.acc1 - 2 * fc.count * m + fc.count * m * m)
                            / (fc.count - 1)));
                    }
                case Sqlx.SUBSTRING:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        string sv = v.ToString();
                        var w = cx.obs[op1]?.Eval(cx)??null;
                        if (sv == null || w == null)
                            return dataType.defaultValue;
                        var x = cx.obs[op2]?.Eval(cx)??null;
                        if (x == null)
                            return new TChar((w == null || w.IsNull) ? null : sv.Substring(w.ToInt().Value));
                        return new TChar(sv.Substring(w.ToInt().Value, x.ToInt().Value));
                    }
                case Sqlx.SUM:
                    {
                        switch (fc?.sumType.kind??Sqlx.NO)
                        {
                            case Sqlx.Null: return TNull.Value;
                            case Sqlx.NULL: return TNull.Value;
                            case Sqlx.REAL: return new TReal(fc.sum1);
                            case Sqlx.INTEGER:
                                if (fc.sumInteger != null)
                                    return new TInteger(fc.sumInteger);
                                else
                                    return new TInt(fc.sumLong);
                            case Sqlx.NUMERIC: return new TNumeric(fc.sumDecimal);
                        }
                        return TNull.Value;
                    }
                case Sqlx.TRANSLATE:
                     v = vl?.Eval(cx);
                    return v; // TBD
                case Sqlx.TRIM:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (v.IsNull)
                            return dataType.defaultValue;
                        string sv = v.ToString();
                        object c = null;
                        if (op1 != -1L)
                        {
                            string s = cx.obs[op1].Eval(cx).ToString();
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
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (!v.IsNull)
                            return new TChar(v.ToString().ToUpper());
                        return dataType.defaultValue;
                    }
                 case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var vcx = new Context(cx);
                        if (vl !=null)
                        {
                            vcx.result = vl.defpos;
                            return new TRvv(vcx,"");
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
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        TypedValue a = cx.obs[op1].Eval(cx);
                        if (a == TBool.True)
                            return v;
                        return cx.obs[op2].Eval(cx);
                    }
                case Sqlx.XMLAGG: return new TChar(fc.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        object a = cx.obs[op2]?.Eval(cx);
                        object x = cx.obs[val]?.Eval(cx);
                        if (a == null || x == null)
                            return null;
                        string n = XmlConvert.EncodeName(cx.obs[op1].Eval(cx).ToString());
                        string r = "<" + n  + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                 case Sqlx.XMLPI:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    return new TChar("<?" + v + " " + cx.obs[op1].Eval(cx) + "?>");
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
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
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
        string XmlEnc(object a)
        {
            return a.ToString().Replace("&", "&amp;").Replace("<", "&lt;")
                .Replace(">", "&gt;").Replace("\r", "&#x0d;");
        }
        long Extract(Sqlx mod,TypedValue v)
        {
            switch (v.dataType.kind)
            {
                case Sqlx.DATE:
                    {
                        DateTime dt = (DateTime)v.Val();
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
                        Interval it = (Interval)v.Val();
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
            var fc = new Register(cx,key,this);
            fc.acc1 = 0.0;
            fc.mset = null;
            var vl = (SqlValue)cx.obs[val];
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
                    fc.mset = new TMultiset(cx._Dom(vl));
                    break;
                case Sqlx.XMLAGG:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    fc.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window != null)
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
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    fc.sumType = Domain.Content;
                    fc.acc = null;
                    break;
                case Sqlx.STDDEV_POP:
                    fc.acc1 = 0.0;
                    fc.sum1 = 0.0;
                    fc.count = 0L;
                    break; 
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUM:
                    fc.sumType = Domain.Content;
                    fc.sumLong = 0L;
                    fc.sumInteger = null;
                    break;
            }
            return fc;
        }
        internal void AddIn(TRow key,Context cx)
        {
            var fc = cx.funcs[from]?[key]?[defpos] ?? StartCounter(cx,key);
            var vl = (SqlValue)cx.obs[val];
            if (mod == Sqlx.DISTINCT)
            {
                var v = vl.Eval(cx);
                if (v != null)
                {
                    if (fc.mset == null)
                        fc.mset = new TMultiset((Domain)
                            cx.Add(new Domain(cx.GetUid(),Sqlx.MULTISET,v.dataType)));
                    else if (fc.mset.Contains(v))
                        return;
                    fc.mset.Add(v);
                }
            }
            switch (kind)
            {
                case Sqlx.AVG:
                    {
                        var v = vl.Eval(cx);
                        if (v == null)
                            break;
                    }
                    fc.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ANY:
                    {
                        if (window != null)
                            goto case Sqlx.COLLECT;
                        var v = vl.Eval(cx);
                        if (v != null)
                        {
                            if (v.Val() is bool)
                                fc.bval = fc.bval || (bool)v.Val();
                        }
                        break;
                    }
                case Sqlx.COLLECT:
                    {
                        if (vl != null)
                        {
                            if (fc.mset == null && vl.Eval(cx) != null)
                                fc.mset = new TMultiset(cx._Dom(vl));
                            var v = vl.Eval(cx);
                            if (v != null)
                                fc.mset.Add(v);
                        }
                        break;
                    }
                case Sqlx.COUNT:
                    {
                        if (mod == Sqlx.TIMES)
                        {
                            fc.count++;
                            break;
                        }
                        var v = vl.Eval(cx);
                        if (v != null && !v.IsNull)
                            fc.count++;
                    }
                    break;
                case Sqlx.EVERY:
                    {
                        var v = vl.Eval(cx);
                        if (v is TBool vb)
                            fc.bval = fc.bval && vb.value.Value;
                        break;
                    }
                case Sqlx.FIRST:
                    if (vl != null && fc.acc == null)
                        fc.acc = vl.Eval(cx);
                    break;
                case Sqlx.FUSION:
                    {
                        if (fc.mset == null || fc.mset.IsNull)
                        {
                            var vv = vl.Eval(cx);
                            if (vv == null || vv.IsNull)
                                fc.mset = new TMultiset(cx._Dom(cx._Dom(vl).elType)); 
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
                        var v = vl.Eval(cx);
                        var mv = v as TMultiset;
                        if (fc.mset == null || fc.mset.IsNull)
                            fc.mset = mv;
                        else
                            fc.mset = TMultiset.Intersect(fc.mset, mv, true);
                        break;
                    }
                case Sqlx.LAST:
                    fc.acc = vl?.Eval(cx);
                    break;
                case Sqlx.MAX:
                    {
                        TypedValue v = vl.Eval(cx);
                        if (v != null && (fc.acc == null || fc.acc.CompareTo(v) < 0))
                            fc.acc = v;
                        break;
                    }
                case Sqlx.MIN:
                    {
                        TypedValue v = vl.Eval(cx);
                        if (v != null && (!v.IsNull) && (fc.acc == null || fc.acc.CompareTo(v) > 0))
                            fc.acc = v;
                        break;
                    }
                case Sqlx.STDDEV_POP: 
                    {
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
                        var v = vl.Eval(cx);
                        if (v == null || v==TNull.Value)
                            return;
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInteger)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumInteger = ((TInteger)v).ivalue;
                                }
                                else if (v is TInt vc)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumLong = vc.value ?? throw new PEException("PE777");
                                }
                                else if (v is TReal)
                                {
                                    fc.sumType = Domain.Real;
                                    fc.sum1 = ((TReal)v).dvalue;
                                }
                                else if (v is TNumeric)
                                {
                                    fc.sumType = Domain.Numeric;
                                    fc.sumDecimal = ((TNumeric)v).value;
                                }
                                else
                                    throw new DBException("22000", kind);
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (fc.sumInteger == null)
                                        fc.sumInteger = new Integer(fc.sumLong) + a;
                                    else
                                        fc.sumInteger = fc.sumInteger + a;
                                }
                                else if (v is TInt vi)
                                {
                                    var a = vi.value ?? throw new PEException("PE777");
                                    if (fc.sumInteger == null)
                                    {
                                        if ((a > 0) ? (fc.sumLong <= long.MaxValue - a) : (fc.sumLong >= long.MinValue - a))
                                            fc.sumLong += a;
                                        else
                                            fc.sumInteger = new Integer(fc.sumLong) + new Integer(a);
                                    }
                                    else
                                        fc.sumInteger = fc.sumInteger + new Integer(a);
                                }
                                break;
                            case Sqlx.REAL:
                                if (v is TReal)
                                {
                                    fc.sum1 += ((TReal)v).dvalue;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                if (v is TNumeric)
                                {
                                    fc.sumDecimal = fc.sumDecimal + ((TNumeric)v).value;
                                }
                                break;
                        }
                        break;
                    }
                case Sqlx.XMLAGG:
                    {
                        fc.sb.Append(' ');
                        var o = vl.Eval(cx);
                        if (o != null)
                            fc.sb.Append(o.ToString());
                        break;
                    }
            }
            var t1 = cx.funcs[from] ?? BTree<TRow, BTree<long, Register>>.Empty;
            var t2 = t1[key] ?? BTree<long,Register>.Empty;
            t2 += (defpos, fc);
            t1 += (key, t2);
            cx.funcs += (from, t1);
        }
        /// <summary>
        /// Window Functions: bmk is a bookmark in cur.wrs
        /// </summary>
        /// <param name="bmk"></param>
        /// <returns></returns>
        bool InWindow(Context cx, RTreeBookmark bmk, Register fc)
        {
            if (bmk == null)
                return false;
            var tr = cx.db;
            var wn = window;
            if (wn.units == Sqlx.RANGE && !(TestStartRange(cx,bmk,fc) && TestEndRange(cx,bmk,fc)))
                return false;
            if (wn.units == Sqlx.ROWS && !(TestStartRows(cx,bmk,fc) && TestEndRows(cx,bmk,fc)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Context cx, RTreeBookmark bmk,Register fc)
        {
            var wn = window;
            if (wn.high == null || wn.high.unbounded)
                return true;
            long limit;
            if (wn.high.current)
                limit = fc.wrb._pos;
            else if (wn.high.preceding)
                limit = fc.wrb._pos - (wn.high.distance?.ToLong()??0);
            else
                limit = fc.wrb._pos + (wn.high.distance?.ToLong()??0);
            return bmk._pos <= limit; 
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Context cx, RTreeBookmark bmk,Register fc)
        {
            var wn = window;
            if (wn.low == null || wn.low.unbounded)
                return true;
            long limit;
            if (wn.low.current)
                limit =fc.wrb._pos;
            else if (wn.low.preceding)
                limit = fc.wrb._pos - (wn.low.distance?.ToLong() ?? 0);
            else
                limit = fc.wrb._pos + (wn.low.distance?.ToLong() ?? 0);
            return bmk._pos >= limit;
        }

        /// <summary>
        /// Test the window against the end of the given range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRange(Context cx, RTreeBookmark bmk, Register fc)
        {
            var wn = window;
            if (wn.high == null || wn.high.unbounded)
                return true;
            var n = val;
            var kt = cx._Dom(val);
            var wrv = fc.wrb[n];
            TypedValue limit;
            var tr = cx.db as Transaction;
            if (tr == null)
                return false;
            if (wn.high.current)
                limit = wrv;
            else if (wn.high.preceding)
                limit = kt.Eval(defpos,cx,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, 
                    wn.high.distance);
            else
                limit = kt.Eval(defpos,cx,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, 
                    wn.high.distance);
            return kt.Compare(bmk[n], limit) <= 0; 
        }
        /// <summary>
        /// Test a window against the start of a range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRange(Context cx, RTreeBookmark bmk,Register fc)
        {
            var wn = window;
            if (wn.low == null || wn.low.unbounded)
                return true;
            var n = val;
            var kt = cx._Dom(val);
            var tv = fc.wrb[n];
            TypedValue limit;
            var tr = cx.db as Transaction;
            if (tr == null)
                return false;
            if (wn.low.current)
                limit = tv;
            else if (wn.low.preceding)
                limit = kt.Eval(defpos,cx,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS, 
                    wn.low.distance);
            else
                limit = kt.Eval(defpos,cx,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS, 
                    wn.low.distance);
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
                qn = ((SqlValue)cx.obs[val])?.Needs(cx, r, qn) ?? qn;
                qn = ((SqlValue)cx.obs[op1])?.Needs(cx, r, qn) ?? qn;
                qn = ((SqlValue)cx.obs[op2])?.Needs(cx, r, qn) ?? qn;
            }
            return qn;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            switch (kind)
            {
                case Sqlx.CHECK:
                case Sqlx.CURRENT_TIME:
                case Sqlx.CURRENT_TIMESTAMP:
                case Sqlx.LAST_DATA:
                case Sqlx.PROVENANCE:
                case Sqlx.RANK:
                case Sqlx.ROW_COUNT:
                case Sqlx.TYPE_URI:
                case Sqlx.ROW_NUMBER:
                case Sqlx.SECURITY:
                    return false;
                case Sqlx.VERSIONING:
                    if (val < 0)
                        break;
                    return false;
            }
            return (cx.obs[val]?.LocallyConstant(cx, rs) ?? true)
                && (cx.obs[op1]?.LocallyConstant(cx, rs) ?? true)
                && (cx.obs[op1]?.LocallyConstant(cx, rs) ?? true);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
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
        internal override string ToString(string sg, Remotes rf, CList<long> cs,
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
                            vl = (cx.obs[val] as SqlValue)?.ToString(sg, rf, cs, ns, cx);
                            o1 = (cx.obs[op1] as SqlValue)?.ToString(sg, rf, cs, ns, cx);
                            o2 = (cx.obs[op2] as SqlValue)?.ToString(sg, rf, cs, ns, cx);
                        } else
                        {
                            vl = (cx.obs[val] is SqlCopy vc)? ns[vc.copyFrom] : ns[val] ?? "";
                            o1 = (cx.obs[op1] is SqlCopy c1)? ns[c1.copyFrom] : ns[op1] ?? "";
                            o2 = (cx.obs[op2] is SqlCopy c2)? ns[c2.copyFrom] : ns[op2]  ?? "";
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
                                sb.Append(kind); sb.Append('(');
                                sb.Append(vl); sb.Append(" as ");
                                sb.Append(cx._Dom(this).name); sb.Append(')');
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
                            case Sqlx.CURRENT_USER:
                            case Sqlx.LOCALTIME:
                            case Sqlx.LOCALTIMESTAMP:
                            case Sqlx.PROVENANCE:
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
            sb.Append(" ");
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
                sb.Append(")");
            }
            if (window!=null)
                sb.Append(window);
            return sb.ToString();
        }
        internal static bool aggregates(Sqlx kind)
        {
            switch (kind)
            {
                case Sqlx.ANY:
                case Sqlx.ARRAY:
                case Sqlx.AVG:
                case Sqlx.COLLECT:
                case Sqlx.COUNT:
                case Sqlx.EVERY:
                case Sqlx.FIRST:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                case Sqlx.LAST:
                case Sqlx.MAX:
                case Sqlx.MIN:
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
                case Sqlx.SOME:
                case Sqlx.SUM:
                case Sqlx.XMLAGG:
                    return true;
            }
            return false;
        }
    }
    /// <summary>
    /// The Parser converts this n-adic function to a binary one
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(Iix dp, Context cx, SqlValue op1, SqlValue op2)
            : base(dp, cx, Sqlx.COALESCE, null, op1, op2, Sqlx.NO) { }
        protected SqlCoalesce(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator+(SqlCoalesce s,(long,object)x)
        {
            return new SqlCoalesce(s.iix, s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[op1].Eval(cx) is TypedValue lf) ? 
                ((lf.IsNull) ? cx.obs[op2].Eval(cx) : lf) : null;
        }
        internal override SqlValue Having(Context c, Domain dm)
        {
            var le = (SqlValue)c.obs[left];
            var nl = le.Having(c, dm);
            var rg = (SqlValue)c.obs[right];
            var nr = rg.Having(c, dm);
            return (le == nl && rg == nr) ? this :
                (SqlValue)c.Add(new SqlCoalesce(c.GetIid(), c, nl, nr));
        }
        internal override bool Match(Context c, SqlValue v)
        {
            if (v is SqlCoalesce sc)
            {
                var le = (SqlValue)c.obs[left];
                var vl = (SqlValue)c.obs[sc.left];
                var rg = (SqlValue)c.obs[right];
                var vr = (SqlValue)c.obs[sc.right];
                return le.Match(c, vl) && rg.Match(c, vr);
            }
            return false;
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
        internal SqlTypeUri(Iix dp, Context cx, SqlValue op1)
            : base(dp, cx, Sqlx.TYPE_URI, null, op1, null, Sqlx.NO) { }
        protected SqlTypeUri(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeUri operator+(SqlTypeUri s,(long,object)x)
        {
            return new SqlTypeUri(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeUri(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTypeUri(new Iix(iix,dp),mem);
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
            TypedValue v = null;
            if (op1 != -1L)
                v = cx.obs[op1].Eval(cx);
            if (v==null || v.IsNull)
                return cx._Dom(this).defaultValue;
            var st = v.dataType;
            if (st.iri != null)
                return v;
            return cx._Dom(this).defaultValue;
        }
        public override string ToString()
        {
            return "TYPE_URI(..)";
        }
    }
    /// <summary>
    /// Used while parsing a RowSetSpec,
    /// and removed at end of the parse (DoStars)
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class SqlStar : SqlValue
    {
        public readonly long prefix = -1L;
        internal SqlStar(Iix dp, long pf) : base(dp, "*", Domain.Content)
        {
            prefix = pf;
        }
        protected SqlStar(Iix dp, long pf, BTree<long, object> m) : base(dp, m)
        {
            prefix = pf;
        }
        protected SqlStar(Iix dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlStar(iix, prefix, m);
        }
        public static SqlStar operator +(SqlStar s, (long, object) x)
        {
            return (SqlStar)s.New(s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return new TInt(-1L);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlStar(new Iix(iix,dp), mem);
        }
        internal override (SqlValue, RowSet) Resolve(Context cx, RowSet q)
        {
            return (this,q);
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return CTree<long, RowSet.Finder>.Empty;
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
            Vals = -355, //CList<long> SqlValue
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
        internal QuantifiedPredicate(Iix iix,Context cx,SqlValue w, Sqlx o, bool a, 
            RowSet s)
            : base(iix,_Mem(cx,w,s) + (What,w.defpos)+(Op,o)+(All,a)+(_Select,s.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(s.defpos,true))
                  +(_Depth,1+_Max(w.depth,s.depth))) {}
        protected QuantifiedPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,RowSet s)
        {
            var m = BTree<long,object>.Empty;
            var dm = Domain.Bool;
            var ag = w.IsAggregation(cx) + cx._Dom(s).aggs;
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static QuantifiedPredicate operator+(QuantifiedPredicate s,(long,object)x)
        {
            return new QuantifiedPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuantifiedPredicate(iix, m);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (what >= 0) r += cx.obs[what]._Rdc(cx);
            if (select >= 0) r += cx.obs[select]._Rdc(cx);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuantifiedPredicate(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (QuantifiedPredicate)base._Relocate(cx);
            r += (What, cx.Fix(what));
            r += (_Select, cx.Fix(select));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (QuantifiedPredicate)base._Fix(cx);
            var nw = cx.Fix(what);
            if (nw != what)
                r += (What, nw);
            var ns = cx.Fix(select);
            if (ns != select)
                r += (_Select, ns);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuantifiedPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(r.what,so,sv);
            if (wh != r.what)
                r += (What, wh);
            var se = cx.ObReplace(r.select, so, sv);
            if (se != r.select)
                r += (_Select, se);
            if (r!=this)
                r = (QuantifiedPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[what])?.Grouped(cx, gs) != false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (QuantifiedPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[r.what]).AddFrom(cx, q);
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
            var w = (SqlValue)cx.obs[what];
            var s = (RowSet)cx.obs[select];
            switch (op)
            {
                case Sqlx.EQL: return new QuantifiedPredicate(iix, cx, w, Sqlx.NEQ, !all, s);
                case Sqlx.NEQ: return new QuantifiedPredicate(iix, cx, w, Sqlx.EQL, !all, s);
                case Sqlx.LEQ: return new QuantifiedPredicate(iix, cx, w, Sqlx.GTR, !all, s);
                case Sqlx.LSS: return new QuantifiedPredicate(iix, cx, w, Sqlx.GEQ, !all, s);
                case Sqlx.GEQ: return new QuantifiedPredicate(iix, cx, w, Sqlx.LSS, !all, s);
                case Sqlx.GTR: return new QuantifiedPredicate(iix, cx, w, Sqlx.LEQ, !all, s);
                default: throw new PEException("PE65");
            }
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = cx.obs[what].Needs(cx, rs);
            r += cx.obs[select].Needs(cx, rs);
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
            var wv = (SqlValue)cx.obs[what];
            for (var rb = ((RowSet)cx.obs[select]).First(cx); rb != null; rb = rb.Next(cx))
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
                    return null;
            }
            return TBool.For(all);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what]).StartCounter(cx, rs, tg);
            return ((RowSet)cx.obs[select]).StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what]).AddIn(cx,rb, tg);
            return ((RowSet)cx.obs[select]).AddIn(cx,rb,tg);
        }
        /// <summary>
        /// We aren't a column reference. If the select needs something
        /// From will add it to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what]).Needs(cx,r,qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            return ((SqlValue)cx.obs[what]).KnownBy(cx, cs);
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((SqlValue)cx.obs[what]).KnownBy(cx, q);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (what >= 0)
            {
                r += ((SqlValue)cx.obs[what]).KnownFragments(cx, kb);
                y = y && r.Contains(what);
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
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
        internal BetweenPredicate(Iix iix,Context cx,SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(iix,_Mem(cx,w,a,h)
                  +(QuantifiedPredicate.What,w.defpos)+(QuantifiedPredicate.Between,b)
                  +(QuantifiedPredicate.Low,a.defpos)+(QuantifiedPredicate.High,h.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(a.defpos,true)+(h.defpos,true))
                  +(_Depth,1+_Max(w.depth,a.depth,h.depth)))
        { }
        protected BetweenPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
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
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static BetweenPredicate operator+(BetweenPredicate s,(long,object)x)
        {
            return new BetweenPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BetweenPredicate(iix,m);
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
            var wh = (SqlValue)cx.obs[what];
            var nw = wh?.Having(cx,dm);
            var lw = (SqlValue)cx.obs[low];
            var nl = lw?.Having(cx,dm);
            var hg = (SqlValue)cx.obs[high];
            var nh = hg?.Having(cx,dm);
            return (wh == nw && lw == nl && hg == nh) ? this :
                (SqlValue)cx.Add(new BetweenPredicate(cx.GetIid(), cx, nw, between, nl, nh));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is BetweenPredicate that)
            {
                if (between != that.between)
                    return false;
                if (cx.obs[what] is SqlValue w)
                    if (!w.Match(cx, (SqlValue)cx.obs[that.what]))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[low] is SqlValue lw)
                    if (!lw.Match(cx, (SqlValue)cx.obs[that.low]))
                        return false;
                    else if (that.what >= 0)
                        return false;
                if (cx.obs[high] is SqlValue hg)
                    if (!hg.Match(cx, (SqlValue)cx.obs[that.high]))
                        return false;
                    else if (that.what >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            var wh = (SqlValue)cx.obs[what];
            var lw = (SqlValue)cx.obs[low];
            var hg = (SqlValue)cx.obs[high];
            return wh?.ConstantIn(cx, rs) != false || lw?.ConstantIn(cx, rs) != false
                || hg?.ConstantIn(cx, rs) != false;
        }
        internal override bool WellDefinedOperands(Context cx)
        {
            if (cx.obs[what] is SqlValue wh && !wh.WellDefinedOperands(cx))
                return false; 
            if (cx.obs[low] is SqlValue lw && !lw.WellDefinedOperands(cx))
                return false; 
            if (cx.obs[high] is SqlValue hi && !hi.WellDefinedOperands(cx))
                return false;
            return base.WellDefinedOperands(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new BetweenPredicate(new Iix(iix,dp),mem);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long,bool>.Empty;
            if (what >= 0) r += cx.obs[what]._Rdc(cx);
            if (low >= 0) r += cx.obs[low]._Rdc(cx);
            if (high >= 0) r += cx.obs[high]._Rdc(cx);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (BetweenPredicate)base._Relocate(cx);
            r += (QuantifiedPredicate.What, cx.Fix(what));
            r += (QuantifiedPredicate.Low, cx.Fix(low));
            r += (QuantifiedPredicate.High, cx.Fix(high));
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[what])?.Grouped(cx, gs) != false &&
            ((SqlValue)cx.obs[low])?.Grouped(cx, gs) != false &&
            ((SqlValue)cx.obs[high])?.Grouped(cx, gs) != false;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (BetweenPredicate)base._Fix(cx);
            var nw = cx.Fix(what);
            if (what!=nw)
                r += (QuantifiedPredicate.What, cx.Fix(what));
            var nl = cx.Fix(low);
            if (low!=nl)
                r += (QuantifiedPredicate.Low, nl);
            var nh = cx.Fix(high);
            if (high !=nh)
                r += (QuantifiedPredicate.High, nh);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (BetweenPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(r.what, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = cx.ObReplace(r.low, so, sv);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = cx.ObReplace(r.high, so, sv);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            if (r!=this)
                r = (BetweenPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return (((SqlValue)cx.obs[low])?.KnownBy(cx, q)??true)
                && (((SqlValue)cx.obs[high])?.KnownBy(cx, q)??true)
                && ((SqlValue)cx.obs[what]).KnownBy(cx, q);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            return (((SqlValue)cx.obs[low])?.KnownBy(cx, cs) ?? true)
                && (((SqlValue)cx.obs[high])?.KnownBy(cx, cs) ?? true)
                && ((SqlValue)cx.obs[what]).KnownBy(cx, cs);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (low >= 0)
            {
                r += ((SqlValue)cx.obs[low]).KnownFragments(cx, kb);
                y = y && r.Contains(low);
            }
            if (high >= 0)
            {
                r += ((SqlValue)cx.obs[high]).KnownFragments(cx, kb);
                y = y && r.Contains(high);
            } 
            if (what >= 0)
            {
                r += ((SqlValue)cx.obs[what]).KnownFragments(cx, kb);
                y = y && r.Contains(what);
            } 
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
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
            return new BetweenPredicate(iix,cx,(SqlValue)cx.obs[what], !between, 
                (SqlValue)cx.obs[low], (SqlValue)cx.obs[high]);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what])?.StartCounter(cx,rs,tg)??tg;
            tg = ((SqlValue)cx.obs[low])?.StartCounter(cx,rs,tg)??tg;
            tg = ((SqlValue)cx.obs[high])?.StartCounter(cx,rs,tg)??tg;
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what])?.AddIn(cx,rb, tg)??tg;
            tg = ((SqlValue)cx.obs[low])?.AddIn(cx,rb, tg)??tg;
            tg = ((SqlValue)cx.obs[high])?.AddIn(cx,rb, tg)??tg;
            return tg;
        }
        internal override void OnRow(Context cx,Cursor bmk)
        {
            ((SqlValue)cx.obs[what])?.OnRow(cx,bmk);
            ((SqlValue)cx.obs[low])?.OnRow(cx,bmk);
            ((SqlValue)cx.obs[high])?.OnRow(cx,bmk);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = cx.obs[what]?.Needs(cx, rs)??CTree<long,RowSet.Finder>.Empty;
            r += cx.obs[low]?.Needs(cx, rs) ?? CTree<long, RowSet.Finder>.Empty;
            r += cx.obs[high]?.Needs(cx, rs) ?? CTree<long, RowSet.Finder>.Empty;
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[what].LocallyConstant(cx, rs)
                && (cx.obs[low]?.LocallyConstant(cx,rs)??true)
                && (cx.obs[high]?.LocallyConstant(cx, rs) ?? true);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var wv = (SqlValue)cx.obs[what];
            if (wv.Eval(cx) is TypedValue w)
            {
                var t = cx._Dom(wv);
                if (cx.obs[low].Eval(cx) is TypedValue lw)
                {
                    if (t.Compare(w, t.Coerce(cx,lw)) < 0)
                        return TBool.False;
                    if (cx.obs[high].Eval(cx) is TypedValue hg)
                        return TBool.For(t.Compare(w, t.Coerce(cx,hg)) <= 0);
                }
            }
            return null;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what])?.Needs(cx,r,qn)??qn;
            qn = ((SqlValue)cx.obs[low])?.Needs(cx,r,qn)??qn;
            qn = ((SqlValue)cx.obs[high])?.Needs(cx,r,qn)??qn;
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(what.ToString());
            sb.Append(" between ");
            sb.Append(low.ToString());
            sb.Append(" and ");
            sb.Append(high.ToString());
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
        internal LikePredicate(Iix dp,Context cx,SqlValue a, bool k, SqlValue b, SqlValue e)
            : base(dp, _Mem(cx,a,b,e) + (Left,a.defpos)+(_Like,k)+(Right,b.defpos)
                  +(_Domain,Domain.Bool.defpos)
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true))
                  +(_Depth,1+_Max(a.depth,b.depth,e?.depth??0)))
        { }
        protected LikePredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, SqlValue a, SqlValue b,SqlValue e)
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
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static LikePredicate operator+(LikePredicate s,(long,object)x)
        {
            return new LikePredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LikePredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new LikePredicate(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (Escape, cx.Fix(escape));
            return r;
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var le = (SqlValue)cx.obs[left];
            var nl = le?.Having(cx, dm);
            var rg = (SqlValue)cx.obs[right];
            var nr = rg?.Having(cx, dm);
            var es = (SqlValue)cx.obs[escape];
            var ne = es?.Having(cx, dm);
            return (le == nl && rg == nr && es == ne) ? this :
                (SqlValue)cx.Add(new LikePredicate(cx.GetIid(), cx, nl, like, nr, ne));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is LikePredicate that)
            {
                if (like != that.like)
                    return false;
                if (cx.obs[left] is SqlValue le)
                    if (!le.Match(cx, (SqlValue)cx.obs[that.left]))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is SqlValue rg)
                    if (!rg.Match(cx, (SqlValue)cx.obs[that.right]))
                        return false;
                    else if (that.right >= 0)
                        return false;
                if (cx.obs[escape] is SqlValue es)
                    if (!es.Match(cx, (SqlValue)cx.obs[that.escape]))
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
            if (escape >= 0) r += cx.obs[escape]._Rdc(cx);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (LikePredicate)base._Fix(cx);
            var ne = cx.Fix(escape);
            if (escape!=ne)
                r += (Escape, ne);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LikePredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(r.left, so, sv);
            if (wh != r.left)
                r += (Left, wh);
            var rg = cx.ObReplace(r.right, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            var esc = cx.ObReplace(r.escape, so, sv);
            if (esc != r.escape)
                r += (Escape, esc);
            if (r!=this)
                r = (LikePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[escape])?.Grouped(cx, gs) != false &&
            ((SqlValue)cx.obs[left]).Grouped(cx, gs) != false &&
            ((SqlValue)cx.obs[right]).Grouped(cx, gs)!=false;
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
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((SqlValue)cx.obs[left]).KnownBy(cx, q)
                && ((SqlValue)cx.obs[right]).KnownBy(cx, q)
                && ((SqlValue)cx.obs[escape])?.KnownBy(cx, q) != false;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            return ((SqlValue)cx.obs[left]).KnownBy(cx, cs)
                && ((SqlValue)cx.obs[right]).KnownBy(cx, cs)
                && ((SqlValue)cx.obs[escape])?.KnownBy(cx, cs) != false;
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (left >= 0)
            {
                r += ((SqlValue)cx.obs[left]).KnownFragments(cx, kb);
                y = y && r.Contains(left);
            }
            if (right >= 0)
            {
                r += ((SqlValue)cx.obs[right]).KnownFragments(cx, kb);
                y = y && r.Contains(right);
            }
            if (escape >= 0)
            {
                r += ((SqlValue)cx.obs[escape]).KnownFragments(cx, kb);
                y = y && r.Contains(escape);
            } 
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            return new LikePredicate(iix,cx,(SqlValue)cx.obs[left], !like, 
                (SqlValue)cx.obs[right], (SqlValue)cx.obs[escape]);
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
        internal override TypedValue Eval(Context cx)
        {
            bool r = false;
            if (cx.obs[left].Eval(cx) is TypedValue lf && 
                cx.obs[right].Eval(cx) is TypedValue rg)
            {
                if (lf.IsNull && rg.IsNull)
                    r = true;
                else if ((!lf.IsNull) & !rg.IsNull)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (escape != -1L)
                        e = cx.obs[escape].Eval(cx).ToString();
                    if (e.Length != 1)
                        throw new DBException("22020").ISO(); // invalid escape character
                    r = Like(a, b, e[0]);
                }
                if (!like)
                    r = !r;
                return r ? TBool.True : TBool.False;
            }
            return null;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, 
            BTree<long, Register> tg)
        {
            tg = cx.obs[left].StartCounter(cx, rs, tg);
            tg = cx.obs[right].StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = cx.obs[left].AddIn(cx,rb, tg);
            tg = cx.obs[right].AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[left]).Needs(cx,r,qn);
            qn = ((SqlValue)cx.obs[right]).Needs(cx,r,qn);
            qn = ((SqlValue)cx.obs[escape])?.Needs(cx,r,qn) ?? qn;
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = cx.obs[left].Needs(cx, rs);
            r += cx.obs[right].Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[left].LocallyConstant(cx, rs)
            && cx.obs[right].LocallyConstant(cx, rs);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(" "); sb.Append(Uid(defpos));
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
        public CList<long> vals => (CList<long>)mem[QuantifiedPredicate.Vals]??CList<long>.Empty;
        public InPredicate(Iix dp,Context cx, SqlValue w, BList<SqlValue> vs = null) 
            : base(dp, _Mem(cx,w,vs)
                  +(Dependents,_Deps(vs)+(w.defpos,true))+(_Depth,_Dep(w,vs)))
        {}
        protected InPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx,SqlValue w,BList<SqlValue> vs)
        {
            var m = BTree<long,object>.Empty;
            var cs = CList<long>.Empty;
            var dm = Domain.Bool;
            var ag = CTree<long, bool>.Empty;
            if (w!=null)
            {
                ag = w.IsAggregation(cx);
                m += (QuantifiedPredicate.What, w.defpos);
            }
            if (vs != null)
            {
                for (var b = vs.First(); b != null; b = b.Next())
                {
                    var s = b.value();
                    cs += s.defpos;
                    ag += s.IsAggregation(cx);
                }
                m += (QuantifiedPredicate.Vals, cs);
            }
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            return ((SqlValue)cx.obs[what]).IsAggregation(cx);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var wh = (SqlValue)cx.obs[what];
            var nw = wh?.Having(cx, dm);
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=vals.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)cx.obs[b.value()];
                var nv = v.Having(cx, dm);
                vs += nv;
                ch = ch || nv != v;
            }
            return (wh == nw && !ch) ? this :
                (SqlValue)cx.Add(new InPredicate(cx.GetIid(), cx, nw, vs));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is InPredicate that)
            {
                if (found != that.found)
                    return false;
                if (cx.obs[what] is SqlValue w)
                    if (!w.Match(cx, (SqlValue)cx.obs[that.what]))
                        return false;
                    else if (that.what >= 0)
                        return false;
                var tb = that.vals.First();
                for (var b = vals.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                {
                    var e = (SqlValue)cx.obs[b.value()];
                    var te = (SqlValue)cx.obs[tb.value()];
                    if (!e.Match(cx, te))
                        return false;
                }
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (what >= 0) r += cx.obs[what]._Rdc(cx);
            if (select >= 0) r += cx.obs[select]._Rdc(cx);
            return r;
        }
        static int _Dep(SqlValue w,BList<SqlValue> vs)
        {
            var r = w.depth + 1;
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().depth >= r)
                    r = b.value().depth + 1;
            return r;
        }
        public static InPredicate operator+(InPredicate s,(long,object)x)
        {
            return new InPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new InPredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new InPredicate(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (InPredicate)base._Relocate(cx);
            r += (QuantifiedPredicate.What, cx.Fix(what));
            r += (QuantifiedPredicate._Select, cx.Fix(select));
            r += (QuantifiedPredicate.Vals, cx.Fix(vals));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (InPredicate)base._Fix(cx);
            var nw = cx.Fix(what);
            if (what!=nw)
                r += (QuantifiedPredicate.What, nw);
            var ns = cx.Fix(select);
            if (select!=ns)
                r += (QuantifiedPredicate._Select, ns);
            var nv = cx.Fix(vals);
            if (vals!=nv)
            r += (QuantifiedPredicate.Vals, nv);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InPredicate)base._Replace(cx, so, sv);
            var wh = cx.ObReplace(r.what, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = cx.ObReplace(r.select, so, sv);
            if (wr != r.select)
                r += (QuantifiedPredicate._Select, wr);
            var vs = vals;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = cx.ObReplace(b.value(), so, sv);
                if (v != b.value())
                    vs += (b.key(), v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            if (r!=this)
                r = (InPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool WellDefinedOperands(Context cx)
        {
            if (cx.obs[what] is SqlValue wh && !wh.WellDefinedOperands(cx))
                return false;
            return base.WellDefinedOperands(cx);
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[what]).Grouped(cx, gs) != false &&
            gs.Grouped(cx, vals) != false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (InPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[what]).AddFrom(cx, q);
            if (a.defpos != r.what)
                r += (QuantifiedPredicate.What, a);
            var vs = r.vals;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = ((SqlValue)cx.obs[b.value()]).AddFrom(cx,q);
                if (v.defpos != b.value())
                    ch = true;
                vs += (b.key(), v.defpos);
            }
            if (ch)
                r += (QuantifiedPredicate.Vals, vs);
            return r;
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return ((SqlValue)cx.obs[what]).KnownBy(cx, q);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, cs))
                    return false;
            return ((SqlValue)cx.obs[what]).KnownBy(cx, cs);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            for (var b=vals.First(); b != null; b = b.Next())
            {
                r += ((SqlValue)cx.obs[b.value()]).KnownFragments(cx, kb);
                y = y && r.Contains(b.value());
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = ((SqlValue)cx.obs[what]).Needs(cx, rs);
            if (select>=0)
                r = cx.obs[select].Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return (select >= 0)? false
                : ((SqlValue)cx.obs[what]).LocallyConstant(cx, rs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[what].Eval(cx) is TypedValue w)
            {
                if (vals != CList<long>.Empty)
                {
                    for (var v = vals.First(); v != null; v = v.Next())
                    {
                        var sv = (SqlValue)cx.obs[v.value()];
                        if (cx._Dom(sv).Compare(w, sv.Eval(cx)) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = ((RowSet)cx.obs[select]).First(cx); 
                        rb != null; rb = rb.Next(cx))
                    {
                        if (w.dataType.kind!=Sqlx.ROW)
                        {
                            var v = rb[0];
                            if (w.CompareTo(v) == 0)
                                return TBool.For(found);
                        }
                        else if (w.CompareTo(rb) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
            }
            return null;
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            for (var v = vals?.First(); v != null; v = v.Next())
                tg = cx.obs[v.value()].StartCounter(cx,rs,tg);
            tg = cx.obs[what].StartCounter(cx,rs,tg);
            tg = base.StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                tg = cx.obs[v.value()].AddIn(cx,rb, tg);
            tg = cx.obs[what].AddIn(cx,rb, tg);
            tg = base.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference. If the where has needed
        /// From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx,long r,CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what]).Needs(cx,r,qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Uid(what));
            if (!found)
                sb.Append(" not");
            sb.Append(" in (");
            if (vals != CList<long>.Empty)
            {
                var cm = "";
                for (var b = vals.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
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
        internal MemberPredicate(Iix dp,Context cx,SqlValue a, bool f, SqlValue b)
            : base(dp, _Mem(cx,a,b)
                  +(Lhs,a)+(Found,f)+(Rhs,b)+(_Depth,1+_Max(a.depth,b.depth))
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true)))
        { }
        protected MemberPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = a.IsAggregation(cx) + b.IsAggregation(cx);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static MemberPredicate operator+(MemberPredicate s,(long,object)x)
        {
            return new MemberPredicate(s.iix, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MemberPredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new MemberPredicate(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (MemberPredicate)base._Relocate(cx);
            r += (Lhs, cx.Fix(lhs));
            r += (Rhs, cx.Fix(rhs));
            return r;
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            return ((SqlValue)cx.obs[lhs]).IsAggregation(cx)
                + ((SqlValue)cx.obs[rhs]).IsAggregation(cx);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var lh = (SqlValue)cx.obs[lhs];
            var nl = lh?.Having(cx, dm);
            var rh = (SqlValue)cx.obs[rhs];
            var nr = rh?.Having(cx, dm);
            return (lh == nl && rh == nr) ? this :
                (SqlValue)cx.Add(new MemberPredicate(cx.GetIid(), cx, nl, found, nr));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is MemberPredicate that)
            {
                if (found != that.found)
                    return false;
                if (cx.obs[lhs] is SqlValue lh)
                    if (!lh.Match(cx, (SqlValue)cx.obs[that.lhs]))
                        return false;
                    else if (that.lhs >= 0)
                        return false;
                if (cx.obs[rhs] is SqlValue rh)
                    if (!rh.Match(cx, (SqlValue)cx.obs[that.rhs]))
                        return false;
                    else if (that.rhs >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = base._Rdc(cx);
            if (lhs >= 0) r += cx.obs[lhs]._Rdc(cx);
            if (rhs >= 0) r += cx.obs[rhs]._Rdc(cx);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (MemberPredicate)base._Fix(cx);
            var nl = cx.Fix(lhs);
            if (nl != lhs)
                r += (Lhs, nl);
            var nr = cx.Fix(rhs);
            if (nr != rhs)
                r += (Rhs, rhs);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MemberPredicate)base._Replace(cx, so, sv);
            var lf = cx.ObReplace(lhs,so,sv);
            if (lf != left)
                r += (Lhs,lf);
            var rg = cx.ObReplace(rhs,so,sv);
            if (rg != rhs)
                r += (Rhs,rg);
            if (r!=this)
                r = (MemberPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[lhs]).Grouped(cx, gs) !=false &&
            ((SqlValue)cx.obs[rhs]).Grouped(cx, gs)!=false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (MemberPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[r.lhs]).AddFrom(cx, q);
            if (a.defpos != r.lhs)
                r += (Lhs, a.defpos);
            a = ((SqlValue)cx.obs[r.rhs]).AddFrom(cx, q);
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
            return new MemberPredicate(iix,cx,(SqlValue)cx.obs[lhs], !found, 
                (SqlValue)cx.obs[rhs]);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = cx.obs[lhs].Needs(cx, rs);
            r += cx.obs[rhs].Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[lhs].LocallyConstant(cx,rs) 
                && cx.obs[rhs].LocallyConstant(cx,rs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[lhs].Eval(cx) is TypedValue a && cx.obs[rhs].Eval(cx) is TypedValue b)
            {
                if (b.IsNull)
                    return cx._Dom(this).defaultValue;
                if (a.IsNull)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                throw cx.db.Exception("42113", b.GetType().Name).Mix();
            }
            return null;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = cx.obs[lhs].StartCounter(cx, rs, tg);
            tg = cx.obs[rhs].StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = cx.obs[lhs].AddIn(cx,rb, tg);
            tg = cx.obs[rhs].AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[lhs]).Needs(cx,r,qn);
            qn = ((SqlValue)cx.obs[rhs]).Needs(cx,r,qn);
            return qn;
        }
        internal override bool KnownBy<V>(Context cx, CTree<long,V> cs)
        {
            return ((SqlValue)cx.obs[lhs]).KnownBy(cx, cs) && ((SqlValue)cx.obs[rhs]).KnownBy(cx, cs);
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((SqlValue)cx.obs[lhs]).KnownBy(cx, q) && ((SqlValue)cx.obs[rhs]).KnownBy(cx, q);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (lhs >= 0)
            {
                r += ((SqlValue)cx.obs[lhs]).KnownFragments(cx, kb);
                y = y && r.Contains(lhs);
            }
            if (rhs >= 0)
            {
                r += ((SqlValue)cx.obs[rhs]).KnownFragments(cx, kb);
                y = y && r.Contains(rhs);
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
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
            (BList<Domain>)mem[MemberPredicate.Rhs] ?? BList<Domain>.Empty; // naughty: MemberPreciate Rhs is SqlValue
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal TypePredicate(Iix dp,SqlValue a, bool f, BList<Domain> r)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool.defpos)
                  +(MemberPredicate.Lhs,a.defpos)+(MemberPredicate.Found,f)
                  +(MemberPredicate.Rhs,r))
        {  }
        protected TypePredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static TypePredicate operator+(TypePredicate s,(long,object)x)
        {
            return new TypePredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TypePredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TypePredicate(new Iix(iix,dp),mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (TypePredicate)base._Relocate(cx);
            r += (MemberPredicate.Lhs, cx.Fix(lhs));
            r += (MemberPredicate.Rhs, cx.Fix(rhs));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (TypePredicate)base._Fix(cx);
            var nl = cx.Fix(lhs);
            if (nl!=lhs)
            r += (MemberPredicate.Lhs, nl);
            var nr = cx.Fix(rhs);
            if (nr!=rhs)
            r += (MemberPredicate.Rhs, nr);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TypePredicate)base._Replace(cx, so, sv);
            var lh = cx.ObReplace(r.lhs, so, sv);
            if (lh != r.lhs)
                r += (MemberPredicate.Lhs, lh);
            if (r!=this)
                r = (TypePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (TypePredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[lhs]).AddFrom(cx, q);
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
            return new TypePredicate(iix,(SqlValue)cx.obs[lhs], !found, rhs);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var a = cx.obs[lhs].Eval(cx);
            if (a == null)
                return null;
            if (a.IsNull)
                return TBool.False;
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
            return ((SqlValue)cx.obs[lhs]).Needs(cx,r,qn);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return cx.obs[lhs].Needs(cx, rs);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        internal Sqlx kind => (Sqlx)mem[Domain.Kind];
        public PeriodPredicate(Iix dp,Context cx,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,_Mem(cx,op1,op2)+(Domain.Kind,o)
                 +(Dependents,new CTree<long,bool>(op1.defpos,true)+(op2.defpos,true))
                 +(_Depth,1+_Max(op1.depth,op2.depth)))
        { }
        protected PeriodPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, SqlValue a, SqlValue b)
        {
            var m = BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = CTree<long,bool>.Empty;
            if (a != null)
            {
                m = m + (Left, a.defpos);
                ag += a.IsAggregation(cx);
            }
            if (b != null)
            {
                m = m + (Right, b.defpos);
                ag += b.IsAggregation(cx);
            }
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static PeriodPredicate operator+(PeriodPredicate s,(long,object)x)
        {
            return new PeriodPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodPredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new PeriodPredicate(new Iix(iix,dp),mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PeriodPredicate)base._Replace(cx, so, sv);
            var a = cx.ObReplace(left, so, sv);
            if (a != left)
                r += (Left, a);
            var b = cx.ObReplace(right, so, sv);
            if (b != r.right)
                r += (Right, b);
            if (r!=this)
                r = (PeriodPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[left]).Grouped(cx, gs) != false &&
            ((SqlValue)cx.obs[right]).Grouped(cx, gs) != false;
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (PeriodPredicate)base.AddFrom(cx, q);
            var a = (cx.obs[r.left] as SqlValue)?.AddFrom(cx, q);
            if (a.defpos != r.left)
                r += (Left, a.defpos);
            a = ((SqlValue)cx.obs[r.right]).AddFrom(cx, q);
            if (a.defpos != r.right)
                r += (Right, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.StartCounter(cx, rs, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.StartCounter(cx, rs, tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            if (cx.obs[left] is SqlValue lf) tg = lf.AddIn(cx,rb, tg);
            if (cx.obs[right] is SqlValue rg) tg = rg.AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[left])?.Needs(cx,r,qn) ??qn;
            qn = ((SqlValue)cx.obs[right])?.Needs(cx,r,qn) ??qn;
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            var r = cx.obs[left].Needs(cx, rs);
            r += cx.obs[right].Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[left].LocallyConstant(cx, rs)
                && cx.obs[right].LocallyConstant(cx, rs);
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var le = (SqlValue)cx.obs[left];
            var nl = le?.Having(cx, dm);
            var rg = (SqlValue)cx.obs[right];
            var nr = rg?.Having(cx, dm);
            return (le == nl && rg == nr) ? this :
                (SqlValue)cx.Add(new PeriodPredicate(cx.GetIid(), cx,  nl, kind, nr));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is PeriodPredicate that)
            {
                if (kind != that.kind)
                    return false;
                if (cx.obs[left] is SqlValue le)
                    if (!le.Match(cx, (SqlValue)cx.obs[that.left]))
                        return false;
                    else if (that.left >= 0)
                        return false;
                if (cx.obs[right] is SqlValue rg)
                    if (!rg.Match(cx, (SqlValue)cx.obs[that.right]))
                        return false;
                    else if (that.right >= 0)
                        return false;
                return true;
            }
            return false;
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
        public RowSetPredicate(Iix dp,Context cx,RowSet e,BTree<long,object>m=null) 
            : base(dp,_Mem(cx,e,m)+(RSExpr,e.defpos)
                  +(Dependents,new CTree<long,bool>(e.defpos,true))+(_Depth,1+e.depth))
        {  }
        protected RowSetPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object>_Mem(Context cx,RowSet e,BTree<long,object>m)
        {
            m = m ?? BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = cx._Dom(e).aggs;
            if (ag!=CTree<long,bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static RowSetPredicate operator+(RowSetPredicate q,(long,object)x)
        {
            return (RowSetPredicate)q.New(q.mem + x);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (expr >= 0) r += cx.obs[expr]._Rdc(cx);
            return r;
        }
        internal override bool ConstantIn(Context cx, RowSet rs)
        {
            return rs.stem.Contains(expr);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (RowSetPredicate)base._Relocate(cx);
            r += (RSExpr, cx.Fix(expr));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RowSetPredicate)base._Fix(cx);
            var nq = cx.Fix(expr);
            if (nq != expr)
                r += (RSExpr, nq);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (RowSetPredicate)base._Replace(cx,so,sv);
            var e = cx.ObReplace(r.expr,so,sv);
            if (e != r.expr)
                r += (RSExpr, e);
            if (r!=this)
                r = (RowSetPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
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
            tg = cx.obs[expr].StartCounter(cx, rs, tg);
            return base.StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = cx.obs[expr].AddIn(cx,rb,tg);
            return base.AddIn(cx,rb, tg);
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return cx.obs[expr].Needs(cx, rs);
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
        public ExistsPredicate(Iix dp,Context cx,RowSet e) : base(dp,cx,e,BTree<long,object>.Empty
            +(Dependents,new CTree<long,bool>(e.defpos,true))+(_Depth,1+e.depth)) { }
        protected ExistsPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static ExistsPredicate operator+(ExistsPredicate s,(long,object)x)
        {
            return new ExistsPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExistsPredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ExistsPredicate(new Iix(iix,dp),mem);
        }
        /// <summary>
        /// The predicate is true if the rowSet has at least one element
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return TBool.For(((RowSet)cx.obs[expr]).First(cx)!=null);
        }
        internal override CTree<long, bool> _Rdc(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            if (expr >= 0) r += cx.obs[expr]._Rdc(cx);
            return r;
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
        public UniquePredicate(Iix dp,Context cx,RowSet e) : base(dp,cx,e) {}
        protected UniquePredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        public static UniquePredicate operator +(UniquePredicate s, (long, object) x)
        {
            return new UniquePredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UniquePredicate(iix,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new UniquePredicate(new Iix(iix,dp),mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (UniquePredicate)base._Replace(cx, so, sv);
            var ex = (RowSet)cx._Replace(r.expr, so, sv);
            if (ex.defpos != r.expr)
                r += (RSExpr, ex.defpos);
            r = (UniquePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var rs = ((RowSet)cx.obs[expr]);
            RTree a = new RTree(rs.defpos,cx,cx._Dom(rs), TreeBehaviour.Disallow, TreeBehaviour.Disallow);
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
        internal NullPredicate(Iix dp,SqlValue v, bool b)
            : base(dp,new BTree<long,object>(_Domain,Domain.Bool.defpos)
                  +(NVal,v.defpos)+(NIsNull,b)+(Dependents,new CTree<long,bool>(v.defpos,true))
                  +(_Depth,1+v.depth))
        { }
        protected NullPredicate(Iix dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long, object> _Mem(Context cx, SqlValue v, BTree<long, object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            var dm = Domain.Bool;
            var ag = v.IsAggregation(cx);
            if (ag != CTree<long, bool>.Empty)
            {
                dm = (Domain)cx.Add(dm.Relocate(cx.GetUid()) + (Domain.Aggs, ag));
                m += (Domain.Aggs, ag);
            }
            m += (_Domain, dm.defpos);
            return m;
        }
        public static NullPredicate operator+(NullPredicate s,(long,object)x)
        {
            return new NullPredicate(s.iix, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new NullPredicate(iix,m);
        }
        internal override CTree<long,bool> IsAggregation(Context cx)
        {
            return ((SqlValue)cx.obs[val]).IsAggregation(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new NullPredicate(new Iix(iix,dp),mem);
        }
        internal override SqlValue AddFrom(Context cx, long q)
        {
            if (from > 0)
                return this;
            var r = (NullPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[val]).AddFrom(cx, q);
            if (a.defpos != val)
                r += (NVal, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (NVal, cx.Fix(val));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (NullPredicate)base._Fix(cx);
            var nv = cx.Fix(val);
            if (nv != val)
                r += (NVal, nv);
            return cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (NullPredicate)base._Replace(cx, so, sv);
            var vl = cx.ObReplace(r.val,so,sv);
            if (vl != r.val)
                r += (NVal, vl);
            if (r!=this)
                r = (NullPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue Having(Context cx, Domain dm)
        {
            var vl = (SqlValue)cx.obs[val];
            var nv = vl?.Having(cx, dm);
            return (vl==nv) ? this :
                (SqlValue)cx.Add(new NullPredicate(cx.GetIid(), nv, isnull));
        }
        internal override bool Match(Context cx, SqlValue v)
        {
            if (v is NullPredicate that)
            {
                if (isnull != that.isnull)
                    return false;
                if (cx.obs[val] is SqlValue w)
                    if (!w.Match(cx, (SqlValue)cx.obs[that.val]))
                        return false;
                    else if (that.val >= 0)
                        return false;
                return true;
            }
            return false;
        }
        internal override bool Grouped(Context cx, GroupSpecification gs)
        {
            return ((SqlValue)cx.obs[val]).Grouped(cx, gs);
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[val].Eval(cx) is TypedValue tv)? TBool.For(tv.IsNull == isnull) : null;
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[val].StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[val].AddIn(cx,rb, tg);
        }
        internal override bool KnownBy<V>(Context cx, CTree<long, V> cs)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, cs);
        }
        internal override bool KnownBy(Context cx, RowSet q)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, q);
        }
        internal override CTree<long, Domain> KnownFragments(Context cx, CTree<long, Domain> kb)
        {
            if (kb.Contains(defpos))
                return new CTree<long, Domain>(defpos, kb[defpos]);
            var r = CTree<long, Domain>.Empty;
            var y = true;
            if (val >= 0)
            {
                r += ((SqlValue)cx.obs[val]).KnownFragments(cx, kb);
                y = y && r.Contains(val);
            }
            if (y)
                return new CTree<long, Domain>(defpos, cx._Dom(this));
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, long r,CTree<long, bool> qn)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx,r,qn);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[val].LocallyConstant(cx, rs);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, long rs)
        {
            return cx.obs[val].Needs(cx, rs);
        }
        public override string ToString()
        {
            return isnull?"is null":"is not null";
        }
    }
}
