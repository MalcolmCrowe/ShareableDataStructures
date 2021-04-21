using System;
using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// 
    /// SqlValues are DBObjects from version 7. However, SqlNames can be transformed into 
    /// other SqlValue sublasses on resolution, retaining the same defpos.
    /// They participate in rowset rewriting where expressions are interesting if they
    /// are locally constant (i.e. entirely needed) or entirely from views.
    /// During rowset construction we fill in the LocalConstant and ViewOrigin structures.
    /// </summary>
    internal class SqlValue : DBObject,IComparable
    {
        internal const long
            _Columns = -299, // CList<long> SqlValue
            Left = -308, // long DBObject to allow for DOT
            LocalConstant = -192, // CTree<long,bool> RowSet
            _Meta = -307, // Metadata
            Right = -309, // long SqlValue
            Sub = -310; // long SqlValue
        public string name => (string)mem[Name]??"";
        internal long left => (long)(mem[Left]??-1L); 
        internal long right => (long)(mem[Right]??-1L);
        internal long sub => (long)(mem[Sub]??-1L);
        internal BTree<Sqlx,object> meta => 
            (BTree<Sqlx,object>)mem[_Meta]??BTree<Sqlx,object>.Empty;
        internal CList<long> columns => (CList<long>)mem[_Columns] ?? CList<long>.Empty;
        internal virtual long target => defpos;
        internal CTree<long, bool> localConstant =>
            (CTree<long, bool>)mem[LocalConstant] ?? CTree<long, bool>.Empty;
        public SqlValue(Ident ic) : this(ic.iix, ic.ident) { }
        public SqlValue(long dp,string nm="",Domain dt=null,CList<long> cols=null,
            BTree<long,object>m=null):
            base(dp,(m??BTree<long, object>.Empty)+(_Domain,dt??Domain.Content)
                +(Name,nm)+(_Columns,cols??CList<long>.Empty))
        { }
        protected SqlValue(Context cx,string nm,Domain dt,long cf=-1L)
            :base(cx.GetUid(),_Mem(cf)+(Name,nm)+(_Domain,dt))
        {
            cx.Add(this);
        }
        protected SqlValue(long dp, BTree<long, object> m) : base(dp, m)
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
            return new SqlValue(defpos, m);
        }
        internal virtual SqlValue Col(Context cx,int i)
        {
            throw new NotImplementedException();
        }
        internal virtual TypedValue Eval(Context cx,int i)
        {
            if (i < 0 || i >= domain.rowType.Length)
                throw new DBException("22007");
            var v = Eval(cx);
            return v[i];
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[left]?.Calls(defpos, cx)==true || cx.obs[right]?.Calls(defpos,cx)==true;
        }
        /// <summary>
        /// The Import transformer allows the the import of an SqlValue expression into a subquery
        /// or view. This means that identifiers/column names will refer to their meanings inside
        /// the subquery.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q">The inner query</param>
        /// <returns></returns>
        internal virtual SqlValue Import(Context cx,Query q)
        {
            return this;
        }
        internal virtual bool KnownBy(Context cx,Query q)
        {
            return q.Knows(cx, defpos);
        }
        internal virtual bool KnownBy(Context cx,RowSet r)
        {
            return false;
        }
        internal string Alias()
        {
            return alias ?? name ?? ("C_" + (defpos&0xfff));
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return new CTree<long,bool>(defpos,true);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx,RowSet rs)
        {
            var fi = rs.finder[defpos];
            return (rs.Knows(cx,fi.rowSet))?CTree<long,RowSet.Finder>.Empty
                :new CTree<long, RowSet.Finder>(defpos, fi);
        }
        internal override bool LocallyConstant(Context cx,RowSet rs)
        {
            return isConstant(cx) || rs.needed.Contains(defpos);
        }
        internal virtual SqlValue Reify(Context cx)
        {
            for (var b = domain.rowType?.First(); b != null; b = b.Next())
            {
                var ci = cx.Inf(b.key());
                if (ci.name == name)
                    return new SqlCopy(defpos, cx, name, defpos, ci.defpos);
            }
            return this;
        }
        internal virtual SqlValue AddFrom(Context cx,Query q)
        {
            if (from > 0)
                return this;
            return (SqlValue)cx.Add(this + (_From, q.defpos));
        }
        internal override SqlValue ToSql(Ident id,Database db)
        {
            return this;
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
        internal virtual void Grouped(Context cx,GroupSpecification gs)
        {
            if (!gs.Has(cx, defpos))
                throw new DBException("42170", name);
        }
        internal virtual (SqlValue,Query) Resolve(Context cx,Query q,string a=null)
        {
            if (domain != Domain.Content)
                return (this,q);
            if (q is From fm)
            {
                if (name == q?.name || name == q?.alias)
                {
                    var st = new SqlTable(defpos, fm);
                    return ((SqlValue)cx.Replace(this, st), q);
                }
                var id = new Ident(name, defpos);
                var ic = new Ident(new Ident(a??q.name, q.defpos), id);
                if (cx.obs[cx.defs[ic]] is DBObject tg)
                {
                    var cp = (tg is SqlCopy sc) ? sc.copyFrom : tg.defpos;
                    var nc = new SqlCopy(defpos, cx, name, q.defpos, cp);
                    if (nc.defpos < tg.defpos)
                        cx.Replace(tg, nc);
                    else
                        cx.Replace(this, nc);
                    q = (Query)cx.obs[q.defpos];
                    return (nc,q);
                }
            }
            if (cx.defs.Contains(name))
            {
                var ob = cx.obs[cx.defs[name].Item1];
                if (q.rowType== CList<long>.Empty)
                    return (this,q);
                if (ob.domain.kind != Sqlx.CONTENT && ob.defpos != defpos && ob is SqlValue sb)
                {
                    var nc = (SqlValue)cx.Replace(this,
                        new SqlValue(defpos,name,sb.domain,null,mem));
                    return (nc,q);
                }
            }
            var rt = q?.rowType;
            var i = PosFor(cx,name);
            if (name != "" && i >= 0)
            {
                var sv = (SqlValue)cx.obs[rt[i]];
                var ns = sv;
                if (sv is SqlCopy sc && alias != null && alias != sc.name)
                    ns = ns + (_Domain, sc.domain);
                else if ((!(sv is SqlCopy)) && sv.domain != Domain.Content)
                    ns = new SqlCopy(defpos, cx, name, q.defpos, sv);
//                ns = ns.Reify(cx, new Ident(alias ?? name, defpos));
                var nc = (SqlValue)cx.Replace(this, ns);
                q += (i, nc);
                return (nc, q);
            }
            return (this,q);
        }
        internal override DBObject AddMatches(Context cx,Query f)
        {
            return f;
        }
        internal int PosFor(Context cx, string nm)
        {
            var i = 0;
            for (var b = columns.First(); b != null; b = b.Next(), i++)
            {
                var p = b.key();
                var ci = cx.Inf(p);
                if (ci.name == nm)
                    return i;
            }
            return -1;
        }
        internal virtual (SqlValue, BTree<SqlValue, string>, BTree<long, string>, 
            BTree<string, bool?>,BTree<long,SqlValue>)
            _ColsForRestView(Context cx, string sg, Query gf, GroupSpecification gs,
            BTree<SqlValue, string> gfc, BTree<long, string> rem, BTree<string, bool?> reg,
            BTree<long,SqlValue> map)
        {
            return (map[defpos]??this, gfc, rem, reg, map);
        }
        internal (SqlValue,BTree<long,SqlValue>) ColsForRestView(Context cx, RestRowSet rrs, 
            ObInfo vi, GroupSpecification gs,BTree<long,SqlValue> map)
        {
            var gf = (Query)cx.obs[rrs.defpos];
            var sg = rrs.sqlAgent;
            var reg = BTree<string, bool?>.Empty;
            var gfc = BTree<SqlValue, string>.Empty;
            var rem = BTree<long, string>.Empty;
            var ors = rrs;
            SqlValue sv;
            var an = alias ?? vi.name;
            if (gs!=null && gs.Has(cx, defpos))
            {
                (sv, _, rem, _,map) = _ColsForRestView(cx, sg, gf, gs, 
                    BTree<SqlValue, string>.Empty, rem, BTree<string, bool?>.Empty,map);
                gfc += (this, an);
                reg += (an, true);
            }
            else
            {
                (sv, gfc, rem, reg,map) = _ColsForRestView(cx, sg, gf, gs, gfc, rem, reg,map);
                an = alias ?? name;
            }
            if (gf != null)
                for (var b = gfc.First(); b != null; b = b.Next())
                {
                    var su = b.key();
                    sv = gf.MaybeAdd(cx, su);
                }
            var cs = cx.obs[vi.defpos + 2] as CursorSpecification;
            if (cs != null)
            {
                var qe = (QueryExpression)cx.obs[cs.union];
                var qs = (QuerySpecification)cx.obs[qe.left];
                var gr = (GroupSpecification)cx.obs[qs.tableExp.group];
                if ((!isConstant(cx)) || gr?.Has(cx, defpos) == true)
                    for (var b = rem.First(); b != null; b = b.Next())
                        cs.Add(cx, (SqlValue)cx.obs[b.key()] + (_Alias, b.value()));
            }
            for (var b = reg.First(); b != null; b = b.Next())
                if (!rrs.remoteGroups.Has(cx, b.key()))
                {
                    var rg = rrs.remoteGroups + (GroupSpecification.Sets,
                        rrs.remoteGroups.sets + b.key());
                    rrs += (RestRowSet.RemoteGroups, rg);
                }
            if (rrs != ors)
                cx.data += (rrs.defpos, rrs);
            if (sv!=null)
                cx._Add(sv);
            return (sv,map);
        }
        /// <summary>
        /// Eval is used to deliver the TypedValue for the current Cursor,
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values.Contains(defpos))
                return cx.values[defpos];
            var f = cx.finder[defpos]; 
            var r = cx.data[f.rowSet];
            if (r == null)
                return null;
            var c = cx.cursors[f.rowSet];
            if (c == null)
                return null;
            return c[f.col] ?? TNull.Value;
        }
        internal override void Set(Context cx, TypedValue v)
        {
            base.Set(cx, v);
            if (cx.data[from] is RowSet rs 
                && cx.cursors[rs.defpos] is Cursor cu)
                cu += (cx, defpos, v);
        }
        internal virtual CTree<long,bool> Disjoin(Context cx)
        {
            return new CTree<long,bool>(defpos, true);
        }
        internal virtual bool Uses(Context cx,long t)
        {
            return false;
        }
        /// <summary>
        /// Used for Window Function evaluation.
        /// Called by GroupingBookmark (when not building) and SelectedCursor
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(Context cx,Cursor bmk)
        { }
        /// <summary>
        /// Analysis stage Conditions(). 
        /// See if q can fully evaluate this.
        /// If so, evaluation of an enclosing QuerySpec column can be moved down to q.
        /// However, at this stage we also look for additional filters from equality conditions
        /// and so the queries can be transformed in this process.
        /// </summary>
        internal virtual Query Conditions(Context cx,Query q,bool disj,out bool move)
        {
            move = false;
            return q;
        }
        internal virtual bool Check(Context cx,GroupSpecification gs)
        {
            return true;
        } 
        /// <summary>
        /// test whether the given SqlValue is structurally equivalent to this (always has the same value in this context)
        /// </summary>
        /// <param name="v">The expression to test against</param>
        /// <returns>Whether the expressions match</returns>
        internal virtual bool _MatchExpr(Context cx,Query q,SqlValue v)
        {
            return defpos==v.defpos;
        }
        /// <summary>
        /// analysis stage conditions(): test to see if this predicate can be distributed.
        /// </summary>
        /// <param name="q">the query to test</param>
        /// <param name="ut">(for RestView) a usingTableType</param>
        /// <returns>true if the whole of thsi is provided by q and/or ut</returns>
        internal virtual bool IsFrom(Context cx,Query q, bool ordered=false, Domain ut=null)
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
            var dm = (Domain)domain._Replace(cx, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
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
        internal new virtual CTree<long,bool> Needs(Context cx, CTree<long,bool> qn)
        {
            return qn.Contains(defpos)?qn:(qn + (defpos,true));
        }
        internal virtual bool isConstant(Context cx)
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
        internal virtual Query JoinCondition(Context cx, JoinPart j, ref CTree<long, bool> joinCond, 
            ref CTree<long, bool> where)
        {
            cx.Add((Query)((Query)cx.obs[j.left]).AddCondition(cx,Query.Where, this, true));
            cx.Add((Query)((Query)cx.obs[j.right]).AddCondition(cx,Query.Where,this, true));
            return j;
        }
        internal bool? Matches(Context cx)
        {
           return (Eval(cx) is TBool tb)?tb.value:null;
        }
        internal virtual bool HasAnd(Context cx,SqlValue s)
        {
            return s == this;
        }
        internal virtual SqlValue Invert(Context cx)
        {
            return new SqlValueExpr(cx.GetUid(), cx, Sqlx.NOT, this, null, Sqlx.NO);
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
        internal virtual RowSet RowSet(long dp,Context cx, Domain xp)
        {
            if (cx.Eval(dp,xp.rowType) is TRow r)
                return new TrivialRowSet(dp,cx, r, -1L,
                    cx.data[from]?.finder?? CTree<long,RowSet.Finder>.Empty);
            return new EmptyRowSet(dp,cx,xp);
        }
        internal virtual Domain FindType(Context cx,Domain dt)
        {
            if (domain == null || domain.kind==Sqlx.CONTENT)
                return dt;
            if (!dt.CanTakeValueOf(domain))
                throw new DBException("22005", dt.kind, domain.kind);
            if ((isConstant(cx) && domain.kind == Sqlx.INTEGER) || domain.kind==Sqlx.Null)
                return dt; // keep union options open
            return domain;
        }
        public virtual int CompareTo(object obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            t = Scan(t, domain.rowType, VIC.OK | VIC.OV);
            t = Scan(t,left,VIC.OK|VIC.OV);
            t = Scan(t,right, VIC.OK | VIC.OV);
            t = Scan(t,sub, VIC.OK | VIC.OV);
            t = Scan(t, (long)(meta[Sqlx.INVERTS]??-1L), VIC.OK);
            t = Scan(t, columns, VIC.OK | VIC.OV);
            t = Scan(t, target, VIC.OK | VIC.OV);
            t = Scan(t, from, VIC.OK | VIC.OV);
            return t;
        }
        internal override DBObject Instance(Context cx)
        {
            var np = cx.obuids[defpos]??defpos;
            if (np != defpos)
                return (DBObject)cx.obs[np].Fix(cx);
            return this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_From)) { sb.Append(" From:"); sb.Append(Uid(from)); }
            if (mem.Contains(_Alias)) { sb.Append(" Alias="); sb.Append(alias); }
            if (left!=-1L) { sb.Append(" Left:"); sb.Append(Uid(left)); }
            if (mem.Contains(_Domain)) { sb.Append(" "); sb.Append(domain); } 
            if (right!=-1L) { sb.Append(" Right:"); sb.Append(Uid(right)); }
            if (sub!=-1L) { sb.Append(" Sub:"); sb.Append(Uid(sub)); }
            return sb.ToString();
        }
        internal override string ToString(string sg,CTree<string,CTree<long,long>> cs,Context cx)
        {
            return (cs.Contains(name))? name : Eval(cx).ToString(cs,cx);
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
        internal override CList<long> _Cols(Context cx)
        {
            return columns;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValue(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlValue)base._Relocate(wr);
            r += (_From, wr.Fix(from));
            r += (Left, wr.Fixed(left)?.defpos??-1L);
            r += (Right, wr.Fixed(right)?.defpos??-1L);
            r += (_Domain, domain._Relocate(wr));
            r += (_Columns, wr.Fix(columns));
            r += (Sub, wr.Fixed(sub)?.defpos??-1L);
            // don't worry about TableRow
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlValue)base.Fix(cx);
            var nf = cx.obuids[from] ?? from;
            if (from!=nf)
                r += (_From, nf);
            var nl = cx.obuids[left] ?? left;
            if (left!=nl)
                r += (Left, nl);
            var nr = cx.obuids[right] ?? right;
            if (right!=nr)
                r += (Right, nr);
            var nd = domain.Fix(cx);
            if (nd!=domain)
            r += (_Domain,nd);
            var nc = cx.Fix(columns);
            if (columns!=nc)
                r += (_Columns, nc);
            var ns = cx.obuids[sub] ?? sub;
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
            cx.obs += (np, r);
            cx.obuids += (defpos, np);
            return r;
        }
    }
    internal class SqlTable : SqlValue
    {
        public SqlTable(long dp, Query fm, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty) + (_From, fm.defpos)
                  +(_Domain,fm.domain)+(Depth,1+fm.depth)) { }
        protected SqlTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTable operator+(SqlTable t,(long,object)x)
        {
            return (SqlTable)t.New(t.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTable(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTable(dp,mem);
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[from];
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return true;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return cx.obs[from].Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (from!=-1L) { sb.Append(" Table:"); sb.Append(Uid(from)); }
            return sb.ToString();
        }
    }
    internal class SqlCopy : SqlValue
    {
        internal const long
            CopyFrom = -284; // long
        public long copyFrom => (long)mem[CopyFrom];
        public SqlCopy(long dp, Context cx, string nm, long fp, SqlValue cf,
            BTree<long, object> m = null)
            : base(dp, _Mem(fp,m) + (CopyFrom, cf.defpos) + (_Columns, cf.columns)
                  + (_Domain, cf.domain) + (Name, nm))
        {  }
        public SqlCopy(long dp, Context cx, string nm, long fp, long cp,
            BTree<long, object> m = null)
            : base(dp, _Mem(fp, m) + (CopyFrom, cp) + (_Columns, cx.Cols(cp))
                 + (_Domain, (cx.obs[cp]??(DBObject)cx.db.objects[cp]).domain)+(Name,nm))
        { }
        static BTree<long,object> _Mem(long fp,BTree<long,object>m)
        {
            m = m ?? BTree<long, object>.Empty;
            if (fp>=0)
                m += (_From, fp);
            return m;
        }
        protected SqlCopy(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCopy operator +(SqlCopy s, (long, object) x)
        {
            return (SqlCopy)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCopy(defpos, m);
        }
        internal override bool IsFrom(Context cx, Query q, bool ordered = false, Domain ut = null)
        {
            return q.domain.representation.Contains(defpos);
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            if (q.Knows(cx,copyFrom))
                return true;
            return base.KnownBy(cx, q);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            if (r.domain.representation.Contains(copyFrom))
                return true;
            return r.finder.Contains(defpos);
        }
        internal override SqlValue Operand(Context cx)
        {
            return cx.obs[copyFrom]?.Operand(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCopy(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlCopy)base._Relocate(wr);
            r += (CopyFrom, wr.Fixed(copyFrom).defpos);
            return r;
        }
   /*     internal override Basis Fix(Context cx) Removed 3 April 2021
    *     We probably never should modify copyFrom during Fix()
        {
            var r = (SqlCopy)base.Fix(cx);
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            var nc = cx.obuids[cf] ?? cf;
            if (nc != copyFrom)
                r += (CopyFrom, nc);
            return r;
        }
/*        internal override SqlValue Reify(Context cx, Ident ic)
        { see SqlValue.Resolve and Parser.Reify
            if (defpos >= Transaction.Executables && defpos < Transaction.HeapStart
                && ic.iix < Transaction.Executables)
                return (SqlCopy)cx.Add(new SqlCopy(ic.iix, cx, ic.ident, from, this));
            else
                return this;
        } */
        internal override (SqlValue, BTree<SqlValue, string>, BTree<long, string>, 
            BTree<string, bool?>, BTree<long, SqlValue>) 
            _ColsForRestView(Context cx, string sg, Query gf, GroupSpecification gs, 
            BTree<SqlValue, string> gfc, BTree<long, string> rem, BTree<string, bool?> reg, 
            BTree<long, SqlValue> map)
        {
            if (map.Contains(copyFrom))
                return map[copyFrom]._ColsForRestView(cx, sg, gf, gs, gfc, rem, reg, map);
            return base._ColsForRestView(cx, sg, gf, gs, gfc, rem, reg, map);
        }
        internal override TypedValue Eval(Context cx)
        {
            var f = cx.finder[copyFrom];
            if (cx.cursors[f.rowSet] is TRow rw && rw.values[f.col] is TypedValue tv)
                return tv;
            if (cx.values.Contains(copyFrom))
                return cx.values[copyFrom]??TNull.Value;
            return base.Eval(cx);
        }
        internal override void Set(Context cx, TypedValue v)
        {
            cx.obs[copyFrom].Set(cx,v);
            base.Set(cx, v);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn.Contains(copyFrom)?qn:(qn+(copyFrom,true));
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            if (cf<0 || (cf>=Transaction.Analysing
                && cx.obs[cf].Needs(cx, rs) == CTree<long, RowSet.Finder>.Empty))
                return CTree<long, RowSet.Finder>.Empty;
            return base.Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            var cf = copyFrom;
            while (cx.obs[cf] is SqlCopy sc)
                cf = sc.copyFrom;
            return (cx.obs[cf]?.LocallyConstant(cx,rs)??false) || base.LocallyConstant(cx, rs);
        }
        internal override bool sticky()
        {
            return true;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            return base.Scan(Scan(t,copyFrom, VIC.OK | VIC.OV));
        }
        internal override DBObject Instance(Context cx)
        {
            return base.Instance(cx);
        }
        public override string ToString()
        {
            return base.ToString() + " copy from "+Uid(copyFrom);
        }
        internal override string ToString(string sg,CTree<string, CTree<long,long>> cs, Context cx)
        {
            return (cs.Contains(name))? name
                : Eval(cx)?.ToString() ?? throw new DBException("42000");
        }
    }
    internal class SqlRowSetCol : SqlValue
    {
        public SqlRowSetCol(long dp, ObInfo oi, long rp)
            : base(dp, BTree<long, object>.Empty + (_From, rp)
                  + (_Domain, oi.domain))
        { }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[from]?[defpos];
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
    }
    /// <summary>
    /// A TYPE value for use in CAST
    /// </summary>
    internal class SqlTypeExpr : SqlValue
    {
        internal static readonly long
            TreatType = -312; // Domain
        internal Domain type=>(Domain)mem[TreatType];
        /// <summary>
        /// constructor: a new Type expression
        /// </summary>
        /// <param name="ty">the type</param>
        internal SqlTypeExpr(long dp,Context cx,Domain ty)
            : base(dp,BTree<long, object>.Empty + (_Domain, Domain.TypeSpec) 
                +(TreatType,ty))
        {}
        protected SqlTypeExpr(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static SqlTypeExpr operator +(SqlTypeExpr s, (long, object) x)
        {
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
            return new TTypeSpec(type);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return true;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            return base.Scan(type.Scan(t));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" TreatType:");sb.Append(type);
            return sb.ToString();
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTypeExpr(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlTypeExpr)base._Relocate(wr);
            var t = (Domain)type._Relocate(wr);
            if (t != type)
                r += (TreatType, t);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlTypeExpr)base.Fix(cx);
            var t = (Domain)type.Fix(cx);
            if (t != type)
                r += (TreatType, t);
            return r;
        }
    }
    /// <summary>
    /// A Subtype value for use in TREAT
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
        internal SqlTreatExpr(long dp,SqlValue v,Domain ty, Context cx)
            : base(dp,_Mem(dp,cx,ty,v) +(TreatExpr,v.defpos)
                  +(Depth,v.depth+1))
        { }
        protected SqlTreatExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(long dp,Context cx,Domain ty,SqlValue v)
        {
            var dv = v.domain;
            var dm = (ty.kind == Sqlx.ONLY && ty.iri != null) ?
                  new Domain(dp, dv.kind, dv.mem + (Domain.Iri, ty.iri)) : ty;
            return BTree<long, object>.Empty + (_Domain, dm);
        }
        public static SqlTreatExpr operator +(SqlTreatExpr s, (long, object) x)
        {
            return (SqlTreatExpr)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTreatExpr(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTreatExpr(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlTreatExpr)base._Relocate(wr);
            r += (TreatExpr, wr.Fixed(val)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlTreatExpr)base.Fix(cx);
            var ne = cx.obuids[val] ?? val;
            if (ne!=val)
            r += (TreatExpr, ne);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            var v = cx.Replace(r.val,so,sv);
            if (v != r.val)
                r += (TreatExpr, v);
            r = (SqlTreatExpr)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, q);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return ((SqlValue)cx.obs[val]).KnownBy(cx, r);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((SqlValue)cx.obs[val]).Uses(cx,t);
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.obs[val].Eval(cx) is TypedValue tv)
            {
                if (!domain.HasValue(cx,tv))
                    throw new DBException("2200G", domain.ToString(), val.ToString()).ISO();
                return tv;
            }
            return null;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx,qn);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx, rs);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            return base.Scan(Scan(t,val, VIC.OK | VIC.OV));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Val= "); sb.Append(Uid(val));
            return sb.ToString();
        }
    }
    internal class SqlElement : SqlValue
    {
        internal SqlElement(long defpos,Context cx,long op,Domain dt,CList<long> cols=null) 
            : base(defpos,"",dt,cols,BTree<long,object>.Empty+(_From,op))
        { }
        protected SqlElement(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlElement operator +(SqlElement s, (long, object) x)
        {
            return (SqlElement)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlElement(defpos,m);
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.obs[from].Eval(cx)[defpos];
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(left.ToString());
            return sb.ToString();
        }
        internal override string ToString(string sg,CTree<string, CTree<long,long>> cs, Context cx)
        {
            string n = "";
            for (var b = cs.First(); b != null; b = b.Next())
                for (var c=b.value().First();c!=null;c=c.Next())
                 if (c.value() == defpos)
                    n = b.key();
            var sb = new StringBuilder(cx.obs[from].ToString(sg,cs,cx));
            sb.Append("["); sb.Append(n); sb.Append("]"); 
            return sb.ToString();
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlElement(dp,mem);
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
        public SqlValueExpr(long dp, Context cx, Sqlx op, SqlValue lf, SqlValue rg, 
            Sqlx m, BTree<long, object> mm = null)
            : base(dp, _Type(dp, cx, op, m, lf, rg, mm)
                  + (Modifier, m) + (Domain.Kind, op)
                  +(Dependents,new CTree<long,bool>(lf?.defpos??-1L,true)+(rg?.defpos??-1L,true))
                  +(Depth,1+_Max((lf?.depth??0),(rg?.depth??0))))
        { }
        protected SqlValueExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueExpr operator +(SqlValueExpr s, (long, object) x)
        {
            return new SqlValueExpr(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueExpr(dp,mem);
        }
        internal override (SqlValue,Query) Resolve(Context cx, Query fm,string a=null)
        {
            SqlValue lf=null, r = this;
            if (cx.obs[left] is SqlValue ol)
                (lf,fm) = ol.Resolve(cx, fm, a);
            var rt = fm?.rowType;
            var rg = (SqlValue)cx.obs[right];
            if (kind == Sqlx.DOT && lf is SqlTable st && st.from==fm?.defpos)
            {
                var i = PosFor(cx,rg.name);
                if (i >= 0)
                {
                    var sc = (SqlCopy)cx.obs[rt[i]];
                    var nn = (lf.alias ?? lf.name) + "." + (rg.alias ?? rg.name);
                    if (alias != null)
                        nn = alias;
                    var nc = sc + (_Alias, nn);
                    fm += (Domain.RowType, rt + (i, nc.defpos));
                    return (nc, fm);
                }
            }
            (rg,fm) = rg?.Resolve(cx, fm, a)??(rg,fm);
            if (lf?.defpos != left || rg?.defpos != right)
                r = (SqlValue)cx.Replace(this,
                    new SqlValueExpr(defpos, cx, kind, lf, rg, mod, mem));
            return (r,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueExpr)base._Replace(cx,so,sv);
            var lf = cx.Replace(r.left, so, sv);
            if (kind == Sqlx.DOT)
            {
                if (lf == so.defpos)
                {
                    if (sv is Query q)
                        lf = q.defpos;
                    else 
                        return Replace(cx, this, sv);
                }
            }
            if (lf != r.left)
                r += (Left, lf);
            var rg = cx.Replace(r.right, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            if (r.domain.kind==Sqlx.UNION || r.domain.kind==Sqlx.CONTENT)
            {
                Domain dm = r.domain;
                if (cx.obs[r.sub] is SqlValue se)
                    dm = se.domain;
                else
                {
                    var ls = cx._Ob(lf) as SqlValue;
                    var rs = cx._Ob(rg) as SqlValue;
                    if (lf!=-1L && kind != Sqlx.DOT)
                        dm = (Domain)_Type(defpos, cx, kind, mod, ls, rs)[_Domain];
                    else if (kind == Sqlx.DOT)
                        dm = rs.domain;
                }
                if (dm != r.domain)
                    r += (_Domain, dm);
            }
            r = (SqlValueExpr)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
        internal override bool KnownBy(Context cx, Query q)
        {
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, q) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, q) != false);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return ((cx.obs[left] as SqlValue)?.KnownBy(cx, r) != false)
                && ((cx.obs[right] as SqlValue)?.KnownBy(cx, r) != false);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((cx.obs[left] as SqlValue)?.Uses(cx,t)==true) 
                || ((cx.obs[right] as SqlValue)?.Uses(cx,t)==true);
        }
        internal override CTree<long, bool> Disjoin(Context cx)
        { // parsing guarantees right associativity
            return (kind == Sqlx.AND)? 
                ((SqlValue)cx.obs[right]).Disjoin(cx)+(left, true)
                :base.Disjoin(cx);
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[left])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[right])?.Grouped(cx, gs);
        }
        internal override SqlValue Import(Context cx,Query q)
        {
            if ((cx.obs[left] as SqlValue)?.Import(cx,q) is SqlValue a)
            {
                var b = ((SqlValue)cx.obs[right])?.Import(cx, q);
                if (left == a.defpos && right == b.defpos)
                    return this;
                if (right == -1L || b.defpos != -1L)
                    return new SqlValueExpr(defpos,cx, kind, a, b, mod) + (_Alias, alias);
            }
            return null;
        }
        const int ea = 1, eg = 2, la = 4, lr = 8, lg = 16, ra = 32, rr = 64, rg = 128;
        internal override (SqlValue, BTree<SqlValue, string>, BTree<long, string>,
           BTree<string, bool?>, BTree<long, SqlValue>)
           _ColsForRestView(Context cx, string sg, Query gf, GroupSpecification gs,
           BTree<SqlValue, string> gfc, BTree<long, string> rem, BTree<string, bool?> reg,
           BTree<long, SqlValue> map)
        {
            var rgl = BTree<string, bool?>.Empty;
            var gfl = BTree<SqlValue, string>.Empty;
            var rel = BTree<long, string>.Empty;
            var rgr = BTree<string, bool?>.Empty;
            var gfr = BTree<SqlValue, string>.Empty;
            var rer = BTree<long, string>.Empty;
            // we distinguish many cases here using the above constants: exp/left/right:agg/grouped/remote
            int cse = 0, csa = 0;
            SqlValue lf = (SqlValue)cx.obs[left], ri = (SqlValue)cx.obs[right];
            var el = lf;
            var er = ri;
            if (gf.aggregates(cx))
                cse += ea;
            if (gs?.Has(cx,alias ?? name) == true)
                cse += eg;
            if (lf.aggregates(cx))
                cse += la;
            if (ri?.aggregates(cx) == true)
                cse += ra;
            var cs = cx.data[gf.defpos].Source(cx);
            if (lf.KnownBy(cx, gf) && (!lf.isConstant(cx)))
            {
                cse += lr;
                (el,gfl,rel,rgl,map) = lf._ColsForRestView(cx, sg, gf, gs, gfl, rel, rgl, map);
            }
            if (ri?.KnownBy(cx, gf) == true && (!ri.isConstant(cx)))
            {
                cse += rr;
                (er,gfr,rer,rgr,map) = ri._ColsForRestView(cx, sg, gf, gs, gfr, rer, rgr, map);
            }
            if (gs.Has(cx,left))
                cse += lg;
            if (gs.Has(cx,right))
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
                        var rs = cx.data[gf.defpos];
                        for (var b = rs.needed.First(); b != null; b = b.Next())
                        {
                            var sv = (SqlValue)cx.obs[b.key()];
                            var id = sv.alias ?? sv.name ?? ("C_" + (sv.defpos&0xfff));
                            gfc+=(sv, id);
                            rem+=(sv.defpos, id);
                            if (aggregates(cx))
                                reg+=(id, true);
                        }
                        return base._ColsForRestView(cx, sg, gf, gs, gfc, rem, reg, map);
                    }
            }
            gfc = BTree<SqlValue, string>.Empty;
            rem = BTree<long, string>.Empty;
            reg = BTree<string, bool?>.Empty;
            SqlValueExpr se = this;
            SqlValue st = null;
            var nn = Alias();
            var nl = el?.Alias();
            var nr = er?.Alias();
            switch (csa)
            {
                case 1: // lr rr : QS->Cexp as exp ; CS->Left’ op right’ as Cexp
                    // rel and rer will have just one entry each
                    st = new SqlValue(cx.GetUid(), nn, domain)+(_Alias,nn);
                    rem+= ((new SqlValueExpr(cx.GetUid(), cx, kind, 
                        (SqlValue)cx.obs[rel.First().key()], 
                        (SqlValue)cx.obs[rer.First().key()], mod)+(_Alias,nn)).defpos,nn);
                    gfc += (st, nn);
                    map += (defpos, st);
                    return (st,gfc,rem,reg,map);
                case 2: // lr: QS->Cleft op right as exp; CS->Left’ as Cleft 
                    // rel will have just one entry, rer will have 0 entries
                    se = new SqlValueExpr(cx.GetUid(),cx, kind,
                        new SqlValue(cx.GetUid(), nl, lf.domain), ri, mod)
                        +(_Alias,alias);
                    rem+= (rel.First().key(), nl);
                    gfc+= (gfl.First().key(), nl);
                    map+=(defpos, se);
                    return (se,gfc,rem,reg,map);
                case 3:// rr: QS->Left op Cright as exp; CS->Right’ as CRight
                    // rer will have just one entry, rel will have 0 entries
                    se = new SqlValueExpr(cx.GetUid(),cx, kind, lf,
                        new SqlValue(cx.GetUid(), er.alias ?? er.name,ri.domain), mod)
                    +(_Alias,alias);
                    rem +=(rer.First().key(), nr);
                    gfc += (gfr.First().key(), nr);
                    map += (defpos, se);
                    return (se,gfc,rem,reg,map);
                case 4: // ea lr rr: QS->SCleft op SCright; CS->Left’ as Cleft,right’ as Cright
                    // gfl, gfr, rgl and rgr may have sevral entries: we need all of them
                    se = new SqlValueExpr(cx.GetUid(), cx, kind, el, er, mod)
                    +(_Alias,nn);
                    gfc += gfl; 
                    gfc += gfr;
                    rem += rel; 
                    rem += rer;
                    map +=(defpos, se);
                    return (se,gfc,rem,reg,map);
                case 5: // ea eg lr rr: QS->Cexp as exp  group by exp; CS->Left’ op right’ as Cexp group by Cexp
                    // rel and rer will have just one entry each
                    reg += (nn, true);
                    goto case 1;
                case 6: // ea eg lr: QS->Cleft op right as exp group by exp; CS-> Left’ as Cleft group by Cleft
                    reg = CopyFrom(reg, rel);
                    goto case 2;
                case 7: // ea eg rr: QS->Left op Cright as exp group by exp; CS->Right’ as Cright group by Cright
                    reg = CopyFrom(reg, rer);
                    goto case 3;
                case 8: // ea eg la lr ra rr: QS->SCleft op SCright as exp group by exp; CS->Left’ as Cleft,right’ as Cright group by Cleft,Cright
                    reg = GroupOperands(cx, reg, rel);
                    reg = GroupOperands(cx, reg, rer);
                    goto case 4;
                case 9: // ea la lr rr rg: QS->SCleft op Cright as exp group by right; CS->Left’ as Cleft,right’ as Cright group by Cright
                    se = new SqlValueExpr(cx.GetUid(),cx, kind, el, er, mod)
                    +(_Alias,alias);
                    gfc += gfl; 
                    rem += rel;
                    map += (defpos, se);
                    return (se,gfc,rem,reg,map);
                case 10: // ea lr lg rg: QS->Left op SCright as exp group by left; CS->Left’ as Cleft,right’ as Cright group by Cleft
                    se = new SqlValueExpr(cx.GetUid(), cx, kind, el, er, mod)
                    +(_Alias,alias);
                    gfc += gfr; 
                    rem += rer;
                    map += (defpos, se);
                    return (se,gfc,rem,reg,map);
                case 11: // ea la lr rg: QS->SCleft op right as exp group by right; CS->Left’ as Cleft
                    se = new SqlValueExpr(cx.GetUid(), cx, kind, el, ri, mod) 
                    + (_Alias,alias);
                    gfc += gfl; 
                    rem += rel;
                    map += (defpos, se);
                    break;
                case 12: // ea lg ra: QS->Left op SCright as exp group by left; CS->Right’ as Cright
                    se = new SqlValueExpr(cx.GetUid(), cx, kind, lf, er, mod)
                        +(_Alias,alias);
                    gfc += gfr; 
                    rem += rer;
                    break;
            }
            se = new SqlValueExpr(cx.GetUid(), cx, kind, el, er, mod)
            + (_Alias,nn);
            if (gs.Has(cx,nn))// what we want if grouped
                st = new SqlValue(cx.GetUid(), nn, se.domain);
            if (gs.Has(cx,nn))
            {
                rem += (se.defpos, se.alias);
                gfc += (se, alias);
            }
            else
            {
                if (!el.isConstant(cx))
                    gfc += (el, nl);
                if (!er.isConstant(cx))
                    gfc += (er, nr);
            }
            map +=(defpos, se);
            return (se,gfc,rem,reg,map);
        }
        BTree<string,bool?> CopyFrom(BTree<string, bool?> dst, BTree<long, string> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
                dst += (b.value(), true);
            return dst;
        }
        BTree<string,bool?> GroupOperands(Context cx,BTree<string, bool?> dst, ATree<long, string> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
             if (cx.obs[b.key()].Operand(cx) is SqlValue sv)
               dst += (sv.alias??sv.name, true);
            return dst;
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
                    return tl.elType;
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
        internal override bool aggregates(Context cx)
        {
            return (cx.obs[left]?.aggregates(cx) == true) 
                || (cx.obs[right]?.aggregates(cx) == true);
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
        /// <summary>
        /// Analysis stage Conditions: set up join conditions.
        /// Code for altering ordSpec has been moved to Analysis stage Orders.
        /// </summary>
        /// <param name="j">a join part</param>
        /// <returns></returns>
        internal override Query JoinCondition(Context cx, JoinPart j,
            ref CTree<long, bool> joinCond, ref CTree<long, bool> where)
        // update j.joinCondition, j.thetaCondition and j.ordSpec
        {
            var lv = cx.obs[left] as SqlValue;
            if (lv == null)
                return ((SqlValue)cx.obs[right]).JoinCondition(cx, j, ref joinCond, ref where);
            var rv = (SqlValue)cx.obs[right];
            switch (kind)
            {
                case Sqlx.AND:
                    j = (JoinPart)lv.JoinCondition(cx, j, ref joinCond, ref where);
                    j = (JoinPart)rv.JoinCondition(cx, j, ref joinCond, ref where);
                    break;
                case Sqlx.LSS:
                case Sqlx.LEQ:
                case Sqlx.GTR:
                case Sqlx.GEQ:
                case Sqlx.NEQ:
                case Sqlx.EQL:
                    {
                        var lq = (Query)cx.obs[j.left];
                        var rq = (Query)cx.obs[j.right];
                        if (lv.isConstant(cx))
                        {
                            if (kind != Sqlx.EQL)
                                break;
                            if (rv.IsFrom(cx,lq, false) && rv.defpos > 0)
                            {
                                lq.AddMatch(cx, rv, lv.Eval(cx));
                                return j;
                            }
                            else if (rv.IsFrom(cx, rq, false) && rv.defpos > 0)
                            {
                                rq.AddMatch(cx, rv, lv.Eval(cx));
                                return j;
                            }
                            break;
                        }
                        if (rv.isConstant(cx))
                        {
                            if (kind != Sqlx.EQL)
                                break;
                            if (lv.IsFrom(cx, lq, false) && lv.defpos > 0)
                            {
                                lq.AddMatch(cx, lv, rv.Eval(cx));
                                return j;
                            }
                            else if (lv.IsFrom(cx, rq, false) && lv.defpos > 0)
                            {
                                rq.AddMatch(cx, lv, rv.Eval(cx));
                                return j;
                            }
                            break;
                        }
                        var ll = lv.IsFrom(cx, lq, true);
                        var rr = rv.IsFrom(cx, rq, true);
                        if (ll && rr)
                            return j += (JoinPart.JoinCond, joinCond += (defpos, true));
                        var rl = rv.IsFrom(cx, lq, true);
                        var lr = lv.IsFrom(cx, rq, true);
                        if (rl && lr)
                        {
                            var nv = new SqlValueExpr(defpos, cx, Sqlx.EQL, rv, lv, mod);
                            return j += (JoinPart.JoinCond, joinCond + (nv.defpos, true));
                        }
                        break;
                    }
            }
            return base.JoinCondition(cx, j, ref joinCond, ref where);
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
            var dm = domain;
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
                            Domain ct = lf.domain;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = rg.Eval(cx);
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return lf.Eval(cx);
                                Domain nt = new Domain(defpos,ct.kind, BTree<long, object>.Empty
                                    + (Domain.Precision, ct.prec) + (Domain.Charset, ct.charSet)
                                    + (Domain.Culture, new CultureInfo(cname)));
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
                            var ld = lf.domain;
                            var rd = rg.domain;
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
                            var rd = rg.domain;
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
                        v = lf.Eval(cx);
                        if (v != null)
                            v = v[right];
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
                                var rd = rg.domain;
                                v = rd.Eval(defpos,cx,new TInt(0), Sqlx.MINUS, tb);
                                return v;
                            }
                            var ta = lf.Eval(cx);
                            if (ta == null)
                                return null;
                            var ld = lf.domain;
                            v = ld.Eval(defpos,cx,ta, kind, tb);
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
                            var ld = lf.domain;
                            return ld.Eval(defpos,cx,ta, kind, tb);
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
                            var ld = lf.domain;
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
                            var ld = lf.domain;
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
                throw new DBException("22000").ISO();
            }
        }
        static BTree<long,object> _Type(long dp, Context cx, Sqlx kind, Sqlx mod, 
            SqlValue left, SqlValue right, BTree<long,object>mm = null)
        {
            mm = mm ?? BTree<long, object>.Empty;
           if (left != null)
                mm += (Left, left.defpos);  
            if (right != null)
                mm += (Right, right.defpos);
            var cs = CList<long>.Empty;
            var dm = Domain.Content;
            var nm = (string)mm?[Name]??""; 
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: dm = right.domain; cs = left.columns;  nm = left.name; break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON: dm = left.domain; nm = left.name; cs = right.columns;  break;// JavaScript
                case Sqlx.CONCATENATE: dm = Domain.Char; break;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        var dl = left.domain;
                        var dr = right.domain;
                        if (dl.kind == Sqlx.INTERVAL && (dr.kind == Sqlx.INTEGER 
                            || dr.kind == Sqlx.NUMERIC))
                        { dm = left.domain; break; }
                        dm = left.FindType(cx,Domain.UnionNumeric); break;
                    }
                case Sqlx.DOT: dm = right.domain; cs = right.columns;
                    if (left!=null && left.name!="" && right.name!="")
                        nm = left.name + "." + right.name; 
                    break;
                case Sqlx.EQL: dm = Domain.Bool; break;
                case Sqlx.EXCEPT: dm = left.domain; break;
                case Sqlx.GTR: dm = Domain.Bool; break;
                case Sqlx.INTERSECT: dm = left.domain; break;
                case Sqlx.LOWER: dm = Domain.Int; break; // JavaScript >> and >>>
                case Sqlx.LSS: dm = Domain.Bool; break;
                case Sqlx.MINUS:
                    if (left != null)
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME)
                        {
                            if (dr == dl)
                                dm = Domain.Interval;
                            else if (dr == Sqlx.INTERVAL)
                                dm = left.domain;
                        }
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            dm = right.domain; 
                        else
                            dm = left.FindType(cx,Domain.UnionDateNumeric);
                        break;
                    }
                    dm = right.FindType(cx,Domain.UnionDateNumeric); break;
                case Sqlx.NEQ: dm = Domain.Bool; break;
                case Sqlx.LEQ: dm = Domain.Bool; break;
                case Sqlx.GEQ: dm = Domain.Bool; break;
                case Sqlx.NO: dm = left.domain; break;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if ((dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME) && dr == Sqlx.INTERVAL)
                            dm = left.domain;
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            dm = right.domain;
                        else
                            dm = left.FindType(cx,Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                        dm = Domain.Content; break;
                case Sqlx.RBRACK:
                        dm= new Domain(Sqlx.ARRAY, left.domain); break;
                case Sqlx.SET: dm = left.domain; cs = left.columns;  nm = left.name; break; // JavaScript
                case Sqlx.TIMES:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.NUMERIC || dr == Sqlx.NUMERIC)
                            dm = Domain.Numeric;
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            dm = left.domain;
                        else if (dr == Sqlx.INTERVAL && (dl == Sqlx.INTEGER || dl == Sqlx.NUMERIC))
                            dm = right.domain;
                        else
                            dm = left.FindType(cx,Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.UNION: dm = left.domain; nm = left.name; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            return mm + (_Domain, dm) + (Name, nm) + (_Columns,cs);
        }
        internal override SqlValue Invert(Context cx)
        {
            var lv = (SqlValue)cx.obs[left];
            var rv = (SqlValue)cx.obs[right];
            switch (kind)
            {
                case Sqlx.AND:
                    return new SqlValueExpr(defpos, cx, Sqlx.OR, lv.Invert(cx),
                        rv.Invert(cx), Sqlx.NULL);
                case Sqlx.OR:
                    return new SqlValueExpr(defpos, cx, Sqlx.AND, lv.Invert(cx),
                        rv.Invert(cx), Sqlx.NULL);
                case Sqlx.NOT: return lv;
                case Sqlx.EQL: return new SqlValueExpr(defpos, cx, Sqlx.NEQ, lv, rv, Sqlx.NULL);
                case Sqlx.GTR: return new SqlValueExpr(defpos, cx, Sqlx.LEQ, lv, rv, Sqlx.NULL);
                case Sqlx.LSS: return new SqlValueExpr(defpos, cx, Sqlx.GEQ, lv, rv, Sqlx.NULL);
                case Sqlx.NEQ: return new SqlValueExpr(defpos, cx, Sqlx.EQL, lv, rv, Sqlx.NULL);
                case Sqlx.GEQ: return new SqlValueExpr(defpos, cx, Sqlx.LSS, lv, rv, Sqlx.NULL);
                case Sqlx.LEQ: return new SqlValueExpr(defpos, cx, Sqlx.GTR, lv, rv, Sqlx.NULL);
            }
            return base.Invert(cx);
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the SqlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Context cx,Query q,SqlValue v)
        {
            var e = v as SqlValueExpr;
            var lv = cx.obs[left] as SqlValue;
            var dm = domain;
            if (e == null || (dm != null && dm != v.domain))
                return false;
            if (lv != null && !lv._MatchExpr(cx, q,cx.obs[e.left] as SqlValue))
                return false;
            if (cx.obs[e.left] != null)
                return false;
            if (cx.obs[right] is SqlValue rv && !rv._MatchExpr(cx, q,cx.obs[e.right] as SqlValue))
                return false;
            if (cx.obs[e.right] != null)
                return false;
            return true;
        }
        internal override CTree<long, TypedValue> AddMatch(Context cx, CTree<long, TypedValue> ma,
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
            return base.AddMatch(cx, ma);
        }
        /// <summary>
        /// analysis stage Conditions()
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            //      var needed = BTree<SqlValue, int>.Empty;
            var oq = q;
            var lv = cx.obs[left] as SqlValue; // might be DBObject
            var rv = (SqlValue)cx.obs[right];
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
                        q = lv.Conditions(cx, q, false, out _);
                        q = rv.Conditions(cx, q, false, out _);
                        break;
                    }
                case Sqlx.EQL:
                    {
                        if (!disj)
                            goto case Sqlx.OR;
                        if (rv.isConstant(cx) && lv.IsFrom(cx, q, false) 
                            && cx.obs[left].defpos > 0)
                        {
                            q.AddMatch(cx, lv, rv.Eval(cx));
                            move = true;
                            return q;
                        }
                        else if (lv.isConstant(cx) && rv.IsFrom(cx, q, false) && rv.defpos > 0)
                        {
                            q.AddMatch(cx, rv, cx.obs[left].Eval(cx));
                            move = true;
                            return q;
                        }
                        goto case Sqlx.AND;
                    }
                case Sqlx.NO:
                case Sqlx.NOT:
                    {
                        q = lv.Conditions(cx, q, false, out _);
                        break;
                    }
            }
            move = false;
            return (Query)oq.New(cx,q.mem);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            var r = qn;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx,r) ?? r;
            r = ((SqlValue)cx.obs[right])?.Needs(cx,r) ?? r;
            return r;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            var r = CTree<long,RowSet.Finder>.Empty;
            if (cx.obs[left] is SqlValue sv)
                r = sv.Needs(cx, rs);
            if (cx.obs[right] is SqlValue sw)
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
        internal override string ToString(string sg,CTree<string, CTree<long, long>> cs, Context cx)
        {
            var sb = new StringBuilder();
            var lp = false;
            if (left >= 0 && right >= 0 && kind != Sqlx.LBRACK && kind != Sqlx.LPAREN)
            {
                sb.Append("(");
                lp = true;
            }
            if (left >= 0)
                sb.Append(cx.obs[left].ToString(sg,cs,cx));
            sb.Append(For(kind));
            if (right>=0)
                sb.Append(cx.obs[right].ToString(sg,cs,cx));
            if (kind == Sqlx.LBRACK)
                sb.Append("]");
            if (lp || kind == Sqlx.LPAREN)
                sb.Append(")");
            if (alias != null)
            {
                sb.Append(" as ");
                sb.Append(alias);
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A SqlValue that is the null literal
    /// </summary>
    internal class SqlNull : SqlValue
    {
        internal readonly static SqlNull Value = new SqlNull();
        /// <summary>
        /// constructor for null
        /// </summary>
        /// <param name="cx">the context</param>
        SqlNull()
            : base(-1L,new BTree<long,object>(_Domain,Domain.Null))
        { }
        /// <summary>
        /// the value of null
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return TNull.Value;
        }
        internal override bool _MatchExpr(Context cx, Query q,SqlValue v)
        {
            return v is SqlNull;
        }
        internal override Query Conditions(Context cx, Query q, bool disj,out bool move)
        {
            move = true;
            return q;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override Basis _Relocate(Writer wr)
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
        internal override string ToString(string sg,CTree<string, CTree<long,long>> cs, Context cx)
        {
            return "null";
        }
    }
    internal class SqlSecurity : SqlValue
    {
        internal SqlSecurity(long dp) : base(dp, "SECURITY", Domain._Level) { }
        protected SqlSecurity(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSecurity(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlSecurity(dp,mem);
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
    }
    /// <summary>
    /// Added for LogRowsRowSet and similar: Values are computed in Cursor constructor
    /// and do not depend on other SqlValues
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlFormal(dp,mem);
        }
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return CTree<long,bool>.Empty;
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return true;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
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
    /// </summary>
    internal class SqlLiteral : SqlValue
    {
        internal const long
            _Val = -317;// TypedValue
        internal TypedValue val=>(TypedValue)mem[_Val];
        internal readonly static SqlLiteral Null = new SqlLiteral(--_uid,Context._system,TNull.Value);
        internal override long target => -1;
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(long dp, Context cx, TypedValue v, Domain td=null,CList<long>cols=null) 
            : base(dp,BTree<long,object>.Empty+(_Domain,td??v.dataType)+(_Val, v)
                  +(_Columns,cols??CList<long>.Empty))
        {
            if (dp == -1L)
                throw new PEException("PE999");
        }
        public SqlLiteral(long dp, BTree<long, object> m) : base(dp, m) 
        {
            if (dp == -1L)
                throw new PEException("PE999");
        }
        public static SqlLiteral operator+(SqlLiteral s,(long,object)x)
        {
            return new SqlLiteral(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlLiteral(defpos,m);
        }
        public SqlLiteral(long dp, Domain dt) : base(dp, BTree<long, object>.Empty
            + (_Domain, dt) + (_Val, dt.defaultValue))
        { }
        internal override DBObject Relocate(long dp)
        {
            return new SqlLiteral(dp,mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlLiteral)base.Fix(cx);
            var nv = val.Fix(cx);
            if (nv != val)
                r += (_Val, nv);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlLiteral)base._Relocate(wr);
            r += (_Val, val.Relocate(wr));
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override Query Conditions(Context cx, Query q, bool disj,out bool move)
        {
            move = true;
            return q;
        }
        /// <summary>
        /// test for structural equivalence
        /// </summary>
        /// <param name="v">an SqlValue</param>
        /// <returns>whether they are structurally equivalent</returns>
        internal override bool _MatchExpr(Context cx,Query q,SqlValue v)
        {
            var c = v as SqlLiteral;
            if (c == null || (domain != null && domain != v.domain))
                return false;
            return val == c.val;
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue Eval(Context cx)
        {
            if (val is TQParam tq && cx.values[tq.qid] is TypedValue tv && !tv.IsNull)
                return tv;
            return val ?? domain.defaultValue;
        }
        public override int CompareTo(object obj)
        {
            var that = obj as SqlLiteral;
            if (that == null)
                return 1;
            return val?.CompareTo(that.val) ?? throw new PEException("PE000");
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return true;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return true;
        }
        /// <summary>
        /// A literal is supplied by any query
        /// </summary>
        /// <param name="q">the query</param>
        /// <returns>true</returns>
        internal override bool IsFrom(Context cx,Query q,bool ordered,Domain ut=null)
        {
            return true;
        }
        internal override bool isConstant(Context cx)
        {
            return !(val is TQParam);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return CTree<long,RowSet.Finder>.Empty;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return true;
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
    /// </summary>
    internal class SqlDateTimeLiteral : SqlLiteral
    {
        /// <summary>
        /// construct a datetime literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="op">the data type</param>
        /// <param name="n">the string version of the date/time</param>
        public SqlDateTimeLiteral(long dp, Context cx, Domain op, string n)
            : base(dp, cx, op.Parse(dp,n))
        {}
        protected SqlDateTimeLiteral(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDateTimeLiteral operator+(SqlDateTimeLiteral s,(long,object)x)
        {
            return new SqlDateTimeLiteral(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDateTimeLiteral(defpos,m);
        }
    }
    /// <summary>
    /// A Row value
    /// </summary>
    internal class SqlRow : SqlValue
    {
        public SqlRow(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(long dp, Context cx, Domain xp, BList<SqlValue> vs,BTree<long,object>m=null)
            : base(dp, _Inf(m,xp,vs) + (Dependents, _Deps(vs)) 
                  + (Depth, 1 + _Depth(vs)))
        { }
        public SqlRow(long dp, Context cx, Domain xp, CList<long> vs, BTree<long, object> m = null)
            : base(dp, _Inf(dp, cx, m, xp,vs) + (Dependents, _Deps(vs))
                  + (Depth, cx.Depth(vs)))
        { }
        protected static BTree<long,object> _Inf(BTree<long, object> m, 
            Domain xp,BList<SqlValue> vs)
        {
            var cs = CList<long>.Empty;
            var dm = Domain.Row;
            var ch = false;
            var cb = xp.First();
            for (var b = vs.First(); b != null; b = b.Next(),cb=cb?.Next())
            {
                var sv = b.value();
                var cd = xp.representation[cb?.value() ?? -1L];
                cs += sv.defpos;
                ch = ch || cd == null || sv.domain.CompareTo(cd) != 0;
                dm += (cb?.value()??sv.defpos, sv.domain);
            }
            return (m ?? BTree<long, object>.Empty)+(_Columns,cs) + (_Domain,ch?dm:xp);
        }
        protected static BTree<long, object> _Inf(long dp, Context cx, BTree<long, object> m,
    Domain xp, CList<long> vs)
        {
            var dm = Domain.Row;
            var ch = false;
            var cb = xp.First();
            for (var b = vs.First(); b != null; b = b.Next(),cb=cb?.Next())
            {
                var ob = cx.obs[b.value()];
                var cd = xp.representation[cb?.value() ?? -1L];
                ch = ch || cd == null || ob.domain.CompareTo(cd) != 0; 
                dm += (cb?.value()??ob.defpos, ob.domain);
            }
            return (m ?? BTree<long, object>.Empty) + (_Columns, vs) + (_Domain, ch?dm:xp);
        }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return (SqlRow)s.New(s.mem + m);
        }
        public static SqlRow operator +(SqlRow s, SqlValue sv)
        {
            return (SqlRow)s.New(s.mem + (_Columns,s.columns+sv.defpos))
                +(_Domain, s.domain+(sv.defpos,sv.domain));
        }
        internal override SqlValue Col(Context cx, int i)
        {
            return (SqlValue)cx.obs[columns[i]];
        }
        internal override TypedValue Eval(Context cx, int i)
        {
            return Col(cx, i).Eval(cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRow(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlRow)base._Relocate(wr);
            r += (_Columns, wr.Fix(columns));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlRow)base.Fix(cx);
            var nv = cx.Fix(columns);
            if (nv!=columns)
            r += (_Columns, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var cs = CList<long>.Empty;
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=columns.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)cx._Replace(b.value(),so,sv);
                cs += v.defpos;
                vs += v;
                if (v.defpos != b.value())
                    ch = true;
            }
            if (ch)
            {
                var dm = new Domain(Sqlx.ROW, vs);
                r = r+ (_Columns, cs) +(_Domain,dm) + (Dependents, _Deps(vs))
                  + (Depth, 1 + _Depth(vs)); // don't use "new SqlRow" here as it won't work for SqlNewRow
            }
            r = (SqlRow)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override (SqlValue, Query) Resolve(Context cx, Query fm, string a = null)
        {
            if (domain.kind != Sqlx.CONTENT)
                return (this, fm);
            var cs = BList<SqlValue>.Empty;
            for (var b = columns.First(); b != null; b = b.Next())
            {
                var c = (SqlValue)cx.obs[b.value()];
                SqlValue v;
                (v, fm) = c.Resolve(cx, fm, a);
                cs += v;
            }
            var sv = new SqlRow(defpos, cx, Domain.Row, cs);
            var r = (SqlRow)cx.Replace(this, sv);
            return (r,fm);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var cs = CList<long>.Empty;
            var ch = false;
            for (var b=r.columns.First();b!=null;b=b.Next())
            {
                var a = ((SqlValue)cx.obs[b.value()]).AddFrom(cx,q);
                if (a.defpos != b.value())
                    ch = true;
                cs += a.defpos;
            }
            if (ch)
                r += (_Columns, cs);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            for (var b=columns.First();b!=null;b=b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx,q))
                        return false;
            return true;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            for (var b = columns.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, r))
                    return false;
            return true;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (cx.values[defpos] is TRow r && !r.IsNull)
                return r;
            var vs = CTree<long,TypedValue>.Empty;
            for (var b=columns.First();b!=null;b=b.Next())
            {
                var s = b.value();
                vs += (s, cx.obs[s].Eval(cx));
            }
            return new TRow(domain, vs);
        }
        internal override bool aggregates(Context cx)
        {
            for (var b = columns.First(); b!=null;b=b.Next())
                if (cx.obs[b.value()].aggregates(cx))
                    return true;
            return false;
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            for (var b=columns.First(); b!=null;b=b.Next())
                tg = cx.obs[b.value()].StartCounter(cx,rs,tg);
            return tg;
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            for (var b = columns.First(); b != null; b = b.Next())
                tg = cx.obs[b.value()].AddIn(cx,rb, tg);
            return tg;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            for (var b = columns.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "";
            sb.Append(" [");
            for (var b=columns.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            sb.Append("]");
            return sb.ToString();
        }
        internal override string ToString(string sg,CTree<string, CTree<long,long>> cs, Context cx)
        {
            var sb = new StringBuilder();
            var cm = "";
            sb.Append(" (");
            for (var b = columns.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(cx.obs[b.value()].ToString(sg,cs,cx));
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// Prepare an SqlValue with reified columns for use in trigger
    /// </summary>
    internal class SqlOldRow : SqlRow
    {
        internal SqlOldRow(Ident ic, Context cx, PTrigger tg, From fm)
            : base(ic.iix, BTree<long, object>.Empty + (_Domain, fm.domain) 
                  + (Name, ic.ident) + (_Columns, fm.rowType) + (_From, fm.defpos))
        { }
        protected SqlOldRow(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlOldRow(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlOldRow(dp,mem);
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
    internal class SqlNewRow : SqlRow
    {
        internal string label => (string)mem[Executable.Label];
        internal SqlNewRow(Ident ic, Context cx, PTrigger tg,From fm)
            : base(ic.iix, BTree<long, object>.Empty + (_Domain, fm.domain) + (Name, ic.ident)
                  + (_Columns,fm.rowType)+(_From,fm.defpos)
                  +(Executable.Label,tg.name))
        { }
        protected SqlNewRow(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override DBObject Relocate(long dp)
        {
            return new SqlNewRow(dp, mem);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlNewRow(defpos, m);
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
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = -319; // CList<long>
        internal CList<long> rows =>
            (CList<long>)mem[Rows]?? CList<long>.Empty;
        public SqlRowArray(long dp,Context cx,Domain ap,CList<long> rs) 
            : base(dp, BTree<long,object>.Empty+(_Domain,ap)+(Rows, rs))
        { }
        internal SqlRowArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlRowArray operator+(SqlRowArray s,(long,object)x)
        {
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlRowArray(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (Rows, wr.Fix(rows));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlRowArray)base.Fix(cx);
            var nr = cx.Fix(rows);
            if (nr != rows)
                r += (Rows, nr);
            return r;
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
                var v = cx.Replace(b.value(),so,sv);
                ch = ch || v != b.value();
                rws += v;
            }
            if (ch)
                r += (Rows, rws);
            r = (SqlRowArray)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlRowArray)base.AddFrom(cx, q);
            var rws = CList<long>.Empty;
            var ch = false;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var o = (SqlRow)cx.obs[b.value()];
                var a = (SqlRow)o.AddFrom(cx, q);
                if (a.defpos != b.value())
                    ch = true;
                rws += a.defpos;
            }
            if (ch)
                r += (Rows, rws);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return true;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, r))
                    return false;
            return true;
        }
        internal override bool Uses(Context cx,long t)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.value()]).Uses(cx,t))
                    return true;
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            var vs = BList<TypedValue>.Empty;
            for (var b=rows.First(); b!=null; b=b.Next())
                vs += cx.obs[b.value()].Eval(cx);
            return new TArray(domain,vs);
        }
        internal override RowSet RowSet(long dp,Context cx, Domain xp)
        {
            var rs = BList<(long,TRow)>.Empty;
            var isConst = true;
            for (var b = rows.First(); b != null && isConst; b = b.Next())
            {
                var v = cx.obs[b.value()];
                isConst = (v as SqlValue)?.isConstant(cx) == true;
                rs += (v.defpos,new TRow(xp,v.Eval(cx).ToArray()));
            }
            if (isConst)
                return new ExplicitRowSet(dp, xp, rs);
            return new SqlRowSet(dp, cx, xp, rows);
        }
        internal override bool aggregates(Context cx)
        {
            for (var b=rows.First(); b!=null; b=b.Next())
                if (cx.obs[b.value()].aggregates(cx))
                    return true;
            return false;
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t,rows, VIC.OK | VIC.OV));
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b=rows.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.value()));
            }
            return sb.ToString();
        }
    }
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
        public SqlXmlValue(long dp, Context cx, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, (m ?? BTree<long, object>.Empty) + (_Domain, Domain.XML) 
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlXmlValue(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlXmlValue)base._Relocate(wr);
            r += (Attrs, wr.Fix(attrs));
            r += (Children, wr.Fix(children));
            r += (Content, wr.Fixed(content)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlXmlValue)base.Fix(cx);
            var na = cx.Fix(attrs);
            if (na!=attrs)
            r += (Attrs, na);
            var nc = cx.Fix(children);
            if (nc!=children)
            r += (Children, nc);
            var nv = cx.obuids[content] ?? content;
            if (content!=nv)
                r += (Content, nv);
            return r;
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
                var v = cx.Replace(ao,so,sv);
                if (v != ao)
                    at = new BList<(XmlName,long)>(at,b.key(), (n, v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = cx.Replace(r.content,so,sv);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for(var b=ch?.First();b!=null;b=b.Next())
            {
                var v = cx.Replace(b.value(),so,sv);
                if (v != b.value())
                    ch = new CList<long>(ch, b.key(), v);
            }
            if (ch != r.children)
                r += (Children, ch);
            r = (SqlXmlValue)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[content]).Needs(cx,qn);
            for (var b = attrs.First(); b != null; b = b.Next())
            {
                var (n, a) = b.value();
                qn = ((SqlValue)cx.obs[a]).Needs(cx, qn);
            }
            for (var b = children.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            t = Scan(t,content, VIC.OK | VIC.OV);
            t = Scan(t, children, VIC.OK | VIC.OV);
            for (var b=attrs.First();b!=null;b=b.Next())
            {
                var (_, p) = b.value();
                t = Scan(t, p, VIC.OK | VIC.OV);
            }
            return base.Scan(t);
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
        internal const long
            ArrayValuedQE = -327; // long QueryExpression
        public long aqe => (long)(mem[ArrayValuedQE]??-1L);
        public SqlSelectArray(long dp, QueryExpression qe, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty
                  + (_Domain, qe.domain) + (ArrayValuedQE, qe.defpos))) { }
        protected SqlSelectArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator+(SqlSelectArray s,(long,object)x)
        {
            return new SqlSelectArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSelectArray(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlSelectArray(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlSelectArray)base._Relocate(wr);
            r += (ArrayValuedQE, wr.Fixed(aqe).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlSelectArray)base.Fix(cx);
            var nq = cx.obuids[aqe] ?? aqe;
            if (nq!=aqe)
            r += (ArrayValuedQE, nq);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlSelectArray)base._Replace(cx,so,sv);
            var ae = cx.Replace(r.aqe,so,sv);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            r = (SqlSelectArray)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override bool Uses(Context cx,long t)
        {
            return ((SqlValue)cx.obs[aqe]).Uses(cx,t);
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = domain;
            var va = BList<TypedValue>.Empty;
            var q = (Query)cx.obs[aqe];
            var ars = q.RowSets(cx, cx.data[from]?.finder??CTree<long, RowSet.Finder>.Empty);
            var et = dm.elType;
            var nm = q.name;
            for (var rb=ars.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb;
                if (et==null)
                    va += rw[nm];
                else
                {
                    var vs = new TypedValue[q.display];
                    for (var i = 0; i < q.display; i++)
                        vs[i] = rw[i];
                    va += new TRow(ars, vs);
                }
            }
            return new TArray(dm,va);
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[aqe].aggregates(cx);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t,aqe, VIC.OK | VIC.OV));
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
        public SqlValueArray(long dp,Context cx,Domain xp,CList<long> v)
            : base(dp,BTree<long,object>.Empty+(_Domain,xp)+(_Columns,xp.rowType) +(Array,v))
        { }
        protected SqlValueArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueArray operator+(SqlValueArray s,(long,object)x)
        {
            return new SqlValueArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueArray(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueArray(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlValueArray)base._Relocate(wr);
            r += (Array, wr.Fix(array));
            r += (Svs, wr.Fixed(svs)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlValueArray)base.Fix(cx);
            r += (Array, cx.Fix(array));
            if (svs>=0)
                r += (Svs, cx.obuids[svs]??svs);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueArray)base._Replace(cx,so,sv);
            var ar = r.array;
            for (var b=ar?.First();b!=null;b=b.Next())
            {
                var v = cx.Replace(b.value(),so,sv);
                if (v != b.value())
                    ar += (b.key(), v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = cx.Replace(r.svs, so, sv);
            if (ss != r.svs)
                r += (Svs, ss);
            r = (SqlValueArray)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = CList<long>.Empty;
            var ch = false;
            for (var b=array.First();b!=null;b=b.Next())
            {
                var a = ((SqlValue)cx.obs[b.value()]).AddFrom(cx, q);
                if (a.defpos != b.value())
                    ch = true;
                ar += a.defpos;
            }
            if (ch)
                r += (Array, ar);
            var s = ((SqlValue)cx.obs[svs])?.AddFrom(cx, q);
            if (s.defpos != svs)
                r += (Svs,s.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return ((SqlValue)cx.obs[svs])?.KnownBy(cx, q) != false;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, r))
                    return false;
            return ((SqlValue)cx.obs[svs])?.KnownBy(cx, r) != false;
        }
        internal override bool Uses(Context cx,long t)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.value()]).Uses(cx,t))
                    return true;
            return ((SqlValue)cx.obs[svs])?.Uses(cx,t)==true;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var dm = domain;
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
        internal override bool aggregates(Context cx)
        {
            for (var b = array.First(); b != null; b = b.Next())
                if (cx.obs[b.value()].aggregates(cx))
                    return true;
            return cx.obs[svs]?.aggregates(cx) ?? false;
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            for (var b = array.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,qn);
            qn = ((SqlValue)cx.obs[svs])?.Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, array, VIC.OK | VIC.OV);
            t = Scan(t, svs, VIC.OK | VIC.OV);
            return base.Scan(t);
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
        internal const long
            Expr = -330, // long Query
            Source = -331; // string
        /// <summary>
        /// the subquery
        /// </summary>
        public long expr =>(long)(mem[Expr]??-1L);
 //       public Domain targetType => (Domain)mem[TargetType];
        public string source => (string)mem[Source];
        public SqlValueSelect(long dp, Query q, string s)
            : base(dp, BTree<long, object>.Empty 
                  + (Expr, q.defpos) + (Source, s) + (_Domain, q.domain)
                  + (Dependents, new CTree<long, bool>(q.defpos, true))
                  + (Depth, q.depth + 1))
        { }
        protected SqlValueSelect(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueSelect operator+(SqlValueSelect s,(long,object)x)
        {
            return new SqlValueSelect(s.defpos, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueSelect(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueSelect(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlValueSelect)base._Relocate(wr);
            var e = (Query)wr.Fixed(expr);
            if (e.defpos != expr)
                r = r._Expr(wr.cx, e);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlValueSelect)base.Fix(cx);
            var ne = cx.obuids[expr] ?? expr;
            if (ne!=expr)
            r += (Expr,ne);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base._Replace(cx,so,sv);
            var ex = (Query)cx._Replace(r.expr,so,sv);
            if (ex.defpos != r.expr)
                r = r._Expr(cx,ex);
            r = (SqlValueSelect)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal SqlValueSelect _Expr(Context cx,Query e)
        {
            var d = Math.Max(depth, e.depth + 1);
            return this + (Expr, e.defpos) + (Dependents, dependents + (e.defpos,true)) + (Depth, d);
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((Query)cx.obs[expr]).Uses(cx,t);
        }
        internal override Query Conditions(Context cx, Query q, bool disj,out bool move)
        {
            move = false;
            return (Query)((Query)cx.obs[expr]).Conditions(cx);
        }
        internal override TypedValue Eval(Context cx)
        {
            var dm = domain;
            var ers = ((Query)cx.obs[expr])
                .RowSets(cx, cx.data[from]?.finder??CTree<long, RowSet.Finder>.Empty);
            if (dm.kind == Sqlx.TABLE)
            {
                var rs = BList<TypedValue>.Empty;
                for (var b = ers.First(cx); b != null; b = b.Next(cx))
                    rs += b;
                return new TArray(domain, rs);
            }
            var rb = ers.First(cx);
            if (rb == null)
                return dm.defaultValue;
            TypedValue tv = rb[0]; // rb._rs.qry.rowType.columns[0].Eval(cx);
   //        if (targetType != null)
   //             tv = targetType.Coerce(tv);
            return tv;
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[expr].aggregates(cx);
        }
        internal override RowSet RowSet(long dp,Context cx, Domain xp)
        {
            var r = ((Query)cx.obs[expr])
                .RowSets(cx, cx.data[from]?.finder?? CTree<long, RowSet.Finder>.Empty);
            cx.data += (expr, r);
            return r;
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return cx.obs[expr].Needs(cx, rs);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t, expr, VIC.OK | VIC.OV));
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
        public ColumnFunction(long dp, Context cx, CList<long> c)
            : base(dp, BTree<long, object>.Empty+(_Domain,Domain.Bool)+ (Bits, c)) { }
        protected ColumnFunction(long dp, BTree<long, object> m) :base(dp, m) { }
        public static ColumnFunction operator+(ColumnFunction s,(long,object)x)
        {
            return new ColumnFunction(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnFunction(defpos,mem);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ColumnFunction(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (Bits, wr.Fix(bits));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (ColumnFunction)base.Fix(cx);
            var nb = cx.Fix(bits);
            if (nb!=bits)
                r += (Bits, nb);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t,bits, VIC.OK | VIC.OV));
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
            Spec = -334; // long CursorSpecification
        internal long spec=>(long)(mem[Spec]??-1L);
        internal SqlCursor(long dp, CursorSpecification cs, string n) 
            : base(dp, BTree<long,object>.Empty+
                  (_Domain,cs.domain)+(Name, n)+(Spec,cs.defpos)
                  +(Dependents,new CTree<long,bool>(cs.defpos,true))
                  +(Depth,1+cs.depth))
        { }
        protected SqlCursor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCursor operator+(SqlCursor s,(long,object)x)
        {
            return new SqlCursor(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCursor(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCursor(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlCursor)base._Relocate(wr);
            r += (Spec, wr.Fixed(spec).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlCursor)base.Fix(cx);
            var ns = cx.obuids[spec] ?? spec;
            if (ns != spec)
                r += (Spec, ns);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCursor)base._Replace(cx,so,sv);
            var sp = cx.Replace(r.spec,so,sv);
            if (sp != r.spec)
                r += (Spec, sp);
            r = (SqlCursor)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override bool Uses(Context cx,long t)
        {
            return ((SqlValue)cx.obs[spec]).Uses(cx,t);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed 
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t, spec, VIC.OK | VIC.OV));
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
    internal class SqlCall : SqlValue
    {
        internal const long
            Call = -335; // long CallStatement
        public long call =>(long)(mem[Call]??-1L);
        public SqlCall(long dp, Context cx, CallStatement c, BTree<long,object>m=null)
            : base(dp, m??BTree<long, object>.Empty 
                  + (_Domain,((ObInfo)cx.role.infos[c.procdefpos])?.domain??Domain.Content)
                  + (Call, c.defpos)+(Dependents,new CTree<long,bool>(c.defpos,true))
                  +(Depth,1+c.depth)+(Name,c.name))
        {  }
        protected SqlCall(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCall operator+(SqlCall c,(long,object)x)
        {
            return (SqlCall)c.New(c.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCall(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCall(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlCall)base._Relocate(wr);
            r += (Call, wr.Fixed(call).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlCall)base.Fix(cx);
            var nc = cx.obuids[call] ?? call;
            if (nc != call)
                r += (Call, nc);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((CallStatement)cx.obs[call]).Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            var ca = cx.Replace(r.call,so,sv);
            if (ca != r.call)
                r += (Call, ca);
            r = (SqlCall)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            var ca = (CallStatement)cx.obs[call];
            for (var b=ca.parms.First();b!=null;b=b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx,q))
                        return false;
            return true;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            var ca = (CallStatement)cx.obs[call];
            for (var b = ca.parms.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, r))
                    return false;
            return true;
        }
        internal override bool Uses(Context cx,long t)
        {
           return ((CallStatement)cx.obs[call]).procdefpos==t;
        }
        internal override bool aggregates(Context cx)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b=c?.parms.First(); b!=null;b=b.Next())
                if (cx.obs[b.value()].aggregates(cx))
                    return true;
            return false;
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c.parms.First(); b != null; b = b.Next())
                tg = cx.obs[b.value()].StartCounter(cx, rs, tg);
            return tg;
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            var c = (CallStatement)cx.obs[call];
            for (var b = c?.parms.First(); b != null; b = b.Next())
                qn = ((SqlValue)cx.obs[b.value()]).Needs(cx,qn);
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t,call, VIC.OK | VIC.OV));
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
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(long dp, Context cx, CallStatement c) : base(dp, cx, c) { }
        protected SqlProcedureCall(long dp,BTree<long,object>m):base(dp,m) { }
        public static SqlProcedureCall operator+(SqlProcedureCall s,(long,object)x)
        {
            return new SqlProcedureCall(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlProcedureCall(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlProcedureCall(dp,mem);
        }
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            var c = (CallStatement)cx.obs[call];
            var dm = domain;
            try
            {
                var proc = (Procedure)tr.objects[c.procdefpos];
                proc.Exec(cx, c.parms);
                return cx.val??dm.defaultValue;
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
    /// </summary>
    internal class SqlMethodCall : SqlCall // instance methods
    {
        /// <summary>
        /// construct a new MethodCall SqlValue.
        /// At construction time the proc and target will be unknown
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="c">the call statement</param>
        public SqlMethodCall(long dp, Context cx, CallStatement c) : base(dp,cx,c)
        { }
        protected SqlMethodCall(long dp,BTree<long, object> m) : base(dp, m) { }
        public static SqlMethodCall operator+(SqlMethodCall s,(long,object)x)
        {
            return new SqlMethodCall(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlMethodCall(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlMethodCall(dp,mem);
        }
        internal override (SqlValue, Query) Resolve(Context cx, Query fm, string a = null)
        {
            SqlValue v;
            var c = (CallStatement)cx.obs[call];
            (v, fm) = base.Resolve(cx, fm, a);
            var mc = (SqlMethodCall)v;
            var ov = (SqlValue)cx.obs[c.var];
            (v, fm) = ov.Resolve(cx, fm, a);
            if (v != ov)
            {
                var oi = (ObInfo)cx.role.infos[v.domain.defpos]; // we need the names
                var p = oi.methodInfos[c.name]?[(int)c.parms.Count] ?? -1L;
                var nc = c + (CallStatement.Var, v.defpos) + (CallStatement.ProcDefPos, p);
                cx.Add(nc);
                var pr = cx.db.objects[p] as Procedure;
                mc = mc + (Call, nc.defpos) + (_Domain, pr.domain);
            }
            return ((SqlValue)cx.Add(mc), fm);
        }
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var c = (CallStatement)cx.obs[call];
            if (c.var == -1L)
                throw new PEException("PE241");
            var proc = (Method)cx.db.objects[c.procdefpos];
            return proc.Exec(cx, c.var, c.parms).val;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(long dp, Context cx, CallStatement c)
            : base(dp, cx, c)
        { }
        protected SqlConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlConstructor operator+(SqlConstructor s,(long,object)x)
        {
            return new SqlConstructor(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlConstructor(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlConstructor(dp,mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlConstructor)base.Fix(cx);
            return r;
        }
        internal override bool Uses(Context cx,long t)
        {
            for (var b=domain.rowType.First();b!=null;b=b.Next())
                if (b.value()==t)
                    return true;
            return false;
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
    }
    /// <summary>
    /// An SqlValue corresponding to a default constructor call
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
        public SqlDefaultConstructor(long dp, Context cx, Domain u, CList<long> ins)
            : base(dp, BTree<long, object>.Empty+(_Domain, u)
                  +(Sce,cx._Add(new SqlRow(cx.GetUid(),cx,u,ins)).defpos)
                  +(_Domain,u)+(Dependents,_Deps(ins))+(Depth,cx.Depth(ins)))
        { }
        protected SqlDefaultConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor s, (long, object) x)
        {
            return (SqlDefaultConstructor)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDefaultConstructor(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlDefaultConstructor(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (SqlDefaultConstructor)base._Relocate(wr);
            if (wr.uids[r.sce]==0)
                wr.cx.obs[r.sce]?._Relocate(wr);
            var sc = wr.Fix(r.sce);
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
            r = (SqlDefaultConstructor)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlDefaultConstructor)base.Fix(cx);
            var ns = cx.obuids[sce] ?? sce;
            if (ns != sce)
                r += (Sce, ns);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlDefaultConstructor)base.AddFrom(cx, q);
            var sc = (SqlRow)cx.obs[r.sce];
            var a = sc.AddFrom(cx, q);
            cx.Add(a);
            return (SqlValue)cx.Add(r);
        }
        internal override bool Uses(Context cx,long t)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() == t)
                    return true;
            return false;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
                for (var b=domain.representation.First();b!=null;b=b.Next(),i++)
                    vs += (b.key(),((SqlRow)cx.obs[sce]).Eval(cx,i));
                return new TRow(domain,vs);
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
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn+cx.obs[sce].Needs(cx);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            return base.Scan(Scan(t,sce, VIC.OK | VIC.OV));
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
    /// </summary>
    internal class SqlFunction : SqlValue
    {
        internal const long
            Filter = -338, //long SqlValue
            Mod = -340, // Sqlx
            Monotonic = -341, // bool
            Op1 = -342, // long SqlValue
            Op2 = -343, // long SqlValue
            Query = -344,//long Query
            _Val = -345,//long SqlValue
            Window = -346, // long WindowSpecification
            WindowId = -347; // long
        /// <summary>
        /// the query
        /// </summary>
        internal long query => (long)(mem[Query]??-1L);
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
        public long filter => (long)(mem[Filter]??-1L);
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
        public SqlFunction(long dp, Context cx, Sqlx f, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m,
            BTree<long, object> mm = null) :
            base(dp, _Mem(mm,dp,f,_Type(cx, f, vl, o1),vl,o1,o2)
                +(Name,f.ToString())+(Domain.Kind,f)+(Mod,m)+(Dependents,_Deps(vl,o1,o2)) +(Depth,_Depth(vl,o1,o2)))
        { }
        protected SqlFunction(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(BTree<long,object> m,long dp,Sqlx f,Domain dm,
            SqlValue vl, SqlValue o1, SqlValue o2)
        {
            m = (m ?? BTree<long, object>.Empty) + (_Domain, dm);
            if (vl != null)
                m += (_Val, vl.defpos);
            if (o1 != null)
                m += (Op1, o1.defpos);
            if (o2 != null)
                m += (Op2, o2.defpos);
            return m;
        }
        public static SqlFunction operator+(SqlFunction s,(Context,long,object)x)
        {
            var (cx, a, v) = x;
            var m = s.mem + (a, v);
            if (a == Op1)
                m += (_Domain, _Type(cx,s.kind, (SqlValue)cx.obs[s.val], (SqlValue)cx.obs[(long)v]));
            if (a == _Val)
                m += (_Domain, _Type(cx,s.kind, (SqlValue)cx.obs[(long)v], (SqlValue)cx.obs[s.op1]));
            return new SqlFunction(s.defpos, m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFunction(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlFunction(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (Filter, wr.Fixed(filter)?.defpos??-1L);
            r += (Op1, wr.Fixed(op1)?.defpos??-1L);
            r += (Op2, wr.Fixed(op2)?.defpos??-1L);
            r += (_Val, wr.Fixed(val)?.defpos??-1L);
            r += (Window, wr.Fixed(window)?.defpos??-1L);
            r += (WindowId, wr.Fix(windowId));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlFunction)base.Fix(cx);
            var nf = cx.obuids[filter] ?? filter;
            if (filter !=nf)
                r += (cx, Filter, nf);
            var n1 = cx.obuids[op1] ?? op1;
            if (op1 !=n1)
                r += (cx, Op1, n1);
            var n2 = cx.obuids[op2] ?? op2;
            if (op2 !=n2)
                r += (cx, Op2, n2);
            var nv = cx.obuids[val] ?? val;
            if (val !=nv)
                r += (cx, _Val, nv);
            var nw = cx.obuids[window] ?? window;
            if (window !=nw)
                r += (cx, Window, nw);
            var ni = cx.obuids[windowId] ?? windowId;
            if (windowId !=ni)
                r += (cx, WindowId, ni);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            return base.AddFrom(cx,q);
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
        static int _Depth(SqlValue vl, SqlValue o1, SqlValue o2)
        {
            int r = 0;
            if (vl != null)
                r = _Max(r, vl.depth);
            if (o1 != null)
                r = _Max(r, o1.depth);
            if (o2 != null)
                r = _Max(r, o2.depth);
            return 1 + r;
        }
        internal override (SqlValue,Query) Resolve(Context cx, Query fm,string a=null)
        {
            SqlValue vl = null, o1 = null, o2 = null;
            (vl,fm) = ((SqlValue)cx.obs[val])?.Resolve(cx, fm, a)??(vl,fm);
            (o1,fm) = ((SqlValue)cx.obs[op1])?.Resolve(cx, fm, a)??(o1,fm);
            (o2,fm) = ((SqlValue)cx.obs[op2])?.Resolve(cx, fm, a)??(o2,fm);
            if ((vl?.defpos??-1L) != val || (o1?.defpos??-1L) != op1 || 
                (o2?.defpos??-1L) != op2 || domain.kind==Sqlx.CONTENT)
                return ((SqlValue)cx.Replace(this,
                    new SqlFunction(defpos, cx, kind, vl, o1, o2, mod, mem)),fm);
            return (this,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlFunction)base._Replace(cx,so,sv);
            var fi = cx.Replace(r.filter, so, sv);
            if(fi != r.filter)
                r += (cx, Filter, fi);
            var o1 = cx.Replace(r.op1, so, sv);
            if (o1 != r.op1)
                    r += (cx, Op1, o1);
            var o2 = cx.Replace(r.op2, so, sv);
            if (o2 != r.op2)
                r += (cx, Op2, o2);
            var vl = cx.Replace(r.val, so, sv);
            if (vl != r.val)
                r += (cx, _Val, vl);
            var q = cx.Replace(r.query, so, sv);
            if (q != r.query)
                r += (cx, Query, q);
            if (domain.kind==Sqlx.UNION || domain.kind==Sqlx.CONTENT)
            {
                var dm = _Type(cx, kind, cx._Ob(val) as SqlValue, cx._Ob(op1) as SqlValue);
                if (dm!=null && dm!=domain)
                    r += (cx,_Domain, dm);
            }
            r = (SqlFunction)New(cx, r.mem);
            cx.done += (defpos, r);
            return cx.Add(r);
        }
        internal override SqlValue Operand(Context cx)
        {
            if (aggregates0())
                return (SqlValue)cx.obs[val];
            return ((SqlValue)cx.obs[val])?.Operand(cx);
        }
        /// <summary>
        /// Prepare Window Function evaluation
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal SqlFunction RowSets(Context cx,Query q)
        {
            // we first compute the needs of this window function
            // The key for registers will consist of partition/grouping columns
            // Each register has a rowset ordered by the order columns if any
            // for the moment we just use the whole source row
            // We build all of the WRS's at this stage for saving in f
            return this+(cx,Window,((WindowSpecification)(cx.obs[window])
            +(WindowSpecification.PartitionType, Domain.Row)));
        }
        internal override void Grouped(Context cx,GroupSpecification gs)
        {
            ((SqlValue)cx.obs[val])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[op1])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[op2])?.Grouped(cx, gs);
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return ((SqlValue)cx.obs[val])?.KnownBy(cx, q)!=false &&
            ((SqlValue)cx.obs[op1])?.KnownBy(cx, q)!=false &&
            ((SqlValue)cx.obs[op2])?.KnownBy(cx, q) !=false;  
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return ((SqlValue)cx.obs[val])?.KnownBy(cx, r) != false &&
            ((SqlValue)cx.obs[op1])?.KnownBy(cx, r) != false &&
            ((SqlValue)cx.obs[op2])?.KnownBy(cx, r) != false;
        }
        internal override 
            (SqlValue, BTree<SqlValue, string>, BTree<long, string>, 
            BTree<string, bool?>,BTree<long,SqlValue>) 
            _ColsForRestView(Context cx, string sg, Query gf, GroupSpecification gs, 
            BTree<SqlValue,string> gfc, BTree<long, string> rem, BTree<string, bool?> reg,
            BTree<long,SqlValue>map)
        {
            var ac = "C_" + defpos;
            var vl = (SqlValue)cx.obs[val];
            var an = alias ?? ac;
            switch (kind)
            {
                case Sqlx.AVG:
                    {
                        var n0 = ac;
                        var n1 = "D_" + defpos;
                        var u = cx.GetUid(9);
                        var c0 = new SqlFunction(u, cx, Sqlx.SUM, vl, null, null, Sqlx.NO,
                            new BTree<long, object>(_Alias, n0));
                        var c1 = new SqlFunction(u + 1, cx, Sqlx.COUNT, vl, null, null, Sqlx.NO,
                            new BTree<long, object>(_Alias, n1));
                        cx.Add(c0);
                        cx.Add(c1);
                        rem = rem + (c0.defpos, n0) + (c1.defpos, n1);
                        var s0 = new SqlValue(u + 2, "", domain, null, BTree<long, object>.Empty
                            + (Query, gf));
                        cx.Add(s0);
                        var s1 = new SqlValue(u + 3, "", Domain.Int, null, BTree<long, object>.Empty
                            + (Query, gf));
                        cx.Add(s1);
                        var ct = (SqlValue)cx.Add(new SqlValueExpr(u + 4, cx, Sqlx.DIVIDE,
                                (SqlValue)cx.Add(new SqlValueExpr(u + 5, cx, Sqlx.TIMES,
                                    (SqlValue)cx.Add(new SqlFunction(u + 6, cx, Sqlx.SUM, s0, null, null,
                                        Sqlx.NO)),
                                    (SqlValue)cx.Add(new SqlLiteral(u + 7, cx, new TReal(Domain.Real, 1.0))), Sqlx.NO)),
                                (SqlValue)cx.Add(new SqlFunction(u + 8, cx, Sqlx.SUM, s1, null, null, Sqlx.NO)),
                                Sqlx.NO, new BTree<long, object>(_Alias, an)));
                        gfc = gfc + (s0, n0) + (s1, n1);
                        cx._Add(ct); // check defpos==ct.defpos
                        return (ct,gfc,rem,reg,map);
                    }
                case Sqlx.EXTRACT:
                    {
                        var u = cx.GetUid(2);
                        var ct = (SqlValue)cx.Add(new SqlFunction(u, cx, mod, vl, null, null, Sqlx.NO,
                        new BTree<long, object>(_Alias, an)));
                        SqlValue st = ct;
                        rem += (ct.defpos, an);
                        st = (SqlValue)cx.Add(new SqlValue(u + 1, "", Domain.Int, null, BTree<long, object>.Empty
                            + (Query, gf)));
                        gfc += (st, an);
                        map += (defpos, st);
                        return (st,gfc,rem,reg,map);
                    }
                case Sqlx.STDDEV_POP:
                    {
                        var n0 = ac;
                        var n1 = "D_" + defpos;
                        var n2 = "E_" + defpos;
                        var u = cx.GetUid(14);
                        var c0 = (SqlValue)cx.Add(new SqlFunction(u, cx, Sqlx.SUM, vl, null, null, Sqlx.NO,
                            new BTree<long, object>(_Alias, n0)));
                        var c1 = (SqlValue)cx.Add(new SqlFunction(u + 1, cx, Sqlx.COUNT, vl, null, null, Sqlx.NO,
                            new BTree<long, object>(_Alias, n1)));
                        var c2 = (SqlValue)cx.Add(new SqlFunction(defpos, cx, Sqlx.SUM,
                            (SqlValue)cx.Add(new SqlValueExpr(u + 2, cx, Sqlx.TIMES, vl, vl, Sqlx.NO)), null, null,
                            Sqlx.NO, new BTree<long, object>(_Alias, n2)));
                        rem = rem + (c0.defpos, n0) + (c1.defpos, n1) + (c2.defpos, n2);
                        // c0 is SUM(x), c1 is COUNT, c2 is SUM(X*X)
                        // SQRT((c2-2*c0*xbar+xbar*xbar)/c1)
                        var s0 = (SqlValue)cx.Add(new SqlValue(u + 3, "", domain, null, BTree<long, object>.Empty
                            + (Query, gf)));
                        var s1 = (SqlValue)cx.Add(new SqlValue(u + 4, "", domain, null, BTree<long, object>.Empty
                            + (Query, gf)));
                        var s2 = (SqlValue)cx.Add(new SqlValue(u + 5, "", domain, null, BTree<long, object>.Empty
                            + (Query, gf)));
                        var xbar = (SqlValue)cx.Add(new SqlValueExpr(u + 6, cx, Sqlx.DIVIDE, s0, s1, Sqlx.NO));
                        var cu = (SqlValue)cx.Add(new SqlFunction(u + 7, cx, Sqlx.SQRT,
                            (SqlValue)cx.Add(new SqlValueExpr(u + 8, cx, Sqlx.DIVIDE,
                                (SqlValue)cx.Add(new SqlValueExpr(u + 9, cx, Sqlx.PLUS,
                                    (SqlValue)cx.Add(new SqlValueExpr(u + 10, cx, Sqlx.MINUS, s2,
                                        (SqlValue)cx.Add(new SqlValueExpr(u + 11, cx, Sqlx.TIMES, xbar,
                                            (SqlValue)cx.Add(new SqlValueExpr(u + 12, cx, Sqlx.TIMES, s0,
                                                (SqlValue)cx.Add(new SqlLiteral(u + 13, cx, new TReal(Domain.Real, 2.0))),
                                                Sqlx.NO)),
                                            Sqlx.NO)),
                                        Sqlx.NO)),
                                    (SqlValue)cx.Add(new SqlValueExpr(defpos, cx, Sqlx.TIMES, xbar, xbar, Sqlx.NO)),
                                    Sqlx.NO)),
                                s1, Sqlx.NO)),
                            null, null, Sqlx.NO));
                        gfc = gfc + (s0, n0) + (s1, n1) + (s2, n2);
                        map += (defpos, cu);
                        return (cu,gfc,rem,reg,map);
                    }
                default:
                    {
                        if (aggregates0())
                        {
                            var nk = kind;
                            var vt = vl?.domain;
                            var vn = ac;
                            if (kind == Sqlx.COUNT)
                            {
                                nk = Sqlx.SUM;
                                vt = Domain.Int;
                            }
                            var st = this + (_Alias, ac);
                            var u = cx.GetUid(2);
                            rem += (st.defpos, ac);
                            var va = (SqlValue)cx.Add(new SqlValue(u, "", vt, null, BTree<long, object>.Empty
                                + (Query, gf)));
                            var sf = (SqlValue)cx.Add(new SqlFunction(u + 1, cx, kind,
                                va, (SqlValue)cx.obs[op1], (SqlValue)cx.obs[op2], mod, new BTree<long, object>(_Alias, an)));
                            gfc += (va, vn);
                            map += (defpos, sf);
                            return (sf,gfc,rem,reg,map);
                        }
                        if (aggregates(cx))
                        {
                            SqlValue nv;
                            (nv, gfc, rem, reg,map) = vl._ColsForRestView(cx, sg, gf, gs, gfc, rem, reg, map);
                            var sr = new SqlFunction(defpos, cx, kind,
                                nv,
                                (SqlValue)cx.obs[op1], (SqlValue)cx.obs[op2], mod,
                                new BTree<long, object>(_Alias, an));
                            map += (defpos, sr);
                            return (sr,gfc,rem,reg,map);
                        }
                        an = alias ?? name;
                        gfc += (this, an);
                        rem += (defpos, an);
                        map += (defpos, this);
                        return (this,gfc,rem,reg,map);
                    }
            }
        }
        internal static Domain _Type(Context cx,Sqlx kind,SqlValue val, SqlValue op1)
        {
            switch (kind)
            {
                case Sqlx.ABS: return val?.domain??Domain.UnionNumeric;
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AVG: return Domain.UnionNumeric;
                case Sqlx.ARRAY: return Domain.Collection; 
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CASE: return val.domain;
                case Sqlx.CAST: return ((SqlTypeExpr)op1).domain;
                case Sqlx.CEIL: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.CEILING: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: cx.affected = Rvv.Empty; return Domain.Char;
                case Sqlx.COLLECT: return Domain.Collection;
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.CURRENT_USER: return Domain.Char;
                case Sqlx.ELEMENT: return val?.domain.elType??Domain.Content;
                case Sqlx.FIRST: return Domain.Content;
                case Sqlx.EXP: return Domain.Real;
                case Sqlx.EVERY: return Domain.Bool;
                case Sqlx.EXTRACT: return Domain.Int;
                case Sqlx.FLOOR: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.FUSION: return Domain.Collection;
                case Sqlx.INTERSECTION: return Domain.Collection;
                case Sqlx.LAST: return Domain.Content;
                case Sqlx.SECURITY: return Domain._Level;
                case Sqlx.LN: return Domain.Real;
                case Sqlx.LOCALTIME: return Domain.Timespan;
                case Sqlx.LOCALTIMESTAMP: return Domain.Timestamp;
                case Sqlx.LOWER: return Domain.Char;
                case Sqlx.MAX: return val?.domain??Domain.Content;
                case Sqlx.MIN: return val?.domain??Domain.Content;
                case Sqlx.MOD: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.NEXT: return val?.domain ?? Domain.UnionDate;
                case Sqlx.NORMALIZE: return Domain.Char;
                case Sqlx.NULLIF: return op1.domain;
                case Sqlx.OCTET_LENGTH: return Domain.Int;
                case Sqlx.OVERLAY: return Domain.Char;
                case Sqlx.PARTITION: return Domain.Char;
                case Sqlx.POSITION: return Domain.Int;
                case Sqlx.PROVENANCE: return Domain.Char;
                case Sqlx.POWER: return Domain.Real;
                case Sqlx.RANK: return Domain.Int;
                case Sqlx.ROW_NUMBER: return Domain.Int;
                case Sqlx.SET: return Domain.Collection;
                case Sqlx.STDDEV_POP: return Domain.Real;
                case Sqlx.STDDEV_SAMP: return Domain.Real;
                case Sqlx.SUBSTRING: return Domain.Char;
                case Sqlx.SUM: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.TRANSLATE: return Domain.Char;
                case Sqlx.TYPE_URI: return Domain.Char;
                case Sqlx.TRIM: return Domain.Char;
                case Sqlx.UPPER: return Domain.Char;
                case Sqlx.VERSIONING: return (val==null)?Domain.Int:Domain.Rvv;
                case Sqlx.WHEN: return val.domain;
                case Sqlx.XMLCAST: return op1.domain;
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return Domain.Null;
        }
        internal override TypedValue Eval(Context cx)
        {
            Cursor firstTie = null;
            Register fc = cx.funcs[defpos];
            TypedValue v = null;
            var vl = (SqlValue)cx.obs[val];
            switch (kind)
            {
                case Sqlx.ABS:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    switch (vl.domain.kind)
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
                                var cs = vl.domain.unionOf;
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
                        if (window == -1L || fc.mset == null || fc.mset.Count == 0)
                            return fc.acc;
                        fc.acc = new TArray(new Domain(Sqlx.ARRAY, fc.mset.tree?.First()?.key().dataType));
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
                        return domain.defaultValue;
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
                case Sqlx.CASE:
                    {
                        v = vl?.Eval(cx);
                        SqlFunction f = this;
                        for (; ; )
                        {
                            SqlFunction fg = cx.obs[f.op2] as SqlFunction;
                            if (fg == null)
                                return cx.obs[f.op2]?.Eval(cx)??null;
                            if (cx.obs[f.op1].domain.Compare(cx.obs[f.op1].Eval(cx), v) == 0)
                                return cx.obs[f.val].Eval(cx);
                            f = fg;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        return domain.Coerce(cx,v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = vl?.Eval(cx);
                    if (v == null)
                        return null;
                    switch (vl.domain.kind)
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
                            return domain.defaultValue;
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK: return new TRvv(cx,"");
                case Sqlx.COLLECT: return domain.Coerce(cx,(TypedValue)fc.mset ??TNull.Value);
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
                        return domain.defaultValue;
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
                    switch (vl.domain.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble()));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val()));
                    }
                    break;
                case Sqlx.FUSION: return domain.Coerce(cx,fc.mset);
                case Sqlx.INTERSECTION:return domain.Coerce(cx,fc.mset);
                case Sqlx.LAST: return fc.mset.tree.Last().key();
                case Sqlx.LAST_DATA:
                    return new TInt(cx.data[from]?.lastData??0L);
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
                        return domain.defaultValue;
                    }
                case Sqlx.MAX: return fc.acc;
                case Sqlx.MIN: return fc.acc;
                case Sqlx.MOD:
                    if (op1 != -1L)
                        v = cx.obs[op1].Eval(cx);
                    if (v.Val() == null)
                        return domain.defaultValue;
                    switch (cx.obs[op1].domain.kind)
                    {
                        case Sqlx.INTEGER:
                            return new TInt(v.ToLong() % cx.obs[op2].Eval(cx).ToLong());
                        case Sqlx.NUMERIC:
                            return new TNumeric(((Numeric)v.Val()) 
                                % (Numeric)cx.obs[op2].Eval(cx).Val());
                    }
                    break;
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
                            return domain.defaultValue;
                        TypedValue b = cx.obs[op2].Eval(cx);
                        if (b == null)
                            return null;
                        if (b.IsNull || cx.obs[op1].domain.Compare(a, b) != 0)
                            return a;
                        return domain.defaultValue;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        v = vl?.Eval(cx);
                        if (v == null)
                            return null;
                        if (v.Val() is byte[] bytes)
                            return new TInt(bytes.Length);
                        return domain.defaultValue;
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
                            return domain.defaultValue;
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
                            return domain.defaultValue;
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
                            return domain.defaultValue;
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
                            return domain.defaultValue;
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
                        return domain.defaultValue;
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
        internal override BTree<long,Register> StartCounter(Context cx,RowSet rs, BTree<long,Register> tg)
        {
            var fc = tg[defpos] ?? new Register();
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
                    fc.mset = new TMultiset(vl.domain);
                    break;
                case Sqlx.XMLAGG:
                    if (window != -1L)
                        goto case Sqlx.COLLECT;
                    fc.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window != -1L)
                        goto case Sqlx.COLLECT;
                    fc.bval = false;
                    break;
                case Sqlx.ARRAY:
                    fc.acc = new TArray(new Domain(Sqlx.ARRAY, Domain.Content));
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
                    if (window != -1L)
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
                    fc.sumInteger = null;
                    break;
                default:
                    if (vl!=null) tg = vl.StartCounter(cx, rs, tg);
                    break;
            }
            return tg += (defpos, fc);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            var tr = cx.db as Transaction;
            var vl = (SqlValue)cx.obs[val];
            if (tr == null)
                return tg;
            if (filter != -1L && ((SqlValue)cx.obs[filter]).Matches(cx)!=true)
                return tg;
            var fc = tg[defpos];
            if (mod == Sqlx.DISTINCT)
            {
                var v = vl.Eval(cx);
                if (v != null)
                {
                    if (fc.mset == null)
                        fc.mset = new TMultiset(new Domain(Sqlx.MULTISET,v.dataType));
                    else if (fc.mset.Contains(v))
                        return tg;
                    fc.mset.Add(v);
         //           etag = ETag.Add(v.etag, etag);
                }
            }
            switch (kind)
            {
                case Sqlx.AVG: // is not used with Remote
                    {
                        var v = vl.Eval(cx);
                        if (v == null)
                        {
       //                     (tr as Transaction)?.Warning("01003");
                            break;
                        }
        //                etag = ETag.Add(v.etag, etag);
                    }
                    fc.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ANY:
                    {
                        if (window != -1L)
                            goto case Sqlx.COLLECT;
                        var v = vl.Eval(cx);
                        if (v != null)
                        {
                            if (v.Val() is bool)
                                fc.bval = fc.bval || (bool)v.Val();
               //             else
               //                 (tr as Transaction)?.Warning("01003");
              //              etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.ARRAY: // Mongo $push
                    if (vl != null)
                    {
                        if (fc.acc == null)
                            fc.acc = new TArray(new Domain(Sqlx.ARRAY, vl.domain));
                        var ar = fc.acc as TArray;
                        var v = vl.Eval(cx);
                        if (v != null)
                        {
                            ar+=v;
                //            etag = ETag.Add(v.etag, etag);
                        }
               //         else
                //            (tr as Transaction)?.Warning("01003");
                    }
                    break;
                case Sqlx.COLLECT:
                    {
                        if (vl != null)
                        {
                            if (fc.mset == null && vl.Eval(cx) != null)
                                fc.mset = new TMultiset(vl.domain);
                            var v = vl.Eval(cx);
                            if (v != null)
                            {
                                fc.mset.Add(v);
                  //              etag = ETag.Add(v.etag, etag);
                            }
                //            else
                //                (tr as Transaction)?.Warning("01003");
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
                        {
                            fc.count++;
            //                etag = ETag.Add(v.etag, etag);
                        }
                    }
                    break;
                case Sqlx.EVERY:
                    {
                        var v = vl.Eval(cx);
                        if (v is TBool vb)
                        {
                            fc.bval = fc.bval && vb.value.Value;
   //                         etag = ETag.Add(v.etag, etag);
                        }
         //               else
         //                   tr.Warning("01003");
                        break;
                    }
                case Sqlx.FIRST:
                    if (vl != null && fc.acc == null)
                    {
                        fc.acc = vl.Eval(cx);
            //            if (fd.cur.acc != null)
             //           {
             //               domain = fd.cur.acc.dataType;
            //                etag = ETag.Add(cur.acc.etag, etag);
             //           }
                    }
                    break;
                case Sqlx.FUSION:
                    {
                        if (fc.mset == null || fc.mset.IsNull)
                        {
                            var vv = vl.Eval(cx);
                            if (vv == null || vv.IsNull)
                                fc.mset = new TMultiset(vl.domain.elType); // check??
            //                else
            //                    (tr as Transaction)?.Warning("01003");
                        }
                        else
                        {
                            var v = vl.Eval(cx);
                            fc.mset = TMultiset.Union(fc.mset, v as TMultiset, true);
              //              etag = ETag.Add(v?.etag, etag);
                        }
                        break;
                    }
                case Sqlx.INTERSECTION:
                    {
                        var v = vl.Eval(cx);
               //         if (v == null)
               //             (tr as Transaction)?.Warning("01003");
               //         else
                        {
                            var mv = v as TMultiset;
                            if (fc.mset == null || fc.mset.IsNull)
                                fc.mset = mv;
                            else
                                fc.mset = TMultiset.Intersect(fc.mset, mv, true);
               //             etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.LAST:
                    if (vl != null)
                    {
                        fc.acc = vl.Eval(cx);
                 //       if (fd.cur.acc != null)
                 //       {
                //            domain = cur.acc.dataType;
                //            etag = ETag.Add(val.etag, etag);
                 //       }
                    }
                    break;
                case Sqlx.MAX:
                    {
                        TypedValue v = vl.Eval(cx);
                        if (v != null && (fc.acc == null || fc.acc.CompareTo(v) < 0))
                        {
                            fc.acc = v;
               //             etag = ETag.Add(v.etag, etag);
                        }
              //          else
               //             (tr as Transaction)?.Warning("01003");
                        break;
                    }
                case Sqlx.MIN:
                    {
                        TypedValue v = vl.Eval(cx);
                        if (v != null && (fc.acc == null || fc.acc.CompareTo(v) > 0))
                        {
                            fc.acc = v;
             //             etag = ETag.Add(v.etag, etag);
                        }
             //           else
             //               (tr as Transaction)?.Warning("01003");
                        break;
                    }
                case Sqlx.STDDEV_POP: // not used for Remote
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
                        if (v==null)
                        {
                //            tr.Warning("01003");
                            return tg;
                        }
               //         etag = ETag.Add(v.etag, etag);
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInt)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumLong = v.ToLong().Value;
                                } else if (v is TInteger)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumInteger = (Integer)v.Val();
                                } else if (v is TReal)
                                {
                                    fc.sumType = Domain.Real;
                                    fc.sum1 = ((TReal)v).dvalue;
                                } else if (v is TNumeric)
                                {
                                    fc.sumType = Domain.Numeric;
                                    fc.sumDecimal = ((TNumeric)v).value;
                                }
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInt)
                                {
                                    long a = v.ToLong().Value;
                                    if (fc.sumInteger == null)
                                    {
                                        if ((a > 0) ? (fc.sumLong <= long.MaxValue - a) : (fc.sumLong >= long.MinValue - a))
                                            fc.sumLong += a;
                                        else
                                            fc.sumInteger = new Integer(fc.sumLong) + new Integer(a);
                                    }
                                    else
                                        fc.sumInteger = fc.sumInteger + new Integer(a);
                                } else if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (fc.sumInteger == null)
                                        fc.sumInteger = new Integer(fc.sumLong) + a;
                                    else
                                        fc.sumInteger = fc.sumInteger + a;
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
                        {
                            fc.sb.Append(o.ToString());
                 //           etag = ETag.Add(o.etag, etag);
                        }
                //        else
                 //           tr.Warning("01003");
                        break;
                    }
                default:
                    vl?.AddIn(cx,rb,tg);
                    break;
            }
            return tg; 
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
            var wn = (WindowSpecification)cx.obs[window];
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
            var wn = (WindowSpecification)cx.obs[window];
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
            var wn = (WindowSpecification)cx.obs[window];
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
            var wn = (WindowSpecification)cx.obs[window];
            if (wn.high == null || wn.high.unbounded)
                return true;
            var n = val;
            var kt = cx.obs[val].domain;
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
            var wn = (WindowSpecification)cx.obs[window];
            if (wn.low == null || wn.low.unbounded)
                return true;
            var n = val;
            var kt = cx.obs[val].domain;
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
        internal override bool Check(Context cx,GroupSpecification group)
        {
            if (aggregates0())
                return false;
            return base.Check(cx,group);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[val])?.Needs(cx,qn) ?? qn;
            qn = ((SqlValue)cx.obs[op1])?.Needs(cx,qn) ?? qn;
            qn = ((SqlValue)cx.obs[op2])?.Needs(cx,qn) ?? qn;
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
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            if (cx.obs[val] is SqlValue v)
                r += v.Needs(cx, rs);
            if (cx.obs[op1] is SqlValue o1)
                r += o1.Needs(cx, rs); 
            if (cx.obs[op2] is SqlValue o2)
                r += o2.Needs(cx, rs);
            return r;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, filter, VIC.OK | VIC.OV);
            t = Scan(t, val, VIC.OK | VIC.OV);
            t = Scan(t, op1, VIC.OK | VIC.OV);
            t = Scan(t, op2, VIC.OK | VIC.OV);
            t = Scan(t, query, VIC.OK | VIC.OV);
            return base.Scan(t);
        }
        // tailor REST call to remote DBMS
        internal override string ToString(string sg, CTree<string, CTree<long,long>> cs, Context cx)
        {
            switch (sg)
            {
                case "Pyrrho":
                    {
                        var sb = new StringBuilder();
                        var vl = (cx.obs[val] as SqlValue)?.ToString(sg, cs, cx);
                        var o1 = (cx.obs[op1] as SqlValue)?.ToString(sg, cs, cx);
                        var o2 = (cx.obs[op2] as SqlValue)?.ToString(sg, cs, cx);
                        switch (kind)
                        {
                            case Sqlx.ABS: 
                            case Sqlx.ANY: 
                            case Sqlx.AVG:
                            case Sqlx.CARDINALITY:
                            case Sqlx.CEIL: 
                            case Sqlx.CEILING: 
                            case Sqlx.CHAR_LENGTH: 
                            case Sqlx.CHARACTER_LENGTH:
                            case Sqlx.COUNT:
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
                                sb.Append(vl); sb.Append(')'); break;
                            case Sqlx.ARRAY: 
                            case Sqlx.CASE: 
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
                                sb.Append(domain.name); sb.Append(')');
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
                        return sb.ToString();
                    }
            }
            return base.ToString(sg, cs, cx);
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
        internal bool aggregates0()
        {
            if (window != -1L)
                return false;
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
        internal override bool aggregates(Context cx)
        {
            return aggregates0() || (cx.obs[val]?.aggregates(cx)==true) 
                || (cx.obs[op1]?.aggregates(cx)==true) 
                || (cx.obs[op2]?.aggregates(cx)==true);
        }
    }
    /// <summary>
    /// The Parser converts this n-adic function to a binary one
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long dp, Context cx, SqlValue op1, SqlValue op2)
            : base(dp, cx, Sqlx.COALESCE, null, op1, op2, Sqlx.NO) { }
        protected SqlCoalesce(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator+(SqlCoalesce s,(long,object)x)
        {
            return new SqlCoalesce(s.defpos, s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[op1].Eval(cx) is TypedValue lf) ? 
                ((lf.IsNull) ? cx.obs[op2].Eval(cx) : lf) : null;
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
            : base(dp, cx, Sqlx.TYPE_URI, null, op1, null, Sqlx.NO) { }
        protected SqlTypeUri(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeUri operator+(SqlTypeUri s,(long,object)x)
        {
            return new SqlTypeUri(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTypeUri(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTypeUri(dp,mem);
        }
        internal override TypedValue Eval(Context cx)
        {
            TypedValue v = null;
            if (op1 != -1L)
                v = cx.obs[op1].Eval(cx);
            if (v==null || v.IsNull)
                return domain.defaultValue;
            var st = v.dataType;
            if (st.iri != null)
                return v;
            return domain.defaultValue;
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
        internal const long // these constants are used in other classes too
            All = -348, // bool
            Between = -349, // bool
            Found = -350, // bool
            High = -351, // long SqlValue
            Low = -352, // long SqlValue
            Op = -353, // Sqlx
            _Select = -354, //long QuerySpecification
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
        /// <param name="s">the query specification to test against</param>
        internal QuantifiedPredicate(long defpos,Context cx,SqlValue w, Sqlx o, bool a, QuerySpecification s)
            : base(defpos,BTree<long,object>.Empty+(_Domain,Domain.Bool)
            + (What,w.defpos)+(Op,o)+(All,a)+(_Select,s.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(s.defpos,true))
                  +(Depth,1+_Max(w.depth,s.depth))) {}
        protected QuantifiedPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QuantifiedPredicate operator+(QuantifiedPredicate s,(long,object)x)
        {
            return new QuantifiedPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuantifiedPredicate(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuantifiedPredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (QuantifiedPredicate)base._Relocate(wr);
            r += (What, wr.Fixed(what)?.defpos??-1L);
            r += (_Select, wr.Fixed(select)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (QuantifiedPredicate)base.Fix(cx);
            var nw = cx.obuids[what] ?? what;
            if (nw != what)
                r += (What, nw);
            var ns = cx.obuids[select] ?? select;
            if (ns != select)
                r += (_Select, ns);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuantifiedPredicate)base._Replace(cx, so, sv);
            var wh = cx.Replace(r.what,so,sv);
            if (wh != r.what)
                r += (What, wh);
            var se = cx.Replace(r.select, so, sv);
            if (se != r.select)
                r += (_Select, se);
            r = (QuantifiedPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[what])?.Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            var s = (QuerySpecification)cx.obs[select];
            switch (op)
            {
                case Sqlx.EQL: return new QuantifiedPredicate(defpos, cx, w, Sqlx.NEQ, !all, s);
                case Sqlx.NEQ: return new QuantifiedPredicate(defpos, cx, w, Sqlx.EQL, !all, s);
                case Sqlx.LEQ: return new QuantifiedPredicate(defpos, cx, w, Sqlx.GTR, !all, s);
                case Sqlx.LSS: return new QuantifiedPredicate(defpos, cx, w, Sqlx.GEQ, !all, s);
                case Sqlx.GEQ: return new QuantifiedPredicate(defpos, cx, w, Sqlx.LSS, !all, s);
                case Sqlx.GTR: return new QuantifiedPredicate(defpos, cx, w, Sqlx.LEQ, !all, s);
                default: throw new PEException("PE65");
            }
        }
        /// <summary>
        /// Analysis stage Conditions: process conditions
        /// </summary>
        internal override Query Conditions(Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            return ((SqlValue)cx.obs[what]).Conditions(cx, q, false, out _);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            var r = cx.obs[what].Needs(cx, rs);
            r += cx.obs[select].Needs(cx, rs);
            return r;
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return false;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var wv = (SqlValue)cx.obs[what];
            for (var rb = ((Query)cx.obs[select])
                .RowSets(cx,cx.data[from]?.finder?? CTree<long, RowSet.Finder>.Empty)
                .First(cx); rb != null; rb = rb.Next(cx))
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
        internal override bool aggregates(Context cx)
        {
            return ((SqlValue)cx.obs[what]).aggregates(cx)||((Query)cx.obs[select]).aggregates(cx);
        }
        internal override BTree<long, Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what]).StartCounter(cx, rs, tg);
            return ((Query)cx.obs[select]).StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            tg = ((SqlValue)cx.obs[what]).AddIn(cx,rb, tg);
            return ((Query)cx.obs[select]).AddIn(cx,rb,tg);
        }
        /// <summary>
        /// We aren't a column reference. If the select needs something
        /// From will add it to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what]).Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, what, VIC.OK | VIC.OV);
            t = Scan(t, select, VIC.OK | VIC.OV);
            return base.Scan(t);
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
        internal BetweenPredicate(long defpos,SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(defpos,BTree<long,object>.Empty+(_Domain,Domain.Bool)
                  +(QuantifiedPredicate.What,w.defpos)+(QuantifiedPredicate.Between,b)
                  +(QuantifiedPredicate.Low,a.defpos)+(QuantifiedPredicate.High,h.defpos)
                  +(Dependents,new CTree<long,bool>(w.defpos,true)+(a.defpos,true)+(h.defpos,true))
                  +(Depth,1+_Max(w.depth,a.depth,h.depth)))
        { }
        protected BetweenPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BetweenPredicate operator+(BetweenPredicate s,(long,object)x)
        {
            return new BetweenPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new BetweenPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new BetweenPredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (BetweenPredicate)base._Relocate(wr);
            r += (QuantifiedPredicate.What, wr.Fixed(what)?.defpos??-1L);
            r += (QuantifiedPredicate.Low, wr.Fixed(low)?.defpos ?? -1L);
            r += (QuantifiedPredicate.High, wr.Fixed(high)?.defpos ?? -1L);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[what])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[low])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[high])?.Grouped(cx, gs);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (BetweenPredicate)base.Fix(cx);
            var nw = cx.obuids[what] ?? what;
            if (what!=nw)
                r += (QuantifiedPredicate.What, cx.obuids[what]??what);
            var nl = cx.obuids[low] ?? low;
            if (low!=nl)
                r += (QuantifiedPredicate.Low, nl);
            var nh = cx.obuids[high] ?? high;
            if (high !=nh)
                r += (QuantifiedPredicate.High, nh);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (BetweenPredicate)base._Replace(cx, so, sv);
            var wh = cx.Replace(r.what, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = cx.Replace(r.low, so, sv);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = cx.Replace(r.high, so, sv);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            r = (BetweenPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool KnownBy(Context cx, Query q)
        {
            return (((SqlValue)cx.obs[low])?.KnownBy(cx, q)??true)
                && (((SqlValue)cx.obs[high])?.KnownBy(cx, q)??true)
                && ((SqlValue)cx.obs[what]).KnownBy(cx, q);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return (((SqlValue)cx.obs[low])?.KnownBy(cx, r) ?? true)
                && (((SqlValue)cx.obs[high])?.KnownBy(cx, r) ?? true)
                && ((SqlValue)cx.obs[what]).KnownBy(cx, r);
        }
        internal override bool Uses(Context cx,long t)
        {
            return (((SqlValue)cx.obs[low])?.Uses(cx,t)??false) 
                || (((SqlValue)cx.obs[high])?.Uses(cx,t)??false) 
                || ((SqlValue)cx.obs[what]).Uses(cx,t);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            return new BetweenPredicate(defpos,(SqlValue)cx.obs[what], !between, 
                (SqlValue)cx.obs[low], (SqlValue)cx.obs[high]);
        }
        internal override bool aggregates(Context cx)
        {
            return ((SqlValue)cx.obs[what])?.aggregates(cx)==true
                || ((SqlValue)cx.obs[low])?.aggregates(cx)==true
                || ((SqlValue)cx.obs[high])?.aggregates(cx)==true;
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
        /// <summary>
        /// Analysis stage Conditions: support distribution of conditions to froms etc
        /// </summary>
        internal override Query Conditions(Context cx,Query q,bool disj,out bool move)
        {
            move = false;
            var oq = q;
            q = ((SqlValue)cx.obs[what])?.Conditions(cx, q, false,out _)??q;
            q = ((SqlValue)cx.obs[low])?.Conditions(cx, q, false, out _)??q;
            q = ((SqlValue)cx.obs[high])?.Conditions(cx, q, false, out _)??q;
            return (Query)oq.New(cx,q.mem);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
                var t = wv.domain;
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what])?.Needs(cx,qn)??qn;
            qn = ((SqlValue)cx.obs[low])?.Needs(cx,qn)??qn;
            qn = ((SqlValue)cx.obs[high])?.Needs(cx,qn)??qn;
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, low, VIC.OK | VIC.OV);
            t = Scan(t, high, VIC.OK | VIC.OV);
            return base.Scan(t);
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
        internal LikePredicate(long dp,SqlValue a, bool k, SqlValue b, SqlValue e)
            : base(dp, _Esc(e)+(Left,a.defpos)+(_Like,k)+(Right,b.defpos)
                  +(_Domain,Domain.Bool)
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true))
                  +(Depth,1+_Max(a.depth,b.depth,e?.depth??0)))
        { }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Esc(SqlValue e)
        {
            var r = BTree<long, object>.Empty;
            if (e != null)
                r = r + (Escape, e.defpos) + (e.defpos, true);
            return r;
        }
        public static LikePredicate operator+(LikePredicate s,(long,object)x)
        {
            return new LikePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LikePredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new LikePredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (Escape, wr.Fixed(escape)?.defpos??-1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (LikePredicate)base.Fix(cx);
            var ne = cx.obuids[escape] ?? escape;
            if (escape!=ne)
                r += (Escape, ne);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LikePredicate)base._Replace(cx, so, sv);
            var wh = cx.Replace(r.left, so, sv);
            if (wh != r.left)
                r += (Left, wh);
            var rg = cx.Replace(r.right, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            var esc = cx.Replace(r.escape, so, sv);
            if (esc != r.escape)
                r += (Escape, esc);
            r = (LikePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[escape])?.Grouped(cx, gs);
            ((SqlValue)cx.obs[left]).Grouped(cx, gs);
            ((SqlValue)cx.obs[right]).Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
        internal override bool KnownBy(Context cx, Query q)
        {
            return ((SqlValue)cx.obs[left]).KnownBy(cx, q)
                && ((SqlValue)cx.obs[right]).KnownBy(cx, q)
                && ((SqlValue)cx.obs[escape])?.KnownBy(cx, q) != false;
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            return ((SqlValue)cx.obs[left]).KnownBy(cx, r)
                && ((SqlValue)cx.obs[right]).KnownBy(cx, r)
                && ((SqlValue)cx.obs[escape])?.KnownBy(cx, r) != false;
        }
        internal override bool Uses(Context cx,long t)
        {
            return ((SqlValue)cx.obs[left]).Uses(cx,t) || 
                ((SqlValue)cx.obs[right]).Uses(cx,t) 
                || ((SqlValue)cx.obs[escape])?.Uses(cx,t)==true;
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert(Context cx)
        {
            return new LikePredicate(defpos,(SqlValue)cx.obs[left], !like, 
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
        internal override bool aggregates(Context cx)
        {
            return cx.obs[left].aggregates(cx) || cx.obs[right].aggregates(cx);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[left]).Needs(cx,qn);
            qn = ((SqlValue)cx.obs[right]).Needs(cx,qn);
            qn = ((SqlValue)cx.obs[escape])?.Needs(cx,qn) ?? qn;
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
    /// </summary>
    internal class InPredicate : SqlValue
    {
        public long what => (long)(mem[QuantifiedPredicate.What]??-1L);
        /// <summary>
        /// In or not in
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// A query should be specified (unless a list of values is supplied instead)
        /// </summary>
        public long select => (long)(mem[QuantifiedPredicate._Select]??-1L); // or
        /// <summary>
        /// A list of values to check (unless a query is supplied instead)
        /// </summary>
        public CList<long> vals => (CList<long>)mem[QuantifiedPredicate.Vals]??CList<long>.Empty;
        public InPredicate(long dp, SqlValue w, BList<SqlValue> vs = null) 
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool)
                  +(QuantifiedPredicate.What,w?.defpos??-1L)+(QuantifiedPredicate.Vals,_Cols(vs))
                  +(Dependents,_Deps(vs)+(w.defpos,true))+(Depth,1+_Max(w.depth,_Depth(vs))))
        {}
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        static CList<long> _Cols(BList<SqlValue> vs)
        {
            var cs = CList<long>.Empty;
            for (var b = vs?.First(); b != null; b = b.Next())
                cs += b.value().defpos;
            return cs;
        }
        public static InPredicate operator+(InPredicate s,(long,object)x)
        {
            return new InPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new InPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new InPredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (InPredicate)base._Relocate(wr);
            r += (QuantifiedPredicate.What, wr.Fixed(what)?.defpos??-1L);
            r += (QuantifiedPredicate._Select, wr.Fixed(select)?.defpos??-1L);
            r += (QuantifiedPredicate.Vals, wr.Fix(vals));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (InPredicate)base.Fix(cx);
            var nw = cx.obuids[what] ?? what;
            if (what!=nw)
                r += (QuantifiedPredicate.What, nw);
            var ns = cx.obuids[select] ?? select;
            if (select!=ns)
                r += (QuantifiedPredicate._Select, ns);
            var nv = cx.Fix(vals);
            if (vals!=nv)
            r += (QuantifiedPredicate.Vals, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InPredicate)base._Replace(cx, so, sv);
            var wh = cx.Replace(r.what, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = cx.Replace(r.select, so, sv);
            if (wr != r.select)
                r += (QuantifiedPredicate._Select, wr);
            var vs = vals;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = cx.Replace(b.value(), so, sv);
                if (v != b.value())
                    vs += (b.key(), v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            r = (InPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[what]).Grouped(cx, gs);
            gs.Grouped(cx, vals);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
        internal override bool KnownBy(Context cx, Query q)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, q))
                    return false;
            return ((SqlValue)cx.obs[what]).KnownBy(cx, q);
        }
        internal override bool KnownBy(Context cx, RowSet r)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (!((SqlValue)cx.obs[b.value()]).KnownBy(cx, r))
                    return false;
            return ((SqlValue)cx.obs[what]).KnownBy(cx, r);
        }
        internal override bool Uses(Context cx,long t)
        {
            for (var b = vals.First(); b != null; b = b.Next())
                if (((SqlValue)cx.obs[b.value()]).Uses(cx, t))
                    return true;
            return ((SqlValue)cx.obs[what]).Uses(cx,t) || ((Query)cx.obs[select])?.Uses(cx,t)==true;
        }
        /// <summary>
        /// Analysis stage Conditions: check to see what conditions can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            var oq = q;
            if (cx.obs[what] is SqlValue w)
            {
                q = w.Conditions(cx, q, false, out _);
                for (var v = vals.First(); v != null; v = v.Next())
                    q = ((SqlValue)cx.obs[v.value()]).Conditions(cx, q, false, out _);
            }
            return (Query)oq.New(cx,q.mem);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
                        if (sv.domain.Compare(w, sv.Eval(cx)) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = ((Query)cx.obs[select])
                        .RowSets(cx,cx.data[from]?.finder?? CTree<long, RowSet.Finder>.Empty)
                        .First(cx); 
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
        internal override bool aggregates(Context cx)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                if (cx.obs[v.value()].aggregates(cx))
                    return true;
            return cx.obs[what].aggregates(cx) || base.aggregates(cx);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[what]).Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, vals, VIC.OK | VIC.OV);
            return base.Scan(t);
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
    /// </summary>
    internal class MemberPredicate : SqlValue
    {
        internal const long
            Found = -360, // bool
            Lhs = -361, // long
            Rhs = -362; // long
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
        internal MemberPredicate(long dp,SqlValue a, bool f, SqlValue b)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool)
                  +(Lhs,a)+(Found,f)+(Rhs,b)+(Depth,1+_Max(a.depth,b.depth))
                  +(Dependents,new CTree<long,bool>(a.defpos,true)+(b.defpos,true)))
        { }
        protected MemberPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static MemberPredicate operator+(MemberPredicate s,(long,object)x)
        {
            return new MemberPredicate(s.defpos, s.mem+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MemberPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new MemberPredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (MemberPredicate)base._Relocate(wr);
            r += (Lhs, wr.Fixed(lhs)?.defpos??-1L);
            r += (Rhs, wr.Fixed(rhs)?.defpos ?? -1L);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (MemberPredicate)base.Fix(cx);
            var nl = cx.obuids[lhs] ?? lhs;
            if (nl != lhs)
                r += (Lhs, nl);
            var nr = cx.obuids[rhs] ?? rhs;
            if (nr != rhs)
                r += (Rhs, rhs);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MemberPredicate)base._Replace(cx, so, sv);
            var lf = cx.Replace(lhs,so,sv);
            if (lf != left)
                r += (Lhs,lf);
            var rg = cx.Replace(rhs,so,sv);
            if (rg != rhs)
                r += (Rhs,rg);
            r = (MemberPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[lhs]).Grouped(cx, gs);
            ((SqlValue)cx.obs[rhs]).Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            return new MemberPredicate(defpos,(SqlValue)cx.obs[lhs], !found, 
                (SqlValue)cx.obs[rhs]);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            var oq = q;
            q = ((SqlValue)cx.obs[lhs]).Conditions(cx, q, false,out _);
            q = ((SqlValue)cx.obs[rhs]).Conditions(cx, q, false, out _);
            return (Query)oq.New(cx,q.mem);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
                    return domain.defaultValue;
                if (a.IsNull)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                throw cx.db.Exception("42113", b.GetType().Name).Mix();
            }
            return null;
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[lhs].aggregates(cx)||cx.obs[rhs].aggregates(cx);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[lhs]).Needs(cx,qn);
            qn = ((SqlValue)cx.obs[rhs]).Needs(cx,qn);
            return qn;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?>t)
        {
            t = Scan(t, lhs, VIC.OK | VIC.OV);
            t = Scan(t, rhs, VIC.OK | VIC.OV);
            return base.Scan(t);
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
        internal TypePredicate(long dp,SqlValue a, bool f, BList<Domain> r)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool)
                  +(MemberPredicate.Lhs,a.defpos)+(MemberPredicate.Found,f)
                  +(MemberPredicate.Rhs,r))
        {  }
        protected TypePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TypePredicate operator+(TypePredicate s,(long,object)x)
        {
            return new TypePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TypePredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TypePredicate(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TypePredicate)base._Relocate(wr);
            r += (MemberPredicate.Lhs, wr.Fixed(lhs).defpos);
            r += (MemberPredicate.Rhs, wr.Fix(rhs));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TypePredicate)base.Fix(cx);
            var nl = cx.obuids[lhs] ?? lhs;
            if (nl!=lhs)
            r += (MemberPredicate.Lhs, nl);
            var nr = cx.Fix(rhs);
            if (nr!=rhs)
            r += (MemberPredicate.Rhs, nr);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TypePredicate)base._Replace(cx, so, sv);
            var lh = cx.Replace(r.lhs, so, sv);
            if (lh != r.lhs)
                r += (MemberPredicate.Lhs, lh);
            r = (TypePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
            return new TypePredicate(defpos,(SqlValue)cx.obs[lhs], !found, rhs);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            var oq = q;
            q = ((SqlValue)cx.obs[lhs]).Conditions(cx, q, false, out _);
            return (Query)oq.New(cx,q.mem);
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
                b = at.EqualOrStrongSubtypeOf(cx,t.value()); // implemented as Equals for ONLY
            return TBool.For(b == found);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return ((SqlValue)cx.obs[lhs]).Needs(cx,qn);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return cx.obs[lhs].Needs(cx, rs);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        internal Sqlx kind => (Sqlx)mem[Domain.Kind];
        public PeriodPredicate(long dp,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,BTree<long,object>.Empty+(_Domain,Domain.Bool)
                 +(Left,op1?.defpos??-1L)+(Right,op2?.defpos??-1L)+(Domain.Kind,o)
                 +(Dependents,new CTree<long,bool>(op1.defpos,true)+(op2.defpos,true))
                 +(Depth,1+_Max(op1.depth,op2.depth)))
        { }
        protected PeriodPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static PeriodPredicate operator+(PeriodPredicate s,(long,object)x)
        {
            return new PeriodPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new PeriodPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new PeriodPredicate(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PeriodPredicate)base._Replace(cx, so, sv);
            var a = cx.Replace(left, so, sv);
            if (a != left)
                r += (Left, a);
            var b = cx.Replace(right, so, sv);
            if (b != r.right)
                r += (Right, b);
            r = (PeriodPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[left]).Grouped(cx, gs);
            ((SqlValue)cx.obs[right]).Grouped(cx, gs);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
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
        internal override bool aggregates(Context cx)
        {
            return (cx.obs[left]?.aggregates(cx)??false)||(cx.obs[right]?.aggregates(cx)??false);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            qn = ((SqlValue)cx.obs[left])?.Needs(cx,qn) ??qn;
            qn = ((SqlValue)cx.obs[right])?.Needs(cx,qn) ??qn;
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
            var sb = new StringBuilder();
            sb.Append(left);
            sb.Append(' '); sb.Append(kind); sb.Append(' ');
            sb.Append(right);
            return sb.ToString();
        }
    }
    /// <summary>
    /// A base class for QueryPredicates such as ANY
    /// </summary>
    internal abstract class QueryPredicate : SqlValue
    {
        internal const long
            QExpr = -363; // long Query
        public long expr => (long)(mem[QExpr]??-1);
        /// <summary>
        /// the base query
        /// </summary>
        public QueryPredicate(long dp,Query e,BTree<long,object>m=null) 
            : base(dp, (m??BTree<long,object>.Empty)+(QExpr,e)
                  +(Dependents,new CTree<long,bool>(e.defpos,true))+(Depth,1+e.depth))
        {  }
        protected QueryPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QueryPredicate operator+(QueryPredicate q,(long,object)x)
        {
            return (QueryPredicate)q.New(q.mem + x);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (QueryPredicate)base._Relocate(wr);
            r += (QExpr, wr.Fixed(expr).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (QueryPredicate)base.Fix(cx);
            var nq = cx.obuids[expr] ?? expr;
            if (nq != expr)
                r += (QExpr, nq);
            return r;
        }
        internal override DBObject _Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QueryPredicate)base._Replace(cx,so,sv);
            var q = (Query)cx._Replace(r.expr,so,sv);
            if (q.defpos != r.expr)
                r += (QExpr, q.defpos);
            r = (QueryPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// analysis stage Conditions: analyse the expr (up to building its rowset)
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            return (Query)cx.obs[expr];
        }
        /// <summary>
        /// if groupby is specified we need to check TableColumns are aggregated or grouped
        /// </summary>
        /// <param name="group"></param>
        internal override bool Check(Context cx, GroupSpecification group)
        {
            var cols = ((Query)cx.obs[expr]).rowType;
            for (var b=cols.First(); b!=null; b=b.Next())
                if (((SqlValue)cx.obs[b.value()]).Check(cx,group))
                    return true;
            return base.Check(cx,group);
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        { }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[expr].aggregates(cx);
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
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
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
    /// </summary>
    internal class ExistsPredicate : QueryPredicate
    {
        public ExistsPredicate(long dp,Query e) : base(dp,e,BTree<long,object>.Empty
            +(Dependents,new CTree<long,bool>(e.defpos,true))+(Depth,1+e.depth)) { }
        protected ExistsPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ExistsPredicate operator+(ExistsPredicate s,(long,object)x)
        {
            return new ExistsPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExistsPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ExistsPredicate(dp,mem);
        }
        /// <summary>
        /// The predicate is true if the rowSet has at least one element
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return TBool.For(((Query)cx.obs[expr])
                .RowSets(cx,cx.data[from]?.finder?? CTree<long, RowSet.Finder>.Empty)
                .First(cx)!=null);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, expr, VIC.OK | VIC.OV);
            return base.Scan(t);
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
    /// </summary>
    internal class UniquePredicate : QueryPredicate
    {
        public UniquePredicate(long dp,Query e) : base(dp,e) {}
        protected UniquePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UniquePredicate operator +(UniquePredicate s, (long, object) x)
        {
            return new UniquePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UniquePredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new UniquePredicate(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (UniquePredicate)base._Replace(cx, so, sv);
            var ex = (Query)cx._Replace(r.expr, so, sv);
            if (ex.defpos != r.expr)
                r += (QExpr, ex.defpos);
            r = (UniquePredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            RowSet rs = ((Query)cx.obs[expr])
                .RowSets(cx,cx.data[from]?.finder?? CTree<long, RowSet.Finder>.Empty);
            RTree a = new RTree(rs.defpos,cx,rs.rt,rs.domain,TreeBehaviour.Disallow, TreeBehaviour.Disallow);
            for (var rb=rs.First(cx);rb!= null;rb=rb.Next(cx))
                if (RTree.Add(ref a, rb, cx.cursors) == TreeBehaviour.Disallow)
                    return TBool.False;
            return TBool.True;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, expr, VIC.OK | VIC.OV);
            return base.Scan(t);
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
        internal const long
            NIsNull = -364, //bool
            NVal = -365; //long
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
            : base(dp,new BTree<long,object>(_Domain,Domain.Bool)
                  +(NVal,v.defpos)+(NIsNull,b)+(Dependents,new CTree<long,bool>(v.defpos,true))
                  +(Depth,1+v.depth))
        { }
        protected NullPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static NullPredicate operator+(NullPredicate s,(long,object)x)
        {
            return new NullPredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new NullPredicate(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new NullPredicate(dp,mem);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (NullPredicate)base.AddFrom(cx, q);
            var a = ((SqlValue)cx.obs[val]).AddFrom(cx, q);
            if (a.defpos != val)
                r += (NVal, a.defpos);
            return (SqlValue)cx.Add(r);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (NVal, wr.Fixed(val).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (NullPredicate)base.Fix(cx);
            var nv = cx.obuids[val] ?? val;
            if (nv != val)
                r += (NVal, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (NullPredicate)base._Replace(cx, so, sv);
            var vl = cx.Replace(r.val,so,sv);
            if (vl != r.val)
                r += (NVal, vl);
            r = (NullPredicate)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Grouped(Context cx, GroupSpecification gs)
        {
            ((SqlValue)cx.obs[val]).Grouped(cx, gs);
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return (cx.obs[val].Eval(cx) is TypedValue tv)? TBool.For(tv.IsNull == isnull) : null;
        }
        internal override bool aggregates(Context cx)
        {
            return cx.obs[val].aggregates(cx);
        }
        internal override BTree<long, Register> StartCounter(Context cx,RowSet rs, BTree<long, Register> tg)
        {
            return cx.obs[val].StartCounter(cx, rs, tg);
        }
        internal override BTree<long, Register> AddIn(Context cx,Cursor rb, BTree<long, Register> tg)
        {
            return cx.obs[val].AddIn(cx,rb, tg);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return ((SqlValue)cx.obs[val]).Needs(cx,qn);
        }
        internal override bool LocallyConstant(Context cx, RowSet rs)
        {
            return cx.obs[val].LocallyConstant(cx, rs);
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return cx.obs[val].Needs(cx, rs);
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, val, VIC.OK | VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            return isnull?"is null":"is not null";
        }
    }
}
